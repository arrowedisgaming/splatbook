/**
 * Applying a command to a card table, and reading back what happened.
 *
 * The shell owns the mechanism — versions, idempotency, the log — and knows
 * nothing about cards. The game supplies two functions and the shell calls
 * them: one that turns a command into a new state, and one that turns a state
 * into the view a given seat may have. Neither is inspected.
 *
 * **How the sync channel stays safe.** An earlier draft of the plan had clients
 * replaying a stream of commands, which meant every command needed its own
 * per-seat projection and a table of per-recipient secrets — a second way out
 * of the building, guarded separately from the first. Syncing projected *state*
 * instead removed the problem rather than defending it: private facts travel
 * only through the game's projection, which has its own leak tests, and the
 * event log carries public facts alone. "Seat 3 drew four cards", never which
 * four. One surface to hold correct instead of two.
 *
 * Server-only.
 */

import { and, asc, eq, gt } from 'drizzle-orm';
import type { Db } from './entities.ts';
import { cardTableEvents, type CardTableEvent, type CardTableRow } from './schema.ts';
import { getCardTable, isExpired, TABLE_RETENTION_MS, type SaveRefusal } from './card-tables.ts';
import { isUniqueViolation, runAtomic } from './atomic.ts';
import { MAX_COMMANDS_PER_TABLE } from '../../card-table-limits.ts';

/**
 * What a game's reducer hands back: the state after the command, and the public
 * fact to log about it.
 *
 * Refusing is allowed but rare by design — the table is permissive, so a
 * refusal here means the impossible (no such zone) or the unsafe (a card named
 * in a hand that is not yours), never the merely illegal.
 */
export type CommandOutcome =
	| { ok: true; state: unknown; stateVersion: number; kind: string; data?: unknown }
	| { ok: false; reason: string };

export type ApplyRefusal = SaveRefusal | 'no-such-table' | 'rejected';

export type ApplyResult =
	| { ok: true; table: CardTableRow; event: CardTableEvent; replayed: boolean }
	| { ok: false; reason: ApplyRefusal; detail?: string };

export interface ApplyCommandInput {
	tableId: string;
	/** Which seat is asking. Null for something the table does to itself. */
	actorSeatId: string | null;
	/** The version the client believed it was working from. */
	expectedVersion: number;
	/**
	 * Stable across retries of the same command and different for a new one.
	 * The unique index does the enforcing; this is only the key.
	 */
	requestHash: string;
	/**
	 * What the caller has just read about this table, if it has.
	 *
	 * A route loads the table to find out who is asking before it can run
	 * anything, so by the time a command gets here the row has usually been read
	 * once already. When that row is at the version the client expects, it is
	 * the row this would read, and reading it again was a round trip and a
	 * second parse of the whole state for nothing (issue #2).
	 *
	 * Trusting it is safe because the save is conditional on the version: if a
	 * command landed after the caller's read, the write matches nothing and this
	 * is the ordinary `conflict`. A row at any other version is ignored and the
	 * stored one read, exactly as when none is given.
	 *
	 * `prior` is the caller's own lookup of the request hash, made in the same
	 * trip as the row (`getTableWithSeatsByToken`): the event if this request has
	 * run before, `null` if it looked and found none, absent if it did not look.
	 * An event is a settled answer whatever the versions say. A `null` saves the
	 * lookup here and weakens nothing, because the lookup was never the lock:
	 * a duplicate that lands after it still loses on the unique index.
	 */
	loaded?: { table: CardTableRow; prior?: CardTableEvent | null };
}

/**
 * The version of an event whose command never finished being written.
 *
 * Nothing writes these any more. Commands used to claim their request hash by
 * inserting the event first, with this in place of a version, and stamp the
 * real one afterwards; a Worker that died between the two left the claim
 * behind. A table that was played under that code may still hold one, so the
 * readers below go on treating it as what it is: not part of anyone's history,
 * and not a settled answer to a retry.
 */
const UNWRITTEN = 0;

/**
 * Run one command against a table.
 *
 * **The request hash is a lock, not a lookup.** Checking for a previous run and
 * then applying would leave a window: a client that retried the same command
 * with a refreshed version — which is exactly what a naive retry-on-conflict
 * loop does — could slip between another copy's check and its write, and the
 * command would apply twice. Two cards moved for one click.
 *
 * So the lookup is only a shortcut to the answer for a command that has
 * already run. What stops a second copy is the unique index on the event's
 * request hash, and the event is written **in the same transaction as the
 * state** (`runAtomic`). Both or neither:
 *
 * - The event is inserted only if the table is still at the version the client
 *   expects, still inside its retention window and still under its command
 *   ceiling. The state is saved under the same condition. Somebody else's
 *   command landing first makes both match nothing, and nothing is written.
 * - If the same request hash has landed in the meantime, the table has moved
 *   on and both match nothing, or (should the versions happen to agree) the
 *   index refuses the event and undoes the whole unit. Either way the caller
 *   is given that earlier run's answer.
 *
 * An earlier version did this in three separate writes, claiming the hash
 * first and releasing the claim if the save lost, because this codebase
 * avoided transactions. That was three trips to D1 for every command (issue
 * #2). It also had a state this one cannot reach: a claim with no command
 * behind it, left by a request that stopped between its writes.
 */
export async function applyCommand(
	db: Db,
	input: ApplyCommandInput,
	reduce: (state: unknown) => CommandOutcome,
	now: number = Date.now()
): Promise<ApplyResult> {
	const loaded = input.loaded;
	if (loaded?.prior) return replayOf(loaded.table, loaded.prior);

	const current = loaded?.table.version === input.expectedVersion ? loaded : undefined;
	const table = current?.table ?? (await getCardTable(db, input.tableId));
	if (!table) return { ok: false, reason: 'no-such-table' };

	const existing =
		current?.prior === null ? undefined : await findByRequest(db, input.tableId, input.requestHash);
	if (existing) return replayOf(table, existing);

	// The reducer is pure, so a refusal costs nothing and writes nothing.
	const outcome = reduce(table.state);
	if (!outcome.ok) return { ok: false, reason: 'rejected', detail: outcome.reason };

	// The event's version is the version the write produces, so a client's
	// cursor is simply the version it already holds — one counter, not two.
	const event: CardTableEvent = {
		id: crypto.randomUUID(),
		tableId: input.tableId,
		version: input.expectedVersion + 1,
		actorSeatId: input.actorSeatId,
		kind: outcome.kind,
		data: outcome.data ?? {},
		requestHash: input.requestHash,
		createdAt: new Date(now)
	};

	// What both writes insist on. An expired table cannot be written back to
	// life, and a table at its ceiling has accepted all the commands it will.
	const stillThere = [
		input.tableId,
		input.expectedVersion,
		now - TABLE_RETENTION_MS,
		MAX_COMMANDS_PER_TABLE
	] as const;

	let changed: number[];
	try {
		changed = await runAtomic(db, [
			{
				sql: `INSERT INTO card_table_events
					(id, table_id, version, actor_seat_id, kind, data, request_hash, created_at)
					SELECT ?, ?, ?, ?, ?, ?, ?, ?
					WHERE EXISTS (
						SELECT 1 FROM card_tables
						WHERE id = ? AND version = ? AND last_active_at >= ? AND command_count < ?
					)`,
				params: [
					event.id,
					event.tableId,
					event.version,
					event.actorSeatId,
					event.kind,
					JSON.stringify(event.data),
					event.requestHash,
					now,
					...stillThere
				]
			},
			{
				// The count is incremented in SQL rather than read and written back,
				// so it cannot be wrong about a command it did not see.
				sql: `UPDATE card_tables
					SET state = ?, state_version = ?, version = ?,
						command_count = command_count + 1, last_active_at = ?
					WHERE id = ? AND version = ? AND last_active_at >= ? AND command_count < ?`,
				params: [
					JSON.stringify(outcome.state),
					outcome.stateVersion,
					event.version,
					now,
					...stillThere
				]
			}
		]);
	} catch (cause) {
		if (!isUniqueViolation(cause)) throw cause;
		// The index refused the event: this request has already landed, and
		// nothing of this attempt was written. The answer is that run's.
		return (await earlierRun(db, input, table)) ?? { ok: false, reason: 'conflict' };
	}

	if (changed[1] !== 1) {
		// The table was not where both writes needed it to be. If that is because
		// this same request got there first, from another copy of it, the caller
		// is owed that run's answer and not a refusal.
		return (
			(await earlierRun(db, input, table)) ?? {
				ok: false,
				reason: await whyNot(db, input.tableId, now)
			}
		);
	}

	return {
		ok: true,
		// The row as the write left it, put together from what is in hand. Asking
		// the database to send it back would mean parsing the whole state again.
		table: {
			...table,
			state: outcome.state,
			stateVersion: outcome.stateVersion,
			version: event.version,
			commandCount: table.commandCount + 1,
			lastActiveAt: new Date(now)
		},
		event,
		replayed: false
	};
}

/** The settled answer of a run of this request that landed before this one. */
async function earlierRun(
	db: Db,
	input: ApplyCommandInput,
	table: CardTableRow
): Promise<ApplyResult | undefined> {
	const theirs = await findByRequest(db, input.tableId, input.requestHash);
	return theirs ? replayOf(table, theirs) : undefined;
}

/**
 * Why a command's writes matched nothing.
 *
 * Three different conditions look the same from the write, and a caller has to
 * tell them apart. Losing a race is normal — the client re-syncs and carries
 * on, which is the "someone got there first" this design accepts as its only
 * ordinary refusal. A table that has expired or exhausted its commands is
 * *finished*, and a client unable to tell the difference would retry it
 * forever. So a failed write spends one read to say which it was, on the path
 * where being wrong is expensive and being slow costs nothing.
 */
async function whyNot(db: Db, tableId: string, now: number): Promise<SaveRefusal> {
	const current = await getCardTable(db, tableId);
	if (!current) return 'gone';
	if (isExpired(current, now)) return 'expired';
	if (current.commandCount >= MAX_COMMANDS_PER_TABLE) return 'exhausted';
	return 'conflict';
}

/**
 * The answer for a command somebody has already run.
 *
 * An event with no version is a claim that older code left unfinished (see
 * `UNWRITTEN`): there is no settled answer to give, and the honest reply is
 * that this caller should re-sync.
 */
function replayOf(table: CardTableRow, event: CardTableEvent): ApplyResult {
	if (event.version === UNWRITTEN) return { ok: false, reason: 'conflict' };
	return { ok: true, table, event, replayed: true };
}

/** The event a given request already produced, if it has run before. */
export async function findByRequest(
	db: Db,
	tableId: string,
	requestHash: string
): Promise<CardTableEvent | undefined> {
	const [row] = await db
		.select()
		.from(cardTableEvents)
		.where(and(eq(cardTableEvents.tableId, tableId), eq(cardTableEvents.requestHash, requestHash)))
		.limit(1);
	return row;
}

/**
 * Events after a client's cursor, oldest first.
 *
 * Unfinished claims are excluded (see `UNWRITTEN`): an event with no version
 * is a command that never landed, and it has no place in a client's history.
 *
 * Capped, because a client returning to a long-running table should be given
 * the current state and the recent past rather than an afternoon of history it
 * cannot use.
 *
 * **Why the log is never trimmed, though retention trims everything else.**
 * The request hash lives on the event row and is the idempotency lock, so
 * deleting old events would delete the record that a command had already run.
 * A retry arriving after the trim would apply a second time — two cards moved
 * for one click, which is the exact failure the lock exists to prevent. The
 * storage it would save is bounded anyway: a table stops accepting commands at
 * the ceiling, and the whole log goes when the table does. Growth within one
 * table's six weeks is the price of the guarantee, and it is worth it.
 */
export async function eventsSince(
	db: Db,
	tableId: string,
	sinceVersion: number,
	limit = 50
): Promise<CardTableEvent[]> {
	return db
		.select()
		.from(cardTableEvents)
		.where(
			and(
				eq(cardTableEvents.tableId, tableId),
				gt(cardTableEvents.version, Math.max(sinceVersion, UNWRITTEN))
			)
		)
		.orderBy(asc(cardTableEvents.version))
		.limit(limit);
}
