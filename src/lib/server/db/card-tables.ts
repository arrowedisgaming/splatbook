/**
 * Card-table persistence (phase 29) — create, find, and tidy the shared
 * surfaces a game contributes.
 *
 * Shaped like the campaign service and for the same reasons: it takes the
 * drizzle `db` first so tests can drive it against an in-memory database, and
 * access control lives here in the WHERE clauses rather than in the routes.
 *
 * Two things differ from campaigns, both deliberate. A table is found by a
 * **token in its URL** rather than by membership, because the people who need
 * to reach it may have no account. And creating one **requires an account**,
 * which is the whole of the abuse story: it removes the only unbounded
 * anonymous write endpoint, and everything left is bounded by the seats a table
 * can hold.
 *
 * `state` is the game's own blob and is never inspected here — the same
 * treatment `entities.data` gets.
 *
 * Server-only.
 */

import { and, asc, count, desc, eq, gte, inArray, lt } from 'drizzle-orm';
import type { Db } from './entities.ts';
import {
	cardTableEvents,
	cardTables,
	cardTableSeats,
	type CardTableEvent,
	type CardTableRow,
	type CardTableSeat
} from './schema.ts';
import { readTogether } from './read-together.ts';
import { MAX_TABLES_PER_OWNER } from '../../card-table-limits.ts';

/** Six weeks, in milliseconds — the retention window, matching Crawlspace. */
export const TABLE_RETENTION_MS = 42 * 24 * 60 * 60 * 1000;

export interface NewCardTableInput {
	gameId: string;
	name: string;
	/** The creator. Sign-in is required precisely here and nowhere else. */
	ownerId: string;
	/** The game's opening table state, opaque to this layer. */
	state: unknown;
	/** The game's own schema version for that blob. */
	stateVersion: number;
}

/** Why a table could not be created. */
export type CreateRefusal = 'too-many-tables';
export type CreateResult = { ok: true; table: CardTableRow } | { ok: false; reason: CreateRefusal };

/**
 * How many live tables an account has. Expired ones do not count — the cap is
 * on tables in play, not on a lifetime of them.
 *
 * A note covering every cap in this file and its neighbour: they are
 * check-then-insert rather than atomic, because this codebase deliberately
 * avoids explicit transactions so the same code runs on better-sqlite3 and on
 * D1. Two requests arriving in the same instant can both pass a cap and
 * overshoot it by one or two. That is accepted — the caps exist to stop
 * *unbounded* growth, and since every request still pays for a count, the
 * overshoot is bounded by how much concurrency someone can muster rather than
 * by how long they are willing to keep asking.
 */
export async function countLiveTables(
	db: Db,
	ownerId: string,
	now: number = Date.now()
): Promise<number> {
	const [row] = await db
		.select({ n: count() })
		.from(cardTables)
		.where(
			and(
				eq(cardTables.ownerId, ownerId),
				gte(cardTables.lastActiveAt, new Date(now - TABLE_RETENTION_MS))
			)
		);
	return row?.n ?? 0;
}

/**
 * Create a table. Its room token is generated for it and goes in the URL.
 *
 * Capped per account. Sign-in makes creation attributable rather than free, but
 * an account is itself cheap to get, so attribution alone is not a limit.
 */
export async function createCardTable(db: Db, input: NewCardTableInput): Promise<CreateResult> {
	if ((await countLiveTables(db, input.ownerId)) >= MAX_TABLES_PER_OWNER) {
		return { ok: false, reason: 'too-many-tables' };
	}
	const [row] = await db
		.insert(cardTables)
		.values({
			gameId: input.gameId,
			name: input.name,
			ownerId: input.ownerId,
			state: input.state,
			stateVersion: input.stateVersion
		})
		.returning();
	return { ok: true, table: row };
}

/** Whether a table has fallen outside the retention window. */
export const isExpired = (row: CardTableRow, now: number = Date.now()): boolean =>
	now - row.lastActiveAt.getTime() > TABLE_RETENTION_MS;

/** Find a table by id. */
export async function getCardTable(db: Db, id: string): Promise<CardTableRow | undefined> {
	const [row] = await db.select().from(cardTables).where(eq(cardTables.id, id)).limit(1);
	return row;
}

/**
 * Find a table by the token in its URL — the lookup that works for someone with
 * no account, which is most of the people who will use one.
 *
 * An expired table reads as **absent**, which is what keeps the retention
 * promise, and this function does **not** delete it. That separation is
 * deliberate and load-bearing: the sync loop polls this path about once a second
 * per client, and a read that writes would turn a quiet table into a steady
 * stream of billed writes on D1 — the opposite of the read budget this phase
 * went to some trouble over. Removing the row is `sweepExpiredTables`'s job,
 * called from page loads and never from a poll.
 */
export async function getCardTableByToken(
	db: Db,
	token: string,
	now: number = Date.now()
): Promise<CardTableRow | undefined> {
	const [row] = await db.select().from(cardTables).where(eq(cardTables.roomToken, token)).limit(1);
	if (!row || isExpired(row, now)) return undefined;
	return row;
}

/** A table, everybody at it, and whether a given request has run before. */
export interface TableWithSeats {
	row: CardTableRow;
	/** Oldest first, the same order as `listSeats`. */
	seats: CardTableSeat[];
	/**
	 * The event a request hash already produced at this table. `null` when the
	 * hash was looked for and is not there; `undefined` when nobody asked.
	 */
	prior: CardTableEvent | null | undefined;
}

/**
 * The table a token names, with its seats, in one trip to the database.
 *
 * Everything that acts on a table needs both: the row for its state, the seats
 * to know who is asking. They were two queries, the second waiting on the id
 * the first returned. Here the seats are found by the token as well, through a
 * subquery, so neither needs the other's answer and `readTogether` can send
 * them as one request on D1 (issue #2).
 *
 * A command also has to know whether its request hash has been seen, which is
 * a third read with the same shape, so passing a `requestHash` adds it to the
 * same trip. It is a lookup and not the lock: the unique index is still what
 * stops a command landing twice (see `applyCommand`).
 *
 * Not a join, unlike `peekTableByToken`, because this one does carry the
 * state, and a join would repeat that blob on every seat's row.
 *
 * Read-only, and an expired table reads as absent, like `getCardTableByToken`.
 */
export async function getTableWithSeatsByToken(
	db: Db,
	token: string,
	requestHash?: string,
	now: number = Date.now()
): Promise<TableWithSeats | undefined> {
	const theTable = db
		.select({ id: cardTables.id })
		.from(cardTables)
		.where(eq(cardTables.roomToken, token));
	const table = db.select().from(cardTables).where(eq(cardTables.roomToken, token)).limit(1);
	const seated = db
		.select()
		.from(cardTableSeats)
		.where(inArray(cardTableSeats.tableId, theTable))
		.orderBy(asc(cardTableSeats.createdAt));

	if (requestHash === undefined) {
		const [[row], seats] = await readTogether(db, [table, seated]);
		if (!row || isExpired(row, now)) return undefined;
		return { row, seats, prior: undefined };
	}

	const seen = db
		.select()
		.from(cardTableEvents)
		.where(
			and(inArray(cardTableEvents.tableId, theTable), eq(cardTableEvents.requestHash, requestHash))
		)
		.limit(1);
	const [[row], seats, [prior]] = await readTogether(db, [table, seated, seen]);
	if (!row || isExpired(row, now)) return undefined;
	return { row, seats, prior: prior ?? null };
}

/** What a poll needs to decide whether anything has changed. */
export interface TablePeek {
	id: string;
	gameId: string;
	version: number;
	/** Everybody at the table, oldest first — the same order as `listSeats`. */
	seats: CardTableSeat[];
}

/**
 * The table a token names and the seats at it, **in one statement** and
 * without its state.
 *
 * This is the poll's first question and, most of the time, its only one. A
 * table sits unchanged between nearly every pair of polls, so the commonest
 * request by far needs to learn just two things: has the version moved, and
 * has the roster. Both are here, in a single round trip — which matters on D1,
 * where each statement is its own trip to a database that may be a region
 * away — and the state blob stays on disk, unread and unparsed, until
 * something has actually changed.
 *
 * A join rather than a batch so the same code runs on better-sqlite3 and D1.
 * The table's columns repeat on every seat row, which is why the state is not
 * among them.
 *
 * Read-only, like `getCardTableByToken`, and for the same reason. An expired
 * table reads as absent.
 */
export async function peekTableByToken(
	db: Db,
	token: string,
	now: number = Date.now()
): Promise<TablePeek | undefined> {
	const rows = await db
		.select({
			id: cardTables.id,
			gameId: cardTables.gameId,
			version: cardTables.version,
			lastActiveAt: cardTables.lastActiveAt,
			seat: cardTableSeats
		})
		.from(cardTables)
		.leftJoin(cardTableSeats, eq(cardTableSeats.tableId, cardTables.id))
		.where(eq(cardTables.roomToken, token))
		.orderBy(asc(cardTableSeats.createdAt));
	const first = rows[0];
	if (!first || now - first.lastActiveAt.getTime() > TABLE_RETENTION_MS) return undefined;
	return {
		id: first.id,
		gameId: first.gameId,
		version: first.version,
		// A table with nobody at it comes back as one row with no seat.
		seats: rows.flatMap((row) => (row.seat ? [row.seat] : []))
	};
}

/**
 * Retire the table a token names, if it has expired. Nothing otherwise.
 *
 * This is the "lazy expiry on read" half of retention, and it is deliberately
 * not part of `getCardTableByToken`: that function is on the sync path, which
 * every client asks about once a second, and it must never write. A *page load*
 * is not a poll — somebody arrived — so it can afford the delete, and doing it
 * there means the commonest way a stale table is discovered is also the way it
 * goes.
 *
 * A token that names nothing costs one select and writes nothing, so guessing
 * at tokens buys no writes. A token that names an expired table can spend that
 * delete once.
 *
 * Returns whether anything was retired.
 */
export async function expireByToken(
	db: Db,
	token: string,
	now: number = Date.now()
): Promise<boolean> {
	// One statement, with the expiry in the WHERE clause rather than in a read
	// before it: a live table is untouched without ever being fetched, and two
	// requests arriving together cannot both decide to delete the same row.
	const rows = await db
		.delete(cardTables)
		.where(
			and(
				eq(cardTables.roomToken, token),
				lt(cardTables.lastActiveAt, new Date(now - TABLE_RETENTION_MS))
			)
		)
		.returning({ id: cardTables.id });
	return rows.length > 0;
}

/**
 * The tables a user created, newest first — the live ones.
 *
 * Expired tables are left out, because every other read path already treats
 * them as absent and a list that disagreed would be the one place the retention
 * promise looked broken: a row with a working-looking link that answers 404.
 * The row may still be on disk; whether it has been swept yet is not something
 * a reader should be able to tell.
 */
export async function listCardTablesForOwner(
	db: Db,
	ownerId: string,
	now: number = Date.now()
): Promise<CardTableRow[]> {
	return db
		.select()
		.from(cardTables)
		.where(
			and(
				eq(cardTables.ownerId, ownerId),
				gte(cardTables.lastActiveAt, new Date(now - TABLE_RETENTION_MS))
			)
		)
		.orderBy(desc(cardTables.lastActiveAt));
}

/**
 * Mark a table as still in use, which is what keeps it alive. Called on any
 * read or write that a person caused — not on a poll, or a table would never
 * expire while a forgotten browser tab sat open on it.
 */
export async function touchCardTable(db: Db, id: string, now: number = Date.now()): Promise<void> {
	await db
		.update(cardTables)
		.set({ lastActiveAt: new Date(now) })
		.where(eq(cardTables.id, id));
}

/**
 * Replace the room token, revoking every outstanding link.
 *
 * Owner-guarded. Seats are not accounts, so this is the only way to shut a
 * table to someone who should no longer be at it.
 */
export async function rotateRoomToken(
	db: Db,
	id: string,
	ownerId: string
): Promise<CardTableRow | undefined> {
	const [row] = await db
		.update(cardTables)
		.set({ roomToken: crypto.randomUUID() })
		.where(and(eq(cardTables.id, id), eq(cardTables.ownerId, ownerId)))
		.returning();
	return row;
}

/**
 * Delete a table and everything at it.
 *
 * Owner-guarded, and the practical deletion route for a guest: they have no
 * account for an erasure request to name them by, so the answer to "take my
 * name off that table" is the GM removing the table, which is immediate and
 * needs no correspondence.
 */
export async function deleteCardTable(db: Db, id: string, ownerId: string): Promise<boolean> {
	const rows = await db
		.delete(cardTables)
		.where(and(eq(cardTables.id, id), eq(cardTables.ownerId, ownerId)))
		.returning();
	return rows.length > 0;
}

/**
 * Retire a bounded number of tables nobody has come back to.
 *
 * Expiry on access alone never reaches a table nobody revisits, which would
 * make "kept for six weeks" a claim the mechanism could not keep. So a page load
 * sweeps a few stale ones on its way past.
 *
 * **Page loads only — never a poll.** This deletes, and the sync loop asks for a
 * table roughly once a second per client; hanging writes off that path would
 * cost more than the whole feature is worth. Bounded for the same reason: it
 * rides along with a request somebody is already waiting on.
 */
export async function sweepExpiredTables(
	db: Db,
	limit = 5,
	now: number = Date.now()
): Promise<number> {
	const stale = await db
		.select({ id: cardTables.id })
		.from(cardTables)
		.where(lt(cardTables.lastActiveAt, new Date(now - TABLE_RETENTION_MS)))
		.orderBy(asc(cardTables.lastActiveAt))
		.limit(limit);
	if (stale.length === 0) return 0;
	// One statement for the batch. A delete per row was up to five extra round
	// trips on D1, charged to a request somebody is already waiting on, for
	// rows nobody is coming back to.
	await db.delete(cardTables).where(
		inArray(
			cardTables.id,
			stale.map((row) => row.id)
		)
	);
	return stale.length;
}

/** The seats at a table, oldest first — the order they sat down. */
export async function listSeats(db: Db, tableId: string): Promise<CardTableSeat[]> {
	return db
		.select()
		.from(cardTableSeats)
		.where(eq(cardTableSeats.tableId, tableId))
		.orderBy(asc(cardTableSeats.createdAt));
}

/**
 * Why a table's state could not be saved.
 *
 * The saving itself is `applyCommand`'s, in `card-table-commands.ts`, where the
 * state and the event that describes the change are written as one unit.
 */
export type SaveRefusal =
	/** Somebody else's command landed first. Re-sync and try again. */
	| 'conflict'
	/** No such table — it may have been deleted while this was in flight. */
	| 'gone'
	/** Past its retention window. Every read path already treats it as absent. */
	| 'expired'
	/** It has accepted all the commands it ever will. */
	| 'exhausted';
