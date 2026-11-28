/**
 * Command service tests. The points of interest: a retried command lands once;
 * losing a race changes nothing at all; the event log carries the version a
 * client already has as its cursor; and nothing private reaches the log.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { migrate } from 'drizzle-orm/better-sqlite3/migrator';
import * as schema from './schema.ts';
import type { Db } from './entities.ts';
import { eq } from 'drizzle-orm';
import {
	createCardTable,
	getCardTable,
	TABLE_RETENTION_MS,
	touchCardTable
} from './card-tables.ts';
import { MAX_COMMANDS_PER_TABLE } from '../../card-table-limits.ts';
import {
	applyCommand,
	eventsSince,
	findByRequest,
	type CommandOutcome
} from './card-table-commands.ts';

function freshDb(): Db {
	const sqlite = new Database(':memory:');
	sqlite.pragma('foreign_keys = ON');
	const db = drizzle(sqlite, { schema });
	migrate(db, { migrationsFolder: './drizzle' });
	return db;
}

let db: Db;
let tableId: string;

beforeEach(async () => {
	db = freshDb();
	const [user] = await db.insert(schema.users).values({ email: 'gm@x' }).returning();
	const created = await createCardTable(db, {
		gameId: 'hmtw',
		name: 'Thursday',
		ownerId: user.id,
		state: { moves: 0 },
		stateVersion: 5
	});
	if (!created.ok) throw new Error('create refused');
	tableId = created.table.id;
});

/** A stand-in reducer: counts moves, and says something public about it. */
const bump = (state: unknown) => ({
	ok: true as const,
	state: { moves: ((state as { moves: number }).moves ?? 0) + 1 },
	stateVersion: 5,
	kind: 'move',
	data: { seat: 'somebody' }
});

const run = (
	over: Partial<Parameters<typeof applyCommand>[1]> = {},
	reduce: (state: unknown) => CommandOutcome = bump
) =>
	applyCommand(
		db,
		{ tableId, actorSeatId: null, expectedVersion: 0, requestHash: 'r1', ...over },
		reduce
	);

describe('applying a command', () => {
	it('advances the state and logs a public fact', async () => {
		const res = await run();
		expect(res.ok).toBe(true);
		if (!res.ok) return;
		expect(res.table.state).toEqual({ moves: 1 });
		expect(res.table.version).toBe(1);
		expect(res.event.kind).toBe('move');
		expect(res.event.version).toBe(1);
		expect(res.replayed).toBe(false);
	});

	it('numbers events with the version a client already holds', async () => {
		// One counter, not two: a client's cursor is its version.
		await run({ requestHash: 'a' });
		await run({ expectedVersion: 1, requestHash: 'b' });
		const events = await eventsSince(db, tableId, 0);
		expect(events.map((e) => e.version)).toEqual([1, 2]);
		expect(await eventsSince(db, tableId, 1)).toHaveLength(1);
	});

	it('lands a retried command once, and gives back the first answer', async () => {
		// A dropped response must not cost a player the card twice.
		const first = await run();
		const retry = await run();
		expect(retry.ok).toBe(true);
		if (!first.ok || !retry.ok) return;
		expect(retry.replayed).toBe(true);
		expect(retry.event.id).toBe(first.event.id);

		const table = await getCardTable(db, tableId);
		expect(table?.state).toEqual({ moves: 1 });
		expect(table?.version).toBe(1);
		expect(await eventsSince(db, tableId, 0)).toHaveLength(1);
	});

	it('changes nothing when somebody else got there first', async () => {
		await run({ requestHash: 'a' });
		const loser = await run({ requestHash: 'b', expectedVersion: 0 });
		expect(loser).toMatchObject({ ok: false, reason: 'conflict' });

		// The reducer is pure, so its result is simply discarded.
		const table = await getCardTable(db, tableId);
		expect(table?.state).toEqual({ moves: 1 });
		expect(await eventsSince(db, tableId, 0)).toHaveLength(1);
	});

	it('loses the same way when the row its caller loaded has gone stale', async () => {
		// The caller read the table, and then somebody else's command landed.
		const loaded = (await getCardTable(db, tableId))!;
		await run({ requestHash: 'a' });

		const loser = await run({ requestHash: 'b', expectedVersion: 0, loaded: { table: loaded } });
		expect(loser).toMatchObject({ ok: false, reason: 'conflict' });
		expect((await getCardTable(db, tableId))?.state).toEqual({ moves: 1 });
		expect(await eventsSince(db, tableId, 0)).toHaveLength(1);
		// And nothing of it was written, so the client's retry is not locked out.
		expect(await findByRequest(db, tableId, 'b')).toBeUndefined();
	});

	it('reads the stored row when the one it was handed is at another version', async () => {
		const loaded = (await getCardTable(db, tableId))!;
		await run({ requestHash: 'a' });

		// The client has caught up to version 1; the caller's row has not.
		const second = await run({ requestHash: 'b', expectedVersion: 1, loaded: { table: loaded } });
		expect(second.ok).toBe(true);
		expect((await getCardTable(db, tableId))?.state).toEqual({ moves: 2 });
	});

	it('replays from a lookup its caller already made', async () => {
		const first = await run({ requestHash: 'a' });
		if (!first.ok) throw new Error(first.reason);
		const table = (await getCardTable(db, tableId))!;

		// The retry arrives still believing in version 0. The event is a settled
		// answer whatever the versions say, and nothing is applied a second time.
		const again = await run({
			requestHash: 'a',
			expectedVersion: 0,
			loaded: { table, prior: first.event }
		});
		expect(again).toMatchObject({ ok: true, replayed: true });
		expect((await getCardTable(db, tableId))?.state).toEqual({ moves: 1 });
	});

	it('still refuses a duplicate that landed after its caller looked', async () => {
		// The caller looked, found nothing, and was told so. Then the same request
		// landed from elsewhere. The lookup was never the lock.
		const table = (await getCardTable(db, tableId))!;
		await run({ requestHash: 'a' });

		const late = await run({
			requestHash: 'a',
			expectedVersion: 0,
			loaded: { table, prior: null }
		});
		expect(late).toMatchObject({ ok: true, replayed: true });
		expect((await getCardTable(db, tableId))?.state).toEqual({ moves: 1 });
		expect(await eventsSince(db, tableId, 0)).toHaveLength(1);
	});

	it('is stopped by the index when the lookup and the version both miss it', async () => {
		// The request has landed, the caller's row is the one it left behind, and
		// the caller was still told the request was new. Only the index is left.
		await run({ requestHash: 'a' });
		const table = (await getCardTable(db, tableId))!;

		const late = await run({
			requestHash: 'a',
			expectedVersion: 1,
			loaded: { table, prior: null }
		});
		expect(late).toMatchObject({ ok: true, replayed: true });
		// The refused event took the state with it: nothing moved.
		expect(await getCardTable(db, tableId)).toEqual(table);
		expect(await eventsSince(db, tableId, 0)).toHaveLength(1);
	});

	it('passes a refusal through without writing', async () => {
		const res = await run({}, () => ({ ok: false, reason: 'no-such-zone' }));
		expect(res).toEqual({ ok: false, reason: 'rejected', detail: 'no-such-zone' });
		expect((await getCardTable(db, tableId))?.version).toBe(0);
		expect(await eventsSince(db, tableId, 0)).toEqual([]);
	});

	it('says so when the table is not there', async () => {
		const res = await applyCommand(
			db,
			{ tableId: 'nope', actorSeatId: null, expectedVersion: 0, requestHash: 'r' },
			bump
		);
		expect(res).toEqual({ ok: false, reason: 'no-such-table' });
	});

	it('keeps request keys to their own table', async () => {
		const [user] = await db.insert(schema.users).values({ email: 'other@x' }).returning();
		const other = await createCardTable(db, {
			gameId: 'hmtw',
			name: 'Other',
			ownerId: user.id,
			state: { moves: 0 },
			stateVersion: 5
		});
		if (!other.ok) throw new Error('create refused');

		await run({ requestHash: 'shared' });
		const elsewhere = await applyCommand(
			db,
			{
				tableId: other.table.id,
				actorSeatId: null,
				expectedVersion: 0,
				requestHash: 'shared'
			},
			bump
		);
		// The same key at a different table is a different command.
		expect(elsewhere.ok).toBe(true);
		if (elsewhere.ok) expect(elsewhere.replayed).toBe(false);
	});
});

describe('what a write insists on', () => {
	it('bumps the version, counts the command, and hands back the row as it now is', async () => {
		const res = await run();
		if (!res.ok) throw new Error(res.reason);
		expect(res.table.version).toBe(1);
		expect(res.table.commandCount).toBe(1);
		expect(res.table.state).toEqual({ moves: 1 });
		// Nothing is read back after the write, so the row and the event handed
		// to the caller are put together from what was in hand. They have to be
		// what the database holds.
		expect(res.table).toEqual(await getCardTable(db, tableId));
		expect(res.event).toEqual(await findByRequest(db, tableId, 'r1'));
	});

	it('counts each command once, however many land', async () => {
		for (let i = 0; i < 5; i++) {
			const res = await run({ expectedVersion: i, requestHash: `r${i}` });
			if (!res.ok) throw new Error(res.reason);
		}
		const current = await getCardTable(db, tableId);
		expect(current?.version).toBe(5);
		expect(current?.commandCount).toBe(5);
		expect((await eventsSince(db, tableId, 0)).map((e) => e.version)).toEqual([1, 2, 3, 4, 5]);
	});

	it('keeps a table alive by being used', async () => {
		await touchCardTable(db, tableId, Date.now() - 60_000);
		const before = (await getCardTable(db, tableId))!.lastActiveAt.getTime();
		await run();
		expect((await getCardTable(db, tableId))!.lastActiveAt.getTime()).toBeGreaterThan(before);
	});

	it('cannot write an expired table back to life', async () => {
		await touchCardTable(db, tableId, Date.now() - TABLE_RETENTION_MS - 1000);
		// Every read path already treats this table as gone; a client still
		// holding its id and version must not be able to resurrect it.
		expect(await run()).toEqual({ ok: false, reason: 'expired' });
		expect((await getCardTable(db, tableId))?.version).toBe(0);
		expect(await findByRequest(db, tableId, 'r1')).toBeUndefined();
	});

	it('stops accepting commands at the ceiling', async () => {
		// Reach the ceiling directly; playing there would take a hundred thousand
		// card moves, which is the point of the number.
		await db
			.update(schema.cardTables)
			.set({ commandCount: MAX_COMMANDS_PER_TABLE })
			.where(eq(schema.cardTables.id, tableId));
		// Distinguishable from a race, because a client that could not tell would
		// retry a finished table forever.
		expect(await run()).toEqual({ ok: false, reason: 'exhausted' });
		expect(await findByRequest(db, tableId, 'r1')).toBeUndefined();
	});

	it('says a table deleted under it is gone, not merely contended', async () => {
		const table = (await getCardTable(db, tableId))!;
		await db.delete(schema.cardTables).where(eq(schema.cardTables.id, tableId));
		expect(await run({ loaded: { table, prior: null } })).toEqual({ ok: false, reason: 'gone' });
	});

	it('lets a failure that is not a lost race be a failure', async () => {
		// A seat that does not exist breaks the event's foreign key. That is not
		// "somebody got there first", and must not be reported as if it were.
		await expect(run({ actorSeatId: 'no-such-seat' })).rejects.toThrow(/foreign key/i);
		expect((await getCardTable(db, tableId))?.version).toBe(0);
	});
});

describe('the request hash is a lock, not a lookup', () => {
	it('will not apply a command twice when a retry arrives with a fresh version', async () => {
		// The window a check-then-apply would leave: a naive retry-on-conflict
		// loop re-reads the version and asks again with the same command. Two
		// cards moved for one click is the failure this prevents.
		const first = await run({ requestHash: 'same' });
		expect(first.ok).toBe(true);

		const retry = await run({ expectedVersion: 1, requestHash: 'same' });
		expect(retry.ok).toBe(true);
		if (!retry.ok) return;
		expect(retry.replayed).toBe(true);

		const table = await getCardTable(db, tableId);
		expect(table?.state).toEqual({ moves: 1 });
		expect(table?.version).toBe(1);
	});

	it('leaves nothing behind when the write loses its race', async () => {
		// Otherwise a client retrying after a conflict would meet its own
		// abandoned attempt and believe the command had landed.
		await run({ requestHash: 'winner' });
		const loser = await run({ expectedVersion: 0, requestHash: 'loser' });
		expect(loser).toMatchObject({ ok: false, reason: 'conflict' });

		// No orphan left behind, so the same command can be tried again properly.
		expect(await findByRequest(db, tableId, 'loser')).toBeUndefined();
		const retried = await run({ expectedVersion: 1, requestHash: 'loser' });
		expect(retried.ok).toBe(true);
	});

	it('leaves no claim behind when the reducer refuses', async () => {
		await run({ requestHash: 'refused' }, () => ({ ok: false, reason: 'no-such-zone' }));
		expect(await findByRequest(db, tableId, 'refused')).toBeUndefined();
	});

	it('keeps a claim in flight out of a client’s history', async () => {
		// A claimed event has no version yet; it is not part of anyone's past
		// until it lands.
		await db.insert(schema.cardTableEvents).values({
			tableId,
			version: 0,
			kind: 'in-flight',
			data: {},
			requestHash: 'pending'
		});
		expect(await eventsSince(db, tableId, 0)).toEqual([]);
	});
});

describe('the event log is public by contract', () => {
	it('records only what the game chose to say out loud', async () => {
		// The reducer decides what goes in `data`. The point of the contract is
		// that the shell stores it verbatim and shows it to everyone, so a game
		// putting a card id here would be leaking it to the whole table.
		const res = await run({}, () => ({
			ok: true,
			state: { moves: 1, secret: 'ace-of-cups' },
			stateVersion: 5,
			kind: 'deal',
			data: { seat: 's3', count: 4 }
		}));
		expect(res.ok).toBe(true);
		if (!res.ok) return;

		// The private half lives in the state, which reaches a client only
		// through the game's per-seat projection.
		expect(JSON.stringify(res.event.data)).not.toContain('ace-of-cups');
		expect(res.event.data).toEqual({ seat: 's3', count: 4 });
	});

	it('finds an earlier run of the same request', async () => {
		await run({ requestHash: 'once' });
		expect(await findByRequest(db, tableId, 'once')).toBeDefined();
		expect(await findByRequest(db, tableId, 'never')).toBeUndefined();
	});

	it('gives a returning client the recent past rather than an afternoon of it', async () => {
		let version = 0;
		for (let i = 0; i < 12; i++) {
			const res = await run({ expectedVersion: version, requestHash: `r${i}` });
			if (res.ok) version = res.table.version;
		}
		expect(await eventsSince(db, tableId, 0, 5)).toHaveLength(5);
		expect(await eventsSince(db, tableId, 10)).toHaveLength(2);
	});

	it('goes when its table goes', async () => {
		await run();
		await db.delete(schema.cardTables);
		expect(await eventsSince(db, tableId, 0)).toEqual([]);
	});
});
