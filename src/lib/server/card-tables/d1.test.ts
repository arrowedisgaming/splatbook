/**
 * The card table against a real D1, counting trips to the database.
 *
 * Every other test here runs on better-sqlite3, which has no `batch` and no
 * round trips, so two things can only be seen on D1 itself: that the batched
 * paths work at all there (drizzle prepares a batch differently from a lone
 * query, and a mistake fails on D1 alone), and how many requests a command
 * actually makes. Issue #2 is about the second: each trip costs a Worker CPU,
 * and a command has 10 ms. The counts below are a budget. A change that adds
 * a trip to a command should have to come here and say so.
 *
 * Runs workerd through Miniflare, as `tools/profile-commands.ts` does. Set
 * `NO_WORKERD=1` to skip it where that binary cannot run.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { Miniflare } from 'miniflare';
import { drizzle } from 'drizzle-orm/d1';
import type { D1Database, D1PreparedStatement } from '@cloudflare/workers-types';
import * as schema from '../db/schema';
import type { Db } from '../db/entities';
import { createCardTable } from '../db/card-tables';
import { claimGmSeat, requestSeat } from '../db/card-table-seats';
import { clearPackCache, loadPack, loadTable, runCommand, syncTable, viewFor } from './service';
import { seededRng } from '$lib/games/hmtw/engine';
import { getGame } from '$lib/games';
import '$lib/games';

/** Serve the real pack from disk. */
const packFetch = (async (url: string) =>
	new Response(await readFile(`static${url}`, 'utf8'), {
		headers: { 'content-type': 'application/json' }
	})) as unknown as typeof fetch;

/**
 * A D1 binding that notes every request made of it: one entry per trip,
 * holding how many statements that trip carried.
 */
function counted(d1: D1Database): { d1: D1Database; trips: number[] } {
	const trips: number[] = [];
	const real = new WeakMap<object, D1PreparedStatement>();
	const SENDS = new Set(['all', 'run', 'raw', 'first']);

	const watch = (stmt: D1PreparedStatement): D1PreparedStatement => {
		const proxy = new Proxy(stmt, {
			get(target, key) {
				const value = Reflect.get(target, key) as unknown;
				if (typeof value !== 'function') return value;
				if (key === 'bind') return (...args: unknown[]) => watch(target.bind(...args));
				return (...args: unknown[]) => {
					if (SENDS.has(String(key))) trips.push(1);
					return (value as (...a: unknown[]) => unknown).apply(target, args);
				};
			}
		});
		real.set(proxy, stmt);
		return proxy;
	};

	const binding = new Proxy(d1, {
		get(target, key) {
			if (key === 'prepare') return (sql: string) => watch(target.prepare(sql));
			if (key === 'batch') {
				return (stmts: D1PreparedStatement[]) => {
					trips.push(stmts.length);
					return target.batch(stmts.map((s) => real.get(s) ?? s));
				};
			}
			const value = Reflect.get(target, key) as unknown;
			return typeof value === 'function' ? value.bind(target) : value;
		}
	});
	return { d1: binding, trips };
}

const MOVE = { type: 'move', from: { zone: 'deck:player' }, to: 'discard:player' };

describe.skipIf(process.env.NO_WORKERD === '1')('the card table on D1', () => {
	let mf: Miniflare;
	let db: Db;
	let trips: number[];
	let token: string;
	let gmClaim: { seatId: string; secret: string };

	beforeAll(async () => {
		mf = new Miniflare({
			modules: true,
			script: 'export default { fetch() { return new Response(null); } }',
			d1Databases: { DB: 'card-table-budget' }
		});
		const raw = (await mf.getD1Database('DB')) as unknown as D1Database;
		const journal = JSON.parse(readFileSync('drizzle/meta/_journal.json', 'utf8')) as {
			entries: { tag: string }[];
		};
		for (const { tag } of journal.entries) {
			const sql = readFileSync(`drizzle/${tag}.sql`, 'utf8');
			for (const statement of sql.split('--> statement-breakpoint')) {
				if (statement.trim()) await raw.prepare(statement).run();
			}
		}
		const watched = counted(raw);
		trips = watched.trips;
		db = drizzle(watched.d1, { schema }) as unknown as Db;
	}, 30_000);

	afterAll(async () => {
		await mf?.dispose();
	});

	beforeEach(async () => {
		clearPackCache();
		const email = `gm-${crypto.randomUUID()}@x`;
		const [user] = await db.insert(schema.users).values({ email }).returning();
		const module = getGame('hmtw')!.cardTable!;
		const opening = module.create(
			await loadPack(packFetch, 'hmtw', module.packFiles),
			seededRng(3)
		);
		const created = await createCardTable(db, {
			gameId: 'hmtw',
			name: 'Thursday',
			ownerId: user.id,
			state: opening.state,
			stateVersion: opening.stateVersion
		});
		if (!created.ok) throw new Error('create refused');
		token = created.table.roomToken;
		const seat = await requestSeat(db, created.table.id, 'The GM');
		if (!seat.ok) throw new Error('seat refused');
		gmClaim = { seatId: seat.ticket.seat.id, secret: seat.ticket.secret };
		await claimGmSeat(db, created.table.id, seat.ticket.seat.id);
		trips.length = 0;
	});

	/** What the commands route does, from the request to the reply. */
	async function command(expectedVersion: number, requestHash: string) {
		const ctx = (await loadTable(db, packFetch, token, gmClaim, undefined, requestHash))!;
		const result = await runCommand(db, ctx, MOVE, expectedVersion, requestHash, seededRng(1));
		if (!result.ok) return { result, view: undefined };
		const view = await viewFor(
			db,
			{ ...ctx, row: result.table, state: result.state ?? ctx.state },
			expectedVersion,
			result.replayed ? undefined : result.event
		);
		return { result, view };
	}

	it('loads a table and its seats in one trip', async () => {
		const ctx = await loadTable(db, packFetch, token, gmClaim, undefined);
		expect(ctx?.seat?.id).toBe(gmClaim.seatId);
		expect(ctx?.seats).toHaveLength(1);
		expect(trips).toEqual([2]);
	});

	it('runs a command in four trips: the reads together, then the three writes', async () => {
		const { result, view } = await command(0, 'request-1');
		expect(result.ok).toBe(true);
		expect(view?.version).toBe(1);
		expect(view?.events).toEqual([{ version: 1, kind: 'move', data: expect.anything() }]);
		expect(trips).toEqual([3, 1, 1, 1]);

		// And it is what a poll then finds.
		const polled = await syncTable(db, packFetch, token, gmClaim, undefined, { since: 0 });
		expect(polled).toMatchObject({ version: 1, events: view?.events });
	});

	it('answers a retried command from the first trip, plus its history', async () => {
		await command(0, 'request-1');
		trips.length = 0;
		const { result, view } = await command(0, 'request-1');
		expect(result).toMatchObject({ ok: true, replayed: true });
		expect(view?.version).toBe(1);
		expect(trips).toEqual([3, 1]);
	});

	it('turns away a command built on a version somebody has moved past', async () => {
		await command(0, 'request-1');
		const { result } = await command(0, 'request-2');
		expect(result).toMatchObject({ ok: false, reason: 'conflict' });
		const polled = await syncTable(db, packFetch, token, gmClaim, undefined, { since: 0 });
		expect(polled).toMatchObject({ version: 1 });
	});
});
