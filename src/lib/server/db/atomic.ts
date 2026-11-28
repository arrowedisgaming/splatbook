/**
 * A few writes that land together or not at all, on either database.
 *
 * Most of this codebase gets by without transactions, so the same code runs on
 * better-sqlite3 and on D1, whose drivers offer them differently. A card-table
 * command is the one place that pays for that: three writes, each its own trip
 * to D1 and each built and mapped by drizzle, with a claim row to hold the
 * door between them (issue #2). This is the alternative, and it is the design
 * Arrowed's guild-book already runs its own card table on (`runAtomic`).
 *
 * Both drivers can run a fixed list of statements as one unit, in order, with
 * each statement seeing the writes before it:
 *
 * - D1: `batch`, which Cloudflare documents as a transaction. One request.
 * - better-sqlite3: `transaction`, synchronous, around the same statements.
 *
 * A statement that fails (a unique index refusing a row) undoes the whole
 * list on both. A statement that merely matches nothing does not: guard each
 * one with the same condition in its WHERE clause and read the counts back.
 *
 * The statements are plain SQL with bound parameters, sent to the driver
 * underneath drizzle. That is deliberate. drizzle's query builder is the
 * largest single cost in a command on Workers, and a write whose result is
 * only "did it land" has no rows for it to map.
 *
 * **Every `sql` is a string literal written in this codebase.** Nothing a
 * request supplied is ever joined into one; request values travel in `params`
 * and are bound.
 *
 * Server-only.
 */

import type { Db } from './entities.ts';

/** A value SQLite can bind on both drivers: no booleans, no dates, no objects. */
export type Bindable = string | number | null;

export interface AtomicStatement {
	sql: string;
	params: readonly Bindable[];
}

interface D1Client {
	prepare(sql: string): { bind(...params: Bindable[]): unknown };
	batch(statements: unknown[]): Promise<{ meta: { changes: number } }[]>;
}

interface SqliteClient {
	prepare(sql: string): { run(...params: Bindable[]): { changes: number } };
	transaction<T>(fn: () => T): () => T;
}

/**
 * Run `statements` as one all-or-nothing unit.
 *
 * Resolves to how many rows each statement changed, in order. Rejects, with
 * nothing written, if any statement fails.
 */
export async function runAtomic(db: Db, statements: readonly AtomicStatement[]): Promise<number[]> {
	if (statements.length === 0) return [];
	// drizzle keeps the driver it was built on here, on both backends.
	const client = (db as unknown as { $client: D1Client | SqliteClient }).$client;

	if ('batch' in client) {
		const results = await client.batch(
			statements.map((s) => client.prepare(s.sql).bind(...s.params))
		);
		return results.map((r) => r.meta.changes);
	}

	return client.transaction(() =>
		statements.map((s) => client.prepare(s.sql).run(...s.params).changes)
	)();
}

/**
 * Whether an error is a unique index refusing a row.
 *
 * By message, because that is the only thing the two drivers agree on: both
 * carry SQLite's own "UNIQUE constraint failed".
 */
export function isUniqueViolation(error: unknown): boolean {
	const message = error instanceof Error ? error.message : String(error);
	return /unique constraint failed/i.test(message);
}
