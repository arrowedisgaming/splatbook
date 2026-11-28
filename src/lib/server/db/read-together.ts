/**
 * Several independent reads in one trip to the database.
 *
 * On D1 every statement is its own request to the database, and each one costs
 * a Worker CPU as well as time: the client serialises the call and parses the
 * reply (issue #2 measured about a fifth of a millisecond a query in workerd,
 * three times that on Cloudflare, against 10 ms for the whole request). D1's
 * `batch` sends a list of statements as one request and runs them in order
 * inside one transaction, so reads that do not need each other's answers can
 * share the trip, and they see one consistent snapshot while they are at it.
 *
 * better-sqlite3 has no `batch` and no trip to save: its queries run in place.
 * So the driver is asked what it can do and the same call works on both. That
 * is the arrangement Arrowed's guild-book settled on (`readIndependently`),
 * where it took a command from twenty round trips towards nine.
 *
 * **Only queries that do not depend on each other.** The whole list is sent
 * together, so one whose input is another's result cannot be in it. Say it in
 * SQL instead, with a subquery.
 *
 * **Query builders, not promises.** The D1 branch hands the list to drizzle's
 * `batch`, which prepares each item itself. A builder is also a promise as far
 * as the type system can tell, so a signature typed on promises would accept
 * `readTogether(db, [getCardTable(db, id)])` and fail only at run time, only
 * on D1, where no better-sqlite3 test would ever see it. `BatchItem` is
 * drizzle's own constraint, and it makes that mistake a type error.
 *
 * Server-only.
 */

import type { BatchItem } from 'drizzle-orm/batch';
import type { Db } from './entities.ts';

type Answers<T extends readonly BatchItem<'sqlite'>[]> = {
	-readonly [K in keyof T]: Awaited<T[K]>;
};

export async function readTogether<T extends readonly BatchItem<'sqlite'>[]>(
	db: Db,
	queries: [...T]
): Promise<Answers<T>> {
	const batchable = db as unknown as {
		batch?: (queries: readonly unknown[]) => Promise<unknown[]>;
	};
	if (typeof batchable.batch === 'function' && queries.length > 0) {
		return (await batchable.batch(queries)) as Answers<T>;
	}
	return (await Promise.all(queries)) as Answers<T>;
}
