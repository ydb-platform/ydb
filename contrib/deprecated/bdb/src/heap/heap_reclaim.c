/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1998, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/heap.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"

/*
 * __heap_truncate --
 *	Truncate a database.
 *
 * PUBLIC: int __heap_truncate __P((DBC *, u_int32_t *));
 */
int
__heap_truncate(dbc, countp)
	DBC *dbc;
	u_int32_t *countp;
{
	DB *dbp;
	DB_LOCK lock, meta_lock;
	DB_MPOOLFILE *mpf;
	DBT log_dbt;
	HEAPHDR *hdr;
	HEAPMETA *meta;
	HEAPPG *pg;
	db_pgno_t pgno;
	int i, ret, t_ret;
	u_int32_t count, next_region, region_size;

	LOCK_INIT(lock);
	dbp = dbc->dbp;
	mpf = dbp->mpf;
	count = 0;
	next_region = FIRST_HEAP_RPAGE;
	region_size = HEAP_REGION_SIZE(dbp);

	/* Traverse the entire database, starting with the metadata pg. */
	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
	    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &meta_lock)) != 0)
		return (ret);
	if ((ret = __memp_fget(mpf, &pgno,
	    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &meta)) != 0) {
		__TLPUT(dbc, lock);
		goto err;
	}

	for (;;) {
		pgno++;
		if ((ret = __db_lget(dbc,
		    LCK_COUPLE, pgno, DB_LOCK_WRITE, 0, &lock)) != 0)
			break;
		if ((ret = __memp_fget(mpf, &pgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &pg)) != 0) {
			if (ret == DB_PAGE_NOTFOUND)
				ret = 0;
			break;
		}
		if (DBC_LOGGING(dbc)) {
			memset(&log_dbt, 0, sizeof(DBT));
			log_dbt.data = pg;
			log_dbt.size = dbp->pgsize;
			if ((ret = __heap_trunc_page_log(dbp, dbc->txn,
			    &LSN(pg), 0, pgno,
			    &log_dbt, (pgno == next_region), &LSN(pg))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(pg));

		if (pgno == next_region) {
			DB_ASSERT(dbp->env, TYPE(pg) == P_IHEAP);
			next_region += region_size + 1;
		} else {
			/*
			 * We can't use pg->entries to calculate the record
			 * count, because it can include split records.  So we
			 * check the header for each entry and only count
			 * non-split records and the first piece of split
			 * records. But if the page is empty, there's no work to
			 * do.
			 */
			if (NUM_ENT(pg) != 0)
				for (i = 0; i <= HEAP_HIGHINDX(pg); i++) {
					if (HEAP_OFFSETTBL(dbp, pg)[i] == 0)
						continue;
					hdr = (HEAPHDR *)P_ENTRY(dbp, pg, i);
					if (!F_ISSET(hdr, HEAP_RECSPLIT) ||
					    F_ISSET(hdr, HEAP_RECFIRST))
						count++;
				}
		}
		if ((ret = __memp_fput(mpf,
		    dbc->thread_info, pg, dbc->priority)) != 0)
			break;
		if ((ret = __memp_fget(mpf, &pgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_FREE, &pg)) != 0)
			break;
	}
	if ((t_ret = __TLPUT(dbc, lock)) != 0 && ret == 0)
		ret = t_ret;

	if (countp != NULL && ret == 0)
		*countp = count;

	if (DBC_LOGGING(dbc)) {
		if ((ret = __heap_trunc_meta_log(dbp, dbc->txn, &LSN(meta), 0,
		    meta->dbmeta.pgno, meta->dbmeta.last_pgno,
		    meta->dbmeta.key_count, meta->dbmeta.record_count,
		    meta->curregion, meta->nregions, &LSN(meta))) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(meta));
	meta->dbmeta.key_count = 0;
	meta->dbmeta.record_count = 0;
	meta->dbmeta.last_pgno = PGNO_BASE_MD + 1;
	meta->curregion = 1;
	meta->nregions = 1;

	if ((ret = __memp_ftruncate(mpf, dbc->txn,
	    dbc->thread_info, PGNO_BASE_MD + 1, MP_TRUNC_NOCACHE)) != 0)
		goto err;

	/* Create the first region. */
	pgno = PGNO_BASE_MD + 1;
	if ((ret = __memp_fget(mpf, &pgno, dbc->thread_info,
	    dbc->txn, DB_MPOOL_CREATE | DB_MPOOL_DIRTY, &pg)) != 0)
		goto err;

	memset(pg, 0, dbp->pgsize);
	P_INIT(pg,
	    dbp->pgsize, 1, PGNO_INVALID, PGNO_INVALID, 0, P_IHEAP);
	ret = __db_log_page(dbp, dbc->txn, &pg->lsn, pgno, (PAGE *)pg);
	if ((t_ret = __memp_fput(
	    mpf, dbc->thread_info, pg, dbp->priority)) != 0 && ret == 0)
		ret = t_ret;

err:	if ((t_ret = __memp_fput(mpf,
	    dbc->thread_info, meta, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, meta_lock)) && ret == 0)
		ret = t_ret;
	return (ret);
}
