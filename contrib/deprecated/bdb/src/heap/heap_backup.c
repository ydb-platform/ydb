/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/heap.h"
#include "dbinc/mp.h"

/*
 * __heap_backup --
 *	Copy a heap database file coordinated with mpool.
 *
 * PUBLIC: int __heap_backup __P((DB_ENV *, DB *,
 * PUBLIC:     DB_THREAD_INFO *, DB_FH *, void *, u_int32_t));
 */
int
__heap_backup(dbenv, dbp, ip, fp, handle, flags)
	DB_ENV *dbenv;
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_FH *fp;
	void *handle;
	u_int32_t flags;
{
	HEAPPG *p;
	db_pgno_t chunk_pgno, high_pgno, max_pgno;
	int ret;

	max_pgno = dbp->mpf->mfp->last_pgno;
	chunk_pgno = FIRST_HEAP_RPAGE;

	for (;;) {
		/*
		 * Get the chunk page and the chunk's highest used page.
		 * Immediately return the page, it makes error handling easier.
		 */
		if ((ret = __memp_fget(dbp->mpf,
		    &chunk_pgno, ip, NULL, 0, &p)) != 0)
			break;
		high_pgno = p->high_pgno;
		if ((ret = __memp_fput(dbp->mpf,
		    ip, p, DB_PRIORITY_UNCHANGED)) != 0)
			break;

		/*
		 * Backup all the used pages in this chunk, starting at the
		 * chunk page.  If this is the very first chunk, be sure to
		 * backup the db meta page, too.
		*/
		if ((ret = __memp_backup_mpf(dbenv->env, dbp->mpf, ip,
		    chunk_pgno == FIRST_HEAP_RPAGE ? 0 : chunk_pgno,
		    high_pgno, fp, handle, flags)) != 0)
			break;
		chunk_pgno += HEAP_REGION_SIZE(dbp) + 1;
		if (chunk_pgno > max_pgno)
			break;
	}

	return (ret);
}
