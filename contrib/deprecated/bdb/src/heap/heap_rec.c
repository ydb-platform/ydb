/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2012 Oracle and/or its affiliates.  All rights reserved.
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/heap.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"

/*
 * __heap_addrem_recover --
 *	Recovery function for addrem.
 *
 * PUBLIC: int __heap_addrem_recover
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__heap_addrem_recover(env, dbtp, lsnp, op, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__heap_addrem_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	DB_THREAD_INFO *ip;
	PAGE *pagep, *regionp;
	db_pgno_t region_pgno;
	int cmp_n, cmp_p, modified, oldspace, ret, space;

	ip = ((DB_TXNHEAD *)info)->thread_info;
	pagep = NULL;
	REC_PRINT(__heap_addrem_print);
	REC_INTRO(__heap_addrem_read, ip, 1);
	region_pgno = HEAP_REGION_PGNO(file_dbp, argp->pgno);

	REC_FGET(mpf, ip, argp->pgno, &pagep, done);
	modified = 0;
	cmp_n = log_compare(lsnp, &LSN(pagep));
	cmp_p = log_compare(&LSN(pagep), &argp->pagelsn);

	if ((cmp_p == 0 && DB_REDO(op) && argp->opcode == DB_ADD_HEAP) ||
	    (cmp_n == 0 && DB_UNDO(op) && argp->opcode == DB_REM_HEAP)) {
		/* We are either redo-ing an add or undoing a delete. */
		REC_DIRTY(mpf, ip, dbc->priority, &pagep);
		if ((ret = __heap_pitem(dbc, pagep,
		    argp->indx, argp->nbytes, &argp->hdr, &argp->dbt)) != 0)
			goto out;
		modified = 1;
	} else if ((cmp_n == 0 && DB_UNDO(op) && argp->opcode == DB_ADD_HEAP) ||
	    (cmp_p == 0 && DB_REDO(op) && argp->opcode == DB_REM_HEAP)) {
		/* We are either undoing an add or redo-ing a delete. */
		REC_DIRTY(mpf, ip, dbc->priority, &pagep);
		if ((ret = __heap_ditem(
		    dbc, pagep, argp->indx, argp->nbytes)) != 0)
			goto out;
		modified = 1;
	}

	if (modified) {
		REC_FGET(mpf, ip, region_pgno, &regionp, done);
		if (DB_REDO(op))
			LSN(pagep) = *lsnp;
		else
			LSN(pagep) = argp->pagelsn;

		/* Update the available space bitmap, if necessary. */
		HEAP_CALCSPACEBITS(
		    file_dbp, HEAP_FREESPACE(file_dbp, pagep), space);
		oldspace = HEAP_SPACE(file_dbp, regionp,
		    argp->pgno - region_pgno - 1);
		if (space != oldspace) {
			REC_DIRTY(mpf, ip, dbc->priority, &regionp);
			HEAP_SETSPACE(file_dbp,
			    regionp, argp->pgno - region_pgno - 1, space);
		}
		if ((ret = __memp_fput(mpf, ip, regionp, dbc->priority)) != 0)
			goto out;
	}

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, ip, pagep, dbc->priority);
	REC_CLOSE;
}

/*
 * __heap_pg_alloc_recover --
 *	Recovery function for pg_alloc.
 *
 * PUBLIC: int __heap_pg_alloc_recover
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__heap_pg_alloc_recover(env, dbtp, lsnp, op, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__heap_pg_alloc_args *argp;
	DB *file_dbp;
	DBC *dbc;
	HEAPMETA *meta;
	DB_MPOOLFILE *mpf;
	DB_THREAD_INFO *ip;
	HEAPPG *pagep;
	db_pgno_t pgno;
	int cmp_n, cmp_p, ret, trunc;

	ip = ((DB_TXNHEAD *)info)->thread_info;
	meta = NULL;
	pagep = NULL;

	REC_PRINT(__heap_pg_alloc_print);
	REC_INTRO(__heap_pg_alloc_read, ip, 0);

	trunc = 0;
	pgno = PGNO_BASE_MD;
	if ((ret = __memp_fget(mpf, &pgno, ip, NULL, 0, &meta)) != 0) {
		/* The metadata page must always exist on redo. */
		if (DB_REDO(op)) {
			ret = __db_pgerr(file_dbp, pgno, ret);
			goto out;
		} else {
			ret = 0;
			goto done;
		}
	}
	cmp_n = log_compare(lsnp, &LSN(meta));
	cmp_p = log_compare(&LSN(meta), &argp->meta_lsn);
	CHECK_LSN(env, op, cmp_p, &LSN(meta), &argp->meta_lsn);
	CHECK_ABORT(env, op, cmp_n, &LSN(meta), lsnp);
	if (cmp_p == 0 && DB_REDO(op)) {
		/* Need to redo update described. */
		REC_DIRTY(mpf, ip, file_dbp->priority, &meta);
		LSN(meta) = *lsnp;
		if (argp->pgno > meta->dbmeta.last_pgno)
			meta->dbmeta.last_pgno = argp->pgno;
		if (argp->ptype == P_IHEAP &&
		    HEAP_REGION_NUM(file_dbp, argp->pgno) > meta->nregions)
			meta->nregions = HEAP_REGION_NUM(file_dbp, argp->pgno);
	} else if (cmp_n == 0 && DB_UNDO(op)) {
		/* Need to undo update described. */
		REC_DIRTY(mpf, ip, file_dbp->priority, &meta);
		LSN(meta) = argp->meta_lsn;
		if (meta->dbmeta.last_pgno != argp->last_pgno) {
			if (file_dbp->mpf->mfp->last_pgno ==
			    meta->dbmeta.last_pgno)
				trunc = 1;
			meta->dbmeta.last_pgno = argp->last_pgno;
		}
		if (argp->ptype == P_IHEAP &&
		    HEAP_REGION_NUM(file_dbp, argp->pgno) == meta->nregions) {
			do
				meta->nregions--;
			while (argp->last_pgno <
			    (meta->nregions - 1) * HEAP_REGION_SIZE(file_dbp));
		}
	}
	/*
	 * Fix up the allocated page.
	 * If we're undoing and the page doesn't exist, there's nothing to do,
	 * if the page does exist we simply zero it out.
	 * Otherwise if we're redoing the operation, we have
	 * to get the page (creating it if it doesn't exist), and update its
	 * LSN.
	 */
	if ((ret = __memp_fget(mpf, &argp->pgno, ip, NULL, 0, &pagep)) != 0) {
		if (DB_UNDO(op)) {
			ret = 0;
			goto do_meta;
		}
		if ((ret = __memp_fget(mpf,
		    &argp->pgno, ip, NULL, DB_MPOOL_CREATE, &pagep)) != 0) {
			ret = __db_pgerr(file_dbp, argp->pgno, ret);
			goto out;
		}
	}

	cmp_n = LOG_COMPARE(lsnp, &LSN(pagep));
	if (DB_REDO(op) && IS_ZERO_LSN(LSN(pagep))) {
		REC_DIRTY(mpf, ip, file_dbp->priority, &pagep);
		P_INIT(pagep, file_dbp->pgsize,
		    argp->pgno, PGNO_INVALID, PGNO_INVALID, 0, argp->ptype);
		LSN(pagep) = *lsnp;
	} else if ((cmp_n == 0 || IS_ZERO_LSN(LSN(pagep))) && DB_UNDO(op)) {
		if (argp->pgno == file_dbp->mpf->mfp->last_pgno)
			trunc = 1;
		else if (!IS_ZERO_LSN(LSN(pagep))) {
			REC_DIRTY(mpf, ip, file_dbp->priority, &pagep);
			memset(pagep, 0, file_dbp->pgsize);
		}
	}
	/* If the page is newly allocated and aborted, give it back. */
	if (pagep != NULL && (trunc == 1 ||
	    (IS_ZERO_LSN(LSN(pagep)) && TYPE(pagep) != P_IHEAP))) {
		if ((ret = __memp_fput(mpf,
		    ip, pagep, file_dbp->priority)) != 0)
			goto out;
		pagep = NULL;
		if ((ret = __memp_fget(mpf,
		    &argp->pgno, ip, NULL, DB_MPOOL_FREE, &pagep)) != 0)
			goto out;
		if (trunc == 0 && argp->pgno <= mpf->mfp->last_flushed_pgno) {
			/*
			 * If this page is on disk we need to zero it.
			 * This is safe since we never free pages other
			 * than backing out an allocation, so there can
			 * not be a previous allocate and free of this
			 * page that is reflected on disk.
			 */
			if ((ret = __db_zero_extend(env, mpf->fhp,
			    argp->pgno, argp->pgno, file_dbp->pgsize)) != 0)
				goto out;
		}
	}
	/*
	 * Keep the region high_pgno up to date	This not logged so we
	 * always need to check it.
	 */
	if (DB_REDO(op)) {
		if ((ret = __memp_fput(mpf,
		    ip, pagep, file_dbp->priority)) != 0)
			goto out;
		pagep = NULL;
		pgno = HEAP_REGION_PGNO(file_dbp, argp->pgno);
		if ((ret = __memp_fget(mpf, &pgno, ip, NULL, 0, &pagep)) != 0)
			goto out;
		if (pagep->high_pgno >= argp->pgno)
			goto done;
		if ((ret = __memp_dirty(mpf, &pagep, ip, NULL,
		    DB_PRIORITY_UNCHANGED, 0)) != 0)
			goto done;
		pagep->high_pgno = argp->pgno;
	}

do_meta:
	if (trunc == 1 &&
	    (ret = __memp_ftruncate(mpf, NULL, ip, meta->dbmeta.last_pgno + 1,
	    MP_TRUNC_RECOVER | MP_TRUNC_NOCACHE)) != 0)
		goto out;

done:	*lsnp = argp->prev_lsn;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, ip, pagep, file_dbp->priority);
	if (meta != NULL)
		(void)__memp_fput(mpf, ip, meta, file_dbp->priority);
	REC_CLOSE;
}

/*
 * __heap_trunc_meta_recover --
 *	Recovery function for trunc_meta.
 *
 * PUBLIC: int __heap_trunc_meta_recover
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__heap_trunc_meta_recover(env, dbtp, lsnp, op, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__heap_trunc_meta_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	DB_THREAD_INFO *ip;
	HEAPMETA *meta;
	PAGE *pagep;
	int cmp_n, cmp_p, ret;

	ip = ((DB_TXNHEAD *)info)->thread_info;
	pagep = NULL;
	REC_PRINT(__heap_trunc_meta_print);
	REC_INTRO(__heap_trunc_meta_read, ip, 1);

	REC_FGET(mpf, ip, argp->pgno, &pagep, done);
	cmp_n = log_compare(lsnp, &LSN(pagep));
	cmp_p = log_compare(&LSN(pagep), &argp->pagelsn);
	meta = (HEAPMETA *)pagep;

	if (cmp_n == 0 && DB_UNDO(op)) {
		REC_DIRTY(mpf, ip, dbc->priority, &pagep);
		meta->dbmeta.last_pgno = argp->last_pgno;
		meta->dbmeta.key_count = argp->key_count;
		meta->dbmeta.record_count = argp->record_count;
		meta->curregion = argp->curregion;
		meta->nregions = argp->nregions;
		LSN(meta) = argp->pagelsn;
	} else if (cmp_p == 0 && DB_REDO(op)) {
		REC_DIRTY(mpf, ip, dbc->priority, &pagep);
		/* last_pgno to 1 to account for region page */
		meta->dbmeta.last_pgno = 1;
		meta->dbmeta.key_count = 0;
		meta->dbmeta.record_count = 0;
		meta->curregion = FIRST_HEAP_RPAGE;
		meta->nregions = 1;
		LSN(meta) = *lsnp;
		if ((ret = __memp_ftruncate(mpf, dbc->txn,
		    ip, PGNO_BASE_MD + 1, MP_TRUNC_NOCACHE)) != 0)
			goto out;
	}

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, ip, pagep, dbc->priority);
	REC_CLOSE;
}

/*
 * __heap_trunc_page_recover --
 *	Recovery function for trunc_page.
 *
 * PUBLIC: int __heap_trunc_page_recover
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__heap_trunc_page_recover(env, dbtp, lsnp, op, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__heap_trunc_page_args *argp;
	DB *file_dbp;
	DBC *dbc;
	DB_MPOOLFILE *mpf;
	DB_THREAD_INFO *ip;
	PAGE *pagep;
	int cmp_p, ret;

	ip = ((DB_TXNHEAD *)info)->thread_info;
	pagep = NULL;
	REC_PRINT(__heap_trunc_page_print);
	REC_INTRO(__heap_trunc_page_read, ip, 1);

	if ((ret = __memp_fget(mpf, &argp->pgno, ip, NULL, 0, &pagep)) != 0) {
		if (DB_REDO(op))
			goto done;
		if ((ret = __memp_fget(mpf,
		    &argp->pgno, ip, NULL, DB_MPOOL_CREATE, &pagep)) != 0) {
			ret = __db_pgerr(file_dbp, argp->pgno, ret);
			goto out;
		}
	}
	cmp_p = log_compare(&LSN(pagep), &argp->pagelsn);

	if (DB_UNDO(op) && IS_ZERO_LSN(LSN(pagep))) {
		REC_DIRTY(mpf, ip, dbc->priority, &pagep);
		memcpy(pagep, argp->old_data.data, argp->old_data.size);
		LSN(pagep) = argp->pagelsn;
	} else if (cmp_p == 0 && DB_REDO(op)) {
		if ((ret = __memp_fput(mpf, ip, pagep, dbc->priority)) != 0)
			goto out;
		pagep = NULL;
		if ((ret = __memp_fget(mpf, &argp->pgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_FREE, &pagep)) != 0)
			goto out;
	}

done:	*lsnp = argp->prev_lsn;
	ret = 0;

out:	if (pagep != NULL)
		(void)__memp_fput(mpf, ip, pagep, dbc->priority);
	REC_CLOSE;
}
