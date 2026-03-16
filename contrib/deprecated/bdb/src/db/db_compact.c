/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1999, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/hash.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc/fop.h"
#ifdef HAVE_FTRUNCATE
static int __db_free_freelist __P((DB *, DB_THREAD_INFO *, DB_TXN *));
static int __db_setup_freelist __P((DB *, db_pglist_t *, u_int32_t));
#endif

#define	SAVE_START							\
	do {								\
		save_data = *c_data;					\
		ret = __db_retcopy(env,				\
		     &save_start, current.data, current.size,		\
		     &save_start.data, &save_start.ulen);		\
	} while (0)

/*
 * Only restore those things that are negated by aborting the
 * transaction.  We don't restore the number of deadlocks, for example.
 */

#define	RESTORE_START							\
	do {								\
		c_data->compact_pages_free =				\
		      save_data.compact_pages_free;			\
		c_data->compact_levels = save_data.compact_levels;	\
		c_data->compact_truncate = save_data.compact_truncate;	\
		c_data->compact_empty_buckets =				\
		    save_data.compact_empty_buckets;			\
		ret = __db_retcopy(env, &current,			\
		     save_start.data, save_start.size,			\
		     &current.data, &current.ulen);			\
	} while (0)

/*
 * __db_compact_int -- compact a database.
 *
 * PUBLIC: int __db_compact_int __P((DB *, DB_THREAD_INFO *, DB_TXN *,
 * PUBLIC:     DBT *, DBT *, DB_COMPACT *, u_int32_t, DBT *));
 */
int
__db_compact_int(dbp, ip, txn, start, stop, c_data, flags, end)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	DBT *start, *stop;
	DB_COMPACT *c_data;
	u_int32_t flags;
	DBT *end;
{
	DBC *dbc;
	DBT current, save_start;
	DB_COMPACT save_data;
	DB_TXN *txn_orig;
	ENV *env;
	u_int32_t empty_buckets, factor, retry;
	int deadlock, have_freelist, isdone, ret, span, t_ret, txn_local;

#ifdef HAVE_FTRUNCATE
	db_pglist_t *list;
	db_pgno_t last_pgno;
	u_int32_t nelems, truncated;
#endif

	env = dbp->env;

	memset(&current, 0, sizeof(current));
	memset(&save_start, 0, sizeof(save_start));
	dbc = NULL;
	factor = 0;
	have_freelist = deadlock = isdone = span = 0;
	ret = retry = 0;
	txn_orig = txn;

#ifdef HAVE_FTRUNCATE
	list = NULL;
	last_pgno = 0;
	nelems = truncated = 0;
#endif

	/*
	 * We pass "current" to the internal routine, indicating where that
	 * routine should begin its work and expecting that it will return to
	 * us the last key that it processed.
	 */
	if (start != NULL && (ret = __db_retcopy(env,
	     &current, start->data, start->size,
	     &current.data, &current.ulen)) != 0)
		return (ret);

	empty_buckets = c_data->compact_empty_buckets;

	if (IS_DB_AUTO_COMMIT(dbp, txn)) {
		txn_local = 1;
		LF_SET(DB_AUTO_COMMIT);
	} else
		txn_local = 0;
	if (!LF_ISSET(DB_FREE_SPACE | DB_FREELIST_ONLY))
		goto no_free;
	if (LF_ISSET(DB_FREELIST_ONLY))
		LF_SET(DB_FREE_SPACE);

#ifdef HAVE_FTRUNCATE
	/* Sort the freelist and set up the in-memory list representation. */
	if (txn_local && (ret = __txn_begin(env, ip, txn_orig, &txn, 0)) != 0)
		goto err;

	if ((ret = __db_free_truncate(dbp, ip,
	     txn, flags, c_data, &list, &nelems, &last_pgno)) != 0) {
		LF_CLR(DB_FREE_SPACE);
		goto terr;
	}

	/* If the freelist is empty and we are not filling, get out. */
	if (nelems == 0 && LF_ISSET(DB_FREELIST_ONLY)) {
		ret = 0;
		LF_CLR(DB_FREE_SPACE);
		goto terr;
	}
	if ((ret = __db_setup_freelist(dbp, list, nelems)) != 0) {
		/* Someone else owns the free list. */
		if (ret == EBUSY)
			ret = 0;
	}
	if (ret == 0)
		have_freelist = 1;

	/* Commit the txn and release the meta page lock. */
terr:	if (txn_local) {
		if ((t_ret = __txn_commit(txn, DB_TXN_NOSYNC)) != 0 && ret == 0)
			ret = t_ret;
		txn = NULL;
	}
	if (ret != 0)
		goto err;

	/* Save the number truncated so far, we will add what we get below. */
	truncated = c_data->compact_pages_truncated;
	if (LF_ISSET(DB_FREELIST_ONLY))
		goto done;
#endif

	/*
	 * We want factor to be the target number of free bytes on each page,
	 * so we know when to stop adding items to a page.   Make sure to
	 * subtract the page overhead when computing this target.  This can
	 * result in a 1-2% error on the smallest page.
	 * First figure out how many bytes we should use:
	 */
no_free:
	factor = dbp->pgsize - SIZEOF_PAGE;
	if (c_data->compact_fillpercent != 0) {
		factor *= c_data->compact_fillpercent;
		factor /= 100;
	}
	/* Now convert to the number of free bytes to target. */
	factor = (dbp->pgsize - SIZEOF_PAGE) - factor;

	if (c_data->compact_pages == 0)
		c_data->compact_pages = DB_MAX_PAGES;

	do {
		deadlock = 0;

		SAVE_START;
		if (ret != 0)
			break;

		if (txn_local) {
			if ((ret =
			    __txn_begin(env, ip, txn_orig, &txn, 0)) != 0)
				break;

			if (c_data->compact_timeout != 0 &&
			    (ret = __txn_set_timeout(txn,
			    c_data->compact_timeout, DB_SET_LOCK_TIMEOUT)) != 0)
				goto err;
		}

		if ((ret = __db_cursor(dbp, ip, txn, &dbc, 0)) != 0)
			goto err;

#ifdef HAVE_HASH
		if (dbp->type == DB_HASH)
			ret = __ham_compact_int(dbc,
			    &current, stop, factor, c_data, &isdone, flags);
		else
#endif
			ret = __bam_compact_int(dbc, &current, stop, factor,
			     &span, c_data, &isdone);
		if (ret == DB_LOCK_DEADLOCK && txn_local) {
			/*
			 * We retry on deadlock.  Cancel the statistics
			 * and reset the start point to before this
			 * iteration.
			 */
			deadlock = 1;
			c_data->compact_deadlock++;
			RESTORE_START;
		}
		/*
		 * If we could not get a lock while holding an internal
		 * node latched, commit the current local transaction otherwise
		 * report a deadlock.
		 */
		if (ret == DB_LOCK_NOTGRANTED) {
			if (txn_local || retry++ < 5)
				ret = 0;
			else
				ret = DB_LOCK_DEADLOCK;
		} else
			retry = 0;

		if ((t_ret = __dbc_close(dbc)) != 0 && ret == 0)
			ret = t_ret;

err:		if (txn_local && txn != NULL) {
			if (ret == 0 && deadlock == 0)
				ret = __txn_commit(txn, DB_TXN_NOSYNC);
			else if ((t_ret = __txn_abort(txn)) != 0 && ret == 0)
				ret = t_ret;
			txn = NULL;
		}
		DB_ASSERT(env, ip == NULL || ip->dbth_pincount == 0);
	} while (ret == 0 && !isdone);

	if (ret == 0 && end != NULL)
		ret = __db_retcopy(env, end, current.data, current.size,
		    &end->data, &end->ulen);
	if (current.data != NULL)
		__os_free(env, current.data);
	if (save_start.data != NULL)
		__os_free(env, save_start.data);

#ifdef HAVE_FTRUNCATE
	/*
	 * Finish up truncation work.  If there are pages left in the free
	 * list we can try to move the internal structures around so that we
	 * can remove more pages from the file.
	 * For BTREE search the internal nodes of the tree as we may have
	 * missed some while walking the leaf nodes.
	 * For HASH we will compact the hash table itself, moving segments
	 * to lower number pages where possible.
	 * Then calculate how many pages we have truncated and release
	 * the in-memory free list.
	 */
done:	if (LF_ISSET(DB_FREE_SPACE)) {
		DBMETA *meta;
		db_pgno_t pgno;

		pgno = PGNO_BASE_MD;
		isdone = 1;
		if (ret == 0 && !LF_ISSET(DB_FREELIST_ONLY) &&
		    __memp_fget(dbp->mpf, &pgno, ip, txn, 0, &meta) == 0) {
			isdone = meta->free == PGNO_INVALID;
			ret = __memp_fput(dbp->mpf, ip, meta, dbp->priority);
		}

#ifdef HAVE_HASH
		if (dbp->type == DB_HASH) {
			c_data->compact_empty_buckets -= empty_buckets;
			if (!isdone || c_data->compact_empty_buckets != 0)
				ret = __ham_compact_hash(dbp,
				    ip, txn_orig, c_data);
			c_data->compact_empty_buckets += empty_buckets;
		} else
#endif
		if (!isdone)
			ret = __bam_truncate_ipages(dbp, ip, txn_orig, c_data);

		/* Clean up the free list. */
		if (list != NULL)
			__os_free(env, list);

		if ((t_ret =
		    __memp_fget(dbp->mpf, &pgno, ip, txn, 0, &meta)) == 0) {
			c_data->compact_pages_truncated =
			    truncated + last_pgno - meta->last_pgno;
			if ((t_ret = __memp_fput(dbp->mpf, ip,
			    meta, dbp->priority)) != 0 && ret == 0)
				ret = t_ret;
		} else if (ret == 0)
			ret = t_ret;

		if (have_freelist && (t_ret =
		    __db_free_freelist(dbp, ip, txn_orig)) != 0 && ret == 0)
			t_ret = ret;
	}
#endif

	return (ret);
}

#ifdef HAVE_FTRUNCATE
static int
__db_setup_freelist(dbp, list, nelems)
	DB *dbp;
	db_pglist_t *list;
	u_int32_t nelems;
{
	DB_MPOOLFILE *mpf;
	db_pgno_t *plist;
	int ret;

	mpf = dbp->mpf;

	if ((ret = __memp_alloc_freelist(mpf, nelems, &plist)) != 0)
		return (ret);

	while (nelems-- != 0)
		*plist++ = list++->pgno;

	return (0);
}

static int
__db_free_freelist(dbp, ip, txn)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
{
	DBC *dbc;
	DB_LOCK lock;
	int auto_commit, ret, t_ret;

	LOCK_INIT(lock);
	auto_commit = ret = 0;

	/*
	 * If we are not in a transaction then we need to get
	 * a lock on the meta page, otherwise we should already
	 * have the lock.
	 */

	dbc = NULL;
	if (IS_DB_AUTO_COMMIT(dbp, txn)) {
		/*
		 * We must not timeout the lock or we will not free the list.
		 * We ignore errors from txn_begin as there is little that
		 * the application can do with the error and we want to
		 * get the lock and free the list if at all possible.
		 */
		if (__txn_begin(dbp->env, ip, txn, &txn, 0) == 0) {
			(void)__lock_set_timeout(dbp->env,
			    txn->locker, 0, DB_SET_TXN_TIMEOUT);
			(void)__lock_set_timeout(dbp->env,
			    txn->locker, 0, DB_SET_LOCK_TIMEOUT);
			auto_commit = 1;
		}
		/* Get a cursor so we can call __db_lget. */
		if ((ret = __db_cursor(dbp, ip, txn, &dbc, 0)) != 0)
			return (ret);

		if ((ret = __db_lget(dbc,
		     0, PGNO_BASE_MD, DB_LOCK_WRITE, 0, &lock)) != 0)
			goto err;
	}

	ret = __memp_free_freelist(dbp->mpf);

err:	if (dbc != NULL && (t_ret = __LPUT(dbc, lock)) != 0 && ret == 0)
		ret = t_ret;

	if (dbc != NULL && (t_ret = __dbc_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	if (auto_commit && (t_ret = __txn_abort(txn)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}
#endif

/*
 * __db_exchange_page -- swap a page with a lower numbered page.
 * The routine will optionally free the higher numbered page.  The cursor
 * has a stack which includes at least the immediate parent of this page.
 * PUBLIC: int __db_exchange_page __P((DBC *, PAGE **, PAGE *, db_pgno_t, int));
 */
int
__db_exchange_page(dbc, pgp, opg, newpgno, flags)
	DBC *dbc;
	PAGE **pgp, *opg;
	db_pgno_t newpgno;
	int flags;
{
	BTREE_CURSOR *cp;
	DB *dbp;
	DBT data, *dp, hdr;
	DB_LSN lsn;
	DB_LOCK lock;
	EPG *epg;
	PAGE *newpage;
	db_pgno_t oldpgno, *pgnop;
	int ret;

	DB_ASSERT(NULL, dbc != NULL);
	dbp = dbc->dbp;
	LOCK_INIT(lock);

	/*
	 * We want to free a page that lives in the part of the file that
	 * can be truncated, so we're going to move it onto a free page
	 * that is in the part of the file that need not be truncated.
	 * In the case of compacting hash table segments the caller already
	 * identified a contiguous set of pages to use.  Otherwise
	 * since the freelist is ordered now, we can simply call __db_new
	 * which will grab the first element off the freelist; we know this
	 * is the lowest numbered free page.
	 */
	if (newpgno != PGNO_INVALID) {
		if ((ret = __memp_fget(dbp->mpf, &newpgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &newpage)) != 0)
			return (ret);
	} else if ((ret = __db_new(dbc, P_DONTEXTEND | TYPE(*pgp),
	     STD_LOCKING(dbc) && TYPE(*pgp) != P_OVERFLOW ? &lock : NULL,
	     &newpage)) != 0)
		return (ret);

	/*
	 * If newpage is null then __db_new would have had to allocate
	 * a new page from the filesystem, so there is no reason
	 * to continue this action.
	 */
	if (newpage == NULL)
		return (0);

	/*
	 * It is possible that a higher page is allocated if other threads
	 * are allocating at the same time, if so, just put it back.
	 */
	if (PGNO(newpage) > PGNO(*pgp)) {
		/* Its unfortunate but you can't just free a new overflow. */
		if (TYPE(newpage) == P_OVERFLOW)
			OV_LEN(newpage) = 0;
		if ((ret = __LPUT(dbc, lock)) != 0)
			return (ret);
		return (__db_free(dbc, newpage, 0));
	}

	/* Log if necessary. */
	if (DBC_LOGGING(dbc)) {
		memset(&hdr, 0, sizeof(hdr));
		hdr.data = *pgp;
		hdr.size = P_OVERHEAD(dbp);
		memset(&data, 0, sizeof(data));
		dp = &data;
		switch (TYPE(*pgp)) {
		case P_OVERFLOW:
			data.data = (u_int8_t *)*pgp + P_OVERHEAD(dbp);
			data.size = OV_LEN(*pgp);
			break;
		case P_BTREEMETA:
			hdr.size = sizeof(BTMETA);
			dp = NULL;
			break;
		case P_HASHMETA:
			hdr.size = sizeof(HMETA);
			dp = NULL;
			break;
		default:
			data.data = (u_int8_t *)*pgp + HOFFSET(*pgp);
			data.size = dbp->pgsize - HOFFSET(*pgp);
			hdr.size += NUM_ENT(*pgp) * sizeof(db_indx_t);
		}
		if ((ret = __db_merge_log(dbp, dbc->txn,
		      &LSN(newpage), 0, PGNO(newpage), &LSN(newpage),
		      PGNO(*pgp), &LSN(*pgp), &hdr, dp, 1)) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(newpage));

	oldpgno = PGNO(*pgp);
	newpgno = PGNO(newpage);
	lsn = LSN(newpage);
	memcpy(newpage, *pgp, dbp->pgsize);
	PGNO(newpage) = newpgno;
	LSN(newpage) = lsn;

	/* Empty the old page. */
	if ((ret = __memp_dirty(dbp->mpf,
	    pgp, dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0)
		goto err;
	if (TYPE(*pgp) == P_OVERFLOW)
		OV_LEN(*pgp) = 0;
	else {
		HOFFSET(*pgp) = dbp->pgsize;
		NUM_ENT(*pgp) = 0;
	}
	LSN(*pgp) = lsn;

	/* Update siblings. */
	switch (TYPE(newpage)) {
	case P_OVERFLOW:
	case P_LBTREE:
	case P_LRECNO:
	case P_LDUP:
	case P_HASH:
		if (NEXT_PGNO(newpage) == PGNO_INVALID &&
		    PREV_PGNO(newpage) == PGNO_INVALID)
			break;
		if ((ret = __db_relink(dbc, *pgp, opg, PGNO(newpage))) != 0)
			goto err;
		break;
	default:
		break;
	}

	/*
	 * For HASH we may reuse the old page for an even higher numbered
	 * page.  Otherwise we free the old page.
	 */
	if (!LF_ISSET(DB_EXCH_FREE)) {
		NEXT_PGNO(*pgp) = PREV_PGNO(*pgp) = PGNO_INVALID;
		ret = __memp_fput(dbp->mpf,
		    dbc->thread_info, *pgp, dbc->priority);
	} else
		ret = __db_free(dbc, *pgp, 0);
	*pgp = newpage;

	if (ret != 0)
		return (ret);

	if (!LF_ISSET(DB_EXCH_PARENT))
		goto done;

	/* Update the parent. */
	cp = (BTREE_CURSOR *)dbc->internal;
	epg = &cp->csp[-1];

	switch (TYPE(epg->page)) {
	case P_IBTREE:
		pgnop = &GET_BINTERNAL(dbp, epg->page, epg->indx)->pgno;
		break;
	case P_IRECNO:
		pgnop = &GET_RINTERNAL(dbp, epg->page, epg->indx)->pgno;
		break;
	case P_LBTREE:
	case P_LRECNO:
	case P_LDUP:
		pgnop = &GET_BOVERFLOW(dbp, epg->page, epg->indx)->pgno;
		break;
	default:
		return (__db_pgfmt(dbp->env, PGNO(epg->page)));
	}
	DB_ASSERT(dbp->env, oldpgno == *pgnop);
	if (DBC_LOGGING(dbc)) {
		if ((ret = __db_pgno_log(dbp, dbc->txn, &LSN(epg->page),
		    0, PGNO(epg->page), &LSN(epg->page), (u_int32_t)epg->indx,
		    *pgnop, PGNO(newpage))) != 0)
			return (ret);
	} else
		LSN_NOT_LOGGED(LSN(epg->page));

	*pgnop = PGNO(newpage);
	cp->csp->page = newpage;
	if ((ret = __TLPUT(dbc, lock)) != 0)
		return (ret);

done:	return (0);

err:	(void)__memp_fput(dbp->mpf, dbc->thread_info, newpage, dbc->priority);
	(void)__TLPUT(dbc, lock);
	return (ret);
}

/*
 * __db_truncate_overflow -- find overflow pages to truncate.
 *	Walk the pages of an overflow chain and swap out
 * high numbered pages.  We are passed the first page
 * but only deal with the second and subsequent pages.
 * PUBLIC:  int __db_truncate_overflow __P((DBC *,
 * PUBLIC:     db_pgno_t, PAGE **, DB_COMPACT *));
 */
int
__db_truncate_overflow(dbc, pgno, ppg, c_data)
	DBC *dbc;
	db_pgno_t pgno;
	PAGE **ppg;
	DB_COMPACT *c_data;
{
	DB *dbp;
	DB_LOCK lock;
	PAGE *page;
	db_pgno_t ppgno;
	int have_lock, ret, t_ret;

	dbp = dbc->dbp;
	page = NULL;
	LOCK_INIT(lock);
	have_lock = ppg == NULL;

	if ((ret = __memp_fget(dbp->mpf, &pgno,
	     dbc->thread_info, dbc->txn, 0, &page)) != 0)
		return (ret);

	while ((pgno = NEXT_PGNO(page)) != PGNO_INVALID) {
		if ((ret = __memp_fput(dbp->mpf,
		     dbc->thread_info, page, dbc->priority)) != 0)
			return (ret);
		if ((ret = __memp_fget(dbp->mpf, &pgno,
		    dbc->thread_info, dbc->txn, 0, &page)) != 0)
			return (ret);
		if (pgno <= c_data->compact_truncate)
			continue;
		if (have_lock == 0) {
			DB_ASSERT(dbp->env, ppg != NULL);
			ppgno = PGNO(*ppg);
			if ((ret = __memp_fput(dbp->mpf, dbc->thread_info,
			     *ppg, dbc->priority)) != 0)
				goto err;
			*ppg = NULL;
			if ((ret = __db_lget(dbc, 0, ppgno,
			     DB_LOCK_WRITE, 0, &lock)) != 0)
				goto err;
			if ((ret = __memp_fget(dbp->mpf, &ppgno,
			    dbc->thread_info,
			    dbc->txn, DB_MPOOL_DIRTY, ppg)) != 0)
				goto err;
			have_lock = 1;
		}
		if ((ret = __db_exchange_page(dbc,
		    &page, NULL, PGNO_INVALID, DB_EXCH_FREE)) != 0)
			break;
	}

err:	if (page != NULL &&
	    (t_ret = __memp_fput( dbp->mpf,
	    dbc->thread_info, page, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, lock)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}
/*
 * __db_truncate_root -- swap a root page for a lower numbered page.
 * PUBLIC: int __db_truncate_root __P((DBC *,
 * PUBLIC:      PAGE *, u_int32_t, db_pgno_t *, u_int32_t));
 */
int
__db_truncate_root(dbc, ppg, indx, pgnop, tlen)
	DBC *dbc;
	PAGE *ppg;
	u_int32_t indx;
	db_pgno_t *pgnop;
	u_int32_t tlen;
{
	DB *dbp;
	DBT orig;
	PAGE *page;
	int ret, t_ret;
	db_pgno_t newpgno;

	dbp = dbc->dbp;

	DB_ASSERT(dbc->dbp->env, IS_DIRTY(ppg));
	if ((ret = __memp_fget(dbp->mpf, pgnop,
	     dbc->thread_info, dbc->txn, 0, &page)) != 0)
		goto err;

	/*
	 * If this is a multiply reference overflow key, then we will just
	 * copy it and decrement the reference count.  This is part of a
	 * fix to get rid of multiple references.
	 */
	if (TYPE(page) == P_OVERFLOW && OV_REF(page) > 1) {
		COMPQUIET(newpgno, 0);
		if ((ret = __db_ovref(dbc, *pgnop)) != 0)
			goto err;
		memset(&orig, 0, sizeof(orig));
		if ((ret = __db_goff(dbc, &orig, tlen, *pgnop,
		    &orig.data, &orig.size)) == 0)
			ret = __db_poff(dbc, &orig, &newpgno);
		if (orig.data != NULL)
			__os_free(dbp->env, orig.data);
		if (ret != 0)
			goto err;
	} else {
		LOCK_CHECK_OFF(dbc->thread_info);
		ret = __db_exchange_page(dbc,
		    &page, NULL, PGNO_INVALID, DB_EXCH_FREE);
		LOCK_CHECK_ON(dbc->thread_info);
		if (ret != 0)
			goto err;
		newpgno = PGNO(page);
		/* If we could not allocate from the free list, give up.*/
		if (newpgno == *pgnop)
			goto err;
	}

	/* Update the reference. */
	if (DBC_LOGGING(dbc)) {
		if ((ret = __db_pgno_log(dbp,
		     dbc->txn, &LSN(ppg), 0, PGNO(ppg),
		     &LSN(ppg), (u_int32_t)indx, *pgnop, newpgno)) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(ppg));

	*pgnop = newpgno;

err:	if (page != NULL && (t_ret =
	      __memp_fput(dbp->mpf, dbc->thread_info,
		  page, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

#ifdef HAVE_FTRUNCATE
/*
 * __db_find_free --
 *	Find a contiguous "size" range of free pages that are lower numbers
 * than the pages starting at "bstart".  We can also return a set of pages
 * that overlaps with the pages at "bstart".
 * PUBLIC: int __db_find_free __P((DBC *, u_int32_t,
 * PUBLIC:	u_int32_t, db_pgno_t, db_pgno_t *));
 */
int
__db_find_free(dbc, type, size, bstart, freep)
	DBC *dbc;
	u_int32_t type;
	u_int32_t size;
	db_pgno_t bstart, *freep;
{
	DB *dbp;
	DBMETA *meta;
	DBT listdbt;
	DB_LOCK metalock;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	PAGE *page, *freepg;
	u_int32_t i, j, start, nelems;
	db_pgno_t *list, next_free, pgno;
	db_pglist_t *lp, *pglist;
	int hash, ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	nelems = 0;
	hash = 0;
	page = NULL;
	pglist = NULL;
	meta = NULL;
	LOCK_INIT(metalock);

#ifdef HAVE_HASH
	if (dbp->type == DB_HASH) {
		if ((ret = __ham_return_meta(dbc, DB_MPOOL_DIRTY, &meta)) != 0)
			return (ret);
		if (meta != NULL)
			hash = 1;
	}
#endif
	if (meta == NULL) {
		pgno = PGNO_BASE_MD;
		if ((ret = __db_lget(dbc,
		    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
			goto err;
		if ((ret = __memp_fget(mpf, &pgno, dbc->thread_info, dbc->txn,
		    DB_MPOOL_DIRTY, &meta)) != 0)
			goto err;
	}

	if ((ret = __memp_get_freelist(mpf, &nelems, &list)) != 0)
		goto err;

	if (nelems == 0) {
		ret = DB_NOTFOUND;
		goto err;
	}

	for (i = 0; i < nelems; i++) {
		if (list[i] > bstart) {
			ret = DB_NOTFOUND;
			goto err;
		}
		start = i;
		if (size == 1)
			goto found;
		while (i < nelems - 1 && list[i] + 1 == list[i + 1]) {
			i++;
			if (i - start == size - 1)
				goto found;
		}
		if (i - start == size - 1)
			goto found;
		/*
		 * If the last set of contiguous free pages we found
		 * are contiguous to the chunk we are trying to move,
		 * then we can slide the allocated chunk back some number
		 * of pages -- figure out how many by calculating the
		 * number of pages before the allocated ones that we have
		 * found in the free list.
		 */
		if (list[i] == bstart - 1) {
			size = (i - start) + 1;
			goto found;
		}
	}
	ret = DB_NOTFOUND;
	goto err;

found:	/* We have size range of pages.  Remove them. */
	next_free = i == nelems - 1 ? PGNO_INVALID : list[i + 1];
	*freep = list[start];
	if (start == 0) {
		page = (PAGE *)meta;
	} else if ((ret = __memp_fget(mpf, &list[start - 1],
	     dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &page)) != 0)
		return (ret);
	if (DBC_LOGGING(dbc)) {
		if ((ret = __os_malloc(dbp->env,
		    size * sizeof(db_pglist_t), &pglist)) != 0)
			goto err;
		lp = pglist;
		for (j = start; j < start + size; j++, lp++) {
			if ((ret = __memp_fget(mpf, &list[j],
			    dbc->thread_info, dbc->txn, 0, &freepg)) != 0)
				goto err;
			lp->pgno = PGNO(freepg);
			lp->next_pgno = NEXT_PGNO(freepg);
			lp->lsn = LSN(freepg);
			if ((ret = __memp_fput(mpf,
			    dbc->thread_info, freepg, dbc->priority)) != 0)
				goto err;
		}
		listdbt.size = size * sizeof(*pglist);
		listdbt.data = pglist;
		if ((ret = __db_realloc_log(dbp, dbc->txn, &lsn, 0,
		    PGNO(page), &LSN(page), next_free, type, &listdbt)) != 0)
			goto err;
		__os_free(dbp->env, pglist);
		pglist = NULL;
	} else
		LSN_NOT_LOGGED(lsn);

	LSN(page) = lsn;
	if (start == 0)
		meta->free = next_free;
	else
		NEXT_PGNO(page) = next_free;

	if (page != (PAGE *)meta && (ret = __memp_fput(mpf,
	    dbc->thread_info, page, dbc->priority)) != 0)
		goto err;

	for (j = start; j < start + size; j++) {
		if ((ret = __memp_fget(mpf,
		    &list[j], dbc->thread_info,
		    dbc->txn, DB_MPOOL_DIRTY, &freepg)) != 0)
			goto err;
		P_INIT(freepg, dbp->pgsize,
		    list[j], PGNO_INVALID, PGNO_INVALID, 0, type);
		LSN(freepg) = lsn;
		if ((ret = __memp_fput(mpf,
		    dbc->thread_info, freepg, dbc->priority)) != 0)
			goto err;
	}

	if (++i != nelems)
		memmove(&list[start], &list[i], (nelems - i) * sizeof(*list));
	if ((ret = __memp_extend_freelist(mpf, nelems - size, &list)) != 0)
		goto err;
	if (hash == 0)
		ret = __memp_fput(mpf, dbc->thread_info, meta, dbc->priority);
	t_ret = __TLPUT(dbc, metalock);

	return (ret == 0 ? t_ret : ret);

err:	if (page != NULL && page != (PAGE *)meta)
		(void)__memp_fput(mpf, dbc->thread_info, page, dbc->priority);
	if (pglist != NULL)
		__os_free(dbp->env, pglist);
	if (meta != NULL && hash == 0)
		(void)__memp_fput(mpf, dbc->thread_info, meta, dbc->priority);
	(void)__TLPUT(dbc, metalock);
	return (ret);
}
#endif

/*
 * __db_relink --
 *	Relink around a deleted page.
 *
 * PUBLIC: int __db_relink __P((DBC *, PAGE *, PAGE *, db_pgno_t));
 *	Otherp can be either the previous or the next page to use if
 * the caller already holds that page.
 */
int
__db_relink(dbc, pagep, otherp, new_pgno)
	DBC *dbc;
	PAGE *pagep, *otherp;
	db_pgno_t new_pgno;
{
	DB *dbp;
	DB_LOCK npl, ppl;
	DB_LSN *nlsnp, *plsnp, ret_lsn;
	DB_MPOOLFILE *mpf;
	PAGE *np, *pp;
	int ret, t_ret;

	dbp = dbc->dbp;
	np = pp = NULL;
	LOCK_INIT(npl);
	LOCK_INIT(ppl);
	nlsnp = plsnp = NULL;
	mpf = dbp->mpf;
	ret = 0;

	/*
	 * Retrieve the one/two pages.  The caller must have them locked
	 * because the parent is latched. For a remove, we may need
	 * two pages (the before and after).  For an add, we only need one
	 * because, the split took care of the prev.
	 */
	if (pagep->next_pgno != PGNO_INVALID) {
		if (((np = otherp) == NULL ||
		    PGNO(otherp) != pagep->next_pgno) &&
		    (ret = __memp_fget(mpf, &pagep->next_pgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &np)) != 0) {
			ret = __db_pgerr(dbp, pagep->next_pgno, ret);
			goto err;
		}
		nlsnp = &np->lsn;
	}
	if (pagep->prev_pgno != PGNO_INVALID) {
		if (((pp = otherp) == NULL ||
		    PGNO(otherp) != pagep->prev_pgno) &&
		    (ret = __memp_fget(mpf, &pagep->prev_pgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &pp)) != 0) {
			ret = __db_pgerr(dbp, pagep->prev_pgno, ret);
			goto err;
		}
		plsnp = &pp->lsn;
	}

	/* Log the change. */
	if (DBC_LOGGING(dbc)) {
		if ((ret = __db_relink_log(dbp, dbc->txn, &ret_lsn, 0,
		    pagep->pgno, new_pgno, pagep->prev_pgno, plsnp,
		    pagep->next_pgno, nlsnp)) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(ret_lsn);
	if (np != NULL)
		np->lsn = ret_lsn;
	if (pp != NULL)
		pp->lsn = ret_lsn;

	/*
	 * Modify and release the two pages.
	 */
	if (np != NULL) {
		if (new_pgno == PGNO_INVALID)
			np->prev_pgno = pagep->prev_pgno;
		else
			np->prev_pgno = new_pgno;
		if (np != otherp)
			ret = __memp_fput(mpf,
			    dbc->thread_info, np, dbc->priority);
		if ((t_ret = __TLPUT(dbc, npl)) != 0 && ret == 0)
			ret = t_ret;
		if (ret != 0)
			goto err;
	}

	if (pp != NULL) {
		if (new_pgno == PGNO_INVALID)
			pp->next_pgno = pagep->next_pgno;
		else
			pp->next_pgno = new_pgno;
		if (pp != otherp)
			ret = __memp_fput(mpf,
			    dbc->thread_info, pp, dbc->priority);
		if ((t_ret = __TLPUT(dbc, ppl)) != 0 && ret == 0)
			ret = t_ret;
		if (ret != 0)
			goto err;
	}
	return (0);

err:	if (np != NULL && np != otherp)
		(void)__memp_fput(mpf, dbc->thread_info, np, dbc->priority);
	if (pp != NULL && pp != otherp)
		(void)__memp_fput(mpf, dbc->thread_info, pp, dbc->priority);
	return (ret);
}

/*
 * __db_move_metadata -- move a meta data page to a lower page number.
 * The meta data page must be exclusively latched on entry.
 *
 * PUBLIC: int __db_move_metadata __P((DBC *, DBMETA **, DB_COMPACT *));
 */
int
__db_move_metadata(dbc, metap, c_data)
	DBC *dbc;
	DBMETA **metap;
	DB_COMPACT *c_data;
{
	BTREE *bt;
	DB *dbp, *mdbp;
	DB_LOCK handle_lock;
	HASH *ht;
	int ret, t_ret;

	dbp = dbc->dbp;

	c_data->compact_pages_examine++;
	if ((ret = __db_exchange_page(dbc,
	     (PAGE**)metap, NULL, PGNO_INVALID, DB_EXCH_FREE)) != 0)
		return (ret);

	if (PGNO(*metap) == dbp->meta_pgno)
		return (0);

	if ((ret = __db_master_open(dbp,
	     dbc->thread_info, dbc->txn, dbp->fname, 0, 0, &mdbp)) != 0)
		return (ret);

	dbp->meta_pgno = PGNO(*metap);

	if ((ret = __db_master_update(mdbp, dbp, dbc->thread_info,
	     dbc->txn, dbp->dname, dbp->type, MU_MOVE, NULL, 0)) != 0)
		goto err;

	/*
	 * The handle lock for subdb's depends on the metadata page number:
	 * swap the old one for the new one.
	 */
	if (STD_LOCKING(dbc)) {
		/*
		 * If this dbp is still in an opening transaction we need to
		 * change its lock in the event.
		 */
		if (dbp->cur_txn != NULL)
			__txn_remlock(dbp->env,
			    dbp->cur_txn, &dbp->handle_lock, DB_LOCK_INVALIDID);

		handle_lock = dbp->handle_lock;
		if ((ret = __fop_lock_handle(dbp->env, dbp,
		    dbp->cur_locker != NULL ? dbp->cur_locker : dbp->locker,
		    dbp->cur_txn != NULL ? DB_LOCK_WRITE : DB_LOCK_READ,
		    NULL, 0)) != 0)
			goto err;

		/* Move all the other handles to the new lock. */
		if ((ret = __lock_change(dbp->env,
		    &handle_lock, &dbp->handle_lock)) != 0)
			goto err;

		/* Reregister the event. */
		if (dbp->cur_txn != NULL)
			ret = __txn_lockevent(dbp->env,
			    dbp->cur_txn, dbp, &dbp->handle_lock, dbp->locker);
	}
	if (dbp->log_filename != NULL)
		dbp->log_filename->meta_pgno = dbp->meta_pgno;
	if (dbp->type == DB_HASH) {
		ht = dbp->h_internal;
		ht->meta_pgno = dbp->meta_pgno;
		ht->revision = ++dbp->mpf->mfp->revision;
	} else {
		bt = dbp->bt_internal;
		bt->bt_meta = dbp->meta_pgno;
		bt->revision = ++dbp->mpf->mfp->revision;
	}

err:	if ((t_ret = __db_close(mdbp, dbc->txn, DB_NOSYNC)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}
