/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/hash.h"
#include "dbinc/lock.h"
#include "dbinc/txn.h"
#include "dbinc/mp.h"

static int __ham_copy_data __P((DBC *, PAGE *, DB_COMPACT *, int *));
static int __ham_truncate_overflow __P((DBC *, u_int32_t, DB_COMPACT *, int *));

/*
 * __ham_compact_int -- internal HASH compaction routine.
 *
 * PUBLIC: int __ham_compact_int __P((DBC *,
 * PUBLIC:      DBT *, DBT *, u_int32_t, DB_COMPACT *, int *, u_int32_t));
 */
int
__ham_compact_int(dbc, start, stop, factor, c_data, donep, flags)
	DBC *dbc;
	DBT *start, *stop;
	u_int32_t factor;
	DB_COMPACT *c_data;
	int *donep;
	u_int32_t flags;
{
	DB *dbp;
	DB_MPOOLFILE *mpf;
	HASH_CURSOR *hcp;
	db_pgno_t origpgno, pgno;
	int check_trunc, pgs_done, ret, t_ret;
	u_int32_t empty_buckets, i, stop_bucket;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	hcp = (HASH_CURSOR *)dbc->internal;
	pgs_done = 0;
	empty_buckets = 0;
	check_trunc = c_data->compact_truncate != PGNO_INVALID;
	if ((ret = __ham_get_meta(dbc)) != 0)
		return (ret);

	if (stop != NULL && stop->size != 0)
		stop_bucket = *(u_int32_t *)stop->data;
	else
		stop_bucket = hcp->hdr->max_bucket;

	if (start != NULL && start->size != 0)
		hcp->bucket = *(u_int32_t *)start->data;
	else
		hcp->bucket = 0;

	for (; hcp->bucket <= stop_bucket && ret == 0; hcp->bucket++) {
		/*
		 * For each bucket first move records toward the head of
		 * the bucket.
		 */
		hcp->indx = NDX_INVALID;
		F_CLR(hcp, H_ISDUP);
		hcp->pgno = BUCKET_TO_PAGE(hcp, hcp->bucket);
		pgno = PGNO_INVALID;
		ret = __ham_item_next(dbc, DB_LOCK_WRITE, &pgno);

		/*
		 * If the bucket is empty, just note it, otherwise process it.
		 * If there are any records there must be some in the head
		 * of the bucket.
		 */
		if (ret == DB_NOTFOUND ) {
			empty_buckets++;
			c_data->compact_pages_examine++;
			DB_ASSERT(dbp->env,
			    PREV_PGNO(hcp->page) == PGNO_INVALID &&
			    NEXT_PGNO(hcp->page) == PGNO_INVALID);
			goto err;
		} else if (ret != 0)
			break;
		c_data->compact_pages_examine++;

		if (NEXT_PGNO(hcp->page) != PGNO_INVALID) {
			if ((ret =
			    __ham_compact_bucket(dbc, c_data, &pgs_done)) != 0)
				goto err;
			pgno = PGNO_INVALID;
			if ((ret = __ham_item(dbc, DB_LOCK_WRITE, &pgno)) != 0)
				goto err;
		}

		/*
		 * Loop through the items in this page in the bucket and process
		 * overflow records and off page duplicate sets.
		 */
		while (ret == 0) {
			/* Handle off page duplicate trees. */
			if (pgno == PGNO_INVALID)
				goto no_opd;
			if (check_trunc &&
			    pgno > c_data->compact_truncate) {
				c_data->compact_pages_examine++;
				/*
				 * Truncate this page if possible.
				 * We must update the parent here
				 * because the page number is
				 * not aligned.
				 */
				if ((ret = __memp_dirty(mpf, &hcp->page,
				    dbc->thread_info,
				    dbc->txn, dbc->priority, 0)) != 0)
					break;
				origpgno = pgno;
				if ((ret = __db_truncate_root(dbc, hcp->page,
				    H_DATAINDEX(hcp->indx), &pgno, 0)) != 0)
					break;
				if (pgno != origpgno) {
					memcpy(HOFFDUP_PGNO(H_PAIRDATA(dbp,
					    hcp->page, hcp->indx)),
					    &pgno, sizeof(db_pgno_t));
					pgs_done++;
					c_data->compact_pages--;
				}
			}
			/*
			 * Compact the off page duplicate tree.
			 */
			if ((ret = __bam_compact_opd(dbc,
			    pgno, NULL, factor, c_data, &pgs_done)) != 0)
				break;

no_opd:			if (check_trunc && HPAGE_PTYPE(H_PAIRDATA(
			    dbp, hcp->page, hcp->indx)) == H_OFFPAGE) {
				/* This is an overflow chain. */
				if ((ret = __ham_truncate_overflow(dbc,
				    H_DATAINDEX(hcp->indx),
				    c_data, &pgs_done)) != 0)
					break;
			}

			/* Check for an overflow key. */
			if (check_trunc && HPAGE_PTYPE(H_PAIRKEY(
			    dbp, hcp->page, hcp->indx)) == H_OFFPAGE) {
				/* This is an overflow chain. */
				if ((ret = __ham_truncate_overflow(dbc,
				    H_KEYINDEX(hcp->indx),
				    c_data, &pgs_done)) != 0)
					break;
			}

			pgno = PGNO_INVALID;
			ret = __ham_item_next(dbc, DB_LOCK_WRITE, &pgno);
		}

err:		if (hcp->page != NULL &&
		    (t_ret = __memp_fput(mpf, dbc->thread_info,
		    hcp->page, dbc->priority)) != 0 && ret == 0)
			ret = t_ret;
		if (ret == DB_NOTFOUND)
			ret = 0;
		hcp->page = NULL;
		hcp->pgno = pgno = PGNO_INVALID;
		/*
		 * If we are in an auto-transaction and we updated something
		 * return to the caller to commit this transaction to
		 * avoid holding locks. Otherwise process the next bucket.
		 * We can drop the lock if we did not do anything.
		 * We always must commit the txn if we are in MVCC
		 * as we have dirtied the hash buckets.
		 */
		if (ret == 0 &&
		    atomic_read(&dbp->mpf->mfp->multiversion) == 0 &&
		    (pgs_done == 0 || dbc->txn == NULL))
			ret = __LPUT(dbc, hcp->lock);
		else if (LF_ISSET(DB_AUTO_COMMIT)) {
			if (ret == 0)
				hcp->bucket++;
			break;
		}
	}
	/*
	 * If we saw any empty buckets and we are freeing space we
	 * want to contract the table before dropping the metadata
	 * page. Wait till we are done with everything else as we
	 * need to get an exclusive lock on the metadata page.
	 */
	if (ret == 0 && empty_buckets != 0 && LF_ISSET(DB_FREE_SPACE)) {
		for (i = 0; i < empty_buckets && hcp->hdr->max_bucket > 2; i++)
			if ((ret = __ham_contract_table(dbc, c_data)) != 0)
				break;
	}

	if (ret == 0)
		ret = __db_retcopy(dbp->env, start, &hcp->bucket,
		    sizeof(hcp->bucket), &start->data, &start->ulen);
	(void)__ham_release_meta(dbc);
	c_data->compact_empty_buckets += empty_buckets;
	if (hcp->bucket > stop_bucket)
		*donep = 1;
	return (ret);
}

/*
 * __ham_compact_bucket -- move data to as few pages as possible.
 *
 * PUBLIC: int __ham_compact_bucket __P((DBC *, DB_COMPACT *, int *));
 */
int
__ham_compact_bucket(dbc, c_data, pgs_donep)
	DBC *dbc;
	DB_COMPACT *c_data;
	int *pgs_donep;
{
	DB *dbp;
	DB_MPOOLFILE *mpf;
	HASH_CURSOR *hcp;
	PAGE *pg;
	db_pgno_t pgno;
	int check_trunc, ret, t_ret;

	hcp = (HASH_CURSOR *)dbc->internal;
	dbp = dbc->dbp;
	mpf = dbp->mpf;
	pg = hcp->page;
	check_trunc = c_data->compact_truncate != PGNO_INVALID;
	ret = 0;

	pgno = hcp->pgno;
	do {
		if (pg == NULL && (ret = __memp_fget(mpf, &pgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &pg)) != 0)
			break;
		/* Sort any unsorted pages before adding to the page. */
		if (TYPE(pg) == P_HASH_UNSORTED) {
			if ((ret = __ham_sort_page_cursor(dbc, pg)) != 0)
				break;
			(*pgs_donep)++;
		}

		/* If this is not the head try to move it to a lower page. */
		if (check_trunc && PREV_PGNO(pg) != PGNO_INVALID  &&
		    PGNO(pg) > c_data->compact_truncate &&
		    (ret = __db_exchange_page(dbc, &pg,
		    hcp->page, PGNO_INVALID, DB_EXCH_FREE)) != 0)
			break;
		if (pgno != PGNO(pg))
			(*pgs_donep)++;

		if (NEXT_PGNO(pg) == PGNO_INVALID)
			break;
		if ((ret = __ham_copy_data(dbc, pg, c_data, pgs_donep)) != 0)
			break;
		pgno = NEXT_PGNO(pg);
		if (pg != hcp->page && (ret = __memp_fput(mpf,
		     dbc->thread_info, pg, dbc->priority)) != 0)
			break;
		pg = NULL;
	} while (pgno != PGNO_INVALID);

	if (pg != NULL && pg != hcp->page &&
	    (t_ret = __memp_fput(mpf, dbc->thread_info, pg, dbc->priority)) &&
	    ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __ham_copy_data -- copy as many records as possible from next page
 */
static int
__ham_copy_data(dbc, pg, c_data, pgs_donep)
	DBC *dbc;
	PAGE *pg;
	DB_COMPACT *c_data;
	int *pgs_donep;
{
	DB *dbp;
	DBC *newdbc;
	DBT data, key;
	DB_MPOOLFILE *mpf;
	HASH_CURSOR *hcp, *ncp;
	PAGE *nextpage;
	db_pgno_t origpgno;
	int i, nument, records, ret, t_ret;
	u_int32_t len;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	hcp = (HASH_CURSOR *)dbc->internal;
	records = 0;

	if ((ret = __dbc_dup(dbc, &newdbc, 0)) != 0)
		return (ret);
	ncp = (HASH_CURSOR *)newdbc->internal;
	ncp->hdr = hcp->hdr;

	/*
	 * Copy data to the front of the bucket. Loop until either we
	 * have not replaced the next page or there is no next page.
	 * If the next page was not removed then it still has data
	 * on it.
	 */
	origpgno = PGNO_INVALID;
	while (origpgno != NEXT_PGNO(pg) &&
	    (origpgno = NEXT_PGNO(pg)) != PGNO_INVALID) {

		if ((ret = __memp_fget(mpf, &NEXT_PGNO(pg), dbc->thread_info,
		    dbc->txn, DB_MPOOL_DIRTY, &nextpage)) != 0)
			break;

		c_data->compact_pages_examine++;
		ncp->page = nextpage;
		ncp->pgno = PGNO(nextpage);
		ncp->indx = 0;
		memset(&key, 0, sizeof(key));
		memset(&data, 0, sizeof(data));
		nument = NUM_ENT(nextpage);
		DB_ASSERT(dbp->env, nument != 0);
		for (i = 0; i < nument; i += 2) {
			len = LEN_HITEM(dbp, nextpage, dbp->pgsize, 0) +
			    LEN_HITEM(dbp, nextpage, dbp->pgsize, 1) +
			    2 * sizeof(db_indx_t);
			if (P_FREESPACE(dbp, pg) < len)
				continue;

			if ((ret =
			    __ham_copypair(dbc, nextpage, 0, pg, NULL, 1)) != 0)
				break;

			records++;
			if ((ret = __ham_del_pair(newdbc,
			    HAM_DEL_IGNORE_OFFPAGE, pg)) != 0)
				break;
			if (!STD_LOCKING(dbc)) {
				if ((ret = __ham_dirty_meta(dbc, 0)) != 0)
					return (ret);
				++hcp->hdr->nelem;
			}
		}
		/*
		 * If we moved all the records then __ham_del_pair will
		 * have deleted the nextpage.
		 */
		if (records >= nument/2) {
			c_data->compact_pages_examine++;
			c_data->compact_pages_free++;
			COMPACT_TRUNCATE(c_data);
		}
		if (ncp->page != NULL &&
		    (t_ret = __memp_fput(mpf, dbc->thread_info,
		    ncp->page, dbc->priority)) != 0 && ret == 0)
			ret = t_ret;
		ncp->page = NULL;
		ncp->pgno = PGNO_INVALID;
	}

	/*
	 * If __ham_del_pair freed a page then we needed to dirty the metapage
	 * and it could change so we need to copy it back to hcp.
	 */
	hcp->hdr = ncp->hdr;
	ncp->hdr = NULL;
	if ((t_ret = __ham_release_meta(newdbc)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __dbc_close(newdbc)) != 0 && ret == 0)
		ret = t_ret;
	if (records != 0)
		(*pgs_donep)++;
	return (ret);
}

/*
 * __ham_truncate_overflow -- try to truncate pages from an overflow chain.
 */
static int
__ham_truncate_overflow(dbc, indx, c_data, pgs_done)
	DBC *dbc;
	u_int32_t indx;
	DB_COMPACT *c_data;
	int *pgs_done;
{
	DB *dbp;
	HASH_CURSOR *hcp;
	db_pgno_t origpgno, pgno;
	int ret;

	hcp = (HASH_CURSOR *)dbc->internal;
	dbp = dbc->dbp;
	memcpy(&pgno,
	    HOFFPAGE_PGNO(P_ENTRY(dbp, hcp->page, indx)), sizeof(db_pgno_t));
	if (pgno > c_data->compact_truncate) {
		c_data->compact_pages_examine++;
		origpgno = pgno;
		if ((ret = __memp_dirty(dbp->mpf, &hcp->page,
		    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0)
			return (ret);
		if ((ret =
		     __db_truncate_root(dbc, hcp->page, indx, &pgno, 0)) != 0)
			return (ret);
		if (pgno != origpgno) {
			memcpy(HOFFPAGE_PGNO(P_ENTRY(dbp, hcp->page, indx)),
			    &pgno, sizeof(db_pgno_t));
			(*pgs_done)++;
			c_data->compact_pages--;
		}
	}
	if ((ret = __db_truncate_overflow(dbc, pgno, NULL, c_data)) != 0)
		return (ret);
	return (0);
}

#ifdef HAVE_FTRUNCATE
/*
 * __ham_compact_hash -- compact the hash table.
 * PUBLIC: int __ham_compact_hash __P((DB *,
 * PUBLIC:       DB_THREAD_INFO *, DB_TXN *, DB_COMPACT *));
 */
int
__ham_compact_hash(dbp, ip, txn, c_data)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	DB_COMPACT *c_data;
{
	DBC *dbc;
	DB_LOCK lock;
	HASH_CURSOR *hcp;
	HMETA *meta;
	PAGE *oldpage;
	db_pgno_t free_pgno, last_pgno, pgno, start_pgno;
	int flags, local_txn, ret, t_ret;
	u_int32_t bucket, i, size;

	local_txn = IS_DB_AUTO_COMMIT(dbp, txn);
	oldpage = NULL;
	dbc = NULL;
	LOCK_INIT(lock);

	if (local_txn &&
	    (ret = __txn_begin(dbp->env, ip, txn, &txn, 0)) != 0)
		return (ret);

	if ((ret = __db_cursor(dbp, ip, txn, &dbc, 0)) != 0)
		goto err1;
	hcp = (HASH_CURSOR *)dbc->internal;

	if ((ret = __ham_get_meta(dbc)) != 0 ||
	    (ret = __ham_dirty_meta(dbc, 0)) != 0)
		goto err1;

	meta = hcp->hdr;

	LOCK_CHECK_OFF(ip);

	/*
	 * Find contiguous lower numbered pages for each hash table segment.
	 */
	for (i = 0; i < NCACHED && meta->spares[i] != PGNO_INVALID; i++) {
		if (i == 0) {
			bucket = 0;
			size = 1;
		} else {
			bucket = 1 << (i - 1);
			size = bucket;
		}
		start_pgno = meta->spares[i] + bucket;
		if ((ret = __db_find_free(dbc, P_HASH,
		    size, start_pgno, &free_pgno)) != 0) {
			if (ret != DB_NOTFOUND)
				break;
			ret = 0;
			continue;
		}
		if (DBC_LOGGING(dbc)) {
			if ((ret = __ham_changeslot_log(dbp,
			    dbc->txn, &LSN(meta),
			    0, &LSN(meta), i, start_pgno, free_pgno)) != 0)
				break;
		} else
			LSN_NOT_LOGGED(LSN(meta));
		last_pgno = free_pgno + bucket;
		/*
		 * March through the list swapping pages.  If the page is
		 * empty we just need to free it.  If we are just sliding
		 * things down don't free the pages that will be reused.
		 * Note that __db_exchange_page returns the new page so
		 * we must put it.
		 */
		for (pgno = start_pgno;
		    pgno < start_pgno + size; pgno++, free_pgno++) {
			if ((ret = __db_lget(dbc,
			    LCK_COUPLE, pgno, DB_LOCK_WRITE, 0, &lock)) != 0)
				goto err;
			if ((ret = __memp_fget(dbp->mpf, &pgno,
			     dbc->thread_info, dbc->txn,
			     DB_MPOOL_CREATE | DB_MPOOL_DIRTY, &oldpage)) != 0)
				goto err;
			if (NUM_ENT(oldpage) != 0) {
				if (pgno < last_pgno)
					flags = 0;
				else
					flags = DB_EXCH_FREE;
				if ((ret = __db_exchange_page(dbc,
				    &oldpage, NULL, free_pgno, flags)) != 0)
					goto err;
			} else if (pgno >= last_pgno) {
				if ((ret = __db_free(dbc, oldpage, 0)) != 0)
					goto err;
				COMPACT_TRUNCATE(c_data);
				oldpage = NULL;
			}
			if (oldpage != NULL && (ret = __memp_fput(dbp->mpf,
			    dbc->thread_info, oldpage, dbc->priority)) != 0)
				goto err;
			ret = 0;
			oldpage = NULL;
			c_data->compact_pages_examine++;
		}
		meta->spares[i] = free_pgno - (size + bucket);
	}
	if (ret == 0 && F_ISSET(dbp, DB_AM_SUBDB) &&
	    PGNO(hcp->hdr) > c_data->compact_truncate)
		ret = __db_move_metadata(dbc, (DBMETA**)&hcp->hdr, c_data);

err:	if (oldpage != NULL && (t_ret = __memp_fput(dbp->mpf,
	    dbc->thread_info, oldpage, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, lock)) != 0 && ret == 0)
		ret = t_ret;
	LOCK_CHECK_ON(ip);
err1:	if (dbc != NULL) {
		if ((t_ret = __ham_release_meta(dbc)) != 0 && ret == 0)
			ret = t_ret;
		if ((t_ret = __dbc_close(dbc)) != 0 && ret == 0)
			ret = t_ret;
	}
	if (local_txn && (t_ret = (ret == 0 ?
	    __txn_commit(txn, 0) : __txn_abort(txn))) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}
#endif
