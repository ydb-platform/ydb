/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/heap.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"

static int  __heap_bulk __P((DBC *, DBT *, u_int32_t));
static int  __heap_getpage __P((DBC *, u_int32_t, u_int8_t *));
static int  __heapc_close __P((DBC *, db_pgno_t, int *));
static int  __heapc_del __P((DBC *, u_int32_t));
static int  __heapc_destroy __P((DBC *));
static int  __heapc_get __P((DBC *, DBT *, DBT *, u_int32_t, db_pgno_t *));
static int  __heapc_put __P((DBC *, DBT *, DBT *, u_int32_t, db_pgno_t *));
static int  __heapc_reloc __P((DBC *, DBT *, DBT *));
static int  __heapc_reloc_partial __P((DBC *, DBT *, DBT *));
static int  __heapc_split __P((DBC *, DBT *, DBT *, int));

/*
 * Acquire a new page/lock.  If we are already holding a page and a lock
 * we discard those and get the new ones.  In this case we can use
 * LCK_COUPLE to save trips to lock manager.  If we are not holding a page or
 * locks, we just get a new lock and page. Lock release done with a
 * transactional lock put.
 */
#undef  ACQUIRE
#define	ACQUIRE(dbc, mode, lpgno, lock, fpgno, pagep, flags, mflags, ret) do { \
	DB_MPOOLFILE *__mpf = (dbc)->dbp->mpf;				\
	if ((pagep) != NULL) {						\
		ret = __memp_fput(__mpf,				\
		    (dbc)->thread_info, pagep, dbc->priority);		\
		pagep = NULL;						\
	}								\
	if ((ret) == 0 && STD_LOCKING(dbc))				\
		ret = __db_lget(dbc,					\
		    LOCK_ISSET(lock) ? LCK_COUPLE : 0,			\
		    lpgno, mode, flags, &(lock));			\
	if ((ret) == 0)							\
		ret = __memp_fget(__mpf, &(fpgno),			\
		    (dbc)->thread_info, (dbc)->txn, mflags, &(pagep));	\
} while (0)

/* Acquire a new page/lock for a heap cursor */
#undef  ACQUIRE_CUR
#define	ACQUIRE_CUR(dbc, mode, p, flags, mflags, ret) do {		\
	HEAP_CURSOR *__cp = (HEAP_CURSOR *)(dbc)->internal;		\
	if (p != __cp->pgno)						\
		__cp->pgno = PGNO_INVALID;				\
	ACQUIRE(dbc, mode, p, __cp->lock, p, __cp->page, flags, mflags, ret); \
	if ((ret) == 0) {						\
		__cp->pgno = p;						\
		__cp->lock_mode = (mode);				\
	}								\
} while (0)

/* Discard the current page/lock for a cursor, indicate txn lock release */
#undef  DISCARD
#define	DISCARD(dbc, pagep, lock, tlock, ret) do {			\
	DB_MPOOLFILE *__mpf = (dbc)->dbp->mpf;				\
	int __t_ret;							\
	__t_ret = 0;							\
	if ((pagep) != NULL) {						\
		__t_ret = __memp_fput(__mpf,				\
		    (dbc)->thread_info, pagep, dbc->priority);		\
		pagep = NULL;						\
	}								\
	if (__t_ret != 0 && (ret) == 0)					\
		ret = __t_ret;						\
	if (tlock == 1)							\
		__t_ret = __TLPUT((dbc), lock);				\
	else								\
		__t_ret = __LPUT((dbc), lock);				\
	if (__t_ret != 0 && (ret) == 0)					\
		ret = __t_ret;						\
} while (0)

/*
 * __heapc_init --
 *	Initialize the access private portion of a cursor
 *
 * PUBLIC: int __heapc_init __P((DBC *));
 */
int
__heapc_init(dbc)
	DBC *dbc;
{
	ENV *env;
	int ret;

	env = dbc->env;

	if (dbc->internal == NULL)
		if ((ret = __os_calloc(
		    env, 1, sizeof(HEAP_CURSOR), &dbc->internal)) != 0)
			return (ret);

	/* Initialize methods. */
	dbc->close = dbc->c_close = __dbc_close_pp;
	dbc->cmp = __dbc_cmp_pp;
	dbc->count = dbc->c_count = __dbc_count_pp;
	dbc->del = dbc->c_del = __dbc_del_pp;
	dbc->dup = dbc->c_dup = __dbc_dup_pp;
	dbc->get = dbc->c_get = __dbc_get_pp;
	dbc->pget = dbc->c_pget = __dbc_pget_pp;
	dbc->put = dbc->c_put = __dbc_put_pp;
	dbc->am_bulk = __heap_bulk;
	dbc->am_close = __heapc_close;
	dbc->am_del = __heapc_del;
	dbc->am_destroy = __heapc_destroy;
	dbc->am_get = __heapc_get;
	dbc->am_put = __heapc_put;
	dbc->am_writelock = NULL;

	return (0);
}

static int
__heap_bulk(dbc, data, flags)
	DBC *dbc;
	DBT *data;
	u_int32_t flags;
{
	DB *dbp;
	DB_HEAP_RID prev_rid, rid;
	DBT sdata;
	HEAP_CURSOR *cp;
	HEAPHDR *hdr;
	HEAPSPLITHDR *shdr;
	PAGE *pg;
	db_lockmode_t lock_type;
	int is_key, ret;
	int32_t *offp;
	u_int32_t data_size, key_size, needed, space;
	u_int8_t *dbuf, *np;

	ret = 0;
	dbp = dbc->dbp;
	cp = (HEAP_CURSOR *)dbc->internal;
	hdr = NULL;
	shdr = NULL;

	/* Check for additional bits for locking */
	if (F_ISSET(dbc, DBC_RMW))
		lock_type = DB_LOCK_WRITE;
	else
		lock_type = DB_LOCK_READ;

	/*
	 * np is the next place to copy things into the buffer.
	 * dbuf always stays at the beginning of the buffer.
	 */
	dbuf = data->data;
	np = dbuf;

	/* Keep track of space that is left.  There is a termination entry */
	space = data->ulen;
	space -= sizeof(*offp);

	/* Build the offset/size table from the end up. */
	offp = (int32_t *)((u_int8_t *)dbuf + data->ulen);
	offp--;

	/*
	 * key_size and data_size hold the 32-bit aligned size of the key and
	 * data values written to the buffer.
	 */
	key_size = DB_ALIGN(DB_HEAP_RID_SZ, sizeof(u_int32_t));
	data_size = 0;

	/* is_key indicates whether keys are returned. */
	is_key = LF_ISSET(DB_MULTIPLE_KEY) ? 1 : 0;

next_pg:
	rid.indx = cp->indx;
	rid.pgno = cp->pgno;
	pg = cp->page;

	/*
	 * Write records to the buffer, in the format needed by the DB_MULTIPLE
	 * macros.  For a description of the data layout, see db.h.
	 */
	do {
		if (HEAP_OFFSETTBL(dbp, pg)[rid.indx] == 0)
			continue;
		hdr = (HEAPHDR *)P_ENTRY(dbp, pg, rid.indx);
		/*
		 * If this is a split record and not the first piece of the
		 * record, skip it.
		 */
		if (F_ISSET(hdr, HEAP_RECSPLIT) &&
		    !F_ISSET(hdr, HEAP_RECFIRST))
			continue;

		/*
		 * Calculate how much space is needed to add this record.  If
		 * there's not enough, we're done.  If we haven't written any
		 * data to the buffer, or if we are doing a DBP->get, return
		 * DB_BUFFER_SMALL.
		 */
		needed = 0;
		if (is_key)
			needed = 2 * sizeof(*offp) + key_size;
		if (F_ISSET(hdr, HEAP_RECSPLIT)) {
			shdr = (HEAPSPLITHDR *)hdr;
			data_size = DB_ALIGN(shdr->tsize, sizeof(u_int32_t));
		} else
			data_size = DB_ALIGN(hdr->size, sizeof(u_int32_t));
		needed += 2 * sizeof(*offp) + data_size;

		if (needed > space) {
			if (np == dbuf || F_ISSET(dbc, DBC_FROM_DB_GET)) {
				data->size = (u_int32_t)DB_ALIGN(
				    needed + data->ulen - space, 1024);
				return (DB_BUFFER_SMALL);
			}
			break;
		}

		if (is_key) {
			memcpy(np, &rid, key_size);
			*offp-- = (int32_t)(np - dbuf);
			*offp-- = (int32_t)DB_HEAP_RID_SZ;
			np += key_size;
		}

		if (F_ISSET(hdr, HEAP_RECSPLIT)) {
			/*
			 * Use __heapc_gsplit to write a split record to the
			 * return buffer.  gsplit will return any fetched pages
			 * to the cache, but will leave the cursor's current
			 * page alone.
			 */
			memset(&sdata, 0, sizeof(DBT));
			sdata.data = np;
			sdata.size = sdata.ulen = shdr->tsize;
			sdata.flags = DB_DBT_USERMEM;
			/* gsplit expects the cursor to be positioned. */
			cp->pgno = rid.pgno;
			cp->indx = rid.indx;
			if ((ret = __heapc_gsplit(
			    dbc, &sdata, NULL, NULL)) != 0)
				return (ret);
		} else {
			memcpy(np,
			    (u_int8_t *)hdr + sizeof(HEAPHDR), hdr->size);
		}
		*offp-- = (int32_t)(np - dbuf);
		if (F_ISSET(hdr, HEAP_RECSPLIT))
			*offp-- = (int32_t)shdr->tsize;
		else
			*offp-- = (int32_t)hdr->size;
		np += data_size;
		space -= needed;
		prev_rid = rid;

		/*
		 * The data and "metadata" ends of the buffer should never
		 * overlap.
		 */
		DB_ASSERT(dbp->env, (void *)np <= (void *)offp);
	} while (++rid.indx < NUM_ENT(pg));

	/* If we are off the page then try the next page. */
	if (rid.indx >= NUM_ENT(pg)) {
		rid.pgno++;
		ACQUIRE_CUR(dbc, lock_type, rid.pgno, 0, 0, ret);
		if (ret == 0) {
			cp->indx = 0;
			goto next_pg;
		} else if (ret != DB_PAGE_NOTFOUND)
			return (ret);
	}

	DB_ASSERT(dbp->env, (ret == 0 || ret == DB_PAGE_NOTFOUND));
	cp->indx = prev_rid.indx;
	cp->pgno = prev_rid.pgno;

	*offp = -1;

	return (0);
}

static int
__heapc_close(dbc, root_pgno, rmroot)
	DBC *dbc;
	db_pgno_t root_pgno;
	int *rmroot;
{
	DB_MPOOLFILE *mpf;
	HEAP_CURSOR *cp;
	int ret;

	COMPQUIET(root_pgno, 0);
	COMPQUIET(rmroot, 0);

	cp = (HEAP_CURSOR *)dbc->internal;
	mpf = dbc->dbp->mpf;
	ret = 0;

	/* Release the page/lock held by the cursor. */
	DISCARD(dbc, cp->page, cp->lock, 1, ret);
	if (ret == 0 && !LOCK_ISSET(cp->lock))
		cp->lock_mode = DB_LOCK_NG;

	return (ret);
}

static int
__heapc_del(dbc, flags)
	DBC *dbc;
	u_int32_t flags;
{
	DB *dbp;
	DB_HEAP_RID next_rid, orig_rid;
	DB_MPOOLFILE *mpf;
	DBT hdr_dbt, log_dbt;
	HEAP *h;
	HEAPHDR *hdr;
	HEAPPG *rpage;
	HEAP_CURSOR *cp;
	db_pgno_t region_pgno;
	int oldspacebits, ret, spacebits, t_ret;
	u_int16_t data_size, size;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	h = dbp->heap_internal;
	cp = (HEAP_CURSOR *)dbc->internal;
	rpage = NULL;
	COMPQUIET(flags, 0);

	/*
	 * We need to be able to reset the cursor after deleting a record split
	 * across multiple pages.
	 */
	orig_rid.pgno = cp->pgno;
	orig_rid.indx = cp->indx;

	/*
	 * This code is always called with a page lock but no page.
	 */
	DB_ASSERT(dbp->env, cp->page == NULL);

	/* We have a read lock, but need a write lock. */
start:	if (STD_LOCKING(dbc) && (ret = __db_lget(dbc,
	    LCK_COUPLE, cp->pgno, DB_LOCK_WRITE, 0, &cp->lock)) != 0)
		return (ret);

	if ((ret = __memp_fget(mpf, &cp->pgno,
	    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &cp->page)) != 0)
		return (ret);

	HEAP_CALCSPACEBITS(dbp, HEAP_FREESPACE(dbp, cp->page), oldspacebits);

	hdr = (HEAPHDR *)P_ENTRY(dbp, cp->page, cp->indx);
	data_size = DB_ALIGN(hdr->size, sizeof(u_int32_t));
	size = data_size + HEAP_HDRSIZE(hdr);
	if (size < sizeof(HEAPSPLITHDR))
		size = sizeof(HEAPSPLITHDR);
	if (F_ISSET(hdr, HEAP_RECSPLIT) && !F_ISSET(hdr, HEAP_RECLAST)) {
		next_rid.pgno = F_ISSET(hdr, HEAP_RECLAST) ?
			PGNO_INVALID : ((HEAPSPLITHDR *)hdr)->nextpg;
		next_rid.indx = F_ISSET(hdr, HEAP_RECLAST) ?
			PGNO_INVALID : ((HEAPSPLITHDR *)hdr)->nextindx;
	} else {
		next_rid.pgno = PGNO_INVALID;
		next_rid.indx = 0;
	}

	/* Log the deletion. */
	if (DBC_LOGGING(dbc)) {
		hdr_dbt.data = hdr;
		hdr_dbt.size = HEAP_HDRSIZE(hdr);
		log_dbt.data = (u_int8_t *)hdr + hdr_dbt.size;
		log_dbt.size = data_size;
		if ((ret = __heap_addrem_log(dbp, dbc->txn, &LSN(cp->page),
		    0, DB_REM_HEAP, cp->pgno, (u_int32_t)cp->indx,
		    size, &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(cp->page));

	if ((ret = __heap_ditem(dbc, cp->page, cp->indx, size)) != 0)
		goto err;

	/*
	 * If the deleted item lived in a region prior to our current, back up
	 * the current region, giving us a chance to reuse the newly available
	 * space on the next insert.
	 */
	region_pgno = HEAP_REGION_PGNO(dbp, cp->pgno);
	if (region_pgno < h->curregion)
		h->curregion = region_pgno;

	HEAP_CALCSPACEBITS(dbp, HEAP_FREESPACE(dbp, cp->page), spacebits);

	if (spacebits != oldspacebits) {
		/*
		 * Get the region page.  We never lock the region page, the data
		 * page lock locks the corresponding bits in the bitmap and
		 * latching serializes access.
		 */
		if ((ret = __memp_fget(mpf, &region_pgno,
		    dbc->thread_info, NULL, DB_MPOOL_DIRTY, &rpage)) != 0)
			goto err;
		HEAP_SETSPACE(dbp, rpage,
		    cp->pgno - region_pgno - 1, spacebits);
	}

err:	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	if (rpage != NULL && (t_ret = __memp_fput(mpf,
	    dbc->thread_info, rpage, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	rpage = NULL;

	if ((t_ret = __memp_fput(mpf,
	    dbc->thread_info, cp->page, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;

	cp->page = NULL;

	if (ret == 0 && next_rid.pgno != PGNO_INVALID) {
		cp->pgno = next_rid.pgno;
		cp->indx = next_rid.indx;
		goto start;
	}

	cp->pgno = orig_rid.pgno;
	cp->indx = orig_rid.indx;

	return (ret);
}

/*
 * __heap_ditem --
 *   Remove an item from a page.
 *
 * PUBLIC: int __heap_ditem
 * PUBLIC:   __P((DBC *, PAGE *, u_int32_t, u_int32_t));
 */
int
__heap_ditem(dbc, pagep, indx, nbytes)
	DBC *dbc;
	PAGE *pagep;
	u_int32_t indx, nbytes;
{
	DB *dbp;
	db_indx_t first, i, max, off, *offtbl, span;
	u_int8_t *src, *dest;

	dbp = dbc->dbp;

	DB_ASSERT(dbp->env, TYPE(pagep) == P_HEAP);
	DB_ASSERT(dbp->env, nbytes == DB_ALIGN(nbytes, sizeof(u_int32_t)));
	DB_ASSERT(dbp->env, nbytes >= sizeof(HEAPSPLITHDR));

	offtbl = (db_indx_t *)HEAP_OFFSETTBL(dbp, pagep);
	off = offtbl[indx];
	/*
	 * Find the lowest offset on the page, and adjust offsets that are about
	 * to be moved.  If the deleted item is the lowest offset on the page,

	 * everything will work, that is not a special case.
	 */
	max = HEAP_HIGHINDX(pagep);
	first = HOFFSET(pagep);
	for (i = 0; i <= max; i++) {
		if (offtbl[i] < off && offtbl[i] != 0)
			offtbl[i] += nbytes;
	}
	offtbl[indx] = 0;

	/*
	 * Coalesce free space at the beginning of the page.  Shift all the data
	 * preceding the deleted entry down, overwriting the deleted entry.
	 */
	src = (u_int8_t *)(pagep) + first;
	dest = src + nbytes;
	span = off - first;
	memmove(dest, src, span);
#ifdef DIAGNOSTIC
	memset(src, CLEAR_BYTE, nbytes);
#endif

	/* Update the page's metadata. */
	NUM_ENT(pagep)--;
	HOFFSET(pagep) += nbytes;
	if (indx < HEAP_FREEINDX(pagep))
		HEAP_FREEINDX(pagep) = indx;
	while (HEAP_HIGHINDX(pagep) > 0 && offtbl[HEAP_HIGHINDX(pagep)] == 0)
		HEAP_HIGHINDX(pagep)--;
	if (NUM_ENT(pagep) == 0)
		HEAP_FREEINDX(pagep) = 0;
	else if (HEAP_FREEINDX(pagep) > HEAP_HIGHINDX(pagep) + 1)
		HEAP_FREEINDX(pagep) = HEAP_HIGHINDX(pagep) + 1;

	return (0);
}

static int
__heapc_destroy(dbc)
	DBC *dbc;
{
	HEAP_CURSOR *cp;

	cp = (HEAP_CURSOR *)dbc->internal;
	__os_free(dbc->env, cp);
	dbc->internal = NULL;

	return (0);
}

/*
 * __heapc_get --
 *	Get using a cursor (heap).
 */
static int
__heapc_get(dbc, key, data, flags, pgnop)
	DBC *dbc;
	DBT *key;
	DBT *data;
	u_int32_t flags;
	db_pgno_t *pgnop;
{
	DB *dbp;
	DB_HEAP_RID rid;
	DB_MPOOLFILE *mpf;
	DB_LOCK meta_lock;
	DBT tmp_val;
	HEAP *h;
	HEAPHDR *hdr;
	HEAPMETA *meta;
	HEAPPG *dpage;
	HEAP_CURSOR *cp;
	db_lockmode_t lock_type;
	db_pgno_t pgno;
	int cmp, f_indx, found, getpage, indx, ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	h = dbp->heap_internal;
	cp = (HEAP_CURSOR *)dbc->internal;
	LOCK_INIT(meta_lock);
	COMPQUIET(pgnop, NULL);

	if (F_ISSET(key, DB_DBT_USERMEM) && key->ulen < DB_HEAP_RID_SZ) {
		key->size = DB_HEAP_RID_SZ;
		return (DB_BUFFER_SMALL);
	}

	/* Check for additional bits for locking */
	if (F_ISSET(dbc, DBC_RMW))
		lock_type = DB_LOCK_WRITE;
	else
		lock_type = DB_LOCK_READ;

	ret = 0;
	found = getpage = FALSE;
	meta = NULL;
	dpage = NULL;
	switch (flags) {
	case DB_CURRENT:

		/*
		 * Acquire the current page with read lock unless user
		 * has asked for a write lock.  Ensure page and record
		 * exist still.
		 */
		ACQUIRE_CUR(dbc, lock_type, cp->pgno, 0, 0, ret);
		if (ret != 0) {
			if (ret == DB_PAGE_NOTFOUND)
				ret = DB_NOTFOUND;
			goto err;
		}

		if (HEAP_OFFSETTBL(dbp, cp->page)[cp->indx] == 0) {
			ret = DB_NOTFOUND;
			goto err;
		}
		dpage = (HEAPPG *)cp->page;
		hdr = (HEAPHDR *)P_ENTRY(dbp, dpage, cp->indx);
		if (F_ISSET(hdr, HEAP_RECSPLIT) &&
		    !F_ISSET(hdr, HEAP_RECFIRST)) {
			ret = DB_NOTFOUND;
			goto err;
		}

		break;
	case DB_FIRST:
		/*
		 * The region pages do not distinguish between an empty
		 * page and page with a something on it.  So, we will
		 * grab the first possible data page and look for the
		 * lowest index with data.  If page is empty we go on to
		 * the next page and look. If no page, then no records.
		 */
first:		pgno = FIRST_HEAP_DPAGE;
		while (!found) {
			/* Put old lock/page and get the new lock/page */
			ACQUIRE_CUR(dbc, lock_type, pgno, 0, 0, ret);
			if (ret != 0 ) {
				if (ret == DB_PAGE_NOTFOUND)
					ret = DB_NOTFOUND;
				goto err;
			}
			dpage = (HEAPPG *)cp->page;
			/*
			 * The page needs to be a data page with entries on
			 * it.  If page is good, loop through the offset table
			 * finding first non-split record or first piece of a
			 * split record, then set up cursor.
			 */
			if (TYPE(dpage) == P_HEAP && NUM_ENT(dpage) != 0) {
				for (indx = 0;
				     indx <= HEAP_HIGHINDX(dpage); indx++) {
					if (HEAP_OFFSETTBL(
					    dbp, dpage)[indx] == 0)
						continue;
					hdr = (HEAPHDR *)P_ENTRY(
					    dbp, dpage, indx);
					if (!F_ISSET(hdr, HEAP_RECSPLIT) ||
					    F_ISSET(hdr, HEAP_RECFIRST)) {
						found = TRUE;
						cp->pgno = pgno;
						cp->indx = indx;
						break;
					}
				}
				if (!found)
					pgno++;
			} else
				pgno++;
		}
		break;
	case DB_LAST:
		/*
		 * Grab the metadata page to find the last page, and start
		 * there looking backwards for the record with the highest
		 * index and return that one.
		 */
last:		pgno = PGNO_BASE_MD;
		ACQUIRE(dbc, DB_LOCK_READ,
		    pgno, meta_lock, pgno, meta, 0, 0, ret);
		if (ret != 0)
			goto err;

		pgno = meta->dbmeta.last_pgno;

		/*
		 * It is possible to have another page added while we are
		 * searching backwards for last record. No need to block
		 * this case from occurring by keeping meta page lock.
		 */
		DISCARD(dbc, meta, meta_lock, 1, ret);
		if (ret != 0)
			goto err;

		while (!found) {
			/* Don't look earlier than the first data page. */
			if (pgno < FIRST_HEAP_DPAGE) {
				ret = DB_NOTFOUND;
				goto err;
			}

			/* Put old lock/page and get the new lock/page. */
			ACQUIRE_CUR(dbc, lock_type, pgno, 0, 0, ret);
			if (ret != 0)
				goto err;
			dpage = (HEAPPG *)cp->page;
			/*
			 * The page needs to be a data page with entries on
			 * it.  If page is good, search backwards until the a
			 * non-split record or the first piece of a split record
			 * is found.
			 */
			if (TYPE(dpage) == P_HEAP && NUM_ENT(dpage) != 0) {
				for (indx = HEAP_HIGHINDX(dpage);
				     indx >= 0; indx--) {
					if (HEAP_OFFSETTBL(
					    dbp, dpage)[indx] == 0)
						continue;
					hdr = (HEAPHDR *)P_ENTRY(
					    dbp, dpage, indx);
					if (!F_ISSET(hdr, HEAP_RECSPLIT) ||
					    F_ISSET(hdr, HEAP_RECFIRST)) {
						found = TRUE;
						cp->pgno = pgno;
						cp->indx = indx;
						break;
					}
				}
				if (!found)
					pgno--;
			} else
				pgno--;
		}
		break;
	case DB_NEXT_NODUP:
	case DB_NEXT:
		/* If cursor not initialize, behave as DB_FIRST */
		if (dbc->internal->pgno == PGNO_INVALID)
			goto first;

		/*
		 * Acquire the current page with the lock we have already,
		 * unless user has asked for a write lock.
		 */
		ACQUIRE_CUR(dbc, lock_type, cp->pgno, 0, 0, ret);
		if (ret != 0)
			goto err;
		dpage = (HEAPPG *)cp->page;

		/* At end of current page, must get next page */
		if (cp->indx >= HEAP_HIGHINDX(dpage))
			getpage = TRUE;

		while (!found) {
			if (getpage) {
				pgno = cp->pgno + 1;

				/* Put current page/lock and get next one */
				ACQUIRE_CUR(dbc, lock_type, pgno, 0, 0, ret);
				if (ret != 0) {
					/* Beyond last page? */
					if (ret == DB_PAGE_NOTFOUND)
						ret = DB_NOTFOUND;
					goto err;
				}
				dpage = (HEAPPG *)cp->page;

				/*
				 * If page is a spam page or its a data
				 * page without entries, try again.
				 */
				if (TYPE(dpage) != P_HEAP ||
				    (TYPE(dpage) == P_HEAP &&
				    NUM_ENT(dpage) == 0))
					continue;

				/* When searching, indx gets bumped to 0 */
				cp->indx = -1;
				getpage = FALSE;
			}

			/*
			 * Bump index and loop through the offset table finding
			 * first nonzero entry.  If the offset is for a split
			 * record, make sure it's the first piece of the split
			 * record. HEAP_HIGHINDX always points to highest filled
			 * entry on page.
			 */
			cp->indx++;
			for (indx=cp->indx;
			     indx <= HEAP_HIGHINDX(dpage); indx++) {
				if (HEAP_OFFSETTBL(dbp, dpage)[indx] == 0)
					continue;
				hdr = (HEAPHDR *)P_ENTRY(dbp, dpage, indx);
				if (!F_ISSET(hdr, HEAP_RECSPLIT) ||
				    F_ISSET(hdr, HEAP_RECFIRST)) {
					found = TRUE;
					cp->indx = indx;
					break;
				}
			}

			/* Nothing of interest on page, so try next */
			if (!found)
				getpage = TRUE;
		}
		break;
	case DB_PREV_NODUP:
	case DB_PREV:
		/* If cursor not initialize, behave as DB_LAST */
		if (dbc->internal->pgno == PGNO_INVALID)
			goto last;

		/*
		 * Acquire the current page with the lock we have already,
		 * unless user has asked for a write lock.
		 */
		ACQUIRE_CUR(dbc, lock_type, cp->pgno, 0, 0, ret);
		if (ret != 0)
			goto err;
		dpage = (HEAPPG *)cp->page;

		/*
		 * Loop through indexes and find first used slot.  Check if
		 * already at the first slot.
		 */
		for (f_indx=0; (f_indx <= HEAP_HIGHINDX(dpage)) &&
		    (HEAP_OFFSETTBL(dbp, dpage)[f_indx] == 0); f_indx++) ;

		/* At the beginning of current page, must get new page */
		if (cp->indx == 0 || cp->indx <= f_indx) {
			if (cp->pgno == FIRST_HEAP_DPAGE) {
				ret = DB_NOTFOUND;
				goto err;
			}
			getpage = TRUE;
		}

		while (!found) {
			if (getpage) {
				pgno = cp->pgno - 1;
				/* Do not go past first page */
				if (pgno < FIRST_HEAP_DPAGE) {
					ret = DB_NOTFOUND;
					goto err;
				}
				/* Put current page/lock and get prev page. */
				ACQUIRE_CUR(dbc, lock_type, pgno, 0, 0, ret);
				if (ret != 0)
					goto err;

				dpage = (HEAPPG *)cp->page;

				/*
				 * If page is a spam page or its a data
				 * page without entries, try again.
				 */
				if (TYPE(dpage) != P_HEAP ||
				    (TYPE(dpage) == P_HEAP &&
				    NUM_ENT(dpage) == 0))
					continue;

				/* When search, this gets bumped to high indx */
				cp->indx = HEAP_HIGHINDX(dpage) + 1;
				getpage = FALSE;
			}

			/*
			 * Decrement index and loop through the offset table
			 * finding previous nonzero entry.
			 */
			cp->indx--;
			for (indx=cp->indx;
			     indx >= 0; indx--) {
				if (HEAP_OFFSETTBL(dbp, dpage)[indx] == 0)
					continue;
				hdr = (HEAPHDR *)P_ENTRY(dbp, dpage, indx);
				if (!F_ISSET(hdr, HEAP_RECSPLIT) ||
				    F_ISSET(hdr, HEAP_RECFIRST)) {
					found = TRUE;
					cp->indx = indx;
					break;
				}
			}

			/* Nothing of interest on page, so try previous */
			if (!found)
				getpage = TRUE;
		}
		break;
	case DB_GET_BOTH_RANGE:
	case DB_GET_BOTH:
	case DB_SET_RANGE:
	case DB_SET:
		pgno = ((DB_HEAP_RID *)key->data)->pgno;
		indx = ((DB_HEAP_RID *)key->data)->indx;

		/* First make sure we're trying to get a data page. */
		if (pgno == PGNO_BASE_MD ||
		    pgno == HEAP_REGION_PGNO(dbp, pgno)) {
			ret = DB_NOTFOUND;
			goto err;
		}

		/* Lock the data page and get it. */
		ACQUIRE_CUR(dbc, lock_type, pgno, 0, 0, ret);

		if (ret != 0) {
			if (ret == DB_PAGE_NOTFOUND)
				ret = DB_NOTFOUND;
			goto err;
		}
		dpage = (HEAPPG *)cp->page;

		/* validate requested index, throw error if not in range */
		if ((indx >  HEAP_HIGHINDX(dpage)) ||
		    (HEAP_OFFSETTBL(dbp, dpage)[indx] == 0)) {
			DISCARD(dbc, cp->page, cp->lock, 0, ret);
			ret = DB_NOTFOUND;
			goto err;
		}
		hdr = (HEAPHDR *)P_ENTRY(dbp, dpage, indx);
		if (F_ISSET(hdr, HEAP_RECSPLIT) &&
		    !F_ISSET(hdr, HEAP_RECFIRST)) {
			DISCARD(dbc, cp->page, cp->lock, 0, ret);
			ret = DB_NOTFOUND;
			goto err;
		}

		cp->pgno = pgno;
		cp->indx = indx;

		if (flags == DB_GET_BOTH || flags == DB_GET_BOTH_RANGE)	{
			memset(&tmp_val, 0, sizeof(DBT));
			/* does the data match ? */
			if (F_ISSET(hdr, HEAP_RECSPLIT)) {
				tmp_val.flags = DB_DBT_MALLOC;
				if ((ret = __heapc_gsplit(
				    dbc, &tmp_val, NULL, 0)) != 0)
					goto err;
			} else {
				tmp_val.data =
				    (void *)((u_int8_t *)hdr + sizeof(HEAPHDR));
				tmp_val.size = hdr->size;
			}
			cmp = __bam_defcmp(dbp, &tmp_val, data);
			if (F_ISSET(&tmp_val, DB_DBT_MALLOC))
				__os_ufree(dbp->env, tmp_val.data);
			if (cmp != 0) {
				ret = DB_NOTFOUND;
				goto err;
			}
		}

		break;
	case DB_NEXT_DUP:
	case DB_PREV_DUP:
		ret = DB_NOTFOUND;
		goto err;
	default:
		/* DB_GET_RECNO, DB_JOIN_ITEM, DB_SET_RECNO are invalid */
		ret = __db_unknown_flag(dbp->env, "__heap_get", flags);
		goto err;

	}

err:	if (ret == 0 ) {
		if (key != NULL) {
			rid.pgno = cp->pgno;
			rid.indx = cp->indx;
			ret = __db_retcopy(dbp->env, key, &rid,
			    DB_HEAP_RID_SZ, &dbc->rkey->data, &dbc->rkey->ulen);
			F_SET(key, DB_DBT_ISSET);
		}

	} else {
		if (meta != NULL)
			(void)__memp_fput(mpf,
			    dbc->thread_info, meta, dbc->priority);
		if (LOCK_ISSET(meta_lock))
			(void)__LPUT(dbc, meta_lock);
		if (LOCK_ISSET(cp->lock))
			 (void)__LPUT(dbc, cp->lock);
	}
	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	return (ret);
}

#undef	IS_FIRST
#define	IS_FIRST (last_rid.pgno == PGNO_INVALID)
/*
 * __heapc_reloc_partial --
 *	 Move data from a too-full page to a new page.  The old data page must
 *	 be write locked before calling this method.
 */
static int
__heapc_reloc_partial(dbc, key, data)
	DBC *dbc;
	DBT *key;
	DBT *data;
{
	DB *dbp;
	DBT hdr_dbt, log_dbt, t_data, t_key;
	DB_HEAP_RID last_rid, next_rid;
	HEAPHDR *old_hdr;
	HEAPSPLITHDR new_hdr;
	HEAP_CURSOR *cp;
	int add_bytes, ret;
	u_int32_t buflen, data_size, dlen, doff, left, old_size;
	u_int32_t remaining, size;
	u_int8_t *buf, *olddata;

	dbp = dbc->dbp;
	cp = (HEAP_CURSOR *)dbc->internal;
	old_hdr = (HEAPHDR *)(P_ENTRY(dbp, cp->page, cp->indx));
	memset(&hdr_dbt, 0, sizeof(DBT));
	memset(&log_dbt, 0, sizeof(DBT));
	buf = NULL;
	COMPQUIET(key, NULL);

	/* We only work on partial puts. */
	DB_ASSERT(dbp->env, F_ISSET(data, DB_DBT_PARTIAL));

	/*
	 * Start by calculating the data_size, total size of the new record, and
	 * dlen, the number of bytes we will actually overwrite.  Keep a local
	 * copy of doff, we'll adjust it as we see pieces of the record so that
	 * it's always relative to the current piece of data.
	 */
	if (F_ISSET(old_hdr, HEAP_RECSPLIT))
		old_size = ((HEAPSPLITHDR *)old_hdr)->tsize;
	else
		old_size = old_hdr->size;
	doff = data->doff;
	if (old_size < doff) {
		/* Post-pending */
		dlen = data->dlen;
		data_size = doff + data->size;
	} else {
		if (old_size - doff < data->dlen)
			dlen = old_size - doff;
		else
			dlen = data->dlen;
		data_size = old_size - dlen + data->size;
	}

	/*
	 * We don't need a buffer large enough to hold the data_size
	 * bytes, just one large enough to hold the bytes that will be
	 * written to an individual page.  We'll realloc to the necessary size
	 * as needed.
	 */
	buflen = 0;
	buf = NULL;

	/*
	 * We are updating an existing record, which will grow into a split
	 * record.  The strategy is to overwrite the existing record (or each
	 * piece of the record if the record is already split.)  If the new
	 * record is shorter than the old, delete any extra pieces.  If the new
	 * record is longer than the old, use heapc_split() to write the extra
	 * data.
	 *
	 * We start each loop with old_hdr pointed at the header for the old
	 * record and the necessary page write locked in cp->page.
	 */
	last_rid.pgno = PGNO_INVALID;
	last_rid.indx = 0;
	add_bytes = 1;
	left = data_size;
	memset(&t_data, 0, sizeof(DBT));
	remaining = 0;
	for (;;) {
		/* Figure out if we have a next piece. */
		if (F_ISSET(old_hdr, HEAP_RECSPLIT)) {
			next_rid.pgno = ((HEAPSPLITHDR *)old_hdr)->nextpg;
			next_rid.indx = ((HEAPSPLITHDR *)old_hdr)->nextindx;
		} else {
			next_rid.pgno = PGNO_INVALID;
			next_rid.indx = 0;
		}

		/*
		 * Before we delete the old data, use it to construct the new
		 * data. First figure out the size of the new piece, including
		 * any remaining data from the last piece.
		 */
		if (doff >= old_hdr->size)
			if (F_ISSET(old_hdr, HEAP_RECLAST) ||
			    !F_ISSET(old_hdr, HEAP_RECSPLIT)) {
				/* Post-pending. */
				data_size = doff + data->size;
			} else {
				/* The new piece is just the old piece. */
				data_size = old_hdr->size;
			}
		else if (doff + dlen > old_hdr->size)
			/*
			 * Some of the to-be-overwritten bytes are on the next
			 * piece, but we'll append all the new bytes to this
			 * piece if we haven't already written them.
			 */
			data_size = doff + (add_bytes ? data->size : 0);
		else
			data_size = old_hdr->size -
				dlen + (add_bytes ? data->size : 0);
		data_size += remaining;

		if (data_size > buflen) {
			if (__os_realloc(dbp->env, data_size, &buf) != 0)
				return (ENOMEM);
			buflen = data_size;
		}
		t_data.data = buf;

		/*
		 * Adjust past any remaining bytes, they've already been moved
		 * to the beginning of the buffer.
		 */
		buf += remaining;
		remaining = 0;

		olddata = (u_int8_t *)old_hdr + HEAP_HDRSIZE(old_hdr);
		if (doff >= old_hdr->size) {
			memcpy(buf, olddata, old_hdr->size);
			doff -= old_hdr->size;
			if (F_ISSET(old_hdr, HEAP_RECLAST) ||
			    !F_ISSET(old_hdr, HEAP_RECSPLIT)) {
				/* Post-pending. */
				buf += old_hdr->size;
				memset(buf, '\0', doff);
				buf += doff;
				memcpy(buf, data->data, data->size);
			}
		} else {
			/* Preserve the first doff bytes. */
			memcpy(buf, olddata, doff);
			buf += doff;
			olddata += doff;
			/* Copy in the new bytes, if needed. */
			if (add_bytes) {
				memcpy(buf, data->data, data->size);
				buf += data->size;
				add_bytes = 0;
			}
			/* Skip dlen bytes. */
			if (doff + dlen < old_hdr->size) {
				olddata += dlen;
				memcpy(buf,
				    olddata, old_hdr->size - doff - dlen);
				dlen = 0;
			} else
				/*
				 * The data to be removed spills over onto the
				 * following page(s).  Adjust dlen to account
				 * for the bytes removed from this page.
				 */
				dlen = doff + dlen - old_hdr->size;
			doff = 0;
		}
		buf = t_data.data;

		/* Delete the old data, after logging it. */
		old_size = DB_ALIGN(
		    old_hdr->size + HEAP_HDRSIZE(old_hdr), sizeof(u_int32_t));
		if (old_size < sizeof(HEAPSPLITHDR))
			old_size = sizeof(HEAPSPLITHDR);
		if (DBC_LOGGING(dbc)) {
			hdr_dbt.data = old_hdr;
			hdr_dbt.size = HEAP_HDRSIZE(old_hdr);
			log_dbt.data = (u_int8_t *)old_hdr + hdr_dbt.size;
			log_dbt.size = DB_ALIGN(
			    old_hdr->size, sizeof(u_int32_t));
			if ((ret = __heap_addrem_log(dbp, dbc->txn,
			    &LSN(cp->page), 0, DB_REM_HEAP, cp->pgno,
			    (u_int32_t)cp->indx, old_size,
			    &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));
		if ((ret = __heap_ditem(
		    dbc, cp->page, cp->indx, old_size)) != 0)
			goto err;

		if (left == 0)
			/*
			 * We've finished writing the new record, we're just
			 * cleaning up the old record now.
			 */
			goto next_pg;

		if (data_size == 0 && !IS_FIRST) {
			/*
			 * This piece is being completely removed.  We need to
			 * adjust the header of the previous piece now.
			 */
			ACQUIRE_CUR(dbc, DB_LOCK_WRITE,
			    last_rid.pgno, 0, DB_MPOOL_DIRTY, ret);
			if (ret != 0)
				goto err;

			cp->indx = last_rid.indx;
			old_hdr = (HEAPHDR *)(P_ENTRY(dbp, cp->page, cp->indx));

			if (DBC_LOGGING(dbc)) {
				old_size = DB_ALIGN(old_hdr->size +
				    HEAP_HDRSIZE(old_hdr), sizeof(u_int32_t));
				hdr_dbt.data = old_hdr;
				hdr_dbt.size = HEAP_HDRSIZE(old_hdr);
				log_dbt.data =
				    (u_int8_t *)old_hdr + hdr_dbt.size;
				log_dbt.size = DB_ALIGN(
				    old_hdr->size, sizeof(u_int32_t));
				if ((ret = __heap_addrem_log(dbp, dbc->txn,
				    &LSN(cp->page), 0, DB_REM_HEAP, cp->pgno,
				    (u_int32_t)cp->indx, old_size,
				    &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
					goto err;
			} else
				LSN_NOT_LOGGED(LSN(cp->page));

			((HEAPSPLITHDR *)old_hdr)->nextpg = next_rid.pgno;
			((HEAPSPLITHDR *)old_hdr)->nextindx = next_rid.indx;

			if (DBC_LOGGING(dbc)) {
				if ((ret = __heap_addrem_log(dbp, dbc->txn,
				    &LSN(cp->page), 0, DB_ADD_HEAP, cp->pgno,
				    (u_int32_t)cp->indx, old_size,
				    &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
					goto err;
			} else
				LSN_NOT_LOGGED(LSN(cp->page));

			DISCARD(dbc, cp->page, cp->lock, 1, ret);

			goto next_pg;
		}

		/* Set up the header for the new record. */
		memset(&new_hdr, 0, sizeof(HEAPSPLITHDR));
		new_hdr.std_hdr.flags = HEAP_RECSPLIT;
		/*
		 * If next_rid.pgno == PGNO_INVALID and there's still more data,
		 * we'll come back and correct the header once we know where the
		 * next piece lives.
		 */
		new_hdr.nextpg = next_rid.pgno;
		new_hdr.nextindx = next_rid.indx;
		/*
		 * Figure out how much we can fit on the page, rounding down to
		 * a multiple of 4.  If we will have to expand the offset table,
		 * account for that. It needs to be enough to at least fit the
		 * split header.
		 */
		size = HEAP_FREESPACE(dbp, cp->page);
		if (NUM_ENT(cp->page) == 0 ||
		    cp->indx > HEAP_HIGHINDX(cp->page))
			size -= sizeof(db_indx_t);
		/* Round down to a multiple of 4. */
		size = DB_ALIGN(
		    size - sizeof(u_int32_t) + 1, sizeof(u_int32_t));
		DB_ASSERT(dbp->env, size >= sizeof(HEAPSPLITHDR));

		/*
		 * We try to fill the page, but cannot write more than
		 * t_data.size bytes, that's all we have in-memory.
		 */
		new_hdr.std_hdr.size = (u_int16_t)
		    (size - sizeof(HEAPSPLITHDR));
		if (new_hdr.std_hdr.size > data_size)
			new_hdr.std_hdr.size = data_size;
		if (new_hdr.std_hdr.size >= left) {
			new_hdr.std_hdr.size = left;
			new_hdr.std_hdr.flags |= HEAP_RECLAST;
			new_hdr.nextpg = PGNO_INVALID;
			new_hdr.nextindx = 0;
		}
		if (IS_FIRST) {
			new_hdr.std_hdr.flags |= HEAP_RECFIRST;
			new_hdr.tsize = left;
		}

		/* Now write the new data to the page. */
		t_data.size = new_hdr.std_hdr.size;
		hdr_dbt.data = &new_hdr;
		hdr_dbt.size = sizeof(HEAPSPLITHDR);
		/* Log the write. */
		if (DBC_LOGGING(dbc)) {
			if ((ret = __heap_addrem_log(dbp,
			    dbc->txn, &LSN(cp->page), 0,
			    DB_ADD_HEAP, cp->pgno, (u_int32_t)cp->indx,
			    size, &hdr_dbt, &t_data, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));
		if ((ret = __heap_pitem(dbc,
		    (PAGE *)cp->page, cp->indx, size, &hdr_dbt, &t_data)) != 0)
			goto err;

		left -= new_hdr.std_hdr.size;
		/*
		 * If any data couldn't fit on this page, it has to go onto the
		 * next.  Copy it to the front of the buffer and it will be
		 * preserved in the next loop.
		 */
		if (new_hdr.std_hdr.size < data_size) {
			remaining = data_size - new_hdr.std_hdr.size;
			memmove(buf, buf + new_hdr.std_hdr.size, remaining);
		}

		/*
		 * Remember this piece's RID, we may need to update the header
		 * if the next data piece is removed, or if this is the final
		 * piece and we add data to the end of the record.
		 */
next_pg:	last_rid.pgno = cp->pgno;
		last_rid.indx = cp->indx;
		/* Get the next page, if any. */
		if (next_rid.pgno != PGNO_INVALID) {
			ACQUIRE_CUR(dbc, DB_LOCK_WRITE,
			    next_rid.pgno, 0, DB_MPOOL_DIRTY, ret);
			if (ret != 0)
				goto err;
			cp->indx = next_rid.indx;
			old_hdr = (HEAPHDR *)(P_ENTRY(dbp, cp->page, cp->indx));
			DB_ASSERT(dbp->env,
			    HEAP_HIGHINDX(cp->page) <= cp->indx);
			DB_ASSERT(dbp->env, F_ISSET(old_hdr, HEAP_RECSPLIT));
		} else {
			/* Discard the page and drop the lock, txn-ally. */
			DISCARD(dbc, cp->page, cp->lock, 1, ret);
			if (ret != 0)
				goto err;
			break;
		}
	}

	/*
	 * If there is more work to do, let heapc_split do it.  After
	 * heapc_split returns we need to update nextpg and nextindx in the
	 * header of the last piece we wrote above.
	 *
	 * For logging purposes, we "delete" the old record and then "add" the
	 * record.  This makes redo/undo work as-is, but we won't actually
	 * delete and re-add the record.
	 */
	if (left > 0) {
		memset(&t_key, 0, sizeof(DBT));
		t_key.size = t_key.ulen = sizeof(DB_HEAP_RID);
		t_key.data = &next_rid;
		t_key.flags = DB_DBT_USERMEM;
		t_data.size = left;
		if ((ret = __heapc_split(dbc, &t_key, &t_data, 0)) != 0)
			goto err;

		ACQUIRE_CUR(dbc,
		    DB_LOCK_WRITE, last_rid.pgno, 0, DB_MPOOL_DIRTY, ret);
		if (ret != 0)
			goto err;

		cp->indx = last_rid.indx;
		old_hdr = (HEAPHDR *)(P_ENTRY(dbp, cp->page, cp->indx));

		if (DBC_LOGGING(dbc)) {
			old_size = DB_ALIGN(old_hdr->size +
			    HEAP_HDRSIZE(old_hdr), sizeof(u_int32_t));
			hdr_dbt.data = old_hdr;
			hdr_dbt.size = HEAP_HDRSIZE(old_hdr);
			log_dbt.data = (u_int8_t *)old_hdr + hdr_dbt.size;
			log_dbt.size = DB_ALIGN(
			    old_hdr->size, sizeof(u_int32_t));
			if ((ret = __heap_addrem_log(dbp, dbc->txn,
			    &LSN(cp->page), 0, DB_REM_HEAP, cp->pgno,
			    (u_int32_t)cp->indx, old_size,
			    &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));

		((HEAPSPLITHDR *)old_hdr)->nextpg = next_rid.pgno;
		((HEAPSPLITHDR *)old_hdr)->nextindx = next_rid.indx;

		if (DBC_LOGGING(dbc)) {
			if ((ret = __heap_addrem_log(dbp, dbc->txn,
			    &LSN(cp->page), 0, DB_ADD_HEAP, cp->pgno,
			    (u_int32_t)cp->indx, old_size,
			    &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));

		DISCARD(dbc, cp->page, cp->lock, 1, ret);
	}

err:	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	if (buf != NULL)
		__os_free(dbp->env, buf);
	return (ret);
}

/*
 * __heapc_reloc --
 *	 Move data from a too-full page to a new page.  The old data page must
 *	 be write locked before calling this method.
 */
static int
__heapc_reloc(dbc, key, data)
	DBC *dbc;
	DBT *key;
	DBT *data;
{
	DB *dbp;
	DBT hdr_dbt, log_dbt, t_data, t_key;
	DB_HEAP_RID last_rid, next_rid;
	HEAPHDR *old_hdr;
	HEAPSPLITHDR new_hdr;
	HEAP_CURSOR *cp;
	int is_first, ret;
	u_int32_t left, old_size, size;

	dbp = dbc->dbp;
	cp = (HEAP_CURSOR *)dbc->internal;
	old_hdr = (HEAPHDR *)(P_ENTRY(dbp, cp->page, cp->indx));
	memset(&hdr_dbt, 0, sizeof(DBT));
	memset(&log_dbt, 0, sizeof(DBT));
	COMPQUIET(key, NULL);

	/*
	 * We are updating an existing record, which will grow into a split
	 * record.  The strategy is to overwrite the existing record (or each
	 * piece of the record if the record is already split.)  If the new
	 * record is shorter than the old, delete any extra pieces.  If the new
	 * record is longer than the old, use heapc_split() to write the extra
	 * data.
	 *
	 * We start each loop with t_data.data positioned to the next byte to be
	 * written, old_hdr pointed at the header for the old record and the
	 * necessary page write locked in cp->page.
	 */
	is_first = 1;
	left = data->size;
	memset(&t_data, 0, sizeof(DBT));
	t_data.data = data->data;
	for (;;) {
		/* Figure out if we have a next piece. */
		if (F_ISSET(old_hdr, HEAP_RECSPLIT)) {
			next_rid.pgno = ((HEAPSPLITHDR *)old_hdr)->nextpg;
			next_rid.indx = ((HEAPSPLITHDR *)old_hdr)->nextindx;
		} else {
			next_rid.pgno = PGNO_INVALID;
			next_rid.indx = 0;
		}

		/* Delete the old data, after logging it. */
		old_size = DB_ALIGN(
		    old_hdr->size + HEAP_HDRSIZE(old_hdr), sizeof(u_int32_t));
		if (old_size < sizeof(HEAPSPLITHDR))
			old_size = sizeof(HEAPSPLITHDR);
		if (DBC_LOGGING(dbc)) {
			hdr_dbt.data = old_hdr;
			hdr_dbt.size = HEAP_HDRSIZE(old_hdr);
			log_dbt.data = (u_int8_t *)old_hdr + hdr_dbt.size;
			log_dbt.size = DB_ALIGN(
			    old_hdr->size, sizeof(u_int32_t));
			if ((ret = __heap_addrem_log(dbp, dbc->txn,
			    &LSN(cp->page), 0, DB_REM_HEAP, cp->pgno,
			    (u_int32_t)cp->indx, old_size,
			    &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));
		if ((ret = __heap_ditem(
		    dbc, cp->page, cp->indx, old_size)) != 0)
			goto err;

		if (left == 0)
			/*
			 * We've finished writing the new record, we're just
			 * cleaning up the old record now.
			 */
			goto next_pg;

		/* Set up the header for the new record. */
		memset(&new_hdr, 0, sizeof(HEAPSPLITHDR));
		new_hdr.std_hdr.flags = HEAP_RECSPLIT;
		/* We'll set this later if next_rid.pgno == PGNO_INVALID. */
		new_hdr.nextpg = next_rid.pgno;
		new_hdr.nextindx = next_rid.indx;
		/*
		 * Figure out how much we can fit on the page, rounding down to
		 * a multiple of 4.  If we will have to expand the offset table,
		 * account for that.It needs to be enough to at least fit the
		 * split header.
		 */
		size = HEAP_FREESPACE(dbp, cp->page);
		if (NUM_ENT(cp->page) == 0 ||
		    cp->indx > HEAP_HIGHINDX(cp->page))
			size -= sizeof(db_indx_t);
		/* Round down to a multiple of 4. */
		size = DB_ALIGN(
		    size - sizeof(u_int32_t) + 1, sizeof(u_int32_t));
		DB_ASSERT(dbp->env, size >= sizeof(HEAPSPLITHDR));
		new_hdr.std_hdr.size =
		    (u_int16_t)(size - sizeof(HEAPSPLITHDR));
		if (new_hdr.std_hdr.size >= left) {
			new_hdr.std_hdr.size = left;
			new_hdr.std_hdr.flags |= HEAP_RECLAST;
			new_hdr.nextpg = PGNO_INVALID;
			new_hdr.nextindx = 0;
		}
		if (is_first) {
			new_hdr.std_hdr.flags |= HEAP_RECFIRST;
			new_hdr.tsize = left;
			is_first = 0;
		}

		/* Now write the new data to the page. */
		t_data.size = new_hdr.std_hdr.size;
		hdr_dbt.data = &new_hdr;
		hdr_dbt.size = sizeof(HEAPSPLITHDR);
		/* Log the write. */
		if (DBC_LOGGING(dbc)) {
			if ((ret = __heap_addrem_log(dbp,
			    dbc->txn, &LSN(cp->page), 0,
			    DB_ADD_HEAP, cp->pgno, (u_int32_t)cp->indx,
			    size, &hdr_dbt, &t_data, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));
		if ((ret = __heap_pitem(dbc,
		    (PAGE *)cp->page, cp->indx, size, &hdr_dbt, &t_data)) != 0)
			goto err;

		left -= new_hdr.std_hdr.size;
		t_data.data = (u_int8_t *)(t_data.data) + new_hdr.std_hdr.size;

		/* Get the next page, if any. */
next_pg:	if (next_rid.pgno != PGNO_INVALID) {
			ACQUIRE_CUR(dbc, DB_LOCK_WRITE,
			    next_rid.pgno, 0, DB_MPOOL_DIRTY, ret);
			if (ret != 0)
				goto err;
			cp->indx = next_rid.indx;
			old_hdr = (HEAPHDR *)(P_ENTRY(dbp, cp->page, cp->indx));
		} else {
			/*
			 * Remember the final piece's RID, we may need to update
			 * the header after writing the rest of the record.
			 */
			last_rid.pgno = cp->pgno;
			last_rid.indx = cp->indx;
			/* Discard the page and drop the lock, txn-ally. */
			DISCARD(dbc, cp->page, cp->lock, 1, ret);
			if (ret != 0)
				goto err;
			break;
		}
	}

	/*
	 * If there is more work to do, let heapc_split do it.  After
	 * heapc_split returns we need to update nextpg and nextindx in the
	 * header of the last piece we wrote above.
	 *
	 * For logging purposes, we "delete" the old record and then "add" the
	 * record.  This makes redo/undo work as-is, but we won't actually
	 * delete and re-add the record.
	 */
	if (left > 0) {
		memset(&t_key, 0, sizeof(DBT));
		t_key.size = t_key.ulen = sizeof(DB_HEAP_RID);
		t_key.data = &next_rid;
		t_key.flags = DB_DBT_USERMEM;
		t_data.size = left;
		if ((ret = __heapc_split(dbc, &t_key, &t_data, 0)) != 0)
			goto err;

		ACQUIRE_CUR(dbc,
		    DB_LOCK_WRITE, last_rid.pgno, 0, DB_MPOOL_DIRTY, ret);
		if (ret != 0)
			goto err;

		cp->indx = last_rid.indx;
		old_hdr = (HEAPHDR *)(P_ENTRY(dbp, cp->page, cp->indx));

		if (DBC_LOGGING(dbc)) {
			old_size = DB_ALIGN(old_hdr->size +
			    HEAP_HDRSIZE(old_hdr), sizeof(u_int32_t));
			hdr_dbt.data = old_hdr;
			hdr_dbt.size = HEAP_HDRSIZE(old_hdr);
			log_dbt.data = (u_int8_t *)old_hdr + hdr_dbt.size;
			log_dbt.size = DB_ALIGN(
			    old_hdr->size, sizeof(u_int32_t));
			if ((ret = __heap_addrem_log(dbp, dbc->txn,
			    &LSN(cp->page), 0, DB_REM_HEAP, cp->pgno,
			    (u_int32_t)cp->indx, old_size,
			    &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));

		((HEAPSPLITHDR *)old_hdr)->nextpg = next_rid.pgno;
		((HEAPSPLITHDR *)old_hdr)->nextindx = next_rid.indx;

		if (DBC_LOGGING(dbc)) {
			if ((ret = __heap_addrem_log(dbp, dbc->txn,
			    &LSN(cp->page), 0, DB_ADD_HEAP, cp->pgno,
			    (u_int32_t)cp->indx,old_size,
			    &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));

		DISCARD(dbc, cp->page, cp->lock, 1, ret);
	}

err:	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	return (ret);
}

/*
 * __heapc_put --
 *
 * Put using a cursor.  If the given key exists, update the associated data.  If
 * the given key does not exsist, return an error.
 */
static int
__heapc_put(dbc, key, data, flags, pgnop)
	DBC *dbc;
	DBT *key;
	DBT *data;
	u_int32_t flags;
	db_pgno_t *pgnop;
{
	DB *dbp;
	DBT hdr_dbt, log_dbt, new_data;
	DB_MPOOLFILE *mpf;
	HEAPHDR hdr, *old_hdr;
	HEAP_CURSOR *cp;
	PAGE *rpage;
	db_pgno_t region_pgno;
	int oldspace, ret, space, t_ret;
	u_int32_t data_size, dlen, new_size, old_flags, old_size, tot_size;
	u_int8_t *buf, *olddata, *src, *dest;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (HEAP_CURSOR *)dbc->internal;
	rpage = NULL;
	buf = dest = src = NULL;
	dlen = 0;

	if (flags != DB_CURRENT) {
		/* We're going to write following the get, so use RMW. */
		old_flags = dbc->flags;
		F_SET(dbc, DBC_RMW);
		ret = __heapc_get(dbc, key, data, DB_SET, pgnop);
		F_CLR(key, DB_DBT_ISSET);
		dbc->flags = old_flags;
		DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
		if (ret != 0)
			return (ret);
		else if (flags == DB_NOOVERWRITE)
			return (DB_KEYEXIST);
		if ((ret = __memp_dirty(mpf, &cp->page,
		    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0)
			return (ret);
	} else {
		/* We have a read lock, but need a write lock. */
		if (STD_LOCKING(dbc) && cp->lock_mode != DB_LOCK_WRITE &&
		    (ret = __db_lget(dbc,
		    LCK_COUPLE, cp->pgno, DB_LOCK_WRITE, 0, &cp->lock)) != 0)
			return (ret);

		if ((ret = __memp_fget(mpf, &cp->pgno, dbc->thread_info,
		    dbc->txn, DB_MPOOL_DIRTY, &cp->page)) != 0)
			return (ret);
	}

	/* We've got the page locked and stored in cp->page. */
	HEAP_CALCSPACEBITS(dbp, HEAP_FREESPACE(dbp, cp->page), oldspace);

	/*
	 * Figure out the spacing issue.  There is a very rare corner case where
	 * we don't have enough space on the page to expand the data. Splitting
	 * the record results in a larger header, if the page is jam packed
	 * there might not be room for the larger header.
	 *
	 * hdr->size is the size of the stored data, it doesn't include any
	 * padding.
	 */
	old_hdr = (HEAPHDR *)(P_ENTRY(dbp, cp->page, cp->indx));
	/* Need data.size + header size, 4-byte aligned. */
	old_size =
	    DB_ALIGN(old_hdr->size + HEAP_HDRSIZE(old_hdr), sizeof(u_int32_t));
	if (old_size < sizeof(HEAPSPLITHDR))
		old_size = sizeof(HEAPSPLITHDR);
	if (F_ISSET(data, DB_DBT_PARTIAL)) {
		if (F_ISSET(old_hdr, HEAP_RECSPLIT))
			tot_size = ((HEAPSPLITHDR *)old_hdr)->tsize;
		else
			tot_size = old_hdr->size;
		if (tot_size < data->doff) {
			/* Post-pending */
			dlen = data->dlen;
			data_size = data->doff + data->size;
		} else {
			if (tot_size - data->doff < data->dlen)
				dlen = tot_size - data->doff;
			else
				dlen = data->dlen;
			data_size = tot_size - dlen + data->size;
		}
	} else
		data_size = data->size;
	new_size = DB_ALIGN(data_size + sizeof(HEAPHDR), sizeof(u_int32_t));
	if (new_size < sizeof(HEAPSPLITHDR))
		new_size = sizeof(HEAPSPLITHDR);

	/* Check whether we actually have enough space on this page. */
	if (F_ISSET(old_hdr, HEAP_RECSPLIT) ||
	    (new_size > old_size &&
	    new_size - old_size > HEAP_FREESPACE(dbp, cp->page))) {
		/*
		 * We've got to split the record, not enough room on the
		 * page.  Splitting the record will remove old_size bytes and
		 * introduce at least sizeof(HEAPSPLITHDR).
		 */
		if (F_ISSET(data, DB_DBT_PARTIAL))
			return (__heapc_reloc_partial(dbc, key, data));
		else
			return (__heapc_reloc(dbc, key, data));
	}

	memset(&new_data, 0, sizeof(DBT));
	new_data.size = data_size;
	if (F_ISSET(data, DB_DBT_PARTIAL)) {
		/*
		 * Before replacing the old data, we need to use it to build the
		 * new data.
		 */
		if ((ret = __os_malloc(dbp->env, data_size, &buf)) != 0)
			goto err;
		new_data.data = buf;

		/*
		 * Preserve data->doff bytes at the start, or all of the old
		 * record plus padding, if post-pending.
		 */
		olddata = (u_int8_t *)old_hdr + sizeof(HEAPHDR);
		if (data->doff > old_hdr->size) {
			memcpy(buf, olddata, old_hdr->size);
			buf += old_hdr->size;
			memset(buf, '\0', data->doff - old_hdr->size);
			buf += data->doff - old_hdr->size;
		} else {
			memcpy(buf, olddata, data->doff);
			buf += data->doff;
		}

		/* Now copy in the user's data. */
		memcpy(buf, data->data, data->size);
		buf += data->size;

		/* Fill in remaining data from the old record, skipping dlen. */
		if (data->doff < old_hdr->size) {
			olddata += data->doff + data->dlen;
			memcpy(buf,
			    olddata, old_hdr->size - data->doff - data->dlen);
		}
	} else {
		new_data.data = data->data;
	}

	/*
	 * Do the update by deleting the old record and writing the new
	 * record.  Start by logging the entire operation.
	 */
	memset(&hdr, 0, sizeof(HEAPHDR));
	hdr.size = data_size;
	if (DBC_LOGGING(dbc)) {
		hdr_dbt.data = old_hdr;
		hdr_dbt.size = HEAP_HDRSIZE(old_hdr);
		log_dbt.data = (u_int8_t *)old_hdr + hdr_dbt.size;
		log_dbt.size = DB_ALIGN(old_hdr->size, sizeof(u_int32_t));
		if ((ret = __heap_addrem_log(dbp, dbc->txn, &LSN(cp->page),
		    0, DB_REM_HEAP, cp->pgno, (u_int32_t)cp->indx,
		    old_size, &hdr_dbt, &log_dbt, &LSN(cp->page))) != 0)
			goto err;
		hdr_dbt.data = &hdr;
		hdr_dbt.size = HEAP_HDRSIZE(&hdr);
		if ((ret = __heap_addrem_log(dbp, dbc->txn, &LSN(cp->page),
		    0, DB_ADD_HEAP, cp->pgno, (u_int32_t)cp->indx,
		    new_size, &hdr_dbt, &new_data, &LSN(cp->page))) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(cp->page));

	if ((ret = __heap_ditem(dbc, cp->page, cp->indx, old_size)) != 0)
		goto err;
	hdr_dbt.data = &hdr;
	hdr_dbt.size = HEAP_HDRSIZE(&hdr);
	if ((ret = __heap_pitem(dbc,
	    (PAGE *)cp->page, cp->indx, new_size, &hdr_dbt, &new_data)) != 0)
		goto err;

	/* Check whether we need to update the space bitmap. */
	HEAP_CALCSPACEBITS(dbp, HEAP_FREESPACE(dbp, cp->page), space);

	if (space != oldspace) {
		/* Get the region page with an exclusive latch. */
		region_pgno = HEAP_REGION_PGNO(dbp, cp->pgno);

		if ((ret = __memp_fget(mpf, &region_pgno,
		    dbc->thread_info, NULL, DB_MPOOL_DIRTY, &rpage)) != 0)
			goto err;

		HEAP_SETSPACE(dbp, rpage, cp->pgno - region_pgno - 1, space);
	}

err:	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	if (rpage != NULL && (t_ret = __memp_fput(mpf,
	    dbc->thread_info, rpage, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	if (F_ISSET(data, DB_DBT_PARTIAL))
		__os_free(dbp->env, new_data.data);

	if (ret != 0 && LOCK_ISSET(cp->lock))
		(void)__TLPUT(dbc, cp->lock);

	return (ret);
}

/*
 * __heap_getpage --
 *	Return a page with sufficient free space.  The page will be write locked
 *	and marked dirty.
 */
static int
__heap_getpage(dbc, size, avail)
	DBC *dbc;
	u_int32_t size;
	u_int8_t *avail;
{
	DB *dbp;
	DBMETA *meta;
	DB_LOCK meta_lock;
	DB_LSN meta_lsn;
	DB_MPOOLFILE *mpf;
	HEAP *h;
	HEAPPG *rpage;
	HEAP_CURSOR *cp;
	db_pgno_t data_pgno, *lkd_pgs, meta_pgno, region_pgno, start_region;
	int i, lk_mode, max, p, ret, space, start, t_ret;

	LOCK_INIT(meta_lock);
	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (HEAP_CURSOR *)dbc->internal;
	h = dbp->heap_internal;
	start_region = region_pgno = h->curregion;
	max = HEAP_REGION_SIZE(dbp);
	i = ret = t_ret = 0;
	lkd_pgs = NULL;

	/*
	 * The algorithm for finding a page:
	 *
	 * Look in the space bitmap of the current region page for a data page
	 * with at least size bytes free.  Once we find a page, try to lock it
	 * and if we get the lock we're done.
	 *
	 * Don't wait for a locked region page, just move on to the next region
	 * page, creating it if it doesn't exist.  If the size of the heap
	 * database is not constrained, just keep creating regions and extending
	 * the database until we find a page with space.  If the database size
	 * is constrained, loop back to the first region page from the final
	 * region page.  If we wind up making it all the way back to where our
	 * search began, we need to start waiting for locked region pages.  If
	 * we finish another loop through the database waiting for every region
	 * page, we know there's no room.
	 */

	/*
	 * Figure out the % of the page the data will occupy and translate that
	 * to the relevant bit-map value we need to look for.
	 */
	HEAP_CALCSPACEBITS(dbp, size, space);

	/*
	 * Get the current region page, with a shared latch.  On the first loop
	 * through a fixed size database, we move on to the next region if the
	 * page is locked.  On the second loop, we wait for locked region
	 * pages.  If the database isn't fixed size, we never wait, we'll
	 * eventually get to use one of the region pages we create.
	 */
	lk_mode = DB_MPOOL_TRY;
find:	while ((ret = __memp_fget(mpf, &region_pgno,
	    dbc->thread_info, NULL, lk_mode, &rpage)) != 0 ||
	    TYPE(rpage) != P_IHEAP) {
		if (ret == DB_LOCK_NOTGRANTED)
			goto next_region;
		if (ret != 0 && ret != DB_PAGE_NOTFOUND)
			return (ret);
		/*
		 * The region page doesn't exist, or hasn't been initialized,
		 * create it, then try again.  If the page exists, we have to
		 * drop it before initializing the region.
		 */
		if (ret == 0 && (ret = __memp_fput(
		    mpf, dbc->thread_info, rpage, dbc->priority)) != 0)
			return (ret);

		if ((ret = __heap_create_region(dbc, region_pgno)) != 0)
			return (ret);
	}

	start = h->curpgindx;
	/*
	 * If this is the last region page in a fixed size db, figure out the
	 * maximum pgno in the bitmap.
	 */
	if (region_pgno + max > h->maxpgno)
		max = h->maxpgno - region_pgno;
	/*
	 * Look in the bitmap for a page with sufficient free space.  We use i
	 * in a slightly strange way.  Because the 2-bits in the bitmap are only
	 * an estimate, there is a chance the data won't fit on the page we
	 * choose.  In that case, we re-start the process and want to be able to
	 * resume this loop where we left off.
	 */
	for (; i < max; i++) {
		p = start + i;
		if (p >= max)
			p -= max;
		if ((*avail = HEAP_SPACE(dbp, rpage, p)) > space)
			continue;
		data_pgno = region_pgno + p + 1;
		ACQUIRE_CUR(dbc,
		    DB_LOCK_WRITE, data_pgno, DB_LOCK_NOWAIT, 0, ret);
		/*
		 * If we have the lock and the page or have the lock and need to
		 * create the page, we're good.  If we don't have the lock, try
		 * to find different page.
		 */
		if (ret == 0 || ret == DB_PAGE_NOTFOUND)
			break;
		else if (ret == DB_LOCK_NOTGRANTED || ret == DB_LOCK_DEADLOCK) {
			ret = 0;
			continue;
		} else
			goto err;
	}

	/*
	 * Keep a worst case range of highest used page in the region.
	 */
	if (i < max && data_pgno > rpage->high_pgno) {
		if ((ret = __memp_dirty(mpf,
		    &rpage, dbc->thread_info, NULL, dbc->priority, 0)) != 0)
			goto err;
		/* We might have blocked, check again */
		if (data_pgno > rpage->high_pgno)
			rpage->high_pgno = data_pgno;
	}

	/* Done with the region page, even if we didn't find a page. */
	if ((ret = __memp_fput(mpf,
	    dbc->thread_info, rpage, dbc->priority)) != 0) {
		/* Did not read the data page, so we can release its lock. */
		DISCARD(dbc, cp->page, cp->lock, 0, t_ret);
		goto err;
	}
	rpage = NULL;

	if (i >= max) {
		/*
		 * No free pages on this region page, advance to the next region
		 * page.  If we're at the end of a fixed size heap db, loop
		 * around to the first region page.  There is not currently a
		 * data page locked.
		 */
next_region:	region_pgno += HEAP_REGION_SIZE(dbp) + 1;

		if (region_pgno > h->maxpgno)
			region_pgno = FIRST_HEAP_RPAGE;

		if (region_pgno == start_region) {
			/*
			 * We're in a fixed size db and we've looped through all
			 * region pages.
			 */

			if (lk_mode == DB_MPOOL_TRY) {
				/*
				 * We may have missed a region page with room,
				 * because we didn't wait for locked pages.  Try
				 * another loop, waiting for all pages.
				 */
				lk_mode = 0;
			} else {
				/*
				 * We've seen every region page, because we
				 * waited for all pages.  No room.
				 */
				ret = DB_HEAP_FULL;
				goto err;
			}
		}

		h->curregion = region_pgno;
		h->curpgindx = 0;
		i = 0;
		goto find;
	}

	/*
	 * At this point we have the page locked.  If we have the page, we need
	 * to mark it dirty.  If we don't have the page (or if the page is
	 * empty) we need to create and initialize it.
	 */
	if (cp->pgno == PGNO_INVALID || PGNO(cp->page) == PGNO_INVALID) {
		/*
		 * The data page needs to be created and the metadata page needs
		 * to be updated.  Once we get the metadata page, we must not
		 * jump to err, the metadata page and lock are put back here.
		 *
		 * It is possible that the page was created by an aborted txn,
		 * in which case the page exists but is all zeros.  We still
		 * need to "create" it and log the creation.
		 *
		 */

		meta_pgno = PGNO_BASE_MD;
		if ((ret = __db_lget(dbc, LCK_ALWAYS, meta_pgno,
		    DB_LOCK_WRITE, DB_LOCK_NOWAIT, &meta_lock)) != 0) {
			/*
			 * We don't want to block while having latched
			 * a page off the end of file.  This could
			 * get truncated by another thread and we
			 * will deadlock.
			 */
			p = cp->page != NULL;
			DISCARD(dbc, cp->page, cp->lock, 0, t_ret);
			if (t_ret != 0 ||
			     (ret != DB_LOCK_NOTGRANTED &&
			     ret != DB_LOCK_DEADLOCK))
				goto pg_err;
			if ((ret = __db_lget(dbc, LCK_ALWAYS, meta_pgno,
			    DB_LOCK_WRITE, 0, &meta_lock)) != 0)
				goto pg_err;
			ACQUIRE_CUR(dbc, DB_LOCK_WRITE,
			    data_pgno, 0, DB_MPOOL_CREATE, ret);
			/*
			 * We can race, having read this page when it was
			 * less than last_pgno but now an aborted
			 * allocation can make this page beyond last_pgno
			 * so we must free it. If we can't get the
			 * lock on the page again, then some other
			 * thread will handle the issue.
			 */
			if (ret != 0) {
pg_err:				if (p != 0) {
					ACQUIRE_CUR(dbc, DB_LOCK_WRITE,
					    data_pgno, 0, 0, t_ret);
					if (t_ret == 0 &&
					     PGNO(cp->page) == PGNO_INVALID) {
						(void)__memp_fput(mpf,
						     dbc->thread_info,
						     cp->page, dbc->priority);
						(void)__memp_fget(mpf,
						     &data_pgno,
						     dbc->thread_info, dbc->txn,
						     DB_MPOOL_FREE, &cp->page);
					}
					(void)__LPUT(dbc, cp->lock);
				}
				(void)__LPUT(dbc, meta_lock);
				goto err;
			}
			/* Check if we lost a race. */
			if (PGNO(cp->page) != PGNO_INVALID) {
				if ((ret = __LPUT(dbc, meta_lock)) != 0)
					goto err;
				goto check;
			}
		}

		/*
		 * Before creating a new page in this region, check that the
		 * region page still exists.  By this point, the transaction
		 * that created the region must have aborted or committed,
		 * because we now hold the metadata lock.  If we can't get the
		 * latch, the page must exist.
		 */
		ret = __memp_fget(mpf, &region_pgno,
		    dbc->thread_info, NULL, DB_MPOOL_TRY, &rpage);
		if (ret == DB_LOCK_NOTGRANTED)
			ret = 0;
		else if (ret != 0) {
			/* 
			 * Free up the metadata lock.  If this was an error
			 * other than a missing region page, bail.
			 */
			if ((t_ret = __LPUT(dbc, meta_lock)) != 0)
				ret = t_ret;
			if (ret != DB_PAGE_NOTFOUND)
				goto err;
			/*
			 * The region no longer exists.  Release the page's lock
			 * (we haven't created the page yet) and find a new page
			 * on a different region.
			 */
			DISCARD(dbc, cp->page, cp->lock, 0, t_ret);
			goto find;
		} else
			ret = __memp_fput(mpf,
			    dbc->thread_info, rpage, dbc->priority);
		rpage = NULL;
		if (ret != 0)
			goto meta_unlock;

		if ((ret = __memp_fget(mpf, &meta_pgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &meta)) != 0)
			goto err;

		/* Log the page creation.  Can't jump to err if it fails. */
		if (DBC_LOGGING(dbc))
			ret = __heap_pg_alloc_log(dbp,
			    dbc->txn, &LSN(meta), 0, &LSN(meta), meta_pgno,
				data_pgno, (u_int32_t)P_HEAP, meta->last_pgno);
		else
			LSN_NOT_LOGGED(LSN(meta));

		/*
		 * We may have created a page earlier with a larger page number
		 * check before updating the metadata page.
		 */
		if (ret == 0 && data_pgno > meta->last_pgno)
			meta->last_pgno = data_pgno;
		meta_lsn = LSN(meta);

		if ((t_ret = __memp_fput(mpf,
		    dbc->thread_info, meta, dbc->priority)) != 0 && ret == 0)
			ret = t_ret;
		meta = NULL;
		if (ret != 0)
			goto meta_unlock;

		/* If the page doesn't actually exist we need to create it. */
		if (cp->pgno == PGNO_INVALID) {
			cp->pgno = data_pgno;
			if ((ret = __memp_fget(mpf, &cp->pgno,
			    dbc->thread_info, dbc->txn,
			    DB_MPOOL_CREATE | DB_MPOOL_DIRTY, &cp->page)) != 0)
				goto meta_unlock;
			DB_ASSERT(dbp->env, cp->pgno == data_pgno);
		} else if ((ret = __memp_dirty(mpf, &cp->page,
		    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0) {
			/* Did not read the page, so we can release the lock. */
			DISCARD(dbc, cp->page, cp->lock, 0, t_ret);
			goto meta_unlock;
		}

		/* Now that we have the page we initialize it and we're done. */
		P_INIT(cp->page,
		    dbp->pgsize, cp->pgno, P_INVALID, P_INVALID, 0, P_HEAP);
		LSN(cp->page) = meta_lsn;

meta_unlock:	if ((t_ret = __TLPUT(dbc, meta_lock)) != 0 && ret == 0)
			ret = t_ret;
		if (ret != 0)
			goto err;
	} else {
		/* Check whether we actually have enough space on this page. */
check:		if (size + sizeof(db_indx_t) > HEAP_FREESPACE(dbp, cp->page)) {
			/* Put back the page and lock, they were never used. */
			DISCARD(dbc, cp->page, cp->lock, 0, ret);
			if (ret != 0)
				goto err;

			/* Re-start the bitmap check on the next page. */
			i++;
			goto find;
		}

		if ((ret = __memp_dirty(mpf, &cp->page,
		    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0) {
			/* Did not read the page, so we can release the lock. */
			DISCARD(dbc, cp->page, cp->lock, 0, t_ret);
			goto err;
		}
	}

	h->curpgindx = data_pgno - region_pgno - 1;
err:	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	if (rpage != NULL && (t_ret = __memp_fput(mpf,
	    dbc->thread_info, rpage, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __heap_append --
 *	Add an item to a heap database.
 *
 * PUBLIC: int __heap_append
 * PUBLIC:     __P((DBC *, DBT *, DBT *));
 */
int
__heap_append(dbc, key, data)
	DBC *dbc;
	DBT *data, *key;
{
	DB *dbp;
	DBT tmp_dbt;
	DB_HEAP_RID rid;
	DB_MPOOLFILE *mpf;
	HEAPPG *rpage;
	HEAPHDR hdr;
	HEAP_CURSOR *cp;
	db_indx_t indx;
	db_pgno_t region_pgno;
	int ret, space, t_ret;
	u_int8_t avail;
	u_int32_t data_size;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	ret = t_ret = 0;
	rpage = NULL;
	cp = (HEAP_CURSOR *)dbc->internal;

	/* Need data.size + header size, 4-byte aligned. */
	if (F_ISSET(data, DB_DBT_PARTIAL))
		data_size = DB_ALIGN(data->doff +
		    data->size + sizeof(HEAPHDR), sizeof(u_int32_t));
	else
		data_size = DB_ALIGN(
		    data->size + sizeof(HEAPHDR), sizeof(u_int32_t));

	if (data_size >= HEAP_MAXDATASIZE(dbp))
		return (__heapc_split(dbc, key, data, 1));
	else if (data_size < sizeof(HEAPSPLITHDR))
		data_size = sizeof(HEAPSPLITHDR);

	if ((ret = __heap_getpage(dbc, data_size, &avail)) != 0)
		goto err;

	indx = HEAP_FREEINDX(cp->page);
	memset(&hdr, 0, sizeof(HEAPHDR));
	hdr.size = data->size;
	if (F_ISSET(data, DB_DBT_PARTIAL))
		hdr.size += data->doff;
	tmp_dbt.data = &hdr;
	tmp_dbt.size = sizeof(HEAPHDR);

	/* Log the write. */
	if (DBC_LOGGING(dbc)) {
		if ((ret = __heap_addrem_log(dbp, dbc->txn, &LSN(cp->page),
		    0, DB_ADD_HEAP, cp->pgno, (u_int32_t)indx,
		    data_size, &tmp_dbt, data, &LSN(cp->page))) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(cp->page));

	if ((ret = __heap_pitem(
	    dbc, (PAGE *)cp->page, indx, data_size, &tmp_dbt, data)) != 0)
		goto err;

	rid.pgno = cp->pgno;
	rid.indx = indx;
	cp->indx = indx;

	/* Check whether we need to update the space bitmap. */
	HEAP_CALCSPACEBITS(dbp, HEAP_FREESPACE(dbp, cp->page), space);

	if (space != avail) {
		/* Get the region page with an exclusive latch. */
		region_pgno = HEAP_REGION_PGNO(dbp, cp->pgno);
		if ((ret = __memp_fget(mpf, &region_pgno,
		    dbc->thread_info, NULL, DB_MPOOL_DIRTY, &rpage)) != 0)
			goto err;

		HEAP_SETSPACE(dbp, rpage, cp->pgno - region_pgno - 1, space);
	}

err:	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	if (rpage != NULL && (t_ret = __memp_fput(mpf,
	    dbc->thread_info, rpage, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;

	if (cp->page != NULL) {
		DISCARD(dbc, cp->page, cp->lock, 1, t_ret);
		if (ret == 0)
			ret = t_ret;
	}

	if (ret == 0 && key != NULL)
		ret = __db_retcopy(dbp->env, key,
		    &rid, DB_HEAP_RID_SZ, &dbc->rkey->data, &dbc->rkey->ulen);

	return (ret);
}

static int
__heapc_split(dbc, key, data, is_first)
	DBC *dbc;
	DBT *key, *data;
	int is_first;
{
	DB *dbp;
	DBT hdr_dbt, t_data;
	DB_HEAP_RID rid;
	DB_MPOOLFILE *mpf;
	HEAPPG *rpage;
	HEAPSPLITHDR hdrs;
	HEAP_CURSOR *cp;
	db_indx_t indx;
	db_pgno_t region_pgno;
	int ret, spacebits, t_ret;
	u_int32_t buflen, doff, left, size;
	u_int8_t availbits, *buf;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	cp = (HEAP_CURSOR *)dbc->internal;
	memset(&hdrs, 0, sizeof(HEAPSPLITHDR));
	memset(&t_data, 0, sizeof(DBT));
	hdrs.std_hdr.flags = HEAP_RECSPLIT | HEAP_RECLAST;

	doff = data->doff;
	rpage = NULL;
	ret = t_ret = 0;
	indx = 0;
	buf = NULL;
	buflen = 0;

	/*
	 * Write the record to multiple pages, in chunks starting from the end.
	 * To reconstruct during a get we need the RID of the next chunk, so if
	 * work our way from back to front during writing we always know the rid
	 * of the "next" chunk, it's the chunk we just wrote.
	 */
	t_data.data = (u_int8_t *)data->data + data->size;
	left = data->size;
	if (F_ISSET(data, DB_DBT_PARTIAL)) {
		left += data->doff;
	}
	hdrs.tsize = left;
	while (left > 0) {
		size = DB_ALIGN(left + sizeof(HEAPSPLITHDR), sizeof(u_int32_t));
		if (size < sizeof(HEAPSPLITHDR))
			size = sizeof(HEAPSPLITHDR);

		if (size > HEAP_MAXDATASIZE(dbp))
			/*
			 * Data won't fit on a single page, find one at least
			 * 33% free.
			 */
			size = DB_ALIGN(dbp->pgsize / 3, sizeof(u_int32_t));
		else
			hdrs.std_hdr.flags |= HEAP_RECFIRST;

		if ((ret = __heap_getpage(dbc, size, &availbits)) != 0)
			return (ret);

		/*
		 * size is the total number of bytes being written to the page.
		 * The header holds the size of the data being written.
		 */
		if (F_ISSET(&(hdrs.std_hdr), HEAP_RECFIRST)) {
			hdrs.std_hdr.size = left;
			/*
			 * If we're called from heapc_reloc, we are only writing
			 * a piece of the full record and shouldn't set
			 * HEAP_RECFIRST.
			 */
			if (!is_first)
				F_CLR(&(hdrs.std_hdr), HEAP_RECFIRST);
		} else {
			/*
			 * Figure out how much room is on the page.  If we will
			 * have to expand the offset table, account for that.
			 */
			size = HEAP_FREESPACE(dbp, cp->page);
			if (NUM_ENT(cp->page) == 0 ||
			    HEAP_FREEINDX(cp->page) > HEAP_HIGHINDX(cp->page))
				size -= sizeof(db_indx_t);
			/* Round down to a multiple of 4. */
			size = DB_ALIGN(
			    size - sizeof(u_int32_t) + 1, sizeof(u_int32_t));
			DB_ASSERT(dbp->env, size >= sizeof(HEAPSPLITHDR));
			hdrs.std_hdr.size =
			    (u_int16_t)(size - sizeof(HEAPSPLITHDR));
		}

		/*
		 * t_data.data points at the end of the data left to write.  Now
		 * that we know how much we're going to write to this page, we
		 * can adjust the pointer to point at the start of the data to
		 * be written.
		 *
		 * If DB_DBT_PARTIAL is set, once data->data is exhausted, we
		 * have to pad with data->doff bytes (or as much as can fit on
		 * this page.)  left - doff gives the number of bytes to use
		 * from data->data.  Once that can't fill t_data, we have to
		 * start padding.
		 */
		t_data.data = (u_int8_t *)(t_data.data) - hdrs.std_hdr.size;
		DB_ASSERT(dbp->env, (F_ISSET(data, DB_DBT_PARTIAL) ||
		    t_data.data >= data->data));
		t_data.size = hdrs.std_hdr.size;
		if (F_ISSET(data, DB_DBT_PARTIAL) &&
		    t_data.size > left - doff) {
			if (buflen < t_data.size) {
				if (__os_realloc(
				    dbp->env, t_data.size, &buf) != 0)
					return (ENOMEM);
				buflen = t_data.size;
			}
			/*
			 * We have to figure out how much data remains.  left
			 * includes doff, so we need (left - doff) bytes from
			 * data.  We also need the amount of padding that can
			 * fit on the page.  That's the amount we can fit on the
			 * page minus the bytes we're taking from data.
			*/
			t_data.data = buf;
			memset(buf, '\0', t_data.size - left + doff);
			buf += t_data.size - left + doff;
			memcpy(buf, data->data, left - doff);
			doff -= t_data.size - left + doff;
			buf = t_data.data;
		}
		hdr_dbt.data = &hdrs;
		hdr_dbt.size = sizeof(HEAPSPLITHDR);
		indx = HEAP_FREEINDX(cp->page);

		/* Log the write. */
		if (DBC_LOGGING(dbc)) {
			if ((ret = __heap_addrem_log(dbp,
			    dbc->txn, &LSN(cp->page), 0,
			    DB_ADD_HEAP, cp->pgno, (u_int32_t)indx,
			    size, &hdr_dbt, &t_data, &LSN(cp->page))) != 0)
				goto err;
		} else
			LSN_NOT_LOGGED(LSN(cp->page));

		if ((ret = __heap_pitem(dbc,
		    (PAGE *)cp->page, indx, size, &hdr_dbt, &t_data)) != 0)
			goto err;
		F_CLR(&(hdrs.std_hdr), HEAP_RECLAST);
		left -= hdrs.std_hdr.size;

		/*
		 * Save the rid where we just wrote, this is the "next"
		 * chunk.
		 */
		hdrs.nextpg = cp->pgno;
		hdrs.nextindx = indx;

		/* Check whether we need to update the space bitmap. */
		HEAP_CALCSPACEBITS(dbp,
		    HEAP_FREESPACE(dbp, cp->page), spacebits);

		if (spacebits != availbits) {
			/* Get the region page with an exclusive latch. */
			region_pgno = HEAP_REGION_PGNO(dbp, cp->pgno);
			if ((ret = __memp_fget(mpf, &region_pgno,
			    dbc->thread_info,
			    NULL, DB_MPOOL_DIRTY, &rpage)) != 0)
				goto err;

			HEAP_SETSPACE(dbp,
			    rpage, cp->pgno - region_pgno - 1, spacebits);
			ret = __memp_fput(mpf,
			    dbc->thread_info, rpage, dbc->priority);
			rpage = NULL;
			if (ret != 0)
				goto err;
		}

	}

	rid.pgno = cp->pgno;
	rid.indx = indx;
	cp->indx = indx;

err:	if (rpage != NULL && (t_ret = __memp_fput(mpf,
	    dbc->thread_info, rpage, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	if (cp->page != NULL) {
		DISCARD(dbc, cp->page, cp->lock, 1, t_ret);
		if (ret == 0)
			ret = t_ret;
	}
	if (buf != NULL)
		__os_free(dbp->env, buf);

	if (ret == 0 && key != NULL)
		ret = __db_retcopy(dbp->env, key,
		    &rid, DB_HEAP_RID_SZ, &dbc->rkey->data, &dbc->rkey->ulen);
	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	return (ret);
}

/*
 * __heapc_pitem --
 *	Put an item on a heap page.  Copy all bytes from the header (if any)
 *	first and then copy from data.
 *
 * PUBLIC: int __heap_pitem __P((DBC *,
 * PUBLIC:	PAGE *, u_int32_t, u_int32_t, DBT *, DBT *));
 */
int
__heap_pitem(dbc, pagep, indx, nbytes, hdr, data)
	DBC *dbc;
	PAGE *pagep;
	u_int32_t indx;
	u_int32_t nbytes;
	DBT *hdr, *data;
{
	DB *dbp;
	u_int8_t *buf;

	dbp = dbc->dbp;

	DB_ASSERT(dbp->env, TYPE(pagep) == P_HEAP);
	DB_ASSERT(dbp->env, IS_DIRTY(pagep));
	DB_ASSERT(dbp->env, nbytes == DB_ALIGN(nbytes, sizeof(u_int32_t)));
	DB_ASSERT(dbp->env, DB_ALIGN(((HEAPHDR *)hdr->data)->size,
	    sizeof(u_int32_t)) >= data->size);
	DB_ASSERT(dbp->env, nbytes >= hdr->size + data->size);

	/*
	 * We're writing data either as a result of DB->put or as a result of
	 * undo-ing a delete.	If we're undo-ing a delete we just need to write
	 * the bytes from hdr to the page.  Otherwise, we need to construct a
	 * heap header, etc.
	 */
	HEAP_OFFSETTBL(dbp, pagep)[indx] = HOFFSET(pagep) - nbytes;
	buf = P_ENTRY(dbp, pagep, indx);
	DB_ASSERT(dbp->env, buf > (u_int8_t*)&HEAP_OFFSETTBL(dbp, pagep)[indx]);

	if (hdr != NULL) {
		memcpy(buf, hdr->data, hdr->size);
		buf += hdr->size;
	}
	if (F_ISSET(data, DB_DBT_PARTIAL)) {
		memset(buf, 0, data->doff);
		buf += data->doff;
	}
	memcpy(buf, data->data, data->size);

	/*
	 * Update data page header.  If DEBUG/DIAGNOSTIC is set, the page might
	 * be filled with 0xdb, so we can't just look for a 0 in the offset
	 * table.  We used the first available index, so start there and scan
	 * forward.  If the table is full, the first available index is the
	 * highest index plus one.
	 */
	if (indx > HEAP_HIGHINDX(pagep)) {
		if (NUM_ENT(pagep) == 0)
			HEAP_FREEINDX(pagep) = 0;
		else if (HEAP_FREEINDX(pagep) >= indx) {
			if (indx > (u_int32_t)HEAP_HIGHINDX(pagep) + 1)
				HEAP_FREEINDX(pagep) = HEAP_HIGHINDX(pagep) + 1;
			else
				HEAP_FREEINDX(pagep) = indx + 1;
		}
		while (++HEAP_HIGHINDX(pagep) < indx)
			HEAP_OFFSETTBL(dbp,pagep)[HEAP_HIGHINDX(pagep)] = 0;
	} else {
		for (; indx <= HEAP_HIGHINDX(pagep); indx++)
			if (HEAP_OFFSETTBL(dbp, pagep)[indx] == 0)
				break;
		HEAP_FREEINDX(pagep) = indx;
	}
	HOFFSET(pagep) -= nbytes;
	NUM_ENT(pagep)++;

	return (0);
}

/*
 * __heapc_dup --
 *      Duplicate a heap cursor, such that the new one holds appropriate
 *      locks for the position of the original.
 *
 * PUBLIC: int __heapc_dup __P((DBC *, DBC *));
 */
int
__heapc_dup(orig_dbc, new_dbc)
	DBC *orig_dbc, *new_dbc;
{
	HEAP_CURSOR *orig, *new;

	orig = (HEAP_CURSOR *)orig_dbc->internal;
	new = (HEAP_CURSOR *)new_dbc->internal;
	new->flags = orig->flags;
	return (0);
}

/*
 * __heapc_gsplit --
 *      Get a heap split record.  The page pointed to by the cursor must
 *	be the first segment of this record.
 *
 * PUBLIC: int __heapc_gsplit __P((DBC *,
 * PUBLIC:     DBT *, void **, u_int32_t *));
 */
int
__heapc_gsplit(dbc, dbt, bpp, bpsz)
	DBC *dbc;
	DBT *dbt;
	void **bpp;
	u_int32_t *bpsz;
{
	DB *dbp;
	DB_MPOOLFILE *mpf;
	DB_HEAP_RID rid;
	DB_LOCK data_lock;
	HEAP_CURSOR *cp;
	ENV *env;
	HEAPPG *dpage;
	HEAPSPLITHDR *hdr;
	db_indx_t bytes;
	u_int32_t curoff, needed, start, tlen;
	u_int8_t *p, *src;
	int putpage, ret, t_ret;

	LOCK_INIT(data_lock);
	dbp = dbc->dbp;
	env = dbp->env;
	mpf = dbp->mpf;
	cp = (HEAP_CURSOR *)dbc->internal;
	putpage = FALSE;
	ret = 0;

	/*
	 * We should have first page, locked already in cursor.  Get the
	 * record id out of the cursor and set up local variables.
	 */
	DB_ASSERT(env, cp->page != NULL);
	rid.pgno = cp->pgno;
	rid.indx = cp->indx;
	dpage = cp->page;
	hdr = (HEAPSPLITHDR *)P_ENTRY(dbp, dpage, rid.indx);
	DB_ASSERT(env, hdr->tsize != 0);
	tlen = hdr->tsize;

	/*
	 * If we doing a partial retrieval, figure out how much we are
	 * actually going to get.
	 */
	if (F_ISSET(dbt, DB_DBT_PARTIAL)) {
		start = dbt->doff;
		if (start > tlen)
			needed = 0;
		else if (dbt->dlen > tlen - start)
			needed = tlen - start;
		else
			needed = dbt->dlen;
	} else {
		start = 0;
		needed = tlen;
	}

	/*
	 * If the caller has not requested any data, return success. This
	 * "early-out" also avoids setting up the streaming optimization when
	 * no page would be retrieved. If it were removed, the streaming code
	 * should only initialize when needed is not 0.
	 */
	if (needed == 0) {
		dbt->size = 0;
		return (0);
	}

	/*
	 * Check if the buffer is big enough; if it is not and we are
	 * allowed to malloc space, then we'll malloc it.  If we are
	 * not (DB_DBT_USERMEM), then we'll set the dbt and return
	 * appropriately.
	 */
	if (F_ISSET(dbt, DB_DBT_USERCOPY))
		goto skip_alloc;

	/* Allocate any necessary memory. */
	if (F_ISSET(dbt, DB_DBT_USERMEM)) {
		if (needed > dbt->ulen) {
			dbt->size = needed;
			return (DB_BUFFER_SMALL);
		}
	} else if (F_ISSET(dbt, DB_DBT_MALLOC)) {
		if ((ret = __os_umalloc(env, needed, &dbt->data)) != 0)
			return (ret);
	} else if (F_ISSET(dbt, DB_DBT_REALLOC)) {
		if ((ret = __os_urealloc(env, needed, &dbt->data)) != 0)
			return (ret);
	} else if (bpsz != NULL && (*bpsz == 0 || *bpsz < needed)) {
		if ((ret = __os_realloc(env, needed, bpp)) != 0)
			return (ret);
		*bpsz = needed;
		dbt->data = *bpp;
	} else if (bpp != NULL)
		dbt->data = *bpp;
	else {
		DB_ASSERT(env,
		    F_ISSET(dbt,
		    DB_DBT_USERMEM | DB_DBT_MALLOC | DB_DBT_REALLOC) ||
		    bpsz != NULL || bpp != NULL);
		return (DB_BUFFER_SMALL);
	}

skip_alloc:
	/*
	 * Go through each of the pieces, copying the data on each one
	 * into the buffer.  Never copy more than the total data length.
	 * We are starting off with the page that is currently pointed to by
	 * the cursor,
	 */
	curoff = 0;
	dbt->size = needed;
	for (p = dbt->data; needed > 0;) {
		/* Check if we need any bytes from this page */
		if (curoff + hdr->std_hdr.size >= start) {
			bytes = hdr->std_hdr.size;
			src = (u_int8_t *)hdr +
			    P_TO_UINT16(sizeof(HEAPSPLITHDR));
			if (start > curoff) {
				src += start - curoff;
				bytes -= start - curoff;
			}
			if (bytes > needed)
				bytes = needed;
			if (F_ISSET(dbt, DB_DBT_USERCOPY)) {
				/*
				 * The offset into the DBT is the total size
				 * less the amount of data still needed.  Care
				 * needs to be taken if doing a partial copy
				 * beginning at an offset other than 0.
				 */
				if ((ret = env->dbt_usercopy(
				    dbt, dbt->size - needed,
				    src, bytes, DB_USERCOPY_SETDATA)) != 0) {
					if (putpage)
						(void)__memp_fput(
						    mpf, dbc->thread_info,
						    dpage, dbp->priority);

					return (ret);
				}
			} else
				memcpy(p, src, bytes);
			p += bytes;
			needed -= bytes;
		}
		curoff += hdr->std_hdr.size;

		/* Find next record piece as long as it exists */
		if (!F_ISSET((HEAPHDR *)hdr, HEAP_RECLAST)) {
			rid.pgno = hdr->nextpg;
			rid.indx = hdr->nextindx;

			/*
			 * First pass through here, we are using the
			 * page pointed to by the cursor, and this page
			 * will get put when the cursor is is closed.
			 * Only pages specifically gotten in this loop
			 * need to be put back.
			 */
			if (putpage) {
				if ((ret = __memp_fput(mpf, dbc->thread_info,
				    dpage, dbp->priority) ) != 0)
					goto err;
				dpage = NULL;
				if ((ret = __TLPUT(dbc, data_lock)) != 0)
					goto err;
			}

			if ((ret = __db_lget(dbc, 0, rid.pgno,
			    DB_LOCK_READ, 0, &data_lock)) != 0)
				goto err;
			if ((ret = __memp_fget(mpf, &rid.pgno,
			    dbc->thread_info, dbc->txn, 0, &dpage)) != 0)
				goto err;
			hdr = (HEAPSPLITHDR *)P_ENTRY(dbp, dpage, rid.indx);
			putpage = TRUE;

			/*
			 * If we have the last piece of this record and we're
			 * reading the entire record, then what we need should
			 * equal what is remaining.
			 */
			if (F_ISSET((HEAPHDR *)hdr, HEAP_RECLAST) &&
			    !F_ISSET(dbt, DB_DBT_PARTIAL) &&
			    (hdr->std_hdr.size != needed)) {
				__db_errx(env, DB_STR_A("1167",
			     "Incorrect record size in header: %s: rid %lu.%lu",
				    "%s %lu %lu"), dbc->dbp->fname,
				    (u_long)(cp->pgno), (u_long)(cp->indx));
				ret = __env_panic(env, DB_RUNRECOVERY);
				goto err;
			}
		}
	}

err:	DB_ASSERT(dbp->env, ret != DB_PAGE_NOTFOUND);
	if (putpage && dpage != NULL && (t_ret = __memp_fput(mpf,
	    dbc->thread_info, dpage, dbp->priority)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, data_lock)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}
/*
 * __heapc_refresh --
 *      do the proper set up for cursor reuse.
 *
 * PUBLIC: int __heapc_refresh __P((DBC *));
 */
int
__heapc_refresh(dbc)
	DBC *dbc;
{
	HEAP_CURSOR *cp;

	cp = (HEAP_CURSOR *)dbc->internal;

	LOCK_INIT(cp->lock);
	cp->lock_mode = DB_LOCK_NG;
	cp->flags = 0;

	return (0);
}
