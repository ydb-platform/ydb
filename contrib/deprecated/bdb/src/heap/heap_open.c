/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2012 Oracle and/or its affiliates.  All rights reserved.
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_swap.h"
#include "dbinc/fop.h"
#include "dbinc/heap.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"

static void __heap_init_meta __P((DB *, HEAPMETA *, db_pgno_t, DB_LSN*));

/*
 * __heap_open --
 *	Open a heap.
 *
 * PUBLIC: int __heap_open __P((DB *, DB_THREAD_INFO *,
 * PUBLIC:      DB_TXN *, const char *, db_pgno_t, u_int32_t));
 */
int
__heap_open(dbp, ip, txn, name, base_pgno, flags)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	const char *name;
	db_pgno_t base_pgno;
	u_int32_t flags;
{
	HEAP *h;
	db_pgno_t npgs;
	int ret;

	h = (HEAP *)dbp->heap_internal;
	COMPQUIET(name, NULL);

	ret = __heap_read_meta(dbp, ip, txn, base_pgno, flags);

	if (h->gbytes != 0 || h->bytes != 0) {
		/*
		 * We don't have to worry about rounding with gbytes, as pgsize
		 * is always a multiple of 2, but we round up if bytes isn't
		 * a multiple of the page size.
		 */
		npgs = (db_pgno_t)(h->gbytes * (GIGABYTE / dbp->pgsize));
		npgs += (db_pgno_t)((h->bytes +dbp->pgsize - 1)/ dbp->pgsize);
		h->maxpgno = npgs - 1;
		if (h->maxpgno < FIRST_HEAP_DPAGE) {
			__db_errx(dbp->env,
			    "requested database size is too small");
			return (EINVAL);
		}
	} else
		/* If not fixed size heap, set maxregion to maximum value */
		h->maxpgno = UINT32_MAX;

	return (ret);
}

/*
 * __heap_metachk --
 *
 * PUBLIC: int __heap_metachk __P((DB *, const char *, HEAPMETA *));
 */
int
__heap_metachk(dbp, name, hm)
	DB *dbp;
	const char *name;
	HEAPMETA *hm;
{
	ENV *env;
	HEAP *h;
	int ret;
	u_int32_t vers;

	env = dbp->env;
	h = (HEAP *)dbp->heap_internal;

	/*
	 * At this point, all we know is that the magic number is for a Heap.
	 * Check the version, the database may be out of date.
	 */
	vers = hm->dbmeta.version;
	if (F_ISSET(dbp, DB_AM_SWAP))
		M_32_SWAP(vers);
	switch (vers) {
	case 1:
		break;
	default:
		__db_errx(env,
		    "%s: unsupported heap version: %lu", name, (u_long)vers);
		return (EINVAL);
	}

	/* Swap the page if needed. */
	if (F_ISSET(dbp, DB_AM_SWAP) &&
	    (ret = __heap_mswap(env, (PAGE *)hm)) != 0)
		return (ret);

	/* Check application info against metadata info. */
	if (h->gbytes != 0 || h->bytes != 0)
		if (h->gbytes != hm->gbytes || h->bytes != hm->bytes) {
			__db_errx(env, DB_STR_A("1155",
		  "%s: specified heap size does not match size set in database",
			    "%s"), name);
			return (EINVAL);
		}

	/* Set the page size. */
	dbp->pgsize = hm->dbmeta.pagesize;

	/* Copy the file's ID. */
	memcpy(dbp->fileid, hm->dbmeta.uid, DB_FILE_ID_LEN);

	return (0);
}

/*
 * __heap_read_meta --
 *	Read the meta page and set up the internal structure.
 *
 * PUBLIC: int __heap_read_meta __P((DB *,
 * PUBLIC:	DB_THREAD_INFO *, DB_TXN *, db_pgno_t, u_int32_t));
 */
int
__heap_read_meta(dbp, ip, txn, meta_pgno, flags)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	db_pgno_t meta_pgno;
	u_int32_t flags;
{
	DBC *dbc;
	DB_LOCK metalock;
	DB_MPOOLFILE *mpf;
	HEAPMETA *meta;
	HEAP *h;
	int ret, t_ret;

	COMPQUIET(flags, 0);

	meta = NULL;
	h = dbp->heap_internal;
	LOCK_INIT(metalock);
	mpf = dbp->mpf;
	ret = 0;

	/* Get a cursor.  */
	if ((ret = __db_cursor(dbp, ip, txn, &dbc, 0)) != 0)
		return (ret);

	/* Get the metadata page. */
	if ((ret =
	    __db_lget(dbc, 0, meta_pgno, DB_LOCK_READ, 0, &metalock)) != 0)
		goto err;
	if ((ret = __memp_fget(mpf, &meta_pgno, ip, dbc->txn, 0, &meta)) != 0)
		goto err;

	/*
	 * If the magic number is set, the heap has been created.  Correct
	 * any fields that may not be right.  Note, all of the local flags
	 * were set by DB->open.
	 *
	 * Otherwise, we'd better be in recovery or abort, in which case the
	 * metadata page will be created/initialized elsewhere.
	 */
	if (meta->dbmeta.magic == DB_HEAPMAGIC) {
		h->curregion = meta->curregion;
		h->curpgindx = 0;
		h->gbytes = meta->gbytes;
		h->bytes = meta->bytes;
		h->region_size = meta->region_size;

		if (PGNO(meta) == PGNO_BASE_MD && !F_ISSET(dbp, DB_AM_RECOVER))
			__memp_set_last_pgno(mpf, meta->dbmeta.last_pgno);
	} else {
		DB_ASSERT(dbp->env,
		    IS_RECOVERING(dbp->env) || F_ISSET(dbp, DB_AM_RECOVER));
	}

err:	/* Put the metadata page back. */
	if (meta != NULL && (t_ret = __memp_fput(mpf,
	    ip, meta, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __LPUT(dbc, metalock)) != 0 && ret == 0)
		ret = t_ret;

	if ((t_ret = __dbc_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __heap_new_file --
 * Create the necessary pages to begin a new database file.
 *
 * PUBLIC: int __heap_new_file __P((DB *,
 * PUBLIC:      DB_THREAD_INFO *, DB_TXN *, DB_FH *, const char *));
 */
int
__heap_new_file(dbp, ip, txn, fhp, name)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	DB_FH *fhp;
	const char *name;
{
	DBT pdbt;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	DB_PGINFO pginfo;
	ENV *env;
	HEAP *h;
	HEAPMETA *meta;
	HEAPPG *region;
	db_pgno_t pgno;
	int ret, t_ret;
	u_int32_t max_size;
	void *buf;

	env = dbp->env;
	mpf = dbp->mpf;
	buf = NULL;
	h = (HEAP *)dbp->heap_internal;
	max_size = HEAP_REGION_COUNT(dbp, dbp->pgsize);

	if (h->region_size == 0)
		h->region_size = HEAP_DEFAULT_REGION_MAX(dbp) > max_size ?
		    max_size : HEAP_DEFAULT_REGION_MAX(dbp);
	else if (h->region_size > max_size) {
		__db_errx(dbp->env, DB_STR_A("1169",
		    "region size may not be larger than %lu",
		    "%lu"), (u_long)max_size);
		return (EINVAL);
	}

	if (F_ISSET(dbp, DB_AM_INMEM)) {
		/* Build the meta-data page. */
		pgno = PGNO_BASE_MD;
		if ((ret = __memp_fget(mpf, &pgno,
		    ip, txn, DB_MPOOL_CREATE | DB_MPOOL_DIRTY, &meta)) != 0)
			return (ret);
		LSN_NOT_LOGGED(lsn);
		__heap_init_meta(dbp, meta, PGNO_BASE_MD, &lsn);
		ret = __db_log_page(dbp, txn, &lsn, pgno, (PAGE *)meta);
		if ((t_ret =
		    __memp_fput(mpf, ip, meta, dbp->priority)) != 0 && ret == 0)
			ret = t_ret;
		meta = NULL;
		if (ret != 0)
			goto err;

		/* Build the first region page. */
		pgno = 1;
		if ((ret = __memp_fget(mpf, &pgno,
		    ip, txn, DB_MPOOL_CREATE | DB_MPOOL_DIRTY, &region)) != 0)
			goto err;
		memset(region, 0, dbp->pgsize);

		P_INIT(region,
		    dbp->pgsize, 1, PGNO_INVALID, PGNO_INVALID, 0, P_IHEAP);
		LSN_NOT_LOGGED(region->lsn);
		ret = __db_log_page(
		    dbp, txn, &region->lsn, pgno, (PAGE *)region);
		if ((t_ret = __memp_fput(
		    mpf, ip, region, dbp->priority)) != 0 && ret == 0)
			ret = t_ret;
		region = NULL;
		if (ret != 0)
			goto err;
	} else {
		memset(&pdbt, 0, sizeof(pdbt));

		/* Build the meta-data page. */
		pginfo.db_pagesize = dbp->pgsize;
		pginfo.flags =
		    F_ISSET(dbp, (DB_AM_CHKSUM | DB_AM_ENCRYPT | DB_AM_SWAP));
		pginfo.type = dbp->type;
		pdbt.data = &pginfo;
		pdbt.size = sizeof(pginfo);
		if ((ret = __os_calloc(env, 1, dbp->pgsize, &buf)) != 0)
			return (ret);
		meta = (HEAPMETA *)buf;
		LSN_NOT_LOGGED(lsn);
		__heap_init_meta(dbp, meta, PGNO_BASE_MD, &lsn);
		if ((ret =
		    __db_pgout(dbp->dbenv, PGNO_BASE_MD, meta, &pdbt)) != 0)
			goto err;
		if ((ret = __fop_write(env, txn, name, dbp->dirname,
		    DB_APP_DATA, fhp,
		    dbp->pgsize, 0, 0, buf, dbp->pgsize, 1, F_ISSET(
		    dbp, DB_AM_NOT_DURABLE) ? DB_LOG_NOT_DURABLE : 0)) != 0)
			goto err;
		meta = NULL;

		/* Build the first region page */
		memset(buf, 0, dbp->pgsize);
		region = (HEAPPG *)buf;
		P_INIT(region,
		    dbp->pgsize, 1, PGNO_INVALID, PGNO_INVALID, 0, P_IHEAP);
		LSN_NOT_LOGGED(region->lsn);
		if ((ret =
		    __db_pgout(dbp->dbenv, region->pgno, region, &pdbt)) != 0)
			goto err;
		if ((ret =
		    __fop_write(env, txn, name, dbp->dirname, DB_APP_DATA,
		    fhp, dbp->pgsize, 1, 0, buf, dbp->pgsize, 1, F_ISSET(
		    dbp, DB_AM_NOT_DURABLE) ? DB_LOG_NOT_DURABLE : 0)) != 0)
			goto err;
		region = NULL;
	}

err:	if (buf != NULL)
		__os_free(env, buf);
	return (ret);
}

/*
 * __heap_create_region --
 * Create a region page
 *
 * PUBLIC: int __heap_create_region __P((DBC *, db_pgno_t));
 */
int
__heap_create_region(dbc, pgno)
	DBC *dbc;
	db_pgno_t pgno;
{
	DB *dbp;
	DB_LOCK meta_lock;
	DB_MPOOLFILE *mpf;
	HEAPMETA *meta;
	HEAPPG *region;
	db_pgno_t meta_pgno;
	int ret, t_ret;

	LOCK_INIT(meta_lock);
	dbp = dbc->dbp;
	mpf = dbp->mpf;
	region = NULL;

	/* We may need to update the last page number on the metadata page. */
	meta_pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
	    LCK_ALWAYS, meta_pgno, DB_LOCK_WRITE, 0, &meta_lock)) != 0)
		return (ret);
	if ((ret = __memp_fget(mpf, &meta_pgno,
	    dbc->thread_info, NULL, DB_MPOOL_DIRTY, &meta)) != 0) {
		(void)__LPUT(dbc, meta_lock);
		return (ret);
	}

	ret = __memp_fget(mpf, &pgno, dbc->thread_info,
	    NULL, DB_MPOOL_CREATE | DB_MPOOL_DIRTY, &region);

	if (ret != 0 || region->pgno != 0)
		/*
		 * There's been an error or someone got here before us and
		 * created the page. Either way, our work here is done.
		 */
		goto done;

	/* Log the page creation. */
	if (DBC_LOGGING(dbc)) {
		if ((ret = __heap_pg_alloc_log(dbp,
		    dbc->txn, &LSN(meta), 0, &LSN(meta), meta_pgno,
		    pgno, (u_int32_t)P_IHEAP, meta->dbmeta.last_pgno)) != 0)
			goto done;
	} else
		LSN_NOT_LOGGED(LSN(&meta->dbmeta));

	memset((void *)region, 0, dbp->pgsize);
	P_INIT(region,
	    dbp->pgsize, pgno, PGNO_INVALID, PGNO_INVALID, 0, P_IHEAP);
	LSN(region) = LSN(&meta->dbmeta);

	/*
	 * We may have created a page earlier with a larger page number
	 * check before updating the metadata page.
	 */
	if (pgno > meta->dbmeta.last_pgno)
		meta->dbmeta.last_pgno = pgno;
	if (HEAP_REGION_NUM(dbp, pgno) > meta->nregions)
		meta->nregions = HEAP_REGION_NUM(dbp, pgno);

done:	if (region != NULL && (t_ret = __memp_fput(mpf,
	    dbc->thread_info, region, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;

	ret = __memp_fput(mpf, dbc->thread_info, meta, dbc->priority);
	if ((t_ret = __TLPUT(dbc, meta_lock)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

static void
__heap_init_meta(dbp, meta, pgno, lsnp)
	DB *dbp;
	HEAPMETA *meta;
	db_pgno_t pgno;
	DB_LSN *lsnp;
{
	HEAP *h;
	ENV *env;

	env = dbp->env;
	h = dbp->heap_internal;

	memset(meta, 0, sizeof(HEAPMETA));
	meta->dbmeta.lsn = *lsnp;
	meta->dbmeta.pgno = pgno;
	meta->dbmeta.magic = DB_HEAPMAGIC;
	meta->dbmeta.version = DB_HEAPVERSION;
	meta->dbmeta.pagesize = dbp->pgsize;
	if (F_ISSET(dbp, DB_AM_CHKSUM))
		FLD_SET(meta->dbmeta.metaflags, DBMETA_CHKSUM);
	if (F_ISSET(dbp, DB_AM_ENCRYPT)) {
		meta->dbmeta.encrypt_alg = env->crypto_handle->alg;
		DB_ASSERT(env, meta->dbmeta.encrypt_alg != 0);
		meta->crypto_magic = meta->dbmeta.magic;
	}
	meta->dbmeta.type = P_HEAPMETA;
	meta->dbmeta.free = PGNO_INVALID;
	meta->dbmeta.last_pgno = FIRST_HEAP_RPAGE;
	memcpy(meta->dbmeta.uid, dbp->fileid, DB_FILE_ID_LEN);
	meta->gbytes = h->gbytes;
	meta->bytes = h->bytes;
	meta->region_size = h->region_size;
	meta->nregions = 1;
	meta->curregion = 1;
}
