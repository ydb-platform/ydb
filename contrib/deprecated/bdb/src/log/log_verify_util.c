/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

/*
 * This file contains helper functions like data structure and in-memory db
 * management, which are used to store various log verification information.
 */
#include "db_config.h"
#include "db_int.h"

#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/btree.h"
#include "dbinc/hash.h"
#include "dbinc/qam.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc/fop.h"

#include "dbinc/log_verify.h"

#define	BDBOP(op)	do {		\
	ret = (op);			\
	if (ret != 0) {			\
		__lv_on_bdbop_err(ret);	\
		goto err;		\
	}				\
} while (0)

#define	BDBOP2(dbenv, op, funct)	do {			\
	ret = (op);						\
	if (ret != 0) {						\
		__lv_on_bdbop_err(ret);				\
		__db_err(dbenv->env, ret, "\n%s", funct);	\
		return (ret);					\
	}							\
} while (0)

#define	BDBOP3(dbenv, op, excpt, funct)	do {				\
	ret = (op);							\
	if (ret != 0) {							\
		__lv_on_bdbop_err(ret);					\
		if (ret != excpt) {					\
			__db_err(dbenv->env, ret, "\n%s", funct);	\
			return (ret);					\
		}							\
	}								\
} while (0)

typedef int (*btcmp_funct)(DB *, const DBT *, const DBT *);
typedef int (*dupcmp_funct)(DB *, const DBT *, const DBT *);

static int __lv_add_recycle_handler __P((
    DB_LOG_VRFY_INFO *, VRFY_TXN_INFO *, void *));
static int __lv_add_recycle_lsn __P((VRFY_TXN_INFO *, const DB_LSN *));
static size_t __lv_dbt_arrsz __P((const DBT *, u_int32_t));
static int __lv_fidpgno_cmp __P((DB *, const DBT *, const DBT *));
static int __lv_i32_cmp __P((DB *, const DBT *, const DBT *));
static int __lv_lsn_cmp __P((DB *, const DBT *, const DBT *));
static void __lv_on_bdbop_err __P((int));
static int __lv_open_db __P((DB_ENV *, DB **, DB_THREAD_INFO *,
    const char *, int, btcmp_funct, u_int32_t, dupcmp_funct));
static int __lv_pack_filereg __P((const VRFY_FILEREG_INFO *, DBT *));
static int __lv_pack_txn_vrfy_info __P((
    const VRFY_TXN_INFO *, DBT *, DBT *data));
static int __lv_seccbk_fname __P((DB *, const DBT *, const DBT *, DBT *));
static int __lv_seccbk_lsn __P((DB *, const DBT *, const DBT *, DBT *));
static int __lv_seccbk_txnpg __P((DB *, const DBT *, const DBT *, DBT *));
static void __lv_setup_logtype_names __P((DB_LOG_VRFY_INFO *lvinfo));
static int __lv_txnrgns_lsn_cmp __P((DB *, const DBT *, const DBT *));
static int __lv_ui32_cmp __P((DB *, const DBT *, const DBT *));
static int __lv_unpack_txn_vrfy_info __P((VRFY_TXN_INFO **, const DBT *));
static int __lv_unpack_filereg __P((const DBT *, VRFY_FILEREG_INFO **));

static void __lv_on_bdbop_err(ret)
	int ret;
{
	/* Pass lint checks. We need the ret and this function for debugging. */
	COMPQUIET(ret, 0);
}

/*
 * __create_log_vrfy_info --
 *	Initialize and return a log verification handle to be used throughout
 *	a verification process.
 *
 * PUBLIC: int __create_log_vrfy_info __P((const DB_LOG_VERIFY_CONFIG *,
 * PUBLIC:     DB_LOG_VRFY_INFO **, DB_THREAD_INFO *));
 */
int
__create_log_vrfy_info(cfg, lvinfopp, ip)
	const DB_LOG_VERIFY_CONFIG *cfg;
	DB_LOG_VRFY_INFO **lvinfopp;
	DB_THREAD_INFO *ip;
{
	const char *envhome;
	int inmem, ret;
	u_int32_t cachesz, envflags;
	const char *dbf1, *dbf2, *dbf3, *dbf4, *dbf5, *dbf6, *dbf7, *dbf8,
	    *dbf9, *dbf10, *dbf11;
	DB_LOG_VRFY_INFO *lvinfop;

	dbf1 = "__db_log_vrfy_txninfo.db";
	dbf2 = "__db_log_vrfy_fileregs.db";
	dbf3 = "__db_log_vrfy_pgtxn.db";
	dbf4 = "__db_log_vrfy_lsntime.db";
	dbf5 = "__db_log_vrfy_timelsn.db";
	dbf6 = "__db_log_vrfy_ckps.db";
	dbf7 = "__db_log_vrfy_dbregids.db";
	dbf8 = "__db_log_vrfy_fnameuid.db";
	dbf9 = "__db_log_vrfy_timerange.db";
	dbf10 = "__db_log_vrfy_txnaborts.db";
	dbf11 = "__db_log_vrfy_txnpg.db";

	envhome = cfg->temp_envhome;
	lvinfop = NULL;
	cachesz = cfg->cachesize;
	if (cachesz== 0)
		cachesz = 1024 * 1024 * 256;

	BDBOP(__os_malloc(NULL, sizeof(DB_LOG_VRFY_INFO), &lvinfop));
	memset(lvinfop, 0, sizeof(DB_LOG_VRFY_INFO));
	lvinfop->ip = ip;
	__lv_setup_logtype_names(lvinfop);
	/* Avoid the VERIFY_PARTIAL bit being cleared if no ckp_lsn exists. */
	lvinfop->valid_lsn.file = lvinfop->valid_lsn.offset = (u_int32_t)-1;

	/*
	 * The envhome parameter determines if we will use an in-memory
	 * environment and databases.
	 */
	if (envhome == NULL) {
		envflags = DB_PRIVATE;
		inmem = 1;
	} else {
		envflags = 0;
		inmem = 0;
	}

	/* Create log verify internal database environment. */
	BDBOP(db_env_create(&lvinfop->dbenv, 0));
	BDBOP(__memp_set_cachesize(lvinfop->dbenv, 0, cachesz, 1));
	/*
	 * Log verification internal db environment should be accessed
	 * single-threaded. No transaction semantics needed.
	 */
	BDBOP(__env_open(lvinfop->dbenv, envhome,
	    envflags | DB_CREATE | DB_INIT_MPOOL, 0666));

	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->txninfo, ip, dbf1,
	    inmem, __lv_ui32_cmp, 0, NULL));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->fileregs, ip, dbf2,
	    inmem, NULL, 0, NULL));

	/* No dup allowed, always overwrite data with same key. */
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->dbregids, ip, dbf7,
	    inmem, __lv_i32_cmp, 0, NULL));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->pgtxn, ip, dbf3,
	    inmem, __lv_fidpgno_cmp, 0, NULL));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->txnpg, ip, dbf11,
	    inmem, __lv_ui32_cmp, DB_DUP | DB_DUPSORT, __lv_fidpgno_cmp));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->lsntime, ip, dbf4,
	    inmem, __lv_lsn_cmp, 0, NULL));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->timelsn, ip, dbf5,
	    inmem, __lv_i32_cmp, DB_DUP | DB_DUPSORT, __lv_lsn_cmp));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->txnaborts, ip, dbf10,
	    inmem, __lv_lsn_cmp, 0, NULL));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->ckps, ip, dbf6,
	    inmem, __lv_lsn_cmp, 0, NULL));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->fnameuid, ip, dbf8,
	    inmem, NULL, 0, NULL));
	BDBOP(__lv_open_db(lvinfop->dbenv, &lvinfop->txnrngs, ip, dbf9,
	    inmem, __lv_ui32_cmp, DB_DUP | DB_DUPSORT, __lv_txnrgns_lsn_cmp));

	BDBOP(__db_associate(lvinfop->lsntime, ip, NULL,
	    lvinfop->timelsn, __lv_seccbk_lsn, DB_CREATE));
	BDBOP(__db_associate(lvinfop->fileregs, ip, NULL,
	    lvinfop->fnameuid, __lv_seccbk_fname, DB_CREATE));
	BDBOP(__db_associate(lvinfop->pgtxn, ip, NULL,
	    lvinfop->txnpg, __lv_seccbk_txnpg, DB_CREATE));

	*lvinfopp = lvinfop;

	return (0);
err:
	if (lvinfop->dbenv && ret != 0)
		__db_err(lvinfop->dbenv->env, ret, "__create_log_vrfy_info");
	(void)__destroy_log_vrfy_info(lvinfop);

	return (ret);
}

/*
 * __destroy_log_vrfy_info --
 *	Destroy and free a log verification handle.
 *
 * PUBLIC: int __destroy_log_vrfy_info __P((DB_LOG_VRFY_INFO *));
 */
int
__destroy_log_vrfy_info(lvinfop)
	DB_LOG_VRFY_INFO *lvinfop;
{
	int ret;

	ret = 0;
	if (lvinfop == NULL)
		return (0);

	if (lvinfop->txnaborts != NULL &&
	    (ret = __db_close(lvinfop->txnaborts, NULL, 0)) != 0)
		goto err;
	if (lvinfop->txninfo != NULL &&
	    (ret = __db_close(lvinfop->txninfo, NULL, 0)) != 0)
		goto err;
	if (lvinfop->dbregids != NULL &&
	    (ret = __db_close(lvinfop->dbregids, NULL, 0)) != 0)
		goto err;
	if (lvinfop->fileregs != NULL &&
	    (ret = __db_close(lvinfop->fileregs, NULL, 0)) != 0)
		goto err;
	if (lvinfop->pgtxn != NULL &&
	    (ret = __db_close(lvinfop->pgtxn, NULL, 0)) != 0)
		goto err;
	if (lvinfop->lsntime != NULL &&
	    (ret = __db_close(lvinfop->lsntime, NULL, 0)) != 0)
		goto err;
	if (lvinfop->ckps != NULL &&
	    (ret = __db_close(lvinfop->ckps, NULL, 0)) != 0)
		goto err;
	if (lvinfop->txnrngs != NULL &&
	    (ret = __db_close(lvinfop->txnrngs, NULL, 0)) != 0)
		goto err;
	if (lvinfop->fnameuid != NULL &&
	    (ret = __db_close(lvinfop->fnameuid, NULL, 0)) != 0)
		goto err;
	if (lvinfop->timelsn != NULL &&
	    (ret = __db_close(lvinfop->timelsn, NULL, 0)) != 0)
		goto err;
	if (lvinfop->txnpg != NULL &&
	    (ret = __db_close(lvinfop->txnpg, NULL, 0)) != 0)
		goto err;
	if (lvinfop->dbenv != NULL &&
	    (ret = __env_close(lvinfop->dbenv, 0)) != 0)
		goto err;
err:
	__os_free(NULL, lvinfop);

	return (ret);
}

/* Secondary index callback function for DB_LOG_VRFY_INFO->timelsn. */
static int
__lv_seccbk_fname(secdb, key, data, result)
	DB *secdb;
	const DBT *key;
	const DBT *data;
	DBT *result;
{
	int ret, tret;
	VRFY_FILEREG_INFO *freg;
	char *buf;
	size_t buflen, slen;

	ret = tret = 0;
	COMPQUIET(key, NULL);
	if ((ret = __lv_unpack_filereg(data, &freg)) != 0)
		goto out;
	if (freg->fname == NULL || (slen = strlen(freg->fname)) == 0) {
		ret = DB_DONOTINDEX;
		goto out;
	}

	buflen = (slen + 1) * sizeof(char);
	if ((ret = __os_umalloc(secdb->dbenv->env, buflen, &buf)) != 0)
		goto out;
	(void)strcpy(buf, freg->fname);
	result->size = (u_int32_t)buflen;
	result->flags |= DB_DBT_APPMALLOC;
	result->data = buf;
out:
	if (freg != NULL && (tret = __free_filereg_info(freg)) != 0 && ret == 0)
		ret = tret;
	return (ret);
}

/* Secondary index callback function for DB_LOG_VRFY_INFO->txnpg. */
static int
__lv_seccbk_txnpg(secdb, key, data, result)
	DB *secdb;
	const DBT *key;
	const DBT *data;
	DBT *result;
{
	COMPQUIET(key, NULL);
	COMPQUIET(secdb, NULL);
	/* Txnid is the secondary key, and it's all the data dbt has. */
	result->data = data->data;
	result->size = data->size;

	return (0);
}

/* Secondary index callback function for DB_LOG_VRFY_INFO->timelsn. */
static int
__lv_seccbk_lsn(secdb, key, data, result)
	DB *secdb;
	const DBT *key;
	const DBT *data;
	DBT *result;
{
	VRFY_TIMESTAMP_INFO *lvti;

	COMPQUIET(key, NULL);
	COMPQUIET(secdb, NULL);

	lvti = (VRFY_TIMESTAMP_INFO *)data->data;
	result->data = &(lvti->timestamp);
	result->size = sizeof(lvti->timestamp);

	return (0);
}

/*
 * Open a BTREE database handle, optionally set the btree compare function
 * and flags if any.
 */
static int
__lv_open_db(dbenv, dbpp, ip, name, inmem, cmpf, sflags, dupcmpf)
	DB_ENV *dbenv;
	DB **dbpp;
	const char *name;
	int inmem;
	btcmp_funct cmpf;
	u_int32_t sflags;
	dupcmp_funct dupcmpf;
	DB_THREAD_INFO *ip;
{
	int ret;
	const char *dbfname, *dbname;
	DB *dbp;

	dbp = NULL;
	ret = 0;
	if (inmem) {
		dbfname = NULL;
		dbname = name;
	} else {
		dbfname = name;
		dbname = NULL;
	}

	BDBOP(db_create(&dbp, dbenv, 0));

	if (cmpf != NULL)
		BDBOP(__bam_set_bt_compare(dbp, cmpf));
	if (dupcmpf != NULL)
		dbp->dup_compare = dupcmpf;
	if (sflags != 0)
		BDBOP(__db_set_flags(dbp, sflags));
	/* No concurrency needed, a big page size reduces overflow pages. */
	BDBOP(__db_set_pagesize(dbp, 16 * 1024));

	BDBOP(__db_open(dbp, ip, NULL, dbfname, dbname, DB_BTREE, DB_CREATE,
	    0666, PGNO_BASE_MD));

	*dbpp = dbp;

	return (0);
err:
	if (dbenv != NULL && ret != 0)
		__db_err(dbenv->env, ret, "__lv_open_db");
	if (dbp != NULL)
		(void)__db_close(dbp, NULL, 0);

	return (ret);
}

/* Btree compare function for a [fileid, pgno] key. */
static int
__lv_fidpgno_cmp(db, dbt1, dbt2)
	DB *db;
	const DBT *dbt1;
	const DBT *dbt2;
{
	db_pgno_t pgno1, pgno2;
	int ret;
	size_t len;

	COMPQUIET(db, NULL);
	len = DB_FILE_ID_LEN;
	ret = memcmp(dbt1->data, dbt2->data, len);
	if (ret == 0) {
		memcpy(&pgno1, (u_int8_t *)dbt1->data + len,
		    sizeof(pgno1));
		memcpy(&pgno2, (u_int8_t *)dbt2->data + len,
		    sizeof(pgno2));
		ret = NUMCMP(pgno1, pgno2);
	}

	return (ret);
}

/* Btree compare function for a int32_t type of key. */
static int
__lv_i32_cmp(db, dbt1, dbt2)
	DB *db;
	const DBT *dbt1;
	const DBT *dbt2;
{
	int32_t k1, k2;

	COMPQUIET(db, NULL);
	memcpy(&k1, dbt1->data, sizeof(k1));
	memcpy(&k2, dbt2->data, sizeof(k2));

	return (NUMCMP(k1, k2));
}

/* Btree compare function for a u_int32_t type of key. */
static int
__lv_ui32_cmp(db, dbt1, dbt2)
	DB *db;
	const DBT *dbt1;
	const DBT *dbt2;
{
	u_int32_t k1, k2;

	COMPQUIET(db, NULL);
	memcpy(&k1, dbt1->data, sizeof(k1));
	memcpy(&k2, dbt2->data, sizeof(k2));

	return (NUMCMP(k1, k2));
}

/* Btree compare function for a DB_LSN type of key. */
static int
__lv_lsn_cmp(db, dbt1, dbt2)
	DB *db;
	const DBT *dbt1;
	const DBT *dbt2;
{
	DB_LSN lsn1, lsn2;

	DB_ASSERT(db->env, dbt1->size == sizeof(DB_LSN));
	DB_ASSERT(db->env, dbt2->size == sizeof(DB_LSN));
	memcpy(&lsn1, dbt1->data, sizeof(DB_LSN));
	memcpy(&lsn2, dbt2->data, sizeof(DB_LSN));

	return (LOG_COMPARE(&lsn1, &lsn2));
}

/*
 * Structure management routines. We keep each structure on a
 * consecutive memory chunk.
 *
 * The get functions will allocate memory via __os_malloc, and callers
 * should free the memory after use. The update functions for VRFY_TXN_INFO
 * and VRFY_FILEREG_INFO may realloc the structure.
 */

/*
 * PUBLIC: int __put_txn_vrfy_info __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:     const VRFY_TXN_INFO *));
 */
int
__put_txn_vrfy_info (lvinfo, txninfop)
	const DB_LOG_VRFY_INFO *lvinfo;
	const VRFY_TXN_INFO *txninfop;
{
	int ret;
	DBT key, data;

	ret = __lv_pack_txn_vrfy_info(txninfop, &key, &data);
	DB_ASSERT(lvinfo->dbenv->env, ret == 0);

	BDBOP2(lvinfo->dbenv, __db_put(lvinfo->txninfo, lvinfo->ip, NULL,
	    &key, &data, 0), "__put_txn_vrfy_info");
	__os_free(lvinfo->dbenv->env, data.data);

	return (0);
}

/* Construct a key and data DBT from the structure. */
static int
__lv_pack_txn_vrfy_info(txninfop, key, data)
	const VRFY_TXN_INFO *txninfop;
	DBT *key, *data;
{
	int ret;
	char *buf, *p;
	size_t bufsz, len;
	u_int32_t i;
	DBT *pdbt;

	memset(key, 0, sizeof(DBT));
	memset(data, 0, sizeof(DBT));
	ret = 0;
	bufsz = TXN_VERIFY_INFO_TOTSIZE(*txninfop);

	if ((ret = __os_malloc(NULL, bufsz, &buf)) != 0)
		goto err;
	memset(buf, 0, bufsz);
	memcpy(buf, txninfop, TXN_VERIFY_INFO_FIXSIZE);
	p = buf + TXN_VERIFY_INFO_FIXSIZE;
	memcpy(p, txninfop->recycle_lsns, len = sizeof(DB_LSN) *
	    txninfop->num_recycle);
	p += len;

	for (i = 0; i < txninfop->filenum; i++) {

		pdbt = &(txninfop->fileups[i]);
		memcpy(p, &(pdbt->size), sizeof(pdbt->size));
		p += sizeof(pdbt->size);
		memcpy(p, pdbt->data, pdbt->size);
		p += pdbt->size;
	}

	key->data = (void *)&txninfop->txnid;
	key->size = sizeof(txninfop->txnid);
	data->data = buf;
	data->size = (u_int32_t)bufsz;
	data->flags |= DB_DBT_MALLOC;
err:
	return (ret);
}

/* Calculate a DBT array's total number of bytes to store. */
static size_t
__lv_dbt_arrsz(arr, arrlen)
	const DBT *arr;
	u_int32_t arrlen;
{
	u_int32_t i;
	size_t sz;

	sz = 0;

	/* For each DBT object, store its size and its data bytes. */
	for (i = 0; i < arrlen; i++)
		sz += arr[i].size + sizeof(arr[i].size);

	return sz;
}

/*
 *  __get_txn_vrfy_info --
 *	Get a VRFY_TXN_INFO object from db by txnid. Callers should free the
 *	object by calling __free_txninfo.
 *
 * PUBLIC: int __get_txn_vrfy_info __P((const DB_LOG_VRFY_INFO *, u_int32_t,
 * PUBLIC:     VRFY_TXN_INFO **));
 */
int
__get_txn_vrfy_info (lvinfo, txnid, txninfopp)
	const DB_LOG_VRFY_INFO *lvinfo;
	u_int32_t txnid;
	VRFY_TXN_INFO **txninfopp;
{
	int ret;
	DBT key, data;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = &txnid;
	key.size = sizeof(txnid);

	BDBOP3(lvinfo->dbenv, __db_get(lvinfo->txninfo, lvinfo->ip, NULL,
	    &key, &data, 0), DB_NOTFOUND, "__get_txn_vrfy_info");

	if (ret != DB_NOTFOUND)
		ret = __lv_unpack_txn_vrfy_info(txninfopp, &data);

	return (ret);
}

/* Construct a structure from a DBT. */
static int
__lv_unpack_txn_vrfy_info(txninfopp, data)
	VRFY_TXN_INFO **txninfopp;
	const DBT *data;
{
	size_t bufsz;
	VRFY_TXN_INFO *buf, *txninfop;
	DB_LSN *lsns, *p;
	u_int32_t i, sz;
	char *pb, *q;
	int ret;

	ret = 0;
	i = sz = 0;
	lsns = p = NULL;
	pb = q = NULL;
	txninfop = (VRFY_TXN_INFO *)data->data;
	lsns = (DB_LSN *)((char *)data->data + TXN_VERIFY_INFO_FIXSIZE);
	pb = (char *)lsns + txninfop->num_recycle * sizeof(DB_LSN);

	if ((ret = __os_malloc(NULL, bufsz = sizeof(VRFY_TXN_INFO), &buf)) != 0)
		goto err;
	memset(buf, 0, bufsz);
	memcpy(buf, data->data, TXN_VERIFY_INFO_FIXSIZE);

	if (txninfop->num_recycle != 0) {
		if ((ret = __os_malloc(NULL,
		    txninfop->num_recycle * sizeof(DB_LSN), &p)) != 0)
			goto err;
		memcpy(p, lsns, txninfop->num_recycle * sizeof(DB_LSN));
		buf->recycle_lsns = p;
	}

	if (txninfop->filenum != 0) {
		if ((ret = __os_malloc(NULL,
		    txninfop->filenum * sizeof(DBT), &q)) != 0)
			goto err;
		memset(q, 0, txninfop->filenum * sizeof(DBT));
		buf->fileups = (DBT *)q;
		for (i = 0; i < txninfop->filenum; i++) {
			memcpy(&sz, pb, sizeof(sz));
			pb += sizeof(sz);
			if ((ret = __os_malloc(NULL, sz, &q)) != 0)
				goto err;
			memcpy(q, pb, sz);
			pb += sz;

			buf->fileups[i].data = q;
			buf->fileups[i].size = sz;
		}
	}

	*txninfopp = buf;
err:
	return (ret);
}

static int
__lv_add_recycle_lsn (txninfop, lsn)
	VRFY_TXN_INFO *txninfop;
	const DB_LSN *lsn;
{
	int ret;

	ret = 0;
	txninfop->num_recycle++;
	if ((ret = __os_realloc(NULL, txninfop->num_recycle * sizeof(DB_LSN),
	    &(txninfop->recycle_lsns))) != 0)
		goto err;
	txninfop->recycle_lsns[txninfop->num_recycle - 1] = *lsn;
err:
	return (ret);
}

/*
 * __add_recycle_lsn_range --
 *	Add recycle info for each txn within the recycled txnid range.
 *
 * PUBLIC: int __add_recycle_lsn_range __P((DB_LOG_VRFY_INFO *,
 * PUBLIC:     const DB_LSN *, u_int32_t, u_int32_t));
 */
int
__add_recycle_lsn_range(lvinfo, lsn, min, max)
	DB_LOG_VRFY_INFO *lvinfo;
	const DB_LSN *lsn;
	u_int32_t min, max;
{
	DBC *csr;
	int ret, tret;
	u_int32_t i;
	DBT key2, data2;
	struct __add_recycle_params param;

	csr = NULL;
	ret = tret = 0;
	memset(&key2, 0, sizeof(DBT));
	memset(&data2, 0, sizeof(DBT));
	memset(&param, 0, sizeof(param));

	if ((ret = __os_malloc(lvinfo->dbenv->env, sizeof(VRFY_TXN_INFO *) *
	    (param.ti2ul = 1024), &(param.ti2u))) != 0)
		goto err;
	param.ti2ui = 0;
	param.recycle_lsn = *lsn;
	param.min = min;
	param.max = max;

	/* Iterate the specified range and process each transaction. */
	if ((ret = __iterate_txninfo(lvinfo, min, max, __lv_add_recycle_handler,
	    &param)) != 0)
		goto err;

	/*
	 * Save updated txninfo structures. We can't do so in the above
	 * iteration, so we have to save them here.
	 */
	BDBOP(__db_cursor(lvinfo->txninfo, lvinfo->ip, NULL, &csr, DBC_BULK));

	for (i = 0; i < param.ti2ui; i++) {
		ret = __lv_pack_txn_vrfy_info(param.ti2u[i], &key2, &data2);
		DB_ASSERT(lvinfo->dbenv->env, ret == 0);
		BDBOP(__dbc_put(csr, &key2, &data2, DB_KEYLAST));
		/*
		 * key2.data refers to param.ti2u[i]'s memory, data2.data is
		 * freed by DB since we set DB_DBT_MALLOC.
		 */
		if ((ret = __free_txninfo(param.ti2u[i])) != 0)
			goto err;
	}

err:
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	__os_free(lvinfo->dbenv->env, param.ti2u);
	if (ret != 0)
		__db_err(lvinfo->dbenv->env, ret,
		    "__add_recycle_lsn_range");

	return (ret);
}

/*
 *  __iterate_txninfo --
 *	Iterate throught the transaction info database as fast as possible,
 *	and process each key/data pair using a callback handler. Break the
 *	iteration if the handler returns non-zero values.
 *
 * PUBLIC: int __iterate_txninfo __P((DB_LOG_VRFY_INFO *, u_int32_t,
 * PUBLIC:     u_int32_t, TXNINFO_HANDLER, void *));
 */
int
__iterate_txninfo(lvinfo, min, max, handler, param)
	DB_LOG_VRFY_INFO *lvinfo;
	u_int32_t min, max;
	TXNINFO_HANDLER handler;
	void *param;
{
	ENV *env;
	VRFY_TXN_INFO *txninfop;
	int ret, tret;
	u_int32_t bufsz, pgsz, txnid;
	size_t retkl, retdl;
	char *btbuf;
	u_int8_t *retk, *retd;
	DBT key, data, data2;
	DBC *csr;
	void *p;

	csr = NULL;
	env = lvinfo->dbenv->env;
	txninfop = NULL;
	ret = tret = 0;
	txnid = 0;
	retkl = retdl = 0;
	bufsz = 64 * 1024;
	btbuf = NULL;
	retk = retd = NULL;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	memset(&data2, 0, sizeof(DBT));

	pgsz = lvinfo->txninfo->pgsize;
	DB_ASSERT(env, ret == 0);

	if (bufsz % pgsz != 0)
		bufsz = pgsz * (bufsz / pgsz);

	if ((ret = __os_malloc(env, bufsz, &btbuf)) != 0)
		goto err;

	BDBOP(__db_cursor(lvinfo->txninfo, lvinfo->ip, NULL, &csr, DBC_BULK));

	/*
	 * Use bulk retrieval to scan the database as fast as possible.
	 */
	data.data = btbuf;
	data.ulen = bufsz;
	data.flags |= DB_DBT_USERMEM;

	for (ret = __dbc_get(csr, &key, &data, DB_FIRST | DB_MULTIPLE_KEY) ;;
	    ret = __dbc_get(csr, &key, &data, DB_NEXT | DB_MULTIPLE_KEY)) {
		switch (ret) {
		case 0:
			break;
		case DB_NOTFOUND:
			goto out;
			/* No break statement allowed by lint here. */
		case DB_BUFFER_SMALL:
			if ((ret = __os_realloc(lvinfo->dbenv->env,
			    bufsz *= 2, &btbuf)) != 0)
				goto out;
			data.ulen = bufsz;
			data.data = btbuf;
			continue;/* Continue the for-loop. */
			/* No break statement allowed by lint here. */
		default:
			goto err;
		}

		/*
		 * Do bulk get. Some txninfo objects may be updated by the
		 * handler, but we can't store them immediately in the same
		 * loop because we wouldn't be able to continue the bulk get
		 * using the same cursor; and we can't use another cursor
		 * otherwise we may self-block. In the handler we need to
		 * store the updated objects and store them to db when we get
		 * out of this loop.
		 */
		DB_MULTIPLE_INIT(p, &data);
		while (1) {
			DB_MULTIPLE_KEY_NEXT(p, &data,
			    retk, retkl, retd, retdl);
			if (p == NULL)
				break;
			DB_ASSERT(env, retkl == sizeof(txnid) && retk != NULL);
			memcpy(&txnid, retk, retkl);
			/*
			 * Process it if txnid in range or no range specified.
			 * The range must be a closed one.
			 */
			if ((min != 0 && txnid >= min && max != 0 &&
			    txnid <= max) || (min == 0 && max == 0)) {
				data2.data = retd;
				data2.size = (u_int32_t)retdl;

				if ((ret = __lv_unpack_txn_vrfy_info(
				    &txninfop, &data2)) != 0)
					goto out;
				if ((ret = handler(lvinfo, txninfop,
				    param)) != 0)
					/* Stop the iteration on error. */
					goto out;
			}
		}

	}
out:
	if (ret == DB_NOTFOUND)
		ret = 0;
err:
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	__os_free(lvinfo->dbenv->env, btbuf);
	return (ret);
}

/* Txninfo iteration handler to add recycle info for affected txns. */
static int
__lv_add_recycle_handler(lvinfo, txninfop, params)
	DB_LOG_VRFY_INFO *lvinfo;
	VRFY_TXN_INFO *txninfop;
	void *params;
{
	int ret;
	struct __add_recycle_params *param;

	ret = 0;
	param = (struct __add_recycle_params *)params;

	/*
	 * If the txnid is reused, update its recycle info and note it for
	 * later update, otherwise free the txninfop structure.
	 */
	if (txninfop->txnid < param->min && txninfop->txnid > param->max) {
		ret = __free_txninfo(txninfop);
		return (ret);
	}

	ret = __lv_add_recycle_lsn(txninfop, &(param->recycle_lsn));

	if (ret != 0)
		goto err;
	/*
	 * Below is one way to tell if a txn is aborted without doing another
	 * backward pass of the log. However if the txn id is not in the
	 * chosen recycled txn id range, we can't tell, until all the log
	 * records are passed --- the remaining active txns are the aborted
	 * txns.
	 * No longer needed since we did another backward pass of the log
	 * and have all the txn lifetimes.
	if (txninfop->status == TXN_STAT_ACTIVE)
		__on_txn_abort(lvinfo, txninfop);
	 */
	if (txninfop->status == TXN_STAT_PREPARE) {
		__db_errx(lvinfo->dbenv->env,
		    "[ERROR] Transaction with ID %u is prepared and not "
		    "committed, but its ID is recycled by log record [%u, %u].",
		    txninfop->txnid, param->recycle_lsn.file,
		    param->recycle_lsn.offset);
	}
	/* Note down to store later. */
	param->ti2u[(param->ti2ui)++] = txninfop;
	if (param->ti2ui == param->ti2ul)
		BDBOP(__os_realloc(lvinfo->dbenv->env,
		    sizeof(VRFY_TXN_INFO *) * (param->ti2ul *= 2),
		    &(param->ti2u)));
err:
	return (ret);

}
/*
 * PUBLIC: int __rem_last_recycle_lsn __P((VRFY_TXN_INFO *));
 */
int
__rem_last_recycle_lsn(txninfop)
	VRFY_TXN_INFO *txninfop;
{
	int ret;

	ret = 0;
	if (txninfop->num_recycle == 0)
		return (0);
	txninfop->num_recycle--;
	if (txninfop->num_recycle > 0)
		BDBOP(__os_realloc(NULL, txninfop->num_recycle * sizeof(DB_LSN),
		    &(txninfop->recycle_lsns)));
	else {
		__os_free(NULL, txninfop->recycle_lsns);
		txninfop->recycle_lsns = NULL;
	}
err:
	return (ret);

}

/*
 * __add_file_updated --
 *	Add a file's dbregid and uid to the updating txn if it's not yet
 *	recorded.
 *
 * PUBLIC: int __add_file_updated __P((VRFY_TXN_INFO *, const DBT *, int32_t));
 */
int
__add_file_updated (txninfop, fileid, dbregid)
	VRFY_TXN_INFO *txninfop;
	const DBT *fileid;
	int32_t dbregid;
{
	int ret;
	DBT *pdbt, *p;
	u_int32_t found, i;

	ret = 0;
	p = pdbt = NULL;

	for (found = 0, i = 0; i < txninfop->filenum; i++) {
		p = &(txninfop->fileups[i]);
		if (p->size == fileid->size &&
		    memcmp(p->data, fileid->data, p->size) == 0) {
			found = 1;
			break;
		}
	}

	if (found)
		return (0);

	/* Add file's uid into the array, deep copy from fileid. */
	txninfop->filenum++;
	if ((ret = __os_realloc(NULL, txninfop->filenum *
	    sizeof(DBT), &(txninfop->fileups))) != 0)
		goto err;

	pdbt = &(txninfop->fileups[txninfop->filenum - 1]);
	memset(pdbt, 0, sizeof(DBT));
	if ((ret = __os_malloc(NULL,
	    pdbt->size = fileid->size, &(pdbt->data))) != 0)
		goto err;
	memcpy(pdbt->data, fileid->data, fileid->size);

	/* Add file dbregid into the array. */
	BDBOP(__os_realloc(NULL, txninfop->filenum *
	    sizeof(int32_t), &(txninfop->dbregid)));
	txninfop->dbregid[txninfop->filenum - 1] = dbregid;
err:
	return (ret);
}

/*
 * PUBLIC: int __del_file_updated __P((VRFY_TXN_INFO *, const DBT *));
 */
int
__del_file_updated (txninfop, fileid)
	VRFY_TXN_INFO *txninfop;
	const DBT *fileid;
{
	u_int32_t found, i;
	int ret;
	DBT *p;
	void *pdbtdata;

	ret = 0;

	if (txninfop->filenum == 0)
		return (0);

	/*
	 * If the array has an element identical to fileid, remove it. fileid
	 * itself is intact after this function call.
	 */
	for (found = 0, i = 0, pdbtdata = NULL; i < txninfop->filenum; i++) {
		p = &(txninfop->fileups[i]);
		if (p->size == fileid->size &&
		    memcmp(p->data, fileid->data, p->size) == 0) {
			pdbtdata = p->data;
			if (txninfop->filenum > 1) {
				memmove(txninfop->fileups + i, txninfop->
				    fileups + i + 1, sizeof(DBT) * (txninfop->
				    filenum - (i + 1)));
				memmove(txninfop->dbregid + i, txninfop->
				    dbregid + i + 1, sizeof(int32_t) *
				    (txninfop->filenum - (i + 1)));
			} else {
				__os_free(NULL, txninfop->fileups);
				__os_free(NULL, txninfop->dbregid);
				txninfop->fileups = NULL;
				txninfop->dbregid = NULL;
			}
			found = 1;
			break;
		}
	}

	if (found) {
		txninfop->filenum--;
		if (txninfop->filenum) {
			BDBOP(__os_realloc(NULL, sizeof(DBT) *
			    txninfop->filenum, &(txninfop->fileups)));
			BDBOP(__os_realloc(NULL, sizeof(int32_t) *
			    txninfop->filenum, &(txninfop->dbregid)));
		}
		__os_free(NULL, pdbtdata);
	}
err:
	return (ret);
}

/*
 * PUBLIC: int __clear_fileups __P((VRFY_TXN_INFO *));
 */
int
__clear_fileups(txninfop)
	VRFY_TXN_INFO *txninfop;
{
	u_int32_t i;

	for (i = 0; i < txninfop->filenum; i++)
		__os_free(NULL, txninfop->fileups[i].data);

	__os_free(NULL, txninfop->fileups);
	__os_free(NULL, txninfop->dbregid);
	txninfop->fileups = NULL;
	txninfop->dbregid = NULL;
	txninfop->filenum = 0;

	return (0);
}

/*
 *  __free_txninfo_stack  --
 *	The object is on stack, only free its internal memory, not itself.
 * PUBLIC: int __free_txninfo_stack __P((VRFY_TXN_INFO *));
 */
int
__free_txninfo_stack (p)
	VRFY_TXN_INFO *p;
{
	u_int32_t i;

	if (p == NULL)
		return (0);

	if (p->fileups != NULL) {
		for (i = 0; i < p->filenum; i++)
			__os_free(NULL, p->fileups[i].data);
		__os_free(NULL, p->fileups);
	}

	if (p->dbregid != NULL)
		__os_free(NULL, p->dbregid);

	if (p->recycle_lsns != NULL)
		__os_free(NULL, p->recycle_lsns);

	return (0);
}
/*
 * PUBLIC: int __free_txninfo __P((VRFY_TXN_INFO *));
 */
int
__free_txninfo(p)
	VRFY_TXN_INFO *p;
{
	(void)__free_txninfo_stack(p);
	__os_free(NULL, p);

	return (0);
}

/* Construct a key and data DBT from the structure. */
static int
__lv_pack_filereg(freginfo, data)
	const VRFY_FILEREG_INFO *freginfo;
	DBT *data;
{
	char *buf, *p;
	size_t bufsz, offset;
	int ret;

	ret = 0;
	if ((ret = __os_malloc(NULL,
	    bufsz = FILE_REG_INFO_TOTSIZE(*freginfo), &buf)) != 0)
		goto err;
	memset(buf, 0, bufsz);

	memcpy(buf, freginfo, FILE_REG_INFO_FIXSIZE);
	p = buf + FILE_REG_INFO_FIXSIZE;

	offset = sizeof(int32_t) * freginfo->regcnt;
	memcpy(p, freginfo->dbregids, offset);
	p += offset;

	memcpy(p, &(freginfo->fileid.size), sizeof(freginfo->fileid.size));
	p += sizeof(freginfo->fileid.size);
	memcpy(p, freginfo->fileid.data, freginfo->fileid.size);
	p += freginfo->fileid.size;
	(void)strcpy(p, freginfo->fname);

	data->data = buf;
	data->size = (u_int32_t)bufsz;
err:
	return (ret);
}

/*
 * PUBLIC: int __put_filereg_info __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:    const VRFY_FILEREG_INFO *));
 */
int __put_filereg_info (lvinfo, freginfo)
	const DB_LOG_VRFY_INFO *lvinfo;
	const VRFY_FILEREG_INFO *freginfo;
{

	int ret;
	DBT data;

	memset(&data, 0, sizeof(DBT));

	if ((ret = __lv_pack_filereg(freginfo, &data)) != 0)
		goto err;

	/*
	 * We store dbregid-filereg map into dbregids.db, but we can't make
	 * dbregids.db the sec db of fileregs.db, because dbregid is only
	 * valid when a db file is open, we want to delete data with same
	 * key in dbregids.db, but we want to keep all filereg_info data in
	 * fileregs.db to track all db file lifetime and status.
	 *
	 * Consequently we will store dbregid-file_uid in dbregs.db, so that we
	 * can delete dbregid when the db handle is closed, and we can
	 * use the dbregid to get the currently open db file's uid.
	 */

	BDBOP2(lvinfo->dbenv, __db_put(lvinfo->fileregs, lvinfo->ip, NULL,
	    (DBT *)&(freginfo->fileid), &data, 0), "__put_filereg_info");

err:
	if (data.data != NULL)
		__os_free(lvinfo->dbenv->env, data.data);

	return (ret);
}

/*
 * PUBLIC: int __del_filelife __P((const DB_LOG_VRFY_INFO *, int32_t));
 */
int
__del_filelife(lvinfo, dbregid)
	const DB_LOG_VRFY_INFO *lvinfo;
	int32_t dbregid;
{
	int ret;
	DBT key;

	memset(&key, 0, sizeof(DBT));
	key.data = &(dbregid);
	key.size = sizeof(dbregid);

	if ((ret = __db_del(lvinfo->dbregids, lvinfo->ip, NULL,
	    &key, 0)) != 0)
		goto err;

err:
	return (ret);
}

/*
 * PUBLIC: int __put_filelife __P((const DB_LOG_VRFY_INFO *, VRFY_FILELIFE *));
 */
int
__put_filelife (lvinfo, pflife)
	const DB_LOG_VRFY_INFO *lvinfo;
	VRFY_FILELIFE *pflife;
{
	int ret;
	DBT key, data;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = &(pflife->dbregid);
	key.size = sizeof(pflife->dbregid);
	data.data = pflife;
	data.size = sizeof(VRFY_FILELIFE);

	if ((ret = __db_put(lvinfo->dbregids, lvinfo->ip, NULL,
	    &key, &data, 0)) != 0)
		goto err;

err:
	return (ret);
}

/*
 * PUBLIC: int __get_filelife __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:     int32_t, VRFY_FILELIFE **));
 */
int
__get_filelife (lvinfo, dbregid, flifepp)
	const DB_LOG_VRFY_INFO *lvinfo;
	int32_t dbregid;
	VRFY_FILELIFE **flifepp;
{
	int ret;
	DBT key, data;
	VRFY_FILELIFE *flifep;

	ret = 0;
	flifep = NULL;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &dbregid;
	key.size = sizeof(dbregid);
	if ((ret = __db_get(lvinfo->dbregids, lvinfo->ip, NULL,
	    &key, &data, 0)) != 0)
		goto err;
	if ((ret = __os_malloc(lvinfo->dbenv->env,
	    sizeof(VRFY_FILELIFE), &flifep)) != 0)
		goto err;
	DB_ASSERT(lvinfo->dbenv->env, flifep != NULL);
	memcpy(flifep, data.data, sizeof(VRFY_FILELIFE));
	*flifepp = flifep;
err:
	return (ret);
}

/*
 * PUBLIC: int __get_filereg_by_dbregid __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:     int32_t, VRFY_FILEREG_INFO **));
 */
int
__get_filereg_by_dbregid(lvinfo, dbregid, freginfopp)
	const DB_LOG_VRFY_INFO *lvinfo;
	int32_t dbregid;
	VRFY_FILEREG_INFO **freginfopp;
{
	int ret;
	DBT key, data;
	char uid[DB_FILE_ID_LEN];
	VRFY_FILELIFE *pflife;

	memset(&data, 0, sizeof(DBT));
	memset(&key, 0, sizeof(DBT));
	key.data = &dbregid;
	key.size = sizeof(dbregid);

	BDBOP3(lvinfo->dbenv, __db_get(lvinfo->dbregids, lvinfo->ip, NULL,
	    &key, &data, 0), DB_NOTFOUND,  "__get_filereg_by_dbregid");
	if (ret == DB_NOTFOUND)
		goto err;

	/* Use the file-uid as key to retrieve from fileregs.db. */
	pflife = (VRFY_FILELIFE *)data.data;
	memcpy((void *)uid, (void *)pflife->fileid, key.size = DB_FILE_ID_LEN);

	key.data = (void *)uid;
	memset(&data, 0, sizeof(DBT));

	BDBOP3(lvinfo->dbenv, __db_get(lvinfo->fileregs, lvinfo->ip, NULL,
	    &key, &data, 0), DB_NOTFOUND,  "__get_filereg_by_dbregid");
	if (ret == DB_NOTFOUND)
		goto err;
	if ((ret = __lv_unpack_filereg(&data, freginfopp)) != 0)
		goto err;

err:
	return (ret);
}

/*
 * PUBLIC: int __add_dbregid __P((DB_LOG_VRFY_INFO *, VRFY_FILEREG_INFO *,
 * PUBLIC:     int32_t, u_int32_t, DB_LSN, DBTYPE, db_pgno_t, int *));
 */
int
__add_dbregid(lvh, freg, dbregid, opcode, lsn, dbtype, meta_pgno, addp)
	DB_LOG_VRFY_INFO *lvh;
	VRFY_FILEREG_INFO *freg;
	int32_t dbregid;
	u_int32_t opcode;
	DB_LSN lsn;
	DBTYPE dbtype;
	db_pgno_t meta_pgno;
	int *addp;
{
	int inarray, ret, tret;
	u_int32_t i, j;
	VRFY_FILELIFE flife;

	inarray = ret = tret = 0;
	for (i = 0; i < freg->regcnt; i++) {
		if (freg->dbregids[i] == dbregid) {
			if (!IS_DBREG_CLOSE(opcode)) {
				/* Opening an open dbreg id. */
				if (IS_DBREG_OPEN(opcode) &&
				    (opcode != DBREG_CHKPNT &&
				    opcode != DBREG_XCHKPNT)) {
					tret = 2;
					goto err;
				}
				tret = 0;
				inarray = 1;
			} else
				/* Found the dbregid; gonna remove it. */
				tret = -1;
			break;
		}
	}

	if (IS_DBREG_OPEN(opcode))
		tret = 1;/* dbregid not in the array, gonna add 1. */

	/*
	 * Remove closed dbregid. dbregid can be recycled, not unique to a db
	 * file, it's dynamically allocated for each db handle.
	 */
	if (tret == -1) {
		for (j = i; j < freg->regcnt - 1; j++)
			freg->dbregids[j] = freg->dbregids[j + 1];
		freg->regcnt--;
		BDBOP(__os_realloc(lvh->dbenv->env,
		    sizeof(int32_t) * freg->regcnt, &(freg->dbregids)));
		/* Don't remove dbregid life info from dbregids db. */
	} else if (tret == 1) {
		if (!inarray) {
			freg->regcnt++;
			BDBOP(__os_realloc(lvh->dbenv->env,
			    sizeof(int32_t) * freg->regcnt, &(freg->dbregids)));
			freg->dbregids[freg->regcnt - 1] = dbregid;
		}
		flife.dbregid = dbregid;
		memcpy(flife.fileid, freg->fileid.data, freg->fileid.size);
		flife.lifetime = opcode;
		flife.dbtype = dbtype;
		flife.lsn = lsn;
		flife.meta_pgno = meta_pgno;
		if ((ret = __put_filelife(lvh, &flife)) != 0)
			goto err;
	}

err:
	*addp = tret;
	return (ret);

}

/*
 * PUBLIC: int __get_filereg_info __P((const DB_LOG_VRFY_INFO *, const DBT *,
 * PUBLIC:     VRFY_FILEREG_INFO **));
 */
int
__get_filereg_info (lvinfo, fuid, freginfopp)
	const DB_LOG_VRFY_INFO *lvinfo;
	const DBT *fuid;
	VRFY_FILEREG_INFO **freginfopp;
{
	int ret;
	DBT data;

	memset(&data, 0, sizeof(DBT));

	BDBOP3(lvinfo->dbenv, __db_get(lvinfo->fileregs, lvinfo->ip, NULL,
	    (DBT *)fuid, &data, 0), DB_NOTFOUND,  "__get_filereg_info");
	if (ret == DB_NOTFOUND)
		goto err;
	if ((ret = __lv_unpack_filereg(&data, freginfopp)) != 0)
		goto err;

err:
	return (ret);
}

static int
__lv_unpack_filereg(data, freginfopp)
	const DBT *data;
	VRFY_FILEREG_INFO **freginfopp;
{
	char *p, *q;
	u_int32_t fidsz, arrsz;
	VRFY_FILEREG_INFO *buf;
	int ret;

	ret = 0;
	p = q = NULL;
	fidsz = arrsz = 0;
	buf = NULL;

	if ((ret = __os_malloc(NULL, sizeof(VRFY_FILEREG_INFO), &buf)) != 0)
		goto err;
	memset(buf, 0, sizeof(VRFY_FILEREG_INFO));

	memcpy(buf, data->data, FILE_REG_INFO_FIXSIZE);
	*freginfopp = (VRFY_FILEREG_INFO *)buf;
	p = ((char *)(data->data)) + FILE_REG_INFO_FIXSIZE;

	if ((ret = __os_malloc(NULL, arrsz = (*freginfopp)->regcnt *
	    sizeof(int32_t), &((*freginfopp)->dbregids))) != 0)
		goto err;
	memcpy((*freginfopp)->dbregids, p, arrsz);
	p += arrsz;

	memcpy(&fidsz, p, sizeof(fidsz));
	p += sizeof(fidsz);
	if ((ret = __os_malloc(NULL, fidsz, &q)) != 0)
		goto err;
	memcpy(q, p, fidsz);
	(*freginfopp)->fileid.data = q;
	(*freginfopp)->fileid.size = fidsz;
	p += fidsz;

	if ((ret = __os_malloc(NULL, sizeof(char) * (strlen(p) + 1), &q)) != 0)
		goto err;
	(void)strcpy(q, p);

	(*freginfopp)->fname = q;
err:
	return (ret);
}

/*
 * PUBLIC: int __free_filereg_info __P((VRFY_FILEREG_INFO *));
 */
int
__free_filereg_info(p)
	VRFY_FILEREG_INFO *p;
{
	if (p == NULL)
		return (0);
	if (p ->fname != NULL)
		__os_free(NULL, (void *)(p->fname));
	if (p->fileid.data != NULL)
		__os_free(NULL, p->fileid.data);
	if (p->dbregids != NULL)
		__os_free(NULL, p->dbregids);
	__os_free(NULL, p);

	return (0);
}

/*
 * PUBLIC: int __get_ckp_info __P((const DB_LOG_VRFY_INFO *, DB_LSN,
 * PUBLIC:     VRFY_CKP_INFO **));
 */
int
__get_ckp_info (lvinfo, lsn, ckpinfopp)
	const DB_LOG_VRFY_INFO *lvinfo;
	DB_LSN lsn;
	VRFY_CKP_INFO **ckpinfopp;
{
	int ret;
	DBT key, data;
	VRFY_CKP_INFO *ckpinfo;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = &lsn;
	key.size = sizeof(DB_LSN);
	BDBOP3(lvinfo->dbenv, __db_get(lvinfo->ckps, lvinfo->ip, NULL,
	    &key, &data, 0), DB_NOTFOUND, "__get_ckp_info");

	if (ret == DB_NOTFOUND)
		goto err;

	if ((ret = __os_malloc(lvinfo->dbenv->env,
	    sizeof(VRFY_CKP_INFO), &ckpinfo)) != 0)
		goto err;
	memcpy(ckpinfo, data.data, sizeof(VRFY_CKP_INFO));
	*ckpinfopp = ckpinfo;
err:
	return (ret);

}

/*
 * PUBLIC: int __get_last_ckp_info __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:     VRFY_CKP_INFO **));
 */
int
__get_last_ckp_info (lvinfo, ckpinfopp)
	const DB_LOG_VRFY_INFO *lvinfo;
	VRFY_CKP_INFO **ckpinfopp;
{
	int ret, tret;
	DBT key, data;
	VRFY_CKP_INFO *ckpinfo;
	DBC *csr;

	csr = NULL;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	BDBOP(__db_cursor(lvinfo->ckps, lvinfo->ip, NULL, &csr, 0));
	if ((ret = __dbc_get(csr, &key, &data, DB_LAST)) != 0)
		goto err;

	if ((ret = __os_malloc(lvinfo->dbenv->env,
	    sizeof(VRFY_CKP_INFO), &ckpinfo)) != 0)
		goto err;
	DB_ASSERT(lvinfo->dbenv->env, sizeof(VRFY_CKP_INFO) == data.size);
	memcpy(ckpinfo, data.data, sizeof(VRFY_CKP_INFO));
	*ckpinfopp = ckpinfo;
err:
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	if (ret != 0 && ret != DB_NOTFOUND)
		__db_err(lvinfo->dbenv->env, ret, "__get_last_ckp_info");
	return (ret);
}

/*
 * PUBLIC: int __put_ckp_info __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:     const VRFY_CKP_INFO *));
 */
int __put_ckp_info (lvinfo, ckpinfo)
	const DB_LOG_VRFY_INFO *lvinfo;
	const VRFY_CKP_INFO *ckpinfo;
{
	int ret;
	DBT key, data;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = (void *)&ckpinfo->lsn;
	key.size = sizeof(DB_LSN);
	data.data = (void *)ckpinfo;
	data.size = sizeof(VRFY_CKP_INFO);

	BDBOP2(lvinfo->dbenv, __db_put(lvinfo->ckps, lvinfo->ip,
	    NULL, &key, &data, 0), "__put_ckp_info");
	return (0);
}

/*
 * PUBLIC: int __get_timestamp_info __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:     DB_LSN, VRFY_TIMESTAMP_INFO **));
 */
int __get_timestamp_info (lvinfo, lsn, tsinfopp)
	const DB_LOG_VRFY_INFO *lvinfo;
	DB_LSN lsn;
	VRFY_TIMESTAMP_INFO **tsinfopp;
{
	int ret;
	DBT key, data;
	VRFY_TIMESTAMP_INFO *tsinfo;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = &lsn;
	key.size = sizeof(DB_LSN);
	BDBOP3(lvinfo->dbenv, __db_get(lvinfo->lsntime, lvinfo->ip, NULL,
	    &key, &data, 0), DB_NOTFOUND, "__get_timestamp_info");

	if (ret == DB_NOTFOUND)
		goto err;

	if ((ret = __os_malloc(lvinfo->dbenv->env,
	    sizeof(VRFY_TIMESTAMP_INFO), &tsinfo)) != 0)
		goto err;

	memcpy(tsinfo, data.data, sizeof(VRFY_TIMESTAMP_INFO));
	*tsinfopp = tsinfo;
err:
	return (ret);
}

/*
 * __get_latest_timestamp_info --
 *	Get latest timestamp info before lsn.
 * PUBLIC: int __get_latest_timestamp_info __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:     DB_LSN, VRFY_TIMESTAMP_INFO **));
 */
int __get_latest_timestamp_info(lvinfo, lsn, tsinfopp)
	const DB_LOG_VRFY_INFO *lvinfo;
	DB_LSN lsn;
	VRFY_TIMESTAMP_INFO **tsinfopp;
{
	int ret, tret;
	DBT key, data;
	VRFY_TIMESTAMP_INFO *tsinfo;
	DBC *csr;

	csr = NULL;
	ret = tret = 0;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &lsn;
	key.size = sizeof(lsn);
	BDBOP(__db_cursor(lvinfo->lsntime, lvinfo->ip, NULL, &csr, 0));

	BDBOP(__dbc_get(csr, &key, &data, DB_SET));
	BDBOP(__dbc_get(csr, &key, &data, DB_PREV));

	if ((ret = __os_malloc(lvinfo->dbenv->env, sizeof(VRFY_TIMESTAMP_INFO),
	    &tsinfo)) != 0)
		goto err;

	memcpy(tsinfo, data.data, sizeof(VRFY_TIMESTAMP_INFO));
	*tsinfopp = tsinfo;

err:
	if (ret != 0 && ret != DB_NOTFOUND)
		__db_err(lvinfo->dbenv->env,
		    ret, "__get_latest_timestamp_info");
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	return (ret);
}

/*
 * PUBLIC: int __put_timestamp_info __P((const DB_LOG_VRFY_INFO *,
 * PUBLIC:     const VRFY_TIMESTAMP_INFO *));
 */
int __put_timestamp_info (lvinfo, tsinfo)
	const DB_LOG_VRFY_INFO *lvinfo;
	const VRFY_TIMESTAMP_INFO *tsinfo;
{
	int ret;
	DBT key, data;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = (void *)&(tsinfo->lsn);
	key.size = sizeof(DB_LSN);
	data.data = (void *)tsinfo;
	data.size = sizeof(VRFY_TIMESTAMP_INFO);
	BDBOP2(lvinfo->dbenv, __db_put(lvinfo->lsntime, lvinfo->ip, NULL,
	    &key, &data, 0), "__put_timestamp_info");

	return (0);
}

static int
__lv_txnrgns_lsn_cmp (db, d1, d2)
	DB *db;
	const DBT *d1, *d2;
{
	struct __lv_txnrange r1, r2;

	DB_ASSERT(db->env, d1->size == sizeof(r1));
	DB_ASSERT(db->env, d2->size == sizeof(r2));
	memcpy(&r1, d1->data, d1->size);
	memcpy(&r2, d2->data, d2->size);

	return (LOG_COMPARE(&(r1.end), &(r2.end)));
}

/*
 * __find_lsnrg_by_timerg --
 *	Find the lsn closed interval [beginlsn, endlsn] so that the
 *	corresponding timestamp interval fully contains interval [begin, end].
 * PUBLIC: int __find_lsnrg_by_timerg __P((DB_LOG_VRFY_INFO *,
 * PUBLIC:     time_t, time_t, DB_LSN *, DB_LSN *));
 */
int
__find_lsnrg_by_timerg(lvinfo, begin, end, startlsn, endlsn)
	DB_LOG_VRFY_INFO *lvinfo;
	time_t begin, end;
	DB_LSN *startlsn, *endlsn;
{
	int ret, tret;
	DBC *csr;
	struct __lv_timestamp_info *t1, *t2;
	DBT key, data;

	ret = tret = 0;
	csr = NULL;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	BDBOP(__db_cursor(lvinfo->timelsn, lvinfo->ip, NULL, &csr, 0));

	/*
	 * We want a lsn range that completely contains [begin, end], so
	 * try move 1 record prev when getting the startlsn.
	 */
	key.data = &begin;
	key.size = sizeof(begin);
	BDBOP(__dbc_get(csr, &key, &data, DB_SET_RANGE));
	if ((ret = __dbc_get(csr, &key, &data, DB_PREV)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;
	if (ret == DB_NOTFOUND)/* begin is smaller than the smallest key. */
		startlsn->file = startlsn->offset = 0;/* beginning. */
	else {
		t1 = (struct __lv_timestamp_info *)data.data;
		*startlsn = t1->lsn;
	}

	/*
	 * Move to the last key/data pair of the duplicate set to get the
	 * biggest lsn having end as timestamp.
	 */
	key.data = &end;
	key.size = sizeof(end);
	if ((ret = __dbc_get(csr, &key, &data, DB_SET_RANGE)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;
	if (ret == DB_NOTFOUND) {
		endlsn->file = endlsn->offset = (u_int32_t)-1;/* Biggest lsn. */
		ret = 0;
		goto err; /* We are done. */
	}

	/*
	 * Go to the biggest lsn of the dup set, if the key is the last one,
	 * go to the last one.
	 */
	if ((ret = __dbc_get(csr, &key, &data, DB_NEXT_NODUP)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;

	if (ret == DB_NOTFOUND)
		BDBOP(__dbc_get(csr, &key, &data, DB_LAST));
	else
		BDBOP(__dbc_get(csr, &key, &data, DB_PREV));

	t2 = (struct __lv_timestamp_info *)data.data;
	*endlsn = t2->lsn;
err:
	if (ret == DB_NOTFOUND)
		ret = 0;
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	return (ret);
}

/*
 * PUBLIC: int __add_txnrange __P((DB_LOG_VRFY_INFO *, u_int32_t,
 * PUBLIC:     DB_LSN, int32_t, int));
 */
int __add_txnrange (lvinfo, txnid, lsn, when, ishead)
	DB_LOG_VRFY_INFO *lvinfo;
	u_int32_t txnid;
	DB_LSN lsn;
	int32_t when;
	int ishead; /* Whether it's the 1st log of the txn. */
{
	int ret, tret;
	DBC *csr;
	struct __lv_txnrange tr, *ptr;
	DBT key, data;

	csr = NULL;
	ret = 0;
	ptr = NULL;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	memset(&tr, 0, sizeof(tr));

	key.data = &txnid;
	key.size = sizeof(txnid);
	tr.txnid = txnid;
	BDBOP(__db_cursor(lvinfo->txnrngs, lvinfo->ip, NULL, &csr, 0));
	/*
	 * Note that we will backward play the logs to gather such information.
	 */
	if (!ishead) {
		tr.end = lsn;
		tr.when_commit = when;
		data.data = &tr;
		data.size = sizeof(tr);
		BDBOP(__dbc_put(csr, &key, &data, DB_KEYFIRST));
	} else {
		/*
		 * Dup data sorted by lsn, and we are backward playing logs,
		 * so the 1st record should be the one we want.
		 */
		BDBOP(__dbc_get(csr, &key, &data, DB_SET));
		ptr = (struct __lv_txnrange *)data.data;
		DB_ASSERT(lvinfo->dbenv->env, IS_ZERO_LSN(ptr->begin));
		ptr->begin = lsn;
		BDBOP(__dbc_put(csr, &key, &data, DB_CURRENT));
	}

err:
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	return (ret);
}

/*
 * __get_aborttxn --
 *	If lsn is the last log of an aborted txn T, T's txnid is
 *	returned via the log verify handle.
 *
 * PUBLIC: int __get_aborttxn __P((DB_LOG_VRFY_INFO *, DB_LSN));
 */
int
__get_aborttxn(lvinfo, lsn)
	DB_LOG_VRFY_INFO *lvinfo;
	DB_LSN lsn;
{
	int ret, tret;
	u_int32_t txnid;
	DBC *csr;
	DBT key, data;

	csr = NULL;
	txnid = 0;
	ret = tret = 0;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &lsn;
	key.size = sizeof(lsn);
	BDBOP(__db_cursor(lvinfo->txnaborts, lvinfo->ip, NULL, &csr, 0));
	BDBOP(__dbc_get(csr, &key, &data, DB_SET));
	memcpy(&txnid, data.data, data.size);
	/*
	 * The lsn is the last op of an aborted txn, call __on_txnabort
	 * before processing next log record.
	 */
	lvinfo->aborted_txnid = txnid;
	lvinfo->aborted_txnlsn = lsn;

err:
	/* It's OK if can't find it. */
	if (ret == DB_NOTFOUND)
		ret = 0;
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	return (ret);
}

/*
 * __txn_started --
 *	Whether txnid is started before lsn and ended after lsn.
 *
 * PUBLIC: int __txn_started __P((DB_LOG_VRFY_INFO *,
 * PUBLIC:     DB_LSN, u_int32_t, int *));
 */
int
__txn_started(lvinfo, lsn, txnid, res)
	DB_LOG_VRFY_INFO *lvinfo;
	DB_LSN lsn;
	u_int32_t txnid;
	int *res;
{
	int ret, tret;
	DBC *csr;
	DBT key, data;
	struct __lv_txnrange *ptr, tr;

	ret = *res = 0;
	csr = NULL;
	memset(&tr, 0, sizeof(tr));
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = &txnid;
	key.size = sizeof(txnid);

	BDBOP(__db_cursor(lvinfo->txnrngs, lvinfo->ip, NULL, &csr, 0));
	BDBOP(__dbc_get(csr, &key, &data, DB_SET));
	for (;ret == 0; ret = __dbc_get(csr, &key, &data, DB_NEXT_DUP)) {
		ptr = (struct __lv_txnrange *)data.data;
		if (LOG_COMPARE(&lsn, &(ptr->begin)) > 0 &&
		    LOG_COMPARE(&lsn, &(ptr->end)) <= 0) {
			*res = 1;
			break;
		}
	}
err:
	if (ret == DB_NOTFOUND)
		ret = 0;/* It's OK if can't find it. */
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	return (ret);
}

/*
 * PUBLIC: int __set_logvrfy_dbfuid __P((DB_LOG_VRFY_INFO *));
 */
int
__set_logvrfy_dbfuid(lvinfo)
	DB_LOG_VRFY_INFO *lvinfo;
{
	int ret;
	const char *p;
	DBT key, data;
	size_t buflen;

	p = NULL;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	/* So far we only support verifying a specific db file. */
	p = lvinfo->lv_config->dbfile;
	buflen = sizeof(char) * (strlen(p) + 1);
	key.data = (char *)p;
	key.size = (u_int32_t)buflen;

	BDBOP2(lvinfo->dbenv, __db_get(lvinfo->fnameuid, lvinfo->ip, NULL,
	    &key, &data, 0), "__set_logvrfy_dbfuid");

	memcpy(lvinfo->target_dbid, data.data, DB_FILE_ID_LEN);

	return (ret);
}

/*
 * __add_page_to_txn --
 *	Try adding a page to a txn, result brings back if really added(0/1)
 *	or if there is an access violation(-1).
 * PUBLIC: int __add_page_to_txn __P((DB_LOG_VRFY_INFO *,
 * PUBLIC:     int32_t, db_pgno_t, u_int32_t, u_int32_t *, int *));
 */
int
__add_page_to_txn (lvinfo, dbregid, pgno, txnid, otxn, result)
	DB_LOG_VRFY_INFO *lvinfo;
	int32_t dbregid;
	db_pgno_t pgno;
	u_int32_t txnid, *otxn;
	int *result;
{
	int ret;
	u_int8_t *buf;
	DBT key, data;
	size_t buflen;
	u_int32_t txnid2;
	VRFY_FILELIFE *pff;

	if (txnid < TXN_MINIMUM) {
		*result = 0;
		return (0);
	}
	buf = NULL;
	ret = 0;
	txnid2 = 0;
	pff = NULL;
	buflen = sizeof(u_int8_t) * DB_FILE_ID_LEN + sizeof(db_pgno_t);
	BDBOP(__os_malloc(lvinfo->dbenv->env, buflen, &buf));
	memset(buf, 0, buflen);
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	/*
	 * We use the file uid as key because a single db file can have
	 * multiple dbregid at the same time, and we may neglect the fact
	 * that the same db file is being updated by multiple txns if we use
	 * dbregid as key.
	 */
	key.data = &dbregid;
	key.size = sizeof(dbregid);
	if ((ret = __db_get(lvinfo->dbregids, lvinfo->ip, NULL,
	    &key, &data, 0)) != 0) {
		if (ret == DB_NOTFOUND) {
			if (F_ISSET(lvinfo, DB_LOG_VERIFY_PARTIAL)) {
				ret = 0;
				goto out;
			} else
				F_SET(lvinfo, DB_LOG_VERIFY_INTERR);
		}
		goto err;
	}
	pff = (VRFY_FILELIFE *)data.data;
	memcpy(buf, pff->fileid, DB_FILE_ID_LEN);
	memcpy(buf + DB_FILE_ID_LEN, (u_int8_t *)&pgno, sizeof(pgno));
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = buf;
	key.size = (u_int32_t)buflen;
	if ((ret = __db_get(lvinfo->pgtxn, lvinfo->ip, NULL,
	    &key, &data, 0)) != 0) {
		if (ret == DB_NOTFOUND) {
			data.data = &txnid;
			data.size = sizeof(txnid);
			BDBOP(__db_put(lvinfo->pgtxn, lvinfo->ip, NULL, &key,
			    &data, 0));
			*result = 1;
			ret = 0;/* This is not an error. */
		}
		goto err;
	}
	DB_ASSERT(lvinfo->dbenv->env, data.size == sizeof(txnid2));
	memcpy(&txnid2, data.data, data.size);
	if (txnid == txnid2)/* The same txn already has the page. */
		*result = 0;
	else {/* Txn txnid is updating pages still held by txnid2. */
		*result = -1;
		*otxn = txnid2;
	}
out:
	/* result is set to -1 on violation, 0 if already has it, 1 if added. */
err:
	if (buf != NULL)
		__os_free(lvinfo->dbenv->env, buf);
	return (ret);
}

/*
 * PUBLIC: int __del_txn_pages __P((DB_LOG_VRFY_INFO *, u_int32_t));
 */
int
__del_txn_pages(lvinfo, txnid)
	DB_LOG_VRFY_INFO *lvinfo;
	u_int32_t txnid;
{
	int ret;
	DBT key;

	ret = 0;
	memset(&key, 0, sizeof(DBT));
	key.data = &txnid;
	key.size = sizeof(txnid);

	BDBOP(__db_del(lvinfo->txnpg, lvinfo->ip, NULL, &key, 0));

err:
	return (ret);
}

/*
 * __is_ancestor_txn --
 *	Tells via res if ptxnid is txnid's parent txn at the moment of lsn.
 *
 * PUBLIC: int __is_ancestor_txn __P((DB_LOG_VRFY_INFO *,
 * PUBLIC:     u_int32_t, u_int32_t, DB_LSN, int *));
 */
int
__is_ancestor_txn (lvinfo, ptxnid, txnid, lsn, res)
	DB_LOG_VRFY_INFO *lvinfo;
	u_int32_t ptxnid, txnid;
	DB_LSN lsn;
	int *res;
{
	u_int32_t ptid;
	int ret, tret;
	DBC *csr;
	DB *pdb;
	DBT key, data;
	struct __lv_txnrange tr;

	ret = 0;
	ptid = txnid;
	csr = NULL;
	pdb = lvinfo->txnrngs;
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	*res = 0;
	BDBOP(__db_cursor(pdb, lvinfo->ip, NULL, &csr, 0));

	/* See if ptxnid is an ancestor of txnid. */
	do {
		key.data = &ptid;
		key.size = sizeof(ptid);
		BDBOP(__dbc_get(csr, &key, &data, DB_SET));
		/* A txnid maybe reused, we want the range having lsn in it. */
		for (;ret == 0;
		    ret = __dbc_get(csr, &key, &data, DB_NEXT_DUP)) {
			DB_ASSERT(pdb->env, sizeof(tr) == data.size);
			memcpy(&tr, data.data, data.size);
			if (tr.ptxnid > 0 &&
			    LOG_COMPARE(&lsn, &(tr.begin)) >= 0 &&
			    LOG_COMPARE(&lsn, &(tr.end)) <= 0)
				break;
		}

		if (tr.ptxnid == ptxnid) {
			*res = 1;
			goto out;
		} else
			ptid = tr.ptxnid;

	} while (ptid != 0);
out:

err:
	if (ret == DB_NOTFOUND)
		ret = 0;
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	return (ret);
}

/*
 * PUBLIC: int __return_txn_pages __P((DB_LOG_VRFY_INFO *,
 * PUBLIC:     u_int32_t, u_int32_t));
 */
int __return_txn_pages(lvh, ctxn, ptxn)
	DB_LOG_VRFY_INFO *lvh;
	u_int32_t ctxn, ptxn;
{
	int ret, tret;
	DBC *csr;
	DB *pdb, *sdb;
	DBT key, key2, data, data2;
	char buf[DB_FILE_ID_LEN + sizeof(db_pgno_t)];

	ret = tret = 0;
	csr = NULL;
	sdb = lvh->txnpg;
	pdb = lvh->pgtxn;
	memset(&key, 0, sizeof(DBT));
	memset(&key2, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	memset(&data2, 0, sizeof(DBT));

	BDBOP(__db_cursor(sdb, lvh->ip, NULL, &csr, 0));
	key.data = &ctxn;
	key.size = sizeof(ctxn);
	key2.data = &ptxn;
	key2.size = sizeof(ptxn);
	data2.data = buf;
	data2.ulen = DB_FILE_ID_LEN + sizeof(db_pgno_t);
	data2.flags = DB_DBT_USERMEM;

	for (ret = __dbc_pget(csr, &key, &data2, &data, DB_SET); ret == 0;
	    ret = __dbc_pget(csr, &key, &data2, &data, DB_NEXT_DUP))
		BDBOP(__db_put(pdb, lvh->ip, NULL, &data2, &key2, 0));
	if ((ret = __del_txn_pages(lvh, ctxn)) != 0 && ret != DB_NOTFOUND)
		goto err;
err:
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;
	return (ret);
}

#define	ADD_ITEM(lvh, logtype) ((lvh)->logtype_names[(logtype)] = (#logtype))
static void
__lv_setup_logtype_names(lvinfo)
	DB_LOG_VRFY_INFO *lvinfo;
{
	ADD_ITEM(lvinfo, DB___bam_irep);
	ADD_ITEM(lvinfo, DB___bam_split_42);
	ADD_ITEM(lvinfo, DB___bam_split);
	ADD_ITEM(lvinfo, DB___bam_rsplit);
	ADD_ITEM(lvinfo, DB___bam_adj);
	ADD_ITEM(lvinfo, DB___bam_cadjust);
	ADD_ITEM(lvinfo, DB___bam_cdel);
	ADD_ITEM(lvinfo, DB___bam_repl);
	ADD_ITEM(lvinfo, DB___bam_root);
	ADD_ITEM(lvinfo, DB___bam_curadj);
	ADD_ITEM(lvinfo, DB___bam_rcuradj);
	ADD_ITEM(lvinfo, DB___bam_relink_43);
	ADD_ITEM(lvinfo, DB___bam_merge_44);
	ADD_ITEM(lvinfo, DB___crdel_metasub);
	ADD_ITEM(lvinfo, DB___crdel_inmem_create);
	ADD_ITEM(lvinfo, DB___crdel_inmem_rename);
	ADD_ITEM(lvinfo, DB___crdel_inmem_remove);
	ADD_ITEM(lvinfo, DB___dbreg_register);
	ADD_ITEM(lvinfo, DB___db_addrem);
	ADD_ITEM(lvinfo, DB___db_big);
	ADD_ITEM(lvinfo, DB___db_ovref);
	ADD_ITEM(lvinfo, DB___db_relink_42);
	ADD_ITEM(lvinfo, DB___db_debug);
	ADD_ITEM(lvinfo, DB___db_noop);
	ADD_ITEM(lvinfo, DB___db_pg_alloc_42);
	ADD_ITEM(lvinfo, DB___db_pg_alloc);
	ADD_ITEM(lvinfo, DB___db_pg_free_42);
	ADD_ITEM(lvinfo, DB___db_pg_free);
	ADD_ITEM(lvinfo, DB___db_cksum);
	ADD_ITEM(lvinfo, DB___db_pg_freedata_42);
	ADD_ITEM(lvinfo, DB___db_pg_freedata);
	ADD_ITEM(lvinfo, DB___db_pg_init);
	ADD_ITEM(lvinfo, DB___db_pg_sort_44);
	ADD_ITEM(lvinfo, DB___db_pg_trunc);
	ADD_ITEM(lvinfo, DB___db_realloc);
	ADD_ITEM(lvinfo, DB___db_relink);
	ADD_ITEM(lvinfo, DB___db_merge);
	ADD_ITEM(lvinfo, DB___db_pgno);
#ifdef HAVE_HASH
	ADD_ITEM(lvinfo, DB___ham_insdel);
	ADD_ITEM(lvinfo, DB___ham_newpage);
	ADD_ITEM(lvinfo, DB___ham_splitdata);
	ADD_ITEM(lvinfo, DB___ham_replace);
	ADD_ITEM(lvinfo, DB___ham_copypage);
	ADD_ITEM(lvinfo, DB___ham_metagroup_42);
	ADD_ITEM(lvinfo, DB___ham_metagroup);
	ADD_ITEM(lvinfo, DB___ham_groupalloc_42);
	ADD_ITEM(lvinfo, DB___ham_groupalloc);
	ADD_ITEM(lvinfo, DB___ham_changeslot);
	ADD_ITEM(lvinfo, DB___ham_contract);
	ADD_ITEM(lvinfo, DB___ham_curadj);
	ADD_ITEM(lvinfo, DB___ham_chgpg);
#endif
#ifdef HAVE_QUEUE
	ADD_ITEM(lvinfo, DB___qam_incfirst);
	ADD_ITEM(lvinfo, DB___qam_mvptr);
	ADD_ITEM(lvinfo, DB___qam_del);
	ADD_ITEM(lvinfo, DB___qam_add);
	ADD_ITEM(lvinfo, DB___qam_delext);
#endif
	ADD_ITEM(lvinfo, DB___txn_regop_42);
	ADD_ITEM(lvinfo, DB___txn_regop);
	ADD_ITEM(lvinfo, DB___txn_ckp_42);
	ADD_ITEM(lvinfo, DB___txn_ckp);
	ADD_ITEM(lvinfo, DB___txn_child);
	ADD_ITEM(lvinfo, DB___txn_xa_regop_42);
	ADD_ITEM(lvinfo, DB___txn_prepare);
	ADD_ITEM(lvinfo, DB___txn_recycle);
	ADD_ITEM(lvinfo, DB___fop_create_42);
	ADD_ITEM(lvinfo, DB___fop_create);
	ADD_ITEM(lvinfo, DB___fop_remove);
	ADD_ITEM(lvinfo, DB___fop_write_42);
	ADD_ITEM(lvinfo, DB___fop_write);
	ADD_ITEM(lvinfo, DB___fop_rename_42);
	ADD_ITEM(lvinfo, DB___fop_rename_noundo_46);
	ADD_ITEM(lvinfo, DB___fop_rename);
	ADD_ITEM(lvinfo, DB___fop_rename_noundo);
	ADD_ITEM(lvinfo, DB___fop_file_remove);
}
