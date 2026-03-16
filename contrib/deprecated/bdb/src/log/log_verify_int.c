/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

/*
 * This file contains verification functions for all types of log records,
 * one for each type. We can't make this automated like the log_type_print/read
 * functions because there are no consistent handling. Each type of log records
 * have unique ways to verify, and unique information to extract.
 *
 * In each verification function, we first call the log_type_read function
 * to get the log_type_args structure, then extract information according to
 * the type of log. The log types can be made into different categories, each
 * of which have similar types of information.
 *
 * For example, txn_regop and txn_ckp types both have timestamps, and we
 * want to maintain (timestamp,lsn) mapping, so we will have a on_timestamp
 * function, and call it in txn_regop_verify and txn_ckp_verify functions,
 * and in the two functions we may call other on_*** functions to extract and
 * verify other information.
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/fop.h"
#include "dbinc/hash.h"
#include "dbinc/heap.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

#include "dbinc/log_verify.h"

static int __log_vrfy_proc __P((DB_LOG_VRFY_INFO *, DB_LSN, DB_LSN,
    u_int32_t, DB_TXN *, int32_t, int *));
static int __lv_ckp_vrfy_handler __P((DB_LOG_VRFY_INFO *,
    VRFY_TXN_INFO *, void *));
static const char *__lv_dbreg_str __P((u_int32_t));
static int __lv_dbregid_to_dbtype __P((DB_LOG_VRFY_INFO *, int32_t, DBTYPE *));
static int __lv_dbt_str __P((const DBT *, char **));
static const char *__lv_dbtype_str __P((DBTYPE));
static u_int32_t __lv_first_offset __P((ENV *));
static int __lv_new_logfile_vrfy __P((DB_LOG_VRFY_INFO *, const DB_LSN *));
static int __lv_log_fwdscr_oncmt __P((DB_LOG_VRFY_INFO *, DB_LSN,
    u_int32_t, u_int32_t, int32_t));
static int __lv_log_fwdscr_onrec __P((DB_LOG_VRFY_INFO *,
    u_int32_t, u_int32_t, DB_LSN, DB_LSN));
static int __lv_log_mismatch __P((DB_LOG_VRFY_INFO *, DB_LSN, DBTYPE, DBTYPE));
static int __lv_on_bam_log __P((DB_LOG_VRFY_INFO *, DB_LSN, int32_t));
static int __lv_on_ham_log __P((DB_LOG_VRFY_INFO *, DB_LSN, int32_t));
static int __lv_on_heap_log __P((DB_LOG_VRFY_INFO *, DB_LSN, int32_t));
static int __lv_on_new_txn __P((DB_LOG_VRFY_INFO *, const DB_LSN *,
    const DB_TXN *, u_int32_t, int32_t, const DBT *));
static int __lv_on_nontxn_update __P((DB_LOG_VRFY_INFO *, const DB_LSN *,
    u_int32_t, u_int32_t, int32_t));
static int __lv_on_page_update __P((DB_LOG_VRFY_INFO *, DB_LSN, int32_t,
    db_pgno_t, DB_TXN *, int *));
static int __lv_on_qam_log __P((DB_LOG_VRFY_INFO *, DB_LSN, int32_t));
static int __lv_on_timestamp __P((DB_LOG_VRFY_INFO *, const DB_LSN *,
    int32_t, u_int32_t));
static int __lv_on_txn_aborted __P((DB_LOG_VRFY_INFO *));
static int __lv_on_txn_logrec __P((DB_LOG_VRFY_INFO *, const DB_LSN *,
    const DB_LSN *, const DB_TXN *, u_int32_t, int32_t));
static int __lv_vrfy_for_dbfile __P((DB_LOG_VRFY_INFO *, int32_t, int *));

/* General error handlers, called when a check fails. */
#define	ON_ERROR(lvh, errv) do {					\
	(lvh)->flags |= (errv);						\
	if (F_ISSET((lvh), DB_LOG_VERIFY_CAF))				\
		ret = 0;/* Ignore the error and continue. */		\
	goto err;							\
} while (0)

/* Used by logs of unsupported types. */
#define	ON_NOT_SUPPORTED(env, lvh, lsn, ltype) do {			\
	__db_errx((env), DB_STR_A("2536",				\
	    "[%lu][%lu] Not supported type of log record %u.",		\
	    "%lu %lu %u"), (u_long)((lsn).file), (u_long)((lsn).offset),\
	    (ltype));							\
	(lvh)->unknown_logrec_cnt++;					\
	goto err;							\
} while (0)

#define	SKIP_FORWARD_CHK(type) ((type) != DB___txn_regop &&		\
    (type) != DB___txn_ckp && (type) != DB___fop_rename &&		\
    (type) != DB___txn_child)

#define	NOTCOMMIT(type) ((type) != DB___txn_regop &&			\
	(type) != DB___txn_child)

#define	LOG_VRFY_PROC(lvh, lsn, argp, fileid) do {			\
	int __lv_log_vrfy_proc_step = 0;				\
	if ((ret = __log_vrfy_proc((lvh), (lsn), (argp)->prev_lsn,	\
	    (argp)->type, (argp)->txnp, (fileid),			\
	    &__lv_log_vrfy_proc_step)) != 0)				\
		goto err;						\
	if (__lv_log_vrfy_proc_step == 1)				\
		goto out;						\
	else if (__lv_log_vrfy_proc_step == -1)				\
		goto err;						\
	else								\
		DB_ASSERT(lvh->dbenv->env,				\
		    __lv_log_vrfy_proc_step == 0);			\
} while (0)

/* Log record handlers used by log types involving page updates. */
#define	ON_PAGE_UPDATE(lvh, lsn, argp, pgno) do {			\
	int __lv_onpgupdate_res;					\
	if ((ret = __lv_on_page_update((lvh), (lsn), (argp)->fileid,	\
	    (pgno), (argp)->txnp, &__lv_onpgupdate_res)) != 0)		\
		goto err;						\
	if (__lv_onpgupdate_res == 1)					\
		goto out;						\
	else if (__lv_onpgupdate_res == -1)				\
		goto err;						\
	else								\
		DB_ASSERT(lvh->dbenv->env, __lv_onpgupdate_res == 0);	\
} while (0)

static int
__lv_on_page_update(lvh, lsn, fileid, pgno, txnp, step)
	DB_LOG_VRFY_INFO *lvh;
	DB_LSN lsn;
	int32_t fileid;
	db_pgno_t pgno;
	DB_TXN *txnp;
	int *step;
{
	u_int32_t otxn, txnid;
	int res, ret;

	txnid = txnp->txnid;
	res = ret = 0;

	if ((ret = __add_page_to_txn(lvh, fileid, pgno,
	    txnid, &otxn, &res)) != 0)
		ON_ERROR(lvh, DB_LOG_VERIFY_INTERR);
	if (res != -1) {/* No access violation, we are done. */
		*step = 0;
		goto out;
	}
	/*
	 * It's OK for a child txn to update its parent's page, but not OK
	 * for a parent txn to update its active child's pages. We can't
	 * detect the child's abort, so we may false alarm that a parent txn
	 * is updating its child's pages.
	 */
	if ((ret = __is_ancestor_txn(lvh, otxn, txnid, lsn, &res)) != 0)
		ON_ERROR(lvh, DB_LOG_VERIFY_INTERR);
	if (res) {/* The txnid is updating its parent otxn's pages. */
		*step = 0;
		goto out;
	}
	if ((ret = __is_ancestor_txn(lvh, txnid, otxn, lsn, &res)) != 0)
		ON_ERROR(lvh, DB_LOG_VERIFY_INTERR);
	if (res) {/* The txnid is updating its active child otxn's pages. */
		__db_errx(lvh->dbenv->env, DB_STR_A("2537",
		    "[%lu][%lu] [WARNING] Parent txn %lx is updating its "
		    "active child txn %lx's pages, or %lx aborted.",
		    "%lu %lu %lx %lx %lx"), (u_long)lsn.file,
		    (u_long)lsn.offset, (u_long)txnid,
		    (u_long)otxn, (u_long)otxn);
		*step = 0;
		goto out;
	}
	/*
	 * It's likely that the two txns are parent-child and the child
	 * aborted, but from the log we can't figure out this fact.
	 */
	__db_errx(lvh->dbenv->env, DB_STR_A("2538",
	    "[%lu][%lu] [WARNING] Txn %lx is updating txn %lx's pages.",
	    "%lu %lu %lx %lx"), (u_long)lsn.file, (u_long)lsn.offset,
	    (u_long)txnid, (u_long)otxn);
	*step = 0;
out:
err:
	return (ret);
}

/*
 * This macro is put in all types of verify functions where a db file is
 * updated, but no page number/lock involved.
 */
#define	ON_PAGE_UPDATE4

/*
 * General log record handler used by all log verify functions.
 */
static int
__log_vrfy_proc(lvh, lsn, prev_lsn, type, txnp, fileid, step)
	DB_LOG_VRFY_INFO *lvh;
	DB_LSN lsn, prev_lsn;
	u_int32_t type; /* Log record type. */
	DB_TXN *txnp;
	int32_t fileid;
	int *step;
{
	int dovrfy, ret;

	dovrfy = 1;
	ret = 0;
	/*
	 * step is used to tell if go on with the rest of the caller, or
	 * goto err/out.
	 * 0: go on after this function; 1: goto out; -1: goto err.
	 */
	*step = 0;

	if (F_ISSET(lvh, DB_LOG_VERIFY_FORWARD)) {
		/* Commits are not abort/beginnings. */
		if (NOTCOMMIT(type) && ((ret = __lv_log_fwdscr_onrec(
		    lvh, txnp->txnid, type, prev_lsn, lsn)) != 0))
			goto err;
		if (SKIP_FORWARD_CHK(type))
			goto out;
	} else {/* Verifying */
		if (F_ISSET(lvh, DB_LOG_VERIFY_VERBOSE))
			__db_errx(lvh->dbenv->env, DB_STR_A("2539",
			    "[%lu][%lu] Verifying log record of type %s",
			    "%lu %lu %s"), (u_long)lsn.file,
			    (u_long)lsn.offset, LOGTYPE_NAME(lvh, type));
		/*
		 * If verifying a log range and we've passed the initial part
		 * which may have partial txns, remove the PARTIAL bit.
		 */
		if (F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL) &&
		    LOG_COMPARE(&lsn, &(lvh->valid_lsn)) >= 0) {
			lvh->valid_lsn.offset = lvh->valid_lsn.file = 0;
			F_CLR(lvh, DB_LOG_VERIFY_PARTIAL);
		}

		if ((ret = __lv_new_logfile_vrfy(lvh, &lsn)) != 0)
			ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
		/* If only verify a db file, ignore logs about other dbs. */
		if (F_ISSET(lvh, DB_LOG_VERIFY_DBFILE) && fileid !=
		    INVAL_DBREGID && (ret = __lv_vrfy_for_dbfile(lvh,
		    fileid, &dovrfy)) != 0)
			goto err;
		if (!dovrfy)
			goto out;
		if (lvh->aborted_txnid != 0 &&
		    ((ret = __lv_on_txn_aborted(lvh)) != 0))
			goto err;
		if ((ret = __get_aborttxn(lvh, lsn)) != 0)
			goto err;
		if (txnp->txnid >= TXN_MINIMUM) {
			if ((ret = __lv_on_txn_logrec(lvh, &lsn, &(prev_lsn),
			    txnp, type, fileid)) != 0)
				ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
		} else {/* Non-txnal updates. */
			if ((ret = __lv_on_nontxn_update(lvh, &lsn,
			    txnp->txnid, type, fileid)) != 0)
				ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
		}
	}
	if (0) {
out:
		*step = 1;
	}
	if (0) {
err:
		*step = -1;
	}
	return (ret);
}

/* Log record handlers used by log types for each access method. */
static int
__lv_on_bam_log(lvh, lsn, fileid)
	DB_LOG_VRFY_INFO *lvh;
	DB_LSN lsn;
	int32_t fileid;
{
	int ret;
	DBTYPE dbtype;
	if ((ret = __lv_dbregid_to_dbtype(lvh, fileid, &dbtype)) == 0 &&
	    dbtype != DB_BTREE && dbtype != DB_RECNO && dbtype != DB_HASH)
		ret = __lv_log_mismatch(lvh, lsn, dbtype, DB_BTREE);
	if (ret == DB_NOTFOUND && F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
		ret = 0;
	return (ret);
}

static int
__lv_on_ham_log(lvh, lsn, fileid)
	DB_LOG_VRFY_INFO *lvh;
	DB_LSN lsn;
	int32_t fileid;
{
	int ret;
	DBTYPE dbtype;
	if ((ret = __lv_dbregid_to_dbtype(lvh, fileid, &dbtype)) == 0 &&
	    dbtype != DB_HASH)
		ret = __lv_log_mismatch(lvh, lsn, dbtype, DB_HASH);
	if (ret == DB_NOTFOUND && F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
		ret = 0;
	return (ret);
}

static int
__lv_on_heap_log(lvh, lsn, fileid)
	DB_LOG_VRFY_INFO *lvh;
	DB_LSN lsn;
	int32_t fileid;
{
	int ret;
	DBTYPE dbtype;
	if ((ret = __lv_dbregid_to_dbtype(lvh, fileid, &dbtype)) == 0 &&
	    dbtype != DB_HEAP)
		ret = __lv_log_mismatch(lvh, lsn, dbtype, DB_HEAP);
	if (ret == DB_NOTFOUND && F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
		ret = 0;
	return (ret);
}

static int
__lv_on_qam_log(lvh, lsn, fileid)
	DB_LOG_VRFY_INFO *lvh;
	DB_LSN lsn;
	int32_t fileid;
{
	int ret;
	DBTYPE dbtype;
	if ((ret = __lv_dbregid_to_dbtype(lvh, fileid, &dbtype)) == 0 &&
	    dbtype != DB_QUEUE)
		ret = __lv_log_mismatch(lvh, lsn, dbtype, DB_QUEUE);
	if (ret == DB_NOTFOUND && F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
		ret = 0;
	return (ret);
}

/* Catch commits and store into lvinfo->txnrngs database. */
static int
__lv_log_fwdscr_oncmt(lvinfo, lsn, txnid, ptxnid, timestamp)
	DB_LOG_VRFY_INFO *lvinfo;
	DB_LSN lsn;
	u_int32_t txnid, ptxnid;
	int32_t timestamp;
{
	int ret;
	struct __lv_txnrange tr;
	DBT key, data;

	memset(&tr, 0, sizeof(tr));
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	tr.txnid = txnid;
	tr.end = lsn;
	tr.when_commit = timestamp;
	tr.ptxnid = ptxnid;
	key.data = &(txnid);
	key.size = sizeof(txnid);
	data.data = &tr;
	data.size = sizeof(tr);
	if ((ret = __db_put(lvinfo->txnrngs, lvinfo->ip, NULL,
	    &key, &data, 0)) != 0)
		goto err;
err:
	return (ret);
}

/* Catch aborts and txn beginnings and store into lvinfo->txnrngs database. */
static int
__lv_log_fwdscr_onrec(lvinfo, txnid, lrtype, prevlsn, lsn)
	DB_LOG_VRFY_INFO *lvinfo;
	u_int32_t txnid, lrtype;
	DB_LSN prevlsn, lsn;
{
	int doput, ret, ret2, tret;
	u_int32_t putflag;
	struct __lv_txnrange tr, *ptr;
	DBC *csr;
	DBT key, key2, data, data2;

	/* Ignore non-txnal log records. */
	if (txnid < TXN_MINIMUM)
		return (0);

	/* Not used for now, but may be used later. Pass lint checks. */
	COMPQUIET(lrtype ,0);
	putflag = 0;
	doput = ret = ret2 = 0;
	csr = NULL;
	memset(&tr, 0, sizeof(tr));
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	memset(&key2, 0, sizeof(DBT));
	memset(&data2, 0, sizeof(DBT));
	key.data = &txnid;
	key.size = sizeof(txnid);
	tr.txnid = txnid;
	tr.when_commit = 0;/* This is not a __txn_regop record. */

	if ((ret = __db_cursor(lvinfo->txnrngs, lvinfo->ip,
	    NULL, &csr, 0)) != 0)
		goto err;
	/*
	 * If the txnid is first seen here or reused later, it's aborted
	 * after this log record; if this log record is the 1st one of a txn,
	 * we have the beginning of the txn; otherwise the log record is one
	 * of the actions taken within the txn, and we don't do anything.
	 */
	if ((ret = __dbc_get(csr, &key, &data, DB_SET)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;

	ptr = (struct __lv_txnrange *)data.data;
	if (ret == DB_NOTFOUND || !IS_ZERO_LSN(ptr->begin)) {
		tr.end = lsn;
		data.data = &tr;
		data.size = sizeof(tr);
		doput = 1;
		key2.data = &lsn;
		key2.size = sizeof(lsn);
		data2.data = &(tr.txnid);
		data2.size = sizeof(tr.txnid);
		putflag = DB_KEYFIRST;
		if ((ret2 = __db_put(lvinfo->txnaborts, lvinfo->ip, NULL,
		    &key2, &data2, 0)) != 0) {
			ret = ret2;
			goto err;
		}
	} else if (ret == 0 && IS_ZERO_LSN(prevlsn)) {/* The beginning of txn.*/
		/* The begin field must be [0, 0]. */
		DB_ASSERT(lvinfo->dbenv->env, IS_ZERO_LSN(ptr->begin));
		ptr->begin = lsn;
		putflag = DB_CURRENT;
		doput = 1;
	}

	if (doput && (ret = __dbc_put(csr, &key, &data, putflag)) != 0)
		goto err;
err:
	if (csr != NULL && (tret = __dbc_close(csr)) != 0 && ret == 0)
		ret = tret;

	return (ret);
}

/*
 * Return 0 from dovrfy if verifying logs for a specified db file, and fileid
 * is not the one we want; Otherwise return 1 from dovrfy. If DB operations
 * failed, the error is returned.
 */
static int
__lv_vrfy_for_dbfile(lvh, fileid, dovrfy)
	DB_LOG_VRFY_INFO *lvh;
	int32_t fileid;
	int *dovrfy;
{
	u_int8_t tmpuid[DB_FILE_ID_LEN];
	VRFY_FILEREG_INFO *fregp;
	u_int32_t i;
	int ret, tret;
	DBT tgtkey;

	ret = tret = 0;
	*dovrfy = 0;
	fregp = NULL;
	memset(tmpuid, 0, sizeof(u_int8_t) * DB_FILE_ID_LEN);
	memset(&tgtkey, 0, sizeof(tgtkey));
	tgtkey.data = lvh->target_dbid;
	tgtkey.size = DB_FILE_ID_LEN;
	ret = __get_filereg_info(lvh, &tgtkey, &fregp);

	/*
	 * If the target db file is not seen yet, we don't verify any file,
	 * and it does not mean anything wrong.
	 */
	if (ret == DB_NOTFOUND) {
		ret = 0;
		goto out;
	}
	if (ret != 0)
		goto err;

	for (i = 0; i < fregp->regcnt; i++)
		if (fregp->dbregids[i] == fileid) {
			*dovrfy = 1;
			goto out;
		}
out:
err:
	if (fregp != NULL &&
	    (tret = __free_filereg_info(fregp)) != 0 && ret == 0)
		ret = tret;

	return (ret);
}

static int
__lv_log_mismatch(lvh, lsn, dbtype, exp_dbtype)
	DB_LOG_VRFY_INFO *lvh;
	DB_LSN lsn;
	DBTYPE dbtype, exp_dbtype;
{
	int ret;

	__db_errx(lvh->dbenv->env, DB_STR_A("2540",
	    "[%lu][%lu] Log record type does not match related database type, "
	    "current database type: %s, expected database type according to "
	    "the log record type: %s.", "%lu %lu %s %s"),
	    (u_long)lsn.file, (u_long)lsn.offset, __lv_dbtype_str(dbtype),
	    __lv_dbtype_str(exp_dbtype));
	ret = DB_LOG_VERIFY_BAD;
	ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
err:
	return (ret);
}

static int
__lv_dbregid_to_dbtype(lvh, id, ptype)
	DB_LOG_VRFY_INFO *lvh;
	int32_t id;
	DBTYPE *ptype;
{
	int ret;
	VRFY_FILELIFE *pflife;

	ret = 0;
	pflife = NULL;

	if ((ret = __get_filelife(lvh, id, &pflife)) != 0)
		goto err;
	*ptype = pflife->dbtype;
err:
	if (pflife != NULL)
	    __os_free(lvh->dbenv->env, pflife);

	return (ret);
}

/*
 * __db_log_verify_global_report --
 *	Report statistics data in DB_LOG_VRFY_INFO handle.
 *
 * PUBLIC: void __db_log_verify_global_report __P((const DB_LOG_VRFY_INFO *));
 */
void __db_log_verify_global_report (lvinfo)
	const DB_LOG_VRFY_INFO *lvinfo;
{
	u_int32_t i, nltype;

	__db_msg(lvinfo->dbenv->env,
	    "Number of active transactions: %u;", lvinfo->ntxn_active);
	__db_msg(lvinfo->dbenv->env,
	    "Number of committed transactions: %u;", lvinfo->ntxn_commit);
	__db_msg(lvinfo->dbenv->env,
	    "Number of aborted transactions: %u;", lvinfo->ntxn_abort);
	__db_msg(lvinfo->dbenv->env,
	    "Number of prepared transactions: %u;", lvinfo->ntxn_prep);
	__db_msg(lvinfo->dbenv->env,
	    "Total number of checkpoint: %u;", lvinfo->nckp);
	__db_msg(lvinfo->dbenv->env,
	    "Total number of non-transactional updates: %u;",
	    lvinfo->non_txnup_cnt);
	__db_msg(lvinfo->dbenv->env,
	    "Total number of unknown log records: %u;",
	    lvinfo->unknown_logrec_cnt);
	__db_msg(lvinfo->dbenv->env,
	    "Total number of app-specific log record: %u;",
	    lvinfo->external_logrec_cnt);
	__db_msg(lvinfo->dbenv->env,
	    "The number of each type of log record:");

	for (i = 0; i < 256; i++) {
		nltype = lvinfo->lrtypes[i];
		if (LOGTYPE_NAME(lvinfo, i) != NULL)
			__db_msg(lvinfo->dbenv->env, "\n\t%s : %u;",
			    LOGTYPE_NAME(lvinfo, i), nltype);
	}
}

/*
 * PUBLIC: int __crdel_metasub_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__crdel_metasub_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__crdel_metasub_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __crdel_metasub_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __crdel_inmem_create_verify __P((ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__crdel_inmem_create_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__crdel_inmem_create_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __crdel_inmem_create_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __crdel_inmem_rename_verify __P((ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__crdel_inmem_rename_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__crdel_inmem_rename_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __crdel_inmem_rename_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __crdel_inmem_remove_verify __P((ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__crdel_inmem_remove_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__crdel_inmem_remove_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __crdel_inmem_remove_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_addrem_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_addrem_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_addrem_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_addrem_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_big_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_big_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_big_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_big_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_ovref_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_ovref_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_ovref_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_ovref_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_relink_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_relink_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_relink_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_relink_42_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_debug_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_debug_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_debug_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __db_debug_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_noop_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_noop_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_noop_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_noop_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_alloc_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_alloc_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_alloc_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_alloc_42_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_alloc_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_alloc_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_alloc_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_alloc_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_free_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_free_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_free_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_free_42_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_free_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_free_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_free_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_free_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_cksum_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_cksum_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_cksum_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __db_cksum_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_freedata_42_verify __P((ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__db_pg_freedata_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_freedata_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_freedata_42_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_freedata_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_freedata_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_freedata_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_freedata_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_init_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_init_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_init_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_init_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_sort_44_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_sort_44_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_sort_44_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_sort_44_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pg_trunc_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pg_trunc_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pg_trunc_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pg_trunc_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE4 /* No pages are locked by txns. */
out:
err:
	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_realloc_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_realloc_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_realloc_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_realloc_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE4 /* No pages are locked by txns. */

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_relink_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_relink_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_relink_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_relink_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_merge_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_merge_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_merge_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_merge_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __db_pgno_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__db_pgno_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__db_pgno_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __db_pgno_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);

out:

err:

	__os_free(env, argp);
	return (ret);
}

static const char *
__lv_dbreg_str(op)
	u_int32_t op;
{
	const char *p;

	switch (op) {
	case DBREG_CHKPNT:
		p = "DBREG_CHKPNT";
		break;
	case DBREG_RCLOSE:
		p = "DBREG_RCLOSE";
		break;
	case DBREG_CLOSE:
		p = "DBREG_CLOSE";
		break;
	case DBREG_OPEN:
		p = "DBREG_OPEN";
		break;
	case DBREG_PREOPEN:
		p = "DBREG_PREOPEN";
		break;
	case DBREG_REOPEN:
		p = "DBREG_REOPEN";
		break;
	case DBREG_XCHKPNT:
		p = "DBREG_XCHKPNT";
		break;
	case DBREG_XOPEN:
		p = "DBREG_XOPEN";
		break;
	case DBREG_XREOPEN:
		p = "DBREG_XREOPEN";
		break;
	default:
		p = DB_STR_P("Unknown dbreg op code");
		break;
	}

	return (p);
}

static int
__lv_dbt_str(dbt, str)
	const DBT *dbt;
	char **str;
{
	char *p, *q;
	u_int32_t buflen, bufsz, i;
	int ret;

	ret = 0;
	p = q = NULL;
	buflen = bufsz = i = 0;
	bufsz = sizeof(char) * dbt->size * 2;

	if ((ret = __os_malloc(NULL, bufsz, &p)) != 0)
		goto err;
	q = (char *)dbt->data;

	memset(p, 0, bufsz);
	/*
	 * Each unprintable character takes up several bytes, so be ware of
	 * memory access violation.
	 */
	for (i = 0; i < dbt->size && buflen < bufsz; i++) {
		buflen = (u_int32_t)strlen(p);
		snprintf(p + buflen, bufsz - (buflen + 1),
		    isprint(q[i]) || q[i] == 0x0a ? "%c" : "%x", q[i]);
	}
	*str = p;
err:
	return (ret);
}

static const char *
__lv_dbtype_str(dbtype)
	DBTYPE dbtype;
{
	char *p;

	switch (dbtype) {
	case DB_BTREE:
		p = "DB_BTREE";
		break;
	case DB_HASH:
		p = "DB_HASH";
		break;
	case DB_RECNO:
		p = "DB_RECNO";
		break;
	case DB_QUEUE:
		p = "DB_QUEUE";
		break;
	default:
		p = DB_STR_P("Unknown db type");
		break;
	}

	return (p);
}

/*
 * PUBLIC: int __dbreg_register_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__dbreg_register_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__dbreg_register_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	VRFY_FILEREG_INFO *fregp, freg;
	VRFY_FILELIFE *pflife, flife;
	int checklife, rmv_dblife, ret, ret2;
	u_int32_t opcode;
	char *puid;
	const char *dbfname;

	dbfname = NULL;
	checklife = 1;
	opcode = 0;
	ret = ret2 = rmv_dblife = 0;
	puid = NULL;
	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;
	fregp = NULL;
	pflife = NULL;
	memset(&flife, 0, sizeof(flife));
	memset(&freg, 0, sizeof(freg));

	if ((ret = __dbreg_register_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	opcode = FLD_ISSET(argp->opcode, DBREG_OP_MASK);
	dbfname = argp->name.size == 0 ? "(null)" : (char *)(argp->name.data);
	/*
	 * We don't call LOG_VRFY_PROC macro here, so we have to copy the code
	 * snippet in __log_vrfy_proc here.
	 */
	if (F_ISSET(lvh, DB_LOG_VERIFY_FORWARD)) {
		if ((ret = __lv_log_fwdscr_onrec(lvh, argp->txnp->txnid,
		    argp->type, argp->prev_lsn, *lsnp)) != 0)
			goto err;
		goto out;
	}
	if (lvh->aborted_txnid != 0 && (ret = __lv_on_txn_aborted(lvh)) != 0)
		goto err;

	if ((ret = __get_filereg_info(lvh, &(argp->uid), &fregp)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;

	/*
	 * When DBREG_CLOSE, we should remove the fileuid-filename mapping
	 * from filereg because the file can be opened again with a different
	 * fileuid after closed.
	 */
	if (ret == 0 && IS_DBREG_CLOSE(opcode)) {
		if ((ret = __db_del(lvh->fileregs, lvh->ip, NULL,
		    &(argp->uid), 0)) != 0)
			goto err;
	}

	/*
	 * If this db file is seen for the 1st time, store filereg and
	 * filelife info. Since we will do a end-to-begin scan before the
	 * verification, we will be able to get the record but it's regcnt
	 * is 0 since we didn't know any dbregid yet.
	 */
	if (ret == DB_NOTFOUND || fregp->regcnt == 0) {
		/* Store filereg info unless it's a CLOSE. */
		freg.fileid = argp->uid;
		if (!IS_DBREG_CLOSE(opcode)) {
			freg.regcnt = 1;
			freg.dbregids = &(argp->fileid);
		} else {
			freg.regcnt = 0;
			freg.dbregids = NULL;
		}
		if (ret == DB_NOTFOUND) {
		/*
		 * If the db file is an in-memory db file, we can arrive
		 * here because there is no __fop_rename log for it;
		 * if the __fop_rename log record is out of the log range we
		 * verify, we will also arrive here.
		 */
			if ((ret = __os_malloc(env, argp->name.size + 1,
			    &(freg.fname))) != 0)
				goto err;
			memset(freg.fname, 0,
			    sizeof(char) * (argp->name.size + 1));
			(void)strncpy(freg.fname,
			    (const char *)(argp->name.data), argp->name.size);
		} else /* We already have the name. */
			if ((ret = __os_strdup(env,
			    fregp->fname, &(freg.fname))) != 0)
				goto err;

		if (!IS_DBREG_OPEN(opcode) &&
		    !F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL)) {
			/* It's likely that the DBREG_OPEN is not seen.*/
			__db_msg(env, DB_STR_A("2541",
			    "[%lu][%lu] Suspicious dbreg operation: %s, the "
			    "database file %s's register in log region does "
			    "not begin with an open operation.",
			    "%lu %lu %s %s"), (u_long)lsnp->file,
			    (u_long)lsnp->offset,
			    __lv_dbreg_str(opcode), dbfname);
		}

		/*
		 * PREOPEN is only generated when opening an in-memory db.
		 * Because we need to log the fileid we're allocating, but we
		 * don't have all the details yet, we are preopening the
		 * database and will actually complete the open later. So
		 * PREOPEN is not a real open, and the log should be ignored
		 * in log_verify.
		 * If fileuid is in a CLOSE operation there is no need to
		 * record it.
		 */
		if ((opcode != DBREG_PREOPEN) && !IS_DBREG_CLOSE(opcode) &&
		    (ret = __put_filereg_info(lvh, &freg)) != 0)
			goto err;

		/* Store filelife info unless it's a CLOSE dbreg operation. */
		if (!IS_DBREG_CLOSE(opcode)) {
			flife.lifetime = opcode;
			flife.dbregid = argp->fileid;
			flife.lsn = *lsnp;
			flife.dbtype = argp->ftype;
			flife.meta_pgno = argp->meta_pgno;
			memcpy(flife.fileid, argp->uid.data, argp->uid.size);
			if ((ret = __put_filelife(lvh, &flife)) != 0)
				goto err;
		}
		/* on_txn_logrec relies on the freg info in db first. */
		LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
		goto out;
	}

	/*
	 * Add dbregid if it's new, and store the file register info; or
	 * remove dbregid from fregp if we are closing the file.
	 */
	if ((ret = __add_dbregid(lvh, fregp, argp->fileid,
	    opcode, *lsnp, argp->ftype, argp->meta_pgno, &ret2)) != 0)
		goto err;
	ret = ret2;
	if (ret != 0 && ret != 1 && ret != 2 && ret != -1)
		goto err;/* DB operation error. */
	if (ret != 0) {
		/* Newly seen dbregid does not need to check life. */
		if (ret == 1)
			checklife = 0;
		else if (ret == -1)
			rmv_dblife = 1;/* The dbreg file id is closed. */
		else if (ret == 2) {
			__db_errx(env, DB_STR_A("2542",
			    "[%lu][%lu] Wrong dbreg operation "
			    "sequence, opening %s for id %d which is already "
			    "open.", "%lu %lu %s %d"),
			    (u_long)lsnp->file, (u_long)lsnp->offset,
			    dbfname, argp->fileid);
			ret = DB_LOG_VERIFY_BAD;
			ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
		}
		if (!rmv_dblife && (ret = __put_filereg_info(lvh, fregp)) != 0)
			goto err;
	}

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	if (!checklife)
		goto out;

	/*
	 * Verify the database type does not change, and the lifetime of a
	 * db file follow an open/chkpnt->[chkpnt]->close order.
	 * A VRFY_FILELIFE record is removed from db on DBREG_CLOSE,
	 * and inserted into db on DBREG_OPEN.
	 */
	if (!IS_DBREG_OPEN(opcode) &&
	    (ret = __get_filelife(lvh, argp->fileid, &pflife)) != 0) {
		if (ret == DB_NOTFOUND) {
			if (!F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL)) {
				__db_errx(env, DB_STR_A("2543",
				    "[%lu][%lu] Wrong dbreg operation sequence,"
				    "file %s with id %d is first seen of "
				    "status: %s", "%lu %lu %s %d"),
				    (u_long)lsnp->file, (u_long)lsnp->offset,
				    dbfname, argp->fileid,
				    __lv_dbreg_str(opcode));
				ret = DB_LOG_VERIFY_BAD;
				ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
			} else
				ret = 0;
		}
		goto err;
	}

	/* Can't go on verifying without pflife. */
	if (pflife == NULL)
		goto out;
	if (argp->ftype != pflife->dbtype) {
		if ((ret = __lv_dbt_str(&(argp->uid), &puid)) != 0)
			goto err;
		__db_errx(env, DB_STR_A("2544",
		    "[%lu][%lu] The dbtype of database file %s with uid %s "
		    " and id %d has changed from %s to %s.",
		    "%lu %lu %s %s %d %s %s"), (u_long)lsnp->file,
		    (u_long)lsnp->offset, dbfname, puid,
		    pflife->dbregid, __lv_dbtype_str(pflife->dbtype),
		    __lv_dbtype_str(argp->ftype));

		__os_free(env, puid);
		ret = DB_LOG_VERIFY_BAD;
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}

	if ((IS_DBREG_CLOSE(opcode) &&
	    (pflife->lifetime != DBREG_CHKPNT ||
	    pflife->lifetime != DBREG_XCHKPNT) &&
	    !IS_DBREG_OPEN(pflife->lifetime))) {
		__db_errx(env, DB_STR_A("2545",
		    "[%lu][%lu] Wrong dbreg operation sequence for file %s "
		    "with id %d, current status: %s, new status: %s",
		    "%lu %lu %s %d %s %s"), (u_long)lsnp->file,
		    (u_long)lsnp->offset, dbfname, pflife->dbregid,
		    __lv_dbreg_str(pflife->lifetime),
		    __lv_dbreg_str(opcode));
		ret = DB_LOG_VERIFY_BAD;
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}

	pflife->lifetime = opcode;
	pflife->lsn = *lsnp;
	if ((!rmv_dblife && (ret = __put_filelife(lvh, pflife)) != 0) ||
	    ((rmv_dblife || IS_DBREG_CLOSE(opcode)) &&
	    ((ret = __del_filelife(lvh, argp->fileid)) != 0)))
		goto err;

out:
	/* There may be something to do here in future. */
err:
	__os_free(env, argp);
	if (fregp != NULL &&
	    (ret2 = __free_filereg_info(fregp)) != 0 && ret == 0)
		ret = ret2;
	if (freg.fname != NULL)
		__os_free(env, freg.fname);
	if (pflife != NULL)
		__os_free(env, pflife);

	return (ret);
}

/*
 * PUBLIC: int __bam_split_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_split_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_split_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_split_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->left);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->right);
	/* Parent page lock is always released before __bam_page returns. */

	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_split_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_split_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_split_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_split_42_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_rsplit_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_rsplit_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_rsplit_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_rsplit_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_adj_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_adj_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_adj_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_adj_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_irep_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_irep_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_irep_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_irep_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_cadjust_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_cadjust_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_cadjust_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_cadjust_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_cdel_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_cdel_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_cdel_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_cdel_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_repl_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_repl_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_repl_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_repl_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_root_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_root_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_root_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_root_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __bam_curadj_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_curadj_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_curadj_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_curadj_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __bam_rcuradj_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_rcuradj_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_rcuradj_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_rcuradj_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	if ((ret = __lv_on_bam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __bam_relink_43_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_relink_43_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_relink_43_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_relink_43_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __bam_merge_44_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__bam_merge_44_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__bam_merge_44_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __bam_merge_44_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __fop_create_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_create_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__fop_create_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __fop_create_42_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __fop_create_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_create_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__fop_create_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __fop_create_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __fop_remove_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_remove_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__fop_remove_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __fop_remove_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __fop_write_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_write_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__fop_write_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __fop_write_42_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID); */
err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __fop_write_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_write_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__fop_write_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __fop_write_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);
	ON_PAGE_UPDATE4 /* No pages are locked by txns. */
out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __fop_rename_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_rename_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__fop_rename_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __fop_rename_42_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __fop_rename_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_rename_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__fop_rename_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	char *buf;
	int ret;
	size_t buflen;
	VRFY_FILEREG_INFO freg, *fregp;

	memset(&freg, 0, sizeof(freg));
	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;
	buf = NULL;

	if ((ret = __fop_rename_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);
	if (F_ISSET(lvh, DB_LOG_VERIFY_FORWARD)) {
		/*
		 * Since we get the fname-fuid map when iterating from end to
		 * beginning, we only store the latest file name, that's the
		 * name supposed to be used currently. So if the fileid is
		 * already stored, and we see it again here, it means the db
		 * file was renamed and we already have its latest name.
		 *
		 * Store the dbfile path (dir/fname) in case there are db
		 * files with same name in different data directories.
		 */
		if (__get_filereg_info(lvh, &(argp->fileid), &fregp) == 0) {
			if (fregp != NULL &&
			    (ret = __free_filereg_info(fregp)) != 0)
				goto err;
			goto out;
		}
		freg.fileid = argp->fileid;
		if ((ret = __os_malloc(env, buflen = argp->dirname.size +
		    argp->newname.size + 2, &buf)) != 0)
			goto err;
		snprintf(buf, buflen, "%s/%s", (char *)argp->dirname.data,
		    (char *)argp->newname.data);
		freg.fname = buf;
		/* Store the dbfilename<-->dbfileid map. */
		if ((ret = __put_filereg_info(lvh, &freg)) != 0)
			goto err;
	}
out:

err:
	if (buf != NULL)
		__os_free(lvh->dbenv->env, buf);
	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __fop_file_remove_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__fop_file_remove_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__fop_file_remove_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __fop_file_remove_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

out:

err:

	__os_free(env, argp);

	return (ret);
}

#ifdef HAVE_HASH
/*
 * PUBLIC: int __ham_insdel_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_insdel_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_insdel_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_insdel_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __ham_newpage_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_newpage_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_newpage_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_newpage_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE4 /* No pages are locked by txns. */
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __ham_splitdata_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_splitdata_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_splitdata_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_splitdata_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __ham_replace_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_replace_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_replace_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_replace_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);

	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __ham_copypage_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_copypage_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_copypage_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_copypage_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __ham_metagroup_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_metagroup_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_metagroup_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_metagroup_42_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __ham_metagroup_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_metagroup_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_metagroup_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_metagroup_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __ham_groupalloc_42_verify __P((ENV *, DBT *,
 * PUBLIC:     DB_LSN *, db_recops, void *));
 */
int
__ham_groupalloc_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_groupalloc_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_groupalloc_42_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __ham_groupalloc_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_groupalloc_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_groupalloc_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	VRFY_FILELIFE *pflife;
	int ret;

	ret = 0;
	pflife = NULL;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_groupalloc_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE4 /* No pages are locked by txns. */

	/*
	 * The __ham_groupalloc record is only generated when creating the
	 * hash sub database so it will always be on the master database's
	 * fileid.
	 */

	if ((ret = __get_filelife(lvh, argp->fileid, &pflife)) != 0)
		goto err;

	if (pflife->meta_pgno != PGNO_BASE_MD) {
		__db_errx(lvh->dbenv->env, DB_STR_A("2546",
		    "[%lu][%lu] __ham_groupalloc should apply only to the "
		    "master database with meta page number 0, current meta "
		    "page number is %d.", "%lu %lu %d"),
		    (u_long)lsnp->file, (u_long)lsnp->offset,
		     pflife->meta_pgno);
		ret = DB_LOG_VERIFY_BAD;
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}

out:

err:
	if (pflife != NULL)
	    __os_free(lvh->dbenv->env, pflife);

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __ham_changeslot_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_changeslot_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_changeslot_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_changeslot_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE4 /* No pages are locked by txns. */
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __ham_contract_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_contract_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_contract_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_contract_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __ham_curadj_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_curadj_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_curadj_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_curadj_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __ham_chgpg_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_chgpg_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__ham_chgpg_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __ham_chgpg_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE4 /* No pages are locked by txns. */
	if ((ret = __lv_on_ham_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);
	return (ret);
}
#endif

#ifdef HAVE_HEAP
/*
 * PUBLIC: int __heap_addrem_verify
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__heap_addrem_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__heap_addrem_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __heap_addrem_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_heap_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;
out:

err:
	__os_free(env, argp);
	return (ret);
}

/*
 * PUBLIC: int __heap_pg_alloc_verify
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__heap_pg_alloc_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__heap_pg_alloc_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __heap_pg_alloc_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_heap_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;
out:

err:
	__os_free(env, argp);
	return (ret);	
}

/*
 * PUBLIC: int __heap_trunc_meta_verify
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__heap_trunc_meta_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__heap_trunc_meta_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __heap_trunc_meta_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_heap_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;
out:

err:

	__os_free(env, argp);
	return (ret);	
}

/*
 * PUBLIC: int __heap_trunc_page_verify
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__heap_trunc_page_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__heap_trunc_page_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __heap_trunc_page_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	ON_PAGE_UPDATE(lvh, *lsnp, argp, argp->pgno);
	if ((ret = __lv_on_heap_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;
out:

err:
	__os_free(env, argp);
	return (ret);
}
#endif

#ifdef HAVE_QUEUE
/*
 * PUBLIC: int __qam_incfirst_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_incfirst_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__qam_incfirst_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __qam_incfirst_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	if ((ret = __lv_on_qam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __qam_mvptr_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_mvptr_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__qam_mvptr_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __qam_mvptr_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	if ((ret = __lv_on_qam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __qam_del_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_del_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__qam_del_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __qam_del_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	if ((ret = __lv_on_qam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __qam_add_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_add_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__qam_add_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __qam_add_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, argp->fileid);
	if ((ret = __lv_on_qam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __qam_delext_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__qam_delext_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__qam_delext_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret =
	    __qam_delext_read(env, NULL, NULL, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);
	if ((ret = __lv_on_qam_log(lvh, *lsnp, argp->fileid)) != 0)
		goto err;

out:

err:

	__os_free(env, argp);

	return (ret);
}
#endif

/*
 * PUBLIC: int __txn_regop_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_regop_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__txn_regop_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __txn_regop_42_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __txn_regop_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_regop_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__txn_regop_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret, ret2, started;
	VRFY_TXN_INFO *ptvi, *pptvi;
	VRFY_TIMESTAMP_INFO tsinfo;

	ptvi = pptvi = NULL;
	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;
	ret = ret2 = started = 0;

	if ((ret = __txn_regop_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	/*
	 * The __lv_log_fwdscr_oncmt call must precede LOG_VRFY_PROC otherwise
	 * this txn will be taken as an aborted txn.
	 */
	if (F_ISSET(lvh, DB_LOG_VERIFY_FORWARD)) {
		if ((ret = __lv_log_fwdscr_oncmt(lvh, *lsnp,
		    argp->txnp->txnid, 0, argp->timestamp)) != 0)
			goto err;

		tsinfo.lsn = *lsnp;
		tsinfo.timestamp = argp->timestamp;
		tsinfo.logtype = argp->type;
		if ((ret = __put_timestamp_info(lvh, &tsinfo)) != 0)
			goto err;
		goto out; /* We are done. */
	}

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);
	if ((ret = __del_txn_pages(lvh, argp->txnp->txnid)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;/* Some txns may have updated no pages. */
	if ((ret = __lv_on_timestamp(lvh, lsnp, argp->timestamp,
	    DB___txn_regop)) != 0)
		goto err;
	if ((ret = __get_txn_vrfy_info(lvh, argp->txnp->txnid, &ptvi)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;
	if (ret == DB_NOTFOUND && !F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL)) {
		if (!IS_ZERO_LSN(lvh->lv_config->start_lsn) &&
		    (ret2 = __txn_started(lvh, lvh->lv_config->start_lsn,
		    argp->txnp->txnid, &started)) == 0 && started != 0) {
			ret = 0;
			goto err;
		}
		if (ret2 != 0)
			ret = ret2;
		__db_errx(lvh->dbenv->env, DB_STR_A("2547",
		    "[%lu][%lu] Can not find an active transaction's "
		    "information, txnid: %lx.", "%lu %lu %lx"),
		    (u_long)lsnp->file, (u_long)lsnp->offset,
		    (u_long)argp->txnp->txnid);
		ON_ERROR(lvh, DB_LOG_VERIFY_INTERR);

	}

	if (ptvi == NULL) {
		if (ret == DB_NOTFOUND &&
		    F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
			ret = 0;
		goto out;

	}
	DB_ASSERT(env, ptvi->ptxnid == 0);

	/*
	 * This log record is only logged when committing a outermost txn,
	 * child txn commits are logged in __txn_child_log.
	 */
	if (ptvi->ptxnid == 0) {
		if (ptvi->status == TXN_STAT_PREPARE)
			lvh->ntxn_prep--;
		else if (ptvi->status == TXN_STAT_ACTIVE)
			lvh->ntxn_active--;
		lvh->ntxn_commit++;
	}
	ptvi->status = TXN_STAT_COMMIT;
	DB_ASSERT(env, IS_ZERO_LSN(ptvi->last_lsn));
	ptvi->last_lsn = *lsnp;
	if ((ret = __put_txn_vrfy_info(lvh, ptvi)) != 0)
		goto err;

	/* Report txn stats. */
	if (F_ISSET(lvh, DB_LOG_VERIFY_VERBOSE))
		__db_msg(env, DB_STR_A("2548",
		    "[%lu][%lu] The number of active, committed and aborted "
		    "child txns of txn %lx: %u, %u, %u.",
		    "%lu %lu %lx %u %u %u"), (u_long)lsnp->file,
		    (u_long)lsnp->offset, (u_long)ptvi->txnid,
		    ptvi->nchild_active, ptvi->nchild_commit,
		    ptvi->nchild_abort);
out:
err:

	if (pptvi != NULL && (ret2 = __free_txninfo(pptvi)) != 0 && ret == 0)
		ret = ret2;
	if (ptvi != NULL && (ret2 = __free_txninfo(ptvi)) != 0 && ret == 0)
		ret = ret2;
	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __txn_ckp_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_ckp_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__txn_ckp_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __txn_ckp_42_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID); */
err:

	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __txn_ckp_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_ckp_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__txn_ckp_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	VRFY_CKP_INFO *lastckp, ckpinfo;
	int ret;
	struct __ckp_verify_params cvp;
	VRFY_TIMESTAMP_INFO tsinfo;
	char timebuf[CTIME_BUFLEN];
	time_t ckp_time, lastckp_time;

	lastckp = NULL;
	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;
	memset(&ckpinfo, 0, sizeof(ckpinfo));
	memset(&cvp, 0, sizeof(cvp));

	if ((ret = __txn_ckp_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

	if (F_ISSET(lvh, DB_LOG_VERIFY_FORWARD)) {
		tsinfo.lsn = *lsnp;
		tsinfo.timestamp = argp->timestamp;
		tsinfo.logtype = argp->type;
		/*
		 * Store the first ckp_lsn, or the least one greater than the
		 * starting point. There will be no partial txns after
		 * valid_lsn.
		 */
		if (!(!IS_ZERO_LSN(lvh->lv_config->start_lsn) &&
		    LOG_COMPARE(&(lvh->lv_config->start_lsn),
		    &(argp->ckp_lsn)) > 0))
			lvh->valid_lsn = argp->ckp_lsn;
		if ((ret = __put_timestamp_info(lvh, &tsinfo)) != 0)
			goto err;
		goto out;/* We are done, exit. */
	}
	lvh->nckp++;
	ckp_time = (time_t)argp->timestamp;
	__db_msg(env, DB_STR_A("2549",
	    "[%lu][%lu] Checkpoint record, ckp_lsn: [%lu][%lu], "
	    "timestamp: %s. Total checkpoint: %u",
	    "%lu %lu %lu %lu %s %u"), (u_long)lsnp->file,
	    (u_long)lsnp->offset, (u_long)argp->ckp_lsn.file,
	    (u_long)argp->ckp_lsn.offset,
	    __os_ctime(&ckp_time, timebuf), lvh->nckp);

	if ((ret = __lv_on_timestamp(lvh, lsnp,
	    argp->timestamp, DB___txn_ckp)) != 0)
		goto err;
	if (((ret = __get_last_ckp_info(lvh, &lastckp)) != 0) &&
	    ret != DB_NOTFOUND)
		return (ret);
	if (ret == DB_NOTFOUND)
		goto cont;

	if (LOG_COMPARE(&(argp->last_ckp), &(lastckp->lsn)) != 0) {
		__db_errx(env, DB_STR_A("2550",
		    "[%lu][%lu] Last known checkpoint [%lu][%lu] not equal "
		    "to last_ckp :[%lu][%lu]. Some checkpoint log records "
		    "may be missing.", "%lu %lu %lu %lu %lu %lu"),
		    (u_long)lsnp->file, (u_long)lsnp->offset,
		    (u_long)lastckp->lsn.file, (u_long)lastckp->lsn.offset,
		    (u_long)argp->last_ckp.file, (u_long)argp->last_ckp.offset);
		ret = DB_LOG_VERIFY_BAD;
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}

	/*
	 * Checkpoint are generally not performed quite often, so we see this
	 * as an error, but in txn commits we see it as a warning.
	 */
	lastckp_time = (time_t)lastckp->timestamp;
	if (argp->timestamp < lastckp->timestamp) {
		__db_errx(env, DB_STR_A("2551",
		    "[%lu][%lu] Last known checkpoint [%lu, %lu] has a "
		    "timestamp %s smaller than this checkpoint timestamp %s.",
		    "%lu %lu %lu %lu %s %s"), (u_long)lsnp->file,
		    (u_long)lsnp->offset, (u_long)lastckp->lsn.file,
		    (u_long)lastckp->lsn.offset,
		    __os_ctime(&lastckp_time, timebuf),
		    __os_ctime(&ckp_time, timebuf));
		ret = DB_LOG_VERIFY_BAD;
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}

cont:
	cvp.env = env;
	cvp.lsn = *lsnp;
	cvp.ckp_lsn = argp->ckp_lsn;

	/*
	 * Verify that all active txn's first lsn is greater than
	 * argp->ckp_lsn.
	 */
	if ((ret = __iterate_txninfo(lvh, 0, 0,
	    __lv_ckp_vrfy_handler, &cvp)) != 0)
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	ckpinfo.timestamp = argp->timestamp;
	ckpinfo.lsn = *lsnp;
	ckpinfo.ckplsn = argp->ckp_lsn;

	if ((ret = __put_ckp_info(lvh, &ckpinfo)) != 0)
		goto err;
out:
err:
	if (argp)
		__os_free(env, argp);
	if (lastckp)
		__os_free(env, lastckp);
	return (ret);
}

static int
__lv_ckp_vrfy_handler(lvinfo, txninfop, param)
	DB_LOG_VRFY_INFO *lvinfo;
	VRFY_TXN_INFO *txninfop;
	void *param;
{
	struct __ckp_verify_params *cvp;
	int ret;

	ret = 0;
	cvp = (struct __ckp_verify_params *)param;
	/* ckp_lsn should be less than any active txn's first lsn. */
	if (txninfop->status == TXN_STAT_ACTIVE && LOG_COMPARE(&(cvp->ckp_lsn),
	    &(txninfop->first_lsn)) >= 0) {
		__db_errx(cvp->env, DB_STR_A("2552",
		    "[%lu][%lu] ckp log's ckp_lsn [%lu][%lu] greater than "
		    "active txn %lx 's first lsn [%lu][%lu]",
		     "%lu %lu %lu %lu %lx %lu %lu"),
		    (u_long)cvp->lsn.file, (u_long)cvp->lsn.offset,
		    (u_long)cvp->ckp_lsn.file, (u_long)cvp->ckp_lsn.offset,
		    (u_long)txninfop->txnid,
		    (u_long)txninfop->first_lsn.file,
		    (u_long)txninfop->first_lsn.offset);
		lvinfo->flags |= DB_LOG_VERIFY_ERR;
		if (!F_ISSET(lvinfo, DB_LOG_VERIFY_CAF))
			/* Stop the iteration. */
			ret = DB_LOG_VERIFY_BAD;
	}

	return (ret);
}

/*
 * PUBLIC: int __txn_child_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_child_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__txn_child_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	VRFY_TXN_INFO *ptvi, *ptvi2;
	int ret, ret2, started;

	/*
	 * This function is called when a txn T0's child txn T1 commits. Before
	 * this log record we don't know T0 and T1's relationship. This means
	 * we never know the T0 has an active child txn T1, all child txns
	 * we know are committed.
	 */
	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;
	ptvi = ptvi2 = NULL;
	ret = ret2 = started = 0;

	if ((ret = __txn_child_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	/*
	 * The __lv_log_fwdscr_oncmt call must precede LOG_VRFY_PROC otherwise
	 * this txn will be taken as an aborted txn.
	 */
	if (F_ISSET(lvh, DB_LOG_VERIFY_FORWARD)) {
		if ((ret = __lv_log_fwdscr_oncmt(lvh, argp->c_lsn, argp->child,
		    argp->txnp->txnid, 0)) != 0)
			goto err;
		if ((ret = __lv_log_fwdscr_onrec(lvh, argp->txnp->txnid,
		    argp->type, argp->prev_lsn, *lsnp)) != 0)
			goto err;
		goto out;/* We are done. */
	}
	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);
	if ((ret = __return_txn_pages(lvh, argp->child,
	    argp->txnp->txnid)) != 0 && ret != DB_NOTFOUND)
		goto err;/* Some txns may have updated no pages. */

	/* Update parent txn info. */
	if ((ret = __get_txn_vrfy_info(lvh, argp->txnp->txnid, &ptvi)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;
	if (ret == DB_NOTFOUND && !F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL)) {
		if (!IS_ZERO_LSN(lvh->lv_config->start_lsn) &&
		    ((ret2 = __txn_started(lvh, lvh->lv_config->start_lsn,
		    argp->txnp->txnid, &started)) == 0) && started != 0) {
			ret = 0;
			goto err;
		}
		if (ret2 != 0)
			ret = ret2;
		__db_errx(lvh->dbenv->env, DB_STR_A("2553",
		    "[%lu][%lu] Can not find an active transaction's "
		    "information, txnid: %lx.", "%lu %lu %lx"),
		    (u_long)lsnp->file, (u_long)lsnp->offset,
		    (u_long)argp->txnp->txnid);
		ON_ERROR(lvh, DB_LOG_VERIFY_INTERR);

	}
	if (ptvi == NULL) {
		if (ret == DB_NOTFOUND &&
		    F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
			ret = 0;
		goto out;

	}
	ptvi->nchild_commit++;
	/*
	 * The start of this child txn caused lvh->ntxn_active to be
	 * incremented unnecessarily, so decrement it.
	 */
	lvh->ntxn_active--;
	if (ptvi->status != TXN_STAT_ACTIVE) {
		__db_errx(lvh->dbenv->env, DB_STR_A("2554",
		    "[%lu][%lu] Parent txn %lx ended "
		    "before child txn %lx ends.", "%lu %lu %lx %lx"),
		    (u_long)lsnp->file, (u_long)lsnp->offset,
		    (u_long)argp->txnp->txnid, (u_long)argp->child);
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}
	if ((ret = __put_txn_vrfy_info(lvh, ptvi)) != 0)
		goto err;

	/* Update child txn info. */
	if ((ret = __get_txn_vrfy_info(lvh, argp->child, &ptvi2)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;
	if (ret == DB_NOTFOUND && !F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL)) {
		if (!IS_ZERO_LSN(lvh->lv_config->start_lsn) &&
		    ((ret2 = __txn_started(lvh, lvh->lv_config->start_lsn,
		    argp->child, &started)) == 0) && started != 0) {
			ret = 0;
			goto err;
		}
		if (ret2 != 0)
			ret = ret2;
		__db_errx(lvh->dbenv->env, DB_STR_A("2555",
		    "[%lu][%lu] Can not find an active "
		    "transaction's information, txnid: %lx.",
		    "%lu %lu %lx"), (u_long)lsnp->file,
		    (u_long)lsnp->offset, (u_long)argp->child);
		ON_ERROR(lvh, DB_LOG_VERIFY_INTERR);

	}
	if (ptvi2 == NULL) {
		if (ret == DB_NOTFOUND &&
		    F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
			ret = 0;
		goto out;

	}
	if (ptvi2->status != TXN_STAT_ACTIVE) {
		__db_errx(lvh->dbenv->env, DB_STR_A("2556",
		    "[%lu][%lu] Txn %lx ended before it commits.",
		    "%lu %lu %lx"), (u_long)lsnp->file,
		    (u_long)lsnp->offset, (u_long)argp->child);
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}
	ptvi2->status = TXN_STAT_COMMIT;
	if ((ret = __put_txn_vrfy_info(lvh, ptvi2)) != 0)
		goto err;
out:
err:
	__os_free(env, argp);
	if (ptvi != NULL && (ret2 = __free_txninfo(ptvi)) != 0 && ret == 0)
		ret = ret2;
	if (ptvi2 != NULL && (ret2 = __free_txninfo(ptvi2)) != 0 && ret == 0)
		ret = ret2;

	return (ret);
}

/*
 * PUBLIC: int __txn_xa_regop_42_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_xa_regop_42_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__txn_xa_regop_42_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __txn_xa_regop_42_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	ON_NOT_SUPPORTED(env, lvh, *lsnp, argp->type);
	/* LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID); */
err:
	__os_free(env, argp);

	return (ret);
}

/*
 * PUBLIC: int __txn_prepare_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_prepare_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__txn_prepare_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	VRFY_TXN_INFO *ptvi;
	int ret, ret2, started;

	ret = ret2 = started = 0;
	ptvi = NULL;
	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;

	if ((ret = __txn_prepare_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

	if ((ret = __get_txn_vrfy_info(lvh, argp->txnp->txnid, &ptvi)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;

	if (ret == DB_NOTFOUND && !F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL)) {
		if (!IS_ZERO_LSN(lvh->lv_config->start_lsn) &&
		    ((ret2 = __txn_started(lvh, lvh->lv_config->start_lsn,
		    argp->txnp->txnid, &started)) == 0) && started != 0) {
			ret = 0;
			goto err;
		}
		if (ret2 != 0)
			ret = ret2;
		__db_errx(lvh->dbenv->env, DB_STR_A("2557",
		    "[%lu][%lu] Can not find an active transaction's "
		    "information, txnid: %lx.", "%lu %lu %lx"),
		    (u_long)lsnp->file, (u_long)lsnp->offset,
		    (u_long)argp->txnp->txnid);
		ON_ERROR(lvh, DB_LOG_VERIFY_INTERR);

	}
	if (ptvi == NULL) {
		if (ret == DB_NOTFOUND &&
		    F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
			ret = 0;
		goto out;

	}
	DB_ASSERT(env,
	    (IS_ZERO_LSN(ptvi->prep_lsn) && ptvi->status != TXN_STAT_PREPARE) ||
	    (!IS_ZERO_LSN(ptvi->prep_lsn) && ptvi->status == TXN_STAT_PREPARE));

	lvh->ntxn_prep++;
	lvh->ntxn_active--;

	if (!IS_ZERO_LSN(ptvi->prep_lsn)) {/* Prepared more than once. */

		__db_errx(lvh->dbenv->env, DB_STR_A("2558",
		    "[%lu][%lu] Multiple txn_prepare log record for "
		    "transaction %lx, previous prepare lsn: [%lu, %lu].",
		    "%lu %lu %lx %lu %lu"), (u_long)lsnp->file,
		    (u_long)lsnp->offset, (u_long)argp->txnp->txnid,
		    (u_long)ptvi->prep_lsn.file, (u_long)ptvi->prep_lsn.offset);
	} else {
		ptvi->prep_lsn = *lsnp;
		ptvi->status = TXN_STAT_PREPARE;
	}
	ret = __put_txn_vrfy_info(lvh, ptvi);
out:
err:
	__os_free(env, argp);
	if (ptvi != NULL && (ret2 = __free_txninfo(ptvi)) != 0 && ret == 0)
		ret = ret2;
	return (ret);
}

/*
 * PUBLIC: int __txn_recycle_verify __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__txn_recycle_verify(env, dbtp, lsnp, notused2, lvhp)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *lvhp;
{
	__txn_recycle_args *argp;
	DB_LOG_VRFY_INFO *lvh;
	int ret;

	notused2 = DB_TXN_LOG_VERIFY;
	lvh = (DB_LOG_VRFY_INFO *)lvhp;
	ret = 0;

	if ((ret = __txn_recycle_read(env, dbtp->data, &argp)) != 0)
		return (ret);

	LOG_VRFY_PROC(lvh, *lsnp, argp, INVAL_DBREGID);

	/* Add recycle info for all txns whose ID is in the [min, max] range. */
	ret = __add_recycle_lsn_range(lvh, lsnp, argp->min, argp->max);

out:

err:

	__os_free(env, argp);
	return (ret);
}

/* Handle log types having timestamps, so far only __txn_ckp and __txn_regop. */
static int
__lv_on_timestamp(lvh, lsn, timestamp, logtype)
	DB_LOG_VRFY_INFO *lvh;
	const DB_LSN *lsn;
	int32_t timestamp;
	u_int32_t logtype;
{
	VRFY_TIMESTAMP_INFO *ltsinfo;
	int ret;

	ltsinfo = NULL;
	ret = 0;
	if ((ret = __get_latest_timestamp_info(lvh, *lsn, &ltsinfo)) == 0) {
		DB_ASSERT(lvh->dbenv->env, ltsinfo != NULL);
		if (ltsinfo->timestamp >= timestamp &&
		    F_ISSET(lvh, DB_LOG_VERIFY_VERBOSE)) {
			__db_errx(lvh->dbenv->env, DB_STR_A("2559",
			    "[%lu][%lu] [WARNING] This log record of type %s "
			    "does not have a greater time stamp than "
			    "[%lu, %lu] of type %s", "%lu %lu %s %lu %lu %s"),
			    (u_long)lsn->file, (u_long)lsn->offset,
			    LOGTYPE_NAME(lvh, logtype),
			    (u_long)ltsinfo->lsn.file,
			    (u_long)ltsinfo->lsn.offset,
			    LOGTYPE_NAME(lvh, ltsinfo->logtype));
			lvh->flags |= DB_LOG_VERIFY_WARNING;
		}
	}
	if (ltsinfo != NULL)
		__os_free(lvh->dbenv->env, ltsinfo);
	if (ret == DB_NOTFOUND)
		ret = 0;

	return (ret);
}

/*
 * Called whenever the log record belongs to a transaction.
 */
static int
__lv_on_txn_logrec(lvh, lsnp, prev_lsnp, txnp, type, dbregid)
	DB_LOG_VRFY_INFO *lvh;
	const DB_LSN *lsnp;
	const DB_LSN *prev_lsnp;
	const DB_TXN *txnp;
	u_int32_t type;
	int32_t dbregid;
{
	DBT fid;
	VRFY_TXN_INFO *pvti;
	u_int32_t txnid;
	VRFY_FILEREG_INFO *fregp;
	int ret, ret2, started;

	ret = ret2 = started = 0;
	pvti = NULL;
	fregp = NULL;
	lvh->lrtypes[type]++;/* Increment per-type log record count. */
	txnid = txnp->txnid;
	memset(&fid, 0, sizeof(fid));

	if (dbregid == INVAL_DBREGID)
		goto cont;
	if ((ret = __get_filereg_by_dbregid(lvh, dbregid, &fregp)) != 0) {
		if (ret == DB_NOTFOUND) {
			/*
			 * It's likely that we are verifying a subset of logs
			 * and the DBREG_OPEN is outside the range.
			 */
			if (!F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
				__db_msg(lvh->dbenv->env, DB_STR_A("2560",
				    "[%lu][%lu] Transaction %lx is updating a "
				    "db file %d not registered.",
				    "%lu %lu %lx %d"),
				    (u_long)lsnp->file, (u_long)lsnp->offset,
				    (u_long)txnp->txnid, dbregid);
			goto cont;
		} else
			goto err;
	}

	fid = fregp->fileid;
cont:
	if (IS_ZERO_LSN(*prev_lsnp) &&
	    (ret = __lv_on_new_txn(lvh, lsnp, txnp, type, dbregid, &fid)) != 0)
		goto err;

	if ((ret = __get_txn_vrfy_info(lvh, txnid, &pvti)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;

	/* If can't find the txn, there is an internal error. */
	if (ret == DB_NOTFOUND && !F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL)) {
		/*
		 * If verifying from middle, it's expected that txns begun
		 * before start are not found.
		 */
		if (!IS_ZERO_LSN(lvh->lv_config->start_lsn) && ((ret2 =
		    __txn_started(lvh, lvh->lv_config->start_lsn, txnid,
		    &started)) == 0) && started != 0) {
			ret = 0;
			goto out;/* We are done. */
		}
		if (ret2 != 0)
			ret = ret2;

		__db_errx(lvh->dbenv->env, DB_STR_A("2561",
		    "[%lu][%lu] Can not find an active transaction's "
		    "information, txnid: %lx.", "%lu %lu %lx"),
		    (u_long)lsnp->file, (u_long)lsnp->offset, (u_long)txnid);
		ON_ERROR(lvh, DB_LOG_VERIFY_INTERR);
	}

	/* Can't proceed without the txn info. */
	if (pvti == NULL) {
		if (ret == DB_NOTFOUND && F_ISSET(lvh, DB_LOG_VERIFY_PARTIAL))
			ret = 0;
		goto out;
	}

	/* Check if prev lsn is wrong, and some log records may be missing. */
	if (!IS_ZERO_LSN(*prev_lsnp) &&
	    LOG_COMPARE(prev_lsnp, &(pvti->cur_lsn)) != 0) {
		__db_errx(lvh->dbenv->env, DB_STR_A("2562",
		    "[%lu][%lu] Previous record for transaction %lx is "
		    "[%lu][%lu] and prev_lsn is [%lu][%lu].",
		    "%lu %lu %lx %lu %lu %lu %lu"), (u_long)lsnp->file,
		    (u_long)lsnp->offset, (u_long)pvti->txnid,
		    (u_long)pvti->cur_lsn.file, (u_long)pvti->cur_lsn.offset,
		    (u_long)prev_lsnp->file, (u_long)prev_lsnp->offset);
		ret = DB_LOG_VERIFY_BAD;
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}

	/*
	 * After the txn is prepared, the only valid log record for this txn
	 * is the commit record.
	 */
	if (pvti->status == TXN_STAT_PREPARE && type != DB___txn_regop) {
		__db_errx(lvh->dbenv->env, DB_STR_A("2563",
		    "[%lu][%lu] Update action is performed in a "
		    "prepared transaction %lx.", "%lu %lu %lx"),
		    (u_long)lsnp->file, (u_long)lsnp->offset, (u_long)txnid);
		ret = DB_LOG_VERIFY_BAD;
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}
	pvti->cur_lsn = *lsnp;
	pvti->flags = txnp->flags;
	if (dbregid != INVAL_DBREGID && fid.size > 0 &&
	    (ret = __add_file_updated(pvti, &fid, dbregid)) != 0)
		goto err;
	if ((ret = __put_txn_vrfy_info(lvh, pvti)) != 0)
		goto err;
out:
err:
	if (pvti != NULL && (ret2 = __free_txninfo(pvti)) != 0 && ret == 0)
		ret = ret2;
	if (fregp != NULL &&
	    (ret2 = __free_filereg_info(fregp)) != 0 && ret == 0)
		ret = ret2;
	return (ret);
}

/*
 * Called whenever a new transaction is started, including child transactions.
 */
static int
__lv_on_new_txn (lvh, lsnp, txnp, type, dbregid, fid)
	DB_LOG_VRFY_INFO *lvh;
	const DB_LSN *lsnp;
	const DB_TXN *txnp;
	u_int32_t type;
	int32_t dbregid;
	const DBT *fid;
{
	VRFY_TXN_INFO vti, *pvti, *vtip;
	int ret, tret;
	u_int32_t txnid;
	ENV *env;

	ret = tret = 0;
	txnid = txnp->txnid;
	pvti = NULL;
	memset(&vti, 0, sizeof(vti));
	vti.txnid = txnid;
	env = lvh->dbenv->env;
	/* Log record type, may be used later. Pass lint checks. */
	COMPQUIET(type, 0);

	/*
	 * It's possible that the new txn is a child txn, we will decrement
	 * this value in __txn_child_verify when we realize this, because
	 * this value only records the number of outermost active txns.
	 */
	lvh->ntxn_active++;

	if ((ret = __get_txn_vrfy_info(lvh, txnid, &pvti)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;
	if (ret == DB_NOTFOUND)
		vtip = &vti;
	else {/* The txnid is reused, may be illegal. */
		vtip = pvti;
		/*
		 * If this txn id was recycled, this use is legal. A legal
		 * recyclable txnid is immediately not recyclable after
		 * it's recycled here. And it's impossible for vtip->status
		 * to be TXN_STAT_ACTIVE, since we have made it TXN_STAT_ABORT
		 * when we detected this txn id recycle just now.
		 */
		if (vtip->num_recycle > 0 && LOG_COMPARE(&(vtip->recycle_lsns
		    [vtip->num_recycle - 1]), lsnp) < 0) {
			DB_ASSERT(env, vtip->status != TXN_STAT_ACTIVE);
			if ((ret = __rem_last_recycle_lsn(vtip)) != 0)
				goto err;
			if ((ret = __clear_fileups(vtip)) != 0)
				goto err;

			vtip->status = 0;
			ZERO_LSN(vtip->prep_lsn);
			ZERO_LSN(vtip->last_lsn);

			vtip->nchild_active = 0;
			vtip->nchild_commit = 0;
			vtip->nchild_abort = 0;
		/*
		 * We may goto the else branch if this txn has child txns
		 * before any updates done on its behalf. So we should
		 * exclude this possibility to conclude a failed verification.
		 */
		} else if (vtip->nchild_active + vtip->nchild_commit +
		    vtip->nchild_abort == 0) {
			__db_errx(lvh->dbenv->env, DB_STR_A("2564",
			    "[%lu][%lu] Transaction id %lx reused without "
			    "being recycled with a __txn_recycle.",
			    "%lu %lu %lx"),
			    (u_long)lsnp->file, (u_long)lsnp->offset,
			    (u_long)txnid);
			ret = DB_LOG_VERIFY_BAD;
			ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
		}
	}

	vtip->first_lsn = *lsnp;
	vtip->cur_lsn = *lsnp;
	vtip->flags = txnp->flags;

	/*
	 * It's possible that the first log rec does not update any file,
	 * like the __txn_child type of record.
	 */
	if (fid->size > 0 && (ret =
	    __add_file_updated(vtip, fid, dbregid)) != 0)
		goto err;
	if ((ret = __put_txn_vrfy_info(lvh, vtip)) != 0)
		goto err;

err:
	if (pvti != NULL && (tret = __free_txninfo(pvti)) != 0 && ret == 0)
		ret = tret;
	if ((tret = __free_txninfo_stack(&vti)) != 0 && ret == 0)
		ret = tret;

	return (ret);
}

/* Called when we detect that a new log file is used. */
static int
__lv_new_logfile_vrfy(lvh, lsnp)
	DB_LOG_VRFY_INFO *lvh;
	const DB_LSN *lsnp;
{
	int ret;

	ret = 0;
	if (IS_ZERO_LSN(lvh->last_lsn) || lvh->last_lsn.file == lsnp->file) {
		lvh->last_lsn = *lsnp;
		return (0);
	}

	/*
	 * If file number changed, it must have been incremented,
	 * and the offset is 0.
	 * */
	if (lsnp->file - lvh->last_lsn.file != 1 || lsnp->offset !=
	    __lv_first_offset(lvh->dbenv->env)) {
		__db_errx(lvh->dbenv->env,
		    "[%lu][%lu] Last log record verified ([%lu][%lu]) is not "
		    "immidiately before the current log record.",
		    (u_long)lsnp->file, (u_long)lsnp->offset,
		    (u_long)lvh->last_lsn.file, (u_long)lvh->last_lsn.offset);
		ret = DB_LOG_VERIFY_BAD;
		ON_ERROR(lvh, DB_LOG_VERIFY_ERR);
	}

	lvh->last_lsn = *lsnp;
err:
	return (ret);
}

static u_int32_t
__lv_first_offset(env)
	ENV *env;
{
	u_int32_t sz;

	if (CRYPTO_ON(env))
		sz = HDR_CRYPTO_SZ;
	else
		sz = HDR_NORMAL_SZ;

	sz += sizeof(LOGP);

	return sz;
}

/* Called when we see a non-transactional update log record. */
static int
__lv_on_nontxn_update(lvh, lsnp, txnid, logtype, fileid)
	DB_LOG_VRFY_INFO *lvh;
	const DB_LSN *lsnp;
	u_int32_t txnid, logtype;
	int32_t fileid;
{
	lvh->lrtypes[logtype]++;
	COMPQUIET(txnid, 0);
	if (fileid != INVAL_DBREGID) {
		lvh->non_txnup_cnt++;
		__db_msg(lvh->dbenv->env, DB_STR_A("2565",
		    "[%lu][%lu] Non-transactional update, "
		    "log type: %u, fileid: %d.", "%lu %lu %u %d"),
		    (u_long)lsnp->file, (u_long)lsnp->offset, logtype, fileid);
	}

	return (0);
}

static int
__lv_on_txn_aborted(lvinfo)
	DB_LOG_VRFY_INFO *lvinfo;
{
	int ret, ret2, sres;
	VRFY_TXN_INFO *ptvi;
	u_int32_t abtid;
	DB_LSN lsn, slsn;

	ret = ret2 = sres = 0;
	abtid = lvinfo->aborted_txnid;
	lsn = lvinfo->aborted_txnlsn;
	slsn = lvinfo->lv_config->start_lsn;
	ptvi = NULL;

	if ((ret = __del_txn_pages(lvinfo, lvinfo->aborted_txnid)) != 0 &&
	    ret != DB_NOTFOUND)
		goto err;/* Some txns may have updated no pages. */
	ret = __get_txn_vrfy_info(lvinfo, lvinfo->aborted_txnid, &ptvi);
	if (ret == DB_NOTFOUND && !F_ISSET(lvinfo, DB_LOG_VERIFY_PARTIAL)) {
		/*
		 * If verifying from slsn and the txn abtid started before
		 * slsn, it's expected that we can't find the txn.
		 */
		if (!IS_ZERO_LSN(slsn) && (ret2 = __txn_started(lvinfo, slsn,
		    abtid, &sres)) == 0 && sres != 0) {
			ret = 0;
			goto err;
		}
		if (ret2 != 0)
			ret = ret2;/* Use the same error msg below. */
		__db_errx(lvinfo->dbenv->env, DB_STR_A("2566",
		    "[%lu][%lu] Can not find an active transaction's "
		    "information, txnid: %lx.", "%lu %lu %lx"),
		    (u_long)lsn.file, (u_long)lsn.offset,
		    (u_long)lvinfo->aborted_txnid);
		ON_ERROR(lvinfo, DB_LOG_VERIFY_INTERR);
	}
	if (ptvi == NULL) {
		if (ret == DB_NOTFOUND &&
		    F_ISSET(lvinfo, DB_LOG_VERIFY_PARTIAL))
			ret = 0;
		goto out;
	}
	ptvi->status = TXN_STAT_ABORT;
	lvinfo->ntxn_abort++;
	lvinfo->ntxn_active--;
	/* Report txn stats. */
	if (F_ISSET(lvinfo, DB_LOG_VERIFY_VERBOSE)) {
		__db_msg(lvinfo->dbenv->env, DB_STR_A("2567",
		    "[%lu][%lu] Txn %lx aborted after this log record.",
		    "%lu %lu %lx"), (u_long)lvinfo->aborted_txnlsn.file,
		    (u_long)lvinfo->aborted_txnlsn.offset, (u_long)ptvi->txnid);
		__db_msg(lvinfo->dbenv->env, DB_STR_A("2568",
		    "\tThe number of active, committed and aborted child txns "
		    "of txn %lx: %u, %u, %u.", "%lx %u %u %u"),
		    (u_long)ptvi->txnid, ptvi->nchild_active,
		    ptvi->nchild_commit, ptvi->nchild_abort);
	}
	lvinfo->aborted_txnid = 0;
	lvinfo->aborted_txnlsn.file = lvinfo->aborted_txnlsn.offset = 0;
	if ((ret = __put_txn_vrfy_info(lvinfo, ptvi)) != 0)
		goto err;
	if ((ret = __free_txninfo(ptvi)) != 0)
		goto err;
out:
err:
	return (ret);
}
