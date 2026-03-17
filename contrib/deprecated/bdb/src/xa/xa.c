/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1998, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/txn.h"
#include "dbinc_auto/xa_ext.h"

static void corrupted_env __P((ENV *, int));

static int __xa_get_txn __P((ENV *,
    XID *, TXN_DETAIL *, DB_TXN **, u_long, int));
static void __xa_put_txn __P((ENV *, DB_TXN *));

static int __xa_txn_get_prepared
	       __P((ENV *, XID *, DB_PREPLIST *, long, long *, u_int32_t));
static int __xa_thread_enter __P((ENV *, DB_THREAD_INFO **));

static int __db_xa_close __P((char *, int, long));
static int __db_xa_commit __P((XID *, int, long));
static int __db_xa_complete __P((int *, int *, int, long));
static int __db_xa_end __P((XID *, int, long));
static int __db_xa_forget __P((XID *, int, long));
static int __db_xa_open __P((char *, int, long));
static int __db_xa_prepare __P((XID *, int, long));
static int __db_xa_recover __P((XID *, long, int, long));
static int __db_xa_rollback __P((XID *, int, long));
static int __db_xa_start __P((XID *, int, long));

/*
 * Possible flag values:
 *	Dynamic registration	0 => no dynamic registration
 *				TMREGISTER => dynamic registration
 *	Asynchronous operation	0 => no support for asynchrony
 *				TMUSEASYNC => async support
 *	Migration support	0 => migration of transactions across
 *				     threads is possible
 *				TMNOMIGRATE => no migration across threads
 */
const struct xa_switch_t db_xa_switch = {
	 "Berkeley DB",		/* name[RMNAMESZ] */
	 TMNOMIGRATE,		/* flags */
	 0,			/* version */
	 __db_xa_open,		/* xa_open_entry */
	 __db_xa_close,		/* xa_close_entry */
	 __db_xa_start,		/* xa_start_entry */
	 __db_xa_end,		/* xa_end_entry */
	 __db_xa_rollback,	/* xa_rollback_entry */
	 __db_xa_prepare,	/* xa_prepare_entry */
	 __db_xa_commit,	/* xa_commit_entry */
	 __db_xa_recover,	/* xa_recover_entry */
	 __db_xa_forget,	/* xa_forget_entry */
	 __db_xa_complete	/* xa_complete_entry */
};

/*
 * __xa_get_txn --
 *	Return a pointer to the current transaction structure for the
 * designated environment.  We take the XA flags so we can specifically
 * test for TMJOIN and TMRESUME.  These are testing for compliance with
 * the XA state machine. The various cases are:
 *
 * TMRESUME: DB_TXN should already exist for this thread and should be
 *	in state SUSPENDED.  Either error or change state.
 * TMJOIN: DB_TXN should *not* exist, but TXN_DETAIL should -- create
 *	the DB_TXN and __txn_continue it.
 * neither: Neither DB_TXN nor TXN_DETAIL should exist (td should be NULL) --
 *	start transaction.
 *
 * In addition, we use this to retrieve the current txn during __db_xa_end.
 * In this case, the td and the txn should exist and the txn should currently
 * be associated.
 *
 */
static int
__xa_get_txn(env, xid, td, txnp, flags, ending)
	ENV *env;
	XID *xid;
	TXN_DETAIL *td;
	DB_TXN **txnp;
	u_long flags;
	int ending;
{
	DB_ENV *dbenv;
	DB_THREAD_INFO *ip;
	int ret;

	dbenv = env->dbenv;
	COMPQUIET(ip, NULL);
	ENV_ENTER_RET(env, ip, ret);
	if (ret != 0)
		return (XAER_RMFAIL);
	else
		ret = XA_OK;
	DB_ASSERT(env, ip != NULL);
	if (ending != 0)
		DB_ASSERT(env,
		    ip->dbth_xa_status == TXN_XA_THREAD_ASSOCIATED);
	else
		DB_ASSERT(env,
		    ip->dbth_xa_status != TXN_XA_THREAD_ASSOCIATED);

	/*
	 * Two cases: the transaction should already exist in this
	 * environment or it should not.  If it should exist, then
	 * we should have found its detail and the JOIN or RESUME
	 * flags should have been set.
	 */
	if (td == NULL) {
		DB_ASSERT(env, ending == 0);
		if (LF_ISSET(TMJOIN | TMRESUME))
			ret = XAER_NOTA;
		/*
		 * The snapshot flag is ignored if the database is not
		 * enabled for MVCC.  This allows MVCC to be used
		 * with XA transactions.
		 */
		else if ((ret = __txn_begin(env,
		    ip, NULL, txnp, DB_TXN_NOWAIT|DB_TXN_SNAPSHOT)) != 0) {
			dbenv->err(dbenv, ret, DB_STR("4540",
			    "xa_get_txn: transaction begin failed"));
			ret = XAER_RMERR;
		} else {
			SH_TAILQ_INSERT_HEAD(&ip->dbth_xatxn,
			    *txnp, xa_links, __db_txn);
			(*txnp)->xa_thr_status = TXN_XA_THREAD_ASSOCIATED;
			ip->dbth_xa_status = TXN_XA_THREAD_ASSOCIATED;

			/* Initialize XA fields in the detail structure. */
			/* XXX Does this need protection of the TXN lock? */
			td = (TXN_DETAIL *)((*txnp)->td);
			memcpy(td->gid, xid->data, XIDDATASIZE);
			td->bqual = (u_int32_t)xid->bqual_length;
			td->gtrid = (u_int32_t)xid->gtrid_length;
			td->format = (int32_t)xid->formatID;
			td->xa_br_status = TXN_XA_ACTIVE;
		}
	} else {
		/* If we get here, the transaction exists. */
		if (ending == 0 && !LF_ISSET(TMRESUME) && !LF_ISSET(TMJOIN)) {
			ret = XAER_DUPID;
			goto out;
		}

		SH_TAILQ_FOREACH(*txnp, &ip->dbth_xatxn, xa_links, __db_txn)
			if ((*txnp)->td == td)
				break;

		/* Check that we are not a child transaction. */
		if (td->parent != INVALID_ROFF) {
			dbenv->err(dbenv, EINVAL, DB_STR("4541",
			    "xa_get_txn: XA transaction with parent"));
			ret = XAER_RMERR;
			goto out;
		}

		if (*txnp != NULL) {
			if (ending) {
				DB_ASSERT(env, (*txnp)->xa_thr_status ==
				    TXN_XA_THREAD_ASSOCIATED);
				DB_ASSERT(env, (*txnp) ==
				    SH_TAILQ_FIRST(&ip->dbth_xatxn, __db_txn));
			} else if (LF_ISSET(TMRESUME)) {
				DB_ASSERT(env, (*txnp)->xa_thr_status ==
				    TXN_XA_THREAD_SUSPENDED);
				DB_ASSERT(env, ip->dbth_xa_status ==
				    TXN_XA_THREAD_SUSPENDED);
				(*txnp)->xa_thr_status =
				    TXN_XA_THREAD_ASSOCIATED;
				ip->dbth_xa_status = TXN_XA_THREAD_ASSOCIATED;
				if ((*txnp) !=
				    SH_TAILQ_FIRST(&ip->dbth_xatxn, __db_txn)) {
					SH_TAILQ_REMOVE(&ip->dbth_xatxn,
					    (*txnp), xa_links, __db_txn);
					SH_TAILQ_INSERT_HEAD(&ip->dbth_xatxn,
					    (*txnp), xa_links, __db_txn);
				}
				if (td->xa_br_status == TXN_XA_IDLE)
					td->xa_br_status = TXN_XA_ACTIVE;
			} else
				ret = XAER_PROTO;
		} else {
			if (LF_ISSET(TMRESUME)) {
				dbenv->err(dbenv, EINVAL, DB_STR("4542",
				    "xa_get_txn: transaction does not exist"));
				ret = XAER_PROTO;
			} else if ((ret =
			    __os_malloc(env, sizeof(DB_TXN), txnp)) == 0) {
				/* We are joining this branch. */
				ret = __txn_continue(env, *txnp, td, ip, 1);
				if (ret != 0) {
					dbenv->err(dbenv, ret, DB_STR("4543",
					    "xa_get_txn: txn_continue fails"));
					ret = XAER_RMFAIL;
				}
				ip->dbth_xa_status = TXN_XA_THREAD_ASSOCIATED;
				(*txnp)->xa_thr_status =
				    TXN_XA_THREAD_ASSOCIATED;
				SH_TAILQ_INSERT_HEAD(&ip->dbth_xatxn,
				    (*txnp), xa_links, __db_txn);
				if (td->xa_br_status == TXN_XA_IDLE)
					td->xa_br_status = TXN_XA_ACTIVE;
			} else {
				dbenv->err(dbenv, ret, DB_STR("4544",
				    "xa_get_txn: os_malloc failed"));
				ret = XAER_RMERR;
			}
		}
	}
out:	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * Release use of this transaction.
 */
static void
__xa_put_txn(env, txnp)
	ENV *env;
	DB_TXN *txnp;
{
	DB_THREAD_INFO *ip;
	TXN_DETAIL *td;

	ip = txnp->thread_info;
	DB_ASSERT(env, ip != NULL);
	SH_TAILQ_REMOVE(&ip->dbth_xatxn, txnp, xa_links, __db_txn);
	TAILQ_REMOVE(&txnp->mgrp->txn_chain, txnp, links);
	td = txnp->td;
	DB_ASSERT(env, td->xa_ref > 0);
	td->xa_ref--;
	__os_free(env, txnp);
	ip->dbth_xa_status = TXN_XA_THREAD_UNASSOCIATED;
}

static
int __xa_thread_enter(env, ipp)
	ENV *env;
	DB_THREAD_INFO **ipp;
{
	int ret;
	DB_THREAD_INFO *ip;

	COMPQUIET(ip, NULL);
	ENV_ENTER_RET(env, ip, ret);
	if (ret == 0)
		ip->dbth_xa_status = TXN_XA_THREAD_UNASSOCIATED;
	*ipp = ip;
	return (ret);
}

/*
 * __xa_txn_get_prepared --
 *	Wrap the internal call to __txn_get_prepared so that we can call
 * it from XA. XA routines are not considered to be running "inside" the
 * library, so when they make calls into the library, we need to use interface
 * routines that support replication and failchk.  Since __txn_get_prepared
 * is internal, there is no user API to call, so we use this wrapper routine
 * instead.
 */
static int
__xa_txn_get_prepared(env, xids, txns, count, retp, flags)
	ENV *env;
	XID *xids;
	DB_PREPLIST *txns;
	long count;		/* This is long for XA compatibility. */
	long *retp;
	u_int32_t flags;
{
	DB_THREAD_INFO *ip;
	int ret;

	ip = NULL;
	ENV_ENTER(env, ip);
	REPLICATION_WRAP(env,
	    (__txn_get_prepared(env, xids, txns, count, retp, flags)), 0, ret);
	ENV_LEAVE(env, ip);
	return (ret);
}

#define	XA_FLAGS \
	(DB_CREATE | DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | \
	DB_INIT_TXN | DB_THREAD | DB_REGISTER | DB_RECOVER)

/*
 * __db_xa_open --
 *	The open call in the XA protocol.  The rmid field is an id number
 * that the TM assigned us and will pass us on every xa call.  We need to
 * map that rmid number into a env structure that we create during
 * initialization.  The file xa_map.c implements all such xa->db mappings.
 *	The xa_info field is instance specific information.  We require
 * that the value of DB_HOME be passed in xa_info.  Since xa_info is the
 * only thing that we get to pass to db_env_create, any config information
 * will have to be done via a config file instead of via the db_env_create
 * call.
 */
static int
__db_xa_open(xa_info, rmid, arg_flags)
	char *xa_info;
	int rmid;
	long arg_flags;
{
	DB_ENV *dbenv;
	DB_THREAD_INFO *ip;
	ENV *env;
	int inmem, ret;
	u_long flags;

	flags = (u_long)arg_flags;	/* Conversion for bit operations. */
	ret = 0;

	if (LF_ISSET(TMASYNC))
		return (XAER_ASYNC);
	if (flags != TMNOFLAGS)
		return (XAER_INVAL);

	/* Verify if we already have this environment open. */
	if (__db_rmid_to_env(rmid, &env) == 0) {
		env->xa_ref++;
		/* Indicate that this thread is in an XA environment. */
		if ((ret = __xa_thread_enter(env, &ip)) == 0) {
			DB_ASSERT(env, ip != NULL);
			ENV_LEAVE(env, ip);
			return (XA_OK);
		} else
			return (XAER_RMERR);
	}

	/* Open a new environment. */
	if ((ret = db_env_create(&dbenv, 0)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4545",
		    "xa_open: Failure creating env handle"));
		return (XAER_RMERR);
	}
	if ((ret = dbenv->set_thread_count(dbenv, 25)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4546",
		    "xa_open: Failure setting thread count"));
		goto err;
	}
	env = dbenv->env;
	if ((ret = dbenv->open(dbenv, xa_info, XA_FLAGS, 0)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4547",
		    "xa_open: Failure opening environment"));
		goto err;
	}

	/*
	 * Make sure that the environment is not configured for in-memory
	 * logging.
	 */
	if ((ret = dbenv->log_get_config(dbenv,
	    DB_LOG_IN_MEMORY, &inmem)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4548",
		    "xa_open: Failure getting log configuration"));
		goto err;
	}
	if (inmem != 0) {
		dbenv->err(dbenv, EINVAL, DB_STR("4549",
	    "xa_open: In-memory logging not allowed in XA environment"));
		goto err;
	}

	/* Create the mapping. */
	__db_map_rmid(rmid, env);
	env->xa_ref = 1;

	/* Indicate that this thread is in an XA environment. */
	if ((ret = __xa_thread_enter(env, &ip)) == 0) {
		ENV_LEAVE(env, ip);
		return (XA_OK);
	} else
		return (XAER_RMERR);

err:	(void)dbenv->close(dbenv, 0);
	/*
	 * If the environment is corrupt, then we need to get all threads
	 * and processes out of it and run recovery.  There is no particularly
	 * clean way to do that, so we'll use a really big hammer and
	 * crash the server.
	 */
	if (ret == DB_RUNRECOVERY)
		exit(1);

	return (XAER_RMERR);
}

/*
 * __db_xa_close --
 *	The close call of the XA protocol.  The only trickiness here
 * is that if there are any active transactions, we must fail.  It is
 * *not* an error to call close on an environment that has already been
 * closed (I am interpreting that to mean it's OK to call close on an
 * environment that has never been opened).
 */
static int
__db_xa_close(xa_info, rmid, arg_flags)
	char *xa_info;
	int rmid;
	long arg_flags;
{
	DB_THREAD_INFO *ip;
	ENV *env;
	int ret, t_ret;
	u_long flags;

	COMPQUIET(xa_info, NULL);
	COMPQUIET(ip, NULL);
	ret = 0;

	flags = (u_long)arg_flags;	/* Conversion for bit operations. */

	if (LF_ISSET(TMASYNC))
		return (XAER_ASYNC);
	if (flags != TMNOFLAGS)
		return (XAER_INVAL);

	/* If the environment is closed, then we're done. */
	if (__db_rmid_to_env(rmid, &env) != 0)
		return (XA_OK);

	/* Check if there are any pending transactions. */
	ENV_ENTER_RET(env, ip, ret);
	/*
	 * If the environment is corrupt, then we need to get all threads
	 * and processes out of it and run recovery.  There is no particularly
	 * clean way to do that, so we'll use a really big hammer and
	 * crash the server.
	 */
	if (ret == DB_RUNRECOVERY)
		exit(1);
	else if (ret != 0)
		return (XAER_RMFAIL);

	/*
	 * If we are calling close without ever having called open, then we
	 * don't want to do anything, because if we do, our ref counts would
	 * be all wrong.
	 */
	if (ip->dbth_xa_status == TXN_XA_THREAD_NOTA) {
		ret = XAER_PROTO;
		goto err;
	}

	/*
	 * It is an error for a transaction manager to call xa_close from
	 * a thread of control that is associated with a transaction branch.
	 */
	if (SH_TAILQ_FIRST(&ip->dbth_xatxn, __db_txn) != NULL) {
		ret = XAER_PROTO;
		goto err;
	}

	if (env->xa_ref > 1) {
		env->xa_ref--;
		goto err;
	} else {
		/* Destroy the mapping. */
		ret = __db_unmap_rmid(rmid);

		/* Close the environment. */
		t_ret = env->dbenv->close(env->dbenv, 0);

		if (ret != 0 || t_ret != 0)
			ret = XAER_RMERR;
		/* Don't try to leave an environment we just closed. */
		goto out;
	}

err:	ENV_LEAVE(env, ip);
out:	return (ret == 0 ? XA_OK : ret);
}

/*
 * __db_xa_start --
 *	Begin a transaction for the current resource manager.
 */
static int
__db_xa_start(xid, rmid, arg_flags)
	XID *xid;
	int rmid;
	long arg_flags;
{
	DB_ENV *dbenv;
	DB_TXN *txnp;
	ENV *env;
	TXN_DETAIL *td;
	int ret;
	u_long flags;

	flags = (u_long)arg_flags;	/* Conversion for bit operations. */
	ret = 0;

#define	OK_FLAGS	(TMJOIN | TMRESUME | TMNOWAIT | TMASYNC | TMNOFLAGS)
	if (LF_ISSET(~OK_FLAGS))
		return (XAER_INVAL);

	if (LF_ISSET(TMJOIN) && LF_ISSET(TMRESUME))
		return (XAER_INVAL);

	if (LF_ISSET(TMASYNC))
		return (XAER_ASYNC);

	if (__db_rmid_to_env(rmid, &env) != 0)
		return (XAER_PROTO);
	dbenv = env->dbenv;

	/* Die if the environment is corrupted. */
	PANIC_CHECK_RET(env, ret);
	if (ret == DB_RUNRECOVERY)
		exit(1);

	/*
	 * If td comes back NULL, then we know that we don't have a
	 * transaction yet.
	 */
	if ((ret = __db_xid_to_txn(env, xid, &td)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4550",
		    "xa_start: failure mapping xid"));
		return (XAER_RMFAIL);
	}

	/*
	 * This can't block, so we can ignore TMNOWAIT.
	 *
	 * Other error conditions: RMERR, OUTSIDE, PROTO, RB*
	 */
	if (td != NULL) {
		if (td->xa_br_status == TXN_XA_DEADLOCKED)
			return (XA_RBDEADLOCK);
		if (td->xa_br_status == TXN_XA_ROLLEDBACK)
			return (XA_RBOTHER);
	}
	if ((ret = __xa_get_txn(env, xid, td, &txnp, flags, 0)) != 0)
		return (ret);

	return (XA_OK);
}

/*
 * __db_xa_end --
 *	Disassociate the current transaction from the current process.
 */
static int
__db_xa_end(xid, rmid, arg_flags)
	XID *xid;
	int rmid;
	long arg_flags;
{
	DB_ENV *dbenv;
	DB_TXN *txn;
	ENV *env;
	TXN_DETAIL *td;
	int ret;
	u_long flags;

	flags = (u_long)arg_flags;	/* Convert for bit manipulation. */
	if (flags != TMNOFLAGS && !LF_ISSET(TMSUSPEND | TMSUCCESS | TMFAIL))
		return (XAER_INVAL);

	if (__db_rmid_to_env(rmid, &env) != 0)
		return (XAER_PROTO);
	dbenv = env->dbenv;

	if ((ret = __db_xid_to_txn(env, xid, &td)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4551",
		    "xa_end: failure mapping xid"));
		return (XAER_RMFAIL);
	}
	if (td == NULL)
		return (XAER_NOTA);

	if ((ret = __xa_get_txn(env, xid, td, &txn, flags, 1)) != 0)
		return (ret);

	/* We are ending; make sure there are no open cursors. */
	if (txn->cursors != 0) {
		dbenv->err(dbenv, EINVAL, DB_STR("4552",
		    "xa_end: cannot end with open cursors"));
		return (XAER_RMERR);
	}

	if (td != txn->td) {
		dbenv->err(dbenv, ret, DB_STR("4553",
		    "xa_end: txn_detail mismatch"));
		return (XAER_RMERR);
	}

	if (td->xa_br_status == TXN_XA_DEADLOCKED)
		return (XA_RBDEADLOCK);

	/*
	 * This happens if this process timed out,
	 * and the TMS called __db_xa_rollback
	 * while this process was holding the txn.
	 * Need to handle the txn in this process.
	 */
	if (td->status == TXN_NEED_ABORT) {
		if (txn->abort(txn) != 0)
			return (XAER_RMERR);
		__xa_put_txn(env, txn);
		return (XA_RBOTHER);
	}

	if (td->xa_br_status == TXN_XA_IDLE) {
		dbenv->err(dbenv, EINVAL, DB_STR("4554",
		    "xa_end: ending transaction that is idle"));
		return (XAER_PROTO);
	}

	/*
	 * If we are deadlocked or prepared, don't change this, but
	 * if we are active and the only handle, then make this transaction
	 * idle.
	 */
	if (td->xa_ref == 1 && td->xa_br_status == TXN_XA_ACTIVE)
		td->xa_br_status = TXN_XA_IDLE;
	if (LF_ISSET(TMSUSPEND)) {
		txn->thread_info->dbth_xa_status = TXN_XA_THREAD_SUSPENDED;
		txn->xa_thr_status = TXN_XA_THREAD_SUSPENDED;
	} else {
		__xa_put_txn(env, txn);
	}
	return (XA_OK);
}

/*
 * If, during a transaction completion operation (commit, abort, prepare)
 * we detect a corrupt environment, we must close and reopen the
 * environment and check if the transaction in question exists.  If it
 * does, then we can complete the operation as requested.  If it does
 * not, then we have to return aborted, because we just recovered the
 * environment, aborting this transaction.
 */
static void
corrupted_env(env, rmid)
	ENV *env;
	int rmid;
{
	DB_ENV *dbenv;
	const char *path;
	char *home;
	int ret;
	ENV *env2;

	COMPQUIET(home, NULL);
	ret = 0;
	dbenv = env->dbenv;
	path = NULL;
	if (dbenv->get_home(dbenv, &path) != 0)
		goto err;
	if (path != NULL && (__os_strdup(NULL, path, &home) != 0))
		goto err;
	/*
	 * Check that no one else came in and cleaned
	 * up the environment before we could.  If they
	 * did then just call __db_xa_open to get the
	 * new environment.  If they have not then
	 * unmap the old handle so no one else can get
	 * it.
	 */
	if (__db_rmid_to_env(rmid, &env2) == 0) {
		PANIC_CHECK_RET(env2, ret);
		if (ret != 0)
			(void)__db_unmap_rmid(rmid);
	}

	/*
	 * If we cannot get the environment then it is
	 * corrupted and are currently unable to run recovery.
	 * In that case all we can do is crash and restart,
	 * and recovery will clean up the lost transaction.
	 */
	if ( __db_xa_open(home, rmid, 0) != XA_OK)
		goto err;

	__os_free(NULL, home);
	if (0) {
err:		exit(1);
	}
}

/*
 * __db_xa_prepare --
 *	Sync the log to disk so we can guarantee recoverability.
 */
static int
__db_xa_prepare(xid, rmid, arg_flags)
	XID *xid;
	int rmid;
	long arg_flags;
{
	DB_ENV *dbenv;
	DB_TXN *txnp;
	ENV *env;
	TXN_DETAIL *td;
	int ret;
	u_long flags;

	flags = (u_long)arg_flags;	/* Conversion for bit operations. */
	ret = 0;

	if (LF_ISSET(TMASYNC))
		return (XAER_ASYNC);
	if (flags != TMNOFLAGS)
		return (XAER_INVAL);

	/*
	 * We need to know if we've ever called prepare on this.
	 * As part of the prepare, we set the xa_status field to
	 * reflect that fact that prepare has been called, and if
	 * it's ever called again, it's an error.
	 */
	if (__db_rmid_to_env(rmid, &env) != 0)
		return (XAER_PROTO);
	dbenv = env->dbenv;

	/*
	 * If the environment is corrupted, reopen it or die if that
	 * is not possible.
	 */
	PANIC_CHECK_RET(env, ret);
	if (ret == DB_RUNRECOVERY) {
		corrupted_env(env, rmid);
		if (__db_rmid_to_env(rmid, &env) != 0)
			return (XAER_PROTO);
		dbenv = env->dbenv;
	}

	if ((ret = __db_xid_to_txn(env, xid, &td)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4555",
		    "xa_prepare: failure mapping xid"));
		return (XAER_RMFAIL);
	}
	if (td == NULL) {
		dbenv->err(dbenv, EINVAL, DB_STR("4556",
		    "xa_prepare: xid not found"));
		return (XAER_NOTA);
	}

	if (td->xa_br_status == TXN_XA_DEADLOCKED)
		return (XA_RBDEADLOCK);
	if (td->xa_br_status == TXN_XA_ROLLEDBACK)
		return (XA_RBOTHER);

	if (td->xa_br_status != TXN_XA_ACTIVE &&
	    td->xa_br_status != TXN_XA_IDLE) {
		dbenv->err(dbenv, EINVAL, DB_STR("4557",
		    "xa_prepare: transaction neither active nor idle"));
		return (XAER_PROTO);
	}

	/* Now, fill in the global transaction structure. */
	if ((ret = __xa_get_txn(env, xid, td, &txnp, TMJOIN, 0)) != 0)
		return (ret);

	if ((ret = txnp->prepare(txnp, (u_int8_t *)xid->data)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4558",
		    "xa_prepare: txnp->prepare failed"));
		td->xa_br_status = TXN_XA_IDLE;
		return (XAER_RMERR);
	}
	td->xa_br_status = TXN_XA_PREPARED;

	__xa_put_txn(env, txnp);
	return (XA_OK);
}

/*
 * __db_xa_commit --
 *	Commit the transaction
 */
static int
__db_xa_commit(xid, rmid, arg_flags)
	XID *xid;
	int rmid;
	long arg_flags;
{
	DB_ENV *dbenv;
	DB_TXN *txnp;
	ENV *env;
	TXN_DETAIL *td;
	int ret;
	u_long flags;

	flags = (u_long)arg_flags;	/* Conversion for bit operations. */
	ret = 0;

	if (LF_ISSET(TMASYNC))
		return (XAER_ASYNC);
#undef	OK_FLAGS
#define	OK_FLAGS	(TMNOFLAGS | TMNOWAIT | TMONEPHASE)
	if (LF_ISSET(~OK_FLAGS))
		return (XAER_INVAL);

	/*
	 * We need to know if we've ever called prepare on this.
	 * We can verify this by examining the xa_status field.
	 */
	if (__db_rmid_to_env(rmid, &env) != 0)
		return (XAER_PROTO);
	dbenv = env->dbenv;

	/*
	 * If the environment is corrupted, reopen it or die if that
	 * is not possible.
	 */
	PANIC_CHECK_RET(env, ret);
	if (ret == DB_RUNRECOVERY) {
		corrupted_env(env, rmid);
		if (__db_rmid_to_env(rmid, &env) != 0)
			return (XAER_PROTO);
		dbenv = env->dbenv;
	}

	if ((ret = __db_xid_to_txn(env, xid, &td)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4559",
		    "xa_commit: failure mapping xid"));
		return (XAER_RMFAIL);
	}
	if (td == NULL) {
		dbenv->err(dbenv, EINVAL, DB_STR("4560",
		    "xa_commit: xid not found"));
		return (XAER_NOTA);
	}

	if (td->xa_br_status == TXN_XA_DEADLOCKED)
		return (XA_RBDEADLOCK);

	if (td->xa_br_status == TXN_XA_ROLLEDBACK)
		return (XA_RBOTHER);

	if (LF_ISSET(TMONEPHASE) && td->xa_br_status != TXN_XA_IDLE) {
		dbenv->err(dbenv, EINVAL, DB_STR("4561",
		    "xa_commit: commiting transaction active in branch"));
		return (XAER_PROTO);
	}

	if (!LF_ISSET(TMONEPHASE) && td->xa_br_status != TXN_XA_PREPARED) {
		dbenv->err(dbenv, EINVAL, DB_STR("4562",
		    "xa_commit: attempting to commit unprepared transaction"));
		return (XAER_PROTO);
	}

	/* Now, fill in the global transaction structure. */
	if ((ret = __xa_get_txn(env, xid, td, &txnp, TMJOIN, 0)) != 0)
		return (ret);

	/*
	 * Because this transaction is currently associated, commit will not free
	 * the transaction structure, which is good, because we need to do that
	 * in xa_put_txn below.
	 */
	if ((ret = txnp->commit(txnp, 0)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4563",
		    "xa_commit: txnp->commit failed"));
		return (XAER_RMERR);
	}

	__xa_put_txn(env, txnp);
	return (XA_OK);
}

/*
 * __db_xa_recover --
 *	Returns a list of prepared and heuristically completed transactions.
 *
 * The return value is the number of xids placed into the xid array (less
 * than or equal to the count parameter).  The flags are going to indicate
 * whether we are starting a scan or continuing one.
 */
static int
__db_xa_recover(xids, count, rmid, flags)
	XID *xids;
	long count, flags;
	int rmid;
{
	ENV *env;
	int ret;
	u_int32_t newflags;
	long rval;

	/* If the environment is closed, then we're done. */
	if (__db_rmid_to_env(rmid, &env) != 0)
		return (XAER_PROTO);

	if (LF_ISSET(TMSTARTRSCAN))
		newflags = DB_FIRST;
	else if (LF_ISSET(TMENDRSCAN))
		newflags = DB_LAST;
	else
		newflags = DB_NEXT;

	rval = 0;
	if ((ret = __xa_txn_get_prepared(env,
	    xids, NULL, count, &rval, newflags)) != 0) {
		env->dbenv->err(env->dbenv, ret, DB_STR("4564",
		    "xa_recover: txn_get_prepared failed"));
		return (XAER_RMERR);
	}

	return (rval);
}

/*
 * __db_xa_rollback
 *	Abort an XA transaction.
 */
static int
__db_xa_rollback(xid, rmid, arg_flags)
	XID *xid;
	int rmid;
	long arg_flags;
{
	DB_ENV *dbenv;
	DB_TXN *txnp;
	ENV *env;
	TXN_DETAIL *td;
	int ret;
	u_long flags;

	flags = (u_long)arg_flags;	/* Conversion for bit operations. */
	ret = 0;

	if (LF_ISSET(TMASYNC))
		return (XAER_ASYNC);
	if (flags != TMNOFLAGS)
		return (XAER_INVAL);

	if (__db_rmid_to_env(rmid, &env) != 0)
		return (XAER_PROTO);
	dbenv = env->dbenv;

	/*
	 * If the environment is corrupted, reopen it or die if that
	 * is not possible.
	 */
	PANIC_CHECK_RET(env, ret);
	if (ret == DB_RUNRECOVERY) {
		corrupted_env(env, rmid);
		if (__db_rmid_to_env(rmid, &env) != 0)
			return (XAER_PROTO);
		dbenv = env->dbenv;
	}

	if ((ret = __db_xid_to_txn(env, xid, &td)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4565",
		    "xa_rollback: failure mapping xid"));
		return (XAER_RMFAIL);
	} if (td == NULL) {
		dbenv->err(dbenv, ret, DB_STR("4566",
		    "xa_rollback: xid not found"));
		return (XAER_NOTA);
	}

	if (td->xa_br_status == TXN_XA_DEADLOCKED)
		return (XA_RBDEADLOCK);

	if (td->xa_br_status == TXN_XA_ROLLEDBACK)
		return (XA_RBOTHER);

	if (td->xa_br_status != TXN_XA_ACTIVE &&
	    td->xa_br_status != TXN_XA_IDLE &&
	    td->xa_br_status != TXN_XA_PREPARED) {
		dbenv->err(dbenv, EINVAL, DB_STR_A("4567",
		    "xa_rollback: transaction in invalid state %d",
		    "%d"), (int)td->xa_br_status);
		return (XAER_PROTO);
	}

	/* Now, fill in the global transaction structure. */
	if ((ret = __xa_get_txn(env, xid, td, &txnp, TMJOIN, 0)) != 0)
		return (ret);
	/*
	 * Normally abort frees the txnp, but if this is an associated XA
	 * transaction, then abort will not free it; we do that below.
	 */
	if ((ret = txnp->abort(txnp)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4568",
		    "xa_rollback: failure aborting transaction"));
		return (XAER_RMERR);
	}

	__xa_put_txn(env, txnp);

	return (XA_OK);
}

/*
 * __db_xa_forget --
 *	Forget about an XID for a transaction that was heuristically
 * completed.  Since we do not heuristically complete anything, I
 * don't think we have to do anything here, but we should make sure
 * that we reclaim the slots in the txnid table.
 */
static int
__db_xa_forget(xid, rmid, arg_flags)
	XID *xid;
	int rmid;
	long arg_flags;
{
	DB_ENV *dbenv;
	DB_TXN *txnp;
	ENV *env;
	TXN_DETAIL *td;
	int ret;
	u_long flags;

	flags = (u_long)arg_flags;	/* Conversion for bit operations. */

	if (LF_ISSET(TMASYNC))
		return (XAER_ASYNC);
	if (flags != TMNOFLAGS)
		return (XAER_INVAL);

	if (__db_rmid_to_env(rmid, &env) != 0)
		return (XAER_PROTO);
	dbenv = env->dbenv;

	/*
	 * If mapping is gone, then we're done.
	 */
	if ((ret = __db_xid_to_txn(env, xid, &td)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4569",
		    "xa_forget: failure mapping xid"));
		return (XAER_RMFAIL);
	}
	if (td == NULL) {
		dbenv->err(dbenv, ret, DB_STR("4570",
		    "xa_forget: xid not found"));
		return (XA_OK);
	}

	if ((ret = __xa_get_txn(env, xid, td, &txnp, TMJOIN, 0)) != 0)
		return (ret);

	if ((ret = txnp->discard(txnp, 0)) != 0) {
		dbenv->err(dbenv, ret, DB_STR("4571",
		    "xa_forget: txnp->discard failed"));
		return (XAER_RMFAIL);
	}

	__xa_put_txn(env, txnp);
	return (XA_OK);
}

/*
 * __db_xa_complete --
 *	Used to wait for asynchronous operations to complete.  Since we're
 *	not doing asynch, this is an invalid operation.
 */
static int
__db_xa_complete(handle, retval, rmid, flags)
	int *handle, *retval, rmid;
	long flags;
{
	COMPQUIET(handle, NULL);
	COMPQUIET(retval, NULL);
	COMPQUIET(rmid, 0);
	COMPQUIET(flags, 0);

	return (XAER_INVAL);
}
