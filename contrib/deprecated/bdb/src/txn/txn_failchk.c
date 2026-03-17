/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2005, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/txn.h"

/*
 * __txn_failchk --
 *	Check for transactions started by dead threads of control.
 *
 * PUBLIC: int __txn_failchk __P((ENV *));
 */
int
__txn_failchk(env)
	ENV *env;
{
	DB_ENV *dbenv;
	DB_TXN *ktxn, *txn;
	DB_TXNMGR *mgr;
	DB_TXNREGION *region;
	TXN_DETAIL *ktd, *td;
	db_threadid_t tid;
	int ret;
	char buf[DB_THREADID_STRLEN];
	pid_t pid;

	mgr = env->tx_handle;
	dbenv = env->dbenv;
	region = mgr->reginfo.primary;

retry:	TXN_SYSTEM_LOCK(env);

	SH_TAILQ_FOREACH(td, &region->active_txn, links, __txn_detail) {
		/*
		 * If this is a child transaction, skip it.
		 * The parent will take care of it.
		 */
		if (td->parent != INVALID_ROFF)
			continue;
		/*
		 * If the txn is prepared, then it does not matter
		 * what the state of the thread is.
		 */
		if (td->status == TXN_PREPARED)
			continue;

		/* If the thread is still alive, it's not a problem. */
		if (dbenv->is_alive(dbenv, td->pid, td->tid, 0))
			continue;

		if (F_ISSET(td, TXN_DTL_INMEMORY)) {
			TXN_SYSTEM_UNLOCK(env);
			return (__db_failed(env, DB_STR("4501",
			    "Transaction has in memory logs"),
			     td->pid, td->tid));
		}

		/* Abort the transaction. */
		TXN_SYSTEM_UNLOCK(env);
		if ((ret = __os_calloc(env, 1, sizeof(DB_TXN), &txn)) != 0)
			return (ret);
		if ((ret = __txn_continue(env, txn, td, NULL, 1)) != 0)
			return (ret);
		SH_TAILQ_FOREACH(ktd, &td->kids, klinks, __txn_detail) {
			if (F_ISSET(ktd, TXN_DTL_INMEMORY))
				return (__db_failed(env, DB_STR("4502",
				    "Transaction has in memory logs"),
				     td->pid, td->tid));
			if ((ret =
			    __os_calloc(env, 1, sizeof(DB_TXN), &ktxn)) != 0)
				return (ret);
			if ((ret =
			    __txn_continue(env, ktxn, ktd, NULL, 1)) != 0)
				return (ret);
			ktxn->parent = txn;
			ktxn->mgrp = txn->mgrp;
			TAILQ_INSERT_HEAD(&txn->kids, ktxn, klinks);
		}
		pid = td->pid;
		tid = td->tid;
		(void)dbenv->thread_id_string(dbenv, pid, tid, buf);
		__db_msg(env, DB_STR_A("4503",
		    "Aborting txn %#lx: %s", "%#lx %s"),
		    (u_long)txn->txnid, buf);
		if ((ret = __txn_abort(txn)) != 0)
			return (__db_failed(env, DB_STR("4504",
			    "Transaction abort failed"), pid, tid));
		goto retry;
	}

	TXN_SYSTEM_UNLOCK(env);

	return (0);
}
