/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"

static int __rep_collect_txn __P((ENV *, DB_LSN *, LSN_COLLECTION *));
static int __rep_do_ckp __P((ENV *, DBT *, __rep_control_args *));
static int __rep_fire_newmaster __P((ENV *, u_int32_t, int));
static int __rep_fire_startupdone __P((ENV *, u_int32_t, int));
static int __rep_getnext __P((ENV *, DB_THREAD_INFO *));
static int __rep_lsn_cmp __P((const void *, const void *));
static int __rep_newfile __P((ENV *, __rep_control_args *, DBT *));
static int __rep_process_rec __P((ENV *, DB_THREAD_INFO *, __rep_control_args *,
    DBT *, db_timespec *, DB_LSN *));
static int __rep_remfirst __P((ENV *, DB_THREAD_INFO *, DBT *, DBT *));
static int __rep_skip_msg __P((ENV *, REP *, int, u_int32_t));

/* Used to consistently designate which messages ought to be received where. */

#define	MASTER_ONLY(rep, rp) do {					\
	if (!F_ISSET(rep, REP_F_MASTER)) {				\
		RPRINT(env, (env, DB_VERB_REP_MSGS,			\
		    "Master record received on client"));		\
		REP_PRINT_MESSAGE(env,					\
		    eid, rp, "rep_process_message", 0);			\
		/* Just skip/ignore it. */				\
		ret = 0;						\
		goto errlock;						\
	}								\
} while (0)

#define	CLIENT_ONLY(rep, rp) do {					\
	if (!F_ISSET(rep, REP_F_CLIENT)) {				\
		RPRINT(env, (env, DB_VERB_REP_MSGS,			\
		    "Client record received on master"));		\
		/*							\
		 * Only broadcast DUPMASTER if leases are not		\
		 * in effect.  If I am an old master, using		\
		 * leases and I get a newer message, my leases		\
		 * had better all be expired.				\
		 */							\
		if (IS_USING_LEASES(env))				\
			DB_ASSERT(env,					\
			    __rep_lease_check(env, 0) ==		\
			    DB_REP_LEASE_EXPIRED);			\
		else {							\
			REP_PRINT_MESSAGE(env,				\
			    eid, rp, "rep_process_message", 0);		\
			(void)__rep_send_message(env, DB_EID_BROADCAST, \
			    REP_DUPMASTER, NULL, NULL, 0, 0);		\
		}							\
		ret = DB_REP_DUPMASTER;					\
		goto errlock;						\
	}								\
} while (0)

/*
 * If a client is attempting to service a request and its gen is not in
 * sync with its database state, it cannot service the request.  Currently
 * the only way to know this is with the heavy hammer of knowing (or not)
 * who the master is.  If the master is invalid, force a rerequest.
 * If we receive an ALIVE, we update both gen and invalidate the
 * master_id.
 */
#define	CLIENT_MASTERCHK do {						\
	if (F_ISSET(rep, REP_F_CLIENT)) {				\
		if (master_id == DB_EID_INVALID) {			\
			STAT(rep->stat.st_client_svc_miss++);		\
			ret = __rep_skip_msg(env, rep, eid, rp->rectype);\
			goto errlock;					\
		}							\
	}								\
} while (0)

/*
 * If a client is attempting to service a request it does not have,
 * call rep_skip_msg to skip this message and force a rerequest to the
 * sender.  We don't hold the mutex for the stats and may miscount.
 */
#define	CLIENT_REREQ do {						\
	if (F_ISSET(rep, REP_F_CLIENT)) {				\
		STAT(rep->stat.st_client_svc_req++);			\
		if (ret == DB_NOTFOUND) {				\
			STAT(rep->stat.st_client_svc_miss++);		\
			ret = __rep_skip_msg(env, rep, eid, rp->rectype);\
		}							\
	}								\
} while (0)

#define	RECOVERING_SKIP do {						\
	if (IS_REP_CLIENT(env) && recovering) {			\
		/* Not holding region mutex, may miscount */		\
		STAT(rep->stat.st_msgs_recover++);			\
		ret = __rep_skip_msg(env, rep, eid, rp->rectype);	\
		goto errlock;						\
	}								\
} while (0)

/*
 * If we're recovering the log we only want log records that are in the
 * range we need to recover.  Otherwise we can end up storing a huge
 * number of "new" records, only to truncate the temp database later after
 * we run recovery.  If we are actively delaying a sync-up, we also skip
 * all incoming log records until the application requests sync-up.
 */
#define	RECOVERING_LOG_SKIP do {					\
	if (F_ISSET(rep, REP_F_DELAY) ||				\
	    rep->master_id == DB_EID_INVALID ||				\
	    (recovering &&						\
	    (rep->sync_state != SYNC_LOG ||				\
	     LOG_COMPARE(&rp->lsn, &rep->last_lsn) >= 0))) {		\
		/* Not holding region mutex, may miscount */		\
		STAT(rep->stat.st_msgs_recover++);			\
		ret = __rep_skip_msg(env, rep, eid, rp->rectype);	\
		goto errlock;						\
	}								\
} while (0)

#define	ANYSITE(rep)

/*
 * __rep_process_message_pp --
 *
 * This routine takes an incoming message and processes it.
 *
 * control: contains the control fields from the record
 * rec: contains the actual record
 * eid: the environment id of the sender of the message;
 * ret_lsnp: On DB_REP_ISPERM and DB_REP_NOTPERM returns, contains the
 *	lsn of the maximum permanent or current not permanent log record
 *	(respectively).
 *
 * PUBLIC: int __rep_process_message_pp
 * PUBLIC:      __P((DB_ENV *, DBT *, DBT *, int, DB_LSN *));
 */
int
__rep_process_message_pp(dbenv, control, rec, eid, ret_lsnp)
	DB_ENV *dbenv;
	DBT *control, *rec;
	int eid;
	DB_LSN *ret_lsnp;
{
	ENV *env;
	int ret;

	env = dbenv->env;
	ret = 0;

	ENV_REQUIRES_CONFIG_XX(
	    env, rep_handle, "DB_ENV->rep_process_message", DB_INIT_REP);

	if (APP_IS_REPMGR(env)) {
		__db_errx(env, DB_STR_A("3512",
		    "%s cannot call from Replication Manager application",
		    "%s"), "DB_ENV->rep_process_message:");
		return (EINVAL);
	}

	/* Control argument must be non-Null. */
	if (control == NULL || control->size == 0) {
		__db_errx(env, DB_STR("3513",
	"DB_ENV->rep_process_message: control argument must be specified"));
		return (EINVAL);
	}

	/*
	 * Make sure site is a master or a client, which implies that
	 * replication has been started.
	 */
	if (!IS_REP_MASTER(env) && !IS_REP_CLIENT(env)) {
		__db_errx(env, DB_STR("3514",
	"Environment not configured as replication master or client"));
		return (EINVAL);
	}

	if ((ret = __dbt_usercopy(env, control)) != 0 ||
	    (ret = __dbt_usercopy(env, rec)) != 0) {
		__dbt_userfree(env, control, rec, NULL);
		__db_errx(env, DB_STR("3515",
	"DB_ENV->rep_process_message: error retrieving DBT contents"));
		return (ret);
	}

	ret = __rep_process_message_int(env, control, rec, eid, ret_lsnp);

	__dbt_userfree(env, control, rec, NULL);
	return (ret);
}

/*
 * __rep_process_message_int --
 *
 * This routine performs the internal steps to process an incoming message.
 *
 * PUBLIC: int __rep_process_message_int
 * PUBLIC:     __P((ENV *, DBT *, DBT *, int, DB_LSN *));
 */
int
__rep_process_message_int(env, control, rec, eid, ret_lsnp)
	ENV *env;
	DBT *control, *rec;
	int eid;
	DB_LSN *ret_lsnp;
{
	DBT data_dbt;
	DB_LOG *dblp;
	DB_LSN last_lsn, lsn;
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	LOG *lp;
	REGENV *renv;
	REGINFO *infop;
	REP *rep;
	REP_46_CONTROL *rp46;
	REP_OLD_CONTROL *orp;
	__rep_control_args *rp, tmprp;
	__rep_egen_args egen_arg;
	size_t len;
	u_int32_t gen, rep_version;
	int cmp, do_sync, lockout, master_id, recovering, ret, t_ret;
	time_t savetime;
	u_int8_t buf[__REP_MAXMSG_SIZE];

	ret = 0;
	do_sync = 0;
	lockout = 0;
	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	infop = env->reginfo;
	renv = infop->primary;
	/*
	 * Casting this to REP_OLD_CONTROL is just kind of stylistic: the
	 * rep_version field of course has to be in the same offset in all
	 * versions in order for this to work.
	 *
	 * We can look at the rep_version unswapped here because if we're
	 * talking to an old version, it will always be unswapped.  If
	 * we're talking to a new version, the only issue is if it is
	 * swapped and we take one of the old version conditionals
	 * incorrectly.  The rep_version would need to be very, very
	 * large for a swapped version to look like a small, older
	 * version.  There is no problem here looking at it unswapped.
	 */
	rep_version = ((REP_OLD_CONTROL *)control->data)->rep_version;
	if (rep_version <= DB_REPVERSION_45) {
		orp = (REP_OLD_CONTROL *)control->data;
		if (rep_version == DB_REPVERSION_45 &&
		    F_ISSET(orp, REPCTL_INIT_45)) {
			F_CLR(orp, REPCTL_INIT_45);
			F_SET(orp, REPCTL_INIT);
		}
		tmprp.rep_version = orp->rep_version;
		tmprp.log_version = orp->log_version;
		tmprp.lsn = orp->lsn;
		tmprp.rectype = orp->rectype;
		tmprp.gen = orp->gen;
		tmprp.flags = orp->flags;
		tmprp.msg_sec = 0;
		tmprp.msg_nsec = 0;
	} else if (rep_version == DB_REPVERSION_46) {
		rp46 = (REP_46_CONTROL *)control->data;
		tmprp.rep_version = rp46->rep_version;
		tmprp.log_version = rp46->log_version;
		tmprp.lsn = rp46->lsn;
		tmprp.rectype = rp46->rectype;
		tmprp.gen = rp46->gen;
		tmprp.flags = rp46->flags;
		tmprp.msg_sec = (u_int32_t)rp46->msg_time.tv_sec;
		tmprp.msg_nsec = (u_int32_t)rp46->msg_time.tv_nsec;
	} else
		if ((ret = __rep_control_unmarshal(env, &tmprp,
		    control->data, control->size, NULL)) != 0)
			return (ret);
	rp = &tmprp;
	if (ret_lsnp != NULL)
		ZERO_LSN(*ret_lsnp);

	ENV_ENTER(env, ip);

	REP_PRINT_MESSAGE(env, eid, rp, "rep_process_message", 0);
	/*
	 * Check the version number for both rep and log.  If it is
	 * an old version we support, convert it.  Otherwise complain.
	 */
	if (rp->rep_version < DB_REPVERSION) {
		if (rp->rep_version < DB_REPVERSION_MIN) {
			__db_errx(env, DB_STR_A("3516",
 "unsupported old replication message version %lu, minimum version %d",
			    "%lu %d"), (u_long)rp->rep_version,
			    DB_REPVERSION_MIN);

			ret = EINVAL;
			goto errlock;
		}
		VPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Received record %lu with old rep version %lu",
		    (u_long)rp->rectype, (u_long)rp->rep_version));
		rp->rectype = __rep_msg_from_old(rp->rep_version, rp->rectype);
		DB_ASSERT(env, rp->rectype != REP_INVALID);
		/*
		 * We should have a valid new record type for all the old
		 * versions.
		 */
		VPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Converted to record %lu with old rep version %lu",
		    (u_long)rp->rectype, (u_long)rp->rep_version));
	} else if (rp->rep_version > DB_REPVERSION) {
		__db_errx(env, DB_STR_A("3517",
		    "unexpected replication message version %lu, expected %d",
		    "%lu %d"), (u_long)rp->rep_version, DB_REPVERSION);
		ret = EINVAL;
		goto errlock;
	}

	if (rp->log_version < DB_LOGVERSION) {
		if (rp->log_version < DB_LOGVERSION_MIN) {
			__db_errx(env, DB_STR_A("3518",
 "unsupported old replication log version %lu, minimum version %d",
			    "%lu %d"), (u_long)rp->log_version,
			    DB_LOGVERSION_MIN);
			ret = EINVAL;
			goto errlock;
		}
		VPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Received record %lu with old log version %lu",
		    (u_long)rp->rectype, (u_long)rp->log_version));
	} else if (rp->log_version > DB_LOGVERSION) {
		__db_errx(env, DB_STR_A("3519",
		    "unexpected log record version %lu, expected %d",
		    "%lu %d"), (u_long)rp->log_version, DB_LOGVERSION);
		ret = EINVAL;
		goto errlock;
	}

	/*
	 * Acquire the replication lock.
	 */
	REP_SYSTEM_LOCK(env);
	if (FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_MSG)) {
		/*
		 * If we're racing with a thread in rep_start, then
		 * just ignore the message and return.
		 */
		RPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Racing replication msg lockout, ignore message."));
		/*
		 * Although we're ignoring the message, there are a few
		 * we need to pay a bit of attention to anyway.  All of
		 * these cases are mutually exclusive.
		 * 1. If it is a PERM message, we don't want to return 0.
		 * 2. If it is a NEWSITE message let the app know so it can
		 * do whatever it needs for connection purposes.
		 * 3. If it is a c2c request, tell the sender we're not
		 * going to handle it.
		 */
		if (F_ISSET(rp, REPCTL_PERM))
			ret = DB_REP_IGNORE;
		REP_SYSTEM_UNLOCK(env);
		/*
		 * If this is new site information return DB_REP_NEWSITE so
		 * that the user can use whatever information may have been
		 * sent for connections.
		 */
		if (rp->rectype == REP_NEWSITE)
			ret = DB_REP_NEWSITE;
		/*
		 * If another client has sent a c2c request to us, it may be a
		 * long time before it resends the request (due to its dual data
		 * streams avoidance heuristic); let it know we can't serve the
		 * request just now.
		 */
		if (F_ISSET(rep, REP_F_CLIENT) && REP_MSG_REQ(rp->rectype)) {
			STAT(rep->stat.st_client_svc_req++);
			STAT(rep->stat.st_client_svc_miss++);
			(void)__rep_send_message(env,
			    eid, REP_REREQUEST, NULL, NULL, 0, 0);
		}
		goto out;
	}
	rep->msg_th++;
	gen = rep->gen;
	master_id = rep->master_id;
	recovering = IS_REP_RECOVERING(rep);
	savetime = renv->rep_timestamp;

	STAT(rep->stat.st_msgs_processed++);
	REP_SYSTEM_UNLOCK(env);

	/*
	 * Check for lease configuration matching.  Leases must be
	 * configured all or none.  If I am a client and I receive a
	 * message requesting a lease, and I'm not using leases, that
	 * is an error.
	 */
	if (!IS_USING_LEASES(env) &&
	    (F_ISSET(rp, REPCTL_LEASE) || rp->rectype == REP_LEASE_GRANT)) {
		__db_errx(env, DB_STR("3520",
		    "Inconsistent lease configuration"));
		RPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Client received lease message and not using leases"));
		ret = EINVAL;
		ret = __env_panic(env, ret);
		goto errlock;
	}

	/*
	 * Check for generation number matching.  Ignore any old messages
	 * except requests that are indicative of a new client that needs
	 * to get in sync.
	 */
	if (rp->gen < gen && rp->rectype != REP_ALIVE_REQ &&
	    rp->rectype != REP_NEWCLIENT && rp->rectype != REP_MASTER_REQ &&
	    rp->rectype != REP_DUPMASTER && rp->rectype != REP_VOTE1) {
		/*
		 * We don't hold the rep mutex, and could miscount if we race.
		 */
		STAT(rep->stat.st_msgs_badgen++);
		if (F_ISSET(rp, REPCTL_PERM))
			ret = DB_REP_IGNORE;
		goto errlock;
	}

	if (rp->gen > gen) {
		/*
		 * If I am a master and am out of date with a lower generation
		 * number, I am in bad shape and should downgrade.
		 */
		if (F_ISSET(rep, REP_F_MASTER)) {
			STAT(rep->stat.st_dupmasters++);
			ret = DB_REP_DUPMASTER;
			/*
			 * Only broadcast DUPMASTER if leases are not
			 * in effect.  If I am an old master, using
			 * leases and I get a newer message, my leases
			 * had better all be expired.
			 */
			if (IS_USING_LEASES(env))
				DB_ASSERT(env,
				    __rep_lease_check(env, 0) ==
				    DB_REP_LEASE_EXPIRED);
			else if (rp->rectype != REP_DUPMASTER)
				(void)__rep_send_message(env,
				    DB_EID_BROADCAST, REP_DUPMASTER,
				    NULL, NULL, 0, 0);
			goto errlock;
		}

		/*
		 * I am a client and am out of date.  If this is an election,
		 * or a response from the first site I contacted, then I can
		 * accept the generation number and participate in future
		 * elections and communication. Otherwise, I need to hear about
		 * a new master and sync up.
		 */
		if (rp->rectype == REP_ALIVE ||
		    rp->rectype == REP_VOTE1 || rp->rectype == REP_VOTE2) {
			REP_SYSTEM_LOCK(env);
			RPRINT(env, (env, DB_VERB_REP_MSGS,
			    "Updating gen from %lu to %lu",
			    (u_long)gen, (u_long)rp->gen));
			rep->master_id = DB_EID_INVALID;
			gen = rp->gen;
			SET_GEN(gen);
			/*
			 * Updating of egen will happen when we process the
			 * message below for each message type.
			 */
			REP_SYSTEM_UNLOCK(env);
			if (rp->rectype == REP_ALIVE)
				(void)__rep_send_message(env,
				    DB_EID_BROADCAST, REP_MASTER_REQ, NULL,
				    NULL, 0, 0);
		} else if (rp->rectype != REP_NEWMASTER) {
			/*
			 * Ignore this message, retransmit if needed.
			 */
			if (__rep_check_doreq(env, rep))
				(void)__rep_send_message(env,
				    DB_EID_BROADCAST, REP_MASTER_REQ,
				    NULL, NULL, 0, 0);
			goto errlock;
		}
		/*
		 * If you get here, then you're a client and either you're
		 * in an election or you have a NEWMASTER or an ALIVE message
		 * whose processing will do the right thing below.
		 */
	}

	/*
	 * If the sender is part of an established group, so are we now.
	 */
	if (F_ISSET(rp, REPCTL_GROUP_ESTD)) {
		REP_SYSTEM_LOCK(env);
#ifdef	DIAGNOSTIC
		if (!F_ISSET(rep, REP_F_GROUP_ESTD))
			RPRINT(env, (env, DB_VERB_REP_MSGS,
			    "I am now part of an established group"));
#endif
		F_SET(rep, REP_F_GROUP_ESTD);
		REP_SYSTEM_UNLOCK(env);
	}

	/*
	 * We need to check if we're in recovery and if we are
	 * then we need to ignore any messages except VERIFY*, VOTE*,
	 * NEW* and ALIVE_REQ, or backup related messages: UPDATE*,
	 * PAGE* and FILE*.  We need to also accept LOG messages
	 * if we're copying the log for recovery/backup.
	 */
	switch (rp->rectype) {
	case REP_ALIVE:
		/*
		 * Handle even if we're recovering.
		 */
		ANYSITE(rep);
		if (rp->rep_version < DB_REPVERSION_47)
			egen_arg.egen = *(u_int32_t *)rec->data;
		else if ((ret = __rep_egen_unmarshal(env, &egen_arg,
		    rec->data, rec->size, NULL)) != 0)
			return (ret);
		REP_SYSTEM_LOCK(env);
		if (egen_arg.egen > rep->egen) {
			/*
			 * If we're currently working futilely at processing an
			 * obsolete egen, treat it like an egen update, so that
			 * we abort the current rep_elect() call and signal the
			 * application to start a new one.
			 */
			if (rep->spent_egen == rep->egen)
				ret = DB_REP_HOLDELECTION;

			RPRINT(env, (env, DB_VERB_REP_MSGS,
			    "Received ALIVE egen of %lu, mine %lu",
			    (u_long)egen_arg.egen, (u_long)rep->egen));
			__rep_elect_done(env, rep);
			rep->egen = egen_arg.egen;
		}
		REP_SYSTEM_UNLOCK(env);
		break;
	case REP_ALIVE_REQ:
		/*
		 * Handle even if we're recovering.
		 */
		ANYSITE(rep);
		LOG_SYSTEM_LOCK(env);
		lsn = lp->lsn;
		LOG_SYSTEM_UNLOCK(env);
#ifdef	CONFIG_TEST
		/*
		 * Send this first, before the ALIVE message because of the
		 * way the test suite and messaging is done sequentially.
		 * In some sequences it is possible to get into a situation
		 * where the test suite cannot get the later NEWMASTER because
		 * we break out of the messaging loop too early.
		 */
		if (F_ISSET(rep, REP_F_MASTER))
			(void)__rep_send_message(env,
			    DB_EID_BROADCAST, REP_NEWMASTER, &lsn, NULL, 0, 0);
#endif
		REP_SYSTEM_LOCK(env);
		egen_arg.egen = rep->egen;
		REP_SYSTEM_UNLOCK(env);
		if (rep->version < DB_REPVERSION_47)
			DB_INIT_DBT(data_dbt, &egen_arg.egen,
			    sizeof(egen_arg.egen));
		else {
			if ((ret = __rep_egen_marshal(env,
			    &egen_arg, buf, __REP_EGEN_SIZE, &len)) != 0)
				goto errlock;
			DB_INIT_DBT(data_dbt, buf, len);
		}
		(void)__rep_send_message(env,
		    eid, REP_ALIVE, &lsn, &data_dbt, 0, 0);
		break;
	case REP_ALL_REQ:
		RECOVERING_SKIP;
		CLIENT_MASTERCHK;
		ret = __rep_allreq(env, rp, eid);
		CLIENT_REREQ;
		break;
	case REP_BULK_LOG:
		RECOVERING_LOG_SKIP;
		CLIENT_ONLY(rep, rp);
		ret = __rep_bulk_log(env, ip, rp, rec, savetime, ret_lsnp);
		break;
	case REP_BULK_PAGE:
		/*
		 * Handle even if we're recovering.
		 */
		CLIENT_ONLY(rep, rp);
		ret = __rep_bulk_page(env, ip, eid, rp, rec);
		break;
	case REP_DUPMASTER:
		/*
		 * Handle even if we're recovering.
		 */
		if (F_ISSET(rep, REP_F_MASTER))
			ret = DB_REP_DUPMASTER;
		break;
#ifdef NOTYET
	case REP_FILE: /* TODO */
		CLIENT_ONLY(rep, rp);
		break;
	case REP_FILE_REQ:
		ret = __rep_send_file(env, rec, eid);
		break;
#endif
	case REP_FILE_FAIL:
		/*
		 * Handle even if we're recovering.
		 */
		CLIENT_ONLY(rep, rp);
		/*
		 * Clean up any internal init that was in progress.
		 */
		if (eid == rep->master_id) {
			REP_SYSTEM_LOCK(env);
			/*
			 * If we're already locking out messages, give up.
			 */
			if (FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_MSG))
				goto errhlk;
			/*
			 * Lock out other messages to prevent race
			 * conditions.
			 */
			if ((ret =
			    __rep_lockout_msg(env, rep, 1)) != 0) {
				goto errhlk;
			}
			lockout = 1;
			/*
			 * Need mtx_clientdb to safely clean up
			 * page database in __rep_init_cleanup().
			 */
			REP_SYSTEM_UNLOCK(env);
			MUTEX_LOCK(env, rep->mtx_clientdb);
			REP_SYSTEM_LOCK(env);
			/*
			 * Clean up internal init if one was in progress.
			 */
			if (ISSET_LOCKOUT_BDB(rep)) {
				RPRINT(env, (env, DB_VERB_REP_MSGS,
    "FILE_FAIL is cleaning up old internal init"));
#ifdef	CONFIG_TEST
				STAT(rep->stat.st_filefail_cleanups++);
#endif
				ret = __rep_init_cleanup(env, rep, DB_FORCE);
				F_CLR(rep, REP_F_ABBREVIATED);
				CLR_RECOVERY_SETTINGS(rep);
			}
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			if (ret != 0) {
				RPRINT(env, (env, DB_VERB_REP_MSGS,
    "FILE_FAIL error cleaning up internal init: %d", ret));
				goto errhlk;
			}
			FLD_CLR(rep->lockout_flags, REP_LOCKOUT_MSG);
			lockout = 0;
			/*
			 * Restart internal init, setting UPDATE flag and
			 * zeroing applicable LSNs.
			 */
			rep->sync_state = SYNC_UPDATE;
			ZERO_LSN(rep->first_lsn);
			ZERO_LSN(rep->ckp_lsn);
			REP_SYSTEM_UNLOCK(env);
			(void)__rep_send_message(env, eid, REP_UPDATE_REQ,
			    NULL, NULL, 0, 0);
		}
		break;
	case REP_LEASE_GRANT:
		/*
		 * Handle even if we're recovering.
		 */
		MASTER_ONLY(rep, rp);
		ret = __rep_lease_grant(env, rp, rec, eid);
		break;
	case REP_LOG:
	case REP_LOG_MORE:
		RECOVERING_LOG_SKIP;
		CLIENT_ONLY(rep, rp);
		ret = __rep_log(env, ip, rp, rec, eid, savetime, ret_lsnp);
		break;
	case REP_LOG_REQ:
		RECOVERING_SKIP;
		CLIENT_MASTERCHK;
		if (F_ISSET(rp, REPCTL_INIT))
			MASTER_UPDATE(env, renv);
		ret = __rep_logreq(env, rp, rec, eid);
		CLIENT_REREQ;
		break;
	case REP_NEWSITE:
		/*
		 * Handle even if we're recovering.
		 */
		/* We don't hold the rep mutex, and may miscount. */
		STAT(rep->stat.st_newsites++);

		/* This is a rebroadcast; simply tell the application. */
		if (F_ISSET(rep, REP_F_MASTER)) {
			dblp = env->lg_handle;
			lp = dblp->reginfo.primary;
			LOG_SYSTEM_LOCK(env);
			lsn = lp->lsn;
			LOG_SYSTEM_UNLOCK(env);
			(void)__rep_send_message(env,
			    eid, REP_NEWMASTER, &lsn, NULL, 0, 0);
		}
		ret = DB_REP_NEWSITE;
		break;
	case REP_NEWCLIENT:
		/*
		 * Handle even if we're recovering.
		 */
		/*
		 * This message was received and should have resulted in the
		 * application entering the machine ID in its machine table.
		 * We respond to this with an ALIVE to send relevant information
		 * to the new client (if we are a master, we'll send a
		 * NEWMASTER, so we only need to send the ALIVE if we're a
		 * client).  But first, broadcast the new client's record to
		 * all the clients.
		 */
		(void)__rep_send_message(env,
		    DB_EID_BROADCAST, REP_NEWSITE, &rp->lsn, rec, 0, 0);

		ret = DB_REP_NEWSITE;

		if (F_ISSET(rep, REP_F_CLIENT)) {
			REP_SYSTEM_LOCK(env);
			egen_arg.egen = rep->egen;

			/*
			 * Clean up any previous master remnants by making
			 * master_id invalid and cleaning up any internal
			 * init that was in progress.
			 */
			if (eid == rep->master_id) {
				rep->master_id = DB_EID_INVALID;

				/*
				 * Already locking out messages, must be
				 * in sync-up recover or internal init,
				 * give up.
				 */
				if (FLD_ISSET(rep->lockout_flags,
				    REP_LOCKOUT_MSG))
					goto errhlk;

				/*
				 * Lock out other messages to prevent race
				 * conditions.
				 */
				if ((t_ret =
				    __rep_lockout_msg(env, rep, 1)) != 0) {
					ret = t_ret;
					goto errhlk;
				}
				lockout = 1;

				/*
				 * Need mtx_clientdb to safely clean up
				 * page database in __rep_init_cleanup().
				 */
				REP_SYSTEM_UNLOCK(env);
				MUTEX_LOCK(env, rep->mtx_clientdb);
				REP_SYSTEM_LOCK(env);

				/*
				 * Clean up internal init if one was in
				 * progress.
				 */
				if (ISSET_LOCKOUT_BDB(rep)) {
					RPRINT(env, (env, DB_VERB_REP_MSGS,
    "NEWCLIENT is cleaning up old internal init for invalid master"));
					t_ret = __rep_init_cleanup(env,
					    rep, DB_FORCE);
					F_CLR(rep, REP_F_ABBREVIATED);
					CLR_RECOVERY_SETTINGS(rep);
				}
				MUTEX_UNLOCK(env, rep->mtx_clientdb);
				if (t_ret != 0) {
					ret = t_ret;
					RPRINT(env, (env, DB_VERB_REP_MSGS,
    "NEWCLIENT error cleaning up internal init for invalid master: %d", ret));
					goto errhlk;
				}
				FLD_CLR(rep->lockout_flags, REP_LOCKOUT_MSG);
				lockout = 0;
			}
			REP_SYSTEM_UNLOCK(env);
			if (rep->version < DB_REPVERSION_47)
				DB_INIT_DBT(data_dbt, &egen_arg.egen,
				    sizeof(egen_arg.egen));
			else {
				if ((ret = __rep_egen_marshal(env, &egen_arg,
				    buf, __REP_EGEN_SIZE, &len)) != 0)
					goto errlock;
				DB_INIT_DBT(data_dbt, buf, len);
			}
			(void)__rep_send_message(env, DB_EID_BROADCAST,
			    REP_ALIVE, &rp->lsn, &data_dbt, 0, 0);
			break;
		}
		/* FALLTHROUGH */
	case REP_MASTER_REQ:
		RECOVERING_SKIP;
		if (F_ISSET(rep, REP_F_MASTER)) {
			LOG_SYSTEM_LOCK(env);
			lsn = lp->lsn;
			LOG_SYSTEM_UNLOCK(env);
			(void)__rep_send_message(env,
			    DB_EID_BROADCAST, REP_NEWMASTER, &lsn, NULL, 0, 0);
			if (IS_USING_LEASES(env))
				(void)__rep_lease_refresh(env);
		}
		/*
		 * If there is no master, then we could get into a state
		 * where an old client lost the initial ALIVE message and
		 * is calling an election under an old gen and can
		 * never get to the current gen.
		 */
		if (F_ISSET(rep, REP_F_CLIENT) && rp->gen < gen) {
			REP_SYSTEM_LOCK(env);
			egen_arg.egen = rep->egen;
			if (eid == rep->master_id)
				rep->master_id = DB_EID_INVALID;
			REP_SYSTEM_UNLOCK(env);
			if (rep->version < DB_REPVERSION_47)
				DB_INIT_DBT(data_dbt, &egen_arg.egen,
				    sizeof(egen_arg.egen));
			else {
				if ((ret = __rep_egen_marshal(env, &egen_arg,
				    buf, __REP_EGEN_SIZE, &len)) != 0)
					goto errlock;
				DB_INIT_DBT(data_dbt, buf, len);
			}
			(void)__rep_send_message(env, eid,
			    REP_ALIVE, &rp->lsn, &data_dbt, 0, 0);
		}
		break;
	case REP_NEWFILE:
		RECOVERING_LOG_SKIP;
		CLIENT_ONLY(rep, rp);
		ret = __rep_apply(env,
		     ip, rp, rec, ret_lsnp, NULL, &last_lsn);
		if (ret == DB_REP_LOGREADY)
			ret = __rep_logready(env, rep, savetime, &last_lsn);
		break;
	case REP_NEWMASTER:
		/*
		 * Handle even if we're recovering.
		 */
		ANYSITE(rep);
		if (F_ISSET(rep, REP_F_MASTER) &&
		    eid != rep->eid) {
			/* We don't hold the rep mutex, and may miscount. */
			STAT(rep->stat.st_dupmasters++);
			ret = DB_REP_DUPMASTER;
			if (IS_USING_LEASES(env))
				DB_ASSERT(env,
				    __rep_lease_check(env, 0) ==
				    DB_REP_LEASE_EXPIRED);
			else
				(void)__rep_send_message(env,
				    DB_EID_BROADCAST, REP_DUPMASTER,
				    NULL, NULL, 0, 0);
			break;
		}
		if ((ret =
		    __rep_new_master(env, rp, eid)) == DB_REP_NEWMASTER)
			ret = __rep_fire_newmaster(env, rp->gen, eid);
		break;
	case REP_PAGE:
	case REP_PAGE_FAIL:
	case REP_PAGE_MORE:
		/*
		 * Handle even if we're recovering.
		 */
		CLIENT_ONLY(rep, rp);
		ret = __rep_page(env, ip, eid, rp, rec);
		if (ret == DB_REP_PAGEDONE)
			ret = 0;
		break;
	case REP_PAGE_REQ:
		RECOVERING_SKIP;
		CLIENT_MASTERCHK;
		MASTER_UPDATE(env, renv);
		ret = __rep_page_req(env, ip, eid, rp, rec);
		CLIENT_REREQ;
		break;
	case REP_REREQUEST:
		/*
		 * Handle even if we're recovering.  Don't do a master
		 * check.
		 */
		CLIENT_ONLY(rep, rp);
		/*
		 * Don't hold any mutex, may miscount.
		 */
		STAT(rep->stat.st_client_rerequests++);
		ret = __rep_resend_req(env, 1);
		break;
	case REP_START_SYNC:
		RECOVERING_SKIP;
		MUTEX_LOCK(env, rep->mtx_clientdb);
		cmp = LOG_COMPARE(&rp->lsn, &lp->ready_lsn);
		/*
		 * The comparison needs to be <= because the LSN in
		 * the message can be the LSN of the first outstanding
		 * txn, which may be the LSN immediately after the
		 * previous commit.  The ready_lsn is the LSN of the
		 * next record expected.  In that case, the LSNs
		 * could be equal and the client has the commit and
		 * wants to sync. [SR #15338]
		 */
		if (cmp <= 0) {
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			do_sync = 1;
		} else {
			STAT(rep->stat.st_startsync_delayed++);
			/*
			 * There are cases where keeping the first ckp_lsn
			 * LSN is advantageous and cases where keeping
			 * a later LSN is better.  If random, earlier
			 * log records are missing, keeping the later
			 * LSN seems to be better.  That is what we'll
			 * do for now.
			 */
			if (LOG_COMPARE(&rp->lsn, &rep->ckp_lsn) > 0)
				rep->ckp_lsn = rp->lsn;
			RPRINT(env, (env, DB_VERB_REP_MSGS,
    "Delayed START_SYNC memp_sync due to missing records."));
			RPRINT(env, (env, DB_VERB_REP_MSGS,
    "ready LSN [%lu][%lu], ckp_lsn [%lu][%lu]",
		    (u_long)lp->ready_lsn.file, (u_long)lp->ready_lsn.offset,
		    (u_long)rep->ckp_lsn.file, (u_long)rep->ckp_lsn.offset));
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
		}
		break;
	case REP_UPDATE:
		/*
		 * Handle even if we're recovering.
		 */
		CLIENT_ONLY(rep, rp);
		if ((ret = __rep_update_setup(env,
		    eid, rp, rec, savetime, &lsn)) == DB_REP_WOULDROLLBACK &&
		    ret_lsnp != NULL) {
			/*
			 * Not for a normal internal init.  But this could
			 * happen here if we had to ask for an UPDATE message in
			 * order to check for materializing NIMDBs; in other
			 * words, an "abbreviated internal init."
			 */
			*ret_lsnp = lsn;
		}
		break;
	case REP_UPDATE_REQ:
		/*
		 * Handle even if we're recovering.
		 */
		MASTER_ONLY(rep, rp);
		infop = env->reginfo;
		renv = infop->primary;
		MASTER_UPDATE(env, renv);
		ret = __rep_update_req(env, rp);
		break;
	case REP_VERIFY:
		if (recovering) {
			MUTEX_LOCK(env, rep->mtx_clientdb);
			cmp = LOG_COMPARE(&lp->verify_lsn, &rp->lsn);
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			/*
			 * If this is not the verify record I want, skip it.
			 */
			if (cmp != 0) {
				ret = __rep_skip_msg(
				    env, rep, eid, rp->rectype);
				break;
			}
		}
		CLIENT_ONLY(rep, rp);
		if ((ret = __rep_verify(env, rp, rec, eid, savetime)) ==
		    DB_REP_WOULDROLLBACK && ret_lsnp != NULL)
			*ret_lsnp = rp->lsn;
		break;
	case REP_VERIFY_FAIL:
		/*
		 * Handle even if we're recovering.
		 */
		CLIENT_ONLY(rep, rp);
		ret = __rep_verify_fail(env, rp);
		break;
	case REP_VERIFY_REQ:
		RECOVERING_SKIP;
		CLIENT_MASTERCHK;
		ret = __rep_verify_req(env, rp, eid);
		CLIENT_REREQ;
		break;
	case REP_VOTE1:
		/*
		 * Handle even if we're recovering.
		 */
		ret = __rep_vote1(env, rp, rec, eid);
		break;
	case REP_VOTE2:
		/*
		 * Handle even if we're recovering.
		 */
		ret = __rep_vote2(env, rp, rec, eid);
		break;
	default:
		__db_errx(env, DB_STR_A("3521",
	"DB_ENV->rep_process_message: unknown replication message: type %lu",
		   "%lu"), (u_long)rp->rectype);
		ret = EINVAL;
		break;
	}

errlock:
	REP_SYSTEM_LOCK(env);
errhlk:	if (lockout)
		FLD_CLR(rep->lockout_flags, REP_LOCKOUT_MSG);
	rep->msg_th--;
	REP_SYSTEM_UNLOCK(env);
	if (do_sync) {
		MUTEX_LOCK(env, rep->mtx_ckp);
		lsn = rp->lsn;
		/*
		 * This is the REP_START_SYNC sync, and so we permit it to be
		 * interrupted.
		 */
		ret = __memp_sync(
		    env, DB_SYNC_CHECKPOINT | DB_SYNC_INTERRUPT_OK, &lsn);
		MUTEX_UNLOCK(env, rep->mtx_ckp);
		RPRINT(env, (env, DB_VERB_REP_MSGS,
		    "START_SYNC: Completed sync [%lu][%lu]",
		    (u_long)lsn.file, (u_long)lsn.offset));
	}
out:
	if (ret == 0 && F_ISSET(rp, REPCTL_PERM)) {
		if (ret_lsnp != NULL)
			*ret_lsnp = rp->lsn;
		ret = DB_REP_NOTPERM;
	}
	__dbt_userfree(env, control, rec, NULL);
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __rep_apply --
 *
 * Handle incoming log records on a client, applying when possible and
 * entering into the bookkeeping table otherwise.  This routine manages
 * the state of the incoming message stream -- processing records, via
 * __rep_process_rec, when possible and enqueuing in the __db.rep.db
 * when necessary.  As gaps in the stream are filled in, this is where
 * we try to process as much as possible from __db.rep.db to catch up.
 *
 * PUBLIC: int __rep_apply __P((ENV *, DB_THREAD_INFO *, __rep_control_args *,
 * PUBLIC:     DBT *, DB_LSN *, int *, DB_LSN *));
 */
int
__rep_apply(env, ip, rp, rec, ret_lsnp, is_dupp, last_lsnp)
	ENV *env;
	DB_THREAD_INFO *ip;
	__rep_control_args *rp;
	DBT *rec;
	DB_LSN *ret_lsnp;
	int *is_dupp;
	DB_LSN *last_lsnp;
{
	DB *dbp;
	DBT control_dbt, key_dbt;
	DBT rec_dbt;
	DB_LOG *dblp;
	DB_LSN max_lsn, save_lsn;
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	db_timespec msg_time, max_ts;
	u_int32_t gen, rectype;
	int cmp, event, master, newfile_seen, ret, set_apply, t_ret;

	COMPQUIET(gen, 0);
	COMPQUIET(master, DB_EID_INVALID);

	db_rep = env->rep_handle;
	rep = db_rep->region;
	event = ret = set_apply = 0;
	memset(&control_dbt, 0, sizeof(control_dbt));
	memset(&rec_dbt, 0, sizeof(rec_dbt));
	ZERO_LSN(max_lsn);
	timespecclear(&max_ts);
	timespecset(&msg_time, rp->msg_sec, rp->msg_nsec);
	cmp = -2;		/* OOB value that LOG_COMPARE can't return. */

	dblp = env->lg_handle;
	MUTEX_LOCK(env, rep->mtx_clientdb);
	/*
	 * Lazily open the temp db.  Always set the startup flag to 0
	 * because it was initialized from rep_start.
	 */
	if (db_rep->rep_db == NULL &&
	    (ret = __rep_client_dbinit(env, 0, REP_DB)) != 0) {
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
		goto out;
	}
	dbp = db_rep->rep_db;
	lp = dblp->reginfo.primary;
	newfile_seen = 0;
	REP_SYSTEM_LOCK(env);
	if (rep->sync_state == SYNC_LOG &&
	    LOG_COMPARE(&lp->ready_lsn, &rep->first_lsn) < 0)
		lp->ready_lsn = rep->first_lsn;
	cmp = LOG_COMPARE(&rp->lsn, &lp->ready_lsn);
	/*
	 * If we are going to skip or process any message other
	 * than a duplicate, make note of it if we're in an
	 * election so that the election can rerequest proactively.
	 */
	if (FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_APPLY) && cmp >= 0)
		F_SET(rep, REP_F_SKIPPED_APPLY);

	/*
	 * If we're in the middle of processing a NEWFILE, we've dropped
	 * the mutex and if this matches it is a duplicate record.  We
	 * do not want this call taking the "matching" code below because
	 * we may then process later records in the temp db and the
	 * original NEWFILE may not have the log file ready.  It will
	 * process those temp db items when it completes.
	 */
	if (F_ISSET(rep, REP_F_NEWFILE) && cmp == 0)
		cmp = -1;

	if (cmp == 0) {
		/*
		 * If we are in an election (i.e. we've sent a vote
		 * with an LSN in it), then we drop the next record
		 * we're expecting.  When we find a master, we'll
		 * either go into sync, or if it was an existing
		 * master, rerequest this one record (later records
		 * are accumulating in the temp db).
		 *
		 * We can simply return here, and rep_process_message
		 * will set NOTPERM if necessary for this record.
		 */
		if (FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_APPLY)) {
			/*
			 * We will simply return now.  All special return
			 * processing should be ignored because the special
			 * values are just initialized.  Variables like
			 * max_lsn are still 0.
			 */
			RPRINT(env, (env, DB_VERB_REP_MISC,
			    "rep_apply: In election. Ignoring [%lu][%lu]",
			    (u_long)rp->lsn.file, (u_long)rp->lsn.offset));
			REP_SYSTEM_UNLOCK(env);
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			goto out;
		}
		rep->apply_th++;
		set_apply = 1;
		VPRINT(env, (env, DB_VERB_REP_MISC,
		    "rep_apply: Set apply_th %d", rep->apply_th));
		REP_SYSTEM_UNLOCK(env);
		if (rp->rectype == REP_NEWFILE)
			newfile_seen = 1;
		if ((ret = __rep_process_rec(env, ip,
		    rp, rec, &max_ts, &max_lsn)) != 0)
			goto err;
		/*
		 * If we get the record we are expecting, reset
		 * the count of records we've received and are applying
		 * towards the request interval.
		 */
		__os_gettime(env, &lp->rcvd_ts, 1);
		ZERO_LSN(lp->max_wait_lsn);

		/*
		 * The __rep_remfirst() and __rep_getnext() functions each open,
		 * use and then close a cursor on the temp db, each time through
		 * the loop.  Although this may seem excessive, it is necessary
		 * to avoid locking problems with checkpoints.
		 */
		while (ret == 0 &&
		    LOG_COMPARE(&lp->ready_lsn, &lp->waiting_lsn) == 0) {
			/*
			 * We just filled in a gap in the log record stream.
			 * Write subsequent records to the log.
			 */
gap_check:
			if ((ret = __rep_remfirst(env, ip,
			     &control_dbt, &rec_dbt)) != 0)
				goto err;

			rp = (__rep_control_args *)control_dbt.data;
			timespecset(&msg_time, rp->msg_sec, rp->msg_nsec);
			rec = &rec_dbt;
			if (rp->rectype == REP_NEWFILE)
				newfile_seen = 1;
			if ((ret = __rep_process_rec(env, ip,
			    rp, rec, &max_ts, &max_lsn)) != 0)
				goto err;

			STAT(--rep->stat.st_log_queued);

			/*
			 * Since we just filled a gap in the log stream, and
			 * we're writing subsequent records to the log, we want
			 * to use rcvd_ts and wait_ts so that we will
			 * request the next gap if we end up with a gap and
			 * not so recent records in the temp db, but not
			 * request if recent records are in the temp db and
			 * likely to arrive on its own shortly.  We want to
			 * avoid requesting the record in that case.  Also
			 * reset max_wait_lsn because the next gap is a
			 * fresh gap.
			 */
			lp->rcvd_ts = lp->last_ts;
			lp->wait_ts = rep->request_gap;
			if ((ret = __rep_getnext(env, ip)) == DB_NOTFOUND) {
				__os_gettime(env, &lp->rcvd_ts, 1);
				ret = 0;
				break;
			} else if (ret != 0)
				goto err;
		}

		/*
		 * Check if we're at a gap in the table and if so, whether we
		 * need to ask for any records.
		 */
		if (!IS_ZERO_LSN(lp->waiting_lsn) &&
		    LOG_COMPARE(&lp->ready_lsn, &lp->waiting_lsn) != 0) {
			/*
			 * We got a record and processed it, but we may
			 * still be waiting for more records.  If we
			 * filled a gap we keep a count of how many other
			 * records are in the temp database and if we should
			 * request the next gap at this time.
			 */
			if (__rep_check_doreq(env, rep) && (ret =
			    __rep_loggap_req(env, rep, &rp->lsn, 0)) != 0)
				goto err;
		} else {
			lp->wait_ts = rep->request_gap;
			ZERO_LSN(lp->max_wait_lsn);
		}

	} else if (cmp > 0) {
		/*
		 * The LSN is higher than the one we were waiting for.
		 * This record isn't in sequence; add it to the temporary
		 * database, update waiting_lsn if necessary, and perform
		 * calculations to determine if we should issue requests
		 * for new records.
		 */
		REP_SYSTEM_UNLOCK(env);
		memset(&key_dbt, 0, sizeof(key_dbt));
		key_dbt.data = rp;
		key_dbt.size = sizeof(*rp);
		ret = __db_put(dbp, ip, NULL, &key_dbt, rec, DB_NOOVERWRITE);
		if (ret == 0) {
			STAT(rep->stat.st_log_queued++);
			__os_gettime(env, &lp->last_ts, 1);
#ifdef HAVE_STATISTICS
			rep->stat.st_log_queued_total++;
			if (rep->stat.st_log_queued_max <
			    rep->stat.st_log_queued)
				rep->stat.st_log_queued_max =
				    rep->stat.st_log_queued;
#endif
		}

		if (ret == DB_KEYEXIST)
			ret = 0;
		if (ret != 0 && ret != ENOMEM)
			goto done;

		/*
		 * If we are using in-memory, and got ENOMEM, it is
		 * not an error.  But in that case we want to skip
		 * comparing the message LSN since we're not storing it.
		 * However, we do want continue to check if we need to
		 * send a request for the gap.
		 */
		if (ret == 0 && (IS_ZERO_LSN(lp->waiting_lsn) ||
		    LOG_COMPARE(&rp->lsn, &lp->waiting_lsn) < 0)) {
			/*
			 * If this is a new gap, then reset the rcvd_ts so
			 * that an out-of-order record after an idle period
			 * does not (likely) immediately rerequest.
			 */
			if (IS_ZERO_LSN(lp->waiting_lsn))
				__os_gettime(env, &lp->rcvd_ts, 1);
			lp->waiting_lsn = rp->lsn;
		}

		if (__rep_check_doreq(env, rep) &&
		    (ret = __rep_loggap_req(env, rep, &rp->lsn, 0) != 0))
			goto err;

		/*
		 * If this is permanent; let the caller know that we have
		 * not yet written it to disk, but we've accepted it.
		 */
		if (ret == 0 && F_ISSET(rp, REPCTL_PERM)) {
			max_lsn = rp->lsn;
			ret = DB_REP_NOTPERM;
		}
		goto done;
	} else {
		STAT(rep->stat.st_log_duplicated++);
		REP_SYSTEM_UNLOCK(env);
		if (is_dupp != NULL) {
			*is_dupp = 1;
			/*
			 * Could get overwritten by max_lsn later.
			 * But max_lsn is guaranteed <= ready_lsn, so
			 * it would be a more conservative LSN to return.
			 */
			*ret_lsnp = lp->ready_lsn;
		}
		LOGCOPY_32(env, &rectype, rec->data);
		if (rectype == DB___txn_regop || rectype == DB___txn_ckp)
			max_lsn = lp->max_perm_lsn;
		/*
		 * We check REPCTL_LEASE here, because this client may
		 * have leases configured but the master may not (especially
		 * in a mixed version group.  If the master has leases
		 * configured, all clients must also.
		 */
		if (IS_USING_LEASES(env) &&
		    F_ISSET(rp, REPCTL_LEASE) &&
		    timespecisset(&msg_time)) {
			if (timespeccmp(&msg_time, &lp->max_lease_ts, >))
				max_ts = msg_time;
			else
				max_ts = lp->max_lease_ts;
		}
		goto done;
	}

	/* Check if we need to go back into the table. */
	if (ret == 0 && LOG_COMPARE(&lp->ready_lsn, &lp->waiting_lsn) == 0)
		goto gap_check;

done:
err:	/*
	 * In case of a race, to make sure only one thread can get
	 * DB_REP_LOGREADY, zero out rep->last_lsn to show that we've gotten to
	 * this point.
	 */
	REP_SYSTEM_LOCK(env);
	if (ret == 0 &&
	    rep->sync_state == SYNC_LOG &&
	    !IS_ZERO_LSN(rep->last_lsn) &&
	    LOG_COMPARE(&lp->ready_lsn, &rep->last_lsn) >= 0) {
		*last_lsnp = max_lsn;
		ZERO_LSN(rep->last_lsn);
		ZERO_LSN(max_lsn);
		ret = DB_REP_LOGREADY;
	}
	/*
	 * Only decrement if we were actually applying log records.
	 * We do not care if we processed a dup record or put one
	 * in the temp db.
	 */
	if (set_apply) {
		rep->apply_th--;
		VPRINT(env, (env, DB_VERB_REP_MISC,
		    "rep_apply: Decrement apply_th %d [%lu][%lu]",
		    rep->apply_th, (u_long)lp->ready_lsn.file,
		    (u_long)lp->ready_lsn.offset));
	}

	if (ret == 0 && rep->sync_state != SYNC_LOG &&
	    !IS_ZERO_LSN(max_lsn)) {
		if (ret_lsnp != NULL)
			*ret_lsnp = max_lsn;
		ret = DB_REP_ISPERM;
		DB_ASSERT(env, LOG_COMPARE(&max_lsn, &lp->max_perm_lsn) >= 0);
		lp->max_perm_lsn = max_lsn;
		if ((t_ret = __rep_notify_threads(env, AWAIT_LSN)) != 0)
			ret = t_ret;
	}

	/*
	 * Start-up is complete when we process (or have already processed) up
	 * to the end of the replication group's log.  In case we miss that
	 * message, as a back-up, we also recognize start-up completion when we
	 * actually process a live log record.  Having cmp==0 here (with a good
	 * "ret" value) implies we actually processed the record.
	 */
	if ((ret == 0 || ret == DB_REP_ISPERM) &&
	    rep->stat.st_startup_complete == 0 &&
	    rep->sync_state != SYNC_LOG &&
	    ((cmp <= 0 && F_ISSET(rp, REPCTL_LOG_END)) ||
	    (cmp == 0 && !F_ISSET(rp, REPCTL_RESEND)))) {
		rep->stat.st_startup_complete = 1;
		event = 1;
		gen = rep->gen;
		master = rep->master_id;
	}
	REP_SYSTEM_UNLOCK(env);
	/*
	 * If we've processed beyond the needed LSN for a pending
	 * start sync, start it now.  We must compare > here
	 * because ready_lsn is the next record we expect and if
	 * the last record is a commit, that will dirty pages on
	 * a client as that txn is applied.
	 */
	if (!IS_ZERO_LSN(rep->ckp_lsn) &&
	    LOG_COMPARE(&lp->ready_lsn, &rep->ckp_lsn) > 0) {
		save_lsn = rep->ckp_lsn;
		ZERO_LSN(rep->ckp_lsn);
	} else
		ZERO_LSN(save_lsn);

	/*
	 * If this is a perm record, we are using leases, update the lease
	 * grant.  We must hold the clientdb mutex.  We must not hold
	 * the region mutex because rep_update_grant will acquire it.
	 */
	if (ret == DB_REP_ISPERM && IS_USING_LEASES(env) &&
	    timespecisset(&max_ts)) {
		if ((t_ret = __rep_update_grant(env, &max_ts)) != 0)
			ret = t_ret;
		else if (timespeccmp(&max_ts, &lp->max_lease_ts, >))
			lp->max_lease_ts = max_ts;
	}

	MUTEX_UNLOCK(env, rep->mtx_clientdb);
	if (!IS_ZERO_LSN(save_lsn)) {
		/*
		 * Now call memp_sync holding only the ckp mutex.
		 */
		MUTEX_LOCK(env, rep->mtx_ckp);
		RPRINT(env, (env, DB_VERB_REP_MISC,
		    "Starting delayed __memp_sync call [%lu][%lu]",
		    (u_long)save_lsn.file, (u_long)save_lsn.offset));
		t_ret = __memp_sync(env,
		    DB_SYNC_CHECKPOINT | DB_SYNC_INTERRUPT_OK, &save_lsn);
		MUTEX_UNLOCK(env, rep->mtx_ckp);
	}
	if (event) {
		RPRINT(env, (env, DB_VERB_REP_MISC,
		    "Start-up is done [%lu][%lu]",
		    (u_long)rp->lsn.file, (u_long)rp->lsn.offset));

		if ((t_ret = __rep_fire_startupdone(env, gen, master)) != 0) {
			DB_ASSERT(env, ret == 0 || ret == DB_REP_ISPERM);
			/* Failure trumps either of those values. */
			ret = t_ret;
			goto out;
		}
	}
	if ((ret == 0 || ret == DB_REP_ISPERM) &&
	    newfile_seen && lp->db_log_autoremove)
		__log_autoremove(env);
	if (control_dbt.data != NULL)
		__os_ufree(env, control_dbt.data);
	if (rec_dbt.data != NULL)
		__os_ufree(env, rec_dbt.data);

out:
	switch (ret) {
	case 0:
		break;
	case DB_REP_ISPERM:
		VPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Returning ISPERM [%lu][%lu], cmp = %d",
		    (u_long)max_lsn.file, (u_long)max_lsn.offset, cmp));
		break;
	case DB_REP_LOGREADY:
		RPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Returning LOGREADY up to [%lu][%lu], cmp = %d",
		    (u_long)last_lsnp->file,
		    (u_long)last_lsnp->offset, cmp));
		break;
	case DB_REP_NOTPERM:
		if (rep->sync_state != SYNC_LOG &&
		    !IS_ZERO_LSN(max_lsn) && ret_lsnp != NULL)
			*ret_lsnp = max_lsn;

		VPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Returning NOTPERM [%lu][%lu], cmp = %d",
		    (u_long)max_lsn.file, (u_long)max_lsn.offset, cmp));
		break;
	default:
		RPRINT(env, (env, DB_VERB_REP_MSGS,
		    "Returning %d [%lu][%lu], cmp = %d", ret,
		    (u_long)max_lsn.file, (u_long)max_lsn.offset, cmp));
		break;
	}

	return (ret);
}

/*
 * __rep_process_txn --
 *
 * This is the routine that actually gets a transaction ready for
 * processing.
 *
 * PUBLIC: int __rep_process_txn __P((ENV *, DBT *));
 */
int
__rep_process_txn(env, rec)
	ENV *env;
	DBT *rec;
{
	DBT data_dbt, *lock_dbt;
	DB_LOCKER *locker;
	DB_LOCKREQ req, *lvp;
	DB_LOGC *logc;
	DB_LSN prev_lsn, *lsnp;
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	DB_TXNHEAD *txninfo;
	LSN_COLLECTION lc;
	REP *rep;
	__txn_regop_args *txn_args;
	__txn_regop_42_args *txn42_args;
	__txn_prepare_args *prep_args;
	u_int32_t rectype;
	u_int i;
	int ret, t_ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	logc = NULL;
	txn_args = NULL;
	txn42_args = NULL;
	prep_args = NULL;
	txninfo = NULL;

	ENV_ENTER(env, ip);
	memset(&data_dbt, 0, sizeof(data_dbt));
	if (F_ISSET(env, ENV_THREAD))
		F_SET(&data_dbt, DB_DBT_REALLOC);

	/*
	 * There are two phases:  First, we have to traverse backwards through
	 * the log records gathering the list of all LSNs in the transaction.
	 * Once we have this information, we can loop through and then apply it.
	 *
	 * We may be passed a prepare (if we're restoring a prepare on upgrade)
	 * instead of a commit (the common case).  Check which it is and behave
	 * appropriately.
	 */
	LOGCOPY_32(env, &rectype, rec->data);
	memset(&lc, 0, sizeof(lc));
	if (rectype == DB___txn_regop) {
		/*
		 * We're the end of a transaction.  Make sure this is
		 * really a commit and not an abort!
		 */
		if (rep->version >= DB_REPVERSION_44) {
			if ((ret = __txn_regop_read(
			    env, rec->data, &txn_args)) != 0)
				return (ret);
			if (txn_args->opcode != TXN_COMMIT) {
				__os_free(env, txn_args);
				return (0);
			}
			prev_lsn = txn_args->prev_lsn;
			lock_dbt = &txn_args->locks;
		} else {
			if ((ret = __txn_regop_42_read(
			    env, rec->data, &txn42_args)) != 0)
				return (ret);
			if (txn42_args->opcode != TXN_COMMIT) {
				__os_free(env, txn42_args);
				return (0);
			}
			prev_lsn = txn42_args->prev_lsn;
			lock_dbt = &txn42_args->locks;
		}
	} else {
		/* We're a prepare. */
		DB_ASSERT(env, rectype == DB___txn_prepare);

		if ((ret = __txn_prepare_read(
		    env, rec->data, &prep_args)) != 0)
			return (ret);
		prev_lsn = prep_args->prev_lsn;
		lock_dbt = &prep_args->locks;
	}

	/* Get locks. */
	if ((ret = __lock_id(env, NULL, &locker)) != 0)
		goto err1;

	/* We are always more important than user transactions. */
	locker->priority = DB_LOCK_MAXPRIORITY;

	if ((ret =
	    __lock_get_list(env, locker, 0, DB_LOCK_WRITE, lock_dbt)) != 0)
		goto err;

	/* Phase 1.  Get a list of the LSNs in this transaction, and sort it. */
	if ((ret = __rep_collect_txn(env, &prev_lsn, &lc)) != 0)
		goto err;
	qsort(lc.array, lc.nlsns, sizeof(DB_LSN), __rep_lsn_cmp);

	/*
	 * The set of records for a transaction may include dbreg_register
	 * records.  Create a txnlist so that they can keep track of file
	 * state between records.
	 */
	if ((ret = __db_txnlist_init(env, ip, 0, 0, NULL, &txninfo)) != 0)
		goto err;

	/* Phase 2: Apply updates. */
	if ((ret = __log_cursor(env, &logc)) != 0)
		goto err;
	for (lsnp = &lc.array[0], i = 0; i < lc.nlsns; i++, lsnp++) {
		if ((ret = __logc_get(logc, lsnp, &data_dbt, DB_SET)) != 0) {
			__db_errx(env, DB_STR_A("3522",
			    "failed to read the log at [%lu][%lu]", "%lu %lu"),
			    (u_long)lsnp->file, (u_long)lsnp->offset);
			goto err;
		}
		if ((ret = __db_dispatch(env, &env->recover_dtab,
		    &data_dbt, lsnp, DB_TXN_APPLY, txninfo)) != 0) {
			__db_errx(env, DB_STR_A("3523",
			    "transaction failed at [%lu][%lu]", "%lu %lu"),
			    (u_long)lsnp->file, (u_long)lsnp->offset);
			goto err;
		}
	}

err:	memset(&req, 0, sizeof(req));
	req.op = DB_LOCK_PUT_ALL;
	if ((t_ret =
	     __lock_vec(env, locker, 0, &req, 1, &lvp)) != 0 && ret == 0)
		ret = t_ret;

	if ((t_ret = __lock_id_free(env, locker)) != 0 && ret == 0)
		ret = t_ret;

err1:	if (txn_args != NULL)
		__os_free(env, txn_args);
	if (txn42_args != NULL)
		__os_free(env, txn42_args);
	if (prep_args != NULL)
		__os_free(env, prep_args);
	if (lc.array != NULL)
		__os_free(env, lc.array);

	if (logc != NULL && (t_ret = __logc_close(logc)) != 0 && ret == 0)
		ret = t_ret;

	if (txninfo != NULL)
		__db_txnlist_end(env, txninfo);

	if (F_ISSET(&data_dbt, DB_DBT_REALLOC) && data_dbt.data != NULL)
		__os_ufree(env, data_dbt.data);

#ifdef HAVE_STATISTICS
	if (ret == 0)
		/*
		 * We don't hold the rep mutex, and could miscount if we race.
		 */
		rep->stat.st_txns_applied++;
#endif

	return (ret);
}

/*
 * __rep_collect_txn
 *	Recursive function that will let us visit every entry in a transaction
 *	chain including all child transactions so that we can then apply
 *	the entire transaction family at once.
 */
static int
__rep_collect_txn(env, lsnp, lc)
	ENV *env;
	DB_LSN *lsnp;
	LSN_COLLECTION *lc;
{
	__txn_child_args *argp;
	DB_LOGC *logc;
	DB_LSN c_lsn;
	DBT data;
	u_int32_t rectype;
	u_int nalloc;
	int ret, t_ret;

	memset(&data, 0, sizeof(data));
	F_SET(&data, DB_DBT_REALLOC);

	if ((ret = __log_cursor(env, &logc)) != 0)
		return (ret);

	while (!IS_ZERO_LSN(*lsnp) &&
	    (ret = __logc_get(logc, lsnp, &data, DB_SET)) == 0) {
		LOGCOPY_32(env, &rectype, data.data);
		if (rectype == DB___txn_child) {
			if ((ret = __txn_child_read(
			    env, data.data, &argp)) != 0)
				goto err;
			c_lsn = argp->c_lsn;
			*lsnp = argp->prev_lsn;
			__os_free(env, argp);
			ret = __rep_collect_txn(env, &c_lsn, lc);
		} else {
			if (lc->nalloc < lc->nlsns + 1) {
				nalloc = lc->nalloc == 0 ? 20 : lc->nalloc * 2;
				if ((ret = __os_realloc(env,
				    nalloc * sizeof(DB_LSN), &lc->array)) != 0)
					goto err;
				lc->nalloc = nalloc;
			}
			lc->array[lc->nlsns++] = *lsnp;

			/*
			 * Explicitly copy the previous lsn.  The record
			 * starts with a u_int32_t record type, a u_int32_t
			 * txn id, and then the DB_LSN (prev_lsn) that we
			 * want.  We copy explicitly because we have no idea
			 * what kind of record this is.
			 */
			LOGCOPY_TOLSN(env, lsnp, (u_int8_t *)data.data +
			    sizeof(u_int32_t) + sizeof(u_int32_t));
		}

		if (ret != 0)
			goto err;
	}
	if (ret != 0)
		__db_errx(env, DB_STR_A("3524",
		    "collect failed at: [%lu][%lu]", "%lu %lu"),
		    (u_long)lsnp->file, (u_long)lsnp->offset);

err:	if ((t_ret = __logc_close(logc)) != 0 && ret == 0)
		ret = t_ret;
	if (data.data != NULL)
		__os_ufree(env, data.data);
	return (ret);
}

/*
 * __rep_lsn_cmp --
 *	qsort-type-compatible wrapper for LOG_COMPARE.
 */
static int
__rep_lsn_cmp(lsn1, lsn2)
	const void *lsn1, *lsn2;
{

	return (LOG_COMPARE((DB_LSN *)lsn1, (DB_LSN *)lsn2));
}

/*
 * __rep_newfile --
 *	NEWFILE messages have the LSN of the last record in the previous
 * log file.  When applying a NEWFILE message, make sure we haven't already
 * swapped files.  Assume caller hold mtx_clientdb.
 */
static int
__rep_newfile(env, rp, rec)
	ENV *env;
	__rep_control_args *rp;
	DBT *rec;
{
	DB_LOG *dblp;
	DB_LSN tmplsn;
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	__rep_newfile_args nf_args;
	int ret;

	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	db_rep = env->rep_handle;
	rep = db_rep->region;

	/*
	 * If a newfile is already in progress, just ignore.
	 */
	if (F_ISSET(rep, REP_F_NEWFILE))
		return (0);
	if (rp->lsn.file + 1 > lp->ready_lsn.file) {
		if (rec == NULL || rec->size == 0) {
			RPRINT(env, (env, DB_VERB_REP_MISC,
"rep_newfile: Old-style NEWFILE msg.  Use control msg log version: %lu",
    (u_long) rp->log_version));
			nf_args.version = rp->log_version;
		} else if (rp->rep_version < DB_REPVERSION_47)
			nf_args.version = *(u_int32_t *)rec->data;
		else if ((ret = __rep_newfile_unmarshal(env, &nf_args,
		    rec->data, rec->size, NULL)) != 0)
			return (ret);
		RPRINT(env, (env, DB_VERB_REP_MISC,
		    "rep_newfile: File %lu vers %lu",
		    (u_long)rp->lsn.file + 1, (u_long)nf_args.version));

		/*
		 * We drop the mtx_clientdb mutex during
		 * the file operation, and then reacquire it when
		 * we're done.  We avoid colliding with new incoming
		 * log records because lp->ready_lsn is not getting
		 * updated and there is no real log record at this
		 * ready_lsn.  We avoid colliding with a duplicate
		 * NEWFILE message by setting an in-progress flag.
		 */
		REP_SYSTEM_LOCK(env);
		F_SET(rep, REP_F_NEWFILE);
		REP_SYSTEM_UNLOCK(env);
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
		LOG_SYSTEM_LOCK(env);
		ret = __log_newfile(dblp, &tmplsn, 0, nf_args.version);
		LOG_SYSTEM_UNLOCK(env);
		MUTEX_LOCK(env, rep->mtx_clientdb);
		REP_SYSTEM_LOCK(env);
		F_CLR(rep, REP_F_NEWFILE);
		REP_SYSTEM_UNLOCK(env);
		if (ret == 0)
			lp->ready_lsn = tmplsn;
		return (ret);
	} else
		/* We've already applied this NEWFILE.  Just ignore it. */
		return (0);
}

/*
 * __rep_do_ckp --
 * Perform the memp_sync necessary for this checkpoint without holding the
 * REP->mtx_clientdb.  Callers of this function must hold REP->mtx_clientdb
 * and must not be holding the region mutex.
 */
static int
__rep_do_ckp(env, rec, rp)
	ENV *env;
	DBT *rec;
	__rep_control_args *rp;
{
	DB_ENV *dbenv;
	__txn_ckp_args *ckp_args;
	DB_LSN ckp_lsn;
	REP *rep;
	int ret;

	dbenv = env->dbenv;

	/* Crack the log record and extract the checkpoint LSN. */
	if ((ret = __txn_ckp_read(env, rec->data, &ckp_args)) != 0)
		return (ret);
	ckp_lsn = ckp_args->ckp_lsn;
	__os_free(env, ckp_args);

	rep = env->rep_handle->region;

	MUTEX_UNLOCK(env, rep->mtx_clientdb);
	DB_TEST_WAIT(env, env->test_check);

	/*
	 * Sync the memory pool.
	 *
	 * This is the real PERM lock record/ckp.  We cannot return ISPERM
	 * if we haven't truly completed the checkpoint, so we don't allow
	 * this call to be interrupted.
	 *
	 * We may be overlapping our log record with an in-progress startsync
	 * of this checkpoint; suppress the max_write settings on any running
	 * cache-flush operation so it completes quickly.
	 */
	(void)__memp_set_config(dbenv, DB_MEMP_SUPPRESS_WRITE, 1);
	MUTEX_LOCK(env, rep->mtx_ckp);
	ret = __memp_sync(env, DB_SYNC_CHECKPOINT, &ckp_lsn);
	MUTEX_UNLOCK(env, rep->mtx_ckp);
	(void)__memp_set_config(dbenv, DB_MEMP_SUPPRESS_WRITE, 0);

	/* Update the last_ckp in the txn region. */
	if (ret == 0)
		ret = __txn_updateckp(env, &rp->lsn);
	else {
		__db_errx(env, DB_STR_A("3525",
		    "Error syncing ckp [%lu][%lu]", "%lu %lu"),
		    (u_long)ckp_lsn.file, (u_long)ckp_lsn.offset);
		ret = __env_panic(env, ret);
	}

	MUTEX_LOCK(env, rep->mtx_clientdb);
	return (ret);
}

/*
 * __rep_remfirst --
 * Remove the first entry from the __db.rep.db
 */
static int
__rep_remfirst(env, ip, cntrl, rec)
	ENV *env;
	DB_THREAD_INFO *ip;
	DBT *cntrl;
	DBT *rec;
{
	DB *dbp;
	DBC *dbc;
	DB_REP *db_rep;
	int ret, t_ret;

	db_rep = env->rep_handle;
	dbp = db_rep->rep_db;
	if ((ret = __db_cursor(dbp, ip, NULL, &dbc, 0)) != 0)
		return (ret);

	/* The DBTs need to persist through another call. */
	F_SET(cntrl, DB_DBT_REALLOC);
	F_SET(rec, DB_DBT_REALLOC);
	if ((ret = __dbc_get(dbc, cntrl, rec, DB_RMW | DB_FIRST)) == 0)
		ret = __dbc_del(dbc, 0);
	if ((t_ret = __dbc_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __rep_getnext --
 * Get the next record out of the __db.rep.db table.
 */
static int
__rep_getnext(env, ip)
	ENV *env;
	DB_THREAD_INFO *ip;
{
	DB *dbp;
	DBC *dbc;
	DBT lsn_dbt, nextrec_dbt;
	DB_LOG *dblp;
	DB_REP *db_rep;
	LOG *lp;
	__rep_control_args *rp;
	int ret, t_ret;

	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;

	db_rep = env->rep_handle;
	dbp = db_rep->rep_db;

	if ((ret = __db_cursor(dbp, ip, NULL, &dbc, 0)) != 0)
		return (ret);

	/*
	 * Update waiting_lsn.  We need to move it
	 * forward to the LSN of the next record
	 * in the queue.
	 *
	 * If the next item in the database is a log
	 * record--the common case--we're not
	 * interested in its contents, just in its LSN.
	 * Optimize by doing a partial get of the data item.
	 */
	memset(&nextrec_dbt, 0, sizeof(nextrec_dbt));
	F_SET(&nextrec_dbt, DB_DBT_PARTIAL);
	nextrec_dbt.ulen = nextrec_dbt.dlen = 0;

	memset(&lsn_dbt, 0, sizeof(lsn_dbt));
	ret = __dbc_get(dbc, &lsn_dbt, &nextrec_dbt, DB_FIRST);
	if (ret != DB_NOTFOUND && ret != 0)
		goto err;

	if (ret == DB_NOTFOUND) {
		ZERO_LSN(lp->waiting_lsn);
		/*
		 * Whether or not the current record is
		 * simple, there's no next one, and
		 * therefore we haven't got anything
		 * else to do right now.  Break out.
		 */
		goto err;
	}
	rp = (__rep_control_args *)lsn_dbt.data;
	lp->waiting_lsn = rp->lsn;

err:	if ((t_ret = __dbc_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __rep_process_rec --
 *
 * Given a record in 'rp', process it.  In the case of a NEWFILE, that means
 * potentially switching files.  In the case of a checkpoint, it means doing
 * the checkpoint, and in other cases, it means simply writing the record into
 * the log.
 */
static int
__rep_process_rec(env, ip, rp, rec, ret_tsp, ret_lsnp)
	ENV *env;
	DB_THREAD_INFO *ip;
	__rep_control_args *rp;
	DBT *rec;
	db_timespec *ret_tsp;
	DB_LSN *ret_lsnp;
{
	DB *dbp;
	DBT control_dbt, key_dbt, rec_dbt;
	DB_LOG *dblp;
	DB_REP *db_rep;
	DB_LOGC *logc;
	LOG *lp;
	REP *rep;
	DB_LSN lsn;
	db_timespec msg_time;
	u_int32_t rectype, txnid;
	int ret, t_ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	dbp = db_rep->rep_db;
	ret = 0;

	memset(&rec_dbt, 0, sizeof(rec_dbt));
	if (rp->rectype == REP_NEWFILE) {
		if ((ret = __rep_newfile(env, rp, rec)) != 0)
			return (ret);

		/*
		 * In SYNC_LOG, in case the end-of-log sync point happens to be
		 * right at the file boundary, we need to make sure ret_lsnp
		 * points to a real log record, rather than the "dead space" at
		 * the end of the file that the NEWFILE msg normally points to.
		 */
		if (rep->sync_state == SYNC_LOG) {
			if ((ret = __log_cursor(env, &logc)) != 0)
				return (ret);
			if ((ret = __logc_get(logc,
			    &lsn, &rec_dbt, DB_LAST)) == 0)
				*ret_lsnp = lsn;
			if ((t_ret = __logc_close(logc)) != 0 && ret == 0)
				ret = t_ret;
		}
		return (ret);
	}

	LOGCOPY_32(env, &rectype, rec->data);
	memset(&control_dbt, 0, sizeof(control_dbt));
	timespecset(&msg_time, rp->msg_sec, rp->msg_nsec);

	/*
	 * We write all records except for checkpoint records here.
	 * All non-checkpoint records need to appear in the log before
	 * we take action upon them (i.e., we enforce write-ahead logging).
	 * However, we can't write the checkpoint record here until the
	 * data buffers are actually written to disk, else we are creating
	 * an invalid log -- one that says all data before a certain point
	 * has been written to disk.
	 *
	 * If two threads are both processing the same checkpoint record
	 * (because, for example, it was resent and the original finally
	 * arrived), we handle that below by checking for the existence of
	 * the log record when we add it to the replication database.
	 *
	 * Any log records that arrive while we are processing the checkpoint
	 * are added to the bookkeeping database because ready_lsn is not yet
	 * updated to point after the checkpoint record.
	 */
	if (rectype != DB___txn_ckp || rep->sync_state == SYNC_LOG) {
		if ((ret = __log_rep_put(env, &rp->lsn, rec, 0)) != 0)
			return (ret);
		STAT(rep->stat.st_log_records++);
		if (rep->sync_state == SYNC_LOG) {
			*ret_lsnp = rp->lsn;
			goto out;
		}
	}

	switch (rectype) {
	case DB___dbreg_register:
		/*
		 * DB opens occur in the context of a transaction, so we can
		 * simply handle them when we process the transaction.  Closes,
		 * however, are not transaction-protected, so we have to handle
		 * them here.
		 *
		 * It should be unsafe for the master to do a close of a file
		 * that was opened in an active transaction, so we should be
		 * guaranteed to get the ordering right.
		 *
		 * !!!
		 * The txn ID is the second 4-byte field of the log record.
		 * We should really be calling __dbreg_register_read() and
		 * working from the __dbreg_register_args structure, but this
		 * is considerably faster and the order of the fields won't
		 * change.
		 */
		LOGCOPY_32(env, &txnid,
		    (u_int8_t *)rec->data + sizeof(u_int32_t));
		if (txnid == TXN_INVALID)
			ret = __db_dispatch(env, &env->recover_dtab,
			    rec, &rp->lsn, DB_TXN_APPLY, NULL);
		break;
	case DB___txn_regop:
		/*
		 * If an application is doing app-specific recovery
		 * and acquires locks while applying a transaction,
		 * it can deadlock.  Any other locks held by this
		 * thread should have been discarded in the
		 * __rep_process_txn error path, so if we simply
		 * retry, we should eventually succeed.
		 */
		do {
			ret = 0;
			if (!F_ISSET(db_rep, DBREP_OPENFILES)) {
				ret = __txn_openfiles(env, ip, NULL, 1);
				F_SET(db_rep, DBREP_OPENFILES);
			}
			if (ret == 0)
				ret = __rep_process_txn(env, rec);
		} while (ret == DB_LOCK_DEADLOCK || ret == DB_LOCK_NOTGRANTED);

		/* Now flush the log unless we're running TXN_NOSYNC. */
		if (ret == 0 && !F_ISSET(env->dbenv, DB_ENV_TXN_NOSYNC))
			ret = __log_flush(env, NULL);
		if (ret != 0) {
			__db_errx(env, DB_STR_A("3526",
			    "Error processing txn [%lu][%lu]", "%lu %lu"),
			    (u_long)rp->lsn.file, (u_long)rp->lsn.offset);
			ret = __env_panic(env, ret);
		}
		*ret_lsnp = rp->lsn;
		break;
	case DB___txn_prepare:
		ret = __log_flush(env, NULL);
		/*
		 * Save the biggest prepared LSN we've seen.
		 */
		rep->max_prep_lsn = rp->lsn;
		VPRINT(env, (env, DB_VERB_REP_MSGS,
		    "process_rec: prepare at [%lu][%lu]",
		    (u_long)rep->max_prep_lsn.file,
		    (u_long)rep->max_prep_lsn.offset));
		break;
	case DB___txn_ckp:
		/*
		 * We do not want to hold the REP->mtx_clientdb mutex while
		 * syncing the mpool, so if we get a checkpoint record we are
		 * supposed to process, add it to the __db.rep.db, do the
		 * memp_sync and then go back and process it later, when the
		 * sync has finished.  If this record is already in the table,
		 * then some other thread will process it, so simply return
		 * REP_NOTPERM.
		 */
		memset(&key_dbt, 0, sizeof(key_dbt));
		key_dbt.data = rp;
		key_dbt.size = sizeof(*rp);

		/*
		 * We want to put this record into the tmp DB only if
		 * it doesn't exist, so use DB_NOOVERWRITE.
		 */
		ret = __db_put(dbp, ip, NULL, &key_dbt, rec, DB_NOOVERWRITE);
		if (ret == DB_KEYEXIST) {
			if (ret_lsnp != NULL)
				*ret_lsnp = rp->lsn;
			ret = DB_REP_NOTPERM;
		}
		if (ret != 0)
			break;

		/*
		 * Now, do the checkpoint.  Regardless of
		 * whether the checkpoint succeeds or not,
		 * we need to remove the record we just put
		 * in the temporary database.  If the
		 * checkpoint failed, return an error.  We
		 * will act like we never received the
		 * checkpoint.
		 */
		if ((ret = __rep_do_ckp(env, rec, rp)) == 0)
			ret = __log_rep_put(env, &rp->lsn, rec,
			    DB_LOG_CHKPNT);
		if ((t_ret = __rep_remfirst(env, ip,
		    &control_dbt, &rec_dbt)) != 0 && ret == 0)
			ret = t_ret;
		/*
		 * If we're successful putting the log record in the
		 * log, flush it for a checkpoint.
		 */
		if (ret == 0) {
			*ret_lsnp = rp->lsn;
			ret = __log_flush(env, NULL);
			if (ret == 0 && lp->db_log_autoremove)
				__log_autoremove(env);
		}
		break;
	default:
		break;
	}

out:
	if (ret == 0 && F_ISSET(rp, REPCTL_PERM))
		*ret_lsnp = rp->lsn;
	if (IS_USING_LEASES(env) &&
	    F_ISSET(rp, REPCTL_LEASE))
		*ret_tsp = msg_time;
	/*
	 * Set ret_lsnp before flushing the log because if the
	 * flush fails, we've still written the record to the
	 * log and the LSN has been entered.
	 */
	if (ret == 0 && F_ISSET(rp, REPCTL_FLUSH))
		ret = __log_flush(env, NULL);
	if (control_dbt.data != NULL)
		__os_ufree(env, control_dbt.data);
	if (rec_dbt.data != NULL)
		__os_ufree(env, rec_dbt.data);

	return (ret);
}

/*
 * __rep_resend_req --
 *	We might have dropped a message, we need to resend our request.
 *	The request we send is dependent on what recovery state we're in.
 *	The caller holds no locks.
 *
 * PUBLIC: int __rep_resend_req __P((ENV *, int));
 */
int
__rep_resend_req(env, rereq)
	ENV *env;
	int rereq;
{
	DB_LOG *dblp;
	DB_LSN lsn, *lsnp;
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	int master, ret;
	repsync_t sync_state;
	u_int32_t gapflags, msgtype, repflags, sendflags;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	ret = 0;
	lsnp = NULL;
	msgtype = REP_INVALID;
	sendflags = 0;

	repflags = rep->flags;
	sync_state = rep->sync_state;
	/*
	 * If we are delayed we do not rerequest anything.
	 */
	if (FLD_ISSET(repflags, REP_F_DELAY))
		return (ret);
	gapflags = rereq ? REP_GAP_REREQUEST : 0;

	if (sync_state == SYNC_VERIFY) {
		MUTEX_LOCK(env, rep->mtx_clientdb);
		lsn = lp->verify_lsn;
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
		if (!IS_ZERO_LSN(lsn)) {
			msgtype = REP_VERIFY_REQ;
			lsnp = &lsn;
			sendflags = DB_REP_REREQUEST;
		}
	} else if (sync_state == SYNC_UPDATE) {
		/*
		 * UPDATE_REQ only goes to the master.
		 */
		msgtype = REP_UPDATE_REQ;
	} else if (sync_state == SYNC_PAGE) {
		REP_SYSTEM_LOCK(env);
		ret = __rep_pggap_req(env, rep, NULL, gapflags);
		REP_SYSTEM_UNLOCK(env);
	} else {
		MUTEX_LOCK(env, rep->mtx_clientdb);
		ret = __rep_loggap_req(env, rep, NULL, gapflags);
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
	}

	if (msgtype != REP_INVALID) {
		master = rep->master_id;
		if (master == DB_EID_INVALID)
			(void)__rep_send_message(env,
			    DB_EID_BROADCAST, REP_MASTER_REQ, NULL, NULL, 0, 0);
		else
			(void)__rep_send_message(env,
			    master, msgtype, lsnp, NULL, 0, sendflags);
	}

	return (ret);
}

/*
 * __rep_check_doreq --
 * PUBLIC: int __rep_check_doreq __P((ENV *, REP *));
 *
 * Check if we need to send another request.  If so, compare with
 * the request limits the user might have set.  This assumes the
 * caller holds the REP->mtx_clientdb mutex.  Returns 1 if a request
 * needs to be made, and 0 if it does not.
 */
int
__rep_check_doreq(env, rep)
	ENV *env;
	REP *rep;
{

	DB_LOG *dblp;
	LOG *lp;
	db_timespec now;
	int req;

	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	__os_gettime(env, &now, 1);
	timespecsub(&now, &lp->rcvd_ts);
	req = timespeccmp(&now, &lp->wait_ts, >=);
	if (req) {
		/*
		 * Add wait_ts to itself to double it.
		 */
		timespecadd(&lp->wait_ts, &lp->wait_ts);
		if (timespeccmp(&lp->wait_ts, &rep->max_gap, >))
			lp->wait_ts = rep->max_gap;
		__os_gettime(env, &lp->rcvd_ts, 1);
	}
	return (req);
}

/*
 * __rep_skip_msg -
 *
 *	If we're in recovery we want to skip/ignore the message, but
 *	we also need to see if we need to re-request any retransmissions.
 */
static int
__rep_skip_msg(env, rep, eid, rectype)
	ENV *env;
	REP *rep;
	int eid;
	u_int32_t rectype;
{
	int do_req, ret;

	ret = 0;
	/*
	 * If we have a request message from a client then immediately
	 * send a REP_REREQUEST back to that client since we're skipping it.
	 */
	if (F_ISSET(rep, REP_F_CLIENT) && REP_MSG_REQ(rectype))
		do_req = 1;
	else {
		/* Check for need to retransmit. */
		MUTEX_LOCK(env, rep->mtx_clientdb);
		do_req = __rep_check_doreq(env, rep);
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
	}
	/*
	 * Don't respond to a MASTER_REQ with
	 * a MASTER_REQ or REREQUEST.
	 */
	if (do_req && rectype != REP_MASTER_REQ) {
		/*
		 * There are three cases:
		 * 1.  If we don't know who the master is, then send MASTER_REQ.
		 * 2.  If the message we're skipping came from the master,
		 * then we need to rerequest.
		 * 3.  If the message didn't come from a master (i.e. client
		 * to client), then send a rerequest back to the sender so
		 * the sender can rerequest it elsewhere, if we are a client.
		 */
		if (rep->master_id == DB_EID_INVALID)	/* Case 1. */
			(void)__rep_send_message(env,
			    DB_EID_BROADCAST, REP_MASTER_REQ, NULL, NULL, 0, 0);
		else if (eid == rep->master_id)		/* Case 2. */
			ret = __rep_resend_req(env, 0);
		else if (F_ISSET(rep, REP_F_CLIENT))	/* Case 3. */
			(void)__rep_send_message(env,
			    eid, REP_REREQUEST, NULL, NULL, 0, 0);
	}
	return (ret);
}

/*
 * __rep_check_missing --
 * PUBLIC: int __rep_check_missing __P((ENV *, u_int32_t, DB_LSN *));
 *
 * Check for and request any missing client information.
 */
int
__rep_check_missing(env, gen, master_perm_lsn)
	ENV *env;
	u_int32_t gen;
	DB_LSN *master_perm_lsn;
{
	DB_LOG *dblp;
	DB_LSN *end_lsn;
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	LOG *lp;
	REGINFO *infop;
	REP *rep;
	__rep_fileinfo_args *curinfo;
	int do_req, has_log_gap, has_page_gap, ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	infop = env->reginfo;
	has_log_gap = has_page_gap = ret = 0;

	ENV_ENTER(env, ip);
	MUTEX_LOCK(env, rep->mtx_clientdb);
	REP_SYSTEM_LOCK(env);
	/*
	 * Check if we are okay to proceed with this operation.  If not,
	 * do not rerequest anything.
	 */
	if (!F_ISSET(rep, REP_F_CLIENT) || rep->master_id == DB_EID_INVALID ||
	    gen != rep->gen || FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_MSG)) {
		REP_SYSTEM_UNLOCK(env);
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
		/*
		 * If this client is out-of-date, ask the master to identify
		 * itself so that this client will synchronize with the
		 * master's later generation.
		 */
		if (gen > rep->gen && __rep_check_doreq(env, rep))
			(void)__rep_send_message(env,
			    DB_EID_BROADCAST, REP_MASTER_REQ,
			    NULL, NULL, 0, 0);
		goto out;
	}

	/*
	 * Prevent message lockout by counting ourself here.
	 * Setting rep->msg_th will prevent a major system
	 * change, such as a role change or running recovery, from
	 * occurring before sending out any rerequests.
	 */
	rep->msg_th++;
	REP_SYSTEM_UNLOCK(env);

	/* Check that it is time to request missing information. */
	if ((do_req = __rep_check_doreq(env, rep))) {
		/* Check for interior or tail page gap. */
		REP_SYSTEM_LOCK(env);
		if (rep->sync_state == SYNC_PAGE &&
		    rep->curinfo_off != INVALID_ROFF) {
			GET_CURINFO(rep, infop, curinfo);
			has_page_gap =
			    rep->waiting_pg != PGNO_INVALID ||
			    rep->ready_pg <= curinfo->max_pgno;
		}
		REP_SYSTEM_UNLOCK(env);
	}
	/* Check for interior or tail log gap. */
	if (do_req && !has_page_gap) {
		lp = dblp->reginfo.primary;
		/*
		 * The LOG_COMPARE test is <= because ready_lsn is
		 * the next LSN we are expecting but we do not have
		 * it yet.  If the needed LSN is at this LSN, it
		 * means we are missing the last record we need.
		 */
		if (rep->sync_state == SYNC_LOG)
			end_lsn = &rep->last_lsn;
		else
			end_lsn = master_perm_lsn;
		has_log_gap = !IS_ZERO_LSN(lp->waiting_lsn) ||
		    LOG_COMPARE(&lp->ready_lsn, end_lsn) <= 0;
	}
	MUTEX_UNLOCK(env, rep->mtx_clientdb);
	/*
	 * If it is time to send a request, only do so if we
	 * have a log gap or a page gap, or we need to resend an
	 * UPDATE_REQ or VERIFY_REQ, or we are in SYNC_LOG to keep
	 * requesting to the current known end of the log.
	 */
	do_req = do_req && (has_log_gap || has_page_gap ||
	    rep->sync_state == SYNC_LOG ||
	    rep->sync_state == SYNC_UPDATE ||
	    rep->sync_state == SYNC_VERIFY);
	/*
	 * Determines request type from current replication
	 * state and resends request.  The request may have
	 * the DB_REP_ANYWHERE flag enabled if appropriate.
	 */
	if (do_req)
		ret = __rep_resend_req(env, 0);

	REP_SYSTEM_LOCK(env);
	rep->msg_th--;
	REP_SYSTEM_UNLOCK(env);

out:	ENV_LEAVE(env, ip);
	return (ret);
}

static int
__rep_fire_newmaster(env, gen, master)
	ENV *env;
	u_int32_t gen;
	int master;
{
	DB_REP *db_rep;
	REP *rep;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	REP_EVENT_LOCK(env);
	/*
	 * The firing of this event should be idempotent with respect to a
	 * particular generation number.
	 */
	if (rep->newmaster_event_gen < gen) {
		__rep_fire_event(env, DB_EVENT_REP_NEWMASTER, &master);
		rep->newmaster_event_gen = gen;
	}
	REP_EVENT_UNLOCK(env);
	return (0);
}

static int
__rep_fire_startupdone(env, gen, master)
	ENV *env;
	u_int32_t gen;
	int master;
{
	DB_REP *db_rep;
	REP *rep;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	REP_EVENT_LOCK(env);
	/*
	 * Usually NEWMASTER will already have been fired.  But if not, fire
	 * it here now, to ensure the application receives events in the
	 * expected order.
	 */
	if (rep->newmaster_event_gen < gen) {
		__rep_fire_event(env, DB_EVENT_REP_NEWMASTER, &master);
		rep->newmaster_event_gen = gen;
	}

	/*
	 * Caller already ensures that it only tries to fire STARTUPDONE once
	 * per generation.  If we did not want to rely on that, we could add a
	 * simple boolean flag (to the set of data protected by the mtx_event).
	 * The precise meaning of that flag would be "STARTUPDONE has been fired
	 * for the generation value stored in `newmaster_event_gen'".  Then the
	 * more accurate test here would be simply to check that flag, and fire
	 * the event (and set the flag) if it were not already set.
	 */
	if (rep->newmaster_event_gen == gen)
		__rep_fire_event(env, DB_EVENT_REP_STARTUPDONE, NULL);
	REP_EVENT_UNLOCK(env);
	return (0);
}
