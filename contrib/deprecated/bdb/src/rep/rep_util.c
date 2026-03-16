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
#include "dbinc/mp.h"
#include "dbinc/txn.h"

#ifdef REP_DIAGNOSTIC
#include "dbinc/db_page.h"
#include "dbinc/fop.h"
#include "dbinc/btree.h"
#include "dbinc/hash.h"
#include "dbinc/qam.h"
#endif

/*
 * rep_util.c:
 *	Miscellaneous replication-related utility functions, including
 *	those called by other subsystems.
 */
#define	TIMESTAMP_CHECK(env, ts, renv) do {				\
	if (renv->op_timestamp != 0 &&					\
	    renv->op_timestamp + DB_REGENV_TIMEOUT < ts) {		\
		REP_SYSTEM_LOCK(env);					\
		F_CLR(renv, DB_REGENV_REPLOCKED);			\
		renv->op_timestamp = 0;					\
		REP_SYSTEM_UNLOCK(env);					\
	}								\
} while (0)

static int __rep_lockout_int __P((ENV *, REP *, u_int32_t *, u_int32_t,
    const char *, u_int32_t));
static int __rep_newmaster_empty __P((ENV *, int));
static int __rep_print_int __P((ENV *, u_int32_t, const char *, va_list));
#ifdef REP_DIAGNOSTIC
static void __rep_print_logmsg __P((ENV *, const DBT *, DB_LSN *));
#endif
static int __rep_show_progress __P((ENV *, const char *, int mins));

/*
 * __rep_bulk_message --
 *	This is a wrapper for putting a record into a bulk buffer.  Since
 * we have different bulk buffers, the caller must hand us the information
 * we need to put the record into the correct buffer.  All bulk buffers
 * are protected by the REP->mtx_clientdb.
 *
 * PUBLIC: int __rep_bulk_message __P((ENV *, REP_BULK *, REP_THROTTLE *,
 * PUBLIC:     DB_LSN *, const DBT *, u_int32_t));
 */
int
__rep_bulk_message(env, bulk, repth, lsn, dbt, flags)
	ENV *env;
	REP_BULK *bulk;
	REP_THROTTLE *repth;
	DB_LSN *lsn;
	const DBT *dbt;
	u_int32_t flags;
{
	DB_REP *db_rep;
	REP *rep;
	__rep_bulk_args b_args;
	size_t len;
	int ret;
	u_int32_t recsize, typemore;
	u_int8_t *p;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	ret = 0;

	/*
	 * Figure out the total number of bytes needed for this record.
	 * !!! The marshalling code includes the given len, but also
	 * puts its own copy of the dbt->size with the DBT portion of
	 * the record.  Account for that here.
	 */
	recsize = sizeof(len) + dbt->size + sizeof(DB_LSN) + sizeof(dbt->size);

	/*
	 * If *this* buffer is actively being transmitted, don't wait,
	 * just return so that it can be sent as a singleton.
	 */
	MUTEX_LOCK(env, rep->mtx_clientdb);
	if (FLD_ISSET(*(bulk->flagsp), BULK_XMIT)) {
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
		return (DB_REP_BULKOVF);
	}

	/*
	 * If the record is bigger than the buffer entirely, send the
	 * current buffer and then return DB_REP_BULKOVF so that this
	 * record is sent as a singleton.  Do we have enough info to
	 * do that here?  XXX
	 */
	if (recsize > bulk->len) {
		RPRINT(env, (env, DB_VERB_REP_MSGS,
		    "bulk_msg: Record %d (0x%x) larger than entire buffer 0x%x",
		    recsize, recsize, bulk->len));
		STAT(rep->stat.st_bulk_overflows++);
		(void)__rep_send_bulk(env, bulk, flags);
		/*
		 * XXX __rep_send_message...
		 */
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
		return (DB_REP_BULKOVF);
	}
	/*
	 * If this record doesn't fit, send the current buffer.
	 * Sending the buffer will reset the offset, but we will
	 * drop the mutex while sending so we need to keep checking
	 * if we're racing.
	 */
	while (recsize + *(bulk->offp) > bulk->len) {
		RPRINT(env, (env, DB_VERB_REP_MSGS,
	    "bulk_msg: Record %lu (%#lx) doesn't fit.  Send %lu (%#lx) now.",
		    (u_long)recsize, (u_long)recsize,
		    (u_long)bulk->len, (u_long)bulk->len));
		STAT(rep->stat.st_bulk_fills++);
		if ((ret = __rep_send_bulk(env, bulk, flags)) != 0) {
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			return (ret);
		}
	}

	/*
	 * If we're using throttling, see if we are at the throttling
	 * limit before we do any more work here, by checking if the
	 * call to rep_send_throttle changed the repth->type to the
	 * *_MORE message type.  If the throttling code hits the limit
	 * then we're done here.
	 */
	if (bulk->type == REP_BULK_LOG)
		typemore = REP_LOG_MORE;
	else
		typemore = REP_PAGE_MORE;
	if (repth != NULL) {
		if ((ret = __rep_send_throttle(env,
		    bulk->eid, repth, REP_THROTTLE_ONLY, flags)) != 0) {
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			return (ret);
		}
		if (repth->type == typemore) {
			VPRINT(env, (env, DB_VERB_REP_MSGS,
			    "bulk_msg: Record %lu (0x%lx) hit throttle limit.",
			    (u_long)recsize, (u_long)recsize));
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			return (ret);
		}
	}

	/*
	 * Now we own the buffer, and we know our record fits into it.
	 * The buffer is structured with the len, LSN and then the record.
	 * Copy the record into the buffer.  Then if we need to,
	 * send the buffer.
	 */
	p = bulk->addr + *(bulk->offp);
	b_args.len = dbt->size;
	b_args.lsn = *lsn;
	b_args.bulkdata = *dbt;
	/*
	 * If we're the first record, we need to save the first
	 * LSN in the bulk structure.
	 */
	if (*(bulk->offp) == 0)
		bulk->lsn = *lsn;
	if (rep->version < DB_REPVERSION_47) {
		len = 0;
		memcpy(p, &dbt->size, sizeof(dbt->size));
		p += sizeof(dbt->size);
		memcpy(p, lsn, sizeof(DB_LSN));
		p += sizeof(DB_LSN);
		memcpy(p, dbt->data, dbt->size);
		p += dbt->size;
	} else if ((ret = __rep_bulk_marshal(env, &b_args, p,
	    bulk->len, &len)) != 0)
		goto err;
	*(bulk->offp) = (roff_t)(p + len - bulk->addr);
	STAT(rep->stat.st_bulk_records++);
	/*
	 * Send the buffer if it is a perm record or a force.
	 */
	if (LF_ISSET(REPCTL_PERM)) {
		VPRINT(env, (env, DB_VERB_REP_MSGS,
		    "bulk_msg: Send buffer after copy due to PERM"));
		ret = __rep_send_bulk(env, bulk, flags);
	}
err:
	MUTEX_UNLOCK(env, rep->mtx_clientdb);
	return (ret);

}

/*
 * __rep_send_bulk --
 *	This function transmits the bulk buffer given.  It assumes the
 * caller holds the REP->mtx_clientdb.  We may release it and reacquire
 * it during this call.  We will return with it held.
 *
 * PUBLIC: int __rep_send_bulk __P((ENV *, REP_BULK *, u_int32_t));
 */
int
__rep_send_bulk(env, bulkp, ctlflags)
	ENV *env;
	REP_BULK *bulkp;
	u_int32_t ctlflags;
{
	DBT dbt;
	DB_REP *db_rep;
	REP *rep;
	int ret;

	/*
	 * If the offset is 0, we're done.  There is nothing to send.
	 */
	if (*(bulkp->offp) == 0)
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;

	/*
	 * Set that this buffer is being actively transmitted.
	 */
	FLD_SET(*(bulkp->flagsp), BULK_XMIT);
	DB_INIT_DBT(dbt, bulkp->addr, *(bulkp->offp));
	MUTEX_UNLOCK(env, rep->mtx_clientdb);
	VPRINT(env, (env, DB_VERB_REP_MSGS,
	    "send_bulk: Send %d (0x%x) bulk buffer bytes", dbt.size, dbt.size));

	/*
	 * Unlocked the mutex and now send the message.
	 */
	STAT(rep->stat.st_bulk_transfers++);
	if ((ret = __rep_send_message(env,
	    bulkp->eid, bulkp->type, &bulkp->lsn, &dbt, ctlflags, 0)) != 0)
		ret = DB_REP_UNAVAIL;

	MUTEX_LOCK(env, rep->mtx_clientdb);
	/*
	 * Ready the buffer for further records.
	 */
	*(bulkp->offp) = 0;
	FLD_CLR(*(bulkp->flagsp), BULK_XMIT);
	return (ret);
}

/*
 * __rep_bulk_alloc --
 *	This function allocates and initializes an internal bulk buffer.
 * This is used by the master when fulfilling a request for a chunk of
 * log records or a bunch of pages.
 *
 * PUBLIC: int __rep_bulk_alloc __P((ENV *, REP_BULK *, int, uintptr_t *,
 * PUBLIC:    u_int32_t *, u_int32_t));
 */
int
__rep_bulk_alloc(env, bulkp, eid, offp, flagsp, type)
	ENV *env;
	REP_BULK *bulkp;
	int eid;
	uintptr_t *offp;
	u_int32_t *flagsp, type;
{
	int ret;

	memset(bulkp, 0, sizeof(REP_BULK));
	*offp = *flagsp = 0;
	bulkp->len = MEGABYTE;
	if ((ret = __os_malloc(env, bulkp->len, &bulkp->addr)) != 0)
		return (ret);

	/*
	 * The cast is safe because offp is an "out" parameter. The value
	 * of offp is meaningless when calling __rep_bulk_alloc.
	 */
	bulkp->offp = (roff_t *)offp;
	bulkp->type = type;
	bulkp->eid = eid;
	bulkp->flagsp = flagsp;
	return (ret);
}

/*
 * __rep_bulk_free --
 *	This function sends the remainder of the bulk buffer and frees it.
 *
 * PUBLIC: int __rep_bulk_free __P((ENV *, REP_BULK *, u_int32_t));
 */
int
__rep_bulk_free(env, bulkp, flags)
	ENV *env;
	REP_BULK *bulkp;
	u_int32_t flags;
{
	DB_REP *db_rep;
	int ret;

	db_rep = env->rep_handle;

	MUTEX_LOCK(env, db_rep->region->mtx_clientdb);
	ret = __rep_send_bulk(env, bulkp, flags);
	MUTEX_UNLOCK(env, db_rep->region->mtx_clientdb);
	__os_free(env, bulkp->addr);
	return (ret);
}

/*
 * __rep_send_message --
 *	This is a wrapper for sending a message.  It takes care of constructing
 * the control structure and calling the user's specified send function.
 *
 * PUBLIC: int __rep_send_message __P((ENV *, int,
 * PUBLIC:     u_int32_t, DB_LSN *, const DBT *, u_int32_t, u_int32_t));
 */
int
__rep_send_message(env, eid, rtype, lsnp, dbt, ctlflags, repflags)
	ENV *env;
	int eid;
	u_int32_t rtype;
	DB_LSN *lsnp;
	const DBT *dbt;
	u_int32_t ctlflags, repflags;
{
	DBT cdbt, scrap_dbt;
	DB_ENV *dbenv;
	DB_LOG *dblp;
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	REP_46_CONTROL cntrl46;
	REP_OLD_CONTROL ocntrl;
	__rep_control_args cntrl;
	db_timespec msg_time;
	int ret;
	u_int32_t myflags;
	u_int8_t buf[__REP_CONTROL_SIZE];
	size_t len;

	dbenv = env->dbenv;
	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	ret = 0;

#if defined(DEBUG_ROP) || defined(DEBUG_WOP)
	if (db_rep->send == NULL)
		return (0);
#endif

	/* Set up control structure. */
	memset(&cntrl, 0, sizeof(cntrl));
	memset(&ocntrl, 0, sizeof(ocntrl));
	memset(&cntrl46, 0, sizeof(cntrl46));
	if (lsnp == NULL)
		ZERO_LSN(cntrl.lsn);
	else
		cntrl.lsn = *lsnp;
	/*
	 * Set the rectype based on the version we need to speak.
	 */
	if (rep->version == DB_REPVERSION)
		cntrl.rectype = rtype;
	else if (rep->version < DB_REPVERSION) {
		cntrl.rectype = __rep_msg_to_old(rep->version, rtype);
		VPRINT(env, (env, DB_VERB_REP_MSGS,
		    "rep_send_msg: rtype %lu to version %lu record %lu.",
		    (u_long)rtype, (u_long)rep->version,
		    (u_long)cntrl.rectype));
		if (cntrl.rectype == REP_INVALID)
			return (ret);
	} else {
		__db_errx(env, DB_STR_A("3503",
    "rep_send_message: Unknown rep version %lu, my version %lu",
		    "%lu %lu"), (u_long)rep->version, (u_long)DB_REPVERSION);
		return (__env_panic(env, EINVAL));
	}
	cntrl.flags = ctlflags;
	cntrl.rep_version = rep->version;
	cntrl.log_version = lp->persist.version;
	cntrl.gen = rep->gen;

	/* Don't assume the send function will be tolerant of NULL records. */
	if (dbt == NULL) {
		memset(&scrap_dbt, 0, sizeof(DBT));
		dbt = &scrap_dbt;
	}

	/*
	 * There are several types of records: commit and checkpoint records
	 * that affect database durability, regular log records that might
	 * be buffered on the master before being transmitted, and control
	 * messages which don't require the guarantees of permanency, but
	 * should not be buffered.
	 *
	 * There are request records that can be sent anywhere, and there
	 * are rerequest records that the app might want to send to the master.
	 */
	myflags = repflags;
	if (FLD_ISSET(ctlflags, REPCTL_PERM)) {
		/*
		 * When writing to a system database, skip setting the PERMANENT
		 * flag.  We don't care; we don't want to wait; and the
		 * application shouldn't be distracted/confused in case there is
		 * a failure.
		 */
		if (!F_ISSET(rep, REP_F_SYS_DB_OP))
			myflags |= DB_REP_PERMANENT;
	} else if (rtype != REP_LOG || FLD_ISSET(ctlflags, REPCTL_RESEND))
		myflags |= DB_REP_NOBUFFER;

	/*
	 * Let everyone know if we've been in an established group.
	 */
	if (F_ISSET(rep, REP_F_GROUP_ESTD))
		F_SET(&cntrl, REPCTL_GROUP_ESTD);

	/*
	 * If we are a master sending a perm record, then set the
	 * REPCTL_LEASE flag to have the client reply.  Also set
	 * the start time that the client will echo back to us.
	 *
	 * !!! If we are a master, using leases, we had better not be
	 * sending to an older version.
	 */
	if (IS_REP_MASTER(env) && IS_USING_LEASES(env) &&
	    FLD_ISSET(ctlflags, REPCTL_LEASE | REPCTL_PERM)) {
		F_SET(&cntrl, REPCTL_LEASE);
		DB_ASSERT(env, rep->version == DB_REPVERSION);
		__os_gettime(env, &msg_time, 1);
		cntrl.msg_sec = (u_int32_t)msg_time.tv_sec;
		cntrl.msg_nsec = (u_int32_t)msg_time.tv_nsec;
	}

	REP_PRINT_MESSAGE(env, eid, &cntrl, "rep_send_message", myflags);
#ifdef REP_DIAGNOSTIC
	if (FLD_ISSET(
	    env->dbenv->verbose, DB_VERB_REP_MSGS) && rtype == REP_LOG)
		__rep_print_logmsg(env, dbt, lsnp);
#endif

	/*
	 * If DB_REP_PERMANENT is set, the LSN better be non-zero.
	 */
	DB_ASSERT(env, !FLD_ISSET(myflags, DB_REP_PERMANENT) ||
	    !IS_ZERO_LSN(cntrl.lsn));

	/*
	 * If we're talking to an old version, send an old control structure.
	 */
	memset(&cdbt, 0, sizeof(cdbt));
	if (rep->version <= DB_REPVERSION_45) {
		if (rep->version == DB_REPVERSION_45 &&
		    F_ISSET(&cntrl, REPCTL_INIT)) {
			F_CLR(&cntrl, REPCTL_INIT);
			F_SET(&cntrl, REPCTL_INIT_45);
		}
		ocntrl.rep_version = cntrl.rep_version;
		ocntrl.log_version = cntrl.log_version;
		ocntrl.lsn = cntrl.lsn;
		ocntrl.rectype = cntrl.rectype;
		ocntrl.gen = cntrl.gen;
		ocntrl.flags = cntrl.flags;
		cdbt.data = &ocntrl;
		cdbt.size = sizeof(ocntrl);
	} else if (rep->version == DB_REPVERSION_46) {
		cntrl46.rep_version = cntrl.rep_version;
		cntrl46.log_version = cntrl.log_version;
		cntrl46.lsn = cntrl.lsn;
		cntrl46.rectype = cntrl.rectype;
		cntrl46.gen = cntrl.gen;
		cntrl46.msg_time.tv_sec = (time_t)cntrl.msg_sec;
		cntrl46.msg_time.tv_nsec = (long)cntrl.msg_nsec;
		cntrl46.flags = cntrl.flags;
		cdbt.data = &cntrl46;
		cdbt.size = sizeof(cntrl46);
	} else {
		(void)__rep_control_marshal(env, &cntrl, buf,
		    __REP_CONTROL_SIZE, &len);
		DB_INIT_DBT(cdbt, buf, len);
	}

	/*
	 * We set the LSN above to something valid.  Give the master the
	 * actual LSN so that they can coordinate with permanent records from
	 * the client if they want to.
	 *
	 * !!! Even though we marshalled the control message for transmission,
	 * give the transport function the real LSN.
	 */
	ret = db_rep->send(dbenv, &cdbt, dbt, &cntrl.lsn, eid, myflags);

	/*
	 * We don't hold the rep lock, so this could miscount if we race.
	 * I don't think it's worth grabbing the mutex for that bit of
	 * extra accuracy.
	 */
	if (ret != 0) {
		RPRINT(env, (env, DB_VERB_REP_MSGS,
		    "rep_send_function returned: %d", ret));
#ifdef HAVE_STATISTICS
		rep->stat.st_msgs_send_failures++;
	} else
		rep->stat.st_msgs_sent++;
#else
	}
#endif
	return (ret);
}

#ifdef REP_DIAGNOSTIC
/*
 * __rep_print_logmsg --
 *	This is a debugging routine for printing out log records that
 * we are about to transmit to a client.
 */
static void
__rep_print_logmsg(env, logdbt, lsnp)
	ENV *env;
	const DBT *logdbt;
	DB_LSN *lsnp;
{
	static int first = 1;
	static DB_DISTAB dtab;

	if (first) {
		first = 0;

		(void)__bam_init_print(env, &dtab);
		(void)__crdel_init_print(env, &dtab);
		(void)__db_init_print(env, &dtab);
		(void)__dbreg_init_print(env, &dtab);
		(void)__fop_init_print(env, &dtab);
		(void)__ham_init_print(env, &dtab);
		(void)__qam_init_print(env, &dtab);
		(void)__repmgr_init_print(env, &dtab);
		(void)__txn_init_print(env, &dtab);
	}

	(void)__db_dispatch(
	    env, &dtab, (DBT *)logdbt, lsnp, DB_TXN_PRINT, NULL);
}
#endif

/*
 * __rep_new_master --
 *	Called after a master election to sync back up with a new master.
 * It's possible that we already know of this new master in which case
 * we don't need to do anything.
 *
 * This is written assuming that this message came from the master; we
 * need to enforce that in __rep_process_record, but right now, we have
 * no way to identify the master.
 *
 * PUBLIC: int __rep_new_master __P((ENV *, __rep_control_args *, int));
 */
int
__rep_new_master(env, cntrl, eid)
	ENV *env;
	__rep_control_args *cntrl;
	int eid;
{
	DBT dbt;
	DB_LOG *dblp;
	DB_LOGC *logc;
	DB_LSN first_lsn, lsn;
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	LOG *lp;
	REGENV *renv;
	REGINFO *infop;
	REP *rep;
	db_timeout_t lease_to;
	u_int32_t unused, vers;
	int change, do_req, lockout_msg, ret, t_ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	ret = 0;
	logc = NULL;
	lockout_msg = 0;
	REP_SYSTEM_LOCK(env);
	change = rep->gen != cntrl->gen || rep->master_id != eid;
	/*
	 * If we're hearing from a current or new master, then we
	 * want to clear EPHASE0 in case this site is waiting to
	 * hear from the master.
	 */
	FLD_CLR(rep->elect_flags, REP_E_PHASE0);
	if (change) {
		/*
		 * If we are already locking out others, we're either
		 * in the middle of sync-up recovery or internal init
		 * when this newmaster comes in (we also lockout in
		 * rep_start, but we cannot be racing that because we
		 * don't allow rep_proc_msg when rep_start is going on).
		 *
		 * We're about to become the client of a new master.  Since we
		 * want to be able to sync with the new master as quickly as
		 * possible, interrupt any STARTSYNC from the old master.  The
		 * new master may need to rely on acks from us and the old
		 * STARTSYNC is now irrelevant.
		 *
		 * Note that, conveniently, the "lockout_msg" flag defines the
		 * section of this code path during which both "message lockout"
		 * and "memp sync interrupt" are in effect.
		 */
		if (FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_MSG))
			goto lckout;

		if ((ret = __rep_lockout_msg(env, rep, 1)) != 0)
			goto errlck;

		(void)__memp_set_config(env->dbenv, DB_MEMP_SYNC_INTERRUPT, 1);
		lockout_msg = 1;
		/*
		 * We must wait any remaining lease time before accepting
		 * this new master.  This must be after the lockout above
		 * so that no new message can be processed and re-grant
		 * the lease out from under us.
		 */
		if (IS_USING_LEASES(env) &&
		    ((lease_to = __rep_lease_waittime(env)) != 0)) {
			REP_SYSTEM_UNLOCK(env);
			__os_yield(env, 0, (u_long)lease_to);
			REP_SYSTEM_LOCK(env);
			F_SET(rep, REP_F_LEASE_EXPIRED);
		}

		vers = lp->persist.version;
		if (cntrl->log_version != vers) {
			/*
			 * Set everything up to the lower version.  If we're
			 * going to be upgrading to the latest version that
			 * can happen automatically as we process later log
			 * records.  We likely want to sync to earlier version.
			 */
			DB_ASSERT(env, vers != 0);
			if (cntrl->log_version < vers)
				vers = cntrl->log_version;
			RPRINT(env, (env, DB_VERB_REP_MISC,
			    "newmaster: Setting log version to %d",vers));
			__log_set_version(env, vers);
			if ((ret = __env_init_rec(env, vers)) != 0)
				goto errlck;
		}

		REP_SYSTEM_UNLOCK(env);

		MUTEX_LOCK(env, rep->mtx_clientdb);
		__os_gettime(env, &lp->rcvd_ts, 1);
		lp->wait_ts = rep->request_gap;
		ZERO_LSN(lp->verify_lsn);
		ZERO_LSN(lp->prev_ckp);
		ZERO_LSN(lp->waiting_lsn);
		ZERO_LSN(lp->max_wait_lsn);
		/*
		 * Open if we need to, in preparation for the truncate
		 * we'll do in a moment.
		 */
		if (db_rep->rep_db == NULL &&
		    (ret = __rep_client_dbinit(env, 0, REP_DB)) != 0) {
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			goto err;
		}

		/*
		 * If we were in the middle of an internal initialization
		 * and we've discovered a new master instead, clean up
		 * our old internal init information.  We need to clean
		 * up any flags and unlock our lockout.
		 */
		REP_SYSTEM_LOCK(env);
		if (ISSET_LOCKOUT_BDB(rep)) {
			ret = __rep_init_cleanup(env, rep, DB_FORCE);
			/*
			 * Note that if an in-progress internal init was indeed
			 * "cleaned up", clearing these flags now will allow the
			 * application to see a completely empty database
			 * environment for a moment (until the master responds
			 * to our ALL_REQ).
			 */
			F_CLR(rep, REP_F_ABBREVIATED);
			CLR_RECOVERY_SETTINGS(rep);
		}
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
		if (ret != 0) {
			/* TODO: consider add'l error recovery steps. */
			goto errlck;
		}
		ENV_GET_THREAD_INFO(env, ip);
		if ((ret = __db_truncate(db_rep->rep_db, ip, NULL, &unused))
		    != 0)
			goto errlck;
		STAT(rep->stat.st_log_queued = 0);

		/*
		 * This needs to be performed under message lockout
		 * if we're actually changing master.
		 */
		__rep_elect_done(env, rep);
		RPRINT(env, (env, DB_VERB_REP_MISC,
		    "Updating gen from %lu to %lu from master %d",
		    (u_long)rep->gen, (u_long)cntrl->gen, eid));
		SET_GEN(cntrl->gen);
		rep->mgen = cntrl->gen;
		if ((ret = __rep_notify_threads(env, AWAIT_GEN)) != 0)
			goto errlck;
		(void)__rep_write_gen(env, rep, rep->gen);
		if (rep->egen <= rep->gen)
			rep->egen = rep->gen + 1;
		rep->master_id = eid;
		STAT(rep->stat.st_master_changes++);
		rep->stat.st_startup_complete = 0;
		rep->version = cntrl->rep_version;
		RPRINT(env, (env, DB_VERB_REP_MISC,
		    "egen: %lu. rep version %lu",
		    (u_long)rep->egen, (u_long)rep->version));

		/*
		 * If we're delaying client sync-up, we know we have a
		 * new/changed master now, set flag indicating we are
		 * actively delaying.
		 */
		if (FLD_ISSET(rep->config, REP_C_DELAYCLIENT))
			F_SET(rep, REP_F_DELAY);
		if ((ret = __rep_lockout_archive(env, rep)) != 0)
			goto errlck;
		rep->sync_state = SYNC_VERIFY;
		FLD_CLR(rep->lockout_flags, REP_LOCKOUT_MSG);
		(void)__memp_set_config(env->dbenv, DB_MEMP_SYNC_INTERRUPT, 0);
		lockout_msg = 0;
	} else
		__rep_elect_done(env, rep);
	REP_SYSTEM_UNLOCK(env);

	MUTEX_LOCK(env, rep->mtx_clientdb);
	lsn = lp->ready_lsn;

	if (!change) {
		ret = 0;
		do_req = __rep_check_doreq(env, rep);
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
		/*
		 * If there wasn't a change, we might still have some
		 * catching up or verification to do.
		 */
		if (do_req &&
		    (rep->sync_state != SYNC_OFF ||
		    LOG_COMPARE(&lsn, &cntrl->lsn) < 0)) {
			ret = __rep_resend_req(env, 0);
			if (ret != 0)
				RPRINT(env, (env, DB_VERB_REP_MISC,
				    "resend_req ret is %lu", (u_long)ret));
		}
		/*
		 * If we're not in one of the recovery modes, we need to
		 * clear the ARCHIVE flag.  Elections set ARCHIVE
		 * and if we called an election and found the same
		 * master, we need to clear ARCHIVE here.
		 */
		if (rep->sync_state == SYNC_OFF) {
			REP_SYSTEM_LOCK(env);
			FLD_CLR(rep->lockout_flags, REP_LOCKOUT_ARCHIVE);
			REP_SYSTEM_UNLOCK(env);
		}
		return (ret);
	}
	MUTEX_UNLOCK(env, rep->mtx_clientdb);

	/*
	 * If the master changed, we need to start the process of
	 * figuring out what our last valid log record is.  However,
	 * if both the master and we agree that the max LSN is 0,0,
	 * then there is no recovery to be done.  If we are at 0 and
	 * the master is not, then we just need to request all the log
	 * records from the master.
	 */
	if (IS_INIT_LSN(lsn) || IS_ZERO_LSN(lsn)) {
		if ((ret = __rep_newmaster_empty(env, eid)) != 0)
			goto err;
		goto newmaster_complete;
	}

	memset(&dbt, 0, sizeof(dbt));
	/*
	 * If this client is farther ahead on the log file than the master, see
	 * if there is any overlap in the logs.  If not, the client is too
	 * far ahead of the master and the client will start over.
	 */
	if (cntrl->lsn.file < lsn.file) {
		if ((ret = __log_cursor(env, &logc)) != 0)
			goto err;
		ret = __logc_get(logc, &first_lsn, &dbt, DB_FIRST);
		if ((t_ret = __logc_close(logc)) != 0 && ret == 0)
			ret = t_ret;
		if (ret == DB_NOTFOUND)
			goto notfound;
		else if (ret != 0)
			goto err;
		if (cntrl->lsn.file < first_lsn.file)
			goto notfound;
	}
	if ((ret = __log_cursor(env, &logc)) != 0)
		goto err;
	ret = __rep_log_backup(env, logc, &lsn, REP_REC_PERM);
	if ((t_ret = __logc_close(logc)) != 0 && ret == 0)
		ret = t_ret;
	if (ret == DB_NOTFOUND)
		goto notfound;
	else if (ret != 0)
		goto err;

	/*
	 * Finally, we have a record to ask for.
	 */
	MUTEX_LOCK(env, rep->mtx_clientdb);
	lp->verify_lsn = lsn;
	__os_gettime(env, &lp->rcvd_ts, 1);
	lp->wait_ts = rep->request_gap;
	MUTEX_UNLOCK(env, rep->mtx_clientdb);
	if (!F_ISSET(rep, REP_F_DELAY))
		(void)__rep_send_message(env,
		    eid, REP_VERIFY_REQ, &lsn, NULL, 0, DB_REP_ANYWHERE);
	goto newmaster_complete;

err:	/*
	 * If we failed, we need to clear the flags we may have set above
	 * because we're not going to be setting the verify_lsn.
	 */
	REP_SYSTEM_LOCK(env);
errlck:	if (lockout_msg) {
		FLD_CLR(rep->lockout_flags, REP_LOCKOUT_MSG);
		(void)__memp_set_config(env->dbenv, DB_MEMP_SYNC_INTERRUPT, 0);
	}
	F_CLR(rep, REP_F_DELAY);
	CLR_RECOVERY_SETTINGS(rep);
lckout:	REP_SYSTEM_UNLOCK(env);
	return (ret);

notfound:
	/*
	 * If we don't have an identification record, we still
	 * might have some log records but we're discarding them
	 * to sync up with the master from the start.
	 * Therefore, truncate our log and treat it as if it
	 * were empty.  In-memory logs can't be completely
	 * zeroed using __log_vtruncate, so just zero them out.
	 */
	RPRINT(env, (env, DB_VERB_REP_MISC,
	    "No commit or ckp found.  Truncate log."));
	if (lp->db_log_inmemory) {
		ZERO_LSN(lsn);
		ret = __log_zero(env, &lsn);
	} else {
		INIT_LSN(lsn);
		ret = __log_vtruncate(env, &lsn, &lsn, NULL);
	}
	if (ret != 0 && ret != DB_NOTFOUND)
		return (ret);
	infop = env->reginfo;
	renv = infop->primary;
	REP_SYSTEM_LOCK(env);
	(void)time(&renv->rep_timestamp);
	REP_SYSTEM_UNLOCK(env);
	if ((ret = __rep_newmaster_empty(env, eid)) != 0)
		goto err;
newmaster_complete:
	return (DB_REP_NEWMASTER);
}

/*
 * __rep_newmaster_empty
 *      Handle the case of a NEWMASTER message received when we have an empty
 * log.  This requires internal init.  If we can't do that because
 * AUTOINIT off, return JOIN_FAILURE.  If F_DELAY is in effect, don't even
 * consider AUTOINIT yet, because they could change it before rep_sync call.
 */
static int
__rep_newmaster_empty(env, eid)
	ENV *env;
	int eid;
{
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	int msg, ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	lp = env->lg_handle->reginfo.primary;
	msg = ret = 0;

	MUTEX_LOCK(env, rep->mtx_clientdb);
	REP_SYSTEM_LOCK(env);
	lp->wait_ts = rep->request_gap;

	/* Usual case is to skip to UPDATE state; we may revise this below. */
	rep->sync_state = SYNC_UPDATE;

	if (F_ISSET(rep, REP_F_DELAY)) {
		/*
		 * Having properly set up wait_ts for later, nothing more to
		 * do now.
		 */
	} else if (!FLD_ISSET(rep->config, REP_C_AUTOINIT)) {
		FLD_CLR(rep->lockout_flags, REP_LOCKOUT_ARCHIVE);
		CLR_RECOVERY_SETTINGS(rep);
		ret = DB_REP_JOIN_FAILURE;
	} else {
		/* Normal case: not DELAY but AUTOINIT. */
		msg = 1;
	}
	REP_SYSTEM_UNLOCK(env);
	MUTEX_UNLOCK(env, rep->mtx_clientdb);

	if (msg)
		(void)__rep_send_message(env, eid, REP_UPDATE_REQ,
		    NULL, NULL, 0, 0);
	return (ret);
}

/*
 * __rep_elect_done
 *	Clear all election information for this site.  Assumes the
 *	caller hold the region mutex.
 *
 * PUBLIC: void __rep_elect_done __P((ENV *, REP *));
 */
void
__rep_elect_done(env, rep)
	ENV *env;
	REP *rep;
{
	int inelect;
	db_timespec endtime;

	inelect = IN_ELECTION(rep);
	FLD_CLR(rep->elect_flags, REP_E_PHASE1 | REP_E_PHASE2 | REP_E_TALLY);

	rep->sites = 0;
	rep->votes = 0;
	if (inelect) {
		if (timespecisset(&rep->etime)) {
			__os_gettime(env, &endtime, 1);
			timespecsub(&endtime, &rep->etime);
#ifdef HAVE_STATISTICS
			rep->stat.st_election_sec = (u_int32_t)endtime.tv_sec;
			rep->stat.st_election_usec = (u_int32_t)
			    (endtime.tv_nsec / NS_PER_US);
#endif
			RPRINT(env, (env, DB_VERB_REP_ELECT,
			    "Election finished in %lu.%09lu sec",
			    (u_long)endtime.tv_sec, (u_long)endtime.tv_nsec));
			timespecclear(&rep->etime);
		}
		rep->egen++;
	}
	RPRINT(env, (env, DB_VERB_REP_ELECT,
	    "Election done; egen %lu", (u_long)rep->egen));
}

/*
 * __env_rep_enter --
 *
 * Check if we are in the middle of replication initialization and/or
 * recovery, and if so, disallow operations.  If operations are allowed,
 * increment handle-counts, so that we do not start recovery while we
 * are operating in the library.
 *
 * PUBLIC: int __env_rep_enter __P((ENV *, int));
 */
int
__env_rep_enter(env, checklock)
	ENV *env;
	int checklock;
{
	DB_REP *db_rep;
	REGENV *renv;
	REGINFO *infop;
	REP *rep;
	int cnt, ret;
	time_t	timestamp;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(env->dbenv, DB_ENV_NOLOCKING))
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;

	infop = env->reginfo;
	renv = infop->primary;
	if (checklock && F_ISSET(renv, DB_REGENV_REPLOCKED)) {
		(void)time(&timestamp);
		TIMESTAMP_CHECK(env, timestamp, renv);
		/*
		 * Check if we're still locked out after checking
		 * the timestamp.
		 */
		if (F_ISSET(renv, DB_REGENV_REPLOCKED))
			return (EINVAL);
	}

	REP_SYSTEM_LOCK(env);
	for (cnt = 0; FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_API);) {
		REP_SYSTEM_UNLOCK(env);
		/*
		 * We're spinning - environment may be hung. Check if
		 * recovery has been initiated.
		 */
		PANIC_CHECK(env);
		if (FLD_ISSET(rep->config, REP_C_NOWAIT)) {
			__db_errx(env, DB_STR("3504",
    "Operation locked out.  Waiting for replication lockout to complete"));
			return (DB_REP_LOCKOUT);
		}
		__os_yield(env, 1, 0);
		if (++cnt % 60 == 0 &&
		    (ret = __rep_show_progress(env,
		    DB_STR_P("DB_ENV handle"), cnt / 60)) != 0)
			return (ret);
		REP_SYSTEM_LOCK(env);
	}
	rep->handle_cnt++;
	REP_SYSTEM_UNLOCK(env);

	return (0);
}

static int
__rep_show_progress(env, which, mins)
	ENV *env;
	const char *which;
	int mins;
{
	DB_LOG *dblp;
	LOG *lp;
	REP *rep;
	DB_LSN ready_lsn;

	rep = env->rep_handle->region;
	dblp = env->lg_handle;
	lp = dblp == NULL ? NULL : dblp->reginfo.primary;

#define	WAITING_MSG DB_STR_A("3505",					\
    "%s waiting %d minutes for replication lockout to complete", "%s %d")
#define	WAITING_ARGS WAITING_MSG, which, mins

	__db_errx(env, WAITING_ARGS);
	RPRINT(env, (env, DB_VERB_REP_SYNC, WAITING_ARGS));

	if (lp == NULL)
		ZERO_LSN(ready_lsn);
	else {
		MUTEX_LOCK(env, rep->mtx_clientdb);
		ready_lsn = lp->ready_lsn;
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
	}
	REP_SYSTEM_LOCK(env);
	switch (rep->sync_state) {
	case SYNC_PAGE:
#define	PAGE_MSG DB_STR_A("3506",					\
    "SYNC_PAGE: files %lu/%lu; pages %lu (%lu next)", "%lu %lu %lu %lu")
#define	PAGE_ARGS (u_long)rep->curfile, (u_long)rep->nfiles, \
		    (u_long)rep->npages, (u_long)rep->ready_pg
		__db_errx(env, PAGE_MSG, PAGE_ARGS);
		RPRINT(env, (env, DB_VERB_REP_SYNC, PAGE_MSG, PAGE_ARGS));
		break;
	case SYNC_LOG:
#define	LSN_ARG(lsn) (u_long)(lsn).file, (u_long)(lsn).offset
#define	LOG_LSN_ARGS LSN_ARG(ready_lsn),				\
	    LSN_ARG(rep->first_lsn), LSN_ARG(rep->last_lsn)
#ifdef	HAVE_STATISTICS
#define	LOG_MSG DB_STR_A("3507",					\
    "SYNC_LOG: thru [%lu][%lu] from [%lu][%lu]/[%lu][%lu] (%lu queued)",\
    "%lu %lu %lu %lu %lu %lu %lu")
#define	LOG_ARGS LOG_LSN_ARGS, (u_long)rep->stat.st_log_queued
#else
#define	LOG_MSG DB_STR_A("3508",					\
    "SYNC_LOG: thru [%lu][%lu] from [%lu][%lu]/[%lu][%lu]",		\
    "%lu %lu %lu %lu %lu %lu")
#define	LOG_ARGS LOG_LSN_ARGS
#endif
		__db_errx(env, LOG_MSG, LOG_ARGS);
		RPRINT(env, (env, DB_VERB_REP_SYNC, LOG_MSG, LOG_ARGS));
		break;
	default:
		RPRINT(env, (env, DB_VERB_REP_SYNC,
		    "sync state %d", (int)rep->sync_state));
		break;
	}
	REP_SYSTEM_UNLOCK(env);
	return (0);
}

/*
 * __env_db_rep_exit --
 *
 *	Decrement handle count upon routine exit.
 *
 * PUBLIC: int __env_db_rep_exit __P((ENV *));
 */
int
__env_db_rep_exit(env)
	ENV *env;
{
	DB_REP *db_rep;
	REP *rep;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(env->dbenv, DB_ENV_NOLOCKING))
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;

	REP_SYSTEM_LOCK(env);
	rep->handle_cnt--;
	REP_SYSTEM_UNLOCK(env);

	return (0);
}

/*
 * __db_rep_enter --
 *	Called in replicated environments to keep track of in-use handles
 * and prevent any concurrent operation during recovery.  If checkgen is
 * non-zero, then we verify that the dbp has the same handle as the env.
 *
 * If return_now is non-zero, we'll return DB_DEADLOCK immediately, else we'll
 * sleep before returning DB_DEADLOCK.  Without the sleep, it is likely
 * the application will immediately try again and could reach a retry
 * limit before replication has a chance to finish.  The sleep increases
 * the probability that an application retry will succeed.
 *
 * Typically calls with txns set return_now so that we return immediately.
 * We want to return immediately because we want the txn to abort ASAP
 * so that the lockout can proceed.
 *
 * PUBLIC: int __db_rep_enter __P((DB *, int, int, int));
 */
int
__db_rep_enter(dbp, checkgen, checklock, return_now)
	DB *dbp;
	int checkgen, checklock, return_now;
{
	DB_REP *db_rep;
	ENV *env;
	REGENV *renv;
	REGINFO *infop;
	REP *rep;
	time_t	timestamp;

	env = dbp->env;
	/* Check if locks have been globally turned off. */
	if (F_ISSET(env->dbenv, DB_ENV_NOLOCKING))
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;
	infop = env->reginfo;
	renv = infop->primary;

	if (checklock && F_ISSET(renv, DB_REGENV_REPLOCKED)) {
		(void)time(&timestamp);
		TIMESTAMP_CHECK(env, timestamp, renv);
		/*
		 * Check if we're still locked out after checking
		 * the timestamp.
		 */
		if (F_ISSET(renv, DB_REGENV_REPLOCKED))
			return (EINVAL);
	}

	/*
	 * Return a dead handle if an internal handle is trying to
	 * get an exclusive lock on this database.
	 */
	if (checkgen && dbp->mpf->mfp && IS_REP_CLIENT(env)) {
		if (dbp->mpf->mfp->excl_lockout) 
			return (DB_REP_HANDLE_DEAD);
	}

	REP_SYSTEM_LOCK(env);
	/*
	 * !!!
	 * Note, we are checking REP_LOCKOUT_OP, but we are
	 * incrementing rep->handle_cnt.  That seems like a mismatch,
	 * but the intention is to return DEADLOCK to the application
	 * which will cause them to abort the txn quickly and allow
	 * the lockout to proceed.
	 *
	 * The correctness of doing this depends on the fact that
	 * lockout of the API always sets REP_LOCKOUT_OP first.
	 */
	if (FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_OP)) {
		REP_SYSTEM_UNLOCK(env);
		if (!return_now)
			__os_yield(env, 5, 0);
		return (DB_LOCK_DEADLOCK);
	}

	if (checkgen && dbp->timestamp != renv->rep_timestamp) {
		REP_SYSTEM_UNLOCK(env);
		return (DB_REP_HANDLE_DEAD);
	}
	rep->handle_cnt++;
	REP_SYSTEM_UNLOCK(env);

	return (0);
}

/*
 * Check for permission to increment handle_cnt, and do so if possible.  Used in
 * cases where we want to count an operation in the context of a transaction,
 * but the operation does not involve a DB handle.
 *
 * PUBLIC: int __op_handle_enter __P((ENV *));
 */
int
__op_handle_enter(env)
	ENV *env;
{
	REP *rep;
	int ret;

	rep = env->rep_handle->region;
	REP_SYSTEM_LOCK(env);
	if (FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_OP))
		ret = DB_LOCK_DEADLOCK;
	else {
		rep->handle_cnt++;
		ret = 0;
	}
	REP_SYSTEM_UNLOCK(env);
	return (ret);
}

/*
 * __op_rep_enter --
 *
 *	Check if we are in the middle of replication initialization and/or
 * recovery, and if so, disallow new multi-step operations, such as
 * transaction and memp gets.  If operations are allowed,
 * increment the op_cnt, so that we do not start recovery while we have
 * active operations.
 *
 * PUBLIC: int __op_rep_enter __P((ENV *, int, int));
 */
int
__op_rep_enter(env, local_nowait, obey_user)
	ENV *env;
	int local_nowait, obey_user;
{
	DB_REP *db_rep;
	REP *rep;
	int cnt, ret;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(env->dbenv, DB_ENV_NOLOCKING))
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;

	REP_SYSTEM_LOCK(env);
	for (cnt = 0; FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_OP);) {
		REP_SYSTEM_UNLOCK(env);
		/*
		 * We're spinning - environment may be hung.  Check if
		 * recovery has been initiated.
		 */
		PANIC_CHECK(env);
		if (local_nowait)
			return (DB_REP_LOCKOUT);
		if (FLD_ISSET(rep->config, REP_C_NOWAIT) && obey_user) {
			__db_errx(env, DB_STR("3509",
    "Operation locked out.  Waiting for replication lockout to complete"));
			return (DB_REP_LOCKOUT);
		}
		__os_yield(env, 5, 0);
		cnt += 5;
		if (++cnt % 60 == 0 &&
		    (ret = __rep_show_progress(env,
		    "__op_rep_enter", cnt / 60)) != 0)
			return (ret);
		REP_SYSTEM_LOCK(env);
	}
	rep->op_cnt++;
	REP_SYSTEM_UNLOCK(env);

	return (0);
}

/*
 * __op_rep_exit --
 *
 *	Decrement op count upon transaction commit/abort/discard or
 *	memp_fput.
 *
 * PUBLIC: int __op_rep_exit __P((ENV *));
 */
int
__op_rep_exit(env)
	ENV *env;
{
	DB_REP *db_rep;
	REP *rep;

	/* Check if locks have been globally turned off. */
	if (F_ISSET(env->dbenv, DB_ENV_NOLOCKING))
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;

	REP_SYSTEM_LOCK(env);
	DB_ASSERT(env, rep->op_cnt > 0);
	rep->op_cnt--;
	REP_SYSTEM_UNLOCK(env);

	return (0);
}

/*
 * __archive_rep_enter
 *	Used by log_archive to determine if it is okay to remove
 * log files.
 *
 * PUBLIC: int __archive_rep_enter __P((ENV *));
 */
int
__archive_rep_enter(env)
	ENV *env;
{
	DB_REP *db_rep;
	REGENV *renv;
	REGINFO *infop;
	REP *rep;
	time_t timestamp;
	int ret;

	ret = 0;
	infop = env->reginfo;
	renv = infop->primary;

	/*
	 * This is tested before REP_ON below because we always need
	 * to obey if any replication process has disabled archiving.
	 * Everything is in the environment region that we need here.
	 */
	if (F_ISSET(renv, DB_REGENV_REPLOCKED)) {
		(void)time(&timestamp);
		TIMESTAMP_CHECK(env, timestamp, renv);
		/*
		 * Check if we're still locked out after checking
		 * the timestamp.
		 */
		if (F_ISSET(renv, DB_REGENV_REPLOCKED))
			return (DB_REP_LOCKOUT);
	}

	if (!REP_ON(env))
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;
	REP_SYSTEM_LOCK(env);
	if (FLD_ISSET(rep->lockout_flags, REP_LOCKOUT_ARCHIVE))
		ret = DB_REP_LOCKOUT;
	else
		rep->arch_th++;
	REP_SYSTEM_UNLOCK(env);
	return (ret);
}

/*
 * __archive_rep_exit
 *	Clean up accounting for log archive threads.
 *
 * PUBLIC: int __archive_rep_exit __P((ENV *));
 */
int
__archive_rep_exit(env)
	ENV *env;
{
	DB_REP *db_rep;
	REP *rep;

	if (!REP_ON(env))
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;
	REP_SYSTEM_LOCK(env);
	rep->arch_th--;
	REP_SYSTEM_UNLOCK(env);
	return (0);
}

/*
 * __rep_lockout_archive --
 *	Coordinate with other threads archiving log files so that
 *	we can run and know that no log files will be removed out
 *	from underneath us.
 *	Assumes the caller holds the region mutex.
 *
 * PUBLIC: int __rep_lockout_archive __P((ENV *, REP *));
 */
int
__rep_lockout_archive(env, rep)
	ENV *env;
	REP *rep;
{
	return (__rep_lockout_int(env, rep, &rep->arch_th, 0,
	    "arch_th", REP_LOCKOUT_ARCHIVE));
}

/*
 * __rep_lockout_api --
 *	Coordinate with other threads in the library and active txns so
 *	that we can run single-threaded, for recovery or internal backup.
 *	Assumes the caller holds the region mutex.
 *
 * PUBLIC: int __rep_lockout_api __P((ENV *, REP *));
 */
int
__rep_lockout_api(env, rep)
	ENV *env;
	REP *rep;
{
	int ret;

	/*
	 * We must drain long-running operations first.  We check
	 * REP_LOCKOUT_OP in __db_rep_enter in order to allow them
	 * to abort existing txns quickly.  Therefore, we must
	 * always lockout REP_LOCKOUT_OP first, then REP_LOCKOUT_API.
	 */
	if ((ret = __rep_lockout_int(env, rep, &rep->op_cnt, 0,
	    "op_cnt", REP_LOCKOUT_OP)) != 0)
		return (ret);
	if ((ret = __rep_lockout_int(env, rep, &rep->handle_cnt, 0,
	    "handle_cnt", REP_LOCKOUT_API)) != 0)
		FLD_CLR(rep->lockout_flags, REP_LOCKOUT_OP);
	return (ret);
}

/*
 * PUBLIC: int __rep_take_apilockout __P((ENV *));
 *
 * For use by repmgr (keep the module boundaries reasonably clean).
 */
int
__rep_take_apilockout(env)
	ENV *env;
{
	REP *rep;
	int ret;

	rep = env->rep_handle->region;
	REP_SYSTEM_LOCK(env);
	ret = __rep_lockout_api(env, rep);
	REP_SYSTEM_UNLOCK(env);
	return (ret);
}

/*
 * PUBLIC: int __rep_clear_apilockout __P((ENV *));
 */
int
__rep_clear_apilockout(env)
	ENV *env;
{
	REP *rep;

	rep = env->rep_handle->region;

	REP_SYSTEM_LOCK(env);
	CLR_LOCKOUT_BDB(rep);
	REP_SYSTEM_UNLOCK(env);
	return (0);
}

/*
 * __rep_lockout_apply --
 *	Coordinate with other threads processing messages so that
 *	we can run single-threaded and know that no incoming
 *	message can apply new log records.
 *	This call should be short-term covering a specific critical
 *	operation where we need to make sure no new records change
 *	the log.  Currently used to coordinate with elections.
 *	Assumes the caller holds the region mutex.
 *
 * PUBLIC: int __rep_lockout_apply __P((ENV *, REP *, u_int32_t));
 */
int
__rep_lockout_apply(env, rep, apply_th)
	ENV *env;
	REP *rep;
	u_int32_t apply_th;
{
	return (__rep_lockout_int(env, rep, &rep->apply_th, apply_th,
	    "apply_th", REP_LOCKOUT_APPLY));
}

/*
 * __rep_lockout_msg --
 *	Coordinate with other threads processing messages so that
 *	we can run single-threaded and know that no incoming
 *	message can change the world (i.e., like a NEWMASTER message).
 *	This call should be short-term covering a specific critical
 *	operation where we need to make sure no new messages arrive
 *	in the middle and all message threads are out before we start it.
 *	Assumes the caller holds the region mutex.
 *
 * PUBLIC: int __rep_lockout_msg __P((ENV *, REP *, u_int32_t));
 */
int
__rep_lockout_msg(env, rep, msg_th)
	ENV *env;
	REP *rep;
	u_int32_t msg_th;
{
	return (__rep_lockout_int(env, rep, &rep->msg_th, msg_th,
	    "msg_th", REP_LOCKOUT_MSG));
}

/*
 * __rep_lockout_int --
 *	Internal common code for locking out and coordinating
 *	with other areas of the code.
 *	Assumes the caller holds the region mutex.
 *
 */
static int
__rep_lockout_int(env, rep, fieldp, field_val, msg, lockout_flag)
	ENV *env;
	REP *rep;
	u_int32_t *fieldp;
	const char *msg;
	u_int32_t field_val, lockout_flag;
{
	int ret, wait_cnt;

	FLD_SET(rep->lockout_flags, lockout_flag);
	for (wait_cnt = 0; *fieldp > field_val;) {
		if ((ret = __rep_notify_threads(env, LOCKOUT)) != 0)
			return (ret);
		REP_SYSTEM_UNLOCK(env);
		/* We're spinning - environment may be hung.  Check if
		 * recovery has been initiated.
		 */
		PANIC_CHECK(env);
		__os_yield(env, 1, 0);
#ifdef DIAGNOSTIC
		if (wait_cnt == 5) {
			RPRINT(env, (env, DB_VERB_REP_MISC,
			    "Waiting for %s (%lu) to complete lockout to %lu",
			    msg, (u_long)*fieldp, (u_long)field_val));
			__db_errx(env, DB_STR_A("3510",
"Waiting for %s (%lu) to complete replication lockout",
			    "%s %lu"), msg, (u_long)*fieldp);
		}
		if (++wait_cnt % 60 == 0)
			__db_errx(env, DB_STR_A("3511",
"Waiting for %s (%lu) to complete replication lockout for %d minutes",
			    "%s %lu %d"), msg, (u_long)*fieldp, wait_cnt / 60);
#endif
		REP_SYSTEM_LOCK(env);
	}

	COMPQUIET(msg, NULL);
	return (0);
}

/*
 * __rep_send_throttle -
 *	Send a record, throttling if necessary.  Callers of this function
 * will throttle - breaking out of their loop, if the repth->type field
 * changes from the normal message type to the *_MORE message type.
 * This function will send the normal type unless throttling gets invoked.
 * Then it sets the type field and sends the _MORE message.
 *
 * Throttling is always only relevant in serving requests, so we always send
 * with REPCTL_RESEND.  Additional desired flags can be passed in the ctlflags
 * argument.
 *
 * PUBLIC: int __rep_send_throttle __P((ENV *, int, REP_THROTTLE *,
 * PUBLIC:    u_int32_t, u_int32_t));
 */
int
__rep_send_throttle(env, eid, repth, flags, ctlflags)
	ENV *env;
	int eid;
	REP_THROTTLE *repth;
	u_int32_t ctlflags, flags;
{
	DB_REP *db_rep;
	REP *rep;
	u_int32_t size, typemore;
	int check_limit;

	check_limit = repth->gbytes != 0 || repth->bytes != 0;
	/*
	 * If we only want to do throttle processing and we don't have it
	 * turned on, return immediately.
	 */
	if (!check_limit && LF_ISSET(REP_THROTTLE_ONLY))
		return (0);

	db_rep = env->rep_handle;
	rep = db_rep->region;
	typemore = 0;
	if (repth->type == REP_LOG)
		typemore = REP_LOG_MORE;
	if (repth->type == REP_PAGE)
		typemore = REP_PAGE_MORE;
	DB_ASSERT(env, typemore != 0);

	/*
	 * data_dbt.size is only the size of the log
	 * record;  it doesn't count the size of the
	 * control structure. Factor that in as well
	 * so we're not off by a lot if our log records
	 * are small.
	 */
	size = repth->data_dbt->size + sizeof(__rep_control_args);
	if (check_limit) {
		while (repth->bytes <= size) {
			if (repth->gbytes > 0) {
				repth->bytes += GIGABYTE;
				--(repth->gbytes);
				continue;
			}
			/*
			 * We don't hold the rep mutex,
			 * and may miscount.
			 */
			STAT(rep->stat.st_nthrottles++);
			repth->type = typemore;
			goto snd;
		}
		repth->bytes -= size;
	}
	/*
	 * Always send if it is typemore, otherwise send only if
	 * REP_THROTTLE_ONLY is not set.
	 *
	 * NOTE:  It is the responsibility of the caller to marshal, if
	 * needed, the data_dbt.  This function just sends what it is given.
	 */
snd:	if ((repth->type == typemore || !LF_ISSET(REP_THROTTLE_ONLY)) &&
	    (__rep_send_message(env, eid, repth->type,
	    &repth->lsn, repth->data_dbt, (REPCTL_RESEND | ctlflags), 0) != 0))
		return (DB_REP_UNAVAIL);
	return (0);
}

/*
 * __rep_msg_to_old --
 *	Convert current message numbers to old message numbers.
 *
 * PUBLIC: u_int32_t __rep_msg_to_old __P((u_int32_t, u_int32_t));
 */
u_int32_t
__rep_msg_to_old(version, rectype)
	u_int32_t version, rectype;
{
	/*
	 * We need to convert from current message numbers to old numbers and
	 * we need to convert from old numbers to current numbers.  Offset by
	 * one for more readable code.
	 */
	/*
	 * Everything for version 0 is invalid, there is no version 0.
	 */
	static const u_int32_t table[DB_REPVERSION][REP_MAX_MSG+1] = {
	/* There is no DB_REPVERSION 0. */
	{   REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID },
	/*
	 * 4.2/DB_REPVERSION 1 no longer supported.
	 */
	{   REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID },
	/*
	 * 4.3/DB_REPVERSION 2 no longer supported.
	 */
	{   REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID },
	/*
	 * From 4.7 message number To 4.4/4.5 message number
	 */
	{   REP_INVALID,	/* NO message 0 */
	    1,			/* REP_ALIVE */
	    2,			/* REP_ALIVE_REQ */
	    3,			/* REP_ALL_REQ */
	    4,			/* REP_BULK_LOG */
	    5,			/* REP_BULK_PAGE */
	    6,			/* REP_DUPMASTER */
	    7,			/* REP_FILE */
	    8,			/* REP_FILE_FAIL */
	    9,			/* REP_FILE_REQ */
	    REP_INVALID,	/* REP_LEASE_GRANT */
	    10,			/* REP_LOG */
	    11,			/* REP_LOG_MORE */
	    12,			/* REP_LOG_REQ */
	    13,			/* REP_MASTER_REQ */
	    14,			/* REP_NEWCLIENT */
	    15,			/* REP_NEWFILE */
	    16,			/* REP_NEWMASTER */
	    17,			/* REP_NEWSITE */
	    18,			/* REP_PAGE */
	    19,			/* REP_PAGE_FAIL */
	    20,			/* REP_PAGE_MORE */
	    21,			/* REP_PAGE_REQ */
	    22,			/* REP_REREQUEST */
	    REP_INVALID,	/* REP_START_SYNC */
	    23,			/* REP_UPDATE */
	    24,			/* REP_UPDATE_REQ */
	    25,			/* REP_VERIFY */
	    26,			/* REP_VERIFY_FAIL */
	    27,			/* REP_VERIFY_REQ */
	    28,			/* REP_VOTE1 */
	    29			/* REP_VOTE2 */
	},
	/*
	 * From 4.7 message number To 4.6 message number.  There are
	 * NO message differences between 4.6 and 4.7.  The
	 * control structure changed.
	 */
	{   REP_INVALID,	/* NO message 0 */
	    1,			/* REP_ALIVE */
	    2,			/* REP_ALIVE_REQ */
	    3,			/* REP_ALL_REQ */
	    4,			/* REP_BULK_LOG */
	    5,			/* REP_BULK_PAGE */
	    6,			/* REP_DUPMASTER */
	    7,			/* REP_FILE */
	    8,			/* REP_FILE_FAIL */
	    9,			/* REP_FILE_REQ */
	    10,			/* REP_LEASE_GRANT */
	    11,			/* REP_LOG */
	    12,			/* REP_LOG_MORE */
	    13,			/* REP_LOG_REQ */
	    14,			/* REP_MASTER_REQ */
	    15,			/* REP_NEWCLIENT */
	    16,			/* REP_NEWFILE */
	    17,			/* REP_NEWMASTER */
	    18,			/* REP_NEWSITE */
	    19,			/* REP_PAGE */
	    20,			/* REP_PAGE_FAIL */
	    21,			/* REP_PAGE_MORE */
	    22,			/* REP_PAGE_REQ */
	    23,			/* REP_REREQUEST */
	    24,			/* REP_START_SYNC */
	    25,			/* REP_UPDATE */
	    26,			/* REP_UPDATE_REQ */
	    27,			/* REP_VERIFY */
	    28,			/* REP_VERIFY_FAIL */
	    29,			/* REP_VERIFY_REQ */
	    30,			/* REP_VOTE1 */
	    31			/* REP_VOTE2 */
	},
	/*
	 * From 5.2 message number To 4.7 message number.  There are
	 * NO message differences between 4.7 and 5.2.  The
	 * content of vote1 changed.
	 */
	{   REP_INVALID,	/* NO message 0 */
	    1,			/* REP_ALIVE */
	    2,			/* REP_ALIVE_REQ */
	    3,			/* REP_ALL_REQ */
	    4,			/* REP_BULK_LOG */
	    5,			/* REP_BULK_PAGE */
	    6,			/* REP_DUPMASTER */
	    7,			/* REP_FILE */
	    8,			/* REP_FILE_FAIL */
	    9,			/* REP_FILE_REQ */
	    10,			/* REP_LEASE_GRANT */
	    11,			/* REP_LOG */
	    12,			/* REP_LOG_MORE */
	    13,			/* REP_LOG_REQ */
	    14,			/* REP_MASTER_REQ */
	    15,			/* REP_NEWCLIENT */
	    16,			/* REP_NEWFILE */
	    17,			/* REP_NEWMASTER */
	    18,			/* REP_NEWSITE */
	    19,			/* REP_PAGE */
	    20,			/* REP_PAGE_FAIL */
	    21,			/* REP_PAGE_MORE */
	    22,			/* REP_PAGE_REQ */
	    23,			/* REP_REREQUEST */
	    24,			/* REP_START_SYNC */
	    25,			/* REP_UPDATE */
	    26,			/* REP_UPDATE_REQ */
	    27,			/* REP_VERIFY */
	    28,			/* REP_VERIFY_FAIL */
	    29,			/* REP_VERIFY_REQ */
	    30,			/* REP_VOTE1 */
	    31			/* REP_VOTE2 */
	},
	/*
	 * From 5.3 message number To 4.7 message number.  There are
	 * NO message differences between 4.7 and 5.3.  The
	 * content of fileinfo changed.
	 */
	{   REP_INVALID,	/* NO message 0 */
	    1,			/* REP_ALIVE */
	    2,			/* REP_ALIVE_REQ */
	    3,			/* REP_ALL_REQ */
	    4,			/* REP_BULK_LOG */
	    5,			/* REP_BULK_PAGE */
	    6,			/* REP_DUPMASTER */
	    7,			/* REP_FILE */
	    8,			/* REP_FILE_FAIL */
	    9,			/* REP_FILE_REQ */
	    10,			/* REP_LEASE_GRANT */
	    11,			/* REP_LOG */
	    12,			/* REP_LOG_MORE */
	    13,			/* REP_LOG_REQ */
	    14,			/* REP_MASTER_REQ */
	    15,			/* REP_NEWCLIENT */
	    16,			/* REP_NEWFILE */
	    17,			/* REP_NEWMASTER */
	    18,			/* REP_NEWSITE */
	    19,			/* REP_PAGE */
	    20,			/* REP_PAGE_FAIL */
	    21,			/* REP_PAGE_MORE */
	    22,			/* REP_PAGE_REQ */
	    23,			/* REP_REREQUEST */
	    24,			/* REP_START_SYNC */
	    25,			/* REP_UPDATE */
	    26,			/* REP_UPDATE_REQ */
	    27,			/* REP_VERIFY */
	    28,			/* REP_VERIFY_FAIL */
	    29,			/* REP_VERIFY_REQ */
	    30,			/* REP_VOTE1 */
	    31			/* REP_VOTE2 */
	}
	};
	return (table[version][rectype]);
}

/*
 * __rep_msg_from_old --
 *	Convert old message numbers to current message numbers.
 *
 * PUBLIC: u_int32_t __rep_msg_from_old __P((u_int32_t, u_int32_t));
 */
u_int32_t
__rep_msg_from_old(version, rectype)
	u_int32_t version, rectype;
{
	/*
	 * We need to convert from current message numbers to old numbers and
	 * we need to convert from old numbers to current numbers.  Offset by
	 * one for more readable code.
	 */
	/*
	 * Everything for version 0 is invalid, there is no version 0.
	 */
	static const u_int32_t table[DB_REPVERSION][REP_MAX_MSG+1] = {
	/* There is no DB_REPVERSION 0. */
	{   REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID },
	/*
	 * 4.2/DB_REPVERSION 1 no longer supported.
	 */
	{   REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID },
	/*
	 * 4.3/DB_REPVERSION 2 no longer supported.
	 */
	{   REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID,
	    REP_INVALID, REP_INVALID, REP_INVALID, REP_INVALID },
	/*
	 * From 4.4/4.5 message number To 4.7 message number
	 */
	{   REP_INVALID,	/* NO message 0 */
	    1,			/* 1, REP_ALIVE */
	    2,			/* 2, REP_ALIVE_REQ */
	    3,			/* 3, REP_ALL_REQ */
	    4,			/* 4, REP_BULK_LOG */
	    5,			/* 5, REP_BULK_PAGE */
	    6,			/* 6, REP_DUPMASTER */
	    7,			/* 7, REP_FILE */
	    8,			/* 8, REP_FILE_FAIL */
	    9,			/* 9, REP_FILE_REQ */
	    /* 10, REP_LEASE_GRANT doesn't exist */
	    11,			/* 10, REP_LOG */
	    12,			/* 11, REP_LOG_MORE */
	    13,			/* 12, REP_LOG_REQ */
	    14,			/* 13, REP_MASTER_REQ */
	    15,			/* 14, REP_NEWCLIENT */
	    16,			/* 15, REP_NEWFILE */
	    17,			/* 16, REP_NEWMASTER */
	    18,			/* 17, REP_NEWSITE */
	    19,			/* 18, REP_PAGE */
	    20,			/* 19, REP_PAGE_FAIL */
	    21,			/* 20, REP_PAGE_MORE */
	    22,			/* 21, REP_PAGE_REQ */
	    23,			/* 22, REP_REREQUEST */
	    /* 24, REP_START_SYNC doesn't exist */
	    25,			/* 23, REP_UPDATE */
	    26,			/* 24, REP_UPDATE_REQ */
	    27,			/* 25, REP_VERIFY */
	    28,			/* 26, REP_VERIFY_FAIL */
	    29,			/* 27, REP_VERIFY_REQ */
	    30,			/* 28, REP_VOTE1 */
	    31,			/* 29, REP_VOTE2 */
	    REP_INVALID,	/* 30, 4.4/4.5 no message */
	    REP_INVALID		/* 31, 4.4/4.5 no message */
	},
	/*
	 * From 4.6 message number To 4.7 message number.  There are
	 * NO message differences between 4.6 and 4.7.  The
	 * control structure changed.
	 */
	{   REP_INVALID,	/* NO message 0 */
	    1,			/* 1, REP_ALIVE */
	    2,			/* 2, REP_ALIVE_REQ */
	    3,			/* 3, REP_ALL_REQ */
	    4,			/* 4, REP_BULK_LOG */
	    5,			/* 5, REP_BULK_PAGE */
	    6,			/* 6, REP_DUPMASTER */
	    7,			/* 7, REP_FILE */
	    8,			/* 8, REP_FILE_FAIL */
	    9,			/* 9, REP_FILE_REQ */
	    10,			/* 10, REP_LEASE_GRANT */
	    11,			/* 11, REP_LOG */
	    12,			/* 12, REP_LOG_MORE */
	    13,			/* 13, REP_LOG_REQ */
	    14,			/* 14, REP_MASTER_REQ */
	    15,			/* 15, REP_NEWCLIENT */
	    16,			/* 16, REP_NEWFILE */
	    17,			/* 17, REP_NEWMASTER */
	    18,			/* 18, REP_NEWSITE */
	    19,			/* 19, REP_PAGE */
	    20,			/* 20, REP_PAGE_FAIL */
	    21,			/* 21, REP_PAGE_MORE */
	    22,			/* 22, REP_PAGE_REQ */
	    23,			/* 22, REP_REREQUEST */
	    24,			/* 24, REP_START_SYNC */
	    25,			/* 25, REP_UPDATE */
	    26,			/* 26, REP_UPDATE_REQ */
	    27,			/* 27, REP_VERIFY */
	    28,			/* 28, REP_VERIFY_FAIL */
	    29,			/* 29, REP_VERIFY_REQ */
	    30,			/* 30, REP_VOTE1 */
	    31			/* 31, REP_VOTE2 */
	},
	/*
	 * From 4.7 message number To 5.2 message number.  There are
	 * NO message differences between them.  The vote1 contents
	 * changed.
	 */
	{   REP_INVALID,	/* NO message 0 */
	    1,			/* 1, REP_ALIVE */
	    2,			/* 2, REP_ALIVE_REQ */
	    3,			/* 3, REP_ALL_REQ */
	    4,			/* 4, REP_BULK_LOG */
	    5,			/* 5, REP_BULK_PAGE */
	    6,			/* 6, REP_DUPMASTER */
	    7,			/* 7, REP_FILE */
	    8,			/* 8, REP_FILE_FAIL */
	    9,			/* 9, REP_FILE_REQ */
	    10,			/* 10, REP_LEASE_GRANT */
	    11,			/* 11, REP_LOG */
	    12,			/* 12, REP_LOG_MORE */
	    13,			/* 13, REP_LOG_REQ */
	    14,			/* 14, REP_MASTER_REQ */
	    15,			/* 15, REP_NEWCLIENT */
	    16,			/* 16, REP_NEWFILE */
	    17,			/* 17, REP_NEWMASTER */
	    18,			/* 18, REP_NEWSITE */
	    19,			/* 19, REP_PAGE */
	    20,			/* 20, REP_PAGE_FAIL */
	    21,			/* 21, REP_PAGE_MORE */
	    22,			/* 22, REP_PAGE_REQ */
	    23,			/* 22, REP_REREQUEST */
	    24,			/* 24, REP_START_SYNC */
	    25,			/* 25, REP_UPDATE */
	    26,			/* 26, REP_UPDATE_REQ */
	    27,			/* 27, REP_VERIFY */
	    28,			/* 28, REP_VERIFY_FAIL */
	    29,			/* 29, REP_VERIFY_REQ */
	    30,			/* 30, REP_VOTE1 */
	    31			/* 31, REP_VOTE2 */
	},
	/*
	 * From 4.7 message number To 5.3 message number.  There are
	 * NO message differences between them.  The fileinfo contents
	 * changed.
	 */
	{   REP_INVALID,	/* NO message 0 */
	    1,			/* 1, REP_ALIVE */
	    2,			/* 2, REP_ALIVE_REQ */
	    3,			/* 3, REP_ALL_REQ */
	    4,			/* 4, REP_BULK_LOG */
	    5,			/* 5, REP_BULK_PAGE */
	    6,			/* 6, REP_DUPMASTER */
	    7,			/* 7, REP_FILE */
	    8,			/* 8, REP_FILE_FAIL */
	    9,			/* 9, REP_FILE_REQ */
	    10,			/* 10, REP_LEASE_GRANT */
	    11,			/* 11, REP_LOG */
	    12,			/* 12, REP_LOG_MORE */
	    13,			/* 13, REP_LOG_REQ */
	    14,			/* 14, REP_MASTER_REQ */
	    15,			/* 15, REP_NEWCLIENT */
	    16,			/* 16, REP_NEWFILE */
	    17,			/* 17, REP_NEWMASTER */
	    18,			/* 18, REP_NEWSITE */
	    19,			/* 19, REP_PAGE */
	    20,			/* 20, REP_PAGE_FAIL */
	    21,			/* 21, REP_PAGE_MORE */
	    22,			/* 22, REP_PAGE_REQ */
	    23,			/* 22, REP_REREQUEST */
	    24,			/* 24, REP_START_SYNC */
	    25,			/* 25, REP_UPDATE */
	    26,			/* 26, REP_UPDATE_REQ */
	    27,			/* 27, REP_VERIFY */
	    28,			/* 28, REP_VERIFY_FAIL */
	    29,			/* 29, REP_VERIFY_REQ */
	    30,			/* 30, REP_VOTE1 */
	    31			/* 31, REP_VOTE2 */
	}
	};
	return (table[version][rectype]);
}

/*
 * __rep_print_system --
 *	Optionally print a verbose message, including to the system file.
 *
 * PUBLIC: int __rep_print_system __P((ENV *, u_int32_t, const char *, ...))
 * PUBLIC:    __attribute__ ((__format__ (__printf__, 3, 4)));
 */
int
#ifdef STDC_HEADERS
__rep_print_system(ENV *env, u_int32_t verbose, const char *fmt, ...)
#else
__rep_print_system(env, verbose, fmt, va_alist)
	ENV *env;
	u_int32_t verbose;
	const char *fmt;
	va_dcl
#endif
{
	va_list ap;
	int ret;

#ifdef STDC_HEADERS
	va_start(ap, fmt);
#else
	va_start(ap);
#endif
	ret = __rep_print_int(env, verbose | DB_VERB_REP_SYSTEM, fmt, ap);
	va_end(ap);
	return (ret);
}

/*
 * __rep_print --
 *	Optionally print a verbose message.
 *
 * PUBLIC: int __rep_print __P((ENV *, u_int32_t, const char *, ...))
 * PUBLIC:    __attribute__ ((__format__ (__printf__, 3, 4)));
 */
int
#ifdef STDC_HEADERS
__rep_print(ENV *env, u_int32_t verbose, const char *fmt, ...)
#else
__rep_print(env, verbose, fmt, va_alist)
	ENV *env;
	u_int32_t verbose;
	const char *fmt;
	va_dcl
#endif
{
	va_list ap;
	int ret;

#ifdef STDC_HEADERS
	va_start(ap, fmt);
#else
	va_start(ap);
#endif
	ret = __rep_print_int(env, verbose, fmt, ap);
	va_end(ap);
	return (ret);
}

/*
 * __rep_print_int --
 *	Optionally print a verbose message.
 *
 * NOTE:
 * One anomaly is that the messaging functions expect/use/require
 * void functions.  The use of a mutex in __rep_print_int requires
 * a return value.
 */
static int
__rep_print_int(env, verbose, fmt, ap)
	ENV *env;
	u_int32_t verbose;
	const char *fmt;
	va_list ap;
{
	DB_MSGBUF mb;
	REP *rep;
	db_timespec ts;
	pid_t pid;
	db_threadid_t tid;
	int diag_msg;
	u_int32_t regular_msg, tmp_verbose;
	const char *s;
	char buf[DB_THREADID_STRLEN];

	tmp_verbose = env->dbenv->verbose;
	if (FLD_ISSET(tmp_verbose, verbose | DB_VERB_REPLICATION) == 0)
		return (0);
	DB_MSGBUF_INIT(&mb);

	diag_msg = 0;
	if (REP_ON(env)) {
		rep = env->rep_handle->region;
		/*
		 * If system diag messages are configured and this message's
		 * verbose level includes DB_VERB_REP_SYSTEM, this is a diag
		 * message.  This means it will be written to the diagnostic
		 * message files.
		 */
		diag_msg = FLD_ISSET(tmp_verbose, DB_VERB_REP_SYSTEM) &&
		    FLD_ISSET(verbose, DB_VERB_REP_SYSTEM) &&
		    !FLD_ISSET(rep->config, REP_C_INMEM);
	} else
		rep = NULL;
	/*
	 * We need to know if this message should be printed out
	 * via the regular, user mechanism.
	 */
	FLD_CLR(tmp_verbose, DB_VERB_REP_SYSTEM);
	regular_msg = FLD_ISSET(tmp_verbose,
	    verbose | DB_VERB_REPLICATION);

	/*
	 * It is possible we could be called before the env is finished
	 * getting set up and we want to skip that.
	 */
	if (diag_msg == 0 && regular_msg == 0)
		return (0);
	s = NULL;
	if (env->dbenv->db_errpfx != NULL)
		s = env->dbenv->db_errpfx;
	else if (rep != NULL) {
		if (F_ISSET(rep, REP_F_CLIENT))
			s = "CLIENT";
		else if (F_ISSET(rep, REP_F_MASTER))
			s = "MASTER";
	}
	if (s == NULL)
		s = "REP_UNDEF";
	__os_id(env->dbenv, &pid, &tid);
	if (diag_msg)
		MUTEX_LOCK(env, rep->mtx_diag);
	__os_gettime(env, &ts, 1);
	__db_msgadd(env, &mb, "[%lu:%lu][%s] %s: ",
	    (u_long)ts.tv_sec, (u_long)ts.tv_nsec/NS_PER_US,
	    env->dbenv->thread_id_string(env->dbenv, pid, tid, buf), s);

	__db_msgadd_ap(env, &mb, fmt, ap);

	DB_MSGBUF_REP_FLUSH(env, &mb, diag_msg, regular_msg);
	if (diag_msg)
		MUTEX_UNLOCK(env, rep->mtx_diag);
	return (0);
}

/*
 * PUBLIC: void __rep_print_message
 * PUBLIC:     __P((ENV *, int, __rep_control_args *, char *, u_int32_t));
 */
void
__rep_print_message(env, eid, rp, str, flags)
	ENV *env;
	int eid;
	__rep_control_args *rp;
	char *str;
	u_int32_t flags;
{
	u_int32_t ctlflags, rectype, verbflag;
	char ftype[64], *home, *type;

	rectype = rp->rectype;
	ctlflags = rp->flags;
	verbflag = DB_VERB_REP_MSGS | DB_VERB_REPLICATION;
	if (rp->rep_version != DB_REPVERSION)
		rectype = __rep_msg_from_old(rp->rep_version, rectype);
	switch (rectype) {
	case REP_ALIVE:
		FLD_SET(verbflag, DB_VERB_REP_ELECT | DB_VERB_REP_MISC);
		type = "alive";
		break;
	case REP_ALIVE_REQ:
		type = "alive_req";
		break;
	case REP_ALL_REQ:
		FLD_SET(verbflag, DB_VERB_REP_MISC);
		type = "all_req";
		break;
	case REP_BULK_LOG:
		FLD_SET(verbflag, DB_VERB_REP_MISC);
		type = "bulk_log";
		break;
	case REP_BULK_PAGE:
		FLD_SET(verbflag, DB_VERB_REP_SYNC);
		type = "bulk_page";
		break;
	case REP_DUPMASTER:
		FLD_SET(verbflag, DB_VERB_REP_SYSTEM);
		type = "dupmaster";
		break;
	case REP_FILE:
		type = "file";
		break;
	case REP_FILE_FAIL:
		type = "file_fail";
		break;
	case REP_FILE_REQ:
		type = "file_req";
		break;
	case REP_LEASE_GRANT:
		FLD_SET(verbflag, DB_VERB_REP_LEASE);
		type = "lease_grant";
		break;
	case REP_LOG:
		FLD_SET(verbflag, DB_VERB_REP_MISC);
		type = "log";
		break;
	case REP_LOG_MORE:
		FLD_SET(verbflag, DB_VERB_REP_MISC);
		type = "log_more";
		break;
	case REP_LOG_REQ:
		FLD_SET(verbflag, DB_VERB_REP_MISC);
		type = "log_req";
		break;
	case REP_MASTER_REQ:
		type = "master_req";
		break;
	case REP_NEWCLIENT:
		FLD_SET(verbflag, DB_VERB_REP_MISC | DB_VERB_REP_SYSTEM);
		type = "newclient";
		break;
	case REP_NEWFILE:
		FLD_SET(verbflag, DB_VERB_REP_MISC);
		type = "newfile";
		break;
	case REP_NEWMASTER:
		FLD_SET(verbflag, DB_VERB_REP_MISC | DB_VERB_REP_SYSTEM);
		type = "newmaster";
		break;
	case REP_NEWSITE:
		type = "newsite";
		break;
	case REP_PAGE:
		FLD_SET(verbflag, DB_VERB_REP_SYNC);
		type = "page";
		break;
	case REP_PAGE_FAIL:
		FLD_SET(verbflag, DB_VERB_REP_SYNC);
		type = "page_fail";
		break;
	case REP_PAGE_MORE:
		FLD_SET(verbflag, DB_VERB_REP_SYNC);
		type = "page_more";
		break;
	case REP_PAGE_REQ:
		FLD_SET(verbflag, DB_VERB_REP_SYNC);
		type = "page_req";
		break;
	case REP_REREQUEST:
		type = "rerequest";
		break;
	case REP_START_SYNC:
		FLD_SET(verbflag, DB_VERB_REP_MISC);
		type = "start_sync";
		break;
	case REP_UPDATE:
		FLD_SET(verbflag, DB_VERB_REP_SYNC | DB_VERB_REP_SYSTEM);
		type = "update";
		break;
	case REP_UPDATE_REQ:
		FLD_SET(verbflag, DB_VERB_REP_SYNC | DB_VERB_REP_SYSTEM);
		type = "update_req";
		break;
	case REP_VERIFY:
		FLD_SET(verbflag, DB_VERB_REP_SYNC | DB_VERB_REP_SYSTEM);
		type = "verify";
		break;
	case REP_VERIFY_FAIL:
		FLD_SET(verbflag, DB_VERB_REP_SYNC | DB_VERB_REP_SYSTEM);
		type = "verify_fail";
		break;
	case REP_VERIFY_REQ:
		FLD_SET(verbflag, DB_VERB_REP_SYNC | DB_VERB_REP_SYSTEM);
		type = "verify_req";
		break;
	case REP_VOTE1:
		FLD_SET(verbflag, DB_VERB_REP_ELECT | DB_VERB_REP_SYSTEM);
		type = "vote1";
		break;
	case REP_VOTE2:
		FLD_SET(verbflag, DB_VERB_REP_ELECT | DB_VERB_REP_SYSTEM);
		type = "vote2";
		break;
	default:
		type = "NOTYPE";
		break;
	}

	/*
	 * !!!
	 * If adding new flags to print out make sure the aggregate
	 * length cannot overflow the buffer.
	 */
	ftype[0] = '\0';
	if (LF_ISSET(DB_REP_ANYWHERE))
		(void)strcat(ftype, " any");		/* 4 */
	if (FLD_ISSET(ctlflags, REPCTL_FLUSH))
		(void)strcat(ftype, " flush");		/* 10 */
	/*
	 * We expect most of the time the messages will indicate
	 * group membership.  Only print if we're not already
	 * part of a group.
	 */
	if (!FLD_ISSET(ctlflags, REPCTL_GROUP_ESTD))
		(void)strcat(ftype, " nogroup");	/* 18 */
	if (FLD_ISSET(ctlflags, REPCTL_LEASE))
		(void)strcat(ftype, " lease");		/* 24 */
	if (LF_ISSET(DB_REP_NOBUFFER))
		(void)strcat(ftype, " nobuf");		/* 30 */
	if (FLD_ISSET(ctlflags, REPCTL_PERM))
		(void)strcat(ftype, " perm");		/* 35 */
	if (LF_ISSET(DB_REP_REREQUEST))
		(void)strcat(ftype, " rereq");		/* 41 */
	if (FLD_ISSET(ctlflags, REPCTL_RESEND))
		(void)strcat(ftype, " resend");		/* 48 */
	if (FLD_ISSET(ctlflags, REPCTL_LOG_END))
		(void)strcat(ftype, " logend");		/* 55 */

	/*
	 * !!!
	 * We selectively turned on bits using different verbose settings
	 * that relate to each message type.  Therefore, since the
	 * DB_VERB_REP_SYSTEM flag is explicitly set above when wanted,
	 * we *must* use the VPRINT macro here.  It will correctly
	 * handle the messages whether or not the SYSTEM flag is set.
	 */
	if ((home = env->db_home) == NULL)
		home = "NULL";
	VPRINT(env, (env, verbflag,
    "%s %s: msgv = %lu logv %lu gen = %lu eid %d, type %s, LSN [%lu][%lu] %s",
	    home, str,
	    (u_long)rp->rep_version, (u_long)rp->log_version, (u_long)rp->gen,
	    eid, type, (u_long)rp->lsn.file, (u_long)rp->lsn.offset, ftype));
	/*
	 * Make sure the version is close, and not swapped
	 * here.  Check for current version,  +/- a little bit.
	 */
	DB_ASSERT(env, rp->rep_version <= DB_REPVERSION+10);
	DB_ASSERT(env, rp->log_version <= DB_LOGVERSION+10);
}

/*
 * PUBLIC: void __rep_fire_event __P((ENV *, u_int32_t, void *));
 */
void
__rep_fire_event(env, event, info)
	ENV *env;
	u_int32_t event;
	void *info;
{
	int ret;

	/*
	 * Give repmgr first crack at handling all replication-related events.
	 * If it can't (or chooses not to) handle the event fully, then pass it
	 * along to the application.
	 */
	ret = __repmgr_handle_event(env, event, info);
	DB_ASSERT(env, ret == 0 || ret == DB_EVENT_NOT_HANDLED);

	if (ret == DB_EVENT_NOT_HANDLED)
		DB_EVENT(env, event, info);
}

/*
 * __rep_msg --
 *      Rep system diagnostic messaging routine.
 * This function is called from the __db_msg subsystem to
 * write out diagnostic messages to replication-owned files.
 *
 * PUBLIC: void __rep_msg __P((const ENV *, const char *));
 */
void
__rep_msg(env, msg)
	const ENV *env;
	const char *msg;
{
	DB_FH *fhp;
	DB_REP *db_rep;
	REP *rep;
	int i;
	size_t cnt, nlcnt;
	char nl = '\n';

	if (PANIC_ISSET(env))
		return;
	db_rep = env->rep_handle;
	rep = db_rep->region;
	DB_ASSERT((ENV *)env, !FLD_ISSET(rep->config, REP_C_INMEM));
	/*
	 * We know the only way we get here is with the mutex locked.  So
	 * we can read, modify and change all the diag related fields.
	 */
	i = rep->diag_index;
	fhp = db_rep->diagfile[i];

	if (db_rep->diag_off != rep->diag_off)
		(void)__os_seek((ENV *)env, fhp, 0, 0, rep->diag_off);
	if (__os_write((ENV *)env, fhp, (void *)msg, strlen(msg), &cnt) != 0)
		return;
	if (__os_write((ENV *)env, fhp, &nl, 1, &nlcnt) != 0)
		return;
	db_rep->diag_off = rep->diag_off += (cnt + nlcnt);
	/*
	 * If writing this message put us over the file size threshold,
	 * then we reset to the next file.  We don't care if it is
	 * exactly at the size, some amount over the file size is fine.
	 */
	if (rep->diag_off >= REP_DIAGSIZE) {
		rep->diag_index = (++i % DBREP_DIAG_FILES);
		rep->diag_off = 0;
	}
	return;
}

/*
 * PUBLIC: int __rep_notify_threads __P((ENV *, rep_waitreason_t));
 *
 * Caller must hold rep region mutex.  In the AWAIT_LSN case, caller must also
 * hold mtx_clientdb.
 */
int
__rep_notify_threads(env, wake_reason)
	ENV *env;
	rep_waitreason_t wake_reason;
{
	REP *rep;
	struct __rep_waiter *waiter;
	struct rep_waitgoal *goal;
	int ret, wake;

	ret = 0;
	rep = env->rep_handle->region;

	SH_TAILQ_FOREACH(waiter, &rep->waiters, links, __rep_waiter) {
		goal = &waiter->goal;
		wake = 0;
		if (wake_reason == LOCKOUT) {
			F_SET(waiter, REP_F_PENDING_LOCKOUT);
			wake = 1;
		} else if (wake_reason == goal->why ||
		    (goal->why == AWAIT_HISTORY && wake_reason == AWAIT_LSN)) {
			/*
			 * It's important that we only call __rep_check_goal
			 * with "goals" that match the wake_reason passed to us
			 * (modulo the LSN-to-HISTORY equivalence), because the
			 * caller has ensured that it is holding the appropriate
			 * mutexes depending on the wake_reason.
			 */
			if ((ret = __rep_check_goal(env, goal)) == 0)
				wake = 1;
			else if (ret == DB_TIMEOUT)
				ret = 0;
			else
				goto out;
		}

		if (wake) {
			MUTEX_UNLOCK(env, waiter->mtx_repwait);
			SH_TAILQ_REMOVE(&rep->waiters,
			    waiter, links, __rep_waiter);
			F_SET(waiter, REP_F_WOKEN);
		}
	}

out:
	return (ret);
}

/*
 * A "wait goal" describes a condition that a thread may be waiting for.
 * Evaluate the condition, returning 0 if the condition has been satisfied, and
 * DB_TIMEOUT if not.
 *
 * Caller must hold REP_SYSTEM lock and/or mtx_clientdb as appropriate.
 *
 * PUBLIC: int __rep_check_goal __P((ENV *, struct rep_waitgoal *));
 */
int
__rep_check_goal(env, goal)
	ENV *env;
	struct rep_waitgoal *goal;
{
	REP *rep;
	LOG *lp;
	int ret;

	rep = env->rep_handle->region;
	lp = env->lg_handle->reginfo.primary;
	ret = DB_TIMEOUT;	/* Pessimistic, to start. */

	/*
	 * Note that while AWAIT_LSN and AWAIT_HISTORY look similar, they are
	 * actually quite different.  With AWAIT_LSN, the u.lsn is the LSN of
	 * the commit of the transaction the caller is waiting for.  So we need
	 * to make sure we have gotten at least that far, thus ">=".
	 *
	 * For AWAIT_HISTORY, the u.lsn is simply a copy of whatever the current
	 * max_perm_lsn was at the time we last checked.  So anything if we have
	 * anything *beyond* that then we should wake up again and check to see
	 * if we now have the desired history (thus ">").  Thus when we're
	 * waiting for HISTORY we're going to get woken *at every commit we
	 * receive*!  Fortunately it should be coming as the first transaction
	 * after the gen change, and waiting for HISTORY should be extremely
	 * rare anyway.
	 */
	switch (goal->why) {
	case AWAIT_LSN:
		/* Have we reached our goal LSN? */
		if (LOG_COMPARE(&lp->max_perm_lsn, &goal->u.lsn) >= 0)
			ret = 0;
		break;
	case AWAIT_HISTORY:
		/*
		 * Have we made any progress whatsoever, beyond where we were at
		 * the time the waiting thread noted the current LSN?
		 *     When we have to wait for replication of the LSN history
		 * database, we don't know what LSN it's going to occur at.  So
		 * we have to wake up every time we get a new transaction.
		 * Fortunately, this should be exceedingly rare, and the number
		 * of transactions we have to plow through should almost never
		 * be more than 1.
		 */
		if (LOG_COMPARE(&lp->max_perm_lsn, &goal->u.lsn) > 0)
			ret = 0;
		break;
	case AWAIT_GEN:
		if (rep->gen >= goal->u.gen)
			ret = 0;
		break;
	case AWAIT_NIMDB:
		if (F_ISSET(rep, REP_F_NIMDBS_LOADED))
			ret = 0;
		break;
	default:
		DB_ASSERT(env, 0);
	}
	return (ret);
}

/*
 * __rep_log_backup --
 *
 * Walk backwards in the log looking for specific kinds of records.
 *
 * PUBLIC: int __rep_log_backup __P((ENV *, DB_LOGC *, DB_LSN *, u_int32_t));
 */
int
__rep_log_backup(env, logc, lsn, match)
	ENV *env;
	DB_LOGC *logc;
	DB_LSN *lsn;
	u_int32_t match;
{
	DBT mylog;
	u_int32_t rectype;
	int ret;

	ret = 0;
	memset(&mylog, 0, sizeof(mylog));
	while ((ret = __logc_get(logc, lsn, &mylog, DB_PREV)) == 0) {
		LOGCOPY_32(env, &rectype, mylog.data);
		/*
		 * Check the record type against the desired match type(s).
		 */
		if ((match == REP_REC_COMMIT &&
		    rectype == DB___txn_regop) ||
		    (match == REP_REC_PERM &&
		    (rectype == DB___txn_ckp || rectype == DB___txn_regop)))
			break;
	}
	return (ret);
}

/*
 * __rep_get_maxpermlsn --
 *
 * Safely retrieve the current max_perm_lsn value.
 *
 * PUBLIC: int __rep_get_maxpermlsn __P((ENV *, DB_LSN *));
 */
int
__rep_get_maxpermlsn(env, max_perm_lsnp)
	ENV *env;
	DB_LSN *max_perm_lsnp;
{
	DB_LOG *dblp;
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	LOG *lp;
	REP *rep;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;

	ENV_ENTER(env, ip);
	MUTEX_LOCK(env, rep->mtx_clientdb);
	*max_perm_lsnp = lp->max_perm_lsn;
	MUTEX_UNLOCK(env, rep->mtx_clientdb);
	ENV_LEAVE(env, ip);
	return (0);
}

/*
 * __rep_is_internal_rep_file --
 *
 * Return 1 if filename is an internal replication file; 0 otherwise.
 * Works for all internal replication files including internal database
 * files.
 *
 * PUBLIC: int __rep_is_internal_rep_file __P((char *));
 */
int
__rep_is_internal_rep_file(filename)
	char *filename;
{
	return (strncmp(filename,
	    REPFILEPREFIX, sizeof(REPFILEPREFIX) - 1) == 0 ? 1 : 0);
}

/*
 * Get the last generation number from the LSN history database.
 *
 * PUBLIC: int __rep_get_datagen __P((ENV *, u_int32_t *));
 */
int
__rep_get_datagen(env, data_genp)
	ENV *env;
	u_int32_t *data_genp;
{
	DB_REP *db_rep;
	DB_TXN *txn;
	DB *dbp;
	DBC *dbc;
	__rep_lsn_hist_key_args key;
	u_int8_t key_buf[__REP_LSN_HIST_KEY_SIZE];
	u_int8_t data_buf[__REP_LSN_HIST_DATA_SIZE];
	DBT key_dbt, data_dbt;
	u_int32_t flags;
	int ret, t_ret, tries;

	db_rep = env->rep_handle;
	ret = 0;
	*data_genp = 0;
	tries = 0;
	flags = DB_LAST;
retry:
	if ((ret = __txn_begin(env, NULL, NULL, &txn, DB_IGNORE_LEASE)) != 0)
		return (ret);

	if ((dbp = db_rep->lsn_db) == NULL) {
		if ((ret = __rep_open_sysdb(env,
		    NULL, txn, REPLSNHIST, 0, &dbp)) != 0) {
			/*
			 * If the database isn't there, it could be because it's
			 * memory-resident, and we haven't yet sync'ed with the
			 * master to materialize it.  It could be that this is
			 * a brand new environment.  We have a 0 datagen.
			 * That is not an error.
			 */
			ret = 0;
			goto out;
		}
		db_rep->lsn_db = dbp;
	}

	if ((ret = __db_cursor(dbp, NULL, txn, &dbc, 0)) != 0)
		goto out;

	DB_INIT_DBT(key_dbt, key_buf, __REP_LSN_HIST_KEY_SIZE);
	key_dbt.ulen = __REP_LSN_HIST_KEY_SIZE;
	F_SET(&key_dbt, DB_DBT_USERMEM);

	memset(&data_dbt, 0, sizeof(data_dbt));
	data_dbt.data = data_buf;
	data_dbt.ulen = __REP_LSN_HIST_DATA_SIZE;
	F_SET(&data_dbt, DB_DBT_USERMEM);
	if ((ret = __dbc_get(dbc, &key_dbt, &data_dbt, flags)) != 0) {
		if ((ret == DB_LOCK_DEADLOCK || ret == DB_LOCK_NOTGRANTED) &&
		    ++tries < 5) /* Limit of 5 is an arbitrary choice. */
			ret = 0;
		if ((t_ret = __dbc_close(dbc)) != 0 && ret == 0)
			ret = t_ret;
		if ((t_ret = __txn_abort(txn)) != 0 && ret == 0)
			ret = t_ret;
		/*
		 * If we have any kind of error at this point, bail.
		 * Otherwise pause and try again.
		 */
		if (ret != 0)
			goto err;
		__os_yield(env, 0, 10000); /* Arbitrary duration. */
		goto retry;
	}
	if ((ret = __dbc_close(dbc)) == 0 &&
	    (ret = __rep_lsn_hist_key_unmarshal(env,
	    &key, key_buf, __REP_LSN_HIST_KEY_SIZE, NULL)) == 0)
		*data_genp = key.gen;
out:
	if ((t_ret = __txn_commit(txn, DB_TXN_NOSYNC)) != 0 && ret == 0)
		ret = t_ret;
err:
	return (ret);
}
