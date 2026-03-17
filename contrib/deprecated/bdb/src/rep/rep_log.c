/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2004, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/log.h"

static int __rep_chk_newfile __P((ENV *, DB_LOGC *, REP *,
    __rep_control_args *, int));
static int __rep_log_split __P((ENV *, DB_THREAD_INFO *,
    __rep_control_args *, DBT *, DB_LSN *, DB_LSN *));

/*
 * __rep_allreq --
 *      Handle a REP_ALL_REQ message.
 *
 * PUBLIC: int __rep_allreq __P((ENV *, __rep_control_args *, int));
 */
int
__rep_allreq(env, rp, eid)
	ENV *env;
	__rep_control_args *rp;
	int eid;
{
	DBT data_dbt, newfiledbt;
	DB_LOGC *logc;
	DB_LSN log_end, oldfilelsn;
	DB_REP *db_rep;
	REP *rep;
	REP_BULK bulk;
	REP_THROTTLE repth;
	__rep_newfile_args nf_args;
	uintptr_t bulkoff;
	u_int32_t bulkflags, end_flag, flags, use_bulk;
	int arch_flag, ret, t_ret;
	u_int8_t buf[__REP_NEWFILE_SIZE];
	size_t len;

	ret = 0;
	db_rep = env->rep_handle;
	rep = db_rep->region;
	end_flag = 0;
	arch_flag = 0;

	if ((ret = __log_cursor(env, &logc)) != 0)
		return (ret);
	memset(&data_dbt, 0, sizeof(data_dbt));
	/*
	 * If we're doing bulk transfer, allocate a bulk buffer to put our
	 * log records in.  We still need to initialize the throttle info
	 * because if we encounter a log record larger than our entire bulk
	 * buffer, we need to send it as a singleton and also we want to
	 * support throttling with bulk.
	 *
	 * Use a local var so we don't need to worry if someone else turns
	 * on/off bulk in the middle of our call.
	 */
	use_bulk = FLD_ISSET(rep->config, REP_C_BULK);
	bulk.addr = NULL;
	if (use_bulk && (ret = __rep_bulk_alloc(env, &bulk, eid,
	    &bulkoff, &bulkflags, REP_BULK_LOG)) != 0)
		goto err;
	memset(&repth, 0, sizeof(repth));
	REP_SYSTEM_LOCK(env);
	if ((ret = __rep_lockout_archive(env, rep)) != 0) {
		REP_SYSTEM_UNLOCK(env);
		goto err;
	}
	arch_flag = 1;
	repth.gbytes = rep->gbytes;
	repth.bytes = rep->bytes;
	oldfilelsn = repth.lsn = rp->lsn;
	repth.type = REP_LOG;
	repth.data_dbt = &data_dbt;
	REP_SYSTEM_UNLOCK(env);

	/*
	 * Get the LSN of the end of the log, so that in our reading loop
	 * (below), we can recognize when we get there, and set the
	 * REPCTL_LOG_END flag.
	 */
	if ((ret = __logc_get(logc, &log_end, &data_dbt, DB_LAST)) != 0) {
		if (ret == DB_NOTFOUND && F_ISSET(rep, REP_F_MASTER))
			ret = 0;
		goto err;
	}

	flags = IS_ZERO_LSN(rp->lsn) ||
	    IS_INIT_LSN(rp->lsn) ?  DB_FIRST : DB_SET;
	/*
	 * We get the first item so that a client servicing requests
	 * can distinguish between not having the records and reaching
	 * the end of its log.  Return the DB_NOTFOUND if the client
	 * cannot get the record.  Return 0 if we finish the loop and
	 * sent all that we have.
	 */
	ret = __logc_get(logc, &repth.lsn, &data_dbt, flags);
	/*
	 * If the client is asking for all records
	 * because it doesn't have any, and our first
	 * record is not in the first log file, then
	 * the client is outdated and needs to get a
	 * VERIFY_FAIL.
	 */
	if (ret == 0 && repth.lsn.file != 1 && flags == DB_FIRST) {
		if (F_ISSET(rep, REP_F_CLIENT))
			ret = DB_NOTFOUND;
		else
			(void)__rep_send_message(env, eid,
			    REP_VERIFY_FAIL, &repth.lsn, NULL, 0, 0);
		goto err;
	}
	/*
	 * If we got DB_NOTFOUND it could be because the LSN we were
	 * given is at the end of the log file and we need to switch
	 * log files.  Reinitialize and get the current record when we return.
	 */
	if (ret == DB_NOTFOUND) {
		ret = __rep_chk_newfile(env, logc, rep, rp, eid);
		/*
		 * If we still get DB_NOTFOUND the client gave us a
		 * bad or unknown LSN.  Ignore it if we're the master.
		 * Any other error is returned.
		 */
		if (ret == 0)
			ret = __logc_get(logc, &repth.lsn,
			    &data_dbt, DB_CURRENT);
		if (ret == DB_NOTFOUND && F_ISSET(rep, REP_F_MASTER)) {
			ret = 0;
			goto err;
		}
		if (ret != 0)
			goto err;
	}

	/*
	 * For singleton log records, we break when we get a REP_LOG_MORE.
	 * Or if we're not using throttling, or we are using bulk, we stop
	 * when we reach the end (i.e. ret != 0).
	 */
	for (end_flag = 0;
	    ret == 0 && repth.type != REP_LOG_MORE && end_flag == 0;
	    ret = __logc_get(logc, &repth.lsn, &data_dbt, DB_NEXT)) {
		/*
		 * If we just changed log files, we need to send the
		 * version of this log file to the client.
		 */
		if (repth.lsn.file != oldfilelsn.file) {
			if ((ret = __logc_version(logc, &nf_args.version)) != 0)
				break;
			memset(&newfiledbt, 0, sizeof(newfiledbt));
			if (rep->version < DB_REPVERSION_47)
				DB_INIT_DBT(newfiledbt, &nf_args.version,
				    sizeof(nf_args.version));
			else {
				if ((ret = __rep_newfile_marshal(env, &nf_args,
				    buf, __REP_NEWFILE_SIZE, &len)) != 0)
					goto err;
				DB_INIT_DBT(newfiledbt, buf, len);
			}
			(void)__rep_send_message(env,
			    eid, REP_NEWFILE, &oldfilelsn, &newfiledbt,
			    REPCTL_RESEND, 0);
		}

		/*
		 * Mark the end of the ALL_REQ response to show that the
		 * receiving client should now be "caught up" with the
		 * replication group.  If we're the master, then our log end is
		 * certainly authoritative.  If we're another client, only if we
		 * ourselves have reached STARTUPDONE.
		 */
		end_flag = (LOG_COMPARE(&repth.lsn, &log_end) >= 0 &&
		    (F_ISSET(rep, REP_F_MASTER) ||
		    rep->stat.st_startup_complete)) ?
		    REPCTL_LOG_END : 0;
		/*
		 * If we are configured for bulk, try to send this as a bulk
		 * request.  If not configured, or it is too big for bulk
		 * then just send normally.
		 */
		if (use_bulk)
			ret = __rep_bulk_message(env, &bulk, &repth,
			    &repth.lsn, &data_dbt, (REPCTL_RESEND | end_flag));
		if (!use_bulk || ret == DB_REP_BULKOVF)
			ret = __rep_send_throttle(env,
			    eid, &repth, 0, end_flag);
		if (ret != 0)
			break;
		/*
		 * If we are about to change files, then we'll need the
		 * last LSN in the previous file.  Save it here.
		 */
		oldfilelsn = repth.lsn;
		oldfilelsn.offset += logc->len;
	}

	if (ret == DB_NOTFOUND || ret == DB_REP_UNAVAIL)
		ret = 0;
	/*
	 * We're done, force out whatever remains in the bulk buffer and
	 * free it.
	 */
err:
	/*
	 * We could have raced an unlink from an earlier log_archive
	 * and the user is removing the files themselves, now.  If
	 * we get an error indicating the log file might no longer
	 * exist, ignore it.
	 */
	if (ret == ENOENT)
		ret = 0;
	if (bulk.addr != NULL && (t_ret = __rep_bulk_free(env, &bulk,
	    (REPCTL_RESEND | end_flag))) != 0 && ret == 0 &&
	    t_ret != DB_REP_UNAVAIL)
		ret = t_ret;
	if (arch_flag) {
		REP_SYSTEM_LOCK(env);
		FLD_CLR(rep->lockout_flags, REP_LOCKOUT_ARCHIVE);
		REP_SYSTEM_UNLOCK(env);
	}
	if ((t_ret = __logc_close(logc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __rep_log --
 *      Handle a REP_LOG/REP_LOG_MORE message.
 *
 * PUBLIC: int __rep_log __P((ENV *, DB_THREAD_INFO *,
 * PUBLIC:     __rep_control_args *, DBT *, int, time_t, DB_LSN *));
 */
int
__rep_log(env, ip, rp, rec, eid, savetime, ret_lsnp)
	ENV *env;
	DB_THREAD_INFO *ip;
	__rep_control_args *rp;
	DBT *rec;
	int eid;
	time_t savetime;
	DB_LSN *ret_lsnp;
{
	DB_LOG *dblp;
	DB_LSN last_lsn, lsn;
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	int is_dup, master, ret;
	u_int32_t gapflags;

	is_dup = ret = 0;
	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;

	ret = __rep_apply(env, ip, rp, rec, ret_lsnp, &is_dup, &last_lsn);
	switch (ret) {
	/*
	 * We're in an internal backup and we've gotten
	 * all the log we need to run recovery.  Do so now.
	 */
	case DB_REP_LOGREADY:
		if ((ret =
		    __rep_logready(env, rep, savetime, &last_lsn)) != 0)
			goto out;
		break;
	/*
	 * If we get any of the "normal" returns, we only process
	 * LOG_MORE if this is not a duplicate record.  If the
	 * record is a duplicate we don't want to handle LOG_MORE
	 * and request a multiple data stream (or trigger internal
	 * initialization) since this could be a very old record
	 * that no longer exists on the master.
	 */
	case DB_REP_ISPERM:
	case DB_REP_NOTPERM:
	case 0:
		if (is_dup)
			goto out;
		else
			break;
	/*
	 * Any other return (errors), we're done.
	 */
	default:
		goto out;
	}
	if (rp->rectype == REP_LOG_MORE) {
		master = rep->master_id;

		/*
		 * Keep the cycle from stalling: In case we got the LOG_MORE out
		 * of order, before some preceding log records, we want to make
		 * sure our follow-up request resumes from where the LOG_MORE
		 * said it should.  (If the preceding log records never arrive,
		 * normal gap processing should take care of asking for them.)
		 * But if we already have this record and/or more, we need to
		 * ask to resume from what we need.  The upshot is we need the
		 * max of lp->lsn and the lsn from the message.
		 */
		MUTEX_LOCK(env, rep->mtx_clientdb);
		lsn = lp->ready_lsn;
		if (LOG_COMPARE(&rp->lsn, &lsn) > 0)
			lsn = rp->lsn;

		/*
		 * If the master_id is invalid, this means that since
		 * the last record was sent, somebody declared an
		 * election and we may not have a master to request
		 * things of.
		 *
		 * This is not an error;  when we find a new master,
		 * we'll re-negotiate where the end of the log is and
		 * try to bring ourselves up to date again anyway.
		 */
		if (master == DB_EID_INVALID) {
			ret = 0;
			MUTEX_UNLOCK(env, rep->mtx_clientdb);
			goto out;
		}
		/*
		 * If we're waiting for records, set the wait_ts
		 * high so that we avoid re-requesting too soon and
		 * end up with multiple data streams.
		 */
		if (IS_ZERO_LSN(lp->waiting_lsn))
			lp->wait_ts = rep->max_gap;
		/*
		 * If preceding log records were from the master, send the
		 * request for further log records to the master instead of
		 * allowing it to default to ANYWHERE.
		 */
		gapflags = REP_GAP_FORCE;
		if (master == eid)
			gapflags = gapflags | REP_GAP_REREQUEST;
		ret = __rep_loggap_req(env, rep, &lsn, gapflags);
		MUTEX_UNLOCK(env, rep->mtx_clientdb);
	}
out:
	return (ret);
}

/*
 * __rep_bulk_log --
 *      Handle a REP_BULK_LOG message.
 *
 * PUBLIC: int __rep_bulk_log __P((ENV *, DB_THREAD_INFO *,
 * PUBLIC:     __rep_control_args *, DBT *, time_t, DB_LSN *));
 */
int
__rep_bulk_log(env, ip, rp, rec, savetime, ret_lsnp)
	ENV *env;
	DB_THREAD_INFO *ip;
	__rep_control_args *rp;
	DBT *rec;
	time_t savetime;
	DB_LSN *ret_lsnp;
{
	DB_LSN last_lsn;
	DB_REP *db_rep;
	REP *rep;
	int ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	ret = __rep_log_split(env, ip, rp, rec, ret_lsnp, &last_lsn);
	switch (ret) {
	/*
	 * We're in an internal backup and we've gotten
	 * all the log we need to run recovery.  Do so now.
	 */
	case DB_REP_LOGREADY:
		ret = __rep_logready(env, rep, savetime, &last_lsn);
		break;
	/*
	 * Any other return (errors), we're done.
	 */
	default:
		break;
	}
	return (ret);
}

/*
 * __rep_log_split --
 *	- Split a log buffer into individual records.
 *
 * This is used by a client to process a bulk log message from the
 * master and convert it into individual __rep_apply requests.
 */
static int
__rep_log_split(env, ip, rp, rec, ret_lsnp, last_lsnp)
	ENV *env;
	DB_THREAD_INFO *ip;
	__rep_control_args *rp;
	DBT *rec;
	DB_LSN *ret_lsnp;
	DB_LSN *last_lsnp;
{
	DBT logrec;
	DB_LSN next_new_lsn, save_lsn, tmp_lsn;
	__rep_control_args tmprp;
	__rep_bulk_args b_args;
	int is_dup, ret, save_ret;
	u_int32_t save_flags;
	u_int8_t *p, *ep;

	memset(&logrec, 0, sizeof(logrec));
	ZERO_LSN(next_new_lsn);
	ZERO_LSN(save_lsn);
	ZERO_LSN(tmp_lsn);
	/*
	 * We're going to be modifying the rp LSN contents so make
	 * our own private copy to play with.
	 */
	memcpy(&tmprp, rp, sizeof(tmprp));
	/*
	 * We send the bulk buffer on a PERM record, so often we will have
	 * DB_LOG_PERM set.  However, we only want to mark the last LSN
	 * we have as a PERM record.  So clear it here, and when we're on
	 * the last record below, set it.  The same applies if the sender
	 * set REPCTL_LOG_END on this message.  We want the end of the
	 * bulk buffer to be marked as the end.
	 */
	save_flags = F_ISSET(rp, REPCTL_LOG_END | REPCTL_PERM);
	F_CLR(&tmprp, REPCTL_LOG_END | REPCTL_PERM);
	is_dup = ret = save_ret = 0;
	for (ep = (u_int8_t *)rec->data + rec->size, p = (u_int8_t *)rec->data;
	    p < ep; ) {
		/*
		 * First thing in the buffer is the length.  Then the LSN
		 * of this record, then the record itself.
		 */
		if (rp->rep_version < DB_REPVERSION_47) {
			memcpy(&b_args.len, p, sizeof(b_args.len));
			p += sizeof(b_args.len);
			memcpy(&tmprp.lsn, p, sizeof(DB_LSN));
			p += sizeof(DB_LSN);
			logrec.data = p;
			logrec.size = b_args.len;
			p += b_args.len;
		} else {
			if ((ret = __rep_bulk_unmarshal(env,
			    &b_args, p, rec->size, &p)) != 0)
				return (ret);
			tmprp.lsn = b_args.lsn;
			logrec.data = b_args.bulkdata.data;
			logrec.size = b_args.len;
		}
		VPRINT(env, (env, DB_VERB_REP_MISC,
		    "log_rep_split: Processing LSN [%lu][%lu]",
		    (u_long)tmprp.lsn.file, (u_long)tmprp.lsn.offset));
		VPRINT(env, (env, DB_VERB_REP_MISC,
    "log_rep_split: p %#lx ep %#lx logrec data %#lx, size %lu (%#lx)",
		    P_TO_ULONG(p), P_TO_ULONG(ep), P_TO_ULONG(logrec.data),
		    (u_long)logrec.size, (u_long)logrec.size));
		if (p >= ep && save_flags)
			F_SET(&tmprp, save_flags);
		/*
		 * A previous call to __rep_apply indicated an earlier
		 * record is a dup and the next_new_lsn we are waiting for.
		 * Skip log records until we catch up with next_new_lsn.
		 */
		if (is_dup && LOG_COMPARE(&tmprp.lsn, &next_new_lsn) < 0) {
			VPRINT(env, (env, DB_VERB_REP_MISC,
			    "log_split: Skip dup LSN [%lu][%lu]",
			    (u_long)tmprp.lsn.file, (u_long)tmprp.lsn.offset));
			continue;
		}
		is_dup = 0;
		ret = __rep_apply(env, ip,
		    &tmprp, &logrec, &tmp_lsn, &is_dup, last_lsnp);
		VPRINT(env, (env, DB_VERB_REP_MISC,
		    "log_split: rep_apply ret %d, dup %d, tmp_lsn [%lu][%lu]",
		    ret, is_dup, (u_long)tmp_lsn.file, (u_long)tmp_lsn.offset));
		if (is_dup)
			next_new_lsn = tmp_lsn;
		switch (ret) {
		/*
		 * If we received the pieces we need for running recovery,
		 * short-circuit because recovery will truncate the log to
		 * the LSN we want anyway.
		 */
		case DB_REP_LOGREADY:
			goto out;
		/*
		 * If we just handled a special record, retain that information.
		 */
		case DB_REP_ISPERM:
		case DB_REP_NOTPERM:
			save_ret = ret;
			save_lsn = tmp_lsn;
			ret = 0;
			break;
		/*
		 * Normal processing, do nothing, just continue.
		 */
		case 0:
			break;
		/*
		 * If we get an error, then stop immediately.
		 */
		default:
			goto out;
		}
	}
out:
	/*
	 * If we finish processing successfully, set our return values
	 * based on what we saw.
	 */
	if (ret == 0) {
		ret = save_ret;
		*ret_lsnp = save_lsn;
	}
	return (ret);
}

/*
 * __rep_log_req --
 *      Handle a REP_LOG_REQ message.
 *
 * PUBLIC: int __rep_logreq __P((ENV *, __rep_control_args *, DBT *, int));
 */
int
__rep_logreq(env, rp, rec, eid)
	ENV *env;
	__rep_control_args *rp;
	DBT *rec;
	int eid;
{
	DBT data_dbt, newfiledbt;
	DB_LOGC *logc;
	DB_LSN firstlsn, lsn, oldfilelsn;
	DB_REP *db_rep;
	REP *rep;
	REP_BULK bulk;
	REP_THROTTLE repth;
	__rep_logreq_args lr_args;
	__rep_newfile_args nf_args;
	uintptr_t bulkoff;
	u_int32_t bulkflags, use_bulk;
	int count, ret, t_ret;
	u_int8_t buf[__REP_NEWFILE_SIZE];
	size_t len;

	ret = 0;
	db_rep = env->rep_handle;
	rep = db_rep->region;

	/* COMPQUIET_LSN is what this is...  */
	ZERO_LSN(lr_args.endlsn);

	if (rec != NULL && rec->size != 0) {
		if (rp->rep_version < DB_REPVERSION_47)
			lr_args.endlsn = *(DB_LSN *)rec->data;
		else if ((ret = __rep_logreq_unmarshal(env, &lr_args,
		    rec->data, rec->size, NULL)) != 0)
			return (ret);
		RPRINT(env, (env, DB_VERB_REP_MISC,
		    "[%lu][%lu]: LOG_REQ max lsn: [%lu][%lu]",
		    (u_long) rp->lsn.file, (u_long)rp->lsn.offset,
		    (u_long)lr_args.endlsn.file,
		    (u_long)lr_args.endlsn.offset));
	}
	/*
	 * There are several different cases here.
	 * 1. We asked logc_get for a particular LSN and got it.
	 * 2. We asked logc_get for an LSN and it's not found because it is
	 *	beyond the end of a log file and we need a NEWFILE msg.
	 *	and then the record that was requested.
	 * 3. We asked logc_get for an LSN and it is already archived.
	 * 4. We asked logc_get for an LSN and it simply doesn't exist, but
	 *    doesn't meet any of those other criteria, in which case
	 *    it's an error (that should never happen on a master).
	 *
	 * If we have a valid LSN and the request has a data_dbt with
	 * it, the sender is asking for a chunk of log records.
	 * Then we need to send all records up to the LSN in the data dbt.
	 */
	memset(&data_dbt, 0, sizeof(data_dbt));
	oldfilelsn = lsn = rp->lsn;
	if ((ret = __log_cursor(env, &logc)) != 0)
		return (ret);
	REP_SYSTEM_LOCK(env);
	if ((ret = __rep_lockout_archive(env, rep)) != 0) {
		REP_SYSTEM_UNLOCK(env);
		goto err;
	}
	REP_SYSTEM_UNLOCK(env);
	if ((ret = __logc_get(logc, &lsn, &data_dbt, DB_SET)) == 0) {
		/* Case 1 */
		(void)__rep_send_message(env,
		   eid, REP_LOG, &lsn, &data_dbt, REPCTL_RESEND, 0);
		oldfilelsn.offset += logc->len;
	} else if (ret == DB_NOTFOUND) {
		/*
		 * If logc_get races with log_archive or the user removing
		 * files from an earlier call to log_archive, it might return
		 * DB_NOTFOUND.  We expect there to be some log record
		 * that is the first one.  Loop until we either get
		 * a log record or some error.  Since we only expect
		 * to get this racing log file removal, bound it to a few
		 * tries.
		 */
		count = 0;
		do {
			ret = __logc_get(logc, &firstlsn, &data_dbt, DB_FIRST);
			/*
			 * If we've raced this many tries and we're still
			 * getting DB_NOTFOUND, then pause a bit to disrupt
			 * the timing cycle that we appear to be in.
			 */
			if (count > 5)
				__os_yield(env, 0, 50000);
			count++;
		} while (ret == DB_NOTFOUND && count < 10);
		if (ret != 0) {
			/*
			 * If we're master we don't want to return DB_NOTFOUND.
			 * We'll just ignore the error and this message.
			 * It will get rerequested if needed.
			 */
			if (ret == DB_NOTFOUND && F_ISSET(rep, REP_F_MASTER))
				ret = 0;
			goto err;
		}
		if (LOG_COMPARE(&firstlsn, &rp->lsn) > 0) {
			/* Case 3 */
			if (F_ISSET(rep, REP_F_CLIENT)) {
				ret = DB_NOTFOUND;
				goto err;
			}
			(void)__rep_send_message(env, eid,
			    REP_VERIFY_FAIL, &rp->lsn, NULL, 0, 0);
			ret = 0;
			goto err;
		}
		ret = __rep_chk_newfile(env, logc, rep, rp, eid);
		if (ret == DB_NOTFOUND) {
			/* Case 4 */
			/*
			 * If we still get DB_NOTFOUND the client gave us an
			 * unknown LSN, perhaps at the end of the log.  Ignore
			 * it if we're the master.  Return DB_NOTFOUND if
			 * we are the client.
			 */
			if (F_ISSET(rep, REP_F_MASTER)) {
				__db_errx(env, DB_STR_A("3501",
				    "Request for LSN [%lu][%lu] not found",
				    "%lu %lu"), (u_long)rp->lsn.file,
				    (u_long)rp->lsn.offset);
				ret = 0;
				goto err;
			} else
				ret = DB_NOTFOUND;
		}
	}

	if (ret != 0)
		goto err;

	/*
	 * If the user requested a gap, send the whole thing, while observing
	 * the limits from rep_set_limit.
	 *
	 * If we're doing bulk transfer, allocate a bulk buffer to put our
	 * log records in.  We still need to initialize the throttle info
	 * because if we encounter a log record larger than our entire bulk
	 * buffer, we need to send it as a singleton.
	 *
	 * Use a local var so we don't need to worry if someone else turns
	 * on/off bulk in the middle of our call.
	 */
	use_bulk = FLD_ISSET(rep->config, REP_C_BULK);
	if (use_bulk && (ret = __rep_bulk_alloc(env, &bulk, eid,
	    &bulkoff, &bulkflags, REP_BULK_LOG)) != 0)
		goto err;
	memset(&repth, 0, sizeof(repth));
	REP_SYSTEM_LOCK(env);
	repth.gbytes = rep->gbytes;
	repth.bytes = rep->bytes;
	repth.type = REP_LOG;
	repth.data_dbt = &data_dbt;
	REP_SYSTEM_UNLOCK(env);
	while (ret == 0 && rec != NULL && rec->size != 0 &&
	    repth.type == REP_LOG) {
		if ((ret =
		    __logc_get(logc, &repth.lsn, &data_dbt, DB_NEXT)) != 0) {
			/*
			 * If we're a client and we only have part of the gap,
			 * return DB_NOTFOUND so that we send a REREQUEST
			 * back to the requester and it can ask for more.
			 */
			if (ret == DB_NOTFOUND && F_ISSET(rep, REP_F_MASTER))
				ret = 0;
			break;
		}
		if (LOG_COMPARE(&repth.lsn, &lr_args.endlsn) >= 0)
			break;
		if (repth.lsn.file != oldfilelsn.file) {
			if ((ret = __logc_version(logc, &nf_args.version)) != 0)
				break;
			memset(&newfiledbt, 0, sizeof(newfiledbt));
			if (rep->version < DB_REPVERSION_47)
				DB_INIT_DBT(newfiledbt, &nf_args.version,
				    sizeof(nf_args.version));
			else {
				if ((ret = __rep_newfile_marshal(env, &nf_args,
				    buf, __REP_NEWFILE_SIZE, &len)) != 0)
					goto err;
				DB_INIT_DBT(newfiledbt, buf, len);
			}
			(void)__rep_send_message(env,
			    eid, REP_NEWFILE, &oldfilelsn, &newfiledbt,
			    REPCTL_RESEND, 0);
		}
		/*
		 * If we are configured for bulk, try to send this as a bulk
		 * request.  If not configured, or it is too big for bulk
		 * then just send normally.
		 */
		if (use_bulk)
			ret = __rep_bulk_message(env, &bulk, &repth,
			    &repth.lsn, &data_dbt, REPCTL_RESEND);
		if (!use_bulk || ret == DB_REP_BULKOVF)
			ret = __rep_send_throttle(env, eid, &repth, 0, 0);
		if (ret != 0) {
			/* Ignore send failure, except to break the loop. */
			if (ret == DB_REP_UNAVAIL)
				ret = 0;
			break;
		}
		/*
		 * If we are about to change files, then we'll need the
		 * last LSN in the previous file.  Save it here.
		 */
		oldfilelsn = repth.lsn;
		oldfilelsn.offset += logc->len;
	}

	/*
	 * We're done, force out whatever remains in the bulk buffer and
	 * free it.
	 */
	if (use_bulk && (t_ret = __rep_bulk_free(env, &bulk,
	    REPCTL_RESEND)) != 0 && ret == 0 &&
	    t_ret != DB_REP_UNAVAIL)
		ret = t_ret;
err:
	/*
	 * We could have raced an unlink from an earlier log_archive
	 * and the user is removing the files themselves, now.  If
	 * we get an error indicating the log file might no longer
	 * exist, ignore it.
	 */
	if (ret == ENOENT)
		ret = 0;
	REP_SYSTEM_LOCK(env);
	FLD_CLR(rep->lockout_flags, REP_LOCKOUT_ARCHIVE);
	REP_SYSTEM_UNLOCK(env);
	if ((t_ret = __logc_close(logc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __rep_loggap_req -
 *	Request a log gap.  Assumes the caller holds the REP->mtx_clientdb.
 *
 * lsnp is the current LSN we're handling.  It is used to help decide
 *	if we ask for a gap or singleton.
 * gapflags are flags that may override the algorithm or control the
 *	processing in some way.
 *
 * PUBLIC: int __rep_loggap_req __P((ENV *, REP *, DB_LSN *, u_int32_t));
 */
int
__rep_loggap_req(env, rep, lsnp, gapflags)
	ENV *env;
	REP *rep;
	DB_LSN *lsnp;
	u_int32_t gapflags;
{
	DBT max_lsn_dbt, *max_lsn_dbtp;
	DB_LOG *dblp;
	DB_LSN next_lsn;
	LOG *lp;
	__rep_logreq_args lr_args;
	size_t len;
	u_int32_t ctlflags, flags, type;
	int master, ret;
	u_int8_t buf[__REP_LOGREQ_SIZE];

	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	if (FLD_ISSET(gapflags, REP_GAP_FORCE))
		next_lsn = *lsnp;
	else
		next_lsn = lp->ready_lsn;
	ctlflags = flags = 0;
	type = REP_LOG_REQ;
	ret = 0;

	/*
	 * Check if we need to ask for the gap.
	 * We ask for the gap if:
	 *	We are forced to with gapflags.
	 *	If max_wait_lsn is ZERO_LSN - we've never asked for
	 *	  records before.
	 *	If we asked for a single record and received it.
	 *
	 * If we want a gap, but don't have an ending LSN (waiting_lsn)
	 * send an ALL_REQ.  This is primarily used by REP_REREQUEST when
	 * an ALL_REQ was not able to be fulfilled by another client.
	 */
	if (FLD_ISSET(gapflags, (REP_GAP_FORCE | REP_GAP_REREQUEST)) ||
	    IS_ZERO_LSN(lp->max_wait_lsn) ||
	    (lsnp != NULL && LOG_COMPARE(lsnp, &lp->max_wait_lsn) == 0)) {
		lp->max_wait_lsn = lp->waiting_lsn;
		/*
		 * In SYNC_LOG, make sure max_wait_lsn is set to avoid sending
		 * an ALL_REQ that could create an unnecessary dual data stream.
		 */
		if (rep->sync_state == SYNC_LOG &&
		    IS_ZERO_LSN(lp->max_wait_lsn))
			lp->max_wait_lsn = rep->last_lsn;
		/*
		 * If we are forcing a gap, we need to send a max_wait_lsn
		 * that may be beyond the current gap/waiting_lsn (but
		 * it may not be).  If we cannot determine any future
		 * waiting LSN, then it should be zero.  If we're in
		 * internal init, it should be our ending LSN.
		 */
		if (FLD_ISSET(gapflags, REP_GAP_FORCE)) {
			if (LOG_COMPARE(&lp->max_wait_lsn, lsnp) <= 0) {
				if (rep->sync_state == SYNC_LOG) {
					DB_ASSERT(env, LOG_COMPARE(lsnp,
					    &rep->last_lsn) <= 0);
					lp->max_wait_lsn = rep->last_lsn;
				} else
					ZERO_LSN(lp->max_wait_lsn);
			}
		}
		if (IS_ZERO_LSN(lp->max_wait_lsn))
			type = REP_ALL_REQ;
		memset(&max_lsn_dbt, 0, sizeof(max_lsn_dbt));
		lr_args.endlsn = lp->max_wait_lsn;
		if (rep->version < DB_REPVERSION_47)
			DB_INIT_DBT(max_lsn_dbt, &lp->max_wait_lsn,
			    sizeof(DB_LSN));
		else {
			if ((ret = __rep_logreq_marshal(env, &lr_args, buf,
			    __REP_LOGREQ_SIZE, &len)) != 0)
				goto err;
			DB_INIT_DBT(max_lsn_dbt, buf, len);
		}
		max_lsn_dbtp = &max_lsn_dbt;
		/*
		 * Gap requests are "new" and can go anywhere, unless
		 * this is already a re-request.
		 */
		if (FLD_ISSET(gapflags, REP_GAP_REREQUEST))
			flags = DB_REP_REREQUEST;
		else
			flags = DB_REP_ANYWHERE;
	} else {
		max_lsn_dbtp = NULL;
		lp->max_wait_lsn = next_lsn;
		/*
		 * If we're dropping to singletons, this is a re-request.
		 */
		flags = DB_REP_REREQUEST;
	}
	if ((master = rep->master_id) != DB_EID_INVALID) {
		STAT_INC(env,
		    rep, log_request, rep->stat.st_log_requested, master);
		if (rep->sync_state == SYNC_LOG)
			ctlflags = REPCTL_INIT;
		(void)__rep_send_message(env, master,
		    type, &next_lsn, max_lsn_dbtp, ctlflags, flags);
	} else
		(void)__rep_send_message(env, DB_EID_BROADCAST,
		    REP_MASTER_REQ, NULL, NULL, 0, 0);
err:
	return (ret);
}

/*
 * __rep_logready -
 *	Handle getting back REP_LOGREADY.  Any call to __rep_apply
 * can return it.
 *
 * PUBLIC: int __rep_logready __P((ENV *, REP *, time_t, DB_LSN *));
 */
int
__rep_logready(env, rep, savetime, last_lsnp)
	ENV *env;
	REP *rep;
	time_t savetime;
	DB_LSN *last_lsnp;
{
	REGENV *renv;
	REGINFO *infop;
	int ret;

	infop = env->reginfo;
	renv = infop->primary;
	if ((ret = __log_flush(env, NULL)) != 0)
		goto err;
	if ((ret = __rep_verify_match(env, last_lsnp, savetime)) != 0)
		goto err;

	REP_SYSTEM_LOCK(env);
	ZERO_LSN(rep->first_lsn);

	if (rep->originfo_off != INVALID_ROFF) {
		MUTEX_LOCK(env, renv->mtx_regenv);
		__env_alloc_free(infop, R_ADDR(infop, rep->originfo_off));
		MUTEX_UNLOCK(env, renv->mtx_regenv);
		rep->originfo_off = INVALID_ROFF;
	}

	rep->sync_state = SYNC_OFF;
	F_SET(rep, REP_F_NIMDBS_LOADED);
	ret = __rep_notify_threads(env, AWAIT_NIMDB);
	REP_SYSTEM_UNLOCK(env);
	if (ret != 0)
		goto err;

	return (0);

err:
	DB_ASSERT(env, ret != DB_REP_WOULDROLLBACK);
	__db_errx(env, DB_STR("3502",
	    "Client initialization failed.  Need to manually restore client"));
	return (__env_panic(env, ret));
}

/*
 * __rep_chk_newfile --
 *     Determine if getting DB_NOTFOUND is because we're at the
 * end of a log file and need to send a NEWFILE message.
 *
 * This function handles these cases:
 * [Case 1 was that we found the record we were looking for - it
 * is already handled by the caller.]
 * 2. We asked logc_get for an LSN and it's not found because it is
 *	beyond the end of a log file and we need a NEWFILE msg.
 * 3. We asked logc_get for an LSN and it simply doesn't exist, but
 *    doesn't meet any of those other criteria, in which case
 *    we return DB_NOTFOUND and the caller decides if it's an error.
 *
 * This function returns 0 if we had to send a message and the bad
 * LSN is dealt with and DB_NOTFOUND if this really is an unknown LSN
 * (on a client) and errors if it isn't found on the master.
 */
static int
__rep_chk_newfile(env, logc, rep, rp, eid)
	ENV *env;
	DB_LOGC *logc;
	REP *rep;
	__rep_control_args *rp;
	int eid;
{
	DBT data_dbt, newfiledbt;
	DB_LOG *dblp;
	DB_LSN endlsn;
	LOG *lp;
	__rep_newfile_args nf_args;
	int ret;
	u_int8_t buf[__REP_NEWFILE_SIZE];
	size_t len;

	ret = 0;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	memset(&data_dbt, 0, sizeof(data_dbt));
	LOG_SYSTEM_LOCK(env);
	endlsn = lp->lsn;
	LOG_SYSTEM_UNLOCK(env);
	if (endlsn.file > rp->lsn.file) {
		/*
		 * Case 2:
		 * Need to find the LSN of the last record in
		 * file lsn.file so that we can send it with
		 * the NEWFILE call.  In order to do that, we
		 * need to try to get {lsn.file + 1, 0} and
		 * then backup.
		 */
		endlsn.file = rp->lsn.file + 1;
		endlsn.offset = 0;
		if ((ret = __logc_get(logc,
		    &endlsn, &data_dbt, DB_SET)) != 0 ||
		    (ret = __logc_get(logc,
			&endlsn, &data_dbt, DB_PREV)) != 0) {
			RPRINT(env, (env, DB_VERB_REP_MISC,
			    "Unable to get prev of [%lu][%lu]",
			    (u_long)rp->lsn.file,
			    (u_long)rp->lsn.offset));
			/*
			 * We want to push the error back
			 * to the client so that the client
			 * does an internal backup.  The
			 * client asked for a log record
			 * we no longer have and it is
			 * outdated.
			 * XXX - This could be optimized by
			 * having the master perform and
			 * send a REP_UPDATE message.  We
			 * currently want the client to set
			 * up its 'update' state prior to
			 * requesting REP_UPDATE_REQ.
			 *
			 * If we're a client servicing a request
			 * just return DB_NOTFOUND.
			 */
			if (F_ISSET(rep, REP_F_MASTER)) {
				ret = 0;
				(void)__rep_send_message(env, eid,
				    REP_VERIFY_FAIL, &rp->lsn,
				    NULL, 0, 0);
			} else
				ret = DB_NOTFOUND;
		} else {
			endlsn.offset += logc->len;
			if ((ret = __logc_version(logc,
			    &nf_args.version)) == 0) {
				memset(&newfiledbt, 0,
				    sizeof(newfiledbt));
				if (rep->version < DB_REPVERSION_47)
					DB_INIT_DBT(newfiledbt,
					    &nf_args.version,
					    sizeof(nf_args.version));
				else {
					if ((ret = __rep_newfile_marshal(env,
					    &nf_args, buf, __REP_NEWFILE_SIZE,
					    &len)) != 0)
						return (ret);
					DB_INIT_DBT(newfiledbt, buf, len);
				}
				(void)__rep_send_message(env, eid,
				    REP_NEWFILE, &endlsn,
				    &newfiledbt, REPCTL_RESEND, 0);
			}
		}
	} else
		ret = DB_NOTFOUND;

	return (ret);
}
