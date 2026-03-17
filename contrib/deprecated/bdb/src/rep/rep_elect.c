/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2004, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"

/*
 * We need to check sites == nsites, not more than half
 * like we do in __rep_elect and the VOTE2 code.  The
 * reason is that we want to process all the incoming votes
 * and not short-circuit once we reach more than half.  The
 * real winner's vote may be in the last half.
 */
#define	IS_PHASE1_DONE(rep)						\
    ((rep)->sites >= (rep)->nsites && (rep)->winner != DB_EID_INVALID)

#define	I_HAVE_WON(rep, winner)						\
    ((rep)->votes >= (rep)->nvotes && winner == (rep)->eid)

static void __rep_cmp_vote __P((ENV *, REP *, int, DB_LSN *,
    u_int32_t, u_int32_t, u_int32_t, u_int32_t, u_int32_t));
static int __rep_elect_init
	       __P((ENV *, u_int32_t, u_int32_t, int *, u_int32_t *));
static int __rep_fire_elected __P((ENV *, REP *, u_int32_t));
static void __rep_elect_master __P((ENV *, REP *));
static int __rep_grow_sites __P((ENV *, u_int32_t));
static void __rep_send_vote __P((ENV *, DB_LSN *, u_int32_t,
    u_int32_t, u_int32_t, u_int32_t, u_int32_t, u_int32_t, int,
    u_int32_t, u_int32_t));
static int __rep_tally __P((ENV *, REP *, int, u_int32_t *, u_int32_t, int));
static int __rep_wait __P((ENV *, db_timeout_t *, int, u_int32_t, u_int32_t));

/*
 * __rep_elect_pp --
 *	Called after master failure to hold/participate in an election for
 *	a new master.
 *
 * PUBLIC:  int __rep_elect_pp
 * PUBLIC:      __P((DB_ENV *, u_int32_t, u_int32_t, u_int32_t));
 */
int
__rep_elect_pp(dbenv, given_nsites, nvotes, flags)
	DB_ENV *dbenv;
	u_int32_t given_nsites, nvotes;
	u_int32_t flags;
{
	DB_REP *db_rep;
	ENV *env;
	int ret;

	env = dbenv->env;
	db_rep = env->rep_handle;
	ret = 0;

	ENV_REQUIRES_CONFIG_XX(
	    env, rep_handle, "DB_ENV->rep_elect", DB_INIT_REP);

	if (APP_IS_REPMGR(env)) {
		__db_errx(env, DB_STR("3527",
"DB_ENV->rep_elect: cannot call from Replication Manager application"));
		return (EINVAL);
	}

	/* We need a transport function because we send messages. */
	if (db_rep->send == NULL) {
		__db_errx(env, DB_STR("3528",
    "DB_ENV->rep_elect: must be called after DB_ENV->rep_set_transport"));
		return (EINVAL);
	}

	if (!IS_REP_STARTED(env)) {
		__db_errx(env, DB_STR("3529",
	    "DB_ENV->rep_elect: must be called after DB_ENV->rep_start"));
		return (EINVAL);
	}

	if (IS_USING_LEASES(env) && given_nsites != 0) {
		__db_errx(env, DB_STR("3530",
	    "DB_ENV->rep_elect: nsites must be zero if leases configured"));
		return (EINVAL);
	}

	ret = __rep_elect_int(env, given_nsites, nvotes, flags);

	/*
	 * The DB_REP_IGNORE return code can be of use to repmgr (which of
	 * course calls __rep_elect_int directly), but it may too subtle to be
	 * useful for (Base API) applications: so preserve the pre-existing API
	 * behavior for applications by making this look like a 0.
	 */
	if (ret == DB_REP_IGNORE)
		ret = 0;
	return (ret);
}

/*
 * __rep_elect_int --
 *	Internal processing to hold/participate in an election for
 *	a new master after master failure.
 *
 * PUBLIC:  int __rep_elect_int
 * PUBLIC:      __P((ENV *, u_int32_t, u_int32_t, u_int32_t));
 */
int
__rep_elect_int(env, given_nsites, nvotes, flags)
	ENV *env;
	u_int32_t given_nsites, nvotes;
	u_int32_t flags;
{
	DB_LOG *dblp;
	DB_LOGC *logc;
	DB_LSN lsn;
	DB_REP *db_rep;
	DB_THREAD_INFO *ip;
	LOG *lp;
	REP *rep;
	int done, elected, in_progress;
	int need_req, ret, send_vote, t_ret;
	u_int32_t ack, ctlflags, data_gen, egen, nsites;
	u_int32_t orig_tally, priority, realpri, repflags, tiebreaker;
	db_timeout_t timeout;

	COMPQUIET(flags, 0);

	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;
	elected = 0;
	egen = 0;
	ret = 0;

	/*
	 * Specifying 0 for nsites signals us to use the value configured
	 * previously via rep_set_nsites.  Similarly, if the given nvotes is 0,
	 * it asks us to compute the value representing a simple majority.
	 */
	nsites = given_nsites == 0 ? rep->config_nsites : given_nsites;
	ack = nvotes == 0 ? ELECTION_MAJORITY(nsites) : nvotes;

	/*
	 * XXX
	 * If users give us less than a majority, they run the risk of
	 * having a network partition.  However, this also allows the
	 * scenario of master/1 client to elect the client.  Allow
	 * sub-majority values, but give a warning.
	 */
	if (ack <= (nsites / 2)) {
		__db_errx(env, DB_STR_A("3531",
    "DB_ENV->rep_elect:WARNING: nvotes (%d) is sub-majority with nsites (%d)",
		    "%d %d"), nvotes, nsites);
	}

	if (nsites < ack) {
		__db_errx(env, DB_STR_A("3532",
	    "DB_ENV->rep_elect: nvotes (%d) is larger than nsites (%d)",
		    "%d %d"), ack, nsites);
		return (EINVAL);
	}

	realpri = rep->priority;

	RPRINT(env, (env, DB_VERB_REP_ELECT,
	    "Start election nsites %d, ack %d, priority %d",
	    nsites, ack, realpri));

	/*
	 * Special case when having an election while running with
	 * sites of potentially mixed versions.  We set a bit indicating
	 * we're an electable site, but set our priority to 0.
	 * Old sites will never elect us, with 0 priority, but if all
	 * we have are new sites, then we can elect the best electable
	 * site of the group.
	 *     Thus 'priority' is this special, possibly-fake, effective
	 * priority that we'll use for this election, while 'realpri' is our
	 * real, configured priority, as retrieved from REP region.
	 */
	ctlflags = realpri != 0 ? REPCTL_ELECTABLE : 0;
	ENV_ENTER(env, ip);

	orig_tally = 0;
	/* If we are already master, simply broadcast that fact and return. */
	if (F_ISSET(rep, REP_F_MASTER)) {
master:		LOG_SYSTEM_LOCK(env);
		lsn = lp->lsn;
		LOG_SYSTEM_UNLOCK(env);
		(void)__rep_send_message(env,
		    DB_EID_BROADCAST, REP_NEWMASTER, &lsn, NULL, 0, 0);
		if (IS_USING_LEASES(env))
			ret = __rep_lease_refresh(env);
		if (ret == 0)
		    ret = DB_REP_IGNORE;
		goto envleave;
	}
	REP_SYSTEM_LOCK(env);

	/*
	 * If leases are configured, wait for them to expire, and
	 * see if we can discover the master while waiting.
	 */
	if (IS_USING_LEASES(env) &&
	    (timeout = __rep_lease_waittime(env)) != 0) {
		FLD_SET(rep->elect_flags, REP_E_PHASE0);
		egen = rep->egen;
		REP_SYSTEM_UNLOCK(env);
		VPRINT(env, (env, DB_VERB_REP_ELECT,
		    "PHASE0 waittime from rep_lease_waittime: %lu",
		    (u_long)timeout));
		(void)__rep_send_message(env, DB_EID_BROADCAST,
		    REP_MASTER_REQ, NULL, NULL, 0, 0);

		/*
		 * The only possible non-zero return from __rep_wait() is a
		 * panic for a mutex failure.  So the state of the PHASE0 flag
		 * doesn't matter much.  If that changes in the future, it is
		 * still best not to clear the flag after an error, because
		 * another thread might be in the middle of its PHASE0 wait (and
		 * not getting an error), so we wouldn't want to cut short its
		 * wait.  If there isn't another concurrent thread, the worst
		 * that would happen would be that we would leave the flag set,
		 * until the next time we came through here and completed a
		 * wait.  Note that the code here is the only place where we
		 * check this flag.
		 */
		if ((ret = __rep_wait(env,
		    &timeout, 0, egen, REP_E_PHASE0)) != 0)
			goto envleave;
		REP_SYSTEM_LOCK(env);
		repflags = rep->elect_flags;
		FLD_CLR(rep->elect_flags, REP_E_PHASE0);
		/*
		 * If any other thread cleared PHASE0 while we were waiting,
		 * then we're done.  Either we heard from a master, or some
		 * other thread completed its PHASE0 wait.
		 *
		 * Or, we could have waited long enough for our lease grant to
		 * expire.  Check it to make sure.
		 */
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "after PHASE0 wait, flags 0x%x, elect_flags 0x%x",
		    rep->flags, rep->elect_flags));
		if (!FLD_ISSET(repflags, REP_E_PHASE0) ||
		    __rep_islease_granted(env) || egen != rep->egen) {
			VPRINT(env, (env, DB_VERB_REP_ELECT,
    "PHASE0 Done: repflags 0x%x, egen %d rep->egen %d, lease_granted %d",
    repflags, egen, rep->egen, __rep_islease_granted(env)));
			goto unlck_lv;
		}
		F_SET(rep, REP_F_LEASE_EXPIRED);
	}

	/*
	 * After acquiring the mutex, and possibly waiting for leases to
	 * expire, without the mutex, we need to recheck our state.  It
	 * may have changed.  If we are now master, we're done.
	 */
	if (F_ISSET(rep, REP_F_MASTER)) {
		REP_SYSTEM_UNLOCK(env);
		goto master;
	}
	if ((ret = __rep_elect_init(env, nsites, ack,
	    &in_progress, &orig_tally)) != 0)
		goto unlck_lv;
	/*
	 * If another thread is in the middle of an election we
	 * just quietly return and not interfere.
	 */
	if (in_progress) {
		ret = DB_REP_IGNORE;
		goto unlck_lv;
	}

	/*
	 * Count threads in the guts of rep_elect, so that we only clear
	 * lockouts when the last thread is finishing.  The "guts" start here,
	 * and do not include the above test where we "quietly return" via
	 * envleave.
	 *
	 * Closely associated with that is the notion that the current thread
	 * "owns" the right to process the election at the current egen.  We set
	 * the local variable "egen" now to "our" egen; if rep->egen ever
	 * advances "out from under us" we know it's time to yield to a new
	 * generation.  Our egen value was vetted in __rep_elect_init(), and we
	 * have not dropped the mutex since then.
	 *
	 * Other than occasionally checking that "our" egen still matches the
	 * current latest rep->egen, there should be no use of rep->egen in this
	 * function after this point.
	 */
	rep->elect_th++;
	egen = rep->egen;
	RPRINT(env, (env, DB_VERB_REP_ELECT,
		"Election thread owns egen %lu", (u_long)egen));

	priority = lp->persist.version != DB_LOGVERSION ? 0 : realpri;
#ifdef	CONFIG_TEST
	/*
	 * This allows us to unit test the ELECTABLE flag simply by
	 * using the priority values.
	 */
	if (priority > 0 && priority <= 5) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
	   "Artificially setting priority 0 (ELECTABLE) for CONFIG_TEST mode"));
		DB_ASSERT(env, ctlflags == REPCTL_ELECTABLE);
		priority = 0;
	}
#endif
	__os_gettime(env, &rep->etime, 1);

	/*
	 * Default to the normal timeout unless the user configured
	 * a full election timeout and we think we need a full election.
	 */
	rep->full_elect = 0;
	timeout = rep->elect_timeout;
	if (!F_ISSET(rep, REP_F_GROUP_ESTD) && rep->full_elect_timeout != 0) {
		rep->full_elect = 1;
		timeout = rep->full_elect_timeout;
	}

	/*
	 * We need to lockout applying incoming log records during
	 * the election.  We need to use a special rep_lockout_apply
	 * instead of rep_lockout_msg because we do not want to
	 * lockout all incoming messages, like other VOTEs!
	 */
	if ((ret = __rep_lockout_apply(env, rep, 0)) != 0)
		goto err_locked;
	if ((ret = __rep_lockout_archive(env, rep)) != 0)
		goto err_locked;

	/*
	 * Since the lockout step (above) could have dropped the mutex, we must
	 * check to see if we still own the right to proceed with the election
	 * at this egen.
	 */
	if (rep->egen != egen) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Found egen %lu, abandon my election at egen %lu",
		    (u_long)rep->egen, (u_long)egen));
		goto err_locked;
	}

	/* Generate a randomized tiebreaker value. */
	__os_unique_id(env, &tiebreaker);

	FLD_SET(rep->elect_flags, REP_E_PHASE1);
	FLD_CLR(rep->elect_flags, REP_E_TALLY);
	/*
	 * We made sure that leases were expired before starting the
	 * election, but an existing master may be slow in responding.
	 * If, during lockout, acquiring mutexes, etc, the client has now
	 * re-granted its lease, we're done - a master exists.
	 */
	if (IS_USING_LEASES(env) &&
	     __rep_islease_granted(env)) {
		ret = 0;
		goto err_locked;
	}

	/*
	 * If we are in the middle of recovering or internal
	 * init, we participate, but we set our priority to 0
	 * and turn off REPCTL_ELECTABLE.  Check whether we
	 * are in an internal init state.  If not,
	 * then that is okay, we can be elected (i.e. we are not
	 * in an inconsistent state).
	 */
	INIT_LSN(lsn);
	if (ISSET_LOCKOUT_BDB(rep) || IN_INTERNAL_INIT(rep) ||
	    rep->sync_state == SYNC_UPDATE) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
	   "Setting priority 0, unelectable, due to internal init/recovery"));
		priority = 0;
		ctlflags = 0;
		data_gen = 0;
	} else {
		/*
		 * Use the last commit record as the LSN in the vote.
		 */
		if ((ret = __log_cursor(env, &logc)) != 0)
			goto err_locked;
		/*
		 * If we've walked back and there are no commit records,
		 * then reset LSN to INIT_LSN.
		 */
		if ((ret = __rep_log_backup(env,
		    logc, &lsn, REP_REC_COMMIT)) == DB_NOTFOUND) {
			INIT_LSN(lsn);
			ret = 0;
		}
		if ((t_ret = __logc_close(logc)) != 0 && ret == 0)
			ret = t_ret;
		if (ret != 0)
			goto err_locked;
		if ((ret = __rep_get_datagen(env, &data_gen)) != 0)
			goto err_locked;
	}

	/*
	 * We are about to participate at this egen.  We must
	 * write out the next egen before participating in this one
	 * so that if we crash we can never participate in this egen
	 * again.
	 */
	if ((ret = __rep_write_egen(env, rep, egen + 1)) != 0)
		goto err_locked;

	/* Tally our own vote */
	if ((ret = __rep_tally(env, rep, rep->eid, &rep->sites, egen, 1))
	    != 0) {
		/*
		 * __rep_tally is telling us that this vote is a duplicate.  But
		 * this is our own vote in this case, and that should be
		 * impossible for a given egen.
		 */
		DB_ASSERT(env, ret != DB_REP_IGNORE);
		goto err_locked;
	}
	__rep_cmp_vote(env, rep, rep->eid, &lsn, priority, rep->gen, data_gen,
	    tiebreaker, ctlflags);

	RPRINT(env, (env, DB_VERB_REP_ELECT, "Beginning an election"));

	/*
	 * Now send vote, remembering the details in case we need them later in
	 * order to send out a duplicate VOTE1.  We must save the nsites and
	 * nvotes values that we originally send in the VOTE1 message, separate
	 * from rep->nsites and rep->nvotes, since the latter can change when we
	 * receive a VOTE1 from another site.
	 */
	send_vote = DB_EID_INVALID;
	done = IS_PHASE1_DONE(rep);
	rep->vote1.lsn = lsn;
	rep->vote1.nsites = nsites;
	rep->vote1.nvotes = ack;
	rep->vote1.priority = priority;
	rep->vote1.tiebreaker = tiebreaker;
	rep->vote1.ctlflags = ctlflags;
	rep->vote1.data_gen = data_gen;
	REP_SYSTEM_UNLOCK(env);

	__rep_send_vote(env, &lsn, nsites, ack, priority, tiebreaker, egen,
	    data_gen, DB_EID_BROADCAST, REP_VOTE1, ctlflags);
	DB_ENV_TEST_RECOVERY(env, DB_TEST_ELECTVOTE1, ret, NULL);
	if (done) {
		REP_SYSTEM_LOCK(env);
		goto vote;
	}

	ret = __rep_wait(env, &timeout, rep->full_elect, egen, REP_E_PHASE1);
	REP_SYSTEM_LOCK(env);
	if (ret != 0)
		goto err_locked;
	if (rep->egen > egen)
		/*
		 * For one reason or another, this election cycle is over; it
		 * doesn't matter why.
		 */
		goto out;

	if (FLD_ISSET(rep->elect_flags, REP_E_PHASE2)) {
		/* Received enough votes while waiting to move us to phase 2. */
		REP_SYSTEM_UNLOCK(env);
		goto phase2;
	}

	/*
	 * If we got here, we haven't heard from everyone, but we've
	 * run out of time, so it's time to decide if we have enough
	 * votes to pick a winner and if so, to send out a vote to
	 * the winner.
	 */
	if (rep->sites >= rep->nvotes) {
vote:
		/* We think we've seen enough to cast a vote. */
		send_vote = rep->winner;
		/*
		 * See if we won.  This will make sure we
		 * don't count ourselves twice if we're racing
		 * with incoming votes.
		 */
		if (rep->winner == rep->eid) {
			if ((ret =__rep_tally(env,
			    rep, rep->eid, &rep->votes, egen, 2)) != 0 &&
			    ret != DB_REP_IGNORE)
				goto err_locked;
			RPRINT(env, (env, DB_VERB_REP_ELECT,
			    "Counted my vote %d", rep->votes));
		}
		FLD_SET(rep->elect_flags, REP_E_PHASE2);
		FLD_CLR(rep->elect_flags, REP_E_PHASE1);
	}
	if (send_vote == DB_EID_INVALID) {
		/* We do not have enough votes to elect. */
		if (rep->sites >= rep->nvotes)
			__db_errx(env, DB_STR_A("3533",
	    "No electable site found: recvd %d of %d votes from %d sites",
			    "%d %d %d"), rep->sites, rep->nvotes, rep->nsites);
		else
			__db_errx(env, DB_STR_A("3534",
	    "Not enough votes to elect: recvd %d of %d from %d sites",
			    "%d %d %d"), rep->sites, rep->nvotes, rep->nsites);
		ret = DB_REP_UNAVAIL;
		goto err_locked;
	}
	REP_SYSTEM_UNLOCK(env);

	/*
	 * We have seen enough vote1's.  Now we need to wait
	 * for all the vote2's.
	 */
	if (send_vote != rep->eid) {
		RPRINT(env, (env, DB_VERB_REP_ELECT, "Sending vote"));
		__rep_send_vote(env, NULL, 0, 0, 0, 0, egen, 0,
		    send_vote, REP_VOTE2, 0);
		/*
		 * If we are NOT the new master we want to send
		 * our vote to the winner, and wait longer.  The
		 * reason is that the winner may be "behind" us
		 * in the election waiting and if the master is
		 * down, the winner will wait the full timeout
		 * and we want to give the winner enough time to
		 * process all the votes.  Otherwise we could
		 * incorrectly return DB_REP_UNAVAIL and start a
		 * new election before the winner can declare
		 * itself.
		 */
		timeout = timeout * 2;
	}

phase2:
	if (I_HAVE_WON(rep, rep->winner)) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Skipping phase2 wait: already got %d votes", rep->votes));
		REP_SYSTEM_LOCK(env);
		goto i_won;
	}
	ret = __rep_wait(env, &timeout, rep->full_elect, egen, REP_E_PHASE2);
	REP_SYSTEM_LOCK(env);
	/*
	 * Since at "err_lock" we're expected to have the lock, it's convenient
	 * to acquire it before testing "ret" here, since we need it anyway for
	 * the following stuff.
	 */
	if (ret != 0)
		goto err_locked;
	if (rep->egen > egen || !IN_ELECTION(rep))
		goto out;

	/* We must have timed out. */
	ret = DB_REP_UNAVAIL;

	RPRINT(env, (env, DB_VERB_REP_ELECT,
	    "After phase 2: votes %d, nvotes %d, nsites %d",
	    rep->votes, rep->nvotes, rep->nsites));

	if (I_HAVE_WON(rep, rep->winner)) {
i_won:		__rep_elect_master(env, rep);
		ret = 0;
		elected = 1;
	}
err_locked:
	/*
	 * If we get here because of a non-election error, then we did not tally
	 * our vote.  In that case we do not want to discard all known election
	 * info.
	 */
	if (ret == 0 || ret == DB_REP_UNAVAIL)
		__rep_elect_done(env, rep);
	else if (orig_tally)
		FLD_SET(rep->elect_flags, orig_tally);

#ifdef CONFIG_TEST
	if (0) {
DB_TEST_RECOVERY_LABEL
		REP_SYSTEM_LOCK(env);
	}
#endif

out:
	/*
	 * We're leaving, so decrement thread count.  If it's still >0 after
	 * that, another thread has come along to handle a later egen.  Only the
	 * last thread to come through here should clear the lockouts.
	 */
	need_req = 0;
	DB_ASSERT(env, rep->elect_th > 0);
	rep->elect_th--;
	if (rep->elect_th == 0) {
		need_req = F_ISSET(rep, REP_F_SKIPPED_APPLY) &&
		    !I_HAVE_WON(rep, rep->winner);
		FLD_CLR(rep->lockout_flags, REP_LOCKOUT_APPLY);
		F_CLR(rep, REP_F_SKIPPED_APPLY);
	}
	/*
	 * Only clear archiving lockout if the election failed.  If
	 * it succeeded, we keep archiving disabled until we either
	 * become master or complete synchronization with a master.
	 */
	if (ret != 0 && rep->elect_th == 0)
		FLD_CLR(rep->lockout_flags, REP_LOCKOUT_ARCHIVE);
	REP_SYSTEM_UNLOCK(env);
	/*
	 * If we skipped any log records, request them now.
	 */
	if (need_req && (t_ret = __rep_resend_req(env, 0)) != 0 &&
	    (ret == 0 || ret == DB_REP_UNAVAIL || ret == DB_REP_IGNORE))
		ret = t_ret;

	/* Note that "elected" implies ret cannot be DB_REP_UNAVAIL here. */
	if (elected) {
		/*
		 * The only way ret can be non-zero is if __rep_resend_req()
		 * failed.  So we don't have to check for UNAVAIL and IGNORE in
		 * deciding whether we're overwriting ret, as we did above.
		 */
		DB_ASSERT(env, ret != DB_REP_UNAVAIL && ret != DB_REP_IGNORE);
		if ((t_ret = __rep_fire_elected(env, rep, egen)) != 0 &&
		    ret == 0)
			ret = t_ret;
	}

	RPRINT(env, (env, DB_VERB_REP_ELECT,
	    "%s %d, e_th %lu, egen %lu, flag 0x%lx, e_fl 0x%lx, lo_fl 0x%lx",
	    "Ended election with ", ret,
	    (u_long) rep->elect_th, (u_long)rep->egen,
	    (u_long)rep->flags, (u_long)rep->elect_flags,
	    (u_long)rep->lockout_flags));

	if (0) {
unlck_lv:	REP_SYSTEM_UNLOCK(env);
	}
envleave:
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * __rep_vote1 --
 *	Handle incoming vote1 message on a client.
 *
 * PUBLIC: int __rep_vote1 __P((ENV *, __rep_control_args *, DBT *, int));
 */
int
__rep_vote1(env, rp, rec, eid)
	ENV *env;
	__rep_control_args *rp;
	DBT *rec;
	int eid;
{
	DBT data_dbt;
	DB_LOG *dblp;
	DB_LSN lsn;
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	REP_OLD_VOTE_INFO *ovi;
	VOTE1_CONTENT vote1;
	__rep_egen_args egen_arg;
	__rep_vote_info_v5_args tmpvi5;
	__rep_vote_info_args tmpvi, *vi;
	u_int32_t egen;
	int elected, master, resend, ret;
	u_int8_t buf[__REP_MAXMSG_SIZE];
	size_t len;

	COMPQUIET(egen, 0);

	elected = resend = ret = 0;
	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;

	if (F_ISSET(rep, REP_F_MASTER)) {
		RPRINT(env, (env, DB_VERB_REP_ELECT, "Master received vote"));
		LOG_SYSTEM_LOCK(env);
		lsn = lp->lsn;
		LOG_SYSTEM_UNLOCK(env);
		(void)__rep_send_message(env,
		    DB_EID_BROADCAST, REP_NEWMASTER, &lsn, NULL, 0, 0);
		return (ret);
	}

	/*
	 * In 4.7 we changed to having fixed sized u_int32_t's from
	 * non-fixed 'int' fields in the vote structure.
	 */
	if (rp->rep_version < DB_REPVERSION_47) {
		ovi = (REP_OLD_VOTE_INFO *)rec->data;
		tmpvi.egen = ovi->egen;
		tmpvi.nsites = (u_int32_t)ovi->nsites;
		tmpvi.nvotes = (u_int32_t)ovi->nvotes;
		tmpvi.priority = (u_int32_t)ovi->priority;
		tmpvi.tiebreaker = ovi->tiebreaker;
		tmpvi.data_gen = 0;
	} else if (rp->rep_version < DB_REPVERSION_52) {
		if ((ret = __rep_vote_info_v5_unmarshal(env,
		    &tmpvi5, rec->data, rec->size, NULL)) != 0)
			return (ret);
		tmpvi.egen = tmpvi5.egen;
		tmpvi.nsites = tmpvi5.nsites;
		tmpvi.nvotes = tmpvi5.nvotes;
		tmpvi.priority = tmpvi5.priority;
		tmpvi.tiebreaker = tmpvi5.tiebreaker;
		tmpvi.data_gen = 0;
	} else
		if ((ret = __rep_vote_info_unmarshal(env,
		    &tmpvi, rec->data, rec->size, NULL)) != 0)
			return (ret);
	vi = &tmpvi;
	REP_SYSTEM_LOCK(env);

	/*
	 * If we get a vote from a later election gen, we
	 * clear everything from the current one, and we'll
	 * start over by tallying it.  If we get an old vote,
	 * send an ALIVE to the old participant.
	 */
	RPRINT(env, (env, DB_VERB_REP_ELECT,
	    "Received vote1 egen %lu, egen %lu",
	    (u_long)vi->egen, (u_long)rep->egen));
	if (vi->egen < rep->egen) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Received old vote %lu, egen %lu, ignoring vote1",
		    (u_long)vi->egen, (u_long)rep->egen));
		egen_arg.egen = rep->egen;
		REP_SYSTEM_UNLOCK(env);
		if (rep->version < DB_REPVERSION_47)
			DB_INIT_DBT(data_dbt, &egen_arg.egen,
			    sizeof(egen_arg.egen));
		else {
			if ((ret = __rep_egen_marshal(env,
			    &egen_arg, buf, __REP_EGEN_SIZE, &len)) != 0)
				return (ret);
			DB_INIT_DBT(data_dbt, buf, len);
		}
		(void)__rep_send_message(env,
		    eid, REP_ALIVE, &rp->lsn, &data_dbt, 0, 0);
		return (0);
	}
	if (vi->egen > rep->egen) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Received VOTE1 from egen %lu, my egen %lu",
		    (u_long)vi->egen, (u_long)rep->egen));
		/*
		 * Terminate an election that may be in progress at the old
		 * egen.  Whether or not there was one, this call will result in
		 * HOLDELECTION (assuming no unexpected failures crop up).
		 */
		__rep_elect_done(env, rep);
		rep->egen = vi->egen;
	}

	/*
	 * If this site (sender of the VOTE1) is the first to the party, simply
	 * initialize values from the message.  Otherwise, see if the site knows
	 * about more sites, and/or requires more votes, than we do.
	 */
	if (!IN_ELECTION_TALLY(rep)) {
		FLD_SET(rep->elect_flags, REP_E_TALLY);
		rep->nsites = vi->nsites;
		rep->nvotes = vi->nvotes;
	} else {
		if (vi->nsites > rep->nsites)
			rep->nsites = vi->nsites;
		if (vi->nvotes > rep->nvotes)
			rep->nvotes = vi->nvotes;
	}

	/*
	 * Ignore vote1's if we're in phase 2.
	 */
	if (FLD_ISSET(rep->elect_flags, REP_E_PHASE2)) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "In phase 2, ignoring vote1"));
		goto err;
	}

	/*
	 * Record this vote.  If we're ignoring it, there's nothing more we need
	 * to do.
	 */
	if ((ret = __rep_tally(env, rep, eid, &rep->sites, vi->egen, 1)) != 0) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Tally returned %d, sites %d", ret, rep->sites));
		if (ret == DB_REP_IGNORE)
			ret = 0;
		goto err;
	}

	RPRINT(env, (env, DB_VERB_REP_ELECT,
"Incoming vote: (eid)%d (pri)%lu %s (gen)%lu (egen)%lu (datagen)%lu [%lu,%lu]",
	    eid, (u_long)vi->priority,
	    F_ISSET(rp, REPCTL_ELECTABLE) ? "ELECTABLE" : "",
	    (u_long)rp->gen, (u_long)vi->egen, (u_long)vi->data_gen,
	    (u_long)rp->lsn.file, (u_long)rp->lsn.offset));
	if (rep->sites > 1)
		RPRINT(env, (env, DB_VERB_REP_ELECT,
"Existing vote: (eid)%d (pri)%lu (gen)%lu (datagen)%lu (sites)%d [%lu,%lu]",
		    rep->winner, (u_long)rep->w_priority,
		    (u_long)rep->w_gen, (u_long)rep->w_datagen, rep->sites,
		    (u_long)rep->w_lsn.file,
		    (u_long)rep->w_lsn.offset));

	__rep_cmp_vote(env, rep, eid, &rp->lsn, vi->priority,
	    rp->gen, vi->data_gen, vi->tiebreaker, rp->flags);
	/*
	 * If you get a vote and you're not yet "in an election" at the proper
	 * egen, we've already recorded this vote.  But that is all we need to
	 * do.  But if you are in an election, check to see if we ought to send
	 * an extra VOTE1.  We know that the VOTE1 we have received is not a
	 * duplicated, because of the successful return from __rep_tally(),
	 * above.
	 */
	if (IN_ELECTION(rep)) {
		/*
		 * If we're doing a full election, and we're into phase 1 (no
		 * REP_E_TALLY), then resend, in case the sender of this VOTE1
		 * missed our VOTE1.
		 */
		if (rep->full_elect &&
		    FLD_ISSET((rep)->elect_flags, REP_E_PHASE1)) {
			resend = 1;
			vote1 = rep->vote1;
			egen = rep->egen;
		}
	} else {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Not in election, but received vote1 0x%x 0x%x",
		    rep->flags, rep->elect_flags));
		ret = DB_REP_HOLDELECTION;
		goto err;
	}

	master = rep->winner;
	lsn = rep->w_lsn;
	if (IS_PHASE1_DONE(rep)) {
		RPRINT(env, (env, DB_VERB_REP_ELECT, "Phase1 election done"));
		RPRINT(env, (env, DB_VERB_REP_ELECT, "Voting for %d%s",
		    master, master == rep->eid ? "(self)" : ""));
		egen = rep->egen;
		FLD_SET(rep->elect_flags, REP_E_PHASE2);
		FLD_CLR(rep->elect_flags, REP_E_PHASE1);
		if (master == rep->eid) {
			if ((ret =__rep_tally(env, rep, rep->eid,
			    &rep->votes, egen, 2)) != 0 &&
			    ret != DB_REP_IGNORE)
				goto err;
			ret = 0;
			RPRINT(env, (env, DB_VERB_REP_ELECT,
			    "After phase 1 done: counted vote %d of %d",
			    rep->votes, rep->nvotes));
			if (I_HAVE_WON(rep, rep->winner)) {
				__rep_elect_master(env, rep);
				elected = 1;
			}
			goto err;
		}
		REP_SYSTEM_UNLOCK(env);

		/* Vote for someone else. */
		__rep_send_vote(env, NULL, 0, 0, 0, 0, egen, 0,
		    master, REP_VOTE2, 0);
	} else
err:		REP_SYSTEM_UNLOCK(env);

	/*
	 * Note that if we're elected, there's no need for resending our VOTE1,
	 * even if we thought it might have been necessary a moment ago.
	 */
	if (elected)
		ret = __rep_fire_elected(env, rep, egen);
	else if (resend)
		__rep_send_vote(env,
		    &vote1.lsn, vote1.nsites, vote1.nvotes, vote1.priority,
		    vote1.tiebreaker, egen, vote1.data_gen,
		    eid, REP_VOTE1, vote1.ctlflags);
	return (ret);
}

/*
 * __rep_vote2 --
 *	Handle incoming vote2 message on a client.
 *
 * PUBLIC: int __rep_vote2 __P((ENV *, __rep_control_args *, DBT *, int));
 */
int
__rep_vote2(env, rp, rec, eid)
	ENV *env;
	__rep_control_args *rp;
	DBT *rec;
	int eid;
{
	DB_LOG *dblp;
	DB_LSN lsn;
	DB_REP *db_rep;
	LOG *lp;
	REP *rep;
	REP_OLD_VOTE_INFO *ovi;
	__rep_vote_info_args tmpvi, *vi;
	u_int32_t egen;
	int ret;

	ret = 0;
	db_rep = env->rep_handle;
	rep = db_rep->region;
	dblp = env->lg_handle;
	lp = dblp->reginfo.primary;

	RPRINT(env, (env, DB_VERB_REP_ELECT, "We received a vote%s",
	    F_ISSET(rep, REP_F_MASTER) ? " (master)" : ""));
	if (F_ISSET(rep, REP_F_MASTER)) {
		LOG_SYSTEM_LOCK(env);
		lsn = lp->lsn;
		LOG_SYSTEM_UNLOCK(env);
		(void)__rep_send_message(env,
		    DB_EID_BROADCAST, REP_NEWMASTER, &lsn, NULL, 0, 0);
		if (IS_USING_LEASES(env))
			ret = __rep_lease_refresh(env);
		return (ret);
	}

	REP_SYSTEM_LOCK(env);
	egen = rep->egen;

	/*
	 * We might be the last to the party and we haven't had
	 * time to tally all the vote1's, but others have and
	 * decided we're the winner.  So, if we're in the process
	 * of tallying sites, keep the vote so that when our
	 * election thread catches up we'll have the votes we
	 * already received.
	 */
	/*
	 * In 4.7 we changed to having fixed sized u_int32_t's from
	 * non-fixed 'int' fields in the vote structure.
	 */
	if (rp->rep_version < DB_REPVERSION_47) {
		ovi = (REP_OLD_VOTE_INFO *)rec->data;
		tmpvi.egen = ovi->egen;
		tmpvi.nsites = (u_int32_t)ovi->nsites;
		tmpvi.nvotes = (u_int32_t)ovi->nvotes;
		tmpvi.priority = (u_int32_t)ovi->priority;
		tmpvi.tiebreaker = ovi->tiebreaker;
	} else
		if ((ret = __rep_vote_info_unmarshal(env,
		    &tmpvi, rec->data, rec->size, NULL)) != 0)
			return (ret);
	vi = &tmpvi;
	if (!IN_ELECTION_TALLY(rep) && vi->egen >= rep->egen) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Not in election gen %lu, at %lu, got vote",
		    (u_long)vi->egen, (u_long)rep->egen));
		ret = DB_REP_HOLDELECTION;
		goto err;
	}

	/*
	 * Record this vote.  In a VOTE2, the only valid entry
	 * in the vote information is the election generation.
	 *
	 * There are several things which can go wrong that we
	 * need to account for:
	 * 1. If we receive a latent VOTE2 from an earlier election,
	 * we want to ignore it.
	 * 2. If we receive a VOTE2 from a site from which we never
	 * received a VOTE1, we want to record it, because we simply
	 * may be processing messages out of order or its vote1 got lost,
	 * but that site got all the votes it needed to send it.
	 * 3. If we have received a duplicate VOTE2 from this election
	 * from the same site we want to ignore it.
	 * 4. If this is from the current election and someone is
	 * really voting for us, then we finally get to record it.
	 */
	/*
	 * Case 1.
	 */
	if (vi->egen != rep->egen) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Bad vote egen %lu.  Mine %lu",
		    (u_long)vi->egen, (u_long)rep->egen));
		ret = 0;
		goto err;
	}

	/*
	 * __rep_tally takes care of cases 2, 3 and 4.
	 */
	if ((ret = __rep_tally(env, rep, eid, &rep->votes, vi->egen, 2)) != 0) {
		if (ret == DB_REP_IGNORE)
			ret = 0;
		goto err;
	}
	RPRINT(env, (env, DB_VERB_REP_ELECT, "Counted vote %d of %d",
	    rep->votes, rep->nvotes));
	if (I_HAVE_WON(rep, rep->winner)) {
		__rep_elect_master(env, rep);
		ret = DB_REP_NEWMASTER;
	}

err:	REP_SYSTEM_UNLOCK(env);
	if (ret == DB_REP_NEWMASTER)
		ret = __rep_fire_elected(env, rep, egen);
	return (ret);
}

/*
 * __rep_tally --
 *	Handle incoming vote message on a client.  This will record either a
 *	VOTE1 or a VOTE2, depending on the "phase" value the caller passed in.
 *
 *	This function will return:
 *	    0                if we successfully tally the vote;
 *	    DB_REP_IGNORE    if the vote is properly ignored;
 *	    (anything else)  in case of an unexpected error.
 *
 *	!!! Caller must hold REP_SYSTEM_LOCK.
 */
static int
__rep_tally(env, rep, eid, countp, egen, phase)
	ENV *env;
	REP *rep;
	int eid;
	u_int32_t *countp;
	u_int32_t egen;
	int phase;
{
	REP_VTALLY *tally, *vtp;
	u_int32_t i;
	int ret;

	if (rep->nsites > rep->asites &&
	    (ret = __rep_grow_sites(env, rep->nsites)) != 0) {
		RPRINT(env, (env, DB_VERB_REP_ELECT,
		    "Grow sites returned error %d", ret));
		return (ret);
	}
	if (phase == 1)
		tally = R_ADDR(env->reginfo, rep->tally_off);
	else
		tally = R_ADDR(env->reginfo, rep->v2tally_off);
	vtp = &tally[0];
	for (i = 0; i < *countp;) {
		/*
		 * Ignore votes from earlier elections (i.e. we've heard
		 * from this site in this election, but its vote from an
		 * earlier election got delayed and we received it now).
		 * However, if we happened to hear from an earlier vote
		 * and we recorded it and we're now hearing from a later
		 * election we want to keep the updated one.  Note that
		 * updating the entry will not increase the count.
		 * Also ignore votes that are duplicates.
		 */
		if (vtp->eid == eid) {
			RPRINT(env, (env, DB_VERB_REP_ELECT,
			    "Tally found[%d] (%d, %lu), this vote (%d, %lu)",
				    i, vtp->eid, (u_long)vtp->egen,
				    eid, (u_long)egen));
			if (vtp->egen >= egen)
				return (DB_REP_IGNORE);
			else {
				vtp->egen = egen;
				return (0);
			}
		}
		i++;
		vtp = &tally[i];
	}

	/*
	 * If we get here, we have a new voter we haven't seen before.  Tally
	 * this vote.
	 */
	RPRINT(env, (env, DB_VERB_REP_ELECT, "Tallying VOTE%d[%d] (%d, %lu)",
	    phase, i, eid, (u_long)egen));

	vtp->eid = eid;
	vtp->egen = egen;
	(*countp)++;
	return (0);
}

/*
 * __rep_cmp_vote --
 *	Compare incoming vote1 message on a client.  Called with the db_rep
 *	mutex held.
 *
 */
static void
__rep_cmp_vote(env, rep, eid, lsnp, priority, gen, data_gen, tiebreaker, flags)
	ENV *env;
	REP *rep;
	int eid;
	DB_LSN *lsnp;
	u_int32_t priority;
	u_int32_t data_gen, flags, gen, tiebreaker;
{
	int cmp, like_pri;

	cmp = LOG_COMPARE(lsnp, &rep->w_lsn);
	/*
	 * If we've seen more than one, compare us to the best so far.
	 * If we're the first, make ourselves the winner to start.
	 */
	if (rep->sites > 1 &&
	    (priority != 0 || LF_ISSET(REPCTL_ELECTABLE))) {
		/*
		 * Special case, if we have a mixed version group of sites,
		 * we set priority to 0, but set the ELECTABLE flag so that
		 * all sites talking at lower versions can correctly elect.
		 * If a non-zero priority comes in and current winner is
		 * zero priority (but was electable), then the non-zero
		 * site takes precedence no matter what its LSN is.
		 *
		 * Then the data_gen determines the winner.  The site with
		 * the more recent generation of data wins.
		 *
		 * Then LSN is determinant only if we're comparing
		 * like-styled version/priorities at the same data_gen.  I.e.
		 * both with 0/ELECTABLE priority or both with non-zero
		 * priority.  Then actual priority value if LSNs
		 * are equal, then tiebreaker if both are equal.
		 */
		/*
		 * Make note if we're comparing the same types of priorities
		 * that indicate electability or not.  We know we are
		 * electable if we are here.
		 */
		like_pri = (priority == 0 && rep->w_priority == 0) ||
		    (priority != 0 && rep->w_priority != 0);

		if ((priority != 0 && rep->w_priority == 0) ||
		    (like_pri && data_gen > rep->w_datagen) ||
		    (like_pri && data_gen == rep->w_datagen && cmp > 0) ||
		    (cmp == 0 && (priority > rep->w_priority ||
		    (priority == rep->w_priority &&
		    (tiebreaker > rep->w_tiebreaker))))) {
			RPRINT(env, (env, DB_VERB_REP_ELECT,
			    "Accepting new vote"));
			rep->winner = eid;
			rep->w_priority = priority;
			rep->w_lsn = *lsnp;
			rep->w_gen = gen;
			rep->w_datagen = data_gen;
			rep->w_tiebreaker = tiebreaker;
		}
	} else if (rep->sites == 1) {
		if (priority != 0 || LF_ISSET(REPCTL_ELECTABLE)) {
			/* Make ourselves the winner to start. */
			rep->winner = eid;
			rep->w_priority = priority;
			rep->w_gen = gen;
			rep->w_datagen = data_gen;
			rep->w_lsn = *lsnp;
			rep->w_tiebreaker = tiebreaker;
		} else {
			rep->winner = DB_EID_INVALID;
			rep->w_priority = 0;
			rep->w_gen = 0;
			rep->w_datagen = 0;
			ZERO_LSN(rep->w_lsn);
			rep->w_tiebreaker = 0;
		}
	}
}

/*
 * __rep_elect_init
 *	Initialize an election.  Sets beginp non-zero if the election is
 * already in progress; makes it 0 otherwise.  Leaves it untouched if we return
 * DB_REP_NEWMASTER.
 *
 * Caller holds the REP_SYSTEM mutex, and relies on us not dropping it.
 */
static int
__rep_elect_init(env, nsites, nvotes, beginp, otally)
	ENV *env;
	u_int32_t nsites, nvotes;
	int *beginp;
	u_int32_t *otally;
{
	DB_REP *db_rep;
	REP *rep;
	int ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	ret = 0;

	if (otally != NULL)
		*otally = FLD_ISSET(rep->elect_flags, REP_E_TALLY);

	DB_ASSERT(env, rep->spent_egen <= rep->egen);
	*beginp = rep->spent_egen == rep->egen;
	if (!*beginp) {
		/*
		 * Make sure that we always initialize all the election fields
		 * before putting ourselves in an election state.  That means
		 * issuing calls that can fail (allocation) before setting all
		 * the variables.
		 */
		if (nsites > rep->asites &&
		    (ret = __rep_grow_sites(env, nsites)) != 0)
			goto err;
		DB_ENV_TEST_RECOVERY(env, DB_TEST_ELECTINIT, ret, NULL);
		rep->spent_egen = rep->egen;

		STAT_INC(env, rep, election, rep->stat.st_elections, rep->egen);

		/*
		 * If we're the first to the party, we simply set initial
		 * values: pre-existing values would be left over from previous
		 * election.
		 */
		if (!IN_ELECTION_TALLY(rep)) {
			rep->nsites = nsites;
			rep->nvotes = nvotes;
		} else {
			if (nsites > rep->nsites)
				rep->nsites = nsites;
			if (nvotes > rep->nvotes)
				rep->nvotes = nvotes;
		}
	}
DB_TEST_RECOVERY_LABEL
err:
	return (ret);
}

/*
 * __rep_elect_master
 *	Set up for new master from election.  Must be called with
 *	the replication region mutex held.
 */
static void
__rep_elect_master(env, rep)
	ENV *env;
	REP *rep;
{
	if (F_ISSET(rep, REP_F_MASTERELECT | REP_F_MASTER)) {
		/* We've been through here already; avoid double counting. */
		return;
	}

	F_SET(rep, REP_F_MASTERELECT);
	STAT_INC(env, rep, election_won, rep->stat.st_elections_won, rep->egen);

	RPRINT(env, (env, DB_VERB_REP_ELECT,
	    "Got enough votes to win; election done; (prev) gen %lu",
	    (u_long)rep->gen));
}

static int
__rep_fire_elected(env, rep, egen)
	ENV *env;
	REP *rep;
	u_int32_t egen;
{
	REP_EVENT_LOCK(env);
	if (rep->notified_egen < egen) {
		__rep_fire_event(env, DB_EVENT_REP_ELECTED, NULL);
		rep->notified_egen = egen;
	}
	REP_EVENT_UNLOCK(env);
	return (0);
}

/*
 * Compute a sleep interval.
 *
 * The user specifies an overall timeout function, but checking is cheap and the
 * timeout may be a generous upper bound.  So sleep for the smaller of .5s and
 * timeout/10.  Make sure we sleep at least 1usec if timeout < 10.
 */
#define	SLEEPTIME(timeout)					\
	((timeout > 5000000) ? 500000 : ((timeout >= 10) ? timeout / 10 : 1))

/*
 * __rep_wait --
 *
 * Sleep until the indicated phase is over, or the timeout expires.  The phase
 * is over when someone clears the phase flag (in the course of processing an
 * incoming message).  This could either be a normal progression one one phase
 * to the other, or it could be due to receiving a NEWMASTER or an egen change.
 * In all cases we simply return 0, and the caller should check the state of the
 * world (generally under mutex protection) to decide what to do next.
 */
static int
__rep_wait(env, timeoutp, full_elect, egen, flags)
	ENV *env;
	db_timeout_t *timeoutp;
	int full_elect;
	u_int32_t egen, flags;
{
	DB_REP *db_rep;
	REP *rep;
	int done;
	u_int32_t sleeptime, sleeptotal, timeout;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	done = 0;

	timeout = *timeoutp;
	sleeptime = SLEEPTIME(timeout);
	sleeptotal = 0;
	while (sleeptotal < timeout) {
		__os_yield(env, 0, sleeptime);
		sleeptotal += sleeptime;
		REP_SYSTEM_LOCK(env);
		/*
		 * Check if group membership changed while we were
		 * sleeping.  Specifically we're trying for a full
		 * election and someone is telling us we're joining
		 * a previously established replication group.  (This is not
		 * applicable for the phase 0 wait, which uses a completely
		 * unrelated timeout value.)
		 */
		if (!LF_ISSET(REP_E_PHASE0) &&
		    full_elect && F_ISSET(rep, REP_F_GROUP_ESTD)) {
			*timeoutp = rep->elect_timeout;
			timeout = *timeoutp;
			if (sleeptotal >= timeout)
				done = 1;
			else
				sleeptime = SLEEPTIME(timeout);
		}

		if (egen != rep->egen || !FLD_ISSET(rep->elect_flags, flags))
			done = 1;
		REP_SYSTEM_UNLOCK(env);

		if (done)
			return (0);
	}
	return (0);
}

/*
 * __rep_grow_sites --
 *	Called to allocate more space in the election tally information.
 * Called with the rep mutex held.  We need to call the region mutex, so
 * we need to make sure that we *never* acquire those mutexes in the
 * opposite order.
 */
static int
__rep_grow_sites(env, nsites)
	ENV *env;
	u_int32_t nsites;
{
	REGENV *renv;
	REGINFO *infop;
	REP *rep;
	int ret, *tally;
	u_int32_t nalloc;

	rep = env->rep_handle->region;

	/*
	 * Allocate either twice the current allocation or nsites,
	 * whichever is more.
	 */
	nalloc = 2 * rep->asites;
	if (nalloc < nsites)
		nalloc = nsites;

	infop = env->reginfo;
	renv = infop->primary;
	MUTEX_LOCK(env, renv->mtx_regenv);

	/*
	 * We allocate 2 tally regions, one for tallying VOTE1's and
	 * one for VOTE2's.  Always grow them in tandem, because if we
	 * get more VOTE1's we'll always expect more VOTE2's then too.
	 */
	if ((ret = __env_alloc(infop,
	    (size_t)nalloc * sizeof(REP_VTALLY), &tally)) == 0) {
		if (rep->tally_off != INVALID_ROFF)
			 __env_alloc_free(
			     infop, R_ADDR(infop, rep->tally_off));
		rep->tally_off = R_OFFSET(infop, tally);
		if ((ret = __env_alloc(infop,
		    (size_t)nalloc * sizeof(REP_VTALLY), &tally)) == 0) {
			/* Success */
			if (rep->v2tally_off != INVALID_ROFF)
				 __env_alloc_free(infop,
				    R_ADDR(infop, rep->v2tally_off));
			rep->v2tally_off = R_OFFSET(infop, tally);
			rep->asites = nalloc;
			rep->nsites = nsites;
		} else {
			/*
			 * We were unable to allocate both.  So, we must
			 * free the first one and reinitialize.  If
			 * v2tally_off is valid, it is from an old
			 * allocation and we are clearing it all out due
			 * to the error.
			 */
			if (rep->v2tally_off != INVALID_ROFF)
				 __env_alloc_free(infop,
				    R_ADDR(infop, rep->v2tally_off));
			__env_alloc_free(infop,
			    R_ADDR(infop, rep->tally_off));
			rep->v2tally_off = rep->tally_off = INVALID_ROFF;
			rep->asites = 0;
		}
	}
	MUTEX_UNLOCK(env, renv->mtx_regenv);
	return (ret);
}

/*
 * __rep_send_vote
 *	Send this site's vote for the election.
 */
static void
__rep_send_vote(env, lsnp,
 nsites, nvotes, pri, tie, egen, data_gen, eid, vtype, flags)
	ENV *env;
	DB_LSN *lsnp;
	int eid;
	u_int32_t nsites, nvotes, pri;
	u_int32_t flags, egen, data_gen, tie, vtype;
{
	DB_REP *db_rep;
	DBT vote_dbt;
	REP *rep;
	REP_OLD_VOTE_INFO ovi;
	__rep_vote_info_args vi;
	__rep_vote_info_v5_args vi5;
	u_int8_t buf[__REP_VOTE_INFO_SIZE];
	size_t len;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	memset(&vi, 0, sizeof(vi));
	memset(&vote_dbt, 0, sizeof(vote_dbt));

	/*
	 * In 4.7 we went to fixed sized fields.  They may not be
	 * the same as the sizes in older versions.  In 5.2 we
	 * added the data_gen.
	 */
	if (rep->version < DB_REPVERSION_47) {
		ovi.egen = egen;
		ovi.priority = (int) pri;
		ovi.nsites = (int) nsites;
		ovi.nvotes = (int) nvotes;
		ovi.tiebreaker = tie;
		DB_INIT_DBT(vote_dbt, &ovi, sizeof(ovi));
	} else if (rep->version < DB_REPVERSION_52) {
		vi5.egen = egen;
		vi5.priority = pri;
		vi5.nsites = nsites;
		vi5.nvotes = nvotes;
		vi5.tiebreaker = tie;
		(void)__rep_vote_info_v5_marshal(env, &vi5, buf,
		    __REP_VOTE_INFO_SIZE, &len);
		DB_INIT_DBT(vote_dbt, buf, len);
	} else {
		vi.egen = egen;
		vi.priority = pri;
		vi.nsites = nsites;
		vi.nvotes = nvotes;
		vi.tiebreaker = tie;
		vi.data_gen = data_gen;
		(void)__rep_vote_info_marshal(env, &vi, buf,
		    __REP_VOTE_INFO_SIZE, &len);
		DB_INIT_DBT(vote_dbt, buf, len);
	}

	(void)__rep_send_message(env, eid, vtype, lsnp, &vote_dbt, flags, 0);
}
