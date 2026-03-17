/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2005, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"

static db_timeout_t __repmgr_compute_response_time __P((ENV *));
static int __repmgr_elect __P((ENV *, u_int32_t, db_timespec *));
static int __repmgr_elect_main __P((ENV *, REPMGR_RUNNABLE *));
static void *__repmgr_elect_thread __P((void *));
static int send_membership __P((ENV *));

/*
 * Starts an election thread.
 *
 * PUBLIC: int __repmgr_init_election __P((ENV *, u_int32_t));
 *
 * !!!
 * Caller must hold mutex.
 */
int
__repmgr_init_election(env, flags)
	ENV *env;
	u_int32_t flags;
{
	DB_REP *db_rep;
	REPMGR_RUNNABLE *th;
	int ret;
	u_int i, new_size;

	COMPQUIET(th, NULL);

	db_rep = env->rep_handle;
	if (db_rep->repmgr_status == stopped) {
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "ignoring elect thread request %#lx; repmgr is stopped",
		    (u_long)flags));
		return (0);
	}

	/* Find an available slot, indexed by 'i'; allocate more if needed. */
	for (i = 0; i < db_rep->aelect_threads; i++) {
		th = db_rep->elect_threads[i];
		if (th == NULL)
			break;
		if (th->finished) {
			if ((ret = __repmgr_thread_join(th)) != 0)
				return (ret);
			/* Reuse the space in a moment. */
			break;
		}
	}
	if (i == db_rep->aelect_threads) {
		new_size = db_rep->aelect_threads + 1;
		if ((ret = __os_realloc(env,
		    sizeof(REPMGR_RUNNABLE*) * new_size,
		    &db_rep->elect_threads)) != 0)
			return (ret);
		db_rep->aelect_threads = new_size;
		STAT(db_rep->region->mstat.st_max_elect_threads = new_size);
		th = db_rep->elect_threads[i] = NULL;
	}

	if (th == NULL &&
	    (ret = __os_malloc(env, sizeof(REPMGR_RUNNABLE), &th)) != 0)
		return (ret);
	th->run = __repmgr_elect_thread;
	th->args.flags = flags;

	if ((ret = __repmgr_thread_start(env, th)) == 0)
		STAT(db_rep->region->mstat.st_elect_threads++);
	else {
		__os_free(env, th);
		th = NULL;
	}
	db_rep->elect_threads[i] = th;

	return (ret);
}

static void *
__repmgr_elect_thread(argsp)
	void *argsp;
{
	REPMGR_RUNNABLE *th;
	ENV *env;
	int ret;

	th = argsp;
	env = th->env;

	RPRINT(env, (env, DB_VERB_REPMGR_MISC, "starting election thread"));

	if ((ret = __repmgr_elect_main(env, th)) != 0) {
		__db_err(env, ret, "election thread failed");
		(void)__repmgr_thread_failure(env, ret);
	}

	RPRINT(env, (env, DB_VERB_REPMGR_MISC, "election thread is exiting"));
	th->finished = TRUE;
	return (NULL);
}

static int
__repmgr_elect_main(env, th)
	ENV *env;
	REPMGR_RUNNABLE *th;
{
	DB_REP *db_rep;
	REP *rep;
#ifdef DB_WIN32
	DWORD duration;
	db_timeout_t t;
#else
	struct timespec deadline;
#endif
	db_timespec failtime, now, repstart_time, target, wait_til;
	db_timeout_t delay_time, response_time, tmp_time;
	u_long sec, usec;
	u_int32_t flags;
	int done_repstart, ret, suppress_election;
	enum { ELECTION, REPSTART } action;

	COMPQUIET(action, ELECTION);

	db_rep = env->rep_handle;
	rep = db_rep->region;
	flags = th->args.flags;

	if (LF_ISSET(ELECT_F_EVENT_NOTIFY))
		DB_EVENT(env, DB_EVENT_REP_MASTER_FAILURE, NULL);

	/*
	 * If leases are enabled, delay the election to allow any straggler
	 * messages to get processed that might grant our lease again and
	 * fool the base code into thinking the master is still there.
	 * Any delay here offsets the time election code will wait for a
	 * lease grant to expire.  So with leases we're not adding more delay.
	 */
	if (FLD_ISSET(db_rep->region->config, REP_C_LEASE)) {
		/*
		 * Use the smallest of the lease timeout, ack timeout,
		 * or connection retry timeout.  We want to give straggler
		 * messages a chance to get processed, but get an election
		 * underway as soon as possible to find a master.
		 */
		if ((ret = __rep_get_timeout(env->dbenv,
		    DB_REP_LEASE_TIMEOUT, &delay_time)) != 0)
			goto out;
		if ((ret = __rep_get_timeout(env->dbenv,
		    DB_REP_ACK_TIMEOUT, &tmp_time)) != 0)
			goto out;
		if (tmp_time < delay_time)
			delay_time = tmp_time;
		if ((ret = __rep_get_timeout(env->dbenv,
		    DB_REP_CONNECTION_RETRY, &tmp_time)) != 0)
			goto out;
		if (tmp_time < delay_time)
			delay_time = tmp_time;
		sec = delay_time / US_PER_SEC;
		usec = delay_time % US_PER_SEC;
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "Election with leases pause sec %lu, usec %lu", sec, usec));
		__os_yield(env, sec, usec);
	}

	/*
	 * As a freshly started thread, lay claim to the title of being
	 * "preferred".  If an older thread is sleeping for retry, when it wakes
	 * up it will relinquish its role (since there's no need for multiple
	 * threads to sleep and retry).
	 */
	LOCK_MUTEX(db_rep->mutex);
	db_rep->preferred_elect_thr = th;
	UNLOCK_MUTEX(db_rep->mutex);

	/*
	 * The 'done_repstart' flag keeps track of which was our most recent
	 * operation (repstart or election), so that we can alternate
	 * appropriately.  There are a few different ways this thread can be
	 * invoked, and all but one specify some form of immediate election be
	 * called.  The one exception is at initial start-up, where we
	 * first probe for a master by sending out rep_start(CLIENT) calls.
	 */
	if (LF_ISSET(ELECT_F_IMMED)) {
		/*
		 * When the election succeeds, we've successfully completed
		 * everything we need to do.  If it fails in an unexpected way,
		 * we abort all processing as usual.  The only time we need to
		 * stay in here and do some more work is on DB_REP_UNAVAIL,
		 * in which case we want to wait a while and retry later.
		 */
		if ((ret = __repmgr_elect(env, flags, &failtime)) ==
		    DB_REP_UNAVAIL)
			done_repstart = FALSE;
		else
			goto out;
	} else {
		/*
		 * We didn't really have an election failure, because in this
		 * case we haven't even done an election yet.  But the timing
		 * we want turns out the same: we want to wait for the election
		 * retry time and then call for an election if nothing else
		 * interesting happens before then.
		 */
		__os_gettime(env, &failtime, 1);

		/*
		 * Although we didn't do a repstart in this thread, we know that
		 * our caller did one just before creating the thread.
		 */
		done_repstart = TRUE;
	}

	LOCK_MUTEX(db_rep->mutex);
	for (;;) {
		ret = 0;

		if (db_rep->repmgr_status == stopped)
			goto unlock;

		/*
		 * If we've become the master (which could happen after an
		 * election in another election thread), or we find we have a
		 * working connection to a known master, then we're quite
		 * content: that's really the essential purpose of this whole
		 * thread.
		 */
		if (__repmgr_master_is_known(env))
			goto unlock;

		/*
		 * When circumstances force us to do an immediate election, we
		 * may be forced to create multiple threads in order to do so.
		 * But we certainly don't need multiple threads sleeping,
		 * alternating and retrying.  The "preferred election thread" is
		 * the one that has the authority and responsibility to
		 * persevere until our work is done.  Note that this role can
		 * switch from one thread to another, depending on the timing of
		 * events.  In particular, when an election fails the thread
		 * that got the failure becomes the chosen one that will remain
		 * to avenge the failure.
		 */
		if (db_rep->preferred_elect_thr != th)
			goto unlock;

		timespecclear(&wait_til);
		__os_gettime(env, &now, 1);

		/*
		 * See if it's time to retry the operation.  Normally it's an
		 * election we're interested in retrying.  But we refrain from
		 * calling for elections if so configured.
		 */
		suppress_election = LF_ISSET(ELECT_F_STARTUP) ?
		    db_rep->init_policy == DB_REP_CLIENT :
		    !FLD_ISSET(rep->config, REP_C_ELECTIONS);
		repstart_time = db_rep->repstart_time;
		target = suppress_election ? repstart_time : failtime;
		TIMESPEC_ADD_DB_TIMEOUT(&target, rep->election_retry_wait);
		if (timespeccmp(&now, &target, >=)) {
			/*
			 * We've surpassed our target retry time.
			 * However, elections should generally alternate with
			 * rep_start calls, so do that if we haven't done one
			 * since the last election.
			 */
			action = suppress_election ? REPSTART :
			    (done_repstart ? ELECTION : REPSTART);

		} else if (db_rep->new_connection) {
			/* Seen a recent new connection, let's do rep_start. */
			action = REPSTART;
		} else
			wait_til = target;

		if (!timespecisset(&wait_til)) {
			response_time = __repmgr_compute_response_time(env);
			target = repstart_time;
			TIMESPEC_ADD_DB_TIMEOUT(&target, response_time);
			if (timespeccmp(&now, &target, <)) {
				/* We haven't waited long enough. */
				wait_til = target;
			}
		}

		if (timespecisset(&wait_til)) {
#ifdef DB_WIN32
			timespecsub(&wait_til, &now);
			DB_TIMESPEC_TO_TIMEOUT(t, &wait_til, TRUE);
			duration = t / US_PER_MS;
			if ((ret = SignalObjectAndWait(*db_rep->mutex,
			    db_rep->check_election, duration, FALSE)) !=
			    WAIT_OBJECT_0 && ret != WAIT_TIMEOUT)
				goto out;

			LOCK_MUTEX(db_rep->mutex);

			/*
			 * Although there could be multiple threads, only the
			 * "preferred" thread resets the event object.  If the
			 * others tried to do so, the preferred thread might
			 * miss the wake-up.  Another way of saying this is that
			 * the precise meaning of the check_election event is
			 * that "there may be some election-thread-related work
			 * to do, and the correct thread to do it has not yet
			 * been woken up".
			 */
			if (ret == WAIT_OBJECT_0 &&
			    db_rep->preferred_elect_thr == th &&
			    !ResetEvent(db_rep->check_election)) {
				ret = GetLastError();
				goto unlock;
			}
#else
			deadline.tv_sec = wait_til.tv_sec;
			deadline.tv_nsec = wait_til.tv_nsec;
			if ((ret = pthread_cond_timedwait(
			    &db_rep->check_election, db_rep->mutex, &deadline))
			    != ETIMEDOUT && ret != 0)
				goto unlock;
#endif
			continue;
		}

		UNLOCK_MUTEX(db_rep->mutex);
		if (action == ELECTION) {
			db_rep->new_connection = FALSE;
			if ((ret = __repmgr_elect(env, 0, &failtime)) ==
			    DB_REP_UNAVAIL)
				done_repstart = FALSE;
			else
				goto out;
			LOCK_MUTEX(db_rep->mutex);
			db_rep->preferred_elect_thr = th;
		} else {
			DB_ASSERT(env, action == REPSTART);

			db_rep->new_connection = FALSE;
			if ((ret = __repmgr_repstart(env, DB_REP_CLIENT)) != 0)
				goto out;
			done_repstart = TRUE;

			LOCK_MUTEX(db_rep->mutex);
			__os_gettime(env, &db_rep->repstart_time, 1);
		}
	}

#ifdef HAVE_STATISTICS
	/*
	 * We normally don't bother taking a mutex to increment statistics.  But
	 * in this case, since we're incrementing and decrementing in pairs, it
	 * could be very weird if we were "off somewhat".  For example, we could
	 * get a negative value.  And this is not a high-traffic, performance-
	 * critical path.
	 *     On the other hand, it suffices to take repmgr's (handle-based)
	 * mutex, rather than the rep mutex which normally protects shared
	 * memory, since all election thread activity must be occurring in the
	 * single listener process, under control of one single rep handle.
	 */
out:
	LOCK_MUTEX(db_rep->mutex);
unlock:
	rep->mstat.st_elect_threads--;
	UNLOCK_MUTEX(db_rep->mutex);
#else
unlock:
	UNLOCK_MUTEX(db_rep->mutex);
out:
#endif
	return (ret);
}

static db_timeout_t
__repmgr_compute_response_time(env)
	ENV *env;
{
	DB_REP *db_rep;
	REP *rep;
	db_timeout_t ato, eto;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	/*
	 * Avoid crowding operations too close together.  If we've just recently
	 * done a rep_start, wait a moment in case there's a master out there,
	 * to give it a chance to respond with a NEWMASTER message.  This is
	 * particularly an issue at start-up time, when we're likely to have
	 * several "new connection establishment" events bombarding us with lots
	 * of rep_start requests in quick succession.
	 *
	 * We don't have a separate user configuration for rep_start response,
	 * but it's reasonable to expect it to be similar to either the ack
	 * timeout or the election timeout, whichever is smaller.  However, only
	 * consider the ack timeout if all signs point to it being in use.
	 */
	ato = rep->ack_timeout;
	eto = rep->elect_timeout;
	if (ato > 0 &&
	    rep->perm_policy != DB_REPMGR_ACKS_NONE &&
	    rep->priority > 0 &&
	    ato < eto)
		return (ato);

	return (eto);
}

static int
__repmgr_elect(env, flags, failtimep)
	ENV *env;
	u_int32_t flags;
	db_timespec *failtimep;
{
	DB_REP *db_rep;
	REP *rep;
	u_int32_t invitation, nsites, nvotes;
	int ret, t_ret;

	db_rep = env->rep_handle;
	nsites = db_rep->region->config_nsites;
	DB_ASSERT(env, nsites > 0);

	/*
	 * With only 2 sites in the group, even a single failure could make it
	 * impossible to get a majority.  So, fudge a little, unless the user
	 * really wants strict safety.
	 */
	if (nsites == 2 &&
	    !FLD_ISSET(db_rep->region->config, REP_C_2SITE_STRICT))
		nvotes = 1;
	else
		nvotes = ELECTION_MAJORITY(nsites);

	if (LF_ISSET(ELECT_F_INVITEE)) {
		/*
		 * We're going to the election party because we were invited by
		 * another site.  Accept the other site's suggested value, if
		 * it's reasonable.  (I.e., the other site may have wanted to do
		 * a "fast" election after losing contact with the master.  If
		 * so, let's not spoil it by imposing our own full nsites count
		 * on it.)
		 */
		rep = db_rep->region;
		invitation = rep->nsites;
		if (invitation == nsites || invitation == nsites - 1) {
			nsites = invitation;
		}
	}
	if (LF_ISSET(ELECT_F_FAST) && nsites > nvotes) {
		/*
		 * If we're doing an election because we noticed that the master
		 * failed, it's reasonable to expect that the master won't
		 * participate.  By not waiting for its vote, we can probably
		 * complete the election faster.  But note that we shouldn't
		 * allow this to affect nvotes calculation.
		 *
		 * However, if we have 2 sites, and strict majority is turned
		 * on, now nvotes would be 2, and it doesn't make sense to
		 * rep_elect to see nsites of 1 in that case.  So only decrement
		 * nsites if it currently exceeds nvotes.
		 */
		nsites--;
	}
	/* The rule for leases overrides all of the above. */
	if (IS_USING_LEASES(env))
		nsites = 0;

	switch (ret = __rep_elect_int(env, nsites, nvotes, 0)) {
	case DB_REP_UNAVAIL:
		__os_gettime(env, failtimep, 1);
		DB_EVENT(env, DB_EVENT_REP_ELECTION_FAILED, NULL);
		if ((t_ret = send_membership(env)) != 0)
			ret = t_ret;
		break;

	case 0:
		if (db_rep->takeover_pending)
			ret = __repmgr_claim_victory(env);
		break;

	case DB_REP_IGNORE:
		ret = 0;
		break;

	default:
		__db_err(env, ret, DB_STR("3629",
		    "unexpected election failure"));
		break;
	}
	return (ret);
}

/*
 * If an election fails with DB_REP_UNAVAIL, it could be because a participating
 * site has an obsolete, too-high notion of the group size.  (This could happen
 * if the site was down/disconnected during removal of some (other) sites.)  To
 * remedy this, broadcast a current copy of the membership list.  Since all
 * sites are doing this, and we always ratchet to the most up-to-date version,
 * this should bring all sites up to date.  We only do this after a failure,
 * during what will normally be an idle period anyway, so that we don't slow
 * down a first election following the loss of an active master.
 */
static int
send_membership(env)
	ENV *env;
{
	DB_REP *db_rep;
	u_int8_t *buf;
	size_t len;
	int ret;

	db_rep = env->rep_handle;
	buf = NULL;
	LOCK_MUTEX(db_rep->mutex);
	if ((ret = __repmgr_marshal_member_list(env, &buf, &len)) != 0)
		goto out;
	RPRINT(env, (env, DB_VERB_REPMGR_MISC,
	    "Broadcast latest membership list"));
	ret = __repmgr_bcast_own_msg(env, REPMGR_SHARING, buf, len);
out:
	UNLOCK_MUTEX(db_rep->mutex);
	if (buf != NULL)
		__os_free(env, buf);
	return (ret);
}

/*
 * Becomes master after we've won an election, if we can.
 *
 * PUBLIC: int __repmgr_claim_victory __P((ENV *));
 */
int
__repmgr_claim_victory(env)
	ENV *env;
{
	int ret;

	env->rep_handle->takeover_pending = FALSE;
	if ((ret = __repmgr_become_master(env)) == DB_REP_UNAVAIL) {
		ret = 0;
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "Won election but lost race with DUPMASTER client intent"));
	}
	return (ret);
}

/*
 * When turning on elections in an already-running system, check to see if we're
 * in a state where we need an election (i.e., we would have started one
 * previously if elections hadn't been turned off), and if so start one.
 *
 * PUBLIC: int __repmgr_turn_on_elections __P((ENV *));
 */
int
__repmgr_turn_on_elections(env)
	ENV *env;
{
	DB_REP *db_rep;
	REP *rep;
	int ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	ret = 0;

	DB_ASSERT(env, REP_ON(env));
	LOCK_MUTEX(db_rep->mutex);
	if (db_rep->selector == NULL ||
	    !FLD_ISSET(rep->config, REP_C_ELECTIONS) ||
	    __repmgr_master_is_known(env))
		goto out;

	ret = __repmgr_init_election(env, ELECT_F_IMMED);

out:
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}
