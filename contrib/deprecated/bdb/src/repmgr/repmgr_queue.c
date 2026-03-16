/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2006, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"

static REPMGR_MESSAGE *available_work __P((ENV *));

/*
 * Deallocates memory used by all messages on the queue.
 *
 * PUBLIC: int __repmgr_queue_destroy __P((ENV *));
 */
int
__repmgr_queue_destroy(env)
	ENV *env;
{
	DB_REP *db_rep;
	REPMGR_MESSAGE *m;
	REPMGR_CONNECTION *conn;
	int ret, t_ret;

	db_rep = env->rep_handle;

	ret = 0;
	while (!STAILQ_EMPTY(&db_rep->input_queue.header)) {
		m = STAILQ_FIRST(&db_rep->input_queue.header);
		STAILQ_REMOVE_HEAD(&db_rep->input_queue.header, entries);
		if (m->msg_hdr.type == REPMGR_APP_MESSAGE) {
			if ((conn = m->v.appmsg.conn) != NULL &&
			    (t_ret = __repmgr_decr_conn_ref(env, conn)) != 0 &&
			    ret == 0)
				ret = t_ret;
		}
		__os_free(env, m);
	}
	return (ret);
}

/*
 * PUBLIC: int __repmgr_queue_get __P((ENV *,
 * PUBLIC:     REPMGR_MESSAGE **, REPMGR_RUNNABLE *));
 *
 * Get the first input message from the queue and return it to the caller.  The
 * caller hereby takes responsibility for the entire message buffer, and should
 * free it when done.
 *
 * Caller must hold mutex.
 */
int
__repmgr_queue_get(env, msgp, th)
	ENV *env;
	REPMGR_MESSAGE **msgp;
	REPMGR_RUNNABLE *th;
{
	DB_REP *db_rep;
	REPMGR_MESSAGE *m;
#ifdef DB_WIN32
	HANDLE wait_events[2];
#endif
	int ret;

	ret = 0;
	db_rep = env->rep_handle;

	while ((m = available_work(env)) == NULL &&
	    db_rep->repmgr_status == running && !th->quit_requested) {
#ifdef DB_WIN32
		/*
		 * On Windows, msg_avail means either there's something in the
		 * queue, or we're all finished.  So, reset the event if that is
		 * not true.
		 */
		if (STAILQ_EMPTY(&db_rep->input_queue.header) &&
		    db_rep->repmgr_status == running &&
		    !ResetEvent(db_rep->msg_avail)) {
			ret = GetLastError();
			goto err;
		}
		wait_events[0] = db_rep->msg_avail;
		wait_events[1] = th->quit_event;
		UNLOCK_MUTEX(db_rep->mutex);
		ret = WaitForMultipleObjects(2, wait_events, FALSE, INFINITE);
		LOCK_MUTEX(db_rep->mutex);
		if (ret == WAIT_FAILED) {
			ret = GetLastError();
			goto err;
		}

#else
		if ((ret = pthread_cond_wait(&db_rep->msg_avail,
		    db_rep->mutex)) != 0)
			goto err;
#endif
	}
	if (db_rep->repmgr_status == stopped || th->quit_requested)
		ret = DB_REP_UNAVAIL;
	else {
		STAILQ_REMOVE(&db_rep->input_queue.header,
		    m, __repmgr_message, entries);
		db_rep->input_queue.size--;
		*msgp = m;
	}

err:
	return (ret);
}

/*
 * Gets an "available" item of work (i.e., a message) from the input queue.  If
 * there are plenty of message threads currently available, then we simply
 * return the first thing on the queue, regardless of what type of message it
 * is.  But otherwise skip over any message type that may possibly turn out to
 * be "long-running", so that we avoid starving out the important rep message
 * processing.
 */
static REPMGR_MESSAGE *
available_work(env)
	ENV *env;
{
	DB_REP *db_rep;
	REPMGR_MESSAGE *m;

	db_rep = env->rep_handle;
	if (STAILQ_EMPTY(&db_rep->input_queue.header))
		return (NULL);
	/*
	 * The "non_rep_th" field is the dynamically varying count of threads
	 * currently processing non-replication messages (a.k.a. possibly
	 * long-running messages, a.k.a. "deferrable").  We always ensure that
	 * db_rep->nthreads > reserved.
	 */
	if (db_rep->nthreads > db_rep->non_rep_th + RESERVED_MSG_TH(env))
		return (STAILQ_FIRST(&db_rep->input_queue.header));
	STAILQ_FOREACH(m, &db_rep->input_queue.header, entries) {
		if (!IS_DEFERRABLE(m->msg_hdr.type))
			return (m);
	}
	return (NULL);
}

/*
 * PUBLIC: int __repmgr_queue_put __P((ENV *, REPMGR_MESSAGE *));
 *
 * !!!
 * Caller must hold repmgr->mutex.
 */
int
__repmgr_queue_put(env, msg)
	ENV *env;
	REPMGR_MESSAGE *msg;
{
	DB_REP *db_rep;

	db_rep = env->rep_handle;

	STAILQ_INSERT_TAIL(&db_rep->input_queue.header, msg, entries);
	db_rep->input_queue.size++;

	return (__repmgr_signal(&db_rep->msg_avail));
}

/*
 * PUBLIC: int __repmgr_queue_size __P((ENV *));
 *
 * !!!
 * Caller must hold repmgr->mutex.
 */
int
__repmgr_queue_size(env)
	ENV *env;
{
	return (env->rep_handle->input_queue.size);
}
