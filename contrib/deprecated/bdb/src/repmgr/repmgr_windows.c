/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2005, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"

/* Convert time-out from microseconds to milliseconds, rounding up. */
#define	DB_TIMEOUT_TO_WINDOWS_TIMEOUT(t) (((t) + (US_PER_MS - 1)) / US_PER_MS)

typedef struct __cond_waiter {
	HANDLE event;
	PREDICATE pred;
	void *ctx;
	int next_free;
} COND_WAITER;

#define	WAITER_SLOT_IN_USE(w) ((w)->pred != NULL)

/*
 * Array slots [0:next_avail-1] are initialized, and either in use or on the
 * free list.  Slots beyond that are virgin territory, whose memory contents
 * could be garbage.  In particular, note that slots [0:next_avail-1] have a
 * Win32 Event Object created for them, which have to be freed when cleaning up
 * this data structure.
 *
 * "first_free" points to a list of not-in-use slots threaded through the first
 * section of the array.
 */
struct __cond_waiters_table {
	struct __cond_waiter *array;
	int size;
	int next_avail;
	int first_free;
};

/*
 * Aggregated control info needed for preparing for WSAWaitForMultipleEvents()
 * call.
 */
struct io_info {
	REPMGR_CONNECTION **connections;
	WSAEVENT *events;
	DWORD nevents;
};

static int allocate_wait_slot __P((ENV *, int *, COND_WAITERS_TABLE *));
static void free_wait_slot __P((ENV *, int, COND_WAITERS_TABLE *));
static int handle_completion __P((ENV *, REPMGR_CONNECTION *));
static int prepare_io __P((ENV *, REPMGR_CONNECTION *, void *));

int
__repmgr_thread_start(env, runnable)
	ENV *env;
	REPMGR_RUNNABLE *runnable;
{
	HANDLE event, thread_id;

	runnable->finished = FALSE;
	runnable->quit_requested = FALSE;
	runnable->env = env;

	if ((event = CreateEvent(NULL, TRUE, FALSE, NULL)) == NULL)
		return (GetLastError());
	thread_id = CreateThread(NULL, 0,
	    (LPTHREAD_START_ROUTINE)runnable->run, runnable, 0, NULL);
	if (thread_id == NULL) {
		CloseHandle(event);
		return (GetLastError());
	}
	runnable->thread_id = thread_id;
	runnable->quit_event = event;
	return (0);
}

int
__repmgr_thread_join(thread)
	REPMGR_RUNNABLE *thread;
{
	int ret;

	ret = 0;
	if (WaitForSingleObject(thread->thread_id, INFINITE) != WAIT_OBJECT_0)
		ret = GetLastError();
	if (!CloseHandle(thread->thread_id) && ret == 0)
		ret = GetLastError();
	if (!CloseHandle(thread->quit_event) && ret == 0)
		ret = GetLastError();

	return (ret);
}

int
__repmgr_set_nonblocking(s)
	SOCKET s;
{
	int ret;
	u_long onoff;

	onoff = 1;		/* any non-zero value */
	if ((ret = ioctlsocket(s, FIONBIO, &onoff)) == SOCKET_ERROR)
		return (WSAGetLastError());
	return (0);
}

int
__repmgr_set_nonblock_conn(conn)
	REPMGR_CONNECTION *conn;
{
	int ret;

	if ((ret = __repmgr_set_nonblocking(conn->fd)) != 0)
		return (ret);

	if ((conn->event_object = WSACreateEvent()) == WSA_INVALID_EVENT) {
		ret = net_errno;
		return (ret);
	}
	return (0);
}

/*
 * !!!
 * Caller must hold the repmgr->mutex, if this thread synchronization is to work
 * properly.
 */
int
__repmgr_wake_waiters(env, w)
	ENV *env;
	waiter_t *w;
{
	DB_REP *db_rep;
	COND_WAITERS_TABLE *waiters;
	COND_WAITER *slot;
	int i, ret;

	ret = 0;
	db_rep = env->rep_handle;
	waiters = *w;
	for (i = 0; i < waiters->next_avail; i++) {
		slot = &waiters->array[i];
		if (!WAITER_SLOT_IN_USE(slot))
			continue;
		if ((*slot->pred)(env, slot->ctx) ||
		    db_rep->repmgr_status == stopped)
			if (!SetEvent(slot->event) && ret == 0)
				ret = GetLastError();
	}
	return (ret);
}

/*
 * !!!
 * Caller must hold mutex.
 */
int
__repmgr_await_cond(env, pred, ctx, timeout, waiters_p)
	ENV *env;
	PREDICATE pred;
	void *ctx;
	db_timeout_t timeout;
	waiter_t *waiters_p;
{
	COND_WAITERS_TABLE *waiters;
	COND_WAITER *waiter;
	DB_REP *db_rep;
	REP *rep;
	DWORD ret, win_timeout;
	int i;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	waiters = *waiters_p;

	if ((ret = allocate_wait_slot(env, &i, waiters)) != 0)
		goto err;
	waiter = &waiters->array[i];

	win_timeout = timeout > 0 ?
	    DB_TIMEOUT_TO_WINDOWS_TIMEOUT(timeout) : INFINITE;
	waiter->pred = pred;
	waiter->ctx = ctx;
	if ((ret = SignalObjectAndWait(*db_rep->mutex,
	    waiter->event, win_timeout, FALSE)) == WAIT_FAILED) {
		ret = GetLastError();
	} else if (ret == WAIT_TIMEOUT)
		ret = DB_TIMEOUT;
	else
		DB_ASSERT(env, ret == WAIT_OBJECT_0);

	LOCK_MUTEX(db_rep->mutex);
	free_wait_slot(env, i, waiters);
	if (db_rep->repmgr_status == stopped)
		ret = DB_REP_UNAVAIL;

err:
	return (ret);
}

/*
 * !!!
 * Caller must hold the mutex.
 */
static int
allocate_wait_slot(env, resultp, table)
	ENV *env;
	int *resultp;
	COND_WAITERS_TABLE *table;
{
	COND_WAITER *w;
	HANDLE event;
	int i, ret;

	if (table->first_free == -1) {
		if (table->next_avail >= table->size) {
			/*
			 * Grow the array.
			 */
			table->size *= 2;
			w = table->array;
			if ((ret = __os_realloc(env, table->size * sizeof(*w),
			     &w)) != 0)
				return (ret);
			table->array = w;
		}
		if ((event = CreateEvent(NULL,
		    FALSE, FALSE, NULL)) == NULL) {
			/* No need to rescind the memory reallocation. */
			return (GetLastError());
		}

		/*
		 * Here if, one way or another, we're good to go for using the
		 * next slot (for the first time).
		 */
		i = table->next_avail++;
		w = &table->array[i];
		w->event = event;
	} else {
		i = table->first_free;
		w = &table->array[i];
		table->first_free = w->next_free;
	}
	/*
	 * Make sure this event state is nonsignaled. It is possible that
	 * late processing could have signaled this event after the end of
	 * the previous wait but before reacquiring the mutex, and this
	 * extra signal would incorrectly cause the next wait to return
	 * immediately.
	 */ 
	(void)WaitForSingleObject(w->event, 0);
	*resultp = i;
	return (0);
}

static void
free_wait_slot(env, slot_index, table)
	ENV *env;
	int slot_index;
	COND_WAITERS_TABLE *table;
{
	DB_REP *db_rep;
	COND_WAITER *slot;

	db_rep = env->rep_handle;
	slot = &table->array[slot_index];

	slot->pred = NULL;	/* show it's not in use */
	slot->next_free = table->first_free;
	table->first_free = slot_index;
}

int
__repmgr_await_gmdbop(env)
	ENV *env;
{
	DB_REP *db_rep;
	int ret;

	db_rep = env->rep_handle;
	while (db_rep->gmdb_busy) {
		if (!ResetEvent(db_rep->gmdb_idle))
			return (GetLastError());
		ret = SignalObjectAndWait(*db_rep->mutex,
		    db_rep->gmdb_idle, INFINITE, FALSE);
		LOCK_MUTEX(db_rep->mutex);
		if (ret == WAIT_FAILED)
			return (GetLastError());
		DB_ASSERT(env, ret == WAIT_OBJECT_0);
	}
	return (0);
}

/* (See requirements described in repmgr_posix.c.) */
int
__repmgr_await_drain(env, conn, timeout)
	ENV *env;
	REPMGR_CONNECTION *conn;
	db_timeout_t timeout;
{
	DB_REP *db_rep;
	db_timespec deadline, delta, now;
	db_timeout_t t;
	DWORD duration, ret;
	int round_up;

	db_rep = env->rep_handle;

	__os_gettime(env, &deadline, 1);
	TIMESPEC_ADD_DB_TIMEOUT(&deadline, timeout);

	while (conn->out_queue_length >= OUT_QUEUE_LIMIT) {
		if (!ResetEvent(conn->drained))
			return (GetLastError());

		/* How long until the deadline? */
		__os_gettime(env, &now, 1);
		if (timespeccmp(&now, &deadline, >=)) {
			conn->state = CONN_CONGESTED;
			return (0);
		}
		delta = deadline;
		timespecsub(&delta, &now);
		round_up = TRUE;
		DB_TIMESPEC_TO_TIMEOUT(t, &delta, round_up);
		duration = DB_TIMEOUT_TO_WINDOWS_TIMEOUT(t);

		ret = SignalObjectAndWait(*db_rep->mutex,
		    conn->drained, duration, FALSE);
		LOCK_MUTEX(db_rep->mutex);
		if (ret == WAIT_FAILED)
			return (GetLastError());
		else if (ret == WAIT_TIMEOUT) {
			conn->state = CONN_CONGESTED;
			return (0);
		} else
			DB_ASSERT(env, ret == WAIT_OBJECT_0);

		if (db_rep->repmgr_status == stopped)
			return (0);
		if (conn->state == CONN_DEFUNCT)
			return (DB_REP_UNAVAIL);
	}
	return (0);
}

/*
 * Creates a manual reset event, which is usually our best choice when we may
 * have multiple threads waiting on a single event.
 */
int
__repmgr_alloc_cond(c)
	cond_var_t *c;
{
	HANDLE event;

	if ((event = CreateEvent(NULL, TRUE, FALSE, NULL)) == NULL)
		return (GetLastError());
	*c = event;
	return (0);
}

int
__repmgr_free_cond(c)
	cond_var_t *c;
{
	if (CloseHandle(*c))
		return (0);
	return (GetLastError());
}

void
__repmgr_env_create_pf(db_rep)
	DB_REP *db_rep;
{
}

int
__repmgr_create_mutex_pf(mutex)
	mgr_mutex_t *mutex;
{
	if ((*mutex = CreateMutex(NULL, FALSE, NULL)) == NULL)
		return (GetLastError());
	return (0);
}

int
__repmgr_destroy_mutex_pf(mutex)
	mgr_mutex_t  *mutex;
{
	return (CloseHandle(*mutex) ? 0 : GetLastError());
}

int
__repmgr_init(env)
     ENV *env;
{
	DB_REP *db_rep;
	WSADATA wsaData;
	int ret;

	db_rep = env->rep_handle;

	if ((ret = WSAStartup(MAKEWORD(2, 2), &wsaData)) != 0) {
		__db_err(env, ret, DB_STR("3589",
		    "unable to initialize Windows networking"));
		return (ret);
	}

	if ((db_rep->signaler = CreateEvent(NULL, /* security attr */
	    FALSE,	/* (not) of the manual reset variety  */
	    FALSE,		/* (not) initially signaled */
	    NULL)) == NULL)		/* name */
		goto geterr;

	if ((db_rep->msg_avail = CreateEvent(NULL, TRUE, FALSE, NULL))
	    == NULL)
		goto geterr;

	if ((db_rep->check_election = CreateEvent(NULL, TRUE, FALSE, NULL))
	    == NULL)
		goto geterr;

	if ((db_rep->gmdb_idle = CreateEvent(NULL, TRUE, FALSE, NULL))
	    == NULL)
		goto geterr;

	if ((ret = __repmgr_init_waiters(env, &db_rep->ack_waiters)) != 0)
		goto err;
	return (0);

geterr:
	ret = GetLastError();
err:
	if (db_rep->gmdb_idle != NULL)
		CloseHandle(db_rep->gmdb_idle);
	if (db_rep->check_election != NULL)
		CloseHandle(db_rep->check_election);
	if (db_rep->msg_avail != NULL)
		CloseHandle(db_rep->msg_avail);
	if (db_rep->signaler != NULL)
		CloseHandle(db_rep->signaler);
	db_rep->msg_avail =
	    db_rep->check_election =
	    db_rep->gmdb_idle =
	    db_rep->signaler = NULL;
	(void)WSACleanup();
	return (ret);
}

int
__repmgr_deinit(env)
     ENV *env;
{
	DB_REP *db_rep;
	int ret, t_ret;

	db_rep = env->rep_handle;
	if (!(REPMGR_INITED(db_rep)))
		return (0);

	ret = 0;
	if (WSACleanup() == SOCKET_ERROR)
		ret = WSAGetLastError();

	if ((t_ret = __repmgr_destroy_waiters(env, &db_rep->ack_waiters))
	    != 0 && ret == 0)
		ret = t_ret;

	if (!CloseHandle(db_rep->gmdb_idle) && ret == 0)
		ret = GetLastError();

	if (!CloseHandle(db_rep->check_election) && ret == 0)
		ret = GetLastError();

	if (!CloseHandle(db_rep->msg_avail) && ret == 0)
		ret = GetLastError();

	if (!CloseHandle(db_rep->signaler) && ret == 0)
		ret = GetLastError();
	db_rep->msg_avail =
	    db_rep->check_election =
	    db_rep->gmdb_idle =
	    db_rep->signaler = NULL;

	return (ret);
}

int
__repmgr_init_waiters(env, waiters)
	ENV *env;
	waiter_t *waiters;
{
#define	INITIAL_ALLOCATION 5		/* arbitrary size */
	COND_WAITERS_TABLE *table;
	int ret;

	table = NULL;

	if ((ret =
	    __os_calloc(env, 1, sizeof(COND_WAITERS_TABLE), &table)) != 0)
		return (ret);

	if ((ret = __os_calloc(env, INITIAL_ALLOCATION, sizeof(COND_WAITER),
	    &table->array)) != 0) {
		__os_free(env, table);
		return (ret);
	}

	table->size = INITIAL_ALLOCATION;
	table->first_free = -1;
	table->next_avail = 0;

	/* There's a restaurant joke in there somewhere. */
	*waiters = table;
	return (0);
}

int
__repmgr_destroy_waiters(env, waitersp)
	ENV *env;
	waiter_t *waitersp;
{
	waiter_t waiters;
	int i, ret;

	waiters = *waitersp;
	ret = 0;
	for (i = 0; i < waiters->next_avail; i++) {
		if (!CloseHandle(waiters->array[i].event) && ret == 0)
			ret = GetLastError();
	}
	__os_free(env, waiters->array);
	__os_free(env, waiters);
	return (ret);
}

int
__repmgr_lock_mutex(mutex)
	mgr_mutex_t  *mutex;
{
	if (WaitForSingleObject(*mutex, INFINITE) == WAIT_OBJECT_0)
		return (0);
	return (GetLastError());
}

int
__repmgr_unlock_mutex(mutex)
	mgr_mutex_t  *mutex;
{
	if (ReleaseMutex(*mutex))
		return (0);
	return (GetLastError());
}

int
__repmgr_signal(v)
	cond_var_t *v;
{
	return (SetEvent(*v) ? 0 : GetLastError());
}

int
__repmgr_wake_msngers(env, n)
	ENV *env;
	u_int n;
{
	DB_REP *db_rep;
	u_int i;

	db_rep = env->rep_handle;

	/* Ask all threads beyond index 'n' to shut down. */
	for (i = n; i< db_rep->nthreads; i++)
		if (!SetEvent(db_rep->messengers[i]->quit_event))
			return (GetLastError());
	return (0);
}

int
__repmgr_wake_main_thread(env)
	ENV *env;
{
	if (!SetEvent(env->rep_handle->signaler))
		return (GetLastError());
	return (0);
}

int
__repmgr_writev(fd, iovec, buf_count, byte_count_p)
	socket_t fd;
	db_iovec_t *iovec;
	int buf_count;
	size_t *byte_count_p;
{
	DWORD bytes;

	if (WSASend(fd, iovec,
	    (DWORD)buf_count, &bytes, 0, NULL, NULL) == SOCKET_ERROR)
		return (net_errno);

	*byte_count_p = (size_t)bytes;
	return (0);
}

int
__repmgr_readv(fd, iovec, buf_count, xfr_count_p)
	socket_t fd;
	db_iovec_t *iovec;
	int buf_count;
	size_t *xfr_count_p;
{
	DWORD bytes, flags;

	flags = 0;
	if (WSARecv(fd, iovec,
	    (DWORD)buf_count, &bytes, &flags, NULL, NULL) == SOCKET_ERROR)
		return (net_errno);

	*xfr_count_p = (size_t)bytes;
	return (0);
}

int
__repmgr_select_loop(env)
	ENV *env;
{
	DB_REP *db_rep;
	DWORD ret;
	DWORD select_timeout;
	REPMGR_CONNECTION *connections[WSA_MAXIMUM_WAIT_EVENTS];
	WSAEVENT events[WSA_MAXIMUM_WAIT_EVENTS];
	db_timespec timeout;
	WSAEVENT listen_event;
	WSANETWORKEVENTS net_events;
	struct io_info io_info;
	int i;

	db_rep = env->rep_handle;
	io_info.connections = connections;
	io_info.events = events;

	if ((listen_event = WSACreateEvent()) == WSA_INVALID_EVENT) {
		__db_err(env, net_errno, DB_STR("3590",
		    "can't create event for listen socket"));
		return (net_errno);
	}
	if (!IS_SUBORDINATE(db_rep) &&
	    WSAEventSelect(db_rep->listen_fd, listen_event, FD_ACCEPT) ==
	    SOCKET_ERROR) {
		ret = net_errno;
		__db_err(env, ret, DB_STR("3591",
		    "can't enable event for listener"));
		(void)WSACloseEvent(listen_event);
		goto out;
	}

	LOCK_MUTEX(db_rep->mutex);
	if ((ret = __repmgr_first_try_connections(env)) != 0)
		goto unlock;
	for (;;) {
		/* Start with the two events that we always wait for. */
#define	SIGNALER_INDEX	0
#define	LISTENER_INDEX	1
		events[SIGNALER_INDEX] = db_rep->signaler;
		if (IS_SUBORDINATE(db_rep))
			io_info.nevents = 1;
		else {
			events[LISTENER_INDEX] = listen_event;
			io_info.nevents = 2;
		}

		if ((ret = __repmgr_each_connection(env,
		    prepare_io, &io_info, TRUE)) != 0)
			goto unlock;

		if (__repmgr_compute_timeout(env, &timeout))
			select_timeout =
			    (DWORD)(timeout.tv_sec * MS_PER_SEC +
			    timeout.tv_nsec / NS_PER_MS);
		else {
			/* No time-based events to wake us up. */
			select_timeout = WSA_INFINITE;
		}

		UNLOCK_MUTEX(db_rep->mutex);
		ret = WSAWaitForMultipleEvents(
		    io_info.nevents, events, FALSE, select_timeout, FALSE);
		if (db_rep->repmgr_status == stopped) {
			ret = 0;
			goto out;
		}
		LOCK_MUTEX(db_rep->mutex);

		/*
		 * !!!
		 * Note that `ret' remains set as the return code from
		 * WSAWaitForMultipleEvents, above.
		 */
		if (ret >= WSA_WAIT_EVENT_0 &&
		    ret < WSA_WAIT_EVENT_0 + io_info.nevents) {
			if ((i = ret - WSA_WAIT_EVENT_0) == SIGNALER_INDEX) {
				/* Another thread woke us. */
			} else if (!IS_SUBORDINATE(db_rep) &&
			    i == LISTENER_INDEX) {
				if ((ret = WSAEnumNetworkEvents(
				    db_rep->listen_fd, listen_event,
				    &net_events)) == SOCKET_ERROR) {
					ret = net_errno;
					goto unlock;
				}
				DB_ASSERT(env,
				    net_events.lNetworkEvents & FD_ACCEPT);
				if ((ret = net_events.iErrorCode[FD_ACCEPT_BIT])
				    != 0)
					goto unlock;
				if ((ret = __repmgr_accept(env)) != 0)
					goto unlock;
			} else {
				if (connections[i]->state != CONN_DEFUNCT &&
				    (ret = handle_completion(env,
				    connections[i])) != 0)
					goto unlock;
			}
		} else if (ret == WSA_WAIT_TIMEOUT) {
			if ((ret = __repmgr_check_timeouts(env)) != 0)
				goto unlock;
		} else if (ret == WSA_WAIT_FAILED) {
			ret = net_errno;
			goto unlock;
		}
	}

unlock:
	UNLOCK_MUTEX(db_rep->mutex);
out:
	if (!CloseHandle(listen_event) && ret == 0)
		ret = GetLastError();
	if (ret == DB_DELETED)
		ret = __repmgr_bow_out(env);
	LOCK_MUTEX(db_rep->mutex);
	(void)__repmgr_net_close(env);
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

static int
prepare_io(env, conn, info_)
	ENV *env;
	REPMGR_CONNECTION *conn;
	void *info_;
{
	struct io_info *info;
	long desired_events;
	int ret;

	if (conn->state == CONN_DEFUNCT)
		return (__repmgr_cleanup_defunct(env, conn));

	/*
	 * Note that even if we're suffering flow control, we
	 * nevertheless still read if we haven't even yet gotten
	 * a handshake.  Why?  (1) Handshakes are important; and
	 * (2) they don't hurt anything flow-control-wise.
	 */
	info = info_;

	/*
	 * If we ever implemented flow control, we would have some conditions to
	 * examine here.  But as it is, we always are willing to accept I/O on
	 * every connection.
	 *
	 * We can only handle as many connections as the number of events the
	 * WSAWaitForMultipleEvents function allows (minus 2, for our overhead:
	 * the listener and the signaler).
	 */
	DB_ASSERT(env, info->nevents < WSA_MAXIMUM_WAIT_EVENTS);
	info->events[info->nevents] = conn->event_object;
	info->connections[info->nevents++] = conn;

	desired_events = FD_READ | FD_CLOSE;
	if (!STAILQ_EMPTY(&conn->outbound_queue))
		desired_events |= FD_WRITE;
	if (WSAEventSelect(conn->fd,
	    conn->event_object, desired_events) == SOCKET_ERROR) {
		ret = net_errno;
		__db_err(env, ret, DB_STR_A("3592",
		    "can't set event bits 0x%lx", "%lx"), desired_events);
	} else
		ret = 0;

	return (ret);
}

static int
handle_completion(env, conn)
	ENV *env;
	REPMGR_CONNECTION *conn;
{
	int error, ret;
	WSANETWORKEVENTS events;

	if ((ret = WSAEnumNetworkEvents(conn->fd, conn->event_object, &events))
	    == SOCKET_ERROR) {
		error = net_errno;
		__db_err(env, error, DB_STR("3593", "EnumNetworkEvents"));
		goto report;
	}

	/* Check both writing and reading. */
	if (events.lNetworkEvents & FD_CLOSE) {
		error = events.iErrorCode[FD_CLOSE_BIT];
		goto report;
	}

	if (events.lNetworkEvents & FD_WRITE) {
		if (events.iErrorCode[FD_WRITE_BIT] != 0) {
			error = events.iErrorCode[FD_WRITE_BIT];
			goto report;
		} else if ((ret =
			__repmgr_write_some(env, conn)) != 0)
			goto err;
	}

	if (events.lNetworkEvents & FD_READ) {
		if (events.iErrorCode[FD_READ_BIT] != 0) {
			error = events.iErrorCode[FD_READ_BIT];
			goto report;
		} else if ((ret =
			__repmgr_read_from_site(env, conn)) != 0)
			goto err;
	}

	if (0) {
report:
		__repmgr_fire_conn_err_event(env, conn, error);
		STAT(env->rep_handle->region->mstat.st_connection_drop++);
		ret = DB_REP_UNAVAIL;
	}
err:
	if (ret == DB_REP_UNAVAIL)
		ret = __repmgr_bust_connection(env, conn);
	return (ret);
}
