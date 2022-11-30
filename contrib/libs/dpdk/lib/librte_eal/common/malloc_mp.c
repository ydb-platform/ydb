#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2018 Intel Corporation
 */

#include <string.h>
#include <sys/time.h>

#include <rte_alarm.h>
#include <rte_errno.h>
#include <rte_string_fns.h>

#include "eal_memalloc.h"
#include "eal_memcfg.h"

#include "malloc_elem.h"
#include "malloc_mp.h"

#define MP_ACTION_SYNC "mp_malloc_sync"
/**< request sent by primary process to notify of changes in memory map */
#define MP_ACTION_ROLLBACK "mp_malloc_rollback"
/**< request sent by primary process to notify of changes in memory map. this is
 * essentially a regular sync request, but we cannot send sync requests while
 * another one is in progress, and we might have to - therefore, we do this as
 * a separate callback.
 */
#define MP_ACTION_REQUEST "mp_malloc_request"
/**< request sent by secondary process to ask for allocation/deallocation */
#define MP_ACTION_RESPONSE "mp_malloc_response"
/**< response sent to secondary process to indicate result of request */

/* forward declarations */
static int
handle_sync_response(const struct rte_mp_msg *request,
		const struct rte_mp_reply *reply);
static int
handle_rollback_response(const struct rte_mp_msg *request,
		const struct rte_mp_reply *reply);

#define MP_TIMEOUT_S 5 /**< 5 seconds timeouts */

/* when we're allocating, we need to store some state to ensure that we can
 * roll back later
 */
struct primary_alloc_req_state {
	struct malloc_heap *heap;
	struct rte_memseg **ms;
	int ms_len;
	struct malloc_elem *elem;
	void *map_addr;
	size_t map_len;
};

enum req_state {
	REQ_STATE_INACTIVE = 0,
	REQ_STATE_ACTIVE,
	REQ_STATE_COMPLETE
};

struct mp_request {
	TAILQ_ENTRY(mp_request) next;
	struct malloc_mp_req user_req; /**< contents of request */
	pthread_cond_t cond; /**< variable we use to time out on this request */
	enum req_state state; /**< indicate status of this request */
	struct primary_alloc_req_state alloc_state;
};

/*
 * We could've used just a single request, but it may be possible for
 * secondaries to timeout earlier than the primary, and send a new request while
 * primary is still expecting replies to the old one. Therefore, each new
 * request will get assigned a new ID, which is how we will distinguish between
 * expected and unexpected messages.
 */
TAILQ_HEAD(mp_request_list, mp_request);
static struct {
	struct mp_request_list list;
	pthread_mutex_t lock;
} mp_request_list = {
	.list = TAILQ_HEAD_INITIALIZER(mp_request_list.list),
	.lock = PTHREAD_MUTEX_INITIALIZER
};

/**
 * General workflow is the following:
 *
 * Allocation:
 * S: send request to primary
 * P: attempt to allocate memory
 *    if failed, sendmsg failure
 *    if success, send sync request
 * S: if received msg of failure, quit
 *    if received sync request, synchronize memory map and reply with result
 * P: if received sync request result
 *    if success, sendmsg success
 *    if failure, roll back allocation and send a rollback request
 * S: if received msg of success, quit
 *    if received rollback request, synchronize memory map and reply with result
 * P: if received sync request result
 *    sendmsg sync request result
 * S: if received msg, quit
 *
 * Aside from timeouts, there are three points where we can quit:
 *  - if allocation failed straight away
 *  - if allocation and sync request succeeded
 *  - if allocation succeeded, sync request failed, allocation rolled back and
 *    rollback request received (irrespective of whether it succeeded or failed)
 *
 * Deallocation:
 * S: send request to primary
 * P: attempt to deallocate memory
 *    if failed, sendmsg failure
 *    if success, send sync request
 * S: if received msg of failure, quit
 *    if received sync request, synchronize memory map and reply with result
 * P: if received sync request result
 *    sendmsg sync request result
 * S: if received msg, quit
 *
 * There is no "rollback" from deallocation, as it's safe to have some memory
 * mapped in some processes - it's absent from the heap, so it won't get used.
 */

static struct mp_request *
find_request_by_id(uint64_t id)
{
	struct mp_request *req;
	TAILQ_FOREACH(req, &mp_request_list.list, next) {
		if (req->user_req.id == id)
			break;
	}
	return req;
}

/* this ID is, like, totally guaranteed to be absolutely unique. pinky swear. */
static uint64_t
get_unique_id(void)
{
	uint64_t id;
	do {
		id = rte_rand();
	} while (find_request_by_id(id) != NULL);
	return id;
}

/* secondary will respond to sync requests thusly */
static int
handle_sync(const struct rte_mp_msg *msg, const void *peer)
{
	struct rte_mp_msg reply;
	const struct malloc_mp_req *req =
			(const struct malloc_mp_req *)msg->param;
	struct malloc_mp_req *resp =
			(struct malloc_mp_req *)reply.param;
	int ret;

	if (req->t != REQ_TYPE_SYNC) {
		RTE_LOG(ERR, EAL, "Unexpected request from primary\n");
		return -1;
	}

	memset(&reply, 0, sizeof(reply));

	reply.num_fds = 0;
	strlcpy(reply.name, msg->name, sizeof(reply.name));
	reply.len_param = sizeof(*resp);

	ret = eal_memalloc_sync_with_primary();

	resp->t = REQ_TYPE_SYNC;
	resp->id = req->id;
	resp->result = ret == 0 ? REQ_RESULT_SUCCESS : REQ_RESULT_FAIL;

	rte_mp_reply(&reply, peer);

	return 0;
}

static int
handle_alloc_request(const struct malloc_mp_req *m,
		struct mp_request *req)
{
	const struct malloc_req_alloc *ar = &m->alloc_req;
	struct malloc_heap *heap;
	struct malloc_elem *elem;
	struct rte_memseg **ms;
	size_t alloc_sz;
	int n_segs;
	void *map_addr;

	alloc_sz = RTE_ALIGN_CEIL(ar->align + ar->elt_size +
			MALLOC_ELEM_TRAILER_LEN, ar->page_sz);
	n_segs = alloc_sz / ar->page_sz;

	heap = ar->heap;

	/* we can't know in advance how many pages we'll need, so we malloc */
	ms = malloc(sizeof(*ms) * n_segs);
	if (ms == NULL) {
		RTE_LOG(ERR, EAL, "Couldn't allocate memory for request state\n");
		goto fail;
	}
	memset(ms, 0, sizeof(*ms) * n_segs);

	elem = alloc_pages_on_heap(heap, ar->page_sz, ar->elt_size, ar->socket,
			ar->flags, ar->align, ar->bound, ar->contig, ms,
			n_segs);

	if (elem == NULL)
		goto fail;

	map_addr = ms[0]->addr;

	eal_memalloc_mem_event_notify(RTE_MEM_EVENT_ALLOC, map_addr, alloc_sz);

	/* we have succeeded in allocating memory, but we still need to sync
	 * with other processes. however, since DPDK IPC is single-threaded, we
	 * send an asynchronous request and exit this callback.
	 */

	req->alloc_state.ms = ms;
	req->alloc_state.ms_len = n_segs;
	req->alloc_state.map_addr = map_addr;
	req->alloc_state.map_len = alloc_sz;
	req->alloc_state.elem = elem;
	req->alloc_state.heap = heap;

	return 0;
fail:
	free(ms);
	return -1;
}

/* first stage of primary handling requests from secondary */
static int
handle_request(const struct rte_mp_msg *msg, const void *peer __rte_unused)
{
	const struct malloc_mp_req *m =
			(const struct malloc_mp_req *)msg->param;
	struct mp_request *entry;
	int ret;

	/* lock access to request */
	pthread_mutex_lock(&mp_request_list.lock);

	/* make sure it's not a dupe */
	entry = find_request_by_id(m->id);
	if (entry != NULL) {
		RTE_LOG(ERR, EAL, "Duplicate request id\n");
		goto fail;
	}

	entry = malloc(sizeof(*entry));
	if (entry == NULL) {
		RTE_LOG(ERR, EAL, "Unable to allocate memory for request\n");
		goto fail;
	}

	/* erase all data */
	memset(entry, 0, sizeof(*entry));

	if (m->t == REQ_TYPE_ALLOC) {
		ret = handle_alloc_request(m, entry);
	} else if (m->t == REQ_TYPE_FREE) {
		eal_memalloc_mem_event_notify(RTE_MEM_EVENT_FREE,
				m->free_req.addr, m->free_req.len);

		ret = malloc_heap_free_pages(m->free_req.addr,
				m->free_req.len);
	} else {
		RTE_LOG(ERR, EAL, "Unexpected request from secondary\n");
		goto fail;
	}

	if (ret != 0) {
		struct rte_mp_msg resp_msg;
		struct malloc_mp_req *resp =
				(struct malloc_mp_req *)resp_msg.param;

		/* send failure message straight away */
		resp_msg.num_fds = 0;
		resp_msg.len_param = sizeof(*resp);
		strlcpy(resp_msg.name, MP_ACTION_RESPONSE,
				sizeof(resp_msg.name));

		resp->t = m->t;
		resp->result = REQ_RESULT_FAIL;
		resp->id = m->id;

		if (rte_mp_sendmsg(&resp_msg)) {
			RTE_LOG(ERR, EAL, "Couldn't send response\n");
			goto fail;
		}
		/* we did not modify the request */
		free(entry);
	} else {
		struct rte_mp_msg sr_msg;
		struct malloc_mp_req *sr =
				(struct malloc_mp_req *)sr_msg.param;
		struct timespec ts;

		memset(&sr_msg, 0, sizeof(sr_msg));

		/* we can do something, so send sync request asynchronously */
		sr_msg.num_fds = 0;
		sr_msg.len_param = sizeof(*sr);
		strlcpy(sr_msg.name, MP_ACTION_SYNC, sizeof(sr_msg.name));

		ts.tv_nsec = 0;
		ts.tv_sec = MP_TIMEOUT_S;

		/* sync requests carry no data */
		sr->t = REQ_TYPE_SYNC;
		sr->id = m->id;

		/* there may be stray timeout still waiting */
		do {
			ret = rte_mp_request_async(&sr_msg, &ts,
					handle_sync_response);
		} while (ret != 0 && rte_errno == EEXIST);
		if (ret != 0) {
			RTE_LOG(ERR, EAL, "Couldn't send sync request\n");
			if (m->t == REQ_TYPE_ALLOC)
				free(entry->alloc_state.ms);
			goto fail;
		}

		/* mark request as in progress */
		memcpy(&entry->user_req, m, sizeof(*m));
		entry->state = REQ_STATE_ACTIVE;

		TAILQ_INSERT_TAIL(&mp_request_list.list, entry, next);
	}
	pthread_mutex_unlock(&mp_request_list.lock);
	return 0;
fail:
	pthread_mutex_unlock(&mp_request_list.lock);
	free(entry);
	return -1;
}

/* callback for asynchronous sync requests for primary. this will either do a
 * sendmsg with results, or trigger rollback request.
 */
static int
handle_sync_response(const struct rte_mp_msg *request,
		const struct rte_mp_reply *reply)
{
	enum malloc_req_result result;
	struct mp_request *entry;
	const struct malloc_mp_req *mpreq =
			(const struct malloc_mp_req *)request->param;
	int i;

	/* lock the request */
	pthread_mutex_lock(&mp_request_list.lock);

	entry = find_request_by_id(mpreq->id);
	if (entry == NULL) {
		RTE_LOG(ERR, EAL, "Wrong request ID\n");
		goto fail;
	}

	result = REQ_RESULT_SUCCESS;

	if (reply->nb_received != reply->nb_sent)
		result = REQ_RESULT_FAIL;

	for (i = 0; i < reply->nb_received; i++) {
		struct malloc_mp_req *resp =
				(struct malloc_mp_req *)reply->msgs[i].param;

		if (resp->t != REQ_TYPE_SYNC) {
			RTE_LOG(ERR, EAL, "Unexpected response to sync request\n");
			result = REQ_RESULT_FAIL;
			break;
		}
		if (resp->id != entry->user_req.id) {
			RTE_LOG(ERR, EAL, "Response to wrong sync request\n");
			result = REQ_RESULT_FAIL;
			break;
		}
		if (resp->result == REQ_RESULT_FAIL) {
			result = REQ_RESULT_FAIL;
			break;
		}
	}

	if (entry->user_req.t == REQ_TYPE_FREE) {
		struct rte_mp_msg msg;
		struct malloc_mp_req *resp = (struct malloc_mp_req *)msg.param;

		memset(&msg, 0, sizeof(msg));

		/* this is a free request, just sendmsg result */
		resp->t = REQ_TYPE_FREE;
		resp->result = result;
		resp->id = entry->user_req.id;
		msg.num_fds = 0;
		msg.len_param = sizeof(*resp);
		strlcpy(msg.name, MP_ACTION_RESPONSE, sizeof(msg.name));

		if (rte_mp_sendmsg(&msg))
			RTE_LOG(ERR, EAL, "Could not send message to secondary process\n");

		TAILQ_REMOVE(&mp_request_list.list, entry, next);
		free(entry);
	} else if (entry->user_req.t == REQ_TYPE_ALLOC &&
			result == REQ_RESULT_SUCCESS) {
		struct malloc_heap *heap = entry->alloc_state.heap;
		struct rte_mp_msg msg;
		struct malloc_mp_req *resp =
				(struct malloc_mp_req *)msg.param;

		memset(&msg, 0, sizeof(msg));

		heap->total_size += entry->alloc_state.map_len;

		/* result is success, so just notify secondary about this */
		resp->t = REQ_TYPE_ALLOC;
		resp->result = result;
		resp->id = entry->user_req.id;
		msg.num_fds = 0;
		msg.len_param = sizeof(*resp);
		strlcpy(msg.name, MP_ACTION_RESPONSE, sizeof(msg.name));

		if (rte_mp_sendmsg(&msg))
			RTE_LOG(ERR, EAL, "Could not send message to secondary process\n");

		TAILQ_REMOVE(&mp_request_list.list, entry, next);
		free(entry->alloc_state.ms);
		free(entry);
	} else if (entry->user_req.t == REQ_TYPE_ALLOC &&
			result == REQ_RESULT_FAIL) {
		struct rte_mp_msg rb_msg;
		struct malloc_mp_req *rb =
				(struct malloc_mp_req *)rb_msg.param;
		struct timespec ts;
		struct primary_alloc_req_state *state =
				&entry->alloc_state;
		int ret;

		memset(&rb_msg, 0, sizeof(rb_msg));

		/* we've failed to sync, so do a rollback */
		eal_memalloc_mem_event_notify(RTE_MEM_EVENT_FREE,
				state->map_addr, state->map_len);

		rollback_expand_heap(state->ms, state->ms_len, state->elem,
				state->map_addr, state->map_len);

		/* send rollback request */
		rb_msg.num_fds = 0;
		rb_msg.len_param = sizeof(*rb);
		strlcpy(rb_msg.name, MP_ACTION_ROLLBACK, sizeof(rb_msg.name));

		ts.tv_nsec = 0;
		ts.tv_sec = MP_TIMEOUT_S;

		/* sync requests carry no data */
		rb->t = REQ_TYPE_SYNC;
		rb->id = entry->user_req.id;

		/* there may be stray timeout still waiting */
		do {
			ret = rte_mp_request_async(&rb_msg, &ts,
					handle_rollback_response);
		} while (ret != 0 && rte_errno == EEXIST);
		if (ret != 0) {
			RTE_LOG(ERR, EAL, "Could not send rollback request to secondary process\n");

			/* we couldn't send rollback request, but that's OK -
			 * secondary will time out, and memory has been removed
			 * from heap anyway.
			 */
			TAILQ_REMOVE(&mp_request_list.list, entry, next);
			free(state->ms);
			free(entry);
			goto fail;
		}
	} else {
		RTE_LOG(ERR, EAL, " to sync request of unknown type\n");
		goto fail;
	}

	pthread_mutex_unlock(&mp_request_list.lock);
	return 0;
fail:
	pthread_mutex_unlock(&mp_request_list.lock);
	return -1;
}

static int
handle_rollback_response(const struct rte_mp_msg *request,
		const struct rte_mp_reply *reply __rte_unused)
{
	struct rte_mp_msg msg;
	struct malloc_mp_req *resp = (struct malloc_mp_req *)msg.param;
	const struct malloc_mp_req *mpreq =
			(const struct malloc_mp_req *)request->param;
	struct mp_request *entry;

	/* lock the request */
	pthread_mutex_lock(&mp_request_list.lock);

	memset(&msg, 0, sizeof(msg));

	entry = find_request_by_id(mpreq->id);
	if (entry == NULL) {
		RTE_LOG(ERR, EAL, "Wrong request ID\n");
		goto fail;
	}

	if (entry->user_req.t != REQ_TYPE_ALLOC) {
		RTE_LOG(ERR, EAL, "Unexpected active request\n");
		goto fail;
	}

	/* we don't care if rollback succeeded, request still failed */
	resp->t = REQ_TYPE_ALLOC;
	resp->result = REQ_RESULT_FAIL;
	resp->id = mpreq->id;
	msg.num_fds = 0;
	msg.len_param = sizeof(*resp);
	strlcpy(msg.name, MP_ACTION_RESPONSE, sizeof(msg.name));

	if (rte_mp_sendmsg(&msg))
		RTE_LOG(ERR, EAL, "Could not send message to secondary process\n");

	/* clean up */
	TAILQ_REMOVE(&mp_request_list.list, entry, next);
	free(entry->alloc_state.ms);
	free(entry);

	pthread_mutex_unlock(&mp_request_list.lock);
	return 0;
fail:
	pthread_mutex_unlock(&mp_request_list.lock);
	return -1;
}

/* final stage of the request from secondary */
static int
handle_response(const struct rte_mp_msg *msg, const void *peer  __rte_unused)
{
	const struct malloc_mp_req *m =
			(const struct malloc_mp_req *)msg->param;
	struct mp_request *entry;

	pthread_mutex_lock(&mp_request_list.lock);

	entry = find_request_by_id(m->id);
	if (entry != NULL) {
		/* update request status */
		entry->user_req.result = m->result;

		entry->state = REQ_STATE_COMPLETE;

		/* trigger thread wakeup */
		pthread_cond_signal(&entry->cond);
	}

	pthread_mutex_unlock(&mp_request_list.lock);

	return 0;
}

/* synchronously request memory map sync, this is only called whenever primary
 * process initiates the allocation.
 */
int
request_sync(void)
{
	struct rte_mp_msg msg;
	struct rte_mp_reply reply;
	struct malloc_mp_req *req = (struct malloc_mp_req *)msg.param;
	struct timespec ts;
	int i, ret = -1;

	memset(&msg, 0, sizeof(msg));
	memset(&reply, 0, sizeof(reply));

	/* no need to create tailq entries as this is entirely synchronous */

	msg.num_fds = 0;
	msg.len_param = sizeof(*req);
	strlcpy(msg.name, MP_ACTION_SYNC, sizeof(msg.name));

	/* sync request carries no data */
	req->t = REQ_TYPE_SYNC;
	req->id = get_unique_id();

	ts.tv_nsec = 0;
	ts.tv_sec = MP_TIMEOUT_S;

	/* there may be stray timeout still waiting */
	do {
		ret = rte_mp_request_sync(&msg, &reply, &ts);
	} while (ret != 0 && rte_errno == EEXIST);
	if (ret != 0) {
		/* if IPC is unsupported, behave as if the call succeeded */
		if (rte_errno != ENOTSUP)
			RTE_LOG(ERR, EAL, "Could not send sync request to secondary process\n");
		else
			ret = 0;
		goto out;
	}

	if (reply.nb_received != reply.nb_sent) {
		RTE_LOG(ERR, EAL, "Not all secondaries have responded\n");
		goto out;
	}

	for (i = 0; i < reply.nb_received; i++) {
		struct malloc_mp_req *resp =
				(struct malloc_mp_req *)reply.msgs[i].param;
		if (resp->t != REQ_TYPE_SYNC) {
			RTE_LOG(ERR, EAL, "Unexpected response from secondary\n");
			goto out;
		}
		if (resp->id != req->id) {
			RTE_LOG(ERR, EAL, "Wrong request ID\n");
			goto out;
		}
		if (resp->result != REQ_RESULT_SUCCESS) {
			RTE_LOG(ERR, EAL, "Secondary process failed to synchronize\n");
			goto out;
		}
	}

	ret = 0;
out:
	free(reply.msgs);
	return ret;
}

/* this is a synchronous wrapper around a bunch of asynchronous requests to
 * primary process. this will initiate a request and wait until responses come.
 */
int
request_to_primary(struct malloc_mp_req *user_req)
{
	struct rte_mp_msg msg;
	struct malloc_mp_req *msg_req = (struct malloc_mp_req *)msg.param;
	struct mp_request *entry;
	struct timespec ts;
	struct timeval now;
	int ret;

	memset(&msg, 0, sizeof(msg));
	memset(&ts, 0, sizeof(ts));

	pthread_mutex_lock(&mp_request_list.lock);

	entry = malloc(sizeof(*entry));
	if (entry == NULL) {
		RTE_LOG(ERR, EAL, "Cannot allocate memory for request\n");
		goto fail;
	}

	memset(entry, 0, sizeof(*entry));

	if (gettimeofday(&now, NULL) < 0) {
		RTE_LOG(ERR, EAL, "Cannot get current time\n");
		goto fail;
	}

	ts.tv_nsec = (now.tv_usec * 1000) % 1000000000;
	ts.tv_sec = now.tv_sec + MP_TIMEOUT_S +
			(now.tv_usec * 1000) / 1000000000;

	/* initialize the request */
	pthread_cond_init(&entry->cond, NULL);

	msg.num_fds = 0;
	msg.len_param = sizeof(*msg_req);
	strlcpy(msg.name, MP_ACTION_REQUEST, sizeof(msg.name));

	/* (attempt to) get a unique id */
	user_req->id = get_unique_id();

	/* copy contents of user request into the message */
	memcpy(msg_req, user_req, sizeof(*msg_req));

	if (rte_mp_sendmsg(&msg)) {
		RTE_LOG(ERR, EAL, "Cannot send message to primary\n");
		goto fail;
	}

	/* copy contents of user request into active request */
	memcpy(&entry->user_req, user_req, sizeof(*user_req));

	/* mark request as in progress */
	entry->state = REQ_STATE_ACTIVE;

	TAILQ_INSERT_TAIL(&mp_request_list.list, entry, next);

	/* finally, wait on timeout */
	do {
		ret = pthread_cond_timedwait(&entry->cond,
				&mp_request_list.lock, &ts);
	} while (ret != 0 && ret != ETIMEDOUT);

	if (entry->state != REQ_STATE_COMPLETE) {
		RTE_LOG(ERR, EAL, "Request timed out\n");
		ret = -1;
	} else {
		ret = 0;
		user_req->result = entry->user_req.result;
	}
	TAILQ_REMOVE(&mp_request_list.list, entry, next);
	free(entry);

	pthread_mutex_unlock(&mp_request_list.lock);
	return ret;
fail:
	pthread_mutex_unlock(&mp_request_list.lock);
	free(entry);
	return -1;
}

int
register_mp_requests(void)
{
	if (rte_eal_process_type() == RTE_PROC_PRIMARY) {
		/* it's OK for primary to not support IPC */
		if (rte_mp_action_register(MP_ACTION_REQUEST, handle_request) &&
				rte_errno != ENOTSUP) {
			RTE_LOG(ERR, EAL, "Couldn't register '%s' action\n",
				MP_ACTION_REQUEST);
			return -1;
		}
	} else {
		if (rte_mp_action_register(MP_ACTION_SYNC, handle_sync)) {
			RTE_LOG(ERR, EAL, "Couldn't register '%s' action\n",
				MP_ACTION_SYNC);
			return -1;
		}
		if (rte_mp_action_register(MP_ACTION_ROLLBACK, handle_sync)) {
			RTE_LOG(ERR, EAL, "Couldn't register '%s' action\n",
				MP_ACTION_SYNC);
			return -1;
		}
		if (rte_mp_action_register(MP_ACTION_RESPONSE,
				handle_response)) {
			RTE_LOG(ERR, EAL, "Couldn't register '%s' action\n",
				MP_ACTION_RESPONSE);
			return -1;
		}
	}
	return 0;
}
