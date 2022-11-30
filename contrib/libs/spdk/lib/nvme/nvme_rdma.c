#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2019-2021 Mellanox Technologies LTD. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * NVMe over RDMA transport
 */

#include "spdk/stdinc.h"

#include "spdk/assert.h"
#include "spdk/log.h"
#include "spdk/trace.h"
#include "spdk/queue.h"
#include "spdk/nvme.h"
#include "spdk/nvmf_spec.h"
#include "spdk/string.h"
#include "spdk/endian.h"
#include "spdk/likely.h"
#include "spdk/config.h"

#include "nvme_internal.h"
#include "spdk_internal/rdma.h"

#define NVME_RDMA_TIME_OUT_IN_MS 2000
#define NVME_RDMA_RW_BUFFER_SIZE 131072

/*
 * NVME RDMA qpair Resource Defaults
 */
#define NVME_RDMA_DEFAULT_TX_SGE		2
#define NVME_RDMA_DEFAULT_RX_SGE		1

/* Max number of NVMe-oF SGL descriptors supported by the host */
#define NVME_RDMA_MAX_SGL_DESCRIPTORS		16

/* number of STAILQ entries for holding pending RDMA CM events. */
#define NVME_RDMA_NUM_CM_EVENTS			256

/* CM event processing timeout */
#define NVME_RDMA_QPAIR_CM_EVENT_TIMEOUT_US	1000000

/* The default size for a shared rdma completion queue. */
#define DEFAULT_NVME_RDMA_CQ_SIZE		4096

/*
 * In the special case of a stale connection we don't expose a mechanism
 * for the user to retry the connection so we need to handle it internally.
 */
#define NVME_RDMA_STALE_CONN_RETRY_MAX		5
#define NVME_RDMA_STALE_CONN_RETRY_DELAY_US	10000

/*
 * Maximum value of transport_retry_count used by RDMA controller
 */
#define NVME_RDMA_CTRLR_MAX_TRANSPORT_RETRY_COUNT	7

/*
 * Maximum value of transport_ack_timeout used by RDMA controller
 */
#define NVME_RDMA_CTRLR_MAX_TRANSPORT_ACK_TIMEOUT	31

/*
 * Number of poller cycles to keep a pointer to destroyed qpairs
 * in the poll group.
 */
#define NVME_RDMA_DESTROYED_QPAIR_EXPIRATION_CYCLES	50

/*
 * The max length of keyed SGL data block (3 bytes)
 */
#define NVME_RDMA_MAX_KEYED_SGL_LENGTH ((1u << 24u) - 1)

#define WC_PER_QPAIR(queue_depth)	(queue_depth * 2)

enum nvme_rdma_wr_type {
	RDMA_WR_TYPE_RECV,
	RDMA_WR_TYPE_SEND,
};

struct nvme_rdma_wr {
	/* Using this instead of the enum allows this struct to only occupy one byte. */
	uint8_t	type;
};

struct spdk_nvmf_cmd {
	struct spdk_nvme_cmd cmd;
	struct spdk_nvme_sgl_descriptor sgl[NVME_RDMA_MAX_SGL_DESCRIPTORS];
};

struct spdk_nvme_rdma_hooks g_nvme_hooks = {};

/* STAILQ wrapper for cm events. */
struct nvme_rdma_cm_event_entry {
	struct rdma_cm_event			*evt;
	STAILQ_ENTRY(nvme_rdma_cm_event_entry)	link;
};

/* NVMe RDMA transport extensions for spdk_nvme_ctrlr */
struct nvme_rdma_ctrlr {
	struct spdk_nvme_ctrlr			ctrlr;

	struct ibv_pd				*pd;

	uint16_t				max_sge;

	struct rdma_event_channel		*cm_channel;

	STAILQ_HEAD(, nvme_rdma_cm_event_entry)	pending_cm_events;

	STAILQ_HEAD(, nvme_rdma_cm_event_entry)	free_cm_events;

	struct nvme_rdma_cm_event_entry		*cm_events;
};

struct nvme_rdma_destroyed_qpair {
	struct nvme_rdma_qpair			*destroyed_qpair_tracker;
	uint32_t				completed_cycles;
	STAILQ_ENTRY(nvme_rdma_destroyed_qpair)	link;
};

struct nvme_rdma_poller_stats {
	uint64_t polls;
	uint64_t idle_polls;
	uint64_t queued_requests;
	uint64_t completions;
	struct spdk_rdma_qp_stats rdma_stats;
};

struct nvme_rdma_poller {
	struct ibv_context		*device;
	struct ibv_cq			*cq;
	int				required_num_wc;
	int				current_num_wc;
	struct nvme_rdma_poller_stats	stats;
	STAILQ_ENTRY(nvme_rdma_poller)	link;
};

struct nvme_rdma_poll_group {
	struct spdk_nvme_transport_poll_group		group;
	STAILQ_HEAD(, nvme_rdma_poller)			pollers;
	uint32_t					num_pollers;
	STAILQ_HEAD(, nvme_rdma_destroyed_qpair)	destroyed_qpairs;
};

/* Memory regions */
union nvme_rdma_mr {
	struct ibv_mr	*mr;
	uint64_t	key;
};

/* NVMe RDMA qpair extensions for spdk_nvme_qpair */
struct nvme_rdma_qpair {
	struct spdk_nvme_qpair			qpair;

	struct spdk_rdma_qp			*rdma_qp;
	struct rdma_cm_id			*cm_id;
	struct ibv_cq				*cq;

	struct	spdk_nvme_rdma_req		*rdma_reqs;

	uint32_t				max_send_sge;

	uint32_t				max_recv_sge;

	uint16_t				num_entries;

	bool					delay_cmd_submit;

	bool					poll_group_disconnect_in_progress;

	uint32_t				num_completions;

	/* Parallel arrays of response buffers + response SGLs of size num_entries */
	struct ibv_sge				*rsp_sgls;
	struct spdk_nvme_rdma_rsp		*rsps;

	struct ibv_recv_wr			*rsp_recv_wrs;

	/* Memory region describing all rsps for this qpair */
	union nvme_rdma_mr			rsp_mr;

	/*
	 * Array of num_entries NVMe commands registered as RDMA message buffers.
	 * Indexed by rdma_req->id.
	 */
	struct spdk_nvmf_cmd			*cmds;

	/* Memory region describing all cmds for this qpair */
	union nvme_rdma_mr			cmd_mr;

	struct spdk_rdma_mem_map		*mr_map;

	TAILQ_HEAD(, spdk_nvme_rdma_req)	free_reqs;
	TAILQ_HEAD(, spdk_nvme_rdma_req)	outstanding_reqs;

	/* Counts of outstanding send and recv objects */
	uint16_t				current_num_recvs;
	uint16_t				current_num_sends;

	/* Placed at the end of the struct since it is not used frequently */
	struct rdma_cm_event			*evt;
	struct nvme_rdma_poller			*poller;

	/* Used by poll group to keep the qpair around until it is ready to remove it. */
	bool					defer_deletion_to_pg;
};

enum NVME_RDMA_COMPLETION_FLAGS {
	NVME_RDMA_SEND_COMPLETED = 1u << 0,
	NVME_RDMA_RECV_COMPLETED = 1u << 1,
};

struct spdk_nvme_rdma_req {
	uint16_t				id;
	uint16_t				completion_flags: 2;
	uint16_t				reserved: 14;
	/* if completion of RDMA_RECV received before RDMA_SEND, we will complete nvme request
	 * during processing of RDMA_SEND. To complete the request we must know the index
	 * of nvme_cpl received in RDMA_RECV, so store it in this field */
	uint16_t				rsp_idx;

	struct nvme_rdma_wr			rdma_wr;

	struct ibv_send_wr			send_wr;

	struct nvme_request			*req;

	struct ibv_sge				send_sgl[NVME_RDMA_DEFAULT_TX_SGE];

	TAILQ_ENTRY(spdk_nvme_rdma_req)		link;
};

struct spdk_nvme_rdma_rsp {
	struct spdk_nvme_cpl	cpl;
	struct nvme_rdma_qpair	*rqpair;
	uint16_t		idx;
	struct nvme_rdma_wr	rdma_wr;
};

static const char *rdma_cm_event_str[] = {
	"RDMA_CM_EVENT_ADDR_RESOLVED",
	"RDMA_CM_EVENT_ADDR_ERROR",
	"RDMA_CM_EVENT_ROUTE_RESOLVED",
	"RDMA_CM_EVENT_ROUTE_ERROR",
	"RDMA_CM_EVENT_CONNECT_REQUEST",
	"RDMA_CM_EVENT_CONNECT_RESPONSE",
	"RDMA_CM_EVENT_CONNECT_ERROR",
	"RDMA_CM_EVENT_UNREACHABLE",
	"RDMA_CM_EVENT_REJECTED",
	"RDMA_CM_EVENT_ESTABLISHED",
	"RDMA_CM_EVENT_DISCONNECTED",
	"RDMA_CM_EVENT_DEVICE_REMOVAL",
	"RDMA_CM_EVENT_MULTICAST_JOIN",
	"RDMA_CM_EVENT_MULTICAST_ERROR",
	"RDMA_CM_EVENT_ADDR_CHANGE",
	"RDMA_CM_EVENT_TIMEWAIT_EXIT"
};

struct nvme_rdma_qpair *nvme_rdma_poll_group_get_qpair_by_id(struct nvme_rdma_poll_group *group,
		uint32_t qp_num);

static inline void *
nvme_rdma_calloc(size_t nmemb, size_t size)
{
	if (!nmemb || !size) {
		return NULL;
	}

	if (!g_nvme_hooks.get_rkey) {
		return calloc(nmemb, size);
	} else {
		return spdk_zmalloc(nmemb * size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	}
}

static inline void
nvme_rdma_free(void *buf)
{
	if (!g_nvme_hooks.get_rkey) {
		free(buf);
	} else {
		spdk_free(buf);
	}
}

static int nvme_rdma_ctrlr_delete_io_qpair(struct spdk_nvme_ctrlr *ctrlr,
		struct spdk_nvme_qpair *qpair);

static inline struct nvme_rdma_qpair *
nvme_rdma_qpair(struct spdk_nvme_qpair *qpair)
{
	assert(qpair->trtype == SPDK_NVME_TRANSPORT_RDMA);
	return SPDK_CONTAINEROF(qpair, struct nvme_rdma_qpair, qpair);
}

static inline struct nvme_rdma_poll_group *
nvme_rdma_poll_group(struct spdk_nvme_transport_poll_group *group)
{
	return (SPDK_CONTAINEROF(group, struct nvme_rdma_poll_group, group));
}

static inline struct nvme_rdma_ctrlr *
nvme_rdma_ctrlr(struct spdk_nvme_ctrlr *ctrlr)
{
	assert(ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_RDMA);
	return SPDK_CONTAINEROF(ctrlr, struct nvme_rdma_ctrlr, ctrlr);
}

static struct spdk_nvme_rdma_req *
nvme_rdma_req_get(struct nvme_rdma_qpair *rqpair)
{
	struct spdk_nvme_rdma_req *rdma_req;

	rdma_req = TAILQ_FIRST(&rqpair->free_reqs);
	if (rdma_req) {
		TAILQ_REMOVE(&rqpair->free_reqs, rdma_req, link);
		TAILQ_INSERT_TAIL(&rqpair->outstanding_reqs, rdma_req, link);
	}

	return rdma_req;
}

static void
nvme_rdma_req_put(struct nvme_rdma_qpair *rqpair, struct spdk_nvme_rdma_req *rdma_req)
{
	rdma_req->completion_flags = 0;
	rdma_req->req = NULL;
	TAILQ_INSERT_HEAD(&rqpair->free_reqs, rdma_req, link);
}

static void
nvme_rdma_req_complete(struct spdk_nvme_rdma_req *rdma_req,
		       struct spdk_nvme_cpl *rsp)
{
	struct nvme_request *req = rdma_req->req;
	struct nvme_rdma_qpair *rqpair;

	assert(req != NULL);

	rqpair = nvme_rdma_qpair(req->qpair);
	TAILQ_REMOVE(&rqpair->outstanding_reqs, rdma_req, link);

	nvme_complete_request(req->cb_fn, req->cb_arg, req->qpair, req, rsp);
	nvme_free_request(req);
}

static const char *
nvme_rdma_cm_event_str_get(uint32_t event)
{
	if (event < SPDK_COUNTOF(rdma_cm_event_str)) {
		return rdma_cm_event_str[event];
	} else {
		return "Undefined";
	}
}


static int
nvme_rdma_qpair_process_cm_event(struct nvme_rdma_qpair *rqpair)
{
	struct rdma_cm_event				*event = rqpair->evt;
	struct spdk_nvmf_rdma_accept_private_data	*accept_data;
	int						rc = 0;

	if (event) {
		switch (event->event) {
		case RDMA_CM_EVENT_ADDR_RESOLVED:
		case RDMA_CM_EVENT_ADDR_ERROR:
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
		case RDMA_CM_EVENT_ROUTE_ERROR:
			break;
		case RDMA_CM_EVENT_CONNECT_REQUEST:
			break;
		case RDMA_CM_EVENT_CONNECT_ERROR:
			break;
		case RDMA_CM_EVENT_UNREACHABLE:
		case RDMA_CM_EVENT_REJECTED:
			break;
		case RDMA_CM_EVENT_CONNECT_RESPONSE:
			rc = spdk_rdma_qp_complete_connect(rqpair->rdma_qp);
		/* fall through */
		case RDMA_CM_EVENT_ESTABLISHED:
			accept_data = (struct spdk_nvmf_rdma_accept_private_data *)event->param.conn.private_data;
			if (accept_data == NULL) {
				rc = -1;
			} else {
				SPDK_DEBUGLOG(nvme, "Requested queue depth %d. Actually got queue depth %d.\n",
					      rqpair->num_entries, accept_data->crqsize);
				rqpair->num_entries = spdk_min(rqpair->num_entries, accept_data->crqsize);
			}
			break;
		case RDMA_CM_EVENT_DISCONNECTED:
			rqpair->qpair.transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_REMOTE;
			break;
		case RDMA_CM_EVENT_DEVICE_REMOVAL:
			rqpair->qpair.transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_LOCAL;
			break;
		case RDMA_CM_EVENT_MULTICAST_JOIN:
		case RDMA_CM_EVENT_MULTICAST_ERROR:
			break;
		case RDMA_CM_EVENT_ADDR_CHANGE:
			rqpair->qpair.transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_LOCAL;
			break;
		case RDMA_CM_EVENT_TIMEWAIT_EXIT:
			break;
		default:
			SPDK_ERRLOG("Unexpected Acceptor Event [%d]\n", event->event);
			break;
		}
		rqpair->evt = NULL;
		rdma_ack_cm_event(event);
	}

	return rc;
}

/*
 * This function must be called under the nvme controller's lock
 * because it touches global controller variables. The lock is taken
 * by the generic transport code before invoking a few of the functions
 * in this file: nvme_rdma_ctrlr_connect_qpair, nvme_rdma_ctrlr_delete_io_qpair,
 * and conditionally nvme_rdma_qpair_process_completions when it is calling
 * completions on the admin qpair. When adding a new call to this function, please
 * verify that it is in a situation where it falls under the lock.
 */
static int
nvme_rdma_poll_events(struct nvme_rdma_ctrlr *rctrlr)
{
	struct nvme_rdma_cm_event_entry	*entry, *tmp;
	struct nvme_rdma_qpair		*event_qpair;
	struct rdma_cm_event		*event;
	struct rdma_event_channel	*channel = rctrlr->cm_channel;

	STAILQ_FOREACH_SAFE(entry, &rctrlr->pending_cm_events, link, tmp) {
		event_qpair = nvme_rdma_qpair(entry->evt->id->context);
		if (event_qpair->evt == NULL) {
			event_qpair->evt = entry->evt;
			STAILQ_REMOVE(&rctrlr->pending_cm_events, entry, nvme_rdma_cm_event_entry, link);
			STAILQ_INSERT_HEAD(&rctrlr->free_cm_events, entry, link);
		}
	}

	while (rdma_get_cm_event(channel, &event) == 0) {
		event_qpair = nvme_rdma_qpair(event->id->context);
		if (event_qpair->evt == NULL) {
			event_qpair->evt = event;
		} else {
			assert(rctrlr == nvme_rdma_ctrlr(event_qpair->qpair.ctrlr));
			entry = STAILQ_FIRST(&rctrlr->free_cm_events);
			if (entry == NULL) {
				rdma_ack_cm_event(event);
				return -ENOMEM;
			}
			STAILQ_REMOVE(&rctrlr->free_cm_events, entry, nvme_rdma_cm_event_entry, link);
			entry->evt = event;
			STAILQ_INSERT_TAIL(&rctrlr->pending_cm_events, entry, link);
		}
	}

	if (errno == EAGAIN || errno == EWOULDBLOCK) {
		return 0;
	} else {
		return errno;
	}
}

static int
nvme_rdma_validate_cm_event(enum rdma_cm_event_type expected_evt_type,
			    struct rdma_cm_event *reaped_evt)
{
	int rc = -EBADMSG;

	if (expected_evt_type == reaped_evt->event) {
		return 0;
	}

	switch (expected_evt_type) {
	case RDMA_CM_EVENT_ESTABLISHED:
		/*
		 * There is an enum ib_cm_rej_reason in the kernel headers that sets 10 as
		 * IB_CM_REJ_STALE_CONN. I can't find the corresponding userspace but we get
		 * the same values here.
		 */
		if (reaped_evt->event == RDMA_CM_EVENT_REJECTED && reaped_evt->status == 10) {
			rc = -ESTALE;
		} else if (reaped_evt->event == RDMA_CM_EVENT_CONNECT_RESPONSE) {
			/*
			 *  If we are using a qpair which is not created using rdma cm API
			 *  then we will receive RDMA_CM_EVENT_CONNECT_RESPONSE instead of
			 *  RDMA_CM_EVENT_ESTABLISHED.
			 */
			return 0;
		}
		break;
	default:
		break;
	}

	SPDK_ERRLOG("Expected %s but received %s (%d) from CM event channel (status = %d)\n",
		    nvme_rdma_cm_event_str_get(expected_evt_type),
		    nvme_rdma_cm_event_str_get(reaped_evt->event), reaped_evt->event,
		    reaped_evt->status);
	return rc;
}

static int
nvme_rdma_process_event(struct nvme_rdma_qpair *rqpair,
			struct rdma_event_channel *channel,
			enum rdma_cm_event_type evt)
{
	struct nvme_rdma_ctrlr	*rctrlr;
	uint64_t timeout_ticks;
	int	rc = 0, rc2;

	if (rqpair->evt != NULL) {
		rc = nvme_rdma_qpair_process_cm_event(rqpair);
		if (rc) {
			return rc;
		}
	}

	timeout_ticks = (NVME_RDMA_QPAIR_CM_EVENT_TIMEOUT_US * spdk_get_ticks_hz()) / SPDK_SEC_TO_USEC +
			spdk_get_ticks();
	rctrlr = nvme_rdma_ctrlr(rqpair->qpair.ctrlr);
	assert(rctrlr != NULL);

	while (!rqpair->evt && spdk_get_ticks() < timeout_ticks && rc == 0) {
		rc = nvme_rdma_poll_events(rctrlr);
	}

	if (rc) {
		return rc;
	}

	if (rqpair->evt == NULL) {
		return -EADDRNOTAVAIL;
	}

	rc = nvme_rdma_validate_cm_event(evt, rqpair->evt);

	rc2 = nvme_rdma_qpair_process_cm_event(rqpair);
	/* bad message takes precedence over the other error codes from processing the event. */
	return rc == 0 ? rc2 : rc;
}

static int
nvme_rdma_qpair_init(struct nvme_rdma_qpair *rqpair)
{
	int			rc;
	struct spdk_rdma_qp_init_attr	attr = {};
	struct ibv_device_attr	dev_attr;
	struct nvme_rdma_ctrlr	*rctrlr;

	rc = ibv_query_device(rqpair->cm_id->verbs, &dev_attr);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to query RDMA device attributes.\n");
		return -1;
	}

	if (rqpair->qpair.poll_group) {
		assert(!rqpair->cq);
		rc = nvme_poll_group_connect_qpair(&rqpair->qpair);
		if (rc) {
			SPDK_ERRLOG("Unable to activate the rdmaqpair.\n");
			return -1;
		}
		assert(rqpair->cq);
	} else {
		rqpair->cq = ibv_create_cq(rqpair->cm_id->verbs, rqpair->num_entries * 2, rqpair, NULL, 0);
		if (!rqpair->cq) {
			SPDK_ERRLOG("Unable to create completion queue: errno %d: %s\n", errno, spdk_strerror(errno));
			return -1;
		}
	}

	rctrlr = nvme_rdma_ctrlr(rqpair->qpair.ctrlr);
	if (g_nvme_hooks.get_ibv_pd) {
		rctrlr->pd = g_nvme_hooks.get_ibv_pd(&rctrlr->ctrlr.trid, rqpair->cm_id->verbs);
	} else {
		rctrlr->pd = NULL;
	}

	attr.pd =		rctrlr->pd;
	attr.stats =		rqpair->poller ? &rqpair->poller->stats.rdma_stats : NULL;
	attr.send_cq		= rqpair->cq;
	attr.recv_cq		= rqpair->cq;
	attr.cap.max_send_wr	= rqpair->num_entries; /* SEND operations */
	attr.cap.max_recv_wr	= rqpair->num_entries; /* RECV operations */
	attr.cap.max_send_sge	= spdk_min(NVME_RDMA_DEFAULT_TX_SGE, dev_attr.max_sge);
	attr.cap.max_recv_sge	= spdk_min(NVME_RDMA_DEFAULT_RX_SGE, dev_attr.max_sge);

	rqpair->rdma_qp = spdk_rdma_qp_create(rqpair->cm_id, &attr);

	if (!rqpair->rdma_qp) {
		return -1;
	}

	/* ibv_create_qp will change the values in attr.cap. Make sure we store the proper value. */
	rqpair->max_send_sge = spdk_min(NVME_RDMA_DEFAULT_TX_SGE, attr.cap.max_send_sge);
	rqpair->max_recv_sge = spdk_min(NVME_RDMA_DEFAULT_RX_SGE, attr.cap.max_recv_sge);
	rqpair->current_num_recvs = 0;
	rqpair->current_num_sends = 0;

	rctrlr->pd = rqpair->rdma_qp->qp->pd;

	rqpair->cm_id->context = &rqpair->qpair;

	return 0;
}

static inline int
nvme_rdma_qpair_submit_sends(struct nvme_rdma_qpair *rqpair)
{
	struct ibv_send_wr *bad_send_wr = NULL;
	int rc;

	rc = spdk_rdma_qp_flush_send_wrs(rqpair->rdma_qp, &bad_send_wr);

	if (spdk_unlikely(rc)) {
		SPDK_ERRLOG("Failed to post WRs on send queue, errno %d (%s), bad_wr %p\n",
			    rc, spdk_strerror(rc), bad_send_wr);
		while (bad_send_wr != NULL) {
			assert(rqpair->current_num_sends > 0);
			rqpair->current_num_sends--;
			bad_send_wr = bad_send_wr->next;
		}
		return rc;
	}

	return 0;
}

static inline int
nvme_rdma_qpair_submit_recvs(struct nvme_rdma_qpair *rqpair)
{
	struct ibv_recv_wr *bad_recv_wr;
	int rc = 0;

	rc = spdk_rdma_qp_flush_recv_wrs(rqpair->rdma_qp, &bad_recv_wr);
	if (spdk_unlikely(rc)) {
		SPDK_ERRLOG("Failed to post WRs on receive queue, errno %d (%s), bad_wr %p\n",
			    rc, spdk_strerror(rc), bad_recv_wr);
		while (bad_recv_wr != NULL) {
			assert(rqpair->current_num_sends > 0);
			rqpair->current_num_recvs--;
			bad_recv_wr = bad_recv_wr->next;
		}
	}

	return rc;
}

/* Append the given send wr structure to the qpair's outstanding sends list. */
/* This function accepts only a single wr. */
static inline int
nvme_rdma_qpair_queue_send_wr(struct nvme_rdma_qpair *rqpair, struct ibv_send_wr *wr)
{
	assert(wr->next == NULL);

	assert(rqpair->current_num_sends < rqpair->num_entries);

	rqpair->current_num_sends++;
	spdk_rdma_qp_queue_send_wrs(rqpair->rdma_qp, wr);

	if (!rqpair->delay_cmd_submit) {
		return nvme_rdma_qpair_submit_sends(rqpair);
	}

	return 0;
}

/* Append the given recv wr structure to the qpair's outstanding recvs list. */
/* This function accepts only a single wr. */
static inline int
nvme_rdma_qpair_queue_recv_wr(struct nvme_rdma_qpair *rqpair, struct ibv_recv_wr *wr)
{

	assert(wr->next == NULL);
	assert(rqpair->current_num_recvs < rqpair->num_entries);

	rqpair->current_num_recvs++;
	spdk_rdma_qp_queue_recv_wrs(rqpair->rdma_qp, wr);

	if (!rqpair->delay_cmd_submit) {
		return nvme_rdma_qpair_submit_recvs(rqpair);
	}

	return 0;
}

#define nvme_rdma_trace_ibv_sge(sg_list) \
	if (sg_list) { \
		SPDK_DEBUGLOG(nvme, "local addr %p length 0x%x lkey 0x%x\n", \
			      (void *)(sg_list)->addr, (sg_list)->length, (sg_list)->lkey); \
	}

static int
nvme_rdma_post_recv(struct nvme_rdma_qpair *rqpair, uint16_t rsp_idx)
{
	struct ibv_recv_wr *wr;

	wr = &rqpair->rsp_recv_wrs[rsp_idx];
	wr->next = NULL;
	nvme_rdma_trace_ibv_sge(wr->sg_list);
	return nvme_rdma_qpair_queue_recv_wr(rqpair, wr);
}

static int
nvme_rdma_reg_mr(struct rdma_cm_id *cm_id, union nvme_rdma_mr *mr, void *mem, size_t length)
{
	if (!g_nvme_hooks.get_rkey) {
		mr->mr = rdma_reg_msgs(cm_id, mem, length);
		if (mr->mr == NULL) {
			SPDK_ERRLOG("Unable to register mr: %s (%d)\n",
				    spdk_strerror(errno), errno);
			return -1;
		}
	} else {
		mr->key = g_nvme_hooks.get_rkey(cm_id->pd, mem, length);
	}

	return 0;
}

static void
nvme_rdma_dereg_mr(union nvme_rdma_mr *mr)
{
	if (!g_nvme_hooks.get_rkey) {
		if (mr->mr && rdma_dereg_mr(mr->mr)) {
			SPDK_ERRLOG("Unable to de-register mr\n");
		}
	} else {
		if (mr->key) {
			g_nvme_hooks.put_rkey(mr->key);
		}
	}
	memset(mr, 0, sizeof(*mr));
}

static uint32_t
nvme_rdma_mr_get_lkey(union nvme_rdma_mr *mr)
{
	uint32_t lkey;

	if (!g_nvme_hooks.get_rkey) {
		lkey = mr->mr->lkey;
	} else {
		lkey = *((uint64_t *) mr->key);
	}

	return lkey;
}

static void
nvme_rdma_unregister_rsps(struct nvme_rdma_qpair *rqpair)
{
	nvme_rdma_dereg_mr(&rqpair->rsp_mr);
}

static void
nvme_rdma_free_rsps(struct nvme_rdma_qpair *rqpair)
{
	nvme_rdma_free(rqpair->rsps);
	rqpair->rsps = NULL;
	nvme_rdma_free(rqpair->rsp_sgls);
	rqpair->rsp_sgls = NULL;
	nvme_rdma_free(rqpair->rsp_recv_wrs);
	rqpair->rsp_recv_wrs = NULL;
}

static int
nvme_rdma_alloc_rsps(struct nvme_rdma_qpair *rqpair)
{
	rqpair->rsps = NULL;
	rqpair->rsp_recv_wrs = NULL;

	rqpair->rsp_sgls = nvme_rdma_calloc(rqpair->num_entries, sizeof(*rqpair->rsp_sgls));
	if (!rqpair->rsp_sgls) {
		SPDK_ERRLOG("Failed to allocate rsp_sgls\n");
		goto fail;
	}

	rqpair->rsp_recv_wrs = nvme_rdma_calloc(rqpair->num_entries, sizeof(*rqpair->rsp_recv_wrs));
	if (!rqpair->rsp_recv_wrs) {
		SPDK_ERRLOG("Failed to allocate rsp_recv_wrs\n");
		goto fail;
	}

	rqpair->rsps = nvme_rdma_calloc(rqpair->num_entries, sizeof(*rqpair->rsps));
	if (!rqpair->rsps) {
		SPDK_ERRLOG("can not allocate rdma rsps\n");
		goto fail;
	}

	return 0;
fail:
	nvme_rdma_free_rsps(rqpair);
	return -ENOMEM;
}

static int
nvme_rdma_register_rsps(struct nvme_rdma_qpair *rqpair)
{
	uint16_t i;
	int rc;
	uint32_t lkey;

	rc = nvme_rdma_reg_mr(rqpair->cm_id, &rqpair->rsp_mr,
			      rqpair->rsps, rqpair->num_entries * sizeof(*rqpair->rsps));

	if (rc < 0) {
		goto fail;
	}

	lkey = nvme_rdma_mr_get_lkey(&rqpair->rsp_mr);

	for (i = 0; i < rqpair->num_entries; i++) {
		struct ibv_sge *rsp_sgl = &rqpair->rsp_sgls[i];
		struct spdk_nvme_rdma_rsp *rsp = &rqpair->rsps[i];

		rsp->rqpair = rqpair;
		rsp->rdma_wr.type = RDMA_WR_TYPE_RECV;
		rsp->idx = i;
		rsp_sgl->addr = (uint64_t)&rqpair->rsps[i];
		rsp_sgl->length = sizeof(struct spdk_nvme_cpl);
		rsp_sgl->lkey = lkey;

		rqpair->rsp_recv_wrs[i].wr_id = (uint64_t)&rsp->rdma_wr;
		rqpair->rsp_recv_wrs[i].next = NULL;
		rqpair->rsp_recv_wrs[i].sg_list = rsp_sgl;
		rqpair->rsp_recv_wrs[i].num_sge = 1;

		rc = nvme_rdma_post_recv(rqpair, i);
		if (rc) {
			goto fail;
		}
	}

	rc = nvme_rdma_qpair_submit_recvs(rqpair);
	if (rc) {
		goto fail;
	}

	return 0;

fail:
	nvme_rdma_unregister_rsps(rqpair);
	return rc;
}

static void
nvme_rdma_unregister_reqs(struct nvme_rdma_qpair *rqpair)
{
	nvme_rdma_dereg_mr(&rqpair->cmd_mr);
}

static void
nvme_rdma_free_reqs(struct nvme_rdma_qpair *rqpair)
{
	if (!rqpair->rdma_reqs) {
		return;
	}

	nvme_rdma_free(rqpair->cmds);
	rqpair->cmds = NULL;

	nvme_rdma_free(rqpair->rdma_reqs);
	rqpair->rdma_reqs = NULL;
}

static int
nvme_rdma_alloc_reqs(struct nvme_rdma_qpair *rqpair)
{
	uint16_t i;

	rqpair->rdma_reqs = nvme_rdma_calloc(rqpair->num_entries, sizeof(struct spdk_nvme_rdma_req));
	if (rqpair->rdma_reqs == NULL) {
		SPDK_ERRLOG("Failed to allocate rdma_reqs\n");
		goto fail;
	}

	rqpair->cmds = nvme_rdma_calloc(rqpair->num_entries, sizeof(*rqpair->cmds));
	if (!rqpair->cmds) {
		SPDK_ERRLOG("Failed to allocate RDMA cmds\n");
		goto fail;
	}


	TAILQ_INIT(&rqpair->free_reqs);
	TAILQ_INIT(&rqpair->outstanding_reqs);
	for (i = 0; i < rqpair->num_entries; i++) {
		struct spdk_nvme_rdma_req	*rdma_req;
		struct spdk_nvmf_cmd		*cmd;

		rdma_req = &rqpair->rdma_reqs[i];
		rdma_req->rdma_wr.type = RDMA_WR_TYPE_SEND;
		cmd = &rqpair->cmds[i];

		rdma_req->id = i;

		/* The first RDMA sgl element will always point
		 * at this data structure. Depending on whether
		 * an NVMe-oF SGL is required, the length of
		 * this element may change. */
		rdma_req->send_sgl[0].addr = (uint64_t)cmd;
		rdma_req->send_wr.wr_id = (uint64_t)&rdma_req->rdma_wr;
		rdma_req->send_wr.next = NULL;
		rdma_req->send_wr.opcode = IBV_WR_SEND;
		rdma_req->send_wr.send_flags = IBV_SEND_SIGNALED;
		rdma_req->send_wr.sg_list = rdma_req->send_sgl;
		rdma_req->send_wr.imm_data = 0;

		TAILQ_INSERT_TAIL(&rqpair->free_reqs, rdma_req, link);
	}

	return 0;
fail:
	nvme_rdma_free_reqs(rqpair);
	return -ENOMEM;
}

static int
nvme_rdma_register_reqs(struct nvme_rdma_qpair *rqpair)
{
	int i;
	int rc;
	uint32_t lkey;

	rc = nvme_rdma_reg_mr(rqpair->cm_id, &rqpair->cmd_mr,
			      rqpair->cmds, rqpair->num_entries * sizeof(*rqpair->cmds));

	if (rc < 0) {
		goto fail;
	}

	lkey = nvme_rdma_mr_get_lkey(&rqpair->cmd_mr);

	for (i = 0; i < rqpair->num_entries; i++) {
		rqpair->rdma_reqs[i].send_sgl[0].lkey = lkey;
	}

	return 0;

fail:
	nvme_rdma_unregister_reqs(rqpair);
	return -ENOMEM;
}

static int
nvme_rdma_resolve_addr(struct nvme_rdma_qpair *rqpair,
		       struct sockaddr *src_addr,
		       struct sockaddr *dst_addr,
		       struct rdma_event_channel *cm_channel)
{
	int ret;

	ret = rdma_resolve_addr(rqpair->cm_id, src_addr, dst_addr,
				NVME_RDMA_TIME_OUT_IN_MS);
	if (ret) {
		SPDK_ERRLOG("rdma_resolve_addr, %d\n", errno);
		return ret;
	}

	ret = nvme_rdma_process_event(rqpair, cm_channel, RDMA_CM_EVENT_ADDR_RESOLVED);
	if (ret) {
		SPDK_ERRLOG("RDMA address resolution error\n");
		return -1;
	}

	if (rqpair->qpair.ctrlr->opts.transport_ack_timeout != SPDK_NVME_TRANSPORT_ACK_TIMEOUT_DISABLED) {
#ifdef SPDK_CONFIG_RDMA_SET_ACK_TIMEOUT
		uint8_t timeout = rqpair->qpair.ctrlr->opts.transport_ack_timeout;
		ret = rdma_set_option(rqpair->cm_id, RDMA_OPTION_ID,
				      RDMA_OPTION_ID_ACK_TIMEOUT,
				      &timeout, sizeof(timeout));
		if (ret) {
			SPDK_NOTICELOG("Can't apply RDMA_OPTION_ID_ACK_TIMEOUT %d, ret %d\n", timeout, ret);
		}
#else
		SPDK_DEBUGLOG(nvme, "transport_ack_timeout is not supported\n");
#endif
	}


	ret = rdma_resolve_route(rqpair->cm_id, NVME_RDMA_TIME_OUT_IN_MS);
	if (ret) {
		SPDK_ERRLOG("rdma_resolve_route\n");
		return ret;
	}

	ret = nvme_rdma_process_event(rqpair, cm_channel, RDMA_CM_EVENT_ROUTE_RESOLVED);
	if (ret) {
		SPDK_ERRLOG("RDMA route resolution error\n");
		return -1;
	}

	return 0;
}

static int
nvme_rdma_connect(struct nvme_rdma_qpair *rqpair)
{
	struct rdma_conn_param				param = {};
	struct spdk_nvmf_rdma_request_private_data	request_data = {};
	struct ibv_device_attr				attr;
	int						ret;
	struct spdk_nvme_ctrlr				*ctrlr;
	struct nvme_rdma_ctrlr				*rctrlr;

	ret = ibv_query_device(rqpair->cm_id->verbs, &attr);
	if (ret != 0) {
		SPDK_ERRLOG("Failed to query RDMA device attributes.\n");
		return ret;
	}

	param.responder_resources = spdk_min(rqpair->num_entries, attr.max_qp_rd_atom);

	ctrlr = rqpair->qpair.ctrlr;
	if (!ctrlr) {
		return -1;
	}
	rctrlr = nvme_rdma_ctrlr(ctrlr);
	assert(rctrlr != NULL);

	request_data.qid = rqpair->qpair.id;
	request_data.hrqsize = rqpair->num_entries;
	request_data.hsqsize = rqpair->num_entries - 1;
	request_data.cntlid = ctrlr->cntlid;

	param.private_data = &request_data;
	param.private_data_len = sizeof(request_data);
	param.retry_count = ctrlr->opts.transport_retry_count;
	param.rnr_retry_count = 7;

	/* Fields below are ignored by rdma cm if qpair has been
	 * created using rdma cm API. */
	param.srq = 0;
	param.qp_num = rqpair->rdma_qp->qp->qp_num;

	ret = rdma_connect(rqpair->cm_id, &param);
	if (ret) {
		SPDK_ERRLOG("nvme rdma connect error\n");
		return ret;
	}

	ret = nvme_rdma_process_event(rqpair, rctrlr->cm_channel, RDMA_CM_EVENT_ESTABLISHED);
	if (ret == -ESTALE) {
		SPDK_NOTICELOG("Received a stale connection notice during connection.\n");
		return -EAGAIN;
	} else if (ret) {
		SPDK_ERRLOG("RDMA connect error %d\n", ret);
		return ret;
	} else {
		return 0;
	}
}

static int
nvme_rdma_parse_addr(struct sockaddr_storage *sa, int family, const char *addr, const char *service)
{
	struct addrinfo *res;
	struct addrinfo hints;
	int ret;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = family;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = 0;

	ret = getaddrinfo(addr, service, &hints, &res);
	if (ret) {
		SPDK_ERRLOG("getaddrinfo failed: %s (%d)\n", gai_strerror(ret), ret);
		return ret;
	}

	if (res->ai_addrlen > sizeof(*sa)) {
		SPDK_ERRLOG("getaddrinfo() ai_addrlen %zu too large\n", (size_t)res->ai_addrlen);
		ret = EINVAL;
	} else {
		memcpy(sa, res->ai_addr, res->ai_addrlen);
	}

	freeaddrinfo(res);
	return ret;
}

static int
_nvme_rdma_ctrlr_connect_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	struct sockaddr_storage dst_addr;
	struct sockaddr_storage src_addr;
	bool src_addr_specified;
	int rc;
	struct nvme_rdma_ctrlr *rctrlr;
	struct nvme_rdma_qpair *rqpair;
	int family;

	rqpair = nvme_rdma_qpair(qpair);
	rctrlr = nvme_rdma_ctrlr(ctrlr);
	assert(rctrlr != NULL);

	switch (ctrlr->trid.adrfam) {
	case SPDK_NVMF_ADRFAM_IPV4:
		family = AF_INET;
		break;
	case SPDK_NVMF_ADRFAM_IPV6:
		family = AF_INET6;
		break;
	default:
		SPDK_ERRLOG("Unhandled ADRFAM %d\n", ctrlr->trid.adrfam);
		return -1;
	}

	SPDK_DEBUGLOG(nvme, "adrfam %d ai_family %d\n", ctrlr->trid.adrfam, family);

	memset(&dst_addr, 0, sizeof(dst_addr));

	SPDK_DEBUGLOG(nvme, "trsvcid is %s\n", ctrlr->trid.trsvcid);
	rc = nvme_rdma_parse_addr(&dst_addr, family, ctrlr->trid.traddr, ctrlr->trid.trsvcid);
	if (rc != 0) {
		SPDK_ERRLOG("dst_addr nvme_rdma_parse_addr() failed\n");
		return -1;
	}

	if (ctrlr->opts.src_addr[0] || ctrlr->opts.src_svcid[0]) {
		memset(&src_addr, 0, sizeof(src_addr));
		rc = nvme_rdma_parse_addr(&src_addr, family, ctrlr->opts.src_addr, ctrlr->opts.src_svcid);
		if (rc != 0) {
			SPDK_ERRLOG("src_addr nvme_rdma_parse_addr() failed\n");
			return -1;
		}
		src_addr_specified = true;
	} else {
		src_addr_specified = false;
	}

	rc = rdma_create_id(rctrlr->cm_channel, &rqpair->cm_id, rqpair, RDMA_PS_TCP);
	if (rc < 0) {
		SPDK_ERRLOG("rdma_create_id() failed\n");
		return -1;
	}

	rc = nvme_rdma_resolve_addr(rqpair,
				    src_addr_specified ? (struct sockaddr *)&src_addr : NULL,
				    (struct sockaddr *)&dst_addr, rctrlr->cm_channel);
	if (rc < 0) {
		SPDK_ERRLOG("nvme_rdma_resolve_addr() failed\n");
		return -1;
	}

	rc = nvme_rdma_qpair_init(rqpair);
	if (rc < 0) {
		SPDK_ERRLOG("nvme_rdma_qpair_init() failed\n");
		return -1;
	}

	rc = nvme_rdma_connect(rqpair);
	if (rc != 0) {
		SPDK_ERRLOG("Unable to connect the rqpair\n");
		return rc;
	}

	rc = nvme_rdma_register_reqs(rqpair);
	SPDK_DEBUGLOG(nvme, "rc =%d\n", rc);
	if (rc) {
		SPDK_ERRLOG("Unable to register rqpair RDMA requests\n");
		return -1;
	}
	SPDK_DEBUGLOG(nvme, "RDMA requests registered\n");

	rc = nvme_rdma_register_rsps(rqpair);
	SPDK_DEBUGLOG(nvme, "rc =%d\n", rc);
	if (rc < 0) {
		SPDK_ERRLOG("Unable to register rqpair RDMA responses\n");
		return -1;
	}
	SPDK_DEBUGLOG(nvme, "RDMA responses registered\n");

	rqpair->mr_map = spdk_rdma_create_mem_map(rqpair->rdma_qp->qp->pd, &g_nvme_hooks);
	if (!rqpair->mr_map) {
		SPDK_ERRLOG("Unable to register RDMA memory translation map\n");
		return -1;
	}

	rc = nvme_fabric_qpair_connect(&rqpair->qpair, rqpair->num_entries);
	if (rc < 0) {
		rqpair->qpair.transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_UNKNOWN;
		SPDK_ERRLOG("Failed to send an NVMe-oF Fabric CONNECT command\n");
		return rc;
	}

	return 0;
}

static int
nvme_rdma_ctrlr_connect_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	int rc;
	int retry_count = 0;

	rc = _nvme_rdma_ctrlr_connect_qpair(ctrlr, qpair);

	/*
	 * -EAGAIN represents the special case where the target side still thought it was connected.
	 * Most NICs will fail the first connection attempt, and the NICs will clean up whatever
	 * state they need to. After that, subsequent connection attempts will succeed.
	 */
	if (rc == -EAGAIN) {
		SPDK_NOTICELOG("Detected stale connection on Target side for qpid: %d\n", qpair->id);
		do {
			nvme_delay(NVME_RDMA_STALE_CONN_RETRY_DELAY_US);
			nvme_transport_ctrlr_disconnect_qpair(ctrlr, qpair);
			rc = _nvme_rdma_ctrlr_connect_qpair(ctrlr, qpair);
			retry_count++;
		} while (rc == -EAGAIN && retry_count < NVME_RDMA_STALE_CONN_RETRY_MAX);
	}

	return rc;
}

/*
 * Build SGL describing empty payload.
 */
static int
nvme_rdma_build_null_request(struct spdk_nvme_rdma_req *rdma_req)
{
	struct nvme_request *req = rdma_req->req;

	req->cmd.psdt = SPDK_NVME_PSDT_SGL_MPTR_CONTIG;

	/* The first element of this SGL is pointing at an
	 * spdk_nvmf_cmd object. For this particular command,
	 * we only need the first 64 bytes corresponding to
	 * the NVMe command. */
	rdma_req->send_sgl[0].length = sizeof(struct spdk_nvme_cmd);

	/* The RDMA SGL needs one element describing the NVMe command. */
	rdma_req->send_wr.num_sge = 1;

	req->cmd.dptr.sgl1.keyed.type = SPDK_NVME_SGL_TYPE_KEYED_DATA_BLOCK;
	req->cmd.dptr.sgl1.keyed.subtype = SPDK_NVME_SGL_SUBTYPE_ADDRESS;
	req->cmd.dptr.sgl1.keyed.length = 0;
	req->cmd.dptr.sgl1.keyed.key = 0;
	req->cmd.dptr.sgl1.address = 0;

	return 0;
}

/*
 * Build inline SGL describing contiguous payload buffer.
 */
static int
nvme_rdma_build_contig_inline_request(struct nvme_rdma_qpair *rqpair,
				      struct spdk_nvme_rdma_req *rdma_req)
{
	struct nvme_request *req = rdma_req->req;
	struct spdk_rdma_memory_translation mem_translation;
	void *payload;
	int rc;

	payload = req->payload.contig_or_cb_arg + req->payload_offset;
	assert(req->payload_size != 0);
	assert(nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_CONTIG);

	rc = spdk_rdma_get_translation(rqpair->mr_map, payload, req->payload_size, &mem_translation);
	if (spdk_unlikely(rc)) {
		SPDK_ERRLOG("Memory translation failed, rc %d\n", rc);
		return -1;
	}

	rdma_req->send_sgl[1].lkey = spdk_rdma_memory_translation_get_lkey(&mem_translation);

	/* The first element of this SGL is pointing at an
	 * spdk_nvmf_cmd object. For this particular command,
	 * we only need the first 64 bytes corresponding to
	 * the NVMe command. */
	rdma_req->send_sgl[0].length = sizeof(struct spdk_nvme_cmd);

	rdma_req->send_sgl[1].addr = (uint64_t)payload;
	rdma_req->send_sgl[1].length = (uint32_t)req->payload_size;

	/* The RDMA SGL contains two elements. The first describes
	 * the NVMe command and the second describes the data
	 * payload. */
	rdma_req->send_wr.num_sge = 2;

	req->cmd.psdt = SPDK_NVME_PSDT_SGL_MPTR_CONTIG;
	req->cmd.dptr.sgl1.unkeyed.type = SPDK_NVME_SGL_TYPE_DATA_BLOCK;
	req->cmd.dptr.sgl1.unkeyed.subtype = SPDK_NVME_SGL_SUBTYPE_OFFSET;
	req->cmd.dptr.sgl1.unkeyed.length = (uint32_t)req->payload_size;
	/* Inline only supported for icdoff == 0 currently.  This function will
	 * not get called for controllers with other values. */
	req->cmd.dptr.sgl1.address = (uint64_t)0;

	return 0;
}

/*
 * Build SGL describing contiguous payload buffer.
 */
static int
nvme_rdma_build_contig_request(struct nvme_rdma_qpair *rqpair,
			       struct spdk_nvme_rdma_req *rdma_req)
{
	struct nvme_request *req = rdma_req->req;
	void *payload = req->payload.contig_or_cb_arg + req->payload_offset;
	struct spdk_rdma_memory_translation mem_translation;
	int rc;

	assert(req->payload_size != 0);
	assert(nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_CONTIG);

	if (spdk_unlikely(req->payload_size > NVME_RDMA_MAX_KEYED_SGL_LENGTH)) {
		SPDK_ERRLOG("SGL length %u exceeds max keyed SGL block size %u\n",
			    req->payload_size, NVME_RDMA_MAX_KEYED_SGL_LENGTH);
		return -1;
	}

	rc = spdk_rdma_get_translation(rqpair->mr_map, payload, req->payload_size, &mem_translation);
	if (spdk_unlikely(rc)) {
		SPDK_ERRLOG("Memory translation failed, rc %d\n", rc);
		return -1;
	}

	req->cmd.dptr.sgl1.keyed.key = spdk_rdma_memory_translation_get_rkey(&mem_translation);

	/* The first element of this SGL is pointing at an
	 * spdk_nvmf_cmd object. For this particular command,
	 * we only need the first 64 bytes corresponding to
	 * the NVMe command. */
	rdma_req->send_sgl[0].length = sizeof(struct spdk_nvme_cmd);

	/* The RDMA SGL needs one element describing the NVMe command. */
	rdma_req->send_wr.num_sge = 1;

	req->cmd.psdt = SPDK_NVME_PSDT_SGL_MPTR_CONTIG;
	req->cmd.dptr.sgl1.keyed.type = SPDK_NVME_SGL_TYPE_KEYED_DATA_BLOCK;
	req->cmd.dptr.sgl1.keyed.subtype = SPDK_NVME_SGL_SUBTYPE_ADDRESS;
	req->cmd.dptr.sgl1.keyed.length = req->payload_size;
	req->cmd.dptr.sgl1.address = (uint64_t)payload;

	return 0;
}

/*
 * Build SGL describing scattered payload buffer.
 */
static int
nvme_rdma_build_sgl_request(struct nvme_rdma_qpair *rqpair,
			    struct spdk_nvme_rdma_req *rdma_req)
{
	struct nvme_request *req = rdma_req->req;
	struct spdk_nvmf_cmd *cmd = &rqpair->cmds[rdma_req->id];
	struct spdk_rdma_memory_translation mem_translation;
	void *virt_addr;
	uint32_t remaining_size;
	uint32_t sge_length;
	int rc, max_num_sgl, num_sgl_desc;

	assert(req->payload_size != 0);
	assert(nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_SGL);
	assert(req->payload.reset_sgl_fn != NULL);
	assert(req->payload.next_sge_fn != NULL);
	req->payload.reset_sgl_fn(req->payload.contig_or_cb_arg, req->payload_offset);

	max_num_sgl = req->qpair->ctrlr->max_sges;

	remaining_size = req->payload_size;
	num_sgl_desc = 0;
	do {
		rc = req->payload.next_sge_fn(req->payload.contig_or_cb_arg, &virt_addr, &sge_length);
		if (rc) {
			return -1;
		}

		sge_length = spdk_min(remaining_size, sge_length);

		if (spdk_unlikely(sge_length > NVME_RDMA_MAX_KEYED_SGL_LENGTH)) {
			SPDK_ERRLOG("SGL length %u exceeds max keyed SGL block size %u\n",
				    sge_length, NVME_RDMA_MAX_KEYED_SGL_LENGTH);
			return -1;
		}
		rc = spdk_rdma_get_translation(rqpair->mr_map, virt_addr, sge_length, &mem_translation);
		if (spdk_unlikely(rc)) {
			SPDK_ERRLOG("Memory translation failed, rc %d\n", rc);
			return -1;
		}

		cmd->sgl[num_sgl_desc].keyed.key = spdk_rdma_memory_translation_get_rkey(&mem_translation);
		cmd->sgl[num_sgl_desc].keyed.type = SPDK_NVME_SGL_TYPE_KEYED_DATA_BLOCK;
		cmd->sgl[num_sgl_desc].keyed.subtype = SPDK_NVME_SGL_SUBTYPE_ADDRESS;
		cmd->sgl[num_sgl_desc].keyed.length = sge_length;
		cmd->sgl[num_sgl_desc].address = (uint64_t)virt_addr;

		remaining_size -= sge_length;
		num_sgl_desc++;
	} while (remaining_size > 0 && num_sgl_desc < max_num_sgl);


	/* Should be impossible if we did our sgl checks properly up the stack, but do a sanity check here. */
	if (remaining_size > 0) {
		return -1;
	}

	req->cmd.psdt = SPDK_NVME_PSDT_SGL_MPTR_CONTIG;

	/* The RDMA SGL needs one element describing some portion
	 * of the spdk_nvmf_cmd structure. */
	rdma_req->send_wr.num_sge = 1;

	/*
	 * If only one SGL descriptor is required, it can be embedded directly in the command
	 * as a data block descriptor.
	 */
	if (num_sgl_desc == 1) {
		/* The first element of this SGL is pointing at an
		 * spdk_nvmf_cmd object. For this particular command,
		 * we only need the first 64 bytes corresponding to
		 * the NVMe command. */
		rdma_req->send_sgl[0].length = sizeof(struct spdk_nvme_cmd);

		req->cmd.dptr.sgl1.keyed.type = cmd->sgl[0].keyed.type;
		req->cmd.dptr.sgl1.keyed.subtype = cmd->sgl[0].keyed.subtype;
		req->cmd.dptr.sgl1.keyed.length = cmd->sgl[0].keyed.length;
		req->cmd.dptr.sgl1.keyed.key = cmd->sgl[0].keyed.key;
		req->cmd.dptr.sgl1.address = cmd->sgl[0].address;
	} else {
		/*
		 * Otherwise, The SGL descriptor embedded in the command must point to the list of
		 * SGL descriptors used to describe the operation. In that case it is a last segment descriptor.
		 */
		uint32_t descriptors_size = sizeof(struct spdk_nvme_sgl_descriptor) * num_sgl_desc;

		if (spdk_unlikely(descriptors_size > rqpair->qpair.ctrlr->ioccsz_bytes)) {
			SPDK_ERRLOG("Size of SGL descriptors (%u) exceeds ICD (%u)\n",
				    descriptors_size, rqpair->qpair.ctrlr->ioccsz_bytes);
			return -1;
		}
		rdma_req->send_sgl[0].length = sizeof(struct spdk_nvme_cmd) + descriptors_size;

		req->cmd.dptr.sgl1.unkeyed.type = SPDK_NVME_SGL_TYPE_LAST_SEGMENT;
		req->cmd.dptr.sgl1.unkeyed.subtype = SPDK_NVME_SGL_SUBTYPE_OFFSET;
		req->cmd.dptr.sgl1.unkeyed.length = descriptors_size;
		req->cmd.dptr.sgl1.address = (uint64_t)0;
	}

	return 0;
}

/*
 * Build inline SGL describing sgl payload buffer.
 */
static int
nvme_rdma_build_sgl_inline_request(struct nvme_rdma_qpair *rqpair,
				   struct spdk_nvme_rdma_req *rdma_req)
{
	struct nvme_request *req = rdma_req->req;
	struct spdk_rdma_memory_translation mem_translation;
	uint32_t length;
	void *virt_addr;
	int rc;

	assert(req->payload_size != 0);
	assert(nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_SGL);
	assert(req->payload.reset_sgl_fn != NULL);
	assert(req->payload.next_sge_fn != NULL);
	req->payload.reset_sgl_fn(req->payload.contig_or_cb_arg, req->payload_offset);

	rc = req->payload.next_sge_fn(req->payload.contig_or_cb_arg, &virt_addr, &length);
	if (rc) {
		return -1;
	}

	if (length < req->payload_size) {
		SPDK_DEBUGLOG(nvme, "Inline SGL request split so sending separately.\n");
		return nvme_rdma_build_sgl_request(rqpair, rdma_req);
	}

	if (length > req->payload_size) {
		length = req->payload_size;
	}

	rc = spdk_rdma_get_translation(rqpair->mr_map, virt_addr, length, &mem_translation);
	if (spdk_unlikely(rc)) {
		SPDK_ERRLOG("Memory translation failed, rc %d\n", rc);
		return -1;
	}

	rdma_req->send_sgl[1].addr = (uint64_t)virt_addr;
	rdma_req->send_sgl[1].length = length;
	rdma_req->send_sgl[1].lkey = spdk_rdma_memory_translation_get_lkey(&mem_translation);

	rdma_req->send_wr.num_sge = 2;

	/* The first element of this SGL is pointing at an
	 * spdk_nvmf_cmd object. For this particular command,
	 * we only need the first 64 bytes corresponding to
	 * the NVMe command. */
	rdma_req->send_sgl[0].length = sizeof(struct spdk_nvme_cmd);

	req->cmd.psdt = SPDK_NVME_PSDT_SGL_MPTR_CONTIG;
	req->cmd.dptr.sgl1.unkeyed.type = SPDK_NVME_SGL_TYPE_DATA_BLOCK;
	req->cmd.dptr.sgl1.unkeyed.subtype = SPDK_NVME_SGL_SUBTYPE_OFFSET;
	req->cmd.dptr.sgl1.unkeyed.length = (uint32_t)req->payload_size;
	/* Inline only supported for icdoff == 0 currently.  This function will
	 * not get called for controllers with other values. */
	req->cmd.dptr.sgl1.address = (uint64_t)0;

	return 0;
}

static int
nvme_rdma_req_init(struct nvme_rdma_qpair *rqpair, struct nvme_request *req,
		   struct spdk_nvme_rdma_req *rdma_req)
{
	struct spdk_nvme_ctrlr *ctrlr = rqpair->qpair.ctrlr;
	enum nvme_payload_type payload_type;
	bool icd_supported;
	int rc;

	assert(rdma_req->req == NULL);
	rdma_req->req = req;
	req->cmd.cid = rdma_req->id;
	payload_type = nvme_payload_type(&req->payload);
	/*
	 * Check if icdoff is non zero, to avoid interop conflicts with
	 * targets with non-zero icdoff.  Both SPDK and the Linux kernel
	 * targets use icdoff = 0.  For targets with non-zero icdoff, we
	 * will currently just not use inline data for now.
	 */
	icd_supported = spdk_nvme_opc_get_data_transfer(req->cmd.opc) == SPDK_NVME_DATA_HOST_TO_CONTROLLER
			&& req->payload_size <= ctrlr->ioccsz_bytes && ctrlr->icdoff == 0;

	if (req->payload_size == 0) {
		rc = nvme_rdma_build_null_request(rdma_req);
	} else if (payload_type == NVME_PAYLOAD_TYPE_CONTIG) {
		if (icd_supported) {
			rc = nvme_rdma_build_contig_inline_request(rqpair, rdma_req);
		} else {
			rc = nvme_rdma_build_contig_request(rqpair, rdma_req);
		}
	} else if (payload_type == NVME_PAYLOAD_TYPE_SGL) {
		if (icd_supported) {
			rc = nvme_rdma_build_sgl_inline_request(rqpair, rdma_req);
		} else {
			rc = nvme_rdma_build_sgl_request(rqpair, rdma_req);
		}
	} else {
		rc = -1;
	}

	if (rc) {
		rdma_req->req = NULL;
		return rc;
	}

	memcpy(&rqpair->cmds[rdma_req->id], &req->cmd, sizeof(req->cmd));
	return 0;
}

static struct spdk_nvme_qpair *
nvme_rdma_ctrlr_create_qpair(struct spdk_nvme_ctrlr *ctrlr,
			     uint16_t qid, uint32_t qsize,
			     enum spdk_nvme_qprio qprio,
			     uint32_t num_requests,
			     bool delay_cmd_submit)
{
	struct nvme_rdma_qpair *rqpair;
	struct spdk_nvme_qpair *qpair;
	int rc;

	rqpair = nvme_rdma_calloc(1, sizeof(struct nvme_rdma_qpair));
	if (!rqpair) {
		SPDK_ERRLOG("failed to get create rqpair\n");
		return NULL;
	}

	rqpair->num_entries = qsize;
	rqpair->delay_cmd_submit = delay_cmd_submit;
	qpair = &rqpair->qpair;
	rc = nvme_qpair_init(qpair, qid, ctrlr, qprio, num_requests);
	if (rc != 0) {
		nvme_rdma_free(rqpair);
		return NULL;
	}

	rc = nvme_rdma_alloc_reqs(rqpair);
	SPDK_DEBUGLOG(nvme, "rc =%d\n", rc);
	if (rc) {
		SPDK_ERRLOG("Unable to allocate rqpair RDMA requests\n");
		nvme_rdma_free(rqpair);
		return NULL;
	}
	SPDK_DEBUGLOG(nvme, "RDMA requests allocated\n");

	rc = nvme_rdma_alloc_rsps(rqpair);
	SPDK_DEBUGLOG(nvme, "rc =%d\n", rc);
	if (rc < 0) {
		SPDK_ERRLOG("Unable to allocate rqpair RDMA responses\n");
		nvme_rdma_free_reqs(rqpair);
		nvme_rdma_free(rqpair);
		return NULL;
	}
	SPDK_DEBUGLOG(nvme, "RDMA responses allocated\n");

	return qpair;
}

static void
nvme_rdma_ctrlr_disconnect_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	struct nvme_rdma_qpair *rqpair = nvme_rdma_qpair(qpair);
	struct nvme_rdma_ctrlr *rctrlr = NULL;
	struct nvme_rdma_cm_event_entry *entry, *tmp;
	int rc;

	spdk_rdma_free_mem_map(&rqpair->mr_map);
	nvme_rdma_unregister_reqs(rqpair);
	nvme_rdma_unregister_rsps(rqpair);

	if (rqpair->evt) {
		rdma_ack_cm_event(rqpair->evt);
		rqpair->evt = NULL;
	}

	/*
	 * This works because we have the controller lock both in
	 * this function and in the function where we add new events.
	 */
	if (qpair->ctrlr != NULL) {
		rctrlr = nvme_rdma_ctrlr(qpair->ctrlr);
		STAILQ_FOREACH_SAFE(entry, &rctrlr->pending_cm_events, link, tmp) {
			if (nvme_rdma_qpair(entry->evt->id->context) == rqpair) {
				STAILQ_REMOVE(&rctrlr->pending_cm_events, entry, nvme_rdma_cm_event_entry, link);
				rdma_ack_cm_event(entry->evt);
				STAILQ_INSERT_HEAD(&rctrlr->free_cm_events, entry, link);
			}
		}
	}

	if (rqpair->cm_id) {
		if (rqpair->rdma_qp) {
			rc = spdk_rdma_qp_disconnect(rqpair->rdma_qp);
			if ((rctrlr != NULL) && (rc == 0)) {
				if (nvme_rdma_process_event(rqpair, rctrlr->cm_channel, RDMA_CM_EVENT_DISCONNECTED)) {
					SPDK_DEBUGLOG(nvme, "Target did not respond to qpair disconnect.\n");
				}
			}
			spdk_rdma_qp_destroy(rqpair->rdma_qp);
			rqpair->rdma_qp = NULL;
		}

		rdma_destroy_id(rqpair->cm_id);
		rqpair->cm_id = NULL;
	}

	if (rqpair->cq) {
		ibv_destroy_cq(rqpair->cq);
		rqpair->cq = NULL;
	}
}

static void nvme_rdma_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr);

static int
nvme_rdma_ctrlr_delete_io_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	struct nvme_rdma_qpair *rqpair;

	assert(qpair != NULL);
	rqpair = nvme_rdma_qpair(qpair);
	nvme_transport_ctrlr_disconnect_qpair(ctrlr, qpair);
	if (rqpair->defer_deletion_to_pg) {
		nvme_qpair_set_state(qpair, NVME_QPAIR_DESTROYING);
		return 0;
	}

	nvme_rdma_qpair_abort_reqs(qpair, 1);
	nvme_qpair_deinit(qpair);

	nvme_rdma_free_reqs(rqpair);
	nvme_rdma_free_rsps(rqpair);
	nvme_rdma_free(rqpair);

	return 0;
}

static struct spdk_nvme_qpair *
nvme_rdma_ctrlr_create_io_qpair(struct spdk_nvme_ctrlr *ctrlr, uint16_t qid,
				const struct spdk_nvme_io_qpair_opts *opts)
{
	return nvme_rdma_ctrlr_create_qpair(ctrlr, qid, opts->io_queue_size, opts->qprio,
					    opts->io_queue_requests,
					    opts->delay_cmd_submit);
}

static int
nvme_rdma_ctrlr_enable(struct spdk_nvme_ctrlr *ctrlr)
{
	/* do nothing here */
	return 0;
}

static int nvme_rdma_ctrlr_destruct(struct spdk_nvme_ctrlr *ctrlr);

static struct spdk_nvme_ctrlr *nvme_rdma_ctrlr_construct(const struct spdk_nvme_transport_id *trid,
		const struct spdk_nvme_ctrlr_opts *opts,
		void *devhandle)
{
	struct nvme_rdma_ctrlr *rctrlr;
	union spdk_nvme_cap_register cap;
	union spdk_nvme_vs_register vs;
	struct ibv_context **contexts;
	struct ibv_device_attr dev_attr;
	int i, flag, rc;

	rctrlr = nvme_rdma_calloc(1, sizeof(struct nvme_rdma_ctrlr));
	if (rctrlr == NULL) {
		SPDK_ERRLOG("could not allocate ctrlr\n");
		return NULL;
	}

	rctrlr->ctrlr.opts = *opts;
	rctrlr->ctrlr.trid = *trid;

	if (opts->transport_retry_count > NVME_RDMA_CTRLR_MAX_TRANSPORT_RETRY_COUNT) {
		SPDK_NOTICELOG("transport_retry_count exceeds max value %d, use max value\n",
			       NVME_RDMA_CTRLR_MAX_TRANSPORT_RETRY_COUNT);
		rctrlr->ctrlr.opts.transport_retry_count = NVME_RDMA_CTRLR_MAX_TRANSPORT_RETRY_COUNT;
	}

	if (opts->transport_ack_timeout > NVME_RDMA_CTRLR_MAX_TRANSPORT_ACK_TIMEOUT) {
		SPDK_NOTICELOG("transport_ack_timeout exceeds max value %d, use max value\n",
			       NVME_RDMA_CTRLR_MAX_TRANSPORT_ACK_TIMEOUT);
		rctrlr->ctrlr.opts.transport_ack_timeout = NVME_RDMA_CTRLR_MAX_TRANSPORT_ACK_TIMEOUT;
	}

	contexts = rdma_get_devices(NULL);
	if (contexts == NULL) {
		SPDK_ERRLOG("rdma_get_devices() failed: %s (%d)\n", spdk_strerror(errno), errno);
		nvme_rdma_free(rctrlr);
		return NULL;
	}

	i = 0;
	rctrlr->max_sge = NVME_RDMA_MAX_SGL_DESCRIPTORS;

	while (contexts[i] != NULL) {
		rc = ibv_query_device(contexts[i], &dev_attr);
		if (rc < 0) {
			SPDK_ERRLOG("Failed to query RDMA device attributes.\n");
			rdma_free_devices(contexts);
			nvme_rdma_free(rctrlr);
			return NULL;
		}
		rctrlr->max_sge = spdk_min(rctrlr->max_sge, (uint16_t)dev_attr.max_sge);
		i++;
	}

	rdma_free_devices(contexts);

	rc = nvme_ctrlr_construct(&rctrlr->ctrlr);
	if (rc != 0) {
		nvme_rdma_free(rctrlr);
		return NULL;
	}

	STAILQ_INIT(&rctrlr->pending_cm_events);
	STAILQ_INIT(&rctrlr->free_cm_events);
	rctrlr->cm_events = nvme_rdma_calloc(NVME_RDMA_NUM_CM_EVENTS, sizeof(*rctrlr->cm_events));
	if (rctrlr->cm_events == NULL) {
		SPDK_ERRLOG("unable to allocat buffers to hold CM events.\n");
		goto destruct_ctrlr;
	}

	for (i = 0; i < NVME_RDMA_NUM_CM_EVENTS; i++) {
		STAILQ_INSERT_TAIL(&rctrlr->free_cm_events, &rctrlr->cm_events[i], link);
	}

	rctrlr->cm_channel = rdma_create_event_channel();
	if (rctrlr->cm_channel == NULL) {
		SPDK_ERRLOG("rdma_create_event_channel() failed\n");
		goto destruct_ctrlr;
	}

	flag = fcntl(rctrlr->cm_channel->fd, F_GETFL);
	if (fcntl(rctrlr->cm_channel->fd, F_SETFL, flag | O_NONBLOCK) < 0) {
		SPDK_ERRLOG("Cannot set event channel to non blocking\n");
		goto destruct_ctrlr;
	}

	rctrlr->ctrlr.adminq = nvme_rdma_ctrlr_create_qpair(&rctrlr->ctrlr, 0,
			       rctrlr->ctrlr.opts.admin_queue_size, 0,
			       rctrlr->ctrlr.opts.admin_queue_size, false);
	if (!rctrlr->ctrlr.adminq) {
		SPDK_ERRLOG("failed to create admin qpair\n");
		goto destruct_ctrlr;
	}

	rc = nvme_transport_ctrlr_connect_qpair(&rctrlr->ctrlr, rctrlr->ctrlr.adminq);
	if (rc < 0) {
		SPDK_ERRLOG("failed to connect admin qpair\n");
		goto destruct_ctrlr;
	}

	if (nvme_ctrlr_get_cap(&rctrlr->ctrlr, &cap)) {
		SPDK_ERRLOG("get_cap() failed\n");
		goto destruct_ctrlr;
	}

	if (nvme_ctrlr_get_vs(&rctrlr->ctrlr, &vs)) {
		SPDK_ERRLOG("get_vs() failed\n");
		goto destruct_ctrlr;
	}

	if (nvme_ctrlr_add_process(&rctrlr->ctrlr, 0) != 0) {
		SPDK_ERRLOG("nvme_ctrlr_add_process() failed\n");
		goto destruct_ctrlr;
	}

	nvme_ctrlr_init_cap(&rctrlr->ctrlr, &cap, &vs);

	SPDK_DEBUGLOG(nvme, "successfully initialized the nvmf ctrlr\n");
	return &rctrlr->ctrlr;

destruct_ctrlr:
	nvme_ctrlr_destruct(&rctrlr->ctrlr);
	return NULL;
}

static int
nvme_rdma_ctrlr_destruct(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_rdma_ctrlr *rctrlr = nvme_rdma_ctrlr(ctrlr);
	struct nvme_rdma_cm_event_entry *entry;

	if (ctrlr->adminq) {
		nvme_rdma_ctrlr_delete_io_qpair(ctrlr, ctrlr->adminq);
	}

	STAILQ_FOREACH(entry, &rctrlr->pending_cm_events, link) {
		rdma_ack_cm_event(entry->evt);
	}

	STAILQ_INIT(&rctrlr->free_cm_events);
	STAILQ_INIT(&rctrlr->pending_cm_events);
	nvme_rdma_free(rctrlr->cm_events);

	if (rctrlr->cm_channel) {
		rdma_destroy_event_channel(rctrlr->cm_channel);
		rctrlr->cm_channel = NULL;
	}

	nvme_ctrlr_destruct_finish(ctrlr);

	nvme_rdma_free(rctrlr);

	return 0;
}

static int
nvme_rdma_qpair_submit_request(struct spdk_nvme_qpair *qpair,
			       struct nvme_request *req)
{
	struct nvme_rdma_qpair *rqpair;
	struct spdk_nvme_rdma_req *rdma_req;
	struct ibv_send_wr *wr;

	rqpair = nvme_rdma_qpair(qpair);
	assert(rqpair != NULL);
	assert(req != NULL);

	rdma_req = nvme_rdma_req_get(rqpair);
	if (spdk_unlikely(!rdma_req)) {
		if (rqpair->poller) {
			rqpair->poller->stats.queued_requests++;
		}
		/* Inform the upper layer to try again later. */
		return -EAGAIN;
	}

	if (nvme_rdma_req_init(rqpair, req, rdma_req)) {
		SPDK_ERRLOG("nvme_rdma_req_init() failed\n");
		TAILQ_REMOVE(&rqpair->outstanding_reqs, rdma_req, link);
		nvme_rdma_req_put(rqpair, rdma_req);
		return -1;
	}

	wr = &rdma_req->send_wr;
	wr->next = NULL;
	nvme_rdma_trace_ibv_sge(wr->sg_list);
	return nvme_rdma_qpair_queue_send_wr(rqpair, wr);
}

static int
nvme_rdma_qpair_reset(struct spdk_nvme_qpair *qpair)
{
	/* Currently, doing nothing here */
	return 0;
}

static void
nvme_rdma_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr)
{
	struct spdk_nvme_rdma_req *rdma_req, *tmp;
	struct spdk_nvme_cpl cpl;
	struct nvme_rdma_qpair *rqpair = nvme_rdma_qpair(qpair);

	cpl.status.sc = SPDK_NVME_SC_ABORTED_SQ_DELETION;
	cpl.status.sct = SPDK_NVME_SCT_GENERIC;
	cpl.status.dnr = dnr;

	/*
	 * We cannot abort requests at the RDMA layer without
	 * unregistering them. If we do, we can still get error
	 * free completions on the shared completion queue.
	 */
	if (nvme_qpair_get_state(qpair) > NVME_QPAIR_DISCONNECTING &&
	    nvme_qpair_get_state(qpair) != NVME_QPAIR_DESTROYING) {
		nvme_ctrlr_disconnect_qpair(qpair);
	}

	TAILQ_FOREACH_SAFE(rdma_req, &rqpair->outstanding_reqs, link, tmp) {
		nvme_rdma_req_complete(rdma_req, &cpl);
		nvme_rdma_req_put(rqpair, rdma_req);
	}
}

static void
nvme_rdma_qpair_check_timeout(struct spdk_nvme_qpair *qpair)
{
	uint64_t t02;
	struct spdk_nvme_rdma_req *rdma_req, *tmp;
	struct nvme_rdma_qpair *rqpair = nvme_rdma_qpair(qpair);
	struct spdk_nvme_ctrlr *ctrlr = qpair->ctrlr;
	struct spdk_nvme_ctrlr_process *active_proc;

	/* Don't check timeouts during controller initialization. */
	if (ctrlr->state != NVME_CTRLR_STATE_READY) {
		return;
	}

	if (nvme_qpair_is_admin_queue(qpair)) {
		active_proc = nvme_ctrlr_get_current_process(ctrlr);
	} else {
		active_proc = qpair->active_proc;
	}

	/* Only check timeouts if the current process has a timeout callback. */
	if (active_proc == NULL || active_proc->timeout_cb_fn == NULL) {
		return;
	}

	t02 = spdk_get_ticks();
	TAILQ_FOREACH_SAFE(rdma_req, &rqpair->outstanding_reqs, link, tmp) {
		assert(rdma_req->req != NULL);

		if (nvme_request_check_timeout(rdma_req->req, rdma_req->id, active_proc, t02)) {
			/*
			 * The requests are in order, so as soon as one has not timed out,
			 * stop iterating.
			 */
			break;
		}
	}
}

static inline int
nvme_rdma_request_ready(struct nvme_rdma_qpair *rqpair, struct spdk_nvme_rdma_req *rdma_req)
{
	nvme_rdma_req_complete(rdma_req, &rqpair->rsps[rdma_req->rsp_idx].cpl);
	nvme_rdma_req_put(rqpair, rdma_req);
	return nvme_rdma_post_recv(rqpair, rdma_req->rsp_idx);
}

#define MAX_COMPLETIONS_PER_POLL 128

static void
nvme_rdma_fail_qpair(struct spdk_nvme_qpair *qpair, int failure_reason)
{
	if (failure_reason == IBV_WC_RETRY_EXC_ERR) {
		qpair->transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_REMOTE;
	} else if (qpair->transport_failure_reason == SPDK_NVME_QPAIR_FAILURE_NONE) {
		qpair->transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_UNKNOWN;
	}

	nvme_ctrlr_disconnect_qpair(qpair);
}

static void
nvme_rdma_conditional_fail_qpair(struct nvme_rdma_qpair *rqpair, struct nvme_rdma_poll_group *group)
{
	struct nvme_rdma_destroyed_qpair	*qpair_tracker;

	assert(rqpair);
	if (group) {
		STAILQ_FOREACH(qpair_tracker, &group->destroyed_qpairs, link) {
			if (qpair_tracker->destroyed_qpair_tracker == rqpair) {
				return;
			}
		}
	}
	nvme_rdma_fail_qpair(&rqpair->qpair, 0);
}

static int
nvme_rdma_cq_process_completions(struct ibv_cq *cq, uint32_t batch_size,
				 struct nvme_rdma_poll_group *group,
				 struct nvme_rdma_qpair *rdma_qpair,
				 uint64_t *rdma_completions)
{
	struct ibv_wc			wc[MAX_COMPLETIONS_PER_POLL];
	struct nvme_rdma_qpair		*rqpair;
	struct spdk_nvme_rdma_req	*rdma_req;
	struct spdk_nvme_rdma_rsp	*rdma_rsp;
	struct nvme_rdma_wr		*rdma_wr;
	uint32_t			reaped = 0;
	int				completion_rc = 0;
	int				rc, i;

	rc = ibv_poll_cq(cq, batch_size, wc);
	if (rc < 0) {
		SPDK_ERRLOG("Error polling CQ! (%d): %s\n",
			    errno, spdk_strerror(errno));
		return -ECANCELED;
	} else if (rc == 0) {
		return 0;
	}

	for (i = 0; i < rc; i++) {
		rdma_wr = (struct nvme_rdma_wr *)wc[i].wr_id;
		switch (rdma_wr->type) {
		case RDMA_WR_TYPE_RECV:
			rdma_rsp = SPDK_CONTAINEROF(rdma_wr, struct spdk_nvme_rdma_rsp, rdma_wr);
			rqpair = rdma_rsp->rqpair;
			assert(rqpair->current_num_recvs > 0);
			rqpair->current_num_recvs--;

			if (wc[i].status) {
				SPDK_ERRLOG("CQ error on Queue Pair %p, Response Index %lu (%d): %s\n",
					    rqpair, wc[i].wr_id, wc[i].status, ibv_wc_status_str(wc[i].status));
				nvme_rdma_conditional_fail_qpair(rqpair, group);
				completion_rc = -ENXIO;
				continue;
			}

			SPDK_DEBUGLOG(nvme, "CQ recv completion\n");

			if (wc[i].byte_len < sizeof(struct spdk_nvme_cpl)) {
				SPDK_ERRLOG("recv length %u less than expected response size\n", wc[i].byte_len);
				nvme_rdma_conditional_fail_qpair(rqpair, group);
				completion_rc = -ENXIO;
				continue;
			}
			rdma_req = &rqpair->rdma_reqs[rdma_rsp->cpl.cid];
			rdma_req->completion_flags |= NVME_RDMA_RECV_COMPLETED;
			rdma_req->rsp_idx = rdma_rsp->idx;

			if ((rdma_req->completion_flags & NVME_RDMA_SEND_COMPLETED) != 0) {
				if (spdk_unlikely(nvme_rdma_request_ready(rqpair, rdma_req))) {
					SPDK_ERRLOG("Unable to re-post rx descriptor\n");
					nvme_rdma_conditional_fail_qpair(rqpair, group);
					completion_rc = -ENXIO;
					continue;
				}
				reaped++;
				rqpair->num_completions++;
			}
			break;

		case RDMA_WR_TYPE_SEND:
			rdma_req = SPDK_CONTAINEROF(rdma_wr, struct spdk_nvme_rdma_req, rdma_wr);

			/* If we are flushing I/O */
			if (wc[i].status) {
				rqpair = rdma_req->req ? nvme_rdma_qpair(rdma_req->req->qpair) : NULL;
				if (!rqpair) {
					rqpair = rdma_qpair != NULL ? rdma_qpair : nvme_rdma_poll_group_get_qpair_by_id(group,
							wc[i].qp_num);
				}
				assert(rqpair);
				assert(rqpair->current_num_sends > 0);
				rqpair->current_num_sends--;
				nvme_rdma_conditional_fail_qpair(rqpair, group);
				SPDK_ERRLOG("CQ error on Queue Pair %p, Response Index %lu (%d): %s\n",
					    rqpair, wc[i].wr_id, wc[i].status, ibv_wc_status_str(wc[i].status));
				completion_rc = -ENXIO;
				continue;
			}

			rqpair = nvme_rdma_qpair(rdma_req->req->qpair);
			rdma_req->completion_flags |= NVME_RDMA_SEND_COMPLETED;
			rqpair->current_num_sends--;

			if ((rdma_req->completion_flags & NVME_RDMA_RECV_COMPLETED) != 0) {
				if (spdk_unlikely(nvme_rdma_request_ready(rqpair, rdma_req))) {
					SPDK_ERRLOG("Unable to re-post rx descriptor\n");
					nvme_rdma_conditional_fail_qpair(rqpair, group);
					completion_rc = -ENXIO;
					continue;
				}
				reaped++;
				rqpair->num_completions++;
			}
			break;

		default:
			SPDK_ERRLOG("Received an unexpected opcode on the CQ: %d\n", rdma_wr->type);
			return -ECANCELED;
		}
	}

	*rdma_completions += rc;

	if (completion_rc) {
		return completion_rc;
	}

	return reaped;
}

static void
dummy_disconnected_qpair_cb(struct spdk_nvme_qpair *qpair, void *poll_group_ctx)
{

}

static int
nvme_rdma_qpair_process_completions(struct spdk_nvme_qpair *qpair,
				    uint32_t max_completions)
{
	struct nvme_rdma_qpair		*rqpair = nvme_rdma_qpair(qpair);
	int				rc = 0, batch_size;
	struct ibv_cq			*cq;
	struct nvme_rdma_ctrlr		*rctrlr;
	uint64_t			rdma_completions = 0;

	/*
	 * This is used during the connection phase. It's possible that we are still reaping error completions
	 * from other qpairs so we need to call the poll group function. Also, it's more correct since the cq
	 * is shared.
	 */
	if (qpair->poll_group != NULL) {
		return spdk_nvme_poll_group_process_completions(qpair->poll_group->group, max_completions,
				dummy_disconnected_qpair_cb);
	}

	if (max_completions == 0) {
		max_completions = rqpair->num_entries;
	} else {
		max_completions = spdk_min(max_completions, rqpair->num_entries);
	}

	if (nvme_qpair_is_admin_queue(&rqpair->qpair)) {
		rctrlr = nvme_rdma_ctrlr(rqpair->qpair.ctrlr);
		nvme_rdma_poll_events(rctrlr);
	}
	nvme_rdma_qpair_process_cm_event(rqpair);

	if (spdk_unlikely(qpair->transport_failure_reason != SPDK_NVME_QPAIR_FAILURE_NONE)) {
		nvme_rdma_fail_qpair(qpair, 0);
		return -ENXIO;
	}

	cq = rqpair->cq;

	rqpair->num_completions = 0;
	do {
		batch_size = spdk_min((max_completions - rqpair->num_completions), MAX_COMPLETIONS_PER_POLL);
		rc = nvme_rdma_cq_process_completions(cq, batch_size, NULL, rqpair, &rdma_completions);

		if (rc == 0) {
			break;
			/* Handle the case where we fail to poll the cq. */
		} else if (rc == -ECANCELED) {
			nvme_rdma_fail_qpair(qpair, 0);
			return -ENXIO;
		} else if (rc == -ENXIO) {
			return rc;
		}
	} while (rqpair->num_completions < max_completions);

	if (spdk_unlikely(nvme_rdma_qpair_submit_sends(rqpair) ||
			  nvme_rdma_qpair_submit_recvs(rqpair))) {
		nvme_rdma_fail_qpair(qpair, 0);
		return -ENXIO;
	}

	if (spdk_unlikely(rqpair->qpair.ctrlr->timeout_enabled)) {
		nvme_rdma_qpair_check_timeout(qpair);
	}

	return rqpair->num_completions;
}

static uint32_t
nvme_rdma_ctrlr_get_max_xfer_size(struct spdk_nvme_ctrlr *ctrlr)
{
	/* max_mr_size by ibv_query_device indicates the largest value that we can
	 * set for a registered memory region.  It is independent from the actual
	 * I/O size and is very likely to be larger than 2 MiB which is the
	 * granularity we currently register memory regions.  Hence return
	 * UINT32_MAX here and let the generic layer use the controller data to
	 * moderate this value.
	 */
	return UINT32_MAX;
}

static uint16_t
nvme_rdma_ctrlr_get_max_sges(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_rdma_ctrlr *rctrlr = nvme_rdma_ctrlr(ctrlr);

	return rctrlr->max_sge;
}

static int
nvme_rdma_qpair_iterate_requests(struct spdk_nvme_qpair *qpair,
				 int (*iter_fn)(struct nvme_request *req, void *arg),
				 void *arg)
{
	struct nvme_rdma_qpair *rqpair = nvme_rdma_qpair(qpair);
	struct spdk_nvme_rdma_req *rdma_req, *tmp;
	int rc;

	assert(iter_fn != NULL);

	TAILQ_FOREACH_SAFE(rdma_req, &rqpair->outstanding_reqs, link, tmp) {
		assert(rdma_req->req != NULL);

		rc = iter_fn(rdma_req->req, arg);
		if (rc != 0) {
			return rc;
		}
	}

	return 0;
}

static void
nvme_rdma_admin_qpair_abort_aers(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_rdma_req *rdma_req, *tmp;
	struct spdk_nvme_cpl cpl;
	struct nvme_rdma_qpair *rqpair = nvme_rdma_qpair(qpair);

	cpl.status.sc = SPDK_NVME_SC_ABORTED_SQ_DELETION;
	cpl.status.sct = SPDK_NVME_SCT_GENERIC;

	TAILQ_FOREACH_SAFE(rdma_req, &rqpair->outstanding_reqs, link, tmp) {
		assert(rdma_req->req != NULL);

		if (rdma_req->req->cmd.opc != SPDK_NVME_OPC_ASYNC_EVENT_REQUEST) {
			continue;
		}

		nvme_rdma_req_complete(rdma_req, &cpl);
		nvme_rdma_req_put(rqpair, rdma_req);
	}
}

static int
nvme_rdma_poller_create(struct nvme_rdma_poll_group *group, struct ibv_context *ctx)
{
	struct nvme_rdma_poller *poller;

	poller = calloc(1, sizeof(*poller));
	if (poller == NULL) {
		SPDK_ERRLOG("Unable to allocate poller.\n");
		return -ENOMEM;
	}

	poller->device = ctx;
	poller->cq = ibv_create_cq(poller->device, DEFAULT_NVME_RDMA_CQ_SIZE, group, NULL, 0);

	if (poller->cq == NULL) {
		free(poller);
		return -EINVAL;
	}

	STAILQ_INSERT_HEAD(&group->pollers, poller, link);
	group->num_pollers++;
	poller->current_num_wc = DEFAULT_NVME_RDMA_CQ_SIZE;
	poller->required_num_wc = 0;
	return 0;
}

static void
nvme_rdma_poll_group_free_pollers(struct nvme_rdma_poll_group *group)
{
	struct nvme_rdma_poller	*poller, *tmp_poller;

	STAILQ_FOREACH_SAFE(poller, &group->pollers, link, tmp_poller) {
		if (poller->cq) {
			ibv_destroy_cq(poller->cq);
		}
		STAILQ_REMOVE(&group->pollers, poller, nvme_rdma_poller, link);
		free(poller);
	}
}

static struct spdk_nvme_transport_poll_group *
nvme_rdma_poll_group_create(void)
{
	struct nvme_rdma_poll_group	*group;
	struct ibv_context		**contexts;
	int i = 0;

	group = calloc(1, sizeof(*group));
	if (group == NULL) {
		SPDK_ERRLOG("Unable to allocate poll group.\n");
		return NULL;
	}

	STAILQ_INIT(&group->pollers);

	contexts = rdma_get_devices(NULL);
	if (contexts == NULL) {
		SPDK_ERRLOG("rdma_get_devices() failed: %s (%d)\n", spdk_strerror(errno), errno);
		free(group);
		return NULL;
	}

	while (contexts[i] != NULL) {
		if (nvme_rdma_poller_create(group, contexts[i])) {
			nvme_rdma_poll_group_free_pollers(group);
			free(group);
			rdma_free_devices(contexts);
			return NULL;
		}
		i++;
	}

	rdma_free_devices(contexts);
	STAILQ_INIT(&group->destroyed_qpairs);
	return &group->group;
}

struct nvme_rdma_qpair *
nvme_rdma_poll_group_get_qpair_by_id(struct nvme_rdma_poll_group *group, uint32_t qp_num)
{
	struct spdk_nvme_qpair *qpair;
	struct nvme_rdma_destroyed_qpair *rqpair_tracker;
	struct nvme_rdma_qpair *rqpair;

	STAILQ_FOREACH(qpair, &group->group.disconnected_qpairs, poll_group_stailq) {
		rqpair = nvme_rdma_qpair(qpair);
		if (rqpair->rdma_qp->qp->qp_num == qp_num) {
			return rqpair;
		}
	}

	STAILQ_FOREACH(qpair, &group->group.connected_qpairs, poll_group_stailq) {
		rqpair = nvme_rdma_qpair(qpair);
		if (rqpair->rdma_qp->qp->qp_num == qp_num) {
			return rqpair;
		}
	}

	STAILQ_FOREACH(rqpair_tracker, &group->destroyed_qpairs, link) {
		rqpair = rqpair_tracker->destroyed_qpair_tracker;
		if (rqpair->rdma_qp->qp->qp_num == qp_num) {
			return rqpair;
		}
	}

	return NULL;
}

static int
nvme_rdma_resize_cq(struct nvme_rdma_qpair *rqpair, struct nvme_rdma_poller *poller)
{
	int	current_num_wc, required_num_wc;

	required_num_wc = poller->required_num_wc + WC_PER_QPAIR(rqpair->num_entries);
	current_num_wc = poller->current_num_wc;
	if (current_num_wc < required_num_wc) {
		current_num_wc = spdk_max(current_num_wc * 2, required_num_wc);
	}

	if (poller->current_num_wc != current_num_wc) {
		SPDK_DEBUGLOG(nvme, "Resize RDMA CQ from %d to %d\n", poller->current_num_wc,
			      current_num_wc);
		if (ibv_resize_cq(poller->cq, current_num_wc)) {
			SPDK_ERRLOG("RDMA CQ resize failed: errno %d: %s\n", errno, spdk_strerror(errno));
			return -1;
		}

		poller->current_num_wc = current_num_wc;
	}

	poller->required_num_wc = required_num_wc;
	return 0;
}

static int
nvme_rdma_poll_group_connect_qpair(struct spdk_nvme_qpair *qpair)
{
	struct nvme_rdma_qpair		*rqpair = nvme_rdma_qpair(qpair);
	struct nvme_rdma_poll_group	*group = nvme_rdma_poll_group(qpair->poll_group);
	struct nvme_rdma_poller		*poller;

	assert(rqpair->cq == NULL);

	STAILQ_FOREACH(poller, &group->pollers, link) {
		if (poller->device == rqpair->cm_id->verbs) {
			if (nvme_rdma_resize_cq(rqpair, poller)) {
				return -EPROTO;
			}
			rqpair->cq = poller->cq;
			rqpair->poller = poller;
			break;
		}
	}

	if (rqpair->cq == NULL) {
		SPDK_ERRLOG("Unable to find a cq for qpair %p on poll group %p\n", qpair, qpair->poll_group);
		return -EINVAL;
	}

	return 0;
}

static int
nvme_rdma_poll_group_disconnect_qpair(struct spdk_nvme_qpair *qpair)
{
	struct nvme_rdma_qpair			*rqpair = nvme_rdma_qpair(qpair);
	struct nvme_rdma_poll_group		*group;
	struct nvme_rdma_destroyed_qpair	*destroyed_qpair;
	enum nvme_qpair_state			state;

	if (rqpair->poll_group_disconnect_in_progress) {
		return -EINPROGRESS;
	}

	rqpair->poll_group_disconnect_in_progress = true;
	state = nvme_qpair_get_state(qpair);
	group = nvme_rdma_poll_group(qpair->poll_group);
	rqpair->cq = NULL;

	/*
	 * We want to guard against an endless recursive loop while making
	 * sure the qpair is disconnected before we disconnect it from the qpair.
	 */
	if (state > NVME_QPAIR_DISCONNECTING && state != NVME_QPAIR_DESTROYING) {
		nvme_ctrlr_disconnect_qpair(qpair);
	}

	/*
	 * If this fails, the system is in serious trouble,
	 * just let the qpair get cleaned up immediately.
	 */
	destroyed_qpair = calloc(1, sizeof(*destroyed_qpair));
	if (destroyed_qpair == NULL) {
		return 0;
	}

	destroyed_qpair->destroyed_qpair_tracker = rqpair;
	destroyed_qpair->completed_cycles = 0;
	STAILQ_INSERT_TAIL(&group->destroyed_qpairs, destroyed_qpair, link);

	rqpair->defer_deletion_to_pg = true;

	rqpair->poll_group_disconnect_in_progress = false;
	return 0;
}

static int
nvme_rdma_poll_group_add(struct spdk_nvme_transport_poll_group *tgroup,
			 struct spdk_nvme_qpair *qpair)
{
	return 0;
}

static int
nvme_rdma_poll_group_remove(struct spdk_nvme_transport_poll_group *tgroup,
			    struct spdk_nvme_qpair *qpair)
{
	if (qpair->poll_group_tailq_head == &tgroup->connected_qpairs) {
		return nvme_poll_group_disconnect_qpair(qpair);
	}

	return 0;
}

static void
nvme_rdma_poll_group_delete_qpair(struct nvme_rdma_poll_group *group,
				  struct nvme_rdma_destroyed_qpair *qpair_tracker)
{
	struct nvme_rdma_qpair *rqpair = qpair_tracker->destroyed_qpair_tracker;

	rqpair->defer_deletion_to_pg = false;
	if (nvme_qpair_get_state(&rqpair->qpair) == NVME_QPAIR_DESTROYING) {
		nvme_rdma_ctrlr_delete_io_qpair(rqpair->qpair.ctrlr, &rqpair->qpair);
	}
	STAILQ_REMOVE(&group->destroyed_qpairs, qpair_tracker, nvme_rdma_destroyed_qpair, link);
	free(qpair_tracker);
}

static int64_t
nvme_rdma_poll_group_process_completions(struct spdk_nvme_transport_poll_group *tgroup,
		uint32_t completions_per_qpair, spdk_nvme_disconnected_qpair_cb disconnected_qpair_cb)
{
	struct spdk_nvme_qpair			*qpair, *tmp_qpair;
	struct nvme_rdma_destroyed_qpair	*qpair_tracker, *tmp_qpair_tracker;
	struct nvme_rdma_qpair			*rqpair;
	struct nvme_rdma_poll_group		*group;
	struct nvme_rdma_poller			*poller;
	int					num_qpairs = 0, batch_size, rc;
	int64_t					total_completions = 0;
	uint64_t				completions_allowed = 0;
	uint64_t				completions_per_poller = 0;
	uint64_t				poller_completions = 0;
	uint64_t				rdma_completions;


	if (completions_per_qpair == 0) {
		completions_per_qpair = MAX_COMPLETIONS_PER_POLL;
	}

	group = nvme_rdma_poll_group(tgroup);
	STAILQ_FOREACH_SAFE(qpair, &tgroup->disconnected_qpairs, poll_group_stailq, tmp_qpair) {
		disconnected_qpair_cb(qpair, tgroup->group->ctx);
	}

	STAILQ_FOREACH_SAFE(qpair, &tgroup->connected_qpairs, poll_group_stailq, tmp_qpair) {
		rqpair = nvme_rdma_qpair(qpair);
		rqpair->num_completions = 0;
		nvme_rdma_qpair_process_cm_event(rqpair);

		if (spdk_unlikely(qpair->transport_failure_reason != SPDK_NVME_QPAIR_FAILURE_NONE)) {
			nvme_rdma_fail_qpair(qpair, 0);
			disconnected_qpair_cb(qpair, tgroup->group->ctx);
			continue;
		}
		num_qpairs++;
	}

	completions_allowed = completions_per_qpair * num_qpairs;
	completions_per_poller = spdk_max(completions_allowed / group->num_pollers, 1);

	STAILQ_FOREACH(poller, &group->pollers, link) {
		poller_completions = 0;
		rdma_completions = 0;
		do {
			poller->stats.polls++;
			batch_size = spdk_min((completions_per_poller - poller_completions), MAX_COMPLETIONS_PER_POLL);
			rc = nvme_rdma_cq_process_completions(poller->cq, batch_size, group, NULL, &rdma_completions);
			if (rc <= 0) {
				if (rc == -ECANCELED) {
					return -EIO;
				} else if (rc == 0) {
					poller->stats.idle_polls++;
				}
				break;
			}

			poller_completions += rc;
		} while (poller_completions < completions_per_poller);
		total_completions += poller_completions;
		poller->stats.completions += rdma_completions;
	}

	STAILQ_FOREACH_SAFE(qpair, &tgroup->connected_qpairs, poll_group_stailq, tmp_qpair) {
		rqpair = nvme_rdma_qpair(qpair);
		if (spdk_unlikely(qpair->ctrlr->timeout_enabled)) {
			nvme_rdma_qpair_check_timeout(qpair);
		}

		nvme_rdma_qpair_submit_sends(rqpair);
		nvme_rdma_qpair_submit_recvs(rqpair);
		nvme_qpair_resubmit_requests(&rqpair->qpair, rqpair->num_completions);
	}

	/*
	 * Once a qpair is disconnected, we can still get flushed completions for those disconnected qpairs.
	 * For most pieces of hardware, those requests will complete immediately. However, there are certain
	 * cases where flushed requests will linger. Default is to destroy qpair after all completions are freed,
	 * but have a fallback for other cases where we don't get all of our completions back.
	 */
	STAILQ_FOREACH_SAFE(qpair_tracker, &group->destroyed_qpairs, link, tmp_qpair_tracker) {
		qpair_tracker->completed_cycles++;
		rqpair = qpair_tracker->destroyed_qpair_tracker;
		if ((rqpair->current_num_sends == 0 && rqpair->current_num_recvs == 0) ||
		    qpair_tracker->completed_cycles > NVME_RDMA_DESTROYED_QPAIR_EXPIRATION_CYCLES) {
			nvme_rdma_poll_group_delete_qpair(group, qpair_tracker);
		}
	}

	return total_completions;
}

static int
nvme_rdma_poll_group_destroy(struct spdk_nvme_transport_poll_group *tgroup)
{
	struct nvme_rdma_poll_group		*group = nvme_rdma_poll_group(tgroup);
	struct nvme_rdma_destroyed_qpair	*qpair_tracker, *tmp_qpair_tracker;
	struct nvme_rdma_qpair			*rqpair;

	if (!STAILQ_EMPTY(&tgroup->connected_qpairs) || !STAILQ_EMPTY(&tgroup->disconnected_qpairs)) {
		return -EBUSY;
	}

	STAILQ_FOREACH_SAFE(qpair_tracker, &group->destroyed_qpairs, link, tmp_qpair_tracker) {
		rqpair = qpair_tracker->destroyed_qpair_tracker;
		if (nvme_qpair_get_state(&rqpair->qpair) == NVME_QPAIR_DESTROYING) {
			rqpair->defer_deletion_to_pg = false;
			nvme_rdma_ctrlr_delete_io_qpair(rqpair->qpair.ctrlr, &rqpair->qpair);
		}

		STAILQ_REMOVE(&group->destroyed_qpairs, qpair_tracker, nvme_rdma_destroyed_qpair, link);
		free(qpair_tracker);
	}

	nvme_rdma_poll_group_free_pollers(group);
	free(group);

	return 0;
}

static int
nvme_rdma_poll_group_get_stats(struct spdk_nvme_transport_poll_group *tgroup,
			       struct spdk_nvme_transport_poll_group_stat **_stats)
{
	struct nvme_rdma_poll_group *group;
	struct spdk_nvme_transport_poll_group_stat *stats;
	struct spdk_nvme_rdma_device_stat *device_stat;
	struct nvme_rdma_poller *poller;
	uint32_t i = 0;

	if (tgroup == NULL || _stats == NULL) {
		SPDK_ERRLOG("Invalid stats or group pointer\n");
		return -EINVAL;
	}

	group = nvme_rdma_poll_group(tgroup);
	stats = calloc(1, sizeof(*stats));
	if (!stats) {
		SPDK_ERRLOG("Can't allocate memory for RDMA stats\n");
		return -ENOMEM;
	}
	stats->trtype = SPDK_NVME_TRANSPORT_RDMA;
	stats->rdma.num_devices = group->num_pollers;
	stats->rdma.device_stats = calloc(stats->rdma.num_devices, sizeof(*stats->rdma.device_stats));
	if (!stats->rdma.device_stats) {
		SPDK_ERRLOG("Can't allocate memory for RDMA device stats\n");
		free(stats);
		return -ENOMEM;
	}

	STAILQ_FOREACH(poller, &group->pollers, link) {
		device_stat = &stats->rdma.device_stats[i];
		device_stat->name = poller->device->device->name;
		device_stat->polls = poller->stats.polls;
		device_stat->idle_polls = poller->stats.idle_polls;
		device_stat->completions = poller->stats.completions;
		device_stat->queued_requests = poller->stats.queued_requests;
		device_stat->total_send_wrs = poller->stats.rdma_stats.send.num_submitted_wrs;
		device_stat->send_doorbell_updates = poller->stats.rdma_stats.send.doorbell_updates;
		device_stat->total_recv_wrs = poller->stats.rdma_stats.recv.num_submitted_wrs;
		device_stat->recv_doorbell_updates = poller->stats.rdma_stats.recv.doorbell_updates;
		i++;
	}

	*_stats = stats;

	return 0;
}

static void
nvme_rdma_poll_group_free_stats(struct spdk_nvme_transport_poll_group *tgroup,
				struct spdk_nvme_transport_poll_group_stat *stats)
{
	if (stats) {
		free(stats->rdma.device_stats);
	}
	free(stats);
}

void
spdk_nvme_rdma_init_hooks(struct spdk_nvme_rdma_hooks *hooks)
{
	g_nvme_hooks = *hooks;
}

const struct spdk_nvme_transport_ops rdma_ops = {
	.name = "RDMA",
	.type = SPDK_NVME_TRANSPORT_RDMA,
	.ctrlr_construct = nvme_rdma_ctrlr_construct,
	.ctrlr_scan = nvme_fabric_ctrlr_scan,
	.ctrlr_destruct = nvme_rdma_ctrlr_destruct,
	.ctrlr_enable = nvme_rdma_ctrlr_enable,

	.ctrlr_set_reg_4 = nvme_fabric_ctrlr_set_reg_4,
	.ctrlr_set_reg_8 = nvme_fabric_ctrlr_set_reg_8,
	.ctrlr_get_reg_4 = nvme_fabric_ctrlr_get_reg_4,
	.ctrlr_get_reg_8 = nvme_fabric_ctrlr_get_reg_8,

	.ctrlr_get_max_xfer_size = nvme_rdma_ctrlr_get_max_xfer_size,
	.ctrlr_get_max_sges = nvme_rdma_ctrlr_get_max_sges,

	.ctrlr_create_io_qpair = nvme_rdma_ctrlr_create_io_qpair,
	.ctrlr_delete_io_qpair = nvme_rdma_ctrlr_delete_io_qpair,
	.ctrlr_connect_qpair = nvme_rdma_ctrlr_connect_qpair,
	.ctrlr_disconnect_qpair = nvme_rdma_ctrlr_disconnect_qpair,

	.qpair_abort_reqs = nvme_rdma_qpair_abort_reqs,
	.qpair_reset = nvme_rdma_qpair_reset,
	.qpair_submit_request = nvme_rdma_qpair_submit_request,
	.qpair_process_completions = nvme_rdma_qpair_process_completions,
	.qpair_iterate_requests = nvme_rdma_qpair_iterate_requests,
	.admin_qpair_abort_aers = nvme_rdma_admin_qpair_abort_aers,

	.poll_group_create = nvme_rdma_poll_group_create,
	.poll_group_connect_qpair = nvme_rdma_poll_group_connect_qpair,
	.poll_group_disconnect_qpair = nvme_rdma_poll_group_disconnect_qpair,
	.poll_group_add = nvme_rdma_poll_group_add,
	.poll_group_remove = nvme_rdma_poll_group_remove,
	.poll_group_process_completions = nvme_rdma_poll_group_process_completions,
	.poll_group_destroy = nvme_rdma_poll_group_destroy,
	.poll_group_get_stats = nvme_rdma_poll_group_get_stats,
	.poll_group_free_stats = nvme_rdma_poll_group_free_stats,
};

SPDK_NVME_TRANSPORT_REGISTER(rdma, &rdma_ops);
