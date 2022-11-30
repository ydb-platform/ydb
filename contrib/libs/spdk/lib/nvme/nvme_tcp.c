#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2020 Mellanox Technologies LTD. All rights reserved.
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
 * NVMe/TCP transport
 */

#include "nvme_internal.h"

#include "spdk/endian.h"
#include "spdk/likely.h"
#include "spdk/string.h"
#include "spdk/stdinc.h"
#include "spdk/crc32.h"
#include "spdk/endian.h"
#include "spdk/assert.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/trace.h"
#include "spdk/util.h"

#include "spdk_internal/nvme_tcp.h"

#define NVME_TCP_RW_BUFFER_SIZE 131072
#define NVME_TCP_TIME_OUT_IN_SECONDS 2

#define NVME_TCP_HPDA_DEFAULT			0
#define NVME_TCP_MAX_R2T_DEFAULT		1
#define NVME_TCP_PDU_H2C_MIN_DATA_SIZE		4096

/* NVMe TCP transport extensions for spdk_nvme_ctrlr */
struct nvme_tcp_ctrlr {
	struct spdk_nvme_ctrlr			ctrlr;
};

struct nvme_tcp_poll_group {
	struct spdk_nvme_transport_poll_group group;
	struct spdk_sock_group *sock_group;
	uint32_t completions_per_qpair;
	int64_t num_completions;

	TAILQ_HEAD(, nvme_tcp_qpair) needs_poll;
};

/* NVMe TCP qpair extensions for spdk_nvme_qpair */
struct nvme_tcp_qpair {
	struct spdk_nvme_qpair			qpair;
	struct spdk_sock			*sock;

	TAILQ_HEAD(, nvme_tcp_req)		free_reqs;
	TAILQ_HEAD(, nvme_tcp_req)		outstanding_reqs;

	TAILQ_HEAD(, nvme_tcp_pdu)		send_queue;
	struct nvme_tcp_pdu			recv_pdu;
	struct nvme_tcp_pdu			*send_pdu; /* only for error pdu and init pdu */
	struct nvme_tcp_pdu			*send_pdus; /* Used by tcp_reqs */
	enum nvme_tcp_pdu_recv_state		recv_state;

	struct nvme_tcp_req			*tcp_reqs;

	uint16_t				num_entries;
	uint16_t				async_complete;

	struct {
		uint16_t host_hdgst_enable: 1;
		uint16_t host_ddgst_enable: 1;
		uint16_t icreq_send_ack: 1;
		uint16_t reserved: 13;
	} flags;

	/** Specifies the maximum number of PDU-Data bytes per H2C Data Transfer PDU */
	uint32_t				maxh2cdata;

	uint32_t				maxr2t;

	/* 0 based value, which is used to guide the padding */
	uint8_t					cpda;

	enum nvme_tcp_qpair_state		state;

	TAILQ_ENTRY(nvme_tcp_qpair)		link;
	bool					needs_poll;
};

enum nvme_tcp_req_state {
	NVME_TCP_REQ_FREE,
	NVME_TCP_REQ_ACTIVE,
	NVME_TCP_REQ_ACTIVE_R2T,
};

struct nvme_tcp_req {
	struct nvme_request			*req;
	enum nvme_tcp_req_state			state;
	uint16_t				cid;
	uint16_t				ttag;
	uint32_t				datao;
	uint32_t				r2tl_remain;
	uint32_t				active_r2ts;
	/* Used to hold a value received from subsequent R2T while we are still
	 * waiting for H2C complete */
	uint16_t				ttag_r2t_next;
	bool					in_capsule_data;
	/* It is used to track whether the req can be safely freed */
	union {
		uint8_t raw;
		struct {
			/* The last send operation completed - kernel released send buffer */
			uint8_t				send_ack : 1;
			/* Data transfer completed - target send resp or last data bit */
			uint8_t				data_recv : 1;
			/* tcp_req is waiting for completion of the previous send operation (buffer reclaim notification
			 * from kernel) to send H2C */
			uint8_t				h2c_send_waiting_ack : 1;
			/* tcp_req received subsequent r2t while it is still waiting for send_ack.
			 * Rare case, actual when dealing with target that can send several R2T requests.
			 * SPDK TCP target sends 1 R2T for the whole data buffer */
			uint8_t				r2t_waiting_h2c_complete : 1;
			uint8_t				reserved : 4;
		} bits;
	} ordering;
	struct nvme_tcp_pdu			*send_pdu;
	struct iovec				iov[NVME_TCP_MAX_SGL_DESCRIPTORS];
	uint32_t				iovcnt;
	/* Used to hold a value received from subsequent R2T while we are still
	 * waiting for H2C ack */
	uint32_t				r2tl_remain_next;
	struct nvme_tcp_qpair			*tqpair;
	TAILQ_ENTRY(nvme_tcp_req)		link;
	struct spdk_nvme_cpl			rsp;
};

static void nvme_tcp_send_h2c_data(struct nvme_tcp_req *tcp_req);
static int64_t nvme_tcp_poll_group_process_completions(struct spdk_nvme_transport_poll_group
		*tgroup, uint32_t completions_per_qpair, spdk_nvme_disconnected_qpair_cb disconnected_qpair_cb);
static void nvme_tcp_icresp_handle(struct nvme_tcp_qpair *tqpair, struct nvme_tcp_pdu *pdu);

static inline struct nvme_tcp_qpair *
nvme_tcp_qpair(struct spdk_nvme_qpair *qpair)
{
	assert(qpair->trtype == SPDK_NVME_TRANSPORT_TCP);
	return SPDK_CONTAINEROF(qpair, struct nvme_tcp_qpair, qpair);
}

static inline struct nvme_tcp_poll_group *
nvme_tcp_poll_group(struct spdk_nvme_transport_poll_group *group)
{
	return SPDK_CONTAINEROF(group, struct nvme_tcp_poll_group, group);
}

static inline struct nvme_tcp_ctrlr *
nvme_tcp_ctrlr(struct spdk_nvme_ctrlr *ctrlr)
{
	assert(ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_TCP);
	return SPDK_CONTAINEROF(ctrlr, struct nvme_tcp_ctrlr, ctrlr);
}

static struct nvme_tcp_req *
nvme_tcp_req_get(struct nvme_tcp_qpair *tqpair)
{
	struct nvme_tcp_req *tcp_req;

	tcp_req = TAILQ_FIRST(&tqpair->free_reqs);
	if (!tcp_req) {
		return NULL;
	}

	assert(tcp_req->state == NVME_TCP_REQ_FREE);
	tcp_req->state = NVME_TCP_REQ_ACTIVE;
	TAILQ_REMOVE(&tqpair->free_reqs, tcp_req, link);
	tcp_req->datao = 0;
	tcp_req->req = NULL;
	tcp_req->in_capsule_data = false;
	tcp_req->r2tl_remain = 0;
	tcp_req->r2tl_remain_next = 0;
	tcp_req->active_r2ts = 0;
	tcp_req->iovcnt = 0;
	tcp_req->ordering.raw = 0;
	memset(tcp_req->send_pdu, 0, sizeof(struct nvme_tcp_pdu));
	memset(&tcp_req->rsp, 0, sizeof(struct spdk_nvme_cpl));
	TAILQ_INSERT_TAIL(&tqpair->outstanding_reqs, tcp_req, link);

	return tcp_req;
}

static void
nvme_tcp_req_put(struct nvme_tcp_qpair *tqpair, struct nvme_tcp_req *tcp_req)
{
	assert(tcp_req->state != NVME_TCP_REQ_FREE);
	tcp_req->state = NVME_TCP_REQ_FREE;
	TAILQ_INSERT_HEAD(&tqpair->free_reqs, tcp_req, link);
}

static int
nvme_tcp_parse_addr(struct sockaddr_storage *sa, int family, const char *addr, const char *service)
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
		ret = -EINVAL;
	} else {
		memcpy(sa, res->ai_addr, res->ai_addrlen);
	}

	freeaddrinfo(res);
	return ret;
}

static void
nvme_tcp_free_reqs(struct nvme_tcp_qpair *tqpair)
{
	free(tqpair->tcp_reqs);
	tqpair->tcp_reqs = NULL;

	spdk_free(tqpair->send_pdus);
	tqpair->send_pdus = NULL;
}

static int
nvme_tcp_alloc_reqs(struct nvme_tcp_qpair *tqpair)
{
	uint16_t i;
	struct nvme_tcp_req	*tcp_req;

	tqpair->tcp_reqs = calloc(tqpair->num_entries, sizeof(struct nvme_tcp_req));
	if (tqpair->tcp_reqs == NULL) {
		SPDK_ERRLOG("Failed to allocate tcp_reqs on tqpair=%p\n", tqpair);
		goto fail;
	}

	/* Add additional one member for the send_pdu owned by the tqpair */
	tqpair->send_pdus = spdk_zmalloc((tqpair->num_entries + 1) * sizeof(struct nvme_tcp_pdu),
					 0x1000, NULL,
					 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

	if (tqpair->send_pdus == NULL) {
		SPDK_ERRLOG("Failed to allocate send_pdus on tqpair=%p\n", tqpair);
		goto fail;
	}

	TAILQ_INIT(&tqpair->send_queue);
	TAILQ_INIT(&tqpair->free_reqs);
	TAILQ_INIT(&tqpair->outstanding_reqs);
	for (i = 0; i < tqpair->num_entries; i++) {
		tcp_req = &tqpair->tcp_reqs[i];
		tcp_req->cid = i;
		tcp_req->tqpair = tqpair;
		tcp_req->send_pdu = &tqpair->send_pdus[i];
		TAILQ_INSERT_TAIL(&tqpair->free_reqs, tcp_req, link);
	}

	tqpair->send_pdu = &tqpair->send_pdus[i];

	return 0;
fail:
	nvme_tcp_free_reqs(tqpair);
	return -ENOMEM;
}

static void
nvme_tcp_ctrlr_disconnect_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);
	struct nvme_tcp_pdu *pdu;
	int rc;
	struct nvme_tcp_poll_group *group;

	if (tqpair->needs_poll) {
		group = nvme_tcp_poll_group(qpair->poll_group);
		TAILQ_REMOVE(&group->needs_poll, tqpair, link);
		tqpair->needs_poll = false;
	}

	rc = spdk_sock_close(&tqpair->sock);

	if (tqpair->sock != NULL) {
		SPDK_ERRLOG("tqpair=%p, errno=%d, rc=%d\n", tqpair, errno, rc);
		/* Set it to NULL manually */
		tqpair->sock = NULL;
	}

	/* clear the send_queue */
	while (!TAILQ_EMPTY(&tqpair->send_queue)) {
		pdu = TAILQ_FIRST(&tqpair->send_queue);
		/* Remove the pdu from the send_queue to prevent the wrong sending out
		 * in the next round connection
		 */
		TAILQ_REMOVE(&tqpair->send_queue, pdu, tailq);
	}
}

static void nvme_tcp_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr);

static int
nvme_tcp_ctrlr_delete_io_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	struct nvme_tcp_qpair *tqpair;

	assert(qpair != NULL);
	nvme_transport_ctrlr_disconnect_qpair(ctrlr, qpair);
	nvme_tcp_qpair_abort_reqs(qpair, 1);
	nvme_qpair_deinit(qpair);
	tqpair = nvme_tcp_qpair(qpair);
	nvme_tcp_free_reqs(tqpair);
	free(tqpair);

	return 0;
}

static int
nvme_tcp_ctrlr_enable(struct spdk_nvme_ctrlr *ctrlr)
{
	return 0;
}

static int
nvme_tcp_ctrlr_destruct(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_tcp_ctrlr *tctrlr = nvme_tcp_ctrlr(ctrlr);

	if (ctrlr->adminq) {
		nvme_tcp_ctrlr_delete_io_qpair(ctrlr, ctrlr->adminq);
	}

	nvme_ctrlr_destruct_finish(ctrlr);

	free(tctrlr);

	return 0;
}

static void
_pdu_write_done(void *cb_arg, int err)
{
	struct nvme_tcp_pdu *pdu = cb_arg;
	struct nvme_tcp_qpair *tqpair = pdu->qpair;
	struct nvme_tcp_poll_group *pgroup = nvme_tcp_poll_group(tqpair->qpair.poll_group);

	/* If there are queued requests, we assume they are queued because they are waiting
	 * for resources to be released. Those resources are almost certainly released in
	 * response to a PDU completing here. However, to attempt to make forward progress
	 * the qpair needs to be polled and we can't rely on another network event to make
	 * that happen. Add it to a list of qpairs to poll regardless of network activity
	 * here. */
	if (pgroup && !STAILQ_EMPTY(&tqpair->qpair.queued_req) && !tqpair->needs_poll) {
		TAILQ_INSERT_TAIL(&pgroup->needs_poll, tqpair, link);
		tqpair->needs_poll = true;
	}

	TAILQ_REMOVE(&tqpair->send_queue, pdu, tailq);

	if (err != 0) {
		nvme_transport_ctrlr_disconnect_qpair(tqpair->qpair.ctrlr, &tqpair->qpair);
		return;
	}

	assert(pdu->cb_fn != NULL);
	pdu->cb_fn(pdu->cb_arg);
}

static void
_tcp_write_pdu(struct nvme_tcp_pdu *pdu)
{
	uint32_t mapped_length = 0;
	struct nvme_tcp_qpair *tqpair = pdu->qpair;

	pdu->sock_req.iovcnt = nvme_tcp_build_iovs(pdu->iov, NVME_TCP_MAX_SGL_DESCRIPTORS, pdu,
			       (bool)tqpair->flags.host_hdgst_enable, (bool)tqpair->flags.host_ddgst_enable,
			       &mapped_length);
	pdu->sock_req.cb_fn = _pdu_write_done;
	pdu->sock_req.cb_arg = pdu;
	TAILQ_INSERT_TAIL(&tqpair->send_queue, pdu, tailq);
	spdk_sock_writev_async(tqpair->sock, &pdu->sock_req);
}

static void
data_crc32_accel_done(void *cb_arg, int status)
{
	struct nvme_tcp_pdu *pdu = cb_arg;

	if (spdk_unlikely(status)) {
		SPDK_ERRLOG("Failed to compute the data digest for pdu =%p\n", pdu);
		_pdu_write_done(pdu, status);
		return;
	}

	pdu->data_digest_crc32 ^= SPDK_CRC32C_XOR;
	MAKE_DIGEST_WORD(pdu->data_digest, pdu->data_digest_crc32);

	_tcp_write_pdu(pdu);
}

static void
pdu_data_crc32_compute(struct nvme_tcp_pdu *pdu)
{
	struct nvme_tcp_qpair *tqpair = pdu->qpair;
	uint32_t crc32c;
	struct nvme_tcp_poll_group *tgroup = nvme_tcp_poll_group(tqpair->qpair.poll_group);

	/* Data Digest */
	if (pdu->data_len > 0 && g_nvme_tcp_ddgst[pdu->hdr.common.pdu_type] &&
	    tqpair->flags.host_ddgst_enable) {
		/* Only suport this limitated case for the first step */
		if (tgroup != NULL && tgroup->group.group->accel_fn_table.submit_accel_crc32c &&
		    spdk_likely(!pdu->dif_ctx && (pdu->data_len % SPDK_NVME_TCP_DIGEST_ALIGNMENT == 0))) {
			tgroup->group.group->accel_fn_table.submit_accel_crc32c(tgroup->group.group->ctx,
					&pdu->data_digest_crc32, pdu->data_iov,
					pdu->data_iovcnt, 0, data_crc32_accel_done, pdu);
			return;
		}

		crc32c = nvme_tcp_pdu_calc_data_digest(pdu);
		MAKE_DIGEST_WORD(pdu->data_digest, crc32c);
	}

	_tcp_write_pdu(pdu);
}

static void
header_crc32_accel_done(void *cb_arg, int status)
{
	struct nvme_tcp_pdu *pdu = cb_arg;

	pdu->header_digest_crc32 ^= SPDK_CRC32C_XOR;
	MAKE_DIGEST_WORD((uint8_t *)pdu->hdr.raw + pdu->hdr.common.hlen, pdu->header_digest_crc32);
	if (spdk_unlikely(status)) {
		SPDK_ERRLOG("Failed to compute header digest on pdu=%p\n", pdu);
		_pdu_write_done(pdu, status);
		return;
	}

	pdu_data_crc32_compute(pdu);
}

static int
nvme_tcp_qpair_write_pdu(struct nvme_tcp_qpair *tqpair,
			 struct nvme_tcp_pdu *pdu,
			 nvme_tcp_qpair_xfer_complete_cb cb_fn,
			 void *cb_arg)
{
	int hlen;
	uint32_t crc32c;
	struct nvme_tcp_poll_group *tgroup = nvme_tcp_poll_group(tqpair->qpair.poll_group);

	hlen = pdu->hdr.common.hlen;

	pdu->cb_fn = cb_fn;
	pdu->cb_arg = cb_arg;
	pdu->qpair = tqpair;

	/* Header Digest */
	if (g_nvme_tcp_hdgst[pdu->hdr.common.pdu_type] && tqpair->flags.host_hdgst_enable) {
		if (tgroup != NULL && tgroup->group.group->accel_fn_table.submit_accel_crc32c) {
			pdu->iov[0].iov_base = &pdu->hdr.raw;
			pdu->iov[0].iov_len = hlen;
			tgroup->group.group->accel_fn_table.submit_accel_crc32c(tgroup->group.group->ctx,
					&pdu->header_digest_crc32,
					pdu->iov, 1, 0, header_crc32_accel_done, pdu);
			return 0;
		} else {
			crc32c = nvme_tcp_pdu_calc_header_digest(pdu);
			MAKE_DIGEST_WORD((uint8_t *)pdu->hdr.raw + hlen, crc32c);
		}
	}

	pdu_data_crc32_compute(pdu);

	return 0;
}

/*
 * Build SGL describing contiguous payload buffer.
 */
static int
nvme_tcp_build_contig_request(struct nvme_tcp_qpair *tqpair, struct nvme_tcp_req *tcp_req)
{
	struct nvme_request *req = tcp_req->req;

	tcp_req->iov[0].iov_base = req->payload.contig_or_cb_arg + req->payload_offset;
	tcp_req->iov[0].iov_len = req->payload_size;
	tcp_req->iovcnt = 1;

	SPDK_DEBUGLOG(nvme, "enter\n");

	assert(nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_CONTIG);

	return 0;
}

/*
 * Build SGL describing scattered payload buffer.
 */
static int
nvme_tcp_build_sgl_request(struct nvme_tcp_qpair *tqpair, struct nvme_tcp_req *tcp_req)
{
	int rc;
	uint32_t length, remaining_size, iovcnt = 0, max_num_sgl;
	struct nvme_request *req = tcp_req->req;

	SPDK_DEBUGLOG(nvme, "enter\n");

	assert(req->payload_size != 0);
	assert(nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_SGL);
	assert(req->payload.reset_sgl_fn != NULL);
	assert(req->payload.next_sge_fn != NULL);
	req->payload.reset_sgl_fn(req->payload.contig_or_cb_arg, req->payload_offset);

	max_num_sgl = spdk_min(req->qpair->ctrlr->max_sges, NVME_TCP_MAX_SGL_DESCRIPTORS);
	remaining_size = req->payload_size;

	do {
		rc = req->payload.next_sge_fn(req->payload.contig_or_cb_arg, &tcp_req->iov[iovcnt].iov_base,
					      &length);
		if (rc) {
			return -1;
		}

		length = spdk_min(length, remaining_size);
		tcp_req->iov[iovcnt].iov_len = length;
		remaining_size -= length;
		iovcnt++;
	} while (remaining_size > 0 && iovcnt < max_num_sgl);


	/* Should be impossible if we did our sgl checks properly up the stack, but do a sanity check here. */
	if (remaining_size > 0) {
		SPDK_ERRLOG("Failed to construct tcp_req=%p, and the iovcnt=%u, remaining_size=%u\n",
			    tcp_req, iovcnt, remaining_size);
		return -1;
	}

	tcp_req->iovcnt = iovcnt;

	return 0;
}

static int
nvme_tcp_req_init(struct nvme_tcp_qpair *tqpair, struct nvme_request *req,
		  struct nvme_tcp_req *tcp_req)
{
	struct spdk_nvme_ctrlr *ctrlr = tqpair->qpair.ctrlr;
	int rc = 0;
	enum spdk_nvme_data_transfer xfer;
	uint32_t max_incapsule_data_size;

	tcp_req->req = req;
	req->cmd.cid = tcp_req->cid;
	req->cmd.psdt = SPDK_NVME_PSDT_SGL_MPTR_CONTIG;
	req->cmd.dptr.sgl1.unkeyed.type = SPDK_NVME_SGL_TYPE_TRANSPORT_DATA_BLOCK;
	req->cmd.dptr.sgl1.unkeyed.subtype = SPDK_NVME_SGL_SUBTYPE_TRANSPORT;
	req->cmd.dptr.sgl1.unkeyed.length = req->payload_size;

	if (nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_CONTIG) {
		rc = nvme_tcp_build_contig_request(tqpair, tcp_req);
	} else if (nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_SGL) {
		rc = nvme_tcp_build_sgl_request(tqpair, tcp_req);
	} else {
		rc = -1;
	}

	if (rc) {
		return rc;
	}

	if (req->cmd.opc == SPDK_NVME_OPC_FABRIC) {
		struct spdk_nvmf_capsule_cmd *nvmf_cmd = (struct spdk_nvmf_capsule_cmd *)&req->cmd;

		xfer = spdk_nvme_opc_get_data_transfer(nvmf_cmd->fctype);
	} else {
		xfer = spdk_nvme_opc_get_data_transfer(req->cmd.opc);
	}
	if (xfer == SPDK_NVME_DATA_HOST_TO_CONTROLLER) {
		max_incapsule_data_size = ctrlr->ioccsz_bytes;
		if ((req->cmd.opc == SPDK_NVME_OPC_FABRIC) || nvme_qpair_is_admin_queue(&tqpair->qpair)) {
			max_incapsule_data_size = SPDK_NVME_TCP_IN_CAPSULE_DATA_MAX_SIZE;
		}

		if (req->payload_size <= max_incapsule_data_size) {
			req->cmd.dptr.sgl1.unkeyed.type = SPDK_NVME_SGL_TYPE_DATA_BLOCK;
			req->cmd.dptr.sgl1.unkeyed.subtype = SPDK_NVME_SGL_SUBTYPE_OFFSET;
			req->cmd.dptr.sgl1.address = 0;
			tcp_req->in_capsule_data = true;
		}
	}

	return 0;
}

static inline bool
nvme_tcp_req_complete_safe(struct nvme_tcp_req *tcp_req)
{
	struct spdk_nvme_cpl	cpl;
	spdk_nvme_cmd_cb	user_cb;
	void			*user_cb_arg;
	struct spdk_nvme_qpair	*qpair;
	struct nvme_request	*req;

	if (!(tcp_req->ordering.bits.send_ack && tcp_req->ordering.bits.data_recv)) {
		return false;
	}

	assert(tcp_req->state == NVME_TCP_REQ_ACTIVE);
	assert(tcp_req->tqpair != NULL);
	assert(tcp_req->req != NULL);

	SPDK_DEBUGLOG(nvme, "complete tcp_req(%p) on tqpair=%p\n", tcp_req, tcp_req->tqpair);

	if (!tcp_req->tqpair->qpair.in_completion_context) {
		tcp_req->tqpair->async_complete++;
	}

	/* Cache arguments to be passed to nvme_complete_request since tcp_req can be zeroed when released */
	memcpy(&cpl, &tcp_req->rsp, sizeof(cpl));
	user_cb		= tcp_req->req->cb_fn;
	user_cb_arg	= tcp_req->req->cb_arg;
	qpair		= tcp_req->req->qpair;
	req		= tcp_req->req;

	TAILQ_REMOVE(&tcp_req->tqpair->outstanding_reqs, tcp_req, link);
	nvme_tcp_req_put(tcp_req->tqpair, tcp_req);
	nvme_free_request(tcp_req->req);
	nvme_complete_request(user_cb, user_cb_arg, qpair, req, &cpl);

	return true;
}

static void
nvme_tcp_qpair_cmd_send_complete(void *cb_arg)
{
	struct nvme_tcp_req *tcp_req = cb_arg;

	SPDK_DEBUGLOG(nvme, "tcp req %p, cid %u, qid %u\n", tcp_req, tcp_req->cid,
		      tcp_req->tqpair->qpair.id);
	tcp_req->ordering.bits.send_ack = 1;
	/* Handle the r2t case */
	if (spdk_unlikely(tcp_req->ordering.bits.h2c_send_waiting_ack)) {
		SPDK_DEBUGLOG(nvme, "tcp req %p, send H2C data\n", tcp_req);
		nvme_tcp_send_h2c_data(tcp_req);
	} else {
		nvme_tcp_req_complete_safe(tcp_req);
	}
}

static int
nvme_tcp_qpair_capsule_cmd_send(struct nvme_tcp_qpair *tqpair,
				struct nvme_tcp_req *tcp_req)
{
	struct nvme_tcp_pdu *pdu;
	struct spdk_nvme_tcp_cmd *capsule_cmd;
	uint32_t plen = 0, alignment;
	uint8_t pdo;

	SPDK_DEBUGLOG(nvme, "enter\n");
	pdu = tcp_req->send_pdu;

	capsule_cmd = &pdu->hdr.capsule_cmd;
	capsule_cmd->common.pdu_type = SPDK_NVME_TCP_PDU_TYPE_CAPSULE_CMD;
	plen = capsule_cmd->common.hlen = sizeof(*capsule_cmd);
	capsule_cmd->ccsqe = tcp_req->req->cmd;

	SPDK_DEBUGLOG(nvme, "capsule_cmd cid=%u on tqpair(%p)\n", tcp_req->req->cmd.cid, tqpair);

	if (tqpair->flags.host_hdgst_enable) {
		SPDK_DEBUGLOG(nvme, "Header digest is enabled for capsule command on tcp_req=%p\n",
			      tcp_req);
		capsule_cmd->common.flags |= SPDK_NVME_TCP_CH_FLAGS_HDGSTF;
		plen += SPDK_NVME_TCP_DIGEST_LEN;
	}

	if ((tcp_req->req->payload_size == 0) || !tcp_req->in_capsule_data) {
		goto end;
	}

	pdo = plen;
	pdu->padding_len = 0;
	if (tqpair->cpda) {
		alignment = (tqpair->cpda + 1) << 2;
		if (alignment > plen) {
			pdu->padding_len = alignment - plen;
			pdo = alignment;
			plen = alignment;
		}
	}

	capsule_cmd->common.pdo = pdo;
	plen += tcp_req->req->payload_size;
	if (tqpair->flags.host_ddgst_enable) {
		capsule_cmd->common.flags |= SPDK_NVME_TCP_CH_FLAGS_DDGSTF;
		plen += SPDK_NVME_TCP_DIGEST_LEN;
	}

	tcp_req->datao = 0;
	nvme_tcp_pdu_set_data_buf(pdu, tcp_req->iov, tcp_req->iovcnt,
				  0, tcp_req->req->payload_size);
end:
	capsule_cmd->common.plen = plen;
	return nvme_tcp_qpair_write_pdu(tqpair, pdu, nvme_tcp_qpair_cmd_send_complete, tcp_req);

}

static int
nvme_tcp_qpair_submit_request(struct spdk_nvme_qpair *qpair,
			      struct nvme_request *req)
{
	struct nvme_tcp_qpair *tqpair;
	struct nvme_tcp_req *tcp_req;

	tqpair = nvme_tcp_qpair(qpair);
	assert(tqpair != NULL);
	assert(req != NULL);

	tcp_req = nvme_tcp_req_get(tqpair);
	if (!tcp_req) {
		/* Inform the upper layer to try again later. */
		return -EAGAIN;
	}

	if (nvme_tcp_req_init(tqpair, req, tcp_req)) {
		SPDK_ERRLOG("nvme_tcp_req_init() failed\n");
		TAILQ_REMOVE(&tcp_req->tqpair->outstanding_reqs, tcp_req, link);
		nvme_tcp_req_put(tqpair, tcp_req);
		return -1;
	}

	return nvme_tcp_qpair_capsule_cmd_send(tqpair, tcp_req);
}

static int
nvme_tcp_qpair_reset(struct spdk_nvme_qpair *qpair)
{
	return 0;
}

static void
nvme_tcp_req_complete(struct nvme_tcp_req *tcp_req,
		      struct spdk_nvme_cpl *rsp)
{
	struct nvme_request *req;

	assert(tcp_req->req != NULL);
	req = tcp_req->req;

	TAILQ_REMOVE(&tcp_req->tqpair->outstanding_reqs, tcp_req, link);
	nvme_complete_request(req->cb_fn, req->cb_arg, req->qpair, req, rsp);
	nvme_free_request(req);
}

static void
nvme_tcp_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr)
{
	struct nvme_tcp_req *tcp_req, *tmp;
	struct spdk_nvme_cpl cpl;
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);

	cpl.status.sc = SPDK_NVME_SC_ABORTED_SQ_DELETION;
	cpl.status.sct = SPDK_NVME_SCT_GENERIC;
	cpl.status.dnr = dnr;

	TAILQ_FOREACH_SAFE(tcp_req, &tqpair->outstanding_reqs, link, tmp) {
		nvme_tcp_req_complete(tcp_req, &cpl);
		nvme_tcp_req_put(tqpair, tcp_req);
	}
}

static void
nvme_tcp_qpair_set_recv_state(struct nvme_tcp_qpair *tqpair,
			      enum nvme_tcp_pdu_recv_state state)
{
	if (tqpair->recv_state == state) {
		SPDK_ERRLOG("The recv state of tqpair=%p is same with the state(%d) to be set\n",
			    tqpair, state);
		return;
	}

	tqpair->recv_state = state;
	switch (state) {
	case NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY:
	case NVME_TCP_PDU_RECV_STATE_ERROR:
		memset(&tqpair->recv_pdu, 0, sizeof(struct nvme_tcp_pdu));
		break;
	case NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_CH:
	case NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PSH:
	case NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PAYLOAD:
	default:
		break;
	}
}

static void
nvme_tcp_qpair_send_h2c_term_req_complete(void *cb_arg)
{
	struct nvme_tcp_qpair *tqpair = cb_arg;

	tqpair->state = NVME_TCP_QPAIR_STATE_EXITING;
}

static void
nvme_tcp_qpair_send_h2c_term_req(struct nvme_tcp_qpair *tqpair, struct nvme_tcp_pdu *pdu,
				 enum spdk_nvme_tcp_term_req_fes fes, uint32_t error_offset)
{
	struct nvme_tcp_pdu *rsp_pdu;
	struct spdk_nvme_tcp_term_req_hdr *h2c_term_req;
	uint32_t h2c_term_req_hdr_len = sizeof(*h2c_term_req);
	uint8_t copy_len;

	rsp_pdu = tqpair->send_pdu;
	memset(rsp_pdu, 0, sizeof(*rsp_pdu));
	h2c_term_req = &rsp_pdu->hdr.term_req;
	h2c_term_req->common.pdu_type = SPDK_NVME_TCP_PDU_TYPE_H2C_TERM_REQ;
	h2c_term_req->common.hlen = h2c_term_req_hdr_len;

	if ((fes == SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD) ||
	    (fes == SPDK_NVME_TCP_TERM_REQ_FES_INVALID_DATA_UNSUPPORTED_PARAMETER)) {
		DSET32(&h2c_term_req->fei, error_offset);
	}

	copy_len = pdu->hdr.common.hlen;
	if (copy_len > SPDK_NVME_TCP_TERM_REQ_ERROR_DATA_MAX_SIZE) {
		copy_len = SPDK_NVME_TCP_TERM_REQ_ERROR_DATA_MAX_SIZE;
	}

	/* Copy the error info into the buffer */
	memcpy((uint8_t *)rsp_pdu->hdr.raw + h2c_term_req_hdr_len, pdu->hdr.raw, copy_len);
	nvme_tcp_pdu_set_data(rsp_pdu, (uint8_t *)rsp_pdu->hdr.raw + h2c_term_req_hdr_len, copy_len);

	/* Contain the header len of the wrong received pdu */
	h2c_term_req->common.plen = h2c_term_req->common.hlen + copy_len;
	nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_ERROR);
	nvme_tcp_qpair_write_pdu(tqpair, rsp_pdu, nvme_tcp_qpair_send_h2c_term_req_complete, tqpair);

}

static void
nvme_tcp_pdu_ch_handle(struct nvme_tcp_qpair *tqpair)
{
	struct nvme_tcp_pdu *pdu;
	uint32_t error_offset = 0;
	enum spdk_nvme_tcp_term_req_fes fes;
	uint32_t expected_hlen, hd_len = 0;
	bool plen_error = false;

	pdu = &tqpair->recv_pdu;

	SPDK_DEBUGLOG(nvme, "pdu type = %d\n", pdu->hdr.common.pdu_type);
	if (pdu->hdr.common.pdu_type == SPDK_NVME_TCP_PDU_TYPE_IC_RESP) {
		if (tqpair->state != NVME_TCP_QPAIR_STATE_INVALID) {
			SPDK_ERRLOG("Already received IC_RESP PDU, and we should reject this pdu=%p\n", pdu);
			fes = SPDK_NVME_TCP_TERM_REQ_FES_PDU_SEQUENCE_ERROR;
			goto err;
		}
		expected_hlen = sizeof(struct spdk_nvme_tcp_ic_resp);
		if (pdu->hdr.common.plen != expected_hlen) {
			plen_error = true;
		}
	} else {
		if (tqpair->state != NVME_TCP_QPAIR_STATE_RUNNING) {
			SPDK_ERRLOG("The TCP/IP tqpair connection is not negotitated\n");
			fes = SPDK_NVME_TCP_TERM_REQ_FES_PDU_SEQUENCE_ERROR;
			goto err;
		}

		switch (pdu->hdr.common.pdu_type) {
		case SPDK_NVME_TCP_PDU_TYPE_CAPSULE_RESP:
			expected_hlen = sizeof(struct spdk_nvme_tcp_rsp);
			if (pdu->hdr.common.flags & SPDK_NVME_TCP_CH_FLAGS_HDGSTF) {
				hd_len = SPDK_NVME_TCP_DIGEST_LEN;
			}

			if (pdu->hdr.common.plen != (expected_hlen + hd_len)) {
				plen_error = true;
			}
			break;
		case SPDK_NVME_TCP_PDU_TYPE_C2H_DATA:
			expected_hlen = sizeof(struct spdk_nvme_tcp_c2h_data_hdr);
			if (pdu->hdr.common.plen < pdu->hdr.common.pdo) {
				plen_error = true;
			}
			break;
		case SPDK_NVME_TCP_PDU_TYPE_C2H_TERM_REQ:
			expected_hlen = sizeof(struct spdk_nvme_tcp_term_req_hdr);
			if ((pdu->hdr.common.plen <= expected_hlen) ||
			    (pdu->hdr.common.plen > SPDK_NVME_TCP_TERM_REQ_PDU_MAX_SIZE)) {
				plen_error = true;
			}
			break;
		case SPDK_NVME_TCP_PDU_TYPE_R2T:
			expected_hlen = sizeof(struct spdk_nvme_tcp_r2t_hdr);
			if (pdu->hdr.common.flags & SPDK_NVME_TCP_CH_FLAGS_HDGSTF) {
				hd_len = SPDK_NVME_TCP_DIGEST_LEN;
			}

			if (pdu->hdr.common.plen != (expected_hlen + hd_len)) {
				plen_error = true;
			}
			break;

		default:
			SPDK_ERRLOG("Unexpected PDU type 0x%02x\n", tqpair->recv_pdu.hdr.common.pdu_type);
			fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
			error_offset = offsetof(struct spdk_nvme_tcp_common_pdu_hdr, pdu_type);
			goto err;
		}
	}

	if (pdu->hdr.common.hlen != expected_hlen) {
		SPDK_ERRLOG("Expected PDU header length %u, got %u\n",
			    expected_hlen, pdu->hdr.common.hlen);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_common_pdu_hdr, hlen);
		goto err;

	} else if (plen_error) {
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_common_pdu_hdr, plen);
		goto err;
	} else {
		nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PSH);
		nvme_tcp_pdu_calc_psh_len(&tqpair->recv_pdu, tqpair->flags.host_hdgst_enable);
		return;
	}
err:
	nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);
}

static struct nvme_tcp_req *
get_nvme_active_req_by_cid(struct nvme_tcp_qpair *tqpair, uint32_t cid)
{
	assert(tqpair != NULL);
	if ((cid >= tqpair->num_entries) || (tqpair->tcp_reqs[cid].state == NVME_TCP_REQ_FREE)) {
		return NULL;
	}

	return &tqpair->tcp_reqs[cid];
}

static void
nvme_tcp_c2h_data_payload_handle(struct nvme_tcp_qpair *tqpair,
				 struct nvme_tcp_pdu *pdu, uint32_t *reaped)
{
	struct nvme_tcp_req *tcp_req;
	struct spdk_nvme_tcp_c2h_data_hdr *c2h_data;
	uint8_t flags;

	tcp_req = pdu->req;
	assert(tcp_req != NULL);

	SPDK_DEBUGLOG(nvme, "enter\n");
	c2h_data = &pdu->hdr.c2h_data;
	tcp_req->datao += pdu->data_len;
	flags = c2h_data->common.flags;

	nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY);
	if (flags & SPDK_NVME_TCP_C2H_DATA_FLAGS_SUCCESS) {
		if (tcp_req->datao == tcp_req->req->payload_size) {
			tcp_req->rsp.status.p = 0;
		} else {
			tcp_req->rsp.status.p = 1;
		}

		tcp_req->rsp.cid = tcp_req->cid;
		tcp_req->rsp.sqid = tqpair->qpair.id;
		tcp_req->ordering.bits.data_recv = 1;

		if (nvme_tcp_req_complete_safe(tcp_req)) {
			(*reaped)++;
		}
	}
}

static const char *spdk_nvme_tcp_term_req_fes_str[] = {
	"Invalid PDU Header Field",
	"PDU Sequence Error",
	"Header Digest Error",
	"Data Transfer Out of Range",
	"Data Transfer Limit Exceeded",
	"Unsupported parameter",
};

static void
nvme_tcp_c2h_term_req_dump(struct spdk_nvme_tcp_term_req_hdr *c2h_term_req)
{
	SPDK_ERRLOG("Error info of pdu(%p): %s\n", c2h_term_req,
		    spdk_nvme_tcp_term_req_fes_str[c2h_term_req->fes]);
	if ((c2h_term_req->fes == SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD) ||
	    (c2h_term_req->fes == SPDK_NVME_TCP_TERM_REQ_FES_INVALID_DATA_UNSUPPORTED_PARAMETER)) {
		SPDK_DEBUGLOG(nvme, "The offset from the start of the PDU header is %u\n",
			      DGET32(c2h_term_req->fei));
	}
	/* we may also need to dump some other info here */
}

static void
nvme_tcp_c2h_term_req_payload_handle(struct nvme_tcp_qpair *tqpair,
				     struct nvme_tcp_pdu *pdu)
{
	nvme_tcp_c2h_term_req_dump(&pdu->hdr.term_req);
	nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_ERROR);
}

static void
nvme_tcp_pdu_payload_handle(struct nvme_tcp_qpair *tqpair,
			    uint32_t *reaped)
{
	int rc = 0;
	struct nvme_tcp_pdu *pdu;
	uint32_t crc32c, error_offset = 0;
	enum spdk_nvme_tcp_term_req_fes fes;

	assert(tqpair->recv_state == NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PAYLOAD);
	pdu = &tqpair->recv_pdu;

	SPDK_DEBUGLOG(nvme, "enter\n");

	/* check data digest if need */
	if (pdu->ddgst_enable) {
		crc32c = nvme_tcp_pdu_calc_data_digest(pdu);
		rc = MATCH_DIGEST_WORD(pdu->data_digest, crc32c);
		if (rc == 0) {
			SPDK_ERRLOG("data digest error on tqpair=(%p) with pdu=%p\n", tqpair, pdu);
			fes = SPDK_NVME_TCP_TERM_REQ_FES_HDGST_ERROR;
			nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);
			return;
		}
	}

	switch (pdu->hdr.common.pdu_type) {
	case SPDK_NVME_TCP_PDU_TYPE_C2H_DATA:
		nvme_tcp_c2h_data_payload_handle(tqpair, pdu, reaped);
		break;

	case SPDK_NVME_TCP_PDU_TYPE_C2H_TERM_REQ:
		nvme_tcp_c2h_term_req_payload_handle(tqpair, pdu);
		break;

	default:
		/* The code should not go to here */
		SPDK_ERRLOG("The code should not go to here\n");
		break;
	}
}

static void
nvme_tcp_send_icreq_complete(void *cb_arg)
{
	struct nvme_tcp_qpair *tqpair = cb_arg;

	SPDK_DEBUGLOG(nvme, "Complete the icreq send for tqpair=%p %u\n", tqpair, tqpair->qpair.id);

	tqpair->flags.icreq_send_ack = true;

	if (tqpair->state == NVME_TCP_QPAIR_STATE_INITIALIZING) {
		SPDK_DEBUGLOG(nvme, "tqpair %p %u, finilize icresp\n", tqpair, tqpair->qpair.id);
		tqpair->state = NVME_TCP_QPAIR_STATE_RUNNING;
	}
}

static void
nvme_tcp_icresp_handle(struct nvme_tcp_qpair *tqpair,
		       struct nvme_tcp_pdu *pdu)
{
	struct spdk_nvme_tcp_ic_resp *ic_resp = &pdu->hdr.ic_resp;
	uint32_t error_offset = 0;
	enum spdk_nvme_tcp_term_req_fes fes;
	int recv_buf_size;

	/* Only PFV 0 is defined currently */
	if (ic_resp->pfv != 0) {
		SPDK_ERRLOG("Expected ICResp PFV %u, got %u\n", 0u, ic_resp->pfv);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_ic_resp, pfv);
		goto end;
	}

	if (ic_resp->maxh2cdata < NVME_TCP_PDU_H2C_MIN_DATA_SIZE) {
		SPDK_ERRLOG("Expected ICResp maxh2cdata >=%u, got %u\n", NVME_TCP_PDU_H2C_MIN_DATA_SIZE,
			    ic_resp->maxh2cdata);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_ic_resp, maxh2cdata);
		goto end;
	}
	tqpair->maxh2cdata = ic_resp->maxh2cdata;

	if (ic_resp->cpda > SPDK_NVME_TCP_CPDA_MAX) {
		SPDK_ERRLOG("Expected ICResp cpda <=%u, got %u\n", SPDK_NVME_TCP_CPDA_MAX, ic_resp->cpda);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_ic_resp, cpda);
		goto end;
	}
	tqpair->cpda = ic_resp->cpda;

	tqpair->flags.host_hdgst_enable = ic_resp->dgst.bits.hdgst_enable ? true : false;
	tqpair->flags.host_ddgst_enable = ic_resp->dgst.bits.ddgst_enable ? true : false;
	SPDK_DEBUGLOG(nvme, "host_hdgst_enable: %u\n", tqpair->flags.host_hdgst_enable);
	SPDK_DEBUGLOG(nvme, "host_ddgst_enable: %u\n", tqpair->flags.host_ddgst_enable);

	/* Now that we know whether digests are enabled, properly size the receive buffer to
	 * handle several incoming 4K read commands according to SPDK_NVMF_TCP_RECV_BUF_SIZE_FACTOR
	 * parameter. */
	recv_buf_size = 0x1000 + sizeof(struct spdk_nvme_tcp_c2h_data_hdr);

	if (tqpair->flags.host_hdgst_enable) {
		recv_buf_size += SPDK_NVME_TCP_DIGEST_LEN;
	}

	if (tqpair->flags.host_ddgst_enable) {
		recv_buf_size += SPDK_NVME_TCP_DIGEST_LEN;
	}

	if (spdk_sock_set_recvbuf(tqpair->sock, recv_buf_size * SPDK_NVMF_TCP_RECV_BUF_SIZE_FACTOR) < 0) {
		SPDK_WARNLOG("Unable to allocate enough memory for receive buffer on tqpair=%p with size=%d\n",
			     tqpair,
			     recv_buf_size);
		/* Not fatal. */
	}

	nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY);

	if (!tqpair->flags.icreq_send_ack) {
		tqpair->state = NVME_TCP_QPAIR_STATE_INITIALIZING;
		SPDK_DEBUGLOG(nvme, "tqpair %p %u, waiting icreq ack\n", tqpair, tqpair->qpair.id);
		return;
	}

	tqpair->state = NVME_TCP_QPAIR_STATE_RUNNING;
	return;
end:
	nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);
}

static void
nvme_tcp_capsule_resp_hdr_handle(struct nvme_tcp_qpair *tqpair, struct nvme_tcp_pdu *pdu,
				 uint32_t *reaped)
{
	struct nvme_tcp_req *tcp_req;
	struct spdk_nvme_tcp_rsp *capsule_resp = &pdu->hdr.capsule_resp;
	uint32_t cid, error_offset = 0;
	enum spdk_nvme_tcp_term_req_fes fes;

	SPDK_DEBUGLOG(nvme, "enter\n");
	cid = capsule_resp->rccqe.cid;
	tcp_req = get_nvme_active_req_by_cid(tqpair, cid);

	if (!tcp_req) {
		SPDK_ERRLOG("no tcp_req is found with cid=%u for tqpair=%p\n", cid, tqpair);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_rsp, rccqe);
		goto end;
	}

	assert(tcp_req->req != NULL);

	tcp_req->rsp = capsule_resp->rccqe;
	tcp_req->ordering.bits.data_recv = 1;

	/* Recv the pdu again */
	nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY);

	if (nvme_tcp_req_complete_safe(tcp_req)) {
		(*reaped)++;
	}

	return;

end:
	nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);
}

static void
nvme_tcp_c2h_term_req_hdr_handle(struct nvme_tcp_qpair *tqpair,
				 struct nvme_tcp_pdu *pdu)
{
	struct spdk_nvme_tcp_term_req_hdr *c2h_term_req = &pdu->hdr.term_req;
	uint32_t error_offset = 0;
	enum spdk_nvme_tcp_term_req_fes fes;

	if (c2h_term_req->fes > SPDK_NVME_TCP_TERM_REQ_FES_INVALID_DATA_UNSUPPORTED_PARAMETER) {
		SPDK_ERRLOG("Fatal Error Stauts(FES) is unknown for c2h_term_req pdu=%p\n", pdu);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_term_req_hdr, fes);
		goto end;
	}

	/* set the data buffer */
	nvme_tcp_pdu_set_data(pdu, (uint8_t *)pdu->hdr.raw + c2h_term_req->common.hlen,
			      c2h_term_req->common.plen - c2h_term_req->common.hlen);
	nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PAYLOAD);
	return;
end:
	nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);
}

static void
nvme_tcp_c2h_data_hdr_handle(struct nvme_tcp_qpair *tqpair, struct nvme_tcp_pdu *pdu)
{
	struct nvme_tcp_req *tcp_req;
	struct spdk_nvme_tcp_c2h_data_hdr *c2h_data = &pdu->hdr.c2h_data;
	uint32_t error_offset = 0;
	enum spdk_nvme_tcp_term_req_fes fes;

	SPDK_DEBUGLOG(nvme, "enter\n");
	SPDK_DEBUGLOG(nvme, "c2h_data info on tqpair(%p): datao=%u, datal=%u, cccid=%d\n",
		      tqpair, c2h_data->datao, c2h_data->datal, c2h_data->cccid);
	tcp_req = get_nvme_active_req_by_cid(tqpair, c2h_data->cccid);
	if (!tcp_req) {
		SPDK_ERRLOG("no tcp_req found for c2hdata cid=%d\n", c2h_data->cccid);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_c2h_data_hdr, cccid);
		goto end;

	}

	SPDK_DEBUGLOG(nvme, "tcp_req(%p) on tqpair(%p): datao=%u, payload_size=%u\n",
		      tcp_req, tqpair, tcp_req->datao, tcp_req->req->payload_size);

	if (c2h_data->datal > tcp_req->req->payload_size) {
		SPDK_ERRLOG("Invalid datal for tcp_req(%p), datal(%u) exceeds payload_size(%u)\n",
			    tcp_req, c2h_data->datal, tcp_req->req->payload_size);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_DATA_TRANSFER_OUT_OF_RANGE;
		goto end;
	}

	if (tcp_req->datao != c2h_data->datao) {
		SPDK_ERRLOG("Invalid datao for tcp_req(%p), received datal(%u) != datao(%u) in tcp_req\n",
			    tcp_req, c2h_data->datao, tcp_req->datao);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_c2h_data_hdr, datao);
		goto end;
	}

	if ((c2h_data->datao + c2h_data->datal) > tcp_req->req->payload_size) {
		SPDK_ERRLOG("Invalid data range for tcp_req(%p), received (datao(%u) + datal(%u)) > datao(%u) in tcp_req\n",
			    tcp_req, c2h_data->datao, c2h_data->datal, tcp_req->req->payload_size);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_DATA_TRANSFER_OUT_OF_RANGE;
		error_offset = offsetof(struct spdk_nvme_tcp_c2h_data_hdr, datal);
		goto end;

	}

	nvme_tcp_pdu_set_data_buf(pdu, tcp_req->iov, tcp_req->iovcnt,
				  c2h_data->datao, c2h_data->datal);
	pdu->req = tcp_req;

	nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PAYLOAD);
	return;

end:
	nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);
}

static void
nvme_tcp_qpair_h2c_data_send_complete(void *cb_arg)
{
	struct nvme_tcp_req *tcp_req = cb_arg;

	assert(tcp_req != NULL);

	tcp_req->ordering.bits.send_ack = 1;
	if (tcp_req->r2tl_remain) {
		nvme_tcp_send_h2c_data(tcp_req);
	} else {
		assert(tcp_req->active_r2ts > 0);
		tcp_req->active_r2ts--;
		tcp_req->state = NVME_TCP_REQ_ACTIVE;

		if (tcp_req->ordering.bits.r2t_waiting_h2c_complete) {
			tcp_req->ordering.bits.r2t_waiting_h2c_complete = 0;
			SPDK_DEBUGLOG(nvme, "tcp_req %p: continue r2t\n", tcp_req);
			assert(tcp_req->active_r2ts > 0);
			tcp_req->ttag = tcp_req->ttag_r2t_next;
			tcp_req->r2tl_remain = tcp_req->r2tl_remain_next;
			tcp_req->state = NVME_TCP_REQ_ACTIVE_R2T;
			nvme_tcp_send_h2c_data(tcp_req);
			return;
		}

		/* Need also call this function to free the resource */
		nvme_tcp_req_complete_safe(tcp_req);
	}
}

static void
nvme_tcp_send_h2c_data(struct nvme_tcp_req *tcp_req)
{
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(tcp_req->req->qpair);
	struct nvme_tcp_pdu *rsp_pdu;
	struct spdk_nvme_tcp_h2c_data_hdr *h2c_data;
	uint32_t plen, pdo, alignment;

	/* Reinit the send_ack and h2c_send_waiting_ack bits */
	tcp_req->ordering.bits.send_ack = 0;
	tcp_req->ordering.bits.h2c_send_waiting_ack = 0;
	rsp_pdu = tcp_req->send_pdu;
	memset(rsp_pdu, 0, sizeof(*rsp_pdu));
	h2c_data = &rsp_pdu->hdr.h2c_data;

	h2c_data->common.pdu_type = SPDK_NVME_TCP_PDU_TYPE_H2C_DATA;
	plen = h2c_data->common.hlen = sizeof(*h2c_data);
	h2c_data->cccid = tcp_req->cid;
	h2c_data->ttag = tcp_req->ttag;
	h2c_data->datao = tcp_req->datao;

	h2c_data->datal = spdk_min(tcp_req->r2tl_remain, tqpair->maxh2cdata);
	nvme_tcp_pdu_set_data_buf(rsp_pdu, tcp_req->iov, tcp_req->iovcnt,
				  h2c_data->datao, h2c_data->datal);
	tcp_req->r2tl_remain -= h2c_data->datal;

	if (tqpair->flags.host_hdgst_enable) {
		h2c_data->common.flags |= SPDK_NVME_TCP_CH_FLAGS_HDGSTF;
		plen += SPDK_NVME_TCP_DIGEST_LEN;
	}

	rsp_pdu->padding_len = 0;
	pdo = plen;
	if (tqpair->cpda) {
		alignment = (tqpair->cpda + 1) << 2;
		if (alignment > plen) {
			rsp_pdu->padding_len = alignment - plen;
			pdo = plen = alignment;
		}
	}

	h2c_data->common.pdo = pdo;
	plen += h2c_data->datal;
	if (tqpair->flags.host_ddgst_enable) {
		h2c_data->common.flags |= SPDK_NVME_TCP_CH_FLAGS_DDGSTF;
		plen += SPDK_NVME_TCP_DIGEST_LEN;
	}

	h2c_data->common.plen = plen;
	tcp_req->datao += h2c_data->datal;
	if (!tcp_req->r2tl_remain) {
		h2c_data->common.flags |= SPDK_NVME_TCP_H2C_DATA_FLAGS_LAST_PDU;
	}

	SPDK_DEBUGLOG(nvme, "h2c_data info: datao=%u, datal=%u, pdu_len=%u for tqpair=%p\n",
		      h2c_data->datao, h2c_data->datal, h2c_data->common.plen, tqpair);

	nvme_tcp_qpair_write_pdu(tqpair, rsp_pdu, nvme_tcp_qpair_h2c_data_send_complete, tcp_req);
}

static void
nvme_tcp_r2t_hdr_handle(struct nvme_tcp_qpair *tqpair, struct nvme_tcp_pdu *pdu)
{
	struct nvme_tcp_req *tcp_req;
	struct spdk_nvme_tcp_r2t_hdr *r2t = &pdu->hdr.r2t;
	uint32_t cid, error_offset = 0;
	enum spdk_nvme_tcp_term_req_fes fes;

	SPDK_DEBUGLOG(nvme, "enter\n");
	cid = r2t->cccid;
	tcp_req = get_nvme_active_req_by_cid(tqpair, cid);
	if (!tcp_req) {
		SPDK_ERRLOG("Cannot find tcp_req for tqpair=%p\n", tqpair);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_r2t_hdr, cccid);
		goto end;
	}

	SPDK_DEBUGLOG(nvme, "r2t info: r2to=%u, r2tl=%u for tqpair=%p\n", r2t->r2to, r2t->r2tl,
		      tqpair);

	if (tcp_req->state == NVME_TCP_REQ_ACTIVE) {
		assert(tcp_req->active_r2ts == 0);
		tcp_req->state = NVME_TCP_REQ_ACTIVE_R2T;
	}

	if (tcp_req->datao != r2t->r2to) {
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = offsetof(struct spdk_nvme_tcp_r2t_hdr, r2to);
		goto end;

	}

	if ((r2t->r2tl + r2t->r2to) > tcp_req->req->payload_size) {
		SPDK_ERRLOG("Invalid R2T info for tcp_req=%p: (r2to(%u) + r2tl(%u)) exceeds payload_size(%u)\n",
			    tcp_req, r2t->r2to, r2t->r2tl, tqpair->maxh2cdata);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_DATA_TRANSFER_OUT_OF_RANGE;
		error_offset = offsetof(struct spdk_nvme_tcp_r2t_hdr, r2tl);
		goto end;
	}

	tcp_req->active_r2ts++;
	if (spdk_unlikely(tcp_req->active_r2ts > tqpair->maxr2t)) {
		if (tcp_req->state == NVME_TCP_REQ_ACTIVE_R2T && !tcp_req->ordering.bits.send_ack) {
			/* We receive a subsequent R2T while we are waiting for H2C transfer to complete */
			SPDK_DEBUGLOG(nvme, "received a subsequent R2T\n");
			assert(tcp_req->active_r2ts == tqpair->maxr2t + 1);
			tcp_req->ttag_r2t_next = r2t->ttag;
			tcp_req->r2tl_remain_next = r2t->r2tl;
			tcp_req->ordering.bits.r2t_waiting_h2c_complete = 1;
			nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY);
			return;
		} else {
			fes = SPDK_NVME_TCP_TERM_REQ_FES_R2T_LIMIT_EXCEEDED;
			SPDK_ERRLOG("Invalid R2T: Maximum number of R2T exceeded! Max: %u for tqpair=%p\n", tqpair->maxr2t,
				    tqpair);
			goto end;
		}
	}

	tcp_req->ttag = r2t->ttag;
	tcp_req->r2tl_remain = r2t->r2tl;
	nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY);

	if (spdk_likely(tcp_req->ordering.bits.send_ack)) {
		nvme_tcp_send_h2c_data(tcp_req);
	} else {
		tcp_req->ordering.bits.h2c_send_waiting_ack = 1;
	}

	return;

end:
	nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);

}

static void
nvme_tcp_pdu_psh_handle(struct nvme_tcp_qpair *tqpair, uint32_t *reaped)
{
	struct nvme_tcp_pdu *pdu;
	int rc;
	uint32_t crc32c, error_offset = 0;
	enum spdk_nvme_tcp_term_req_fes fes;

	assert(tqpair->recv_state == NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PSH);
	pdu = &tqpair->recv_pdu;

	SPDK_DEBUGLOG(nvme, "enter: pdu type =%u\n", pdu->hdr.common.pdu_type);
	/* check header digest if needed */
	if (pdu->has_hdgst) {
		crc32c = nvme_tcp_pdu_calc_header_digest(pdu);
		rc = MATCH_DIGEST_WORD((uint8_t *)pdu->hdr.raw + pdu->hdr.common.hlen, crc32c);
		if (rc == 0) {
			SPDK_ERRLOG("header digest error on tqpair=(%p) with pdu=%p\n", tqpair, pdu);
			fes = SPDK_NVME_TCP_TERM_REQ_FES_HDGST_ERROR;
			nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);
			return;

		}
	}

	switch (pdu->hdr.common.pdu_type) {
	case SPDK_NVME_TCP_PDU_TYPE_IC_RESP:
		nvme_tcp_icresp_handle(tqpair, pdu);
		break;
	case SPDK_NVME_TCP_PDU_TYPE_CAPSULE_RESP:
		nvme_tcp_capsule_resp_hdr_handle(tqpair, pdu, reaped);
		break;
	case SPDK_NVME_TCP_PDU_TYPE_C2H_DATA:
		nvme_tcp_c2h_data_hdr_handle(tqpair, pdu);
		break;

	case SPDK_NVME_TCP_PDU_TYPE_C2H_TERM_REQ:
		nvme_tcp_c2h_term_req_hdr_handle(tqpair, pdu);
		break;
	case SPDK_NVME_TCP_PDU_TYPE_R2T:
		nvme_tcp_r2t_hdr_handle(tqpair, pdu);
		break;

	default:
		SPDK_ERRLOG("Unexpected PDU type 0x%02x\n", tqpair->recv_pdu.hdr.common.pdu_type);
		fes = SPDK_NVME_TCP_TERM_REQ_FES_INVALID_HEADER_FIELD;
		error_offset = 1;
		nvme_tcp_qpair_send_h2c_term_req(tqpair, pdu, fes, error_offset);
		break;
	}

}

static int
nvme_tcp_read_pdu(struct nvme_tcp_qpair *tqpair, uint32_t *reaped)
{
	int rc = 0;
	struct nvme_tcp_pdu *pdu;
	uint32_t data_len;
	enum nvme_tcp_pdu_recv_state prev_state;

	/* The loop here is to allow for several back-to-back state changes. */
	do {
		prev_state = tqpair->recv_state;
		switch (tqpair->recv_state) {
		/* If in a new state */
		case NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY:
			nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_CH);
			break;
		/* common header */
		case NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_CH:
			pdu = &tqpair->recv_pdu;
			if (pdu->ch_valid_bytes < sizeof(struct spdk_nvme_tcp_common_pdu_hdr)) {
				rc = nvme_tcp_read_data(tqpair->sock,
							sizeof(struct spdk_nvme_tcp_common_pdu_hdr) - pdu->ch_valid_bytes,
							(uint8_t *)&pdu->hdr.common + pdu->ch_valid_bytes);
				if (rc < 0) {
					nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_ERROR);
					break;
				}
				pdu->ch_valid_bytes += rc;
				if (pdu->ch_valid_bytes < sizeof(struct spdk_nvme_tcp_common_pdu_hdr)) {
					rc =  NVME_TCP_PDU_IN_PROGRESS;
					goto out;
				}
			}

			/* The command header of this PDU has now been read from the socket. */
			nvme_tcp_pdu_ch_handle(tqpair);
			break;
		/* Wait for the pdu specific header  */
		case NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PSH:
			pdu = &tqpair->recv_pdu;
			rc = nvme_tcp_read_data(tqpair->sock,
						pdu->psh_len - pdu->psh_valid_bytes,
						(uint8_t *)&pdu->hdr.raw + sizeof(struct spdk_nvme_tcp_common_pdu_hdr) + pdu->psh_valid_bytes);
			if (rc < 0) {
				nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_ERROR);
				break;
			}

			pdu->psh_valid_bytes += rc;
			if (pdu->psh_valid_bytes < pdu->psh_len) {
				rc =  NVME_TCP_PDU_IN_PROGRESS;
				goto out;
			}

			/* All header(ch, psh, head digist) of this PDU has now been read from the socket. */
			nvme_tcp_pdu_psh_handle(tqpair, reaped);
			break;
		case NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PAYLOAD:
			pdu = &tqpair->recv_pdu;
			/* check whether the data is valid, if not we just return */
			if (!pdu->data_len) {
				return NVME_TCP_PDU_IN_PROGRESS;
			}

			data_len = pdu->data_len;
			/* data digest */
			if (spdk_unlikely((pdu->hdr.common.pdu_type == SPDK_NVME_TCP_PDU_TYPE_C2H_DATA) &&
					  tqpair->flags.host_ddgst_enable)) {
				data_len += SPDK_NVME_TCP_DIGEST_LEN;
				pdu->ddgst_enable = true;
			}

			rc = nvme_tcp_read_payload_data(tqpair->sock, pdu);
			if (rc < 0) {
				nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_ERROR);
				break;
			}

			pdu->rw_offset += rc;
			if (pdu->rw_offset < data_len) {
				rc =  NVME_TCP_PDU_IN_PROGRESS;
				goto out;
			}

			assert(pdu->rw_offset == data_len);
			/* All of this PDU has now been read from the socket. */
			nvme_tcp_pdu_payload_handle(tqpair, reaped);
			break;
		case NVME_TCP_PDU_RECV_STATE_ERROR:
			rc = NVME_TCP_PDU_FATAL;
			break;
		default:
			assert(0);
			break;
		}
	} while (prev_state != tqpair->recv_state);

out:
	*reaped += tqpair->async_complete;
	tqpair->async_complete = 0;

	return rc;
}

static void
nvme_tcp_qpair_check_timeout(struct spdk_nvme_qpair *qpair)
{
	uint64_t t02;
	struct nvme_tcp_req *tcp_req, *tmp;
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);
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
	TAILQ_FOREACH_SAFE(tcp_req, &tqpair->outstanding_reqs, link, tmp) {
		assert(tcp_req->req != NULL);

		if (nvme_request_check_timeout(tcp_req->req, tcp_req->cid, active_proc, t02)) {
			/*
			 * The requests are in order, so as soon as one has not timed out,
			 * stop iterating.
			 */
			break;
		}
	}
}

static int
nvme_tcp_qpair_process_completions(struct spdk_nvme_qpair *qpair, uint32_t max_completions)
{
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);
	uint32_t reaped;
	int rc;

	if (qpair->poll_group == NULL) {
		rc = spdk_sock_flush(tqpair->sock);
		if (rc < 0) {
			return rc;
		}
	}

	if (max_completions == 0) {
		max_completions = tqpair->num_entries;
	} else {
		max_completions = spdk_min(max_completions, tqpair->num_entries);
	}

	reaped = 0;
	do {
		rc = nvme_tcp_read_pdu(tqpair, &reaped);
		if (rc < 0) {
			SPDK_DEBUGLOG(nvme, "Error polling CQ! (%d): %s\n",
				      errno, spdk_strerror(errno));
			goto fail;
		} else if (rc == 0) {
			/* Partial PDU is read */
			break;
		}

	} while (reaped < max_completions);

	if (spdk_unlikely(tqpair->qpair.ctrlr->timeout_enabled)) {
		nvme_tcp_qpair_check_timeout(qpair);
	}

	return reaped;
fail:

	/*
	 * Since admin queues take the ctrlr_lock before entering this function,
	 * we can call nvme_transport_ctrlr_disconnect_qpair. For other qpairs we need
	 * to call the generic function which will take the lock for us.
	 */
	qpair->transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_UNKNOWN;

	if (nvme_qpair_is_admin_queue(qpair)) {
		nvme_transport_ctrlr_disconnect_qpair(qpair->ctrlr, qpair);
	} else {
		nvme_ctrlr_disconnect_qpair(qpair);
	}
	return -ENXIO;
}

static void
nvme_tcp_qpair_sock_cb(void *ctx, struct spdk_sock_group *group, struct spdk_sock *sock)
{
	struct spdk_nvme_qpair *qpair = ctx;
	struct nvme_tcp_poll_group *pgroup = nvme_tcp_poll_group(qpair->poll_group);
	int32_t num_completions;
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);

	if (tqpair->needs_poll) {
		TAILQ_REMOVE(&pgroup->needs_poll, tqpair, link);
		tqpair->needs_poll = false;
	}

	num_completions = spdk_nvme_qpair_process_completions(qpair, pgroup->completions_per_qpair);

	if (pgroup->num_completions >= 0 && num_completions >= 0) {
		pgroup->num_completions += num_completions;
	} else {
		pgroup->num_completions = -ENXIO;
	}
}

static void
dummy_disconnected_qpair_cb(struct spdk_nvme_qpair *qpair, void *poll_group_ctx)
{
}

static int
nvme_tcp_qpair_icreq_send(struct nvme_tcp_qpair *tqpair)
{
	struct spdk_nvme_tcp_ic_req *ic_req;
	struct nvme_tcp_pdu *pdu;
	uint64_t icreq_timeout_tsc;
	int rc;

	pdu = tqpair->send_pdu;
	memset(tqpair->send_pdu, 0, sizeof(*tqpair->send_pdu));
	ic_req = &pdu->hdr.ic_req;

	ic_req->common.pdu_type = SPDK_NVME_TCP_PDU_TYPE_IC_REQ;
	ic_req->common.hlen = ic_req->common.plen = sizeof(*ic_req);
	ic_req->pfv = 0;
	ic_req->maxr2t = NVME_TCP_MAX_R2T_DEFAULT - 1;
	ic_req->hpda = NVME_TCP_HPDA_DEFAULT;

	ic_req->dgst.bits.hdgst_enable = tqpair->qpair.ctrlr->opts.header_digest;
	ic_req->dgst.bits.ddgst_enable = tqpair->qpair.ctrlr->opts.data_digest;

	nvme_tcp_qpair_write_pdu(tqpair, pdu, nvme_tcp_send_icreq_complete, tqpair);

	icreq_timeout_tsc = spdk_get_ticks() + (NVME_TCP_TIME_OUT_IN_SECONDS * spdk_get_ticks_hz());
	do {
		if (tqpair->qpair.poll_group) {
			rc = (int)nvme_tcp_poll_group_process_completions(tqpair->qpair.poll_group, 0,
					dummy_disconnected_qpair_cb);
		} else {
			rc = nvme_tcp_qpair_process_completions(&tqpair->qpair, 0);
		}
	} while ((tqpair->state != NVME_TCP_QPAIR_STATE_RUNNING) &&
		 (rc >= 0) && (spdk_get_ticks() <= icreq_timeout_tsc));

	if (tqpair->state != NVME_TCP_QPAIR_STATE_RUNNING) {
		SPDK_ERRLOG("Failed to construct the tqpair=%p via correct icresp\n", tqpair);
		return -1;
	}

	SPDK_DEBUGLOG(nvme, "Succesfully construct the tqpair=%p via correct icresp\n", tqpair);

	return 0;
}

static int
nvme_tcp_qpair_connect_sock(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	struct sockaddr_storage dst_addr;
	struct sockaddr_storage src_addr;
	int rc;
	struct nvme_tcp_qpair *tqpair;
	int family;
	long int port;
	struct spdk_sock_opts opts;

	tqpair = nvme_tcp_qpair(qpair);

	switch (ctrlr->trid.adrfam) {
	case SPDK_NVMF_ADRFAM_IPV4:
		family = AF_INET;
		break;
	case SPDK_NVMF_ADRFAM_IPV6:
		family = AF_INET6;
		break;
	default:
		SPDK_ERRLOG("Unhandled ADRFAM %d\n", ctrlr->trid.adrfam);
		rc = -1;
		return rc;
	}

	SPDK_DEBUGLOG(nvme, "adrfam %d ai_family %d\n", ctrlr->trid.adrfam, family);

	memset(&dst_addr, 0, sizeof(dst_addr));

	SPDK_DEBUGLOG(nvme, "trsvcid is %s\n", ctrlr->trid.trsvcid);
	rc = nvme_tcp_parse_addr(&dst_addr, family, ctrlr->trid.traddr, ctrlr->trid.trsvcid);
	if (rc != 0) {
		SPDK_ERRLOG("dst_addr nvme_tcp_parse_addr() failed\n");
		return rc;
	}

	if (ctrlr->opts.src_addr[0] || ctrlr->opts.src_svcid[0]) {
		memset(&src_addr, 0, sizeof(src_addr));
		rc = nvme_tcp_parse_addr(&src_addr, family, ctrlr->opts.src_addr, ctrlr->opts.src_svcid);
		if (rc != 0) {
			SPDK_ERRLOG("src_addr nvme_tcp_parse_addr() failed\n");
			return rc;
		}
	}

	port = spdk_strtol(ctrlr->trid.trsvcid, 10);
	if (port <= 0 || port >= INT_MAX) {
		SPDK_ERRLOG("Invalid port: %s\n", ctrlr->trid.trsvcid);
		rc = -1;
		return rc;
	}

	opts.opts_size = sizeof(opts);
	spdk_sock_get_default_opts(&opts);
	opts.priority = ctrlr->trid.priority;
	opts.zcopy = !nvme_qpair_is_admin_queue(qpair);
	tqpair->sock = spdk_sock_connect_ext(ctrlr->trid.traddr, port, NULL, &opts);
	if (!tqpair->sock) {
		SPDK_ERRLOG("sock connection error of tqpair=%p with addr=%s, port=%ld\n",
			    tqpair, ctrlr->trid.traddr, port);
		rc = -1;
		return rc;
	}

	return 0;
}

static int
nvme_tcp_ctrlr_connect_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	int rc = 0;
	struct nvme_tcp_qpair *tqpair;

	tqpair = nvme_tcp_qpair(qpair);

	if (!tqpair->sock) {
		rc = nvme_tcp_qpair_connect_sock(ctrlr, qpair);
		if (rc < 0) {
			return rc;
		}
	}

	if (qpair->poll_group) {
		rc = nvme_poll_group_connect_qpair(qpair);
		if (rc) {
			SPDK_ERRLOG("Unable to activate the tcp qpair.\n");
			return rc;
		}
	}

	tqpair->maxr2t = NVME_TCP_MAX_R2T_DEFAULT;
	/* Explicitly set the state and recv_state of tqpair */
	tqpair->state = NVME_TCP_QPAIR_STATE_INVALID;
	if (tqpair->recv_state != NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY) {
		nvme_tcp_qpair_set_recv_state(tqpair, NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY);
	}
	rc = nvme_tcp_qpair_icreq_send(tqpair);
	if (rc != 0) {
		SPDK_ERRLOG("Unable to connect the tqpair\n");
		return rc;
	}

	rc = nvme_fabric_qpair_connect(&tqpair->qpair, tqpair->num_entries);
	if (rc < 0) {
		SPDK_ERRLOG("Failed to send an NVMe-oF Fabric CONNECT command\n");
		return rc;
	}

	return 0;
}

static struct spdk_nvme_qpair *
nvme_tcp_ctrlr_create_qpair(struct spdk_nvme_ctrlr *ctrlr,
			    uint16_t qid, uint32_t qsize,
			    enum spdk_nvme_qprio qprio,
			    uint32_t num_requests)
{
	struct nvme_tcp_qpair *tqpair;
	struct spdk_nvme_qpair *qpair;
	int rc;

	tqpair = calloc(1, sizeof(struct nvme_tcp_qpair));
	if (!tqpair) {
		SPDK_ERRLOG("failed to get create tqpair\n");
		return NULL;
	}

	tqpair->num_entries = qsize;
	qpair = &tqpair->qpair;
	rc = nvme_qpair_init(qpair, qid, ctrlr, qprio, num_requests);
	if (rc != 0) {
		free(tqpair);
		return NULL;
	}

	rc = nvme_tcp_alloc_reqs(tqpair);
	if (rc) {
		nvme_tcp_ctrlr_delete_io_qpair(ctrlr, qpair);
		return NULL;
	}

	/* spdk_nvme_qpair_get_optimal_poll_group needs socket information.
	 * So create the socket first when creating a qpair. */
	rc = nvme_tcp_qpair_connect_sock(ctrlr, qpair);
	if (rc) {
		nvme_tcp_ctrlr_delete_io_qpair(ctrlr, qpair);
		return NULL;
	}

	return qpair;
}

static struct spdk_nvme_qpair *
nvme_tcp_ctrlr_create_io_qpair(struct spdk_nvme_ctrlr *ctrlr, uint16_t qid,
			       const struct spdk_nvme_io_qpair_opts *opts)
{
	return nvme_tcp_ctrlr_create_qpair(ctrlr, qid, opts->io_queue_size, opts->qprio,
					   opts->io_queue_requests);
}

static struct spdk_nvme_ctrlr *nvme_tcp_ctrlr_construct(const struct spdk_nvme_transport_id *trid,
		const struct spdk_nvme_ctrlr_opts *opts,
		void *devhandle)
{
	struct nvme_tcp_ctrlr *tctrlr;
	union spdk_nvme_cap_register cap;
	union spdk_nvme_vs_register vs;
	int rc;

	tctrlr = calloc(1, sizeof(*tctrlr));
	if (tctrlr == NULL) {
		SPDK_ERRLOG("could not allocate ctrlr\n");
		return NULL;
	}

	tctrlr->ctrlr.opts = *opts;
	tctrlr->ctrlr.trid = *trid;

	rc = nvme_ctrlr_construct(&tctrlr->ctrlr);
	if (rc != 0) {
		free(tctrlr);
		return NULL;
	}

	tctrlr->ctrlr.adminq = nvme_tcp_ctrlr_create_qpair(&tctrlr->ctrlr, 0,
			       tctrlr->ctrlr.opts.admin_queue_size, 0,
			       tctrlr->ctrlr.opts.admin_queue_size);
	if (!tctrlr->ctrlr.adminq) {
		SPDK_ERRLOG("failed to create admin qpair\n");
		nvme_tcp_ctrlr_destruct(&tctrlr->ctrlr);
		return NULL;
	}

	rc = nvme_transport_ctrlr_connect_qpair(&tctrlr->ctrlr, tctrlr->ctrlr.adminq);
	if (rc < 0) {
		SPDK_ERRLOG("failed to connect admin qpair\n");
		nvme_tcp_ctrlr_destruct(&tctrlr->ctrlr);
		return NULL;
	}

	if (nvme_ctrlr_get_cap(&tctrlr->ctrlr, &cap)) {
		SPDK_ERRLOG("get_cap() failed\n");
		nvme_ctrlr_destruct(&tctrlr->ctrlr);
		return NULL;
	}

	if (nvme_ctrlr_get_vs(&tctrlr->ctrlr, &vs)) {
		SPDK_ERRLOG("get_vs() failed\n");
		nvme_ctrlr_destruct(&tctrlr->ctrlr);
		return NULL;
	}

	if (nvme_ctrlr_add_process(&tctrlr->ctrlr, 0) != 0) {
		SPDK_ERRLOG("nvme_ctrlr_add_process() failed\n");
		nvme_ctrlr_destruct(&tctrlr->ctrlr);
		return NULL;
	}

	nvme_ctrlr_init_cap(&tctrlr->ctrlr, &cap, &vs);

	return &tctrlr->ctrlr;
}

static uint32_t
nvme_tcp_ctrlr_get_max_xfer_size(struct spdk_nvme_ctrlr *ctrlr)
{
	/* TCP transport doens't limit maximum IO transfer size. */
	return UINT32_MAX;
}

static uint16_t
nvme_tcp_ctrlr_get_max_sges(struct spdk_nvme_ctrlr *ctrlr)
{
	/*
	 * We do not support >1 SGE in the initiator currently,
	 *  so we can only return 1 here.  Once that support is
	 *  added, this should return ctrlr->cdata.nvmf_specific.msdbd
	 *  instead.
	 */
	return 1;
}

static int
nvme_tcp_qpair_iterate_requests(struct spdk_nvme_qpair *qpair,
				int (*iter_fn)(struct nvme_request *req, void *arg),
				void *arg)
{
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);
	struct nvme_tcp_req *tcp_req, *tmp;
	int rc;

	assert(iter_fn != NULL);

	TAILQ_FOREACH_SAFE(tcp_req, &tqpair->outstanding_reqs, link, tmp) {
		assert(tcp_req->req != NULL);

		rc = iter_fn(tcp_req->req, arg);
		if (rc != 0) {
			return rc;
		}
	}

	return 0;
}

static void
nvme_tcp_admin_qpair_abort_aers(struct spdk_nvme_qpair *qpair)
{
	struct nvme_tcp_req *tcp_req, *tmp;
	struct spdk_nvme_cpl cpl;
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);

	cpl.status.sc = SPDK_NVME_SC_ABORTED_SQ_DELETION;
	cpl.status.sct = SPDK_NVME_SCT_GENERIC;

	TAILQ_FOREACH_SAFE(tcp_req, &tqpair->outstanding_reqs, link, tmp) {
		assert(tcp_req->req != NULL);
		if (tcp_req->req->cmd.opc != SPDK_NVME_OPC_ASYNC_EVENT_REQUEST) {
			continue;
		}

		nvme_tcp_req_complete(tcp_req, &cpl);
		nvme_tcp_req_put(tqpair, tcp_req);
	}
}

static struct spdk_nvme_transport_poll_group *
nvme_tcp_poll_group_create(void)
{
	struct nvme_tcp_poll_group *group = calloc(1, sizeof(*group));

	if (group == NULL) {
		SPDK_ERRLOG("Unable to allocate poll group.\n");
		return NULL;
	}

	TAILQ_INIT(&group->needs_poll);

	group->sock_group = spdk_sock_group_create(group);
	if (group->sock_group == NULL) {
		free(group);
		SPDK_ERRLOG("Unable to allocate sock group.\n");
		return NULL;
	}

	return &group->group;
}

static struct spdk_nvme_transport_poll_group *
nvme_tcp_qpair_get_optimal_poll_group(struct spdk_nvme_qpair *qpair)
{
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);
	struct spdk_sock_group *group = NULL;
	int rc;

	rc = spdk_sock_get_optimal_sock_group(tqpair->sock, &group);
	if (!rc && group != NULL) {
		return spdk_sock_group_get_ctx(group);
	}

	return NULL;
}

static int
nvme_tcp_poll_group_connect_qpair(struct spdk_nvme_qpair *qpair)
{
	struct nvme_tcp_poll_group *group = nvme_tcp_poll_group(qpair->poll_group);
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);

	if (spdk_sock_group_add_sock(group->sock_group, tqpair->sock, nvme_tcp_qpair_sock_cb, qpair)) {
		return -EPROTO;
	}
	return 0;
}

static int
nvme_tcp_poll_group_disconnect_qpair(struct spdk_nvme_qpair *qpair)
{
	struct nvme_tcp_poll_group *group = nvme_tcp_poll_group(qpair->poll_group);
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);

	if (tqpair->needs_poll) {
		TAILQ_REMOVE(&group->needs_poll, tqpair, link);
		tqpair->needs_poll = false;
	}

	if (tqpair->sock && group->sock_group) {
		if (spdk_sock_group_remove_sock(group->sock_group, tqpair->sock)) {
			return -EPROTO;
		}
	}
	return 0;
}

static int
nvme_tcp_poll_group_add(struct spdk_nvme_transport_poll_group *tgroup,
			struct spdk_nvme_qpair *qpair)
{
	struct nvme_tcp_qpair *tqpair = nvme_tcp_qpair(qpair);
	struct nvme_tcp_poll_group *group = nvme_tcp_poll_group(tgroup);

	/* disconnected qpairs won't have a sock to add. */
	if (nvme_qpair_get_state(qpair) >= NVME_QPAIR_CONNECTED) {
		if (spdk_sock_group_add_sock(group->sock_group, tqpair->sock, nvme_tcp_qpair_sock_cb, qpair)) {
			return -EPROTO;
		}
	}

	return 0;
}

static int
nvme_tcp_poll_group_remove(struct spdk_nvme_transport_poll_group *tgroup,
			   struct spdk_nvme_qpair *qpair)
{
	if (qpair->poll_group_tailq_head == &tgroup->connected_qpairs) {
		return nvme_poll_group_disconnect_qpair(qpair);
	}

	return 0;
}

static int64_t
nvme_tcp_poll_group_process_completions(struct spdk_nvme_transport_poll_group *tgroup,
					uint32_t completions_per_qpair, spdk_nvme_disconnected_qpair_cb disconnected_qpair_cb)
{
	struct nvme_tcp_poll_group *group = nvme_tcp_poll_group(tgroup);
	struct spdk_nvme_qpair *qpair, *tmp_qpair;
	struct nvme_tcp_qpair *tqpair, *tmp_tqpair;

	group->completions_per_qpair = completions_per_qpair;
	group->num_completions = 0;

	spdk_sock_group_poll(group->sock_group);

	STAILQ_FOREACH_SAFE(qpair, &tgroup->disconnected_qpairs, poll_group_stailq, tmp_qpair) {
		disconnected_qpair_cb(qpair, tgroup->group->ctx);
	}

	/* If any qpairs were marked as needing to be polled due to an asynchronous write completion
	 * and they weren't polled as a consequence of calling spdk_sock_group_poll above, poll them now. */
	TAILQ_FOREACH_SAFE(tqpair, &group->needs_poll, link, tmp_tqpair) {
		nvme_tcp_qpair_sock_cb(&tqpair->qpair, group->sock_group, tqpair->sock);
	}

	return group->num_completions;
}

static int
nvme_tcp_poll_group_destroy(struct spdk_nvme_transport_poll_group *tgroup)
{
	int rc;
	struct nvme_tcp_poll_group *group = nvme_tcp_poll_group(tgroup);

	if (!STAILQ_EMPTY(&tgroup->connected_qpairs) || !STAILQ_EMPTY(&tgroup->disconnected_qpairs)) {
		return -EBUSY;
	}

	rc = spdk_sock_group_close(&group->sock_group);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to close the sock group for a tcp poll group.\n");
		assert(false);
	}

	free(tgroup);

	return 0;
}

const struct spdk_nvme_transport_ops tcp_ops = {
	.name = "TCP",
	.type = SPDK_NVME_TRANSPORT_TCP,
	.ctrlr_construct = nvme_tcp_ctrlr_construct,
	.ctrlr_scan = nvme_fabric_ctrlr_scan,
	.ctrlr_destruct = nvme_tcp_ctrlr_destruct,
	.ctrlr_enable = nvme_tcp_ctrlr_enable,

	.ctrlr_set_reg_4 = nvme_fabric_ctrlr_set_reg_4,
	.ctrlr_set_reg_8 = nvme_fabric_ctrlr_set_reg_8,
	.ctrlr_get_reg_4 = nvme_fabric_ctrlr_get_reg_4,
	.ctrlr_get_reg_8 = nvme_fabric_ctrlr_get_reg_8,

	.ctrlr_get_max_xfer_size = nvme_tcp_ctrlr_get_max_xfer_size,
	.ctrlr_get_max_sges = nvme_tcp_ctrlr_get_max_sges,

	.ctrlr_create_io_qpair = nvme_tcp_ctrlr_create_io_qpair,
	.ctrlr_delete_io_qpair = nvme_tcp_ctrlr_delete_io_qpair,
	.ctrlr_connect_qpair = nvme_tcp_ctrlr_connect_qpair,
	.ctrlr_disconnect_qpair = nvme_tcp_ctrlr_disconnect_qpair,

	.qpair_abort_reqs = nvme_tcp_qpair_abort_reqs,
	.qpair_reset = nvme_tcp_qpair_reset,
	.qpair_submit_request = nvme_tcp_qpair_submit_request,
	.qpair_process_completions = nvme_tcp_qpair_process_completions,
	.qpair_iterate_requests = nvme_tcp_qpair_iterate_requests,
	.admin_qpair_abort_aers = nvme_tcp_admin_qpair_abort_aers,

	.poll_group_create = nvme_tcp_poll_group_create,
	.qpair_get_optimal_poll_group = nvme_tcp_qpair_get_optimal_poll_group,
	.poll_group_connect_qpair = nvme_tcp_poll_group_connect_qpair,
	.poll_group_disconnect_qpair = nvme_tcp_poll_group_disconnect_qpair,
	.poll_group_add = nvme_tcp_poll_group_add,
	.poll_group_remove = nvme_tcp_poll_group_remove,
	.poll_group_process_completions = nvme_tcp_poll_group_process_completions,
	.poll_group_destroy = nvme_tcp_poll_group_destroy,
};

SPDK_NVME_TRANSPORT_REGISTER(tcp, &tcp_ops);
