#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021 Mellanox Technologies LTD. All rights reserved.
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
 * NVMe transport abstraction
 */

#include "nvme_internal.h"
#include "spdk/queue.h"

#define SPDK_MAX_NUM_OF_TRANSPORTS 16

struct spdk_nvme_transport {
	struct spdk_nvme_transport_ops	ops;
	TAILQ_ENTRY(spdk_nvme_transport)	link;
};

TAILQ_HEAD(nvme_transport_list, spdk_nvme_transport) g_spdk_nvme_transports =
	TAILQ_HEAD_INITIALIZER(g_spdk_nvme_transports);

struct spdk_nvme_transport g_spdk_transports[SPDK_MAX_NUM_OF_TRANSPORTS] = {};
int g_current_transport_index = 0;

const struct spdk_nvme_transport *
nvme_get_first_transport(void)
{
	return TAILQ_FIRST(&g_spdk_nvme_transports);
}

const struct spdk_nvme_transport *
nvme_get_next_transport(const struct spdk_nvme_transport *transport)
{
	return TAILQ_NEXT(transport, link);
}

/*
 * Unfortunately, due to NVMe PCIe multiprocess support, we cannot store the
 * transport object in either the controller struct or the admin qpair. THis means
 * that a lot of admin related transport calls will have to call nvme_get_transport
 * in order to knwo which functions to call.
 * In the I/O path, we have the ability to store the transport struct in the I/O
 * qpairs to avoid taking a performance hit.
 */
const struct spdk_nvme_transport *
nvme_get_transport(const char *transport_name)
{
	struct spdk_nvme_transport *registered_transport;

	TAILQ_FOREACH(registered_transport, &g_spdk_nvme_transports, link) {
		if (strcasecmp(transport_name, registered_transport->ops.name) == 0) {
			return registered_transport;
		}
	}

	return NULL;
}

bool
spdk_nvme_transport_available(enum spdk_nvme_transport_type trtype)
{
	return nvme_get_transport(spdk_nvme_transport_id_trtype_str(trtype)) == NULL ? false : true;
}

bool
spdk_nvme_transport_available_by_name(const char *transport_name)
{
	return nvme_get_transport(transport_name) == NULL ? false : true;
}

void spdk_nvme_transport_register(const struct spdk_nvme_transport_ops *ops)
{
	struct spdk_nvme_transport *new_transport;

	if (nvme_get_transport(ops->name)) {
		SPDK_ERRLOG("Double registering NVMe transport %s is prohibited.\n", ops->name);
		assert(false);
	}

	if (g_current_transport_index == SPDK_MAX_NUM_OF_TRANSPORTS) {
		SPDK_ERRLOG("Unable to register new NVMe transport.\n");
		assert(false);
		return;
	}
	new_transport = &g_spdk_transports[g_current_transport_index++];

	new_transport->ops = *ops;
	TAILQ_INSERT_TAIL(&g_spdk_nvme_transports, new_transport, link);
}

struct spdk_nvme_ctrlr *nvme_transport_ctrlr_construct(const struct spdk_nvme_transport_id *trid,
		const struct spdk_nvme_ctrlr_opts *opts,
		void *devhandle)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(trid->trstring);
	struct spdk_nvme_ctrlr *ctrlr;

	if (transport == NULL) {
		SPDK_ERRLOG("Transport %s doesn't exist.", trid->trstring);
		return NULL;
	}

	ctrlr = transport->ops.ctrlr_construct(trid, opts, devhandle);

	return ctrlr;
}

int
nvme_transport_ctrlr_scan(struct spdk_nvme_probe_ctx *probe_ctx,
			  bool direct_connect)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(probe_ctx->trid.trstring);

	if (transport == NULL) {
		SPDK_ERRLOG("Transport %s doesn't exist.", probe_ctx->trid.trstring);
		return -ENOENT;
	}

	return transport->ops.ctrlr_scan(probe_ctx, direct_connect);
}

int
nvme_transport_ctrlr_destruct(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	return transport->ops.ctrlr_destruct(ctrlr);
}

int
nvme_transport_ctrlr_enable(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	return transport->ops.ctrlr_enable(ctrlr);
}

int
nvme_transport_ctrlr_set_reg_4(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint32_t value)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	return transport->ops.ctrlr_set_reg_4(ctrlr, offset, value);
}

int
nvme_transport_ctrlr_set_reg_8(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint64_t value)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	return transport->ops.ctrlr_set_reg_8(ctrlr, offset, value);
}

int
nvme_transport_ctrlr_get_reg_4(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint32_t *value)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	return transport->ops.ctrlr_get_reg_4(ctrlr, offset, value);
}

int
nvme_transport_ctrlr_get_reg_8(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint64_t *value)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	return transport->ops.ctrlr_get_reg_8(ctrlr, offset, value);
}

uint32_t
nvme_transport_ctrlr_get_max_xfer_size(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	return transport->ops.ctrlr_get_max_xfer_size(ctrlr);
}

uint16_t
nvme_transport_ctrlr_get_max_sges(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	return transport->ops.ctrlr_get_max_sges(ctrlr);
}

int
nvme_transport_ctrlr_reserve_cmb(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	if (transport->ops.ctrlr_reserve_cmb != NULL) {
		return transport->ops.ctrlr_reserve_cmb(ctrlr);
	}

	return -ENOTSUP;
}

void *
nvme_transport_ctrlr_map_cmb(struct spdk_nvme_ctrlr *ctrlr, size_t *size)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	if (transport->ops.ctrlr_map_cmb != NULL) {
		return transport->ops.ctrlr_map_cmb(ctrlr, size);
	}

	return NULL;
}

int
nvme_transport_ctrlr_unmap_cmb(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	if (transport->ops.ctrlr_unmap_cmb != NULL) {
		return transport->ops.ctrlr_unmap_cmb(ctrlr);
	}

	return 0;
}

int
nvme_transport_ctrlr_enable_pmr(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	if (transport->ops.ctrlr_enable_pmr != NULL) {
		return transport->ops.ctrlr_enable_pmr(ctrlr);
	}

	return -ENOSYS;
}

int
nvme_transport_ctrlr_disable_pmr(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	if (transport->ops.ctrlr_disable_pmr != NULL) {
		return transport->ops.ctrlr_disable_pmr(ctrlr);
	}

	return -ENOSYS;
}

void *
nvme_transport_ctrlr_map_pmr(struct spdk_nvme_ctrlr *ctrlr, size_t *size)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	if (transport->ops.ctrlr_map_pmr != NULL) {
		return transport->ops.ctrlr_map_pmr(ctrlr, size);
	}

	return NULL;
}

int
nvme_transport_ctrlr_unmap_pmr(struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	if (transport->ops.ctrlr_unmap_pmr != NULL) {
		return transport->ops.ctrlr_unmap_pmr(ctrlr);
	}

	return -ENOSYS;
}

struct spdk_nvme_qpair *
nvme_transport_ctrlr_create_io_qpair(struct spdk_nvme_ctrlr *ctrlr, uint16_t qid,
				     const struct spdk_nvme_io_qpair_opts *opts)
{
	struct spdk_nvme_qpair *qpair;
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);
	qpair = transport->ops.ctrlr_create_io_qpair(ctrlr, qid, opts);
	if (qpair != NULL && !nvme_qpair_is_admin_queue(qpair)) {
		qpair->transport = transport;
	}

	return qpair;
}

int
nvme_transport_ctrlr_delete_io_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	assert(transport != NULL);

	/* Do not rely on qpair->transport.  For multi-process cases, a foreign process may delete
	 * the IO qpair, in which case the transport object would be invalid (each process has their
	 * own unique transport objects since they contain function pointers).  So we look up the
	 * transport object in the delete_io_qpair case.
	 */
	return transport->ops.ctrlr_delete_io_qpair(ctrlr, qpair);
}

int
nvme_transport_ctrlr_connect_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);
	uint8_t transport_failure_reason;
	int rc;

	assert(transport != NULL);
	if (!nvme_qpair_is_admin_queue(qpair)) {
		qpair->transport = transport;
	}

	transport_failure_reason = qpair->transport_failure_reason;
	qpair->transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_NONE;

	nvme_qpair_set_state(qpair, NVME_QPAIR_CONNECTING);
	rc = transport->ops.ctrlr_connect_qpair(ctrlr, qpair);
	if (rc != 0) {
		goto err;
	}

	nvme_qpair_set_state(qpair, NVME_QPAIR_CONNECTED);
	if (qpair->poll_group) {
		rc = nvme_poll_group_connect_qpair(qpair);
		if (rc) {
			goto err;
		}
	}

	return rc;

err:
	/* If the qpair was unable to reconnect, restore the original failure reason. */
	qpair->transport_failure_reason = transport_failure_reason;
	nvme_transport_ctrlr_disconnect_qpair(ctrlr, qpair);
	nvme_qpair_set_state(qpair, NVME_QPAIR_DISCONNECTED);
	return rc;
}

void
nvme_transport_ctrlr_disconnect_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(ctrlr->trid.trstring);

	if (nvme_qpair_get_state(qpair) == NVME_QPAIR_DISCONNECTING ||
	    nvme_qpair_get_state(qpair) == NVME_QPAIR_DISCONNECTED) {
		return;
	}

	nvme_qpair_set_state(qpair, NVME_QPAIR_DISCONNECTING);
	assert(transport != NULL);
	if (qpair->poll_group) {
		nvme_poll_group_disconnect_qpair(qpair);
	}

	transport->ops.ctrlr_disconnect_qpair(ctrlr, qpair);

	nvme_qpair_abort_reqs(qpair, 0);
	nvme_qpair_set_state(qpair, NVME_QPAIR_DISCONNECTED);
}

void
nvme_transport_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr)
{
	const struct spdk_nvme_transport *transport;

	assert(dnr <= 1);
	if (spdk_likely(!nvme_qpair_is_admin_queue(qpair))) {
		qpair->transport->ops.qpair_abort_reqs(qpair, dnr);
	} else {
		transport = nvme_get_transport(qpair->ctrlr->trid.trstring);
		assert(transport != NULL);
		transport->ops.qpair_abort_reqs(qpair, dnr);
	}
}

int
nvme_transport_qpair_reset(struct spdk_nvme_qpair *qpair)
{
	const struct spdk_nvme_transport *transport;

	if (spdk_likely(!nvme_qpair_is_admin_queue(qpair))) {
		return qpair->transport->ops.qpair_reset(qpair);
	}

	transport = nvme_get_transport(qpair->ctrlr->trid.trstring);
	assert(transport != NULL);
	return transport->ops.qpair_reset(qpair);
}

int
nvme_transport_qpair_submit_request(struct spdk_nvme_qpair *qpair, struct nvme_request *req)
{
	const struct spdk_nvme_transport *transport;

	if (spdk_likely(!nvme_qpair_is_admin_queue(qpair))) {
		return qpair->transport->ops.qpair_submit_request(qpair, req);
	}

	transport = nvme_get_transport(qpair->ctrlr->trid.trstring);
	assert(transport != NULL);
	return transport->ops.qpair_submit_request(qpair, req);
}

int32_t
nvme_transport_qpair_process_completions(struct spdk_nvme_qpair *qpair, uint32_t max_completions)
{
	const struct spdk_nvme_transport *transport;

	if (spdk_likely(!nvme_qpair_is_admin_queue(qpair))) {
		return qpair->transport->ops.qpair_process_completions(qpair, max_completions);
	}

	transport = nvme_get_transport(qpair->ctrlr->trid.trstring);
	assert(transport != NULL);
	return transport->ops.qpair_process_completions(qpair, max_completions);
}

int
nvme_transport_qpair_iterate_requests(struct spdk_nvme_qpair *qpair,
				      int (*iter_fn)(struct nvme_request *req, void *arg),
				      void *arg)
{
	const struct spdk_nvme_transport *transport;

	if (spdk_likely(!nvme_qpair_is_admin_queue(qpair))) {
		return qpair->transport->ops.qpair_iterate_requests(qpair, iter_fn, arg);
	}

	transport = nvme_get_transport(qpair->ctrlr->trid.trstring);
	assert(transport != NULL);
	return transport->ops.qpair_iterate_requests(qpair, iter_fn, arg);
}

void
nvme_transport_admin_qpair_abort_aers(struct spdk_nvme_qpair *qpair)
{
	const struct spdk_nvme_transport *transport = nvme_get_transport(qpair->ctrlr->trid.trstring);

	assert(transport != NULL);
	transport->ops.admin_qpair_abort_aers(qpair);
}

struct spdk_nvme_transport_poll_group *
nvme_transport_poll_group_create(const struct spdk_nvme_transport *transport)
{
	struct spdk_nvme_transport_poll_group *group = NULL;

	group = transport->ops.poll_group_create();
	if (group) {
		group->transport = transport;
		STAILQ_INIT(&group->connected_qpairs);
		STAILQ_INIT(&group->disconnected_qpairs);
	}

	return group;
}

struct spdk_nvme_transport_poll_group *
nvme_transport_qpair_get_optimal_poll_group(const struct spdk_nvme_transport *transport,
		struct spdk_nvme_qpair *qpair)
{
	if (transport->ops.qpair_get_optimal_poll_group) {
		return transport->ops.qpair_get_optimal_poll_group(qpair);
	} else {
		return NULL;
	}
}

int
nvme_transport_poll_group_add(struct spdk_nvme_transport_poll_group *tgroup,
			      struct spdk_nvme_qpair *qpair)
{
	int rc;

	rc = tgroup->transport->ops.poll_group_add(tgroup, qpair);
	if (rc == 0) {
		qpair->poll_group = tgroup;
		assert(nvme_qpair_get_state(qpair) < NVME_QPAIR_CONNECTED);
		qpair->poll_group_tailq_head = &tgroup->disconnected_qpairs;
		STAILQ_INSERT_TAIL(&tgroup->disconnected_qpairs, qpair, poll_group_stailq);
	}

	return rc;
}

int
nvme_transport_poll_group_remove(struct spdk_nvme_transport_poll_group *tgroup,
				 struct spdk_nvme_qpair *qpair)
{
	int rc;

	rc = tgroup->transport->ops.poll_group_remove(tgroup, qpair);
	if (rc == 0) {
		if (qpair->poll_group_tailq_head == &tgroup->connected_qpairs) {
			STAILQ_REMOVE(&tgroup->connected_qpairs, qpair, spdk_nvme_qpair, poll_group_stailq);
		} else if (qpair->poll_group_tailq_head == &tgroup->disconnected_qpairs) {
			STAILQ_REMOVE(&tgroup->disconnected_qpairs, qpair, spdk_nvme_qpair, poll_group_stailq);
		} else {
			return -ENOENT;
		}

		qpair->poll_group = NULL;
		qpair->poll_group_tailq_head = NULL;
	}

	return rc;
}

int64_t
nvme_transport_poll_group_process_completions(struct spdk_nvme_transport_poll_group *tgroup,
		uint32_t completions_per_qpair, spdk_nvme_disconnected_qpair_cb disconnected_qpair_cb)
{
	struct spdk_nvme_qpair *qpair;
	int64_t rc;

	tgroup->in_completion_context = true;
	rc = tgroup->transport->ops.poll_group_process_completions(tgroup, completions_per_qpair,
			disconnected_qpair_cb);
	tgroup->in_completion_context = false;

	if (spdk_unlikely(tgroup->num_qpairs_to_delete > 0)) {
		/* deleted qpairs are more likely to be in the disconnected qpairs list. */
		STAILQ_FOREACH(qpair, &tgroup->disconnected_qpairs, poll_group_stailq) {
			if (spdk_unlikely(qpair->delete_after_completion_context)) {
				spdk_nvme_ctrlr_free_io_qpair(qpair);
				if (--tgroup->num_qpairs_to_delete == 0) {
					return rc;
				}
			}
		}

		STAILQ_FOREACH(qpair, &tgroup->connected_qpairs, poll_group_stailq) {
			if (spdk_unlikely(qpair->delete_after_completion_context)) {
				spdk_nvme_ctrlr_free_io_qpair(qpair);
				if (--tgroup->num_qpairs_to_delete == 0) {
					return rc;
				}
			}
		}
		/* Just in case. */
		SPDK_DEBUGLOG(nvme, "Mismatch between qpairs to delete and poll group number.\n");
		tgroup->num_qpairs_to_delete = 0;
	}

	return rc;
}

int
nvme_transport_poll_group_destroy(struct spdk_nvme_transport_poll_group *tgroup)
{
	return tgroup->transport->ops.poll_group_destroy(tgroup);
}

int
nvme_transport_poll_group_disconnect_qpair(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_transport_poll_group *tgroup;
	int rc;

	tgroup = qpair->poll_group;

	if (qpair->poll_group_tailq_head == &tgroup->disconnected_qpairs) {
		return 0;
	}

	if (qpair->poll_group_tailq_head == &tgroup->connected_qpairs) {
		rc = tgroup->transport->ops.poll_group_disconnect_qpair(qpair);
		if (rc == 0) {
			qpair->poll_group_tailq_head = &tgroup->disconnected_qpairs;
			STAILQ_REMOVE(&tgroup->connected_qpairs, qpair, spdk_nvme_qpair, poll_group_stailq);
			STAILQ_INSERT_TAIL(&tgroup->disconnected_qpairs, qpair, poll_group_stailq);
			/* EINPROGRESS indicates that a call has already been made to this function.
			 * It just keeps us from segfaulting on a double removal/insert.
			 */
		}

		return rc == -EINPROGRESS ? 0 : rc;
	}

	return -EINVAL;
}

int
nvme_transport_poll_group_connect_qpair(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_transport_poll_group *tgroup;
	int rc;

	tgroup = qpair->poll_group;

	if (qpair->poll_group_tailq_head == &tgroup->connected_qpairs) {
		return 0;
	}

	if (qpair->poll_group_tailq_head == &tgroup->disconnected_qpairs) {
		rc = tgroup->transport->ops.poll_group_connect_qpair(qpair);
		if (rc == 0) {
			qpair->poll_group_tailq_head = &tgroup->connected_qpairs;
			STAILQ_REMOVE(&tgroup->disconnected_qpairs, qpair, spdk_nvme_qpair, poll_group_stailq);
			STAILQ_INSERT_TAIL(&tgroup->connected_qpairs, qpair, poll_group_stailq);
		}

		return rc == -EINPROGRESS ? 0 : rc;
	}


	return -EINVAL;
}

int
nvme_transport_poll_group_get_stats(struct spdk_nvme_transport_poll_group *tgroup,
				    struct spdk_nvme_transport_poll_group_stat **stats)
{
	if (tgroup->transport->ops.poll_group_get_stats) {
		return tgroup->transport->ops.poll_group_get_stats(tgroup, stats);
	}
	return -ENOTSUP;
}

void
nvme_transport_poll_group_free_stats(struct spdk_nvme_transport_poll_group *tgroup,
				     struct spdk_nvme_transport_poll_group_stat *stats)
{
	if (tgroup->transport->ops.poll_group_free_stats) {
		tgroup->transport->ops.poll_group_free_stats(tgroup, stats);
	}
}

enum spdk_nvme_transport_type nvme_transport_get_trtype(const struct spdk_nvme_transport *transport)
{
	return transport->ops.type;
}
