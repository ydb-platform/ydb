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

#include "spdk/stdinc.h"

#include "nvme_internal.h"
#include "nvme_io_msg.h"

#include "spdk/env.h"
#include "spdk/string.h"

struct nvme_active_ns_ctx;

static void nvme_ctrlr_destruct_namespaces(struct spdk_nvme_ctrlr *ctrlr);
static int nvme_ctrlr_construct_and_submit_aer(struct spdk_nvme_ctrlr *ctrlr,
		struct nvme_async_event_request *aer);
static void nvme_ctrlr_identify_active_ns_async(struct nvme_active_ns_ctx *ctx);
static int nvme_ctrlr_identify_ns_async(struct spdk_nvme_ns *ns);
static int nvme_ctrlr_identify_ns_iocs_specific_async(struct spdk_nvme_ns *ns);
static int nvme_ctrlr_identify_id_desc_async(struct spdk_nvme_ns *ns);

#define CTRLR_STRING(ctrlr) \
	((ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_TCP || ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_RDMA) ? \
	ctrlr->trid.subnqn : ctrlr->trid.traddr)

#define NVME_CTRLR_ERRLOG(ctrlr, format, ...) \
	SPDK_ERRLOG("[%s] " format, CTRLR_STRING(ctrlr), ##__VA_ARGS__);

#define NVME_CTRLR_WARNLOG(ctrlr, format, ...) \
	SPDK_WARNLOG("[%s] " format, CTRLR_STRING(ctrlr), ##__VA_ARGS__);

#define NVME_CTRLR_NOTICELOG(ctrlr, format, ...) \
	SPDK_NOTICELOG("[%s] " format, CTRLR_STRING(ctrlr), ##__VA_ARGS__);

#define NVME_CTRLR_INFOLOG(ctrlr, format, ...) \
	SPDK_INFOLOG(nvme, "[%s] " format, CTRLR_STRING(ctrlr), ##__VA_ARGS__);

#ifdef DEBUG
#define NVME_CTRLR_DEBUGLOG(ctrlr, format, ...) \
	SPDK_DEBUGLOG(nvme, "[%s] " format, CTRLR_STRING(ctrlr), ##__VA_ARGS__);
#else
#define NVME_CTRLR_DEBUGLOG(ctrlr, ...) do { } while (0)
#endif

static int
nvme_ctrlr_get_cc(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_cc_register *cc)
{
	return nvme_transport_ctrlr_get_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, cc.raw),
					      &cc->raw);
}

static int
nvme_ctrlr_get_csts(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_csts_register *csts)
{
	return nvme_transport_ctrlr_get_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, csts.raw),
					      &csts->raw);
}

int
nvme_ctrlr_get_cap(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_cap_register *cap)
{
	return nvme_transport_ctrlr_get_reg_8(ctrlr, offsetof(struct spdk_nvme_registers, cap.raw),
					      &cap->raw);
}

int
nvme_ctrlr_get_vs(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_vs_register *vs)
{
	return nvme_transport_ctrlr_get_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, vs.raw),
					      &vs->raw);
}

static int
nvme_ctrlr_set_cc(struct spdk_nvme_ctrlr *ctrlr, const union spdk_nvme_cc_register *cc)
{
	return nvme_transport_ctrlr_set_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, cc.raw),
					      cc->raw);
}

int
nvme_ctrlr_get_cmbsz(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_cmbsz_register *cmbsz)
{
	return nvme_transport_ctrlr_get_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, cmbsz.raw),
					      &cmbsz->raw);
}

int
nvme_ctrlr_get_pmrcap(struct spdk_nvme_ctrlr *ctrlr, union spdk_nvme_pmrcap_register *pmrcap)
{
	return nvme_transport_ctrlr_get_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, pmrcap.raw),
					      &pmrcap->raw);
}

static int
nvme_ctrlr_set_nssr(struct spdk_nvme_ctrlr *ctrlr, uint32_t nssr_value)
{
	return nvme_transport_ctrlr_set_reg_4(ctrlr, offsetof(struct spdk_nvme_registers, nssr),
					      nssr_value);
}

bool
nvme_ctrlr_multi_iocs_enabled(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->cap.bits.css & SPDK_NVME_CAP_CSS_IOCS &&
	       ctrlr->opts.command_set == SPDK_NVME_CC_CSS_IOCS;
}

/* When the field in spdk_nvme_ctrlr_opts are changed and you change this function, please
 * also update the nvme_ctrl_opts_init function in nvme_ctrlr.c
 */
void
spdk_nvme_ctrlr_get_default_ctrlr_opts(struct spdk_nvme_ctrlr_opts *opts, size_t opts_size)
{
	char host_id_str[SPDK_UUID_STRING_LEN];

	assert(opts);

	opts->opts_size = opts_size;

#define FIELD_OK(field) \
	offsetof(struct spdk_nvme_ctrlr_opts, field) + sizeof(opts->field) <= opts_size

#define SET_FIELD(field, value) \
	if (offsetof(struct spdk_nvme_ctrlr_opts, field) + sizeof(opts->field) <= opts_size) { \
		opts->field = value; \
	} \

	SET_FIELD(num_io_queues, DEFAULT_MAX_IO_QUEUES);
	SET_FIELD(use_cmb_sqs, true);
	SET_FIELD(no_shn_notification, false);
	SET_FIELD(arb_mechanism, SPDK_NVME_CC_AMS_RR);
	SET_FIELD(arbitration_burst, 0);
	SET_FIELD(low_priority_weight, 0);
	SET_FIELD(medium_priority_weight, 0);
	SET_FIELD(high_priority_weight, 0);
	SET_FIELD(keep_alive_timeout_ms, MIN_KEEP_ALIVE_TIMEOUT_IN_MS);
	SET_FIELD(transport_retry_count, SPDK_NVME_DEFAULT_RETRY_COUNT);
	SET_FIELD(io_queue_size, DEFAULT_IO_QUEUE_SIZE);

	if (nvme_driver_init() == 0) {
		if (FIELD_OK(hostnqn)) {
			spdk_uuid_fmt_lower(host_id_str, sizeof(host_id_str),
					    &g_spdk_nvme_driver->default_extended_host_id);
			snprintf(opts->hostnqn, sizeof(opts->hostnqn),
				 "nqn.2014-08.org.nvmexpress:uuid:%s", host_id_str);
		}

		if (FIELD_OK(extended_host_id)) {
			memcpy(opts->extended_host_id, &g_spdk_nvme_driver->default_extended_host_id,
			       sizeof(opts->extended_host_id));
		}

	}

	SET_FIELD(io_queue_requests, DEFAULT_IO_QUEUE_REQUESTS);

	if (FIELD_OK(src_addr)) {
		memset(opts->src_addr, 0, sizeof(opts->src_addr));
	}

	if (FIELD_OK(src_svcid)) {
		memset(opts->src_svcid, 0, sizeof(opts->src_svcid));
	}

	if (FIELD_OK(host_id)) {
		memset(opts->host_id, 0, sizeof(opts->host_id));
	}

	SET_FIELD(command_set, CHAR_BIT);
	SET_FIELD(admin_timeout_ms, NVME_MAX_ADMIN_TIMEOUT_IN_SECS * 1000);
	SET_FIELD(header_digest, false);
	SET_FIELD(data_digest, false);
	SET_FIELD(disable_error_logging, false);
	SET_FIELD(transport_ack_timeout, SPDK_NVME_DEFAULT_TRANSPORT_ACK_TIMEOUT);
	SET_FIELD(admin_queue_size, DEFAULT_ADMIN_QUEUE_SIZE);
	SET_FIELD(fabrics_connect_timeout_us, NVME_FABRIC_CONNECT_COMMAND_TIMEOUT);

#undef FIELD_OK
#undef SET_FIELD
}

/**
 * This function will be called when the process allocates the IO qpair.
 * Note: the ctrlr_lock must be held when calling this function.
 */
static void
nvme_ctrlr_proc_add_io_qpair(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_ctrlr_process	*active_proc;
	struct spdk_nvme_ctrlr		*ctrlr = qpair->ctrlr;

	active_proc = nvme_ctrlr_get_current_process(ctrlr);
	if (active_proc) {
		TAILQ_INSERT_TAIL(&active_proc->allocated_io_qpairs, qpair, per_process_tailq);
		qpair->active_proc = active_proc;
	}
}

/**
 * This function will be called when the process frees the IO qpair.
 * Note: the ctrlr_lock must be held when calling this function.
 */
static void
nvme_ctrlr_proc_remove_io_qpair(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_ctrlr_process	*active_proc;
	struct spdk_nvme_ctrlr		*ctrlr = qpair->ctrlr;
	struct spdk_nvme_qpair          *active_qpair, *tmp_qpair;

	active_proc = nvme_ctrlr_get_current_process(ctrlr);
	if (!active_proc) {
		return;
	}

	TAILQ_FOREACH_SAFE(active_qpair, &active_proc->allocated_io_qpairs,
			   per_process_tailq, tmp_qpair) {
		if (active_qpair == qpair) {
			TAILQ_REMOVE(&active_proc->allocated_io_qpairs,
				     active_qpair, per_process_tailq);

			break;
		}
	}
}

void
spdk_nvme_ctrlr_get_default_io_qpair_opts(struct spdk_nvme_ctrlr *ctrlr,
		struct spdk_nvme_io_qpair_opts *opts,
		size_t opts_size)
{
	assert(ctrlr);

	assert(opts);

	memset(opts, 0, opts_size);

#define FIELD_OK(field) \
	offsetof(struct spdk_nvme_io_qpair_opts, field) + sizeof(opts->field) <= opts_size

	if (FIELD_OK(qprio)) {
		opts->qprio = SPDK_NVME_QPRIO_URGENT;
	}

	if (FIELD_OK(io_queue_size)) {
		opts->io_queue_size = ctrlr->opts.io_queue_size;
	}

	if (FIELD_OK(io_queue_requests)) {
		opts->io_queue_requests = ctrlr->opts.io_queue_requests;
	}

	if (FIELD_OK(delay_cmd_submit)) {
		opts->delay_cmd_submit = false;
	}

	if (FIELD_OK(sq.vaddr)) {
		opts->sq.vaddr = NULL;
	}

	if (FIELD_OK(sq.paddr)) {
		opts->sq.paddr = 0;
	}

	if (FIELD_OK(sq.buffer_size)) {
		opts->sq.buffer_size = 0;
	}

	if (FIELD_OK(cq.vaddr)) {
		opts->cq.vaddr = NULL;
	}

	if (FIELD_OK(cq.paddr)) {
		opts->cq.paddr = 0;
	}

	if (FIELD_OK(cq.buffer_size)) {
		opts->cq.buffer_size = 0;
	}

	if (FIELD_OK(create_only)) {
		opts->create_only = false;
	}

#undef FIELD_OK
}

static struct spdk_nvme_qpair *
nvme_ctrlr_create_io_qpair(struct spdk_nvme_ctrlr *ctrlr,
			   const struct spdk_nvme_io_qpair_opts *opts)
{
	int32_t					qid;
	struct spdk_nvme_qpair			*qpair;
	union spdk_nvme_cc_register		cc;

	if (!ctrlr) {
		return NULL;
	}

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	if (nvme_ctrlr_get_cc(ctrlr, &cc)) {
		NVME_CTRLR_ERRLOG(ctrlr, "get_cc failed\n");
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return NULL;
	}

	if (opts->qprio & ~SPDK_NVME_CREATE_IO_SQ_QPRIO_MASK) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return NULL;
	}

	/*
	 * Only value SPDK_NVME_QPRIO_URGENT(0) is valid for the
	 * default round robin arbitration method.
	 */
	if ((cc.bits.ams == SPDK_NVME_CC_AMS_RR) && (opts->qprio != SPDK_NVME_QPRIO_URGENT)) {
		NVME_CTRLR_ERRLOG(ctrlr, "invalid queue priority for default round robin arbitration method\n");
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return NULL;
	}

	qid = spdk_nvme_ctrlr_alloc_qid(ctrlr);
	if (qid < 0) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return NULL;
	}

	qpair = nvme_transport_ctrlr_create_io_qpair(ctrlr, qid, opts);
	if (qpair == NULL) {
		NVME_CTRLR_ERRLOG(ctrlr, "nvme_transport_ctrlr_create_io_qpair() failed\n");
		spdk_nvme_ctrlr_free_qid(ctrlr, qid);
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return NULL;
	}

	TAILQ_INSERT_TAIL(&ctrlr->active_io_qpairs, qpair, tailq);

	nvme_ctrlr_proc_add_io_qpair(qpair);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return qpair;
}

int
spdk_nvme_ctrlr_connect_io_qpair(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair)
{
	int rc;

	if (nvme_qpair_get_state(qpair) != NVME_QPAIR_DISCONNECTED) {
		return -EISCONN;
	}

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	rc = nvme_transport_ctrlr_connect_qpair(ctrlr, qpair);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	if (ctrlr->quirks & NVME_QUIRK_DELAY_AFTER_QUEUE_ALLOC) {
		spdk_delay_us(100);
	}

	return rc;
}

void
spdk_nvme_ctrlr_disconnect_io_qpair(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_ctrlr *ctrlr = qpair->ctrlr;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	nvme_transport_ctrlr_disconnect_qpair(ctrlr, qpair);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

struct spdk_nvme_qpair *
spdk_nvme_ctrlr_alloc_io_qpair(struct spdk_nvme_ctrlr *ctrlr,
			       const struct spdk_nvme_io_qpair_opts *user_opts,
			       size_t opts_size)
{

	struct spdk_nvme_qpair		*qpair;
	struct spdk_nvme_io_qpair_opts	opts;
	int				rc;

	/*
	 * Get the default options, then overwrite them with the user-provided options
	 * up to opts_size.
	 *
	 * This allows for extensions of the opts structure without breaking
	 * ABI compatibility.
	 */
	spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
	if (user_opts) {
		memcpy(&opts, user_opts, spdk_min(sizeof(opts), opts_size));

		/* If user passes buffers, make sure they're big enough for the requested queue size */
		if (opts.sq.vaddr) {
			if (opts.sq.buffer_size < (opts.io_queue_size * sizeof(struct spdk_nvme_cmd))) {
				NVME_CTRLR_ERRLOG(ctrlr, "sq buffer size %" PRIx64 " is too small for sq size %zx\n",
						  opts.sq.buffer_size, (opts.io_queue_size * sizeof(struct spdk_nvme_cmd)));
				return NULL;
			}
		}
		if (opts.cq.vaddr) {
			if (opts.cq.buffer_size < (opts.io_queue_size * sizeof(struct spdk_nvme_cpl))) {
				NVME_CTRLR_ERRLOG(ctrlr, "cq buffer size %" PRIx64 " is too small for cq size %zx\n",
						  opts.cq.buffer_size, (opts.io_queue_size * sizeof(struct spdk_nvme_cpl)));
				return NULL;
			}
		}
	}

	qpair = nvme_ctrlr_create_io_qpair(ctrlr, &opts);

	if (qpair == NULL || opts.create_only == true) {
		return qpair;
	}

	rc = spdk_nvme_ctrlr_connect_io_qpair(ctrlr, qpair);
	if (rc != 0) {
		NVME_CTRLR_ERRLOG(ctrlr, "nvme_transport_ctrlr_connect_io_qpair() failed\n");
		nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
		nvme_ctrlr_proc_remove_io_qpair(qpair);
		TAILQ_REMOVE(&ctrlr->active_io_qpairs, qpair, tailq);
		spdk_bit_array_set(ctrlr->free_io_qids, qpair->id);
		nvme_transport_ctrlr_delete_io_qpair(ctrlr, qpair);
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return NULL;
	}

	return qpair;
}

int
spdk_nvme_ctrlr_reconnect_io_qpair(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_ctrlr *ctrlr;
	enum nvme_qpair_state qpair_state;
	int rc;

	assert(qpair != NULL);
	assert(nvme_qpair_is_admin_queue(qpair) == false);
	assert(qpair->ctrlr != NULL);

	ctrlr = qpair->ctrlr;
	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	qpair_state = nvme_qpair_get_state(qpair);

	if (ctrlr->is_removed) {
		rc = -ENODEV;
		goto out;
	}

	if (ctrlr->is_resetting || qpair_state == NVME_QPAIR_DISCONNECTING) {
		rc = -EAGAIN;
		goto out;
	}

	if (ctrlr->is_failed || qpair_state == NVME_QPAIR_DESTROYING) {
		rc = -ENXIO;
		goto out;
	}

	if (qpair_state != NVME_QPAIR_DISCONNECTED) {
		rc = 0;
		goto out;
	}

	rc = nvme_transport_ctrlr_connect_qpair(ctrlr, qpair);
	if (rc) {
		rc = -EAGAIN;
		goto out;
	}

out:
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

spdk_nvme_qp_failure_reason
spdk_nvme_ctrlr_get_admin_qp_failure_reason(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->adminq->transport_failure_reason;
}

/*
 * This internal function will attempt to take the controller
 * lock before calling disconnect on a controller qpair.
 * Functions already holding the controller lock should
 * call nvme_transport_ctrlr_disconnect_qpair directly.
 */
void
nvme_ctrlr_disconnect_qpair(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_ctrlr *ctrlr = qpair->ctrlr;

	assert(ctrlr != NULL);
	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	nvme_transport_ctrlr_disconnect_qpair(ctrlr, qpair);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

int
spdk_nvme_ctrlr_free_io_qpair(struct spdk_nvme_qpair *qpair)
{
	struct spdk_nvme_ctrlr *ctrlr;

	if (qpair == NULL) {
		return 0;
	}

	ctrlr = qpair->ctrlr;

	if (qpair->in_completion_context) {
		/*
		 * There are many cases where it is convenient to delete an io qpair in the context
		 *  of that qpair's completion routine.  To handle this properly, set a flag here
		 *  so that the completion routine will perform an actual delete after the context
		 *  unwinds.
		 */
		qpair->delete_after_completion_context = 1;
		return 0;
	}

	if (qpair->poll_group && qpair->poll_group->in_completion_context) {
		/* Same as above, but in a poll group. */
		qpair->poll_group->num_qpairs_to_delete++;
		qpair->delete_after_completion_context = 1;
		return 0;
	}

	if (qpair->poll_group) {
		spdk_nvme_poll_group_remove(qpair->poll_group->group, qpair);
	}

	/* Do not retry. */
	nvme_qpair_set_state(qpair, NVME_QPAIR_DESTROYING);

	/* In the multi-process case, a process may call this function on a foreign
	 * I/O qpair (i.e. one that this process did not create) when that qpairs process
	 * exits unexpectedly.  In that case, we must not try to abort any reqs associated
	 * with that qpair, since the callbacks will also be foreign to this process.
	 */
	if (qpair->active_proc == nvme_ctrlr_get_current_process(ctrlr)) {
		nvme_qpair_abort_reqs(qpair, 1);
	}

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	nvme_ctrlr_proc_remove_io_qpair(qpair);

	TAILQ_REMOVE(&ctrlr->active_io_qpairs, qpair, tailq);
	spdk_nvme_ctrlr_free_qid(ctrlr, qpair->id);

	if (nvme_transport_ctrlr_delete_io_qpair(ctrlr, qpair)) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -1;
	}

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return 0;
}

static void
nvme_ctrlr_construct_intel_support_log_page_list(struct spdk_nvme_ctrlr *ctrlr,
		struct spdk_nvme_intel_log_page_directory *log_page_directory)
{
	if (log_page_directory == NULL) {
		return;
	}

	if (ctrlr->cdata.vid != SPDK_PCI_VID_INTEL) {
		return;
	}

	ctrlr->log_page_supported[SPDK_NVME_INTEL_LOG_PAGE_DIRECTORY] = true;

	if (log_page_directory->read_latency_log_len ||
	    (ctrlr->quirks & NVME_INTEL_QUIRK_READ_LATENCY)) {
		ctrlr->log_page_supported[SPDK_NVME_INTEL_LOG_READ_CMD_LATENCY] = true;
	}
	if (log_page_directory->write_latency_log_len ||
	    (ctrlr->quirks & NVME_INTEL_QUIRK_WRITE_LATENCY)) {
		ctrlr->log_page_supported[SPDK_NVME_INTEL_LOG_WRITE_CMD_LATENCY] = true;
	}
	if (log_page_directory->temperature_statistics_log_len) {
		ctrlr->log_page_supported[SPDK_NVME_INTEL_LOG_TEMPERATURE] = true;
	}
	if (log_page_directory->smart_log_len) {
		ctrlr->log_page_supported[SPDK_NVME_INTEL_LOG_SMART] = true;
	}
	if (log_page_directory->marketing_description_log_len) {
		ctrlr->log_page_supported[SPDK_NVME_INTEL_MARKETING_DESCRIPTION] = true;
	}
}

static int nvme_ctrlr_set_intel_support_log_pages(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc = 0;
	struct nvme_completion_poll_status	*status;
	struct spdk_nvme_intel_log_page_directory *log_page_directory;

	log_page_directory = spdk_zmalloc(sizeof(struct spdk_nvme_intel_log_page_directory),
					  64, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	if (log_page_directory == NULL) {
		NVME_CTRLR_ERRLOG(ctrlr, "could not allocate log_page_directory\n");
		return -ENXIO;
	}

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		spdk_free(log_page_directory);
		return -ENOMEM;
	}

	rc = spdk_nvme_ctrlr_cmd_get_log_page(ctrlr, SPDK_NVME_INTEL_LOG_PAGE_DIRECTORY,
					      SPDK_NVME_GLOBAL_NS_TAG, log_page_directory,
					      sizeof(struct spdk_nvme_intel_log_page_directory),
					      0, nvme_completion_poll_cb, status);
	if (rc != 0) {
		spdk_free(log_page_directory);
		free(status);
		return rc;
	}

	if (nvme_wait_for_completion_timeout(ctrlr->adminq, status,
					     ctrlr->opts.admin_timeout_ms * 1000)) {
		spdk_free(log_page_directory);
		NVME_CTRLR_WARNLOG(ctrlr, "Intel log pages not supported on Intel drive!\n");
		if (!status->timed_out) {
			free(status);
		}
		return 0;
	}

	nvme_ctrlr_construct_intel_support_log_page_list(ctrlr, log_page_directory);
	spdk_free(log_page_directory);
	free(status);
	return 0;
}

static int
nvme_ctrlr_update_ana_log_page(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_completion_poll_status *status;
	int rc;

	status = calloc(1, sizeof(*status));
	if (status == NULL) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	rc = spdk_nvme_ctrlr_cmd_get_log_page(ctrlr, SPDK_NVME_LOG_ASYMMETRIC_NAMESPACE_ACCESS,
					      SPDK_NVME_GLOBAL_NS_TAG, ctrlr->ana_log_page,
					      ctrlr->ana_log_page_size, 0,
					      nvme_completion_poll_cb, status);
	if (rc != 0) {
		free(status);
		return rc;
	}

	if (nvme_wait_for_completion_robust_lock_timeout(ctrlr->adminq, status, &ctrlr->ctrlr_lock,
			ctrlr->opts.admin_timeout_ms * 1000)) {
		if (!status->timed_out) {
			free(status);
		}
		return -EIO;
	}

	free(status);
	return 0;
}

static int
nvme_ctrlr_init_ana_log_page(struct spdk_nvme_ctrlr *ctrlr)
{
	uint32_t ana_log_page_size;

	ana_log_page_size = sizeof(struct spdk_nvme_ana_page) + ctrlr->cdata.nanagrpid *
			    sizeof(struct spdk_nvme_ana_group_descriptor) + ctrlr->cdata.nn *
			    sizeof(uint32_t);

	ctrlr->ana_log_page = spdk_zmalloc(ana_log_page_size, 64, NULL, SPDK_ENV_SOCKET_ID_ANY,
					   SPDK_MALLOC_DMA);
	if (ctrlr->ana_log_page == NULL) {
		NVME_CTRLR_ERRLOG(ctrlr, "could not allocate ANA log page buffer\n");
		return -ENXIO;
	}
	ctrlr->ana_log_page_size = ana_log_page_size;

	ctrlr->log_page_supported[SPDK_NVME_LOG_ASYMMETRIC_NAMESPACE_ACCESS] = true;

	return nvme_ctrlr_update_ana_log_page(ctrlr);
}

static int
nvme_ctrlr_update_ns_ana_states(const struct spdk_nvme_ana_group_descriptor *desc,
				void *cb_arg)
{
	struct spdk_nvme_ctrlr *ctrlr = cb_arg;
	struct spdk_nvme_ns *ns;
	uint32_t i, nsid;

	for (i = 0; i < desc->num_of_nsid; i++) {
		nsid = desc->nsid[i];
		if (nsid == 0 || nsid > ctrlr->cdata.nn) {
			continue;
		}

		ns = &ctrlr->ns[nsid - 1];

		ns->ana_group_id = desc->ana_group_id;
		ns->ana_state = desc->ana_state;
	}

	return 0;
}

int
nvme_ctrlr_parse_ana_log_page(struct spdk_nvme_ctrlr *ctrlr,
			      spdk_nvme_parse_ana_log_page_cb cb_fn, void *cb_arg)
{
	struct spdk_nvme_ana_group_descriptor *desc;
	uint32_t i;
	int rc = 0;

	if (ctrlr->ana_log_page == NULL) {
		return -EINVAL;
	}

	desc = (void *)((uint8_t *)ctrlr->ana_log_page + sizeof(struct spdk_nvme_ana_page));

	for (i = 0; i < ctrlr->ana_log_page->num_ana_group_desc; i++) {
		rc = cb_fn(desc, cb_arg);
		if (rc != 0) {
			break;
		}
		desc = (void *)((uint8_t *)desc + sizeof(struct spdk_nvme_ana_group_descriptor) +
				desc->num_of_nsid * sizeof(uint32_t));
	}

	return rc;
}

static int
nvme_ctrlr_set_supported_log_pages(struct spdk_nvme_ctrlr *ctrlr)
{
	int	rc = 0;

	memset(ctrlr->log_page_supported, 0, sizeof(ctrlr->log_page_supported));
	/* Mandatory pages */
	ctrlr->log_page_supported[SPDK_NVME_LOG_ERROR] = true;
	ctrlr->log_page_supported[SPDK_NVME_LOG_HEALTH_INFORMATION] = true;
	ctrlr->log_page_supported[SPDK_NVME_LOG_FIRMWARE_SLOT] = true;
	if (ctrlr->cdata.lpa.celp) {
		ctrlr->log_page_supported[SPDK_NVME_LOG_COMMAND_EFFECTS_LOG] = true;
	}
	if (ctrlr->cdata.vid == SPDK_PCI_VID_INTEL && !(ctrlr->quirks & NVME_INTEL_QUIRK_NO_LOG_PAGES)) {
		rc = nvme_ctrlr_set_intel_support_log_pages(ctrlr);
		if (rc != 0) {
			goto out;
		}
	}
	if (ctrlr->cdata.cmic.ana_reporting) {
		rc = nvme_ctrlr_init_ana_log_page(ctrlr);
		if (rc == 0) {
			nvme_ctrlr_parse_ana_log_page(ctrlr, nvme_ctrlr_update_ns_ana_states,
						      ctrlr);
		}
	}

out:
	return rc;
}

static void
nvme_ctrlr_set_intel_supported_features(struct spdk_nvme_ctrlr *ctrlr)
{
	ctrlr->feature_supported[SPDK_NVME_INTEL_FEAT_MAX_LBA] = true;
	ctrlr->feature_supported[SPDK_NVME_INTEL_FEAT_NATIVE_MAX_LBA] = true;
	ctrlr->feature_supported[SPDK_NVME_INTEL_FEAT_POWER_GOVERNOR_SETTING] = true;
	ctrlr->feature_supported[SPDK_NVME_INTEL_FEAT_SMBUS_ADDRESS] = true;
	ctrlr->feature_supported[SPDK_NVME_INTEL_FEAT_LED_PATTERN] = true;
	ctrlr->feature_supported[SPDK_NVME_INTEL_FEAT_RESET_TIMED_WORKLOAD_COUNTERS] = true;
	ctrlr->feature_supported[SPDK_NVME_INTEL_FEAT_LATENCY_TRACKING] = true;
}

static void
nvme_ctrlr_set_arbitration_feature(struct spdk_nvme_ctrlr *ctrlr)
{
	uint32_t cdw11;
	struct nvme_completion_poll_status *status;

	if (ctrlr->opts.arbitration_burst == 0) {
		return;
	}

	if (ctrlr->opts.arbitration_burst > 7) {
		NVME_CTRLR_WARNLOG(ctrlr, "Valid arbitration burst values is from 0-7\n");
		return;
	}

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return;
	}

	cdw11 = ctrlr->opts.arbitration_burst;

	if (spdk_nvme_ctrlr_get_flags(ctrlr) & SPDK_NVME_CTRLR_WRR_SUPPORTED) {
		cdw11 |= (uint32_t)ctrlr->opts.low_priority_weight << 8;
		cdw11 |= (uint32_t)ctrlr->opts.medium_priority_weight << 16;
		cdw11 |= (uint32_t)ctrlr->opts.high_priority_weight << 24;
	}

	if (spdk_nvme_ctrlr_cmd_set_feature(ctrlr, SPDK_NVME_FEAT_ARBITRATION,
					    cdw11, 0, NULL, 0,
					    nvme_completion_poll_cb, status) < 0) {
		NVME_CTRLR_ERRLOG(ctrlr, "Set arbitration feature failed\n");
		free(status);
		return;
	}

	if (nvme_wait_for_completion_timeout(ctrlr->adminq, status,
					     ctrlr->opts.admin_timeout_ms * 1000)) {
		NVME_CTRLR_ERRLOG(ctrlr, "Timeout to set arbitration feature\n");
	}

	if (!status->timed_out) {
		free(status);
	}
}

static void
nvme_ctrlr_set_supported_features(struct spdk_nvme_ctrlr *ctrlr)
{
	memset(ctrlr->feature_supported, 0, sizeof(ctrlr->feature_supported));
	/* Mandatory features */
	ctrlr->feature_supported[SPDK_NVME_FEAT_ARBITRATION] = true;
	ctrlr->feature_supported[SPDK_NVME_FEAT_POWER_MANAGEMENT] = true;
	ctrlr->feature_supported[SPDK_NVME_FEAT_TEMPERATURE_THRESHOLD] = true;
	ctrlr->feature_supported[SPDK_NVME_FEAT_ERROR_RECOVERY] = true;
	ctrlr->feature_supported[SPDK_NVME_FEAT_NUMBER_OF_QUEUES] = true;
	ctrlr->feature_supported[SPDK_NVME_FEAT_INTERRUPT_COALESCING] = true;
	ctrlr->feature_supported[SPDK_NVME_FEAT_INTERRUPT_VECTOR_CONFIGURATION] = true;
	ctrlr->feature_supported[SPDK_NVME_FEAT_WRITE_ATOMICITY] = true;
	ctrlr->feature_supported[SPDK_NVME_FEAT_ASYNC_EVENT_CONFIGURATION] = true;
	/* Optional features */
	if (ctrlr->cdata.vwc.present) {
		ctrlr->feature_supported[SPDK_NVME_FEAT_VOLATILE_WRITE_CACHE] = true;
	}
	if (ctrlr->cdata.apsta.supported) {
		ctrlr->feature_supported[SPDK_NVME_FEAT_AUTONOMOUS_POWER_STATE_TRANSITION] = true;
	}
	if (ctrlr->cdata.hmpre) {
		ctrlr->feature_supported[SPDK_NVME_FEAT_HOST_MEM_BUFFER] = true;
	}
	if (ctrlr->cdata.vid == SPDK_PCI_VID_INTEL) {
		nvme_ctrlr_set_intel_supported_features(ctrlr);
	}

	nvme_ctrlr_set_arbitration_feature(ctrlr);
}

bool
spdk_nvme_ctrlr_is_failed(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->is_failed;
}

void
nvme_ctrlr_fail(struct spdk_nvme_ctrlr *ctrlr, bool hot_remove)
{
	/*
	 * Set the flag here and leave the work failure of qpairs to
	 * spdk_nvme_qpair_process_completions().
	 */
	if (hot_remove) {
		ctrlr->is_removed = true;
	}

	if (ctrlr->is_failed) {
		NVME_CTRLR_NOTICELOG(ctrlr, "already in failed state\n");
		return;
	}

	ctrlr->is_failed = true;
	nvme_transport_ctrlr_disconnect_qpair(ctrlr, ctrlr->adminq);
	NVME_CTRLR_ERRLOG(ctrlr, "in failed state.\n");
}

/**
 * This public API function will try to take the controller lock.
 * Any private functions being called from a thread already holding
 * the ctrlr lock should call nvme_ctrlr_fail directly.
 */
void
spdk_nvme_ctrlr_fail(struct spdk_nvme_ctrlr *ctrlr)
{
	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	nvme_ctrlr_fail(ctrlr, false);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

static void
nvme_ctrlr_shutdown_async(struct spdk_nvme_ctrlr *ctrlr,
			  struct nvme_ctrlr_detach_ctx *ctx)
{
	union spdk_nvme_cc_register	cc;

	if (ctrlr->is_removed) {
		ctx->shutdown_complete = true;
		return;
	}

	if (nvme_ctrlr_get_cc(ctrlr, &cc)) {
		NVME_CTRLR_ERRLOG(ctrlr, "get_cc() failed\n");
		ctx->shutdown_complete = true;
		return;
	}

	cc.bits.shn = SPDK_NVME_SHN_NORMAL;

	if (nvme_ctrlr_set_cc(ctrlr, &cc)) {
		NVME_CTRLR_ERRLOG(ctrlr, "set_cc() failed\n");
		ctx->shutdown_complete = true;
		return;
	}

	/*
	 * The NVMe specification defines RTD3E to be the time between
	 *  setting SHN = 1 until the controller will set SHST = 10b.
	 * If the device doesn't report RTD3 entry latency, or if it
	 *  reports RTD3 entry latency less than 10 seconds, pick
	 *  10 seconds as a reasonable amount of time to
	 *  wait before proceeding.
	 */
	NVME_CTRLR_DEBUGLOG(ctrlr, "RTD3E = %" PRIu32 " us\n", ctrlr->cdata.rtd3e);
	ctx->shutdown_timeout_ms = SPDK_CEIL_DIV(ctrlr->cdata.rtd3e, 1000);
	ctx->shutdown_timeout_ms = spdk_max(ctx->shutdown_timeout_ms, 10000);
	NVME_CTRLR_DEBUGLOG(ctrlr, "shutdown timeout = %" PRIu32 " ms\n", ctx->shutdown_timeout_ms);

	ctx->shutdown_start_tsc = spdk_get_ticks();
}

static int
nvme_ctrlr_shutdown_poll_async(struct spdk_nvme_ctrlr *ctrlr,
			       struct nvme_ctrlr_detach_ctx *ctx)
{
	union spdk_nvme_csts_register	csts;
	uint32_t			ms_waited;

	ms_waited = (spdk_get_ticks() - ctx->shutdown_start_tsc) * 1000 / spdk_get_ticks_hz();

	if (nvme_ctrlr_get_csts(ctrlr, &csts)) {
		NVME_CTRLR_ERRLOG(ctrlr, "get_csts() failed\n");
		return -EIO;
	}

	if (csts.bits.shst == SPDK_NVME_SHST_COMPLETE) {
		NVME_CTRLR_DEBUGLOG(ctrlr, "shutdown complete in %u milliseconds\n", ms_waited);
		return 0;
	}

	if (ms_waited < ctx->shutdown_timeout_ms) {
		return -EAGAIN;
	}

	NVME_CTRLR_ERRLOG(ctrlr, "did not shutdown within %u milliseconds\n",
			  ctx->shutdown_timeout_ms);
	if (ctrlr->quirks & NVME_QUIRK_SHST_COMPLETE) {
		NVME_CTRLR_ERRLOG(ctrlr, "likely due to shutdown handling in the VMWare emulated NVMe SSD\n");
	}

	return 0;
}

static int
nvme_ctrlr_enable(struct spdk_nvme_ctrlr *ctrlr)
{
	union spdk_nvme_cc_register	cc;
	int				rc;

	rc = nvme_transport_ctrlr_enable(ctrlr);
	if (rc != 0) {
		NVME_CTRLR_ERRLOG(ctrlr, "transport ctrlr_enable failed\n");
		return rc;
	}

	if (nvme_ctrlr_get_cc(ctrlr, &cc)) {
		NVME_CTRLR_ERRLOG(ctrlr, "get_cc() failed\n");
		return -EIO;
	}

	if (cc.bits.en != 0) {
		NVME_CTRLR_ERRLOG(ctrlr, "called with CC.EN = 1\n");
		return -EINVAL;
	}

	cc.bits.en = 1;
	cc.bits.css = 0;
	cc.bits.shn = 0;
	cc.bits.iosqes = 6; /* SQ entry size == 64 == 2^6 */
	cc.bits.iocqes = 4; /* CQ entry size == 16 == 2^4 */

	/* Page size is 2 ^ (12 + mps). */
	cc.bits.mps = spdk_u32log2(ctrlr->page_size) - 12;

	/*
	 * Since NVMe 1.0, a controller should have at least one bit set in CAP.CSS.
	 * A controller that does not have any bit set in CAP.CSS is not spec compliant.
	 * Try to support such a controller regardless.
	 */
	if (ctrlr->cap.bits.css == 0) {
		NVME_CTRLR_INFOLOG(ctrlr, "Drive reports no command sets supported. Assuming NVM is supported.\n");
		ctrlr->cap.bits.css = SPDK_NVME_CAP_CSS_NVM;
	}

	/*
	 * If the user did not explicitly request a command set, or supplied a value larger than
	 * what can be saved in CC.CSS, use the most reasonable default.
	 */
	if (ctrlr->opts.command_set >= CHAR_BIT) {
		if (ctrlr->cap.bits.css & SPDK_NVME_CAP_CSS_IOCS) {
			ctrlr->opts.command_set = SPDK_NVME_CC_CSS_IOCS;
		} else if (ctrlr->cap.bits.css & SPDK_NVME_CAP_CSS_NVM) {
			ctrlr->opts.command_set = SPDK_NVME_CC_CSS_NVM;
		} else if (ctrlr->cap.bits.css & SPDK_NVME_CAP_CSS_NOIO) {
			ctrlr->opts.command_set = SPDK_NVME_CC_CSS_NOIO;
		} else {
			/* Invalid supported bits detected, falling back to NVM. */
			ctrlr->opts.command_set = SPDK_NVME_CC_CSS_NVM;
		}
	}

	/* Verify that the selected command set is supported by the controller. */
	if (!(ctrlr->cap.bits.css & (1u << ctrlr->opts.command_set))) {
		NVME_CTRLR_DEBUGLOG(ctrlr, "Requested I/O command set %u but supported mask is 0x%x\n",
				    ctrlr->opts.command_set, ctrlr->cap.bits.css);
		NVME_CTRLR_DEBUGLOG(ctrlr, "Falling back to NVM. Assuming NVM is supported.\n");
		ctrlr->opts.command_set = SPDK_NVME_CC_CSS_NVM;
	}

	cc.bits.css = ctrlr->opts.command_set;

	switch (ctrlr->opts.arb_mechanism) {
	case SPDK_NVME_CC_AMS_RR:
		break;
	case SPDK_NVME_CC_AMS_WRR:
		if (SPDK_NVME_CAP_AMS_WRR & ctrlr->cap.bits.ams) {
			break;
		}
		return -EINVAL;
	case SPDK_NVME_CC_AMS_VS:
		if (SPDK_NVME_CAP_AMS_VS & ctrlr->cap.bits.ams) {
			break;
		}
		return -EINVAL;
	default:
		return -EINVAL;
	}

	cc.bits.ams = ctrlr->opts.arb_mechanism;

	if (nvme_ctrlr_set_cc(ctrlr, &cc)) {
		NVME_CTRLR_ERRLOG(ctrlr, "set_cc() failed\n");
		return -EIO;
	}

	return 0;
}

static int
nvme_ctrlr_disable(struct spdk_nvme_ctrlr *ctrlr)
{
	union spdk_nvme_cc_register	cc;

	if (nvme_ctrlr_get_cc(ctrlr, &cc)) {
		NVME_CTRLR_ERRLOG(ctrlr, "get_cc() failed\n");
		return -EIO;
	}

	if (cc.bits.en == 0) {
		return 0;
	}

	cc.bits.en = 0;

	if (nvme_ctrlr_set_cc(ctrlr, &cc)) {
		NVME_CTRLR_ERRLOG(ctrlr, "set_cc() failed\n");
		return -EIO;
	}

	return 0;
}

#ifdef DEBUG
static const char *
nvme_ctrlr_state_string(enum nvme_ctrlr_state state)
{
	switch (state) {
	case NVME_CTRLR_STATE_INIT_DELAY:
		return "delay init";
	case NVME_CTRLR_STATE_INIT:
		return "init";
	case NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_1:
		return "disable and wait for CSTS.RDY = 1";
	case NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_0:
		return "disable and wait for CSTS.RDY = 0";
	case NVME_CTRLR_STATE_ENABLE:
		return "enable controller by writing CC.EN = 1";
	case NVME_CTRLR_STATE_ENABLE_WAIT_FOR_READY_1:
		return "wait for CSTS.RDY = 1";
	case NVME_CTRLR_STATE_RESET_ADMIN_QUEUE:
		return "reset admin queue";
	case NVME_CTRLR_STATE_IDENTIFY:
		return "identify controller";
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY:
		return "wait for identify controller";
	case NVME_CTRLR_STATE_IDENTIFY_IOCS_SPECIFIC:
		return "identify controller iocs specific";
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_IOCS_SPECIFIC:
		return "wait for identify controller iocs specific";
	case NVME_CTRLR_STATE_GET_ZNS_CMD_EFFECTS_LOG:
		return "get zns cmd and effects log page";
	case NVME_CTRLR_STATE_WAIT_FOR_GET_ZNS_CMD_EFFECTS_LOG:
		return "wait for get zns cmd and effects log page";
	case NVME_CTRLR_STATE_SET_NUM_QUEUES:
		return "set number of queues";
	case NVME_CTRLR_STATE_WAIT_FOR_SET_NUM_QUEUES:
		return "wait for set number of queues";
	case NVME_CTRLR_STATE_CONSTRUCT_NS:
		return "construct namespaces";
	case NVME_CTRLR_STATE_IDENTIFY_ACTIVE_NS:
		return "identify active ns";
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_ACTIVE_NS:
		return "wait for identify active ns";
	case NVME_CTRLR_STATE_IDENTIFY_NS:
		return "identify ns";
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_NS:
		return "wait for identify ns";
	case NVME_CTRLR_STATE_IDENTIFY_ID_DESCS:
		return "identify namespace id descriptors";
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_ID_DESCS:
		return "wait for identify namespace id descriptors";
	case NVME_CTRLR_STATE_IDENTIFY_NS_IOCS_SPECIFIC:
		return "identify ns iocs specific";
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_NS_IOCS_SPECIFIC:
		return "wait for identify ns iocs specific";
	case NVME_CTRLR_STATE_CONFIGURE_AER:
		return "configure AER";
	case NVME_CTRLR_STATE_WAIT_FOR_CONFIGURE_AER:
		return "wait for configure aer";
	case NVME_CTRLR_STATE_SET_SUPPORTED_LOG_PAGES:
		return "set supported log pages";
	case NVME_CTRLR_STATE_SET_SUPPORTED_FEATURES:
		return "set supported features";
	case NVME_CTRLR_STATE_SET_DB_BUF_CFG:
		return "set doorbell buffer config";
	case NVME_CTRLR_STATE_WAIT_FOR_DB_BUF_CFG:
		return "wait for doorbell buffer config";
	case NVME_CTRLR_STATE_SET_KEEP_ALIVE_TIMEOUT:
		return "set keep alive timeout";
	case NVME_CTRLR_STATE_WAIT_FOR_KEEP_ALIVE_TIMEOUT:
		return "wait for set keep alive timeout";
	case NVME_CTRLR_STATE_SET_HOST_ID:
		return "set host ID";
	case NVME_CTRLR_STATE_WAIT_FOR_HOST_ID:
		return "wait for set host ID";
	case NVME_CTRLR_STATE_READY:
		return "ready";
	case NVME_CTRLR_STATE_ERROR:
		return "error";
	}
	return "unknown";
};
#endif /* DEBUG */

static void
nvme_ctrlr_set_state(struct spdk_nvme_ctrlr *ctrlr, enum nvme_ctrlr_state state,
		     uint64_t timeout_in_ms)
{
	uint64_t ticks_per_ms, timeout_in_ticks, now_ticks;

	ctrlr->state = state;
	if (timeout_in_ms == NVME_TIMEOUT_INFINITE) {
		goto inf;
	}

	ticks_per_ms = spdk_get_ticks_hz() / 1000;
	if (timeout_in_ms > UINT64_MAX / ticks_per_ms) {
		NVME_CTRLR_ERRLOG(ctrlr,
				  "Specified timeout would cause integer overflow. Defaulting to no timeout.\n");
		goto inf;
	}

	now_ticks = spdk_get_ticks();
	timeout_in_ticks = timeout_in_ms * ticks_per_ms;
	if (timeout_in_ticks > UINT64_MAX - now_ticks) {
		NVME_CTRLR_ERRLOG(ctrlr,
				  "Specified timeout would cause integer overflow. Defaulting to no timeout.\n");
		goto inf;
	}

	ctrlr->state_timeout_tsc = timeout_in_ticks + now_ticks;
	NVME_CTRLR_DEBUGLOG(ctrlr, "setting state to %s (timeout %" PRIu64 " ms)\n",
			    nvme_ctrlr_state_string(ctrlr->state), timeout_in_ms);
	return;
inf:
	NVME_CTRLR_DEBUGLOG(ctrlr, "setting state to %s (no timeout)\n",
			    nvme_ctrlr_state_string(ctrlr->state));
	ctrlr->state_timeout_tsc = NVME_TIMEOUT_INFINITE;
}

static void
nvme_ctrlr_free_zns_specific_data(struct spdk_nvme_ctrlr *ctrlr)
{
	spdk_free(ctrlr->cdata_zns);
	ctrlr->cdata_zns = NULL;
}

static void
nvme_ctrlr_free_iocs_specific_data(struct spdk_nvme_ctrlr *ctrlr)
{
	nvme_ctrlr_free_zns_specific_data(ctrlr);
}

static void
nvme_ctrlr_free_doorbell_buffer(struct spdk_nvme_ctrlr *ctrlr)
{
	if (ctrlr->shadow_doorbell) {
		spdk_free(ctrlr->shadow_doorbell);
		ctrlr->shadow_doorbell = NULL;
	}

	if (ctrlr->eventidx) {
		spdk_free(ctrlr->eventidx);
		ctrlr->eventidx = NULL;
	}
}

static void
nvme_ctrlr_set_doorbell_buffer_config_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_ctrlr *ctrlr = (struct spdk_nvme_ctrlr *)arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		NVME_CTRLR_WARNLOG(ctrlr, "Doorbell buffer config failed\n");
	} else {
		NVME_CTRLR_INFOLOG(ctrlr, "Doorbell buffer config enabled\n");
	}
	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_KEEP_ALIVE_TIMEOUT,
			     ctrlr->opts.admin_timeout_ms);
}

static int
nvme_ctrlr_set_doorbell_buffer_config(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc = 0;
	uint64_t prp1, prp2, len;

	if (!ctrlr->cdata.oacs.doorbell_buffer_config) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_KEEP_ALIVE_TIMEOUT,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	if (ctrlr->trid.trtype != SPDK_NVME_TRANSPORT_PCIE) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_KEEP_ALIVE_TIMEOUT,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	/* only 1 page size for doorbell buffer */
	ctrlr->shadow_doorbell = spdk_zmalloc(ctrlr->page_size, ctrlr->page_size,
					      NULL, SPDK_ENV_LCORE_ID_ANY,
					      SPDK_MALLOC_DMA | SPDK_MALLOC_SHARE);
	if (ctrlr->shadow_doorbell == NULL) {
		rc = -ENOMEM;
		goto error;
	}

	len = ctrlr->page_size;
	prp1 = spdk_vtophys(ctrlr->shadow_doorbell, &len);
	if (prp1 == SPDK_VTOPHYS_ERROR || len != ctrlr->page_size) {
		rc = -EFAULT;
		goto error;
	}

	ctrlr->eventidx = spdk_zmalloc(ctrlr->page_size, ctrlr->page_size,
				       NULL, SPDK_ENV_LCORE_ID_ANY,
				       SPDK_MALLOC_DMA | SPDK_MALLOC_SHARE);
	if (ctrlr->eventidx == NULL) {
		rc = -ENOMEM;
		goto error;
	}

	len = ctrlr->page_size;
	prp2 = spdk_vtophys(ctrlr->eventidx, &len);
	if (prp2 == SPDK_VTOPHYS_ERROR || len != ctrlr->page_size) {
		rc = -EFAULT;
		goto error;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_DB_BUF_CFG,
			     ctrlr->opts.admin_timeout_ms);

	rc = nvme_ctrlr_cmd_doorbell_buffer_config(ctrlr, prp1, prp2,
			nvme_ctrlr_set_doorbell_buffer_config_done, ctrlr);
	if (rc != 0) {
		goto error;
	}

	return 0;

error:
	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
	nvme_ctrlr_free_doorbell_buffer(ctrlr);
	return rc;
}

static void
nvme_ctrlr_abort_queued_aborts(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_request	*req, *tmp;
	struct spdk_nvme_cpl	cpl = {};

	cpl.status.sc = SPDK_NVME_SC_ABORTED_SQ_DELETION;
	cpl.status.sct = SPDK_NVME_SCT_GENERIC;

	STAILQ_FOREACH_SAFE(req, &ctrlr->queued_aborts, stailq, tmp) {
		STAILQ_REMOVE_HEAD(&ctrlr->queued_aborts, stailq);

		nvme_complete_request(req->cb_fn, req->cb_arg, req->qpair, req, &cpl);
		nvme_free_request(req);
	}
}

int
spdk_nvme_ctrlr_reset(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc = 0, rc_tmp = 0;
	struct spdk_nvme_qpair	*qpair;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	if (ctrlr->is_resetting || ctrlr->is_removed) {
		/*
		 * Controller is already resetting or has been removed. Return
		 *  immediately since there is no need to kick off another
		 *  reset in these cases.
		 */
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return ctrlr->is_resetting ? 0 : -ENXIO;
	}

	ctrlr->is_resetting = true;
	ctrlr->is_failed = false;

	NVME_CTRLR_NOTICELOG(ctrlr, "resetting controller\n");

	/* Abort all of the queued abort requests */
	nvme_ctrlr_abort_queued_aborts(ctrlr);

	nvme_transport_admin_qpair_abort_aers(ctrlr->adminq);

	/* Disable all queues before disabling the controller hardware. */
	TAILQ_FOREACH(qpair, &ctrlr->active_io_qpairs, tailq) {
		qpair->transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_LOCAL;
	}

	ctrlr->adminq->transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_LOCAL;
	nvme_transport_ctrlr_disconnect_qpair(ctrlr, ctrlr->adminq);
	rc = nvme_transport_ctrlr_connect_qpair(ctrlr, ctrlr->adminq);
	if (rc != 0) {
		NVME_CTRLR_ERRLOG(ctrlr, "Controller reinitialization failed.\n");
		goto out;
	}

	/* Doorbell buffer config is invalid during reset */
	nvme_ctrlr_free_doorbell_buffer(ctrlr);

	/* I/O Command Set Specific Identify Controller data is invalidated during reset */
	nvme_ctrlr_free_iocs_specific_data(ctrlr);

	spdk_bit_array_free(&ctrlr->free_io_qids);

	/* Set the state back to INIT to cause a full hardware reset. */
	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_INIT, NVME_TIMEOUT_INFINITE);

	nvme_qpair_set_state(ctrlr->adminq, NVME_QPAIR_ENABLED);
	while (ctrlr->state != NVME_CTRLR_STATE_READY) {
		if (nvme_ctrlr_process_init(ctrlr) != 0) {
			NVME_CTRLR_ERRLOG(ctrlr, "controller reinitialization failed\n");
			rc = -1;
			break;
		}
	}

	/*
	 * For PCIe controllers, the memory locations of the transport qpair
	 * don't change when the controller is reset. They simply need to be
	 * re-enabled with admin commands to the controller. For fabric
	 * controllers we need to disconnect and reconnect the qpair on its
	 * own thread outside of the context of the reset.
	 */
	if (rc == 0 && ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
		/* Reinitialize qpairs */
		TAILQ_FOREACH(qpair, &ctrlr->active_io_qpairs, tailq) {
			assert(spdk_bit_array_get(ctrlr->free_io_qids, qpair->id));
			spdk_bit_array_clear(ctrlr->free_io_qids, qpair->id);
			rc_tmp = nvme_transport_ctrlr_connect_qpair(ctrlr, qpair);
			if (rc_tmp != 0) {
				rc = rc_tmp;
				qpair->transport_failure_reason = SPDK_NVME_QPAIR_FAILURE_LOCAL;
				continue;
			}
		}
	}

out:
	if (rc) {
		nvme_ctrlr_fail(ctrlr, false);
	}
	ctrlr->is_resetting = false;

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	if (!ctrlr->cdata.oaes.ns_attribute_notices) {
		/*
		 * If controller doesn't support ns_attribute_notices and
		 * namespace attributes change (e.g. number of namespaces)
		 * we need to update system handling device reset.
		 */
		nvme_io_msg_ctrlr_update(ctrlr);
	}

	return rc;
}

int
spdk_nvme_ctrlr_reset_subsystem(struct spdk_nvme_ctrlr *ctrlr)
{
	union spdk_nvme_cap_register cap;
	int rc = 0;

	cap = spdk_nvme_ctrlr_get_regs_cap(ctrlr);
	if (cap.bits.nssrs == 0) {
		NVME_CTRLR_WARNLOG(ctrlr, "subsystem reset is not supported\n");
		return -ENOTSUP;
	}

	NVME_CTRLR_NOTICELOG(ctrlr, "resetting subsystem\n");
	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	ctrlr->is_resetting = true;
	rc = nvme_ctrlr_set_nssr(ctrlr, SPDK_NVME_NSSR_VALUE);
	ctrlr->is_resetting = false;

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	/*
	 * No more cleanup at this point like in the ctrlr reset. A subsystem reset will cause
	 * a hot remove for PCIe transport. The hot remove handling does all the necessary ctrlr cleanup.
	 */
	return rc;
}

int
spdk_nvme_ctrlr_set_trid(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_transport_id *trid)
{
	int rc = 0;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	if (ctrlr->is_failed == false) {
		rc = -EPERM;
		goto out;
	}

	if (trid->trtype != ctrlr->trid.trtype) {
		rc = -EINVAL;
		goto out;
	}

	if (strncmp(trid->subnqn, ctrlr->trid.subnqn, SPDK_NVMF_NQN_MAX_LEN)) {
		rc = -EINVAL;
		goto out;
	}

	ctrlr->trid = *trid;

out:
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

void
spdk_nvme_ctrlr_set_remove_cb(struct spdk_nvme_ctrlr *ctrlr,
			      spdk_nvme_remove_cb remove_cb, void *remove_ctx)
{
	if (!spdk_process_is_primary()) {
		return;
	}

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	ctrlr->remove_cb = remove_cb;
	ctrlr->cb_ctx = remove_ctx;
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

static void
nvme_ctrlr_identify_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_ctrlr *ctrlr = (struct spdk_nvme_ctrlr *)arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		NVME_CTRLR_ERRLOG(ctrlr, "nvme_identify_controller failed!\n");
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return;
	}

	/*
	 * Use MDTS to ensure our default max_xfer_size doesn't exceed what the
	 *  controller supports.
	 */
	ctrlr->max_xfer_size = nvme_transport_ctrlr_get_max_xfer_size(ctrlr);
	NVME_CTRLR_DEBUGLOG(ctrlr, "transport max_xfer_size %u\n", ctrlr->max_xfer_size);
	if (ctrlr->cdata.mdts > 0) {
		ctrlr->max_xfer_size = spdk_min(ctrlr->max_xfer_size,
						ctrlr->min_page_size * (1 << ctrlr->cdata.mdts));
		NVME_CTRLR_DEBUGLOG(ctrlr, "MDTS max_xfer_size %u\n", ctrlr->max_xfer_size);
	}

	NVME_CTRLR_DEBUGLOG(ctrlr, "CNTLID 0x%04" PRIx16 "\n", ctrlr->cdata.cntlid);
	if (ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
		ctrlr->cntlid = ctrlr->cdata.cntlid;
	} else {
		/*
		 * Fabrics controllers should already have CNTLID from the Connect command.
		 *
		 * If CNTLID from Connect doesn't match CNTLID in the Identify Controller data,
		 * trust the one from Connect.
		 */
		if (ctrlr->cntlid != ctrlr->cdata.cntlid) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "Identify CNTLID 0x%04" PRIx16 " != Connect CNTLID 0x%04" PRIx16 "\n",
					    ctrlr->cdata.cntlid, ctrlr->cntlid);
		}
	}

	if (ctrlr->cdata.sgls.supported) {
		assert(ctrlr->cdata.sgls.supported != 0x3);
		ctrlr->flags |= SPDK_NVME_CTRLR_SGL_SUPPORTED;
		if (ctrlr->cdata.sgls.supported == 0x2) {
			ctrlr->flags |= SPDK_NVME_CTRLR_SGL_REQUIRES_DWORD_ALIGNMENT;
		}
		/*
		 * Use MSDBD to ensure our max_sges doesn't exceed what the
		 *  controller supports.
		 */
		ctrlr->max_sges = nvme_transport_ctrlr_get_max_sges(ctrlr);
		if (ctrlr->cdata.nvmf_specific.msdbd != 0) {
			ctrlr->max_sges = spdk_min(ctrlr->cdata.nvmf_specific.msdbd, ctrlr->max_sges);
		} else {
			/* A value 0 indicates no limit. */
		}
		NVME_CTRLR_DEBUGLOG(ctrlr, "transport max_sges %u\n", ctrlr->max_sges);
	}

	if (ctrlr->cdata.oacs.security && !(ctrlr->quirks & NVME_QUIRK_OACS_SECURITY)) {
		ctrlr->flags |= SPDK_NVME_CTRLR_SECURITY_SEND_RECV_SUPPORTED;
	}

	if (ctrlr->cdata.oacs.directives) {
		ctrlr->flags |= SPDK_NVME_CTRLR_DIRECTIVES_SUPPORTED;
	}

	NVME_CTRLR_DEBUGLOG(ctrlr, "fuses compare and write: %d\n",
			    ctrlr->cdata.fuses.compare_and_write);
	if (ctrlr->cdata.fuses.compare_and_write) {
		ctrlr->flags |= SPDK_NVME_CTRLR_COMPARE_AND_WRITE_SUPPORTED;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_IOCS_SPECIFIC,
			     ctrlr->opts.admin_timeout_ms);
}

static int
nvme_ctrlr_identify(struct spdk_nvme_ctrlr *ctrlr)
{
	int	rc;

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY,
			     ctrlr->opts.admin_timeout_ms);

	rc = nvme_ctrlr_cmd_identify(ctrlr, SPDK_NVME_IDENTIFY_CTRLR, 0, 0, 0,
				     &ctrlr->cdata, sizeof(ctrlr->cdata),
				     nvme_ctrlr_identify_done, ctrlr);
	if (rc != 0) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return rc;
	}

	return 0;
}

static void
nvme_ctrlr_get_zns_cmd_and_effects_log_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_cmds_and_effect_log_page *log_page;
	struct spdk_nvme_ctrlr *ctrlr = arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		NVME_CTRLR_ERRLOG(ctrlr, "nvme_ctrlr_get_zns_cmd_and_effects_log failed!\n");
		spdk_free(ctrlr->tmp_ptr);
		ctrlr->tmp_ptr = NULL;
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return;
	}

	log_page = ctrlr->tmp_ptr;

	if (log_page->io_cmds_supported[SPDK_NVME_OPC_ZONE_APPEND].csupp) {
		ctrlr->flags |= SPDK_NVME_CTRLR_ZONE_APPEND_SUPPORTED;
	}
	spdk_free(ctrlr->tmp_ptr);
	ctrlr->tmp_ptr = NULL;

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_NUM_QUEUES, ctrlr->opts.admin_timeout_ms);
}

static int
nvme_ctrlr_get_zns_cmd_and_effects_log(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc;

	assert(!ctrlr->tmp_ptr);
	ctrlr->tmp_ptr = spdk_zmalloc(sizeof(struct spdk_nvme_cmds_and_effect_log_page), 64, NULL,
				      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_SHARE | SPDK_MALLOC_DMA);
	if (!ctrlr->tmp_ptr) {
		rc = -ENOMEM;
		goto error;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_GET_ZNS_CMD_EFFECTS_LOG,
			     ctrlr->opts.admin_timeout_ms);

	rc = spdk_nvme_ctrlr_cmd_get_log_page_ext(ctrlr, SPDK_NVME_LOG_COMMAND_EFFECTS_LOG,
			0, ctrlr->tmp_ptr, sizeof(struct spdk_nvme_cmds_and_effect_log_page),
			0, 0, 0, SPDK_NVME_CSI_ZNS << 24,
			nvme_ctrlr_get_zns_cmd_and_effects_log_done, ctrlr);
	if (rc != 0) {
		goto error;
	}

	return 0;

error:
	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
	spdk_free(ctrlr->tmp_ptr);
	ctrlr->tmp_ptr = NULL;
	return rc;
}

static void
nvme_ctrlr_identify_zns_specific_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_ctrlr *ctrlr = (struct spdk_nvme_ctrlr *)arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		/* no need to print an error, the controller simply does not support ZNS */
		nvme_ctrlr_free_zns_specific_data(ctrlr);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_NUM_QUEUES,
				     ctrlr->opts.admin_timeout_ms);
		return;
	}

	/* A zero zasl value means use mdts */
	if (ctrlr->cdata_zns->zasl) {
		uint32_t max_append = ctrlr->min_page_size * (1 << ctrlr->cdata_zns->zasl);
		ctrlr->max_zone_append_size = spdk_min(ctrlr->max_xfer_size, max_append);
	} else {
		ctrlr->max_zone_append_size = ctrlr->max_xfer_size;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_GET_ZNS_CMD_EFFECTS_LOG,
			     ctrlr->opts.admin_timeout_ms);
}

/**
 * This function will try to fetch the I/O Command Specific Controller data structure for
 * each I/O Command Set supported by SPDK.
 *
 * If an I/O Command Set is not supported by the controller, "Invalid Field in Command"
 * will be returned. Since we are fetching in a exploratively way, getting an error back
 * from the controller should not be treated as fatal.
 *
 * I/O Command Sets not supported by SPDK will be skipped (e.g. Key Value Command Set).
 *
 * I/O Command Sets without a IOCS specific data structure (i.e. a zero-filled IOCS specific
 * data structure) will be skipped (e.g. NVM Command Set, Key Value Command Set).
 */
static int
nvme_ctrlr_identify_iocs_specific(struct spdk_nvme_ctrlr *ctrlr)
{
	int	rc;

	if (!nvme_ctrlr_multi_iocs_enabled(ctrlr)) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_NUM_QUEUES,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	/*
	 * Since SPDK currently only needs to fetch a single Command Set, keep the code here,
	 * instead of creating multiple NVME_CTRLR_STATE_IDENTIFY_IOCS_SPECIFIC substates,
	 * which would require additional functions and complexity for no good reason.
	 */
	assert(!ctrlr->cdata_zns);
	ctrlr->cdata_zns = spdk_zmalloc(sizeof(*ctrlr->cdata_zns), 64, NULL, SPDK_ENV_SOCKET_ID_ANY,
					SPDK_MALLOC_SHARE | SPDK_MALLOC_DMA);
	if (!ctrlr->cdata_zns) {
		rc = -ENOMEM;
		goto error;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_IOCS_SPECIFIC,
			     ctrlr->opts.admin_timeout_ms);

	rc = nvme_ctrlr_cmd_identify(ctrlr, SPDK_NVME_IDENTIFY_CTRLR_IOCS, 0, 0, SPDK_NVME_CSI_ZNS,
				     ctrlr->cdata_zns, sizeof(*ctrlr->cdata_zns),
				     nvme_ctrlr_identify_zns_specific_done, ctrlr);
	if (rc != 0) {
		goto error;
	}

	return 0;

error:
	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
	nvme_ctrlr_free_zns_specific_data(ctrlr);
	return rc;
}

enum nvme_active_ns_state {
	NVME_ACTIVE_NS_STATE_IDLE,
	NVME_ACTIVE_NS_STATE_PROCESSING,
	NVME_ACTIVE_NS_STATE_DONE,
	NVME_ACTIVE_NS_STATE_ERROR
};

typedef void (*nvme_active_ns_ctx_deleter)(struct nvme_active_ns_ctx *);

struct nvme_active_ns_ctx {
	struct spdk_nvme_ctrlr *ctrlr;
	uint32_t page;
	uint32_t num_pages;
	uint32_t next_nsid;
	uint32_t *new_ns_list;
	nvme_active_ns_ctx_deleter deleter;

	enum nvme_active_ns_state state;
};

static struct nvme_active_ns_ctx *
nvme_active_ns_ctx_create(struct spdk_nvme_ctrlr *ctrlr, nvme_active_ns_ctx_deleter deleter)
{
	struct nvme_active_ns_ctx *ctx;
	uint32_t num_pages = 0;
	uint32_t *new_ns_list = NULL;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate nvme_active_ns_ctx!\n");
		return NULL;
	}

	if (ctrlr->num_ns) {
		/* The allocated size must be a multiple of sizeof(struct spdk_nvme_ns_list) */
		num_pages = (ctrlr->num_ns * sizeof(new_ns_list[0]) - 1) / sizeof(struct spdk_nvme_ns_list) + 1;
		new_ns_list = spdk_zmalloc(num_pages * sizeof(struct spdk_nvme_ns_list), ctrlr->page_size,
					   NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA | SPDK_MALLOC_SHARE);
		if (!new_ns_list) {
			NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate active_ns_list!\n");
			free(ctx);
			return NULL;
		}
	}

	ctx->num_pages = num_pages;
	ctx->new_ns_list = new_ns_list;
	ctx->ctrlr = ctrlr;
	ctx->deleter = deleter;

	return ctx;
}

static void
nvme_active_ns_ctx_destroy(struct nvme_active_ns_ctx *ctx)
{
	spdk_free(ctx->new_ns_list);
	free(ctx);
}

static void
nvme_ctrlr_identify_active_ns_swap(struct spdk_nvme_ctrlr *ctrlr, uint32_t **new_ns_list)
{
	spdk_free(ctrlr->active_ns_list);
	ctrlr->active_ns_list = *new_ns_list;
	*new_ns_list = NULL;
}

static void
nvme_ctrlr_identify_active_ns_async_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct nvme_active_ns_ctx *ctx = arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		ctx->state = NVME_ACTIVE_NS_STATE_ERROR;
		goto out;
	}

	ctx->next_nsid = ctx->new_ns_list[1024 * ctx->page + 1023];
	if (ctx->next_nsid == 0 || ++ctx->page == ctx->num_pages) {
		ctx->state = NVME_ACTIVE_NS_STATE_DONE;
		goto out;
	}

	nvme_ctrlr_identify_active_ns_async(ctx);
	return;

out:
	if (ctx->deleter) {
		ctx->deleter(ctx);
	}
}

static void
nvme_ctrlr_identify_active_ns_async(struct nvme_active_ns_ctx *ctx)
{
	struct spdk_nvme_ctrlr *ctrlr = ctx->ctrlr;
	uint32_t i;
	int rc;

	if (ctrlr->num_ns == 0) {
		ctx->state = NVME_ACTIVE_NS_STATE_DONE;
		goto out;
	}

	assert(ctx->new_ns_list != NULL);

	/*
	 * If controller doesn't support active ns list CNS 0x02 dummy up
	 * an active ns list, i.e. all namespaces report as active
	 */
	if (ctrlr->vs.raw < SPDK_NVME_VERSION(1, 1, 0) || ctrlr->quirks & NVME_QUIRK_IDENTIFY_CNS) {
		for (i = 0; i < ctrlr->num_ns; i++) {
			ctx->new_ns_list[i] = i + 1;
		}

		ctx->state = NVME_ACTIVE_NS_STATE_DONE;
		goto out;
	}

	ctx->state = NVME_ACTIVE_NS_STATE_PROCESSING;
	rc = nvme_ctrlr_cmd_identify(ctrlr, SPDK_NVME_IDENTIFY_ACTIVE_NS_LIST, 0, ctx->next_nsid, 0,
				     &ctx->new_ns_list[1024 * ctx->page], sizeof(struct spdk_nvme_ns_list),
				     nvme_ctrlr_identify_active_ns_async_done, ctx);
	if (rc != 0) {
		ctx->state = NVME_ACTIVE_NS_STATE_ERROR;
		goto out;
	}

	return;

out:
	if (ctx->deleter) {
		ctx->deleter(ctx);
	}
}

static void
_nvme_active_ns_ctx_deleter(struct nvme_active_ns_ctx *ctx)
{
	struct spdk_nvme_ctrlr *ctrlr = ctx->ctrlr;

	if (ctx->state == NVME_ACTIVE_NS_STATE_ERROR) {
		nvme_ctrlr_destruct_namespaces(ctrlr);
		nvme_active_ns_ctx_destroy(ctx);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return;
	}

	assert(ctx->state == NVME_ACTIVE_NS_STATE_DONE);
	nvme_ctrlr_identify_active_ns_swap(ctrlr, &ctx->new_ns_list);
	nvme_active_ns_ctx_destroy(ctx);
	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_NS, ctrlr->opts.admin_timeout_ms);
}

static void
_nvme_ctrlr_identify_active_ns(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_active_ns_ctx *ctx;

	ctx = nvme_active_ns_ctx_create(ctrlr, _nvme_active_ns_ctx_deleter);
	if (!ctx) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_ACTIVE_NS,
			     ctrlr->opts.admin_timeout_ms);
	nvme_ctrlr_identify_active_ns_async(ctx);
}

int
nvme_ctrlr_identify_active_ns(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_active_ns_ctx *ctx;
	int rc;

	ctx = nvme_active_ns_ctx_create(ctrlr, NULL);
	if (!ctx) {
		return -ENOMEM;
	}

	nvme_ctrlr_identify_active_ns_async(ctx);
	while (ctx->state == NVME_ACTIVE_NS_STATE_PROCESSING) {
		rc = spdk_nvme_qpair_process_completions(ctrlr->adminq, 0);
		if (rc < 0) {
			ctx->state = NVME_ACTIVE_NS_STATE_ERROR;
			break;
		}
	}

	if (ctx->state == NVME_ACTIVE_NS_STATE_ERROR) {
		nvme_active_ns_ctx_destroy(ctx);
		return -ENXIO;
	}

	assert(ctx->state == NVME_ACTIVE_NS_STATE_DONE);
	nvme_ctrlr_identify_active_ns_swap(ctrlr, &ctx->new_ns_list);
	nvme_active_ns_ctx_destroy(ctx);

	return 0;
}

static void
nvme_ctrlr_identify_ns_async_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_ns *ns = (struct spdk_nvme_ns *)arg;
	struct spdk_nvme_ctrlr *ctrlr = ns->ctrlr;
	uint32_t nsid;
	int rc;

	if (spdk_nvme_cpl_is_error(cpl)) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return;
	}

	nvme_ns_set_identify_data(ns);

	/* move on to the next active NS */
	nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, ns->id);
	ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
	if (ns == NULL) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_ID_DESCS,
				     ctrlr->opts.admin_timeout_ms);
		return;
	}
	ns->ctrlr = ctrlr;
	ns->id = nsid;

	rc = nvme_ctrlr_identify_ns_async(ns);
	if (rc) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
	}
}

static int
nvme_ctrlr_identify_ns_async(struct spdk_nvme_ns *ns)
{
	struct spdk_nvme_ctrlr *ctrlr = ns->ctrlr;
	struct spdk_nvme_ns_data *nsdata;

	nsdata = &ns->nsdata;

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_NS,
			     ctrlr->opts.admin_timeout_ms);
	return nvme_ctrlr_cmd_identify(ns->ctrlr, SPDK_NVME_IDENTIFY_NS, 0, ns->id, 0,
				       nsdata, sizeof(*nsdata),
				       nvme_ctrlr_identify_ns_async_done, ns);
}

static int
nvme_ctrlr_identify_namespaces(struct spdk_nvme_ctrlr *ctrlr)
{
	uint32_t nsid;
	struct spdk_nvme_ns *ns;
	int rc;

	nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr);
	ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
	if (ns == NULL) {
		/* No active NS, move on to the next state */
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_ID_DESCS,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	ns->ctrlr = ctrlr;
	ns->id = nsid;

	rc = nvme_ctrlr_identify_ns_async(ns);
	if (rc) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
	}

	return rc;
}

static int
nvme_ctrlr_identify_namespaces_iocs_specific_next(struct spdk_nvme_ctrlr *ctrlr, uint32_t prev_nsid)
{
	uint32_t nsid;
	struct spdk_nvme_ns *ns;
	int rc;

	if (!prev_nsid) {
		nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr);
	} else {
		/* move on to the next active NS */
		nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, prev_nsid);
	}

	ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
	if (ns == NULL) {
		/* No first/next active NS, move on to the next state */
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_CONFIGURE_AER,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	/* loop until we find a ns which has (supported) iocs specific data */
	while (!nvme_ns_has_supported_iocs_specific_data(ns)) {
		nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, ns->id);
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == NULL) {
			/* no namespace with (supported) iocs specific data found */
			nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_CONFIGURE_AER,
					     ctrlr->opts.admin_timeout_ms);
			return 0;
		}
	}

	rc = nvme_ctrlr_identify_ns_iocs_specific_async(ns);
	if (rc) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
	}

	return rc;
}

static void
nvme_ctrlr_identify_ns_zns_specific_async_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_ns *ns = (struct spdk_nvme_ns *)arg;
	struct spdk_nvme_ctrlr *ctrlr = ns->ctrlr;

	if (spdk_nvme_cpl_is_error(cpl)) {
		nvme_ns_free_zns_specific_data(ns);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return;
	}

	nvme_ctrlr_identify_namespaces_iocs_specific_next(ctrlr, ns->id);
}

static int
nvme_ctrlr_identify_ns_iocs_specific_async(struct spdk_nvme_ns *ns)
{
	struct spdk_nvme_ctrlr *ctrlr = ns->ctrlr;
	int rc;

	switch (ns->csi) {
	case SPDK_NVME_CSI_ZNS:
		break;
	default:
		/*
		 * This switch must handle all cases for which
		 * nvme_ns_has_supported_iocs_specific_data() returns true,
		 * other cases should never happen.
		 */
		assert(0);
	}

	assert(!ns->nsdata_zns);
	ns->nsdata_zns = spdk_zmalloc(sizeof(*ns->nsdata_zns), 64, NULL, SPDK_ENV_SOCKET_ID_ANY,
				      SPDK_MALLOC_SHARE);
	if (!ns->nsdata_zns) {
		return -ENOMEM;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_NS_IOCS_SPECIFIC,
			     ctrlr->opts.admin_timeout_ms);
	rc = nvme_ctrlr_cmd_identify(ns->ctrlr, SPDK_NVME_IDENTIFY_NS_IOCS, 0, ns->id, ns->csi,
				     ns->nsdata_zns, sizeof(*ns->nsdata_zns),
				     nvme_ctrlr_identify_ns_zns_specific_async_done, ns);
	if (rc) {
		nvme_ns_free_zns_specific_data(ns);
	}

	return rc;
}

static int
nvme_ctrlr_identify_namespaces_iocs_specific(struct spdk_nvme_ctrlr *ctrlr)
{
	if (!nvme_ctrlr_multi_iocs_enabled(ctrlr)) {
		/* Multi IOCS not supported/enabled, move on to the next state */
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_CONFIGURE_AER,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	return nvme_ctrlr_identify_namespaces_iocs_specific_next(ctrlr, 0);
}

static void
nvme_ctrlr_identify_id_desc_async_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_ns *ns = (struct spdk_nvme_ns *)arg;
	struct spdk_nvme_ctrlr *ctrlr = ns->ctrlr;
	uint32_t nsid;
	int rc;

	if (spdk_nvme_cpl_is_error(cpl)) {
		/*
		 * Many controllers claim to be compatible with NVMe 1.3, however,
		 * they do not implement NS ID Desc List. Therefore, instead of setting
		 * the state to NVME_CTRLR_STATE_ERROR, silently ignore the completion
		 * error and move on to the next state.
		 *
		 * The proper way is to create a new quirk for controllers that violate
		 * the NVMe 1.3 spec by not supporting NS ID Desc List.
		 * (Re-using the NVME_QUIRK_IDENTIFY_CNS quirk is not possible, since
		 * it is too generic and was added in order to handle controllers that
		 * violate the NVMe 1.1 spec by not supporting ACTIVE LIST).
		 */
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_NS_IOCS_SPECIFIC,
				     ctrlr->opts.admin_timeout_ms);
		return;
	}

	nvme_ns_set_id_desc_list_data(ns);

	/* move on to the next active NS */
	nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, ns->id);
	ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
	if (ns == NULL) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_NS_IOCS_SPECIFIC,
				     ctrlr->opts.admin_timeout_ms);
		return;
	}

	rc = nvme_ctrlr_identify_id_desc_async(ns);
	if (rc) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
	}
}

static int
nvme_ctrlr_identify_id_desc_async(struct spdk_nvme_ns *ns)
{
	struct spdk_nvme_ctrlr *ctrlr = ns->ctrlr;

	memset(ns->id_desc_list, 0, sizeof(ns->id_desc_list));

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_ID_DESCS,
			     ctrlr->opts.admin_timeout_ms);
	return nvme_ctrlr_cmd_identify(ns->ctrlr, SPDK_NVME_IDENTIFY_NS_ID_DESCRIPTOR_LIST,
				       0, ns->id, 0, ns->id_desc_list, sizeof(ns->id_desc_list),
				       nvme_ctrlr_identify_id_desc_async_done, ns);
}

static int
nvme_ctrlr_identify_id_desc_namespaces(struct spdk_nvme_ctrlr *ctrlr)
{
	uint32_t nsid;
	struct spdk_nvme_ns *ns;
	int rc;

	if ((ctrlr->vs.raw < SPDK_NVME_VERSION(1, 3, 0) &&
	     !(ctrlr->cap.bits.css & SPDK_NVME_CAP_CSS_IOCS)) ||
	    (ctrlr->quirks & NVME_QUIRK_IDENTIFY_CNS)) {
		NVME_CTRLR_DEBUGLOG(ctrlr, "Version < 1.3; not attempting to retrieve NS ID Descriptor List\n");
		/* NS ID Desc List not supported, move on to the next state */
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_NS_IOCS_SPECIFIC,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr);
	ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
	if (ns == NULL) {
		/* No active NS, move on to the next state */
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_NS_IOCS_SPECIFIC,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	rc = nvme_ctrlr_identify_id_desc_async(ns);
	if (rc) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
	}

	return rc;
}

static void
nvme_ctrlr_update_nvmf_ioccsz(struct spdk_nvme_ctrlr *ctrlr)
{
	if (ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_RDMA ||
	    ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_TCP ||
	    ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_FC) {
		if (ctrlr->cdata.nvmf_specific.ioccsz < 4) {
			NVME_CTRLR_ERRLOG(ctrlr, "Incorrect IOCCSZ %u, the minimum value should be 4\n",
					  ctrlr->cdata.nvmf_specific.ioccsz);
			ctrlr->cdata.nvmf_specific.ioccsz = 4;
			assert(0);
		}
		ctrlr->ioccsz_bytes = ctrlr->cdata.nvmf_specific.ioccsz * 16 - sizeof(struct spdk_nvme_cmd);
		ctrlr->icdoff = ctrlr->cdata.nvmf_specific.icdoff;
	}
}

static void
nvme_ctrlr_set_num_queues_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	uint32_t cq_allocated, sq_allocated, min_allocated, i;
	struct spdk_nvme_ctrlr *ctrlr = (struct spdk_nvme_ctrlr *)arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		NVME_CTRLR_ERRLOG(ctrlr, "Set Features - Number of Queues failed!\n");
		ctrlr->opts.num_io_queues = 0;
	} else {
		/*
		 * Data in cdw0 is 0-based.
		 * Lower 16-bits indicate number of submission queues allocated.
		 * Upper 16-bits indicate number of completion queues allocated.
		 */
		sq_allocated = (cpl->cdw0 & 0xFFFF) + 1;
		cq_allocated = (cpl->cdw0 >> 16) + 1;

		/*
		 * For 1:1 queue mapping, set number of allocated queues to be minimum of
		 * submission and completion queues.
		 */
		min_allocated = spdk_min(sq_allocated, cq_allocated);

		/* Set number of queues to be minimum of requested and actually allocated. */
		ctrlr->opts.num_io_queues = spdk_min(min_allocated, ctrlr->opts.num_io_queues);
	}

	ctrlr->free_io_qids = spdk_bit_array_create(ctrlr->opts.num_io_queues + 1);
	if (ctrlr->free_io_qids == NULL) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return;
	}

	/* Initialize list of free I/O queue IDs. QID 0 is the admin queue (implicitly allocated). */
	for (i = 1; i <= ctrlr->opts.num_io_queues; i++) {
		spdk_nvme_ctrlr_free_qid(ctrlr, i);
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_CONSTRUCT_NS,
			     ctrlr->opts.admin_timeout_ms);
}

static int
nvme_ctrlr_set_num_queues(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc;

	if (ctrlr->opts.num_io_queues > SPDK_NVME_MAX_IO_QUEUES) {
		NVME_CTRLR_NOTICELOG(ctrlr, "Limiting requested num_io_queues %u to max %d\n",
				     ctrlr->opts.num_io_queues, SPDK_NVME_MAX_IO_QUEUES);
		ctrlr->opts.num_io_queues = SPDK_NVME_MAX_IO_QUEUES;
	} else if (ctrlr->opts.num_io_queues < 1) {
		NVME_CTRLR_NOTICELOG(ctrlr, "Requested num_io_queues 0, increasing to 1\n");
		ctrlr->opts.num_io_queues = 1;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_SET_NUM_QUEUES,
			     ctrlr->opts.admin_timeout_ms);

	rc = nvme_ctrlr_cmd_set_num_queues(ctrlr, ctrlr->opts.num_io_queues,
					   nvme_ctrlr_set_num_queues_done, ctrlr);
	if (rc != 0) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return rc;
	}

	return 0;
}

static void
nvme_ctrlr_set_keep_alive_timeout_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	uint32_t keep_alive_interval_us;
	struct spdk_nvme_ctrlr *ctrlr = (struct spdk_nvme_ctrlr *)arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		if ((cpl->status.sct == SPDK_NVME_SCT_GENERIC) &&
		    (cpl->status.sc == SPDK_NVME_SC_INVALID_FIELD)) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "Keep alive timeout Get Feature is not supported\n");
		} else {
			NVME_CTRLR_ERRLOG(ctrlr, "Keep alive timeout Get Feature failed: SC %x SCT %x\n",
					  cpl->status.sc, cpl->status.sct);
			ctrlr->opts.keep_alive_timeout_ms = 0;
			nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
			return;
		}
	} else {
		if (ctrlr->opts.keep_alive_timeout_ms != cpl->cdw0) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "Controller adjusted keep alive timeout to %u ms\n",
					    cpl->cdw0);
		}

		ctrlr->opts.keep_alive_timeout_ms = cpl->cdw0;
	}

	if (ctrlr->opts.keep_alive_timeout_ms == 0) {
		ctrlr->keep_alive_interval_ticks = 0;
	} else {
		keep_alive_interval_us = ctrlr->opts.keep_alive_timeout_ms * 1000 / 2;

		NVME_CTRLR_DEBUGLOG(ctrlr, "Sending keep alive every %u us\n", keep_alive_interval_us);

		ctrlr->keep_alive_interval_ticks = (keep_alive_interval_us * spdk_get_ticks_hz()) /
						   UINT64_C(1000000);

		/* Schedule the first Keep Alive to be sent as soon as possible. */
		ctrlr->next_keep_alive_tick = spdk_get_ticks();
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_HOST_ID,
			     ctrlr->opts.admin_timeout_ms);
}

static int
nvme_ctrlr_set_keep_alive_timeout(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc;

	if (ctrlr->opts.keep_alive_timeout_ms == 0) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_HOST_ID,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	if (ctrlr->cdata.kas == 0) {
		NVME_CTRLR_DEBUGLOG(ctrlr, "Controller KAS is 0 - not enabling Keep Alive\n");
		ctrlr->opts.keep_alive_timeout_ms = 0;
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_HOST_ID,
				     ctrlr->opts.admin_timeout_ms);
		return 0;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_KEEP_ALIVE_TIMEOUT,
			     ctrlr->opts.admin_timeout_ms);

	/* Retrieve actual keep alive timeout, since the controller may have adjusted it. */
	rc = spdk_nvme_ctrlr_cmd_get_feature(ctrlr, SPDK_NVME_FEAT_KEEP_ALIVE_TIMER, 0, NULL, 0,
					     nvme_ctrlr_set_keep_alive_timeout_done, ctrlr);
	if (rc != 0) {
		NVME_CTRLR_ERRLOG(ctrlr, "Keep alive timeout Get Feature failed: %d\n", rc);
		ctrlr->opts.keep_alive_timeout_ms = 0;
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return rc;
	}

	return 0;
}

static void
nvme_ctrlr_set_host_id_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct spdk_nvme_ctrlr *ctrlr = (struct spdk_nvme_ctrlr *)arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		/*
		 * Treat Set Features - Host ID failure as non-fatal, since the Host ID feature
		 * is optional.
		 */
		NVME_CTRLR_WARNLOG(ctrlr, "Set Features - Host ID failed: SC 0x%x SCT 0x%x\n",
				   cpl->status.sc, cpl->status.sct);
	} else {
		NVME_CTRLR_DEBUGLOG(ctrlr, "Set Features - Host ID was successful\n");
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_READY, NVME_TIMEOUT_INFINITE);
}

static int
nvme_ctrlr_set_host_id(struct spdk_nvme_ctrlr *ctrlr)
{
	uint8_t *host_id;
	uint32_t host_id_size;
	int rc;

	if (ctrlr->trid.trtype != SPDK_NVME_TRANSPORT_PCIE) {
		/*
		 * NVMe-oF sends the host ID during Connect and doesn't allow
		 * Set Features - Host Identifier after Connect, so we don't need to do anything here.
		 */
		NVME_CTRLR_DEBUGLOG(ctrlr, "NVMe-oF transport - not sending Set Features - Host ID\n");
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_READY, NVME_TIMEOUT_INFINITE);
		return 0;
	}

	if (ctrlr->cdata.ctratt.host_id_exhid_supported) {
		NVME_CTRLR_DEBUGLOG(ctrlr, "Using 128-bit extended host identifier\n");
		host_id = ctrlr->opts.extended_host_id;
		host_id_size = sizeof(ctrlr->opts.extended_host_id);
	} else {
		NVME_CTRLR_DEBUGLOG(ctrlr, "Using 64-bit host identifier\n");
		host_id = ctrlr->opts.host_id;
		host_id_size = sizeof(ctrlr->opts.host_id);
	}

	/* If the user specified an all-zeroes host identifier, don't send the command. */
	if (spdk_mem_all_zero(host_id, host_id_size)) {
		NVME_CTRLR_DEBUGLOG(ctrlr, "User did not specify host ID - not sending Set Features - Host ID\n");
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_READY, NVME_TIMEOUT_INFINITE);
		return 0;
	}

	SPDK_LOGDUMP(nvme, "host_id", host_id, host_id_size);

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_HOST_ID,
			     ctrlr->opts.admin_timeout_ms);

	rc = nvme_ctrlr_cmd_set_host_id(ctrlr, host_id, host_id_size, nvme_ctrlr_set_host_id_done, ctrlr);
	if (rc != 0) {
		NVME_CTRLR_ERRLOG(ctrlr, "Set Features - Host ID failed: %d\n", rc);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return rc;
	}

	return 0;
}

static void
nvme_ctrlr_destruct_namespaces(struct spdk_nvme_ctrlr *ctrlr)
{
	if (ctrlr->ns) {
		uint32_t i, num_ns = ctrlr->num_ns;

		for (i = 0; i < num_ns; i++) {
			nvme_ns_destruct(&ctrlr->ns[i]);
		}

		spdk_free(ctrlr->ns);
		ctrlr->ns = NULL;
		ctrlr->num_ns = 0;
	}

	spdk_free(ctrlr->active_ns_list);
	ctrlr->active_ns_list = NULL;
}

static void
nvme_ctrlr_update_namespaces(struct spdk_nvme_ctrlr *ctrlr)
{
	uint32_t i, nn = ctrlr->cdata.nn;
	struct spdk_nvme_ns_data *nsdata;
	bool ns_is_active;

	for (i = 0; i < nn; i++) {
		struct spdk_nvme_ns	*ns = &ctrlr->ns[i];
		uint32_t		nsid = i + 1;

		nsdata = &ns->nsdata;
		ns_is_active = spdk_nvme_ctrlr_is_active_ns(ctrlr, nsid);

		if (nsdata->ncap && ns_is_active) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "Namespace %u was updated\n", nsid);
			if (nvme_ns_update(ns) != 0) {
				NVME_CTRLR_ERRLOG(ctrlr, "Failed to update active NS %u\n", nsid);
				continue;
			}
		}

		if ((nsdata->ncap == 0) && ns_is_active) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "Namespace %u was added\n", nsid);
			if (nvme_ns_construct(ns, nsid, ctrlr) != 0) {
				continue;
			}
		}

		if (nsdata->ncap && !ns_is_active) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "Namespace %u was removed\n", nsid);
			nvme_ns_destruct(ns);
		}
	}
}

static int
nvme_ctrlr_construct_namespaces(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc = 0;
	uint32_t i, nn = ctrlr->cdata.nn;

	/* ctrlr->num_ns may be 0 (startup) or a different number of namespaces (reset),
	 * so check if we need to reallocate.
	 */
	if (nn != ctrlr->num_ns) {
		nvme_ctrlr_destruct_namespaces(ctrlr);

		if (nn == 0) {
			NVME_CTRLR_WARNLOG(ctrlr, "controller has 0 namespaces\n");
			return 0;
		}

		ctrlr->ns = spdk_zmalloc(nn * sizeof(struct spdk_nvme_ns), 64, NULL,
					 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_SHARE);
		if (ctrlr->ns == NULL) {
			rc = -ENOMEM;
			goto fail;
		}

		ctrlr->num_ns = nn;
	} else {
		/*
		 * The controller could have been reset with the same number of namespaces.
		 * If so, we still need to free the iocs specific data, to get a clean slate.
		 */
		for (i = 0; i < ctrlr->num_ns; i++) {
			nvme_ns_free_iocs_specific_data(&ctrlr->ns[i]);
		}
	}

	return 0;

fail:
	nvme_ctrlr_destruct_namespaces(ctrlr);
	return rc;
}

void
nvme_ctrlr_process_async_event(struct spdk_nvme_ctrlr *ctrlr,
			       const struct spdk_nvme_cpl *cpl)
{
	union spdk_nvme_async_event_completion event;
	struct spdk_nvme_ctrlr_process *active_proc;
	int rc;

	event.raw = cpl->cdw0;

	if ((event.bits.async_event_type == SPDK_NVME_ASYNC_EVENT_TYPE_NOTICE) &&
	    (event.bits.async_event_info == SPDK_NVME_ASYNC_EVENT_NS_ATTR_CHANGED)) {
		rc = nvme_ctrlr_identify_active_ns(ctrlr);
		if (rc) {
			return;
		}
		nvme_ctrlr_update_namespaces(ctrlr);
		nvme_io_msg_ctrlr_update(ctrlr);
	}

	if ((event.bits.async_event_type == SPDK_NVME_ASYNC_EVENT_TYPE_NOTICE) &&
	    (event.bits.async_event_info == SPDK_NVME_ASYNC_EVENT_ANA_CHANGE)) {
		rc = nvme_ctrlr_update_ana_log_page(ctrlr);
		if (rc) {
			return;
		}
		nvme_ctrlr_parse_ana_log_page(ctrlr, nvme_ctrlr_update_ns_ana_states, ctrlr);
	}

	active_proc = nvme_ctrlr_get_current_process(ctrlr);
	if (active_proc && active_proc->aer_cb_fn) {
		active_proc->aer_cb_fn(active_proc->aer_cb_arg, cpl);
	}
}

static void
nvme_ctrlr_queue_async_event(struct spdk_nvme_ctrlr *ctrlr,
			     const struct spdk_nvme_cpl *cpl)
{
	struct  spdk_nvme_ctrlr_aer_completion_list *nvme_event;

	nvme_event = calloc(1, sizeof(*nvme_event));
	if (!nvme_event) {
		NVME_CTRLR_ERRLOG(ctrlr, "Alloc nvme event failed, ignore the event\n");
		return;
	}

	nvme_event->cpl = *cpl;
	STAILQ_INSERT_TAIL(&ctrlr->async_events, nvme_event, link);
}

void
nvme_ctrlr_complete_queued_async_events(struct spdk_nvme_ctrlr *ctrlr)
{
	struct  spdk_nvme_ctrlr_aer_completion_list  *nvme_event, *nvme_event_tmp;

	STAILQ_FOREACH_SAFE(nvme_event, &ctrlr->async_events, link, nvme_event_tmp) {
		STAILQ_REMOVE(&ctrlr->async_events, nvme_event,
			      spdk_nvme_ctrlr_aer_completion_list, link);
		nvme_ctrlr_process_async_event(ctrlr, &nvme_event->cpl);
		free(nvme_event);
	}
}

static void
nvme_ctrlr_async_event_cb(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct nvme_async_event_request	*aer = arg;
	struct spdk_nvme_ctrlr		*ctrlr = aer->ctrlr;

	if (cpl->status.sct == SPDK_NVME_SCT_GENERIC &&
	    cpl->status.sc == SPDK_NVME_SC_ABORTED_SQ_DELETION) {
		/*
		 *  This is simulated when controller is being shut down, to
		 *  effectively abort outstanding asynchronous event requests
		 *  and make sure all memory is freed.  Do not repost the
		 *  request in this case.
		 */
		return;
	}

	if (cpl->status.sct == SPDK_NVME_SCT_COMMAND_SPECIFIC &&
	    cpl->status.sc == SPDK_NVME_SC_ASYNC_EVENT_REQUEST_LIMIT_EXCEEDED) {
		/*
		 *  SPDK will only send as many AERs as the device says it supports,
		 *  so this status code indicates an out-of-spec device.  Do not repost
		 *  the request in this case.
		 */
		NVME_CTRLR_ERRLOG(ctrlr, "Controller appears out-of-spec for asynchronous event request\n"
				  "handling.  Do not repost this AER.\n");
		return;
	}

	/* Add the events to the list */
	nvme_ctrlr_queue_async_event(ctrlr, cpl);

	/* If the ctrlr was removed or in the destruct state, we should not send aer again */
	if (ctrlr->is_removed || ctrlr->is_destructed) {
		return;
	}

	/*
	 * Repost another asynchronous event request to replace the one
	 *  that just completed.
	 */
	if (nvme_ctrlr_construct_and_submit_aer(ctrlr, aer)) {
		/*
		 * We can't do anything to recover from a failure here,
		 * so just print a warning message and leave the AER unsubmitted.
		 */
		NVME_CTRLR_ERRLOG(ctrlr, "resubmitting AER failed!\n");
	}
}

static int
nvme_ctrlr_construct_and_submit_aer(struct spdk_nvme_ctrlr *ctrlr,
				    struct nvme_async_event_request *aer)
{
	struct nvme_request *req;

	aer->ctrlr = ctrlr;
	req = nvme_allocate_request_null(ctrlr->adminq, nvme_ctrlr_async_event_cb, aer);
	aer->req = req;
	if (req == NULL) {
		return -1;
	}

	req->cmd.opc = SPDK_NVME_OPC_ASYNC_EVENT_REQUEST;
	return nvme_ctrlr_submit_admin_request(ctrlr, req);
}

static void
nvme_ctrlr_configure_aer_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct nvme_async_event_request		*aer;
	int					rc;
	uint32_t				i;
	struct spdk_nvme_ctrlr *ctrlr =	(struct spdk_nvme_ctrlr *)arg;

	if (spdk_nvme_cpl_is_error(cpl)) {
		NVME_CTRLR_NOTICELOG(ctrlr, "nvme_ctrlr_configure_aer failed!\n");
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_SUPPORTED_LOG_PAGES,
				     ctrlr->opts.admin_timeout_ms);
		return;
	}

	/* aerl is a zero-based value, so we need to add 1 here. */
	ctrlr->num_aers = spdk_min(NVME_MAX_ASYNC_EVENTS, (ctrlr->cdata.aerl + 1));

	for (i = 0; i < ctrlr->num_aers; i++) {
		aer = &ctrlr->aer[i];
		rc = nvme_ctrlr_construct_and_submit_aer(ctrlr, aer);
		if (rc) {
			NVME_CTRLR_ERRLOG(ctrlr, "nvme_ctrlr_construct_and_submit_aer failed!\n");
			nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
			return;
		}
	}
	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_SUPPORTED_LOG_PAGES,
			     ctrlr->opts.admin_timeout_ms);
}

static int
nvme_ctrlr_configure_aer(struct spdk_nvme_ctrlr *ctrlr)
{
	union spdk_nvme_feat_async_event_configuration	config;
	int						rc;

	config.raw = 0;
	config.bits.crit_warn.bits.available_spare = 1;
	config.bits.crit_warn.bits.temperature = 1;
	config.bits.crit_warn.bits.device_reliability = 1;
	config.bits.crit_warn.bits.read_only = 1;
	config.bits.crit_warn.bits.volatile_memory_backup = 1;

	if (ctrlr->vs.raw >= SPDK_NVME_VERSION(1, 2, 0)) {
		if (ctrlr->cdata.oaes.ns_attribute_notices) {
			config.bits.ns_attr_notice = 1;
		}
		if (ctrlr->cdata.oaes.fw_activation_notices) {
			config.bits.fw_activation_notice = 1;
		}
		if (ctrlr->cdata.oaes.ana_change_notices) {
			config.bits.ana_change_notice = 1;
		}
	}
	if (ctrlr->vs.raw >= SPDK_NVME_VERSION(1, 3, 0) && ctrlr->cdata.lpa.telemetry) {
		config.bits.telemetry_log_notice = 1;
	}

	nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_WAIT_FOR_CONFIGURE_AER,
			     ctrlr->opts.admin_timeout_ms);

	rc = nvme_ctrlr_cmd_set_async_event_config(ctrlr, config,
			nvme_ctrlr_configure_aer_done,
			ctrlr);
	if (rc != 0) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ERROR, NVME_TIMEOUT_INFINITE);
		return rc;
	}

	return 0;
}

struct spdk_nvme_ctrlr_process *
nvme_ctrlr_get_process(struct spdk_nvme_ctrlr *ctrlr, pid_t pid)
{
	struct spdk_nvme_ctrlr_process	*active_proc;

	TAILQ_FOREACH(active_proc, &ctrlr->active_procs, tailq) {
		if (active_proc->pid == pid) {
			return active_proc;
		}
	}

	return NULL;
}

struct spdk_nvme_ctrlr_process *
nvme_ctrlr_get_current_process(struct spdk_nvme_ctrlr *ctrlr)
{
	return nvme_ctrlr_get_process(ctrlr, getpid());
}

/**
 * This function will be called when a process is using the controller.
 *  1. For the primary process, it is called when constructing the controller.
 *  2. For the secondary process, it is called at probing the controller.
 * Note: will check whether the process is already added for the same process.
 */
int
nvme_ctrlr_add_process(struct spdk_nvme_ctrlr *ctrlr, void *devhandle)
{
	struct spdk_nvme_ctrlr_process	*ctrlr_proc;
	pid_t				pid = getpid();

	/* Check whether the process is already added or not */
	if (nvme_ctrlr_get_process(ctrlr, pid)) {
		return 0;
	}

	/* Initialize the per process properties for this ctrlr */
	ctrlr_proc = spdk_zmalloc(sizeof(struct spdk_nvme_ctrlr_process),
				  64, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_SHARE);
	if (ctrlr_proc == NULL) {
		NVME_CTRLR_ERRLOG(ctrlr, "failed to allocate memory to track the process props\n");

		return -1;
	}

	ctrlr_proc->is_primary = spdk_process_is_primary();
	ctrlr_proc->pid = pid;
	STAILQ_INIT(&ctrlr_proc->active_reqs);
	ctrlr_proc->devhandle = devhandle;
	ctrlr_proc->ref = 0;
	TAILQ_INIT(&ctrlr_proc->allocated_io_qpairs);

	TAILQ_INSERT_TAIL(&ctrlr->active_procs, ctrlr_proc, tailq);

	return 0;
}

/**
 * This function will be called when the process detaches the controller.
 * Note: the ctrlr_lock must be held when calling this function.
 */
static void
nvme_ctrlr_remove_process(struct spdk_nvme_ctrlr *ctrlr,
			  struct spdk_nvme_ctrlr_process *proc)
{
	struct spdk_nvme_qpair	*qpair, *tmp_qpair;

	assert(STAILQ_EMPTY(&proc->active_reqs));

	TAILQ_FOREACH_SAFE(qpair, &proc->allocated_io_qpairs, per_process_tailq, tmp_qpair) {
		spdk_nvme_ctrlr_free_io_qpair(qpair);
	}

	TAILQ_REMOVE(&ctrlr->active_procs, proc, tailq);

	if (ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
		spdk_pci_device_detach(proc->devhandle);
	}

	spdk_free(proc);
}

/**
 * This function will be called when the process exited unexpectedly
 *  in order to free any incomplete nvme request, allocated IO qpairs
 *  and allocated memory.
 * Note: the ctrlr_lock must be held when calling this function.
 */
static void
nvme_ctrlr_cleanup_process(struct spdk_nvme_ctrlr_process *proc)
{
	struct nvme_request	*req, *tmp_req;
	struct spdk_nvme_qpair	*qpair, *tmp_qpair;

	STAILQ_FOREACH_SAFE(req, &proc->active_reqs, stailq, tmp_req) {
		STAILQ_REMOVE(&proc->active_reqs, req, nvme_request, stailq);

		assert(req->pid == proc->pid);

		nvme_free_request(req);
	}

	TAILQ_FOREACH_SAFE(qpair, &proc->allocated_io_qpairs, per_process_tailq, tmp_qpair) {
		TAILQ_REMOVE(&proc->allocated_io_qpairs, qpair, per_process_tailq);

		/*
		 * The process may have been killed while some qpairs were in their
		 *  completion context.  Clear that flag here to allow these IO
		 *  qpairs to be deleted.
		 */
		qpair->in_completion_context = 0;

		qpair->no_deletion_notification_needed = 1;

		spdk_nvme_ctrlr_free_io_qpair(qpair);
	}

	spdk_free(proc);
}

/**
 * This function will be called when destructing the controller.
 *  1. There is no more admin request on this controller.
 *  2. Clean up any left resource allocation when its associated process is gone.
 */
void
nvme_ctrlr_free_processes(struct spdk_nvme_ctrlr *ctrlr)
{
	struct spdk_nvme_ctrlr_process	*active_proc, *tmp;

	/* Free all the processes' properties and make sure no pending admin IOs */
	TAILQ_FOREACH_SAFE(active_proc, &ctrlr->active_procs, tailq, tmp) {
		TAILQ_REMOVE(&ctrlr->active_procs, active_proc, tailq);

		assert(STAILQ_EMPTY(&active_proc->active_reqs));

		spdk_free(active_proc);
	}
}

/**
 * This function will be called when any other process attaches or
 *  detaches the controller in order to cleanup those unexpectedly
 *  terminated processes.
 * Note: the ctrlr_lock must be held when calling this function.
 */
static int
nvme_ctrlr_remove_inactive_proc(struct spdk_nvme_ctrlr *ctrlr)
{
	struct spdk_nvme_ctrlr_process	*active_proc, *tmp;
	int				active_proc_count = 0;

	TAILQ_FOREACH_SAFE(active_proc, &ctrlr->active_procs, tailq, tmp) {
		if ((kill(active_proc->pid, 0) == -1) && (errno == ESRCH)) {
			NVME_CTRLR_ERRLOG(ctrlr, "process %d terminated unexpected\n", active_proc->pid);

			TAILQ_REMOVE(&ctrlr->active_procs, active_proc, tailq);

			nvme_ctrlr_cleanup_process(active_proc);
		} else {
			active_proc_count++;
		}
	}

	return active_proc_count;
}

void
nvme_ctrlr_proc_get_ref(struct spdk_nvme_ctrlr *ctrlr)
{
	struct spdk_nvme_ctrlr_process	*active_proc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	nvme_ctrlr_remove_inactive_proc(ctrlr);

	active_proc = nvme_ctrlr_get_current_process(ctrlr);
	if (active_proc) {
		active_proc->ref++;
	}

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

void
nvme_ctrlr_proc_put_ref(struct spdk_nvme_ctrlr *ctrlr)
{
	struct spdk_nvme_ctrlr_process	*active_proc;
	int				proc_count;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	proc_count = nvme_ctrlr_remove_inactive_proc(ctrlr);

	active_proc = nvme_ctrlr_get_current_process(ctrlr);
	if (active_proc) {
		active_proc->ref--;
		assert(active_proc->ref >= 0);

		/*
		 * The last active process will be removed at the end of
		 * the destruction of the controller.
		 */
		if (active_proc->ref == 0 && proc_count != 1) {
			nvme_ctrlr_remove_process(ctrlr, active_proc);
		}
	}

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

int
nvme_ctrlr_get_ref_count(struct spdk_nvme_ctrlr *ctrlr)
{
	struct spdk_nvme_ctrlr_process	*active_proc;
	int				ref = 0;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	nvme_ctrlr_remove_inactive_proc(ctrlr);

	TAILQ_FOREACH(active_proc, &ctrlr->active_procs, tailq) {
		ref += active_proc->ref;
	}

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return ref;
}

/**
 *  Get the PCI device handle which is only visible to its associated process.
 */
struct spdk_pci_device *
nvme_ctrlr_proc_get_devhandle(struct spdk_nvme_ctrlr *ctrlr)
{
	struct spdk_nvme_ctrlr_process	*active_proc;
	struct spdk_pci_device		*devhandle = NULL;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	active_proc = nvme_ctrlr_get_current_process(ctrlr);
	if (active_proc) {
		devhandle = active_proc->devhandle;
	}

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return devhandle;
}

/**
 * This function will be called repeatedly during initialization until the controller is ready.
 */
int
nvme_ctrlr_process_init(struct spdk_nvme_ctrlr *ctrlr)
{
	union spdk_nvme_cc_register cc;
	union spdk_nvme_csts_register csts;
	uint32_t ready_timeout_in_ms;
	uint64_t ticks;
	int rc = 0;

	ticks = spdk_get_ticks();

	/*
	 * May need to avoid accessing any register on the target controller
	 * for a while. Return early without touching the FSM.
	 * Check sleep_timeout_tsc > 0 for unit test.
	 */
	if ((ctrlr->sleep_timeout_tsc > 0) &&
	    (ticks <= ctrlr->sleep_timeout_tsc)) {
		return 0;
	}
	ctrlr->sleep_timeout_tsc = 0;

	if (nvme_ctrlr_get_cc(ctrlr, &cc) ||
	    nvme_ctrlr_get_csts(ctrlr, &csts)) {
		if (!ctrlr->is_failed && ctrlr->state_timeout_tsc != NVME_TIMEOUT_INFINITE) {
			/* While a device is resetting, it may be unable to service MMIO reads
			 * temporarily. Allow for this case.
			 */
			NVME_CTRLR_DEBUGLOG(ctrlr, "Get registers failed while waiting for CSTS.RDY == 0\n");
			goto init_timeout;
		}
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to read CC and CSTS in state %d\n", ctrlr->state);
		return -EIO;
	}

	ready_timeout_in_ms = 500 * ctrlr->cap.bits.to;

	/*
	 * Check if the current initialization step is done or has timed out.
	 */
	switch (ctrlr->state) {
	case NVME_CTRLR_STATE_INIT_DELAY:
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_INIT, ready_timeout_in_ms);
		if (ctrlr->quirks & NVME_QUIRK_DELAY_BEFORE_INIT) {
			/*
			 * Controller may need some delay before it's enabled.
			 *
			 * This is a workaround for an issue where the PCIe-attached NVMe controller
			 * is not ready after VFIO reset. We delay the initialization rather than the
			 * enabling itself, because this is required only for the very first enabling
			 * - directly after a VFIO reset.
			 */
			NVME_CTRLR_DEBUGLOG(ctrlr, "Adding 2 second delay before initializing the controller\n");
			ctrlr->sleep_timeout_tsc = ticks + (2000 * spdk_get_ticks_hz() / 1000);
		}
		break;

	case NVME_CTRLR_STATE_INIT:
		/* Begin the hardware initialization by making sure the controller is disabled. */
		if (cc.bits.en) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "CC.EN = 1\n");
			/*
			 * Controller is currently enabled. We need to disable it to cause a reset.
			 *
			 * If CC.EN = 1 && CSTS.RDY = 0, the controller is in the process of becoming ready.
			 *  Wait for the ready bit to be 1 before disabling the controller.
			 */
			if (csts.bits.rdy == 0) {
				NVME_CTRLR_DEBUGLOG(ctrlr, "CC.EN = 1 && CSTS.RDY = 0 - waiting for reset to complete\n");
				nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_1, ready_timeout_in_ms);
				return 0;
			}

			/* CC.EN = 1 && CSTS.RDY == 1, so we can immediately disable the controller. */
			NVME_CTRLR_DEBUGLOG(ctrlr, "Setting CC.EN = 0\n");
			cc.bits.en = 0;
			if (nvme_ctrlr_set_cc(ctrlr, &cc)) {
				NVME_CTRLR_ERRLOG(ctrlr, "set_cc() failed\n");
				return -EIO;
			}
			nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_0, ready_timeout_in_ms);

			/*
			 * Wait 2.5 seconds before accessing PCI registers.
			 * Not using sleep() to avoid blocking other controller's initialization.
			 */
			if (ctrlr->quirks & NVME_QUIRK_DELAY_BEFORE_CHK_RDY) {
				NVME_CTRLR_DEBUGLOG(ctrlr, "Applying quirk: delay 2.5 seconds before reading registers\n");
				ctrlr->sleep_timeout_tsc = ticks + (2500 * spdk_get_ticks_hz() / 1000);
			}
			return 0;
		} else {
			if (csts.bits.rdy == 1) {
				NVME_CTRLR_DEBUGLOG(ctrlr, "CC.EN = 0 && CSTS.RDY = 1 - waiting for shutdown to complete\n");
			}

			nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_0, ready_timeout_in_ms);
			return 0;
		}
		break;

	case NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_1:
		if (csts.bits.rdy == 1) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "CC.EN = 1 && CSTS.RDY = 1 - disabling controller\n");
			/* CC.EN = 1 && CSTS.RDY = 1, so we can set CC.EN = 0 now. */
			NVME_CTRLR_DEBUGLOG(ctrlr, "Setting CC.EN = 0\n");
			cc.bits.en = 0;
			if (nvme_ctrlr_set_cc(ctrlr, &cc)) {
				NVME_CTRLR_ERRLOG(ctrlr, "set_cc() failed\n");
				return -EIO;
			}
			nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_0, ready_timeout_in_ms);
			return 0;
		}
		break;

	case NVME_CTRLR_STATE_DISABLE_WAIT_FOR_READY_0:
		if (csts.bits.rdy == 0) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "CC.EN = 0 && CSTS.RDY = 0\n");
			nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ENABLE, ready_timeout_in_ms);
			/*
			 * Delay 100us before setting CC.EN = 1.  Some NVMe SSDs miss CC.EN getting
			 *  set to 1 if it is too soon after CSTS.RDY is reported as 0.
			 */
			spdk_delay_us(100);
			return 0;
		}
		break;

	case NVME_CTRLR_STATE_ENABLE:
		NVME_CTRLR_DEBUGLOG(ctrlr, "Setting CC.EN = 1\n");
		rc = nvme_ctrlr_enable(ctrlr);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_ENABLE_WAIT_FOR_READY_1, ready_timeout_in_ms);
		return rc;

	case NVME_CTRLR_STATE_ENABLE_WAIT_FOR_READY_1:
		if (csts.bits.rdy == 1) {
			NVME_CTRLR_DEBUGLOG(ctrlr, "CC.EN = 1 && CSTS.RDY = 1 - controller is ready\n");
			/*
			 * The controller has been enabled.
			 *  Perform the rest of initialization serially.
			 */
			nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_RESET_ADMIN_QUEUE,
					     ctrlr->opts.admin_timeout_ms);
			return 0;
		}
		break;

	case NVME_CTRLR_STATE_RESET_ADMIN_QUEUE:
		nvme_transport_qpair_reset(ctrlr->adminq);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY,
				     ctrlr->opts.admin_timeout_ms);
		break;

	case NVME_CTRLR_STATE_IDENTIFY:
		rc = nvme_ctrlr_identify(ctrlr);
		break;

	case NVME_CTRLR_STATE_IDENTIFY_IOCS_SPECIFIC:
		rc = nvme_ctrlr_identify_iocs_specific(ctrlr);
		break;

	case NVME_CTRLR_STATE_GET_ZNS_CMD_EFFECTS_LOG:
		rc = nvme_ctrlr_get_zns_cmd_and_effects_log(ctrlr);
		break;

	case NVME_CTRLR_STATE_SET_NUM_QUEUES:
		nvme_ctrlr_update_nvmf_ioccsz(ctrlr);
		rc = nvme_ctrlr_set_num_queues(ctrlr);
		break;

	case NVME_CTRLR_STATE_CONSTRUCT_NS:
		rc = nvme_ctrlr_construct_namespaces(ctrlr);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_IDENTIFY_ACTIVE_NS,
				     ctrlr->opts.admin_timeout_ms);
		break;

	case NVME_CTRLR_STATE_IDENTIFY_ACTIVE_NS:
		_nvme_ctrlr_identify_active_ns(ctrlr);
		break;

	case NVME_CTRLR_STATE_IDENTIFY_NS:
		rc = nvme_ctrlr_identify_namespaces(ctrlr);
		break;

	case NVME_CTRLR_STATE_IDENTIFY_ID_DESCS:
		rc = nvme_ctrlr_identify_id_desc_namespaces(ctrlr);
		break;

	case NVME_CTRLR_STATE_IDENTIFY_NS_IOCS_SPECIFIC:
		rc = nvme_ctrlr_identify_namespaces_iocs_specific(ctrlr);
		break;

	case NVME_CTRLR_STATE_CONFIGURE_AER:
		rc = nvme_ctrlr_configure_aer(ctrlr);
		break;

	case NVME_CTRLR_STATE_SET_SUPPORTED_LOG_PAGES:
		rc = nvme_ctrlr_set_supported_log_pages(ctrlr);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_SUPPORTED_FEATURES,
				     ctrlr->opts.admin_timeout_ms);
		break;

	case NVME_CTRLR_STATE_SET_SUPPORTED_FEATURES:
		nvme_ctrlr_set_supported_features(ctrlr);
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_SET_DB_BUF_CFG,
				     ctrlr->opts.admin_timeout_ms);
		break;

	case NVME_CTRLR_STATE_SET_DB_BUF_CFG:
		rc = nvme_ctrlr_set_doorbell_buffer_config(ctrlr);
		break;

	case NVME_CTRLR_STATE_SET_KEEP_ALIVE_TIMEOUT:
		rc = nvme_ctrlr_set_keep_alive_timeout(ctrlr);
		break;

	case NVME_CTRLR_STATE_SET_HOST_ID:
		rc = nvme_ctrlr_set_host_id(ctrlr);
		break;

	case NVME_CTRLR_STATE_READY:
		NVME_CTRLR_DEBUGLOG(ctrlr, "Ctrlr already in ready state\n");
		return 0;

	case NVME_CTRLR_STATE_ERROR:
		NVME_CTRLR_ERRLOG(ctrlr, "Ctrlr is in error state\n");
		return -1;

	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY:
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_IOCS_SPECIFIC:
	case NVME_CTRLR_STATE_WAIT_FOR_GET_ZNS_CMD_EFFECTS_LOG:
	case NVME_CTRLR_STATE_WAIT_FOR_SET_NUM_QUEUES:
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_ACTIVE_NS:
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_NS:
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_ID_DESCS:
	case NVME_CTRLR_STATE_WAIT_FOR_IDENTIFY_NS_IOCS_SPECIFIC:
	case NVME_CTRLR_STATE_WAIT_FOR_CONFIGURE_AER:
	case NVME_CTRLR_STATE_WAIT_FOR_DB_BUF_CFG:
	case NVME_CTRLR_STATE_WAIT_FOR_KEEP_ALIVE_TIMEOUT:
	case NVME_CTRLR_STATE_WAIT_FOR_HOST_ID:
		spdk_nvme_qpair_process_completions(ctrlr->adminq, 0);
		break;

	default:
		assert(0);
		return -1;
	}

init_timeout:
	/* Note: we use the ticks captured when we entered this function.
	 * This covers environments where the SPDK process gets swapped out after
	 * we tried to advance the state but before we check the timeout here.
	 * It is not normal for this to happen, but harmless to handle it in this
	 * way.
	 */
	if (ctrlr->state_timeout_tsc != NVME_TIMEOUT_INFINITE &&
	    ticks > ctrlr->state_timeout_tsc) {
		NVME_CTRLR_ERRLOG(ctrlr, "Initialization timed out in state %d\n", ctrlr->state);
		return -1;
	}

	return rc;
}

int
nvme_robust_mutex_init_recursive_shared(pthread_mutex_t *mtx)
{
	pthread_mutexattr_t attr;
	int rc = 0;

	if (pthread_mutexattr_init(&attr)) {
		return -1;
	}
	if (pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) ||
#ifndef __FreeBSD__
	    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST) ||
	    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) ||
#endif
	    pthread_mutex_init(mtx, &attr)) {
		rc = -1;
	}
	pthread_mutexattr_destroy(&attr);
	return rc;
}

int
nvme_ctrlr_construct(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc;

	if (ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_INIT_DELAY, NVME_TIMEOUT_INFINITE);
	} else {
		nvme_ctrlr_set_state(ctrlr, NVME_CTRLR_STATE_INIT, NVME_TIMEOUT_INFINITE);
	}

	if (ctrlr->opts.admin_queue_size > SPDK_NVME_ADMIN_QUEUE_MAX_ENTRIES) {
		NVME_CTRLR_ERRLOG(ctrlr, "admin_queue_size %u exceeds max defined by NVMe spec, use max value\n",
				  ctrlr->opts.admin_queue_size);
		ctrlr->opts.admin_queue_size = SPDK_NVME_ADMIN_QUEUE_MAX_ENTRIES;
	}

	if (ctrlr->opts.admin_queue_size < SPDK_NVME_ADMIN_QUEUE_MIN_ENTRIES) {
		NVME_CTRLR_ERRLOG(ctrlr,
				  "admin_queue_size %u is less than minimum defined by NVMe spec, use min value\n",
				  ctrlr->opts.admin_queue_size);
		ctrlr->opts.admin_queue_size = SPDK_NVME_ADMIN_QUEUE_MIN_ENTRIES;
	}

	ctrlr->flags = 0;
	ctrlr->free_io_qids = NULL;
	ctrlr->is_resetting = false;
	ctrlr->is_failed = false;
	ctrlr->is_destructed = false;

	TAILQ_INIT(&ctrlr->active_io_qpairs);
	STAILQ_INIT(&ctrlr->queued_aborts);
	STAILQ_INIT(&ctrlr->async_events);
	ctrlr->outstanding_aborts = 0;

	ctrlr->ana_log_page = NULL;
	ctrlr->ana_log_page_size = 0;

	rc = nvme_robust_mutex_init_recursive_shared(&ctrlr->ctrlr_lock);
	if (rc != 0) {
		return rc;
	}

	TAILQ_INIT(&ctrlr->active_procs);

	return rc;
}

/* This function should be called once at ctrlr initialization to set up constant properties. */
void
nvme_ctrlr_init_cap(struct spdk_nvme_ctrlr *ctrlr, const union spdk_nvme_cap_register *cap,
		    const union spdk_nvme_vs_register *vs)
{
	ctrlr->cap = *cap;
	ctrlr->vs = *vs;

	if (ctrlr->cap.bits.ams & SPDK_NVME_CAP_AMS_WRR) {
		ctrlr->flags |= SPDK_NVME_CTRLR_WRR_SUPPORTED;
	}

	ctrlr->min_page_size = 1u << (12 + ctrlr->cap.bits.mpsmin);

	/* For now, always select page_size == min_page_size. */
	ctrlr->page_size = ctrlr->min_page_size;

	ctrlr->opts.io_queue_size = spdk_max(ctrlr->opts.io_queue_size, SPDK_NVME_IO_QUEUE_MIN_ENTRIES);
	ctrlr->opts.io_queue_size = spdk_min(ctrlr->opts.io_queue_size, MAX_IO_QUEUE_ENTRIES);
	ctrlr->opts.io_queue_size = spdk_min(ctrlr->opts.io_queue_size, ctrlr->cap.bits.mqes + 1u);

	ctrlr->opts.io_queue_requests = spdk_max(ctrlr->opts.io_queue_requests, ctrlr->opts.io_queue_size);
}

void
nvme_ctrlr_destruct_finish(struct spdk_nvme_ctrlr *ctrlr)
{
	pthread_mutex_destroy(&ctrlr->ctrlr_lock);
}

void
nvme_ctrlr_destruct_async(struct spdk_nvme_ctrlr *ctrlr,
			  struct nvme_ctrlr_detach_ctx *ctx)
{
	struct spdk_nvme_qpair *qpair, *tmp;

	NVME_CTRLR_DEBUGLOG(ctrlr, "Prepare to destruct SSD\n");

	ctrlr->is_destructed = true;

	spdk_nvme_qpair_process_completions(ctrlr->adminq, 0);

	nvme_ctrlr_abort_queued_aborts(ctrlr);
	nvme_transport_admin_qpair_abort_aers(ctrlr->adminq);

	TAILQ_FOREACH_SAFE(qpair, &ctrlr->active_io_qpairs, tailq, tmp) {
		spdk_nvme_ctrlr_free_io_qpair(qpair);
	}

	nvme_ctrlr_free_doorbell_buffer(ctrlr);
	nvme_ctrlr_free_iocs_specific_data(ctrlr);

	if (ctrlr->opts.no_shn_notification) {
		NVME_CTRLR_INFOLOG(ctrlr, "Disable SSD without shutdown notification\n");
		nvme_ctrlr_disable(ctrlr);
		ctx->shutdown_complete = true;
	} else {
		nvme_ctrlr_shutdown_async(ctrlr, ctx);
	}
}

int
nvme_ctrlr_destruct_poll_async(struct spdk_nvme_ctrlr *ctrlr,
			       struct nvme_ctrlr_detach_ctx *ctx)
{
	int rc = 0;

	if (!ctx->shutdown_complete) {
		rc = nvme_ctrlr_shutdown_poll_async(ctrlr, ctx);
		if (rc == -EAGAIN) {
			return -EAGAIN;
		}
		/* Destruct ctrlr forcefully for any other error. */
	}

	if (ctx->cb_fn) {
		ctx->cb_fn(ctrlr);
	}

	nvme_ctrlr_destruct_namespaces(ctrlr);

	spdk_bit_array_free(&ctrlr->free_io_qids);

	spdk_free(ctrlr->ana_log_page);
	ctrlr->ana_log_page = NULL;
	ctrlr->ana_log_page_size = 0;

	nvme_transport_ctrlr_destruct(ctrlr);

	return rc;
}

void
nvme_ctrlr_destruct(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_ctrlr_detach_ctx ctx = {};
	int rc;

	nvme_ctrlr_destruct_async(ctrlr, &ctx);

	while (1) {
		rc = nvme_ctrlr_destruct_poll_async(ctrlr, &ctx);
		if (rc != -EAGAIN) {
			break;
		}
		nvme_delay(1000);
	}
}

int
nvme_ctrlr_submit_admin_request(struct spdk_nvme_ctrlr *ctrlr,
				struct nvme_request *req)
{
	return nvme_qpair_submit_request(ctrlr->adminq, req);
}

static void
nvme_keep_alive_completion(void *cb_ctx, const struct spdk_nvme_cpl *cpl)
{
	/* Do nothing */
}

/*
 * Check if we need to send a Keep Alive command.
 * Caller must hold ctrlr->ctrlr_lock.
 */
static int
nvme_ctrlr_keep_alive(struct spdk_nvme_ctrlr *ctrlr)
{
	uint64_t now;
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc = 0;

	now = spdk_get_ticks();
	if (now < ctrlr->next_keep_alive_tick) {
		return rc;
	}

	req = nvme_allocate_request_null(ctrlr->adminq, nvme_keep_alive_completion, NULL);
	if (req == NULL) {
		return rc;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_KEEP_ALIVE;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	if (rc != 0) {
		NVME_CTRLR_ERRLOG(ctrlr, "Submitting Keep Alive failed\n");
		rc = -ENXIO;
	}

	ctrlr->next_keep_alive_tick = now + ctrlr->keep_alive_interval_ticks;
	return rc;
}

int32_t
spdk_nvme_ctrlr_process_admin_completions(struct spdk_nvme_ctrlr *ctrlr)
{
	int32_t num_completions;
	int32_t rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	if (ctrlr->keep_alive_interval_ticks) {
		rc = nvme_ctrlr_keep_alive(ctrlr);
		if (rc) {
			nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
			return rc;
		}
	}

	rc = nvme_io_msg_process(ctrlr);
	if (rc < 0) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return rc;
	}
	num_completions = rc;

	rc = spdk_nvme_qpair_process_completions(ctrlr->adminq, 0);

	nvme_ctrlr_complete_queued_async_events(ctrlr);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	if (rc < 0) {
		num_completions = rc;
	} else {
		num_completions += rc;
	}

	return num_completions;
}

const struct spdk_nvme_ctrlr_data *
spdk_nvme_ctrlr_get_data(struct spdk_nvme_ctrlr *ctrlr)
{
	return &ctrlr->cdata;
}

union spdk_nvme_csts_register spdk_nvme_ctrlr_get_regs_csts(struct spdk_nvme_ctrlr *ctrlr)
{
	union spdk_nvme_csts_register csts;

	if (nvme_ctrlr_get_csts(ctrlr, &csts)) {
		csts.raw = SPDK_NVME_INVALID_REGISTER_VALUE;
	}
	return csts;
}

union spdk_nvme_cap_register spdk_nvme_ctrlr_get_regs_cap(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->cap;
}

union spdk_nvme_vs_register spdk_nvme_ctrlr_get_regs_vs(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->vs;
}

union spdk_nvme_cmbsz_register spdk_nvme_ctrlr_get_regs_cmbsz(struct spdk_nvme_ctrlr *ctrlr)
{
	union spdk_nvme_cmbsz_register cmbsz;

	if (nvme_ctrlr_get_cmbsz(ctrlr, &cmbsz)) {
		cmbsz.raw = 0;
	}

	return cmbsz;
}

union spdk_nvme_pmrcap_register spdk_nvme_ctrlr_get_regs_pmrcap(struct spdk_nvme_ctrlr *ctrlr)
{
	union spdk_nvme_pmrcap_register pmrcap;

	if (nvme_ctrlr_get_pmrcap(ctrlr, &pmrcap)) {
		pmrcap.raw = 0;
	}

	return pmrcap;
}

uint64_t
spdk_nvme_ctrlr_get_pmrsz(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->pmr_size;
}

uint32_t
spdk_nvme_ctrlr_get_num_ns(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->num_ns;
}

static int32_t
nvme_ctrlr_active_ns_idx(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid)
{
	int32_t result = -1;

	if (ctrlr->active_ns_list == NULL || nsid == 0 || nsid > ctrlr->num_ns) {
		return result;
	}

	int32_t lower = 0;
	int32_t upper = ctrlr->num_ns - 1;
	int32_t mid;

	while (lower <= upper) {
		mid = lower + (upper - lower) / 2;
		if (ctrlr->active_ns_list[mid] == nsid) {
			result = mid;
			break;
		} else {
			if (ctrlr->active_ns_list[mid] != 0 && ctrlr->active_ns_list[mid] < nsid) {
				lower = mid + 1;
			} else {
				upper = mid - 1;
			}

		}
	}

	return result;
}

bool
spdk_nvme_ctrlr_is_active_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid)
{
	return nvme_ctrlr_active_ns_idx(ctrlr, nsid) != -1;
}

uint32_t
spdk_nvme_ctrlr_get_first_active_ns(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->active_ns_list ? ctrlr->active_ns_list[0] : 0;
}

uint32_t
spdk_nvme_ctrlr_get_next_active_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t prev_nsid)
{
	int32_t nsid_idx = nvme_ctrlr_active_ns_idx(ctrlr, prev_nsid);
	if (ctrlr->active_ns_list && nsid_idx >= 0 && (uint32_t)nsid_idx < ctrlr->num_ns - 1) {
		return ctrlr->active_ns_list[nsid_idx + 1];
	}
	return 0;
}

struct spdk_nvme_ns *
spdk_nvme_ctrlr_get_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid)
{
	if (nsid < 1 || nsid > ctrlr->num_ns) {
		return NULL;
	}

	return &ctrlr->ns[nsid - 1];
}

struct spdk_pci_device *
spdk_nvme_ctrlr_get_pci_device(struct spdk_nvme_ctrlr *ctrlr)
{
	if (ctrlr == NULL) {
		return NULL;
	}

	if (ctrlr->trid.trtype != SPDK_NVME_TRANSPORT_PCIE) {
		return NULL;
	}

	return nvme_ctrlr_proc_get_devhandle(ctrlr);
}

uint32_t
spdk_nvme_ctrlr_get_max_xfer_size(const struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->max_xfer_size;
}

void
spdk_nvme_ctrlr_register_aer_callback(struct spdk_nvme_ctrlr *ctrlr,
				      spdk_nvme_aer_cb aer_cb_fn,
				      void *aer_cb_arg)
{
	struct spdk_nvme_ctrlr_process *active_proc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	active_proc = nvme_ctrlr_get_current_process(ctrlr);
	if (active_proc) {
		active_proc->aer_cb_fn = aer_cb_fn;
		active_proc->aer_cb_arg = aer_cb_arg;
	}

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

void
spdk_nvme_ctrlr_register_timeout_callback(struct spdk_nvme_ctrlr *ctrlr,
		uint64_t timeout_us, spdk_nvme_timeout_cb cb_fn, void *cb_arg)
{
	struct spdk_nvme_ctrlr_process	*active_proc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	active_proc = nvme_ctrlr_get_current_process(ctrlr);
	if (active_proc) {
		active_proc->timeout_ticks = timeout_us * spdk_get_ticks_hz() / 1000000ULL;
		active_proc->timeout_cb_fn = cb_fn;
		active_proc->timeout_cb_arg = cb_arg;
	}

	ctrlr->timeout_enabled = true;

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

bool
spdk_nvme_ctrlr_is_log_page_supported(struct spdk_nvme_ctrlr *ctrlr, uint8_t log_page)
{
	/* No bounds check necessary, since log_page is uint8_t and log_page_supported has 256 entries */
	SPDK_STATIC_ASSERT(sizeof(ctrlr->log_page_supported) == 256, "log_page_supported size mismatch");
	return ctrlr->log_page_supported[log_page];
}

bool
spdk_nvme_ctrlr_is_feature_supported(struct spdk_nvme_ctrlr *ctrlr, uint8_t feature_code)
{
	/* No bounds check necessary, since feature_code is uint8_t and feature_supported has 256 entries */
	SPDK_STATIC_ASSERT(sizeof(ctrlr->feature_supported) == 256, "feature_supported size mismatch");
	return ctrlr->feature_supported[feature_code];
}

int
spdk_nvme_ctrlr_attach_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
			  struct spdk_nvme_ctrlr_list *payload)
{
	struct nvme_completion_poll_status	*status;
	int					res;
	struct spdk_nvme_ns			*ns;

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	res = nvme_ctrlr_cmd_attach_ns(ctrlr, nsid, payload,
				       nvme_completion_poll_cb, status);
	if (res) {
		free(status);
		return res;
	}
	if (nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock)) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_attach_ns failed!\n");
		if (!status->timed_out) {
			free(status);
		}
		return -ENXIO;
	}
	free(status);

	res = nvme_ctrlr_identify_active_ns(ctrlr);
	if (res) {
		return res;
	}

	ns = &ctrlr->ns[nsid - 1];
	return nvme_ns_construct(ns, nsid, ctrlr);
}

int
spdk_nvme_ctrlr_detach_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
			  struct spdk_nvme_ctrlr_list *payload)
{
	struct nvme_completion_poll_status	*status;
	int					res;
	struct spdk_nvme_ns			*ns;

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	res = nvme_ctrlr_cmd_detach_ns(ctrlr, nsid, payload,
				       nvme_completion_poll_cb, status);
	if (res) {
		free(status);
		return res;
	}
	if (nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock)) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_detach_ns failed!\n");
		if (!status->timed_out) {
			free(status);
		}
		return -ENXIO;
	}
	free(status);

	res = nvme_ctrlr_identify_active_ns(ctrlr);
	if (res) {
		return res;
	}

	ns = &ctrlr->ns[nsid - 1];
	/* Inactive NS */
	nvme_ns_destruct(ns);

	return 0;
}

uint32_t
spdk_nvme_ctrlr_create_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns_data *payload)
{
	struct nvme_completion_poll_status	*status;
	int					res;
	uint32_t				nsid;
	struct spdk_nvme_ns			*ns;

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return 0;
	}

	res = nvme_ctrlr_cmd_create_ns(ctrlr, payload, nvme_completion_poll_cb, status);
	if (res) {
		free(status);
		return 0;
	}
	if (nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock)) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_create_ns failed!\n");
		if (!status->timed_out) {
			free(status);
		}
		return 0;
	}

	nsid = status->cpl.cdw0;
	ns = &ctrlr->ns[nsid - 1];
	free(status);
	/* Inactive NS */
	res = nvme_ns_construct(ns, nsid, ctrlr);
	if (res) {
		return 0;
	}

	/* Return the namespace ID that was created */
	return nsid;
}

int
spdk_nvme_ctrlr_delete_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid)
{
	struct nvme_completion_poll_status	*status;
	int					res;
	struct spdk_nvme_ns			*ns;

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	res = nvme_ctrlr_cmd_delete_ns(ctrlr, nsid, nvme_completion_poll_cb, status);
	if (res) {
		free(status);
		return res;
	}
	if (nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock)) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_delete_ns failed!\n");
		if (!status->timed_out) {
			free(status);
		}
		return -ENXIO;
	}
	free(status);

	res = nvme_ctrlr_identify_active_ns(ctrlr);
	if (res) {
		return res;
	}

	ns = &ctrlr->ns[nsid - 1];
	nvme_ns_destruct(ns);

	return 0;
}

int
spdk_nvme_ctrlr_format(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
		       struct spdk_nvme_format *format)
{
	struct nvme_completion_poll_status	*status;
	int					res;

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	res = nvme_ctrlr_cmd_format(ctrlr, nsid, format, nvme_completion_poll_cb,
				    status);
	if (res) {
		free(status);
		return res;
	}
	if (nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock)) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_format failed!\n");
		if (!status->timed_out) {
			free(status);
		}
		return -ENXIO;
	}
	free(status);

	return spdk_nvme_ctrlr_reset(ctrlr);
}

int
spdk_nvme_ctrlr_update_firmware(struct spdk_nvme_ctrlr *ctrlr, void *payload, uint32_t size,
				int slot, enum spdk_nvme_fw_commit_action commit_action, struct spdk_nvme_status *completion_status)
{
	struct spdk_nvme_fw_commit		fw_commit;
	struct nvme_completion_poll_status	*status;
	int					res;
	unsigned int				size_remaining;
	unsigned int				offset;
	unsigned int				transfer;
	void					*p;

	if (!completion_status) {
		return -EINVAL;
	}
	memset(completion_status, 0, sizeof(struct spdk_nvme_status));
	if (size % 4) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_update_firmware invalid size!\n");
		return -1;
	}

	/* Current support only for SPDK_NVME_FW_COMMIT_REPLACE_IMG
	 * and SPDK_NVME_FW_COMMIT_REPLACE_AND_ENABLE_IMG
	 */
	if ((commit_action != SPDK_NVME_FW_COMMIT_REPLACE_IMG) &&
	    (commit_action != SPDK_NVME_FW_COMMIT_REPLACE_AND_ENABLE_IMG)) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_update_firmware invalid command!\n");
		return -1;
	}

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	/* Firmware download */
	size_remaining = size;
	offset = 0;
	p = payload;

	while (size_remaining > 0) {
		transfer = spdk_min(size_remaining, ctrlr->min_page_size);

		memset(status, 0, sizeof(*status));
		res = nvme_ctrlr_cmd_fw_image_download(ctrlr, transfer, offset, p,
						       nvme_completion_poll_cb,
						       status);
		if (res) {
			free(status);
			return res;
		}

		if (nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock)) {
			NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_fw_image_download failed!\n");
			if (!status->timed_out) {
				free(status);
			}
			return -ENXIO;
		}
		p += transfer;
		offset += transfer;
		size_remaining -= transfer;
	}

	/* Firmware commit */
	memset(&fw_commit, 0, sizeof(struct spdk_nvme_fw_commit));
	fw_commit.fs = slot;
	fw_commit.ca = commit_action;

	memset(status, 0, sizeof(*status));
	res = nvme_ctrlr_cmd_fw_commit(ctrlr, &fw_commit, nvme_completion_poll_cb,
				       status);
	if (res) {
		free(status);
		return res;
	}

	res = nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock);

	memcpy(completion_status, &status->cpl.status, sizeof(struct spdk_nvme_status));

	if (!status->timed_out) {
		free(status);
	}

	if (res) {
		if (completion_status->sct != SPDK_NVME_SCT_COMMAND_SPECIFIC ||
		    completion_status->sc != SPDK_NVME_SC_FIRMWARE_REQ_NVM_RESET) {
			if (completion_status->sct == SPDK_NVME_SCT_COMMAND_SPECIFIC  &&
			    completion_status->sc == SPDK_NVME_SC_FIRMWARE_REQ_CONVENTIONAL_RESET) {
				NVME_CTRLR_NOTICELOG(ctrlr,
						     "firmware activation requires conventional reset to be performed. !\n");
			} else {
				NVME_CTRLR_ERRLOG(ctrlr, "nvme_ctrlr_cmd_fw_commit failed!\n");
			}
			return -ENXIO;
		}
	}

	return spdk_nvme_ctrlr_reset(ctrlr);
}

int
spdk_nvme_ctrlr_reserve_cmb(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc, size;
	union spdk_nvme_cmbsz_register cmbsz;

	cmbsz = spdk_nvme_ctrlr_get_regs_cmbsz(ctrlr);

	if (cmbsz.bits.rds == 0 || cmbsz.bits.wds == 0) {
		return -ENOTSUP;
	}

	size = cmbsz.bits.sz * (0x1000 << (cmbsz.bits.szu * 4));

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	rc = nvme_transport_ctrlr_reserve_cmb(ctrlr);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	if (rc < 0) {
		return rc;
	}

	return size;
}

void *
spdk_nvme_ctrlr_map_cmb(struct spdk_nvme_ctrlr *ctrlr, size_t *size)
{
	void *buf;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	buf = nvme_transport_ctrlr_map_cmb(ctrlr, size);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return buf;
}

void
spdk_nvme_ctrlr_unmap_cmb(struct spdk_nvme_ctrlr *ctrlr)
{
	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	nvme_transport_ctrlr_unmap_cmb(ctrlr);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

int
spdk_nvme_ctrlr_enable_pmr(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	rc = nvme_transport_ctrlr_enable_pmr(ctrlr);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
spdk_nvme_ctrlr_disable_pmr(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	rc = nvme_transport_ctrlr_disable_pmr(ctrlr);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

void *
spdk_nvme_ctrlr_map_pmr(struct spdk_nvme_ctrlr *ctrlr, size_t *size)
{
	void *buf;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	buf = nvme_transport_ctrlr_map_pmr(ctrlr, size);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return buf;
}

int
spdk_nvme_ctrlr_unmap_pmr(struct spdk_nvme_ctrlr *ctrlr)
{
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	rc = nvme_transport_ctrlr_unmap_pmr(ctrlr);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

bool
spdk_nvme_ctrlr_is_discovery(struct spdk_nvme_ctrlr *ctrlr)
{
	assert(ctrlr);

	return !strncmp(ctrlr->trid.subnqn, SPDK_NVMF_DISCOVERY_NQN,
			strlen(SPDK_NVMF_DISCOVERY_NQN));
}

int
spdk_nvme_ctrlr_security_receive(struct spdk_nvme_ctrlr *ctrlr, uint8_t secp,
				 uint16_t spsp, uint8_t nssf, void *payload, size_t size)
{
	struct nvme_completion_poll_status	*status;
	int					res;

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	res = spdk_nvme_ctrlr_cmd_security_receive(ctrlr, secp, spsp, nssf, payload, size,
			nvme_completion_poll_cb, status);
	if (res) {
		free(status);
		return res;
	}
	if (nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock)) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_cmd_security_receive failed!\n");
		if (!status->timed_out) {
			free(status);
		}
		return -ENXIO;
	}
	free(status);

	return 0;
}

int
spdk_nvme_ctrlr_security_send(struct spdk_nvme_ctrlr *ctrlr, uint8_t secp,
			      uint16_t spsp, uint8_t nssf, void *payload, size_t size)
{
	struct nvme_completion_poll_status	*status;
	int					res;

	status = calloc(1, sizeof(*status));
	if (!status) {
		NVME_CTRLR_ERRLOG(ctrlr, "Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	res = spdk_nvme_ctrlr_cmd_security_send(ctrlr, secp, spsp, nssf, payload, size,
						nvme_completion_poll_cb,
						status);
	if (res) {
		free(status);
		return res;
	}
	if (nvme_wait_for_completion_robust_lock(ctrlr->adminq, status, &ctrlr->ctrlr_lock)) {
		NVME_CTRLR_ERRLOG(ctrlr, "spdk_nvme_ctrlr_cmd_security_send failed!\n");
		if (!status->timed_out) {
			free(status);
		}
		return -ENXIO;
	}

	free(status);

	return 0;
}

uint64_t
spdk_nvme_ctrlr_get_flags(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->flags;
}

const struct spdk_nvme_transport_id *
spdk_nvme_ctrlr_get_transport_id(struct spdk_nvme_ctrlr *ctrlr)
{
	return &ctrlr->trid;
}

int32_t
spdk_nvme_ctrlr_alloc_qid(struct spdk_nvme_ctrlr *ctrlr)
{
	uint32_t qid;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	qid = spdk_bit_array_find_first_set(ctrlr->free_io_qids, 1);
	if (qid > ctrlr->opts.num_io_queues) {
		NVME_CTRLR_ERRLOG(ctrlr, "No free I/O queue IDs\n");
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -1;
	}

	spdk_bit_array_clear(ctrlr->free_io_qids, qid);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return qid;
}

void
spdk_nvme_ctrlr_free_qid(struct spdk_nvme_ctrlr *ctrlr, uint16_t qid)
{
	assert(qid <= ctrlr->opts.num_io_queues);

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	spdk_bit_array_set(ctrlr->free_io_qids, qid);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
}

static int
nvme_cmd_map_prps(void *prv, struct spdk_nvme_cmd *cmd, struct iovec *iovs,
		  uint32_t max_iovcnt, uint32_t len, size_t mps,
		  void *(*gpa_to_vva)(void *prv, uint64_t addr, uint64_t len))
{
	uint64_t prp1, prp2;
	void *vva;
	uint32_t i;
	uint32_t residue_len, nents;
	uint64_t *prp_list;
	uint32_t iovcnt;

	assert(max_iovcnt > 0);

	prp1 = cmd->dptr.prp.prp1;
	prp2 = cmd->dptr.prp.prp2;

	/* PRP1 may started with unaligned page address */
	residue_len = mps - (prp1 % mps);
	residue_len = spdk_min(len, residue_len);

	vva = gpa_to_vva(prv, prp1, residue_len);
	if (spdk_unlikely(vva == NULL)) {
		SPDK_ERRLOG("GPA to VVA failed\n");
		return -EINVAL;
	}
	len -= residue_len;
	if (len && max_iovcnt < 2) {
		SPDK_ERRLOG("Too many page entries, at least two iovs are required\n");
		return -ERANGE;
	}
	iovs[0].iov_base = vva;
	iovs[0].iov_len = residue_len;

	if (len) {
		if (spdk_unlikely(prp2 == 0)) {
			SPDK_ERRLOG("no PRP2, %d remaining\n", len);
			return -EINVAL;
		}

		if (len <= mps) {
			/* 2 PRP used */
			iovcnt = 2;
			vva = gpa_to_vva(prv, prp2, len);
			if (spdk_unlikely(vva == NULL)) {
				SPDK_ERRLOG("no VVA for %#" PRIx64 ", len%#x\n",
					    prp2, len);
				return -EINVAL;
			}
			iovs[1].iov_base = vva;
			iovs[1].iov_len = len;
		} else {
			/* PRP list used */
			nents = (len + mps - 1) / mps;
			if (spdk_unlikely(nents + 1 > max_iovcnt)) {
				SPDK_ERRLOG("Too many page entries\n");
				return -ERANGE;
			}

			vva = gpa_to_vva(prv, prp2, nents * sizeof(*prp_list));
			if (spdk_unlikely(vva == NULL)) {
				SPDK_ERRLOG("no VVA for %#" PRIx64 ", nents=%#x\n",
					    prp2, nents);
				return -EINVAL;
			}
			prp_list = vva;
			i = 0;
			while (len != 0) {
				residue_len = spdk_min(len, mps);
				vva = gpa_to_vva(prv, prp_list[i], residue_len);
				if (spdk_unlikely(vva == NULL)) {
					SPDK_ERRLOG("no VVA for %#" PRIx64 ", residue_len=%#x\n",
						    prp_list[i], residue_len);
					return -EINVAL;
				}
				iovs[i + 1].iov_base = vva;
				iovs[i + 1].iov_len = residue_len;
				len -= residue_len;
				i++;
			}
			iovcnt = i + 1;
		}
	} else {
		/* 1 PRP used */
		iovcnt = 1;
	}

	assert(iovcnt <= max_iovcnt);
	return iovcnt;
}

static int
nvme_cmd_map_sgls_data(void *prv, struct spdk_nvme_sgl_descriptor *sgls, uint32_t num_sgls,
		       struct iovec *iovs, uint32_t max_iovcnt,
		       void *(*gpa_to_vva)(void *prv, uint64_t addr, uint64_t len))
{
	uint32_t i;
	void *vva;

	if (spdk_unlikely(max_iovcnt < num_sgls)) {
		return -ERANGE;
	}

	for (i = 0; i < num_sgls; i++) {
		if (spdk_unlikely(sgls[i].unkeyed.type != SPDK_NVME_SGL_TYPE_DATA_BLOCK)) {
			SPDK_ERRLOG("Invalid SGL type %u\n", sgls[i].unkeyed.type);
			return -EINVAL;
		}
		vva = gpa_to_vva(prv, sgls[i].address, sgls[i].unkeyed.length);
		if (spdk_unlikely(vva == NULL)) {
			SPDK_ERRLOG("GPA to VVA failed\n");
			return -EINVAL;
		}
		iovs[i].iov_base = vva;
		iovs[i].iov_len = sgls[i].unkeyed.length;
	}

	return num_sgls;
}

static int
nvme_cmd_map_sgls(void *prv, struct spdk_nvme_cmd *cmd, struct iovec *iovs, uint32_t max_iovcnt,
		  uint32_t len, size_t mps,
		  void *(*gpa_to_vva)(void *prv, uint64_t addr, uint64_t len))
{
	struct spdk_nvme_sgl_descriptor *sgl, *last_sgl;
	uint32_t num_sgls, seg_len;
	void *vva;
	int ret;
	uint32_t total_iovcnt = 0;

	/* SGL cases */
	sgl = &cmd->dptr.sgl1;

	/* only one SGL segment */
	if (sgl->unkeyed.type == SPDK_NVME_SGL_TYPE_DATA_BLOCK) {
		assert(max_iovcnt > 0);
		vva = gpa_to_vva(prv, sgl->address, sgl->unkeyed.length);
		if (spdk_unlikely(vva == NULL)) {
			SPDK_ERRLOG("GPA to VVA failed\n");
			return -EINVAL;
		}
		iovs[0].iov_base = vva;
		iovs[0].iov_len = sgl->unkeyed.length;
		assert(sgl->unkeyed.length == len);

		return 1;
	}

	for (;;) {
		if (spdk_unlikely((sgl->unkeyed.type != SPDK_NVME_SGL_TYPE_SEGMENT) &&
				  (sgl->unkeyed.type != SPDK_NVME_SGL_TYPE_LAST_SEGMENT))) {
			SPDK_ERRLOG("Invalid SGL type %u\n", sgl->unkeyed.type);
			return -EINVAL;
		}

		seg_len = sgl->unkeyed.length;
		if (spdk_unlikely(seg_len % sizeof(struct spdk_nvme_sgl_descriptor))) {
			SPDK_ERRLOG("Invalid SGL segment len %u\n", seg_len);
			return -EINVAL;
		}

		num_sgls = seg_len / sizeof(struct spdk_nvme_sgl_descriptor);
		vva = gpa_to_vva(prv, sgl->address, sgl->unkeyed.length);
		if (spdk_unlikely(vva == NULL)) {
			SPDK_ERRLOG("GPA to VVA failed\n");
			return -EINVAL;
		}

		/* sgl point to the first segment */
		sgl = (struct spdk_nvme_sgl_descriptor *)vva;
		last_sgl = &sgl[num_sgls - 1];

		/* we are done */
		if (last_sgl->unkeyed.type == SPDK_NVME_SGL_TYPE_DATA_BLOCK) {
			/* map whole sgl list */
			ret = nvme_cmd_map_sgls_data(prv, sgl, num_sgls, &iovs[total_iovcnt],
						     max_iovcnt - total_iovcnt, gpa_to_vva);
			if (spdk_unlikely(ret < 0)) {
				return ret;
			}
			total_iovcnt += ret;

			return total_iovcnt;
		}

		if (num_sgls > 1) {
			/* map whole sgl exclude last_sgl */
			ret = nvme_cmd_map_sgls_data(prv, sgl, num_sgls - 1, &iovs[total_iovcnt],
						     max_iovcnt - total_iovcnt, gpa_to_vva);
			if (spdk_unlikely(ret < 0)) {
				return ret;
			}
			total_iovcnt += ret;
		}

		/* move to next level's segments */
		sgl = last_sgl;
	}

	return 0;
}

/* FIXME need to specify max number of iovs */
int
spdk_nvme_map_prps(void *prv, struct spdk_nvme_cmd *cmd, struct iovec *iovs,
		   uint32_t len, size_t mps,
		   void *(*gpa_to_vva)(void *prv, uint64_t addr, uint64_t len))
{
	if (cmd->psdt == SPDK_NVME_PSDT_PRP) {
		return nvme_cmd_map_prps(prv, cmd, iovs, UINT32_MAX, len, mps, gpa_to_vva);
	}

	return -EINVAL;
}

int
spdk_nvme_map_cmd(void *prv, struct spdk_nvme_cmd *cmd, struct iovec *iovs, uint32_t max_iovcnt,
		  uint32_t len, size_t mps,
		  void *(*gpa_to_vva)(void *prv, uint64_t addr, uint64_t len))
{
	if (cmd->psdt == SPDK_NVME_PSDT_PRP) {
		return nvme_cmd_map_prps(prv, cmd, iovs, max_iovcnt, len, mps, gpa_to_vva);
	}

	return nvme_cmd_map_sgls(prv, cmd, iovs, max_iovcnt, len, mps, gpa_to_vva);
}
