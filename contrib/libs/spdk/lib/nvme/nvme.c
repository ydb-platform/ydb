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

#include "spdk/nvmf_spec.h"
#include "spdk/string.h"
#include "spdk/env.h"
#include "nvme_internal.h"
#include "nvme_io_msg.h"

#define SPDK_NVME_DRIVER_NAME "spdk_nvme_driver"

struct nvme_driver	*g_spdk_nvme_driver;
pid_t			g_spdk_nvme_pid;

/* gross timeout of 180 seconds in milliseconds */
static int g_nvme_driver_timeout_ms = 3 * 60 * 1000;

/* Per-process attached controller list */
static TAILQ_HEAD(, spdk_nvme_ctrlr) g_nvme_attached_ctrlrs =
	TAILQ_HEAD_INITIALIZER(g_nvme_attached_ctrlrs);

/* Returns true if ctrlr should be stored on the multi-process shared_attached_ctrlrs list */
static bool
nvme_ctrlr_shared(const struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_PCIE;
}

void
nvme_ctrlr_connected(struct spdk_nvme_probe_ctx *probe_ctx,
		     struct spdk_nvme_ctrlr *ctrlr)
{
	TAILQ_INSERT_TAIL(&probe_ctx->init_ctrlrs, ctrlr, tailq);
}

static void
nvme_ctrlr_detach_async_finish(struct spdk_nvme_ctrlr *ctrlr)
{
	nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);
	if (nvme_ctrlr_shared(ctrlr)) {
		TAILQ_REMOVE(&g_spdk_nvme_driver->shared_attached_ctrlrs, ctrlr, tailq);
	} else {
		TAILQ_REMOVE(&g_nvme_attached_ctrlrs, ctrlr, tailq);
	}
	nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);
}

static int
nvme_ctrlr_detach_async(struct spdk_nvme_ctrlr *ctrlr,
			struct nvme_ctrlr_detach_ctx **_ctx)
{
	struct nvme_ctrlr_detach_ctx *ctx;
	int ref_count;

	nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);

	ref_count = nvme_ctrlr_get_ref_count(ctrlr);
	assert(ref_count > 0);

	if (ref_count == 1) {
		/* This is the last reference to the controller, so we need to
		 * allocate a context to destruct it.
		 */
		ctx = calloc(1, sizeof(*ctx));
		if (ctx == NULL) {
			nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);

			return -ENOMEM;
		}
		ctx->ctrlr = ctrlr;
		ctx->cb_fn = nvme_ctrlr_detach_async_finish;

		nvme_ctrlr_proc_put_ref(ctrlr);

		nvme_io_msg_ctrlr_detach(ctrlr);

		nvme_ctrlr_destruct_async(ctrlr, ctx);

		*_ctx = ctx;
	} else {
		nvme_ctrlr_proc_put_ref(ctrlr);
	}

	nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);

	return 0;
}

static int
nvme_ctrlr_detach_poll_async(struct nvme_ctrlr_detach_ctx *ctx)
{
	int rc;

	rc = nvme_ctrlr_destruct_poll_async(ctx->ctrlr, ctx);
	if (rc == -EAGAIN) {
		return -EAGAIN;
	}

	free(ctx);

	return rc;
}

int
spdk_nvme_detach(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_ctrlr_detach_ctx *ctx = NULL;
	int rc;

	rc = nvme_ctrlr_detach_async(ctrlr, &ctx);
	if (rc != 0) {
		return rc;
	} else if (ctx == NULL) {
		/* ctrlr was detached from the caller process but any other process
		 * still attaches it.
		 */
		return 0;
	}

	while (1) {
		rc = nvme_ctrlr_detach_poll_async(ctx);
		if (rc != -EAGAIN) {
			break;
		}
		nvme_delay(1000);
	}

	return 0;
}

int
spdk_nvme_detach_async(struct spdk_nvme_ctrlr *ctrlr,
		       struct spdk_nvme_detach_ctx **_detach_ctx)
{
	struct spdk_nvme_detach_ctx *detach_ctx;
	struct nvme_ctrlr_detach_ctx *ctx = NULL;
	int rc;

	if (ctrlr == NULL || _detach_ctx == NULL) {
		return -EINVAL;
	}

	/* Use a context header to poll detachement for multiple controllers.
	 * Allocate an new one if not allocated yet, or use the passed one otherwise.
	 */
	detach_ctx = *_detach_ctx;
	if (detach_ctx == NULL) {
		detach_ctx = calloc(1, sizeof(*detach_ctx));
		if (detach_ctx == NULL) {
			return -ENOMEM;
		}
		TAILQ_INIT(&detach_ctx->head);
	} else if (detach_ctx->polling_started) {
		SPDK_ERRLOG("Busy at polling detachment now.\n");
		return -EBUSY;
	}

	rc = nvme_ctrlr_detach_async(ctrlr, &ctx);
	if (rc != 0 || ctx == NULL) {
		/* If this detach failed and the context header is empty, it means we just
		 * allocated the header and need to free it before returning.
		 */
		if (TAILQ_EMPTY(&detach_ctx->head)) {
			free(detach_ctx);
		}
		return rc;
	}

	/* Append a context for this detachment to the context header. */
	TAILQ_INSERT_TAIL(&detach_ctx->head, ctx, link);

	*_detach_ctx = detach_ctx;

	return 0;
}

int
spdk_nvme_detach_poll_async(struct spdk_nvme_detach_ctx *detach_ctx)
{
	struct nvme_ctrlr_detach_ctx *ctx, *tmp_ctx;
	int rc;

	if (detach_ctx == NULL) {
		return -EINVAL;
	}

	detach_ctx->polling_started = true;

	TAILQ_FOREACH_SAFE(ctx, &detach_ctx->head, link, tmp_ctx) {
		TAILQ_REMOVE(&detach_ctx->head, ctx, link);

		rc = nvme_ctrlr_detach_poll_async(ctx);
		if (rc == -EAGAIN) {
			/* If not -EAGAIN, ctx was freed by nvme_ctrlr_detach_poll_async(). */
			TAILQ_INSERT_HEAD(&detach_ctx->head, ctx, link);
		}
	}

	if (!TAILQ_EMPTY(&detach_ctx->head)) {
		return -EAGAIN;
	}

	free(detach_ctx);
	return 0;
}

void
nvme_completion_poll_cb(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct nvme_completion_poll_status	*status = arg;

	if (status->timed_out) {
		/* There is no routine waiting for the completion of this request, free allocated memory */
		free(status);
		return;
	}

	/*
	 * Copy status into the argument passed by the caller, so that
	 *  the caller can check the status to determine if the
	 *  the request passed or failed.
	 */
	memcpy(&status->cpl, cpl, sizeof(*cpl));
	status->done = true;
}

static void
dummy_disconnected_qpair_cb(struct spdk_nvme_qpair *qpair, void *poll_group_ctx)
{
}

/**
 * Poll qpair for completions until a command completes.
 *
 * \param qpair queue to poll
 * \param status completion status. The user must fill this structure with zeroes before calling
 * this function
 * \param robust_mutex optional robust mutex to lock while polling qpair
 * \param timeout_in_usecs optional timeout
 *
 * \return 0 if command completed without error,
 * -EIO if command completed with error,
 * -ECANCELED if command is not completed due to transport/device error or time expired
 *
 *  The command to wait upon must be submitted with nvme_completion_poll_cb as the callback
 *  and status as the callback argument.
 */
int
nvme_wait_for_completion_robust_lock_timeout(
	struct spdk_nvme_qpair *qpair,
	struct nvme_completion_poll_status *status,
	pthread_mutex_t *robust_mutex,
	uint64_t timeout_in_usecs)
{
	uint64_t timeout_tsc = 0;
	int rc = 0;

	if (timeout_in_usecs) {
		timeout_tsc = spdk_get_ticks() + timeout_in_usecs * spdk_get_ticks_hz() / SPDK_SEC_TO_USEC;
	}

	while (status->done == false) {
		if (robust_mutex) {
			nvme_robust_mutex_lock(robust_mutex);
		}

		if (qpair->poll_group) {
			rc = (int)spdk_nvme_poll_group_process_completions(qpair->poll_group->group, 0,
					dummy_disconnected_qpair_cb);
		} else {
			rc = spdk_nvme_qpair_process_completions(qpair, 0);
		}

		if (robust_mutex) {
			nvme_robust_mutex_unlock(robust_mutex);
		}

		if (rc < 0) {
			status->cpl.status.sct = SPDK_NVME_SCT_GENERIC;
			status->cpl.status.sc = SPDK_NVME_SC_ABORTED_SQ_DELETION;
			break;
		}
		if (timeout_tsc && spdk_get_ticks() > timeout_tsc) {
			rc = -1;
			break;
		}
		if (qpair->ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
			union spdk_nvme_csts_register csts = spdk_nvme_ctrlr_get_regs_csts(qpair->ctrlr);
			if (csts.raw == SPDK_NVME_INVALID_REGISTER_VALUE) {
				status->cpl.status.sct = SPDK_NVME_SCT_GENERIC;
				status->cpl.status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
				break;
			}
		}
	}

	if (status->done == false) {
		status->timed_out = true;
	}

	if (rc < 0) {
		return -ECANCELED;
	}

	return spdk_nvme_cpl_is_error(&status->cpl) ? -EIO : 0;
}

/**
 * Poll qpair for completions until a command completes.
 *
 * \param qpair queue to poll
 * \param status completion status. The user must fill this structure with zeroes before calling
 * this function
 * \param robust_mutex optional robust mutex to lock while polling qpair
 *
 * \return 0 if command completed without error,
 * -EIO if command completed with error,
 * -ECANCELED if command is not completed due to transport/device error
 *
 * The command to wait upon must be submitted with nvme_completion_poll_cb as the callback
 * and status as the callback argument.
 */
int
nvme_wait_for_completion_robust_lock(
	struct spdk_nvme_qpair *qpair,
	struct nvme_completion_poll_status *status,
	pthread_mutex_t *robust_mutex)
{
	return nvme_wait_for_completion_robust_lock_timeout(qpair, status, robust_mutex, 0);
}

int
nvme_wait_for_completion(struct spdk_nvme_qpair *qpair,
			 struct nvme_completion_poll_status *status)
{
	return nvme_wait_for_completion_robust_lock_timeout(qpair, status, NULL, 0);
}

/**
 * Poll qpair for completions until a command completes.
 *
 * \param qpair queue to poll
 * \param status completion status. The user must fill this structure with zeroes before calling
 * this function
 * \param timeout_in_usecs optional timeout
 *
 * \return 0 if command completed without error,
 * -EIO if command completed with error,
 * -ECANCELED if command is not completed due to transport/device error or time expired
 *
 * The command to wait upon must be submitted with nvme_completion_poll_cb as the callback
 * and status as the callback argument.
 */
int
nvme_wait_for_completion_timeout(struct spdk_nvme_qpair *qpair,
				 struct nvme_completion_poll_status *status,
				 uint64_t timeout_in_usecs)
{
	return nvme_wait_for_completion_robust_lock_timeout(qpair, status, NULL, timeout_in_usecs);
}

static void
nvme_user_copy_cmd_complete(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct nvme_request *req = arg;
	enum spdk_nvme_data_transfer xfer;

	if (req->user_buffer && req->payload_size) {
		/* Copy back to the user buffer and free the contig buffer */
		assert(nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_CONTIG);
		xfer = spdk_nvme_opc_get_data_transfer(req->cmd.opc);
		if (xfer == SPDK_NVME_DATA_CONTROLLER_TO_HOST ||
		    xfer == SPDK_NVME_DATA_BIDIRECTIONAL) {
			assert(req->pid == getpid());
			memcpy(req->user_buffer, req->payload.contig_or_cb_arg, req->payload_size);
		}

		spdk_free(req->payload.contig_or_cb_arg);
	}

	/* Call the user's original callback now that the buffer has been copied */
	req->user_cb_fn(req->user_cb_arg, cpl);
}

/**
 * Allocate a request as well as a DMA-capable buffer to copy to/from the user's buffer.
 *
 * This is intended for use in non-fast-path functions (admin commands, reservations, etc.)
 * where the overhead of a copy is not a problem.
 */
struct nvme_request *
nvme_allocate_request_user_copy(struct spdk_nvme_qpair *qpair,
				void *buffer, uint32_t payload_size, spdk_nvme_cmd_cb cb_fn,
				void *cb_arg, bool host_to_controller)
{
	struct nvme_request *req;
	void *dma_buffer = NULL;

	if (buffer && payload_size) {
		dma_buffer = spdk_zmalloc(payload_size, 4096, NULL,
					  SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
		if (!dma_buffer) {
			return NULL;
		}

		if (host_to_controller) {
			memcpy(dma_buffer, buffer, payload_size);
		}
	}

	req = nvme_allocate_request_contig(qpair, dma_buffer, payload_size, nvme_user_copy_cmd_complete,
					   NULL);
	if (!req) {
		spdk_free(dma_buffer);
		return NULL;
	}

	req->user_cb_fn = cb_fn;
	req->user_cb_arg = cb_arg;
	req->user_buffer = buffer;
	req->cb_arg = req;

	return req;
}

/**
 * Check if a request has exceeded the controller timeout.
 *
 * \param req request to check for timeout.
 * \param cid command ID for command submitted by req (will be passed to timeout_cb_fn)
 * \param active_proc per-process data for the controller associated with req
 * \param now_tick current time from spdk_get_ticks()
 * \return 0 if requests submitted more recently than req should still be checked for timeouts, or
 * 1 if requests newer than req need not be checked.
 *
 * The request's timeout callback will be called if needed; the caller is only responsible for
 * calling this function on each outstanding request.
 */
int
nvme_request_check_timeout(struct nvme_request *req, uint16_t cid,
			   struct spdk_nvme_ctrlr_process *active_proc,
			   uint64_t now_tick)
{
	struct spdk_nvme_qpair *qpair = req->qpair;
	struct spdk_nvme_ctrlr *ctrlr = qpair->ctrlr;

	assert(active_proc->timeout_cb_fn != NULL);

	if (req->timed_out || req->submit_tick == 0) {
		return 0;
	}

	if (req->pid != g_spdk_nvme_pid) {
		return 0;
	}

	if (nvme_qpair_is_admin_queue(qpair) &&
	    req->cmd.opc == SPDK_NVME_OPC_ASYNC_EVENT_REQUEST) {
		return 0;
	}

	if (req->submit_tick + active_proc->timeout_ticks > now_tick) {
		return 1;
	}

	req->timed_out = true;

	/*
	 * We don't want to expose the admin queue to the user,
	 * so when we're timing out admin commands set the
	 * qpair to NULL.
	 */
	active_proc->timeout_cb_fn(active_proc->timeout_cb_arg, ctrlr,
				   nvme_qpair_is_admin_queue(qpair) ? NULL : qpair,
				   cid);
	return 0;
}

int
nvme_robust_mutex_init_shared(pthread_mutex_t *mtx)
{
	int rc = 0;

#ifdef __FreeBSD__
	pthread_mutex_init(mtx, NULL);
#else
	pthread_mutexattr_t attr;

	if (pthread_mutexattr_init(&attr)) {
		return -1;
	}
	if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) ||
	    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST) ||
	    pthread_mutex_init(mtx, &attr)) {
		rc = -1;
	}
	pthread_mutexattr_destroy(&attr);
#endif

	return rc;
}

int
nvme_driver_init(void)
{
	static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;
	int ret = 0;
	/* Any socket ID */
	int socket_id = -1;

	/* Use a special process-private mutex to ensure the global
	 * nvme driver object (g_spdk_nvme_driver) gets initialized by
	 * only one thread.  Once that object is established and its
	 * mutex is initialized, we can unlock this mutex and use that
	 * one instead.
	 */
	pthread_mutex_lock(&g_init_mutex);

	/* Each process needs its own pid. */
	g_spdk_nvme_pid = getpid();

	/*
	 * Only one thread from one process will do this driver init work.
	 * The primary process will reserve the shared memory and do the
	 *  initialization.
	 * The secondary process will lookup the existing reserved memory.
	 */
	if (spdk_process_is_primary()) {
		/* The unique named memzone already reserved. */
		if (g_spdk_nvme_driver != NULL) {
			pthread_mutex_unlock(&g_init_mutex);
			return 0;
		} else {
			g_spdk_nvme_driver = spdk_memzone_reserve(SPDK_NVME_DRIVER_NAME,
					     sizeof(struct nvme_driver), socket_id,
					     SPDK_MEMZONE_NO_IOVA_CONTIG);
		}

		if (g_spdk_nvme_driver == NULL) {
			SPDK_ERRLOG("primary process failed to reserve memory\n");
			pthread_mutex_unlock(&g_init_mutex);
			return -1;
		}
	} else {
		g_spdk_nvme_driver = spdk_memzone_lookup(SPDK_NVME_DRIVER_NAME);

		/* The unique named memzone already reserved by the primary process. */
		if (g_spdk_nvme_driver != NULL) {
			int ms_waited = 0;

			/* Wait the nvme driver to get initialized. */
			while ((g_spdk_nvme_driver->initialized == false) &&
			       (ms_waited < g_nvme_driver_timeout_ms)) {
				ms_waited++;
				nvme_delay(1000); /* delay 1ms */
			}
			if (g_spdk_nvme_driver->initialized == false) {
				SPDK_ERRLOG("timeout waiting for primary process to init\n");
				pthread_mutex_unlock(&g_init_mutex);
				return -1;
			}
		} else {
			SPDK_ERRLOG("primary process is not started yet\n");
			pthread_mutex_unlock(&g_init_mutex);
			return -1;
		}

		pthread_mutex_unlock(&g_init_mutex);
		return 0;
	}

	/*
	 * At this moment, only one thread from the primary process will do
	 * the g_spdk_nvme_driver initialization
	 */
	assert(spdk_process_is_primary());

	ret = nvme_robust_mutex_init_shared(&g_spdk_nvme_driver->lock);
	if (ret != 0) {
		SPDK_ERRLOG("failed to initialize mutex\n");
		spdk_memzone_free(SPDK_NVME_DRIVER_NAME);
		pthread_mutex_unlock(&g_init_mutex);
		return ret;
	}

	/* The lock in the shared g_spdk_nvme_driver object is now ready to
	 * be used - so we can unlock the g_init_mutex here.
	 */
	pthread_mutex_unlock(&g_init_mutex);
	nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);

	g_spdk_nvme_driver->initialized = false;
	g_spdk_nvme_driver->hotplug_fd = spdk_pci_event_listen();
	if (g_spdk_nvme_driver->hotplug_fd < 0) {
		SPDK_DEBUGLOG(nvme, "Failed to open uevent netlink socket\n");
	}

	TAILQ_INIT(&g_spdk_nvme_driver->shared_attached_ctrlrs);

	spdk_uuid_generate(&g_spdk_nvme_driver->default_extended_host_id);

	nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);

	return ret;
}

/* This function must only be called while holding g_spdk_nvme_driver->lock */
int
nvme_ctrlr_probe(const struct spdk_nvme_transport_id *trid,
		 struct spdk_nvme_probe_ctx *probe_ctx, void *devhandle)
{
	struct spdk_nvme_ctrlr *ctrlr;
	struct spdk_nvme_ctrlr_opts opts;

	assert(trid != NULL);

	spdk_nvme_ctrlr_get_default_ctrlr_opts(&opts, sizeof(opts));

	if (!probe_ctx->probe_cb || probe_ctx->probe_cb(probe_ctx->cb_ctx, trid, &opts)) {
		ctrlr = nvme_get_ctrlr_by_trid_unsafe(trid);
		if (ctrlr) {
			/* This ctrlr already exists. */

			if (ctrlr->is_destructed) {
				/* This ctrlr is being destructed asynchronously. */
				SPDK_ERRLOG("NVMe controller for SSD: %s is being destructed\n",
					    trid->traddr);
				return -EBUSY;
			}

			/* Increase the ref count before calling attach_cb() as the user may
			* call nvme_detach() immediately. */
			nvme_ctrlr_proc_get_ref(ctrlr);

			if (probe_ctx->attach_cb) {
				nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);
				probe_ctx->attach_cb(probe_ctx->cb_ctx, &ctrlr->trid, ctrlr, &ctrlr->opts);
				nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);
			}
			return 0;
		}

		ctrlr = nvme_transport_ctrlr_construct(trid, &opts, devhandle);
		if (ctrlr == NULL) {
			SPDK_ERRLOG("Failed to construct NVMe controller for SSD: %s\n", trid->traddr);
			return -1;
		}
		ctrlr->remove_cb = probe_ctx->remove_cb;
		ctrlr->cb_ctx = probe_ctx->cb_ctx;

		if (ctrlr->quirks & NVME_QUIRK_MINIMUM_IO_QUEUE_SIZE &&
		    ctrlr->opts.io_queue_size == DEFAULT_IO_QUEUE_SIZE) {
			/* If the user specifically set an IO queue size different than the
			 * default, use that value.  Otherwise overwrite with the quirked value.
			 * This allows this quirk to be overridden when necessary.
			 * However, cap.mqes still needs to be respected.
			 */
			ctrlr->opts.io_queue_size = spdk_min(DEFAULT_IO_QUEUE_SIZE_FOR_QUIRK, ctrlr->cap.bits.mqes + 1u);
		}

		nvme_qpair_set_state(ctrlr->adminq, NVME_QPAIR_ENABLED);
		TAILQ_INSERT_TAIL(&probe_ctx->init_ctrlrs, ctrlr, tailq);
		return 0;
	}

	return 1;
}

static void
nvme_ctrlr_poll_internal(struct spdk_nvme_ctrlr *ctrlr,
			 struct spdk_nvme_probe_ctx *probe_ctx)
{
	int rc = 0;

	rc = nvme_ctrlr_process_init(ctrlr);

	if (rc) {
		/* Controller failed to initialize. */
		TAILQ_REMOVE(&probe_ctx->init_ctrlrs, ctrlr, tailq);
		SPDK_ERRLOG("Failed to initialize SSD: %s\n", ctrlr->trid.traddr);
		nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
		nvme_ctrlr_fail(ctrlr, false);
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		nvme_ctrlr_destruct(ctrlr);
		return;
	}

	if (ctrlr->state != NVME_CTRLR_STATE_READY) {
		return;
	}

	STAILQ_INIT(&ctrlr->io_producers);

	/*
	 * Controller has been initialized.
	 *  Move it to the attached_ctrlrs list.
	 */
	TAILQ_REMOVE(&probe_ctx->init_ctrlrs, ctrlr, tailq);

	nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);
	if (nvme_ctrlr_shared(ctrlr)) {
		TAILQ_INSERT_TAIL(&g_spdk_nvme_driver->shared_attached_ctrlrs, ctrlr, tailq);
	} else {
		TAILQ_INSERT_TAIL(&g_nvme_attached_ctrlrs, ctrlr, tailq);
	}

	/*
	 * Increase the ref count before calling attach_cb() as the user may
	 * call nvme_detach() immediately.
	 */
	nvme_ctrlr_proc_get_ref(ctrlr);
	nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);

	if (probe_ctx->attach_cb) {
		probe_ctx->attach_cb(probe_ctx->cb_ctx, &ctrlr->trid, ctrlr, &ctrlr->opts);
	}
}

static int
nvme_init_controllers(struct spdk_nvme_probe_ctx *probe_ctx)
{
	int rc = 0;

	while (true) {
		rc = spdk_nvme_probe_poll_async(probe_ctx);
		if (rc != -EAGAIN) {
			return rc;
		}
	}

	return rc;
}

/* This function must not be called while holding g_spdk_nvme_driver->lock */
static struct spdk_nvme_ctrlr *
nvme_get_ctrlr_by_trid(const struct spdk_nvme_transport_id *trid)
{
	struct spdk_nvme_ctrlr *ctrlr;

	nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);
	ctrlr = nvme_get_ctrlr_by_trid_unsafe(trid);
	nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);

	return ctrlr;
}

/* This function must be called while holding g_spdk_nvme_driver->lock */
struct spdk_nvme_ctrlr *
nvme_get_ctrlr_by_trid_unsafe(const struct spdk_nvme_transport_id *trid)
{
	struct spdk_nvme_ctrlr *ctrlr;

	/* Search per-process list */
	TAILQ_FOREACH(ctrlr, &g_nvme_attached_ctrlrs, tailq) {
		if (spdk_nvme_transport_id_compare(&ctrlr->trid, trid) == 0) {
			return ctrlr;
		}
	}

	/* Search multi-process shared list */
	TAILQ_FOREACH(ctrlr, &g_spdk_nvme_driver->shared_attached_ctrlrs, tailq) {
		if (spdk_nvme_transport_id_compare(&ctrlr->trid, trid) == 0) {
			return ctrlr;
		}
	}

	return NULL;
}

/* This function must only be called while holding g_spdk_nvme_driver->lock */
static int
nvme_probe_internal(struct spdk_nvme_probe_ctx *probe_ctx,
		    bool direct_connect)
{
	int rc;
	struct spdk_nvme_ctrlr *ctrlr, *ctrlr_tmp;

	spdk_nvme_trid_populate_transport(&probe_ctx->trid, probe_ctx->trid.trtype);
	if (!spdk_nvme_transport_available_by_name(probe_ctx->trid.trstring)) {
		SPDK_ERRLOG("NVMe trtype %u not available\n", probe_ctx->trid.trtype);
		return -1;
	}

	nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);

	rc = nvme_transport_ctrlr_scan(probe_ctx, direct_connect);
	if (rc != 0) {
		SPDK_ERRLOG("NVMe ctrlr scan failed\n");
		TAILQ_FOREACH_SAFE(ctrlr, &probe_ctx->init_ctrlrs, tailq, ctrlr_tmp) {
			TAILQ_REMOVE(&probe_ctx->init_ctrlrs, ctrlr, tailq);
			nvme_transport_ctrlr_destruct(ctrlr);
		}
		nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);
		return -1;
	}

	/*
	 * Probe controllers on the shared_attached_ctrlrs list
	 */
	if (!spdk_process_is_primary() && (probe_ctx->trid.trtype == SPDK_NVME_TRANSPORT_PCIE)) {
		TAILQ_FOREACH(ctrlr, &g_spdk_nvme_driver->shared_attached_ctrlrs, tailq) {
			/* Do not attach other ctrlrs if user specify a valid trid */
			if ((strlen(probe_ctx->trid.traddr) != 0) &&
			    (spdk_nvme_transport_id_compare(&probe_ctx->trid, &ctrlr->trid))) {
				continue;
			}

			/* Do not attach if we failed to initialize it in this process */
			if (nvme_ctrlr_get_current_process(ctrlr) == NULL) {
				continue;
			}

			nvme_ctrlr_proc_get_ref(ctrlr);

			/*
			 * Unlock while calling attach_cb() so the user can call other functions
			 *  that may take the driver lock, like nvme_detach().
			 */
			if (probe_ctx->attach_cb) {
				nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);
				probe_ctx->attach_cb(probe_ctx->cb_ctx, &ctrlr->trid, ctrlr, &ctrlr->opts);
				nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);
			}
		}
	}

	nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);

	return 0;
}

static void
nvme_probe_ctx_init(struct spdk_nvme_probe_ctx *probe_ctx,
		    const struct spdk_nvme_transport_id *trid,
		    void *cb_ctx,
		    spdk_nvme_probe_cb probe_cb,
		    spdk_nvme_attach_cb attach_cb,
		    spdk_nvme_remove_cb remove_cb)
{
	probe_ctx->trid = *trid;
	probe_ctx->cb_ctx = cb_ctx;
	probe_ctx->probe_cb = probe_cb;
	probe_ctx->attach_cb = attach_cb;
	probe_ctx->remove_cb = remove_cb;
	TAILQ_INIT(&probe_ctx->init_ctrlrs);
}

int
spdk_nvme_probe(const struct spdk_nvme_transport_id *trid, void *cb_ctx,
		spdk_nvme_probe_cb probe_cb, spdk_nvme_attach_cb attach_cb,
		spdk_nvme_remove_cb remove_cb)
{
	struct spdk_nvme_transport_id trid_pcie;
	struct spdk_nvme_probe_ctx *probe_ctx;

	if (trid == NULL) {
		memset(&trid_pcie, 0, sizeof(trid_pcie));
		spdk_nvme_trid_populate_transport(&trid_pcie, SPDK_NVME_TRANSPORT_PCIE);
		trid = &trid_pcie;
	}

	probe_ctx = spdk_nvme_probe_async(trid, cb_ctx, probe_cb,
					  attach_cb, remove_cb);
	if (!probe_ctx) {
		SPDK_ERRLOG("Create probe context failed\n");
		return -1;
	}

	/*
	 * Keep going even if one or more nvme_attach() calls failed,
	 *  but maintain the value of rc to signal errors when we return.
	 */
	return nvme_init_controllers(probe_ctx);
}

static bool
nvme_connect_probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
		      struct spdk_nvme_ctrlr_opts *opts)
{
	struct spdk_nvme_ctrlr_opts *requested_opts = cb_ctx;

	assert(requested_opts);
	memcpy(opts, requested_opts, sizeof(*opts));

	return true;
}

static void
nvme_ctrlr_opts_init(struct spdk_nvme_ctrlr_opts *opts,
		     const struct spdk_nvme_ctrlr_opts *opts_user,
		     size_t opts_size_user)
{
	assert(opts);
	assert(opts_user);

	spdk_nvme_ctrlr_get_default_ctrlr_opts(opts, opts_size_user);

#define FIELD_OK(field) \
	offsetof(struct spdk_nvme_ctrlr_opts, field) + sizeof(opts->field) <= (opts->opts_size)

#define SET_FIELD(field) \
	if (FIELD_OK(field)) { \
			opts->field = opts_user->field; \
	}

#define SET_FIELD_ARRAY(field) \
	if (FIELD_OK(field)) { \
		memcpy(opts->field, opts_user->field, sizeof(opts_user->field)); \
	}

	SET_FIELD(num_io_queues);
	SET_FIELD(use_cmb_sqs);
	SET_FIELD(no_shn_notification);
	SET_FIELD(arb_mechanism);
	SET_FIELD(arbitration_burst);
	SET_FIELD(low_priority_weight);
	SET_FIELD(medium_priority_weight);
	SET_FIELD(high_priority_weight);
	SET_FIELD(keep_alive_timeout_ms);
	SET_FIELD(transport_retry_count);
	SET_FIELD(io_queue_size);
	SET_FIELD_ARRAY(hostnqn);
	SET_FIELD(io_queue_requests);
	SET_FIELD_ARRAY(src_addr);
	SET_FIELD_ARRAY(src_svcid);
	SET_FIELD_ARRAY(host_id);
	SET_FIELD_ARRAY(extended_host_id);
	SET_FIELD(command_set);
	SET_FIELD(admin_timeout_ms);
	SET_FIELD(header_digest);
	SET_FIELD(data_digest);
	SET_FIELD(disable_error_logging);
	SET_FIELD(transport_ack_timeout);
	SET_FIELD(admin_queue_size);
	SET_FIELD(fabrics_connect_timeout_us);

	/* Do not remove this statement. When you add a new field, please do update this
	 * assert with the correct size. And do not forget to add a new SET_FIELD statement
	 * related with your new added field. */
	SPDK_STATIC_ASSERT(sizeof(struct spdk_nvme_ctrlr_opts) == 608, "Incorrect size");

#undef FIELD_OK
#undef SET_FIELD
#undef SET_FIELD_ARRAY
}

struct spdk_nvme_ctrlr *
spdk_nvme_connect(const struct spdk_nvme_transport_id *trid,
		  const struct spdk_nvme_ctrlr_opts *opts, size_t opts_size)
{
	int rc;
	struct spdk_nvme_ctrlr *ctrlr = NULL;
	struct spdk_nvme_probe_ctx *probe_ctx;
	struct spdk_nvme_ctrlr_opts *opts_local_p = NULL;
	struct spdk_nvme_ctrlr_opts opts_local;

	if (trid == NULL) {
		SPDK_ERRLOG("No transport ID specified\n");
		return NULL;
	}

	if (opts) {
		opts_local_p = &opts_local;
		nvme_ctrlr_opts_init(opts_local_p, opts, opts_size);
	}

	probe_ctx = spdk_nvme_connect_async(trid, opts_local_p, NULL);
	if (!probe_ctx) {
		SPDK_ERRLOG("Create probe context failed\n");
		return NULL;
	}

	rc = nvme_init_controllers(probe_ctx);
	if (rc != 0) {
		return NULL;
	}

	ctrlr = nvme_get_ctrlr_by_trid(trid);

	return ctrlr;
}

void
spdk_nvme_trid_populate_transport(struct spdk_nvme_transport_id *trid,
				  enum spdk_nvme_transport_type trtype)
{
	const char *trstring = "";

	trid->trtype = trtype;
	switch (trtype) {
	case SPDK_NVME_TRANSPORT_FC:
		trstring = SPDK_NVME_TRANSPORT_NAME_FC;
		break;
	case SPDK_NVME_TRANSPORT_PCIE:
		trstring = SPDK_NVME_TRANSPORT_NAME_PCIE;
		break;
	case SPDK_NVME_TRANSPORT_RDMA:
		trstring = SPDK_NVME_TRANSPORT_NAME_RDMA;
		break;
	case SPDK_NVME_TRANSPORT_TCP:
		trstring = SPDK_NVME_TRANSPORT_NAME_TCP;
		break;
	case SPDK_NVME_TRANSPORT_VFIOUSER:
		trstring = SPDK_NVME_TRANSPORT_NAME_VFIOUSER;
		break;
	case SPDK_NVME_TRANSPORT_CUSTOM:
		trstring = SPDK_NVME_TRANSPORT_NAME_CUSTOM;
		break;
	default:
		SPDK_ERRLOG("no available transports\n");
		assert(0);
		return;
	}
	snprintf(trid->trstring, SPDK_NVMF_TRSTRING_MAX_LEN, "%s", trstring);
}

int
spdk_nvme_transport_id_populate_trstring(struct spdk_nvme_transport_id *trid, const char *trstring)
{
	int len, i, rc;

	if (trstring == NULL) {
		return -EINVAL;
	}

	len = strnlen(trstring, SPDK_NVMF_TRSTRING_MAX_LEN);
	if (len == SPDK_NVMF_TRSTRING_MAX_LEN) {
		return -EINVAL;
	}

	rc = snprintf(trid->trstring, SPDK_NVMF_TRSTRING_MAX_LEN, "%s", trstring);
	if (rc < 0) {
		return rc;
	}

	/* cast official trstring to uppercase version of input. */
	for (i = 0; i < len; i++) {
		trid->trstring[i] = toupper(trid->trstring[i]);
	}
	return 0;
}

int
spdk_nvme_transport_id_parse_trtype(enum spdk_nvme_transport_type *trtype, const char *str)
{
	if (trtype == NULL || str == NULL) {
		return -EINVAL;
	}

	if (strcasecmp(str, "PCIe") == 0) {
		*trtype = SPDK_NVME_TRANSPORT_PCIE;
	} else if (strcasecmp(str, "RDMA") == 0) {
		*trtype = SPDK_NVME_TRANSPORT_RDMA;
	} else if (strcasecmp(str, "FC") == 0) {
		*trtype = SPDK_NVME_TRANSPORT_FC;
	} else if (strcasecmp(str, "TCP") == 0) {
		*trtype = SPDK_NVME_TRANSPORT_TCP;
	} else if (strcasecmp(str, "VFIOUSER") == 0) {
		*trtype = SPDK_NVME_TRANSPORT_VFIOUSER;
	} else {
		*trtype = SPDK_NVME_TRANSPORT_CUSTOM;
	}
	return 0;
}

const char *
spdk_nvme_transport_id_trtype_str(enum spdk_nvme_transport_type trtype)
{
	switch (trtype) {
	case SPDK_NVME_TRANSPORT_PCIE:
		return "PCIe";
	case SPDK_NVME_TRANSPORT_RDMA:
		return "RDMA";
	case SPDK_NVME_TRANSPORT_FC:
		return "FC";
	case SPDK_NVME_TRANSPORT_TCP:
		return "TCP";
	case SPDK_NVME_TRANSPORT_VFIOUSER:
		return "VFIOUSER";
	case SPDK_NVME_TRANSPORT_CUSTOM:
		return "CUSTOM";
	default:
		return NULL;
	}
}

int
spdk_nvme_transport_id_parse_adrfam(enum spdk_nvmf_adrfam *adrfam, const char *str)
{
	if (adrfam == NULL || str == NULL) {
		return -EINVAL;
	}

	if (strcasecmp(str, "IPv4") == 0) {
		*adrfam = SPDK_NVMF_ADRFAM_IPV4;
	} else if (strcasecmp(str, "IPv6") == 0) {
		*adrfam = SPDK_NVMF_ADRFAM_IPV6;
	} else if (strcasecmp(str, "IB") == 0) {
		*adrfam = SPDK_NVMF_ADRFAM_IB;
	} else if (strcasecmp(str, "FC") == 0) {
		*adrfam = SPDK_NVMF_ADRFAM_FC;
	} else {
		return -ENOENT;
	}
	return 0;
}

const char *
spdk_nvme_transport_id_adrfam_str(enum spdk_nvmf_adrfam adrfam)
{
	switch (adrfam) {
	case SPDK_NVMF_ADRFAM_IPV4:
		return "IPv4";
	case SPDK_NVMF_ADRFAM_IPV6:
		return "IPv6";
	case SPDK_NVMF_ADRFAM_IB:
		return "IB";
	case SPDK_NVMF_ADRFAM_FC:
		return "FC";
	default:
		return NULL;
	}
}

static size_t
parse_next_key(const char **str, char *key, char *val, size_t key_buf_size, size_t val_buf_size)
{

	const char *sep, *sep1;
	const char *whitespace = " \t\n";
	size_t key_len, val_len;

	*str += strspn(*str, whitespace);

	sep = strchr(*str, ':');
	if (!sep) {
		sep = strchr(*str, '=');
		if (!sep) {
			SPDK_ERRLOG("Key without ':' or '=' separator\n");
			return 0;
		}
	} else {
		sep1 = strchr(*str, '=');
		if ((sep1 != NULL) && (sep1 < sep)) {
			sep = sep1;
		}
	}

	key_len = sep - *str;
	if (key_len >= key_buf_size) {
		SPDK_ERRLOG("Key length %zu greater than maximum allowed %zu\n",
			    key_len, key_buf_size - 1);
		return 0;
	}

	memcpy(key, *str, key_len);
	key[key_len] = '\0';

	*str += key_len + 1; /* Skip key: */
	val_len = strcspn(*str, whitespace);
	if (val_len == 0) {
		SPDK_ERRLOG("Key without value\n");
		return 0;
	}

	if (val_len >= val_buf_size) {
		SPDK_ERRLOG("Value length %zu greater than maximum allowed %zu\n",
			    val_len, val_buf_size - 1);
		return 0;
	}

	memcpy(val, *str, val_len);
	val[val_len] = '\0';

	*str += val_len;

	return val_len;
}

int
spdk_nvme_transport_id_parse(struct spdk_nvme_transport_id *trid, const char *str)
{
	size_t val_len;
	char key[32];
	char val[1024];

	if (trid == NULL || str == NULL) {
		return -EINVAL;
	}

	while (*str != '\0') {

		val_len = parse_next_key(&str, key, val, sizeof(key), sizeof(val));

		if (val_len == 0) {
			SPDK_ERRLOG("Failed to parse transport ID\n");
			return -EINVAL;
		}

		if (strcasecmp(key, "trtype") == 0) {
			if (spdk_nvme_transport_id_populate_trstring(trid, val) != 0) {
				SPDK_ERRLOG("invalid transport '%s'\n", val);
				return -EINVAL;
			}
			if (spdk_nvme_transport_id_parse_trtype(&trid->trtype, val) != 0) {
				SPDK_ERRLOG("Unknown trtype '%s'\n", val);
				return -EINVAL;
			}
		} else if (strcasecmp(key, "adrfam") == 0) {
			if (spdk_nvme_transport_id_parse_adrfam(&trid->adrfam, val) != 0) {
				SPDK_ERRLOG("Unknown adrfam '%s'\n", val);
				return -EINVAL;
			}
		} else if (strcasecmp(key, "traddr") == 0) {
			if (val_len > SPDK_NVMF_TRADDR_MAX_LEN) {
				SPDK_ERRLOG("traddr length %zu greater than maximum allowed %u\n",
					    val_len, SPDK_NVMF_TRADDR_MAX_LEN);
				return -EINVAL;
			}
			memcpy(trid->traddr, val, val_len + 1);
		} else if (strcasecmp(key, "trsvcid") == 0) {
			if (val_len > SPDK_NVMF_TRSVCID_MAX_LEN) {
				SPDK_ERRLOG("trsvcid length %zu greater than maximum allowed %u\n",
					    val_len, SPDK_NVMF_TRSVCID_MAX_LEN);
				return -EINVAL;
			}
			memcpy(trid->trsvcid, val, val_len + 1);
		} else if (strcasecmp(key, "priority") == 0) {
			if (val_len > SPDK_NVMF_PRIORITY_MAX_LEN) {
				SPDK_ERRLOG("priority length %zu greater than maximum allowed %u\n",
					    val_len, SPDK_NVMF_PRIORITY_MAX_LEN);
				return -EINVAL;
			}
			trid->priority = spdk_strtol(val, 10);
		} else if (strcasecmp(key, "subnqn") == 0) {
			if (val_len > SPDK_NVMF_NQN_MAX_LEN) {
				SPDK_ERRLOG("subnqn length %zu greater than maximum allowed %u\n",
					    val_len, SPDK_NVMF_NQN_MAX_LEN);
				return -EINVAL;
			}
			memcpy(trid->subnqn, val, val_len + 1);
		} else if (strcasecmp(key, "hostaddr") == 0) {
			continue;
		} else if (strcasecmp(key, "hostsvcid") == 0) {
			continue;
		} else if (strcasecmp(key, "hostnqn") == 0) {
			continue;
		} else if (strcasecmp(key, "ns") == 0) {
			/*
			 * Special case.  The namespace id parameter may
			 * optionally be passed in the transport id string
			 * for an SPDK application (e.g. nvme/perf)
			 * and additionally parsed therein to limit
			 * targeting a specific namespace.  For this
			 * scenario, just silently ignore this key
			 * rather than letting it default to logging
			 * it as an invalid key.
			 */
			continue;
		} else if (strcasecmp(key, "alt_traddr") == 0) {
			/*
			 * Used by applications for enabling transport ID failover.
			 * Please see the case above for more information on custom parameters.
			 */
			continue;
		} else {
			SPDK_ERRLOG("Unknown transport ID key '%s'\n", key);
		}
	}

	return 0;
}

int
spdk_nvme_host_id_parse(struct spdk_nvme_host_id *hostid, const char *str)
{

	size_t key_size = 32;
	size_t val_size = 1024;
	size_t val_len;
	char key[key_size];
	char val[val_size];

	if (hostid == NULL || str == NULL) {
		return -EINVAL;
	}

	while (*str != '\0') {

		val_len = parse_next_key(&str, key, val, key_size, val_size);

		if (val_len == 0) {
			SPDK_ERRLOG("Failed to parse host ID\n");
			return val_len;
		}

		/* Ignore the rest of the options from the transport ID. */
		if (strcasecmp(key, "trtype") == 0) {
			continue;
		} else if (strcasecmp(key, "adrfam") == 0) {
			continue;
		} else if (strcasecmp(key, "traddr") == 0) {
			continue;
		} else if (strcasecmp(key, "trsvcid") == 0) {
			continue;
		} else if (strcasecmp(key, "subnqn") == 0) {
			continue;
		} else if (strcasecmp(key, "priority") == 0) {
			continue;
		} else if (strcasecmp(key, "ns") == 0) {
			continue;
		} else if (strcasecmp(key, "hostaddr") == 0) {
			if (val_len > SPDK_NVMF_TRADDR_MAX_LEN) {
				SPDK_ERRLOG("hostaddr length %zu greater than maximum allowed %u\n",
					    val_len, SPDK_NVMF_TRADDR_MAX_LEN);
				return -EINVAL;
			}
			memcpy(hostid->hostaddr, val, val_len + 1);

		} else if (strcasecmp(key, "hostsvcid") == 0) {
			if (val_len > SPDK_NVMF_TRSVCID_MAX_LEN) {
				SPDK_ERRLOG("trsvcid length %zu greater than maximum allowed %u\n",
					    val_len, SPDK_NVMF_TRSVCID_MAX_LEN);
				return -EINVAL;
			}
			memcpy(hostid->hostsvcid, val, val_len + 1);
		} else {
			SPDK_ERRLOG("Unknown transport ID key '%s'\n", key);
		}
	}

	return 0;
}

static int
cmp_int(int a, int b)
{
	return a - b;
}

int
spdk_nvme_transport_id_compare(const struct spdk_nvme_transport_id *trid1,
			       const struct spdk_nvme_transport_id *trid2)
{
	int cmp;

	if (trid1->trtype == SPDK_NVME_TRANSPORT_CUSTOM) {
		cmp = strcasecmp(trid1->trstring, trid2->trstring);
	} else {
		cmp = cmp_int(trid1->trtype, trid2->trtype);
	}

	if (cmp) {
		return cmp;
	}

	if (trid1->trtype == SPDK_NVME_TRANSPORT_PCIE) {
		struct spdk_pci_addr pci_addr1 = {};
		struct spdk_pci_addr pci_addr2 = {};

		/* Normalize PCI addresses before comparing */
		if (spdk_pci_addr_parse(&pci_addr1, trid1->traddr) < 0 ||
		    spdk_pci_addr_parse(&pci_addr2, trid2->traddr) < 0) {
			return -1;
		}

		/* PCIe transport ID only uses trtype and traddr */
		return spdk_pci_addr_compare(&pci_addr1, &pci_addr2);
	}

	cmp = strcasecmp(trid1->traddr, trid2->traddr);
	if (cmp) {
		return cmp;
	}

	cmp = cmp_int(trid1->adrfam, trid2->adrfam);
	if (cmp) {
		return cmp;
	}

	cmp = strcasecmp(trid1->trsvcid, trid2->trsvcid);
	if (cmp) {
		return cmp;
	}

	cmp = strcmp(trid1->subnqn, trid2->subnqn);
	if (cmp) {
		return cmp;
	}

	return 0;
}

int
spdk_nvme_prchk_flags_parse(uint32_t *prchk_flags, const char *str)
{
	size_t val_len;
	char key[32];
	char val[1024];

	if (prchk_flags == NULL || str == NULL) {
		return -EINVAL;
	}

	while (*str != '\0') {
		val_len = parse_next_key(&str, key, val, sizeof(key), sizeof(val));

		if (val_len == 0) {
			SPDK_ERRLOG("Failed to parse prchk\n");
			return -EINVAL;
		}

		if (strcasecmp(key, "prchk") == 0) {
			if (strcasestr(val, "reftag") != NULL) {
				*prchk_flags |= SPDK_NVME_IO_FLAGS_PRCHK_REFTAG;
			}
			if (strcasestr(val, "guard") != NULL) {
				*prchk_flags |= SPDK_NVME_IO_FLAGS_PRCHK_GUARD;
			}
		} else {
			SPDK_ERRLOG("Unknown key '%s'\n", key);
			return -EINVAL;
		}
	}

	return 0;
}

const char *
spdk_nvme_prchk_flags_str(uint32_t prchk_flags)
{
	if (prchk_flags & SPDK_NVME_IO_FLAGS_PRCHK_REFTAG) {
		if (prchk_flags & SPDK_NVME_IO_FLAGS_PRCHK_GUARD) {
			return "prchk:reftag|guard";
		} else {
			return "prchk:reftag";
		}
	} else {
		if (prchk_flags & SPDK_NVME_IO_FLAGS_PRCHK_GUARD) {
			return "prchk:guard";
		} else {
			return NULL;
		}
	}
}

struct spdk_nvme_probe_ctx *
spdk_nvme_probe_async(const struct spdk_nvme_transport_id *trid,
		      void *cb_ctx,
		      spdk_nvme_probe_cb probe_cb,
		      spdk_nvme_attach_cb attach_cb,
		      spdk_nvme_remove_cb remove_cb)
{
	int rc;
	struct spdk_nvme_probe_ctx *probe_ctx;

	rc = nvme_driver_init();
	if (rc != 0) {
		return NULL;
	}

	probe_ctx = calloc(1, sizeof(*probe_ctx));
	if (!probe_ctx) {
		return NULL;
	}

	nvme_probe_ctx_init(probe_ctx, trid, cb_ctx, probe_cb, attach_cb, remove_cb);
	rc = nvme_probe_internal(probe_ctx, false);
	if (rc != 0) {
		free(probe_ctx);
		return NULL;
	}

	return probe_ctx;
}

int
spdk_nvme_probe_poll_async(struct spdk_nvme_probe_ctx *probe_ctx)
{
	struct spdk_nvme_ctrlr *ctrlr, *ctrlr_tmp;

	if (!spdk_process_is_primary() && probe_ctx->trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
		free(probe_ctx);
		return 0;
	}

	TAILQ_FOREACH_SAFE(ctrlr, &probe_ctx->init_ctrlrs, tailq, ctrlr_tmp) {
		nvme_ctrlr_poll_internal(ctrlr, probe_ctx);
	}

	if (TAILQ_EMPTY(&probe_ctx->init_ctrlrs)) {
		nvme_robust_mutex_lock(&g_spdk_nvme_driver->lock);
		g_spdk_nvme_driver->initialized = true;
		nvme_robust_mutex_unlock(&g_spdk_nvme_driver->lock);
		free(probe_ctx);
		return 0;
	}

	return -EAGAIN;
}

struct spdk_nvme_probe_ctx *
spdk_nvme_connect_async(const struct spdk_nvme_transport_id *trid,
			const struct spdk_nvme_ctrlr_opts *opts,
			spdk_nvme_attach_cb attach_cb)
{
	int rc;
	spdk_nvme_probe_cb probe_cb = NULL;
	struct spdk_nvme_probe_ctx *probe_ctx;

	rc = nvme_driver_init();
	if (rc != 0) {
		return NULL;
	}

	probe_ctx = calloc(1, sizeof(*probe_ctx));
	if (!probe_ctx) {
		return NULL;
	}

	if (opts) {
		probe_cb = nvme_connect_probe_cb;
	}

	nvme_probe_ctx_init(probe_ctx, trid, (void *)opts, probe_cb, attach_cb, NULL);
	rc = nvme_probe_internal(probe_ctx, true);
	if (rc != 0) {
		free(probe_ctx);
		return NULL;
	}

	return probe_ctx;
}

SPDK_LOG_REGISTER_COMPONENT(nvme)
