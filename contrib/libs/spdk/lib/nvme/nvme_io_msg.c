#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
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

#include "nvme_internal.h"
#include "nvme_io_msg.h"

#define SPDK_NVME_MSG_IO_PROCESS_SIZE 8

/**
 * Send message to IO queue.
 */
int
nvme_io_msg_send(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid, spdk_nvme_io_msg_fn fn,
		 void *arg)
{
	int rc;
	struct spdk_nvme_io_msg *io;

	/* Protect requests ring against preemptive producers */
	pthread_mutex_lock(&ctrlr->external_io_msgs_lock);

	io = (struct spdk_nvme_io_msg *)calloc(1, sizeof(struct spdk_nvme_io_msg));
	if (!io) {
		SPDK_ERRLOG("IO msg allocation failed.");
		pthread_mutex_unlock(&ctrlr->external_io_msgs_lock);
		return -ENOMEM;
	}

	io->ctrlr = ctrlr;
	io->nsid = nsid;
	io->fn = fn;
	io->arg = arg;

	rc = spdk_ring_enqueue(ctrlr->external_io_msgs, (void **)&io, 1, NULL);
	if (rc != 1) {
		assert(false);
		free(io);
		pthread_mutex_unlock(&ctrlr->external_io_msgs_lock);
		return -ENOMEM;
	}

	pthread_mutex_unlock(&ctrlr->external_io_msgs_lock);

	return 0;
}

int
nvme_io_msg_process(struct spdk_nvme_ctrlr *ctrlr)
{
	int i;
	int count;
	struct spdk_nvme_io_msg *io;
	void *requests[SPDK_NVME_MSG_IO_PROCESS_SIZE];

	if (!ctrlr->external_io_msgs || !ctrlr->external_io_msgs_qpair) {
		/* Not ready or pending reset */
		return 0;
	}

	if (!spdk_process_is_primary()) {
		return 0;
	}

	spdk_nvme_qpair_process_completions(ctrlr->external_io_msgs_qpair, 0);

	count = spdk_ring_dequeue(ctrlr->external_io_msgs, requests,
				  SPDK_NVME_MSG_IO_PROCESS_SIZE);
	if (count == 0) {
		return 0;
	}

	for (i = 0; i < count; i++) {
		io = requests[i];

		assert(io != NULL);

		io->fn(io->ctrlr, io->nsid, io->arg);
		free(io);
	}

	return count;
}

static bool
nvme_io_msg_is_producer_registered(struct spdk_nvme_ctrlr *ctrlr,
				   struct nvme_io_msg_producer *io_msg_producer)
{
	struct nvme_io_msg_producer *tmp;

	STAILQ_FOREACH(tmp, &ctrlr->io_producers, link) {
		if (tmp == io_msg_producer) {
			return true;
		}
	}
	return false;
}

int
nvme_io_msg_ctrlr_register(struct spdk_nvme_ctrlr *ctrlr,
			   struct nvme_io_msg_producer *io_msg_producer)
{
	if (io_msg_producer == NULL) {
		SPDK_ERRLOG("io_msg_producer cannot be NULL\n");
		return -EINVAL;
	}

	if (nvme_io_msg_is_producer_registered(ctrlr, io_msg_producer)) {
		return -EEXIST;
	}

	if (!STAILQ_EMPTY(&ctrlr->io_producers) || ctrlr->is_resetting) {
		/* There are registered producers - IO messaging already started */
		STAILQ_INSERT_TAIL(&ctrlr->io_producers, io_msg_producer, link);
		return 0;
	}

	pthread_mutex_init(&ctrlr->external_io_msgs_lock, NULL);

	/**
	 * Initialize ring and qpair for controller
	 */
	ctrlr->external_io_msgs = spdk_ring_create(SPDK_RING_TYPE_MP_SC, 65536, SPDK_ENV_SOCKET_ID_ANY);
	if (!ctrlr->external_io_msgs) {
		SPDK_ERRLOG("Unable to allocate memory for message ring\n");
		return -ENOMEM;
	}

	ctrlr->external_io_msgs_qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, NULL, 0);
	if (ctrlr->external_io_msgs_qpair == NULL) {
		SPDK_ERRLOG("spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
		spdk_ring_free(ctrlr->external_io_msgs);
		ctrlr->external_io_msgs = NULL;
		return -ENOMEM;
	}

	STAILQ_INSERT_TAIL(&ctrlr->io_producers, io_msg_producer, link);

	return 0;
}

void
nvme_io_msg_ctrlr_update(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_io_msg_producer *io_msg_producer;

	/* Update all producers */
	STAILQ_FOREACH(io_msg_producer, &ctrlr->io_producers, link) {
		io_msg_producer->update(ctrlr);
	}
}

void
nvme_io_msg_ctrlr_detach(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_io_msg_producer *io_msg_producer, *tmp;

	/* Stop all producers */
	STAILQ_FOREACH_SAFE(io_msg_producer, &ctrlr->io_producers, link, tmp) {
		io_msg_producer->stop(ctrlr);
		STAILQ_REMOVE(&ctrlr->io_producers, io_msg_producer, nvme_io_msg_producer, link);
	}

	if (ctrlr->external_io_msgs) {
		spdk_ring_free(ctrlr->external_io_msgs);
		ctrlr->external_io_msgs = NULL;
	}

	if (ctrlr->external_io_msgs_qpair) {
		spdk_nvme_ctrlr_free_io_qpair(ctrlr->external_io_msgs_qpair);
		ctrlr->external_io_msgs_qpair = NULL;
	}

	pthread_mutex_destroy(&ctrlr->external_io_msgs_lock);
}

void
nvme_io_msg_ctrlr_unregister(struct spdk_nvme_ctrlr *ctrlr,
			     struct nvme_io_msg_producer *io_msg_producer)
{
	assert(io_msg_producer != NULL);

	if (!nvme_io_msg_is_producer_registered(ctrlr, io_msg_producer)) {
		return;
	}

	STAILQ_REMOVE(&ctrlr->io_producers, io_msg_producer, nvme_io_msg_producer, link);
	if (STAILQ_EMPTY(&ctrlr->io_producers)) {
		nvme_io_msg_ctrlr_detach(ctrlr);
	}
}
