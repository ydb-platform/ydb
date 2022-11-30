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

/** \file
 * SPDK cuse
 */


#ifndef SPDK_NVME_IO_MSG_H_
#define SPDK_NVME_IO_MSG_H_

typedef void (*spdk_nvme_io_msg_fn)(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
				    void *arg);

struct spdk_nvme_io_msg {
	struct spdk_nvme_ctrlr	*ctrlr;
	uint32_t		nsid;

	spdk_nvme_io_msg_fn	fn;
	void			*arg;
};

struct nvme_io_msg_producer {
	const char *name;
	void (*update)(struct spdk_nvme_ctrlr *ctrlr);
	void (*stop)(struct spdk_nvme_ctrlr *ctrlr);
	STAILQ_ENTRY(nvme_io_msg_producer) link;
};

int nvme_io_msg_send(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid, spdk_nvme_io_msg_fn fn,
		     void *arg);

/**
 * Process IO message sent to controller from external module.
 *
 * This call process requests from the ring, send IO to an allocated qpair or
 * admin commands in its context. This call is non-blocking and intended to be
 * polled by SPDK thread to provide safe environment for NVMe request
 * completition sent by external module to controller.
 *
 * The caller must ensure that each controller is polled by only one thread at
 * a time.
 *
 * This function may be called at any point while the controller is attached to
 * the SPDK NVMe driver.
 *
 * \param ctrlr Opaque handle to NVMe controller.
 *
 * \return number of processed external IO messages.
 */
int nvme_io_msg_process(struct spdk_nvme_ctrlr *ctrlr);

int nvme_io_msg_ctrlr_register(struct spdk_nvme_ctrlr *ctrlr,
			       struct nvme_io_msg_producer *io_msg_producer);
void nvme_io_msg_ctrlr_unregister(struct spdk_nvme_ctrlr *ctrlr,
				  struct nvme_io_msg_producer *io_msg_producer);
void nvme_io_msg_ctrlr_detach(struct spdk_nvme_ctrlr *ctrlr);
void nvme_io_msg_ctrlr_update(struct spdk_nvme_ctrlr *ctrlr);

#endif /* SPDK_NVME_IO_MSG_H_ */
