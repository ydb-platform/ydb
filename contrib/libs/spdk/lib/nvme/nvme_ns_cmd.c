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

#include "nvme_internal.h"

static inline struct nvme_request *_nvme_ns_cmd_rw(struct spdk_nvme_ns *ns,
		struct spdk_nvme_qpair *qpair,
		const struct nvme_payload *payload, uint32_t payload_offset, uint32_t md_offset,
		uint64_t lba, uint32_t lba_count, spdk_nvme_cmd_cb cb_fn,
		void *cb_arg, uint32_t opc, uint32_t io_flags,
		uint16_t apptag_mask, uint16_t apptag, bool check_sgl, int *rc);

static bool
nvme_ns_check_request_length(uint32_t lba_count, uint32_t sectors_per_max_io,
			     uint32_t sectors_per_stripe, uint32_t qdepth)
{
	uint32_t child_per_io = UINT32_MAX;

	/* After a namespace is destroyed(e.g. hotplug), all the fields associated with the
	 * namespace will be cleared to zero, the function will return TRUE for this case,
	 * and -EINVAL will be returned to caller.
	 */
	if (sectors_per_stripe > 0) {
		child_per_io = (lba_count + sectors_per_stripe - 1) / sectors_per_stripe;
	} else if (sectors_per_max_io > 0) {
		child_per_io = (lba_count + sectors_per_max_io - 1) / sectors_per_max_io;
	}

	SPDK_DEBUGLOG(nvme, "checking maximum i/o length %d\n", child_per_io);

	return child_per_io >= qdepth;
}

static inline int
nvme_ns_map_failure_rc(uint32_t lba_count, uint32_t sectors_per_max_io,
		       uint32_t sectors_per_stripe, uint32_t qdepth, int rc)
{
	assert(rc);
	if (rc == -ENOMEM &&
	    nvme_ns_check_request_length(lba_count, sectors_per_max_io, sectors_per_stripe, qdepth)) {
		return -EINVAL;
	}
	return rc;
}

static inline bool
_nvme_md_excluded_from_xfer(struct spdk_nvme_ns *ns, uint32_t io_flags)
{
	return (io_flags & SPDK_NVME_IO_FLAGS_PRACT) &&
	       (ns->flags & SPDK_NVME_NS_EXTENDED_LBA_SUPPORTED) &&
	       (ns->flags & SPDK_NVME_NS_DPS_PI_SUPPORTED) &&
	       (ns->md_size == 8);
}

static inline uint32_t
_nvme_get_host_buffer_sector_size(struct spdk_nvme_ns *ns, uint32_t io_flags)
{
	return _nvme_md_excluded_from_xfer(ns, io_flags) ?
	       ns->sector_size : ns->extended_lba_size;
}

static inline uint32_t
_nvme_get_sectors_per_max_io(struct spdk_nvme_ns *ns, uint32_t io_flags)
{
	return _nvme_md_excluded_from_xfer(ns, io_flags) ?
	       ns->sectors_per_max_io_no_md : ns->sectors_per_max_io;
}

static struct nvme_request *
_nvme_add_child_request(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			const struct nvme_payload *payload,
			uint32_t payload_offset, uint32_t md_offset,
			uint64_t lba, uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t opc,
			uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag,
			struct nvme_request *parent, bool check_sgl, int *rc)
{
	struct nvme_request	*child;

	child = _nvme_ns_cmd_rw(ns, qpair, payload, payload_offset, md_offset, lba, lba_count, cb_fn,
				cb_arg, opc, io_flags, apptag_mask, apptag, check_sgl, rc);
	if (child == NULL) {
		nvme_request_free_children(parent);
		nvme_free_request(parent);
		return NULL;
	}

	nvme_request_add_child(parent, child);
	return child;
}

static struct nvme_request *
_nvme_ns_cmd_split_request(struct spdk_nvme_ns *ns,
			   struct spdk_nvme_qpair *qpair,
			   const struct nvme_payload *payload,
			   uint32_t payload_offset, uint32_t md_offset,
			   uint64_t lba, uint32_t lba_count,
			   spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t opc,
			   uint32_t io_flags, struct nvme_request *req,
			   uint32_t sectors_per_max_io, uint32_t sector_mask,
			   uint16_t apptag_mask, uint16_t apptag, int *rc)
{
	uint32_t		sector_size = _nvme_get_host_buffer_sector_size(ns, io_flags);
	uint32_t		remaining_lba_count = lba_count;
	struct nvme_request	*child;

	while (remaining_lba_count > 0) {
		lba_count = sectors_per_max_io - (lba & sector_mask);
		lba_count = spdk_min(remaining_lba_count, lba_count);

		child = _nvme_add_child_request(ns, qpair, payload, payload_offset, md_offset,
						lba, lba_count, cb_fn, cb_arg, opc,
						io_flags, apptag_mask, apptag, req, true, rc);
		if (child == NULL) {
			return NULL;
		}

		remaining_lba_count -= lba_count;
		lba += lba_count;
		payload_offset += lba_count * sector_size;
		md_offset += lba_count * ns->md_size;
	}

	return req;
}

static inline bool
_is_io_flags_valid(uint32_t io_flags)
{
	if (io_flags & ~SPDK_NVME_IO_FLAGS_VALID_MASK) {
		/* Invalid io_flags */
		SPDK_ERRLOG("Invalid io_flags 0x%x\n", io_flags);
		return false;
	}

	return true;
}

static void
_nvme_ns_cmd_setup_request(struct spdk_nvme_ns *ns, struct nvme_request *req,
			   uint32_t opc, uint64_t lba, uint32_t lba_count,
			   uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag)
{
	struct spdk_nvme_cmd	*cmd;

	assert(_is_io_flags_valid(io_flags));

	cmd = &req->cmd;
	cmd->opc = opc;
	cmd->nsid = ns->id;

	*(uint64_t *)&cmd->cdw10 = lba;

	if (ns->flags & SPDK_NVME_NS_DPS_PI_SUPPORTED) {
		switch (ns->pi_type) {
		case SPDK_NVME_FMT_NVM_PROTECTION_TYPE1:
		case SPDK_NVME_FMT_NVM_PROTECTION_TYPE2:
			cmd->cdw14 = (uint32_t)lba;
			break;
		}
	}

	cmd->fuse = (io_flags & SPDK_NVME_IO_FLAGS_FUSE_MASK);

	cmd->cdw12 = lba_count - 1;
	cmd->cdw12 |= (io_flags & SPDK_NVME_IO_FLAGS_CDW12_MASK);

	cmd->cdw15 = apptag_mask;
	cmd->cdw15 = (cmd->cdw15 << 16 | apptag);
}

static struct nvme_request *
_nvme_ns_cmd_split_request_prp(struct spdk_nvme_ns *ns,
			       struct spdk_nvme_qpair *qpair,
			       const struct nvme_payload *payload,
			       uint32_t payload_offset, uint32_t md_offset,
			       uint64_t lba, uint32_t lba_count,
			       spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t opc,
			       uint32_t io_flags, struct nvme_request *req,
			       uint16_t apptag_mask, uint16_t apptag, int *rc)
{
	spdk_nvme_req_reset_sgl_cb reset_sgl_fn = req->payload.reset_sgl_fn;
	spdk_nvme_req_next_sge_cb next_sge_fn = req->payload.next_sge_fn;
	void *sgl_cb_arg = req->payload.contig_or_cb_arg;
	bool start_valid, end_valid, last_sge, child_equals_parent;
	uint64_t child_lba = lba;
	uint32_t req_current_length = 0;
	uint32_t child_length = 0;
	uint32_t sge_length;
	uint32_t page_size = qpair->ctrlr->page_size;
	uintptr_t address;

	reset_sgl_fn(sgl_cb_arg, payload_offset);
	next_sge_fn(sgl_cb_arg, (void **)&address, &sge_length);
	while (req_current_length < req->payload_size) {

		if (sge_length == 0) {
			continue;
		} else if (req_current_length + sge_length > req->payload_size) {
			sge_length = req->payload_size - req_current_length;
		}

		/*
		 * The start of the SGE is invalid if the start address is not page aligned,
		 *  unless it is the first SGE in the child request.
		 */
		start_valid = child_length == 0 || _is_page_aligned(address, page_size);

		/* Boolean for whether this is the last SGE in the parent request. */
		last_sge = (req_current_length + sge_length == req->payload_size);

		/*
		 * The end of the SGE is invalid if the end address is not page aligned,
		 *  unless it is the last SGE in the parent request.
		 */
		end_valid = last_sge || _is_page_aligned(address + sge_length, page_size);

		/*
		 * This child request equals the parent request, meaning that no splitting
		 *  was required for the parent request (the one passed into this function).
		 *  In this case, we do not create a child request at all - we just send
		 *  the original request as a single request at the end of this function.
		 */
		child_equals_parent = (child_length + sge_length == req->payload_size);

		if (start_valid) {
			/*
			 * The start of the SGE is valid, so advance the length parameters,
			 *  to include this SGE with previous SGEs for this child request
			 *  (if any).  If it is not valid, we do not advance the length
			 *  parameters nor get the next SGE, because we must send what has
			 *  been collected before this SGE as a child request.
			 */
			child_length += sge_length;
			req_current_length += sge_length;
			if (req_current_length < req->payload_size) {
				next_sge_fn(sgl_cb_arg, (void **)&address, &sge_length);
				/*
				 * If the next SGE is not page aligned, we will need to create a
				 *  child request for what we have so far, and then start a new
				 *  child request for the next SGE.
				 */
				start_valid = _is_page_aligned(address, page_size);
			}
		}

		if (start_valid && end_valid && !last_sge) {
			continue;
		}

		/*
		 * We need to create a split here.  Send what we have accumulated so far as a child
		 *  request.  Checking if child_equals_parent allows us to *not* create a child request
		 *  when no splitting is required - in that case we will fall-through and just create
		 *  a single request with no children for the entire I/O.
		 */
		if (!child_equals_parent) {
			struct nvme_request *child;
			uint32_t child_lba_count;

			if ((child_length % ns->extended_lba_size) != 0) {
				SPDK_ERRLOG("child_length %u not even multiple of lba_size %u\n",
					    child_length, ns->extended_lba_size);
				*rc = -EINVAL;
				return NULL;
			}
			child_lba_count = child_length / ns->extended_lba_size;
			/*
			 * Note the last parameter is set to "false" - this tells the recursive
			 *  call to _nvme_ns_cmd_rw() to not bother with checking for SGL splitting
			 *  since we have already verified it here.
			 */
			child = _nvme_add_child_request(ns, qpair, payload, payload_offset, md_offset,
							child_lba, child_lba_count,
							cb_fn, cb_arg, opc, io_flags,
							apptag_mask, apptag, req, false, rc);
			if (child == NULL) {
				return NULL;
			}
			payload_offset += child_length;
			md_offset += child_lba_count * ns->md_size;
			child_lba += child_lba_count;
			child_length = 0;
		}
	}

	if (child_length == req->payload_size) {
		/* No splitting was required, so setup the whole payload as one request. */
		_nvme_ns_cmd_setup_request(ns, req, opc, lba, lba_count, io_flags, apptag_mask, apptag);
	}

	return req;
}

static struct nvme_request *
_nvme_ns_cmd_split_request_sgl(struct spdk_nvme_ns *ns,
			       struct spdk_nvme_qpair *qpair,
			       const struct nvme_payload *payload,
			       uint32_t payload_offset, uint32_t md_offset,
			       uint64_t lba, uint32_t lba_count,
			       spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t opc,
			       uint32_t io_flags, struct nvme_request *req,
			       uint16_t apptag_mask, uint16_t apptag, int *rc)
{
	spdk_nvme_req_reset_sgl_cb reset_sgl_fn = req->payload.reset_sgl_fn;
	spdk_nvme_req_next_sge_cb next_sge_fn = req->payload.next_sge_fn;
	void *sgl_cb_arg = req->payload.contig_or_cb_arg;
	uint64_t child_lba = lba;
	uint32_t req_current_length = 0;
	uint32_t child_length = 0;
	uint32_t sge_length;
	uint16_t max_sges, num_sges;
	uintptr_t address;

	max_sges = ns->ctrlr->max_sges;

	reset_sgl_fn(sgl_cb_arg, payload_offset);
	num_sges = 0;

	while (req_current_length < req->payload_size) {
		next_sge_fn(sgl_cb_arg, (void **)&address, &sge_length);

		if (req_current_length + sge_length > req->payload_size) {
			sge_length = req->payload_size - req_current_length;
		}

		child_length += sge_length;
		req_current_length += sge_length;
		num_sges++;

		if (num_sges < max_sges && req_current_length < req->payload_size) {
			continue;
		}

		/*
		 * We need to create a split here.  Send what we have accumulated so far as a child
		 *  request.  Checking if the child equals the full payload allows us to *not*
		 *  create a child request when no splitting is required - in that case we will
		 *  fall-through and just create a single request with no children for the entire I/O.
		 */
		if (child_length != req->payload_size) {
			struct nvme_request *child;
			uint32_t child_lba_count;

			if ((child_length % ns->extended_lba_size) != 0) {
				SPDK_ERRLOG("child_length %u not even multiple of lba_size %u\n",
					    child_length, ns->extended_lba_size);
				*rc = -EINVAL;
				return NULL;
			}
			child_lba_count = child_length / ns->extended_lba_size;
			/*
			 * Note the last parameter is set to "false" - this tells the recursive
			 *  call to _nvme_ns_cmd_rw() to not bother with checking for SGL splitting
			 *  since we have already verified it here.
			 */
			child = _nvme_add_child_request(ns, qpair, payload, payload_offset, md_offset,
							child_lba, child_lba_count,
							cb_fn, cb_arg, opc, io_flags,
							apptag_mask, apptag, req, false, rc);
			if (child == NULL) {
				return NULL;
			}
			payload_offset += child_length;
			md_offset += child_lba_count * ns->md_size;
			child_lba += child_lba_count;
			child_length = 0;
			num_sges = 0;
		}
	}

	if (child_length == req->payload_size) {
		/* No splitting was required, so setup the whole payload as one request. */
		_nvme_ns_cmd_setup_request(ns, req, opc, lba, lba_count, io_flags, apptag_mask, apptag);
	}

	return req;
}

static inline struct nvme_request *
_nvme_ns_cmd_rw(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
		const struct nvme_payload *payload, uint32_t payload_offset, uint32_t md_offset,
		uint64_t lba, uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t opc,
		uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag, bool check_sgl, int *rc)
{
	struct nvme_request	*req;
	uint32_t		sector_size = _nvme_get_host_buffer_sector_size(ns, io_flags);
	uint32_t		sectors_per_max_io = _nvme_get_sectors_per_max_io(ns, io_flags);
	uint32_t		sectors_per_stripe = ns->sectors_per_stripe;

	assert(rc != NULL);
	assert(*rc == 0);

	req = nvme_allocate_request(qpair, payload, lba_count * sector_size, lba_count * ns->md_size,
				    cb_fn, cb_arg);
	if (req == NULL) {
		*rc = -ENOMEM;
		return NULL;
	}

	req->payload_offset = payload_offset;
	req->md_offset = md_offset;

	/* Zone append commands cannot be split. */
	if (opc == SPDK_NVME_OPC_ZONE_APPEND) {
		assert(ns->csi == SPDK_NVME_CSI_ZNS);
		/*
		 * As long as we disable driver-assisted striping for Zone append commands,
		 * _nvme_ns_cmd_rw() should never cause a proper request to be split.
		 * If a request is split, after all, error handling is done in caller functions.
		 */
		sectors_per_stripe = 0;
	}

	/*
	 * Intel DC P3*00 NVMe controllers benefit from driver-assisted striping.
	 * If this controller defines a stripe boundary and this I/O spans a stripe
	 *  boundary, split the request into multiple requests and submit each
	 *  separately to hardware.
	 */
	if (sectors_per_stripe > 0 &&
	    (((lba & (sectors_per_stripe - 1)) + lba_count) > sectors_per_stripe)) {

		return _nvme_ns_cmd_split_request(ns, qpair, payload, payload_offset, md_offset, lba, lba_count,
						  cb_fn,
						  cb_arg, opc,
						  io_flags, req, sectors_per_stripe, sectors_per_stripe - 1, apptag_mask, apptag, rc);
	} else if (lba_count > sectors_per_max_io) {
		return _nvme_ns_cmd_split_request(ns, qpair, payload, payload_offset, md_offset, lba, lba_count,
						  cb_fn,
						  cb_arg, opc,
						  io_flags, req, sectors_per_max_io, 0, apptag_mask, apptag, rc);
	} else if (nvme_payload_type(&req->payload) == NVME_PAYLOAD_TYPE_SGL && check_sgl) {
		if (ns->ctrlr->flags & SPDK_NVME_CTRLR_SGL_SUPPORTED) {
			return _nvme_ns_cmd_split_request_sgl(ns, qpair, payload, payload_offset, md_offset,
							      lba, lba_count, cb_fn, cb_arg, opc, io_flags,
							      req, apptag_mask, apptag, rc);
		} else {
			return _nvme_ns_cmd_split_request_prp(ns, qpair, payload, payload_offset, md_offset,
							      lba, lba_count, cb_fn, cb_arg, opc, io_flags,
							      req, apptag_mask, apptag, rc);
		}
	}

	_nvme_ns_cmd_setup_request(ns, req, opc, lba, lba_count, io_flags, apptag_mask, apptag);
	return req;
}

int
spdk_nvme_ns_cmd_compare(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, void *buffer,
			 uint64_t lba,
			 uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
			 uint32_t io_flags)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_CONTIG(buffer, NULL);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg,
			      SPDK_NVME_OPC_COMPARE,
			      io_flags, 0,
			      0, false, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_compare_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				 void *buffer,
				 void *metadata,
				 uint64_t lba,
				 uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
				 uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_CONTIG(buffer, metadata);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg,
			      SPDK_NVME_OPC_COMPARE,
			      io_flags,
			      apptag_mask, apptag, false, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_comparev(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			  uint64_t lba, uint32_t lba_count,
			  spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
			  spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
			  spdk_nvme_req_next_sge_cb next_sge_fn)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	if (reset_sgl_fn == NULL || next_sge_fn == NULL) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_SGL(reset_sgl_fn, next_sge_fn, cb_arg, NULL);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg,
			      SPDK_NVME_OPC_COMPARE,
			      io_flags, 0, 0, true, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_comparev_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				  uint64_t lba, uint32_t lba_count,
				  spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
				  spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
				  spdk_nvme_req_next_sge_cb next_sge_fn, void *metadata,
				  uint16_t apptag_mask, uint16_t apptag)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	if (reset_sgl_fn == NULL || next_sge_fn == NULL) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_SGL(reset_sgl_fn, next_sge_fn, cb_arg, metadata);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg,
			      SPDK_NVME_OPC_COMPARE, io_flags, apptag_mask, apptag, true, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_read(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, void *buffer,
		      uint64_t lba,
		      uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
		      uint32_t io_flags)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_CONTIG(buffer, NULL);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg, SPDK_NVME_OPC_READ,
			      io_flags, 0,
			      0, false, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_read_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, void *buffer,
			      void *metadata,
			      uint64_t lba,
			      uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
			      uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_CONTIG(buffer, metadata);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg, SPDK_NVME_OPC_READ,
			      io_flags,
			      apptag_mask, apptag, false, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_readv(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
		       uint64_t lba, uint32_t lba_count,
		       spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
		       spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
		       spdk_nvme_req_next_sge_cb next_sge_fn)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	if (reset_sgl_fn == NULL || next_sge_fn == NULL) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_SGL(reset_sgl_fn, next_sge_fn, cb_arg, NULL);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg, SPDK_NVME_OPC_READ,
			      io_flags, 0, 0, true, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_readv_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			       uint64_t lba, uint32_t lba_count,
			       spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
			       spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
			       spdk_nvme_req_next_sge_cb next_sge_fn, void *metadata,
			       uint16_t apptag_mask, uint16_t apptag)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	if (reset_sgl_fn == NULL || next_sge_fn == NULL) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_SGL(reset_sgl_fn, next_sge_fn, cb_arg, metadata);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg, SPDK_NVME_OPC_READ,
			      io_flags, apptag_mask, apptag, true, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_write(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
		       void *buffer, uint64_t lba,
		       uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
		       uint32_t io_flags)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_CONTIG(buffer, NULL);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg, SPDK_NVME_OPC_WRITE,
			      io_flags, 0, 0, false, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

static int
nvme_ns_cmd_check_zone_append(struct spdk_nvme_ns *ns, uint32_t lba_count, uint32_t io_flags)
{
	uint32_t sector_size;

	/* Not all NVMe Zoned Namespaces support the zone append command. */
	if (!(ns->ctrlr->flags & SPDK_NVME_CTRLR_ZONE_APPEND_SUPPORTED)) {
		return -EINVAL;
	}

	sector_size =  _nvme_get_host_buffer_sector_size(ns, io_flags);

	/* Fail a too large zone append command early. */
	if (lba_count * sector_size > ns->ctrlr->max_zone_append_size) {
		return -EINVAL;
	}

	return 0;
}

int
nvme_ns_cmd_zone_append_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				void *buffer, void *metadata, uint64_t zslba,
				uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
				uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	rc = nvme_ns_cmd_check_zone_append(ns, lba_count, io_flags);
	if (rc) {
		return rc;
	}

	payload = NVME_PAYLOAD_CONTIG(buffer, metadata);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, zslba, lba_count, cb_fn, cb_arg,
			      SPDK_NVME_OPC_ZONE_APPEND,
			      io_flags, apptag_mask, apptag, false, &rc);
	if (req != NULL) {
		/*
		 * Zone append commands cannot be split (num_children has to be 0).
		 * For NVME_PAYLOAD_TYPE_CONTIG, _nvme_ns_cmd_rw() should never cause a split
		 * to happen, since a too large request would have already been failed by
		 * nvme_ns_cmd_check_zone_append(), since zasl <= mdts.
		 */
		assert(req->num_children == 0);
		if (req->num_children) {
			nvme_request_free_children(req);
			nvme_free_request(req);
			return -EINVAL;
		}
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
nvme_ns_cmd_zone_appendv_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				 uint64_t zslba, uint32_t lba_count,
				 spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
				 spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
				 spdk_nvme_req_next_sge_cb next_sge_fn, void *metadata,
				 uint16_t apptag_mask, uint16_t apptag)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	if (reset_sgl_fn == NULL || next_sge_fn == NULL) {
		return -EINVAL;
	}

	rc = nvme_ns_cmd_check_zone_append(ns, lba_count, io_flags);
	if (rc) {
		return rc;
	}

	payload = NVME_PAYLOAD_SGL(reset_sgl_fn, next_sge_fn, cb_arg, metadata);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, zslba, lba_count, cb_fn, cb_arg,
			      SPDK_NVME_OPC_ZONE_APPEND,
			      io_flags, apptag_mask, apptag, true, &rc);
	if (req != NULL) {
		/*
		 * Zone append commands cannot be split (num_children has to be 0).
		 * For NVME_PAYLOAD_TYPE_SGL, _nvme_ns_cmd_rw() can cause a split.
		 * However, _nvme_ns_cmd_split_request_sgl() and _nvme_ns_cmd_split_request_prp()
		 * do not always cause a request to be split. These functions verify payload size,
		 * verify num sge < max_sge, and verify SGE alignment rules (in case of PRPs).
		 * If any of the verifications fail, they will split the request.
		 * In our case, a split is very unlikely, since we already verified the size using
		 * nvme_ns_cmd_check_zone_append(), however, we still need to call these functions
		 * in order to perform the verification part. If they do cause a split, we return
		 * an error here. For proper requests, these functions will never cause a split.
		 */
		if (req->num_children) {
			nvme_request_free_children(req);
			nvme_free_request(req);
			return -EINVAL;
		}
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_write_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			       void *buffer, void *metadata, uint64_t lba,
			       uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
			       uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_CONTIG(buffer, metadata);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg, SPDK_NVME_OPC_WRITE,
			      io_flags, apptag_mask, apptag, false, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_writev(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			uint64_t lba, uint32_t lba_count,
			spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
			spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
			spdk_nvme_req_next_sge_cb next_sge_fn)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	if (reset_sgl_fn == NULL || next_sge_fn == NULL) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_SGL(reset_sgl_fn, next_sge_fn, cb_arg, NULL);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg, SPDK_NVME_OPC_WRITE,
			      io_flags, 0, 0, true, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_writev_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				uint64_t lba, uint32_t lba_count,
				spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
				spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
				spdk_nvme_req_next_sge_cb next_sge_fn, void *metadata,
				uint16_t apptag_mask, uint16_t apptag)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	int rc = 0;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	if (reset_sgl_fn == NULL || next_sge_fn == NULL) {
		return -EINVAL;
	}

	payload = NVME_PAYLOAD_SGL(reset_sgl_fn, next_sge_fn, cb_arg, metadata);

	req = _nvme_ns_cmd_rw(ns, qpair, &payload, 0, 0, lba, lba_count, cb_fn, cb_arg, SPDK_NVME_OPC_WRITE,
			      io_flags, apptag_mask, apptag, true, &rc);
	if (req != NULL) {
		return nvme_qpair_submit_request(qpair, req);
	} else {
		return nvme_ns_map_failure_rc(lba_count,
					      ns->sectors_per_max_io,
					      ns->sectors_per_stripe,
					      qpair->ctrlr->opts.io_queue_requests,
					      rc);
	}
}

int
spdk_nvme_ns_cmd_write_zeroes(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			      uint64_t lba, uint32_t lba_count,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg,
			      uint32_t io_flags)
{
	struct nvme_request	*req;
	struct spdk_nvme_cmd	*cmd;
	uint64_t		*tmp_lba;

	if (!_is_io_flags_valid(io_flags)) {
		return -EINVAL;
	}

	if (lba_count == 0 || lba_count > UINT16_MAX + 1) {
		return -EINVAL;
	}

	req = nvme_allocate_request_null(qpair, cb_fn, cb_arg);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_WRITE_ZEROES;
	cmd->nsid = ns->id;

	tmp_lba = (uint64_t *)&cmd->cdw10;
	*tmp_lba = lba;
	cmd->cdw12 = lba_count - 1;
	cmd->fuse = (io_flags & SPDK_NVME_IO_FLAGS_FUSE_MASK);
	cmd->cdw12 |= (io_flags & SPDK_NVME_IO_FLAGS_CDW12_MASK);

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ns_cmd_write_uncorrectable(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				     uint64_t lba, uint32_t lba_count,
				     spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request	*req;
	struct spdk_nvme_cmd	*cmd;
	uint64_t		*tmp_lba;

	if (lba_count == 0 || lba_count > UINT16_MAX + 1) {
		return -EINVAL;
	}

	req = nvme_allocate_request_null(qpair, cb_fn, cb_arg);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_WRITE_UNCORRECTABLE;
	cmd->nsid = ns->id;

	tmp_lba = (uint64_t *)&cmd->cdw10;
	*tmp_lba = lba;
	cmd->cdw12 = lba_count - 1;

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ns_cmd_dataset_management(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				    uint32_t type,
				    const struct spdk_nvme_dsm_range *ranges, uint16_t num_ranges,
				    spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request	*req;
	struct spdk_nvme_cmd	*cmd;

	if (num_ranges == 0 || num_ranges > SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES) {
		return -EINVAL;
	}

	if (ranges == NULL) {
		return -EINVAL;
	}

	req = nvme_allocate_request_user_copy(qpair, (void *)ranges,
					      num_ranges * sizeof(struct spdk_nvme_dsm_range),
					      cb_fn, cb_arg, true);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_DATASET_MANAGEMENT;
	cmd->nsid = ns->id;

	cmd->cdw10_bits.dsm.nr = num_ranges - 1;
	cmd->cdw11 = type;

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ns_cmd_flush(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
		       spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request	*req;
	struct spdk_nvme_cmd	*cmd;

	req = nvme_allocate_request_null(qpair, cb_fn, cb_arg);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_FLUSH;
	cmd->nsid = ns->id;

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ns_cmd_reservation_register(struct spdk_nvme_ns *ns,
				      struct spdk_nvme_qpair *qpair,
				      struct spdk_nvme_reservation_register_data *payload,
				      bool ignore_key,
				      enum spdk_nvme_reservation_register_action action,
				      enum spdk_nvme_reservation_register_cptpl cptpl,
				      spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request	*req;
	struct spdk_nvme_cmd	*cmd;

	req = nvme_allocate_request_user_copy(qpair,
					      payload, sizeof(struct spdk_nvme_reservation_register_data),
					      cb_fn, cb_arg, true);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_RESERVATION_REGISTER;
	cmd->nsid = ns->id;

	cmd->cdw10_bits.resv_register.rrega = action;
	cmd->cdw10_bits.resv_register.iekey = ignore_key;
	cmd->cdw10_bits.resv_register.cptpl = cptpl;

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ns_cmd_reservation_release(struct spdk_nvme_ns *ns,
				     struct spdk_nvme_qpair *qpair,
				     struct spdk_nvme_reservation_key_data *payload,
				     bool ignore_key,
				     enum spdk_nvme_reservation_release_action action,
				     enum spdk_nvme_reservation_type type,
				     spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request	*req;
	struct spdk_nvme_cmd	*cmd;

	req = nvme_allocate_request_user_copy(qpair,
					      payload, sizeof(struct spdk_nvme_reservation_key_data), cb_fn,
					      cb_arg, true);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_RESERVATION_RELEASE;
	cmd->nsid = ns->id;

	cmd->cdw10_bits.resv_release.rrela = action;
	cmd->cdw10_bits.resv_release.iekey = ignore_key;
	cmd->cdw10_bits.resv_release.rtype = type;

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ns_cmd_reservation_acquire(struct spdk_nvme_ns *ns,
				     struct spdk_nvme_qpair *qpair,
				     struct spdk_nvme_reservation_acquire_data *payload,
				     bool ignore_key,
				     enum spdk_nvme_reservation_acquire_action action,
				     enum spdk_nvme_reservation_type type,
				     spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request	*req;
	struct spdk_nvme_cmd	*cmd;

	req = nvme_allocate_request_user_copy(qpair,
					      payload, sizeof(struct spdk_nvme_reservation_acquire_data),
					      cb_fn, cb_arg, true);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_RESERVATION_ACQUIRE;
	cmd->nsid = ns->id;

	cmd->cdw10_bits.resv_acquire.racqa = action;
	cmd->cdw10_bits.resv_acquire.iekey = ignore_key;
	cmd->cdw10_bits.resv_acquire.rtype = type;

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ns_cmd_reservation_report(struct spdk_nvme_ns *ns,
				    struct spdk_nvme_qpair *qpair,
				    void *payload, uint32_t len,
				    spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	uint32_t		num_dwords;
	struct nvme_request	*req;
	struct spdk_nvme_cmd	*cmd;

	if (len % 4) {
		return -EINVAL;
	}
	num_dwords = len / 4;

	req = nvme_allocate_request_user_copy(qpair, payload, len, cb_fn, cb_arg, false);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_RESERVATION_REPORT;
	cmd->nsid = ns->id;

	cmd->cdw10 = num_dwords;

	return nvme_qpair_submit_request(qpair, req);
}
