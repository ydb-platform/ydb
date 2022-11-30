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

int
spdk_nvme_ctrlr_io_cmd_raw_no_payload_build(struct spdk_nvme_ctrlr *ctrlr,
		struct spdk_nvme_qpair *qpair,
		struct spdk_nvme_cmd *cmd,
		spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct nvme_payload payload;

	if (ctrlr->trid.trtype != SPDK_NVME_TRANSPORT_PCIE) {
		return -EINVAL;
	}

	memset(&payload, 0, sizeof(payload));
	req = nvme_allocate_request(qpair, &payload, 0, 0, cb_fn, cb_arg);

	if (req == NULL) {
		return -ENOMEM;
	}

	memcpy(&req->cmd, cmd, sizeof(req->cmd));

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ctrlr_cmd_io_raw(struct spdk_nvme_ctrlr *ctrlr,
			   struct spdk_nvme_qpair *qpair,
			   struct spdk_nvme_cmd *cmd,
			   void *buf, uint32_t len,
			   spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request	*req;

	req = nvme_allocate_request_contig(qpair, buf, len, cb_fn, cb_arg);

	if (req == NULL) {
		return -ENOMEM;
	}

	memcpy(&req->cmd, cmd, sizeof(req->cmd));

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ctrlr_cmd_io_raw_with_md(struct spdk_nvme_ctrlr *ctrlr,
				   struct spdk_nvme_qpair *qpair,
				   struct spdk_nvme_cmd *cmd,
				   void *buf, uint32_t len, void *md_buf,
				   spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct nvme_payload payload;
	uint32_t md_len = 0;

	payload = NVME_PAYLOAD_CONTIG(buf, md_buf);

	/* Caculate metadata length */
	if (md_buf) {
		struct spdk_nvme_ns *ns = &ctrlr->ns[cmd->nsid - 1];

		assert(ns->sector_size != 0);
		md_len =  len / ns->sector_size * ns->md_size;
	}

	req = nvme_allocate_request(qpair, &payload, len, md_len, cb_fn, cb_arg);
	if (req == NULL) {
		return -ENOMEM;
	}

	memcpy(&req->cmd, cmd, sizeof(req->cmd));

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_ctrlr_cmd_admin_raw(struct spdk_nvme_ctrlr *ctrlr,
			      struct spdk_nvme_cmd *cmd,
			      void *buf, uint32_t len,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request	*req;
	int			rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_contig(ctrlr->adminq, buf, len, cb_fn, cb_arg);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	memcpy(&req->cmd, cmd, sizeof(req->cmd));

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

int
nvme_ctrlr_cmd_identify(struct spdk_nvme_ctrlr *ctrlr, uint8_t cns, uint16_t cntid, uint32_t nsid,
			uint8_t csi, void *payload, size_t payload_size,
			spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;

	req = nvme_allocate_request_user_copy(ctrlr->adminq,
					      payload, payload_size,
					      cb_fn, cb_arg, false);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_IDENTIFY;
	cmd->cdw10_bits.identify.cns = cns;
	cmd->cdw10_bits.identify.cntid = cntid;
	cmd->cdw11_bits.identify.csi = csi;
	cmd->nsid = nsid;

	return nvme_ctrlr_submit_admin_request(ctrlr, req);
}

int
nvme_ctrlr_cmd_attach_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
			 struct spdk_nvme_ctrlr_list *payload, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request			*req;
	struct spdk_nvme_cmd			*cmd;
	int					rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq,
					      payload, sizeof(struct spdk_nvme_ctrlr_list),
					      cb_fn, cb_arg, true);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_NS_ATTACHMENT;
	cmd->nsid = nsid;
	cmd->cdw10_bits.ns_attach.sel = SPDK_NVME_NS_CTRLR_ATTACH;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

int
nvme_ctrlr_cmd_detach_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
			 struct spdk_nvme_ctrlr_list *payload, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request			*req;
	struct spdk_nvme_cmd			*cmd;
	int					rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq,
					      payload, sizeof(struct spdk_nvme_ctrlr_list),
					      cb_fn, cb_arg, true);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_NS_ATTACHMENT;
	cmd->nsid = nsid;
	cmd->cdw10_bits.ns_attach.sel = SPDK_NVME_NS_CTRLR_DETACH;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

int
nvme_ctrlr_cmd_create_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns_data *payload,
			 spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request			*req;
	struct spdk_nvme_cmd			*cmd;
	int					rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq,
					      payload, sizeof(struct spdk_nvme_ns_data),
					      cb_fn, cb_arg, true);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_NS_MANAGEMENT;
	cmd->cdw10_bits.ns_manage.sel = SPDK_NVME_NS_MANAGEMENT_CREATE;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

int
nvme_ctrlr_cmd_delete_ns(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid, spdk_nvme_cmd_cb cb_fn,
			 void *cb_arg)
{
	struct nvme_request			*req;
	struct spdk_nvme_cmd			*cmd;
	int					rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_null(ctrlr->adminq, cb_fn, cb_arg);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_NS_MANAGEMENT;
	cmd->cdw10_bits.ns_manage.sel = SPDK_NVME_NS_MANAGEMENT_DELETE;
	cmd->nsid = nsid;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

int
nvme_ctrlr_cmd_doorbell_buffer_config(struct spdk_nvme_ctrlr *ctrlr, uint64_t prp1, uint64_t prp2,
				      spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request			*req;
	struct spdk_nvme_cmd			*cmd;
	int					rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_null(ctrlr->adminq, cb_fn, cb_arg);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_DOORBELL_BUFFER_CONFIG;
	cmd->dptr.prp.prp1 = prp1;
	cmd->dptr.prp.prp2 = prp2;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

int
nvme_ctrlr_cmd_format(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid, struct spdk_nvme_format *format,
		      spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_null(ctrlr->adminq, cb_fn, cb_arg);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_FORMAT_NVM;
	cmd->nsid = nsid;
	memcpy(&cmd->cdw10, format, sizeof(uint32_t));

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
spdk_nvme_ctrlr_cmd_set_feature(struct spdk_nvme_ctrlr *ctrlr, uint8_t feature,
				uint32_t cdw11, uint32_t cdw12, void *payload, uint32_t payload_size,
				spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq, payload, payload_size, cb_fn, cb_arg,
					      true);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_SET_FEATURES;
	cmd->cdw10_bits.set_features.fid = feature;
	cmd->cdw11 = cdw11;
	cmd->cdw12 = cdw12;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
spdk_nvme_ctrlr_cmd_get_feature(struct spdk_nvme_ctrlr *ctrlr, uint8_t feature,
				uint32_t cdw11, void *payload, uint32_t payload_size,
				spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq, payload, payload_size, cb_fn, cb_arg,
					      false);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_GET_FEATURES;
	cmd->cdw10_bits.get_features.fid = feature;
	cmd->cdw11 = cdw11;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
spdk_nvme_ctrlr_cmd_get_feature_ns(struct spdk_nvme_ctrlr *ctrlr, uint8_t feature,
				   uint32_t cdw11, void *payload,
				   uint32_t payload_size, spdk_nvme_cmd_cb cb_fn,
				   void *cb_arg, uint32_t ns_id)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq, payload, payload_size, cb_fn, cb_arg,
					      false);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_GET_FEATURES;
	cmd->cdw10_bits.get_features.fid = feature;
	cmd->cdw11 = cdw11;
	cmd->nsid = ns_id;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int spdk_nvme_ctrlr_cmd_set_feature_ns(struct spdk_nvme_ctrlr *ctrlr, uint8_t feature,
				       uint32_t cdw11, uint32_t cdw12, void *payload,
				       uint32_t payload_size, spdk_nvme_cmd_cb cb_fn,
				       void *cb_arg, uint32_t ns_id)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq, payload, payload_size, cb_fn, cb_arg,
					      true);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_SET_FEATURES;
	cmd->cdw10_bits.set_features.fid = feature;
	cmd->cdw11 = cdw11;
	cmd->cdw12 = cdw12;
	cmd->nsid = ns_id;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
nvme_ctrlr_cmd_set_num_queues(struct spdk_nvme_ctrlr *ctrlr,
			      uint32_t num_queues, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	union spdk_nvme_feat_number_of_queues feat_num_queues;

	feat_num_queues.raw = 0;
	feat_num_queues.bits.nsqr = num_queues - 1;
	feat_num_queues.bits.ncqr = num_queues - 1;

	return spdk_nvme_ctrlr_cmd_set_feature(ctrlr, SPDK_NVME_FEAT_NUMBER_OF_QUEUES, feat_num_queues.raw,
					       0,
					       NULL, 0, cb_fn, cb_arg);
}

int
nvme_ctrlr_cmd_get_num_queues(struct spdk_nvme_ctrlr *ctrlr,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return spdk_nvme_ctrlr_cmd_get_feature(ctrlr, SPDK_NVME_FEAT_NUMBER_OF_QUEUES, 0, NULL, 0,
					       cb_fn, cb_arg);
}

int
nvme_ctrlr_cmd_set_async_event_config(struct spdk_nvme_ctrlr *ctrlr,
				      union spdk_nvme_feat_async_event_configuration config, spdk_nvme_cmd_cb cb_fn,
				      void *cb_arg)
{
	uint32_t cdw11;

	cdw11 = config.raw;
	return spdk_nvme_ctrlr_cmd_set_feature(ctrlr, SPDK_NVME_FEAT_ASYNC_EVENT_CONFIGURATION, cdw11, 0,
					       NULL, 0,
					       cb_fn, cb_arg);
}

int
nvme_ctrlr_cmd_set_host_id(struct spdk_nvme_ctrlr *ctrlr, void *host_id, uint32_t host_id_size,
			   spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	union spdk_nvme_feat_host_identifier feat_host_identifier;

	feat_host_identifier.raw = 0;
	if (host_id_size == 16) {
		/* 128-bit extended host identifier */
		feat_host_identifier.bits.exhid = 1;
	} else if (host_id_size == 8) {
		/* 64-bit host identifier */
		feat_host_identifier.bits.exhid = 0;
	} else {
		SPDK_ERRLOG("Invalid host ID size %u\n", host_id_size);
		return -EINVAL;
	}

	return spdk_nvme_ctrlr_cmd_set_feature(ctrlr, SPDK_NVME_FEAT_HOST_IDENTIFIER,
					       feat_host_identifier.raw, 0,
					       host_id, host_id_size, cb_fn, cb_arg);
}

int
spdk_nvme_ctrlr_cmd_get_log_page_ext(struct spdk_nvme_ctrlr *ctrlr, uint8_t log_page,
				     uint32_t nsid, void *payload, uint32_t payload_size,
				     uint64_t offset, uint32_t cdw10,
				     uint32_t cdw11, uint32_t cdw14,
				     spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	uint32_t numd, numdl, numdu;
	uint32_t lpol, lpou;
	int rc;

	if (payload_size == 0) {
		return -EINVAL;
	}

	if (offset & 3) {
		return -EINVAL;
	}

	numd = spdk_nvme_bytes_to_numd(payload_size);
	numdl = numd & 0xFFFFu;
	numdu = (numd >> 16) & 0xFFFFu;

	lpol = (uint32_t)offset;
	lpou = (uint32_t)(offset >> 32);

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);

	if (offset && !ctrlr->cdata.lpa.edlp) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -EINVAL;
	}

	req = nvme_allocate_request_user_copy(ctrlr->adminq,
					      payload, payload_size, cb_fn, cb_arg, false);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_GET_LOG_PAGE;
	cmd->nsid = nsid;
	cmd->cdw10 = cdw10;
	cmd->cdw10_bits.get_log_page.numdl = numdl;
	cmd->cdw10_bits.get_log_page.lid = log_page;

	cmd->cdw11 = cdw11;
	cmd->cdw11_bits.get_log_page.numdu = numdu;
	cmd->cdw12 = lpol;
	cmd->cdw13 = lpou;
	cmd->cdw14 = cdw14;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
spdk_nvme_ctrlr_cmd_get_log_page(struct spdk_nvme_ctrlr *ctrlr, uint8_t log_page,
				 uint32_t nsid, void *payload, uint32_t payload_size,
				 uint64_t offset, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return spdk_nvme_ctrlr_cmd_get_log_page_ext(ctrlr, log_page, nsid, payload,
			payload_size, offset, 0, 0, 0, cb_fn, cb_arg);
}

static void
nvme_ctrlr_retry_queued_abort(struct spdk_nvme_ctrlr *ctrlr)
{
	struct nvme_request	*next, *tmp;
	int rc;

	if (ctrlr->is_resetting || ctrlr->is_destructed) {
		return;
	}

	STAILQ_FOREACH_SAFE(next, &ctrlr->queued_aborts, stailq, tmp) {
		STAILQ_REMOVE_HEAD(&ctrlr->queued_aborts, stailq);
		ctrlr->outstanding_aborts++;
		rc = nvme_ctrlr_submit_admin_request(ctrlr, next);
		if (rc < 0) {
			SPDK_ERRLOG("Failed to submit queued abort.\n");
			memset(&next->cpl, 0, sizeof(next->cpl));
			next->cpl.status.sct = SPDK_NVME_SCT_GENERIC;
			next->cpl.status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
			next->cpl.status.dnr = 1;
			nvme_complete_request(next->cb_fn, next->cb_arg, next->qpair, next, &next->cpl);
			nvme_free_request(next);
		} else {
			/* If the first abort succeeds, stop iterating. */
			break;
		}
	}
}

static int
_nvme_ctrlr_submit_abort_request(struct spdk_nvme_ctrlr *ctrlr,
				 struct nvme_request *req)
{
	/* ACL is a 0's based value. */
	if (ctrlr->outstanding_aborts >= ctrlr->cdata.acl + 1U) {
		STAILQ_INSERT_TAIL(&ctrlr->queued_aborts, req, stailq);
		return 0;
	} else {
		ctrlr->outstanding_aborts++;
		return nvme_ctrlr_submit_admin_request(ctrlr, req);
	}
}

static void
nvme_ctrlr_cmd_abort_cpl(void *ctx, const struct spdk_nvme_cpl *cpl)
{
	struct nvme_request	*req = ctx;
	struct spdk_nvme_ctrlr	*ctrlr;

	ctrlr = req->qpair->ctrlr;

	ctrlr->outstanding_aborts--;
	nvme_ctrlr_retry_queued_abort(ctrlr);

	req->user_cb_fn(req->user_cb_arg, cpl);
}

int
spdk_nvme_ctrlr_cmd_abort(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair,
			  uint16_t cid, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	int rc;
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;

	if (qpair == NULL) {
		qpair = ctrlr->adminq;
	}

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_null(ctrlr->adminq, nvme_ctrlr_cmd_abort_cpl, NULL);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}
	req->cb_arg = req;
	req->user_cb_fn = cb_fn;
	req->user_cb_arg = cb_arg;

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_ABORT;
	cmd->cdw10_bits.abort.sqid = qpair->id;
	cmd->cdw10_bits.abort.cid = cid;

	rc = _nvme_ctrlr_submit_abort_request(ctrlr, req);

	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

static void
nvme_complete_abort_request(void *ctx, const struct spdk_nvme_cpl *cpl)
{
	struct nvme_request *req = ctx;
	struct nvme_request *parent = req->parent;
	struct spdk_nvme_ctrlr *ctrlr;

	ctrlr = req->qpair->ctrlr;

	ctrlr->outstanding_aborts--;
	nvme_ctrlr_retry_queued_abort(ctrlr);

	nvme_request_remove_child(parent, req);

	if (!spdk_nvme_cpl_is_abort_success(cpl)) {
		parent->parent_status.cdw0 |= 1U;
	}

	if (parent->num_children == 0) {
		nvme_complete_request(parent->cb_fn, parent->cb_arg, parent->qpair,
				      parent, &parent->parent_status);
		nvme_free_request(parent);
	}
}

static int
nvme_request_add_abort(struct nvme_request *req, void *arg)
{
	struct nvme_request *parent = arg;
	struct nvme_request *child;
	void *cmd_cb_arg;

	cmd_cb_arg = parent->user_cb_arg;

	if (req->cb_arg != cmd_cb_arg &&
	    (req->parent == NULL || req->parent->cb_arg != cmd_cb_arg)) {
		return 0;
	}

	child = nvme_allocate_request_null(parent->qpair->ctrlr->adminq,
					   nvme_complete_abort_request, NULL);
	if (child == NULL) {
		return -ENOMEM;
	}

	child->cb_arg = child;

	child->cmd.opc = SPDK_NVME_OPC_ABORT;
	/* Copy SQID from the parent. */
	child->cmd.cdw10_bits.abort.sqid = parent->cmd.cdw10_bits.abort.sqid;
	child->cmd.cdw10_bits.abort.cid = req->cmd.cid;

	child->parent = parent;

	TAILQ_INSERT_TAIL(&parent->children, child, child_tailq);
	parent->num_children++;

	return 0;
}

int
spdk_nvme_ctrlr_cmd_abort_ext(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_qpair *qpair,
			      void *cmd_cb_arg,
			      spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	int rc = 0;
	struct nvme_request *parent, *child, *tmp;
	bool child_failed = false;
	int aborted = 0;

	if (cmd_cb_arg == NULL) {
		return -EINVAL;
	}

	pthread_mutex_lock(&ctrlr->ctrlr_lock);

	if (qpair == NULL) {
		qpair = ctrlr->adminq;
	}

	parent = nvme_allocate_request_null(ctrlr->adminq, cb_fn, cb_arg);
	if (parent == NULL) {
		pthread_mutex_unlock(&ctrlr->ctrlr_lock);

		return -ENOMEM;
	}

	TAILQ_INIT(&parent->children);
	parent->num_children = 0;

	parent->cmd.opc = SPDK_NVME_OPC_ABORT;
	memset(&parent->parent_status, 0, sizeof(struct spdk_nvme_cpl));

	/* Hold SQID that the requests to abort are associated with.
	 * This will be copied to the children.
	 *
	 * CID is not set here because the parent is not submitted directly
	 * and CID is not determined until request to abort is found.
	 */
	parent->cmd.cdw10_bits.abort.sqid = qpair->id;

	/* This is used to find request to abort. */
	parent->user_cb_arg = cmd_cb_arg;

	/* Add an abort request for each outstanding request which has cmd_cb_arg
	 * as its callback context.
	 */
	rc = nvme_transport_qpair_iterate_requests(qpair, nvme_request_add_abort, parent);
	if (rc != 0) {
		/* Free abort requests already added. */
		child_failed = true;
	}

	TAILQ_FOREACH_SAFE(child, &parent->children, child_tailq, tmp) {
		if (spdk_likely(!child_failed)) {
			rc = _nvme_ctrlr_submit_abort_request(ctrlr, child);
			if (spdk_unlikely(rc != 0)) {
				child_failed = true;
			}
		} else {
			/* Free remaining abort requests. */
			nvme_request_remove_child(parent, child);
			nvme_free_request(child);
		}
	}

	if (spdk_likely(!child_failed)) {
		/* There is no error so far. Abort requests were submitted successfully
		 * or there was no outstanding request to abort.
		 *
		 * Hence abort queued requests which has cmd_cb_arg as its callback
		 * context next.
		 */
		aborted = nvme_qpair_abort_queued_reqs(qpair, cmd_cb_arg);
		if (parent->num_children == 0) {
			/* There was no outstanding request to abort. */
			if (aborted > 0) {
				/* The queued requests were successfully aborted. Hence
				 * complete the parent request with success synchronously.
				 */
				nvme_complete_request(parent->cb_fn, parent->cb_arg, parent->qpair,
						      parent, &parent->parent_status);
				nvme_free_request(parent);
			} else {
				/* There was no queued request to abort. */
				rc = -ENOENT;
			}
		}
	} else {
		/* Failed to add or submit abort request. */
		if (parent->num_children != 0) {
			/* Return success since we must wait for those children
			 * to complete but set the parent request to failure.
			 */
			parent->parent_status.cdw0 |= 1U;
			rc = 0;
		}
	}

	if (rc != 0) {
		nvme_free_request(parent);
	}

	pthread_mutex_unlock(&ctrlr->ctrlr_lock);
	return rc;
}

int
nvme_ctrlr_cmd_fw_commit(struct spdk_nvme_ctrlr *ctrlr,
			 const struct spdk_nvme_fw_commit *fw_commit,
			 spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_null(ctrlr->adminq, cb_fn, cb_arg);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_FIRMWARE_COMMIT;
	memcpy(&cmd->cdw10, fw_commit, sizeof(uint32_t));

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;

}

int
nvme_ctrlr_cmd_fw_image_download(struct spdk_nvme_ctrlr *ctrlr,
				 uint32_t size, uint32_t offset, void *payload,
				 spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq, payload, size, cb_fn, cb_arg, true);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_FIRMWARE_IMAGE_DOWNLOAD;
	cmd->cdw10 = spdk_nvme_bytes_to_numd(size);
	cmd->cdw11 = offset >> 2;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
spdk_nvme_ctrlr_cmd_security_receive(struct spdk_nvme_ctrlr *ctrlr, uint8_t secp,
				     uint16_t spsp, uint8_t nssf, void *payload,
				     uint32_t payload_size, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq, payload, payload_size,
					      cb_fn, cb_arg, false);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_SECURITY_RECEIVE;
	cmd->cdw10_bits.sec_send_recv.nssf = nssf;
	cmd->cdw10_bits.sec_send_recv.spsp0 = (uint8_t)spsp;
	cmd->cdw10_bits.sec_send_recv.spsp1 = (uint8_t)(spsp >> 8);
	cmd->cdw10_bits.sec_send_recv.secp = secp;
	cmd->cdw11 = payload_size;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
spdk_nvme_ctrlr_cmd_security_send(struct spdk_nvme_ctrlr *ctrlr, uint8_t secp,
				  uint16_t spsp, uint8_t nssf, void *payload,
				  uint32_t payload_size, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq, payload, payload_size,
					      cb_fn, cb_arg, true);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_SECURITY_SEND;
	cmd->cdw10_bits.sec_send_recv.nssf = nssf;
	cmd->cdw10_bits.sec_send_recv.spsp0 = (uint8_t)spsp;
	cmd->cdw10_bits.sec_send_recv.spsp1 = (uint8_t)(spsp >> 8);
	cmd->cdw10_bits.sec_send_recv.secp = secp;
	cmd->cdw11 = payload_size;

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
nvme_ctrlr_cmd_sanitize(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
			struct spdk_nvme_sanitize *sanitize, uint32_t cdw11,
			spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_null(ctrlr->adminq, cb_fn, cb_arg);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_SANITIZE;
	cmd->nsid = nsid;
	cmd->cdw11 = cdw11;
	memcpy(&cmd->cdw10, sanitize, sizeof(cmd->cdw10));

	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

static int
nvme_ctrlr_cmd_directive(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
			 uint32_t doper, uint32_t dtype, uint32_t dspec,
			 void *payload, uint32_t payload_size, uint32_t cdw12,
			 uint32_t cdw13, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
			 uint16_t opc_type, bool host_to_ctrlr)
{
	struct nvme_request *req = NULL;
	struct spdk_nvme_cmd *cmd = NULL;
	int rc;

	nvme_robust_mutex_lock(&ctrlr->ctrlr_lock);
	req = nvme_allocate_request_user_copy(ctrlr->adminq, payload, payload_size,
					      cb_fn, cb_arg, host_to_ctrlr);
	if (req == NULL) {
		nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);
		return -ENOMEM;
	}
	cmd = &req->cmd;
	cmd->opc = opc_type;
	cmd->nsid = nsid;

	cmd->cdw10 = (payload_size >> 2) - 1;
	cmd->cdw11_bits.directive.doper = doper;
	cmd->cdw11_bits.directive.dtype = dtype;
	cmd->cdw11_bits.directive.dspec = dspec;
	cmd->cdw12 = cdw12;
	cmd->cdw13 = cdw13;
	rc = nvme_ctrlr_submit_admin_request(ctrlr, req);
	nvme_robust_mutex_unlock(&ctrlr->ctrlr_lock);

	return rc;
}

int
spdk_nvme_ctrlr_cmd_directive_send(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
				   uint32_t doper, uint32_t dtype, uint32_t dspec,
				   void *payload, uint32_t payload_size, uint32_t cdw12,
				   uint32_t cdw13, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_ctrlr_cmd_directive(ctrlr, nsid, doper, dtype, dspec,
					payload, payload_size, cdw12, cdw13, cb_fn, cb_arg,
					SPDK_NVME_OPC_DIRECTIVE_SEND, true);
}

int
spdk_nvme_ctrlr_cmd_directive_receive(struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid,
				      uint32_t doper, uint32_t dtype, uint32_t dspec,
				      void *payload, uint32_t payload_size, uint32_t cdw12,
				      uint32_t cdw13, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_ctrlr_cmd_directive(ctrlr, nsid, doper, dtype, dspec,
					payload, payload_size, cdw12, cdw13, cb_fn, cb_arg,
					SPDK_NVME_OPC_DIRECTIVE_RECEIVE, false);
}
