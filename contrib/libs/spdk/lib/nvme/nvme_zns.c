#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) 2020, Western Digital Corporation. All rights reserved.
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

#include "spdk/nvme_zns.h"
#include "nvme_internal.h"

const struct spdk_nvme_zns_ns_data *
spdk_nvme_zns_ns_get_data(struct spdk_nvme_ns *ns)
{
	return ns->nsdata_zns;
}

uint64_t
spdk_nvme_zns_ns_get_zone_size_sectors(struct spdk_nvme_ns *ns)
{
	const struct spdk_nvme_zns_ns_data *nsdata_zns = spdk_nvme_zns_ns_get_data(ns);
	const struct spdk_nvme_ns_data *nsdata = spdk_nvme_ns_get_data(ns);

	return nsdata_zns->lbafe[nsdata->flbas.format].zsze;
}

uint64_t
spdk_nvme_zns_ns_get_zone_size(struct spdk_nvme_ns *ns)
{
	return spdk_nvme_zns_ns_get_zone_size_sectors(ns) * spdk_nvme_ns_get_sector_size(ns);
}

uint64_t
spdk_nvme_zns_ns_get_num_zones(struct spdk_nvme_ns *ns)
{
	return spdk_nvme_ns_get_num_sectors(ns) / spdk_nvme_zns_ns_get_zone_size_sectors(ns);
}

uint32_t
spdk_nvme_zns_ns_get_max_open_zones(struct spdk_nvme_ns *ns)
{
	const struct spdk_nvme_zns_ns_data *nsdata_zns = spdk_nvme_zns_ns_get_data(ns);

	return nsdata_zns->mor + 1;
}

uint32_t
spdk_nvme_zns_ns_get_max_active_zones(struct spdk_nvme_ns *ns)
{
	const struct spdk_nvme_zns_ns_data *nsdata_zns = spdk_nvme_zns_ns_get_data(ns);

	return nsdata_zns->mar + 1;
}

const struct spdk_nvme_zns_ctrlr_data *
spdk_nvme_zns_ctrlr_get_data(struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->cdata_zns;
}

uint32_t
spdk_nvme_zns_ctrlr_get_max_zone_append_size(const struct spdk_nvme_ctrlr *ctrlr)
{
	return ctrlr->max_zone_append_size;
}

int
spdk_nvme_zns_zone_append(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			  void *buffer, uint64_t zslba,
			  uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
			  uint32_t io_flags)
{
	return nvme_ns_cmd_zone_append_with_md(ns, qpair, buffer, NULL, zslba, lba_count,
					       cb_fn, cb_arg, io_flags, 0, 0);
}

int
spdk_nvme_zns_zone_append_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				  void *buffer, void *metadata, uint64_t zslba,
				  uint32_t lba_count, spdk_nvme_cmd_cb cb_fn, void *cb_arg,
				  uint32_t io_flags, uint16_t apptag_mask, uint16_t apptag)
{
	return nvme_ns_cmd_zone_append_with_md(ns, qpair, buffer, metadata, zslba, lba_count,
					       cb_fn, cb_arg, io_flags, apptag_mask, apptag);
}

int
spdk_nvme_zns_zone_appendv(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			   uint64_t zslba, uint32_t lba_count,
			   spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
			   spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
			   spdk_nvme_req_next_sge_cb next_sge_fn)
{
	return nvme_ns_cmd_zone_appendv_with_md(ns, qpair, zslba, lba_count, cb_fn, cb_arg,
						io_flags, reset_sgl_fn, next_sge_fn,
						NULL, 0, 0);
}

int
spdk_nvme_zns_zone_appendv_with_md(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
				   uint64_t zslba, uint32_t lba_count,
				   spdk_nvme_cmd_cb cb_fn, void *cb_arg, uint32_t io_flags,
				   spdk_nvme_req_reset_sgl_cb reset_sgl_fn,
				   spdk_nvme_req_next_sge_cb next_sge_fn, void *metadata,
				   uint16_t apptag_mask, uint16_t apptag)
{
	return nvme_ns_cmd_zone_appendv_with_md(ns, qpair, zslba, lba_count, cb_fn, cb_arg,
						io_flags, reset_sgl_fn, next_sge_fn,
						metadata, apptag_mask, apptag);
}

static int
nvme_zns_zone_mgmt_recv(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			void *payload, uint32_t payload_size, uint64_t slba,
			uint8_t zone_recv_action, uint8_t zra_spec_field, bool zra_spec_feats,
			spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;

	req = nvme_allocate_request_user_copy(qpair, payload, payload_size, cb_fn, cb_arg, false);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_ZONE_MGMT_RECV;
	cmd->nsid = ns->id;

	*(uint64_t *)&cmd->cdw10 = slba;
	cmd->cdw12 = spdk_nvme_bytes_to_numd(payload_size);
	cmd->cdw13 = zone_recv_action | zra_spec_field << 8 | zra_spec_feats << 16;

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_zns_report_zones(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			   void *payload, uint32_t payload_size, uint64_t slba,
			   enum spdk_nvme_zns_zra_report_opts report_opts, bool partial_report,
			   spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_zns_zone_mgmt_recv(ns, qpair, payload, payload_size, slba,
				       SPDK_NVME_ZONE_REPORT, report_opts, partial_report,
				       cb_fn, cb_arg);
}

static int
nvme_zns_zone_mgmt_send(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
			uint64_t slba, bool select_all, uint8_t zone_send_action,
			spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	struct nvme_request *req;
	struct spdk_nvme_cmd *cmd;

	req = nvme_allocate_request_null(qpair, cb_fn, cb_arg);
	if (req == NULL) {
		return -ENOMEM;
	}

	cmd = &req->cmd;
	cmd->opc = SPDK_NVME_OPC_ZONE_MGMT_SEND;
	cmd->nsid = ns->id;

	if (!select_all) {
		*(uint64_t *)&cmd->cdw10 = slba;
	}

	cmd->cdw13 = zone_send_action | select_all << 8;

	return nvme_qpair_submit_request(qpair, req);
}

int
spdk_nvme_zns_close_zone(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, uint64_t slba,
			 bool select_all, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_zns_zone_mgmt_send(ns, qpair, slba, select_all, SPDK_NVME_ZONE_CLOSE,
				       cb_fn, cb_arg);
}

int
spdk_nvme_zns_finish_zone(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, uint64_t slba,
			  bool select_all, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_zns_zone_mgmt_send(ns, qpair, slba, select_all, SPDK_NVME_ZONE_FINISH,
				       cb_fn, cb_arg);
}

int
spdk_nvme_zns_open_zone(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, uint64_t slba,
			bool select_all, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_zns_zone_mgmt_send(ns, qpair, slba, select_all, SPDK_NVME_ZONE_OPEN,
				       cb_fn, cb_arg);
}

int
spdk_nvme_zns_reset_zone(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, uint64_t slba,
			 bool select_all, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_zns_zone_mgmt_send(ns, qpair, slba, select_all, SPDK_NVME_ZONE_RESET,
				       cb_fn, cb_arg);
}

int
spdk_nvme_zns_offline_zone(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, uint64_t slba,
			   bool select_all, spdk_nvme_cmd_cb cb_fn, void *cb_arg)
{
	return nvme_zns_zone_mgmt_send(ns, qpair, slba, select_all, SPDK_NVME_ZONE_OFFLINE,
				       cb_fn, cb_arg);
}
