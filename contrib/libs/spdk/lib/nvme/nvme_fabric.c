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
 * NVMe over Fabrics transport-independent functions
 */

#include "nvme_internal.h"

#include "spdk/endian.h"
#include "spdk/string.h"

static int
nvme_fabric_prop_set_cmd(struct spdk_nvme_ctrlr *ctrlr,
			 uint32_t offset, uint8_t size, uint64_t value)
{
	struct spdk_nvmf_fabric_prop_set_cmd cmd = {};
	struct nvme_completion_poll_status *status;
	int rc;

	assert(size == SPDK_NVMF_PROP_SIZE_4 || size == SPDK_NVMF_PROP_SIZE_8);

	status = calloc(1, sizeof(*status));
	if (!status) {
		SPDK_ERRLOG("Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	cmd.opcode = SPDK_NVME_OPC_FABRIC;
	cmd.fctype = SPDK_NVMF_FABRIC_COMMAND_PROPERTY_SET;
	cmd.ofst = offset;
	cmd.attrib.size = size;
	cmd.value.u64 = value;

	rc = spdk_nvme_ctrlr_cmd_admin_raw(ctrlr, (struct spdk_nvme_cmd *)&cmd,
					   NULL, 0,
					   nvme_completion_poll_cb, status);
	if (rc < 0) {
		free(status);
		return rc;
	}

	if (nvme_wait_for_completion(ctrlr->adminq, status)) {
		if (!status->timed_out) {
			free(status);
		}
		SPDK_ERRLOG("Property Set failed\n");
		return -1;
	}
	free(status);

	return 0;
}

static int
nvme_fabric_prop_get_cmd(struct spdk_nvme_ctrlr *ctrlr,
			 uint32_t offset, uint8_t size, uint64_t *value)
{
	struct spdk_nvmf_fabric_prop_set_cmd cmd = {};
	struct nvme_completion_poll_status *status;
	struct spdk_nvmf_fabric_prop_get_rsp *response;
	int rc;

	assert(size == SPDK_NVMF_PROP_SIZE_4 || size == SPDK_NVMF_PROP_SIZE_8);

	status = calloc(1, sizeof(*status));
	if (!status) {
		SPDK_ERRLOG("Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	cmd.opcode = SPDK_NVME_OPC_FABRIC;
	cmd.fctype = SPDK_NVMF_FABRIC_COMMAND_PROPERTY_GET;
	cmd.ofst = offset;
	cmd.attrib.size = size;

	rc = spdk_nvme_ctrlr_cmd_admin_raw(ctrlr, (struct spdk_nvme_cmd *)&cmd,
					   NULL, 0, nvme_completion_poll_cb,
					   status);
	if (rc < 0) {
		free(status);
		return rc;
	}

	if (nvme_wait_for_completion(ctrlr->adminq, status)) {
		if (!status->timed_out) {
			free(status);
		}
		SPDK_ERRLOG("Property Get failed\n");
		return -1;
	}

	response = (struct spdk_nvmf_fabric_prop_get_rsp *)&status->cpl;

	if (size == SPDK_NVMF_PROP_SIZE_4) {
		*value = response->value.u32.low;
	} else {
		*value = response->value.u64;
	}

	free(status);

	return 0;
}

int
nvme_fabric_ctrlr_set_reg_4(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint32_t value)
{
	return nvme_fabric_prop_set_cmd(ctrlr, offset, SPDK_NVMF_PROP_SIZE_4, value);
}

int
nvme_fabric_ctrlr_set_reg_8(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint64_t value)
{
	return nvme_fabric_prop_set_cmd(ctrlr, offset, SPDK_NVMF_PROP_SIZE_8, value);
}

int
nvme_fabric_ctrlr_get_reg_4(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint32_t *value)
{
	uint64_t tmp_value;
	int rc;
	rc = nvme_fabric_prop_get_cmd(ctrlr, offset, SPDK_NVMF_PROP_SIZE_4, &tmp_value);

	if (!rc) {
		*value = (uint32_t)tmp_value;
	}
	return rc;
}

int
nvme_fabric_ctrlr_get_reg_8(struct spdk_nvme_ctrlr *ctrlr, uint32_t offset, uint64_t *value)
{
	return nvme_fabric_prop_get_cmd(ctrlr, offset, SPDK_NVMF_PROP_SIZE_8, value);
}

static void
nvme_fabric_discover_probe(struct spdk_nvmf_discovery_log_page_entry *entry,
			   struct spdk_nvme_probe_ctx *probe_ctx,
			   int discover_priority)
{
	struct spdk_nvme_transport_id trid;
	uint8_t *end;
	size_t len;

	memset(&trid, 0, sizeof(trid));

	if (entry->subtype == SPDK_NVMF_SUBTYPE_DISCOVERY) {
		SPDK_WARNLOG("Skipping unsupported discovery service referral\n");
		return;
	} else if (entry->subtype != SPDK_NVMF_SUBTYPE_NVME) {
		SPDK_WARNLOG("Skipping unknown subtype %u\n", entry->subtype);
		return;
	}

	trid.trtype = entry->trtype;
	spdk_nvme_transport_id_populate_trstring(&trid, spdk_nvme_transport_id_trtype_str(entry->trtype));
	if (!spdk_nvme_transport_available_by_name(trid.trstring)) {
		SPDK_WARNLOG("NVMe transport type %u not available; skipping probe\n",
			     trid.trtype);
		return;
	}

	snprintf(trid.trstring, sizeof(trid.trstring), "%s", probe_ctx->trid.trstring);
	trid.adrfam = entry->adrfam;

	/* Ensure that subnqn is null terminated. */
	end = memchr(entry->subnqn, '\0', SPDK_NVMF_NQN_MAX_LEN + 1);
	if (!end) {
		SPDK_ERRLOG("Discovery entry SUBNQN is not null terminated\n");
		return;
	}
	len = end - entry->subnqn;
	memcpy(trid.subnqn, entry->subnqn, len);
	trid.subnqn[len] = '\0';

	/* Convert traddr to a null terminated string. */
	len = spdk_strlen_pad(entry->traddr, sizeof(entry->traddr), ' ');
	memcpy(trid.traddr, entry->traddr, len);
	if (spdk_str_chomp(trid.traddr) != 0) {
		SPDK_DEBUGLOG(nvme, "Trailing newlines removed from discovery TRADDR\n");
	}

	/* Convert trsvcid to a null terminated string. */
	len = spdk_strlen_pad(entry->trsvcid, sizeof(entry->trsvcid), ' ');
	memcpy(trid.trsvcid, entry->trsvcid, len);
	if (spdk_str_chomp(trid.trsvcid) != 0) {
		SPDK_DEBUGLOG(nvme, "Trailing newlines removed from discovery TRSVCID\n");
	}

	SPDK_DEBUGLOG(nvme, "subnqn=%s, trtype=%u, traddr=%s, trsvcid=%s\n",
		      trid.subnqn, trid.trtype,
		      trid.traddr, trid.trsvcid);

	/* Copy the priority from the discovery ctrlr */
	trid.priority = discover_priority;

	nvme_ctrlr_probe(&trid, probe_ctx, NULL);
}

static int
nvme_fabric_get_discovery_log_page(struct spdk_nvme_ctrlr *ctrlr,
				   void *log_page, uint32_t size, uint64_t offset)
{
	struct nvme_completion_poll_status *status;
	int rc;

	status = calloc(1, sizeof(*status));
	if (!status) {
		SPDK_ERRLOG("Failed to allocate status tracker\n");
		return -ENOMEM;
	}

	rc = spdk_nvme_ctrlr_cmd_get_log_page(ctrlr, SPDK_NVME_LOG_DISCOVERY, 0, log_page, size, offset,
					      nvme_completion_poll_cb, status);
	if (rc < 0) {
		free(status);
		return -1;
	}

	if (nvme_wait_for_completion(ctrlr->adminq, status)) {
		if (!status->timed_out) {
			free(status);
		}
		return -1;
	}
	free(status);

	return 0;
}

int
nvme_fabric_ctrlr_scan(struct spdk_nvme_probe_ctx *probe_ctx,
		       bool direct_connect)
{
	struct spdk_nvme_ctrlr_opts discovery_opts;
	struct spdk_nvme_ctrlr *discovery_ctrlr;
	union spdk_nvme_cc_register cc;
	int rc;
	struct nvme_completion_poll_status *status;

	if (strcmp(probe_ctx->trid.subnqn, SPDK_NVMF_DISCOVERY_NQN) != 0) {
		/* It is not a discovery_ctrlr info and try to directly connect it */
		rc = nvme_ctrlr_probe(&probe_ctx->trid, probe_ctx, NULL);
		return rc;
	}

	spdk_nvme_ctrlr_get_default_ctrlr_opts(&discovery_opts, sizeof(discovery_opts));
	/* For discovery_ctrlr set the timeout to 0 */
	discovery_opts.keep_alive_timeout_ms = 0;

	discovery_ctrlr = nvme_transport_ctrlr_construct(&probe_ctx->trid, &discovery_opts, NULL);
	if (discovery_ctrlr == NULL) {
		return -1;
	}
	nvme_qpair_set_state(discovery_ctrlr->adminq, NVME_QPAIR_ENABLED);

	/* TODO: this should be using the normal NVMe controller initialization process +1 */
	cc.raw = 0;
	cc.bits.en = 1;
	cc.bits.iosqes = 6; /* SQ entry size == 64 == 2^6 */
	cc.bits.iocqes = 4; /* CQ entry size == 16 == 2^4 */
	rc = nvme_transport_ctrlr_set_reg_4(discovery_ctrlr, offsetof(struct spdk_nvme_registers, cc.raw),
					    cc.raw);
	if (rc < 0) {
		SPDK_ERRLOG("Failed to set cc\n");
		nvme_ctrlr_destruct(discovery_ctrlr);
		return -1;
	}

	status = calloc(1, sizeof(*status));
	if (!status) {
		SPDK_ERRLOG("Failed to allocate status tracker\n");
		nvme_ctrlr_destruct(discovery_ctrlr);
		return -ENOMEM;
	}

	/* get the cdata info */
	rc = nvme_ctrlr_cmd_identify(discovery_ctrlr, SPDK_NVME_IDENTIFY_CTRLR, 0, 0, 0,
				     &discovery_ctrlr->cdata, sizeof(discovery_ctrlr->cdata),
				     nvme_completion_poll_cb, status);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to identify cdata\n");
		nvme_ctrlr_destruct(discovery_ctrlr);
		free(status);
		return rc;
	}

	if (nvme_wait_for_completion(discovery_ctrlr->adminq, status)) {
		SPDK_ERRLOG("nvme_identify_controller failed!\n");
		nvme_ctrlr_destruct(discovery_ctrlr);
		if (!status->timed_out) {
			free(status);
		}
		return -ENXIO;
	}

	free(status);

	/* Direct attach through spdk_nvme_connect() API */
	if (direct_connect == true) {
		/* Set the ready state to skip the normal init process */
		discovery_ctrlr->state = NVME_CTRLR_STATE_READY;
		nvme_ctrlr_connected(probe_ctx, discovery_ctrlr);
		nvme_ctrlr_add_process(discovery_ctrlr, 0);
		return 0;
	}

	rc = nvme_fabric_ctrlr_discover(discovery_ctrlr, probe_ctx);
	nvme_ctrlr_destruct(discovery_ctrlr);
	return rc;
}

int
nvme_fabric_ctrlr_discover(struct spdk_nvme_ctrlr *ctrlr,
			   struct spdk_nvme_probe_ctx *probe_ctx)
{
	struct spdk_nvmf_discovery_log_page *log_page;
	struct spdk_nvmf_discovery_log_page_entry *log_page_entry;
	char buffer[4096];
	int rc;
	uint64_t i, numrec, buffer_max_entries_first, buffer_max_entries, log_page_offset = 0;
	uint64_t remaining_num_rec = 0;
	uint16_t recfmt;

	memset(buffer, 0x0, 4096);
	buffer_max_entries_first = (sizeof(buffer) - offsetof(struct spdk_nvmf_discovery_log_page,
				    entries[0])) /
				   sizeof(struct spdk_nvmf_discovery_log_page_entry);
	buffer_max_entries = sizeof(buffer) / sizeof(struct spdk_nvmf_discovery_log_page_entry);
	do {
		rc = nvme_fabric_get_discovery_log_page(ctrlr, buffer, sizeof(buffer), log_page_offset);
		if (rc < 0) {
			SPDK_DEBUGLOG(nvme, "Get Log Page - Discovery error\n");
			return rc;
		}

		if (!remaining_num_rec) {
			log_page = (struct spdk_nvmf_discovery_log_page *)buffer;
			recfmt = from_le16(&log_page->recfmt);
			if (recfmt != 0) {
				SPDK_ERRLOG("Unrecognized discovery log record format %" PRIu16 "\n", recfmt);
				return -EPROTO;
			}
			remaining_num_rec = log_page->numrec;
			log_page_offset = offsetof(struct spdk_nvmf_discovery_log_page, entries[0]);
			log_page_entry = &log_page->entries[0];
			numrec = spdk_min(remaining_num_rec, buffer_max_entries_first);
		} else {
			numrec = spdk_min(remaining_num_rec, buffer_max_entries);
			log_page_entry = (struct spdk_nvmf_discovery_log_page_entry *)buffer;
		}

		for (i = 0; i < numrec; i++) {
			nvme_fabric_discover_probe(log_page_entry++, probe_ctx, ctrlr->trid.priority);
		}
		remaining_num_rec -= numrec;
		log_page_offset += numrec * sizeof(struct spdk_nvmf_discovery_log_page_entry);
	} while (remaining_num_rec != 0);

	return 0;
}

int
nvme_fabric_qpair_connect(struct spdk_nvme_qpair *qpair, uint32_t num_entries)
{
	struct nvme_completion_poll_status *status;
	struct spdk_nvmf_fabric_connect_rsp *rsp;
	struct spdk_nvmf_fabric_connect_cmd cmd;
	struct spdk_nvmf_fabric_connect_data *nvmf_data;
	struct spdk_nvme_ctrlr *ctrlr;
	int rc;

	if (num_entries == 0 || num_entries > SPDK_NVME_IO_QUEUE_MAX_ENTRIES) {
		return -EINVAL;
	}

	ctrlr = qpair->ctrlr;
	if (!ctrlr) {
		return -EINVAL;
	}

	nvmf_data = spdk_zmalloc(sizeof(*nvmf_data), 0, NULL,
				 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	if (!nvmf_data) {
		SPDK_ERRLOG("nvmf_data allocation error\n");
		return -ENOMEM;
	}

	status = calloc(1, sizeof(*status));
	if (!status) {
		SPDK_ERRLOG("Failed to allocate status tracker\n");
		spdk_free(nvmf_data);
		return -ENOMEM;
	}

	memset(&cmd, 0, sizeof(cmd));
	cmd.opcode = SPDK_NVME_OPC_FABRIC;
	cmd.fctype = SPDK_NVMF_FABRIC_COMMAND_CONNECT;
	cmd.qid = qpair->id;
	cmd.sqsize = num_entries - 1;
	cmd.kato = ctrlr->opts.keep_alive_timeout_ms;

	if (nvme_qpair_is_admin_queue(qpair)) {
		nvmf_data->cntlid = 0xFFFF;
	} else {
		nvmf_data->cntlid = ctrlr->cntlid;
	}

	SPDK_STATIC_ASSERT(sizeof(nvmf_data->hostid) == sizeof(ctrlr->opts.extended_host_id),
			   "host ID size mismatch");
	memcpy(nvmf_data->hostid, ctrlr->opts.extended_host_id, sizeof(nvmf_data->hostid));
	snprintf(nvmf_data->hostnqn, sizeof(nvmf_data->hostnqn), "%s", ctrlr->opts.hostnqn);
	snprintf(nvmf_data->subnqn, sizeof(nvmf_data->subnqn), "%s", ctrlr->trid.subnqn);

	rc = spdk_nvme_ctrlr_cmd_io_raw(ctrlr, qpair,
					(struct spdk_nvme_cmd *)&cmd,
					nvmf_data, sizeof(*nvmf_data),
					nvme_completion_poll_cb, status);
	if (rc < 0) {
		SPDK_ERRLOG("Failed to allocate/submit FABRIC_CONNECT command, rc %d\n", rc);
		spdk_free(nvmf_data);
		free(status);
		return rc;
	}

	/* If we time out, the qpair will abort the request upon destruction. */
	rc = nvme_wait_for_completion_timeout(qpair, status, ctrlr->opts.fabrics_connect_timeout_us);
	if (rc) {
		SPDK_ERRLOG("Connect command failed, rc %d, trtype:%s adrfam:%s traddr:%s trsvcid:%s subnqn:%s\n",
			    rc,
			    spdk_nvme_transport_id_trtype_str(ctrlr->trid.trtype),
			    spdk_nvme_transport_id_adrfam_str(ctrlr->trid.adrfam),
			    ctrlr->trid.traddr,
			    ctrlr->trid.trsvcid,
			    ctrlr->trid.subnqn);
		if (spdk_nvme_cpl_is_error(&status->cpl)) {
			SPDK_ERRLOG("Connect command completed with error: sct %d, sc %d\n", status->cpl.status.sct,
				    status->cpl.status.sc);
		}
		spdk_free(nvmf_data);
		if (!status->timed_out) {
			free(status);
		}
		return -EIO;
	}

	if (nvme_qpair_is_admin_queue(qpair)) {
		rsp = (struct spdk_nvmf_fabric_connect_rsp *)&status->cpl;
		ctrlr->cntlid = rsp->status_code_specific.success.cntlid;
		SPDK_DEBUGLOG(nvme, "CNTLID 0x%04" PRIx16 "\n", ctrlr->cntlid);
	}

	spdk_free(nvmf_data);
	free(status);
	return 0;
}
