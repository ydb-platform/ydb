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
#include "spdk/nvme_ocssd.h"

#define NVME_CMD_DPTR_STR_SIZE 256

static int nvme_qpair_resubmit_request(struct spdk_nvme_qpair *qpair, struct nvme_request *req);

struct nvme_string {
	uint16_t	value;
	const char	*str;
};

static const struct nvme_string admin_opcode[] = {
	{ SPDK_NVME_OPC_DELETE_IO_SQ, "DELETE IO SQ" },
	{ SPDK_NVME_OPC_CREATE_IO_SQ, "CREATE IO SQ" },
	{ SPDK_NVME_OPC_GET_LOG_PAGE, "GET LOG PAGE" },
	{ SPDK_NVME_OPC_DELETE_IO_CQ, "DELETE IO CQ" },
	{ SPDK_NVME_OPC_CREATE_IO_CQ, "CREATE IO CQ" },
	{ SPDK_NVME_OPC_IDENTIFY, "IDENTIFY" },
	{ SPDK_NVME_OPC_ABORT, "ABORT" },
	{ SPDK_NVME_OPC_SET_FEATURES, "SET FEATURES" },
	{ SPDK_NVME_OPC_GET_FEATURES, "GET FEATURES" },
	{ SPDK_NVME_OPC_ASYNC_EVENT_REQUEST, "ASYNC EVENT REQUEST" },
	{ SPDK_NVME_OPC_NS_MANAGEMENT, "NAMESPACE MANAGEMENT" },
	{ SPDK_NVME_OPC_FIRMWARE_COMMIT, "FIRMWARE COMMIT" },
	{ SPDK_NVME_OPC_FIRMWARE_IMAGE_DOWNLOAD, "FIRMWARE IMAGE DOWNLOAD" },
	{ SPDK_NVME_OPC_DEVICE_SELF_TEST, "DEVICE SELF-TEST" },
	{ SPDK_NVME_OPC_NS_ATTACHMENT, "NAMESPACE ATTACHMENT" },
	{ SPDK_NVME_OPC_KEEP_ALIVE, "KEEP ALIVE" },
	{ SPDK_NVME_OPC_DIRECTIVE_SEND, "DIRECTIVE SEND" },
	{ SPDK_NVME_OPC_DIRECTIVE_RECEIVE, "DIRECTIVE RECEIVE" },
	{ SPDK_NVME_OPC_VIRTUALIZATION_MANAGEMENT, "VIRTUALIZATION MANAGEMENT" },
	{ SPDK_NVME_OPC_NVME_MI_SEND, "NVME-MI SEND" },
	{ SPDK_NVME_OPC_NVME_MI_RECEIVE, "NVME-MI RECEIVE" },
	{ SPDK_NVME_OPC_DOORBELL_BUFFER_CONFIG, "DOORBELL BUFFER CONFIG" },
	{ SPDK_NVME_OPC_FABRIC, "FABRIC" },
	{ SPDK_NVME_OPC_FORMAT_NVM, "FORMAT NVM" },
	{ SPDK_NVME_OPC_SECURITY_SEND, "SECURITY SEND" },
	{ SPDK_NVME_OPC_SECURITY_RECEIVE, "SECURITY RECEIVE" },
	{ SPDK_NVME_OPC_SANITIZE, "SANITIZE" },
	{ SPDK_NVME_OPC_GET_LBA_STATUS, "GET LBA STATUS" },
	{ SPDK_OCSSD_OPC_GEOMETRY, "OCSSD / GEOMETRY" },
	{ 0xFFFF, "ADMIN COMMAND" }
};

static const struct nvme_string fabric_opcode[] = {
	{ SPDK_NVMF_FABRIC_COMMAND_PROPERTY_SET, "PROPERTY SET" },
	{ SPDK_NVMF_FABRIC_COMMAND_CONNECT, "CONNECT" },
	{ SPDK_NVMF_FABRIC_COMMAND_PROPERTY_GET, "PROPERTY GET" },
	{ SPDK_NVMF_FABRIC_COMMAND_AUTHENTICATION_SEND, "AUTHENTICATION SEND" },
	{ SPDK_NVMF_FABRIC_COMMAND_AUTHENTICATION_RECV, "AUTHENTICATION RECV" },
	{ 0xFFFF, "RESERVED / VENDOR SPECIFIC" }
};

static const struct nvme_string feat_opcode[] = {
	{ SPDK_NVME_FEAT_ARBITRATION, "ARBITRATION" },
	{ SPDK_NVME_FEAT_POWER_MANAGEMENT, "POWER MANAGEMENT" },
	{ SPDK_NVME_FEAT_LBA_RANGE_TYPE, "LBA RANGE TYPE" },
	{ SPDK_NVME_FEAT_TEMPERATURE_THRESHOLD, "TEMPERATURE THRESHOLD" },
	{ SPDK_NVME_FEAT_ERROR_RECOVERY, "ERROR_RECOVERY" },
	{ SPDK_NVME_FEAT_VOLATILE_WRITE_CACHE, "VOLATILE WRITE CACHE" },
	{ SPDK_NVME_FEAT_NUMBER_OF_QUEUES, "NUMBER OF QUEUES" },
	{ SPDK_NVME_FEAT_INTERRUPT_COALESCING, "INTERRUPT COALESCING" },
	{ SPDK_NVME_FEAT_INTERRUPT_VECTOR_CONFIGURATION, "INTERRUPT VECTOR CONFIGURATION" },
	{ SPDK_NVME_FEAT_WRITE_ATOMICITY, "WRITE ATOMICITY" },
	{ SPDK_NVME_FEAT_ASYNC_EVENT_CONFIGURATION, "ASYNC EVENT CONFIGURATION" },
	{ SPDK_NVME_FEAT_AUTONOMOUS_POWER_STATE_TRANSITION, "AUTONOMOUS POWER STATE TRANSITION" },
	{ SPDK_NVME_FEAT_HOST_MEM_BUFFER, "HOST MEM BUFFER" },
	{ SPDK_NVME_FEAT_TIMESTAMP, "TIMESTAMP" },
	{ SPDK_NVME_FEAT_KEEP_ALIVE_TIMER, "KEEP ALIVE TIMER" },
	{ SPDK_NVME_FEAT_HOST_CONTROLLED_THERMAL_MANAGEMENT, "HOST CONTROLLED THERMAL MANAGEMENT" },
	{ SPDK_NVME_FEAT_NON_OPERATIONAL_POWER_STATE_CONFIG, "NON OPERATIONAL POWER STATE CONFIG" },
	{ SPDK_NVME_FEAT_SOFTWARE_PROGRESS_MARKER, "SOFTWARE PROGRESS MARKER" },
	{ SPDK_NVME_FEAT_HOST_IDENTIFIER, "HOST IDENTIFIER" },
	{ SPDK_NVME_FEAT_HOST_RESERVE_MASK, "HOST RESERVE MASK" },
	{ SPDK_NVME_FEAT_HOST_RESERVE_PERSIST, "HOST RESERVE PERSIST" },
	{ 0xFFFF, "RESERVED" }
};

static const struct nvme_string io_opcode[] = {
	{ SPDK_NVME_OPC_FLUSH, "FLUSH" },
	{ SPDK_NVME_OPC_WRITE, "WRITE" },
	{ SPDK_NVME_OPC_READ, "READ" },
	{ SPDK_NVME_OPC_WRITE_UNCORRECTABLE, "WRITE UNCORRECTABLE" },
	{ SPDK_NVME_OPC_COMPARE, "COMPARE" },
	{ SPDK_NVME_OPC_WRITE_ZEROES, "WRITE ZEROES" },
	{ SPDK_NVME_OPC_DATASET_MANAGEMENT, "DATASET MANAGEMENT" },
	{ SPDK_NVME_OPC_RESERVATION_REGISTER, "RESERVATION REGISTER" },
	{ SPDK_NVME_OPC_RESERVATION_REPORT, "RESERVATION REPORT" },
	{ SPDK_NVME_OPC_RESERVATION_ACQUIRE, "RESERVATION ACQUIRE" },
	{ SPDK_NVME_OPC_RESERVATION_RELEASE, "RESERVATION RELEASE" },
	{ SPDK_OCSSD_OPC_VECTOR_RESET, "OCSSD / VECTOR RESET" },
	{ SPDK_OCSSD_OPC_VECTOR_WRITE, "OCSSD / VECTOR WRITE" },
	{ SPDK_OCSSD_OPC_VECTOR_READ, "OCSSD / VECTOR READ" },
	{ SPDK_OCSSD_OPC_VECTOR_COPY, "OCSSD / VECTOR COPY" },
	{ 0xFFFF, "IO COMMAND" }
};

static const struct nvme_string sgl_type[] = {
	{ SPDK_NVME_SGL_TYPE_DATA_BLOCK, "DATA BLOCK" },
	{ SPDK_NVME_SGL_TYPE_BIT_BUCKET, "BIT BUCKET" },
	{ SPDK_NVME_SGL_TYPE_SEGMENT, "SEGMENT" },
	{ SPDK_NVME_SGL_TYPE_LAST_SEGMENT, "LAST SEGMENT" },
	{ SPDK_NVME_SGL_TYPE_TRANSPORT_DATA_BLOCK, "TRANSPORT DATA BLOCK" },
	{ SPDK_NVME_SGL_TYPE_VENDOR_SPECIFIC, "VENDOR SPECIFIC" },
	{ 0xFFFF, "RESERVED" }
};

static const struct nvme_string sgl_subtype[] = {
	{ SPDK_NVME_SGL_SUBTYPE_ADDRESS, "ADDRESS" },
	{ SPDK_NVME_SGL_SUBTYPE_OFFSET, "OFFSET" },
	{ SPDK_NVME_SGL_SUBTYPE_TRANSPORT, "TRANSPORT" },
	{ SPDK_NVME_SGL_SUBTYPE_INVALIDATE_KEY, "INVALIDATE KEY" },
	{ 0xFFFF, "RESERVED" }
};

static const char *
nvme_get_string(const struct nvme_string *strings, uint16_t value)
{
	const struct nvme_string *entry;

	entry = strings;

	while (entry->value != 0xFFFF) {
		if (entry->value == value) {
			return entry->str;
		}
		entry++;
	}
	return entry->str;
}

static void
nvme_get_sgl_unkeyed(char *buf, size_t size, struct spdk_nvme_cmd *cmd)
{
	struct spdk_nvme_sgl_descriptor *sgl = &cmd->dptr.sgl1;

	snprintf(buf, size, " len:0x%x", sgl->unkeyed.length);
}

static void
nvme_get_sgl_keyed(char *buf, size_t size, struct spdk_nvme_cmd *cmd)
{
	struct spdk_nvme_sgl_descriptor *sgl = &cmd->dptr.sgl1;

	snprintf(buf, size, " len:0x%x key:0x%x", sgl->keyed.length, sgl->keyed.key);
}

static void
nvme_get_sgl(char *buf, size_t size, struct spdk_nvme_cmd *cmd)
{
	struct spdk_nvme_sgl_descriptor *sgl = &cmd->dptr.sgl1;
	int c;

	c = snprintf(buf, size, "SGL %s %s 0x%" PRIx64, nvme_get_string(sgl_type, sgl->generic.type),
		     nvme_get_string(sgl_subtype, sgl->generic.subtype), sgl->address);
	assert(c >= 0 && (size_t)c < size);

	if (sgl->generic.type == SPDK_NVME_SGL_TYPE_KEYED_DATA_BLOCK) {
		nvme_get_sgl_unkeyed(buf + c, size - c, cmd);
	}

	if (sgl->generic.type == SPDK_NVME_SGL_TYPE_DATA_BLOCK) {
		nvme_get_sgl_keyed(buf + c, size - c, cmd);
	}
}

static void
nvme_get_prp(char *buf, size_t size, struct spdk_nvme_cmd *cmd)
{
	snprintf(buf, size, "PRP1 0x%" PRIx64 " PRP2 0x%" PRIx64, cmd->dptr.prp.prp1, cmd->dptr.prp.prp2);
}

static void
nvme_get_dptr(char *buf, size_t size, struct spdk_nvme_cmd *cmd)
{
	if (spdk_nvme_opc_get_data_transfer(cmd->opc) != SPDK_NVME_DATA_NONE) {
		switch (cmd->psdt) {
		case SPDK_NVME_PSDT_PRP:
			nvme_get_prp(buf, size, cmd);
			break;
		case SPDK_NVME_PSDT_SGL_MPTR_CONTIG:
		case SPDK_NVME_PSDT_SGL_MPTR_SGL:
			nvme_get_sgl(buf, size, cmd);
			break;
		default:
			;
		}
	}
}

static void
nvme_admin_qpair_print_command(uint16_t qid, struct spdk_nvme_cmd *cmd)
{
	struct spdk_nvmf_capsule_cmd *fcmd = (void *)cmd;
	char dptr[NVME_CMD_DPTR_STR_SIZE] = {'\0'};

	assert(cmd != NULL);

	nvme_get_dptr(dptr, sizeof(dptr), cmd);

	switch ((int)cmd->opc) {
	case SPDK_NVME_OPC_SET_FEATURES:
	case SPDK_NVME_OPC_GET_FEATURES:
		SPDK_NOTICELOG("%s %s cid:%d cdw10:%08x %s\n",
			       nvme_get_string(admin_opcode, cmd->opc), nvme_get_string(feat_opcode,
					       cmd->cdw10_bits.set_features.fid), cmd->cid, cmd->cdw10, dptr);
		break;
	case SPDK_NVME_OPC_FABRIC:
		SPDK_NOTICELOG("%s %s qid:%d cid:%d %s\n",
			       nvme_get_string(admin_opcode, cmd->opc), nvme_get_string(fabric_opcode, fcmd->fctype), qid,
			       fcmd->cid, dptr);
		break;
	default:
		SPDK_NOTICELOG("%s (%02x) qid:%d cid:%d nsid:%x cdw10:%08x cdw11:%08x %s\n",
			       nvme_get_string(admin_opcode, cmd->opc), cmd->opc, qid, cmd->cid, cmd->nsid, cmd->cdw10,
			       cmd->cdw11, dptr);
	}
}

static void
nvme_io_qpair_print_command(uint16_t qid, struct spdk_nvme_cmd *cmd)
{
	char dptr[NVME_CMD_DPTR_STR_SIZE] = {'\0'};

	assert(cmd != NULL);

	nvme_get_dptr(dptr, sizeof(dptr), cmd);

	switch ((int)cmd->opc) {
	case SPDK_NVME_OPC_WRITE:
	case SPDK_NVME_OPC_READ:
	case SPDK_NVME_OPC_WRITE_UNCORRECTABLE:
	case SPDK_NVME_OPC_COMPARE:
		SPDK_NOTICELOG("%s sqid:%d cid:%d nsid:%d "
			       "lba:%llu len:%d %s\n",
			       nvme_get_string(io_opcode, cmd->opc), qid, cmd->cid, cmd->nsid,
			       ((unsigned long long)cmd->cdw11 << 32) + cmd->cdw10,
			       (cmd->cdw12 & 0xFFFF) + 1, dptr);
		break;
	case SPDK_NVME_OPC_FLUSH:
	case SPDK_NVME_OPC_DATASET_MANAGEMENT:
		SPDK_NOTICELOG("%s sqid:%d cid:%d nsid:%d\n",
			       nvme_get_string(io_opcode, cmd->opc), qid, cmd->cid, cmd->nsid);
		break;
	default:
		SPDK_NOTICELOG("%s (%02x) sqid:%d cid:%d nsid:%d\n",
			       nvme_get_string(io_opcode, cmd->opc), cmd->opc, qid, cmd->cid, cmd->nsid);
		break;
	}
}

void
spdk_nvme_print_command(uint16_t qid, struct spdk_nvme_cmd *cmd)
{
	assert(cmd != NULL);

	if (qid == 0 || cmd->opc == SPDK_NVME_OPC_FABRIC) {
		nvme_admin_qpair_print_command(qid, cmd);
	} else {
		nvme_io_qpair_print_command(qid, cmd);
	}
}

void
spdk_nvme_qpair_print_command(struct spdk_nvme_qpair *qpair, struct spdk_nvme_cmd *cmd)
{
	assert(qpair != NULL);
	assert(cmd != NULL);

	spdk_nvme_print_command(qpair->id, cmd);
}

static const struct nvme_string generic_status[] = {
	{ SPDK_NVME_SC_SUCCESS, "SUCCESS" },
	{ SPDK_NVME_SC_INVALID_OPCODE, "INVALID OPCODE" },
	{ SPDK_NVME_SC_INVALID_FIELD, "INVALID FIELD" },
	{ SPDK_NVME_SC_COMMAND_ID_CONFLICT, "COMMAND ID CONFLICT" },
	{ SPDK_NVME_SC_DATA_TRANSFER_ERROR, "DATA TRANSFER ERROR" },
	{ SPDK_NVME_SC_ABORTED_POWER_LOSS, "ABORTED - POWER LOSS" },
	{ SPDK_NVME_SC_INTERNAL_DEVICE_ERROR, "INTERNAL DEVICE ERROR" },
	{ SPDK_NVME_SC_ABORTED_BY_REQUEST, "ABORTED - BY REQUEST" },
	{ SPDK_NVME_SC_ABORTED_SQ_DELETION, "ABORTED - SQ DELETION" },
	{ SPDK_NVME_SC_ABORTED_FAILED_FUSED, "ABORTED - FAILED FUSED" },
	{ SPDK_NVME_SC_ABORTED_MISSING_FUSED, "ABORTED - MISSING FUSED" },
	{ SPDK_NVME_SC_INVALID_NAMESPACE_OR_FORMAT, "INVALID NAMESPACE OR FORMAT" },
	{ SPDK_NVME_SC_COMMAND_SEQUENCE_ERROR, "COMMAND SEQUENCE ERROR" },
	{ SPDK_NVME_SC_INVALID_SGL_SEG_DESCRIPTOR, "INVALID SGL SEGMENT DESCRIPTOR" },
	{ SPDK_NVME_SC_INVALID_NUM_SGL_DESCIRPTORS, "INVALID NUMBER OF SGL DESCRIPTORS" },
	{ SPDK_NVME_SC_DATA_SGL_LENGTH_INVALID, "DATA SGL LENGTH INVALID" },
	{ SPDK_NVME_SC_METADATA_SGL_LENGTH_INVALID, "METADATA SGL LENGTH INVALID" },
	{ SPDK_NVME_SC_SGL_DESCRIPTOR_TYPE_INVALID, "SGL DESCRIPTOR TYPE INVALID" },
	{ SPDK_NVME_SC_INVALID_CONTROLLER_MEM_BUF, "INVALID CONTROLLER MEMORY BUFFER" },
	{ SPDK_NVME_SC_INVALID_PRP_OFFSET, "INVALID PRP OFFSET" },
	{ SPDK_NVME_SC_ATOMIC_WRITE_UNIT_EXCEEDED, "ATOMIC WRITE UNIT EXCEEDED" },
	{ SPDK_NVME_SC_OPERATION_DENIED, "OPERATION DENIED" },
	{ SPDK_NVME_SC_INVALID_SGL_OFFSET, "INVALID SGL OFFSET" },
	{ SPDK_NVME_SC_HOSTID_INCONSISTENT_FORMAT, "HOSTID INCONSISTENT FORMAT" },
	{ SPDK_NVME_SC_KEEP_ALIVE_EXPIRED, "KEEP ALIVE EXPIRED" },
	{ SPDK_NVME_SC_KEEP_ALIVE_INVALID, "KEEP ALIVE INVALID" },
	{ SPDK_NVME_SC_ABORTED_PREEMPT, "ABORTED - PREEMPT AND ABORT" },
	{ SPDK_NVME_SC_SANITIZE_FAILED, "SANITIZE FAILED" },
	{ SPDK_NVME_SC_SANITIZE_IN_PROGRESS, "SANITIZE IN PROGRESS" },
	{ SPDK_NVME_SC_SGL_DATA_BLOCK_GRANULARITY_INVALID, "DATA BLOCK GRANULARITY INVALID" },
	{ SPDK_NVME_SC_COMMAND_INVALID_IN_CMB, "COMMAND NOT SUPPORTED FOR QUEUE IN CMB" },
	{ SPDK_NVME_SC_LBA_OUT_OF_RANGE, "LBA OUT OF RANGE" },
	{ SPDK_NVME_SC_CAPACITY_EXCEEDED, "CAPACITY EXCEEDED" },
	{ SPDK_NVME_SC_NAMESPACE_NOT_READY, "NAMESPACE NOT READY" },
	{ SPDK_NVME_SC_RESERVATION_CONFLICT, "RESERVATION CONFLICT" },
	{ SPDK_NVME_SC_FORMAT_IN_PROGRESS, "FORMAT IN PROGRESS" },
	{ 0xFFFF, "GENERIC" }
};

static const struct nvme_string command_specific_status[] = {
	{ SPDK_NVME_SC_COMPLETION_QUEUE_INVALID, "INVALID COMPLETION QUEUE" },
	{ SPDK_NVME_SC_INVALID_QUEUE_IDENTIFIER, "INVALID QUEUE IDENTIFIER" },
	{ SPDK_NVME_SC_INVALID_QUEUE_SIZE, "INVALID QUEUE SIZE" },
	{ SPDK_NVME_SC_ABORT_COMMAND_LIMIT_EXCEEDED, "ABORT CMD LIMIT EXCEEDED" },
	{ SPDK_NVME_SC_ASYNC_EVENT_REQUEST_LIMIT_EXCEEDED, "ASYNC LIMIT EXCEEDED" },
	{ SPDK_NVME_SC_INVALID_FIRMWARE_SLOT, "INVALID FIRMWARE SLOT" },
	{ SPDK_NVME_SC_INVALID_FIRMWARE_IMAGE, "INVALID FIRMWARE IMAGE" },
	{ SPDK_NVME_SC_INVALID_INTERRUPT_VECTOR, "INVALID INTERRUPT VECTOR" },
	{ SPDK_NVME_SC_INVALID_LOG_PAGE, "INVALID LOG PAGE" },
	{ SPDK_NVME_SC_INVALID_FORMAT, "INVALID FORMAT" },
	{ SPDK_NVME_SC_FIRMWARE_REQ_CONVENTIONAL_RESET, "FIRMWARE REQUIRES CONVENTIONAL RESET" },
	{ SPDK_NVME_SC_INVALID_QUEUE_DELETION, "INVALID QUEUE DELETION" },
	{ SPDK_NVME_SC_FEATURE_ID_NOT_SAVEABLE, "FEATURE ID NOT SAVEABLE" },
	{ SPDK_NVME_SC_FEATURE_NOT_CHANGEABLE, "FEATURE NOT CHANGEABLE" },
	{ SPDK_NVME_SC_FEATURE_NOT_NAMESPACE_SPECIFIC, "FEATURE NOT NAMESPACE SPECIFIC" },
	{ SPDK_NVME_SC_FIRMWARE_REQ_NVM_RESET, "FIRMWARE REQUIRES NVM RESET" },
	{ SPDK_NVME_SC_FIRMWARE_REQ_RESET, "FIRMWARE REQUIRES RESET" },
	{ SPDK_NVME_SC_FIRMWARE_REQ_MAX_TIME_VIOLATION, "FIRMWARE REQUIRES MAX TIME VIOLATION" },
	{ SPDK_NVME_SC_FIRMWARE_ACTIVATION_PROHIBITED, "FIRMWARE ACTIVATION PROHIBITED" },
	{ SPDK_NVME_SC_OVERLAPPING_RANGE, "OVERLAPPING RANGE" },
	{ SPDK_NVME_SC_NAMESPACE_INSUFFICIENT_CAPACITY, "NAMESPACE INSUFFICIENT CAPACITY" },
	{ SPDK_NVME_SC_NAMESPACE_ID_UNAVAILABLE, "NAMESPACE ID UNAVAILABLE" },
	{ SPDK_NVME_SC_NAMESPACE_ALREADY_ATTACHED, "NAMESPACE ALREADY ATTACHED" },
	{ SPDK_NVME_SC_NAMESPACE_IS_PRIVATE, "NAMESPACE IS PRIVATE" },
	{ SPDK_NVME_SC_NAMESPACE_NOT_ATTACHED, "NAMESPACE NOT ATTACHED" },
	{ SPDK_NVME_SC_THINPROVISIONING_NOT_SUPPORTED, "THINPROVISIONING NOT SUPPORTED" },
	{ SPDK_NVME_SC_CONTROLLER_LIST_INVALID, "CONTROLLER LIST INVALID" },
	{ SPDK_NVME_SC_DEVICE_SELF_TEST_IN_PROGRESS, "DEVICE SELF-TEST IN PROGRESS" },
	{ SPDK_NVME_SC_BOOT_PARTITION_WRITE_PROHIBITED, "BOOT PARTITION WRITE PROHIBITED" },
	{ SPDK_NVME_SC_INVALID_CTRLR_ID, "INVALID CONTROLLER ID" },
	{ SPDK_NVME_SC_INVALID_SECONDARY_CTRLR_STATE, "INVALID SECONDARY CONTROLLER STATE" },
	{ SPDK_NVME_SC_INVALID_NUM_CTRLR_RESOURCES, "INVALID NUMBER OF CONTROLLER RESOURCES" },
	{ SPDK_NVME_SC_INVALID_RESOURCE_ID, "INVALID RESOURCE IDENTIFIER" },
	{ SPDK_NVME_SC_STREAM_RESOURCE_ALLOCATION_FAILED, "STREAM RESOURCE ALLOCATION FAILED"},
	{ SPDK_NVME_SC_CONFLICTING_ATTRIBUTES, "CONFLICTING ATTRIBUTES" },
	{ SPDK_NVME_SC_INVALID_PROTECTION_INFO, "INVALID PROTECTION INFO" },
	{ SPDK_NVME_SC_ATTEMPTED_WRITE_TO_RO_RANGE, "WRITE TO RO RANGE" },
	{ 0xFFFF, "COMMAND SPECIFIC" }
};

static const struct nvme_string media_error_status[] = {
	{ SPDK_NVME_SC_WRITE_FAULTS, "WRITE FAULTS" },
	{ SPDK_NVME_SC_UNRECOVERED_READ_ERROR, "UNRECOVERED READ ERROR" },
	{ SPDK_NVME_SC_GUARD_CHECK_ERROR, "GUARD CHECK ERROR" },
	{ SPDK_NVME_SC_APPLICATION_TAG_CHECK_ERROR, "APPLICATION TAG CHECK ERROR" },
	{ SPDK_NVME_SC_REFERENCE_TAG_CHECK_ERROR, "REFERENCE TAG CHECK ERROR" },
	{ SPDK_NVME_SC_COMPARE_FAILURE, "COMPARE FAILURE" },
	{ SPDK_NVME_SC_ACCESS_DENIED, "ACCESS DENIED" },
	{ SPDK_NVME_SC_DEALLOCATED_OR_UNWRITTEN_BLOCK, "DEALLOCATED OR UNWRITTEN BLOCK" },
	{ SPDK_OCSSD_SC_OFFLINE_CHUNK, "RESET OFFLINE CHUNK" },
	{ SPDK_OCSSD_SC_INVALID_RESET, "INVALID RESET" },
	{ SPDK_OCSSD_SC_WRITE_FAIL_WRITE_NEXT_UNIT, "WRITE FAIL WRITE NEXT UNIT" },
	{ SPDK_OCSSD_SC_WRITE_FAIL_CHUNK_EARLY_CLOSE, "WRITE FAIL CHUNK EARLY CLOSE" },
	{ SPDK_OCSSD_SC_OUT_OF_ORDER_WRITE, "OUT OF ORDER WRITE" },
	{ SPDK_OCSSD_SC_READ_HIGH_ECC, "READ HIGH ECC" },
	{ 0xFFFF, "MEDIA ERROR" }
};

static const struct nvme_string path_status[] = {
	{ SPDK_NVME_SC_INTERNAL_PATH_ERROR, "INTERNAL PATH ERROR" },
	{ SPDK_NVME_SC_CONTROLLER_PATH_ERROR, "CONTROLLER PATH ERROR" },
	{ SPDK_NVME_SC_HOST_PATH_ERROR, "HOST PATH ERROR" },
	{ SPDK_NVME_SC_ABORTED_BY_HOST, "ABORTED BY HOST" },
	{ 0xFFFF, "PATH ERROR" }
};

const char *
spdk_nvme_cpl_get_status_string(const struct spdk_nvme_status *status)
{
	const struct nvme_string *entry;

	switch (status->sct) {
	case SPDK_NVME_SCT_GENERIC:
		entry = generic_status;
		break;
	case SPDK_NVME_SCT_COMMAND_SPECIFIC:
		entry = command_specific_status;
		break;
	case SPDK_NVME_SCT_MEDIA_ERROR:
		entry = media_error_status;
		break;
	case SPDK_NVME_SCT_PATH:
		entry = path_status;
		break;
	case SPDK_NVME_SCT_VENDOR_SPECIFIC:
		return "VENDOR SPECIFIC";
	default:
		return "RESERVED";
	}

	return nvme_get_string(entry, status->sc);
}

void
spdk_nvme_print_completion(uint16_t qid, struct spdk_nvme_cpl *cpl)
{
	assert(cpl != NULL);

	/* Check that sqid matches qid. Note that sqid is reserved
	 * for fabrics so don't print an error when sqid is 0. */
	if (cpl->sqid != qid && cpl->sqid != 0) {
		SPDK_ERRLOG("sqid %u doesn't match qid\n", cpl->sqid);
	}

	SPDK_NOTICELOG("%s (%02x/%02x) qid:%d cid:%d cdw0:%x sqhd:%04x p:%x m:%x dnr:%x\n",
		       spdk_nvme_cpl_get_status_string(&cpl->status),
		       cpl->status.sct, cpl->status.sc, qid, cpl->cid, cpl->cdw0,
		       cpl->sqhd, cpl->status.p, cpl->status.m, cpl->status.dnr);
}

void
spdk_nvme_qpair_print_completion(struct spdk_nvme_qpair *qpair, struct spdk_nvme_cpl *cpl)
{
	spdk_nvme_print_completion(qpair->id, cpl);
}

bool
nvme_completion_is_retry(const struct spdk_nvme_cpl *cpl)
{
	/*
	 * TODO: spec is not clear how commands that are aborted due
	 *  to TLER will be marked.  So for now, it seems
	 *  NAMESPACE_NOT_READY is the only case where we should
	 *  look at the DNR bit.
	 */
	switch ((int)cpl->status.sct) {
	case SPDK_NVME_SCT_GENERIC:
		switch ((int)cpl->status.sc) {
		case SPDK_NVME_SC_NAMESPACE_NOT_READY:
		case SPDK_NVME_SC_FORMAT_IN_PROGRESS:
			if (cpl->status.dnr) {
				return false;
			} else {
				return true;
			}
		case SPDK_NVME_SC_INVALID_OPCODE:
		case SPDK_NVME_SC_INVALID_FIELD:
		case SPDK_NVME_SC_COMMAND_ID_CONFLICT:
		case SPDK_NVME_SC_DATA_TRANSFER_ERROR:
		case SPDK_NVME_SC_ABORTED_POWER_LOSS:
		case SPDK_NVME_SC_INTERNAL_DEVICE_ERROR:
		case SPDK_NVME_SC_ABORTED_BY_REQUEST:
		case SPDK_NVME_SC_ABORTED_SQ_DELETION:
		case SPDK_NVME_SC_ABORTED_FAILED_FUSED:
		case SPDK_NVME_SC_ABORTED_MISSING_FUSED:
		case SPDK_NVME_SC_INVALID_NAMESPACE_OR_FORMAT:
		case SPDK_NVME_SC_COMMAND_SEQUENCE_ERROR:
		case SPDK_NVME_SC_LBA_OUT_OF_RANGE:
		case SPDK_NVME_SC_CAPACITY_EXCEEDED:
		default:
			return false;
		}
	case SPDK_NVME_SCT_PATH:
		/*
		 * Per NVMe TP 4028 (Path and Transport Error Enhancements), retries should be
		 * based on the setting of the DNR bit for Internal Path Error
		 */
		switch ((int)cpl->status.sc) {
		case SPDK_NVME_SC_INTERNAL_PATH_ERROR:
			return !cpl->status.dnr;
		default:
			return false;
		}
	case SPDK_NVME_SCT_COMMAND_SPECIFIC:
	case SPDK_NVME_SCT_MEDIA_ERROR:
	case SPDK_NVME_SCT_VENDOR_SPECIFIC:
	default:
		return false;
	}
}

static void
nvme_qpair_manual_complete_request(struct spdk_nvme_qpair *qpair,
				   struct nvme_request *req, uint32_t sct, uint32_t sc,
				   uint32_t dnr, bool print_on_error)
{
	struct spdk_nvme_cpl	cpl;
	bool			error;

	memset(&cpl, 0, sizeof(cpl));
	cpl.sqid = qpair->id;
	cpl.status.sct = sct;
	cpl.status.sc = sc;
	cpl.status.dnr = dnr;

	error = spdk_nvme_cpl_is_error(&cpl);

	if (error && print_on_error && !qpair->ctrlr->opts.disable_error_logging) {
		SPDK_NOTICELOG("Command completed manually:\n");
		spdk_nvme_qpair_print_command(qpair, &req->cmd);
		spdk_nvme_qpair_print_completion(qpair, &cpl);
	}

	nvme_complete_request(req->cb_fn, req->cb_arg, qpair, req, &cpl);
	nvme_free_request(req);
}

static void
_nvme_qpair_abort_queued_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr)
{
	struct nvme_request		*req;
	STAILQ_HEAD(, nvme_request)	tmp;

	STAILQ_INIT(&tmp);
	STAILQ_SWAP(&tmp, &qpair->queued_req, nvme_request);

	while (!STAILQ_EMPTY(&tmp)) {
		req = STAILQ_FIRST(&tmp);
		STAILQ_REMOVE_HEAD(&tmp, stailq);
		if (!qpair->ctrlr->opts.disable_error_logging) {
			SPDK_ERRLOG("aborting queued i/o\n");
		}
		nvme_qpair_manual_complete_request(qpair, req, SPDK_NVME_SCT_GENERIC,
						   SPDK_NVME_SC_ABORTED_BY_REQUEST, dnr, true);
	}
}

/* The callback to a request may submit the next request which is queued and
 * then the same callback may abort it immediately. This repetition may cause
 * infinite recursive calls. Hence move aborting requests to another list here
 * and abort them later at resubmission.
 */
static void
_nvme_qpair_complete_abort_queued_reqs(struct spdk_nvme_qpair *qpair)
{
	struct nvme_request		*req;

	while (!STAILQ_EMPTY(&qpair->aborting_queued_req)) {
		req = STAILQ_FIRST(&qpair->aborting_queued_req);
		STAILQ_REMOVE_HEAD(&qpair->aborting_queued_req, stailq);
		nvme_qpair_manual_complete_request(qpair, req, SPDK_NVME_SCT_GENERIC,
						   SPDK_NVME_SC_ABORTED_BY_REQUEST, 1, true);
	}
}

uint32_t
nvme_qpair_abort_queued_reqs(struct spdk_nvme_qpair *qpair, void *cmd_cb_arg)
{
	struct nvme_request	*req, *tmp;
	uint32_t		aborting = 0;

	STAILQ_FOREACH_SAFE(req, &qpair->queued_req, stailq, tmp) {
		if (req->cb_arg == cmd_cb_arg) {
			STAILQ_REMOVE(&qpair->queued_req, req, nvme_request, stailq);
			STAILQ_INSERT_TAIL(&qpair->aborting_queued_req, req, stailq);
			if (!qpair->ctrlr->opts.disable_error_logging) {
				SPDK_ERRLOG("aborting queued i/o\n");
			}
			aborting++;
		}
	}

	return aborting;
}

static inline bool
nvme_qpair_check_enabled(struct spdk_nvme_qpair *qpair)
{
	struct nvme_request *req;

	/*
	 * Either during initial connect or reset, the qpair should follow the given state machine.
	 * QPAIR_DISABLED->QPAIR_CONNECTING->QPAIR_CONNECTED->QPAIR_ENABLING->QPAIR_ENABLED. In the
	 * reset case, once the qpair is properly connected, we need to abort any outstanding requests
	 * from the old transport connection and encourage the application to retry them. We also need
	 * to submit any queued requests that built up while we were in the connected or enabling state.
	 */
	if (nvme_qpair_get_state(qpair) == NVME_QPAIR_CONNECTED && !qpair->ctrlr->is_resetting) {
		nvme_qpair_set_state(qpair, NVME_QPAIR_ENABLING);
		/*
		 * PCIe is special, for fabrics transports, we can abort requests before disconnect during reset
		 * but we have historically not disconnected pcie qpairs during reset so we have to abort requests
		 * here.
		 */
		if (qpair->ctrlr->trid.trtype == SPDK_NVME_TRANSPORT_PCIE) {
			nvme_qpair_abort_reqs(qpair, 0);
		}
		nvme_qpair_set_state(qpair, NVME_QPAIR_ENABLED);
		while (!STAILQ_EMPTY(&qpair->queued_req)) {
			req = STAILQ_FIRST(&qpair->queued_req);
			STAILQ_REMOVE_HEAD(&qpair->queued_req, stailq);
			if (nvme_qpair_resubmit_request(qpair, req)) {
				break;
			}
		}
	}

	/*
	 * When doing a reset, we must disconnect the qpair on the proper core.
	 * Note, reset is the only case where we set the failure reason without
	 * setting the qpair state since reset is done at the generic layer on the
	 * controller thread and we can't disconnect I/O qpairs from the controller
	 * thread.
	 */
	if (qpair->transport_failure_reason != SPDK_NVME_QPAIR_FAILURE_NONE &&
	    nvme_qpair_get_state(qpair) == NVME_QPAIR_ENABLED) {
		/* Don't disconnect PCIe qpairs. They are a special case for reset. */
		if (qpair->ctrlr->trid.trtype != SPDK_NVME_TRANSPORT_PCIE) {
			nvme_ctrlr_disconnect_qpair(qpair);
		}
		return false;
	}

	return nvme_qpair_get_state(qpair) == NVME_QPAIR_ENABLED;
}

void
nvme_qpair_resubmit_requests(struct spdk_nvme_qpair *qpair, uint32_t num_requests)
{
	uint32_t i;
	int resubmit_rc;
	struct nvme_request *req;

	for (i = 0; i < num_requests; i++) {
		if (qpair->ctrlr->is_resetting) {
			break;
		}
		if ((req = STAILQ_FIRST(&qpair->queued_req)) == NULL) {
			break;
		}
		STAILQ_REMOVE_HEAD(&qpair->queued_req, stailq);
		resubmit_rc = nvme_qpair_resubmit_request(qpair, req);
		if (spdk_unlikely(resubmit_rc != 0)) {
			SPDK_DEBUGLOG(nvme, "Unable to resubmit as many requests as we completed.\n");
			break;
		}
	}

	_nvme_qpair_complete_abort_queued_reqs(qpair);
}

int32_t
spdk_nvme_qpair_process_completions(struct spdk_nvme_qpair *qpair, uint32_t max_completions)
{
	int32_t ret;
	struct nvme_request *req, *tmp;

	if (spdk_unlikely(qpair->ctrlr->is_failed)) {
		if (qpair->ctrlr->is_removed) {
			nvme_qpair_set_state(qpair, NVME_QPAIR_DESTROYING);
			nvme_qpair_abort_reqs(qpair, 1 /* Do not retry */);
		}
		return -ENXIO;
	}

	if (spdk_unlikely(!nvme_qpair_check_enabled(qpair) &&
			  !(nvme_qpair_get_state(qpair) == NVME_QPAIR_CONNECTING))) {
		/*
		 * qpair is not enabled, likely because a controller reset is
		 *  in progress.
		 */
		return -ENXIO;
	}

	/* error injection for those queued error requests */
	if (spdk_unlikely(!STAILQ_EMPTY(&qpair->err_req_head))) {
		STAILQ_FOREACH_SAFE(req, &qpair->err_req_head, stailq, tmp) {
			if (spdk_get_ticks() - req->submit_tick > req->timeout_tsc) {
				STAILQ_REMOVE(&qpair->err_req_head, req, nvme_request, stailq);
				nvme_qpair_manual_complete_request(qpair, req,
								   req->cpl.status.sct,
								   req->cpl.status.sc, 0, true);
			}
		}
	}

	qpair->in_completion_context = 1;
	ret = nvme_transport_qpair_process_completions(qpair, max_completions);
	if (ret < 0) {
		SPDK_ERRLOG("CQ transport error %d on qpair id %hu\n", ret, qpair->id);
		if (nvme_qpair_is_admin_queue(qpair)) {
			nvme_ctrlr_fail(qpair->ctrlr, false);
		}
	}
	qpair->in_completion_context = 0;
	if (qpair->delete_after_completion_context) {
		/*
		 * A request to delete this qpair was made in the context of this completion
		 *  routine - so it is safe to delete it now.
		 */
		spdk_nvme_ctrlr_free_io_qpair(qpair);
		return ret;
	}

	/*
	 * At this point, ret must represent the number of completions we reaped.
	 * submit as many queued requests as we completed.
	 */
	nvme_qpair_resubmit_requests(qpair, ret);

	return ret;
}

spdk_nvme_qp_failure_reason
spdk_nvme_qpair_get_failure_reason(struct spdk_nvme_qpair *qpair)
{
	return qpair->transport_failure_reason;
}

int
nvme_qpair_init(struct spdk_nvme_qpair *qpair, uint16_t id,
		struct spdk_nvme_ctrlr *ctrlr,
		enum spdk_nvme_qprio qprio,
		uint32_t num_requests)
{
	size_t req_size_padded;
	uint32_t i;

	qpair->id = id;
	qpair->qprio = qprio;

	qpair->in_completion_context = 0;
	qpair->delete_after_completion_context = 0;
	qpair->no_deletion_notification_needed = 0;

	qpair->ctrlr = ctrlr;
	qpair->trtype = ctrlr->trid.trtype;

	STAILQ_INIT(&qpair->free_req);
	STAILQ_INIT(&qpair->queued_req);
	STAILQ_INIT(&qpair->aborting_queued_req);
	TAILQ_INIT(&qpair->err_cmd_head);
	STAILQ_INIT(&qpair->err_req_head);

	req_size_padded = (sizeof(struct nvme_request) + 63) & ~(size_t)63;

	qpair->req_buf = spdk_zmalloc(req_size_padded * num_requests, 64, NULL,
				      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_SHARE);
	if (qpair->req_buf == NULL) {
		SPDK_ERRLOG("no memory to allocate qpair(cntlid:0x%x sqid:%d) req_buf with %d request\n",
			    ctrlr->cntlid, qpair->id, num_requests);
		return -ENOMEM;
	}

	for (i = 0; i < num_requests; i++) {
		struct nvme_request *req = qpair->req_buf + i * req_size_padded;

		req->qpair = qpair;
		STAILQ_INSERT_HEAD(&qpair->free_req, req, stailq);
	}

	return 0;
}

void
nvme_qpair_complete_error_reqs(struct spdk_nvme_qpair *qpair)
{
	struct nvme_request		*req;

	while (!STAILQ_EMPTY(&qpair->err_req_head)) {
		req = STAILQ_FIRST(&qpair->err_req_head);
		STAILQ_REMOVE_HEAD(&qpair->err_req_head, stailq);
		nvme_qpair_manual_complete_request(qpair, req,
						   req->cpl.status.sct,
						   req->cpl.status.sc, 0, true);
	}
}

void
nvme_qpair_deinit(struct spdk_nvme_qpair *qpair)
{
	struct nvme_error_cmd *cmd, *entry;

	_nvme_qpair_abort_queued_reqs(qpair, 1);
	_nvme_qpair_complete_abort_queued_reqs(qpair);
	nvme_qpair_complete_error_reqs(qpair);

	TAILQ_FOREACH_SAFE(cmd, &qpair->err_cmd_head, link, entry) {
		TAILQ_REMOVE(&qpair->err_cmd_head, cmd, link);
		spdk_free(cmd);
	}

	spdk_free(qpair->req_buf);
}

static inline int
_nvme_qpair_submit_request(struct spdk_nvme_qpair *qpair, struct nvme_request *req)
{
	int			rc = 0;
	struct nvme_request	*child_req, *tmp;
	struct nvme_error_cmd	*cmd;
	struct spdk_nvme_ctrlr	*ctrlr = qpair->ctrlr;
	bool			child_req_failed = false;

	nvme_qpair_check_enabled(qpair);

	if (spdk_unlikely(nvme_qpair_get_state(qpair) == NVME_QPAIR_DISCONNECTED ||
			  nvme_qpair_get_state(qpair) == NVME_QPAIR_DISCONNECTING ||
			  nvme_qpair_get_state(qpair) == NVME_QPAIR_DESTROYING)) {
		TAILQ_FOREACH_SAFE(child_req, &req->children, child_tailq, tmp) {
			nvme_request_remove_child(req, child_req);
			nvme_request_free_children(child_req);
			nvme_free_request(child_req);
		}
		if (req->parent != NULL) {
			nvme_request_remove_child(req->parent, req);
		}
		nvme_free_request(req);
		return -ENXIO;
	}

	if (req->num_children) {
		/*
		 * This is a split (parent) request. Submit all of the children but not the parent
		 * request itself, since the parent is the original unsplit request.
		 */
		TAILQ_FOREACH_SAFE(child_req, &req->children, child_tailq, tmp) {
			if (spdk_likely(!child_req_failed)) {
				rc = nvme_qpair_submit_request(qpair, child_req);
				if (spdk_unlikely(rc != 0)) {
					child_req_failed = true;
				}
			} else { /* free remaining child_reqs since one child_req fails */
				nvme_request_remove_child(req, child_req);
				nvme_request_free_children(child_req);
				nvme_free_request(child_req);
			}
		}

		if (spdk_unlikely(child_req_failed)) {
			/* part of children requests have been submitted,
			 * return success since we must wait for those children to complete,
			 * but set the parent request to failure.
			 */
			if (req->num_children) {
				req->cpl.status.sct = SPDK_NVME_SCT_GENERIC;
				req->cpl.status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
				return 0;
			}
			goto error;
		}

		return rc;
	}

	/* queue those requests which matches with opcode in err_cmd list */
	if (spdk_unlikely(!TAILQ_EMPTY(&qpair->err_cmd_head))) {
		TAILQ_FOREACH(cmd, &qpair->err_cmd_head, link) {
			if (!cmd->do_not_submit) {
				continue;
			}

			if ((cmd->opc == req->cmd.opc) && cmd->err_count) {
				/* add to error request list and set cpl */
				req->timeout_tsc = cmd->timeout_tsc;
				req->submit_tick = spdk_get_ticks();
				req->cpl.status.sct = cmd->status.sct;
				req->cpl.status.sc = cmd->status.sc;
				STAILQ_INSERT_TAIL(&qpair->err_req_head, req, stailq);
				cmd->err_count--;
				return 0;
			}
		}
	}

	if (spdk_unlikely(ctrlr->is_failed)) {
		rc = -ENXIO;
		goto error;
	}

	/* assign submit_tick before submitting req to specific transport */
	if (spdk_unlikely(ctrlr->timeout_enabled)) {
		if (req->submit_tick == 0) { /* req submitted for the first time */
			req->submit_tick = spdk_get_ticks();
			req->timed_out = false;
		}
	} else {
		req->submit_tick = 0;
	}

	/* Allow two cases:
	 * 1. NVMe qpair is enabled.
	 * 2. Always allow fabrics commands through - these get
	 * the controller out of reset state.
	 */
	if (spdk_likely(nvme_qpair_get_state(qpair) == NVME_QPAIR_ENABLED) ||
	    (req->cmd.opc == SPDK_NVME_OPC_FABRIC &&
	     nvme_qpair_get_state(qpair) == NVME_QPAIR_CONNECTING)) {
		rc = nvme_transport_qpair_submit_request(qpair, req);
	} else {
		/* The controller is being reset - queue this request and
		 *  submit it later when the reset is completed.
		 */
		return -EAGAIN;
	}

	if (spdk_likely(rc == 0)) {
		req->queued = false;
		return 0;
	}

	if (rc == -EAGAIN) {
		return -EAGAIN;
	}

error:
	if (req->parent != NULL) {
		nvme_request_remove_child(req->parent, req);
	}

	/* The request is from queued_req list we should trigger the callback from caller */
	if (spdk_unlikely(req->queued)) {
		nvme_qpair_manual_complete_request(qpair, req, SPDK_NVME_SCT_GENERIC,
						   SPDK_NVME_SC_INTERNAL_DEVICE_ERROR, true, true);
		return rc;
	}

	nvme_free_request(req);

	return rc;
}

int
nvme_qpair_submit_request(struct spdk_nvme_qpair *qpair, struct nvme_request *req)
{
	int rc;

	if (spdk_unlikely(!STAILQ_EMPTY(&qpair->queued_req) && req->num_children == 0)) {
		/*
		 * requests that have no children should be sent to the transport after all
		 * currently queued requests. Requests with chilren will be split and go back
		 * through this path.
		 */
		STAILQ_INSERT_TAIL(&qpair->queued_req, req, stailq);
		req->queued = true;
		return 0;
	}

	rc = _nvme_qpair_submit_request(qpair, req);
	if (rc == -EAGAIN) {
		STAILQ_INSERT_TAIL(&qpair->queued_req, req, stailq);
		req->queued = true;
		rc = 0;
	}

	return rc;
}

static int
nvme_qpair_resubmit_request(struct spdk_nvme_qpair *qpair, struct nvme_request *req)
{
	int rc;

	/*
	 * We should never have a request with children on the queue.
	 * This is necessary to preserve the 1:1 relationship between
	 * completions and resubmissions.
	 */
	assert(req->num_children == 0);
	assert(req->queued);
	rc = _nvme_qpair_submit_request(qpair, req);
	if (spdk_unlikely(rc == -EAGAIN)) {
		STAILQ_INSERT_HEAD(&qpair->queued_req, req, stailq);
	}

	return rc;
}

void
nvme_qpair_abort_reqs(struct spdk_nvme_qpair *qpair, uint32_t dnr)
{
	nvme_qpair_complete_error_reqs(qpair);
	_nvme_qpair_abort_queued_reqs(qpair, dnr);
	_nvme_qpair_complete_abort_queued_reqs(qpair);
	nvme_transport_qpair_abort_reqs(qpair, dnr);
}

int
spdk_nvme_qpair_add_cmd_error_injection(struct spdk_nvme_ctrlr *ctrlr,
					struct spdk_nvme_qpair *qpair,
					uint8_t opc, bool do_not_submit,
					uint64_t timeout_in_us,
					uint32_t err_count,
					uint8_t sct, uint8_t sc)
{
	struct nvme_error_cmd *entry, *cmd = NULL;

	if (qpair == NULL) {
		qpair = ctrlr->adminq;
	}

	TAILQ_FOREACH(entry, &qpair->err_cmd_head, link) {
		if (entry->opc == opc) {
			cmd = entry;
			break;
		}
	}

	if (cmd == NULL) {
		cmd = spdk_zmalloc(sizeof(*cmd), 64, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
		if (!cmd) {
			return -ENOMEM;
		}
		TAILQ_INSERT_TAIL(&qpair->err_cmd_head, cmd, link);
	}

	cmd->do_not_submit = do_not_submit;
	cmd->err_count = err_count;
	cmd->timeout_tsc = timeout_in_us * spdk_get_ticks_hz() / 1000000ULL;
	cmd->opc = opc;
	cmd->status.sct = sct;
	cmd->status.sc = sc;

	return 0;
}

void
spdk_nvme_qpair_remove_cmd_error_injection(struct spdk_nvme_ctrlr *ctrlr,
		struct spdk_nvme_qpair *qpair,
		uint8_t opc)
{
	struct nvme_error_cmd *cmd, *entry;

	if (qpair == NULL) {
		qpair = ctrlr->adminq;
	}

	TAILQ_FOREACH_SAFE(cmd, &qpair->err_cmd_head, link, entry) {
		if (cmd->opc == opc) {
			TAILQ_REMOVE(&qpair->err_cmd_head, cmd, link);
			spdk_free(cmd);
			return;
		}
	}

	return;
}

uint16_t
spdk_nvme_qpair_get_id(struct spdk_nvme_qpair *qpair)
{
	return qpair->id;
}
