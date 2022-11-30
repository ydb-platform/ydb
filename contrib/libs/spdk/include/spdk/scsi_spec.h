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

/**
 * \file
 * SCSI specification definitions
 */

#ifndef SPDK_SCSI_SPEC_H
#define SPDK_SCSI_SPEC_H

#include "spdk/stdinc.h"

#include "spdk/assert.h"

enum spdk_scsi_group_code {
	SPDK_SCSI_6BYTE_CMD = 0x00,
	SPDK_SCSI_10BYTE_CMD = 0x20,
	SPDK_SCSI_10BYTE_CMD2 = 0x40,
	SPDK_SCSI_16BYTE_CMD = 0x80,
	SPDK_SCSI_12BYTE_CMD = 0xa0,
};

#define SPDK_SCSI_GROUP_MASK	0xe0
#define SPDK_SCSI_OPCODE_MASK	0x1f

enum spdk_scsi_status {
	SPDK_SCSI_STATUS_GOOD = 0x00,
	SPDK_SCSI_STATUS_CHECK_CONDITION = 0x02,
	SPDK_SCSI_STATUS_CONDITION_MET = 0x04,
	SPDK_SCSI_STATUS_BUSY = 0x08,
	SPDK_SCSI_STATUS_INTERMEDIATE = 0x10,
	SPDK_SCSI_STATUS_INTERMEDIATE_CONDITION_MET = 0x14,
	SPDK_SCSI_STATUS_RESERVATION_CONFLICT = 0x18,
	SPDK_SCSI_STATUS_Obsolete = 0x22,
	SPDK_SCSI_STATUS_TASK_SET_FULL = 0x28,
	SPDK_SCSI_STATUS_ACA_ACTIVE = 0x30,
	SPDK_SCSI_STATUS_TASK_ABORTED = 0x40,
};

enum spdk_scsi_sense {
	SPDK_SCSI_SENSE_NO_SENSE = 0x00,
	SPDK_SCSI_SENSE_RECOVERED_ERROR = 0x01,
	SPDK_SCSI_SENSE_NOT_READY = 0x02,
	SPDK_SCSI_SENSE_MEDIUM_ERROR = 0x03,
	SPDK_SCSI_SENSE_HARDWARE_ERROR = 0x04,
	SPDK_SCSI_SENSE_ILLEGAL_REQUEST = 0x05,
	SPDK_SCSI_SENSE_UNIT_ATTENTION = 0x06,
	SPDK_SCSI_SENSE_DATA_PROTECT = 0x07,
	SPDK_SCSI_SENSE_BLANK_CHECK = 0x08,
	SPDK_SCSI_SENSE_VENDOR_SPECIFIC = 0x09,
	SPDK_SCSI_SENSE_COPY_ABORTED = 0x0a,
	SPDK_SCSI_SENSE_ABORTED_COMMAND = 0x0b,
	SPDK_SCSI_SENSE_VOLUME_OVERFLOW = 0x0d,
	SPDK_SCSI_SENSE_MISCOMPARE = 0x0e,
};

enum spdk_scsi_asc {
	SPDK_SCSI_ASC_NO_ADDITIONAL_SENSE = 0x00,
	SPDK_SCSI_ASC_PERIPHERAL_DEVICE_WRITE_FAULT = 0x03,
	SPDK_SCSI_ASC_LOGICAL_UNIT_NOT_READY = 0x04,
	SPDK_SCSI_ASC_WARNING = 0x0b,
	SPDK_SCSI_ASC_LOGICAL_BLOCK_GUARD_CHECK_FAILED = 0x10,
	SPDK_SCSI_ASC_LOGICAL_BLOCK_APP_TAG_CHECK_FAILED = 0x10,
	SPDK_SCSI_ASC_LOGICAL_BLOCK_REF_TAG_CHECK_FAILED = 0x10,
	SPDK_SCSI_ASC_UNRECOVERED_READ_ERROR = 0x11,
	SPDK_SCSI_ASC_MISCOMPARE_DURING_VERIFY_OPERATION = 0x1d,
	SPDK_SCSI_ASC_INVALID_COMMAND_OPERATION_CODE = 0x20,
	SPDK_SCSI_ASC_ACCESS_DENIED = 0x20,
	SPDK_SCSI_ASC_LOGICAL_BLOCK_ADDRESS_OUT_OF_RANGE = 0x21,
	SPDK_SCSI_ASC_INVALID_FIELD_IN_CDB = 0x24,
	SPDK_SCSI_ASC_LOGICAL_UNIT_NOT_SUPPORTED = 0x25,
	SPDK_SCSI_ASC_WRITE_PROTECTED = 0x27,
	SPDK_SCSI_ASC_FORMAT_COMMAND_FAILED = 0x31,
	SPDK_SCSI_ASC_SAVING_PARAMETERS_NOT_SUPPORTED = 0x39,
	SPDK_SCSI_ASC_INTERNAL_TARGET_FAILURE = 0x44,
};

enum spdk_scsi_ascq {
	SPDK_SCSI_ASCQ_CAUSE_NOT_REPORTABLE = 0x00,
	SPDK_SCSI_ASCQ_BECOMING_READY = 0x01,
	SPDK_SCSI_ASCQ_FORMAT_COMMAND_FAILED = 0x01,
	SPDK_SCSI_ASCQ_LOGICAL_BLOCK_GUARD_CHECK_FAILED = 0x01,
	SPDK_SCSI_ASCQ_LOGICAL_BLOCK_APP_TAG_CHECK_FAILED = 0x02,
	SPDK_SCSI_ASCQ_NO_ACCESS_RIGHTS = 0x02,
	SPDK_SCSI_ASCQ_LOGICAL_BLOCK_REF_TAG_CHECK_FAILED = 0x03,
	SPDK_SCSI_ASCQ_POWER_LOSS_EXPECTED = 0x08,
	SPDK_SCSI_ASCQ_INVALID_LU_IDENTIFIER = 0x09,
};

enum spdk_spc_opcode {
	/* SPC3 related */
	SPDK_SPC_ACCESS_CONTROL_IN = 0x86,
	SPDK_SPC_ACCESS_CONTROL_OUT = 0x87,
	SPDK_SPC_EXTENDED_COPY = 0x83,
	SPDK_SPC_INQUIRY = 0x12,
	SPDK_SPC_LOG_SELECT = 0x4c,
	SPDK_SPC_LOG_SENSE = 0x4d,
	SPDK_SPC_MODE_SELECT_6 = 0x15,
	SPDK_SPC_MODE_SELECT_10 = 0x55,
	SPDK_SPC_MODE_SENSE_6 = 0x1a,
	SPDK_SPC_MODE_SENSE_10 = 0x5a,
	SPDK_SPC_PERSISTENT_RESERVE_IN = 0x5e,
	SPDK_SPC_PERSISTENT_RESERVE_OUT = 0x5f,
	SPDK_SPC_PREVENT_ALLOW_MEDIUM_REMOVAL = 0x1e,
	SPDK_SPC_READ_ATTRIBUTE = 0x8c,
	SPDK_SPC_READ_BUFFER = 0x3c,
	SPDK_SPC_RECEIVE_COPY_RESULTS = 0x84,
	SPDK_SPC_RECEIVE_DIAGNOSTIC_RESULTS = 0x1c,
	SPDK_SPC_REPORT_LUNS = 0xa0,
	SPDK_SPC_REQUEST_SENSE = 0x03,
	SPDK_SPC_SEND_DIAGNOSTIC = 0x1d,
	SPDK_SPC_TEST_UNIT_READY = 0x00,
	SPDK_SPC_WRITE_ATTRIBUTE = 0x8d,
	SPDK_SPC_WRITE_BUFFER = 0x3b,

	SPDK_SPC_SERVICE_ACTION_IN_12 = 0xab,
	SPDK_SPC_SERVICE_ACTION_OUT_12 = 0xa9,
	SPDK_SPC_SERVICE_ACTION_IN_16 = 0x9e,
	SPDK_SPC_SERVICE_ACTION_OUT_16 = 0x9f,

	SPDK_SPC_VARIABLE_LENGTH = 0x7f,

	SPDK_SPC_MO_CHANGE_ALIASES = 0x0b,
	SPDK_SPC_MO_SET_DEVICE_IDENTIFIER = 0x06,
	SPDK_SPC_MO_SET_PRIORITY = 0x0e,
	SPDK_SPC_MO_SET_TARGET_PORT_GROUPS = 0x0a,
	SPDK_SPC_MO_SET_TIMESTAMP = 0x0f,
	SPDK_SPC_MI_REPORT_ALIASES = 0x0b,
	SPDK_SPC_MI_REPORT_DEVICE_IDENTIFIER = 0x05,
	SPDK_SPC_MI_REPORT_PRIORITY = 0x0e,
	SPDK_SPC_MI_REPORT_SUPPORTED_OPERATION_CODES = 0x0c,
	SPDK_SPC_MI_REPORT_SUPPORTED_TASK_MANAGEMENT_FUNCTIONS = 0x0d,
	SPDK_SPC_MI_REPORT_TARGET_PORT_GROUPS = 0x0a,
	SPDK_SPC_MI_REPORT_TIMESTAMP = 0x0f,

	/* SPC2 related (Obsolete) */
	SPDK_SPC2_RELEASE_6 = 0x17,
	SPDK_SPC2_RELEASE_10 = 0x57,
	SPDK_SPC2_RESERVE_6 = 0x16,
	SPDK_SPC2_RESERVE_10 = 0x56,
};

enum spdk_scc_opcode {
	SPDK_SCC_MAINTENANCE_IN = 0xa3,
	SPDK_SCC_MAINTENANCE_OUT = 0xa4,
};

enum spdk_sbc_opcode {
	SPDK_SBC_COMPARE_AND_WRITE = 0x89,
	SPDK_SBC_FORMAT_UNIT = 0x04,
	SPDK_SBC_GET_LBA_STATUS = 0x0012009e,
	SPDK_SBC_ORWRITE_16 = 0x8b,
	SPDK_SBC_PRE_FETCH_10 = 0x34,
	SPDK_SBC_PRE_FETCH_16 = 0x90,
	SPDK_SBC_READ_6 = 0x08,
	SPDK_SBC_READ_10 = 0x28,
	SPDK_SBC_READ_12 = 0xa8,
	SPDK_SBC_READ_16 = 0x88,
	SPDK_SBC_READ_ATTRIBUTE = 0x8c,
	SPDK_SBC_READ_BUFFER = 0x3c,
	SPDK_SBC_READ_CAPACITY_10 = 0x25,
	SPDK_SBC_READ_DEFECT_DATA_10 = 0x37,
	SPDK_SBC_READ_DEFECT_DATA_12 = 0xb7,
	SPDK_SBC_READ_LONG_10 = 0x3e,
	SPDK_SBC_REASSIGN_BLOCKS = 0x07,
	SPDK_SBC_SANITIZE = 0x48,
	SPDK_SBC_START_STOP_UNIT = 0x1b,
	SPDK_SBC_SYNCHRONIZE_CACHE_10 = 0x35,
	SPDK_SBC_SYNCHRONIZE_CACHE_16 = 0x91,
	SPDK_SBC_UNMAP = 0x42,
	SPDK_SBC_VERIFY_10 = 0x2f,
	SPDK_SBC_VERIFY_12 = 0xaf,
	SPDK_SBC_VERIFY_16 = 0x8f,
	SPDK_SBC_WRITE_6 = 0x0a,
	SPDK_SBC_WRITE_10 = 0x2a,
	SPDK_SBC_WRITE_12 = 0xaa,
	SPDK_SBC_WRITE_16 = 0x8a,
	SPDK_SBC_WRITE_AND_VERIFY_10 = 0x2e,
	SPDK_SBC_WRITE_AND_VERIFY_12 = 0xae,
	SPDK_SBC_WRITE_AND_VERIFY_16 = 0x8e,
	SPDK_SBC_WRITE_LONG_10 = 0x3f,
	SPDK_SBC_WRITE_SAME_10 = 0x41,
	SPDK_SBC_WRITE_SAME_16 = 0x93,
	SPDK_SBC_XDREAD_10 = 0x52,
	SPDK_SBC_XDWRITE_10 = 0x50,
	SPDK_SBC_XDWRITEREAD_10 = 0x53,
	SPDK_SBC_XPWRITE_10 = 0x51,

	SPDK_SBC_SAI_READ_CAPACITY_16 = 0x10,
	SPDK_SBC_SAI_READ_LONG_16 = 0x11,
	SPDK_SBC_SAO_WRITE_LONG_16 = 0x11,

	SPDK_SBC_VL_READ_32 = 0x0009,
	SPDK_SBC_VL_VERIFY_32 = 0x000a,
	SPDK_SBC_VL_WRITE_32 = 0x000b,
	SPDK_SBC_VL_WRITE_AND_VERIFY_32 = 0x000c,
	SPDK_SBC_VL_WRITE_SAME_32 = 0x000d,
	SPDK_SBC_VL_XDREAD_32 = 0x0003,
	SPDK_SBC_VL_XDWRITE_32 = 0x0004,
	SPDK_SBC_VL_XDWRITEREAD_32 = 0x0007,
	SPDK_SBC_VL_XPWRITE_32 = 0x0006,
};

#define SPDK_SBC_START_STOP_UNIT_START_BIT (1 << 0)

enum spdk_mmc_opcode {
	/* MMC6 */
	SPDK_MMC_READ_DISC_STRUCTURE = 0xad,

	/* MMC4 */
	SPDK_MMC_BLANK = 0xa1,
	SPDK_MMC_CLOSE_TRACK_SESSION = 0x5b,
	SPDK_MMC_ERASE_10 = 0x2c,
	SPDK_MMC_FORMAT_UNIT = 0x04,
	SPDK_MMC_GET_CONFIGURATION = 0x46,
	SPDK_MMC_GET_EVENT_STATUS_NOTIFICATION = 0x4a,
	SPDK_MMC_GET_PERFORMANCE = 0xac,
	SPDK_MMC_INQUIRY = 0x12,
	SPDK_MMC_LOAD_UNLOAD_MEDIUM = 0xa6,
	SPDK_MMC_MECHANISM_STATUS = 0xbd,
	SPDK_MMC_MODE_SELECT_10 = 0x55,
	SPDK_MMC_MODE_SENSE_10 = 0x5a,
	SPDK_MMC_PAUSE_RESUME = 0x4b,
	SPDK_MMC_PLAY_AUDIO_10 = 0x45,
	SPDK_MMC_PLAY_AUDIO_12 = 0xa5,
	SPDK_MMC_PLAY_AUDIO_MSF = 0x47,
	SPDK_MMC_PREVENT_ALLOW_MEDIUM_REMOVAL = 0x1e,
	SPDK_MMC_READ_10 = 0x28,
	SPDK_MMC_READ_12 = 0xa8,
	SPDK_MMC_READ_BUFFER = 0x3c,
	SPDK_MMC_READ_BUFFER_CAPACITY = 0x5c,
	SPDK_MMC_READ_CAPACITY = 0x25,
	SPDK_MMC_READ_CD = 0xbe,
	SPDK_MMC_READ_CD_MSF = 0xb9,
	SPDK_MMC_READ_DISC_INFORMATION = 0x51,
	SPDK_MMC_READ_DVD_STRUCTURE = 0xad,
	SPDK_MMC_READ_FORMAT_CAPACITIES = 0x23,
	SPDK_MMC_READ_SUB_CHANNEL = 0x42,
	SPDK_MMC_READ_TOC_PMA_ATIP = 0x43,
	SPDK_MMC_READ_TRACK_INFORMATION = 0x52,
	SPDK_MMC_REPAIR_TRACK = 0x58,
	SPDK_MMC_REPORT_KEY = 0xa4,
	SPDK_MMC_REQUEST_SENSE = 0x03,
	SPDK_MMC_RESERVE_TRACK = 0x53,
	SPDK_MMC_SCAN = 0xba,
	SPDK_MMC_SEEK_10 = 0x2b,
	SPDK_MMC_SEND_CUE_SHEET = 0x5d,
	SPDK_MMC_SEND_DVD_STRUCTURE = 0xbf,
	SPDK_MMC_SEND_KEY = 0xa3,
	SPDK_MMC_SEND_OPC_INFORMATION = 0x54,
	SPDK_MMC_SET_CD_SPEED = 0xbb,
	SPDK_MMC_SET_READ_AHEAD = 0xa7,
	SPDK_MMC_SET_STREAMING = 0xb6,
	SPDK_MMC_START_STOP_UNIT = 0x1b,
	SPDK_MMC_STOP_PLAY_SCAN = 0x4e,
	SPDK_MMC_SYNCHRONIZE_CACHE = 0x35,
	SPDK_MMC_TEST_UNIT_READY = 0x00,
	SPDK_MMC_VERIFY_10 = 0x2f,
	SPDK_MMC_WRITE_10 = 0xa2,
	SPDK_MMC_WRITE_12 = 0xaa,
	SPDK_MMC_WRITE_AND_VERIFY_10 = 0x2e,
	SPDK_MMC_WRITE_BUFFER = 0x3b,
};

enum spdk_ssc_opcode {
	SPDK_SSC_ERASE_6 = 0x19,
	SPDK_SSC_FORMAT_MEDIUM = 0x04,
	SPDK_SSC_LOAD_UNLOAD = 0x1b,
	SPDK_SSC_LOCATE_10 = 0x2b,
	SPDK_SSC_LOCATE_16 = 0x92,
	SPDK_SSC_MOVE_MEDIUM_ATTACHED = 0xa7,
	SPDK_SSC_READ_6 = 0x08,
	SPDK_SSC_READ_BLOCK_LIMITS = 0x05,
	SPDK_SSC_READ_ELEMENT_STATUS_ATTACHED = 0xb4,
	SPDK_SSC_READ_POSITION = 0x34,
	SPDK_SSC_READ_REVERSE_6 = 0x0f,
	SPDK_SSC_RECOVER_BUFFERED_DATA = 0x14,
	SPDK_SSC_REPORT_DENSITY_SUPPORT = 0x44,
	SPDK_SSC_REWIND = 0x01,
	SPDK_SSC_SET_CAPACITY = 0x0b,
	SPDK_SSC_SPACE_6 = 0x11,
	SPDK_SSC_SPACE_16 = 0x91,
	SPDK_SSC_VERIFY_6 = 0x13,
	SPDK_SSC_WRITE_6 = 0x0a,
	SPDK_SSC_WRITE_FILEMARKS_6 = 0x10,
};

enum spdk_spc_vpd {
	SPDK_SPC_VPD_DEVICE_IDENTIFICATION = 0x83,
	SPDK_SPC_VPD_EXTENDED_INQUIRY_DATA = 0x86,
	SPDK_SPC_VPD_MANAGEMENT_NETWORK_ADDRESSES = 0x85,
	SPDK_SPC_VPD_MODE_PAGE_POLICY = 0x87,
	SPDK_SPC_VPD_SCSI_PORTS = 0x88,
	SPDK_SPC_VPD_SOFTWARE_INTERFACE_IDENTIFICATION = 0x84,
	SPDK_SPC_VPD_SUPPORTED_VPD_PAGES = 0x00,
	SPDK_SPC_VPD_UNIT_SERIAL_NUMBER = 0x80,
	SPDK_SPC_VPD_BLOCK_LIMITS = 0xb0,
	SPDK_SPC_VPD_BLOCK_DEV_CHARS = 0xb1,
	SPDK_SPC_VPD_BLOCK_THIN_PROVISION = 0xb2,
};

enum spdk_spc_peripheral_qualifier {
	SPDK_SPC_PERIPHERAL_QUALIFIER_CONNECTED = 0,
	SPDK_SPC_PERIPHERAL_QUALIFIER_NOT_CONNECTED = 1,
	SPDK_SPC_PERIPHERAL_QUALIFIER_NOT_CAPABLE = 3,
};

enum {
	SPDK_SPC_PERIPHERAL_DEVICE_TYPE_DISK = 0x00,
	SPDK_SPC_PERIPHERAL_DEVICE_TYPE_TAPE = 0x01,
	SPDK_SPC_PERIPHERAL_DEVICE_TYPE_DVD = 0x05,
	SPDK_SPC_PERIPHERAL_DEVICE_TYPE_CHANGER = 0x08,

	SPDK_SPC_VERSION_NONE = 0x00,
	SPDK_SPC_VERSION_SPC = 0x03,
	SPDK_SPC_VERSION_SPC2 = 0x04,
	SPDK_SPC_VERSION_SPC3 = 0x05,
	SPDK_SPC_VERSION_SPC4 = 0x06,

	SPDK_SPC_PROTOCOL_IDENTIFIER_FC = 0x00,
	SPDK_SPC_PROTOCOL_IDENTIFIER_PSCSI = 0x01,
	SPDK_SPC_PROTOCOL_IDENTIFIER_SSA = 0x02,
	SPDK_SPC_PROTOCOL_IDENTIFIER_IEEE1394 = 0x03,
	SPDK_SPC_PROTOCOL_IDENTIFIER_RDMA = 0x04,
	SPDK_SPC_PROTOCOL_IDENTIFIER_ISCSI = 0x05,
	SPDK_SPC_PROTOCOL_IDENTIFIER_SAS = 0x06,
	SPDK_SPC_PROTOCOL_IDENTIFIER_ADT = 0x07,
	SPDK_SPC_PROTOCOL_IDENTIFIER_ATA = 0x08,

	SPDK_SPC_VPD_CODE_SET_BINARY = 0x01,
	SPDK_SPC_VPD_CODE_SET_ASCII = 0x02,
	SPDK_SPC_VPD_CODE_SET_UTF8 = 0x03,

	SPDK_SPC_VPD_ASSOCIATION_LOGICAL_UNIT = 0x00,
	SPDK_SPC_VPD_ASSOCIATION_TARGET_PORT = 0x01,
	SPDK_SPC_VPD_ASSOCIATION_TARGET_DEVICE = 0x02,

	SPDK_SPC_VPD_IDENTIFIER_TYPE_VENDOR_SPECIFIC = 0x00,
	SPDK_SPC_VPD_IDENTIFIER_TYPE_T10_VENDOR_ID = 0x01,
	SPDK_SPC_VPD_IDENTIFIER_TYPE_EUI64 = 0x02,
	SPDK_SPC_VPD_IDENTIFIER_TYPE_NAA = 0x03,
	SPDK_SPC_VPD_IDENTIFIER_TYPE_RELATIVE_TARGET_PORT = 0x04,
	SPDK_SPC_VPD_IDENTIFIER_TYPE_TARGET_PORT_GROUP = 0x05,
	SPDK_SPC_VPD_IDENTIFIER_TYPE_LOGICAL_UNIT_GROUP = 0x06,
	SPDK_SPC_VPD_IDENTIFIER_TYPE_MD5_LOGICAL_UNIT = 0x07,
	SPDK_SPC_VPD_IDENTIFIER_TYPE_SCSI_NAME = 0x08,
};

struct spdk_scsi_cdb_inquiry {
	uint8_t opcode;
	uint8_t evpd;
	uint8_t page_code;
	uint8_t alloc_len[2];
	uint8_t control;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_cdb_inquiry) == 6, "incorrect CDB size");

struct spdk_scsi_cdb_inquiry_data {
	uint8_t peripheral_device_type : 5;
	uint8_t peripheral_qualifier : 3;
	uint8_t rmb;
	uint8_t version;
	uint8_t response;
	uint8_t add_len;
	uint8_t flags;
	uint8_t flags2;
	uint8_t flags3;
	uint8_t t10_vendor_id[8];
	uint8_t product_id[16];
	uint8_t product_rev[4];
	uint8_t vendor[20];
	uint8_t ius;
	uint8_t reserved;
	uint8_t desc[];
};

struct spdk_scsi_vpd_page {
	uint8_t peripheral_device_type : 5;
	uint8_t peripheral_qualifier : 3;
	uint8_t page_code;
	uint8_t alloc_len[2];
	uint8_t params[];
};

#define SPDK_SCSI_VEXT_REF_CHK		0x01
#define SPDK_SCSI_VEXT_APP_CHK		0x02
#define SPDK_SCSI_VEXT_GRD_CHK		0x04
#define SPDK_SCSI_VEXT_SIMPSUP		0x01
#define SPDK_SCSI_VEXT_ORDSUP		0x02
#define SPDK_SCSI_VEXT_HEADSUP		0x04
#define SPDK_SCSI_VEXT_PRIOR_SUP	0x08
#define SPDK_SCSI_VEXT_GROUP_SUP	0x10
#define SPDK_SCSI_VEXT_UASK_SUP		0x20
#define SPDK_SCSI_VEXT_V_SUP		0x01
#define SPDK_SCSI_VEXT_NV_SUP		0x02
#define SPDK_SCSI_VEXT_CRD_SUP		0x04
#define SPDK_SCSI_VEXT_WU_SUP		0x08

struct spdk_scsi_vpd_ext_inquiry {
	uint8_t peripheral;
	uint8_t page_code;
	uint8_t alloc_len[2];
	uint8_t check;
	uint8_t sup;
	uint8_t sup2;
	uint8_t luiclr;
	uint8_t cbcs;
	uint8_t micro_dl;
	uint8_t reserved[54];
};

#define SPDK_SPC_VPD_DESIG_PIV	0x80

/* designation descriptor */
struct spdk_scsi_desig_desc {
	uint8_t code_set	: 4;
	uint8_t protocol_id	: 4;
	uint8_t type		: 4;
	uint8_t association	: 2;
	uint8_t reserved0	: 1;
	uint8_t piv		: 1;
	uint8_t reserved1;
	uint8_t	len;
	uint8_t desig[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_desig_desc) == 4, "Invalid size");

/* mode page policy descriptor */
struct spdk_scsi_mpage_policy_desc {
	uint8_t page_code;
	uint8_t sub_page_code;
	uint8_t policy;
	uint8_t reserved;
};

/* target port descriptor */
struct spdk_scsi_tgt_port_desc {
	uint8_t code_set;
	uint8_t desig_type;
	uint8_t reserved;
	uint8_t	len;
	uint8_t designator[];
};

/* SCSI port designation descriptor */
struct spdk_scsi_port_desc {
	uint16_t reserved;
	uint16_t rel_port_id;
	uint16_t reserved2;
	uint16_t init_port_len;
	uint16_t init_port_id;
	uint16_t reserved3;
	uint16_t tgt_desc_len;
	uint8_t tgt_desc[];
};

/* iSCSI initiator port TransportID header */
struct spdk_scsi_iscsi_transport_id {
	uint8_t protocol_id : 4;
	uint8_t reserved1   : 2;
	uint8_t format      : 2;
	uint8_t reserved2;
	uint16_t additional_len;
	uint8_t name[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_iscsi_transport_id) == 4, "Incorrect size");

/* SCSI UNMAP block descriptor */
struct spdk_scsi_unmap_bdesc {
	/* UNMAP LOGICAL BLOCK ADDRESS */
	uint64_t lba;

	/* NUMBER OF LOGICAL BLOCKS */
	uint32_t block_count;

	/* RESERVED */
	uint32_t reserved;
};

/* SCSI Persistent Reserve In action codes */
enum spdk_scsi_pr_in_action_code {
	/* Read all registered reservation keys */
	SPDK_SCSI_PR_IN_READ_KEYS		= 0x00,
	/* Read current persistent reservations */
	SPDK_SCSI_PR_IN_READ_RESERVATION	= 0x01,
	/* Return capabilities information */
	SPDK_SCSI_PR_IN_REPORT_CAPABILITIES	= 0x02,
	/* Read all registrations and persistent reservations */
	SPDK_SCSI_PR_IN_READ_FULL_STATUS	= 0x03,
	/* 0x04h - 0x1fh Reserved */
};

enum spdk_scsi_pr_scope_code {
	/* Persistent reservation applies to full logical unit */
	SPDK_SCSI_PR_LU_SCOPE			= 0x00,
};

/* SCSI Persistent Reservation type codes */
enum spdk_scsi_pr_type_code {
	/* Write Exclusive */
	SPDK_SCSI_PR_WRITE_EXCLUSIVE		= 0x01,
	/* Exclusive Access */
	SPDK_SCSI_PR_EXCLUSIVE_ACCESS		= 0x03,
	/* Write Exclusive - Registrants Only */
	SPDK_SCSI_PR_WRITE_EXCLUSIVE_REGS_ONLY	= 0x05,
	/* Exclusive Access - Registrants Only */
	SPDK_SCSI_PR_EXCLUSIVE_ACCESS_REGS_ONLY	= 0x06,
	/* Write Exclusive - All Registrants */
	SPDK_SCSI_PR_WRITE_EXCLUSIVE_ALL_REGS	= 0x07,
	/* Exclusive Access - All Registrants */
	SPDK_SCSI_PR_EXCLUSIVE_ACCESS_ALL_REGS	= 0x08,
};

/* SCSI Persistent Reserve In header for
 * Read Keys, Read Reservation, Read Full Status
 */
struct spdk_scsi_pr_in_read_header {
	/* persistent reservation generation */
	uint32_t pr_generation;
	uint32_t additional_len;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_pr_in_read_header) == 8, "Incorrect size");

/* SCSI Persistent Reserve In read keys data */
struct spdk_scsi_pr_in_read_keys_data {
	struct spdk_scsi_pr_in_read_header header;
	/* reservation key list */
	uint64_t rkeys[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_pr_in_read_keys_data) == 8, "Incorrect size");

/* SCSI Persistent Reserve In read reservations data */
struct spdk_scsi_pr_in_read_reservations_data {
	/* Fixed 0x10 with reservation and 0 for no reservation */
	struct spdk_scsi_pr_in_read_header header;
	/* reservation key */
	uint64_t rkey;
	uint32_t obsolete1;
	uint8_t reserved;
	uint8_t type  : 4;
	uint8_t scope : 4;
	uint16_t obsolete2;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_pr_in_read_reservations_data) == 24, "Incorrect size");

/* SCSI Persistent Reserve In report capabilities data */
struct spdk_scsi_pr_in_report_capabilities_data {
	/* Fixed value 0x8 */
	uint16_t length;

	/* Persist through power loss capable */
	uint8_t ptpl_c    : 1;
	uint8_t reserved1 : 1;
	/* All target ports capable */
	uint8_t atp_c     : 1;
	/* Specify initiator port capable */
	uint8_t sip_c     : 1;
	/* Compatible reservation handing bit to indicate
	 * SPC-2 reserve/release is supported
	 */
	uint8_t crh       : 1;
	uint8_t reserved2 : 3;
	/* Persist through power loss activated */
	uint8_t ptpl_a    : 1;
	uint8_t reserved3 : 6;
	/* Type mask valid */
	uint8_t tmv       : 1;

	/* Type mask format */
	uint8_t reserved4 : 1;
	/* Write Exclusive */
	uint8_t wr_ex     : 1;
	uint8_t reserved5 : 1;
	/* Exclusive Access */
	uint8_t ex_ac     : 1;
	uint8_t reserved6 : 1;
	/* Write Exclusive - Registrants Only */
	uint8_t wr_ex_ro  : 1;
	/* Exclusive Access - Registrants Only */
	uint8_t ex_ac_ro  : 1;
	/* Write Exclusive - All Registrants */
	uint8_t wr_ex_ar  : 1;
	/* Exclusive Access - All Registrants */
	uint8_t ex_ac_ar  : 1;
	uint8_t reserved7 : 7;

	uint8_t reserved8[2];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_pr_in_report_capabilities_data) == 8, "Incorrect size");

/* SCSI Persistent Reserve In full status descriptor */
struct spdk_scsi_pr_in_full_status_desc {
	/* Reservation key */
	uint64_t rkey;
	uint8_t reserved1[4];

	/* 0 - Registrant only
	 * 1 - Registrant and reservation holder
	 */
	uint8_t r_holder  : 1;
	/* All target ports */
	uint8_t all_tg_pt : 1;
	uint8_t reserved2 : 6;

	/* Reservation type */
	uint8_t type      : 4;
	/* Set to LU_SCOPE */
	uint8_t scope     : 4;

	uint8_t reserved3[4];
	uint16_t relative_target_port_id;
	/* Size of TransportID */
	uint32_t desc_len;

	uint8_t transport_id[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_pr_in_full_status_desc) == 24, "Incorrect size");

/* SCSI Persistent Reserve In full status data */
struct spdk_scsi_pr_in_full_status_data {
	struct spdk_scsi_pr_in_read_header header;
	/* Full status descriptors */
	struct spdk_scsi_pr_in_full_status_desc desc_list[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_pr_in_full_status_data) == 8, "Incorrect size");

/* SCSI Persistent Reserve Out service action codes */
enum spdk_scsi_pr_out_service_action_code {
	/* Register/unregister a reservation key */
	SPDK_SCSI_PR_OUT_REGISTER		= 0x00,
	/* Create a persistent reservation */
	SPDK_SCSI_PR_OUT_RESERVE		= 0x01,
	/* Release a persistent reservation */
	SPDK_SCSI_PR_OUT_RELEASE		= 0x02,
	/* Clear all reservation keys and persistent reservations */
	SPDK_SCSI_PR_OUT_CLEAR			= 0x03,
	/* Preempt persistent reservations and/or remove registrants */
	SPDK_SCSI_PR_OUT_PREEMPT		= 0x04,
	/* Preempt persistent reservations and or remove registrants
	 * and abort all tasks for all preempted I_T nexuses
	 */
	SPDK_SCSI_PR_OUT_PREEMPT_AND_ABORT	= 0x05,
	/* Register/unregister a reservation key based on the ignore bit */
	SPDK_SCSI_PR_OUT_REG_AND_IGNORE_KEY	= 0x06,
	/* Register a reservation key for another I_T nexus
	 * and move a persistent reservation to that I_T nexus
	 */
	SPDK_SCSI_PR_OUT_REG_AND_MOVE		= 0x07,
	/* 0x08 - 0x1f Reserved */
};

/* SCSI Persistent Reserve Out parameter list */
struct spdk_scsi_pr_out_param_list {
	/* Reservation key */
	uint64_t rkey;
	/* Service action reservation key */
	uint64_t sa_rkey;
	uint8_t obsolete1[4];

	/* Active persist through power loss */
	uint8_t aptpl     : 1;
	uint8_t reserved1 : 1;
	/* All target ports */
	uint8_t all_tg_pt : 1;
	/* Specify initiator ports */
	uint8_t spec_i_pt : 1;
	uint8_t reserved2 : 4;

	uint8_t reserved3;
	uint16_t obsolete2;

	uint8_t param_data[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_pr_out_param_list) == 24, "Incorrect size");

struct spdk_scsi_pr_out_reg_and_move_param_list {
	/* Reservation key */
	uint64_t rkey;
	/* Service action reservation key */
	uint64_t sa_rkey;
	uint8_t reserved1;

	/* Active persist through power loss */
	uint8_t aptpl     : 1;
	/* Unregister */
	uint8_t unreg     : 1;
	uint8_t reserved2 : 6;

	uint16_t relative_target_port_id;
	/* TransportID parameter data length */
	uint32_t transport_id_len;
	uint8_t transport_id[];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_scsi_pr_out_reg_and_move_param_list) == 24, "Incorrect size");

/*
 * SPC-4
 * Table-258 SECURITY PROTOCOL field in SECURITY PROTOCOL IN command
 */
#define SPDK_SCSI_SECP_INFO	0x00
#define SPDK_SCSI_SECP_TCG	0x01

#define SPDK_SCSI_UNMAP_LBPU			1 << 7
#define SPDK_SCSI_UNMAP_LBPWS			1 << 6
#define SPDK_SCSI_UNMAP_LBPWS10			1 << 5

#define SPDK_SCSI_UNMAP_FULL_PROVISIONING	0x00
#define SPDK_SCSI_UNMAP_RESOURCE_PROVISIONING	0x01
#define SPDK_SCSI_UNMAP_THIN_PROVISIONING	0x02

#endif /* SPDK_SCSI_SPEC_H */
