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

#ifndef SPDK_OPAL_SPEC_H
#define SPDK_OPAL_SPEC_H

#include "spdk/stdinc.h"
#include "spdk/assert.h"

/*
 * TCG Storage Architecture Core Spec v2.01 r1.00
 * 3.2.2.3 Tokens
 */
#define SPDK_TINY_ATOM_TYPE_MAX			0x7F
#define SPDK_SHORT_ATOM_TYPE_MAX		0xBF
#define SPDK_MEDIUM_ATOM_TYPE_MAX		0xDF
#define SPDK_LONG_ATOM_TYPE_MAX			0xE3

#define SPDK_TINY_ATOM_SIGN_FLAG		0x40

#define SPDK_TINY_ATOM_DATA_MASK		0x3F

#define SPDK_SHORT_ATOM_ID			0x80
#define SPDK_SHORT_ATOM_BYTESTRING_FLAG		0x20
#define SPDK_SHORT_ATOM_SIGN_FLAG		0x10
#define SPDK_SHORT_ATOM_LEN_MASK		0x0F

#define SPDK_MEDIUM_ATOM_ID			0xC0
#define SPDK_MEDIUM_ATOM_BYTESTRING_FLAG	0x10

#define SPDK_MEDIUM_ATOM_SIGN_FLAG		0x08
#define SPDK_MEDIUM_ATOM_LEN_MASK		0x07

#define SPDK_LONG_ATOM_ID			0xE0
#define SPDK_LONG_ATOM_BYTESTRING_FLAG		0x02
#define SPDK_LONG_ATOM_SIGN_FLAG		0x01

/*
 * TCG Storage Architecture Core Spec v2.01 r1.00
 * Table-26 ComID management
 */
#define LV0_DISCOVERY_COMID			0x01

/*
 * TCG Storage Opal v2.01 r1.00
 * 5.2.3 Type Table Modification
 */
#define OPAL_MANUFACTURED_INACTIVE		0x08

#define LOCKING_RANGE_NON_GLOBAL		0x03

#define SPDK_OPAL_MAX_PASSWORD_SIZE		32 /* in bytes */

#define SPDK_OPAL_MAX_LOCKING_RANGE		8 /* maximum 8 ranges defined  by spec */

/*
 * Feature Code
 */
enum spdk_lv0_discovery_feature_code {
	/*
	 * TCG Storage Architecture Core Spec v2.01 r1.00
	 * 3.3.6 Level 0 Discovery
	 */
	FEATURECODE_TPER	= 0x0001,
	FEATURECODE_LOCKING	= 0x0002,

	/*
	 * Opal SSC 1.00 r3.00 Final
	 * 3.1.1.4 Opal SSC Feature
	 */
	FEATURECODE_OPALV100	= 0x0200,

	/*
	 * TCG Storage Opal v2.01 r1.00
	 * 3.1.1.4 Geometry Reporting Feature
	 * 3.1.1.5 Opal SSC V2.00 Feature
	 */
	FEATURECODE_OPALV200	= 0x0203,
	FEATURECODE_GEOMETRY	= 0x0003,

	/*
	 * TCG Storage Opal Feature Set Single User Mode v1.00 r2.00
	 * 4.2.1 Single User Mode Feature Descriptor
	 */
	FEATURECODE_SINGLEUSER	= 0x0201,

	/*
	 * TCG Storage Opal Feature Set Additional DataStore Tables v1.00 r1.00
	 * 4.1.1 DataStore Table Feature Descriptor
	 */
	FEATURECODE_DATASTORE	= 0x0202,
};

/*
 * TCG Storage Architecture Core Spec v2.01 r1.00
 * 5.1.4 Abstract Type
 */
enum spdk_opal_token {
	/* boolean */
	SPDK_OPAL_TRUE			= 0x01,
	SPDK_OPAL_FALSE			= 0x00,

	/* cell_block
	 * 5.1.4.2.3 */
	SPDK_OPAL_TABLE			= 0x00,
	SPDK_OPAL_STARTROW		= 0x01,
	SPDK_OPAL_ENDROW		= 0x02,
	SPDK_OPAL_STARTCOLUMN		= 0x03,
	SPDK_OPAL_ENDCOLUMN		= 0x04,
	SPDK_OPAL_VALUES		= 0x01,

	/* C_PIN table
	 * 5.3.2.12 */
	SPDK_OPAL_PIN			= 0x03,

	/* locking table
	 * 5.7.2.2 */
	SPDK_OPAL_RANGESTART		= 0x03,
	SPDK_OPAL_RANGELENGTH		= 0x04,
	SPDK_OPAL_READLOCKENABLED	= 0x05,
	SPDK_OPAL_WRITELOCKENABLED	= 0x06,
	SPDK_OPAL_READLOCKED		= 0x07,
	SPDK_OPAL_WRITELOCKED		= 0x08,
	SPDK_OPAL_ACTIVEKEY		= 0x0A,

	/* locking info table */
	SPDK_OPAL_MAXRANGES		= 0x04,

	/* mbr control */
	SPDK_OPAL_MBRENABLE		= 0x01,
	SPDK_OPAL_MBRDONE		= 0x02,

	/* properties */
	SPDK_OPAL_HOSTPROPERTIES	= 0x00,

	/* control tokens */
	SPDK_OPAL_STARTLIST		= 0xF0,
	SPDK_OPAL_ENDLIST		= 0xF1,
	SPDK_OPAL_STARTNAME		= 0xF2,
	SPDK_OPAL_ENDNAME		= 0xF3,
	SPDK_OPAL_CALL			= 0xF8,
	SPDK_OPAL_ENDOFDATA		= 0xF9,
	SPDK_OPAL_ENDOFSESSION		= 0xFA,
	SPDK_OPAL_STARTTRANSACTON	= 0xFB,
	SPDK_OPAL_ENDTRANSACTON		= 0xFC,
	SPDK_OPAL_EMPTYATOM		= 0xFF,
	SPDK_OPAL_WHERE			= 0x00,

	/* life cycle */
	SPDK_OPAL_LIFECYCLE		= 0x06,

	/* Autority table */
	SPDK_OPAL_AUTH_ENABLE		= 0x05,

	/* ACE table */
	SPDK_OPAL_BOOLEAN_EXPR		= 0x03,
};

/*
 * TCG Storage Architecture Core Spec v2.01 r1.00
 * Table-39 Level0 Discovery Header Format
 */
struct spdk_opal_d0_hdr {
	uint32_t length;
	uint32_t revision;
	uint32_t reserved_0;
	uint32_t reserved_1;
	uint8_t vendor_specfic[32];
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_hdr) == 48, "Incorrect size");

/*
 * Level 0 Discovery Feature Header
 */
struct spdk_opal_d0_feat_hdr {
	uint16_t	code;
	uint8_t		reserved : 4;
	uint8_t		version : 4;
	uint8_t		length;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_feat_hdr) == 4, "Incorrect size");


/*
 * TCG Storage Architecture Core Spec v2.01 r1.00
 * Table-42 TPer Feature Descriptor
 */
struct __attribute__((packed)) spdk_opal_d0_tper_feat {
	struct spdk_opal_d0_feat_hdr hdr;
	uint8_t sync : 1;
	uint8_t async : 1;
	uint8_t acknack : 1;
	uint8_t buffer_management : 1;
	uint8_t streaming : 1;
	uint8_t reserved_1 : 1;
	uint8_t comid_management : 1;
	uint8_t reserved_2 : 1;

	uint8_t reserved_3[3];
	uint32_t reserved_4;
	uint32_t reserved_5;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_tper_feat) == 16, "Incorrect size");

/*
 * TCG Storage Architecture Core Spec v2.01 r1.00
 * Table-43 Locking Feature Descriptor
 */
struct __attribute__((packed)) spdk_opal_d0_locking_feat {
	struct spdk_opal_d0_feat_hdr hdr;
	uint8_t locking_supported : 1;
	uint8_t locking_enabled : 1;
	uint8_t locked : 1;
	uint8_t media_encryption : 1;
	uint8_t mbr_enabled : 1;
	uint8_t mbr_done : 1;
	uint8_t reserved_1 : 1;
	uint8_t reserved_2 : 1;

	uint8_t reserved_3[3];
	uint32_t reserved_4;
	uint32_t reserved_5;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_locking_feat) == 16, "Incorrect size");

/*
 * TCG Storage Opal Feature Set Single User Mode v1.00 r2.00
 * 4.2.1 Single User Mode Feature Descriptor
 */
struct __attribute__((packed)) spdk_opal_d0_single_user_mode_feat {
	struct spdk_opal_d0_feat_hdr hdr;
	uint32_t num_locking_objects;
	uint8_t any : 1;
	uint8_t all : 1;
	uint8_t policy : 1;
	uint8_t reserved_1 : 5;

	uint8_t reserved_2;
	uint16_t reserved_3;
	uint32_t reserved_4;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_single_user_mode_feat) == 16, "Incorrect size");

/*
 * TCG Storage Opal v2.01 r1.00
 * 3.1.1.4 Geometry Reporting Feature
 */
struct __attribute__((packed)) spdk_opal_d0_geo_feat {
	struct spdk_opal_d0_feat_hdr hdr;
	uint8_t align : 1;
	uint8_t reserved_1 : 7;
	uint8_t reserved_2[7];
	uint32_t logical_block_size;
	uint64_t alignment_granularity;
	uint64_t lowest_aligned_lba;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_geo_feat) == 32, "Incorrect size");

/*
 * TCG Storage Opal Feature Set Additional DataStore Tables v1.00 r1.00
 * 4.1.1 DataStore Table Feature Descriptor
 */
struct __attribute__((packed)) spdk_opal_d0_datastore_feat {
	struct spdk_opal_d0_feat_hdr hdr;
	uint16_t reserved_1;
	uint16_t max_tables;
	uint32_t max_table_size;
	uint32_t alignment;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_datastore_feat) == 16, "Incorrect size");

/*
 * Opal SSC 1.00 r3.00 Final
 * 3.1.1.4 Opal SSC Feature
 */
struct __attribute__((packed)) spdk_opal_d0_v100_feat {
	struct spdk_opal_d0_feat_hdr hdr;
	uint16_t base_comid;
	uint16_t number_comids;
	uint8_t range_crossing : 1;

	uint8_t reserved_1 : 7;
	uint8_t reserved_2;
	uint16_t reserved_3;
	uint32_t reserved_4;
	uint32_t reserved_5;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_v100_feat) == 20, "Incorrect size");

/*
 * TCG Storage Opal v2.01 r1.00
 * 3.1.1.4 Geometry Reporting Feature
 * 3.1.1.5 Opal SSC V2.00 Feature
 */
struct __attribute__((packed)) spdk_opal_d0_v200_feat {
	struct spdk_opal_d0_feat_hdr hdr;
	uint16_t base_comid;
	uint16_t num_comids;
	uint8_t range_crossing : 1;
	uint8_t reserved_1 : 7;
	uint16_t num_locking_admin_auth; /* Number of Locking SP Admin Authorities Supported */
	uint16_t num_locking_user_auth;
	uint8_t initial_pin;
	uint8_t reverted_pin;

	uint8_t reserved_2;
	uint32_t reserved_3;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_d0_v200_feat) == 20, "Incorrect size");

/*
 * TCG Storage Architecture Core Spec v2.01 r1.00
 * 3.2.3 ComPackets, Packets & Subpackets
 */

/* CommPacket header format
 * (big-endian)
 */
struct __attribute__((packed)) spdk_opal_compacket {
	uint32_t reserved;
	uint8_t comid[2];
	uint8_t extended_comid[2];
	uint32_t outstanding_data;
	uint32_t min_transfer;
	uint32_t length;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_compacket) == 20, "Incorrect size");

/* packet header format */
struct __attribute__((packed)) spdk_opal_packet {
	uint32_t session_tsn;
	uint32_t session_hsn;
	uint32_t seq_number;
	uint16_t reserved;
	uint16_t ack_type;
	uint32_t acknowledgment;
	uint32_t length;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_packet) == 24, "Incorrect size");

/* data subpacket header */
struct __attribute__((packed)) spdk_opal_data_subpacket {
	uint8_t reserved[6];
	uint16_t kind;
	uint32_t length;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_opal_data_subpacket) == 12, "Incorrect size");

#endif
