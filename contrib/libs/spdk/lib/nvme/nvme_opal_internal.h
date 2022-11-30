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

#ifndef SPDK_OPAL_INTERNAL_H
#define SPDK_OPAL_INTERNAL_H

#include "spdk/opal_spec.h"
#include "spdk/opal.h"
#include "spdk/scsi_spec.h"

#define IO_BUFFER_LENGTH		2048
#define MAX_TOKS			64
#define OPAL_KEY_MAX			256
#define OPAL_UID_LENGTH			8

#define GENERIC_HOST_SESSION_NUM	0x69

#define OPAL_INVAL_PARAM		12

#define SPDK_DTAERROR_NO_METHOD_STATUS	0x89

enum opal_token_type {
	OPAL_DTA_TOKENID_BYTESTRING	= 0xE0,
	OPAL_DTA_TOKENID_SINT		= 0xE1,
	OPAL_DTA_TOKENID_UINT		= 0xE2,
	OPAL_DTA_TOKENID_TOKEN		= 0xE3, /* actual token is returned */
	OPAL_DTA_TOKENID_INVALID	= 0X0,
};

enum opal_atom_width {
	OPAL_WIDTH_TINY,    /* 1 byte in length */
	OPAL_WIDTH_SHORT,   /* a 1-byte header and contain up to 15 bytes of data */
	OPAL_WIDTH_MEDIUM,  /* a 2-byte header and contain up to 2047 bytes of data */
	OPAL_WIDTH_LONG,    /* a 4-byte header and which contain up to 16,777,215 bytes of data */
	OPAL_WIDTH_TOKEN
};

enum opal_uid_enum {
	/* users */
	UID_SMUID,
	UID_THISSP,
	UID_ADMINSP,
	UID_LOCKINGSP,
	UID_ANYBODY,
	UID_SID,
	UID_ADMIN1,
	UID_USER1,
	UID_USER2,

	/* tables */
	UID_LOCKINGRANGE_GLOBAL,
	UID_LOCKINGRANGE_ACE_RDLOCKED,
	UID_LOCKINGRANGE_ACE_WRLOCKED,
	UID_MBRCONTROL,
	UID_MBR,
	UID_AUTHORITY_TABLE,
	UID_C_PIN_TABLE,
	UID_LOCKING_INFO_TABLE,
	UID_PSID,

	/* C_PIN_TABLE object ID's */
	UID_C_PIN_MSID,
	UID_C_PIN_SID,
	UID_C_PIN_ADMIN1,
	UID_C_PIN_USER1,

	/* half UID's (only first 4 bytes used) */
	UID_HALF_AUTHORITY_OBJ_REF,
	UID_HALF_BOOLEAN_ACE,
};

/* enum for indexing the spdk_opal_method array */
enum opal_method_enum {
	PROPERTIES_METHOD,
	STARTSESSION_METHOD,
	REVERT_METHOD,
	ACTIVATE_METHOD,
	NEXT_METHOD,
	GETACL_METHOD,
	GENKEY_METHOD,
	REVERTSP_METHOD,
	GET_METHOD,
	SET_METHOD,
	AUTHENTICATE_METHOD,
	RANDOM_METHOD,
	ERASE_METHOD,
};

struct spdk_opal_key {
	uint8_t key_len;
	uint8_t key[OPAL_KEY_MAX];
};

const uint8_t spdk_opal_uid[][OPAL_UID_LENGTH] = {
	/* users */
	[UID_SMUID] = /* Session Manager UID */
	{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff },
	[UID_THISSP] =
	{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 },
	[UID_ADMINSP] =
	{ 0x00, 0x00, 0x02, 0x05, 0x00, 0x00, 0x00, 0x01 },
	[UID_LOCKINGSP] =
	{ 0x00, 0x00, 0x02, 0x05, 0x00, 0x00, 0x00, 0x02 },
	[UID_ANYBODY] =
	{ 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x01 },
	[UID_SID] = /* Security Identifier UID */
	{ 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x06 },
	[UID_ADMIN1] =
	{ 0x00, 0x00, 0x00, 0x09, 0x00, 0x01, 0x00, 0x01 },
	[UID_USER1] =
	{ 0x00, 0x00, 0x00, 0x09, 0x00, 0x03, 0x00, 0x01 },
	[UID_USER2] =
	{ 0x00, 0x00, 0x00, 0x09, 0x00, 0x03, 0x00, 0x02 },

	/* tables */
	[UID_LOCKINGRANGE_GLOBAL] =
	{ 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00, 0x01 },
	[UID_LOCKINGRANGE_ACE_RDLOCKED] =
	{ 0x00, 0x00, 0x00, 0x08, 0x00, 0x03, 0xE0, 0x01 },
	[UID_LOCKINGRANGE_ACE_WRLOCKED] =
	{ 0x00, 0x00, 0x00, 0x08, 0x00, 0x03, 0xE8, 0x01 },
	[UID_MBRCONTROL] =
	{ 0x00, 0x00, 0x08, 0x03, 0x00, 0x00, 0x00, 0x01 },
	[UID_MBR] =
	{ 0x00, 0x00, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00 },
	[UID_AUTHORITY_TABLE] =
	{ 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00},
	[UID_C_PIN_TABLE] =
	{ 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x00},
	[UID_LOCKING_INFO_TABLE] =
	{ 0x00, 0x00, 0x08, 0x01, 0x00, 0x00, 0x00, 0x01 },
	[UID_PSID] =
	{ 0x00, 0x00, 0x00, 0x09, 0x00, 0x01, 0xff, 0x01 },

	/* C_PIN_TABLE object ID's */
	[UID_C_PIN_MSID] =
	{ 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x84, 0x02},
	[UID_C_PIN_SID] =
	{ 0x00, 0x00, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x01},
	[UID_C_PIN_ADMIN1] =
	{ 0x00, 0x00, 0x00, 0x0B, 0x00, 0x01, 0x00, 0x01},
	[UID_C_PIN_USER1] =
	{ 0x00, 0x00, 0x00, 0x0B, 0x00, 0x03, 0x00, 0x01},

	/* half UID's (only first 4 bytes used) */
	[UID_HALF_AUTHORITY_OBJ_REF] =
	{ 0x00, 0x00, 0x0C, 0x05, 0xff, 0xff, 0xff, 0xff },
	[UID_HALF_BOOLEAN_ACE] =
	{ 0x00, 0x00, 0x04, 0x0E, 0xff, 0xff, 0xff, 0xff },
};

/*
 * TCG Storage SSC Methods.
 */
const uint8_t spdk_opal_method[][OPAL_UID_LENGTH] = {
	[PROPERTIES_METHOD] =
	{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x01 },
	[STARTSESSION_METHOD] =
	{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x02 },
	[REVERT_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x02, 0x02 },
	[ACTIVATE_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x02, 0x03 },
	[NEXT_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x08 },
	[GETACL_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x0d },
	[GENKEY_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x10 },
	[REVERTSP_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x11 },
	[GET_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x16 },
	[SET_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x17 },
	[AUTHENTICATE_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x1c },
	[RANDOM_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x06, 0x01 },
	[ERASE_METHOD] =
	{ 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x08, 0x03 },
};

/*
 * Response token
 */
struct spdk_opal_resp_token {
	const uint8_t *pos;
	uint8_t _padding[7];
	union {
		uint64_t unsigned_num;
		int64_t signed_num;
	} stored;
	size_t len; /* header + data */
	enum opal_token_type type;
	enum opal_atom_width width;
};

struct spdk_opal_resp_parsed {
	int num;
	struct spdk_opal_resp_token resp_tokens[MAX_TOKS];
};

/* header of a response */
struct spdk_opal_header {
	struct spdk_opal_compacket com_packet;
	struct spdk_opal_packet packet;
	struct spdk_opal_data_subpacket sub_packet;
};

struct opal_session;
struct spdk_opal_dev;

typedef void (*opal_sess_cb)(struct opal_session *sess, int status, void *ctx);

struct opal_session {
	uint32_t hsn;
	uint32_t tsn;
	size_t cmd_pos;
	uint8_t cmd[IO_BUFFER_LENGTH];
	uint8_t resp[IO_BUFFER_LENGTH];
	struct spdk_opal_resp_parsed parsed_resp;

	opal_sess_cb sess_cb;
	void *cb_arg;
	bool done;
	int status;
	struct spdk_opal_dev *dev;
};

struct spdk_opal_dev {
	struct spdk_nvme_ctrlr *ctrlr;

	uint16_t comid;

	struct spdk_opal_d0_features_info feat_info;

	uint8_t max_ranges; /* max locking range number */
	struct spdk_opal_locking_range_info locking_ranges[SPDK_OPAL_MAX_LOCKING_RANGE];
};

#endif
