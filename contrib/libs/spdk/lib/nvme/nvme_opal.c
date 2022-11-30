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
#include "spdk/opal.h"
#include "spdk/log.h"
#include "spdk/util.h"

#include "nvme_opal_internal.h"

static void
opal_nvme_security_recv_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct opal_session *sess = arg;
	struct spdk_opal_dev *dev = sess->dev;
	void *response = sess->resp;
	struct spdk_opal_compacket *header = response;
	int ret;

	if (spdk_nvme_cpl_is_error(cpl)) {
		sess->sess_cb(sess, -EIO, sess->cb_arg);
		return;
	}

	if (!header->outstanding_data && !header->min_transfer) {
		sess->sess_cb(sess, 0, sess->cb_arg);
		return;
	}

	memset(response, 0, IO_BUFFER_LENGTH);
	ret = spdk_nvme_ctrlr_cmd_security_receive(dev->ctrlr, SPDK_SCSI_SECP_TCG,
			dev->comid, 0, sess->resp, IO_BUFFER_LENGTH,
			opal_nvme_security_recv_done, sess);
	if (ret) {
		sess->sess_cb(sess, ret, sess->cb_arg);
	}
}

static void
opal_nvme_security_send_done(void *arg, const struct spdk_nvme_cpl *cpl)
{
	struct opal_session *sess = arg;
	struct spdk_opal_dev *dev = sess->dev;
	int ret;

	if (spdk_nvme_cpl_is_error(cpl)) {
		sess->sess_cb(sess, -EIO, sess->cb_arg);
		return;
	}

	ret = spdk_nvme_ctrlr_cmd_security_receive(dev->ctrlr, SPDK_SCSI_SECP_TCG,
			dev->comid, 0, sess->resp, IO_BUFFER_LENGTH,
			opal_nvme_security_recv_done, sess);
	if (ret) {
		sess->sess_cb(sess, ret, sess->cb_arg);
	}
}

static int
opal_nvme_security_send(struct spdk_opal_dev *dev, struct opal_session *sess,
			opal_sess_cb sess_cb, void *cb_arg)
{
	sess->sess_cb = sess_cb;
	sess->cb_arg = cb_arg;

	return spdk_nvme_ctrlr_cmd_security_send(dev->ctrlr, SPDK_SCSI_SECP_TCG, dev->comid,
			0, sess->cmd, IO_BUFFER_LENGTH,
			opal_nvme_security_send_done, sess);
}

static void
opal_send_recv_done(struct opal_session *sess, int status, void *ctx)
{
	sess->status = status;
	sess->done = true;
}

static int
opal_send_recv(struct spdk_opal_dev *dev, struct opal_session *sess)
{
	int ret;

	sess->done = false;
	ret = opal_nvme_security_send(dev, sess, opal_send_recv_done, NULL);
	if (ret) {
		return ret;
	}

	while (!sess->done) {
		spdk_nvme_ctrlr_process_admin_completions(dev->ctrlr);
	}

	return sess->status;
}

static struct opal_session *
opal_alloc_session(struct spdk_opal_dev *dev)
{
	struct opal_session *sess;

	sess = calloc(1, sizeof(*sess));
	if (!sess) {
		return NULL;
	}
	sess->dev = dev;

	return sess;
}

static void
opal_add_token_u8(int *err, struct opal_session *sess, uint8_t token)
{
	if (*err) {
		return;
	}
	if (sess->cmd_pos >= IO_BUFFER_LENGTH - 1) {
		SPDK_ERRLOG("Error adding u8: end of buffer.\n");
		*err = -ERANGE;
		return;
	}
	sess->cmd[sess->cmd_pos++] = token;
}

static void
opal_add_short_atom_header(struct opal_session *sess, bool bytestring,
			   bool has_sign, size_t len)
{
	uint8_t atom;
	int err = 0;

	atom = SPDK_SHORT_ATOM_ID;
	atom |= bytestring ? SPDK_SHORT_ATOM_BYTESTRING_FLAG : 0;
	atom |= has_sign ? SPDK_SHORT_ATOM_SIGN_FLAG : 0;
	atom |= len & SPDK_SHORT_ATOM_LEN_MASK;

	opal_add_token_u8(&err, sess, atom);
}

static void
opal_add_medium_atom_header(struct opal_session *sess, bool bytestring,
			    bool has_sign, size_t len)
{
	uint8_t header;

	header = SPDK_MEDIUM_ATOM_ID;
	header |= bytestring ? SPDK_MEDIUM_ATOM_BYTESTRING_FLAG : 0;
	header |= has_sign ? SPDK_MEDIUM_ATOM_SIGN_FLAG : 0;
	header |= (len >> 8) & SPDK_MEDIUM_ATOM_LEN_MASK;
	sess->cmd[sess->cmd_pos++] = header;
	sess->cmd[sess->cmd_pos++] = len;
}

static void
opal_add_token_bytestring(int *err, struct opal_session *sess,
			  const uint8_t *bytestring, size_t len)
{
	size_t header_len = 1;
	bool is_short_atom = true;

	if (*err) {
		return;
	}

	if (len & ~SPDK_SHORT_ATOM_LEN_MASK) {
		header_len = 2;
		is_short_atom = false;
	}

	if (len >= IO_BUFFER_LENGTH - sess->cmd_pos - header_len) {
		SPDK_ERRLOG("Error adding bytestring: end of buffer.\n");
		*err = -ERANGE;
		return;
	}

	if (is_short_atom) {
		opal_add_short_atom_header(sess, true, false, len);
	} else {
		opal_add_medium_atom_header(sess, true, false, len);
	}

	memcpy(&sess->cmd[sess->cmd_pos], bytestring, len);
	sess->cmd_pos += len;
}

static void
opal_add_token_u64(int *err, struct opal_session *sess, uint64_t number)
{
	int startat = 0;

	if (*err) {
		return;
	}

	/* add header first */
	if (number <= SPDK_TINY_ATOM_DATA_MASK) {
		sess->cmd[sess->cmd_pos++] = (uint8_t) number & SPDK_TINY_ATOM_DATA_MASK;
	} else {
		if (number < 0x100) {
			sess->cmd[sess->cmd_pos++] = 0x81; /* short atom, 1 byte length */
			startat = 0;
		} else if (number < 0x10000) {
			sess->cmd[sess->cmd_pos++] = 0x82; /* short atom, 2 byte length */
			startat = 1;
		} else if (number < 0x100000000) {
			sess->cmd[sess->cmd_pos++] = 0x84; /* short atom, 4 byte length */
			startat = 3;
		} else {
			sess->cmd[sess->cmd_pos++] = 0x88; /* short atom, 8 byte length */
			startat = 7;
		}

		/* add number value */
		for (int i = startat; i > -1; i--) {
			sess->cmd[sess->cmd_pos++] = (uint8_t)((number >> (i * 8)) & 0xff);
		}
	}
}

static void
opal_add_tokens(int *err, struct opal_session *sess, int num, ...)
{
	int i;
	va_list args_ptr;
	enum spdk_opal_token tmp;

	va_start(args_ptr, num);

	for (i = 0; i < num; i++) {
		tmp = va_arg(args_ptr, enum spdk_opal_token);
		opal_add_token_u8(err, sess, tmp);
		if (*err != 0) { break; }
	}

	va_end(args_ptr);
}

static int
opal_cmd_finalize(struct opal_session *sess, uint32_t hsn, uint32_t tsn, bool eod)
{
	struct spdk_opal_header *hdr;
	int err = 0;

	if (eod) {
		opal_add_tokens(&err, sess, 6, SPDK_OPAL_ENDOFDATA,
				SPDK_OPAL_STARTLIST,
				0, 0, 0,
				SPDK_OPAL_ENDLIST);
	}

	if (err) {
		SPDK_ERRLOG("Error finalizing command.\n");
		return -EFAULT;
	}

	hdr = (struct spdk_opal_header *)sess->cmd;

	to_be32(&hdr->packet.session_tsn, tsn);
	to_be32(&hdr->packet.session_hsn, hsn);

	to_be32(&hdr->sub_packet.length, sess->cmd_pos - sizeof(*hdr));
	while (sess->cmd_pos % 4) {
		if (sess->cmd_pos >= IO_BUFFER_LENGTH) {
			SPDK_ERRLOG("Error: Buffer overrun\n");
			return -ERANGE;
		}
		sess->cmd[sess->cmd_pos++] = 0;
	}
	to_be32(&hdr->packet.length, sess->cmd_pos - sizeof(hdr->com_packet) -
		sizeof(hdr->packet));
	to_be32(&hdr->com_packet.length, sess->cmd_pos - sizeof(hdr->com_packet));

	return 0;
}

static size_t
opal_response_parse_tiny(struct spdk_opal_resp_token *token,
			 const uint8_t *pos)
{
	token->pos = pos;
	token->len = 1;
	token->width = OPAL_WIDTH_TINY;

	if (pos[0] & SPDK_TINY_ATOM_SIGN_FLAG) {
		token->type = OPAL_DTA_TOKENID_SINT;
	} else {
		token->type = OPAL_DTA_TOKENID_UINT;
		token->stored.unsigned_num = pos[0] & SPDK_TINY_ATOM_DATA_MASK;
	}

	return token->len;
}

static int
opal_response_parse_short(struct spdk_opal_resp_token *token,
			  const uint8_t *pos)
{
	token->pos = pos;
	token->len = (pos[0] & SPDK_SHORT_ATOM_LEN_MASK) + 1; /* plus 1-byte header */
	token->width = OPAL_WIDTH_SHORT;

	if (pos[0] & SPDK_SHORT_ATOM_BYTESTRING_FLAG) {
		token->type = OPAL_DTA_TOKENID_BYTESTRING;
	} else if (pos[0] & SPDK_SHORT_ATOM_SIGN_FLAG) {
		token->type = OPAL_DTA_TOKENID_SINT;
	} else {
		uint64_t u_integer = 0;
		size_t i, b = 0;

		token->type = OPAL_DTA_TOKENID_UINT;
		if (token->len > 9) {
			SPDK_ERRLOG("uint64 with more than 8 bytes\n");
			return -EINVAL;
		}
		for (i = token->len - 1; i > 0; i--) {
			u_integer |= ((uint64_t)pos[i] << (8 * b));
			b++;
		}
		token->stored.unsigned_num = u_integer;
	}

	return token->len;
}

static size_t
opal_response_parse_medium(struct spdk_opal_resp_token *token,
			   const uint8_t *pos)
{
	token->pos = pos;
	token->len = (((pos[0] & SPDK_MEDIUM_ATOM_LEN_MASK) << 8) | pos[1]) + 2; /* plus 2-byte header */
	token->width = OPAL_WIDTH_MEDIUM;

	if (pos[0] & SPDK_MEDIUM_ATOM_BYTESTRING_FLAG) {
		token->type = OPAL_DTA_TOKENID_BYTESTRING;
	} else if (pos[0] & SPDK_MEDIUM_ATOM_SIGN_FLAG) {
		token->type = OPAL_DTA_TOKENID_SINT;
	} else {
		token->type = OPAL_DTA_TOKENID_UINT;
	}

	return token->len;
}

static size_t
opal_response_parse_long(struct spdk_opal_resp_token *token,
			 const uint8_t *pos)
{
	token->pos = pos;
	token->len = ((pos[1] << 16) | (pos[2] << 8) | pos[3]) + 4; /* plus 4-byte header */
	token->width = OPAL_WIDTH_LONG;

	if (pos[0] & SPDK_LONG_ATOM_BYTESTRING_FLAG) {
		token->type = OPAL_DTA_TOKENID_BYTESTRING;
	} else if (pos[0] & SPDK_LONG_ATOM_SIGN_FLAG) {
		token->type = OPAL_DTA_TOKENID_SINT;
	} else {
		token->type = OPAL_DTA_TOKENID_UINT;
	}

	return token->len;
}

static size_t
opal_response_parse_token(struct spdk_opal_resp_token *token,
			  const uint8_t *pos)
{
	token->pos = pos;
	token->len = 1;
	token->type = OPAL_DTA_TOKENID_TOKEN;
	token->width = OPAL_WIDTH_TOKEN;

	return token->len;
}

static int
opal_response_parse(const uint8_t *buf, size_t length,
		    struct spdk_opal_resp_parsed *resp)
{
	const struct spdk_opal_header *hdr;
	struct spdk_opal_resp_token *token_iter;
	int num_entries = 0;
	int total;
	size_t token_length;
	const uint8_t *pos;
	uint32_t clen, plen, slen;

	if (!buf || !resp) {
		return -EINVAL;
	}

	hdr = (struct spdk_opal_header *)buf;
	pos = buf + sizeof(*hdr);

	clen = from_be32(&hdr->com_packet.length);
	plen = from_be32(&hdr->packet.length);
	slen = from_be32(&hdr->sub_packet.length);
	SPDK_DEBUGLOG(opal, "Response size: cp: %u, pkt: %u, subpkt: %u\n",
		      clen, plen, slen);

	if (clen == 0 || plen == 0 || slen == 0 ||
	    slen > IO_BUFFER_LENGTH - sizeof(*hdr)) {
		SPDK_ERRLOG("Bad header length. cp: %u, pkt: %u, subpkt: %u\n",
			    clen, plen, slen);
		return -EINVAL;
	}

	if (pos > buf + length) {
		SPDK_ERRLOG("Pointer out of range\n");
		return -EFAULT;
	}

	token_iter = resp->resp_tokens;
	total = slen;

	while (total > 0) {
		if (pos[0] <= SPDK_TINY_ATOM_TYPE_MAX) { /* tiny atom */
			token_length = opal_response_parse_tiny(token_iter, pos);
		} else if (pos[0] <= SPDK_SHORT_ATOM_TYPE_MAX) { /* short atom */
			token_length = opal_response_parse_short(token_iter, pos);
		} else if (pos[0] <= SPDK_MEDIUM_ATOM_TYPE_MAX) { /* medium atom */
			token_length = opal_response_parse_medium(token_iter, pos);
		} else if (pos[0] <= SPDK_LONG_ATOM_TYPE_MAX) { /* long atom */
			token_length = opal_response_parse_long(token_iter, pos);
		} else { /* TOKEN */
			token_length = opal_response_parse_token(token_iter, pos);
		}

		if (token_length <= 0) {
			SPDK_ERRLOG("Parse response failure.\n");
			return -EINVAL;
		}

		pos += token_length;
		total -= token_length;
		token_iter++;
		num_entries++;

		if (total < 0) {
			SPDK_ERRLOG("Length not matching.\n");
			return -EINVAL;
		}
	}

	if (num_entries == 0) {
		SPDK_ERRLOG("Couldn't parse response.\n");
		return -EINVAL;
	}
	resp->num = num_entries;

	return 0;
}

static inline bool
opal_response_token_matches(const struct spdk_opal_resp_token *token,
			    uint8_t match)
{
	if (!token ||
	    token->type != OPAL_DTA_TOKENID_TOKEN ||
	    token->pos[0] != match) {
		return false;
	}
	return true;
}

static const struct spdk_opal_resp_token *
opal_response_get_token(const struct spdk_opal_resp_parsed *resp, int index)
{
	const struct spdk_opal_resp_token *token;

	if (index >= resp->num) {
		SPDK_ERRLOG("Token number doesn't exist: %d, resp: %d\n",
			    index, resp->num);
		return NULL;
	}

	token = &resp->resp_tokens[index];
	if (token->len == 0) {
		SPDK_ERRLOG("Token length must be non-zero\n");
		return NULL;
	}

	return token;
}

static uint64_t
opal_response_get_u64(const struct spdk_opal_resp_parsed *resp, int index)
{
	if (!resp) {
		SPDK_ERRLOG("Response is NULL\n");
		return 0;
	}

	if (resp->resp_tokens[index].type != OPAL_DTA_TOKENID_UINT) {
		SPDK_ERRLOG("Token is not unsigned int: %d\n",
			    resp->resp_tokens[index].type);
		return 0;
	}

	if (!(resp->resp_tokens[index].width == OPAL_WIDTH_TINY ||
	      resp->resp_tokens[index].width == OPAL_WIDTH_SHORT)) {
		SPDK_ERRLOG("Atom is not short or tiny: %d\n",
			    resp->resp_tokens[index].width);
		return 0;
	}

	return resp->resp_tokens[index].stored.unsigned_num;
}

static uint16_t
opal_response_get_u16(const struct spdk_opal_resp_parsed *resp, int index)
{
	uint64_t i = opal_response_get_u64(resp, index);
	if (i > 0xffffull) {
		SPDK_ERRLOG("parse reponse u16 failed. Overflow\n");
		return 0;
	}
	return (uint16_t) i;
}

static uint8_t
opal_response_get_u8(const struct spdk_opal_resp_parsed *resp, int index)
{
	uint64_t i = opal_response_get_u64(resp, index);
	if (i > 0xffull) {
		SPDK_ERRLOG("parse reponse u8 failed. Overflow\n");
		return 0;
	}
	return (uint8_t) i;
}

static size_t
opal_response_get_string(const struct spdk_opal_resp_parsed *resp, int n,
			 const char **store)
{
	uint8_t header_len;
	struct spdk_opal_resp_token token;
	*store = NULL;
	if (!resp) {
		SPDK_ERRLOG("Response is NULL\n");
		return 0;
	}

	if (n > resp->num) {
		SPDK_ERRLOG("Response has %d tokens. Can't access %d\n",
			    resp->num, n);
		return 0;
	}

	token = resp->resp_tokens[n];
	if (token.type != OPAL_DTA_TOKENID_BYTESTRING) {
		SPDK_ERRLOG("Token is not a byte string!\n");
		return 0;
	}

	switch (token.width) {
	case OPAL_WIDTH_SHORT:
		header_len = 1;
		break;
	case OPAL_WIDTH_MEDIUM:
		header_len = 2;
		break;
	case OPAL_WIDTH_LONG:
		header_len = 4;
		break;
	default:
		SPDK_ERRLOG("Can't get string from this Token\n");
		return 0;
	}

	*store = token.pos + header_len;
	return token.len - header_len;
}

static int
opal_response_status(const struct spdk_opal_resp_parsed *resp)
{
	const struct spdk_opal_resp_token *tok;

	/* if we get an EOS token, just return 0 */
	tok = opal_response_get_token(resp, 0);
	if (opal_response_token_matches(tok, SPDK_OPAL_ENDOFSESSION)) {
		return 0;
	}

	if (resp->num < 5) {
		return SPDK_DTAERROR_NO_METHOD_STATUS;
	}

	tok = opal_response_get_token(resp, resp->num - 5); /* the first token should be STARTLIST */
	if (!opal_response_token_matches(tok, SPDK_OPAL_STARTLIST)) {
		return SPDK_DTAERROR_NO_METHOD_STATUS;
	}

	tok = opal_response_get_token(resp, resp->num - 1); /* the last token should be ENDLIST */
	if (!opal_response_token_matches(tok, SPDK_OPAL_ENDLIST)) {
		return SPDK_DTAERROR_NO_METHOD_STATUS;
	}

	/* The second and third values in the status list are reserved, and are
	defined in core spec to be 0x00 and 0x00 and SHOULD be ignored by the host. */
	return (int)opal_response_get_u64(resp,
					  resp->num - 4); /* We only need the first value in the status list. */
}

static int
opal_parse_and_check_status(struct opal_session *sess)
{
	int error;

	error = opal_response_parse(sess->resp, IO_BUFFER_LENGTH, &sess->parsed_resp);
	if (error) {
		SPDK_ERRLOG("Couldn't parse response.\n");
		return error;
	}
	return opal_response_status(&sess->parsed_resp);
}

static inline void
opal_clear_cmd(struct opal_session *sess)
{
	sess->cmd_pos = sizeof(struct spdk_opal_header);
	memset(sess->cmd, 0, IO_BUFFER_LENGTH);
}

static inline void
opal_set_comid(struct opal_session *sess, uint16_t comid)
{
	struct spdk_opal_header *hdr = (struct spdk_opal_header *)sess->cmd;

	hdr->com_packet.comid[0] = comid >> 8;
	hdr->com_packet.comid[1] = comid;
	hdr->com_packet.extended_comid[0] = 0;
	hdr->com_packet.extended_comid[1] = 0;
}

static inline int
opal_init_key(struct spdk_opal_key *opal_key, const char *passwd)
{
	int len;

	if (passwd == NULL || passwd[0] == '\0') {
		SPDK_ERRLOG("Password is empty. Create key failed\n");
		return -EINVAL;
	}

	len = strlen(passwd);

	if (len >= OPAL_KEY_MAX) {
		SPDK_ERRLOG("Password too long. Create key failed\n");
		return -EINVAL;
	}

	opal_key->key_len = len;
	memcpy(opal_key->key, passwd, opal_key->key_len);

	return 0;
}

static void
opal_build_locking_range(uint8_t *buffer, uint8_t locking_range)
{
	memcpy(buffer, spdk_opal_uid[UID_LOCKINGRANGE_GLOBAL], OPAL_UID_LENGTH);

	/* global */
	if (locking_range == 0) {
		return;
	}

	/* non-global */
	buffer[5] = LOCKING_RANGE_NON_GLOBAL;
	buffer[7] = locking_range;
}

static void
opal_check_tper(struct spdk_opal_dev *dev, const void *data)
{
	const struct spdk_opal_d0_tper_feat *tper = data;

	dev->feat_info.tper = *tper;
}

/*
 * check single user mode
 */
static bool
opal_check_sum(struct spdk_opal_dev *dev, const void *data)
{
	const struct spdk_opal_d0_single_user_mode_feat *sum = data;
	uint32_t num_locking_objects = from_be32(&sum->num_locking_objects);

	if (num_locking_objects == 0) {
		SPDK_NOTICELOG("Need at least one locking object.\n");
		return false;
	}

	dev->feat_info.single_user = *sum;

	return true;
}

static void
opal_check_lock(struct spdk_opal_dev *dev, const void *data)
{
	const struct spdk_opal_d0_locking_feat *lock = data;

	dev->feat_info.locking = *lock;
}

static void
opal_check_geometry(struct spdk_opal_dev *dev, const void *data)
{
	const struct spdk_opal_d0_geo_feat *geo = data;

	dev->feat_info.geo = *geo;
}

static void
opal_check_datastore(struct spdk_opal_dev *dev, const void *data)
{
	const struct spdk_opal_d0_datastore_feat *datastore = data;

	dev->feat_info.datastore = *datastore;
}

static uint16_t
opal_get_comid_v100(struct spdk_opal_dev *dev, const void *data)
{
	const struct spdk_opal_d0_v100_feat *v100 = data;
	uint16_t base_comid = from_be16(&v100->base_comid);

	dev->feat_info.v100 = *v100;

	return base_comid;
}

static uint16_t
opal_get_comid_v200(struct spdk_opal_dev *dev, const void *data)
{
	const struct spdk_opal_d0_v200_feat *v200 = data;
	uint16_t base_comid = from_be16(&v200->base_comid);

	dev->feat_info.v200 = *v200;

	return base_comid;
}

static int
opal_discovery0_end(struct spdk_opal_dev *dev, void *payload, uint32_t payload_size)
{
	bool supported = false, single_user = false;
	const struct spdk_opal_d0_hdr *hdr = (struct spdk_opal_d0_hdr *)payload;
	struct spdk_opal_d0_feat_hdr *feat_hdr;
	const uint8_t *epos = payload, *cpos = payload;
	uint16_t comid = 0;
	uint32_t hlen = from_be32(&(hdr->length));

	if (hlen > payload_size - sizeof(*hdr)) {
		SPDK_ERRLOG("Discovery length overflows buffer (%zu+%u)/%u\n",
			    sizeof(*hdr), hlen, payload_size);
		return -EFAULT;
	}

	epos += hlen; /* end of buffer */
	cpos += sizeof(*hdr); /* current position on buffer */

	while (cpos < epos) {
		feat_hdr = (struct spdk_opal_d0_feat_hdr *)cpos;
		uint16_t feat_code = from_be16(&feat_hdr->code);

		switch (feat_code) {
		case FEATURECODE_TPER:
			opal_check_tper(dev, cpos);
			break;
		case FEATURECODE_SINGLEUSER:
			single_user = opal_check_sum(dev, cpos);
			break;
		case FEATURECODE_GEOMETRY:
			opal_check_geometry(dev, cpos);
			break;
		case FEATURECODE_LOCKING:
			opal_check_lock(dev, cpos);
			break;
		case FEATURECODE_DATASTORE:
			opal_check_datastore(dev, cpos);
			break;
		case FEATURECODE_OPALV100:
			comid = opal_get_comid_v100(dev, cpos);
			supported = true;
			break;
		case FEATURECODE_OPALV200:
			comid = opal_get_comid_v200(dev, cpos);
			supported = true;
			break;
		default:
			SPDK_INFOLOG(opal, "Unknow feature code: %d\n", feat_code);
		}
		cpos += feat_hdr->length + sizeof(*feat_hdr);
	}

	if (supported == false) {
		SPDK_ERRLOG("Opal Not Supported.\n");
		return -ENOTSUP;
	}

	if (single_user == false) {
		SPDK_INFOLOG(opal, "Single User Mode Not Supported\n");
	}

	dev->comid = comid;
	return 0;
}

static int
opal_discovery0(struct spdk_opal_dev *dev, void *payload, uint32_t payload_size)
{
	int ret;

	ret = spdk_nvme_ctrlr_security_receive(dev->ctrlr, SPDK_SCSI_SECP_TCG, LV0_DISCOVERY_COMID,
					       0, payload, payload_size);
	if (ret) {
		return ret;
	}

	return opal_discovery0_end(dev, payload, payload_size);
}

static int
opal_end_session(struct spdk_opal_dev *dev, struct opal_session *sess, uint16_t comid)
{
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, comid);
	opal_add_token_u8(&err, sess, SPDK_OPAL_ENDOFSESSION);

	if (err < 0) {
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, false);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	sess->hsn = 0;
	sess->tsn = 0;

	return opal_parse_and_check_status(sess);
}

void
spdk_opal_dev_destruct(struct spdk_opal_dev *dev)
{
	free(dev);
}

static int
opal_start_session_done(struct opal_session *sess)
{
	uint32_t hsn, tsn;
	int error = 0;

	error = opal_parse_and_check_status(sess);
	if (error) {
		return error;
	}

	hsn = opal_response_get_u64(&sess->parsed_resp, 4);
	tsn = opal_response_get_u64(&sess->parsed_resp, 5);

	if (hsn == 0 && tsn == 0) {
		SPDK_ERRLOG("Couldn't authenticate session\n");
		return -EPERM;
	}

	sess->hsn = hsn;
	sess->tsn = tsn;

	return 0;
}

static int
opal_start_generic_session(struct spdk_opal_dev *dev,
			   struct opal_session *sess,
			   enum opal_uid_enum auth,
			   enum opal_uid_enum sp_type,
			   const char *key,
			   uint8_t key_len)
{
	uint32_t hsn;
	int err = 0;
	int ret;

	if (key == NULL && auth != UID_ANYBODY) {
		return OPAL_INVAL_PARAM;
	}

	opal_clear_cmd(sess);

	opal_set_comid(sess, dev->comid);
	hsn = GENERIC_HOST_SESSION_NUM;

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_SMUID],
				  OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[STARTSESSION_METHOD],
				  OPAL_UID_LENGTH);
	opal_add_token_u8(&err, sess, SPDK_OPAL_STARTLIST);
	opal_add_token_u64(&err, sess, hsn);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[sp_type], OPAL_UID_LENGTH);
	opal_add_token_u8(&err, sess, SPDK_OPAL_TRUE); /* Write */

	switch (auth) {
	case UID_ANYBODY:
		opal_add_token_u8(&err, sess, SPDK_OPAL_ENDLIST);
		break;
	case UID_ADMIN1:
	case UID_SID:
		opal_add_token_u8(&err, sess, SPDK_OPAL_STARTNAME);
		opal_add_token_u8(&err, sess, 0); /* HostChallenge */
		opal_add_token_bytestring(&err, sess, key, key_len);
		opal_add_tokens(&err, sess, 3,    /* number of token */
				SPDK_OPAL_ENDNAME,
				SPDK_OPAL_STARTNAME,
				3);/* HostSignAuth */
		opal_add_token_bytestring(&err, sess, spdk_opal_uid[auth],
					  OPAL_UID_LENGTH);
		opal_add_token_u8(&err, sess, SPDK_OPAL_ENDNAME);
		opal_add_token_u8(&err, sess, SPDK_OPAL_ENDLIST);
		break;
	default:
		SPDK_ERRLOG("Cannot start Admin SP session with auth %d\n", auth);
		return -EINVAL;
	}

	if (err) {
		SPDK_ERRLOG("Error building start adminsp session command.\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_start_session_done(sess);
}

static int
opal_get_msid_cpin_pin_done(struct opal_session *sess,
			    struct spdk_opal_key *opal_key)
{
	const char *msid_pin;
	size_t strlen;
	int error = 0;

	error = opal_parse_and_check_status(sess);
	if (error) {
		return error;
	}

	strlen = opal_response_get_string(&sess->parsed_resp, 4, &msid_pin);
	if (!msid_pin) {
		SPDK_ERRLOG("Couldn't extract PIN from response\n");
		return -EINVAL;
	}

	opal_key->key_len = strlen;
	memcpy(opal_key->key, msid_pin, opal_key->key_len);

	SPDK_DEBUGLOG(opal, "MSID = %p\n", opal_key->key);
	return 0;
}

static int
opal_get_msid_cpin_pin(struct spdk_opal_dev *dev, struct opal_session *sess,
		       struct spdk_opal_key *opal_key)
{
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_C_PIN_MSID],
				  OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[GET_METHOD], OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 12, SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_STARTCOLUMN,
			SPDK_OPAL_PIN,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_ENDCOLUMN,
			SPDK_OPAL_PIN,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error building Get MSID CPIN PIN command.\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_get_msid_cpin_pin_done(sess, opal_key);
}

static int
opal_build_generic_pw_cmd(struct opal_session *sess, uint8_t *key, size_t key_len,
			  uint8_t *cpin_uid, struct spdk_opal_dev *dev)
{
	int err = 0;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, cpin_uid, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[SET_METHOD],
				  OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 6,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_VALUES,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_PIN);
	opal_add_token_bytestring(&err, sess, key, key_len);
	opal_add_tokens(&err, sess, 4,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST);
	if (err) {
		return err;
	}

	return opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
}

static int
opal_get_locking_sp_lifecycle_done(struct opal_session *sess)
{
	uint8_t lifecycle;
	int error = 0;

	error = opal_parse_and_check_status(sess);
	if (error) {
		return error;
	}

	lifecycle = opal_response_get_u64(&sess->parsed_resp, 4);
	if (lifecycle != OPAL_MANUFACTURED_INACTIVE) { /* status before activate */
		SPDK_ERRLOG("Couldn't determine the status of the Lifecycle state\n");
		return -EINVAL;
	}

	return 0;
}

static int
opal_get_locking_sp_lifecycle(struct spdk_opal_dev *dev, struct opal_session *sess)
{
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_LOCKINGSP],
				  OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[GET_METHOD], OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 12, SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_STARTCOLUMN,
			SPDK_OPAL_LIFECYCLE,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_ENDCOLUMN,
			SPDK_OPAL_LIFECYCLE,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error Building GET Lifecycle Status command\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_get_locking_sp_lifecycle_done(sess);
}

static int
opal_activate(struct spdk_opal_dev *dev, struct opal_session *sess)
{
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_LOCKINGSP],
				  OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[ACTIVATE_METHOD],
				  OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 2, SPDK_OPAL_STARTLIST, SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error building Activate LockingSP command.\n");
		return err;
	}

	/* TODO: Single User Mode for activatation */

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

static int
opal_start_auth_session(struct spdk_opal_dev *dev,
			struct opal_session *sess,
			enum spdk_opal_user user,
			struct spdk_opal_key *opal_key)
{
	uint8_t uid_user[OPAL_UID_LENGTH];
	int err = 0;
	int ret;
	uint32_t hsn = GENERIC_HOST_SESSION_NUM;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	if (user != OPAL_ADMIN1) {
		memcpy(uid_user, spdk_opal_uid[UID_USER1], OPAL_UID_LENGTH);
		uid_user[7] = user;
	} else {
		memcpy(uid_user, spdk_opal_uid[UID_ADMIN1], OPAL_UID_LENGTH);
	}

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_SMUID],
				  OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[STARTSESSION_METHOD],
				  OPAL_UID_LENGTH);

	opal_add_token_u8(&err, sess, SPDK_OPAL_STARTLIST);
	opal_add_token_u64(&err, sess, hsn);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_LOCKINGSP],
				  OPAL_UID_LENGTH);
	opal_add_tokens(&err, sess, 3, SPDK_OPAL_TRUE, SPDK_OPAL_STARTNAME,
			0); /* True for a Read-Write session  */
	opal_add_token_bytestring(&err, sess, opal_key->key, opal_key->key_len);
	opal_add_tokens(&err, sess, 3, SPDK_OPAL_ENDNAME, SPDK_OPAL_STARTNAME, 3); /* HostSignAuth */
	opal_add_token_bytestring(&err, sess, uid_user, OPAL_UID_LENGTH);
	opal_add_tokens(&err, sess, 2, SPDK_OPAL_ENDNAME, SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error building STARTSESSION command.\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_start_session_done(sess);
}

static int
opal_lock_unlock_range(struct spdk_opal_dev *dev, struct opal_session *sess,
		       enum spdk_opal_locking_range locking_range,
		       enum spdk_opal_lock_state l_state)
{
	uint8_t uid_locking_range[OPAL_UID_LENGTH];
	uint8_t read_locked, write_locked;
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_build_locking_range(uid_locking_range, locking_range);

	switch (l_state) {
	case OPAL_READONLY:
		read_locked = 0;
		write_locked = 1;
		break;
	case OPAL_READWRITE:
		read_locked = 0;
		write_locked = 0;
		break;
	case OPAL_RWLOCK:
		read_locked = 1;
		write_locked = 1;
		break;
	default:
		SPDK_ERRLOG("Tried to set an invalid locking state.\n");
		return -EINVAL;
	}

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, uid_locking_range, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[SET_METHOD], OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 15, SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_VALUES,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_READLOCKED,
			read_locked,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_WRITELOCKED,
			write_locked,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error building SET command.\n");
		return err;
	}
	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

static int opal_generic_locking_range_enable_disable(struct spdk_opal_dev *dev,
		struct opal_session *sess,
		uint8_t *uid, bool read_lock_enabled, bool write_lock_enabled)
{
	int err = 0;

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, uid, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[SET_METHOD], OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 23, SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_VALUES,
			SPDK_OPAL_STARTLIST,

			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_READLOCKENABLED,
			read_lock_enabled,
			SPDK_OPAL_ENDNAME,

			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_WRITELOCKENABLED,
			write_lock_enabled,
			SPDK_OPAL_ENDNAME,

			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_READLOCKED,
			0,
			SPDK_OPAL_ENDNAME,

			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_WRITELOCKED,
			0,
			SPDK_OPAL_ENDNAME,

			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST);
	if (err) {
		SPDK_ERRLOG("Error building locking range enable/disable command.\n");
	}
	return err;
}

static int
opal_setup_locking_range(struct spdk_opal_dev *dev, struct opal_session *sess,
			 enum spdk_opal_locking_range locking_range,
			 uint64_t range_start, uint64_t range_length,
			 bool read_lock_enabled, bool write_lock_enabled)
{
	uint8_t uid_locking_range[OPAL_UID_LENGTH];
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_build_locking_range(uid_locking_range, locking_range);

	if (locking_range == 0) {
		err = opal_generic_locking_range_enable_disable(dev, sess, uid_locking_range,
				read_lock_enabled, write_lock_enabled);
	} else {
		opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
		opal_add_token_bytestring(&err, sess, uid_locking_range, OPAL_UID_LENGTH);
		opal_add_token_bytestring(&err, sess, spdk_opal_method[SET_METHOD],
					  OPAL_UID_LENGTH);

		opal_add_tokens(&err, sess, 6,
				SPDK_OPAL_STARTLIST,
				SPDK_OPAL_STARTNAME,
				SPDK_OPAL_VALUES,
				SPDK_OPAL_STARTLIST,
				SPDK_OPAL_STARTNAME,
				SPDK_OPAL_RANGESTART);
		opal_add_token_u64(&err, sess, range_start);
		opal_add_tokens(&err, sess, 3,
				SPDK_OPAL_ENDNAME,
				SPDK_OPAL_STARTNAME,
				SPDK_OPAL_RANGELENGTH);
		opal_add_token_u64(&err, sess, range_length);
		opal_add_tokens(&err, sess, 3,
				SPDK_OPAL_ENDNAME,
				SPDK_OPAL_STARTNAME,
				SPDK_OPAL_READLOCKENABLED);
		opal_add_token_u64(&err, sess, read_lock_enabled);
		opal_add_tokens(&err, sess, 3,
				SPDK_OPAL_ENDNAME,
				SPDK_OPAL_STARTNAME,
				SPDK_OPAL_WRITELOCKENABLED);
		opal_add_token_u64(&err, sess, write_lock_enabled);
		opal_add_tokens(&err, sess, 4,
				SPDK_OPAL_ENDNAME,
				SPDK_OPAL_ENDLIST,
				SPDK_OPAL_ENDNAME,
				SPDK_OPAL_ENDLIST);
	}
	if (err) {
		SPDK_ERRLOG("Error building Setup Locking range command.\n");
		return err;

	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

static int
opal_get_max_ranges_done(struct opal_session *sess)
{
	int error = 0;

	error = opal_parse_and_check_status(sess);
	if (error) {
		return error;
	}

	/* "MaxRanges" is token 4 of response */
	return opal_response_get_u16(&sess->parsed_resp, 4);
}

static int
opal_get_max_ranges(struct spdk_opal_dev *dev, struct opal_session *sess)
{
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_LOCKING_INFO_TABLE],
				  OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[GET_METHOD], OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 12, SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_STARTCOLUMN,
			SPDK_OPAL_MAXRANGES,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_ENDCOLUMN,
			SPDK_OPAL_MAXRANGES,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error Building GET Lifecycle Status command\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_get_max_ranges_done(sess);
}

static int
opal_get_locking_range_info_done(struct opal_session *sess,
				 struct spdk_opal_locking_range_info *info)
{
	int error = 0;

	error = opal_parse_and_check_status(sess);
	if (error) {
		return error;
	}

	info->range_start = opal_response_get_u64(&sess->parsed_resp, 4);
	info->range_length = opal_response_get_u64(&sess->parsed_resp, 8);
	info->read_lock_enabled = opal_response_get_u8(&sess->parsed_resp, 12);
	info->write_lock_enabled = opal_response_get_u8(&sess->parsed_resp, 16);
	info->read_locked = opal_response_get_u8(&sess->parsed_resp, 20);
	info->write_locked = opal_response_get_u8(&sess->parsed_resp, 24);

	return 0;
}

static int
opal_get_locking_range_info(struct spdk_opal_dev *dev,
			    struct opal_session *sess,
			    enum spdk_opal_locking_range locking_range_id)
{
	int err = 0;
	int ret;
	uint8_t uid_locking_range[OPAL_UID_LENGTH];
	struct spdk_opal_locking_range_info *info;

	opal_build_locking_range(uid_locking_range, locking_range_id);

	assert(locking_range_id < SPDK_OPAL_MAX_LOCKING_RANGE);
	info = &dev->locking_ranges[locking_range_id];
	memset(info, 0, sizeof(*info));
	info->locking_range_id = locking_range_id;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, uid_locking_range, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[GET_METHOD], OPAL_UID_LENGTH);


	opal_add_tokens(&err, sess, 12, SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_STARTCOLUMN,
			SPDK_OPAL_RANGESTART,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_ENDCOLUMN,
			SPDK_OPAL_WRITELOCKED,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error Building get locking range info command\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_get_locking_range_info_done(sess, info);
}

static int
opal_enable_user(struct spdk_opal_dev *dev, struct opal_session *sess,
		 enum spdk_opal_user user)
{
	int err = 0;
	int ret;
	uint8_t uid_user[OPAL_UID_LENGTH];

	memcpy(uid_user, spdk_opal_uid[UID_USER1], OPAL_UID_LENGTH);
	uid_user[7] = user;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, uid_user, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[SET_METHOD], OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 11,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_VALUES,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_AUTH_ENABLE,
			SPDK_OPAL_TRUE,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error Building enable user command\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

static int
opal_add_user_to_locking_range(struct spdk_opal_dev *dev,
			       struct opal_session *sess,
			       enum spdk_opal_user user,
			       enum spdk_opal_locking_range locking_range,
			       enum spdk_opal_lock_state l_state)
{
	int err = 0;
	int ret;
	uint8_t uid_user[OPAL_UID_LENGTH];
	uint8_t uid_locking_range[OPAL_UID_LENGTH];

	memcpy(uid_user, spdk_opal_uid[UID_USER1], OPAL_UID_LENGTH);
	uid_user[7] = user;

	switch (l_state) {
	case OPAL_READONLY:
		memcpy(uid_locking_range, spdk_opal_uid[UID_LOCKINGRANGE_ACE_RDLOCKED], OPAL_UID_LENGTH);
		break;
	case OPAL_READWRITE:
		memcpy(uid_locking_range, spdk_opal_uid[UID_LOCKINGRANGE_ACE_WRLOCKED], OPAL_UID_LENGTH);
		break;
	default:
		SPDK_ERRLOG("locking state should only be OPAL_READONLY or OPAL_READWRITE\n");
		return -EINVAL;
	}

	uid_locking_range[7] = locking_range;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, uid_locking_range, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[SET_METHOD], OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 8,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_VALUES,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_BOOLEAN_EXPR,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_HALF_AUTHORITY_OBJ_REF],
				  OPAL_UID_LENGTH / 2);
	opal_add_token_bytestring(&err, sess, uid_user, OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 2, SPDK_OPAL_ENDNAME, SPDK_OPAL_STARTNAME);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_HALF_AUTHORITY_OBJ_REF],
				  OPAL_UID_LENGTH / 2);
	opal_add_token_bytestring(&err, sess, uid_user, OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 2, SPDK_OPAL_ENDNAME, SPDK_OPAL_STARTNAME);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_HALF_BOOLEAN_ACE], OPAL_UID_LENGTH / 2);
	opal_add_tokens(&err, sess, 7,
			SPDK_OPAL_TRUE,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST);
	if (err) {
		SPDK_ERRLOG("Error building add user to locking range command\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

static int
opal_new_user_passwd(struct spdk_opal_dev *dev, struct opal_session *sess,
		     enum spdk_opal_user user,
		     struct spdk_opal_key *opal_key)
{
	uint8_t uid_cpin[OPAL_UID_LENGTH];
	int ret;

	if (user == OPAL_ADMIN1) {
		memcpy(uid_cpin, spdk_opal_uid[UID_C_PIN_ADMIN1], OPAL_UID_LENGTH);
	} else {
		memcpy(uid_cpin, spdk_opal_uid[UID_C_PIN_USER1], OPAL_UID_LENGTH);
		uid_cpin[7] = user;
	}

	ret = opal_build_generic_pw_cmd(sess, opal_key->key, opal_key->key_len, uid_cpin, dev);
	if (ret != 0) {
		SPDK_ERRLOG("Error building set password command\n");
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

static int
opal_set_sid_cpin_pin(struct spdk_opal_dev *dev, struct opal_session *sess, char *new_passwd)
{
	uint8_t cpin_uid[OPAL_UID_LENGTH];
	struct spdk_opal_key opal_key = {};
	int ret;

	ret = opal_init_key(&opal_key, new_passwd);
	if (ret != 0) {
		return ret;
	}

	memcpy(cpin_uid, spdk_opal_uid[UID_C_PIN_SID], OPAL_UID_LENGTH);

	if (opal_build_generic_pw_cmd(sess, opal_key.key, opal_key.key_len, cpin_uid, dev)) {
		SPDK_ERRLOG("Error building Set SID cpin\n");
		return -ERANGE;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

int
spdk_opal_cmd_take_ownership(struct spdk_opal_dev *dev, char *new_passwd)
{
	int ret;
	struct spdk_opal_key opal_key = {};
	struct opal_session *sess;

	assert(dev != NULL);

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_generic_session(dev, sess, UID_ANYBODY, UID_ADMINSP, NULL, 0);
	if (ret) {
		SPDK_ERRLOG("start admin SP session error %d\n", ret);
		goto end;
	}

	ret = opal_get_msid_cpin_pin(dev, sess, &opal_key);
	if (ret) {
		SPDK_ERRLOG("get msid error %d\n", ret);
		opal_end_session(dev, sess, dev->comid);
		goto end;
	}

	ret = opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
		goto end;
	}

	/* reuse the session structure */
	memset(sess, 0, sizeof(*sess));
	sess->dev = dev;
	ret = opal_start_generic_session(dev, sess, UID_SID, UID_ADMINSP,
					 opal_key.key, opal_key.key_len);
	if (ret) {
		SPDK_ERRLOG("start admin SP session error %d\n", ret);
		goto end;
	}
	memset(&opal_key, 0, sizeof(struct spdk_opal_key));

	ret = opal_set_sid_cpin_pin(dev, sess, new_passwd);
	if (ret) {
		SPDK_ERRLOG("set cpin error %d\n", ret);
		opal_end_session(dev, sess, dev->comid);
		goto end;
	}

	ret = opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

end:
	free(sess);
	return ret;
}

struct spdk_opal_dev *
	spdk_opal_dev_construct(struct spdk_nvme_ctrlr *ctrlr)
{
	struct spdk_opal_dev *dev;
	void *payload;

	dev = calloc(1, sizeof(*dev));
	if (!dev) {
		SPDK_ERRLOG("Memory allocation failed\n");
		return NULL;
	}

	dev->ctrlr = ctrlr;

	payload = calloc(1, IO_BUFFER_LENGTH);
	if (!payload) {
		free(dev);
		return NULL;
	}

	if (opal_discovery0(dev, payload, IO_BUFFER_LENGTH)) {
		SPDK_INFOLOG(opal, "Opal is not supported on this device\n");
		free(dev);
		free(payload);
		return NULL;
	}

	free(payload);
	return dev;
}

static int
opal_build_revert_tper_cmd(struct spdk_opal_dev *dev, struct opal_session *sess)
{
	int err = 0;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, spdk_opal_uid[UID_ADMINSP],
				  OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[REVERT_METHOD],
				  OPAL_UID_LENGTH);
	opal_add_token_u8(&err, sess, SPDK_OPAL_STARTLIST);
	opal_add_token_u8(&err, sess, SPDK_OPAL_ENDLIST);
	if (err) {
		SPDK_ERRLOG("Error building REVERT TPER command.\n");
		return -ERANGE;
	}

	return opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
}

static int
opal_gen_new_active_key(struct spdk_opal_dev *dev, struct opal_session *sess,
			struct spdk_opal_key *active_key)
{
	uint8_t uid_data[OPAL_UID_LENGTH] = {0};
	int err = 0;
	int length;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	if (active_key->key_len == 0) {
		SPDK_ERRLOG("Error finding previous data to generate new active key\n");
		return -EINVAL;
	}

	length = spdk_min(active_key->key_len, OPAL_UID_LENGTH);
	memcpy(uid_data, active_key->key, length);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, uid_data, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[GENKEY_METHOD],
				  OPAL_UID_LENGTH);

	opal_add_tokens(&err, sess, 2, SPDK_OPAL_STARTLIST, SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error building new key generation command.\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

static int
opal_get_active_key_done(struct opal_session *sess, struct spdk_opal_key *active_key)
{
	const char *key;
	size_t str_len;
	int error = 0;

	error = opal_parse_and_check_status(sess);
	if (error) {
		return error;
	}

	str_len = opal_response_get_string(&sess->parsed_resp, 4, &key);
	if (!key) {
		SPDK_ERRLOG("Couldn't extract active key from response\n");
		return -EINVAL;
	}

	active_key->key_len = str_len;
	memcpy(active_key->key, key, active_key->key_len);

	SPDK_DEBUGLOG(opal, "active key = %p\n", active_key->key);
	return 0;
}

static int
opal_get_active_key(struct spdk_opal_dev *dev, struct opal_session *sess,
		    enum spdk_opal_locking_range locking_range,
		    struct spdk_opal_key *active_key)
{
	uint8_t uid_locking_range[OPAL_UID_LENGTH];
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_build_locking_range(uid_locking_range, locking_range);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, uid_locking_range, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[GET_METHOD],
				  OPAL_UID_LENGTH);
	opal_add_tokens(&err, sess, 12,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTLIST,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_STARTCOLUMN,
			SPDK_OPAL_ACTIVEKEY,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_STARTNAME,
			SPDK_OPAL_ENDCOLUMN,
			SPDK_OPAL_ACTIVEKEY,
			SPDK_OPAL_ENDNAME,
			SPDK_OPAL_ENDLIST,
			SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error building get active key command.\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_get_active_key_done(sess, active_key);
}

static int
opal_erase_locking_range(struct spdk_opal_dev *dev, struct opal_session *sess,
			 enum spdk_opal_locking_range locking_range)
{
	uint8_t uid_locking_range[OPAL_UID_LENGTH];
	int err = 0;
	int ret;

	opal_clear_cmd(sess);
	opal_set_comid(sess, dev->comid);

	opal_build_locking_range(uid_locking_range, locking_range);

	opal_add_token_u8(&err, sess, SPDK_OPAL_CALL);
	opal_add_token_bytestring(&err, sess, uid_locking_range, OPAL_UID_LENGTH);
	opal_add_token_bytestring(&err, sess, spdk_opal_method[ERASE_METHOD],
				  OPAL_UID_LENGTH);
	opal_add_tokens(&err, sess, 2, SPDK_OPAL_STARTLIST, SPDK_OPAL_ENDLIST);

	if (err) {
		SPDK_ERRLOG("Error building erase locking range.\n");
		return err;
	}

	ret = opal_cmd_finalize(sess, sess->hsn, sess->tsn, true);
	if (ret) {
		return ret;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		return ret;
	}

	return opal_parse_and_check_status(sess);
}

int
spdk_opal_cmd_revert_tper(struct spdk_opal_dev *dev, const char *passwd)
{
	int ret;
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};

	assert(dev != NULL);

	ret = opal_init_key(&opal_key, passwd);
	if (ret) {
		SPDK_ERRLOG("Init key failed\n");
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_generic_session(dev, sess, UID_SID, UID_ADMINSP,
					 opal_key.key, opal_key.key_len);
	if (ret) {
		SPDK_ERRLOG("Error on starting admin SP session with error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_build_revert_tper_cmd(dev, sess);
	if (ret) {
		opal_end_session(dev, sess, dev->comid);
		SPDK_ERRLOG("Build revert tper command with error %d\n", ret);
		goto end;
	}

	ret = opal_send_recv(dev, sess);
	if (ret) {
		opal_end_session(dev, sess, dev->comid);
		SPDK_ERRLOG("Error on reverting TPer with error %d\n", ret);
		goto end;
	}

	ret = opal_parse_and_check_status(sess);
	if (ret) {
		opal_end_session(dev, sess, dev->comid);
		SPDK_ERRLOG("Error on reverting TPer with error %d\n", ret);
	}
	/* No opal_end_session() required here for successful case */

end:
	free(sess);
	return ret;
}

int
spdk_opal_cmd_activate_locking_sp(struct spdk_opal_dev *dev, const char *passwd)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	int ret;

	ret = opal_init_key(&opal_key, passwd);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_generic_session(dev, sess, UID_SID, UID_ADMINSP,
					 opal_key.key, opal_key.key_len);
	if (ret) {
		SPDK_ERRLOG("Error on starting admin SP session with error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_get_locking_sp_lifecycle(dev, sess);
	if (ret) {
		SPDK_ERRLOG("Error on getting SP lifecycle with error %d\n", ret);
		goto end;
	}

	ret = opal_activate(dev, sess);
	if (ret) {
		SPDK_ERRLOG("Error on activation with error %d\n", ret);
	}

end:
	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("Error on ending session with error %d\n", ret);
	}

	free(sess);
	return ret;
}

int
spdk_opal_cmd_lock_unlock(struct spdk_opal_dev *dev, enum spdk_opal_user user,
			  enum spdk_opal_lock_state flag, enum spdk_opal_locking_range locking_range,
			  const char *passwd)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	int ret;

	assert(dev != NULL);

	ret = opal_init_key(&opal_key, passwd);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_auth_session(dev, sess, user, &opal_key);
	if (ret) {
		SPDK_ERRLOG("start authenticate session error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_lock_unlock_range(dev, sess, locking_range, flag);
	if (ret) {
		SPDK_ERRLOG("lock unlock range error %d\n", ret);
	}

	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

	free(sess);
	return ret;
}

int
spdk_opal_cmd_setup_locking_range(struct spdk_opal_dev *dev, enum spdk_opal_user user,
				  enum spdk_opal_locking_range locking_range_id, uint64_t range_start,
				  uint64_t range_length, const char *passwd)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	int ret;

	assert(dev != NULL);

	ret = opal_init_key(&opal_key, passwd);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_auth_session(dev, sess, user, &opal_key);
	if (ret) {
		SPDK_ERRLOG("start authenticate session error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_setup_locking_range(dev, sess, locking_range_id, range_start, range_length, true,
				       true);
	if (ret) {
		SPDK_ERRLOG("setup locking range error %d\n", ret);
	}

	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

	free(sess);
	return ret;
}

int
spdk_opal_cmd_get_max_ranges(struct spdk_opal_dev *dev, const char *passwd)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	int ret;

	assert(dev != NULL);

	if (dev->max_ranges) {
		return dev->max_ranges;
	}

	ret = opal_init_key(&opal_key, passwd);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_auth_session(dev, sess, OPAL_ADMIN1, &opal_key);
	if (ret) {
		SPDK_ERRLOG("start authenticate session error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_get_max_ranges(dev, sess);
	if (ret > 0) {
		dev->max_ranges = ret;
	}

	ret = opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

	free(sess);

	return (ret == 0 ? dev->max_ranges : ret);
}

int
spdk_opal_cmd_get_locking_range_info(struct spdk_opal_dev *dev, const char *passwd,
				     enum spdk_opal_user user_id,
				     enum spdk_opal_locking_range locking_range_id)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	int ret;

	assert(dev != NULL);

	ret = opal_init_key(&opal_key, passwd);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_auth_session(dev, sess, user_id, &opal_key);
	if (ret) {
		SPDK_ERRLOG("start authenticate session error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_get_locking_range_info(dev, sess, locking_range_id);
	if (ret) {
		SPDK_ERRLOG("get locking range info error %d\n", ret);
	}

	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

	free(sess);
	return ret;
}

int
spdk_opal_cmd_enable_user(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
			  const char *passwd)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	int ret;

	assert(dev != NULL);

	ret = opal_init_key(&opal_key, passwd);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret =  opal_start_generic_session(dev, sess, UID_ADMIN1, UID_LOCKINGSP,
					  opal_key.key, opal_key.key_len);
	if (ret) {
		SPDK_ERRLOG("start locking SP session error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_enable_user(dev, sess, user_id);
	if (ret) {
		SPDK_ERRLOG("enable user error %d\n", ret);
	}

	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

	free(sess);
	return ret;
}

int
spdk_opal_cmd_add_user_to_locking_range(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
					enum spdk_opal_locking_range locking_range_id,
					enum spdk_opal_lock_state lock_flag, const char *passwd)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	int ret;

	assert(dev != NULL);

	ret = opal_init_key(&opal_key, passwd);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret =  opal_start_generic_session(dev, sess, UID_ADMIN1, UID_LOCKINGSP,
					  opal_key.key, opal_key.key_len);
	if (ret) {
		SPDK_ERRLOG("start locking SP session error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_add_user_to_locking_range(dev, sess, user_id, locking_range_id, lock_flag);
	if (ret) {
		SPDK_ERRLOG("add user to locking range error %d\n", ret);
	}

	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

	free(sess);
	return ret;
}

int
spdk_opal_cmd_set_new_passwd(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
			     const char *new_passwd, const char *old_passwd, bool new_user)
{
	struct opal_session *sess;
	struct spdk_opal_key old_key = {};
	struct spdk_opal_key new_key = {};
	int ret;

	assert(dev != NULL);

	ret = opal_init_key(&old_key, old_passwd);
	if (ret != 0) {
		return ret;
	}

	ret = opal_init_key(&new_key, new_passwd);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_auth_session(dev, sess, new_user ? OPAL_ADMIN1 : user_id,
				      &old_key);
	if (ret) {
		SPDK_ERRLOG("start authenticate session error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_new_user_passwd(dev, sess, user_id, &new_key);
	if (ret) {
		SPDK_ERRLOG("set new passwd error %d\n", ret);
	}

	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

	free(sess);
	return ret;
}

int
spdk_opal_cmd_erase_locking_range(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
				  enum spdk_opal_locking_range locking_range_id, const char *password)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	int ret;

	assert(dev != NULL);

	ret = opal_init_key(&opal_key, password);
	if (ret != 0) {
		return ret;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		return -ENOMEM;
	}

	ret = opal_start_auth_session(dev, sess, user_id, &opal_key);
	if (ret) {
		SPDK_ERRLOG("start authenticate session error %d\n", ret);
		free(sess);
		return ret;
	}

	ret = opal_erase_locking_range(dev, sess, locking_range_id);
	if (ret) {
		SPDK_ERRLOG("get active key error %d\n", ret);
	}

	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}

	free(sess);
	return ret;
}

int
spdk_opal_cmd_secure_erase_locking_range(struct spdk_opal_dev *dev, enum spdk_opal_user user_id,
		enum spdk_opal_locking_range locking_range_id, const char *password)
{
	struct opal_session *sess;
	struct spdk_opal_key opal_key = {};
	struct spdk_opal_key *active_key;
	int ret;

	assert(dev != NULL);

	ret = opal_init_key(&opal_key, password);
	if (ret != 0) {
		return ret;
	}

	active_key = calloc(1, sizeof(*active_key));
	if (!active_key) {
		return -ENOMEM;
	}

	sess = opal_alloc_session(dev);
	if (!sess) {
		free(active_key);
		return -ENOMEM;
	}

	ret = opal_start_auth_session(dev, sess, user_id, &opal_key);
	if (ret) {
		SPDK_ERRLOG("start authenticate session error %d\n", ret);
		free(active_key);
		free(sess);
		return ret;
	}

	ret = opal_get_active_key(dev, sess, locking_range_id, active_key);
	if (ret) {
		SPDK_ERRLOG("get active key error %d\n", ret);
		goto end;
	}

	ret = opal_gen_new_active_key(dev, sess, active_key);
	if (ret) {
		SPDK_ERRLOG("generate new active key error %d\n", ret);
		goto end;
	}
	memset(active_key, 0, sizeof(struct spdk_opal_key));

end:
	ret += opal_end_session(dev, sess, dev->comid);
	if (ret) {
		SPDK_ERRLOG("end session error %d\n", ret);
	}
	free(active_key);
	free(sess);
	return ret;
}

struct spdk_opal_d0_features_info *
spdk_opal_get_d0_features_info(struct spdk_opal_dev *dev)
{
	return &dev->feat_info;
}

struct spdk_opal_locking_range_info *
spdk_opal_get_locking_range_info(struct spdk_opal_dev *dev, enum spdk_opal_locking_range id)
{
	assert(id < SPDK_OPAL_MAX_LOCKING_RANGE);
	return &dev->locking_ranges[id];
}

void
spdk_opal_free_locking_range_info(struct spdk_opal_dev *dev, enum spdk_opal_locking_range id)
{
	struct spdk_opal_locking_range_info *info;

	assert(id < SPDK_OPAL_MAX_LOCKING_RANGE);
	info = &dev->locking_ranges[id];
	memset(info, 0, sizeof(*info));
}

/* Log component for opal submodule */
SPDK_LOG_REGISTER_COMPONENT(opal)
