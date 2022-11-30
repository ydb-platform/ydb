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

#ifndef SPDK_INTERNAL_NVME_TCP_H
#define SPDK_INTERNAL_NVME_TCP_H

#include "spdk/likely.h"
#include "spdk/sock.h"
#include "spdk/dif.h"

#define SPDK_CRC32C_XOR				0xffffffffUL
#define SPDK_NVME_TCP_DIGEST_LEN		4
#define SPDK_NVME_TCP_DIGEST_ALIGNMENT		4
#define SPDK_NVME_TCP_QPAIR_EXIT_TIMEOUT	30
#define SPDK_NVMF_TCP_RECV_BUF_SIZE_FACTOR	8
#define SPDK_NVME_TCP_IN_CAPSULE_DATA_MAX_SIZE	8192u
/*
 * Maximum number of SGL elements.
 */
#define NVME_TCP_MAX_SGL_DESCRIPTORS	(16)

#define MAKE_DIGEST_WORD(BUF, CRC32C) \
        (   ((*((uint8_t *)(BUF)+0)) = (uint8_t)((uint32_t)(CRC32C) >> 0)), \
            ((*((uint8_t *)(BUF)+1)) = (uint8_t)((uint32_t)(CRC32C) >> 8)), \
            ((*((uint8_t *)(BUF)+2)) = (uint8_t)((uint32_t)(CRC32C) >> 16)), \
            ((*((uint8_t *)(BUF)+3)) = (uint8_t)((uint32_t)(CRC32C) >> 24)))

#define MATCH_DIGEST_WORD(BUF, CRC32C) \
        (    ((((uint32_t) *((uint8_t *)(BUF)+0)) << 0)         \
            | (((uint32_t) *((uint8_t *)(BUF)+1)) << 8)         \
            | (((uint32_t) *((uint8_t *)(BUF)+2)) << 16)        \
            | (((uint32_t) *((uint8_t *)(BUF)+3)) << 24))       \
            == (CRC32C))

#define DGET32(B)                                                               \
        (((  (uint32_t) *((uint8_t *)(B)+0)) << 0)                              \
         | (((uint32_t) *((uint8_t *)(B)+1)) << 8)                              \
         | (((uint32_t) *((uint8_t *)(B)+2)) << 16)                             \
         | (((uint32_t) *((uint8_t *)(B)+3)) << 24))

#define DSET32(B,D)                                                             \
        (((*((uint8_t *)(B)+0)) = (uint8_t)((uint32_t)(D) >> 0)),               \
         ((*((uint8_t *)(B)+1)) = (uint8_t)((uint32_t)(D) >> 8)),               \
         ((*((uint8_t *)(B)+2)) = (uint8_t)((uint32_t)(D) >> 16)),              \
         ((*((uint8_t *)(B)+3)) = (uint8_t)((uint32_t)(D) >> 24)))

typedef void (*nvme_tcp_qpair_xfer_complete_cb)(void *cb_arg);

struct _nvme_tcp_sgl {
	struct iovec	*iov;
	int		iovcnt;
	uint32_t	iov_offset;
	uint32_t	total_size;
};

struct nvme_tcp_pdu {
	union {
		/* to hold error pdu data */
		uint8_t					raw[SPDK_NVME_TCP_TERM_REQ_PDU_MAX_SIZE];
		struct spdk_nvme_tcp_common_pdu_hdr	common;
		struct spdk_nvme_tcp_ic_req		ic_req;
		struct spdk_nvme_tcp_term_req_hdr	term_req;
		struct spdk_nvme_tcp_cmd		capsule_cmd;
		struct spdk_nvme_tcp_h2c_data_hdr	h2c_data;
		struct spdk_nvme_tcp_ic_resp		ic_resp;
		struct spdk_nvme_tcp_rsp		capsule_resp;
		struct spdk_nvme_tcp_c2h_data_hdr	c2h_data;
		struct spdk_nvme_tcp_r2t_hdr		r2t;

	} hdr;

	bool						has_hdgst;
	bool						ddgst_enable;
	uint32_t					header_digest_crc32;
	uint32_t					data_digest_crc32;
	uint8_t						data_digest[SPDK_NVME_TCP_DIGEST_LEN];

	uint8_t						ch_valid_bytes;
	uint8_t						psh_valid_bytes;
	uint8_t						psh_len;

	nvme_tcp_qpair_xfer_complete_cb			cb_fn;
	void						*cb_arg;

	/* The sock request ends with a 0 length iovec. Place the actual iovec immediately
	 * after it. There is a static assert below to check if the compiler inserted
	 * any unwanted padding */
	struct spdk_sock_request			sock_req;
	struct iovec					iov[NVME_TCP_MAX_SGL_DESCRIPTORS * 2];

	struct iovec					data_iov[NVME_TCP_MAX_SGL_DESCRIPTORS];
	uint32_t					data_iovcnt;
	uint32_t					data_len;

	uint32_t					rw_offset;
	TAILQ_ENTRY(nvme_tcp_pdu)			tailq;
	uint32_t					remaining;
	uint32_t					padding_len;
	struct _nvme_tcp_sgl				sgl;

	struct spdk_dif_ctx				*dif_ctx;

	void						*req; /* data tied to a tcp request */
	void						*qpair;
};
SPDK_STATIC_ASSERT(offsetof(struct nvme_tcp_pdu,
			    sock_req) + sizeof(struct spdk_sock_request) == offsetof(struct nvme_tcp_pdu, iov),
		   "Compiler inserted padding between iov and sock_req");

enum nvme_tcp_pdu_recv_state {
	/* Ready to wait for PDU */
	NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_READY,

	/* Active tqpair waiting for any PDU common header */
	NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_CH,

	/* Active tqpair waiting for any PDU specific header */
	NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PSH,

	/* Active tqpair waiting for a tcp request, only use in target side */
	NVME_TCP_PDU_RECV_STATE_AWAIT_REQ,

	/* Active tqpair waiting for payload */
	NVME_TCP_PDU_RECV_STATE_AWAIT_PDU_PAYLOAD,

	/* Active tqpair does not wait for payload */
	NVME_TCP_PDU_RECV_STATE_ERROR,
};

enum nvme_tcp_error_codes {
	NVME_TCP_PDU_IN_PROGRESS        = 0,
	NVME_TCP_CONNECTION_FATAL       = -1,
	NVME_TCP_PDU_FATAL              = -2,
};

enum nvme_tcp_qpair_state {
	NVME_TCP_QPAIR_STATE_INVALID = 0,
	NVME_TCP_QPAIR_STATE_INITIALIZING = 1,
	NVME_TCP_QPAIR_STATE_RUNNING = 2,
	NVME_TCP_QPAIR_STATE_EXITING = 3,
	NVME_TCP_QPAIR_STATE_EXITED = 4,
};

static const bool g_nvme_tcp_hdgst[] = {
	[SPDK_NVME_TCP_PDU_TYPE_IC_REQ]         = false,
	[SPDK_NVME_TCP_PDU_TYPE_IC_RESP]        = false,
	[SPDK_NVME_TCP_PDU_TYPE_H2C_TERM_REQ]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_C2H_TERM_REQ]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_CAPSULE_CMD]    = true,
	[SPDK_NVME_TCP_PDU_TYPE_CAPSULE_RESP]   = true,
	[SPDK_NVME_TCP_PDU_TYPE_H2C_DATA]       = true,
	[SPDK_NVME_TCP_PDU_TYPE_C2H_DATA]       = true,
	[SPDK_NVME_TCP_PDU_TYPE_R2T]            = true
};

static const bool g_nvme_tcp_ddgst[] = {
	[SPDK_NVME_TCP_PDU_TYPE_IC_REQ]         = false,
	[SPDK_NVME_TCP_PDU_TYPE_IC_RESP]        = false,
	[SPDK_NVME_TCP_PDU_TYPE_H2C_TERM_REQ]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_C2H_TERM_REQ]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_CAPSULE_CMD]    = true,
	[SPDK_NVME_TCP_PDU_TYPE_CAPSULE_RESP]   = false,
	[SPDK_NVME_TCP_PDU_TYPE_H2C_DATA]       = true,
	[SPDK_NVME_TCP_PDU_TYPE_C2H_DATA]       = true,
	[SPDK_NVME_TCP_PDU_TYPE_R2T]            = false
};

static uint32_t
nvme_tcp_pdu_calc_header_digest(struct nvme_tcp_pdu *pdu)
{
	uint32_t crc32c;
	uint32_t hlen = pdu->hdr.common.hlen;

	crc32c = spdk_crc32c_update(&pdu->hdr.raw, hlen, ~0);
	crc32c = crc32c ^ SPDK_CRC32C_XOR;
	return crc32c;
}

static uint32_t
_update_crc32c_iov(struct iovec *iov, int iovcnt, uint32_t crc32c)
{
	int i;

	for (i = 0; i < iovcnt; i++) {
		assert(iov[i].iov_base != NULL);
		assert(iov[i].iov_len != 0);
		crc32c = spdk_crc32c_update(iov[i].iov_base, iov[i].iov_len, crc32c);
	}

	return crc32c;
}

static uint32_t
nvme_tcp_pdu_calc_data_digest(struct nvme_tcp_pdu *pdu)
{
	uint32_t crc32c = SPDK_CRC32C_XOR;
	uint32_t mod;

	assert(pdu->data_len != 0);

	if (spdk_likely(!pdu->dif_ctx)) {
		crc32c = _update_crc32c_iov(pdu->data_iov, pdu->data_iovcnt, crc32c);
	} else {
		spdk_dif_update_crc32c_stream(pdu->data_iov, pdu->data_iovcnt,
					      0, pdu->data_len, &crc32c, pdu->dif_ctx);
	}

	mod = pdu->data_len % SPDK_NVME_TCP_DIGEST_ALIGNMENT;
	if (mod != 0) {
		uint32_t pad_length = SPDK_NVME_TCP_DIGEST_ALIGNMENT - mod;
		uint8_t pad[3] = {0, 0, 0};

		assert(pad_length > 0);
		assert(pad_length <= sizeof(pad));
		crc32c = spdk_crc32c_update(pad, pad_length, crc32c);
	}
	crc32c = crc32c ^ SPDK_CRC32C_XOR;
	return crc32c;
}

static inline void
_nvme_tcp_sgl_init(struct _nvme_tcp_sgl *s, struct iovec *iov, int iovcnt,
		   uint32_t iov_offset)
{
	s->iov = iov;
	s->iovcnt = iovcnt;
	s->iov_offset = iov_offset;
	s->total_size = 0;
}

static inline void
_nvme_tcp_sgl_advance(struct _nvme_tcp_sgl *s, uint32_t step)
{
	s->iov_offset += step;
	while (s->iovcnt > 0) {
		if (s->iov_offset < s->iov->iov_len) {
			break;
		}

		s->iov_offset -= s->iov->iov_len;
		s->iov++;
		s->iovcnt--;
	}
}

static inline void
_nvme_tcp_sgl_get_buf(struct _nvme_tcp_sgl *s, void **_buf, uint32_t *_buf_len)
{
	if (_buf != NULL) {
		*_buf = s->iov->iov_base + s->iov_offset;
	}
	if (_buf_len != NULL) {
		*_buf_len = s->iov->iov_len - s->iov_offset;
	}
}

static inline bool
_nvme_tcp_sgl_append(struct _nvme_tcp_sgl *s, uint8_t *data, uint32_t data_len)
{
	if (s->iov_offset >= data_len) {
		s->iov_offset -= data_len;
	} else {
		assert(s->iovcnt > 0);
		s->iov->iov_base = data + s->iov_offset;
		s->iov->iov_len = data_len - s->iov_offset;
		s->total_size += data_len - s->iov_offset;
		s->iov_offset = 0;
		s->iov++;
		s->iovcnt--;
		if (s->iovcnt == 0) {
			return false;
		}
	}

	return true;
}

static inline bool
_nvme_tcp_sgl_append_multi(struct _nvme_tcp_sgl *s, struct iovec *iov, int iovcnt)
{
	int i;

	for (i = 0; i < iovcnt; i++) {
		if (!_nvme_tcp_sgl_append(s, iov[i].iov_base, iov[i].iov_len)) {
			return false;
		}
	}

	return true;
}

static inline uint32_t
_get_iov_array_size(struct iovec *iov, int iovcnt)
{
	int i;
	uint32_t size = 0;

	for (i = 0; i < iovcnt; i++) {
		size += iov[i].iov_len;
	}

	return size;
}

static inline bool
_nvme_tcp_sgl_append_multi_with_md(struct _nvme_tcp_sgl *s, struct iovec *iov, int iovcnt,
				   uint32_t data_len, const struct spdk_dif_ctx *dif_ctx)
{
	int rc;
	uint32_t mapped_len = 0;

	if (s->iov_offset >= data_len) {
		s->iov_offset -= _get_iov_array_size(iov, iovcnt);
	} else {
		rc = spdk_dif_set_md_interleave_iovs(s->iov, s->iovcnt, iov, iovcnt,
						     s->iov_offset, data_len - s->iov_offset,
						     &mapped_len, dif_ctx);
		if (rc < 0) {
			SPDK_ERRLOG("Failed to setup iovs for DIF insert/strip.\n");
			return false;
		}

		s->total_size += mapped_len;
		s->iov_offset = 0;
		assert(s->iovcnt >= rc);
		s->iovcnt -= rc;
		s->iov += rc;

		if (s->iovcnt == 0) {
			return false;
		}
	}

	return true;
}

static int
nvme_tcp_build_iovs(struct iovec *iov, int iovcnt, struct nvme_tcp_pdu *pdu,
		    bool hdgst_enable, bool ddgst_enable, uint32_t *_mapped_length)
{
	uint32_t hlen, plen;
	struct _nvme_tcp_sgl *sgl;

	if (iovcnt == 0) {
		return 0;
	}

	sgl = &pdu->sgl;
	_nvme_tcp_sgl_init(sgl, iov, iovcnt, 0);
	hlen = pdu->hdr.common.hlen;

	/* Header Digest */
	if (g_nvme_tcp_hdgst[pdu->hdr.common.pdu_type] && hdgst_enable) {
		hlen += SPDK_NVME_TCP_DIGEST_LEN;
	}

	plen = hlen;
	if (!pdu->data_len) {
		/* PDU header + possible header digest */
		_nvme_tcp_sgl_append(sgl, (uint8_t *)&pdu->hdr.raw, hlen);
		goto end;
	}

	/* Padding */
	if (pdu->padding_len > 0) {
		hlen += pdu->padding_len;
		plen = hlen;
	}

	if (!_nvme_tcp_sgl_append(sgl, (uint8_t *)&pdu->hdr.raw, hlen)) {
		goto end;
	}

	/* Data Segment */
	plen += pdu->data_len;
	if (spdk_likely(!pdu->dif_ctx)) {
		if (!_nvme_tcp_sgl_append_multi(sgl, pdu->data_iov, pdu->data_iovcnt)) {
			goto end;
		}
	} else {
		if (!_nvme_tcp_sgl_append_multi_with_md(sgl, pdu->data_iov, pdu->data_iovcnt,
							pdu->data_len, pdu->dif_ctx)) {
			goto end;
		}
	}

	/* Data Digest */
	if (g_nvme_tcp_ddgst[pdu->hdr.common.pdu_type] && ddgst_enable) {
		plen += SPDK_NVME_TCP_DIGEST_LEN;
		_nvme_tcp_sgl_append(sgl, pdu->data_digest, SPDK_NVME_TCP_DIGEST_LEN);
	}

	assert(plen == pdu->hdr.common.plen);

end:
	if (_mapped_length != NULL) {
		*_mapped_length = sgl->total_size;
	}

	return iovcnt - sgl->iovcnt;
}

static int
nvme_tcp_build_payload_iovs(struct iovec *iov, int iovcnt, struct nvme_tcp_pdu *pdu,
			    bool ddgst_enable, uint32_t *_mapped_length)
{
	struct _nvme_tcp_sgl *sgl;

	if (iovcnt == 0) {
		return 0;
	}

	sgl = &pdu->sgl;
	_nvme_tcp_sgl_init(sgl, iov, iovcnt, pdu->rw_offset);

	if (spdk_likely(!pdu->dif_ctx)) {
		if (!_nvme_tcp_sgl_append_multi(sgl, pdu->data_iov, pdu->data_iovcnt)) {
			goto end;
		}
	} else {
		if (!_nvme_tcp_sgl_append_multi_with_md(sgl, pdu->data_iov, pdu->data_iovcnt,
							pdu->data_len, pdu->dif_ctx)) {
			goto end;
		}
	}

	/* Data Digest */
	if (ddgst_enable) {
		_nvme_tcp_sgl_append(sgl, pdu->data_digest, SPDK_NVME_TCP_DIGEST_LEN);
	}

end:
	if (_mapped_length != NULL) {
		*_mapped_length = sgl->total_size;
	}
	return iovcnt - sgl->iovcnt;
}

static int
nvme_tcp_read_data(struct spdk_sock *sock, int bytes,
		   void *buf)
{
	int ret;

	ret = spdk_sock_recv(sock, buf, bytes);

	if (ret > 0) {
		return ret;
	}

	if (ret < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return 0;
		}

		/* For connect reset issue, do not output error log */
		if (errno != ECONNRESET) {
			SPDK_ERRLOG("spdk_sock_recv() failed, errno %d: %s\n",
				    errno, spdk_strerror(errno));
		}
	}

	/* connection closed */
	return NVME_TCP_CONNECTION_FATAL;
}

static int
nvme_tcp_readv_data(struct spdk_sock *sock, struct iovec *iov, int iovcnt)
{
	int ret;

	assert(sock != NULL);
	if (iov == NULL || iovcnt == 0) {
		return 0;
	}

	if (iovcnt == 1) {
		return nvme_tcp_read_data(sock, iov->iov_len, iov->iov_base);
	}

	ret = spdk_sock_readv(sock, iov, iovcnt);

	if (ret > 0) {
		return ret;
	}

	if (ret < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return 0;
		}

		/* For connect reset issue, do not output error log */
		if (errno != ECONNRESET) {
			SPDK_ERRLOG("spdk_sock_readv() failed, errno %d: %s\n",
				    errno, spdk_strerror(errno));
		}
	}

	/* connection closed */
	return NVME_TCP_CONNECTION_FATAL;
}


static int
nvme_tcp_read_payload_data(struct spdk_sock *sock, struct nvme_tcp_pdu *pdu)
{
	struct iovec iov[NVME_TCP_MAX_SGL_DESCRIPTORS + 1];
	int iovcnt;

	iovcnt = nvme_tcp_build_payload_iovs(iov, NVME_TCP_MAX_SGL_DESCRIPTORS + 1, pdu,
					     pdu->ddgst_enable, NULL);
	assert(iovcnt >= 0);

	return nvme_tcp_readv_data(sock, iov, iovcnt);
}

static void
_nvme_tcp_pdu_set_data(struct nvme_tcp_pdu *pdu, void *data, uint32_t data_len)
{
	pdu->data_iov[0].iov_base = data;
	pdu->data_iov[0].iov_len = data_len;
	pdu->data_iovcnt = 1;
}

static void
nvme_tcp_pdu_set_data(struct nvme_tcp_pdu *pdu, void *data, uint32_t data_len)
{
	_nvme_tcp_pdu_set_data(pdu, data, data_len);
	pdu->data_len = data_len;
}

static void
nvme_tcp_pdu_set_data_buf(struct nvme_tcp_pdu *pdu,
			  struct iovec *iov, int iovcnt,
			  uint32_t data_offset, uint32_t data_len)
{
	uint32_t buf_offset, buf_len, remain_len, len;
	uint8_t *buf;
	struct _nvme_tcp_sgl *pdu_sgl, buf_sgl;

	pdu->data_len = data_len;

	if (spdk_likely(!pdu->dif_ctx)) {
		buf_offset = data_offset;
		buf_len = data_len;
	} else {
		spdk_dif_ctx_set_data_offset(pdu->dif_ctx, data_offset);
		spdk_dif_get_range_with_md(data_offset, data_len,
					   &buf_offset, &buf_len, pdu->dif_ctx);
	}

	if (iovcnt == 1) {
		_nvme_tcp_pdu_set_data(pdu, (void *)((uint64_t)iov[0].iov_base + buf_offset), buf_len);
	} else {
		pdu_sgl = &pdu->sgl;

		_nvme_tcp_sgl_init(pdu_sgl, pdu->data_iov, NVME_TCP_MAX_SGL_DESCRIPTORS, 0);
		_nvme_tcp_sgl_init(&buf_sgl, iov, iovcnt, 0);

		_nvme_tcp_sgl_advance(&buf_sgl, buf_offset);
		remain_len = buf_len;

		while (remain_len > 0) {
			_nvme_tcp_sgl_get_buf(&buf_sgl, (void *)&buf, &len);
			len = spdk_min(len, remain_len);

			_nvme_tcp_sgl_advance(&buf_sgl, len);
			remain_len -= len;

			if (!_nvme_tcp_sgl_append(pdu_sgl, buf, len)) {
				break;
			}
		}

		assert(remain_len == 0);
		assert(pdu_sgl->total_size == buf_len);

		pdu->data_iovcnt = NVME_TCP_MAX_SGL_DESCRIPTORS - pdu_sgl->iovcnt;
	}
}

static void
nvme_tcp_pdu_calc_psh_len(struct nvme_tcp_pdu *pdu, bool hdgst_enable)
{
	uint8_t psh_len, pdo, padding_len;

	psh_len = pdu->hdr.common.hlen;

	if (g_nvme_tcp_hdgst[pdu->hdr.common.pdu_type] && hdgst_enable) {
		pdu->has_hdgst = true;
		psh_len += SPDK_NVME_TCP_DIGEST_LEN;
	}
	if (pdu->hdr.common.plen > psh_len) {
		pdo = pdu->hdr.common.pdo;
		padding_len = pdo - psh_len;
		if (padding_len > 0) {
			psh_len = pdo;
		}
	}

	psh_len -= sizeof(struct spdk_nvme_tcp_common_pdu_hdr);
	pdu->psh_len = psh_len;
}

#endif /* SPDK_INTERNAL_NVME_TCP_H */
