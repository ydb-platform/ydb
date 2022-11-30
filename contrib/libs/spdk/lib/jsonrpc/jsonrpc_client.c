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

#include "spdk/util.h"
#include "jsonrpc_internal.h"

static int
capture_version(const struct spdk_json_val *val, void *out)
{
	const struct spdk_json_val **vptr = out;

	if (spdk_json_strequal(val, "2.0") != true) {
		return SPDK_JSON_PARSE_INVALID;
	}

	*vptr = val;
	return 0;
}

static int
capture_id(const struct spdk_json_val *val, void *out)
{
	const struct spdk_json_val **vptr = out;

	if (val->type != SPDK_JSON_VAL_STRING && val->type != SPDK_JSON_VAL_NUMBER) {
		return -EINVAL;
	}

	*vptr = val;
	return 0;
}

static int
capture_any(const struct spdk_json_val *val, void *out)
{
	const struct spdk_json_val **vptr = out;

	*vptr = val;
	return 0;
}

static const struct spdk_json_object_decoder jsonrpc_response_decoders[] = {
	{"jsonrpc", offsetof(struct spdk_jsonrpc_client_response, version), capture_version},
	{"id", offsetof(struct spdk_jsonrpc_client_response, id), capture_id, true},
	{"result", offsetof(struct spdk_jsonrpc_client_response, result), capture_any, true},
	{"error", offsetof(struct spdk_jsonrpc_client_response, error), capture_any, true},
};

int
jsonrpc_parse_response(struct spdk_jsonrpc_client *client)
{
	struct spdk_jsonrpc_client_response_internal *r;
	ssize_t rc;
	size_t buf_len;
	size_t values_cnt;
	void *end = NULL;


	/* Check to see if we have received a full JSON value. */
	rc = spdk_json_parse(client->recv_buf, client->recv_offset, NULL, 0, &end, 0);
	if (rc == SPDK_JSON_PARSE_INCOMPLETE) {
		return 0;
	}

	SPDK_DEBUGLOG(rpc_client, "JSON string is :\n%s\n", client->recv_buf);
	if (rc < 0 || rc > SPDK_JSONRPC_CLIENT_MAX_VALUES) {
		SPDK_ERRLOG("JSON parse error (rc: %zd)\n", rc);
		/*
		 * Can't recover from parse error (no guaranteed resync point in streaming JSON).
		 * Return an error to indicate that the connection should be closed.
		 */
		return -EINVAL;
	}

	values_cnt = rc;

	r = calloc(1, sizeof(*r) + sizeof(struct spdk_json_val) * (values_cnt + 1));
	if (!r) {
		return -errno;
	}

	if (client->resp) {
		free(r);
		return -ENOSPC;
	}

	client->resp = r;

	r->buf = client->recv_buf;
	buf_len = client->recv_offset;
	r->values_cnt = values_cnt;

	client->recv_buf_size = 0;
	client->recv_offset = 0;
	client->recv_buf = NULL;

	/* Decode a second time now that there is a full JSON value available. */
	rc = spdk_json_parse(r->buf, buf_len, r->values, values_cnt, &end,
			     SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE);
	if (rc != (ssize_t)values_cnt) {
		SPDK_ERRLOG("JSON parse error on second pass (rc: %zd, expected: %zu)\n", rc, values_cnt);
		goto err;
	}

	assert(end != NULL);

	if (r->values[0].type != SPDK_JSON_VAL_OBJECT_BEGIN) {
		SPDK_ERRLOG("top-level JSON value was not object\n");
		goto err;
	}

	if (spdk_json_decode_object(r->values, jsonrpc_response_decoders,
				    SPDK_COUNTOF(jsonrpc_response_decoders), &r->jsonrpc)) {
		goto err;
	}

	r->ready = 1;
	return 1;

err:
	client->resp = NULL;
	spdk_jsonrpc_client_free_response(&r->jsonrpc);
	return -EINVAL;
}

static int
jsonrpc_client_write_cb(void *cb_ctx, const void *data, size_t size)
{
	struct spdk_jsonrpc_client_request *request = cb_ctx;
	size_t new_size = request->send_buf_size;

	while (new_size - request->send_len < size) {
		if (new_size >= SPDK_JSONRPC_SEND_BUF_SIZE_MAX) {
			SPDK_ERRLOG("Send buf exceeded maximum size (%zu)\n",
				    (size_t)SPDK_JSONRPC_SEND_BUF_SIZE_MAX);
			return -ENOSPC;
		}

		new_size *= 2;
	}

	if (new_size != request->send_buf_size) {
		uint8_t *new_buf;

		new_buf = realloc(request->send_buf, new_size);
		if (new_buf == NULL) {
			SPDK_ERRLOG("Resizing send_buf failed (current size %zu, new size %zu)\n",
				    request->send_buf_size, new_size);
			return -ENOMEM;
		}

		request->send_buf = new_buf;
		request->send_buf_size = new_size;
	}

	memcpy(request->send_buf + request->send_len, data, size);
	request->send_len += size;

	return 0;
}

struct spdk_json_write_ctx *
spdk_jsonrpc_begin_request(struct spdk_jsonrpc_client_request *request, int32_t id,
			   const char *method)
{
	struct spdk_json_write_ctx *w;

	w = spdk_json_write_begin(jsonrpc_client_write_cb, request, 0);
	if (w == NULL) {
		return NULL;
	}

	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "jsonrpc", "2.0");

	if (id >= 0) {
		spdk_json_write_named_int32(w, "id", id);
	}

	if (method) {
		spdk_json_write_named_string(w, "method", method);
	}

	return w;
}

void
spdk_jsonrpc_end_request(struct spdk_jsonrpc_client_request *request, struct spdk_json_write_ctx *w)
{
	assert(w != NULL);

	spdk_json_write_object_end(w);
	spdk_json_write_end(w);
	jsonrpc_client_write_cb(request, "\n", 1);
}

SPDK_LOG_REGISTER_COMPONENT(rpc_client)
