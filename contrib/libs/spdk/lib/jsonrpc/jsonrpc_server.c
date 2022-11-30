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

#include "jsonrpc_internal.h"

#include "spdk/util.h"

struct jsonrpc_request {
	const struct spdk_json_val *version;
	const struct spdk_json_val *method;
	const struct spdk_json_val *params;
	const struct spdk_json_val *id;
};

static int
capture_val(const struct spdk_json_val *val, void *out)
{
	const struct spdk_json_val **vptr = out;

	*vptr = val;
	return 0;
}

static const struct spdk_json_object_decoder jsonrpc_request_decoders[] = {
	{"jsonrpc", offsetof(struct jsonrpc_request, version), capture_val, true},
	{"method", offsetof(struct jsonrpc_request, method), capture_val},
	{"params", offsetof(struct jsonrpc_request, params), capture_val, true},
	{"id", offsetof(struct jsonrpc_request, id), capture_val, true},
};

static void
parse_single_request(struct spdk_jsonrpc_request *request, struct spdk_json_val *values)
{
	struct jsonrpc_request req = {};
	const struct spdk_json_val *params = NULL;

	if (spdk_json_decode_object(values, jsonrpc_request_decoders,
				    SPDK_COUNTOF(jsonrpc_request_decoders),
				    &req)) {
		goto invalid;
	}

	if (req.version && (req.version->type != SPDK_JSON_VAL_STRING ||
			    !spdk_json_strequal(req.version, "2.0"))) {
		goto invalid;
	}

	if (!req.method || req.method->type != SPDK_JSON_VAL_STRING) {
		goto invalid;
	}

	if (req.id) {
		if (req.id->type == SPDK_JSON_VAL_STRING ||
		    req.id->type == SPDK_JSON_VAL_NUMBER ||
		    req.id->type == SPDK_JSON_VAL_NULL) {
			request->id = req.id;
		} else  {
			goto invalid;
		}
	}

	if (req.params) {
		/* null json value is as if there were no parameters */
		if (req.params->type != SPDK_JSON_VAL_NULL) {
			if (req.params->type != SPDK_JSON_VAL_ARRAY_BEGIN &&
			    req.params->type != SPDK_JSON_VAL_OBJECT_BEGIN) {
				goto invalid;
			}
			params = req.params;
		}
	}

	jsonrpc_server_handle_request(request, req.method, params);
	return;

invalid:
	jsonrpc_server_handle_error(request, SPDK_JSONRPC_ERROR_INVALID_REQUEST);
}

static int
jsonrpc_server_write_cb(void *cb_ctx, const void *data, size_t size)
{
	struct spdk_jsonrpc_request *request = cb_ctx;
	size_t new_size = request->send_buf_size;

	while (new_size - request->send_len < size) {
		if (new_size >= SPDK_JSONRPC_SEND_BUF_SIZE_MAX) {
			SPDK_ERRLOG("Send buf exceeded maximum size (%zu)\n",
				    (size_t)SPDK_JSONRPC_SEND_BUF_SIZE_MAX);
			return -1;
		}

		new_size *= 2;
	}

	if (new_size != request->send_buf_size) {
		uint8_t *new_buf;

		new_buf = realloc(request->send_buf, new_size);
		if (new_buf == NULL) {
			SPDK_ERRLOG("Resizing send_buf failed (current size %zu, new size %zu)\n",
				    request->send_buf_size, new_size);
			return -1;
		}

		request->send_buf = new_buf;
		request->send_buf_size = new_size;
	}

	memcpy(request->send_buf + request->send_len, data, size);
	request->send_len += size;

	return 0;
}

int
jsonrpc_parse_request(struct spdk_jsonrpc_server_conn *conn, const void *json, size_t size)
{
	struct spdk_jsonrpc_request *request;
	ssize_t rc;
	size_t len;
	void *end = NULL;

	/* Check to see if we have received a full JSON value. It is safe to cast away const
	 * as we don't decode in place. */
	rc = spdk_json_parse((void *)json, size, NULL, 0, &end, 0);
	if (rc == SPDK_JSON_PARSE_INCOMPLETE) {
		return 0;
	}

	request = calloc(1, sizeof(*request));
	if (request == NULL) {
		SPDK_DEBUGLOG(rpc, "Out of memory allocating request\n");
		return -1;
	}

	conn->outstanding_requests++;

	request->conn = conn;

	len = end - json;
	request->recv_buffer = malloc(len + 1);
	if (request->recv_buffer == NULL) {
		SPDK_ERRLOG("Failed to allocate buffer to copy request (%zu bytes)\n", len + 1);
		jsonrpc_free_request(request);
		return -1;
	}

	memcpy(request->recv_buffer, json, len);
	request->recv_buffer[len] = '\0';

	if (rc > 0 && rc <= SPDK_JSONRPC_MAX_VALUES) {
		request->values_cnt = rc;
		request->values = malloc(request->values_cnt * sizeof(request->values[0]));
		if (request->values == NULL) {
			SPDK_ERRLOG("Failed to allocate buffer for JSON values (%zu bytes)\n",
				    request->values_cnt * sizeof(request->values[0]));
			jsonrpc_free_request(request);
			return -1;
		}
	}

	request->send_offset = 0;
	request->send_len = 0;
	request->send_buf_size = SPDK_JSONRPC_SEND_BUF_SIZE_INIT;
	request->send_buf = malloc(request->send_buf_size);
	if (request->send_buf == NULL) {
		SPDK_ERRLOG("Failed to allocate send_buf (%zu bytes)\n", request->send_buf_size);
		jsonrpc_free_request(request);
		return -1;
	}

	request->response = spdk_json_write_begin(jsonrpc_server_write_cb, request, 0);
	if (request->response == NULL) {
		SPDK_ERRLOG("Failed to allocate response JSON write context.\n");
		jsonrpc_free_request(request);
		return -1;
	}

	if (rc <= 0 || rc > SPDK_JSONRPC_MAX_VALUES) {
		SPDK_DEBUGLOG(rpc, "JSON parse error\n");
		jsonrpc_server_handle_error(request, SPDK_JSONRPC_ERROR_PARSE_ERROR);

		/*
		 * Can't recover from parse error (no guaranteed resync point in streaming JSON).
		 * Return an error to indicate that the connection should be closed.
		 */
		return -1;
	}

	/* Decode a second time now that there is a full JSON value available. */
	rc = spdk_json_parse(request->recv_buffer, size, request->values, request->values_cnt, &end,
			     SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE);
	if (rc < 0 || rc > SPDK_JSONRPC_MAX_VALUES) {
		SPDK_DEBUGLOG(rpc, "JSON parse error on second pass\n");
		jsonrpc_server_handle_error(request, SPDK_JSONRPC_ERROR_PARSE_ERROR);
		return -1;
	}

	assert(end != NULL);

	if (request->values[0].type == SPDK_JSON_VAL_OBJECT_BEGIN) {
		parse_single_request(request, request->values);
	} else if (request->values[0].type == SPDK_JSON_VAL_ARRAY_BEGIN) {
		SPDK_DEBUGLOG(rpc, "Got batch array (not currently supported)\n");
		jsonrpc_server_handle_error(request, SPDK_JSONRPC_ERROR_INVALID_REQUEST);
	} else {
		SPDK_DEBUGLOG(rpc, "top-level JSON value was not array or object\n");
		jsonrpc_server_handle_error(request, SPDK_JSONRPC_ERROR_INVALID_REQUEST);
	}

	return len;
}

struct spdk_jsonrpc_server_conn *
spdk_jsonrpc_get_conn(struct spdk_jsonrpc_request *request)
{
	return request->conn;
}

/* Never return NULL */
static struct spdk_json_write_ctx *
begin_response(struct spdk_jsonrpc_request *request)
{
	struct spdk_json_write_ctx *w = request->response;

	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "jsonrpc", "2.0");

	spdk_json_write_name(w, "id");
	if (request->id) {
		spdk_json_write_val(w, request->id);
	} else {
		spdk_json_write_null(w);
	}

	return w;
}

static void
skip_response(struct spdk_jsonrpc_request *request)
{
	request->send_len = 0;
	spdk_json_write_end(request->response);
	request->response = NULL;
	jsonrpc_server_send_response(request);
}

static void
end_response(struct spdk_jsonrpc_request *request)
{
	spdk_json_write_object_end(request->response);
	spdk_json_write_end(request->response);
	request->response = NULL;

	jsonrpc_server_write_cb(request, "\n", 1);
	jsonrpc_server_send_response(request);
}

void
jsonrpc_free_request(struct spdk_jsonrpc_request *request)
{
	if (!request) {
		return;
	}

	/* We must send or skip response explicitly */
	assert(request->response == NULL);

	request->conn->outstanding_requests--;
	free(request->recv_buffer);
	free(request->values);
	free(request->send_buf);
	free(request);
}

struct spdk_json_write_ctx *
spdk_jsonrpc_begin_result(struct spdk_jsonrpc_request *request)
{
	struct spdk_json_write_ctx *w = begin_response(request);

	spdk_json_write_name(w, "result");
	return w;
}

void
spdk_jsonrpc_end_result(struct spdk_jsonrpc_request *request, struct spdk_json_write_ctx *w)
{
	assert(w != NULL);
	assert(w == request->response);

	/* If there was no ID in request we skip response. */
	if (request->id && request->id->type != SPDK_JSON_VAL_NULL) {
		end_response(request);
	} else {
		skip_response(request);
	}
}

void
spdk_jsonrpc_send_bool_response(struct spdk_jsonrpc_request *request, bool value)
{
	struct spdk_json_write_ctx *w;

	w = spdk_jsonrpc_begin_result(request);
	assert(w != NULL);
	spdk_json_write_bool(w, value);
	spdk_jsonrpc_end_result(request, w);
}

void
spdk_jsonrpc_send_error_response(struct spdk_jsonrpc_request *request,
				 int error_code, const char *msg)
{
	struct spdk_json_write_ctx *w = begin_response(request);

	spdk_json_write_named_object_begin(w, "error");
	spdk_json_write_named_int32(w, "code", error_code);
	spdk_json_write_named_string(w, "message", msg);
	spdk_json_write_object_end(w);

	end_response(request);
}

void
spdk_jsonrpc_send_error_response_fmt(struct spdk_jsonrpc_request *request,
				     int error_code, const char *fmt, ...)
{
	struct spdk_json_write_ctx *w = begin_response(request);
	va_list args;

	spdk_json_write_named_object_begin(w, "error");
	spdk_json_write_named_int32(w, "code", error_code);
	va_start(args, fmt);
	spdk_json_write_named_string_fmt_v(w, "message", fmt, args);
	va_end(args);
	spdk_json_write_object_end(w);

	end_response(request);
}

SPDK_LOG_REGISTER_COMPONENT(rpc)
