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

#include "spdk/json.h"

#include "spdk_internal/utf.h"

struct spdk_json_write_ctx {
	spdk_json_write_cb write_cb;
	void *cb_ctx;
	uint32_t flags;
	uint32_t indent;
	bool new_indent;
	bool first_value;
	bool failed;
	size_t buf_filled;
	uint8_t buf[4096];
};

static int emit_buf_full(struct spdk_json_write_ctx *w, const void *data, size_t size);

static int
fail(struct spdk_json_write_ctx *w)
{
	w->failed = true;
	return -1;
}

static int
flush_buf(struct spdk_json_write_ctx *w)
{
	int rc;

	rc = w->write_cb(w->cb_ctx, w->buf, w->buf_filled);
	if (rc != 0) {
		return fail(w);
	}

	w->buf_filled = 0;

	return 0;
}

struct spdk_json_write_ctx *
spdk_json_write_begin(spdk_json_write_cb write_cb, void *cb_ctx, uint32_t flags)
{
	struct spdk_json_write_ctx *w;

	w = calloc(1, sizeof(*w));
	if (w == NULL) {
		return w;
	}

	w->write_cb = write_cb;
	w->cb_ctx = cb_ctx;
	w->flags = flags;
	w->indent = 0;
	w->new_indent = false;
	w->first_value = true;
	w->failed = false;
	w->buf_filled = 0;

	return w;
}

int
spdk_json_write_end(struct spdk_json_write_ctx *w)
{
	bool failed;
	int rc;

	if (w == NULL) {
		return 0;
	}

	failed = w->failed;

	rc = flush_buf(w);
	if (rc != 0) {
		failed = true;
	}

	free(w);

	return failed ? -1 : 0;
}

static inline int
emit(struct spdk_json_write_ctx *w, const void *data, size_t size)
{
	size_t buf_remain = sizeof(w->buf) - w->buf_filled;

	if (spdk_unlikely(size > buf_remain)) {
		/* Not enough space in buffer for the new data. */
		return emit_buf_full(w, data, size);
	}

	/* Copy the new data into buf. */
	memcpy(w->buf + w->buf_filled, data, size);
	w->buf_filled += size;
	return 0;
}

static int
emit_buf_full(struct spdk_json_write_ctx *w, const void *data, size_t size)
{
	size_t buf_remain = sizeof(w->buf) - w->buf_filled;
	int rc;

	assert(size > buf_remain);

	/* Copy as much of the new data as possible into the buffer and flush it. */
	memcpy(w->buf + w->buf_filled, data, buf_remain);
	w->buf_filled += buf_remain;

	rc = flush_buf(w);
	if (rc != 0) {
		return fail(w);
	}

	/* Recurse to emit the rest of the data. */
	return emit(w, data + buf_remain, size - buf_remain);
}

static int
emit_fmt(struct spdk_json_write_ctx *w, const void *data, size_t size)
{
	if (w->flags & SPDK_JSON_WRITE_FLAG_FORMATTED) {
		return emit(w, data, size);
	}
	return 0;
}

static int
emit_indent(struct spdk_json_write_ctx *w)
{
	uint32_t i;

	if (w->flags & SPDK_JSON_WRITE_FLAG_FORMATTED) {
		for (i = 0; i < w->indent; i++) {
			if (emit(w, "  ", 2)) { return fail(w); }
		}
	}
	return 0;
}

static int
begin_value(struct spdk_json_write_ctx *w)
{
	/* TODO: check for value state */
	if (w->new_indent) {
		if (emit_fmt(w, "\n", 1)) { return fail(w); }
		if (emit_indent(w)) { return fail(w); }
	}
	if (!w->first_value) {
		if (emit(w, ",", 1)) { return fail(w); }
		if (emit_fmt(w, "\n", 1)) { return fail(w); }
		if (emit_indent(w)) { return fail(w); }
	}
	w->first_value = false;
	w->new_indent = false;
	return 0;
}

int
spdk_json_write_val_raw(struct spdk_json_write_ctx *w, const void *data, size_t len)
{
	if (begin_value(w)) { return fail(w); }
	return emit(w, data, len);
}

int
spdk_json_write_null(struct spdk_json_write_ctx *w)
{
	if (begin_value(w)) { return fail(w); }
	return emit(w, "null", 4);
}

int
spdk_json_write_bool(struct spdk_json_write_ctx *w, bool val)
{
	if (begin_value(w)) { return fail(w); }
	if (val) {
		return emit(w, "true", 4);
	} else {
		return emit(w, "false", 5);
	}
}

int
spdk_json_write_int32(struct spdk_json_write_ctx *w, int32_t val)
{
	char buf[32];
	int count;

	if (begin_value(w)) { return fail(w); }
	count = snprintf(buf, sizeof(buf), "%" PRId32, val);
	if (count <= 0 || (size_t)count >= sizeof(buf)) { return fail(w); }
	return emit(w, buf, count);
}

int
spdk_json_write_uint32(struct spdk_json_write_ctx *w, uint32_t val)
{
	char buf[32];
	int count;

	if (begin_value(w)) { return fail(w); }
	count = snprintf(buf, sizeof(buf), "%" PRIu32, val);
	if (count <= 0 || (size_t)count >= sizeof(buf)) { return fail(w); }
	return emit(w, buf, count);
}

int
spdk_json_write_int64(struct spdk_json_write_ctx *w, int64_t val)
{
	char buf[32];
	int count;

	if (begin_value(w)) { return fail(w); }
	count = snprintf(buf, sizeof(buf), "%" PRId64, val);
	if (count <= 0 || (size_t)count >= sizeof(buf)) { return fail(w); }
	return emit(w, buf, count);
}

int
spdk_json_write_uint64(struct spdk_json_write_ctx *w, uint64_t val)
{
	char buf[32];
	int count;

	if (begin_value(w)) { return fail(w); }
	count = snprintf(buf, sizeof(buf), "%" PRIu64, val);
	if (count <= 0 || (size_t)count >= sizeof(buf)) { return fail(w); }
	return emit(w, buf, count);
}

static void
write_hex_4(void *dest, uint16_t val)
{
	uint8_t *p = dest;
	char hex[] = "0123456789ABCDEF";

	p[0] = hex[(val >> 12)];
	p[1] = hex[(val >> 8) & 0xF];
	p[2] = hex[(val >> 4) & 0xF];
	p[3] = hex[val & 0xF];
}

static inline int
write_codepoint(struct spdk_json_write_ctx *w, uint32_t codepoint)
{
	static const uint8_t escapes[] = {
		['\b'] = 'b',
		['\f'] = 'f',
		['\n'] = 'n',
		['\r'] = 'r',
		['\t'] = 't',
		['"'] = '"',
		['\\'] = '\\',
		/*
		 * Forward slash (/) is intentionally not converted to an escape
		 *  (it is valid unescaped).
		 */
	};
	uint16_t high, low;
	char out[13];
	size_t out_len;

	if (codepoint < sizeof(escapes) && escapes[codepoint]) {
		out[0] = '\\';
		out[1] = escapes[codepoint];
		out_len = 2;
	} else if (codepoint >= 0x20 && codepoint < 0x7F) {
		/*
		 * Encode plain ASCII directly (except 0x7F, since it is really
		 *  a control character, despite the JSON spec not considering it one).
		 */
		out[0] = (uint8_t)codepoint;
		out_len = 1;
	} else if (codepoint < 0x10000) {
		out[0] = '\\';
		out[1] = 'u';
		write_hex_4(&out[2], (uint16_t)codepoint);
		out_len = 6;
	} else {
		utf16_encode_surrogate_pair(codepoint, &high, &low);
		out[0] = '\\';
		out[1] = 'u';
		write_hex_4(&out[2], high);
		out[6] = '\\';
		out[7] = 'u';
		write_hex_4(&out[8], low);
		out_len = 12;
	}

	return emit(w, out, out_len);
}

static int
write_string_or_name(struct spdk_json_write_ctx *w, const char *val, size_t len)
{
	const uint8_t *p = val;
	const uint8_t *end = val + len;

	if (emit(w, "\"", 1)) { return fail(w); }

	while (p != end) {
		int codepoint_len;
		uint32_t codepoint;

		codepoint_len = utf8_valid(p, end);
		switch (codepoint_len) {
		case 1:
			codepoint = utf8_decode_unsafe_1(p);
			break;
		case 2:
			codepoint = utf8_decode_unsafe_2(p);
			break;
		case 3:
			codepoint = utf8_decode_unsafe_3(p);
			break;
		case 4:
			codepoint = utf8_decode_unsafe_4(p);
			break;
		default:
			return fail(w);
		}

		if (write_codepoint(w, codepoint)) { return fail(w); }
		p += codepoint_len;
	}

	return emit(w, "\"", 1);
}

static int
write_string_or_name_utf16le(struct spdk_json_write_ctx *w, const uint16_t *val, size_t len)
{
	const uint16_t *p = val;
	const uint16_t *end = val + len;

	if (emit(w, "\"", 1)) { return fail(w); }

	while (p != end) {
		int codepoint_len;
		uint32_t codepoint;

		codepoint_len = utf16le_valid(p, end);
		switch (codepoint_len) {
		case 1:
			codepoint = from_le16(&p[0]);
			break;
		case 2:
			codepoint = utf16_decode_surrogate_pair(from_le16(&p[0]), from_le16(&p[1]));
			break;
		default:
			return fail(w);
		}

		if (write_codepoint(w, codepoint)) { return fail(w); }
		p += codepoint_len;
	}

	return emit(w, "\"", 1);
}

int
spdk_json_write_string_raw(struct spdk_json_write_ctx *w, const char *val, size_t len)
{
	if (begin_value(w)) { return fail(w); }
	return write_string_or_name(w, val, len);
}

int
spdk_json_write_string(struct spdk_json_write_ctx *w, const char *val)
{
	return spdk_json_write_string_raw(w, val, strlen(val));
}

int
spdk_json_write_string_utf16le_raw(struct spdk_json_write_ctx *w, const uint16_t *val, size_t len)
{
	if (begin_value(w)) { return fail(w); }
	return write_string_or_name_utf16le(w, val, len);
}

int
spdk_json_write_string_utf16le(struct spdk_json_write_ctx *w, const uint16_t *val)
{
	const uint16_t *p;
	size_t len;

	for (len = 0, p = val; *p; p++) {
		len++;
	}

	return spdk_json_write_string_utf16le_raw(w, val, len);
}

int
spdk_json_write_string_fmt(struct spdk_json_write_ctx *w, const char *fmt, ...)
{
	va_list args;
	int rc;

	va_start(args, fmt);
	rc = spdk_json_write_string_fmt_v(w, fmt, args);
	va_end(args);

	return rc;
}

int
spdk_json_write_string_fmt_v(struct spdk_json_write_ctx *w, const char *fmt, va_list args)
{
	char *s;
	int rc;

	s = spdk_vsprintf_alloc(fmt, args);
	if (s == NULL) {
		return -1;
	}

	rc = spdk_json_write_string(w, s);
	free(s);
	return rc;
}

int
spdk_json_write_array_begin(struct spdk_json_write_ctx *w)
{
	if (begin_value(w)) { return fail(w); }
	w->first_value = true;
	w->new_indent = true;
	w->indent++;
	if (emit(w, "[", 1)) { return fail(w); }
	return 0;
}

int
spdk_json_write_array_end(struct spdk_json_write_ctx *w)
{
	w->first_value = false;
	if (w->indent == 0) { return fail(w); }
	w->indent--;
	if (!w->new_indent) {
		if (emit_fmt(w, "\n", 1)) { return fail(w); }
		if (emit_indent(w)) { return fail(w); }
	}
	w->new_indent = false;
	return emit(w, "]", 1);
}

int
spdk_json_write_object_begin(struct spdk_json_write_ctx *w)
{
	if (begin_value(w)) { return fail(w); }
	w->first_value = true;
	w->new_indent = true;
	w->indent++;
	if (emit(w, "{", 1)) { return fail(w); }
	return 0;
}

int
spdk_json_write_object_end(struct spdk_json_write_ctx *w)
{
	w->first_value = false;
	w->indent--;
	if (!w->new_indent) {
		if (emit_fmt(w, "\n", 1)) { return fail(w); }
		if (emit_indent(w)) { return fail(w); }
	}
	w->new_indent = false;
	return emit(w, "}", 1);
}

int
spdk_json_write_name_raw(struct spdk_json_write_ctx *w, const char *name, size_t len)
{
	/* TODO: check that container is an object */
	if (begin_value(w)) { return fail(w); }
	if (write_string_or_name(w, name, len)) { return fail(w); }
	w->first_value = true;
	if (emit(w, ":", 1)) { return fail(w); }
	return emit_fmt(w, " ", 1);
}

int
spdk_json_write_name(struct spdk_json_write_ctx *w, const char *name)
{
	return spdk_json_write_name_raw(w, name, strlen(name));
}

int
spdk_json_write_val(struct spdk_json_write_ctx *w, const struct spdk_json_val *val)
{
	size_t num_values, i;

	switch (val->type) {
	case SPDK_JSON_VAL_NUMBER:
		return spdk_json_write_val_raw(w, val->start, val->len);

	case SPDK_JSON_VAL_STRING:
		return spdk_json_write_string_raw(w, val->start, val->len);

	case SPDK_JSON_VAL_NAME:
		return spdk_json_write_name_raw(w, val->start, val->len);

	case SPDK_JSON_VAL_TRUE:
		return spdk_json_write_bool(w, true);

	case SPDK_JSON_VAL_FALSE:
		return spdk_json_write_bool(w, false);

	case SPDK_JSON_VAL_NULL:
		return spdk_json_write_null(w);

	case SPDK_JSON_VAL_ARRAY_BEGIN:
	case SPDK_JSON_VAL_OBJECT_BEGIN:
		num_values = val[0].len;

		if (val[0].type == SPDK_JSON_VAL_OBJECT_BEGIN) {
			if (spdk_json_write_object_begin(w)) {
				return fail(w);
			}
		} else {
			if (spdk_json_write_array_begin(w)) {
				return fail(w);
			}
		}

		/* Loop up to and including the _END value */
		for (i = 0; i < num_values + 1;) {
			if (spdk_json_write_val(w, &val[i + 1])) {
				return fail(w);
			}
			if (val[i + 1].type == SPDK_JSON_VAL_ARRAY_BEGIN ||
			    val[i + 1].type == SPDK_JSON_VAL_OBJECT_BEGIN) {
				i += val[i + 1].len + 2;
			} else {
				i++;
			}
		}
		return 0;

	case SPDK_JSON_VAL_ARRAY_END:
		return spdk_json_write_array_end(w);

	case SPDK_JSON_VAL_OBJECT_END:
		return spdk_json_write_object_end(w);

	case SPDK_JSON_VAL_INVALID:
		/* Handle INVALID to make the compiler happy (and catch other unhandled types) */
		return fail(w);
	}

	return fail(w);
}

int spdk_json_write_named_null(struct spdk_json_write_ctx *w, const char *name)
{
	int rc = spdk_json_write_name(w, name);
	return rc ? rc : spdk_json_write_null(w);
}

int spdk_json_write_named_bool(struct spdk_json_write_ctx *w, const char *name, bool val)
{
	int rc = spdk_json_write_name(w, name);

	return rc ? rc : spdk_json_write_bool(w, val);
}

int spdk_json_write_named_int32(struct spdk_json_write_ctx *w, const char *name, int32_t val)
{
	int rc = spdk_json_write_name(w, name);

	return rc ? rc : spdk_json_write_int32(w, val);
}

int spdk_json_write_named_uint32(struct spdk_json_write_ctx *w, const char *name, uint32_t val)
{
	int rc = spdk_json_write_name(w, name);

	return rc ? rc : spdk_json_write_uint32(w, val);
}

int spdk_json_write_named_uint64(struct spdk_json_write_ctx *w, const char *name, uint64_t val)
{
	int rc = spdk_json_write_name(w, name);

	return rc ? rc : spdk_json_write_uint64(w, val);
}

int spdk_json_write_named_int64(struct spdk_json_write_ctx *w, const char *name, int64_t val)
{
	int rc = spdk_json_write_name(w, name);

	return rc ? rc : spdk_json_write_int64(w, val);
}

int spdk_json_write_named_string(struct spdk_json_write_ctx *w, const char *name, const char *val)
{
	int rc = spdk_json_write_name(w, name);

	return rc ? rc : spdk_json_write_string(w, val);
}

int spdk_json_write_named_string_fmt(struct spdk_json_write_ctx *w, const char *name,
				     const char *fmt, ...)
{
	va_list args;
	int rc;

	va_start(args, fmt);
	rc = spdk_json_write_named_string_fmt_v(w, name, fmt, args);
	va_end(args);

	return rc;
}

int spdk_json_write_named_string_fmt_v(struct spdk_json_write_ctx *w, const char *name,
				       const char *fmt, va_list args)
{
	char *s;
	int rc;

	rc = spdk_json_write_name(w, name);
	if (rc) {
		return rc;
	}

	s = spdk_vsprintf_alloc(fmt, args);

	if (s == NULL) {
		return -1;
	}

	rc = spdk_json_write_string(w, s);
	free(s);
	return rc;
}

int spdk_json_write_named_array_begin(struct spdk_json_write_ctx *w, const char *name)
{
	int rc = spdk_json_write_name(w, name);

	return rc ? rc : spdk_json_write_array_begin(w);
}

int spdk_json_write_named_object_begin(struct spdk_json_write_ctx *w, const char *name)
{
	int rc = spdk_json_write_name(w, name);

	return rc ? rc : spdk_json_write_object_begin(w);
}
