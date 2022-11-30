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

#define SPDK_JSON_MAX_NESTING_DEPTH	64

static int
hex_value(uint8_t c)
{
#define V(x, y) [x] = y + 1
	static const int8_t val[256] = {
		V('0', 0), V('1', 1), V('2', 2), V('3', 3), V('4', 4),
		V('5', 5), V('6', 6), V('7', 7), V('8', 8), V('9', 9),
		V('A', 0xA), V('B', 0xB), V('C', 0xC), V('D', 0xD), V('E', 0xE), V('F', 0xF),
		V('a', 0xA), V('b', 0xB), V('c', 0xC), V('d', 0xD), V('e', 0xE), V('f', 0xF),
	};
#undef V

	return val[c] - 1;
}

static int
json_decode_string_escape_unicode(uint8_t **strp, uint8_t *buf_end, uint8_t *out)
{
	uint8_t *str = *strp;
	int v0, v1, v2, v3;
	uint32_t val;
	uint32_t surrogate_high = 0;
	int rc;
decode:
	/* \uXXXX */
	assert(buf_end > str);

	if (*str++ != '\\') { return SPDK_JSON_PARSE_INVALID; }
	if (buf_end == str) { return SPDK_JSON_PARSE_INCOMPLETE; }

	if (*str++ != 'u') { return SPDK_JSON_PARSE_INVALID; }
	if (buf_end == str) { return SPDK_JSON_PARSE_INCOMPLETE; }

	if ((v3 = hex_value(*str++)) < 0) { return SPDK_JSON_PARSE_INVALID; }
	if (buf_end == str) { return SPDK_JSON_PARSE_INCOMPLETE; }

	if ((v2 = hex_value(*str++)) < 0) { return SPDK_JSON_PARSE_INVALID; }
	if (buf_end == str) { return SPDK_JSON_PARSE_INCOMPLETE; }

	if ((v1 = hex_value(*str++)) < 0) { return SPDK_JSON_PARSE_INVALID; }
	if (buf_end == str) { return SPDK_JSON_PARSE_INCOMPLETE; }

	if ((v0 = hex_value(*str++)) < 0) { return SPDK_JSON_PARSE_INVALID; }
	if (buf_end == str) { return SPDK_JSON_PARSE_INCOMPLETE; }

	val = v0 | (v1 << 4) | (v2 << 8) | (v3 << 12);

	if (surrogate_high) {
		/* We already parsed the high surrogate, so this should be the low part. */
		if (!utf16_valid_surrogate_low(val)) {
			return SPDK_JSON_PARSE_INVALID;
		}

		/* Convert UTF-16 surrogate pair into codepoint and fall through to utf8_encode. */
		val = utf16_decode_surrogate_pair(surrogate_high, val);
	} else if (utf16_valid_surrogate_high(val)) {
		surrogate_high = val;

		/*
		 * We parsed a \uXXXX sequence that decoded to the first half of a
		 *  UTF-16 surrogate pair, so it must be immediately followed by another
		 *  \uXXXX escape.
		 *
		 * Loop around to get the low half of the surrogate pair.
		 */
		if (buf_end == str) { return SPDK_JSON_PARSE_INCOMPLETE; }
		goto decode;
	} else if (utf16_valid_surrogate_low(val)) {
		/*
		 * We found the second half of surrogate pair without the first half;
		 *  this is an invalid encoding.
		 */
		return SPDK_JSON_PARSE_INVALID;
	}

	/*
	 * Convert Unicode escape (or surrogate pair) to UTF-8 in place.
	 *
	 * This is safe (will not write beyond the buffer) because the \uXXXX sequence is 6 bytes
	 *  (or 12 bytes for surrogate pairs), and the longest possible UTF-8 encoding of a
	 *  single codepoint is 4 bytes.
	 */
	if (out) {
		rc = utf8_encode_unsafe(out, val);
	} else {
		rc = utf8_codepoint_len(val);
	}
	if (rc < 0) {
		return SPDK_JSON_PARSE_INVALID;
	}

	*strp = str; /* update input pointer */
	return rc; /* return number of bytes decoded */
}

static int
json_decode_string_escape_twochar(uint8_t **strp, uint8_t *buf_end, uint8_t *out)
{
	static const uint8_t escapes[256] = {
		['b'] = '\b',
		['f'] = '\f',
		['n'] = '\n',
		['r'] = '\r',
		['t'] = '\t',
		['/'] = '/',
		['"'] = '"',
		['\\'] = '\\',
	};
	uint8_t *str = *strp;
	uint8_t c;

	assert(buf_end > str);
	if (buf_end - str < 2) {
		return SPDK_JSON_PARSE_INCOMPLETE;
	}

	assert(str[0] == '\\');

	c = escapes[str[1]];
	if (c) {
		if (out) {
			*out = c;
		}
		*strp += 2; /* consumed two bytes */
		return 1; /* produced one byte */
	}

	return SPDK_JSON_PARSE_INVALID;
}

/*
 * Decode JSON string backslash escape.
 * \param strp pointer to pointer to first character of escape (the backslash).
 *  *strp is also advanced to indicate how much input was consumed.
 *
 * \return Number of bytes appended to out
 */
static int
json_decode_string_escape(uint8_t **strp, uint8_t *buf_end, uint8_t *out)
{
	int rc;

	rc = json_decode_string_escape_twochar(strp, buf_end, out);
	if (rc > 0) {
		return rc;
	}

	return json_decode_string_escape_unicode(strp, buf_end, out);
}

/*
 * Decode JSON string in place.
 *
 * \param str_start Pointer to the beginning of the string (the opening " character).
 *
 * \return Number of bytes in decoded string (beginning from start).
 */
static int
json_decode_string(uint8_t *str_start, uint8_t *buf_end, uint8_t **str_end, uint32_t flags)
{
	uint8_t *str = str_start;
	uint8_t *out = str_start + 1; /* Decode string in place (skip the initial quote) */
	int rc;

	if (buf_end - str_start < 2) {
		/*
		 * Shortest valid string (the empty string) is two bytes (""),
		 *  so this can't possibly be valid
		 */
		*str_end = str;
		return SPDK_JSON_PARSE_INCOMPLETE;
	}

	if (*str++ != '"') {
		*str_end = str;
		return SPDK_JSON_PARSE_INVALID;
	}

	while (str < buf_end) {
		if (str[0] == '"') {
			/*
			 * End of string.
			 * Update str_end to point at next input byte and return output length.
			 */
			*str_end = str + 1;
			return out - str_start - 1;
		} else if (str[0] == '\\') {
			rc = json_decode_string_escape(&str, buf_end,
						       flags & SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE ? out : NULL);
			assert(rc != 0);
			if (rc < 0) {
				*str_end = str;
				return rc;
			}
			out += rc;
		} else if (str[0] <= 0x1f) {
			/* control characters must be escaped */
			*str_end = str;
			return SPDK_JSON_PARSE_INVALID;
		} else {
			rc = utf8_valid(str, buf_end);
			if (rc == 0) {
				*str_end = str;
				return SPDK_JSON_PARSE_INCOMPLETE;
			} else if (rc < 0) {
				*str_end = str;
				return SPDK_JSON_PARSE_INVALID;
			}

			if (out && out != str && (flags & SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE)) {
				memmove(out, str, rc);
			}
			out += rc;
			str += rc;
		}
	}

	/* If execution gets here, we ran out of buffer. */
	*str_end = str;
	return SPDK_JSON_PARSE_INCOMPLETE;
}

static int
json_valid_number(uint8_t *start, uint8_t *buf_end)
{
	uint8_t *p = start;
	uint8_t c;

	if (p >= buf_end) { return -1; }

	c = *p++;
	if (c >= '1' && c <= '9') { goto num_int_digits; }
	if (c == '0') { goto num_frac_or_exp; }
	if (c == '-') { goto num_int_first_digit; }
	p--;
	goto done_invalid;

num_int_first_digit:
	if (spdk_likely(p != buf_end)) {
		c = *p++;
		if (c == '0') { goto num_frac_or_exp; }
		if (c >= '1' && c <= '9') { goto num_int_digits; }
		p--;
	}
	goto done_invalid;

num_int_digits:
	if (spdk_likely(p != buf_end)) {
		c = *p++;
		if (c >= '0' && c <= '9') { goto num_int_digits; }
		if (c == '.') { goto num_frac_first_digit; }
		if (c == 'e' || c == 'E') { goto num_exp_sign; }
		p--;
	}
	goto done_valid;

num_frac_or_exp:
	if (spdk_likely(p != buf_end)) {
		c = *p++;
		if (c == '.') { goto num_frac_first_digit; }
		if (c == 'e' || c == 'E') { goto num_exp_sign; }
		p--;
	}
	goto done_valid;

num_frac_first_digit:
	if (spdk_likely(p != buf_end)) {
		c = *p++;
		if (c >= '0' && c <= '9') { goto num_frac_digits; }
		p--;
	}
	goto done_invalid;

num_frac_digits:
	if (spdk_likely(p != buf_end)) {
		c = *p++;
		if (c >= '0' && c <= '9') { goto num_frac_digits; }
		if (c == 'e' || c == 'E') { goto num_exp_sign; }
		p--;
	}
	goto done_valid;

num_exp_sign:
	if (spdk_likely(p != buf_end)) {
		c = *p++;
		if (c >= '0' && c <= '9') { goto num_exp_digits; }
		if (c == '-' || c == '+') { goto num_exp_first_digit; }
		p--;
	}
	goto done_invalid;

num_exp_first_digit:
	if (spdk_likely(p != buf_end)) {
		c = *p++;
		if (c >= '0' && c <= '9') { goto num_exp_digits; }
		p--;
	}
	goto done_invalid;

num_exp_digits:
	if (spdk_likely(p != buf_end)) {
		c = *p++;
		if (c >= '0' && c <= '9') { goto num_exp_digits; }
		p--;
	}
	goto done_valid;

done_valid:
	/* Valid end state */
	return p - start;

done_invalid:
	/* Invalid end state */
	if (p == buf_end) {
		/* Hit the end of the buffer - the stream is incomplete. */
		return SPDK_JSON_PARSE_INCOMPLETE;
	}

	/* Found an invalid character in an invalid end state */
	return SPDK_JSON_PARSE_INVALID;
}

static int
json_valid_comment(const uint8_t *start, const uint8_t *buf_end)
{
	const uint8_t *p = start;
	bool multiline;

	assert(buf_end > p);
	if (buf_end - p < 2) {
		return SPDK_JSON_PARSE_INCOMPLETE;
	}

	if (p[0] != '/') {
		return SPDK_JSON_PARSE_INVALID;
	}
	if (p[1] == '*') {
		multiline = true;
	} else if (p[1] == '/') {
		multiline = false;
	} else {
		return SPDK_JSON_PARSE_INVALID;
	}
	p += 2;

	if (multiline) {
		while (p != buf_end - 1) {
			if (p[0] == '*' && p[1] == '/') {
				/* Include the terminating star and slash in the comment */
				return p - start + 2;
			}
			p++;
		}
	} else {
		while (p != buf_end) {
			if (*p == '\r' || *p == '\n') {
				/* Do not include the line terminator in the comment */
				return p - start;
			}
			p++;
		}
	}

	return SPDK_JSON_PARSE_INCOMPLETE;
}

struct json_literal {
	enum spdk_json_val_type type;
	uint32_t len;
	uint8_t str[8];
};

/*
 * JSON only defines 3 possible literals; they can be uniquely identified by bits
 *  3 and 4 of the first character:
 *   'f' = 0b11[00]110
 *   'n' = 0b11[01]110
 *   't' = 0b11[10]100
 * These two bits can be used as an index into the g_json_literals array.
 */
static const struct json_literal g_json_literals[] = {
	{SPDK_JSON_VAL_FALSE, 5, "false"},
	{SPDK_JSON_VAL_NULL,  4, "null"},
	{SPDK_JSON_VAL_TRUE,  4, "true"},
	{}
};

static int
match_literal(const uint8_t *start, const uint8_t *end, const uint8_t *literal, size_t len)
{
	assert(end >= start);
	if ((size_t)(end - start) < len) {
		return SPDK_JSON_PARSE_INCOMPLETE;
	}

	if (memcmp(start, literal, len) != 0) {
		return SPDK_JSON_PARSE_INVALID;
	}

	return len;
}

ssize_t
spdk_json_parse(void *json, size_t size, struct spdk_json_val *values, size_t num_values,
		void **end, uint32_t flags)
{
	uint8_t *json_end = json + size;
	enum spdk_json_val_type containers[SPDK_JSON_MAX_NESTING_DEPTH];
	size_t con_value[SPDK_JSON_MAX_NESTING_DEPTH];
	enum spdk_json_val_type con_type = SPDK_JSON_VAL_INVALID;
	bool trailing_comma = false;
	size_t depth = 0; /* index into containers */
	size_t cur_value = 0; /* index into values */
	size_t con_start_value;
	uint8_t *data = json;
	uint8_t *new_data;
	int rc = 0;
	const struct json_literal *lit;
	enum {
		STATE_VALUE, /* initial state */
		STATE_VALUE_SEPARATOR, /* value separator (comma) */
		STATE_NAME, /* "name": value */
		STATE_NAME_SEPARATOR, /* colon */
		STATE_END, /* parsed the complete value, so only whitespace is valid */
	} state = STATE_VALUE;

#define ADD_VALUE(t, val_start_ptr, val_end_ptr) \
	if (values && cur_value < num_values) { \
		values[cur_value].type = t; \
		values[cur_value].start = val_start_ptr; \
		values[cur_value].len = val_end_ptr - val_start_ptr; \
	} \
	cur_value++

	while (data < json_end) {
		uint8_t c = *data;

		switch (c) {
		case ' ':
		case '\t':
		case '\r':
		case '\n':
			/* Whitespace is allowed between any tokens. */
			data++;
			break;

		case 't':
		case 'f':
		case 'n':
			/* true, false, or null */
			if (state != STATE_VALUE) { goto done_invalid; }
			lit = &g_json_literals[(c >> 3) & 3]; /* See comment above g_json_literals[] */
			assert(lit->str[0] == c);
			rc = match_literal(data, json_end, lit->str, lit->len);
			if (rc < 0) { goto done_rc; }
			ADD_VALUE(lit->type, data, data + rc);
			data += rc;
			state = depth ? STATE_VALUE_SEPARATOR : STATE_END;
			trailing_comma = false;
			break;

		case '"':
			if (state != STATE_VALUE && state != STATE_NAME) { goto done_invalid; }
			rc = json_decode_string(data, json_end, &new_data, flags);
			if (rc < 0) {
				data = new_data;
				goto done_rc;
			}
			/*
			 * Start is data + 1 to skip initial quote.
			 * Length is data + rc - 1 to skip both quotes.
			 */
			ADD_VALUE(state == STATE_VALUE ? SPDK_JSON_VAL_STRING : SPDK_JSON_VAL_NAME,
				  data + 1, data + rc - 1);
			data = new_data;
			if (state == STATE_NAME) {
				state = STATE_NAME_SEPARATOR;
			} else {
				state = depth ? STATE_VALUE_SEPARATOR : STATE_END;
			}
			trailing_comma = false;
			break;

		case '-':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			if (state != STATE_VALUE) { goto done_invalid; }
			rc = json_valid_number(data, json_end);
			if (rc < 0) { goto done_rc; }
			ADD_VALUE(SPDK_JSON_VAL_NUMBER, data, data + rc);
			data += rc;
			state = depth ? STATE_VALUE_SEPARATOR : STATE_END;
			trailing_comma = false;
			break;

		case '{':
		case '[':
			if (state != STATE_VALUE) { goto done_invalid; }
			if (depth == SPDK_JSON_MAX_NESTING_DEPTH) {
				rc = SPDK_JSON_PARSE_MAX_DEPTH_EXCEEDED;
				goto done_rc;
			}
			if (c == '{') {
				con_type = SPDK_JSON_VAL_OBJECT_BEGIN;
				state = STATE_NAME;
			} else {
				con_type = SPDK_JSON_VAL_ARRAY_BEGIN;
				state = STATE_VALUE;
			}
			con_value[depth] = cur_value;
			containers[depth++] = con_type;
			ADD_VALUE(con_type, data, data + 1);
			data++;
			trailing_comma = false;
			break;

		case '}':
		case ']':
			if (trailing_comma) { goto done_invalid; }
			if (depth == 0) { goto done_invalid; }
			con_type = containers[--depth];
			con_start_value = con_value[depth];
			if (values && con_start_value < num_values) {
				values[con_start_value].len = cur_value - con_start_value - 1;
			}
			if (c == '}') {
				if (state != STATE_NAME && state != STATE_VALUE_SEPARATOR) {
					goto done_invalid;
				}
				if (con_type != SPDK_JSON_VAL_OBJECT_BEGIN) {
					goto done_invalid;
				}
				ADD_VALUE(SPDK_JSON_VAL_OBJECT_END, data, data + 1);
			} else {
				if (state != STATE_VALUE && state != STATE_VALUE_SEPARATOR) {
					goto done_invalid;
				}
				if (con_type != SPDK_JSON_VAL_ARRAY_BEGIN) {
					goto done_invalid;
				}
				ADD_VALUE(SPDK_JSON_VAL_ARRAY_END, data, data + 1);
			}
			con_type = depth == 0 ? SPDK_JSON_VAL_INVALID : containers[depth - 1];
			data++;
			state = depth ? STATE_VALUE_SEPARATOR : STATE_END;
			trailing_comma = false;
			break;

		case ',':
			if (state != STATE_VALUE_SEPARATOR) { goto done_invalid; }
			data++;
			assert(con_type == SPDK_JSON_VAL_ARRAY_BEGIN ||
			       con_type == SPDK_JSON_VAL_OBJECT_BEGIN);
			state = con_type == SPDK_JSON_VAL_ARRAY_BEGIN ? STATE_VALUE : STATE_NAME;
			trailing_comma = true;
			break;

		case ':':
			if (state != STATE_NAME_SEPARATOR) { goto done_invalid; }
			data++;
			state = STATE_VALUE;
			break;

		case '/':
			if (!(flags & SPDK_JSON_PARSE_FLAG_ALLOW_COMMENTS)) {
				goto done_invalid;
			}
			rc = json_valid_comment(data, json_end);
			if (rc < 0) { goto done_rc; }
			/* Skip over comment */
			data += rc;
			break;

		default:
			goto done_invalid;
		}

		if (state == STATE_END) {
			break;
		}
	}

	if (state == STATE_END) {
		/* Skip trailing whitespace */
		while (data < json_end) {
			uint8_t c = *data;

			if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
				data++;
			} else {
				break;
			}
		}

		/*
		 * These asserts are just for sanity checking - they are guaranteed by the allowed
		 *  state transitions.
		 */
		assert(depth == 0);
		assert(trailing_comma == false);
		assert(data <= json_end);
		if (end) {
			*end = data;
		}
		return cur_value;
	}

	/* Invalid end state - ran out of data */
	rc = SPDK_JSON_PARSE_INCOMPLETE;

done_rc:
	assert(rc < 0);
	if (end) {
		*end = data;
	}
	return rc;

done_invalid:
	rc = SPDK_JSON_PARSE_INVALID;
	goto done_rc;
}
