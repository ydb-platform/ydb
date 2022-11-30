#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright(c) Intel Corporation. All rights reserved.
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

#include "spdk/stdinc.h"
#include "spdk/endian.h"
#include "spdk/base64.h"

#ifdef __aarch64__
#include "base64_neon.c"
#endif

#define BASE64_ENC_BITMASK 0x3FUL
#define BASE64_PADDING_CHAR '='

static const char base64_enc_table[] =
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	"abcdefghijklmnopqrstuvwxyz"
	"0123456789+/";

static const char base64_urfsafe_enc_table[] =
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	"abcdefghijklmnopqrstuvwxyz"
	"0123456789-_";

static const uint8_t
base64_dec_table[] = {
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,  62, 255, 255, 255,  63,
	52,  53,  54,  55,  56,  57,  58,  59,  60,  61, 255, 255, 255, 255, 255, 255,
	255,   0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,
	15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25, 255, 255, 255, 255, 255,
	255,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,
	41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
};

static const uint8_t
base64_urlsafe_dec_table[] = {
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,  62, 255, 255,
	52,  53,  54,  55,  56,  57,  58,  59,  60,  61, 255, 255, 255, 255, 255, 255,
	255,   0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,
	15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25, 255, 255, 255, 255,  63,
	255,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,
	41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
};

static int
base64_encode(char *dst, const char *enc_table, const void *src, size_t src_len)
{
	uint32_t raw_u32;

	if (!dst || !src || src_len <= 0) {
		return -EINVAL;
	}

#ifdef __aarch64__
	base64_encode_neon64(&dst, enc_table, &src, &src_len);
#endif

	while (src_len >= 4) {
		raw_u32 = from_be32(src);

		*dst++ = enc_table[(raw_u32 >> 26) & BASE64_ENC_BITMASK];
		*dst++ = enc_table[(raw_u32 >> 20) & BASE64_ENC_BITMASK];
		*dst++ = enc_table[(raw_u32 >> 14) & BASE64_ENC_BITMASK];
		*dst++ = enc_table[(raw_u32 >> 8) & BASE64_ENC_BITMASK];

		src_len -= 3;
		src += 3;
	}

	if (src_len == 0) {
		goto out;
	}

	raw_u32 = 0;
	memcpy(&raw_u32, src, src_len);
	raw_u32 = from_be32(&raw_u32);

	*dst++ = enc_table[(raw_u32 >> 26) & BASE64_ENC_BITMASK];
	*dst++ = enc_table[(raw_u32 >> 20) & BASE64_ENC_BITMASK];
	*dst++ = (src_len >= 2) ? enc_table[(raw_u32 >> 14) & BASE64_ENC_BITMASK] : BASE64_PADDING_CHAR;
	*dst++ = (src_len == 3) ? enc_table[(raw_u32 >> 8) & BASE64_ENC_BITMASK] : BASE64_PADDING_CHAR;

out:
	*dst = '\0';

	return 0;
}

int
spdk_base64_encode(char *dst, const void *src, size_t src_len)
{
	return base64_encode(dst, base64_enc_table, src, src_len);
}

int
spdk_base64_urlsafe_encode(char *dst, const void *src, size_t src_len)
{
	return base64_encode(dst, base64_urfsafe_enc_table, src, src_len);
}

#ifdef __aarch64__
static int
base64_decode(void *dst, size_t *_dst_len, const uint8_t *dec_table,
	      const uint8_t *dec_table_opt, const char *src)
#else
static int
base64_decode(void *dst, size_t *_dst_len, const uint8_t *dec_table, const char *src)
#endif
{
	size_t src_strlen;
	size_t tail_len = 0;
	const uint8_t *src_in;
	uint32_t tmp[4];
	int i;

	if (!src) {
		return -EINVAL;
	}

	src_strlen = strlen(src);

	/* strlen of src should be 4n */
	if (src_strlen == 0 || src_strlen % 4 != 0) {
		return -EINVAL;
	}

	/* Consider Base64 padding, it at most has 2 padding characters. */
	for (i = 0; i < 2; i++) {
		if (src[src_strlen - 1] != BASE64_PADDING_CHAR) {
			break;
		}
		src_strlen--;
	}

	/* strlen of src without padding shouldn't be 4n+1 */
	if (src_strlen == 0 || src_strlen % 4 == 1) {
		return -EINVAL;
	}

	if (_dst_len) {
		*_dst_len = spdk_base64_get_decoded_len(src_strlen);
	}

	/* If dst is NULL, the client is only concerned w/ _dst_len, return */
	if (!dst) {
		return 0;
	}

	src_in = (const uint8_t *) src;

#ifdef __aarch64__
	base64_decode_neon64(&dst, dec_table_opt, &src_in, &src_strlen);

	if (src_strlen == 0) {
		return 0;
	}
#endif

	/* space of dst can be used by to_be32 */
	while (src_strlen > 4) {
		tmp[0] = dec_table[*src_in++];
		tmp[1] = dec_table[*src_in++];
		tmp[2] = dec_table[*src_in++];
		tmp[3] = dec_table[*src_in++];

		if (tmp[0] == 255 || tmp[1] == 255 || tmp[2] == 255 || tmp[3] == 255) {
			return -EINVAL;
		}

		to_be32(dst, tmp[3] << 8 | tmp[2] << 14 | tmp[1] << 20 | tmp[0] << 26);

		dst += 3;
		src_strlen -= 4;
	}

	/* space of dst is not enough to be used by to_be32 */
	tmp[0] = dec_table[src_in[0]];
	tmp[1] = dec_table[src_in[1]];
	tmp[2] = (src_strlen >= 3) ? dec_table[src_in[2]] : 0;
	tmp[3] = (src_strlen == 4) ? dec_table[src_in[3]] : 0;
	tail_len = src_strlen - 1;

	if (tmp[0] == 255 || tmp[1] == 255 || tmp[2] == 255 || tmp[3] == 255) {
		return -EINVAL;
	}

	to_be32(&tmp[3], tmp[3] << 8 | tmp[2] << 14 | tmp[1] << 20 | tmp[0] << 26);
	memcpy(dst, (uint8_t *)&tmp[3], tail_len);

	return 0;
}

int
spdk_base64_decode(void *dst, size_t *dst_len, const char *src)
{
#ifdef __aarch64__
	return base64_decode(dst, dst_len, base64_dec_table, base64_dec_table_neon64, src);
#else
	return base64_decode(dst, dst_len, base64_dec_table, src);
#endif
}

int
spdk_base64_urlsafe_decode(void *dst, size_t *dst_len, const char *src)
{
#ifdef __aarch64__
	return base64_decode(dst, dst_len, base64_urlsafe_dec_table, base64_urlsafe_dec_table_neon64,
			     src);
#else
	return base64_decode(dst, dst_len, base64_urlsafe_dec_table, src);
#endif
}
