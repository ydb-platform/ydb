#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) 2005-2007, Nick Galbreath
 *   Copyright (c) 2013-2017, Alfred Klomp
 *   Copyright (c) 2015-2017, Wojciech Mula
 *   Copyright (c) 2016-2017, Matthieu Darbois
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions are
 *   met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 *   TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 *   PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 *   TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __aarch64__
#error Unsupported hardware
#endif

#include "spdk/stdinc.h"
/*
 * Encoding
 * Use a 64-byte lookup to do the encoding.
 * Reuse existing base64_dec_table and base64_dec_table.

 * Decoding
 * The input consists of five valid character sets in the Base64 alphabet,
 * which we need to map back to the 6-bit values they represent.
 * There are three ranges, two singles, and then there's the rest.
 *
 * LUT1[0-63] = base64_dec_table_neon64[0-63]
 * LUT2[0-63] = base64_dec_table_neon64[64-127]
 *   #  From       To        LUT  Characters
 *   1  [0..42]    [255]      #1  invalid input
 *   2  [43]       [62]       #1  +
 *   3  [44..46]   [255]      #1  invalid input
 *   4  [47]       [63]       #1  /
 *   5  [48..57]   [52..61]   #1  0..9
 *   6  [58..63]   [255]      #1  invalid input
 *   7  [64]       [255]      #2  invalid input
 *   8  [65..90]   [0..25]    #2  A..Z
 *   9  [91..96]   [255]      #2 invalid input
 *  10  [97..122]  [26..51]   #2  a..z
 *  11  [123..126] [255]      #2 invalid input
 * (12) Everything else => invalid input
 */
static const uint8_t base64_dec_table_neon64[] = {
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,  62, 255, 255, 255,  63,
	52,  53,  54,  55,  56,  57,  58,  59,  60,  61, 255, 255, 255, 255, 255, 255,
	0, 255,   0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,
	14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25, 255, 255, 255, 255,
	255, 255,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,
	40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51, 255, 255, 255, 255
};

/*
 * LUT1[0-63] = base64_urlsafe_dec_table_neon64[0-63]
 * LUT2[0-63] = base64_urlsafe_dec_table_neon64[64-127]
 *   #  From       To        LUT  Characters
 *   1  [0..44]    [255]      #1  invalid input
 *   2  [45]       [62]       #1  -
 *   3  [46..47]   [255]      #1  invalid input
 *   5  [48..57]   [52..61]   #1  0..9
 *   6  [58..63]   [255]      #1  invalid input
 *   7  [64]       [255]      #2  invalid input
 *   8  [65..90]   [0..25]    #2  A..Z
 *   9  [91..94]   [255]      #2  invalid input
 *  10  [95]       [63]       #2  _
 *  11  [96]       [255]      #2  invalid input
 *  12  [97..122]  [26..51]   #2  a..z
 *  13  [123..126] [255]      #2 invalid input
 * (14) Everything else => invalid input
 */
static const uint8_t base64_urlsafe_dec_table_neon64[] = {
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,  62, 255, 255,
	52,  53,  54,  55,  56,  57,  58,  59,  60,  61, 255, 255, 255, 255, 255, 255,
	0, 255,   0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,
	14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25, 255, 255, 255, 255,
	63, 255,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,
	40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51, 255, 255, 255, 255
};

#include <arm_neon.h>
#define CMPGT(s,n)      vcgtq_u8((s), vdupq_n_u8(n))

static inline uint8x16x4_t
load_64byte_table(const uint8_t *p)
{
	uint8x16x4_t ret;
	ret.val[0] = vld1q_u8(p +  0);
	ret.val[1] = vld1q_u8(p + 16);
	ret.val[2] = vld1q_u8(p + 32);
	ret.val[3] = vld1q_u8(p + 48);
	return ret;
}

static void
base64_encode_neon64(char **dst, const char *enc_table, const void **src, size_t *src_len)
{
	const uint8x16x4_t tbl_enc = load_64byte_table(enc_table);

	while (*src_len >= 48) {
		uint8x16x3_t str;
		uint8x16x4_t res;

		/* Load 48 bytes and deinterleave */
		str = vld3q_u8((uint8_t *)*src);

		/* Divide bits of three input bytes over four output bytes and clear top two bits */
		res.val[0] = vshrq_n_u8(str.val[0], 2);
		res.val[1] = vandq_u8(vorrq_u8(vshrq_n_u8(str.val[1], 4), vshlq_n_u8(str.val[0], 4)),
				      vdupq_n_u8(0x3F));
		res.val[2] = vandq_u8(vorrq_u8(vshrq_n_u8(str.val[2], 6), vshlq_n_u8(str.val[1], 2)),
				      vdupq_n_u8(0x3F));
		res.val[3] = vandq_u8(str.val[2], vdupq_n_u8(0x3F));

		/*
		 * The bits have now been shifted to the right locations;
		 * translate their values 0..63 to the Base64 alphabet.
		 * Use a 64-byte table lookup:
		 */
		res.val[0] = vqtbl4q_u8(tbl_enc, res.val[0]);
		res.val[1] = vqtbl4q_u8(tbl_enc, res.val[1]);
		res.val[2] = vqtbl4q_u8(tbl_enc, res.val[2]);
		res.val[3] = vqtbl4q_u8(tbl_enc, res.val[3]);

		/* Interleave and store result */
		vst4q_u8((uint8_t *)*dst, res);

		*src += 48;      /* 3 * 16 bytes of input */
		*dst += 64;      /* 4 * 16 bytes of output */
		*src_len -= 48;
	}
}

static void
base64_decode_neon64(void **dst, const uint8_t *dec_table_neon64, const uint8_t **src,
		     size_t *src_len)
{
	/*
	 * First LUT tbl_dec1 will use VTBL instruction (out of range indices are set to 0 in destination).
	 * Second LUT tbl_dec2 will use VTBX instruction (out of range indices will be unchanged in destination).
	 * Input [64..126] will be mapped to index [1..63] in tb1_dec2. Index 0 means that value comes from tb1_dec1.
	 */
	const uint8x16x4_t tbl_dec1 = load_64byte_table(dec_table_neon64);
	const uint8x16x4_t tbl_dec2 = load_64byte_table(dec_table_neon64 + 64);
	const uint8x16_t offset = vdupq_n_u8(63U);

	while (*src_len >= 64) {

		uint8x16x4_t dec1, dec2;
		uint8x16x3_t dec;

		/* Load 64 bytes and deinterleave */
		uint8x16x4_t str = vld4q_u8((uint8_t *)*src);

		/* Get indices for 2nd LUT */
		dec2.val[0] = vqsubq_u8(str.val[0], offset);
		dec2.val[1] = vqsubq_u8(str.val[1], offset);
		dec2.val[2] = vqsubq_u8(str.val[2], offset);
		dec2.val[3] = vqsubq_u8(str.val[3], offset);

		/* Get values from 1st LUT */
		dec1.val[0] = vqtbl4q_u8(tbl_dec1, str.val[0]);
		dec1.val[1] = vqtbl4q_u8(tbl_dec1, str.val[1]);
		dec1.val[2] = vqtbl4q_u8(tbl_dec1, str.val[2]);
		dec1.val[3] = vqtbl4q_u8(tbl_dec1, str.val[3]);

		/* Get values from 2nd LUT */
		dec2.val[0] = vqtbx4q_u8(dec2.val[0], tbl_dec2, dec2.val[0]);
		dec2.val[1] = vqtbx4q_u8(dec2.val[1], tbl_dec2, dec2.val[1]);
		dec2.val[2] = vqtbx4q_u8(dec2.val[2], tbl_dec2, dec2.val[2]);
		dec2.val[3] = vqtbx4q_u8(dec2.val[3], tbl_dec2, dec2.val[3]);

		/* Get final values */
		str.val[0] = vorrq_u8(dec1.val[0], dec2.val[0]);
		str.val[1] = vorrq_u8(dec1.val[1], dec2.val[1]);
		str.val[2] = vorrq_u8(dec1.val[2], dec2.val[2]);
		str.val[3] = vorrq_u8(dec1.val[3], dec2.val[3]);

		/* Check for invalid input, any value larger than 63 */
		uint8x16_t classified = CMPGT(str.val[0], 63);
		classified = vorrq_u8(classified, CMPGT(str.val[1], 63));
		classified = vorrq_u8(classified, CMPGT(str.val[2], 63));
		classified = vorrq_u8(classified, CMPGT(str.val[3], 63));

		/* check that all bits are zero */
		if (vmaxvq_u8(classified) != 0U) {
			break;
		}

		/* Compress four bytes into three */
		dec.val[0] = vorrq_u8(vshlq_n_u8(str.val[0], 2), vshrq_n_u8(str.val[1], 4));
		dec.val[1] = vorrq_u8(vshlq_n_u8(str.val[1], 4), vshrq_n_u8(str.val[2], 2));
		dec.val[2] = vorrq_u8(vshlq_n_u8(str.val[2], 6), str.val[3]);

		/* Interleave and store decoded result */
		vst3q_u8((uint8_t *)*dst, dec);

		*src += 64;
		*dst += 48;
		*src_len -= 64;
	}
}
