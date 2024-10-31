/**************************************************************
  Copyright (c) 2021 Linaro Ltd.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Huawei Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/
.text
.align		6
.arch		armv8-a+sve

#include "../include/aarch64_label.h"

.global cdecl(gf_5vect_mad_sve)
#ifndef __APPLE__
.type gf_5vect_mad_sve, %function
#endif

/* gf_5vect_mad_sve(int len, int vec, int vec_i, unsigned char *gftbls,
			     unsigned char *src, unsigned char **dest);
 */
/* arguments */
x_len		.req	x0
x_vec		.req	x1
x_vec_i		.req	x2
x_tbl		.req	x3
x_src		.req	x4
x_dest		.req	x5

/* returns */
w_ret		.req	w0

/* local variables */
x_pos		.req	x6
x_dest2		.req	x7
x_dest3		.req	x8
x_dest4		.req	x9
x_dest5		.req	x10
x_dest1		.req	x12

/* vectors */
z_mask0f	.req	z0

z_src		.req	z1
z_src_lo	.req	z2
z_src_hi	.req	z_src

z_dest1		.req	z3

z_tmp_lo	.req	z4
z_tmp_hi	.req	z5

z_gft1_lo	.req	z6
z_gft1_hi	.req	z7
q_gft1_lo	.req	q6
q_gft1_hi	.req	q7

/* bottom 64-bit of v8..v15 must be preserved if used */
z_gft2_lo	.req	z17
z_gft2_hi	.req	z18
q_gft2_lo	.req	q17
q_gft2_hi	.req	q18

z_gft3_lo	.req	z19
z_gft3_hi	.req	z20
q_gft3_lo	.req	q19
q_gft3_hi	.req	q20

z_gft4_lo	.req	z21
z_gft4_hi	.req	z22
q_gft4_lo	.req	q21
q_gft4_hi	.req	q22

z_gft5_lo	.req	z23
z_gft5_hi	.req	z24
q_gft5_lo	.req	q23
q_gft5_hi	.req	q24

z_dest2		.req	z27
z_dest3		.req	z28
z_dest4		.req	z29
z_dest5		.req	z30

cdecl(gf_5vect_mad_sve):
	/* less than 16 bytes, return_fail */
	cmp	x_len, #16
	blt	.return_fail

	mov	z_mask0f.b, #0x0f		/* z_mask0f = 0x0F0F...0F */
	/* load table 1 */
	add	x_tbl, x_tbl, x_vec_i, LSL #5	/* x_tbl += x_vec_i * 2^5 */

	/* Load with NEON instruction ldp */
	ldp	q_gft1_lo, q_gft1_hi, [x_tbl]
	/* load table 2 */
	add	x_tbl, x_tbl, x_vec, LSL #5	/* x_tbl += x_vec * 2^5 */
	ldp	q_gft2_lo, q_gft2_hi, [x_tbl]
	/* load table 3 */
	add	x_tbl, x_tbl, x_vec, LSL #5	/* x_tbl += x_vec * 2^5 */
	ldp	q_gft3_lo, q_gft3_hi, [x_tbl]
	/* load table 4 */
	add	x_tbl, x_tbl, x_vec, LSL #5	/* x_tbl += x_vec * 2^5 */
	ldp	q_gft4_lo, q_gft4_hi, [x_tbl]
	/* load table 5 */
	add	x_tbl, x_tbl, x_vec, LSL #5	/* x_tbl += x_vec * 2^5 */
	ldp	q_gft5_lo, q_gft5_hi, [x_tbl]

	ldr	x_dest1, [x_dest, #8*0]		/* pointer to dest1 */
	ldr	x_dest2, [x_dest, #8*1]		/* pointer to dest2 */
	ldr	x_dest3, [x_dest, #8*2]		/* pointer to dest3 */
	ldr	x_dest4, [x_dest, #8*3]		/* pointer to dest4 */
	ldr	x_dest5, [x_dest, #8*4]		/* pointer to dest5 */

	mov	x_pos, #0

	/* vector length agnostic */
.Lloopsve_vl:
	whilelo	p0.b, x_pos, x_len
	b.none	.return_pass

	prfb	pldl2strm, p0, [x_dest1, x_pos]
	prfb	pldl2strm, p0, [x_dest2, x_pos]

	/* load src data, governed by p0 */
	ld1b	z_src.b,  p0/z, [x_src, x_pos]

	/* split 4-bit lo; 4-bit hi */
	and	z_src_lo.d, z_src.d, z_mask0f.d
	lsr	z_src_hi.b, z_src.b, #4

	/* load dest data, governed by p0 */
	ld1b	z_dest1.b, p0/z, [x_dest1, x_pos]
	ld1b	z_dest2.b, p0/z, [x_dest2, x_pos]

	prfb	pldl2strm, p0, [x_dest3, x_pos]
	prfb	pldl2strm, p0, [x_dest4, x_pos]

	/* dest1 */
	/* table indexing, ie. gf(2^8) multiplication */
	tbl	z_tmp_lo.b, {z_gft1_lo.b}, z_src_lo.b
	tbl	z_tmp_hi.b, {z_gft1_hi.b}, z_src_hi.b
	/* exclusive or, ie. gf(2^8) add */
	eor	z_dest1.d, z_tmp_lo.d, z_dest1.d
	eor	z_dest1.d, z_tmp_hi.d, z_dest1.d

	ld1b	z_dest3.b, p0/z, [x_dest3, x_pos]
	ld1b	z_dest4.b, p0/z, [x_dest4, x_pos]
	prfb	pldl2strm, p0, [x_dest5, x_pos]

	/* dest2 */
	tbl	z_tmp_lo.b, {z_gft2_lo.b}, z_src_lo.b
	tbl	z_tmp_hi.b, {z_gft2_hi.b}, z_src_hi.b
	eor	z_dest2.d, z_tmp_lo.d, z_dest2.d
	eor	z_dest2.d, z_tmp_hi.d, z_dest2.d

	ld1b	z_dest5.b, p0/z, [x_dest5, x_pos]

	/* store dest data, governed by p0 */
	st1b	z_dest1.b, p0, [x_dest1, x_pos]

	/* dest3 */
	tbl	z_tmp_lo.b, {z_gft3_lo.b}, z_src_lo.b
	tbl	z_tmp_hi.b, {z_gft3_hi.b}, z_src_hi.b
	eor	z_dest3.d, z_tmp_lo.d, z_dest3.d
	eor	z_dest3.d, z_tmp_hi.d, z_dest3.d

	/* dest4 */
	tbl	z_tmp_lo.b, {z_gft4_lo.b}, z_src_lo.b
	tbl	z_tmp_hi.b, {z_gft4_hi.b}, z_src_hi.b
	eor	z_dest4.d, z_tmp_lo.d, z_dest4.d
	eor	z_dest4.d, z_tmp_hi.d, z_dest4.d

	/* store dest data, governed by p0 */
	st1b	z_dest2.b, p0, [x_dest2, x_pos]
	st1b	z_dest3.b, p0, [x_dest3, x_pos]

	/* dest5 */
	tbl	z_tmp_lo.b, {z_gft5_lo.b}, z_src_lo.b
	tbl	z_tmp_hi.b, {z_gft5_hi.b}, z_src_hi.b
	eor	z_dest5.d, z_tmp_lo.d, z_dest5.d
	eor	z_dest5.d, z_tmp_hi.d, z_dest5.d

	/* store dest data, governed by p0 */
	st1b	z_dest4.b, p0, [x_dest4, x_pos]
	st1b	z_dest5.b, p0, [x_dest5, x_pos]
	/* increment one vector length */
	incb	x_pos

	b	.Lloopsve_vl

.return_pass:
	mov	w_ret, #0
	ret

.return_fail:
	mov	w_ret, #1
	ret
