/*************************************************************
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

.global cdecl(gf_3vect_dot_prod_sve)
#ifndef __APPLE__
.type gf_3vect_dot_prod_sve, %function
#endif
/* void gf_3vect_dot_prod_sve(int len, int vlen, unsigned char *gftbls,
				   unsigned char **src, unsigned char **dest);
 */

/* arguments */
x_len		.req	x0	/* vector length */
x_vec		.req	x1	/* number of source vectors (ie. data blocks) */
x_tbl		.req	x2
x_src		.req	x3
x_dest		.req	x4

/* returns */
w_ret		.req	w0

/* local variables */
x_vec_i		.req	x5
x_ptr		.req	x6
x_pos		.req	x7

x_tbl1		.req	x8
x_tbl2		.req	x9
x_tbl3		.req	x10
x_dest1		.req	x11
x_dest2		.req	x12
x_dest3		.req	x_dest	/* reused */

/* r16,r17,r18,r29,r30: special role registers, avoided */
/* r19..r29 and SP must be preserved */

/* vectors */
z_mask0f	.req	z0

z_src		.req	z1
z_src_lo	.req	z2
z_src_hi	.req	z_src

z_dest1		.req	z3

z_gft1_lo	.req	z4
z_gft1_hi	.req	z5
q_gft1_lo	.req	q4
q_gft1_hi	.req	q5

/* bottom 64-bit of v8..v15 must be preserved if used */
z_gft2_lo	.req	z17
z_gft2_hi	.req	z18
q_gft2_lo	.req	q17
q_gft2_hi	.req	q18

z_gft3_lo	.req	z19
z_gft3_hi	.req	z20
q_gft3_lo	.req	q19
q_gft3_hi	.req	q20

z_dest2		.req	z27
z_dest3		.req	z28

cdecl(gf_3vect_dot_prod_sve):
	/* less than 16 bytes, return_fail */
	cmp	x_len, #16
	blt	.return_fail

	mov	z_mask0f.b, #0x0f		/* z_mask0f = 0x0F0F...0F */
	mov	x_pos, #0
	lsl	x_vec, x_vec, #3
	ldp	x_dest1, x_dest2, [x_dest, #8*0]
	ldr	x_dest3, [x_dest, #8*2]

/* Loop 1: x_len, vector length */
.Lloopsve_vl:
	whilelo	p0.b, x_pos, x_len
	b.none	.return_pass

	mov	x_vec_i, #0			/* clear x_vec_i */
	ldr	x_ptr, [x_src, x_vec_i]		/* x_ptr: src base addr. */

	mov	z_dest1.b, #0			/* clear z_dest1 */
	mov	z_dest2.b, #0			/* clear z_dest2 */
	mov	z_dest3.b, #0			/* clear z_dest3 */

	/* gf_tbl base = (x_tbl + dest_idx * x_vec * 32) */
	mov	x_tbl1, x_tbl			/* reset x_tbl1 */
	add	x_tbl2, x_tbl1, x_vec, LSL #2	/* reset x_tbl2 */
	add	x_tbl3, x_tbl2, x_vec, LSL #2	/* reset x_tbl3 */

/* Loop 2: x_vec, number of source vectors (ie. data blocks) */
.Lloopsve_vl_vects:
	/* load src data, governed by p0 */
	ld1b	z_src.b,  p0/z, [x_ptr, x_pos]	/* load from: src base + pos offset */
	/* split 4-bit lo; 4-bit hi */
	and	z_src_lo.d, z_src.d, z_mask0f.d
	lsr	z_src_hi.b, z_src.b, #4


	/* gf_tbl addr: (x_tbl + dest_idx * x_vec * 32) + src_vec_idx * 32 */
	/* load gf_table's */
	ldp	q_gft1_lo, q_gft1_hi, [x_tbl1], #32	/* x_tbl1 is post-added by #32 for each src vect */
	ldp	q_gft2_lo, q_gft2_hi, [x_tbl2], #32

	/* prefetch */
	prfb	pldl2keep, p0, [x_tbl1]
	prfb	pldl2keep, p0, [x_tbl2]

	/* calc for next */
	add	x_vec_i, x_vec_i, #8		/* move x_vec_i to next */
	ldr	x_ptr, [x_src, x_vec_i]		/* x_ptr: src base addr. */

	/* dest 1 */
	/* table indexing, ie. gf(2^8) multiplication */
	tbl	z_gft1_lo.b, {z_gft1_lo.b}, z_src_lo.b
	tbl	z_gft1_hi.b, {z_gft1_hi.b}, z_src_hi.b
	/* exclusive or, ie. gf(2^8) add */
	eor	z_dest1.d, z_gft1_lo.d, z_dest1.d
	eor	z_dest1.d, z_dest1.d, z_gft1_hi.d

	ldp	q_gft3_lo, q_gft3_hi, [x_tbl3], #32
	prfb	pldl2keep, p0, [x_tbl3]

	/* dest 2 */
	tbl	z_gft2_lo.b, {z_gft2_lo.b}, z_src_lo.b
	tbl	z_gft2_hi.b, {z_gft2_hi.b}, z_src_hi.b
	eor	z_dest2.d, z_gft2_lo.d, z_dest2.d
	eor	z_dest2.d, z_dest2.d, z_gft2_hi.d

	/* dest 3 */
	tbl	z_gft3_lo.b, {z_gft3_lo.b}, z_src_lo.b
	tbl	z_gft3_hi.b, {z_gft3_hi.b}, z_src_hi.b
	eor	z_dest3.d, z_gft3_lo.d, z_dest3.d
	eor	z_dest3.d, z_dest3.d, z_gft3_hi.d

	cmp	x_vec_i, x_vec
	blt	.Lloopsve_vl_vects
/* end of Loop 2 */

	/* store dest data, governed by p0 */
	st1b	z_dest1.b, p0, [x_dest1, x_pos]
	st1b	z_dest2.b, p0, [x_dest2, x_pos]
	st1b	z_dest3.b, p0, [x_dest3, x_pos]

	/* increment one vector length */
	incb	x_pos
	b	.Lloopsve_vl
/* end of Loop 1 */

.return_pass:
	mov	w_ret, #0
	ret

.return_fail:
	mov	w_ret, #1
	ret
