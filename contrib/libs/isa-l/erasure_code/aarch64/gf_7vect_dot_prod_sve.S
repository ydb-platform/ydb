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

.global cdecl(gf_7vect_dot_prod_sve)
#ifndef __APPLE__
.type gf_7vect_dot_prod_sve, %function
#endif
/* void gf_7vect_dot_prod_sve(int len, int vlen, unsigned char *gftbls,
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
x_tbl4		.req	x11
x_tbl5		.req	x12
x_tbl6		.req	x13
x_tbl7		.req	x14

x_dest1		.req	x15

/* r16,r17,r18,r29,r30: special role registers, avoided */
/* r19..r29 and SP must be preserved */
x_dest2		.req	x19
x_dest3		.req	x20
x_dest4		.req	x21
x_dest5		.req	x22
x_dest6		.req	x23
x_dest7		.req	x_dest	/* reused */

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

z_gft7_lo	.req	z6
z_gft7_hi	.req	z7
q_gft7_lo	.req	q6
q_gft7_hi	.req	q7

/* bottom 64-bit of v8..v15 must be preserved if used */
z_dest7		.req	z16

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

z_gft6_lo	.req	z25
z_gft6_hi	.req	z26
q_gft6_lo	.req	q25
q_gft6_hi	.req	q26

z_dest2		.req	z27
z_dest3		.req	z28
z_dest4		.req	z29
z_dest5		.req	z30
z_dest6		.req	z31

cdecl(gf_7vect_dot_prod_sve):
	/* less than 16 bytes, return_fail */
	cmp	x_len, #16
	blt	.return_fail

	/* save r19..r29  */
	sub	sp, sp, #48			/* alignment */
	stp	x19, x20, [sp]
	stp	x21, x22, [sp, #16]
	str	x23, [sp, #32]

	mov	z_mask0f.b, #0x0f		/* z_mask0f = 0x0F0F...0F */
	mov	x_pos, #0
	lsl	x_vec, x_vec, #3
	ldp	x_dest1, x_dest2, [x_dest, #8*0]
	ldp	x_dest3, x_dest4, [x_dest, #8*2]
	ldp	x_dest5, x_dest6, [x_dest, #8*4]
	ldr	x_dest7, [x_dest, #8*6]		/* x_dest7 reuses x_dest */

/* Loop 1: x_len, vector length */
.Lloopsve_vl:
	whilelo	p0.b, x_pos, x_len
	b.none	.return_pass

	mov	x_vec_i, #0			/* clear x_vec_i */
	ldr	x_ptr, [x_src, x_vec_i]		/* x_ptr: src base addr. */

	mov	z_dest1.b, #0			/* clear z_dest1 */
	mov	z_dest2.b, #0			/* clear z_dest2 */
	mov	z_dest3.b, #0			/* clear z_dest3 */
	mov	z_dest4.b, #0			/* clear z_dest4 */
	mov	z_dest5.b, #0			/* clear z_dest5 */
	mov	z_dest6.b, #0			/* clear z_dest6 */
	mov	z_dest7.b, #0			/* clear z_dest7 */

	/* gf_tbl base = (x_tbl + dest_idx * x_vec * 32) */
	mov	x_tbl1, x_tbl			/* reset x_tbl1 */
	add	x_tbl2, x_tbl1, x_vec, LSL #2	/* reset x_tbl2 */
	add	x_tbl3, x_tbl2, x_vec, LSL #2	/* reset x_tbl3 */
	add	x_tbl4, x_tbl3, x_vec, LSL #2	/* reset x_tbl4 */
	add	x_tbl5, x_tbl4, x_vec, LSL #2	/* reset x_tbl5 */
	add	x_tbl6, x_tbl5, x_vec, LSL #2	/* reset x_tbl6 */
	add	x_tbl7, x_tbl6, x_vec, LSL #2	/* reset x_tbl7 */

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

	/* calc for next and prefetch */
	add	x_vec_i, x_vec_i, #8		/* move x_vec_i to next */
	ldr	x_ptr, [x_src, x_vec_i]		/* x_ptr: src base addr. */

	/* dest 1 */
	/* table indexing, ie. gf(2^8) multiplication */
	tbl	z_gft1_lo.b, {z_gft1_lo.b}, z_src_lo.b
	tbl	z_gft1_hi.b, {z_gft1_hi.b}, z_src_hi.b
	/* exclusive or, ie. gf(2^8) add */
	eor	z_dest1.d, z_gft1_lo.d, z_dest1.d
	eor	z_dest1.d, z_gft1_hi.d, z_dest1.d

	ldp	q_gft3_lo, q_gft3_hi, [x_tbl3], #32
	ldp	q_gft4_lo, q_gft4_hi, [x_tbl4], #32
	prfb	pldl2keep, p0, [x_tbl3]
	prfb	pldl2keep, p0, [x_tbl4]

	/* dest 2 */
	tbl	z_gft2_lo.b, {z_gft2_lo.b}, z_src_lo.b
	tbl	z_gft2_hi.b, {z_gft2_hi.b}, z_src_hi.b
	eor	z_dest2.d, z_gft2_lo.d, z_dest2.d
	eor	z_dest2.d, z_gft2_hi.d, z_dest2.d

	/* dest 3 */
	tbl	z_gft3_lo.b, {z_gft3_lo.b}, z_src_lo.b
	tbl	z_gft3_hi.b, {z_gft3_hi.b}, z_src_hi.b
	eor	z_dest3.d, z_gft3_lo.d, z_dest3.d
	eor	z_dest3.d, z_gft3_hi.d, z_dest3.d

	ldp	q_gft5_lo, q_gft5_hi, [x_tbl5], #32
	ldp	q_gft6_lo, q_gft6_hi, [x_tbl6], #32
	prfb	pldl2keep, p0, [x_tbl5]
	prfb	pldl2keep, p0, [x_tbl6]

	/* dest 4 */
	tbl	z_gft4_lo.b, {z_gft4_lo.b}, z_src_lo.b
	tbl	z_gft4_hi.b, {z_gft4_hi.b}, z_src_hi.b
	eor	z_dest4.d, z_gft4_lo.d, z_dest4.d
	eor	z_dest4.d, z_gft4_hi.d, z_dest4.d

	/* dest 5 */
	tbl	z_gft5_lo.b, {z_gft5_lo.b}, z_src_lo.b
	tbl	z_gft5_hi.b, {z_gft5_hi.b}, z_src_hi.b
	eor	z_dest5.d, z_gft5_lo.d, z_dest5.d
	eor	z_dest5.d, z_gft5_hi.d, z_dest5.d

	ldp	q_gft7_lo, q_gft7_hi, [x_tbl7], #32
	prfb	pldl2keep, p0, [x_tbl7]

	/* dest 6 */
	tbl	z_gft6_lo.b, {z_gft6_lo.b}, z_src_lo.b
	tbl	z_gft6_hi.b, {z_gft6_hi.b}, z_src_hi.b
	eor	z_dest6.d, z_gft6_lo.d, z_dest6.d
	eor	z_dest6.d, z_gft6_hi.d, z_dest6.d

	/* dest 7 */
	tbl	z_gft7_lo.b, {z_gft7_lo.b}, z_src_lo.b
	tbl	z_gft7_hi.b, {z_gft7_hi.b}, z_src_hi.b
	eor	z_dest7.d, z_gft7_lo.d, z_dest7.d
	eor	z_dest7.d, z_gft7_hi.d, z_dest7.d

	cmp	x_vec_i, x_vec
	blt	.Lloopsve_vl_vects
/* end of Loop 2 */

	/* store dest data, governed by p0 */
	st1b	z_dest1.b, p0, [x_dest1, x_pos]
	st1b	z_dest2.b, p0, [x_dest2, x_pos]
	st1b	z_dest3.b, p0, [x_dest3, x_pos]
	st1b	z_dest4.b, p0, [x_dest4, x_pos]
	st1b	z_dest5.b, p0, [x_dest5, x_pos]
	st1b	z_dest6.b, p0, [x_dest6, x_pos]
	st1b	z_dest7.b, p0, [x_dest7, x_pos]

	/* increment one vector length */
	incb	x_pos
	b	.Lloopsve_vl
/* end of Loop 1 */

.return_pass:
	/* restore r19..r29  */
	ldr	x23, [sp, #32]
	ldp	x21, x22, [sp, #16]
	ldp	x19, x20, [sp]
	add	sp, sp, #48

	mov	w_ret, #0
	ret

.return_fail:
	mov	w_ret, #1
	ret
