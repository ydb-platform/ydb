/**************************************************************
  Copyright (c) 2019 Huawei Technologies Co., Ltd.

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

#include "../include/aarch64_label.h"

.text

.global cdecl(gf_3vect_dot_prod_neon)
#ifndef __APPLE__
.type gf_3vect_dot_prod_neon, %function
#endif

/* arguments */
x_len		.req	x0
x_vec		.req	x1
x_tbl		.req	x2
x_src		.req	x3
x_dest		.req	x4

/* returns */
w_ret		.req	w0

/* local variables */
x_vec_i		.req	x5
x_ptr		.req	x6
x_pos		.req	x7
x_tmp		.req	x8
x_dest1		.req	x9
x_tbl1		.req	x10
x_dest2		.req	x11
x_tbl2		.req	x12
x_dest3		.req	x13
x_tbl3		.req	x14

/* vectors */
v_gft1_lo	.req	v0
v_gft1_hi	.req	v1
v_gft2_lo	.req	v2
v_gft2_hi	.req	v3
v_gft3_lo	.req	v4
v_gft3_hi	.req	v5
q_gft1_lo	.req	q0
q_gft1_hi	.req	q1
q_gft2_lo	.req	q2
q_gft2_hi	.req	q3
q_gft3_lo	.req	q4
q_gft3_hi	.req	q5

v_mask0f	.req	v6
q_mask0f	.req	q6
v_tmp1		.req	v7

v_data_0	.req	v8
v_data_1	.req	v9
v_data_2	.req	v10
v_data_3	.req	v11
q_data_0	.req	q8
q_data_1	.req	q9
q_data_2	.req	q10
q_data_3	.req	q11

v_tmp1_lo	.req	v12
v_tmp1_hi	.req	v13

v_p1_0		.req	v20
v_p1_1		.req	v21
v_p1_2		.req	v22
v_p1_3		.req	v23
v_p2_0		.req	v24
v_p2_1		.req	v25
v_p2_2		.req	v26
v_p2_3		.req	v27
v_p3_0		.req	v28
v_p3_1		.req	v29
v_p3_2		.req	v30
v_p3_3		.req	v31

q_p1_0		.req	q20
q_p1_1		.req	q21
q_p1_2		.req	q22
q_p1_3		.req	q23
q_p2_0		.req	q24
q_p2_1		.req	q25
q_p2_2		.req	q26
q_p2_3		.req	q27
q_p3_0		.req	q28
q_p3_1		.req	q29
q_p3_2		.req	q30
q_p3_3		.req	q31

v_data		.req	v_p1_1
q_data		.req	q_p1_1
v_data_lo	.req	v_p1_2
v_data_hi	.req	v_p1_3


cdecl(gf_3vect_dot_prod_neon):
	/* less than 16 bytes, return_fail */
	cmp	x_len, #16
	blt	.return_fail

	movi	v_mask0f.16b, #0x0f
	mov	x_pos, #0
	lsl	x_vec, x_vec, #3
	ldr	x_dest1, [x_dest, #8*0]
	ldr	x_dest2, [x_dest, #8*1]
	ldr	x_dest3, [x_dest, #8*2]

.Lloop64_init:
	/* less than 64 bytes, goto Lloop16_init */
	cmp	x_len, #64
	blt	.Lloop16_init

	/* save d8 ~ d15 to stack */
	sub	sp, sp, #64
	stp	d8, d9, [sp]
	stp	d10, d11, [sp, #16]
	stp	d12, d13, [sp, #32]
	stp	d14, d15, [sp, #48]

	sub	x_len, x_len, #64

.Lloop64:
	movi	v_p1_0.16b, #0
	movi	v_p1_1.16b, #0
	movi	v_p1_2.16b, #0
	movi	v_p1_3.16b, #0
	movi	v_p2_0.16b, #0
	movi	v_p2_1.16b, #0
	movi	v_p2_2.16b, #0
	movi	v_p2_3.16b, #0
	movi	v_p3_0.16b, #0
	movi	v_p3_1.16b, #0
	movi	v_p3_2.16b, #0
	movi	v_p3_3.16b, #0

	mov	x_tbl1, x_tbl
	add	x_tbl2, x_tbl1, x_vec, lsl #2
	add	x_tbl3, x_tbl2, x_vec, lsl #2
	mov	x_vec_i, #0

.Lloop64_vects:
	ldr	x_ptr, [x_src, x_vec_i]
	add	x_vec_i, x_vec_i, #8
	add	x_ptr, x_ptr, x_pos

	ldr	q_data_0, [x_ptr], #16
	ldr	q_data_1, [x_ptr], #16

	ldp	q_gft1_lo, q_gft1_hi, [x_tbl1], #32
	ldp	q_gft2_lo, q_gft2_hi, [x_tbl2], #32
	ldp	q_gft3_lo, q_gft3_hi, [x_tbl3], #32

	ldr	q_data_2, [x_ptr], #16
	ldr	q_data_3, [x_ptr], #16
	prfm	pldl1strm, [x_ptr]
	prfm	pldl1keep, [x_tbl1]
	prfm	pldl1keep, [x_tbl2]
	prfm	pldl1keep, [x_tbl3]

	/* data_0 */
	and	v_tmp1.16b, v_data_0.16b, v_mask0f.16b
	ushr	v_data_0.16b, v_data_0.16b, #4

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_0.16b
	eor	v_p1_0.16b, v_tmp1_lo.16b, v_p1_0.16b
	eor	v_p1_0.16b, v_p1_0.16b, v_tmp1_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft2_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft2_hi.16b}, v_data_0.16b
	eor	v_p2_0.16b, v_tmp1_lo.16b, v_p2_0.16b
	eor	v_p2_0.16b, v_p2_0.16b, v_tmp1_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft3_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft3_hi.16b}, v_data_0.16b
	eor	v_p3_0.16b, v_tmp1_lo.16b, v_p3_0.16b
	eor	v_p3_0.16b, v_p3_0.16b, v_tmp1_hi.16b

	/* data_1 */
	and	v_tmp1.16b, v_data_1.16b, v_mask0f.16b
	ushr	v_data_1.16b, v_data_1.16b, #4

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_1.16b
	eor	v_p1_1.16b, v_tmp1_lo.16b, v_p1_1.16b
	eor	v_p1_1.16b, v_p1_1.16b, v_tmp1_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft2_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft2_hi.16b}, v_data_1.16b
	eor	v_p2_1.16b, v_tmp1_lo.16b, v_p2_1.16b
	eor	v_p2_1.16b, v_p2_1.16b, v_tmp1_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft3_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft3_hi.16b}, v_data_1.16b
	eor	v_p3_1.16b, v_tmp1_lo.16b, v_p3_1.16b
	eor	v_p3_1.16b, v_p3_1.16b, v_tmp1_hi.16b

	/* data_2 */
	and	v_tmp1.16b, v_data_2.16b, v_mask0f.16b
	ushr	v_data_2.16b, v_data_2.16b, #4

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_2.16b
	eor	v_p1_2.16b, v_tmp1_lo.16b, v_p1_2.16b
	eor	v_p1_2.16b, v_p1_2.16b, v_tmp1_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft2_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft2_hi.16b}, v_data_2.16b
	eor	v_p2_2.16b, v_tmp1_lo.16b, v_p2_2.16b
	eor	v_p2_2.16b, v_p2_2.16b, v_tmp1_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft3_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft3_hi.16b}, v_data_2.16b
	eor	v_p3_2.16b, v_tmp1_lo.16b, v_p3_2.16b
	eor	v_p3_2.16b, v_p3_2.16b, v_tmp1_hi.16b

	/* data_3 */
	and	v_tmp1.16b, v_data_3.16b, v_mask0f.16b
	ushr	v_data_3.16b, v_data_3.16b, #4

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_3.16b
	eor	v_p1_3.16b, v_tmp1_lo.16b, v_p1_3.16b
	eor	v_p1_3.16b, v_p1_3.16b, v_tmp1_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft2_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft2_hi.16b}, v_data_3.16b
	eor	v_p2_3.16b, v_tmp1_lo.16b, v_p2_3.16b
	eor	v_p2_3.16b, v_p2_3.16b, v_tmp1_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft3_lo.16b}, v_tmp1.16b
	tbl	v_tmp1_hi.16b, {v_gft3_hi.16b}, v_data_3.16b
	eor	v_p3_3.16b, v_tmp1_lo.16b, v_p3_3.16b
	eor	v_p3_3.16b, v_p3_3.16b, v_tmp1_hi.16b

	cmp	x_vec_i, x_vec
	blt	.Lloop64_vects

.Lloop64_vects_end:
	add	x_ptr, x_dest1, x_pos
	stp	q_p1_0, q_p1_1, [x_ptr], #32
	stp	q_p1_2, q_p1_3, [x_ptr]

	add	x_ptr, x_dest2, x_pos
	stp	q_p2_0, q_p2_1, [x_ptr], #32
	stp	q_p2_2, q_p2_3, [x_ptr]

	add	x_ptr, x_dest3, x_pos
	stp	q_p3_0, q_p3_1, [x_ptr], #32
	stp	q_p3_2, q_p3_3, [x_ptr]

	add	x_pos, x_pos, #64
	cmp	x_pos, x_len
	ble	.Lloop64

.Lloop64_end:
	/* restore d8 ~ d15 */
	ldp	d8,  d9,  [sp]
	ldp	d10, d11, [sp, #16]
	ldp	d12, d13, [sp, #32]
	ldp	d14, d15, [sp, #48]
	add	sp, sp, #64

	add	x_len, x_len, #64
	cmp	x_pos, x_len
	beq	.return_pass

.Lloop16_init:
	sub	x_len, x_len, #16
	cmp	x_pos, x_len
	bgt	.lessthan16_init

.Lloop16:
	movi	v_p1_0.16b, #0
	movi	v_p2_0.16b, #0
	movi	v_p3_0.16b, #0
	mov	x_tbl1, x_tbl
	add	x_tbl2, x_tbl1, x_vec, lsl #2
	add	x_tbl3, x_tbl2, x_vec, lsl #2
	mov	x_vec_i, #0

.Lloop16_vects:
	ldr	x_ptr, [x_src, x_vec_i]
	add	x_vec_i, x_vec_i, #8
	ldr	q_data, [x_ptr, x_pos]

	ldp	q_gft1_lo, q_gft1_hi, [x_tbl1], #32
	ldp	q_gft2_lo, q_gft2_hi, [x_tbl2], #32
	ldp	q_gft3_lo, q_gft3_hi, [x_tbl3], #32

	and	v_data_lo.16b, v_data.16b, v_mask0f.16b
	ushr	v_data_hi.16b, v_data.16b, #4

	tbl	v_gft1_lo.16b, {v_gft1_lo.16b}, v_data_lo.16b
	tbl	v_gft1_hi.16b, {v_gft1_hi.16b}, v_data_hi.16b
	tbl	v_gft2_lo.16b, {v_gft2_lo.16b}, v_data_lo.16b
	tbl	v_gft2_hi.16b, {v_gft2_hi.16b}, v_data_hi.16b
	tbl	v_gft3_lo.16b, {v_gft3_lo.16b}, v_data_lo.16b
	tbl	v_gft3_hi.16b, {v_gft3_hi.16b}, v_data_hi.16b

	eor	v_p1_0.16b, v_gft1_hi.16b, v_p1_0.16b
	eor	v_p1_0.16b, v_p1_0.16b, v_gft1_lo.16b
	eor	v_p2_0.16b, v_gft2_hi.16b, v_p2_0.16b
	eor	v_p2_0.16b, v_p2_0.16b, v_gft2_lo.16b
	eor	v_p3_0.16b, v_gft3_hi.16b, v_p3_0.16b
	eor	v_p3_0.16b, v_p3_0.16b, v_gft3_lo.16b

	cmp	x_vec_i, x_vec
	bne	.Lloop16_vects

.Lloop16_vects_end:
	str	q_p1_0, [x_dest1, x_pos]
	str	q_p2_0, [x_dest2, x_pos]
	str	q_p3_0, [x_dest3, x_pos]
	add	x_pos, x_pos, #16
	cmp	x_pos, x_len
	ble	.Lloop16

.Lloop16_end:
	sub	x_tmp, x_pos, x_len
	cmp	x_tmp, #16
	beq	.return_pass

.lessthan16_init:
	mov	x_pos, x_len
	b	.Lloop16

.return_pass:
	mov	w_ret, #0
	ret

.return_fail:
	mov	w_ret, #1
	ret
