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

.global cdecl(gf_vect_mul_neon)
#ifndef __APPLE__
.type gf_vect_mul_neon, %function
#endif

/* arguments */
x_len		.req	x0
x_tbl		.req	x1
x_src		.req	x2
x_dest		.req	x3

/* returns */
w_ret		.req	w0

/* local variables */
x_dest1		.req	x_dest
x_src_end	.req	x4
x_tmp		.req	x5

/* vectors */
v_mask0f	.req	v0

v_gft1_lo	.req	v2
v_gft1_hi	.req	v3
q_gft1_lo	.req	q2
q_gft1_hi	.req	q3

v_data_0	.req	v16
v_data_1	.req	v17
v_data_2	.req	v18
v_data_3	.req	v19
v_data_4	.req	v20
v_data_5	.req	v21
v_data_6	.req	v22
v_data_7	.req	v23
q_data_0	.req	q16
q_data_1	.req	q17
q_data_2	.req	q18
q_data_3	.req	q19
q_data_4	.req	q20
q_data_5	.req	q21
q_data_6	.req	q22
q_data_7	.req	q23

v_data_0_lo	.req	v24
v_data_1_lo	.req	v25
v_data_2_lo	.req	v26
v_data_3_lo	.req	v27
v_data_4_lo	.req	v28
v_data_5_lo	.req	v29
v_data_6_lo	.req	v30
v_data_7_lo	.req	v31
v_data_0_hi	.req	v_data_0
v_data_1_hi	.req	v_data_1
v_data_2_hi	.req	v_data_2
v_data_3_hi	.req	v_data_3
v_data_4_hi	.req	v_data_4
v_data_5_hi	.req	v_data_5
v_data_6_hi	.req	v_data_6
v_data_7_hi	.req	v_data_7


cdecl(gf_vect_mul_neon):
	/* less than 32 bytes, return_fail */
	cmp	x_len, #32
	blt	.return_fail

	movi	v_mask0f.16b, #0x0f
	add	x_src_end, x_src, x_len
	ldr	q_gft1_lo, [x_tbl]
	ldr	q_gft1_hi, [x_tbl, #16]


.Lloop128_init:
	/* less than 128 bytes, goto Lloop16_init */
	cmp	x_len, #128
	blt	.Lloop32_init

	/* save d8 ~ d15 to stack */
	sub	sp, sp, #64
	stp	d8, d9, [sp]
	stp	d10, d11, [sp, #16]
	stp	d12, d13, [sp, #32]
	stp	d14, d15, [sp, #48]

	sub	x_src_end, x_src_end, #128

.Lloop128:
	ldr	q_data_0, [x_src, #16*0]
	ldr	q_data_1, [x_src, #16*1]
	ldr	q_data_2, [x_src, #16*2]
	ldr	q_data_3, [x_src, #16*3]
	ldr	q_data_4, [x_src, #16*4]
	ldr	q_data_5, [x_src, #16*5]
	ldr	q_data_6, [x_src, #16*6]
	ldr	q_data_7, [x_src, #16*7]

	and	v_data_0_lo.16b, v_data_0.16b, v_mask0f.16b
	and	v_data_1_lo.16b, v_data_1.16b, v_mask0f.16b
	and	v_data_2_lo.16b, v_data_2.16b, v_mask0f.16b
	and	v_data_3_lo.16b, v_data_3.16b, v_mask0f.16b
	and	v_data_4_lo.16b, v_data_4.16b, v_mask0f.16b
	and	v_data_5_lo.16b, v_data_5.16b, v_mask0f.16b
	and	v_data_6_lo.16b, v_data_6.16b, v_mask0f.16b
	and	v_data_7_lo.16b, v_data_7.16b, v_mask0f.16b

	ushr	v_data_0_hi.16b, v_data_0.16b, #4
	ushr	v_data_1_hi.16b, v_data_1.16b, #4
	ushr	v_data_2_hi.16b, v_data_2.16b, #4
	ushr	v_data_3_hi.16b, v_data_3.16b, #4
	ushr	v_data_4_hi.16b, v_data_4.16b, #4
	ushr	v_data_5_hi.16b, v_data_5.16b, #4
	ushr	v_data_6_hi.16b, v_data_6.16b, #4
	ushr	v_data_7_hi.16b, v_data_7.16b, #4

	tbl	v_data_0_lo.16b, {v_gft1_lo.16b}, v_data_0_lo.16b
	tbl	v_data_1_lo.16b, {v_gft1_lo.16b}, v_data_1_lo.16b
	tbl	v_data_2_lo.16b, {v_gft1_lo.16b}, v_data_2_lo.16b
	tbl	v_data_3_lo.16b, {v_gft1_lo.16b}, v_data_3_lo.16b
	tbl	v_data_4_lo.16b, {v_gft1_lo.16b}, v_data_4_lo.16b
	tbl	v_data_5_lo.16b, {v_gft1_lo.16b}, v_data_5_lo.16b
	tbl	v_data_6_lo.16b, {v_gft1_lo.16b}, v_data_6_lo.16b
	tbl	v_data_7_lo.16b, {v_gft1_lo.16b}, v_data_7_lo.16b

	tbl	v_data_0_hi.16b, {v_gft1_hi.16b}, v_data_0_hi.16b
	tbl	v_data_1_hi.16b, {v_gft1_hi.16b}, v_data_1_hi.16b
	tbl	v_data_2_hi.16b, {v_gft1_hi.16b}, v_data_2_hi.16b
	tbl	v_data_3_hi.16b, {v_gft1_hi.16b}, v_data_3_hi.16b
	tbl	v_data_4_hi.16b, {v_gft1_hi.16b}, v_data_4_hi.16b
	tbl	v_data_5_hi.16b, {v_gft1_hi.16b}, v_data_5_hi.16b
	tbl	v_data_6_hi.16b, {v_gft1_hi.16b}, v_data_6_hi.16b
	tbl	v_data_7_hi.16b, {v_gft1_hi.16b}, v_data_7_hi.16b

	eor	v_data_0.16b, v_data_0_hi.16b, v_data_0_lo.16b
	eor	v_data_1.16b, v_data_1_hi.16b, v_data_1_lo.16b
	eor	v_data_2.16b, v_data_2_hi.16b, v_data_2_lo.16b
	eor	v_data_3.16b, v_data_3_hi.16b, v_data_3_lo.16b
	eor	v_data_4.16b, v_data_4_hi.16b, v_data_4_lo.16b
	eor	v_data_5.16b, v_data_5_hi.16b, v_data_5_lo.16b
	eor	v_data_6.16b, v_data_6_hi.16b, v_data_6_lo.16b
	eor	v_data_7.16b, v_data_7_hi.16b, v_data_7_lo.16b

	str	q_data_0, [x_dest1, #16*0]
	str	q_data_1, [x_dest1, #16*1]
	str	q_data_2, [x_dest1, #16*2]
	str	q_data_3, [x_dest1, #16*3]
	str	q_data_4, [x_dest1, #16*4]
	str	q_data_5, [x_dest1, #16*5]
	str	q_data_6, [x_dest1, #16*6]
	str	q_data_7, [x_dest1, #16*7]

	add	x_src, x_src, #128
	add	x_dest1, x_dest1, #128
	cmp	x_src, x_src_end
	bls	.Lloop128

.Lloop128_end:
	/* restore d8 ~ d15 */
	ldp	d8,  d9,  [sp]
	ldp	d10, d11, [sp, #16]
	ldp	d12, d13, [sp, #32]
	ldp	d14, d15, [sp, #48]
	add	sp, sp, #64
	add	x_src_end, x_src_end, #128
	cmp	x_src, x_src_end
	beq	.return_pass

.Lloop32_init:
	sub	x_src_end, x_src_end, #32
	cmp	x_src, x_src_end
	bhi	.return_fail

.Lloop32:
	ldr	q_data_0, [x_src, #16*0]
	ldr	q_data_1, [x_src, #16*1]

	and	v_data_0_lo.16b, v_data_0.16b, v_mask0f.16b
	and	v_data_1_lo.16b, v_data_1.16b, v_mask0f.16b
	ushr	v_data_0_hi.16b, v_data_0.16b, #4
	ushr	v_data_1_hi.16b, v_data_1.16b, #4
	tbl	v_data_0_lo.16b, {v_gft1_lo.16b}, v_data_0_lo.16b
	tbl	v_data_1_lo.16b, {v_gft1_lo.16b}, v_data_1_lo.16b
	tbl	v_data_0_hi.16b, {v_gft1_hi.16b}, v_data_0_hi.16b
	tbl	v_data_1_hi.16b, {v_gft1_hi.16b}, v_data_1_hi.16b
	eor	v_data_0.16b, v_data_0_hi.16b, v_data_0_lo.16b
	eor	v_data_1.16b, v_data_1_hi.16b, v_data_1_lo.16b
	str	q_data_0, [x_dest1, #16*0]
	str	q_data_1, [x_dest1, #16*1]

	add	x_dest1, x_dest1, #32
	add	x_src, x_src, #32
	cmp	x_src, x_src_end
	bls	.Lloop32

.Lloop32_end:
	sub	x_tmp, x_src, x_src_end
	cmp	x_tmp, #32
	beq	.return_pass
	b	.return_fail

.return_pass:
	mov	w_ret, #0
	ret

.return_fail:
	mov	w_ret, #1
	ret
