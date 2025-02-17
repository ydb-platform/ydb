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

.global cdecl(gf_vect_mad_neon)
#ifndef __APPLE__
.type gf_vect_mad_neon, %function
#endif

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
x_src_end	.req	x6
x_dest1		.req	x_dest
x_tmp		.req	x7
x_const		.req	x8

/* vectors */
v_mask0f	.req	v0
v_tmp		.req	v1
q_tmp		.req	q1

v_tmp1_lo	.req	v2
v_tmp1_hi	.req	v3
v_tmp2_lo	.req	v4
v_tmp2_hi	.req	v5

v_gft1_lo	.req	v6
v_gft1_hi	.req	v7
q_gft1_lo	.req	q6
q_gft1_hi	.req	q7

v_data_0	.req	v8
v_data_1	.req	v9
v_data_2	.req	v10
v_data_3	.req	v11
v_data_4	.req	v12
v_data_5	.req	v13
v_data_6	.req	v14
v_data_7	.req	v15
q_data_0	.req	q8
q_data_1	.req	q9
q_data_2	.req	q10
q_data_3	.req	q11
q_data_4	.req	q12
q_data_5	.req	q13
q_data_6	.req	q14
q_data_7	.req	q15

v_data_0_lo	.req	v16
v_data_1_lo	.req	v17
v_data_2_lo	.req	v18
v_data_3_lo	.req	v19
v_data_4_lo	.req	v20
v_data_5_lo	.req	v21
v_data_6_lo	.req	v22
v_data_7_lo	.req	v23
v_data_0_hi	.req	v_data_0
v_data_1_hi	.req	v_data_1
v_data_2_hi	.req	v_data_2
v_data_3_hi	.req	v_data_3
v_data_4_hi	.req	v_data_4
v_data_5_hi	.req	v_data_5
v_data_6_hi	.req	v_data_6
v_data_7_hi	.req	v_data_7

v_d1_0		.req	v24
v_d1_1		.req	v25
v_d1_2		.req	v26
v_d1_3		.req	v27
v_d1_4		.req	v28
v_d1_5		.req	v29
v_d1_6		.req	v30
v_d1_7		.req	v31
q_d1_0		.req	q24
q_d1_1		.req	q25
q_d1_2		.req	q26
q_d1_3		.req	q27
q_d1_4		.req	q28
q_d1_5		.req	q29
q_d1_6		.req	q30
q_d1_7		.req	q31

v_data		.req	v_d1_1
q_data		.req	q_d1_1
v_data_lo	.req	v_d1_2
v_data_hi	.req	v_d1_3


cdecl(gf_vect_mad_neon):
	/* less than 16 bytes, return_fail */
	cmp	x_len, #16
	blt	.return_fail

	movi	v_mask0f.16b, #0x0f
	lsl	x_vec_i, x_vec_i, #5
	add	x_tbl, x_tbl, x_vec_i
	add	x_src_end, x_src, x_len

	ldr	q_gft1_lo, [x_tbl]
	ldr	q_gft1_hi, [x_tbl, #16]

.Lloop128_init:
	/* less than 128 bytes, goto Lloop16_init */
	cmp	x_len, #128
	blt	.Lloop16_init

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

	ldr	q_d1_0, [x_dest1, #16*0]
	ldr	q_d1_1, [x_dest1, #16*1]
	ldr	q_d1_2, [x_dest1, #16*2]
	ldr	q_d1_3, [x_dest1, #16*3]
	ldr	q_d1_4, [x_dest1, #16*4]
	ldr	q_d1_5, [x_dest1, #16*5]
	ldr	q_d1_6, [x_dest1, #16*6]
	ldr	q_d1_7, [x_dest1, #16*7]

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

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_data_0_lo.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_0_hi.16b
	tbl	v_tmp2_lo.16b, {v_gft1_lo.16b}, v_data_1_lo.16b
	tbl	v_tmp2_hi.16b, {v_gft1_hi.16b}, v_data_1_hi.16b

	eor	v_d1_0.16b, v_tmp1_lo.16b, v_d1_0.16b
	eor	v_d1_0.16b, v_d1_0.16b, v_tmp1_hi.16b
	eor	v_d1_1.16b, v_tmp2_lo.16b, v_d1_1.16b
	eor	v_d1_1.16b, v_d1_1.16b, v_tmp2_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_data_2_lo.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_2_hi.16b
	tbl	v_tmp2_lo.16b, {v_gft1_lo.16b}, v_data_3_lo.16b
	tbl	v_tmp2_hi.16b, {v_gft1_hi.16b}, v_data_3_hi.16b

	eor	v_d1_2.16b, v_tmp1_lo.16b, v_d1_2.16b
	eor	v_d1_2.16b, v_d1_2.16b, v_tmp1_hi.16b
	eor	v_d1_3.16b, v_tmp2_lo.16b, v_d1_3.16b
	eor	v_d1_3.16b, v_d1_3.16b, v_tmp2_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_data_4_lo.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_4_hi.16b
	tbl	v_tmp2_lo.16b, {v_gft1_lo.16b}, v_data_5_lo.16b
	tbl	v_tmp2_hi.16b, {v_gft1_hi.16b}, v_data_5_hi.16b

	eor	v_d1_4.16b, v_tmp1_lo.16b, v_d1_4.16b
	eor	v_d1_4.16b, v_d1_4.16b, v_tmp1_hi.16b
	eor	v_d1_5.16b, v_tmp2_lo.16b, v_d1_5.16b
	eor	v_d1_5.16b, v_d1_5.16b, v_tmp2_hi.16b

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_data_6_lo.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_6_hi.16b
	tbl	v_tmp2_lo.16b, {v_gft1_lo.16b}, v_data_7_lo.16b
	tbl	v_tmp2_hi.16b, {v_gft1_hi.16b}, v_data_7_hi.16b

	eor	v_d1_6.16b, v_tmp1_lo.16b, v_d1_6.16b
	eor	v_d1_6.16b, v_d1_6.16b, v_tmp1_hi.16b
	eor	v_d1_7.16b, v_tmp2_lo.16b, v_d1_7.16b
	eor	v_d1_7.16b, v_d1_7.16b, v_tmp2_hi.16b

	str	q_d1_0, [x_dest1, #16*0]
	str	q_d1_1, [x_dest1, #16*1]
	str	q_d1_2, [x_dest1, #16*2]
	str	q_d1_3, [x_dest1, #16*3]
	str	q_d1_4, [x_dest1, #16*4]
	str	q_d1_5, [x_dest1, #16*5]
	str	q_d1_6, [x_dest1, #16*6]
	str	q_d1_7, [x_dest1, #16*7]

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

.Lloop16_init:
	sub	x_src_end, x_src_end, #16
	cmp	x_src, x_src_end
	bhi	.lessthan16_init

.Lloop16:
	ldr	q_data, [x_src]
	ldr	q_d1_0, [x_dest1]

	and	v_data_lo.16b, v_data.16b, v_mask0f.16b
	ushr	v_data_hi.16b, v_data.16b, #4

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_data_lo.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_hi.16b
	eor	v_d1_0.16b, v_tmp1_lo.16b, v_d1_0.16b
	eor	v_d1_0.16b, v_d1_0.16b, v_tmp1_hi.16b

	str	q_d1_0, [x_dest1]

	add	x_dest1, x_dest1, #16
	add	x_src, x_src, #16
	cmp	x_src, x_src_end
	bls	.Lloop16

.lessthan16_init:
	sub	x_tmp, x_src, x_src_end
	cmp	x_tmp, #16
	beq	.return_pass

.lessthan16:
	mov	x_src, x_src_end
	sub	x_dest1, x_dest1, x_tmp

#ifndef __APPLE__
	adrp	x_const, const_tbl
	add	x_const, x_const, :lo12:const_tbl
#else
	adrp	x_const, const_tbl@PAGE
	add	x_const, x_const, const_tbl@PAGEOFF
#endif
	sub	x_const, x_const, x_tmp
	ldr	q_tmp, [x_const, #16]

	ldr	q_data, [x_src]
	ldr	q_d1_0, [x_dest1]

	and	v_data_lo.16b, v_data.16b, v_mask0f.16b
	ushr	v_data_hi.16b, v_data.16b, #4

	tbl	v_tmp1_lo.16b, {v_gft1_lo.16b}, v_data_lo.16b
	tbl	v_tmp1_hi.16b, {v_gft1_hi.16b}, v_data_hi.16b
	eor	v_tmp1_hi.16b, v_tmp1_lo.16b, v_tmp1_hi.16b
	and	v_tmp1_hi.16b, v_tmp1_hi.16b, v_tmp.16b
	eor	v_d1_0.16b, v_d1_0.16b, v_tmp1_hi.16b

	str	q_d1_0, [x_dest1]

.return_pass:
	mov	w_ret, #0
	ret

.return_fail:
	mov	w_ret, #1
	ret

ASM_DEF_RODATA
.balign 8
const_tbl:
	.dword 0x0000000000000000, 0x0000000000000000
	.dword 0xffffffffffffffff, 0xffffffffffffffff
