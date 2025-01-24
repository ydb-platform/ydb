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

.global cdecl(gf_5vect_mad_neon)
#ifndef __APPLE__
.type gf_5vect_mad_neon, %function
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
x_dest1		.req	x7
x_dest2		.req	x8
x_dest3		.req	x9
x_dest4		.req	x10
x_dest5		.req	x_dest
x_tmp		.req	x11
x_tbl1		.req	x12
x_tbl2		.req	x13
x_tbl3		.req	x14
x_tbl4		.req	x15
x_tbl5		.req	x16
x_const		.req	x17

/* vectors */
v_mask0f	.req	v0
v_tmp_lo	.req	v1
v_tmp_hi	.req	v2
v_tmp		.req	v3
q_tmp		.req	q3

v_gft1_lo	.req	v4
v_gft1_hi	.req	v5
v_gft2_lo	.req	v6
v_gft2_hi	.req	v7
v_gft3_lo	.req	v16
v_gft3_hi	.req	v17
q_gft1_lo	.req	q4
q_gft1_hi	.req	q5
q_gft2_lo	.req	q6
q_gft2_hi	.req	q7
q_gft3_lo	.req	q16
q_gft3_hi	.req	q17

v_gft4_lo	.req	v18
v_gft4_hi	.req	v19
q_gft4_lo	.req	q18
q_gft4_hi	.req	q19
v_gft5_lo	.req	v_gft2_lo
v_gft5_hi	.req	v_gft2_hi
q_gft5_lo	.req	q_gft2_lo
q_gft5_hi	.req	q_gft2_hi

v_data_0	.req	v8
v_data_1	.req	v9
v_data_2	.req	v10
v_data_3	.req	v11
q_data_0	.req	q8
q_data_1	.req	q9
q_data_2	.req	q10
q_data_3	.req	q11

v_data_0_lo	.req	v12
v_data_1_lo	.req	v13
v_data_2_lo	.req	v14
v_data_3_lo	.req	v15
v_data_0_hi	.req	v_data_0
v_data_1_hi	.req	v_data_1
v_data_2_hi	.req	v_data_2
v_data_3_hi	.req	v_data_3

v_d1_0		.req	v20
v_d1_1		.req	v21
v_d1_2		.req	v22
v_d1_3		.req	v23
v_d2_0		.req	v24
v_d2_1		.req	v25
v_d2_2		.req	v26
v_d2_3		.req	v27
v_d3_0		.req	v28
v_d3_1		.req	v29
v_d3_2		.req	v30
v_d3_3		.req	v31
q_d1_0		.req	q20
q_d1_1		.req	q21
q_d1_2		.req	q22
q_d1_3		.req	q23
q_d2_0		.req	q24
q_d2_1		.req	q25
q_d2_2		.req	q26
q_d2_3		.req	q27
q_d3_0		.req	q28
q_d3_1		.req	q29
q_d3_2		.req	q30
q_d3_3		.req	q31

v_d4_0		.req	v_d1_0
v_d4_1		.req	v_d1_1
v_d4_2		.req	v_d1_2
v_d4_3		.req	v_d1_3
q_d4_0		.req	q_d1_0
q_d4_1		.req	q_d1_1
q_d4_2		.req	q_d1_2
q_d4_3		.req	q_d1_3
v_d5_0		.req	v_d2_0
v_d5_1		.req	v_d2_1
v_d5_2		.req	v_d2_2
v_d5_3		.req	v_d2_3
q_d5_0		.req	q_d2_0
q_d5_1		.req	q_d2_1
q_d5_2		.req	q_d2_2
q_d5_3		.req	q_d2_3

v_data		.req	v21
q_data		.req	q21
v_data_lo	.req	v22
v_data_hi	.req	v23

cdecl(gf_5vect_mad_neon):
	/* less than 16 bytes, return_fail */
	cmp	x_len, #16
	blt	.return_fail

	movi	v_mask0f.16b, #0x0f
	lsl	x_vec_i, x_vec_i, #5
	lsl	x_vec, x_vec, #5
	add	x_tbl1, x_tbl, x_vec_i
	add	x_tbl2, x_tbl1, x_vec
	add	x_tbl3, x_tbl2, x_vec
	add	x_tbl4, x_tbl3, x_vec
	add	x_tbl5, x_tbl4, x_vec
	add	x_src_end, x_src, x_len
	ldr	x_dest1, [x_dest, #8*0]
	ldr	x_dest2, [x_dest, #8*1]
	ldr	x_dest3, [x_dest, #8*2]
	ldr	x_dest4, [x_dest, #8*3]
	ldr	x_dest5, [x_dest, #8*4]
	ldr	q_gft1_lo, [x_tbl1]
	ldr	q_gft1_hi, [x_tbl1, #16]
	ldr	q_gft3_lo, [x_tbl3]
	ldr	q_gft3_hi, [x_tbl3, #16]
	ldr	q_gft4_lo, [x_tbl4]
	ldr	q_gft4_hi, [x_tbl4, #16]

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

	sub	x_src_end, x_src_end, #64

.Lloop64:
	ldr	q_data_0, [x_src, #16*0]
	ldr	q_data_1, [x_src, #16*1]
	ldr	q_data_2, [x_src, #16*2]
	ldr	q_data_3, [x_src, #16*3]
	add	x_src, x_src, #64

	ldr	q_d1_0, [x_dest1, #16*0]
	ldr	q_d1_1, [x_dest1, #16*1]
	ldr	q_d1_2, [x_dest1, #16*2]
	ldr	q_d1_3, [x_dest1, #16*3]

	ldr	q_d2_0, [x_dest2, #16*0]
	ldr	q_d2_1, [x_dest2, #16*1]
	ldr	q_d2_2, [x_dest2, #16*2]
	ldr	q_d2_3, [x_dest2, #16*3]

	ldr	q_d3_0, [x_dest3, #16*0]
	ldr	q_d3_1, [x_dest3, #16*1]
	ldr	q_d3_2, [x_dest3, #16*2]
	ldr	q_d3_3, [x_dest3, #16*3]

	ldr	q_gft2_lo, [x_tbl2]
	ldr	q_gft2_hi, [x_tbl2, #16]

	and	v_data_0_lo.16b, v_data_0.16b, v_mask0f.16b
	and	v_data_1_lo.16b, v_data_1.16b, v_mask0f.16b
	and	v_data_2_lo.16b, v_data_2.16b, v_mask0f.16b
	and	v_data_3_lo.16b, v_data_3.16b, v_mask0f.16b

	ushr	v_data_0_hi.16b, v_data_0.16b, #4
	ushr	v_data_1_hi.16b, v_data_1.16b, #4
	ushr	v_data_2_hi.16b, v_data_2.16b, #4
	ushr	v_data_3_hi.16b, v_data_3.16b, #4

	/* dest1 */
	tbl	v_tmp_lo.16b, {v_gft1_lo.16b}, v_data_0_lo.16b
	tbl	v_tmp_hi.16b, {v_gft1_hi.16b}, v_data_0_hi.16b
	eor	v_d1_0.16b, v_tmp_lo.16b, v_d1_0.16b
	eor	v_d1_0.16b, v_d1_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft1_lo.16b}, v_data_1_lo.16b
	tbl	v_tmp_hi.16b, {v_gft1_hi.16b}, v_data_1_hi.16b
	eor	v_d1_1.16b, v_tmp_lo.16b, v_d1_1.16b
	eor	v_d1_1.16b, v_d1_1.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft1_lo.16b}, v_data_2_lo.16b
	tbl	v_tmp_hi.16b, {v_gft1_hi.16b}, v_data_2_hi.16b
	eor	v_d1_2.16b, v_tmp_lo.16b, v_d1_2.16b
	eor	v_d1_2.16b, v_d1_2.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft1_lo.16b}, v_data_3_lo.16b
	tbl	v_tmp_hi.16b, {v_gft1_hi.16b}, v_data_3_hi.16b
	eor	v_d1_3.16b, v_tmp_lo.16b, v_d1_3.16b
	eor	v_d1_3.16b, v_d1_3.16b, v_tmp_hi.16b

	/* dest2 */
	tbl	v_tmp_lo.16b, {v_gft2_lo.16b}, v_data_0_lo.16b
	tbl	v_tmp_hi.16b, {v_gft2_hi.16b}, v_data_0_hi.16b
	eor	v_d2_0.16b, v_tmp_lo.16b, v_d2_0.16b
	eor	v_d2_0.16b, v_d2_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft2_lo.16b}, v_data_1_lo.16b
	tbl	v_tmp_hi.16b, {v_gft2_hi.16b}, v_data_1_hi.16b
	eor	v_d2_1.16b, v_tmp_lo.16b, v_d2_1.16b
	eor	v_d2_1.16b, v_d2_1.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft2_lo.16b}, v_data_2_lo.16b
	tbl	v_tmp_hi.16b, {v_gft2_hi.16b}, v_data_2_hi.16b
	eor	v_d2_2.16b, v_tmp_lo.16b, v_d2_2.16b
	eor	v_d2_2.16b, v_d2_2.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft2_lo.16b}, v_data_3_lo.16b
	tbl	v_tmp_hi.16b, {v_gft2_hi.16b}, v_data_3_hi.16b
	eor	v_d2_3.16b, v_tmp_lo.16b, v_d2_3.16b
	eor	v_d2_3.16b, v_d2_3.16b, v_tmp_hi.16b

	/* dest3 */
	tbl	v_tmp_lo.16b, {v_gft3_lo.16b}, v_data_0_lo.16b
	tbl	v_tmp_hi.16b, {v_gft3_hi.16b}, v_data_0_hi.16b
	eor	v_d3_0.16b, v_tmp_lo.16b, v_d3_0.16b
	eor	v_d3_0.16b, v_d3_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft3_lo.16b}, v_data_1_lo.16b
	tbl	v_tmp_hi.16b, {v_gft3_hi.16b}, v_data_1_hi.16b
	eor	v_d3_1.16b, v_tmp_lo.16b, v_d3_1.16b
	eor	v_d3_1.16b, v_d3_1.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft3_lo.16b}, v_data_2_lo.16b
	tbl	v_tmp_hi.16b, {v_gft3_hi.16b}, v_data_2_hi.16b
	eor	v_d3_2.16b, v_tmp_lo.16b, v_d3_2.16b
	eor	v_d3_2.16b, v_d3_2.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft3_lo.16b}, v_data_3_lo.16b
	tbl	v_tmp_hi.16b, {v_gft3_hi.16b}, v_data_3_hi.16b
	eor	v_d3_3.16b, v_tmp_lo.16b, v_d3_3.16b
	eor	v_d3_3.16b, v_d3_3.16b, v_tmp_hi.16b

	str	q_d1_0, [x_dest1, #16*0]
	str	q_d1_1, [x_dest1, #16*1]
	str	q_d1_2, [x_dest1, #16*2]
	str	q_d1_3, [x_dest1, #16*3]
	add	x_dest1, x_dest1, #64

	str	q_d2_0, [x_dest2, #16*0]
	str	q_d2_1, [x_dest2, #16*1]
	str	q_d2_2, [x_dest2, #16*2]
	str	q_d2_3, [x_dest2, #16*3]
	add	x_dest2, x_dest2, #64

	str	q_d3_0, [x_dest3, #16*0]
	str	q_d3_1, [x_dest3, #16*1]
	str	q_d3_2, [x_dest3, #16*2]
	str	q_d3_3, [x_dest3, #16*3]
	add	x_dest3, x_dest3, #64

	ldr	q_d4_0, [x_dest4, #16*0]
	ldr	q_d4_1, [x_dest4, #16*1]
	ldr	q_d4_2, [x_dest4, #16*2]
	ldr	q_d4_3, [x_dest4, #16*3]

	ldr	q_d5_0, [x_dest5, #16*0]
	ldr	q_d5_1, [x_dest5, #16*1]
	ldr	q_d5_2, [x_dest5, #16*2]
	ldr	q_d5_3, [x_dest5, #16*3]

	ldr	q_gft5_lo, [x_tbl5]
	ldr	q_gft5_hi, [x_tbl5, #16]

	/* dest4 */
	tbl	v_tmp_lo.16b, {v_gft4_lo.16b}, v_data_0_lo.16b
	tbl	v_tmp_hi.16b, {v_gft4_hi.16b}, v_data_0_hi.16b
	eor	v_d4_0.16b, v_tmp_lo.16b, v_d4_0.16b
	eor	v_d4_0.16b, v_d4_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft4_lo.16b}, v_data_1_lo.16b
	tbl	v_tmp_hi.16b, {v_gft4_hi.16b}, v_data_1_hi.16b
	eor	v_d4_1.16b, v_tmp_lo.16b, v_d4_1.16b
	eor	v_d4_1.16b, v_d4_1.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft4_lo.16b}, v_data_2_lo.16b
	tbl	v_tmp_hi.16b, {v_gft4_hi.16b}, v_data_2_hi.16b
	eor	v_d4_2.16b, v_tmp_lo.16b, v_d4_2.16b
	eor	v_d4_2.16b, v_d4_2.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft4_lo.16b}, v_data_3_lo.16b
	tbl	v_tmp_hi.16b, {v_gft4_hi.16b}, v_data_3_hi.16b
	eor	v_d4_3.16b, v_tmp_lo.16b, v_d4_3.16b
	eor	v_d4_3.16b, v_d4_3.16b, v_tmp_hi.16b

	/* dest5 */
	tbl	v_tmp_lo.16b, {v_gft5_lo.16b}, v_data_0_lo.16b
	tbl	v_tmp_hi.16b, {v_gft5_hi.16b}, v_data_0_hi.16b
	eor	v_d5_0.16b, v_tmp_lo.16b, v_d5_0.16b
	eor	v_d5_0.16b, v_d5_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft5_lo.16b}, v_data_1_lo.16b
	tbl	v_tmp_hi.16b, {v_gft5_hi.16b}, v_data_1_hi.16b
	eor	v_d5_1.16b, v_tmp_lo.16b, v_d5_1.16b
	eor	v_d5_1.16b, v_d5_1.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft5_lo.16b}, v_data_2_lo.16b
	tbl	v_tmp_hi.16b, {v_gft5_hi.16b}, v_data_2_hi.16b
	eor	v_d5_2.16b, v_tmp_lo.16b, v_d5_2.16b
	eor	v_d5_2.16b, v_d5_2.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft5_lo.16b}, v_data_3_lo.16b
	tbl	v_tmp_hi.16b, {v_gft5_hi.16b}, v_data_3_hi.16b
	eor	v_d5_3.16b, v_tmp_lo.16b, v_d5_3.16b
	eor	v_d5_3.16b, v_d5_3.16b, v_tmp_hi.16b

	str	q_d4_0, [x_dest4, #16*0]
	str	q_d4_1, [x_dest4, #16*1]
	str	q_d4_2, [x_dest4, #16*2]
	str	q_d4_3, [x_dest4, #16*3]
	add	x_dest4, x_dest4, #64

	str	q_d5_0, [x_dest5, #16*0]
	str	q_d5_1, [x_dest5, #16*1]
	str	q_d5_2, [x_dest5, #16*2]
	str	q_d5_3, [x_dest5, #16*3]
	add	x_dest5, x_dest5, #64

	cmp	x_src, x_src_end
	bls	.Lloop64

.Lloop64_end:
	/* restore d8 ~ d15 */
	ldp	d8,  d9,  [sp]
	ldp	d10, d11, [sp, #16]
	ldp	d12, d13, [sp, #32]
	ldp	d14, d15, [sp, #48]
	add	sp, sp, #64
	add	x_src_end, x_src_end, #64

.Lloop16_init:
	sub	x_src_end, x_src_end, #16
	cmp	x_src, x_src_end
	bhi	.lessthan16_init

.Lloop16:
	ldr	q_data, [x_src]

	ldr	q_d1_0, [x_dest1]
	ldr	q_d2_0, [x_dest2]
	ldr	q_d3_0, [x_dest3]
	ldr	q_gft2_lo, [x_tbl2]
	ldr	q_gft2_hi, [x_tbl2, #16]

	and	v_data_lo.16b, v_data.16b, v_mask0f.16b
	ushr	v_data_hi.16b, v_data.16b, #4

	tbl	v_tmp_lo.16b, {v_gft1_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft1_hi.16b}, v_data_hi.16b
	eor	v_d1_0.16b, v_tmp_lo.16b, v_d1_0.16b
	eor	v_d1_0.16b, v_d1_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft2_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft2_hi.16b}, v_data_hi.16b
	eor	v_d2_0.16b, v_tmp_lo.16b, v_d2_0.16b
	eor	v_d2_0.16b, v_d2_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft3_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft3_hi.16b}, v_data_hi.16b
	eor	v_d3_0.16b, v_tmp_lo.16b, v_d3_0.16b
	eor	v_d3_0.16b, v_d3_0.16b, v_tmp_hi.16b

	str	q_d1_0, [x_dest1]
	str	q_d2_0, [x_dest2]
	str	q_d3_0, [x_dest3]

	ldr	q_d4_0, [x_dest4]
	ldr	q_d5_0, [x_dest5]
	ldr	q_gft5_lo, [x_tbl5]
	ldr	q_gft5_hi, [x_tbl5, #16]

	tbl	v_tmp_lo.16b, {v_gft4_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft4_hi.16b}, v_data_hi.16b
	eor	v_d4_0.16b, v_tmp_lo.16b, v_d4_0.16b
	eor	v_d4_0.16b, v_d4_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft5_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft5_hi.16b}, v_data_hi.16b
	eor	v_d5_0.16b, v_tmp_lo.16b, v_d5_0.16b
	eor	v_d5_0.16b, v_d5_0.16b, v_tmp_hi.16b

	str	q_d4_0, [x_dest4]
	str	q_d5_0, [x_dest5]

	add	x_src, x_src, #16
	add	x_dest1, x_dest1, #16
	add	x_dest2, x_dest2, #16
	add	x_dest3, x_dest3, #16
	add	x_dest4, x_dest4, #16
	add	x_dest5, x_dest5, #16
	cmp	x_src, x_src_end
	bls	.Lloop16

.lessthan16_init:
	sub	x_tmp, x_src, x_src_end
	cmp	x_tmp, #16
	beq	.return_pass

.lessthan16:
	mov	x_src, x_src_end
	sub	x_dest1, x_dest1, x_tmp
	sub	x_dest2, x_dest2, x_tmp
	sub	x_dest3, x_dest3, x_tmp
	sub	x_dest4, x_dest4, x_tmp
	sub	x_dest5, x_dest5, x_tmp

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
	ldr	q_d2_0, [x_dest2]
	ldr	q_d3_0, [x_dest3]
	ldr	q_gft2_lo, [x_tbl2]
	ldr	q_gft2_hi, [x_tbl2, #16]

	and	v_data_lo.16b, v_data.16b, v_mask0f.16b
	ushr	v_data_hi.16b, v_data.16b, #4

	tbl	v_tmp_lo.16b, {v_gft1_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft1_hi.16b}, v_data_hi.16b
	eor	v_tmp_hi.16b, v_tmp_lo.16b, v_tmp_hi.16b
	and	v_tmp_hi.16b, v_tmp_hi.16b, v_tmp.16b
	eor	v_d1_0.16b, v_d1_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft2_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft2_hi.16b}, v_data_hi.16b
	eor	v_tmp_hi.16b, v_tmp_lo.16b, v_tmp_hi.16b
	and	v_tmp_hi.16b, v_tmp_hi.16b, v_tmp.16b
	eor	v_d2_0.16b, v_d2_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft3_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft3_hi.16b}, v_data_hi.16b
	eor	v_tmp_hi.16b, v_tmp_lo.16b, v_tmp_hi.16b
	and	v_tmp_hi.16b, v_tmp_hi.16b, v_tmp.16b
	eor	v_d3_0.16b, v_d3_0.16b, v_tmp_hi.16b

	str	q_d1_0, [x_dest1]
	str	q_d2_0, [x_dest2]
	str	q_d3_0, [x_dest3]

	ldr	q_d4_0, [x_dest4]
	ldr	q_d5_0, [x_dest5]
	ldr	q_gft5_lo, [x_tbl5]
	ldr	q_gft5_hi, [x_tbl5, #16]

	tbl	v_tmp_lo.16b, {v_gft4_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft4_hi.16b}, v_data_hi.16b
	eor	v_tmp_hi.16b, v_tmp_lo.16b, v_tmp_hi.16b
	and	v_tmp_hi.16b, v_tmp_hi.16b, v_tmp.16b
	eor	v_d4_0.16b, v_d4_0.16b, v_tmp_hi.16b

	tbl	v_tmp_lo.16b, {v_gft5_lo.16b}, v_data_lo.16b
	tbl	v_tmp_hi.16b, {v_gft5_hi.16b}, v_data_hi.16b
	eor	v_tmp_hi.16b, v_tmp_lo.16b, v_tmp_hi.16b
	and	v_tmp_hi.16b, v_tmp_hi.16b, v_tmp.16b
	eor	v_d5_0.16b, v_d5_0.16b, v_tmp_hi.16b

	str	q_d4_0, [x_dest4]
	str	q_d5_0, [x_dest5]

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
