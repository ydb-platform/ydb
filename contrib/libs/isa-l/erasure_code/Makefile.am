########################################################################
#  Copyright(c) 2011-2019 Intel Corporation All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions
#  are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in
#      the documentation and/or other materials provided with the
#      distribution.
#    * Neither the name of Intel Corporation nor the names of its
#      contributors may be used to endorse or promote products derived
#      from this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
#  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
#  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
########################################################################

include erasure_code/aarch64/Makefile.am

include erasure_code/ppc64le/Makefile.am

lsrc         += erasure_code/ec_base.c

lsrc_base_aliases += erasure_code/ec_base_aliases.c
lsrc_x86_64  += \
		erasure_code/ec_highlevel_func.c \
		erasure_code/gf_vect_mul_sse.asm \
		erasure_code/gf_vect_mul_avx.asm \
		erasure_code/gf_vect_dot_prod_sse.asm  \
		erasure_code/gf_vect_dot_prod_avx.asm \
		erasure_code/gf_vect_dot_prod_avx2.asm \
		erasure_code/gf_2vect_dot_prod_sse.asm \
		erasure_code/gf_3vect_dot_prod_sse.asm \
		erasure_code/gf_4vect_dot_prod_sse.asm \
		erasure_code/gf_5vect_dot_prod_sse.asm \
		erasure_code/gf_6vect_dot_prod_sse.asm \
		erasure_code/gf_2vect_dot_prod_avx.asm \
		erasure_code/gf_3vect_dot_prod_avx.asm \
		erasure_code/gf_4vect_dot_prod_avx.asm \
		erasure_code/gf_5vect_dot_prod_avx.asm \
		erasure_code/gf_6vect_dot_prod_avx.asm \
		erasure_code/gf_2vect_dot_prod_avx2.asm \
		erasure_code/gf_3vect_dot_prod_avx2.asm \
		erasure_code/gf_4vect_dot_prod_avx2.asm \
		erasure_code/gf_5vect_dot_prod_avx2.asm \
		erasure_code/gf_6vect_dot_prod_avx2.asm \
		erasure_code/gf_vect_mad_sse.asm \
		erasure_code/gf_2vect_mad_sse.asm \
		erasure_code/gf_3vect_mad_sse.asm \
		erasure_code/gf_4vect_mad_sse.asm \
		erasure_code/gf_5vect_mad_sse.asm \
		erasure_code/gf_6vect_mad_sse.asm \
		erasure_code/gf_vect_mad_avx.asm \
		erasure_code/gf_2vect_mad_avx.asm \
		erasure_code/gf_3vect_mad_avx.asm \
		erasure_code/gf_4vect_mad_avx.asm \
		erasure_code/gf_5vect_mad_avx.asm \
		erasure_code/gf_6vect_mad_avx.asm \
		erasure_code/gf_vect_mad_avx2.asm \
		erasure_code/gf_2vect_mad_avx2.asm \
		erasure_code/gf_3vect_mad_avx2.asm \
		erasure_code/gf_4vect_mad_avx2.asm \
		erasure_code/gf_5vect_mad_avx2.asm \
		erasure_code/gf_6vect_mad_avx2.asm \
		erasure_code/ec_multibinary.asm

#if HAVE_AVX512
lsrc_x86_64  += \
		erasure_code/gf_vect_mad_avx2_gfni.asm \
		erasure_code/gf_2vect_mad_avx2_gfni.asm \
		erasure_code/gf_3vect_mad_avx2_gfni.asm \
		erasure_code/gf_4vect_mad_avx2_gfni.asm \
		erasure_code/gf_5vect_mad_avx2_gfni.asm \
		erasure_code/gf_vect_dot_prod_avx512.asm \
		erasure_code/gf_2vect_dot_prod_avx512.asm \
		erasure_code/gf_3vect_dot_prod_avx512.asm \
		erasure_code/gf_4vect_dot_prod_avx512.asm \
		erasure_code/gf_5vect_dot_prod_avx512.asm \
		erasure_code/gf_6vect_dot_prod_avx512.asm \
		erasure_code/gf_vect_dot_prod_avx512_gfni.asm \
		erasure_code/gf_vect_dot_prod_avx2_gfni.asm \
		erasure_code/gf_2vect_dot_prod_avx2_gfni.asm \
		erasure_code/gf_3vect_dot_prod_avx2_gfni.asm \
		erasure_code/gf_2vect_dot_prod_avx512_gfni.asm \
		erasure_code/gf_3vect_dot_prod_avx512_gfni.asm \
		erasure_code/gf_4vect_dot_prod_avx512_gfni.asm \
		erasure_code/gf_5vect_dot_prod_avx512_gfni.asm \
		erasure_code/gf_6vect_dot_prod_avx512_gfni.asm \
		erasure_code/gf_vect_mad_avx512.asm \
		erasure_code/gf_2vect_mad_avx512.asm \
		erasure_code/gf_3vect_mad_avx512.asm \
		erasure_code/gf_4vect_mad_avx512.asm \
		erasure_code/gf_5vect_mad_avx512.asm \
		erasure_code/gf_6vect_mad_avx512.asm \
		erasure_code/gf_vect_mad_avx512_gfni.asm \
		erasure_code/gf_2vect_mad_avx512_gfni.asm \
		erasure_code/gf_3vect_mad_avx512_gfni.asm \
		erasure_code/gf_4vect_mad_avx512_gfni.asm \
		erasure_code/gf_5vect_mad_avx512_gfni.asm \
		erasure_code/gf_6vect_mad_avx512_gfni.asm

lsrc_x86_32  += \
		erasure_code/ec_highlevel_func.c \
		erasure_code/ec_multibinary.asm \
		erasure_code/gf_vect_dot_prod_avx.asm \
		erasure_code/gf_2vect_dot_prod_avx.asm \
		erasure_code/gf_3vect_dot_prod_avx.asm \
		erasure_code/gf_4vect_dot_prod_avx.asm \
		erasure_code/gf_vect_dot_prod_sse.asm \
		erasure_code/gf_2vect_dot_prod_sse.asm \
		erasure_code/gf_3vect_dot_prod_sse.asm \
		erasure_code/gf_4vect_dot_prod_sse.asm \
		erasure_code/gf_vect_dot_prod_avx2.asm \
		erasure_code/gf_2vect_dot_prod_avx2.asm \
		erasure_code/gf_3vect_dot_prod_avx2.asm \
		erasure_code/gf_4vect_dot_prod_avx2.asm

unit_tests32 += erasure_code/erasure_code_base_test \
		erasure_code/erasure_code_test \
		erasure_code/gf_vect_mul_test \
		erasure_code/gf_vect_mul_base_test \
		erasure_code/gf_vect_dot_prod_base_test \
		erasure_code/gf_vect_dot_prod_test

perf_tests32 += erasure_code/gf_vect_mul_perf \
		erasure_code/gf_vect_dot_prod_perf \
		erasure_code/erasure_code_perf \
		erasure_code/erasure_code_base_perf \
		erasure_code/gf_vect_dot_prod_1tbl

src_include += -I $(srcdir)/erasure_code
extern_hdrs  += include/erasure_code.h \
		include/gf_vect_mul.h

other_src    += erasure_code/ec_base.h \
		include/multibinary.asm \
		include/reg_sizes.asm

check_tests  += erasure_code/gf_vect_mul_test \
		erasure_code/erasure_code_test \
		erasure_code/gf_inverse_test \
		erasure_code/erasure_code_update_test

unit_tests   += \
		erasure_code/gf_vect_mul_base_test \
		erasure_code/gf_vect_dot_prod_base_test \
		erasure_code/gf_vect_dot_prod_test \
		erasure_code/gf_vect_mad_test \
		erasure_code/erasure_code_base_test

perf_tests   += erasure_code/gf_vect_mul_perf \
		erasure_code/gf_vect_dot_prod_perf \
		erasure_code/gf_vect_dot_prod_1tbl \
		erasure_code/erasure_code_perf \
		erasure_code/erasure_code_base_perf \
		erasure_code/erasure_code_update_perf

other_tests  += erasure_code/gen_rs_matrix_limits

other_src    += include/test.h
