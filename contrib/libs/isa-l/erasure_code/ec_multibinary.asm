;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Copyright(c) 2011-2015 Intel Corporation All rights reserved.
;
;  Redistribution and use in source and binary forms, with or without
;  modification, are permitted provided that the following conditions
;  are met:
;    * Redistributions of source code must retain the above copyright
;      notice, this list of conditions and the following disclaimer.
;    * Redistributions in binary form must reproduce the above copyright
;      notice, this list of conditions and the following disclaimer in
;      the documentation and/or other materials provided with the
;      distribution.
;    * Neither the name of Intel Corporation nor the names of its
;      contributors may be used to endorse or promote products derived
;      from this software without specific prior written permission.
;
;  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
;  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
;  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
;  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
;  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
;  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
;  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
;  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
;  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
;  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
;  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

%include "reg_sizes.asm"
%include "multibinary.asm"

%ifidn __OUTPUT_FORMAT__, elf32
 [bits 32]
%else
 default rel
 [bits 64]

 extern ec_encode_data_update_sse
 extern ec_encode_data_update_avx
 extern ec_encode_data_update_avx2
%ifdef HAVE_AS_KNOWS_AVX512
 extern ec_encode_data_avx512
 extern gf_vect_dot_prod_avx512
 extern ec_encode_data_update_avx512
 extern gf_vect_mad_avx512
%endif
 extern gf_vect_mul_sse
 extern gf_vect_mul_avx

 extern gf_vect_mad_sse
 extern gf_vect_mad_avx
 extern gf_vect_mad_avx2
%endif

extern gf_vect_mul_base
extern ec_encode_data_base
extern ec_encode_data_update_base
extern gf_vect_dot_prod_base
extern gf_vect_mad_base

extern gf_vect_dot_prod_sse
extern gf_vect_dot_prod_avx
extern gf_vect_dot_prod_avx2
extern ec_encode_data_sse
extern ec_encode_data_avx
extern ec_encode_data_avx2

mbin_interface ec_encode_data
mbin_interface gf_vect_dot_prod
mbin_interface gf_vect_mul
mbin_interface ec_encode_data_update
mbin_interface gf_vect_mad

%ifidn __OUTPUT_FORMAT__, elf32
 mbin_dispatch_init5 ec_encode_data, ec_encode_data_base, ec_encode_data_sse, ec_encode_data_avx, ec_encode_data_avx2
 mbin_dispatch_init5 gf_vect_dot_prod, gf_vect_dot_prod_base, gf_vect_dot_prod_sse, gf_vect_dot_prod_avx, gf_vect_dot_prod_avx2
 mbin_dispatch_init2 gf_vect_mul, gf_vect_mul_base
 mbin_dispatch_init2 ec_encode_data_update, ec_encode_data_update_base
 mbin_dispatch_init2 gf_vect_mad, gf_vect_mad_base
%else

 mbin_dispatch_init5 gf_vect_mul, gf_vect_mul_base, gf_vect_mul_sse, gf_vect_mul_avx, gf_vect_mul_avx
 mbin_dispatch_init6 ec_encode_data, ec_encode_data_base, ec_encode_data_sse, ec_encode_data_avx, ec_encode_data_avx2, ec_encode_data_avx512
 mbin_dispatch_init6 ec_encode_data_update, ec_encode_data_update_base, ec_encode_data_update_sse, ec_encode_data_update_avx, ec_encode_data_update_avx2, ec_encode_data_update_avx512
 mbin_dispatch_init6 gf_vect_mad, gf_vect_mad_base, gf_vect_mad_sse, gf_vect_mad_avx, gf_vect_mad_avx2, gf_vect_mad_avx512
 mbin_dispatch_init6 gf_vect_dot_prod, gf_vect_dot_prod_base, gf_vect_dot_prod_sse, gf_vect_dot_prod_avx, gf_vect_dot_prod_avx2, gf_vect_dot_prod_avx512
%endif

;;;       func                 		core, ver, snum
slversion ec_encode_data,		00,   06,  0133
slversion gf_vect_mul,			00,   05,  0134
slversion ec_encode_data_update,	00,   05,  0212
slversion gf_vect_dot_prod,		00,   05,  0138
slversion gf_vect_mad,			00,   04,  0213
