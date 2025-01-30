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

 extern _ec_encode_data_update_sse
 extern _ec_encode_data_update_avx
 extern _ec_encode_data_update_avx2
%ifdef HAVE_AS_KNOWS_AVX512
 extern _ec_encode_data_avx512
 extern _gf_vect_dot_prod_avx512
 extern _ec_encode_data_update_avx512
 extern _gf_vect_mad_avx512
%endif
 extern _gf_vect_mul_sse
 extern _gf_vect_mul_avx

 extern _gf_vect_mad_sse
 extern _gf_vect_mad_avx
 extern _gf_vect_mad_avx2
%endif

%if (AS_FEATURE_LEVEL) >= 10
 extern _ec_init_tables_gfni
 extern _ec_encode_data_avx512_gfni
 extern _ec_encode_data_avx2_gfni
 extern _ec_encode_data_update_avx512_gfni
 extern _ec_encode_data_update_avx2_gfni
%endif

extern _ec_init_tables_base

extern _gf_vect_mul_base
extern _ec_encode_data_base
extern _ec_encode_data_update_base
extern _gf_vect_dot_prod_base
extern _gf_vect_mad_base

extern _gf_vect_dot_prod_sse
extern _gf_vect_dot_prod_avx
extern _gf_vect_dot_prod_avx2
extern _ec_encode_data_sse
extern _ec_encode_data_avx
extern _ec_encode_data_avx2

mbin_interface _ec_encode_data
mbin_interface _gf_vect_dot_prod
mbin_interface _gf_vect_mul
mbin_interface _ec_encode_data_update
mbin_interface _gf_vect_mad
mbin_interface _ec_init_tables

%ifidn __OUTPUT_FORMAT__, elf32
 mbin_dispatch_init5 _ec_encode_data, _ec_encode_data_base, _ec_encode_data_sse, _ec_encode_data_avx, _ec_encode_data_avx2
 mbin_dispatch_init5 _gf_vect_dot_prod, _gf_vect_dot_prod_base, _gf_vect_dot_prod_sse, _gf_vect_dot_prod_avx, _gf_vect_dot_prod_avx2
 mbin_dispatch_init2 _gf_vect_mul, _gf_vect_mul_base
 mbin_dispatch_init2 _ec_encode_data_update, _ec_encode_data_update_base
 mbin_dispatch_init2 _gf_vect_mad, _gf_vect_mad_base
 mbin_dispatch_init2 _ec_init_tables, _ec_init_tables_base
%else

 mbin_dispatch_init5 _gf_vect_mul, _gf_vect_mul_base, _gf_vect_mul_sse, _gf_vect_mul_avx, _gf_vect_mul_avx
 mbin_dispatch_init8 _ec_encode_data, _ec_encode_data_base, _ec_encode_data_sse, _ec_encode_data_avx, _ec_encode_data_avx2, _ec_encode_data_avx512, _ec_encode_data_avx2_gfni, _ec_encode_data_avx512_gfni
 mbin_dispatch_init8 _ec_encode_data_update, _ec_encode_data_update_base, _ec_encode_data_update_sse, _ec_encode_data_update_avx, _ec_encode_data_update_avx2, _ec_encode_data_update_avx512, _ec_encode_data_update_avx2_gfni, _ec_encode_data_update_avx512_gfni
 mbin_dispatch_init6 _gf_vect_mad, _gf_vect_mad_base, _gf_vect_mad_sse, _gf_vect_mad_avx, _gf_vect_mad_avx2, _gf_vect_mad_avx512
 mbin_dispatch_init6 _gf_vect_dot_prod, _gf_vect_dot_prod_base, _gf_vect_dot_prod_sse, _gf_vect_dot_prod_avx, _gf_vect_dot_prod_avx2, _gf_vect_dot_prod_avx512
 mbin_dispatch_init8 _ec_init_tables, _ec_init_tables_base, _ec_init_tables_base, _ec_init_tables_base, _ec_init_tables_base, _ec_init_tables_base, _ec_init_tables_gfni, _ec_init_tables_gfni
%endif
