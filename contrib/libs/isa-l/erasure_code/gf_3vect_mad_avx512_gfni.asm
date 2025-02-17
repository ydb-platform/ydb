;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Copyright(c) 2023 Intel Corporation All rights reserved.
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

;;;
;;; gf_3vect_mad_avx512_gfni(len, vec, vec_i, mul_array, src, dest);
;;;

%include "reg_sizes.asm"
%include "gf_vect_gfni.inc"

%if AS_FEATURE_LEVEL >= 10

%ifidn __OUTPUT_FORMAT__, elf64
 %define arg0   rdi
 %define arg1   rsi
 %define arg2   rdx
 %define arg3   rcx
 %define arg4   r8
 %define arg5   r9
 %define tmp    r11
 %define func(x) x: endbranch
 %define FUNC_SAVE
 %define FUNC_RESTORE
%endif

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9
 %define arg4   r12 		; must be saved, loaded and restored
 %define arg5   r13 		; must be saved and restored
 %define tmp    r11
 %define stack_size 16*4 + 3*8
 %define arg(x)      [rsp + stack_size + 8 + 8*x]
 %define func(x) proc_frame x

 %macro FUNC_SAVE 0
	sub	rsp, stack_size
	vmovdqa	[rsp + 16*0], xmm6
	vmovdqa	[rsp + 16*1], xmm7
	vmovdqa	[rsp + 16*2], xmm8
	vmovdqa	[rsp + 16*3], xmm9
	mov	[rsp + 4*16 + 0*8], r12
	mov	[rsp + 4*16 + 1*8], r13
	end_prolog
	mov	arg4, arg(4)
	mov	arg5, arg(5)
 %endmacro

 %macro FUNC_RESTORE 0
	vmovdqa	xmm6, [rsp + 16*0]
	vmovdqa	xmm7, [rsp + 16*1]
	vmovdqa	xmm8, [rsp + 16*2]
	vmovdqa	xmm9, [rsp + 16*3]
	mov	r12,  [rsp + 4*16 + 0*8]
	mov	r13,  [rsp + 4*16 + 1*8]
	add	rsp, stack_size
 %endmacro
%endif

%define len   arg0
%define vec   arg1
%define vec_i arg2
%define mul_array arg3
%define	src   arg4
%define dest1 arg5
%define pos   rax
%define dest2 mul_array
%define dest3 vec_i

%ifndef EC_ALIGNED_ADDR
;;; Use Un-aligned load/store
 %define XLDR vmovdqu8
 %define XSTR vmovdqu8
%else
;;; Use Non-temporal load/stor
 %ifdef NO_NT_LDST
  %define XLDR vmovdqa64
  %define XSTR vmovdqa64
 %else
  %define XLDR vmovntdqa
  %define XSTR vmovntdq
 %endif
%endif

default rel
[bits 64]
section .text

%define x0        zmm0
%define xgft1     zmm1
%define xgft2     zmm2
%define xgft3     zmm3
%define xd1       zmm4
%define xd2       zmm5
%define xd3       zmm6

%define xret1  zmm7
%define xret2  zmm8
%define xret3  zmm9

;;
;; Encodes 64 bytes of a single source into 3x 64 bytes (parity disks)
;;
%macro ENCODE_64B_3 0-1
%define %%KMASK %1

%if %0 == 1
	vmovdqu8 x0{%%KMASK}, [src + pos]	;Get next source vector
	vmovdqu8 xd1{%%KMASK}, [dest1 + pos]	;Get next dest vector
	vmovdqu8 xd2{%%KMASK}, [dest2 + pos]	;Get next dest vector
	vmovdqu8 xd3{%%KMASK}, [dest3 + pos]	;Get next dest vector
%else
	XLDR	x0, [src + pos]	;Get next source vector
	XLDR	xd1, [dest1 + pos]	;Get next dest vector
	XLDR	xd2, [dest2 + pos]	;Get next dest vector
	XLDR	xd3, [dest3 + pos]	;Get next dest vector
%endif

        GF_MUL_XOR EVEX, x0, xgft1, xret1, xd1, xgft2, xret2, xd2, xgft3, xret3, xd3

%if %0 == 1
	vmovdqu8 [dest1 + pos]{%%KMASK}, xd1
	vmovdqu8 [dest2 + pos]{%%KMASK}, xd2
	vmovdqu8 [dest3 + pos]{%%KMASK}, xd3
%else
	XSTR	[dest1 + pos], xd1
	XSTR	[dest2 + pos], xd2
	XSTR	[dest3 + pos], xd3
%endif
%endmacro

align 16
global gf_3vect_mad_avx512_gfni, function
func(gf_3vect_mad_avx512_gfni)
	FUNC_SAVE

	xor	pos, pos
	shl	vec_i, 3		;Multiply by 8
	shl	vec, 3			;Multiply by 8
	lea	tmp, [mul_array + vec_i]
        vbroadcastf32x2 xgft1, [tmp]
        vbroadcastf32x2 xgft2, [tmp + vec]
        vbroadcastf32x2 xgft3, [tmp + vec*2]
	mov	dest2, [dest1 + 8]		; reuse mul_array
	mov	dest3, [dest1 + 2*8]		; reuse vec
	mov	dest1, [dest1]

        cmp     len, 64
        jl      .len_lt_64
.loop64:
        ENCODE_64B_3

	add	pos, 64			;Loop on 64 bytes at a time
        sub     len, 64
	cmp	len, 64
	jge	.loop64

.len_lt_64:
        cmp     len, 0
        jle     .exit

        xor     tmp, tmp
        bts     tmp, len
        dec     tmp
        kmovq   k1, tmp

        ENCODE_64B_3 k1

.exit:
        vzeroupper

	FUNC_RESTORE
	ret

endproc_frame
%endif  ; if AS_FEATURE_LEVEL >= 10
