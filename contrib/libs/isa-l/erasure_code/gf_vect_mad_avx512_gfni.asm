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
;;; gf_vect_mad_avx512_gfni(len, vec, vec_i, mul_array, src, dest);
;;;

%include "reg_sizes.asm"
%include "gf_vect_gfni.inc"

%if AS_FEATURE_LEVEL >= 10

%ifidn __OUTPUT_FORMAT__, elf64
 %define arg0  rdi
 %define arg1  rsi
 %define arg2  rdx
 %define arg3  rcx
 %define arg4  r8
 %define arg5  r9
 %define tmp   r11
 %define func(x) x: endbranch
 %define FUNC_SAVE
 %define FUNC_RESTORE
%endif

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9
 %define arg4   r12 		; must be saved and loaded
 %define arg5   r13
 %define tmp    r11
 %define stack_size 3*8
 %define arg(x)      [rsp + stack_size + 8 + 8*x]
 %define func(x) proc_frame x

 %macro FUNC_SAVE 0
	sub	rsp, stack_size
	mov	[rsp + 0*8], r12
	mov	[rsp + 1*8], r13
	end_prolog
	mov	arg4, arg(4)
	mov	arg5, arg(5)
 %endmacro

 %macro FUNC_RESTORE 0
	mov	r12,  [rsp + 0*8]
	mov	r13,  [rsp + 1*8]
	add	rsp, stack_size
 %endmacro
%endif

;;; gf_vect_mad_avx512_gfni(len, vec, vec_i, mul_array, src, dest)
%define len   arg0
%define vec   arg1
%define vec_i    arg2
%define mul_array arg3
%define	src   arg4
%define dest  arg5
%define pos   rax

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

%define x0       zmm0
%define xd       zmm1
%define xgft1    zmm2
%define xret1    zmm3

;;
;; Encodes 64 bytes of a single source into 64 bytes (single parity disk)
;;
%macro ENCODE_64B 0-1
%define %%KMASK %1

%if %0 == 1
	vmovdqu8 x0{%%KMASK}, [src + pos]	;Get next source vector
	vmovdqu8 xd{%%KMASK}, [dest + pos]	;Get next dest vector
%else
	XLDR	x0, [src + pos]	;Get next source vector
	XLDR	xd, [dest + pos]		;Get next dest vector
%endif

        GF_MUL_XOR EVEX, x0, xgft1, xret1, xd

%if %0 == 1
	vmovdqu8 [dest + pos]{%%KMASK}, xd
%else
	XSTR	[dest + pos], xd
%endif
%endmacro

align 16
global gf_vect_mad_avx512_gfni, function
func(gf_vect_mad_avx512_gfni)
	FUNC_SAVE
	xor	pos, pos
	shl	vec_i, 3		;Multiply by 8

        vbroadcastf32x2 xgft1, [vec_i + mul_array]

        cmp     len, 64
        jl      .len_lt_64
.loop64:
        ENCODE_64B

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

        ENCODE_64B k1

.exit:
        vzeroupper

	FUNC_RESTORE
	ret

endproc_frame
%endif  ; if AS_FEATURE_LEVEL >= 10
