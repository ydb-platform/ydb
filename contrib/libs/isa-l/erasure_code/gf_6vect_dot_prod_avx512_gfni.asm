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
;;; gf_6vect_dot_prod_avx512_gfni(len, vec, *g_tbls, **buffs, **dests);
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
 %define tmp2  r10
 %define tmp3  r13		; must be saved and restored
 %define tmp4  r12		; must be saved and restored
 %define tmp5  r14		; must be saved and restored
 %define tmp6  r15		; must be saved and restored
 %define tmp7  rbp		; must be saved and restored
 %define tmp8  rbx		; must be saved and restored

 %define func(x) x: endbranch
 %macro FUNC_SAVE 0
	push	r12
	push	r13
	push	r14
	push	r15
	push	rbp
	push	rbx
 %endmacro
 %macro FUNC_RESTORE 0
	pop	rbx
	pop	rbp
	pop	r15
	pop	r14
	pop	r13
	pop	r12
 %endmacro
%endif

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9

 %define arg4   r12 		; must be saved, loaded and restored
 %define arg5   r15 		; must be saved and restored
 %define tmp    r11
 %define tmp2   r10
 %define tmp3   r13		; must be saved and restored
 %define tmp4   r14		; must be saved and restored
 %define tmp5   rdi		; must be saved and restored
 %define tmp6   rsi		; must be saved and restored
 %define tmp7   rbp		; must be saved and restored
 %define tmp8   rbx		; must be saved and restored
 %define stack_size  7*16 + 9*8		; must be an odd multiple of 8
 %define arg(x)      [rsp + stack_size + 8 + 8*x]

 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
	alloc_stack	stack_size
	vmovdqa	[rsp + 0*16], xmm6
	vmovdqa	[rsp + 1*16], xmm7
	vmovdqa	[rsp + 2*16], xmm8
	vmovdqa	[rsp + 3*16], xmm9
	vmovdqa	[rsp + 4*16], xmm10
	vmovdqa	[rsp + 5*16], xmm11
	vmovdqa	[rsp + 6*16], xmm12
	mov	[rsp + 7*16 + 0*8], r12
	mov	[rsp + 7*16 + 1*8], r13
	mov	[rsp + 7*16 + 2*8], r14
	mov	[rsp + 7*16 + 3*8], r15
	mov	[rsp + 7*16 + 4*8], rdi
	mov	[rsp + 7*16 + 5*8], rsi
	mov	[rsp + 7*16 + 6*8], rbp
	mov	[rsp + 7*16 + 7*8], rbx
	end_prolog
	mov	arg4, arg(4)
 %endmacro

 %macro FUNC_RESTORE 0
	vmovdqa	xmm6, [rsp + 0*16]
	vmovdqa	xmm7, [rsp + 1*16]
	vmovdqa	xmm8, [rsp + 2*16]
	vmovdqa	xmm9, [rsp + 3*16]
	vmovdqa	xmm10, [rsp + 4*16]
	vmovdqa	xmm11, [rsp + 5*16]
	vmovdqa	xmm12, [rsp + 6*16]
	mov	r12,  [rsp + 7*16 + 0*8]
	mov	r13,  [rsp + 7*16 + 1*8]
	mov	r14,  [rsp + 7*16 + 2*8]
	mov	r15,  [rsp + 7*16 + 3*8]
	mov	rdi,  [rsp + 7*16 + 4*8]
	mov	rsi,  [rsp + 7*16 + 5*8]
	mov	rbp,  [rsp + 7*16 + 6*8]
	mov	rbx,  [rsp + 7*16 + 7*8]
	add	rsp, stack_size
 %endmacro
%endif


%define len    arg0
%define vec    arg1
%define mul_array arg2
%define src    arg3
%define dest1  arg4
%define ptr    arg5
%define vec_i  tmp2
%define dest2  tmp3
%define dest3  tmp4
%define dest4  tmp5
%define vskip3 tmp6
%define dest5  tmp7
%define vskip5 tmp8
%define pos    rax


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

%define xgft1  zmm7
%define xgft2  zmm8
%define xgft3  zmm9
%define xgft4  zmm10
%define xgft5  zmm11
%define xgft6  zmm12

%define x0     zmm0
%define xp1    zmm1
%define xp2    zmm2
%define xp3    zmm3
%define xp4    zmm4
%define xp5    zmm5
%define xp6    zmm6

default rel
[bits 64]

section .text

;;
;; Encodes 64 bytes of all "k" sources into 6x 64 bytes (parity disks)
;;
%macro ENCODE_64B_6 0-1
%define %%KMASK %1

	vpxorq	xp1, xp1, xp1
	vpxorq	xp2, xp2, xp2
	vpxorq	xp3, xp3, xp3
	vpxorq	xp4, xp4, xp4
	vpxorq	xp5, xp5, xp5
	vpxorq	xp6, xp6, xp6
	mov	tmp, mul_array
	xor	vec_i, vec_i

%%next_vect:
	mov	ptr, [src + vec_i]
%if %0 == 1
	vmovdqu8 x0{%%KMASK}, [ptr + pos]	;Get next source vector (less than 64 bytes)
%else
	XLDR	x0, [ptr + pos]		;Get next source vector (64 bytes)
%endif
	add	vec_i, 8

        vbroadcastf32x2 xgft1, [tmp]
        vbroadcastf32x2 xgft2, [tmp + vec]
        vbroadcastf32x2 xgft3, [tmp + vec*2]
        vbroadcastf32x2 xgft4, [tmp + vskip3]
        vbroadcastf32x2 xgft5, [tmp + vec*4]
        vbroadcastf32x2 xgft6, [tmp + vskip5]
	add	tmp, 8

        GF_MUL_XOR EVEX, x0, xgft1, xgft1, xp1, xgft2, xgft2, xp2, xgft3, xgft3, xp3, \
                       xgft4, xgft4, xp4, xgft5, xgft5, xp5, xgft6, xgft6, xp6

	cmp	vec_i, vec
	jl	%%next_vect

        mov     ptr, [dest1]			;reuse ptr
        mov     tmp, [dest1 + 5*8]		;reuse tmp

%if %0 == 1
	vmovdqu8 [dest2 + pos]{%%KMASK}, xp2
	vmovdqu8 [dest3 + pos]{%%KMASK}, xp3
	vmovdqu8 [dest4 + pos]{%%KMASK}, xp4
	vmovdqu8 [dest5 + pos]{%%KMASK}, xp5
	vmovdqu8 [ptr + pos]{%%KMASK}, xp1 ; dest 1
	vmovdqu8 [tmp + pos]{%%KMASK}, xp6 ; dest 6
%else
	XSTR	[dest2 + pos], xp2
	XSTR	[dest3 + pos], xp3
	XSTR	[dest4 + pos], xp4
	XSTR	[dest5 + pos], xp5
	XSTR	[ptr + pos], xp1 ; dest 1
	XSTR	[tmp + pos], xp6 ; dest 6
%endif
%endmacro

align 16
global gf_6vect_dot_prod_avx512_gfni, function
func(gf_6vect_dot_prod_avx512_gfni)
	FUNC_SAVE

	xor	pos, pos
	mov	vskip3, vec
	imul	vskip3, 3*8
	mov	vskip5, vec
	imul	vskip5, 5*8
	shl	vec, 3		;vec *= 8. Make vec_i count by 8
	mov	dest2, [dest1 + 8]
	mov	dest3, [dest1 + 2*8]
	mov	dest4, [dest1 + 3*8]
	mov	dest5, [dest1 + 4*8]      ;dest1 and dest6 are calculated later

	cmp	len, 64
        jl      .len_lt_64

.loop64:

        ENCODE_64B_6

	add	pos, 64	                ;Loop on 64 bytes at a time
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

        ENCODE_64B_6 k1

.exit:
        vzeroupper

	FUNC_RESTORE
	ret

endproc_frame
%endif  ; if AS_FEATURE_LEVEL >= 10
