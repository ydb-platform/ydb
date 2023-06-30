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

;;;
;;; gf_4vect_dot_prod_avx512(len, vec, *g_tbls, **buffs, **dests);
;;;

%include "reg_sizes.asm"

%ifdef HAVE_AS_KNOWS_AVX512

%ifidn __OUTPUT_FORMAT__, elf64
 %define arg0  rdi
 %define arg1  rsi
 %define arg2  rdx
 %define arg3  rcx
 %define arg4  r8
 %define arg5  r9

 %define tmp   r11
 %define tmp.w r11d
 %define tmp.b r11b
 %define tmp2  r10
 %define tmp3  r13		; must be saved and restored
 %define tmp4  r12		; must be saved and restored
 %define tmp5  r14		; must be saved and restored
 %define tmp6  r15		; must be saved and restored
 %define return rax
 %define PS     8
 %define LOG_PS 3

 %define func(x) x:
 %macro FUNC_SAVE 0
	push	r12
	push	r13
	push	r14
	push	r15
 %endmacro
 %macro FUNC_RESTORE 0
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
 %define tmp.w  r11d
 %define tmp.b  r11b
 %define tmp2   r10
 %define tmp3   r13		; must be saved and restored
 %define tmp4   r14		; must be saved and restored
 %define tmp5   rdi		; must be saved and restored
 %define tmp6   rsi		; must be saved and restored
 %define return rax
 %define PS     8
 %define LOG_PS 3
 %define stack_size  9*16 + 7*8		; must be an odd multiple of 8
 %define arg(x)      [rsp + stack_size + PS + PS*x]

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
	vmovdqa	[rsp + 7*16], xmm13
	vmovdqa	[rsp + 8*16], xmm14
	save_reg	r12,  9*16 + 0*8
	save_reg	r13,  9*16 + 1*8
	save_reg	r14,  9*16 + 2*8
	save_reg	r15,  9*16 + 3*8
	save_reg	rdi,  9*16 + 4*8
	save_reg	rsi,  9*16 + 5*8
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
	vmovdqa	xmm13, [rsp + 7*16]
	vmovdqa	xmm14, [rsp + 8*16]
	mov	r12,  [rsp + 9*16 + 0*8]
	mov	r13,  [rsp + 9*16 + 1*8]
	mov	r14,  [rsp + 9*16 + 2*8]
	mov	r15,  [rsp + 9*16 + 3*8]
	mov	rdi,  [rsp + 9*16 + 4*8]
	mov	rsi,  [rsp + 9*16 + 5*8]
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
%define pos    return


%ifndef EC_ALIGNED_ADDR
;;; Use Un-aligned load/store
 %define XLDR vmovdqu8
 %define XSTR vmovdqu8
%else
;;; Use Non-temporal load/stor
 %ifdef NO_NT_LDST
  %define XLDR vmovdqa
  %define XSTR vmovdqa
 %else
  %define XLDR vmovntdqa
  %define XSTR vmovntdq
 %endif
%endif

%define xmask0f   zmm14
%define xgft1_lo  zmm13
%define xgft1_loy ymm13
%define xgft1_hi  zmm12
%define xgft2_lo  zmm11
%define xgft2_loy ymm11
%define xgft2_hi  zmm10
%define xgft3_lo  zmm9
%define xgft3_loy ymm9
%define xgft3_hi  zmm8
%define xgft4_lo  zmm7
%define xgft4_loy ymm7
%define xgft4_hi  zmm6

%define x0        zmm0
%define xtmpa     zmm1
%define xp1       zmm2
%define xp2       zmm3
%define xp3       zmm4
%define xp4       zmm5

default rel
[bits 64]

section .text

align 16
global gf_4vect_dot_prod_avx512:ISAL_SYM_TYPE_FUNCTION
func(gf_4vect_dot_prod_avx512)
%ifidn __OUTPUT_FORMAT__, macho64
global _gf_4vect_dot_prod_avx512:ISAL_SYM_TYPE_FUNCTION
func(_gf_4vect_dot_prod_avx512)
%endif

	FUNC_SAVE
	sub	len, 64
	jl	.return_fail

	xor	pos, pos
	mov	tmp, 0x0f
	vpbroadcastb xmask0f, tmp	;Construct mask 0x0f0f0f...
	mov	vskip3, vec
	imul	vskip3, 96
	sal	vec, LOG_PS		;vec *= PS. Make vec_i count by PS
	mov	dest2, [dest1+PS]
	mov	dest3, [dest1+2*PS]
	mov	dest4, [dest1+3*PS]
	mov	dest1, [dest1]

.loop64:
	vpxorq	xp1, xp1, xp1
	vpxorq	xp2, xp2, xp2
	vpxorq	xp3, xp3, xp3
	vpxorq	xp4, xp4, xp4
	mov	tmp, mul_array
	xor	vec_i, vec_i

.next_vect:
	mov	ptr, [src+vec_i]
	XLDR	x0, [ptr+pos]		;Get next source vector
	add	vec_i, PS

	vpandq	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpandq	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	vmovdqu8 xgft1_loy, [tmp]		;Load array Ax{00}..{0f}, Ax{00}..{f0}
	vmovdqu8 xgft2_loy, [tmp+vec*(32/PS)]	;Load array Bx{00}..{0f}, Bx{00}..{f0}
	vmovdqu8 xgft3_loy, [tmp+vec*(64/PS)]	;Load array Cx{00}..{0f}, Cx{00}..{f0}
	vmovdqu8 xgft4_loy, [tmp+vskip3]	;Load array Dx{00}..{0f}, Dx{00}..{f0}
	add	tmp, 32

	vshufi64x2 xgft1_hi, xgft1_lo, xgft1_lo, 0x55
	vshufi64x2 xgft1_lo, xgft1_lo, xgft1_lo, 0x00
	vshufi64x2 xgft2_hi, xgft2_lo, xgft2_lo, 0x55
	vshufi64x2 xgft2_lo, xgft2_lo, xgft2_lo, 0x00

	vpshufb	xgft1_hi, xgft1_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft1_lo, xgft1_lo, xtmpa	;Lookup mul table of low nibble
	vpxorq	xgft1_hi, xgft1_hi, xgft1_lo	;GF add high and low partials
	vpxorq	xp1, xp1, xgft1_hi		;xp1 += partial

	vpshufb	xgft2_hi, xgft2_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft2_lo, xgft2_lo, xtmpa	;Lookup mul table of low nibble
	vpxorq	xgft2_hi, xgft2_hi, xgft2_lo	;GF add high and low partials
	vpxorq	xp2, xp2, xgft2_hi		;xp2 += partial

	vshufi64x2 xgft3_hi, xgft3_lo, xgft3_lo, 0x55
	vshufi64x2 xgft3_lo, xgft3_lo, xgft3_lo, 0x00
	vshufi64x2 xgft4_hi, xgft4_lo, xgft4_lo, 0x55
	vshufi64x2 xgft4_lo, xgft4_lo, xgft4_lo, 0x00

	vpshufb	xgft3_hi, xgft3_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft3_lo, xgft3_lo, xtmpa	;Lookup mul table of low nibble
	vpxorq	xgft3_hi, xgft3_hi, xgft3_lo	;GF add high and low partials
	vpxorq	xp3, xp3, xgft3_hi		;xp3 += partial

	vpshufb	xgft4_hi, xgft4_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft4_lo, xgft4_lo, xtmpa 	;Lookup mul table of low nibble
	vpxorq	xgft4_hi, xgft4_hi, xgft4_lo	;GF add high and low partials
	vpxorq	xp4, xp4, xgft4_hi		;xp4 += partial

	cmp	vec_i, vec
	jl	.next_vect

	XSTR	[dest1+pos], xp1
	XSTR	[dest2+pos], xp2
	XSTR	[dest3+pos], xp3
	XSTR	[dest4+pos], xp4

	add	pos, 64			;Loop on 64 bytes at a time
	cmp	pos, len
	jle	.loop64

	lea	tmp, [len + 64]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, len	;Overlapped offset length-64
	jmp	.loop64		;Do one more overlap pass

.return_pass:
	mov	return, 0
	FUNC_RESTORE
	ret

.return_fail:
	mov	return, 1
	FUNC_RESTORE
	ret

endproc_frame

%else
%ifidn __OUTPUT_FORMAT__, win64
global no_gf_4vect_dot_prod_avx512
no_gf_4vect_dot_prod_avx512:
%endif
%endif  ; ifdef HAVE_AS_KNOWS_AVX512
