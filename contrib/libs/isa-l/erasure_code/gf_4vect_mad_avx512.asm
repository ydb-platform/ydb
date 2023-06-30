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
;;; gf_4vect_mad_avx512(len, vec, vec_i, mul_array, src, dest);
;;;

%include "reg_sizes.asm"

%ifdef HAVE_AS_KNOWS_AVX512

%ifidn __OUTPUT_FORMAT__, elf64
 %define arg0   rdi
 %define arg1   rsi
 %define arg2   rdx
 %define arg3   rcx
 %define arg4   r8
 %define arg5   r9
 %define tmp    r11
 %define return rax
 %define func(x) x:
 %define FUNC_SAVE
 %define FUNC_RESTORE
%endif

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9
 %define arg4   r12
 %define arg5   r15
 %define tmp    r11
 %define return rax
 %define stack_size 16*10 + 3*8
 %define arg(x)      [rsp + stack_size + PS + PS*x]
 %define func(x) proc_frame x

%macro FUNC_SAVE 0
	sub	rsp, stack_size
	movdqa	[rsp+16*0],xmm6
	movdqa	[rsp+16*1],xmm7
	movdqa	[rsp+16*2],xmm8
	movdqa	[rsp+16*3],xmm9
	movdqa	[rsp+16*4],xmm10
	movdqa	[rsp+16*5],xmm11
	movdqa	[rsp+16*6],xmm12
	movdqa	[rsp+16*7],xmm13
	movdqa	[rsp+16*8],xmm14
	movdqa	[rsp+16*9],xmm15
	save_reg	r12,  10*16 + 0*8
	save_reg	r15,  10*16 + 1*8
	end_prolog
	mov	arg4, arg(4)
	mov	arg5, arg(5)
%endmacro

%macro FUNC_RESTORE 0
	movdqa	xmm6, [rsp+16*0]
	movdqa	xmm7, [rsp+16*1]
	movdqa	xmm8, [rsp+16*2]
	movdqa	xmm9, [rsp+16*3]
	movdqa	xmm10, [rsp+16*4]
	movdqa	xmm11, [rsp+16*5]
	movdqa	xmm12, [rsp+16*6]
	movdqa	xmm13, [rsp+16*7]
	movdqa	xmm14, [rsp+16*8]
	movdqa	xmm15, [rsp+16*9]
	mov	r12,  [rsp + 10*16 + 0*8]
	mov	r15,  [rsp + 10*16 + 1*8]
	add	rsp, stack_size
%endmacro
%endif

%define PS    8
%define len   arg0
%define vec   arg1
%define vec_i arg2
%define mul_array arg3
%define	src   arg4
%define dest1 arg5
%define pos   return
%define dest2 mul_array
%define dest3 vec
%define dest4 vec_i

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

default rel
[bits 64]
section .text

%define x0        zmm0
%define xtmpa     zmm1
%define xtmpl1    zmm2
%define xtmph1    zmm3
%define xtmph2    zmm4
%define xtmph3    zmm5
%define xtmph4    zmm6
%define xgft1_hi  zmm7
%define xgft1_lo  zmm8
%define xgft1_loy ymm8
%define xgft2_hi  zmm9
%define xgft2_lo  zmm10
%define xgft2_loy ymm10
%define xgft3_hi  zmm11
%define xgft3_lo  zmm12
%define xgft3_loy ymm12
%define xgft4_hi  zmm13
%define xgft4_lo  zmm14
%define xgft4_loy ymm14
%define xd1       zmm15
%define xd2       zmm16
%define xd3       zmm17
%define xd4       zmm18
%define xmask0f   zmm19
%define xtmpl2    zmm20
%define xtmpl3    zmm21
%define xtmpl4    zmm22
%define xtmpl5    zmm23

align 16
global gf_4vect_mad_avx512:ISAL_SYM_TYPE_FUNCTION
func(gf_4vect_mad_avx512)
%ifidn __OUTPUT_FORMAT__, macho64
global _gf_4vect_mad_avx512:ISAL_SYM_TYPE_FUNCTION
func(_gf_4vect_mad_avx512)
%endif

	FUNC_SAVE
	sub	len, 64
	jl	.return_fail
	xor	pos, pos
	mov	tmp, 0x0f
	vpbroadcastb xmask0f, tmp	;Construct mask 0x0f0f0f...
	sal	vec_i, 5		;Multiply by 32
	sal	vec, 5			;Multiply by 32
	lea	tmp, [mul_array + vec_i]
	vmovdqu	xgft1_loy, [tmp]	;Load array Ax{00}..{0f}, Ax{00}..{f0}
	vmovdqu	xgft2_loy, [tmp+vec]	;Load array Bx{00}..{0f}, Bx{00}..{f0}
	vmovdqu	xgft3_loy, [tmp+2*vec]	;Load array Cx{00}..{0f}, Cx{00}..{f0}
	add	tmp, vec
	vmovdqu	xgft4_loy, [tmp+2*vec]	;Load array Dx{00}..{0f}, Dx{00}..{f0}
	vshufi64x2 xgft1_hi, xgft1_lo, xgft1_lo, 0x55
	vshufi64x2 xgft1_lo, xgft1_lo, xgft1_lo, 0x00
	vshufi64x2 xgft2_hi, xgft2_lo, xgft2_lo, 0x55
	vshufi64x2 xgft2_lo, xgft2_lo, xgft2_lo, 0x00
	vshufi64x2 xgft3_hi, xgft3_lo, xgft3_lo, 0x55
	vshufi64x2 xgft3_lo, xgft3_lo, xgft3_lo, 0x00
	vshufi64x2 xgft4_hi, xgft4_lo, xgft4_lo, 0x55
	vshufi64x2 xgft4_lo, xgft4_lo, xgft4_lo, 0x00
	mov	dest2, [dest1+PS]		; reuse mul_array
	mov	dest3, [dest1+2*PS]		; reuse vec
	mov	dest4, [dest1+3*PS]		; reuse vec_i
	mov	dest1, [dest1]
	mov	tmp, -1
	kmovq	k1, tmp

.loop64:
	XLDR	x0, [src+pos]		;Get next source vector
	XLDR	xd1, [dest1+pos]	;Get next dest vector
	XLDR	xd2, [dest2+pos]	;Get next dest vector
	XLDR	xd3, [dest3+pos]	;Get next dest vector
	XLDR	xd4, [dest4+pos]	;reuse xtmpl1. Get next dest vector

	vpandq	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpandq	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	; dest1
	vpshufb	xtmph1 {k1}{z}, xgft1_hi, x0	;Lookup mul table of high nibble
	vpshufb	xtmpl1 {k1}{z}, xgft1_lo, xtmpa	;Lookup mul table of low nibble
	vpxorq	xtmph1, xtmph1, xtmpl1		;GF add high and low partials
	vpxorq	xd1, xd1, xtmph1		;xd1 += partial

	; dest2
	vpshufb	xtmph2 {k1}{z}, xgft2_hi, x0	;Lookup mul table of high nibble
	vpshufb	xtmpl2 {k1}{z}, xgft2_lo, xtmpa	;Lookup mul table of low nibble
	vpxorq	xtmph2, xtmph2, xtmpl2		;GF add high and low partials
	vpxorq	xd2, xd2, xtmph2		;xd2 += partial

	; dest3
	vpshufb	xtmph3 {k1}{z}, xgft3_hi, x0	;Lookup mul table of high nibble
	vpshufb	xtmpl3 {k1}{z}, xgft3_lo, xtmpa	;Lookup mul table of low nibble
	vpxorq	xtmph3, xtmph3, xtmpl3		;GF add high and low partials
	vpxorq	xd3, xd3, xtmph3		;xd2 += partial

	; dest4
	vpshufb	xtmph4 {k1}{z}, xgft4_hi, x0	;Lookup mul table of high nibble
	vpshufb	xtmpl4 {k1}{z}, xgft4_lo, xtmpa	;Lookup mul table of low nibble
	vpxorq	xtmph4, xtmph4, xtmpl4		;GF add high and low partials
	vpxorq	xd4, xd4, xtmph4		;xd2 += partial

	XSTR	[dest1+pos], xd1
	XSTR	[dest2+pos], xd2
	XSTR	[dest3+pos], xd3
	XSTR	[dest4+pos], xd4

	add	pos, 64			;Loop on 64 bytes at a time
	cmp	pos, len
	jle	.loop64

	lea	tmp, [len + 64]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, (1 << 63)
	lea	tmp, [len + 64 - 1]
	and	tmp, 63
	sarx	pos, pos, tmp
	kmovq	k1, pos
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
global no_gf_4vect_mad_avx512
no_gf_4vect_mad_avx512:
%endif
%endif  ; ifdef HAVE_AS_KNOWS_AVX512
