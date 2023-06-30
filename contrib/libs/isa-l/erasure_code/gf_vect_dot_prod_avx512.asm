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
;;; gf_vect_dot_prod_avx512(len, vec, *g_tbls, **buffs, *dest);
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
 %define tmp2  r10
 %define return rax
 %define PS     8
 %define LOG_PS 3

 %define func(x) x:
 %define FUNC_SAVE
 %define FUNC_RESTORE
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
 %define return rax
 %define PS     8
 %define LOG_PS 3
 %define stack_size  0*16 + 3*8		; must be an odd multiple of 8
 %define arg(x)      [rsp + stack_size + PS + PS*x]

 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
	alloc_stack	stack_size
	save_reg	r12,  9*16 + 0*8
	save_reg	r15,  9*16 + 3*8
	end_prolog
	mov	arg4, arg(4)
 %endmacro

 %macro FUNC_RESTORE 0
	mov	r12,  [rsp + 9*16 + 0*8]
	mov	r15,  [rsp + 9*16 + 3*8]
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

%define xmask0f   zmm5
%define xgft1_lo  zmm4
%define xgft1_loy ymm4
%define xgft1_hi  zmm3
%define x0        zmm0
%define xgft1_loy ymm4
%define x0y       ymm0
%define xtmpa     zmm1
%define xp1       zmm2
%define xp1y      ymm2

default rel
[bits 64]
section .text

align 16
global gf_vect_dot_prod_avx512:ISAL_SYM_TYPE_FUNCTION
func(gf_vect_dot_prod_avx512)
%ifidn __OUTPUT_FORMAT__, macho64
global _gf_vect_dot_prod_avx512:ISAL_SYM_TYPE_FUNCTION
func(_gf_vect_dot_prod_avx512)
%endif

	FUNC_SAVE
	xor	pos, pos
	mov	tmp, 0x0f
	vpbroadcastb xmask0f, tmp	;Construct mask 0x0f0f0f...
	sal	vec, LOG_PS		;vec *= PS. Make vec_i count by PS
	sub	len, 64
	jl	.len_lt_64

.loop64:
	vpxorq	xp1, xp1, xp1
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
	add	tmp, 32

	vshufi64x2 xgft1_hi, xgft1_lo, xgft1_lo, 0x55
	vshufi64x2 xgft1_lo, xgft1_lo, xgft1_lo, 0x00

	vpshufb	xgft1_hi, xgft1_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft1_lo, xgft1_lo, xtmpa	;Lookup mul table of low nibble
	vpxorq	xgft1_hi, xgft1_hi, xgft1_lo	;GF add high and low partials
	vpxorq	xp1, xp1, xgft1_hi		;xp1 += partial

	cmp	vec_i, vec
	jl	.next_vect

	XSTR	[dest1+pos], xp1

	add	pos, 64			;Loop on 64 bytes at a time
	cmp	pos, len
	jle	.loop64

	lea	tmp, [len + 64]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, len	;Overlapped offset length-64
	jmp	.loop64		;Do one more overlap pass


.len_lt_64:			; 32-byte version
	add	len, 32
	jl	.return_fail

.loop32:
	vpxorq	xp1, xp1, xp1
	mov	tmp, mul_array
	xor	vec_i, vec_i

.next_vect2:
	mov	ptr, [src+vec_i]
	XLDR	x0y, [ptr+pos]		;Get next source vector 32B
	add	vec_i, PS
	vpsraw	xtmpa, x0, 4		;Shift to put high nibble into bits 4-0
	vshufi64x2 x0, x0, xtmpa, 0x44	;put x0 = xl:xh
	vpandq	x0, x0, xmask0f		;Mask bits 4-0
	vmovdqu8 xgft1_loy, [tmp]	;Load array Ax{00}..{0f}, Ax{00}..{f0}
	add	tmp, 32
	vshufi64x2 xgft1_lo, xgft1_lo, xgft1_lo, 0x50	;=AlAh:AlAh
	vpshufb	   xgft1_lo, xgft1_lo, x0		;Lookup mul table
	vshufi64x2 xgft1_hi, xgft1_lo, xgft1_lo, 0x0e	;=xh:
	vpxorq	xgft1_hi, xgft1_hi, xgft1_lo	;GF add high and low partials
	vpxorq	xp1, xp1, xgft1_hi		;xp1 += partial
	cmp	vec_i, vec
	jl	.next_vect2

	XSTR	[dest1+pos], xp1y
	add	pos, 32			;Loop on 32 bytes at a time
	cmp	pos, len
	jle	.loop32

	lea	tmp, [len + 32]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, len	;Overlapped offset length-32
	jmp	.loop32		;Do one more overlap pass

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
global no_gf_vect_dot_prod_avx512
no_gf_vect_dot_prod_avx512:
%endif
%endif  ; ifdef HAVE_AS_KNOWS_AVX512
