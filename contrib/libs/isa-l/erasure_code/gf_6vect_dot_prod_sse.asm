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
;;; gf_6vect_dot_prod_sse(len, vec, *g_tbls, **buffs, **dests);
;;;

%include "reg_sizes.asm"

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
 %define return rax
 %define PS 8
 %define LOG_PS 3

 %define func(x) x: endbranch
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
 %define tmp2   r10
 %define tmp3   r13		; must be saved and restored
 %define tmp4   r14		; must be saved and restored
 %define tmp5   rdi		; must be saved and restored
 %define tmp6   rsi		; must be saved and restored
 %define return rax
 %define PS     8
 %define LOG_PS 3
 %define stack_size  10*16 + 7*8		; must be an odd multiple of 8
 %define arg(x)      [rsp + stack_size + PS + PS*x]

 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
	alloc_stack	stack_size
	save_xmm128	xmm6, 0*16
	save_xmm128	xmm7, 1*16
	save_xmm128	xmm8, 2*16
	save_xmm128	xmm9, 3*16
	save_xmm128	xmm10, 4*16
	save_xmm128	xmm11, 5*16
	save_xmm128	xmm12, 6*16
	save_xmm128	xmm13, 7*16
	save_xmm128	xmm14, 8*16
	save_xmm128	xmm15, 9*16
	save_reg	r12,  10*16 + 0*8
	save_reg	r13,  10*16 + 1*8
	save_reg	r14,  10*16 + 2*8
	save_reg	r15,  10*16 + 3*8
	save_reg	rdi,  10*16 + 4*8
	save_reg	rsi,  10*16 + 5*8
	end_prolog
	mov	arg4, arg(4)
 %endmacro

 %macro FUNC_RESTORE 0
	movdqa	xmm6, [rsp + 0*16]
	movdqa	xmm7, [rsp + 1*16]
	movdqa	xmm8, [rsp + 2*16]
	movdqa	xmm9, [rsp + 3*16]
	movdqa	xmm10, [rsp + 4*16]
	movdqa	xmm11, [rsp + 5*16]
	movdqa	xmm12, [rsp + 6*16]
	movdqa	xmm13, [rsp + 7*16]
	movdqa	xmm14, [rsp + 8*16]
	movdqa	xmm15, [rsp + 9*16]
	mov	r12,  [rsp + 10*16 + 0*8]
	mov	r13,  [rsp + 10*16 + 1*8]
	mov	r14,  [rsp + 10*16 + 2*8]
	mov	r15,  [rsp + 10*16 + 3*8]
	mov	rdi,  [rsp + 10*16 + 4*8]
	mov	rsi,  [rsp + 10*16 + 5*8]
	add	rsp, stack_size
 %endmacro
%endif

%define len    arg0
%define vec    arg1
%define mul_array arg2
%define	src    arg3
%define dest   arg4
%define ptr    arg5
%define vec_i  tmp2
%define dest1  tmp3
%define dest2  tmp4
%define vskip1 tmp5
%define vskip3 tmp6
%define pos    return


%ifndef EC_ALIGNED_ADDR
;;; Use Un-aligned load/store
 %define XLDR movdqu
 %define XSTR movdqu
%else
;;; Use Non-temporal load/stor
 %ifdef NO_NT_LDST
  %define XLDR movdqa
  %define XSTR movdqa
 %else
  %define XLDR movntdqa
  %define XSTR movntdq
 %endif
%endif


default rel

[bits 64]
section .text

%define xmask0f   xmm15
%define xgft1_lo  xmm2
%define xgft1_hi  xmm3
%define xgft2_lo  xmm4
%define xgft2_hi  xmm5
%define xgft3_lo  xmm6
%define xgft3_hi  xmm7
%define x0     xmm0
%define xtmpa  xmm1
%define xp1    xmm8
%define xp2    xmm9
%define xp3    xmm10
%define xp4    xmm11
%define xp5    xmm12
%define xp6    xmm13

align 16
global gf_6vect_dot_prod_sse, function
func(gf_6vect_dot_prod_sse)
	FUNC_SAVE
	sub	len, 16
	jl	.return_fail
	xor	pos, pos
	movdqa	xmask0f, [mask0f]	;Load mask of lower nibble in each byte
	mov	vskip1, vec
	imul	vskip1, 32
	mov	vskip3, vec
	imul	vskip3, 96
	sal	vec, LOG_PS		;vec *= PS. Make vec_i count by PS
	mov	dest1, [dest]
	mov	dest2, [dest+PS]


.loop16:
	mov	tmp, mul_array
	xor	vec_i, vec_i
	pxor	xp1, xp1
	pxor	xp2, xp2
	pxor	xp3, xp3
	pxor	xp4, xp4
	pxor	xp5, xp5
	pxor	xp6, xp6

.next_vect:
	mov	ptr, [src+vec_i]
	add	vec_i, PS
	XLDR	x0, [ptr+pos]		;Get next source vector

	movdqu	xgft1_lo, [tmp]			;Load array Ax{00}, Ax{01}, ..., Ax{0f}
	movdqu	xgft1_hi, [tmp+16]		;     "     Ax{00}, Ax{10}, ..., Ax{f0}
	movdqu	xgft2_lo, [tmp+vskip1*1]	;Load array Bx{00}, Bx{01}, ..., Bx{0f}
	movdqu	xgft2_hi, [tmp+vskip1*1+16]	;     "     Bx{00}, Bx{10}, ..., Bx{f0}
	movdqu	xgft3_lo, [tmp+vskip1*2]	;Load array Cx{00}, Cx{01}, ..., Cx{0f}
	movdqu	xgft3_hi, [tmp+vskip1*2+16]	;     "     Cx{00}, Cx{10}, ..., Cx{f0}
	lea	ptr, [vskip1 + vskip1*4]	;ptr = vskip5

	movdqa	xtmpa, x0		;Keep unshifted copy of src
	psraw	x0, 4			;Shift to put high nibble into bits 4-0
	pand	x0, xmask0f		;Mask high src nibble in bits 4-0
	pand	xtmpa, xmask0f		;Mask low src nibble in bits 4-0

	pshufb	xgft1_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft1_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft1_hi, xgft1_lo	;GF add high and low partials
	pxor	xp1, xgft1_hi		;xp1 += partial

	pshufb	xgft2_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft2_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft2_hi, xgft2_lo	;GF add high and low partials
	pxor	xp2, xgft2_hi		;xp2 += partial

	pshufb	xgft3_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft3_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft3_hi, xgft3_lo	;GF add high and low partials
	pxor	xp3, xgft3_hi		;xp3 += partial


	movdqu	xgft1_lo, [tmp+vskip3]		;Load array Dx{00}, Dx{01}, ..., Dx{0f}
	movdqu	xgft1_hi, [tmp+vskip3+16]	;     "     Dx{00}, Dx{10}, ..., Dx{f0}
	movdqu	xgft2_lo, [tmp+vskip1*4]	;Load array Ex{00}, Ex{01}, ..., Ex{0f}
	movdqu	xgft2_hi, [tmp+vskip1*4+16]	;     "     Ex{00}, Ex{10}, ..., Ex{f0}
	movdqu	xgft3_lo, [tmp+ptr]		;Load array Fx{00}, Fx{01}, ..., Fx{0f}
	movdqu	xgft3_hi, [tmp+ptr+16]		;     "     Fx{00}, Fx{10}, ..., Fx{f0}
	add	tmp, 32


	pshufb	xgft1_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft1_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft1_hi, xgft1_lo	;GF add high and low partials
	pxor	xp4, xgft1_hi		;xp4 += partial

	pshufb	xgft2_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft2_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft2_hi, xgft2_lo	;GF add high and low partials
	pxor	xp5, xgft2_hi		;xp5 += partial

	pshufb	xgft3_hi, x0		;Lookup mul table of high nibble
	pshufb	xgft3_lo, xtmpa		;Lookup mul table of low nibble
	pxor	xgft3_hi, xgft3_lo	;GF add high and low partials
	pxor	xp6, xgft3_hi		;xp6 += partial

	cmp	vec_i, vec
	jl	.next_vect


	mov	tmp, [dest+2*PS]
	mov	ptr, [dest+3*PS]
	mov	vec_i, [dest+4*PS]

	XSTR	[dest1+pos], xp1
	XSTR	[dest2+pos], xp2
	XSTR	[tmp+pos], xp3
	mov	tmp, [dest+5*PS]
	XSTR	[ptr+pos], xp4
	XSTR	[vec_i+pos], xp5
	XSTR	[tmp+pos], xp6

	add	pos, 16			;Loop on 16 bytes at a time
	cmp	pos, len
	jle	.loop16

	lea	tmp, [len + 16]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, len	;Overlapped offset length-16
	jmp	.loop16		;Do one more overlap pass

.return_pass:
	FUNC_RESTORE
	mov	return, 0
	ret

.return_fail:
	FUNC_RESTORE
	mov	return, 1
	ret

endproc_frame

section .data

align 16
mask0f:	dq 0x0f0f0f0f0f0f0f0f, 0x0f0f0f0f0f0f0f0f
