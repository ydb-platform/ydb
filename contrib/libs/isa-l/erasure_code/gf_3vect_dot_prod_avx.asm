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
;;; gf_3vect_dot_prod_avx(len, vec, *g_tbls, **buffs, **dests);
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
 %define return rax
 %macro  SLDR 2
 %endmacro
 %define SSTR SLDR
 %define PS 8
 %define LOG_PS 3

 %define func(x) x: endbranch
 %macro FUNC_SAVE 0
	push	r12
	push	r13
 %endmacro
 %macro FUNC_RESTORE 0
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
 %define return rax
 %macro  SLDR 2
 %endmacro
 %define SSTR SLDR
 %define PS     8
 %define LOG_PS 3
 %define stack_size  6*16 + 5*8 	; must be an odd multiple of 8
 %define arg(x)      [rsp + stack_size + PS + PS*x]

 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
	alloc_stack	stack_size
	vmovdqa		[rsp + 0*16], xmm6
	vmovdqa		[rsp + 1*16], xmm7
	vmovdqa		[rsp + 2*16], xmm8
	vmovdqa		[rsp + 3*16], xmm9
	vmovdqa		[rsp + 4*16], xmm10
	vmovdqa		[rsp + 5*16], xmm11
	save_reg	r12,  6*16 + 0*8
	save_reg	r13,  6*16 + 1*8
	save_reg	r14,  6*16 + 2*8
	save_reg	r15,  6*16 + 3*8
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
	mov	r12,  [rsp + 6*16 + 0*8]
	mov	r13,  [rsp + 6*16 + 1*8]
	mov	r14,  [rsp + 6*16 + 2*8]
	mov	r15,  [rsp + 6*16 + 3*8]
	add	rsp, stack_size
 %endmacro
%endif

%ifidn __OUTPUT_FORMAT__, elf32

;;;================== High Address;
;;;	arg4
;;;	arg3
;;;	arg2
;;;	arg1
;;;	arg0
;;;	return
;;;<================= esp of caller
;;;	ebp
;;;<================= ebp = esp
;;;	var0
;;;	var1
;;;	esi
;;;	edi
;;;	ebx
;;;<================= esp of callee
;;;
;;;================== Low Address;

 %define PS 4
 %define LOG_PS 2
 %define func(x) x: endbranch
 %define arg(x) [ebp + PS*2 + PS*x]
 %define var(x) [ebp - PS - PS*x]

 %define trans   ecx
 %define trans2  esi
 %define arg0    trans		;trans and trans2 are for the variables in stack
 %define arg0_m  arg(0)
 %define arg1    ebx
 %define arg2    arg2_m
 %define arg2_m  arg(2)
 %define arg3    trans
 %define arg3_m  arg(3)
 %define arg4    trans
 %define arg4_m  arg(4)
 %define arg5	 trans2
 %define tmp	 edx
 %define tmp2    edi
 %define tmp3    trans2
 %define tmp3_m  var(0)
 %define tmp4    trans2
 %define tmp4_m  var(1)
 %define return  eax
 %macro SLDR 2	;; stack load/restore
	mov %1, %2
 %endmacro
 %define SSTR SLDR

 %macro FUNC_SAVE 0
	push	ebp
	mov	ebp, esp
	sub	esp, PS*2		;2 local variables
	push	esi
	push	edi
	push	ebx
	mov	arg1, arg(1)
 %endmacro

 %macro FUNC_RESTORE 0
	pop	ebx
	pop	edi
	pop	esi
	add	esp, PS*2		;2 local variables
	pop	ebp
 %endmacro

%endif	; output formats

%define len   arg0
%define vec   arg1
%define mul_array arg2
%define	src   arg3
%define dest1  arg4
%define ptr   arg5

%define vec_i tmp2
%define dest2 tmp3
%define dest3 tmp4
%define pos   return

 %ifidn PS,4				;32-bit code
	%define  len_m 	arg0_m
	%define  src_m 	arg3_m
	%define  dest1_m arg4_m
	%define  dest2_m tmp3_m
	%define  dest3_m tmp4_m
 %endif

%ifndef EC_ALIGNED_ADDR
;;; Use Un-aligned load/store
 %define XLDR vmovdqu
 %define XSTR vmovdqu
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

%ifidn PS,8			; 64-bit code
 default rel
  [bits 64]
%endif


section .text

%ifidn PS,8			;64-bit code
 %define xmask0f   xmm11
 %define xgft1_lo  xmm10
 %define xgft1_hi  xmm9
 %define xgft2_lo  xmm8
 %define xgft2_hi  xmm7
 %define xgft3_lo  xmm6
 %define xgft3_hi  xmm5

 %define x0     xmm0
 %define xtmpa  xmm1
 %define xp1    xmm2
 %define xp2    xmm3
 %define xp3    xmm4
%else
 %define xmask0f   xmm7
 %define xgft1_lo  xmm6
 %define xgft1_hi  xmm5
 %define xgft2_lo  xgft1_lo
 %define xgft2_hi  xgft1_hi
 %define xgft3_lo  xgft1_lo
 %define xgft3_hi  xgft1_hi

 %define x0     xmm0
 %define xtmpa  xmm1
 %define xp1    xmm2
 %define xp2    xmm3
 %define xp3    xmm4
%endif

align 16
global gf_3vect_dot_prod_avx, function
func(gf_3vect_dot_prod_avx)
	FUNC_SAVE
	SLDR	len, len_m
	sub	len, 16
	SSTR	len_m, len
	jl	.return_fail
	xor	pos, pos
	vmovdqa	xmask0f, [mask0f]	;Load mask of lower nibble in each byte
	sal	vec, LOG_PS		;vec *= PS. Make vec_i count by PS
	SLDR	dest1, dest1_m
	mov	dest2, [dest1+PS]
	SSTR	dest2_m, dest2
	mov	dest3, [dest1+2*PS]
	SSTR	dest3_m, dest3
	mov	dest1, [dest1]
	SSTR	dest1_m, dest1

.loop16:
	vpxor	xp1, xp1
	vpxor	xp2, xp2
	vpxor	xp3, xp3
	mov	tmp, mul_array
	xor	vec_i, vec_i

.next_vect:
	SLDR	src, src_m
	mov	ptr, [src+vec_i]

	vmovdqu	xgft1_lo, [tmp]		;Load array Ax{00}, Ax{01}, ..., Ax{0f}
	vmovdqu	xgft1_hi, [tmp+16]	;     "     Ax{00}, Ax{10}, ..., Ax{f0}
 %ifidn PS,8				; 64-bit code
	vmovdqu	xgft2_lo, [tmp+vec*(32/PS)]	;Load array Bx{00}, Bx{01}, ..., Bx{0f}
	vmovdqu	xgft2_hi, [tmp+vec*(32/PS)+16]	;     "     Bx{00}, Bx{10}, ..., Bx{f0}
	vmovdqu	xgft3_lo, [tmp+vec*(64/PS)]	;Load array Cx{00}, Cx{01}, ..., Cx{0f}
	vmovdqu	xgft3_hi, [tmp+vec*(64/PS)+16]	;     "     Cx{00}, Cx{10}, ..., Cx{f0}
	add	tmp, 32
	add	vec_i, PS
 %endif
	XLDR	x0, [ptr+pos]		;Get next source vector

	vpand	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	vpshufb	xgft1_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft1_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft1_hi, xgft1_lo	;GF add high and low partials
	vpxor	xp1, xgft1_hi		;xp1 += partial

 %ifidn PS,4				; 32-bit code
	vmovdqu	xgft2_lo, [tmp+vec*(32/PS)]	;Load array Bx{00}, Bx{01}, ..., Bx{0f}
	vmovdqu	xgft2_hi, [tmp+vec*(32/PS)+16]	;     "     Bx{00}, Bx{10}, ..., Bx{f0}
 %endif
	vpshufb	xgft2_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft2_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft2_hi, xgft2_lo	;GF add high and low partials
	vpxor	xp2, xgft2_hi		;xp2 += partial

 %ifidn PS,4				; 32-bit code
	sal	vec, 1
	vmovdqu	xgft3_lo, [tmp+vec*(32/PS)]	;Load array Cx{00}, Cx{01}, ..., Cx{0f}
	vmovdqu	xgft3_hi, [tmp+vec*(32/PS)+16]	;     "     Cx{00}, Cx{10}, ..., Cx{f0}
	sar	vec, 1
	add	tmp, 32
	add	vec_i, PS
 %endif
	vpshufb	xgft3_hi, x0		;Lookup mul table of high nibble
	vpshufb	xgft3_lo, xtmpa		;Lookup mul table of low nibble
	vpxor	xgft3_hi, xgft3_lo	;GF add high and low partials
	vpxor	xp3, xgft3_hi		;xp3 += partial

	cmp	vec_i, vec
	jl	.next_vect

	SLDR	dest1, dest1_m
	SLDR	dest2, dest2_m
	XSTR	[dest1+pos], xp1
	XSTR	[dest2+pos], xp2
	SLDR	dest3, dest3_m
	XSTR	[dest3+pos], xp3

	SLDR	len, len_m
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
	mov	return, 0
	FUNC_RESTORE
	ret

.return_fail:
	mov	return, 1
	FUNC_RESTORE
	ret

endproc_frame

section .data

align 16
mask0f: dq 0x0f0f0f0f0f0f0f0f, 0x0f0f0f0f0f0f0f0f
