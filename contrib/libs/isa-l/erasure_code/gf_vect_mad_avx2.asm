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
;;; gf_vect_mad_avx2(len, vec, vec_i, mul_array, src, dest);
;;;

%include "reg_sizes.asm"

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg0.w ecx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9
 %define arg4   r12 		; must be saved and loaded
 %define arg5   r15

 %define tmp    r11
 %define tmp.w  r11d
 %define tmp.b  r11b
 %define return rax
 %define return.w eax
 %define PS 8
 %define stack_size 16*3 + 3*8
 %define arg(x)      [rsp + stack_size + PS + PS*x]
 %define func(x) proc_frame x

 %macro FUNC_SAVE 0
	sub	rsp, stack_size
	vmovdqa	[rsp+16*0],xmm6
	vmovdqa	[rsp+16*1],xmm7
	vmovdqa	[rsp+16*2],xmm8
	save_reg	r12,  3*16 + 0*8
	save_reg	r15,  3*16 + 1*8
	end_prolog
	mov	arg4, arg(4)
	mov	arg5, arg(5)
 %endmacro

 %macro FUNC_RESTORE 0
	vmovdqa	xmm6, [rsp+16*0]
	vmovdqa	xmm7, [rsp+16*1]
	vmovdqa	xmm8, [rsp+16*2]
	mov	r12,  [rsp + 3*16 + 0*8]
	mov	r15,  [rsp + 3*16 + 1*8]
	add	rsp, stack_size
 %endmacro

%elifidn __OUTPUT_FORMAT__, elf64
 %define arg0  rdi
 %define arg0.w edi
 %define arg1  rsi
 %define arg2  rdx
 %define arg3  rcx
 %define arg4  r8
 %define arg5  r9

 %define tmp      r11
 %define tmp.w    r11d
 %define tmp.b    r11b
 %define return rax
 %define return.w eax

 %define func(x) x: endbranch
 %define FUNC_SAVE
 %define FUNC_RESTORE
%endif


;;; gf_vect_mad_avx2(len, vec, vec_i, mul_array, src, dest)
%define len   arg0
%define len.w arg0.w
%define vec   arg1
%define vec_i    arg2
%define mul_array arg3
%define	src   arg4
%define dest  arg5
%define pos   return
%define pos.w return.w

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


default rel

[bits 64]
section .text

%define xmask0f  ymm8
%define xmask0fx xmm8
%define xgft_lo  ymm7
%define xgft_hi  ymm6

%define x0     ymm0
%define xtmpa  ymm1
%define xtmph  ymm2
%define xtmpl  ymm3
%define xd     ymm4
%define xtmpd  ymm5

align 16
global gf_vect_mad_avx2, function
func(gf_vect_mad_avx2)
	FUNC_SAVE
	sub	len, 32
	jl	.return_fail
	xor	pos, pos
	mov	tmp.b, 0x0f
	vpinsrb	xmask0fx, xmask0fx, tmp.w, 0
	vpbroadcastb xmask0f, xmask0fx	;Construct mask 0x0f0f0f...

	sal	vec_i, 5		;Multiply by 32
	vmovdqu	xgft_lo, [vec_i+mul_array]	;Load array Cx{00}, Cx{01}, Cx{02}, ...
						; " Cx{00}, Cx{10}, Cx{20}, ... , Cx{f0}
	vperm2i128 xgft_hi, xgft_lo, xgft_lo, 0x11 ; swapped to hi | hi
	vperm2i128 xgft_lo, xgft_lo, xgft_lo, 0x00 ; swapped to lo | lo

	XLDR	xtmpd, [dest+len]	;backup the last 32 bytes in dest

.loop32:
	XLDR	xd, [dest+pos]		;Get next dest vector
.loop32_overlap:
	XLDR	x0, [src+pos]		;Get next source vector

	vpand	xtmpa, x0, xmask0f	;Mask low src nibble in bits 4-0
	vpsraw	x0, x0, 4		;Shift to put high nibble into bits 4-0
	vpand	x0, x0, xmask0f		;Mask high src nibble in bits 4-0

	vpshufb	xtmph, xgft_hi, x0	;Lookup mul table of high nibble
	vpshufb	xtmpl, xgft_lo, xtmpa	;Lookup mul table of low nibble
	vpxor	xtmph, xtmph, xtmpl ;GF add high and low partials
	vpxor	xd, xd, xtmph		;xd += partial

	XSTR	[dest+pos], xd
	add	pos, 32			;Loop on 32 bytes at a time
	cmp	pos, len
	jle	.loop32

	lea	tmp, [len + 32]
	cmp	pos, tmp
	je	.return_pass

	;; Tail len
	mov	pos, len	;Overlapped offset length-32
	vmovdqa	xd, xtmpd	;Restore xd
	jmp	.loop32_overlap	;Do one more overlap pass

.return_pass:
	mov	return, 0
	FUNC_RESTORE
	ret

.return_fail:
	mov	return, 1
	FUNC_RESTORE
	ret

endproc_frame
