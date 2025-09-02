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
;;; gf_vect_mad_avx2_gfni(len, vec, vec_i, mul_array, src, dest);
;;;

%include "reg_sizes.asm"
%include "gf_vect_gfni.inc"
%include "memcpy.asm"

%if AS_FEATURE_LEVEL >= 10

%ifidn __OUTPUT_FORMAT__, elf64
 %define arg0   rdi
 %define arg1   rsi
 %define arg2   rdx
 %define arg3   rcx
 %define arg4   r8
 %define arg5   r9
 %define tmp    r11
 %define tmp2   r10
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
 %define tmp2   r10
 %define stack_size 16*3 + 3*8
 %define arg(x)      [rsp + stack_size + 8 + 8*x]
 %define func(x) proc_frame x

 %macro FUNC_SAVE 0
        sub	rsp, stack_size
        vmovdqa [rsp + 0*16], xmm6
        vmovdqa [rsp + 1*16], xmm7
        vmovdqa [rsp + 2*16], xmm8
        mov	[rsp + 3*16 + 0*8], r12
        mov	[rsp + 3*16 + 1*8], r13
        end_prolog
        mov	arg4, arg(4)
        mov	arg5, arg(5)
 %endmacro

 %macro FUNC_RESTORE 0
        vmovdqa xmm6, [rsp + 0*16]
        vmovdqa xmm7, [rsp + 1*16]
        vmovdqa xmm8, [rsp + 2*16]
        mov	r12,  [rsp + 3*16 + 0*8]
        mov	r13,  [rsp + 3*16 + 1*8]
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

%define x0l     ymm0
%define x0h     ymm1
%define x0x     ymm2

%define xgft1   ymm3

%define xd1l    ymm4
%define xd1h    ymm5
%define xd1x    ymm6

%define xret1l  ymm7
%define xret1h  ymm8

%define x0      x0l
%define xd1     xd1l
%define xret1   xret1l

;;
;; Encodes 96 bytes of a single source and updates a single parity disk
;;
%macro ENCODE_96B 0
        ;; get next source vector
        XLDR    x0l, [src + pos]
        XLDR    x0h, [src + pos + 32]
        XLDR    x0x, [src + pos + 64]
        ;; get next dest vector
        XLDR    xd1l, [dest1 + pos]
        XLDR    xd1h, [dest1 + pos + 32]
        XLDR    xd1x, [dest1 + pos + 64]

        GF_MUL_XOR VEX, x0l, xgft1, xret1l, xd1l
        GF_MUL_XOR VEX, x0h, xgft1, xret1h, xd1h
        GF_MUL_XOR VEX, x0x, xgft1, xret1l, xd1x

        XSTR    [dest1 + pos], xd1l
        XSTR    [dest1 + pos + 32], xd1h
        XSTR    [dest1 + pos + 64], xd1x
%endmacro

;;
;; Encodes 64 bytes of a single source and updates a single parity disk
;;
%macro ENCODE_64B 0
        ;; get next source vector
        XLDR    x0l, [src + pos]
        XLDR    x0h, [src + pos + 32]
        ;; get next dest vector
        XLDR    xd1l, [dest1 + pos]
        XLDR    xd1h, [dest1 + pos + 32]

        GF_MUL_XOR VEX, x0l, xgft1, xret1l, xd1l
        GF_MUL_XOR VEX, x0h, xgft1, xret1h, xd1h

        XSTR    [dest1 + pos], xd1l
        XSTR    [dest1 + pos + 32], xd1h
%endmacro

;;
;; Encodes 32 bytes of a single source and updates single parity disk
;;
%macro ENCODE_32B 0
        ;; get next source vector
        XLDR    x0, [src + pos]
        ;; get next dest vector
        XLDR    xd1, [dest1 + pos]

        GF_MUL_XOR VEX, x0, xgft1, xret1, xd1

        XSTR    [dest1 + pos], xd1
%endmacro

;;
;; Encodes less than 32 bytes of a single source and updates parity disk
;;
%macro ENCODE_LT_32B 1
%define %%LEN   %1
        ;; get next source vector
        simd_load_avx2 x0, src + pos, %%LEN, tmp, tmp2
        ;; get next dest vector
        simd_load_avx2 xd1, dest1 + pos, %%LEN, tmp, tmp2

        GF_MUL_XOR VEX, x0, xgft1, xret1, xd1

        lea     dest1, [dest1 + pos]
        simd_store_avx2 dest1, xd1, %%LEN, tmp, tmp2
%endmacro

align 16
global gf_vect_mad_avx2_gfni, function
func(gf_vect_mad_avx2_gfni)
        FUNC_SAVE

        xor     pos, pos
        shl     vec_i, 3                ;Multiply by 8
        shl     vec, 3                  ;Multiply by 8
        lea     tmp, [mul_array + vec_i]
        vbroadcastsd xgft1, [tmp]

        cmp     len, 96
        jl      .len_lt_96

.loop96:
        ENCODE_96B              ;; loop on 96 bytes at a time
        add     pos, 96
        sub     len, 96
        cmp     len, 96
        jge     .loop96

.len_lt_96:
        cmp     len, 64
        jl      .len_lt_64

        ENCODE_64B              ;; encode next 64 bytes

        add     pos, 64
        sub     len, 64

.len_lt_64:
        cmp     len, 32
        jl      .len_lt_32

        ENCODE_32B              ;; encode next 32 bytes

        add     pos, 32
        sub     len, 32

.len_lt_32:
        cmp     len, 0
        jle     .exit

        ENCODE_LT_32B len     ;; encode final bytes

.exit:
        vzeroupper

        FUNC_RESTORE
        ret

endproc_frame
%endif  ; if AS_FEATURE_LEVEL >= 10
