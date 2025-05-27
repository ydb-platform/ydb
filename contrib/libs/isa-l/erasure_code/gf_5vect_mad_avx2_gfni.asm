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
;;; gf_5vect_mad_avx2_gfni(len, vec, vec_i, mul_array, src, dest);
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
 %define tmp3   r12
 %define tmp4   r13
 %define func(x) x: endbranch
 %define stack_size  2*8
 %macro FUNC_SAVE 0
        sub     rsp, stack_size
        mov     [rsp + 0*8], r12
        mov     [rsp + 1*8], r13
 %endmacro
 %macro FUNC_RESTORE 0
        mov     r12, [rsp + 0*8]
        mov     r13, [rsp + 1*8]
        add     rsp, stack_size
 %endmacro
%endif

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9
 %define arg4   r12     ; must be saved, loaded and restored
 %define arg5   r13     ; must be saved and restored
 %define tmp    r11
 %define tmp2   r10
 %define tmp3   r14
 %define tmp4   r15
 %define stack_size 16*10 + 5*8
 %define arg(x) [rsp + stack_size + 8 + 8*x]
 %define func(x) proc_frame x

 %macro FUNC_SAVE 0
        sub     rsp, stack_size
        vmovdqa [rsp + 0*16], xmm6
        vmovdqa [rsp + 1*16], xmm7
        vmovdqa [rsp + 2*16], xmm8
        vmovdqa [rsp + 3*16], xmm9
        vmovdqa [rsp + 4*16], xmm10
        vmovdqa [rsp + 5*16], xmm11
        vmovdqa [rsp + 6*16], xmm12
        vmovdqa [rsp + 7*16], xmm13
        vmovdqa [rsp + 8*16], xmm14
        vmovdqa [rsp + 9*16], xmm15
        mov     [rsp + 10*16 + 0*8], r12
        mov     [rsp + 10*16 + 1*8], r13
        mov     [rsp + 10*16 + 2*8], r14
        mov     [rsp + 10*16 + 3*8], r15
        end_prolog
        mov     arg4, arg(4)
        mov     arg5, arg(5)
 %endmacro

 %macro FUNC_RESTORE 0
        vmovdqa xmm6, [rsp + 0*16]
        vmovdqa xmm7, [rsp + 1*16]
        vmovdqa xmm8, [rsp + 2*16]
        vmovdqa xmm9, [rsp + 3*16]
        vmovdqa xmm10, [rsp + 4*16]
        vmovdqa xmm11, [rsp + 5*16]
        vmovdqa xmm12, [rsp + 6*16]
        vmovdqa xmm13, [rsp + 7*16]
        vmovdqa xmm14, [rsp + 8*16]
        vmovdqa xmm15, [rsp + 9*16]
        mov     r12,  [rsp + 10*16 + 0*8]
        mov     r13,  [rsp + 10*16 + 1*8]
        mov     r14,  [rsp + 10*16 + 2*8]
        mov     r15,  [rsp + 10*16 + 3*8]
        add     rsp, stack_size
 %endmacro
%endif

%define len   arg0
%define vec   arg1
%define vec_i arg2
%define mul_array arg3
%define src   arg4
%define dest1 arg5
%define pos   rax
%define dest2 mul_array
%define dest3 vec_i
%define dest4 tmp3
%define dest5 tmp4

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

%define x0      ymm0
%define xd1     ymm1
%define xd2     ymm2
%define xd3     ymm3
%define xd4     ymm4
%define xd5     ymm5
%define xgft1   ymm6
%define xgft2   ymm7
%define xgft3   ymm8
%define xgft4   ymm9
%define xgft5   ymm10
%define xret1   ymm11
%define xret2   ymm12
%define xret3   ymm13
%define xret4   ymm14
%define xret5   ymm15

;;
;; Encodes 32 bytes of a single source into 5x 32 bytes (parity disks)
;;
%macro ENCODE_32B_5 0
        ;; get next source vector
        XLDR    x0, [src + pos]
        ;; get next dest vectors
        XLDR    xd1, [dest1 + pos]
        XLDR    xd2, [dest2 + pos]
        XLDR    xd3, [dest3 + pos]
        XLDR    xd4, [dest4 + pos]
        XLDR    xd5, [dest5 + pos]

        GF_MUL_XOR VEX, x0, xgft1, xret1, xd1, xgft2, xret2, xd2, \
                xgft3, xret3, xd3, xgft4, xret4, xd4, xgft5, xret5, xd5

        XSTR    [dest1 + pos], xd1
        XSTR    [dest2 + pos], xd2
        XSTR    [dest3 + pos], xd3
        XSTR    [dest4 + pos], xd4
        XSTR    [dest5 + pos], xd5
%endmacro

;;
;; Encodes less than 32 bytes of a single source into 5x parity disks
;;
%macro ENCODE_LT_32B_5 1
%define %%LEN   %1
        ;; get next source vector
        simd_load_avx2 x0, src + pos, %%LEN, tmp, tmp2
        ;; get next dest vectors
        simd_load_avx2 xd1, dest1 + pos, %%LEN, tmp, tmp2
        simd_load_avx2 xd2, dest2 + pos, %%LEN, tmp, tmp2
        simd_load_avx2 xd3, dest3 + pos, %%LEN, tmp, tmp2
        simd_load_avx2 xd4, dest4 + pos, %%LEN, tmp, tmp2
        simd_load_avx2 xd5, dest5 + pos, %%LEN, tmp, tmp2

        GF_MUL_XOR VEX, x0, xgft1, xret1, xd1, xgft2, xret2, xd2, \
                xgft3, xret3, xd3, xgft4, xret4, xd4, xgft5, xret5, xd5

        lea     dest1, [dest1 + pos]
        simd_store_avx2 dest1, xd1, %%LEN, tmp, tmp2
        lea     dest2, [dest2 + pos]
        simd_store_avx2 dest2, xd2, %%LEN, tmp, tmp2
        lea     dest3, [dest3 + pos]
        simd_store_avx2 dest3, xd3, %%LEN, tmp, tmp2
        lea     dest4, [dest4 + pos]
        simd_store_avx2 dest4, xd4, %%LEN, tmp, tmp2
        lea     dest5, [dest5 + pos]
        simd_store_avx2 dest5, xd5, %%LEN, tmp, tmp2
%endmacro

align 16
global gf_5vect_mad_avx2_gfni, function
func(gf_5vect_mad_avx2_gfni)
        FUNC_SAVE

        xor     pos, pos
        shl     vec_i, 3                ;Multiply by 8
        shl     vec, 3                  ;Multiply by 8
        lea     tmp, [mul_array + vec_i]
        lea     tmp2, [vec*3]
        vbroadcastsd xgft1, [tmp]
        vbroadcastsd xgft2, [tmp + vec]
        vbroadcastsd xgft3, [tmp + vec*2]
        vbroadcastsd xgft4, [tmp + tmp2]
        vbroadcastsd xgft5, [tmp + vec*4]
        mov     dest2, [dest1 + 1*8]    ; reuse mul_array
        mov     dest3, [dest1 + 2*8]    ; reuse vec_i
        mov     dest4, [dest1 + 3*8]
        mov     dest5, [dest1 + 4*8]
        mov     dest1, [dest1]

        cmp     len, 32
        jl      .len_lt_32

.loop32:
        ENCODE_32B_5            ;; loop on 32 bytes at a time

        add     pos, 32
        sub     len, 32
        cmp     len, 32
        jge     .loop32

.len_lt_32:
        cmp     len, 0
        jle     .exit

        ENCODE_LT_32B_5 len     ;; encode final bytes

.exit:
        vzeroupper

        FUNC_RESTORE
        ret

endproc_frame
%endif  ; if AS_FEATURE_LEVEL >= 10
