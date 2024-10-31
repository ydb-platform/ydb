;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;1;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
;;; gf_2vect_dot_prod_avx2_gfni(len, vec, *g_tbls, **buffs, **dests);
;;;

%include "reg_sizes.asm"
%include "gf_vect_gfni.inc"
%include "memcpy.asm"

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
 %define tmp3  r13      ; must be saved and restored
 %define tmp4  r12      ; must be saved and restored
 %define tmp5  r14      ; must be saved and restored

 %define stack_size  3*8
 %define func(x) x: endbranch
 %macro FUNC_SAVE 0
    sub     rsp, stack_size
    mov     [rsp + 0*8], r12
    mov     [rsp + 1*8], r13
    mov     [rsp + 2*8], r14
 %endmacro
 %macro FUNC_RESTORE 0
    mov     r12, [rsp + 0*8]
    mov     r13, [rsp + 1*8]
    mov     r14, [rsp + 2*8]
    add     rsp, stack_size
 %endmacro
%endif

%ifidn __OUTPUT_FORMAT__, win64
 %define arg0   rcx
 %define arg1   rdx
 %define arg2   r8
 %define arg3   r9

 %define arg4   r12     ; must be saved, loaded and restored
 %define arg5   r15     ; must be saved and restored
 %define tmp    r11
 %define tmp2   r10
 %define tmp3   r13     ; must be saved and restored
 %define tmp4   r14     ; must be saved and restored
 %define tmp5   rdi     ; must be saved and restored
 %define stack_size  7*16 + 5*8     ; must be an odd multiple of 8
 %define arg(x)      [rsp + stack_size + 8 + 8*x]

 %define func(x) proc_frame x
 %macro FUNC_SAVE 0
        alloc_stack stack_size
        vmovdqa [rsp + 0*16], xmm6
        vmovdqa [rsp + 1*16], xmm7
        vmovdqa [rsp + 2*16], xmm8
        vmovdqa [rsp + 3*16], xmm9
        vmovdqa [rsp + 4*16], xmm10
        vmovdqa [rsp + 5*16], xmm11
        vmovdqa [rsp + 6*16], xmm12
        mov     [rsp + 7*16 + 0*8], r12
        mov     [rsp + 7*16 + 1*8], r13
        mov     [rsp + 7*16 + 2*8], r14
        mov     [rsp + 7*16 + 3*8], r15
        mov     [rsp + 7*16 + 4*8], rdi
        end_prolog
        mov     arg4, arg(4)
 %endmacro

 %macro FUNC_RESTORE 0
        vmovdqa xmm6, [rsp + 0*16]
        vmovdqa xmm7, [rsp + 1*16]
        vmovdqa xmm8, [rsp + 2*16]
        vmovdqa xmm9, [rsp + 3*16]
        vmovdqa xmm10, [rsp + 4*16]
        vmovdqa xmm11, [rsp + 5*16]
        vmovdqa xmm12, [rsp + 6*16]
        mov     r12,  [rsp + 7*16 + 0*8]
        mov     r13,  [rsp + 7*16 + 1*8]
        mov     r14,  [rsp + 7*16 + 2*8]
        mov     r15,  [rsp + 7*16 + 3*8]
        mov     rdi,  [rsp + 7*16 + 4*8]
        add     rsp, stack_size
 %endmacro
%endif


%define len    arg0
%define vec    arg1
%define mul_array arg2
%define src    arg3
%define dest   arg4
%define ptr    arg5
%define vec_i  tmp2
%define dest2  tmp3
%define dest1  tmp5
%define pos    rax

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

%define x0l     ymm0
%define x0h     ymm1
%define x0x     ymm2

%define xgft1   ymm3
%define xgft2   ymm4

%define xtmp1   ymm5
%define xtmp2   ymm6

%define xp1l    ymm7
%define xp2l    ymm8

%define xp1h    ymm9
%define xp2h    ymm10

%define xp1x    ymm11
%define xp2x    ymm12

%define x0      x0l
%define xp1     xp1l
%define xp2     xp2l

default rel
[bits 64]

section .text

;;
;; Encodes 96 bytes of all "k" sources into 2x 96 bytes (parity disk)
;;
%macro ENCODE_96B_2 0
        vpxor   xp1l, xp1l, xp1l
        vpxor   xp1h, xp1h, xp1h
        vpxor   xp1x, xp1x, xp1x

        vpxor   xp2l, xp2l, xp2l
        vpxor   xp2h, xp2h, xp2h
        vpxor   xp2x, xp2x, xp2x
        mov     tmp, mul_array
        xor     vec_i, vec_i

%%next_vect:
        ;; load next source vector
        mov     ptr, [src + vec_i]
        XLDR    x0l, [ptr + pos]
        XLDR    x0h, [ptr + pos + 32]
        XLDR    x0x, [ptr + pos + 64]
        add     vec_i, 8

        vbroadcastsd xgft1, [tmp]
        vbroadcastsd xgft2, [tmp + vec]

        GF_MUL_XOR VEX, x0l, xgft1, xtmp1, xp1l, xgft2, xtmp2, xp2l
        GF_MUL_XOR VEX, x0h, xgft1, xtmp1, xp1h, xgft2, xtmp2, xp2h
        GF_MUL_XOR VEX, x0x, xgft1, xtmp1, xp1x, xgft2, xtmp2, xp2x
        add     tmp, 8

        cmp     vec_i, vec
        jl      %%next_vect

        XSTR    [dest1 + pos], xp1l
        XSTR    [dest1 + pos + 32], xp1h
        XSTR    [dest1 + pos + 64], xp1x
        XSTR    [dest2 + pos], xp2l
        XSTR    [dest2 + pos + 32], xp2h
        XSTR    [dest2 + pos + 64], xp2x
%endmacro

;;
;; Encodes 64 bytes of all "k" sources into 2x 64 bytes (parity disks)
;;
%macro ENCODE_64B_2 0
        vpxor   xp1l, xp1l, xp1l
        vpxor   xp1h, xp1h, xp1h
        vpxor   xp2l, xp2l, xp2l
        vpxor   xp2h, xp2h, xp2h
        mov     tmp, mul_array
        xor     vec_i, vec_i

%%next_vect:
        mov     ptr, [src + vec_i]
        XLDR    x0l, [ptr + pos]        ;; Get next source vector low 32 bytes
        XLDR    x0h, [ptr + pos + 32]   ;; Get next source vector high 32 bytes
        add     vec_i, 8

        vbroadcastsd xgft1, [tmp]
        vbroadcastsd xgft2, [tmp + vec]
        add     tmp, 8

        GF_MUL_XOR VEX, x0l, xgft1, xtmp1, xp1l, xgft2, xtmp2, xp2l
        GF_MUL_XOR VEX, x0h, xgft1, xgft1, xp1h, xgft2, xgft2, xp2h

        cmp     vec_i, vec
        jl      %%next_vect

        XSTR    [dest1 + pos], xp1l
        XSTR    [dest1 + pos + 32], xp1h
        XSTR    [dest2 + pos], xp2l
        XSTR    [dest2 + pos + 32], xp2h
%endmacro

;;
;; Encodes 32 bytes of all "k" sources into 2x 32 bytes (parity disks)
;;
%macro ENCODE_32B_2 0
        vpxor   xp1, xp1, xp1
        vpxor   xp2, xp2, xp2
        mov     tmp, mul_array
        xor     vec_i, vec_i

%%next_vect:
        mov     ptr, [src + vec_i]
        XLDR    x0, [ptr + pos]     ;Get next source vector (32 bytes)
        add	    vec_i, 8

        vbroadcastsd xgft1, [tmp]
        vbroadcastsd xgft2, [tmp + vec]
        add     tmp, 8

        GF_MUL_XOR VEX, x0, xgft1, xgft1, xp1, xgft2, xgft2, xp2

        cmp     vec_i, vec
        jl      %%next_vect

        XSTR    [dest1 + pos], xp1
        XSTR    [dest2 + pos], xp2
%endmacro

;;
;; Encodes less than 32 bytes of all "k" sources into 2 parity disks
;;
%macro ENCODE_LT_32B_2 1
%define %%LEN   %1

        vpxor   xp1, xp1, xp1
        vpxor   xp2, xp2, xp2
        xor     vec_i, vec_i

%%next_vect:
        mov     ptr, [src + vec_i]
        simd_load_avx2 x0, ptr + pos, %%LEN, tmp, tmp4 ;Get next source vector
        add     vec_i, 8

        vbroadcastsd xgft1, [mul_array]
        vbroadcastsd xgft2, [mul_array + vec]
        add     mul_array, 8

        GF_MUL_XOR VEX, x0, xgft1, xgft1, xp1, xgft2, xgft2, xp2

        cmp     vec_i, vec
        jl      %%next_vect

        ;Store updated encoded data
        lea     ptr, [dest1 + pos]
        simd_store_avx2 ptr, xp1, %%LEN, tmp, tmp4

        lea     ptr, [dest2 + pos]
        simd_store_avx2 ptr, xp2, %%LEN, tmp, tmp4
%endmacro

align 16
global gf_2vect_dot_prod_avx2_gfni, function
func(gf_2vect_dot_prod_avx2_gfni)
        FUNC_SAVE

        xor     pos, pos
        shl     vec, 3      ;; vec *= 8. Make vec_i count by 8
        mov     dest1, [dest]
        mov     dest2, [dest + 8]

        cmp     len, 96
        jl      .len_lt_96

.loop96:
        ENCODE_96B_2

        add     pos, 96     ;; Loop on 96 bytes at a time first
        sub     len, 96
        cmp     len, 96
        jge     .loop96

.len_lt_96:
        cmp     len, 64
        jl      .len_lt_64

        ENCODE_64B_2

        add     pos, 64     ;; encode next 64 bytes
        sub     len, 64

.len_lt_64:
        cmp     len, 32
        jl      .len_lt_32

        ENCODE_32B_2

        add     pos, 32     ;; encode next 32 bytes
        sub     len, 32

.len_lt_32:
        cmp     len, 0
        jle     .exit

        ENCODE_LT_32B_2 len ;; encode remaining bytes

.exit:
        vzeroupper

        FUNC_RESTORE
        ret

endproc_frame
%endif  ; if AS_FEATURE_LEVEL >= 10
