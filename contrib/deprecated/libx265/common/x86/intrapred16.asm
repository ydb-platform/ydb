;*****************************************************************************
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Dnyaneshwar Gorade <dnyaneshwar@multicorewareinc.com>
;*          Yuvaraj Venkatesh <yuvaraj@multicorewareinc.com>
;*          Min Chen <chenm003@163.com> <min.chen@multicorewareinc.com>
;*
;* This program is free software; you can redistribute it and/or modify
;* it under the terms of the GNU General Public License as published by
;* the Free Software Foundation; either version 2 of the License, or
;* (at your option) any later version.
;*
;* This program is distributed in the hope that it will be useful,
;* but WITHOUT ANY WARRANTY; without even the implied warranty of
;* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;* GNU General Public License for more details.
;*
;* You should have received a copy of the GNU General Public License
;* along with this program; if not, write to the Free Software
;* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
;*
;* This program is also available under a commercial proprietary license.
;* For more information, contact us at license @ x265.com.
;*****************************************************************************/

%include "x86inc.asm"
%include "x86util.asm"

SECTION_RODATA 32

const ang_table
%assign x 0
%rep 32
    times 4 dw (32-x), x
%assign x x+1
%endrep

const ang_table_avx2
%assign x 0
%rep 32
    times 8 dw (32-x), x
%assign x x+1
%endrep

const pw_ang16_12_24,               db  0,  0,  0,  0,  0,  0,  0,  0, 14, 15, 14, 15,  0,  1,  0,  1
const pw_ang16_13_23,               db  2,  3,  2,  3, 14, 15, 14, 15,  6,  7,  6,  7,  0,  1,  0,  1
const pw_ang16_14_22,               db  2,  3,  2,  3, 10, 11, 10, 11,  6,  7,  6,  7,  0,  1,  0,  1
const pw_ang16_15_21,               db 12, 13, 12, 13,  8,  9,  8,  9,  4,  5,  4,  5,  0,  1,  0,  1
const pw_ang16_16_20,               db  8,  9,  8,  9,  6,  7,  6,  7,  2,  3,  2,  3,  0,  1,  0,  1

const pw_ang32_12_24,               db  0,  1,  0,  1,  2,  3,  2,  3,  4,  5,  4,  5,  6,  7,  6,  7
const pw_ang32_13_23,               db  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 14, 15,  6,  7,  0,  1
const pw_ang32_14_22,               db  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 10, 11,  6,  7,  0,  1
const pw_ang32_15_21,               db  0,  0,  0,  0,  0,  0,  0,  0, 12, 13,  8,  9,  4,  5,  0,  1
const pw_ang32_16_20,               db  0,  0,  0,  0,  0,  0,  0,  0,  8,  9,  6,  7,  2,  3,  0,  1
const pw_ang32_17_19_0,             db  0,  0,  0,  0, 12, 13, 10, 11,  8,  9,  6,  7,  2,  3,  0,  1

const shuf_mode_13_23,              db  0,  0, 14, 15,  6,  7,  0,  1,  0,  0,  0,  0,  0,  0,  0,  0
const shuf_mode_14_22,              db 14, 15, 10, 11,  4,  5,  0,  1,  0,  0,  0,  0,  0,  0,  0,  0
const shuf_mode_15_21,              db 12, 13,  8,  9,  4,  5,  0,  1,  0,  0,  0,  0,  0,  0,  0,  0
const shuf_mode_16_20,              db  2,  3,  0,  1, 14, 15, 12, 13,  8,  9,  6,  7,  2,  3,  0,  1
const shuf_mode_17_19,              db  0,  1, 14, 15, 12, 13, 10, 11,  6,  7,  4,  5,  2,  3,  0,  1
const shuf_mode32_18,               db 14, 15, 12, 13, 10, 11,  8,  9,  6,  7,  4,  5,  2,  3,  0,  1
const pw_punpcklwd,                 db  0,  1,  2,  3,  2,  3,  4,  5,  4,  5,  6,  7,  6,  7,  8,  9
const c_mode32_10_0,                db  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1

const pw_ang8_12,                   db  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 12, 13,  0,  1
const pw_ang8_13,                   db  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 14, 15,  8,  9,  0,  1
const pw_ang8_14,                   db  0,  0,  0,  0,  0,  0,  0,  0, 14, 15, 10, 11,  4,  5,  0,  1
const pw_ang8_15,                   db  0,  0,  0,  0,  0,  0,  0,  0, 12, 13,  8,  9,  4,  5,  0,  1
const pw_ang8_16,                   db  0,  0,  0,  0,  0,  0, 12, 13, 10, 11,  6,  7,  4,  5,  0,  1
const pw_ang8_17,                   db  0,  0, 14, 15, 12, 13, 10, 11,  8,  9,  4,  5,  2,  3,  0,  1
const pw_swap16,            times 2 db 14, 15, 12, 13, 10, 11,  8,  9,  6,  7,  4,  5,  2,  3,  0,  1

const pw_ang16_13,                  db 14, 15,  8,  9,  0,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0
const pw_ang16_16,                  db  0,  0,  0,  0,  0,  0, 10, 11,  8,  9,  6,  7,  2,  3,  0,  1

intra_filter4_shuf0:                db  2,  3,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 ,11, 12, 13
intra_filter4_shuf1:                db 14, 15,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 ,11, 12, 13
intra_filter4_shuf2:        times 2 db  4,  5,  0,  1,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15

;; (blkSize - 1 - x)
pw_planar4_0:                       dw  3,  2,  1,  0,  3,  2,  1,  0

const planar32_table
%assign x 31
%rep 8
    dd x, x-1, x-2, x-3
%assign x x-4
%endrep

const planar32_table1
%assign x 1
%rep 8
    dd x, x+1, x+2, x+3
%assign x x+4
%endrep

SECTION .text

cextern pb_01
cextern pw_1
cextern pw_2
cextern pw_3
cextern pw_7
cextern pw_4
cextern pw_8
cextern pw_15
cextern pw_16
cextern pw_31
cextern pw_32
cextern pd_15
cextern pd_16
cextern pd_31
cextern pd_32
cextern pd_0000ffff
cextern pw_4096
cextern pw_pixel_max
cextern multiL
cextern multiH
cextern multiH2
cextern multiH3
cextern multi_2Row
cextern pw_swap
cextern pb_unpackwq1
cextern pb_unpackwq2
cextern pw_planar16_mul
cextern pd_planar16_mul0
cextern pd_planar16_mul1
cextern pw_planar32_mul
cextern pd_planar32_mul1
cextern pd_planar32_mul2
cextern pd_planar16_mul2

;-----------------------------------------------------------------------------------
; void intra_pred_dc(pixel* dst, intptr_t dstStride, pixel* above, int, int filter)
;-----------------------------------------------------------------------------------
INIT_XMM sse2
cglobal intra_pred_dc4, 5,6,2
    movh        m0,             [r2 + 18]          ; sumAbove
    movh        m1,             [r2 + 2]           ; sumLeft

    paddw       m0,             m1
    pshuflw     m1,             m0, 0x4E
    paddw       m0,             m1
    pshuflw     m1,             m0, 0xB1
    paddw       m0,             m1

    test        r4d,            r4d

    paddw       m0,             [pw_4]
    psrlw       m0,             3

    ; store DC 4x4
    movh        [r0],           m0
    movh        [r0 + r1 * 2],  m0
    movh        [r0 + r1 * 4],  m0
    lea         r5,             [r0 + r1 * 4]
    movh        [r5 + r1 * 2],  m0

    ; do DC filter
    jz          .end
    movh        m1,             m0
    psllw       m1,             1
    paddw       m1,             [pw_2]
    movd        r3d,            m1
    paddw       m0,             m1
    ; filter top
    movh        m1,             [r2 + 2]
    paddw       m1,             m0
    psrlw       m1,             2
    movh        [r0],           m1             ; overwrite top-left pixel, we will update it later

    ; filter top-left
    movzx       r3d,            r3w
    movzx       r4d, word       [r2 + 18]
    add         r3d,            r4d
    movzx       r4d, word       [r2 + 2]
    add         r4d,            r3d
    shr         r4d,            2
    mov         [r0],           r4w

    ; filter left
    movu        m1,             [r2 + 20]
    paddw       m1,             m0
    psrlw       m1,             2
    movd        r3d,            m1
    mov         [r0 + r1 * 2],  r3w
    shr         r3d,            16
    mov         [r0 + r1 * 4],  r3w
    pextrw      r3d,            m1, 2
    mov         [r5 + r1 * 2],  r3w
.end:
    RET

;-----------------------------------------------------------------------------------
; void intra_pred_dc(pixel* dst, intptr_t dstStride, pixel* above, int, int filter)
;-----------------------------------------------------------------------------------
INIT_XMM sse2
cglobal intra_pred_dc8, 5, 8, 2
    movu            m0,            [r2 + 34]
    movu            m1,            [r2 + 2]

    paddw           m0,            m1
    movhlps         m1,            m0
    paddw           m0,            m1
    pshufd          m1,            m0, 1
    paddw           m0,            m1
    pmaddwd         m0,            [pw_1]

    paddw           m0,            [pw_8]
    psrlw           m0,            4              ; sum = sum / 16
    pshuflw         m0,            m0, 0
    pshufd          m0,            m0, 0          ; m0 = word [dc_val ...]

    test            r4d,           r4d

    ; store DC 8x8
    lea             r6,            [r1 + r1 * 4]
    lea             r6,            [r6 + r1]
    lea             r5,            [r6 + r1 * 4]
    lea             r7,            [r6 + r1 * 8]
    movu            [r0],          m0
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 4], m0
    movu            [r0 + r6],     m0
    movu            [r0 + r1 * 8], m0
    movu            [r0 + r5],     m0
    movu            [r0 + r6 * 2], m0
    movu            [r0 + r7],     m0

    ; Do DC Filter
    jz              .end
    mova            m1,            [pw_2]
    pmullw          m1,            m0
    paddw           m1,            [pw_2]
    movd            r4d,           m1             ; r4d = DC * 2 + 2
    paddw           m1,            m0             ; m1 = DC * 3 + 2
    pshuflw         m1,            m1, 0
    pshufd          m1,            m1, 0          ; m1 = pixDCx3

    ; filter top
    movu            m0,            [r2 + 2]
    paddw           m0,            m1
    psrlw           m0,            2
    movu            [r0],          m0

    ; filter top-left
    movzx           r4d,           r4w
    movzx           r3d, word      [r2 + 34]
    add             r4d,           r3d
    movzx           r3d, word      [r2 + 2]
    add             r3d,           r4d
    shr             r3d,           2
    mov             [r0],          r3w

    ; filter left
    movu            m0,            [r2 + 36]
    paddw           m0,            m1
    psrlw           m0,            2
    movh            r3,            m0
    mov             [r0 + r1 * 2], r3w
    shr             r3,            16
    mov             [r0 + r1 * 4], r3w
    shr             r3,            16
    mov             [r0 + r6],     r3w
    shr             r3,            16
    mov             [r0 + r1 * 8], r3w
    pshufd          m0,            m0, 0x6E
    movh            r3,            m0
    mov             [r0 + r5],     r3w
    shr             r3,            16
    mov             [r0 + r6 * 2], r3w
    shr             r3,            16
    mov             [r0 + r7],     r3w
.end:
    RET

;-------------------------------------------------------------------------------------------------------
; void intra_pred_dc(pixel* dst, intptr_t dstStride, pixel* left, pixel* above, int dirMode, int filter)
;-------------------------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal intra_pred_dc16, 5, 10, 4
    lea             r3,                  [r2 + 66]
    add             r1,                  r1
    movu            m0,                  [r3]
    movu            m1,                  [r3 + 16]
    movu            m2,                  [r2 + 2]
    movu            m3,                  [r2 + 18]

    paddw           m0,                  m1
    paddw           m2,                  m3
    paddw           m0,                  m2
    HADDUW          m0,                  m1
    paddd           m0,                  [pd_16]
    psrld           m0,                  5

    movd            r5d,                 m0
    pshuflw         m0,                  m0, 0 ; m0 = word [dc_val ...]
    pshufd          m0,                  m0, 0

    test            r4d,                 r4d

    ; store DC 16x16
    lea             r6,                  [r1 + r1 * 2]        ;index 3
    lea             r7,                  [r1 + r1 * 4]        ;index 5
    lea             r8,                  [r6 + r1 * 4]        ;index 7
    lea             r9,                  [r0 + r8]            ;base + 7
    movu            [r0],                m0
    movu            [r0 + 16],           m0
    movu            [r0 + r1],           m0
    movu            [r0 + 16 + r1],      m0
    movu            [r0 + r1 * 2],       m0
    movu            [r0 + r1 * 2 + 16],  m0
    movu            [r0 + r6],           m0
    movu            [r0 + r6 + 16],      m0
    movu            [r0 + r1 * 4],       m0
    movu            [r0 + r1 * 4 + 16],  m0
    movu            [r0 + r7],           m0
    movu            [r0 + r7 + 16],      m0
    movu            [r0 + r6 * 2],       m0
    movu            [r0 + r6 * 2 + 16],  m0
    movu            [r9],                m0
    movu            [r9 + 16],           m0
    movu            [r0 + r1 * 8],       m0
    movu            [r0 + r1 * 8 + 16],  m0
    movu            [r9 + r1 * 2],       m0
    movu            [r9 + r1 * 2 + 16],  m0
    movu            [r0 + r7 * 2],       m0
    movu            [r0 + r7 * 2 + 16],  m0
    movu            [r9 + r1 * 4],       m0
    movu            [r9 + r1 * 4 + 16],  m0
    movu            [r0 + r6 * 4],       m0
    movu            [r0 + r6 * 4 + 16],  m0
    movu            [r9 + r6 * 2],       m0
    movu            [r9 + r6 * 2 + 16],  m0
    movu            [r9 + r8],           m0
    movu            [r9 + r8 + 16],      m0
    movu            [r9 + r1 * 8],       m0
    movu            [r9 + r1 * 8 + 16],  m0

    ; Do DC Filter
    jz              .end
    mova            m1,                  [pw_2]
    pmullw          m1,                  m0
    paddw           m1,                  [pw_2]
    movd            r4d,                 m1
    paddw           m1,                  m0

    ; filter top
    movu            m2,                  [r2 + 2]
    paddw           m2,                  m1
    psrlw           m2,                  2
    movu            [r0],                m2
    movu            m3,                  [r2 + 18]
    paddw           m3,                  m1
    psrlw           m3,                  2
    movu            [r0 + 16],           m3

    ; filter top-left
    movzx           r4d,                 r4w
    movzx           r5d, word            [r3]
    add             r4d,                 r5d
    movzx           r5d, word            [r2 + 2]
    add             r5d,                 r4d
    shr             r5d,                 2
    mov             [r0],                r5w

    ; filter left
    movu            m2,                  [r3 + 2]
    paddw           m2,                  m1
    psrlw           m2,                  2

    movq            r2,                  m2
    pshufd          m2,                  m2, 0xEE
    mov             [r0 + r1],           r2w
    shr             r2,                  16
    mov             [r0 + r1 * 2],       r2w
    shr             r2,                  16
    mov             [r0 + r6],           r2w
    shr             r2,                  16
    mov             [r0 + r1 * 4],       r2w
    movq            r2,                  m2
    mov             [r0 + r7],           r2w
    shr             r2,                  16
    mov             [r0 + r6 * 2],       r2w
    shr             r2,                  16
    mov             [r9],                r2w
    shr             r2,                  16
    mov             [r0 + r1 * 8],       r2w

    movu            m3,                  [r3 + 18]
    paddw           m3,                  m1
    psrlw           m3,                  2

    movq            r3,                  m3
    pshufd          m3,                  m3, 0xEE
    mov             [r9 + r1 * 2],       r3w
    shr             r3,                  16
    mov             [r0 + r7 * 2],       r3w
    shr             r3,                  16
    mov             [r9 + r1 * 4],       r3w
    shr             r3,                  16
    mov             [r0 + r6 * 4],       r3w
    movq            r3,                  m3
    mov             [r9 + r6 * 2],       r3w
    shr             r3,                  16
    mov             [r9 + r8],           r3w
    shr             r3,                  16
    mov             [r9 + r1 * 8],       r3w
.end:
    RET

;-------------------------------------------------------------------------------------------
; void intra_pred_dc(pixel* above, pixel* left, pixel* dst, intptr_t dstStride, int filter)
;-------------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal intra_pred_dc32, 3, 4, 6
    lea             r3,                  [r2 + 130]     ;130 = 32*sizeof(pixel)*2 + 1*sizeof(pixel)
    add             r2,                  2
    add             r1,                  r1
    movu            m0,                  [r3]
    movu            m1,                  [r3 + 16]
    movu            m2,                  [r3 + 32]
    movu            m3,                  [r3 + 48]
    paddw           m0,                  m1
    paddw           m2,                  m3
    paddw           m0,                  m2
    HADDUWD         m0,                  m1

    movu            m1,                  [r2]
    movu            m2,                  [r2 + 16]
    movu            m3,                  [r2 + 32]
    movu            m4,                  [r2 + 48]
    paddw           m1,                  m2
    paddw           m3,                  m4
    paddw           m1,                  m3
    HADDUWD         m1,                  m2

    paddd           m0,                  m1
    HADDD           m0,                  m1
    paddd           m0,                  [pd_32]     ; sum = sum + 32
    psrld           m0,                  6           ; sum = sum / 64
    pshuflw         m0,                  m0, 0
    pshufd          m0,                  m0, 0

    lea             r2,                 [r1 * 3]
    ; store DC 32x32
%assign x 1
%rep 8
    movu            [r0 +  0],          m0
    movu            [r0 + 16],          m0
    movu            [r0 + 32],          m0
    movu            [r0 + 48],          m0
    movu            [r0 + r1 +  0],     m0
    movu            [r0 + r1 + 16],     m0
    movu            [r0 + r1 + 32],     m0
    movu            [r0 + r1 + 48],     m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + 16], m0
    movu            [r0 + r1 * 2 + 32], m0
    movu            [r0 + r1 * 2 + 48], m0
    movu            [r0 + r2 +  0],     m0
    movu            [r0 + r2 + 16],     m0
    movu            [r0 + r2 + 32],     m0
    movu            [r0 + r2 + 48],     m0
    %if x < 8
    lea             r0, [r0 + r1 * 4]
    %endif
%assign x x + 1
%endrep
    RET

;-------------------------------------------------------------------------------------------------------
; void intra_pred_dc(pixel* dst, intptr_t dstStride, pixel* left, pixel* above, int dirMode, int filter)
;-------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal intra_pred_dc16, 3, 9, 4
    mov             r3d,                 r4m
    add             r1d,                 r1d
    movu            m0,                  [r2 + 66]
    movu            m2,                  [r2 +  2]
    paddw           m0,                  m2                 ; dynamic range 13 bits

    vextracti128    xm1,                 m0, 1
    paddw           xm0,                 xm1                ; dynamic range 14 bits
    movhlps         xm1,                 xm0
    paddw           xm0,                 xm1                ; dynamic range 15 bits
    pmaddwd         xm0,                 [pw_1]
    phaddd          xm0,                 xm0
    paddd           xm0,                 [pd_16]
    psrld           xm0,                 5
    movd            r5d,                 xm0
    vpbroadcastw    m0,                  xm0

    test            r3d,                 r3d

    ; store DC 16x16
    lea             r6,                  [r1 + r1 * 2]        ; index 3
    lea             r7,                  [r1 + r1 * 4]        ; index 5
    lea             r8,                  [r6 + r1 * 4]        ; index 7
    lea             r4,                  [r0 + r8 * 1]        ; base + 7

    movu            [r0],                m0
    movu            [r0 + r1],           m0
    movu            [r0 + r1 * 2],       m0
    movu            [r0 + r6],           m0
    movu            [r0 + r1 * 4],       m0
    movu            [r0 + r7],           m0
    movu            [r0 + r6 * 2],       m0
    movu            [r4],                m0
    movu            [r0 + r1 * 8],       m0
    movu            [r4 + r1 * 2],       m0
    movu            [r0 + r7 * 2],       m0
    movu            [r4 + r1 * 4],       m0
    movu            [r0 + r6 * 4],       m0
    movu            [r4 + r6 * 2],       m0
    movu            [r4 + r8],           m0
    movu            [r4 + r1 * 8],       m0

    ; Do DC Filter
    jz              .end
    mova            m1,                  [pw_2]
    pmullw          m1,                  m0
    paddw           m1,                  [pw_2]
    movd            r3d,                 xm1
    paddw           m1,                  m0

    ; filter top
    movu            m2,                  [r2 + 2]
    paddw           m2,                  m1
    psrlw           m2,                  2
    movu            [r0],                m2

    ; filter top-left
    movzx           r3d,                 r3w
    movzx           r5d, word            [r2 + 66]
    add             r3d,                 r5d
    movzx           r5d, word            [r2 + 2]
    add             r5d,                 r3d
    shr             r5d,                 2
    mov             [r0],                r5w

    ; filter left
    movu            m2,                  [r2 + 68]
    paddw           m2,                  m1
    psrlw           m2,                  2
    vextracti128    xm3,                 m2, 1

    movq            r3,                  xm2
    pshufd          xm2,                 xm2, 0xEE
    mov             [r0 + r1],           r3w
    shr             r3,                  16
    mov             [r0 + r1 * 2],       r3w
    shr             r3,                  16
    mov             [r0 + r6],           r3w
    shr             r3,                  16
    mov             [r0 + r1 * 4],       r3w
    movq            r3,                  xm2
    mov             [r0 + r7],           r3w
    shr             r3,                  16
    mov             [r0 + r6 * 2],       r3w
    shr             r3,                  16
    mov             [r4],                r3w
    shr             r3,                  16
    mov             [r0 + r1 * 8],       r3w

    movq            r3,                  xm3
    pshufd          xm3,                 xm3, 0xEE
    mov             [r4 + r1 * 2],       r3w
    shr             r3,                  16
    mov             [r0 + r7 * 2],       r3w
    shr             r3,                  16
    mov             [r4 + r1 * 4],       r3w
    shr             r3,                  16
    mov             [r0 + r6 * 4],       r3w
    movq            r3,                  xm3
    mov             [r4 + r6 * 2],       r3w
    shr             r3,                  16
    mov             [r4 + r8],           r3w
    shr             r3,                  16
    mov             [r4 + r1 * 8],       r3w
.end:
    RET

;---------------------------------------------------------------------------------------------
; void intra_pred_dc(pixel* dst, intptr_t dstStride, pixel *srcPix, int dirMode, int bFilter)
;---------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal intra_pred_dc32, 3,3,3
    add              r2, 2
    add             r1d, r1d
    movu             m0, [r2]
    movu             m1, [r2 + 32]
    add              r2, mmsize*4                       ; r2 += 128
    paddw            m0, m1                             ; dynamic range 13 bits
    movu             m1, [r2]
    movu             m2, [r2 + 32]
    paddw            m1, m2                             ; dynamic range 13 bits
    paddw            m0, m1                             ; dynamic range 14 bits
    vextracti128    xm1, m0, 1
    paddw           xm0, xm1                            ; dynamic range 15 bits
    pmaddwd         xm0, [pw_1]
    movhlps         xm1, xm0
    paddd           xm0, xm1
    phaddd          xm0, xm0
    paddd           xm0, [pd_32]                        ; sum = sum + 32
    psrld           xm0, 6                              ; sum = sum / 64
    vpbroadcastw     m0, xm0

    lea              r2, [r1 * 3]
    ; store DC 32x32
    movu            [r0 + r1 * 0 +  0], m0
    movu            [r0 + r1 * 0 + mmsize], m0
    movu            [r0 + r1 * 1 +  0], m0
    movu            [r0 + r1 * 1 + mmsize], m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + mmsize], m0
    movu            [r0 + r2 * 1 +  0], m0
    movu            [r0 + r2 * 1 + mmsize], m0
    lea             r0, [r0 + r1 * 4]
    movu            [r0 + r1 * 0 +  0], m0
    movu            [r0 + r1 * 0 + mmsize], m0
    movu            [r0 + r1 * 1 +  0], m0
    movu            [r0 + r1 * 1 + mmsize], m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + mmsize], m0
    movu            [r0 + r2 * 1 +  0], m0
    movu            [r0 + r2 * 1 + mmsize], m0
    lea             r0, [r0 + r1 * 4]
    movu            [r0 + r1 * 0 +  0], m0
    movu            [r0 + r1 * 0 + mmsize], m0
    movu            [r0 + r1 * 1 +  0], m0
    movu            [r0 + r1 * 1 + mmsize], m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + mmsize], m0
    movu            [r0 + r2 * 1 +  0], m0
    movu            [r0 + r2 * 1 + mmsize], m0
    lea             r0, [r0 + r1 * 4]
    movu            [r0 + r1 * 0 +  0], m0
    movu            [r0 + r1 * 0 + mmsize], m0
    movu            [r0 + r1 * 1 +  0], m0
    movu            [r0 + r1 * 1 + mmsize], m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + mmsize], m0
    movu            [r0 + r2 * 1 +  0], m0
    movu            [r0 + r2 * 1 + mmsize], m0
    lea             r0, [r0 + r1 * 4]
    movu            [r0 + r1 * 0 +  0], m0
    movu            [r0 + r1 * 0 + mmsize], m0
    movu            [r0 + r1 * 1 +  0], m0
    movu            [r0 + r1 * 1 + mmsize], m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + mmsize], m0
    movu            [r0 + r2 * 1 +  0], m0
    movu            [r0 + r2 * 1 + mmsize], m0
    lea             r0, [r0 + r1 * 4]
    movu            [r0 + r1 * 0 +  0], m0
    movu            [r0 + r1 * 0 + mmsize], m0
    movu            [r0 + r1 * 1 +  0], m0
    movu            [r0 + r1 * 1 + mmsize], m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + mmsize], m0
    movu            [r0 + r2 * 1 +  0], m0
    movu            [r0 + r2 * 1 + mmsize], m0
    lea             r0, [r0 + r1 * 4]
    movu            [r0 + r1 * 0 +  0], m0
    movu            [r0 + r1 * 0 + mmsize], m0
    movu            [r0 + r1 * 1 +  0], m0
    movu            [r0 + r1 * 1 + mmsize], m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + mmsize], m0
    movu            [r0 + r2 * 1 +  0], m0
    movu            [r0 + r2 * 1 + mmsize], m0
    lea             r0, [r0 + r1 * 4]
    movu            [r0 + r1 * 0 +  0], m0
    movu            [r0 + r1 * 0 + mmsize], m0
    movu            [r0 + r1 * 1 +  0], m0
    movu            [r0 + r1 * 1 + mmsize], m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + mmsize], m0
    movu            [r0 + r2 * 1 +  0], m0
    movu            [r0 + r2 * 1 + mmsize], m0
    RET

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal intra_pred_planar8, 3,3,5
    movu            m1, [r2 + 2]
    movu            m2, [r2 + 34]

    movd            m3, [r2 + 18]           ; topRight   = above[8];
    movd            m4, [r2 + 50]           ; bottomLeft = left[8];

    pshuflw         m3, m3, 0
    pshuflw         m4, m4, 0
    pshufd          m3, m3, 0               ; v_topRight
    pshufd          m4, m4, 0               ; v_bottomLeft

    pmullw          m3, [multiL]            ; (x + 1) * topRight
    pmullw          m0, m1, [pw_7]          ; (blkSize - 1 - y) * above[x]
    paddw           m3, [pw_8]
    paddw           m3, m4
    paddw           m3, m0
    psubw           m4, m1

%macro INTRA_PRED_PLANAR_8 1
%if (%1 < 4)
    pshuflw         m1, m2, 0x55 * %1
    pshufd          m1, m1, 0
%else
    pshufhw         m1, m2, 0x55 * (%1 - 4)
    pshufd          m1, m1, 0xAA
%endif
    pmullw          m1, [pw_planar16_mul + mmsize]
    paddw           m1, m3
    psrlw           m1, 4
    movu            [r0], m1
%if (%1 < 7)
    paddw           m3, m4
    lea             r0, [r0 + r1 * 2]
%endif
%endmacro

    INTRA_PRED_PLANAR_8 0
    INTRA_PRED_PLANAR_8 1
    INTRA_PRED_PLANAR_8 2
    INTRA_PRED_PLANAR_8 3
    INTRA_PRED_PLANAR_8 4
    INTRA_PRED_PLANAR_8 5
    INTRA_PRED_PLANAR_8 6
    INTRA_PRED_PLANAR_8 7
    RET

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse2
%if ARCH_X86_64 == 1 && BIT_DEPTH == 12
cglobal intra_pred_planar16, 3,5,13
    add             r1d, r1d
    pxor            m12, m12

    movu            m2, [r2 + 2]
    movu            m10, [r2 + 18]

    punpckhwd       m7, m2, m12
    punpcklwd       m2, m12
    punpckhwd       m0, m10, m12
    punpcklwd       m10, m12

    movzx           r3d, word [r2 + 34]                     ; topRight   = above[16]
    lea             r4, [pd_planar16_mul1]

    movd            m3, r3d
    pshufd          m3, m3, 0                               ; topRight

    pmaddwd         m8, m3, [r4 + 3*mmsize]                 ; (x + 1) * topRight
    pmaddwd         m4, m3, [r4 + 2*mmsize]                 ; (x + 1) * topRight
    pmaddwd         m9, m3, [r4 + 1*mmsize]                 ; (x + 1) * topRight
    pmaddwd         m3, m3, [r4 + 0*mmsize]                 ; (x + 1) * topRight

    mova            m11, [pd_15]
    pmaddwd         m1, m2,  m11                            ; (blkSize - 1 - y) * above[x]
    pmaddwd         m6, m7,  m11                            ; (blkSize - 1 - y) * above[x]
    pmaddwd         m5, m10, m11                            ; (blkSize - 1 - y) * above[x]
    pmaddwd         m11, m0                                 ; (blkSize - 1 - y) * above[x]

    paddd           m4, m5
    paddd           m3, m1
    paddd           m8, m11
    paddd           m9, m6

    mova            m5, [pd_16]
    paddd           m3, m5
    paddd           m9, m5
    paddd           m4, m5
    paddd           m8, m5

    movzx           r4d, word [r2 + 98]                     ; bottomLeft = left[16]
    movd            m6, r4d
    pshufd          m6, m6, 0                               ; bottomLeft

    paddd           m4, m6
    paddd           m3, m6
    paddd           m8, m6
    paddd           m9, m6

    psubd           m1, m6, m0                              ; column 12-15
    psubd           m11, m6, m10                            ; column 8-11
    psubd           m10, m6, m7                             ; column 4-7
    psubd           m6, m2                                  ; column 0-3

    add             r2, 66
    lea             r4, [pd_planar16_mul0]

%macro INTRA_PRED_PLANAR16_sse2 1
    movzx           r3d, word [r2 + %1*2]
    movd            m5, r3d
    pshufd          m5, m5, 0

    pmaddwd         m0, m5, [r4 + 3*mmsize]                 ; column 12-15
    pmaddwd         m2, m5, [r4 + 2*mmsize]                 ; column 8-11
    pmaddwd         m7, m5, [r4 + 1*mmsize]                 ; column 4-7
    pmaddwd         m5, m5, [r4 + 0*mmsize]                 ; column 0-3

    paddd           m0, m8
    paddd           m2, m4
    paddd           m7, m9
    paddd           m5, m3

    paddd           m8, m1
    paddd           m4, m11
    paddd           m9, m10
    paddd           m3, m6

    psrad           m0, 5
    psrad           m2, 5
    psrad           m7, 5
    psrad           m5, 5

    packssdw        m2, m0
    packssdw        m5, m7
    movu            [r0], m5
    movu            [r0 + mmsize], m2

    add             r0, r1
%endmacro

    INTRA_PRED_PLANAR16_sse2 0
    INTRA_PRED_PLANAR16_sse2 1
    INTRA_PRED_PLANAR16_sse2 2
    INTRA_PRED_PLANAR16_sse2 3
    INTRA_PRED_PLANAR16_sse2 4
    INTRA_PRED_PLANAR16_sse2 5
    INTRA_PRED_PLANAR16_sse2 6
    INTRA_PRED_PLANAR16_sse2 7
    INTRA_PRED_PLANAR16_sse2 8
    INTRA_PRED_PLANAR16_sse2 9
    INTRA_PRED_PLANAR16_sse2 10
    INTRA_PRED_PLANAR16_sse2 11
    INTRA_PRED_PLANAR16_sse2 12
    INTRA_PRED_PLANAR16_sse2 13
    INTRA_PRED_PLANAR16_sse2 14
    INTRA_PRED_PLANAR16_sse2 15
    RET

%else
; code for BIT_DEPTH == 10
cglobal intra_pred_planar16, 3,3,8
    movu            m2, [r2 + 2]
    movu            m7, [r2 + 18]

    movd            m3, [r2 + 34]               ; topRight   = above[16]
    movd            m6, [r2 + 98]               ; bottomLeft = left[16]

    pshuflw         m3, m3, 0
    pshuflw         m6, m6, 0
    pshufd          m3, m3, 0                   ; v_topRight
    pshufd          m6, m6, 0                   ; v_bottomLeft

    pmullw          m4, m3, [multiH]            ; (x + 1) * topRight
    pmullw          m3, [multiL]                ; (x + 1) * topRight
    pmullw          m1, m2, [pw_15]             ; (blkSize - 1 - y) * above[x]
    pmullw          m5, m7, [pw_15]             ; (blkSize - 1 - y) * above[x]
    paddw           m4, [pw_16]
    paddw           m3, [pw_16]
    paddw           m4, m6
    paddw           m3, m6
    paddw           m4, m5
    paddw           m3, m1
    psubw           m1, m6, m7
    psubw           m6, m2

    movu            m2, [r2 + 66]
    movu            m7, [r2 + 82]

%macro INTRA_PRED_PLANAR_16 1
%if (%1 < 4)
    pshuflw         m5, m2, 0x55 * %1
    pshufd          m5, m5, 0
%else
%if (%1 < 8)
    pshufhw         m5, m2, 0x55 * (%1 - 4)
    pshufd          m5, m5, 0xAA
%else
%if (%1 < 12)
    pshuflw         m5, m7, 0x55 * (%1 - 8)
    pshufd          m5, m5, 0
%else
    pshufhw         m5, m7, 0x55 * (%1 - 12)
    pshufd          m5, m5, 0xAA
%endif
%endif
%endif
%if (%1 > 0)
    paddw           m3, m6
    paddw           m4, m1
    lea             r0, [r0 + r1 * 2]
%endif
    pmullw          m0, m5, [pw_planar16_mul + mmsize]
    pmullw          m5, [pw_planar16_mul]
    paddw           m0, m4
    paddw           m5, m3
    psraw           m5, 5
    psraw           m0, 5
    movu            [r0], m5
    movu            [r0 + 16], m0
%endmacro

    INTRA_PRED_PLANAR_16 0
    INTRA_PRED_PLANAR_16 1
    INTRA_PRED_PLANAR_16 2
    INTRA_PRED_PLANAR_16 3
    INTRA_PRED_PLANAR_16 4
    INTRA_PRED_PLANAR_16 5
    INTRA_PRED_PLANAR_16 6
    INTRA_PRED_PLANAR_16 7
    INTRA_PRED_PLANAR_16 8
    INTRA_PRED_PLANAR_16 9
    INTRA_PRED_PLANAR_16 10
    INTRA_PRED_PLANAR_16 11
    INTRA_PRED_PLANAR_16 12
    INTRA_PRED_PLANAR_16 13
    INTRA_PRED_PLANAR_16 14
    INTRA_PRED_PLANAR_16 15
    RET
%endif

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse2
%if ARCH_X86_64 == 1 && BIT_DEPTH == 12
cglobal intra_pred_planar32, 3,7,16
    ; NOTE: align stack to 64 bytes, so all of local data in same cache line
    mov             r6, rsp
    sub             rsp, 4*mmsize
    and             rsp, ~63
    %define         m16 [rsp + 0 * mmsize]
    %define         m17 [rsp + 1 * mmsize]
    %define         m18 [rsp + 2 * mmsize]
    %define         m19 [rsp + 3 * mmsize]

    add             r1, r1
    pxor            m12, m12

    movzx           r3d, word [r2 + 66]
    lea             r4, [planar32_table1]

    movd            m0, r3d
    pshufd          m0, m0, 0

    pmaddwd         m8, m0, [r4 + 0]
    pmaddwd         m9, m0, [r4 + 16]
    pmaddwd         m10, m0, [r4 + 32]
    pmaddwd         m11, m0, [r4 + 48]
    pmaddwd         m7, m0, [r4 + 64]
    pmaddwd         m13, m0, [r4 + 80]
    pmaddwd         m14, m0, [r4 + 96]
    pmaddwd         m15, m0, [r4 + 112]

    movzx           r3d, word [r2 + 194]
    movd            m0, r3d
    pshufd          m0, m0, 0

    paddd           m8, m0
    paddd           m9, m0
    paddd           m10, m0
    paddd           m11, m0
    paddd           m7, m0
    paddd           m13, m0
    paddd           m14, m0
    paddd           m15, m0

    paddd           m8, [pd_32]
    paddd           m9, [pd_32]
    paddd           m10, [pd_32]
    paddd           m11, [pd_32]
    paddd           m7, [pd_32]
    paddd           m13, [pd_32]
    paddd           m14, [pd_32]
    paddd           m15, [pd_32]

    movu            m1, [r2 + 2]
    punpckhwd       m5, m1, m12
    pmaddwd         m2, m5, [pd_31]
    paddd           m9, m2
    psubd           m2, m0, m5

    punpcklwd       m1, m12
    pmaddwd         m5, m1, [pd_31]
    paddd           m8, m5
    psubd           m3, m0, m1

    movu            m1, [r2 + 18]
    punpckhwd       m5, m1, m12
    pmaddwd         m4, m5, [pd_31]
    paddd           m11, m4
    psubd           m4, m0, m5

    punpcklwd       m1, m12
    pmaddwd         m5, m1, [pd_31]
    paddd           m10, m5
    psubd           m5, m0, m1
    mova            m16, m5

    movu            m1, [r2 + 34]
    punpckhwd       m6, m1, m12
    psubd           m5, m0, m6
    pmaddwd         m6, [pd_31]
    paddd           m13, m6

    punpcklwd       m6, m1, m12
    psubd           m1, m0, m6
    mova            m17, m1
    pmaddwd         m6, [pd_31]
    paddd           m7, m6

    movu            m1, [r2 + 50]
    mova            m18, m1
    punpckhwd       m6, m1, m12
    psubd           m1, m0, m6
    pmaddwd         m6, [pd_31]
    paddd           m15, m6

    punpcklwd       m6, m18, m12
    psubd           m12, m0, m6
    mova            m19, m12
    pmaddwd         m6, [pd_31]
    paddd           m14, m6

    add             r2, 130
    lea             r5, [planar32_table]

%macro INTRA_PRED_PLANAR32_sse2 0
    movzx           r3d, word [r2]
    movd            m0, r3d
    pshufd          m0, m0, 0

    pmaddwd         m6, m0, [r5]
    pmaddwd         m12, m0, [r5 + 16]
    paddd           m6, m8
    paddd           m12, m9
    paddd           m8, m3
    paddd           m9, m2
    psrad           m6, 6
    psrad           m12, 6
    packssdw        m6, m12
    movu            [r0], m6

    pmaddwd         m6, m0, [r5 + 32]
    pmaddwd         m12, m0, [r5 + 48]
    paddd           m6, m10
    paddd           m12, m11
    paddd           m10, m16
    paddd           m11, m4
    psrad           m6, 6
    psrad           m12, 6
    packssdw        m6, m12
    movu            [r0 + 16], m6

    pmaddwd         m6, m0, [r5 + 64]
    pmaddwd         m12, m0, [r5 + 80]
    paddd           m6, m7
    paddd           m12, m13
    paddd           m7, m17
    paddd           m13, m5
    psrad           m6, 6
    psrad           m12, 6
    packssdw        m6, m12
    movu            [r0 + 32], m6

    pmaddwd         m6, m0, [r5 + 96]
    pmaddwd         m12, m0, [r5 + 112]
    paddd           m6, m14
    paddd           m12, m15
    paddd           m14, m19
    paddd           m15, m1
    psrad           m6, 6
    psrad           m12, 6
    packssdw        m6, m12
    movu            [r0 + 48], m6

    lea             r0, [r0 + r1]
    add             r2, 2
%endmacro

    mov             r4, 8
.loop:
    INTRA_PRED_PLANAR32_sse2
    INTRA_PRED_PLANAR32_sse2
    INTRA_PRED_PLANAR32_sse2
    INTRA_PRED_PLANAR32_sse2
    dec             r4
    jnz             .loop
    mov             rsp, r6
    RET

%else
;code for BIT_DEPTH == 10
;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal intra_pred_planar32, 3,3,16
    movd            m3, [r2 + 66]               ; topRight   = above[32]

    pshuflw         m3, m3, 0x00
    pshufd          m3, m3, 0x44

    pmullw          m0, m3, [multiL]            ; (x + 1) * topRight
    pmullw          m1, m3, [multiH]            ; (x + 1) * topRight
    pmullw          m2, m3, [multiH2]           ; (x + 1) * topRight
    pmullw          m3, [multiH3]               ; (x + 1) * topRight

    movd            m6, [r2 + 194]               ; bottomLeft = left[32]
    pshuflw         m6, m6, 0x00
    pshufd          m6, m6, 0x44
    mova            m5, m6
    paddw           m5, [pw_32]

    paddw           m0, m5
    paddw           m1, m5
    paddw           m2, m5
    paddw           m3, m5
    mova            m8, m6
    mova            m9, m6
    mova            m10, m6

    mova            m12, [pw_31]
    movu            m4, [r2 + 2]
    psubw           m8, m4
    pmullw          m4, m12
    paddw           m0, m4

    movu            m5, [r2 + 18]
    psubw           m9, m5
    pmullw          m5, m12
    paddw           m1, m5

    movu            m4, [r2 + 34]
    psubw           m10, m4
    pmullw          m4, m12
    paddw           m2, m4

    movu            m5, [r2 + 50]
    psubw           m6, m5
    pmullw          m5, m12
    paddw           m3, m5

    mova            m12, [pw_planar32_mul]
    mova            m13, [pw_planar32_mul + mmsize]
    mova            m14, [pw_planar16_mul]
    mova            m15, [pw_planar16_mul + mmsize]
    add             r1, r1

%macro PROCESS 1
    pmullw          m5, %1, m12
    pmullw          m11, %1, m13
    paddw           m5, m0
    paddw           m11, m1
    psrlw           m5, 6
    psrlw           m11, 6
    movu            [r0], m5
    movu            [r0 + 16], m11

    pmullw          m5, %1, m14
    pmullw          %1, m15
    paddw           m5, m2
    paddw           %1, m3
    psrlw           m5, 6
    psrlw           %1, 6
    movu            [r0 + 32], m5
    movu            [r0 + 48], %1
%endmacro

%macro  INCREMENT 0
    paddw           m2, m10
    paddw           m3, m6
    paddw           m0, m8
    paddw           m1, m9
    add             r0, r1
%endmacro

    add             r2, 130             ;130 = 32*sizeof(pixel)*2 + 1*sizeof(pixel)
%assign x 0
%rep 4
    movu            m4, [r2]
    add             r2, 16
%assign y 0
%rep 8
    %if y < 4
    pshuflw         m7, m4, 0x55 * y
    pshufd          m7, m7, 0x44
    %else
    pshufhw         m7, m4, 0x55 * (y - 4)
    pshufd          m7, m7, 0xEE
    %endif
        PROCESS m7
    %if x + y < 10
    INCREMENT
    %endif
%assign y y+1
%endrep
%assign x x+1
%endrep
    RET
%endif

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_YMM avx2
%if ARCH_X86_64 == 1 && BIT_DEPTH == 12
cglobal intra_pred_planar32, 3,4,16
    pmovzxwd        m1, [r2 + 2]
    pmovzxwd        m4, [r2 + 34]
    pmovzxwd        m2, [r2 + 18]
    pmovzxwd        m3, [r2 + 50]
    lea             r2, [r2 + 66]

    movzx           r3d, word [r2]
    movd            xm5, r3d
    vpbroadcastd    m5, xm5

    pslld           m8, m5, 3
    pmulld          m7, m5, [pd_planar16_mul1 + 32]
    psubd           m6, m7, m8
    pmulld          m9, m5, [pd_planar32_mul2 + 32]
    psubd           m8, m9, m8

    movzx           r3d, word [r2 + 128]
    movd            xm10, r3d
    vpbroadcastd    m10, xm10

    mova            m11, m10
    paddd           m11, [pd_32]

    paddd           m6, m11
    paddd           m7, m11
    paddd           m8, m11
    paddd           m9, m11

    psubd           m0, m10, m1
    mova            m13, m0
    pslld           m5, m1, 5
    psubd           m1, m5, m1
    paddd           m12, m6, m1

    psubd           m5, m10, m4
    mova            m6, m5
    pslld           m1, m4, 5
    psubd           m4, m1, m4
    paddd           m14, m8, m4

    psubd           m1, m10, m2
    mova            m8, m1
    pslld           m4, m2, 5
    psubd           m2, m4, m2
    paddd           m7, m2

    psubd           m11, m10, m3
    mova            m15, m11
    pslld           m4, m3, 5
    psubd           m3, m4, m3
    paddd           m9, m3

    mova            m2, [pd_planar32_mul1 + 32]
    mova            m4, [pd_planar16_mul2 + 32]

    add             r1, r1

%macro PROCESS_AVX2 1
    movzx           r3d, word [r2 + %1 * 2]
    movd            xm0, r3d
    vpbroadcastd    m0, xm0

    pmulld          m1, m0, m2
    pslld           m3, m0, 3
    paddd           m5, m1, m3
    pmulld          m0, m4
    paddd           m11, m0, m3

    paddd           m5, m12
    paddd           m1, m7
    paddd           m11, m14
    paddd           m0, m9

    psrad           m5, 6
    psrad           m1, 6
    psrad           m11, 6
    psrad           m0, 6

    packssdw        m5, m1
    packssdw        m11, m0

    vpermq          m5, m5, q3120
    vpermq          m11, m11, q3120

    movu            [r0], m5
    movu            [r0 + mmsize], m11
%endmacro

%macro  INCREMENT_AVX2 0
    paddd           m12, m13
    paddd           m14, m6
    paddd           m7, m8
    paddd           m9, m15

    add             r0, r1
%endmacro

    add             r2, mmsize*2
%assign x 0
%rep 4
%assign y 0
%rep 8
    PROCESS_AVX2 y
%if x + y < 10
    INCREMENT_AVX2
%endif
%assign y y+1
%endrep
lea                 r2, [r2 + 16]
%assign x x+1
%endrep
    RET

%else
; code for BIT_DEPTH == 10
;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
cglobal intra_pred_planar32, 3,3,8
    movu            m1, [r2 + 2]
    movu            m4, [r2 + 34]
    lea             r2, [r2 + 66]
    vpbroadcastw    m3, [r2]                    ; topRight   = above[32]
    pmullw          m0, m3, [multiL]            ; (x + 1) * topRight
    pmullw          m2, m3, [multiH2]           ; (x + 1) * topRight
    vpbroadcastw    m6, [r2 + 128]              ; bottomLeft = left[32]
    mova            m5, m6
    paddw           m5, [pw_32]

    paddw           m0, m5
    paddw           m2, m5
    mova            m5, m6
    psubw           m3, m6, m1
    pmullw          m1, [pw_31]
    paddw           m0, m1
    psubw           m5, m4
    pmullw          m4, [pw_31]
    paddw           m2, m4

    mova            m6, [pw_planar32_mul]
    mova            m4, [pw_planar16_mul]
    add             r1, r1

%macro PROCESS_AVX2 1
    vpbroadcastw    m7, [r2 + %1 * 2]
    pmullw          m1, m7, m6
    pmullw          m7, m4
    paddw           m1, m0
    paddw           m7, m2
    psrlw           m1, 6
    psrlw           m7, 6
    movu            [r0], m1
    movu            [r0 + mmsize], m7
%endmacro

%macro  INCREMENT_AVX2 0
    paddw           m2, m5
    paddw           m0, m3
    add             r0, r1
%endmacro

    add             r2, mmsize*2
%assign x 0
%rep 4
%assign y 0
%rep 8
    PROCESS_AVX2 y
%if x + y < 10
    INCREMENT_AVX2
%endif
%assign y y+1
%endrep
lea     r2, [r2 + 16]
%assign x x+1
%endrep
    RET
%endif

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_YMM avx2
%if ARCH_X86_64 == 1 && BIT_DEPTH == 12
cglobal intra_pred_planar16, 3,3,11
    add             r1d, r1d

    movzx           r4d, word [r2 + 34]
    movd            xm3, r4d
    vpbroadcastd    m3, xm3

    movzx           r4d, word [r2 + 98]
    movd            xm4, r4d
    vpbroadcastd    m4, xm4

    pmovzxwd        m2, [r2 + 2]
    pmovzxwd        m5, [r2 + 18]

    pmulld          m10, m3, [pd_planar16_mul1]
    pmulld          m7, m3, [pd_planar16_mul1 + 32]

    psubd           m10, m2
    pslld           m1, m2, 4
    paddd           m10, m1

    psubd           m7, m5
    pslld           m6, m5, 4
    paddd           m9, m6, m7

    paddd           m10, [pd_16]
    paddd           m9, [pd_16]
    paddd           m7, m10, m4
    paddd           m9, m4

    psubd           m0, m4, m2
    psubd           m8, m4, m5

    add             r2, 66
    mova            m5, [pd_planar16_mul0]
    mova            m6, [pd_planar16_mul0 + 32]
    mova            m10, [pd_0000ffff]

%macro INTRA_PRED_PLANAR16_AVX2 1
    vpbroadcastd    m2, [r2 + %1]
    pand            m1, m2, m10
    psrld           m2, 16

    pmulld          m3, m1, m5
    pmulld          m4, m1, m6
    pmulld          m1, m2, m5
    pmulld          m2, m2, m6

    paddd           m3, m7
    paddd           m4, m9
    paddd           m7, m0
    paddd           m9, m8

    psrad           m3, 5
    psrad           m4, 5

    paddd           m1, m7
    paddd           m2, m9

    psrad           m1, 5
    psrad           m2, 5

    paddd           m7, m0
    paddd           m9, m8

    packssdw        m3, m4
    packssdw        m1, m2

    vpermq          m3, m3, q3120
    vpermq          m1, m1, q3120

    movu            [r0], m3
    movu            [r0 + r1], m1
%if %1 <= 24
    lea             r0, [r0 + r1 * 2]
%endif
%endmacro
    INTRA_PRED_PLANAR16_AVX2 0
    INTRA_PRED_PLANAR16_AVX2 4
    INTRA_PRED_PLANAR16_AVX2 8
    INTRA_PRED_PLANAR16_AVX2 12
    INTRA_PRED_PLANAR16_AVX2 16
    INTRA_PRED_PLANAR16_AVX2 20
    INTRA_PRED_PLANAR16_AVX2 24
    INTRA_PRED_PLANAR16_AVX2 28
%undef INTRA_PRED_PLANAR16_AVX2
    RET

%else
; code for BIT_DEPTH == 10
;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
cglobal intra_pred_planar16, 3,3,4
    add             r1d, r1d
    vpbroadcastw    m3, [r2 + 34]
    vpbroadcastw    m4, [r2 + 98]
    mova            m0, [pw_planar16_mul]
    movu            m2, [r2 + 2]

    pmullw          m3, [multiL]                ; (x + 1) * topRight
    pmullw          m1, m2, [pw_15]             ; (blkSize - 1 - y) * above[x]
    paddw           m3, [pw_16]
    paddw           m3, m4
    paddw           m3, m1
    psubw           m4, m2
    add             r2, 66

%macro INTRA_PRED_PLANAR16_AVX2 1
    vpbroadcastw    m1, [r2 + %1]
    vpbroadcastw    m2, [r2 + %1 + 2]

    pmullw          m1, m0
    pmullw          m2, m0
    paddw           m1, m3
    paddw           m3, m4
    psraw           m1, 5
    paddw           m2, m3
    psraw           m2, 5
    paddw           m3, m4
    movu            [r0], m1
    movu            [r0 + r1], m2
%if %1 <= 24
    lea             r0, [r0 + r1 * 2]
%endif
%endmacro
    INTRA_PRED_PLANAR16_AVX2 0
    INTRA_PRED_PLANAR16_AVX2 4
    INTRA_PRED_PLANAR16_AVX2 8
    INTRA_PRED_PLANAR16_AVX2 12
    INTRA_PRED_PLANAR16_AVX2 16
    INTRA_PRED_PLANAR16_AVX2 20
    INTRA_PRED_PLANAR16_AVX2 24
    INTRA_PRED_PLANAR16_AVX2 28
%undef INTRA_PRED_PLANAR16_AVX2
    RET
%endif

%macro TRANSPOSE_4x4 0
    punpckhwd    m0, m1, m3
    punpcklwd    m1, m3
    punpckhwd    m3, m1, m0
    punpcklwd    m1, m0
%endmacro

%macro STORE_4x4 0
    add         r1, r1
    movh        [r0], m1
    movhps      [r0 + r1], m1
    movh        [r0 + r1 * 2], m3
    lea         r1, [r1 * 3]
    movhps      [r0 + r1], m3
%endmacro

%macro CALC_4x4 4
    mova    m0, [pd_16]
    pmaddwd m1, [ang_table + %1 * 16]
    paddd   m1, m0
    psrld   m1, 5

    pmaddwd m2, [ang_table + %2 * 16]
    paddd   m2, m0
    psrld   m2, 5
    packssdw m1, m2

    pmaddwd m3, [ang_table + %3 * 16]
    paddd   m3, m0
    psrld   m3, 5

    pmaddwd m4, [ang_table + %4 * 16]
    paddd   m4, m0
    psrld   m4, 5
    packssdw m3, m4
%endmacro

;-----------------------------------------------------------------------------------------
; void intraPredAng4(pixel* dst, intptr_t dstStride, pixel* src, int dirMode, int bFilter)
;-----------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal intra_pred_ang4_2, 3,5,4
    lea         r4,            [r2 + 4]
    add         r2,            20
    cmp         r3m,           byte 34
    cmove       r2,            r4

    add         r1,            r1
    movu        m0,            [r2]
    movh        [r0],          m0
    psrldq      m0,            2
    movh        [r0 + r1],     m0
    psrldq      m0,            2
    movh        [r0 + r1 * 2], m0
    lea         r1,            [r1 * 3]
    psrldq      m0,            2
    movh        [r0 + r1],     m0
    RET

cglobal intra_pred_ang4_3, 3,3,5
    movu        m0, [r2 + 18]           ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m0
    psrldq      m0, 2
    punpcklwd   m2, m0                  ;[6 5 5 4 4 3 3 2]
    mova        m3, m0
    psrldq      m0, 2
    punpcklwd   m3, m0                  ;[7 6 6 5 5 4 4 3]
    mova        m4, m0
    psrldq      m0, 2
    punpcklwd   m4, m0                  ;[8 7 7 6 6 5 5 4]

    CALC_4x4 26, 20, 14, 8

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_33, 3,3,5
    movu        m0, [r2 + 2]            ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m0
    psrldq      m0, 2
    punpcklwd   m2, m0                  ;[6 5 5 4 4 3 3 2]
    mova        m3, m0
    psrldq      m0, 2
    punpcklwd   m3, m0                  ;[7 6 6 5 5 4 4 3]
    mova        m4, m0
    psrldq      m0, 2
    punpcklwd   m4, m0                  ;[8 7 7 6 6 5 5 4]

    CALC_4x4 26, 20, 14, 8

    STORE_4x4
    RET

cglobal intra_pred_ang4_4, 3,3,5
    movu        m0, [r2 + 18]           ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m0
    psrldq      m0, 2
    punpcklwd   m2, m0                  ;[6 5 5 4 4 3 3 2]
    mova        m3, m2
    mova        m4, m0
    psrldq      m0, 2
    punpcklwd   m4, m0                  ;[7 6 6 5 5 4 4 3]

    CALC_4x4 21, 10, 31, 20

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_6, 3,3,5
    movu        m0, [r2 + 18]           ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m1
    mova        m3, m0
    psrldq      m0, 2
    punpcklwd   m3, m0                  ;[6 5 5 4 4 3 3 2]
    mova        m4, m3

    CALC_4x4 13, 26, 7, 20

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_7, 3,3,5
    movu        m0, [r2 + 18]           ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m0
    psrldq      m0, 2
    punpcklwd   m4, m0                  ;[6 5 5 4 4 3 3 2]

    CALC_4x4 9, 18, 27, 4

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_8, 3,3,5
    movu        m0, [r2 + 18]           ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m1

    CALC_4x4 5, 10, 15, 20

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_9, 3,3,5
    movu        m0, [r2 + 18]           ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m1

    CALC_4x4 2, 4, 6, 8

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_10, 3,3,3
    movh        m0,             [r2 + 18] ;[4 3 2 1]

    punpcklwd   m0,             m0      ;[4 4 3 3 2 2 1 1]
    pshufd      m1,             m0, 0xFA
    add         r1d,            r1d
    pshufd      m0,             m0, 0x50
    movhps      [r0 + r1],      m0
    movh        [r0 + r1 * 2],  m1
    lea         r1d,            [r1 * 3]
    movhps      [r0 + r1],      m1

    cmp         r4m,            byte 0
    jz         .quit

    ; filter
    movd        m2,             [r2]    ;[7 6 5 4 3 2 1 0]
    pshuflw     m2,             m2, 0x00
    movh        m1,             [r2 + 2]
    psubw       m1,             m2
    psraw       m1,             1
    paddw       m0,             m1
    pxor        m1,             m1
    pmaxsw      m0,             m1
    pminsw      m0,             [pw_pixel_max]
.quit:
    movh        [r0],           m0
    RET

cglobal intra_pred_ang4_11, 3,3,5
    movh        m0, [r2 + 18]           ;[x x x 4 3 2 1 0]
    movh        m1, [r2 - 6]
    punpcklqdq  m1, m0
    psrldq      m1, 6
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m1

    CALC_4x4 30, 28, 26, 24

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_12, 3,3,5
    movh        m0, [r2 + 18]
    movh        m1, [r2 - 6]
    punpcklqdq  m1, m0
    psrldq      m1, 6
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m1

    CALC_4x4 27, 22, 17, 12

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_13, 3,3,5
    movd        m4, [r2 + 6]
    movd        m1, [r2 - 2]
    movh        m0, [r2 + 18]
    punpcklwd   m4, m1
    punpcklqdq  m4, m0
    psrldq      m4, 4
    mova        m1, m4
    psrldq      m1, 2
    punpcklwd   m4, m1                  ;[3 2 2 1 1 0 0 x]
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m2, m1
    mova        m3, m1

    CALC_4x4 23, 14, 5, 28

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_14, 3,3,5
    movd        m4, [r2 + 2]
    movd        m1, [r2 - 2]
    movh        m0, [r2 + 18]
    punpcklwd   m4, m1
    punpcklqdq  m4, m0
    psrldq      m4, 4
    mova        m1, m4
    psrldq      m1, 2
    punpcklwd   m4, m1                  ;[3 2 2 1 1 0 0 x]
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m2, m1
    mova        m3, m4

    CALC_4x4 19, 6, 25, 12

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_15, 3,3,5
    movd        m3, [r2]                ;[x x x A]
    movh        m4, [r2 + 4]            ;[x C x B]
    movh        m0, [r2 + 18]           ;[4 3 2 1]
    pshuflw     m4, m4, 0x22            ;[B C B C]
    punpcklqdq  m4, m3                  ;[x x x A B C B C]
    psrldq      m4, 2                   ;[x x x x A B C B]
    punpcklqdq  m4, m0
    psrldq      m4, 2
    mova        m1, m4
    mova        m2, m4
    psrldq      m1, 4
    psrldq      m2, 2
    punpcklwd   m4, m2                  ;[2 1 1 0 0 x x y]
    punpcklwd   m2, m1                  ;[3 2 2 1 1 0 0 x]
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m3, m2

    CALC_4x4 15, 30, 13, 28

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_16, 3,3,5
    movd        m3, [r2]                ;[x x x A]
    movd        m4, [r2 + 4]            ;[x x C B]
    movh        m0, [r2 + 18]           ;[4 3 2 1]
    punpcklwd   m4, m3                  ;[x C A B]
    pshuflw     m4, m4, 0x4A            ;[A B C C]
    punpcklqdq  m4, m0                  ;[4 3 2 1 A B C C]
    psrldq      m4, 2
    mova        m1, m4
    mova        m2, m4
    psrldq      m1, 4
    psrldq      m2, 2
    punpcklwd   m4, m2                  ;[2 1 1 0 0 x x y]
    punpcklwd   m2, m1                  ;[3 2 2 1 1 0 0 x]
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m3, m2

    CALC_4x4 11, 22, 1, 12

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_17, 3,3,5
    movd        m3, [r2]
    movh        m4, [r2 + 2]            ;[D x C B]
    pshuflw     m4, m4, 0x1F            ;[B C D D]
    punpcklqdq  m4, m3                  ;[x x x A B C D D]
    psrldq      m4, 2                   ;[x x x x A B C D]
    movhps      m4, [r2 + 18]

    mova        m3, m4
    psrldq      m3, 2
    punpcklwd   m4, m3
    mova        m2, m3
    psrldq      m2, 2
    punpcklwd   m3, m2
    mova        m1, m2
    psrldq      m1, 2
    punpcklwd   m2, m1
    mova        m0, m1
    psrldq      m0, 2
    punpcklwd   m1, m0

    CALC_4x4 6, 12, 18, 24

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_18, 3,3,1
    movh        m0, [r2 + 16]
    pinsrw      m0, [r2], 0
    pshuflw     m0, m0, q0123
    movhps      m0, [r2 + 2]
    add         r1, r1
    lea         r2, [r1 * 3]
    movh        [r0 + r2], m0
    psrldq      m0, 2
    movh        [r0 + r1 * 2], m0
    psrldq      m0, 2
    movh        [r0 + r1], m0
    psrldq      m0, 2
    movh        [r0], m0
    RET

    cglobal intra_pred_ang4_19, 3,3,5
    movd        m3, [r2]
    movh        m4, [r2 + 18]           ;[D x C B]
    pshuflw     m4, m4, 0x1F            ;[B C D D]
    punpcklqdq  m4, m3                  ;[x x x A B C D D]
    psrldq      m4, 2                   ;[x x x x A B C D]
    movhps      m4, [r2 + 2]

    mova        m3, m4
    psrldq      m3, 2
    punpcklwd   m4, m3
    mova        m2, m3
    psrldq      m2, 2
    punpcklwd   m3, m2
    mova        m1, m2
    psrldq      m1, 2
    punpcklwd   m2, m1
    mova        m0, m1
    psrldq      m0, 2
    punpcklwd   m1, m0

    CALC_4x4 6, 12, 18, 24

    STORE_4x4
    RET

cglobal intra_pred_ang4_20, 3,3,5
    movd        m3, [r2]                ;[x x x A]
    movd        m4, [r2 + 20]           ;[x x C B]
    movh        m0, [r2 + 2]            ;[4 3 2 1]
    punpcklwd   m4, m3                  ;[x C A B]
    pshuflw     m4, m4, 0x4A            ;[A B C C]
    punpcklqdq  m4, m0                  ;[4 3 2 1 A B C C]
    psrldq      m4, 2
    mova        m1, m4
    mova        m2, m4
    psrldq      m1, 4
    psrldq      m2, 2
    punpcklwd   m4, m2                  ;[2 1 1 0 0 x x y]
    punpcklwd   m2, m1                  ;[3 2 2 1 1 0 0 x]
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m3, m2

    CALC_4x4 11, 22, 1, 12

    STORE_4x4
    RET

cglobal intra_pred_ang4_21, 3,3,5
    movd        m3, [r2]                ;[x x x A]
    movh        m4, [r2 + 20]           ;[x C x B]
    movh        m0, [r2 + 2]            ;[4 3 2 1]
    pshuflw     m4, m4, 0x22            ;[B C B C]
    punpcklqdq  m4, m3                  ;[x x x A B C B C]
    psrldq      m4, 2                   ;[x x x x A B C B]
    punpcklqdq  m4, m0
    psrldq      m4, 2
    mova        m1, m4
    mova        m2, m4
    psrldq      m1, 4
    psrldq      m2, 2
    punpcklwd   m4, m2                  ;[2 1 1 0 0 x x y]
    punpcklwd   m2, m1                  ;[3 2 2 1 1 0 0 x]
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m3, m2

    CALC_4x4 15, 30, 13, 28

    STORE_4x4
    RET

cglobal intra_pred_ang4_22, 3,3,5
    movd        m4, [r2 + 18]
    movd        m1, [r2 - 2]
    movh        m0, [r2 + 2]
    punpcklwd   m4, m1
    punpcklqdq  m4, m0
    psrldq      m4, 4
    mova        m1, m4
    psrldq      m1, 2
    punpcklwd   m4, m1                  ;[3 2 2 1 1 0 0 x]
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m2, m1
    mova        m3, m4

    CALC_4x4 19, 6, 25, 12

    STORE_4x4
    RET

cglobal intra_pred_ang4_23, 3,3,5
    movd        m4, [r2 + 22]
    movd        m1, [r2 - 2]
    movh        m0, [r2 + 2]
    punpcklwd   m4, m1
    punpcklqdq  m4, m0
    psrldq      m4, 4
    mova        m1, m4
    psrldq      m1, 2
    punpcklwd   m4, m1                  ;[3 2 2 1 1 0 0 x]
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m2, m1
    mova        m3, m1

    CALC_4x4 23, 14, 5, 28

    STORE_4x4
    RET

cglobal intra_pred_ang4_24, 3,3,5
    movh        m0, [r2 + 2]
    movh        m1, [r2 - 6]
    punpcklqdq  m1, m0
    psrldq      m1, 6
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m1

    CALC_4x4 27, 22, 17, 12

    STORE_4x4
    RET

cglobal intra_pred_ang4_25, 3,3,5
    movh        m0, [r2 + 2]            ;[x x x 4 3 2 1 0]
    movh        m1, [r2 - 6]
    punpcklqdq  m1, m0
    psrldq      m1, 6
    punpcklwd   m1, m0                  ;[4 3 3 2 2 1 1 0]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m1

    CALC_4x4 30, 28, 26, 24

    STORE_4x4
    RET

cglobal intra_pred_ang4_26, 3,3,3
    movh        m0,             [r2 + 2] ;[8 7 6 5 4 3 2 1]
    add         r1d,            r1d
    ; store
    movh        [r0],           m0
    movh        [r0 + r1],      m0
    movh        [r0 + r1 * 2],  m0
    lea         r3,             [r1 * 3]
    movh        [r0 + r3],      m0

    ; filter
    cmp         r4m,            byte 0
    jz         .quit

    pshuflw     m0,             m0, 0x00
    movd        m2,             [r2]
    pshuflw     m2,             m2, 0x00
    movh        m1,             [r2 + 18]
    psubw       m1,             m2
    psraw       m1,             1
    paddw       m0,             m1
    pxor        m1,             m1
    pmaxsw      m0,             m1
    pminsw      m0,             [pw_pixel_max]

    movh        r2,             m0
    mov         [r0],           r2w
    shr         r2,             16
    mov         [r0 + r1],      r2w
    shr         r2,             16
    mov         [r0 + r1 * 2],  r2w
    shr         r2,             16
    mov         [r0 + r3],      r2w
.quit:
    RET

cglobal intra_pred_ang4_27, 3,3,5
    movu        m0, [r2 + 2]            ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m1

    CALC_4x4 2, 4, 6, 8

    STORE_4x4
    RET

cglobal intra_pred_ang4_28, 3,3,5

    movu        m0, [r2 + 2]            ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m1

    CALC_4x4 5, 10, 15, 20

    STORE_4x4
    RET

cglobal intra_pred_ang4_29, 3,3,5
    movu        m0, [r2 + 2]            ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m1
    mova        m3, m1
    mova        m4, m0
    psrldq      m0, 2
    punpcklwd   m4, m0                  ;[6 5 5 4 4 3 3 2]

    CALC_4x4 9, 18, 27, 4

    STORE_4x4
    RET

cglobal intra_pred_ang4_30, 3,3,5
    movu        m0, [r2 + 2]            ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m1
    mova        m3, m0
    psrldq      m0, 2
    punpcklwd   m3, m0                  ;[6 5 5 4 4 3 3 2]
    mova        m4, m3

    CALC_4x4 13, 26, 7, 20

    STORE_4x4
    RET

cglobal intra_pred_ang4_5, 3,3,5
    movu        m0, [r2 + 18]           ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m0
    psrldq      m0, 2
    punpcklwd   m2, m0                  ;[6 5 5 4 4 3 3 2]
    mova        m3, m2
    mova        m4, m0
    psrldq      m0, 2
    punpcklwd   m4, m0                  ;[7 6 6 5 5 4 4 3]

    CALC_4x4 17, 2, 19, 4

    TRANSPOSE_4x4

    STORE_4x4
    RET

cglobal intra_pred_ang4_31, 3,3,5
    movu        m0, [r2 + 2]            ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m0
    psrldq      m0, 2
    punpcklwd   m2, m0                  ;[6 5 5 4 4 3 3 2]
    mova        m3, m2
    mova        m4, m0
    psrldq      m0, 2
    punpcklwd   m4, m0                  ;[7 6 6 5 5 4 4 3]

    CALC_4x4 17, 2, 19, 4

    STORE_4x4
    RET

    cglobal intra_pred_ang4_32, 3,3,5
    movu        m0, [r2 + 2]            ;[8 7 6 5 4 3 2 1]
    mova        m1, m0
    psrldq      m0, 2
    punpcklwd   m1, m0                  ;[5 4 4 3 3 2 2 1]
    mova        m2, m0
    psrldq      m0, 2
    punpcklwd   m2, m0                  ;[6 5 5 4 4 3 3 2]
    mova        m3, m2
    mova        m4, m0
    psrldq      m0, 2
    punpcklwd   m4, m0                  ;[7 6 6 5 5 4 4 3]

    CALC_4x4 21, 10, 31, 20

    STORE_4x4
    RET

;-----------------------------------------------------------------------------------
; void intra_pred_dc(pixel* dst, intptr_t dstStride, pixel* above, int, int filter)
;-----------------------------------------------------------------------------------
INIT_XMM sse4
cglobal intra_pred_dc4, 5,6,2
    lea         r3,             [r2 + 18]
    add         r2,             2

    movh        m0,             [r3]           ; sumAbove
    movh        m1,             [r2]           ; sumLeft

    paddw       m0,             m1
    pshufd      m1,             m0, 1
    paddw       m0,             m1
    phaddw      m0,             m0             ; m0 = sum

    test        r4d,            r4d

    pmulhrsw    m0,             [pw_4096]      ; m0 = (sum + 4) / 8
    movd        r4d,            m0             ; r4d = dc_val
    movzx       r4d,            r4w
    pshuflw     m0,             m0, 0          ; m0 = word [dc_val ...]

    ; store DC 4x4
    movh        [r0],           m0
    movh        [r0 + r1 * 2],  m0
    movh        [r0 + r1 * 4],  m0
    lea         r5,             [r0 + r1 * 4]
    movh        [r5 + r1 * 2],  m0

    ; do DC filter
    jz          .end
    lea         r5d,            [r4d * 2 + 2]  ; r5d = DC * 2 + 2
    add         r4d,            r5d            ; r4d = DC * 3 + 2
    movd        m0,             r4d
    pshuflw     m0,             m0, 0          ; m0 = pixDCx3

    ; filter top
    movu        m1,             [r2]
    paddw       m1,             m0
    psrlw       m1,             2
    movh        [r0],           m1             ; overwrite top-left pixel, we will update it later

    ; filter top-left
    movzx       r4d, word       [r3]
    add         r5d,            r4d
    movzx       r4d, word       [r2]
    add         r4d,            r5d
    shr         r4d,            2
    mov         [r0],           r4w

    ; filter left
    lea         r0,             [r0 + r1 * 2]
    movu        m1,             [r3 + 2]
    paddw       m1,             m0
    psrlw       m1,             2
    movd        r3d,            m1
    mov         [r0],           r3w
    shr         r3d,            16
    mov         [r0 + r1 * 2],  r3w
    pextrw      [r0 + r1 * 4],  m1, 2
.end:
    RET

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal intra_pred_planar4, 3,3,5
    movu            m1, [r2 + 2]
    movu            m2, [r2 + 18]
    pshufhw         m3, m1, 0               ; topRight
    pshufd          m3, m3, 0xAA
    pshufhw         m4, m2, 0               ; bottomLeft
    pshufd          m4, m4, 0xAA

    pmullw          m3, [multi_2Row]        ; (x + 1) * topRight
    pmullw          m0, m1, [pw_3]          ; (blkSize - 1 - y) * above[x]

    paddw           m3, [pw_4]
    paddw           m3, m4
    paddw           m3, m0
    psubw           m4, m1

    pshuflw         m1, m2, 0
    pmullw          m1, [pw_planar4_0]
    paddw           m1, m3
    paddw           m3, m4
    psraw           m1, 3
    movh            [r0], m1

    pshuflw         m1, m2, 01010101b
    pmullw          m1, [pw_planar4_0]
    paddw           m1, m3
    paddw           m3, m4
    psraw           m1, 3
    movh            [r0 + r1 * 2], m1
    lea             r0, [r0 + 4 * r1]

    pshuflw         m1, m2, 10101010b
    pmullw          m1, [pw_planar4_0]
    paddw           m1, m3
    paddw           m3, m4
    psraw           m1, 3
    movh            [r0], m1

    pshuflw         m1, m2, 11111111b
    pmullw          m1, [pw_planar4_0]
    paddw           m1, m3
    psraw           m1, 3
    movh            [r0 + r1 * 2], m1
    RET

;-----------------------------------------------------------------------------------
; void intra_pred_dc(pixel* dst, intptr_t dstStride, pixel* above, int, int filter)
;-----------------------------------------------------------------------------------
INIT_XMM sse4
cglobal intra_pred_dc8, 5, 7, 2
    lea             r3, [r2 + 34]
    add             r2,            2
    add             r1,            r1
    movu            m0,            [r3]
    movu            m1,            [r2]

    paddw           m0,            m1
    movhlps         m1,            m0
    paddw           m0,            m1
    phaddw          m0,            m0
    pmaddwd         m0,            [pw_1]

    movd            r5d,           m0
    add             r5d,           8
    shr             r5d,           4              ; sum = sum / 16
    movd            m1,            r5d
    pshuflw         m1,            m1, 0          ; m1 = word [dc_val ...]
    pshufd          m1,            m1, 0

    test            r4d,           r4d

    ; store DC 8x8
    mov             r6,            r0
    movu            [r0],          m1
    movu            [r0 + r1],     m1
    movu            [r0 + r1 * 2], m1
    lea             r0,            [r0 + r1 * 2]
    movu            [r0 + r1],     m1
    movu            [r0 + r1 * 2], m1
    lea             r0,            [r0 + r1 * 2]
    movu            [r0 + r1],     m1
    movu            [r0 + r1 * 2], m1
    lea             r0,            [r0 + r1 * 2]
    movu            [r0 + r1],     m1

    ; Do DC Filter
    jz              .end
    lea             r4d,           [r5d * 2 + 2]  ; r4d = DC * 2 + 2
    add             r5d,           r4d            ; r5d = DC * 3 + 2
    movd            m1,            r5d
    pshuflw         m1,            m1, 0          ; m1 = pixDCx3
    pshufd          m1,            m1, 0

    ; filter top
    movu            m0,            [r2]
    paddw           m0,            m1
    psrlw           m0,            2
    movu            [r6],          m0

    ; filter top-left
    movzx           r5d, word      [r3]
    add             r4d,           r5d
    movzx           r5d, word      [r2]
    add             r5d,           r4d
    shr             r5d,           2
    mov             [r6],          r5w

    ; filter left
    add             r6,            r1
    movu            m0,            [r3 + 2]
    paddw           m0,            m1
    psrlw           m0,            2
    pextrw          [r6],          m0, 0
    pextrw          [r6 + r1],     m0, 1
    pextrw          [r6 + r1 * 2], m0, 2
    lea             r6,            [r6 + r1 * 2]
    pextrw          [r6 + r1],     m0, 3
    pextrw          [r6 + r1 * 2], m0, 4
    lea             r6,            [r6 + r1 * 2]
    pextrw          [r6 + r1],     m0, 5
    pextrw          [r6 + r1 * 2], m0, 6
.end:
    RET

;-------------------------------------------------------------------------------------------------------
; void intra_pred_dc(pixel* dst, intptr_t dstStride, pixel* left, pixel* above, int dirMode, int filter)
;-------------------------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal intra_pred_dc16, 5, 7, 4
    lea             r3,                  [r2 + 66]
    add             r2,                  2
    add             r1,                  r1
    movu            m0,                  [r3]
    movu            m1,                  [r3 + 16]
    movu            m2,                  [r2]
    movu            m3,                  [r2 + 16]

    paddw           m0,                  m1                     ; dynamic range 13 bits
    paddw           m2,                  m3
    paddw           m0,                  m2                     ; dynamic range 14 bits
    movhlps         m1,                  m0                     ; dynamic range 15 bits
    paddw           m0,                  m1                     ; dynamic range 16 bits
    pmaddwd         m0,                  [pw_1]
    phaddd          m0,                  m0

    movd            r5d,                 m0
    add             r5d,                 16
    shr             r5d,                 5     ; sum = sum / 16
    movd            m1,                  r5d
    pshuflw         m1,                  m1, 0 ; m1 = word [dc_val ...]
    pshufd          m1,                  m1, 0

    test            r4d,                 r4d

    ; store DC 16x16
    mov             r6,                  r0
    movu            [r0],                m1
    movu            [r0 + 16],           m1
    movu            [r0 + r1],           m1
    movu            [r0 + 16 + r1],      m1
    lea             r0,                  [r0 + r1 * 2]
    movu            [r0],                m1
    movu            [r0 + 16],           m1
    movu            [r0 + r1],           m1
    movu            [r0 + 16 + r1],      m1
    lea             r0,                  [r0 + r1 * 2]
    movu            [r0],                m1
    movu            [r0 + 16],           m1
    movu            [r0 + r1],           m1
    movu            [r0 + 16 + r1],      m1
    lea             r0,                  [r0 + r1 * 2]
    movu            [r0],                m1
    movu            [r0 + 16],           m1
    movu            [r0 + r1],           m1
    movu            [r0 + 16 + r1],      m1
    lea             r0,                  [r0 + r1 * 2]
    movu            [r0],                m1
    movu            [r0 + 16],           m1
    movu            [r0 + r1],           m1
    movu            [r0 + 16 + r1],      m1
    lea             r0,                  [r0 + r1 * 2]
    movu            [r0],                m1
    movu            [r0 + 16],           m1
    movu            [r0 + r1],           m1
    movu            [r0 + 16 + r1],      m1
    lea             r0,                  [r0 + r1 * 2]
    movu            [r0],                m1
    movu            [r0 + 16],           m1
    movu            [r0 + r1],           m1
    movu            [r0 + 16 + r1],      m1
    lea             r0,                  [r0 + r1 * 2]
    movu            [r0],                m1
    movu            [r0 + 16],           m1
    movu            [r0 + r1],           m1
    movu            [r0 + 16 + r1],      m1

    ; Do DC Filter
    jz              .end
    lea             r4d,                 [r5d * 2 + 2]  ; r4d = DC * 2 + 2
    add             r5d,                 r4d            ; r5d = DC * 3 + 2
    movd            m1,                  r5d
    pshuflw         m1,                  m1, 0          ; m1 = pixDCx3
    pshufd          m1,                  m1, 0

    ; filter top
    movu            m2,                  [r2]
    paddw           m2,                  m1
    psrlw           m2,                  2
    movu            [r6],                m2
    movu            m3,                  [r2 + 16]
    paddw           m3,                  m1
    psrlw           m3,                  2
    movu            [r6 + 16],           m3

    ; filter top-left
    movzx           r5d, word            [r3]
    add             r4d,                 r5d
    movzx           r5d, word            [r2]
    add             r5d,                 r4d
    shr             r5d,                 2
    mov             [r6],                r5w

    ; filter left
    add             r6,                  r1
    movu            m2,                  [r3 + 2]
    paddw           m2,                  m1
    psrlw           m2,                  2

    pextrw          [r6],                m2, 0
    pextrw          [r6 + r1],           m2, 1
    lea             r6,                  [r6 + r1 * 2]
    pextrw          [r6],                m2, 2
    pextrw          [r6 + r1],           m2, 3
    lea             r6,                  [r6 + r1 * 2]
    pextrw          [r6],                m2, 4
    pextrw          [r6 + r1],           m2, 5
    lea             r6,                  [r6 + r1 * 2]
    pextrw          [r6],                m2, 6
    pextrw          [r6 + r1],           m2, 7

    lea             r6,                  [r6 + r1 * 2]
    movu            m3,                  [r3 + 18]
    paddw           m3,                  m1
    psrlw           m3,                  2

    pextrw          [r6],                m3, 0
    pextrw          [r6 + r1],           m3, 1
    lea             r6,                  [r6 + r1 * 2]
    pextrw          [r6],                m3, 2
    pextrw          [r6 + r1],           m3, 3
    lea             r6,                  [r6 + r1 * 2]
    pextrw          [r6],                m3, 4
    pextrw          [r6 + r1],           m3, 5
    lea             r6,                  [r6 + r1 * 2]
    pextrw          [r6],                m3, 6
.end:
    RET

;-------------------------------------------------------------------------------------------
; void intra_pred_dc(pixel* above, pixel* left, pixel* dst, intptr_t dstStride, int filter)
;-------------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal intra_pred_dc32, 3, 5, 6
    lea             r3,                  [r2 + 130]     ;130 = 32*sizeof(pixel)*2 + 1*sizeof(pixel)
    add             r2,                  2
    add             r1,                  r1
    movu            m0,                  [r3]
    movu            m1,                  [r3 + 16]
    movu            m2,                  [r3 + 32]
    movu            m3,                  [r3 + 48]
    paddw           m0,                  m1             ; dynamic range 13 bits
    paddw           m2,                  m3
    paddw           m0,                  m2             ; dynamic range 14 bits
    movu            m1,                  [r2]
    movu            m3,                  [r2 + 16]
    movu            m4,                  [r2 + 32]
    movu            m5,                  [r2 + 48]
    paddw           m1,                  m3             ; dynamic range 13 bits
    paddw           m4,                  m5
    paddw           m1,                  m4             ; dynamic range 14 bits
    paddw           m0,                  m1             ; dynamic range 15 bits
    pmaddwd         m0,                  [pw_1]
    movhlps         m1,                  m0
    paddd           m0,                  m1
    phaddd          m0,                  m0

    paddd           m0,                  [pd_32]     ; sum = sum + 32
    psrld           m0,                  6           ; sum = sum / 64
    pshuflw         m0,                  m0, 0
    pshufd          m0,                  m0, 0

    lea             r2,                 [r1 * 3]
    mov             r3d,                4
.loop:
    ; store DC 32x32
    movu            [r0 +  0],          m0
    movu            [r0 + 16],          m0
    movu            [r0 + 32],          m0
    movu            [r0 + 48],          m0
    movu            [r0 + r1 +  0],     m0
    movu            [r0 + r1 + 16],     m0
    movu            [r0 + r1 + 32],     m0
    movu            [r0 + r1 + 48],     m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + 16], m0
    movu            [r0 + r1 * 2 + 32], m0
    movu            [r0 + r1 * 2 + 48], m0
    movu            [r0 + r2 +  0],     m0
    movu            [r0 + r2 + 16],     m0
    movu            [r0 + r2 + 32],     m0
    movu            [r0 + r2 + 48],     m0
    lea             r0, [r0 + r1 * 4]
    movu            [r0 +  0],          m0
    movu            [r0 + 16],          m0
    movu            [r0 + 32],          m0
    movu            [r0 + 48],          m0
    movu            [r0 + r1 +  0],     m0
    movu            [r0 + r1 + 16],     m0
    movu            [r0 + r1 + 32],     m0
    movu            [r0 + r1 + 48],     m0
    movu            [r0 + r1 * 2 +  0], m0
    movu            [r0 + r1 * 2 + 16], m0
    movu            [r0 + r1 * 2 + 32], m0
    movu            [r0 + r1 * 2 + 48], m0
    movu            [r0 + r2 +  0],     m0
    movu            [r0 + r2 + 16],     m0
    movu            [r0 + r2 + 32],     m0
    movu            [r0 + r2 + 48],     m0
    lea             r0, [r0 + r1 * 4]
    dec             r3d
    jnz            .loop
    RET

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal intra_pred_planar4, 3,3,5
    add             r1, r1
    movu            m1, [r2 + 2]
    movu            m2, [r2 + 18]
    pshufhw         m3, m1, 0               ; topRight
    pshufd          m3, m3, 0xAA
    pshufhw         m4, m2, 0               ; bottomLeft
    pshufd          m4, m4, 0xAA

    pmullw          m3, [multi_2Row]        ; (x + 1) * topRight
    pmullw          m0, m1, [pw_3]          ; (blkSize - 1 - y) * above[x]

    paddw           m3, [pw_4]
    paddw           m3, m4
    paddw           m3, m0
    psubw           m4, m1
    mova            m0, [pw_planar4_0]

    pshuflw         m1, m2, 0
    pmullw          m1, m0
    paddw           m1, m3
    paddw           m3, m4
    psraw           m1, 3
    movh            [r0], m1

    pshuflw         m1, m2, 01010101b
    pmullw          m1, m0
    paddw           m1, m3
    paddw           m3, m4
    psraw           m1, 3
    movh            [r0 + r1], m1
    lea             r0, [r0 + 2 * r1]

    pshuflw         m1, m2, 10101010b
    pmullw          m1, m0
    paddw           m1, m3
    paddw           m3, m4
    psraw           m1, 3
    movh            [r0], m1

    pshuflw         m1, m2, 11111111b
    pmullw          m1, m0
    paddw           m1, m3
    paddw           m3, m4
    psraw           m1, 3
    movh            [r0 + r1], m1
    RET

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal intra_pred_planar8, 3,3,5
    add             r1, r1
    movu            m1, [r2 + 2]
    movu            m2, [r2 + 34]

    movd            m3, [r2 + 18]           ; topRight   = above[8];
    movd            m4, [r2 + 50]           ; bottomLeft = left[8];

    pshuflw         m3, m3, 0
    pshuflw         m4, m4, 0
    pshufd          m3, m3, 0               ; v_topRight
    pshufd          m4, m4, 0               ; v_bottomLeft

    pmullw          m3, [multiL]            ; (x + 1) * topRight
    pmullw          m0, m1, [pw_7]          ; (blkSize - 1 - y) * above[x]
    paddw           m3, [pw_8]
    paddw           m3, m4
    paddw           m3, m0
    psubw           m4, m1
    mova            m0, [pw_planar16_mul + mmsize]

%macro INTRA_PRED_PLANAR8 1
%if (%1 < 4)
    pshuflw         m1, m2, 0x55 * %1
    pshufd          m1, m1, 0
%else
    pshufhw         m1, m2, 0x55 * (%1 - 4)
    pshufd          m1, m1, 0xAA
%endif
    pmullw          m1, m0
    paddw           m1, m3
    paddw           m3, m4
    psrlw           m1, 4
    movu            [r0], m1
    lea             r0, [r0 + r1]
%endmacro

    INTRA_PRED_PLANAR8 0
    INTRA_PRED_PLANAR8 1
    INTRA_PRED_PLANAR8 2
    INTRA_PRED_PLANAR8 3
    INTRA_PRED_PLANAR8 4
    INTRA_PRED_PLANAR8 5
    INTRA_PRED_PLANAR8 6
    INTRA_PRED_PLANAR8 7
    RET

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse4
%if ARCH_X86_64 == 1 && BIT_DEPTH == 12
cglobal intra_pred_planar16, 3,5,12
    add             r1d, r1d

    pmovzxwd        m2, [r2 + 2]
    pmovzxwd        m7, [r2 + 10]
    pmovzxwd        m10, [r2 + 18]
    pmovzxwd        m0, [r2 + 26]

    movzx           r3d, word [r2 + 34]                     ; topRight   = above[16]
    lea             r4, [pd_planar16_mul1]

    movd            m3, r3d
    pshufd          m3, m3, 0                               ; topRight

    pslld           m8, m3, 2
    pmulld          m3, m3, [r4 + 0*mmsize]                 ; (x + 1) * topRight
    paddd           m9, m3, m8
    paddd           m4, m9, m8
    paddd           m8, m4

    pslld           m1, m2, 4
    pslld           m6, m7, 4
    pslld           m5, m10, 4
    pslld           m11, m0, 4
    psubd           m1, m2
    psubd           m6, m7
    psubd           m5, m10
    psubd           m11, m0

    paddd           m4, m5
    paddd           m3, m1
    paddd           m8, m11
    paddd           m9, m6

    mova            m5, [pd_16]
    paddd           m3, m5
    paddd           m9, m5
    paddd           m4, m5
    paddd           m8, m5

    movzx           r4d, word [r2 + 98]                     ; bottomLeft = left[16]
    movd            m6, r4d
    pshufd          m6, m6, 0                               ; bottomLeft

    paddd           m4, m6
    paddd           m3, m6
    paddd           m8, m6
    paddd           m9, m6

    psubd           m1, m6, m0                              ; column 12-15
    psubd           m11, m6, m10                            ; column 8-11
    psubd           m10, m6, m7                             ; column 4-7
    psubd           m6, m2                                  ; column 0-3

    add             r2, 66
    lea             r4, [pd_planar16_mul0]

%macro INTRA_PRED_PLANAR16 1
    movzx           r3d, word [r2]
    movd            m5, r3d
    pshufd          m5, m5, 0

    pmulld          m0, m5, [r4 + 3*mmsize]                 ; column 12-15
    pmulld          m2, m5, [r4 + 2*mmsize]                 ; column 8-11
    pmulld          m7, m5, [r4 + 1*mmsize]                 ; column 4-7
    pmulld          m5, m5, [r4 + 0*mmsize]                 ; column 0-3

    paddd           m0, m8
    paddd           m2, m4
    paddd           m7, m9
    paddd           m5, m3

    paddd           m8, m1
    paddd           m4, m11
    paddd           m9, m10
    paddd           m3, m6

    psrad           m0, 5
    psrad           m2, 5
    psrad           m7, 5
    psrad           m5, 5

    packusdw        m2, m0
    packusdw        m5, m7
    movu            [r0], m5
    movu            [r0 + mmsize], m2

    add             r2, 2
    lea             r0, [r0 + r1]
%endmacro

    INTRA_PRED_PLANAR16 0
    INTRA_PRED_PLANAR16 1
    INTRA_PRED_PLANAR16 2
    INTRA_PRED_PLANAR16 3
    INTRA_PRED_PLANAR16 4
    INTRA_PRED_PLANAR16 5
    INTRA_PRED_PLANAR16 6
    INTRA_PRED_PLANAR16 7
    INTRA_PRED_PLANAR16 8
    INTRA_PRED_PLANAR16 9
    INTRA_PRED_PLANAR16 10
    INTRA_PRED_PLANAR16 11
    INTRA_PRED_PLANAR16 12
    INTRA_PRED_PLANAR16 13
    INTRA_PRED_PLANAR16 14
    INTRA_PRED_PLANAR16 15
    RET

%else
; code for BIT_DEPTH == 10
cglobal intra_pred_planar16, 3,3,8
    add             r1, r1
    movu            m2, [r2 + 2]
    movu            m7, [r2 + 18]

    movd            m3, [r2 + 34]               ; topRight   = above[16]
    movd            m6, [r2 + 98]               ; bottomLeft = left[16]

    pshuflw         m3, m3, 0
    pshuflw         m6, m6, 0
    pshufd          m3, m3, 0                   ; v_topRight
    pshufd          m6, m6, 0                   ; v_bottomLeft

    pmullw          m4, m3, [multiH]            ; (x + 1) * topRight
    pmullw          m3, [multiL]                ; (x + 1) * topRight
    pmullw          m1, m2, [pw_15]             ; (blkSize - 1 - y) * above[x]
    pmullw          m5, m7, [pw_15]             ; (blkSize - 1 - y) * above[x]
    paddw           m4, [pw_16]
    paddw           m3, [pw_16]
    paddw           m4, m6
    paddw           m3, m6
    paddw           m4, m5
    paddw           m3, m1
    psubw           m1, m6, m7
    psubw           m6, m2

    movu            m2, [r2 + 66]
    movu            m7, [r2 + 82]

%macro INTRA_PRED_PLANAR16 1
%if (%1 < 4)
    pshuflw         m5, m2, 0x55 * %1
    pshufd          m5, m5, 0
%else
%if (%1 < 8)
    pshufhw         m5, m2, 0x55 * (%1 - 4)
    pshufd          m5, m5, 0xAA
%else
%if (%1 < 12)
    pshuflw         m5, m7, 0x55 * (%1 - 8)
    pshufd          m5, m5, 0
%else
    pshufhw         m5, m7, 0x55 * (%1 - 12)
    pshufd          m5, m5, 0xAA
%endif
%endif
%endif
    pmullw          m0, m5, [pw_planar16_mul + mmsize]
    pmullw          m5, [pw_planar16_mul]
    paddw           m0, m4
    paddw           m5, m3
    paddw           m3, m6
    paddw           m4, m1
    psraw           m5, 5
    psraw           m0, 5
    movu            [r0], m5
    movu            [r0 + 16], m0
    lea             r0, [r0 + r1]
%endmacro

    INTRA_PRED_PLANAR16 0
    INTRA_PRED_PLANAR16 1
    INTRA_PRED_PLANAR16 2
    INTRA_PRED_PLANAR16 3
    INTRA_PRED_PLANAR16 4
    INTRA_PRED_PLANAR16 5
    INTRA_PRED_PLANAR16 6
    INTRA_PRED_PLANAR16 7
    INTRA_PRED_PLANAR16 8
    INTRA_PRED_PLANAR16 9
    INTRA_PRED_PLANAR16 10
    INTRA_PRED_PLANAR16 11
    INTRA_PRED_PLANAR16 12
    INTRA_PRED_PLANAR16 13
    INTRA_PRED_PLANAR16 14
    INTRA_PRED_PLANAR16 15
    RET
%endif

;---------------------------------------------------------------------------------------
; void intra_pred_planar(pixel* dst, intptr_t dstStride, pixel*srcPix, int, int filter)
;---------------------------------------------------------------------------------------
INIT_XMM sse4
%if ARCH_X86_64 == 1
cglobal intra_pred_planar32, 3,7,16
  ; NOTE: align stack to 64 bytes, so all of local data in same cache line
  mov               r6, rsp
  sub               rsp, 4*mmsize
  and               rsp, ~63
  %define           m16 [rsp + 0 * mmsize]
  %define           m17 [rsp + 1 * mmsize]
  %define           m18 [rsp + 2 * mmsize]
  %define           m19 [rsp + 3 * mmsize]
%else
cglobal intra_pred_planar32, 3,7,8
  ; NOTE: align stack to 64 bytes, so all of local data in same cache line
  mov               r6, rsp
  sub               rsp, 12*mmsize
  and               rsp, ~63
  %define           m8  [rsp + 0  * mmsize]
  %define           m9  [rsp + 1  * mmsize]
  %define           m10 [rsp + 2  * mmsize]
  %define           m11 [rsp + 3  * mmsize]
  %define           m12 [rsp + 4  * mmsize]
  %define           m13 [rsp + 5  * mmsize]
  %define           m14 [rsp + 6  * mmsize]
  %define           m15 [rsp + 7  * mmsize]
  %define           m16 [rsp + 8  * mmsize]
  %define           m17 [rsp + 9  * mmsize]
  %define           m18 [rsp + 10 * mmsize]
  %define           m19 [rsp + 11 * mmsize]
%endif
    add             r1, r1
    lea             r5, [planar32_table1]

    movzx           r3d, word [r2 + 66]         ; topRight   = above[32]
    movd            m7, r3d
    pshufd          m7, m7, 0                   ; v_topRight

    pmulld          m0, m7, [r5 + 0  ]          ; (x + 1) * topRight
    pmulld          m1, m7, [r5 + 16 ]
    pmulld          m2, m7, [r5 + 32 ]
    pmulld          m3, m7, [r5 + 48 ]
    pmulld          m4, m7, [r5 + 64 ]
    pmulld          m5, m7, [r5 + 80 ]
    pmulld          m6, m7, [r5 + 96 ]
    pmulld          m7, m7, [r5 + 112]

    mova            m12, m4
    mova            m13, m5
    mova            m14, m6
    mova            m15, m7

    movzx           r3d, word [r2 + 194]        ; bottomLeft = left[32]
    movd            m6, r3d
    pshufd          m6, m6, 0                   ; v_bottomLeft

    paddd           m0, m6
    paddd           m1, m6
    paddd           m2, m6
    paddd           m3, m6
    paddd           m0, [pd_32]
    paddd           m1, [pd_32]
    paddd           m2, [pd_32]
    paddd           m3, [pd_32]

    mova            m4, m12
    mova            m5, m13
    paddd           m4, m6
    paddd           m5, m6
    paddd           m4, [pd_32]
    paddd           m5, [pd_32]
    mova            m12, m4
    mova            m13, m5

    mova            m4, m14
    mova            m5, m15
    paddd           m4, m6
    paddd           m5, m6
    paddd           m4, [pd_32]
    paddd           m5, [pd_32]
    mova            m14, m4
    mova            m15, m5

    ; above[0-3] * (blkSize - 1 - y)
    pmovzxwd        m4, [r2 + 2]
    pmulld          m5, m4, [pd_31]
    paddd           m0, m5
    psubd           m5, m6, m4
    mova            m8, m5

    ; above[4-7] * (blkSize - 1 - y)
    pmovzxwd        m4, [r2 + 10]
    pmulld          m5, m4, [pd_31]
    paddd           m1, m5
    psubd           m5, m6, m4
    mova            m9, m5

    ; above[8-11] * (blkSize - 1 - y)
    pmovzxwd        m4, [r2 + 18]
    pmulld          m5, m4, [pd_31]
    paddd           m2, m5
    psubd           m5, m6, m4
    mova            m10, m5

    ; above[12-15] * (blkSize - 1 - y)
    pmovzxwd        m4, [r2 + 26]
    pmulld          m5, m4, [pd_31]
    paddd           m3, m5
    psubd           m5, m6, m4
    mova            m11, m5

    ; above[16-19] * (blkSize - 1 - y)
    pmovzxwd        m4, [r2 + 34]
    mova            m7, m12
    pmulld          m5, m4, [pd_31]
    paddd           m7, m5
    mova            m12, m7
    psubd           m5, m6, m4
    mova            m16, m5

    ; above[20-23] * (blkSize - 1 - y)
    pmovzxwd        m4, [r2 + 42]
    mova            m7, m13
    pmulld          m5, m4, [pd_31]
    paddd           m7, m5
    mova            m13, m7
    psubd           m5, m6, m4
    mova            m17, m5

    ; above[24-27] * (blkSize - 1 - y)
    pmovzxwd        m4, [r2 + 50]
    mova            m7, m14
    pmulld          m5, m4, [pd_31]
    paddd           m7, m5
    mova            m14, m7
    psubd           m5, m6, m4
    mova            m18, m5

    ; above[28-31] * (blkSize - 1 - y)
    pmovzxwd        m4, [r2 + 58]
    mova            m7, m15
    pmulld          m5, m4, [pd_31]
    paddd           m7, m5
    mova            m15, m7
    psubd           m5, m6, m4
    mova            m19, m5

    add             r2, 130                      ; (2 * blkSize + 1)
    lea             r5, [planar32_table]

%macro INTRA_PRED_PLANAR32 0
    movzx           r3d, word [r2]
    movd            m4, r3d
    pshufd          m4, m4, 0

    pmulld          m5, m4, [r5]
    pmulld          m6, m4, [r5 + 16]
    paddd           m5, m0
    paddd           m6, m1
    paddd           m0, m8
    paddd           m1, m9
    psrad           m5, 6
    psrad           m6, 6
    packusdw        m5, m6
    movu            [r0], m5

    pmulld          m5, m4, [r5 + 32]
    pmulld          m6, m4, [r5 + 48]
    paddd           m5, m2
    paddd           m6, m3
    paddd           m2, m10
    paddd           m3, m11
    psrad           m5, 6
    psrad           m6, 6
    packusdw        m5, m6
    movu            [r0 + 16], m5

    pmulld          m5, m4, [r5 + 64]
    pmulld          m6, m4, [r5 + 80]
    paddd           m5, m12
    paddd           m6, m13
    psrad           m5, 6
    psrad           m6, 6
    packusdw        m5, m6
    movu            [r0 + 32], m5
    mova            m5, m12
    mova            m6, m13
    paddd           m5, m16
    paddd           m6, m17
    mova            m12, m5
    mova            m13, m6

    pmulld          m5, m4, [r5 + 96]
    pmulld          m4, [r5 + 112]
    paddd           m5, m14
    paddd           m4, m15
    psrad           m5, 6
    psrad           m4, 6
    packusdw        m5, m4
    movu            [r0 + 48], m5
    mova            m4, m14
    mova            m5, m15
    paddd           m4, m18
    paddd           m5, m19
    mova            m14, m4
    mova            m15, m5

    lea             r0, [r0 + r1]
    add             r2, 2
%endmacro

    mov             r4, 8
.loop:
    INTRA_PRED_PLANAR32
    INTRA_PRED_PLANAR32
    INTRA_PRED_PLANAR32
    INTRA_PRED_PLANAR32
    dec             r4
    jnz             .loop
    mov             rsp, r6
    RET

;-----------------------------------------------------------------------------------------
; void intraPredAng4(pixel* dst, intptr_t dstStride, pixel* src, int dirMode, int bFilter)
;-----------------------------------------------------------------------------------------
INIT_XMM ssse3
cglobal intra_pred_ang4_2, 3,5,4
    lea         r4,            [r2 + 4]
    add         r2,            20
    cmp         r3m,           byte 34
    cmove       r2,            r4

    add         r1,            r1
    movu        m0,            [r2]
    movh        [r0],          m0
    palignr     m1,            m0, 2
    movh        [r0 + r1],     m1
    palignr     m2,            m0, 4
    movh        [r0 + r1 * 2], m2
    lea         r1,            [r1 * 3]
    psrldq      m0,            6
    movh        [r0 + r1],     m0
    RET

INIT_XMM sse4
cglobal intra_pred_ang4_3, 3,5,8
    mov         r4, 2
    cmp         r3m, byte 33
    mov         r3, 18
    cmove       r3, r4

    movu        m0, [r2 + r3]   ; [8 7 6 5 4 3 2 1]
    palignr     m1, m0, 2       ; [x 8 7 6 5 4 3 2]
    punpcklwd   m2, m0, m1      ; [5 4 4 3 3 2 2 1]
    palignr     m5, m0, 4       ; [x x 8 7 6 5 4 3]
    punpcklwd   m3, m1, m5      ; [6 5 5 4 4 3 3 2]
    palignr     m1, m0, 6       ; [x x x 8 7 6 5 4]
    punpcklwd   m4, m5 ,m1      ; [7 6 6 5 5 4 4 3]
    movhlps     m0, m0          ; [x x x x 8 7 6 5]
    punpcklwd   m5, m1, m0      ; [8 7 7 6 6 5 5 4]

    lea         r3, [ang_table + 20 * 16]
    mova        m0, [r3 + 6 * 16]   ; [26]
    mova        m1, [r3]            ; [20]
    mova        m6, [r3 - 6 * 16]   ; [14]
    mova        m7, [r3 - 12 * 16]  ; [ 8]
    jmp        .do_filter4x4

ALIGN 16
.do_filter4x4:
    pmaddwd m2, m0
    paddd   m2, [pd_16]
    psrld   m2, 5

    pmaddwd m3, m1
    paddd   m3, [pd_16]
    psrld   m3, 5
    packusdw m2, m3

    pmaddwd m4, m6
    paddd   m4, [pd_16]
    psrld   m4, 5

    pmaddwd m5, m7
    paddd   m5, [pd_16]
    psrld   m5, 5
    packusdw m4, m5

    jz         .store

    ; transpose 4x4
    punpckhwd    m0, m2, m4
    punpcklwd    m2, m4
    punpckhwd    m4, m2, m0
    punpcklwd    m2, m0

.store:
    add         r1, r1
    movh        [r0], m2
    movhps      [r0 + r1], m2
    movh        [r0 + r1 * 2], m4
    lea         r1, [r1 * 3]
    movhps      [r0 + r1], m4
    RET

cglobal intra_pred_ang4_4, 3,5,8
    mov         r4, 2
    cmp         r3m, byte 32
    mov         r3, 18
    cmove       r3, r4

    movu        m0, [r2 + r3]   ; [8 7 6 5 4 3 2 1]
    palignr     m1, m0, 2       ; [x 8 7 6 5 4 3 2]
    punpcklwd   m2, m0, m1      ; [5 4 4 3 3 2 2 1]
    palignr     m6, m0, 4       ; [x x 8 7 6 5 4 3]
    punpcklwd   m3, m1, m6      ; [6 5 5 4 4 3 3 2]
    mova        m4, m3
    palignr     m7, m0, 6       ; [x x x 8 7 6 5 4]
    punpcklwd   m5, m6, m7      ; [7 6 6 5 5 4 4 3]

    lea         r3, [ang_table + 18 * 16]
    mova        m0, [r3 +  3 * 16]  ; [21]
    mova        m1, [r3 -  8 * 16]  ; [10]
    mova        m6, [r3 + 13 * 16]  ; [31]
    mova        m7, [r3 +  2 * 16]  ; [20]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_5, 3,5,8
    mov         r4, 2
    cmp         r3m, byte 31
    mov         r3, 18
    cmove       r3, r4

    movu        m0, [r2 + r3]   ; [8 7 6 5 4 3 2 1]
    palignr     m1, m0, 2       ; [x 8 7 6 5 4 3 2]
    punpcklwd   m2, m0, m1      ; [5 4 4 3 3 2 2 1]
    palignr     m6, m0, 4       ; [x x 8 7 6 5 4 3]
    punpcklwd   m3, m1, m6      ; [6 5 5 4 4 3 3 2]
    mova        m4, m3
    palignr     m7, m0, 6       ; [x x x 8 7 6 5 4]
    punpcklwd   m5, m6, m7      ; [7 6 6 5 5 4 4 3]

    lea         r3, [ang_table + 10 * 16]
    mova        m0, [r3 +  7 * 16]  ; [17]
    mova        m1, [r3 -  8 * 16]  ; [ 2]
    mova        m6, [r3 +  9 * 16]  ; [19]
    mova        m7, [r3 -  6 * 16]  ; [ 4]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_6, 3,5,8
    mov         r4, 2
    cmp         r3m, byte 30
    mov         r3, 18
    cmove       r3, r4

    movu        m0, [r2 + r3]   ; [8 7 6 5 4 3 2 1]
    palignr     m1, m0, 2       ; [x 8 7 6 5 4 3 2]
    punpcklwd   m2, m0, m1      ; [5 4 4 3 3 2 2 1]
    mova        m3, m2
    palignr     m6, m0, 4       ; [x x 8 7 6 5 4 3]
    punpcklwd   m4, m1, m6      ; [6 5 5 4 4 3 3 2]
    mova        m5, m4

    lea         r3, [ang_table + 19 * 16]
    mova        m0, [r3 -  6 * 16]  ; [13]
    mova        m1, [r3 +  7 * 16]  ; [26]
    mova        m6, [r3 - 12 * 16]  ; [ 7]
    mova        m7, [r3 +  1 * 16]  ; [20]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_7, 3,5,8
    mov         r4, 2
    cmp         r3m, byte 29
    mov         r3, 18
    cmove       r3, r4

    movu        m0, [r2 + r3]   ; [8 7 6 5 4 3 2 1]
    palignr     m1, m0, 2       ; [x 8 7 6 5 4 3 2]
    punpcklwd   m2, m0, m1      ; [5 4 4 3 3 2 2 1]
    mova        m3, m2
    mova        m4, m2
    palignr     m6, m0, 4       ; [x x 8 7 6 5 4 3]
    punpcklwd   m5, m1, m6      ; [6 5 5 4 4 3 3 2]

    lea         r3, [ang_table + 20 * 16]
    mova        m0, [r3 - 11 * 16]  ; [ 9]
    mova        m1, [r3 -  2 * 16]  ; [18]
    mova        m6, [r3 +  7 * 16]  ; [27]
    mova        m7, [r3 - 16 * 16]  ; [ 4]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_8, 3,5,8
    mov         r4, 2
    cmp         r3m, byte 28
    mov         r3, 18
    cmove       r3, r4

    movu        m0, [r2 + r3]   ; [8 7 6 5 4 3 2 1]
    palignr     m1, m0, 2       ; [x 8 7 6 5 4 3 2]
    punpcklwd   m2, m0, m1      ; [5 4 4 3 3 2 2 1]
    mova        m3, m2
    mova        m4, m2
    mova        m5, m2

    lea         r3, [ang_table + 13 * 16]
    mova        m0, [r3 -  8 * 16]  ; [ 5]
    mova        m1, [r3 -  3 * 16]  ; [10]
    mova        m6, [r3 +  2 * 16]  ; [15]
    mova        m7, [r3 +  7 * 16]  ; [20]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_9, 3,5,8
    mov         r4, 2
    cmp         r3m, byte 27
    mov         r3, 18
    cmove       r3, r4

    movu        m0, [r2 + r3]   ; [8 7 6 5 4 3 2 1]
    palignr     m1, m0, 2       ; [x 8 7 6 5 4 3 2]
    punpcklwd   m2, m0, m1      ; [5 4 4 3 3 2 2 1]
    mova        m3, m2
    mova        m4, m2
    mova        m5, m2

    lea         r3, [ang_table + 4 * 16]
    mova        m0, [r3 -  2 * 16]  ; [ 2]
    mova        m1, [r3 -  0 * 16]  ; [ 4]
    mova        m6, [r3 +  2 * 16]  ; [ 6]
    mova        m7, [r3 +  4 * 16]  ; [ 8]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_10, 3,3,4
    movh        m0,             [r2 + 18]           ; [4 3 2 1]
    pshufb      m2,             m0, [pb_unpackwq2]  ; [4 4 4 4 3 3 3 3]
    pshufb      m0,             [pb_unpackwq1]      ; [2 2 2 2 1 1 1 1]
    add         r1,             r1
    movhlps     m1,             m0                  ; [2 2 2 2]
    movhlps     m3,             m2                  ; [4 4 4 4]
    movh        [r0 + r1],      m1
    movh        [r0 + r1 * 2],  m2
    lea         r1,             [r1 * 3]
    movh        [r0 + r1],      m3

    cmp         r4m,            byte 0
    jz         .quit

    ; filter
    movu        m1,             [r2]                ; [7 6 5 4 3 2 1 0]
    pshufb      m2,             m1, [pb_unpackwq1]  ; [0 0 0 0]
    palignr     m1,             m1, 2               ; [4 3 2 1]
    psubw       m1,             m2
    psraw       m1,             1
    paddw       m0,             m1
    pxor        m1,             m1
    pmaxsw      m0,             m1
    pminsw      m0,             [pw_pixel_max]
.quit:
    movh        [r0],           m0
    RET

cglobal intra_pred_ang4_26, 3,4,3
    movh        m0,             [r2 + 2]            ; [8 7 6 5 4 3 2 1]
    add         r1,             r1
    ; store
    movh        [r0],           m0
    movh        [r0 + r1],      m0
    movh        [r0 + r1 * 2],  m0
    lea         r3,             [r1 * 3]
    movh        [r0 + r3],      m0

    ; filter
    cmp         r4m,            byte 0
    jz         .quit

    pshufb      m0,             [pb_unpackwq1]      ; [2 2 2 2 1 1 1 1]
    movu        m1,             [r2 + 16]
    pinsrw      m1,             [r2], 0             ; [7 6 5 4 3 2 1 0]
    pshufb      m2,             m1, [pb_unpackwq1]  ; [0 0 0 0]
    palignr     m1,             m1, 2               ; [4 3 2 1]
    psubw       m1,             m2
    psraw       m1,             1
    paddw       m0,             m1
    pxor        m1,             m1
    pmaxsw      m0,             m1
    pminsw      m0,             [pw_pixel_max]

    pextrw      [r0],           m0, 0
    pextrw      [r0 + r1],      m0, 1
    pextrw      [r0 + r1 * 2],  m0, 2
    pextrw      [r0 + r3],      m0, 3
.quit:
    RET

cglobal intra_pred_ang4_11, 3,5,8
    xor         r4, r4
    cmp         r3m, byte 25
    mov         r3, 16
    cmove       r3, r4

    movu        m2, [r2 + r3]   ; [x x x 4 3 2 1 0]
    pinsrw      m2, [r2], 0
    palignr     m1, m2, 2       ; [x x x x 4 3 2 1]
    punpcklwd   m2, m1          ; [4 3 3 2 2 1 1 0]
    mova        m3, m2
    mova        m4, m2
    mova        m5, m2

    lea         r3, [ang_table + 24 * 16]
    mova        m0, [r3 +  6 * 16]  ; [24]
    mova        m1, [r3 +  4 * 16]  ; [26]
    mova        m6, [r3 +  2 * 16]  ; [28]
    mova        m7, [r3 +  0 * 16]  ; [30]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_12, 3,5,8
    xor         r4, r4
    cmp         r3m, byte 24
    mov         r3, 16
    cmove       r3, r4

    movu        m2, [r2 + r3]   ; [x x x 4 3 2 1 0]
    pinsrw      m2, [r2], 0
    palignr     m1, m2, 2       ; [x x x x 4 3 2 1]
    punpcklwd   m2, m1          ; [4 3 3 2 2 1 1 0]
    mova        m3, m2
    mova        m4, m2
    mova        m5, m2

    lea         r3, [ang_table + 20 * 16]
    mova        m0, [r3 +  7 * 16]  ; [27]
    mova        m1, [r3 +  2 * 16]  ; [22]
    mova        m6, [r3 -  3 * 16]  ; [17]
    mova        m7, [r3 -  8 * 16]  ; [12]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_13, 3,5,8
    xor         r4, r4
    cmp         r3m, byte 23
    mov         r3, 16
    jz          .next
    xchg        r3, r4
.next:
    movu        m5, [r2 + r4 - 2]   ; [x x 4 3 2 1 0 x]
    pinsrw      m5, [r2], 1
    palignr     m2, m5, 2       ; [x x x 4 3 2 1 0]
    palignr     m0, m5, 4       ; [x x x x 4 3 2 1]
    pinsrw      m5, [r2 + r3 + 8], 0
    punpcklwd   m5, m2          ; [3 2 2 1 1 0 0 x]
    punpcklwd   m2, m0          ; [4 3 3 2 2 1 1 0]
    mova        m3, m2
    mova        m4, m2

    lea         r3, [ang_table + 21 * 16]
    mova        m0, [r3 +  2 * 16]  ; [23]
    mova        m1, [r3 -  7 * 16]  ; [14]
    mova        m6, [r3 - 16 * 16]  ; [ 5]
    mova        m7, [r3 +  7 * 16]  ; [28]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_14, 3,5,8
    xor         r4, r4
    cmp         r3m, byte 22
    mov         r3, 16
    jz          .next
    xchg        r3, r4
.next:
    movu        m5, [r2 + r4 - 2]   ; [x x 4 3 2 1 0 x]
    pinsrw      m5, [r2], 1
    palignr     m2, m5, 2       ; [x x x 4 3 2 1 0]
    palignr     m0, m5, 4       ; [x x x x 4 3 2 1]
    pinsrw      m5, [r2 + r3 + 4], 0
    punpcklwd   m5, m2          ; [3 2 2 1 1 0 0 x]
    punpcklwd   m2, m0          ; [4 3 3 2 2 1 1 0]
    mova        m3, m2
    mova        m4, m5

    lea         r3, [ang_table + 19 * 16]
    mova        m0, [r3 +  0 * 16]  ; [19]
    mova        m1, [r3 - 13 * 16]  ; [ 6]
    mova        m6, [r3 +  6 * 16]  ; [25]
    mova        m7, [r3 -  7 * 16]  ; [12]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_15, 3,5,8
    xor         r4, r4
    cmp         r3m, byte 21
    mov         r3, 16
    jz          .next
    xchg        r3, r4
.next:
    movu        m3, [r2 + r4 - 2]   ; [x x 4 3 2 1 0 x]
    pinsrw      m3, [r2], 1
    palignr     m2, m3, 2       ; [x x x 4 3 2 1 0]
    palignr     m0, m3, 4       ; [x x x x 4 3 2 1]
    pinsrw      m3, [r2 + r3 + 4], 0
    pslldq      m5, m3, 2       ; [x 4 3 2 1 0 x y]
    pinsrw      m5, [r2 + r3 + 8], 0
    punpcklwd   m5, m3          ; [2 1 1 0 0 x x y]
    punpcklwd   m3, m2          ; [3 2 2 1 1 0 0 x]
    punpcklwd   m2, m0          ; [4 3 3 2 2 1 1 0]
    mova        m4, m3

    lea         r3, [ang_table + 23 * 16]
    mova        m0, [r3 -  8 * 16]  ; [15]
    mova        m1, [r3 +  7 * 16]  ; [30]
    mova        m6, [r3 - 10 * 16]  ; [13]
    mova        m7, [r3 +  5 * 16]  ; [28]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_16, 3,5,8
    xor         r4, r4
    cmp         r3m, byte 20
    mov         r3, 16
    jz          .next
    xchg        r3, r4
.next:
    movu        m3, [r2 + r4 - 2]   ; [x x 4 3 2 1 0 x]
    pinsrw      m3, [r2], 1
    palignr     m2, m3, 2       ; [x x x 4 3 2 1 0]
    palignr     m0, m3, 4       ; [x x x x 4 3 2 1]
    pinsrw      m3, [r2 + r3 + 4], 0
    pslldq      m5, m3, 2       ; [x 4 3 2 1 0 x y]
    pinsrw      m5, [r2 + r3 + 6], 0
    punpcklwd   m5, m3          ; [2 1 1 0 0 x x y]
    punpcklwd   m3, m2          ; [3 2 2 1 1 0 0 x]
    punpcklwd   m2, m0          ; [4 3 3 2 2 1 1 0]
    mova        m4, m3

    lea         r3, [ang_table + 19 * 16]
    mova        m0, [r3 -  8 * 16]  ; [11]
    mova        m1, [r3 +  3 * 16]  ; [22]
    mova        m6, [r3 - 18 * 16]  ; [ 1]
    mova        m7, [r3 -  7 * 16]  ; [12]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_17, 3,5,8
    xor         r4, r4
    cmp         r3m, byte 19
    mov         r3, 16
    jz          .next
    xchg        r3, r4
.next:
    movu        m6, [r2 + r4 - 2]   ; [- - 4 3 2 1 0 x]
    pinsrw      m6, [r2], 1
    palignr     m2, m6, 2       ; [- - - 4 3 2 1 0]
    palignr     m1, m6, 4       ; [- - - - 4 3 2 1]
    mova        m4, m2
    punpcklwd   m2, m1          ; [4 3 3 2 2 1 1 0]

    pinsrw      m6, [r2 + r3 + 2], 0
    punpcklwd   m3, m6, m4      ; [3 2 2 1 1 0 0 x]

    pslldq      m4, m6, 2       ; [- 4 3 2 1 0 x y]
    pinsrw      m4, [r2 + r3 + 4], 0
    pslldq      m5, m4, 2       ; [4 3 2 1 0 x y z]
    pinsrw      m5, [r2 + r3 + 8], 0
    punpcklwd   m5, m4          ; [1 0 0 x x y y z]
    punpcklwd   m4, m6          ; [2 1 1 0 0 x x y]

    lea         r3, [ang_table + 14 * 16]
    mova        m0, [r3 -  8 * 16]  ; [ 6]
    mova        m1, [r3 -  2 * 16]  ; [12]
    mova        m6, [r3 +  4 * 16]  ; [18]
    mova        m7, [r3 + 10 * 16]  ; [24]
    jmp         mangle(private_prefix %+ _ %+ intra_pred_ang4_3 %+ SUFFIX %+ .do_filter4x4)

cglobal intra_pred_ang4_18, 3,3,1
    movh        m0, [r2 + 16]
    pinsrw      m0, [r2], 0
    pshufb      m0, [pw_swap]
    movhps      m0, [r2 + 2]
    add         r1, r1
    lea         r2, [r1 * 3]
    movh        [r0 + r2], m0
    psrldq      m0, 2
    movh        [r0 + r1 * 2], m0
    psrldq      m0, 2
    movh        [r0 + r1], m0
    psrldq      m0, 2
    movh        [r0], m0
    RET

;-----------------------------------------------------------------------------------------
; void intraPredAng8(pixel* dst, intptr_t dstStride, pixel* src, int dirMode, int bFilter)
;-----------------------------------------------------------------------------------------
INIT_XMM ssse3
cglobal intra_pred_ang8_2, 3,5,3
    lea         r4,            [r2]
    add         r2,            32
    cmp         r3m,           byte 34
    cmove       r2,            r4
    add         r1,            r1
    lea         r3,            [r1 * 3]
    movu        m0,            [r2 + 4]
    movu        m1,            [r2 + 20]
    movu        [r0],          m0
    palignr     m2,            m1, m0, 2
    movu        [r0 + r1],     m2
    palignr     m2,            m1, m0, 4
    movu        [r0 + r1 * 2], m2
    palignr     m2,            m1, m0, 6
    movu        [r0 + r3],     m2
    lea         r0,            [r0 + r1 * 4]
    palignr     m2,            m1, m0, 8
    movu        [r0],          m2
    palignr     m2,            m1, m0, 10
    movu        [r0 + r1],     m2
    palignr     m2,            m1, m0, 12
    movu        [r0 + r1 * 2], m2
    palignr     m1,            m0, 14
    movu        [r0 + r3],     m1
    RET

INIT_XMM sse4
cglobal intra_pred_ang8_3, 3,5,8
    add         r2,        32
    lea         r3,        [ang_table + 14 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]
    punpckhwd   m1,        m4                         ; [x 16 16 15 15 14 14 13]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 12 * 16]             ; [26]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 12 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m2,        [r3 + 6 * 16]              ; [20]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m6,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m6,        [r3 + 6 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    palignr     m6,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m6,        [r3]                       ; [14]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m7,        [r3]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m7,        [r3 - 6 * 16]              ; [ 8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m3,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    pmaddwd     m3,        [r3 - 6 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m7,        m3

    punpckhwd   m3,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m3, m2
    punpckhdq   m3,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m7
    movhps      [r0 + r1],       m7
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r2,              [r0 + r1 * 4]
    movh        [r2],            m6
    movhps      [r2 + r1],       m6
    movh        [r2 + r1 * 2],   m3
    movhps      [r2 + r4],       m3

    mova        m4,        m0
    pmaddwd     m4,        [r3 - 12 * 16]             ; [ 2]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m5
    pmaddwd     m2,        [r3 - 12 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m0
    pmaddwd     m2,        [r3 + 14 * 16]             ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m5
    pmaddwd     m6,        [r3 + 14 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    palignr     m6,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m6,        [r3 + 8 * 16]              ; [22]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m7,        m1, m5, 4                  ; [14 13 13 12 12 11 11 10]
    pmaddwd     m7,        [r3 + 8 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m7,        [r3 + 2 * 16]              ; [16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, 8                      ; [15 14 14 13 13 12 12 11]
    pmaddwd     m1,        [r3 + 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    punpckhwd   m3,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m3, m2
    punpckhdq   m3,        m2

    movh        [r0 + 8],            m7
    movhps      [r0 + r1 + 8],       m7
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m3
    movhps      [r0 + r4 + 8],       m3
    RET

cglobal intra_pred_ang8_4, 3,6,8
    add         r2,        32
    lea         r3,        [ang_table + 19 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 2 * 16]              ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 2 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 9 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 12 * 16]             ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 12 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 + 1 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m1,        [r3 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    punpckhwd   m1,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m1, m2
    punpckhdq   m1,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m7
    movhps      [r0 + r1],       m7
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r5,              [r0 + r1 * 4]
    movh        [r5],            m6
    movhps      [r5 + r1],       m6
    movh        [r5 + r1 * 2],   m1
    movhps      [r5 + r4],       m1

    palignr     m4,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    mova        m2,        m4
    pmaddwd     m4,        [r3 - 10 * 16]             ; [ 9]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m3,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m6,        m3
    pmaddwd     m3,        [r3 - 10 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m4,        m3

    pmaddwd     m2,        [r3 + 11 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m6,        [r3 + 11 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m0
    pmaddwd     m6,        [r3]                       ; [19]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m5
    pmaddwd     m7,        [r3]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    movh        m1,        [r2 + 26]                  ; [16 15 14 13]
    palignr     m7,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m7,        [r3 - 11 * 16]             ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, 4                      ; [14 13 13 12 12 11 11 10]
    pmaddwd     m1,        [r3 - 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    punpckhwd   m3,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m3, m2
    punpckhdq   m3,        m2

    movh        [r0 + 8],            m7
    movhps      [r0 + r1 + 8],       m7
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m3
    movhps      [r0 + r4 + 8],       m3
    RET

cglobal intra_pred_ang8_5, 3,5,8
    add         r2,        32
    lea         r3,        [ang_table + 13 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 4 * 16]              ; [17]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 4 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 11 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 6 * 16]              ; [19]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 6 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 - 9 * 16]              ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m1,        [r3 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    punpckhwd   m1,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m1, m2
    punpckhdq   m1,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m7
    movhps      [r0 + r1],       m7
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r2,              [r0 + r1 * 4]
    movh        [r2],            m6
    movhps      [r2 + r1],       m6
    movh        [r2 + r1 * 2],   m1
    movhps      [r2 + r4],       m1

    palignr     m4,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m4,        [r3 + 8 * 16]              ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m2,        [r3 + 8 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 7 * 16]              ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 10 * 16]             ; [23]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 10 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    mova        m7,        m0
    pmaddwd     m7,        [r3 - 5 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m5
    pmaddwd     m1,        [r3 - 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    punpckhwd   m3,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m3, m2
    punpckhdq   m3,        m2

    movh        [r0 + 8],            m7
    movhps      [r0 + r1 + 8],       m7
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m3
    movhps      [r0 + r4 + 8],       m3
    RET

cglobal intra_pred_ang8_6, 3,5,8
    add         r2,        32
    lea         r3,        [ang_table + 14 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 1 * 16]              ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 12 * 16]             ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m6,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m7,        m6
    pmaddwd     m6,        [r3 - 7 * 16]              ; [7]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m7,        [r3 + 6 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 + 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    punpckhwd   m1,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m1, m2
    punpckhdq   m1,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m7
    movhps      [r0 + r1],       m7
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r2,              [r0 + r1 * 4]
    movh        [r2],            m6
    movhps      [r2 + r1],       m6
    movh        [r2 + r1 * 2],   m1
    movhps      [r2 + r4],       m1

    palignr     m4,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    mova        m6,        m4
    pmaddwd     m4,        [r3 - 13 * 16]             ; [1]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    mova        m7,        m2
    pmaddwd     m2,        [r3 - 13 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    pmaddwd     m2,        m6, [r3]                   ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m1,        m7, [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 13 * 16]             ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 13 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m7,        [r3 - 6 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m5,        m0, 12                     ; [12 11 11 10 10 9 9 8]
    pmaddwd     m5,        [r3 - 6 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m7,        m5

    punpckhwd   m3,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m3, m2
    punpckhdq   m3,        m2

    movh        [r0 + 8],            m7
    movhps      [r0 + r1 + 8],       m7
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m3
    movhps      [r0 + r4 + 8],       m3
    RET

cglobal intra_pred_ang8_7, 3,5,8
    add         r2,        32
    lea         r3,        [ang_table + 18 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 9 * 16]              ; [9]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3]                       ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 9 * 16]              ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m7,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m7,        [r3 - 14 * 16]             ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    punpckhwd   m1,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m1, m2
    punpckhdq   m1,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m7
    movhps      [r0 + r1],       m7
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r2,              [r0 + r1 * 4]
    movh        [r2],            m6
    movhps      [r2 + r1],       m6
    movh        [r2 + r1 * 2],   m1
    movhps      [r2 + r4],       m1

    palignr     m4,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m4
    pmaddwd     m4,        [r3 - 5 * 16]              ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m2
    pmaddwd     m2,        [r3 - 5 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    pmaddwd     m2,        m6, [r3 + 4 * 16]          ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m1,        m7, [r3 + 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 13 * 16]             ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 13 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 - 10 * 16]             ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m5,        m0, 8                      ; [11 10 10 9 9 8 8 7]
    pmaddwd     m5,        [r3 - 10 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m7,        m5

    punpckhwd   m3,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m7
    punpcklwd   m6,        m7

    punpckldq   m7,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m3, m2
    punpckhdq   m3,        m2

    movh        [r0 + 8],            m7
    movhps      [r0 + r1 + 8],       m7
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m3
    movhps      [r0 + r4 + 8],       m3
    RET

cglobal intra_pred_ang8_8, 3,6,7
    add         r2,        32
    lea         r3,        [ang_table + 17 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 4]                   ; [9 8 7 6 5 4 3 2]

    punpcklwd   m3,        m0, m1                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m1                         ; [9 8 8 7 7 6 6 5]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 12 * 16]             ; [5]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 12 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 7 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 2 * 16]              ; [15]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m5,        m3
    pmaddwd     m5,        [r3 + 3 * 16]              ; [20]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    punpckhwd   m1,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m1, m2
    punpckhdq   m1,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r5,              [r0 + r1 * 4]
    movh        [r5],            m6
    movhps      [r5 + r1],       m6
    movh        [r5 + r1 * 2],   m1
    movhps      [r5 + r4],       m1

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 8 * 16]              ; [25]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 8 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 13 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    movh        m1,        [r2 + 18]                  ; [12 11 10 9]

    palignr     m6,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m5,        m6
    pmaddwd     m6,        [r3 - 14 * 16]             ; [3]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m1,        m0, 4                      ; [10 9 9 8 8 7 7 6]
    mova        m3,        m1
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m5,        [r3 - 9 * 16]              ; [8]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    pmaddwd     m3,        [r3 - 9 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m5,        m3

    punpckhwd   m3,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m3, m2
    punpckhdq   m3,        m2

    movh        [r0 + 8],            m5
    movhps      [r0 + r1 + 8],       m5
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m3
    movhps      [r0 + r4 + 8],       m3
    RET

cglobal intra_pred_ang8_9, 3,5,7
    add         r2,        32
    lea         r3,        [ang_table + 9 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 4]                   ; [9 8 7 6 5 4 3 2]

    punpcklwd   m3,        m0, m1                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m1                         ; [9 8 8 7 7 6 6 5]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 7 * 16]              ; [2]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 7 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 5 * 16]              ; [4]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 3 * 16]              ; [6]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m5,        m3
    pmaddwd     m5,        [r3 - 1 * 16]              ; [8]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    punpckhwd   m1,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m1, m2
    punpckhdq   m1,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r2,              [r0 + r1 * 4]
    movh        [r2],            m6
    movhps      [r2 + r1],       m6
    movh        [r2 + r1 * 2],   m1
    movhps      [r2 + r4],       m1

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 1 * 16]              ; [10]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 3 * 16]              ; [12]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 5 * 16]              ; [14]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 + 5 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pmaddwd     m3,        [r3 + 7 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 + 7 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    punpckhwd   m5,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m3
    punpcklwd   m6,        m3

    punpckldq   m3,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m5, m2
    punpckhdq   m5,        m2

    movh        [r0 + 8],            m3
    movhps      [r0 + r1 + 8],       m3
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m5
    movhps      [r0 + r4 + 8],       m5
    RET

cglobal intra_pred_ang8_10, 3,6,3
    movu        m1,             [r2 + 34]    ; [8 7 6 5 4 3 2 1]
    pshufb      m0,             m1, [pb_01]  ; [1 1 1 1 1 1 1 1]
    add         r1,             r1
    lea         r3,             [r1 * 3]

    psrldq      m1,             2
    pshufb      m2,             m1, [pb_01]  ; [2 2 2 2 2 2 2 2]
    movu        [r0 + r1],      m2
    psrldq      m1,             2
    pshufb      m2,             m1, [pb_01]  ; [3 3 3 3 3 3 3 3]
    movu        [r0 + r1 * 2],  m2
    psrldq      m1,             2
    pshufb      m2,             m1, [pb_01]  ; [4 4 4 4 4 4 4 4]
    movu        [r0 + r3],      m2

    lea         r5,             [r0 + r1 *4]
    psrldq      m1,             2
    pshufb      m2,             m1, [pb_01]  ; [5 5 5 5 5 5 5 5]
    movu        [r5],           m2
    psrldq      m1,             2
    pshufb      m2,             m1, [pb_01]  ; [6 6 6 6 6 6 6 6]
    movu        [r5 + r1],      m2
    psrldq      m1,             2
    pshufb      m2,             m1, [pb_01]  ; [7 7 7 7 7 7 7 7]
    movu        [r5 + r1 * 2],  m2
    psrldq      m1,             2
    pshufb      m2,             m1, [pb_01]  ; [8 8 8 8 8 8 8 8]
    movu        [r5 + r3],      m2

    cmp         r4m,            byte 0
    jz         .quit

    ; filter

    movh        m1,             [r2]                ; [3 2 1 0]
    pshufb      m2,             m1, [pb_01]  ; [0 0 0 0 0 0 0 0]
    movu        m1,             [r2 + 2]            ; [8 7 6 5 4 3 2 1]
    psubw       m1,             m2
    psraw       m1,             1
    paddw       m0,             m1
    pxor        m1,             m1
    pmaxsw      m0,             m1
    pminsw      m0,             [pw_pixel_max]
.quit:
    movu        [r0],           m0
    RET

cglobal intra_pred_ang8_11, 3,5,7
    lea         r3,        [ang_table + 23 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 32]                       ; [7 6 5 4 3 2 1 0]
    pinsrw      m0,        [r2], 0
    movu        m1,        [r2 + 34]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 7 * 16]              ; [30]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 7 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 5 * 16]              ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 3 * 16]              ; [26]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m5,        m3
    pmaddwd     m5,        [r3 + 1 * 16]              ; [24]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    punpckhwd   m1,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m1, m2
    punpckhdq   m1,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r2,              [r0 + r1 * 4]
    movh        [r2],            m6
    movhps      [r2 + r1],       m6
    movh        [r2 + r1 * 2],   m1
    movhps      [r2 + r4],       m1

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 1 * 16]              ; [22]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 3 * 16]              ; [20]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 5 * 16]              ; [18]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 5 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pmaddwd     m3,        [r3 - 7 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 - 7 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    punpckhwd   m5,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m3
    punpcklwd   m6,        m3

    punpckldq   m3,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m5, m2
    punpckhdq   m5,        m2

    movh        [r0 + 8],            m3
    movhps      [r0 + r1 + 8],       m3
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m5
    movhps      [r0 + r4 + 8],       m5
    RET

cglobal intra_pred_ang8_12, 3,6,7
    lea         r5,        [ang_table + 16 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 32]                  ; [7 6 5 4 3 2 1 0]
    pinsrw      m0,        [r2], 0
    movu        m1,        [r2 + 34]                  ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r5 + 11 * 16]             ; [27]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 + 11 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r5 + 6 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r5 + 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r5 + 1 * 16]              ; [17]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r5 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m5,        m3
    pmaddwd     m5,        [r5 - 4 * 16]              ; [12]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m1,        m0
    pmaddwd     m1,        [r5 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    punpckhwd   m1,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m1, m2
    punpckhdq   m1,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r3,              [r0 + r1 * 4]
    movh        [r3],            m6
    movhps      [r3 + r1],       m6
    movh        [r3 + r1 * 2],   m1
    movhps      [r3 + r4],       m1

    mova        m4,        m3
    pmaddwd     m4,        [r5 - 9 * 16]              ; [7]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 - 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r5 - 14 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r5 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m0,        m3, 12
    movu        m1,        [r2]
    pshufb      m1,        [pw_ang8_12]
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r5 + 13 * 16]             ; [29]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 + 13 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pmaddwd     m3,        [r5 + 8 * 16]              ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r5 + 8 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    punpckhwd   m5,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m3
    punpcklwd   m6,        m3

    punpckldq   m3,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m5, m2
    punpckhdq   m5,        m2

    movh        [r0 + 8],            m3
    movhps      [r0 + r1 + 8],       m3
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m5
    movhps      [r0 + r4 + 8],       m5
    RET

cglobal intra_pred_ang8_13, 3,6,8
    lea         r5,        [ang_table + 14 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 32]                  ; [7 6 5 4 3 2 1 0]
    pinsrw      m0,        [r2], 0
    movu        m1,        [r2 + 34]                  ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r5 + 9 * 16]              ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 + 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r5]                       ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r5]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r5 - 9 * 16]              ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r5 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m0,        m3, 12
    movu        m1,        [r2]
    pshufb      m1,        [pw_ang8_13]
    palignr     m3,        m1, 12

    mova        m5,        m3
    pmaddwd     m5,        [r5 + 14 * 16]             ; [28]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r5 + 14 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    punpckhwd   m7,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m7, m2
    punpckhdq   m7,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r2,              [r0 + r1 * 4]
    movh        [r2],            m6
    movhps      [r2 + r1],       m6
    movh        [r2 + r1 * 2],   m7
    movhps      [r2 + r4],       m7

    mova        m4,        m3
    pmaddwd     m4,        [r5 + 5 * 16]              ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 + 5 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r5 - 4 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 4 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r5 - 13 * 16]             ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 13 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    pmaddwd     m3,        [r5 + 10 * 16]             ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r5 + 10 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    punpckhwd   m5,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m3
    punpcklwd   m6,        m3

    punpckldq   m3,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m5, m2
    punpckhdq   m5,        m2

    movh        [r0 + 8],            m3
    movhps      [r0 + r1 + 8],       m3
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m5
    movhps      [r0 + r4 + 8],       m5
    RET

cglobal intra_pred_ang8_14, 3,6,8
    lea         r5,        [ang_table + 18 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 32]                  ; [7 6 5 4 3 2 1 0]
    pinsrw      m0,        [r2], 0
    movu        m1,        [r2 + 34]                  ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r5 + 1 * 16]              ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 + 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r5 - 12 * 16]             ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r5 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m0,        m3, 12
    movu        m1,        [r2]
    pshufb      m1,        [pw_ang8_14]
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r5 + 7 * 16]              ; [25]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 + 7 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    mova        m5,        m3
    pmaddwd     m5,        [r5 - 6 * 16]              ; [12]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r5 - 6 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    punpckhwd   m7,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m7, m2
    punpckhdq   m7,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r2,              [r0 + r1 * 4]
    movh        [r2],            m6
    movhps      [r2 + r1],       m6
    movh        [r2 + r1 * 2],   m7
    movhps      [r2 + r4],       m7

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m4,        m3
    pmaddwd     m4,        [r5 + 13 * 16]             ; [31]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 + 13 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r5]                       ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r5 - 13 * 16]             ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 13 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    pmaddwd     m3,        [r5 + 6 * 16]              ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r5 + 6 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    punpckhwd   m5,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m3
    punpcklwd   m6,        m3

    punpckldq   m3,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m5, m2
    punpckhdq   m5,        m2

    movh        [r0 + 8],            m3
    movhps      [r0 + r1 + 8],       m3
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m5
    movhps      [r0 + r4 + 8],       m5
    RET

cglobal intra_pred_ang8_15, 3,6,8
    lea         r5,        [ang_table + 20 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 32]                  ; [7 6 5 4 3 2 1 0]
    pinsrw      m0,        [r2], 0
    movu        m1,        [r2 + 34]                  ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r5 - 5 * 16]              ; [15]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 - 5 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    movu        m1,        [r2]
    pshufb      m1,        [pw_ang8_15]
    palignr     m3,        m1, 12

    mova        m2,        m3
    pmaddwd     m2,        [r5 + 10 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 + 10 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r5 - 7 * 16]              ; [13]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 7 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m5,        m3
    pmaddwd     m5,        [r5 + 8 * 16]              ; [28]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r5 + 8 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    punpckhwd   m7,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m7, m2
    punpckhdq   m7,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r3,              [r0 + r1 * 4]
    movh        [r3],            m6
    movhps      [r3 + r1],       m6
    movh        [r3 + r1 * 2],   m7
    movhps      [r3 + r4],       m7

    mova        m4,        m3
    pmaddwd     m4,        [r5 - 9 * 16]              ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 - 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m2,        m3
    pmaddwd     m2,        [r5 + 6 * 16]              ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 + 6 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r5 - 11 * 16]             ; [9]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 11 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12
    pinsrw      m3,        [r2 + 16], 0

    pmaddwd     m3,        [r5 + 4 * 16]              ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r5 + 4 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    punpckhwd   m5,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m3
    punpcklwd   m6,        m3

    punpckldq   m3,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m5, m2
    punpckhdq   m5,        m2

    movh        [r0 + 8],            m3
    movhps      [r0 + r1 + 8],       m3
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m5
    movhps      [r0 + r4 + 8],       m5
    RET

cglobal intra_pred_ang8_16, 3,6,8
    lea         r5,        [ang_table + 13 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 32]                  ; [7 6 5 4 3 2 1 0]
    pinsrw      m0,        [r2], 0
    movu        m1,        [r2 + 34]                  ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r5 - 2 * 16]              ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 - 2 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    movu        m1,        [r2]
    pshufb      m1,        [pw_ang8_16]
    palignr     m3,        m1, 12

    mova        m2,        m3
    pmaddwd     m2,        [r5 + 9 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 + 9 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r5 - 12 * 16]             ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 12 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m5,        m3
    pmaddwd     m5,        [r5 - 1 * 16]              ; [12]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r5 - 1 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    punpckhwd   m7,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m7, m2
    punpckhdq   m7,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r3,              [r0 + r1 * 4]
    movh        [r3],            m6
    movhps      [r3 + r1],       m6
    movh        [r3 + r1 * 2],   m7
    movhps      [r3 + r4],       m7

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m4,        m3
    pmaddwd     m4,        [r5 + 10 * 16]             ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 + 10 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r5 - 11 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 11 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r5]                       ; [13]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12
    pinsrw      m3,        [r2 + 16], 0

    pmaddwd     m3,        [r5 + 11 * 16]             ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r5 + 11 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    punpckhwd   m5,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m3
    punpcklwd   m6,        m3

    punpckldq   m3,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m5, m2
    punpckhdq   m5,        m2

    movh        [r0 + 8],            m3
    movhps      [r0 + r1 + 8],       m3
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m5
    movhps      [r0 + r4 + 8],       m5
    RET

cglobal intra_pred_ang8_17, 3,6,8
    lea         r5,        [ang_table + 17 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 32]                  ; [7 6 5 4 3 2 1 0]
    pinsrw      m0,        [r2], 0
    movu        m1,        [r2 + 34]                  ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r5 - 11 * 16]             ; [6]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 - 11 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    movu        m1,        [r2]
    pshufb      m1,        [pw_ang8_17]
    palignr     m3,        m1, 12

    mova        m2,        m3
    pmaddwd     m2,        [r5 - 5 * 16]              ; [12]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 5 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r5 + 1 * 16]              ; [18]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 + 1 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m5,        m3
    pmaddwd     m5,        [r5 + 7 * 16]              ; [24]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r5 + 7 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    punpckhwd   m7,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m5
    punpcklwd   m6,        m5

    punpckldq   m5,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m7, m2
    punpckhdq   m7,        m2

    lea         r4,              [r1 * 3]
    movh        [r0],            m5
    movhps      [r0 + r1],       m5
    movh        [r0 + r1 * 2],   m4
    movhps      [r0 + r4],       m4
    lea         r3,              [r0 + r1 * 4]
    movh        [r3],            m6
    movhps      [r3 + r1],       m6
    movh        [r3 + r1 * 2],   m7
    movhps      [r3 + r4],       m7

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m4,        m3
    pmaddwd     m4,        [r5 + 13 * 16]             ; [30]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r5 + 13 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r5 - 13 * 16]             ; [4]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 13 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r5 - 7 * 16]              ; [10]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r5 - 7 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    pmaddwd     m3,        [r5 - 1 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r5 - 1 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    punpckhwd   m5,        m4, m2
    punpcklwd   m4,        m2
    punpckhwd   m2,        m6, m3
    punpcklwd   m6,        m3

    punpckldq   m3,        m4, m6
    punpckhdq   m4,        m6
    punpckldq   m6,        m5, m2
    punpckhdq   m5,        m2

    movh        [r0 + 8],            m3
    movhps      [r0 + r1 + 8],       m3
    movh        [r0 + r1 * 2 + 8],   m4
    movhps      [r0 + r4 + 8],       m4
    lea         r0,                  [r0 + r1 * 4]
    movh        [r0 + 8],            m6
    movhps      [r0 + r1 + 8],       m6
    movh        [r0 + r1 * 2 + 8],   m5
    movhps      [r0 + r4 + 8],       m5
    RET

cglobal intra_pred_ang8_18, 3,4,3
    add         r1,              r1
    lea         r3,              [r1 * 3]
    movu        m1,              [r2]
    movu        m0,              [r2 + 34]
    pshufb      m0,              [pw_swap16]
    movu        [r0],            m1
    palignr     m2,              m1, m0, 14
    movu        [r0 + r1],       m2
    palignr     m2,              m1, m0, 12
    movu        [r0 + r1 * 2],   m2
    palignr     m2,              m1, m0, 10
    movu        [r0 + r3],       m2
    lea         r0,              [r0 + r1 * 4]
    palignr     m2,              m1, m0, 8
    movu        [r0],            m2
    palignr     m2,              m1, m0, 6
    movu        [r0 + r1],       m2
    palignr     m2,              m1, m0, 4
    movu        [r0 + r1 * 2],   m2
    palignr     m1,              m0, 2
    movu        [r0 + r3],       m1
    RET

cglobal intra_pred_ang8_19, 3,5,8
    lea         r3,        [ang_table + 17 * 16]
    add         r1,        r1

    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 11 * 16]             ; [6]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 11 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    movu        m1,        [r2 + 32]
    pinsrw      m1,        [r2], 0
    pshufb      m1,        [pw_ang8_17]
    palignr     m3,        m1, 12

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 5 * 16]              ; [12]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 5 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 1 * 16]              ; [18]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 + 1 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m5,        m3
    pmaddwd     m5,        [r3 + 7 * 16]              ; [24]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r3 + 7 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 13 * 16]             ; [30]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 13 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 13 * 16]             ; [4]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 13 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 7 * 16]              ; [10]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 7 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    pmaddwd     m3,        [r3 - 1 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 - 1 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m3
    RET

cglobal intra_pred_ang8_20, 3,5,8
    lea         r3,        [ang_table + 13 * 16]
    add         r1,        r1

    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 2 * 16]              ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 2 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    movu        m1,        [r2 + 32]
    pinsrw      m1,        [r2], 0
    pshufb      m1,        [pw_ang8_16]
    palignr     m3,        m1, 12

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 9 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 + 9 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 12 * 16]             ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 12 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m5,        m3
    pmaddwd     m5,        [r3 - 1 * 16]              ; [12]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r3 - 1 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 10 * 16]             ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 10 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 11 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 11 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r3]                       ; [13]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12
    pinsrw      m3,        [r2 + 16 + 32], 0

    pmaddwd     m3,        [r3 + 11 * 16]             ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 + 11 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m3
    RET

cglobal intra_pred_ang8_21, 3,5,8
    lea         r3,        [ang_table + 20 * 16]
    add         r1,        r1

    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 5 * 16]              ; [15]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 5 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    movu        m1,        [r2 + 32]
    pinsrw      m1,        [r2], 0
    pshufb      m1,        [pw_ang8_15]
    palignr     m3,        m1, 12

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 10 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 + 10 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 7 * 16]              ; [13]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 7 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m5,        m3
    pmaddwd     m5,        [r3 + 8 * 16]              ; [28]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r3 + 8 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 9 * 16]              ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 6 * 16]              ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 + 6 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 11 * 16]             ; [9]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 11 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12
    pinsrw      m3,        [r2 + 16 + 32], 0

    pmaddwd     m3,        [r3 + 4 * 16]              ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 + 4 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m3
    RET

cglobal intra_pred_ang8_22, 3,5,8
    lea         r3,        [ang_table + 18 * 16]
    add         r1,        r1

    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 1 * 16]              ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 12 * 16]             ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m0,        m3, 12
    movu        m1,        [r2 + 32]
    pinsrw      m1,        [r2], 0
    pshufb      m1,        [pw_ang8_14]
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 7 * 16]              ; [25]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 + 7 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    mova        m5,        m3
    pmaddwd     m5,        [r3 - 6 * 16]              ; [12]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r3 - 6 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 13 * 16]             ; [31]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 13 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3]                       ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 13 * 16]             ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 13 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    pmaddwd     m3,        [r3 + 6 * 16]              ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 + 6 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m3
    RET

cglobal intra_pred_ang8_23, 3,5,8
    lea         r3,        [ang_table + 14 * 16]
    add         r1,        r1

    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 9 * 16]              ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3]                       ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 9 * 16]              ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m0,        m3, 12
    movu        m1,        [r2 + 32]
    pinsrw      m1,        [r2], 0
    pshufb      m1,        [pw_ang8_13]
    palignr     m3,        m1, 12

    mova        m5,        m3
    pmaddwd     m5,        [r3 + 14 * 16]             ; [28]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m7,        m0
    pmaddwd     m7,        [r3 + 14 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m5,        m7

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 5 * 16]              ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 5 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 4 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 4 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m2,        m5

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 13 * 16]             ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 13 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pslldq      m1,        2
    palignr     m0,        m3, 12
    palignr     m3,        m1, 12

    pmaddwd     m3,        [r3 + 10 * 16]             ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 + 10 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m3
    RET

cglobal intra_pred_ang8_24, 3,5,7
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1

    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 11 * 16]             ; [27]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 11 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 6 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 1 * 16]              ; [17]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m5,        m3
    pmaddwd     m5,        [r3 - 4 * 16]              ; [12]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 9 * 16]              ; [7]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 14 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m0,        m3, 12
    movu        m1,        [r2 + 32]
    pinsrw      m1,        [r2], 0
    pshufb      m1,        [pw_ang8_12]
    palignr     m3,        m1, 12

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 13 * 16]             ; [29]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 + 13 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pmaddwd     m3,        [r3 + 8 * 16]              ; [24]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 + 8 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m3
    RET

cglobal intra_pred_ang8_25, 3,5,7
    lea         r3,        [ang_table + 23 * 16]
    add         r1,        r1

    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 7 * 16]              ; [30]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 7 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 5 * 16]              ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 3 * 16]              ; [26]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m5,        m3
    pmaddwd     m5,        [r3 + 1 * 16]              ; [24]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 1 * 16]              ; [22]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 3 * 16]              ; [20]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 5 * 16]              ; [18]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 - 5 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pmaddwd     m3,        [r3 - 7 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 - 7 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m3
    RET

cglobal intra_pred_ang8_26, 3,6,3
    movu        m0,             [r2 + 2]            ; [8 7 6 5 4 3 2 1]
    add         r1,             r1
    lea         r5,             [r1 * 3]

    movu        [r0],           m0
    movu        [r0 + r1],      m0
    movu        [r0 + r1 * 2],  m0
    movu        [r0 + r5],      m0

    lea         r3,             [r0 + r1 *4]
    movu        [r3],           m0
    movu        [r3 + r1],      m0
    movu        [r3 + r1 * 2],  m0
    movu        [r3 + r5],      m0

    cmp         r4m,            byte 0
    jz         .quit

    ; filter
    pshufb      m0,             [pb_01]
    pinsrw      m1,             [r2], 0             ; [3 2 1 0]
    pshufb      m2,             m1, [pb_01]         ; [0 0 0 0 0 0 0 0]
    movu        m1,             [r2 + 2 + 32]       ; [8 7 6 5 4 3 2 1]
    psubw       m1,             m2
    psraw       m1,             1
    paddw       m0,             m1
    pxor        m1,             m1
    pmaxsw      m0,             m1
    pminsw      m0,             [pw_pixel_max]
    pextrw      [r0],          m0, 0
    pextrw      [r0 + r1],     m0, 1
    pextrw      [r0 + r1 * 2], m0, 2
    pextrw      [r0 + r5],     m0, 3
    pextrw      [r3],          m0, 4
    pextrw      [r3 + r1],     m0, 5
    pextrw      [r3 + r1 * 2], m0, 6
    pextrw      [r3 + r5],     m0, 7
.quit:
    RET

cglobal intra_pred_ang8_27, 3,5,7
    lea         r3,        [ang_table + 9 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 4]                   ; [9 8 7 6 5 4 3 2]

    punpcklwd   m3,        m0, m1                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m1                         ; [9 8 8 7 7 6 6 5]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 7 * 16]              ; [2]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 7 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 5 * 16]              ; [4]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 3 * 16]              ; [6]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m5,        m3
    pmaddwd     m5,        [r3 - 1 * 16]              ; [8]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 1 * 16]              ; [10]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 3 * 16]              ; [12]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 5 * 16]              ; [14]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m5,        m0
    pmaddwd     m5,        [r3 + 5 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m6,        m5

    pmaddwd     m3,        [r3 + 7 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 + 7 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m3
    RET

cglobal intra_pred_ang8_28, 3,5,7
    lea         r3,        [ang_table + 17 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 4]                   ; [9 8 7 6 5 4 3 2]

    punpcklwd   m3,        m0, m1                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m1                         ; [9 8 8 7 7 6 6 5]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 12 * 16]             ; [5]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 12 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 7 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 2 * 16]              ; [15]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m5,        m3
    pmaddwd     m5,        [r3 + 3 * 16]              ; [20]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 8 * 16]              ; [25]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 8 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 13 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    movh        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]

    palignr     m6,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m5,        m6
    pmaddwd     m6,        [r3 - 14 * 16]             ; [3]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m1,        m0, 4                      ; [10 9 9 8 8 7 7 6]
    mova        m3,        m1
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m5,        [r3 - 9 * 16]              ; [8]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    pmaddwd     m3,        [r3 - 9 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m5,        m3

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m5
    RET

cglobal intra_pred_ang8_29, 3,5,8
    lea         r3,        [ang_table + 18 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 9 * 16]              ; [9]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3]                       ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 9 * 16]              ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m7,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m7,        [r3 - 14 * 16]             ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7

    palignr     m4,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m4
    pmaddwd     m4,        [r3 - 5 * 16]              ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m2
    pmaddwd     m2,        [r3 - 5 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    pmaddwd     m2,        m6, [r3 + 4 * 16]          ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m1,        m7, [r3 + 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 13 * 16]             ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 13 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 - 10 * 16]             ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m5,        m0, 8                      ; [11 10 10 9 9 8 8 7]
    pmaddwd     m5,        [r3 - 10 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m7,        m5

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7
    RET

cglobal intra_pred_ang8_30, 3,5,8
    lea         r3,        [ang_table + 14 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 1 * 16]              ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 12 * 16]             ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m6,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m7,        m6
    pmaddwd     m6,        [r3 - 7 * 16]              ; [7]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m7,        [r3 + 6 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 + 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7

    palignr     m4,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    mova        m6,        m4
    pmaddwd     m4,        [r3 - 13 * 16]             ; [1]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    mova        m7,        m2
    pmaddwd     m2,        [r3 - 13 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    pmaddwd     m2,        m6, [r3]                   ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m1,        m7, [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 13 * 16]             ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 13 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m7,        [r3 - 6 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m5,        m0, 12                     ; [12 11 11 10 10 9 9 8]
    pmaddwd     m5,        [r3 - 6 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m7,        m5

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7
    RET

cglobal intra_pred_ang8_31, 3,5,8
    lea         r3,        [ang_table + 13 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 4 * 16]              ; [17]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 4 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 11 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 6 * 16]              ; [19]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 6 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 - 9 * 16]              ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m1,        [r3 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7

    palignr     m4,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m4,        [r3 + 8 * 16]              ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m2,        [r3 + 8 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 7 * 16]              ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 10 * 16]             ; [23]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 10 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    mova        m7,        m0
    pmaddwd     m7,        [r3 - 5 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m5
    pmaddwd     m1,        [r3 - 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7
    RET

cglobal intra_pred_ang8_32, 3,5,8
    lea         r3,        [ang_table + 19 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 2 * 16]              ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 2 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 9 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 12 * 16]             ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 12 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 + 1 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m1,        [r3 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7

    palignr     m4,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    mova        m2,        m4
    pmaddwd     m4,        [r3 - 10 * 16]             ; [ 9]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m3,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m6,        m3
    pmaddwd     m3,        [r3 - 10 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m4,        m3

    pmaddwd     m2,        [r3 + 11 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m6,        [r3 + 11 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m0
    pmaddwd     m6,        [r3]                       ; [19]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m5
    pmaddwd     m7,        [r3]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    movh        m1,        [r2 + 26]                  ; [16 15 14 13]
    palignr     m7,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m7,        [r3 - 11 * 16]             ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, 4                      ; [14 13 13 12 12 11 11 10]
    pmaddwd     m1,        [r3 - 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7
    RET

cglobal intra_pred_ang8_33, 3,5,8
    lea         r3,        [ang_table + 14 * 16]
    add         r1,        r1

    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]
    punpckhwd   m1,        m4                         ; [x 16 16 15 15 14 14 13]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 12 * 16]             ; [26]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 12 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m2,        [r3 + 6 * 16]              ; [20]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m6,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m6,        [r3 + 6 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    palignr     m6,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m6,        [r3]                       ; [14]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m7,        [r3]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m7,        [r3 - 6 * 16]              ; [ 8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m3,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    pmaddwd     m3,        [r3 - 6 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m7,        m3

    lea         r4,              [r1 * 3]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7

    mova        m4,        m0
    pmaddwd     m4,        [r3 - 12 * 16]             ; [ 2]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m5
    pmaddwd     m2,        [r3 - 12 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m0
    pmaddwd     m2,        [r3 + 14 * 16]             ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m5
    pmaddwd     m6,        [r3 + 14 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    palignr     m6,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m6,        [r3 + 8 * 16]              ; [22]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m7,        m1, m5, 4                  ; [14 13 13 12 12 11 11 10]
    pmaddwd     m7,        [r3 + 8 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m7,        [r3 + 2 * 16]              ; [16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, 8                      ; [15 14 14 13 13 12 12 11]
    pmaddwd     m1,        [r3 + 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r0,              [r0 + r1 * 4]
    movu        [r0],            m4
    movu        [r0 + r1],       m2
    movu        [r0 + r1 * 2],   m6
    movu        [r0 + r4],       m7
    RET

%macro TRANSPOSE_STORE 6
    jnz         .skip%6
    punpckhwd   %5,        %1, %2
    punpcklwd   %1,        %2
    punpckhwd   %2,        %3, %4
    punpcklwd   %3,        %4

    punpckldq   %4,        %1, %3
    punpckhdq   %1,        %3
    punpckldq   %3,        %5, %2
    punpckhdq   %5,        %2

    movh        [r0 + %6],            %4
    movhps      [r0 + r1 + %6],       %4
    movh        [r0 + r1 * 2 + %6],   %1
    movhps      [r0 + r4 + %6],       %1
    lea         r5,                   [r0 + r1 * 4]
    movh        [r5 + %6],            %3
    movhps      [r5 + r1 + %6],       %3
    movh        [r5 + r1 * 2 + %6],   %5
    movhps      [r5 + r4 + %6],       %5
    jmp         .end%6

.skip%6:
    movu        [r5],            %1
    movu        [r5 + r1],       %2
    movu        [r5 + r1 * 2],   %3
    movu        [r5 + r4],       %4
.end%6:
%endmacro

INIT_XMM sse4
cglobal ang16_mode_3_33
    test        r6d,       r6d
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]
    punpckhwd   m1,        m4                         ; [x 16 16 15 15 14 14 13]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 10 * 16]             ; [26]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 10 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m2,        [r3 + 4 * 16]              ; [20]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m6,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m6,        [r3 + 4 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    palignr     m6,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m6,        [r3 - 2 * 16]              ; [14]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m7,        [r3 - 2 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m7,        [r3 - 8 * 16]              ; [ 8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m3,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    pmaddwd     m3,        [r3 - 8 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m7,        m3

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m3, 0

    mova        m4,        m0
    pmaddwd     m4,        [r3 - 14 * 16]             ; [ 2]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m5
    pmaddwd     m2,        [r3 - 14 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m0
    pmaddwd     m2,        [r3 + 12 * 16]             ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m5
    pmaddwd     m6,        [r3 + 12 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    palignr     m6,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m6,        [r3 + 6 * 16]              ; [22]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m7,        m1, m5, 4                  ; [14 13 13 12 12 11 11 10]
    pmaddwd     m7,        [r3 + 6 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m7,        [r3]                       ; [16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, 8                      ; [15 14 14 13 13 12 12 11]
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m3, 8

    movu        m1,        [r2 + 26]                  ; [20 19 18 17 16 15 14 13]
    psrldq      m4,        m1, 2                      ; [x 20 19 18 17 16 15 14]

    punpcklwd   m3,        m1, m4                     ; [17 16 16 15 15 14 14 13]
    punpckhwd   m1,        m4                         ; [x 20 20 19 19 18 18 17]

    palignr     m4,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    pmaddwd     m4,        [r3 - 6 * 16]              ; [10]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m3, m5, 12                 ; [15 16 15 14 14 13 13 12]
    pmaddwd     m2,        [r3 - 6 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m5
    pmaddwd     m2,        [r3 - 12 * 16]             ; [4]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m3
    pmaddwd     m6,        [r3 - 12 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m5
    pmaddwd     m6,        [r3 + 14 * 16]             ; [30]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m3
    pmaddwd     m7,        [r3 + 14 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m3, m5, 4                  ; [14 13 13 12 12 11 11 10]
    pmaddwd     m7,        [r3 + 8 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m0,        m1, m3, 4                  ; [18 17 17 16 16 15 15 14]
    pmaddwd     m0,        [r3 + 8 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m7,        m0

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m0, 16

    palignr     m4,        m3, m5, 8                  ; [15 14 14 13 13 12 12 11]
    pmaddwd     m4,        [r3 + 2 * 16]              ; [18]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m1, m3, 8                  ; [19 18 18 17 17 16 16 15]
    pmaddwd     m2,        [r3 + 2 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m3, m5, 12                 ; [16 15 15 14 14 13 13 12]
    pmaddwd     m2,        [r3 - 4 * 16]              ; [12]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m6,        m1, m3, 12                 ; [20 19 19 18 18 17 17 16]
    pmaddwd     m6,        [r3 - 4 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    pinsrw      m1,        [r2 + 42], 7
    pmaddwd     m3,        [r3 - 10 * 16]             ; [6]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m1,        [r3 - 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m3,        m1

    movu        m7,        [r2 + 28]

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m3, m7, m0, 24

    ret

cglobal ang16_mode_4_32
    test        r6d,       r6d
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 3 * 16]              ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 3 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 8 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 13 * 16]             ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 13 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 + 2 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m1,        [r3 + 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    palignr     m4,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    mova        m2,        m4
    pmaddwd     m4,        [r3 - 9 * 16]              ; [9]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m7,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m6,        m7
    pmaddwd     m7,        [r3 - 9 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m4,        m7

    pmaddwd     m2,        [r3 + 12 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m6,        [r3 + 12 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m0
    pmaddwd     m6,        [r3 + 1 * 16]              ; [19]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m5
    pmaddwd     m7,        [r3 + 1 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    movu        m1,        [r2 + 26]                  ; [20 19 18 17 16 15 14 13]

    palignr     m7,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m7,        [r3 - 10 * 16]             ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m3,        m1, m5, 4                  ; [14 13 13 12 12 11 11 10]
    pmaddwd     m3,        [r3 - 10 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m7,        m3

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m3, 8

    psrldq      m4,        m1, 2                      ; [x 20 19 18 17 16 15 14]

    punpcklwd   m3,        m1, m4                     ; [17 16 16 15 15 14 14 13]
    punpckhwd   m1,        m4                         ; [x 20 20 19 19 18 18 17]

    palignr     m4,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m4,        [r3 + 11 * 16]             ; [29]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m3, m5, 4                  ; [14 13 13 12 12 11 11 10]
    pmaddwd     m2,        [r3 + 11 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m2,        [r3]                       ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m6,        m3, m5, 8                  ; [15 14 14 13 13 12 12 11]
    pmaddwd     m6,        [r3]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    palignr     m6,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m7,        m6
    pmaddwd     m6,        [r3 - 11 * 16]             ; [7]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m0,        m3, m5, 12                 ; [15 16 15 14 14 13 13 12]
    pmaddwd     m0,        [r3 - 11 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m6,        m0

    pmaddwd     m7,        [r3 + 10 * 16]             ; [28]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m0,        m3, m5, 12                 ; [15 16 15 14 14 13 13 12]
    pmaddwd     m0,        [r3 + 10 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m7,        m0

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m0, 16

    mova        m4,        m5
    pmaddwd     m4,        [r3 - 1 * 16]              ; [17]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m3
    pmaddwd     m2,        [r3 - 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m3, m5, 4                  ; [14 13 13 12 12 11 11 10]
    mova        m7,        m2
    pmaddwd     m2,        [r3 - 12 * 16]             ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m6,        m1, m3, 4                  ; [18 17 17 16 16 15 15 14]
    mova        m0,        m6
    pmaddwd     m6,        [r3 - 12 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    pmaddwd     m7,        [r3 + 9 * 16]              ; [27]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    pmaddwd     m0,        [r3 + 9 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m7,        m0

    palignr     m0,        m3, m5, 8                  ; [15 14 14 13 13 12 12 11]
    pmaddwd     m0,        [r3 - 2 * 16]              ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    palignr     m1,        m3, 8                      ; [19 18 18 17 17 16 16 15]
    pmaddwd     m1,        [r3 - 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m0,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m0, m3, 24

    ret

cglobal ang16_mode_5_31
    test        r6d,       r6d
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 1 * 16]              ; [17]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 14 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 3 * 16]              ; [19]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 3 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 - 12 * 16]             ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m1,        [r3 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    palignr     m4,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m4,        [r3 + 5 * 16]              ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m7,        [r3 + 5 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m4,        m7

    palignr     m2,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 10 * 16]             ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m1,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 7 * 16]              ; [23]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 7 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    mova        m7,        m0
    pmaddwd     m7,        [r3 - 8 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m3,        m5
    pmaddwd     m3,        [r3 - 8 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m7,        m3

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m3, 8

    movu        m1,        [r2 + 26]                  ; [20 19 18 17 16 15 14 13]
    psrldq      m4,        m1, 2                      ; [x 20 19 18 17 16 15 14]

    punpcklwd   m3,        m1, m4                     ; [17 16 16 15 15 14 14 13]

    mova        m4,        m0
    pmaddwd     m4,        [r3 + 9 * 16]              ; [25]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m5
    pmaddwd     m2,        [r3 + 9 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m6,        m2
    pmaddwd     m2,        [r3 - 6 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m7,        m3, m5, 4                  ; [14 13 13 12 12 11 11 10]
    mova        m1,        m7
    pmaddwd     m7,        [r3 - 6 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m2,        m7

    pmaddwd     m6,        [r3 + 11 * 16]             ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m1,        [r3 + 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m7,        [r3 - 4 * 16]              ; [12]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m3, m5, 8                  ; [15 14 14 13 13 12 12 11]
    pmaddwd     m1,        [r3 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    palignr     m4,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m4,        [r3 + 13 * 16]             ; [29]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m3, m5, 8                  ; [15 14 14 13 13 12 12 11]
    pmaddwd     m2,        [r3 + 13 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m2,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m7,        m2
    pmaddwd     m2,        [r3 - 2 * 16]              ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    palignr     m6,        m3, m5, 12                 ; [15 16 15 14 14 13 13 12]
    mova        m0,        m6
    pmaddwd     m6,        [r3 - 2 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    pmaddwd     m7,        [r3 + 15 * 16]             ; [31]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    pmaddwd     m0,        [r3 + 15 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m7,        m0

    pmaddwd     m5,        [r3]                       ; [16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    pmaddwd     m3,        [r3]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m5,        m3

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m5, m3, 24

    ret

cglobal ang16_mode_6_30
    test        r6d,       r6d
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 2 * 16]              ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 2 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 11 * 16]             ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m6,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m7,        m6
    pmaddwd     m6,        [r3 - 8 * 16]              ; [7]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 - 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m7,        [r3 + 5 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    palignr     m4,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    mova        m6,        m4
    pmaddwd     m4,        [r3 - 14 * 16]             ; [1]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m6
    pmaddwd     m2,        [r3 - 1 * 16]              ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m7
    pmaddwd     m1,        [r3 - 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 12 * 16]             ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 12 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m7,        [r3 - 7 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    pmaddwd     m1,        [r3 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    palignr     m4,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m4,        [r3 + 6 * 16]              ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m2,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    pmaddwd     m2,        [r3 + 6 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m0
    pmaddwd     m2,        [r3 - 13 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m7,        m5
    pmaddwd     m7,        [r3 - 13 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m2,        m7

    mova        m6,        m0
    pmaddwd     m6,        [r3]                       ; [15]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m5
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m0
    pmaddwd     m7,        [r3 + 13 * 16]             ; [28]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m5
    pmaddwd     m1,        [r3 + 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    movh        m3,        [r2 + 26]                  ; [16 15 14 13]

    palignr     m4,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m2,        m4
    pmaddwd     m4,        [r3 - 6 * 16]              ; [9]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m1,        m3, m5, 4                  ; [14 13 13 12 12 11 11 10]
    mova        m6,        m1
    pmaddwd     m1,        [r3 - 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m2,        [r3 + 7 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m6
    pmaddwd     m1,        [r3 + 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    psrldq      m3,        2
    palignr     m7,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    mova        m5,        m7
    pmaddwd     m7,        [r3 - 12 * 16]             ; [3]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m3,        m6, 4                      ; [15 14 14 13 13 12 12 11]
    mova        m1,        m3
    pmaddwd     m3,        [r3 - 12 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m7,        m3

    pmaddwd     m5,        [r3 + 1 * 16]              ; [16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    pmaddwd     m1,        [r3 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m5, m3, 24

    ret

cglobal ang16_mode_7_29
    test        r6d,       r6d
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 8 * 16]              ; [9]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 8 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 1 * 16]              ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 10 * 16]             ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m7,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m7,        [r3 - 13 * 16]             ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 - 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    palignr     m4,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m6,        m4
    pmaddwd     m4,        [r3 - 4 * 16]              ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m7,        m1
    pmaddwd     m1,        [r3 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m6
    pmaddwd     m2,        [r3 + 5 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m7
    pmaddwd     m1,        [r3 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m6,        [r3 + 14 * 16]             ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m7,        [r3 + 14 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    pmaddwd     m7,        [r3 - 9 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    pmaddwd     m1,        [r3 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    palignr     m4,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    mova        m2,        m4
    pmaddwd     m4,        [r3]                       ; [17]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m1,        m5, m0, 8                  ; [11 10 10 9 9 8 8 7]
    mova        m7,        m1
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m2,        [r3 + 9 * 16]              ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m7,        [r3 + 9 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m2,        m7

    palignr     m6,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m6,        [r3 - 14 * 16]             ; [3]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m1,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m7,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    pmaddwd     m7,        [r3 - 5 * 16]             ; [12]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    pmaddwd     m1,        [r3 - 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    palignr     m4,        m0, m3, 12                 ; [8 7 7 6 6 5 5 4]
    mova        m2,        m4
    pmaddwd     m4,        [r3 + 4 * 16]              ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m1,        m5, m0, 12                 ; [12 11 11 10 10 9 9 8]
    mova        m3,        m1
    pmaddwd     m1,        [r3 + 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m2,        [r3 + 13 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    pmaddwd     m3,        [r3 + 13 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m2,        m3

    mova        m7,        m0
    pmaddwd     m7,        [r3 - 10 * 16]             ; [7]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m3,        m5
    pmaddwd     m3,        [r3 - 10 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m7,        m3

    pmaddwd     m0,        [r3 - 1 * 16]              ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    pmaddwd     m5,        [r3 - 1 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m0,        m5

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m0, m3, 24

    ret

cglobal ang16_mode_8_28
    test        r6d,       r6d
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m2,        m1, m0, 2                  ; [9 8 7 6 5 4 3 2]
    psrldq      m4,        m1, 2                      ; [x 16 15 14 13 12 11 10]

    punpcklwd   m3,        m0, m2                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m2                         ; [9 8 8 7 7 6 6 5]
    punpcklwd   m5,        m1, m4                     ; [13 12 12 11 11 10 10 9]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 10 * 16]             ; [5]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 10 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 5 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3]                       ; [15]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r3 + 5 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 10 * 16]             ; [25]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 15 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 15 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m6,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m6,        [r3 - 12 * 16]             ; [3]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    palignr     m7,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m7,        [r3 - 12 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    palignr     m7,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m7,        [r3 - 7 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    palignr     m4,        m0, m3, 4                  ; [6 5 5 4 4 3 3 2]
    mova        m7,        m4
    pmaddwd     m4,        [r3 - 2 *16]               ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m6,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    mova        m1,        m6
    pmaddwd     m6,        [r3 - 2 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    mova        m2,        m7
    pmaddwd     m2,        [r3 + 3 * 16]              ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m1
    pmaddwd     m6,        [r3 + 3 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m7
    pmaddwd     m6,        [r3 + 8 * 16]              ; [23]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    pmaddwd     m1,        [r3 + 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m7,        [r3 + 13 * 16]             ; [28]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    palignr     m1,        m5, m0, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        [r3 + 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    palignr     m1,        m0, m3, 8                  ; [7 6 6 5 5 4 4 3]
    mova        m4,        m1
    pmaddwd     m4,        [r3 - 14 * 16]             ; [1]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    palignr     m5,        m0, 8                      ; [11 10 10 9 9 8 8 7]
    mova        m0,        m5
    pmaddwd     m0,        [r3 - 14 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m4,        m0

    mova        m2,        m1
    pmaddwd     m2,        [r3 - 9 * 16]              ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m3,        m5
    pmaddwd     m3,        [r3 - 9 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m2,        m3

    mova        m7,        m1
    pmaddwd     m7,        [r3 - 4 * 16]              ; [11]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m3,        m5
    pmaddwd     m3,        [r3 - 4 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    packusdw    m7,        m3

    pmaddwd     m1,        [r3 + 1 * 16]              ; [16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    pmaddwd     m5,        [r3 + 1 * 16]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m1,        m5

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m1, m3, 24

    ret

cglobal ang16_mode_9_27
    test        r6d,       r6d
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m1,        [r2 + 4]                   ; [9 8 7 6 5 4 3 2]

    punpcklwd   m3,        m0, m1                     ; [5 4 4 3 3 2 2 1]
    punpckhwd   m0,        m1                         ; [9 8 8 7 7 6 6 5]

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 14 * 16]             ; [2]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 - 14 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 12 * 16]             ; [4]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 10 *16]             ; [6]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r3 - 8 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 6 * 16]              ; [10]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 4 * 16]              ; [12]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 2 * 16]              ; [14]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m0
    pmaddwd     m7,        [r3 - 2 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    mova        m7,        m3
    pmaddwd     m7,        [r3]                       ; [16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 2 *16]               ; [18]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m6,        m0
    pmaddwd     m6,        [r3 + 2 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 4 * 16]              ; [20]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m0
    pmaddwd     m6,        [r3 + 4 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 6 * 16]              ; [22]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r3 + 8 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 10 * 16]             ; [26]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 12 * 16]             ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pmaddwd     m3,        [r3 + 14 * 16]             ; [30]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r3 + 14 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    movu        m7,        [r2 + 4]

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m3, m7, m1, 24

    ret

cglobal ang16_mode_11_25
    test        r6d,       r6d
    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 14 * 16]             ; [30]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r3 + 14 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 12 * 16]             ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 10 *16]             ; [26]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r3 + 8 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    mova        m4,        m3
    pmaddwd     m4,        [r3 + 6 * 16]              ; [22]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r3 + 4 * 16]              ; [20]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 + 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r3 + 2 * 16]              ; [18]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m0
    pmaddwd     m7,        [r3 + 2 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    mova        m7,        m3
    pmaddwd     m7,        [r3]                       ; [16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 2 *16]               ; [14]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m6,        m0
    pmaddwd     m6,        [r3 - 2 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 4 * 16]              ; [12]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m0
    pmaddwd     m6,        [r3 - 4 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m3
    pmaddwd     m6,        [r3 - 6 * 16]              ; [10]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r3 - 8 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    mova        m4,        m3
    pmaddwd     m4,        [r3 - 10 * 16]             ; [6]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r3 - 12 * 16]             ; [4]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r3 - 14 * 16]             ; [2]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r3 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    movu        m3,        [r2]

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m3, m1, 24

    ret

cglobal ang16_mode_12_24
    test        r3d,       r3d
    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 11 * 16]             ; [27]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r6 + 11 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 6 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r6 + 1 *16]              ; [17]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 4 * 16]              ; [12]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 9 * 16]              ; [7]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 14 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m6,        m3
    pmaddwd     m6,        [r6 + 13 * 16]             ; [29]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m0
    pmaddwd     m7,        [r6 + 13 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 8 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 3 *16]               ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 + 3 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 2 * 16]              ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 - 2 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 7 * 16]              ; [9]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 12 * 16]             ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 15 * 16]             ; [31]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 15 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 10 * 16]             ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 5 * 16]              ; [21]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    pmaddwd     m3,        [r6]                       ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r6]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m3, m1, 24

    ret

cglobal ang16_mode_13_23
    test        r3d,       r3d
    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 8 * 16]              ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r6 + 8 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 1 * 16]              ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 10 *16]             ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 13 * 16]             ; [28]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 4 * 16]              ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 5 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 14 * 16]             ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m0
    pmaddwd     m7,        [r6 - 14 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 9 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    mova        m4,        m3
    pmaddwd     m4,        [r6]                       ; [15]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 9 * 16]              ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 - 9 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m6,        m3
    pmaddwd     m6,        [r6 + 14 * 16]             ; [29]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 5 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 4 * 16]              ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 13 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 10 * 16]             ; [25]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    pmaddwd     m3,        [r6 + 1 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r6 + 1 *16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m3, m1, 24

    ret

cglobal ang16_mode_14_22
    test        r3d,       r3d
    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 1 * 16]              ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r6 + 1 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 12 * 16]             ; [6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m6,        m3
    pmaddwd     m6,        [r6 + 7 * 16]              ; [25]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 7 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 6 * 16]              ; [12]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 13 * 16]             ; [31]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r6]                       ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 13 * 16]             ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m0
    pmaddwd     m7,        [r6 - 13 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 6 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 6 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 7 * 16]              ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 - 7 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 12 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 + 12 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 1 * 16]              ; [17]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 14 * 16]             ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 5 * 16]              ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 8 * 16]              ; [10]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 11 * 16]             ; [29]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    pmaddwd     m3,        [r6 - 2 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r6 - 2 *16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m3, m1, 24

    ret

cglobal ang16_mode_15_21
    test        r3d,       r3d
    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    palignr     m6,        m0, m5, 2

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r6]                       ; [15]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r6]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    palignr     m3,        m6, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 15 * 16]             ; [30]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 15 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 2 * 16]              ; [13]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 13 * 16]             ; [28]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 4 * 16]              ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 11 * 16]             ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 6 * 16]              ; [9]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m0
    pmaddwd     m7,        [r6 - 6 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 9 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 8 * 16]              ; [7]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 - 8 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 7 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 + 7 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 10 * 16]             ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 5 * 16]              ; [20]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 5 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 12 * 16]             ; [3]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 3 * 16]              ; [18]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 3 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 14 * 16]             ; [1]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    pmaddwd     m3,        [r6 + 1 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r6 + 1 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m3, m1, 24

    ret

cglobal ang16_mode_16_20
    test        r4d,       r4d
    lea         r4,        [r1 * 3]
    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    palignr     m6,        m0, m5, 2

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 2 * 16]              ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r6 - 2 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    palignr     m3,        m6, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 9 * 16]              ; [22]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 12 * 16]             ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 1 * 16]              ; [12]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 1 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 10 * 16]             ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 11 * 16]             ; [2]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m6,        m3
    pmaddwd     m6,        [r6]                       ; [13]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m0
    pmaddwd     m7,        [r6]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 11 * 16]             ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 11 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 10 * 16]             ; [3]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 - 10 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 1 * 16]              ; [14]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 + 1 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m6,        m3
    pmaddwd     m6,        [r6 + 12 * 16]             ; [25]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 9 * 16]              ; [4]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 9 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 2 * 16]              ; [15]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m5,        [r3]
    pshufb      m5,        [pw_ang8_16]

    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 13 * 16]             ; [26]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 13 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 8 * 16]              ; [5]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    pmaddwd     m3,        [r6 + 3 * 16]              ; [16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r6 + 3 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m3, m1, 24

    ret

cglobal ang16_mode_17_19
    test        r4d,       r4d
    lea         r4,        [r1 * 3]
    movu        m0,        [r2]                       ; [7 6 5 4 3 2 1 0]
    movu        m1,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]

    palignr     m6,        m0, m5, 2

    punpcklwd   m3,        m0, m1                     ; [4 3 3 2 2 1 1 0]
    punpckhwd   m0,        m1                         ; [8 7 7 6 6 5 5 4]

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 10 * 16]             ; [6]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m2,        m0
    pmaddwd     m2,        [r6 - 10 * 16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m4,        m2

    palignr     m0,        m3, 12
    palignr     m3,        m6, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 4 * 16]              ; [12]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m6,        m3
    pmaddwd     m6,        [r6 + 2 * 16]              ; [18]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 8 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    mov         r5,        r0

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 0

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 14 * 16]             ; [30]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    mova        m2,        m3
    pmaddwd     m2,        [r6 - 12 * 16]             ; [4]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 12 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 6 * 16]              ; [10]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m7,        m0
    pmaddwd     m7,        [r6 - 6 * 16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6]                      ; [16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r0 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 8

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m4,        m3
    pmaddwd     m4,        [r6 + 6 * 16]              ; [22]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 + 6 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m4,        m6

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 12 * 16]             ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m6,        m0
    pmaddwd     m6,        [r6 + 12 * 16]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m2,        m6

    mova        m6,        m3
    pmaddwd     m6,        [r6 - 14 * 16]             ; [2]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 14 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m5,        [r3]
    pshufb      m5,        [pw_ang8_17]

    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 - 8 * 16]              ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 8 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m6, m7, m1, 16

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m4,        m3
    pmaddwd     m4,        [r6 - 2 * 16]              ; [14]
    paddd       m4,        [pd_16]
    psrld       m4,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 - 2 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m2,        m3
    pmaddwd     m2,        [r6 + 4 * 16]              ; [20]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 4 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m2,        m1

    pslldq      m5,        2
    palignr     m0,        m3, 12
    palignr     m3,        m5, 12

    mova        m7,        m3
    pmaddwd     m7,        [r6 + 10 * 16]             ; [26]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    mova        m1,        m0
    pmaddwd     m1,        [r6 + 10 * 16]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m7,        m1

    pmaddwd     m3,        [r6 - 16 * 16]
    paddd       m3,        [pd_16]
    psrld       m3,        5
    pmaddwd     m0,        [r6 - 16 * 16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m3,        m0

    lea         r5,        [r5 + r1 * 4]

    TRANSPOSE_STORE m4, m2, m7, m3, m1, 24

    ret

;------------------------------------------------------------------------------------------
; void intraPredAng16(pixel* dst, intptr_t dstStride, pixel* src, int dirMode, int bFilter)
;------------------------------------------------------------------------------------------
INIT_XMM ssse3
cglobal intra_pred_ang16_2, 3,5,5
    lea         r4,                 [r2]
    add         r2,                 64
    cmp         r3m,                byte 34
    cmove       r2,                 r4
    add         r1,                 r1
    lea         r3,                 [r1 * 3]
    movu        m0,                 [r2 + 4]
    movu        m1,                 [r2 + 20]
    movu        m2,                 [r2 + 36]

    movu        [r0],               m0
    movu        [r0 + 16],          m1
    palignr     m3,                 m1, m0, 2
    palignr     m4,                 m2, m1, 2
    movu        [r0 + r1],          m3
    movu        [r0 + r1 + 16],     m4
    palignr     m3,                 m1, m0, 4
    palignr     m4,                 m2, m1, 4
    movu        [r0 + r1 * 2],      m3
    movu        [r0 + r1 * 2 + 16], m4
    palignr     m3,                 m1, m0, 6
    palignr     m4,                 m2, m1, 6
    movu        [r0 + r3],          m3
    movu        [r0 + r3 + 16],     m4

    lea         r0,                 [r0 + r1 * 4]
    palignr     m3,                 m1, m0, 8
    palignr     m4,                 m2, m1, 8
    movu        [r0],               m3
    movu        [r0 + 16],          m4
    palignr     m3,                 m1, m0, 10
    palignr     m4,                 m2, m1, 10
    movu        [r0 + r1],          m3
    movu        [r0 + r1 + 16],     m4
    palignr     m3,                 m1, m0, 12
    palignr     m4,                 m2, m1, 12
    movu        [r0 + r1 * 2],      m3
    movu        [r0 + r1 * 2 + 16], m4
    palignr     m3,                 m1, m0, 14
    palignr     m4,                 m2, m1, 14
    movu        [r0 + r3],          m3
    movu        [r0 + r3 + 16],     m4

    movu        m0,                 [r2 + 52]
    lea         r0,                 [r0 + r1 * 4]
    movu        [r0],               m1
    movu        [r0 + 16],          m2
    palignr     m3,                 m2, m1, 2
    palignr     m4,                 m0, m2, 2
    movu        [r0 + r1],          m3
    movu        [r0 + r1 + 16],     m4
    palignr     m3,                 m2, m1, 4
    palignr     m4,                 m0, m2, 4
    movu        [r0 + r1 * 2],      m3
    movu        [r0 + r1 * 2 + 16], m4
    palignr     m3,                 m2, m1, 6
    palignr     m4,                 m0, m2, 6
    movu        [r0 + r3],          m3
    movu        [r0 + r3 + 16],     m4

    lea         r0,                 [r0 + r1 * 4]
    palignr     m3,                 m2, m1, 8
    palignr     m4,                 m0, m2, 8
    movu        [r0],               m3
    movu        [r0 + 16],          m4
    palignr     m3,                 m2, m1, 10
    palignr     m4,                 m0, m2, 10
    movu        [r0 + r1],          m3
    movu        [r0 + r1 + 16],     m4
    palignr     m3,                 m2, m1, 12
    palignr     m4,                 m0, m2, 12
    movu        [r0 + r1 * 2],      m3
    movu        [r0 + r1 * 2 + 16], m4
    palignr     m3,                 m2, m1, 14
    palignr     m4,                 m0, m2, 14
    movu        [r0 + r3],          m3
    movu        [r0 + r3 + 16],     m4
    RET

INIT_XMM sse4    
cglobal intra_pred_ang16_3, 3,7,8
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_3_33

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + r1 * 8]

    call        ang16_mode_3_33
    RET

cglobal intra_pred_ang16_33, 3,7,8
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_3_33

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + 16]

    call        ang16_mode_3_33
    RET

cglobal intra_pred_ang16_4, 3,7,8
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table + 18 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_4_32

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + r1 * 8]

    call        ang16_mode_4_32
    RET

cglobal intra_pred_ang16_32, 3,7,8
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table + 18 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_4_32

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + 16]

    call        ang16_mode_4_32
    RET

cglobal intra_pred_ang16_5, 3,7,8
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_5_31

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + r1 * 8]

    call        ang16_mode_5_31
    RET

cglobal intra_pred_ang16_31, 3,7,8
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_5_31

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + 16]

    call        ang16_mode_5_31
    RET

cglobal intra_pred_ang16_6, 3,7,8
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table + 15 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_6_30

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + r1 * 8]

    call        ang16_mode_6_30
    RET

cglobal intra_pred_ang16_30, 3,7,8
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table + 15 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_6_30

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + 16]

    call        ang16_mode_6_30
    RET

cglobal intra_pred_ang16_7, 3,7,8
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table + 17 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_7_29

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + r1 * 8]

    call        ang16_mode_7_29
    RET

cglobal intra_pred_ang16_29, 3,7,8
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table + 17 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_7_29

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + 16]

    call        ang16_mode_7_29
    RET

cglobal intra_pred_ang16_8, 3,7,8
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table + 15 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_8_28

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + r1 * 8]

    call        ang16_mode_8_28
    RET

cglobal intra_pred_ang16_28, 3,7,8
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table + 15 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_8_28

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + 16]

    call        ang16_mode_8_28
    RET

cglobal intra_pred_ang16_9, 3,7,8
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_9_27

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + r1 * 8]

    call        ang16_mode_9_27
    RET

cglobal intra_pred_ang16_27, 3,7,8
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_9_27

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + 16]

    call        ang16_mode_9_27
    RET

cglobal intra_pred_ang16_11, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_11_25

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + r1 * 8]

    call        ang16_mode_11_25

    mov         r6d,       [rsp]
    mov         [r2 - 16], r6w
    RET

cglobal intra_pred_ang16_25, 3,7,8
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table + 16 * 16]
    add         r1,        r1
    lea         r4,        [r1 * 3]

    call        ang16_mode_11_25

    lea         r2,        [r2 + 16]
    lea         r0,        [r0 + 16]

    call        ang16_mode_11_25
    RET

cglobal intra_pred_ang16_12, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r4,        [r1 * 3]
    lea         r6,        [ang_table + 16 * 16]
    movu        m5,        [r2]
    pshufb      m5,        [pw_ang8_12]
    pinsrw      m5,        [r2 + 26], 5
    xor         r3d,       r3d
    add         r2,        64

    call        ang16_mode_12_24

    lea         r0,        [r0 + r1 * 8]
    movu        m5,        [r2 + 2]
    lea         r2,        [r2 + 16]

    call        ang16_mode_12_24

    mov         r6d,       [rsp]
    mov         [r2 - 16], r6w
    RET

cglobal intra_pred_ang16_24, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r4,        [r1 * 3]
    lea         r6,        [ang_table + 16 * 16]
    movu        m5,        [r2 + 64]
    pshufb      m5,        [pw_ang8_12]
    pinsrw      m5,        [r2 + 26 + 64], 5
    xor         r3d,       r3d
    inc         r3d

    call        ang16_mode_12_24

    lea         r0,        [r0 + 16]
    movu        m5,        [r2 + 2]
    lea         r2,        [r2 + 16]

    call        ang16_mode_12_24

    mov         r6d,       [rsp]
    mov         [r2 + 48], r6w
    RET

cglobal intra_pred_ang16_13, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r4,        [r1 * 3]
    lea         r6,        [ang_table + 15 * 16]
    movu        m5,        [r2]
    pshufb      m5,        [pw_ang16_13]
    movu        m6,        [r2 + 14]
    pshufb      m6,        [pw_ang8_13]
    pslldq      m6,        2
    palignr     m5,        m6, 6
    xor         r3d,       r3d
    add         r2,        64

    call        ang16_mode_13_23

    lea         r0,        [r0 + r1 * 8]
    movu        m5,        [r2 + 2]
    lea         r2,        [r2 + 16]

    call        ang16_mode_13_23

    mov         r6d,       [rsp]
    mov         [r2 - 16], r6w
    RET

cglobal intra_pred_ang16_23, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r4,        [r1 * 3]
    lea         r6,        [ang_table + 15 * 16]
    movu        m5,        [r2 + 64]
    pshufb      m5,        [pw_ang16_13]
    movu        m6,        [r2 + 14 + 64]
    pshufb      m6,        [pw_ang8_13]
    pslldq      m6,        2
    palignr     m5,        m6, 6
    xor         r3d,       r3d
    inc         r3d

    call        ang16_mode_13_23

    lea         r0,        [r0 + 16]
    movu        m5,        [r2 + 2]
    lea         r2,        [r2 + 16]

    call        ang16_mode_13_23

    mov         r6d,       [rsp]
    mov         [r2 + 48], r6w
    RET

cglobal intra_pred_ang16_14, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r4,        [r1 * 3]
    lea         r6,        [ang_table + 18 * 16]
    movu        m6,        [r2]
    pshufb      m6,        [pw_ang8_14]
    movu        m5,        [r2 + 20]
    pshufb      m5,        [pw_ang8_14]
    punpckhqdq  m5,        m6
    xor         r3d,       r3d
    add         r2,        64

    call        ang16_mode_14_22

    lea         r0,        [r0 + r1 * 8]
    movu        m5,        [r2 + 2]
    lea         r2,        [r2 + 16]

    call        ang16_mode_14_22

    mov         r6d,       [rsp]
    mov         [r2 - 16], r6w
    RET

cglobal intra_pred_ang16_22, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r4,        [r1 * 3]
    lea         r6,        [ang_table + 18 * 16]
    movu        m6,        [r2 + 64]
    pshufb      m6,        [pw_ang8_14]
    movu        m5,        [r2 + 20 + 64]
    pshufb      m5,        [pw_ang8_14]
    punpckhqdq  m5,        m6
    xor         r3d,       r3d
    inc         r3d

    call        ang16_mode_14_22

    lea         r0,        [r0 + 16]
    movu        m5,        [r2 + 2]
    lea         r2,        [r2 + 16]

    call        ang16_mode_14_22

    mov         r6d,       [rsp]
    mov         [r2 + 48], r6w
    RET

cglobal intra_pred_ang16_15, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r4,        [r1 * 3]
    lea         r6,        [ang_table + 15 * 16]
    movu        m6,        [r2 + 4]
    pshufb      m6,        [pw_ang8_15]
    movu        m5,        [r2 + 18]
    pshufb      m5,        [pw_ang8_15]
    punpckhqdq  m5,        m6
    xor         r3d,       r3d
    add         r2,        64

    call        ang16_mode_15_21

    lea         r0,        [r0 + r1 * 8]
    movu        m5,        [r2]
    lea         r2,        [r2 + 16]

    call        ang16_mode_15_21

    mov         r6d,       [rsp]
    mov         [r2 - 16], r6w
    RET

cglobal intra_pred_ang16_21, 3,7,8, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r4,        [r1 * 3]
    lea         r6,        [ang_table + 15 * 16]
    movu        m6,        [r2 + 4 + 64]
    pshufb      m6,        [pw_ang8_15]
    movu        m5,        [r2 + 18 + 64]
    pshufb      m5,        [pw_ang8_15]
    punpckhqdq  m5,        m6
    xor         r3d,       r3d
    inc         r3d

    call        ang16_mode_15_21

    lea         r0,        [r0 + 16]
    movu        m5,        [r2]
    lea         r2,        [r2 + 16]

    call        ang16_mode_15_21

    mov         r6d,       [rsp]
    mov         [r2 + 48], r6w
    RET

cglobal intra_pred_ang16_16, 3,7,8,0-(1*mmsize+4)
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp + 16], r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r6,        [ang_table + 13 * 16]
    movu        m6,        [r2 + 4]
    pshufb      m6,        [pw_ang16_16]
    movu        m5,        [r2 + 16]
    pshufb      m5,        [pw_ang16_16]
    punpckhqdq  m5,        m6
    mov         [rsp],     r2
    lea         r3,        [r2 + 24]
    add         r2,        64
    xor         r4,        r4

    call        ang16_mode_16_20

    lea         r0,        [r0 + r1 * 8]
    mov         r3,        [rsp]
    movu        m5,        [r2]
    lea         r2,        [r2 + 16]
    xor         r4,        r4

    call        ang16_mode_16_20

    mov         r6d,       [rsp + 16]
    mov         [r2 - 16], r6w
    RET

cglobal intra_pred_ang16_20, 3,7,8,0-(1*mmsize+4)
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp + 16], r5w
    mov         [r2 + 64], r6w

    lea         r3,        [r2 + 64]
    add         r1,        r1
    lea         r6,        [ang_table + 13 * 16]
    movu        m6,        [r3 + 4]
    pshufb      m6,        [pw_ang16_16]
    movu        m5,        [r3 + 16]
    pshufb      m5,        [pw_ang16_16]
    punpckhqdq  m5,        m6
    mov         [rsp],     r3
    lea         r3,        [r3 + 24]
    xor         r4,        r4
    inc         r4

    call        ang16_mode_16_20

    lea         r0,        [r0 + 16]
    mov         r3,        [rsp]
    movu        m5,        [r2]
    lea         r2,        [r2 + 16]
    xor         r4,        r4
    inc         r4

    call        ang16_mode_16_20
    mov         r6d,       [rsp + 16]
    mov         [r3],      r6w
    RET

cglobal intra_pred_ang16_17, 3,7,8,0-(1*mmsize+4)
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp + 16], r5w
    mov         [r2 + 64], r6w

    add         r1,        r1
    lea         r6,        [ang_table + 16 * 16]
    movu        m6,        [r2 + 2]
    pshufb      m6,        [pw_ang16_16]
    movu        m5,        [r2 + 12]
    pshufb      m5,        [pw_ang16_16]
    punpckhqdq  m5,        m6
    mov         [rsp],     r2
    lea         r3,        [r2 + 20]
    add         r2,        64
    xor         r4,        r4

    call        ang16_mode_17_19

    lea         r0,        [r0 + r1 * 8]
    mov         r3,        [rsp]
    movu        m5,        [r2]
    lea         r2,        [r2 + 16]
    xor         r4,        r4

    call        ang16_mode_17_19

    mov         r6d,       [rsp + 16]
    mov         [r2 - 16], r6w
    RET

cglobal intra_pred_ang16_19, 3,7,8,0-(1*mmsize+4)
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp + 16], r5w
    mov         [r2 + 64], r6w

    lea         r3,        [r2 + 64]
    add         r1,        r1
    lea         r6,        [ang_table + 16 * 16]
    movu        m6,        [r3 + 2]
    pshufb      m6,        [pw_ang16_16]
    movu        m5,        [r3 + 12]
    pshufb      m5,        [pw_ang16_16]
    punpckhqdq  m5,        m6
    mov         [rsp],     r3
    lea         r3,        [r3 + 20]
    xor         r4,        r4
    inc         r4

    call        ang16_mode_17_19

    lea         r0,        [r0 + 16]
    mov         r3,        [rsp]
    movu        m5,        [r2]
    lea         r2,        [r2 + 16]
    xor         r4,        r4
    inc         r4

    call        ang16_mode_17_19

    mov         r6d,       [rsp + 16]
    mov         [r3],      r6w
    RET

cglobal intra_pred_ang16_18, 3,5,4
    add         r1,                  r1
    lea         r4,                  [r1 * 3]
    movu        m1,                  [r2]
    movu        m3,                  [r2 + 16]
    movu        m0,                  [r2 + 2 + 64]
    pshufb      m0,                  [pw_swap16]
    movu        [r0],                m1
    movu        [r0 + 16],           m3
    palignr     m2,                  m1, m0, 14
    movu        [r0 + r1],           m2
    palignr     m2,                  m3, m1, 14
    movu        [r0 + r1 + 16],      m2
    palignr     m2,                  m1, m0, 12
    movu        [r0 + r1 * 2],       m2
    palignr     m2,                  m3, m1, 12
    movu        [r0 + r1 * 2 + 16],  m2
    palignr     m2,                  m1, m0, 10
    movu        [r0 + r4],           m2
    palignr     m2,                  m3, m1, 10
    movu        [r0 + r4 + 16],      m2

    lea         r0,                  [r0 + r1 * 4]
    palignr     m2,                  m1, m0, 8
    movu        [r0],                m2
    palignr     m2,                  m3, m1, 8
    movu        [r0 + 16],           m2
    palignr     m2,                  m1, m0, 6
    movu        [r0 + r1],           m2
    palignr     m2,                  m3, m1, 6
    movu        [r0 + r1 + 16],      m2
    palignr     m2,                  m1, m0, 4
    movu        [r0 + r1 * 2],       m2
    palignr     m2,                  m3, m1, 4
    movu        [r0 + r1 * 2 + 16],  m2
    palignr     m2,                  m1, m0, 2
    movu        [r0 + r4],           m2
    palignr     m3,                  m1, 2
    movu        [r0 + r4 + 16],      m3

    lea         r0,                  [r0 + r1 * 4]
    movu        [r0],                m0
    movu        [r0 + 16],           m1
    movu        m3,                  [r2 + 18 + 64]
    pshufb      m3,                  [pw_swap16]
    palignr     m2,                  m0, m3, 14
    movu        [r0 + r1],           m2
    palignr     m2,                  m1, m0, 14
    movu        [r0 + r1 + 16],      m2
    palignr     m2,                  m0, m3, 12
    movu        [r0 + r1 * 2],       m2
    palignr     m2,                  m1, m0, 12
    movu        [r0 + r1 * 2 + 16],  m2
    palignr     m2,                  m0, m3, 10
    movu        [r0 + r4],           m2
    palignr     m2,                  m1, m0, 10
    movu        [r0 + r4 + 16],      m2

    lea         r0,                  [r0 + r1 * 4]
    palignr     m2,                  m0, m3, 8
    movu        [r0],                m2
    palignr     m2,                  m1, m0, 8
    movu        [r0 + 16],           m2
    palignr     m2,                  m0, m3, 6
    movu        [r0 + r1],           m2
    palignr     m2,                  m1, m0, 6
    movu        [r0 + r1 + 16],      m2
    palignr     m2,                  m0, m3, 4
    movu        [r0 + r1 * 2],       m2
    palignr     m2,                  m1, m0, 4
    movu        [r0 + r1 * 2 + 16],  m2
    palignr     m2,                  m0, m3, 2
    movu        [r0 + r4],           m2
    palignr     m1,                  m0, 2
    movu        [r0 + r4 + 16],      m1
    RET

cglobal intra_pred_ang16_10, 3,6,4
    mov         r5d,                    r4m
    movu        m1,                     [r2 + 2 + 64]       ; [8 7 6 5 4 3 2 1]
    movu        m3,                     [r2 + 18 + 64]      ; [16 15 14 13 12 11 10 9]
    pshufb      m0,                     m1, [pb_01]  ; [1 1 1 1 1 1 1 1]
    add         r1,                     r1
    lea         r4,                     [r1 * 3]

    psrldq      m1,                     2
    pshufb      m2,                     m1, [pb_01]  ; [2 2 2 2 2 2 2 2]
    movu        [r0 + r1],              m2
    movu        [r0 + r1 + 16],         m2
    psrldq      m1,                     2
    pshufb      m2,                     m1, [pb_01]  ; [3 3 3 3 3 3 3 3]
    movu        [r0 + r1 * 2],          m2
    movu        [r0 + r1 * 2 + 16],     m2
    psrldq      m1,                     2
    pshufb      m2,                     m1, [pb_01]  ; [4 4 4 4 4 4 4 4]
    movu        [r0 + r4],              m2
    movu        [r0 + r4 + 16],         m2

    lea         r3,                     [r0 + r1 *4]
    psrldq      m1,                     2
    pshufb      m2,                     m1, [pb_01]  ; [5 5 5 5 5 5 5 5]
    movu        [r3],                   m2
    movu        [r3 + 16],              m2
    psrldq      m1,                     2
    pshufb      m2,                     m1, [pb_01]  ; [6 6 6 6 6 6 6 6]
    movu        [r3 + r1],              m2
    movu        [r3 + r1 + 16],         m2
    psrldq      m1,                     2
    pshufb      m2,                     m1, [pb_01]  ; [7 7 7 7 7 7 7 7]
    movu        [r3 + r1 * 2],          m2
    movu        [r3 + r1 * 2 + 16],     m2
    psrldq      m1,                     2
    pshufb      m2,                     m1, [pb_01]  ; [8 8 8 8 8 8 8 8]
    movu        [r3 + r4],              m2
    movu        [r3 + r4 + 16],         m2

    lea         r3,                     [r3 + r1 *4]
    pshufb      m2,                     m3, [pb_01]  ; [9 9 9 9 9 9 9 9]
    movu        [r3],                   m2
    movu        [r3 + 16],              m2
    psrldq      m3,                     2
    pshufb      m2,                     m3, [pb_01]  ; [10 10 10 10 10 10 10 10]
    movu        [r3 + r1],              m2
    movu        [r3 + r1 + 16],         m2
    psrldq      m3,                     2
    pshufb      m2,                     m3, [pb_01]  ; [11 11 11 11 11 11 11 11]
    movu        [r3 + r1 * 2],          m2
    movu        [r3 + r1 * 2 + 16],     m2
    psrldq      m3,                     2
    pshufb      m2,                     m3, [pb_01]  ; [12 12 12 12 12 12 12 12]
    movu        [r3 + r4],              m2
    movu        [r3 + r4 + 16],         m2

    lea         r3,                     [r3 + r1 *4]
    psrldq      m3,                     2
    pshufb      m2,                     m3, [pb_01]  ; [13 13 13 13 13 13 13 13]
    movu        [r3],                   m2
    movu        [r3 + 16],              m2
    psrldq      m3,                     2
    pshufb      m2,                     m3, [pb_01]  ; [14 14 14 14 14 14 14 14]
    movu        [r3 + r1],              m2
    movu        [r3 + r1 + 16],         m2
    psrldq      m3,                     2
    pshufb      m2,                     m3, [pb_01]  ; [15 15 15 15 15 15 15 15]
    movu        [r3 + r1 * 2],          m2
    movu        [r3 + r1 * 2 + 16],     m2
    psrldq      m3,                     2
    pshufb      m2,                     m3, [pb_01]  ; [16 16 16 16 16 16 16 16]
    movu        [r3 + r4],              m2
    movu        [r3 + r4 + 16],         m2
    mova        m3,                     m0

    cmp         r5d,                    byte 0
    jz         .quit

    ; filter
    pinsrw      m1,                     [r2], 0             ; [3 2 1 0]
    pshufb      m2,                     m1, [pb_01]  ; [0 0 0 0 0 0 0 0]
    movu        m1,                     [r2 + 2]            ; [8 7 6 5 4 3 2 1]
    movu        m3,                     [r2 + 18]           ; [16 15 14 13 12 11 10 9]
    psubw       m1,                     m2
    psubw       m3,                     m2
    psraw       m1,                     1
    psraw       m3,                     1
    paddw       m3,                     m0
    paddw       m0,                     m1
    pxor        m1,                     m1
    pmaxsw      m0,                     m1
    pminsw      m0,                     [pw_pixel_max]
    pmaxsw      m3,                     m1
    pminsw      m3,                     [pw_pixel_max]
.quit:
    movu        [r0],                   m0
    movu        [r0 + 16],              m3
    RET

cglobal intra_pred_ang16_26, 3,6,4
    mov         r5d,                r4m
    movu        m0,                 [r2 + 2]            ; [8 7 6 5 4 3 2 1]
    movu        m3,                 [r2 + 18]           ; [16 15 14 13 12 11 10 9]
    add         r1,                 r1
    lea         r4,                 [r1 * 3]

    movu        [r0],               m0
    movu        [r0 + 16],          m3
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 16],     m3
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 16], m3
    movu        [r0 + r4],          m0
    movu        [r0 + r4 + 16],     m3

    lea         r3,                 [r0 + r1 *4]
    movu        [r3],               m0
    movu        [r3 + 16],          m3
    movu        [r3 + r1],          m0
    movu        [r3 + r1 + 16],     m3
    movu        [r3 + r1 * 2],      m0
    movu        [r3 + r1 * 2 + 16], m3
    movu        [r3 + r4],          m0
    movu        [r3 + r4 + 16],     m3

    lea         r3,                 [r3 + r1 *4]
    movu        [r3],               m0
    movu        [r3 + 16],          m3
    movu        [r3 + r1],          m0
    movu        [r3 + r1 + 16],     m3
    movu        [r3 + r1 * 2],      m0
    movu        [r3 + r1 * 2 + 16], m3
    movu        [r3 + r4],          m0
    movu        [r3 + r4 + 16],     m3

    lea         r3,                 [r3 + r1 *4]
    movu        [r3],               m0
    movu        [r3 + 16],          m3
    movu        [r3 + r1],          m0
    movu        [r3 + r1 + 16],     m3
    movu        [r3 + r1 * 2],      m0
    movu        [r3 + r1 * 2 + 16], m3
    movu        [r3 + r4],          m0
    movu        [r3 + r4 + 16],     m3

    cmp         r5d,                byte 0
    jz         .quit

    ; filter

    pshufb      m0,                 [pb_01]
    pinsrw      m1,                 [r2], 0             ; [3 2 1 0]
    pshufb      m2,                 m1, [pb_01]         ; [0 0 0 0 0 0 0 0]
    movu        m1,                 [r2 + 2 + 64]       ; [8 7 6 5 4 3 2 1]
    movu        m3,                 [r2 + 18 + 64]      ; [16 15 14 13 12 11 10 9]
    psubw       m1,                 m2
    psubw       m3,                 m2
    psraw       m1,                 1
    psraw       m3,                 1
    paddw       m3,                 m0
    paddw       m0,                 m1
    pxor        m1,                 m1
    pmaxsw      m0,                 m1
    pminsw      m0,                 [pw_pixel_max]
    pmaxsw      m3,                 m1
    pminsw      m3,                 [pw_pixel_max]
    pextrw      [r0],               m0, 0
    pextrw      [r0 + r1],          m0, 1
    pextrw      [r0 + r1 * 2],      m0, 2
    pextrw      [r0 + r4],          m0, 3
    lea         r0,                 [r0 + r1 * 4]
    pextrw      [r0],               m0, 4
    pextrw      [r0 + r1],          m0, 5
    pextrw      [r0 + r1 * 2],      m0, 6
    pextrw      [r0 + r4],          m0, 7
    lea         r0,                 [r0 + r1 * 4]
    pextrw      [r0],               m3, 0
    pextrw      [r0 + r1],          m3, 1
    pextrw      [r0 + r1 * 2],      m3, 2
    pextrw      [r0 + r4],          m3, 3
    pextrw      [r3],               m3, 4
    pextrw      [r3 + r1],          m3, 5
    pextrw      [r3 + r1 * 2],      m3, 6
    pextrw      [r3 + r4],          m3, 7
.quit:
    RET

;-------------------------------------------------------------------------------------------------------
; avx2 code for intra_pred_ang16 mode 2 to 34 start
;-------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal intra_pred_ang16_2, 3,5,3
    lea         r4,                 [r2]
    add         r2,                 64
    cmp         r3m,                byte 34
    cmove       r2,                 r4
    add         r1d,                 r1d
    lea         r3,                 [r1 * 3]
    movu        m0,                 [r2 + 4]
    movu        m1,                 [r2 + 20]

    movu        [r0],               m0
    palignr     m2,                 m1, m0, 2
    movu        [r0 + r1],          m2
    palignr     m2,                 m1, m0, 4
    movu        [r0 + r1 * 2],      m2
    palignr     m2,                 m1, m0, 6
    movu        [r0 + r3],          m2

    lea         r0,                 [r0 + r1 * 4]
    palignr     m2,                 m1, m0, 8
    movu        [r0],               m2
    palignr     m2,                 m1, m0, 10
    movu        [r0 + r1],          m2
    palignr     m2,                 m1, m0, 12
    movu        [r0 + r1 * 2],      m2
    palignr     m2,                 m1, m0, 14
    movu        [r0 + r3],          m2

    movu        m0,                 [r2 + 36]
    lea         r0,                 [r0 + r1 * 4]
    movu        [r0],               m1
    palignr     m2,                 m0, m1, 2
    movu        [r0 + r1],          m2
    palignr     m2,                 m0, m1, 4
    movu        [r0 + r1 * 2],      m2
    palignr     m2,                 m0, m1, 6
    movu        [r0 + r3],          m2

    lea         r0,                 [r0 + r1 * 4]
    palignr     m2,                 m0, m1, 8
    movu        [r0],               m2
    palignr     m2,                 m0, m1, 10
    movu        [r0 + r1],          m2
    palignr     m2,                 m0, m1, 12
    movu        [r0 + r1 * 2],      m2
    palignr     m2,                 m0, m1, 14
    movu        [r0 + r3],          m2
    RET

%macro TRANSPOSE_STORE_AVX2 11
    jnz             .skip%11
    punpckhwd       m%9,  m%1,  m%2
    punpcklwd       m%1,  m%2
    punpckhwd       m%2,  m%3,  m%4
    punpcklwd       m%3,  m%4

    punpckldq       m%4,  m%1,  m%3
    punpckhdq       m%1,  m%3
    punpckldq       m%3,  m%9,  m%2
    punpckhdq       m%9,  m%2

    punpckhwd       m%10, m%5,  m%6
    punpcklwd       m%5,  m%6
    punpckhwd       m%6,  m%7,  m%8
    punpcklwd       m%7,  m%8

    punpckldq       m%8,  m%5,  m%7
    punpckhdq       m%5,  m%7
    punpckldq       m%7,  m%10, m%6
    punpckhdq       m%10, m%6

    punpcklqdq      m%6,  m%4,  m%8
    punpckhqdq      m%2,  m%4,  m%8
    punpcklqdq      m%4,  m%1,  m%5
    punpckhqdq      m%8,  m%1,  m%5

    punpcklqdq      m%1,  m%3,  m%7
    punpckhqdq      m%5,  m%3,  m%7
    punpcklqdq      m%3,  m%9,  m%10
    punpckhqdq      m%7,  m%9,  m%10

    movu            [r0 + r1 * 0 + %11], xm%6
    movu            [r0 + r1 * 1 + %11], xm%2
    movu            [r0 + r1 * 2 + %11], xm%4
    movu            [r0 + r4 * 1 + %11], xm%8

    lea             r5, [r0 + r1 * 4]
    movu            [r5 + r1 * 0 + %11], xm%1
    movu            [r5 + r1 * 1 + %11], xm%5
    movu            [r5 + r1 * 2 + %11], xm%3
    movu            [r5 + r4 * 1 + %11], xm%7

    lea             r5, [r5 + r1 * 4]
    vextracti128    [r5 + r1 * 0 + %11], m%6, 1
    vextracti128    [r5 + r1 * 1 + %11], m%2, 1
    vextracti128    [r5 + r1 * 2 + %11], m%4, 1
    vextracti128    [r5 + r4 * 1 + %11], m%8, 1

    lea             r5, [r5 + r1 * 4]
    vextracti128    [r5 + r1 * 0 + %11], m%1, 1
    vextracti128    [r5 + r1 * 1 + %11], m%5, 1
    vextracti128    [r5 + r1 * 2 + %11], m%3, 1
    vextracti128    [r5 + r4 * 1 + %11], m%7, 1
    jmp             .end%11
.skip%11:
    movu            [r0 + r1 * 0], m%1
    movu            [r0 + r1 * 1], m%2
    movu            [r0 + r1 * 2], m%3
    movu            [r0 + r4 * 1], m%4

    lea             r0, [r0 + r1 * 4]
    movu            [r0 + r1 * 0], m%5
    movu            [r0 + r1 * 1], m%6
    movu            [r0 + r1 * 2], m%7
    movu            [r0 + r4 * 1], m%8
    lea             r0, [r0 + r1 * 4]
.end%11:
%endmacro

;; angle 16, modes 3 and 33
cglobal ang16_mode_3_33
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m1, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m1, m4                      ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    punpckhwd       m1, m4                          ; [25 24 24 23 23 22 22 21 17 16 16 15 15 14 14 13]

    pmaddwd         m4, m3, [r3 + 10 * 32]          ; [26]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 + 10 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m5, m0, m3, 4                   ; [14 13 13 12 12 11 11 10  6  5  5  4  4  3  3  2]
    pmaddwd         m5, [r3 + 4 * 32]               ; [20]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m6, m2, m0, 4                   ; [18 17 17 16 16 15 15 14 10  9  9  8  8  7  7  6]
    pmaddwd         m6, [r3 + 4 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    palignr         m6, m0, m3, 8                   ; [15 14 14 13 13 12 12 11  7  6  6  5  5  4  4  3]
    pmaddwd         m6, [r3 - 2 * 32]               ; [14]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m7, m2, m0, 8                   ; [19 18 18 17 17 16 16 15 11 10 10  9  9  8  8  7]
    pmaddwd         m7, [r3 - 2 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m7, m0, m3, 12                  ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]
    pmaddwd         m7, [r3 - 8 * 32]               ; [8]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m8, m2, m0, 12                  ; [20 19 19 18 18 17 17 16 12 11 11 10 10  9  9  8]
    pmaddwd         m8, [r3 - 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m0, [r3 - 14 * 32]          ; [2]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m3, m2, [r3 - 14 * 32]          ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m8, m3

    pmaddwd         m9, m0, [r3 + 12 * 32]          ; [28]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, m2, [r3 + 12 * 32]          ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    palignr         m10, m2, m0, 4                  ; [18 17 17 16 16 15 15 14 10  9  9  8  8  7  7  6]
    pmaddwd         m10, [r3 + 6 * 32]              ; [22]
    paddd           m10, [pd_16]
    psrld           m10, 5
    palignr         m3, m1, m2, 4                   ; [22 21 21 20 20 19 19 18 14 13 13 12 12 11 11 10]
    pmaddwd         m3, [r3 + 6 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m10, m3

    palignr         m11, m2, m0, 8                  ; [19 18 18 17 17 16 16 15 11 10 10  9  9  8  8  7]
    pmaddwd         m11, [r3]                       ; [16]
    paddd           m11, [pd_16]
    psrld           m11, 5
    palignr         m3, m1, m2, 8                   ; [23 22 22 21 21 20 20 19 15 14 14 13 13 12 12 11]
    pmaddwd         m3, [r3]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m11, m3

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 3, 0

    palignr         m4, m2, m0, 12                  ; [20 19 19 18 18 17 17 16 12 11 11 10 10  9  9  8]
    pmaddwd         m4, [r3 - 6 * 32]               ; [10]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m5, m1, m2, 12                  ; [24 23 23 22 22 21 21 20 15 16 15 14 14 13 13 12]
    pmaddwd         m5, [r3 - 6 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m2, [r3 - 12 * 32]          ; [4]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m1, [r3 - 12 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    movu            m0, [r2 + 34]                   ; [32 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17]
    pmaddwd         m6, m2, [r3 + 14 * 32]          ; [30]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, m1, [r3 + 14 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m3, m0, m0, 2                   ; [ x 32 31 30 29 28 27 26  x 24 23 22 21 20 19 18]
    punpcklwd       m0, m3                          ; [29 29 28 28 27 27 26 22 21 20 20 19 19 18 18 17]

    palignr         m7, m1, m2, 4
    pmaddwd         m7, [r3 + 8 * 32]               ; [24]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m8, m0, m1, 4
    pmaddwd         m8, [r3 + 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m8, m1, m2, 8
    pmaddwd         m8, [r3 + 2 * 32]               ; [18]
    paddd           m8, [pd_16]
    psrld           m8, 5
    palignr         m9, m0, m1, 8
    pmaddwd         m9, [r3 + 2 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m9, m1, m2, 12
    pmaddwd         m9, [r3 - 4 * 32]               ; [12]
    paddd           m9, [pd_16]
    psrld           m9, 5
    palignr         m3, m0, m1, 12
    pmaddwd         m3, [r3 - 4 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m1, [r3 - 10 * 32]              ; [6]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m0, [r3 - 10 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m1, m0

    movu            m2, [r2 + 28]
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 1, 2, 0, 3, 16
    ret

;; angle 16, modes 4 and 32
cglobal ang16_mode_4_32
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m1, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m1, m4                      ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    punpckhwd       m1, m4                          ; [25 24 24 23 23 22 22 21 17 16 16 15 15 14 14 13]

    pmaddwd         m4, m3, [r3 + 3 * 32]           ; [21]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 + 3 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m0, m3, 4                   ; [14 13 13 12 12 11 11 10  6  5  5  4  4  3  3  2]
    pmaddwd         m5, m6, [r3 - 8 * 32]           ; [10]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m7, m2, m0, 4                   ; [18 17 17 16 16 15 15 14 10  9  9  8  8  7  7  6]
    pmaddwd         m8, m7, [r3 - 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, [r3 + 13 * 32]              ; [31]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 + 13 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m7, m0, m3, 8                   ; [15 14 14 13 13 12 12 11  7  6  6  5  5  4  4  3]
    pmaddwd         m7, [r3 + 2 * 32]               ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m8, m2, m0, 8                   ; [19 18 18 17 17 16 16 15 11 10 10  9  9  8  8  7]
    pmaddwd         m8, [r3 + 2 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m9, m0, m3, 12
    pmaddwd         m8, m9, [r3 - 9 * 32]           ; [9]
    paddd           m8, [pd_16]
    psrld           m8, 5
    palignr         m3, m2, m0, 12
    pmaddwd         m10, m3, [r3 - 9 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m8, m10

    pmaddwd         m9, [r3 + 12 * 32]              ; [30]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, [r3 + 12 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, m0, [r3 + 1 * 32]          ; [19]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m3, m2, [r3 + 1 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m10, m3

    palignr         m11, m2, m0, 4
    pmaddwd         m11, [r3 - 10 * 32]             ; [8]
    paddd           m11, [pd_16]
    psrld           m11, 5
    palignr         m3, m1, m2, 4
    pmaddwd         m3, [r3 - 10 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m11, m3

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 3, 0

    palignr         m4, m2, m0, 4
    pmaddwd         m4, [r3 + 11 * 32]              ; [29]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m5, m1, m2, 4
    pmaddwd         m5, [r3 + 11  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m5, m2, m0, 8
    pmaddwd         m5, [r3]                        ; [18]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m6, m1, m2, 8
    pmaddwd         m6, [r3]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    palignr         m7, m2, m0, 12
    pmaddwd         m6, m7, [r3 - 11 * 32]          ; [7]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m8, m1, m2, 12
    pmaddwd         m3, m8, [r3 - 11 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m6, m3

    pmaddwd         m7, [r3 + 10 * 32]              ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, [r3 + 10 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    movu            m0, [r2 + 34]                   ; [32 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17]
    pmaddwd         m8, m2, [r3 - 1 * 32]           ; [17]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m1, [r3 - 1 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m3, m0, m0, 2                   ; [ x 32 31 30 29 28 27 26  x 24 23 22 21 20 19 18]
    punpcklwd       m0, m3                          ; [29 29 28 28 27 27 26 22 21 20 20 19 19 18 18 17]

    palignr         m10, m1, m2, 4
    pmaddwd         m9, m10, [r3 - 12 * 32]         ; [6]
    paddd           m9, [pd_16]
    psrld           m9, 5
    palignr         m11, m0, m1, 4
    pmaddwd         m3, m11, [r3 - 12 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, [r3 + 9 * 32]              ; [27]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 9 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m3, m1, m2, 8
    pmaddwd         m3, [r3 - 2 * 32]               ; [16]
    paddd           m3, [pd_16]
    psrld           m3, 5
    palignr         m0, m1, 8
    pmaddwd         m0, [r3 - 2 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 3, 0, 1, 16
    ret

;; angle 16, modes 5 and 31
cglobal ang16_mode_5_31
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m1, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m1, m4                      ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    punpckhwd       m1, m4                          ; [25 24 24 23 23 22 22 21 17 16 16 15 15 14 14 13]

    pmaddwd         m4, m3, [r3 + 1 * 32]           ; [17]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 + 1 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m0, m3, 4
    pmaddwd         m5, m6, [r3 - 14 * 32]          ; [2]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m7, m2, m0, 4
    pmaddwd         m8, m7, [r3 - 14 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, [r3 + 3 * 32]               ; [19]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 + 3 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m8, m0, m3, 8
    pmaddwd         m7, m8, [r3 - 12 * 32]          ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m9, m2, m0, 8
    pmaddwd         m10, m9, [r3 - 12 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    pmaddwd         m8, [r3 + 5 * 32]               ; [21]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 5 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m10, m0, m3, 12
    pmaddwd         m9, m10, [r3 - 10 * 32]         ; [6]
    paddd           m9, [pd_16]
    psrld           m9, 5
    palignr         m11, m2, m0, 12
    pmaddwd         m3, m11, [r3 - 10 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, [r3 + 7 * 32]              ; [23]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 7 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    pmaddwd         m11, m0, [r3 - 8 * 32]          ; [8]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m3, m2, [r3 - 8 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m11, m3

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 3, 0

    pmaddwd         m4, m0, [r3 + 9 * 32]           ; [25]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 + 9  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m2, m0, 4
    pmaddwd         m5, m6, [r3 - 6 * 32]           ; [10]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m7, m1, m2, 4
    pmaddwd         m3, m7, [r3 - 6 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m5, m3

    pmaddwd         m6, [r3 + 11 * 32]              ; [27]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 + 11 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m8, m2, m0, 8
    pmaddwd         m7, m8, [r3 - 4 * 32]           ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m9, m1, m2, 8
    pmaddwd         m3, m9, [r3 - 4 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m7, m3

    pmaddwd         m8, [r3 + 13 * 32]              ; [29]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 13 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m10, m2, m0, 12
    pmaddwd         m9, m10, [r3 - 2 * 32]          ; [14]
    paddd           m9, [pd_16]
    psrld           m9, 5
    palignr         m11, m1, m2, 12
    pmaddwd         m3, m11, [r3 - 2 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, [r3 + 15 * 32]             ; [31]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 15 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    pmaddwd         m2, [r3]                        ; [16]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m1, [r3]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m2, m1
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 2, 0, 1, 16
    ret

;; angle 16, modes 6 and 30
cglobal ang16_mode_6_30
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m1, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m1, m4                      ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    punpckhwd       m1, m4                          ; [25 24 24 23 23 22 22 21 17 16 16 15 15 14 14 13]

    pmaddwd         m4, m3, [r3 - 2 * 32]           ; [13]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 2 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 11 * 32]          ; [26]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 11 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m7, m0, m3, 4
    pmaddwd         m6, m7, [r3 - 8 * 32]           ; [7]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m8, m2, m0, 4
    pmaddwd         m9, m8, [r3 - 8 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, [r3 + 5 * 32]               ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, [r3 + 5 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m10, m0, m3, 8
    pmaddwd         m8, m10, [r3 - 14 * 32]         ; [1]
    paddd           m8, [pd_16]
    psrld           m8, 5
    palignr         m11, m2, m0, 8
    pmaddwd         m9, m11, [r3 - 14 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m10, [r3 - 1 * 32]          ; [14]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m12, m11, [r3 - 1 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m9, m12

    pmaddwd         m10, [r3 + 12 * 32]             ; [27]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 12 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m11, m0, m3, 12
    pmaddwd         m11, [r3 - 7 * 32]              ; [8]
    paddd           m11, [pd_16]
    psrld           m11, 5
    palignr         m12, m2, m0, 12
    pmaddwd         m12, [r3 - 7 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m4, m0, m3, 12
    pmaddwd         m4, [r3 + 6 * 32]               ; [21]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m5, m2, m0, 12
    pmaddwd         m5, [r3 + 6  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m0, [r3 - 13 * 32]          ; [2]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m3, m2, [r3 - 13 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m5, m3

    pmaddwd         m6, m0, [r3]                    ; [15]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, m2, [r3]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    pmaddwd         m7, m0, [r3 + 13 * 32]          ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m3, m2, [r3 + 13 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m7, m3

    palignr         m9, m2, m0, 4
    pmaddwd         m8, m9, [r3 - 6 * 32]           ; [9]
    paddd           m8, [pd_16]
    psrld           m8, 5
    palignr         m3, m1, m2, 4
    pmaddwd         m10, m3, [r3 - 6 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m8, m10

    pmaddwd         m9, [r3 + 7 * 32]               ; [22]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, [r3 + 7 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    palignr         m11, m2, m0, 8
    pmaddwd         m10, m11, [r3 - 12 * 32]        ; [3]
    paddd           m10, [pd_16]
    psrld           m10, 5
    palignr         m3, m1, m2, 8
    pmaddwd         m12, m3, [r3 - 12 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m11, [r3 + 1 * 32]              ; [16]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m3, [r3 + 1 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m11, m3
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 0, 1, 16
    ret

;; angle 16, modes 7 and 29
cglobal ang16_mode_7_29
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m2, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m4                          ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]

    pmaddwd         m4, m3, [r3 - 8 * 32]           ; [9]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 8 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 1 * 32]           ; [18]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 1 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m3, [r3 + 10 * 32]          ; [27]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m0, [r3 + 10 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    palignr         m10, m0, m3, 4
    pmaddwd         m7, m10, [r3 - 13 * 32]         ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m11, m2, m0, 4
    pmaddwd         m8, m11, [r3 - 13 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m10, [r3 - 4 * 32]          ; [13]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m11, [r3 - 4 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m10, [r3 + 5 * 32]          ; [22]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m12, m11, [r3 + 5 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m9, m12

    pmaddwd         m10, [r3 + 14 * 32]             ; [31]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 14 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m11, m0, m3, 8
    pmaddwd         m11, [r3 - 9 * 32]              ; [8]
    paddd           m11, [pd_16]
    psrld           m11, 5
    palignr         m12, m2, m0, 8
    pmaddwd         m12, [r3 - 9 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 0

    palignr         m5, m0, m3, 8
    pmaddwd         m4, m5, [r3]                    ; [17]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m6, m2, m0, 8
    pmaddwd         m7, m6, [r3]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m4, m7

    pmaddwd         m5, [r3 + 9 * 32]               ; [26]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, [r3 + 9 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    palignr         m9, m0, m3, 12
    pmaddwd         m6, m9, [r3 - 14 * 32]          ; [3]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m3, m2, m0, 12
    pmaddwd         m7, m3, [r3 - 14 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    pmaddwd         m7, m9, [r3 - 5 * 32]           ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m3, [r3 - 5 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m9, [r3 + 4 * 32]           ; [21]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m10, m3, [r3 + 4 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m8, m10

    pmaddwd         m9, [r3 + 13 * 32]              ; [30]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, [r3 + 13 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, m0, [r3 - 10 * 32]         ; [7]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m2, [r3 - 10 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m0, [r3 - 1 * 32]               ; [16]
    paddd           m0, [pd_16]
    psrld           m0, 5
    pmaddwd         m2, [r3 - 1 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m0, m2
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 0, 1, 2, 16
    ret

;; angle 16, modes 8 and 28
cglobal ang16_mode_8_28
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m2, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m4                          ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]

    pmaddwd         m4, m3, [r3 - 10 * 32]           ; [5]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 10 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 - 5 * 32]           ; [10]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 - 5 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m3, [r3]                    ; [15]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m0, [r3]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, m3, [r3 + 5 * 32]           ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m0, [r3 + 5 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m3, [r3 + 10 * 32]          ; [25]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 + 10 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 + 15 * 32]          ; [30]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m0, [r3 + 15 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    palignr         m11, m0, m3, 4
    pmaddwd         m10, m11, [r3 - 12 * 32]        ; [3]
    paddd           m10, [pd_16]
    psrld           m10, 5
    palignr         m1, m2, m0, 4
    pmaddwd         m12, m1, [r3 - 12 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m11, [r3 - 7 * 32]              ; [8]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m1, [r3 - 7 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m11, m1

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 0

    palignr         m7, m0, m3, 4
    pmaddwd         m4, m7, [r3 - 2 * 32]           ; [13]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m1, m2, m0, 4
    pmaddwd         m5, m1, [r3 - 2 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m7, [r3 + 3 * 32]           ; [18]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m1, [r3 + 3 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m7, [r3 + 8 * 32]           ; [23]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m1, [r3 + 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, [r3 + 13 * 32]              ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m1, [r3 + 13 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m7, m1

    palignr         m1, m0, m3, 8
    pmaddwd         m8, m1, [r3 - 14 * 32]          ; [1]
    paddd           m8, [pd_16]
    psrld           m8, 5
    palignr         m2, m0, 8
    pmaddwd         m9, m2, [r3 - 14 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m1, [r3 - 9 * 32]           ; [6]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, m2, [r3 - 9 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m3, m1, [r3 - 4 * 32]           ; [11]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, m2, [r3 - 4 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0

    pmaddwd         m1, [r3 + 1 * 32]               ; [16]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m2, [r3 + 1 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m1, m2
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 3, 1, 0, 2, 16
    ret

;; angle 16, modes 9 and 27
cglobal ang16_mode_9_27
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m2, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m4                          ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]

    pmaddwd         m4, m3, [r3 - 14 * 32]          ; [2]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 14 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 - 12 * 32]          ; [4]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 - 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m3, [r3 - 10 * 32]          ; [6]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m0, [r3 - 10 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, m3, [r3 - 8 * 32]           ; [8]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m0, [r3 - 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m3, [r3 - 6 * 32]           ; [10]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 - 6 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 - 4 * 32]           ; [12]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m0, [r3 - 4 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m3, [r3 - 2 * 32]          ; [14]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m1, m0, [r3 - 2 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m10, m1

    pmaddwd         m11, m3, [r3]                   ; [16]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m1, m0, [r3]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m11, m1

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 2, 1, 0

    pmaddwd         m4, m3, [r3 + 2 * 32]           ; [18]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 + 2 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 4 * 32]           ; [20]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m0, [r3 + 4 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m3, [r3 + 6 * 32]           ; [22]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m0, [r3 + 6 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m3, [r3 + 8 * 32]           ; [24]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m1, m0, [r3 + 8 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m7, m1

    pmaddwd         m8, m3, [r3 + 10 * 32]          ; [26]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 + 10 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 + 12 * 32]          ; [28]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m1, m0, [r3 + 12 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m9, m1

    pmaddwd         m3, [r3 + 14 * 32]              ; [30]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, [r3 + 14 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0

    movu            m1, [r2 + 4]
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 3, 1, 0, 2, 16
    ret

;; angle 16, modes 11 and 25
cglobal ang16_mode_11_25
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m1, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m1                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m0, m1                          ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 + 14 * 32]          ; [30]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 + 14 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 12 * 32]          ; [28]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m3, [r3 + 10 * 32]          ; [26]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m0, [r3 + 10 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, m3, [r3 + 8 * 32]           ; [24]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m0, [r3 + 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m3, [r3 + 6 * 32]           ; [22]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 + 6 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 + 4 * 32]           ; [20]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m0, [r3 + 4 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m3, [r3 + 2 * 32]          ; [18]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m1, m0, [r3 + 2 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m10, m1

    pmaddwd         m11, m3, [r3]                   ; [16]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m1, m0, [r3]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m11, m1

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 2, 1, 0

    pmaddwd         m4, m3, [r3 - 2 * 32]           ; [14]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 2 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 - 4 * 32]           ; [12]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m0, [r3 - 4 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m3, [r3 - 6 * 32]           ; [10]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m0, [r3 - 6 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m3, [r3 - 8 * 32]           ; [8]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m1, m0, [r3 - 8 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m7, m1

    pmaddwd         m8, m3, [r3 - 10 * 32]          ; [6]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 - 10 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 - 12 * 32]          ; [4]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m1, m0, [r3 - 12 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m9, m1

    pmaddwd         m3, [r3 - 14 * 32]              ; [2]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, [r3 - 14 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0

    movu            m1, [r2]
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 3, 1, 0, 2, 16
    ret

;; angle 16, modes 12 and 24
cglobal ang16_mode_12_24
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m2, m0, m4                      ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 + 11 * 32]          ; [27]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 + 11 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 6 * 32]           ; [22]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m2, [r3 + 6 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m3, [r3 + 1 * 32]           ; [17]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m2, [r3 + 1 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, m3, [r3 - 4 * 32]           ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m2, [r3 - 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m3, [r3 - 9 * 32]           ; [7]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m2, [r3 - 9 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 - 14 * 32]          ; [2]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m2, [r3 - 14 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m9, m2

    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]
    vinserti128     m1, m1, xm0, 1                  ; [ 7  7  6  6  5  5  4  4  6  6 13 13  x  x  x  x]

    palignr         m2, m3, m1, 14
    palignr         m13, m0, m3, 14

    pmaddwd         m10, m2, [r3 + 13 * 32]         ; [29]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 + 13 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m11, m2, [r3 + 8 * 32]          ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 + 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m13, m0, m3, 14

    pmaddwd         m4, m2, [r3 + 3 * 32]           ; [19]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 + 3 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m2, [r3 - 2 * 32]           ; [14]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m13, [r3 - 2 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m2, [r3 - 7 * 32]           ; [9]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m13, [r3 - 7 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m2, [r3 - 12 * 32]          ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 - 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m0, m3, 10
    palignr         m3, m1, 10

    pmaddwd         m8, m3, [r3 + 15 * 32]          ; [31]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 + 15 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 + 10 * 32]          ; [26]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m1, m0, [r3 + 10 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m9, m1

    pmaddwd         m1, m3, [r3 + 5 * 32]           ; [21]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m2, m0, [r3 + 5 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m1, m2

    pmaddwd         m3, [r3]                        ; [16]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, [r3]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 1, 3, 0, 2, 16
    ret

;; angle 16, modes 13 and 23
cglobal ang16_mode_13_23
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m2, m0, m4                      ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 + 7 * 32]           ; [23]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 + 7 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 - 2 * 32]           ; [14]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m2, [r3 - 2 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m3, [r3 - 11 * 32]          ; [5]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m2, [r3 - 11 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m6, m2

    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]
    vinserti128     m1, m1, xm0, 1                  ; [ 7  7  6  6  5  5  4  4  4  4  7  7 11 11 14 14]

    palignr         m2, m3, m1, 14
    palignr         m13, m0, m3, 14

    pmaddwd         m7, m2, [r3 + 12 * 32]          ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 + 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m2, [r3 + 3 * 32]           ; [19]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 + 3 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 - 6 * 32]           ; [10]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 - 6 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m2, [r3 - 15 * 32]         ; [1]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 - 15 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    palignr         m2, m3, m1, 10
    palignr         m13, m0, m3, 10

    pmaddwd         m11, m2, [r3 + 8 * 32]          ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 + 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m13, m0, m3, 10

    pmaddwd         m4, m2, [r3 - 1 * 32]           ; [15]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 - 1 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m2, [r3 - 10 * 32]          ; [6]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m13, [r3 - 10 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    palignr         m2, m3, m1, 6
    palignr         m13, m0, m3, 6

    pmaddwd         m6, m2, [r3 + 13 * 32]          ; [29]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m13, [r3 + 13 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m2, [r3 + 4 * 32]           ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 + 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m2, [r3 - 5 * 32]           ; [11]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 - 5 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 - 14 * 32]          ; [2]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m13, [r3 - 14 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m9, m13

    palignr         m0, m3, 2
    palignr         m3, m1, 2

    pmaddwd         m1, m3, [r3 + 9 * 32]           ; [25]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m2, m0, [r3 + 9 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m1, m2

    pmaddwd         m3, [r3]                        ; [16]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, [r3]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 1, 3, 0, 2, 16
    ret

;; angle 16, modes 14 and 22
cglobal ang16_mode_14_22
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m2, m0, m4                      ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 + 3 * 32]           ; [19]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 + 3 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 - 10 * 32]          ; [6]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m2, [r3 - 10 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m5, m2

    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]
    vinserti128     m1, m1, xm0, 1                  ; [ 7  7  6  6  5  5  4  4  2  2  5  5  7  7 10 10]
    vinserti128     m14, m14, xm3, 1                ; [ 3  3  2  2  1  1  0  0 12 12 15 15  x  x  x  x]

    palignr         m2, m3, m1, 14
    palignr         m13, m0, m3, 14

    pmaddwd         m6, m2, [r3 + 9 * 32]           ; [25]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m13, [r3 + 9 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, m2, [r3 - 4 * 32]           ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 - 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m2, m3, m1, 10                  ; [10  9  9  8  8  7  7  6  2  1  1  0  0  2  2  5]
    palignr         m13, m0, m3, 10                 ; [14 13 13 12 12 11 11 10  6  5  5  4  4  3  3  2]

    pmaddwd         m8, m2, [r3 + 15 * 32]          ; [31]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 + 15 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 + 2 * 32]           ; [18]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 + 2 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m2, [r3 - 11 * 32]         ; [5]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 - 11 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    palignr         m2, m3, m1, 6                   ; [ 9  8  8  7  7  6  6  5  1  0  0  2  2  5  5  7]
    palignr         m13, m0, m3, 6                  ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]

    pmaddwd         m11, m2, [r3 + 8 * 32]          ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 + 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m13, m0, m3, 6

    pmaddwd         m4, m2, [r3 - 5 * 32]           ; [11]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 - 5 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m2, m0, m3, 2                   ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    palignr         m13, m3, m1, 2                  ; [ 8  7  7  6  6  5  5  4  0  2  2  5  5  7  7 10]

    pmaddwd         m5, m13, [r3 + 14 * 32]         ; [30]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m2, [r3 + 14 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m13, [r3 + 1 * 32]          ; [17]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m2, [r3 + 1 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m13, [r3 - 12 * 32]         ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m2, [r3 - 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m2, m1, m14, 14                 ; [ 7  6  6  5  5  4  4  3  2  5  5  7  7 10 10 12]
    palignr         m0, m3, m1, 14                  ; [11 10 10  9  9  8  8  7  3  2  2  1  1  0  0  2]

    pmaddwd         m8, m2, [r3 + 7 * 32]           ; [23]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 + 7 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 - 6 * 32]           ; [10]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m2, m0, [r3 - 6 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m9, m2

    palignr         m3, m1, 10                      ; [10  9  9  8  8  7  7  6  2  1  1  0  0  2  2  5]
    palignr         m1, m14, 10                     ; [ 6  5  5  4  4  3  3  2  5  7  7 10 10 12 12 15]

    pmaddwd         m2, m1, [r3 + 13 * 32]          ; [29]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m0, m3, [r3 + 13 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m2, m0

    pmaddwd         m1, [r3]                        ; [16]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m3, [r3]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m1, m3
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 2, 1, 0, 3, 16
    ret

;; angle 16, modes 15 and 21
cglobal ang16_mode_15_21
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m2, m0, m4                      ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 - 1 * 32]           ; [15]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 - 1 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]
    vinserti128     m1, m1, xm0, 1
    vinserti128     m14, m14, xm3, 1

    palignr         m2, m3, m1, 14
    palignr         m13, m0, m3, 14

    pmaddwd         m5, m2, [r3 + 14 * 32]          ; [30]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m13, [r3 + 14 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m2, [r3 - 3 * 32]           ; [13]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m13, [r3 - 3 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    palignr         m2, m3, m1, 10
    palignr         m13, m0, m3, 10

    pmaddwd         m7, m2, [r3 + 12 * 32]          ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 + 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m2, [r3 - 5 * 32]           ; [11]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 - 5 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m2, m3, m1, 6
    palignr         m13, m0, m3, 6

    pmaddwd         m9, m2, [r3 + 10 * 32]          ; [26]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 + 10 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m2, [r3 - 7 * 32]          ; [9]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 - 7 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    palignr         m2, m3, m1, 2
    palignr         m13, m0, m3, 2

    pmaddwd         m11, m2, [r3 + 8 * 32]          ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 + 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m13, m0, m3, 2

    pmaddwd         m4, m2, [r3 - 9 * 32]           ; [7]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 - 9 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m1, m14, 14
    palignr         m7, m3, m1, 14

    pmaddwd         m5, m6, [r3 + 6 * 32]           ; [22]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m7, [r3 + 6 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, [r3 - 11 * 32]              ; [5]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 - 11 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m8, m1, m14, 10
    palignr         m9, m3, m1, 10

    pmaddwd         m7, m8, [r3 + 4 * 32]           ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m10, m9, [r3 + 4 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    pmaddwd         m8, [r3 - 13 * 32]              ; [3]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 - 13 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m2, m1, m14, 6
    palignr         m0, m3, m1, 6

    pmaddwd         m9, m2, [r3 + 2 * 32]           ; [18]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m13, m0, [r3 + 2 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m9, m13

    pmaddwd         m2, [r3 - 15 * 32]              ; [1]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m0, [r3 - 15 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m2, m0

    palignr         m3, m1, 2
    palignr         m1, m14, 2

    pmaddwd         m1, [r3]                        ; [16]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m3, [r3]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m1, m3
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 2, 1, 0, 3, 16
    ret

;; angle 16, modes 16 and 20
cglobal ang16_mode_16_20
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m12, m0, m4                     ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 - 5 * 32]           ; [11]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m12, [r3 - 5 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]
    vinserti128     m1, m1, xm0, 1                  ; [ 7  7  6  6  5  5  4  4  2  2  3  3  5  5  6  6]
    vinserti128     m14, m14, xm3, 1                ; [ 3  3  2  2  1  1  0  0  8  8  9  9 11 11 12 12]
    vinserti128     m2, m2, xm1, 1                  ; [ 2  2  3  3  5  5  6  6 14 14 15 15  x  x  x  x]

    palignr         m12, m3, m1, 14
    palignr         m13, m0, m3, 14

    pmaddwd         m5, m12, [r3 + 6 * 32]          ; [22]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m13, [r3 + 6 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m12, [r3 - 15 * 32]         ; [1]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m13, [r3 - 15 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    palignr         m12, m3, m1, 10
    palignr         m13, m0, m3, 10

    pmaddwd         m7, m12, [r3 - 4 * 32]          ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 - 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m12, m3, m1, 6
    palignr         m13, m0, m3, 6

    pmaddwd         m8, m12, [r3 + 7 * 32]          ; [23]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 + 7 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m12, [r3 - 14 * 32]         ; [2]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 - 14 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    palignr         m12, m3, m1, 2
    palignr         m13, m0, m3, 2

    pmaddwd         m10, m12, [r3 - 3 * 32]         ; [13]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, m13, [r3 - 3 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m12, m1, m14, 14
    palignr         m13, m3, m1, 14

    pmaddwd         m11, m12, [r3 + 8 * 32]         ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 + 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 0, 13, 0

    palignr         m13, m3, m1, 14

    pmaddwd         m4, m12, [r3 - 13 * 32]         ; [3]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 - 13 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m1, m14, 10
    palignr         m7, m3, m1, 10

    pmaddwd         m5, m6, [r3 - 2 * 32]           ; [14]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m7, [r3 - 2 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m7, m1, m14, 6
    palignr         m10, m3, m1, 6

    pmaddwd         m6, m7, [r3 + 9 * 32]           ; [25]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m10, [r3 + 9 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, [r3 - 12 * 32]              ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m10, [r3 - 12 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    palignr         m8, m1, m14, 2                  ; [ 4  3  3  2  2  1  1  0  6  8  8  9  9 11 11 12]
    palignr         m9, m3, m1, 2                   ; [ 8  7  7  6  6  5  5  4  0  2  2  3  3  5  5  6]

    pmaddwd         m8, [r3 - 1 * 32]               ; [15]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 - 1 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m12, m14, m2, 14
    palignr         m0, m1, m14, 14

    pmaddwd         m9, m12, [r3 + 10 * 32]         ; [26]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m13, m0, [r3 + 10 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m9, m13

    pmaddwd         m12, [r3 - 11 * 32]             ; [5]
    paddd           m12, [pd_16]
    psrld           m12, 5
    pmaddwd         m0, [r3 - 11 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m12, m0

    palignr         m1, m14, 10
    palignr         m14, m2, 10

    pmaddwd         m14, [r3]                       ; [16]
    paddd           m14, [pd_16]
    psrld           m14, 5
    pmaddwd         m1, [r3]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m14, m1
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 12, 14, 0, 3, 16
    ret

;; angle 16, modes 17 and 19
cglobal ang16_mode_17_19
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m12, m0, m4                     ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 - 10 * 32]           ; [6]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m12, [r3 - 10 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]
    vinserti128     m1, m1, xm0, 1                  ; [ 7  7  6  6  5  5  4  4  2  2  3  3  5  5  6  6]
    vinserti128     m14, m14, xm3, 1                ; [ 3  3  2  2  1  1  0  0  8  8  9  9 11 11 12 12]
    vinserti128     m2, m2, xm1, 1                  ; [ 2  2  3  3  5  5  6  6 14 14 15 15  x  x  x  x]

    palignr         m12, m3, m1, 14
    palignr         m13, m0, m3, 14

    pmaddwd         m5, m12, [r3 - 4 * 32]          ; [12]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m13, [r3 - 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m12, m3, m1, 10
    palignr         m13, m0, m3, 10

    pmaddwd         m6, m12, [r3 + 2 * 32]          ; [18]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m13, [r3 + 2 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    palignr         m12, m3, m1, 6
    palignr         m13, m0, m3, 6

    pmaddwd         m7, m12, [r3 + 8 * 32]          ; [24]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 + 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m12, m3, m1, 2
    palignr         m13, m0, m3, 2

    pmaddwd         m8, m12, [r3 + 14 * 32]         ; [30]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 + 14 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m12, [r3 - 12 * 32]         ; [4]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 - 12 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    palignr         m12, m1, m14, 14
    palignr         m13, m3, m1, 14

    pmaddwd         m10, m12, [r3 - 6 * 32]         ; [10]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, m13, [r3 - 6 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m12, m1, m14, 10
    palignr         m13, m3, m1, 10

    pmaddwd         m11, m12, [r3]                  ; [16]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 0, 13, 0

    palignr         m12, m1, m14, 6
    palignr         m13, m3, m1, 6

    pmaddwd         m4, m12, [r3 + 6 * 32]          ; [22]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 + 6 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m12, m1, m14, 2
    palignr         m13, m3, m1, 2

    pmaddwd         m5, m12, [r3 + 12 * 32]         ; [28]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m13, [r3 + 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m12, [r3 - 14 * 32]         ; [2]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m13, [r3 - 14 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    palignr         m7, m14, m2, 14
    palignr         m0, m1, m14, 14

    pmaddwd         m7, [r3 - 8 * 32]               ; [8]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m0, [r3 - 8 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m7, m0

    palignr         m8, m14, m2, 10
    palignr         m9, m1, m14, 10

    pmaddwd         m8, [r3 - 2 * 32]               ; [14]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 - 2 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m9, m14, m2, 6
    palignr         m13, m1, m14, 6

    pmaddwd         m9, [r3 + 4 * 32]               ; [20]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m13, [r3 + 4 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m9, m13

    palignr         m1, m14, 2
    palignr         m14, m2, 2

    pmaddwd         m12, m14, [r3 + 10 * 32]        ; [26]
    paddd           m12, [pd_16]
    psrld           m12, 5
    pmaddwd         m0, m1, [r3 + 10 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m12, m0

    pmaddwd         m14, [r3 - 16 * 32]             ; [0]
    paddd           m14, [pd_16]
    psrld           m14, 5
    pmaddwd         m1, [r3 - 16 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m14, m1
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 12, 14, 0, 3, 16
    ret

cglobal intra_pred_ang16_3, 3,7,13
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_3_33
    RET

cglobal intra_pred_ang16_33, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_3_33
    RET

cglobal intra_pred_ang16_4, 3,7,13
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 18 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_4_32
    RET

cglobal intra_pred_ang16_32, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 18 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_4_32
    RET

cglobal intra_pred_ang16_5, 3,7,13
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_5_31
    RET

cglobal intra_pred_ang16_31, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_5_31
    RET

cglobal intra_pred_ang16_6, 3,7,14
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 15 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_6_30
    RET

cglobal intra_pred_ang16_30, 3,7,14
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 15 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_6_30
    RET

cglobal intra_pred_ang16_7, 3,7,13
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 17 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_7_29
    RET

cglobal intra_pred_ang16_29, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 17 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_7_29
    RET

cglobal intra_pred_ang16_8, 3,7,13
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 15 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_8_28
    RET

cglobal intra_pred_ang16_28, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 15 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_8_28
    RET

cglobal intra_pred_ang16_9, 3,7,12
    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_9_27
    RET

cglobal intra_pred_ang16_27, 3,7,12
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_9_27
    RET

cglobal intra_pred_ang16_10, 3,6,3
    mov             r5d, r4m
    add             r1d, r1d
    lea             r4, [r1 * 3]

    vpbroadcastw    m2, [r2 + 2 + 64]       ; [1...]
    mova            m0, m2
    movu            [r0], m2
    vpbroadcastw    m1, [r2 + 2 + 64 + 2]   ; [2...]
    movu            [r0 + r1], m1
    vpbroadcastw    m2, [r2 + 2 + 64 + 4]   ; [3...]
    movu            [r0 + r1 * 2], m2
    vpbroadcastw    m1, [r2 + 2 + 64 + 6]   ; [4...]
    movu            [r0 + r4], m1

    lea             r3, [r0 + r1 * 4]
    vpbroadcastw    m2, [r2 + 2 + 64 + 8]   ; [5...]
    movu            [r3], m2
    vpbroadcastw    m1, [r2 + 2 + 64 + 10]  ; [6...]
    movu            [r3 + r1], m1
    vpbroadcastw    m2, [r2 + 2 + 64 + 12]  ; [7...]
    movu            [r3 + r1 * 2], m2
    vpbroadcastw    m1, [r2 + 2 + 64 + 14]  ; [8...]
    movu            [r3 + r4], m1

    lea             r3, [r3 + r1 *4]
    vpbroadcastw    m2, [r2 + 2 + 64 + 16]  ; [9...]
    movu            [r3], m2
    vpbroadcastw    m1, [r2 + 2 + 64 + 18]  ; [10...]
    movu            [r3 + r1], m1
    vpbroadcastw    m2, [r2 + 2 + 64 + 20]  ; [11...]
    movu            [r3 + r1 * 2], m2
    vpbroadcastw    m1, [r2 + 2 + 64 + 22]  ; [12...]
    movu            [r3 + r4], m1

    lea             r3, [r3 + r1 *4]
    vpbroadcastw    m2, [r2 + 2 + 64 + 24]  ; [13...]
    movu            [r3], m2
    vpbroadcastw    m1, [r2 + 2 + 64 + 26]  ; [14...]
    movu            [r3 + r1], m1
    vpbroadcastw    m2, [r2 + 2 + 64 + 28]  ; [15...]
    movu            [r3 + r1 * 2], m2
    vpbroadcastw    m1, [r2 + 2 + 64 + 30]  ; [16...]
    movu            [r3 + r4], m1

    cmp             r5d, byte 0
    jz              .quit

    ; filter
    vpbroadcastw    m2, [r2]                ; [0 0...]
    movu            m1, [r2 + 2]            ; [16 15 14 13 12 11 10 9 8 7 6 5 4 3 2 1]
    psubw           m1, m2
    psraw           m1, 1
    paddw           m0, m1
    pxor            m1, m1
    pmaxsw          m0, m1
    pminsw          m0, [pw_pixel_max]
.quit:
    movu            [r0], m0
    RET

cglobal intra_pred_ang16_26, 3,6,4
    mov         r5d,                r4m
    movu        m0,                 [r2 + 2]            ; [16 15 14 13 12 11 10 9 8 7 6 5 4 3 2 1]
    add         r1d,                r1d
    lea         r4,                 [r1 * 3]

    movu        [r0],               m0
    movu        [r0 + r1],          m0
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r4],          m0

    lea         r3,                 [r0 + r1 *4]
    movu        [r3],               m0
    movu        [r3 + r1],          m0
    movu        [r3 + r1 * 2],      m0
    movu        [r3 + r4],          m0

    lea         r3,                 [r3 + r1 *4]
    movu        [r3],               m0
    movu        [r3 + r1],          m0
    movu        [r3 + r1 * 2],      m0
    movu        [r3 + r4],          m0

    lea         r3,                 [r3 + r1 *4]
    movu        [r3],               m0
    movu        [r3 + r1],          m0
    movu        [r3 + r1 * 2],      m0
    movu        [r3 + r4],          m0

    cmp         r5d,                byte 0
    jz         .quit

    ; filter

    vpbroadcastw m0,                xm0
    vpbroadcastw m2,                [r2]
    movu        m1,                 [r2 + 2 + 64]
    psubw       m1,                 m2
    psraw       m1,                 1
    paddw       m0,                 m1
    pxor        m1,                 m1
    pmaxsw      m0,                 m1
    pminsw      m0,                 [pw_pixel_max]
    pextrw      [r0],               xm0, 0
    pextrw      [r0 + r1],          xm0, 1
    pextrw      [r0 + r1 * 2],      xm0, 2
    pextrw      [r0 + r4],          xm0, 3
    lea         r0,                 [r0 + r1 * 4]
    pextrw      [r0],               xm0, 4
    pextrw      [r0 + r1],          xm0, 5
    pextrw      [r0 + r1 * 2],      xm0, 6
    pextrw      [r0 + r4],          xm0, 7
    lea         r0,                 [r0 + r1 * 4]
    vpermq      m0,                 m0, 11101110b
    pextrw      [r0],               xm0, 0
    pextrw      [r0 + r1],          xm0, 1
    pextrw      [r0 + r1 * 2],      xm0, 2
    pextrw      [r0 + r4],          xm0, 3
    pextrw      [r3],               xm0, 4
    pextrw      [r3 + r1],          xm0, 5
    pextrw      [r3 + r1 * 2],      xm0, 6
    pextrw      [r3 + r4],          xm0, 7
.quit:
    RET

cglobal intra_pred_ang16_11, 3,7,12, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r2,        64
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_11_25

    mov         r6d,       [rsp]
    mov         [r2],      r6w
    RET

cglobal intra_pred_ang16_25, 3,7,12
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]

    call        ang16_mode_11_25
    RET

cglobal intra_pred_ang16_12, 3,7,14, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 12]            ; [13 12 11 10  9  8  7  6]
    pshufb      xm1,       [pw_ang16_12_24]     ; [ 6  6 13 13  x  x  x  x]
    xor         r6d,       r6d
    add         r2,        64

    call        ang16_mode_12_24

    mov         r6d,       [rsp]
    mov         [r2], r6w
    RET

cglobal intra_pred_ang16_24, 3,7,14, 0-4
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 76]            ; [13 12 11 10  9  8  7  6]
    pshufb      xm1,       [pw_ang16_12_24]     ; [ 6  6 13 13  x  x  x  x]
    xor         r6d,       r6d
    inc         r6d

    call        ang16_mode_12_24
    RET

cglobal intra_pred_ang16_13, 3,7,14, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 8]             ; [11  x  x  x  7  x  x  4]
    pinsrw      xm1,       [r2 + 28], 1         ; [11  x  x  x  7  x 14  4]
    pshufb      xm1,       [pw_ang16_13_23]     ; [ 4  4  7  7 11 11 14 14]
    xor         r6d,       r6d
    add         r2,        64

    call        ang16_mode_13_23

    mov         r6d,       [rsp]
    mov         [r2], r6w
    RET

cglobal intra_pred_ang16_23, 3,7,14, 0-4
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 72]            ; [11 10  9  8  7  6  5  4]
    pinsrw      xm1,       [r2 + 92], 1         ; [11  x  x  x  7  x 14  4]
    pshufb      xm1,       [pw_ang16_13_23]     ; [ 4  4  7  7 11 11 14 14]
    xor         r6d,       r6d
    inc         r6d

    call        ang16_mode_13_23
    RET

cglobal intra_pred_ang16_14, 3,7,15, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 4]             ; [ x  x  7  x  5  x  x  2]
    pinsrw      xm1,       [r2 + 20], 1         ; [ x  x  7  x  5  x 10  2]
    movu        xm14,      [r2 + 24]            ; [ x  x  x  x 15  x  x 12]
    pshufb      xm14,      [pw_ang16_14_22]     ; [12 12 15 15  x  x  x  x]
    pshufb      xm1,       [pw_ang16_14_22]     ; [ 2  2  5  5  7  7 10 10]
    xor         r6d,       r6d
    add         r2,        64

    call        ang16_mode_14_22

    mov         r6d,       [rsp]
    mov         [r2], r6w
    RET

cglobal intra_pred_ang16_22, 3,7,15, 0-4
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 68]            ; [ x  x  7  x  5  x  x  2]
    pinsrw      xm1,       [r2 + 84], 1         ; [ x  x  7  x  5  x 10  2]
    movu        xm14,      [r2 + 88]            ; [ x  x  x  x 15  x  x 12]
    pshufb      xm14,      [pw_ang16_14_22]     ; [12 12 15 15  x  x  x  x]
    pshufb      xm1,       [pw_ang16_14_22]     ; [ 2  2  5  5  7  7 10 10]
    xor         r6d,       r6d
    inc         r6d

    call        ang16_mode_14_22
    RET

cglobal intra_pred_ang16_15, 3,7,15, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 4]             ; [ x  8  x  6  x  4  x  2]
    movu        xm14,      [r2 + 18]            ; [ x 15  x 13  x 11  x  9]
    pshufb      xm14,      [pw_ang16_15_21]     ; [ 9  9 11 11 13 13 15 15]
    pshufb      xm1,       [pw_ang16_15_21]     ; [ 2  2  4  4  6  6  8  8]
    xor         r6d,       r6d
    add         r2,        64

    call        ang16_mode_15_21

    mov         r6d,       [rsp]
    mov         [r2], r6w
    RET

cglobal intra_pred_ang16_21, 3,7,15, 0-4
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 68]            ; [ x  8  x  6  x  4  x  2]
    movu        xm14,      [r2 + 82]            ; [ x 15  x 13  x 11  x  9]
    pshufb      xm14,      [pw_ang16_15_21]     ; [ 9  9 11 11 13 13 15 15]
    pshufb      xm1,       [pw_ang16_15_21]     ; [ 2  2  4  4  6  6  8  8]
    xor         r6d,       r6d
    inc         r6d

    call        ang16_mode_15_21
    RET

cglobal intra_pred_ang16_16, 3,7,15, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 4]             ; [ x  x  x  6  5  x  3  2]
    movu        xm14,      [r2 + 16]            ; [ x  x  x 12 11  x  9  8]
    movu        xm2,       [r2 + 28]            ; [ x  x  x  x  x  x 15 14]
    pshufb      xm14,      [pw_ang16_16_20]     ; [ 8  8  9  9 11 11 12 12]
    pshufb      xm1,       [pw_ang16_16_20]     ; [ 2  2  3  3  5  5  6  6]
    pshufb      xm2,       [pw_ang16_16_20]     ; [14 14 15 15  x  x  x  x]
    xor         r6d,       r6d
    add         r2,        64

    call        ang16_mode_16_20

    mov         r6d,       [rsp]
    mov         [r2], r6w
    RET

cglobal intra_pred_ang16_20, 3,7,15, 0-4
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 68]            ; [ x  x  x  6  5  x  3  2]
    movu        xm14,      [r2 + 80]            ; [ x  x  x 12 11  x  9  8]
    movu        xm2,       [r2 + 92]            ; [ x  x  x  x  x  x 15 14]
    pshufb      xm14,      [pw_ang16_16_20]     ; [ 8  8  9  9 11 11 12 12]
    pshufb      xm1,       [pw_ang16_16_20]     ; [ 2  2  3  3  5  5  6  6]
    pshufb      xm2,       [pw_ang16_16_20]     ; [14 14 15 15  x  x  x  x]
    xor         r6d,       r6d
    inc         r6d

    call        ang16_mode_16_20
    RET

cglobal intra_pred_ang16_17, 3,7,15, 0-4
    movzx       r5d,       word [r2 + 64]
    movzx       r6d,       word [r2]
    mov         [rsp],     r5w
    mov         [r2 + 64], r6w

    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 2]             ; [ x  x  x  6  5  x  3  2]
    movu        xm14,      [r2 + 12]            ; [ x  x  x 12 11  x  9  8]
    movu        xm2,       [r2 + 22]            ; [ x  x  x  x  x  x 15 14]
    pshufb      xm14,      [pw_ang16_16_20]     ; [ 8  8  9  9 11 11 12 12]
    pshufb      xm1,       [pw_ang16_16_20]     ; [ 2  2  3  3  5  5  6  6]
    pshufb      xm2,       [pw_ang16_16_20]     ; [14 14 15 15  x  x  x  x]
    xor         r6d,       r6d
    add         r2,        64

    call        ang16_mode_17_19

    mov         r6d,       [rsp]
    mov         [r2], r6w
    RET

cglobal intra_pred_ang16_19, 3,7,15, 0-4
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r3,        [ang_table_avx2 + 16 * 32]
    movu        xm1,       [r2 + 66]            ; [ x  x  x  6  5  x  3  2]
    movu        xm14,      [r2 + 76]            ; [ x  x  x 12 11  x  9  8]
    movu        xm2,       [r2 + 86]            ; [ x  x  x  x  x  x 15 14]
    pshufb      xm14,      [pw_ang16_16_20]     ; [ 8  8  9  9 11 11 12 12]
    pshufb      xm1,       [pw_ang16_16_20]     ; [ 2  2  3  3  5  5  6  6]
    pshufb      xm2,       [pw_ang16_16_20]     ; [14 14 15 15  x  x  x  x]
    xor         r6d,       r6d
    inc         r6d

    call        ang16_mode_17_19
    RET

cglobal intra_pred_ang16_18, 3,5,4
    add         r1d,                 r1d
    lea         r4,                  [r1 * 3]
    movu        m1,                  [r2]
    movu        m0,                  [r2 + 2 + 64]
    pshufb      m0,                  [pw_swap16]
    mova        m3,                  m0
    vinserti128 m0,                  m0, xm1, 1
    movu        [r0],                m1
    palignr     m2,                  m1, m0, 14
    movu        [r0 + r1],           m2

    palignr     m2,                  m1, m0, 12
    movu        [r0 + r1 * 2],       m2
    palignr     m2,                  m1, m0, 10
    movu        [r0 + r4],           m2

    lea         r0,                  [r0 + r1 * 4]
    palignr     m2,                  m1, m0, 8
    movu        [r0],                m2
    palignr     m2,                  m1, m0, 6
    movu        [r0 + r1],           m2
    palignr     m2,                  m1, m0, 4
    movu        [r0 + r1 * 2],       m2
    palignr     m2,                  m1, m0, 2
    movu        [r0 + r4],           m2

    lea         r0,                  [r0 + r1 * 4]
    movu        [r0],                m0
    vpermq      m3,                  m3, 01001110b
    palignr     m2,                  m0, m3, 14
    movu        [r0 + r1],           m2
    palignr     m2,                  m0, m3, 12
    movu        [r0 + r1 * 2],       m2
    palignr     m2,                  m0, m3, 10
    movu        [r0 + r4],           m2
    palignr     m2,                  m1, m0, 10

    lea         r0,                  [r0 + r1 * 4]
    palignr     m2,                  m0, m3, 8
    movu        [r0],                m2
    palignr     m2,                  m0, m3, 6
    movu        [r0 + r1],           m2
    palignr     m2,                  m0, m3, 4
    movu        [r0 + r1 * 2],       m2
    palignr     m2,                  m0, m3, 2
    movu        [r0 + r4],           m2
    palignr     m1,                  m0, 2
    RET

;-------------------------------------------------------------------------------------------------------
; end of avx2 code for intra_pred_ang16 mode 2 to 34
;-------------------------------------------------------------------------------------------------------

;-------------------------------------------------------------------------------------------------------
; avx2 code for intra_pred_ang32 mode 2 to 34 start
;-------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal intra_pred_ang32_2, 3,5,6
    lea         r4,                 [r2]
    add         r2,                 128
    cmp         r3m,                byte 34
    cmove       r2,                 r4
    add         r1d,                 r1d
    lea         r3,                 [r1 * 3]
    movu        m0,                 [r2 + 4]
    movu        m1,                 [r2 + 20]
    movu        m3,                 [r2 + 36]
    movu        m4,                 [r2 + 52]

    movu        [r0],               m0
    movu        [r0 + 32],          m3
    palignr     m2,                 m1, m0, 2
    palignr     m5,                 m4, m3, 2
    movu        [r0 + r1],          m2
    movu        [r0 + r1 + 32],     m5
    palignr     m2,                 m1, m0, 4
    palignr     m5,                 m4, m3, 4
    movu        [r0 + r1 * 2],      m2
    movu        [r0 + r1 * 2 + 32], m5
    palignr     m2,                 m1, m0, 6
    palignr     m5,                 m4, m3, 6
    movu        [r0 + r3],          m2
    movu        [r0 + r3 + 32],     m5

    lea         r0,                 [r0 + r1 * 4]
    palignr     m2,                 m1, m0, 8
    palignr     m5,                 m4, m3, 8
    movu        [r0],               m2
    movu        [r0 + 32],          m5
    palignr     m2,                 m1, m0, 10
    palignr     m5,                 m4, m3, 10
    movu        [r0 + r1],          m2
    movu        [r0 + r1 + 32],     m5
    palignr     m2,                 m1, m0, 12
    palignr     m5,                 m4, m3, 12
    movu        [r0 + r1 * 2],      m2
    movu        [r0 + r1 * 2 + 32], m5
    palignr     m2,                 m1, m0, 14
    palignr     m5,                 m4, m3, 14
    movu        [r0 + r3],          m2
    movu        [r0 + r3 + 32],     m5

    movu        m0,                 [r2 + 36]
    movu        m3,                 [r2 + 68]
    lea         r0,                 [r0 + r1 * 4]
    movu        [r0],               m1
    movu        [r0 + 32],          m4
    palignr     m2,                 m0, m1, 2
    palignr     m5,                 m3, m4, 2
    movu        [r0 + r1],          m2
    movu        [r0 + r1 + 32],     m5
    palignr     m2,                 m0, m1, 4
    palignr     m5,                 m3, m4, 4
    movu        [r0 + r1 * 2],      m2
    movu        [r0 + r1 * 2 + 32], m5
    palignr     m2,                 m0, m1, 6
    palignr     m5,                 m3, m4, 6
    movu        [r0 + r3],          m2
    movu        [r0 + r3 + 32],     m5

    lea         r0,                 [r0 + r1 * 4]
    palignr     m2,                 m0, m1, 8
    palignr     m5,                 m3, m4, 8
    movu        [r0],               m2
    movu        [r0 + 32],          m5
    palignr     m2,                 m0, m1, 10
    palignr     m5,                 m3, m4, 10
    movu        [r0 + r1],          m2
    movu        [r0 + r1 + 32],     m5
    palignr     m2,                 m0, m1, 12
    palignr     m5,                 m3, m4, 12
    movu        [r0 + r1 * 2],      m2
    movu        [r0 + r1 * 2 + 32], m5
    palignr     m2,                 m0, m1, 14
    palignr     m5,                 m3, m4, 14
    movu        [r0 + r3],          m2
    movu        [r0 + r3 + 32],     m5

    lea         r0,                 [r0 + r1 * 4]
    movu        m1,                 [r2 + 52]
    movu        m4,                 [r2 + 84]

    movu        [r0],               m0
    movu        [r0 + 32],          m3
    palignr     m2,                 m1, m0, 2
    palignr     m5,                 m4, m3, 2
    movu        [r0 + r1],          m2
    movu        [r0 + r1 + 32],     m5
    palignr     m2,                 m1, m0, 4
    palignr     m5,                 m4, m3, 4
    movu        [r0 + r1 * 2],      m2
    movu        [r0 + r1 * 2 + 32], m5
    palignr     m2,                 m1, m0, 6
    palignr     m5,                 m4, m3, 6
    movu        [r0 + r3],          m2
    movu        [r0 + r3 + 32],     m5

    lea         r0,                 [r0 + r1 * 4]
    palignr     m2,                 m1, m0, 8
    palignr     m5,                 m4, m3, 8
    movu        [r0],               m2
    movu        [r0 + 32],          m5
    palignr     m2,                 m1, m0, 10
    palignr     m5,                 m4, m3, 10
    movu        [r0 + r1],          m2
    movu        [r0 + r1 + 32],     m5
    palignr     m2,                 m1, m0, 12
    palignr     m5,                 m4, m3, 12
    movu        [r0 + r1 * 2],      m2
    movu        [r0 + r1 * 2 + 32], m5
    palignr     m2,                 m1, m0, 14
    palignr     m5,                 m4, m3, 14
    movu        [r0 + r3],          m2
    movu        [r0 + r3 + 32],     m5

    movu        m0,                 [r2 + 68]
    movu        m3,                 [r2 + 100]
    lea         r0,                 [r0 + r1 * 4]
    movu        [r0],               m1
    movu        [r0 + 32],          m4
    palignr     m2,                 m0, m1, 2
    palignr     m5,                 m3, m4, 2
    movu        [r0 + r1],          m2
    movu        [r0 + r1 + 32],     m5
    palignr     m2,                 m0, m1, 4
    palignr     m5,                 m3, m4, 4
    movu        [r0 + r1 * 2],      m2
    movu        [r0 + r1 * 2 + 32], m5
    palignr     m2,                 m0, m1, 6
    palignr     m5,                 m3, m4, 6
    movu        [r0 + r3],          m2
    movu        [r0 + r3 + 32],     m5

    lea         r0,                 [r0 + r1 * 4]
    palignr     m2,                 m0, m1, 8
    palignr     m5,                 m3, m4, 8
    movu        [r0],               m2
    movu        [r0 + 32],          m5
    palignr     m2,                 m0, m1, 10
    palignr     m5,                 m3, m4, 10
    movu        [r0 + r1],          m2
    movu        [r0 + r1 + 32],     m5
    palignr     m2,                 m0, m1, 12
    palignr     m5,                 m3, m4, 12
    movu        [r0 + r1 * 2],      m2
    movu        [r0 + r1 * 2 + 32], m5
    palignr     m2,                 m0, m1, 14
    palignr     m5,                 m3, m4, 14
    movu        [r0 + r3],          m2
    movu        [r0 + r3 + 32],     m5
    RET

cglobal intra_pred_ang32_3, 3,8,13
    add         r2,        128
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r7,        [r0 + 8 * r1]

    call        ang16_mode_3_33

    add         r2,        26
    lea         r0,        [r0 + 32]

    call        ang16_mode_3_33

    add         r2,        6
    lea         r0,        [r7 + 8 * r1]

    call        ang16_mode_3_33

    add         r2,        26
    lea         r0,        [r0 + 32]

    call        ang16_mode_3_33
    RET

cglobal intra_pred_ang32_33, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r5,        [r0 + 32]

    call        ang16_mode_3_33

    add         r2,        26

    call        ang16_mode_3_33

    add         r2,        6
    mov         r0,        r5

    call        ang16_mode_3_33

    add         r2,        26

    call        ang16_mode_3_33
    RET

;; angle 32, modes 4 and 32
cglobal ang32_mode_4_32
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m1, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m1, m4                      ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    punpckhwd       m1, m4                          ; [25 24 24 23 23 22 22 21 17 16 16 15 15 14 14 13]

    pmaddwd         m4, m3, [r3 - 13 * 32]          ; [5]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 13 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 8 * 32]           ; [26]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m6, m0, m3, 4                   ; [14 13 13 12 12 11 11 10  6  5  5  4  4  3  3  2]
    pmaddwd         m6, [r3 - 3 * 32]               ; [15]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m7, m2, m0, 4                   ; [18 17 17 16 16 15 15 14 10  9  9  8  8  7  7  6]
    pmaddwd         m7, [r3 - 3 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m8, m0, m3, 8                   ; [15 14 14 13 13 12 12 11  7  6  6  5  5  4  4  3]
    pmaddwd         m7, m8, [r3 - 14 * 32]              ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m9, m2, m0, 8                   ; [19 18 18 17 17 16 16 15 11 10 10  9  9  8  8  7]
    pmaddwd         m10, m9, [r3 - 14 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    pmaddwd         m8, [r3 + 7 * 32]               ; [25]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 7 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m9, m0, m3, 12
    pmaddwd         m9, [r3 - 4 * 32]               ; [14]
    paddd           m9, [pd_16]
    psrld           m9, 5
    palignr         m3, m2, m0, 12
    pmaddwd         m3, [r3 - 4 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, m0, [r3 - 15 * 32]         ; [3]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m3, m2, [r3 - 15 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m10, m3

    pmaddwd         m11, m0, [r3 + 6 * 32]          ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m3, m2, [r3 + 6 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m11, m3

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 3, 0

    palignr         m4, m2, m0, 4
    pmaddwd         m4, [r3 - 5* 32]                ; [13]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m5, m1, m2, 4
    pmaddwd         m5, [r3 - 5  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m2, m0, 8
    pmaddwd         m5, m6, [r3 - 16 * 32]          ; [2]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m7, m1, m2, 8
    pmaddwd         m8, m7, [r3 - 16 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, [r3 + 5 * 32]               ; [23]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 + 5 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m7, m2, m0, 12
    pmaddwd         m7, [r3 - 6 * 32]               ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m8, m1, m2, 12
    pmaddwd         m8, [r3 - 6 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    movu            m0, [r2 + 34]                   ; [32 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17]
    pmaddwd         m8, m2, [r3 - 17 * 32]          ; [1]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m1, [r3 - 17 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m3, m0, m0, 2                   ; [ x 32 31 30 29 28 27 26  x 24 23 22 21 20 19 18]
    punpcklwd       m0, m3                          ; [29 29 28 28 27 27 26 22 21 20 20 19 19 18 18 17]

    pmaddwd         m9, m2, [r3 + 4 * 32]           ; [22]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, m1, [r3 + 4 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    palignr         m10, m1, m2, 4
    pmaddwd         m10, [r3 - 7 * 32]              ; [11]
    paddd           m10, [pd_16]
    psrld           m10, 5
    palignr         m11, m0, m1, 4
    pmaddwd         m11, [r3 - 7 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m3, m1, m2, 8
    pmaddwd         m3, [r3 - 18 * 32]              ; [0]
    paddd           m3, [pd_16]
    psrld           m3, 5
    palignr         m0, m1, 8
    pmaddwd         m0, [r3 - 18 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 3, 0, 1, 16
    ret

cglobal intra_pred_ang32_4, 3,8,13
    add         r2,        128
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 18 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r7,        [r0 + 8 * r1]

    call        ang16_mode_4_32

    add         r2,        22
    lea         r0,        [r0 + 32]

    call        ang32_mode_4_32

    add         r2,        10
    lea         r0,        [r7 + 8 * r1]

    call        ang16_mode_4_32

    add         r2,        22
    lea         r0,        [r0 + 32]

    call        ang32_mode_4_32
    RET

cglobal intra_pred_ang32_32, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 18 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r5,        [r0 + 32]

    call        ang16_mode_4_32

    add         r2,        22

    call        ang32_mode_4_32

    add         r2,        10
    mov         r0,        r5

    call        ang16_mode_4_32

    add         r2,        22

    call        ang32_mode_4_32
    RET

;; angle 32, modes 5 and 31
cglobal ang32_mode_5_31
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m1, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m1, m4                      ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    punpckhwd       m1, m4                          ; [25 24 24 23 23 22 22 21 17 16 16 15 15 14 14 13]

    pmaddwd         m4, m3, [r3 - 15 * 32]          ; [1]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 15 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 2 * 32]           ; [18]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 2 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m7, m0, m3, 4
    pmaddwd         m6, m7, [r3 - 13 * 32]          ; [3]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m8, m2, m0, 4
    pmaddwd         m9, m8, [r3 - 13 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, [r3 + 4 * 32]               ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, [r3 + 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m9, m0, m3, 8
    pmaddwd         m8, m9, [r3 - 11 * 32]          ; [5]
    paddd           m8, [pd_16]
    psrld           m8, 5
    palignr         m10, m2, m0, 8
    pmaddwd         m11, m10, [r3 - 11 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m8, m11

    pmaddwd         m9, [r3 + 6 * 32]               ; [22]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, [r3 + 6 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    palignr         m11, m0, m3, 12
    pmaddwd         m10, m11, [r3 - 9 * 32]         ; [7]
    paddd           m10, [pd_16]
    psrld           m10, 5
    palignr         m12, m2, m0, 12
    pmaddwd         m3, m12, [r3 - 9 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m10, m3

    pmaddwd         m11, [r3 + 8 * 32]              ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, [r3 + 8 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 3, 0

    pmaddwd         m4, m0, [r3 - 7 * 32]           ; [9]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 - 7  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m0, [r3 + 10 * 32]          ; [26]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m3, m2, [r3 + 10 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m5, m3

    palignr         m7, m2, m0, 4
    pmaddwd         m6, m7, [r3 - 5 * 32]           ; [11]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m8, m1, m2, 4
    pmaddwd         m9, m8, [r3 - 5 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, [r3 + 12 * 32]              ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, [r3 + 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m9, m2, m0, 8
    pmaddwd         m8, m9, [r3 - 3 * 32]           ; [13]
    paddd           m8, [pd_16]
    psrld           m8, 5
    palignr         m3, m1, m2, 8
    pmaddwd         m10, m3, [r3 - 3 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m8, m10

    pmaddwd         m9, [r3 + 14 * 32]              ; [30]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, [r3 + 14 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    palignr         m10, m2, m0, 12
    pmaddwd         m10, [r3 - 1 * 32]              ; [15]
    paddd           m10, [pd_16]
    psrld           m10, 5
    palignr         m11, m1, m2, 12
    pmaddwd         m11, [r3 - 1 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    pmaddwd         m2, [r3 - 16 * 32]              ; [0]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m1, [r3 - 16 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m2, m1
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 2, 0, 1, 16
    ret

cglobal intra_pred_ang32_5, 3,8,13
    add         r2,        128
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r7,        [r0 + 8 * r1]

    call        ang16_mode_5_31

    add         r2,        18
    lea         r0,        [r0 + 32]

    call        ang32_mode_5_31

    add         r2,        14
    lea         r0,        [r7 + 8 * r1]

    call        ang16_mode_5_31

    add         r2,        18
    lea         r0,        [r0 + 32]

    call        ang32_mode_5_31
    RET

cglobal intra_pred_ang32_31, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r5,        [r0 + 32]

    call        ang16_mode_5_31

    add         r2,        18

    call        ang32_mode_5_31

    add         r2,        14
    mov         r0,        r5

    call        ang16_mode_5_31

    add         r2,        18

    call        ang32_mode_5_31
    RET

;; angle 32, modes 6 and 30
cglobal ang32_mode_6_30
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m1, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m1, m4                      ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    punpckhwd       m1, m4                          ; [25 24 24 23 23 22 22 21 17 16 16 15 15 14 14 13]

    pmaddwd         m4, m3, [r3 + 14 * 32]          ; [29]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 + 14 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m0, m3, 4
    pmaddwd         m5, m6, [r3 - 5 * 32]           ; [10]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m7, m2, m0, 4
    pmaddwd         m8, m7, [r3 - 5 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, [r3 + 8 * 32]               ; [23]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 + 8 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m9, m0, m3, 8
    pmaddwd         m7, m9, [r3 - 11 * 32]          ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m12, m2, m0, 8
    pmaddwd         m11, m12, [r3 - 11 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m7, m11

    pmaddwd         m8, m9, [r3 + 2 * 32]           ; [17]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m10, m12, [r3 + 2 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m8, m10

    pmaddwd         m9, [r3 + 15 * 32]              ; [30]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m12, [r3 + 15 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m9, m12

    palignr         m11, m0, m3, 12
    pmaddwd         m10, m11, [r3 - 4 * 32]         ; [11]
    paddd           m10, [pd_16]
    psrld           m10, 5
    palignr         m12, m2, m0, 12
    pmaddwd         m3, m12, [r3 - 4 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m10, m3

    pmaddwd         m11, [r3 + 9 * 32]              ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, [r3 + 9 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    pmaddwd         m4, m0, [r3 - 10 * 32]          ; [5]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 - 10  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m0, [r3 + 3 * 32]           ; [18]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m3, m2, [r3 + 3 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m5, m3

    pmaddwd         m6, m0, [r3 + 16 * 32]          ; [31]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, m2, [r3 + 16 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m8, m2, m0, 4
    pmaddwd         m7, m8, [r3 - 3 * 32]           ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m9, m1, m2, 4
    pmaddwd         m3, m9, [r3 - 3 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m7, m3

    pmaddwd         m8, [r3 + 10 * 32]              ; [25]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 10 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m10, m2, m0, 8
    pmaddwd         m9, m10, [r3 - 9 * 32]          ; [6]
    paddd           m9, [pd_16]
    psrld           m9, 5
    palignr         m12, m1, m2, 8
    pmaddwd         m3, m12, [r3 - 9 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, [r3 + 4 * 32]              ; [19]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, [r3 + 4 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    palignr         m11, m2, m0, 12
    pmaddwd         m11, [r3 - 15 * 32]             ; [0]
    paddd           m11, [pd_16]
    psrld           m11, 5
    palignr         m3, m1, m2, 12
    pmaddwd         m3, [r3 - 15 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m11, m3
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 0, 1, 16
    ret

cglobal intra_pred_ang32_6, 3,8,14
    add         r2,        128
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 15 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r7,        [r0 + 8 * r1]

    call        ang16_mode_6_30

    add         r2,        12
    lea         r0,        [r0 + 32]

    call        ang32_mode_6_30

    add         r2,        20
    lea         r0,        [r7 + 8 * r1]

    call        ang16_mode_6_30

    add         r2,        12
    lea         r0,        [r0 + 32]

    call        ang32_mode_6_30
    RET

cglobal intra_pred_ang32_30, 3,7,14
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 15 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r5,        [r0 + 32]

    call        ang16_mode_6_30

    add         r2,        12

    call        ang32_mode_6_30

    add         r2,        20
    mov         r0,        r5

    call        ang16_mode_6_30

    add         r2,        12

    call        ang32_mode_6_30
    RET

;; angle 32, modes 7 and 29
cglobal ang32_mode_7_29
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m1, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m1, m4                      ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]
    punpckhwd       m1, m4                          ; [25 24 24 23 23 22 22 21 17 16 16 15 15 14 14 13]

    pmaddwd         m4, m3, [r3 + 8 * 32]           ; [25]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 + 8 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m8, m0, m3, 4
    pmaddwd         m5, m8, [r3 - 15 * 32]          ; [2]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m9, m2, m0, 4
    pmaddwd         m10, m9, [r3 - 15 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m5, m10

    pmaddwd         m6, m8, [r3 - 6 * 32]           ; [11]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, m9, [r3 - 6 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    pmaddwd         m7, m8, [r3 + 3 * 32]           ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m10, m9, [r3 + 3 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    pmaddwd         m8, [r3 + 12 * 32]              ; [29]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 12 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m11, m0, m3, 8
    pmaddwd         m9, m11, [r3 - 11 * 32]         ; [6]
    paddd           m9, [pd_16]
    psrld           m9, 5
    palignr         m12, m2, m0, 8
    pmaddwd         m10, m12, [r3 - 11 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m11, [r3 - 2 * 32]         ; [15]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m13, m12, [r3 - 2 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m10, m13

    pmaddwd         m11, [r3 + 7 * 32]              ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, [r3 + 7 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m5, m0, m3, 12
    pmaddwd         m4, m5, [r3 - 16 * 32]          ; [1]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m6, m2, m0, 12
    pmaddwd         m7, m6, [r3 - 16 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m4, m7

    pmaddwd         m5, [r3 - 7 * 32]               ; [10]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, [r3 - 7 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    palignr         m9, m0, m3, 12
    pmaddwd         m6, m9, [r3 + 2 * 32]           ; [19]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m3, m2, m0, 12
    pmaddwd         m7, m3, [r3 + 2 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    pmaddwd         m7, m9, [r3 + 11 * 32]          ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m3, [r3 + 11 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m0, [r3 - 12 * 32]          ; [5]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m10, m2, [r3 - 12 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m8, m10

    pmaddwd         m9, m0, [r3 - 3 * 32]           ; [14]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, m2, [r3 - 3 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, m0, [r3 + 6 * 32]          ; [23]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m2, [r3 + 6 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    palignr         m11, m2, m0, 4
    pmaddwd         m11, [r3 - 17 * 32]             ; [0]
    paddd           m11, [pd_16]
    psrld           m11, 5
    palignr         m12, m1, m2, 4
    pmaddwd         m12, [r3 - 17 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 3, 2, 16
    ret

cglobal intra_pred_ang32_7, 3,8,14
    add         r2,        128
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 17 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r7,        [r0 + 8 * r1]

    call        ang16_mode_7_29

    add         r2,        8
    lea         r0,        [r0 + 32]

    call        ang32_mode_7_29

    add         r2,        24
    lea         r0,        [r7 + 8 * r1]

    call        ang16_mode_7_29

    add         r2,        8
    lea         r0,        [r0 + 32]

    call        ang32_mode_7_29
    RET

cglobal intra_pred_ang32_29, 3,7,14
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 17 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r5,        [r0 + 32]

    call        ang16_mode_7_29

    add         r2,        8

    call        ang32_mode_7_29

    add         r2,        24
    mov         r0,        r5

    call        ang16_mode_7_29

    add         r2,        8

    call        ang32_mode_7_29
    RET

;; angle 32, modes 8 and 28
cglobal ang32_mode_8_28
    test            r6d, r6d

    movu            m0, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]
    movu            m1, [r2 + 4]                    ; [17 16 15 14 13 12 11 10  9  8  7  6  5  4  3  2]

    punpcklwd       m3, m0, m1                      ; [13 12 12 11 11 10 10  9  5  4  4  3  3  2  2  1]
    punpckhwd       m0, m1                          ; [17 16 16 15 15 14 14 13  9  8  8  7  7  6  6  5]

    movu            m2, [r2 + 18]                   ; [24 23 22 21 20 19 18 17 16 15 14 13 12 11 10  9]
    movu            m4, [r2 + 20]                   ; [25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10]
    punpcklwd       m2, m4                          ; [21 20 20 19 19 18 18 17 13 12 12 11 11 10 10  9]

    pmaddwd         m4, m3, [r3 + 6 * 32]           ; [21]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 + 6 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 11 * 32]          ; [26]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 11 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m3, [r3 + 16 * 32]          ; [31]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m0, [r3 + 16 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    palignr         m11, m0, m3, 4
    pmaddwd         m7, m11, [r3 - 11 * 32]         ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    palignr         m1, m2, m0, 4
    pmaddwd         m8, m1, [r3 - 11 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m11, [r3 - 6 * 32]          ; [9]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m1, [r3 - 6 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m11, [r3 - 1 * 32]          ; [14]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m1, [r3 - 1 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m11, [r3 + 4 * 32]         ; [19]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m1, [r3 + 4 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m11, [r3 + 9 * 32]              ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m1, [r3 + 9 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m11, m1

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 0

    palignr         m4, m0, m3, 4
    pmaddwd         m4, [r3 + 14 * 32]              ; [29]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m5, m2, m0, 4
    pmaddwd         m5, [r3 + 14 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m1, m0, m3, 8
    pmaddwd         m5, m1, [r3 - 13 * 32]          ; [2]
    paddd           m5, [pd_16]
    psrld           m5, 5
    palignr         m10, m2, m0, 8
    pmaddwd         m6, m10, [r3 - 13 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m1, [r3 - 8 * 32]           ; [7]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m10, [r3 - 8 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m1, [r3 - 3 * 32]           ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m10, [r3 - 3 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m1, [r3 + 2 * 32]           ; [17]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m10, [r3 + 2 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m1, [r3 + 7 * 32]           ; [22]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m11, m10, [r3 + 7 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m9, m11

    pmaddwd         m1, [r3 + 12 * 32]              ; [27]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m10, [r3 + 12 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m1, m10

    palignr         m11, m0, m3, 12
    pmaddwd         m11, [r3 - 15 * 32]             ; [0]
    paddd           m11, [pd_16]
    psrld           m11, 5
    palignr         m2, m0, 12
    pmaddwd         m2, [r3 - 15 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m11, m2
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 1, 11, 0, 2, 16
    ret

cglobal intra_pred_ang32_8, 3,8,13
    add         r2,        128
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 15 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r7,        [r0 + 8 * r1]

    call        ang16_mode_8_28

    add         r2,        4
    lea         r0,        [r0 + 32]

    call        ang32_mode_8_28

    add         r2,        28
    lea         r0,        [r7 + 8 * r1]

    call        ang16_mode_8_28

    add         r2,        4
    lea         r0,        [r0 + 32]

    call        ang32_mode_8_28
    RET

cglobal intra_pred_ang32_28, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 15 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r5,        [r0 + 32]

    call        ang16_mode_8_28

    add         r2,        4

    call        ang32_mode_8_28

    add         r2,        28
    mov         r0,        r5

    call        ang16_mode_8_28

    add         r2,        4

    call        ang32_mode_8_28
    RET

cglobal intra_pred_ang32_9, 3,8,13
    add         r2,        128
    xor         r6d,       r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r7,        [r0 + 8 * r1]

    call        ang16_mode_9_27

    add         r2,        2
    lea         r0,        [r0 + 32]

    call        ang16_mode_9_27

    add         r2,        30
    lea         r0,        [r7 + 8 * r1]

    call        ang16_mode_9_27

    add         r2,        2
    lea         r0,        [r0 + 32]

    call        ang16_mode_9_27
    RET

cglobal intra_pred_ang32_27, 3,7,13
    xor         r6d,       r6d
    inc         r6d
    lea         r3,        [ang_table_avx2 + 16 * 32]
    add         r1d,       r1d
    lea         r4,        [r1 * 3]
    lea         r5,        [r0 + 32]

    call        ang16_mode_9_27

    add         r2,        2

    call        ang16_mode_9_27

    add         r2,        30
    mov         r0,        r5

    call        ang16_mode_9_27

    add         r2,        2

    call        ang16_mode_9_27
    RET

cglobal intra_pred_ang32_10, 3,4,2
    add             r2, mmsize*4
    add             r1d, r1d
    lea             r3, [r1 * 3]

    vpbroadcastw    m0, [r2 + 2]       ; [1...]
    movu            [r0], m0
    movu            [r0 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 2]   ; [2...]
    movu            [r0 + r1], m1
    movu            [r0 + r1 + 32], m1
    vpbroadcastw    m0, [r2 + 2 + 4]   ; [3...]
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 2 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 6]   ; [4...]
    movu            [r0 + r3], m1
    movu            [r0 + r3 + 32], m1

    lea             r0, [r0 + r1 * 4]
    vpbroadcastw    m0, [r2 + 2 + 8]   ; [5...]
    movu            [r0], m0
    movu            [r0 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 10]  ; [6...]
    movu            [r0 + r1], m1
    movu            [r0 + r1 + 32], m1
    vpbroadcastw    m0, [r2 + 2 + 12]  ; [7...]
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 2 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 14]  ; [8...]
    movu            [r0 + r3], m1
    movu            [r0 + r3 + 32], m1

    lea             r0, [r0 + r1 *4]
    vpbroadcastw    m0, [r2 + 2 + 16]  ; [9...]
    movu            [r0], m0
    movu            [r0 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 18]  ; [10...]
    movu            [r0 + r1], m1
    movu            [r0 + r1 + 32], m1
    vpbroadcastw    m0, [r2 + 2 + 20]  ; [11...]
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 2 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 22]  ; [12...]
    movu            [r0 + r3], m1
    movu            [r0 + r3 + 32], m1

    lea             r0, [r0 + r1 *4]
    vpbroadcastw    m0, [r2 + 2 + 24]  ; [13...]
    movu            [r0], m0
    movu            [r0 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 26]  ; [14...]
    movu            [r0 + r1], m1
    movu            [r0 + r1 + 32], m1
    vpbroadcastw    m0, [r2 + 2 + 28]  ; [15...]
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 2 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 30]  ; [16...]
    movu            [r0 + r3], m1
    movu            [r0 + r3 + 32], m1

    lea             r0, [r0 + r1 *4]
    vpbroadcastw    m0, [r2 + 2 + 32]  ; [17...]
    movu            [r0], m0
    movu            [r0 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 34]  ; [18...]
    movu            [r0 + r1], m1
    movu            [r0 + r1 + 32], m1
    vpbroadcastw    m0, [r2 + 2 + 36]  ; [19...]
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 2 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 38]  ; [20...]
    movu            [r0 + r3], m1
    movu            [r0 + r3 + 32], m1

    lea             r0, [r0 + r1 *4]
    vpbroadcastw    m0, [r2 + 2 + 40]  ; [21...]
    movu            [r0], m0
    movu            [r0 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 42]  ; [22...]
    movu            [r0 + r1], m1
    movu            [r0 + r1 + 32], m1
    vpbroadcastw    m0, [r2 + 2 + 44]  ; [23...]
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 2 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 46]  ; [24...]
    movu            [r0 + r3], m1
    movu            [r0 + r3 + 32], m1

    lea             r0, [r0 + r1 *4]
    vpbroadcastw    m0, [r2 + 2 + 48]  ; [25...]
    movu            [r0], m0
    movu            [r0 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 50]  ; [26...]
    movu            [r0 + r1], m1
    movu            [r0 + r1 + 32], m1
    vpbroadcastw    m0, [r2 + 2 + 52]  ; [27...]
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 2 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 54]  ; [28...]
    movu            [r0 + r3], m1
    movu            [r0 + r3 + 32], m1

    lea             r0, [r0 + r1 *4]
    vpbroadcastw    m0, [r2 + 2 + 56]  ; [29...]
    movu            [r0], m0
    movu            [r0 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 58]  ; [30...]
    movu            [r0 + r1], m1
    movu            [r0 + r1 + 32], m1
    vpbroadcastw    m0, [r2 + 2 + 60]  ; [31...]
    movu            [r0 + r1 * 2], m0
    movu            [r0 + r1 * 2 + 32], m0
    vpbroadcastw    m1, [r2 + 2 + 62]  ; [32...]
    movu            [r0 + r3], m1
    movu            [r0 + r3 + 32], m1
    RET

cglobal intra_pred_ang32_26, 3,3,2
    movu        m0,                 [r2 + 2]
    movu        m1,                 [r2 + 34]
    add         r1d,                r1d
    lea         r2,                 [r1 * 3]

    movu        [r0],               m0
    movu        [r0 + 32],          m1
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 32],     m1
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 32], m1
    movu        [r0 + r2],          m0
    movu        [r0 + r2 + 32],     m1

    lea         r0,                 [r0 + r1 *4]
    movu        [r0],               m0
    movu        [r0 + 32],          m1
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 32],     m1
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 32], m1
    movu        [r0 + r2],          m0
    movu        [r0 + r2 + 32],     m1

    lea         r0,                 [r0 + r1 *4]
    movu        [r0],               m0
    movu        [r0 + 32],          m1
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 32],     m1
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 32], m1
    movu        [r0 + r2],          m0
    movu        [r0 + r2 + 32],     m1

    lea         r0,                 [r0 + r1 *4]
    movu        [r0],               m0
    movu        [r0 + 32],          m1
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 32],     m1
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 32], m1
    movu        [r0 + r2],          m0
    movu        [r0 + r2 + 32],     m1

    lea         r0,                 [r0 + r1 *4]
    movu        [r0],               m0
    movu        [r0 + 32],          m1
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 32],     m1
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 32], m1
    movu        [r0 + r2],          m0
    movu        [r0 + r2 + 32],     m1

    lea         r0,                 [r0 + r1 *4]
    movu        [r0],               m0
    movu        [r0 + 32],          m1
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 32],     m1
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 32], m1
    movu        [r0 + r2],          m0
    movu        [r0 + r2 + 32],     m1

    lea         r0,                 [r0 + r1 *4]
    movu        [r0],               m0
    movu        [r0 + 32],          m1
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 32],     m1
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 32], m1
    movu        [r0 + r2],          m0
    movu        [r0 + r2 + 32],     m1

    lea         r0,                 [r0 + r1 *4]
    movu        [r0],               m0
    movu        [r0 + 32],          m1
    movu        [r0 + r1],          m0
    movu        [r0 + r1 + 32],     m1
    movu        [r0 + r1 * 2],      m0
    movu        [r0 + r1 * 2 + 32], m1
    movu        [r0 + r2],          m0
    movu        [r0 + r2 + 32],     m1
    RET

cglobal intra_pred_ang32_11, 3,8,12, 0-8
    movzx       r5d,        word [r2 + 128]  ; [0]
    movzx       r6d,        word [r2]
    mov         [rsp],      r5w
    mov         [r2 + 128], r6w

    movzx       r5d,        word [r2 + 126]  ; [16]
    movzx       r6d,        word [r2 + 32]
    mov         [rsp + 4],  r5w
    mov         [r2 + 126], r6w

    add         r2,         128
    xor         r6d,        r6d
    lea         r3,         [ang_table_avx2 + 16 * 32]
    add         r1d,        r1d
    lea         r4,         [r1 * 3]
    lea         r7,         [r0 + 8 * r1]

    call        ang16_mode_11_25

    sub         r2,         2
    lea         r0,         [r0 + 32]

    call        ang16_mode_11_25

    add         r2,         34
    lea         r0,         [r7 + 8 * r1]

    call        ang16_mode_11_25

    sub         r2,         2
    lea         r0,         [r0 + 32]

    call        ang16_mode_11_25

    mov         r6d,        [rsp]
    mov         [r2 - 30], r6w
    mov         r6d,       [rsp + 4]
    mov         [r2 - 32], r6w
    RET

cglobal intra_pred_ang32_25, 3,7,12, 0-4
    xor         r6d,        r6d
    inc         r6d
    lea         r3,         [ang_table_avx2 + 16 * 32]
    add         r1d,        r1d

    movzx       r4d,        word [r2 - 2]
    movzx       r5d,        word [r2 + 160]     ; [16]
    mov         [rsp],      r4w
    mov         [r2 - 2],   r5w

    lea         r4,         [r1 * 3]
    lea         r5,         [r0 + 32]

    call        ang16_mode_11_25

    sub         r2,         2

    call        ang16_mode_11_25

    add         r2,         34
    mov         r0,         r5

    call        ang16_mode_11_25

    sub         r2,         2

    call        ang16_mode_11_25

    mov         r5d,        [rsp]
    mov         [r2 - 32],  r5w
    RET

;; angle 32, modes 12 and 24, row 0 to 15
cglobal ang32_mode_12_24_0_15
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m2, m0, m4                      ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 + 11 * 32]          ; [27]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 + 11 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 6 * 32]           ; [22]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m2, [r3 + 6 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m3, [r3 + 1 * 32]           ; [17]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m2, [r3 + 1 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, m3, [r3 - 4 * 32]           ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m2, [r3 - 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m3, [r3 - 9 * 32]           ; [7]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m2, [r3 - 9 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 - 14 * 32]          ; [2]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m2, [r3 - 14 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m9, m2

    movu            xm1, [r2 - 8]
    pshufb          xm1, [pw_ang32_12_24]
    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]
    vinserti128     m1, m1, xm0, 1                  ; [ 7  7  6  6  5  5  4  4  6  6 13 13 19 19 26 26]

    palignr         m2, m3, m1, 14                  ; [11 10 10  9  9  8  8  7  3  2  2  1  1  0  0  6]
    palignr         m13, m0, m3, 14                 ; [15 14 14 13 13 12 12 11  7  6  6  5  5  4  4  3]

    pmaddwd         m10, m2, [r3 + 13 * 32]         ; [29]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 + 13 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m11, m2, [r3 + 8 * 32]          ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 + 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m13, m0, m3, 14

    pmaddwd         m4, m2, [r3 + 3 * 32]           ; [19]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 + 3 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m2, [r3 - 2 * 32]           ; [14]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m13, [r3 - 2 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m2, [r3 - 7 * 32]           ; [9]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m13, [r3 - 7 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m2, [r3 - 12 * 32]          ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 - 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m0, m3, 10
    palignr         m3, m1, 10

    pmaddwd         m8, m3, [r3 + 15 * 32]          ; [31]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 + 15 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 + 10 * 32]          ; [26]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m0, [r3 + 10 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m3, [r3 + 5 * 32]           ; [21]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m2, m0, [r3 + 5 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m10, m2

    pmaddwd         m3, [r3]                        ; [16]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, [r3]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 3, 0, 2, 16
    ret

;; angle 32, modes 12 and 24, row 16 to 31
cglobal ang32_mode_12_24_16_31
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m2, m0, m4                      ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]

    palignr         m2, m3, m1, 10
    palignr         m13, m0, m3, 10

    pmaddwd         m4, m2, [r3 - 5 * 32]           ; [11]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 - 5 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m2, [r3 - 10 * 32]          ; [6]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m13, [r3 - 10 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m2, [r3 - 15 * 32]          ; [1]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m13, [r3 - 15 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    palignr         m2, m3, m1, 6
    palignr         m13, m0, m3, 6

    pmaddwd         m7, m2, [r3 + 12 * 32]          ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 + 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m2, [r3 + 7 * 32]           ; [23]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 + 7 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 + 2 * 32]           ; [18]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 + 2 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m2, [r3 - 3 * 32]          ; [13]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 - 3 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m11, m2, [r3 - 8 * 32]          ; [8]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 - 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m13, m0, m3, 6

    pmaddwd         m4, m2, [r3 - 13 * 32]          ; [3]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 - 13 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m2, m3, m1, 2
    palignr         m13, m0, m3, 2

    pmaddwd         m5, m2, [r3 + 14 * 32]          ; [30]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m13, [r3 + 14 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m2, [r3 + 9 * 32]           ; [25]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m13, [r3 + 9 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m2, [r3 + 4 * 32]           ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 + 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m2, [r3 - 1 * 32]           ; [15]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 - 1 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 - 6 * 32]           ; [10]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 - 6 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m2, [r3 - 11 * 32]         ; [5]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 - 11 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m2, [r3 - 16 * 32]              ; [0]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m13, [r3 - 16 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m2, m13
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 2, 0, 3, 16
    ret

cglobal intra_pred_ang32_12, 3,8,14, 0-16
    movu        xm0, [r2 + 114]
    mova        [rsp], xm0

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    pinsrw      xm1, [r2], 7        ; [0]
    pinsrw      xm1, [r2 + 12], 6   ; [6]
    pinsrw      xm1, [r2 + 26], 5   ; [13]
    pinsrw      xm1, [r2 + 38], 4   ; [19]
    pinsrw      xm1, [r2 + 52], 3   ; [26]
    movu        [r2 + 114], xm1

    xor         r6d, r6d
    add         r2, 128
    lea         r7, [r0 + 8 * r1]

    call        ang32_mode_12_24_0_15

    lea         r0, [r0 + 32]

    call        ang32_mode_12_24_16_31

    add         r2, 32
    lea         r0, [r7 + 8 * r1]

    call        ang32_mode_12_24_0_15

    lea         r0, [r0 + 32]

    call        ang32_mode_12_24_16_31

    mova        xm0, [rsp]
    movu        [r2 - 46], xm0
    RET

cglobal intra_pred_ang32_24, 3,7,14, 0-16
    movu        xm0, [r2 - 16]
    mova        [rsp], xm0

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    pinsrw      xm1, [r2 + 140], 7   ; [6]
    pinsrw      xm1, [r2 + 154], 6   ; [13]
    pinsrw      xm1, [r2 + 166], 5   ; [19]
    pinsrw      xm1, [r2 + 180], 4   ; [26]
    movu        [r2 - 16], xm1

    xor         r6d, r6d
    inc         r6d
    lea         r5, [r0 + 32]

    call        ang32_mode_12_24_0_15

    call        ang32_mode_12_24_16_31

    add         r2, 32
    mov         r0, r5

    call        ang32_mode_12_24_0_15

    call        ang32_mode_12_24_16_31

    mova        xm0, [rsp]
    movu        [r2 - 48], xm0
    RET

;; angle 32, modes 13 and 23, row 0 to 15
cglobal ang32_mode_13_23_row_0_15
    test            r6d, r6d

    movu            m0, [r2]                        ; [15 14 13 12 11 10  9  8  7  6  5  4  3  2  1  0]
    movu            m4, [r2 + 2]                    ; [16 15 14 13 12 11 10  9  8  7  6  5  4  3  2  1]

    punpcklwd       m3, m0, m4                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]
    punpckhwd       m2, m0, m4                      ; [16 15 15 14 14 13 13 12  8  7  7  6  6  5  5  4]

    pmaddwd         m4, m3, [r3 + 7 * 32]           ; [23]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 + 7 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 - 2 * 32]           ; [14]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m2, [r3 - 2 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m3, [r3 - 11 * 32]          ; [5]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m2, [r3 - 11 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m6, m2

    movu            xm1, [r2 - 8]
    pshufb          xm1, [pw_ang32_12_24]
    punpcklwd       m3, m0, m0                      ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    punpckhwd       m0, m0                          ; [15 15 14 14 13 13 12 12  7  7  6  6  5  5  4  4]
    vinserti128     m1, m1, xm0, 1                  ; [ 7  7  6  6  5  5  4  4  4  4  7  7 11 11 14 14]

    palignr         m2, m3, m1, 14
    palignr         m13, m0, m3, 14

    pmaddwd         m7, m2, [r3 + 12 * 32]          ; [28]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 + 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m2, [r3 + 3 * 32]           ; [19]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 + 3 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 - 6 * 32]           ; [10]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 - 6 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m2, [r3 - 15 * 32]         ; [1]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 - 15 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    palignr         m2, m3, m1, 10
    palignr         m13, m0, m3, 10

    pmaddwd         m11, m2, [r3 + 8 * 32]          ; [24]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 + 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m13, m0, m3, 10

    pmaddwd         m4, m2, [r3 - 1 * 32]           ; [15]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 - 1 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m2, [r3 - 10 * 32]          ; [6]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m13, [r3 - 10 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    palignr         m2, m3, m1, 6
    palignr         m13, m0, m3, 6

    pmaddwd         m6, m2, [r3 + 13 * 32]          ; [29]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m13, [r3 + 13 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m2, [r3 + 4 * 32]           ; [20]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 + 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m2, [r3 - 5 * 32]           ; [11]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 - 5 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 - 14 * 32]          ; [2]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m13, [r3 - 14 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m9, m13

    palignr         m0, m3, 2
    palignr         m3, m1, 2

    pmaddwd         m1, m3, [r3 + 9 * 32]           ; [25]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m2, m0, [r3 + 9 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m1, m2

    pmaddwd         m3, [r3]                        ; [16]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, [r3]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 1, 3, 0, 2, 16
    ret

;; angle 32, modes 13 and 23, row 16 to 31
cglobal ang32_mode_13_23_row_16_31
    test            r6d, r6d

    movu            m0, [r2]                        ; [11 10  9  8  7  6  5  4  3  2  1  0  4  7 11 14]
    movu            m5, [r2 + 2]                    ; [12 11 10  9  8  7  6  5  4  3  2  1  0  4  7 11]

    punpcklwd       m4, m0, m5                      ; [ 8  7  7  6  6  5  5  4  0  4  4  7  7 11 11 14]
    punpckhwd       m2, m0, m5                      ; [12 11 11 10 10  9  9  8  4  3  3  2  2  1  1  0]

    pmaddwd         m4, [r3 - 9 * 32]               ; [7]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m2, [r3 - 9 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m4, m2

    movu            xm1, [r2 - 8]
    pshufb          xm1, [pw_ang32_12_24]           ; [18 18 21 21 25 25 28 28]
    punpcklwd       m3, m0, m0                      ; [ 7  7  6  6  5  5  4  4  4  4  7  7 11 11 14 14]
    punpckhwd       m0, m0                          ; [11 11 10 10  9  9  8  8  3  3  2  2  1  1  0  0]
    vinserti128     m1, m1, xm0, 1                  ; [ 3  3  2  2  1  1  0  0 18 18 21 21 25 25 28 28]

    palignr         m2, m3, m1, 14
    palignr         m13, m0, m3, 14

    pmaddwd         m5, m2, [r3 + 14 * 32]          ; [30]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m13, [r3 + 14 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m2, [r3 + 5 * 32]           ; [21]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, m13, [r3 + 5 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    pmaddwd         m7, m2, [r3 - 4 * 32]           ; [12]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 - 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    pmaddwd         m8, m2, [r3 - 13 * 32]          ; [3]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m13, [r3 - 13 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m2, m3, m1, 10
    palignr         m13, m0, m3, 10

    pmaddwd         m9, m2, [r3 + 10 * 32]          ; [26]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m13, [r3 + 10 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m2, [r3 + 1 * 32]          ; [17]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m13, [r3 + 1 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m11, m2, [r3 - 8 * 32]          ; [8]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m13, [r3 - 8 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m11, m13

    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0

    palignr         m2, m3, m1, 6
    palignr         m13, m0, m3, 6

    pmaddwd         m4, m2, [r3 + 15 * 32]          ; [31]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m13, [r3 + 15 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m2, [r3 + 6 * 32]           ; [22]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m13, [r3 + 6 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m2, [r3 - 3 * 32]           ; [13]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m13, [r3 - 3 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m2, [r3 - 12 * 32]          ; [4]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, m13, [r3 - 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m0, m3, 2
    palignr         m3, m1, 2

    pmaddwd         m8, m3, [r3 + 11 * 32]          ; [27]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 + 11 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m3, [r3 + 2 * 32]           ; [18]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m0, [r3 + 2 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m1, m3, [r3 - 7 * 32]           ; [9]
    paddd           m1, [pd_16]
    psrld           m1, 5
    pmaddwd         m2, m0, [r3 - 7 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m1, m2

    pmaddwd         m3, [r3 - 16 * 32]              ; [0]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, [r3 - 16 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0
    TRANSPOSE_STORE_AVX2 4, 5, 6, 7, 8, 9, 1, 3, 0, 2, 16
    ret

cglobal intra_pred_ang32_13, 3,8,14, 0-mmsize
    movu        m0, [r2 + 112]
    mova        [rsp], m0

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 8]
    movu        xm2, [r2 + 36]
    pshufb      xm1, [pw_ang32_13_23]
    pshufb      xm2, [pw_ang32_13_23]
    pinsrw      xm1, [r2 + 28], 4
    pinsrw      xm2, [r2 + 56], 4
    punpckhqdq  xm2, xm1            ; [ 4  7  8 11 18 21 25 28]

    movzx       r6d, word [r2]
    mov         [r2 + 128], r6w
    movu        [r2 + 112], xm2

    xor         r6d, r6d
    add         r2, 128
    lea         r7, [r0 + 8 * r1]

    call        ang32_mode_13_23_row_0_15

    sub         r2, 8
    lea         r0, [r0 + 32]

    call        ang32_mode_13_23_row_16_31

    add         r2, 40
    lea         r0, [r7 + 8 * r1]

    call        ang32_mode_13_23_row_0_15

    sub         r2, 8
    lea         r0, [r0 + 32]

    call        ang32_mode_13_23_row_16_31

    mova        m0, [rsp]
    movu        [r2 - 40], m0
    RET

cglobal intra_pred_ang32_23, 3,7,14, 0-16
    movu        xm0, [r2 - 16]
    mova        [rsp], xm0

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 136]
    movu        xm2, [r2 + 164]
    pshufb      xm1, [pw_ang32_13_23]
    pshufb      xm2, [pw_ang32_13_23]
    pinsrw      xm1, [r2 + 156], 4
    pinsrw      xm2, [r2 + 184], 4
    punpckhqdq  xm2, xm1            ; [ 4  7  8 11 18 21 25 28]

    movu        [r2 - 16], xm2

    xor         r6d, r6d
    inc         r6d
    lea         r5, [r0 + 32]

    call        ang32_mode_13_23_row_0_15

    sub         r2, 8

    call        ang32_mode_13_23_row_16_31

    add         r2, 40
    mov         r0, r5

    call        ang32_mode_13_23_row_0_15

    sub         r2, 8

    call        ang32_mode_13_23_row_16_31

    mova        xm0, [rsp]
    movu        [r2 - 40], xm0
    RET

%macro TRANSPOSE_STORE_AVX2_STACK 11
    jnz             .skip%11
    punpckhwd       m%9,  m%1,  m%2
    punpcklwd       m%1,  m%2
    punpckhwd       m%2,  m%3,  m%4
    punpcklwd       m%3,  m%4

    punpckldq       m%4,  m%1,  m%3
    punpckhdq       m%1,  m%3
    punpckldq       m%3,  m%9,  m%2
    punpckhdq       m%9,  m%2

    punpckhwd       m%10, m%5,  m%6
    punpcklwd       m%5,  m%6
    punpckhwd       m%6,  m%7,  m%8
    punpcklwd       m%7,  m%8

    punpckldq       m%8,  m%5,  m%7
    punpckhdq       m%5,  m%7
    punpckldq       m%7,  m%10, m%6
    punpckhdq       m%10, m%6

    punpcklqdq      m%6,  m%4,  m%8
    punpckhqdq      m%2,  m%4,  m%8
    punpcklqdq      m%4,  m%1,  m%5
    punpckhqdq      m%8,  m%1,  m%5

    punpcklqdq      m%1,  m%3,  m%7
    punpckhqdq      m%5,  m%3,  m%7
    punpcklqdq      m%3,  m%9,  m%10
    punpckhqdq      m%7,  m%9,  m%10

    movu            [r0 + r1 * 0 + %11], xm%6
    movu            [r0 + r1 * 1 + %11], xm%2
    movu            [r0 + r1 * 2 + %11], xm%4
    movu            [r0 + r4 * 1 + %11], xm%8

    lea             r5, [r0 + r1 * 4]
    movu            [r5 + r1 * 0 + %11], xm%1
    movu            [r5 + r1 * 1 + %11], xm%5
    movu            [r5 + r1 * 2 + %11], xm%3
    movu            [r5 + r4 * 1 + %11], xm%7

    lea             r5, [r5 + r1 * 4]
    vextracti128    [r5 + r1 * 0 + %11], m%6, 1
    vextracti128    [r5 + r1 * 1 + %11], m%2, 1
    vextracti128    [r5 + r1 * 2 + %11], m%4, 1
    vextracti128    [r5 + r4 * 1 + %11], m%8, 1

    lea             r5, [r5 + r1 * 4]
    vextracti128    [r5 + r1 * 0 + %11], m%1, 1
    vextracti128    [r5 + r1 * 1 + %11], m%5, 1
    vextracti128    [r5 + r1 * 2 + %11], m%3, 1
    vextracti128    [r5 + r4 * 1 + %11], m%7, 1
    jmp             .end%11
.skip%11:
%if %11 == 16
    lea             r7, [r0 + 8 * r1]
%else
    lea             r7, [r0]
%endif
    movu            [r7 + r1 * 0], m%1
    movu            [r7 + r1 * 1], m%2
    movu            [r7 + r1 * 2], m%3
    movu            [r7 + r4 * 1], m%4

%if %11 == 16
    lea             r7, [r7 + r1 * 4]
%else
    lea             r7, [r7 + r1 * 4]
%endif
    movu            [r7 + r1 * 0], m%5
    movu            [r7 + r1 * 1], m%6
    movu            [r7 + r1 * 2], m%7
    movu            [r7 + r4 * 1], m%8
.end%11:
%endmacro

;; angle 32, modes 14 and 22, row 0 to 15
cglobal ang32_mode_14_22_rows_0_15
    test            r6d, r6d

    movu            m0, [r2 - 12]
    movu            m1, [r2 - 10]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m1, [r2 + 4]
    movu            m4, [r2 + 6]
    punpcklwd       m2, m1, m4
    punpckhwd       m1, m4

    pmaddwd         m4, m3, [r3]                    ; [16]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 13 * 32]          ; [29]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 13 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m7, m0, m3, 4
    pmaddwd         m6, m7, [r3 - 6 * 32]           ; [10]
    paddd           m6, [pd_16]
    psrld           m6, 5
    palignr         m8, m2, m0, 4
    pmaddwd         m9, m8, [r3 - 6 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, [r3 + 7 * 32]               ; [23]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, [r3 + 7 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m10, m0, m3, 8
    pmaddwd         m8, m10, [r3 - 12 * 32]         ; [4]
    paddd           m8, [pd_16]
    psrld           m8, 5
    palignr         m12, m2, m0, 8
    pmaddwd         m9, m12, [r3 - 12 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m10, [r3 + 1 * 32]          ; [17]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m11, m12, [r3 + 1 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m9, m11

    pmaddwd         m10, [r3 + 14 * 32]             ; [30]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, [r3 + 14 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    palignr         m11, m0, m3, 12
    pmaddwd         m11, [r3 - 5 * 32]              ; [11]
    paddd           m11, [pd_16]
    psrld           m11, 5
    palignr         m12, m2, m0, 12
    pmaddwd         m12, [r3 - 5 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2_STACK 11, 10, 9, 8, 7, 6, 5, 4, 12, 13, 16

    palignr         m4, m0, m3, 12
    pmaddwd         m4, [r3 + 8 * 32]               ; [24]
    paddd           m4, [pd_16]
    psrld           m4, 5
    palignr         m5, m2, m0, 12
    pmaddwd         m5, [r3 + 8  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m0, [r3 - 11 * 32]          ; [5]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m3, m2, [r3 - 11 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m5, m3

    pmaddwd         m6, m0, [r3 + 2 * 32]           ; [18]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, m2, [r3 + 2 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    pmaddwd         m7, m0, [r3 + 15 * 32]          ; [31]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m3, m2, [r3 + 15 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m7, m3

    palignr         m9, m2, m0, 4
    palignr         m10, m1, m2, 4
    pmaddwd         m8, m9, [r3 - 4 * 32]           ; [12]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m11, m10, [r3 - 4 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m8, m11

    pmaddwd         m9, [r3 + 9 * 32]               ; [25]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, [r3 + 9 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    palignr         m1, m2, 8
    palignr         m2, m0, 8

    pmaddwd         m10, m2, [r3 - 10 * 32]         ; [6]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m12, m1, [r3 - 10 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m10, m12

    pmaddwd         m2, [r3 + 3 * 32]               ; [19]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m1, [r3 + 3 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m2, m1
    TRANSPOSE_STORE_AVX2_STACK 2, 10, 9, 8, 7, 6, 5, 4, 0, 1, 0
    ret

;; angle 32, modes 14 and 22, rows 16 to 31
cglobal ang32_mode_14_22_rows_16_31
    test            r6d, r6d

    movu            m0, [r2 - 24]
    movu            m1, [r2 - 22]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m1, [r2 - 8]
    movu            m4, [r2 - 6]
    punpcklwd       m2, m1, m4
    punpckhwd       m1, m4

    pmaddwd         m4, m3, [r3 - 16 * 32]          ; [0]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 16 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 - 3 * 32]           ; [13]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 - 3 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, m3, [r3 + 10 * 32]          ; [26]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m0, [r3 + 10 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    palignr         m8, m0, m3, 4
    palignr         m9, m2, m0, 4
    pmaddwd         m7, m8, [r3 - 9 * 32]           ; [7]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m10, m9, [r3 - 9 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    pmaddwd         m8, [r3 + 4 * 32]               ; [20]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 4 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m11, m0, m3, 8
    palignr         m12, m2, m0, 8
    pmaddwd         m9, m11, [r3 - 15 * 32]         ; [1]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m12, [r3 - 15 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m11, [r3 - 2 * 32]         ; [14]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m13, m12, [r3 - 2 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m10, m13

    pmaddwd         m11, [r3 + 11 * 32]             ; [27]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, [r3 + 11 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2_STACK 11, 10, 9, 8, 7, 6, 5, 4, 12, 13, 16

    palignr         m5, m0, m3, 12
    palignr         m6, m2, m0, 12
    pmaddwd         m4, m5, [r3 - 8 * 32]           ; [8]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m7, m6, [r3 - 8  * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m4, m7

    pmaddwd         m5, [r3 + 5 * 32]               ; [21]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, [r3 + 5 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    pmaddwd         m6, m0, [r3 - 14 * 32]          ; [2]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, m2, [r3 - 14 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    pmaddwd         m7, m0, [r3 - 1 * 32]           ; [15]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m3, m2, [r3 - 1 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m7, m3

    pmaddwd         m8, m0, [r3 + 12 * 32]          ; [28]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m11, m2, [r3 + 12 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m8, m11

    palignr         m10, m2, m0, 4
    palignr         m11, m1, m2, 4

    pmaddwd         m9, m10, [r3 - 7 * 32]          ; [9]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, m11, [r3 - 7 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, [r3 + 6 * 32]              ; [22]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 6 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m1, m2, 8
    palignr         m2, m0, 8

    pmaddwd         m2, [r3 - 13 * 32]              ; [3]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m1, [r3 - 13 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m2, m1
    TRANSPOSE_STORE_AVX2_STACK 2, 10, 9, 8, 7, 6, 5, 4, 0, 1, 0
    ret

cglobal intra_pred_ang32_14, 3,8,14
    mov         r6, rsp
    sub         rsp, 4*mmsize+gprsize
    and         rsp, ~63
    mov         [rsp+4*mmsize], r6

    movu        m0, [r2 + 128]
    movu        m1, [r2 + 160]
    movd        xm2, [r2 + 192]

    mova        [rsp + 1*mmsize], m0
    mova        [rsp + 2*mmsize], m1
    movd        [rsp + 3*mmsize], xm2

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 4]
    movu        xm2, [r2 + 24]
    movu        xm3, [r2 + 44]
    pshufb      xm1, [pw_ang32_14_22]
    pshufb      xm2, [pw_ang32_14_22]
    pshufb      xm3, [pw_ang32_14_22]
    pinsrw      xm1, [r2 + 20], 4
    pinsrw      xm2, [r2 + 40], 4
    pinsrw      xm3, [r2 + 60], 4

    punpckhqdq  xm2, xm1            ; [ 2  5  7 10 12 15 17 20]
    punpckhqdq  xm3, xm3            ; [22 25 27 30 22 25 27 30]

    movzx       r6d, word [r2]
    mov         [rsp + 1*mmsize], r6w
    movu        [rsp + 16], xm2
    movq        [rsp + 8], xm3

    xor         r6d, r6d
    lea         r2, [rsp + 1*mmsize]
    lea         r7, [r0 + 8 * r1]

    call        ang32_mode_14_22_rows_0_15

    lea         r0, [r0 + 32]

    call        ang32_mode_14_22_rows_16_31

    add         r2, 32
    lea         r0, [r7 + 8 * r1]

    call        ang32_mode_14_22_rows_0_15

    lea         r0, [r0 + 32]

    call        ang32_mode_14_22_rows_16_31

    mov         rsp, [rsp+4*mmsize]
    RET

cglobal intra_pred_ang32_22, 3,8,14
    mov         r6, rsp
    sub         rsp, 4*mmsize+gprsize
    and         rsp, ~63
    mov         [rsp+4*mmsize], r6

    movu        m0, [r2]
    movu        m1, [r2 + 32]
    movd        xm2, [r2 + 64]

    mova        [rsp + 1*mmsize], m0
    mova        [rsp + 2*mmsize], m1
    movd        [rsp + 3*mmsize], xm2

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 132]
    movu        xm2, [r2 + 152]
    movu        xm3, [r2 + 172]
    pshufb      xm1, [pw_ang32_14_22]
    pshufb      xm2, [pw_ang32_14_22]
    pshufb      xm3, [pw_ang32_14_22]
    pinsrw      xm1, [r2 + 148], 4
    pinsrw      xm2, [r2 + 168], 4
    pinsrw      xm3, [r2 + 188], 4

    punpckhqdq  xm2, xm1            ; [ 2  5  7 10 12 15 17 20]
    punpckhqdq  xm3, xm3            ; [22 25 27 30 22 25 27 30]

    movu        [rsp + 16], xm2
    movq        [rsp + 8], xm3

    xor         r6d, r6d
    inc         r6d
    lea         r2, [rsp + 1*mmsize]
    lea         r5, [r0 + 32]

    call        ang32_mode_14_22_rows_0_15

    lea         r0, [r0 + 8 * r1]
    lea         r0, [r0 + 8 * r1]

    call        ang32_mode_14_22_rows_16_31

    add         r2, 32
    mov         r0, r5

    call        ang32_mode_14_22_rows_0_15

    lea         r0, [r0 + 8 * r1]
    lea         r0, [r0 + 8 * r1]

    call        ang32_mode_14_22_rows_16_31

    mov         rsp, [rsp+4*mmsize]
    RET

;; angle 32, modes 15 and 21, row 0 to 15
cglobal ang32_mode_15_21_rows_0_15
    test            r6d, r6d

    movu            m0, [r2 - 16]
    movu            m1, [r2 - 14]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m1, [r2]
    movu            m4, [r2 + 2]
    punpcklwd       m2, m1, m4
    punpckhwd       m1, m4

    pmaddwd         m4, m3, [r3]                    ; [16]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m0, m3, 4
    palignr         m7, m2, m0, 4
    pmaddwd         m5, m6, [r3 - 15 * 32]          ; [1]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m7, [r3 - 15 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, [r3 + 2 * 32]               ; [18]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 + 2 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m8, m0, m3, 8
    palignr         m9, m2, m0, 8
    pmaddwd         m7, m8, [r3 - 13 * 32]          ; [3]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m10, m9, [r3 - 13 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    pmaddwd         m8, [r3 + 4 * 32]               ; [20]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 4 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m10, m0, m3, 12
    palignr         m11, m2, m0, 12
    pmaddwd         m9, m10, [r3 - 11 * 32]         ; [5]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m12, m11, [r3 - 11 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m9, m12

    pmaddwd         m10, [r3 + 6 * 32]              ; [22]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 6 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    pmaddwd         m11, m0, [r3 - 9 * 32]          ; [7]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, m2, [r3 - 9 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2_STACK 11, 10, 9, 8, 7, 6, 5, 4, 12, 13, 16

    pmaddwd         m4, m0, [r3 + 8 * 32]           ; [24]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m2, [r3 + 8  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m2, m0, 4
    palignr         m7, m1, m2, 4
    pmaddwd         m5, m6, [r3 - 7 * 32]           ; [9]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m3, m7, [r3 - 7 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m5, m3

    pmaddwd         m6, [r3 + 10 * 32]              ; [26]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 + 10 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m8, m2, m0, 8
    palignr         m9, m1, m2, 8
    pmaddwd         m7, m8, [r3 - 5 * 32]           ; [11]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m3, m9, [r3 - 5 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m7, m3

    pmaddwd         m8, [r3 + 12 * 32]              ; [28]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 12 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m10, m2, m0, 12
    palignr         m11, m1, m2, 12
    pmaddwd         m9, m10, [r3 - 3 * 32]          ; [13]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, m11, [r3 - 3 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    pmaddwd         m10, [r3 + 14 * 32]             ; [30]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 14 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    pmaddwd         m2, [r3 - 1 * 32]               ; [15]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m1, [r3 - 1 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m2, m1
    TRANSPOSE_STORE_AVX2_STACK 2, 10, 9, 8, 7, 6, 5, 4, 0, 1, 0
    ret

;; angle 32, modes 15 and 21, rows 16 to 31
cglobal ang32_mode_15_21_rows_16_31
    test            r6d, r6d

    movu            m0, [r2 - 32]
    movu            m1, [r2 - 30]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m1, [r2 - 16]
    movu            m4, [r2 - 14]
    punpcklwd       m2, m1, m4
    punpckhwd       m1, m4

    pmaddwd         m4, m3, [r3 - 16 * 32]          ; [0]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 16 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 1 * 32]           ; [17]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 1 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m7, m0, m3, 4
    palignr         m8, m2, m0, 4
    pmaddwd         m6, m7, [r3 - 14 * 32]          ; [2]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m8, [r3 - 14 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, [r3 + 3 * 32]               ; [19]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, [r3 + 3 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m9, m0, m3, 8
    palignr         m10, m2, m0, 8
    pmaddwd         m8, m9, [r3 - 12 * 32]          ; [4]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m11, m10, [r3 - 12 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m8, m11

    pmaddwd         m9, [r3 + 5 * 32]               ; [21]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, [r3 + 5 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    palignr         m11, m0, m3, 12
    palignr         m12, m2, m0, 12
    pmaddwd         m10, m11, [r3 - 10 * 32]        ; [6]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m13, m12, [r3 - 10 * 32]
    paddd           m13, [pd_16]
    psrld           m13, 5
    packusdw        m10, m13

    pmaddwd         m11, [r3 + 7 * 32]              ; [23]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, [r3 + 7 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2_STACK 11, 10, 9, 8, 7, 6, 5, 4, 12, 13, 16

    pmaddwd         m4, m0, [r3 - 8 * 32]           ; [8]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m7, m2, [r3 - 8  * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m4, m7

    pmaddwd         m5, m0, [r3 + 9 * 32]           ; [25]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, m2, [r3 + 9 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    palignr         m7, m2, m0, 4
    palignr         m8, m1, m2, 4
    pmaddwd         m6, m7, [r3 - 6 * 32]           ; [10]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m3, m8, [r3 - 6 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m6, m3

    pmaddwd         m7, [r3 + 11 * 32]              ; [27]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, [r3 + 11 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m9, m2, m0, 8
    palignr         m3, m1, m2, 8
    pmaddwd         m8, m9, [r3 - 4 * 32]           ; [12]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m11, m3, [r3 - 4 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m8, m11

    pmaddwd         m9, [r3 + 13 * 32]              ; [29]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, [r3 + 13 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    palignr         m1, m2, 12
    palignr         m2, m0, 12
    pmaddwd         m10, m2, [r3 - 2 * 32]          ; [14]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, m1, [r3 - 2 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    pmaddwd         m2, [r3 + 15 * 32]              ; [31]
    paddd           m2, [pd_16]
    psrld           m2, 5
    pmaddwd         m1, [r3 + 15 * 32]
    paddd           m1, [pd_16]
    psrld           m1, 5
    packusdw        m2, m1
    TRANSPOSE_STORE_AVX2_STACK 2, 10, 9, 8, 7, 6, 5, 4, 0, 1, 0
    ret

cglobal intra_pred_ang32_15, 3,8,14
    mov         r6, rsp
    sub         rsp, 4*mmsize+gprsize
    and         rsp, ~63
    mov         [rsp+4*mmsize], r6

    movu        m0, [r2 + 128]
    movu        m1, [r2 + 160]
    movd        xm2, [r2 + 192]

    mova        [rsp + 1*mmsize], m0
    mova        [rsp + 2*mmsize], m1
    movd        [rsp + 3*mmsize], xm2

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 4]
    movu        xm2, [r2 + 18]
    movu        xm3, [r2 + 34]
    movu        xm4, [r2 + 48]
    pshufb      xm1, [pw_ang32_15_21]
    pshufb      xm2, [pw_ang32_15_21]
    pshufb      xm3, [pw_ang32_15_21]
    pshufb      xm4, [pw_ang32_15_21]

    punpckhqdq  xm2, xm1
    punpckhqdq  xm4, xm3

    movzx       r6d, word [r2]
    mov         [rsp + 1*mmsize], r6w
    movu        [rsp + 16], xm2
    movu        [rsp], xm4

    xor         r6d, r6d
    lea         r2, [rsp + 1*mmsize]
    lea         r7, [r0 + 8 * r1]

    call        ang32_mode_15_21_rows_0_15

    lea         r0, [r0 + 32]

    call        ang32_mode_15_21_rows_16_31

    add         r2, 32
    lea         r0, [r7 + 8 * r1]

    call        ang32_mode_15_21_rows_0_15

    lea         r0, [r0 + 32]

    call        ang32_mode_15_21_rows_16_31

    mov         rsp, [rsp+4*mmsize]
    RET

cglobal intra_pred_ang32_21, 3,8,14
    mov         r6, rsp
    sub         rsp, 4*mmsize+gprsize
    and         rsp, ~63
    mov         [rsp+4*mmsize], r6

    movu        m0, [r2]
    movu        m1, [r2 + 32]
    movd        xm2, [r2 + 64]

    mova        [rsp + 1*mmsize], m0
    mova        [rsp + 2*mmsize], m1
    movd        [rsp + 3*mmsize], xm2

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 132]
    movu        xm2, [r2 + 146]
    movu        xm3, [r2 + 162]
    movu        xm4, [r2 + 176]
    pshufb      xm1, [pw_ang32_15_21]
    pshufb      xm2, [pw_ang32_15_21]
    pshufb      xm3, [pw_ang32_15_21]
    pshufb      xm4, [pw_ang32_15_21]

    punpckhqdq  xm2, xm1
    punpckhqdq  xm4, xm3

    movu        [rsp + 16], xm2
    movu        [rsp], xm4

    xor         r6d, r6d
    inc         r6d
    lea         r2, [rsp + 1*mmsize]
    lea         r5, [r0 + 32]

    call        ang32_mode_15_21_rows_0_15

    lea         r0, [r0 + 8 * r1]
    lea         r0, [r0 + 8 * r1]

    call        ang32_mode_15_21_rows_16_31

    add         r2, 32
    mov         r0, r5

    call        ang32_mode_15_21_rows_0_15

    lea         r0, [r0 + 8 * r1]
    lea         r0, [r0 + 8 * r1]

    call        ang32_mode_15_21_rows_16_31

    mov         rsp, [rsp+4*mmsize]
    RET

;; angle 32, modes 16 and 20, row 0 to 15
cglobal ang32_mode_16_20_rows_0_15
    test            r6d, r6d

    movu            m0, [r2 - 20]
    movu            m1, [r2 - 18]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m1, [r2 - 4]                    ; [ 3  2  0 -1 -2 -3 -4 -5  -6  -7  -8  -9 -10 -11 -12 -13]
    movu            m4, [r2 - 2]                    ; [ 2  0 -1 -2 -3 -4 -5 -6  -7  -8  -9 -10 -11 -12 -13 -14]
    punpcklwd       m2, m1, m4                      ; [-3 -2 -4 -3 -5 -4 -6 -5 -11 -10 -12 -11 -13 -12 -14 -13]
    punpckhwd       m1, m4                          ; [ 2  3  2  0 -1  0 -2 -1  -7  -6  -8  -7  -9  -8 -10  -9]

    pmaddwd         m4, m3, [r3]                    ; [16]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m6, m0, m3, 4
    palignr         m7, m2, m0, 4
    pmaddwd         m5, m6, [r3 - 11 * 32]          ; [5]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m7, [r3 - 11 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    pmaddwd         m6, [r3 + 10 * 32]              ; [26]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m7, [r3 + 10 * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m6, m7

    palignr         m8, m0, m3, 8
    palignr         m9, m2, m0, 8
    pmaddwd         m7, m8, [r3 - 1 * 32]           ; [15]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m10, m9, [r3 - 1 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    palignr         m9, m0, m3, 12
    palignr         m12, m2, m0, 12
    pmaddwd         m8, m9, [r3 - 12 * 32]          ; [4]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m10, m12, [r3 - 12 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m8, m10

    pmaddwd         m9, [r3 + 9 * 32]               ; [25]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m12, [r3 + 9 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m9, m12

    pmaddwd         m10, m0, [r3 - 2 * 32]          ; [14]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, m2, [r3 - 2 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m11, m2, m0, 4
    palignr         m12, m1, m2, 4
    pmaddwd         m11, [r3 - 13 * 32]             ; [3]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, [r3 - 13 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2_STACK 11, 10, 9, 8, 7, 6, 5, 4, 12, 13, 16

    palignr         m4, m2, m0, 4
    palignr         m5, m1, m2, 4
    pmaddwd         m4, [r3 + 8 * 32]               ; [24]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, [r3 + 8  * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m5, m2, m0, 8
    palignr         m3, m1, m2, 8
    pmaddwd         m5, [r3 - 3 * 32]               ; [13]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m3, [r3 - 3 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m5, m3

    palignr         m7, m2, m0, 12
    palignr         m3, m1, m2, 12
    pmaddwd         m6, m7, [r3 - 14 * 32]          ; [2]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m3, [r3 - 14 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, [r3 + 7 * 32]               ; [23]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m3, [r3 + 7 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m7, m3

    pmaddwd         m8, m2, [r3 - 4 * 32]           ; [12]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m1, [r3 - 4 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    movu            m0, [r2 - 2]
    movu            m1, [r2]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m2, [r2 + 14]
    movu            m1, [r2 + 16]
    punpcklwd       m2, m1

    pmaddwd         m9, m3, [r3 - 15 * 32]          ; [1]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, m0, [r3 - 15 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    pmaddwd         m10, m3, [r3 + 6 * 32]          ; [22]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, m0, [r3 + 6 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m2, m0, 4
    palignr         m0, m3, 4
    pmaddwd         m0, [r3 - 5 * 32]               ; [11]
    paddd           m0, [pd_16]
    psrld           m0, 5
    pmaddwd         m2, [r3 - 5 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m0, m2
    TRANSPOSE_STORE_AVX2_STACK 0, 10, 9, 8, 7, 6, 5, 4, 2, 1, 0
    ret

;; angle 32, modes 16 and 20, rows 16 to 31
cglobal ang32_mode_16_20_rows_16_31
    test            r6d, r6d

    movu            m0, [r2 - 40]
    movu            m1, [r2 - 38]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m1, [r2 - 24]
    movu            m4, [r2 - 22]
    punpcklwd       m2, m1, m4
    punpckhwd       m1, m4

    pmaddwd         m4, m3, [r3 - 16 * 32]          ; [0]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 16 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 5 * 32]           ; [21]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 5 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m7, m0, m3, 4
    palignr         m8, m2, m0, 4
    pmaddwd         m6, m7, [r3 - 6 * 32]           ; [10]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m9, m8, [r3 - 6 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m6, m9

    pmaddwd         m7, [r3 + 15 * 32]              ; [31]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m8, [r3 + 15 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m7, m8

    palignr         m8, m0, m3, 8
    palignr         m9, m2, m0, 8
    pmaddwd         m8, [r3 + 4 * 32]               ; [20]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 4 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m10, m0, m3, 12
    palignr         m11, m2, m0, 12
    pmaddwd         m9, m10, [r3 - 7 * 32]          ; [9]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m12, m11, [r3 - 7 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m9, m12

    pmaddwd         m10, [r3 + 14 * 32]             ; [30]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 + 14 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    pmaddwd         m11, m0, [r3 + 3 * 32]          ; [19]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, m2, [r3 + 3 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2_STACK 11, 10, 9, 8, 7, 6, 5, 4, 12, 13, 16

    palignr         m5, m2, m0, 4
    palignr         m6, m1, m2, 4
    pmaddwd         m4, m5, [r3 - 8 * 32]           ; [8]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m7, m6, [r3 - 8  * 32]
    paddd           m7, [pd_16]
    psrld           m7, 5
    packusdw        m4, m7

    pmaddwd         m5, [r3 + 13 * 32]              ; [29]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m6, [r3 + 13 * 32]
    paddd           m6, [pd_16]
    psrld           m6, 5
    packusdw        m5, m6

    palignr         m6, m2, m0, 8
    palignr         m3, m1, m2, 8
    pmaddwd         m6, [r3 + 2 * 32]               ; [18]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m3, [r3 + 2 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m6, m3

    palignr         m8, m2, m0, 12
    palignr         m9, m1, m2, 12
    pmaddwd         m7, m8, [r3 - 9 * 32]           ; [7]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m10, m9, [r3 - 9 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m7, m10

    pmaddwd         m8, [r3 + 12 * 32]              ; [28]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, [r3 + 12 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    pmaddwd         m9, m2, [r3 + 1 * 32]           ; [17]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m3, m1, [r3 + 1 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m9, m3

    movu            m0, [r2 - 22]
    movu            m1, [r2 - 20]
    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    pmaddwd         m10, m3, [r3 - 10 * 32]         ; [6]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, m0, [r3 - 10 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    pmaddwd         m3, [r3 + 11 * 32]              ; [27]
    paddd           m3, [pd_16]
    psrld           m3, 5
    pmaddwd         m0, [r3 + 11 * 32]
    paddd           m0, [pd_16]
    psrld           m0, 5
    packusdw        m3, m0
    TRANSPOSE_STORE_AVX2_STACK 3, 10, 9, 8, 7, 6, 5, 4, 0, 1, 0
    ret

cglobal intra_pred_ang32_16, 3,8,14
    mov         r6, rsp
    sub         rsp, 5*mmsize+gprsize
    and         rsp, ~63
    mov         [rsp+5*mmsize], r6

    movu        m0, [r2 + 128]
    movu        m1, [r2 + 160]
    movd        xm2, [r2 + 192]

    mova        [rsp + 2*mmsize], m0
    mova        [rsp + 3*mmsize], m1
    movd        [rsp + 4*mmsize], xm2

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 4]
    movu        xm2, [r2 + 16]
    movu        xm3, [r2 + 28]
    movu        xm4, [r2 + 40]
    movu        xm5, [r2 + 52]
    pshufb      xm1, [pw_ang32_16_20]
    pshufb      xm2, [pw_ang32_16_20]
    pshufb      xm3, [pw_ang32_16_20]
    pshufb      xm4, [pw_ang32_16_20]
    pshufb      xm5, [pw_ang32_16_20]

    punpckhqdq  xm2, xm1
    punpckhqdq  xm4, xm3
    punpckhqdq  xm5, xm5

    movzx       r6d, word [r2]
    mov         [rsp + 2*mmsize], r6w
    movu        [rsp + 48], xm2
    movu        [rsp + 32], xm4
    movq        [rsp + 24], xm5

    xor         r6d, r6d
    lea         r2, [rsp + 2*mmsize]
    lea         r7, [r0 + 8 * r1]

    call        ang32_mode_16_20_rows_0_15

    lea         r0, [r0 + 32]

    call        ang32_mode_16_20_rows_16_31

    add         r2, 32
    lea         r0, [r7 + 8 * r1]

    call        ang32_mode_16_20_rows_0_15

    lea         r0, [r0 + 32]

    call        ang32_mode_16_20_rows_16_31

    mov         rsp, [rsp+5*mmsize]
    RET

cglobal intra_pred_ang32_20, 3,8,14
    mov         r6, rsp
    sub         rsp, 5*mmsize+gprsize
    and         rsp, ~63
    mov         [rsp+5*mmsize], r6

    movu        m0, [r2]
    movu        m1, [r2 + 32]
    movd        xm2, [r2 + 64]

    mova        [rsp + 2*mmsize], m0
    mova        [rsp + 3*mmsize], m1
    movd        [rsp + 4*mmsize], xm2

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 132]
    movu        xm2, [r2 + 144]
    movu        xm3, [r2 + 156]
    movu        xm4, [r2 + 168]
    movu        xm5, [r2 + 180]
    pshufb      xm1, [pw_ang32_16_20]
    pshufb      xm2, [pw_ang32_16_20]
    pshufb      xm3, [pw_ang32_16_20]
    pshufb      xm4, [pw_ang32_16_20]
    pshufb      xm5, [pw_ang32_16_20]

    punpckhqdq  xm2, xm1
    punpckhqdq  xm4, xm3
    punpckhqdq  xm5, xm5

    movu        [rsp + 48], xm2
    movu        [rsp + 32], xm4
    movq        [rsp + 24], xm5

    xor         r6d, r6d
    inc         r6d
    lea         r2, [rsp + 2*mmsize]
    lea         r5, [r0 + 32]

    call        ang32_mode_16_20_rows_0_15

    lea         r0, [r0 + 8 * r1]
    lea         r0, [r0 + 8 * r1]

    call        ang32_mode_16_20_rows_16_31

    add         r2, 32
    mov         r0, r5

    call        ang32_mode_16_20_rows_0_15

    lea         r0, [r0 + 8 * r1]
    lea         r0, [r0 + 8 * r1]

    call        ang32_mode_16_20_rows_16_31

    mov         rsp, [rsp+5*mmsize]
    RET

;; angle 32, modes 17 and 19, row 0 to 15
cglobal ang32_mode_17_19_rows_0_15
    test            r6d, r6d

    movu            m0, [r2 - 24]
    movu            m1, [r2 - 22]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m1, [r2 - 8]
    movu            m4, [r2 - 6]
    punpcklwd       m2, m1, m4
    punpckhwd       m1, m4

    pmaddwd         m4, m3, [r3 - 16 * 32]              ; [0]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, m0, [r3 - 16 * 32]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    pmaddwd         m5, m3, [r3 + 10 * 32]              ; [26]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m8, m0, [r3 + 10 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m5, m8

    palignr         m6, m0, m3, 4
    palignr         m8, m2, m0, 4
    pmaddwd         m6, [r3 + 4 * 32]                   ; [20]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, [r3 + 4 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    palignr         m7, m0, m3, 8
    palignr         m9, m2, m0, 8
    pmaddwd         m7, [r3 - 2 * 32]                   ; [14]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m9, [r3 - 2 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m7, m9

    palignr         m8, m0, m3, 12
    palignr         m10, m2, m0, 12
    pmaddwd         m8, [r3 - 8 * 32]                   ; [8]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m10, [r3 - 8 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m8, m10

    pmaddwd         m9, m0, [r3 - 14 * 32]              ; [2]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m12, m2, [r3 - 14 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m9, m12

    pmaddwd         m10, m0, [r3 + 12 * 32]             ; [28]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, m2, [r3 + 12 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m11, m2, m0, 4
    palignr         m12, m1, m2, 4
    pmaddwd         m11, [r3 + 6 * 32]                  ; [22]
    paddd           m11, [pd_16]
    psrld           m11, 5
    pmaddwd         m12, [r3 + 6 * 32]
    paddd           m12, [pd_16]
    psrld           m12, 5
    packusdw        m11, m12

    TRANSPOSE_STORE_AVX2_STACK 11, 10, 9, 8, 7, 6, 5, 4, 12, 13, 16

    palignr         m4, m2, m0, 8
    palignr         m5, m1, m2, 8
    pmaddwd         m4, [r3]                            ; [16]
    paddd           m4, [pd_16]
    psrld           m4, 5
    pmaddwd         m5, [r3]
    paddd           m5, [pd_16]
    psrld           m5, 5
    packusdw        m4, m5

    palignr         m5, m2, m0, 12
    palignr         m3, m1, m2, 12
    pmaddwd         m5, [r3 - 6 * 32]                   ; [10]
    paddd           m5, [pd_16]
    psrld           m5, 5
    pmaddwd         m3, [r3 - 6 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m5, m3

    pmaddwd         m6, m2, [r3 - 12 * 32]              ; [4]
    paddd           m6, [pd_16]
    psrld           m6, 5
    pmaddwd         m8, m1, [r3 - 12 * 32]
    paddd           m8, [pd_16]
    psrld           m8, 5
    packusdw        m6, m8

    pmaddwd         m7, m2, [r3 + 14 * 32]              ; [30]
    paddd           m7, [pd_16]
    psrld           m7, 5
    pmaddwd         m3, m1, [r3 + 14 * 32]
    paddd           m3, [pd_16]
    psrld           m3, 5
    packusdw        m7, m3

    movu            m0, [r2 - 6]
    movu            m1, [r2 - 4]

    punpcklwd       m3, m0, m1
    punpckhwd       m0, m1

    movu            m2, [r2 + 10]
    movu            m1, [r2 + 12]
    punpcklwd       m2, m1

    pmaddwd         m8, m3, [r3 + 8 * 32]               ; [24]
    paddd           m8, [pd_16]
    psrld           m8, 5
    pmaddwd         m9, m0, [r3 + 8 * 32]
    paddd           m9, [pd_16]
    psrld           m9, 5
    packusdw        m8, m9

    palignr         m9, m0, m3, 4
    palignr         m10, m2, m0, 4
    pmaddwd         m9, [r3 + 2 * 32]                   ; [18]
    paddd           m9, [pd_16]
    psrld           m9, 5
    pmaddwd         m10, [r3 + 2 * 32]
    paddd           m10, [pd_16]
    psrld           m10, 5
    packusdw        m9, m10

    palignr         m10, m0, m3, 8
    palignr         m11, m2, m0, 8
    pmaddwd         m10, [r3 - 4 * 32]                  ; [12]
    paddd           m10, [pd_16]
    psrld           m10, 5
    pmaddwd         m11, [r3 - 4 * 32]
    paddd           m11, [pd_16]
    psrld           m11, 5
    packusdw        m10, m11

    palignr         m2, m0, 12
    palignr         m0, m3, 12
    pmaddwd         m0, [r3 - 10 * 32]                  ; [6]
    paddd           m0, [pd_16]
    psrld           m0, 5
    pmaddwd         m2, [r3 - 10 * 32]
    paddd           m2, [pd_16]
    psrld           m2, 5
    packusdw        m0, m2
    TRANSPOSE_STORE_AVX2_STACK 0, 10, 9, 8, 7, 6, 5, 4, 2, 1, 0
    ret

cglobal intra_pred_ang32_17, 3,8,14
    mov         r6, rsp
    sub         rsp, 5*mmsize+gprsize
    and         rsp, ~63
    mov         [rsp+5*mmsize], r6

    movu        m0, [r2 + 128]
    movu        m1, [r2 + 160]
    movd        xm2, [r2 + 192]

    mova        [rsp + 2*mmsize], m0
    mova        [rsp + 3*mmsize], m1
    movd        [rsp + 4*mmsize], xm2

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 2]
    movu        xm2, [r2 + 18]
    movu        xm3, [r2 + 34]
    movu        xm4, [r2 + 50]
    pshufb      xm1, [pw_ang32_17_19_0]
    pshufb      xm2, [shuf_mode_17_19]
    pshufb      xm3, [pw_ang32_17_19_0]
    pshufb      xm4, [shuf_mode_17_19]

    movzx       r6d, word [r2]
    mov         [rsp + 2*mmsize], r6w
    movu        [rsp + 48], xm1
    movu        [rsp + 36], xm2
    movu        [rsp + 22], xm3
    movu        [rsp + 10], xm4

    xor         r6d, r6d
    lea         r2, [rsp + 2*mmsize]
    lea         r7, [r0 + 8 * r1]

    call        ang32_mode_17_19_rows_0_15

    sub         r2, 26
    lea         r0, [r0 + 32]

    call        ang32_mode_17_19_rows_0_15

    add         r2, 58
    lea         r0, [r7 + 8 * r1]

    call        ang32_mode_17_19_rows_0_15

    sub         r2, 26
    lea         r0, [r0 + 32]

    call        ang32_mode_17_19_rows_0_15

    mov         rsp, [rsp+5*mmsize]
    RET

cglobal intra_pred_ang32_19, 3,8,14
    mov         r6, rsp
    sub         rsp, 5*mmsize+gprsize
    and         rsp, ~63
    mov         [rsp+5*mmsize], r6

    movu        m0, [r2]
    movu        m1, [r2 + 32]
    movd        xm2, [r2 + 64]

    mova        [rsp + 2*mmsize], m0
    mova        [rsp + 3*mmsize], m1
    movd        [rsp + 4*mmsize], xm2

    add         r1d, r1d
    lea         r4, [r1 * 3]
    lea         r3, [ang_table_avx2 + 16 * 32]

    movu        xm1, [r2 + 130]
    movu        xm2, [r2 + 146]
    movu        xm3, [r2 + 162]
    movu        xm4, [r2 + 178]
    pshufb      xm1, [pw_ang32_17_19_0]
    pshufb      xm2, [shuf_mode_17_19]
    pshufb      xm3, [pw_ang32_17_19_0]
    pshufb      xm4, [shuf_mode_17_19]

    movu        [rsp + 48], xm1
    movu        [rsp + 36], xm2
    movu        [rsp + 22], xm3
    movu        [rsp + 10], xm4

    xor         r6d, r6d
    inc         r6d
    lea         r2, [rsp + 2*mmsize]
    lea         r5, [r0 + 32]

    call        ang32_mode_17_19_rows_0_15

    sub         r2, 26
    lea         r0, [r0 + 8 * r1]
    lea         r0, [r0 + 8 * r1]

    call        ang32_mode_17_19_rows_0_15

    add         r2, 58
    mov         r0, r5

    call        ang32_mode_17_19_rows_0_15

    sub         r2, 26
    lea         r0, [r0 + 8 * r1]
    lea         r0, [r0 + 8 * r1]

    call        ang32_mode_17_19_rows_0_15

    mov         rsp, [rsp+5*mmsize]
    RET

cglobal intra_pred_ang32_18, 3,6,6
    mov         r4,                 rsp
    sub         rsp,                4*mmsize+gprsize
    and         rsp,                ~63
    mov         [rsp+4*mmsize],     r4

    movu        m0,                 [r2]
    movu        m1,                 [r2 + 32]
    mova        [rsp + 2*mmsize],   m0
    mova        [rsp + 3*mmsize],   m1

    movu        m2,                 [r2 + 130]
    movu        m3,                 [r2 + 162]
    pshufb      m2,                 [pw_swap16]
    pshufb      m3,                 [pw_swap16]
    vpermq      m2,                 m2, 01001110b
    vpermq      m3,                 m3, 01001110b
    mova        [rsp + 1*mmsize],   m2
    mova        [rsp + 0*mmsize],   m3

    add         r1d,                r1d
    lea         r2,                 [rsp+2*mmsize]
    lea         r4,                 [r1 * 2]
    lea         r3,                 [r1 * 3]
    lea         r5,                 [r1 * 4]

    movu        m0,                 [r2]
    movu        m1,                 [r2 + 32]
    movu        m2,                 [r2 - 16]
    movu        m3,                 [r2 + 16]

    movu        [r0],               m0
    movu        [r0 + 32],          m1

    palignr     m4,                 m0, m2, 14
    palignr     m5,                 m1, m3, 14
    movu        [r0 + r1],          m4
    movu        [r0 + r1 + 32],     m5

    palignr     m4,                 m0, m2, 12
    palignr     m5,                 m1, m3, 12
    movu        [r0 + r4],          m4
    movu        [r0 + r4 + 32],     m5

    palignr     m4,                 m0, m2, 10
    palignr     m5,                 m1, m3, 10
    movu        [r0 + r3],          m4
    movu        [r0 + r3 + 32],     m5

    add         r0,                 r5

    palignr     m4,                 m0, m2, 8
    palignr     m5,                 m1, m3, 8
    movu        [r0],               m4
    movu        [r0 + 32],          m5

    palignr     m4,                 m0, m2, 6
    palignr     m5,                 m1, m3, 6
    movu        [r0 + r1],          m4
    movu        [r0 + r1 + 32],     m5

    palignr     m4,                 m0, m2, 4
    palignr     m5,                 m1, m3, 4
    movu        [r0 + r4],          m4
    movu        [r0 + r4 + 32],     m5

    palignr     m4,                 m0, m2, 2
    palignr     m5,                 m1, m3, 2
    movu        [r0 + r3],          m4
    movu        [r0 + r3 + 32],     m5

    add         r0,                 r5

    movu        [r0],               m2
    movu        [r0 + 32],          m3

    movu        m0,                 [r2 - 32]
    movu        m1,                 [r2]

    palignr     m4,                 m2, m0, 14
    palignr     m5,                 m3, m1, 14
    movu        [r0 + r1],          m4
    movu        [r0 + r1 + 32],     m5

    palignr     m4,                 m2, m0, 12
    palignr     m5,                 m3, m1, 12
    movu        [r0 + r4],          m4
    movu        [r0 + r4 + 32],     m5

    palignr     m4,                 m2, m0, 10
    palignr     m5,                 m3, m1, 10
    movu        [r0 + r3],          m4
    movu        [r0 + r3 + 32],     m5

    add         r0,                 r5

    palignr     m4,                 m2, m0, 8
    palignr     m5,                 m3, m1, 8
    movu        [r0],               m4
    movu        [r0 + 32],          m5

    palignr     m4,                 m2, m0, 6
    palignr     m5,                 m3, m1, 6
    movu        [r0 + r1],          m4
    movu        [r0 + r1 + 32],     m5

    palignr     m4,                 m2, m0, 4
    palignr     m5,                 m3, m1, 4
    movu        [r0 + r4],          m4
    movu        [r0 + r4 + 32],     m5

    palignr     m4,                 m2, m0, 2
    palignr     m5,                 m3, m1, 2
    movu        [r0 + r3],          m4
    movu        [r0 + r3 + 32],     m5

    add         r0,                 r5

    movu        [r0],               m0
    movu        [r0 + 32],          m1

    movu        m2,                 [r2 - 48]
    movu        m3,                 [r2 - 16]

    palignr     m4,                 m0, m2, 14
    palignr     m5,                 m1, m3, 14
    movu        [r0 + r1],          m4
    movu        [r0 + r1 + 32],     m5

    palignr     m4,                 m0, m2, 12
    palignr     m5,                 m1, m3, 12
    movu        [r0 + r4],          m4
    movu        [r0 + r4 + 32],     m5

    palignr     m4,                 m0, m2, 10
    palignr     m5,                 m1, m3, 10
    movu        [r0 + r3],          m4
    movu        [r0 + r3 + 32],     m5

    add         r0,                 r5

    palignr     m4,                 m0, m2, 8
    palignr     m5,                 m1, m3, 8
    movu        [r0],               m4
    movu        [r0 + 32],          m5

    palignr     m4,                 m0, m2, 6
    palignr     m5,                 m1, m3, 6
    movu        [r0 + r1],          m4
    movu        [r0 + r1 + 32],     m5

    palignr     m4,                 m0, m2, 4
    palignr     m5,                 m1, m3, 4
    movu        [r0 + r4],          m4
    movu        [r0 + r4 + 32],     m5

    palignr     m4,                 m0, m2, 2
    palignr     m5,                 m1, m3, 2
    movu        [r0 + r3],          m4
    movu        [r0 + r3 + 32],     m5

    add         r0,                 r5

    movu        [r0],               m2
    movu        [r0 + 32],          m3

    movu        m0,                 [r2 - 64]
    movu        m1,                 [r2 - 32]

    palignr     m4,                 m2, m0, 14
    palignr     m5,                 m3, m1, 14
    movu        [r0 + r1],          m4
    movu        [r0 + r1 + 32],     m5

    palignr     m4,                 m2, m0, 12
    palignr     m5,                 m3, m1, 12
    movu        [r0 + r4],          m4
    movu        [r0 + r4 + 32],     m5

    palignr     m4,                 m2, m0, 10
    palignr     m5,                 m3, m1, 10
    movu        [r0 + r3],          m4
    movu        [r0 + r3 + 32],     m5

    add         r0,                 r5

    palignr     m4,                 m2, m0, 8
    palignr     m5,                 m3, m1, 8
    movu        [r0],               m4
    movu        [r0 + 32],          m5

    palignr     m4,                 m2, m0, 6
    palignr     m5,                 m3, m1, 6
    movu        [r0 + r1],          m4
    movu        [r0 + r1 + 32],     m5

    palignr     m4,                 m2, m0, 4
    palignr     m5,                 m3, m1, 4
    movu        [r0 + r4],          m4
    movu        [r0 + r4 + 32],     m5

    palignr     m4,                 m2, m0, 2
    palignr     m5,                 m3, m1, 2
    movu        [r0 + r3],          m4
    movu        [r0 + r3 + 32],     m5

    mov         rsp,                [rsp+4*mmsize]
    RET
;-------------------------------------------------------------------------------------------------------
; end of avx2 code for intra_pred_ang32 mode 2 to 34
;-------------------------------------------------------------------------------------------------------

%macro MODE_2_34 0
    movu            m0, [r2 + 4]
    movu            m1, [r2 + 20]
    movu            m2, [r2 + 36]
    movu            m3, [r2 + 52]
    movu            m4, [r2 + 68]
    movu            [r0], m0
    movu            [r0 + 16], m1
    movu            [r0 + 32], m2
    movu            [r0 + 48], m3
    palignr         m5, m1, m0, 2
    movu            [r0 + r1], m5
    palignr         m5, m2, m1, 2
    movu            [r0 + r1 + 16], m5
    palignr         m5, m3, m2, 2
    movu            [r0 + r1 + 32], m5
    palignr         m5, m4, m3, 2
    movu            [r0 + r1 + 48], m5
    palignr         m5, m1, m0, 4
    movu            [r0 + r3], m5
    palignr         m5, m2, m1, 4
    movu            [r0 + r3 + 16], m5
    palignr         m5, m3, m2, 4
    movu            [r0 + r3 + 32], m5
    palignr         m5, m4, m3, 4
    movu            [r0 + r3 + 48], m5
    palignr         m5, m1, m0, 6
    movu            [r0 + r4], m5
    palignr         m5, m2, m1, 6
    movu            [r0 + r4 + 16], m5
    palignr         m5, m3, m2, 6
    movu            [r0 + r4 + 32], m5
    palignr         m5, m4, m3, 6
    movu            [r0 + r4 + 48], m5
    lea             r0, [r0 + r1 * 4]
    palignr         m5, m1, m0, 8
    movu            [r0], m5
    palignr         m5, m2, m1, 8
    movu            [r0 + 16], m5
    palignr         m5, m3, m2, 8
    movu            [r0 + 32], m5
    palignr         m5, m4, m3, 8
    movu            [r0 + 48], m5
    palignr         m5, m1, m0, 10
    movu            [r0 + r1], m5
    palignr         m5, m2, m1, 10
    movu            [r0 + r1 + 16], m5
    palignr         m5, m3, m2, 10
    movu            [r0 + r1 + 32], m5
    palignr         m5, m4, m3, 10
    movu            [r0 + r1 + 48], m5
    palignr         m5, m1, m0, 12
    movu            [r0 + r3], m5
    palignr         m5, m2, m1, 12
    movu            [r0 + r3 + 16], m5
    palignr         m5, m3, m2, 12
    movu            [r0 + r3 + 32], m5
    palignr         m5, m4, m3, 12
    movu            [r0 + r3 + 48], m5
    palignr         m5, m1, m0, 14
    movu            [r0 + r4], m5
    palignr         m5, m2, m1, 14
    movu            [r0 + r4 + 16], m5
    palignr         m5, m3, m2, 14
    movu            [r0 + r4 + 32], m5
    palignr         m5, m4, m3, 14
    movu            [r0 + r4 + 48], m5
    lea             r0, [r0 + r1 * 4]
    movu            m0, [r2 + 84]
    movu            [r0], m1
    movu            [r0 + 16], m2
    movu            [r0 + 32], m3
    movu            [r0 + 48], m4
    palignr         m5, m2, m1, 2
    movu            [r0 + r1], m5
    palignr         m5, m3, m2, 2
    movu            [r0 + r1 + 16], m5
    palignr         m5, m4, m3, 2
    movu            [r0 + r1 + 32], m5
    palignr         m5, m0, m4, 2
    movu            [r0 + r1 + 48], m5
    palignr         m5, m2, m1, 4
    movu            [r0 + r3], m5
    palignr         m5, m3, m2, 4
    movu            [r0 + r3 + 16], m5
    palignr         m5, m4, m3, 4
    movu            [r0 + r3 + 32], m5
    palignr         m5, m0, m4, 4
    movu            [r0 + r3 + 48], m5
    palignr         m5, m2, m1, 6
    movu            [r0 + r4], m5
    palignr         m5, m3, m2, 6
    movu            [r0 + r4 + 16], m5
    palignr         m5, m4, m3, 6
    movu            [r0 + r4 + 32], m5
    palignr         m5, m0, m4, 6
    movu            [r0 + r4 + 48], m5
    lea             r0, [r0 + r1 * 4]
    palignr         m5, m2, m1, 8
    movu            [r0], m5
    palignr         m5, m3, m2, 8
    movu            [r0 + 16], m5
    palignr         m5, m4, m3, 8
    movu            [r0 + 32], m5
    palignr         m5, m0, m4, 8
    movu            [r0 + 48], m5
    palignr         m5, m2, m1, 10
    movu            [r0 + r1], m5
    palignr         m5, m3, m2, 10
    movu            [r0 + r1 + 16], m5
    palignr         m5, m4, m3, 10
    movu            [r0 + r1 + 32], m5
    palignr         m5, m0, m4, 10
    movu            [r0 + r1 + 48], m5
    palignr         m5, m2, m1, 12
    movu            [r0 + r3], m5
    palignr         m5, m3, m2, 12
    movu            [r0 + r3 + 16], m5
    palignr         m5, m4, m3, 12
    movu            [r0 + r3 + 32], m5
    palignr         m5, m0, m4, 12
    movu            [r0 + r3 + 48], m5
    palignr         m5, m2, m1, 14
    movu            [r0 + r4], m5
    palignr         m5, m3, m2, 14
    movu            [r0 + r4 + 16], m5
    palignr         m5, m4, m3, 14
    movu            [r0 + r4 + 32], m5
    palignr         m5, m0, m4, 14
    movu            [r0 + r4 + 48], m5
    lea             r0,    [r0 + r1 * 4]
%endmacro

%macro TRANSPOSE_STORE_8x8 6
  %if %2 == 1
    ; transpose 4x8 and then store, used by angle BLOCK_16x16 and BLOCK_32x32
    punpckhwd   m0, %3, %4
    punpcklwd   %3, %4
    punpckhwd   %4, %3, m0
    punpcklwd   %3, m0

    punpckhwd   m0, %5, %6
    punpcklwd   %5, %6
    punpckhwd   %6, %5, m0
    punpcklwd   %5, m0

    punpckhqdq  m0, %3, %5
    punpcklqdq  %3, %5
    punpcklqdq  %5, %4, %6
    punpckhqdq  %4, %6

    movu        [r0 + %1], %3
    movu        [r0 + r1 + %1], m0
    movu        [r0 + r1 * 2 + %1], %5
    movu        [r0 + r5 + %1], %4
  %else
    ; store 8x4, used by angle BLOCK_16x16 and BLOCK_32x32
    movh        [r0], %3
    movhps      [r0 + r1], %3
    movh        [r0 + r1 * 2], %4
    movhps      [r0 + r5], %4
    lea         r0, [r0 + r1 * 4]
    movh        [r0], %5
    movhps      [r0 + r1], %5
    movh        [r0 + r1 * 2], %6
    movhps      [r0 + r5], %6
    lea         r0, [r0 + r1 * 4]
  %endif
%endmacro

%macro MODE_3_33 1
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m3,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    mova        m7,        m0

    palignr     m1,        m3, m0, 2                  ; [9 8 7 6 5 4 3 2]
    punpckhwd   m2,        m0, m1                     ; [9 8 8 7 7 6 6 5] xmm2
    punpcklwd   m0,        m1                         ; [5 4 4 3 3 2 2 1] xmm0

    palignr     m1,        m2, m0, 4                  ; [6 5 5 4 4 3 3 2] xmm1
    pmaddwd     m4,        m0, [r3 + 10 * 16]         ; [26]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m5,        m1, [r3 + 4 * 16]          ; [20]
    paddd       m5,        [pd_16]
    psrld       m5,        5
    packusdw    m4,        m5

    palignr     m5,        m2, m0, 8
    pmaddwd     m5,        [r3 - 2 * 16]              ; [14]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m6,        m2, m0, 12
    pmaddwd     m6,        [r3 - 8 * 16]              ; [ 8]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m2, [r3 - 14 * 16]         ; [ 2]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m2, [r3 + 12 * 16]         ; [28]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m0,        m3, m2, 4                  ; [10 9 9 8 8 7 7 6]
    pmaddwd     m1,        m0, [r3 + 6 * 16]          ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    psrldq      m2,        m3, 2   ; [x 16 15 14 13 12 11 10]
    palignr     m2,        m0, 4   ;[11 10 10 9 9 8 8 7]

    pmaddwd     m2,        [r3]                       ; [16]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m1,        m2

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    palignr     m0,        m3, m7, 14                 ; [15 14 13 12 11 10 9 8]
    movu        m3,        [r2 + 32]                  ; [23 22 21 20 19 18 17 16]
    palignr     m1,        m3, m0, 2                  ; [16 15 14 13 12 11 10 9]
    punpckhwd   m7,        m0, m1                     ; [16 15 15 14 14 13 13 12]
    punpcklwd   m0,        m1                         ; [12 11 11 10 10 9 9 8]

    palignr     m5,        m7, m0, 4                  ; [13 12 12 11 11 10 10 9]
    pmaddwd     m4,        m0, [r3 - 6 * 16]          ; [10]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m5, [r3 - 12 * 16]         ; [04]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        [r3 + 14 * 16]             ; [30]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m6,        m7, m0, 8                  ; [14 13 13 12 12 11 11 10]
    pmaddwd     m6,        [r3 + 8 * 16]              ; [24]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    palignr     m1,        m7, m0, 12                 ; [15 14 14 13 13 12 12 11]
    pmaddwd     m6,        m1, [r3 + 2 * 16]          ; [18]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m7, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m2,        m3, m7, 4                  ; [17 16 16 15 15 14 14 13]
    pmaddwd     m1,        m2, [r3 - 10 * 16]         ; [6]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2 + 28]                  ; [00]

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    movu        m0,        [r2 + 28]                  ; [35 34 33 32 31 30 29 28]
    palignr     m1,        m0, 2                      ; [ x 35 34 33 32 31 30 29]
    punpckhwd   m2,        m0, m1                     ; [ x 35 35 34 34 33 33 32]
    punpcklwd   m0,        m1                         ; [32 31 31 30 30 29 29 28]

    pmaddwd     m4,        m0, [r3 + 10 * 16]         ; [26]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    palignr     m1,        m2, m0, 4                  ; [33 32 32 31 31 30 30 29]
    pmaddwd     m1,        [r3 + 4 * 16]              ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    palignr     m5,        m2, m0, 8                  ; [34 33 33 32 32 31 31 30]
    pmaddwd     m5,        [r3 - 2 * 16]              ; [14]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m6,        m2, m0, 12                 ; [35 34 34 33 33 32 32 31]
    pmaddwd     m6,        [r3 - 8 * 16]              ; [ 8]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pinsrw      m2,        [r2 + 44], 7               ; [35 34 34 33 33 32 32 31]
    pmaddwd     m6,        m2, [r3 - 14 * 16]         ; [ 2]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m2,        [r3 + 12 * 16]             ; [28]
    paddd       m2,        [pd_16]
    psrld       m2,        5
    packusdw    m6,        m2

    movu        m3,        [r2 + 38]                  ; [45 44 43 42 41 40 39 38]
    palignr     m1,        m3, 2                      ; [ x 45 44 43 42 41 40 39]
    punpckhwd   m2,        m3, m1                     ; [ x 35 35 34 34 33 33 32]
    punpcklwd   m3,        m1                         ; [32 31 31 30 30 29 29 28]

    pmaddwd     m1,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    palignr     m0,        m2, m3, 4
    pmaddwd     m0,        [r3]                       ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    palignr     m5,        m2, m3, 8
    pmaddwd     m4,        m5, [r3 - 6 * 16]          ; [10]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    palignr     m5,        m2, m3, 12
    pmaddwd     m1,        m5, [r3 - 12 * 16]         ; [04]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        [r3 + 14 * 16]             ; [30]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 46]
    palignr     m1,        m3, 2
    punpckhwd   m2,        m3, m1
    punpcklwd   m3,        m1

    pmaddwd     m6,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    palignr     m6,        m2, m3, 4
    pmaddwd     m6,        [r3 + 2 * 16]              ; [18]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    palignr     m1,        m2, m3, 8
    pmaddwd     m1,        [r3 - 4 * 16]              ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m1,        m2, m3, 12
    pmaddwd     m1,        [r3 - 10 * 16]             ; [06]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2 + 54]                  ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_4_32 1
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m3,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m1,        m3, m0, 2                  ; [9 8 7 6 5 4 3 2]
    punpckhwd   m2,        m0, m1                     ; [9 8 8 7 7 6 6 5]
    punpcklwd   m0,        m1                         ; [5 4 4 3 3 2 2 1]

    pmaddwd     m4,        m0, [r3 + 5 * 16]          ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    palignr     m5,        m2, m0, 4                  ; [6 5 5 4 4 3 3 2]
    pmaddwd     m1,        m5, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        [r3 + 15 * 16]             ; [31]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m6,        m2, m0, 8
    pmaddwd     m6,        [r3 + 4 * 16]              ; [ 20]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    palignr     m1,        m2, m0, 12
    pmaddwd     m6,        m1, [r3 - 7 * 16]          ; [ 9]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        [r3 + 14 * 16]             ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m2, [r3 + 3 * 16]          ; [19]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    palignr     m7,        m3, m2, 4                  ; [10 9 9 8 7 6 5 4]
    pmaddwd     m0,        m7, [r3 - 8 * 16]          ; [8]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m7, [r3 + 13 * 16]         ; [29]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m0,        [r2 + 34]                  ; [24 23 22 21 20 19 18 17]

    palignr     m2,        m0, m3, 2                  ; [17 16 15 14 13 12 11 10]
    palignr     m1,        m0, m3, 4                  ; [18 17 16 15 14 13 12 11]
    punpckhwd   m3,        m2, m1                     ; [18 17 17 16 16 15 15 14]
    punpcklwd   m2,        m1                         ; [14 13 13 12 12 11 11 10]

    palignr     m1,        m2, m7, 4                  ; [11 10 10 9 9 8 7 6]
    pmaddwd     m1,        [r3 +  2 * 16]             ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    palignr     m5,        m2, m7, 8
    mova        m6,        m5
    pmaddwd     m5,        [r3 - 9 * 16]              ; [07]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        [r3 + 12 * 16]             ; [28]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    palignr     m6,        m2, m7, 12
    pmaddwd     m6,        [r3 +      16]             ; [17]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m2, [r3 - 10 * 16]         ; [06]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m2, [r3 + 11 * 16]         ; [27]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    palignr     m7,        m3, m2, 4
    pmaddwd     m7,        [r3]                       ; [16]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m1,        m7
    mova        m7,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    palignr     m0,        m3, m2, 8
    pmaddwd     m4,        m0, [r3 - 11 * 16]         ; [5]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m0, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    palignr     m5,        m3, m2, 12
    pmaddwd     m5,        [r3 - 16]                  ; [15]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m1,        m3, [r3 - 12 * 16]         ; [4]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    pmaddwd     m6,        m3, [r3 + 9 * 16]          ; [25]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m0,        [r2 + 50]                  ; [32 31 30 29 28 27 26 25]
    palignr     m2,        m0, m7, 2                  ; [25 24 23 22 21 20 19 18]
    palignr     m1,        m0, m7, 4                  ; [26 25 24 23 22 21 20 19]
    punpckhwd   m7,        m2, m1                     ; [26 25 25 24 24 23 23 22]
    punpcklwd   m2,        m1                         ; [22 21 21 20 20 19 19 18]

    palignr     m1,        m2, m3, 4
    pmaddwd     m1,        [r3 - 2 * 16]              ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m1,        m2, m3, 8
    mova        m0,        m1
    pmaddwd     m1,        [r3 - 13 * 16]             ; [3]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        [r3 + 8 * 16]              ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    palignr     m4,        m2, m3, 12
    pmaddwd     m4,        [r3 - 3 * 16]              ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m2, [r3 - 14 * 16]         ; [2]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m2, [r3 + 7 * 16]          ; [23]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m6,        m7, m2, 4
    pmaddwd     m6,        [r3 - 4 * 16]              ; [12]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    palignr     m1,        m7, m2, 8
    pmaddwd     m6,        m1, [r3 - 15 * 16]         ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        [r3 + 6 * 16]              ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m1,        m7, m2, 12
    pmaddwd     m1,        [r3 - 5 * 16]              ; [11]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m1,        m1
    movhps      m1,        [r2 + 44]                  ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_5_31 1
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m3,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m1,        m3, m0, 2                  ; [9 8 7 6 5 4 3 2]
    punpckhwd   m2,        m0, m1                     ; [9 8 8 7 7 6 6 5]
    punpcklwd   m0,        m1                         ; [5 4 4 3 3 2 2 1]

    pmaddwd     m4,        m0, [r3 + 16]              ; [17]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    palignr     m1,        m2, m0, 4
    mova        m5,        m1
    pmaddwd     m1,        [r3 - 14 * 16]             ; [2]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        [r3 + 3 * 16]              ; [19]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m6,        m2, m0, 8
    mova        m1,        m6
    pmaddwd     m6,        [r3 - 12 * 16]             ; [4]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m1, [r3 + 5 * 16]          ; [21]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    palignr     m1,        m2, m0, 12
    mova        m7,        m1
    pmaddwd     m7,        [r3 - 10 * 16]             ; [6]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    pmaddwd     m1,        [r3 + 7 * 16]              ; [23]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m7,        m2, [r3 - 8 * 16]          ; [8]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m1,        m7

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m2, [r3 + 9 * 16]          ; [25]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    palignr     m7,        m3, m2, 4                  ; [10 9 9 8 7 6 5 4]
    pmaddwd     m1,        m7, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m7, [r3 + 11 * 16]         ; [27]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m0,        [r2 + 34]                  ; [24 23 22 21 20 19 18 17]
    palignr     m2,        m0, m3, 2                  ; [17 16 15 14 13 12 11 10]
    palignr     m1,        m0, m3, 4                  ; [18 17 16 15 14 13 12 11]
    punpckhwd   m3,        m2, m1                     ; [18 17 17 16 16 15 15 14]
    punpcklwd   m2,        m1                         ; [14 13 13 12 12 11 11 10]

    palignr     m6,        m2, m7, 4
    pmaddwd     m1,        m6, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    pmaddwd     m6,        [r3 + 13 * 16]             ; [29]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    palignr     m1,        m2, m7, 8
    mova        m0,        m1
    pmaddwd     m1,        [r3 - 2 * 16]              ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m0, [r3 + 15 * 16]         ; [31]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    palignr     m0,        m2, m7, 12
    pmaddwd     m0,        [r3]                       ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    pmaddwd     m4,        m2, [r3 - 15 * 16]         ; [1]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m2, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    palignr     m1,        m3, m2, 4
    pmaddwd     m5,        m1, [r3 - 13 * 16]         ; [3]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m1,        [r3 + 4 * 16]              ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    palignr     m1,        m3, m2, 8
    pmaddwd     m6,        m1, [r3 - 11 * 16]         ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        [r3 + 6 * 16]              ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m7,        m3, m2, 12
    pmaddwd     m1,        m7, [r3 - 9 * 16]          ; [7]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m7,        [r3 + 8 * 16]              ; [24]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m1,        m7

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 7 * 16]          ; [9]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m0,        [r2 + 36]                  ; [25 24 23 22 21 20 19 18]
    palignr     m1,        m0, 2                      ; [x 25 24 23 22 21 20 19]
    punpcklwd   m0,        m1                         ; [22 21 21 20 20 19 19 18]

    palignr     m1,        m0, m3, 4
    pmaddwd     m5,        m1, [r3 - 5 * 16]          ; [11]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m1,        [r3 + 12 * 16]             ; [28]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    palignr     m1,        m0, m3, 8
    pmaddwd     m6,        m1, [r3 - 3 * 16]          ; [13]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        [r3 + 14 * 16]             ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m1,        m0, m3, 12
    pmaddwd     m1,        [r3 - 16]                  ; [15]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m1,        m1
    movhps      m1,        [r2 + 36]                  ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_6_30 1
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movu        m3,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m1,        m3, m0, 2                  ; [9 8 7 6 5 4 3 2]
    punpckhwd   m2,        m0, m1                     ; [9 8 8 7 7 6 6 5]
    punpcklwd   m0,        m1                         ; [5 4 4 3 3 2 2 1]

    pmaddwd     m4,        m0, [r3 - 3 * 16]          ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m0, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    palignr     m1,        m2, m0, 4
    pmaddwd     m5,        m1, [r3 - 9 * 16]          ; [7]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m1,        [r3 + 4 * 16]              ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    palignr     m1,        m2, m0, 8
    pmaddwd     m6,        m1, [r3 - 15 * 16]         ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m7,        m1, [r3 - 2 * 16]          ; [14]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    pmaddwd     m1,        [r3 + 11 * 16]             ; [27]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    palignr     m7,        m2, m0, 12
    pmaddwd     m0,        m7, [r3 - 8 * 16]          ; [8]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m7, [r3 +  5 * 16]         ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m2, [r3 - 14 * 16]         ; [2]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m2, [r3 - 16]              ; [15]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m2, [r3 + 12 * 16]         ; [28]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    palignr     m7,        m3, m2, 4
    pmaddwd     m6,        m7, [r3 - 7 * 16]          ; [9]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m7, [r3 + 6 * 16]          ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m0,        [r2 + 34]                  ; [24 23 22 21 20 19 18 17]
    palignr     m2,        m0, m3, 2                  ; [17 16 15 14 13 12 11 10]
    palignr     m1,        m0, m3, 4                  ; [18 17 16 15 14 13 12 11]
    punpckhwd   m3,        m2, m1                     ; [18 17 17 16 16 15 15 14]
    punpcklwd   m2,        m1                         ; [14 13 13 12 12 11 11 10]

    palignr     m0,        m2, m7, 4
    pmaddwd     m1,        m0, [r3 - 13 * 16]         ; [3]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        [r3]                       ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    palignr     m4,        m2, m7, 4
    pmaddwd     m4,        [r3 +  13 * 16]            ; [29]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    palignr     m5,        m2, m7, 8
    pmaddwd     m1,        m5, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        [r3 + 7 * 16]              ; [23]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m1,        m2, m7, 12
    pmaddwd     m6,        m1, [r3 - 12 * 16]         ; [4]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m1, [r3 + 16]              ; [17]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        [r3 + 14 * 16]             ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m2, [r3 - 5 * 16]          ; [11]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m2, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    palignr     m5,        m3, m2, 4
    pmaddwd     m4,        m5, [r3 - 11 * 16]         ; [5]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m5, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        [r3 + 15 * 16]             ; [31]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m6,        m3, m2, 8
    pmaddwd     m1,        m6, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m5,        m1

    pmaddwd     m6,        [r3 + 9 * 16]              ; [25]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    palignr     m1,        m3, m2, 12
    pmaddwd     m0,        m1, [r3 - 10 * 16]         ; [6]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m6,        m0

    pmaddwd     m1,        [r3 + 3 * 16]              ; [19]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m1,        m1
    movhps      m1,        [r2 + 28]                  ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_7_29 1
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movd        m3,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m1,        m3, m0, 2                  ; [9 8 7 6 5 4 3 2]
    punpckhwd   m2,        m0, m1                     ; [9 8 8 7 7 6 6 5]
    punpcklwd   m0,        m1                         ; [5 4 4 3 3 2 2 1]

    pmaddwd     m4,        m0, [r3 - 7 * 16]          ; [9]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m0, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m0, [r3 + 11 * 16]         ; [27]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m1,        m2, m0, 4
    pmaddwd     m6,        m1, [r3 - 12 * 16]         ; [4]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m1, [r3 - 3 * 16]          ; [13]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m7,        m1, [r3 + 6 * 16]          ; [22]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m6,        m7

    pmaddwd     m1,        [r3 + 15 * 16]             ; [31]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    mova        m3,        m0
    palignr     m7,        m2, m0, 8
    pmaddwd     m0,        m7, [r3 - 8 * 16]          ; [8]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m7, [r3 + 16]              ; [17]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m7, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    palignr     m1,        m2, m3, 12
    pmaddwd     m5,        m1, [r3 - 13 * 16]         ; [3]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m1, [r3 - 4 * 16]          ; [12]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m1, [r3 + 5 * 16]          ; [21]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        [r3 + 14 * 16]             ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m2, [r3 - 9 * 16]          ; [7]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m2, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    pmaddwd     m4,        m2, [r3 + 9 * 16]          ; [25]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m7,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m1,        m7, 2                      ; [x 16 15 14 13 12 11 10]
    punpcklwd   m7,        m1                         ; [13 12 12 11 11 10 10 9]

    palignr     m6,        m7, m2, 4
    pmaddwd     m1,        m6, [r3 - 14 * 16]         ; [2]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m6, [r3 - 5 * 16]          ; [11]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m0,        m6, [r3 + 4 * 16]          ; [20]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        [r3 + 13 * 16]             ; [29]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    palignr     m0,        m7, m2, 8
    pmaddwd     m1,        m0, [r3 - 10 * 16]         ; [6]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m0, [r3 - 16]              ; [15]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        [r3 + 8 * 16]              ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    palignr     m0,        m7, m2, 12
    pmaddwd     m4,        m0, [r3 - 15 * 16]         ; [1]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m0, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m0, [r3 + 3 * 16]          ; [19]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m0,        [r3 + 12 * 16]             ; [28]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m7, [r3 - 11 * 16]         ; [5]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m0,        m7, [r3 - 2 * 16]          ; [14]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m6,        m0

    pmaddwd     m1,        m7, [r3 + 7 * 16]          ; [23]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m1,        m1
    movhps      m1,        [r2 + 20]                  ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_8_28 1
    movu        m0,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    movd        m3,        [r2 + 18]                  ; [16 15 14 13 12 11 10 9]
    palignr     m1,        m3, m0, 2                  ; [9 8 7 6 5 4 3 2]
    punpckhwd   m2,        m0, m1                     ; [9 8 8 7 7 6 6 5]
    punpcklwd   m0,        m1                         ; [5 4 4 3 3 2 2 1]

    pmaddwd     m4,        m0, [r3 - 11 * 16]         ; [5]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m0, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m0, [r3 - 16]              ; [15]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m0, [r3 + 4 * 16]          ; [20]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m0, [r3 + 9 * 16]          ; [25]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m0, [r3 + 14 * 16]         ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    palignr     m7,        m2, m0, 4
    pmaddwd     m1,        m7, [r3 - 13 * 16]         ; [3]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    mova        m3,        m0
    pmaddwd     m0,        m7, [r3 - 8 * 16]          ; [8]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m7, [r3 - 3 * 16]          ; [13]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m7, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m7, [r3 + 7 * 16]          ; [23]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m7, [r3 + 12 * 16]         ; [28]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    palignr     m7,        m2, m3, 8
    pmaddwd     m6,        m7, [r3 - 15 * 16]         ; [1]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m7, [r3 - 10 * 16]         ; [6]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m7, [r3 - 5 * 16]          ; [11]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m7, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    pmaddwd     m4,        m7, [r3 + 5 * 16]          ; [21]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m7, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m7, [r3 + 15 * 16]         ; [31]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    palignr     m7,        m2, m3, 12
    pmaddwd     m0,        m7, [r3 - 12 * 16]         ; [4]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m7, [r3 - 7 * 16]          ; [9]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m7, [r3 - 2 * 16]          ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m7, [r3 + 3 * 16]          ; [19]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m7, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    pmaddwd     m4,        m7, [r3 + 13 * 16]         ; [29]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m2, [r3 - 14 * 16]         ; [2]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m2, [r3 - 9 * 16]          ; [7]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m0,        m2, [r3 - 4 * 16]          ; [12]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m2, [r3 + 16]              ; [17]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m0,        m2, [r3 + 6 * 16]          ; [22]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m6,        m0

    pmaddwd     m1,        m2, [r3 + 11 * 16]         ; [27]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m1,        m1
    movhps      m1,        [r2 + 12]                  ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_9_27 1
    movu        m3,        [r2 + 2]                   ; [8 7 6 5 4 3 2 1]
    palignr     m1,        m3, 2                      ; [9 8 7 6 5 4 3 2]
    punpckhwd   m2,        m3, m1                     ; [9 8 8 7 7 6 6 5]
    punpcklwd   m3,        m1                         ; [5 4 4 3 3 2 2 1]

    pmaddwd     m4,        m3, [r3 - 14 * 16]         ; [2]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 12 * 16]         ; [4]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 10 * 16]         ; [6]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 8 * 16]          ; [8]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2 + 4]                   ; [00]

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    palignr     m7,        m2, m3, 4
    pmaddwd     m4,        m7, [r3 - 14 * 16]         ; [2]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m7, [r3 - 12 * 16]         ; [4]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m7, [r3 - 10 * 16]         ; [6]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m0,        m7, [r3 - 8 * 16]          ; [8]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m7, [r3 - 6 * 16]          ; [10]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m7, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m7, [r3 - 2 * 16]          ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m7, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    pmaddwd     m4,        m7, [r3 + 2 * 16]          ; [18]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m7, [r3 + 4 * 16]          ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m7, [r3 + 6 * 16]          ; [22]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m0,        m7, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m7, [r3 + 10 * 16]         ; [26]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m0,        m7, [r3 + 12 * 16]         ; [28]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m6,        m0

    pmaddwd     m7,        [r3 + 14 * 16]             ; [30]
    paddd       m7,        [pd_16]
    psrld       m7,        5
    packusdw    m7,        m7
    movhps      m7,        [r2 + 6]                   ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m7
%endmacro

%macro MODE_11_25 1
    movu        m3,        [r2 + 2]                   ; [7 6 5 4 3 2 1 0]
    pshufb      m3,        [pw_punpcklwd]             ; [4 3 3 2 2 1 1 0]

    pmaddwd     m4,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 8 * 16]          ; [8]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 10 * 16]         ; [6]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 12 * 16]         ; [4]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 14 * 16]         ; [2]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2 + 2]                   ; [00]

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    movu        m3,        [r2]                       ; [6 5 4 3 2 1 0 16]
    pshufb      m3,        [pw_punpcklwd]             ; [3 2 2 1 1 0 0 16]

    pmaddwd     m4,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m0,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 8 * 16]          ; [8]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 10 * 16]         ; [6]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 12 * 16]         ; [4]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 14 * 16]         ; [2]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2]                       ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_12_24 1
    movu        m3,        [r2 + 8]                   ; [7 6 5 4 3 2 1 0]
    pshufb      m3,        m2                         ; [4 3 3 2 2 1 1 0]

    pmaddwd     m4,        m3, [r3 + 11 * 16]         ; [27]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 + 16]              ; [17]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 9 * 16]          ; [7]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 14 * 16]         ; [2]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 6]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 13 * 16]         ; [29]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 + 3 * 16]          ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 7 * 16]          ; [9]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 12 * 16]         ; [4]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 4]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 15 * 16]         ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 + 5 * 16]          ; [21]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 5 * 16]          ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 10 * 16]         ; [6]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 15 * 16]         ; [1]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 2]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m3, [r3 + 7 * 16]          ; [23]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 3 * 16]          ; [13]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3 - 8 * 16]          ; [8]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 13 * 16]         ; [3]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 + 9 * 16]          ; [25]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 16]              ; [15]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 11 * 16]         ; [5]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2]                       ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_13_23 1
    movu        m3,        [r2 + 16]                  ; [7 6 5 4 3 2 1 0]
    pshufb      m3,        m2                         ; [4 3 3 2 2 1 1 0]

    pmaddwd     m4,        m3, [r3 + 7 * 16]          ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 11 * 16]         ; [05]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 14]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 + 3 * 16]          ; [19]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 15 * 16]         ; [01]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    movu        m3,        [r2 + 12]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 16]              ; [15]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 10 * 16]         ; [06]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m3,        [r2 + 10]
    pshufb      m3,        m2

    pmaddwd     m5,        m3, [r3 + 13 * 16]         ; [29]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 5 * 16]          ; [11]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 14 * 16]         ; [02]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 8]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 9 * 16]          ; [25]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 9 * 16]          ; [07]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 6]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 + 5 * 16]          ; [21]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m0,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m3, [r3 - 13 * 16]         ; [03]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m3,        [r2 + 4]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 + 16]              ; [17]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3 - 8 * 16]          ; [08]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 2]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 + 15 * 16]         ; [31]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 3 * 16]          ; [13]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 12 * 16]         ; [04]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 11 * 16]         ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 7 * 16]          ; [09]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2]                       ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_14_22 1
    movu        m3,        [r2 + 24]                  ; [7 6 5 4 3 2 1 0]
    pshufb      m3,        m2                         ; [4 3 3 2 2 1 1 0]

    pmaddwd     m4,        m3, [r3 + 3 * 16]          ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 10 * 16]         ; [06]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m3,        [r2 + 22]
    pshufb      m3,        m2

    pmaddwd     m5,        m3, [r3 + 9 * 16]          ; [25]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 20]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 15 * 16]         ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 11 * 16]         ; [05]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    movu        m3,        [r2 + 18]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 5 * 16]          ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 16]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 + 16]              ; [17]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 12 * 16]         ; [04]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 14]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 7 * 16]          ; [23]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 12]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 13 * 16]         ; [29]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 13 * 16]         ; [03]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 10]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 7 * 16]          ; [09]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 8]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m3, [r3 - 16]              ; [15]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 14 * 16]         ; [02]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 6]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 5 * 16]          ; [21]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3 - 8 * 16]          ; [08]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 4]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 + 11 * 16]         ; [27]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 15 * 16]         ; [01]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 2]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 9 * 16]          ; [07]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m3,        [r2]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 3 * 16]          ; [13]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2]                       ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_15_21 1
    movu        m3,        [r2 + 32]                  ; [7 6 5 4 3 2 1 0]
    pshufb      m3,        m2                         ; [4 3 3 2 2 1 1 0]

    pmaddwd     m4,        m3, [r3 - 16]              ; [15]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 30]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 3 * 16]          ; [13]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 28]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 5 * 16]          ; [11]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m3,        [r2 + 26]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 7 * 16]          ; [09]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    movu        m3,        [r2 + 24]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 9 * 16]          ; [07]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 22]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 11 * 16]         ; [05]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 20]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    pmaddwd     m6,        m3, [r3 - 13 * 16]         ; [03]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m3,        [r2 + 18]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 15 * 16]         ; [01]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    movu        m3,        [r2 + 16]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 14]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 + 15 * 16]         ; [31]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m3,        [r2 + 12]
    pshufb      m3,        m2

    pmaddwd     m5,        m3, [r3 + 13 * 16]         ; [29]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m0,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    movu        m3,        [r2 + 10]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 11 * 16]         ; [27]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 8]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 9 * 16]          ; [25]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3 - 8 * 16]          ; [08]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 6]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 + 7 * 16]          ; [23]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 10 * 16]         ; [06]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m3,        [r2 + 4]
    pshufb      m3,        m2

    pmaddwd     m5,        m3, [r3 + 5 * 16]          ; [21]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 12 * 16]         ; [04]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 2]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 3 * 16]          ; [19]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 14 * 16]         ; [02]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 16]              ; [17]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2]                       ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_16_20 1
    movu        m3,        [r2 + 40]                  ; [7 6 5 4 3 2 1 0]
    pshufb      m3,        m2                         ; [4 3 3 2 2 1 1 0]

    pmaddwd     m4,        m3, [r3 - 5 * 16]          ; [11]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 38]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 15 * 16]         ; [01]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 36]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 34]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 7 * 16]          ; [23]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 14 * 16]         ; [02]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 32]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 - 3 * 16]          ; [13]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    movu        m3,        [r2 + 30]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    pmaddwd     m4,        m3, [r3 - 13 * 16]         ; [03]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 28]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m3,        [r2 + 26]
    pshufb      m3,        m2

    pmaddwd     m5,        m3, [r3 + 9 * 16]          ; [25]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    pmaddwd     m6,        m3, [r3 - 12 * 16]         ; [04]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 24]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 - 16]              ; [15]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m3,        [r2 + 22]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    pmaddwd     m1,        m3, [r3 - 11 * 16]         ; [05]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    movu        m3,        [r2 + 20]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 18]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 + 11 * 16]         ; [27]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    pmaddwd     m1,        m3, [r3 - 10 * 16]         ; [06]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m3,        [r2 + 16]
    pshufb      m3,        m2

    pmaddwd     m5,        m3, [r3 + 16]              ; [17]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 14]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    pmaddwd     m6,        m3, [r3 - 9 * 16]          ; [07]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m3,        [r2 + 12]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 10]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 13 * 16]         ; [29]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    pmaddwd     m0,        m3, [r3 - 8 * 16]          ; [08]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 8]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 + 3 * 16]          ; [19]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 6]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 7 * 16]          ; [09]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 4]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 2]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 15 * 16]         ; [31]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 5 * 16]          ; [21]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2]                       ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

%macro MODE_17_19 1
    movu        m3,        [r2 + 50]                  ; [7 6 5 4 3 2 1 0]
    pshufb      m3,        m2                         ; [4 3 3 2 2 1 1 0]

    pmaddwd     m4,        m3, [r3 - 10 * 16]         ; [06]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 48]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m3,        [r2 + 46]
    pshufb      m3,        m2

    pmaddwd     m5,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 44]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 42]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 12 * 16]         ; [04]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 40]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    movu        m3,        [r2 + 38]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 0, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 36]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 34]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 14 * 16]         ; [02]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 32]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 - 8 * 16]          ; [08]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 30]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m3,        [r2 + 28]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 26]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2 + 26]                  ; [00]

    TRANSPOSE_STORE_8x8 16, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 24]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 - 10 * 16]         ; [06]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 22]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 - 4 * 16]          ; [12]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    movu        m3,        [r2 + 20]
    pshufb      m3,        m2

    pmaddwd     m5,        m3, [r3 + 2 * 16]          ; [18]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 18]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3 + 8 * 16]          ; [24]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m5,        m0

    movu        m3,        [r2 + 16]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 + 14 * 16]         ; [30]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    pmaddwd     m1,        m3, [r3 - 12 * 16]         ; [04]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2 + 14]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 - 6 * 16]          ; [10]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    movu        m3,        [r2 + 12]
    pshufb      m3,        m2

    pmaddwd     m0,        m3, [r3]                   ; [16]
    paddd       m0,        [pd_16]
    psrld       m0,        5
    packusdw    m1,        m0

    TRANSPOSE_STORE_8x8 32, %1, m4, m5, m6, m1

    movu        m3,        [r2 + 10]
    pshufb      m3,        m2

    pmaddwd     m4,        m3, [r3 + 6 * 16]          ; [22]
    paddd       m4,        [pd_16]
    psrld       m4,        5

    movu        m3,        [r2 + 8]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 12 * 16]         ; [28]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m4,        m1

    pmaddwd     m5,        m3, [r3 - 14 * 16]         ; [02]
    paddd       m5,        [pd_16]
    psrld       m5,        5

    movu        m3,        [r2 + 6]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 - 8 * 16]          ; [08]
    paddd       m6,        [pd_16]
    psrld       m6,        5
    packusdw    m5,        m6

    movu        m3,        [r2 + 4]
    pshufb      m3,        m2

    pmaddwd     m6,        m3, [r3 - 2 * 16]          ; [14]
    paddd       m6,        [pd_16]
    psrld       m6,        5

    movu        m3,        [r2 + 2]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 4 * 16]          ; [20]
    paddd       m1,        [pd_16]
    psrld       m1,        5
    packusdw    m6,        m1

    movu        m3,        [r2]
    pshufb      m3,        m2

    pmaddwd     m1,        m3, [r3 + 10 * 16]         ; [26]
    paddd       m1,        [pd_16]
    psrld       m1,        5

    packusdw    m1,        m1
    movhps      m1,        [r2]                       ; [00]

    TRANSPOSE_STORE_8x8 48, %1, m4, m5, m6, m1
%endmacro

;------------------------------------------------------------------------------------------
; void intraPredAng32(pixel* dst, intptr_t dstStride, pixel* src, int dirMode, int bFilter)
;------------------------------------------------------------------------------------------
INIT_XMM ssse3
cglobal intra_pred_ang32_2, 3,6,6
    lea             r4, [r2]
    add             r2, 128
    cmp             r3m, byte 34
    cmove           r2, r4

    add             r1, r1
    lea             r3, [r1 * 2]
    lea             r4, [r1 * 3]
    mov             r5, 2

.loop:
    MODE_2_34
    add             r2, 32
    dec             r5
    jnz             .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_3, 3,6,8
    add         r2, 128
    lea         r3, [ang_table + 16 * 16]
    mov         r4d, 8
    add         r1, r1
    lea         r5, [r1 * 3]

.loop:
    MODE_3_33 1
    lea         r0, [r0 + r1 * 4 ]
    add         r2, 8
    dec         r4
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_4, 3,6,8
    add         r2, 128
    lea         r3, [ang_table + 16 * 16]
    mov         r4d, 8
    add         r1, r1
    lea         r5, [r1 * 3]

.loop:
    MODE_4_32 1
    lea         r0, [r0 + r1 * 4 ]
    add         r2, 8
    dec         r4
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_5, 3,6,8
    add         r2, 128
    lea         r3, [ang_table + 16 * 16]
    mov         r4d, 8
    add         r1, r1
    lea         r5, [r1 * 3]

.loop:
    MODE_5_31 1
    lea         r0, [r0 + r1 * 4 ]
    add         r2, 8
    dec         r4
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_6, 3,6,8
    add         r2, 128
    lea         r3, [ang_table + 16 * 16]
    mov         r4d, 8
    add         r1, r1
    lea         r5, [r1 * 3]

.loop:
    MODE_6_30 1
    lea         r0, [r0 + r1 * 4 ]
    add         r2, 8
    dec         r4
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_7, 3,6,8
    add         r2, 128
    lea         r3, [ang_table + 16 * 16]
    mov         r4d, 8
    add         r1, r1
    lea         r5, [r1 * 3]

.loop:
    MODE_7_29 1
    lea         r0, [r0 + r1 * 4 ]
    add         r2, 8
    dec         r4
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_8, 3,6,8
    add         r2, 128
    lea         r3, [ang_table + 16 * 16]
    mov         r4d, 8
    add         r1, r1
    lea         r5, [r1 * 3]

.loop:
    MODE_8_28 1
    lea         r0, [r0 + r1 * 4 ]
    add         r2, 8
    dec         r4
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_9, 3,6,8
    add         r2, 128
    lea         r3, [ang_table + 16 * 16]
    mov         r4d, 8
    add         r1, r1
    lea         r5, [r1 * 3]

.loop:
    MODE_9_27 1
    lea         r0, [r0 + r1 * 4 ]
    add         r2, 8
    dec         r4
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_10, 3,7,8
    add         r2, 128
    mov         r6d, 4
    add         r1, r1
    lea         r5, [r1 * 3]
    lea         r4, [r1 * 2]
    lea         r3, [r1 * 4]
    mova        m7, [c_mode32_10_0]

.loop:
    movu        m0, [r2 + 2]
    pshufb      m1, m0, m7
    movu        [r0], m1
    movu        [r0 + 16], m1
    movu        [r0 + 32], m1
    movu        [r0 + 48], m1

    palignr     m1, m0, 2
    pshufb      m1, m7
    movu        [r0 + r1], m1
    movu        [r0 + r1 + 16], m1
    movu        [r0 + r1 + 32], m1
    movu        [r0 + r1 + 48], m1

    palignr     m1, m0, 4
    pshufb      m1, m7
    movu        [r0 + r4], m1
    movu        [r0 + r4 + 16], m1
    movu        [r0 + r4 + 32], m1
    movu        [r0 + r4 + 48], m1

    palignr     m1, m0, 6
    pshufb      m1, m7
    movu        [r0 + r5], m1
    movu        [r0 + r5 + 16], m1
    movu        [r0 + r5 + 32], m1
    movu        [r0 + r5 + 48], m1

    add         r0, r3

    palignr     m1, m0, 8
    pshufb      m1, m7
    movu        [r0], m1
    movu        [r0 + 16], m1
    movu        [r0 + 32], m1
    movu        [r0 + 48], m1

    palignr     m1, m0, 10
    pshufb      m1, m7
    movu        [r0 + r1], m1
    movu        [r0 + r1 + 16], m1
    movu        [r0 + r1 + 32], m1
    movu        [r0 + r1 + 48], m1

    palignr     m1, m0, 12
    pshufb      m1, m7
    movu        [r0 + r4], m1
    movu        [r0 + r4 + 16], m1
    movu        [r0 + r4 + 32], m1
    movu        [r0 + r4 + 48], m1

    palignr     m1, m0, 14
    pshufb      m1, m7
    movu        [r0 + r5], m1
    movu        [r0 + r5 + 16], m1
    movu        [r0 + r5 + 32], m1
    movu        [r0 + r5 + 48], m1

    add         r0, r3
    add         r2, 16
    dec         r6d
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_11, 3,6,7,0-(4*mmsize+4)
    mov      r3, r2mp
    add      r2, 128
    movu     m0, [r2 + 0*mmsize]
    pinsrw   m0, [r3], 0
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 0*mmsize + 2], m0
    movu     [rsp + 1*mmsize + 2], m1
    movu     [rsp + 2*mmsize + 2], m2
    movu     [rsp + 3*mmsize + 2], m3
    mov      r4w, [r3+32]
    mov      [rsp], r4w
    mov      r4w, [r2+64]
    mov      [rsp+66], r4w

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]

.loop:
    MODE_11_25 1
    lea      r0, [r0 + r1 * 4 ]
    add      r2, 8
    dec      r4
    jnz      .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_12, 3,6,7,0-(4*mmsize+10)
    mov      r3, r2mp
    add      r2, 128
    movu     m0, [r2 + 0*mmsize]
    pinsrw   m0, [r3], 0
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 0*mmsize + 8], m0
    movu     [rsp + 1*mmsize + 8], m1
    movu     [rsp + 2*mmsize + 8], m2
    movu     [rsp + 3*mmsize + 8], m3

    mov      r4w, [r2+64]
    mov      [rsp+72], r4w
    mov      r4w, [r3+12]
    mov      [rsp+6], r4w
    mov      r4w, [r3+26]
    mov      [rsp+4], r4w
    mov      r4w, [r3+38]
    mov      [rsp+2], r4w
    mov      r4w, [r3+52]
    mov      [rsp], r4w

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]

.loop:
    MODE_12_24 1
    lea      r0, [r0 + r1 * 4 ]
    add      r2, 8
    dec      r4
    jnz      .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_13, 3,6,7,0-(5*mmsize+2)
    mov      r3, r2mp
    add      r2, 128
    movu     m0, [r2 + 0*mmsize]
    pinsrw   m0, [r3], 0
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 1*mmsize], m0
    movu     [rsp + 2*mmsize], m1
    movu     [rsp + 3*mmsize], m2
    movu     [rsp + 4*mmsize], m3

    mov      r4w, [r2+64]
    mov      [rsp+80], r4w
    movu     m0, [r3 + 8]
    movu     m1, [r3 + 36]
    pshufb   m0, [shuf_mode_13_23]
    pshufb   m1, [shuf_mode_13_23]
    movh     [rsp + 8], m0
    movh     [rsp], m1
    mov      r4w, [r3+28]
    mov      [rsp+8], r4w
    mov      r4w, [r3+56]
    mov      [rsp], r4w

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]

.loop:
    MODE_13_23 1
    lea      r0, [r0 + r1 * 4 ]
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_14, 3,6,7,0-(5*mmsize+10)
    mov      r3, r2mp
    add      r2, 128
    movu     m0, [r2 + 0*mmsize]
    pinsrw   m0, [r3], 0
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 1*mmsize + 8], m0
    movu     [rsp + 2*mmsize + 8], m1
    movu     [rsp + 3*mmsize + 8], m2
    movu     [rsp + 4*mmsize + 8], m3

    mov      r4w, [r2 + 64]
    mov      [rsp + 88], r4w
    mov      r4w, [r3+4]
    mov      [rsp+22], r4w
    movu     m0, [r3 + 10]
    movu     m1, [r3 + 30]
    movu     m2, [r3 + 50]
    pshufb   m0, [shuf_mode_14_22]
    pshufb   m1, [shuf_mode_14_22]
    pshufb   m2, [shuf_mode_14_22]
    movh     [rsp + 14], m0
    movh     [rsp + 6], m1
    movh     [rsp - 2], m2

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]

.loop:
    MODE_14_22 1
    lea      r0, [r0 + r1 * 4 ]
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_15, 3,6,7,0-(6*mmsize+2)
    mov      r3, r2mp
    add      r2, 128
    movu     m0, [r2 + 0*mmsize]
    pinsrw   m0, [r3], 0
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 2*mmsize], m0
    movu     [rsp + 3*mmsize], m1
    movu     [rsp + 4*mmsize], m2
    movu     [rsp + 5*mmsize], m3

    mov      r4w, [r2 + 64]
    mov      [rsp + 96], r4w
    movu     m0, [r3 + 4]
    movu     m1, [r3 + 18]
    movu     m2, [r3 + 34]
    movu     m3, [r3 + 48]
    pshufb   m0, [shuf_mode_15_21]
    pshufb   m1, [shuf_mode_15_21]
    pshufb   m2, [shuf_mode_15_21]
    pshufb   m3, [shuf_mode_15_21]
    movh     [rsp + 24], m0
    movh     [rsp + 16], m1
    movh     [rsp + 8], m2
    movh     [rsp], m3

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]

.loop:
    MODE_15_21 1
    lea      r0, [r0 + r1 * 4 ]
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_16, 3,6,7,0-(6*mmsize+10)
    mov      r3, r2mp
    add      r2, 128
    movu     m0, [r2 + 0*mmsize]
    pinsrw   m0, [r3], 0
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 2*mmsize + 8], m0
    movu     [rsp + 3*mmsize + 8], m1
    movu     [rsp + 4*mmsize + 8], m2
    movu     [rsp + 5*mmsize + 8], m3

    mov      r4w, [r2 + 64]
    mov      [rsp + 104], r4w
    movu     m0, [r3 + 4]
    movu     m1, [r3 + 22]
    movu     m2, [r3 + 40]
    movd     m3, [r3 + 58]
    pshufb   m0, [shuf_mode_16_20]
    pshufb   m1, [shuf_mode_16_20]
    pshufb   m2, [shuf_mode_16_20]
    pshufb   m3, [shuf_mode_16_20]
    movu     [rsp + 24], m0
    movu     [rsp + 12], m1
    movu     [rsp], m2
    movd     [rsp], m3

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]

.loop:
    MODE_16_20 1
    lea      r0, [r0 + r1 * 4 ]
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_17, 3,6,7,0-(7*mmsize+4)
    mov      r3, r2mp
    add      r2, 128
    movu     m0, [r2 + 0*mmsize]
    pinsrw   m0, [r3], 0
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 3*mmsize + 2], m0
    movu     [rsp + 4*mmsize + 2], m1
    movu     [rsp + 5*mmsize + 2], m2
    movu     [rsp + 6*mmsize + 2], m3

    mov      r4w, [r2 + 64]
    mov      [rsp + 114], r4w
    movu     m0, [r3 + 8]
    movu     m1, [r3 + 30]
    movu     m2, [r3 + 50]
    movd     m3, [r3 + 2]
    pshufb   m0, [shuf_mode_17_19]
    pshufb   m1, [shuf_mode_17_19]
    pshufb   m2, [shuf_mode_17_19]
    pshufb   m3, [shuf_mode_16_20]
    movd     [rsp + 46], m3
    movu     [rsp + 30], m0
    movu     [rsp + 12], m1
    movu     [rsp - 4], m2
    mov      r4w, [r3 + 24]
    mov      [rsp + 30], r4w
    mov      r4w, [r3 + 28]
    mov      [rsp + 28], r4w
    mov      r4w, [r3 + 46]
    mov      [rsp + 12], r4w

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]

.loop:
    MODE_17_19 1
    lea      r0, [r0 + r1 * 4 ]
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_18, 3,7,8
    mov      r3, r2mp
    add      r2, 128
    movu        m0, [r3]               ; [7 6 5 4 3 2 1 0]
    movu        m1, [r3 + 16]          ; [15 14 13 12 11 10 9 8]
    movu        m2, [r3 + 32]          ; [23 22 21 20 19 18 17 16]
    movu        m3, [r3 + 48]          ; [31 30 29 28 27 26 25 24]
    movu        m4, [r2 + 2]           ; [8 7 6 5 4 3 2 1]
    movu        m5, [r2 + 18]          ; [16 15 14 13 12 11 10 9]

    add         r1, r1
    lea         r6, [r1 * 2]
    lea         r3, [r1 * 3]
    lea         r4, [r1 * 4]

    movu        [r0], m0
    movu        [r0 + 16], m1
    movu        [r0 + 32], m2
    movu        [r0 + 48], m3

    pshufb      m4, [shuf_mode32_18]   ; [1 2 3 4 5 6 7 8]
    pshufb      m5, [shuf_mode32_18]   ; [9 10 11 12 13 14 15 16]

    palignr     m6, m0, m4, 14
    movu        [r0 + r1], m6
    palignr     m6, m1, m0, 14
    movu        [r0 + r1 + 16], m6
    palignr     m6, m2, m1, 14
    movu        [r0 + r1 + 32], m6
    palignr     m6, m3, m2, 14
    movu        [r0 + r1 + 48], m6

    palignr     m6, m0, m4, 12
    movu        [r0 + r6], m6
    palignr     m6, m1, m0, 12
    movu        [r0 + r6 + 16], m6
    palignr     m6, m2, m1, 12
    movu        [r0 + r6 + 32], m6
    palignr     m6, m3, m2, 12
    movu        [r0 + r6 + 48], m6

    palignr     m6, m0, m4, 10
    movu        [r0 + r3], m6
    palignr     m6, m1, m0, 10
    movu        [r0 + r3 + 16], m6
    palignr     m6, m2, m1, 10
    movu        [r0 + r3 + 32], m6
    palignr     m6, m3, m2, 10
    movu        [r0 + r3 + 48], m6

    add         r0, r4

    palignr     m6, m0, m4, 8
    movu        [r0], m6
    palignr     m6, m1, m0, 8
    movu        [r0 + 16], m6
    palignr     m6, m2, m1, 8
    movu        [r0 + 32], m6
    palignr     m6, m3, m2, 8
    movu        [r0 + 48], m6

    palignr     m6, m0, m4, 6
    movu        [r0 + r1], m6
    palignr     m6, m1, m0, 6
    movu        [r0 + r1 + 16], m6
    palignr     m6, m2, m1, 6
    movu        [r0 + r1 + 32], m6
    palignr     m6, m3, m2, 6
    movu        [r0 + r1 + 48], m6

    palignr     m6, m0, m4, 4
    movu        [r0 + r6], m6
    palignr     m6, m1, m0, 4
    movu        [r0 + r6 + 16], m6
    palignr     m6, m2, m1, 4
    movu        [r0 + r6 + 32], m6
    palignr     m6, m3, m2, 4
    movu        [r0 + r6 + 48], m6

    palignr     m6, m0, m4, 2
    movu        [r0 + r3], m6
    palignr     m6, m1, m0, 2
    movu        [r0 + r3 + 16], m6
    palignr     m6, m2, m1, 2
    movu        [r0 + r3 + 32], m6
    palignr     m6, m3, m2, 2
    movu        [r0 + r3 + 48], m6

    add         r0, r4

    movu        [r0], m4
    movu        [r0 + 16], m0
    movu        [r0 + 32], m1
    movu        [r0 + 48], m2

    palignr     m6, m4, m5, 14
    movu        [r0 + r1], m6
    palignr     m6, m0, m4, 14
    movu        [r0 + r1 + 16], m6
    palignr     m6, m1, m0, 14
    movu        [r0 + r1 + 32], m6
    palignr     m6, m2, m1, 14
    movu        [r0 + r1 + 48], m6

    palignr     m6, m4, m5, 12
    movu        [r0 + r6], m6
    palignr     m6, m0, m4, 12
    movu        [r0 + r6 + 16], m6
    palignr     m6, m1, m0, 12
    movu        [r0 + r6 + 32], m6
    palignr     m6, m2, m1, 12
    movu        [r0 + r6 + 48], m6

    palignr     m6, m4, m5, 10
    movu        [r0 + r3], m6
    palignr     m6, m0, m4, 10
    movu        [r0 + r3 + 16], m6
    palignr     m6, m1, m0, 10
    movu        [r0 + r3 + 32], m6
    palignr     m6, m2, m1, 10
    movu        [r0 + r3 + 48], m6

    add         r0, r4

    palignr     m6, m4, m5, 8
    movu        [r0], m6
    palignr     m6, m0, m4, 8
    movu        [r0 + 16], m6
    palignr     m6, m1, m0, 8
    movu        [r0 + 32], m6
    palignr     m6, m2, m1, 8
    movu        [r0 + 48], m6

    palignr     m6, m4, m5, 6
    movu        [r0 + r1], m6
    palignr     m6, m0, m4, 6
    movu        [r0 + r1 + 16], m6
    palignr     m6, m1, m0, 6
    movu        [r0 + r1 + 32], m6
    palignr     m6, m2, m1, 6
    movu        [r0 + r1 + 48], m6

    palignr     m6, m4, m5, 4
    movu        [r0 + r6], m6
    palignr     m6, m0, m4, 4
    movu        [r0 + r6 + 16], m6
    palignr     m6, m1, m0, 4
    movu        [r0 + r6 + 32], m6
    palignr     m6, m2, m1, 4
    movu        [r0 + r6 + 48], m6

    palignr     m6, m4, m5, 2
    movu        [r0 + r3], m6
    palignr     m6, m0, m4, 2
    movu        [r0 + r3 + 16], m6
    palignr     m6, m1, m0, 2
    movu        [r0 + r3 + 32], m6
    palignr     m6, m2, m1, 2
    movu        [r0 + r3 + 48], m6

    add         r0, r4

    movu        m2, [r2 + 34]
    movu        m3, [r2 + 50]
    pshufb      m2, [shuf_mode32_18]
    pshufb      m3, [shuf_mode32_18]

    movu        [r0], m5
    movu        [r0 + 16], m4
    movu        [r0 + 32], m0
    movu        [r0 + 48], m1

    palignr     m6, m5, m2, 14
    movu        [r0 + r1], m6
    palignr     m6, m4, m5, 14
    movu        [r0 + r1 + 16], m6
    palignr     m6, m0, m4, 14
    movu        [r0 + r1 + 32], m6
    palignr     m6, m1, m0, 14
    movu        [r0 + r1 + 48], m6

    palignr     m6, m5, m2, 12
    movu        [r0 + r6], m6
    palignr     m6, m4, m5, 12
    movu        [r0 + r6 + 16], m6
    palignr     m6, m0, m4, 12
    movu        [r0 + r6 + 32], m6
    palignr     m6, m1, m0, 12
    movu        [r0 + r6 + 48], m6

    palignr     m6, m5, m2, 10
    movu        [r0 + r3], m6
    palignr     m6, m4, m5, 10
    movu        [r0 + r3 + 16], m6
    palignr     m6, m0, m4, 10
    movu        [r0 + r3 + 32], m6
    palignr     m6, m1, m0, 10
    movu        [r0 + r3 + 48], m6

    add         r0, r4

    palignr     m6, m5, m2, 8
    movu        [r0], m6
    palignr     m6, m4, m5, 8
    movu        [r0 + 16], m6
    palignr     m6, m0, m4, 8
    movu        [r0 + 32], m6
    palignr     m6, m1, m0, 8
    movu        [r0 + 48], m6

    palignr     m6, m5, m2, 6
    movu        [r0 + r1], m6
    palignr     m6, m4, m5, 6
    movu        [r0 + r1 + 16], m6
    palignr     m6, m0, m4, 6
    movu        [r0 + r1 + 32], m6
    palignr     m6, m1, m0, 6
    movu        [r0 + r1 + 48], m6

    palignr     m6, m5, m2, 4
    movu        [r0 + r6], m6
    palignr     m6, m4, m5, 4
    movu        [r0 + r6 + 16], m6
    palignr     m6, m0, m4, 4
    movu        [r0 + r6 + 32], m6
    palignr     m6, m1, m0, 4
    movu        [r0 + r6 + 48], m6

    palignr     m6, m5, m2, 2
    movu        [r0 + r3], m6
    palignr     m6, m4, m5, 2
    movu        [r0 + r3 + 16], m6
    palignr     m6, m0, m4, 2
    movu        [r0 + r3 + 32], m6
    palignr     m6, m1, m0, 2
    movu        [r0 + r3 + 48], m6

    add         r0, r4

    movu        [r0], m2
    movu        [r0 + 16], m5
    movu        [r0 + 32], m4
    movu        [r0 + 48], m0

    palignr     m6, m2, m3, 14
    movu        [r0 + r1], m6
    palignr     m6, m5, m2, 14
    movu        [r0 + r1 + 16], m6
    palignr     m6, m4, m5, 14
    movu        [r0 + r1 + 32], m6
    palignr     m6, m0, m4, 14
    movu        [r0 + r1 + 48], m6

    palignr     m6, m2, m3, 12
    movu        [r0 + r6], m6
    palignr     m6, m5, m2, 12
    movu        [r0 + r6 + 16], m6
    palignr     m6, m4, m5, 12
    movu        [r0 + r6 + 32], m6
    palignr     m6, m0, m4, 12
    movu        [r0 + r6 + 48], m6

    palignr     m6, m2, m3, 10
    movu        [r0 + r3], m6
    palignr     m6, m5, m2, 10
    movu        [r0 + r3 + 16], m6
    palignr     m6, m4, m5, 10
    movu        [r0 + r3 + 32], m6
    palignr     m6, m0, m4, 10
    movu        [r0 + r3 + 48], m6

    add         r0, r4

    palignr     m6, m2, m3, 8
    movu        [r0], m6
    palignr     m6, m5, m2, 8
    movu        [r0 + 16], m6
    palignr     m6, m4, m5, 8
    movu        [r0 + 32], m6
    palignr     m6, m0, m4, 8
    movu        [r0 + 48], m6

    palignr     m6, m2, m3, 6
    movu        [r0 + r1], m6
    palignr     m6, m5, m2, 6
    movu        [r0 + r1 + 16], m6
    palignr     m6, m4, m5, 6
    movu        [r0 + r1 + 32], m6
    palignr     m6, m0, m4, 6
    movu        [r0 + r1 + 48], m6

    palignr     m6, m2, m3, 4
    movu        [r0 + r6], m6
    palignr     m6, m5, m2, 4
    movu        [r0 + r6 + 16], m6
    palignr     m6, m4, m5, 4
    movu        [r0 + r6 + 32], m6
    palignr     m6, m0, m4, 4
    movu        [r0 + r6 + 48], m6

    palignr     m6, m2, m3, 2
    movu        [r0 + r3], m6
    palignr     m6, m5, m2, 2
    movu        [r0 + r3 + 16], m6
    palignr     m6, m4, m5, 2
    movu        [r0 + r3 + 32], m6
    palignr     m6, m0, m4, 2
    movu        [r0 + r3 + 48], m6
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_19, 3,7,7,0-(7*mmsize+4)
    lea      r3, [r2 + 128]
    movu     m0, [r2 + 0*mmsize]
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 3*mmsize + 2], m0
    movu     [rsp + 4*mmsize + 2], m1
    movu     [rsp + 5*mmsize + 2], m2
    movu     [rsp + 6*mmsize + 2], m3

    mov      r4w, [r2 + 64]
    mov      [rsp + 114], r4w
    movu     m0, [r3 + 8]
    movu     m1, [r3 + 30]
    movu     m2, [r3 + 50]
    movd     m3, [r3 + 2]
    pshufb   m0, [shuf_mode_17_19]
    pshufb   m1, [shuf_mode_17_19]
    pshufb   m2, [shuf_mode_17_19]
    pshufb   m3, [shuf_mode_16_20]
    movd     [rsp + 46], m3
    movu     [rsp + 30], m0
    movu     [rsp + 12], m1
    movu     [rsp - 4], m2
    mov      r4w, [r3 + 24]
    mov      [rsp + 30], r4w
    mov      r4w, [r3 + 28]
    mov      [rsp + 28], r4w
    mov      r4w, [r3 + 46]
    mov      [rsp + 12], r4w

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]
    mov      r6, r0

.loop:
    MODE_17_19 0
    add      r6, 8
    mov      r0, r6
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_20, 3,7,7,0-(6*mmsize+10)
    lea      r3, [r2 + 128]
    movu     m0, [r2 + 0*mmsize]
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 2*mmsize + 8], m0
    movu     [rsp + 3*mmsize + 8], m1
    movu     [rsp + 4*mmsize + 8], m2
    movu     [rsp + 5*mmsize + 8], m3

    mov      r4w, [r2 + 64]
    mov      [rsp + 104], r4w
    movu     m0, [r3 + 4]
    movu     m1, [r3 + 22]
    movu     m2, [r3 + 40]
    movd     m3, [r3 + 58]
    pshufb   m0, [shuf_mode_16_20]
    pshufb   m1, [shuf_mode_16_20]
    pshufb   m2, [shuf_mode_16_20]
    pshufb   m3, [shuf_mode_16_20]
    movu     [rsp + 24], m0
    movu     [rsp + 12], m1
    movu     [rsp], m2
    movd     [rsp], m3

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]
    mov      r6, r0

.loop:
    MODE_16_20 0
    add      r6, 8
    mov      r0, r6
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_21, 3,7,7,0-(6*mmsize+2)
    lea      r3, [r2 + 128]
    movu     m0, [r2 + 0*mmsize]
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 2*mmsize], m0
    movu     [rsp + 3*mmsize], m1
    movu     [rsp + 4*mmsize], m2
    movu     [rsp + 5*mmsize], m3

    mov      r4w, [r2 + 64]
    mov      [rsp + 96], r4w
    movu     m0, [r3 + 4]
    movu     m1, [r3 + 18]
    movu     m2, [r3 + 34]
    movu     m3, [r3 + 48]
    pshufb   m0, [shuf_mode_15_21]
    pshufb   m1, [shuf_mode_15_21]
    pshufb   m2, [shuf_mode_15_21]
    pshufb   m3, [shuf_mode_15_21]
    movh     [rsp + 24], m0
    movh     [rsp + 16], m1
    movh     [rsp + 8], m2
    movh     [rsp], m3

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]
    mov      r6, r0

.loop:
    MODE_15_21 0
    add      r6, 8
    mov      r0, r6
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_22, 3,7,7,0-(5*mmsize+10)
    lea      r3, [r2 + 128]
    movu     m0, [r2 + 0*mmsize]
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 1*mmsize + 8], m0
    movu     [rsp + 2*mmsize + 8], m1
    movu     [rsp + 3*mmsize + 8], m2
    movu     [rsp + 4*mmsize + 8], m3

    mov      r4w, [r2 + 64]
    mov      [rsp + 88], r4w
    mov      r4w, [r3+4]
    mov      [rsp+22], r4w
    movu     m0, [r3 + 10]
    movu     m1, [r3 + 30]
    movu     m2, [r3 + 50]
    pshufb   m0, [shuf_mode_14_22]
    pshufb   m1, [shuf_mode_14_22]
    pshufb   m2, [shuf_mode_14_22]
    movh     [rsp + 14], m0
    movh     [rsp + 6], m1
    movh     [rsp - 2], m2

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]
    mov      r6, r0

.loop:
    MODE_14_22 0
    add      r6, 8
    mov      r0, r6
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_23, 3,7,7,0-(5*mmsize+2)
    lea      r3, [r2 + 128]
    movu     m0, [r2 + 0*mmsize]
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 1*mmsize], m0
    movu     [rsp + 2*mmsize], m1
    movu     [rsp + 3*mmsize], m2
    movu     [rsp + 4*mmsize], m3

    mov      r4w, [r2+64]
    mov      [rsp+80], r4w
    movu     m0, [r3 + 8]
    movu     m1, [r3 + 36]
    pshufb   m0, [shuf_mode_13_23]
    pshufb   m1, [shuf_mode_13_23]
    movh     [rsp + 8], m0
    movh     [rsp], m1
    mov      r4w, [r3+28]
    mov      [rsp+8], r4w
    mov      r4w, [r3+56]
    mov      [rsp], r4w

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mova     m2, [pw_punpcklwd]
    mov      r6, r0

.loop:
    MODE_13_23 0
    add      r6, 8
    mov      r0, r6
    add      r2, 8
    dec      r4
    jnz     .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_24, 3,7,7,0-(4*mmsize+10)
    lea      r3, [r2 + 128]
    movu     m0, [r2 + 0*mmsize]
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]

    movu     [rsp + 0*mmsize + 8], m0
    movu     [rsp + 1*mmsize + 8], m1
    movu     [rsp + 2*mmsize + 8], m2
    movu     [rsp + 3*mmsize + 8], m3

    mov      r4w, [r2+64]
    mov      [rsp+72], r4w
    mov      r4w, [r3+12]
    mov      [rsp+6], r4w
    mov      r4w, [r3+26]
    mov      [rsp+4], r4w
    mov      r4w, [r3+38]
    mov      [rsp+2], r4w
    mov      r4w, [r3+52]
    mov      [rsp], r4w

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mov     r6, r0
    mova     m2, [pw_punpcklwd]

.loop:
    MODE_12_24 0
    add      r6, 8
    mov      r0, r6
    add      r2, 8
    dec      r4
    jnz      .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_25, 3,7,7,0-(4*mmsize+4)
    lea      r3, [r2 + 128]
    movu     m0, [r2 + 0*mmsize]
    movu     m1, [r2 + 1*mmsize]
    movu     m2, [r2 + 2*mmsize]
    movu     m3, [r2 + 3*mmsize]
    movu     [rsp + 0*mmsize + 2], m0
    movu     [rsp + 1*mmsize + 2], m1
    movu     [rsp + 2*mmsize + 2], m2
    movu     [rsp + 3*mmsize + 2], m3
    mov      r4w, [r3+32]
    mov      [rsp], r4w
    mov      r4w, [r2+64]
    mov      [rsp+66], r4w

    lea      r3, [ang_table + 16 * 16]
    mov      r4d, 8
    mov      r2, rsp
    add      r1, r1
    lea      r5, [r1 * 3]
    mov      r6, r0

.loop:
    MODE_11_25 0
    add      r6, 8
    mov      r0, r6
    add      r2, 8
    dec      r4
    jnz      .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_26, 3,7,5
    mov         r6d, 4
    add         r1, r1
    lea         r3, [r1 * 2]
    lea         r4, [r1 * 3]
    lea         r5, [r1 * 4]
    mova        m4, [c_mode32_10_0]

    movu        m0, [r2 + 2 ]
    movu        m1, [r2 + 18]
    movu        m2, [r2 + 34]
    movu        m3, [r2 + 50]

.loop:
    movu        [r0], m0
    movu        [r0 + 16], m1
    movu        [r0 + 32], m2
    movu        [r0 + 48], m3

    movu        [r0 + r1], m0
    movu        [r0 + r1 + 16], m1
    movu        [r0 + r1 + 32], m2
    movu        [r0 + r1 + 48], m3

    movu        [r0 + r3], m0
    movu        [r0 + r3 + 16], m1
    movu        [r0 + r3 + 32], m2
    movu        [r0 + r3 + 48], m3

    movu        [r0 + r4], m0
    movu        [r0 + r4 + 16], m1
    movu        [r0 + r4 + 32], m2
    movu        [r0 + r4 + 48], m3

    add         r0, r5

    movu        [r0], m0
    movu        [r0 + 16], m1
    movu        [r0 + 32], m2
    movu        [r0 + 48], m3

    movu        [r0 + r1], m0
    movu        [r0 + r1 + 16], m1
    movu        [r0 + r1 + 32], m2
    movu        [r0 + r1 + 48], m3

    movu        [r0 + r3], m0
    movu        [r0 + r3 + 16], m1
    movu        [r0 + r3 + 32], m2
    movu        [r0 + r3 + 48], m3

    movu        [r0 + r4], m0
    movu        [r0 + r4 + 16], m1
    movu        [r0 + r4 + 32], m2
    movu        [r0 + r4 + 48], m3

    add         r0, r5
    dec         r6d
    jnz         .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_27, 3,7,8
    lea    r3, [ang_table + 16 * 16]
    add    r1, r1
    lea    r5, [r1 * 3]
    mov    r6, r0
    mov    r4d, 8

.loop:
    MODE_9_27 0
    add    r6, 8
    mov    r0, r6
    add    r2, 8
    dec    r4
    jnz    .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_28, 3,7,8
    lea    r3, [ang_table + 16 * 16]
    add    r1, r1
    lea    r5, [r1 * 3]
    mov    r6, r0
    mov    r4d, 8

.loop:
    MODE_8_28 0
    add    r6, 8
    mov    r0, r6
    add    r2, 8
    dec    r4
    jnz    .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_29, 3,7,8
    lea    r3, [ang_table + 16 * 16]
    add    r1, r1
    lea    r5, [r1 * 3]
    mov    r6, r0
    mov    r4d, 8

.loop:
    MODE_7_29 0
    add    r6, 8
    mov    r0, r6
    add    r2, 8
    dec    r4
    jnz    .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_30, 3,7,8
    lea    r3, [ang_table + 16 * 16]
    add    r1, r1
    lea    r5, [r1 * 3]
    mov    r6, r0
    mov    r4d, 8

.loop:
    MODE_6_30 0
    add    r6, 8
    mov    r0, r6
    add    r2, 8
    dec    r4
    jnz    .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_31, 3,7,8
    lea    r3, [ang_table + 16 * 16]
    add    r1, r1
    lea    r5, [r1 * 3]
    mov    r6, r0
    mov    r4d, 8

.loop:
    MODE_5_31 0
    add    r6, 8
    mov    r0, r6
    add    r2, 8
    dec    r4
    jnz    .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_32, 3,7,8
    lea    r3, [ang_table + 16 * 16]
    add    r1, r1
    lea    r5, [r1 * 3]
    mov    r6, r0
    mov    r4d, 8

.loop:
    MODE_4_32 0
    add    r6, 8
    mov    r0, r6
    add    r2, 8
    dec    r4
    jnz    .loop
    RET

INIT_XMM sse4
cglobal intra_pred_ang32_33, 3,7,8
    lea    r3, [ang_table + 16 * 16]
    add    r1, r1
    lea    r5, [r1 * 3]
    mov    r6, r0
    mov    r4d, 8
.loop:
    MODE_3_33 0
    add    r6, 8
    mov    r0, r6
    add    r2, 8
    dec    r4
    jnz    .loop
    RET

;-----------------------------------------------------------------------------------
; void intra_filter_NxN(const pixel* references, pixel* filtered)
;-----------------------------------------------------------------------------------
INIT_XMM sse4
cglobal intra_filter_4x4, 2,4,5
    mov             r2w, word [r0 + 16]             ; topLast
    mov             r3w, word [r0 + 32]             ; LeftLast

    ; filtering top
    movu            m0, [r0 +  0]
    movu            m1, [r0 + 16]
    movu            m2, [r0 + 32]

    pshufb          m4, m0, [intra_filter4_shuf0]   ; [6 5 4 3 2 1 0 1] samples[i - 1]
    palignr         m3, m1, m0, 4
    pshufb          m3, [intra_filter4_shuf1]       ; [8 7 6 5 4 3 2 9] samples[i + 1]

    psllw           m0, 1
    paddw           m4, m3
    paddw           m0, m4
    paddw           m0, [pw_2]
    psrlw           m0, 2

    ; filtering left
    palignr         m4, m1, m1, 14
    pinsrw          m4, [r0], 1
    palignr         m3, m2, m1, 4
    pshufb          m3, [intra_filter4_shuf1]

    psllw           m1, 1
    paddw           m4, m3
    paddw           m1, m4
    paddw           m1, [pw_2]
    psrlw           m1, 2

    movu            [r1], m0
    movu            [r1 + 16], m1
    mov             [r1 + 16], r2w                  ; topLast
    mov             [r1 + 32], r3w                  ; LeftLast
    RET

INIT_XMM sse4
cglobal intra_filter_8x8, 2,4,6
    mov             r2w, word [r0 + 32]             ; topLast
    mov             r3w, word [r0 + 64]             ; LeftLast

    ; filtering top
    movu            m0, [r0]
    movu            m1, [r0 + 16]
    movu            m2, [r0 + 32]

    pshufb          m4, m0, [intra_filter4_shuf0]
    palignr         m5, m1, m0, 2
    pinsrw          m5, [r0 + 34], 0

    palignr         m3, m1, m0, 14
    psllw           m0, 1
    paddw           m4, m5
    paddw           m0, m4
    paddw           m0, [pw_2]
    psrlw           m0, 2

    palignr         m4, m2, m1, 2
    psllw           m1, 1
    paddw           m4, m3
    paddw           m1, m4
    paddw           m1, [pw_2]
    psrlw           m1, 2
    movu            [r1], m0
    movu            [r1 + 16], m1

    ; filtering left
    movu            m1, [r0 + 48]
    movu            m0, [r0 + 64]

    palignr         m4, m2, m2, 14
    pinsrw          m4, [r0], 1
    palignr         m5, m1, m2, 2

    palignr         m3, m1, m2, 14
    palignr         m0, m1, 2

    psllw           m2, 1
    paddw           m4, m5
    paddw           m2, m4
    paddw           m2, [pw_2]
    psrlw           m2, 2

    psllw           m1, 1
    paddw           m0, m3
    paddw           m1, m0
    paddw           m1, [pw_2]
    psrlw           m1, 2

    movu            [r1 + 32], m2
    movu            [r1 + 48], m1
    mov             [r1 + 32], r2w                  ; topLast
    mov             [r1 + 64], r3w                  ; LeftLast
    RET

INIT_XMM sse4
cglobal intra_filter_16x16, 2,4,6
    mov             r2w, word [r0 +  64]            ; topLast
    mov             r3w, word [r0 + 128]            ; LeftLast

    ; filtering top
    movu            m0, [r0]
    movu            m1, [r0 + 16]
    movu            m2, [r0 + 32]

    pshufb          m4, m0, [intra_filter4_shuf0]
    palignr         m5, m1, m0, 2
    pinsrw          m5, [r0 + 66], 0

    palignr         m3, m1, m0, 14
    psllw           m0, 1
    paddw           m4, m5
    paddw           m0, m4
    paddw           m0, [pw_2]
    psrlw           m0, 2

    palignr         m4, m2, m1, 2
    psllw           m5, m1, 1
    paddw           m4, m3
    paddw           m5, m4
    paddw           m5, [pw_2]
    psrlw           m5, 2
    movu            [r1], m0
    movu            [r1 + 16], m5

    movu            m0, [r0 + 48]
    movu            m5, [r0 + 64]

    palignr         m3, m2, m1, 14
    palignr         m4, m0, m2, 2

    psllw           m1, m2, 1
    paddw           m3, m4
    paddw           m1, m3
    paddw           m1, [pw_2]
    psrlw           m1, 2

    palignr         m3, m0, m2, 14
    palignr         m4, m5, m0, 2

    psllw           m0, 1
    paddw           m4, m3
    paddw           m0, m4
    paddw           m0, [pw_2]
    psrlw           m0, 2
    movu            [r1 + 32], m1
    movu            [r1 + 48], m0

    ; filtering left
    movu            m1, [r0 + 80]
    movu            m2, [r0 + 96]

    palignr         m4, m5, m5, 14
    pinsrw          m4, [r0], 1
    palignr         m0, m1, m5, 2

    psllw           m3, m5, 1
    paddw           m4, m0
    paddw           m3, m4
    paddw           m3, [pw_2]
    psrlw           m3, 2

    palignr         m0, m1, m5, 14
    palignr         m4, m2, m1, 2

    psllw           m5, m1, 1
    paddw           m4, m0
    paddw           m5, m4
    paddw           m5, [pw_2]
    psrlw           m5, 2
    movu            [r1 + 64], m3
    movu            [r1 + 80], m5

    movu            m5, [r0 + 112]
    movu            m0, [r0 + 128]

    palignr         m3, m2, m1, 14
    palignr         m4, m5, m2, 2

    psllw           m1, m2, 1
    paddw           m3, m4
    paddw           m1, m3
    paddw           m1, [pw_2]
    psrlw           m1, 2

    palignr         m3, m5, m2, 14
    palignr         m4, m0, m5, 2

    psllw           m5, 1
    paddw           m4, m3
    paddw           m5, m4
    paddw           m5, [pw_2]
    psrlw           m5, 2
    movu            [r1 +  96], m1
    movu            [r1 + 112], m5

    mov             [r1 +  64], r2w                 ; topLast
    mov             [r1 + 128], r3w                 ; LeftLast
    RET

INIT_XMM sse4
cglobal intra_filter_32x32, 2,4,6
    mov             r2w, word [r0 + 128]            ; topLast
    mov             r3w, word [r0 + 256]            ; LeftLast

    ; filtering top
    ; 0 to 15
    movu            m0, [r0 +  0]
    movu            m1, [r0 + 16]
    movu            m2, [r0 + 32]

    pshufb          m4, m0, [intra_filter4_shuf0]
    palignr         m5, m1, m0, 2
    pinsrw          m5, [r0 + 130], 0

    palignr         m3, m1, m0, 14
    psllw           m0, 1
    paddw           m4, m5
    paddw           m0, m4
    paddw           m0, [pw_2]
    psrlw           m0, 2

    palignr         m4, m2, m1, 2
    psllw           m5, m1, 1
    paddw           m4, m3
    paddw           m5, m4
    paddw           m5, [pw_2]
    psrlw           m5, 2
    movu            [r1], m0
    movu            [r1 + 16], m5

    ; 16 to 31
    movu            m0, [r0 + 48]
    movu            m5, [r0 + 64]

    palignr         m3, m2, m1, 14
    palignr         m4, m0, m2, 2

    psllw           m1, m2, 1
    paddw           m3, m4
    paddw           m1, m3
    paddw           m1, [pw_2]
    psrlw           m1, 2

    palignr         m3, m0, m2, 14
    palignr         m4, m5, m0, 2

    psllw           m2, m0, 1
    paddw           m4, m3
    paddw           m2, m4
    paddw           m2, [pw_2]
    psrlw           m2, 2
    movu            [r1 + 32], m1
    movu            [r1 + 48], m2

    ; 32 to 47
    movu            m1, [r0 + 80]
    movu            m2, [r0 + 96]

    palignr         m3, m5, m0, 14
    palignr         m4, m1, m5, 2

    psllw           m0, m5, 1
    paddw           m3, m4
    paddw           m0, m3
    paddw           m0, [pw_2]
    psrlw           m0, 2

    palignr         m3, m1, m5, 14
    palignr         m4, m2, m1, 2

    psllw           m5, m1, 1
    paddw           m4, m3
    paddw           m5, m4
    paddw           m5, [pw_2]
    psrlw           m5, 2
    movu            [r1 + 64], m0
    movu            [r1 + 80], m5

    ; 48 to 63
    movu            m0, [r0 + 112]
    movu            m5, [r0 + 128]

    palignr         m3, m2, m1, 14
    palignr         m4, m0, m2, 2

    psllw           m1, m2, 1
    paddw           m3, m4
    paddw           m1, m3
    paddw           m1, [pw_2]
    psrlw           m1, 2

    palignr         m3, m0, m2, 14
    palignr         m4, m5, m0, 2

    psllw           m0, 1
    paddw           m4, m3
    paddw           m0, m4
    paddw           m0, [pw_2]
    psrlw           m0, 2
    movu            [r1 +  96], m1
    movu            [r1 + 112], m0

    ; filtering left
    ; 64 to 79
    movu            m1, [r0 + 144]
    movu            m2, [r0 + 160]

    palignr         m4, m5, m5, 14
    pinsrw          m4, [r0], 1
    palignr         m0, m1, m5, 2

    psllw           m3, m5, 1
    paddw           m4, m0
    paddw           m3, m4
    paddw           m3, [pw_2]
    psrlw           m3, 2

    palignr         m0, m1, m5, 14
    palignr         m4, m2, m1, 2

    psllw           m5, m1, 1
    paddw           m4, m0
    paddw           m5, m4
    paddw           m5, [pw_2]
    psrlw           m5, 2
    movu            [r1 + 128], m3
    movu            [r1 + 144], m5

    ; 80 to 95
    movu            m5, [r0 + 176]
    movu            m0, [r0 + 192]

    palignr         m3, m2, m1, 14
    palignr         m4, m5, m2, 2

    psllw           m1, m2, 1
    paddw           m3, m4
    paddw           m1, m3
    paddw           m1, [pw_2]
    psrlw           m1, 2

    palignr         m3, m5, m2, 14
    palignr         m4, m0, m5, 2

    psllw           m2, m5, 1
    paddw           m4, m3
    paddw           m2, m4
    paddw           m2, [pw_2]
    psrlw           m2, 2
    movu            [r1 + 160], m1
    movu            [r1 + 176], m2

    ; 96 to 111
    movu            m1, [r0 + 208]
    movu            m2, [r0 + 224]

    palignr         m3, m0, m5, 14
    palignr         m4, m1, m0, 2

    psllw           m5, m0, 1
    paddw           m3, m4
    paddw           m5, m3
    paddw           m5, [pw_2]
    psrlw           m5, 2

    palignr         m3, m1, m0, 14
    palignr         m4, m2, m1, 2

    psllw           m0, m1, 1
    paddw           m4, m3
    paddw           m0, m4
    paddw           m0, [pw_2]
    psrlw           m0, 2
    movu            [r1 + 192], m5
    movu            [r1 + 208], m0

    ; 112 to 127
    movu            m5, [r0 + 240]
    movu            m0, [r0 + 256]

    palignr         m3, m2, m1, 14
    palignr         m4, m5, m2, 2

    psllw           m1, m2, 1
    paddw           m3, m4
    paddw           m1, m3
    paddw           m1, [pw_2]
    psrlw           m1, 2

    palignr         m3, m5, m2, 14
    palignr         m4, m0, m5, 2

    psllw           m5, 1
    paddw           m4, m3
    paddw           m5, m4
    paddw           m5, [pw_2]
    psrlw           m5, 2
    movu            [r1 + 224], m1
    movu            [r1 + 240], m5

    mov             [r1 + 128], r2w                 ; topLast
    mov             [r1 + 256], r3w                 ; LeftLast
    RET

INIT_YMM avx2
cglobal intra_filter_4x4, 2,4,4
    mov             r2w, word [r0 + 16]         ; topLast
    mov             r3w, word [r0 + 32]         ; LeftLast

    ; filtering top
    movu            m0, [r0]
    vpbroadcastw    m2, xm0
    movu            m1, [r0 + 16]

    palignr         m3, m0, m2, 14              ; [6 5 4 3 2 1 0 0] [14 13 12 11 10 9 8 0]
    pshufb          m3, [intra_filter4_shuf2]   ; [6 5 4 3 2 1 0 1] [14 13 12 11 10 9 0 9] samples[i - 1]
    palignr         m1, m0, 4                   ; [9 8 7 6 5 4 3 2]
    palignr         m1, m1, 14                  ; [9 8 7 6 5 4 3 2]

    psllw           m0, 1
    paddw           m3, m1
    paddw           m0, m3
    paddw           m0, [pw_2]
    psrlw           m0, 2

    movu            [r1], m0
    mov             [r1 + 16], r2w              ; topLast
    mov             [r1 + 32], r3w              ; LeftLast
    RET
