;*****************************************************************************
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Praveen Kumar Tiwari <praveen@multicorewareinc.com>
;*          Murugan Vairavel <murugan@multicorewareinc.com>
;*          Min Chen <chenm003@163.com>
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

cextern pb_4
cextern pb_1
cextern pb_16
cextern pb_64
cextern pw_4
cextern pb_8
cextern pb_32
cextern pb_128

SECTION .text

;-----------------------------------------------------------------------------
; void blockcopy_pp_2x4(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_2x4, 4, 7, 0
    mov    r4w,    [r2]
    mov    r5w,    [r2 + r3]
    mov    r6w,    [r2 + 2 * r3]
    lea    r3,     [r3 + 2 * r3]
    mov    r3w,    [r2 + r3]

    mov    [r0],          r4w
    mov    [r0 + r1],     r5w
    mov    [r0 + 2 * r1], r6w
    lea    r1,            [r1 + 2 * r1]
    mov    [r0 + r1],     r3w
RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_2x8(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_2x8, 4, 7, 0
    lea     r5,      [3 * r1]
    lea     r6,      [3 * r3]

    mov     r4w,           [r2]
    mov     [r0],          r4w
    mov     r4w,           [r2 + r3]
    mov     [r0 + r1],     r4w
    mov     r4w,           [r2 + 2 * r3]
    mov     [r0 + 2 * r1], r4w
    mov     r4w,           [r2 + r6]
    mov     [r0 + r5],     r4w

    lea     r2,            [r2 + 4 * r3]
    mov     r4w,           [r2]
    lea     r0,            [r0 + 4 * r1]
    mov     [r0],          r4w

    mov     r4w,           [r2 + r3]
    mov     [r0 + r1],     r4w
    mov     r4w,           [r2 + 2 * r3]
    mov     [r0 + 2 * r1], r4w
    mov     r4w,           [r2 + r6]
    mov     [r0 + r5],     r4w
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_2x16(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_2x16, 4, 7, 0
    lea     r5,      [3 * r1]
    lea     r6,      [3 * r3]

    mov     r4w,           [r2]
    mov     [r0],          r4w
    mov     r4w,           [r2 + r3]
    mov     [r0 + r1],     r4w
    mov     r4w,           [r2 + 2 * r3]
    mov     [r0 + 2 * r1], r4w
    mov     r4w,           [r2 + r6]
    mov     [r0 + r5],     r4w

%rep 3
    lea     r2,            [r2 + 4 * r3]
    mov     r4w,           [r2]
    lea     r0,            [r0 + 4 * r1]
    mov     [r0],          r4w
    mov     r4w,           [r2 + r3]
    mov     [r0 + r1],     r4w
    mov     r4w,           [r2 + 2 * r3]
    mov     [r0 + 2 * r1], r4w
    mov     r4w,           [r2 + r6]
    mov     [r0 + r5],     r4w
%endrep
    RET


;-----------------------------------------------------------------------------
; void blockcopy_pp_4x2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_4x2, 4, 6, 0
    mov     r4d,     [r2]
    mov     r5d,     [r2 + r3]

    mov     [r0],            r4d
    mov     [r0 + r1],       r5d
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_4x4(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_4x4, 4, 4, 4
    movd     m0,     [r2]
    movd     m1,     [r2 + r3]
    movd     m2,     [r2 + 2 * r3]
    lea      r3,     [r3 + r3 * 2]
    movd     m3,     [r2 + r3]

    movd     [r0],            m0
    movd     [r0 + r1],       m1
    movd     [r0 + 2 * r1],   m2
    lea      r1,              [r1 + 2 * r1]
    movd     [r0 + r1],       m3
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_4x8(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_4x8, 4, 6, 4

    lea     r4,    [3 * r1]
    lea     r5,    [3 * r3]

    movd     m0,     [r2]
    movd     m1,     [r2 + r3]
    movd     m2,     [r2 + 2 * r3]
    movd     m3,     [r2 + r5]

    movd     [r0],          m0
    movd     [r0 + r1],     m1
    movd     [r0 + 2 * r1], m2
    movd     [r0 + r4],     m3

    lea      r2,     [r2 + 4 * r3]
    movd     m0,     [r2]
    movd     m1,     [r2 + r3]
    movd     m2,     [r2 + 2 * r3]
    movd     m3,     [r2 + r5]

    lea      r0,            [r0 + 4 * r1]
    movd     [r0],          m0
    movd     [r0 + r1],     m1
    movd     [r0 + 2 * r1], m2
    movd     [r0 + r4],     m3
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W4_H8 2
INIT_XMM sse2
cglobal blockcopy_pp_%1x%2, 4, 7, 4
    mov    r4d,    %2/8
    lea    r5,     [3 * r1]
    lea    r6,     [3 * r3]

.loop:
    movd     m0,     [r2]
    movd     m1,     [r2 + r3]
    movd     m2,     [r2 + 2 * r3]
    movd     m3,     [r2 + r6]

    movd     [r0],          m0
    movd     [r0 + r1],     m1
    movd     [r0 + 2 * r1], m2
    movd     [r0 + r5],     m3

    lea      r2,     [r2 + 4 * r3]
    movd     m0,     [r2]
    movd     m1,     [r2 + r3]
    movd     m2,     [r2 + 2 * r3]
    movd     m3,     [r2 + r6]

    lea      r0,            [r0 + 4 * r1]
    movd     [r0],          m0
    movd     [r0 + r1],     m1
    movd     [r0 + 2 * r1], m2
    movd     [r0 + r5],     m3

    lea       r0,                  [r0 + 4 * r1]
    lea       r2,                  [r2 + 4 * r3]

    dec       r4d
    jnz       .loop
    RET
%endmacro

BLOCKCOPY_PP_W4_H8 4, 16
BLOCKCOPY_PP_W4_H8 4, 32

;-----------------------------------------------------------------------------
; void blockcopy_pp_6x8(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_6x8, 4, 7, 3

    movd     m0,  [r2]
    mov      r4w, [r2 + 4]
    movd     m1,  [r2 + r3]
    mov      r5w, [r2 + r3 + 4]
    movd     m2,  [r2 + 2 * r3]
    mov      r6w, [r2 + 2 * r3 + 4]

    movd     [r0],              m0
    mov      [r0 + 4],          r4w
    movd     [r0 + r1],         m1
    mov      [r0 + r1 + 4],     r5w
    movd     [r0 + 2 * r1],     m2
    mov      [r0 + 2 * r1 + 4], r6w

    lea      r2,  [r2 + 2 * r3]
    movd     m0,  [r2 + r3]
    mov      r4w, [r2 + r3 + 4]
    movd     m1,  [r2 + 2 * r3]
    mov      r5w, [r2 + 2 * r3 + 4]
    lea      r2,  [r2 + 2 * r3]
    movd     m2,  [r2 + r3]
    mov      r6w, [r2 + r3 + 4]

    lea      r0,                [r0 + 2 * r1]
    movd     [r0 + r1],         m0
    mov      [r0 + r1 + 4],     r4w
    movd     [r0 + 2 * r1],     m1
    mov      [r0 + 2 * r1 + 4], r5w
    lea      r0,                [r0 + 2 * r1]
    movd     [r0 + r1],         m2
    mov      [r0 + r1 + 4],     r6w

    lea      r2,                [r2 + 2 * r3]
    movd     m0,                [r2]
    mov      r4w,               [r2 + 4]
    movd     m1,                [r2 + r3]
    mov      r5w,               [r2 + r3 + 4]

    lea      r0,            [r0 + 2 * r1]
    movd     [r0],          m0
    mov      [r0 + 4],      r4w
    movd     [r0 + r1],     m1
    mov      [r0 + r1 + 4], r5w
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_6x16(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_6x16, 4, 7, 2
    mov     r6d,    16/2
.loop:
    movd    m0,     [r2]
    mov     r4w,    [r2 + 4]
    movd    m1,     [r2 + r3]
    mov     r5w,    [r2 + r3 + 4]
    lea     r2,     [r2 + r3 * 2]
    movd    [r0],           m0
    mov     [r0 + 4],       r4w
    movd    [r0 + r1],      m1
    mov     [r0 + r1 + 4],  r5w
    lea     r0,     [r0 + r1 * 2]
    dec     r6d
    jnz     .loop
    RET


;-----------------------------------------------------------------------------
; void blockcopy_pp_8x2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_8x2, 4, 4, 2
    movh     m0,        [r2]
    movh     m1,        [r2 + r3]

    movh     [r0],       m0
    movh     [r0 + r1],  m1
RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_8x4(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_8x4, 4, 4, 4
    movh     m0,     [r2]
    movh     m1,     [r2 + r3]
    movh     m2,     [r2 + 2 * r3]
    lea      r3,     [r3 + r3 * 2]
    movh     m3,     [r2 + r3]

    movh     [r0],            m0
    movh     [r0 + r1],       m1
    movh     [r0 + 2 * r1],   m2
    lea      r1,              [r1 + 2 * r1]
    movh     [r0 + r1],       m3
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_8x6(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_8x6, 4, 4, 6
    movh     m0,     [r2]
    movh     m1,     [r2 + r3]
    lea      r2,     [r2 + 2 * r3]
    movh     m2,     [r2]
    movh     m3,     [r2 + r3]
    lea      r2,     [r2 + 2 * r3]
    movh     m4,     [r2]
    movh     m5,     [r2 + r3]

    movh     [r0],          m0
    movh     [r0 + r1],     m1
    lea      r0,            [r0 + 2 * r1]
    movh     [r0],          m2
    movh     [r0 + r1],     m3
    lea      r0,            [r0 + 2 * r1]
    movh     [r0],          m4
    movh     [r0 + r1],     m5
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_8x12(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_8x12, 4, 6, 4

    lea      r4, [3 * r3]
    lea      r5, [3 * r1]

    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3

    %rep 2
    lea      r2, [r2 + 4 * r3]
    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    lea      r0,            [r0 + 4 * r1]
    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3
    %endrep
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_8x8(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_8x8, 4, 6, 4

    lea      r4, [3 * r3]
    lea      r5, [3 * r1]

    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3

    lea      r2, [r2 + 4 * r3]
    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    lea      r0,            [r0 + 4 * r1]
    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_8x16(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_8x16, 4, 6, 4

    lea      r4, [3 * r3]
    lea      r5, [3 * r1]

    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3

    %rep 3
    lea      r2, [r2 + 4 * r3]
    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    lea      r0,            [r0 + 4 * r1]
    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3
    %endrep
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_8x32(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_8x32, 4, 6, 4

    lea      r4, [3 * r3]
    lea      r5, [3 * r1]

    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3

    %rep 7
    lea      r2, [r2 + 4 * r3]
    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    lea      r0,            [r0 + 4 * r1]
    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3
    %endrep
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_8x64(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_pp_8x64, 4, 6, 4

    lea      r4, [3 * r3]
    lea      r5, [3 * r1]

    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3

    %rep 15
    lea      r2, [r2 + 4 * r3]
    movh     m0, [r2]
    movh     m1, [r2 + r3]
    movh     m2, [r2 + 2 * r3]
    movh     m3, [r2 + r4]

    lea      r0,            [r0 + 4 * r1]
    movh     [r0],          m0
    movh     [r0 + r1],     m1
    movh     [r0 + 2 * r1], m2
    movh     [r0 + r5],     m3
    %endrep
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W12_H4 2
INIT_XMM sse2
cglobal blockcopy_pp_%1x%2, 4, 5, 4
    mov         r4d,       %2/4

.loop:
    movh    m0,     [r2]
    movd    m1,     [r2 + 8]
    movh    m2,     [r2 + r3]
    movd    m3,     [r2 + r3 + 8]
    lea     r2,     [r2 + 2 * r3]

    movh    [r0],             m0
    movd    [r0 + 8],         m1
    movh    [r0 + r1],        m2
    movd    [r0 + r1 + 8],    m3
    lea     r0,               [r0 + 2 * r1]

    movh    m0,     [r2]
    movd    m1,     [r2 + 8]
    movh    m2,     [r2 + r3]
    movd    m3,     [r2 + r3 + 8]

    movh    [r0],             m0
    movd    [r0 + 8],         m1
    movh    [r0 + r1],        m2
    movd    [r0 + r1 + 8],    m3

    dec     r4d
    lea     r0,               [r0 + 2 * r1]
    lea     r2,               [r2 + 2 * r3]
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_PP_W12_H4 12, 16

BLOCKCOPY_PP_W12_H4 12, 32

;-----------------------------------------------------------------------------
; void blockcopy_pp_16x4(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W16_H4 2
INIT_XMM sse2
cglobal blockcopy_pp_%1x%2, 4, 5, 4
    mov    r4d,    %2/4

.loop:
    movu    m0,    [r2]
    movu    m1,    [r2 + r3]
    lea     r2,    [r2 + 2 * r3]
    movu    m2,    [r2]
    movu    m3,    [r2 + r3]

    movu    [r0],         m0
    movu    [r0 + r1],    m1
    lea     r0,           [r0 + 2 * r1]
    movu    [r0],         m2
    movu    [r0 + r1],    m3

    dec     r4d
    lea     r0,               [r0 + 2 * r1]
    lea     r2,               [r2 + 2 * r3]
    jnz     .loop

    RET
%endmacro

BLOCKCOPY_PP_W16_H4 16, 4
BLOCKCOPY_PP_W16_H4 16, 12

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W16_H8 2
INIT_XMM sse2
cglobal blockcopy_pp_%1x%2, 4, 5, 6
    mov    r4d,    %2/8

.loop:
    movu    m0,    [r2]
    movu    m1,    [r2 + r3]
    lea     r2,    [r2 + 2 * r3]
    movu    m2,    [r2]
    movu    m3,    [r2 + r3]
    lea     r2,    [r2 + 2 * r3]
    movu    m4,    [r2]
    movu    m5,    [r2 + r3]
    lea     r2,    [r2 + 2 * r3]

    movu    [r0],         m0
    movu    [r0 + r1],    m1
    lea     r0,           [r0 + 2 * r1]
    movu    [r0],         m2
    movu    [r0 + r1],    m3
    lea     r0,           [r0 + 2 * r1]
    movu    [r0],         m4
    movu    [r0 + r1],    m5
    lea     r0,           [r0 + 2 * r1]

    movu    m0,           [r2]
    movu    m1,           [r2 + r3]
    movu    [r0],         m0
    movu    [r0 + r1],    m1

    dec    r4d
    lea    r0,    [r0 + 2 * r1]
    lea    r2,    [r2 + 2 * r3]
    jnz    .loop
    RET
%endmacro

BLOCKCOPY_PP_W16_H8 16, 8
BLOCKCOPY_PP_W16_H8 16, 16
BLOCKCOPY_PP_W16_H8 16, 32
BLOCKCOPY_PP_W16_H8 16, 64

BLOCKCOPY_PP_W16_H8 16, 24

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W24_H4 2
INIT_XMM sse2
cglobal blockcopy_pp_%1x%2, 4, 5, 6
    mov    r4d,    %2/4

.loop:
    movu    m0,    [r2]
    movh    m1,    [r2 + 16]
    movu    m2,    [r2 + r3]
    movh    m3,    [r2 + r3 + 16]
    lea     r2,    [r2 + 2 * r3]
    movu    m4,    [r2]
    movh    m5,    [r2 + 16]

    movu    [r0],              m0
    movh    [r0 + 16],         m1
    movu    [r0 + r1],         m2
    movh    [r0 + r1 + 16],    m3
    lea     r0,                [r0 + 2 * r1]
    movu    [r0],              m4
    movh    [r0 + 16],         m5

    movu    m0,                [r2 + r3]
    movh    m1,                [r2 + r3 + 16]
    movu    [r0 + r1],         m0
    movh    [r0 + r1 + 16],    m1

    dec    r4d
    lea    r0,    [r0 + 2 * r1]
    lea    r2,    [r2 + 2 * r3]
    jnz    .loop
    RET
%endmacro

BLOCKCOPY_PP_W24_H4 24, 32

BLOCKCOPY_PP_W24_H4 24, 64

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W32_H4 2
INIT_XMM sse2
cglobal blockcopy_pp_%1x%2, 4, 5, 4
    mov    r4d,    %2/4

.loop:
    movu    m0,    [r2]
    movu    m1,    [r2 + 16]
    movu    m2,    [r2 + r3]
    movu    m3,    [r2 + r3 + 16]
    lea     r2,    [r2 + 2 * r3]

    movu    [r0],              m0
    movu    [r0 + 16],         m1
    movu    [r0 + r1],         m2
    movu    [r0 + r1 + 16],    m3
    lea     r0,                [r0 + 2 * r1]

    movu    m0,    [r2]
    movu    m1,    [r2 + 16]
    movu    m2,    [r2 + r3]
    movu    m3,    [r2 + r3 + 16]

    movu    [r0],              m0
    movu    [r0 + 16],         m1
    movu    [r0 + r1],         m2
    movu    [r0 + r1 + 16],    m3

    dec    r4d
    lea    r0,    [r0 + 2 * r1]
    lea    r2,    [r2 + 2 * r3]
    jnz    .loop
    RET
%endmacro

BLOCKCOPY_PP_W32_H4 32, 8
BLOCKCOPY_PP_W32_H4 32, 16
BLOCKCOPY_PP_W32_H4 32, 24
BLOCKCOPY_PP_W32_H4 32, 32
BLOCKCOPY_PP_W32_H4 32, 64

BLOCKCOPY_PP_W32_H4 32, 48

INIT_YMM avx
cglobal blockcopy_pp_32x8, 4, 6, 6
    lea    r4, [3 * r1]
    lea    r5, [3 * r3]

    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]
    lea     r2, [r2 + 4 * r3]
    movu    m4, [r2]
    movu    m5, [r2 + r3]

    movu    [r0], m0
    movu    [r0 + r1], m1
    movu    [r0 + 2 * r1], m2
    movu    [r0 + r4], m3
    lea     r0, [r0 + 4 * r1]
    movu    [r0], m4
    movu    [r0 + r1], m5

    movu    m0, [r2 + 2 * r3]
    movu    m1, [r2 + r5]

    movu    [r0 + 2 * r1], m0
    movu    [r0 + r4], m1
    RET

INIT_YMM avx
cglobal blockcopy_pp_32x16, 4, 6, 6
    lea    r4,  [3 * r1]
    lea    r5,  [3 * r3]

    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]
    lea     r2, [r2 + 4 * r3]
    movu    m4, [r2]
    movu    m5, [r2 + r3]

    movu    [r0], m0
    movu    [r0 + r1], m1
    movu    [r0 + 2 * r1], m2
    movu    [r0 + r4], m3
    lea     r0, [r0 + 4 * r1]
    movu    [r0], m4
    movu    [r0 + r1], m5

    movu    m0, [r2 + 2 * r3]
    movu    m1, [r2 + r5]
    lea     r2, [r2 + 4 * r3]
    movu    m2, [r2]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + 2 * r3]
    movu    m5, [r2 + r5]

    movu    [r0 + 2 * r1], m0
    movu    [r0 + r4], m1
    lea     r0, [r0 + 4 * r1]
    movu    [r0], m2
    movu    [r0 + r1], m3
    movu    [r0 + 2 * r1], m4
    movu    [r0 + r4], m5

    lea     r2, [r2 + 4 * r3]
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]

    lea     r0, [r0 + 4 * r1]
    movu    [r0], m0
    movu    [r0 + r1], m1
    movu    [r0 + 2 * r1], m2
    movu    [r0 + r4], m3
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_32x24(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_YMM avx
cglobal blockcopy_pp_32x24, 4, 7, 6
lea    r4,  [3 * r1]
lea    r5,  [3 * r3]
mov    r6d, 24/8

.loop:
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]
    lea     r2, [r2 + 4 * r3]
    movu    m4, [r2]
    movu    m5, [r2 + r3]

    movu    [r0], m0
    movu    [r0 + r1], m1
    movu    [r0 + 2 * r1], m2
    movu    [r0 + r4], m3
    lea     r0, [r0 + 4 * r1]
    movu    [r0], m4
    movu    [r0 + r1], m5

    movu    m0, [r2 + 2 * r3]
    movu    m1, [r2 + r5]

    movu    [r0 + 2 * r1], m0
    movu    [r0 + r4], m1

    lea     r2, [r2 + 4 * r3]
    lea     r0, [r0 + 4 * r1]
    dec     r6d
    jnz     .loop
    RET

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W32_H16_avx 2
INIT_YMM avx
cglobal blockcopy_pp_%1x%2, 4, 7, 6
    lea    r4,  [3 * r1]
    lea    r5,  [3 * r3]
    mov    r6d, %2/16

.loop:
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]
    lea     r2, [r2 + 4 * r3]
    movu    m4, [r2]
    movu    m5, [r2 + r3]
  
    movu    [r0], m0
    movu    [r0 + r1], m1
    movu    [r0 + 2 * r1], m2
    movu    [r0 + r4], m3
    lea     r0, [r0 + 4 * r1]
    movu    [r0], m4
    movu    [r0 + r1], m5

    movu    m0, [r2 + 2 * r3]
    movu    m1, [r2 + r5]
    lea     r2, [r2 + 4 * r3]
    movu    m2, [r2]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + 2 * r3]
    movu    m5, [r2 + r5]

    movu    [r0 + 2 * r1], m0
    movu    [r0 + r4], m1
    lea     r0, [r0 + 4 * r1]
    movu    [r0], m2
    movu    [r0 + r1], m3
    movu    [r0 + 2 * r1], m4
    movu    [r0 + r4], m5

    lea     r2, [r2 + 4 * r3]
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]

    lea     r0, [r0 + 4 * r1]
    movu    [r0], m0
    movu    [r0 + r1], m1
    movu    [r0 + 2 * r1], m2
    movu    [r0 + r4], m3

    lea     r2, [r2 + 4 * r3]
    lea     r0, [r0 + 4 * r1]
    dec     r6d
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_PP_W32_H16_avx 32, 32
BLOCKCOPY_PP_W32_H16_avx 32, 48
BLOCKCOPY_PP_W32_H16_avx 32, 64

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W48_H2 2
INIT_XMM sse2
cglobal blockcopy_pp_%1x%2, 4, 5, 6
    mov    r4d,    %2/4

.loop:
    movu    m0,    [r2]
    movu    m1,    [r2 + 16]
    movu    m2,    [r2 + 32]
    movu    m3,    [r2 + r3]
    movu    m4,    [r2 + r3 + 16]
    movu    m5,    [r2 + r3 + 32]
    lea     r2,    [r2 + 2 * r3]

    movu    [r0],              m0
    movu    [r0 + 16],         m1
    movu    [r0 + 32],         m2
    movu    [r0 + r1],         m3
    movu    [r0 + r1 + 16],    m4
    movu    [r0 + r1 + 32],    m5
    lea     r0,    [r0 + 2 * r1]

    movu    m0,    [r2]
    movu    m1,    [r2 + 16]
    movu    m2,    [r2 + 32]
    movu    m3,    [r2 + r3]
    movu    m4,    [r2 + r3 + 16]
    movu    m5,    [r2 + r3 + 32]

    movu    [r0],              m0
    movu    [r0 + 16],         m1
    movu    [r0 + 32],         m2
    movu    [r0 + r1],         m3
    movu    [r0 + r1 + 16],    m4
    movu    [r0 + r1 + 32],    m5

    dec    r4d
    lea    r0,    [r0 + 2 * r1]
    lea    r2,    [r2 + 2 * r3]
    jnz    .loop
    RET
%endmacro

BLOCKCOPY_PP_W48_H2 48, 64

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W48_H4_avx 2
INIT_YMM avx
cglobal blockcopy_pp_%1x%2, 4, 5, 4
    mov    r4d,    %2/4

.loop:
    movu    m0,    [r2]
    movu    xm1,   [r2 + 32]
    movu    m2,    [r2 + r3]
    movu    xm3,   [r2 + r3 + 32]
    lea     r2,    [r2 + 2 * r3]

    movu    [r0],              m0
    movu    [r0 + 32],         xm1
    movu    [r0 + r1],         m2
    movu    [r0 + r1 + 32],    xm3
    lea     r0,                [r0 + 2 * r1]

    movu    m0,    [r2]
    movu    xm1,   [r2 + 32]
    movu    m2,    [r2 + r3]
    movu    xm3,   [r2 + r3 + 32]

    movu    [r0],              m0
    movu    [r0 + 32],         xm1
    movu    [r0 + r1],         m2
    movu    [r0 + r1 + 32],    xm3

    dec    r4d
    lea    r0,    [r0 + 2 * r1]
    lea    r2,    [r2 + 2 * r3]
    jnz    .loop
    RET
%endmacro

BLOCKCOPY_PP_W48_H4_avx 48, 64

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W64_H4 2
INIT_XMM sse2
cglobal blockcopy_pp_%1x%2, 4, 5, 6
    mov    r4d,    %2/4

.loop:
    movu    m0,    [r2]
    movu    m1,    [r2 + 16]
    movu    m2,    [r2 + 32]
    movu    m3,    [r2 + 48]
    movu    m4,    [r2 + r3]
    movu    m5,    [r2 + r3 + 16]

    movu    [r0],              m0
    movu    [r0 + 16],         m1
    movu    [r0 + 32],         m2
    movu    [r0 + 48],         m3
    movu    [r0 + r1],         m4
    movu    [r0 + r1 + 16],    m5

    movu    m0,    [r2 + r3 + 32]
    movu    m1,    [r2 + r3 + 48]
    lea     r2,    [r2 + 2 * r3]
    movu    m2,    [r2]
    movu    m3,    [r2 + 16]
    movu    m4,    [r2 + 32]
    movu    m5,    [r2 + 48]

    movu    [r0 + r1 + 32],    m0
    movu    [r0 + r1 + 48],    m1
    lea     r0,                [r0 + 2 * r1]
    movu    [r0],              m2
    movu    [r0 + 16],         m3
    movu    [r0 + 32],         m4
    movu    [r0 + 48],         m5

    movu    m0,    [r2 + r3]
    movu    m1,    [r2 + r3 + 16]
    movu    m2,    [r2 + r3 + 32]
    movu    m3,    [r2 + r3 + 48]

    movu    [r0 + r1],         m0
    movu    [r0 + r1 + 16],    m1
    movu    [r0 + r1 + 32],    m2
    movu    [r0 + r1 + 48],    m3

    dec    r4d
    lea    r0,    [r0 + 2 * r1]
    lea    r2,    [r2 + 2 * r3]
    jnz    .loop
    RET
%endmacro

BLOCKCOPY_PP_W64_H4 64, 16
BLOCKCOPY_PP_W64_H4 64, 32
BLOCKCOPY_PP_W64_H4 64, 48
BLOCKCOPY_PP_W64_H4 64, 64

;-----------------------------------------------------------------------------
; void blockcopy_pp_%1x%2(pixel* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PP_W64_H4_avx 2
INIT_YMM avx
cglobal blockcopy_pp_%1x%2, 4, 7, 6
    lea    r4,  [3 * r1]
    lea    r5,  [3 * r3]
    mov    r6d, %2/4

.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 32]
    movu    m2, [r2 + r3]
    movu    m3, [r2 + r3 + 32]
    movu    m4, [r2 + 2 * r3]
    movu    m5, [r2 + 2 * r3 + 32]

    movu    [r0], m0
    movu    [r0 + 32], m1
    movu    [r0 + r1], m2
    movu    [r0 + r1 + 32], m3
    movu    [r0 + 2 * r1], m4
    movu    [r0 + 2 * r1 + 32], m5

    movu    m0, [r2 + r5]
    movu    m1, [r2 + r5 + 32]

    movu    [r0 + r4], m0
    movu    [r0 + r4 + 32], m1

    lea     r2, [r2 + 4 * r3]
    lea     r0, [r0 + 4 * r1]
    dec     r6d
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_PP_W64_H4_avx 64, 16
BLOCKCOPY_PP_W64_H4_avx 64, 32
BLOCKCOPY_PP_W64_H4_avx 64, 48
BLOCKCOPY_PP_W64_H4_avx 64, 64

;-----------------------------------------------------------------------------
; void blockcopy_sp_2x4(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_sp_2x4, 4, 5, 2

add        r3, r3

;Row 0-1
movd       m0, [r2]
movd       m1, [r2 + r3]
packuswb   m0, m1
movd       r4d, m0
mov        [r0], r4w
pextrw     [r0 + r1], m0, 4

;Row 2-3
movd       m0, [r2 + 2 * r3]
lea        r2, [r2 + 2 * r3]
movd       m1, [r2 + r3]
packuswb   m0, m1
movd       r4d, m0
mov        [r0 + 2 * r1], r4w
lea        r0, [r0 + 2 * r1]
pextrw     [r0 + r1], m0, 4

RET


;-----------------------------------------------------------------------------
; void blockcopy_sp_2x8(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_sp_2x8, 4, 5, 2

add        r3, r3

;Row 0-1
movd       m0, [r2]
movd       m1, [r2 + r3]
packuswb   m0, m1
movd       r4d, m0
mov        [r0], r4w
pextrw     [r0 + r1], m0, 4

;Row 2-3
movd       m0, [r2 + 2 * r3]
lea        r2, [r2 + 2 * r3]
movd       m1, [r2 + r3]
packuswb   m0, m1
movd       r4d, m0
mov        [r0 + 2 * r1], r4w
lea        r0, [r0 + 2 * r1]
pextrw     [r0 + r1], m0, 4

;Row 4-5
movd       m0, [r2 + 2 * r3]
lea        r2, [r2 + 2 * r3]
movd       m1, [r2 + r3]
packuswb   m0, m1
movd       r4d, m0
mov        [r0 + 2 * r1], r4w
lea        r0, [r0 + 2 * r1]
pextrw     [r0 + r1], m0, 4

;Row 6-7
movd       m0, [r2 + 2 * r3]
lea        r2, [r2 + 2 * r3]
movd       m1, [r2 + r3]
packuswb   m0, m1
movd       r4d, m0
mov        [r0 + 2 * r1], r4w
lea        r0, [r0 + 2 * r1]
pextrw     [r0 + r1], m0, 4

RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W2_H2 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 7, 2, dst, dstStride, src, srcStride
    add         r3,     r3
    mov         r6d,    %2/2
.loop:
    movd        m0,     [r2]
    movd        m1,     [r2 + r3]
    dec         r6d
    lea         r2,     [r2 + r3 * 2]
    packuswb    m0,     m0
    packuswb    m1,     m1
    movd        r4d,        m0
    movd        r5d,        m1
    mov         [r0],       r4w
    mov         [r0 + r1],  r5w
    lea         r0,         [r0 + r1 * 2]
    jnz         .loop
    RET
%endmacro

BLOCKCOPY_SP_W2_H2 2,  4
BLOCKCOPY_SP_W2_H2 2,  8

BLOCKCOPY_SP_W2_H2 2, 16

;-----------------------------------------------------------------------------
; void blockcopy_sp_4x2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_sp_4x2, 4, 4, 2, dst, dstStride, src, srcStride

add        r3,        r3

movh       m0,        [r2]
movh       m1,        [r2 + r3]

packuswb   m0,        m1

movd       [r0],      m0
pshufd     m0,        m0,        2
movd       [r0 + r1], m0

RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_4x4(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_sp_4x4, 4, 4, 4, dst, dstStride, src, srcStride

add        r3,     r3

movh       m0,     [r2]
movh       m1,     [r2 + r3]
movh       m2,     [r2 + 2 * r3]
lea        r2,     [r2 + 2 * r3]
movh       m3,     [r2 + r3]

packuswb   m0,            m1
packuswb   m2,            m3

movd       [r0],          m0
pshufd     m0,            m0,         2
movd       [r0 + r1],     m0
movd       [r0 + 2 * r1], m2
lea        r0,            [r0 + 2 * r1]
pshufd     m2,            m2,         2
movd       [r0 + r1],     m2

RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_4x8(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_sp_4x8, 4, 4, 8, dst, dstStride, src, srcStride

add        r3,      r3

movh       m0,      [r2]
movh       m1,      [r2 + r3]
movh       m2,      [r2 + 2 * r3]
lea        r2,      [r2 + 2 * r3]
movh       m3,      [r2 + r3]
movh       m4,      [r2 + 2 * r3]
lea        r2,      [r2 + 2 * r3]
movh       m5,      [r2 + r3]
movh       m6,      [r2 + 2 * r3]
lea        r2,      [r2 + 2 * r3]
movh       m7,      [r2 + r3]

packuswb   m0,      m1
packuswb   m2,      m3
packuswb   m4,      m5
packuswb   m6,      m7

movd       [r0],          m0
pshufd     m0,            m0,         2
movd       [r0 + r1],     m0
movd       [r0 + 2 * r1], m2
lea        r0,            [r0 + 2 * r1]
pshufd     m2,            m2,         2
movd       [r0 + r1],     m2
movd       [r0 + 2 * r1], m4
lea        r0,            [r0 + 2 * r1]
pshufd     m4,            m4,         2
movd       [r0 + r1],     m4
movd       [r0 + 2 * r1], m6
lea        r0,            [r0 + 2 * r1]
pshufd     m6,            m6,         2
movd       [r0 + r1],     m6

RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W4_H8 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 8, dst, dstStride, src, srcStride

mov         r4d,    %2/8

add         r3,     r3

.loop:
     movh       m0,      [r2]
     movh       m1,      [r2 + r3]
     movh       m2,      [r2 + 2 * r3]
     lea        r2,      [r2 + 2 * r3]
     movh       m3,      [r2 + r3]
     movh       m4,      [r2 + 2 * r3]
     lea        r2,      [r2 + 2 * r3]
     movh       m5,      [r2 + r3]
     movh       m6,      [r2 + 2 * r3]
     lea        r2,      [r2 + 2 * r3]
     movh       m7,      [r2 + r3]

     packuswb   m0,      m1
     packuswb   m2,      m3
     packuswb   m4,      m5
     packuswb   m6,      m7

     movd       [r0],          m0
     pshufd     m0,            m0,         2
     movd       [r0 + r1],     m0
     movd       [r0 + 2 * r1], m2
     lea        r0,            [r0 + 2 * r1]
     pshufd     m2,            m2,         2
     movd       [r0 + r1],     m2
     movd       [r0 + 2 * r1], m4
     lea        r0,            [r0 + 2 * r1]
     pshufd     m4,            m4,         2
     movd       [r0 + r1],     m4
     movd       [r0 + 2 * r1], m6
     lea        r0,            [r0 + 2 * r1]
     pshufd     m6,            m6,         2
     movd       [r0 + r1],     m6

     lea        r0,            [r0 + 2 * r1]
     lea        r2,            [r2 + 2 * r3]

     dec        r4d
     jnz        .loop

RET
%endmacro

BLOCKCOPY_SP_W4_H8 4, 16

BLOCKCOPY_SP_W4_H8 4, 32

;-----------------------------------------------------------------------------
; void blockcopy_sp_6x8(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_sp_6x8, 4, 4, 2

    add       r3, r3

    movu      m0, [r2]
    movu      m1, [r2 + r3]
    packuswb  m0, m1

    movd      [r0], m0
    pextrw    [r0 + 4], m0, 2

    movhlps   m0, m0
    movd      [r0 + r1], m0
    pextrw    [r0 + r1 + 4], m0, 2

    lea       r0, [r0 + 2 * r1]
    lea       r2, [r2 + 2 * r3]

    movu      m0, [r2]
    movu      m1, [r2 + r3]
    packuswb  m0, m1

    movd      [r0], m0
    pextrw    [r0 + 4], m0, 2

    movhlps   m0, m0
    movd      [r0 + r1], m0
    pextrw    [r0 + r1 + 4], m0, 2

    lea       r0, [r0 + 2 * r1]
    lea       r2, [r2 + 2 * r3]

    movu      m0, [r2]
    movu      m1, [r2 + r3]
    packuswb  m0, m1

    movd      [r0], m0
    pextrw    [r0 + 4], m0, 2

    movhlps   m0, m0
    movd      [r0 + r1], m0
    pextrw    [r0 + r1 + 4], m0, 2

    lea       r0, [r0 + 2 * r1]
    lea       r2, [r2 + 2 * r3]

    movu      m0, [r2]
    movu      m1, [r2 + r3]
    packuswb  m0, m1

    movd      [r0], m0
    pextrw    [r0 + 4], m0, 2

    movhlps   m0, m0
    movd      [r0 + r1], m0
    pextrw    [r0 + r1 + 4], m0, 2

    RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W6_H2 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 7, 4, dst, dstStride, src, srcStride
    add         r3,     r3
    mov         r6d,    %2/2
.loop:
    movh        m0, [r2]
    movd        m2, [r2 + 8]
    movh        m1, [r2 + r3]
    movd        m3, [r2 + r3 + 8]
    dec         r6d
    lea         r2, [r2 + r3 * 2]
    packuswb    m0, m0
    packuswb    m2, m2
    packuswb    m1, m1
    packuswb    m3, m3
    movd        r4d,            m2
    movd        r5d,            m3
    movd        [r0],           m0
    mov         [r0 + 4],       r4w
    movd        [r0 + r1],      m1
    mov         [r0 + r1 + 4],  r5w
    lea         r0, [r0 + r1 * 2]
    jnz         .loop
    RET
%endmacro

BLOCKCOPY_SP_W6_H2 6,  8

BLOCKCOPY_SP_W6_H2 6, 16

;-----------------------------------------------------------------------------
; void blockcopy_sp_8x2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_sp_8x2, 4, 4, 2, dst, dstStride, src, srcStride

add        r3,         r3

movu       m0,         [r2]
movu       m1,         [r2 + r3]

packuswb   m0,         m1

movlps     [r0],       m0
movhps     [r0 + r1],  m0

RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_8x4(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_sp_8x4, 4, 4, 4, dst, dstStride, src, srcStride

add        r3,     r3

movu       m0,     [r2]
movu       m1,     [r2 + r3]
movu       m2,     [r2 + 2 * r3]
lea        r2,     [r2 + 2 * r3]
movu       m3,     [r2 + r3]

packuswb   m0,            m1
packuswb   m2,            m3

movlps     [r0],          m0
movhps     [r0 + r1],     m0
movlps     [r0 + 2 * r1], m2
lea        r0,            [r0 + 2 * r1]
movhps     [r0 + r1],     m2

RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_8x6(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_sp_8x6, 4, 4, 6, dst, dstStride, src, srcStride

add        r3,      r3

movu       m0,      [r2]
movu       m1,      [r2 + r3]
movu       m2,      [r2 + 2 * r3]
lea        r2,      [r2 + 2 * r3]
movu       m3,      [r2 + r3]
movu       m4,      [r2 + 2 * r3]
lea        r2,      [r2 + 2 * r3]
movu       m5,      [r2 + r3]

packuswb   m0,            m1
packuswb   m2,            m3
packuswb   m4,            m5

movlps     [r0],          m0
movhps     [r0 + r1],     m0
movlps     [r0 + 2 * r1], m2
lea        r0,            [r0 + 2 * r1]
movhps     [r0 + r1],     m2
movlps     [r0 + 2 * r1], m4
lea        r0,            [r0 + 2 * r1]
movhps     [r0 + r1],     m4

RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_8x8(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_sp_8x8, 4, 4, 8, dst, dstStride, src, srcStride

add        r3,      r3

movu       m0,      [r2]
movu       m1,      [r2 + r3]
movu       m2,      [r2 + 2 * r3]
lea        r2,      [r2 + 2 * r3]
movu       m3,      [r2 + r3]
movu       m4,      [r2 + 2 * r3]
lea        r2,      [r2 + 2 * r3]
movu       m5,      [r2 + r3]
movu       m6,      [r2 + 2 * r3]
lea        r2,      [r2 + 2 * r3]
movu       m7,      [r2 + r3]

packuswb   m0,      m1
packuswb   m2,      m3
packuswb   m4,      m5
packuswb   m6,      m7

movlps     [r0],          m0
movhps     [r0 + r1],     m0
movlps     [r0 + 2 * r1], m2
lea        r0,            [r0 + 2 * r1]
movhps     [r0 + r1],     m2
movlps     [r0 + 2 * r1], m4
lea        r0,            [r0 + 2 * r1]
movhps     [r0 + r1],     m4
movlps     [r0 + 2 * r1], m6
lea        r0,            [r0 + 2 * r1]
movhps     [r0 + r1],     m6

RET

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W8_H4 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 4, dst, dstStride, src, srcStride
    add         r3,     r3
    mov         r4d,    %2/4
.loop:
    movu        m0,     [r2]
    movu        m1,     [r2 + r3]
    lea         r2,     [r2 + r3 * 2]
    movu        m2,     [r2]
    movu        m3,     [r2 + r3]
    dec         r4d
    lea         r2,     [r2 + r3 * 2]
    packuswb    m0,     m1
    packuswb    m2,     m3
    movlps      [r0],       m0
    movhps      [r0 + r1],  m0
    lea         r0,         [r0 + r1 * 2]
    movlps      [r0],       m2
    movhps      [r0 + r1],  m2
    lea         r0,         [r0 + r1 * 2]
    jnz         .loop
    RET
%endmacro

BLOCKCOPY_SP_W8_H4 8, 12

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W8_H8 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 8, dst, dstStride, src, srcStride

mov         r4d,    %2/8

add         r3,     r3

.loop:
     movu       m0,      [r2]
     movu       m1,      [r2 + r3]
     movu       m2,      [r2 + 2 * r3]
     lea        r2,      [r2 + 2 * r3]
     movu       m3,      [r2 + r3]
     movu       m4,      [r2 + 2 * r3]
     lea        r2,      [r2 + 2 * r3]
     movu       m5,      [r2 + r3]
     movu       m6,      [r2 + 2 * r3]
     lea        r2,      [r2 + 2 * r3]
     movu       m7,      [r2 + r3]

     packuswb   m0,      m1
     packuswb   m2,      m3
     packuswb   m4,      m5
     packuswb   m6,      m7

     movlps     [r0],          m0
     movhps     [r0 + r1],     m0
     movlps     [r0 + 2 * r1], m2
     lea        r0,            [r0 + 2 * r1]
     movhps     [r0 + r1],     m2
     movlps     [r0 + 2 * r1], m4
     lea        r0,            [r0 + 2 * r1]
     movhps     [r0 + r1],     m4
     movlps     [r0 + 2 * r1], m6
     lea        r0,            [r0 + 2 * r1]
     movhps     [r0 + r1],     m6

    lea         r0,            [r0 + 2 * r1]
    lea         r2,            [r2 + 2 * r3]

    dec         r4d
    jnz         .loop

RET
%endmacro

BLOCKCOPY_SP_W8_H8 8, 16
BLOCKCOPY_SP_W8_H8 8, 32

BLOCKCOPY_SP_W8_H8 8, 64

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W12_H4 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 8, dst, dstStride, src, srcStride

mov             r4d,     %2/4

add             r3,      r3

.loop:
     movu       m0,      [r2]
     movu       m1,      [r2 + 16]
     movu       m2,      [r2 + r3]
     movu       m3,      [r2 + r3 + 16]
     movu       m4,      [r2 + 2 * r3]
     movu       m5,      [r2 + 2 * r3 + 16]
     lea        r2,      [r2 + 2 * r3]
     movu       m6,      [r2 + r3]
     movu       m7,      [r2 + r3 + 16]

     packuswb   m0,      m1
     packuswb   m2,      m3
     packuswb   m4,      m5
     packuswb   m6,      m7

     movh       [r0],              m0
     pshufd     m0,                m0,    2
     movd       [r0 + 8],          m0

     movh       [r0 + r1],         m2
     pshufd     m2,                m2,    2
     movd       [r0 + r1 + 8],     m2

     movh       [r0 + 2 * r1],     m4
     pshufd     m4,                m4,    2
     movd       [r0 + 2 * r1 + 8], m4

     lea        r0,                [r0 + 2 * r1]
     movh       [r0 + r1],         m6
     pshufd     m6,                m6,    2
     movd       [r0 + r1 + 8],     m6

     lea        r0,                [r0 + 2 * r1]
     lea        r2,                [r2 + 2 * r3]

     dec        r4d
     jnz        .loop

RET
%endmacro

BLOCKCOPY_SP_W12_H4 12, 16

BLOCKCOPY_SP_W12_H4 12, 32

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W16_H4 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 8, dst, dstStride, src, srcStride

mov             r4d,     %2/4

add             r3,      r3

.loop:
     movu       m0,      [r2]
     movu       m1,      [r2 + 16]
     movu       m2,      [r2 + r3]
     movu       m3,      [r2 + r3 + 16]
     movu       m4,      [r2 + 2 * r3]
     movu       m5,      [r2 + 2 * r3 + 16]
     lea        r2,      [r2 + 2 * r3]
     movu       m6,      [r2 + r3]
     movu       m7,      [r2 + r3 + 16]

     packuswb   m0,      m1
     packuswb   m2,      m3
     packuswb   m4,      m5
     packuswb   m6,      m7

     movu       [r0],              m0
     movu       [r0 + r1],         m2
     movu       [r0 + 2 * r1],     m4
     lea        r0,                [r0 + 2 * r1]
     movu       [r0 + r1],         m6

     lea        r0,                [r0 + 2 * r1]
     lea        r2,                [r2 + 2 * r3]

     dec        r4d
     jnz        .loop

RET
%endmacro

BLOCKCOPY_SP_W16_H4 16,  4
BLOCKCOPY_SP_W16_H4 16,  8
BLOCKCOPY_SP_W16_H4 16, 12
BLOCKCOPY_SP_W16_H4 16, 16
BLOCKCOPY_SP_W16_H4 16, 32
BLOCKCOPY_SP_W16_H4 16, 64
BLOCKCOPY_SP_W16_H4 16, 24
;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W16_H8_avx2 2
INIT_YMM avx2
cglobal blockcopy_sp_%1x%2, 4, 7, 4, dst, dstStride, src, srcStride
    mov    r4d, %2/8
    add    r3,  r3
    lea    r5,  [3 * r3]
    lea    r6,  [3 * r1]

.loop:
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]

    packuswb    m0, m1
    packuswb    m2, m3

    vpermq    m0, m0, 11011000b
    vpermq    m2, m2, 11011000b

    vextracti128 xm1, m0, 1
    vextracti128 xm3, m2, 1

    movu    [r0],          xm0
    movu    [r0 + r1],     xm1
    movu    [r0 + 2 * r1], xm2
    movu    [r0 + r6],     xm3

    lea     r2, [r2 + 4 * r3]
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]

    packuswb    m0, m1
    packuswb    m2, m3

    vpermq    m0, m0, 11011000b
    vpermq    m2, m2, 11011000b

    vextracti128 xm1, m0, 1
    vextracti128 xm3, m2, 1

    lea     r0,            [r0 + 4 * r1]
    movu    [r0],          xm0
    movu    [r0 + r1],     xm1
    movu    [r0 + 2 * r1], xm2
    movu    [r0 + r6],     xm3

    lea    r0, [r0 + 4 * r1]
    lea    r2, [r2 + 4 * r3]

    dec    r4d
    jnz    .loop
    RET
%endmacro

BLOCKCOPY_SP_W16_H8_avx2 16, 16
BLOCKCOPY_SP_W16_H8_avx2 16, 32
;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W24_H2 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 6, dst, dstStride, src, srcStride

mov             r4d,     %2/2

add             r3,      r3

.loop:
     movu       m0,      [r2]
     movu       m1,      [r2 + 16]
     movu       m2,      [r2 + 32]
     movu       m3,      [r2 + r3]
     movu       m4,      [r2 + r3 + 16]
     movu       m5,      [r2 + r3 + 32]

     packuswb   m0,      m1
     packuswb   m2,      m3
     packuswb   m4,      m5

     movu       [r0],            m0
     movlps     [r0 + 16],       m2
     movhps     [r0 + r1],       m2
     movu       [r0 + r1 + 8],   m4

     lea        r0,              [r0 + 2 * r1]
     lea        r2,              [r2 + 2 * r3]

     dec        r4d
     jnz        .loop

RET
%endmacro

BLOCKCOPY_SP_W24_H2 24, 32

BLOCKCOPY_SP_W24_H2 24, 64

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W32_H2 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 8, dst, dstStride, src, srcStride

mov             r4d,     %2/2

add             r3,      r3

.loop:
     movu       m0,      [r2]
     movu       m1,      [r2 + 16]
     movu       m2,      [r2 + 32]
     movu       m3,      [r2 + 48]
     movu       m4,      [r2 + r3]
     movu       m5,      [r2 + r3 + 16]
     movu       m6,      [r2 + r3 + 32]
     movu       m7,      [r2 + r3 + 48]

     packuswb   m0,      m1
     packuswb   m2,      m3
     packuswb   m4,      m5
     packuswb   m6,      m7

     movu       [r0],            m0
     movu       [r0 + 16],       m2
     movu       [r0 + r1],       m4
     movu       [r0 + r1 + 16],  m6

     lea        r0,              [r0 + 2 * r1]
     lea        r2,              [r2 + 2 * r3]

     dec        r4d
     jnz        .loop

RET
%endmacro

BLOCKCOPY_SP_W32_H2 32,  8
BLOCKCOPY_SP_W32_H2 32, 16
BLOCKCOPY_SP_W32_H2 32, 24
BLOCKCOPY_SP_W32_H2 32, 32
BLOCKCOPY_SP_W32_H2 32, 64

BLOCKCOPY_SP_W32_H2 32, 48

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W32_H4_avx2 2
INIT_YMM avx2
cglobal blockcopy_sp_%1x%2, 4, 7, 4, dst, dstStride, src, srcStride
    mov    r4d, %2/4
    add    r3,  r3
    lea    r5,  [3 * r3]
    lea    r6,  [3 * r1]

.loop:
    movu       m0, [r2]
    movu       m1, [r2 + 32]
    movu       m2, [r2 + r3]
    movu       m3, [r2 + r3 + 32]

    packuswb   m0, m1
    packuswb   m2, m3

    vpermq    m0, m0, 11011000b
    vpermq    m2, m2, 11011000b

    movu       [r0],      m0
    movu       [r0 + r1], m2

    movu       m0, [r2 + 2 * r3]
    movu       m1, [r2 + 2 * r3 + 32]
    movu       m2, [r2 + r5]
    movu       m3, [r2 + r5 + 32]

    packuswb   m0, m1
    packuswb   m2, m3

    vpermq    m0, m0, 11011000b
    vpermq    m2, m2, 11011000b

    movu       [r0 + 2 * r1], m0
    movu       [r0 + r6],     m2

    lea        r0, [r0 + 4 * r1]
    lea        r2, [r2 + 4 * r3]

    dec        r4d
    jnz        .loop
    RET
%endmacro

BLOCKCOPY_SP_W32_H4_avx2 32, 32
BLOCKCOPY_SP_W32_H4_avx2 32, 64

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W48_H2 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 6, dst, dstStride, src, srcStride

mov             r4d,     %2

add             r3,      r3

.loop:
     movu       m0,        [r2]
     movu       m1,        [r2 + 16]
     movu       m2,        [r2 + 32]
     movu       m3,        [r2 + 48]
     movu       m4,        [r2 + 64]
     movu       m5,        [r2 + 80]

     packuswb   m0,        m1
     packuswb   m2,        m3
     packuswb   m4,        m5

     movu       [r0],      m0
     movu       [r0 + 16], m2
     movu       [r0 + 32], m4

     lea        r0,        [r0 + r1]
     lea        r2,        [r2 + r3]

     dec        r4d
     jnz        .loop

RET
%endmacro

BLOCKCOPY_SP_W48_H2 48, 64

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W64_H1 2
INIT_XMM sse2
cglobal blockcopy_sp_%1x%2, 4, 5, 8, dst, dstStride, src, srcStride

mov             r4d,       %2

add             r3,         r3

.loop:
      movu      m0,        [r2]
      movu      m1,        [r2 + 16]
      movu      m2,        [r2 + 32]
      movu      m3,        [r2 + 48]
      movu      m4,        [r2 + 64]
      movu      m5,        [r2 + 80]
      movu      m6,        [r2 + 96]
      movu      m7,        [r2 + 112]

     packuswb   m0,        m1
     packuswb   m2,        m3
     packuswb   m4,        m5
     packuswb   m6,        m7

      movu      [r0],      m0
      movu      [r0 + 16], m2
      movu      [r0 + 32], m4
      movu      [r0 + 48], m6

      lea       r0,        [r0 + r1]
      lea       r2,        [r2 + r3]

      dec       r4d
      jnz       .loop

RET
%endmacro

BLOCKCOPY_SP_W64_H1 64, 16
BLOCKCOPY_SP_W64_H1 64, 32
BLOCKCOPY_SP_W64_H1 64, 48
BLOCKCOPY_SP_W64_H1 64, 64

;-----------------------------------------------------------------------------
; void blockcopy_sp_%1x%2(pixel* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SP_W64_H4_avx2 2
INIT_YMM avx2
cglobal blockcopy_sp_%1x%2, 4, 7, 4, dst, dstStride, src, srcStride
    mov    r4d, %2/4
    add    r3,  r3
    lea    r5,  [3 * r3]
    lea    r6,  [3 * r1]

.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 32]
    movu    m2, [r2 + 64]
    movu    m3, [r2 + 96]

    packuswb    m0, m1
    packuswb    m2, m3

    vpermq    m0, m0, 11011000b
    vpermq    m2, m2, 11011000b

    movu    [r0],      m0
    movu    [r0 + 32], m2

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 32]
    movu    m2, [r2 + r3 + 64]
    movu    m3, [r2 + r3 + 96]

    packuswb    m0, m1
    packuswb    m2, m3

    vpermq    m0, m0, 11011000b
    vpermq    m2, m2, 11011000b

    movu    [r0 + r1],      m0
    movu    [r0 + r1 + 32], m2

    movu    m0, [r2 + 2 * r3]
    movu    m1, [r2 + 2 * r3 + 32]
    movu    m2, [r2 + 2 * r3 + 64]
    movu    m3, [r2 + 2 * r3 + 96]

    packuswb    m0, m1
    packuswb    m2, m3

    vpermq    m0, m0, 11011000b
    vpermq    m2, m2, 11011000b

    movu    [r0 + 2 * r1],      m0
    movu    [r0 + 2 * r1 + 32], m2

    movu    m0, [r2 + r5]
    movu    m1, [r2 + r5 + 32]
    movu    m2, [r2 + r5 + 64]
    movu    m3, [r2 + r5 + 96]

    packuswb    m0, m1
    packuswb    m2, m3

    vpermq    m0, m0, 11011000b
    vpermq    m2, m2, 11011000b

    movu    [r0 + r6],      m0
    movu    [r0 + r6 + 32], m2

    lea    r0, [r0 + 4 * r1]
    lea    r2, [r2 + 4 * r3]

    dec    r4d
    jnz    .loop
    RET
%endmacro

BLOCKCOPY_SP_W64_H4_avx2 64, 64

;-----------------------------------------------------------------------------
; void blockfill_s_4x4(int16_t* dst, intptr_t dstride, int16_t val)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockfill_s_4x4, 3, 3, 1, dst, dstStride, val

add        r1,            r1

movd       m0,            r2d
pshuflw    m0,            m0,         0

movh       [r0],          m0
movh       [r0 + r1],     m0
movh       [r0 + 2 * r1], m0
lea        r0,            [r0 + 2 * r1]
movh       [r0 + r1],     m0

RET

;-----------------------------------------------------------------------------
; void blockfill_s_8x8(int16_t* dst, intptr_t dstride, int16_t val)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockfill_s_8x8, 3, 4, 1, dst, dstStride, val

add        r1,            r1
lea        r3,            [3 * r1]

movd       m0,            r2d
pshuflw    m0,            m0,         0
pshufd     m0,            m0,         0

movu       [r0],          m0
movu       [r0 + r1],     m0
movu       [r0 + 2 * r1], m0

movu       [r0 + r3],     m0

lea        r0,            [r0 + 4 * r1]
movu       [r0],          m0
movu       [r0 + r1],     m0
movu       [r0 + 2 * r1], m0
movu       [r0 + r3],     m0
RET

;-----------------------------------------------------------------------------
; void blockfill_s_16x16(int16_t* dst, intptr_t dstride, int16_t val)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockfill_s_16x16, 3, 4, 1, dst, dstStride, val

add        r1,            r1
lea        r3,            [3 * r1]

movd       m0,            r2d
pshuflw    m0,            m0,         0
pshufd     m0,            m0,         0

movu       [r0],           m0
movu       [r0 + 16],      m0
movu       [r0 + r1],      m0
movu       [r0 + r1 + 16], m0
movu       [r0 + 2 * r1],  m0
movu       [r0 + 2 * r1 + 16], m0

movu       [r0 + r3],          m0
movu       [r0 + r3 + 16],     m0
movu       [r0 + 4 * r1],      m0
movu       [r0 + 4 * r1 + 16], m0

lea        r0,                 [r0 + 4 * r1]
movu       [r0 + r1],          m0
movu       [r0 + r1 + 16],     m0
movu       [r0 + 2 * r1],      m0
movu       [r0 + 2 * r1 + 16], m0
movu       [r0 + r3],          m0
movu       [r0 + r3 + 16],     m0
movu       [r0 + 4 * r1],      m0
movu       [r0 + 4 * r1 + 16], m0

lea        r0,                 [r0 + 4 * r1]
movu       [r0 + r1],          m0
movu       [r0 + r1 + 16],     m0
movu       [r0 + 2 * r1],      m0
movu       [r0 + 2 * r1 + 16], m0
movu       [r0 + r3],          m0
movu       [r0 + r3 + 16],     m0
movu       [r0 + 4 * r1],      m0
movu       [r0 + 4 * r1 + 16], m0

lea        r0,                 [r0 + 4 * r1]
movu       [r0 + r1],          m0
movu       [r0 + r1 + 16],     m0
movu       [r0 + 2 * r1],      m0
movu       [r0 + 2 * r1 + 16], m0
movu       [r0 + r3],          m0
movu       [r0 + r3 + 16],     m0
RET

INIT_YMM avx2
cglobal blockfill_s_16x16, 3, 4, 1
add          r1, r1
lea          r3, [3 * r1]
movd         xm0, r2d
vpbroadcastw m0, xm0

movu       [r0], m0
movu       [r0 + r1], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + r3], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + r1], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + r3], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + r1], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + r3], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + r1], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + r3], m0
RET

;-----------------------------------------------------------------------------
; void blockfill_s_%1x%2(int16_t* dst, intptr_t dstride, int16_t val)
;-----------------------------------------------------------------------------
%macro BLOCKFILL_S_W32_H8 2
INIT_XMM sse2
cglobal blockfill_s_%1x%2, 3, 5, 1, dst, dstStride, val

mov        r3d,           %2/8

add        r1,            r1
lea        r4,            [3 * r1]

movd       m0,            r2d
pshuflw    m0,            m0,       0
pshufd     m0,            m0,       0

.loop:
     movu       [r0],               m0
     movu       [r0 + 16],          m0
     movu       [r0 + 32],          m0
     movu       [r0 + 48],          m0

     movu       [r0 + r1],          m0
     movu       [r0 + r1 + 16],     m0
     movu       [r0 + r1 + 32],     m0
     movu       [r0 + r1 + 48],     m0

     movu       [r0 + 2 * r1],      m0
     movu       [r0 + 2 * r1 + 16], m0
     movu       [r0 + 2 * r1 + 32], m0
     movu       [r0 + 2 * r1 + 48], m0

     movu       [r0 + r4],          m0
     movu       [r0 + r4 + 16],     m0
     movu       [r0 + r4 + 32],     m0
     movu       [r0 + r4 + 48],     m0

     movu       [r0 + 4 * r1],      m0
     movu       [r0 + 4 * r1 + 16], m0
     movu       [r0 + 4 * r1 + 32], m0
     movu       [r0 + 4 * r1 + 48], m0

     lea        r0,                 [r0 + 4 * r1]
     movu       [r0 + r1],          m0
     movu       [r0 + r1 + 16],     m0
     movu       [r0 + r1 + 32],     m0
     movu       [r0 + r1 + 48],     m0

     movu       [r0 + 2 * r1],      m0
     movu       [r0 + 2 * r1 + 16], m0
     movu       [r0 + 2 * r1 + 32], m0
     movu       [r0 + 2 * r1 + 48], m0

     movu       [r0 + r4],          m0
     movu       [r0 + r4 + 16],     m0
     movu       [r0 + r4 + 32],     m0
     movu       [r0 + r4 + 48],     m0

     lea        r0,                 [r0 + 4 * r1]

     dec        r3d
     jnz        .loop

RET
%endmacro

BLOCKFILL_S_W32_H8 32, 32

INIT_YMM avx2
cglobal blockfill_s_32x32, 3, 4, 1
add          r1, r1
lea          r3, [3 * r1]
movd         xm0, r2d
vpbroadcastw m0, xm0

movu       [r0], m0
movu       [r0 + 32], m0
movu       [r0 + r1], m0
movu       [r0 + r1 + 32], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + 2 * r1 + 32], m0
movu       [r0 + r3], m0
movu       [r0 + r3 + 32], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + 32], m0
movu       [r0 + r1], m0
movu       [r0 + r1 + 32], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + 2 * r1 + 32], m0
movu       [r0 + r3], m0
movu       [r0 + r3 + 32], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + 32], m0
movu       [r0 + r1], m0
movu       [r0 + r1 + 32], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + 2 * r1 + 32], m0
movu       [r0 + r3], m0
movu       [r0 + r3 + 32], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + 32], m0
movu       [r0 + r1], m0
movu       [r0 + r1 + 32], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + 2 * r1 + 32], m0
movu       [r0 + r3], m0
movu       [r0 + r3 + 32], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + 32], m0
movu       [r0 + r1], m0
movu       [r0 + r1 + 32], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + 2 * r1 + 32], m0
movu       [r0 + r3], m0
movu       [r0 + r3 + 32], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + 32], m0
movu       [r0 + r1], m0
movu       [r0 + r1 + 32], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + 2 * r1 + 32], m0
movu       [r0 + r3], m0
movu       [r0 + r3 + 32], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + 32], m0
movu       [r0 + r1], m0
movu       [r0 + r1 + 32], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + 2 * r1 + 32], m0
movu       [r0 + r3], m0
movu       [r0 + r3 + 32], m0
lea        r0, [r0 + 4 * r1]
movu       [r0], m0
movu       [r0 + 32], m0
movu       [r0 + r1], m0
movu       [r0 + r1 + 32], m0
movu       [r0 + 2 * r1], m0
movu       [r0 + 2 * r1 + 32], m0
movu       [r0 + r3], m0
movu       [r0 + r3 + 32], m0
RET

;-----------------------------------------------------------------------------
; void blockcopy_ps_2x4(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_2x4, 4, 4, 1, dst, dstStride, src, srcStride

add        r1,            r1

movd       m0,            [r2]
pmovzxbw   m0,            m0
movd       [r0],          m0

movd       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movd       [r0 + r1],     m0

movd       m0,            [r2 + 2 * r3]
pmovzxbw   m0,            m0
movd       [r0 + 2 * r1], m0

lea        r2,            [r2 + 2 * r3]
lea        r0,            [r0 + 2 * r1]

movd       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movd       [r0 + r1],     m0

RET


;-----------------------------------------------------------------------------
; void blockcopy_ps_2x8(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_2x8, 4, 4, 1, dst, dstStride, src, srcStride

add        r1,            r1

movd       m0,            [r2]
pmovzxbw   m0,            m0
movd       [r0],          m0

movd       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movd       [r0 + r1],     m0

movd       m0,            [r2 + 2 * r3]
pmovzxbw   m0,            m0
movd       [r0 + 2 * r1], m0

lea        r2,            [r2 + 2 * r3]
lea        r0,            [r0 + 2 * r1]

movd       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movd       [r0 + r1],     m0

movd       m0,            [r2 + 2 * r3]
pmovzxbw   m0,            m0
movd       [r0 + 2 * r1], m0

lea        r2,            [r2 + 2 * r3]
lea        r0,            [r0 + 2 * r1]

movd       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movd       [r0 + r1],     m0

movd       m0,            [r2 + 2 * r3]
pmovzxbw   m0,            m0
movd       [r0 + 2 * r1], m0

lea        r2,            [r2 + 2 * r3]
lea        r0,            [r0 + 2 * r1]

movd       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movd       [r0 + r1],     m0

RET


;-----------------------------------------------------------------------------
; void blockcopy_ps_2x16(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_2x16, 4, 5, 2, dst, dstStride, src, srcStride
    add         r1,         r1
    mov         r4d,        16/2
.loop:
    movd        m0,         [r2]
    movd        m1,         [r2 + r3]
    dec         r4d
    lea         r2,         [r2 + r3 * 2]
    pmovzxbw    m0,         m0
    pmovzxbw    m1,         m1
    movd        [r0],       m0
    movd        [r0 + r1],  m1
    lea         r0,         [r0 + r1 * 2]
    jnz         .loop
    RET


;-----------------------------------------------------------------------------
; void blockcopy_ps_4x2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_4x2, 4, 4, 1, dst, dstStride, src, srcStride

add        r1,         r1

movd       m0,         [r2]
pmovzxbw   m0,         m0
movh       [r0],       m0

movd       m0,         [r2 + r3]
pmovzxbw   m0,         m0
movh       [r0 + r1],  m0

RET


;-----------------------------------------------------------------------------
; void blockcopy_ps_4x4(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_4x4, 4, 4, 1, dst, dstStride, src, srcStride

add        r1,            r1

movd       m0,            [r2]
pmovzxbw   m0,            m0
movh       [r0],          m0

movd       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movh       [r0 + r1],     m0

movd       m0,            [r2 + 2 * r3]
pmovzxbw   m0,            m0
movh       [r0 + 2 * r1], m0

lea        r2,            [r2 + 2 * r3]
lea        r0,            [r0 + 2 * r1]

movd       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movh       [r0 + r1],     m0

RET


;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W4_H4 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 1, dst, dstStride, src, srcStride

add     r1,      r1
mov    r4d,      %2/4

.loop:
      movd       m0,            [r2]
      pmovzxbw   m0,            m0
      movh       [r0],          m0

      movd       m0,            [r2 + r3]
      pmovzxbw   m0,            m0
      movh       [r0 + r1],     m0

      movd       m0,            [r2 + 2 * r3]
      pmovzxbw   m0,            m0
      movh       [r0 + 2 * r1], m0

      lea        r2,            [r2 + 2 * r3]
      lea        r0,            [r0 + 2 * r1]

      movd       m0,            [r2 + r3]
      pmovzxbw   m0,            m0
      movh       [r0 + r1],     m0

      lea        r0,            [r0 + 2 * r1]
      lea        r2,            [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W4_H4 4, 8
BLOCKCOPY_PS_W4_H4 4, 16

BLOCKCOPY_PS_W4_H4 4, 32


;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W6_H4 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 1, dst, dstStride, src, srcStride

add     r1,      r1
mov    r4d,      %2/4

.loop:
      movh       m0,                [r2]
      pmovzxbw   m0,                m0
      movh       [r0],              m0
      pextrd     [r0 + 8],          m0,            2

      movh       m0,                [r2 + r3]
      pmovzxbw   m0,                m0
      movh       [r0 + r1],         m0
      pextrd     [r0 + r1 + 8],     m0,            2

      movh       m0,                [r2 + 2 * r3]
      pmovzxbw   m0,                m0
      movh       [r0 + 2 * r1],     m0
      pextrd     [r0 + 2 * r1 + 8], m0,            2

      lea        r2,                [r2 + 2 * r3]
      lea        r0,                [r0 + 2 * r1]

      movh       m0,                [r2 + r3]
      pmovzxbw   m0,                m0
      movh       [r0 + r1],         m0
      pextrd     [r0 + r1 + 8],     m0,            2

      lea        r0,                [r0 + 2 * r1]
      lea        r2,                [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W6_H4 6, 8

BLOCKCOPY_PS_W6_H4 6, 16

;-----------------------------------------------------------------------------
; void blockcopy_ps_8x2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_8x2, 4, 4, 1, dst, dstStride, src, srcStride

add        r1,         r1

movh       m0,         [r2]
pmovzxbw   m0,         m0
movu       [r0],       m0

movh       m0,         [r2 + r3]
pmovzxbw   m0,         m0
movu       [r0 + r1],  m0

RET

;-----------------------------------------------------------------------------
; void blockcopy_ps_8x4(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_8x4, 4, 4, 1, dst, dstStride, src, srcStride

add        r1,            r1

movh       m0,            [r2]
pmovzxbw   m0,            m0
movu       [r0],          m0

movh       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movu       [r0 + r1],     m0

movh       m0,            [r2 + 2 * r3]
pmovzxbw   m0,            m0
movu       [r0 + 2 * r1], m0

lea        r2,            [r2 + 2 * r3]
lea        r0,            [r0 + 2 * r1]

movh       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movu       [r0 + r1],     m0

RET

;-----------------------------------------------------------------------------
; void blockcopy_ps_8x6(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_8x6, 4, 4, 1, dst, dstStride, src, srcStride

add        r1,            r1

movh       m0,            [r2]
pmovzxbw   m0,            m0
movu       [r0],          m0

movh       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movu       [r0 + r1],     m0

movh       m0,            [r2 + 2 * r3]
pmovzxbw   m0,            m0
movu       [r0 + 2 * r1], m0

lea        r2,            [r2 + 2 * r3]
lea        r0,            [r0 + 2 * r1]

movh       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movu       [r0 + r1],     m0

movh       m0,            [r2 + 2 * r3]
pmovzxbw   m0,            m0
movu       [r0 + 2 * r1], m0

lea        r2,            [r2 + 2 * r3]
lea        r0,            [r0 + 2 * r1]

movh       m0,            [r2 + r3]
pmovzxbw   m0,            m0
movu       [r0 + r1],     m0

RET

;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W8_H4 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 1, dst, dstStride, src, srcStride

add     r1,      r1
mov    r4d,      %2/4

.loop:
      movh       m0,            [r2]
      pmovzxbw   m0,            m0
      movu       [r0],          m0

      movh       m0,            [r2 + r3]
      pmovzxbw   m0,            m0
      movu       [r0 + r1],     m0

      movh       m0,            [r2 + 2 * r3]
      pmovzxbw   m0,            m0
      movu       [r0 + 2 * r1], m0

      lea        r2,            [r2 + 2 * r3]
      lea        r0,            [r0 + 2 * r1]

      movh       m0,            [r2 + r3]
      pmovzxbw   m0,            m0
      movu       [r0 + r1],     m0

      lea        r0,            [r0 + 2 * r1]
      lea        r2,            [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W8_H4  8,  8
BLOCKCOPY_PS_W8_H4  8, 16
BLOCKCOPY_PS_W8_H4  8, 32

BLOCKCOPY_PS_W8_H4  8, 12
BLOCKCOPY_PS_W8_H4  8, 64


;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W12_H2 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 3, dst, dstStride, src, srcStride

add        r1,      r1
mov        r4d,     %2/2
pxor       m0,      m0

.loop:
      movu       m1,             [r2]
      pmovzxbw   m2,             m1
      movu       [r0],           m2
      punpckhbw  m1,             m0
      movh       [r0 + 16],      m1

      movu       m1,             [r2 + r3]
      pmovzxbw   m2,             m1
      movu       [r0 + r1],      m2
      punpckhbw  m1,             m0
      movh       [r0 + r1 + 16], m1

      lea        r0,             [r0 + 2 * r1]
      lea        r2,             [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W12_H2 12, 16

BLOCKCOPY_PS_W12_H2 12, 32

;-----------------------------------------------------------------------------
; void blockcopy_ps_16x4(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal blockcopy_ps_16x4, 4, 4, 3, dst, dstStride, src, srcStride

add        r1,      r1
pxor       m0,      m0

movu       m1,                 [r2]
pmovzxbw   m2,                 m1
movu       [r0],               m2
punpckhbw  m1,                 m0
movu       [r0 + 16],          m1

movu       m1,                 [r2 + r3]
pmovzxbw   m2,                 m1
movu       [r0 + r1],          m2
punpckhbw  m1,                 m0
movu       [r0 + r1 + 16],     m1

movu       m1,                 [r2 + 2 * r3]
pmovzxbw   m2,                 m1
movu       [r0 + 2 * r1],      m2
punpckhbw  m1,                 m0
movu       [r0 + 2 * r1 + 16], m1

lea        r0,                 [r0 + 2 * r1]
lea        r2,                 [r2 + 2 * r3]

movu       m1,                 [r2 + r3]
pmovzxbw   m2,                 m1
movu       [r0 + r1],          m2
punpckhbw  m1,                 m0
movu       [r0 + r1 + 16],     m1

RET

;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W16_H4 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 3, dst, dstStride, src, srcStride

add        r1,      r1
mov        r4d,     %2/4
pxor       m0,      m0

.loop:
      movu       m1,                 [r2]
      pmovzxbw   m2,                 m1
      movu       [r0],               m2
      punpckhbw  m1,                 m0
      movu       [r0 + 16],          m1

      movu       m1,                 [r2 + r3]
      pmovzxbw   m2,                 m1
      movu       [r0 + r1],          m2
      punpckhbw  m1,                 m0
      movu       [r0 + r1 + 16],     m1

      movu       m1,                 [r2 + 2 * r3]
      pmovzxbw   m2,                 m1
      movu       [r0 + 2 * r1],      m2
      punpckhbw  m1,                 m0
      movu       [r0 + 2 * r1 + 16], m1

      lea        r0,                 [r0 + 2 * r1]
      lea        r2,                 [r2 + 2 * r3]

      movu       m1,                 [r2 + r3]
      pmovzxbw   m2,                 m1
      movu       [r0 + r1],          m2
      punpckhbw  m1,                 m0
      movu       [r0 + r1 + 16],     m1

      lea        r0,                 [r0 + 2 * r1]
      lea        r2,                 [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W16_H4 16,  8
BLOCKCOPY_PS_W16_H4 16, 12
BLOCKCOPY_PS_W16_H4 16, 16
BLOCKCOPY_PS_W16_H4 16, 32
BLOCKCOPY_PS_W16_H4 16, 64
BLOCKCOPY_PS_W16_H4 16, 24
;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W16_H4_avx2 2
INIT_YMM avx2
cglobal blockcopy_ps_%1x%2, 4, 7, 3

    add     r1, r1
    mov     r4d, %2/4
    lea     r5, [3 * r3]
    lea     r6, [3 * r1]
    pxor    m0, m0

.loop:
    movu        xm1, [r2]
    pmovzxbw    m2, xm1
    movu        [r0], m2
    movu        xm1, [r2 + r3]
    pmovzxbw    m2, xm1
    movu        [r0 + r1], m2
    movu        xm1, [r2 + 2 * r3]
    pmovzxbw    m2, xm1
    movu        [r0 + 2 * r1], m2
    movu        xm1, [r2 + r5]
    pmovzxbw    m2, xm1
    movu        [r0 + r6], m2

    lea         r0, [r0 + 4 * r1]
    lea         r2, [r2 + 4 * r3]

    dec         r4d
    jnz         .loop
    RET
%endmacro

BLOCKCOPY_PS_W16_H4_avx2 16, 16
BLOCKCOPY_PS_W16_H4_avx2 16, 32
;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W24_H2 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 3, dst, dstStride, src, srcStride

add        r1,      r1
mov        r4d,     %2/2
pxor       m0,      m0

.loop:
      movu       m1,             [r2]
      pmovzxbw   m2,             m1
      movu       [r0],           m2
      punpckhbw  m1,             m0
      movu       [r0 + 16],      m1

      movh       m1,             [r2 + 16]
      pmovzxbw   m1,             m1
      movu       [r0 + 32],      m1

      movu       m1,             [r2 + r3]
      pmovzxbw   m2,             m1
      movu       [r0 + r1],      m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 16], m1

      movh       m1,             [r2 + r3 + 16]
      pmovzxbw   m1,             m1
      movu       [r0 + r1 + 32], m1

      lea        r0,             [r0 + 2 * r1]
      lea        r2,             [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W24_H2 24, 32

BLOCKCOPY_PS_W24_H2 24, 64

;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W32_H2 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 3, dst, dstStride, src, srcStride

add        r1,      r1
mov        r4d,     %2/2
pxor       m0,      m0

.loop:
      movu       m1,             [r2]
      pmovzxbw   m2,             m1
      movu       [r0],           m2
      punpckhbw  m1,             m0
      movu       [r0 + 16],      m1

      movu       m1,             [r2 + 16]
      pmovzxbw   m2,             m1
      movu       [r0 + 32],      m2
      punpckhbw  m1,             m0
      movu       [r0 + 48],      m1

      movu       m1,             [r2 + r3]
      pmovzxbw   m2,             m1
      movu       [r0 + r1],      m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 16], m1

      movu       m1,             [r2 + r3 + 16]
      pmovzxbw   m2,             m1
      movu       [r0 + r1 + 32], m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 48], m1

      lea        r0,             [r0 + 2 * r1]
      lea        r2,             [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W32_H2 32,  8
BLOCKCOPY_PS_W32_H2 32, 16
BLOCKCOPY_PS_W32_H2 32, 24
BLOCKCOPY_PS_W32_H2 32, 32
BLOCKCOPY_PS_W32_H2 32, 64

BLOCKCOPY_PS_W32_H2 32, 48
;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W32_H4_avx2 2
INIT_YMM avx2
cglobal blockcopy_ps_%1x%2, 4, 7, 2
    add     r1, r1
    mov     r4d, %2/4
    lea     r5, [3 * r3]
    lea     r6, [3 * r1]
.loop:
    pmovzxbw      m0, [r2 +  0]
    pmovzxbw      m1, [r2 + 16]
    movu          [r0 +  0], m0
    movu          [r0 + 32], m1

    pmovzxbw      m0, [r2 + r3 +  0]
    pmovzxbw      m1, [r2 + r3 + 16]
    movu          [r0 + r1 +  0], m0
    movu          [r0 + r1 + 32], m1

    pmovzxbw      m0, [r2 + r3 * 2 +  0]
    pmovzxbw      m1, [r2 + r3 * 2 + 16]
    movu          [r0 + r1 * 2 +  0], m0
    movu          [r0 + r1 * 2 + 32], m1

    pmovzxbw      m0, [r2 + r5 +  0]
    pmovzxbw      m1, [r2 + r5 + 16]
    movu          [r0 + r6 +  0], m0
    movu          [r0 + r6 + 32], m1
    lea           r0, [r0 + 4 * r1]
    lea           r2, [r2 + 4 * r3]
    dec           r4d
    jnz           .loop
    RET
%endmacro

BLOCKCOPY_PS_W32_H4_avx2 32, 32
BLOCKCOPY_PS_W32_H4_avx2 32, 64

;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W48_H2 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 3, dst, dstStride, src, srcStride

add        r1,      r1
mov        r4d,     %2/2
pxor       m0,      m0

.loop:
      movu       m1,             [r2]
      pmovzxbw   m2,             m1
      movu       [r0],           m2
      punpckhbw  m1,             m0
      movu       [r0 + 16],      m1

      movu       m1,             [r2 + 16]
      pmovzxbw   m2,             m1
      movu       [r0 + 32],      m2
      punpckhbw  m1,             m0
      movu       [r0 + 48],      m1

      movu       m1,             [r2 + 32]
      pmovzxbw   m2,             m1
      movu       [r0 + 64],      m2
      punpckhbw  m1,             m0
      movu       [r0 + 80],      m1

      movu       m1,             [r2 + r3]
      pmovzxbw   m2,             m1
      movu       [r0 + r1],      m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 16], m1

      movu       m1,             [r2 + r3 + 16]
      pmovzxbw   m2,             m1
      movu       [r0 + r1 + 32], m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 48], m1

      movu       m1,             [r2 + r3 + 32]
      pmovzxbw   m2,             m1
      movu       [r0 + r1 + 64], m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 80], m1

      lea        r0,             [r0 + 2 * r1]
      lea        r2,             [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W48_H2 48, 64

;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_PS_W64_H2 2
INIT_XMM sse4
cglobal blockcopy_ps_%1x%2, 4, 5, 3, dst, dstStride, src, srcStride

add        r1,      r1
mov        r4d,     %2/2
pxor       m0,      m0

.loop:
      movu       m1,             [r2]
      pmovzxbw   m2,             m1
      movu       [r0],           m2
      punpckhbw  m1,             m0
      movu       [r0 + 16],      m1

      movu       m1,             [r2 + 16]
      pmovzxbw   m2,             m1
      movu       [r0 + 32],      m2
      punpckhbw  m1,             m0
      movu       [r0 + 48],      m1

      movu       m1,             [r2 + 32]
      pmovzxbw   m2,             m1
      movu       [r0 + 64],      m2
      punpckhbw  m1,             m0
      movu       [r0 + 80],      m1

      movu       m1,             [r2 + 48]
      pmovzxbw   m2,             m1
      movu       [r0 + 96],      m2
      punpckhbw  m1,             m0
      movu       [r0 + 112],     m1

      movu       m1,             [r2 + r3]
      pmovzxbw   m2,             m1
      movu       [r0 + r1],      m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 16], m1

      movu       m1,             [r2 + r3 + 16]
      pmovzxbw   m2,             m1
      movu       [r0 + r1 + 32], m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 48], m1

      movu       m1,             [r2 + r3 + 32]
      pmovzxbw   m2,             m1
      movu       [r0 + r1 + 64], m2
      punpckhbw  m1,             m0
      movu       [r0 + r1 + 80], m1

      movu       m1,              [r2 + r3 + 48]
      pmovzxbw   m2,              m1
      movu       [r0 + r1 + 96],  m2
      punpckhbw  m1,              m0
      movu       [r0 + r1 + 112], m1

      lea        r0,              [r0 + 2 * r1]
      lea        r2,              [r2 + 2 * r3]

      dec        r4d
      jnz        .loop

RET
%endmacro

BLOCKCOPY_PS_W64_H2 64, 16
BLOCKCOPY_PS_W64_H2 64, 32
BLOCKCOPY_PS_W64_H2 64, 48
BLOCKCOPY_PS_W64_H2 64, 64
;-----------------------------------------------------------------------------
; void blockcopy_ps_%1x%2(int16_t* dst, intptr_t dstStride, const pixel* src, intptr_t srcStride);
;-----------------------------------------------------------------------------
INIT_YMM avx2
cglobal blockcopy_ps_64x64, 4, 7, 4
    add     r1, r1
    mov     r4d, 64/8
    lea     r5, [3 * r3]
    lea     r6, [3 * r1]
.loop:
%rep 2
    pmovzxbw      m0, [r2 +  0]
    pmovzxbw      m1, [r2 + 16]
    pmovzxbw      m2, [r2 + 32]
    pmovzxbw      m3, [r2 + 48]
    movu          [r0 +  0], m0
    movu          [r0 + 32], m1
    movu          [r0 + 64], m2
    movu          [r0 + 96], m3

    pmovzxbw      m0, [r2 + r3 +  0]
    pmovzxbw      m1, [r2 + r3 + 16]
    pmovzxbw      m2, [r2 + r3 + 32]
    pmovzxbw      m3, [r2 + r3 + 48]
    movu          [r0 + r1 +  0], m0
    movu          [r0 + r1 + 32], m1
    movu          [r0 + r1 + 64], m2
    movu          [r0 + r1 + 96], m3

    pmovzxbw      m0, [r2 + r3 * 2 +  0]
    pmovzxbw      m1, [r2 + r3 * 2 + 16]
    pmovzxbw      m2, [r2 + r3 * 2 + 32]
    pmovzxbw      m3, [r2 + r3 * 2 + 48]
    movu          [r0 + r1 * 2 +  0], m0
    movu          [r0 + r1 * 2 + 32], m1
    movu          [r0 + r1 * 2 + 64], m2
    movu          [r0 + r1 * 2 + 96], m3

    pmovzxbw      m0, [r2 + r5 +  0]
    pmovzxbw      m1, [r2 + r5 + 16]
    pmovzxbw      m2, [r2 + r5 + 32]
    pmovzxbw      m3, [r2 + r5 + 48]
    movu          [r0 + r6 +  0], m0
    movu          [r0 + r6 + 32], m1
    movu          [r0 + r6 + 64], m2
    movu          [r0 + r6 + 96], m3
    lea           r0, [r0 + 4 * r1]
    lea           r2, [r2 + 4 * r3]
%endrep
    dec           r4d
    jnz           .loop
    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_2x4(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_2x4, 4, 6, 0
    add    r1, r1
    add    r3, r3

    mov    r4d, [r2]
    mov    r5d, [r2 + r3]
    mov    [r0], r4d
    mov    [r0 + r1], r5d

    lea    r2, [r2 + r3 * 2]
    lea    r0, [r0 + 2 * r1]

    mov    r4d, [r2]
    mov    r5d, [r2 + r3]
    mov    [r0], r4d
    mov    [r0 + r1], r5d

    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_2x8(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_2x8, 4, 6, 0
    add    r1, r1
    add    r3, r3

    mov    r4d, [r2]
    mov    r5d, [r2 + r3]
    mov    [r0], r4d
    mov    [r0 + r1], r5d

    lea    r2, [r2 + r3 * 2]
    lea    r0, [r0 + 2 * r1]

    mov    r4d, [r2]
    mov    r5d, [r2 + r3]
    mov    [r0], r4d
    mov    [r0 + r1], r5d

    lea    r2, [r2 + r3 * 2]
    lea    r0, [r0 + 2 * r1]

    mov    r4d, [r2]
    mov    r5d, [r2 + r3]
    mov    [r0], r4d
    mov    [r0 + r1], r5d

    lea    r2, [r2 + r3 * 2]
    lea    r0, [r0 + 2 * r1]

    mov    r4d, [r2]
    mov    r5d, [r2 + r3]
    mov    [r0], r4d
    mov    [r0 + r1], r5d

    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_2x16(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_2x16, 4, 7, 0
    add     r1, r1
    add     r3, r3
    mov     r6d,    16/2
.loop:
    mov     r4d,    [r2]
    mov     r5d,    [r2 + r3]
    dec     r6d
    lea     r2, [r2 + r3 * 2]
    mov     [r0],       r4d
    mov     [r0 + r1],  r5d
    lea     r0, [r0 + r1 * 2]
    jnz     .loop
    RET


;-----------------------------------------------------------------------------
; void blockcopy_ss_4x2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_4x2, 4, 4, 2
    add     r1, r1
    add     r3, r3

    movh    m0, [r2]
    movh    m1, [r2 + r3]

    movh    [r0], m0
    movh    [r0 + r1], m1

    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_4x4(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_4x4, 4, 4, 4
    add     r1, r1
    add     r3, r3
    movh    m0, [r2]
    movh    m1, [r2 + r3]
    lea     r2, [r2 + r3 * 2]
    movh    m2, [r2]
    movh    m3, [r2 + r3]

    movh    [r0], m0
    movh    [r0 + r1], m1
    lea     r0, [r0 + 2 * r1]
    movh    [r0], m2
    movh    [r0 + r1], m3
    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W4_H8 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 4
    mov     r4d, %2/8
    add     r1, r1
    add     r3, r3
.loop:
    movh    m0, [r2]
    movh    m1, [r2 + r3]
    lea     r2, [r2 + r3 * 2]
    movh    m2, [r2]
    movh    m3, [r2 + r3]

    movh    [r0], m0
    movh    [r0 + r1], m1
    lea     r0, [r0 + 2 * r1]
    movh    [r0], m2
    movh    [r0 + r1], m3

    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]
    movh    m0, [r2]
    movh    m1, [r2 + r3]
    lea     r2, [r2 + r3 * 2]
    movh    m2, [r2]
    movh    m3, [r2 + r3]

    movh    [r0], m0
    movh    [r0 + r1], m1
    lea     r0, [r0 + 2 * r1]
    movh    [r0], m2
    movh    [r0 + r1], m3
    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]

    dec     r4d
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W4_H8 4, 8
BLOCKCOPY_SS_W4_H8 4, 16

BLOCKCOPY_SS_W4_H8 4, 32

;-----------------------------------------------------------------------------
; void blockcopy_ss_6x8(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_6x8, 4, 4, 4
    add       r1, r1
    add       r3, r3

    movu      m0, [r2]
    movu      m1, [r2 + r3]
    pshufd    m2, m0, 2
    pshufd    m3, m1, 2
    movh      [r0], m0
    movd      [r0 + 8], m2
    movh      [r0 + r1], m1
    movd      [r0 + r1 + 8], m3

    lea       r0, [r0 + 2 * r1]
    lea       r2, [r2 + 2 * r3]

    movu      m0, [r2]
    movu      m1, [r2 + r3]
    pshufd    m2, m0, 2
    pshufd    m3, m1, 2
    movh      [r0], m0
    movd      [r0 + 8], m2
    movh      [r0 + r1], m1
    movd      [r0 + r1 + 8], m3

    lea       r0, [r0 + 2 * r1]
    lea       r2, [r2 + 2 * r3]

    movu      m0, [r2]
    movu      m1, [r2 + r3]
    pshufd    m2, m0, 2
    pshufd    m3, m1, 2
    movh      [r0], m0
    movd      [r0 + 8], m2
    movh      [r0 + r1], m1
    movd      [r0 + r1 + 8], m3

    lea       r0, [r0 + 2 * r1]
    lea       r2, [r2 + 2 * r3]

    movu      m0, [r2]
    movu      m1, [r2 + r3]
    pshufd    m2, m0, 2
    pshufd    m3, m1, 2
    movh      [r0], m0
    movd      [r0 + 8], m2
    movh      [r0 + r1], m1
    movd      [r0 + r1 + 8], m3

    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_6x16(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_6x16, 4, 5, 4
    add     r1, r1
    add     r3, r3
    mov     r4d,    16/2
.loop:
    movh    m0, [r2]
    movd    m2, [r2 + 8]
    movh    m1, [r2 + r3]
    movd    m3, [r2 + r3 + 8]
    dec     r4d
    lea     r2, [r2 + r3 * 2]
    movh    [r0],           m0
    movd    [r0 + 8],       m2
    movh    [r0 + r1],      m1
    movd    [r0 + r1 + 8],  m3
    lea     r0, [r0 + r1 * 2]
    jnz     .loop
    RET


;-----------------------------------------------------------------------------
; void blockcopy_ss_8x2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_8x2, 4, 4, 2
    add     r1, r1
    add     r3, r3

    movu    m0, [r2]
    movu    m1, [r2 + r3]

    movu    [r0], m0
    movu    [r0 + r1], m1

    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_8x4(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_8x4, 4, 4, 4
    add     r1, r1
    add     r3, r3

    movu    m0, [r2]
    movu    m1, [r2 + r3]
    lea     r2, [r2 + r3 * 2]
    movu    m2, [r2]
    movu    m3, [r2 + r3]

    movu    [r0], m0
    movu    [r0 + r1], m1
    lea     r0, [r0 + 2 * r1]
    movu    [r0], m2
    movu    [r0 + r1], m3
    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_8x6(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_8x6, 4, 4, 4

    add     r1, r1
    add     r3, r3
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    lea     r2, [r2 + r3 * 2]
    movu    m2, [r2]
    movu    m3, [r2 + r3]

    movu    [r0], m0
    movu    [r0 + r1], m1
    lea     r0, [r0 + 2 * r1]
    movu    [r0], m2
    movu    [r0 + r1], m3

    lea     r2, [r2 + r3 * 2]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    [r0], m0
    movu    [r0 + r1], m1
    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_8x12(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal blockcopy_ss_8x12, 4, 5, 2
    add     r1, r1
    add     r3, r3
    mov     r4d, 12/2
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    lea     r2, [r2 + 2 * r3]
    dec     r4d
    movu    [r0], m0
    movu    [r0 + r1], m1
    lea     r0, [r0 + 2 * r1]
    jnz     .loop
    RET


;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W8_H8 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 4
    mov     r4d, %2/8
    add     r1, r1
    add     r3, r3
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    lea     r2, [r2 + r3 * 2]
    movu    m2, [r2]
    movu    m3, [r2 + r3]

    movu    [r0], m0
    movu    [r0 + r1], m1
    lea     r0, [r0 + 2 * r1]
    movu    [r0], m2
    movu    [r0 + r1], m3


    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + r3]
    lea     r2, [r2 + r3 * 2]
    movu    m2, [r2]
    movu    m3, [r2 + r3]

    movu    [r0], m0
    movu    [r0 + r1], m1
    lea     r0, [r0 + 2 * r1]
    movu    [r0], m2
    movu    [r0 + r1], m3

    dec     r4d
    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]
    jnz    .loop
RET
%endmacro

BLOCKCOPY_SS_W8_H8 8, 8
BLOCKCOPY_SS_W8_H8 8, 16
BLOCKCOPY_SS_W8_H8 8, 32

BLOCKCOPY_SS_W8_H8 8, 64

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W12_H4 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 4

    mov     r4d, %2/4
    add     r1, r1
    add     r3, r3
.loop:
    movu    m0, [r2]
    movh    m1, [r2 + 16]
    movu    m2, [r2 + r3]
    movh    m3, [r2 + r3 + 16]
    lea     r2, [r2 + 2 * r3]

    movu    [r0], m0
    movh    [r0 + 16], m1
    movu    [r0 + r1], m2
    movh    [r0 + r1 + 16], m3

    lea     r0, [r0 + 2 * r1]
    movu    m0, [r2]
    movh    m1, [r2 + 16]
    movu    m2, [r2 + r3]
    movh    m3, [r2 + r3 + 16]

    movu    [r0], m0
    movh    [r0 + 16], m1
    movu    [r0 + r1], m2
    movh    [r0 + r1 + 16], m3

    dec     r4d
    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W12_H4 12, 16

BLOCKCOPY_SS_W12_H4 12, 32

;-----------------------------------------------------------------------------
; void blockcopy_ss_16x4(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W16_H4 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 4
    mov     r4d, %2/4
    add     r1, r1
    add     r3, r3
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + r3]
    movu    m3, [r2 + r3 + 16]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + r1], m2
    movu    [r0 + r1 + 16], m3

    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + r3]
    movu    m3, [r2 + r3 + 16]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + r1], m2
    movu    [r0 + r1 + 16], m3

    dec     r4d
    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W16_H4 16, 4
BLOCKCOPY_SS_W16_H4 16, 12

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W16_H4_avx 2
INIT_YMM avx
cglobal blockcopy_ss_%1x%2, 4, 7, 4
    mov     r4d, %2/4
    add     r1, r1
    add     r3, r3
    lea     r5, [3 * r3]
    lea     r6, [3 * r1]
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + r3]
    movu    m2, [r2 + 2 * r3]
    movu    m3, [r2 + r5]

    movu    [r0], m0
    movu    [r0 + r1], m1
    movu    [r0 + 2 * r1], m2
    movu    [r0 + r6], m3

    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]
    dec     r4d
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W16_H4_avx 16, 4
BLOCKCOPY_SS_W16_H4_avx 16, 12
BLOCKCOPY_SS_W16_H4_avx 16, 8
BLOCKCOPY_SS_W16_H4_avx 16, 16
BLOCKCOPY_SS_W16_H4_avx 16, 24
BLOCKCOPY_SS_W16_H4_avx 16, 32
BLOCKCOPY_SS_W16_H4_avx 16, 64

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W16_H8 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 4
    mov     r4d, %2/8
    add     r1, r1
    add     r3, r3
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + r3]
    movu    m3, [r2 + r3 + 16]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + r1], m2
    movu    [r0 + r1 + 16], m3

    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + r3]
    movu    m3, [r2 + r3 + 16]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + r1], m2
    movu    [r0 + r1 + 16], m3

    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + r3]
    movu    m3, [r2 + r3 + 16]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + r1], m2
    movu    [r0 + r1 + 16], m3

    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + r3]
    movu    m3, [r2 + r3 + 16]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + r1], m2
    movu    [r0 + r1 + 16], m3

    dec     r4d
    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W16_H8 16, 8
BLOCKCOPY_SS_W16_H8 16, 16
BLOCKCOPY_SS_W16_H8 16, 32
BLOCKCOPY_SS_W16_H8 16, 64

BLOCKCOPY_SS_W16_H8 16, 24

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W24_H4 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 6
    mov     r4d, %2/4
    add     r1, r1
    add     r3, r3
.loop
    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + r3 + 16]
    movu    m5, [r2 + r3 + 32]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + 32], m2
    movu    [r0 + r1], m3
    movu    [r0 + r1 + 16], m4
    movu    [r0 + r1 + 32], m5

    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + r3 + 16]
    movu    m5, [r2 + r3 + 32]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + 32], m2
    movu    [r0 + r1], m3
    movu    [r0 + r1 + 16], m4
    movu    [r0 + r1 + 32], m5

    dec     r4d
    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W24_H4 24, 32

BLOCKCOPY_SS_W24_H4 24, 64
;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W24_H4_avx 2
INIT_YMM avx
cglobal blockcopy_ss_%1x%2, 4, 7, 2

    mov    r4d, %2/4
    add    r1, r1
    add    r3, r3
    lea    r5, [3 * r3]
    lea    r6, [3 * r1]

.loop
    movu    m0, [r2]
    movu    xm1, [r2 + 32]
    movu    [r0], m0
    movu    [r0 + 32], xm1
    movu    m0, [r2 + r3]
    movu    xm1, [r2 + r3 + 32]
    movu    [r0 + r1], m0
    movu    [r0 + r1 + 32], xm1
    movu    m0, [r2 + 2 * r3]
    movu    xm1, [r2 + 2 * r3 + 32]
    movu    [r0 + 2 * r1], m0
    movu    [r0 + 2 * r1 + 32], xm1
    movu    m0, [r2 + r5]
    movu    xm1, [r2 + r5 + 32]
    movu    [r0 + r6], m0
    movu    [r0 + r6 + 32], xm1
    dec     r4d
    lea     r2, [r2 + 4 * r3]
    lea     r0, [r0 + 4 * r1]
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W24_H4_avx 24, 32
BLOCKCOPY_SS_W24_H4_avx 24, 64

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W32_H4 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 4
    mov     r4d, %2/4
    add     r1, r1
    add     r3, r3
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 48]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + 32], m2
    movu    [r0 + 48], m3

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 16]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 48]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 16], m1
    movu    [r0 + r1 + 32], m2
    movu    [r0 + r1 + 48], m3

    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 48]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + 32], m2
    movu    [r0 + 48], m3

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 16]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 48]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 16], m1
    movu    [r0 + r1 + 32], m2
    movu    [r0 + r1 + 48], m3

    dec     r4d
    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W32_H4 32, 8
BLOCKCOPY_SS_W32_H4 32, 16
BLOCKCOPY_SS_W32_H4 32, 24
BLOCKCOPY_SS_W32_H4 32, 32
BLOCKCOPY_SS_W32_H4 32, 64

BLOCKCOPY_SS_W32_H4 32, 48
;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W32_H4_avx 2
INIT_YMM avx
cglobal blockcopy_ss_%1x%2, 4, 7, 4

    mov    r4d, %2/4
    add    r1, r1
    add    r3, r3
    lea    r5, [3 * r1]
    lea    r6, [3 * r3]

.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 32]

    movu    [r0], m0
    movu    [r0 + 32], m1

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 32]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 32], m1

    movu    m0, [r2 + 2 * r3]
    movu    m1, [r2 + 2 * r3 + 32]

    movu    [r0 + 2 * r1], m0
    movu    [r0 + 2 * r1 + 32], m1

    movu    m0, [r2 + r6]
    movu    m1, [r2 + r6 + 32]

    movu    [r0 + r5], m0
    movu    [r0 + r5 + 32], m1

    dec     r4d
    lea     r2, [r2 + 4 * r3]
    lea     r0, [r0 + 4 * r1]
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W32_H4_avx 32,  8
BLOCKCOPY_SS_W32_H4_avx 32, 16
BLOCKCOPY_SS_W32_H4_avx 32, 24
BLOCKCOPY_SS_W32_H4_avx 32, 32
BLOCKCOPY_SS_W32_H4_avx 32, 48
BLOCKCOPY_SS_W32_H4_avx 32, 64

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W48_H2 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 6
    mov     r4d, %2/4
    add     r1, r1
    add     r3, r3
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 48]
    movu    m4, [r2 + 64]
    movu    m5, [r2 + 80]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + 32], m2
    movu    [r0 + 48], m3
    movu    [r0 + 64], m4
    movu    [r0 + 80], m5

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 16]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 48]
    movu    m4, [r2 + r3 + 64]
    movu    m5, [r2 + r3 + 80]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 16], m1
    movu    [r0 + r1 + 32], m2
    movu    [r0 + r1 + 48], m3
    movu    [r0 + r1 + 64], m4
    movu    [r0 + r1 + 80], m5

    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 48]
    movu    m4, [r2 + 64]
    movu    m5, [r2 + 80]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + 32], m2
    movu    [r0 + 48], m3
    movu    [r0 + 64], m4
    movu    [r0 + 80], m5

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 16]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 48]
    movu    m4, [r2 + r3 + 64]
    movu    m5, [r2 + r3 + 80]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 16], m1
    movu    [r0 + r1 + 32], m2
    movu    [r0 + r1 + 48], m3
    movu    [r0 + r1 + 64], m4
    movu    [r0 + r1 + 80], m5

    dec     r4d
    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]
    jnz     .loop
RET
%endmacro

BLOCKCOPY_SS_W48_H2 48, 64
;-----------------------------------------------------------------------------
; void blockcopy_ss_48x64(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
INIT_YMM avx
cglobal blockcopy_ss_48x64, 4, 7, 6

    mov    r4d, 64/4
    add    r1, r1
    add    r3, r3
    lea    r5, [3 * r3]
    lea    r6, [3 * r1]

.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 32]
    movu    m2, [r2 + 64]

    movu    [r0], m0
    movu    [r0 + 32], m1
    movu    [r0 + 64], m2

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 32]
    movu    m2, [r2 + r3 + 64]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 32], m1
    movu    [r0 + r1 + 64], m2

    movu    m0, [r2 + 2 * r3]
    movu    m1, [r2 + 2 * r3 + 32]
    movu    m2, [r2 + 2 * r3 + 64]

    movu    [r0 + 2 * r1], m0
    movu    [r0 + 2 * r1 + 32], m1
    movu    [r0 + 2 * r1 + 64], m2

    movu    m0, [r2 + r5]
    movu    m1, [r2 + r5 + 32]
    movu    m2, [r2 + r5 + 64]

    movu    [r0 + r6], m0
    movu    [r0 + r6 + 32], m1
    movu    [r0 + r6 + 64], m2

    dec     r4d
    lea     r2, [r2 + 4 * r3]
    lea     r0, [r0 + 4 * r1]
    jnz     .loop
    RET

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W64_H4 2
INIT_XMM sse2
cglobal blockcopy_ss_%1x%2, 4, 5, 6, dst, dstStride, src, srcStride
    mov     r4d, %2/4
    add     r1, r1
    add     r3, r3
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 48]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + 32], m2
    movu    [r0 + 48], m3

    movu    m0,    [r2 + 64]
    movu    m1,    [r2 + 80]
    movu    m2,    [r2 + 96]
    movu    m3,    [r2 + 112]

    movu    [r0 + 64], m0
    movu    [r0 + 80], m1
    movu    [r0 + 96], m2
    movu    [r0 + 112], m3

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 16]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 48]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 16], m1
    movu    [r0 + r1 + 32], m2
    movu    [r0 + r1 + 48], m3

    movu    m0, [r2 + r3 + 64]
    movu    m1, [r2 + r3 + 80]
    movu    m2, [r2 + r3 + 96]
    movu    m3, [r2 + r3 + 112]

    movu    [r0 + r1 + 64], m0
    movu    [r0 + r1 + 80], m1
    movu    [r0 + r1 + 96], m2
    movu    [r0 + r1 + 112], m3

    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]

    movu    m0, [r2]
    movu    m1, [r2 + 16]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 48]

    movu    [r0], m0
    movu    [r0 + 16], m1
    movu    [r0 + 32], m2
    movu    [r0 + 48], m3

    movu    m0,    [r2 + 64]
    movu    m1,    [r2 + 80]
    movu    m2,    [r2 + 96]
    movu    m3,    [r2 + 112]

    movu    [r0 + 64], m0
    movu    [r0 + 80], m1
    movu    [r0 + 96], m2
    movu    [r0 + 112], m3

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 16]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 48]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 16], m1
    movu    [r0 + r1 + 32], m2
    movu    [r0 + r1 + 48], m3

    movu    m0, [r2 + r3 + 64]
    movu    m1, [r2 + r3 + 80]
    movu    m2, [r2 + r3 + 96]
    movu    m3, [r2 + r3 + 112]

    movu    [r0 + r1 + 64], m0
    movu    [r0 + r1 + 80], m1
    movu    [r0 + r1 + 96], m2
    movu    [r0 + r1 + 112], m3

    dec     r4d
    lea     r2, [r2 + 2 * r3]
    lea     r0, [r0 + 2 * r1]
    jnz     .loop

    RET
%endmacro

BLOCKCOPY_SS_W64_H4 64, 16
BLOCKCOPY_SS_W64_H4 64, 32
BLOCKCOPY_SS_W64_H4 64, 48
BLOCKCOPY_SS_W64_H4 64, 64

;-----------------------------------------------------------------------------
; void blockcopy_ss_%1x%2(int16_t* dst, intptr_t dstStride, const int16_t* src, intptr_t srcStride)
;-----------------------------------------------------------------------------
%macro BLOCKCOPY_SS_W64_H4_avx 2
INIT_YMM avx
cglobal blockcopy_ss_%1x%2, 4, 7, 4, dst, dstStride, src, srcStride
    mov     r4d, %2/4
    add     r1, r1
    add     r3, r3
    lea     r5, [3 * r1]
    lea     r6, [3 * r3]
.loop:
    movu    m0, [r2]
    movu    m1, [r2 + 32]
    movu    m2, [r2 + 64]
    movu    m3, [r2 + 96]

    movu    [r0], m0
    movu    [r0 + 32], m1
    movu    [r0 + 64], m2
    movu    [r0 + 96], m3

    movu    m0, [r2 + r3]
    movu    m1, [r2 + r3 + 32]
    movu    m2, [r2 + r3 + 64]
    movu    m3, [r2 + r3 + 96]

    movu    [r0 + r1], m0
    movu    [r0 + r1 + 32], m1
    movu    [r0 + r1 + 64], m2
    movu    [r0 + r1 + 96], m3

    movu    m0, [r2 + 2 * r3]
    movu    m1, [r2 + 2 * r3 + 32]
    movu    m2, [r2 + 2 * r3 + 64]
    movu    m3, [r2 + 2 * r3 + 96]

    movu    [r0 + 2 * r1], m0
    movu    [r0 + 2 * r1 + 32], m1
    movu    [r0 + 2 * r1 + 64], m2
    movu    [r0 + 2 * r1 + 96], m3

    movu    m0, [r2 + r6]
    movu    m1, [r2 + r6 + 32]
    movu    m2, [r2 + r6 + 64]
    movu    m3, [r2 + r6 + 96]
    lea     r2, [r2 + 4 * r3]

    movu    [r0 + r5], m0
    movu    [r0 + r5 + 32], m1
    movu    [r0 + r5 + 64], m2
    movu    [r0 + r5 + 96], m3
    lea     r0, [r0 + 4 * r1]

    dec     r4d
    jnz     .loop
    RET
%endmacro

BLOCKCOPY_SS_W64_H4_avx 64, 16
BLOCKCOPY_SS_W64_H4_avx 64, 32
BLOCKCOPY_SS_W64_H4_avx 64, 48
BLOCKCOPY_SS_W64_H4_avx 64, 64

;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shr(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy2Dto1D_shr_4, 3, 4, 4
    add             r2d, r2d
    movd            m0, r3m
    pcmpeqw         m1, m1
    psllw           m1, m0
    psraw           m1, 1

    ; register alloc
    ; r0 - dst
    ; r1 - src
    ; r2 - srcStride
    ; m0 - shift
    ; m1 - word [-round]

    ; Row 0-3
    movh            m2, [r1]
    movhps          m2, [r1 + r2]
    lea             r1, [r1 + r2 * 2]
    movh            m3, [r1]
    movhps          m3, [r1 + r2]
    psubw           m2, m1
    psubw           m3, m1
    psraw           m2, m0
    psraw           m3, m0
    mova            [r0 + 0 * mmsize], m2
    mova            [r0 + 1 * mmsize], m3
    RET


;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shr(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy2Dto1D_shr_8, 3, 5, 4
    add             r2d, r2d
    movd            m0, r3m
    pcmpeqw         m1, m1
    psllw           m1, m0
    psraw           m1, 1
    mov             r3d, 8/4
    lea             r4, [r2 * 3]

    ; register alloc
    ; r0 - dst
    ; r1 - src
    ; r2 - srcStride
    ; r3 - loop counter
    ; r4 - stride * 3
    ; m0 - shift
    ; m1 - word [-round]

.loop:
    ; Row 0-1
    mova            m2, [r1]
    mova            m3, [r1 + r2]
    psubw           m2, m1
    psubw           m3, m1
    psraw           m2, m0
    psraw           m3, m0
    mova            [r0 + 0 * mmsize], m2
    mova            [r0 + 1 * mmsize], m3

    ; Row 2-3
    mova            m2, [r1 + r2 * 2]
    mova            m3, [r1 + r4]
    psubw           m2, m1
    psubw           m3, m1
    psraw           m2, m0
    psraw           m3, m0
    mova            [r0 + 2 * mmsize], m2
    mova            [r0 + 3 * mmsize], m3

    add             r0, 4 * mmsize
    lea             r1, [r1 + r2 * 4]
    dec             r3d
    jnz            .loop
    RET

INIT_YMM avx2
cglobal cpy2Dto1D_shr_8, 3, 4, 4
    add        r2d, r2d
    movd       xm0, r3m
    pcmpeqw    m1, m1
    psllw      m1, xm0
    psraw      m1, 1
    lea        r3, [r2 * 3]

    ; Row 0-3
    movu           xm2, [r1]
    vinserti128    m2, m2, [r1 + r2], 1
    movu           xm3, [r1 + 2 * r2]
    vinserti128    m3, m3, [r1 + r3], 1
    psubw          m2, m1
    psraw          m2, xm0
    psubw          m3, m1
    psraw          m3, xm0
    movu           [r0], m2
    movu           [r0 + 32], m3

    ; Row 4-7
    lea            r1, [r1 + 4 * r2]
    movu           xm2, [r1]
    vinserti128    m2, m2, [r1 + r2], 1
    movu           xm3, [r1 + 2 * r2]
    vinserti128    m3, m3, [r1 + r3], 1
    psubw          m2, m1
    psraw          m2, xm0
    psubw          m3, m1
    psraw          m3, xm0
    movu           [r0 + 64], m2
    movu           [r0 + 96], m3
    RET


;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shr(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy2Dto1D_shr_16, 3, 4, 4
    add             r2d, r2d
    movd            m0, r3m
    pcmpeqw         m1, m1
    psllw           m1, m0
    psraw           m1, 1
    mov             r3d, 16/2

    ; register alloc
    ; r0 - dst
    ; r1 - src
    ; r2 - srcStride
    ; r3 - loop counter
    ; m0 - shift
    ; m1 - word [-round]

.loop:
    ; Row 0
    mova            m2, [r1 + 0 * mmsize]
    mova            m3, [r1 + 1 * mmsize]
    psubw           m2, m1
    psubw           m3, m1
    psraw           m2, m0
    psraw           m3, m0
    mova            [r0 + 0 * mmsize], m2
    mova            [r0 + 1 * mmsize], m3

    ; Row 1
    mova            m2, [r1 + r2 + 0 * mmsize]
    mova            m3, [r1 + r2 + 1 * mmsize]
    psubw           m2, m1
    psubw           m3, m1
    psraw           m2, m0
    psraw           m3, m0
    mova            [r0 + 2 * mmsize], m2
    mova            [r0 + 3 * mmsize], m3

    add             r0, 4 * mmsize
    lea             r1, [r1 + r2 * 2]
    dec             r3d
    jnz            .loop
    RET

INIT_YMM avx2
cglobal cpy2Dto1D_shr_16, 4, 5, 4
    add        r2d, r2d
    movd       xm0, r3d
    pcmpeqw    m1, m1
    psllw      m1, xm0
    psraw      m1, 1
    lea        r3, [r2 * 3]
    mov        r4d, 16/8

.loop:
    ; Row 0-1
    movu       m2, [r1]
    movu       m3, [r1 + r2]
    psubw      m2, m1
    psraw      m2, xm0
    psubw      m3, m1
    psraw      m3, xm0
    movu       [r0 + 0 * mmsize], m2
    movu       [r0 + 1 * mmsize], m3

    ; Row 2-3
    movu       m2, [r1 + 2 * r2]
    movu       m3, [r1 + r3]
    psubw      m2, m1
    psraw      m2, xm0
    psubw      m3, m1
    psraw      m3, xm0
    movu       [r0 + 2 * mmsize], m2
    movu       [r0 + 3 * mmsize], m3

    ; Row 4-5
    lea        r1, [r1 + 4 * r2]
    movu       m2, [r1]
    movu       m3, [r1 + r2]
    psubw      m2, m1
    psraw      m2, xm0
    psubw      m3, m1
    psraw      m3, xm0
    movu       [r0 + 4 * mmsize], m2
    movu       [r0 + 5 * mmsize], m3

    ; Row 6-7
    movu       m2, [r1 + 2 * r2]
    movu       m3, [r1 + r3]
    psubw      m2, m1
    psraw      m2, xm0
    psubw      m3, m1
    psraw      m3, xm0
    movu       [r0 + 6 * mmsize], m2
    movu       [r0 + 7 * mmsize], m3

    add        r0, 8 * mmsize
    lea        r1, [r1 + 4 * r2]
    dec        r4d
    jnz        .loop
    RET


;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shr(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy2Dto1D_shr_32, 3, 4, 6
    add             r2d, r2d
    movd            m0, r3m
    pcmpeqw         m1, m1
    psllw           m1, m0
    psraw           m1, 1
    mov             r3d, 32/1

    ; register alloc
    ; r0 - dst
    ; r1 - src
    ; r2 - srcStride
    ; r3 - loop counter
    ; m0 - shift
    ; m1 - word [-round]

.loop:
    ; Row 0
    mova            m2, [r1 + 0 * mmsize]
    mova            m3, [r1 + 1 * mmsize]
    mova            m4, [r1 + 2 * mmsize]
    mova            m5, [r1 + 3 * mmsize]
    psubw           m2, m1
    psubw           m3, m1
    psubw           m4, m1
    psubw           m5, m1
    psraw           m2, m0
    psraw           m3, m0
    psraw           m4, m0
    psraw           m5, m0
    mova            [r0 + 0 * mmsize], m2
    mova            [r0 + 1 * mmsize], m3
    mova            [r0 + 2 * mmsize], m4
    mova            [r0 + 3 * mmsize], m5

    add             r0, 4 * mmsize
    add             r1, r2
    dec             r3d
    jnz            .loop
    RET

INIT_YMM avx2
cglobal cpy2Dto1D_shr_32, 4, 5, 4
    add        r2d, r2d
    movd       xm0, r3d
    pcmpeqw    m1, m1
    psllw      m1, xm0
    psraw      m1, 1
    lea        r3, [r2 * 3]
    mov        r4d, 32/4

.loop:
    ; Row 0
    movu       m2, [r1]
    movu       m3, [r1 + 32]
    psubw      m2, m1
    psraw      m2, xm0
    psubw      m3, m1
    psraw      m3, xm0
    movu       [r0 + 0 * mmsize], m2
    movu       [r0 + 1 * mmsize], m3

    ; Row 1
    movu       m2, [r1 + r2]
    movu       m3, [r1 + r2 + 32]
    psubw      m2, m1
    psraw      m2, xm0
    psubw      m3, m1
    psraw      m3, xm0
    movu       [r0 + 2 * mmsize], m2
    movu       [r0 + 3 * mmsize], m3

    ; Row 2
    movu       m2, [r1 + 2 * r2]
    movu       m3, [r1 + 2 * r2 + 32]
    psubw      m2, m1
    psraw      m2, xm0
    psubw      m3, m1
    psraw      m3, xm0
    movu       [r0 + 4 * mmsize], m2
    movu       [r0 + 5 * mmsize], m3

    ; Row 3
    movu       m2, [r1 + r3]
    movu       m3, [r1 + r3 + 32]
    psubw      m2, m1
    psraw      m2, xm0
    psubw      m3, m1
    psraw      m3, xm0
    movu       [r0 + 6 * mmsize], m2
    movu       [r0 + 7 * mmsize], m3

    add        r0, 8 * mmsize
    lea        r1, [r1 + 4 * r2]
    dec        r4d
    jnz        .loop
    RET

;--------------------------------------------------------------------------------------
; void cpy1Dto2D_shl(int16_t* dst, const int16_t* src, intptr_t dstStride, int shift)
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy1Dto2D_shl_4, 3, 3, 3
    add         r2d, r2d
    movd        m0, r3m

    ; Row 0-3
    mova        m1, [r1 + 0 * mmsize]
    mova        m2, [r1 + 1 * mmsize]
    psllw       m1, m0
    psllw       m2, m0
    movh        [r0], m1
    movhps      [r0 + r2], m1
    movh        [r0 + r2 * 2], m2
    lea         r2, [r2 * 3]
    movhps      [r0 + r2], m2
    RET


INIT_YMM avx2
cglobal cpy1Dto2D_shl_4, 3, 3, 2
    add         r2d, r2d
    movd        xm0, r3m

    ; Row 0-3
    movu        m1, [r1]
    psllw       m1, xm0
    vextracti128 xm0, m1, 1
    movq        [r0], xm1
    movhps      [r0 + r2], xm1
    lea         r0, [r0 + r2 * 2]
    movq        [r0], xm0
    movhps      [r0 + r2], xm0
    RET


;--------------------------------------------------------------------------------------
; void cpy1Dto2D_shl(int16_t* dst, const int16_t* src, intptr_t dstStride, int shift)
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy1Dto2D_shl_8, 3, 4, 5
    add         r2d, r2d
    movd        m0, r3m
    lea         r3, [r2 * 3]

    ; Row 0-3
    mova        m1, [r1 + 0 * mmsize]
    mova        m2, [r1 + 1 * mmsize]
    mova        m3, [r1 + 2 * mmsize]
    mova        m4, [r1 + 3 * mmsize]
    psllw       m1, m0
    psllw       m2, m0
    psllw       m3, m0
    psllw       m4, m0
    mova        [r0], m1
    mova        [r0 + r2], m2
    mova        [r0 + r2 * 2], m3
    mova        [r0 + r3], m4
    lea         r0, [r0 + r2 * 4]

    ; Row 4-7
    mova        m1, [r1 + 4 * mmsize]
    mova        m2, [r1 + 5 * mmsize]
    mova        m3, [r1 + 6 * mmsize]
    mova        m4, [r1 + 7 * mmsize]
    psllw       m1, m0
    psllw       m2, m0
    psllw       m3, m0
    psllw       m4, m0
    mova        [r0], m1
    mova        [r0 + r2], m2
    mova        [r0 + r2 * 2], m3
    mova        [r0 + r3], m4
    RET


INIT_YMM avx2
cglobal cpy1Dto2D_shl_8, 3, 4, 3
    add         r2d, r2d
    movd        xm0, r3m
    lea         r3, [r2 * 3]

    ; Row 0-3
    movu        m1, [r1 + 0 * mmsize]
    movu        m2, [r1 + 1 * mmsize]
    psllw       m1, xm0
    psllw       m2, xm0
    movu        [r0], xm1
    vextracti128 [r0 + r2], m1, 1
    movu        [r0 + r2 * 2], xm2
    vextracti128 [r0 + r3], m2, 1

    ; Row 4-7
    movu        m1, [r1 + 2 * mmsize]
    movu        m2, [r1 + 3 * mmsize]
    lea         r0, [r0 + r2 * 4]
    psllw       m1, xm0
    psllw       m2, xm0
    movu        [r0], xm1
    vextracti128 [r0 + r2], m1, 1
    movu        [r0 + r2 * 2], xm2
    vextracti128 [r0 + r3], m2, 1
    RET


;--------------------------------------------------------------------------------------
; void cpy1Dto2D_shl(int16_t* dst, const int16_t* src, intptr_t dstStride, int shift)
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy1Dto2D_shl_16, 3, 4, 5
    add         r2d, r2d
    movd        m0, r3m
    mov         r3d, 16/4

.loop:
    ; Row 0-1
    mova        m1, [r1 + 0 * mmsize]
    mova        m2, [r1 + 1 * mmsize]
    mova        m3, [r1 + 2 * mmsize]
    mova        m4, [r1 + 3 * mmsize]
    psllw       m1, m0
    psllw       m2, m0
    psllw       m3, m0
    psllw       m4, m0
    mova        [r0], m1
    mova        [r0 + 16], m2
    mova        [r0 + r2], m3
    mova        [r0 + r2 + 16], m4

    ; Row 2-3
    mova        m1, [r1 + 4 * mmsize]
    mova        m2, [r1 + 5 * mmsize]
    mova        m3, [r1 + 6 * mmsize]
    mova        m4, [r1 + 7 * mmsize]
    lea         r0, [r0 + r2 * 2]
    psllw       m1, m0
    psllw       m2, m0
    psllw       m3, m0
    psllw       m4, m0
    mova        [r0], m1
    mova        [r0 + 16], m2
    mova        [r0 + r2], m3
    mova        [r0 + r2 + 16], m4

    add         r1, 8 * mmsize
    lea         r0, [r0 + r2 * 2]
    dec         r3d
    jnz        .loop
    RET


INIT_YMM avx2
cglobal cpy1Dto2D_shl_16, 3, 5, 3
    add         r2d, r2d
    movd        xm0, r3m
    mov         r3d, 16/4
    lea         r4, [r2 * 3]

.loop:
    ; Row 0-1
    movu        m1, [r1 + 0 * mmsize]
    movu        m2, [r1 + 1 * mmsize]
    psllw       m1, xm0
    psllw       m2, xm0
    movu        [r0], m1
    movu        [r0 + r2], m2

    ; Row 2-3
    movu        m1, [r1 + 2 * mmsize]
    movu        m2, [r1 + 3 * mmsize]
    psllw       m1, xm0
    psllw       m2, xm0
    movu        [r0 + r2 * 2], m1
    movu        [r0 + r4], m2

    add         r1, 4 * mmsize
    lea         r0, [r0 + r2 * 4]
    dec         r3d
    jnz        .loop
    RET


;--------------------------------------------------------------------------------------
; void cpy1Dto2D_shl(int16_t* dst, const int16_t* src, intptr_t dstStride, int shift)
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy1Dto2D_shl_32, 3, 4, 5
    add         r2d, r2d
    movd        m0, r3m
    mov         r3d, 32/2

.loop:
    ; Row 0
    mova        m1, [r1 + 0 * mmsize]
    mova        m2, [r1 + 1 * mmsize]
    mova        m3, [r1 + 2 * mmsize]
    mova        m4, [r1 + 3 * mmsize]
    psllw       m1, m0
    psllw       m2, m0
    psllw       m3, m0
    psllw       m4, m0
    mova        [r0 + 0 * mmsize], m1
    mova        [r0 + 1 * mmsize], m2
    mova        [r0 + 2 * mmsize], m3
    mova        [r0 + 3 * mmsize], m4

    ; Row 1
    mova        m1, [r1 + 4 * mmsize]
    mova        m2, [r1 + 5 * mmsize]
    mova        m3, [r1 + 6 * mmsize]
    mova        m4, [r1 + 7 * mmsize]
    psllw       m1, m0
    psllw       m2, m0
    psllw       m3, m0
    psllw       m4, m0
    mova        [r0 + r2 + 0 * mmsize], m1
    mova        [r0 + r2 + 1 * mmsize], m2
    mova        [r0 + r2 + 2 * mmsize], m3
    mova        [r0 + r2 + 3 * mmsize], m4

    add         r1, 8 * mmsize
    lea         r0, [r0 + r2 * 2]
    dec         r3d
    jnz        .loop
    RET


INIT_YMM avx2
cglobal cpy1Dto2D_shl_32, 3, 4, 5
    add         r2d, r2d
    movd        xm0, r3m
    mov         r3d, 32/2

.loop:
    ; Row 0-1
    movu        m1, [r1 + 0 * mmsize]
    movu        m2, [r1 + 1 * mmsize]
    movu        m3, [r1 + 2 * mmsize]
    movu        m4, [r1 + 3 * mmsize]
    psllw       m1, xm0
    psllw       m2, xm0
    psllw       m3, xm0
    psllw       m4, xm0
    movu        [r0], m1
    movu        [r0 + mmsize], m2
    movu        [r0 + r2], m3
    movu        [r0 + r2 + mmsize], m4

    add         r1, 4 * mmsize
    lea         r0, [r0 + r2 * 2]
    dec         r3d
    jnz        .loop
    RET


;--------------------------------------------------------------------------------------
; uint32_t copy_cnt(int16_t* dst, const int16_t* src, intptr_t srcStride);
;--------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal copy_cnt_4, 3,3,3
    add         r2d, r2d
    pxor        m2, m2

    ; row 0 & 1
    movh        m0, [r1]
    movhps      m0, [r1 + r2]
    mova        [r0], m0

    ; row 2 & 3
    movh        m1, [r1 + r2 * 2]
    lea         r2, [r2 * 3]
    movhps      m1, [r1 + r2]
    mova        [r0 + 16], m1

    packsswb    m0, m1
    pcmpeqb     m0, m2

    ; get count
    ; CHECK_ME: Intel documents said POPCNT is SSE4.2 instruction, but just implement after Nehalem
%if 0
    pmovmskb    eax, m0
    not         ax
    popcnt      ax, ax
%else
    mova        m1, [pb_1]
    paddb       m0, m1
    psadbw      m0, m2
    pshufd      m1, m0, 2
    paddw       m0, m1
    movd        eax, m0
%endif
    RET


;--------------------------------------------------------------------------------------
; uint32_t copy_cnt(int16_t* dst, const int16_t* src, intptr_t srcStride);
;--------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal copy_cnt_8, 3,3,6
    add         r2d, r2d
    pxor        m4, m4
    pxor        m5, m5

   ; row 0 & 1
    movu         m0, [r1]
    movu        m1, [r1 + r2]
    movu        [r0], m0
    movu        [r0 + 16], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    ; row 2 & 3
    lea         r1, [r1 + 2 * r2]
    movu        m0, [r1]
    movu        m1, [r1 + r2]
    movu        [r0 + 32], m0
    movu        [r0 + 48], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    ; row 4 & 5
    lea         r1, [r1 + 2 * r2]
    movu        m0, [r1]
    movu        m1, [r1 + r2]
    movu        [r0 + 64], m0
    movu        [r0 + 80], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    ; row 6 & 7
    lea         r1, [r1 + 2 * r2]
    movu        m0, [r1]
    movu        m1, [r1 + r2]
    movu        [r0 + 96], m0
    movu        [r0 + 112], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    ; get count
    mova        m0, [pb_4]
    paddb       m5, m0
    psadbw      m5, m4
    pshufd      m0, m5, 2
    paddw       m5, m0
    movd        eax, m5
     RET


INIT_YMM avx2
cglobal copy_cnt_8, 3,4,5
    add         r2d, r2d
    lea         r3, [r2 * 3]

    ; row 0 - 1
    movu        xm0, [r1]
    vinserti128 m0, m0, [r1 + r2], 1
    movu        [r0], m0

    ; row 2 - 3
    movu        xm1, [r1 + r2 * 2]
    vinserti128 m1, m1, [r1 + r3], 1
    movu        [r0 + 32], m1
    lea         r1,  [r1 + r2 * 4]

    ; row 4 - 5
    movu        xm2, [r1]
    vinserti128 m2, m2, [r1 + r2], 1
    movu        [r0 + 64], m2

    ; row 6 - 7
    movu        xm3, [r1 + r2 * 2]
    vinserti128 m3, m3, [r1 + r3], 1
    movu        [r0 + 96], m3

    ; get count
    xorpd        m4, m4
    vpacksswb    m0, m1
    vpacksswb    m2, m3
    pminub       m0, [pb_1]
    pminub       m2, [pb_1]
    paddb        m0, m2
    vextracti128 xm1, m0, 1
    paddb        xm0, xm1
    psadbw       xm0, xm4
    movhlps      xm1, xm0
    paddd        xm0, xm1
    movd         eax, xm0
    RET


;--------------------------------------------------------------------------------------
; uint32_t copy_cnt(int16_t* dst, const int16_t* src, intptr_t srcStride);
;--------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal copy_cnt_16, 3,4,6
     add         r2d, r2d
     mov         r3d, 4
     pxor        m4, m4
     pxor        m5, m5

.loop
    ; row 0
    movu        m0, [r1]
    movu        m1, [r1 + 16]
    movu        [r0], m0
    movu        [r0 + 16], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

     ; row 1
    movu        m0, [r1 + r2]
    movu        m1, [r1 + r2 + 16]
    movu        [r0 + 32], m0
    movu        [r0 + 48], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    ; row 2
    movu        m0, [r1 + 2 * r2]
    movu        m1, [r1 + 2 * r2 + 16]
    movu        [r0 + 64], m0
    movu        [r0 + 80], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    ; row 3
    lea         r1, [r1 + 2 * r2]
    movu        m0, [r1 + r2]
    movu        m1, [r1 + r2 + 16]
    movu        [r0 + 96], m0
    movu        [r0 + 112], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    add         r0, 128
    lea         r1, [r1 + 2 * r2]
     dec         r3d
     jnz        .loop

    mova        m0, [pb_16]
    paddb       m5, m0
    psadbw      m5, m4
    pshufd      m0, m5, 2
    paddw       m5, m0
    movd        eax, m5
     RET


INIT_YMM avx2
cglobal copy_cnt_16, 3, 5, 5
    add         r2d, r2d
    lea         r3,  [r2 * 3]
    mov         r4d, 16/4

    mova        m3, [pb_1]
    xorpd       m4, m4

.loop:
    ; row 0 - 1
    movu        m0, [r1]
    movu        [r0], m0
    movu        m1, [r1 + r2]
    movu        [r0 + 32], m1

    packsswb    m0, m1
    pminub      m0, m3

    ; row 2 - 3
    movu        m1, [r1 + r2 * 2]
    movu        [r0 + 64], m1
    movu        m2, [r1 + r3]
    movu        [r0 + 96], m2

    packsswb    m1, m2
    pminub      m1, m3
    paddb       m0, m1
    paddb       m4, m0

    add         r0, 128
    lea         r1, [r1 + 4 * r2]
    dec         r4d
    jnz         .loop

    ; get count
    xorpd        m0,  m0
    vextracti128 xm1, m4, 1
    paddb        xm4, xm1
    psadbw       xm4, xm0
    movhlps      xm1, xm4
    paddd        xm4, xm1
    movd         eax, xm4
    RET

;--------------------------------------------------------------------------------------
; uint32_t copy_cnt(int32_t* dst, const int16_t* src, intptr_t stride);
;--------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal copy_cnt_32, 3,4,6
    add         r2d, r2d
    mov         r3d, 16
    pxor        m4, m4
    pxor        m5, m5

.loop
    ; row 0
    movu        m0, [r1]
    movu        m1, [r1 + 16]
    movu        [r0], m0
    movu        [r0 + 16], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    movu        m0, [r1 + 32]
    movu        m1, [r1 + 48]
    movu        [r0 + 32], m0
    movu        [r0 + 48], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    ; row 1
    movu        m0, [r1 + r2]
    movu        m1, [r1 + r2 + 16]
    movu        [r0 + 64], m0
    movu        [r0 + 80], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    movu        m0, [r1 + r2 + 32]
    movu        m1, [r1 + r2 + 48]
    movu        [r0 + 96], m0
    movu        [r0 + 112], m1

    packsswb    m0, m1
    pcmpeqb     m0, m4
    paddb       m5, m0

    add         r0, 128
    lea         r1, [r1 + 2 * r2]
     dec         r3d
     jnz        .loop

     ; get count
    mova        m0, [pb_64]
    paddb       m5, m0
    psadbw      m5, m4
    pshufd      m0, m5, 2
    paddw       m5, m0
    movd        eax, m5
     RET


INIT_YMM avx2
cglobal copy_cnt_32, 3, 5, 5
    add         r2d, r2d
    mov         r3d, 32/2

    mova        m3, [pb_1]
    xorpd       m4, m4

.loop:
    ; row 0
    movu        m0, [r1]
    movu        [r0], m0
    movu        m1, [r1 + 32]
    movu        [r0 + 32], m1

    packsswb    m0, m1
    pminub      m0, m3

    ; row 1
    movu        m1, [r1 + r2]
    movu        [r0 + 64], m1
    movu        m2, [r1 + r2 + 32]
    movu        [r0 + 96], m2

    packsswb    m1, m2
    pminub      m1, m3
    paddb       m0, m1
    paddb       m4, m0

    add         r0, 128
    lea         r1, [r1 + 2 * r2]
    dec         r3d
    jnz         .loop

    ; get count
    xorpd        m0,  m0
    vextracti128 xm1, m4, 1
    paddb        xm4, xm1
    psadbw       xm4, xm0
    movhlps      xm1, xm4
    paddd        xm4, xm1
    movd         eax, xm4
    RET


;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shl(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy2Dto1D_shl_4, 4, 4, 4
    add             r2d, r2d
    movd            m0, r3d

    ; register alloc
    ; r0 - dst
    ; r1 - src
    ; r2 - srcStride
    ; m0 - shift

    ; Row 0-3
    movh            m2, [r1]
    movhps          m2, [r1 + r2]
    lea             r1, [r1 + r2 * 2]
    movh            m3, [r1]
    movhps          m3, [r1 + r2]
    psllw           m2, m0
    psllw           m3, m0
    mova            [r0 + 0 * mmsize], m2
    mova            [r0 + 1 * mmsize], m3

    RET


;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shl(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy2Dto1D_shl_8, 4, 5, 4
    add             r2d, r2d
    movd            m0, r3d
    mov             r3d, 8/4
    lea             r4, [r2 * 3]

    ; register alloc
    ; r0 - dst
    ; r1 - src
    ; r2 - srcStride
    ; r3 - loop counter
    ; r4 - stride * 3
    ; m0 - shift

.loop:
    ; Row 0, 1
    mova            m2, [r1]
    mova            m3, [r1 + r2]
    psllw           m2, m0
    psllw           m3, m0
    mova            [r0 + 0 * mmsize], m2
    mova            [r0 + 1 * mmsize], m3

    ; Row 2, 3
    mova            m2, [r1 + r2 * 2]
    mova            m3, [r1 + r4]
    psllw           m2, m0
    psllw           m3, m0
    mova            [r0 + 2 * mmsize], m2
    mova            [r0 + 3 * mmsize], m3

    add             r0, 4 * mmsize
    lea             r1, [r1 + r2 * 4]
    dec             r3d
    jnz            .loop
    RET

;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shl_8(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal cpy2Dto1D_shl_8, 4, 5, 2
    add     r2d, r2d
    movd    xm0, r3d
    lea     r4, [3 * r2]

    ; Row 0, 1
    movu           xm1, [r1]
    vinserti128    m1, m1, [r1 + r2], 1
    psllw          m1, xm0
    movu           [r0], m1

    ; Row 2, 3
    movu           xm1, [r1 + 2 * r2]
    vinserti128    m1, m1, [r1 + r4], 1
    psllw          m1, xm0
    movu           [r0 + 32], m1

    lea            r1, [r1 + 4 * r2]

    ; Row 4, 5
    movu           xm1, [r1]
    vinserti128    m1, m1, [r1 + r2], 1
    psllw          m1, xm0
    movu           [r0 + 64], m1

    ; Row 6, 7
    movu           xm1, [r1 + 2 * r2]
    vinserti128    m1, m1, [r1 + r4], 1
    psllw          m1, xm0
    movu           [r0 + 96], m1
    RET


;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shl(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy2Dto1D_shl_16, 4, 4, 4
    add             r2d, r2d
    movd            m0, r3d
    mov             r3d, 16/2

    ; register alloc
    ; r0 - dst
    ; r1 - src
    ; r2 - srcStride
    ; r3 - loop counter
    ; m0 - shift

.loop:
    ; Row 0
    mova            m2, [r1 + 0 * mmsize]
    mova            m3, [r1 + 1 * mmsize]
    psllw           m2, m0
    psllw           m3, m0
    mova            [r0 + 0 * mmsize], m2
    mova            [r0 + 1 * mmsize], m3

    ; Row 1
    mova            m2, [r1 + r2 + 0 * mmsize]
    mova            m3, [r1 + r2 + 1 * mmsize]
    psllw           m2, m0
    psllw           m3, m0
    mova            [r0 + 2 * mmsize], m2
    mova            [r0 + 3 * mmsize], m3

    add             r0, 4 * mmsize
    lea             r1, [r1 + r2 * 2]
    dec             r3d
    jnz            .loop
    RET

;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shl_16(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal cpy2Dto1D_shl_16, 3, 5, 3
    add    r2d, r2d
    movd   xm0, r3m
    mov    r3d, 16/4
    lea     r4, [r2 * 3]

.loop:
    ; Row 0-1
    movu     m1, [r1]
    movu     m2, [r1 + r2]
    psllw    m1, xm0
    psllw    m2, xm0
    movu     [r0 + 0 * mmsize], m1
    movu     [r0 + 1 * mmsize], m2

    ; Row 2-3
    movu     m1, [r1 + 2 * r2]
    movu     m2, [r1 + r4]
    psllw    m1, xm0
    psllw    m2, xm0
    movu     [r0 + 2 * mmsize], m1
    movu     [r0 + 3 * mmsize], m2

    add      r0, 4 * mmsize
    lea      r1, [r1 + r2 * 4]
    dec      r3d
    jnz      .loop
    RET

;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shl(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy2Dto1D_shl_32, 4, 4, 6
    add             r2d, r2d
    movd            m0, r3d
    mov             r3d, 32/1

    ; register alloc
    ; r0 - dst
    ; r1 - src
    ; r2 - srcStride
    ; r3 - loop counter
    ; m0 - shift

.loop:
    ; Row 0
    mova            m2, [r1 + 0 * mmsize]
    mova            m3, [r1 + 1 * mmsize]
    mova            m4, [r1 + 2 * mmsize]
    mova            m5, [r1 + 3 * mmsize]
    psllw           m2, m0
    psllw           m3, m0
    psllw           m4, m0
    psllw           m5, m0
    mova            [r0 + 0 * mmsize], m2
    mova            [r0 + 1 * mmsize], m3
    mova            [r0 + 2 * mmsize], m4
    mova            [r0 + 3 * mmsize], m5

    add             r0, 4 * mmsize
    add             r1, r2
    dec             r3d
    jnz            .loop
    RET

;--------------------------------------------------------------------------------------
; void cpy2Dto1D_shl_32(int16_t* dst, const int16_t* src, intptr_t srcStride, int shift);
;--------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal cpy2Dto1D_shl_32, 3, 5, 5
    add     r2d, r2d
    movd    xm0, r3m
    mov     r3d, 32/4
    lea     r4, [3 * r2]

.loop:
    ; Row 0-1
    movu     m1, [r1]
    movu     m2, [r1 + 32]
    movu     m3, [r1 + r2]
    movu     m4, [r1 + r2 + 32]

    psllw    m1, xm0
    psllw    m2, xm0
    psllw    m3, xm0
    psllw    m4, xm0
    movu     [r0], m1
    movu     [r0 + mmsize], m2
    movu     [r0 + 2 * mmsize], m3
    movu     [r0 + 3 * mmsize], m4

    ; Row 2-3
    movu     m1, [r1 + 2 * r2]
    movu     m2, [r1 + 2 * r2 + 32]
    movu     m3, [r1 + r4]
    movu     m4, [r1 + r4 + 32]

    psllw    m1, xm0
    psllw    m2, xm0
    psllw    m3, xm0
    psllw    m4, xm0
    movu     [r0 + 4 * mmsize], m1
    movu     [r0 + 5 * mmsize], m2
    movu     [r0 + 6 * mmsize], m3
    movu     [r0 + 7 * mmsize], m4

    add      r0, 8 * mmsize
    lea      r1, [r1 + r2 * 4]
    dec      r3d
    jnz      .loop
    RET

;--------------------------------------------------------------------------------------
; void cpy1Dto2D_shr(int16_t* dst, const int16_t* src, intptr_t dstStride, int shift)
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy1Dto2D_shr_4, 3, 3, 4
    add         r2d, r2d
    movd        m0, r3m
    pcmpeqw     m1, m1
    psllw       m1, m0
    psraw       m1, 1

    ; Row 0-3
    mova        m2, [r1 + 0 * mmsize]
    mova        m3, [r1 + 1 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psraw       m2, m0
    psraw       m3, m0
    movh        [r0], m2
    movhps      [r0 + r2], m2
    movh        [r0 + r2 * 2], m3
    lea         r2, [r2 * 3]
    movhps      [r0 + r2], m3
    RET


INIT_YMM avx2
cglobal cpy1Dto2D_shr_4, 3, 3, 3
    add         r2d, r2d
    movd        xm0, r3m
    pcmpeqw     m1, m1
    psllw       m1, xm0
    psraw       m1, 1

    ; Row 0-3
    movu        m2, [r1]
    psubw       m2, m1
    psraw       m2, xm0
    vextracti128 xm1, m2, 1
    movq        [r0], xm2
    movhps      [r0 + r2], xm2
    lea         r0, [r0 + r2 * 2]
    movq        [r0], xm1
    movhps      [r0 + r2], xm1
    RET


;--------------------------------------------------------------------------------------
; void cpy1Dto2D_shr(int16_t* dst, const int16_t* src, intptr_t dstStride, int shift)
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy1Dto2D_shr_8, 3, 4, 6
    add         r2d, r2d
    movd        m0, r3m
    pcmpeqw     m1, m1
    psllw       m1, m0
    psraw       m1, 1
    lea         r3, [r2 * 3]

    ; Row 0-3
    mova        m2, [r1 + 0 * mmsize]
    mova        m3, [r1 + 1 * mmsize]
    mova        m4, [r1 + 2 * mmsize]
    mova        m5, [r1 + 3 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psubw       m4, m1
    psubw       m5, m1
    psraw       m2, m0
    psraw       m3, m0
    psraw       m4, m0
    psraw       m5, m0
    mova        [r0], m2
    mova        [r0 + r2], m3
    mova        [r0 + r2 * 2], m4
    mova        [r0 + r3], m5

    ; Row 4-7
    mova        m2, [r1 + 4 * mmsize]
    mova        m3, [r1 + 5 * mmsize]
    mova        m4, [r1 + 6 * mmsize]
    mova        m5, [r1 + 7 * mmsize]
    lea         r0, [r0 + r2 * 4]
    psubw       m2, m1
    psubw       m3, m1
    psubw       m4, m1
    psubw       m5, m1
    psraw       m2, m0
    psraw       m3, m0
    psraw       m4, m0
    psraw       m5, m0
    mova        [r0], m2
    mova        [r0 + r2], m3
    mova        [r0 + r2 * 2], m4
    mova        [r0 + r3], m5
    RET


INIT_YMM avx2
cglobal cpy1Dto2D_shr_8, 3, 4, 4
    add         r2d, r2d
    movd        xm0, r3m
    pcmpeqw     m1, m1
    psllw       m1, xm0
    psraw       m1, 1
    lea         r3, [r2 * 3]

    ; Row 0-3
    movu        m2, [r1 + 0 * mmsize]
    movu        m3, [r1 + 1 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psraw       m2, xm0
    psraw       m3, xm0
    movu        [r0], xm2
    vextracti128 [r0 + r2], m2, 1
    movu        [r0 + r2 * 2], xm3
    vextracti128 [r0 + r3], m3, 1

    ; Row 4-7
    movu        m2, [r1 + 2 * mmsize]
    movu        m3, [r1 + 3 * mmsize]
    lea         r0, [r0 + r2 * 4]
    psubw       m2, m1
    psubw       m3, m1
    psraw       m2, xm0
    psraw       m3, xm0
    movu        [r0], xm2
    vextracti128 [r0 + r2], m2, 1
    movu        [r0 + r2 * 2], xm3
    vextracti128 [r0 + r3], m3, 1
    RET


;--------------------------------------------------------------------------------------
; void cpy1Dto2D_shr(int16_t* dst, const int16_t* src, intptr_t dstStride, int shift)
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy1Dto2D_shr_16, 3, 5, 6
    add         r2d, r2d
    movd        m0, r3m
    pcmpeqw     m1, m1
    psllw       m1, m0
    psraw       m1, 1
    mov         r3d, 16/4
    lea         r4, [r2 * 3]

.loop:
    ; Row 0-1
    mova        m2, [r1 + 0 * mmsize]
    mova        m3, [r1 + 1 * mmsize]
    mova        m4, [r1 + 2 * mmsize]
    mova        m5, [r1 + 3 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psubw       m4, m1
    psubw       m5, m1
    psraw       m2, m0
    psraw       m3, m0
    psraw       m4, m0
    psraw       m5, m0
    mova        [r0], m2
    mova        [r0 + mmsize], m3
    mova        [r0 + r2], m4
    mova        [r0 + r2 + mmsize], m5

    ; Row 2-3
    mova        m2, [r1 + 4 * mmsize]
    mova        m3, [r1 + 5 * mmsize]
    mova        m4, [r1 + 6 * mmsize]
    mova        m5, [r1 + 7 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psubw       m4, m1
    psubw       m5, m1
    psraw       m2, m0
    psraw       m3, m0
    psraw       m4, m0
    psraw       m5, m0
    mova        [r0 + r2 * 2], m2
    mova        [r0 + r2 * 2 + mmsize], m3
    mova        [r0 + r4], m4
    mova        [r0 + r4 + mmsize], m5

    add         r1, 8 * mmsize
    lea         r0, [r0 + r2 * 4]
    dec         r3d
    jnz        .loop
    RET


INIT_YMM avx2
cglobal cpy1Dto2D_shr_16, 3, 5, 4
    add         r2d, r2d
    movd        xm0, r3m
    pcmpeqw     m1, m1
    psllw       m1, xm0
    psraw       m1, 1
    mov         r3d, 16/4
    lea         r4, [r2 * 3]

.loop:
    ; Row 0-1
    movu        m2, [r1 + 0 * mmsize]
    movu        m3, [r1 + 1 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psraw       m2, xm0
    psraw       m3, xm0
    movu        [r0], m2
    movu        [r0 + r2], m3

    ; Row 2-3
    movu        m2, [r1 + 2 * mmsize]
    movu        m3, [r1 + 3 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psraw       m2, xm0
    psraw       m3, xm0
    movu        [r0 + r2 * 2], m2
    movu        [r0 + r4], m3

    add         r1, 4 * mmsize
    lea         r0, [r0 + r2 * 4]
    dec         r3d
    jnz        .loop
    RET


;--------------------------------------------------------------------------------------
; void cpy1Dto2D_shr(int16_t* dst, const int16_t* src, intptr_t dstStride, int shift)
;--------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal cpy1Dto2D_shr_32, 3, 4, 6
    add         r2d, r2d
    movd        m0, r3m
    pcmpeqw     m1, m1
    psllw       m1, m0
    psraw       m1, 1
    mov         r3d, 32/2

.loop:
    ; Row 0
    mova        m2, [r1 + 0 * mmsize]
    mova        m3, [r1 + 1 * mmsize]
    mova        m4, [r1 + 2 * mmsize]
    mova        m5, [r1 + 3 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psubw       m4, m1
    psubw       m5, m1
    psraw       m2, m0
    psraw       m3, m0
    psraw       m4, m0
    psraw       m5, m0
    mova        [r0 + 0 * mmsize], m2
    mova        [r0 + 1 * mmsize], m3
    mova        [r0 + 2 * mmsize], m4
    mova        [r0 + 3 * mmsize], m5

    ; Row 1
    mova        m2, [r1 + 4 * mmsize]
    mova        m3, [r1 + 5 * mmsize]
    mova        m4, [r1 + 6 * mmsize]
    mova        m5, [r1 + 7 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psubw       m4, m1
    psubw       m5, m1
    psraw       m2, m0
    psraw       m3, m0
    psraw       m4, m0
    psraw       m5, m0
    mova        [r0 + r2 + 0 * mmsize], m2
    mova        [r0 + r2 + 1 * mmsize], m3
    mova        [r0 + r2 + 2 * mmsize], m4
    mova        [r0 + r2 + 3 * mmsize], m5

    add         r1, 8 * mmsize
    lea         r0, [r0 + r2 * 2]
    dec         r3d
    jnz        .loop
    RET


INIT_YMM avx2
cglobal cpy1Dto2D_shr_32, 3, 4, 6
    add         r2d, r2d
    movd        xm0, r3m
    pcmpeqw     m1, m1
    psllw       m1, xm0
    psraw       m1, 1
    mov         r3d, 32/2

.loop:
    ; Row 0-1
    movu        m2, [r1 + 0 * mmsize]
    movu        m3, [r1 + 1 * mmsize]
    movu        m4, [r1 + 2 * mmsize]
    movu        m5, [r1 + 3 * mmsize]
    psubw       m2, m1
    psubw       m3, m1
    psubw       m4, m1
    psubw       m5, m1
    psraw       m2, xm0
    psraw       m3, xm0
    psraw       m4, xm0
    psraw       m5, xm0
    movu        [r0], m2
    movu        [r0 + mmsize], m3
    movu        [r0 + r2], m4
    movu        [r0 + r2 + mmsize], m5

    add         r1, 4 * mmsize
    lea         r0, [r0 + r2 * 2]
    dec         r3d
    jnz        .loop
    RET
