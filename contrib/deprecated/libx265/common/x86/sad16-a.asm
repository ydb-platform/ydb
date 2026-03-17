;*****************************************************************************
;* sad16-a.asm: x86 high depth sad functions
;*****************************************************************************
;* Copyright (C) 2003-2013 x264 project
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Oskar Arvidsson <oskar@irock.se>
;*          Henrik Gramner <henrik@gramner.com>
;*          Dnyaneshwar Gorade <dnyaneshwar@multicorewareinc.com>
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
;*****************************************************************************

%include "x86inc.asm"
%include "x86util.asm"

SECTION .text

cextern pw_1

;=============================================================================
; SAD MMX
;=============================================================================

%macro SAD_INC_1x16P_MMX 0
    movu    m1, [r0+ 0]
    movu    m2, [r0+ 8]
    movu    m3, [r0+16]
    movu    m4, [r0+24]
    psubw   m1, [r2+ 0]
    psubw   m2, [r2+ 8]
    psubw   m3, [r2+16]
    psubw   m4, [r2+24]
    ABSW2   m1, m2, m1, m2, m5, m6
    ABSW2   m3, m4, m3, m4, m7, m5
    lea     r0, [r0+2*r1]
    lea     r2, [r2+2*r3]
    paddw   m1, m2
    paddw   m3, m4
  %if BIT_DEPTH <= 10
    paddw   m0, m1
    paddw   m0, m3
  %else
    paddw   m1, m3
    pmaddwd m1, [pw_1]
    paddd   m0, m1
  %endif
%endmacro

%macro SAD_INC_2x8P_MMX 0
    movu    m1, [r0+0]
    movu    m2, [r0+8]
    movu    m3, [r0+2*r1+0]
    movu    m4, [r0+2*r1+8]
    psubw   m1, [r2+0]
    psubw   m2, [r2+8]
    psubw   m3, [r2+2*r3+0]
    psubw   m4, [r2+2*r3+8]
    ABSW2   m1, m2, m1, m2, m5, m6
    ABSW2   m3, m4, m3, m4, m7, m5
    lea     r0, [r0+4*r1]
    lea     r2, [r2+4*r3]
    paddw   m1, m2
    paddw   m3, m4
  %if BIT_DEPTH <= 10
    paddw   m0, m1
    paddw   m0, m3
  %else
    paddw   m1, m3
    pmaddwd m1, [pw_1]
    paddd   m0, m1
  %endif
%endmacro

%macro SAD_INC_2x4P_MMX 0
    movu    m1, [r0]
    movu    m2, [r0+2*r1]
    psubw   m1, [r2]
    psubw   m2, [r2+2*r3]
    ABSW2   m1, m2, m1, m2, m3, m4
    lea     r0, [r0+4*r1]
    lea     r2, [r2+4*r3]
  %if BIT_DEPTH <= 10
    paddw   m0, m1
    paddw   m0, m2
  %else
    paddw   m1, m2
    pmaddwd m1, [pw_1]
    paddd   m0, m1
  %endif
%endmacro

;-----------------------------------------------------------------------------
; int pixel_sad_NxM( uint16_t *, intptr_t, uint16_t *, intptr_t )
;-----------------------------------------------------------------------------
%macro SAD_MMX 3
cglobal pixel_sad_%1x%2, 4,5-(%2&4/4)
    pxor    m0, m0
%if %2 == 4
    SAD_INC_%3x%1P_MMX
    SAD_INC_%3x%1P_MMX
%else
    mov    r4d, %2/%3
.loop:
    SAD_INC_%3x%1P_MMX
    dec    r4d
    jg .loop
%endif
%if %1*%2 == 256
  %if BIT_DEPTH <= 10
    HADDUW  m0, m1
  %else
    HADDD  m0, m1
  %endif
%else
  %if BIT_DEPTH <= 10
    HADDW   m0, m1
  %else
    HADDD  m0, m1
  %endif
%endif
    movd   eax, m0
    RET
%endmacro

INIT_MMX mmx2
SAD_MMX 16, 16, 1
SAD_MMX 16,  8, 1
SAD_MMX  8, 16, 2
SAD_MMX  8,  8, 2
SAD_MMX  8,  4, 2
SAD_MMX  4,  8, 2
SAD_MMX  4,  4, 2
SAD_MMX  4,  16, 2
INIT_MMX ssse3
SAD_MMX  4,  8, 2
SAD_MMX  4,  4, 2

;=============================================================================
; SAD XMM
;=============================================================================

%macro SAD_1x32 0
    movu    m1, [r2+ 0]
    movu    m2, [r2+16]
    movu    m3, [r2+32]
    movu    m4, [r2+48]
    psubw   m1, [r0+0]
    psubw   m2, [r0+16]
    psubw   m3, [r0+32]
    psubw   m4, [r0+48]
    ABSW2   m1, m2, m1, m2, m5, m6
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
    lea     r0, [r0+2*r1]
    lea     r2, [r2+2*r3]
    ABSW2   m3, m4, m3, m4, m7, m5
    pmaddwd m3, [pw_1]
    pmaddwd m4, [pw_1]
    paddd   m1, m2
    paddd   m3, m4
    paddd   m0, m1
    paddd   m0, m3
%endmacro

%macro SAD_1x24 0
    movu    m1, [r2+ 0]
    movu    m2, [r2+16]
    movu    m3, [r2+32]
    psubw   m1, [r0+0]
    psubw   m2, [r0+16]
    psubw   m3, [r0+32]
    ABSW2   m1, m2, m1, m2, m4, m6
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
    lea     r0, [r0+2*r1]
    lea     r2, [r2+2*r3]
    pxor    m4, m4
    psubw    m4, m3
    pmaxsw  m3, m4
    pmaddwd m3, [pw_1]
    paddd   m1, m2
    paddd   m0, m1
    paddd   m0, m3
%endmacro

%macro SAD_1x48 0
    movu    m1, [r2+ 0]
    movu    m2, [r2+16]
    movu    m3, [r2+32]
    movu    m4, [r2+48]
    psubw   m1, [r0+0]
    psubw   m2, [r0+16]
    psubw   m3, [r0+32]
    psubw   m4, [r0+48]
    ABSW2   m1, m2, m1, m2, m5, m6
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
    ABSW2   m3, m4, m3, m4, m7, m5
    pmaddwd m3, [pw_1]
    pmaddwd m4, [pw_1]
    paddd   m1, m2
    paddd   m3, m4
    paddd   m0, m1
    paddd   m0, m3
    movu    m1, [r2+64]
    movu    m2, [r2+80]
    psubw   m1, [r0+64]
    psubw   m2, [r0+80]
    ABSW2   m1, m2, m1, m2, m3, m4
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
    lea     r0, [r0+2*r1]
    lea     r2, [r2+2*r3]
    paddd   m0, m1
    paddd   m0, m2
%endmacro

%macro SAD_1x64 0
    movu    m1, [r2+ 0]
    movu    m2, [r2+16]
    movu    m3, [r2+32]
    movu    m4, [r2+48]
    psubw   m1, [r0+0]
    psubw   m2, [r0+16]
    psubw   m3, [r0+32]
    psubw   m4, [r0+48]
    ABSW2   m1, m2, m1, m2, m5, m6
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
    ABSW2   m3, m4, m3, m4, m7, m5
    pmaddwd m3, [pw_1]
    pmaddwd m4, [pw_1]
    paddd   m1, m2
    paddd   m3, m4
    paddd   m0, m1
    paddd   m0, m3
    movu    m1, [r2+64]
    movu    m2, [r2+80]
    movu    m3, [r2+96]
    movu    m4, [r2+112]
    psubw   m1, [r0+64]
    psubw   m2, [r0+80]
    psubw   m3, [r0+96]
    psubw   m4, [r0+112]
    ABSW2   m1, m2, m1, m2, m5, m6
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
    ABSW2   m3, m4, m3, m4, m7, m5
    pmaddwd m3, [pw_1]
    pmaddwd m4, [pw_1]
    paddd   m1, m2
    paddd   m3, m4
    paddd   m0, m1
    paddd   m0, m3
    lea     r0, [r0+2*r1]
    lea     r2, [r2+2*r3]
%endmacro

%macro SAD_1x12 0
    movu    m1, [r2+0]
    movh    m2, [r2+16]
    psubw   m1, [r0+0]
    movh    m3, [r0+16]
    psubw   m2, m3
    ABSW2   m1, m2, m1, m2, m4, m6
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
    lea     r0, [r0+2*r1]
    lea     r2, [r2+2*r3]
    paddd   m1, m2
    paddd   m0, m1
%endmacro

%macro SAD_INC_2ROW 1
%if 2*%1 > mmsize
    movu    m1, [r2+ 0]
    movu    m2, [r2+16]
    movu    m3, [r2+2*r3+ 0]
    movu    m4, [r2+2*r3+16]
    psubw   m1, [r0+ 0]
    psubw   m2, [r0+16]
    psubw   m3, [r0+2*r1+ 0]
    psubw   m4, [r0+2*r1+16]
    ABSW2   m1, m2, m1, m2, m5, m6
    lea     r0, [r0+4*r1]
    lea     r2, [r2+4*r3]
    ABSW2   m3, m4, m3, m4, m7, m5
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3
    pmaddwd m1, [pw_1]
    paddd   m0, m1
%else
    movu    m1, [r2]
    movu    m2, [r2+2*r3]
    psubw   m1, [r0]
    psubw   m2, [r0+2*r1]
    ABSW2   m1, m2, m1, m2, m3, m4
    lea     r0, [r0+4*r1]
    lea     r2, [r2+4*r3]
    paddw   m1, m2
    pmaddwd m1, [pw_1]
    paddd   m0, m1
%endif
%endmacro

%macro SAD_INC_2ROW_Nx64 1
%if 2*%1 > mmsize
    movu    m1, [r2 + 0]
    movu    m2, [r2 + 16]
    movu    m3, [r2 + 2 * r3 + 0]
    movu    m4, [r2 + 2 * r3 + 16]
    psubw   m1, [r0 + 0]
    psubw   m2, [r0 + 16]
    psubw   m3, [r0 + 2 * r1 + 0]
    psubw   m4, [r0 + 2 * r1 + 16]
    ABSW2   m1, m2, m1, m2, m5, m6
    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]
    ABSW2   m3, m4, m3, m4, m7, m5
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3
    pmaddwd m1, [pw_1]
    paddd   m0, m1
%else
    movu    m1, [r2]
    movu    m2, [r2 + 2 * r3]
    psubw   m1, [r0]
    psubw   m2, [r0 + 2 * r1]
    ABSW2   m1, m2, m1, m2, m3, m4
    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]
    paddw   m1, m2
    pmaddwd m1, [pw_1]
    paddd   m0, m1
%endif
%endmacro

; ---------------------------------------------------------------------------- -
; int pixel_sad_NxM(uint16_t *, intptr_t, uint16_t *, intptr_t)
; ---------------------------------------------------------------------------- -
%macro SAD 2
cglobal pixel_sad_%1x%2, 4,5,8
    pxor    m0, m0
%if %2 == 4
    SAD_INC_2ROW %1
    SAD_INC_2ROW %1
%else
    mov     r4d, %2/2
.loop:
    SAD_INC_2ROW %1
    dec    r4d
    jg .loop
%endif
    HADDD   m0, m1
    movd    eax, xm0
    RET
%endmacro

; ---------------------------------------------------------------------------- -
; int pixel_sad_Nx64(uint16_t *, intptr_t, uint16_t *, intptr_t)
; ---------------------------------------------------------------------------- -
%macro SAD_Nx64 1
cglobal pixel_sad_%1x64, 4,5, 8
    pxor    m0, m0
    mov     r4d, 64 / 2
.loop:
    SAD_INC_2ROW_Nx64 %1
    dec    r4d
    jg .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET
%endmacro

INIT_XMM sse2
SAD  16,  4
SAD  16,  8
SAD  16, 12
SAD  16, 16
SAD  16, 32
SAD_Nx64  16

INIT_XMM sse2
SAD  8,  4
SAD  8,  8
SAD  8, 16
SAD  8, 32

INIT_YMM avx2
SAD  16,  4
SAD  16,  8
SAD  16, 12
SAD  16, 16
SAD  16, 32

INIT_YMM avx2
cglobal pixel_sad_16x64, 4,5,5
    pxor    m0, m0
    mov     r4d, 16
    mova    m4, [pw_1]
.loop:
    movu    m1, [r2]
    movu    m2, [r2 + r3 * 2]
    psubw   m1, [r0]
    psubw   m2, [r0 + r1 * 2]
    pabsw   m1, m1
    pabsw   m2, m2
    paddw   m3, m1, m2
    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]

    movu    m1, [r2]
    movu    m2, [r2 + r3 * 2]
    psubw   m1, [r0]
    psubw   m2, [r0 + r1 * 2]
    pabsw   m1, m1
    pabsw   m2, m2
    paddw   m1, m2
    pmaddwd m3, m4
    paddd   m0, m3
    pmaddwd m1, m4
    paddd   m0, m1
    lea     r0, [r0+4*r1]
    lea     r2, [r2+4*r3]
    dec     r4d
    jg      .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_32x8, 4,7,7
    pxor    m0, m0
    mov     r4d, 8/4
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
    lea     r5d,     [r1 * 3]
    lea     r6d,     [r3 * 3]
.loop:
    movu    m1, [r2]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + r3 + 32]
    psubw   m1, [r0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + r1]
    psubw   m4, [r0 + r1 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + 2 * r3]
    movu    m2, [r2 + 2 * r3 + 32]
    movu    m3, [r2 + r6]
    movu    m4, [r2 + r6 + 32]
    psubw   m1, [r0 + 2 * r1]
    psubw   m2, [r0 + 2 * r1 + 32]
    psubw   m3, [r0 + r5]
    psubw   m4, [r0 + r5 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1
    dec    r4d
    jg .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_32x16, 4,7,7
    pxor    m0, m0
    mov     r4d, 16/8
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
    lea     r5d,     [r1 * 3]
    lea     r6d,     [r3 * 3]
.loop:
    movu    m1, [r2]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + r3 + 32]
    psubw   m1, [r0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + r1]
    psubw   m4, [r0 + r1 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + 2 * r3]
    movu    m2, [r2 + 2 * r3 + 32]
    movu    m3, [r2 + r6]
    movu    m4, [r2 + r6 + 32]
    psubw   m1, [r0 + 2 * r1]
    psubw   m2, [r0 + 2 * r1 + 32]
    psubw   m3, [r0 + r5]
    psubw   m4, [r0 + r5 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1

    movu    m1, [r2]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + r3 + 32]
    psubw   m1, [r0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + r1]
    psubw   m4, [r0 + r1 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + 2 * r3]
    movu    m2, [r2 + 2 * r3 + 32]
    movu    m3, [r2 + r6]
    movu    m4, [r2 + r6 + 32]
    psubw   m1, [r0 + 2 * r1]
    psubw   m2, [r0 + 2 * r1 + 32]
    psubw   m3, [r0 + r5]
    psubw   m4, [r0 + r5 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1
    dec    r4d
    jg .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_32x24, 4,7,7
    pxor    m0, m0
    mov     r4d, 24/4
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
    lea     r5d,     [r1 * 3]
    lea     r6d,     [r3 * 3]
.loop:
    movu    m1, [r2]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + r3 + 32]
    psubw   m1, [r0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + r1]
    psubw   m4, [r0 + r1 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + 2 * r3]
    movu    m2, [r2 + 2 * r3 + 32]
    movu    m3, [r2 + r6]
    movu    m4, [r2 + r6 + 32]
    psubw   m1, [r0 + 2 * r1]
    psubw   m2, [r0 + 2 * r1 + 32]
    psubw   m3, [r0 + r5]
    psubw   m4, [r0 + r5 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3
    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1
    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]

    dec    r4d
    jg .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_32x32, 4,7,7
    pxor    m0, m0
    mov     r4d, 32/4
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
    lea     r5d,     [r1 * 3]
    lea     r6d,     [r3 * 3]
.loop:
    movu    m1, [r2]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + r3 + 32]
    psubw   m1, [r0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + r1]
    psubw   m4, [r0 + r1 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + 2 * r3]
    movu    m2, [r2 + 2 * r3 + 32]
    movu    m3, [r2 + r6]
    movu    m4, [r2 + r6 + 32]
    psubw   m1, [r0 + 2 * r1]
    psubw   m2, [r0 + 2 * r1 + 32]
    psubw   m3, [r0 + r5]
    psubw   m4, [r0 + r5 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1

    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]

    dec    r4d
    jg .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_32x64, 4,7,7
    pxor    m0, m0
    mov     r4d, 64 / 4
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
    lea     r5d,     [r1 * 3]
    lea     r6d,     [r3 * 3]
.loop:
    movu    m1, [r2]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + r3]
    movu    m4, [r2 + r3 + 32]
    psubw   m1, [r0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + r1]
    psubw   m4, [r0 + r1 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + 2 * r3]
    movu    m2, [r2 + 2 * r3 + 32]
    movu    m3, [r2 + r6]
    movu    m4, [r2 + r6 + 32]
    psubw   m1, [r0 + 2 * r1]
    psubw   m2, [r0 + 2 * r1 + 32]
    psubw   m3, [r0 + r5]
    psubw   m4, [r0 + r5 + 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1

    lea     r0, [r0 + 4 * r1]
    lea     r2, [r2 + 4 * r3]

    dec     r4d
    jg .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_48x64, 4, 5, 7
    pxor    m0, m0
    mov     r4d, 64/2
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
.loop:
    movu    m1, [r2 + 0 * mmsize]
    movu    m2, [r2 + 1 * mmsize]
    movu    m3, [r2 + 2 * mmsize]
    psubw   m1, [r0 + 0 * mmsize]
    psubw   m2, [r0 + 1 * mmsize]
    psubw   m3, [r0 + 2 * mmsize]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    paddw   m1, m2
    paddw   m5, m3, m1

    movu    m1, [r2 + r3 + 0 * mmsize]
    movu    m2, [r2 + r3 + 1 * mmsize]
    movu    m3, [r2 + r3 + 2 * mmsize]
    psubw   m1, [r0 + r1 + 0 * mmsize]
    psubw   m2, [r0 + r1 + 1 * mmsize]
    psubw   m3, [r0 + r1 + 2 * mmsize]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    paddw   m1, m2
    paddw   m3, m1

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m3, m6
    paddd   m0, m3
    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]

    dec     r4d
    jg      .loop

    HADDD   m0, m3
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_64x16, 4, 5, 7
    pxor    m0, m0
    mov     r4d, 16 / 2
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
.loop:
    movu    m1, [r2 + 0]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 2 * 32]
    movu    m4, [r2 + 3 * 32]
    psubw   m1, [r0 + 0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + 2 * 32]
    psubw   m4, [r0 + 3 * 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + r3]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 64]
    movu    m4, [r2 + r3 + 96]
    psubw   m1, [r0 + r1]
    psubw   m2, [r0 + r1 + 32]
    psubw   m3, [r0 + r1 + 64]
    psubw   m4, [r0 + r1 + 96]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1

    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]

    dec     r4d
    jg      .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_64x32, 4, 5, 7
    pxor    m0, m0
    mov     r4d, 32 / 2
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
.loop:
    movu    m1, [r2 + 0]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 2 * 32]
    movu    m4, [r2 + 3 * 32]
    psubw   m1, [r0 + 0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + 2 * 32]
    psubw   m4, [r0 + 3 * 32]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + r3]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 64]
    movu    m4, [r2 + r3 + 96]
    psubw   m1, [r0 + r1]
    psubw   m2, [r0 + r1 + 32]
    psubw   m3, [r0 + r1 + 64]
    psubw   m4, [r0 + r1 + 96]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1
    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]

    dec     r4d
    jg      .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_64x48, 4, 5, 7
    pxor    m0, m0
    mov     r4d, 48 / 2
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
.loop:
    movu    m1, [r2 + 0]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 64]
    movu    m4, [r2 + 96]
    psubw   m1, [r0 + 0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + 64]
    psubw   m4, [r0 + 96]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + r3]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 64]
    movu    m4, [r2 + r3 + 96]
    psubw   m1, [r0 + r1]
    psubw   m2, [r0 + r1 + 32]
    psubw   m3, [r0 + r1 + 64]
    psubw   m4, [r0 + r1 + 96]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1

    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]

    dec     r4d
    jg      .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_64x64, 4, 5, 7
    pxor    m0, m0
    mov     r4d, 64 / 2
    mova    m6, [pw_1]
    add     r3d, r3d
    add     r1d, r1d
.loop:
    movu    m1, [r2 + 0]
    movu    m2, [r2 + 32]
    movu    m3, [r2 + 64]
    movu    m4, [r2 + 96]
    psubw   m1, [r0 + 0]
    psubw   m2, [r0 + 32]
    psubw   m3, [r0 + 64]
    psubw   m4, [r0 + 96]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m5, m1, m3

    movu    m1, [r2 + r3]
    movu    m2, [r2 + r3 + 32]
    movu    m3, [r2 + r3 + 64]
    movu    m4, [r2 + r3 + 96]
    psubw   m1, [r0 + r1]
    psubw   m2, [r0 + r1 + 32]
    psubw   m3, [r0 + r1 + 64]
    psubw   m4, [r0 + r1 + 96]
    pabsw   m1, m1
    pabsw   m2, m2
    pabsw   m3, m3
    pabsw   m4, m4
    paddw   m1, m2
    paddw   m3, m4
    paddw   m1, m3

    pmaddwd m5, m6
    paddd   m0, m5
    pmaddwd m1, m6
    paddd   m0, m1

    lea     r0, [r0 + 2 * r1]
    lea     r2, [r2 + 2 * r3]

    dec     r4d
    jg      .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET

;------------------------------------------------------------------
; int pixel_sad_32xN( uint16_t *, intptr_t, uint16_t *, intptr_t )
;------------------------------------------------------------------
%macro SAD_32 2
cglobal pixel_sad_%1x%2, 4,5,8
    pxor    m0,  m0
    mov     r4d, %2/4
.loop:
    SAD_1x32
    SAD_1x32
    SAD_1x32
    SAD_1x32
    dec     r4d
    jnz     .loop

    HADDD   m0, m1
    movd    eax, xm0
    RET
%endmacro

INIT_XMM sse2
SAD_32  32,  8
SAD_32  32, 16
SAD_32  32, 24
SAD_32  32, 32
SAD_32  32, 64

;------------------------------------------------------------------
; int pixel_sad_64xN( uint16_t *, intptr_t, uint16_t *, intptr_t )
;------------------------------------------------------------------
%macro SAD_64 2
cglobal pixel_sad_%1x%2, 4,5,8
    pxor    m0, m0
    mov     r4d, %2/4
.loop:
    SAD_1x64
    SAD_1x64
    SAD_1x64
    SAD_1x64
    dec     r4d
    jnz     .loop

    HADDD   m0, m1
    movd    eax, xmm0
    RET
%endmacro

INIT_XMM sse2
SAD_64  64, 16
SAD_64  64, 32
SAD_64  64, 48
SAD_64  64, 64

;------------------------------------------------------------------
; int pixel_sad_48xN( uint16_t *, intptr_t, uint16_t *, intptr_t )
;------------------------------------------------------------------
%macro SAD_48 2
cglobal pixel_sad_%1x%2, 4,5,8
    pxor    m0, m0
    mov     r4d, %2/4
.loop:
    SAD_1x48
    SAD_1x48
    SAD_1x48
    SAD_1x48
    dec     r4d
    jnz     .loop

    HADDD   m0, m1
    movd    eax, xmm0
    RET
%endmacro

INIT_XMM sse2
SAD_48  48, 64

;------------------------------------------------------------------
; int pixel_sad_24xN( uint16_t *, intptr_t, uint16_t *, intptr_t )
;------------------------------------------------------------------
%macro SAD_24 2
cglobal pixel_sad_%1x%2, 4,5,8
    pxor    m0, m0
    mov     r4d, %2/4
.loop:
    SAD_1x24
    SAD_1x24
    SAD_1x24
    SAD_1x24
    dec     r4d
    jnz     .loop

    HADDD   m0, m1
    movd    eax, xmm0
    RET
%endmacro

INIT_XMM sse2
SAD_24  24, 32

;------------------------------------------------------------------
; int pixel_sad_12xN( uint16_t *, intptr_t, uint16_t *, intptr_t )
;------------------------------------------------------------------
%macro SAD_12 2
cglobal pixel_sad_%1x%2, 4,5,8
    pxor    m0,  m0
    mov     r4d, %2/4
.loop:
    SAD_1x12
    SAD_1x12
    SAD_1x12
    SAD_1x12
    dec     r4d
    jnz     .loop

    HADDD   m0, m1
    movd    eax, xmm0
    RET
%endmacro

INIT_XMM sse2
SAD_12  12, 16


;=============================================================================
; SAD x3/x4
;=============================================================================

%macro SAD_X3_INC_P 0
    add     r0, 4*FENC_STRIDE
    lea     r1, [r1+4*r4]
    lea     r2, [r2+4*r4]
    lea     r3, [r3+4*r4]
%endmacro

%macro SAD_X3_ONE_START 0
    mova    m3, [r0]
    movu    m0, [r1]
    movu    m1, [r2]
    movu    m2, [r3]
    psubw   m0, m3
    psubw   m1, m3
    psubw   m2, m3
    ABSW2   m0, m1, m0, m1, m4, m5
    ABSW    m2, m2, m6
    pmaddwd m0, [pw_1]
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
%endmacro

%macro SAD_X3_ONE 2
    mova    m6, [r0+%1]
    movu    m3, [r1+%2]
    movu    m4, [r2+%2]
    movu    m5, [r3+%2]
    psubw   m3, m6
    psubw   m4, m6
    psubw   m5, m6
    ABSW2   m3, m4, m3, m4, m7, m6
    ABSW    m5, m5, m6
    pmaddwd m3, [pw_1]
    pmaddwd m4, [pw_1]
    pmaddwd m5, [pw_1]
    paddd   m0, m3
    paddd   m1, m4
    paddd   m2, m5
%endmacro

%macro SAD_X3_END 2
%if mmsize == 8 && %1*%2 == 256
    HADDUW   m0, m3
    HADDUW   m1, m4
    HADDUW   m2, m5
%else
    HADDD    m0, m3
    HADDD    m1, m4
    HADDD    m2, m5
%endif
%if UNIX64
    movd [r5+0], xm0
    movd [r5+4], xm1
    movd [r5+8], xm2
%else
    mov      r0, r5mp
    movd [r0+0], xm0
    movd [r0+4], xm1
    movd [r0+8], xm2
%endif
    RET
%endmacro

%macro SAD_X4_INC_P 0
    add     r0, 4*FENC_STRIDE
    lea     r1, [r1+4*r5]
    lea     r2, [r2+4*r5]
    lea     r3, [r3+4*r5]
    lea     r4, [r4+4*r5]
%endmacro

%macro SAD_X4_ONE_START 0
    mova    m4, [r0]
    movu    m0, [r1]
    movu    m1, [r2]
    movu    m2, [r3]
    movu    m3, [r4]
    psubw   m0, m4
    psubw   m1, m4
    psubw   m2, m4
    psubw   m3, m4
    ABSW2   m0, m1, m0, m1, m5, m6
    ABSW2   m2, m3, m2, m3, m4, m7
    pmaddwd m0, [pw_1]
    pmaddwd m1, [pw_1]
    pmaddwd m2, [pw_1]
    pmaddwd m3, [pw_1]
%endmacro

%macro SAD_X4_ONE 2
    mova    m4, [r0+%1]
    movu    m5, [r1+%2]
    movu    m6, [r2+%2]
%if num_mmregs > 8
    movu    m7, [r3+%2]
    movu    m8, [r4+%2]
    psubw   m5, m4
    psubw   m6, m4
    psubw   m7, m4
    psubw   m8, m4
    ABSW2   m5, m6, m5, m6, m9, m10
    ABSW2   m7, m8, m7, m8, m9, m10
    pmaddwd m5, [pw_1]
    pmaddwd m6, [pw_1]
    pmaddwd m7, [pw_1]
    pmaddwd m8, [pw_1]
    paddd   m0, m5
    paddd   m1, m6
    paddd   m2, m7
    paddd   m3, m8
%elif cpuflag(ssse3)
    movu    m7, [r3+%2]
    psubw   m5, m4
    psubw   m6, m4
    psubw   m7, m4
    movu    m4, [r4+%2]
    pabsw   m5, m5
    psubw   m4, [r0+%1]
    pabsw   m6, m6
    pabsw   m7, m7
    pabsw   m4, m4
    pmaddwd m5, [pw_1]
    pmaddwd m6, [pw_1]
    pmaddwd m7, [pw_1]
    pmaddwd m4, [pw_1]
    paddd   m0, m5
    paddd   m1, m6
    paddd   m2, m7
    paddd   m3, m4
%else ; num_mmregs == 8 && !ssse3
    psubw   m5, m4
    psubw   m6, m4
    ABSW    m5, m5, m7
    ABSW    m6, m6, m7
    pmaddwd m5, [pw_1]
    pmaddwd m6, [pw_1]
    paddd   m0, m5
    paddd   m1, m6
    movu    m5, [r3+%2]
    movu    m6, [r4+%2]
    psubw   m5, m4
    psubw   m6, m4
    ABSW2   m5, m6, m5, m6, m7, m4
    pmaddwd m5, [pw_1]
    pmaddwd m6, [pw_1]
    paddd   m2, m5
    paddd   m3, m6
%endif
%endmacro

%macro SAD_X4_END 2
%if mmsize == 8 && %1*%2 == 256
    HADDUW    m0, m4
    HADDUW    m1, m5
    HADDUW    m2, m6
    HADDUW    m3, m7
%else
    HADDD     m0, m4
    HADDD     m1, m5
    HADDD     m2, m6
    HADDD     m3, m7
%endif
    mov       r0, r6mp
    movd [r0+ 0], xm0
    movd [r0+ 4], xm1
    movd [r0+ 8], xm2
    movd [r0+12], xm3
    RET
%endmacro

%macro SAD_X_2xNP 4
    %assign x %3
%rep %4
    SAD_X%1_ONE x*mmsize, x*mmsize
    SAD_X%1_ONE 2*FENC_STRIDE+x*mmsize, 2*%2+x*mmsize
    %assign x x+1
%endrep
%endmacro

%macro PIXEL_VSAD 0
cglobal pixel_vsad, 3,3,8
    mova      m0, [r0]
    mova      m1, [r0+16]
    mova      m2, [r0+2*r1]
    mova      m3, [r0+2*r1+16]
    lea       r0, [r0+4*r1]
    psubw     m0, m2
    psubw     m1, m3
    ABSW2     m0, m1, m0, m1, m4, m5
    paddw     m0, m1
    sub      r2d, 2
    je .end
.loop:
    mova      m4, [r0]
    mova      m5, [r0+16]
    mova      m6, [r0+2*r1]
    mova      m7, [r0+2*r1+16]
    lea       r0, [r0+4*r1]
    psubw     m2, m4
    psubw     m3, m5
    psubw     m4, m6
    psubw     m5, m7
    ABSW      m2, m2, m1
    ABSW      m3, m3, m1
    ABSW      m4, m4, m1
    ABSW      m5, m5, m1
    paddw     m0, m2
    paddw     m0, m3
    paddw     m0, m4
    paddw     m0, m5
    mova      m2, m6
    mova      m3, m7
    sub r2d, 2
    jg .loop
.end:
%if BIT_DEPTH == 9
    HADDW     m0, m1 ; max sum: 62(pixel diffs)*511(pixel_max)=31682
%else
    HADDUW    m0, m1 ; max sum: 62(pixel diffs)*1023(pixel_max)=63426
%endif
    movd     eax, m0
    RET
%endmacro
INIT_XMM sse2
PIXEL_VSAD
INIT_XMM ssse3
PIXEL_VSAD
INIT_XMM xop
PIXEL_VSAD

INIT_YMM avx2
cglobal pixel_vsad, 3,3
    mova      m0, [r0]
    mova      m1, [r0+2*r1]
    lea       r0, [r0+4*r1]
    psubw     m0, m1
    pabsw     m0, m0
    sub      r2d, 2
    je .end
.loop:
    mova      m2, [r0]
    mova      m3, [r0+2*r1]
    lea       r0, [r0+4*r1]
    psubw     m1, m2
    psubw     m2, m3
    pabsw     m1, m1
    pabsw     m2, m2
    paddw     m0, m1
    paddw     m0, m2
    mova      m1, m3
    sub      r2d, 2
    jg .loop
.end:
%if BIT_DEPTH == 9
    HADDW     m0, m1
%else
    HADDUW    m0, m1
%endif
    movd     eax, xm0
    RET
;-----------------------------------------------------------------------------
; void pixel_sad_xN_WxH( uint16_t *fenc, uint16_t *pix0, uint16_t *pix1,
;                        uint16_t *pix2, intptr_t i_stride, int scores[3] )
;-----------------------------------------------------------------------------
%macro SAD_X 3
cglobal pixel_sad_x%1_%2x%3, 6,7,XMM_REGS
    %assign regnum %1+1
    %xdefine STRIDE r %+ regnum
    mov     r6, %3/2-1
    SAD_X%1_ONE_START
    SAD_X%1_ONE 2*FENC_STRIDE, 2*STRIDE
    SAD_X_2xNP %1, STRIDE, 1, %2/(mmsize/2)-1
.loop:
    SAD_X%1_INC_P
    SAD_X_2xNP %1, STRIDE, 0, %2/(mmsize/2)
    dec     r6
    jg .loop
%if %1 == 4
    mov     r6, r6m
%endif
    SAD_X%1_END %2, %3
%endmacro

INIT_MMX mmx2
%define XMM_REGS 0
SAD_X 3, 16, 16
SAD_X 3, 16,  8
SAD_X 3, 12, 16
SAD_X 3,  8, 16
SAD_X 3,  8,  8
SAD_X 3,  8,  4
SAD_X 3,  4, 16
SAD_X 3,  4,  8
SAD_X 3,  4,  4
SAD_X 4, 16, 16
SAD_X 4, 16,  8
SAD_X 4, 12, 16
SAD_X 4,  8, 16
SAD_X 4,  8,  8
SAD_X 4,  8,  4
SAD_X 4,  4, 16
SAD_X 4,  4,  8
SAD_X 4,  4,  4
INIT_MMX ssse3
SAD_X 3,  4,  8
SAD_X 3,  4,  4
SAD_X 4,  4,  8
SAD_X 4,  4,  4
INIT_XMM ssse3
%define XMM_REGS 7
SAD_X 3, 16, 16
SAD_X 3, 16,  8
SAD_X 3,  8, 16
SAD_X 3,  8,  8
SAD_X 3,  8,  4
%define XMM_REGS 9
SAD_X 4, 16, 16
SAD_X 4, 16,  8
SAD_X 4,  8, 16
SAD_X 4,  8,  8
SAD_X 4,  8,  4
INIT_XMM sse2
%define XMM_REGS 8
SAD_X 3, 64, 64
SAD_X 3, 64, 48
SAD_X 3, 64, 32
SAD_X 3, 64, 16
SAD_X 3, 48, 64
SAD_X 3, 32, 64
SAD_X 3, 32, 32
SAD_X 3, 32, 24
SAD_X 3, 32, 16
SAD_X 3, 32,  8
SAD_X 3, 24, 32
SAD_X 3, 16, 64
SAD_X 3, 16, 32
SAD_X 3, 16, 16
SAD_X 3, 16, 12
SAD_X 3, 16,  8
SAD_X 3, 16,  4
SAD_X 3,  8, 32
SAD_X 3,  8, 16
SAD_X 3,  8,  8
SAD_X 3,  8,  4
%define XMM_REGS 11
SAD_X 4, 64, 64
SAD_X 4, 64, 48
SAD_X 4, 64, 32
SAD_X 4, 64, 16
SAD_X 4, 48, 64
SAD_X 4, 32, 64
SAD_X 4, 32, 32
SAD_X 4, 32, 24
SAD_X 4, 32, 16
SAD_X 4, 32,  8
SAD_X 4, 24, 32
SAD_X 4, 16, 64
SAD_X 4, 16, 32
SAD_X 4, 16, 16
SAD_X 4, 16, 12
SAD_X 4, 16,  8
SAD_X 4, 16,  4
SAD_X 4,  8, 32
SAD_X 4,  8, 16
SAD_X 4,  8,  8
SAD_X 4,  8,  4
INIT_YMM avx2
%define XMM_REGS 7
SAD_X 3, 16,  4
SAD_X 3, 16,  8
SAD_X 3, 16,  12
SAD_X 3, 16,  16
SAD_X 3, 16,  32
SAD_X 3, 16,  64
SAD_X 3, 32,  8
SAD_X 3, 32, 16
SAD_X 3, 32, 24
SAD_X 3, 32, 32
SAD_X 3, 32, 64
SAD_X 3, 48, 64
SAD_X 3, 64, 16
SAD_X 3, 64, 32
SAD_X 3, 64, 48
SAD_X 3, 64, 64
%define XMM_REGS 9
SAD_X 4, 16,  4
SAD_X 4, 16,  8
SAD_X 4, 16,  12
SAD_X 4, 16,  16
SAD_X 4, 16,  32
SAD_X 4, 16,  64
SAD_X 4, 32,  8
SAD_X 4, 32, 16
SAD_X 4, 32, 24
SAD_X 4, 32, 32
SAD_X 4, 32, 64
SAD_X 4, 48, 64
SAD_X 4, 64, 16
SAD_X 4, 64, 32
SAD_X 4, 64, 48
SAD_X 4, 64, 64

