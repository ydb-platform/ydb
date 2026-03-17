;*****************************************************************************
;* sad-a.asm: x86 sad functions
;*****************************************************************************
;* Copyright (C) 2003-2013 x264 project
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Loren Merritt <lorenm@u.washington.edu>
;*          Fiona Glaser <fiona@x264.com>
;*          Laurent Aimar <fenrir@via.ecp.fr>
;*          Alex Izvorski <aizvorksi@gmail.com>
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

SECTION_RODATA 32

MSK:                  db 255,255,255,255,255,255,255,255,255,255,255,255,0,0,0,0

SECTION .text

cextern pb_3
cextern pb_shuf8x8c
cextern pw_8
cextern pd_64

;=============================================================================
; SAD MMX
;=============================================================================

%macro SAD_INC_2x16P 0
    movq    mm1,    [r0]
    movq    mm2,    [r0+8]
    movq    mm3,    [r0+r1]
    movq    mm4,    [r0+r1+8]
    psadbw  mm1,    [r2]
    psadbw  mm2,    [r2+8]
    psadbw  mm3,    [r2+r3]
    psadbw  mm4,    [r2+r3+8]
    lea     r0,     [r0+2*r1]
    paddw   mm1,    mm2
    paddw   mm3,    mm4
    lea     r2,     [r2+2*r3]
    paddw   mm0,    mm1
    paddw   mm0,    mm3
%endmacro

%macro SAD_INC_2x8P 0
    movq    mm1,    [r0]
    movq    mm2,    [r0+r1]
    psadbw  mm1,    [r2]
    psadbw  mm2,    [r2+r3]
    lea     r0,     [r0+2*r1]
    paddw   mm0,    mm1
    paddw   mm0,    mm2
    lea     r2,     [r2+2*r3]
%endmacro

%macro SAD_INC_2x4P 0
    movd    mm1,    [r0]
    movd    mm2,    [r2]
    punpckldq mm1,  [r0+r1]
    punpckldq mm2,  [r2+r3]
    psadbw  mm1,    mm2
    paddw   mm0,    mm1
    lea     r0,     [r0+2*r1]
    lea     r2,     [r2+2*r3]
%endmacro

;-----------------------------------------------------------------------------
; int pixel_sad_16x16( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
%macro SAD 2
cglobal pixel_sad_%1x%2_mmx2, 4,4
    pxor    mm0, mm0
%rep %2/2
    SAD_INC_2x%1P
%endrep
    movd    eax, mm0
    RET
%endmacro

SAD 16, 16
SAD 16,  8
SAD  8, 16
SAD  8,  8
SAD  8,  4
SAD  4, 16
SAD  4,  8
SAD  4,  4



;=============================================================================
; SAD XMM
;=============================================================================

%macro SAD_END_SSE2 0
    movhlps m1, m0
    paddw   m0, m1
    movd   eax, m0
    RET
%endmacro

%macro PROCESS_SAD_12x4 0
    movu    m1,  [r2]
    movu    m2,  [r0]
    pand    m1,  m4
    pand    m2,  m4
    psadbw  m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]
    movu    m1,  [r2]
    movu    m2,  [r0]
    pand    m1,  m4
    pand    m2,  m4
    psadbw  m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]
    movu    m1,  [r2]
    movu    m2,  [r0]
    pand    m1,  m4
    pand    m2,  m4
    psadbw  m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]
    movu    m1,  [r2]
    movu    m2,  [r0]
    pand    m1,  m4
    pand    m2,  m4
    psadbw  m1,  m2
    paddd   m0,  m1
%endmacro

%macro PROCESS_SAD_16x4 0
    movu    m1,  [r2]
    movu    m2,  [r2 + r3]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + r1]
    paddd   m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + 2 * r3]
    lea     r0,  [r0 + 2 * r1]
    movu    m1,  [r2]
    movu    m2,  [r2 + r3]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + r1]
    paddd   m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + 2 * r3]
    lea     r0,  [r0 + 2 * r1]
%endmacro

%macro PROCESS_SAD_24x4 0
    movu        m1,  [r2]
    movq        m2,  [r2 + 16]
    lea         r2,  [r2 + r3]
    movu        m3,  [r2]
    movq        m4,  [r2 + 16]
    psadbw      m1,  [r0]
    psadbw      m3,  [r0 + r1]
    paddd       m0,  m1
    paddd       m0,  m3
    movq        m1,  [r0 + 16]
    lea         r0,  [r0 + r1]
    movq        m3,  [r0 + 16]
    punpcklqdq  m2,  m4
    punpcklqdq  m1,  m3
    psadbw      m2, m1
    paddd       m0, m2
    lea         r2,  [r2 + r3]
    lea         r0,  [r0 + r1]

    movu        m1,  [r2]
    movq        m2,  [r2 + 16]
    lea         r2,  [r2 + r3]
    movu        m3,  [r2]
    movq        m4,  [r2 + 16]
    psadbw      m1,  [r0]
    psadbw      m3,  [r0 + r1]
    paddd       m0,  m1
    paddd       m0,  m3
    movq        m1,  [r0 + 16]
    lea         r0,  [r0 + r1]
    movq        m3,  [r0 + 16]
    punpcklqdq  m2,  m4
    punpcklqdq  m1,  m3
    psadbw      m2, m1
    paddd       m0, m2
%endmacro

%macro PROCESS_SAD_32x4 0
    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    paddd   m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]
    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    paddd   m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]
    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    paddd   m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]
    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    paddd   m1,  m2
    paddd   m0,  m1
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]
%endmacro

%macro PROCESS_SAD_48x4 0
    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    movu    m3,  [r2 + 32]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    psadbw  m3,  [r0 + 32]
    paddd   m1,  m2
    paddd   m0,  m1
    paddd   m0,  m3
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]

    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    movu    m3,  [r2 + 32]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    psadbw  m3,  [r0 + 32]
    paddd   m1,  m2
    paddd   m0,  m1
    paddd   m0,  m3
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]

    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    movu    m3,  [r2 + 32]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    psadbw  m3,  [r0 + 32]
    paddd   m1,  m2
    paddd   m0,  m1
    paddd   m0,  m3
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]

    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    movu    m3,  [r2 + 32]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    psadbw  m3,  [r0 + 32]
    paddd   m1,  m2
    paddd   m0,  m1
    paddd   m0,  m3
%endmacro

%macro PROCESS_SAD_8x4 0
    movq        m1, [r2]
    movq        m2, [r2 + r3]
    lea         r2, [r2 + 2 * r3]
    movq        m3, [r0]
    movq        m4, [r0 + r1]
    lea         r0, [r0 + 2 * r1]
    punpcklqdq  m1, m2
    punpcklqdq  m3, m4
    psadbw      m1, m3
    paddd       m0, m1
    movq        m1, [r2]
    movq        m2, [r2 + r3]
    lea         r2, [r2 + 2 * r3]
    movq        m3, [r0]
    movq        m4, [r0 + r1]
    lea         r0, [r0 + 2 * r1]
    punpcklqdq  m1, m2
    punpcklqdq  m3, m4
    psadbw      m1, m3
    paddd       m0, m1
%endmacro

%macro PROCESS_SAD_64x4 0
    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    movu    m3,  [r2 + 32]
    movu    m4,  [r2 + 48]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    psadbw  m3,  [r0 + 32]
    psadbw  m4,  [r0 + 48]
    paddd   m1,  m2
    paddd   m3,  m4
    paddd   m0,  m1
    paddd   m0,  m3
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]

    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    movu    m3,  [r2 + 32]
    movu    m4,  [r2 + 48]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    psadbw  m3,  [r0 + 32]
    psadbw  m4,  [r0 + 48]
    paddd   m1,  m2
    paddd   m3,  m4
    paddd   m0,  m1
    paddd   m0,  m3
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]

    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    movu    m3,  [r2 + 32]
    movu    m4,  [r2 + 48]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    psadbw  m3,  [r0 + 32]
    psadbw  m4,  [r0 + 48]
    paddd   m1,  m2
    paddd   m3,  m4
    paddd   m0,  m1
    paddd   m0,  m3
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]

    movu    m1,  [r2]
    movu    m2,  [r2 + 16]
    movu    m3,  [r2 + 32]
    movu    m4,  [r2 + 48]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + 16]
    psadbw  m3,  [r0 + 32]
    psadbw  m4,  [r0 + 48]
    paddd   m1,  m2
    paddd   m3,  m4
    paddd   m0,  m1
    paddd   m0,  m3
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]
%endmacro

%macro SAD_W16 0
;-----------------------------------------------------------------------------
; int pixel_sad_16x16( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_16x16, 4,4,8
    movu    m0, [r2]
    movu    m1, [r2+r3]
    lea     r2, [r2+2*r3]
    movu    m2, [r2]
    movu    m3, [r2+r3]
    lea     r2, [r2+2*r3]
    psadbw  m0, [r0]
    psadbw  m1, [r0+r1]
    lea     r0, [r0+2*r1]
    movu    m4, [r2]
    paddw   m0, m1
    psadbw  m2, [r0]
    psadbw  m3, [r0+r1]
    lea     r0, [r0+2*r1]
    movu    m5, [r2+r3]
    lea     r2, [r2+2*r3]
    paddw   m2, m3
    movu    m6, [r2]
    movu    m7, [r2+r3]
    lea     r2, [r2+2*r3]
    paddw   m0, m2
    psadbw  m4, [r0]
    psadbw  m5, [r0+r1]
    lea     r0, [r0+2*r1]
    movu    m1, [r2]
    paddw   m4, m5
    psadbw  m6, [r0]
    psadbw  m7, [r0+r1]
    lea     r0, [r0+2*r1]
    movu    m2, [r2+r3]
    lea     r2, [r2+2*r3]
    paddw   m6, m7
    movu    m3, [r2]
    paddw   m0, m4
    movu    m4, [r2+r3]
    lea     r2, [r2+2*r3]
    paddw   m0, m6
    psadbw  m1, [r0]
    psadbw  m2, [r0+r1]
    lea     r0, [r0+2*r1]
    movu    m5, [r2]
    paddw   m1, m2
    psadbw  m3, [r0]
    psadbw  m4, [r0+r1]
    lea     r0, [r0+2*r1]
    movu    m6, [r2+r3]
    lea     r2, [r2+2*r3]
    paddw   m3, m4
    movu    m7, [r2]
    paddw   m0, m1
    movu    m1, [r2+r3]
    paddw   m0, m3
    psadbw  m5, [r0]
    psadbw  m6, [r0+r1]
    lea     r0, [r0+2*r1]
    paddw   m5, m6
    psadbw  m7, [r0]
    psadbw  m1, [r0+r1]
    paddw   m7, m1
    paddw   m0, m5
    paddw   m0, m7
    SAD_END_SSE2

;-----------------------------------------------------------------------------
; int pixel_sad_16x8( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_16x8, 4,4
    movu    m0, [r2]
    movu    m2, [r2+r3]
    lea     r2, [r2+2*r3]
    movu    m3, [r2]
    movu    m4, [r2+r3]
    psadbw  m0, [r0]
    psadbw  m2, [r0+r1]
    lea     r0, [r0+2*r1]
    psadbw  m3, [r0]
    psadbw  m4, [r0+r1]
    lea     r0, [r0+2*r1]
    lea     r2, [r2+2*r3]
    paddw   m0, m2
    paddw   m3, m4
    paddw   m0, m3
    movu    m1, [r2]
    movu    m2, [r2+r3]
    lea     r2, [r2+2*r3]
    movu    m3, [r2]
    movu    m4, [r2+r3]
    psadbw  m1, [r0]
    psadbw  m2, [r0+r1]
    lea     r0, [r0+2*r1]
    psadbw  m3, [r0]
    psadbw  m4, [r0+r1]
    lea     r0, [r0+2*r1]
    lea     r2, [r2+2*r3]
    paddw   m1, m2
    paddw   m3, m4
    paddw   m0, m1
    paddw   m0, m3
    SAD_END_SSE2

;-----------------------------------------------------------------------------
; int pixel_sad_16x12( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_16x12, 4,4,3
    pxor m0, m0

    PROCESS_SAD_16x4
    PROCESS_SAD_16x4
    PROCESS_SAD_16x4

    movhlps m1, m0
    paddd   m0, m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_16x32( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_16x32, 4,5,3
    pxor m0,  m0
    mov  r4d, 4
.loop:
    PROCESS_SAD_16x4
    PROCESS_SAD_16x4
    dec  r4d
    jnz .loop

    movhlps m1, m0
    paddd   m0, m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_16x64( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_16x64, 4,5,3
    pxor m0,  m0
    mov  r4d, 8
.loop:
    PROCESS_SAD_16x4
    PROCESS_SAD_16x4
    dec  r4d
    jnz .loop

    movhlps m1, m0
    paddd   m0, m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_16x4( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_16x4, 4,4,3

    movu    m0,  [r2]
    movu    m1,  [r2 + r3]
    psadbw  m0,  [r0]
    psadbw  m1,  [r0 + r1]
    paddd   m0,  m1
    lea     r2,  [r2 + 2 * r3]
    lea     r0,  [r0 + 2 * r1]
    movu    m1,  [r2]
    movu    m2,  [r2 + r3]
    psadbw  m1,  [r0]
    psadbw  m2,  [r0 + r1]
    paddd   m1,  m2
    paddd   m0,  m1

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_32x8( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_32x8, 4,4,3
    pxor  m0,  m0

    PROCESS_SAD_32x4
    PROCESS_SAD_32x4

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_32x24( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_32x24, 4,5,3
    pxor  m0,  m0
    mov   r4d, 3
.loop:
    PROCESS_SAD_32x4
    PROCESS_SAD_32x4
    dec r4d
    jnz .loop

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_32x32( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_32x32, 4,5,3
    pxor  m0,  m0
    mov   r4d, 4
.loop:
    PROCESS_SAD_32x4
    PROCESS_SAD_32x4
    dec r4d
    jnz .loop

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_32x16( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_32x16, 4,4,3
    pxor  m0,  m0

    PROCESS_SAD_32x4
    PROCESS_SAD_32x4
    PROCESS_SAD_32x4
    PROCESS_SAD_32x4

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_32x64( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_32x64, 4,5,3
    pxor  m0,  m0
    mov   r4d, 8
.loop:
    PROCESS_SAD_32x4
    PROCESS_SAD_32x4
    dec  r4d
    jnz .loop

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_8x32( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_8x32, 4,5,3
    pxor  m0,  m0
    mov   r4d, 4
.loop:
    PROCESS_SAD_8x4
    PROCESS_SAD_8x4
    dec  r4d
    jnz .loop

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_64x16( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_64x16, 4,4,5
    pxor  m0,  m0

    PROCESS_SAD_64x4
    PROCESS_SAD_64x4
    PROCESS_SAD_64x4
    PROCESS_SAD_64x4

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_64x32( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_64x32, 4,5,5
    pxor  m0,  m0
    mov   r4,  4

.loop:
    PROCESS_SAD_64x4
    PROCESS_SAD_64x4

    dec   r4
    jnz   .loop

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_64x48( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_64x48, 4,5,5
    pxor  m0,  m0
    mov   r4,  6

.loop:
    PROCESS_SAD_64x4
    PROCESS_SAD_64x4
    dec     r4d
    jnz     .loop

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_64x64( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_64x64, 4,5,5
    pxor  m0,  m0
    mov   r4,  8

.loop:
    PROCESS_SAD_64x4
    PROCESS_SAD_64x4
    dec   r4
    jnz   .loop

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_48x64( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_48x64, 4,5,5
    pxor  m0,  m0
    mov   r4,  64

.loop:
    PROCESS_SAD_48x4
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]

    PROCESS_SAD_48x4
    lea     r2,  [r2 + r3]
    lea     r0,  [r0 + r1]

    sub   r4,  8
    cmp   r4,  8

jnz .loop
    PROCESS_SAD_48x4
    lea   r2,  [r2 + r3]
    lea   r0,  [r0 + r1]
    PROCESS_SAD_48x4

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_24x32( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_24x32, 4,5,4
    pxor  m0,  m0
    mov   r4,  32

.loop:
    PROCESS_SAD_24x4
    lea         r2,  [r2 + r3]
    lea         r0,  [r0 + r1]
    PROCESS_SAD_24x4
    lea         r2,  [r2 + r3]
    lea         r0,  [r0 + r1]
    sub   r4,  8
    cmp   r4,  8
jnz .loop
    PROCESS_SAD_24x4
    lea         r2,  [r2 + r3]
    lea         r0,  [r0 + r1]
    PROCESS_SAD_24x4

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

;-----------------------------------------------------------------------------
; int pixel_sad_12x16( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sad_12x16, 4,4,4
    mova  m4,  [MSK]
    pxor  m0,  m0

    PROCESS_SAD_12x4
    lea         r2,  [r2 + r3]
    lea         r0,  [r0 + r1]
    PROCESS_SAD_12x4
    lea         r2,  [r2 + r3]
    lea         r0,  [r0 + r1]
    PROCESS_SAD_12x4
    lea         r2,  [r2 + r3]
    lea         r0,  [r0 + r1]
    PROCESS_SAD_12x4

    movhlps m1,  m0
    paddd   m0,  m1
    movd    eax, m0
    RET

%endmacro

INIT_XMM sse2
SAD_W16
INIT_XMM sse3
SAD_W16
INIT_XMM sse2, aligned
SAD_W16

%macro SAD_INC_4x8P_SSE 1
    movq    m1, [r0]
    movq    m2, [r0+r1]
    lea     r0, [r0+2*r1]
    movq    m3, [r2]
    movq    m4, [r2+r3]
    lea     r2, [r2+2*r3]
    movhps  m1, [r0]
    movhps  m2, [r0+r1]
    movhps  m3, [r2]
    movhps  m4, [r2+r3]
    lea     r0, [r0+2*r1]
    psadbw  m1, m3
    psadbw  m2, m4
    lea     r2, [r2+2*r3]
    ACCUM paddw, 0, 1, %1
    paddw   m0, m2
%endmacro

INIT_XMM
;Even on Nehalem, no sizes other than 8x16 benefit from this method.
cglobal pixel_sad_8x16_sse2, 4,4
    SAD_INC_4x8P_SSE 0
    SAD_INC_4x8P_SSE 1
    SAD_INC_4x8P_SSE 1
    SAD_INC_4x8P_SSE 1
    SAD_END_SSE2
    RET

;=============================================================================
; SAD x3/x4 MMX
;=============================================================================

%macro SAD_X3_START_1x8P 0
    movq    mm3,    [r0]
    movq    mm0,    [r1]
    movq    mm1,    [r2]
    movq    mm2,    [r3]
    psadbw  mm0,    mm3
    psadbw  mm1,    mm3
    psadbw  mm2,    mm3
%endmacro

%macro SAD_X3_1x8P 2
    movq    mm3,    [r0+%1]
    movq    mm4,    [r1+%2]
    movq    mm5,    [r2+%2]
    movq    mm6,    [r3+%2]
    psadbw  mm4,    mm3
    psadbw  mm5,    mm3
    psadbw  mm6,    mm3
    paddw   mm0,    mm4
    paddw   mm1,    mm5
    paddw   mm2,    mm6
%endmacro

%macro SAD_X3_START_2x4P 3
    movd      mm3,  [r0]
    movd      %1,   [r1]
    movd      %2,   [r2]
    movd      %3,   [r3]
    punpckldq mm3,  [r0+FENC_STRIDE]
    punpckldq %1,   [r1+r4]
    punpckldq %2,   [r2+r4]
    punpckldq %3,   [r3+r4]
    psadbw    %1,   mm3
    psadbw    %2,   mm3
    psadbw    %3,   mm3
%endmacro

%macro SAD_X3_2x16P 1
%if %1
    SAD_X3_START_1x8P
%else
    SAD_X3_1x8P 0, 0
%endif
    SAD_X3_1x8P 8, 8
    SAD_X3_1x8P FENC_STRIDE, r4
    SAD_X3_1x8P FENC_STRIDE+8, r4+8
    add     r0, 2*FENC_STRIDE
    lea     r1, [r1+2*r4]
    lea     r2, [r2+2*r4]
    lea     r3, [r3+2*r4]
%endmacro

%macro SAD_X3_2x8P 1
%if %1
    SAD_X3_START_1x8P
%else
    SAD_X3_1x8P 0, 0
%endif
    SAD_X3_1x8P FENC_STRIDE, r4
    add     r0, 2*FENC_STRIDE
    lea     r1, [r1+2*r4]
    lea     r2, [r2+2*r4]
    lea     r3, [r3+2*r4]
%endmacro

%macro SAD_X3_2x4P 1
%if %1
    SAD_X3_START_2x4P mm0, mm1, mm2
%else
    SAD_X3_START_2x4P mm4, mm5, mm6
    paddw     mm0,  mm4
    paddw     mm1,  mm5
    paddw     mm2,  mm6
%endif
    add     r0, 2*FENC_STRIDE
    lea     r1, [r1+2*r4]
    lea     r2, [r2+2*r4]
    lea     r3, [r3+2*r4]
%endmacro

%macro SAD_X4_START_1x8P 0
    movq    mm7,    [r0]
    movq    mm0,    [r1]
    movq    mm1,    [r2]
    movq    mm2,    [r3]
    movq    mm3,    [r4]
    psadbw  mm0,    mm7
    psadbw  mm1,    mm7
    psadbw  mm2,    mm7
    psadbw  mm3,    mm7
%endmacro

%macro SAD_X4_1x8P 2
    movq    mm7,    [r0+%1]
    movq    mm4,    [r1+%2]
    movq    mm5,    [r2+%2]
    movq    mm6,    [r3+%2]
    psadbw  mm4,    mm7
    psadbw  mm5,    mm7
    psadbw  mm6,    mm7
    psadbw  mm7,    [r4+%2]
    paddw   mm0,    mm4
    paddw   mm1,    mm5
    paddw   mm2,    mm6
    paddw   mm3,    mm7
%endmacro

%macro SAD_X4_START_2x4P 0
    movd      mm7,  [r0]
    movd      mm0,  [r1]
    movd      mm1,  [r2]
    movd      mm2,  [r3]
    movd      mm3,  [r4]
    punpckldq mm7,  [r0+FENC_STRIDE]
    punpckldq mm0,  [r1+r5]
    punpckldq mm1,  [r2+r5]
    punpckldq mm2,  [r3+r5]
    punpckldq mm3,  [r4+r5]
    psadbw    mm0,  mm7
    psadbw    mm1,  mm7
    psadbw    mm2,  mm7
    psadbw    mm3,  mm7
%endmacro

%macro SAD_X4_INC_2x4P 0
    movd      mm7,  [r0]
    movd      mm4,  [r1]
    movd      mm5,  [r2]
    punpckldq mm7,  [r0+FENC_STRIDE]
    punpckldq mm4,  [r1+r5]
    punpckldq mm5,  [r2+r5]
    psadbw    mm4,  mm7
    psadbw    mm5,  mm7
    paddw     mm0,  mm4
    paddw     mm1,  mm5
    movd      mm4,  [r3]
    movd      mm5,  [r4]
    punpckldq mm4,  [r3+r5]
    punpckldq mm5,  [r4+r5]
    psadbw    mm4,  mm7
    psadbw    mm5,  mm7
    paddw     mm2,  mm4
    paddw     mm3,  mm5
%endmacro

%macro SAD_X4_2x16P 1
%if %1
    SAD_X4_START_1x8P
%else
    SAD_X4_1x8P 0, 0
%endif
    SAD_X4_1x8P 8, 8
    SAD_X4_1x8P FENC_STRIDE, r5
    SAD_X4_1x8P FENC_STRIDE+8, r5+8
    add     r0, 2*FENC_STRIDE
    lea     r1, [r1+2*r5]
    lea     r2, [r2+2*r5]
    lea     r3, [r3+2*r5]
    lea     r4, [r4+2*r5]
%endmacro

%macro SAD_X4_2x8P 1
%if %1
    SAD_X4_START_1x8P
%else
    SAD_X4_1x8P 0, 0
%endif
    SAD_X4_1x8P FENC_STRIDE, r5
    add     r0, 2*FENC_STRIDE
    lea     r1, [r1+2*r5]
    lea     r2, [r2+2*r5]
    lea     r3, [r3+2*r5]
    lea     r4, [r4+2*r5]
%endmacro

%macro SAD_X4_2x4P 1
%if %1
    SAD_X4_START_2x4P
%else
    SAD_X4_INC_2x4P
%endif
    add     r0, 2*FENC_STRIDE
    lea     r1, [r1+2*r5]
    lea     r2, [r2+2*r5]
    lea     r3, [r3+2*r5]
    lea     r4, [r4+2*r5]
%endmacro

%macro SAD_X3_END 0
%if UNIX64
    movd    [r5+0], mm0
    movd    [r5+4], mm1
    movd    [r5+8], mm2
%else
    mov     r0, r5mp
    movd    [r0+0], mm0
    movd    [r0+4], mm1
    movd    [r0+8], mm2
%endif
    RET
%endmacro

%macro SAD_X4_END 0
    mov     r0, r6mp
    movd    [r0+0], mm0
    movd    [r0+4], mm1
    movd    [r0+8], mm2
    movd    [r0+12], mm3
    RET
%endmacro

%macro SAD_X3_12x4 0
    mova    m3,  [r0]
    movu    m5,  [r1]
    pand    m3,  m4
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r2]
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r3]
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m2,  m5
    mova    m3,  [r0 + FENC_STRIDE]
    movu    m5,  [r1 + r4]
    pand    m3,  m4
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r2 + r4]
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r3 + r4]
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m2,  m5
    mova    m3,  [r0 + FENC_STRIDE * 2]
    movu    m5,  [r1 + r4 * 2]
    pand    m3,  m4
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r2 + r4 * 2]
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r3 + r4 * 2]
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m2,  m5
    lea     r1, [r1 + r4 * 2]
    lea     r2, [r2 + r4 * 2]
    lea     r3, [r3 + r4 * 2]
    mova    m3,  [r0 + FENC_STRIDE + FENC_STRIDE * 2]
    movu    m5,  [r1 + r4]
    pand    m3,  m4
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r2 + r4]
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r3 + r4]
    pand    m5,  m4
    psadbw  m5,  m3
    paddd   m2,  m5
    lea     r0,  [r0 + FENC_STRIDE * 4]
    lea     r1,  [r1 + r4 * 2]
    lea     r2,  [r2 + r4 * 2]
    lea     r3,  [r3 + r4 * 2]
%endmacro

%macro SAD_X4_12x4 0
    mova    m4,  [r0]
    movu    m5,  [r1]
    pand    m4,  m6
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m2,  m5
    movu    m5,  [r4]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m3,  m5
    mova    m4,  [r0 + FENC_STRIDE]
    movu    m5,  [r1 + r5]
    pand    m4,  m6
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + r5]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + r5]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m2,  m5
    movu    m5,  [r4 + r5]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m3,  m5
    mova    m4,  [r0 + FENC_STRIDE * 2]
    movu    m5,  [r1 + r5 * 2]
    pand    m4,  m6
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + r5 * 2]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + r5 * 2]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m2,  m5
    movu    m5,  [r4 + r5 * 2]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m3,  m5
    lea     r1, [r1 + r5 * 2]
    lea     r2, [r2 + r5 * 2]
    lea     r3, [r3 + r5 * 2]
    lea     r4, [r4 + r5 * 2]
    mova    m4,  [r0 + FENC_STRIDE + FENC_STRIDE * 2]
    movu    m5,  [r1 + r5]
    pand    m4,  m6
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + r5]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + r5]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m2,  m5
    movu    m5,  [r4 + r5]
    pand    m5,  m6
    psadbw  m5,  m4
    paddd   m3,  m5
    lea     r0,  [r0 + FENC_STRIDE * 4]
    lea     r1,  [r1 + r5 * 2]
    lea     r2,  [r2 + r5 * 2]
    lea     r3,  [r3 + r5 * 2]
    lea     r4,  [r4 + r5 * 2]
%endmacro

%macro SAD_X3_24x4 0
    mova    m3,  [r0]
    mova    m4,  [r0 + 16]
    movu    m5,  [r1]
    movu    m6,  [r1 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m0,  m5
    movu    m5,  [r2]
    movu    m6,  [r2 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m1,  m5
    movu    m5,  [r3]
    movu    m6,  [r3 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m2,  m5

    mova    m3,  [r0 + FENC_STRIDE]
    mova    m4,  [r0 + 16 + FENC_STRIDE]
    movu    m5,  [r1 + r4]
    movu    m6,  [r1 + 16 + r4]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m0,  m5
    movu    m5,  [r2 + r4]
    movu    m6,  [r2 + 16 + r4]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m1,  m5
    movu    m5,  [r3 + r4]
    movu    m6,  [r3 + 16 + r4]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m2,  m5

    mova    m3,  [r0 + FENC_STRIDE * 2]
    mova    m4,  [r0 + 16 + FENC_STRIDE * 2]
    movu    m5,  [r1 + r4 * 2]
    movu    m6,  [r1 + 16 + r4 * 2]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m0,  m5
    movu    m5,  [r2 + r4 * 2]
    movu    m6,  [r2 + 16 + r4 * 2]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m1,  m5
    movu    m5,  [r3 + r4 * 2]
    movu    m6,  [r3 + 16 + r4 * 2]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m2,  m5
    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r4 * 2]
    lea     r2,  [r2 + r4 * 2]
    lea     r3,  [r3 + r4 * 2]

    mova    m3,  [r0 + FENC_STRIDE]
    mova    m4,  [r0 + 16 + FENC_STRIDE]
    movu    m5,  [r1 + r4]
    movu    m6,  [r1 + 16 + r4]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m0,  m5
    movu    m5,  [r2 + r4]
    movu    m6,  [r2 + 16 + r4]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m1,  m5
    movu    m5,  [r3 + r4]
    movu    m6,  [r3 + 16 + r4]
    psadbw  m5,  m3
    psadbw  m6,  m4
    pshufd  m6,  m6, 84
    paddd   m5,  m6
    paddd   m2,  m5
    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r4 * 2]
    lea     r2,  [r2 + r4 * 2]
    lea     r3,  [r3 + r4 * 2]
%endmacro

%macro SAD_X4_24x4 0
    mova    m4,  [r0]
    mova    m5,  [r0 + 16]
    movu    m6,  [r1]
    movu    m7,  [r1 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m0,  m6
    movu    m6,  [r2]
    movu    m7,  [r2 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m1,  m6
    movu    m6,  [r3]
    movu    m7,  [r3 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m2,  m6
    movu    m6,  [r4]
    movu    m7,  [r4 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m3,  m6

    mova    m4,  [r0 + FENC_STRIDE]
    mova    m5,  [r0 + 16 + FENC_STRIDE]
    movu    m6,  [r1 + r5]
    movu    m7,  [r1 + 16 + r5]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m0,  m6
    movu    m6,  [r2 + r5]
    movu    m7,  [r2 + 16 + r5]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m1,  m6
    movu    m6,  [r3 + r5]
    movu    m7,  [r3 + 16 + r5]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m2,  m6
    movu    m6,  [r4 + r5]
    movu    m7,  [r4 + 16 + r5]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m3,  m6

    mova    m4,  [r0 + FENC_STRIDE * 2]
    mova    m5,  [r0 + 16 + FENC_STRIDE * 2]
    movu    m6,  [r1 + r5 * 2]
    movu    m7,  [r1 + 16 + r5 * 2]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m0,  m6
    movu    m6,  [r2 + r5 * 2]
    movu    m7,  [r2 + 16 + r5 * 2]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m1,  m6
    movu    m6,  [r3 + r5 * 2]
    movu    m7,  [r3 + 16 + r5 * 2]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m2,  m6
    movu    m6,  [r4 + r5 * 2]
    movu    m7,  [r4 + 16 + r5 * 2]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m3,  m6
    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r5 * 2]
    lea     r2,  [r2 + r5 * 2]
    lea     r3,  [r3 + r5 * 2]
    lea     r4,  [r4 + r5 * 2]
    mova    m4,  [r0 + FENC_STRIDE]
    mova    m5,  [r0 + 16 + FENC_STRIDE]
    movu    m6,  [r1 + r5]
    movu    m7,  [r1 + 16 + r5]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m0,  m6
    movu    m6,  [r2 + r5]
    movu    m7,  [r2 + 16 + r5]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m1,  m6
    movu    m6,  [r3 + r5]
    movu    m7,  [r3 + 16 + r5]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m2,  m6
    movu    m6,  [r4 + r5]
    movu    m7,  [r4 + 16 + r5]
    psadbw  m6,  m4
    psadbw  m7,  m5
    pshufd  m7,  m7, 84
    paddd   m6,  m7
    paddd   m3,  m6
    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r5 * 2]
    lea     r2,  [r2 + r5 * 2]
    lea     r3,  [r3 + r5 * 2]
    lea     r4,  [r4 + r5 * 2]
%endmacro

%macro SAD_X3_32x4 0
    mova    m3,  [r0]
    mova    m4,  [r0 + 16]
    movu    m5,  [r1]
    movu    m6,  [r1 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m0,  m5
    movu    m5,  [r2]
    movu    m6,  [r2 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m1,  m5
    movu    m5,  [r3]
    movu    m6,  [r3 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m2,  m5
    lea     r0,  [r0 + FENC_STRIDE]
    lea     r1,  [r1 + r4]
    lea     r2,  [r2 + r4]
    lea     r3,  [r3 + r4]
    mova    m3,  [r0]
    mova    m4,  [r0 + 16]
    movu    m5,  [r1]
    movu    m6,  [r1 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m0,  m5
    movu    m5,  [r2]
    movu    m6,  [r2 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m1,  m5
    movu    m5,  [r3]
    movu    m6,  [r3 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m2,  m5
    lea     r0,  [r0 + FENC_STRIDE]
    lea     r1,  [r1 + r4]
    lea     r2,  [r2 + r4]
    lea     r3,  [r3 + r4]
    mova    m3,  [r0]
    mova    m4,  [r0 + 16]
    movu    m5,  [r1]
    movu    m6,  [r1 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m0,  m5
    movu    m5,  [r2]
    movu    m6,  [r2 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m1,  m5
    movu    m5,  [r3]
    movu    m6,  [r3 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m2,  m5
    lea     r0,  [r0 + FENC_STRIDE]
    lea     r1,  [r1 + r4]
    lea     r2,  [r2 + r4]
    lea     r3,  [r3 + r4]
    mova    m3,  [r0]
    mova    m4,  [r0 + 16]
    movu    m5,  [r1]
    movu    m6,  [r1 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m0,  m5
    movu    m5,  [r2]
    movu    m6,  [r2 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m1,  m5
    movu    m5,  [r3]
    movu    m6,  [r3 + 16]
    psadbw  m5,  m3
    psadbw  m6,  m4
    paddd   m5,  m6
    paddd   m2,  m5
    lea     r0,  [r0 + FENC_STRIDE]
    lea     r1,  [r1 + r4]
    lea     r2,  [r2 + r4]
    lea     r3,  [r3 + r4]
%endmacro

%macro SAD_X4_32x4 0
    mova    m4,  [r0]
    mova    m5,  [r0 + 16]
    movu    m6,  [r1]
    movu    m7,  [r1 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m0,  m6
    movu    m6,  [r2]
    movu    m7,  [r2 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m1,  m6
    movu    m6,  [r3]
    movu    m7,  [r3 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m2,  m6
    movu    m6,  [r4]
    movu    m7,  [r4 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m3,  m6
    lea     r0,  [r0 + FENC_STRIDE]
    lea     r1,  [r1 + r5]
    lea     r2,  [r2 + r5]
    lea     r3,  [r3 + r5]
    lea     r4,  [r4 + r5]
    mova    m4,  [r0]
    mova    m5,  [r0 + 16]
    movu    m6,  [r1]
    movu    m7,  [r1 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m0,  m6
    movu    m6,  [r2]
    movu    m7,  [r2 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m1,  m6
    movu    m6,  [r3]
    movu    m7,  [r3 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m2,  m6
    movu    m6,  [r4]
    movu    m7,  [r4 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m3,  m6
    lea     r0,  [r0 + FENC_STRIDE]
    lea     r1,  [r1 + r5]
    lea     r2,  [r2 + r5]
    lea     r3,  [r3 + r5]
    lea     r4,  [r4 + r5]
    mova    m4,  [r0]
    mova    m5,  [r0 + 16]
    movu    m6,  [r1]
    movu    m7,  [r1 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m0,  m6
    movu    m6,  [r2]
    movu    m7,  [r2 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m1,  m6
    movu    m6,  [r3]
    movu    m7,  [r3 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m2,  m6
    movu    m6,  [r4]
    movu    m7,  [r4 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m3,  m6
    lea     r0,  [r0 + FENC_STRIDE]
    lea     r1,  [r1 + r5]
    lea     r2,  [r2 + r5]
    lea     r3,  [r3 + r5]
    lea     r4,  [r4 + r5]
    mova    m4,  [r0]
    mova    m5,  [r0 + 16]
    movu    m6,  [r1]
    movu    m7,  [r1 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m0,  m6
    movu    m6,  [r2]
    movu    m7,  [r2 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m1,  m6
    movu    m6,  [r3]
    movu    m7,  [r3 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m2,  m6
    movu    m6,  [r4]
    movu    m7,  [r4 + 16]
    psadbw  m6,  m4
    psadbw  m7,  m5
    paddd   m6,  m7
    paddd   m3,  m6
    lea     r0,  [r0 + FENC_STRIDE]
    lea     r1,  [r1 + r5]
    lea     r2,  [r2 + r5]
    lea     r3,  [r3 + r5]
    lea     r4,  [r4 + r5]
%endmacro

%macro SAD_X3_48x4 0
    mova    m3,  [r0]
    mova    m4,  [r0 + 16]
    mova    m5,  [r0 + 32]
    movu    m6,  [r1]
    psadbw  m6,  m3
    paddd   m0,  m6
    movu    m6,  [r1 + 16]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 32]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2]
    psadbw  m6,  m3
    paddd   m1,  m6
    movu    m6,  [r2 + 16]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 32]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3]
    psadbw  m6,  m3
    paddd   m2,  m6
    movu    m6,  [r3 + 16]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 32]
    psadbw  m6,  m5
    paddd   m2,  m6

    mova    m3,  [r0 + FENC_STRIDE]
    mova    m4,  [r0 + 16 + FENC_STRIDE]
    mova    m5,  [r0 + 32 + FENC_STRIDE]
    movu    m6,  [r1 + r4]
    psadbw  m6,  m3
    paddd   m0,  m6
    movu    m6,  [r1 + 16 + r4]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 32 + r4]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + r4]
    psadbw  m6,  m3
    paddd   m1,  m6
    movu    m6,  [r2 + 16 + r4]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 32 + r4]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + r4]
    psadbw  m6,  m3
    paddd   m2,  m6
    movu    m6,  [r3 + 16 + r4]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 32 + r4]
    psadbw  m6,  m5
    paddd   m2,  m6

    mova    m3,  [r0 + FENC_STRIDE * 2]
    mova    m4,  [r0 + 16 + FENC_STRIDE * 2]
    mova    m5,  [r0 + 32 + FENC_STRIDE * 2]
    movu    m6,  [r1 + r4 * 2]
    psadbw  m6,  m3
    paddd   m0,  m6
    movu    m6,  [r1 + 16 + r4 * 2]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 32 + r4 * 2]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + r4 * 2]
    psadbw  m6,  m3
    paddd   m1,  m6
    movu    m6,  [r2 + 16 + r4 * 2]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 32 + r4 * 2]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + r4 * 2]
    psadbw  m6,  m3
    paddd   m2,  m6
    movu    m6,  [r3 + 16 + r4 * 2]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 32 + r4 * 2]
    psadbw  m6,  m5
    paddd   m2,  m6

    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r4 * 2]
    lea     r2,  [r2 + r4 * 2]
    lea     r3,  [r3 + r4 * 2]
    mova    m3,  [r0 + FENC_STRIDE]
    mova    m4,  [r0 + 16 + FENC_STRIDE]
    mova    m5,  [r0 + 32 + FENC_STRIDE]
    movu    m6,  [r1 + r4]
    psadbw  m6,  m3
    paddd   m0,  m6
    movu    m6,  [r1 + 16 + r4]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 32 + r4]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + r4]
    psadbw  m6,  m3
    paddd   m1,  m6
    movu    m6,  [r2 + 16 + r4]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 32 + r4]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + r4]
    psadbw  m6,  m3
    paddd   m2,  m6
    movu    m6,  [r3 + 16 + r4]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 32 + r4]
    psadbw  m6,  m5
    paddd   m2,  m6
    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r4 * 2]
    lea     r2,  [r2 + r4 * 2]
    lea     r3,  [r3 + r4 * 2]
%endmacro

%macro SAD_X4_48x4 0
    mova    m4,  [r0]
    mova    m5,  [r0 + 16]
    mova    m6,  [r0 + 32]
    movu    m7,  [r1]
    psadbw  m7,  m4
    paddd   m0,  m7
    movu    m7,  [r1 + 16]
    psadbw  m7,  m5
    paddd   m0,  m7
    movu    m7,  [r1 + 32]
    psadbw  m7,  m6
    paddd   m0,  m7
    movu    m7,  [r2]
    psadbw  m7,  m4
    paddd   m1,  m7
    movu    m7,  [r2 + 16]
    psadbw  m7,  m5
    paddd   m1,  m7
    movu    m7,  [r2 + 32]
    psadbw  m7,  m6
    paddd   m1,  m7
    movu    m7,  [r3]
    psadbw  m7,  m4
    paddd   m2,  m7
    movu    m7,  [r3 + 16]
    psadbw  m7,  m5
    paddd   m2,  m7
    movu    m7,  [r3 + 32]
    psadbw  m7,  m6
    paddd   m2,  m7
    movu    m7,  [r4]
    psadbw  m7,  m4
    paddd   m3,  m7
    movu    m7,  [r4 + 16]
    psadbw  m7,  m5
    paddd   m3,  m7
    movu    m7,  [r4 + 32]
    psadbw  m7,  m6
    paddd   m3,  m7

    mova    m4,  [r0 + FENC_STRIDE]
    mova    m5,  [r0 + 16 + FENC_STRIDE]
    mova    m6,  [r0 + 32 + FENC_STRIDE]
    movu    m7,  [r1 + r5]
    psadbw  m7,  m4
    paddd   m0,  m7
    movu    m7,  [r1 + 16 + r5]
    psadbw  m7,  m5
    paddd   m0,  m7
    movu    m7,  [r1 + 32 + r5]
    psadbw  m7,  m6
    paddd   m0,  m7
    movu    m7,  [r2 + r5]
    psadbw  m7,  m4
    paddd   m1,  m7
    movu    m7,  [r2 + 16 + r5]
    psadbw  m7,  m5
    paddd   m1,  m7
    movu    m7,  [r2 + 32 + r5]
    psadbw  m7,  m6
    paddd   m1,  m7
    movu    m7,  [r3 + r5]
    psadbw  m7,  m4
    paddd   m2,  m7
    movu    m7,  [r3 + 16 + r5]
    psadbw  m7,  m5
    paddd   m2,  m7
    movu    m7,  [r3 + 32 + r5]
    psadbw  m7,  m6
    paddd   m2,  m7
    movu    m7,  [r4 + r5]
    psadbw  m7,  m4
    paddd   m3,  m7
    movu    m7,  [r4 + 16 + r5]
    psadbw  m7,  m5
    paddd   m3,  m7
    movu    m7,  [r4 + 32 + r5]
    psadbw  m7,  m6
    paddd   m3,  m7

    mova    m4,  [r0 + FENC_STRIDE * 2]
    mova    m5,  [r0 + 16 + FENC_STRIDE * 2]
    mova    m6,  [r0 + 32 + FENC_STRIDE * 2]
    movu    m7,  [r1 + r5 * 2]
    psadbw  m7,  m4
    paddd   m0,  m7
    movu    m7,  [r1 + 16 + r5 * 2]
    psadbw  m7,  m5
    paddd   m0,  m7
    movu    m7,  [r1 + 32 + r5 * 2]
    psadbw  m7,  m6
    paddd   m0,  m7
    movu    m7,  [r2 + r5 * 2]
    psadbw  m7,  m4
    paddd   m1,  m7
    movu    m7,  [r2 + 16 + r5 * 2]
    psadbw  m7,  m5
    paddd   m1,  m7
    movu    m7,  [r2 + 32 + r5 * 2]
    psadbw  m7,  m6
    paddd   m1,  m7
    movu    m7,  [r3 + r5 * 2]
    psadbw  m7,  m4
    paddd   m2,  m7
    movu    m7,  [r3 + 16 + r5 * 2]
    psadbw  m7,  m5
    paddd   m2,  m7
    movu    m7,  [r3 + 32 + r5 * 2]
    psadbw  m7,  m6
    paddd   m2,  m7
    movu    m7,  [r4 + r5 * 2]
    psadbw  m7,  m4
    paddd   m3,  m7
    movu    m7,  [r4 + 16 + r5 * 2]
    psadbw  m7,  m5
    paddd   m3,  m7
    movu    m7,  [r4 + 32 + r5 * 2]
    psadbw  m7,  m6
    paddd   m3,  m7

    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r5 * 2]
    lea     r2,  [r2 + r5 * 2]
    lea     r3,  [r3 + r5 * 2]
    lea     r4,  [r4 + r5 * 2]
    mova    m4,  [r0 + FENC_STRIDE]
    mova    m5,  [r0 + 16 + FENC_STRIDE]
    mova    m6,  [r0 + 32 + FENC_STRIDE]
    movu    m7,  [r1 + r5]
    psadbw  m7,  m4
    paddd   m0,  m7
    movu    m7,  [r1 + 16 + r5]
    psadbw  m7,  m5
    paddd   m0,  m7
    movu    m7,  [r1 + 32 + r5]
    psadbw  m7,  m6
    paddd   m0,  m7
    movu    m7,  [r2 + r5]
    psadbw  m7,  m4
    paddd   m1,  m7
    movu    m7,  [r2 + 16 + r5]
    psadbw  m7,  m5
    paddd   m1,  m7
    movu    m7,  [r2 + 32 + r5]
    psadbw  m7,  m6
    paddd   m1,  m7
    movu    m7,  [r3 + r5]
    psadbw  m7,  m4
    paddd   m2,  m7
    movu    m7,  [r3 + 16 + r5]
    psadbw  m7,  m5
    paddd   m2,  m7
    movu    m7,  [r3 + 32 + r5]
    psadbw  m7,  m6
    paddd   m2,  m7
    movu    m7,  [r4 + r5]
    psadbw  m7,  m4
    paddd   m3,  m7
    movu    m7,  [r4 + 16 + r5]
    psadbw  m7,  m5
    paddd   m3,  m7
    movu    m7,  [r4 + 32 + r5]
    psadbw  m7,  m6
    paddd   m3,  m7
    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r5 * 2]
    lea     r2,  [r2 + r5 * 2]
    lea     r3,  [r3 + r5 * 2]
    lea     r4,  [r4 + r5 * 2]
%endmacro

%macro SAD_X3_64x4 0
    mova    m3,  [r0]
    mova    m4,  [r0 + 16]
    movu    m5,  [r1]
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r1 + 16]
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2]
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r2 + 16]
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3]
    psadbw  m5,  m3
    paddd   m2,  m5
    movu    m5,  [r3 + 16]
    psadbw  m5,  m4
    paddd   m2,  m5
    mova    m3,  [r0 + 32]
    mova    m4,  [r0 + 48]
    movu    m5,  [r1 + 32]
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r1 + 48]
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + 32]
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r2 + 48]
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + 32]
    psadbw  m5,  m3
    paddd   m2,  m5
    movu    m5,  [r3 + 48]
    psadbw  m5,  m4
    paddd   m2,  m5

    mova    m3,  [r0 + FENC_STRIDE]
    mova    m4,  [r0 + 16 + FENC_STRIDE]
    movu    m5,  [r1 + r4]
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r1 + 16 + r4]
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + r4]
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r2 + 16 + r4]
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + r4]
    psadbw  m5,  m3
    paddd   m2,  m5
    movu    m5,  [r3 + 16 + r4]
    psadbw  m5,  m4
    paddd   m2,  m5
    mova    m3,  [r0 + 32 + FENC_STRIDE]
    mova    m4,  [r0 + 48 + FENC_STRIDE]
    movu    m5,  [r1 + 32 + r4]
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r1 + 48 + r4]
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + 32 + r4]
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r2 + 48 + r4]
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + 32 + r4]
    psadbw  m5,  m3
    paddd   m2,  m5
    movu    m5,  [r3 + 48 + r4]
    psadbw  m5,  m4
    paddd   m2,  m5

    mova    m3,  [r0 + FENC_STRIDE * 2]
    mova    m4,  [r0 + 16 + FENC_STRIDE * 2]
    movu    m5,  [r1 + r4 * 2]
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r1 + 16 + r4 * 2]
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + r4 * 2]
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r2 + 16 + r4 * 2]
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + r4 * 2]
    psadbw  m5,  m3
    paddd   m2,  m5
    movu    m5,  [r3 + 16 + r4 * 2]
    psadbw  m5,  m4
    paddd   m2,  m5
    mova    m3,  [r0 + 32 + FENC_STRIDE * 2]
    mova    m4,  [r0 + 48 + FENC_STRIDE * 2]
    movu    m5,  [r1 + 32 + r4 * 2]
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r1 + 48 + r4 * 2]
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + 32 + r4 * 2]
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r2 + 48 + r4 * 2]
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + 32 + r4 * 2]
    psadbw  m5,  m3
    paddd   m2,  m5
    movu    m5,  [r3 + 48 + r4 * 2]
    psadbw  m5,  m4
    paddd   m2,  m5

    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r4 * 2]
    lea     r2,  [r2 + r4 * 2]
    lea     r3,  [r3 + r4 * 2]
    mova    m3,  [r0 + FENC_STRIDE]
    mova    m4,  [r0 + 16 + FENC_STRIDE]
    movu    m5,  [r1 + r4]
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r1 + 16 + r4]
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + r4]
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r2 + 16 + r4]
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + r4]
    psadbw  m5,  m3
    paddd   m2,  m5
    movu    m5,  [r3 + 16 + r4]
    psadbw  m5,  m4
    paddd   m2,  m5
    mova    m3,  [r0 + 32 + FENC_STRIDE]
    mova    m4,  [r0 + 48 + FENC_STRIDE]
    movu    m5,  [r1 + 32 + r4]
    psadbw  m5,  m3
    paddd   m0,  m5
    movu    m5,  [r1 + 48 + r4]
    psadbw  m5,  m4
    paddd   m0,  m5
    movu    m5,  [r2 + 32 + r4]
    psadbw  m5,  m3
    paddd   m1,  m5
    movu    m5,  [r2 + 48 + r4]
    psadbw  m5,  m4
    paddd   m1,  m5
    movu    m5,  [r3 + 32 + r4]
    psadbw  m5,  m3
    paddd   m2,  m5
    movu    m5,  [r3 + 48 + r4]
    psadbw  m5,  m4
    paddd   m2,  m5
    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r4 * 2]
    lea     r2,  [r2 + r4 * 2]
    lea     r3,  [r3 + r4 * 2]
%endmacro

%macro SAD_X4_64x4 0
    mova    m4,  [r0]
    mova    m5,  [r0 + 16]
    movu    m6,  [r1]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 16]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 16]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 16]
    psadbw  m6,  m5
    paddd   m2,  m6
    movu    m6,  [r4]
    psadbw  m6,  m4
    paddd   m3,  m6
    movu    m6,  [r4 + 16]
    psadbw  m6,  m5
    paddd   m3,  m6
    mova    m4,  [r0 + 32]
    mova    m5,  [r0 + 48]
    movu    m6,  [r1 + 32]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 48]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + 32]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 48]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + 32]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 48]
    psadbw  m6,  m5
    paddd   m2,  m6
    movu    m6,  [r4 + 32]
    psadbw  m6,  m4
    paddd   m3,  m6
    movu    m6,  [r4 + 48]
    psadbw  m6,  m5
    paddd   m3,  m6

    mova    m4,  [r0 + FENC_STRIDE]
    mova    m5,  [r0 + 16 + FENC_STRIDE]
    movu    m6,  [r1 + r5]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 16 + r5]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + r5]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 16 + r5]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + r5]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 16 + r5]
    psadbw  m6,  m5
    paddd   m2,  m6
    movu    m6,  [r4 + r5]
    psadbw  m6,  m4
    paddd   m3,  m6
    movu    m6,  [r4 + 16 + r5]
    psadbw  m6,  m5
    paddd   m3,  m6
    mova    m4,  [r0 + 32 + FENC_STRIDE]
    mova    m5,  [r0 + 48 + FENC_STRIDE]
    movu    m6,  [r1 + 32 + r5]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 48 + r5]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + 32 + r5]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 48 + r5]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + 32 + r5]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 48 + r5]
    psadbw  m6,  m5
    paddd   m2,  m6
    movu    m6,  [r4 + 32 + r5]
    psadbw  m6,  m4
    paddd   m3,  m6
    movu    m6,  [r4 + 48 + r5]
    psadbw  m6,  m5
    paddd   m3,  m6

    mova    m4,  [r0 + FENC_STRIDE * 2]
    mova    m5,  [r0 + 16 + FENC_STRIDE * 2]
    movu    m6,  [r1 + r5 * 2]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 16 + r5 * 2]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + r5 * 2]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 16 + r5 * 2]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + r5 * 2]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 16 + r5 * 2]
    psadbw  m6,  m5
    paddd   m2,  m6
    movu    m6,  [r4 + r5 * 2]
    psadbw  m6,  m4
    paddd   m3,  m6
    movu    m6,  [r4 + 16 + r5 * 2]
    psadbw  m6,  m5
    paddd   m3,  m6
    mova    m4,  [r0 + 32 + FENC_STRIDE * 2]
    mova    m5,  [r0 + 48 + FENC_STRIDE * 2]
    movu    m6,  [r1 + 32 + r5 * 2]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 48 + r5 * 2]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + 32 + r5 * 2]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 48 + r5 * 2]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + 32 + r5 * 2]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 48 + r5 * 2]
    psadbw  m6,  m5
    paddd   m2,  m6
    movu    m6,  [r4 + 32 + r5 * 2]
    psadbw  m6,  m4
    paddd   m3,  m6
    movu    m6,  [r4 + 48 + r5 * 2]
    psadbw  m6,  m5
    paddd   m3,  m6

    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r5 * 2]
    lea     r2,  [r2 + r5 * 2]
    lea     r3,  [r3 + r5 * 2]
    lea     r4,  [r4 + r5 * 2]
    mova    m4,  [r0 + FENC_STRIDE]
    mova    m5,  [r0 + 16 + FENC_STRIDE]
    movu    m6,  [r1 + r5]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 16 + r5]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + r5]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 16 + r5]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + r5]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 16 + r5]
    psadbw  m6,  m5
    paddd   m2,  m6
    movu    m6,  [r4 + r5]
    psadbw  m6,  m4
    paddd   m3,  m6
    movu    m6,  [r4 + 16 + r5]
    psadbw  m6,  m5
    paddd   m3,  m6
    mova    m4,  [r0 + 32 + FENC_STRIDE]
    mova    m5,  [r0 + 48 + FENC_STRIDE]
    movu    m6,  [r1 + 32 + r5]
    psadbw  m6,  m4
    paddd   m0,  m6
    movu    m6,  [r1 + 48 + r5]
    psadbw  m6,  m5
    paddd   m0,  m6
    movu    m6,  [r2 + 32 + r5]
    psadbw  m6,  m4
    paddd   m1,  m6
    movu    m6,  [r2 + 48 + r5]
    psadbw  m6,  m5
    paddd   m1,  m6
    movu    m6,  [r3 + 32 + r5]
    psadbw  m6,  m4
    paddd   m2,  m6
    movu    m6,  [r3 + 48 + r5]
    psadbw  m6,  m5
    paddd   m2,  m6
    movu    m6,  [r4 + 32 + r5]
    psadbw  m6,  m4
    paddd   m3,  m6
    movu    m6,  [r4 + 48 + r5]
    psadbw  m6,  m5
    paddd   m3,  m6
    lea     r0,  [r0 + FENC_STRIDE * 2]
    lea     r1,  [r1 + r5 * 2]
    lea     r2,  [r2 + r5 * 2]
    lea     r3,  [r3 + r5 * 2]
    lea     r4,  [r4 + r5 * 2]
%endmacro

;-----------------------------------------------------------------------------
; void pixel_sad_x3_16x16( uint8_t *fenc, uint8_t *pix0, uint8_t *pix1,
;                          uint8_t *pix2, intptr_t i_stride, int scores[3] )
;-----------------------------------------------------------------------------
%macro SAD_X 3
cglobal pixel_sad_x%1_%2x%3_mmx2, %1+2, %1+2
    SAD_X%1_2x%2P 1
%rep %3/2-1
    SAD_X%1_2x%2P 0
%endrep
    SAD_X%1_END
%endmacro

INIT_MMX
SAD_X 3, 16, 16
SAD_X 3, 16,  8
SAD_X 3,  8, 16
SAD_X 3,  8,  8
SAD_X 3,  8,  4
SAD_X 3,  4, 16
SAD_X 3,  4,  8
SAD_X 3,  4,  4
SAD_X 4, 16, 16
SAD_X 4, 16,  8
SAD_X 4,  8, 16
SAD_X 4,  8,  8
SAD_X 4,  8,  4
SAD_X 4,  4, 16
SAD_X 4,  4,  8
SAD_X 4,  4,  4



;=============================================================================
; SAD x3/x4 XMM
;=============================================================================

%macro SAD_X3_START_1x16P_SSE2 0
    mova     m2, [r0]
%if cpuflag(avx)
    psadbw   m0, m2, [r1]
    psadbw   m1, m2, [r2]
    psadbw   m2, [r3]
%else
    movu     m0, [r1]
    movu     m1, [r2]
    movu     m3, [r3]
    psadbw   m0, m2
    psadbw   m1, m2
    psadbw   m2, m3
%endif
%endmacro

%macro SAD_X3_1x16P_SSE2 2
    mova     m3, [r0+%1]
%if cpuflag(avx)
    psadbw   m4, m3, [r1+%2]
    psadbw   m5, m3, [r2+%2]
    psadbw   m3, [r3+%2]
%else
    movu     m4, [r1+%2]
    movu     m5, [r2+%2]
    movu     m6, [r3+%2]
    psadbw   m4, m3
    psadbw   m5, m3
    psadbw   m3, m6
%endif
    paddd    m0, m4
    paddd    m1, m5
    paddd    m2, m3
%endmacro

%if ARCH_X86_64
    DECLARE_REG_TMP 6
%else
    DECLARE_REG_TMP 5
%endif

%macro SAD_X3_4x16P_SSE2 2
%if %1==0
    lea  t0, [r4*3]
    SAD_X3_START_1x16P_SSE2
%else
    SAD_X3_1x16P_SSE2 FENC_STRIDE*(0+(%1&1)*4), r4*0
%endif
    SAD_X3_1x16P_SSE2 FENC_STRIDE*(1+(%1&1)*4), r4*1
    SAD_X3_1x16P_SSE2 FENC_STRIDE*(2+(%1&1)*4), r4*2
    SAD_X3_1x16P_SSE2 FENC_STRIDE*(3+(%1&1)*4), t0
%if %1 != %2-1
%if (%1&1) != 0
    add  r0, 8*FENC_STRIDE
%endif
    lea  r1, [r1+4*r4]
    lea  r2, [r2+4*r4]
    lea  r3, [r3+4*r4]
%endif
%endmacro

%macro SAD_X3_START_2x8P_SSE2 0
    movq     m3, [r0]
    movq     m0, [r1]
    movq     m1, [r2]
    movq     m2, [r3]
    movhps   m3, [r0+FENC_STRIDE]
    movhps   m0, [r1+r4]
    movhps   m1, [r2+r4]
    movhps   m2, [r3+r4]
    psadbw   m0, m3
    psadbw   m1, m3
    psadbw   m2, m3
%endmacro

%macro SAD_X3_2x8P_SSE2 4
    movq     m6, [r0+%1]
    movq     m3, [r1+%2]
    movq     m4, [r2+%2]
    movq     m5, [r3+%2]
    movhps   m6, [r0+%3]
    movhps   m3, [r1+%4]
    movhps   m4, [r2+%4]
    movhps   m5, [r3+%4]
    psadbw   m3, m6
    psadbw   m4, m6
    psadbw   m5, m6
    paddd    m0, m3
    paddd    m1, m4
    paddd    m2, m5
%endmacro

%macro SAD_X4_START_2x8P_SSE2 0
    movq     m4, [r0]
    movq     m0, [r1]
    movq     m1, [r2]
    movq     m2, [r3]
    movq     m3, [r4]
    movhps   m4, [r0+FENC_STRIDE]
    movhps   m0, [r1+r5]
    movhps   m1, [r2+r5]
    movhps   m2, [r3+r5]
    movhps   m3, [r4+r5]
    psadbw   m0, m4
    psadbw   m1, m4
    psadbw   m2, m4
    psadbw   m3, m4
%endmacro

%macro SAD_X4_2x8P_SSE2 4
    movq     m6, [r0+%1]
    movq     m4, [r1+%2]
    movq     m5, [r2+%2]
    movhps   m6, [r0+%3]
    movhps   m4, [r1+%4]
    movhps   m5, [r2+%4]
    psadbw   m4, m6
    psadbw   m5, m6
    paddd    m0, m4
    paddd    m1, m5
    movq     m4, [r3+%2]
    movq     m5, [r4+%2]
    movhps   m4, [r3+%4]
    movhps   m5, [r4+%4]
    psadbw   m4, m6
    psadbw   m5, m6
    paddd    m2, m4
    paddd    m3, m5
%endmacro

%macro SAD_X4_START_1x16P_SSE2 0
    mova     m3, [r0]
%if cpuflag(avx)
    psadbw   m0, m3, [r1]
    psadbw   m1, m3, [r2]
    psadbw   m2, m3, [r3]
    psadbw   m3, [r4]
%else
    movu     m0, [r1]
    movu     m1, [r2]
    movu     m2, [r3]
    movu     m4, [r4]
    psadbw   m0, m3
    psadbw   m1, m3
    psadbw   m2, m3
    psadbw   m3, m4
%endif
%endmacro

%macro SAD_X4_1x16P_SSE2 2
    mova     m6, [r0+%1]
%if cpuflag(avx)
    psadbw   m4, m6, [r1+%2]
    psadbw   m5, m6, [r2+%2]
%else
    movu     m4, [r1+%2]
    movu     m5, [r2+%2]
    psadbw   m4, m6
    psadbw   m5, m6
%endif
    paddd    m0, m4
    paddd    m1, m5
%if cpuflag(avx)
    psadbw   m4, m6, [r3+%2]
    psadbw   m5, m6, [r4+%2]
%else
    movu     m4, [r3+%2]
    movu     m5, [r4+%2]
    psadbw   m4, m6
    psadbw   m5, m6
%endif
    paddd    m2, m4
    paddd    m3, m5
%endmacro

%macro SAD_X4_4x16P_SSE2 2
%if %1==0
    lea  r6, [r5*3]
    SAD_X4_START_1x16P_SSE2
%else
    SAD_X4_1x16P_SSE2 FENC_STRIDE*(0+(%1&1)*4), r5*0
%endif
    SAD_X4_1x16P_SSE2 FENC_STRIDE*(1+(%1&1)*4), r5*1
    SAD_X4_1x16P_SSE2 FENC_STRIDE*(2+(%1&1)*4), r5*2
    SAD_X4_1x16P_SSE2 FENC_STRIDE*(3+(%1&1)*4), r6
%if %1 != %2-1
%if (%1&1) != 0
    add  r0, 8*FENC_STRIDE
%endif
    lea  r1, [r1+4*r5]
    lea  r2, [r2+4*r5]
    lea  r3, [r3+4*r5]
    lea  r4, [r4+4*r5]
%endif
%endmacro

%macro SAD_X3_4x8P_SSE2 2
%if %1==0
    lea  t0, [r4*3]
    SAD_X3_START_2x8P_SSE2
%else
    SAD_X3_2x8P_SSE2 FENC_STRIDE*(0+(%1&1)*4), r4*0, FENC_STRIDE*(1+(%1&1)*4), r4*1
%endif
    SAD_X3_2x8P_SSE2 FENC_STRIDE*(2+(%1&1)*4), r4*2, FENC_STRIDE*(3+(%1&1)*4), t0
%if %1 != %2-1
%if (%1&1) != 0
    add  r0, 8*FENC_STRIDE
%endif
    lea  r1, [r1+4*r4]
    lea  r2, [r2+4*r4]
    lea  r3, [r3+4*r4]
%endif
%endmacro

%macro SAD_X4_4x8P_SSE2 2
%if %1==0
    lea    r6, [r5*3]
    SAD_X4_START_2x8P_SSE2
%else
    SAD_X4_2x8P_SSE2 FENC_STRIDE*(0+(%1&1)*4), r5*0, FENC_STRIDE*(1+(%1&1)*4), r5*1
%endif
    SAD_X4_2x8P_SSE2 FENC_STRIDE*(2+(%1&1)*4), r5*2, FENC_STRIDE*(3+(%1&1)*4), r6
%if %1 != %2-1
%if (%1&1) != 0
    add  r0, 8*FENC_STRIDE
%endif
    lea  r1, [r1+4*r5]
    lea  r2, [r2+4*r5]
    lea  r3, [r3+4*r5]
    lea  r4, [r4+4*r5]
%endif
%endmacro

%macro SAD_X3_END_SSE2 1
    movifnidn r5, r5mp
    movhlps    m3, m0
    movhlps    m4, m1
    movhlps    m5, m2
    paddd      m0, m3
    paddd      m1, m4
    paddd      m2, m5
    movd   [r5+0], m0
    movd   [r5+4], m1
    movd   [r5+8], m2
    RET
%endmacro

%macro SAD_X4_END_SSE2 1
    mov      r0, r6mp
    psllq      m1, 32
    psllq      m3, 32
    paddd      m0, m1
    paddd      m2, m3
    movhlps    m1, m0
    movhlps    m3, m2
    paddd      m0, m1
    paddd      m2, m3
    movq   [r0+0], m0
    movq   [r0+8], m2
    RET
%endmacro

%macro SAD_X3_START_2x16P_AVX2 0
    movu    m3, [r0] ; assumes FENC_STRIDE == 16
    movu   xm0, [r1]
    movu   xm1, [r2]
    movu   xm2, [r3]
    vinserti128  m0, m0, [r1+r4], 1
    vinserti128  m1, m1, [r2+r4], 1
    vinserti128  m2, m2, [r3+r4], 1
    psadbw  m0, m3
    psadbw  m1, m3
    psadbw  m2, m3
%endmacro

%macro SAD_X3_2x16P_AVX2 3
    movu    m3, [r0+%1] ; assumes FENC_STRIDE == 16
    movu   xm4, [r1+%2]
    movu   xm5, [r2+%2]
    movu   xm6, [r3+%2]
    vinserti128  m4, m4, [r1+%3], 1
    vinserti128  m5, m5, [r2+%3], 1
    vinserti128  m6, m6, [r3+%3], 1
    psadbw  m4, m3
    psadbw  m5, m3
    psadbw  m6, m3
    paddw   m0, m4
    paddw   m1, m5
    paddw   m2, m6
%endmacro

%macro SAD_X3_4x16P_AVX2 2
%if %1==0
    lea  t0, [r4*3]
    SAD_X3_START_2x16P_AVX2
%else
    SAD_X3_2x16P_AVX2 FENC_STRIDE*(0+(%1&1)*4), r4*0, r4*1
%endif
    SAD_X3_2x16P_AVX2 FENC_STRIDE*(2+(%1&1)*4), r4*2, t0
%if %1 != %2-1
%if (%1&1) != 0
    add  r0, 8*FENC_STRIDE
%endif
    lea  r1, [r1+4*r4]
    lea  r2, [r2+4*r4]
    lea  r3, [r3+4*r4]
%endif
%endmacro

%macro SAD_X4_START_2x16P_AVX2 0
    vbroadcasti128 m4, [r0]
    vbroadcasti128 m5, [r0+FENC_STRIDE]
    movu   xm0, [r1]
    movu   xm1, [r2]
    movu   xm2, [r1+r5]
    movu   xm3, [r2+r5]
    vinserti128 m0, m0, [r3], 1
    vinserti128 m1, m1, [r4], 1
    vinserti128 m2, m2, [r3+r5], 1
    vinserti128 m3, m3, [r4+r5], 1
    psadbw  m0, m4
    psadbw  m1, m4
    psadbw  m2, m5
    psadbw  m3, m5
    paddw   m0, m2
    paddw   m1, m3
%endmacro

%macro SAD_X4_2x16P_AVX2 4
    vbroadcasti128 m6, [r0+%1]
    vbroadcasti128 m7, [r0+%3]
    movu   xm2, [r1+%2]
    movu   xm3, [r2+%2]
    movu   xm4, [r1+%4]
    movu   xm5, [r2+%4]
    vinserti128 m2, m2, [r3+%2], 1
    vinserti128 m3, m3, [r4+%2], 1
    vinserti128 m4, m4, [r3+%4], 1
    vinserti128 m5, m5, [r4+%4], 1
    psadbw  m2, m6
    psadbw  m3, m6
    psadbw  m4, m7
    psadbw  m5, m7
    paddd   m0, m2
    paddd   m1, m3
    paddd   m0, m4
    paddd   m1, m5
%endmacro

%macro SAD_X4_4x16P_AVX2 2
%if %1==0
    lea  r6, [r5*3]
    SAD_X4_START_2x16P_AVX2
%else
    SAD_X4_2x16P_AVX2 FENC_STRIDE*(0+(%1&1)*4), r5*0, FENC_STRIDE*(1+(%1&1)*4), r5*1
%endif
    SAD_X4_2x16P_AVX2 FENC_STRIDE*(2+(%1&1)*4), r5*2, FENC_STRIDE*(3+(%1&1)*4), r6
%if %1 != %2-1
%if (%1&1) != 0
    add  r0, 8*FENC_STRIDE
%endif
    lea  r1, [r1+4*r5]
    lea  r2, [r2+4*r5]
    lea  r3, [r3+4*r5]
    lea  r4, [r4+4*r5]
%endif
%endmacro

%macro SAD_X4_START_2x32P_AVX2 0
    mova        m4, [r0]
    movu        m0, [r1]
    movu        m2, [r2]
    movu        m1, [r3]
    movu        m3, [r4]
    psadbw      m0, m4
    psadbw      m2, m4
    psadbw      m1, m4
    psadbw      m3, m4
    packusdw    m0, m2
    packusdw    m1, m3

    mova        m6, [r0+FENC_STRIDE]
    movu        m2, [r1+r5]
    movu        m4, [r2+r5]
    movu        m3, [r3+r5]
    movu        m5, [r4+r5]
    psadbw      m2, m6
    psadbw      m4, m6
    psadbw      m3, m6
    psadbw      m5, m6
    packusdw    m2, m4
    packusdw    m3, m5
    paddd       m0, m2
    paddd       m1, m3
%endmacro

%macro SAD_X4_2x32P_AVX2 4
    mova        m6, [r0+%1]
    movu        m2, [r1+%2]
    movu        m4, [r2+%2]
    movu        m3, [r3+%2]
    movu        m5, [r4+%2]
    psadbw      m2, m6
    psadbw      m4, m6
    psadbw      m3, m6
    psadbw      m5, m6
    packusdw    m2, m4
    packusdw    m3, m5
    paddd       m0, m2
    paddd       m1, m3

    mova        m6, [r0+%3]
    movu        m2, [r1+%4]
    movu        m4, [r2+%4]
    movu        m3, [r3+%4]
    movu        m5, [r4+%4]
    psadbw      m2, m6
    psadbw      m4, m6
    psadbw      m3, m6
    psadbw      m5, m6
    packusdw    m2, m4
    packusdw    m3, m5
    paddd       m0, m2
    paddd       m1, m3
%endmacro

%macro SAD_X4_4x32P_AVX2 2
%if %1==0
    lea  r6, [r5*3]
    SAD_X4_START_2x32P_AVX2
%else
    SAD_X4_2x32P_AVX2 FENC_STRIDE*(0+(%1&1)*4), r5*0, FENC_STRIDE*(1+(%1&1)*4), r5*1
%endif
    SAD_X4_2x32P_AVX2 FENC_STRIDE*(2+(%1&1)*4), r5*2, FENC_STRIDE*(3+(%1&1)*4), r6
%if %1 != %2-1
%if (%1&1) != 0
    add  r0, 8*FENC_STRIDE
%endif
    lea  r1, [r1+4*r5]
    lea  r2, [r2+4*r5]
    lea  r3, [r3+4*r5]
    lea  r4, [r4+4*r5]
%endif
%endmacro

%macro SAD_X3_END_AVX2 0
    movifnidn r5, r5mp
    packssdw  m0, m1        ; 0 0 1 1 0 0 1 1
    packssdw  m2, m2        ; 2 2 _ _ 2 2 _ _
    phaddd    m0, m2        ; 0 1 2 _ 0 1 2 _
    vextracti128 xm1, m0, 1
    paddd    xm0, xm1       ; 0 1 2 _
    mova    [r5], xm0
    RET
%endmacro

%macro SAD_X4_END_AVX2 0
    mov       r0, r6mp
    pshufd     m0, m0, 0x8
    pshufd     m1, m1, 0x8
    vextracti128 xm2, m0, 1
    vextracti128 xm3, m1, 1
    punpcklqdq   xm0, xm1
    punpcklqdq   xm2, xm3
    phaddd   xm0, xm2       ; 0 1 2 3
    mova    [r0], xm0
    RET
%endmacro

%macro SAD_X4_32P_END_AVX2 0
    mov          r0, r6mp
    vextracti128 xm2, m0, 1
    vextracti128 xm3, m1, 1
    paddd        xm0, xm2
    paddd        xm1, xm3
    phaddd       xm0, xm1
    mova         [r0], xm0
    RET
%endmacro

;-----------------------------------------------------------------------------
; void pixel_sad_x3_16x16( uint8_t *fenc, uint8_t *pix0, uint8_t *pix1,
;                          uint8_t *pix2, intptr_t i_stride, int scores[3] )
;-----------------------------------------------------------------------------
%macro SAD_X_SSE2 4
cglobal pixel_sad_x%1_%2x%3, 2+%1,3+%1,%4
%assign x 0
%rep %3/4
    SAD_X%1_4x%2P_SSE2 x, %3/4
%assign x x+1
%endrep
%if %3 == 64
    SAD_X%1_END_SSE2 1
%else
    SAD_X%1_END_SSE2 0
%endif
%endmacro

%macro SAD_X3_W12 0
cglobal pixel_sad_x3_12x16, 5, 7, 8
    mova  m4,  [MSK]
    pxor  m0,  m0
    pxor  m1,  m1
    pxor  m2,  m2

    SAD_X3_12x4
    SAD_X3_12x4
    SAD_X3_12x4
    SAD_X3_12x4
    SAD_X3_END_SSE2 1
%endmacro

%macro SAD_X4_W12 0
cglobal pixel_sad_x4_12x16, 6, 8, 8
    mova  m6,  [MSK]
    pxor  m0,  m0
    pxor  m1,  m1
    pxor  m2,  m2
    pxor  m3,  m3

    SAD_X4_12x4
    SAD_X4_12x4
    SAD_X4_12x4
    SAD_X4_12x4
    SAD_X4_END_SSE2 1
%endmacro

%macro SAD_X3_W24 0
cglobal pixel_sad_x3_24x32, 5, 7, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    mov   r6, 32

.loop:
    SAD_X3_24x4
    SAD_X3_24x4
    SAD_X3_24x4
    SAD_X3_24x4

    sub r6,  16
    cmp r6,  0
jnz .loop
    SAD_X3_END_SSE2 1
%endmacro

%macro SAD_X4_W24 0
%if ARCH_X86_64 == 1
cglobal pixel_sad_x4_24x32, 6, 8, 8
%define count r7
%else
cglobal pixel_sad_x4_24x32, 6, 7, 8, 0-4
%define count dword [rsp]
%endif
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3
    mov   count, 32

.loop:
    SAD_X4_24x4
    SAD_X4_24x4
    SAD_X4_24x4
    SAD_X4_24x4

    sub count,  16
    jnz .loop
    SAD_X4_END_SSE2 1

%endmacro

%macro SAD_X3_W32 0
cglobal pixel_sad_x3_32x8, 5, 6, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2

    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_END_SSE2 1

cglobal pixel_sad_x3_32x16, 5, 6, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2

    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_END_SSE2 1

cglobal pixel_sad_x3_32x24, 5, 6, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2

    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_END_SSE2 1

cglobal pixel_sad_x3_32x32, 5, 7, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    mov   r6, 32

.loop:
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4

    sub r6,  16
    cmp r6,  0
jnz .loop
    SAD_X3_END_SSE2 1

cglobal pixel_sad_x3_32x64, 5, 7, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    mov   r6, 64

.loop1:
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4
    SAD_X3_32x4

    sub r6,  16
    cmp r6,  0
jnz .loop1
    SAD_X3_END_SSE2 1
%endmacro

%macro SAD_X4_W32 0
cglobal pixel_sad_x4_32x8, 6, 7, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3

    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_END_SSE2 1

cglobal pixel_sad_x4_32x16, 6, 7, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3

    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_END_SSE2 1

cglobal pixel_sad_x4_32x24, 6, 7, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3

    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_END_SSE2 1

%if ARCH_X86_64 == 1
cglobal pixel_sad_x4_32x32, 6, 8, 8
%define count r7
%else
cglobal pixel_sad_x4_32x32, 6, 7, 8, 0-4
%define count dword [rsp]
%endif
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3
    mov   count, 32

.loop:
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4

    sub count,  16
    jnz .loop
    SAD_X4_END_SSE2 1

%if ARCH_X86_64 == 1
cglobal pixel_sad_x4_32x64, 6, 8, 8
%define count r7
%else
cglobal pixel_sad_x4_32x64, 6, 7, 8, 0-4
%define count dword [rsp]
%endif
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3
    mov   count, 64

.loop:
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4
    SAD_X4_32x4

    sub count,  16
    jnz .loop
    SAD_X4_END_SSE2 1

%endmacro

%macro SAD_X3_W48 0
cglobal pixel_sad_x3_48x64, 5, 7, 8
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    mov   r6, 64

.loop:
    SAD_X3_48x4
    SAD_X3_48x4
    SAD_X3_48x4
    SAD_X3_48x4

    sub r6,  16
    jnz .loop
    SAD_X3_END_SSE2 1
%endmacro

%macro SAD_X4_W48 0
%if ARCH_X86_64 == 1
cglobal pixel_sad_x4_48x64, 6, 8, 8
%define count r7
%else
cglobal pixel_sad_x4_48x64, 6, 7, 8, 0-4
%define count dword [rsp]
%endif
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3
    mov   count, 64

.loop:
    SAD_X4_48x4
    SAD_X4_48x4
    SAD_X4_48x4
    SAD_X4_48x4

    sub count,  16
    jnz .loop
    SAD_X4_END_SSE2 1
%endmacro

%macro SAD_X3_W64 0
cglobal pixel_sad_x3_64x16, 5, 7, 7
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    mov   r6, 16

.loop:
    SAD_X3_64x4
    SAD_X3_64x4

    sub r6,  8
    jnz .loop
    SAD_X3_END_SSE2 1

cglobal pixel_sad_x3_64x32, 5, 7, 7
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    mov   r6, 32

.loop:
    SAD_X3_64x4
    SAD_X3_64x4

    sub r6,  8
    jnz .loop
    SAD_X3_END_SSE2 1

cglobal pixel_sad_x3_64x48, 5, 7, 7
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    mov   r6, 48

.loop:
    SAD_X3_64x4
    SAD_X3_64x4

    sub r6,  8
    jnz .loop
    SAD_X3_END_SSE2 1

cglobal pixel_sad_x3_64x64, 5, 7, 7
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    mov   r6, 64

.loop:
    SAD_X3_64x4
    SAD_X3_64x4

    sub r6,  8
    jnz .loop
    SAD_X3_END_SSE2 1
%endmacro

%macro SAD_X4_W64 0
%if ARCH_X86_64 == 1
cglobal pixel_sad_x4_64x16, 6, 8, 8
%define count r7
%else
cglobal pixel_sad_x4_64x16, 6, 7, 8, 0-4
%define count dword [rsp]
%endif
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3
    mov   count, 16

.loop:
    SAD_X4_64x4
    SAD_X4_64x4

    sub count,  8
    jnz .loop
    SAD_X4_END_SSE2 1

%if ARCH_X86_64 == 1
cglobal pixel_sad_x4_64x32, 6, 8, 8
%define count r7
%else
cglobal pixel_sad_x4_64x32, 6, 7, 8, 0-4
%define count dword [rsp]
%endif
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3
    mov   count, 32

.loop:
    SAD_X4_64x4
    SAD_X4_64x4

    sub count,  8
    jnz .loop
    SAD_X4_END_SSE2 1

%if ARCH_X86_64 == 1
cglobal pixel_sad_x4_64x48, 6, 8, 8
%define count r7
%else
cglobal pixel_sad_x4_64x48, 6, 7, 8, 0-4
%define count dword [rsp]
%endif
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3
    mov   count, 48

.loop:
    SAD_X4_64x4
    SAD_X4_64x4

    sub count,  8
    jnz .loop
    SAD_X4_END_SSE2 1

%if ARCH_X86_64 == 1
cglobal pixel_sad_x4_64x64, 6, 8, 8
%define count r7
%else
cglobal pixel_sad_x4_64x64, 6, 7, 8, 0-4
%define count dword [rsp]
%endif
    pxor  m0, m0
    pxor  m1, m1
    pxor  m2, m2
    pxor  m3, m3
    mov   count, 64

.loop:
    SAD_X4_64x4
    SAD_X4_64x4

    sub count,  8
    jnz .loop
    SAD_X4_END_SSE2 1
%endmacro

%if ARCH_X86_64 == 1 && HIGH_BIT_DEPTH == 0
INIT_YMM avx2
%macro SAD_X4_64x8_AVX2 0
    movu            m4, [r0]
    movu            m5, [r1]
    movu            m6, [r2]
    movu            m7, [r3]
    movu            m8, [r4]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + mmsize]
    movu            m5, [r1 + mmsize]
    movu            m6, [r2 + mmsize]
    movu            m7, [r3 + mmsize]
    movu            m8, [r4 + mmsize]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE]
    movu            m5, [r1 + r5]
    movu            m6, [r2 + r5]
    movu            m7, [r3 + r5]
    movu            m8, [r4 + r5]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE + mmsize]
    movu            m5, [r1 + r5 + mmsize]
    movu            m6, [r2 + r5 + mmsize]
    movu            m7, [r3 + r5 + mmsize]
    movu            m8, [r4 + r5 + mmsize]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 2]
    movu            m5, [r1 + r5 * 2]
    movu            m6, [r2 + r5 * 2]
    movu            m7, [r3 + r5 * 2]
    movu            m8, [r4 + r5 * 2]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 2 + mmsize]
    movu            m5, [r1 + r5 * 2 + mmsize]
    movu            m6, [r2 + r5 * 2 + mmsize]
    movu            m7, [r3 + r5 * 2 + mmsize]
    movu            m8, [r4 + r5 * 2 + mmsize]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 3]
    movu            m5, [r1 + r7]
    movu            m6, [r2 + r7]
    movu            m7, [r3 + r7]
    movu            m8, [r4 + r7]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 3 + mmsize]
    movu            m5, [r1 + r7 + mmsize]
    movu            m6, [r2 + r7 + mmsize]
    movu            m7, [r3 + r7 + mmsize]
    movu            m8, [r4 + r7 + mmsize]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    movu            m4, [r0]
    movu            m5, [r1]
    movu            m6, [r2]
    movu            m7, [r3]
    movu            m8, [r4]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + mmsize]
    movu            m5, [r1 + mmsize]
    movu            m6, [r2 + mmsize]
    movu            m7, [r3 + mmsize]
    movu            m8, [r4 + mmsize]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE]
    movu            m5, [r1 + r5]
    movu            m6, [r2 + r5]
    movu            m7, [r3 + r5]
    movu            m8, [r4 + r5]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE + mmsize]
    movu            m5, [r1 + r5 + mmsize]
    movu            m6, [r2 + r5 + mmsize]
    movu            m7, [r3 + r5 + mmsize]
    movu            m8, [r4 + r5 + mmsize]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 2]
    movu            m5, [r1 + r5 * 2]
    movu            m6, [r2 + r5 * 2]
    movu            m7, [r3 + r5 * 2]
    movu            m8, [r4 + r5 * 2]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 2 + mmsize]
    movu            m5, [r1 + r5 * 2 + mmsize]
    movu            m6, [r2 + r5 * 2 + mmsize]
    movu            m7, [r3 + r5 * 2 + mmsize]
    movu            m8, [r4 + r5 * 2 + mmsize]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 3]
    movu            m5, [r1 + r7]
    movu            m6, [r2 + r7]
    movu            m7, [r3 + r7]
    movu            m8, [r4 + r7]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 3 + mmsize]
    movu            m5, [r1 + r7 + mmsize]
    movu            m6, [r2 + r7 + mmsize]
    movu            m7, [r3 + r7 + mmsize]
    movu            m8, [r4 + r7 + mmsize]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4
%endmacro

%macro PIXEL_SAD_X4_END_AVX2 0
    vextracti128   xm4, m0, 1
    vextracti128   xm5, m1, 1
    vextracti128   xm6, m2, 1
    vextracti128   xm7, m3, 1
    paddd           m0, m4
    paddd           m1, m5
    paddd           m2, m6
    paddd           m3, m7
    pshufd         xm4, xm0, 2
    pshufd         xm5, xm1, 2
    pshufd         xm6, xm2, 2
    pshufd         xm7, xm3, 2
    paddd           m0, m4
    paddd           m1, m5
    paddd           m2, m6
    paddd           m3, m7

    movd            [r6 + 0], xm0
    movd            [r6 + 4], xm1
    movd            [r6 + 8], xm2
    movd            [r6 + 12], xm3
%endmacro

cglobal pixel_sad_x4_64x16, 7,8,10
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    pxor            m3, m3
    lea             r7, [r5 * 3]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2
    PIXEL_SAD_X4_END_AVX2
    RET

cglobal pixel_sad_x4_64x32, 7,8,10
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    pxor            m3, m3
    lea             r7, [r5 * 3]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2
    PIXEL_SAD_X4_END_AVX2
    RET

cglobal pixel_sad_x4_64x48, 7,8,10
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    pxor            m3, m3
    lea             r7, [r5 * 3]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2
    PIXEL_SAD_X4_END_AVX2
    RET

cglobal pixel_sad_x4_64x64, 7,8,10
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    pxor            m3, m3
    lea             r7, [r5 * 3]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_64x8_AVX2
    PIXEL_SAD_X4_END_AVX2
    RET

%macro SAD_X4_48x8_AVX2 0
    movu            m4, [r0]
    movu            m5, [r1]
    movu            m6, [r2]
    movu            m7, [r3]
    movu            m8, [r4]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            xm4, [r0 + mmsize]
    movu            xm5, [r1 + mmsize]
    movu            xm6, [r2 + mmsize]
    movu            xm7, [r3 + mmsize]
    movu            xm8, [r4 + mmsize]

    vinserti128     m4, m4, [r0 + FENC_STRIDE], 1
    vinserti128     m5, m5, [r1 + r5], 1
    vinserti128     m6, m6, [r2 + r5], 1
    vinserti128     m7, m7, [r3 + r5], 1
    vinserti128     m8, m8, [r4 + r5], 1

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE + mmsize/2]
    movu            m5, [r1 + r5 + mmsize/2]
    movu            m6, [r2 + r5 + mmsize/2]
    movu            m7, [r3 + r5 + mmsize/2]
    movu            m8, [r4 + r5 + mmsize/2]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 2]
    movu            m5, [r1 + r5 * 2]
    movu            m6, [r2 + r5 * 2]
    movu            m7, [r3 + r5 * 2]
    movu            m8, [r4 + r5 * 2]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            xm4, [r0 + FENC_STRIDE * 2 + mmsize]
    movu            xm5, [r1 + r5 * 2 + mmsize]
    movu            xm6, [r2 + r5 * 2 + mmsize]
    movu            xm7, [r3 + r5 * 2 + mmsize]
    movu            xm8, [r4 + r5 * 2 + mmsize]
    vinserti128     m4, m4, [r0 + FENC_STRIDE * 3], 1
    vinserti128     m5, m5, [r1 + r7], 1
    vinserti128     m6, m6, [r2 + r7], 1
    vinserti128     m7, m7, [r3 + r7], 1
    vinserti128     m8, m8, [r4 + r7], 1

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 3 + mmsize/2]
    movu            m5, [r1 + r7 + mmsize/2]
    movu            m6, [r2 + r7 + mmsize/2]
    movu            m7, [r3 + r7 + mmsize/2]
    movu            m8, [r4 + r7 + mmsize/2]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    movu            m4, [r0]
    movu            m5, [r1]
    movu            m6, [r2]
    movu            m7, [r3]
    movu            m8, [r4]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            xm4, [r0 + mmsize]
    movu            xm5, [r1 + mmsize]
    movu            xm6, [r2 + mmsize]
    movu            xm7, [r3 + mmsize]
    movu            xm8, [r4 + mmsize]
    vinserti128     m4, m4, [r0 + FENC_STRIDE], 1
    vinserti128     m5, m5, [r1 + r5], 1
    vinserti128     m6, m6, [r2 + r5], 1
    vinserti128     m7, m7, [r3 + r5], 1
    vinserti128     m8, m8, [r4 + r5], 1

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE + mmsize/2]
    movu            m5, [r1 + r5 + mmsize/2]
    movu            m6, [r2 + r5 + mmsize/2]
    movu            m7, [r3 + r5 + mmsize/2]
    movu            m8, [r4 + r5 + mmsize/2]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 2]
    movu            m5, [r1 + r5 * 2]
    movu            m6, [r2 + r5 * 2]
    movu            m7, [r3 + r5 * 2]
    movu            m8, [r4 + r5 * 2]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            xm4, [r0 + FENC_STRIDE * 2 + mmsize]
    movu            xm5, [r1 + r5 * 2 + mmsize]
    movu            xm6, [r2 + r5 * 2 + mmsize]
    movu            xm7, [r3 + r5 * 2 + mmsize]
    movu            xm8, [r4 + r5 * 2 + mmsize]
    vinserti128     m4, m4, [r0 + FENC_STRIDE * 3], 1
    vinserti128     m5, m5, [r1 + r7], 1
    vinserti128     m6, m6, [r2 + r7], 1
    vinserti128     m7, m7, [r3 + r7], 1
    vinserti128     m8, m8, [r4 + r7], 1

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4

    movu            m4, [r0 + FENC_STRIDE * 3 + mmsize/2]
    movu            m5, [r1 + r7 + mmsize/2]
    movu            m6, [r2 + r7 + mmsize/2]
    movu            m7, [r3 + r7 + mmsize/2]
    movu            m8, [r4 + r7 + mmsize/2]

    psadbw          m9, m4, m5
    paddd           m0, m9
    psadbw          m5, m4, m6
    paddd           m1, m5
    psadbw          m6, m4, m7
    paddd           m2, m6
    psadbw          m4, m8
    paddd           m3, m4
%endmacro

INIT_YMM avx2
cglobal pixel_sad_x4_48x64, 7,8,10
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    pxor            m3, m3
    lea             r7, [r5 * 3]

    SAD_X4_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r5 * 4]
    lea             r2, [r2 + r5 * 4]
    lea             r3, [r3 + r5 * 4]
    lea             r4, [r4 + r5 * 4]

    SAD_X4_48x8_AVX2
    PIXEL_SAD_X4_END_AVX2
    RET
%endif

INIT_XMM sse2
SAD_X_SSE2 3, 16, 16, 7
SAD_X_SSE2 3, 16,  8, 7
SAD_X_SSE2 3,  8, 16, 7
SAD_X_SSE2 3,  8,  8, 7
SAD_X_SSE2 3,  8,  4, 7
SAD_X_SSE2 4, 16, 16, 7
SAD_X_SSE2 4, 16,  8, 7
SAD_X_SSE2 4,  8, 16, 7
SAD_X_SSE2 4,  8,  8, 7
SAD_X_SSE2 4,  8,  4, 7

INIT_XMM sse3
SAD_X_SSE2 3, 16, 16, 7
SAD_X_SSE2 3, 16,  8, 7
SAD_X_SSE2 3, 16,  4, 7
SAD_X_SSE2 4, 16, 16, 7
SAD_X_SSE2 4, 16,  8, 7
SAD_X_SSE2 4, 16,  4, 7

INIT_XMM ssse3
SAD_X3_W12
SAD_X3_W32
SAD_X3_W24
SAD_X3_W48
SAD_X3_W64
SAD_X_SSE2  3, 16, 64, 7
SAD_X_SSE2  3, 16, 32, 7
SAD_X_SSE2  3, 16, 16, 7
SAD_X_SSE2  3, 16, 12, 7
SAD_X_SSE2  3, 16,  8, 7
SAD_X_SSE2  3,  8, 32, 7
SAD_X_SSE2  3,  8, 16, 7
SAD_X4_W12
SAD_X4_W24
SAD_X4_W32
SAD_X4_W48
SAD_X4_W64
SAD_X_SSE2  4, 16, 64, 7
SAD_X_SSE2  4, 16, 32, 7
SAD_X_SSE2  4, 16, 16, 7
SAD_X_SSE2  4, 16, 12, 7
SAD_X_SSE2  4, 16,  8, 7
SAD_X_SSE2  4,  8, 32, 7
SAD_X_SSE2  4,  8, 16, 7
SAD_X_SSE2  4,  8,  8, 7
SAD_X_SSE2  4,  8,  4, 7

INIT_XMM avx
SAD_X3_W12
SAD_X3_W32
SAD_X3_W24
SAD_X3_W48
SAD_X3_W64
SAD_X_SSE2 3, 16, 64, 7
SAD_X_SSE2 3, 16, 32, 6
SAD_X_SSE2 3, 16, 16, 6
SAD_X_SSE2 3, 16, 12, 6
SAD_X_SSE2 3, 16,  8, 6
SAD_X_SSE2 3, 16,  4, 6
SAD_X4_W12
SAD_X4_W24
SAD_X4_W32
SAD_X4_W48
SAD_X4_W64
SAD_X_SSE2 4, 16, 64, 7
SAD_X_SSE2 4, 16, 32, 7
SAD_X_SSE2 4, 16, 16, 7
SAD_X_SSE2 4, 16, 12, 7
SAD_X_SSE2 4, 16,  8, 7
SAD_X_SSE2 4, 16,  4, 7

%macro SAD_X_AVX2 4
cglobal pixel_sad_x%1_%2x%3, 2+%1,3+%1,%4
%assign x 0
%rep %3/4
    SAD_X%1_4x%2P_AVX2 x, %3/4
%assign x x+1
%endrep

  %if (%1==4) && (%2==32)
    SAD_X%1_32P_END_AVX2
  %else
    SAD_X%1_END_AVX2
  %endif
%endmacro

INIT_YMM avx2
SAD_X_AVX2 3, 16, 32, 7
SAD_X_AVX2 3, 16, 16, 7
SAD_X_AVX2 3, 16, 12, 7
SAD_X_AVX2 3, 16,  8, 7
SAD_X_AVX2 4, 16, 32, 8
SAD_X_AVX2 4, 16, 16, 8
SAD_X_AVX2 4, 16, 12, 8
SAD_X_AVX2 4, 16,  8, 8

SAD_X_AVX2 4, 32,  8, 8
SAD_X_AVX2 4, 32, 16, 8
SAD_X_AVX2 4, 32, 24, 8
SAD_X_AVX2 4, 32, 32, 8
SAD_X_AVX2 4, 32, 64, 8

;=============================================================================
; SAD cacheline split
;=============================================================================

; Core2 (Conroe) can load unaligned data just as quickly as aligned data...
; unless the unaligned data spans the border between 2 cachelines, in which
; case it's really slow. The exact numbers may differ, but all Intel cpus prior
; to Nehalem have a large penalty for cacheline splits.
; (8-byte alignment exactly half way between two cachelines is ok though.)
; LDDQU was supposed to fix this, but it only works on Pentium 4.
; So in the split case we load aligned data and explicitly perform the
; alignment between registers. Like on archs that have only aligned loads,
; except complicated by the fact that PALIGNR takes only an immediate, not
; a variable alignment.
; It is also possible to hoist the realignment to the macroblock level (keep
; 2 copies of the reference frame, offset by 32 bytes), but the extra memory
; needed for that method makes it often slower.

; sad 16x16 costs on Core2:
; good offsets: 49 cycles (50/64 of all mvs)
; cacheline split: 234 cycles (14/64 of all mvs. ammortized: +40 cycles)
; page split: 3600 cycles (14/4096 of all mvs. ammortized: +11.5 cycles)
; cache or page split with palignr: 57 cycles (ammortized: +2 cycles)

; computed jump assumes this loop is exactly 80 bytes
%macro SAD16_CACHELINE_LOOP_SSE2 1 ; alignment
ALIGN 16
sad_w16_align%1_sse2:
    movdqa  xmm1, [r2+16]
    movdqa  xmm2, [r2+r3+16]
    movdqa  xmm3, [r2]
    movdqa  xmm4, [r2+r3]
    pslldq  xmm1, 16-%1
    pslldq  xmm2, 16-%1
    psrldq  xmm3, %1
    psrldq  xmm4, %1
    por     xmm1, xmm3
    por     xmm2, xmm4
    psadbw  xmm1, [r0]
    psadbw  xmm2, [r0+r1]
    paddw   xmm0, xmm1
    paddw   xmm0, xmm2
    lea     r0,   [r0+2*r1]
    lea     r2,   [r2+2*r3]
    dec     r4
    jg sad_w16_align%1_sse2
    ret
%endmacro

; computed jump assumes this loop is exactly 64 bytes
%macro SAD16_CACHELINE_LOOP_SSSE3 1 ; alignment
ALIGN 16
sad_w16_align%1_ssse3:
    movdqa  xmm1, [r2+16]
    movdqa  xmm2, [r2+r3+16]
    palignr xmm1, [r2], %1
    palignr xmm2, [r2+r3], %1
    psadbw  xmm1, [r0]
    psadbw  xmm2, [r0+r1]
    paddw   xmm0, xmm1
    paddw   xmm0, xmm2
    lea     r0,   [r0+2*r1]
    lea     r2,   [r2+2*r3]
    dec     r4
    jg sad_w16_align%1_ssse3
    ret
%endmacro

%macro SAD16_CACHELINE_FUNC 2 ; cpu, height
cglobal pixel_sad_16x%2_cache64_%1
    mov     eax, r2m
    and     eax, 0x37
    cmp     eax, 0x30
    jle pixel_sad_16x%2_sse2
    PROLOGUE 4,6
    mov     r4d, r2d
    and     r4d, 15
%ifidn %1, ssse3
    shl     r4d, 6  ; code size = 64
%else
    lea     r4, [r4*5]
    shl     r4d, 4  ; code size = 80
%endif
%define sad_w16_addr (sad_w16_align1_%1 + (sad_w16_align1_%1 - sad_w16_align2_%1))
%ifdef PIC
    lea     r5, [sad_w16_addr]
    add     r5, r4
%else
    lea     r5, [sad_w16_addr + r4]
%endif
    and     r2, ~15
    mov     r4d, %2/2
    pxor    xmm0, xmm0
    call    r5
    movhlps xmm1, xmm0
    paddw   xmm0, xmm1
    movd    eax,  xmm0
    RET
%endmacro

%macro SAD_CACHELINE_START_MMX2 4 ; width, height, iterations, cacheline
    mov    eax, r2m
    and    eax, 0x17|%1|(%4>>1)
    cmp    eax, 0x10|%1|(%4>>1)
    jle pixel_sad_%1x%2_mmx2
    and    eax, 7
    shl    eax, 3
    movd   mm6, [pd_64]
    movd   mm7, eax
    psubw  mm6, mm7
    PROLOGUE 4,5
    and    r2, ~7
    mov    r4d, %3
    pxor   mm0, mm0
%endmacro

%macro SAD16_CACHELINE_FUNC_MMX2 2 ; height, cacheline
cglobal pixel_sad_16x%1_cache%2_mmx2
    SAD_CACHELINE_START_MMX2 16, %1, %1, %2
.loop:
    movq   mm1, [r2]
    movq   mm2, [r2+8]
    movq   mm3, [r2+16]
    movq   mm4, mm2
    psrlq  mm1, mm7
    psllq  mm2, mm6
    psllq  mm3, mm6
    psrlq  mm4, mm7
    por    mm1, mm2
    por    mm3, mm4
    psadbw mm1, [r0]
    psadbw mm3, [r0+8]
    paddw  mm0, mm1
    paddw  mm0, mm3
    add    r2, r3
    add    r0, r1
    dec    r4
    jg .loop
    movd   eax, mm0
    RET
%endmacro

%macro SAD8_CACHELINE_FUNC_MMX2 2 ; height, cacheline
cglobal pixel_sad_8x%1_cache%2_mmx2
    SAD_CACHELINE_START_MMX2 8, %1, %1/2, %2
.loop:
    movq   mm1, [r2+8]
    movq   mm2, [r2+r3+8]
    movq   mm3, [r2]
    movq   mm4, [r2+r3]
    psllq  mm1, mm6
    psllq  mm2, mm6
    psrlq  mm3, mm7
    psrlq  mm4, mm7
    por    mm1, mm3
    por    mm2, mm4
    psadbw mm1, [r0]
    psadbw mm2, [r0+r1]
    paddw  mm0, mm1
    paddw  mm0, mm2
    lea    r2, [r2+2*r3]
    lea    r0, [r0+2*r1]
    dec    r4
    jg .loop
    movd   eax, mm0
    RET
%endmacro

; sad_x3/x4_cache64: check each mv.
; if they're all within a cacheline, use normal sad_x3/x4.
; otherwise, send them individually to sad_cache64.
%macro CHECK_SPLIT 3 ; pix, width, cacheline
    mov  eax, %1
    and  eax, 0x17|%2|(%3>>1)
    cmp  eax, 0x10|%2|(%3>>1)
    jg .split
%endmacro

%macro SADX3_CACHELINE_FUNC 6 ; width, height, cacheline, normal_ver, split_ver, name
cglobal pixel_sad_x3_%1x%2_cache%3_%6
    CHECK_SPLIT r1m, %1, %3
    CHECK_SPLIT r2m, %1, %3
    CHECK_SPLIT r3m, %1, %3
    jmp pixel_sad_x3_%1x%2_%4
.split:
%if ARCH_X86_64
    PROLOGUE 6,9
    push r3
    push r2
%if WIN64
    movsxd r4, r4d
    sub rsp, 40 ; shadow space and alignment
%endif
    mov  r2, r1
    mov  r1, FENC_STRIDE
    mov  r3, r4
    mov  r7, r0
    mov  r8, r5
    call pixel_sad_%1x%2_cache%3_%5
    mov  [r8], eax
%if WIN64
    mov  r2, [rsp+40+0*8]
%else
    pop  r2
%endif
    mov  r0, r7
    call pixel_sad_%1x%2_cache%3_%5
    mov  [r8+4], eax
%if WIN64
    mov  r2, [rsp+40+1*8]
%else
    pop  r2
%endif
    mov  r0, r7
    call pixel_sad_%1x%2_cache%3_%5
    mov  [r8+8], eax
%if WIN64
    add  rsp, 40+2*8
%endif
    RET
%else
    push edi
    mov  edi, [esp+28]
    push dword [esp+24]
    push dword [esp+16]
    push dword 16
    push dword [esp+20]
    call pixel_sad_%1x%2_cache%3_%5
    mov  ecx, [esp+32]
    mov  [edi], eax
    mov  [esp+8], ecx
    call pixel_sad_%1x%2_cache%3_%5
    mov  ecx, [esp+36]
    mov  [edi+4], eax
    mov  [esp+8], ecx
    call pixel_sad_%1x%2_cache%3_%5
    mov  [edi+8], eax
    add  esp, 16
    pop  edi
    ret
%endif
%endmacro

%macro SADX4_CACHELINE_FUNC 6 ; width, height, cacheline, normal_ver, split_ver, name
cglobal pixel_sad_x4_%1x%2_cache%3_%6
    CHECK_SPLIT r1m, %1, %3
    CHECK_SPLIT r2m, %1, %3
    CHECK_SPLIT r3m, %1, %3
    CHECK_SPLIT r4m, %1, %3
    jmp pixel_sad_x4_%1x%2_%4
.split:
%if ARCH_X86_64
    PROLOGUE 6,9
    mov  r8,  r6mp
    push r4
    push r3
    push r2
%if WIN64
    sub rsp, 32 ; shadow space
%endif
    mov  r2, r1
    mov  r1, FENC_STRIDE
    mov  r3, r5
    mov  r7, r0
    call pixel_sad_%1x%2_cache%3_%5
    mov  [r8], eax
%if WIN64
    mov  r2, [rsp+32+0*8]
%else
    pop  r2
%endif
    mov  r0, r7
    call pixel_sad_%1x%2_cache%3_%5
    mov  [r8+4], eax
%if WIN64
    mov  r2, [rsp+32+1*8]
%else
    pop  r2
%endif
    mov  r0, r7
    call pixel_sad_%1x%2_cache%3_%5
    mov  [r8+8], eax
%if WIN64
    mov  r2, [rsp+32+2*8]
%else
    pop  r2
%endif
    mov  r0, r7
    call pixel_sad_%1x%2_cache%3_%5
    mov  [r8+12], eax
%if WIN64
    add  rsp, 32+3*8
%endif
    RET
%else
    push edi
    mov  edi, [esp+32]
    push dword [esp+28]
    push dword [esp+16]
    push dword 16
    push dword [esp+20]
    call pixel_sad_%1x%2_cache%3_%5
    mov  ecx, [esp+32]
    mov  [edi], eax
    mov  [esp+8], ecx
    call pixel_sad_%1x%2_cache%3_%5
    mov  ecx, [esp+36]
    mov  [edi+4], eax
    mov  [esp+8], ecx
    call pixel_sad_%1x%2_cache%3_%5
    mov  ecx, [esp+40]
    mov  [edi+8], eax
    mov  [esp+8], ecx
    call pixel_sad_%1x%2_cache%3_%5
    mov  [edi+12], eax
    add  esp, 16
    pop  edi
    ret
%endif
%endmacro

%macro SADX34_CACHELINE_FUNC 1+
    SADX3_CACHELINE_FUNC %1
    SADX4_CACHELINE_FUNC %1
%endmacro


; instantiate the aligned sads

INIT_MMX
%if ARCH_X86_64 == 0
SAD16_CACHELINE_FUNC_MMX2  8, 32
SAD16_CACHELINE_FUNC_MMX2 16, 32
SAD8_CACHELINE_FUNC_MMX2   4, 32
SAD8_CACHELINE_FUNC_MMX2   8, 32
SAD8_CACHELINE_FUNC_MMX2  16, 32
SAD16_CACHELINE_FUNC_MMX2  8, 64
SAD16_CACHELINE_FUNC_MMX2 16, 64
%endif ; !ARCH_X86_64
SAD8_CACHELINE_FUNC_MMX2   4, 64
SAD8_CACHELINE_FUNC_MMX2   8, 64
SAD8_CACHELINE_FUNC_MMX2  16, 64

%if ARCH_X86_64 == 0
SADX34_CACHELINE_FUNC 16, 16, 32, mmx2, mmx2, mmx2
SADX34_CACHELINE_FUNC 16,  8, 32, mmx2, mmx2, mmx2
SADX34_CACHELINE_FUNC  8, 16, 32, mmx2, mmx2, mmx2
SADX34_CACHELINE_FUNC  8,  8, 32, mmx2, mmx2, mmx2
SADX34_CACHELINE_FUNC 16, 16, 64, mmx2, mmx2, mmx2
SADX34_CACHELINE_FUNC 16,  8, 64, mmx2, mmx2, mmx2
%endif ; !ARCH_X86_64
SADX34_CACHELINE_FUNC  8, 16, 64, mmx2, mmx2, mmx2
SADX34_CACHELINE_FUNC  8,  8, 64, mmx2, mmx2, mmx2

%if ARCH_X86_64 == 0
SAD16_CACHELINE_FUNC sse2, 8
SAD16_CACHELINE_FUNC sse2, 16
%assign i 1
%rep 15
SAD16_CACHELINE_LOOP_SSE2 i
%assign i i+1
%endrep
SADX34_CACHELINE_FUNC 16, 16, 64, sse2, sse2, sse2
SADX34_CACHELINE_FUNC 16,  8, 64, sse2, sse2, sse2
%endif ; !ARCH_X86_64
SADX34_CACHELINE_FUNC  8, 16, 64, sse2, mmx2, sse2

SAD16_CACHELINE_FUNC ssse3, 8
SAD16_CACHELINE_FUNC ssse3, 16
%assign i 1
%rep 15
SAD16_CACHELINE_LOOP_SSSE3 i
%assign i i+1
%endrep
SADX34_CACHELINE_FUNC 16, 16, 64, sse2, ssse3, ssse3
SADX34_CACHELINE_FUNC 16,  8, 64, sse2, ssse3, ssse3

%if HIGH_BIT_DEPTH==0
INIT_YMM avx2
cglobal pixel_sad_x3_8x4, 6,6,5
    xorps           m0, m0
    xorps           m1, m1

    sub             r2, r1          ; rebase on pointer r1
    sub             r3, r1

    ; row 0
    vpbroadcastq   xm2, [r0 + 0 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4
    add             r1, r4

    ; row 1
    vpbroadcastq   xm2, [r0 + 1 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4
    add             r1, r4

    ; row 2
    vpbroadcastq   xm2, [r0 + 2 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4
    add             r1, r4

    ; row 3
    vpbroadcastq   xm2, [r0 + 3 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4

    pshufd          xm0, xm0, q0020
    movq            [r5 + 0], xm0
    movd            [r5 + 8], xm1
    RET

INIT_YMM avx2
cglobal pixel_sad_x3_8x8, 6,6,5
    xorps           m0, m0
    xorps           m1, m1

    sub             r2, r1          ; rebase on pointer r1
    sub             r3, r1
%assign x 0
%rep 4
    ; row 0
    vpbroadcastq   xm2, [r0 + 0 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4
    add             r1, r4

    ; row 1
    vpbroadcastq   xm2, [r0 + 1 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4

%assign x x+1
  %if x < 4
    add             r1, r4
    add             r0, 2 * FENC_STRIDE
  %endif
%endrep

    pshufd          xm0, xm0, q0020
    movq            [r5 + 0], xm0
    movd            [r5 + 8], xm1
    RET

INIT_YMM avx2
cglobal pixel_sad_x3_8x16, 6,6,5
    xorps           m0, m0
    xorps           m1, m1

    sub             r2, r1          ; rebase on pointer r1
    sub             r3, r1
%assign x 0
%rep 8
    ; row 0
    vpbroadcastq   xm2, [r0 + 0 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4
    add             r1, r4

    ; row 1
    vpbroadcastq   xm2, [r0 + 1 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4

%assign x x+1
  %if x < 8
    add             r1, r4
    add             r0, 2 * FENC_STRIDE
  %endif
%endrep

    pshufd          xm0, xm0, q0020
    movq            [r5 + 0], xm0
    movd            [r5 + 8], xm1
    RET

%if ARCH_X86_64 == 1 && HIGH_BIT_DEPTH == 0
INIT_YMM avx2
%macro SAD_X3_32x8_AVX2 0
    movu            m3, [r0]
    movu            m4, [r1]
    movu            m5, [r2]
    movu            m6, [r3]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m7, m3, m5
    paddd           m1, m7
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE]
    movu            m4, [r1 + r4]
    movu            m5, [r2 + r4]
    movu            m6, [r3 + r4]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 2]
    movu            m4, [r1 + r4 * 2]
    movu            m5, [r2 + r4 * 2]
    movu            m6, [r3 + r4 * 2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 3]
    movu            m4, [r1 + r6]
    movu            m5, [r2 + r6]
    movu            m6, [r3 + r6]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    movu            m3, [r0]
    movu            m4, [r1]
    movu            m5, [r2]
    movu            m6, [r3]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE]
    movu            m4, [r1 + r4]
    movu            m5, [r2 + r4]
    movu            m6, [r3 + r4]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 2]
    movu            m4, [r1 + r4 * 2]
    movu            m5, [r2 + r4 * 2]
    movu            m6, [r3 + r4 * 2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 3]
    movu            m4, [r1 + r6]
    movu            m5, [r2 + r6]
    movu            m6, [r3 + r6]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3
%endmacro

%macro SAD_X3_64x8_AVX2 0
    movu            m3, [r0]
    movu            m4, [r1]
    movu            m5, [r2]
    movu            m6, [r3]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + mmsize]
    movu            m4, [r1 + mmsize]
    movu            m5, [r2 + mmsize]
    movu            m6, [r3 + mmsize]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE]
    movu            m4, [r1 + r4]
    movu            m5, [r2 + r4]
    movu            m6, [r3 + r4]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE + mmsize]
    movu            m4, [r1 + r4 + mmsize]
    movu            m5, [r2 + r4 + mmsize]
    movu            m6, [r3 + r4 + mmsize]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 2]
    movu            m4, [r1 + r4 * 2]
    movu            m5, [r2 + r4 * 2]
    movu            m6, [r3 + r4 * 2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 2 + mmsize]
    movu            m4, [r1 + r4 * 2 + mmsize]
    movu            m5, [r2 + r4 * 2 + mmsize]
    movu            m6, [r3 + r4 * 2 + mmsize]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 3]
    movu            m4, [r1 + r6]
    movu            m5, [r2 + r6]
    movu            m6, [r3 + r6]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 3 + mmsize]
    movu            m4, [r1 + r6 + mmsize]
    movu            m5, [r2 + r6 + mmsize]
    movu            m6, [r3 + r6 + mmsize]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    movu            m3, [r0]
    movu            m4, [r1]
    movu            m5, [r2]
    movu            m6, [r3]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + mmsize]
    movu            m4, [r1 + mmsize]
    movu            m5, [r2 + mmsize]
    movu            m6, [r3 + mmsize]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE]
    movu            m4, [r1 + r4]
    movu            m5, [r2 + r4]
    movu            m6, [r3 + r4]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE + mmsize]
    movu            m4, [r1 + r4 + mmsize]
    movu            m5, [r2 + r4 + mmsize]
    movu            m6, [r3 + r4 + mmsize]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 2]
    movu            m4, [r1 + r4 * 2]
    movu            m5, [r2 + r4 * 2]
    movu            m6, [r3 + r4 * 2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 2 + mmsize]
    movu            m4, [r1 + r4 * 2 + mmsize]
    movu            m5, [r2 + r4 * 2 + mmsize]
    movu            m6, [r3 + r4 * 2 + mmsize]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 3]
    movu            m4, [r1 + r6]
    movu            m5, [r2 + r6]
    movu            m6, [r3 + r6]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 3 + mmsize]
    movu            m4, [r1 + r6 + mmsize]
    movu            m5, [r2 + r6 + mmsize]
    movu            m6, [r3 + r6 + mmsize]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3
%endmacro

%macro SAD_X3_48x8_AVX2 0
    movu            m3, [r0]
    movu            m4, [r1]
    movu            m5, [r2]
    movu            m6, [r3]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            xm3, [r0 + mmsize]
    movu            xm4, [r1 + mmsize]
    movu            xm5, [r2 + mmsize]
    movu            xm6, [r3 + mmsize]
    vinserti128     m3, m3, [r0 + FENC_STRIDE], 1
    vinserti128     m4, m4, [r1 + r4], 1
    vinserti128     m5, m5, [r2 + r4], 1
    vinserti128     m6, m6, [r3 + r4], 1

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE + mmsize/2]
    movu            m4, [r1 + r4 + mmsize/2]
    movu            m5, [r2 + r4 + mmsize/2]
    movu            m6, [r3 + r4 + mmsize/2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 2]
    movu            m4, [r1 + r4 * 2]
    movu            m5, [r2 + r4 * 2]
    movu            m6, [r3 + r4 * 2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            xm3, [r0 + FENC_STRIDE * 2 + mmsize]
    movu            xm4, [r1 + r4 * 2 + mmsize]
    movu            xm5, [r2 + r4 * 2 + mmsize]
    movu            xm6, [r3 + r4 * 2 + mmsize]
    vinserti128     m3, m3, [r0 + FENC_STRIDE * 3], 1
    vinserti128     m4, m4, [r1 + r6], 1
    vinserti128     m5, m5, [r2 + r6], 1
    vinserti128     m6, m6, [r3 + r6], 1

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 3 + mmsize/2]
    movu            m4, [r1 + r6 + mmsize/2]
    movu            m5, [r2 + r6 + mmsize/2]
    movu            m6, [r3 + r6 + mmsize/2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    movu            m3, [r0]
    movu            m4, [r1]
    movu            m5, [r2]
    movu            m6, [r3]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            xm3, [r0 + mmsize]
    movu            xm4, [r1 + mmsize]
    movu            xm5, [r2 + mmsize]
    movu            xm6, [r3 + mmsize]
    vinserti128     m3, m3, [r0 + FENC_STRIDE], 1
    vinserti128     m4, m4, [r1 + r4], 1
    vinserti128     m5, m5, [r2 + r4], 1
    vinserti128     m6, m6, [r3 + r4], 1

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE + mmsize/2]
    movu            m4, [r1 + r4 + mmsize/2]
    movu            m5, [r2 + r4 + mmsize/2]
    movu            m6, [r3 + r4 + mmsize/2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 2]
    movu            m4, [r1 + r4 * 2]
    movu            m5, [r2 + r4 * 2]
    movu            m6, [r3 + r4 * 2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            xm3, [r0 + FENC_STRIDE * 2 + mmsize]
    movu            xm4, [r1 + r4 * 2 + mmsize]
    movu            xm5, [r2 + r4 * 2 + mmsize]
    movu            xm6, [r3 + r4 * 2 + mmsize]
    vinserti128     m3, m3, [r0 + FENC_STRIDE * 3], 1
    vinserti128     m4, m4, [r1 + r6], 1
    vinserti128     m5, m5, [r2 + r6], 1
    vinserti128     m6, m6, [r3 + r6], 1

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3

    movu            m3, [r0 + FENC_STRIDE * 3 + mmsize/2]
    movu            m4, [r1 + r6 + mmsize/2]
    movu            m5, [r2 + r6 + mmsize/2]
    movu            m6, [r3 + r6 + mmsize/2]

    psadbw          m7, m3, m4
    paddd           m0, m7
    psadbw          m4, m3, m5
    paddd           m1, m4
    psadbw          m3, m6
    paddd           m2, m3
%endmacro

%macro PIXEL_SAD_X3_END_AVX2 0
    vextracti128   xm3, m0, 1
    vextracti128   xm4, m1, 1
    vextracti128   xm5, m2, 1
    paddd           m0, m3
    paddd           m1, m4
    paddd           m2, m5
    pshufd         xm3, xm0, 2
    pshufd         xm4, xm1, 2
    pshufd         xm5, xm2, 2
    paddd           m0, m3
    paddd           m1, m4
    paddd           m2, m5

    movd            [r5 + 0], xm0
    movd            [r5 + 4], xm1
    movd            [r5 + 8], xm2
%endmacro

cglobal pixel_sad_x3_32x8, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_32x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_32x16, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_32x24, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_32x32, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_32x64, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_32x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_64x16, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_64x32, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_64x48, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_64x64, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_64x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET

cglobal pixel_sad_x3_48x64, 6,7,8
    pxor            m0, m0
    pxor            m1, m1
    pxor            m2, m2
    lea             r6, [r4 * 3]

    SAD_X3_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_48x8_AVX2

    add             r0, FENC_STRIDE * 4
    lea             r1, [r1 + r4 * 4]
    lea             r2, [r2 + r4 * 4]
    lea             r3, [r3 + r4 * 4]

    SAD_X3_48x8_AVX2
    PIXEL_SAD_X3_END_AVX2
    RET
%endif

INIT_YMM avx2
cglobal pixel_sad_x4_8x8, 7,7,5
    xorps           m0, m0
    xorps           m1, m1

    sub             r2, r1          ; rebase on pointer r1
    sub             r3, r1
    sub             r4, r1
%assign x 0
%rep 4
    ; row 0
    vpbroadcastq   xm2, [r0 + 0 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    movhps         xm4, [r1 + r4]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4
    add             r1, r5

    ; row 1
    vpbroadcastq   xm2, [r0 + 1 * FENC_STRIDE]
    movq           xm3, [r1]
    movhps         xm3, [r1 + r2]
    movq           xm4, [r1 + r3]
    movhps         xm4, [r1 + r4]
    psadbw         xm3, xm2
    psadbw         xm4, xm2
    paddd          xm0, xm3
    paddd          xm1, xm4

%assign x x+1
  %if x < 4
    add             r1, r5
    add             r0, 2 * FENC_STRIDE
  %endif
%endrep

    pshufd          xm0, xm0, q0020
    pshufd          xm1, xm1, q0020
    movq            [r6 + 0], xm0
    movq            [r6 + 8], xm1
    RET

INIT_YMM avx2
cglobal pixel_sad_32x8, 4,4,6
    xorps           m0, m0
    xorps           m5, m5

    movu           m1, [r0]               ; row 0 of pix0
    movu           m2, [r2]               ; row 0 of pix1
    movu           m3, [r0 + r1]          ; row 1 of pix0
    movu           m4, [r2 + r3]          ; row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 2 * r3]
    lea     r0,     [r0 + 2 * r1]

    movu           m1, [r0]               ; row 2 of pix0
    movu           m2, [r2]               ; row 2 of pix1
    movu           m3, [r0 + r1]          ; row 3 of pix0
    movu           m4, [r2 + r3]          ; row 3 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 2 * r3]
    lea     r0,     [r0 + 2 * r1]

    movu           m1, [r0]               ; row 4 of pix0
    movu           m2, [r2]               ; row 4 of pix1
    movu           m3, [r0 + r1]          ; row 5 of pix0
    movu           m4, [r2 + r3]          ; row 5 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 2 * r3]
    lea     r0,     [r0 + 2 * r1]

    movu           m1, [r0]               ; row 6 of pix0
    movu           m2, [r2]               ; row 6 of pix1
    movu           m3, [r0 + r1]          ; row 7 of pix0
    movu           m4, [r2 + r3]          ; row 7 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    paddd          m0, m5
    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd           eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_32x16, 4,5,6
    xorps           m0, m0
    xorps           m5, m5
    mov             r4d, 4

.loop
    movu           m1, [r0]               ; row 0 of pix0
    movu           m2, [r2]               ; row 0 of pix1
    movu           m3, [r0 + r1]          ; row 1 of pix0
    movu           m4, [r2 + r3]          ; row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 2 * r3]
    lea     r0,     [r0 + 2 * r1]

    movu           m1, [r0]               ; row 2 of pix0
    movu           m2, [r2]               ; row 2 of pix1
    movu           m3, [r0 + r1]          ; row 3 of pix0
    movu           m4, [r2 + r3]          ; row 3 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 2 * r3]
    lea     r0,     [r0 + 2 * r1]

    dec         r4d
    jnz         .loop

    paddd          m0, m5
    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd           eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_32x24, 4,7,6
    xorps           m0, m0
    xorps           m5, m5
    mov             r4d, 6
    lea             r5, [r1 * 3]
    lea             r6, [r3 * 3]
.loop
    movu           m1, [r0]               ; row 0 of pix0
    movu           m2, [r2]               ; row 0 of pix1
    movu           m3, [r0 + r1]          ; row 1 of pix0
    movu           m4, [r2 + r3]          ; row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + 2 * r1]      ; row 2 of pix0
    movu           m2, [r2 + 2 * r3]      ; row 2 of pix1
    movu           m3, [r0 + r5]          ; row 3 of pix0
    movu           m4, [r2 + r6]          ; row 3 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 4 * r3]
    lea     r0,     [r0 + 4 * r1]

    dec         r4d
    jnz         .loop

    paddd          m0, m5
    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd           eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_32x32, 4,7,5
    xorps           m0, m0
    mov             r4d, 32/4
    lea             r5, [r1 * 3]
    lea             r6, [r3 * 3]

.loop
    movu           m1, [r0]               ; row 0 of pix0
    movu           m2, [r2]               ; row 0 of pix1
    movu           m3, [r0 + r1]          ; row 1 of pix0
    movu           m4, [r2 + r3]          ; row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m0, m3

    movu           m1, [r0 + 2 * r1]      ; row 2 of pix0
    movu           m2, [r2 + 2 * r3]      ; row 2 of pix1
    movu           m3, [r0 + r5]          ; row 3 of pix0
    movu           m4, [r2 + r6]          ; row 3 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m0, m3

    lea            r2,     [r2 + 4 * r3]
    lea            r0,     [r0 + 4 * r1]

    dec            r4d
    jnz           .loop

    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd            eax, xm0
    RET

 INIT_YMM avx2
cglobal pixel_sad_32x64, 4,7,5
    xorps           m0, m0
    mov             r4d, 64/8
    lea             r5, [r1 * 3]
    lea             r6, [r3 * 3]

.loop
    movu           m1, [r0]               ; row 0 of pix0
    movu           m2, [r2]               ; row 0 of pix1
    movu           m3, [r0 + r1]          ; row 1 of pix0
    movu           m4, [r2 + r3]          ; row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m0, m3

    movu           m1, [r0 + 2 * r1]      ; row 2 of pix0
    movu           m2, [r2 + 2 * r3]      ; row 2 of pix1
    movu           m3, [r0 + r5]          ; row 3 of pix0
    movu           m4, [r2 + r6]          ; row 3 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m0, m3

    lea            r2,     [r2 + 4 * r3]
    lea            r0,     [r0 + 4 * r1]

    movu           m1, [r0]               ; row 4 of pix0
    movu           m2, [r2]               ; row 4 of pix1
    movu           m3, [r0 + r1]          ; row 5 of pix0
    movu           m4, [r2 + r3]          ; row 5 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m0, m3

    movu           m1, [r0 + 2 * r1]      ; row 6 of pix0
    movu           m2, [r2 + 2 * r3]      ; row 6 of pix1
    movu           m3, [r0 + r5]          ; row 7 of pix0
    movu           m4, [r2 + r6]          ; row 7 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m0, m3

    lea            r2,     [r2 + 4 * r3]
    lea            r0,     [r0 + 4 * r1]

    dec            r4d
    jnz           .loop

    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd            eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_48x64, 4,7,7
    xorps           m0, m0
    mov             r4d, 64/4
    lea             r5, [r1 * 3]
    lea             r6, [r3 * 3]
.loop
    movu           m1, [r0]               ; row 0 of pix0
    movu           m2, [r2]               ; row 0 of pix1
    movu           m3, [r0 + r1]          ; row 1 of pix0
    movu           m4, [r2 + r3]          ; row 1 of pix1
    movu           xm5, [r0 +32]          ; last 16 of row 0 of pix0
    vinserti128    m5, m5, [r0 + r1 + 32], 1
    movu           xm6, [r2 +32]          ; last 16 of row 0 of pix1
    vinserti128    m6, m6, [r2 + r3 + 32], 1

    psadbw         m1, m2
    psadbw         m3, m4
    psadbw         m5, m6
    paddd          m0, m1
    paddd          m0, m3
    paddd          m0, m5

    movu           m1, [r0 + 2 * r1]      ; row 2 of pix0
    movu           m2, [r2 + 2 * r3]      ; row 2 of pix1
    movu           m3, [r0 + r5]          ; row 3 of pix0
    movu           m4, [r2 + r6]          ; row 3 of pix1
    movu           xm5, [r0 +32 + 2 * r1]
    vinserti128    m5, m5, [r0 + r5 + 32], 1
    movu           xm6, [r2 +32 + 2 * r3]
    vinserti128    m6, m6, [r2 + r6 + 32], 1

    psadbw         m1, m2
    psadbw         m3, m4
    psadbw         m5, m6
    paddd          m0, m1
    paddd          m0, m3
    paddd          m0, m5

    lea     r2,     [r2 + 4 * r3]
    lea     r0,     [r0 + 4 * r1]

    dec         r4d
    jnz         .loop

    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd            eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_64x16, 4,5,6
    xorps           m0, m0
    xorps           m5, m5
    mov             r4d, 4
.loop
    movu           m1, [r0]               ; first 32 of row 0 of pix0
    movu           m2, [r2]               ; first 32 of row 0 of pix1
    movu           m3, [r0 + 32]          ; second 32 of row 0 of pix0
    movu           m4, [r2 + 32]          ; second 32 of row 0 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r1]          ; first 32 of row 1 of pix0
    movu           m2, [r2 + r3]          ; first 32 of row 1 of pix1
    movu           m3, [r0 + 32 + r1]     ; second 32 of row 1 of pix0
    movu           m4, [r2 + 32 + r3]     ; second 32 of row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 2 * r3]
    lea     r0,     [r0 + 2 * r1]

    movu           m1, [r0]               ; first 32 of row 2 of pix0
    movu           m2, [r2]               ; first 32 of row 2 of pix1
    movu           m3, [r0 + 32]          ; second 32 of row 2 of pix0
    movu           m4, [r2 + 32]          ; second 32 of row 2 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r1]          ; first 32 of row 3 of pix0
    movu           m2, [r2 + r3]          ; first 32 of row 3 of pix1
    movu           m3, [r0 + 32 + r1]     ; second 32 of row 3 of pix0
    movu           m4, [r2 + 32 + r3]     ; second 32 of row 3 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 2 * r3]
    lea     r0,     [r0 + 2 * r1]

    dec         r4d
    jnz         .loop

    paddd          m0, m5
    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd            eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_64x32, 4,5,6
    xorps           m0, m0
    xorps           m5, m5
    mov             r4d, 16
.loop
    movu           m1, [r0]               ; first 32 of row 0 of pix0
    movu           m2, [r2]               ; first 32 of row 0 of pix1
    movu           m3, [r0 + 32]          ; second 32 of row 0 of pix0
    movu           m4, [r2 + 32]          ; second 32 of row 0 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r1]          ; first 32 of row 1 of pix0
    movu           m2, [r2 + r3]          ; first 32 of row 1 of pix1
    movu           m3, [r0 + 32 + r1]     ; second 32 of row 1 of pix0
    movu           m4, [r2 + 32 + r3]     ; second 32 of row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 2 * r3]
    lea     r0,     [r0 + 2 * r1]

    dec         r4d
    jnz         .loop

    paddd          m0, m5
    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd            eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_64x48, 4,7,6
    xorps           m0, m0
    xorps           m5, m5
    mov             r4d, 12
    lea             r5, [r1 * 3]
    lea             r6, [r3 * 3]
.loop
    movu           m1, [r0]               ; first 32 of row 0 of pix0
    movu           m2, [r2]               ; first 32 of row 0 of pix1
    movu           m3, [r0 + 32]          ; second 32 of row 0 of pix0
    movu           m4, [r2 + 32]          ; second 32 of row 0 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r1]          ; first 32 of row 1 of pix0
    movu           m2, [r2 + r3]          ; first 32 of row 1 of pix1
    movu           m3, [r0 + 32 + r1]     ; second 32 of row 1 of pix0
    movu           m4, [r2 + 32 + r3]     ; second 32 of row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + 2 * r1]      ; first 32 of row 0 of pix0
    movu           m2, [r2 + 2 * r3]      ; first 32 of row 0 of pix1
    movu           m3, [r0 + 2 * r1 + 32] ; second 32 of row 0 of pix0
    movu           m4, [r2 + 2 * r3 + 32] ; second 32 of row 0 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r5]          ; first 32 of row 1 of pix0
    movu           m2, [r2 + r6]          ; first 32 of row 1 of pix1
    movu           m3, [r0 + 32 + r5]     ; second 32 of row 1 of pix0
    movu           m4, [r2 + 32 + r6]     ; second 32 of row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 4 * r3]
    lea     r0,     [r0 + 4 * r1]

    dec         r4d
    jnz         .loop

    paddd          m0, m5
    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd            eax, xm0
    RET

INIT_YMM avx2
cglobal pixel_sad_64x64, 4,7,6
    xorps           m0, m0
    xorps           m5, m5
    mov             r4d, 8
    lea             r5, [r1 * 3]
    lea             r6, [r3 * 3]
.loop
    movu           m1, [r0]               ; first 32 of row 0 of pix0
    movu           m2, [r2]               ; first 32 of row 0 of pix1
    movu           m3, [r0 + 32]          ; second 32 of row 0 of pix0
    movu           m4, [r2 + 32]          ; second 32 of row 0 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r1]          ; first 32 of row 1 of pix0
    movu           m2, [r2 + r3]          ; first 32 of row 1 of pix1
    movu           m3, [r0 + 32 + r1]     ; second 32 of row 1 of pix0
    movu           m4, [r2 + 32 + r3]     ; second 32 of row 1 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + 2 * r1]      ; first 32 of row 2 of pix0
    movu           m2, [r2 + 2 * r3]      ; first 32 of row 2 of pix1
    movu           m3, [r0 + 2 * r1 + 32] ; second 32 of row 2 of pix0
    movu           m4, [r2 + 2 * r3 + 32] ; second 32 of row 2 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r5]          ; first 32 of row 3 of pix0
    movu           m2, [r2 + r6]          ; first 32 of row 3 of pix1
    movu           m3, [r0 + 32 + r5]     ; second 32 of row 3 of pix0
    movu           m4, [r2 + 32 + r6]     ; second 32 of row 3 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 4 * r3]
    lea     r0,     [r0 + 4 * r1]

    movu           m1, [r0]               ; first 32 of row 4 of pix0
    movu           m2, [r2]               ; first 32 of row 4 of pix1
    movu           m3, [r0 + 32]          ; second 32 of row 4 of pix0
    movu           m4, [r2 + 32]          ; second 32 of row 4 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r1]          ; first 32 of row 5 of pix0
    movu           m2, [r2 + r3]          ; first 32 of row 5 of pix1
    movu           m3, [r0 + 32 + r1]     ; second 32 of row 5 of pix0
    movu           m4, [r2 + 32 + r3]     ; second 32 of row 5 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + 2 * r1]      ; first 32 of row 6 of pix0
    movu           m2, [r2 + 2 * r3]      ; first 32 of row 6 of pix1
    movu           m3, [r0 + 2 * r1 + 32] ; second 32 of row 6 of pix0
    movu           m4, [r2 + 2 * r3 + 32] ; second 32 of row 6 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    movu           m1, [r0 + r5]          ; first 32 of row 7 of pix0
    movu           m2, [r2 + r6]          ; first 32 of row 7 of pix1
    movu           m3, [r0 + 32 + r5]     ; second 32 of row 7 of pix0
    movu           m4, [r2 + 32 + r6]     ; second 32 of row 7 of pix1

    psadbw         m1, m2
    psadbw         m3, m4
    paddd          m0, m1
    paddd          m5, m3

    lea     r2,     [r2 + 4 * r3]
    lea     r0,     [r0 + 4 * r1]

    dec         r4d
    jnz         .loop

    paddd          m0, m5
    vextracti128   xm1, m0, 1
    paddd          xm0, xm1
    pshufd         xm1, xm0, 2
    paddd          xm0,xm1
    movd            eax, xm0
    RET

%endif
