;*****************************************************************************
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Nabajit Deka <nabajit@multicorewareinc.com>
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


%define INTERP_OFFSET_PP        pd_32
%define INTERP_SHIFT_PP         6

%if BIT_DEPTH == 10
    %define INTERP_SHIFT_PS         2
    %define INTERP_OFFSET_PS        pd_n32768
    %define INTERP_SHIFT_SP         10
    %define INTERP_OFFSET_SP        pd_524800
%elif BIT_DEPTH == 12
    %define INTERP_SHIFT_PS         4
    %define INTERP_OFFSET_PS        pd_n131072
    %define INTERP_SHIFT_SP         8
    %define INTERP_OFFSET_SP        pd_524416
%else
    %error Unsupport bit depth!
%endif


SECTION_RODATA 32

tab_c_32:         times 8 dd 32
tab_c_524800:     times 4 dd 524800
tab_c_n8192:      times 8 dw -8192
pd_524800:        times 8 dd 524800

tab_Tm16:         db 0, 1, 2, 3, 4,  5,  6, 7, 2, 3, 4,  5, 6, 7, 8, 9

tab_ChromaCoeff:  dw  0, 64,  0,  0
                  dw -2, 58, 10, -2
                  dw -4, 54, 16, -2
                  dw -6, 46, 28, -4
                  dw -4, 36, 36, -4
                  dw -4, 28, 46, -6
                  dw -2, 16, 54, -4
                  dw -2, 10, 58, -2

const tab_ChromaCoeffV,  times 8 dw 0, 64
                         times 8 dw 0, 0

                         times 8 dw -2, 58
                         times 8 dw 10, -2

                         times 8 dw -4, 54
                         times 8 dw 16, -2

                         times 8 dw -6, 46
                         times 8 dw 28, -4

                         times 8 dw -4, 36
                         times 8 dw 36, -4

                         times 8 dw -4, 28
                         times 8 dw 46, -6

                         times 8 dw -2, 16
                         times 8 dw 54, -4

                         times 8 dw -2, 10
                         times 8 dw 58, -2

tab_ChromaCoeffVer: times 8 dw 0, 64
                    times 8 dw 0, 0

                    times 8 dw -2, 58
                    times 8 dw 10, -2

                    times 8 dw -4, 54
                    times 8 dw 16, -2

                    times 8 dw -6, 46
                    times 8 dw 28, -4

                    times 8 dw -4, 36
                    times 8 dw 36, -4

                    times 8 dw -4, 28
                    times 8 dw 46, -6

                    times 8 dw -2, 16
                    times 8 dw 54, -4

                    times 8 dw -2, 10
                    times 8 dw 58, -2

tab_LumaCoeff:    dw   0, 0,  0,  64,  0,   0,  0,  0
                  dw  -1, 4, -10, 58,  17, -5,  1,  0
                  dw  -1, 4, -11, 40,  40, -11, 4, -1
                  dw   0, 1, -5,  17,  58, -10, 4, -1

ALIGN 32
tab_LumaCoeffV:   times 4 dw 0, 0
                  times 4 dw 0, 64
                  times 4 dw 0, 0
                  times 4 dw 0, 0

                  times 4 dw -1, 4
                  times 4 dw -10, 58
                  times 4 dw 17, -5
                  times 4 dw 1, 0

                  times 4 dw -1, 4
                  times 4 dw -11, 40
                  times 4 dw 40, -11
                  times 4 dw 4, -1

                  times 4 dw 0, 1
                  times 4 dw -5, 17
                  times 4 dw 58, -10
                  times 4 dw 4, -1
ALIGN 32
tab_LumaCoeffVer: times 8 dw 0, 0
                  times 8 dw 0, 64
                  times 8 dw 0, 0
                  times 8 dw 0, 0

                  times 8 dw -1, 4
                  times 8 dw -10, 58
                  times 8 dw 17, -5
                  times 8 dw 1, 0

                  times 8 dw -1, 4
                  times 8 dw -11, 40
                  times 8 dw 40, -11
                  times 8 dw 4, -1

                  times 8 dw 0, 1
                  times 8 dw -5, 17
                  times 8 dw 58, -10
                  times 8 dw 4, -1

const interp8_hps_shuf,     dd 0, 4, 1, 5, 2, 6, 3, 7

const interp8_hpp_shuf,     db 0, 1, 2, 3, 4, 5, 6, 7, 2, 3, 4, 5, 6, 7, 8, 9
                            db 4, 5, 6, 7, 8, 9, 10, 11, 6, 7, 8, 9, 10, 11, 12, 13

const interp8_hpp_shuf_new, db 0, 1, 2, 3, 2, 3, 4, 5, 4, 5, 6, 7, 6, 7, 8, 9
                            db 4, 5, 6, 7, 6, 7, 8, 9, 8, 9, 10, 11, 10, 11, 12, 13

SECTION .text
cextern pd_8
cextern pd_32
cextern pw_pixel_max
cextern pd_524416
cextern pd_n32768
cextern pd_n131072
cextern pw_2000
cextern idct8_shuf2

%macro FILTER_LUMA_HOR_4_sse2 1
    movu        m4,     [r0 + %1]       ; m4 = src[0-7]
    movu        m5,     [r0 + %1 + 2]   ; m5 = src[1-8]
    pmaddwd     m4,     m0
    pmaddwd     m5,     m0
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m4,     m4,     q3120
    pshufd      m5,     m5,     q3120
    punpcklqdq  m4,     m5

    movu        m5,     [r0 + %1 + 4]   ; m5 = src[2-9]
    movu        m3,     [r0 + %1 + 6]   ; m3 = src[3-10]
    pmaddwd     m5,     m0
    pmaddwd     m3,     m0
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m2,     m3,     q2301
    paddd       m3,     m2
    pshufd      m5,     m5,     q3120
    pshufd      m3,     m3,     q3120
    punpcklqdq  m5,     m3

    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m4,     m4,     q3120
    pshufd      m5,     m5,     q3120
    punpcklqdq  m4,     m5
    paddd       m4,     m1
%endmacro

%macro FILTER_LUMA_HOR_8_sse2 1
    movu        m4,     [r0 + %1]       ; m4 = src[0-7]
    movu        m5,     [r0 + %1 + 2]   ; m5 = src[1-8]
    pmaddwd     m4,     m0
    pmaddwd     m5,     m0
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m4,     m4,     q3120
    pshufd      m5,     m5,     q3120
    punpcklqdq  m4,     m5

    movu        m5,     [r0 + %1 + 4]   ; m5 = src[2-9]
    movu        m3,     [r0 + %1 + 6]   ; m3 = src[3-10]
    pmaddwd     m5,     m0
    pmaddwd     m3,     m0
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m2,     m3,     q2301
    paddd       m3,     m2
    pshufd      m5,     m5,     q3120
    pshufd      m3,     m3,     q3120
    punpcklqdq  m5,     m3

    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m4,     m4,     q3120
    pshufd      m5,     m5,     q3120
    punpcklqdq  m4,     m5
    paddd       m4,     m1

    movu        m5,     [r0 + %1 + 8]   ; m5 = src[4-11]
    movu        m6,     [r0 + %1 + 10]  ; m6 = src[5-12]
    pmaddwd     m5,     m0
    pmaddwd     m6,     m0
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m2,     m6,     q2301
    paddd       m6,     m2
    pshufd      m5,     m5,     q3120
    pshufd      m6,     m6,     q3120
    punpcklqdq  m5,     m6

    movu        m6,     [r0 + %1 + 12]  ; m6 = src[6-13]
    movu        m3,     [r0 + %1 + 14]  ; m3 = src[7-14]
    pmaddwd     m6,     m0
    pmaddwd     m3,     m0
    pshufd      m2,     m6,     q2301
    paddd       m6,     m2
    pshufd      m2,     m3,     q2301
    paddd       m3,     m2
    pshufd      m6,     m6,     q3120
    pshufd      m3,     m3,     q3120
    punpcklqdq  m6,     m3

    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m2,     m6,     q2301
    paddd       m6,     m2
    pshufd      m5,     m5,     q3120
    pshufd      m6,     m6,     q3120
    punpcklqdq  m5,     m6
    paddd       m5,     m1
%endmacro

;------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_p%3_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_sse2 3
INIT_XMM sse2
cglobal interp_8tap_horiz_%3_%1x%2, 4, 7, 8
    mov         r4d,    r4m
    sub         r0,     6
    shl         r4d,    4
    add         r1d,    r1d
    add         r3d,    r3d

%ifdef PIC
    lea         r6,     [tab_LumaCoeff]
    mova        m0,     [r6 + r4]
%else
    mova        m0,     [tab_LumaCoeff + r4]
%endif

%ifidn %3, pp
    mova        m1,     [pd_32]
    pxor        m7,     m7
%else
    mova        m1,     [INTERP_OFFSET_PS]
%endif

    mov         r4d,    %2
%ifidn %3, ps
    cmp         r5m,    byte 0
    je          .loopH
    lea         r6,     [r1 + 2 * r1]
    sub         r0,     r6
    add         r4d,    7
%endif

.loopH:
%assign x 0
%rep %1/8
    FILTER_LUMA_HOR_8_sse2 x

%ifidn %3, pp
    psrad       m4,     6
    psrad       m5,     6
    packssdw    m4,     m5
    CLIPW       m4,     m7,     [pw_pixel_max]
%else
  %if BIT_DEPTH == 10
    psrad       m4,     2
    psrad       m5,     2
  %elif BIT_DEPTH == 12
    psrad       m4,     4
    psrad       m5,     4
  %endif
    packssdw    m4,     m5
%endif

    movu        [r2 + x], m4
%assign x x+16
%endrep

%rep (%1 % 8)/4
    FILTER_LUMA_HOR_4_sse2 x

%ifidn %3, pp
    psrad       m4,     6
    packssdw    m4,     m4
    CLIPW       m4,     m7,     [pw_pixel_max]
%else
  %if BIT_DEPTH == 10
    psrad       m4,     2
  %elif BIT_DEPTH == 12
    psrad       m4,     4
  %endif
    packssdw    m4,     m4
%endif

    movh        [r2 + x], m4
%endrep

    add         r0,     r1
    add         r2,     r3

    dec         r4d
    jnz         .loopH
    RET

%endmacro

;------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;------------------------------------------------------------------------------------------------------------
    FILTER_HOR_LUMA_sse2 4, 4, pp
    FILTER_HOR_LUMA_sse2 4, 8, pp
    FILTER_HOR_LUMA_sse2 4, 16, pp
    FILTER_HOR_LUMA_sse2 8, 4, pp
    FILTER_HOR_LUMA_sse2 8, 8, pp
    FILTER_HOR_LUMA_sse2 8, 16, pp
    FILTER_HOR_LUMA_sse2 8, 32, pp
    FILTER_HOR_LUMA_sse2 12, 16, pp
    FILTER_HOR_LUMA_sse2 16, 4, pp
    FILTER_HOR_LUMA_sse2 16, 8, pp
    FILTER_HOR_LUMA_sse2 16, 12, pp
    FILTER_HOR_LUMA_sse2 16, 16, pp
    FILTER_HOR_LUMA_sse2 16, 32, pp
    FILTER_HOR_LUMA_sse2 16, 64, pp
    FILTER_HOR_LUMA_sse2 24, 32, pp
    FILTER_HOR_LUMA_sse2 32, 8, pp
    FILTER_HOR_LUMA_sse2 32, 16, pp
    FILTER_HOR_LUMA_sse2 32, 24, pp
    FILTER_HOR_LUMA_sse2 32, 32, pp
    FILTER_HOR_LUMA_sse2 32, 64, pp
    FILTER_HOR_LUMA_sse2 48, 64, pp
    FILTER_HOR_LUMA_sse2 64, 16, pp
    FILTER_HOR_LUMA_sse2 64, 32, pp
    FILTER_HOR_LUMA_sse2 64, 48, pp
    FILTER_HOR_LUMA_sse2 64, 64, pp

;---------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;---------------------------------------------------------------------------------------------------------------------------
    FILTER_HOR_LUMA_sse2 4, 4, ps
    FILTER_HOR_LUMA_sse2 4, 8, ps
    FILTER_HOR_LUMA_sse2 4, 16, ps
    FILTER_HOR_LUMA_sse2 8, 4, ps
    FILTER_HOR_LUMA_sse2 8, 8, ps
    FILTER_HOR_LUMA_sse2 8, 16, ps
    FILTER_HOR_LUMA_sse2 8, 32, ps
    FILTER_HOR_LUMA_sse2 12, 16, ps
    FILTER_HOR_LUMA_sse2 16, 4, ps
    FILTER_HOR_LUMA_sse2 16, 8, ps
    FILTER_HOR_LUMA_sse2 16, 12, ps
    FILTER_HOR_LUMA_sse2 16, 16, ps
    FILTER_HOR_LUMA_sse2 16, 32, ps
    FILTER_HOR_LUMA_sse2 16, 64, ps
    FILTER_HOR_LUMA_sse2 24, 32, ps
    FILTER_HOR_LUMA_sse2 32, 8, ps
    FILTER_HOR_LUMA_sse2 32, 16, ps
    FILTER_HOR_LUMA_sse2 32, 24, ps
    FILTER_HOR_LUMA_sse2 32, 32, ps
    FILTER_HOR_LUMA_sse2 32, 64, ps
    FILTER_HOR_LUMA_sse2 48, 64, ps
    FILTER_HOR_LUMA_sse2 64, 16, ps
    FILTER_HOR_LUMA_sse2 64, 32, ps
    FILTER_HOR_LUMA_sse2 64, 48, ps
    FILTER_HOR_LUMA_sse2 64, 64, ps

%macro PROCESS_LUMA_VER_W4_4R_sse2 0
    movq       m0, [r0]
    movq       m1, [r0 + r1]
    punpcklwd  m0, m1                          ;m0=[0 1]
    pmaddwd    m0, [r6 + 0 *16]                ;m0=[0+1]  Row1

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m1, m4                          ;m1=[1 2]
    pmaddwd    m1, [r6 + 0 *16]                ;m1=[1+2]  Row2

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[2 3]
    pmaddwd    m2, m4, [r6 + 0 *16]            ;m2=[2+3]  Row3
    pmaddwd    m4, [r6 + 1 * 16]
    paddd      m0, m4                          ;m0=[0+1+2+3]  Row1

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m5, m4                          ;m5=[3 4]
    pmaddwd    m3, m5, [r6 + 0 *16]            ;m3=[3+4]  Row4
    pmaddwd    m5, [r6 + 1 * 16]
    paddd      m1, m5                          ;m1 = [1+2+3+4]  Row2

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[4 5]
    pmaddwd    m6, m4, [r6 + 1 * 16]
    paddd      m2, m6                          ;m2=[2+3+4+5]  Row3
    pmaddwd    m4, [r6 + 2 * 16]
    paddd      m0, m4                          ;m0=[0+1+2+3+4+5]  Row1

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m5, m4                          ;m5=[5 6]
    pmaddwd    m6, m5, [r6 + 1 * 16]
    paddd      m3, m6                          ;m3=[3+4+5+6]  Row4
    pmaddwd    m5, [r6 + 2 * 16]
    paddd      m1, m5                          ;m1=[1+2+3+4+5+6]  Row2

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[6 7]
    pmaddwd    m6, m4, [r6 + 2 * 16]
    paddd      m2, m6                          ;m2=[2+3+4+5+6+7]  Row3
    pmaddwd    m4, [r6 + 3 * 16]
    paddd      m0, m4                          ;m0=[0+1+2+3+4+5+6+7]  Row1 end

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m5, m4                          ;m5=[7 8]
    pmaddwd    m6, m5, [r6 + 2 * 16]
    paddd      m3, m6                          ;m3=[3+4+5+6+7+8]  Row4
    pmaddwd    m5, [r6 + 3 * 16]
    paddd      m1, m5                          ;m1=[1+2+3+4+5+6+7+8]  Row2 end

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[8 9]
    pmaddwd    m4, [r6 + 3 * 16]
    paddd      m2, m4                          ;m2=[2+3+4+5+6+7+8+9]  Row3 end

    movq       m4, [r0 + 2 * r1]
    punpcklwd  m5, m4                          ;m5=[9 10]
    pmaddwd    m5, [r6 + 3 * 16]
    paddd      m3, m5                          ;m3=[3+4+5+6+7+8+9+10]  Row4 end
%endmacro

;--------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_%1_%2x%3(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;--------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_LUMA_sse2 3
INIT_XMM sse2
cglobal interp_8tap_vert_%1_%2x%3, 5, 7, 8

    add       r1d, r1d
    add       r3d, r3d
    lea       r5, [r1 + 2 * r1]
    sub       r0, r5
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_LumaCoeffV]
    lea       r6, [r5 + r4]
%else
    lea       r6, [tab_LumaCoeffV + r4]
%endif

%ifidn %1,pp
    mova      m7, [INTERP_OFFSET_PP]
%define SHIFT 6
%elifidn %1,ps
    mova      m7, [INTERP_OFFSET_PS]
  %if BIT_DEPTH == 10
    %define SHIFT 2
  %elif BIT_DEPTH == 12
    %define SHIFT 4
  %endif
%endif

    mov         r4d, %3/4
.loopH:
%assign x 0
%rep %2/4
    PROCESS_LUMA_VER_W4_4R_sse2

    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7

    psrad     m0, SHIFT
    psrad     m1, SHIFT
    psrad     m2, SHIFT
    psrad     m3, SHIFT

    packssdw  m0, m1
    packssdw  m2, m3

%ifidn %1,pp
    pxor      m1, m1
    CLIPW2    m0, m2, m1, [pw_pixel_max]
%endif

    movh      [r2 + x], m0
    movhps    [r2 + r3 + x], m0
    lea       r5, [r2 + 2 * r3]
    movh      [r5 + x], m2
    movhps    [r5 + r3 + x], m2

    lea       r5, [8 * r1 - 2 * 4]
    sub       r0, r5
%assign x x+8
%endrep

    lea       r0, [r0 + 4 * r1 - 2 * %2]
    lea       r2, [r2 + 4 * r3]

    dec         r4d
    jnz       .loopH

    RET
%endmacro

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_pp_%2x%3(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;-------------------------------------------------------------------------------------------------------------
    FILTER_VER_LUMA_sse2 pp, 4, 4
    FILTER_VER_LUMA_sse2 pp, 8, 8
    FILTER_VER_LUMA_sse2 pp, 8, 4
    FILTER_VER_LUMA_sse2 pp, 4, 8
    FILTER_VER_LUMA_sse2 pp, 16, 16
    FILTER_VER_LUMA_sse2 pp, 16, 8
    FILTER_VER_LUMA_sse2 pp, 8, 16
    FILTER_VER_LUMA_sse2 pp, 16, 12
    FILTER_VER_LUMA_sse2 pp, 12, 16
    FILTER_VER_LUMA_sse2 pp, 16, 4
    FILTER_VER_LUMA_sse2 pp, 4, 16
    FILTER_VER_LUMA_sse2 pp, 32, 32
    FILTER_VER_LUMA_sse2 pp, 32, 16
    FILTER_VER_LUMA_sse2 pp, 16, 32
    FILTER_VER_LUMA_sse2 pp, 32, 24
    FILTER_VER_LUMA_sse2 pp, 24, 32
    FILTER_VER_LUMA_sse2 pp, 32, 8
    FILTER_VER_LUMA_sse2 pp, 8, 32
    FILTER_VER_LUMA_sse2 pp, 64, 64
    FILTER_VER_LUMA_sse2 pp, 64, 32
    FILTER_VER_LUMA_sse2 pp, 32, 64
    FILTER_VER_LUMA_sse2 pp, 64, 48
    FILTER_VER_LUMA_sse2 pp, 48, 64
    FILTER_VER_LUMA_sse2 pp, 64, 16
    FILTER_VER_LUMA_sse2 pp, 16, 64

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_ps_%2x%3(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;-------------------------------------------------------------------------------------------------------------
    FILTER_VER_LUMA_sse2 ps, 4, 4
    FILTER_VER_LUMA_sse2 ps, 8, 8
    FILTER_VER_LUMA_sse2 ps, 8, 4
    FILTER_VER_LUMA_sse2 ps, 4, 8
    FILTER_VER_LUMA_sse2 ps, 16, 16
    FILTER_VER_LUMA_sse2 ps, 16, 8
    FILTER_VER_LUMA_sse2 ps, 8, 16
    FILTER_VER_LUMA_sse2 ps, 16, 12
    FILTER_VER_LUMA_sse2 ps, 12, 16
    FILTER_VER_LUMA_sse2 ps, 16, 4
    FILTER_VER_LUMA_sse2 ps, 4, 16
    FILTER_VER_LUMA_sse2 ps, 32, 32
    FILTER_VER_LUMA_sse2 ps, 32, 16
    FILTER_VER_LUMA_sse2 ps, 16, 32
    FILTER_VER_LUMA_sse2 ps, 32, 24
    FILTER_VER_LUMA_sse2 ps, 24, 32
    FILTER_VER_LUMA_sse2 ps, 32, 8
    FILTER_VER_LUMA_sse2 ps, 8, 32
    FILTER_VER_LUMA_sse2 ps, 64, 64
    FILTER_VER_LUMA_sse2 ps, 64, 32
    FILTER_VER_LUMA_sse2 ps, 32, 64
    FILTER_VER_LUMA_sse2 ps, 64, 48
    FILTER_VER_LUMA_sse2 ps, 48, 64
    FILTER_VER_LUMA_sse2 ps, 64, 16
    FILTER_VER_LUMA_sse2 ps, 16, 64

%macro FILTERH_W2_4_sse3 2
    movh        m3,     [r0 + %1]
    movhps      m3,     [r0 + %1 + 2]
    pmaddwd     m3,     m0
    movh        m4,     [r0 + r1 + %1]
    movhps      m4,     [r0 + r1 + %1 + 2]
    pmaddwd     m4,     m0
    pshufd      m2,     m3,     q2301
    paddd       m3,     m2
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m3,     m3,     q3120
    pshufd      m4,     m4,     q3120
    punpcklqdq  m3,     m4
    paddd       m3,     m1
    movh        m5,     [r0 + 2 * r1 + %1]
    movhps      m5,     [r0 + 2 * r1 + %1 + 2]
    pmaddwd     m5,     m0
    movh        m4,     [r0 + r4 + %1]
    movhps      m4,     [r0 + r4 + %1 + 2]
    pmaddwd     m4,     m0
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m5,     m5,     q3120
    pshufd      m4,     m4,     q3120
    punpcklqdq  m5,     m4
    paddd       m5,     m1
%ifidn %2, pp
    psrad       m3,     6
    psrad       m5,     6
    packssdw    m3,     m5
    CLIPW       m3,     m7,     m6
%else
    psrad       m3,     INTERP_SHIFT_PS
    psrad       m5,     INTERP_SHIFT_PS
    packssdw    m3,     m5
%endif
    movd        [r2 + %1], m3
    psrldq      m3,     4
    movd        [r2 + r3 + %1], m3
    psrldq      m3,     4
    movd        [r2 + r3 * 2 + %1], m3
    psrldq      m3,     4
    movd        [r2 + r5 + %1], m3
%endmacro

%macro FILTERH_W2_3_sse3 1
    movh        m3,     [r0 + %1]
    movhps      m3,     [r0 + %1 + 2]
    pmaddwd     m3,     m0
    movh        m4,     [r0 + r1 + %1]
    movhps      m4,     [r0 + r1 + %1 + 2]
    pmaddwd     m4,     m0
    pshufd      m2,     m3,     q2301
    paddd       m3,     m2
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m3,     m3,     q3120
    pshufd      m4,     m4,     q3120
    punpcklqdq  m3,     m4
    paddd       m3,     m1

    movh        m5,     [r0 + 2 * r1 + %1]
    movhps      m5,     [r0 + 2 * r1 + %1 + 2]
    pmaddwd     m5,     m0

    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m5,     m5,     q3120
    paddd       m5,     m1

    psrad       m3,     INTERP_SHIFT_PS
    psrad       m5,     INTERP_SHIFT_PS
    packssdw    m3,     m5

    movd        [r2 + %1], m3
    psrldq      m3,     4
    movd        [r2 + r3 + %1], m3
    psrldq      m3,     4
    movd        [r2 + r3 * 2 + %1], m3
%endmacro

%macro FILTERH_W4_2_sse3 2
    movh        m3,     [r0 + %1]
    movhps      m3,     [r0 + %1 + 2]
    pmaddwd     m3,     m0
    movh        m4,     [r0 + %1 + 4]
    movhps      m4,     [r0 + %1 + 6]
    pmaddwd     m4,     m0
    pshufd      m2,     m3,     q2301
    paddd       m3,     m2
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m3,     m3,     q3120
    pshufd      m4,     m4,     q3120
    punpcklqdq  m3,     m4
    paddd       m3,     m1

    movh        m5,     [r0 + r1 + %1]
    movhps      m5,     [r0 + r1 + %1 + 2]
    pmaddwd     m5,     m0
    movh        m4,     [r0 + r1 + %1 + 4]
    movhps      m4,     [r0 + r1 + %1 + 6]
    pmaddwd     m4,     m0
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m5,     m5,     q3120
    pshufd      m4,     m4,     q3120
    punpcklqdq  m5,     m4
    paddd       m5,     m1
%ifidn %2, pp
    psrad       m3,     6
    psrad       m5,     6
    packssdw    m3,     m5
    CLIPW       m3,     m7,     m6
%else
    psrad       m3,     INTERP_SHIFT_PS
    psrad       m5,     INTERP_SHIFT_PS
    packssdw    m3,     m5
%endif
    movh        [r2 + %1], m3
    movhps      [r2 + r3 + %1], m3
%endmacro

%macro FILTERH_W4_1_sse3 1
    movh        m3,     [r0 + 2 * r1 + %1]
    movhps      m3,     [r0 + 2 * r1 + %1 + 2]
    pmaddwd     m3,     m0
    movh        m4,     [r0 + 2 * r1 + %1 + 4]
    movhps      m4,     [r0 + 2 * r1 + %1 + 6]
    pmaddwd     m4,     m0
    pshufd      m2,     m3,     q2301
    paddd       m3,     m2
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m3,     m3,     q3120
    pshufd      m4,     m4,     q3120
    punpcklqdq  m3,     m4
    paddd       m3,     m1

    psrad       m3,     INTERP_SHIFT_PS
    packssdw    m3,     m3
    movh        [r2 + r3 * 2 + %1], m3
%endmacro

%macro FILTERH_W8_1_sse3 2
    movh        m3,     [r0 + %1]
    movhps      m3,     [r0 + %1 + 2]
    pmaddwd     m3,     m0
    movh        m4,     [r0 + %1 + 4]
    movhps      m4,     [r0 + %1 + 6]
    pmaddwd     m4,     m0
    pshufd      m2,     m3,     q2301
    paddd       m3,     m2
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m3,     m3,     q3120
    pshufd      m4,     m4,     q3120
    punpcklqdq  m3,     m4
    paddd       m3,     m1

    movh        m5,     [r0 + %1 + 8]
    movhps      m5,     [r0 + %1 + 10]
    pmaddwd     m5,     m0
    movh        m4,     [r0 + %1 + 12]
    movhps      m4,     [r0 + %1 + 14]
    pmaddwd     m4,     m0
    pshufd      m2,     m5,     q2301
    paddd       m5,     m2
    pshufd      m2,     m4,     q2301
    paddd       m4,     m2
    pshufd      m5,     m5,     q3120
    pshufd      m4,     m4,     q3120
    punpcklqdq  m5,     m4
    paddd       m5,     m1
%ifidn %2, pp
    psrad       m3,     6
    psrad       m5,     6
    packssdw    m3,     m5
    CLIPW       m3,     m7,     m6
%else
    psrad       m3,     INTERP_SHIFT_PS
    psrad       m5,     INTERP_SHIFT_PS
    packssdw    m3,     m5
%endif
    movdqu      [r2 + %1], m3
%endmacro

;-----------------------------------------------------------------------------
; void interp_4tap_horiz_%3_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------
%macro FILTER_HOR_CHROMA_sse3 3
INIT_XMM sse3
cglobal interp_4tap_horiz_%3_%1x%2, 4, 7, 8
    add         r3,     r3
    add         r1,     r1
    sub         r0,     2
    mov         r4d,    r4m
    add         r4d,    r4d

%ifdef PIC
    lea         r6,     [tab_ChromaCoeff]
    movddup     m0,     [r6 + r4 * 4]
%else
    movddup     m0,     [tab_ChromaCoeff + r4 * 4]
%endif

%ifidn %3, ps
    mova        m1,     [INTERP_OFFSET_PS]
    cmp         r5m,    byte 0
%if %1 <= 6
    lea         r4,     [r1 * 3]
    lea         r5,     [r3 * 3]
%endif
    je          .skip
    sub         r0,     r1
%if %1 <= 6
%assign y 1
%else
%assign y 3
%endif
%assign z 0
%rep y
%assign x 0
%rep %1/8
    FILTERH_W8_1_sse3 x, %3
%assign x x+16
%endrep
%if %1 == 4 || (%1 == 6 && z == 0) || (%1 == 12 && z == 0)
    FILTERH_W4_2_sse3 x, %3
    FILTERH_W4_1_sse3 x
%assign x x+8
%endif
%if %1 == 2 || (%1 == 6 && z == 0)
    FILTERH_W2_3_sse3 x
%endif
%if %1 <= 6
    lea         r0,     [r0 + r4]
    lea         r2,     [r2 + r5]
%else
    lea         r0,     [r0 + r1]
    lea         r2,     [r2 + r3]
%endif
%assign z z+1
%endrep
.skip:
%elifidn %3, pp
    pxor        m7,     m7
    mova        m6,     [pw_pixel_max]
    mova        m1,     [tab_c_32]
%if %1 == 2 || %1 == 6
    lea         r4,     [r1 * 3]
    lea         r5,     [r3 * 3]
%endif
%endif

%if %1 == 2
%assign y %2/4
%elif %1 <= 6
%assign y %2/2
%else
%assign y %2
%endif
%assign z 0
%rep y
%assign x 0
%rep %1/8
    FILTERH_W8_1_sse3 x, %3
%assign x x+16
%endrep
%if %1 == 4 || %1 == 6 || (%1 == 12 && (z % 2) == 0)
    FILTERH_W4_2_sse3 x, %3
%assign x x+8
%endif
%if %1 == 2 || (%1 == 6 && (z % 2) == 0)
    FILTERH_W2_4_sse3 x, %3
%endif
%assign z z+1
%if z < y
%if %1 == 2
    lea         r0,     [r0 + 4 * r1]
    lea         r2,     [r2 + 4 * r3]
%elif %1 <= 6
    lea         r0,     [r0 + 2 * r1]
    lea         r2,     [r2 + 2 * r3]
%else
    lea         r0,     [r0 + r1]
    lea         r2,     [r2 + r3]
%endif
%endif ;z < y
%endrep

    RET
%endmacro

;-----------------------------------------------------------------------------
; void interp_4tap_horiz_pp_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------

FILTER_HOR_CHROMA_sse3 2, 4, pp
FILTER_HOR_CHROMA_sse3 2, 8, pp
FILTER_HOR_CHROMA_sse3 2, 16, pp
FILTER_HOR_CHROMA_sse3 4, 2, pp
FILTER_HOR_CHROMA_sse3 4, 4, pp
FILTER_HOR_CHROMA_sse3 4, 8, pp
FILTER_HOR_CHROMA_sse3 4, 16, pp
FILTER_HOR_CHROMA_sse3 4, 32, pp
FILTER_HOR_CHROMA_sse3 6, 8, pp
FILTER_HOR_CHROMA_sse3 6, 16, pp
FILTER_HOR_CHROMA_sse3 8, 2, pp
FILTER_HOR_CHROMA_sse3 8, 4, pp
FILTER_HOR_CHROMA_sse3 8, 6, pp
FILTER_HOR_CHROMA_sse3 8, 8, pp
FILTER_HOR_CHROMA_sse3 8, 12, pp
FILTER_HOR_CHROMA_sse3 8, 16, pp
FILTER_HOR_CHROMA_sse3 8, 32, pp
FILTER_HOR_CHROMA_sse3 8, 64, pp
FILTER_HOR_CHROMA_sse3 12, 16, pp
FILTER_HOR_CHROMA_sse3 12, 32, pp
FILTER_HOR_CHROMA_sse3 16, 4, pp
FILTER_HOR_CHROMA_sse3 16, 8, pp
FILTER_HOR_CHROMA_sse3 16, 12, pp
FILTER_HOR_CHROMA_sse3 16, 16, pp
FILTER_HOR_CHROMA_sse3 16, 24, pp
FILTER_HOR_CHROMA_sse3 16, 32, pp
FILTER_HOR_CHROMA_sse3 16, 64, pp
FILTER_HOR_CHROMA_sse3 24, 32, pp
FILTER_HOR_CHROMA_sse3 24, 64, pp
FILTER_HOR_CHROMA_sse3 32, 8, pp
FILTER_HOR_CHROMA_sse3 32, 16, pp
FILTER_HOR_CHROMA_sse3 32, 24, pp
FILTER_HOR_CHROMA_sse3 32, 32, pp
FILTER_HOR_CHROMA_sse3 32, 48, pp
FILTER_HOR_CHROMA_sse3 32, 64, pp
FILTER_HOR_CHROMA_sse3 48, 64, pp
FILTER_HOR_CHROMA_sse3 64, 16, pp
FILTER_HOR_CHROMA_sse3 64, 32, pp
FILTER_HOR_CHROMA_sse3 64, 48, pp
FILTER_HOR_CHROMA_sse3 64, 64, pp

;-----------------------------------------------------------------------------
; void interp_4tap_horiz_ps_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------

FILTER_HOR_CHROMA_sse3 2, 4, ps
FILTER_HOR_CHROMA_sse3 2, 8, ps
FILTER_HOR_CHROMA_sse3 2, 16, ps
FILTER_HOR_CHROMA_sse3 4, 2, ps
FILTER_HOR_CHROMA_sse3 4, 4, ps
FILTER_HOR_CHROMA_sse3 4, 8, ps
FILTER_HOR_CHROMA_sse3 4, 16, ps
FILTER_HOR_CHROMA_sse3 4, 32, ps
FILTER_HOR_CHROMA_sse3 6, 8, ps
FILTER_HOR_CHROMA_sse3 6, 16, ps
FILTER_HOR_CHROMA_sse3 8, 2, ps
FILTER_HOR_CHROMA_sse3 8, 4, ps
FILTER_HOR_CHROMA_sse3 8, 6, ps
FILTER_HOR_CHROMA_sse3 8, 8, ps
FILTER_HOR_CHROMA_sse3 8, 12, ps
FILTER_HOR_CHROMA_sse3 8, 16, ps
FILTER_HOR_CHROMA_sse3 8, 32, ps
FILTER_HOR_CHROMA_sse3 8, 64, ps
FILTER_HOR_CHROMA_sse3 12, 16, ps
FILTER_HOR_CHROMA_sse3 12, 32, ps
FILTER_HOR_CHROMA_sse3 16, 4, ps
FILTER_HOR_CHROMA_sse3 16, 8, ps
FILTER_HOR_CHROMA_sse3 16, 12, ps
FILTER_HOR_CHROMA_sse3 16, 16, ps
FILTER_HOR_CHROMA_sse3 16, 24, ps
FILTER_HOR_CHROMA_sse3 16, 32, ps
FILTER_HOR_CHROMA_sse3 16, 64, ps
FILTER_HOR_CHROMA_sse3 24, 32, ps
FILTER_HOR_CHROMA_sse3 24, 64, ps
FILTER_HOR_CHROMA_sse3 32, 8, ps
FILTER_HOR_CHROMA_sse3 32, 16, ps
FILTER_HOR_CHROMA_sse3 32, 24, ps
FILTER_HOR_CHROMA_sse3 32, 32, ps
FILTER_HOR_CHROMA_sse3 32, 48, ps
FILTER_HOR_CHROMA_sse3 32, 64, ps
FILTER_HOR_CHROMA_sse3 48, 64, ps
FILTER_HOR_CHROMA_sse3 64, 16, ps
FILTER_HOR_CHROMA_sse3 64, 32, ps
FILTER_HOR_CHROMA_sse3 64, 48, ps
FILTER_HOR_CHROMA_sse3 64, 64, ps

%macro FILTER_P2S_2_4_sse2 1
    movd        m0,     [r0 + %1]
    movd        m2,     [r0 + r1 * 2 + %1]
    movhps      m0,     [r0 + r1 + %1]
    movhps      m2,     [r0 + r4 + %1]
    psllw       m0,     (14 - BIT_DEPTH)
    psllw       m2,     (14 - BIT_DEPTH)
    psubw       m0,     m1
    psubw       m2,     m1

    movd        [r2 + r3 * 0 + %1], m0
    movd        [r2 + r3 * 2 + %1], m2
    movhlps     m0,     m0
    movhlps     m2,     m2
    movd        [r2 + r3 * 1 + %1], m0
    movd        [r2 + r5 + %1], m2
%endmacro

%macro FILTER_P2S_4_4_sse2 1
    movh        m0,     [r0 + %1]
    movhps      m0,     [r0 + r1 + %1]
    psllw       m0,     (14 - BIT_DEPTH)
    psubw       m0,     m1
    movh        [r2 + r3 * 0 + %1], m0
    movhps      [r2 + r3 * 1 + %1], m0

    movh        m2,     [r0 + r1 * 2 + %1]
    movhps      m2,     [r0 + r4 + %1]
    psllw       m2,     (14 - BIT_DEPTH)
    psubw       m2,     m1
    movh        [r2 + r3 * 2 + %1], m2
    movhps      [r2 + r5 + %1], m2
%endmacro

%macro FILTER_P2S_4_2_sse2 0
    movh        m0,     [r0]
    movhps      m0,     [r0 + r1 * 2]
    psllw       m0,     (14 - BIT_DEPTH)
    psubw       m0,     [pw_2000]
    movh        [r2 + r3 * 0], m0
    movhps      [r2 + r3 * 2], m0
%endmacro

%macro FILTER_P2S_8_4_sse2 1
    movu        m0,     [r0 + %1]
    movu        m2,     [r0 + r1 + %1]
    psllw       m0,     (14 - BIT_DEPTH)
    psllw       m2,     (14 - BIT_DEPTH)
    psubw       m0,     m1
    psubw       m2,     m1
    movu        [r2 + r3 * 0 + %1], m0
    movu        [r2 + r3 * 1 + %1], m2

    movu        m3,     [r0 + r1 * 2 + %1]
    movu        m4,     [r0 + r4 + %1]
    psllw       m3,     (14 - BIT_DEPTH)
    psllw       m4,     (14 - BIT_DEPTH)
    psubw       m3,     m1
    psubw       m4,     m1
    movu        [r2 + r3 * 2 + %1], m3
    movu        [r2 + r5 + %1], m4
%endmacro

%macro FILTER_P2S_8_2_sse2 1
    movu        m0,     [r0 + %1]
    movu        m2,     [r0 + r1 + %1]
    psllw       m0,     (14 - BIT_DEPTH)
    psllw       m2,     (14 - BIT_DEPTH)
    psubw       m0,     m1
    psubw       m2,     m1
    movu        [r2 + r3 * 0 + %1], m0
    movu        [r2 + r3 * 1 + %1], m2
%endmacro

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro FILTER_PIX_TO_SHORT_sse2 2
INIT_XMM sse2
cglobal filterPixelToShort_%1x%2, 4, 6, 3
%if %2 == 2
%if %1 == 4
    FILTER_P2S_4_2_sse2
%elif %1 == 8
    add        r1d, r1d
    add        r3d, r3d
    mova       m1, [pw_2000]
    FILTER_P2S_8_2_sse2 0
%endif
%else
    add        r1d, r1d
    add        r3d, r3d
    mova       m1, [pw_2000]
    lea        r4, [r1 * 3]
    lea        r5, [r3 * 3]
%assign y 1
%rep %2/4
%assign x 0
%rep %1/8
    FILTER_P2S_8_4_sse2 x
%if %2 == 6
    lea         r0,     [r0 + 4 * r1]
    lea         r2,     [r2 + 4 * r3]
    FILTER_P2S_8_2_sse2 x
%endif
%assign x x+16
%endrep
%rep (%1 % 8)/4
    FILTER_P2S_4_4_sse2 x
%assign x x+8
%endrep
%rep (%1 % 4)/2
    FILTER_P2S_2_4_sse2 x
%endrep
%if y < %2/4
    lea         r0,     [r0 + 4 * r1]
    lea         r2,     [r2 + 4 * r3]
%assign y y+1
%endif
%endrep
%endif
RET
%endmacro

    FILTER_PIX_TO_SHORT_sse2 2, 4
    FILTER_PIX_TO_SHORT_sse2 2, 8
    FILTER_PIX_TO_SHORT_sse2 2, 16
    FILTER_PIX_TO_SHORT_sse2 4, 2
    FILTER_PIX_TO_SHORT_sse2 4, 4
    FILTER_PIX_TO_SHORT_sse2 4, 8
    FILTER_PIX_TO_SHORT_sse2 4, 16
    FILTER_PIX_TO_SHORT_sse2 4, 32
    FILTER_PIX_TO_SHORT_sse2 6, 8
    FILTER_PIX_TO_SHORT_sse2 6, 16
    FILTER_PIX_TO_SHORT_sse2 8, 2
    FILTER_PIX_TO_SHORT_sse2 8, 4
    FILTER_PIX_TO_SHORT_sse2 8, 6
    FILTER_PIX_TO_SHORT_sse2 8, 8
    FILTER_PIX_TO_SHORT_sse2 8, 12
    FILTER_PIX_TO_SHORT_sse2 8, 16
    FILTER_PIX_TO_SHORT_sse2 8, 32
    FILTER_PIX_TO_SHORT_sse2 8, 64
    FILTER_PIX_TO_SHORT_sse2 12, 16
    FILTER_PIX_TO_SHORT_sse2 12, 32
    FILTER_PIX_TO_SHORT_sse2 16, 4
    FILTER_PIX_TO_SHORT_sse2 16, 8
    FILTER_PIX_TO_SHORT_sse2 16, 12
    FILTER_PIX_TO_SHORT_sse2 16, 16
    FILTER_PIX_TO_SHORT_sse2 16, 24
    FILTER_PIX_TO_SHORT_sse2 16, 32
    FILTER_PIX_TO_SHORT_sse2 16, 64
    FILTER_PIX_TO_SHORT_sse2 24, 32
    FILTER_PIX_TO_SHORT_sse2 24, 64
    FILTER_PIX_TO_SHORT_sse2 32, 8
    FILTER_PIX_TO_SHORT_sse2 32, 16
    FILTER_PIX_TO_SHORT_sse2 32, 24
    FILTER_PIX_TO_SHORT_sse2 32, 32
    FILTER_PIX_TO_SHORT_sse2 32, 48
    FILTER_PIX_TO_SHORT_sse2 32, 64
    FILTER_PIX_TO_SHORT_sse2 48, 64
    FILTER_PIX_TO_SHORT_sse2 64, 16
    FILTER_PIX_TO_SHORT_sse2 64, 32
    FILTER_PIX_TO_SHORT_sse2 64, 48
    FILTER_PIX_TO_SHORT_sse2 64, 64

;------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_4x4(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W4 3
INIT_XMM sse4
cglobal interp_8tap_horiz_%3_%1x%2, 4, 7, 8
    mov         r4d, r4m
    sub         r0, 6
    shl         r4d, 4
    add         r1, r1
    add         r3, r3

%ifdef PIC
    lea         r6, [tab_LumaCoeff]
    mova        m0, [r6 + r4]
%else
    mova        m0, [tab_LumaCoeff + r4]
%endif

%ifidn %3, pp
    mova        m1, [pd_32]
    pxor        m6, m6
    mova        m7, [pw_pixel_max]
%else
    mova        m1, [INTERP_OFFSET_PS]
%endif

    mov         r4d, %2
%ifidn %3, ps
    cmp         r5m, byte 0
    je          .loopH
    lea         r6, [r1 + 2 * r1]
    sub         r0, r6
    add         r4d, 7
%endif

.loopH:
    movu        m2, [r0]                     ; m2 = src[0-7]
    movu        m3, [r0 + 16]                ; m3 = src[8-15]

    pmaddwd     m4, m2, m0
    palignr     m5, m3, m2, 2                ; m5 = src[1-8]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m3, m2, 4                ; m5 = src[2-9]
    pmaddwd     m5, m0
    palignr     m3, m2, 6                    ; m3 = src[3-10]
    pmaddwd     m3, m0
    phaddd      m5, m3

    phaddd      m4, m5
    paddd       m4, m1
%ifidn %3, pp
    psrad       m4, 6
    packusdw    m4, m4
    CLIPW       m4, m6, m7
%else
    psrad       m4, INTERP_SHIFT_PS
    packssdw    m4, m4
%endif

    movh        [r2], m4

    add         r0, r1
    add         r2, r3

    dec         r4d
    jnz         .loopH
    RET
%endmacro

;------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_4x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W4 4, 4, pp
FILTER_HOR_LUMA_W4 4, 8, pp
FILTER_HOR_LUMA_W4 4, 16, pp

;---------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_4x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;---------------------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W4 4, 4, ps
FILTER_HOR_LUMA_W4 4, 8, ps
FILTER_HOR_LUMA_W4 4, 16, ps

;------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_%3_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W8 3
INIT_XMM sse4
cglobal interp_8tap_horiz_%3_%1x%2, 4, 7, 8

    add         r1, r1
    add         r3, r3
    mov         r4d, r4m
    sub         r0, 6
    shl         r4d, 4

%ifdef PIC
    lea         r6, [tab_LumaCoeff]
    mova        m0, [r6 + r4]
%else
    mova        m0, [tab_LumaCoeff + r4]
%endif

%ifidn %3, pp
    mova        m1, [pd_32]
    pxor        m7, m7
%else
    mova        m1, [INTERP_OFFSET_PS]
%endif

    mov         r4d, %2
%ifidn %3, ps
    cmp         r5m, byte 0
    je         .loopH
    lea         r6, [r1 + 2 * r1]
    sub         r0, r6
    add         r4d, 7
%endif

.loopH:
    movu        m2, [r0]                     ; m2 = src[0-7]
    movu        m3, [r0 + 16]                ; m3 = src[8-15]

    pmaddwd     m4, m2, m0
    palignr     m5, m3, m2, 2                ; m5 = src[1-8]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m3, m2, 4                ; m5 = src[2-9]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 6                ; m6 = src[3-10]
    pmaddwd     m6, m0
    phaddd      m5, m6
    phaddd      m4, m5
    paddd       m4, m1

    palignr     m5, m3, m2, 8                ; m5 = src[4-11]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 10               ; m6 = src[5-12]
    pmaddwd     m6, m0
    phaddd      m5, m6

    palignr     m6, m3, m2, 12               ; m6 = src[6-13]
    pmaddwd     m6, m0
    palignr     m3, m2, 14                   ; m3 = src[7-14]
    pmaddwd     m3, m0
    phaddd      m6, m3
    phaddd      m5, m6
    paddd       m5, m1
%ifidn %3, pp
    psrad       m4, 6
    psrad       m5, 6
    packusdw    m4, m5
    CLIPW       m4, m7, [pw_pixel_max]
%else
    psrad       m4, INTERP_SHIFT_PS
    psrad       m5, INTERP_SHIFT_PS
    packssdw    m4, m5
%endif

    movu        [r2], m4

    add         r0, r1
    add         r2, r3

    dec         r4d
    jnz        .loopH
    RET
%endmacro

;------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_8x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W8 8, 4, pp
FILTER_HOR_LUMA_W8 8, 8, pp
FILTER_HOR_LUMA_W8 8, 16, pp
FILTER_HOR_LUMA_W8 8, 32, pp

;---------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_8x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;---------------------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W8 8, 4, ps
FILTER_HOR_LUMA_W8 8, 8, ps
FILTER_HOR_LUMA_W8 8, 16, ps
FILTER_HOR_LUMA_W8 8, 32, ps

;--------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_%3_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;--------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W12 3
INIT_XMM sse4
cglobal interp_8tap_horiz_%3_%1x%2, 4, 7, 8

    add         r1, r1
    add         r3, r3
    mov         r4d, r4m
    sub         r0, 6
    shl         r4d, 4

%ifdef PIC
    lea         r6, [tab_LumaCoeff]
    mova        m0, [r6 + r4]
%else
    mova        m0, [tab_LumaCoeff + r4]
%endif
%ifidn %3, pp
    mova        m1, [INTERP_OFFSET_PP]
%else
    mova        m1, [INTERP_OFFSET_PS]
%endif

    mov         r4d, %2
%ifidn %3, ps
    cmp         r5m, byte 0
    je          .loopH
    lea         r6, [r1 + 2 * r1]
    sub         r0, r6
    add         r4d, 7
%endif

.loopH:
    movu        m2, [r0]                     ; m2 = src[0-7]
    movu        m3, [r0 + 16]                ; m3 = src[8-15]

    pmaddwd     m4, m2, m0
    palignr     m5, m3, m2, 2                ; m5 = src[1-8]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m3, m2, 4                ; m5 = src[2-9]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 6                ; m6 = src[3-10]
    pmaddwd     m6, m0
    phaddd      m5, m6
    phaddd      m4, m5
    paddd       m4, m1

    palignr     m5, m3, m2, 8                ; m5 = src[4-11]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 10               ; m6 = src[5-12]
    pmaddwd     m6, m0
    phaddd      m5, m6

    palignr     m6, m3, m2, 12               ; m6 = src[6-13]
    pmaddwd     m6, m0
    palignr     m7, m3, m2, 14               ; m2 = src[7-14]
    pmaddwd     m7, m0
    phaddd      m6, m7
    phaddd      m5, m6
    paddd       m5, m1
%ifidn %3, pp
    psrad       m4, INTERP_SHIFT_PP
    psrad       m5, INTERP_SHIFT_PP
    packusdw    m4, m5
    pxor        m5, m5
    CLIPW       m4, m5, [pw_pixel_max]
%else
    psrad       m4, INTERP_SHIFT_PS
    psrad       m5, INTERP_SHIFT_PS
    packssdw    m4, m5
%endif

    movu        [r2], m4

    movu        m2, [r0 + 32]                ; m2 = src[16-23]

    pmaddwd     m4, m3, m0                   ; m3 = src[8-15]
    palignr     m5, m2, m3, 2                ; m5 = src[9-16]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m2, m3, 4                ; m5 = src[10-17]
    pmaddwd     m5, m0
    palignr     m2, m3, 6                    ; m2 = src[11-18]
    pmaddwd     m2, m0
    phaddd      m5, m2
    phaddd      m4, m5
    paddd       m4, m1
%ifidn %3, pp
    psrad       m4, INTERP_SHIFT_PP
    packusdw    m4, m4
    pxor        m5, m5
    CLIPW       m4, m5, [pw_pixel_max]
%else
    psrad       m4, INTERP_SHIFT_PS
    packssdw    m4, m4
%endif

    movh        [r2 + 16], m4

    add         r0, r1
    add         r2, r3

    dec         r4d
    jnz         .loopH
    RET
%endmacro

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_12x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W12 12, 16, pp

;----------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_12x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;----------------------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W12 12, 16, ps

;--------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_%3_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;--------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W16 3
INIT_XMM sse4
cglobal interp_8tap_horiz_%3_%1x%2, 4, 7, 8

    add         r1, r1
    add         r3, r3
    mov         r4d, r4m
    sub         r0, 6
    shl         r4d, 4

%ifdef PIC
    lea         r6, [tab_LumaCoeff]
    mova        m0, [r6 + r4]
%else
    mova        m0, [tab_LumaCoeff + r4]
%endif

%ifidn %3, pp
    mova        m1, [pd_32]
%else
    mova        m1, [INTERP_OFFSET_PS]
%endif

    mov         r4d, %2
%ifidn %3, ps
    cmp         r5m, byte 0
    je          .loopH
    lea         r6, [r1 + 2 * r1]
    sub         r0, r6
    add         r4d, 7
%endif

.loopH:
%assign x 0
%rep %1 / 16
    movu        m2, [r0 + x]                 ; m2 = src[0-7]
    movu        m3, [r0 + 16 + x]            ; m3 = src[8-15]

    pmaddwd     m4, m2, m0
    palignr     m5, m3, m2, 2                ; m5 = src[1-8]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m3, m2, 4                ; m5 = src[2-9]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 6                ; m6 = src[3-10]
    pmaddwd     m6, m0
    phaddd      m5, m6
    phaddd      m4, m5
    paddd       m4, m1

    palignr     m5, m3, m2, 8                ; m5 = src[4-11]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 10               ; m6 = src[5-12]
    pmaddwd     m6, m0
    phaddd      m5, m6

    palignr     m6, m3, m2, 12               ; m6 = src[6-13]
    pmaddwd     m6, m0
    palignr     m7, m3, m2, 14               ; m2 = src[7-14]
    pmaddwd     m7, m0
    phaddd      m6, m7
    phaddd      m5, m6
    paddd       m5, m1
%ifidn %3, pp
    psrad       m4, INTERP_SHIFT_PP
    psrad       m5, INTERP_SHIFT_PP
    packusdw    m4, m5
    pxor        m5, m5
    CLIPW       m4, m5, [pw_pixel_max]
%else
    psrad       m4, INTERP_SHIFT_PS
    psrad       m5, INTERP_SHIFT_PS
    packssdw    m4, m5
%endif
    movu        [r2 + x], m4

    movu        m2, [r0 + 32 + x]            ; m2 = src[16-23]

    pmaddwd     m4, m3, m0                   ; m3 = src[8-15]
    palignr     m5, m2, m3, 2                ; m5 = src[9-16]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m2, m3, 4                ; m5 = src[10-17]
    pmaddwd     m5, m0
    palignr     m6, m2, m3, 6                ; m6 = src[11-18]
    pmaddwd     m6, m0
    phaddd      m5, m6
    phaddd      m4, m5
    paddd       m4, m1

    palignr     m5, m2, m3, 8                ; m5 = src[12-19]
    pmaddwd     m5, m0
    palignr     m6, m2, m3, 10               ; m6 = src[13-20]
    pmaddwd     m6, m0
    phaddd      m5, m6

    palignr     m6, m2, m3, 12               ; m6 = src[14-21]
    pmaddwd     m6, m0
    palignr     m2, m3, 14                   ; m3 = src[15-22]
    pmaddwd     m2, m0
    phaddd      m6, m2
    phaddd      m5, m6
    paddd       m5, m1
%ifidn %3, pp
    psrad       m4, INTERP_SHIFT_PP
    psrad       m5, INTERP_SHIFT_PP
    packusdw    m4, m5
    pxor        m5, m5
    CLIPW       m4, m5, [pw_pixel_max]
%else
    psrad       m4, INTERP_SHIFT_PS
    psrad       m5, INTERP_SHIFT_PS
    packssdw    m4, m5
%endif
    movu        [r2 + 16 + x], m4

%assign x x+32
%endrep

    add         r0, r1
    add         r2, r3

    dec         r4d
    jnz         .loopH
    RET
%endmacro

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_16x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W16 16, 4, pp
FILTER_HOR_LUMA_W16 16, 8, pp
FILTER_HOR_LUMA_W16 16, 12, pp
FILTER_HOR_LUMA_W16 16, 16, pp
FILTER_HOR_LUMA_W16 16, 32, pp
FILTER_HOR_LUMA_W16 16, 64, pp

;----------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_16x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;----------------------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W16 16, 4, ps
FILTER_HOR_LUMA_W16 16, 8, ps
FILTER_HOR_LUMA_W16 16, 12, ps
FILTER_HOR_LUMA_W16 16, 16, ps
FILTER_HOR_LUMA_W16 16, 32, ps
FILTER_HOR_LUMA_W16 16, 64, ps

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_32x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W16 32, 8, pp
FILTER_HOR_LUMA_W16 32, 16, pp
FILTER_HOR_LUMA_W16 32, 24, pp
FILTER_HOR_LUMA_W16 32, 32, pp
FILTER_HOR_LUMA_W16 32, 64, pp

;----------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_32x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;----------------------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W16 32, 8, ps
FILTER_HOR_LUMA_W16 32, 16, ps
FILTER_HOR_LUMA_W16 32, 24, ps
FILTER_HOR_LUMA_W16 32, 32, ps
FILTER_HOR_LUMA_W16 32, 64, ps

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_48x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W16 48, 64, pp

;----------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_48x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;----------------------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W16 48, 64, ps

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_64x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W16 64, 16, pp
FILTER_HOR_LUMA_W16 64, 32, pp
FILTER_HOR_LUMA_W16 64, 48, pp
FILTER_HOR_LUMA_W16 64, 64, pp

;----------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_64x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;----------------------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W16 64, 16, ps
FILTER_HOR_LUMA_W16 64, 32, ps
FILTER_HOR_LUMA_W16 64, 48, ps
FILTER_HOR_LUMA_W16 64, 64, ps

;--------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_%3_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;--------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W24 3
INIT_XMM sse4
cglobal interp_8tap_horiz_%3_%1x%2, 4, 7, 8

    add         r1, r1
    add         r3, r3
    mov         r4d, r4m
    sub         r0, 6
    shl         r4d, 4

%ifdef PIC
    lea         r6, [tab_LumaCoeff]
    mova        m0, [r6 + r4]
%else
    mova        m0, [tab_LumaCoeff + r4]
%endif
%ifidn %3, pp
    mova        m1, [pd_32]
%else
    mova        m1, [INTERP_OFFSET_PS]
%endif

    mov         r4d, %2
%ifidn %3, ps
    cmp         r5m, byte 0
    je          .loopH
    lea         r6, [r1 + 2 * r1]
    sub         r0, r6
    add         r4d, 7
%endif

.loopH:
    movu        m2, [r0]                     ; m2 = src[0-7]
    movu        m3, [r0 + 16]                ; m3 = src[8-15]

    pmaddwd     m4, m2, m0
    palignr     m5, m3, m2, 2                ; m5 = src[1-8]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m3, m2, 4                ; m5 = src[2-9]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 6                ; m6 = src[3-10]
    pmaddwd     m6, m0
    phaddd      m5, m6
    phaddd      m4, m5
    paddd       m4, m1

    palignr     m5, m3, m2, 8                ; m5 = src[4-11]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 10               ; m6 = src[5-12]
    pmaddwd     m6, m0
    phaddd      m5, m6

    palignr     m6, m3, m2, 12               ; m6 = src[6-13]
    pmaddwd     m6, m0
    palignr     m7, m3, m2, 14               ; m7 = src[7-14]
    pmaddwd     m7, m0
    phaddd      m6, m7
    phaddd      m5, m6
    paddd       m5, m1
%ifidn %3, pp
    psrad       m4, INTERP_SHIFT_PP
    psrad       m5, INTERP_SHIFT_PP
    packusdw    m4, m5
    pxor        m5, m5
    CLIPW       m4, m5, [pw_pixel_max]
%else
    psrad       m4, INTERP_SHIFT_PS
    psrad       m5, INTERP_SHIFT_PS
    packssdw    m4, m5
%endif
    movu        [r2], m4

    movu        m2, [r0 + 32]                ; m2 = src[16-23]

    pmaddwd     m4, m3, m0                   ; m3 = src[8-15]
    palignr     m5, m2, m3, 2                ; m5 = src[1-8]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m2, m3, 4                ; m5 = src[2-9]
    pmaddwd     m5, m0
    palignr     m6, m2, m3, 6                ; m6 = src[3-10]
    pmaddwd     m6, m0
    phaddd      m5, m6
    phaddd      m4, m5
    paddd       m4, m1

    palignr     m5, m2, m3, 8                ; m5 = src[4-11]
    pmaddwd     m5, m0
    palignr     m6, m2, m3, 10               ; m6 = src[5-12]
    pmaddwd     m6, m0
    phaddd      m5, m6

    palignr     m6, m2, m3, 12               ; m6 = src[6-13]
    pmaddwd     m6, m0
    palignr     m7, m2, m3, 14               ; m7 = src[7-14]
    pmaddwd     m7, m0
    phaddd      m6, m7
    phaddd      m5, m6
    paddd       m5, m1
%ifidn %3, pp
    psrad       m4, INTERP_SHIFT_PP
    psrad       m5, INTERP_SHIFT_PP
    packusdw    m4, m5
    pxor        m5, m5
    CLIPW       m4, m5, [pw_pixel_max]
%else
    psrad       m4, INTERP_SHIFT_PS
    psrad       m5, INTERP_SHIFT_PS
    packssdw    m4, m5
%endif
    movu        [r2 + 16], m4

    movu        m3, [r0 + 48]                ; m3 = src[24-31]

    pmaddwd     m4, m2, m0                   ; m2 = src[16-23]
    palignr     m5, m3, m2, 2                ; m5 = src[1-8]
    pmaddwd     m5, m0
    phaddd      m4, m5

    palignr     m5, m3, m2, 4                ; m5 = src[2-9]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 6                ; m6 = src[3-10]
    pmaddwd     m6, m0
    phaddd      m5, m6
    phaddd      m4, m5
    paddd       m4, m1

    palignr     m5, m3, m2, 8                ; m5 = src[4-11]
    pmaddwd     m5, m0
    palignr     m6, m3, m2, 10               ; m6 = src[5-12]
    pmaddwd     m6, m0
    phaddd      m5, m6

    palignr     m6, m3, m2, 12               ; m6 = src[6-13]
    pmaddwd     m6, m0
    palignr     m7, m3, m2, 14               ; m7 = src[7-14]
    pmaddwd     m7, m0
    phaddd      m6, m7
    phaddd      m5, m6
    paddd       m5, m1
%ifidn %3, pp
    psrad       m4, INTERP_SHIFT_PP
    psrad       m5, INTERP_SHIFT_PP
    packusdw    m4, m5
    pxor        m5, m5
    CLIPW       m4, m5, [pw_pixel_max]
%else
    psrad       m4, INTERP_SHIFT_PS
    psrad       m5, INTERP_SHIFT_PS
    packssdw    m4, m5
%endif
    movu        [r2 + 32], m4

    add         r0, r1
    add         r2, r3

    dec         r4d
    jnz         .loopH
    RET
%endmacro

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp_24x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W24 24, 32, pp

;----------------------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_ps_24x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;----------------------------------------------------------------------------------------------------------------------------
FILTER_HOR_LUMA_W24 24, 32, ps

%macro FILTER_W2_2 1
    movu        m3,         [r0]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + r1]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    packusdw    m3,         m3
    CLIPW       m3,         m7,    m6
%else
    psrad       m3,         INTERP_SHIFT_PS
    packssdw    m3,         m3
%endif
    movd        [r2],       m3
    pextrd      [r2 + r3],  m3, 1
%endmacro

%macro FILTER_W4_2 1
    movu        m3,         [r0]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + r1]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + r1 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m7,    m6
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2],       m3
    movhps      [r2 + r3],  m3
%endmacro

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W4_avx2 1
INIT_YMM avx2
cglobal interp_8tap_horiz_pp_4x%1, 4,7,7
    add              r1d, r1d
    add              r3d, r3d
    sub              r0, 6
    mov              r4d, r4m
    shl              r4d, 4
%ifdef PIC
    lea              r5, [tab_LumaCoeff]
    vpbroadcastq     m0, [r5 + r4]
    vpbroadcastq     m1, [r5 + r4 + 8]
%else
    vpbroadcastq     m0, [tab_LumaCoeff + r4]
    vpbroadcastq     m1, [tab_LumaCoeff + r4 + 8]
%endif
    lea              r6, [pw_pixel_max]
    mova             m3, [interp8_hpp_shuf]
    mova             m6, [pd_32]
    pxor             m2, m2

    ; register map
    ; m0 , m1 interpolate coeff

    mov              r4d, %1/2

.loop:
    vbroadcasti128   m4, [r0]
    vbroadcasti128   m5, [r0 + 8]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    phaddd           m4, m4
    vpermq           m4, m4, q3120
    paddd            m4, m6
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [r6]
    movq             [r2], xm4

    vbroadcasti128   m4, [r0 + r1]
    vbroadcasti128   m5, [r0 + r1 + 8]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    phaddd           m4, m4
    vpermq           m4, m4, q3120
    paddd            m4, m6
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [r6]
    movq             [r2 + r3], xm4

    lea              r2, [r2 + 2 * r3]
    lea              r0, [r0 + 2 * r1]
    dec              r4d
    jnz              .loop
    RET
%endmacro
FILTER_HOR_LUMA_W4_avx2 4
FILTER_HOR_LUMA_W4_avx2 8
FILTER_HOR_LUMA_W4_avx2 16

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W8 1
INIT_YMM avx2
cglobal interp_8tap_horiz_pp_8x%1, 4,6,8
    add              r1d, r1d
    add              r3d, r3d
    sub              r0, 6
    mov              r4d, r4m
    shl              r4d, 4
%ifdef PIC
    lea              r5, [tab_LumaCoeff]
    vpbroadcastq     m0, [r5 + r4]
    vpbroadcastq     m1, [r5 + r4 + 8]
%else
    vpbroadcastq     m0, [tab_LumaCoeff + r4]
    vpbroadcastq     m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova             m3, [interp8_hpp_shuf]
    mova             m7, [pd_32]
    pxor             m2, m2

    ; register map
    ; m0 , m1 interpolate coeff

    mov              r4d, %1/2

.loop:
    vbroadcasti128   m4, [r0]
    vbroadcasti128   m5, [r0 + 8]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 8]
    vbroadcasti128   m6, [r0 + 16]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2], xm4

    vbroadcasti128   m4, [r0 + r1]
    vbroadcasti128   m5, [r0 + r1 + 8]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + r1 + 8]
    vbroadcasti128   m6, [r0 + r1 + 16]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + r3], xm4

    lea              r2, [r2 + 2 * r3]
    lea              r0, [r0 + 2 * r1]
    dec              r4d
    jnz              .loop
    RET
%endmacro
FILTER_HOR_LUMA_W8 4
FILTER_HOR_LUMA_W8 8
FILTER_HOR_LUMA_W8 16
FILTER_HOR_LUMA_W8 32

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W16 1
INIT_YMM avx2
cglobal interp_8tap_horiz_pp_16x%1, 4,6,8
    add              r1d, r1d
    add              r3d, r3d
    sub              r0, 6
    mov              r4d, r4m
    shl              r4d, 4
%ifdef PIC
    lea              r5, [tab_LumaCoeff]
    vpbroadcastq     m0, [r5 + r4]
    vpbroadcastq     m1, [r5 + r4 + 8]
%else
    vpbroadcastq     m0, [tab_LumaCoeff + r4]
    vpbroadcastq     m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova             m3, [interp8_hpp_shuf]
    mova             m7, [pd_32]
    pxor             m2, m2

    ; register map
    ; m0 , m1 interpolate coeff

    mov              r4d, %1

.loop:
    vbroadcasti128   m4, [r0]
    vbroadcasti128   m5, [r0 + 8]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 8]
    vbroadcasti128   m6, [r0 + 16]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2], xm4

    vbroadcasti128   m4, [r0 + 16]
    vbroadcasti128   m5, [r0 + 24]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 24]
    vbroadcasti128   m6, [r0 + 32]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + 16], xm4

    add              r2, r3
    add              r0, r1
    dec              r4d
    jnz              .loop
    RET
%endmacro
FILTER_HOR_LUMA_W16 4
FILTER_HOR_LUMA_W16 8
FILTER_HOR_LUMA_W16 12
FILTER_HOR_LUMA_W16 16
FILTER_HOR_LUMA_W16 32
FILTER_HOR_LUMA_W16 64

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
%macro FILTER_HOR_LUMA_W32 2
INIT_YMM avx2
cglobal interp_8tap_horiz_pp_%1x%2, 4,6,8
    add              r1d, r1d
    add              r3d, r3d
    sub              r0, 6
    mov              r4d, r4m
    shl              r4d, 4
%ifdef PIC
    lea              r5, [tab_LumaCoeff]
    vpbroadcastq     m0, [r5 + r4]
    vpbroadcastq     m1, [r5 + r4 + 8]
%else
    vpbroadcastq     m0, [tab_LumaCoeff + r4]
    vpbroadcastq     m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova             m3, [interp8_hpp_shuf]
    mova             m7, [pd_32]
    pxor             m2, m2

    ; register map
    ; m0 , m1 interpolate coeff

    mov              r4d, %2

.loop:
%assign x 0
%rep %1/16
    vbroadcasti128   m4, [r0 + x]
    vbroadcasti128   m5, [r0 + 8 + x]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 8 + x]
    vbroadcasti128   m6, [r0 + 16 + x]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + x], xm4

    vbroadcasti128   m4, [r0 + 16 + x]
    vbroadcasti128   m5, [r0 + 24 + x]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 24 + x]
    vbroadcasti128   m6, [r0 + 32 + x]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + 16 + x], xm4

%assign x x+32
%endrep

    add              r2, r3
    add              r0, r1
    dec              r4d
    jnz              .loop
    RET
%endmacro
FILTER_HOR_LUMA_W32 32, 8
FILTER_HOR_LUMA_W32 32, 16
FILTER_HOR_LUMA_W32 32, 24
FILTER_HOR_LUMA_W32 32, 32
FILTER_HOR_LUMA_W32 32, 64
FILTER_HOR_LUMA_W32 64, 16
FILTER_HOR_LUMA_W32 64, 32
FILTER_HOR_LUMA_W32 64, 48
FILTER_HOR_LUMA_W32 64, 64

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal interp_8tap_horiz_pp_12x16, 4,6,8
    add              r1d, r1d
    add              r3d, r3d
    sub              r0, 6
    mov              r4d, r4m
    shl              r4d, 4
%ifdef PIC
    lea              r5, [tab_LumaCoeff]
    vpbroadcastq     m0, [r5 + r4]
    vpbroadcastq     m1, [r5 + r4 + 8]
%else
    vpbroadcastq     m0, [tab_LumaCoeff + r4]
    vpbroadcastq     m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova             m3, [interp8_hpp_shuf]
    mova             m7, [pd_32]
    pxor             m2, m2

    ; register map
    ; m0 , m1 interpolate coeff

    mov              r4d, 16

.loop:
    vbroadcasti128   m4, [r0]
    vbroadcasti128   m5, [r0 + 8]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 8]
    vbroadcasti128   m6, [r0 + 16]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2], xm4

    vbroadcasti128   m4, [r0 + 16]
    vbroadcasti128   m5, [r0 + 24]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 24]
    vbroadcasti128   m6, [r0 + 32]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movq             [r2 + 16], xm4

    add              r2, r3
    add              r0, r1
    dec              r4d
    jnz              .loop
    RET

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal interp_8tap_horiz_pp_24x32, 4,6,8
    add              r1d, r1d
    add              r3d, r3d
    sub              r0, 6
    mov              r4d, r4m
    shl              r4d, 4
%ifdef PIC
    lea              r5, [tab_LumaCoeff]
    vpbroadcastq     m0, [r5 + r4]
    vpbroadcastq     m1, [r5 + r4 + 8]
%else
    vpbroadcastq     m0, [tab_LumaCoeff + r4]
    vpbroadcastq     m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova             m3, [interp8_hpp_shuf]
    mova             m7, [pd_32]
    pxor             m2, m2

    ; register map
    ; m0 , m1 interpolate coeff

    mov              r4d, 32

.loop:
    vbroadcasti128   m4, [r0]
    vbroadcasti128   m5, [r0 + 8]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 8]
    vbroadcasti128   m6, [r0 + 16]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2], xm4

    vbroadcasti128   m4, [r0 + 16]
    vbroadcasti128   m5, [r0 + 24]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 24]
    vbroadcasti128   m6, [r0 + 32]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + 16], xm4

    vbroadcasti128   m4, [r0 + 32]
    vbroadcasti128   m5, [r0 + 40]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 40]
    vbroadcasti128   m6, [r0 + 48]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + 32], xm4

    add              r2, r3
    add              r0, r1
    dec              r4d
    jnz              .loop
    RET

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_horiz_pp(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal interp_8tap_horiz_pp_48x64, 4,6,8
    add              r1d, r1d
    add              r3d, r3d
    sub              r0, 6
    mov              r4d, r4m
    shl              r4d, 4
%ifdef PIC
    lea              r5, [tab_LumaCoeff]
    vpbroadcastq     m0, [r5 + r4]
    vpbroadcastq     m1, [r5 + r4 + 8]
%else
    vpbroadcastq     m0, [tab_LumaCoeff + r4]
    vpbroadcastq     m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova             m3, [interp8_hpp_shuf]
    mova             m7, [pd_32]
    pxor             m2, m2

    ; register map
    ; m0 , m1 interpolate coeff

    mov              r4d, 64

.loop:
%assign x 0
%rep 2
    vbroadcasti128   m4, [r0 + x]
    vbroadcasti128   m5, [r0 + 8 + x]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 8 + x]
    vbroadcasti128   m6, [r0 + 16 + x]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + x], xm4

    vbroadcasti128   m4, [r0 + 16 + x]
    vbroadcasti128   m5, [r0 + 24 + x]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 24 + x]
    vbroadcasti128   m6, [r0 + 32 + x]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + 16 + x], xm4

    vbroadcasti128   m4, [r0 + 32 + x]
    vbroadcasti128   m5, [r0 + 40 + x]
    pshufb           m4, m3
    pshufb           m5, m3

    pmaddwd          m4, m0
    pmaddwd          m5, m1
    paddd            m4, m5

    vbroadcasti128   m5, [r0 + 40 + x]
    vbroadcasti128   m6, [r0 + 48 + x]
    pshufb           m5, m3
    pshufb           m6, m3

    pmaddwd          m5, m0
    pmaddwd          m6, m1
    paddd            m5, m6

    phaddd           m4, m5
    vpermq           m4, m4, q3120
    paddd            m4, m7
    psrad            m4, INTERP_SHIFT_PP

    packusdw         m4, m4
    vpermq           m4, m4, q2020
    CLIPW            m4, m2, [pw_pixel_max]
    movu             [r2 + 32 + x], xm4

%assign x x+48
%endrep

    add              r2, r3
    add              r0, r1
    dec              r4d
    jnz              .loop
    RET

;-----------------------------------------------------------------------------
; void interp_4tap_horiz_%3_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------
%macro FILTER_CHROMA_H 6
INIT_XMM sse4
cglobal interp_4tap_horiz_%3_%1x%2, 4, %4, %5

    add         r3,       r3
    add         r1,       r1
    sub         r0,       2
    mov         r4d,      r4m
    add         r4d,      r4d

%ifdef PIC
    lea         r%6,      [tab_ChromaCoeff]
    movh        m0,       [r%6 + r4 * 4]
%else
    movh        m0,       [tab_ChromaCoeff + r4 * 4]
%endif

    punpcklqdq  m0,       m0
    mova        m2,       [tab_Tm16]

%ifidn %3, ps
    mova        m1,       [INTERP_OFFSET_PS]
    cmp         r5m, byte 0
    je          .skip
    sub         r0,       r1
    movu        m3,       [r0]
    pshufb      m3,       m3, m2
    pmaddwd     m3,       m0

  %if %1 == 4
    movu        m4,       [r0 + 4]
    pshufb      m4,       m4, m2
    pmaddwd     m4,       m0
    phaddd      m3,       m4
  %else
    phaddd      m3,       m3
  %endif

    paddd       m3,       m1
    psrad       m3,       INTERP_SHIFT_PS
    packssdw    m3,       m3

  %if %1 == 2
    movd        [r2],     m3
  %else
    movh        [r2],     m3
  %endif

    add         r0,       r1
    add         r2,       r3
    FILTER_W%1_2 %3
    lea         r0,       [r0 + 2 * r1]
    lea         r2,       [r2 + 2 * r3]

.skip:

%else     ;%ifidn %3, ps
    pxor        m7,       m7
    mova        m6,       [pw_pixel_max]
    mova        m1,       [tab_c_32]
%endif    ;%ifidn %3, ps

    FILTER_W%1_2 %3

%rep (%2/2) - 1
    lea         r0,       [r0 + 2 * r1]
    lea         r2,       [r2 + 2 * r3]
    FILTER_W%1_2 %3
%endrep
    RET
%endmacro

FILTER_CHROMA_H 2, 4, pp, 6, 8, 5
FILTER_CHROMA_H 2, 8, pp, 6, 8, 5
FILTER_CHROMA_H 4, 2, pp, 6, 8, 5
FILTER_CHROMA_H 4, 4, pp, 6, 8, 5
FILTER_CHROMA_H 4, 8, pp, 6, 8, 5
FILTER_CHROMA_H 4, 16, pp, 6, 8, 5

FILTER_CHROMA_H 2, 4, ps, 7, 5, 6
FILTER_CHROMA_H 2, 8, ps, 7, 5, 6
FILTER_CHROMA_H 4, 2, ps, 7, 6, 6
FILTER_CHROMA_H 4, 4, ps, 7, 6, 6
FILTER_CHROMA_H 4, 8, ps, 7, 6, 6
FILTER_CHROMA_H 4, 16, ps, 7, 6, 6

FILTER_CHROMA_H 2, 16, pp, 6, 8, 5
FILTER_CHROMA_H 4, 32, pp, 6, 8, 5
FILTER_CHROMA_H 2, 16, ps, 7, 5, 6
FILTER_CHROMA_H 4, 32, ps, 7, 6, 6


%macro FILTER_W6_1 1
    movu        m3,         [r0]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m4,         [r0 + 8]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m4,         m4
    paddd       m4,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m4,         INTERP_SHIFT_PP
    packusdw    m3,         m4
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m4,         INTERP_SHIFT_PS
    packssdw    m3,         m4
%endif
    movh        [r2],       m3
    pextrd      [r2 + 8],   m3, 2
%endmacro

cglobal chroma_filter_pp_6x1_internal
    FILTER_W6_1 pp
    ret

cglobal chroma_filter_ps_6x1_internal
    FILTER_W6_1 ps
    ret

%macro FILTER_W8_1 1
    movu        m3,         [r0]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 8]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 12]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2],       m3
    movhps      [r2 + 8],   m3
%endmacro

cglobal chroma_filter_pp_8x1_internal
    FILTER_W8_1 pp
    ret

cglobal chroma_filter_ps_8x1_internal
    FILTER_W8_1 ps
    ret

%macro FILTER_W12_1 1
    movu        m3,         [r0]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 8]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 12]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2],       m3
    movhps      [r2 + 8],   m3

    movu        m3,         [r0 + 16]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 20]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    packusdw    m3,         m3
    CLIPW       m3,         m6, m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    packssdw    m3,         m3
%endif
    movh        [r2 + 16],  m3
%endmacro

cglobal chroma_filter_pp_12x1_internal
    FILTER_W12_1 pp
    ret

cglobal chroma_filter_ps_12x1_internal
    FILTER_W12_1 ps
    ret

%macro FILTER_W16_1 1
    movu        m3,         [r0]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 8]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 12]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2],       m3
    movhps      [r2 + 8],   m3

    movu        m3,         [r0 + 16]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 20]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 24]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 28]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2 + 16],  m3
    movhps      [r2 + 24],  m3
%endmacro

cglobal chroma_filter_pp_16x1_internal
    FILTER_W16_1 pp
    ret

cglobal chroma_filter_ps_16x1_internal
    FILTER_W16_1 ps
    ret

%macro FILTER_W24_1 1
    movu        m3,         [r0]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 8]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 12]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2],       m3
    movhps      [r2 + 8],   m3

    movu        m3,         [r0 + 16]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 20]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 24]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 28]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2 + 16],  m3
    movhps      [r2 + 24],  m3

    movu        m3,         [r0 + 32]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 36]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 40]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 44]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2 + 32],  m3
    movhps      [r2 + 40],  m3
%endmacro

cglobal chroma_filter_pp_24x1_internal
    FILTER_W24_1 pp
    ret

cglobal chroma_filter_ps_24x1_internal
    FILTER_W24_1 ps
    ret

%macro FILTER_W32_1 1
    movu        m3,         [r0]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 8]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 12]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2],       m3
    movhps      [r2 + 8],   m3

    movu        m3,         [r0 + 16]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 20]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 24]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 28]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2 + 16],  m3
    movhps      [r2 + 24],  m3

    movu        m3,         [r0 + 32]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 36]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 40]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 44]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2 + 32],  m3
    movhps      [r2 + 40],  m3

    movu        m3,         [r0 + 48]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + 52]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + 56]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + 60]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2 + 48],  m3
    movhps      [r2 + 56],  m3
%endmacro

cglobal chroma_filter_pp_32x1_internal
    FILTER_W32_1 pp
    ret

cglobal chroma_filter_ps_32x1_internal
    FILTER_W32_1 ps
    ret

%macro FILTER_W8o_1 2
    movu        m3,         [r0 + %2]
    pshufb      m3,         m3, m2
    pmaddwd     m3,         m0
    movu        m4,         [r0 + %2 + 4]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m3,         m4
    paddd       m3,         m1

    movu        m5,         [r0 + %2 + 8]
    pshufb      m5,         m5, m2
    pmaddwd     m5,         m0
    movu        m4,         [r0 + %2 + 12]
    pshufb      m4,         m4, m2
    pmaddwd     m4,         m0
    phaddd      m5,         m4
    paddd       m5,         m1
%ifidn %1, pp
    psrad       m3,         INTERP_SHIFT_PP
    psrad       m5,         INTERP_SHIFT_PP
    packusdw    m3,         m5
    CLIPW       m3,         m6,    m7
%else
    psrad       m3,         INTERP_SHIFT_PS
    psrad       m5,         INTERP_SHIFT_PS
    packssdw    m3,         m5
%endif
    movh        [r2 + %2],       m3
    movhps      [r2 + %2 + 8],   m3
%endmacro

%macro FILTER_W48_1 1
    FILTER_W8o_1 %1, 0
    FILTER_W8o_1 %1, 16
    FILTER_W8o_1 %1, 32
    FILTER_W8o_1 %1, 48
    FILTER_W8o_1 %1, 64
    FILTER_W8o_1 %1, 80
%endmacro

cglobal chroma_filter_pp_48x1_internal
    FILTER_W48_1 pp
    ret

cglobal chroma_filter_ps_48x1_internal
    FILTER_W48_1 ps
    ret

%macro FILTER_W64_1 1
    FILTER_W8o_1 %1, 0
    FILTER_W8o_1 %1, 16
    FILTER_W8o_1 %1, 32
    FILTER_W8o_1 %1, 48
    FILTER_W8o_1 %1, 64
    FILTER_W8o_1 %1, 80
    FILTER_W8o_1 %1, 96
    FILTER_W8o_1 %1, 112
%endmacro

cglobal chroma_filter_pp_64x1_internal
    FILTER_W64_1 pp
    ret

cglobal chroma_filter_ps_64x1_internal
    FILTER_W64_1 ps
    ret


;-----------------------------------------------------------------------------
; void interp_4tap_horiz_%3_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------

INIT_XMM sse4
%macro IPFILTER_CHROMA 6
cglobal interp_4tap_horiz_%3_%1x%2, 4, %5, %6

    add         r3,        r3
    add         r1,        r1
    sub         r0,         2
    mov         r4d,        r4m
    add         r4d,        r4d

%ifdef PIC
    lea         r%4,       [tab_ChromaCoeff]
    movh        m0,       [r%4 + r4 * 4]
%else
    movh        m0,       [tab_ChromaCoeff + r4 * 4]
%endif

    punpcklqdq  m0,       m0
    mova        m2,       [tab_Tm16]

%ifidn %3, ps
    mova        m1,       [INTERP_OFFSET_PS]
    cmp         r5m, byte 0
    je          .skip
    sub         r0, r1
    call chroma_filter_%3_%1x1_internal
    add         r0, r1
    add         r2, r3
    call chroma_filter_%3_%1x1_internal
    add         r0, r1
    add         r2, r3
    call chroma_filter_%3_%1x1_internal
    add         r0, r1
    add         r2, r3
.skip:
%else
    mova        m1,         [tab_c_32]
    pxor        m6,         m6
    mova        m7,         [pw_pixel_max]
%endif

    call chroma_filter_%3_%1x1_internal
%rep %2 - 1
    add         r0,       r1
    add         r2,       r3
    call chroma_filter_%3_%1x1_internal
%endrep
RET
%endmacro
IPFILTER_CHROMA 6, 8, pp, 5, 6, 8
IPFILTER_CHROMA 8, 2, pp, 5, 6, 8
IPFILTER_CHROMA 8, 4, pp, 5, 6, 8
IPFILTER_CHROMA 8, 6, pp, 5, 6, 8
IPFILTER_CHROMA 8, 8, pp, 5, 6, 8
IPFILTER_CHROMA 8, 16, pp, 5, 6, 8
IPFILTER_CHROMA 8, 32, pp, 5, 6, 8
IPFILTER_CHROMA 12, 16, pp, 5, 6, 8
IPFILTER_CHROMA 16, 4, pp, 5, 6, 8
IPFILTER_CHROMA 16, 8, pp, 5, 6, 8
IPFILTER_CHROMA 16, 12, pp, 5, 6, 8
IPFILTER_CHROMA 16, 16, pp, 5, 6, 8
IPFILTER_CHROMA 16, 32, pp, 5, 6, 8
IPFILTER_CHROMA 24, 32, pp, 5, 6, 8
IPFILTER_CHROMA 32, 8, pp, 5, 6, 8
IPFILTER_CHROMA 32, 16, pp, 5, 6, 8
IPFILTER_CHROMA 32, 24, pp, 5, 6, 8
IPFILTER_CHROMA 32, 32, pp, 5, 6, 8

IPFILTER_CHROMA 6, 8, ps, 6, 7, 6
IPFILTER_CHROMA 8, 2, ps, 6, 7, 6
IPFILTER_CHROMA 8, 4, ps, 6, 7, 6
IPFILTER_CHROMA 8, 6, ps, 6, 7, 6
IPFILTER_CHROMA 8, 8, ps, 6, 7, 6
IPFILTER_CHROMA 8, 16, ps, 6, 7, 6
IPFILTER_CHROMA 8, 32, ps, 6, 7, 6
IPFILTER_CHROMA 12, 16, ps, 6, 7, 6
IPFILTER_CHROMA 16, 4, ps, 6, 7, 6
IPFILTER_CHROMA 16, 8, ps, 6, 7, 6
IPFILTER_CHROMA 16, 12, ps, 6, 7, 6
IPFILTER_CHROMA 16, 16, ps, 6, 7, 6
IPFILTER_CHROMA 16, 32, ps, 6, 7, 6
IPFILTER_CHROMA 24, 32, ps, 6, 7, 6
IPFILTER_CHROMA 32, 8, ps, 6, 7, 6
IPFILTER_CHROMA 32, 16, ps, 6, 7, 6
IPFILTER_CHROMA 32, 24, ps, 6, 7, 6
IPFILTER_CHROMA 32, 32, ps, 6, 7, 6

IPFILTER_CHROMA 6, 16, pp, 5, 6, 8
IPFILTER_CHROMA 8, 12, pp, 5, 6, 8
IPFILTER_CHROMA 8, 64, pp, 5, 6, 8
IPFILTER_CHROMA 12, 32, pp, 5, 6, 8
IPFILTER_CHROMA 16, 24, pp, 5, 6, 8
IPFILTER_CHROMA 16, 64, pp, 5, 6, 8
IPFILTER_CHROMA 24, 64, pp, 5, 6, 8
IPFILTER_CHROMA 32, 48, pp, 5, 6, 8
IPFILTER_CHROMA 32, 64, pp, 5, 6, 8
IPFILTER_CHROMA 6, 16, ps, 6, 7, 6
IPFILTER_CHROMA 8, 12, ps, 6, 7, 6
IPFILTER_CHROMA 8, 64, ps, 6, 7, 6
IPFILTER_CHROMA 12, 32, ps, 6, 7, 6
IPFILTER_CHROMA 16, 24, ps, 6, 7, 6
IPFILTER_CHROMA 16, 64, ps, 6, 7, 6
IPFILTER_CHROMA 24, 64, ps, 6, 7, 6
IPFILTER_CHROMA 32, 48, ps, 6, 7, 6
IPFILTER_CHROMA 32, 64, ps, 6, 7, 6

IPFILTER_CHROMA 48, 64, pp, 5, 6, 8
IPFILTER_CHROMA 64, 48, pp, 5, 6, 8
IPFILTER_CHROMA 64, 64, pp, 5, 6, 8
IPFILTER_CHROMA 64, 32, pp, 5, 6, 8
IPFILTER_CHROMA 64, 16, pp, 5, 6, 8
IPFILTER_CHROMA 48, 64, ps, 6, 7, 6
IPFILTER_CHROMA 64, 48, ps, 6, 7, 6
IPFILTER_CHROMA 64, 64, ps, 6, 7, 6
IPFILTER_CHROMA 64, 32, ps, 6, 7, 6
IPFILTER_CHROMA 64, 16, ps, 6, 7, 6


%macro PROCESS_CHROMA_SP_W4_4R 0
    movq       m0, [r0]
    movq       m1, [r0 + r1]
    punpcklwd  m0, m1                          ;m0=[0 1]
    pmaddwd    m0, [r6 + 0 *32]                ;m0=[0+1]         Row1

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m1, m4                          ;m1=[1 2]
    pmaddwd    m1, [r6 + 0 *32]                ;m1=[1+2]         Row2

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[2 3]
    pmaddwd    m2, m4, [r6 + 0 *32]            ;m2=[2+3]         Row3
    pmaddwd    m4, [r6 + 1 * 32]
    paddd      m0, m4                          ;m0=[0+1+2+3]     Row1 done

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m5, m4                          ;m5=[3 4]
    pmaddwd    m3, m5, [r6 + 0 *32]            ;m3=[3+4]         Row4
    pmaddwd    m5, [r6 + 1 * 32]
    paddd      m1, m5                          ;m1 = [1+2+3+4]   Row2

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[4 5]
    pmaddwd    m4, [r6 + 1 * 32]
    paddd      m2, m4                          ;m2=[2+3+4+5]     Row3

    movq       m4, [r0 + 2 * r1]
    punpcklwd  m5, m4                          ;m5=[5 6]
    pmaddwd    m5, [r6 + 1 * 32]
    paddd      m3, m5                          ;m3=[3+4+5+6]     Row4
%endmacro

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
%macro IPFILTER_CHROMA_avx2_6xN 1
cglobal interp_4tap_horiz_pp_6x%1, 5,6,8
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    mov             r4d, %1/2
.loop:
    vbroadcasti128  m3, [r0]
    vbroadcasti128  m4, [r0 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, INTERP_SHIFT_PP           ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6                      ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movq            [r2], xm3
    pextrd          [r2 + 8], xm3, 2

    vbroadcasti128  m3, [r0 + r1]
    vbroadcasti128  m4, [r0 + r1 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, INTERP_SHIFT_PP           ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6                      ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movq            [r2 + r3], xm3
    pextrd          [r2 + r3 + 8], xm3, 2

    lea             r0, [r0 + r1 * 2]
    lea             r2, [r2 + r3 * 2]
    dec             r4d
    jnz             .loop
    RET
%endmacro
IPFILTER_CHROMA_avx2_6xN 8
IPFILTER_CHROMA_avx2_6xN 16

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal interp_4tap_horiz_pp_8x2, 5,6,8
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    vbroadcasti128  m3, [r0]
    vbroadcasti128  m4, [r0 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, INTERP_SHIFT_PP          ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3,q2020
    pshufb          xm3, xm6                     ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movu            [r2], xm3

    vbroadcasti128  m3, [r0 + r1]
    vbroadcasti128  m4, [r0 + r1 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, INTERP_SHIFT_PP           ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3,q2020
    pshufb          xm3, xm6                      ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movu            [r2 + r3], xm3
    RET

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal interp_4tap_horiz_pp_8x4, 5,6,8
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

%rep 2
    vbroadcasti128  m3, [r0]
    vbroadcasti128  m4, [r0 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6                       ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3,q2020
    pshufb          xm3, xm6                    ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movu            [r2], xm3

    vbroadcasti128  m3, [r0 + r1]
    vbroadcasti128  m4, [r0 + r1 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6                       ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3,q2020
    pshufb          xm3, xm6                    ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movu            [r2 + r3], xm3

    lea             r0, [r0 + r1 * 2]
    lea             r2, [r2 + r3 * 2]
%endrep
    RET

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
%macro IPFILTER_CHROMA_avx2_8xN 1
cglobal interp_4tap_horiz_pp_8x%1, 5,6,8
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    mov             r4d, %1/2
.loop:
    vbroadcasti128  m3, [r0]
    vbroadcasti128  m4, [r0 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6                         ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6                      ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movu            [r2], xm3

    vbroadcasti128  m3, [r0 + r1]
    vbroadcasti128  m4, [r0 + r1 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6                         ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6                      ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movu            [r2 + r3], xm3

    lea             r0, [r0 + r1 * 2]
    lea             r2, [r2 + r3 * 2]
    dec             r4d
    jnz             .loop
    RET
%endmacro
IPFILTER_CHROMA_avx2_8xN 6
IPFILTER_CHROMA_avx2_8xN 8
IPFILTER_CHROMA_avx2_8xN 12
IPFILTER_CHROMA_avx2_8xN 16
IPFILTER_CHROMA_avx2_8xN 32
IPFILTER_CHROMA_avx2_8xN 64

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
%macro IPFILTER_CHROMA_avx2_16xN 1
%if ARCH_X86_64
cglobal interp_4tap_horiz_pp_16x%1, 5,6,9
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    mov             r4d, %1
.loop:
    vbroadcasti128  m3, [r0]
    vbroadcasti128  m4, [r0 + 8]

    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6                       ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6                     ; m3 = WORD[7 6 5 4 3 2 1 0]

    vbroadcasti128  m4, [r0 + 16]
    vbroadcasti128  m8, [r0 + 24]

    pshufb          m4, m1
    pshufb          m8, m1

    pmaddwd         m4, m0
    pmaddwd         m8, m0
    phaddd          m4, m8
    paddd           m4, m2
    psrad           m4, 6                       ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m4, m4
    vpermq          m4, m4, q2020
    pshufb          xm4, xm6                    ; m3 = WORD[7 6 5 4 3 2 1 0]
    vinserti128     m3, m3, xm4, 1
    CLIPW           m3, m5, m7
    movu            [r2], m3

    add             r0, r1
    add             r2, r3
    dec             r4d
    jnz             .loop
    RET
%endif
%endmacro
IPFILTER_CHROMA_avx2_16xN 4
IPFILTER_CHROMA_avx2_16xN 8
IPFILTER_CHROMA_avx2_16xN 12
IPFILTER_CHROMA_avx2_16xN 16
IPFILTER_CHROMA_avx2_16xN 24
IPFILTER_CHROMA_avx2_16xN 32
IPFILTER_CHROMA_avx2_16xN 64

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
%macro IPFILTER_CHROMA_avx2_32xN 1
%if ARCH_X86_64
cglobal interp_4tap_horiz_pp_32x%1, 5,6,9
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    mov             r6d, %1
.loop:
%assign x 0
%rep 2
    vbroadcasti128  m3, [r0 + x]
    vbroadcasti128  m4, [r0 + 8 + x]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6                       ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6                     ; m3 = WORD[7 6 5 4 3 2 1 0]

    vbroadcasti128  m4, [r0 + 16 + x]
    vbroadcasti128  m8, [r0 + 24 + x]
    pshufb          m4, m1
    pshufb          m8, m1

    pmaddwd         m4, m0
    pmaddwd         m8, m0
    phaddd          m4, m8
    paddd           m4, m2
    psrad           m4, 6                       ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m4, m4
    vpermq          m4, m4, q2020
    pshufb          xm4, xm6                    ; m3 = WORD[7 6 5 4 3 2 1 0]
    vinserti128     m3, m3, xm4, 1
    CLIPW           m3, m5, m7
    movu            [r2 + x], m3
    %assign x x+32
    %endrep

    add             r0, r1
    add             r2, r3
    dec             r6d
    jnz             .loop
    RET
%endif
%endmacro
IPFILTER_CHROMA_avx2_32xN 8
IPFILTER_CHROMA_avx2_32xN 16
IPFILTER_CHROMA_avx2_32xN 24
IPFILTER_CHROMA_avx2_32xN 32
IPFILTER_CHROMA_avx2_32xN 48
IPFILTER_CHROMA_avx2_32xN 64

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
%macro IPFILTER_CHROMA_avx2_12xN 1
%if ARCH_X86_64
cglobal interp_4tap_horiz_pp_12x%1, 5,6,8
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    mov             r4d, %1
.loop:
    vbroadcasti128  m3, [r0]
    vbroadcasti128  m4, [r0 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6                       ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6                     ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movu            [r2], xm3

    vbroadcasti128  m3, [r0 + 16]
    vbroadcasti128  m4, [r0 + 24]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6                       ; m3 = DWORD[7 6 3 2 5 4 1 0]

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6                    ; m3 = WORD[7 6 5 4 3 2 1 0]
    CLIPW           xm3, xm5, xm7
    movq            [r2 + 16], xm3

    add             r0, r1
    add             r2, r3
    dec             r4d
    jnz             .loop
    RET
%endif
%endmacro
IPFILTER_CHROMA_avx2_12xN 16
IPFILTER_CHROMA_avx2_12xN 32

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
%macro IPFILTER_CHROMA_avx2_24xN 1
%if ARCH_X86_64
cglobal interp_4tap_horiz_pp_24x%1, 5,6,9
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    mov             r4d, %1
.loop:
    vbroadcasti128  m3, [r0]
    vbroadcasti128  m4, [r0 + 8]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6

    vbroadcasti128  m4, [r0 + 16]
    vbroadcasti128  m8, [r0 + 24]
    pshufb          m4, m1
    pshufb          m8, m1

    pmaddwd         m4, m0
    pmaddwd         m8, m0
    phaddd          m4, m8
    paddd           m4, m2
    psrad           m4, 6

    packusdw        m3, m4
    vpermq          m3, m3, q3120
    pshufb          m3, m6
    CLIPW           m3, m5, m7
    movu            [r2], m3

    vbroadcasti128  m3, [r0 + 32]
    vbroadcasti128  m4, [r0 + 40]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6

    packusdw        m3, m3
    vpermq          m3, m3, q2020
    pshufb          xm3, xm6
    CLIPW           xm3, xm5, xm7
    movu            [r2 + 32], xm3

    add             r0, r1
    add             r2, r3
    dec             r4d
    jnz             .loop
    RET
%endif
%endmacro
IPFILTER_CHROMA_avx2_24xN 32
IPFILTER_CHROMA_avx2_24xN 64

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
%macro IPFILTER_CHROMA_avx2_64xN 1
%if ARCH_X86_64
cglobal interp_4tap_horiz_pp_64x%1, 5,6,9
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    mov             r6d, %1
.loop:
%assign x 0
%rep 4
    vbroadcasti128  m3, [r0 + x]
    vbroadcasti128  m4, [r0 + 8 + x]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6

    vbroadcasti128  m4, [r0 + 16 + x]
    vbroadcasti128  m8, [r0 + 24 + x]
    pshufb          m4, m1
    pshufb          m8, m1

    pmaddwd         m4, m0
    pmaddwd         m8, m0
    phaddd          m4, m8
    paddd           m4, m2
    psrad           m4, 6

    packusdw        m3, m4
    vpermq          m3, m3, q3120
    pshufb          m3, m6
    CLIPW           m3, m5, m7
    movu            [r2 + x], m3
    %assign x x+32
    %endrep

    add             r0, r1
    add             r2, r3
    dec             r6d
    jnz             .loop
    RET
%endif
%endmacro
IPFILTER_CHROMA_avx2_64xN 16
IPFILTER_CHROMA_avx2_64xN 32
IPFILTER_CHROMA_avx2_64xN 48
IPFILTER_CHROMA_avx2_64xN 64

;-------------------------------------------------------------------------------------------------------------
; void interp_4tap_horiz_pp(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx
;-------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
%if ARCH_X86_64
cglobal interp_4tap_horiz_pp_48x64, 5,6,9
    add             r1d, r1d
    add             r3d, r3d
    sub             r0, 2
    mov             r4d, r4m
%ifdef PIC
    lea             r5, [tab_ChromaCoeff]
    vpbroadcastq    m0, [r5 + r4 * 8]
%else
    vpbroadcastq    m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova            m1, [interp8_hpp_shuf]
    vpbroadcastd    m2, [pd_32]
    pxor            m5, m5
    mova            m6, [idct8_shuf2]
    mova            m7, [pw_pixel_max]

    mov             r4d, 64
.loop:
%assign x 0
%rep 3
    vbroadcasti128  m3, [r0 + x]
    vbroadcasti128  m4, [r0 + 8 + x]
    pshufb          m3, m1
    pshufb          m4, m1

    pmaddwd         m3, m0
    pmaddwd         m4, m0
    phaddd          m3, m4
    paddd           m3, m2
    psrad           m3, 6

    vbroadcasti128  m4, [r0 + 16 + x]
    vbroadcasti128  m8, [r0 + 24 + x]
    pshufb          m4, m1
    pshufb          m8, m1

    pmaddwd         m4, m0
    pmaddwd         m8, m0
    phaddd          m4, m8
    paddd           m4, m2
    psrad           m4, 6

    packusdw        m3, m4
    vpermq          m3, m3, q3120
    pshufb          m3, m6
    CLIPW           m3, m5, m7
    movu            [r2 + x], m3
%assign x x+32
%endrep

    add             r0, r1
    add             r2, r3
    dec             r4d
    jnz             .loop
    RET
%endif

;-----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert_%3_%1x%2(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_SS 4
INIT_XMM sse2
cglobal interp_4tap_vert_%3_%1x%2, 5, 7, %4 ,0-gprsize

    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r6, [r5 + r4]
%else
    lea       r6, [tab_ChromaCoeffV + r4]
%endif

    mov       dword [rsp], %2/4

%ifnidn %3, ss
    %ifnidn %3, ps
        mova      m7, [pw_pixel_max]
        %ifidn %3, pp
            mova      m6, [INTERP_OFFSET_PP]
        %else
            mova      m6, [INTERP_OFFSET_SP]
        %endif
    %else
        mova      m6, [INTERP_OFFSET_PS]
    %endif
%endif

.loopH:
    mov       r4d, (%1/4)
.loopW:
    PROCESS_CHROMA_SP_W4_4R

%ifidn %3, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    packssdw  m0, m1
    packssdw  m2, m3
%elifidn %3, ps
    paddd     m0, m6
    paddd     m1, m6
    paddd     m2, m6
    paddd     m3, m6
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
%else
    paddd     m0, m6
    paddd     m1, m6
    paddd     m2, m6
    paddd     m3, m6
    %ifidn %3, pp
        psrad     m0, INTERP_SHIFT_PP
        psrad     m1, INTERP_SHIFT_PP
        psrad     m2, INTERP_SHIFT_PP
        psrad     m3, INTERP_SHIFT_PP
    %else
        psrad     m0, INTERP_SHIFT_SP
        psrad     m1, INTERP_SHIFT_SP
        psrad     m2, INTERP_SHIFT_SP
        psrad     m3, INTERP_SHIFT_SP
    %endif
    packssdw  m0, m1
    packssdw  m2, m3
    pxor      m5, m5
    CLIPW2    m0, m2, m5, m7
%endif

    movh      [r2], m0
    movhps    [r2 + r3], m0
    lea       r5, [r2 + 2 * r3]
    movh      [r5], m2
    movhps    [r5 + r3], m2

    lea       r5, [4 * r1 - 2 * 4]
    sub       r0, r5
    add       r2, 2 * 4

    dec       r4d
    jnz       .loopW

    lea       r0, [r0 + 4 * r1 - 2 * %1]
    lea       r2, [r2 + 4 * r3 - 2 * %1]

    dec       dword [rsp]
    jnz       .loopH

    RET
%endmacro

    FILTER_VER_CHROMA_SS 4, 4, ss, 6
    FILTER_VER_CHROMA_SS 4, 8, ss, 6
    FILTER_VER_CHROMA_SS 16, 16, ss, 6
    FILTER_VER_CHROMA_SS 16, 8, ss, 6
    FILTER_VER_CHROMA_SS 16, 12, ss, 6
    FILTER_VER_CHROMA_SS 12, 16, ss, 6
    FILTER_VER_CHROMA_SS 16, 4, ss, 6
    FILTER_VER_CHROMA_SS 4, 16, ss, 6
    FILTER_VER_CHROMA_SS 32, 32, ss, 6
    FILTER_VER_CHROMA_SS 32, 16, ss, 6
    FILTER_VER_CHROMA_SS 16, 32, ss, 6
    FILTER_VER_CHROMA_SS 32, 24, ss, 6
    FILTER_VER_CHROMA_SS 24, 32, ss, 6
    FILTER_VER_CHROMA_SS 32, 8, ss, 6

    FILTER_VER_CHROMA_SS 4, 4, ps, 7
    FILTER_VER_CHROMA_SS 4, 8, ps, 7
    FILTER_VER_CHROMA_SS 16, 16, ps, 7
    FILTER_VER_CHROMA_SS 16, 8, ps, 7
    FILTER_VER_CHROMA_SS 16, 12, ps, 7
    FILTER_VER_CHROMA_SS 12, 16, ps, 7
    FILTER_VER_CHROMA_SS 16, 4, ps, 7
    FILTER_VER_CHROMA_SS 4, 16, ps, 7
    FILTER_VER_CHROMA_SS 32, 32, ps, 7
    FILTER_VER_CHROMA_SS 32, 16, ps, 7
    FILTER_VER_CHROMA_SS 16, 32, ps, 7
    FILTER_VER_CHROMA_SS 32, 24, ps, 7
    FILTER_VER_CHROMA_SS 24, 32, ps, 7
    FILTER_VER_CHROMA_SS 32, 8, ps, 7

    FILTER_VER_CHROMA_SS 4, 4, sp, 8
    FILTER_VER_CHROMA_SS 4, 8, sp, 8
    FILTER_VER_CHROMA_SS 16, 16, sp, 8
    FILTER_VER_CHROMA_SS 16, 8, sp, 8
    FILTER_VER_CHROMA_SS 16, 12, sp, 8
    FILTER_VER_CHROMA_SS 12, 16, sp, 8
    FILTER_VER_CHROMA_SS 16, 4, sp, 8
    FILTER_VER_CHROMA_SS 4, 16, sp, 8
    FILTER_VER_CHROMA_SS 32, 32, sp, 8
    FILTER_VER_CHROMA_SS 32, 16, sp, 8
    FILTER_VER_CHROMA_SS 16, 32, sp, 8
    FILTER_VER_CHROMA_SS 32, 24, sp, 8
    FILTER_VER_CHROMA_SS 24, 32, sp, 8
    FILTER_VER_CHROMA_SS 32, 8, sp, 8

    FILTER_VER_CHROMA_SS 4, 4, pp, 8
    FILTER_VER_CHROMA_SS 4, 8, pp, 8
    FILTER_VER_CHROMA_SS 16, 16, pp, 8
    FILTER_VER_CHROMA_SS 16, 8, pp, 8
    FILTER_VER_CHROMA_SS 16, 12, pp, 8
    FILTER_VER_CHROMA_SS 12, 16, pp, 8
    FILTER_VER_CHROMA_SS 16, 4, pp, 8
    FILTER_VER_CHROMA_SS 4, 16, pp, 8
    FILTER_VER_CHROMA_SS 32, 32, pp, 8
    FILTER_VER_CHROMA_SS 32, 16, pp, 8
    FILTER_VER_CHROMA_SS 16, 32, pp, 8
    FILTER_VER_CHROMA_SS 32, 24, pp, 8
    FILTER_VER_CHROMA_SS 24, 32, pp, 8
    FILTER_VER_CHROMA_SS 32, 8, pp, 8


    FILTER_VER_CHROMA_SS 16, 24, ss, 6
    FILTER_VER_CHROMA_SS 12, 32, ss, 6
    FILTER_VER_CHROMA_SS 4, 32, ss, 6
    FILTER_VER_CHROMA_SS 32, 64, ss, 6
    FILTER_VER_CHROMA_SS 16, 64, ss, 6
    FILTER_VER_CHROMA_SS 32, 48, ss, 6
    FILTER_VER_CHROMA_SS 24, 64, ss, 6

    FILTER_VER_CHROMA_SS 16, 24, ps, 7
    FILTER_VER_CHROMA_SS 12, 32, ps, 7
    FILTER_VER_CHROMA_SS 4, 32, ps, 7
    FILTER_VER_CHROMA_SS 32, 64, ps, 7
    FILTER_VER_CHROMA_SS 16, 64, ps, 7
    FILTER_VER_CHROMA_SS 32, 48, ps, 7
    FILTER_VER_CHROMA_SS 24, 64, ps, 7

    FILTER_VER_CHROMA_SS 16, 24, sp, 8
    FILTER_VER_CHROMA_SS 12, 32, sp, 8
    FILTER_VER_CHROMA_SS 4, 32, sp, 8
    FILTER_VER_CHROMA_SS 32, 64, sp, 8
    FILTER_VER_CHROMA_SS 16, 64, sp, 8
    FILTER_VER_CHROMA_SS 32, 48, sp, 8
    FILTER_VER_CHROMA_SS 24, 64, sp, 8

    FILTER_VER_CHROMA_SS 16, 24, pp, 8
    FILTER_VER_CHROMA_SS 12, 32, pp, 8
    FILTER_VER_CHROMA_SS 4, 32, pp, 8
    FILTER_VER_CHROMA_SS 32, 64, pp, 8
    FILTER_VER_CHROMA_SS 16, 64, pp, 8
    FILTER_VER_CHROMA_SS 32, 48, pp, 8
    FILTER_VER_CHROMA_SS 24, 64, pp, 8


    FILTER_VER_CHROMA_SS 48, 64, ss, 6
    FILTER_VER_CHROMA_SS 64, 48, ss, 6
    FILTER_VER_CHROMA_SS 64, 64, ss, 6
    FILTER_VER_CHROMA_SS 64, 32, ss, 6
    FILTER_VER_CHROMA_SS 64, 16, ss, 6

    FILTER_VER_CHROMA_SS 48, 64, ps, 7
    FILTER_VER_CHROMA_SS 64, 48, ps, 7
    FILTER_VER_CHROMA_SS 64, 64, ps, 7
    FILTER_VER_CHROMA_SS 64, 32, ps, 7
    FILTER_VER_CHROMA_SS 64, 16, ps, 7

    FILTER_VER_CHROMA_SS 48, 64, sp, 8
    FILTER_VER_CHROMA_SS 64, 48, sp, 8
    FILTER_VER_CHROMA_SS 64, 64, sp, 8
    FILTER_VER_CHROMA_SS 64, 32, sp, 8
    FILTER_VER_CHROMA_SS 64, 16, sp, 8

    FILTER_VER_CHROMA_SS 48, 64, pp, 8
    FILTER_VER_CHROMA_SS 64, 48, pp, 8
    FILTER_VER_CHROMA_SS 64, 64, pp, 8
    FILTER_VER_CHROMA_SS 64, 32, pp, 8
    FILTER_VER_CHROMA_SS 64, 16, pp, 8


%macro PROCESS_CHROMA_SP_W2_4R 1
    movd       m0, [r0]
    movd       m1, [r0 + r1]
    punpcklwd  m0, m1                          ;m0=[0 1]

    lea        r0, [r0 + 2 * r1]
    movd       m2, [r0]
    punpcklwd  m1, m2                          ;m1=[1 2]
    punpcklqdq m0, m1                          ;m0=[0 1 1 2]
    pmaddwd    m0, [%1 + 0 *32]                ;m0=[0+1 1+2] Row 1-2

    movd       m1, [r0 + r1]
    punpcklwd  m2, m1                          ;m2=[2 3]

    lea        r0, [r0 + 2 * r1]
    movd       m3, [r0]
    punpcklwd  m1, m3                          ;m2=[3 4]
    punpcklqdq m2, m1                          ;m2=[2 3 3 4]

    pmaddwd    m4, m2, [%1 + 1 * 32]           ;m4=[2+3 3+4] Row 1-2
    pmaddwd    m2, [%1 + 0 * 32]               ;m2=[2+3 3+4] Row 3-4
    paddd      m0, m4                          ;m0=[0+1+2+3 1+2+3+4] Row 1-2

    movd       m1, [r0 + r1]
    punpcklwd  m3, m1                          ;m3=[4 5]

    movd       m4, [r0 + 2 * r1]
    punpcklwd  m1, m4                          ;m1=[5 6]
    punpcklqdq m3, m1                          ;m2=[4 5 5 6]
    pmaddwd    m3, [%1 + 1 * 32]               ;m3=[4+5 5+6] Row 3-4
    paddd      m2, m3                          ;m2=[2+3+4+5 3+4+5+6] Row 3-4
%endmacro

;---------------------------------------------------------------------------------------------------------------------
; void interp_4tap_vertical_%2_2x%1(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;---------------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W2 3
INIT_XMM sse4
cglobal interp_4tap_vert_%2_2x%1, 5, 6, %3

    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r5, [r5 + r4]
%else
    lea       r5, [tab_ChromaCoeffV + r4]
%endif

    mov       r4d, (%1/4)
%ifnidn %2, ss
    %ifnidn %2, ps
        pxor      m7, m7
        mova      m6, [pw_pixel_max]
        %ifidn %2, pp
            mova      m5, [INTERP_OFFSET_PP]
        %else
            mova      m5, [INTERP_OFFSET_SP]
        %endif
    %else
        mova      m5, [INTERP_OFFSET_PS]
    %endif
%endif

.loopH:
    PROCESS_CHROMA_SP_W2_4R r5
%ifidn %2, ss
    psrad     m0, 6
    psrad     m2, 6
    packssdw  m0, m2
%elifidn %2, ps
    paddd     m0, m5
    paddd     m2, m5
    psrad     m0, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    packssdw  m0, m2
%else
    paddd     m0, m5
    paddd     m2, m5
    %ifidn %2, pp
        psrad     m0, INTERP_SHIFT_PP
        psrad     m2, INTERP_SHIFT_PP
    %else
        psrad     m0, INTERP_SHIFT_SP
        psrad     m2, INTERP_SHIFT_SP
    %endif
    packusdw  m0, m2
    CLIPW     m0, m7,    m6
%endif

    movd      [r2], m0
    pextrd    [r2 + r3], m0, 1
    lea       r2, [r2 + 2 * r3]
    pextrd    [r2], m0, 2
    pextrd    [r2 + r3], m0, 3

    lea       r2, [r2 + 2 * r3]

    dec       r4d
    jnz       .loopH
    RET
%endmacro

FILTER_VER_CHROMA_W2 4, ss, 5
FILTER_VER_CHROMA_W2 8, ss, 5

FILTER_VER_CHROMA_W2 4, pp, 8
FILTER_VER_CHROMA_W2 8, pp, 8

FILTER_VER_CHROMA_W2 4, ps, 6
FILTER_VER_CHROMA_W2 8, ps, 6

FILTER_VER_CHROMA_W2 4, sp, 8
FILTER_VER_CHROMA_W2 8, sp, 8

FILTER_VER_CHROMA_W2 16, ss, 5
FILTER_VER_CHROMA_W2 16, pp, 8
FILTER_VER_CHROMA_W2 16, ps, 6
FILTER_VER_CHROMA_W2 16, sp, 8


;---------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert_%1_4x2(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;---------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W4 3
INIT_XMM sse4
cglobal interp_4tap_vert_%2_4x%1, 5, 6, %3
    add        r1d, r1d
    add        r3d, r3d
    sub        r0, r1
    shl        r4d, 6

%ifdef PIC
    lea        r5, [tab_ChromaCoeffV]
    lea        r5, [r5 + r4]
%else
    lea        r5, [tab_ChromaCoeffV + r4]
%endif

%ifnidn %2, 2
    mov        r4d, %1/2
%endif

%ifnidn %2, ss
    %ifnidn %2, ps
        pxor      m6, m6
        mova      m5, [pw_pixel_max]
        %ifidn %2, pp
            mova      m4, [INTERP_OFFSET_PP]
        %else
            mova      m4, [INTERP_OFFSET_SP]
        %endif
    %else
        mova      m4, [INTERP_OFFSET_PS]
    %endif
%endif

%ifnidn %2, 2
.loop:
%endif

    movh       m0, [r0]
    movh       m1, [r0 + r1]
    punpcklwd  m0, m1                          ;m0=[0 1]
    pmaddwd    m0, [r5 + 0 *32]                ;m0=[0+1]  Row1

    lea        r0, [r0 + 2 * r1]
    movh       m2, [r0]
    punpcklwd  m1, m2                          ;m1=[1 2]
    pmaddwd    m1, [r5 + 0 *32]                ;m1=[1+2]  Row2

    movh       m3, [r0 + r1]
    punpcklwd  m2, m3                          ;m4=[2 3]
    pmaddwd    m2, [r5 + 1 * 32]
    paddd      m0, m2                          ;m0=[0+1+2+3]  Row1 done

    movh       m2, [r0 + 2 * r1]
    punpcklwd  m3, m2                          ;m5=[3 4]
    pmaddwd    m3, [r5 + 1 * 32]
    paddd      m1, m3                          ;m1=[1+2+3+4]  Row2 done

%ifidn %2, ss
    psrad     m0, 6
    psrad     m1, 6
    packssdw  m0, m1
%elifidn %2, ps
    paddd     m0, m4
    paddd     m1, m4
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    packssdw  m0, m1
%else
    paddd     m0, m4
    paddd     m1, m4
    %ifidn %2, pp
        psrad     m0, INTERP_SHIFT_PP
        psrad     m1, INTERP_SHIFT_PP
    %else
        psrad     m0, INTERP_SHIFT_SP
        psrad     m1, INTERP_SHIFT_SP
    %endif
    packusdw  m0, m1
    CLIPW     m0, m6,    m5
%endif

    movh       [r2], m0
    movhps     [r2 + r3], m0

%ifnidn %2, 2
    lea        r2, [r2 + r3 * 2]
    dec        r4d
    jnz        .loop
%endif
    RET
%endmacro

FILTER_VER_CHROMA_W4 2, ss, 4
FILTER_VER_CHROMA_W4 2, pp, 7
FILTER_VER_CHROMA_W4 2, ps, 5
FILTER_VER_CHROMA_W4 2, sp, 7

FILTER_VER_CHROMA_W4 4, ss, 4
FILTER_VER_CHROMA_W4 4, pp, 7
FILTER_VER_CHROMA_W4 4, ps, 5
FILTER_VER_CHROMA_W4 4, sp, 7

;-------------------------------------------------------------------------------------------------------------------
; void interp_4tap_vertical_%1_6x8(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-------------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W6 3
INIT_XMM sse4
cglobal interp_4tap_vert_%2_6x%1, 5, 7, %3
    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r6, [r5 + r4]
%else
    lea       r6, [tab_ChromaCoeffV + r4]
%endif

    mov       r4d, %1/4

%ifnidn %2, ss
    %ifnidn %2, ps
        mova      m7, [pw_pixel_max]
        %ifidn %2, pp
            mova      m6, [INTERP_OFFSET_PP]
        %else
            mova      m6, [INTERP_OFFSET_SP]
        %endif
    %else
        mova      m6, [INTERP_OFFSET_PS]
    %endif
%endif

.loopH:
    PROCESS_CHROMA_SP_W4_4R

%ifidn %2, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    packssdw  m0, m1
    packssdw  m2, m3
%elifidn %2, ps
    paddd     m0, m6
    paddd     m1, m6
    paddd     m2, m6
    paddd     m3, m6
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
%else
    paddd     m0, m6
    paddd     m1, m6
    paddd     m2, m6
    paddd     m3, m6
    %ifidn %2, pp
        psrad     m0, INTERP_SHIFT_PP
        psrad     m1, INTERP_SHIFT_PP
        psrad     m2, INTERP_SHIFT_PP
        psrad     m3, INTERP_SHIFT_PP
    %else
        psrad     m0, INTERP_SHIFT_SP
        psrad     m1, INTERP_SHIFT_SP
        psrad     m2, INTERP_SHIFT_SP
        psrad     m3, INTERP_SHIFT_SP
    %endif
    packssdw  m0, m1
    packssdw  m2, m3
    pxor      m5, m5
    CLIPW2    m0, m2, m5, m7
%endif

    movh      [r2], m0
    movhps    [r2 + r3], m0
    lea       r5, [r2 + 2 * r3]
    movh      [r5], m2
    movhps    [r5 + r3], m2

    lea       r5, [4 * r1 - 2 * 4]
    sub       r0, r5
    add       r2, 2 * 4

    PROCESS_CHROMA_SP_W2_4R r6

%ifidn %2, ss
    psrad     m0, 6
    psrad     m2, 6
    packssdw  m0, m2
%elifidn %2, ps
    paddd     m0, m6
    paddd     m2, m6
    psrad     m0, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    packssdw  m0, m2
%else
    paddd     m0, m6
    paddd     m2, m6
    %ifidn %2, pp
        psrad     m0, INTERP_SHIFT_PP
        psrad     m2, INTERP_SHIFT_PP
    %else
        psrad     m0, INTERP_SHIFT_SP
        psrad     m2, INTERP_SHIFT_SP
    %endif
    packusdw  m0, m2
    CLIPW     m0, m5,    m7
%endif

    movd      [r2], m0
    pextrd    [r2 + r3], m0, 1
    lea       r2, [r2 + 2 * r3]
    pextrd    [r2], m0, 2
    pextrd    [r2 + r3], m0, 3

    sub       r0, 2 * 4
    lea       r2, [r2 + 2 * r3 - 2 * 4]

    dec       r4d
    jnz       .loopH
    RET
%endmacro

FILTER_VER_CHROMA_W6 8, ss, 6
FILTER_VER_CHROMA_W6 8, ps, 7
FILTER_VER_CHROMA_W6 8, sp, 8
FILTER_VER_CHROMA_W6 8, pp, 8

FILTER_VER_CHROMA_W6 16, ss, 6
FILTER_VER_CHROMA_W6 16, ps, 7
FILTER_VER_CHROMA_W6 16, sp, 8
FILTER_VER_CHROMA_W6 16, pp, 8

%macro PROCESS_CHROMA_SP_W8_2R 0
    movu       m1, [r0]
    movu       m3, [r0 + r1]
    punpcklwd  m0, m1, m3
    pmaddwd    m0, [r5 + 0 * 32]                ;m0 = [0l+1l]  Row1l
    punpckhwd  m1, m3
    pmaddwd    m1, [r5 + 0 * 32]                ;m1 = [0h+1h]  Row1h

    movu       m4, [r0 + 2 * r1]
    punpcklwd  m2, m3, m4
    pmaddwd    m2, [r5 + 0 * 32]                ;m2 = [1l+2l]  Row2l
    punpckhwd  m3, m4
    pmaddwd    m3, [r5 + 0 * 32]                ;m3 = [1h+2h]  Row2h

    lea        r0, [r0 + 2 * r1]
    movu       m5, [r0 + r1]
    punpcklwd  m6, m4, m5
    pmaddwd    m6, [r5 + 1 * 32]                ;m6 = [2l+3l]  Row1l
    paddd      m0, m6                           ;m0 = [0l+1l+2l+3l]  Row1l sum
    punpckhwd  m4, m5
    pmaddwd    m4, [r5 + 1 * 32]                ;m6 = [2h+3h]  Row1h
    paddd      m1, m4                           ;m1 = [0h+1h+2h+3h]  Row1h sum

    movu       m4, [r0 + 2 * r1]
    punpcklwd  m6, m5, m4
    pmaddwd    m6, [r5 + 1 * 32]                ;m6 = [3l+4l]  Row2l
    paddd      m2, m6                           ;m2 = [1l+2l+3l+4l]  Row2l sum
    punpckhwd  m5, m4
    pmaddwd    m5, [r5 + 1 * 32]                ;m1 = [3h+4h]  Row2h
    paddd      m3, m5                           ;m3 = [1h+2h+3h+4h]  Row2h sum
%endmacro

;----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert_%3_%1x%2(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W8 4
INIT_XMM sse2
cglobal interp_4tap_vert_%3_%1x%2, 5, 6, %4

    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r5, [r5 + r4]
%else
    lea       r5, [tab_ChromaCoeffV + r4]
%endif

    mov       r4d, %2/2

%ifidn %3, pp
    mova      m7, [INTERP_OFFSET_PP]
%elifidn %3, sp
    mova      m7, [INTERP_OFFSET_SP]
%elifidn %3, ps
    mova      m7, [INTERP_OFFSET_PS]
%endif

.loopH:
    PROCESS_CHROMA_SP_W8_2R

%ifidn %3, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    packssdw  m0, m1
    packssdw  m2, m3
%elifidn %3, ps
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
%else
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    %ifidn %3, pp
        psrad     m0, INTERP_SHIFT_PP
        psrad     m1, INTERP_SHIFT_PP
        psrad     m2, INTERP_SHIFT_PP
        psrad     m3, INTERP_SHIFT_PP
    %else
        psrad     m0, INTERP_SHIFT_SP
        psrad     m1, INTERP_SHIFT_SP
        psrad     m2, INTERP_SHIFT_SP
        psrad     m3, INTERP_SHIFT_SP
    %endif
    packssdw  m0, m1
    packssdw  m2, m3
    pxor      m5, m5
    mova      m6, [pw_pixel_max]
    CLIPW2    m0, m2, m5, m6
%endif

    movu      [r2], m0
    movu      [r2 + r3], m2

    lea       r2, [r2 + 2 * r3]

    dec       r4d
    jnz       .loopH
    RET
%endmacro

FILTER_VER_CHROMA_W8 8, 2, ss, 7
FILTER_VER_CHROMA_W8 8, 4, ss, 7
FILTER_VER_CHROMA_W8 8, 6, ss, 7
FILTER_VER_CHROMA_W8 8, 8, ss, 7
FILTER_VER_CHROMA_W8 8, 16, ss, 7
FILTER_VER_CHROMA_W8 8, 32, ss, 7

FILTER_VER_CHROMA_W8 8, 2, sp, 8
FILTER_VER_CHROMA_W8 8, 4, sp, 8
FILTER_VER_CHROMA_W8 8, 6, sp, 8
FILTER_VER_CHROMA_W8 8, 8, sp, 8
FILTER_VER_CHROMA_W8 8, 16, sp, 8
FILTER_VER_CHROMA_W8 8, 32, sp, 8

FILTER_VER_CHROMA_W8 8, 2, ps, 8
FILTER_VER_CHROMA_W8 8, 4, ps, 8
FILTER_VER_CHROMA_W8 8, 6, ps, 8
FILTER_VER_CHROMA_W8 8, 8, ps, 8
FILTER_VER_CHROMA_W8 8, 16, ps, 8
FILTER_VER_CHROMA_W8 8, 32, ps, 8

FILTER_VER_CHROMA_W8 8, 2, pp, 8
FILTER_VER_CHROMA_W8 8, 4, pp, 8
FILTER_VER_CHROMA_W8 8, 6, pp, 8
FILTER_VER_CHROMA_W8 8, 8, pp, 8
FILTER_VER_CHROMA_W8 8, 16, pp, 8
FILTER_VER_CHROMA_W8 8, 32, pp, 8

FILTER_VER_CHROMA_W8 8, 12, ss, 7
FILTER_VER_CHROMA_W8 8, 64, ss, 7
FILTER_VER_CHROMA_W8 8, 12, sp, 8
FILTER_VER_CHROMA_W8 8, 64, sp, 8
FILTER_VER_CHROMA_W8 8, 12, ps, 8
FILTER_VER_CHROMA_W8 8, 64, ps, 8
FILTER_VER_CHROMA_W8 8, 12, pp, 8
FILTER_VER_CHROMA_W8 8, 64, pp, 8

%macro PROCESS_CHROMA_VERT_W16_2R 0
    movu       m1, [r0]
    movu       m3, [r0 + r1]
    punpcklwd  m0, m1, m3
    pmaddwd    m0, [r5 + 0 * 32]
    punpckhwd  m1, m3
    pmaddwd    m1, [r5 + 0 * 32]

    movu       m4, [r0 + 2 * r1]
    punpcklwd  m2, m3, m4
    pmaddwd    m2, [r5 + 0 * 32]
    punpckhwd  m3, m4
    pmaddwd    m3, [r5 + 0 * 32]

    lea        r0, [r0 + 2 * r1]
    movu       m5, [r0 + r1]
    punpcklwd  m6, m4, m5
    pmaddwd    m6, [r5 + 1 * 32]
    paddd      m0, m6
    punpckhwd  m4, m5
    pmaddwd    m4, [r5 + 1 * 32]
    paddd      m1, m4

    movu       m4, [r0 + 2 * r1]
    punpcklwd  m6, m5, m4
    pmaddwd    m6, [r5 + 1 * 32]
    paddd      m2, m6
    punpckhwd  m5, m4
    pmaddwd    m5, [r5 + 1 * 32]
    paddd      m3, m5
%endmacro

;-----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_AVX2_6xN 2
INIT_YMM avx2
%if ARCH_X86_64
cglobal interp_4tap_vert_%2_6x%1, 4, 7, 10
    mov             r4d, r4m
    add             r1d, r1d
    add             r3d, r3d
    shl             r4d, 6

%ifdef PIC
    lea             r5, [tab_ChromaCoeffV]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffV + r4]
%endif

    sub             r0, r1
    mov             r6d, %1/4

%ifidn %2,pp
    vbroadcasti128  m8, [INTERP_OFFSET_PP]
%elifidn %2, sp
    vbroadcasti128  m8, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m8, [INTERP_OFFSET_PS]
%endif

.loopH:
    movu            xm0, [r0]
    movu            xm1, [r0 + r1]
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]

    movu            xm2, [r0 + r1 * 2]
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]

    lea             r4, [r1 * 3]
    movu            xm3, [r0 + r4]
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m4

    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    pmaddwd         m3, [r5]
    paddd           m1, m5

    movu            xm5, [r0 + r1]
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    pmaddwd         m4, [r5]
    paddd           m2, m6

    movu            xm6, [r0 + r1 * 2]
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    pmaddwd         m5, [r5]
    paddd           m3, m7
    lea             r4, [r3 * 3]
%ifidn %2,ss
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
%else
    paddd           m0, m8
    paddd           m1, m8
    paddd           m2, m8
    paddd           m3, m8
%ifidn %2,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
    psrad           m3, INTERP_SHIFT_PP
%elifidn %2, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m1
    packssdw        m2, m3
    vpermq          m0, m0, q3120
    vpermq          m2, m2, q3120
    pxor            m5, m5
    mova            m9, [pw_pixel_max]
%ifidn %2,pp
    CLIPW           m0, m5, m9
    CLIPW           m2, m5, m9
%elifidn %2, sp
    CLIPW           m0, m5, m9
    CLIPW           m2, m5, m9
%endif

    vextracti128    xm1, m0, 1
    vextracti128    xm3, m2, 1
    movq            [r2], xm0
    pextrd          [r2 + 8], xm0, 2
    movq            [r2 + r3], xm1
    pextrd          [r2 + r3 + 8], xm1, 2
    movq            [r2 + r3 * 2], xm2
    pextrd          [r2 + r3 * 2 + 8], xm2, 2
    movq            [r2 + r4], xm3
    pextrd          [r2 + r4 + 8], xm3, 2

    lea             r2, [r2 + r3 * 4]
    dec r6d
    jnz .loopH
    RET
%endif
%endmacro
FILTER_VER_CHROMA_AVX2_6xN 8, pp
FILTER_VER_CHROMA_AVX2_6xN 8, ps
FILTER_VER_CHROMA_AVX2_6xN 8, ss
FILTER_VER_CHROMA_AVX2_6xN 8, sp
FILTER_VER_CHROMA_AVX2_6xN 16, pp
FILTER_VER_CHROMA_AVX2_6xN 16, ps
FILTER_VER_CHROMA_AVX2_6xN 16, ss
FILTER_VER_CHROMA_AVX2_6xN 16, sp

;-----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W16_16xN_avx2 3
INIT_YMM avx2
cglobal interp_4tap_vert_%2_16x%1, 5, 6, %3
    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r5, [r5 + r4]
%else
    lea       r5, [tab_ChromaCoeffV + r4]
%endif

    mov       r4d, %1/2

%ifidn %2, pp
    vbroadcasti128  m7, [INTERP_OFFSET_PP]
%elifidn %2, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%elifidn %2, ps
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif

.loopH:
    PROCESS_CHROMA_VERT_W16_2R
%ifidn %2, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    packssdw  m0, m1
    packssdw  m2, m3
%elifidn %2, ps
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
%else
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
 %ifidn %2, pp
    psrad     m0, INTERP_SHIFT_PP
    psrad     m1, INTERP_SHIFT_PP
    psrad     m2, INTERP_SHIFT_PP
    psrad     m3, INTERP_SHIFT_PP
%else
    psrad     m0, INTERP_SHIFT_SP
    psrad     m1, INTERP_SHIFT_SP
    psrad     m2, INTERP_SHIFT_SP
    psrad     m3, INTERP_SHIFT_SP
%endif
    packssdw  m0, m1
    packssdw  m2, m3
    pxor      m5, m5
    CLIPW2    m0, m2, m5, [pw_pixel_max]
%endif

    movu      [r2], m0
    movu      [r2 + r3], m2
    lea       r2, [r2 + 2 * r3]
    dec       r4d
    jnz       .loopH
    RET
%endmacro
    FILTER_VER_CHROMA_W16_16xN_avx2 4, pp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 8, pp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 12, pp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 24, pp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 16, pp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 32, pp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 64, pp, 8

    FILTER_VER_CHROMA_W16_16xN_avx2 4, ps, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 8, ps, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 12, ps, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 24, ps, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 16, ps, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 32, ps, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 64, ps, 8

    FILTER_VER_CHROMA_W16_16xN_avx2 4, ss, 7
    FILTER_VER_CHROMA_W16_16xN_avx2 8, ss, 7
    FILTER_VER_CHROMA_W16_16xN_avx2 12, ss, 7
    FILTER_VER_CHROMA_W16_16xN_avx2 24, ss, 7
    FILTER_VER_CHROMA_W16_16xN_avx2 16, ss, 7
    FILTER_VER_CHROMA_W16_16xN_avx2 32, ss, 7
    FILTER_VER_CHROMA_W16_16xN_avx2 64, ss, 7

    FILTER_VER_CHROMA_W16_16xN_avx2 4, sp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 8, sp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 12, sp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 24, sp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 16, sp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 32, sp, 8
    FILTER_VER_CHROMA_W16_16xN_avx2 64, sp, 8

%macro PROCESS_CHROMA_VERT_W32_2R 0
    movu       m1, [r0]
    movu       m3, [r0 + r1]
    punpcklwd  m0, m1, m3
    pmaddwd    m0, [r5 + 0 * mmsize]
    punpckhwd  m1, m3
    pmaddwd    m1, [r5 + 0 * mmsize]

    movu       m9, [r0 + mmsize]
    movu       m11, [r0 + r1 + mmsize]
    punpcklwd  m8, m9, m11
    pmaddwd    m8, [r5 + 0 * mmsize]
    punpckhwd  m9, m11
    pmaddwd    m9, [r5 + 0 * mmsize]

    movu       m4, [r0 + 2 * r1]
    punpcklwd  m2, m3, m4
    pmaddwd    m2, [r5 + 0 * mmsize]
    punpckhwd  m3, m4
    pmaddwd    m3, [r5 + 0 * mmsize]

    movu       m12, [r0 + 2 * r1 + mmsize]
    punpcklwd  m10, m11, m12
    pmaddwd    m10, [r5 + 0 * mmsize]
    punpckhwd  m11, m12
    pmaddwd    m11, [r5 + 0 * mmsize]

    lea        r6, [r0 + 2 * r1]
    movu       m5, [r6 + r1]
    punpcklwd  m6, m4, m5
    pmaddwd    m6, [r5 + 1 * mmsize]
    paddd      m0, m6
    punpckhwd  m4, m5
    pmaddwd    m4, [r5 + 1 * mmsize]
    paddd      m1, m4

    movu       m13, [r6 + r1 + mmsize]
    punpcklwd  m14, m12, m13
    pmaddwd    m14, [r5 + 1 * mmsize]
    paddd      m8, m14
    punpckhwd  m12, m13
    pmaddwd    m12, [r5 + 1 * mmsize]
    paddd      m9, m12

    movu       m4, [r6 + 2 * r1]
    punpcklwd  m6, m5, m4
    pmaddwd    m6, [r5 + 1 * mmsize]
    paddd      m2, m6
    punpckhwd  m5, m4
    pmaddwd    m5, [r5 + 1 * mmsize]
    paddd      m3, m5

    movu       m12, [r6 + 2 * r1 + mmsize]
    punpcklwd  m14, m13, m12
    pmaddwd    m14, [r5 + 1 * mmsize]
    paddd      m10, m14
    punpckhwd  m13, m12
    pmaddwd    m13, [r5 + 1 * mmsize]
    paddd      m11, m13
%endmacro

;-----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W16_32xN_avx2 3
INIT_YMM avx2
%if ARCH_X86_64
cglobal interp_4tap_vert_%2_32x%1, 5, 7, %3
    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r5, [r5 + r4]
%else
    lea       r5, [tab_ChromaCoeffV + r4]
%endif
    mov       r4d, %1/2

%ifidn %2, pp
    vbroadcasti128  m7, [INTERP_OFFSET_PP]
%elifidn %2, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%elifidn %2, ps
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif

.loopH:
    PROCESS_CHROMA_VERT_W32_2R
%ifidn %2, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    psrad     m8, 6
    psrad     m9, 6
    psrad     m10, 6
    psrad     m11, 6

    packssdw  m0, m1
    packssdw  m2, m3
    packssdw  m8, m9
    packssdw  m10, m11
%elifidn %2, ps
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS
    paddd     m8, m7
    paddd     m9, m7
    paddd     m10, m7
    paddd     m11, m7
    psrad     m8, INTERP_SHIFT_PS
    psrad     m9, INTERP_SHIFT_PS
    psrad     m10, INTERP_SHIFT_PS
    psrad     m11, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
    packssdw  m8, m9
    packssdw  m10, m11
%else
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    paddd     m8, m7
    paddd     m9, m7
    paddd     m10, m7
    paddd     m11, m7
 %ifidn %2, pp
    psrad     m0, INTERP_SHIFT_PP
    psrad     m1, INTERP_SHIFT_PP
    psrad     m2, INTERP_SHIFT_PP
    psrad     m3, INTERP_SHIFT_PP
    psrad     m8, INTERP_SHIFT_PP
    psrad     m9, INTERP_SHIFT_PP
    psrad     m10, INTERP_SHIFT_PP
    psrad     m11, INTERP_SHIFT_PP
%else
    psrad     m0, INTERP_SHIFT_SP
    psrad     m1, INTERP_SHIFT_SP
    psrad     m2, INTERP_SHIFT_SP
    psrad     m3, INTERP_SHIFT_SP
    psrad     m8, INTERP_SHIFT_SP
    psrad     m9, INTERP_SHIFT_SP
    psrad     m10, INTERP_SHIFT_SP
    psrad     m11, INTERP_SHIFT_SP
%endif
    packssdw  m0, m1
    packssdw  m2, m3
    packssdw  m8, m9
    packssdw  m10, m11
    pxor      m5, m5
    CLIPW2    m0, m2, m5, [pw_pixel_max]
    CLIPW2    m8, m10, m5, [pw_pixel_max]
%endif

    movu      [r2], m0
    movu      [r2 + r3], m2
    movu      [r2 + mmsize], m8
    movu      [r2 + r3 + mmsize], m10
    lea       r2, [r2 + 2 * r3]
    lea       r0, [r0 + 2 * r1]
    dec       r4d
    jnz       .loopH
    RET
%endif
%endmacro
    FILTER_VER_CHROMA_W16_32xN_avx2 8, pp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 16, pp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 24, pp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 32, pp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 48, pp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 64, pp, 15

    FILTER_VER_CHROMA_W16_32xN_avx2 8, ps, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 16, ps, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 24, ps, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 32, ps, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 48, ps, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 64, ps, 15

    FILTER_VER_CHROMA_W16_32xN_avx2 8, ss, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 16, ss, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 24, ss, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 32, ss, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 48, ss, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 64, ss, 15

    FILTER_VER_CHROMA_W16_32xN_avx2 8, sp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 16, sp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 24, sp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 32, sp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 48, sp, 15
    FILTER_VER_CHROMA_W16_32xN_avx2 64, sp, 15

;-----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W16_64xN_avx2 3
INIT_YMM avx2
cglobal interp_4tap_vert_%2_64x%1, 5, 7, %3
    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r5, [r5 + r4]
%else
    lea       r5, [tab_ChromaCoeffV + r4]
%endif
    mov       r4d, %1/2

%ifidn %2, pp
    vbroadcasti128  m7, [INTERP_OFFSET_PP]
%elifidn %2, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%elifidn %2, ps
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif

.loopH:
%assign x 0
%rep 4
    movu       m1, [r0 + x]
    movu       m3, [r0 + r1 + x]
    movu       m5, [r5 + 0 * mmsize]
    punpcklwd  m0, m1, m3
    pmaddwd    m0, m5
    punpckhwd  m1, m3
    pmaddwd    m1, m5

    movu       m4, [r0 + 2 * r1 + x]
    punpcklwd  m2, m3, m4
    pmaddwd    m2, m5
    punpckhwd  m3, m4
    pmaddwd    m3, m5

    lea        r6, [r0 + 2 * r1]
    movu       m5, [r6 + r1 + x]
    punpcklwd  m6, m4, m5
    pmaddwd    m6, [r5 + 1 * mmsize]
    paddd      m0, m6
    punpckhwd  m4, m5
    pmaddwd    m4, [r5 + 1 * mmsize]
    paddd      m1, m4

    movu       m4, [r6 + 2 * r1 + x]
    punpcklwd  m6, m5, m4
    pmaddwd    m6, [r5 + 1 * mmsize]
    paddd      m2, m6
    punpckhwd  m5, m4
    pmaddwd    m5, [r5 + 1 * mmsize]
    paddd      m3, m5

%ifidn %2, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    packssdw  m0, m1
    packssdw  m2, m3
%elifidn %2, ps
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
%else
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
%ifidn %2, pp
    psrad     m0, INTERP_SHIFT_PP
    psrad     m1, INTERP_SHIFT_PP
    psrad     m2, INTERP_SHIFT_PP
    psrad     m3, INTERP_SHIFT_PP
%else
    psrad     m0, INTERP_SHIFT_SP
    psrad     m1, INTERP_SHIFT_SP
    psrad     m2, INTERP_SHIFT_SP
    psrad     m3, INTERP_SHIFT_SP
%endif
    packssdw  m0, m1
    packssdw  m2, m3
    pxor      m5, m5
    CLIPW2    m0, m2, m5, [pw_pixel_max]
%endif

    movu      [r2 + x], m0
    movu      [r2 + r3 + x], m2
%assign x x+mmsize
%endrep

    lea       r2, [r2 + 2 * r3]
    lea       r0, [r0 + 2 * r1]
    dec       r4d
    jnz       .loopH
    RET
%endmacro
    FILTER_VER_CHROMA_W16_64xN_avx2 16, ss, 7
    FILTER_VER_CHROMA_W16_64xN_avx2 32, ss, 7
    FILTER_VER_CHROMA_W16_64xN_avx2 48, ss, 7
    FILTER_VER_CHROMA_W16_64xN_avx2 64, ss, 7
    FILTER_VER_CHROMA_W16_64xN_avx2 16, sp, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 32, sp, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 48, sp, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 64, sp, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 16, ps, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 32, ps, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 48, ps, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 64, ps, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 16, pp, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 32, pp, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 48, pp, 8
    FILTER_VER_CHROMA_W16_64xN_avx2 64, pp, 8

;-----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W16_12xN_avx2 3
INIT_YMM avx2
cglobal interp_4tap_vert_%2_12x%1, 5, 8, %3
    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r5, [r5 + r4]
%else
    lea       r5, [tab_ChromaCoeffV + r4]
%endif
    mov       r4d, %1/2

%ifidn %2, pp
    vbroadcasti128  m7, [INTERP_OFFSET_PP]
%elifidn %2, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%elifidn %2, ps
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif

.loopH:
    PROCESS_CHROMA_VERT_W16_2R
%ifidn %2, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    packssdw  m0, m1
    packssdw  m2, m3
%elifidn %2, ps
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
%else
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
 %ifidn %2, pp
    psrad     m0, INTERP_SHIFT_PP
    psrad     m1, INTERP_SHIFT_PP
    psrad     m2, INTERP_SHIFT_PP
    psrad     m3, INTERP_SHIFT_PP
%else
    psrad     m0, INTERP_SHIFT_SP
    psrad     m1, INTERP_SHIFT_SP
    psrad     m2, INTERP_SHIFT_SP
    psrad     m3, INTERP_SHIFT_SP
%endif
    packssdw  m0, m1
    packssdw  m2, m3
    pxor      m5, m5
    CLIPW2    m0, m2, m5, [pw_pixel_max]
%endif

    movu      [r2], xm0
    movu      [r2 + r3], xm2
    vextracti128 xm0, m0, 1
    vextracti128 xm2, m2, 1
    movq      [r2 + 16], xm0
    movq      [r2 + r3 + 16], xm2
    lea       r2, [r2 + 2 * r3]
    dec       r4d
    jnz       .loopH
    RET
%endmacro
    FILTER_VER_CHROMA_W16_12xN_avx2 16, ss, 7
    FILTER_VER_CHROMA_W16_12xN_avx2 16, sp, 8
    FILTER_VER_CHROMA_W16_12xN_avx2 16, ps, 8
    FILTER_VER_CHROMA_W16_12xN_avx2 16, pp, 8
    FILTER_VER_CHROMA_W16_12xN_avx2 32, ss, 7
    FILTER_VER_CHROMA_W16_12xN_avx2 32, sp, 8
    FILTER_VER_CHROMA_W16_12xN_avx2 32, ps, 8
    FILTER_VER_CHROMA_W16_12xN_avx2 32, pp, 8

%macro PROCESS_CHROMA_VERT_W24_2R 0
    movu       m1, [r0]
    movu       m3, [r0 + r1]
    punpcklwd  m0, m1, m3
    pmaddwd    m0, [r5 + 0 * mmsize]
    punpckhwd  m1, m3
    pmaddwd    m1, [r5 + 0 * mmsize]

    movu       xm9, [r0 + mmsize]
    movu       xm11, [r0 + r1 + mmsize]
    punpcklwd  xm8, xm9, xm11
    pmaddwd    xm8, [r5 + 0 * mmsize]
    punpckhwd  xm9, xm11
    pmaddwd    xm9, [r5 + 0 * mmsize]

    movu       m4, [r0 + 2 * r1]
    punpcklwd  m2, m3, m4
    pmaddwd    m2, [r5 + 0 * mmsize]
    punpckhwd  m3, m4
    pmaddwd    m3, [r5 + 0 * mmsize]

    movu       xm12, [r0 + 2 * r1 + mmsize]
    punpcklwd  xm10, xm11, xm12
    pmaddwd    xm10, [r5 + 0 * mmsize]
    punpckhwd  xm11, xm12
    pmaddwd    xm11, [r5 + 0 * mmsize]

    lea        r6, [r0 + 2 * r1]
    movu       m5, [r6 + r1]
    punpcklwd  m6, m4, m5
    pmaddwd    m6, [r5 + 1 * mmsize]
    paddd      m0, m6
    punpckhwd  m4, m5
    pmaddwd    m4, [r5 + 1 * mmsize]
    paddd      m1, m4

    movu       xm13, [r6 + r1 + mmsize]
    punpcklwd  xm14, xm12, xm13
    pmaddwd    xm14, [r5 + 1 * mmsize]
    paddd      xm8, xm14
    punpckhwd  xm12, xm13
    pmaddwd    xm12, [r5 + 1 * mmsize]
    paddd      xm9, xm12

    movu       m4, [r6 + 2 * r1]
    punpcklwd  m6, m5, m4
    pmaddwd    m6, [r5 + 1 * mmsize]
    paddd      m2, m6
    punpckhwd  m5, m4
    pmaddwd    m5, [r5 + 1 * mmsize]
    paddd      m3, m5

    movu       xm12, [r6 + 2 * r1 + mmsize]
    punpcklwd  xm14, xm13, xm12
    pmaddwd    xm14, [r5 + 1 * mmsize]
    paddd      xm10, xm14
    punpckhwd  xm13, xm12
    pmaddwd    xm13, [r5 + 1 * mmsize]
    paddd      xm11, xm13
%endmacro

;-----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W16_24xN_avx2 3
INIT_YMM avx2
%if ARCH_X86_64
cglobal interp_4tap_vert_%2_24x%1, 5, 7, %3
    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r5, [r5 + r4]
%else
    lea       r5, [tab_ChromaCoeffV + r4]
%endif
    mov       r4d, %1/2

%ifidn %2, pp
    vbroadcasti128  m7, [INTERP_OFFSET_PP]
%elifidn %2, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%elifidn %2, ps
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif

.loopH:
    PROCESS_CHROMA_VERT_W24_2R
%ifidn %2, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    psrad     m8, 6
    psrad     m9, 6
    psrad     m10, 6
    psrad     m11, 6

    packssdw  m0, m1
    packssdw  m2, m3
    packssdw  m8, m9
    packssdw  m10, m11
%elifidn %2, ps
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS
    paddd     m8, m7
    paddd     m9, m7
    paddd     m10, m7
    paddd     m11, m7
    psrad     m8, INTERP_SHIFT_PS
    psrad     m9, INTERP_SHIFT_PS
    psrad     m10, INTERP_SHIFT_PS
    psrad     m11, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
    packssdw  m8, m9
    packssdw  m10, m11
%else
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    paddd     m8, m7
    paddd     m9, m7
    paddd     m10, m7
    paddd     m11, m7
 %ifidn %2, pp
    psrad     m0, INTERP_SHIFT_PP
    psrad     m1, INTERP_SHIFT_PP
    psrad     m2, INTERP_SHIFT_PP
    psrad     m3, INTERP_SHIFT_PP
    psrad     m8, INTERP_SHIFT_PP
    psrad     m9, INTERP_SHIFT_PP
    psrad     m10, INTERP_SHIFT_PP
    psrad     m11, INTERP_SHIFT_PP
%else
    psrad     m0, INTERP_SHIFT_SP
    psrad     m1, INTERP_SHIFT_SP
    psrad     m2, INTERP_SHIFT_SP
    psrad     m3, INTERP_SHIFT_SP
    psrad     m8, INTERP_SHIFT_SP
    psrad     m9, INTERP_SHIFT_SP
    psrad     m10, INTERP_SHIFT_SP
    psrad     m11, INTERP_SHIFT_SP
%endif
    packssdw  m0, m1
    packssdw  m2, m3
    packssdw  m8, m9
    packssdw  m10, m11
    pxor      m5, m5
    CLIPW2    m0, m2, m5, [pw_pixel_max]
    CLIPW2    m8, m10, m5, [pw_pixel_max]
%endif

    movu      [r2], m0
    movu      [r2 + r3], m2
    movu      [r2 + mmsize], xm8
    movu      [r2 + r3 + mmsize], xm10
    lea       r2, [r2 + 2 * r3]
    lea       r0, [r0 + 2 * r1]
    dec       r4d
    jnz       .loopH
    RET
%endif
%endmacro
    FILTER_VER_CHROMA_W16_24xN_avx2 32, ss, 15
    FILTER_VER_CHROMA_W16_24xN_avx2 32, sp, 15
    FILTER_VER_CHROMA_W16_24xN_avx2 32, ps, 15
    FILTER_VER_CHROMA_W16_24xN_avx2 32, pp, 15
    FILTER_VER_CHROMA_W16_24xN_avx2 64, ss, 15
    FILTER_VER_CHROMA_W16_24xN_avx2 64, sp, 15
    FILTER_VER_CHROMA_W16_24xN_avx2 64, ps, 15
    FILTER_VER_CHROMA_W16_24xN_avx2 64, pp, 15

;-----------------------------------------------------------------------------------------------------------------
; void interp_4tap_vert(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_CHROMA_W16_48x64_avx2 2
INIT_YMM avx2
cglobal interp_4tap_vert_%1_48x64, 5, 7, %2
    add       r1d, r1d
    add       r3d, r3d
    sub       r0, r1
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_ChromaCoeffV]
    lea       r5, [r5 + r4]
%else
    lea       r5, [tab_ChromaCoeffV + r4]
%endif
    mov       r4d, 32

%ifidn %1, pp
    vbroadcasti128  m7, [INTERP_OFFSET_PP]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%elifidn %1, ps
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif

.loopH:
%assign x 0
%rep 3
    movu       m1, [r0 + x]
    movu       m3, [r0 + r1 + x]
    movu       m5, [r5 + 0 * mmsize]
    punpcklwd  m0, m1, m3
    pmaddwd    m0, m5
    punpckhwd  m1, m3
    pmaddwd    m1, m5

    movu       m4, [r0 + 2 * r1 + x]
    punpcklwd  m2, m3, m4
    pmaddwd    m2, m5
    punpckhwd  m3, m4
    pmaddwd    m3, m5

    lea        r6, [r0 + 2 * r1]
    movu       m5, [r6 + r1 + x]
    punpcklwd  m6, m4, m5
    pmaddwd    m6, [r5 + 1 * mmsize]
    paddd      m0, m6
    punpckhwd  m4, m5
    pmaddwd    m4, [r5 + 1 * mmsize]
    paddd      m1, m4

    movu       m4, [r6 + 2 * r1 + x]
    punpcklwd  m6, m5, m4
    pmaddwd    m6, [r5 + 1 * mmsize]
    paddd      m2, m6
    punpckhwd  m5, m4
    pmaddwd    m5, [r5 + 1 * mmsize]
    paddd      m3, m5

%ifidn %1, ss
    psrad     m0, 6
    psrad     m1, 6
    psrad     m2, 6
    psrad     m3, 6

    packssdw  m0, m1
    packssdw  m2, m3
%elifidn %1, ps
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3
%else
    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7
%ifidn %1, pp
    psrad     m0, INTERP_SHIFT_PP
    psrad     m1, INTERP_SHIFT_PP
    psrad     m2, INTERP_SHIFT_PP
    psrad     m3, INTERP_SHIFT_PP
%else
    psrad     m0, INTERP_SHIFT_SP
    psrad     m1, INTERP_SHIFT_SP
    psrad     m2, INTERP_SHIFT_SP
    psrad     m3, INTERP_SHIFT_SP
%endif
    packssdw  m0, m1
    packssdw  m2, m3
    pxor      m5, m5
    CLIPW2    m0, m2, m5, [pw_pixel_max]
%endif

    movu      [r2 + x], m0
    movu      [r2 + r3 + x], m2
%assign x x+mmsize
%endrep

    lea       r2, [r2 + 2 * r3]
    lea       r0, [r0 + 2 * r1]
    dec       r4d
    jnz       .loopH
    RET
%endmacro

    FILTER_VER_CHROMA_W16_48x64_avx2 pp, 8
    FILTER_VER_CHROMA_W16_48x64_avx2 ps, 8
    FILTER_VER_CHROMA_W16_48x64_avx2 ss, 7
    FILTER_VER_CHROMA_W16_48x64_avx2 sp, 8

INIT_XMM sse2
cglobal chroma_p2s, 3, 7, 3
    ; load width and height
    mov         r3d, r3m
    mov         r4d, r4m
    add         r1, r1

    ; load constant
    mova        m2, [tab_c_n8192]

.loopH:

    xor         r5d, r5d
.loopW:
    lea         r6, [r0 + r5 * 2]

    movu        m0, [r6]
    psllw       m0, (14 - BIT_DEPTH)
    paddw       m0, m2

    movu        m1, [r6 + r1]
    psllw       m1, (14 - BIT_DEPTH)
    paddw       m1, m2

    add         r5d, 8
    cmp         r5d, r3d
    lea         r6, [r2 + r5 * 2]
    jg          .width4
    movu        [r6 + FENC_STRIDE / 2 * 0 - 16], m0
    movu        [r6 + FENC_STRIDE / 2 * 2 - 16], m1
    je          .nextH
    jmp         .loopW

.width4:
    test        r3d, 4
    jz          .width2
    test        r3d, 2
    movh        [r6 + FENC_STRIDE / 2 * 0 - 16], m0
    movh        [r6 + FENC_STRIDE / 2 * 2 - 16], m1
    lea         r6, [r6 + 8]
    pshufd      m0, m0, 2
    pshufd      m1, m1, 2
    jz          .nextH

.width2:
    movd        [r6 + FENC_STRIDE / 2 * 0 - 16], m0
    movd        [r6 + FENC_STRIDE / 2 * 2 - 16], m1

.nextH:
    lea         r0, [r0 + r1 * 2]
    add         r2, FENC_STRIDE / 2 * 4

    sub         r4d, 2
    jnz         .loopH
    RET

%macro PROCESS_LUMA_VER_W4_4R 0
    movq       m0, [r0]
    movq       m1, [r0 + r1]
    punpcklwd  m0, m1                          ;m0=[0 1]
    pmaddwd    m0, [r6 + 0 *16]                ;m0=[0+1]  Row1

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m1, m4                          ;m1=[1 2]
    pmaddwd    m1, [r6 + 0 *16]                ;m1=[1+2]  Row2

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[2 3]
    pmaddwd    m2, m4, [r6 + 0 *16]            ;m2=[2+3]  Row3
    pmaddwd    m4, [r6 + 1 * 16]
    paddd      m0, m4                          ;m0=[0+1+2+3]  Row1

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m5, m4                          ;m5=[3 4]
    pmaddwd    m3, m5, [r6 + 0 *16]            ;m3=[3+4]  Row4
    pmaddwd    m5, [r6 + 1 * 16]
    paddd      m1, m5                          ;m1 = [1+2+3+4]  Row2

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[4 5]
    pmaddwd    m6, m4, [r6 + 1 * 16]
    paddd      m2, m6                          ;m2=[2+3+4+5]  Row3
    pmaddwd    m4, [r6 + 2 * 16]
    paddd      m0, m4                          ;m0=[0+1+2+3+4+5]  Row1

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m5, m4                          ;m5=[5 6]
    pmaddwd    m6, m5, [r6 + 1 * 16]
    paddd      m3, m6                          ;m3=[3+4+5+6]  Row4
    pmaddwd    m5, [r6 + 2 * 16]
    paddd      m1, m5                          ;m1=[1+2+3+4+5+6]  Row2

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[6 7]
    pmaddwd    m6, m4, [r6 + 2 * 16]
    paddd      m2, m6                          ;m2=[2+3+4+5+6+7]  Row3
    pmaddwd    m4, [r6 + 3 * 16]
    paddd      m0, m4                          ;m0=[0+1+2+3+4+5+6+7]  Row1 end

    lea        r0, [r0 + 2 * r1]
    movq       m4, [r0]
    punpcklwd  m5, m4                          ;m5=[7 8]
    pmaddwd    m6, m5, [r6 + 2 * 16]
    paddd      m3, m6                          ;m3=[3+4+5+6+7+8]  Row4
    pmaddwd    m5, [r6 + 3 * 16]
    paddd      m1, m5                          ;m1=[1+2+3+4+5+6+7+8]  Row2 end

    movq       m5, [r0 + r1]
    punpcklwd  m4, m5                          ;m4=[8 9]
    pmaddwd    m4, [r6 + 3 * 16]
    paddd      m2, m4                          ;m2=[2+3+4+5+6+7+8+9]  Row3 end

    movq       m4, [r0 + 2 * r1]
    punpcklwd  m5, m4                          ;m5=[9 10]
    pmaddwd    m5, [r6 + 3 * 16]
    paddd      m3, m5                          ;m3=[3+4+5+6+7+8+9+10]  Row4 end
%endmacro

;--------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_pp_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;--------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_LUMA_PP 2
INIT_XMM sse4
cglobal interp_8tap_vert_pp_%1x%2, 5, 7, 8 ,0-gprsize

    add       r1d, r1d
    add       r3d, r3d
    lea       r5, [r1 + 2 * r1]
    sub       r0, r5
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_LumaCoeffV]
    lea       r6, [r5 + r4]
%else
    lea       r6, [tab_LumaCoeffV + r4]
%endif

    mova      m7, [INTERP_OFFSET_PP]

    mov       dword [rsp], %2/4
.loopH:
    mov       r4d, (%1/4)
.loopW:
    PROCESS_LUMA_VER_W4_4R

    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7

    psrad     m0, INTERP_SHIFT_PP
    psrad     m1, INTERP_SHIFT_PP
    psrad     m2, INTERP_SHIFT_PP
    psrad     m3, INTERP_SHIFT_PP

    packssdw  m0, m1
    packssdw  m2, m3

    pxor      m1, m1
    CLIPW2    m0, m2, m1, [pw_pixel_max]

    movh      [r2], m0
    movhps    [r2 + r3], m0
    lea       r5, [r2 + 2 * r3]
    movh      [r5], m2
    movhps    [r5 + r3], m2

    lea       r5, [8 * r1 - 2 * 4]
    sub       r0, r5
    add       r2, 2 * 4

    dec       r4d
    jnz       .loopW

    lea       r0, [r0 + 4 * r1 - 2 * %1]
    lea       r2, [r2 + 4 * r3 - 2 * %1]

    dec       dword [rsp]
    jnz       .loopH
    RET
%endmacro

;-------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_pp_%1x%2(pixel *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;-------------------------------------------------------------------------------------------------------------
    FILTER_VER_LUMA_PP 4, 4
    FILTER_VER_LUMA_PP 8, 8
    FILTER_VER_LUMA_PP 8, 4
    FILTER_VER_LUMA_PP 4, 8
    FILTER_VER_LUMA_PP 16, 16
    FILTER_VER_LUMA_PP 16, 8
    FILTER_VER_LUMA_PP 8, 16
    FILTER_VER_LUMA_PP 16, 12
    FILTER_VER_LUMA_PP 12, 16
    FILTER_VER_LUMA_PP 16, 4
    FILTER_VER_LUMA_PP 4, 16
    FILTER_VER_LUMA_PP 32, 32
    FILTER_VER_LUMA_PP 32, 16
    FILTER_VER_LUMA_PP 16, 32
    FILTER_VER_LUMA_PP 32, 24
    FILTER_VER_LUMA_PP 24, 32
    FILTER_VER_LUMA_PP 32, 8
    FILTER_VER_LUMA_PP 8, 32
    FILTER_VER_LUMA_PP 64, 64
    FILTER_VER_LUMA_PP 64, 32
    FILTER_VER_LUMA_PP 32, 64
    FILTER_VER_LUMA_PP 64, 48
    FILTER_VER_LUMA_PP 48, 64
    FILTER_VER_LUMA_PP 64, 16
    FILTER_VER_LUMA_PP 16, 64

%macro FILTER_VER_LUMA_AVX2_4x4 1
INIT_YMM avx2
cglobal interp_8tap_vert_%1_4x4, 4, 6, 7
    mov             r4d, r4m
    add             r1d, r1d
    add             r3d, r3d
    shl             r4d, 7

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4

%ifidn %1,pp
    vbroadcasti128  m6, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m6, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m6, [INTERP_OFFSET_PS]
%endif

    movq            xm0, [r0]
    movq            xm1, [r0 + r1]
    punpcklwd       xm0, xm1
    movq            xm2, [r0 + r1 * 2]
    punpcklwd       xm1, xm2
    vinserti128     m0, m0, xm1, 1                  ; m0 = [2 1 1 0]
    pmaddwd         m0, [r5]
    movq            xm3, [r0 + r4]
    punpcklwd       xm2, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm4, [r0]
    punpcklwd       xm3, xm4
    vinserti128     m2, m2, xm3, 1                  ; m2 = [4 3 3 2]
    pmaddwd         m5, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m5
    movq            xm3, [r0 + r1]
    punpcklwd       xm4, xm3
    movq            xm1, [r0 + r1 * 2]
    punpcklwd       xm3, xm1
    vinserti128     m4, m4, xm3, 1                  ; m4 = [6 5 5 4]
    pmaddwd         m5, m4, [r5 + 2 * mmsize]
    pmaddwd         m4, [r5 + 1 * mmsize]
    paddd           m0, m5
    paddd           m2, m4
    movq            xm3, [r0 + r4]
    punpcklwd       xm1, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm4, [r0]
    punpcklwd       xm3, xm4
    vinserti128     m1, m1, xm3, 1                  ; m1 = [8 7 7 6]
    pmaddwd         m5, m1, [r5 + 3 * mmsize]
    pmaddwd         m1, [r5 + 2 * mmsize]
    paddd           m0, m5
    paddd           m2, m1
    movq            xm3, [r0 + r1]
    punpcklwd       xm4, xm3
    movq            xm1, [r0 + 2 * r1]
    punpcklwd       xm3, xm1
    vinserti128     m4, m4, xm3, 1                  ; m4 = [A 9 9 8]
    pmaddwd         m4, [r5 + 3 * mmsize]
    paddd           m2, m4

%ifidn %1,ss
    psrad           m0, 6
    psrad           m2, 6
%else
    paddd           m0, m6
    paddd           m2, m6
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m2
    pxor            m1, m1
%ifidn %1,pp
    CLIPW           m0, m1, [pw_pixel_max]
%elifidn %1, sp
    CLIPW           m0, m1, [pw_pixel_max]
%endif

    vextracti128    xm2, m0, 1
    lea             r4, [r3 * 3]
    movq            [r2], xm0
    movq            [r2 + r3], xm2
    movhps          [r2 + r3 * 2], xm0
    movhps          [r2 + r4], xm2
    RET
%endmacro

FILTER_VER_LUMA_AVX2_4x4 pp
FILTER_VER_LUMA_AVX2_4x4 ps
FILTER_VER_LUMA_AVX2_4x4 sp
FILTER_VER_LUMA_AVX2_4x4 ss

%macro FILTER_VER_LUMA_AVX2_8x8 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_vert_%1_8x8, 4, 6, 12
    mov             r4d, r4m
    add             r1d, r1d
    add             r3d, r3d
    shl             r4d, 7

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4

%ifidn %1,pp
    vbroadcasti128  m11, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m11, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m11, [INTERP_OFFSET_PS]
%endif

    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m4
    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    pmaddwd         m3, [r5]
    paddd           m1, m5
    movu            xm5, [r0 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 2 * mmsize]
    paddd           m0, m6
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    paddd           m2, m6
    pmaddwd         m4, [r5]
    movu            xm6, [r0 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 2 * mmsize]
    paddd           m1, m7
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    pmaddwd         m5, [r5]
    paddd           m3, m7
    movu            xm7, [r0 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 3 * mmsize]
    paddd           m0, m8
    pmaddwd         m8, m6, [r5 + 2 * mmsize]
    paddd           m2, m8
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    pmaddwd         m6, [r5]
    paddd           m4, m8
    lea             r0, [r0 + r1 * 4]
    movu            xm8, [r0]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m9, m7, [r5 + 3 * mmsize]
    paddd           m1, m9
    pmaddwd         m9, m7, [r5 + 2 * mmsize]
    paddd           m3, m9
    pmaddwd         m9, m7, [r5 + 1 * mmsize]
    pmaddwd         m7, [r5]
    paddd           m5, m9
    movu            xm9, [r0 + r1]                  ; m9 = row 9
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m10, m8, [r5 + 3 * mmsize]
    paddd           m2, m10
    pmaddwd         m10, m8, [r5 + 2 * mmsize]
    pmaddwd         m8, [r5 + 1 * mmsize]
    paddd           m4, m10
    paddd           m6, m8
    movu            xm10, [r0 + r1 * 2]             ; m10 = row 10
    punpckhwd       xm8, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm8, 1
    pmaddwd         m8, m9, [r5 + 3 * mmsize]
    paddd           m3, m8
    pmaddwd         m8, m9, [r5 + 2 * mmsize]
    pmaddwd         m9, [r5 + 1 * mmsize]
    paddd           m5, m8
    paddd           m7, m9
    movu            xm8, [r0 + r4]                  ; m8 = row 11
    punpckhwd       xm9, xm10, xm8
    punpcklwd       xm10, xm8
    vinserti128     m10, m10, xm9, 1
    pmaddwd         m9, m10, [r5 + 3 * mmsize]
    pmaddwd         m10, [r5 + 2 * mmsize]
    paddd           m4, m9
    paddd           m6, m10

    lea             r4, [r3 * 3]
%ifidn %1,ss
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
%else
    paddd           m0, m11
    paddd           m1, m11
    paddd           m2, m11
    paddd           m3, m11
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
    psrad           m3, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m1
    packssdw        m2, m3
    vpermq          m0, m0, 11011000b
    vpermq          m2, m2, 11011000b
    pxor            m10, m10
    mova            m9, [pw_pixel_max]
%ifidn %1,pp
    CLIPW           m0, m10, m9
    CLIPW           m2, m10, m9
%elifidn %1, sp
    CLIPW           m0, m10, m9
    CLIPW           m2, m10, m9
%endif

    vextracti128    xm1, m0, 1
    vextracti128    xm3, m2, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r4], xm3

    lea             r0, [r0 + r1 * 4]
    movu            xm2, [r0]                       ; m2 = row 12
    punpckhwd       xm3, xm8, xm2
    punpcklwd       xm8, xm2
    vinserti128     m8, m8, xm3, 1
    pmaddwd         m3, m8, [r5 + 3 * mmsize]
    pmaddwd         m8, [r5 + 2 * mmsize]
    paddd           m5, m3
    paddd           m7, m8
    movu            xm3, [r0 + r1]                  ; m3 = row 13
    punpckhwd       xm0, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm0, 1
    pmaddwd         m2, [r5 + 3 * mmsize]
    paddd           m6, m2
    movu            xm0, [r0 + r1 * 2]              ; m0 = row 14
    punpckhwd       xm1, xm3, xm0
    punpcklwd       xm3, xm0
    vinserti128     m3, m3, xm1, 1
    pmaddwd         m3, [r5 + 3 * mmsize]
    paddd           m7, m3

%ifidn %1,ss
    psrad           m4, 6
    psrad           m5, 6
    psrad           m6, 6
    psrad           m7, 6
%else
    paddd           m4, m11
    paddd           m5, m11
    paddd           m6, m11
    paddd           m7, m11
%ifidn %1,pp
    psrad           m4, INTERP_SHIFT_PP
    psrad           m5, INTERP_SHIFT_PP
    psrad           m6, INTERP_SHIFT_PP
    psrad           m7, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m4, INTERP_SHIFT_SP
    psrad           m5, INTERP_SHIFT_SP
    psrad           m6, INTERP_SHIFT_SP
    psrad           m7, INTERP_SHIFT_SP
%else
    psrad           m4, INTERP_SHIFT_PS
    psrad           m5, INTERP_SHIFT_PS
    psrad           m6, INTERP_SHIFT_PS
    psrad           m7, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m4, m5
    packssdw        m6, m7
    vpermq          m4, m4, 11011000b
    vpermq          m6, m6, 11011000b
%ifidn %1,pp
    CLIPW           m4, m10, m9
    CLIPW           m6, m10, m9
%elifidn %1, sp
    CLIPW           m4, m10, m9
    CLIPW           m6, m10, m9
%endif
    vextracti128    xm5, m4, 1
    vextracti128    xm7, m6, 1
    lea             r2, [r2 + r3 * 4]
    movu            [r2], xm4
    movu            [r2 + r3], xm5
    movu            [r2 + r3 * 2], xm6
    movu            [r2 + r4], xm7
    RET
%endif
%endmacro

FILTER_VER_LUMA_AVX2_8x8 pp
FILTER_VER_LUMA_AVX2_8x8 ps
FILTER_VER_LUMA_AVX2_8x8 sp
FILTER_VER_LUMA_AVX2_8x8 ss

%macro PROCESS_LUMA_AVX2_W8_16R 1
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5]
    lea             r7, [r0 + r1 * 4]
    movu            xm4, [r7]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    paddd           m1, m5
    pmaddwd         m3, [r5]
    movu            xm5, [r7 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 2 * mmsize]
    paddd           m0, m6
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    paddd           m2, m6
    pmaddwd         m4, [r5]
    movu            xm6, [r7 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 2 * mmsize]
    paddd           m1, m7
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    paddd           m3, m7
    pmaddwd         m5, [r5]
    movu            xm7, [r7 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 3 * mmsize]
    paddd           m0, m8
    pmaddwd         m8, m6, [r5 + 2 * mmsize]
    paddd           m2, m8
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    paddd           m4, m8
    pmaddwd         m6, [r5]
    lea             r7, [r7 + r1 * 4]
    movu            xm8, [r7]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m9, m7, [r5 + 3 * mmsize]
    paddd           m1, m9
    pmaddwd         m9, m7, [r5 + 2 * mmsize]
    paddd           m3, m9
    pmaddwd         m9, m7, [r5 + 1 * mmsize]
    paddd           m5, m9
    pmaddwd         m7, [r5]
    movu            xm9, [r7 + r1]                  ; m9 = row 9
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m10, m8, [r5 + 3 * mmsize]
    paddd           m2, m10
    pmaddwd         m10, m8, [r5 + 2 * mmsize]
    paddd           m4, m10
    pmaddwd         m10, m8, [r5 + 1 * mmsize]
    paddd           m6, m10
    pmaddwd         m8, [r5]
    movu            xm10, [r7 + r1 * 2]             ; m10 = row 10
    punpckhwd       xm11, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm11, 1
    pmaddwd         m11, m9, [r5 + 3 * mmsize]
    paddd           m3, m11
    pmaddwd         m11, m9, [r5 + 2 * mmsize]
    paddd           m5, m11
    pmaddwd         m11, m9, [r5 + 1 * mmsize]
    paddd           m7, m11
    pmaddwd         m9, [r5]
    movu            xm11, [r7 + r4]                 ; m11 = row 11
    punpckhwd       xm12, xm10, xm11
    punpcklwd       xm10, xm11
    vinserti128     m10, m10, xm12, 1
    pmaddwd         m12, m10, [r5 + 3 * mmsize]
    paddd           m4, m12
    pmaddwd         m12, m10, [r5 + 2 * mmsize]
    paddd           m6, m12
    pmaddwd         m12, m10, [r5 + 1 * mmsize]
    paddd           m8, m12
    pmaddwd         m10, [r5]
    lea             r7, [r7 + r1 * 4]
    movu            xm12, [r7]                      ; m12 = row 12
    punpckhwd       xm13, xm11, xm12
    punpcklwd       xm11, xm12
    vinserti128     m11, m11, xm13, 1
    pmaddwd         m13, m11, [r5 + 3 * mmsize]
    paddd           m5, m13
    pmaddwd         m13, m11, [r5 + 2 * mmsize]
    paddd           m7, m13
    pmaddwd         m13, m11, [r5 + 1 * mmsize]
    paddd           m9, m13
    pmaddwd         m11, [r5]

%ifidn %1,ss
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
    psrad           m4, 6
    psrad           m5, 6
%else
    paddd           m0, m14
    paddd           m1, m14
    paddd           m2, m14
    paddd           m3, m14
    paddd           m4, m14
    paddd           m5, m14
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
    psrad           m3, INTERP_SHIFT_PP
    psrad           m4, INTERP_SHIFT_PP
    psrad           m5, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
    psrad           m4, INTERP_SHIFT_SP
    psrad           m5, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
    psrad           m4, INTERP_SHIFT_PS
    psrad           m5, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m1
    packssdw        m2, m3
    packssdw        m4, m5
    vpermq          m0, m0, 11011000b
    vpermq          m2, m2, 11011000b
    vpermq          m4, m4, 11011000b
    pxor            m5, m5
    mova            m3, [pw_pixel_max]
%ifidn %1,pp
    CLIPW           m0, m5, m3
    CLIPW           m2, m5, m3
    CLIPW           m4, m5, m3
%elifidn %1, sp
    CLIPW           m0, m5, m3
    CLIPW           m2, m5, m3
    CLIPW           m4, m5, m3
%endif

    vextracti128    xm1, m0, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    vextracti128    xm1, m2, 1
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r6], xm1
    lea             r8, [r2 + r3 * 4]
    vextracti128    xm1, m4, 1
    movu            [r8], xm4
    movu            [r8 + r3], xm1

    movu            xm13, [r7 + r1]                 ; m13 = row 13
    punpckhwd       xm0, xm12, xm13
    punpcklwd       xm12, xm13
    vinserti128     m12, m12, xm0, 1
    pmaddwd         m0, m12, [r5 + 3 * mmsize]
    paddd           m6, m0
    pmaddwd         m0, m12, [r5 + 2 * mmsize]
    paddd           m8, m0
    pmaddwd         m0, m12, [r5 + 1 * mmsize]
    paddd           m10, m0
    pmaddwd         m12, [r5]
    movu            xm0, [r7 + r1 * 2]              ; m0 = row 14
    punpckhwd       xm1, xm13, xm0
    punpcklwd       xm13, xm0
    vinserti128     m13, m13, xm1, 1
    pmaddwd         m1, m13, [r5 + 3 * mmsize]
    paddd           m7, m1
    pmaddwd         m1, m13, [r5 + 2 * mmsize]
    paddd           m9, m1
    pmaddwd         m1, m13, [r5 + 1 * mmsize]
    paddd           m11, m1
    pmaddwd         m13, [r5]

%ifidn %1,ss
    psrad           m6, 6
    psrad           m7, 6
%else
    paddd           m6, m14
    paddd           m7, m14
%ifidn %1,pp
    psrad           m6, INTERP_SHIFT_PP
    psrad           m7, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m6, INTERP_SHIFT_SP
    psrad           m7, INTERP_SHIFT_SP
%else
    psrad           m6, INTERP_SHIFT_PS
    psrad           m7, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m6, m7
    vpermq          m6, m6, 11011000b
%ifidn %1,pp
    CLIPW           m6, m5, m3
%elifidn %1, sp
    CLIPW           m6, m5, m3
%endif
    vextracti128    xm7, m6, 1
    movu            [r8 + r3 * 2], xm6
    movu            [r8 + r6], xm7

    movu            xm1, [r7 + r4]                  ; m1 = row 15
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m2, m0, [r5 + 3 * mmsize]
    paddd           m8, m2
    pmaddwd         m2, m0, [r5 + 2 * mmsize]
    paddd           m10, m2
    pmaddwd         m2, m0, [r5 + 1 * mmsize]
    paddd           m12, m2
    pmaddwd         m0, [r5]
    lea             r7, [r7 + r1 * 4]
    movu            xm2, [r7]                       ; m2 = row 16
    punpckhwd       xm6, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm6, 1
    pmaddwd         m6, m1, [r5 + 3 * mmsize]
    paddd           m9, m6
    pmaddwd         m6, m1, [r5 + 2 * mmsize]
    paddd           m11, m6
    pmaddwd         m6, m1, [r5 + 1 * mmsize]
    paddd           m13, m6
    pmaddwd         m1, [r5]
    movu            xm6, [r7 + r1]                  ; m6 = row 17
    punpckhwd       xm4, xm2, xm6
    punpcklwd       xm2, xm6
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 3 * mmsize]
    paddd           m10, m4
    pmaddwd         m4, m2, [r5 + 2 * mmsize]
    paddd           m12, m4
    pmaddwd         m2, [r5 + 1 * mmsize]
    paddd           m0, m2
    movu            xm4, [r7 + r1 * 2]              ; m4 = row 18
    punpckhwd       xm2, xm6, xm4
    punpcklwd       xm6, xm4
    vinserti128     m6, m6, xm2, 1
    pmaddwd         m2, m6, [r5 + 3 * mmsize]
    paddd           m11, m2
    pmaddwd         m2, m6, [r5 + 2 * mmsize]
    paddd           m13, m2
    pmaddwd         m6, [r5 + 1 * mmsize]
    paddd           m1, m6
    movu            xm2, [r7 + r4]                  ; m2 = row 19
    punpckhwd       xm6, xm4, xm2
    punpcklwd       xm4, xm2
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 3 * mmsize]
    paddd           m12, m6
    pmaddwd         m4, [r5 + 2 * mmsize]
    paddd           m0, m4
    lea             r7, [r7 + r1 * 4]
    movu            xm6, [r7]                       ; m6 = row 20
    punpckhwd       xm7, xm2, xm6
    punpcklwd       xm2, xm6
    vinserti128     m2, m2, xm7, 1
    pmaddwd         m7, m2, [r5 + 3 * mmsize]
    paddd           m13, m7
    pmaddwd         m2, [r5 + 2 * mmsize]
    paddd           m1, m2
    movu            xm7, [r7 + r1]                  ; m7 = row 21
    punpckhwd       xm2, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm2, 1
    pmaddwd         m6, [r5 + 3 * mmsize]
    paddd           m0, m6
    movu            xm2, [r7 + r1 * 2]              ; m2 = row 22
    punpckhwd       xm6, xm7, xm2
    punpcklwd       xm7, xm2
    vinserti128     m7, m7, xm6, 1
    pmaddwd         m7, [r5 + 3 * mmsize]
    paddd           m1, m7

%ifidn %1,ss
    psrad           m8, 6
    psrad           m9, 6
    psrad           m10, 6
    psrad           m11, 6
    psrad           m12, 6
    psrad           m13, 6
    psrad           m0, 6
    psrad           m1, 6
%else
    paddd           m8, m14
    paddd           m9, m14
    paddd           m10, m14
    paddd           m11, m14
    paddd           m12, m14
    paddd           m13, m14
    paddd           m0, m14
    paddd           m1, m14
%ifidn %1,pp
    psrad           m8, INTERP_SHIFT_PP
    psrad           m9, INTERP_SHIFT_PP
    psrad           m10, INTERP_SHIFT_PP
    psrad           m11, INTERP_SHIFT_PP
    psrad           m12, INTERP_SHIFT_PP
    psrad           m13, INTERP_SHIFT_PP
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m8, INTERP_SHIFT_SP
    psrad           m9, INTERP_SHIFT_SP
    psrad           m10, INTERP_SHIFT_SP
    psrad           m11, INTERP_SHIFT_SP
    psrad           m12, INTERP_SHIFT_SP
    psrad           m13, INTERP_SHIFT_SP
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
%else
    psrad           m8, INTERP_SHIFT_PS
    psrad           m9, INTERP_SHIFT_PS
    psrad           m10, INTERP_SHIFT_PS
    psrad           m11, INTERP_SHIFT_PS
    psrad           m12, INTERP_SHIFT_PS
    psrad           m13, INTERP_SHIFT_PS
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m8, m9
    packssdw        m10, m11
    packssdw        m12, m13
    packssdw        m0, m1
    vpermq          m8, m8, 11011000b
    vpermq          m10, m10, 11011000b
    vpermq          m12, m12, 11011000b
    vpermq          m0, m0, 11011000b
%ifidn %1,pp
    CLIPW           m8, m5, m3
    CLIPW           m10, m5, m3
    CLIPW           m12, m5, m3
    CLIPW           m0, m5, m3
%elifidn %1, sp
    CLIPW           m8, m5, m3
    CLIPW           m10, m5, m3
    CLIPW           m12, m5, m3
    CLIPW           m0, m5, m3
%endif
    vextracti128    xm9, m8, 1
    vextracti128    xm11, m10, 1
    vextracti128    xm13, m12, 1
    vextracti128    xm1, m0, 1
    lea             r8, [r8 + r3 * 4]
    movu            [r8], xm8
    movu            [r8 + r3], xm9
    movu            [r8 + r3 * 2], xm10
    movu            [r8 + r6], xm11
    lea             r8, [r8 + r3 * 4]
    movu            [r8], xm12
    movu            [r8 + r3], xm13
    movu            [r8 + r3 * 2], xm0
    movu            [r8 + r6], xm1
%endmacro

%macro FILTER_VER_LUMA_AVX2_Nx16 2
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_vert_%1_%2x16, 4, 10, 15
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m14, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m14, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m14, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]
    mov             r9d, %2 / 8
.loopW:
    PROCESS_LUMA_AVX2_W8_16R %1
    add             r2, 16
    add             r0, 16
    dec             r9d
    jnz             .loopW
    RET
%endif
%endmacro

FILTER_VER_LUMA_AVX2_Nx16 pp, 16
FILTER_VER_LUMA_AVX2_Nx16 pp, 32
FILTER_VER_LUMA_AVX2_Nx16 pp, 64
FILTER_VER_LUMA_AVX2_Nx16 ps, 16
FILTER_VER_LUMA_AVX2_Nx16 ps, 32
FILTER_VER_LUMA_AVX2_Nx16 ps, 64
FILTER_VER_LUMA_AVX2_Nx16 sp, 16
FILTER_VER_LUMA_AVX2_Nx16 sp, 32
FILTER_VER_LUMA_AVX2_Nx16 sp, 64
FILTER_VER_LUMA_AVX2_Nx16 ss, 16
FILTER_VER_LUMA_AVX2_Nx16 ss, 32
FILTER_VER_LUMA_AVX2_Nx16 ss, 64

%macro FILTER_VER_LUMA_AVX2_NxN 3
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_vert_%3_%1x%2, 4, 12, 15
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4

%ifidn %3,pp
    vbroadcasti128  m14, [pd_32]
%elifidn %3, sp
    vbroadcasti128  m14, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m14, [INTERP_OFFSET_PS]
%endif

    lea             r6, [r3 * 3]
    lea             r11, [r1 * 4]
    mov             r9d, %2 / 16
.loopH:
    mov             r10d, %1 / 8
.loopW:
    PROCESS_LUMA_AVX2_W8_16R %3
    add             r2, 16
    add             r0, 16
    dec             r10d
    jnz             .loopW
    sub             r7, r11
    lea             r0, [r7 - 2 * %1 + 16]
    lea             r2, [r8 + r3 * 4 - 2 * %1 + 16]
    dec             r9d
    jnz             .loopH
    RET
%endif
%endmacro

FILTER_VER_LUMA_AVX2_NxN 16, 32, pp
FILTER_VER_LUMA_AVX2_NxN 16, 64, pp
FILTER_VER_LUMA_AVX2_NxN 24, 32, pp
FILTER_VER_LUMA_AVX2_NxN 32, 32, pp
FILTER_VER_LUMA_AVX2_NxN 32, 64, pp
FILTER_VER_LUMA_AVX2_NxN 48, 64, pp
FILTER_VER_LUMA_AVX2_NxN 64, 32, pp
FILTER_VER_LUMA_AVX2_NxN 64, 48, pp
FILTER_VER_LUMA_AVX2_NxN 64, 64, pp
FILTER_VER_LUMA_AVX2_NxN 16, 32, ps
FILTER_VER_LUMA_AVX2_NxN 16, 64, ps
FILTER_VER_LUMA_AVX2_NxN 24, 32, ps
FILTER_VER_LUMA_AVX2_NxN 32, 32, ps
FILTER_VER_LUMA_AVX2_NxN 32, 64, ps
FILTER_VER_LUMA_AVX2_NxN 48, 64, ps
FILTER_VER_LUMA_AVX2_NxN 64, 32, ps
FILTER_VER_LUMA_AVX2_NxN 64, 48, ps
FILTER_VER_LUMA_AVX2_NxN 64, 64, ps
FILTER_VER_LUMA_AVX2_NxN 16, 32, sp
FILTER_VER_LUMA_AVX2_NxN 16, 64, sp
FILTER_VER_LUMA_AVX2_NxN 24, 32, sp
FILTER_VER_LUMA_AVX2_NxN 32, 32, sp
FILTER_VER_LUMA_AVX2_NxN 32, 64, sp
FILTER_VER_LUMA_AVX2_NxN 48, 64, sp
FILTER_VER_LUMA_AVX2_NxN 64, 32, sp
FILTER_VER_LUMA_AVX2_NxN 64, 48, sp
FILTER_VER_LUMA_AVX2_NxN 64, 64, sp
FILTER_VER_LUMA_AVX2_NxN 16, 32, ss
FILTER_VER_LUMA_AVX2_NxN 16, 64, ss
FILTER_VER_LUMA_AVX2_NxN 24, 32, ss
FILTER_VER_LUMA_AVX2_NxN 32, 32, ss
FILTER_VER_LUMA_AVX2_NxN 32, 64, ss
FILTER_VER_LUMA_AVX2_NxN 48, 64, ss
FILTER_VER_LUMA_AVX2_NxN 64, 32, ss
FILTER_VER_LUMA_AVX2_NxN 64, 48, ss
FILTER_VER_LUMA_AVX2_NxN 64, 64, ss

%macro FILTER_VER_LUMA_AVX2_8xN 2
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_vert_%1_8x%2, 4, 9, 15
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m14, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m14, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m14, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]
    lea             r7, [r1 * 4]
    mov             r8d, %2 / 16
.loopH:
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    paddd           m1, m5
    pmaddwd         m3, [r5]
    movu            xm5, [r0 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 2 * mmsize]
    paddd           m0, m6
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    paddd           m2, m6
    pmaddwd         m4, [r5]
    movu            xm6, [r0 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 2 * mmsize]
    paddd           m1, m7
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    paddd           m3, m7
    pmaddwd         m5, [r5]
    movu            xm7, [r0 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 3 * mmsize]
    paddd           m0, m8
    pmaddwd         m8, m6, [r5 + 2 * mmsize]
    paddd           m2, m8
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    paddd           m4, m8
    pmaddwd         m6, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm8, [r0]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m9, m7, [r5 + 3 * mmsize]
    paddd           m1, m9
    pmaddwd         m9, m7, [r5 + 2 * mmsize]
    paddd           m3, m9
    pmaddwd         m9, m7, [r5 + 1 * mmsize]
    paddd           m5, m9
    pmaddwd         m7, [r5]
    movu            xm9, [r0 + r1]                  ; m9 = row 9
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m10, m8, [r5 + 3 * mmsize]
    paddd           m2, m10
    pmaddwd         m10, m8, [r5 + 2 * mmsize]
    paddd           m4, m10
    pmaddwd         m10, m8, [r5 + 1 * mmsize]
    paddd           m6, m10
    pmaddwd         m8, [r5]
    movu            xm10, [r0 + r1 * 2]             ; m10 = row 10
    punpckhwd       xm11, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm11, 1
    pmaddwd         m11, m9, [r5 + 3 * mmsize]
    paddd           m3, m11
    pmaddwd         m11, m9, [r5 + 2 * mmsize]
    paddd           m5, m11
    pmaddwd         m11, m9, [r5 + 1 * mmsize]
    paddd           m7, m11
    pmaddwd         m9, [r5]
    movu            xm11, [r0 + r4]                 ; m11 = row 11
    punpckhwd       xm12, xm10, xm11
    punpcklwd       xm10, xm11
    vinserti128     m10, m10, xm12, 1
    pmaddwd         m12, m10, [r5 + 3 * mmsize]
    paddd           m4, m12
    pmaddwd         m12, m10, [r5 + 2 * mmsize]
    paddd           m6, m12
    pmaddwd         m12, m10, [r5 + 1 * mmsize]
    paddd           m8, m12
    pmaddwd         m10, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm12, [r0]                      ; m12 = row 12
    punpckhwd       xm13, xm11, xm12
    punpcklwd       xm11, xm12
    vinserti128     m11, m11, xm13, 1
    pmaddwd         m13, m11, [r5 + 3 * mmsize]
    paddd           m5, m13
    pmaddwd         m13, m11, [r5 + 2 * mmsize]
    paddd           m7, m13
    pmaddwd         m13, m11, [r5 + 1 * mmsize]
    paddd           m9, m13
    pmaddwd         m11, [r5]

%ifidn %1,ss
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
    psrad           m4, 6
    psrad           m5, 6
%else
    paddd           m0, m14
    paddd           m1, m14
    paddd           m2, m14
    paddd           m3, m14
    paddd           m4, m14
    paddd           m5, m14
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
    psrad           m3, INTERP_SHIFT_PP
    psrad           m4, INTERP_SHIFT_PP
    psrad           m5, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
    psrad           m4, INTERP_SHIFT_SP
    psrad           m5, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
    psrad           m4, INTERP_SHIFT_PS
    psrad           m5, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m1
    packssdw        m2, m3
    packssdw        m4, m5
    vpermq          m0, m0, 11011000b
    vpermq          m2, m2, 11011000b
    vpermq          m4, m4, 11011000b
    pxor            m5, m5
    mova            m3, [pw_pixel_max]
%ifidn %1,pp
    CLIPW           m0, m5, m3
    CLIPW           m2, m5, m3
    CLIPW           m4, m5, m3
%elifidn %1, sp
    CLIPW           m0, m5, m3
    CLIPW           m2, m5, m3
    CLIPW           m4, m5, m3
%endif

    vextracti128    xm1, m0, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    vextracti128    xm1, m2, 1
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r6], xm1
    lea             r2, [r2 + r3 * 4]
    vextracti128    xm1, m4, 1
    movu            [r2], xm4
    movu            [r2 + r3], xm1

    movu            xm13, [r0 + r1]                 ; m13 = row 13
    punpckhwd       xm0, xm12, xm13
    punpcklwd       xm12, xm13
    vinserti128     m12, m12, xm0, 1
    pmaddwd         m0, m12, [r5 + 3 * mmsize]
    paddd           m6, m0
    pmaddwd         m0, m12, [r5 + 2 * mmsize]
    paddd           m8, m0
    pmaddwd         m0, m12, [r5 + 1 * mmsize]
    paddd           m10, m0
    pmaddwd         m12, [r5]
    movu            xm0, [r0 + r1 * 2]              ; m0 = row 14
    punpckhwd       xm1, xm13, xm0
    punpcklwd       xm13, xm0
    vinserti128     m13, m13, xm1, 1
    pmaddwd         m1, m13, [r5 + 3 * mmsize]
    paddd           m7, m1
    pmaddwd         m1, m13, [r5 + 2 * mmsize]
    paddd           m9, m1
    pmaddwd         m1, m13, [r5 + 1 * mmsize]
    paddd           m11, m1
    pmaddwd         m13, [r5]

%ifidn %1,ss
    psrad           m6, 6
    psrad           m7, 6
%else
    paddd           m6, m14
    paddd           m7, m14
%ifidn %1,pp
    psrad           m6, INTERP_SHIFT_PP
    psrad           m7, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m6, INTERP_SHIFT_SP
    psrad           m7, INTERP_SHIFT_SP
%else
    psrad           m6, INTERP_SHIFT_PS
    psrad           m7, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m6, m7
    vpermq          m6, m6, 11011000b
%ifidn %1,pp
    CLIPW           m6, m5, m3
%elifidn %1, sp
    CLIPW           m6, m5, m3
%endif
    vextracti128    xm7, m6, 1
    movu            [r2 + r3 * 2], xm6
    movu            [r2 + r6], xm7

    movu            xm1, [r0 + r4]                  ; m1 = row 15
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m2, m0, [r5 + 3 * mmsize]
    paddd           m8, m2
    pmaddwd         m2, m0, [r5 + 2 * mmsize]
    paddd           m10, m2
    pmaddwd         m2, m0, [r5 + 1 * mmsize]
    paddd           m12, m2
    pmaddwd         m0, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm2, [r0]                       ; m2 = row 16
    punpckhwd       xm6, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm6, 1
    pmaddwd         m6, m1, [r5 + 3 * mmsize]
    paddd           m9, m6
    pmaddwd         m6, m1, [r5 + 2 * mmsize]
    paddd           m11, m6
    pmaddwd         m6, m1, [r5 + 1 * mmsize]
    paddd           m13, m6
    pmaddwd         m1, [r5]
    movu            xm6, [r0 + r1]                  ; m6 = row 17
    punpckhwd       xm4, xm2, xm6
    punpcklwd       xm2, xm6
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 3 * mmsize]
    paddd           m10, m4
    pmaddwd         m4, m2, [r5 + 2 * mmsize]
    paddd           m12, m4
    pmaddwd         m2, [r5 + 1 * mmsize]
    paddd           m0, m2
    movu            xm4, [r0 + r1 * 2]              ; m4 = row 18
    punpckhwd       xm2, xm6, xm4
    punpcklwd       xm6, xm4
    vinserti128     m6, m6, xm2, 1
    pmaddwd         m2, m6, [r5 + 3 * mmsize]
    paddd           m11, m2
    pmaddwd         m2, m6, [r5 + 2 * mmsize]
    paddd           m13, m2
    pmaddwd         m6, [r5 + 1 * mmsize]
    paddd           m1, m6
    movu            xm2, [r0 + r4]                  ; m2 = row 19
    punpckhwd       xm6, xm4, xm2
    punpcklwd       xm4, xm2
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 3 * mmsize]
    paddd           m12, m6
    pmaddwd         m4, [r5 + 2 * mmsize]
    paddd           m0, m4
    lea             r0, [r0 + r1 * 4]
    movu            xm6, [r0]                       ; m6 = row 20
    punpckhwd       xm7, xm2, xm6
    punpcklwd       xm2, xm6
    vinserti128     m2, m2, xm7, 1
    pmaddwd         m7, m2, [r5 + 3 * mmsize]
    paddd           m13, m7
    pmaddwd         m2, [r5 + 2 * mmsize]
    paddd           m1, m2
    movu            xm7, [r0 + r1]                  ; m7 = row 21
    punpckhwd       xm2, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm2, 1
    pmaddwd         m6, [r5 + 3 * mmsize]
    paddd           m0, m6
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 22
    punpckhwd       xm6, xm7, xm2
    punpcklwd       xm7, xm2
    vinserti128     m7, m7, xm6, 1
    pmaddwd         m7, [r5 + 3 * mmsize]
    paddd           m1, m7

%ifidn %1,ss
    psrad           m8, 6
    psrad           m9, 6
    psrad           m10, 6
    psrad           m11, 6
    psrad           m12, 6
    psrad           m13, 6
    psrad           m0, 6
    psrad           m1, 6
%else
    paddd           m8, m14
    paddd           m9, m14
    paddd           m10, m14
    paddd           m11, m14
    paddd           m12, m14
    paddd           m13, m14
    paddd           m0, m14
    paddd           m1, m14
%ifidn %1,pp
    psrad           m8, INTERP_SHIFT_PP
    psrad           m9, INTERP_SHIFT_PP
    psrad           m10, INTERP_SHIFT_PP
    psrad           m11, INTERP_SHIFT_PP
    psrad           m12, INTERP_SHIFT_PP
    psrad           m13, INTERP_SHIFT_PP
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m8, INTERP_SHIFT_SP
    psrad           m9, INTERP_SHIFT_SP
    psrad           m10, INTERP_SHIFT_SP
    psrad           m11, INTERP_SHIFT_SP
    psrad           m12, INTERP_SHIFT_SP
    psrad           m13, INTERP_SHIFT_SP
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
%else
    psrad           m8, INTERP_SHIFT_PS
    psrad           m9, INTERP_SHIFT_PS
    psrad           m10, INTERP_SHIFT_PS
    psrad           m11, INTERP_SHIFT_PS
    psrad           m12, INTERP_SHIFT_PS
    psrad           m13, INTERP_SHIFT_PS
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m8, m9
    packssdw        m10, m11
    packssdw        m12, m13
    packssdw        m0, m1
    vpermq          m8, m8, 11011000b
    vpermq          m10, m10, 11011000b
    vpermq          m12, m12, 11011000b
    vpermq          m0, m0, 11011000b
%ifidn %1,pp
    CLIPW           m8, m5, m3
    CLIPW           m10, m5, m3
    CLIPW           m12, m5, m3
    CLIPW           m0, m5, m3
%elifidn %1, sp
    CLIPW           m8, m5, m3
    CLIPW           m10, m5, m3
    CLIPW           m12, m5, m3
    CLIPW           m0, m5, m3
%endif
    vextracti128    xm9, m8, 1
    vextracti128    xm11, m10, 1
    vextracti128    xm13, m12, 1
    vextracti128    xm1, m0, 1
    lea             r2, [r2 + r3 * 4]
    movu            [r2], xm8
    movu            [r2 + r3], xm9
    movu            [r2 + r3 * 2], xm10
    movu            [r2 + r6], xm11
    lea             r2, [r2 + r3 * 4]
    movu            [r2], xm12
    movu            [r2 + r3], xm13
    movu            [r2 + r3 * 2], xm0
    movu            [r2 + r6], xm1
    lea             r2, [r2 + r3 * 4]
    sub             r0, r7
    dec             r8d
    jnz             .loopH
    RET
%endif
%endmacro

FILTER_VER_LUMA_AVX2_8xN pp, 16
FILTER_VER_LUMA_AVX2_8xN pp, 32
FILTER_VER_LUMA_AVX2_8xN ps, 16
FILTER_VER_LUMA_AVX2_8xN ps, 32
FILTER_VER_LUMA_AVX2_8xN sp, 16
FILTER_VER_LUMA_AVX2_8xN sp, 32
FILTER_VER_LUMA_AVX2_8xN ss, 16
FILTER_VER_LUMA_AVX2_8xN ss, 32

%macro PROCESS_LUMA_AVX2_W8_8R 1
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5]
    lea             r7, [r0 + r1 * 4]
    movu            xm4, [r7]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    paddd           m1, m5
    pmaddwd         m3, [r5]
    movu            xm5, [r7 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 2 * mmsize]
    paddd           m0, m6
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    paddd           m2, m6
    pmaddwd         m4, [r5]
    movu            xm6, [r7 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 2 * mmsize]
    paddd           m1, m7
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    paddd           m3, m7
    pmaddwd         m5, [r5]
    movu            xm7, [r7 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 3 * mmsize]
    paddd           m0, m8
    pmaddwd         m8, m6, [r5 + 2 * mmsize]
    paddd           m2, m8
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    paddd           m4, m8
    pmaddwd         m6, [r5]
    lea             r7, [r7 + r1 * 4]
    movu            xm8, [r7]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m9, m7, [r5 + 3 * mmsize]
    paddd           m1, m9
    pmaddwd         m9, m7, [r5 + 2 * mmsize]
    paddd           m3, m9
    pmaddwd         m9, m7, [r5 + 1 * mmsize]
    paddd           m5, m9
    pmaddwd         m7, [r5]
    movu            xm9, [r7 + r1]                  ; m9 = row 9
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m10, m8, [r5 + 3 * mmsize]
    paddd           m2, m10
    pmaddwd         m10, m8, [r5 + 2 * mmsize]
    paddd           m4, m10
    pmaddwd         m8, [r5 + 1 * mmsize]
    paddd           m6, m8
    movu            xm10, [r7 + r1 * 2]             ; m10 = row 10
    punpckhwd       xm8, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm8, 1
    pmaddwd         m8, m9, [r5 + 3 * mmsize]
    paddd           m3, m8
    pmaddwd         m8, m9, [r5 + 2 * mmsize]
    paddd           m5, m8
    pmaddwd         m9, [r5 + 1 * mmsize]
    paddd           m7, m9
    movu            xm8, [r7 + r4]                  ; m8 = row 11
    punpckhwd       xm9, xm10, xm8
    punpcklwd       xm10, xm8
    vinserti128     m10, m10, xm9, 1
    pmaddwd         m9, m10, [r5 + 3 * mmsize]
    paddd           m4, m9
    pmaddwd         m10, [r5 + 2 * mmsize]
    paddd           m6, m10
    lea             r7, [r7 + r1 * 4]
    movu            xm9, [r7]                       ; m9 = row 12
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m10, m8, [r5 + 3 * mmsize]
    paddd           m5, m10
    pmaddwd         m8, [r5 + 2 * mmsize]
    paddd           m7, m8

%ifidn %1,ss
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
    psrad           m4, 6
    psrad           m5, 6
%else
    paddd           m0, m11
    paddd           m1, m11
    paddd           m2, m11
    paddd           m3, m11
    paddd           m4, m11
    paddd           m5, m11
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
    psrad           m3, INTERP_SHIFT_PP
    psrad           m4, INTERP_SHIFT_PP
    psrad           m5, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
    psrad           m4, INTERP_SHIFT_SP
    psrad           m5, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
    psrad           m4, INTERP_SHIFT_PS
    psrad           m5, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m1
    packssdw        m2, m3
    packssdw        m4, m5
    vpermq          m0, m0, 11011000b
    vpermq          m2, m2, 11011000b
    vpermq          m4, m4, 11011000b
    pxor            m8, m8
%ifidn %1,pp
    CLIPW           m0, m8, m12
    CLIPW           m2, m8, m12
    CLIPW           m4, m8, m12
%elifidn %1, sp
    CLIPW           m0, m8, m12
    CLIPW           m2, m8, m12
    CLIPW           m4, m8, m12
%endif

    vextracti128    xm1, m0, 1
    vextracti128    xm3, m2, 1
    vextracti128    xm5, m4, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r6], xm3
    lea             r8, [r2 + r3 * 4]
    movu            [r8], xm4
    movu            [r8 + r3], xm5

    movu            xm10, [r7 + r1]                 ; m10 = row 13
    punpckhwd       xm0, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm0, 1
    pmaddwd         m9, [r5 + 3 * mmsize]
    paddd           m6, m9
    movu            xm0, [r7 + r1 * 2]              ; m0 = row 14
    punpckhwd       xm1, xm10, xm0
    punpcklwd       xm10, xm0
    vinserti128     m10, m10, xm1, 1
    pmaddwd         m10, [r5 + 3 * mmsize]
    paddd           m7, m10

%ifidn %1,ss
    psrad           m6, 6
    psrad           m7, 6
%else
    paddd           m6, m11
    paddd           m7, m11
%ifidn %1,pp
    psrad           m6, INTERP_SHIFT_PP
    psrad           m7, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m6, INTERP_SHIFT_SP
    psrad           m7, INTERP_SHIFT_SP
%else
    psrad           m6, INTERP_SHIFT_PS
    psrad           m7, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m6, m7
    vpermq          m6, m6, 11011000b
%ifidn %1,pp
    CLIPW           m6, m8, m12
%elifidn %1, sp
    CLIPW           m6, m8, m12
%endif
    vextracti128    xm7, m6, 1
    movu            [r8 + r3 * 2], xm6
    movu            [r8 + r6], xm7
%endmacro

%macro FILTER_VER_LUMA_AVX2_Nx8 2
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_vert_%1_%2x8, 4, 10, 13
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m11, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m11, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m11, [INTERP_OFFSET_PS]
%endif
    mova            m12, [pw_pixel_max]
    lea             r6, [r3 * 3]
    mov             r9d, %2 / 8
.loopW:
    PROCESS_LUMA_AVX2_W8_8R %1
    add             r2, 16
    add             r0, 16
    dec             r9d
    jnz             .loopW
    RET
%endif
%endmacro

FILTER_VER_LUMA_AVX2_Nx8 pp, 32
FILTER_VER_LUMA_AVX2_Nx8 pp, 16
FILTER_VER_LUMA_AVX2_Nx8 ps, 32
FILTER_VER_LUMA_AVX2_Nx8 ps, 16
FILTER_VER_LUMA_AVX2_Nx8 sp, 32
FILTER_VER_LUMA_AVX2_Nx8 sp, 16
FILTER_VER_LUMA_AVX2_Nx8 ss, 32
FILTER_VER_LUMA_AVX2_Nx8 ss, 16

%macro FILTER_VER_LUMA_AVX2_32x24 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_vert_%1_32x24, 4, 10, 15
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m14, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m14, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m14, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]
    mov             r9d, 4
.loopW:
    PROCESS_LUMA_AVX2_W8_16R %1
    add             r2, 16
    add             r0, 16
    dec             r9d
    jnz             .loopW
    lea             r9, [r1 * 4]
    sub             r7, r9
    lea             r0, [r7 - 48]
    lea             r2, [r8 + r3 * 4 - 48]
    mova            m11, m14
    mova            m12, m3
    mov             r9d, 4
.loop:
    PROCESS_LUMA_AVX2_W8_8R %1
    add             r2, 16
    add             r0, 16
    dec             r9d
    jnz             .loop
    RET
%endif
%endmacro

FILTER_VER_LUMA_AVX2_32x24 pp
FILTER_VER_LUMA_AVX2_32x24 ps
FILTER_VER_LUMA_AVX2_32x24 sp
FILTER_VER_LUMA_AVX2_32x24 ss

%macro PROCESS_LUMA_AVX2_W8_4R 1
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    paddd           m1, m5
    pmaddwd         m3, [r5]
    movu            xm5, [r0 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 2 * mmsize]
    paddd           m0, m6
    pmaddwd         m4, [r5 + 1 * mmsize]
    paddd           m2, m4
    movu            xm6, [r0 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm4, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm4, 1
    pmaddwd         m4, m5, [r5 + 2 * mmsize]
    paddd           m1, m4
    pmaddwd         m5, [r5 + 1 * mmsize]
    paddd           m3, m5
    movu            xm4, [r0 + r4]                  ; m4 = row 7
    punpckhwd       xm5, xm6, xm4
    punpcklwd       xm6, xm4
    vinserti128     m6, m6, xm5, 1
    pmaddwd         m5, m6, [r5 + 3 * mmsize]
    paddd           m0, m5
    pmaddwd         m6, [r5 + 2 * mmsize]
    paddd           m2, m6
    lea             r0, [r0 + r1 * 4]
    movu            xm5, [r0]                       ; m5 = row 8
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 3 * mmsize]
    paddd           m1, m6
    pmaddwd         m4, [r5 + 2 * mmsize]
    paddd           m3, m4
    movu            xm6, [r0 + r1]                  ; m6 = row 9
    punpckhwd       xm4, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm4, 1
    pmaddwd         m5, [r5 + 3 * mmsize]
    paddd           m2, m5
    movu            xm4, [r0 + r1 * 2]              ; m4 = row 10
    punpckhwd       xm5, xm6, xm4
    punpcklwd       xm6, xm4
    vinserti128     m6, m6, xm5, 1
    pmaddwd         m6, [r5 + 3 * mmsize]
    paddd           m3, m6

%ifidn %1,ss
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
%else
    paddd           m0, m7
    paddd           m1, m7
    paddd           m2, m7
    paddd           m3, m7
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
    psrad           m3, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m1
    packssdw        m2, m3
    vpermq          m0, m0, 11011000b
    vpermq          m2, m2, 11011000b
    pxor            m4, m4
%ifidn %1,pp
    CLIPW           m0, m4, [pw_pixel_max]
    CLIPW           m2, m4, [pw_pixel_max]
%elifidn %1, sp
    CLIPW           m0, m4, [pw_pixel_max]
    CLIPW           m2, m4, [pw_pixel_max]
%endif

    vextracti128    xm1, m0, 1
    vextracti128    xm3, m2, 1
%endmacro

%macro FILTER_VER_LUMA_AVX2_16x4 1
INIT_YMM avx2
cglobal interp_8tap_vert_%1_16x4, 4, 7, 8, 0-gprsize
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m7, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif
    mov             dword [rsp], 2
.loopW:
    PROCESS_LUMA_AVX2_W8_4R %1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    movu            [r2 + r3 * 2], xm2
    lea             r6, [r3 * 3]
    movu            [r2 + r6], xm3
    add             r2, 16
    lea             r6, [8 * r1 - 16]
    sub             r0, r6
    dec             dword [rsp]
    jnz             .loopW
    RET
%endmacro

FILTER_VER_LUMA_AVX2_16x4 pp
FILTER_VER_LUMA_AVX2_16x4 ps
FILTER_VER_LUMA_AVX2_16x4 sp
FILTER_VER_LUMA_AVX2_16x4 ss

%macro FILTER_VER_LUMA_AVX2_8x4 1
INIT_YMM avx2
cglobal interp_8tap_vert_%1_8x4, 4, 6, 8
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m7, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif

    PROCESS_LUMA_AVX2_W8_4R %1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    movu            [r2 + r3 * 2], xm2
    lea             r4, [r3 * 3]
    movu            [r2 + r4], xm3
    RET
%endmacro

FILTER_VER_LUMA_AVX2_8x4 pp
FILTER_VER_LUMA_AVX2_8x4 ps
FILTER_VER_LUMA_AVX2_8x4 sp
FILTER_VER_LUMA_AVX2_8x4 ss

%macro FILTER_VER_LUMA_AVX2_16x12 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_vert_%1_16x12, 4, 10, 15
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m14, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m14, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m14, [INTERP_OFFSET_PS]
%endif
    mova            m13, [pw_pixel_max]
    pxor            m12, m12
    lea             r6, [r3 * 3]
    mov             r9d, 2
.loopW:
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5]
    lea             r7, [r0 + r1 * 4]
    movu            xm4, [r7]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    paddd           m1, m5
    pmaddwd         m3, [r5]
    movu            xm5, [r7 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 2 * mmsize]
    paddd           m0, m6
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    paddd           m2, m6
    pmaddwd         m4, [r5]
    movu            xm6, [r7 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 2 * mmsize]
    paddd           m1, m7
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    paddd           m3, m7
    pmaddwd         m5, [r5]
    movu            xm7, [r7 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 3 * mmsize]
    paddd           m0, m8
    pmaddwd         m8, m6, [r5 + 2 * mmsize]
    paddd           m2, m8
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    paddd           m4, m8
    pmaddwd         m6, [r5]
    lea             r7, [r7 + r1 * 4]
    movu            xm8, [r7]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m9, m7, [r5 + 3 * mmsize]
    paddd           m1, m9
    pmaddwd         m9, m7, [r5 + 2 * mmsize]
    paddd           m3, m9
    pmaddwd         m9, m7, [r5 + 1 * mmsize]
    paddd           m5, m9
    pmaddwd         m7, [r5]
    movu            xm9, [r7 + r1]                  ; m9 = row 9
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m10, m8, [r5 + 3 * mmsize]
    paddd           m2, m10
    pmaddwd         m10, m8, [r5 + 2 * mmsize]
    paddd           m4, m10
    pmaddwd         m10, m8, [r5 + 1 * mmsize]
    paddd           m6, m10
    pmaddwd         m8, [r5]
    movu            xm10, [r7 + r1 * 2]             ; m10 = row 10
    punpckhwd       xm11, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm11, 1
    pmaddwd         m11, m9, [r5 + 3 * mmsize]
    paddd           m3, m11
    pmaddwd         m11, m9, [r5 + 2 * mmsize]
    paddd           m5, m11
    pmaddwd         m11, m9, [r5 + 1 * mmsize]
    paddd           m7, m11
    pmaddwd         m9, [r5]

%ifidn %1,ss
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
%else
    paddd           m0, m14
    paddd           m1, m14
    paddd           m2, m14
    paddd           m3, m14
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
    psrad           m3, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m1
    packssdw        m2, m3
    vpermq          m0, m0, 11011000b
    vpermq          m2, m2, 11011000b
%ifidn %1,pp
    CLIPW           m0, m12, m13
    CLIPW           m2, m12, m13
%elifidn %1, sp
    CLIPW           m0, m12, m13
    CLIPW           m2, m12, m13
%endif

    vextracti128    xm1, m0, 1
    vextracti128    xm3, m2, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r6], xm3

    movu            xm11, [r7 + r4]                 ; m11 = row 11
    punpckhwd       xm0, xm10, xm11
    punpcklwd       xm10, xm11
    vinserti128     m10, m10, xm0, 1
    pmaddwd         m0, m10, [r5 + 3 * mmsize]
    paddd           m4, m0
    pmaddwd         m0, m10, [r5 + 2 * mmsize]
    paddd           m6, m0
    pmaddwd         m0, m10, [r5 + 1 * mmsize]
    paddd           m8, m0
    pmaddwd         m10, [r5]
    lea             r7, [r7 + r1 * 4]
    movu            xm0, [r7]                      ; m0 = row 12
    punpckhwd       xm1, xm11, xm0
    punpcklwd       xm11, xm0
    vinserti128     m11, m11, xm1, 1
    pmaddwd         m1, m11, [r5 + 3 * mmsize]
    paddd           m5, m1
    pmaddwd         m1, m11, [r5 + 2 * mmsize]
    paddd           m7, m1
    pmaddwd         m1, m11, [r5 + 1 * mmsize]
    paddd           m9, m1
    pmaddwd         m11, [r5]
    movu            xm2, [r7 + r1]                 ; m2 = row 13
    punpckhwd       xm1, xm0, xm2
    punpcklwd       xm0, xm2
    vinserti128     m0, m0, xm1, 1
    pmaddwd         m1, m0, [r5 + 3 * mmsize]
    paddd           m6, m1
    pmaddwd         m1, m0, [r5 + 2 * mmsize]
    paddd           m8, m1
    pmaddwd         m0, [r5 + 1 * mmsize]
    paddd           m10, m0
    movu            xm0, [r7 + r1 * 2]              ; m0 = row 14
    punpckhwd       xm1, xm2, xm0
    punpcklwd       xm2, xm0
    vinserti128     m2, m2, xm1, 1
    pmaddwd         m1, m2, [r5 + 3 * mmsize]
    paddd           m7, m1
    pmaddwd         m1, m2, [r5 + 2 * mmsize]
    paddd           m9, m1
    pmaddwd         m2, [r5 + 1 * mmsize]
    paddd           m11, m2

%ifidn %1,ss
    psrad           m4, 6
    psrad           m5, 6
    psrad           m6, 6
    psrad           m7, 6
%else
    paddd           m4, m14
    paddd           m5, m14
    paddd           m6, m14
    paddd           m7, m14
%ifidn %1,pp
    psrad           m4, INTERP_SHIFT_PP
    psrad           m5, INTERP_SHIFT_PP
    psrad           m6, INTERP_SHIFT_PP
    psrad           m7, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m4, INTERP_SHIFT_SP
    psrad           m5, INTERP_SHIFT_SP
    psrad           m6, INTERP_SHIFT_SP
    psrad           m7, INTERP_SHIFT_SP
%else
    psrad           m4, INTERP_SHIFT_PS
    psrad           m5, INTERP_SHIFT_PS
    psrad           m6, INTERP_SHIFT_PS
    psrad           m7, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m4, m5
    packssdw        m6, m7
    vpermq          m4, m4, 11011000b
    vpermq          m6, m6, 11011000b
%ifidn %1,pp
    CLIPW           m4, m12, m13
    CLIPW           m6, m12, m13
%elifidn %1, sp
    CLIPW           m4, m12, m13
    CLIPW           m6, m12, m13
%endif
    lea             r8, [r2 + r3 * 4]
    vextracti128    xm1, m4, 1
    vextracti128    xm7, m6, 1
    movu            [r8], xm4
    movu            [r8 + r3], xm1
    movu            [r8 + r3 * 2], xm6
    movu            [r8 + r6], xm7

    movu            xm1, [r7 + r4]                  ; m1 = row 15
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m2, m0, [r5 + 3 * mmsize]
    paddd           m8, m2
    pmaddwd         m0, [r5 + 2 * mmsize]
    paddd           m10, m0
    lea             r7, [r7 + r1 * 4]
    movu            xm2, [r7]                       ; m2 = row 16
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m3, m1, [r5 + 3 * mmsize]
    paddd           m9, m3
    pmaddwd         m1, [r5 + 2 * mmsize]
    paddd           m11, m1
    movu            xm3, [r7 + r1]                  ; m3 = row 17
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m2, [r5 + 3 * mmsize]
    paddd           m10, m2
    movu            xm4, [r7 + r1 * 2]              ; m4 = row 18
    punpckhwd       xm2, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm2, 1
    pmaddwd         m3, [r5 + 3 * mmsize]
    paddd           m11, m3

%ifidn %1,ss
    psrad           m8, 6
    psrad           m9, 6
    psrad           m10, 6
    psrad           m11, 6
%else
    paddd           m8, m14
    paddd           m9, m14
    paddd           m10, m14
    paddd           m11, m14
%ifidn %1,pp
    psrad           m8, INTERP_SHIFT_PP
    psrad           m9, INTERP_SHIFT_PP
    psrad           m10, INTERP_SHIFT_PP
    psrad           m11, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m8, INTERP_SHIFT_SP
    psrad           m9, INTERP_SHIFT_SP
    psrad           m10, INTERP_SHIFT_SP
    psrad           m11, INTERP_SHIFT_SP
%else
    psrad           m8, INTERP_SHIFT_PS
    psrad           m9, INTERP_SHIFT_PS
    psrad           m10, INTERP_SHIFT_PS
    psrad           m11, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m8, m9
    packssdw        m10, m11
    vpermq          m8, m8, 11011000b
    vpermq          m10, m10, 11011000b
%ifidn %1,pp
    CLIPW           m8, m12, m13
    CLIPW           m10, m12, m13
%elifidn %1, sp
    CLIPW           m8, m12, m13
    CLIPW           m10, m12, m13
%endif
    vextracti128    xm9, m8, 1
    vextracti128    xm11, m10, 1
    lea             r8, [r8 + r3 * 4]
    movu            [r8], xm8
    movu            [r8 + r3], xm9
    movu            [r8 + r3 * 2], xm10
    movu            [r8 + r6], xm11
    add             r2, 16
    add             r0, 16
    dec             r9d
    jnz             .loopW
    RET
%endif
%endmacro

FILTER_VER_LUMA_AVX2_16x12 pp
FILTER_VER_LUMA_AVX2_16x12 ps
FILTER_VER_LUMA_AVX2_16x12 sp
FILTER_VER_LUMA_AVX2_16x12 ss

%macro FILTER_VER_LUMA_AVX2_4x8 1
INIT_YMM avx2
cglobal interp_8tap_vert_%1_4x8, 4, 7, 8
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4

%ifidn %1,pp
    vbroadcasti128  m7, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]

    movq            xm0, [r0]
    movq            xm1, [r0 + r1]
    punpcklwd       xm0, xm1
    movq            xm2, [r0 + r1 * 2]
    punpcklwd       xm1, xm2
    vinserti128     m0, m0, xm1, 1                  ; m0 = [2 1 1 0]
    pmaddwd         m0, [r5]
    movq            xm3, [r0 + r4]
    punpcklwd       xm2, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm4, [r0]
    punpcklwd       xm3, xm4
    vinserti128     m2, m2, xm3, 1                  ; m2 = [4 3 3 2]
    pmaddwd         m5, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m5
    movq            xm3, [r0 + r1]
    punpcklwd       xm4, xm3
    movq            xm1, [r0 + r1 * 2]
    punpcklwd       xm3, xm1
    vinserti128     m4, m4, xm3, 1                  ; m4 = [6 5 5 4]
    pmaddwd         m5, m4, [r5 + 2 * mmsize]
    paddd           m0, m5
    pmaddwd         m5, m4, [r5 + 1 * mmsize]
    paddd           m2, m5
    pmaddwd         m4, [r5]
    movq            xm3, [r0 + r4]
    punpcklwd       xm1, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm6, [r0]
    punpcklwd       xm3, xm6
    vinserti128     m1, m1, xm3, 1                  ; m1 = [8 7 7 6]
    pmaddwd         m5, m1, [r5 + 3 * mmsize]
    paddd           m0, m5
    pmaddwd         m5, m1, [r5 + 2 * mmsize]
    paddd           m2, m5
    pmaddwd         m5, m1, [r5 + 1 * mmsize]
    paddd           m4, m5
    pmaddwd         m1, [r5]
    movq            xm3, [r0 + r1]
    punpcklwd       xm6, xm3
    movq            xm5, [r0 + 2 * r1]
    punpcklwd       xm3, xm5
    vinserti128     m6, m6, xm3, 1                  ; m6 = [A 9 9 8]
    pmaddwd         m3, m6, [r5 + 3 * mmsize]
    paddd           m2, m3
    pmaddwd         m3, m6, [r5 + 2 * mmsize]
    paddd           m4, m3
    pmaddwd         m6, [r5 + 1 * mmsize]
    paddd           m1, m6

%ifidn %1,ss
    psrad           m0, 6
    psrad           m2, 6
%else
    paddd           m0, m7
    paddd           m2, m7
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m2
    pxor            m6, m6
    mova            m3, [pw_pixel_max]
%ifidn %1,pp
    CLIPW           m0, m6, m3
%elifidn %1, sp
    CLIPW           m0, m6, m3
%endif

    vextracti128    xm2, m0, 1
    movq            [r2], xm0
    movq            [r2 + r3], xm2
    movhps          [r2 + r3 * 2], xm0
    movhps          [r2 + r6], xm2

    movq            xm2, [r0 + r4]
    punpcklwd       xm5, xm2
    lea             r0, [r0 + 4 * r1]
    movq            xm0, [r0]
    punpcklwd       xm2, xm0
    vinserti128     m5, m5, xm2, 1                  ; m5 = [C B B A]
    pmaddwd         m2, m5, [r5 + 3 * mmsize]
    paddd           m4, m2
    pmaddwd         m5, [r5 + 2 * mmsize]
    paddd           m1, m5
    movq            xm2, [r0 + r1]
    punpcklwd       xm0, xm2
    movq            xm5, [r0 + 2 * r1]
    punpcklwd       xm2, xm5
    vinserti128     m0, m0, xm2, 1                  ; m0 = [E D D C]
    pmaddwd         m0, [r5 + 3 * mmsize]
    paddd           m1, m0

%ifidn %1,ss
    psrad           m4, 6
    psrad           m1, 6
%else
    paddd           m4, m7
    paddd           m1, m7
%ifidn %1,pp
    psrad           m4, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m4, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
%else
    psrad           m4, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m4, m1
%ifidn %1,pp
    CLIPW           m4, m6, m3
%elifidn %1, sp
    CLIPW           m4, m6, m3
%endif

    vextracti128    xm1, m4, 1
    lea             r2, [r2 + r3 * 4]
    movq            [r2], xm4
    movq            [r2 + r3], xm1
    movhps          [r2 + r3 * 2], xm4
    movhps          [r2 + r6], xm1
    RET
%endmacro

FILTER_VER_LUMA_AVX2_4x8 pp
FILTER_VER_LUMA_AVX2_4x8 ps
FILTER_VER_LUMA_AVX2_4x8 sp
FILTER_VER_LUMA_AVX2_4x8 ss

%macro PROCESS_LUMA_AVX2_W4_16R 1
    movq            xm0, [r0]
    movq            xm1, [r0 + r1]
    punpcklwd       xm0, xm1
    movq            xm2, [r0 + r1 * 2]
    punpcklwd       xm1, xm2
    vinserti128     m0, m0, xm1, 1                  ; m0 = [2 1 1 0]
    pmaddwd         m0, [r5]
    movq            xm3, [r0 + r4]
    punpcklwd       xm2, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm4, [r0]
    punpcklwd       xm3, xm4
    vinserti128     m2, m2, xm3, 1                  ; m2 = [4 3 3 2]
    pmaddwd         m5, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m5
    movq            xm3, [r0 + r1]
    punpcklwd       xm4, xm3
    movq            xm1, [r0 + r1 * 2]
    punpcklwd       xm3, xm1
    vinserti128     m4, m4, xm3, 1                  ; m4 = [6 5 5 4]
    pmaddwd         m5, m4, [r5 + 2 * mmsize]
    paddd           m0, m5
    pmaddwd         m5, m4, [r5 + 1 * mmsize]
    paddd           m2, m5
    pmaddwd         m4, [r5]
    movq            xm3, [r0 + r4]
    punpcklwd       xm1, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm6, [r0]
    punpcklwd       xm3, xm6
    vinserti128     m1, m1, xm3, 1                  ; m1 = [8 7 7 6]
    pmaddwd         m5, m1, [r5 + 3 * mmsize]
    paddd           m0, m5
    pmaddwd         m5, m1, [r5 + 2 * mmsize]
    paddd           m2, m5
    pmaddwd         m5, m1, [r5 + 1 * mmsize]
    paddd           m4, m5
    pmaddwd         m1, [r5]
    movq            xm3, [r0 + r1]
    punpcklwd       xm6, xm3
    movq            xm5, [r0 + 2 * r1]
    punpcklwd       xm3, xm5
    vinserti128     m6, m6, xm3, 1                  ; m6 = [10 9 9 8]
    pmaddwd         m3, m6, [r5 + 3 * mmsize]
    paddd           m2, m3
    pmaddwd         m3, m6, [r5 + 2 * mmsize]
    paddd           m4, m3
    pmaddwd         m3, m6, [r5 + 1 * mmsize]
    paddd           m1, m3
    pmaddwd         m6, [r5]

%ifidn %1,ss
    psrad           m0, 6
    psrad           m2, 6
%else
    paddd           m0, m7
    paddd           m2, m7
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m2, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m2
    pxor            m3, m3
%ifidn %1,pp
    CLIPW           m0, m3, [pw_pixel_max]
%elifidn %1, sp
    CLIPW           m0, m3, [pw_pixel_max]
%endif

    vextracti128    xm2, m0, 1
    movq            [r2], xm0
    movq            [r2 + r3], xm2
    movhps          [r2 + r3 * 2], xm0
    movhps          [r2 + r6], xm2

    movq            xm2, [r0 + r4]
    punpcklwd       xm5, xm2
    lea             r0, [r0 + 4 * r1]
    movq            xm0, [r0]
    punpcklwd       xm2, xm0
    vinserti128     m5, m5, xm2, 1                  ; m5 = [12 11 11 10]
    pmaddwd         m2, m5, [r5 + 3 * mmsize]
    paddd           m4, m2
    pmaddwd         m2, m5, [r5 + 2 * mmsize]
    paddd           m1, m2
    pmaddwd         m2, m5, [r5 + 1 * mmsize]
    paddd           m6, m2
    pmaddwd         m5, [r5]
    movq            xm2, [r0 + r1]
    punpcklwd       xm0, xm2
    movq            xm3, [r0 + 2 * r1]
    punpcklwd       xm2, xm3
    vinserti128     m0, m0, xm2, 1                  ; m0 = [14 13 13 12]
    pmaddwd         m2, m0, [r5 + 3 * mmsize]
    paddd           m1, m2
    pmaddwd         m2, m0, [r5 + 2 * mmsize]
    paddd           m6, m2
    pmaddwd         m2, m0, [r5 + 1 * mmsize]
    paddd           m5, m2
    pmaddwd         m0, [r5]

%ifidn %1,ss
    psrad           m4, 6
    psrad           m1, 6
%else
    paddd           m4, m7
    paddd           m1, m7
%ifidn %1,pp
    psrad           m4, INTERP_SHIFT_PP
    psrad           m1, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m4, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
%else
    psrad           m4, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m4, m1
    pxor            m2, m2
%ifidn %1,pp
    CLIPW           m4, m2, [pw_pixel_max]
%elifidn %1, sp
    CLIPW           m4, m2, [pw_pixel_max]
%endif

    vextracti128    xm1, m4, 1
    lea             r2, [r2 + r3 * 4]
    movq            [r2], xm4
    movq            [r2 + r3], xm1
    movhps          [r2 + r3 * 2], xm4
    movhps          [r2 + r6], xm1

    movq            xm4, [r0 + r4]
    punpcklwd       xm3, xm4
    lea             r0, [r0 + 4 * r1]
    movq            xm1, [r0]
    punpcklwd       xm4, xm1
    vinserti128     m3, m3, xm4, 1                  ; m3 = [16 15 15 14]
    pmaddwd         m4, m3, [r5 + 3 * mmsize]
    paddd           m6, m4
    pmaddwd         m4, m3, [r5 + 2 * mmsize]
    paddd           m5, m4
    pmaddwd         m4, m3, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m3, [r5]
    movq            xm4, [r0 + r1]
    punpcklwd       xm1, xm4
    movq            xm2, [r0 + 2 * r1]
    punpcklwd       xm4, xm2
    vinserti128     m1, m1, xm4, 1                  ; m1 = [18 17 17 16]
    pmaddwd         m4, m1, [r5 + 3 * mmsize]
    paddd           m5, m4
    pmaddwd         m4, m1, [r5 + 2 * mmsize]
    paddd           m0, m4
    pmaddwd         m1, [r5 + 1 * mmsize]
    paddd           m3, m1

%ifidn %1,ss
    psrad           m6, 6
    psrad           m5, 6
%else
    paddd           m6, m7
    paddd           m5, m7
%ifidn %1,pp
    psrad           m6, INTERP_SHIFT_PP
    psrad           m5, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m6, INTERP_SHIFT_SP
    psrad           m5, INTERP_SHIFT_SP
%else
    psrad           m6, INTERP_SHIFT_PS
    psrad           m5, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m6, m5
    pxor            m1, m1
%ifidn %1,pp
    CLIPW           m6, m1, [pw_pixel_max]
%elifidn %1, sp
    CLIPW           m6, m1, [pw_pixel_max]
%endif

    vextracti128    xm5, m6, 1
    lea             r2, [r2 + r3 * 4]
    movq            [r2], xm6
    movq            [r2 + r3], xm5
    movhps          [r2 + r3 * 2], xm6
    movhps          [r2 + r6], xm5

    movq            xm4, [r0 + r4]
    punpcklwd       xm2, xm4
    lea             r0, [r0 + 4 * r1]
    movq            xm6, [r0]
    punpcklwd       xm4, xm6
    vinserti128     m2, m2, xm4, 1                  ; m2 = [20 19 19 18]
    pmaddwd         m4, m2, [r5 + 3 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5 + 2 * mmsize]
    paddd           m3, m2
    movq            xm4, [r0 + r1]
    punpcklwd       xm6, xm4
    movq            xm2, [r0 + 2 * r1]
    punpcklwd       xm4, xm2
    vinserti128     m6, m6, xm4, 1                  ; m6 = [22 21 21 20]
    pmaddwd         m6, [r5 + 3 * mmsize]
    paddd           m3, m6

%ifidn %1,ss
    psrad           m0, 6
    psrad           m3, 6
%else
    paddd           m0, m7
    paddd           m3, m7
%ifidn %1,pp
    psrad           m0, INTERP_SHIFT_PP
    psrad           m3, INTERP_SHIFT_PP
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m3
%ifidn %1,pp
    CLIPW           m0, m1, [pw_pixel_max]
%elifidn %1, sp
    CLIPW           m0, m1, [pw_pixel_max]
%endif

    vextracti128    xm3, m0, 1
    lea             r2, [r2 + r3 * 4]
    movq            [r2], xm0
    movq            [r2 + r3], xm3
    movhps          [r2 + r3 * 2], xm0
    movhps          [r2 + r6], xm3
%endmacro

%macro FILTER_VER_LUMA_AVX2_4x16 1
INIT_YMM avx2
cglobal interp_8tap_vert_%1_4x16, 4, 7, 8
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m7, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]
    PROCESS_LUMA_AVX2_W4_16R %1
    RET
%endmacro

FILTER_VER_LUMA_AVX2_4x16 pp
FILTER_VER_LUMA_AVX2_4x16 ps
FILTER_VER_LUMA_AVX2_4x16 sp
FILTER_VER_LUMA_AVX2_4x16 ss

%macro FILTER_VER_LUMA_AVX2_12x16 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_vert_%1_12x16, 4, 9, 15
    mov             r4d, r4m
    shl             r4d, 7
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_LumaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_LumaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r4
%ifidn %1,pp
    vbroadcasti128  m14, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m14, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m14, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]
    PROCESS_LUMA_AVX2_W8_16R %1
    add             r2, 16
    add             r0, 16
    mova            m7, m14
    PROCESS_LUMA_AVX2_W4_16R %1
    RET
%endif
%endmacro

FILTER_VER_LUMA_AVX2_12x16 pp
FILTER_VER_LUMA_AVX2_12x16 ps
FILTER_VER_LUMA_AVX2_12x16 sp
FILTER_VER_LUMA_AVX2_12x16 ss

;---------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_ps_%1x%2(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;---------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_LUMA_PS 2
INIT_XMM sse4
cglobal interp_8tap_vert_ps_%1x%2, 5, 7, 8 ,0-gprsize

    add       r1d, r1d
    add       r3d, r3d
    lea       r5, [r1 + 2 * r1]
    sub       r0, r5
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_LumaCoeffV]
    lea       r6, [r5 + r4]
%else
    lea       r6, [tab_LumaCoeffV + r4]
%endif

    mova      m7, [INTERP_OFFSET_PS]

    mov       dword [rsp], %2/4
.loopH:
    mov       r4d, (%1/4)
.loopW:
    PROCESS_LUMA_VER_W4_4R

    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7

    psrad     m0, INTERP_SHIFT_PS
    psrad     m1, INTERP_SHIFT_PS
    psrad     m2, INTERP_SHIFT_PS
    psrad     m3, INTERP_SHIFT_PS

    packssdw  m0, m1
    packssdw  m2, m3

    movh      [r2], m0
    movhps    [r2 + r3], m0
    lea       r5, [r2 + 2 * r3]
    movh      [r5], m2
    movhps    [r5 + r3], m2

    lea       r5, [8 * r1 - 2 * 4]
    sub       r0, r5
    add       r2, 2 * 4

    dec       r4d
    jnz       .loopW

    lea       r0, [r0 + 4 * r1 - 2 * %1]
    lea       r2, [r2 + 4 * r3 - 2 * %1]

    dec       dword [rsp]
    jnz       .loopH
    RET
%endmacro

;---------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_ps_%1x%2(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;---------------------------------------------------------------------------------------------------------------
    FILTER_VER_LUMA_PS 4, 4
    FILTER_VER_LUMA_PS 8, 8
    FILTER_VER_LUMA_PS 8, 4
    FILTER_VER_LUMA_PS 4, 8
    FILTER_VER_LUMA_PS 16, 16
    FILTER_VER_LUMA_PS 16, 8
    FILTER_VER_LUMA_PS 8, 16
    FILTER_VER_LUMA_PS 16, 12
    FILTER_VER_LUMA_PS 12, 16
    FILTER_VER_LUMA_PS 16, 4
    FILTER_VER_LUMA_PS 4, 16
    FILTER_VER_LUMA_PS 32, 32
    FILTER_VER_LUMA_PS 32, 16
    FILTER_VER_LUMA_PS 16, 32
    FILTER_VER_LUMA_PS 32, 24
    FILTER_VER_LUMA_PS 24, 32
    FILTER_VER_LUMA_PS 32, 8
    FILTER_VER_LUMA_PS 8, 32
    FILTER_VER_LUMA_PS 64, 64
    FILTER_VER_LUMA_PS 64, 32
    FILTER_VER_LUMA_PS 32, 64
    FILTER_VER_LUMA_PS 64, 48
    FILTER_VER_LUMA_PS 48, 64
    FILTER_VER_LUMA_PS 64, 16
    FILTER_VER_LUMA_PS 16, 64

;--------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_sp_%1x%2(int16_t *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;--------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_LUMA_SP 2
INIT_XMM sse4
cglobal interp_8tap_vert_sp_%1x%2, 5, 7, 8 ,0-gprsize

    add       r1d, r1d
    add       r3d, r3d
    lea       r5, [r1 + 2 * r1]
    sub       r0, r5
    shl       r4d, 6

%ifdef PIC
    lea       r5, [tab_LumaCoeffV]
    lea       r6, [r5 + r4]
%else
    lea       r6, [tab_LumaCoeffV + r4]
%endif

    mova      m7, [INTERP_OFFSET_SP]

    mov       dword [rsp], %2/4
.loopH:
    mov       r4d, (%1/4)
.loopW:
    PROCESS_LUMA_VER_W4_4R

    paddd     m0, m7
    paddd     m1, m7
    paddd     m2, m7
    paddd     m3, m7

    psrad     m0, INTERP_SHIFT_SP
    psrad     m1, INTERP_SHIFT_SP
    psrad     m2, INTERP_SHIFT_SP
    psrad     m3, INTERP_SHIFT_SP

    packssdw  m0, m1
    packssdw  m2, m3

    pxor      m1, m1
    CLIPW2    m0, m2, m1, [pw_pixel_max]

    movh      [r2], m0
    movhps    [r2 + r3], m0
    lea       r5, [r2 + 2 * r3]
    movh      [r5], m2
    movhps    [r5 + r3], m2

    lea       r5, [8 * r1 - 2 * 4]
    sub       r0, r5
    add       r2, 2 * 4

    dec       r4d
    jnz       .loopW

    lea       r0, [r0 + 4 * r1 - 2 * %1]
    lea       r2, [r2 + 4 * r3 - 2 * %1]

    dec       dword [rsp]
    jnz       .loopH
    RET
%endmacro

;--------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_sp_%1x%2(int16_t *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int coeffIdx)
;--------------------------------------------------------------------------------------------------------------
    FILTER_VER_LUMA_SP 4, 4
    FILTER_VER_LUMA_SP 8, 8
    FILTER_VER_LUMA_SP 8, 4
    FILTER_VER_LUMA_SP 4, 8
    FILTER_VER_LUMA_SP 16, 16
    FILTER_VER_LUMA_SP 16, 8
    FILTER_VER_LUMA_SP 8, 16
    FILTER_VER_LUMA_SP 16, 12
    FILTER_VER_LUMA_SP 12, 16
    FILTER_VER_LUMA_SP 16, 4
    FILTER_VER_LUMA_SP 4, 16
    FILTER_VER_LUMA_SP 32, 32
    FILTER_VER_LUMA_SP 32, 16
    FILTER_VER_LUMA_SP 16, 32
    FILTER_VER_LUMA_SP 32, 24
    FILTER_VER_LUMA_SP 24, 32
    FILTER_VER_LUMA_SP 32, 8
    FILTER_VER_LUMA_SP 8, 32
    FILTER_VER_LUMA_SP 64, 64
    FILTER_VER_LUMA_SP 64, 32
    FILTER_VER_LUMA_SP 32, 64
    FILTER_VER_LUMA_SP 64, 48
    FILTER_VER_LUMA_SP 48, 64
    FILTER_VER_LUMA_SP 64, 16
    FILTER_VER_LUMA_SP 16, 64

;-----------------------------------------------------------------------------------------------------------------
; void interp_8tap_vert_ss_%1x%2(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride, int coeffIdx)
;-----------------------------------------------------------------------------------------------------------------
%macro FILTER_VER_LUMA_SS 2
INIT_XMM sse2
cglobal interp_8tap_vert_ss_%1x%2, 5, 7, 7 ,0-gprsize

    add        r1d, r1d
    add        r3d, r3d
    lea        r5, [3 * r1]
    sub        r0, r5
    shl        r4d, 6

%ifdef PIC
    lea        r5, [tab_LumaCoeffV]
    lea        r6, [r5 + r4]
%else
    lea        r6, [tab_LumaCoeffV + r4]
%endif

    mov        dword [rsp], %2/4
.loopH:
    mov        r4d, (%1/4)
.loopW:
    PROCESS_LUMA_VER_W4_4R

    psrad      m0, 6
    psrad      m1, 6
    packssdw   m0, m1
    movlps     [r2], m0
    movhps     [r2 + r3], m0

    psrad      m2, 6
    psrad      m3, 6
    packssdw   m2, m3
    movlps     [r2 + 2 * r3], m2
    lea        r5, [3 * r3]
    movhps     [r2 + r5], m2

    lea        r5, [8 * r1 - 2 * 4]
    sub        r0, r5
    add        r2, 2 * 4

    dec        r4d
    jnz        .loopW

    lea        r0, [r0 + 4 * r1 - 2 * %1]
    lea        r2, [r2 + 4 * r3 - 2 * %1]

    dec        dword [rsp]
    jnz        .loopH
    RET
%endmacro

    FILTER_VER_LUMA_SS 4, 4
    FILTER_VER_LUMA_SS 8, 8
    FILTER_VER_LUMA_SS 8, 4
    FILTER_VER_LUMA_SS 4, 8
    FILTER_VER_LUMA_SS 16, 16
    FILTER_VER_LUMA_SS 16, 8
    FILTER_VER_LUMA_SS 8, 16
    FILTER_VER_LUMA_SS 16, 12
    FILTER_VER_LUMA_SS 12, 16
    FILTER_VER_LUMA_SS 16, 4
    FILTER_VER_LUMA_SS 4, 16
    FILTER_VER_LUMA_SS 32, 32
    FILTER_VER_LUMA_SS 32, 16
    FILTER_VER_LUMA_SS 16, 32
    FILTER_VER_LUMA_SS 32, 24
    FILTER_VER_LUMA_SS 24, 32
    FILTER_VER_LUMA_SS 32, 8
    FILTER_VER_LUMA_SS 8, 32
    FILTER_VER_LUMA_SS 64, 64
    FILTER_VER_LUMA_SS 64, 32
    FILTER_VER_LUMA_SS 32, 64
    FILTER_VER_LUMA_SS 64, 48
    FILTER_VER_LUMA_SS 48, 64
    FILTER_VER_LUMA_SS 64, 16
    FILTER_VER_LUMA_SS 16, 64

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_2xN 1
INIT_XMM sse4
cglobal filterPixelToShort_2x%1, 3, 6, 2
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r1 * 3]
    lea        r5, [r3 * 3]

    ; load constant
    mova       m1, [pw_2000]

%rep %1/4
    movd       m0, [r0]
    movhps     m0, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m1

    movd       [r2 + r3 * 0], m0
    pextrd     [r2 + r3 * 1], m0, 2

    movd       m0, [r0 + r1 * 2]
    movhps     m0, [r0 + r4]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m1

    movd       [r2 + r3 * 2], m0
    pextrd     [r2 + r5], m0, 2

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]
%endrep
    RET
%endmacro
P2S_H_2xN 4
P2S_H_2xN 8
P2S_H_2xN 16

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_4xN 1
INIT_XMM ssse3
cglobal filterPixelToShort_4x%1, 3, 6, 2
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load constant
    mova       m1, [pw_2000]

%rep %1/4
    movh       m0, [r0]
    movhps     m0, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m1
    movh       [r2 + r3 * 0], m0
    movhps     [r2 + r3 * 1], m0

    movh       m0, [r0 + r1 * 2]
    movhps     m0, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m1
    movh       [r2 + r3 * 2], m0
    movhps     [r2 + r4], m0

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]
%endrep
    RET
%endmacro
P2S_H_4xN 4
P2S_H_4xN 8
P2S_H_4xN 16
P2S_H_4xN 32

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
INIT_XMM ssse3
cglobal filterPixelToShort_4x2, 3, 4, 1
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d

    movh       m0, [r0]
    movhps     m0, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, [pw_2000]
    movh       [r2 + r3 * 0], m0
    movhps     [r2 + r3 * 1], m0
    RET

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_6xN 1
INIT_XMM sse4
cglobal filterPixelToShort_6x%1, 3, 7, 3
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m2, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movh       [r2 + r3 * 0], m0
    pextrd     [r2 + r3 * 0 + 8], m0, 2
    movh       [r2 + r3 * 1], m1
    pextrd     [r2 + r3 * 1 + 8], m1, 2

    movu       m0, [r0 + r1 * 2]
    movu       m1, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movh       [r2 + r3 * 2], m0
    pextrd     [r2 + r3 * 2 + 8], m0, 2
    movh       [r2 + r4], m1
    pextrd     [r2 + r4 + 8], m1, 2

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_6xN 8
P2S_H_6xN 16

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_8xN 1
INIT_XMM ssse3
cglobal filterPixelToShort_8x%1, 3, 7, 2
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m1, [pw_2000]

.loop
    movu       m0, [r0]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m1
    movu       [r2 + r3 * 0], m0

    movu       m0, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m1
    movu       [r2 + r3 * 1], m0

    movu       m0, [r0 + r1 * 2]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m1
    movu       [r2 + r3 * 2], m0

    movu       m0, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m1
    movu       [r2 + r4], m0

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_8xN 8
P2S_H_8xN 4
P2S_H_8xN 16
P2S_H_8xN 32
P2S_H_8xN 12
P2S_H_8xN 64

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
INIT_XMM ssse3
cglobal filterPixelToShort_8x2, 3, 4, 2
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d

    movu       m0, [r0]
    movu       m1, [r0 + r1]

    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, [pw_2000]
    psubw      m1, [pw_2000]

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1
    RET

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
INIT_XMM ssse3
cglobal filterPixelToShort_8x6, 3, 7, 4
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r1 * 3]
    lea        r5, [r1 * 5]
    lea        r6, [r3 * 3]

    ; load constant
    mova       m3, [pw_2000]

    movu       m0, [r0]
    movu       m1, [r0 + r1]
    movu       m2, [r0 + r1 * 2]

    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m3
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m3
    psllw      m2, (14 - BIT_DEPTH)
    psubw      m2, m3

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1
    movu       [r2 + r3 * 2], m2

    movu       m0, [r0 + r4]
    movu       m1, [r0 + r1 * 4]
    movu       m2, [r0 + r5 ]

    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m3
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m3
    psllw      m2, (14 - BIT_DEPTH)
    psubw      m2, m3

    movu       [r2 + r6], m0
    movu       [r2 + r3 * 4], m1
    lea        r2, [r2 + r3 * 4]
    movu       [r2 + r3], m2
    RET

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_16xN 1
INIT_XMM ssse3
cglobal filterPixelToShort_16x%1, 3, 7, 3
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m2, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m2

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1

    movu       m0, [r0 + r1 * 2]
    movu       m1, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m2

    movu       [r2 + r3 * 2], m0
    movu       [r2 + r4], m1

    movu       m0, [r0 + 16]
    movu       m1, [r0 + r1 + 16]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m2

    movu       [r2 + r3 * 0 + 16], m0
    movu       [r2 + r3 * 1 + 16], m1

    movu       m0, [r0 + r1 * 2 + 16]
    movu       m1, [r0 + r5 + 16]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m2

    movu       [r2 + r3 * 2 + 16], m0
    movu       [r2 + r4 + 16], m1

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_16xN 16
P2S_H_16xN 4
P2S_H_16xN 8
P2S_H_16xN 12
P2S_H_16xN 32
P2S_H_16xN 64
P2S_H_16xN 24

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_16xN_avx2 1
INIT_YMM avx2
cglobal filterPixelToShort_16x%1, 3, 7, 3
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m2, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m2

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1

    movu       m0, [r0 + r1 * 2]
    movu       m1, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m2

    movu       [r2 + r3 * 2], m0
    movu       [r2 + r4], m1

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_16xN_avx2 16
P2S_H_16xN_avx2 4
P2S_H_16xN_avx2 8
P2S_H_16xN_avx2 12
P2S_H_16xN_avx2 32
P2S_H_16xN_avx2 64
P2S_H_16xN_avx2 24

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_32xN 1
INIT_XMM ssse3
cglobal filterPixelToShort_32x%1, 3, 7, 5
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m4, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    movu       m2, [r0 + r1 * 2]
    movu       m3, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1
    movu       [r2 + r3 * 2], m2
    movu       [r2 + r4], m3

    movu       m0, [r0 + 16]
    movu       m1, [r0 + r1 + 16]
    movu       m2, [r0 + r1 * 2 + 16]
    movu       m3, [r0 + r5 + 16]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 16], m0
    movu       [r2 + r3 * 1 + 16], m1
    movu       [r2 + r3 * 2 + 16], m2
    movu       [r2 + r4 + 16], m3

    movu       m0, [r0 + 32]
    movu       m1, [r0 + r1 + 32]
    movu       m2, [r0 + r1 * 2 + 32]
    movu       m3, [r0 + r5 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 32], m0
    movu       [r2 + r3 * 1 + 32], m1
    movu       [r2 + r3 * 2 + 32], m2
    movu       [r2 + r4 + 32], m3

    movu       m0, [r0 + 48]
    movu       m1, [r0 + r1 + 48]
    movu       m2, [r0 + r1 * 2 + 48]
    movu       m3, [r0 + r5 + 48]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 48], m0
    movu       [r2 + r3 * 1 + 48], m1
    movu       [r2 + r3 * 2 + 48], m2
    movu       [r2 + r4 + 48], m3

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_32xN 32
P2S_H_32xN 8
P2S_H_32xN 16
P2S_H_32xN 24
P2S_H_32xN 64
P2S_H_32xN 48

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_32xN_avx2 1
INIT_YMM avx2
cglobal filterPixelToShort_32x%1, 3, 7, 3
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m2, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m1, m2

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1

    movu       m0, [r0 + r1 * 2]
    movu       m1, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 2], m0
    movu       [r2 + r4], m1

    movu       m0, [r0 + 32]
    movu       m1, [r0 + r1 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 0 + 32], m0
    movu       [r2 + r3 * 1 + 32], m1

    movu       m0, [r0 + r1 * 2 + 32]
    movu       m1, [r0 + r5 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 2 + 32], m0
    movu       [r2 + r4 + 32], m1

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_32xN_avx2 32
P2S_H_32xN_avx2 8
P2S_H_32xN_avx2 16
P2S_H_32xN_avx2 24
P2S_H_32xN_avx2 64
P2S_H_32xN_avx2 48

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_64xN 1
INIT_XMM ssse3
cglobal filterPixelToShort_64x%1, 3, 7, 5
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m4, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    movu       m2, [r0 + r1 * 2]
    movu       m3, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1
    movu       [r2 + r3 * 2], m2
    movu       [r2 + r4], m3

    movu       m0, [r0 + 16]
    movu       m1, [r0 + r1 + 16]
    movu       m2, [r0 + r1 * 2 + 16]
    movu       m3, [r0 + r5 + 16]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 16], m0
    movu       [r2 + r3 * 1 + 16], m1
    movu       [r2 + r3 * 2 + 16], m2
    movu       [r2 + r4 + 16], m3

    movu       m0, [r0 + 32]
    movu       m1, [r0 + r1 + 32]
    movu       m2, [r0 + r1 * 2 + 32]
    movu       m3, [r0 + r5 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 32], m0
    movu       [r2 + r3 * 1 + 32], m1
    movu       [r2 + r3 * 2 + 32], m2
    movu       [r2 + r4 + 32], m3

    movu       m0, [r0 + 48]
    movu       m1, [r0 + r1 + 48]
    movu       m2, [r0 + r1 * 2 + 48]
    movu       m3, [r0 + r5 + 48]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 48], m0
    movu       [r2 + r3 * 1 + 48], m1
    movu       [r2 + r3 * 2 + 48], m2
    movu       [r2 + r4 + 48], m3

    movu       m0, [r0 + 64]
    movu       m1, [r0 + r1 + 64]
    movu       m2, [r0 + r1 * 2 + 64]
    movu       m3, [r0 + r5 + 64]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 64], m0
    movu       [r2 + r3 * 1 + 64], m1
    movu       [r2 + r3 * 2 + 64], m2
    movu       [r2 + r4 + 64], m3

    movu       m0, [r0 + 80]
    movu       m1, [r0 + r1 + 80]
    movu       m2, [r0 + r1 * 2 + 80]
    movu       m3, [r0 + r5 + 80]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 80], m0
    movu       [r2 + r3 * 1 + 80], m1
    movu       [r2 + r3 * 2 + 80], m2
    movu       [r2 + r4 + 80], m3

    movu       m0, [r0 + 96]
    movu       m1, [r0 + r1 + 96]
    movu       m2, [r0 + r1 * 2 + 96]
    movu       m3, [r0 + r5 + 96]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 96], m0
    movu       [r2 + r3 * 1 + 96], m1
    movu       [r2 + r3 * 2 + 96], m2
    movu       [r2 + r4 + 96], m3

    movu       m0, [r0 + 112]
    movu       m1, [r0 + r1 + 112]
    movu       m2, [r0 + r1 * 2 + 112]
    movu       m3, [r0 + r5 + 112]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 112], m0
    movu       [r2 + r3 * 1 + 112], m1
    movu       [r2 + r3 * 2 + 112], m2
    movu       [r2 + r4 + 112], m3

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_64xN 64
P2S_H_64xN 16
P2S_H_64xN 32
P2S_H_64xN 48

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_64xN_avx2 1
INIT_YMM avx2
cglobal filterPixelToShort_64x%1, 3, 7, 3
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m2, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1

    movu       m0, [r0 + r1 * 2]
    movu       m1, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 2], m0
    movu       [r2 + r4], m1

    movu       m0, [r0 + 32]
    movu       m1, [r0 + r1 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 0 + 32], m0
    movu       [r2 + r3 * 1 + 32], m1

    movu       m0, [r0 + r1 * 2 + 32]
    movu       m1, [r0 + r5 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 2 + 32], m0
    movu       [r2 + r4 + 32], m1

    movu       m0, [r0 + 64]
    movu       m1, [r0 + r1 + 64]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 0 + 64], m0
    movu       [r2 + r3 * 1 + 64], m1

    movu       m0, [r0 + r1 * 2 + 64]
    movu       m1, [r0 + r5 + 64]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 2 + 64], m0
    movu       [r2 + r4 + 64], m1

    movu       m0, [r0 + 96]
    movu       m1, [r0 + r1 + 96]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 0 + 96], m0
    movu       [r2 + r3 * 1 + 96], m1

    movu       m0, [r0 + r1 * 2 + 96]
    movu       m1, [r0 + r5 + 96]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 2 + 96], m0
    movu       [r2 + r4 + 96], m1

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_64xN_avx2 64
P2S_H_64xN_avx2 16
P2S_H_64xN_avx2 32
P2S_H_64xN_avx2 48

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_24xN 1
INIT_XMM ssse3
cglobal filterPixelToShort_24x%1, 3, 7, 5
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m4, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    movu       m2, [r0 + r1 * 2]
    movu       m3, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1
    movu       [r2 + r3 * 2], m2
    movu       [r2 + r4], m3

    movu       m0, [r0 + 16]
    movu       m1, [r0 + r1 + 16]
    movu       m2, [r0 + r1 * 2 + 16]
    movu       m3, [r0 + r5 + 16]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 16], m0
    movu       [r2 + r3 * 1 + 16], m1
    movu       [r2 + r3 * 2 + 16], m2
    movu       [r2 + r4 + 16], m3

    movu       m0, [r0 + 32]
    movu       m1, [r0 + r1 + 32]
    movu       m2, [r0 + r1 * 2 + 32]
    movu       m3, [r0 + r5 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 32], m0
    movu       [r2 + r3 * 1 + 32], m1
    movu       [r2 + r3 * 2 + 32], m2
    movu       [r2 + r4 + 32], m3

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_24xN 32
P2S_H_24xN 64

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_24xN_avx2 1
INIT_YMM avx2
cglobal filterPixelToShort_24x%1, 3, 7, 3
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m2, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2
    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 0 + 32], xm1

    movu       m0, [r0 + r1]
    movu       m1, [r0 + r1 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2
    movu       [r2 + r3 * 1], m0
    movu       [r2 + r3 * 1 + 32], xm1

    movu       m0, [r0 + r1 * 2]
    movu       m1, [r0 + r1 * 2 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2
    movu       [r2 + r3 * 2], m0
    movu       [r2 + r3 * 2 + 32], xm1

    movu       m0, [r0 + r5]
    movu       m1, [r0 + r5 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2
    movu       [r2 + r4], m0
    movu       [r2 + r4 + 32], xm1

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_24xN_avx2 32
P2S_H_24xN_avx2 64

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
%macro P2S_H_12xN 1
INIT_XMM ssse3
cglobal filterPixelToShort_12x%1, 3, 7, 3
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, %1/4

    ; load constant
    mova       m2, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1

    movu       m0, [r0 + r1 * 2]
    movu       m1, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psubw      m0, m2
    psubw      m1, m2

    movu       [r2 + r3 * 2], m0
    movu       [r2 + r4], m1

    movh       m0, [r0 + 16]
    movhps     m0, [r0 + r1 + 16]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2

    movh       [r2 + r3 * 0 + 16], m0
    movhps     [r2 + r3 * 1 + 16], m0

    movh       m0, [r0 + r1 * 2 + 16]
    movhps     m0, [r0 + r5 + 16]
    psllw      m0, (14 - BIT_DEPTH)
    psubw      m0, m2

    movh       [r2 + r3 * 2 + 16], m0
    movhps     [r2 + r4 + 16], m0

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET
%endmacro
P2S_H_12xN 16
P2S_H_12xN 32

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
INIT_XMM ssse3
cglobal filterPixelToShort_48x64, 3, 7, 5
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, 16

    ; load constant
    mova       m4, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + r1]
    movu       m2, [r0 + r1 * 2]
    movu       m3, [r0 + r5]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 1], m1
    movu       [r2 + r3 * 2], m2
    movu       [r2 + r4], m3

    movu       m0, [r0 + 16]
    movu       m1, [r0 + r1 + 16]
    movu       m2, [r0 + r1 * 2 + 16]
    movu       m3, [r0 + r5 + 16]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 16], m0
    movu       [r2 + r3 * 1 + 16], m1
    movu       [r2 + r3 * 2 + 16], m2
    movu       [r2 + r4 + 16], m3

    movu       m0, [r0 + 32]
    movu       m1, [r0 + r1 + 32]
    movu       m2, [r0 + r1 * 2 + 32]
    movu       m3, [r0 + r5 + 32]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 32], m0
    movu       [r2 + r3 * 1 + 32], m1
    movu       [r2 + r3 * 2 + 32], m2
    movu       [r2 + r4 + 32], m3

    movu       m0, [r0 + 48]
    movu       m1, [r0 + r1 + 48]
    movu       m2, [r0 + r1 * 2 + 48]
    movu       m3, [r0 + r5 + 48]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 48], m0
    movu       [r2 + r3 * 1 + 48], m1
    movu       [r2 + r3 * 2 + 48], m2
    movu       [r2 + r4 + 48], m3

    movu       m0, [r0 + 64]
    movu       m1, [r0 + r1 + 64]
    movu       m2, [r0 + r1 * 2 + 64]
    movu       m3, [r0 + r5 + 64]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 64], m0
    movu       [r2 + r3 * 1 + 64], m1
    movu       [r2 + r3 * 2 + 64], m2
    movu       [r2 + r4 + 64], m3

    movu       m0, [r0 + 80]
    movu       m1, [r0 + r1 + 80]
    movu       m2, [r0 + r1 * 2 + 80]
    movu       m3, [r0 + r5 + 80]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psllw      m3, (14 - BIT_DEPTH)
    psubw      m0, m4
    psubw      m1, m4
    psubw      m2, m4
    psubw      m3, m4

    movu       [r2 + r3 * 0 + 80], m0
    movu       [r2 + r3 * 1 + 80], m1
    movu       [r2 + r3 * 2 + 80], m2
    movu       [r2 + r4 + 80], m3

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET

;-----------------------------------------------------------------------------
; void filterPixelToShort(pixel *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
;-----------------------------------------------------------------------------
INIT_YMM avx2
cglobal filterPixelToShort_48x64, 3, 7, 4
    add        r1d, r1d
    mov        r3d, r3m
    add        r3d, r3d
    lea        r4, [r3 * 3]
    lea        r5, [r1 * 3]

    ; load height
    mov        r6d, 16

    ; load constant
    mova       m3, [pw_2000]

.loop
    movu       m0, [r0]
    movu       m1, [r0 + 32]
    movu       m2, [r0 + 64]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psubw      m0, m3
    psubw      m1, m3
    psubw      m2, m3
    movu       [r2 + r3 * 0], m0
    movu       [r2 + r3 * 0 + 32], m1
    movu       [r2 + r3 * 0 + 64], m2

    movu       m0, [r0 + r1]
    movu       m1, [r0 + r1 + 32]
    movu       m2, [r0 + r1 + 64]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psubw      m0, m3
    psubw      m1, m3
    psubw      m2, m3
    movu       [r2 + r3 * 1], m0
    movu       [r2 + r3 * 1 + 32], m1
    movu       [r2 + r3 * 1 + 64], m2

    movu       m0, [r0 + r1 * 2]
    movu       m1, [r0 + r1 * 2 + 32]
    movu       m2, [r0 + r1 * 2 + 64]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psubw      m0, m3
    psubw      m1, m3
    psubw      m2, m3
    movu       [r2 + r3 * 2], m0
    movu       [r2 + r3 * 2 + 32], m1
    movu       [r2 + r3 * 2 + 64], m2

    movu       m0, [r0 + r5]
    movu       m1, [r0 + r5 + 32]
    movu       m2, [r0 + r5 + 64]
    psllw      m0, (14 - BIT_DEPTH)
    psllw      m1, (14 - BIT_DEPTH)
    psllw      m2, (14 - BIT_DEPTH)
    psubw      m0, m3
    psubw      m1, m3
    psubw      m2, m3
    movu       [r2 + r4], m0
    movu       [r2 + r4 + 32], m1
    movu       [r2 + r4 + 64], m2

    lea        r0, [r0 + r1 * 4]
    lea        r2, [r2 + r3 * 4]

    dec        r6d
    jnz        .loop
    RET


;-----------------------------------------------------------------------------------------------------------------------------
;void interp_horiz_ps_c(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt)
;-----------------------------------------------------------------------------------------------------------------------------

%macro IPFILTER_LUMA_PS_4xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_horiz_ps_4x%1, 6,8,7
    mov                         r5d,               r5m
    mov                         r4d,               r4m
    add                         r1d,               r1d
    add                         r3d,               r3d

%ifdef PIC
    lea                         r6,                [tab_LumaCoeff]
    lea                         r4,                [r4 * 8]
    vbroadcasti128              m0,                [r6 + r4 * 2]
%else
    lea                         r4,                [r4 * 8]
    vbroadcasti128              m0,                [tab_LumaCoeff + r4 * 2]
%endif

    vbroadcasti128              m2,                [INTERP_OFFSET_PS]

    ; register map
    ; m0 - interpolate coeff
    ; m1 - shuffle order table
    ; m2 - pw_2000

    sub                         r0,                6
    test                        r5d,               r5d
    mov                         r7d,               %1                                    ; loop count variable - height
    jz                         .preloop
    lea                         r6,                [r1 * 3]                              ; r6 = (N / 2 - 1) * srcStride
    sub                         r0,                r6                                    ; r0(src) - 3 * srcStride
    add                         r7d,               6                                     ;7 - 1(since last row not in loop)                            ; need extra 7 rows, just set a specially flag here, blkheight += N - 1  (7 - 3 = 4 ; since the last three rows not in loop)

.preloop:
    lea                         r6,                [r3 * 3]
.loop
    ; Row 0
    movu                        xm3,                [r0]                                 ; [x x x x x A 9 8 7 6 5 4 3 2 1 0]
    movu                        xm4,                [r0 + 2]                             ; [x x x x x A 9 8 7 6 5 4 3 2 1 0]
    vinserti128                 m3,                 m3,                xm4,       1
    movu                        xm4,                [r0 + 4]
    movu                        xm5,                [r0 + 6]
    vinserti128                 m4,                 m4,                xm5,       1
    pmaddwd                     m3,                m0
    pmaddwd                     m4,                m0
    phaddd                      m3,                m4                                    ; DWORD [R1D R1C R0D R0C R1B R1A R0B R0A]

    ; Row 1
    movu                        xm4,                [r0 + r1]                            ; [x x x x x A 9 8 7 6 5 4 3 2 1 0]
    movu                        xm5,                [r0 + r1 + 2]                        ; [x x x x x A 9 8 7 6 5 4 3 2 1 0]
    vinserti128                 m4,                 m4,                xm5,       1
    movu                        xm5,                [r0 + r1 + 4]
    movu                        xm6,                [r0 + r1 + 6]
    vinserti128                 m5,                 m5,                xm6,       1
    pmaddwd                     m4,                m0
    pmaddwd                     m5,                m0
    phaddd                      m4,                m5                                     ; DWORD [R3D R3C R2D R2C R3B R3A R2B R2A]
    phaddd                      m3,                m4                                     ; all rows and col completed.

    mova                        m5,                [interp8_hps_shuf]
    vpermd                      m3,                m5,                  m3
    paddd                       m3,                m2
    vextracti128                xm4,               m3,                  1
    psrad                       xm3,               INTERP_SHIFT_PS
    psrad                       xm4,               INTERP_SHIFT_PS
    packssdw                    xm3,               xm3
    packssdw                    xm4,               xm4

    movq                        [r2],              xm3                                   ;row 0
    movq                        [r2 + r3],         xm4                                   ;row 1
    lea                         r0,                [r0 + r1 * 2]                         ; first loop src ->5th row(i.e 4)
    lea                         r2,                [r2 + r3 * 2]                         ; first loop dst ->5th row(i.e 4)

    sub                         r7d,               2
    jg                          .loop
    test                        r5d,               r5d
    jz                          .end

    ; Row 10
    movu                        xm3,                [r0]
    movu                        xm4,                [r0 + 2]
    vinserti128                 m3,                 m3,                 xm4,      1
    movu                        xm4,                [r0 + 4]
    movu                        xm5,                [r0 + 6]
    vinserti128                 m4,                 m4,                 xm5,      1
    pmaddwd                     m3,                m0
    pmaddwd                     m4,                m0
    phaddd                      m3,                m4

    ; Row11
    phaddd                      m3,                m4                                    ; all rows and col completed.

    mova                        m5,                [interp8_hps_shuf]
    vpermd                      m3,                m5,                  m3
    paddd                       m3,                m2
    vextracti128                xm4,               m3,                  1
    psrad                       xm3,               INTERP_SHIFT_PS
    psrad                       xm4,               INTERP_SHIFT_PS
    packssdw                    xm3,               xm3
    packssdw                    xm4,               xm4

    movq                        [r2],              xm3                                   ;row 0
.end
    RET
%endif
%endmacro

    IPFILTER_LUMA_PS_4xN_AVX2 4
    IPFILTER_LUMA_PS_4xN_AVX2 8
    IPFILTER_LUMA_PS_4xN_AVX2 16

%macro IPFILTER_LUMA_PS_8xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_horiz_ps_8x%1, 4, 6, 8
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m
    shl                 r4d, 4
%ifdef PIC
    lea                 r6, [tab_LumaCoeff]
    vpbroadcastq        m0, [r6 + r4]
    vpbroadcastq        m1, [r6 + r4 + 8]
%else
    vpbroadcastq        m0, [tab_LumaCoeff + r4]
    vpbroadcastq        m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 6
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    lea                 r6, [r1*3]
    sub                 r0, r6
    add                 r4d, 7

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m7, m5, m3
    pmaddwd             m4, m0
    pmaddwd             m7, m1
    paddd               m4, m7

    vbroadcasti128      m6, [r0 + 16]
    pshufb              m5, m3
    pshufb              m6, m3
    pmaddwd             m5, m0
    pmaddwd             m6, m1
    paddd               m5, m6

    phaddd              m4, m5
    vpermq              m4, m4, q3120
    paddd               m4, m2
    vextracti128        xm5,m4, 1
    psrad               xm4, INTERP_SHIFT_PS
    psrad               xm5, INTERP_SHIFT_PS
    packssdw            xm4, xm5

    movu                [r2], xm4
    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

    IPFILTER_LUMA_PS_8xN_AVX2 4
    IPFILTER_LUMA_PS_8xN_AVX2 8
    IPFILTER_LUMA_PS_8xN_AVX2 16
    IPFILTER_LUMA_PS_8xN_AVX2 32

INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_horiz_ps_24x32, 4, 6, 8
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m
    shl                 r4d, 4
%ifdef PIC
    lea                 r6, [tab_LumaCoeff]
    vpbroadcastq        m0, [r6 + r4]
    vpbroadcastq        m1, [r6 + r4 + 8]
%else
    vpbroadcastq        m0, [tab_LumaCoeff + r4]
    vpbroadcastq        m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 6
    test                r5d, r5d
    mov                 r4d, 32
    jz                  .loop0
    lea                 r6, [r1*3]
    sub                 r0, r6
    add                 r4d, 7

.loop0:
%assign x 0
%rep 24/8
    vbroadcasti128      m4, [r0 + x]
    vbroadcasti128      m5, [r0 + 8 + x]
    pshufb              m4, m3
    pshufb              m7, m5, m3
    pmaddwd             m4, m0
    pmaddwd             m7, m1
    paddd               m4, m7

    vbroadcasti128      m6, [r0 + 16 + x]
    pshufb              m5, m3
    pshufb              m6, m3
    pmaddwd             m5, m0
    pmaddwd             m6, m1
    paddd               m5, m6

    phaddd              m4, m5
    vpermq              m4, m4, q3120
    paddd               m4, m2
    vextracti128        xm5,m4, 1
    psrad               xm4, INTERP_SHIFT_PS
    psrad               xm5, INTERP_SHIFT_PS
    packssdw            xm4, xm5

    movu                [r2 + x], xm4
    %assign x x+16
    %endrep

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif


%macro IPFILTER_LUMA_PS_32_64_AVX2 2
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_horiz_ps_%1x%2, 4, 6, 8

    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m
    shl                 r4d, 6
%ifdef PIC
    lea                 r6, [tab_LumaCoeffV]
    movu                m0, [r6 + r4]
    movu                m1, [r6 + r4 + mmsize]
%else
    movu                m0, [tab_LumaCoeffV + r4]
    movu                m1, [tab_LumaCoeffV + r4 + mmsize]
%endif
    mova                m3, [interp8_hpp_shuf_new]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 6
    test                r5d, r5d
    mov                 r4d, %2
    jz                 .loop0
    lea                 r6, [r1*3]
    sub                 r0, r6
    add                 r4d, 7

.loop0:
%assign x 0
%rep %1/16
    vbroadcasti128      m4, [r0 + x]
    vbroadcasti128      m5, [r0 + 4 * SIZEOF_PIXEL + x]
    pshufb              m4, m3
    pshufb              m5, m3

    pmaddwd             m4, m0
    pmaddwd             m7, m5, m1
    paddd               m4, m7
    vextracti128        xm7, m4, 1
    paddd               xm4, xm7
    paddd               xm4, xm2
    psrad               xm4, INTERP_SHIFT_PS

    vbroadcasti128      m6, [r0 + 16 + x]
    pshufb              m6, m3

    pmaddwd             m5, m0
    pmaddwd             m7, m6, m1
    paddd               m5, m7
    vextracti128        xm7, m5, 1
    paddd               xm5, xm7
    paddd               xm5, xm2
    psrad               xm5, INTERP_SHIFT_PS

    packssdw            xm4, xm5
    movu                [r2 + x], xm4

    vbroadcasti128      m5, [r0 + 24 + x]
    pshufb              m5, m3

    pmaddwd             m6, m0
    pmaddwd             m7, m5, m1
    paddd               m6, m7
    vextracti128        xm7, m6, 1
    paddd               xm6, xm7
    paddd               xm6, xm2
    psrad               xm6, INTERP_SHIFT_PS

    vbroadcasti128      m7, [r0 + 32 + x]
    pshufb              m7, m3

    pmaddwd             m5, m0
    pmaddwd             m7, m1
    paddd               m5, m7
    vextracti128        xm7, m5, 1
    paddd               xm5, xm7
    paddd               xm5, xm2
    psrad               xm5, INTERP_SHIFT_PS

    packssdw            xm6, xm5
    movu                [r2 + 16 + x], xm6

%assign x x+32
%endrep

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                .loop0
    RET
%endif
%endmacro

    IPFILTER_LUMA_PS_32_64_AVX2 32, 8
    IPFILTER_LUMA_PS_32_64_AVX2 32, 16
    IPFILTER_LUMA_PS_32_64_AVX2 32, 24
    IPFILTER_LUMA_PS_32_64_AVX2 32, 32
    IPFILTER_LUMA_PS_32_64_AVX2 32, 64

    IPFILTER_LUMA_PS_32_64_AVX2 64, 16
    IPFILTER_LUMA_PS_32_64_AVX2 64, 32
    IPFILTER_LUMA_PS_32_64_AVX2 64, 48
    IPFILTER_LUMA_PS_32_64_AVX2 64, 64

    IPFILTER_LUMA_PS_32_64_AVX2 48, 64

%macro IPFILTER_LUMA_PS_16xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_horiz_ps_16x%1, 4, 6, 8

    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m
    shl                 r4d, 4
%ifdef PIC
    lea                 r6, [tab_LumaCoeff]
    vpbroadcastq        m0, [r6 + r4]
    vpbroadcastq        m1, [r6 + r4 + 8]
%else
    vpbroadcastq        m0, [tab_LumaCoeff + r4]
    vpbroadcastq        m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 6
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    lea                 r6, [r1*3]
    sub                 r0, r6
    add                 r4d, 7

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m7, m5, m3
    pmaddwd             m4, m0
    pmaddwd             m7, m1
    paddd               m4, m7

    vbroadcasti128      m6, [r0 + 16]
    pshufb              m5, m3
    pshufb              m7, m6, m3
    pmaddwd             m5, m0
    pmaddwd             m7, m1
    paddd               m5, m7

    phaddd              m4, m5
    vpermq              m4, m4, q3120
    paddd               m4, m2
    vextracti128        xm5, m4, 1
    psrad               xm4, INTERP_SHIFT_PS
    psrad               xm5, INTERP_SHIFT_PS
    packssdw            xm4, xm5
    movu                [r2], xm4

    vbroadcasti128      m5, [r0 + 24]
    pshufb              m6, m3
    pshufb              m7, m5, m3
    pmaddwd             m6, m0
    pmaddwd             m7, m1
    paddd               m6, m7

    vbroadcasti128      m7, [r0 + 32]
    pshufb              m5, m3
    pshufb              m7, m3
    pmaddwd             m5, m0
    pmaddwd             m7, m1
    paddd               m5, m7

    phaddd              m6, m5
    vpermq              m6, m6, q3120
    paddd               m6, m2
    vextracti128        xm5,m6, 1
    psrad               xm6, INTERP_SHIFT_PS
    psrad               xm5, INTERP_SHIFT_PS
    packssdw            xm6, xm5
    movu                [r2 + 16], xm6

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

    IPFILTER_LUMA_PS_16xN_AVX2 4
    IPFILTER_LUMA_PS_16xN_AVX2 8
    IPFILTER_LUMA_PS_16xN_AVX2 12
    IPFILTER_LUMA_PS_16xN_AVX2 16
    IPFILTER_LUMA_PS_16xN_AVX2 32
    IPFILTER_LUMA_PS_16xN_AVX2 64

INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_8tap_horiz_ps_12x16, 4, 6, 8
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m
    shl                 r4d, 4
%ifdef PIC
    lea                 r6, [tab_LumaCoeff]
    vpbroadcastq        m0, [r6 + r4]
    vpbroadcastq        m1, [r6 + r4 + 8]
%else
    vpbroadcastq        m0, [tab_LumaCoeff + r4]
    vpbroadcastq        m1, [tab_LumaCoeff + r4 + 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 6
    test                r5d, r5d
    mov                 r4d, 16
    jz                  .loop0
    lea                 r6, [r1*3]
    sub                 r0, r6
    add                 r4d, 7

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m7, m5, m3
    pmaddwd             m4, m0
    pmaddwd             m7, m1
    paddd               m4, m7

    vbroadcasti128      m6, [r0 + 16]
    pshufb              m5, m3
    pshufb              m7, m6, m3
    pmaddwd             m5, m0
    pmaddwd             m7, m1
    paddd               m5, m7

    phaddd              m4, m5
    vpermq              m4, m4, q3120
    paddd               m4, m2
    vextracti128        xm5,m4, 1
    psrad               xm4, INTERP_SHIFT_PS
    psrad               xm5, INTERP_SHIFT_PS
    packssdw            xm4, xm5
    movu                [r2], xm4

    vbroadcasti128      m5, [r0 + 24]
    pshufb              m6, m3
    pshufb              m5, m3
    pmaddwd             m6, m0
    pmaddwd             m5, m1
    paddd               m6, m5

    phaddd              m6, m6
    vpermq              m6, m6, q3120
    paddd               xm6, xm2
    psrad               xm6, INTERP_SHIFT_PS
    packssdw            xm6, xm6
    movq                [r2 + 16], xm6

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif

%macro IPFILTER_CHROMA_PS_8xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_horiz_ps_8x%1, 4, 7, 6
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m

%ifdef PIC
    lea                 r6, [tab_ChromaCoeff]
    vpbroadcastq        m0, [r6 + r4 * 8]
%else
    vpbroadcastq        m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 2
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    sub                 r0, r1
    add                 r4d, 3

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2], xm4
    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

    IPFILTER_CHROMA_PS_8xN_AVX2 4
    IPFILTER_CHROMA_PS_8xN_AVX2 8
    IPFILTER_CHROMA_PS_8xN_AVX2 16
    IPFILTER_CHROMA_PS_8xN_AVX2 32
    IPFILTER_CHROMA_PS_8xN_AVX2 6
    IPFILTER_CHROMA_PS_8xN_AVX2 2
    IPFILTER_CHROMA_PS_8xN_AVX2 12
    IPFILTER_CHROMA_PS_8xN_AVX2 64

%macro IPFILTER_CHROMA_PS_16xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_horiz_ps_16x%1, 4, 7, 6
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m

%ifdef PIC
    lea                 r6, [tab_ChromaCoeff]
    vpbroadcastq        m0, [r6 + r4 * 8]
%else
    vpbroadcastq        m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 2
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    sub                 r0, r1
    add                 r4d, 3

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2], xm4

    vbroadcasti128      m4, [r0 + 16]
    vbroadcasti128      m5, [r0 + 24]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 16], xm4

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

IPFILTER_CHROMA_PS_16xN_AVX2 16
IPFILTER_CHROMA_PS_16xN_AVX2 8
IPFILTER_CHROMA_PS_16xN_AVX2 32
IPFILTER_CHROMA_PS_16xN_AVX2 12
IPFILTER_CHROMA_PS_16xN_AVX2 4
IPFILTER_CHROMA_PS_16xN_AVX2 64
IPFILTER_CHROMA_PS_16xN_AVX2 24

%macro IPFILTER_CHROMA_PS_24xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_horiz_ps_24x%1, 4, 7, 6
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m

%ifdef PIC
    lea                 r6, [tab_ChromaCoeff]
    vpbroadcastq        m0, [r6 + r4 * 8]
%else
    vpbroadcastq        m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 2
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    sub                 r0, r1
    add                 r4d, 3

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2], xm4

    vbroadcasti128      m4, [r0 + 16]
    vbroadcasti128      m5, [r0 + 24]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 16], xm4

    vbroadcasti128      m4, [r0 + 32]
    vbroadcasti128      m5, [r0 + 40]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 32], xm4

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

IPFILTER_CHROMA_PS_24xN_AVX2 32
IPFILTER_CHROMA_PS_24xN_AVX2 64

%macro IPFILTER_CHROMA_PS_12xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_horiz_ps_12x%1, 4, 7, 6
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m

%ifdef PIC
    lea                 r6, [tab_ChromaCoeff]
    vpbroadcastq        m0, [r6 + r4 * 8]
%else
    vpbroadcastq        m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 2
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    sub                 r0, r1
    add                 r4d, 3

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2], xm4

    vbroadcasti128      m4, [r0 + 16]
    pshufb              m4, m3
    pmaddwd             m4, m0
    phaddd              m4, m4
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movq                [r2 + 16], xm4

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

IPFILTER_CHROMA_PS_12xN_AVX2 16
IPFILTER_CHROMA_PS_12xN_AVX2 32

%macro IPFILTER_CHROMA_PS_32xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_horiz_ps_32x%1, 4, 7, 6
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m

%ifdef PIC
    lea                 r6, [tab_ChromaCoeff]
    vpbroadcastq        m0, [r6 + r4 * 8]
%else
    vpbroadcastq        m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 2
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    sub                 r0, r1
    add                 r4d, 3

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2], xm4

    vbroadcasti128      m4, [r0 + 16]
    vbroadcasti128      m5, [r0 + 24]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 16], xm4

    vbroadcasti128      m4, [r0 + 32]
    vbroadcasti128      m5, [r0 + 40]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 32], xm4

    vbroadcasti128      m4, [r0 + 48]
    vbroadcasti128      m5, [r0 + 56]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 48], xm4

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

IPFILTER_CHROMA_PS_32xN_AVX2 32
IPFILTER_CHROMA_PS_32xN_AVX2 16
IPFILTER_CHROMA_PS_32xN_AVX2 24
IPFILTER_CHROMA_PS_32xN_AVX2 8
IPFILTER_CHROMA_PS_32xN_AVX2 64
IPFILTER_CHROMA_PS_32xN_AVX2 48


%macro IPFILTER_CHROMA_PS_64xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_horiz_ps_64x%1, 4, 7, 6
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m

%ifdef PIC
    lea                 r6, [tab_ChromaCoeff]
    vpbroadcastq        m0, [r6 + r4 * 8]
%else
    vpbroadcastq        m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 2
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    sub                 r0, r1
    add                 r4d, 3

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2], xm4

    vbroadcasti128      m4, [r0 + 16]
    vbroadcasti128      m5, [r0 + 24]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 16], xm4

    vbroadcasti128      m4, [r0 + 32]
    vbroadcasti128      m5, [r0 + 40]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 32], xm4

    vbroadcasti128      m4, [r0 + 48]
    vbroadcasti128      m5, [r0 + 56]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 48], xm4

    vbroadcasti128      m4, [r0 + 64]
    vbroadcasti128      m5, [r0 + 72]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 64], xm4

    vbroadcasti128      m4, [r0 + 80]
    vbroadcasti128      m5, [r0 + 88]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 80], xm4

    vbroadcasti128      m4, [r0 + 96]
    vbroadcasti128      m5, [r0 + 104]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 96], xm4

    vbroadcasti128      m4, [r0 + 112]
    vbroadcasti128      m5, [r0 + 120]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 112], xm4

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

IPFILTER_CHROMA_PS_64xN_AVX2 64
IPFILTER_CHROMA_PS_64xN_AVX2 48
IPFILTER_CHROMA_PS_64xN_AVX2 32
IPFILTER_CHROMA_PS_64xN_AVX2 16

INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_horiz_ps_48x64, 4, 7, 6
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m

%ifdef PIC
    lea                 r6, [tab_ChromaCoeff]
    vpbroadcastq        m0, [r6 + r4 * 8]
%else
    vpbroadcastq        m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 2
    test                r5d, r5d
    mov                 r4d, 64
    jz                  .loop0
    sub                 r0, r1
    add                 r4d, 3

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2], xm4

    vbroadcasti128      m4, [r0 + 16]
    vbroadcasti128      m5, [r0 + 24]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 16], xm4

    vbroadcasti128      m4, [r0 + 32]
    vbroadcasti128      m5, [r0 + 40]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 32], xm4

    vbroadcasti128      m4, [r0 + 48]
    vbroadcasti128      m5, [r0 + 56]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 48], xm4

    vbroadcasti128      m4, [r0 + 64]
    vbroadcasti128      m5, [r0 + 72]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 64], xm4

    vbroadcasti128      m4, [r0 + 80]
    vbroadcasti128      m5, [r0 + 88]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movu                [r2 + 80], xm4

    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif

%macro IPFILTER_CHROMA_PS_6xN_AVX2 1
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_horiz_ps_6x%1, 4, 7, 6
    add                 r1d, r1d
    add                 r3d, r3d
    mov                 r4d, r4m
    mov                 r5d, r5m

%ifdef PIC
    lea                 r6, [tab_ChromaCoeff]
    vpbroadcastq        m0, [r6 + r4 * 8]
%else
    vpbroadcastq        m0, [tab_ChromaCoeff + r4 * 8]
%endif
    mova                m3, [interp8_hpp_shuf]
    vbroadcasti128      m2, [INTERP_OFFSET_PS]

    ; register map
    ; m0 , m1 interpolate coeff

    sub                 r0, 2
    test                r5d, r5d
    mov                 r4d, %1
    jz                  .loop0
    sub                 r0, r1
    add                 r4d, 3

.loop0:
    vbroadcasti128      m4, [r0]
    vbroadcasti128      m5, [r0 + 8]
    pshufb              m4, m3
    pshufb              m5, m3
    pmaddwd             m4, m0
    pmaddwd             m5, m0
    phaddd              m4, m5
    paddd               m4, m2
    vpermq              m4, m4, q3120
    psrad               m4, INTERP_SHIFT_PS
    vextracti128        xm5, m4, 1
    packssdw            xm4, xm5
    movq                [r2], xm4
    pextrd              [r2 + 8], xm4, 2
    add                 r2, r3
    add                 r0, r1
    dec                 r4d
    jnz                 .loop0
    RET
%endif
%endmacro

    IPFILTER_CHROMA_PS_6xN_AVX2 8
    IPFILTER_CHROMA_PS_6xN_AVX2 16

%macro FILTER_VER_CHROMA_AVX2_8xN 2
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_vert_%1_8x%2, 4, 9, 15
    mov             r4d, r4m
    shl             r4d, 6
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1
%ifidn %1,pp
    vbroadcasti128  m14, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m14, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m14, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]
    lea             r7, [r1 * 4]
    mov             r8d, %2 / 16
.loopH:
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]

    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]

    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5]

    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    paddd           m1, m5
    pmaddwd         m3, [r5]

    movu            xm5, [r0 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    paddd           m2, m6
    pmaddwd         m4, [r5]

    movu            xm6, [r0 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    paddd           m3, m7
    pmaddwd         m5, [r5]

    movu            xm7, [r0 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    paddd           m4, m8
    pmaddwd         m6, [r5]

    lea             r0, [r0 + r1 * 4]
    movu            xm8, [r0]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m9, m7, [r5 + 1 * mmsize]
    paddd           m5, m9
    pmaddwd         m7, [r5]


    movu            xm9, [r0 + r1]                  ; m9 = row 9
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m10, m8, [r5 + 1 * mmsize]
    paddd           m6, m10
    pmaddwd         m8, [r5]


    movu            xm10, [r0 + r1 * 2]             ; m10 = row 10
    punpckhwd       xm11, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm11, 1
    pmaddwd         m11, m9, [r5 + 1 * mmsize]
    paddd           m7, m11
    pmaddwd         m9, [r5]

    movu            xm11, [r0 + r4]                 ; m11 = row 11
    punpckhwd       xm12, xm10, xm11
    punpcklwd       xm10, xm11
    vinserti128     m10, m10, xm12, 1
    pmaddwd         m12, m10, [r5 + 1 * mmsize]
    paddd           m8, m12
    pmaddwd         m10, [r5]

    lea             r0, [r0 + r1 * 4]
    movu            xm12, [r0]                      ; m12 = row 12
    punpckhwd       xm13, xm11, xm12
    punpcklwd       xm11, xm12
    vinserti128     m11, m11, xm13, 1
    pmaddwd         m13, m11, [r5 + 1 * mmsize]
    paddd           m9, m13
    pmaddwd         m11, [r5]

%ifidn %1,ss
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
    psrad           m4, 6
    psrad           m5, 6
%else
    paddd           m0, m14
    paddd           m1, m14
    paddd           m2, m14
    paddd           m3, m14
    paddd           m4, m14
    paddd           m5, m14
%ifidn %1,pp
    psrad           m0, 6
    psrad           m1, 6
    psrad           m2, 6
    psrad           m3, 6
    psrad           m4, 6
    psrad           m5, 6
%elifidn %1, sp
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
    psrad           m2, INTERP_SHIFT_SP
    psrad           m3, INTERP_SHIFT_SP
    psrad           m4, INTERP_SHIFT_SP
    psrad           m5, INTERP_SHIFT_SP
%else
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
    psrad           m2, INTERP_SHIFT_PS
    psrad           m3, INTERP_SHIFT_PS
    psrad           m4, INTERP_SHIFT_PS
    psrad           m5, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m0, m1
    packssdw        m2, m3
    packssdw        m4, m5
    vpermq          m0, m0, q3120
    vpermq          m2, m2, q3120
    vpermq          m4, m4, q3120
    pxor            m5, m5
    mova            m3, [pw_pixel_max]
%ifidn %1,pp
    CLIPW           m0, m5, m3
    CLIPW           m2, m5, m3
    CLIPW           m4, m5, m3
%elifidn %1, sp
    CLIPW           m0, m5, m3
    CLIPW           m2, m5, m3
    CLIPW           m4, m5, m3
%endif

    vextracti128    xm1, m0, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    vextracti128    xm1, m2, 1
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r6], xm1
    lea             r2, [r2 + r3 * 4]
    vextracti128    xm1, m4, 1
    movu            [r2], xm4
    movu            [r2 + r3], xm1

    movu            xm13, [r0 + r1]                 ; m13 = row 13
    punpckhwd       xm0, xm12, xm13
    punpcklwd       xm12, xm13
    vinserti128     m12, m12, xm0, 1
    pmaddwd         m0, m12, [r5 + 1 * mmsize]
    paddd           m10, m0
    pmaddwd         m12, [r5]

    movu            xm0, [r0 + r1 * 2]              ; m0 = row 14
    punpckhwd       xm1, xm13, xm0
    punpcklwd       xm13, xm0
    vinserti128     m13, m13, xm1, 1
    pmaddwd         m1, m13, [r5 + 1 * mmsize]
    paddd           m11, m1
    pmaddwd         m13, [r5]

%ifidn %1,ss
    psrad           m6, 6
    psrad           m7, 6
%else
    paddd           m6, m14
    paddd           m7, m14
%ifidn %1,pp
    psrad           m6, 6
    psrad           m7, 6
%elifidn %1, sp
    psrad           m6, INTERP_SHIFT_SP
    psrad           m7, INTERP_SHIFT_SP
%else
    psrad           m6, INTERP_SHIFT_PS
    psrad           m7, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m6, m7
    vpermq          m6, m6, q3120
%ifidn %1,pp
    CLIPW           m6, m5, m3
%elifidn %1, sp
    CLIPW           m6, m5, m3
%endif
    vextracti128    xm7, m6, 1
    movu            [r2 + r3 * 2], xm6
    movu            [r2 + r6], xm7

    movu            xm1, [r0 + r4]                  ; m1 = row 15
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m2, m0, [r5 + 1 * mmsize]
    paddd           m12, m2
    pmaddwd         m0, [r5]

    lea             r0, [r0 + r1 * 4]
    movu            xm2, [r0]                       ; m2 = row 16
    punpckhwd       xm6, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm6, 1
    pmaddwd         m6, m1, [r5 + 1 * mmsize]
    paddd           m13, m6
    pmaddwd         m1, [r5]

    movu            xm6, [r0 + r1]                  ; m6 = row 17
    punpckhwd       xm4, xm2, xm6
    punpcklwd       xm2, xm6
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m2, [r5 + 1 * mmsize]
    paddd           m0, m2

    movu            xm4, [r0 + r1 * 2]              ; m4 = row 18
    punpckhwd       xm2, xm6, xm4
    punpcklwd       xm6, xm4
    vinserti128     m6, m6, xm2, 1
    pmaddwd         m6, [r5 + 1 * mmsize]
    paddd           m1, m6

%ifidn %1,ss
    psrad           m8, 6
    psrad           m9, 6
    psrad           m10, 6
    psrad           m11, 6
    psrad           m12, 6
    psrad           m13, 6
    psrad           m0, 6
    psrad           m1, 6
%else
    paddd           m8, m14
    paddd           m9, m14
    paddd           m10, m14
    paddd           m11, m14
    paddd           m12, m14
    paddd           m13, m14
    paddd           m0, m14
    paddd           m1, m14
%ifidn %1,pp
    psrad           m8, 6
    psrad           m9, 6
    psrad           m10, 6
    psrad           m11, 6
    psrad           m12, 6
    psrad           m13, 6
    psrad           m0, 6
    psrad           m1, 6
%elifidn %1, sp
    psrad           m8, INTERP_SHIFT_SP
    psrad           m9, INTERP_SHIFT_SP
    psrad           m10, INTERP_SHIFT_SP
    psrad           m11, INTERP_SHIFT_SP
    psrad           m12, INTERP_SHIFT_SP
    psrad           m13, INTERP_SHIFT_SP
    psrad           m0, INTERP_SHIFT_SP
    psrad           m1, INTERP_SHIFT_SP
%else
    psrad           m8, INTERP_SHIFT_PS
    psrad           m9, INTERP_SHIFT_PS
    psrad           m10, INTERP_SHIFT_PS
    psrad           m11, INTERP_SHIFT_PS
    psrad           m12, INTERP_SHIFT_PS
    psrad           m13, INTERP_SHIFT_PS
    psrad           m0, INTERP_SHIFT_PS
    psrad           m1, INTERP_SHIFT_PS
%endif
%endif

    packssdw        m8, m9
    packssdw        m10, m11
    packssdw        m12, m13
    packssdw        m0, m1
    vpermq          m8, m8, q3120
    vpermq          m10, m10, q3120
    vpermq          m12, m12, q3120
    vpermq          m0, m0, q3120
%ifidn %1,pp
    CLIPW           m8, m5, m3
    CLIPW           m10, m5, m3
    CLIPW           m12, m5, m3
    CLIPW           m0, m5, m3
%elifidn %1, sp
    CLIPW           m8, m5, m3
    CLIPW           m10, m5, m3
    CLIPW           m12, m5, m3
    CLIPW           m0, m5, m3
%endif
    vextracti128    xm9, m8, 1
    vextracti128    xm11, m10, 1
    vextracti128    xm13, m12, 1
    vextracti128    xm1, m0, 1
    lea             r2, [r2 + r3 * 4]
    movu            [r2], xm8
    movu            [r2 + r3], xm9
    movu            [r2 + r3 * 2], xm10
    movu            [r2 + r6], xm11
    lea             r2, [r2 + r3 * 4]
    movu            [r2], xm12
    movu            [r2 + r3], xm13
    movu            [r2 + r3 * 2], xm0
    movu            [r2 + r6], xm1
    lea             r2, [r2 + r3 * 4]
    dec             r8d
    jnz             .loopH
    RET
%endif
%endmacro

FILTER_VER_CHROMA_AVX2_8xN pp, 16
FILTER_VER_CHROMA_AVX2_8xN ps, 16
FILTER_VER_CHROMA_AVX2_8xN ss, 16
FILTER_VER_CHROMA_AVX2_8xN sp, 16
FILTER_VER_CHROMA_AVX2_8xN pp, 32
FILTER_VER_CHROMA_AVX2_8xN ps, 32
FILTER_VER_CHROMA_AVX2_8xN sp, 32
FILTER_VER_CHROMA_AVX2_8xN ss, 32
FILTER_VER_CHROMA_AVX2_8xN pp, 64
FILTER_VER_CHROMA_AVX2_8xN ps, 64
FILTER_VER_CHROMA_AVX2_8xN sp, 64
FILTER_VER_CHROMA_AVX2_8xN ss, 64

%macro PROCESS_CHROMA_AVX2_8x2 3
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]

    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]

    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m2, m2, [r5 + 1 * mmsize]
    paddd           m0, m2

    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m3, m3, [r5 + 1 * mmsize]
    paddd           m1, m3

%ifnidn %1,ss
    paddd           m0, m7
    paddd           m1, m7
%endif
    psrad           m0, %3
    psrad           m1, %3

    packssdw        m0, m1
    vpermq          m0, m0, q3120
    pxor            m4, m4

%if %2
    CLIPW           m0, m4, [pw_pixel_max]
%endif
    vextracti128    xm1, m0, 1
%endmacro


%macro FILTER_VER_CHROMA_AVX2_8x2 3
INIT_YMM avx2
cglobal interp_4tap_vert_%1_8x2, 4, 6, 8
    mov             r4d, r4m
    shl             r4d, 6
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1
%ifidn %1,pp
    vbroadcasti128  m7, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif

    PROCESS_CHROMA_AVX2_8x2 %1, %2, %3
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    RET
%endmacro

FILTER_VER_CHROMA_AVX2_8x2 pp, 1, 6
FILTER_VER_CHROMA_AVX2_8x2 ps, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_8x2 sp, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_8x2 ss, 0, 6

%macro FILTER_VER_CHROMA_AVX2_4x2 3
INIT_YMM avx2
cglobal interp_4tap_vert_%1_4x2, 4, 6, 7
    mov             r4d, r4m
    add             r1d, r1d
    add             r3d, r3d
    shl             r4d, 6

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1

%ifidn %1,pp
    vbroadcasti128  m6, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m6, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m6, [INTERP_OFFSET_PS]
%endif

    movq            xm0, [r0]                       ; row 0
    movq            xm1, [r0 + r1]                  ; row 1
    punpcklwd       xm0, xm1

    movq            xm2, [r0 + r1 * 2]              ; row 2
    punpcklwd       xm1, xm2
    vinserti128     m0, m0, xm1, 1                  ; m0 = [2 1 1 0]
    pmaddwd         m0, [r5]

    movq            xm3, [r0 + r4]                  ; row 3
    punpcklwd       xm2, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm4, [r0]                       ; row 4
    punpcklwd       xm3, xm4
    vinserti128     m2, m2, xm3, 1                  ; m2 = [4 3 3 2]
    pmaddwd         m5, m2, [r5 + 1 * mmsize]
    paddd           m0, m5

%ifnidn %1, ss
    paddd           m0, m6
%endif
    psrad           m0, %3
    packssdw        m0, m0
    pxor            m1, m1

%if %2
    CLIPW           m0, m1, [pw_pixel_max]
%endif

    vextracti128    xm2, m0, 1
    lea             r4, [r3 * 3]
    movq            [r2], xm0
    movq            [r2 + r3], xm2
    RET
%endmacro

FILTER_VER_CHROMA_AVX2_4x2 pp, 1, 6
FILTER_VER_CHROMA_AVX2_4x2 ps, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_4x2 sp, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_4x2 ss, 0, 6

%macro FILTER_VER_CHROMA_AVX2_4x4 3
INIT_YMM avx2
cglobal interp_4tap_vert_%1_4x4, 4, 6, 7
    mov             r4d, r4m
    add             r1d, r1d
    add             r3d, r3d
    shl             r4d, 6

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1

%ifidn %1,pp
   vbroadcasti128  m6, [pd_32]
%elifidn %1, sp
   vbroadcasti128  m6, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m6, [INTERP_OFFSET_PS]
%endif
    movq            xm0, [r0]                       ; row 0
    movq            xm1, [r0 + r1]                  ; row 1
    punpcklwd       xm0, xm1

    movq            xm2, [r0 + r1 * 2]              ; row 2
    punpcklwd       xm1, xm2
    vinserti128     m0, m0, xm1, 1                  ; m0 = [2 1 1 0]
    pmaddwd         m0, [r5]

    movq            xm3, [r0 + r4]                  ; row 3
    punpcklwd       xm2, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm4, [r0]                       ; row 4
    punpcklwd       xm3, xm4
    vinserti128     m2, m2, xm3, 1                  ; m2 = [4 3 3 2]
    pmaddwd         m5, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m5

    movq            xm3, [r0 + r1]                  ; row 5
    punpcklwd       xm4, xm3
    movq            xm1, [r0 + r1 * 2]              ; row 6
    punpcklwd       xm3, xm1
    vinserti128     m4, m4, xm3, 1                  ; m4 = [6 5 5 4]
    pmaddwd         m4, [r5 + 1 * mmsize]
    paddd           m2, m4

%ifnidn %1,ss
    paddd           m0, m6
    paddd           m2, m6
%endif
    psrad           m0, %3
    psrad           m2, %3

    packssdw        m0, m2
    pxor            m1, m1
%if %2
    CLIPW           m0, m1, [pw_pixel_max]
%endif

    vextracti128    xm2, m0, 1
    lea             r4, [r3 * 3]
    movq            [r2], xm0
    movq            [r2 + r3], xm2
    movhps          [r2 + r3 * 2], xm0
    movhps          [r2 + r4], xm2
    RET
%endmacro

FILTER_VER_CHROMA_AVX2_4x4 pp, 1, 6
FILTER_VER_CHROMA_AVX2_4x4 ps, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_4x4 sp, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_4x4 ss, 0, 6


%macro FILTER_VER_CHROMA_AVX2_4x8 3
INIT_YMM avx2
cglobal interp_4tap_vert_%1_4x8, 4, 7, 8
    mov             r4d, r4m
    shl             r4d, 6
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1

%ifidn %1,pp
    vbroadcasti128  m7, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]

    movq            xm0, [r0]                       ; row 0
    movq            xm1, [r0 + r1]                  ; row 1
    punpcklwd       xm0, xm1
    movq            xm2, [r0 + r1 * 2]              ; row 2
    punpcklwd       xm1, xm2
    vinserti128     m0, m0, xm1, 1                  ; m0 = [2 1 1 0]
    pmaddwd         m0, [r5]

    movq            xm3, [r0 + r4]                  ; row 3
    punpcklwd       xm2, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm4, [r0]                       ; row 4
    punpcklwd       xm3, xm4
    vinserti128     m2, m2, xm3, 1                  ; m2 = [4 3 3 2]
    pmaddwd         m5, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m5

    movq            xm3, [r0 + r1]                  ; row 5
    punpcklwd       xm4, xm3
    movq            xm1, [r0 + r1 * 2]              ; row 6
    punpcklwd       xm3, xm1
    vinserti128     m4, m4, xm3, 1                  ; m4 = [6 5 5 4]
    pmaddwd         m5, m4, [r5 + 1 * mmsize]
    paddd           m2, m5
    pmaddwd         m4, [r5]

    movq            xm3, [r0 + r4]                  ; row 7
    punpcklwd       xm1, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm6, [r0]                       ; row 8
    punpcklwd       xm3, xm6
    vinserti128     m1, m1, xm3, 1                  ; m1 = [8 7 7 6]
    pmaddwd         m5, m1, [r5 + 1 * mmsize]
    paddd           m4, m5
    pmaddwd         m1, [r5]

    movq            xm3, [r0 + r1]                  ; row 9
    punpcklwd       xm6, xm3
    movq            xm5, [r0 + 2 * r1]              ; row 10
    punpcklwd       xm3, xm5
    vinserti128     m6, m6, xm3, 1                  ; m6 = [A 9 9 8]
    pmaddwd         m6, [r5 + 1 * mmsize]
    paddd           m1, m6
%ifnidn %1,ss
    paddd           m0, m7
    paddd           m2, m7
%endif
    psrad           m0, %3
    psrad           m2, %3
    packssdw        m0, m2
    pxor            m6, m6
    mova            m3, [pw_pixel_max]
%if %2
    CLIPW           m0, m6, m3
%endif
    vextracti128    xm2, m0, 1
    movq            [r2], xm0
    movq            [r2 + r3], xm2
    movhps          [r2 + r3 * 2], xm0
    movhps          [r2 + r6], xm2
%ifnidn %1,ss
    paddd           m4, m7
    paddd           m1, m7
%endif
    psrad           m4, %3
    psrad           m1, %3
    packssdw        m4, m1
%if %2
    CLIPW           m4, m6, m3
%endif
    vextracti128    xm1, m4, 1
    lea             r2, [r2 + r3 * 4]
    movq            [r2], xm4
    movq            [r2 + r3], xm1
    movhps          [r2 + r3 * 2], xm4
    movhps          [r2 + r6], xm1
    RET
%endmacro

FILTER_VER_CHROMA_AVX2_4x8 pp, 1, 6
FILTER_VER_CHROMA_AVX2_4x8 ps, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_4x8 sp, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_4x8 ss, 0 , 6

%macro PROCESS_LUMA_AVX2_W4_16R_4TAP 3
    movq            xm0, [r0]                       ; row 0
    movq            xm1, [r0 + r1]                  ; row 1
    punpcklwd       xm0, xm1
    movq            xm2, [r0 + r1 * 2]              ; row 2
    punpcklwd       xm1, xm2
    vinserti128     m0, m0, xm1, 1                  ; m0 = [2 1 1 0]
    pmaddwd         m0, [r5]
    movq            xm3, [r0 + r4]                  ; row 3
    punpcklwd       xm2, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm4, [r0]                       ; row 4
    punpcklwd       xm3, xm4
    vinserti128     m2, m2, xm3, 1                  ; m2 = [4 3 3 2]
    pmaddwd         m5, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m5
    movq            xm3, [r0 + r1]                  ; row 5
    punpcklwd       xm4, xm3
    movq            xm1, [r0 + r1 * 2]              ; row 6
    punpcklwd       xm3, xm1
    vinserti128     m4, m4, xm3, 1                  ; m4 = [6 5 5 4]
    pmaddwd         m5, m4, [r5 + 1 * mmsize]
    paddd           m2, m5
    pmaddwd         m4, [r5]
    movq            xm3, [r0 + r4]                  ; row 7
    punpcklwd       xm1, xm3
    lea             r0, [r0 + 4 * r1]
    movq            xm6, [r0]                       ; row 8
    punpcklwd       xm3, xm6
    vinserti128     m1, m1, xm3, 1                  ; m1 = [8 7 7 6]
    pmaddwd         m5, m1, [r5 + 1 * mmsize]
    paddd           m4, m5
    pmaddwd         m1, [r5]
    movq            xm3, [r0 + r1]                  ; row 9
    punpcklwd       xm6, xm3
    movq            xm5, [r0 + 2 * r1]              ; row 10
    punpcklwd       xm3, xm5
    vinserti128     m6, m6, xm3, 1                  ; m6 = [10 9 9 8]
    pmaddwd         m3, m6, [r5 + 1 * mmsize]
    paddd           m1, m3
    pmaddwd         m6, [r5]
%ifnidn %1,ss
    paddd           m0, m7
    paddd           m2, m7
%endif
    psrad           m0, %3
    psrad           m2, %3
    packssdw        m0, m2
    pxor            m3, m3
%if %2
    CLIPW           m0, m3, [pw_pixel_max]
%endif
    vextracti128    xm2, m0, 1
    movq            [r2], xm0
    movq            [r2 + r3], xm2
    movhps          [r2 + r3 * 2], xm0
    movhps          [r2 + r6], xm2
    movq            xm2, [r0 + r4]                  ;row 11
    punpcklwd       xm5, xm2
    lea             r0, [r0 + 4 * r1]
    movq            xm0, [r0]                       ; row 12
    punpcklwd       xm2, xm0
    vinserti128     m5, m5, xm2, 1                  ; m5 = [12 11 11 10]
    pmaddwd         m2, m5, [r5 + 1 * mmsize]
    paddd           m6, m2
    pmaddwd         m5, [r5]
    movq            xm2, [r0 + r1]                  ; row 13
    punpcklwd       xm0, xm2
    movq            xm3, [r0 + 2 * r1]              ; row 14
    punpcklwd       xm2, xm3
    vinserti128     m0, m0, xm2, 1                  ; m0 = [14 13 13 12]
    pmaddwd         m2, m0, [r5 + 1 * mmsize]
    paddd           m5, m2
    pmaddwd         m0, [r5]
%ifnidn %1,ss
    paddd           m4, m7
    paddd           m1, m7
%endif
    psrad           m4, %3
    psrad           m1, %3
    packssdw        m4, m1
    pxor            m2, m2
%if %2
    CLIPW           m4, m2, [pw_pixel_max]
%endif

    vextracti128    xm1, m4, 1
    lea             r2, [r2 + r3 * 4]
    movq            [r2], xm4
    movq            [r2 + r3], xm1
    movhps          [r2 + r3 * 2], xm4
    movhps          [r2 + r6], xm1
    movq            xm4, [r0 + r4]                  ; row 15
    punpcklwd       xm3, xm4
    lea             r0, [r0 + 4 * r1]
    movq            xm1, [r0]                       ; row 16
    punpcklwd       xm4, xm1
    vinserti128     m3, m3, xm4, 1                  ; m3 = [16 15 15 14]
    pmaddwd         m4, m3, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m3, [r5]
    movq            xm4, [r0 + r1]                  ; row 17
    punpcklwd       xm1, xm4
    movq            xm2, [r0 + 2 * r1]              ; row 18
    punpcklwd       xm4, xm2
    vinserti128     m1, m1, xm4, 1                  ; m1 = [18 17 17 16]
    pmaddwd         m1, [r5 + 1 * mmsize]
    paddd           m3, m1

%ifnidn %1,ss
    paddd           m6, m7
    paddd           m5, m7
%endif
    psrad           m6, %3
    psrad           m5, %3
    packssdw        m6, m5
    pxor            m1, m1
%if %2
    CLIPW           m6, m1, [pw_pixel_max]
%endif
    vextracti128    xm5, m6, 1
    lea             r2, [r2 + r3 * 4]
    movq            [r2], xm6
    movq            [r2 + r3], xm5
    movhps          [r2 + r3 * 2], xm6
    movhps          [r2 + r6], xm5
%ifnidn %1,ss
    paddd           m0, m7
    paddd           m3, m7
%endif
    psrad           m0, %3
    psrad           m3, %3
    packssdw        m0, m3
%if %2
    CLIPW           m0, m1, [pw_pixel_max]
%endif
    vextracti128    xm3, m0, 1
    lea             r2, [r2 + r3 * 4]
    movq            [r2], xm0
    movq            [r2 + r3], xm3
    movhps          [r2 + r3 * 2], xm0
    movhps          [r2 + r6], xm3
%endmacro


%macro FILTER_VER_CHROMA_AVX2_4xN 4
INIT_YMM avx2
cglobal interp_4tap_vert_%1_4x%2, 4, 8, 8
    mov             r4d, r4m
    shl             r4d, 6
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1
    mov             r7d, %2 / 16
%ifidn %1,pp
    vbroadcasti128  m7, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]
.loopH:
    PROCESS_LUMA_AVX2_W4_16R_4TAP %1, %3, %4
    lea             r2, [r2 + r3 * 4]
    dec             r7d
    jnz             .loopH
    RET
%endmacro

FILTER_VER_CHROMA_AVX2_4xN pp, 16, 1, 6
FILTER_VER_CHROMA_AVX2_4xN ps, 16, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_4xN sp, 16, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_4xN ss, 16, 0, 6
FILTER_VER_CHROMA_AVX2_4xN pp, 32, 1, 6
FILTER_VER_CHROMA_AVX2_4xN ps, 32, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_4xN sp, 32, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_4xN ss, 32, 0, 6

%macro FILTER_VER_CHROMA_AVX2_8x8 3
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_vert_%1_8x8, 4, 6, 12
    mov             r4d, r4m
    add             r1d, r1d
    add             r3d, r3d
    shl             r4d, 6

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1

%ifidn %1,pp
    vbroadcasti128  m11, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m11, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m11, [INTERP_OFFSET_PS]
%endif

    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m4                          ; res row0 done(0,1,2,3)
    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    pmaddwd         m3, [r5]
    paddd           m1, m5                          ;res row1 done(1, 2, 3, 4)
    movu            xm5, [r0 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    pmaddwd         m4, [r5]
    paddd           m2, m6                          ;res row2 done(2,3,4,5)
    movu            xm6, [r0 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    pmaddwd         m5, [r5]
    paddd           m3, m7                          ;res row3 done(3,4,5,6)
    movu            xm7, [r0 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    pmaddwd         m6, [r5]
    paddd           m4, m8                          ;res row4 done(4,5,6,7)
    lea             r0, [r0 + r1 * 4]
    movu            xm8, [r0]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m9, m7, [r5 + 1 * mmsize]
    pmaddwd         m7, [r5]
    paddd           m5, m9                          ;res row5 done(5,6,7,8)
    movu            xm9, [r0 + r1]                  ; m9 = row 9
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m8, [r5 + 1 * mmsize]
    paddd           m6, m8                          ;res row6 done(6,7,8,9)
    movu            xm10, [r0 + r1 * 2]             ; m10 = row 10
    punpckhwd       xm8, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm8, 1
    pmaddwd         m9, [r5 + 1 * mmsize]
    paddd           m7, m9                          ;res row7 done 7,8,9,10
    lea             r4, [r3 * 3]
%ifnidn %1,ss
    paddd           m0, m11
    paddd           m1, m11
    paddd           m2, m11
    paddd           m3, m11
%endif
    psrad           m0, %3
    psrad           m1, %3
    psrad           m2, %3
    psrad           m3, %3
    packssdw        m0, m1
    packssdw        m2, m3
    vpermq          m0, m0, q3120
    vpermq          m2, m2, q3120
    pxor            m1, m1
    mova            m3, [pw_pixel_max]
%if %2
    CLIPW           m0, m1, m3
    CLIPW           m2, m1, m3
%endif
    vextracti128    xm9, m0, 1
    vextracti128    xm8, m2, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm9
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r4], xm8
%ifnidn %1,ss
    paddd           m4, m11
    paddd           m5, m11
    paddd           m6, m11
    paddd           m7, m11
%endif
    psrad           m4, %3
    psrad           m5, %3
    psrad           m6, %3
    psrad           m7, %3
    packssdw        m4, m5
    packssdw        m6, m7
    vpermq          m4, m4, q3120
    vpermq          m6, m6, q3120
%if %2
    CLIPW           m4, m1, m3
    CLIPW           m6, m1, m3
%endif
    vextracti128    xm5, m4, 1
    vextracti128    xm7, m6, 1
    lea             r2, [r2 + r3 * 4]
    movu            [r2], xm4
    movu            [r2 + r3], xm5
    movu            [r2 + r3 * 2], xm6
    movu            [r2 + r4], xm7
    RET
%endif
%endmacro

FILTER_VER_CHROMA_AVX2_8x8 pp, 1, 6
FILTER_VER_CHROMA_AVX2_8x8 ps, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_8x8 sp, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_8x8 ss, 0, 6

%macro FILTER_VER_CHROMA_AVX2_8x6 3
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_vert_%1_8x6, 4, 6, 12
    mov             r4d, r4m
    add             r1d, r1d
    add             r3d, r3d
    shl             r4d, 6

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1

%ifidn %1,pp
    vbroadcasti128  m11, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m11, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m11, [INTERP_OFFSET_PS]
%endif

    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    pmaddwd         m2, [r5]
    paddd           m0, m4                          ; r0 done(0,1,2,3)
    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    pmaddwd         m3, [r5]
    paddd           m1, m5                          ;r1 done(1, 2, 3, 4)
    movu            xm5, [r0 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    pmaddwd         m4, [r5]
    paddd           m2, m6                          ;r2 done(2,3,4,5)
    movu            xm6, [r0 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    pmaddwd         m5, [r5]
    paddd           m3, m7                          ;r3 done(3,4,5,6)
    movu            xm7, [r0 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    paddd           m4, m8                          ;r4 done(4,5,6,7)
    lea             r0, [r0 + r1 * 4]
    movu            xm8, [r0]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m7, m7, [r5 + 1 * mmsize]
    paddd           m5, m7                          ;r5 done(5,6,7,8)
    lea             r4, [r3 * 3]
%ifnidn %1,ss
    paddd           m0, m11
    paddd           m1, m11
    paddd           m2, m11
    paddd           m3, m11
%endif
    psrad           m0, %3
    psrad           m1, %3
    psrad           m2, %3
    psrad           m3, %3
    packssdw        m0, m1
    packssdw        m2, m3
    vpermq          m0, m0, q3120
    vpermq          m2, m2, q3120
    pxor            m10, m10
    mova            m9, [pw_pixel_max]
%if %2
    CLIPW           m0, m10, m9
    CLIPW           m2, m10, m9
%endif
    vextracti128    xm1, m0, 1
    vextracti128    xm3, m2, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r4], xm3
%ifnidn %1,ss
    paddd           m4, m11
    paddd           m5, m11
%endif
    psrad           m4, %3
    psrad           m5, %3
    packssdw        m4, m5
    vpermq          m4, m4, 11011000b
%if %2
    CLIPW           m4, m10, m9
%endif
    vextracti128    xm5, m4, 1
    lea             r2, [r2 + r3 * 4]
    movu            [r2], xm4
    movu            [r2 + r3], xm5
    RET
%endif
%endmacro

FILTER_VER_CHROMA_AVX2_8x6 pp, 1, 6
FILTER_VER_CHROMA_AVX2_8x6 ps, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_8x6 sp, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_8x6 ss, 0, 6

%macro PROCESS_CHROMA_AVX2 3
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    paddd           m1, m5
    pmaddwd         m3, [r5]
    movu            xm5, [r0 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m4, [r5 + 1 * mmsize]
    paddd           m2, m4
    movu            xm6, [r0 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm4, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm4, 1
    pmaddwd         m5, [r5 + 1 * mmsize]
    paddd           m3, m5
%ifnidn %1,ss
    paddd           m0, m7
    paddd           m1, m7
    paddd           m2, m7
    paddd           m3, m7
%endif
    psrad           m0, %3
    psrad           m1, %3
    psrad           m2, %3
    psrad           m3, %3
    packssdw        m0, m1
    packssdw        m2, m3
    vpermq          m0, m0, q3120
    vpermq          m2, m2, q3120
    pxor            m4, m4
%if %2
    CLIPW           m0, m4, [pw_pixel_max]
    CLIPW           m2, m4, [pw_pixel_max]
%endif
    vextracti128    xm1, m0, 1
    vextracti128    xm3, m2, 1
%endmacro


%macro FILTER_VER_CHROMA_AVX2_8x4 3
INIT_YMM avx2
cglobal interp_4tap_vert_%1_8x4, 4, 6, 8
    mov             r4d, r4m
    shl             r4d, 6
    add             r1d, r1d
    add             r3d, r3d
%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif
    lea             r4, [r1 * 3]
    sub             r0, r1
%ifidn %1,pp
    vbroadcasti128  m7, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m7, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m7, [INTERP_OFFSET_PS]
%endif
    PROCESS_CHROMA_AVX2 %1, %2, %3
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    movu            [r2 + r3 * 2], xm2
    lea             r4, [r3 * 3]
    movu            [r2 + r4], xm3
    RET
%endmacro

FILTER_VER_CHROMA_AVX2_8x4 pp, 1, 6
FILTER_VER_CHROMA_AVX2_8x4 ps, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_8x4 sp, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_8x4 ss, 0, 6

%macro FILTER_VER_CHROMA_AVX2_8x12 3
INIT_YMM avx2
%if ARCH_X86_64 == 1
cglobal interp_4tap_vert_%1_8x12, 4, 7, 15
    mov             r4d, r4m
    shl             r4d, 6
    add             r1d, r1d
    add             r3d, r3d

%ifdef PIC
    lea             r5, [tab_ChromaCoeffVer]
    add             r5, r4
%else
    lea             r5, [tab_ChromaCoeffVer + r4]
%endif

    lea             r4, [r1 * 3]
    sub             r0, r1
%ifidn %1,pp
    vbroadcasti128  m14, [pd_32]
%elifidn %1, sp
    vbroadcasti128  m14, [INTERP_OFFSET_SP]
%else
    vbroadcasti128  m14, [INTERP_OFFSET_PS]
%endif
    lea             r6, [r3 * 3]
    movu            xm0, [r0]                       ; m0 = row 0
    movu            xm1, [r0 + r1]                  ; m1 = row 1
    punpckhwd       xm2, xm0, xm1
    punpcklwd       xm0, xm1
    vinserti128     m0, m0, xm2, 1
    pmaddwd         m0, [r5]
    movu            xm2, [r0 + r1 * 2]              ; m2 = row 2
    punpckhwd       xm3, xm1, xm2
    punpcklwd       xm1, xm2
    vinserti128     m1, m1, xm3, 1
    pmaddwd         m1, [r5]
    movu            xm3, [r0 + r4]                  ; m3 = row 3
    punpckhwd       xm4, xm2, xm3
    punpcklwd       xm2, xm3
    vinserti128     m2, m2, xm4, 1
    pmaddwd         m4, m2, [r5 + 1 * mmsize]
    paddd           m0, m4
    pmaddwd         m2, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm4, [r0]                       ; m4 = row 4
    punpckhwd       xm5, xm3, xm4
    punpcklwd       xm3, xm4
    vinserti128     m3, m3, xm5, 1
    pmaddwd         m5, m3, [r5 + 1 * mmsize]
    paddd           m1, m5
    pmaddwd         m3, [r5]
    movu            xm5, [r0 + r1]                  ; m5 = row 5
    punpckhwd       xm6, xm4, xm5
    punpcklwd       xm4, xm5
    vinserti128     m4, m4, xm6, 1
    pmaddwd         m6, m4, [r5 + 1 * mmsize]
    paddd           m2, m6
    pmaddwd         m4, [r5]
    movu            xm6, [r0 + r1 * 2]              ; m6 = row 6
    punpckhwd       xm7, xm5, xm6
    punpcklwd       xm5, xm6
    vinserti128     m5, m5, xm7, 1
    pmaddwd         m7, m5, [r5 + 1 * mmsize]
    paddd           m3, m7
    pmaddwd         m5, [r5]
    movu            xm7, [r0 + r4]                  ; m7 = row 7
    punpckhwd       xm8, xm6, xm7
    punpcklwd       xm6, xm7
    vinserti128     m6, m6, xm8, 1
    pmaddwd         m8, m6, [r5 + 1 * mmsize]
    paddd           m4, m8
    pmaddwd         m6, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm8, [r0]                       ; m8 = row 8
    punpckhwd       xm9, xm7, xm8
    punpcklwd       xm7, xm8
    vinserti128     m7, m7, xm9, 1
    pmaddwd         m9, m7, [r5 + 1 * mmsize]
    paddd           m5, m9
    pmaddwd         m7, [r5]
    movu            xm9, [r0 + r1]                  ; m9 = row 9
    punpckhwd       xm10, xm8, xm9
    punpcklwd       xm8, xm9
    vinserti128     m8, m8, xm10, 1
    pmaddwd         m10, m8, [r5 + 1 * mmsize]
    paddd           m6, m10
    pmaddwd         m8, [r5]
    movu            xm10, [r0 + r1 * 2]             ; m10 = row 10
    punpckhwd       xm11, xm9, xm10
    punpcklwd       xm9, xm10
    vinserti128     m9, m9, xm11, 1
    pmaddwd         m11, m9, [r5 + 1 * mmsize]
    paddd           m7, m11
    pmaddwd         m9, [r5]
    movu            xm11, [r0 + r4]                 ; m11 = row 11
    punpckhwd       xm12, xm10, xm11
    punpcklwd       xm10, xm11
    vinserti128     m10, m10, xm12, 1
    pmaddwd         m12, m10, [r5 + 1 * mmsize]
    paddd           m8, m12
    pmaddwd         m10, [r5]
    lea             r0, [r0 + r1 * 4]
    movu            xm12, [r0]                      ; m12 = row 12
    punpckhwd       xm13, xm11, xm12
    punpcklwd       xm11, xm12
    vinserti128     m11, m11, xm13, 1
    pmaddwd         m13, m11, [r5 + 1 * mmsize]
    paddd           m9, m13
    pmaddwd         m11, [r5]
%ifnidn %1,ss
    paddd           m0, m14
    paddd           m1, m14
    paddd           m2, m14
    paddd           m3, m14
    paddd           m4, m14
    paddd           m5, m14
%endif
    psrad           m0, %3
    psrad           m1, %3
    psrad           m2, %3
    psrad           m3, %3
    psrad           m4, %3
    psrad           m5, %3
    packssdw        m0, m1
    packssdw        m2, m3
    packssdw        m4, m5
    vpermq          m0, m0, q3120
    vpermq          m2, m2, q3120
    vpermq          m4, m4, q3120
    pxor            m5, m5
    mova            m3, [pw_pixel_max]
%if %2
    CLIPW           m0, m5, m3
    CLIPW           m2, m5, m3
    CLIPW           m4, m5, m3
%endif
    vextracti128    xm1, m0, 1
    movu            [r2], xm0
    movu            [r2 + r3], xm1
    vextracti128    xm1, m2, 1
    movu            [r2 + r3 * 2], xm2
    movu            [r2 + r6], xm1
    lea             r2, [r2 + r3 * 4]
    vextracti128    xm1, m4, 1
    movu            [r2], xm4
    movu            [r2 + r3], xm1
    movu            xm13, [r0 + r1]                 ; m13 = row 13
    punpckhwd       xm0, xm12, xm13
    punpcklwd       xm12, xm13
    vinserti128     m12, m12, xm0, 1
    pmaddwd         m12, m12, [r5 + 1 * mmsize]
    paddd           m10, m12
    movu            xm0, [r0 + r1 * 2]              ; m0 = row 14
    punpckhwd       xm1, xm13, xm0
    punpcklwd       xm13, xm0
    vinserti128     m13, m13, xm1, 1
    pmaddwd         m13, m13, [r5 + 1 * mmsize]
    paddd           m11, m13
%ifnidn %1,ss
    paddd           m6, m14
    paddd           m7, m14
    paddd           m8, m14
    paddd           m9, m14
    paddd           m10, m14
    paddd           m11, m14
%endif
    psrad           m6, %3
    psrad           m7, %3
    psrad           m8, %3
    psrad           m9, %3
    psrad           m10, %3
    psrad           m11, %3
    packssdw        m6, m7
    packssdw        m8, m9
    packssdw        m10, m11
    vpermq          m6, m6, q3120
    vpermq          m8, m8, q3120
    vpermq          m10, m10, q3120
%if %2
    CLIPW           m6, m5, m3
    CLIPW           m8, m5, m3
    CLIPW           m10, m5, m3
%endif
    vextracti128    xm7, m6, 1
    vextracti128    xm9, m8, 1
    vextracti128    xm11, m10, 1
    movu            [r2 + r3 * 2], xm6
    movu            [r2 + r6], xm7
    lea             r2, [r2 + r3 * 4]
    movu            [r2], xm8
    movu            [r2 + r3], xm9
    movu            [r2 + r3 * 2], xm10
    movu            [r2 + r6], xm11
    RET
%endif
%endmacro

FILTER_VER_CHROMA_AVX2_8x12 pp, 1, 6
FILTER_VER_CHROMA_AVX2_8x12 ps, 0, INTERP_SHIFT_PS
FILTER_VER_CHROMA_AVX2_8x12 sp, 1, INTERP_SHIFT_SP
FILTER_VER_CHROMA_AVX2_8x12 ss, 0, 6
