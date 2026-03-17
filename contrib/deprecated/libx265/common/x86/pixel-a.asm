;*****************************************************************************
;* pixel.asm: x86 pixel metrics
;*****************************************************************************
;* Copyright (C) 2003-2013 x264 project
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Loren Merritt <lorenm@u.washington.edu>
;*          Holger Lubitz <holger@lubitz.org>
;*          Laurent Aimar <fenrir@via.ecp.fr>
;*          Alex Izvorski <aizvorksi@gmail.com>
;*          Fiona Glaser <fiona@x264.com>
;*          Oskar Arvidsson <oskar@irock.se>
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
hmul_8p:   times 8 db 1
           times 4 db 1, -1
           times 8 db 1
           times 4 db 1, -1
hmul_4p:   times 4 db 1, 1, 1, 1, 1, -1, 1, -1
mask_10:   times 4 dw 0, -1
mask_1100: times 2 dd 0, -1
hmul_8w:   times 4 dw 1
           times 2 dw 1, -1
           times 4 dw 1
           times 2 dw 1, -1

ALIGN 32
transd_shuf1: SHUFFLE_MASK_W 0, 8, 2, 10, 4, 12, 6, 14
transd_shuf2: SHUFFLE_MASK_W 1, 9, 3, 11, 5, 13, 7, 15

SECTION .text

cextern pb_0
cextern pb_1
cextern pw_1
cextern pw_8
cextern pw_16
cextern pw_32
cextern pw_00ff
cextern pw_ppppmmmm
cextern pw_ppmmppmm
cextern pw_pmpmpmpm
cextern pw_pmmpzzzz
cextern pd_1
cextern pd_2
cextern hmul_16p
cextern pb_movemask
cextern pb_movemask_32
cextern pw_pixel_max

;=============================================================================
; SATD
;=============================================================================

%macro JDUP 2
%if cpuflag(sse4)
    ; just use shufps on anything post conroe
    shufps %1, %2, 0
%elif cpuflag(ssse3) && notcpuflag(atom)
    ; join 2x 32 bit and duplicate them
    ; emulating shufps is faster on conroe
    punpcklqdq %1, %2
    movsldup %1, %1
%else
    ; doesn't need to dup. sse2 does things by zero extending to words and full h_2d
    punpckldq %1, %2
%endif
%endmacro

%macro HSUMSUB 5
    pmaddubsw m%2, m%5
    pmaddubsw m%1, m%5
    pmaddubsw m%4, m%5
    pmaddubsw m%3, m%5
%endmacro

%macro DIFF_UNPACK_SSE2 5
    punpcklbw m%1, m%5
    punpcklbw m%2, m%5
    punpcklbw m%3, m%5
    punpcklbw m%4, m%5
    psubw m%1, m%2
    psubw m%3, m%4
%endmacro

%macro DIFF_SUMSUB_SSSE3 5
    HSUMSUB %1, %2, %3, %4, %5
    psubw m%1, m%2
    psubw m%3, m%4
%endmacro

%macro LOAD_DUP_2x4P 4 ; dst, tmp, 2* pointer
    movd %1, %3
    movd %2, %4
    JDUP %1, %2
%endmacro

%macro LOAD_DUP_4x8P_CONROE 8 ; 4*dst, 4*pointer
    movddup m%3, %6
    movddup m%4, %8
    movddup m%1, %5
    movddup m%2, %7
%endmacro

%macro LOAD_DUP_4x8P_PENRYN 8
    ; penryn and nehalem run punpcklqdq and movddup in different units
    movh m%3, %6
    movh m%4, %8
    punpcklqdq m%3, m%3
    movddup m%1, %5
    punpcklqdq m%4, m%4
    movddup m%2, %7
%endmacro

%macro LOAD_SUMSUB_8x2P 9
    LOAD_DUP_4x8P %1, %2, %3, %4, %6, %7, %8, %9
    DIFF_SUMSUB_SSSE3 %1, %3, %2, %4, %5
%endmacro

%macro LOAD_SUMSUB_8x4P_SSSE3 7-11 r0, r2, 0, 0
; 4x dest, 2x tmp, 1x mul, [2* ptr], [increment?]
    LOAD_SUMSUB_8x2P %1, %2, %5, %6, %7, [%8], [%9], [%8+r1], [%9+r3]
    LOAD_SUMSUB_8x2P %3, %4, %5, %6, %7, [%8+2*r1], [%9+2*r3], [%8+r4], [%9+r5]
%if %10
    lea %8, [%8+4*r1]
    lea %9, [%9+4*r3]
%endif
%endmacro

%macro LOAD_SUMSUB_16P_SSSE3 7 ; 2*dst, 2*tmp, mul, 2*ptr
    movddup m%1, [%7]
    movddup m%2, [%7+8]
    mova m%4, [%6]
    movddup m%3, m%4
    punpckhqdq m%4, m%4
    DIFF_SUMSUB_SSSE3 %1, %3, %2, %4, %5
%endmacro

%macro LOAD_SUMSUB_16P_SSE2 7 ; 2*dst, 2*tmp, mask, 2*ptr
    movu  m%4, [%7]
    mova  m%2, [%6]
    DEINTB %1, %2, %3, %4, %5
    psubw m%1, m%3
    psubw m%2, m%4
    SUMSUB_BA w, %1, %2, %3
%endmacro

%macro LOAD_SUMSUB_16x4P 10-13 r0, r2, none
; 8x dest, 1x tmp, 1x mul, [2* ptr] [2nd tmp]
    LOAD_SUMSUB_16P %1, %5, %2, %3, %10, %11, %12
    LOAD_SUMSUB_16P %2, %6, %3, %4, %10, %11+r1, %12+r3
    LOAD_SUMSUB_16P %3, %7, %4, %9, %10, %11+2*r1, %12+2*r3
    LOAD_SUMSUB_16P %4, %8, %13, %9, %10, %11+r4, %12+r5
%endmacro

%macro LOAD_SUMSUB_16x2P_AVX2 9
; 2*dst, 2*tmp, mul, 4*ptr
    vbroadcasti128 m%1, [%6]
    vbroadcasti128 m%3, [%7]
    vbroadcasti128 m%2, [%8]
    vbroadcasti128 m%4, [%9]
    DIFF_SUMSUB_SSSE3 %1, %3, %2, %4, %5
%endmacro

%macro LOAD_SUMSUB_16x4P_AVX2 7-11 r0, r2, 0, 0
; 4x dest, 2x tmp, 1x mul, [2* ptr], [increment?]
    LOAD_SUMSUB_16x2P_AVX2 %1, %2, %5, %6, %7, %8, %9, %8+r1, %9+r3
    LOAD_SUMSUB_16x2P_AVX2 %3, %4, %5, %6, %7, %8+2*r1, %9+2*r3, %8+r4, %9+r5
%if %10
    lea  %8, [%8+4*r1]
    lea  %9, [%9+4*r3]
%endif
%endmacro

%macro LOAD_DUP_4x16P_AVX2 8 ; 4*dst, 4*pointer
    mova  xm%3, %6
    mova  xm%4, %8
    mova  xm%1, %5
    mova  xm%2, %7
    vpermq m%3, m%3, q0011
    vpermq m%4, m%4, q0011
    vpermq m%1, m%1, q0011
    vpermq m%2, m%2, q0011
%endmacro

%macro LOAD_SUMSUB8_16x2P_AVX2 9
; 2*dst, 2*tmp, mul, 4*ptr
    LOAD_DUP_4x16P_AVX2 %1, %2, %3, %4, %6, %7, %8, %9
    DIFF_SUMSUB_SSSE3 %1, %3, %2, %4, %5
%endmacro

%macro LOAD_SUMSUB8_16x4P_AVX2 7-11 r0, r2, 0, 0
; 4x dest, 2x tmp, 1x mul, [2* ptr], [increment?]
    LOAD_SUMSUB8_16x2P_AVX2 %1, %2, %5, %6, %7, [%8], [%9], [%8+r1], [%9+r3]
    LOAD_SUMSUB8_16x2P_AVX2 %3, %4, %5, %6, %7, [%8+2*r1], [%9+2*r3], [%8+r4], [%9+r5]
%if %10
    lea  %8, [%8+4*r1]
    lea  %9, [%9+4*r3]
%endif
%endmacro

; in: r4=3*stride1, r5=3*stride2
; in: %2 = horizontal offset
; in: %3 = whether we need to increment pix1 and pix2
; clobber: m3..m7
; out: %1 = satd
%macro SATD_4x4_MMX 3
    %xdefine %%n nn%1
    %assign offset %2*SIZEOF_PIXEL
    LOAD_DIFF m4, m3, none, [r0+     offset], [r2+     offset]
    LOAD_DIFF m5, m3, none, [r0+  r1+offset], [r2+  r3+offset]
    LOAD_DIFF m6, m3, none, [r0+2*r1+offset], [r2+2*r3+offset]
    LOAD_DIFF m7, m3, none, [r0+  r4+offset], [r2+  r5+offset]
%if %3
    lea  r0, [r0+4*r1]
    lea  r2, [r2+4*r3]
%endif
    HADAMARD4_2D 4, 5, 6, 7, 3, %%n
    paddw m4, m6
;%if HIGH_BIT_DEPTH && (BIT_DEPTH == 12)
;    pxor m5, m5
;    punpcklwd m6, m4, m5
;    punpckhwd m4, m5
;    paddd m4, m6
;%endif
    SWAP %%n, 4
%endmacro

; in: %1 = horizontal if 0, vertical if 1
%macro SATD_8x4_SSE 8-9
%if %1
    HADAMARD4_2D_SSE %2, %3, %4, %5, %6, amax
%else
    HADAMARD4_V %2, %3, %4, %5, %6
    ; doing the abs first is a slight advantage
    ABSW2 m%2, m%4, m%2, m%4, m%6, m%7
    ABSW2 m%3, m%5, m%3, m%5, m%6, m%7
    HADAMARD 1, max, %2, %4, %6, %7
%endif
%ifnidn %9, swap
  %if (BIT_DEPTH == 12)
    pxor m%6, m%6
    punpcklwd m%7, m%2, m%6
    punpckhwd m%2, m%6
    paddd m%8, m%7
    paddd m%8, m%2
  %else
    paddw m%8, m%2
  %endif
%else
    SWAP %8, %2
  %if (BIT_DEPTH == 12)
    pxor m%6, m%6
    punpcklwd m%7, m%8, m%6
    punpckhwd m%8, m%6
    paddd m%8, m%7
  %endif
%endif
%if %1
  %if (BIT_DEPTH == 12)
    pxor m%6, m%6
    punpcklwd m%7, m%4, m%6
    punpckhwd m%4, m%6
    paddd m%8, m%7
    paddd m%8, m%4
  %else
    paddw m%8, m%4
  %endif
%else
    HADAMARD 1, max, %3, %5, %6, %7
  %if (BIT_DEPTH == 12)
    pxor m%6, m%6
    punpcklwd m%7, m%3, m%6
    punpckhwd m%3, m%6
    paddd m%8, m%7
    paddd m%8, m%3
  %else
    paddw m%8, m%3
  %endif
%endif
%endmacro

%macro SATD_8x4_1_SSE 10
%if %1
    HADAMARD4_2D_SSE %2, %3, %4, %5, %6, amax
%else
    HADAMARD4_V %2, %3, %4, %5, %6
    ; doing the abs first is a slight advantage
    ABSW2 m%2, m%4, m%2, m%4, m%6, m%7
    ABSW2 m%3, m%5, m%3, m%5, m%6, m%7
    HADAMARD 1, max, %2, %4, %6, %7
%endif

    pxor m%10, m%10
    punpcklwd m%9, m%2, m%10
    paddd m%8, m%9
    punpckhwd m%9, m%2, m%10
    paddd m%8, m%9

%if %1
    pxor m%10, m%10
    punpcklwd m%9, m%4, m%10
    paddd m%8, m%9
    punpckhwd m%9, m%4, m%10
    paddd m%8, m%9
%else
    HADAMARD 1, max, %3, %5, %6, %7
    pxor m%10, m%10
    punpcklwd m%9, m%3, m%10
    paddd m%8, m%9
    punpckhwd m%9, m%3, m%10
    paddd m%8, m%9
%endif
%endmacro

%macro SATD_START_MMX 0
    FIX_STRIDES r1, r3
    lea  r4, [3*r1] ; 3*stride1
    lea  r5, [3*r3] ; 3*stride2
%endmacro

%macro SATD_END_MMX 0
%if HIGH_BIT_DEPTH
    HADDUW      m0, m1
    movd       eax, m0
%else ; !HIGH_BIT_DEPTH
    pshufw      m1, m0, q1032
    paddw       m0, m1
    pshufw      m1, m0, q2301
    paddw       m0, m1
    movd       eax, m0
    and        eax, 0xffff
%endif ; HIGH_BIT_DEPTH
    EMMS
    RET
%endmacro

; FIXME avoid the spilling of regs to hold 3*stride.
; for small blocks on x86_32, modify pixel pointer instead.

;-----------------------------------------------------------------------------
; int pixel_satd_16x16( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
INIT_MMX mmx2
cglobal pixel_satd_4x4, 4,6
    SATD_START_MMX
    SATD_4x4_MMX m0, 0, 0
    SATD_END_MMX

%macro SATD_START_SSE2 2-3 0
    FIX_STRIDES r1, r3
%if HIGH_BIT_DEPTH && %3
    pxor    %2, %2
%elif cpuflag(ssse3) && notcpuflag(atom)
%if mmsize==32
    mova    %2, [hmul_16p]
%else
    mova    %2, [hmul_8p]
%endif
%endif
    lea     r4, [3*r1]
    lea     r5, [3*r3]
    pxor    %1, %1
%endmacro

%macro SATD_END_SSE2 1-2
%if HIGH_BIT_DEPTH
  %if BIT_DEPTH == 12
    HADDD   %1, xm0
  %else ; BIT_DEPTH == 12
    HADDUW  %1, xm0
  %endif ; BIT_DEPTH == 12
  %if %0 == 2
    paddd   %1, %2
  %endif
%else
    HADDW   %1, xm7
%endif
    movd   eax, %1
    RET
%endmacro

%macro SATD_ACCUM 3
%if HIGH_BIT_DEPTH
    HADDUW %1, %2
    paddd  %3, %1
    pxor   %1, %1
%endif
%endmacro

%macro BACKUP_POINTERS 0
%if ARCH_X86_64
%if WIN64
    PUSH r7
%endif
    mov     r6, r0
    mov     r7, r2
%endif
%endmacro

%macro RESTORE_AND_INC_POINTERS 0
%if ARCH_X86_64
    lea     r0, [r6+8*SIZEOF_PIXEL]
    lea     r2, [r7+8*SIZEOF_PIXEL]
%if WIN64
    POP r7
%endif
%else
    mov     r0, r0mp
    mov     r2, r2mp
    add     r0, 8*SIZEOF_PIXEL
    add     r2, 8*SIZEOF_PIXEL
%endif
%endmacro

%macro SATD_4x8_SSE 3-4
%if HIGH_BIT_DEPTH
    movh    m0, [r0+0*r1]
    movh    m4, [r2+0*r3]
    movh    m1, [r0+1*r1]
    movh    m5, [r2+1*r3]
    movhps  m0, [r0+4*r1]
    movhps  m4, [r2+4*r3]
    movh    m2, [r0+2*r1]
    movh    m6, [r2+2*r3]
    psubw   m0, m4
    movh    m3, [r0+r4]
    movh    m4, [r2+r5]
    lea     r0, [r0+4*r1]
    lea     r2, [r2+4*r3]
    movhps  m1, [r0+1*r1]
    movhps  m5, [r2+1*r3]
    movhps  m2, [r0+2*r1]
    movhps  m6, [r2+2*r3]
    psubw   m1, m5
    movhps  m3, [r0+r4]
    movhps  m4, [r2+r5]
    psubw   m2, m6
    psubw   m3, m4
%else ; !HIGH_BIT_DEPTH
    movd m4, [r2]
    movd m5, [r2+r3]
    movd m6, [r2+2*r3]
    add r2, r5
    movd m0, [r0]
    movd m1, [r0+r1]
    movd m2, [r0+2*r1]
    add r0, r4
    movd m3, [r2+r3]
    JDUP m4, m3
    movd m3, [r0+r1]
    JDUP m0, m3
    movd m3, [r2+2*r3]
    JDUP m5, m3
    movd m3, [r0+2*r1]
    JDUP m1, m3
%if %1==0 && %2==1
    mova m3, [hmul_4p]
    DIFFOP 0, 4, 1, 5, 3
%else
    DIFFOP 0, 4, 1, 5, 7
%endif
    movd m5, [r2]
    add r2, r5
    movd m3, [r0]
    add r0, r4
    movd m4, [r2]
    JDUP m6, m4
    movd m4, [r0]
    JDUP m2, m4
    movd m4, [r2+r3]
    JDUP m5, m4
    movd m4, [r0+r1]
    JDUP m3, m4
%if %1==0 && %2==1
    mova m4, [hmul_4p]
    DIFFOP 2, 6, 3, 5, 4
%else
    DIFFOP 2, 6, 3, 5, 7
%endif
%endif ; HIGH_BIT_DEPTH
%if %0 == 4
    SATD_8x4_1_SSE %1, 0, 1, 2, 3, 4, 5, 7, %3, %4
%else
    SATD_8x4_SSE %1, 0, 1, 2, 3, 4, 5, 7, %3
%endif
%endmacro

;-----------------------------------------------------------------------------
; int pixel_satd_8x4( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
%macro SATDS_SSE2 0
%define vertical ((notcpuflag(ssse3) || cpuflag(atom)) || HIGH_BIT_DEPTH)

%if cpuflag(ssse3) && (vertical==0 || HIGH_BIT_DEPTH)
cglobal pixel_satd_4x4, 4, 6, 6
    SATD_START_MMX
    mova m4, [hmul_4p]
    LOAD_DUP_2x4P m2, m5, [r2], [r2+r3]
    LOAD_DUP_2x4P m3, m5, [r2+2*r3], [r2+r5]
    LOAD_DUP_2x4P m0, m5, [r0], [r0+r1]
    LOAD_DUP_2x4P m1, m5, [r0+2*r1], [r0+r4]
    DIFF_SUMSUB_SSSE3 0, 2, 1, 3, 4
    HADAMARD 0, sumsub, 0, 1, 2, 3
    HADAMARD 4, sumsub, 0, 1, 2, 3
    HADAMARD 1, amax, 0, 1, 2, 3
    HADDW m0, m1
    movd eax, m0
    RET
%endif

cglobal pixel_satd_4x8, 4, 6, 8
    SATD_START_MMX
%if vertical==0
    mova m7, [hmul_4p]
%endif
    SATD_4x8_SSE vertical, 0, swap
%if BIT_DEPTH == 12
    HADDD m7, m1
%else
    HADDUW m7, m1
%endif
    movd eax, m7
    RET

cglobal pixel_satd_4x16, 4, 6, 8
    SATD_START_MMX
%if vertical==0
    mova m7, [hmul_4p]
%endif
    SATD_4x8_SSE vertical, 0, swap
    lea r0, [r0+r1*2*SIZEOF_PIXEL]
    lea r2, [r2+r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
%if BIT_DEPTH == 12
    HADDD m7, m1
%else
    HADDUW m7, m1
%endif
    movd eax, m7
    RET

cglobal pixel_satd_8x8_internal
    LOAD_SUMSUB_8x4P 0, 1, 2, 3, 4, 5, 7, r0, r2, 1, 0
    SATD_8x4_SSE vertical, 0, 1, 2, 3, 4, 5, 6
%%pixel_satd_8x4_internal:
    LOAD_SUMSUB_8x4P 0, 1, 2, 3, 4, 5, 7, r0, r2, 1, 0
    SATD_8x4_SSE vertical, 0, 1, 2, 3, 4, 5, 6
    ret

cglobal pixel_satd_8x8_internal2
%if WIN64
    LOAD_SUMSUB_8x4P 0, 1, 2, 3, 4, 5, 7, r0, r2, 1, 0
    SATD_8x4_1_SSE vertical, 0, 1, 2, 3, 4, 5, 6, 12, 13
%%pixel_satd_8x4_internal2:
    LOAD_SUMSUB_8x4P 0, 1, 2, 3, 4, 5, 7, r0, r2, 1, 0
    SATD_8x4_1_SSE vertical, 0, 1, 2, 3, 4, 5, 6, 12, 13
%else
    LOAD_SUMSUB_8x4P 0, 1, 2, 3, 4, 5, 7, r0, r2, 1, 0
    SATD_8x4_1_SSE vertical, 0, 1, 2, 3, 4, 5, 6, 4, 5
%%pixel_satd_8x4_internal2:
    LOAD_SUMSUB_8x4P 0, 1, 2, 3, 4, 5, 7, r0, r2, 1, 0
    SATD_8x4_1_SSE vertical, 0, 1, 2, 3, 4, 5, 6, 4, 5
%endif
    ret

; 16x8 regresses on phenom win64, 16x16 is almost the same (too many spilled registers)
; These aren't any faster on AVX systems with fast movddup (Bulldozer, Sandy Bridge)
%if HIGH_BIT_DEPTH == 0 && (WIN64 || UNIX64) && notcpuflag(avx)

cglobal pixel_satd_16x4_internal2
    LOAD_SUMSUB_16x4P 0, 1, 2, 3, 4, 8, 5, 9, 6, 7, r0, r2, 11
    lea  r2, [r2+4*r3]
    lea  r0, [r0+4*r1]
    SATD_8x4_1_SSE 0, 0, 1, 2, 3, 6, 11, 10, 12, 13
    SATD_8x4_1_SSE 0, 4, 8, 5, 9, 6, 3, 10, 12, 13
    ret

cglobal pixel_satd_16x4, 4,6,14
    SATD_START_SSE2 m10, m7
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_16x8, 4,6,14
    SATD_START_SSE2 m10, m7
%if vertical
    mova m7, [pw_00ff]
%endif
    jmp %%pixel_satd_16x8_internal

cglobal pixel_satd_16x12, 4,6,14
    SATD_START_SSE2 m10, m7
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    jmp %%pixel_satd_16x8_internal

cglobal pixel_satd_16x32, 4,6,14
    SATD_START_SSE2 m10, m7
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    jmp %%pixel_satd_16x8_internal

cglobal pixel_satd_16x64, 4,6,14
    SATD_START_SSE2 m10, m7
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    jmp %%pixel_satd_16x8_internal

cglobal pixel_satd_16x16, 4,6,14
    SATD_START_SSE2 m10, m7
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
%%pixel_satd_16x8_internal:
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_32x8, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_32x16, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd    eax, m10
    RET

cglobal pixel_satd_32x24, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_32x32, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_32x64, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_48x64, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 32]
    lea r2, [r7 + 32]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_64x16, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 32]
    lea r2, [r7 + 32]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 48]
    lea r2, [r7 + 48]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_64x32, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 32]
    lea r2, [r7 + 32]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 48]
    lea r2, [r7 + 48]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2

    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_64x48, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 32]
    lea r2, [r7 + 32]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 48]
    lea r2, [r7 + 48]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2

    HADDD m10, m0
    movd eax, m10
    RET

cglobal pixel_satd_64x64, 4,8,14    ;if WIN64 && notcpuflag(avx)
    SATD_START_SSE2 m10, m7
    mov r6, r0
    mov r7, r2
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 16]
    lea r2, [r7 + 16]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 32]
    lea r2, [r7 + 32]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    lea r0, [r6 + 48]
    lea r2, [r7 + 48]
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2
    call pixel_satd_16x4_internal2

    HADDD m10, m0
    movd eax, m10
    RET

%else
%if WIN64
cglobal pixel_satd_16x24, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd   eax, m6
    RET
%else
cglobal pixel_satd_16x24, 4,7,8,0-gprsize    ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif
%if WIN64
cglobal pixel_satd_32x48, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_32x48, 4,7,8,0-gprsize    ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_24x64, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_24x64, 4,7,8,0-gprsize    ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_8x64, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_8x64, 4,7,8,0-gprsize    ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_8x12, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call %%pixel_satd_8x4_internal2
    pxor    m7, m7
    movhlps m7, m6
    paddd   m6, m7
    pshufd  m7, m6, 1
    paddd   m6, m7
    movd   eax, m6
    RET
%else
cglobal pixel_satd_8x12, 4,7,8,0-gprsize    ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call %%pixel_satd_8x4_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if HIGH_BIT_DEPTH
%if WIN64
cglobal pixel_satd_12x32, 4,8,8   ;if WIN64 && cpuflag(avx)
    SATD_START_MMX
    mov r6, r0
    mov r7, r2
    pxor m7, m7
    SATD_4x8_SSE vertical, 0, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r6 + 4*SIZEOF_PIXEL]
    lea r2, [r7 + 4*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    HADDD m7, m0
    movd eax, m7
    RET
%else
cglobal pixel_satd_12x32, 4,7,8,0-gprsize
    SATD_START_MMX
    mov r6, r0
    mov [rsp], r2
    pxor m7, m7
    SATD_4x8_SSE vertical, 0, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r6 + 4*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 4*SIZEOF_PIXEL
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    HADDD m7, m0
    movd eax, m7
    RET
%endif
%else ;HIGH_BIT_DEPTH
%if WIN64
cglobal pixel_satd_12x32, 4,8,8   ;if WIN64 && cpuflag(avx)
    SATD_START_MMX
    mov r6, r0
    mov r7, r2
%if vertical==0
    mova m7, [hmul_4p]
%endif
    SATD_4x8_SSE vertical, 0, swap
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r6 + 4*SIZEOF_PIXEL]
    lea r2, [r7 + 4*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    HADDW m7, m1
    movd eax, m7
    RET
%else
cglobal pixel_satd_12x32, 4,7,8,0-gprsize
    SATD_START_MMX
    mov r6, r0
    mov [rsp], r2
%if vertical==0
    mova m7, [hmul_4p]
%endif
    SATD_4x8_SSE vertical, 0, swap
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r6 + 4*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 4*SIZEOF_PIXEL
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    HADDW m7, m1
    movd eax, m7
    RET
%endif
%endif

%if HIGH_BIT_DEPTH
%if WIN64
cglobal pixel_satd_4x32, 4,8,8   ;if WIN64 && cpuflag(avx)
    SATD_START_MMX
    mov r6, r0
    mov r7, r2
    pxor m7, m7
    SATD_4x8_SSE vertical, 0, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    HADDD m7, m0
    movd eax, m7
    RET
%else
cglobal pixel_satd_4x32, 4,7,8,0-gprsize
    SATD_START_MMX
    mov r6, r0
    mov [rsp], r2
    pxor m7, m7
    SATD_4x8_SSE vertical, 0, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    pxor    m1, m1
    movhlps m1, m7
    paddd   m7, m1
    pshufd  m1, m7, 1
    paddd   m7, m1
    movd   eax, m7
    RET
%endif
%else
%if WIN64
cglobal pixel_satd_4x32, 4,8,8   ;if WIN64 && cpuflag(avx)
    SATD_START_MMX
    mov r6, r0
    mov r7, r2
%if vertical==0
    mova m7, [hmul_4p]
%endif
    SATD_4x8_SSE vertical, 0, swap
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    HADDW m7, m1
    movd eax, m7
    RET
%else
cglobal pixel_satd_4x32, 4,7,8,0-gprsize
    SATD_START_MMX
    mov r6, r0
    mov [rsp], r2
%if vertical==0
    mova m7, [hmul_4p]
%endif
    SATD_4x8_SSE vertical, 0, swap
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    HADDW m7, m1
    movd eax, m7
    RET
%endif
%endif

%if WIN64
cglobal pixel_satd_32x8, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_32x8, 4,7,8,0-gprsize    ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_32x16, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_32x16, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_32x24, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_32x24, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_32x32, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_32x32, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_32x64, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_32x64, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_48x64, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    lea r2, [r7 + 32*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    lea r2, [r7 + 40*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_48x64, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,32*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,40*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif


%if WIN64
cglobal pixel_satd_64x16, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    lea r2, [r7 + 32*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    lea r2, [r7 + 40*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 48*SIZEOF_PIXEL]
    lea r2, [r7 + 48*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 56*SIZEOF_PIXEL]
    lea r2, [r7 + 56*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_64x16, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,32*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,40*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 48*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,48*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 56*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2,56*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_64x32, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    lea r2, [r7 + 32*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    lea r2, [r7 + 40*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 48*SIZEOF_PIXEL]
    lea r2, [r7 + 48*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 56*SIZEOF_PIXEL]
    lea r2, [r7 + 56*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_64x32, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 32*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 40*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 48*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 48*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 56*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 56*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_64x48, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    lea r2, [r7 + 32*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    lea r2, [r7 + 40*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 48*SIZEOF_PIXEL]
    lea r2, [r7 + 48*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 56*SIZEOF_PIXEL]
    lea r2, [r7 + 56*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_64x48, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 32*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 40*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 48*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 48*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 56*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 56*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_64x64, 4,8,14    ;if WIN64 && cpuflag(avx)
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    lea r2, [r7 + 24*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    lea r2, [r7 + 32*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    lea r2, [r7 + 40*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 48*SIZEOF_PIXEL]
    lea r2, [r7 + 48*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 56*SIZEOF_PIXEL]
    lea r2, [r7 + 56*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_64x64, 4,7,8,0-gprsize   ;if !WIN64
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 24*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 24*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 32*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 32*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 40*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 40*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 48*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 48*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 56*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 56*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if WIN64
cglobal pixel_satd_16x4, 4,6,14
%else
cglobal pixel_satd_16x4, 4,6,8
%endif
    SATD_START_SSE2 m6, m7
    BACKUP_POINTERS
    call %%pixel_satd_8x4_internal2
    RESTORE_AND_INC_POINTERS
    call %%pixel_satd_8x4_internal2
    HADDD m6, m0
    movd eax, m6
    RET

%if WIN64
cglobal pixel_satd_16x8, 4,6,14
%else
cglobal pixel_satd_16x8, 4,6,8
%endif
    SATD_START_SSE2 m6, m7
    BACKUP_POINTERS
    call pixel_satd_8x8_internal2
    RESTORE_AND_INC_POINTERS
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET

%if WIN64
cglobal pixel_satd_16x12, 4,6,14
%else
cglobal pixel_satd_16x12, 4,6,8
%endif
    SATD_START_SSE2 m6, m7, 1
    BACKUP_POINTERS
    call pixel_satd_8x8_internal2
    call %%pixel_satd_8x4_internal2
    RESTORE_AND_INC_POINTERS
    call pixel_satd_8x8_internal2
    call %%pixel_satd_8x4_internal2
    HADDD m6, m0
    movd eax, m6
    RET

%if WIN64
cglobal pixel_satd_16x16, 4,6,14
%else
cglobal pixel_satd_16x16, 4,6,8
%endif
    SATD_START_SSE2 m6, m7, 1
    BACKUP_POINTERS
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    RESTORE_AND_INC_POINTERS
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET

%if WIN64
cglobal pixel_satd_16x32, 4,6,14
%else
cglobal pixel_satd_16x32, 4,6,8
%endif
    SATD_START_SSE2 m6, m7, 1
    BACKUP_POINTERS
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    RESTORE_AND_INC_POINTERS
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET

%if WIN64
cglobal pixel_satd_16x64, 4,6,14
%else
cglobal pixel_satd_16x64, 4,6,8
%endif
    SATD_START_SSE2 m6, m7, 1
    BACKUP_POINTERS
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    RESTORE_AND_INC_POINTERS
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif

%if HIGH_BIT_DEPTH
%if WIN64
cglobal pixel_satd_12x16, 4,8,8
    SATD_START_MMX
    mov r6, r0
    mov r7, r2
    pxor m7, m7
    SATD_4x8_SSE vertical, 0, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r6 + 4*SIZEOF_PIXEL]
    lea r2, [r7 + 4*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    HADDD m7, m0
    movd eax, m7
    RET
%else
cglobal pixel_satd_12x16, 4,7,8,0-gprsize
    SATD_START_MMX
    mov r6, r0
    mov [rsp], r2
    pxor m7, m7
    SATD_4x8_SSE vertical, 0, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r6 + 4*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 4*SIZEOF_PIXEL
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    SATD_4x8_SSE vertical, 1, 4, 5
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, 4, 5
    HADDD m7, m0
    movd eax, m7
    RET
%endif
%else    ;HIGH_BIT_DEPTH
%if WIN64
cglobal pixel_satd_12x16, 4,8,8
    SATD_START_MMX
    mov r6, r0
    mov r7, r2
%if vertical==0
    mova m7, [hmul_4p]
%endif
    SATD_4x8_SSE vertical, 0, swap
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r6 + 4*SIZEOF_PIXEL]
    lea r2, [r7 + 4*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    HADDW m7, m1
    movd eax, m7
    RET
%else
cglobal pixel_satd_12x16, 4,7,8,0-gprsize
    SATD_START_MMX
    mov r6, r0
    mov [rsp], r2
%if vertical==0
    mova m7, [hmul_4p]
%endif
    SATD_4x8_SSE vertical, 0, swap
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r6 + 4*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 4*SIZEOF_PIXEL
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    SATD_4x8_SSE vertical, 1, add
    lea r0, [r0 + r1*2*SIZEOF_PIXEL]
    lea r2, [r2 + r3*2*SIZEOF_PIXEL]
    SATD_4x8_SSE vertical, 1, add
    HADDW m7, m1
    movd eax, m7
    RET
%endif
%endif

%if WIN64
cglobal pixel_satd_24x32, 4,8,14
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov r7, r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    lea r2, [r7 + 8*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    lea r2, [r7 + 16*SIZEOF_PIXEL]
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%else
cglobal pixel_satd_24x32, 4,7,8,0-gprsize
    SATD_START_SSE2 m6, m7
    mov r6, r0
    mov [rsp], r2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 8*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 8*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    lea r0, [r6 + 16*SIZEOF_PIXEL]
    mov r2, [rsp]
    add r2, 16*SIZEOF_PIXEL
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET
%endif    ;WIN64

%if WIN64
cglobal pixel_satd_8x32, 4,6,14
%else
cglobal pixel_satd_8x32, 4,6,8
%endif
    SATD_START_SSE2 m6, m7
%if vertical
    mova m7, [pw_00ff]
%endif
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET

%if WIN64
cglobal pixel_satd_8x16, 4,6,14
%else
cglobal pixel_satd_8x16, 4,6,8
%endif
    SATD_START_SSE2 m6, m7
    call pixel_satd_8x8_internal2
    call pixel_satd_8x8_internal2
    HADDD m6, m0
    movd eax, m6
    RET

cglobal pixel_satd_8x8, 4,6,8
    SATD_START_SSE2 m6, m7
    call pixel_satd_8x8_internal
    SATD_END_SSE2 m6

%if WIN64
cglobal pixel_satd_8x4, 4,6,14
%else
cglobal pixel_satd_8x4, 4,6,8
%endif
    SATD_START_SSE2 m6, m7
    call %%pixel_satd_8x4_internal2
    SATD_END_SSE2 m6
%endmacro ; SATDS_SSE2


;=============================================================================
; SA8D
;=============================================================================

%macro SA8D_INTER 0
%if ARCH_X86_64
    %define lh m10
    %define rh m0
%else
    %define lh m0
    %define rh [esp+48]
%endif
%if HIGH_BIT_DEPTH
    HADDUW  m0, m1
    paddd   lh, rh
%else
    paddusw lh, rh
%endif ; HIGH_BIT_DEPTH
%endmacro

%macro SA8D_8x8 0
    call pixel_sa8d_8x8_internal
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%else
    HADDW m0, m1
%endif ; HIGH_BIT_DEPTH
    paddd  m0, [pd_1]
    psrld  m0, 1
    paddd  m12, m0
%endmacro

%macro SA8D_16x16 0
    call pixel_sa8d_8x8_internal ; pix[0]
    add  r2, 8*SIZEOF_PIXEL
    add  r0, 8*SIZEOF_PIXEL
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova m10, m0
    call pixel_sa8d_8x8_internal ; pix[8]
    lea  r2, [r2+8*r3]
    lea  r0, [r0+8*r1]
    SA8D_INTER
    call pixel_sa8d_8x8_internal ; pix[8*stride+8]
    sub  r2, 8*SIZEOF_PIXEL
    sub  r0, 8*SIZEOF_PIXEL
    SA8D_INTER
    call pixel_sa8d_8x8_internal ; pix[8*stride]
    SA8D_INTER
    SWAP 0, 10
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    paddd  m0, [pd_1]
    psrld  m0, 1
    paddd  m12, m0
%endmacro

%macro AVG_16x16 0
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov dword [esp+36], r4d
%endmacro

%macro SA8D 0
; sse2 doesn't seem to like the horizontal way of doing things
%define vertical ((notcpuflag(ssse3) || cpuflag(atom)) || HIGH_BIT_DEPTH)

%if ARCH_X86_64
;-----------------------------------------------------------------------------
; int pixel_sa8d_8x8( uint8_t *, intptr_t, uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
cglobal pixel_sa8d_8x8_internal
    lea  r6, [r0+4*r1]
    lea  r7, [r2+4*r3]
    LOAD_SUMSUB_8x4P 0, 1, 2, 8, 5, 6, 7, r0, r2
    LOAD_SUMSUB_8x4P 4, 5, 3, 9, 11, 6, 7, r6, r7
%if vertical
    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
%else ; non-sse2
    HADAMARD8_2D_HMUL 0, 1, 2, 8, 4, 5, 3, 9, 6, 11
%endif
    paddw m0, m1
    paddw m0, m2
    paddw m0, m8
    SAVE_MM_PERMUTATION
    ret

cglobal pixel_sa8d_8x8, 4,8,12
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    call pixel_sa8d_8x8_internal
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%else
    HADDW m0, m1
%endif ; HIGH_BIT_DEPTH
    movd eax, m0
    add eax, 1
    shr eax, 1
    RET

cglobal pixel_sa8d_16x16, 4,8,12
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    call pixel_sa8d_8x8_internal ; pix[0]
    add  r2, 8*SIZEOF_PIXEL
    add  r0, 8*SIZEOF_PIXEL
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova m10, m0
    call pixel_sa8d_8x8_internal ; pix[8]
    lea  r2, [r2+8*r3]
    lea  r0, [r0+8*r1]
    SA8D_INTER
    call pixel_sa8d_8x8_internal ; pix[8*stride+8]
    sub  r2, 8*SIZEOF_PIXEL
    sub  r0, 8*SIZEOF_PIXEL
    SA8D_INTER
    call pixel_sa8d_8x8_internal ; pix[8*stride]
    SA8D_INTER
    SWAP 0, 10
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd eax, m0
    add  eax, 1
    shr  eax, 1
    RET

cglobal pixel_sa8d_8x16, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_8x8
    lea r0, [r0 + 8*r1]
    lea r2, [r2 + 8*r3]
    SA8D_8x8
    movd eax, m12
    RET

cglobal pixel_sa8d_8x32, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_8x8
    lea r0, [r0 + r1*8]
    lea r2, [r2 + r3*8]
    SA8D_8x8
    lea r0, [r0 + r1*8]
    lea r2, [r2 + r3*8]
    SA8D_8x8
    lea r0, [r0 + r1*8]
    lea r2, [r2 + r3*8]
    SA8D_8x8
    movd eax, m12
    RET

cglobal pixel_sa8d_16x8, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    movd eax, m12
    RET

cglobal pixel_sa8d_16x32, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_16x64, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_24x32, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    lea r0, [r0 + r1*8]
    lea r2, [r2 + r3*8]
    SA8D_8x8
    sub r0, 8*SIZEOF_PIXEL
    sub r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    sub r0, 8*SIZEOF_PIXEL
    sub r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    lea r0, [r0 + r1*8]
    lea r2, [r2 + r3*8]
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    lea r0, [r0 + r1*8]
    lea r2, [r2 + r3*8]
    SA8D_8x8
    sub r0, 8*SIZEOF_PIXEL
    sub r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    sub r0, 8*SIZEOF_PIXEL
    sub r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    movd eax, m12
    RET

cglobal pixel_sa8d_32x8, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    movd eax, m12
    RET

cglobal pixel_sa8d_32x16, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_32x24, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    lea r0, [r0 + r1*8]
    lea r2, [r2 + r3*8]
    SA8D_8x8
    sub r0, 8*SIZEOF_PIXEL
    sub r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    sub r0, 8*SIZEOF_PIXEL
    sub r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    sub r0, 8*SIZEOF_PIXEL
    sub r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    lea r0, [r0 + r1*8]
    lea r2, [r2 + r3*8]
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    add r0, 8*SIZEOF_PIXEL
    add r2, 8*SIZEOF_PIXEL
    SA8D_8x8
    movd eax, m12
    RET

cglobal pixel_sa8d_32x32, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_32x64, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_48x64, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_64x16, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_64x32, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_64x48, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    movd eax, m12
    RET

cglobal pixel_sa8d_64x64, 4,8,13
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    pxor m12, m12
%if vertical == 0
    mova m7, [hmul_8p]
%endif
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    add  r2, 16*SIZEOF_PIXEL
    add  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea r0, [r0+8*r1]
    lea r2, [r2+8*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    lea  r4, [8*r1]
    lea  r5, [8*r3]
    sub  r0, r4
    sub  r2, r5
    sub  r2, 16*SIZEOF_PIXEL
    sub  r0, 16*SIZEOF_PIXEL
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    SA8D_16x16
    movd eax, m12
    RET

%else ; ARCH_X86_32
%if mmsize == 16
cglobal pixel_sa8d_8x8_internal
    %define spill0 [esp+4]
    %define spill1 [esp+20]
    %define spill2 [esp+36]
%if vertical
    LOAD_DIFF_8x4P 0, 1, 2, 3, 4, 5, 6, r0, r2, 1
    HADAMARD4_2D 0, 1, 2, 3, 4
    movdqa spill0, m3
    LOAD_DIFF_8x4P 4, 5, 6, 7, 3, 3, 2, r0, r2, 1
    HADAMARD4_2D 4, 5, 6, 7, 3
    HADAMARD2_2D 0, 4, 1, 5, 3, qdq, amax
    movdqa m3, spill0
    paddw m0, m1
    HADAMARD2_2D 2, 6, 3, 7, 5, qdq, amax
%else ; mmsize == 8
    mova m7, [hmul_8p]
    LOAD_SUMSUB_8x4P 0, 1, 2, 3, 5, 6, 7, r0, r2, 1
    ; could do first HADAMARD4_V here to save spilling later
    ; surprisingly, not a win on conroe or even p4
    mova spill0, m2
    mova spill1, m3
    mova spill2, m1
    SWAP 1, 7
    LOAD_SUMSUB_8x4P 4, 5, 6, 7, 2, 3, 1, r0, r2, 1
    HADAMARD4_V 4, 5, 6, 7, 3
    mova m1, spill2
    mova m2, spill0
    mova m3, spill1
    mova spill0, m6
    mova spill1, m7
    HADAMARD4_V 0, 1, 2, 3, 7
    SUMSUB_BADC w, 0, 4, 1, 5, 7
    HADAMARD 2, sumsub, 0, 4, 7, 6
    HADAMARD 2, sumsub, 1, 5, 7, 6
    HADAMARD 1, amax, 0, 4, 7, 6
    HADAMARD 1, amax, 1, 5, 7, 6
    mova m6, spill0
    mova m7, spill1
    paddw m0, m1
    SUMSUB_BADC w, 2, 6, 3, 7, 4
    HADAMARD 2, sumsub, 2, 6, 4, 5
    HADAMARD 2, sumsub, 3, 7, 4, 5
    HADAMARD 1, amax, 2, 6, 4, 5
    HADAMARD 1, amax, 3, 7, 4, 5
%endif ; sse2/non-sse2
    paddw m0, m2
    paddw m0, m3
    SAVE_MM_PERMUTATION
    ret
%endif ; ifndef mmx2

cglobal pixel_sa8d_8x8_internal2
    %define spill0 [esp+4]
    LOAD_DIFF_8x4P 0, 1, 2, 3, 4, 5, 6, r0, r2, 1
    HADAMARD4_2D 0, 1, 2, 3, 4
    movdqa spill0, m3
    LOAD_DIFF_8x4P 4, 5, 6, 7, 3, 3, 2, r0, r2, 1
    HADAMARD4_2D 4, 5, 6, 7, 3
    HADAMARD2_2D 0, 4, 1, 5, 3, qdq, amax
    movdqa m3, spill0
    paddw m0, m1
    HADAMARD2_2D 2, 6, 3, 7, 5, qdq, amax
    paddw m0, m2
    paddw m0, m3
    SAVE_MM_PERMUTATION
    ret

cglobal pixel_sa8d_8x8, 4,7
    FIX_STRIDES r1, r3
    mov    r6, esp
    and   esp, ~15
    sub   esp, 48
    lea    r4, [3*r1]
    lea    r5, [3*r3]
    call pixel_sa8d_8x8_internal
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%else
    HADDW  m0, m1
%endif ; HIGH_BIT_DEPTH
    movd  eax, m0
    add   eax, 1
    shr   eax, 1
    mov   esp, r6
    RET

cglobal pixel_sa8d_16x16, 4,7
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    call pixel_sa8d_8x8_internal
%if mmsize == 8
    lea  r0, [r0+4*r1]
    lea  r2, [r2+4*r3]
%endif
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal
    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal
%if mmsize == 8
    lea  r0, [r0+4*r1]
    lea  r2, [r2+4*r3]
%else
    SA8D_INTER
%endif
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal
%if HIGH_BIT_DEPTH
    SA8D_INTER
%else ; !HIGH_BIT_DEPTH
    paddusw m0, [esp+64-mmsize]
%if mmsize == 16
    HADDUW m0, m1
%else
    mova m2, [esp+48]
    pxor m7, m7
    mova m1, m0
    mova m3, m2
    punpcklwd m0, m7
    punpckhwd m1, m7
    punpcklwd m2, m7
    punpckhwd m3, m7
    paddd m0, m1
    paddd m2, m3
    paddd m0, m2
    HADDD m0, m1
%endif
%endif ; HIGH_BIT_DEPTH
    movd eax, m0
    add  eax, 1
    shr  eax, 1
    mov  esp, r6
    RET

cglobal pixel_sa8d_8x16, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_8x32, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_16x8, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_16x32, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_16x64, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_24x32, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_32x8, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_32x16, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_32x24, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
    HADDUW m0, m1
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add  r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_32x32, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_32x64, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_48x64, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_64x16, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_64x32, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_64x48, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET

cglobal pixel_sa8d_64x64, 4,7,8
    FIX_STRIDES r1, r3
    mov  r6, esp
    and  esp, ~15
    sub  esp, 64

    lea  r4, [r1 + 2*r1]
    lea  r5, [r3 + 2*r3]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [rsp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    mov dword [esp+36], r4d

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    lea  r0, [r0 + r1*8]
    lea  r2, [r2 + r3*8]
    mov  [r6+20], r0
    mov  [r6+28], r2

    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 8*SIZEOF_PIXEL
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 16*SIZEOF_PIXEL
    add  r2, 16*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 24*SIZEOF_PIXEL
    add  r2, 24*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 32*SIZEOF_PIXEL
    add  r2, 32*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 40*SIZEOF_PIXEL
    add  r2, 40*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    AVG_16x16

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 48*SIZEOF_PIXEL
    add  r2, 48*SIZEOF_PIXEL
    lea  r4, [r1 + 2*r1]
    call pixel_sa8d_8x8_internal2
%if HIGH_BIT_DEPTH
    HADDUW m0, m1
%endif
    mova [esp+48], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+48], m0

    mov  r0, [r6+20]
    mov  r2, [r6+28]
    add  r0, 56*SIZEOF_PIXEL
    add  r2, 56*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
    mova [esp+64-mmsize], m0
    call pixel_sa8d_8x8_internal2
    SA8D_INTER
%if HIGH_BIT_DEPTH == 0
    HADDUW m0, m1
%endif
    movd r4d, m0
    add  r4d, 1
    shr  r4d, 1
    add r4d, dword [esp+36]
    mov eax, r4d
    mov esp, r6
    RET
%endif ; !ARCH_X86_64
%endmacro ; SA8D


%if ARCH_X86_64 == 1 && BIT_DEPTH == 12
INIT_YMM avx2
cglobal sa8d_8x8_12bit
    pmovzxwd        m0, [r0]
    pmovzxwd        m9, [r2]
    psubd           m0, m9

    pmovzxwd        m1, [r0 + r1]
    pmovzxwd        m9, [r2 + r3]
    psubd           m1, m9

    pmovzxwd        m2, [r0 + r1 * 2]
    pmovzxwd        m9, [r2 + r3 * 2]
    psubd           m2, m9

    pmovzxwd        m8, [r0 + r4]
    pmovzxwd        m9, [r2 + r5]
    psubd           m8, m9

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]

    pmovzxwd        m4, [r0]
    pmovzxwd        m9, [r2]
    psubd           m4, m9

    pmovzxwd        m5, [r0 + r1]
    pmovzxwd        m9, [r2 + r3]
    psubd           m5, m9

    pmovzxwd        m3, [r0 + r1 * 2]
    pmovzxwd        m9, [r2 + r3 * 2]
    psubd           m3, m9

    pmovzxwd        m7, [r0 + r4]
    pmovzxwd        m9, [r2 + r5]
    psubd           m7, m9

    mova            m6, m0
    paddd           m0, m1
    psubd           m1, m6
    mova            m6, m2
    paddd           m2, m8
    psubd           m8, m6
    mova            m6, m0

    punpckldq       m0, m1
    punpckhdq       m6, m1

    mova            m1, m0
    paddd           m0, m6
    psubd           m6, m1
    mova            m1, m2

    punpckldq       m2, m8
    punpckhdq       m1, m8

    mova            m8, m2
    paddd           m2, m1
    psubd           m1, m8
    mova            m8, m4
    paddd           m4, m5
    psubd           m5, m8
    mova            m8, m3
    paddd           m3, m7
    psubd           m7, m8
    mova            m8, m4

    punpckldq       m4, m5
    punpckhdq       m8, m5

    mova            m5, m4
    paddd           m4, m8
    psubd           m8, m5
    mova            m5, m3
    punpckldq       m3, m7
    punpckhdq       m5, m7

    mova            m7, m3
    paddd           m3, m5
    psubd           m5, m7
    mova            m7, m0
    paddd           m0, m2
    psubd           m2, m7
    mova            m7, m6
    paddd           m6, m1
    psubd           m1, m7
    mova            m7, m0

    punpcklqdq      m0, m2
    punpckhqdq      m7, m2

    mova            m2, m0
    paddd           m0, m7
    psubd           m7, m2
    mova            m2, m6

    punpcklqdq      m6, m1
    punpckhqdq      m2, m1

    mova            m1, m6
    paddd           m6, m2
    psubd           m2, m1
    mova            m1, m4
    paddd           m4, m3
    psubd           m3, m1
    mova            m1, m8
    paddd           m8, m5
    psubd           m5, m1
    mova            m1, m4

    punpcklqdq      m4, m3
    punpckhqdq      m1, m3

    mova            m3, m4
    paddd           m4, m1
    psubd           m1, m3
    mova            m3, m8

    punpcklqdq      m8, m5
    punpckhqdq      m3, m5

    mova            m5, m8
    paddd           m8, m3
    psubd           m3, m5
    mova            m5, m0
    paddd           m0, m4
    psubd           m4, m5
    mova            m5, m7
    paddd           m7, m1
    psubd           m1, m5
    mova            m5, m0

    vinserti128     m0, m0, xm4, 1
    vperm2i128      m5, m5, m4, 00110001b

    pxor            m4, m4
    psubd           m4, m0
    pmaxsd          m0, m4
    pxor            m4, m4
    psubd           m4, m5
    pmaxsd          m5, m4
    pmaxsd          m0, m5
    mova            m4, m7

    vinserti128     m7, m7, xm1, 1
    vperm2i128      m4, m4, m1, 00110001b

    pxor            m1, m1
    psubd           m1, m7
    pmaxsd          m7, m1
    pxor            m1, m1
    psubd           m1, m4
    pmaxsd          m4, m1
    pmaxsd          m7, m4
    mova            m1, m6
    paddd           m6, m8
    psubd           m8, m1
    mova            m1, m2
    paddd           m2, m3
    psubd           m3, m1
    mova            m1, m6

    vinserti128     m6, m6, xm8, 1
    vperm2i128      m1, m1, m8, 00110001b

    pxor            m8, m8
    psubd           m8, m6
    pmaxsd          m6, m8
    pxor            m8, m8
    psubd           m8, m1
    pmaxsd          m1, m8
    pmaxsd          m6, m1
    mova            m8, m2

    vinserti128     m2, m2, xm3, 1
    vperm2i128      m8, m8, m3, 00110001b

    pxor            m3, m3
    psubd           m3, m2
    pmaxsd          m2, m3
    pxor            m3, m3
    psubd           m3, m8
    pmaxsd          m8, m3
    pmaxsd          m2, m8
    paddd           m0, m6
    paddd           m0, m7
    paddd           m0, m2
    ret

cglobal pixel_sa8d_8x8, 4,6,10
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [r1 + r1 * 2]
    lea             r5, [r3 + r3 * 2]

    call            sa8d_8x8_12bit

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    movd            eax, xm0
    add             eax, 1
    shr             eax, 1
    RET

cglobal pixel_sa8d_8x16, 4,7,11
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [r1 + r1 * 2]
    lea             r5, [r3 + r3 * 2]
    pxor            m10, m10

    call            sa8d_8x8_12bit

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm10, xm0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm0, xm10
    movd            eax, xm0
    RET

cglobal pixel_sa8d_16x16, 4,8,11
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [r1 + r1 * 2]
    lea             r5, [r3 + r3 * 2]
    mov             r6, r0
    mov             r7, r2
    pxor            m10, m10

    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    movd            eax, xm0
    add             eax, 1
    shr             eax, 1
    RET

cglobal pixel_sa8d_16x32, 4,8,12
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [r1 + r1 * 2]
    lea             r5, [r3 + r3 * 2]
    mov             r6, r0
    mov             r7, r2
    pxor            m10, m10
    pxor            m11, m11

    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    lea             r6, [r6 + r1 * 8]
    lea             r6, [r6 + r1 * 8]
    lea             r7, [r7 + r3 * 8]
    lea             r7, [r7 + r3 * 8]
    pxor            m10, m10
    mov             r0, r6
    mov             r2, r7
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0
    movd            eax, xm11
    RET

cglobal pixel_sa8d_32x32, 4,8,12
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [r1 + r1 * 2]
    lea             r5, [r3 + r3 * 2]
    mov             r6, r0
    mov             r7, r2
    pxor            m10, m10
    pxor            m11, m11

    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    lea             r6, [r6 + r1 * 8]
    lea             r6, [r6 + r1 * 8]
    lea             r7, [r7 + r3 * 8]
    lea             r7, [r7 + r3 * 8]
    pxor            m10, m10
    mov             r0, r6
    mov             r2, r7
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0
    movd            eax, xm11
    RET

cglobal pixel_sa8d_32x64, 4,8,12
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [r1 + r1 * 2]
    lea             r5, [r3 + r3 * 2]
    mov             r6, r0
    mov             r7, r2
    pxor            m10, m10
    pxor            m11, m11

    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    lea             r6, [r6 + r1 * 8]
    lea             r6, [r6 + r1 * 8]
    lea             r7, [r7 + r3 * 8]
    lea             r7, [r7 + r3 * 8]
    pxor            m10, m10
    mov             r0, r6
    mov             r2, r7
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    lea             r6, [r6 + r1 * 8]
    lea             r6, [r6 + r1 * 8]
    lea             r7, [r7 + r3 * 8]
    lea             r7, [r7 + r3 * 8]
    pxor            m10, m10
    mov             r0, r6
    mov             r2, r7
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    lea             r6, [r6 + r1 * 8]
    lea             r6, [r6 + r1 * 8]
    lea             r7, [r7 + r3 * 8]
    lea             r7, [r7 + r3 * 8]
    pxor            m10, m10
    mov             r0, r6
    mov             r2, r7
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0
    movd            eax, xm11
    RET

cglobal pixel_sa8d_64x64, 4,8,12
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [r1 + r1 * 2]
    lea             r5, [r3 + r3 * 2]
    mov             r6, r0
    mov             r7, r2
    pxor            m10, m10
    pxor            m11, m11

    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 80]
    lea             r2, [r7 + 80]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 96]
    lea             r2, [r7 + 96]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 112]
    lea             r2, [r7 + 112]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    lea             r6, [r6 + r1 * 8]
    lea             r6, [r6 + r1 * 8]
    lea             r7, [r7 + r3 * 8]
    lea             r7, [r7 + r3 * 8]
    pxor            m10, m10
    mov             r0, r6
    mov             r2, r7
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 80]
    lea             r2, [r7 + 80]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 96]
    lea             r2, [r7 + 96]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 112]
    lea             r2, [r7 + 112]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    lea             r6, [r6 + r1 * 8]
    lea             r6, [r6 + r1 * 8]
    lea             r7, [r7 + r3 * 8]
    lea             r7, [r7 + r3 * 8]
    pxor            m10, m10
    mov             r0, r6
    mov             r2, r7
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 80]
    lea             r2, [r7 + 80]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 96]
    lea             r2, [r7 + 96]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 112]
    lea             r2, [r7 + 112]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    lea             r6, [r6 + r1 * 8]
    lea             r6, [r6 + r1 * 8]
    lea             r7, [r7 + r3 * 8]
    lea             r7, [r7 + r3 * 8]
    pxor            m10, m10
    mov             r0, r6
    mov             r2, r7
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 80]
    lea             r2, [r7 + 80]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0

    pxor            m10, m10
    lea             r0, [r6 + 96]
    lea             r2, [r7 + 96]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r6 + 112]
    lea             r2, [r7 + 112]
    call            sa8d_8x8_12bit
    paddd           m10, m0

    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    call            sa8d_8x8_12bit
    paddd           m0, m10

    vextracti128    xm6, m0, 1
    paddd           xm0, xm6

    movhlps         xm6, xm0
    paddd           xm0, xm6

    pshuflw         xm6, xm0, 0Eh
    paddd           xm0, xm6
    paddd           xm0, [pd_1]
    psrld           xm0, 1
    paddd           xm11, xm0
    movd            eax, xm11
    RET
%endif


;=============================================================================
; INTRA SATD
;=============================================================================
%define TRANS TRANS_SSE2
%define DIFFOP DIFF_UNPACK_SSE2
%define LOAD_SUMSUB_8x4P LOAD_DIFF_8x4P
%define LOAD_SUMSUB_16P  LOAD_SUMSUB_16P_SSE2
%define movdqa movaps ; doesn't hurt pre-nehalem, might as well save size
%define movdqu movups
%define punpcklqdq movlhps
INIT_XMM sse2
%if BIT_DEPTH <= 10
SA8D
%endif
SATDS_SSE2

%if HIGH_BIT_DEPTH == 0
INIT_XMM ssse3,atom
SATDS_SSE2
SA8D
%endif

%define DIFFOP DIFF_SUMSUB_SSSE3
%define LOAD_DUP_4x8P LOAD_DUP_4x8P_CONROE
%if HIGH_BIT_DEPTH == 0
%define LOAD_SUMSUB_8x4P LOAD_SUMSUB_8x4P_SSSE3
%define LOAD_SUMSUB_16P  LOAD_SUMSUB_16P_SSSE3
%endif
INIT_XMM ssse3
%if BIT_DEPTH <= 10
SA8D
%endif
SATDS_SSE2
%undef movdqa ; nehalem doesn't like movaps
%undef movdqu ; movups
%undef punpcklqdq ; or movlhps

%define TRANS TRANS_SSE4
%define LOAD_DUP_4x8P LOAD_DUP_4x8P_PENRYN
INIT_XMM sse4
%if BIT_DEPTH <= 10
SA8D
%endif
SATDS_SSE2

; Sandy/Ivy Bridge and Bulldozer do movddup in the load unit, so
; it's effectively free.
%define LOAD_DUP_4x8P LOAD_DUP_4x8P_CONROE
INIT_XMM avx
SA8D
SATDS_SSE2

%define TRANS TRANS_XOP
INIT_XMM xop
%if BIT_DEPTH <= 10
SA8D
%endif
SATDS_SSE2

%if HIGH_BIT_DEPTH == 0
%define LOAD_SUMSUB_8x4P LOAD_SUMSUB8_16x4P_AVX2
%define LOAD_DUP_4x8P LOAD_DUP_4x16P_AVX2
%define TRANS TRANS_SSE4

%macro LOAD_SUMSUB_8x8P_AVX2 7 ; 4*dst, 2*tmp, mul]
    movddup xm%1, [r0]
    movddup xm%3, [r2]
    movddup xm%2, [r0+4*r1]
    movddup xm%5, [r2+4*r3]
    vinserti128 m%1, m%1, xm%2, 1
    vinserti128 m%3, m%3, xm%5, 1

    movddup xm%2, [r0+r1]
    movddup xm%4, [r2+r3]
    movddup xm%5, [r0+r4]
    movddup xm%6, [r2+r5]
    vinserti128 m%2, m%2, xm%5, 1
    vinserti128 m%4, m%4, xm%6, 1

    DIFF_SUMSUB_SSSE3 %1, %3, %2, %4, %7
    lea      r0, [r0+2*r1]
    lea      r2, [r2+2*r3]

    movddup xm%3, [r0]
    movddup xm%5, [r0+4*r1]
    vinserti128 m%3, m%3, xm%5, 1

    movddup xm%5, [r2]
    movddup xm%4, [r2+4*r3]
    vinserti128 m%5, m%5, xm%4, 1

    movddup xm%4, [r0+r1]
    movddup xm%6, [r0+r4]
    vinserti128 m%4, m%4, xm%6, 1

    movq   xm%6, [r2+r3]
    movhps xm%6, [r2+r5]
    vpermq m%6, m%6, q1100
    DIFF_SUMSUB_SSSE3 %3, %5, %4, %6, %7
%endmacro

%macro SATD_START_AVX2 2-3 0
    FIX_STRIDES r1, r3
%if %3
    mova    %2, [hmul_8p]
    lea     r4, [5*r1]
    lea     r5, [5*r3]
%else
    mova    %2, [hmul_16p]
    lea     r4, [3*r1]
    lea     r5, [3*r3]
%endif
    pxor    %1, %1
%endmacro

%define TRANS TRANS_SSE4
INIT_YMM avx2
cglobal pixel_satd_16x8_internal
    LOAD_SUMSUB_16x4P_AVX2 0, 1, 2, 3, 4, 5, 7, r0, r2, 1
    SATD_8x4_SSE 0, 0, 1, 2, 3, 4, 5, 6
    LOAD_SUMSUB_16x4P_AVX2 0, 1, 2, 3, 4, 5, 7, r0, r2, 0
    SATD_8x4_SSE 0, 0, 1, 2, 3, 4, 5, 6
    ret

cglobal pixel_satd_16x16, 4,6,8
    SATD_START_AVX2 m6, m7
    call pixel_satd_16x8_internal
    lea  r0, [r0+4*r1]
    lea  r2, [r2+4*r3]
pixel_satd_16x8_internal:
    call pixel_satd_16x8_internal
    vextracti128 xm0, m6, 1
    paddw        xm0, xm6
    SATD_END_SSE2 xm0
    RET

cglobal pixel_satd_16x8, 4,6,8
    SATD_START_AVX2 m6, m7
    jmp pixel_satd_16x8_internal

cglobal pixel_satd_8x8_internal
    LOAD_SUMSUB_8x8P_AVX2 0, 1, 2, 3, 4, 5, 7
    SATD_8x4_SSE 0, 0, 1, 2, 3, 4, 5, 6
    ret

cglobal pixel_satd_8x16, 4,6,8
    SATD_START_AVX2 m6, m7, 1
    call pixel_satd_8x8_internal
    lea  r0, [r0+2*r1]
    lea  r2, [r2+2*r3]
    lea  r0, [r0+4*r1]
    lea  r2, [r2+4*r3]
    call pixel_satd_8x8_internal
    vextracti128 xm0, m6, 1
    paddw        xm0, xm6
    SATD_END_SSE2 xm0
    RET

cglobal pixel_satd_8x8, 4,6,8
    SATD_START_AVX2 m6, m7, 1
    call pixel_satd_8x8_internal
    vextracti128 xm0, m6, 1
    paddw        xm0, xm6
    SATD_END_SSE2 xm0
    RET

cglobal pixel_sa8d_8x8_internal
    LOAD_SUMSUB_8x8P_AVX2 0, 1, 2, 3, 4, 5, 7
    HADAMARD4_V 0, 1, 2, 3, 4
    HADAMARD 8, sumsub, 0, 1, 4, 5
    HADAMARD 8, sumsub, 2, 3, 4, 5
    HADAMARD 2, sumsub, 0, 1, 4, 5
    HADAMARD 2, sumsub, 2, 3, 4, 5
    HADAMARD 1, amax, 0, 1, 4, 5
    HADAMARD 1, amax, 2, 3, 4, 5
    paddw  m6, m0
    paddw  m6, m2
    ret

cglobal pixel_sa8d_8x8, 4,6,8
    SATD_START_AVX2 m6, m7, 1
    call pixel_sa8d_8x8_internal
    vextracti128 xm1, m6, 1
    paddw xm6, xm1
    HADDW xm6, xm1
    movd  eax, xm6
    add   eax, 1
    shr   eax, 1
    RET

cglobal pixel_sa8d_16x16, 4,6,8
    SATD_START_AVX2 m6, m7, 1

    call pixel_sa8d_8x8_internal ; pix[0]

    sub  r0, r1
    sub  r0, r1
    add  r0, 8*SIZEOF_PIXEL
    sub  r2, r3
    sub  r2, r3
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal ; pix[8]

    add  r0, r4
    add  r0, r1
    add  r2, r5
    add  r2, r3
    call pixel_sa8d_8x8_internal ; pix[8*stride+8]

    sub  r0, r1
    sub  r0, r1
    sub  r0, 8*SIZEOF_PIXEL
    sub  r2, r3
    sub  r2, r3
    sub  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal ; pix[8*stride]

    ; TODO: analyze Dynamic Range
    vextracti128 xm0, m6, 1
    paddusw xm6, xm0
    HADDUW xm6, xm0
    movd  eax, xm6
    add   eax, 1
    shr   eax, 1
    RET

cglobal pixel_sa8d_16x16_internal
    call pixel_sa8d_8x8_internal ; pix[0]

    sub  r0, r1
    sub  r0, r1
    add  r0, 8*SIZEOF_PIXEL
    sub  r2, r3
    sub  r2, r3
    add  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal ; pix[8]

    add  r0, r4
    add  r0, r1
    add  r2, r5
    add  r2, r3
    call pixel_sa8d_8x8_internal ; pix[8*stride+8]

    sub  r0, r1
    sub  r0, r1
    sub  r0, 8*SIZEOF_PIXEL
    sub  r2, r3
    sub  r2, r3
    sub  r2, 8*SIZEOF_PIXEL
    call pixel_sa8d_8x8_internal ; pix[8*stride]

    ; TODO: analyze Dynamic Range
    vextracti128 xm0, m6, 1
    paddusw xm6, xm0
    HADDUW xm6, xm0
    movd  eax, xm6
    add   eax, 1
    shr   eax, 1
    ret

%if ARCH_X86_64
cglobal pixel_sa8d_32x32, 4,8,8
    ; TODO: R6 is RAX on x64 platform, so we use it directly

    SATD_START_AVX2 m6, m7, 1
    xor     r7d, r7d

    call    pixel_sa8d_16x16_internal   ; [0]
    pxor    m6, m6
    add     r7d, eax

    add     r0, r4
    add     r0, r1
    add     r2, r5
    add     r2, r3
    call    pixel_sa8d_16x16_internal   ; [2]
    pxor    m6, m6
    add     r7d, eax

    lea     eax, [r4 * 5 - 16]
    sub     r0, rax
    sub     r0, r1
    lea     eax, [r5 * 5 - 16]
    sub     r2, rax
    sub     r2, r3
    call    pixel_sa8d_16x16_internal   ; [1]
    pxor    m6, m6
    add     r7d, eax

    add     r0, r4
    add     r0, r1
    add     r2, r5
    add     r2, r3
    call    pixel_sa8d_16x16_internal   ; [3]
    add     eax, r7d
    RET
%endif ; ARCH_X86_64=1
%endif ; HIGH_BIT_DEPTH

; Input 10bit, Output 8bit
;------------------------------------------------------------------------------------------------------------------------
;void planecopy_sc(uint16_t *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int width, int height, int shift, uint16_t mask)
;------------------------------------------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal downShift_16, 4,7,3
    mov         r4d, r4m
    mov         r5d, r5m
    movd        m0, r6m        ; m0 = shift
    add         r1, r1

    dec         r5d
.loopH:
    xor         r6, r6

.loopW:
    movu        m1, [r0 + r6 * 2]
    movu        m2, [r0 + r6 * 2 + mmsize]
    psrlw       m1, m0
    psrlw       m2, m0
    packuswb    m1, m2
    movu        [r2 + r6], m1

    add         r6, mmsize
    cmp         r6d, r4d
    jl         .loopW

    ; move to next row
    add         r0, r1
    add         r2, r3
    dec         r5d
    jnz        .loopH

    ;processing last row of every frame [To handle width which not a multiple of 16]
    ; r4d must be more than or equal to 16(mmsize)
.loop16:
    movu        m1, [r0 + (r4 - mmsize) * 2]
    movu        m2, [r0 + (r4 - mmsize) * 2 + mmsize]
    psrlw       m1, m0
    psrlw       m2, m0
    packuswb    m1, m2
    movu        [r2 + r4 - mmsize], m1

    sub         r4d, mmsize
    jz         .end
    cmp         r4d, mmsize
    jge        .loop16

    ; process partial pixels
    movu        m1, [r0]
    movu        m2, [r0 + mmsize]
    psrlw       m1, m0
    psrlw       m2, m0
    packuswb    m1, m2
    movu        [r2], m1

.end:
    RET

; Input 10bit, Output 8bit
;-------------------------------------------------------------------------------------------------------------------------------------
;void planecopy_sp(uint16_t *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int width, int height, int shift, uint16_t mask)
;-------------------------------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal downShift_16, 4,7,3
    mov         r4d, r4m
    mov         r5d, r5m
    movd        xm0, r6m        ; m0 = shift
    add         r1d, r1d

    dec         r5d
.loopH:
    xor         r6, r6

.loopW:
    movu        m1, [r0 + r6 * 2 +  0]
    movu        m2, [r0 + r6 * 2 + 32]
    vpsrlw      m1, xm0
    vpsrlw      m2, xm0
    packuswb    m1, m2
    vpermq      m1, m1, 11011000b
    movu        [r2 + r6], m1

    add         r6d, mmsize
    cmp         r6d, r4d
    jl         .loopW

    ; move to next row
    add         r0, r1
    add         r2, r3
    dec         r5d
    jnz        .loopH

    ; processing last row of every frame [To handle width which not a multiple of 32]

.loop32:
    movu        m1, [r0 + (r4 - mmsize) * 2]
    movu        m2, [r0 + (r4 - mmsize) * 2 + mmsize]
    psrlw       m1, xm0
    psrlw       m2, xm0
    packuswb    m1, m2
    vpermq      m1, m1, q3120
    movu        [r2 + r4 - mmsize], m1

    sub         r4d, mmsize
    jz         .end
    cmp         r4d, mmsize
    jge        .loop32

    ; process partial pixels
    movu        m1, [r0]
    movu        m2, [r0 + mmsize]
    psrlw       m1, xm0
    psrlw       m2, xm0
    packuswb    m1, m2
    vpermq      m1, m1, q3120
    movu        [r2], m1

.end:
    RET

; Input 8bit, Output 10bit
;---------------------------------------------------------------------------------------------------------------------
;void planecopy_cp(uint8_t *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int width, int height, int shift)
;---------------------------------------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal upShift_8, 6,7,3
    movd        xm2, r6m
    add         r3d, r3d
    dec         r5d

.loopH:
    xor         r6, r6
.loopW:
    pmovzxbw    m0,[r0 + r6]
    pmovzxbw    m1,[r0 + r6 + mmsize/2]
    psllw       m0, m2
    psllw       m1, m2
    movu        [r2 + r6 * 2], m0
    movu        [r2 + r6 * 2 + mmsize], m1

    add         r6d, mmsize
    cmp         r6d, r4d
    jl         .loopW

    ; move to next row
    add         r0, r1
    add         r2, r3
    dec         r5d
    jg         .loopH

    ; processing last row of every frame [To handle width which not a multiple of 16]
    mov         r1d, (mmsize/2 - 1)
    and         r1d, r4d
    sub         r1, mmsize/2

    ; NOTE: Width MUST BE more than or equal to 8
    shr         r4d, 3          ; log2(mmsize)
.loopW8:
    pmovzxbw    m0,[r0]
    psllw       m0, m2
    movu        [r2], m0
    add         r0, mmsize/2
    add         r2, mmsize
    dec         r4d
    jg         .loopW8

    ; Mac OS X can't read beyond array bound, so rollback some bytes
    pmovzxbw    m0,[r0 + r1]
    psllw       m0, m2
    movu        [r2 + r1 * 2], m0
    RET


;---------------------------------------------------------------------------------------------------------------------
;void planecopy_cp(uint8_t *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int width, int height, int shift)
;---------------------------------------------------------------------------------------------------------------------
%if ARCH_X86_64
INIT_YMM avx2
cglobal upShift_8, 6,7,3
    movd        xm2, r6m
    add         r3d, r3d
    dec         r5d

.loopH:
    xor         r6, r6
.loopW:
    pmovzxbw    m0,[r0 + r6]
    pmovzxbw    m1,[r0 + r6 + mmsize/2]
    psllw       m0, xm2
    psllw       m1, xm2
    movu        [r2 + r6 * 2], m0
    movu        [r2 + r6 * 2 + mmsize], m1

    add         r6d, mmsize
    cmp         r6d, r4d
    jl         .loopW

    ; move to next row
    add         r0, r1
    add         r2, r3
    dec         r5d
    jg         .loopH

    ; processing last row of every frame [To handle width which not a multiple of 32]
    mov         r1d, (mmsize/2 - 1)
    and         r1d, r4d
    sub         r1, mmsize/2

    ; NOTE: Width MUST BE more than or equal to 16
    shr         r4d, 4          ; log2(mmsize)
.loopW16:
    pmovzxbw    m0,[r0]
    psllw       m0, xm2
    movu        [r2], m0
    add         r0, mmsize/2
    add         r2, mmsize
    dec         r4d
    jg         .loopW16

    ; Mac OS X can't read beyond array bound, so rollback some bytes
    pmovzxbw    m0,[r0 + r1]
    psllw       m0, xm2
    movu        [r2 + r1 * 2], m0
    RET
%endif

%macro ABSD2 6 ; dst1, dst2, src1, src2, tmp, tmp
%if cpuflag(ssse3)
    pabsd   %1, %3
    pabsd   %2, %4
%elifidn %1, %3
    pxor    %5, %5
    pxor    %6, %6
    psubd   %5, %1
    psubd   %6, %2
    pmaxsd  %1, %5
    pmaxsd  %2, %6
%else
    pxor    %1, %1
    pxor    %2, %2
    psubd   %1, %3
    psubd   %2, %4
    pmaxsd  %1, %3
    pmaxsd  %2, %4
%endif
%endmacro


; Input 10bit, Output 12bit
;------------------------------------------------------------------------------------------------------------------------
;void planecopy_sp_shl(uint16_t *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int width, int height, int shift, uint16_t mask)
;------------------------------------------------------------------------------------------------------------------------
INIT_XMM sse2
cglobal upShift_16, 4,7,4
    mov         r4d, r4m
    mov         r5d, r5m
    movd        m0, r6m        ; m0 = shift
    mova        m3, [pw_pixel_max]
    FIX_STRIDES r1d, r3d
    dec         r5d
.loopH:
    xor         r6d, r6d
.loopW:
    movu        m1, [r0 + r6 * SIZEOF_PIXEL]
    movu        m2, [r0 + r6 * SIZEOF_PIXEL + mmsize]
    psllw       m1, m0
    psllw       m2, m0
    ; TODO: if input always valid, we can remove below 2 instructions.
    pand        m1, m3
    pand        m2, m3
    movu        [r2 + r6 * SIZEOF_PIXEL], m1
    movu        [r2 + r6 * SIZEOF_PIXEL + mmsize], m2

    add         r6, mmsize * 2 / SIZEOF_PIXEL
    cmp         r6d, r4d
    jl         .loopW

    ; move to next row
    add         r0, r1
    add         r2, r3
    dec         r5d
    jnz        .loopH

    ;processing last row of every frame [To handle width which not a multiple of 16]

    ; WARNING: width(r4d) MUST BE more than or equal to 16(mmsize) in here
.loop16:
    movu        m1, [r0 + (r4 - mmsize) * 2]
    movu        m2, [r0 + (r4 - mmsize) * 2 + mmsize]
    psllw       m1, m0
    psllw       m2, m0
    pand        m1, m3
    pand        m2, m3
    movu        [r2 + (r4 - mmsize) * 2], m1
    movu        [r2 + (r4 - mmsize) * 2 + mmsize], m2

    sub         r4d, mmsize
    jz         .end
    cmp         r4d, mmsize
    jge        .loop16

    ; process partial pixels
    movu        m1, [r0]
    movu        m2, [r0 + mmsize]
    psllw       m1, m0
    psllw       m2, m0
    pand        m1, m3
    pand        m2, m3
    movu        [r2], m1
    movu        [r2 + mmsize], m2

.end:
    RET

; Input 10bit, Output 12bit
;-------------------------------------------------------------------------------------------------------------------------------------
;void planecopy_sp_shl(uint16_t *src, intptr_t srcStride, pixel *dst, intptr_t dstStride, int width, int height, int shift, uint16_t mask)
;-------------------------------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal upShift_16, 4,7,4
    mov         r4d, r4m
    mov         r5d, r5m
    movd        xm0, r6m        ; m0 = shift
    vbroadcasti128 m3, [pw_pixel_max]
    FIX_STRIDES r1d, r3d
    dec         r5d
.loopH:
    xor         r6d, r6d
.loopW:
    movu        m1, [r0 + r6 * SIZEOF_PIXEL]
    movu        m2, [r0 + r6 * SIZEOF_PIXEL + mmsize]
    psllw       m1, xm0
    psllw       m2, xm0
    pand        m1, m3
    pand        m2, m3
    movu        [r2 + r6 * SIZEOF_PIXEL], m1
    movu        [r2 + r6 * SIZEOF_PIXEL + mmsize], m2

    add         r6, mmsize * 2 / SIZEOF_PIXEL
    cmp         r6d, r4d
    jl         .loopW

    ; move to next row
    add         r0, r1
    add         r2, r3
    dec         r5d
    jnz        .loopH

    ; processing last row of every frame [To handle width which not a multiple of 32]

.loop32:
    movu        m1, [r0 + (r4 - mmsize) * 2]
    movu        m2, [r0 + (r4 - mmsize) * 2 + mmsize]
    psllw       m1, xm0
    psllw       m2, xm0
    pand        m1, m3
    pand        m2, m3
    movu        [r2 + (r4 - mmsize) * 2], m1
    movu        [r2 + (r4 - mmsize) * 2 + mmsize], m2

    sub         r4d, mmsize
    jz         .end
    cmp         r4d, mmsize
    jge        .loop32

    ; process partial pixels
    movu        m1, [r0]
    movu        m2, [r0 + mmsize]
    psllw       m1, xm0
    psllw       m2, xm0
    pand        m1, m3
    pand        m2, m3
    movu        [r2], m1
    movu        [r2 + mmsize], m2

.end:
    RET


;---------------------------------------------------------------------------------------------------------------------
;int psyCost_pp(const pixel* source, intptr_t sstride, const pixel* recon, intptr_t rstride)
;---------------------------------------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal psyCost_pp_4x4, 4, 5, 8

%if HIGH_BIT_DEPTH
    FIX_STRIDES r1, r3
    lea             r4, [3 * r1]
    movddup         m0, [r0]
    movddup         m1, [r0 + r1]
    movddup         m2, [r0 + r1 * 2]
    movddup         m3, [r0 + r4]
    mova            m4, [hmul_8w]
    pmaddwd         m0, m4
    pmaddwd         m1, m4
    pmaddwd         m2, m4
    pmaddwd         m3, m4

    paddd           m5, m0, m1
    paddd           m5, m2
    paddd           m5, m3
    psrldq          m4, m5, 4
    paddd           m5, m4
    psrld           m5, 2

    SUMSUB_BA d, 0, 1, 4
    SUMSUB_BA d, 2, 3, 4
    SUMSUB_BA d, 0, 2, 4
    SUMSUB_BA d, 1, 3, 4
    %define ORDER unord
    TRANS q, ORDER, 0, 2, 4, 6
    TRANS q, ORDER, 1, 3, 4, 6
    ABSD2 m0, m2, m0, m2, m4, m6
    pmaxsd          m0, m2
    ABSD2 m1, m3, m1, m3, m4, m6
    pmaxsd          m1, m3
    paddd           m0, m1
    movhlps         m1, m0
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1

    psubd           m7, m0, m5

    lea             r4, [3 * r3]
    movddup         m0, [r2]
    movddup         m1, [r2 + r3]
    movddup         m2, [r2 + r3 * 2]
    movddup         m3, [r2 + r4]
    mova            m4, [hmul_8w]
    pmaddwd         m0, m4
    pmaddwd         m1, m4
    pmaddwd         m2, m4
    pmaddwd         m3, m4

    paddd           m5, m0, m1
    paddd           m5, m2
    paddd           m5, m3
    psrldq          m4, m5, 4
    paddd           m5, m4
    psrld           m5, 2

    SUMSUB_BA d, 0, 1, 4
    SUMSUB_BA d, 2, 3, 4
    SUMSUB_BA d, 0, 2, 4
    SUMSUB_BA d, 1, 3, 4
    %define ORDER unord
    TRANS q, ORDER, 0, 2, 4, 6
    TRANS q, ORDER, 1, 3, 4, 6
    ABSD2 m0, m2, m0, m2, m4, m6
    pmaxsd          m0, m2
    ABSD2 m1, m3, m1, m3, m4, m6
    pmaxsd          m1, m3
    paddd           m0, m1
    movhlps         m1, m0
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1

    psubd           m0, m5

    psubd           m7, m0
    pabsd           m0, m7
    movd            eax, m0

%else ; !HIGH_BIT_DEPTH
    lea             r4, [3 * r1]
    movd            m0, [r0]
    movd            m1, [r0 + r1]
    movd            m2, [r0 + r1 * 2]
    movd            m3, [r0 + r4]
    shufps          m0, m1, 0
    shufps          m2, m3, 0
    mova            m4, [hmul_4p]
    pmaddubsw       m0, m4
    pmaddubsw       m2, m4

    paddw           m5, m0, m2
    movhlps         m4, m5
    paddw           m5, m4
    pmaddwd         m5, [pw_1]
    psrld           m5, 2

    HADAMARD 0, sumsub, 0, 2, 1, 3
    HADAMARD 4, sumsub, 0, 2, 1, 3
    HADAMARD 1, amax, 0, 2, 1, 3
    HADDW m0, m2

    psubd           m6, m0, m5

    lea             r4, [3 * r3]
    movd            m0, [r2]
    movd            m1, [r2 + r3]
    movd            m2, [r2 + r3 * 2]
    movd            m3, [r2 + r4]
    shufps          m0, m1, 0
    shufps          m2, m3, 0
    mova            m4, [hmul_4p]
    pmaddubsw       m0, m4
    pmaddubsw       m2, m4

    paddw           m5, m0, m2
    movhlps         m4, m5
    paddw           m5, m4
    pmaddwd         m5, [pw_1]
    psrld           m5, 2

    HADAMARD 0, sumsub, 0, 2, 1, 3
    HADAMARD 4, sumsub, 0, 2, 1, 3
    HADAMARD 1, amax, 0, 2, 1, 3
    HADDW m0, m2

    psubd           m0, m5

    psubd           m6, m0
    pabsd           m0, m6
    movd            eax, m0
%endif ; HIGH_BIT_DEPTH
    RET

%if ARCH_X86_64
INIT_XMM sse4
cglobal psyCost_pp_8x8, 4, 6, 13

%if HIGH_BIT_DEPTH
    FIX_STRIDES r1, r3
    lea             r4, [3 * r1]
    pxor            m10, m10
    movu            m0, [r0]
    movu            m1, [r0 + r1]
    movu            m2, [r0 + r1 * 2]
    movu            m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r1]
    movu            m6, [r5 + r1 * 2]
    movu            m7, [r5 + r4]

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, [pw_1]
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    HADAMARD8_2D 0, 1, 2, 3, 4, 5, 6, 7, 9, amax

    paddd           m0, m1
    paddd           m0, m2
    paddd           m0, m3
    HADDUW m0, m1
    paddd           m0, [pd_1]
    psrld           m0, 1
    psubd           m10, m0, m8

    lea             r4, [3 * r3]
    movu            m0, [r2]
    movu            m1, [r2 + r3]
    movu            m2, [r2 + r3 * 2]
    movu            m3, [r2 + r4]
    lea             r5, [r2 + r3 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r3]
    movu            m6, [r5 + r3 * 2]
    movu            m7, [r5 + r4]

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, [pw_1]
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    HADAMARD8_2D 0, 1, 2, 3, 4, 5, 6, 7, 9, amax

    paddd           m0, m1
    paddd           m0, m2
    paddd           m0, m3
    HADDUW m0, m1
    paddd           m0, [pd_1]
    psrld           m0, 1
    psubd           m0, m8
    psubd           m10, m0
    pabsd           m0, m10
    movd            eax, m0
%else ; !HIGH_BIT_DEPTH
    lea             r4, [3 * r1]
    mova            m8, [hmul_8p]

    movddup         m0, [r0]
    movddup         m1, [r0 + r1]
    movddup         m2, [r0 + r1 * 2]
    movddup         m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movddup         m4, [r5]
    movddup         m5, [r5 + r1]
    movddup         m6, [r5 + r1 * 2]
    movddup         m7, [r5 + r4]

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, [pw_1]
    psrldq          m10, m11, 4
    paddd           m11, m10
    psrld           m11, 2

    HADAMARD8_2D_HMUL 0, 1, 2, 3, 4, 5, 6, 7, 9, 10

    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    HADDW m0, m1

    paddd           m0, [pd_1]
    psrld           m0, 1
    psubd           m12, m0, m11

    lea             r4, [3 * r3]

    movddup         m0, [r2]
    movddup         m1, [r2 + r3]
    movddup         m2, [r2 + r3 * 2]
    movddup         m3, [r2 + r4]
    lea             r5, [r2 + r3 * 4]
    movddup         m4, [r5]
    movddup         m5, [r5 + r3]
    movddup         m6, [r5 + r3 * 2]
    movddup         m7, [r5 + r4]

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, [pw_1]
    psrldq          m10, m11, 4
    paddd           m11, m10
    psrld           m11, 2

    HADAMARD8_2D_HMUL 0, 1, 2, 3, 4, 5, 6, 7, 9, 10

    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    HADDW m0, m1

    paddd           m0, [pd_1]
    psrld           m0, 1
    psubd           m0, m11
    psubd           m12, m0
    pabsd           m0, m12
    movd            eax, m0
%endif ; HIGH_BIT_DEPTH
    RET
%endif

%if ARCH_X86_64
%if HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal psyCost_pp_16x16, 4, 9, 14

    FIX_STRIDES r1, r3
    lea             r4, [3 * r1]
    lea             r8, [3 * r3]
    mova            m12, [pw_1]
    mova            m13, [pd_1]
    pxor            m11, m11
    mov             r7d, 2
.loopH:
    mov             r6d, 2
.loopW:
    pxor            m10, m10
    movu            m0, [r0]
    movu            m1, [r0 + r1]
    movu            m2, [r0 + r1 * 2]
    movu            m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r1]
    movu            m6, [r5 + r1 * 2]
    movu            m7, [r5 + r4]

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, m12
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    HADAMARD8_2D 0, 1, 2, 3, 4, 5, 6, 7, 9, amax

    paddd           m0, m1
    paddd           m0, m2
    paddd           m0, m3
    HADDUW m0, m1
    paddd           m0, m13
    psrld           m0, 1
    psubd           m10, m0, m8

    movu            m0, [r2]
    movu            m1, [r2 + r3]
    movu            m2, [r2 + r3 * 2]
    movu            m3, [r2 + r8]
    lea             r5, [r2 + r3 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r3]
    movu            m6, [r5 + r3 * 2]
    movu            m7, [r5 + r8]

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, m12
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    HADAMARD8_2D 0, 1, 2, 3, 4, 5, 6, 7, 9, amax

    paddd           m0, m1
    paddd           m0, m2
    paddd           m0, m3
    HADDUW m0, m1
    paddd           m0, m13
    psrld           m0, 1
    psubd           m0, m8
    psubd           m10, m0
    pabsd           m0, m10
    paddd           m11, m0
    add             r0, 16
    add             r2, 16
    dec             r6d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 32]
    lea             r2, [r2 + r3 * 8 - 32]
    dec             r7d
    jnz             .loopH
    movd            eax, m11
    RET
%else ; !HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal psyCost_pp_16x16, 4, 9, 15
    lea             r4, [3 * r1]
    lea             r8, [3 * r3]
    mova            m8, [hmul_8p]
    mova            m10, [pw_1]
    mova            m14, [pd_1]
    pxor            m13, m13
    mov             r7d, 2
.loopH:
    mov             r6d, 2
.loopW:
    pxor            m12, m12
    movddup         m0, [r0]
    movddup         m1, [r0 + r1]
    movddup         m2, [r0 + r1 * 2]
    movddup         m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movddup         m4, [r5]
    movddup         m5, [r5 + r1]
    movddup         m6, [r5 + r1 * 2]
    movddup         m7, [r5 + r4]

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, m10
    psrldq          m9, m11, 4
    paddd           m11, m9
    psrld           m11, 2

    HADAMARD8_2D_HMUL 0, 1, 2, 3, 4, 5, 6, 7, 9, 9

    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    HADDW m0, m1

    paddd           m0, m14
    psrld           m0, 1
    psubd           m12, m0, m11

    movddup         m0, [r2]
    movddup         m1, [r2 + r3]
    movddup         m2, [r2 + r3 * 2]
    movddup         m3, [r2 + r8]
    lea             r5, [r2 + r3 * 4]
    movddup         m4, [r5]
    movddup         m5, [r5 + r3]
    movddup         m6, [r5 + r3 * 2]
    movddup         m7, [r5 + r8]

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, m10
    psrldq          m9, m11, 4
    paddd           m11, m9
    psrld           m11, 2

    HADAMARD8_2D_HMUL 0, 1, 2, 3, 4, 5, 6, 7, 9, 9

    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    HADDW m0, m1

    paddd           m0, m14
    psrld           m0, 1
    psubd           m0, m11
    psubd           m12, m0
    pabsd           m0, m12
    paddd           m13, m0
    add             r0, 8
    add             r2, 8
    dec             r6d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 16]
    lea             r2, [r2 + r3 * 8 - 16]
    dec             r7d
    jnz             .loopH
    movd            eax, m13
    RET
%endif ; HIGH_BIT_DEPTH
%endif

%if ARCH_X86_64
%if HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal psyCost_pp_32x32, 4, 9, 14

    FIX_STRIDES r1, r3
    lea             r4, [3 * r1]
    lea             r8, [3 * r3]
    mova            m12, [pw_1]
    mova            m13, [pd_1]
    pxor            m11, m11
    mov             r7d, 4
.loopH:
    mov             r6d, 4
.loopW:
    pxor            m10, m10
    movu            m0, [r0]
    movu            m1, [r0 + r1]
    movu            m2, [r0 + r1 * 2]
    movu            m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r1]
    movu            m6, [r5 + r1 * 2]
    movu            m7, [r5 + r4]

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, m12
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    HADAMARD8_2D 0, 1, 2, 3, 4, 5, 6, 7, 9, amax

    paddd           m0, m1
    paddd           m0, m2
    paddd           m0, m3
    HADDUW m0, m1
    paddd           m0, m13
    psrld           m0, 1
    psubd           m10, m0, m8

    movu            m0, [r2]
    movu            m1, [r2 + r3]
    movu            m2, [r2 + r3 * 2]
    movu            m3, [r2 + r8]
    lea             r5, [r2 + r3 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r3]
    movu            m6, [r5 + r3 * 2]
    movu            m7, [r5 + r8]

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, m12
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    HADAMARD8_2D 0, 1, 2, 3, 4, 5, 6, 7, 9, amax

    paddd           m0, m1
    paddd           m0, m2
    paddd           m0, m3
    HADDUW m0, m1
    paddd           m0, m13
    psrld           m0, 1
    psubd           m0, m8
    psubd           m10, m0
    pabsd           m0, m10
    paddd           m11, m0
    add             r0, 16
    add             r2, 16
    dec             r6d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 64]
    lea             r2, [r2 + r3 * 8 - 64]
    dec             r7d
    jnz             .loopH
    movd            eax, m11
    RET

%else ; !HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal psyCost_pp_32x32, 4, 9, 15

    lea             r4, [3 * r1]
    lea             r8, [3 * r3]
    mova            m8, [hmul_8p]
    mova            m10, [pw_1]
    mova            m14, [pd_1]
    pxor            m13, m13
    mov             r7d, 4
.loopH:
    mov             r6d, 4
.loopW:
    pxor            m12, m12
    movddup         m0, [r0]
    movddup         m1, [r0 + r1]
    movddup         m2, [r0 + r1 * 2]
    movddup         m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movddup         m4, [r5]
    movddup         m5, [r5 + r1]
    movddup         m6, [r5 + r1 * 2]
    movddup         m7, [r5 + r4]

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, m10
    psrldq          m9, m11, 4
    paddd           m11, m9
    psrld           m11, 2

    HADAMARD8_2D_HMUL 0, 1, 2, 3, 4, 5, 6, 7, 9, 9

    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    HADDW m0, m1

    paddd           m0, m14
    psrld           m0, 1
    psubd           m12, m0, m11

    movddup         m0, [r2]
    movddup         m1, [r2 + r3]
    movddup         m2, [r2 + r3 * 2]
    movddup         m3, [r2 + r8]
    lea             r5, [r2 + r3 * 4]
    movddup         m4, [r5]
    movddup         m5, [r5 + r3]
    movddup         m6, [r5 + r3 * 2]
    movddup         m7, [r5 + r8]

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, m10
    psrldq          m9, m11, 4
    paddd           m11, m9
    psrld           m11, 2

    HADAMARD8_2D_HMUL 0, 1, 2, 3, 4, 5, 6, 7, 9, 9

    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    HADDW m0, m1

    paddd           m0, m14
    psrld           m0, 1
    psubd           m0, m11
    psubd           m12, m0
    pabsd           m0, m12
    paddd           m13, m0
    add             r0, 8
    add             r2, 8
    dec             r6d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 32]
    lea             r2, [r2 + r3 * 8 - 32]
    dec             r7d
    jnz             .loopH
    movd            eax, m13
    RET
%endif ; HIGH_BIT_DEPTH
%endif

%if ARCH_X86_64
%if HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal psyCost_pp_64x64, 4, 9, 14

    FIX_STRIDES r1, r3
    lea             r4, [3 * r1]
    lea             r8, [3 * r3]
    mova            m12, [pw_1]
    mova            m13, [pd_1]
    pxor            m11, m11
    mov             r7d, 8
.loopH:
    mov             r6d, 8
.loopW:
    pxor            m10, m10
    movu            m0, [r0]
    movu            m1, [r0 + r1]
    movu            m2, [r0 + r1 * 2]
    movu            m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r1]
    movu            m6, [r5 + r1 * 2]
    movu            m7, [r5 + r4]

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, m12
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    HADAMARD8_2D 0, 1, 2, 3, 4, 5, 6, 7, 9, amax

    paddd           m0, m1
    paddd           m0, m2
    paddd           m0, m3
    HADDUW m0, m1
    paddd           m0, m13
    psrld           m0, 1
    psubd           m10, m0, m8

    movu            m0, [r2]
    movu            m1, [r2 + r3]
    movu            m2, [r2 + r3 * 2]
    movu            m3, [r2 + r8]
    lea             r5, [r2 + r3 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r3]
    movu            m6, [r5 + r3 * 2]
    movu            m7, [r5 + r8]

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, m12
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    HADAMARD8_2D 0, 1, 2, 3, 4, 5, 6, 7, 9, amax

    paddd           m0, m1
    paddd           m0, m2
    paddd           m0, m3
    HADDUW m0, m1
    paddd           m0, m13
    psrld           m0, 1
    psubd           m0, m8
    psubd           m10, m0
    pabsd           m0, m10
    paddd           m11, m0
    add             r0, 16
    add             r2, 16
    dec             r6d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 128]
    lea             r2, [r2 + r3 * 8 - 128]
    dec             r7d
    jnz             .loopH
    movd            eax, m11
    RET

%else ; !HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal psyCost_pp_64x64, 4, 9, 15

    lea             r4, [3 * r1]
    lea             r8, [3 * r3]
    mova            m8, [hmul_8p]
    mova            m10, [pw_1]
    mova            m14, [pd_1]
    pxor            m13, m13
    mov             r7d, 8
.loopH:
    mov             r6d, 8
.loopW:
    pxor            m12, m12
    movddup         m0, [r0]
    movddup         m1, [r0 + r1]
    movddup         m2, [r0 + r1 * 2]
    movddup         m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movddup         m4, [r5]
    movddup         m5, [r5 + r1]
    movddup         m6, [r5 + r1 * 2]
    movddup         m7, [r5 + r4]

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, m10
    psrldq          m9, m11, 4
    paddd           m11, m9
    psrld           m11, 2

    HADAMARD8_2D_HMUL 0, 1, 2, 3, 4, 5, 6, 7, 9, 9

    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    HADDW m0, m1

    paddd           m0, m14
    psrld           m0, 1
    psubd           m12, m0, m11

    movddup         m0, [r2]
    movddup         m1, [r2 + r3]
    movddup         m2, [r2 + r3 * 2]
    movddup         m3, [r2 + r8]
    lea             r5, [r2 + r3 * 4]
    movddup         m4, [r5]
    movddup         m5, [r5 + r3]
    movddup         m6, [r5 + r3 * 2]
    movddup         m7, [r5 + r8]

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, m10
    psrldq          m9, m11, 4
    paddd           m11, m9
    psrld           m11, 2

    HADAMARD8_2D_HMUL 0, 1, 2, 3, 4, 5, 6, 7, 9, 9

    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    HADDW m0, m1

    paddd           m0, m14
    psrld           m0, 1
    psubd           m0, m11
    psubd           m12, m0
    pabsd           m0, m12
    paddd           m13, m0
    add             r0, 8
    add             r2, 8
    dec             r6d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 64]
    lea             r2, [r2 + r3 * 8 - 64]
    dec             r7d
    jnz             .loopH
    movd            eax, m13
    RET
%endif ; HIGH_BIT_DEPTH
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal psyCost_pp_4x4, 4, 5, 6
    add             r1d, r1d
    add             r3d, r3d
    lea              r4, [r1 * 3]
    movddup         xm0, [r0]
    movddup         xm1, [r0 + r1]
    movddup         xm2, [r0 + r1 * 2]
    movddup         xm3, [r0 + r4]

    lea              r4, [r3 * 3]
    movddup         xm4, [r2]
    movddup         xm5, [r2 + r3]
    vinserti128      m0, m0, xm4, 1
    vinserti128      m1, m1, xm5, 1
    movddup         xm4, [r2 + r3 * 2]
    movddup         xm5, [r2 + r4]
    vinserti128      m2, m2, xm4, 1
    vinserti128      m3, m3, xm5, 1

    mova             m4, [hmul_8w]
    pmaddwd          m0, m4
    pmaddwd          m1, m4
    pmaddwd          m2, m4
    pmaddwd          m3, m4
    paddd            m5, m0, m1
    paddd            m4, m2, m3
    paddd            m5, m4
    psrldq           m4, m5, 4
    paddd            m5, m4
    psrld            m5, 2

    mova             m4, m0
    paddd            m0, m1
    psubd            m1, m4
    mova             m4, m2
    paddd            m2, m3
    psubd            m3, m4
    mova             m4, m0
    paddd            m0, m2
    psubd            m2, m4
    mova             m4, m1
    paddd            m1, m3
    psubd            m3, m4
    movaps           m4, m0
    vshufps          m4, m4, m2, 11011101b
    vshufps          m0, m0, m2, 10001000b
    movaps           m2, m1
    vshufps          m2, m2, m3, 11011101b
    vshufps          m1, m1, m3, 10001000b
    pabsd            m0, m0
    pabsd            m4, m4
    pmaxsd           m0, m4
    pabsd            m1, m1
    pabsd            m2, m2
    pmaxsd           m1, m2
    paddd            m0, m1

    vpermq           m1, m0, 11110101b
    paddd            m0, m1
    psrldq           m1, m0, 4
    paddd            m0, m1
    psubd            m0, m5

    vextracti128    xm1, m0, 1
    psubd           xm1, xm0
    pabsd           xm1, xm1
    movd            eax, xm1
    RET
%else ; !HIGH_BIT_DEPTH
cglobal psyCost_pp_4x4, 4, 5, 6
    lea             r4, [3 * r1]
    movd            xm0, [r0]
    movd            xm1, [r0 + r1]
    movd            xm2, [r0 + r1 * 2]
    movd            xm3, [r0 + r4]
    vshufps         xm0, xm1, 0
    vshufps         xm2, xm3, 0

    lea             r4, [3 * r3]
    movd            xm1, [r2]
    movd            xm3, [r2 + r3]
    movd            xm4, [r2 + r3 * 2]
    movd            xm5, [r2 + r4]
    vshufps         xm1, xm3, 0
    vshufps         xm4, xm5, 0

    vinserti128     m0, m0, xm1, 1
    vinserti128     m2, m2, xm4, 1

    mova            m4, [hmul_4p]
    pmaddubsw       m0, m4
    pmaddubsw       m2, m4

    paddw           m5, m0, m2
    mova            m1, m5
    psrldq          m4, m5, 8
    paddw           m5, m4
    pmaddwd         m5, [pw_1]
    psrld           m5, 2

    vpsubw          m2, m2, m0
    vpunpckhqdq     m0, m1, m2
    vpunpcklqdq     m1, m1, m2
    vpaddw          m2, m1, m0
    vpsubw          m0, m0, m1
    vpblendw        m1, m2, m0, 10101010b
    vpslld          m0, m0, 10h
    vpsrld          m2, m2, 10h
    vpor            m0, m0, m2
    vpabsw          m1, m1
    vpabsw          m0, m0
    vpmaxsw         m1, m1, m0
    vpmaddwd        m1, m1, [pw_1]
    psrldq          m2, m1, 8
    paddd           m1, m2
    psrldq          m3, m1, 4
    paddd           m1, m3
    psubd           m1, m5
    vextracti128    xm2, m1, 1
    psubd           m1, m2
    pabsd           m1, m1
    movd            eax, xm1
    RET
%endif

%macro PSY_PP_8x8 0
    movddup         m0, [r0 + r1 * 0]
    movddup         m1, [r0 + r1 * 1]
    movddup         m2, [r0 + r1 * 2]
    movddup         m3, [r0 + r4 * 1]

    lea             r5, [r0 + r1 * 4]

    movddup         m4, [r2 + r3 * 0]
    movddup         m5, [r2 + r3 * 1]
    movddup         m6, [r2 + r3 * 2]
    movddup         m7, [r2 + r7 * 1]

    lea             r6, [r2 + r3 * 4]

    vinserti128     m0, m0, xm4, 1
    vinserti128     m1, m1, xm5, 1
    vinserti128     m2, m2, xm6, 1
    vinserti128     m3, m3, xm7, 1

    movddup         m4, [r5 + r1 * 0]
    movddup         m5, [r5 + r1 * 1]
    movddup         m6, [r5 + r1 * 2]
    movddup         m7, [r5 + r4 * 1]

    movddup         m9, [r6 + r3 * 0]
    movddup         m10, [r6 + r3 * 1]
    movddup         m11, [r6 + r3 * 2]
    movddup         m12, [r6 + r7 * 1]

    vinserti128     m4, m4, xm9, 1
    vinserti128     m5, m5, xm10, 1
    vinserti128     m6, m6, xm11, 1
    vinserti128     m7, m7, xm12, 1

    pmaddubsw       m0, m8
    pmaddubsw       m1, m8
    pmaddubsw       m2, m8
    pmaddubsw       m3, m8
    pmaddubsw       m4, m8
    pmaddubsw       m5, m8
    pmaddubsw       m6, m8
    pmaddubsw       m7, m8

    paddw           m11, m0, m1
    paddw           m11, m2
    paddw           m11, m3
    paddw           m11, m4
    paddw           m11, m5
    paddw           m11, m6
    paddw           m11, m7

    pmaddwd         m11, [pw_1]
    psrldq          m10, m11, 4
    paddd           m11, m10
    psrld           m11, 2

    mova            m9, m0
    paddw           m0, m1      ; m0+m1
    psubw           m1, m9      ; m1-m0
    mova            m9, m2
    paddw           m2, m3      ; m2+m3
    psubw           m3, m9      ; m3-m2
    mova            m9, m0
    paddw           m0, m2      ; m0+m1+m2+m3
    psubw           m2, m9      ; m2+m3-m0+m1
    mova            m9, m1
    paddw           m1, m3      ; m1-m0+m3-m2
    psubw           m3, m9      ; m3-m2-m1-m0

    movdqa          m9, m4
    paddw           m4, m5      ; m4+m5
    psubw           m5, m9      ; m5-m4
    movdqa          m9, m6
    paddw           m6, m7      ; m6+m7
    psubw           m7, m9      ; m7-m6
    movdqa          m9, m4
    paddw           m4, m6      ; m4+m5+m6+m7
    psubw           m6, m9      ; m6+m7-m4+m5
    movdqa          m9, m5
    paddw           m5, m7      ; m5-m4+m7-m6
    psubw           m7, m9      ; m7-m6-m5-m4

    movdqa          m9, m0
    paddw           m0, m4      ; (m0+m1+m2+m3)+(m4+m5+m6+m7)
    psubw           m4, m9      ; (m4+m5+m6+m7)-(m0+m1+m2+m3)
    movdqa          m9, m1
    paddw           m1, m5      ; (m1-m0+m3-m2)+(m5-m4+m7-m6)
    psubw           m5, m9      ; (m5-m4+m7-m6)-(m1-m0+m3-m2)

    mova            m9, m0
    vshufps         m9, m9, m4, 11011101b
    vshufps         m0, m0, m4, 10001000b

    movdqa          m4, m0
    paddw           m0, m9      ; (a0 + a4) + (a4 - a0)
    psubw           m9, m4      ; (a0 + a4) - (a4 - a0) == (a0 + a4) + (a0 - a4)

    movaps          m4, m1
    vshufps         m4, m4, m5, 11011101b
    vshufps         m1, m1, m5, 10001000b

    movdqa          m5, m1
    paddw           m1, m4
    psubw           m4, m5
    movdqa          m5, m2
    paddw           m2, m6
    psubw           m6, m5
    movdqa          m5, m3
    paddw           m3, m7
    psubw           m7, m5

    movaps          m5, m2
    vshufps         m5, m5, m6, 11011101b
    vshufps         m2, m2, m6, 10001000b

    movdqa          m6, m2
    paddw           m2, m5
    psubw           m5, m6
    movaps          m6, m3

    vshufps         m6, m6, m7, 11011101b
    vshufps         m3, m3, m7, 10001000b

    movdqa          m7, m3
    paddw           m3, m6
    psubw           m6, m7
    movdqa          m7, m0

    pblendw         m0, m9, 10101010b
    pslld           m9, 10h
    psrld           m7, 10h
    por             m9, m7
    pabsw           m0, m0
    pabsw           m9, m9
    pmaxsw          m0, m9
    movdqa          m7, m1
    pblendw         m1, m4, 10101010b
    pslld           m4, 10h
    psrld           m7, 10h
    por             m4, m7
    pabsw           m1, m1
    pabsw           m4, m4
    pmaxsw          m1, m4
    movdqa          m7, m2
    pblendw         m2, m5, 10101010b
    pslld           m5, 10h
    psrld           m7, 10h
    por             m5, m7
    pabsw           m2, m2
    pabsw           m5, m5
    pmaxsw          m2, m5
    mova            m7, m3

    pblendw         m3, m6, 10101010b
    pslld           m6, 10h
    psrld           m7, 10h
    por             m6, m7
    pabsw           m3, m3
    pabsw           m6, m6
    pmaxsw          m3, m6
    paddw           m0, m1
    paddw           m0, m2
    paddw           m0, m3
    pmaddwd         m0, [pw_1]
    psrldq          m1, m0, 8
    paddd           m0, m1

    pshuflw         m1, m0, 00001110b
    paddd           m0, m1
    paddd           m0, [pd_1]
    psrld           m0, 1

    psubd           m0, m11

    vextracti128    xm1, m0, 1
    psubd           m0, m1
    pabsd           m0, m0
%endmacro

%macro PSY_PP_8x8_AVX2 0
    lea             r4, [r1 * 3]
    movu           xm0, [r0]
    movu           xm1, [r0 + r1]
    movu           xm2, [r0 + r1 * 2]
    movu           xm3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movu           xm4, [r5]
    movu           xm5, [r5 + r1]
    movu           xm6, [r5 + r1 * 2]
    movu           xm7, [r5 + r4]

    lea             r4, [r3 * 3]
    vinserti128     m0, m0, [r2], 1
    vinserti128     m1, m1, [r2 + r3], 1
    vinserti128     m2, m2, [r2 + r3 * 2], 1
    vinserti128     m3, m3, [r2 + r4], 1
    lea             r5, [r2 + r3 * 4]
    vinserti128     m4, m4, [r5], 1
    vinserti128     m5, m5, [r5 + r3], 1
    vinserti128     m6, m6, [r5 + r3 * 2], 1
    vinserti128     m7, m7, [r5 + r4], 1

    paddw           m8, m0, m1
    paddw           m8, m2
    paddw           m8, m3
    paddw           m8, m4
    paddw           m8, m5
    paddw           m8, m6
    paddw           m8, m7
    pmaddwd         m8, [pw_1]

    psrldq          m9, m8, 8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    psubw           m9, m1, m0
    paddw           m0, m1
    psubw           m1, m3, m2
    paddw           m2, m3
    punpckhwd       m3, m0, m9
    punpcklwd       m0, m9
    psubw           m9, m3, m0
    paddw           m0, m3
    punpckhwd       m3, m2, m1
    punpcklwd       m2, m1
    psubw           m10, m3, m2
    paddw           m2, m3
    psubw           m3, m5, m4
    paddw           m4, m5
    psubw           m5, m7, m6
    paddw           m6, m7
    punpckhwd       m1, m4, m3
    punpcklwd       m4, m3
    psubw           m7, m1, m4
    paddw           m4, m1
    punpckhwd       m3, m6, m5
    punpcklwd       m6, m5
    psubw           m1, m3, m6
    paddw           m6, m3
    psubw           m3, m2, m0
    paddw           m0, m2
    psubw           m2, m10, m9
    paddw           m9, m10
    punpckhdq       m5, m0, m3
    punpckldq       m0, m3
    psubw           m10, m5, m0
    paddw           m0, m5
    punpckhdq       m3, m9, m2
    punpckldq       m9, m2
    psubw           m5, m3, m9
    paddw           m9, m3
    psubw           m3, m6, m4
    paddw           m4, m6
    psubw           m6, m1, m7
    paddw           m7, m1
    punpckhdq       m2, m4, m3
    punpckldq       m4, m3
    psubw           m1, m2, m4
    paddw           m4, m2
    punpckhdq       m3, m7, m6
    punpckldq       m7, m6
    psubw           m2, m3, m7
    paddw           m7, m3
    psubw           m3, m4, m0
    paddw           m0, m4
    psubw           m4, m1, m10
    paddw           m10, m1
    punpckhqdq      m6, m0, m3
    punpcklqdq      m0, m3
    pabsw           m0, m0
    pabsw           m6, m6
    pmaxsw          m0, m6
    punpckhqdq      m3, m10, m4
    punpcklqdq      m10, m4
    pabsw           m10, m10
    pabsw           m3, m3
    pmaxsw          m10, m3
    psubw           m3, m7, m9
    paddw           m9, m7
    psubw           m7, m2, m5
    paddw           m5, m2
    punpckhqdq      m4, m9, m3
    punpcklqdq      m9, m3
    pabsw           m9, m9
    pabsw           m4, m4
    pmaxsw          m9, m4
    punpckhqdq      m3, m5, m7
    punpcklqdq      m5, m7
    pabsw           m5, m5
    pabsw           m3, m3
    pmaxsw          m5, m3
    paddd           m0, m9
    paddd           m0, m10
    paddd           m0, m5
    psrld           m9, m0, 16
    pslld           m0, 16
    psrld           m0, 16
    paddd           m0, m9
    psrldq          m9, m0, 8
    paddd           m0, m9
    psrldq          m9, m0, 4
    paddd           m0, m9
    paddd           m0, [pd_1]
    psrld           m0, 1
    psubd           m0, m8

    vextracti128   xm1, m0, 1
    psubd          xm1, xm0
    pabsd          xm1, xm1
%endmacro

%macro PSY_COST_PP_8x8_MAIN12 0
    ; load source pixels
    lea             r4, [r1 * 3]
    pmovzxwd        m0, [r0]
    pmovzxwd        m1, [r0 + r1]
    pmovzxwd        m2, [r0 + r1 * 2]
    pmovzxwd        m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    pmovzxwd        m4, [r5]
    pmovzxwd        m5, [r5 + r1]
    pmovzxwd        m6, [r5 + r1 * 2]
    pmovzxwd        m7, [r5 + r4]

    ; source SAD
    paddd           m8, m0, m1
    paddd           m8, m2
    paddd           m8, m3
    paddd           m8, m4
    paddd           m8, m5
    paddd           m8, m6
    paddd           m8, m7

    vextracti128    xm9, m8, 1
    paddd           m8, m9              ; sad_8x8
    movhlps         xm9, xm8
    paddd           xm8, xm9
    pshuflw         xm9, xm8, 0Eh
    paddd           xm8, xm9
    psrld           m8, 2

    ; source SA8D
    psubd           m9, m1, m0
    paddd           m0, m1
    psubd           m1, m3, m2
    paddd           m2, m3
    punpckhdq       m3, m0, m9
    punpckldq       m0, m9
    psubd           m9, m3, m0
    paddd           m0, m3
    punpckhdq       m3, m2, m1
    punpckldq       m2, m1
    psubd           m10, m3, m2
    paddd           m2, m3
    psubd           m3, m5, m4
    paddd           m4, m5
    psubd           m5, m7, m6
    paddd           m6, m7
    punpckhdq       m1, m4, m3
    punpckldq       m4, m3
    psubd           m7, m1, m4
    paddd           m4, m1
    punpckhdq       m3, m6, m5
    punpckldq       m6, m5
    psubd           m1, m3, m6
    paddd           m6, m3
    psubd           m3, m2, m0
    paddd           m0, m2
    psubd           m2, m10, m9
    paddd           m9, m10
    punpckhqdq      m5, m0, m3
    punpcklqdq      m0, m3
    psubd           m10, m5, m0
    paddd           m0, m5
    punpckhqdq      m3, m9, m2
    punpcklqdq      m9, m2
    psubd           m5, m3, m9
    paddd           m9, m3
    psubd           m3, m6, m4
    paddd           m4, m6
    psubd           m6, m1, m7
    paddd           m7, m1
    punpckhqdq      m2, m4, m3
    punpcklqdq      m4, m3
    psubd           m1, m2, m4
    paddd           m4, m2
    punpckhqdq      m3, m7, m6
    punpcklqdq      m7, m6
    psubd           m2, m3, m7
    paddd           m7, m3
    psubd           m3, m4, m0
    paddd           m0, m4
    psubd           m4, m1, m10
    paddd           m10, m1
    vinserti128     m6, m0, xm3, 1
    vperm2i128      m0, m0, m3, 00110001b
    pabsd           m0, m0
    pabsd           m6, m6
    pmaxsd          m0, m6
    vinserti128     m3, m10, xm4, 1
    vperm2i128      m10, m10, m4, 00110001b
    pabsd           m10, m10
    pabsd           m3, m3
    pmaxsd          m10, m3
    psubd           m3, m7, m9
    paddd           m9, m7
    psubd           m7, m2, m5
    paddd           m5, m2
    vinserti128     m4, m9, xm3, 1
    vperm2i128      m9, m9, m3, 00110001b
    pabsd           m9, m9
    pabsd           m4, m4
    pmaxsd          m9, m4
    vinserti128     m3, m5, xm7, 1
    vperm2i128      m5, m5, m7, 00110001b
    pabsd           m5, m5
    pabsd           m3, m3
    pmaxsd          m5, m3
    paddd           m0, m9
    paddd           m0, m10
    paddd           m0, m5

    vextracti128    xm9, m0, 1
    paddd           m0, m9              ; sad_8x8
    movhlps         xm9, xm0
    paddd           xm0, xm9
    pshuflw         xm9, xm0, 0Eh
    paddd           xm0, xm9
    paddd           m0, [pd_1]
    psrld           m0, 1               ; sa8d_8x8
    psubd           m11, m0, m8         ; sa8d_8x8 - sad_8x8

    ; load recon pixels
    lea             r4, [r3 * 3]
    pmovzxwd        m0, [r2]
    pmovzxwd        m1, [r2 + r3]
    pmovzxwd        m2, [r2 + r3 * 2]
    pmovzxwd        m3, [r2 + r4]
    lea             r5, [r2 + r3 * 4]
    pmovzxwd        m4, [r5]
    pmovzxwd        m5, [r5 + r3]
    pmovzxwd        m6, [r5 + r3 * 2]
    pmovzxwd        m7, [r5 + r4]

    ; recon SAD
    paddd           m8, m0, m1
    paddd           m8, m2
    paddd           m8, m3
    paddd           m8, m4
    paddd           m8, m5
    paddd           m8, m6
    paddd           m8, m7

    vextracti128    xm9, m8, 1
    paddd           m8, m9              ; sad_8x8
    movhlps         xm9, xm8
    paddd           xm8, xm9
    pshuflw         xm9, xm8, 0Eh
    paddd           xm8, xm9
    psrld           m8, 2

    ; recon SA8D
    psubd           m9, m1, m0
    paddd           m0, m1
    psubd           m1, m3, m2
    paddd           m2, m3
    punpckhdq       m3, m0, m9
    punpckldq       m0, m9
    psubd           m9, m3, m0
    paddd           m0, m3
    punpckhdq       m3, m2, m1
    punpckldq       m2, m1
    psubd           m10, m3, m2
    paddd           m2, m3
    psubd           m3, m5, m4
    paddd           m4, m5
    psubd           m5, m7, m6
    paddd           m6, m7
    punpckhdq       m1, m4, m3
    punpckldq       m4, m3
    psubd           m7, m1, m4
    paddd           m4, m1
    punpckhdq       m3, m6, m5
    punpckldq       m6, m5
    psubd           m1, m3, m6
    paddd           m6, m3
    psubd           m3, m2, m0
    paddd           m0, m2
    psubd           m2, m10, m9
    paddd           m9, m10
    punpckhqdq      m5, m0, m3
    punpcklqdq      m0, m3
    psubd           m10, m5, m0
    paddd           m0, m5
    punpckhqdq      m3, m9, m2
    punpcklqdq      m9, m2
    psubd           m5, m3, m9
    paddd           m9, m3
    psubd           m3, m6, m4
    paddd           m4, m6
    psubd           m6, m1, m7
    paddd           m7, m1
    punpckhqdq      m2, m4, m3
    punpcklqdq      m4, m3
    psubd           m1, m2, m4
    paddd           m4, m2
    punpckhqdq      m3, m7, m6
    punpcklqdq      m7, m6
    psubd           m2, m3, m7
    paddd           m7, m3
    psubd           m3, m4, m0
    paddd           m0, m4
    psubd           m4, m1, m10
    paddd           m10, m1
    vinserti128     m6, m0, xm3, 1
    vperm2i128      m0, m0, m3, 00110001b
    pabsd           m0, m0
    pabsd           m6, m6
    pmaxsd          m0, m6
    vinserti128     m3, m10, xm4, 1
    vperm2i128      m10, m10, m4, 00110001b
    pabsd           m10, m10
    pabsd           m3, m3
    pmaxsd          m10, m3
    psubd           m3, m7, m9
    paddd           m9, m7
    psubd           m7, m2, m5
    paddd           m5, m2
    vinserti128     m4, m9, xm3, 1
    vperm2i128      m9, m9, m3, 00110001b
    pabsd           m9, m9
    pabsd           m4, m4
    pmaxsd          m9, m4
    vinserti128     m3, m5, xm7, 1
    vperm2i128      m5, m5, m7, 00110001b
    pabsd           m5, m5
    pabsd           m3, m3
    pmaxsd          m5, m3
    paddd           m0, m9
    paddd           m0, m10
    paddd           m0, m5

    vextracti128    xm9, m0, 1
    paddd           m0, m9              ; sad_8x8
    movhlps         xm9, xm0
    paddd           xm0, xm9
    pshuflw         xm9, xm0, 0Eh
    paddd           xm0, xm9
    paddd           m0, [pd_1]
    psrld           m0, 1               ; sa8d_8x8
    psubd           m0, m8              ; sa8d_8x8 - sad_8x8

    psubd          m11, m0
    pabsd          m11, m11
%endmacro

%if ARCH_X86_64
INIT_YMM avx2
%if HIGH_BIT_DEPTH && BIT_DEPTH == 12
cglobal psyCost_pp_8x8, 4, 8, 12
    add             r1d, r1d
    add             r3d, r3d
    PSY_COST_PP_8x8_MAIN12
    movd           eax, xm11
    RET
%endif

%if HIGH_BIT_DEPTH && BIT_DEPTH == 10
cglobal psyCost_pp_8x8, 4, 8, 11
    add            r1d, r1d
    add            r3d, r3d
    PSY_PP_8x8_AVX2
    movd           eax, xm1
    RET
%endif

%if BIT_DEPTH == 8
cglobal psyCost_pp_8x8, 4, 8, 13
    lea             r4, [3 * r1]
    lea             r7, [3 * r3]
    mova            m8, [hmul_8p]

    PSY_PP_8x8

    movd            eax, xm0
    RET
%endif
%endif

%if ARCH_X86_64
INIT_YMM avx2
%if HIGH_BIT_DEPTH && BIT_DEPTH == 12
cglobal psyCost_pp_16x16, 4, 10, 13
    add            r1d, r1d
    add            r3d, r3d
    pxor           m12, m12

    mov            r8d, 2
.loopH:
    mov            r9d, 2
.loopW:
    PSY_COST_PP_8x8_MAIN12

    paddd         xm12, xm11
    add             r0, 16
    add             r2, 16
    dec            r9d
    jnz            .loopW
    lea             r0, [r0 + r1 * 8 - 32]
    lea             r2, [r2 + r3 * 8 - 32]
    dec            r8d
    jnz            .loopH
    movd           eax, xm12
    RET
%endif

%if HIGH_BIT_DEPTH && BIT_DEPTH == 10
cglobal psyCost_pp_16x16, 4, 10, 12
    add            r1d, r1d
    add            r3d, r3d
    pxor           m11, m11

    mov            r8d, 2
.loopH:
    mov            r9d, 2
.loopW:
    PSY_PP_8x8_AVX2

    paddd         xm11, xm1
    add             r0, 16
    add             r2, 16
    dec            r9d
    jnz            .loopW
    lea             r0, [r0 + r1 * 8 - 32]
    lea             r2, [r2 + r3 * 8 - 32]
    dec            r8d
    jnz            .loopH
    movd           eax, xm11
    RET
%endif

%if BIT_DEPTH == 8
cglobal psyCost_pp_16x16, 4, 10, 14
    lea             r4, [3 * r1]
    lea             r7, [3 * r3]
    mova            m8, [hmul_8p]
    pxor            m13, m13

    mov             r8d, 2
.loopH:
    mov             r9d, 2
.loopW:
    PSY_PP_8x8

    paddd           m13, m0
    add             r0, 8
    add             r2, 8
    dec             r9d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 16]
    lea             r2, [r2 + r3 * 8 - 16]
    dec             r8d
    jnz             .loopH
    movd            eax, xm13
    RET
%endif
%endif

%if ARCH_X86_64
INIT_YMM avx2
%if HIGH_BIT_DEPTH && BIT_DEPTH == 12
cglobal psyCost_pp_32x32, 4, 10, 13
    add            r1d, r1d
    add            r3d, r3d
    pxor           m12, m12

    mov            r8d, 4
.loopH:
    mov            r9d, 4
.loopW:
    PSY_COST_PP_8x8_MAIN12

    paddd         xm12, xm11
    add             r0, 16
    add             r2, 16
    dec            r9d
    jnz            .loopW
    lea             r0, [r0 + r1 * 8 - 64]
    lea             r2, [r2 + r3 * 8 - 64]
    dec            r8d
    jnz            .loopH
    movd           eax, xm12
    RET
%endif

%if HIGH_BIT_DEPTH && BIT_DEPTH == 10
cglobal psyCost_pp_32x32, 4, 10, 12
    add            r1d, r1d
    add            r3d, r3d
    pxor           m11, m11

    mov            r8d, 4
.loopH:
    mov            r9d, 4
.loopW:
    PSY_PP_8x8_AVX2

    paddd         xm11, xm1
    add             r0, 16
    add             r2, 16
    dec            r9d
    jnz            .loopW
    lea             r0, [r0 + r1 * 8 - 64]
    lea             r2, [r2 + r3 * 8 - 64]
    dec            r8d
    jnz            .loopH
    movd           eax, xm11
    RET
%endif

%if BIT_DEPTH == 8
cglobal psyCost_pp_32x32, 4, 10, 14
    lea             r4, [3 * r1]
    lea             r7, [3 * r3]
    mova            m8, [hmul_8p]
    pxor            m13, m13

    mov             r8d, 4
.loopH:
    mov             r9d, 4
.loopW:
    PSY_PP_8x8

    paddd           m13, m0
    add             r0, 8
    add             r2, 8
    dec             r9d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 32]
    lea             r2, [r2 + r3 * 8 - 32]
    dec             r8d
    jnz             .loopH
    movd            eax, xm13
    RET
%endif
%endif

%if ARCH_X86_64
INIT_YMM avx2
%if HIGH_BIT_DEPTH && BIT_DEPTH == 12
cglobal psyCost_pp_64x64, 4, 10, 13
    add            r1d, r1d
    add            r3d, r3d
    pxor           m12, m12

    mov            r8d, 8
.loopH:
    mov            r9d, 8
.loopW:
    PSY_COST_PP_8x8_MAIN12

    paddd         xm12, xm11
    add             r0, 16
    add             r2, 16
    dec            r9d
    jnz            .loopW
    lea             r0, [r0 + r1 * 8 - 128]
    lea             r2, [r2 + r3 * 8 - 128]
    dec            r8d
    jnz            .loopH
    movd           eax, xm12
    RET
%endif

%if HIGH_BIT_DEPTH && BIT_DEPTH == 10
cglobal psyCost_pp_64x64, 4, 10, 12
    add            r1d, r1d
    add            r3d, r3d
    pxor           m11, m11

    mov            r8d, 8
.loopH:
    mov            r9d, 8
.loopW:
    PSY_PP_8x8_AVX2

    paddd         xm11, xm1
    add             r0, 16
    add             r2, 16
    dec            r9d
    jnz            .loopW
    lea             r0, [r0 + r1 * 8 - 128]
    lea             r2, [r2 + r3 * 8 - 128]
    dec            r8d
    jnz            .loopH
    movd           eax, xm11
    RET
%endif

%if BIT_DEPTH == 8
cglobal psyCost_pp_64x64, 4, 10, 14
    lea             r4, [3 * r1]
    lea             r7, [3 * r3]
    mova            m8, [hmul_8p]
    pxor            m13, m13

    mov             r8d, 8
.loopH:
    mov             r9d, 8
.loopW:
    PSY_PP_8x8

    paddd           m13, m0
    add             r0, 8
    add             r2, 8
    dec             r9d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 64]
    lea             r2, [r2 + r3 * 8 - 64]
    dec             r8d
    jnz             .loopH
    movd            eax, xm13
    RET
%endif
%endif

;---------------------------------------------------------------------------------------------------------------------
;int psyCost_ss(const int16_t* source, intptr_t sstride, const int16_t* recon, intptr_t rstride)
;---------------------------------------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal psyCost_ss_4x4, 4, 5, 8

    add             r1, r1
    lea             r4, [3 * r1]
    movddup         m0, [r0]
    movddup         m1, [r0 + r1]
    movddup         m2, [r0 + r1 * 2]
    movddup         m3, [r0 + r4]

    pabsw           m4, m0
    pabsw           m5, m1
    paddw           m5, m4
    pabsw           m4, m2
    paddw           m5, m4
    pabsw           m4, m3
    paddw           m5, m4
    pmaddwd         m5, [pw_1]
    psrldq          m4, m5, 4
    paddd           m5, m4
    psrld           m6, m5, 2

    mova            m4, [hmul_8w]
    pmaddwd         m0, m4
    pmaddwd         m1, m4
    pmaddwd         m2, m4
    pmaddwd         m3, m4

    psrldq          m4, m0, 4
    psubd           m5, m0, m4
    paddd           m0, m4
    shufps          m0, m5, 10001000b

    psrldq          m4, m1, 4
    psubd           m5, m1, m4
    paddd           m1, m4
    shufps          m1, m5, 10001000b

    psrldq          m4, m2, 4
    psubd           m5, m2, m4
    paddd           m2, m4
    shufps          m2, m5, 10001000b

    psrldq          m4, m3, 4
    psubd           m5, m3, m4
    paddd           m3, m4
    shufps          m3, m5, 10001000b

    mova            m4, m0
    paddd           m0, m1
    psubd           m1, m4
    mova            m4, m2
    paddd           m2, m3
    psubd           m3, m4
    mova            m4, m0
    paddd           m0, m2
    psubd           m2, m4
    mova            m4, m1
    paddd           m1, m3
    psubd           m3, m4

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    movhlps         m1, m0
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1
    psrld           m0, 1
    psubd           m7, m0, m6

    add             r3, r3
    lea             r4, [3 * r3]
    movddup         m0, [r2]
    movddup         m1, [r2 + r3]
    movddup         m2, [r2 + r3 * 2]
    movddup         m3, [r2 + r4]

    pabsw           m4, m0
    pabsw           m5, m1
    paddw           m5, m4
    pabsw           m4, m2
    paddw           m5, m4
    pabsw           m4, m3
    paddw           m5, m4
    pmaddwd         m5, [pw_1]
    psrldq          m4, m5, 4
    paddd           m5, m4
    psrld           m6, m5, 2

    mova            m4, [hmul_8w]
    pmaddwd         m0, m4
    pmaddwd         m1, m4
    pmaddwd         m2, m4
    pmaddwd         m3, m4

    psrldq          m4, m0, 4
    psubd           m5, m0, m4
    paddd           m0, m4
    shufps          m0, m5, 10001000b

    psrldq          m4, m1, 4
    psubd           m5, m1, m4
    paddd           m1, m4
    shufps          m1, m5, 10001000b

    psrldq          m4, m2, 4
    psubd           m5, m2, m4
    paddd           m2, m4
    shufps          m2, m5, 10001000b

    psrldq          m4, m3, 4
    psubd           m5, m3, m4
    paddd           m3, m4
    shufps          m3, m5, 10001000b

    mova            m4, m0
    paddd           m0, m1
    psubd           m1, m4
    mova            m4, m2
    paddd           m2, m3
    psubd           m3, m4
    mova            m4, m0
    paddd           m0, m2
    psubd           m2, m4
    mova            m4, m1
    paddd           m1, m3
    psubd           m3, m4

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    movhlps         m1, m0
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1
    psrld           m0, 1
    psubd           m0, m6
    psubd           m7, m0
    pabsd           m0, m7
    movd            eax, m0
    RET

%if ARCH_X86_64
INIT_XMM sse4
cglobal psyCost_ss_8x8, 4, 6, 15

    mova            m13, [pw_pmpmpmpm]
    mova            m14, [pw_1]
    add             r1, r1
    add             r3, r3
    lea             r4, [3 * r1]
    movu            m0, [r0]
    movu            m1, [r0 + r1]
    movu            m2, [r0 + r1 * 2]
    movu            m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r1]
    movu            m6, [r5 + r1 * 2]
    movu            m7, [r5 + r4]

    pabsw           m8, m0
    pabsw           m9, m1
    paddw           m8, m9
    pabsw           m10, m2
    pabsw           m11, m3
    paddw           m10, m11
    paddw           m8, m10
    pabsw           m9, m4
    pabsw           m10, m5
    paddw           m9, m10
    pabsw           m11, m6
    pabsw           m12, m7
    paddw           m11, m12
    paddw           m9, m11
    paddw           m8, m9
    movhlps         m9, m8
    pmovzxwd        m8, m8
    pmovzxwd        m9, m9
    paddd           m8, m9
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    pmaddwd         m0, m13
    pmaddwd         m1, m13
    pmaddwd         m2, m13
    pmaddwd         m3, m13

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    pmaddwd         m4, m13
    pmaddwd         m5, m13
    pmaddwd         m6, m13
    pmaddwd         m7, m13

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m11, m0, m7

    movu            m0, [r0]
    movu            m1, [r0 + r1]
    movu            m2, [r0 + r1 * 2]
    movu            m3, [r0 + r4]

    pmaddwd         m0, m14
    pmaddwd         m1, m14
    pmaddwd         m2, m14
    pmaddwd         m3, m14

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    movu            m4, [r5]
    movu            m5, [r5 + r1]
    movu            m6, [r5 + r1 * 2]
    movu            m7, [r5 + r4]

    pmaddwd         m4, m14
    pmaddwd         m5, m14
    pmaddwd         m6, m14
    pmaddwd         m7, m14

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m0, m7
    paddd           m0, m11

    movhlps         m1, m0
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1
    paddd           m0, [pd_2]
    psrld           m0, 2
    psubd           m12, m0, m8

    lea             r4, [3 * r3]
    movu            m0, [r2]
    movu            m1, [r2 + r3]
    movu            m2, [r2 + r3 * 2]
    movu            m3, [r2 + r4]
    lea             r5, [r2 + r3 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r3]
    movu            m6, [r5 + r3 * 2]
    movu            m7, [r5 + r4]

    pabsw           m8, m0
    pabsw           m9, m1
    paddw           m8, m9
    pabsw           m10, m2
    pabsw           m11, m3
    paddw           m10, m11
    paddw           m8, m10
    pabsw           m9, m4
    pabsw           m10, m5
    paddw           m9, m10
    pabsw           m11, m6
    pabsw           m10, m7
    paddw           m11, m10
    paddw           m9, m11
    paddw           m8, m9
    movhlps         m9, m8
    pmovzxwd        m8, m8
    pmovzxwd        m9, m9
    paddd           m8, m9
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    pmaddwd         m0, m13
    pmaddwd         m1, m13
    pmaddwd         m2, m13
    pmaddwd         m3, m13

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    pmaddwd         m4, m13
    pmaddwd         m5, m13
    pmaddwd         m6, m13
    pmaddwd         m7, m13

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m11, m0, m7

    movu            m0, [r2]
    movu            m1, [r2 + r3]
    movu            m2, [r2 + r3 * 2]
    movu            m3, [r2 + r4]

    pmaddwd         m0, m14
    pmaddwd         m1, m14
    pmaddwd         m2, m14
    pmaddwd         m3, m14

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    movu            m4, [r5]
    movu            m5, [r5 + r3]
    movu            m6, [r5 + r3 * 2]
    movu            m7, [r5 + r4]

    pmaddwd         m4, m14
    pmaddwd         m5, m14
    pmaddwd         m6, m14
    pmaddwd         m7, m14

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m0, m7
    paddd           m0, m11

    movhlps         m1, m0
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1
    paddd           m0, [pd_2]
    psrld           m0, 2
    psubd           m0, m8

    psubd           m12, m0
    pabsd           m0, m12
    movd            eax, m0
    RET
%endif

%macro psy_cost_ss 0
    movu            m0, [r0]
    movu            m1, [r0 + r1]
    movu            m2, [r0 + r1 * 2]
    movu            m3, [r0 + r4]
    lea             r5, [r0 + r1 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r1]
    movu            m6, [r5 + r1 * 2]
    movu            m7, [r5 + r4]

    pabsw           m8, m0
    pabsw           m9, m1
    paddw           m8, m9
    pabsw           m10, m2
    pabsw           m11, m3
    paddw           m10, m11
    paddw           m8, m10
    pabsw           m9, m4
    pabsw           m10, m5
    paddw           m9, m10
    pabsw           m11, m6
    pabsw           m12, m7
    paddw           m11, m12
    paddw           m9, m11
    paddw           m8, m9
    movhlps         m9, m8
    pmovzxwd        m8, m8
    pmovzxwd        m9, m9
    paddd           m8, m9
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    pmaddwd         m0, m13
    pmaddwd         m1, m13
    pmaddwd         m2, m13
    pmaddwd         m3, m13

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    pmaddwd         m4, m13
    pmaddwd         m5, m13
    pmaddwd         m6, m13
    pmaddwd         m7, m13

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m11, m0, m7

    movu            m0, [r0]
    movu            m1, [r0 + r1]
    movu            m2, [r0 + r1 * 2]
    movu            m3, [r0 + r4]

    pmaddwd         m0, m14
    pmaddwd         m1, m14
    pmaddwd         m2, m14
    pmaddwd         m3, m14

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    movu            m4, [r5]
    movu            m5, [r5 + r1]
    movu            m6, [r5 + r1 * 2]
    movu            m7, [r5 + r4]

    pmaddwd         m4, m14
    pmaddwd         m5, m14
    pmaddwd         m6, m14
    pmaddwd         m7, m14

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m0, m7
    paddd           m0, m11

    movhlps         m1, m0
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1
    paddd           m0, [pd_2]
    psrld           m0, 2
    psubd           m12, m0, m8

    movu            m0, [r2]
    movu            m1, [r2 + r3]
    movu            m2, [r2 + r3 * 2]
    movu            m3, [r2 + r6]
    lea             r5, [r2 + r3 * 4]
    movu            m4, [r5]
    movu            m5, [r5 + r3]
    movu            m6, [r5 + r3 * 2]
    movu            m7, [r5 + r6]

    pabsw           m8, m0
    pabsw           m9, m1
    paddw           m8, m9
    pabsw           m10, m2
    pabsw           m11, m3
    paddw           m10, m11
    paddw           m8, m10
    pabsw           m9, m4
    pabsw           m10, m5
    paddw           m9, m10
    pabsw           m11, m6
    pabsw           m10, m7
    paddw           m11, m10
    paddw           m9, m11
    paddw           m8, m9
    movhlps         m9, m8
    pmovzxwd        m8, m8
    pmovzxwd        m9, m9
    paddd           m8, m9
    movhlps         m9, m8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2

    pmaddwd         m0, m13
    pmaddwd         m1, m13
    pmaddwd         m2, m13
    pmaddwd         m3, m13

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    pmaddwd         m4, m13
    pmaddwd         m5, m13
    pmaddwd         m6, m13
    pmaddwd         m7, m13

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m11, m0, m7

    movu            m0, [r2]
    movu            m1, [r2 + r3]
    movu            m2, [r2 + r3 * 2]
    movu            m3, [r2 + r6]

    pmaddwd         m0, m14
    pmaddwd         m1, m14
    pmaddwd         m2, m14
    pmaddwd         m3, m14

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    shufps          m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    shufps          m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    shufps          m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    shufps          m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    movu            m4, [r5]
    movu            m5, [r5 + r3]
    movu            m6, [r5 + r3 * 2]
    movu            m7, [r5 + r6]

    pmaddwd         m4, m14
    pmaddwd         m5, m14
    pmaddwd         m6, m14
    pmaddwd         m7, m14

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    shufps          m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    shufps          m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    shufps          m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    shufps          m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m0, m7
    paddd           m0, m11

    movhlps         m1, m0
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1
    paddd           m0, [pd_2]
    psrld           m0, 2
    psubd           m0, m8

    psubd           m12, m0
    pabsd           m0, m12
    paddd           m15, m0
%endmacro

%if ARCH_X86_64
INIT_XMM sse4
cglobal psyCost_ss_16x16, 4, 9, 16

    mova            m13, [pw_pmpmpmpm]
    mova            m14, [pw_1]
    add             r1, r1
    add             r3, r3
    lea             r4, [3 * r1]
    lea             r6, [3 * r3]
    pxor            m15, m15
    mov             r7d, 2
.loopH:
    mov             r8d, 2
.loopW:
    psy_cost_ss
    add             r0, 16
    add             r2, 16
    dec             r8d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 32]
    lea             r2, [r2 + r3 * 8 - 32]
    dec             r7d
    jnz             .loopH
    movd            eax, m15
    RET
%endif

%if ARCH_X86_64
INIT_XMM sse4
cglobal psyCost_ss_32x32, 4, 9, 16

    mova            m13, [pw_pmpmpmpm]
    mova            m14, [pw_1]
    add             r1, r1
    add             r3, r3
    lea             r4, [3 * r1]
    lea             r6, [3 * r3]
    pxor            m15, m15
    mov             r7d, 4
.loopH:
    mov             r8d, 4
.loopW:
    psy_cost_ss
    add             r0, 16
    add             r2, 16
    dec             r8d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 64]
    lea             r2, [r2 + r3 * 8 - 64]
    dec             r7d
    jnz             .loopH
    movd            eax, m15
    RET
%endif

%if ARCH_X86_64
INIT_XMM sse4
cglobal psyCost_ss_64x64, 4, 9, 16

    mova            m13, [pw_pmpmpmpm]
    mova            m14, [pw_1]
    add             r1, r1
    add             r3, r3
    lea             r4, [3 * r1]
    lea             r6, [3 * r3]
    pxor            m15, m15
    mov             r7d, 8
.loopH:
    mov             r8d, 8
.loopW:
    psy_cost_ss
    add             r0, 16
    add             r2, 16
    dec             r8d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 128]
    lea             r2, [r2 + r3 * 8 - 128]
    dec             r7d
    jnz             .loopH
    movd            eax, m15
    RET
%endif

INIT_YMM avx2
cglobal psyCost_ss_4x4, 4, 5, 8
    add             r1, r1
    add             r3, r3
    lea             r4, [3 * r1]
    movddup         m0, [r0]
    movddup         m1, [r0 + r1]
    movddup         m2, [r0 + r1 * 2]
    movddup         m3, [r0 + r4]

    lea             r4, [3 * r3]
    movddup         m4, [r2]
    movddup         m5, [r2 + r3]
    movddup         m6, [r2 + r3 * 2]
    movddup         m7, [r2 + r4]

    vinserti128     m0, m0, xm4, 1
    vinserti128     m1, m1, xm5, 1
    vinserti128     m2, m2, xm6, 1
    vinserti128     m3, m3, xm7, 1

    pabsw           m4, m0
    pabsw           m5, m1
    paddw           m5, m4
    pabsw           m4, m2
    paddw           m5, m4
    pabsw           m4, m3
    paddw           m5, m4
    pmaddwd         m5, [pw_1]
    psrldq          m4, m5, 4
    paddd           m5, m4
    psrld           m6, m5, 2

    mova            m4, [hmul_8w]
    pmaddwd         m0, m4
    pmaddwd         m1, m4
    pmaddwd         m2, m4
    pmaddwd         m3, m4

    psrldq          m4, m0, 4
    psubd           m5, m0, m4
    paddd           m0, m4
    shufps          m0, m0, m5, 10001000b

    psrldq          m4, m1, 4
    psubd           m5, m1, m4
    paddd           m1, m4
    shufps          m1, m1, m5, 10001000b

    psrldq          m4, m2, 4
    psubd           m5, m2, m4
    paddd           m2, m4
    shufps          m2, m2, m5, 10001000b

    psrldq          m4, m3, 4
    psubd           m5, m3, m4
    paddd           m3, m4
    shufps          m3, m3, m5, 10001000b

    mova            m4, m0
    paddd           m0, m1
    psubd           m1, m4
    mova            m4, m2
    paddd           m2, m3
    psubd           m3, m4
    mova            m4, m0
    paddd           m0, m2
    psubd           m2, m4
    mova            m4, m1
    paddd           m1, m3
    psubd           m3, m4

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    psrldq          m1, m0, 8
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1
    psrld           m0, 1
    psubd           m0, m6
    vextracti128    xm1, m0, 1
    psubd           m0, m1
    pabsd           m0, m0
    movd            eax, xm0
    RET

%macro PSY_SS_8x8 0
    lea             r4, [3 * r1]
    lea             r6, [r0 + r1 * 4]
    movu            xm0, [r0]
    movu            xm1, [r0 + r1]
    movu            xm2, [r0 + r1 * 2]
    movu            xm3, [r0 + r4]
    movu            xm4, [r6]
    movu            xm5, [r6 + r1]
    movu            xm6, [r6 + r1 * 2]
    movu            xm7, [r6 + r4]

    lea             r4, [3 * r3]
    lea             r6, [r2 + r3 * 4]
    movu            xm8, [r2]
    movu            xm9, [r2 + r3]
    movu            xm10, [r2 + r3 * 2]
    movu            xm11, [r2 + r4]
    vinserti128     m0, m0, xm8, 1
    vinserti128     m1, m1, xm9, 1
    vinserti128     m2, m2, xm10, 1
    vinserti128     m3, m3, xm11, 1
    movu            xm8, [r6]
    movu            xm9, [r6 + r3]
    movu            xm10, [r6 + r3 * 2]
    movu            xm11, [r6 + r4]
    vinserti128     m4, m4, xm8, 1
    vinserti128     m5, m5, xm9, 1
    vinserti128     m6, m6, xm10, 1
    vinserti128     m7, m7, xm11, 1

    ;; store on stack to use later
    mova            [rsp + 0 * mmsize], m0
    mova            [rsp + 1 * mmsize], m1
    mova            [rsp + 2 * mmsize], m2
    mova            [rsp + 3 * mmsize], m3
    mova            [rsp + 4 * mmsize], m4
    mova            [rsp + 5 * mmsize], m5
    mova            [rsp + 6 * mmsize], m6
    mova            [rsp + 7 * mmsize], m7

    pabsw           m8, m0
    pabsw           m9, m1
    paddw           m8, m9
    pabsw           m10, m2
    pabsw           m11, m3
    paddw           m10, m11
    paddw           m8, m10
    pabsw           m9, m4
    pabsw           m10, m5
    paddw           m9, m10
    pabsw           m11, m6
    pabsw           m10, m7
    paddw           m11, m10
    paddw           m9, m11
    paddw           m8, m9
    psrldq          m9, m8, 8

    vextracti128    xm10, m8, 1
    vextracti128    xm11, m9, 1

    vpmovzxwd       m8, xm8
    vpmovzxwd       m9, xm9
    vpmovzxwd       m10, xm10
    vpmovzxwd       m11, xm11

    vinserti128     m8, m8, xm10, 1
    vinserti128     m9, m9, xm11, 1

    paddd           m8, m9
    psrldq          m9, m8, 8
    paddd           m8, m9
    psrldq          m9, m8, 4
    paddd           m8, m9
    psrld           m8, 2       ; sad_4x4

    pmaddwd         m0, m13
    pmaddwd         m1, m13
    pmaddwd         m2, m13
    pmaddwd         m3, m13

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    vshufps         m0, m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    vshufps         m0, m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    vshufps         m1, m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    vshufps         m1, m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    vshufps         m2, m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    vshufps         m2, m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    vshufps         m3, m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    vshufps         m3, m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    pmaddwd         m4, m13
    pmaddwd         m5, m13
    pmaddwd         m6, m13
    pmaddwd         m7, m13

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    vshufps         m4, m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    vshufps         m4, m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    vshufps         m5, m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    vshufps         m5, m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    vshufps         m6, m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    vshufps         m6, m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    vshufps         m7, m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    vshufps         m7, m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m11, m0, m7

    pmaddwd         m0, m12, [rsp + 0 * mmsize]
    pmaddwd         m1, m12, [rsp + 1 * mmsize]
    pmaddwd         m2, m12, [rsp + 2 * mmsize]
    pmaddwd         m3, m12, [rsp + 3 * mmsize]

    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    vshufps         m0, m0, m10, 10001000b
    psrldq          m9, m0, 4
    psubd           m10, m0, m9
    paddd           m0, m9
    vshufps         m0, m0, m10, 10001000b

    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    vshufps         m1, m1, m10, 10001000b
    psrldq          m9, m1, 4
    psubd           m10, m1, m9
    paddd           m1, m9
    vshufps         m1, m1, m10, 10001000b

    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    vshufps         m2, m2, m10, 10001000b
    psrldq          m9, m2, 4
    psubd           m10, m2, m9
    paddd           m2, m9
    vshufps         m2, m2, m10, 10001000b

    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    vshufps         m3, m3, m10, 10001000b
    psrldq          m9, m3, 4
    psubd           m10, m3, m9
    paddd           m3, m9
    vshufps         m3, m3, m10, 10001000b

    SUMSUB_BA d, 0, 1, 9
    SUMSUB_BA d, 2, 3, 9
    SUMSUB_BA d, 0, 2, 9
    SUMSUB_BA d, 1, 3, 9

    pmaddwd         m4, m12, [rsp + 4 * mmsize]
    pmaddwd         m5, m12, [rsp + 5 * mmsize]
    pmaddwd         m6, m12, [rsp + 6 * mmsize]
    pmaddwd         m7, m12, [rsp + 7 * mmsize]

    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    vshufps         m4, m4, m10, 10001000b
    psrldq          m9, m4, 4
    psubd           m10, m4, m9
    paddd           m4, m9
    vshufps         m4, m4, m10, 10001000b

    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    vshufps         m5, m5, m10, 10001000b
    psrldq          m9, m5, 4
    psubd           m10, m5, m9
    paddd           m5, m9
    vshufps         m5, m5, m10, 10001000b

    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    vshufps         m6, m6, m10, 10001000b
    psrldq          m9, m6, 4
    psubd           m10, m6, m9
    paddd           m6, m9
    vshufps         m6, m6, m10, 10001000b

    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    vshufps         m7, m7, m10, 10001000b
    psrldq          m9, m7, 4
    psubd           m10, m7, m9
    paddd           m7, m9
    vshufps         m7, m7, m10, 10001000b

    SUMSUB_BA d, 4, 5, 9
    SUMSUB_BA d, 6, 7, 9
    SUMSUB_BA d, 4, 6, 9
    SUMSUB_BA d, 5, 7, 9

    SUMSUB_BA d, 0, 4, 9
    SUMSUB_BA d, 1, 5, 9
    SUMSUB_BA d, 2, 6, 9
    SUMSUB_BA d, 3, 7, 9

    pabsd           m0, m0
    pabsd           m2, m2
    pabsd           m1, m1
    pabsd           m3, m3
    pabsd           m4, m4
    pabsd           m5, m5
    pabsd           m6, m6
    pabsd           m7, m7

    paddd           m0, m2
    paddd           m1, m3
    paddd           m0, m1
    paddd           m5, m4
    paddd           m0, m5
    paddd           m7, m6
    paddd           m0, m7
    paddd           m0, m11

    psrldq          m1, m0, 8
    paddd           m0, m1
    psrldq          m1, m0, 4
    paddd           m0, m1
    paddd           m0, [pd_2]
    psrld           m0, 2
    psubd           m0, m8
    vextracti128    xm1, m0, 1
    psubd           m0, m1
    pabsd           m0, m0
%endmacro

%if ARCH_X86_64
INIT_YMM avx2
cglobal psyCost_ss_8x8, 4, 7, 14
    ; NOTE: align stack to 64 bytes, so all of local data in same cache line
    mov             r5, rsp
    sub             rsp, 8*mmsize
    and             rsp, ~63

    mova            m12, [pw_1]
    mova            m13, [pw_pmpmpmpm]
    add             r1, r1
    add             r3, r3

    PSY_SS_8x8

    movd            eax, xm0
    mov             rsp, r5
    RET
%endif

%if ARCH_X86_64
INIT_YMM avx2
cglobal psyCost_ss_16x16, 4, 9, 15
    ; NOTE: align stack to 64 bytes, so all of local data in same cache line
    mov             r5, rsp
    sub             rsp, 8*mmsize
    and             rsp, ~63

    mova            m12, [pw_1]
    mova            m13, [pw_pmpmpmpm]
    add             r1, r1
    add             r3, r3
    pxor            m14, m14

    mov             r7d, 2
.loopH:
    mov             r8d, 2
.loopW:
    PSY_SS_8x8

    paddd           m14, m0
    add             r0, 16
    add             r2, 16
    dec             r8d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 32]
    lea             r2, [r2 + r3 * 8 - 32]
    dec             r7d
    jnz             .loopH
    movd            eax, xm14
    mov             rsp, r5
    RET
%endif

%if ARCH_X86_64
INIT_YMM avx2
cglobal psyCost_ss_32x32, 4, 9, 15
    ; NOTE: align stack to 64 bytes, so all of local data in same cache line
    mov             r5, rsp
    sub             rsp, 8*mmsize
    and             rsp, ~63

    mova            m12, [pw_1]
    mova            m13, [pw_pmpmpmpm]
    add             r1, r1
    add             r3, r3
    pxor            m14, m14

    mov             r7d, 4
.loopH:
    mov             r8d, 4
.loopW:
    PSY_SS_8x8

    paddd           m14, m0
    add             r0, 16
    add             r2, 16
    dec             r8d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 64]
    lea             r2, [r2 + r3 * 8 - 64]
    dec             r7d
    jnz             .loopH
    movd            eax, xm14
    mov             rsp, r5
    RET
%endif

%if ARCH_X86_64
INIT_YMM avx2
cglobal psyCost_ss_64x64, 4, 9, 15
    ; NOTE: align stack to 64 bytes, so all of local data in same cache line
    mov             r5, rsp
    sub             rsp, 8*mmsize
    and             rsp, ~63

    mova            m12, [pw_1]
    mova            m13, [pw_pmpmpmpm]
    add             r1, r1
    add             r3, r3
    pxor            m14, m14

    mov             r7d, 8
.loopH:
    mov             r8d, 8
.loopW:
    PSY_SS_8x8

    paddd           m14, m0
    add             r0, 16
    add             r2, 16
    dec             r8d
    jnz             .loopW
    lea             r0, [r0 + r1 * 8 - 128]
    lea             r2, [r2 + r3 * 8 - 128]
    dec             r7d
    jnz             .loopH
    movd            eax, xm14
    mov             rsp, r5
    RET
%endif

;;---------------------------------------------------------------
;; SATD AVX2
;; int pixel_satd(const pixel*, intptr_t, const pixel*, intptr_t)
;;---------------------------------------------------------------
;; r0   - pix0
;; r1   - pix0Stride
;; r2   - pix1
;; r3   - pix1Stride

%if ARCH_X86_64 == 1 && HIGH_BIT_DEPTH == 0
INIT_YMM avx2
cglobal calc_satd_16x8    ; function to compute satd cost for 16 columns, 8 rows
    pxor                m6, m6
    vbroadcasti128      m0, [r0]
    vbroadcasti128      m4, [r2]
    vbroadcasti128      m1, [r0 + r1]
    vbroadcasti128      m5, [r2 + r3]
    pmaddubsw           m4, m7
    pmaddubsw           m0, m7
    pmaddubsw           m5, m7
    pmaddubsw           m1, m7
    psubw               m0, m4
    psubw               m1, m5
    vbroadcasti128      m2, [r0 + r1 * 2]
    vbroadcasti128      m4, [r2 + r3 * 2]
    vbroadcasti128      m3, [r0 + r4]
    vbroadcasti128      m5, [r2 + r5]
    pmaddubsw           m4, m7
    pmaddubsw           m2, m7
    pmaddubsw           m5, m7
    pmaddubsw           m3, m7
    psubw               m2, m4
    psubw               m3, m5
    lea                 r0, [r0 + r1 * 4]
    lea                 r2, [r2 + r3 * 4]
    paddw               m4, m0, m1
    psubw               m1, m1, m0
    paddw               m0, m2, m3
    psubw               m3, m2
    paddw               m2, m4, m0
    psubw               m0, m4
    paddw               m4, m1, m3
    psubw               m3, m1
    pabsw               m2, m2
    pabsw               m0, m0
    pabsw               m4, m4
    pabsw               m3, m3
    pblendw             m1, m2, m0, 10101010b
    pslld               m0, 16
    psrld               m2, 16
    por                 m0, m2
    pmaxsw              m1, m0
    paddw               m6, m1
    pblendw             m2, m4, m3, 10101010b
    pslld               m3, 16
    psrld               m4, 16
    por                 m3, m4
    pmaxsw              m2, m3
    paddw               m6, m2
    vbroadcasti128      m1, [r0]
    vbroadcasti128      m4, [r2]
    vbroadcasti128      m2, [r0 + r1]
    vbroadcasti128      m5, [r2 + r3]
    pmaddubsw           m4, m7
    pmaddubsw           m1, m7
    pmaddubsw           m5, m7
    pmaddubsw           m2, m7
    psubw               m1, m4
    psubw               m2, m5
    vbroadcasti128      m0, [r0 + r1 * 2]
    vbroadcasti128      m4, [r2 + r3 * 2]
    vbroadcasti128      m3, [r0 + r4]
    vbroadcasti128      m5, [r2 + r5]
    lea                 r0, [r0 + r1 * 4]
    lea                 r2, [r2 + r3 * 4]
    pmaddubsw           m4, m7
    pmaddubsw           m0, m7
    pmaddubsw           m5, m7
    pmaddubsw           m3, m7
    psubw               m0, m4
    psubw               m3, m5
    paddw               m4, m1, m2
    psubw               m2, m1
    paddw               m1, m0, m3
    psubw               m3, m0
    paddw               m0, m4, m1
    psubw               m1, m4
    paddw               m4, m2, m3
    psubw               m3, m2
    pabsw               m0, m0
    pabsw               m1, m1
    pabsw               m4, m4
    pabsw               m3, m3
    pblendw             m2, m0, m1, 10101010b
    pslld               m1, 16
    psrld               m0, 16
    por                 m1, m0
    pmaxsw              m2, m1
    paddw               m6, m2
    pblendw             m0, m4, m3, 10101010b
    pslld               m3, 16
    psrld               m4, 16
    por                 m3, m4
    pmaxsw              m0, m3
    paddw               m6, m0
    vextracti128        xm0, m6, 1
    pmovzxwd            m6, xm6
    pmovzxwd            m0, xm0
    paddd               m8, m6
    paddd               m9, m0
    ret

cglobal calc_satd_16x4    ; function to compute satd cost for 16 columns, 4 rows
    pxor                m6, m6
    vbroadcasti128      m0, [r0]
    vbroadcasti128      m4, [r2]
    vbroadcasti128      m1, [r0 + r1]
    vbroadcasti128      m5, [r2 + r3]
    pmaddubsw           m4, m7
    pmaddubsw           m0, m7
    pmaddubsw           m5, m7
    pmaddubsw           m1, m7
    psubw               m0, m4
    psubw               m1, m5
    vbroadcasti128      m2, [r0 + r1 * 2]
    vbroadcasti128      m4, [r2 + r3 * 2]
    vbroadcasti128      m3, [r0 + r4]
    vbroadcasti128      m5, [r2 + r5]
    pmaddubsw           m4, m7
    pmaddubsw           m2, m7
    pmaddubsw           m5, m7
    pmaddubsw           m3, m7
    psubw               m2, m4
    psubw               m3, m5
    paddw               m4, m0, m1
    psubw               m1, m1, m0
    paddw               m0, m2, m3
    psubw               m3, m2
    paddw               m2, m4, m0
    psubw               m0, m4
    paddw               m4, m1, m3
    psubw               m3, m1
    pabsw               m2, m2
    pabsw               m0, m0
    pabsw               m4, m4
    pabsw               m3, m3
    pblendw             m1, m2, m0, 10101010b
    pslld               m0, 16
    psrld               m2, 16
    por                 m0, m2
    pmaxsw              m1, m0
    paddw               m6, m1
    pblendw             m2, m4, m3, 10101010b
    pslld               m3, 16
    psrld               m4, 16
    por                 m3, m4
    pmaxsw              m2, m3
    paddw               m6, m2
    vextracti128        xm0, m6, 1
    pmovzxwd            m6, xm6
    pmovzxwd            m0, xm0
    paddd               m8, m6
    paddd               m9, m0
    ret

cglobal pixel_satd_16x4, 4,6,10         ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9

    call            calc_satd_16x4

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_16x12, 4,6,10        ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9

    call            calc_satd_16x8
    call            calc_satd_16x4

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_16x32, 4,6,10        ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_16x64, 4,6,10        ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_32x8, 4,8,10          ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]

    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_32x16, 4,8,10         ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]

    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_32x24, 4,8,10         ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_32x32, 4,8,10         ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_32x64, 4,8,10         ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_48x64, 4,8,10        ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_64x16, 4,8,10         ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_64x32, 4,8,10         ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_64x48, 4,8,10        ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET

cglobal pixel_satd_64x64, 4,8,10        ; if WIN64 && cpuflag(avx2)
    mova            m7, [hmul_16p]
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m8, m8
    pxor            m9, m9
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 16]
    lea             r2, [r7 + 16]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    lea             r0, [r6 + 48]
    lea             r2, [r7 + 48]
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    paddd           m8, m9
    vextracti128    xm0, m8, 1
    paddd           xm0, xm8
    movhlps         xm1, xm0
    paddd           xm0, xm1
    pshuflw         xm1, xm0, q0032
    paddd           xm0, xm1
    movd            eax, xm0
    RET
%endif ; ARCH_X86_64 == 1 && HIGH_BIT_DEPTH == 0

%if ARCH_X86_64 == 1 && HIGH_BIT_DEPTH == 1
INIT_YMM avx2
cglobal calc_satd_16x8    ; function to compute satd cost for 16 columns, 8 rows
    ; rows 0-3
    movu            m0, [r0]
    movu            m4, [r2]
    psubw           m0, m4
    movu            m1, [r0 + r1]
    movu            m5, [r2 + r3]
    psubw           m1, m5
    movu            m2, [r0 + r1 * 2]
    movu            m4, [r2 + r3 * 2]
    psubw           m2, m4
    movu            m3, [r0 + r4]
    movu            m5, [r2 + r5]
    psubw           m3, m5
    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    paddw           m4, m0, m1
    psubw           m1, m0
    paddw           m0, m2, m3
    psubw           m3, m2
    punpckhwd       m2, m4, m1
    punpcklwd       m4, m1
    punpckhwd       m1, m0, m3
    punpcklwd       m0, m3
    paddw           m3, m4, m0
    psubw           m0, m4
    paddw           m4, m2, m1
    psubw           m1, m2
    punpckhdq       m2, m3, m0
    punpckldq       m3, m0
    paddw           m0, m3, m2
    psubw           m2, m3
    punpckhdq       m3, m4, m1
    punpckldq       m4, m1
    paddw           m1, m4, m3
    psubw           m3, m4
    punpckhqdq      m4, m0, m1
    punpcklqdq      m0, m1
    pabsw           m0, m0
    pabsw           m4, m4
    pmaxsw          m0, m0, m4
    punpckhqdq      m1, m2, m3
    punpcklqdq      m2, m3
    pabsw           m2, m2
    pabsw           m1, m1
    pmaxsw          m2, m1
    pxor            m7, m7
    mova            m1, m0
    punpcklwd       m1, m7
    paddd           m6, m1
    mova            m1, m0
    punpckhwd       m1, m7
    paddd           m6, m1
    pxor            m7, m7
    mova            m1, m2
    punpcklwd       m1, m7
    paddd           m6, m1
    mova            m1, m2
    punpckhwd       m1, m7
    paddd           m6, m1
    ; rows 4-7
    movu            m0, [r0]
    movu            m4, [r2]
    psubw           m0, m4
    movu            m1, [r0 + r1]
    movu            m5, [r2 + r3]
    psubw           m1, m5
    movu            m2, [r0 + r1 * 2]
    movu            m4, [r2 + r3 * 2]
    psubw           m2, m4
    movu            m3, [r0 + r4]
    movu            m5, [r2 + r5]
    psubw           m3, m5
    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    paddw           m4, m0, m1
    psubw           m1, m0
    paddw           m0, m2, m3
    psubw           m3, m2
    punpckhwd       m2, m4, m1
    punpcklwd       m4, m1
    punpckhwd       m1, m0, m3
    punpcklwd       m0, m3
    paddw           m3, m4, m0
    psubw           m0, m4
    paddw           m4, m2, m1
    psubw           m1, m2
    punpckhdq       m2, m3, m0
    punpckldq       m3, m0
    paddw           m0, m3, m2
    psubw           m2, m3
    punpckhdq       m3, m4, m1
    punpckldq       m4, m1
    paddw           m1, m4, m3
    psubw           m3, m4
    punpckhqdq      m4, m0, m1
    punpcklqdq      m0, m1
    pabsw           m0, m0
    pabsw           m4, m4
    pmaxsw          m0, m0, m4
    punpckhqdq      m1, m2, m3
    punpcklqdq      m2, m3
    pabsw           m2, m2
    pabsw           m1, m1
    pmaxsw          m2, m1
    pxor            m7, m7
    mova            m1, m0
    punpcklwd       m1, m7
    paddd           m6, m1
    mova            m1, m0
    punpckhwd       m1, m7
    paddd           m6, m1
    pxor            m7, m7
    mova            m1, m2
    punpcklwd       m1, m7
    paddd           m6, m1
    mova            m1, m2
    punpckhwd       m1, m7
    paddd           m6, m1
    ret

cglobal calc_satd_16x4    ; function to compute satd cost for 16 columns, 4 rows
    ; rows 0-3
    movu            m0, [r0]
    movu            m4, [r2]
    psubw           m0, m4
    movu            m1, [r0 + r1]
    movu            m5, [r2 + r3]
    psubw           m1, m5
    movu            m2, [r0 + r1 * 2]
    movu            m4, [r2 + r3 * 2]
    psubw           m2, m4
    movu            m3, [r0 + r4]
    movu            m5, [r2 + r5]
    psubw           m3, m5
    lea             r0, [r0 + r1 * 4]
    lea             r2, [r2 + r3 * 4]
    paddw           m4, m0, m1
    psubw           m1, m0
    paddw           m0, m2, m3
    psubw           m3, m2
    punpckhwd       m2, m4, m1
    punpcklwd       m4, m1
    punpckhwd       m1, m0, m3
    punpcklwd       m0, m3
    paddw           m3, m4, m0
    psubw           m0, m4
    paddw           m4, m2, m1
    psubw           m1, m2
    punpckhdq       m2, m3, m0
    punpckldq       m3, m0
    paddw           m0, m3, m2
    psubw           m2, m3
    punpckhdq       m3, m4, m1
    punpckldq       m4, m1
    paddw           m1, m4, m3
    psubw           m3, m4
    punpckhqdq      m4, m0, m1
    punpcklqdq      m0, m1
    pabsw           m0, m0
    pabsw           m4, m4
    pmaxsw          m0, m0, m4
    punpckhqdq      m1, m2, m3
    punpcklqdq      m2, m3
    pabsw           m2, m2
    pabsw           m1, m1
    pmaxsw          m2, m1
    pxor            m7, m7
    mova            m1, m0
    punpcklwd       m1, m7
    paddd           m6, m1
    mova            m1, m0
    punpckhwd       m1, m7
    paddd           m6, m1
    pxor            m7, m7
    mova            m1, m2
    punpcklwd       m1, m7
    paddd           m6, m1
    mova            m1, m2
    punpckhwd       m1, m7
    paddd           m6, m1
    ret

cglobal pixel_satd_16x4, 4,6,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6

    call            calc_satd_16x4

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_16x8, 4,6,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6

    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_16x12, 4,6,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6

    call            calc_satd_16x8
    call            calc_satd_16x4

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_16x16, 4,6,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6

    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_16x32, 4,6,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_16x64, 4,6,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_32x8, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_32x16, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_32x24, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_32x32, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_32x64, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_48x64, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_64x16, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]

    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 96]
    lea             r2, [r7 + 96]

    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_64x32, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 96]
    lea             r2, [r7 + 96]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_64x48, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 96]
    lea             r2, [r7 + 96]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET

cglobal pixel_satd_64x64, 4,8,8
    add             r1d, r1d
    add             r3d, r3d
    lea             r4, [3 * r1]
    lea             r5, [3 * r3]
    pxor            m6, m6
    mov             r6, r0
    mov             r7, r2

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 32]
    lea             r2, [r7 + 32]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 64]
    lea             r2, [r7 + 64]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    lea             r0, [r6 + 96]
    lea             r2, [r7 + 96]

    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8
    call            calc_satd_16x8

    vextracti128    xm7, m6, 1
    paddd           xm6, xm7
    pxor            xm7, xm7
    movhlps         xm7, xm6
    paddd           xm6, xm7
    pshufd          xm7, xm6, 1
    paddd           xm6, xm7
    movd            eax, xm6
    RET
%endif ; ARCH_X86_64 == 1 && HIGH_BIT_DEPTH == 1


;-------------------------------------------------------------------------------------------------------------------------------------
; pixel planeClipAndMax(pixel *src, intptr_t stride, int width, int height, uint64_t *outsum, const pixel minPix, const pixel maxPix)
;-------------------------------------------------------------------------------------------------------------------------------------
%if ARCH_X86_64 == 1 && HIGH_BIT_DEPTH == 0
INIT_YMM avx2
cglobal planeClipAndMax, 5,7,8
    movd            xm0, r5m
    vpbroadcastb    m0, xm0                 ; m0 = [min]
    vpbroadcastb    m1, r6m                 ; m1 = [max]
    pxor            m2, m2                  ; m2 = sumLuma
    pxor            m3, m3                  ; m3 = maxLumaLevel
    pxor            m4, m4                  ; m4 = zero

    ; get mask to partial register pixels
    mov             r5d, r2d
    and             r2d, ~(mmsize - 1)
    sub             r5d, r2d
    lea             r6, [pb_movemask_32 + mmsize]
    sub             r6, r5
    movu            m5, [r6]                ; m5 = mask for last couple column

.loopH:
    lea             r5d, [r2 - mmsize]

.loopW:
    movu            m6, [r0 + r5]
    pmaxub          m6, m0
    pminub          m6, m1
    movu            [r0 + r5], m6           ; store back
    pmaxub          m3, m6                  ; update maxLumaLevel
    psadbw          m6, m4
    paddq           m2, m6

    sub             r5d, mmsize
    jge            .loopW

    ; partial pixels
    movu            m7, [r0 + r2]
    pmaxub          m6, m7, m0
    pminub          m6, m1

    pand            m7, m5                  ; get invalid/unchange pixel
    pandn           m6, m5, m6              ; clear invalid pixels
    por             m7, m6                  ; combin valid & invalid pixels
    movu            [r0 + r2], m7           ; store back
    pmaxub          m3, m6                  ; update maxLumaLevel
    psadbw          m6, m4
    paddq           m2, m6

.next:
    add             r0, r1
    dec             r3d
    jg             .loopH

    ; sumLuma
    vextracti128    xm0, m2, 1
    paddq           xm0, xm2
    movhlps         xm1, xm0
    paddq           xm0, xm1
    movq            [r4], xm0

    ; maxLumaLevel
    vextracti128    xm0, m3, 1
    pmaxub          xm0, xm3
    movhlps         xm3, xm0
    pmaxub          xm0, xm3
    pmovzxbw        xm0, xm0
    pxor            xm0, [pb_movemask + 16]
    phminposuw      xm0, xm0

    movd            eax, xm0
    not             al
    movzx           eax, al
    RET
%endif ; ARCH_X86_64 == 1 && HIGH_BIT_DEPTH == 0


%if HIGH_BIT_DEPTH == 1 && BIT_DEPTH == 10
%macro LOAD_DIFF_AVX2 4
    movu       %1, %3
    movu       %2, %4
    psubw      %1, %2
%endmacro

%macro LOAD_DIFF_8x4P_AVX2 6-8 r0,r2 ; 4x dest, 2x temp, 2x pointer
    LOAD_DIFF_AVX2 xm%1, xm%5, [%7],      [%8]
    LOAD_DIFF_AVX2 xm%2, xm%6, [%7+r1],   [%8+r3]
    LOAD_DIFF_AVX2 xm%3, xm%5, [%7+2*r1], [%8+2*r3]
    LOAD_DIFF_AVX2 xm%4, xm%6, [%7+r4],   [%8+r5]

    ;lea %7, [%7+4*r1]
    ;lea %8, [%8+4*r3]
%endmacro

INIT_YMM avx2
cglobal pixel_satd_8x8, 4,4,7

    FIX_STRIDES r1, r3
    pxor    xm6, xm6

    ; load_diff 0 & 4
    movu    xm0, [r0]
    movu    xm1, [r2]
    vinserti128 m0, m0, [r0 + r1 * 4], 1
    vinserti128 m1, m1, [r2 + r3 * 4], 1
    psubw   m0, m1
    add     r0, r1
    add     r2, r3

    ; load_diff 1 & 5
    movu    xm1, [r0]
    movu    xm2, [r2]
    vinserti128 m1, m1, [r0 + r1 * 4], 1
    vinserti128 m2, m2, [r2 + r3 * 4], 1
    psubw   m1, m2
    add     r0, r1
    add     r2, r3

    ; load_diff 2 & 6
    movu    xm2, [r0]
    movu    xm3, [r2]
    vinserti128 m2, m2, [r0 + r1 * 4], 1
    vinserti128 m3, m3, [r2 + r3 * 4], 1
    psubw   m2, m3
    add     r0, r1
    add     r2, r3

    ; load_diff 3 & 7
    movu    xm3, [r0]
    movu    xm4, [r2]
    vinserti128 m3, m3, [r0 + r1 * 4], 1
    vinserti128 m4, m4, [r2 + r3 * 4], 1
    psubw   m3, m4

    SATD_8x4_SSE vertical, 0, 1, 2, 3, 4, 5, 6

    vextracti128 xm0, m6, 1
    paddw xm6, xm0
    HADDUW xm6, xm0
    movd   eax, xm6
    RET

INIT_XMM avx2
cglobal pixel_sa8d_8x8_internal
    lea  r6, [r0+4*r1]
    lea  r7, [r2+4*r3]
    LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    ;HADAMARD2_2D 0, 1, 2, 8, 6, wd
    ;HADAMARD2_2D 4, 5, 3, 9, 6, wd
    ;HADAMARD2_2D 0, 2, 1, 8, 6, dq
    ;HADAMARD2_2D 4, 3, 5, 9, 6, dq
    ;HADAMARD2_2D 0, 4, 2, 3, 6, qdq, amax
    ;HADAMARD2_2D 1, 5, 8, 9, 6, qdq, amax

    paddw m0, m1
    paddw m0, m2
    paddw m0, m8
    SAVE_MM_PERMUTATION
    ret


INIT_XMM avx2
cglobal pixel_sa8d_8x8, 4,8,12
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    call pixel_sa8d_8x8_internal
    HADDUW m0, m1
    movd eax, m0
    add eax, 1
    shr eax, 1
    RET


INIT_YMM avx2
cglobal pixel_sa8d_16x16, 4,8,12
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    lea  r6, [r0+4*r1]
    lea  r7, [r2+4*r3]
    vbroadcasti128 m7, [pw_1]

    ; Top 16x8
    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]                                   ; 10 bits
    movu m5, [r2]
    psubw m0, m5                                    ; 11 bits
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax    ; 16 bits
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m10, m0, m2

    lea  r0, [r0+8*r1]
    lea  r2, [r2+8*r3]
    lea  r6, [r6+8*r1]
    lea  r7, [r7+8*r3]

    ; Bottom 16x8
    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m10, m0
    paddd m10, m2

    HADDD m10, m0

    movd eax, xm10
    add  eax, 1
    shr  eax, 1
    RET


; TODO: optimize me, need more 2 of YMM registers because C model get partial result every 16x16 block
INIT_YMM avx2
cglobal pixel_sa8d_32x32, 4,8,14
    FIX_STRIDES r1, r3
    lea  r4, [3*r1]
    lea  r5, [3*r3]
    lea  r6, [r0+4*r1]
    lea  r7, [r2+4*r3]
    vbroadcasti128 m7, [pw_1]


    ;SA8D[16x8] ; pix[0]
    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m10, m0, m2


    ; SA8D[16x8] ; pix[16]
    add  r0, mmsize
    add  r2, mmsize
    add  r6, mmsize
    add  r7, mmsize

    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m12, m0, m2


    ; SA8D[16x8] ; pix[8*stride+16]
    lea  r0, [r0+8*r1]
    lea  r2, [r2+8*r3]
    lea  r6, [r6+8*r1]
    lea  r7, [r7+8*r3]

    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m12, m0
    paddd m12, m2

    ; sum[1]
    HADDD m12, m0


    ; SA8D[16x8] ; pix[8*stride]
    sub  r0, mmsize
    sub  r2, mmsize
    sub  r6, mmsize
    sub  r7, mmsize

    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m10, m0
    paddd m10, m2

    ; sum[0]
    HADDD m10, m0
    punpckldq xm10, xm12


    ;SA8D[16x8] ; pix[16*stridr]
    lea  r0, [r0+8*r1]
    lea  r2, [r2+8*r3]
    lea  r6, [r6+8*r1]
    lea  r7, [r7+8*r3]

    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m12, m0, m2


    ; SA8D[16x8] ; pix[16*stride+16]
    add  r0, mmsize
    add  r2, mmsize
    add  r6, mmsize
    add  r7, mmsize

    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m13, m0, m2


    ; SA8D[16x8] ; pix[24*stride+16]
    lea  r0, [r0+8*r1]
    lea  r2, [r2+8*r3]
    lea  r6, [r6+8*r1]
    lea  r7, [r7+8*r3]

    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m13, m0
    paddd m13, m2

    ; sum[3]
    HADDD m13, m0


    ; SA8D[16x8] ; pix[24*stride]
    sub  r0, mmsize
    sub  r2, mmsize
    sub  r6, mmsize
    sub  r7, mmsize

    ;LOAD_DIFF_8x4P_AVX2 0, 1, 2, 8, 5, 6, r0, r2
    movu m0, [r0]
    movu m5, [r2]
    psubw m0, m5
    movu m1, [r0 + r1]
    movu m6, [r2 + r3]
    psubw m1, m6
    movu m2, [r0 + r1 * 2]
    movu m5, [r2 + r3 * 2]
    psubw m2, m5
    movu m8, [r0 + r4]
    movu m6, [r2 + r5]
    psubw m8, m6

    ;LOAD_DIFF_8x4P_AVX2 4, 5, 3, 9, 11, 6, r6, r7
    movu m4, [r6]
    movu m11, [r7]
    psubw m4, m11
    movu m5, [r6 + r1]
    movu m6, [r7 + r3]
    psubw m5, m6
    movu m3, [r6 + r1 * 2]
    movu m11, [r7 + r3 * 2]
    psubw m3, m11
    movu m9, [r6 + r4]
    movu m6, [r7 + r5]
    psubw m9, m6

    HADAMARD8_2D 0, 1, 2, 8, 4, 5, 3, 9, 6, amax
    pmaddwd m0, m7
    pmaddwd m1, m7
    pmaddwd m2, m7
    pmaddwd m8, m7
    paddd m0, m1
    paddd m2, m8
    paddd m12, m0
    paddd m12, m2

    ; sum[2]
    HADDD m12, m0
    punpckldq xm12, xm13

    ; SA8D
    punpcklqdq xm0, xm10, xm12
    paddd xm0, [pd_1]
    psrld xm0, 1
    HADDD xm0, xm1

    movd eax, xm0
    RET

%endif ; HIGH_BIT_DEPTH == 1 && BIT_DEPTH == 10
