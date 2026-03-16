;*****************************************************************************
;* mc-a2.asm: x86 motion compensation
;*****************************************************************************
;* Copyright (C) 2003-2013 x264 project
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Loren Merritt <lorenm@u.washington.edu>
;*          Fiona Glaser <fiona@x264.com>
;*          Holger Lubitz <holger@lubitz.org>
;*          Mathieu Monnier <manao@melix.net>
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

deinterleave_shuf: times 2 db 0,2,4,6,8,10,12,14,1,3,5,7,9,11,13,15

%if HIGH_BIT_DEPTH
deinterleave_shuf32a: SHUFFLE_MASK_W 0,2,4,6,8,10,12,14
deinterleave_shuf32b: SHUFFLE_MASK_W 1,3,5,7,9,11,13,15
%else
deinterleave_shuf32a: db 0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30
deinterleave_shuf32b: db 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31
%endif

cutree_fix8_unpack_shuf: db -1,-1, 0, 1,-1,-1, 2, 3,-1,-1, 4, 5,-1,-1, 6, 7
                         db -1,-1, 8, 9,-1,-1,10,11,-1,-1,12,13,-1,-1,14,15

const pq_256,       times 4 dq 256.0
const pd_inv256,    times 4 dq 0.00390625
const pd_0_5,       times 4 dq 0.5

SECTION .text

cextern pb_0
cextern pw_1
cextern pw_16
cextern pw_32
cextern pw_512
cextern pw_00ff
cextern pw_1024
cextern pw_3fff
cextern pw_pixel_max
cextern pd_ffff
cextern pd_16

;The hpel_filter routines use non-temporal writes for output.
;The following defines may be uncommented for testing.
;Doing the hpel_filter temporal may be a win if the last level cache
;is big enough (preliminary benching suggests on the order of 4* framesize).

;%define movntq movq
;%define movntps movaps
;%define sfence

%if HIGH_BIT_DEPTH == 0
%undef movntq
%undef movntps
%undef sfence
%endif ; !HIGH_BIT_DEPTH

;-----------------------------------------------------------------------------
; void plane_copy_core( pixel *dst, intptr_t i_dst,
;                       pixel *src, intptr_t i_src, int w, int h )
;-----------------------------------------------------------------------------
; assumes i_dst and w are multiples of 16, and i_dst>w
INIT_MMX
cglobal plane_copy_core_mmx2, 6,7
    FIX_STRIDES r1, r3, r4d
%if HIGH_BIT_DEPTH == 0
    movsxdifnidn r4, r4d
%endif
    sub    r1,  r4
    sub    r3,  r4
.loopy:
    lea   r6d, [r4-63]
.loopx:
    prefetchnta [r2+256]
    movq   m0, [r2   ]
    movq   m1, [r2+ 8]
    movntq [r0   ], m0
    movntq [r0+ 8], m1
    movq   m2, [r2+16]
    movq   m3, [r2+24]
    movntq [r0+16], m2
    movntq [r0+24], m3
    movq   m4, [r2+32]
    movq   m5, [r2+40]
    movntq [r0+32], m4
    movntq [r0+40], m5
    movq   m6, [r2+48]
    movq   m7, [r2+56]
    movntq [r0+48], m6
    movntq [r0+56], m7
    add    r2,  64
    add    r0,  64
    sub    r6d, 64
    jg .loopx
    prefetchnta [r2+256]
    add    r6d, 63
    jle .end16
.loop16:
    movq   m0, [r2  ]
    movq   m1, [r2+8]
    movntq [r0  ], m0
    movntq [r0+8], m1
    add    r2,  16
    add    r0,  16
    sub    r6d, 16
    jg .loop16
.end16:
    add    r0, r1
    add    r2, r3
    dec    r5d
    jg .loopy
    sfence
    emms
    RET


%macro INTERLEAVE 4-5 ; dst, srcu, srcv, is_aligned, nt_hint
%if HIGH_BIT_DEPTH
%assign x 0
%rep 16/mmsize
    mov%4     m0, [%2+(x/2)*mmsize]
    mov%4     m1, [%3+(x/2)*mmsize]
    punpckhwd m2, m0, m1
    punpcklwd m0, m1
    mov%5a    [%1+(x+0)*mmsize], m0
    mov%5a    [%1+(x+1)*mmsize], m2
    %assign x (x+2)
%endrep
%else
    movq   m0, [%2]
%if mmsize==16
%ifidn %4, a
    punpcklbw m0, [%3]
%else
    movq   m1, [%3]
    punpcklbw m0, m1
%endif
    mov%5a [%1], m0
%else
    movq   m1, [%3]
    punpckhbw m2, m0, m1
    punpcklbw m0, m1
    mov%5a [%1+0], m0
    mov%5a [%1+8], m2
%endif
%endif ; HIGH_BIT_DEPTH
%endmacro

%macro DEINTERLEAVE 6 ; dstu, dstv, src, dstv==dstu+8, shuffle constant, is aligned
%if HIGH_BIT_DEPTH
%assign n 0
%rep 16/mmsize
    mova     m0, [%3+(n+0)*mmsize]
    mova     m1, [%3+(n+1)*mmsize]
    psrld    m2, m0, 16
    psrld    m3, m1, 16
    pand     m0, %5
    pand     m1, %5
    packssdw m0, m1
    packssdw m2, m3
    mov%6    [%1+(n/2)*mmsize], m0
    mov%6    [%2+(n/2)*mmsize], m2
    %assign n (n+2)
%endrep
%else ; !HIGH_BIT_DEPTH
%if mmsize==16
    mova   m0, [%3]
%if cpuflag(ssse3)
    pshufb m0, %5
%else
    mova   m1, m0
    pand   m0, %5
    psrlw  m1, 8
    packuswb m0, m1
%endif
%if %4
    mova   [%1], m0
%else
    movq   [%1], m0
    movhps [%2], m0
%endif
%else
    mova   m0, [%3]
    mova   m1, [%3+8]
    mova   m2, m0
    mova   m3, m1
    pand   m0, %5
    pand   m1, %5
    psrlw  m2, 8
    psrlw  m3, 8
    packuswb m0, m1
    packuswb m2, m3
    mova   [%1], m0
    mova   [%2], m2
%endif ; mmsize == 16
%endif ; HIGH_BIT_DEPTH
%endmacro

%macro PLANE_INTERLEAVE 0
;-----------------------------------------------------------------------------
; void plane_copy_interleave_core( uint8_t *dst,  intptr_t i_dst,
;                                  uint8_t *srcu, intptr_t i_srcu,
;                                  uint8_t *srcv, intptr_t i_srcv, int w, int h )
;-----------------------------------------------------------------------------
; assumes i_dst and w are multiples of 16, and i_dst>2*w
cglobal plane_copy_interleave_core, 6,9
    mov   r6d, r6m
%if HIGH_BIT_DEPTH
    FIX_STRIDES r1, r3, r5, r6d
    movifnidn r1mp, r1
    movifnidn r3mp, r3
    mov  r6m, r6d
%endif
    lea    r0, [r0+r6*2]
    add    r2,  r6
    add    r4,  r6
%if ARCH_X86_64
    DECLARE_REG_TMP 7,8
%else
    DECLARE_REG_TMP 1,3
%endif
    mov  t1, r1
    shr  t1, SIZEOF_PIXEL
    sub  t1, r6
    mov  t0d, r7m
.loopy:
    mov    r6d, r6m
    neg    r6
.prefetch:
    prefetchnta [r2+r6]
    prefetchnta [r4+r6]
    add    r6, 64
    jl .prefetch
    mov    r6d, r6m
    neg    r6
.loopx:
    INTERLEAVE r0+r6*2+ 0*SIZEOF_PIXEL, r2+r6+0*SIZEOF_PIXEL, r4+r6+0*SIZEOF_PIXEL, u, nt
    INTERLEAVE r0+r6*2+16*SIZEOF_PIXEL, r2+r6+8*SIZEOF_PIXEL, r4+r6+8*SIZEOF_PIXEL, u, nt
    add    r6, 16*SIZEOF_PIXEL
    jl .loopx
.pad:
%assign n 0
%rep SIZEOF_PIXEL
%if mmsize==8
    movntq [r0+r6*2+(n+ 0)], m0
    movntq [r0+r6*2+(n+ 8)], m0
    movntq [r0+r6*2+(n+16)], m0
    movntq [r0+r6*2+(n+24)], m0
%else
    movntdq [r0+r6*2+(n+ 0)], m0
    movntdq [r0+r6*2+(n+16)], m0
%endif
    %assign n n+32
%endrep
    add    r6, 16*SIZEOF_PIXEL
    cmp    r6, t1
    jl .pad
    add    r0, r1mp
    add    r2, r3mp
    add    r4, r5
    dec    t0d
    jg .loopy
    sfence
    emms
    RET

;-----------------------------------------------------------------------------
; void store_interleave_chroma( uint8_t *dst, intptr_t i_dst, uint8_t *srcu, uint8_t *srcv, int height )
;-----------------------------------------------------------------------------
cglobal store_interleave_chroma, 5,5
    FIX_STRIDES r1
.loop:
    INTERLEAVE r0+ 0, r2+           0, r3+           0, a
    INTERLEAVE r0+r1, r2+FDEC_STRIDEB, r3+FDEC_STRIDEB, a
    add    r2, FDEC_STRIDEB*2
    add    r3, FDEC_STRIDEB*2
    lea    r0, [r0+r1*2]
    sub   r4d, 2
    jg .loop
    RET
%endmacro ; PLANE_INTERLEAVE

%macro DEINTERLEAVE_START 0
%if HIGH_BIT_DEPTH
    mova   m4, [pd_ffff]
%elif cpuflag(ssse3)
    mova   m4, [deinterleave_shuf]
%else
    mova   m4, [pw_00ff]
%endif ; HIGH_BIT_DEPTH
%endmacro

%macro PLANE_DEINTERLEAVE 0
;-----------------------------------------------------------------------------
; void plane_copy_deinterleave( pixel *dstu, intptr_t i_dstu,
;                               pixel *dstv, intptr_t i_dstv,
;                               pixel *src,  intptr_t i_src, int w, int h )
;-----------------------------------------------------------------------------
cglobal plane_copy_deinterleave, 6,7
    DEINTERLEAVE_START
    mov    r6d, r6m
    FIX_STRIDES r1, r3, r5, r6d
%if HIGH_BIT_DEPTH
    mov    r6m, r6d
%endif
    add    r0,  r6
    add    r2,  r6
    lea    r4, [r4+r6*2]
.loopy:
    mov    r6d, r6m
    neg    r6
.loopx:
    DEINTERLEAVE r0+r6+0*SIZEOF_PIXEL, r2+r6+0*SIZEOF_PIXEL, r4+r6*2+ 0*SIZEOF_PIXEL, 0, m4, u
    DEINTERLEAVE r0+r6+8*SIZEOF_PIXEL, r2+r6+8*SIZEOF_PIXEL, r4+r6*2+16*SIZEOF_PIXEL, 0, m4, u
    add    r6, 16*SIZEOF_PIXEL
    jl .loopx
    add    r0, r1
    add    r2, r3
    add    r4, r5
    dec dword r7m
    jg .loopy
    RET

;-----------------------------------------------------------------------------
; void load_deinterleave_chroma_fenc( pixel *dst, pixel *src, intptr_t i_src, int height )
;-----------------------------------------------------------------------------
cglobal load_deinterleave_chroma_fenc, 4,4
    DEINTERLEAVE_START
    FIX_STRIDES r2
.loop:
    DEINTERLEAVE r0+           0, r0+FENC_STRIDEB*1/2, r1+ 0, 1, m4, a
    DEINTERLEAVE r0+FENC_STRIDEB, r0+FENC_STRIDEB*3/2, r1+r2, 1, m4, a
    add    r0, FENC_STRIDEB*2
    lea    r1, [r1+r2*2]
    sub   r3d, 2
    jg .loop
    RET

;-----------------------------------------------------------------------------
; void load_deinterleave_chroma_fdec( pixel *dst, pixel *src, intptr_t i_src, int height )
;-----------------------------------------------------------------------------
cglobal load_deinterleave_chroma_fdec, 4,4
    DEINTERLEAVE_START
    FIX_STRIDES r2
.loop:
    DEINTERLEAVE r0+           0, r0+FDEC_STRIDEB*1/2, r1+ 0, 0, m4, a
    DEINTERLEAVE r0+FDEC_STRIDEB, r0+FDEC_STRIDEB*3/2, r1+r2, 0, m4, a
    add    r0, FDEC_STRIDEB*2
    lea    r1, [r1+r2*2]
    sub   r3d, 2
    jg .loop
    RET
%endmacro ; PLANE_DEINTERLEAVE

%if HIGH_BIT_DEPTH
INIT_MMX mmx2
PLANE_INTERLEAVE
INIT_MMX mmx
PLANE_DEINTERLEAVE
INIT_XMM sse2
PLANE_INTERLEAVE
PLANE_DEINTERLEAVE
INIT_XMM avx
PLANE_INTERLEAVE
PLANE_DEINTERLEAVE
%else
INIT_MMX mmx2
PLANE_INTERLEAVE
INIT_MMX mmx
PLANE_DEINTERLEAVE
INIT_XMM sse2
PLANE_INTERLEAVE
PLANE_DEINTERLEAVE
INIT_XMM ssse3
PLANE_DEINTERLEAVE
%endif

; These functions are not general-use; not only do the SSE ones require aligned input,
; but they also will fail if given a non-mod16 size.
; memzero SSE will fail for non-mod128.

;-----------------------------------------------------------------------------
; void *memcpy_aligned( void *dst, const void *src, size_t n );
;-----------------------------------------------------------------------------
%macro MEMCPY 0
cglobal memcpy_aligned, 3,3
%if mmsize == 16
    test r2d, 16
    jz .copy2
    mova  m0, [r1+r2-16]
    mova [r0+r2-16], m0
    sub  r2d, 16
.copy2:
%endif
    test r2d, 2*mmsize
    jz .copy4start
    mova  m0, [r1+r2-1*mmsize]
    mova  m1, [r1+r2-2*mmsize]
    mova [r0+r2-1*mmsize], m0
    mova [r0+r2-2*mmsize], m1
    sub  r2d, 2*mmsize
.copy4start:
    test r2d, r2d
    jz .ret
.copy4:
    mova  m0, [r1+r2-1*mmsize]
    mova  m1, [r1+r2-2*mmsize]
    mova  m2, [r1+r2-3*mmsize]
    mova  m3, [r1+r2-4*mmsize]
    mova [r0+r2-1*mmsize], m0
    mova [r0+r2-2*mmsize], m1
    mova [r0+r2-3*mmsize], m2
    mova [r0+r2-4*mmsize], m3
    sub  r2d, 4*mmsize
    jg .copy4
.ret:
    REP_RET
%endmacro

INIT_MMX mmx
MEMCPY
INIT_XMM sse
MEMCPY

;-----------------------------------------------------------------------------
; void *memzero_aligned( void *dst, size_t n );
;-----------------------------------------------------------------------------
%macro MEMZERO 1
cglobal memzero_aligned, 2,2
    add  r0, r1
    neg  r1
%if mmsize == 8
    pxor m0, m0
%else
    xorps m0, m0
%endif
.loop:
%assign i 0
%rep %1
    mova [r0 + r1 + i], m0
%assign i i+mmsize
%endrep
    add r1, mmsize*%1
    jl .loop
    RET
%endmacro

INIT_MMX mmx
MEMZERO 8
INIT_XMM sse
MEMZERO 8
INIT_YMM avx
MEMZERO 4

%if HIGH_BIT_DEPTH == 0
;-----------------------------------------------------------------------------
; void integral_init4h( uint16_t *sum, uint8_t *pix, intptr_t stride )
;-----------------------------------------------------------------------------
%macro INTEGRAL_INIT4H 0
cglobal integral_init4h, 3,4
    lea     r3, [r0+r2*2]
    add     r1, r2
    neg     r2
    pxor    m4, m4
.loop:
    mova    m0, [r1+r2]
%if mmsize==32
    movu    m1, [r1+r2+8]
%else
    mova    m1, [r1+r2+16]
    palignr m1, m0, 8
%endif
    mpsadbw m0, m4, 0
    mpsadbw m1, m4, 0
    paddw   m0, [r0+r2*2]
    paddw   m1, [r0+r2*2+mmsize]
    mova  [r3+r2*2   ], m0
    mova  [r3+r2*2+mmsize], m1
    add     r2, mmsize
    jl .loop
    RET
%endmacro

INIT_XMM sse4
INTEGRAL_INIT4H
INIT_YMM avx2
INTEGRAL_INIT4H

%macro INTEGRAL_INIT8H 0
cglobal integral_init8h, 3,4
    lea     r3, [r0+r2*2]
    add     r1, r2
    neg     r2
    pxor    m4, m4
.loop:
    mova    m0, [r1+r2]
%if mmsize==32
    movu    m1, [r1+r2+8]
    mpsadbw m2, m0, m4, 100100b
    mpsadbw m3, m1, m4, 100100b
%else
    mova    m1, [r1+r2+16]
    palignr m1, m0, 8
    mpsadbw m2, m0, m4, 100b
    mpsadbw m3, m1, m4, 100b
%endif
    mpsadbw m0, m4, 0
    mpsadbw m1, m4, 0
    paddw   m0, [r0+r2*2]
    paddw   m1, [r0+r2*2+mmsize]
    paddw   m0, m2
    paddw   m1, m3
    mova  [r3+r2*2   ], m0
    mova  [r3+r2*2+mmsize], m1
    add     r2, mmsize
    jl .loop
    RET
%endmacro

INIT_XMM sse4
INTEGRAL_INIT8H
INIT_XMM avx
INTEGRAL_INIT8H
INIT_YMM avx2
INTEGRAL_INIT8H
%endif ; !HIGH_BIT_DEPTH

%macro INTEGRAL_INIT_8V 0
;-----------------------------------------------------------------------------
; void integral_init8v( uint16_t *sum8, intptr_t stride )
;-----------------------------------------------------------------------------
cglobal integral_init8v, 3,3
    add   r1, r1
    add   r0, r1
    lea   r2, [r0+r1*8]
    neg   r1
.loop:
    mova  m0, [r2+r1]
    mova  m1, [r2+r1+mmsize]
    psubw m0, [r0+r1]
    psubw m1, [r0+r1+mmsize]
    mova  [r0+r1], m0
    mova  [r0+r1+mmsize], m1
    add   r1, 2*mmsize
    jl .loop
    RET
%endmacro

INIT_MMX mmx
INTEGRAL_INIT_8V
INIT_XMM sse2
INTEGRAL_INIT_8V
INIT_YMM avx2
INTEGRAL_INIT_8V

;-----------------------------------------------------------------------------
; void integral_init4v( uint16_t *sum8, uint16_t *sum4, intptr_t stride )
;-----------------------------------------------------------------------------
INIT_MMX mmx
cglobal integral_init4v, 3,5
    shl   r2, 1
    lea   r3, [r0+r2*4]
    lea   r4, [r0+r2*8]
    mova  m0, [r0+r2]
    mova  m4, [r4+r2]
.loop:
    mova  m1, m4
    psubw m1, m0
    mova  m4, [r4+r2-8]
    mova  m0, [r0+r2-8]
    paddw m1, m4
    mova  m3, [r3+r2-8]
    psubw m1, m0
    psubw m3, m0
    mova  [r0+r2-8], m1
    mova  [r1+r2-8], m3
    sub   r2, 8
    jge .loop
    RET

INIT_XMM sse2
cglobal integral_init4v, 3,5
    shl     r2, 1
    add     r0, r2
    add     r1, r2
    lea     r3, [r0+r2*4]
    lea     r4, [r0+r2*8]
    neg     r2
.loop:
    mova    m0, [r0+r2]
    mova    m1, [r4+r2]
    mova    m2, m0
    mova    m4, m1
    shufpd  m0, [r0+r2+16], 1
    shufpd  m1, [r4+r2+16], 1
    paddw   m0, m2
    paddw   m1, m4
    mova    m3, [r3+r2]
    psubw   m1, m0
    psubw   m3, m2
    mova  [r0+r2], m1
    mova  [r1+r2], m3
    add     r2, 16
    jl .loop
    RET

INIT_XMM ssse3
cglobal integral_init4v, 3,5
    shl     r2, 1
    add     r0, r2
    add     r1, r2
    lea     r3, [r0+r2*4]
    lea     r4, [r0+r2*8]
    neg     r2
.loop:
    mova    m2, [r0+r2]
    mova    m0, [r0+r2+16]
    mova    m4, [r4+r2]
    mova    m1, [r4+r2+16]
    palignr m0, m2, 8
    palignr m1, m4, 8
    paddw   m0, m2
    paddw   m1, m4
    mova    m3, [r3+r2]
    psubw   m1, m0
    psubw   m3, m2
    mova  [r0+r2], m1
    mova  [r1+r2], m3
    add     r2, 16
    jl .loop
    RET

INIT_YMM avx2
cglobal integral_init4v, 3,5
    add     r2, r2
    add     r0, r2
    add     r1, r2
    lea     r3, [r0+r2*4]
    lea     r4, [r0+r2*8]
    neg     r2
.loop:
    mova    m2, [r0+r2]
    movu    m1, [r4+r2+8]
    paddw   m0, m2, [r0+r2+8]
    paddw   m1, [r4+r2]
    mova    m3, [r3+r2]
    psubw   m1, m0
    psubw   m3, m2
    mova  [r0+r2], m1
    mova  [r1+r2], m3
    add     r2, 32
    jl .loop
    RET

%macro FILT8x4 7
    mova      %3, [r0+%7]
    mova      %4, [r0+r5+%7]
    pavgb     %3, %4
    pavgb     %4, [r0+r5*2+%7]
    PALIGNR   %1, %3, 1, m6
    PALIGNR   %2, %4, 1, m6
%if cpuflag(xop)
    pavgb     %1, %3
    pavgb     %2, %4
%else
    pavgb     %1, %3
    pavgb     %2, %4
    psrlw     %5, %1, 8
    psrlw     %6, %2, 8
    pand      %1, m7
    pand      %2, m7
%endif
%endmacro

%macro FILT32x4U 4
    movu      m1, [r0+r5]
    pavgb     m0, m1, [r0]
    movu      m3, [r0+r5+1]
    pavgb     m2, m3, [r0+1]
    pavgb     m1, [r0+r5*2]
    pavgb     m3, [r0+r5*2+1]
    pavgb     m0, m2
    pavgb     m1, m3

    movu      m3, [r0+r5+mmsize]
    pavgb     m2, m3, [r0+mmsize]
    movu      m5, [r0+r5+1+mmsize]
    pavgb     m4, m5, [r0+1+mmsize]
    pavgb     m3, [r0+r5*2+mmsize]
    pavgb     m5, [r0+r5*2+1+mmsize]
    pavgb     m2, m4
    pavgb     m3, m5

    pshufb    m0, m7
    pshufb    m1, m7
    pshufb    m2, m7
    pshufb    m3, m7
    punpckhqdq m4, m0, m2
    punpcklqdq m0, m0, m2
    punpckhqdq m5, m1, m3
    punpcklqdq m2, m1, m3
    vpermq    m0, m0, q3120
    vpermq    m1, m4, q3120
    vpermq    m2, m2, q3120
    vpermq    m3, m5, q3120
    movu    [%1], m0
    movu    [%2], m1
    movu    [%3], m2
    movu    [%4], m3
%endmacro

%macro FILT16x2 4
    mova      m3, [r0+%4+mmsize]
    mova      m2, [r0+%4]
    pavgb     m3, [r0+%4+r5+mmsize]
    pavgb     m2, [r0+%4+r5]
    PALIGNR   %1, m3, 1, m6
    pavgb     %1, m3
    PALIGNR   m3, m2, 1, m6
    pavgb     m3, m2
%if cpuflag(xop)
    vpperm    m5, m3, %1, m7
    vpperm    m3, m3, %1, m6
%else
    psrlw     m5, m3, 8
    psrlw     m4, %1, 8
    pand      m3, m7
    pand      %1, m7
    packuswb  m3, %1
    packuswb  m5, m4
%endif
    mova    [%2], m3
    mova    [%3], m5
    mova      %1, m2
%endmacro

%macro FILT8x2U 3
    mova      m3, [r0+%3+8]
    mova      m2, [r0+%3]
    pavgb     m3, [r0+%3+r5+8]
    pavgb     m2, [r0+%3+r5]
    mova      m1, [r0+%3+9]
    mova      m0, [r0+%3+1]
    pavgb     m1, [r0+%3+r5+9]
    pavgb     m0, [r0+%3+r5+1]
    pavgb     m1, m3
    pavgb     m0, m2
    psrlw     m3, m1, 8
    psrlw     m2, m0, 8
    pand      m1, m7
    pand      m0, m7
    packuswb  m0, m1
    packuswb  m2, m3
    mova    [%1], m0
    mova    [%2], m2
%endmacro

%macro FILT8xU 3
    mova      m3, [r0+%3+8]
    mova      m2, [r0+%3]
    pavgw     m3, [r0+%3+r5+8]
    pavgw     m2, [r0+%3+r5]
    movu      m1, [r0+%3+10]
    movu      m0, [r0+%3+2]
    pavgw     m1, [r0+%3+r5+10]
    pavgw     m0, [r0+%3+r5+2]
    pavgw     m1, m3
    pavgw     m0, m2
    psrld     m3, m1, 16
    psrld     m2, m0, 16
    pand      m1, m7
    pand      m0, m7
    packssdw  m0, m1
    packssdw  m2, m3
    movu    [%1], m0
    mova    [%2], m2
%endmacro

%macro FILT8xA 4
    movu      m3, [r0+%4+mmsize]
    movu      m2, [r0+%4]
    pavgw     m3, [r0+%4+r5+mmsize]
    pavgw     m2, [r0+%4+r5]
    PALIGNR   %1, m3, 2, m6
    pavgw     %1, m3
    PALIGNR   m3, m2, 2, m6
    pavgw     m3, m2
%if cpuflag(xop)
    vpperm    m5, m3, %1, m7
    vpperm    m3, m3, %1, m6
%else
    psrld     m5, m3, 16
    psrld     m4, %1, 16
    pand      m3, m7
    pand      %1, m7
    packssdw  m3, %1
    packssdw  m5, m4
%endif
%if cpuflag(avx2)
    vpermq     m3, m3, q3120
    vpermq     m5, m5, q3120
%endif
    movu    [%2], m3
    movu    [%3], m5
    movu      %1, m2
%endmacro

;-----------------------------------------------------------------------------
; void frame_init_lowres_core( uint8_t *src0, uint8_t *dst0, uint8_t *dsth, uint8_t *dstv, uint8_t *dstc,
;                              intptr_t src_stride, intptr_t dst_stride, int width, int height )
;-----------------------------------------------------------------------------
%macro FRAME_INIT_LOWRES 0
cglobal frame_init_lowres_core, 6,7,(12-4*(BIT_DEPTH/9)) ; 8 for HIGH_BIT_DEPTH, 12 otherwise
%if HIGH_BIT_DEPTH
    shl   dword r6m, 1
    FIX_STRIDES r5
    shl   dword r7m, 1
%endif
%if mmsize >= 16
    add   dword r7m, mmsize-1
    and   dword r7m, ~(mmsize-1)
%endif
    ; src += 2*(height-1)*stride + 2*width
    mov      r6d, r8m
    dec      r6d
    imul     r6d, r5d
    add      r6d, r7m
    lea       r0, [r0+r6*2]
    ; dst += (height-1)*stride + width
    mov      r6d, r8m
    dec      r6d
    imul     r6d, r6m
    add      r6d, r7m
    add       r1, r6
    add       r2, r6
    add       r3, r6
    add       r4, r6
    ; gap = stride - width
    mov      r6d, r6m
    sub      r6d, r7m
    PUSH      r6
    %define dst_gap [rsp+gprsize]
    mov      r6d, r5d
    sub      r6d, r7m
    shl      r6d, 1
    PUSH      r6
    %define src_gap [rsp]
%if HIGH_BIT_DEPTH
%if cpuflag(xop)
    mova      m6, [deinterleave_shuf32a]
    mova      m7, [deinterleave_shuf32b]
%else
    pcmpeqw   m7, m7
    psrld     m7, 16
%endif
.vloop:
    mov      r6d, r7m
%ifnidn cpuname, mmx2
    movu      m0, [r0]
    movu      m1, [r0+r5]
    pavgw     m0, m1
    pavgw     m1, [r0+r5*2]
%endif
.hloop:
    sub       r0, mmsize*2
    sub       r1, mmsize
    sub       r2, mmsize
    sub       r3, mmsize
    sub       r4, mmsize
%ifidn cpuname, mmx2
    FILT8xU r1, r2, 0
    FILT8xU r3, r4, r5
%else
    FILT8xA m0, r1, r2, 0
    FILT8xA m1, r3, r4, r5
%endif
    sub      r6d, mmsize
    jg .hloop
%else ; !HIGH_BIT_DEPTH
%if cpuflag(avx2)
    mova      m7, [deinterleave_shuf]
%elif cpuflag(xop)
    mova      m6, [deinterleave_shuf32a]
    mova      m7, [deinterleave_shuf32b]
%else
    pcmpeqb   m7, m7
    psrlw     m7, 8
%endif
.vloop:
    mov      r6d, r7m
%ifnidn cpuname, mmx2
%if mmsize <= 16
    mova      m0, [r0]
    mova      m1, [r0+r5]
    pavgb     m0, m1
    pavgb     m1, [r0+r5*2]
%endif
%endif
.hloop:
    sub       r0, mmsize*2
    sub       r1, mmsize
    sub       r2, mmsize
    sub       r3, mmsize
    sub       r4, mmsize
%if mmsize==32
    FILT32x4U r1, r2, r3, r4
%elifdef m8
    FILT8x4   m0, m1, m2, m3, m10, m11, mmsize
    mova      m8, m0
    mova      m9, m1
    FILT8x4   m2, m3, m0, m1, m4, m5, 0
%if cpuflag(xop)
    vpperm    m4, m2, m8, m7
    vpperm    m2, m2, m8, m6
    vpperm    m5, m3, m9, m7
    vpperm    m3, m3, m9, m6
%else
    packuswb  m2, m8
    packuswb  m3, m9
    packuswb  m4, m10
    packuswb  m5, m11
%endif
    mova    [r1], m2
    mova    [r2], m4
    mova    [r3], m3
    mova    [r4], m5
%elifidn cpuname, mmx2
    FILT8x2U  r1, r2, 0
    FILT8x2U  r3, r4, r5
%else
    FILT16x2  m0, r1, r2, 0
    FILT16x2  m1, r3, r4, r5
%endif
    sub      r6d, mmsize
    jg .hloop
%endif ; HIGH_BIT_DEPTH
.skip:
    mov       r6, dst_gap
    sub       r0, src_gap
    sub       r1, r6
    sub       r2, r6
    sub       r3, r6
    sub       r4, r6
    dec    dword r8m
    jg .vloop
    ADD      rsp, 2*gprsize
    emms
    RET
%endmacro ; FRAME_INIT_LOWRES

INIT_MMX mmx2
FRAME_INIT_LOWRES
%if ARCH_X86_64 == 0
INIT_MMX cache32, mmx2
FRAME_INIT_LOWRES
%endif
INIT_XMM sse2
FRAME_INIT_LOWRES
INIT_XMM ssse3
FRAME_INIT_LOWRES
INIT_XMM avx
FRAME_INIT_LOWRES
INIT_XMM xop
FRAME_INIT_LOWRES
%if ARCH_X86_64 == 1
INIT_YMM avx2
FRAME_INIT_LOWRES
%endif

;-----------------------------------------------------------------------------
; void mbtree_propagate_cost( int *dst, uint16_t *propagate_in, int32_t *intra_costs,
;                             uint16_t *inter_costs, int32_t *inv_qscales, double *fps_factor, int len )
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal mbtree_propagate_cost, 7,7,7
    dec         r6d
    movsd       m6, [r5]
    mulpd       m6, [pd_inv256]
    xor         r5d, r5d
    lea         r0, [r0+r5*2]
    pxor        m4, m4
    movlhps     m6, m6
    mova        m5, [pw_3fff]

.loop:
    movh        m2, [r2+r5*4]       ; intra
    movh        m0, [r4+r5*4]       ; invq
    movd        m3, [r3+r5*2]       ; inter
    pand        m3, m5
    punpcklwd   m3, m4

    ; PMINSD
    pcmpgtd     m1, m2, m3
    pand        m3, m1
    pandn       m1, m2
    por         m3, m1

    movd        m1, [r1+r5*2]       ; prop
    punpckldq   m2, m2
    punpckldq   m0, m0
    pmuludq     m0, m2
    pshufd      m2, m2, q3120
    pshufd      m0, m0, q3120

    punpcklwd   m1, m4
    cvtdq2pd    m0, m0
    mulpd       m0, m6              ; intra*invq*fps_factor>>8
    cvtdq2pd    m1, m1              ; prop
    addpd       m0, m1              ; prop + (intra*invq*fps_factor>>8)
    ;cvtdq2ps    m1, m2              ; intra
    cvtdq2pd    m1, m2              ; intra
    psubd       m2, m3              ; intra - inter
    cvtdq2pd    m2, m2              ; intra - inter
    ;rcpps       m3, m1
    ;mulps       m1, m3              ; intra * (1/intra 1st approx)
    ;mulps       m1, m3              ; intra * (1/intra 1st approx)^2
    ;addps       m3, m3              ; 2 * (1/intra 1st approx)
    ;subps       m3, m1              ; 2nd approximation for 1/intra
    ;cvtps2pd    m3, m3              ; 1 / intra 1st approximation
    mulpd       m0, m2              ; (prop + (intra*invq*fps_factor>>8)) * (intra - inter)
    ;mulpd       m0, m3              ; / intra

    ; TODO: DIVPD very slow, but match to C model output, since it is not bottleneck function, I comment above faster code
    divpd       m0, m1
    addpd       m0, [pd_0_5]
    cvttpd2dq    m0, m0

    movh        [r0+r5*4], m0
    add         r5d, 2
    cmp         r5d, r6d
    jl         .loop

    xor         r6d, r5d
    jnz         .even
    movd        m2, [r2+r5*4]       ; intra
    movd        m0, [r4+r5*4]       ; invq
    movd        m3, [r3+r5*2]       ; inter
    pand        m3, m5
    punpcklwd   m3, m4

    ; PMINSD
    pcmpgtd     m1, m2, m3
    pand        m3, m1
    pandn       m1, m2
    por         m3, m1

    movd        m1, [r1+r5*2]       ; prop
    punpckldq   m2, m2              ; DWORD [_ 1 _ 0]
    punpckldq   m0, m0
    pmuludq     m0, m2              ; QWORD [m1 m0]
    pshufd      m2, m2, q3120
    pshufd      m0, m0, q3120
    punpcklwd   m1, m4
    cvtdq2pd    m0, m0
    mulpd       m0, m6              ; intra*invq*fps_factor>>8
    cvtdq2pd    m1, m1              ; prop
    addpd       m0, m1              ; prop + (intra*invq*fps_factor>>8)
    cvtdq2pd    m1, m2              ; intra
    psubd       m2, m3              ; intra - inter
    cvtdq2pd    m2, m2              ; intra - inter
    mulpd       m0, m2              ; (prop + (intra*invq*fps_factor>>8)) * (intra - inter)

    divpd       m0, m1
    addpd       m0, [pd_0_5]
    cvttpd2dq    m0, m0
    movd        [r0+r5*4], m0
.even:
    RET


;-----------------------------------------------------------------------------
; void mbtree_propagate_cost( int *dst, uint16_t *propagate_in, int32_t *intra_costs,
;                             uint16_t *inter_costs, int32_t *inv_qscales, double *fps_factor, int len )
;-----------------------------------------------------------------------------
; FIXME: align loads/stores to 16 bytes
%macro MBTREE_AVX 0
cglobal mbtree_propagate_cost, 7,7,7
    sub             r6d, 3
    vbroadcastsd    m6, [r5]
    mulpd           m6, [pd_inv256]
    xor             r5d, r5d
    mova            m5, [pw_3fff]

.loop:
    movu            xm2, [r2+r5*4]      ; intra
    movu            xm0, [r4+r5*4]      ; invq
    pmovzxwd        xm3, [r3+r5*2]      ; inter
    pand            xm3, xm5
    pminsd          xm3, xm2

    pmovzxwd        xm1, [r1+r5*2]      ; prop
    pmulld          xm0, xm2
    cvtdq2pd        m0, xm0
    cvtdq2pd        m1, xm1             ; prop
;%if cpuflag(avx2)
;    fmaddpd         m0, m0, m6, m1
;%else
    mulpd           m0, m6              ; intra*invq*fps_factor>>8
    addpd           m0, m1              ; prop + (intra*invq*fps_factor>>8)
;%endif
    cvtdq2pd        m1, xm2             ; intra
    psubd           xm2, xm3            ; intra - inter
    cvtdq2pd        m2, xm2             ; intra - inter
    mulpd           m0, m2              ; (prop + (intra*invq*fps_factor>>8)) * (intra - inter)

    ; TODO: DIVPD very slow, but match to C model output, since it is not bottleneck function, I comment above faster code
    divpd           m0, m1
    addpd           m0, [pd_0_5]
    cvttpd2dq       xm0, m0

    movu            [r0+r5*4], xm0
    add             r5d, 4              ; process 4 values in one iteration
    cmp             r5d, r6d
    jl             .loop

    add             r6d, 3
    xor             r6d, r5d
    jz              .even               ; if loop counter is multiple of 4, all values are processed

    and             r6d, 3              ; otherwise, remaining unprocessed values must be 1, 2 or 3
    cmp             r6d, 1
    je              .process1           ; if only 1 value is unprocessed

    ; process 2 values here
    movq            xm2, [r2+r5*4]      ; intra
    movq            xm0, [r4+r5*4]      ; invq
    movd            xm3, [r3+r5*2]      ; inter
    pmovzxwd        xm3, xm3
    pand            xm3, xm5
    pminsd          xm3, xm2

    movd            xm1, [r1+r5*2]      ; prop
    pmovzxwd        xm1, xm1
    pmulld          xm0, xm2
    cvtdq2pd        m0, xm0
    cvtdq2pd        m1, xm1             ; prop
;%if cpuflag(avx2)
;    fmaddpd         m0, m0, m6, m1
;%else
    mulpd           m0, m6              ; intra*invq*fps_factor>>8
    addpd           m0, m1              ; prop + (intra*invq*fps_factor>>8)
;%endif
    cvtdq2pd        m1, xm2             ; intra
    psubd           xm2, xm3            ; intra - inter
    cvtdq2pd        m2, xm2             ; intra - inter
    mulpd           m0, m2              ; (prop + (intra*invq*fps_factor>>8)) * (intra - inter)

    divpd           m0, m1
    addpd           m0, [pd_0_5]
    cvttpd2dq       xm0, m0
    movq            [r0+r5*4], xm0

    xor             r6d, 2
    jz              .even
    add             r5d, 2

    ; process 1 value here
.process1:
    movd            xm2, [r2+r5*4]      ; intra
    movd            xm0, [r4+r5*4]      ; invq
    movzx           r6d, word [r3+r5*2] ; inter
    movd            xm3, r6d
    pand            xm3, xm5
    pminsd          xm3, xm2

    movzx           r6d, word [r1+r5*2] ; prop
    movd            xm1, r6d
    pmulld          xm0, xm2
    cvtdq2pd        m0, xm0
    cvtdq2pd        m1, xm1             ; prop
;%if cpuflag(avx2)
;    fmaddpd         m0, m0, m6, m1
;%else
    mulpd           m0, m6              ; intra*invq*fps_factor>>8
    addpd           m0, m1              ; prop + (intra*invq*fps_factor>>8)
;%endif
    cvtdq2pd        m1, xm2             ; intra
    psubd           xm2, xm3            ; intra - inter
    cvtdq2pd        m2, xm2             ; intra - inter
    mulpd           m0, m2              ; (prop + (intra*invq*fps_factor>>8)) * (intra - inter)

    divpd           m0, m1
    addpd           m0, [pd_0_5]
    cvttpd2dq       xm0, m0
    movd            [r0+r5*4], xm0
.even:
    RET
%endmacro

INIT_YMM avx
MBTREE_AVX

INIT_YMM avx2
MBTREE_AVX


%macro CUTREE_FIX8 0
;-----------------------------------------------------------------------------
; void cutree_fix8_pack( uint16_t *dst, double *src, int count )
;-----------------------------------------------------------------------------
cglobal cutree_fix8_pack, 3, 4, 5
    movapd       m2, [pq_256]
    sub          r2d, mmsize / 2
    movsxdifnidn r2, r2d
    lea          r1, [r1 + 8 * r2]
    lea          r0, [r0 + 2 * r2]
    neg          r2
    jg .skip_loop
.loop:
    mulpd        m0, m2, [r1 + 8 * r2]
    mulpd        m1, m2, [r1 + 8 * r2 + mmsize]
    mulpd        m3, m2, [r1 + 8 * r2 + 2 * mmsize]
    mulpd        m4, m2, [r1 + 8 * r2 + 3 * mmsize]
    cvttpd2dq    xm0, m0
    cvttpd2dq    xm1, m1
    cvttpd2dq    xm3, m3
    cvttpd2dq    xm4, m4
%if mmsize == 32
    vinserti128  m0, m0, xm3, 1
    vinserti128  m1, m1, xm4, 1
    packssdw     m0, m1
%else
    punpcklqdq   m0, m1
    punpcklqdq   m3, m4
    packssdw     m0, m3
%endif
    mova         [r0 + 2 * r2], m0
    add          r2, mmsize / 2
    jle .loop
.skip_loop:
    sub          r2, mmsize / 2
    jz .end
    ; Do the remaining values in scalar in order to avoid overreading src.
.scalar:
    movq         xm0, [r1 + 8 * r2 + 4 * mmsize] 
    mulsd        xm0, xm2
    cvttsd2si    r3d, xm0
    mov          [r0 + 2 * r2 + mmsize], r3w
    inc          r2
    jl .scalar
.end:
    RET

;-----------------------------------------------------------------------------
; void cutree_fix8_unpack( double *dst, uint16_t *src, int count )
;-----------------------------------------------------------------------------
cglobal cutree_fix8_unpack, 3, 4, 7
%if mmsize != 32
    mova           m4, [cutree_fix8_unpack_shuf+16]
%endif
    movapd         m2, [pd_inv256]
    mova           m3, [cutree_fix8_unpack_shuf]
    sub            r2d, mmsize / 2
    movsxdifnidn   r2, r2d
    lea            r1, [r1 + 2 * r2]
    lea            r0, [r0 + 8 * r2]
    neg            r2
    jg .skip_loop
.loop:
%if mmsize == 32
    vbroadcasti128 m0, [r1 + 2 * r2]
    vbroadcasti128 m1, [r1 + 2 * r2 + 16]
    pshufb         m0, m3
    pshufb         m1, m3
%else
    mova           m1, [r1 + 2 * r2]
    pshufb         m0, m1, m3
    pshufb         m1, m4
%endif
    psrad          m0, 16 ; sign-extend
    psrad          m1, 16
    cvtdq2pd       m5, xm0
    cvtdq2pd       m6, xm1
%if mmsize == 32
    vpermq         m0, m0, q1032
    vpermq         m1, m1, q1032
%else
    psrldq         m0, 8
    psrldq         m1, 8
%endif
    cvtdq2pd       m0, xm0
    cvtdq2pd       m1, xm1
    mulpd          m0, m2
    mulpd          m1, m2
    mulpd          m5, m2
    mulpd          m6, m2
    movapd         [r0 + 8 * r2], m5
    movapd         [r0 + 8 * r2 + mmsize], m0
    movapd         [r0 + 8 * r2 + mmsize * 2], m6
    movapd         [r0 + 8 * r2 + mmsize * 3], m1
    add            r2, mmsize / 2
    jle .loop
.skip_loop:
    sub            r2, mmsize / 2
    jz .end
.scalar:
    movzx          r3d, word [r1 + 2 * r2 + mmsize]
    movsx          r3d, r3w
    cvtsi2sd       xm0, r3d
    mulsd          xm0, xm2
    movsd          [r0 + 8 * r2 + 4 * mmsize], xm0
    inc            r2
    jl .scalar
.end:
    RET
%endmacro

INIT_XMM ssse3
CUTREE_FIX8

INIT_YMM avx2
CUTREE_FIX8
