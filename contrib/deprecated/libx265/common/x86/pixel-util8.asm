;*****************************************************************************
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Min Chen <chenm003@163.com> <min.chen@multicorewareinc.com>
;*          Nabajit Deka <nabajit@multicorewareinc.com>
;*          Rajesh Paulraj <rajesh@multicorewareinc.com>
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

%if BIT_DEPTH == 12
ssim_c1:   times 4 dd 107321.76    ; .01*.01*4095*4095*64
ssim_c2:   times 4 dd 60851437.92  ; .03*.03*4095*4095*64*63
pf_64:     times 4 dd 64.0
pf_128:    times 4 dd 128.0
%elif BIT_DEPTH == 10
ssim_c1:   times 4 dd 6697.7856    ; .01*.01*1023*1023*64
ssim_c2:   times 4 dd 3797644.4352 ; .03*.03*1023*1023*64*63
pf_64:     times 4 dd 64.0
pf_128:    times 4 dd 128.0
%elif BIT_DEPTH == 9
ssim_c1:   times 4 dd 1671         ; .01*.01*511*511*64
ssim_c2:   times 4 dd 947556       ; .03*.03*511*511*64*63
%else ; 8-bit
ssim_c1:   times 4 dd 416          ; .01*.01*255*255*64
ssim_c2:   times 4 dd 235963       ; .03*.03*255*255*64*63
%endif

mask_ff:                times 16 db 0xff
                        times 16 db 0
deinterleave_shuf:      times  2 db 0, 2, 4, 6, 8, 10, 12, 14, 1, 3, 5, 7, 9, 11, 13, 15
interleave_shuf:        times  2 db 0, 8, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15
deinterleave_word_shuf: times  2 db 0, 1, 4, 5, 8, 9, 12, 13, 2, 3, 6, 7, 10, 11, 14, 15
hmulw_16p:              times  8 dw 1
                        times  4 dw 1, -1

SECTION .text

cextern pw_1
cextern pw_0_7
cextern pb_1
cextern pb_128
cextern pw_00ff
cextern pw_1023
cextern pw_3fff
cextern pw_2000
cextern pw_pixel_max
cextern pd_1
cextern pd_32767
cextern pd_n32768
cextern pb_2
cextern pb_4
cextern pb_8
cextern pb_15
cextern pb_16
cextern pb_32
cextern pb_64
cextern hmul_16p
cextern trans8_shuf
cextern_naked private_prefix %+ _entropyStateBits
cextern pb_movemask
cextern pw_exp2_0_15

;-----------------------------------------------------------------------------
; void getResidual(pixel *fenc, pixel *pred, int16_t *residual, intptr_t stride)
;-----------------------------------------------------------------------------
INIT_XMM sse2
%if HIGH_BIT_DEPTH
cglobal getResidual4, 4,4,4
    add      r3,    r3

    ; row 0-1
    movh         m0, [r0]
    movh         m1, [r0 + r3]
    movh         m2, [r1]
    movh         m3, [r1 + r3]
    punpcklqdq   m0, m1
    punpcklqdq   m2, m3
    psubw        m0, m2

    movh         [r2], m0
    movhps       [r2 + r3], m0
    lea          r0, [r0 + r3 * 2]
    lea          r1, [r1 + r3 * 2]
    lea          r2, [r2 + r3 * 2]

    ; row 2-3
    movh         m0, [r0]
    movh         m1, [r0 + r3]
    movh         m2, [r1]
    movh         m3, [r1 + r3]
    punpcklqdq   m0, m1
    punpcklqdq   m2, m3
    psubw        m0, m2
    movh        [r2], m0
    movhps      [r2 + r3], m0
    RET
%else
cglobal getResidual4, 4,4,5
    pxor        m0, m0

    ; row 0-1
    movd        m1, [r0]
    movd        m2, [r0 + r3]
    movd        m3, [r1]
    movd        m4, [r1 + r3]
    punpckldq   m1, m2
    punpcklbw   m1, m0
    punpckldq   m3, m4
    punpcklbw   m3, m0
    psubw       m1, m3
    movh        [r2], m1
    movhps      [r2 + r3 * 2], m1
    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 4]

    ; row 2-3
    movd        m1, [r0]
    movd        m2, [r0 + r3]
    movd        m3, [r1]
    movd        m4, [r1 + r3]
    punpckldq   m1, m2
    punpcklbw   m1, m0
    punpckldq   m3, m4
    punpcklbw   m3, m0
    psubw       m1, m3
    movh        [r2], m1
    movhps      [r2 + r3 * 2], m1
    RET
%endif


INIT_XMM sse2
%if HIGH_BIT_DEPTH
cglobal getResidual8, 4,4,4
    add      r3,    r3

%assign x 0
%rep 8/2
    ; row 0-1
    movu        m1, [r0]
    movu        m2, [r0 + r3]
    movu        m3, [r1]
    movu        m4, [r1 + r3]
    psubw       m1, m3
    psubw       m2, m4
    movu        [r2], m1
    movu        [r2 + r3], m2
%assign x x+1
%if (x != 4)
    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 2]
%endif
%endrep
    RET
%else
cglobal getResidual8, 4,4,5
    pxor        m0, m0

%assign x 0
%rep 8/2
    ; row 0-1
    movh        m1, [r0]
    movh        m2, [r0 + r3]
    movh        m3, [r1]
    movh        m4, [r1 + r3]
    punpcklbw   m1, m0
    punpcklbw   m2, m0
    punpcklbw   m3, m0
    punpcklbw   m4, m0
    psubw       m1, m3
    psubw       m2, m4
    movu        [r2], m1
    movu        [r2 + r3 * 2], m2
%assign x x+1
%if (x != 4)
    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 4]
%endif
%endrep
    RET
%endif


%if HIGH_BIT_DEPTH
INIT_XMM sse2
cglobal getResidual16, 4,5,6
    add         r3, r3
    mov         r4d, 16/4
.loop:
    ; row 0-1
    movu        m0, [r0]
    movu        m1, [r0 + 16]
    movu        m2, [r0 + r3]
    movu        m3, [r0 + r3 + 16]
    movu        m4, [r1]
    movu        m5, [r1 + 16]
    psubw       m0, m4
    psubw       m1, m5
    movu        m4, [r1 + r3]
    movu        m5, [r1 + r3 + 16]
    psubw       m2, m4
    psubw       m3, m5
    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]

    movu        [r2], m0
    movu        [r2 + 16], m1
    movu        [r2 + r3], m2
    movu        [r2 + r3 + 16], m3
    lea         r2, [r2 + r3 * 2]

    ; row 2-3
    movu        m0, [r0]
    movu        m1, [r0 + 16]
    movu        m2, [r0 + r3]
    movu        m3, [r0 + r3 + 16]
    movu        m4, [r1]
    movu        m5, [r1 + 16]
    psubw       m0, m4
    psubw       m1, m5
    movu        m4, [r1 + r3]
    movu        m5, [r1 + r3 + 16]
    psubw       m2, m4
    psubw       m3, m5

    movu        [r2], m0
    movu        [r2 + 16], m1
    movu        [r2 + r3], m2
    movu        [r2 + r3 + 16], m3

    dec         r4d

    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 2]
    jnz        .loop
    RET
%else
INIT_XMM sse4
cglobal getResidual16, 4,5,8
    mov         r4d, 16/4
    pxor        m0, m0
.loop:
    ; row 0-1
    movu        m1, [r0]
    movu        m2, [r0 + r3]
    movu        m3, [r1]
    movu        m4, [r1 + r3]
    pmovzxbw    m5, m1
    punpckhbw   m1, m0
    pmovzxbw    m6, m2
    punpckhbw   m2, m0
    pmovzxbw    m7, m3
    punpckhbw   m3, m0
    psubw       m5, m7
    psubw       m1, m3
    pmovzxbw    m7, m4
    punpckhbw   m4, m0
    psubw       m6, m7
    psubw       m2, m4

    movu        [r2], m5
    movu        [r2 + 16], m1
    movu        [r2 + r3 * 2], m6
    movu        [r2 + r3 * 2 + 16], m2

    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 4]

    ; row 2-3
    movu        m1, [r0]
    movu        m2, [r0 + r3]
    movu        m3, [r1]
    movu        m4, [r1 + r3]
    pmovzxbw    m5, m1
    punpckhbw   m1, m0
    pmovzxbw    m6, m2
    punpckhbw   m2, m0
    pmovzxbw    m7, m3
    punpckhbw   m3, m0
    psubw       m5, m7
    psubw       m1, m3
    pmovzxbw    m7, m4
    punpckhbw   m4, m0
    psubw       m6, m7
    psubw       m2, m4

    movu        [r2], m5
    movu        [r2 + 16], m1
    movu        [r2 + r3 * 2], m6
    movu        [r2 + r3 * 2 + 16], m2

    dec         r4d

    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 4]
    jnz        .loop
    RET
%endif

%if HIGH_BIT_DEPTH
INIT_YMM avx2
cglobal getResidual16, 4,4,5
    add         r3, r3
    pxor        m0, m0

%assign x 0
%rep 16/2
    movu        m1, [r0]
    movu        m2, [r0 + r3]
    movu        m3, [r1]
    movu        m4, [r1 + r3]

    psubw       m1, m3
    psubw       m2, m4
    movu        [r2], m1
    movu        [r2 + r3], m2
%assign x x+1
%if (x != 8)
    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 2]
%endif
%endrep
    RET
%else
INIT_YMM avx2
cglobal getResidual16, 4,5,8
    lea         r4, [r3 * 2]
    add         r4d, r3d
%assign x 0
%rep 4
    pmovzxbw    m0, [r0]
    pmovzxbw    m1, [r0 + r3]
    pmovzxbw    m2, [r0 + r3 * 2]
    pmovzxbw    m3, [r0 + r4]
    pmovzxbw    m4, [r1]
    pmovzxbw    m5, [r1 + r3]
    pmovzxbw    m6, [r1 + r3 * 2]
    pmovzxbw    m7, [r1 + r4]
    psubw       m0, m4
    psubw       m1, m5
    psubw       m2, m6
    psubw       m3, m7
    movu        [r2], m0
    movu        [r2 + r3 * 2], m1
    movu        [r2 + r3 * 2 * 2], m2
    movu        [r2 + r4 * 2], m3
%assign x x+1
%if (x != 4)
    lea         r0, [r0 + r3 * 2 * 2]
    lea         r1, [r1 + r3 * 2 * 2]
    lea         r2, [r2 + r3 * 4 * 2]
%endif
%endrep
    RET
%endif

%if HIGH_BIT_DEPTH
INIT_XMM sse2
cglobal getResidual32, 4,5,6
    add         r3, r3
    mov         r4d, 32/2
.loop:
    ; row 0
    movu        m0, [r0]
    movu        m1, [r0 + 16]
    movu        m2, [r0 + 32]
    movu        m3, [r0 + 48]
    movu        m4, [r1]
    movu        m5, [r1 + 16]
    psubw       m0, m4
    psubw       m1, m5
    movu        m4, [r1 + 32]
    movu        m5, [r1 + 48]
    psubw       m2, m4
    psubw       m3, m5

    movu        [r2], m0
    movu        [r2 + 16], m1
    movu        [r2 + 32], m2
    movu        [r2 + 48], m3

    ; row 1
    movu        m0, [r0 + r3]
    movu        m1, [r0 + r3 + 16]
    movu        m2, [r0 + r3 + 32]
    movu        m3, [r0 + r3 + 48]
    movu        m4, [r1 + r3]
    movu        m5, [r1 + r3 + 16]
    psubw       m0, m4
    psubw       m1, m5
    movu        m4, [r1 + r3 + 32]
    movu        m5, [r1 + r3 + 48]
    psubw       m2, m4
    psubw       m3, m5

    movu        [r2 + r3], m0
    movu        [r2 + r3 + 16], m1
    movu        [r2 + r3 + 32], m2
    movu        [r2 + r3 + 48], m3

    dec         r4d

    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 2]
    jnz        .loop
    RET
%else
INIT_XMM sse4
cglobal getResidual32, 4,5,7
    mov         r4d, 32/2
    pxor        m0, m0
.loop:
    movu        m1, [r0]
    movu        m2, [r0 + 16]
    movu        m3, [r1]
    movu        m4, [r1 + 16]
    pmovzxbw    m5, m1
    punpckhbw   m1, m0
    pmovzxbw    m6, m3
    punpckhbw   m3, m0
    psubw       m5, m6
    psubw       m1, m3
    movu        [r2 + 0 * 16], m5
    movu        [r2 + 1 * 16], m1

    pmovzxbw    m5, m2
    punpckhbw   m2, m0
    pmovzxbw    m6, m4
    punpckhbw   m4, m0
    psubw       m5, m6
    psubw       m2, m4
    movu        [r2 + 2 * 16], m5
    movu        [r2 + 3 * 16], m2

    movu        m1, [r0 + r3]
    movu        m2, [r0 + r3 + 16]
    movu        m3, [r1 + r3]
    movu        m4, [r1 + r3 + 16]
    pmovzxbw    m5, m1
    punpckhbw   m1, m0
    pmovzxbw    m6, m3
    punpckhbw   m3, m0
    psubw       m5, m6
    psubw       m1, m3
    movu        [r2 + r3 * 2 + 0 * 16], m5
    movu        [r2 + r3 * 2 + 1 * 16], m1

    pmovzxbw    m5, m2
    punpckhbw   m2, m0
    pmovzxbw    m6, m4
    punpckhbw   m4, m0
    psubw       m5, m6
    psubw       m2, m4
    movu        [r2 + r3 * 2 + 2 * 16], m5
    movu        [r2 + r3 * 2 + 3 * 16], m2

    dec         r4d

    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 4]
    jnz        .loop
    RET
%endif


%if HIGH_BIT_DEPTH
INIT_YMM avx2
cglobal getResidual32, 4,4,5
    add         r3, r3
    pxor        m0, m0

%assign x 0
%rep 32
    movu        m1, [r0]
    movu        m2, [r0 + 32]
    movu        m3, [r1]
    movu        m4, [r1 + 32]

    psubw       m1, m3
    psubw       m2, m4
    movu        [r2], m1
    movu        [r2 + 32], m2
%assign x x+1
%if (x != 32)
    lea         r0, [r0 + r3]
    lea         r1, [r1 + r3]
    lea         r2, [r2 + r3]
%endif
%endrep
    RET
%else
INIT_YMM avx2
cglobal getResidual32, 4,5,8
    lea         r4, [r3 * 2]
%assign x 0
%rep 16
    pmovzxbw    m0, [r0]
    pmovzxbw    m1, [r0 + 16]
    pmovzxbw    m2, [r0 + r3]
    pmovzxbw    m3, [r0 + r3 + 16]

    pmovzxbw    m4, [r1]
    pmovzxbw    m5, [r1 + 16]
    pmovzxbw    m6, [r1 + r3]
    pmovzxbw    m7, [r1 + r3 + 16]

    psubw       m0, m4
    psubw       m1, m5
    psubw       m2, m6
    psubw       m3, m7

    movu        [r2 + 0 ], m0
    movu        [r2 + 32], m1
    movu        [r2 + r4 + 0], m2
    movu        [r2 + r4 + 32], m3
%assign x x+1
%if (x != 16)
    lea         r0, [r0 + r3 * 2]
    lea         r1, [r1 + r3 * 2]
    lea         r2, [r2 + r3 * 4]
%endif
%endrep
    RET
%endif
;-----------------------------------------------------------------------------
; uint32_t quant(int16_t *coef, int32_t *quantCoeff, int32_t *deltaU, int16_t *qCoef, int qBits, int add, int numCoeff);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal quant, 5,6,8
    ; fill qbits
    movd        m4, r4d         ; m4 = qbits

    ; fill qbits-8
    sub         r4d, 8
    movd        m6, r4d         ; m6 = qbits8

    ; fill offset
    movd        m5, r5m
    pshufd      m5, m5, 0       ; m5 = add

    lea         r5, [pd_1]

    mov         r4d, r6m
    shr         r4d, 3
    pxor        m7, m7          ; m7 = numZero
.loop:
    ; 4 coeff
    pmovsxwd    m0, [r0]        ; m0 = level
    pabsd       m1, m0
    pmulld      m1, [r1]        ; m0 = tmpLevel1
    paddd       m2, m1, m5
    psrad       m2, m4          ; m2 = level1

    pslld       m3, m2, 8
    psrad       m1, m6
    psubd       m1, m3          ; m1 = deltaU1

    movu        [r2], m1
    psignd      m3, m2, m0
    pminud      m2, [r5]
    paddd       m7, m2
    packssdw    m3, m3
    movh        [r3], m3

    ; 4 coeff
    pmovsxwd    m0, [r0 + 8]    ; m0 = level
    pabsd       m1, m0
    pmulld      m1, [r1 + 16]   ; m0 = tmpLevel1
    paddd       m2, m1, m5
    psrad       m2, m4          ; m2 = level1
    pslld       m3, m2, 8
    psrad       m1, m6
    psubd       m1, m3          ; m1 = deltaU1
    movu        [r2 + 16], m1
    psignd      m3, m2, m0
    pminud      m2, [r5]
    paddd       m7, m2
    packssdw    m3, m3
    movh        [r3 + 8], m3

    add         r0, 16
    add         r1, 32
    add         r2, 32
    add         r3, 16

    dec         r4d
    jnz        .loop

    pshufd      m0, m7, 00001110b
    paddd       m0, m7
    pshufd      m1, m0, 00000001b
    paddd       m0, m1
    movd        eax, m0
    RET


%if ARCH_X86_64 == 1
INIT_YMM avx2
cglobal quant, 5,6,9
    ; fill qbits
    movd            xm4, r4d            ; m4 = qbits

    ; fill qbits-8
    sub             r4d, 8
    movd            xm6, r4d            ; m6 = qbits8

    ; fill offset
%if UNIX64 == 0
    vpbroadcastd    m5, r5m             ; m5 = add
%else ; Mac
    movd           xm5, r5m
    vpbroadcastd    m5, xm5             ; m5 = add
%endif

    lea             r5, [pw_1]

    mov             r4d, r6m
    shr             r4d, 4
    pxor            m7, m7              ; m7 = numZero
.loop:
    ; 8 coeff
    pmovsxwd        m0, [r0]            ; m0 = level
    pabsd           m1, m0
    pmulld          m1, [r1]            ; m0 = tmpLevel1
    paddd           m2, m1, m5
    psrad           m2, xm4             ; m2 = level1

    pslld           m3, m2, 8
    psrad           m1, xm6
    psubd           m1, m3              ; m1 = deltaU1
    movu            [r2], m1
    psignd          m2, m0

    ; 8 coeff
    pmovsxwd        m0, [r0 + mmsize/2] ; m0 = level
    pabsd           m1, m0
    pmulld          m1, [r1 + mmsize]   ; m0 = tmpLevel1
    paddd           m3, m1, m5
    psrad           m3, xm4             ; m2 = level1

    pslld           m8, m3, 8
    psrad           m1, xm6
    psubd           m1, m8              ; m1 = deltaU1
    movu            [r2 + mmsize], m1
    psignd          m3, m0

    packssdw        m2, m3
    vpermq          m2, m2, q3120
    movu            [r3], m2

    ; count non-zero coeff
    ; TODO: popcnt is faster, but some CPU can't support
    pminuw          m2, [r5]
    paddw           m7, m2

    add             r0, mmsize
    add             r1, mmsize*2
    add             r2, mmsize*2
    add             r3, mmsize

    dec             r4d
    jnz            .loop

    ; sum count
    xorpd           m0, m0
    psadbw          m7, m0
    vextracti128    xm1, m7, 1
    paddd           xm7, xm1
    movhlps         xm0, xm7
    paddd           xm7, xm0
    movd            eax, xm7
    RET

%else ; ARCH_X86_64 == 1
INIT_YMM avx2
cglobal quant, 5,6,8
    ; fill qbits
    movd            xm4, r4d        ; m4 = qbits

    ; fill qbits-8
    sub             r4d, 8
    movd            xm6, r4d        ; m6 = qbits8

    ; fill offset
%if UNIX64 == 0
    vpbroadcastd    m5, r5m         ; m5 = add
%else ; Mac
    movd           xm5, r5m
    vpbroadcastd    m5, xm5         ; m5 = add
%endif

    lea             r5, [pd_1]

    mov             r4d, r6m
    shr             r4d, 4
    pxor            m7, m7          ; m7 = numZero
.loop:
    ; 8 coeff
    pmovsxwd        m0, [r0]        ; m0 = level
    pabsd           m1, m0
    pmulld          m1, [r1]        ; m0 = tmpLevel1
    paddd           m2, m1, m5
    psrad           m2, xm4         ; m2 = level1

    pslld           m3, m2, 8
    psrad           m1, xm6
    psubd           m1, m3          ; m1 = deltaU1

    movu            [r2], m1
    psignd          m3, m2, m0
    pminud          m2, [r5]
    paddd           m7, m2
    packssdw        m3, m3
    vpermq          m3, m3, q0020
    movu            [r3], xm3

    ; 8 coeff
    pmovsxwd        m0, [r0 + mmsize/2]        ; m0 = level
    pabsd           m1, m0
    pmulld          m1, [r1 + mmsize]        ; m0 = tmpLevel1
    paddd           m2, m1, m5
    psrad           m2, xm4         ; m2 = level1

    pslld           m3, m2, 8
    psrad           m1, xm6
    psubd           m1, m3          ; m1 = deltaU1

    movu            [r2 + mmsize], m1
    psignd          m3, m2, m0
    pminud          m2, [r5]
    paddd           m7, m2
    packssdw        m3, m3
    vpermq          m3, m3, q0020
    movu            [r3 + mmsize/2], xm3

    add             r0, mmsize
    add             r1, mmsize*2
    add             r2, mmsize*2
    add             r3, mmsize

    dec             r4d
    jnz            .loop

    xorpd           m0, m0
    psadbw          m7, m0
    vextracti128    xm1, m7, 1
    paddd           xm7, xm1
    movhlps         xm0, xm7
    paddd           xm7, xm0
    movd            eax, xm7
    RET
%endif ; ARCH_X86_64 == 1


;-----------------------------------------------------------------------------
; uint32_t nquant(int16_t *coef, int32_t *quantCoeff, int16_t *qCoef, int qBits, int add, int numCoeff);
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal nquant, 3,5,8
    movd        m6, r4m
    mov         r4d, r5m
    pxor        m7, m7          ; m7 = numZero
    movd        m5, r3m         ; m5 = qbits
    pshufd      m6, m6, 0       ; m6 = add
    mov         r3d, r4d        ; r3 = numCoeff
    shr         r4d, 3
    pxor        m4, m4

.loop:
    pmovsxwd    m0, [r0]        ; m0 = level
    pmovsxwd    m1, [r0 + 8]    ; m1 = level

    pabsd       m2, m0
    pmulld      m2, [r1]        ; m0 = tmpLevel1 * qcoeff
    paddd       m2, m6
    psrad       m2, m5          ; m0 = level1
    psignd      m2, m0

    pabsd       m3, m1
    pmulld      m3, [r1 + 16]   ; m1 = tmpLevel1 * qcoeff
    paddd       m3, m6
    psrad       m3, m5          ; m1 = level1
    psignd      m3, m1

    packssdw    m2, m3
    pabsw       m2, m2

    movu        [r2], m2
    add         r0, 16
    add         r1, 32
    add         r2, 16

    pcmpeqw     m2, m4
    psubw       m7, m2

    dec         r4d
    jnz         .loop

    packuswb    m7, m7
    psadbw      m7, m4
    mov         eax, r3d
    movd        r4d, m7
    sub         eax, r4d        ; numSig
    RET


INIT_YMM avx2
cglobal nquant, 3,5,7
%if UNIX64 == 0
    vpbroadcastd m4, r4m
%else ; Mac
    movd         xm4, r4m
    vpbroadcastd m4, xm4
%endif
    vpbroadcastd m6, [pw_1]
    mov         r4d, r5m
    pxor        m5, m5              ; m7 = numZero
    movd        xm3, r3m            ; m5 = qbits
    mov         r3d, r4d            ; r3 = numCoeff
    shr         r4d, 4

.loop:
    pmovsxwd    m0, [r0]            ; m0 = level
    pabsd       m1, m0
    pmulld      m1, [r1]            ; m0 = tmpLevel1 * qcoeff
    paddd       m1, m4
    psrad       m1, xm3             ; m0 = level1
    psignd      m1, m0

    pmovsxwd    m0, [r0 + mmsize/2] ; m0 = level
    pabsd       m2, m0
    pmulld      m2, [r1 + mmsize]   ; m0 = tmpLevel1 * qcoeff
    paddd       m2, m4
    psrad       m2, xm3             ; m0 = level1
    psignd      m2, m0

    packssdw    m1, m2
    pabsw       m1, m1

    vpermq      m2, m1, q3120
    movu        [r2], m2

    add         r0, mmsize
    add         r1, mmsize * 2
    add         r2, mmsize

    pminuw      m1, m6
    paddw       m5, m1

    dec         r4d
    jnz         .loop

    pxor        m0, m0
    psadbw      m5, m0
    vextracti128 xm0, m5, 1
    paddd       xm5, xm0
    pshufd      xm0, xm5, 2
    paddd       xm5, xm0
    movd        eax, xm5
    RET


;-----------------------------------------------------------------------------
; void dequant_normal(const int16_t* quantCoef, int32_t* coef, int num, int scale, int shift)
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal dequant_normal, 5,5,5
    mova        m2, [pw_1]
%if HIGH_BIT_DEPTH
    cmp         r3d, 32767
    jle         .skip
    shr         r3d, (BIT_DEPTH - 8)
    sub         r4d, (BIT_DEPTH - 8)
.skip:
%endif
    movd        m0, r4d             ; m0 = shift
    add         r4d, 15
    bts         r3d, r4d
    movd        m1, r3d
    pshufd      m1, m1, 0           ; m1 = dword [add scale]
    ; m0 = shift
    ; m1 = scale
    ; m2 = word [1]
.loop:
    movu        m3, [r0]
    punpckhwd   m4, m3, m2
    punpcklwd   m3, m2
    pmaddwd     m3, m1              ; m3 = dword (clipQCoef * scale + add)
    pmaddwd     m4, m1
    psrad       m3, m0
    psrad       m4, m0
    packssdw    m3, m4
    mova        [r1], m3

    add         r0, 16
    add         r1, 16

    sub         r2d, 8
    jnz        .loop
    RET

;----------------------------------------------------------------------------------------------------------------------
;void dequant_scaling(const int16_t* src, const int32_t* dequantCoef, int16_t* dst, int num, int mcqp_miper, int shift)
;----------------------------------------------------------------------------------------------------------------------
INIT_XMM sse4
cglobal dequant_scaling, 6,6,6
    add         r5d, 4
    shr         r3d, 3          ; num/8
    cmp         r5d, r4d
    jle         .skip
    sub         r5d, r4d
    mova        m0, [pd_1]
    movd        m1, r5d         ; shift - per
    dec         r5d
    movd        m2, r5d         ; shift - per - 1
    pslld       m0, m2          ; 1 << shift - per - 1

.part0:
    pmovsxwd    m2, [r0]
    pmovsxwd    m4, [r0 + 8]
    movu        m3, [r1]
    movu        m5, [r1 + 16]
    pmulld      m2, m3
    pmulld      m4, m5
    paddd       m2, m0
    paddd       m4, m0
    psrad       m2, m1
    psrad       m4, m1
    packssdw    m2, m4
    movu        [r2], m2

    add         r0, 16
    add         r1, 32
    add         r2, 16
    dec         r3d
    jnz         .part0
    jmp         .end

.skip:
    sub         r4d, r5d        ; per - shift
    movd        m0, r4d

.part1:
    pmovsxwd    m2, [r0]
    pmovsxwd    m4, [r0 + 8]
    movu        m3, [r1]
    movu        m5, [r1 + 16]
    pmulld      m2, m3
    pmulld      m4, m5
    packssdw    m2, m4
    pmovsxwd    m1, m2
    psrldq      m2, 8
    pmovsxwd    m2, m2
    pslld       m1, m0
    pslld       m2, m0
    packssdw    m1, m2
    movu        [r2], m1

    add         r0, 16
    add         r1, 32
    add         r2, 16
    dec         r3d
    jnz         .part1
.end:
    RET

;----------------------------------------------------------------------------------------------------------------------
;void dequant_scaling(const int16_t* src, const int32_t* dequantCoef, int16_t* dst, int num, int mcqp_miper, int shift)
;----------------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
cglobal dequant_scaling, 6,6,6
    add         r5d, 4
    shr         r3d, 4          ; num/16
    cmp         r5d, r4d
    jle         .skip
    sub         r5d, r4d
    mova        m0, [pd_1]
    movd        xm1, r5d         ; shift - per
    dec         r5d
    movd        xm2, r5d         ; shift - per - 1
    pslld       m0, xm2          ; 1 << shift - per - 1

.part0:
    pmovsxwd    m2, [r0]
    pmovsxwd    m4, [r0 + 16]
    movu        m3, [r1]
    movu        m5, [r1 + 32]
    pmulld      m2, m3
    pmulld      m4, m5
    paddd       m2, m0
    paddd       m4, m0
    psrad       m2, xm1
    psrad       m4, xm1
    packssdw    m2, m4
    vpermq      m2, m2, 11011000b
    movu        [r2], m2

    add         r0, 32
    add         r1, 64
    add         r2, 32
    dec         r3d
    jnz         .part0
    jmp         .end

.skip:
    sub         r4d, r5d        ; per - shift
    movd        xm0, r4d

.part1:
    pmovsxwd    m2, [r0]
    pmovsxwd    m4, [r0 + 16]
    movu        m3, [r1]
    movu        m5, [r1 + 32]
    pmulld      m2, m3
    pmulld      m4, m5
    packssdw    m2, m4
    vextracti128 xm4, m2, 1
    pmovsxwd    m1, xm2
    pmovsxwd    m2, xm4
    pslld       m1, xm0
    pslld       m2, xm0
    packssdw    m1, m2
    movu        [r2], m1

    add         r0, 32
    add         r1, 64
    add         r2, 32
    dec         r3d
    jnz         .part1
.end:
    RET

INIT_YMM avx2
cglobal dequant_normal, 5,5,7
    vpbroadcastd    m2, [pw_1]          ; m2 = word [1]
    vpbroadcastd    m5, [pd_32767]      ; m5 = dword [32767]
    vpbroadcastd    m6, [pd_n32768]     ; m6 = dword [-32768]
%if HIGH_BIT_DEPTH
    cmp             r3d, 32767
    jle            .skip
    shr             r3d, (BIT_DEPTH - 8)
    sub             r4d, (BIT_DEPTH - 8)
.skip:
%endif
    movd            xm0, r4d            ; m0 = shift
    add             r4d, -1+16
    bts             r3d, r4d

    movd            xm1, r3d
    vpbroadcastd    m1, xm1             ; m1 = dword [add scale]

    ; m0 = shift
    ; m1 = scale
    ; m2 = word [1]
    shr             r2d, 4
.loop:
    movu            m3, [r0]
    punpckhwd       m4, m3, m2
    punpcklwd       m3, m2
    pmaddwd         m3, m1              ; m3 = dword (clipQCoef * scale + add)
    pmaddwd         m4, m1
    psrad           m3, xm0
    psrad           m4, xm0
    pminsd          m3, m5
    pmaxsd          m3, m6
    pminsd          m4, m5
    pmaxsd          m4, m6
    packssdw        m3, m4
    mova            [r1 + 0 * mmsize/2], xm3
    vextracti128    [r1 + 1 * mmsize/2], m3, 1

    add             r0, mmsize
    add             r1, mmsize

    dec             r2d
    jnz            .loop
    RET


;-----------------------------------------------------------------------------
; int x265_count_nonzero_4x4_sse2(const int16_t *quantCoeff);
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal count_nonzero_4x4, 1,1,2
    pxor            m0, m0

    mova            m1, [r0 + 0]
    packsswb        m1, [r0 + 16]
    pcmpeqb         m1, m0
    paddb           m1, [pb_1]

    psadbw          m1, m0
    pshufd          m0, m1, 2
    paddd           m0, m1
    movd            eax, m0
    RET


;-----------------------------------------------------------------------------
; int x265_count_nonzero_4x4_avx2(const int16_t *quantCoeff);
;-----------------------------------------------------------------------------
INIT_YMM avx2
cglobal count_nonzero_4x4, 1,1,2
    pxor            m0, m0
    movu            m1, [r0]
    pcmpeqw         m1, m0
    pmovmskb        eax, m1
    not             eax
    popcnt          eax, eax
    shr             eax, 1
    RET

;-----------------------------------------------------------------------------
; int x265_count_nonzero_8x8_sse2(const int16_t *quantCoeff);
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal count_nonzero_8x8, 1,1,3
    pxor            m0, m0
    movu            m1, [pb_4]

%rep 4
    mova            m2, [r0 + 0]
    packsswb        m2, [r0 + 16]
    add             r0, 32
    pcmpeqb         m2, m0
    paddb           m1, m2
%endrep

    psadbw          m1, m0
    pshufd          m0, m1, 2
    paddd           m0, m1
    movd            eax, m0
    RET


;-----------------------------------------------------------------------------
; int x265_count_nonzero_8x8_avx2(const int16_t *quantCoeff);
;-----------------------------------------------------------------------------
INIT_YMM avx2
cglobal count_nonzero_8x8, 1,1,3
    pxor            m0, m0
    movu            m1, [pb_2]

    mova            m2, [r0]
    packsswb        m2, [r0 + 32]
    pcmpeqb         m2, m0
    paddb           m1, m2

    mova            m2, [r0 + 64]
    packsswb        m2, [r0 + 96]
    pcmpeqb         m2, m0
    paddb           m1, m2
 
    psadbw          m1, m0
    vextracti128    xm0, m1, 1
    paddd           m0, m1
    pshufd          m1, m0, 2
    paddd           m0, m1
    movd            eax, xm0
    RET


;-----------------------------------------------------------------------------
; int x265_count_nonzero_16x16_sse2(const int16_t *quantCoeff);
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal count_nonzero_16x16, 1,1,3
    pxor            m0, m0
    movu            m1, [pb_16]

%rep 16
    mova            m2, [r0 + 0]
    packsswb        m2, [r0 + 16]
    add             r0, 32
    pcmpeqb         m2, m0
    paddb           m1, m2
%endrep

    psadbw          m1, m0
    pshufd          m0, m1, 2
    paddd           m0, m1
    movd            eax, m0
    RET


;-----------------------------------------------------------------------------
; int x265_count_nonzero_16x16_avx2(const int16_t *quantCoeff);
;-----------------------------------------------------------------------------
INIT_YMM avx2
cglobal count_nonzero_16x16, 1,1,3
    pxor            m0, m0
    movu            m1, [pb_8]

%assign x 0
%rep 8
    mova            m2, [r0 + x]
    packsswb        m2, [r0 + x + 32]
%assign x x+64
    pcmpeqb         m2, m0
    paddb           m1, m2
%endrep 

    psadbw          m1, m0
    vextracti128    xm0, m1, 1
    paddd           m0, m1
    pshufd          m1, m0, 2
    paddd           m0, m1
    movd            eax, xm0
    RET


;-----------------------------------------------------------------------------
; int x265_count_nonzero_32x32_sse2(const int16_t *quantCoeff);
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal count_nonzero_32x32, 1,1,3
    pxor            m0, m0
    movu            m1, [pb_64]

%rep 64 
    mova            m2, [r0 + 0]
    packsswb        m2, [r0 + 16]
    add             r0, 32
    pcmpeqb         m2, m0
    paddb           m1, m2
%endrep

    psadbw          m1, m0
    pshufd          m0, m1, 2
    paddd           m0, m1
    movd            eax, m0
    RET


;-----------------------------------------------------------------------------
; int x265_count_nonzero_32x32_avx2(const int16_t *quantCoeff);
;-----------------------------------------------------------------------------
INIT_YMM avx2
cglobal count_nonzero_32x32, 1,1,3
    pxor            m0, m0
    movu            m1, [pb_32]

%assign x 0
%rep 32
    mova            m2, [r0 + x]
    packsswb        m2, [r0 + x + 32]
%assign x x+64
    pcmpeqb         m2, m0
    paddb           m1, m2
%endrep 

    psadbw          m1, m0
    vextracti128    xm0, m1, 1
    paddd           m0, m1
    pshufd          m1, m0, 2
    paddd           m0, m1
    movd            eax, xm0
    RET


;-----------------------------------------------------------------------------------------------------------------------------------------------
;void weight_pp(pixel *src, pixel *dst, intptr_t stride, int width, int height, int w0, int round, int shift, int offset)
;-----------------------------------------------------------------------------------------------------------------------------------------------
%if HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal weight_pp, 4,7,7
%define correction      (14 - BIT_DEPTH)
    mova        m6, [pw_pixel_max]
    mov         r6d, r6m
    mov         r4d, r4m
    mov         r5d, r5m
    shl         r6d, 16 - correction
    or          r6d, r5d    ; assuming both (w0) and round are using maximum of 16 bits each.
    movd        m0, r6d
    pshufd      m0, m0, 0   ; m0 = [w0, round]
    mov         r5d, r7m
    sub         r5d, correction
    movd        m1, r5d
    movd        m2, r8m
    pshufd      m2, m2, 0
    mova        m5, [pw_1]
    sub         r2d, r3d
    add         r2d, r2d
    shr         r3d, 4

.loopH:
    mov         r5d, r3d

.loopW:
    movu        m4, [r0]
    punpcklwd   m3, m4, m5
    pmaddwd     m3, m0
    psrad       m3, m1
    paddd       m3, m2      ; TODO: we can put Offset into Round, but we have to analyze Dynamic Range before that.

    punpckhwd   m4, m5
    pmaddwd     m4, m0
    psrad       m4, m1
    paddd       m4, m2

    packusdw    m3, m4
    pminuw      m3, m6
    movu        [r1], m3

    movu        m4, [r0 + mmsize]
    punpcklwd   m3, m4, m5
    pmaddwd     m3, m0
    psrad       m3, m1
    paddd       m3, m2

    punpckhwd   m4, m5
    pmaddwd     m4, m0
    psrad       m4, m1
    paddd       m4, m2

    packusdw    m3, m4
    pminuw      m3, m6
    movu        [r1 + mmsize], m3

    add         r0, 2 * mmsize
    add         r1, 2 * mmsize

    dec         r5d
    jnz        .loopW

    add         r0, r2
    add         r1, r2

    dec         r4d
    jnz         .loopH
    RET

%else   ; end of (HIGH_BIT_DEPTH == 1)

INIT_XMM sse4
cglobal weight_pp, 6,7,6
    shl         r5d, 6      ; m0 = [w0<<6]
    mov         r6d, r6m
    shl         r6d, 16
    or          r6d, r5d    ; assuming both (w0<<6) and round are using maximum of 16 bits each.
    movd        m0, r6d
    pshufd      m0, m0, 0   ; m0 = [w0<<6, round]
    movd        m1, r7m
    movd        m2, r8m
    pshufd      m2, m2, 0
    mova        m5, [pw_1]
    sub         r2d, r3d
    shr         r3d, 4

.loopH:
    mov         r5d, r3d

.loopW:
    pmovzxbw    m4, [r0]
    punpcklwd   m3, m4, m5
    pmaddwd     m3, m0
    psrad       m3, m1
    paddd       m3, m2

    punpckhwd   m4, m5
    pmaddwd     m4, m0
    psrad       m4, m1
    paddd       m4, m2

    packssdw    m3, m4
    packuswb    m3, m3
    movh        [r1], m3

    pmovzxbw    m4, [r0 + 8]
    punpcklwd   m3, m4, m5
    pmaddwd     m3, m0
    psrad       m3, m1
    paddd       m3, m2

    punpckhwd   m4, m5
    pmaddwd     m4, m0
    psrad       m4, m1
    paddd       m4, m2

    packssdw    m3, m4
    packuswb    m3, m3
    movh        [r1 + 8], m3

    add         r0, 16
    add         r1, 16

    dec         r5d
    jnz         .loopW

    lea         r0, [r0 + r2]
    lea         r1, [r1 + r2]

    dec         r4d
    jnz         .loopH
    RET
%endif  ; end of (HIGH_BIT_DEPTH == 0)


%if HIGH_BIT_DEPTH
INIT_YMM avx2
cglobal weight_pp, 6, 7, 7
%define correction      (14 - BIT_DEPTH)
    mov          r6d, r6m
    shl          r6d, 16 - correction
    or           r6d, r5d          ; assuming both w0 and round are using maximum of 16 bits each.

    movd         xm0, r6d
    vpbroadcastd m0, xm0

    mov          r5d, r7m
    sub          r5d, correction
    movd         xm1, r5d
    vpbroadcastd m2, r8m
    mova         m5, [pw_1]
    mova         m6, [pw_pixel_max]
    add         r2d, r2d
    add         r3d, r3d
    sub          r2d, r3d
    shr          r3d, 5

.loopH:
    mov          r5d, r3d

.loopW:
    movu        m4, [r0]
    punpcklwd   m3, m4, m5
    pmaddwd     m3, m0
    psrad       m3, xm1
    paddd       m3, m2

    punpckhwd   m4, m5
    pmaddwd     m4, m0
    psrad       m4, xm1
    paddd       m4, m2

    packusdw    m3, m4
    pminuw      m3, m6
    movu        [r1], m3

    add         r0, 32
    add         r1, 32

    dec         r5d
    jnz         .loopW

    lea         r0, [r0 + r2]
    lea         r1, [r1 + r2]

    dec         r4d
    jnz         .loopH
%undef correction
    RET
%else
INIT_YMM avx2
cglobal weight_pp, 6, 7, 6

    shl          r5d, 6            ; m0 = [w0<<6]
    mov          r6d, r6m
    shl          r6d, 16
    or           r6d, r5d          ; assuming both (w0<<6) and round are using maximum of 16 bits each.

    movd         xm0, r6d
    vpbroadcastd m0, xm0

    movd         xm1, r7m
    vpbroadcastd m2, r8m
    mova         m5, [pw_1]
    sub          r2d, r3d
    shr          r3d, 4

.loopH:
    mov          r5d, r3d

.loopW:
    pmovzxbw    m4, [r0]
    punpcklwd   m3, m4, m5
    pmaddwd     m3, m0
    psrad       m3, xm1
    paddd       m3, m2

    punpckhwd   m4, m5
    pmaddwd     m4, m0
    psrad       m4, xm1
    paddd       m4, m2

    packssdw    m3, m4
    vextracti128 xm4, m3, 1
    packuswb    xm3, xm4
    movu        [r1], xm3

    add         r0, 16
    add         r1, 16

    dec         r5d
    jnz         .loopW

    lea         r0, [r0 + r2]
    lea         r1, [r1 + r2]

    dec         r4d
    jnz         .loopH
    RET
%endif
;-------------------------------------------------------------------------------------------------------------------------------------------------
;void weight_sp(int16_t *src, pixel *dst, intptr_t srcStride, intptr_t dstStride, int width, int height, int w0, int round, int shift, int offset)
;-------------------------------------------------------------------------------------------------------------------------------------------------
%if HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal weight_sp, 6,7,8
    mova        m1, [pw_pixel_max]
    mova        m2, [pw_1]
    mov         r6d, r7m
    shl         r6d, 16
    or          r6d, r6m    ; assuming both (w0) and round are using maximum of 16 bits each.
    movd        m3, r6d
    pshufd      m3, m3, 0   ; m3 = [round w0]

    movd        m4, r8m     ; m4 = [shift]
    movd        m5, r9m
    pshufd      m5, m5, 0   ; m5 = [offset]

    ; correct row stride
    add         r3d, r3d
    add         r2d, r2d
    mov         r6d, r4d
    and         r6d, ~(mmsize / SIZEOF_PIXEL - 1)
    sub         r3d, r6d
    sub         r3d, r6d
    sub         r2d, r6d
    sub         r2d, r6d

    ; generate partial width mask (MUST BE IN XMM0)
    mov         r6d, r4d
    and         r6d, (mmsize / SIZEOF_PIXEL - 1)
    movd        m0, r6d
    pshuflw     m0, m0, 0
    punpcklqdq  m0, m0
    pcmpgtw     m0, [pw_0_7]

.loopH:
    mov         r6d, r4d

.loopW:
    movu        m6, [r0]
    paddw       m6, [pw_2000]

    punpcklwd   m7, m6, m2
    pmaddwd     m7, m3
    psrad       m7, m4
    paddd       m7, m5

    punpckhwd   m6, m2
    pmaddwd     m6, m3
    psrad       m6, m4
    paddd       m6, m5

    packusdw    m7, m6
    pminuw      m7, m1

    sub         r6d, (mmsize / SIZEOF_PIXEL)
    jl         .widthLess8
    movu        [r1], m7
    lea         r0, [r0 + mmsize]
    lea         r1, [r1 + mmsize]
    je         .nextH
    jmp        .loopW

.widthLess8:
    movu        m6, [r1]
    pblendvb    m6, m7, m0
    movu        [r1], m6

.nextH:
    add         r0, r2
    add         r1, r3

    dec         r5d
    jnz         .loopH
    RET

%else   ; end of (HIGH_BIT_DEPTH == 1)

INIT_XMM sse4
%if ARCH_X86_64
cglobal weight_sp, 6, 7+2, 7
    %define tmp_r0      r7
    %define tmp_r1      r8
%else ; ARCH_X86_64 = 0
cglobal weight_sp, 6, 7, 7, 0-(2*4)
    %define tmp_r0      [(rsp + 0 * 4)]
    %define tmp_r1      [(rsp + 1 * 4)]
%endif ; ARCH_X86_64

    movd        m0, r6m         ; m0 = [w0]

    movd        m1, r7m         ; m1 = [round]
    punpcklwd   m0, m1
    pshufd      m0, m0, 0       ; m0 = [w0 round]

    movd        m1, r8m         ; m1 = [shift]

    movd        m2, r9m
    pshufd      m2, m2, 0       ; m2 =[offset]

    mova        m3, [pw_1]
    mova        m4, [pw_2000]

    add         r2d, r2d

.loopH:
    mov         r6d, r4d

    ; save old src and dst
    mov         tmp_r0, r0
    mov         tmp_r1, r1
.loopW:
    movu        m5, [r0]
    paddw       m5, m4

    punpcklwd   m6,m5, m3
    pmaddwd     m6, m0
    psrad       m6, m1
    paddd       m6, m2

    punpckhwd   m5, m3
    pmaddwd     m5, m0
    psrad       m5, m1
    paddd       m5, m2

    packssdw    m6, m5
    packuswb    m6, m6

    sub         r6d, 8
    jl          .width4
    movh        [r1], m6
    je          .nextH
    add         r0, 16
    add         r1, 8

    jmp         .loopW

.width4:
    cmp         r6d, -4
    jl          .width2
    movd        [r1], m6
    je          .nextH
    add         r1, 4
    pshufd      m6, m6, 1

.width2:
    pextrw      [r1], m6, 0

.nextH:
    mov         r0, tmp_r0
    mov         r1, tmp_r1
    lea         r0, [r0 + r2]
    lea         r1, [r1 + r3]

    dec         r5d
    jnz         .loopH
    RET
%endif


%if ARCH_X86_64 == 1
%if HIGH_BIT_DEPTH
INIT_YMM avx2
cglobal weight_sp, 6,7,9
    mova                      m1, [pw_pixel_max]
    mova                      m2, [pw_1]
    mov                       r6d, r7m
    shl                       r6d, 16
    or                        r6d, r6m
    movd                      xm3, r6d
    vpbroadcastd              m3, xm3      ; m3 = [round w0]
    movd                      xm4, r8m     ; m4 = [shift]
    vpbroadcastd              m5, r9m      ; m5 = [offset]

    ; correct row stride
    add                       r3d, r3d
    add                       r2d, r2d
    mov                       r6d, r4d
    and                       r6d, ~(mmsize / SIZEOF_PIXEL - 1)
    sub                       r3d, r6d
    sub                       r3d, r6d
    sub                       r2d, r6d
    sub                       r2d, r6d

    ; generate partial width mask (MUST BE IN YMM0)
    mov                       r6d, r4d
    and                       r6d, (mmsize / SIZEOF_PIXEL - 1)
    movd                      xm0, r6d
    pshuflw                   m0, m0, 0
    punpcklqdq                m0, m0
    vinserti128               m0, m0, xm0, 1
    pcmpgtw                   m0, [pw_0_7]

.loopH:
    mov                       r6d, r4d

.loopW:
    movu                      m6, [r0]
    paddw                     m6, [pw_2000]

    punpcklwd                 m7, m6, m2
    pmaddwd                   m7, m3       ;(round w0)
    psrad                     m7, xm4      ;(shift)
    paddd                     m7, m5       ;(offset)

    punpckhwd                 m6, m2
    pmaddwd                   m6, m3
    psrad                     m6, xm4
    paddd                     m6, m5

    packusdw                  m7, m6
    pminuw                    m7, m1

    sub                       r6d, (mmsize / SIZEOF_PIXEL)
    jl                        .width14
    movu                      [r1], m7
    lea                       r0, [r0 + mmsize]
    lea                       r1, [r1 + mmsize]
    je                        .nextH
    jmp                       .loopW

.width14:
    add                       r6d, 16
    cmp                       r6d, 14
    jl                        .width12
    movu                      [r1], xm7
    vextracti128              xm8, m7, 1
    movq                      [r1 + 16], xm8
    pextrd                    [r1 + 24], xm8, 2
    je                        .nextH

.width12:
    cmp                       r6d, 12
    jl                        .width10
    movu                      [r1], xm7
    vextracti128              xm8, m7, 1
    movq                      [r1 + 16], xm8
    je                        .nextH

.width10:
    cmp                       r6d, 10
    jl                        .width8
    movu                      [r1], xm7
    vextracti128              xm8, m7, 1
    movd                      [r1 + 16], xm8
    je                        .nextH

.width8:
    cmp                       r6d, 8
    jl                        .width6
    movu                      [r1], xm7
    je                        .nextH

.width6
    cmp                       r6d, 6
    jl                        .width4
    movq                      [r1], xm7
    pextrd                    [r1 + 8], xm7, 2
    je                        .nextH

.width4:
    cmp                       r6d, 4
    jl                        .width2
    movq                      [r1], xm7
    je                        .nextH
    add                       r1, 4
    pshufd                    m6, m6, 1
    je                        .nextH

.width2:
    movd                      [r1], xm7

.nextH:
    add                       r0, r2
    add                       r1, r3

    dec                       r5d
    jnz                       .loopH
    RET

%else
INIT_YMM avx2
cglobal weight_sp, 6, 9, 7
    mov             r7d, r7m
    shl             r7d, 16
    or              r7d, r6m
    movd            xm0, r7d
    vpbroadcastd    m0, xm0            ; m0 = times 8 dw w0, round
    movd            xm1, r8m           ; m1 = [shift]
    vpbroadcastd    m2, r9m            ; m2 = times 16 dw offset
    vpbroadcastw    m3, [pw_1]
    vpbroadcastw    m4, [pw_2000]

    add             r2d, r2d            ; 2 * srcstride

    mov             r7, r0
    mov             r8, r1
.loopH:
    mov             r6d, r4d            ; width

    ; save old src and dst
    mov             r0, r7              ; src
    mov             r1, r8              ; dst
.loopW:
    movu            m5, [r0]
    paddw           m5, m4

    punpcklwd       m6,m5, m3
    pmaddwd         m6, m0
    psrad           m6, xm1
    paddd           m6, m2

    punpckhwd       m5, m3
    pmaddwd         m5, m0
    psrad           m5, xm1
    paddd           m5, m2

    packssdw        m6, m5
    packuswb        m6, m6
    vpermq          m6, m6, 10001000b

    sub             r6d, 16
    jl              .width8
    movu            [r1], xm6
    je              .nextH
    add             r0, 32
    add             r1, 16
    jmp             .loopW

.width8:
    add             r6d, 16
    cmp             r6d, 8
    jl              .width4
    movq            [r1], xm6
    je              .nextH
    psrldq          m6, 8
    sub             r6d, 8
    add             r1, 8

.width4:
    cmp             r6d, 4
    jl              .width2
    movd            [r1], xm6
    je              .nextH
    add             r1, 4
    pshufd          m6, m6, 1

.width2:
    pextrw          [r1], xm6, 0

.nextH:
    lea             r7, [r7 + r2]
    lea             r8, [r8 + r3]

    dec             r5d
    jnz             .loopH
    RET
%endif
%endif

;-----------------------------------------------------------------
; void transpose_4x4(pixel *dst, pixel *src, intptr_t stride)
;-----------------------------------------------------------------
INIT_XMM sse2
cglobal transpose4, 3, 3, 4, dest, src, stride
%if HIGH_BIT_DEPTH == 1
    add          r2,    r2
    movh         m0,    [r1]
    movh         m1,    [r1 + r2]
    movh         m2,    [r1 + 2 * r2]
    lea          r1,    [r1 + 2 * r2]
    movh         m3,    [r1 + r2]
    punpcklwd    m0,    m1
    punpcklwd    m2,    m3
    punpckhdq    m1,    m0,    m2
    punpckldq    m0,    m2
    movu         [r0],       m0
    movu         [r0 + 16],  m1
%else ;HIGH_BIT_DEPTH == 0
    movd         m0,    [r1]
    movd         m1,    [r1 + r2]
    movd         m2,    [r1 + 2 * r2]
    lea          r1,    [r1 + 2 * r2]
    movd         m3,    [r1 + r2]

    punpcklbw    m0,    m1
    punpcklbw    m2,    m3
    punpcklwd    m0,    m2
    movu         [r0],    m0
%endif
    RET

;-----------------------------------------------------------------
; void transpose_8x8(pixel *dst, pixel *src, intptr_t stride)
;-----------------------------------------------------------------
%if HIGH_BIT_DEPTH == 1
%if ARCH_X86_64 == 1
INIT_YMM avx2
cglobal transpose8, 3, 5, 5
    add          r2, r2
    lea          r3, [3 * r2]
    lea          r4, [r1 + 4 * r2]
    movu         xm0, [r1]
    vinserti128  m0, m0, [r4], 1
    movu         xm1, [r1 + r2]
    vinserti128  m1, m1, [r4 + r2], 1
    movu         xm2, [r1 + 2 * r2]
    vinserti128  m2, m2, [r4 + 2 * r2], 1
    movu         xm3, [r1 + r3]
    vinserti128  m3, m3, [r4 + r3], 1

    punpcklwd    m4, m0, m1          ;[1 - 4][row1row2;row5row6]
    punpckhwd    m0, m1              ;[5 - 8][row1row2;row5row6]

    punpcklwd    m1, m2, m3          ;[1 - 4][row3row4;row7row8]
    punpckhwd    m2, m3              ;[5 - 8][row3row4;row7row8]

    punpckldq    m3, m4, m1          ;[1 - 2][row1row2row3row4;row5row6row7row8]
    punpckhdq    m4, m1              ;[3 - 4][row1row2row3row4;row5row6row7row8]

    punpckldq    m1, m0, m2          ;[5 - 6][row1row2row3row4;row5row6row7row8]
    punpckhdq    m0, m2              ;[7 - 8][row1row2row3row4;row5row6row7row8]

    vpermq       m3, m3, 0xD8        ;[1 ; 2][row1row2row3row4row5row6row7row8]
    vpermq       m4, m4, 0xD8        ;[3 ; 4][row1row2row3row4row5row6row7row8]
    vpermq       m1, m1, 0xD8        ;[5 ; 6][row1row2row3row4row5row6row7row8]
    vpermq       m0, m0, 0xD8        ;[7 ; 8][row1row2row3row4row5row6row7row8]

    movu         [r0 + 0 * 32], m3
    movu         [r0 + 1 * 32], m4
    movu         [r0 + 2 * 32], m1
    movu         [r0 + 3 * 32], m0
    RET
%endif

INIT_XMM sse2
%macro TRANSPOSE_4x4 1
    movh         m0,    [r1]
    movh         m1,    [r1 + r2]
    movh         m2,    [r1 + 2 * r2]
    lea          r1,    [r1 + 2 * r2]
    movh         m3,    [r1 + r2]
    punpcklwd    m0,    m1
    punpcklwd    m2,    m3
    punpckhdq    m1,    m0,    m2
    punpckldq    m0,    m2
    movh         [r0],             m0
    movhps       [r0 + %1],        m0
    movh         [r0 + 2 * %1],    m1
    lea            r0,               [r0 + 2 * %1]
    movhps         [r0 + %1],        m1
%endmacro
cglobal transpose8_internal
    TRANSPOSE_4x4 r5
    lea    r1,    [r1 + 2 * r2]
    lea    r0,    [r3 + 8]
    TRANSPOSE_4x4 r5
    lea    r1,    [r1 + 2 * r2]
    neg    r2
    lea    r1,    [r1 + r2 * 8 + 8]
    neg    r2
    lea    r0,    [r3 + 4 * r5]
    TRANSPOSE_4x4 r5
    lea    r1,    [r1 + 2 * r2]
    lea    r0,    [r3 + 8 + 4 * r5]
    TRANSPOSE_4x4 r5
    ret
cglobal transpose8, 3, 6, 4, dest, src, stride
    add    r2,    r2
    mov    r3,    r0
    mov    r5,    16
    call   transpose8_internal
    RET
%else ;HIGH_BIT_DEPTH == 0
%if ARCH_X86_64 == 1
INIT_YMM avx2
cglobal transpose8, 3, 4, 4
    lea          r3, [r2 * 3]
    movq         xm0, [r1]
    movhps       xm0, [r1 + 2 * r2]
    movq         xm1, [r1 + r2]
    movhps       xm1, [r1 + r3]
    lea          r1, [r1 + 4 * r2]
    movq         xm2, [r1]
    movhps       xm2, [r1 + 2 * r2]
    movq         xm3, [r1 + r2]
    movhps       xm3, [r1 + r3]

    vinserti128  m0, m0, xm2, 1             ;[row1 row3 row5 row7]
    vinserti128  m1, m1, xm3, 1             ;[row2 row4 row6 row8]

    punpcklbw    m2, m0, m1                 ;[1 - 8; 1 - 8][row1row2; row5row6]
    punpckhbw    m0, m1                     ;[1 - 8; 1 - 8][row3row4; row7row8]

    punpcklwd    m1, m2, m0                 ;[1 - 4; 1 - 4][row1row2row3row4; row5row6row7row8]
    punpckhwd    m2, m0                     ;[5 - 8; 5 - 8][row1row2row3row4; row5row6row7row8]

    mova         m0, [trans8_shuf]

    vpermd       m1, m0, m1                 ;[1 - 2; 3 - 4][row1row2row3row4row5row6row7row8]
    vpermd       m2, m0, m2                 ;[4 - 5; 6 - 7][row1row2row3row4row5row6row7row8]

    movu         [r0], m1
    movu         [r0 + 32], m2
    RET
%endif

INIT_XMM sse2
cglobal transpose8, 3, 5, 8, dest, src, stride
    lea          r3,    [2 * r2]
    lea          r4,    [3 * r2]
    movh         m0,    [r1]
    movh         m1,    [r1 + r2]
    movh         m2,    [r1 + r3]
    movh         m3,    [r1 + r4]
    movh         m4,    [r1 + 4 * r2]
    lea          r1,    [r1 + 4 * r2]
    movh         m5,    [r1 + r2]
    movh         m6,    [r1 + r3]
    movh         m7,    [r1 + r4]

    punpcklbw    m0,    m1
    punpcklbw    m2,    m3
    punpcklbw    m4,    m5
    punpcklbw    m6,    m7

    punpckhwd    m1,    m0,    m2
    punpcklwd    m0,    m2
    punpckhwd    m5,    m4,    m6
    punpcklwd    m4,    m6
    punpckhdq    m2,    m0,    m4
    punpckldq    m0,    m4
    punpckhdq    m3,    m1,    m5
    punpckldq    m1,    m5

    movu         [r0],         m0
    movu         [r0 + 16],    m2
    movu         [r0 + 32],    m1
    movu         [r0 + 48],    m3
    RET
%endif

%macro TRANSPOSE_8x8 1

    movh         m0,    [r1]
    movh         m1,    [r1 + r2]
    movh         m2,    [r1 + 2 * r2]
    lea          r1,    [r1 + 2 * r2]
    movh         m3,    [r1 + r2]
    movh         m4,    [r1 + 2 * r2]
    lea          r1,    [r1 + 2 * r2]
    movh         m5,    [r1 + r2]
    movh         m6,    [r1 + 2 * r2]
    lea          r1,    [r1 + 2 * r2]
    movh         m7,    [r1 + r2]

    punpcklbw    m0,    m1
    punpcklbw    m2,    m3
    punpcklbw    m4,    m5
    punpcklbw    m6,    m7

    punpckhwd    m1,    m0,    m2
    punpcklwd    m0,    m2
    punpckhwd    m5,    m4,    m6
    punpcklwd    m4,    m6
    punpckhdq    m2,    m0,    m4
    punpckldq    m0,    m4
    punpckhdq    m3,    m1,    m5
    punpckldq    m1,    m5

    movh           [r0],             m0
    movhps         [r0 + %1],        m0
    movh           [r0 + 2 * %1],    m2
    lea            r0,               [r0 + 2 * %1]
    movhps         [r0 + %1],        m2
    movh           [r0 + 2 * %1],    m1
    lea            r0,               [r0 + 2 * %1]
    movhps         [r0 + %1],        m1
    movh           [r0 + 2 * %1],    m3
    lea            r0,               [r0 + 2 * %1]
    movhps         [r0 + %1],        m3

%endmacro


;-----------------------------------------------------------------
; void transpose_16x16(pixel *dst, pixel *src, intptr_t stride)
;-----------------------------------------------------------------
%if HIGH_BIT_DEPTH == 1
%if ARCH_X86_64 == 1
INIT_YMM avx2
cglobal transpose16x8_internal
    movu         m0, [r1]
    movu         m1, [r1 + r2]
    movu         m2, [r1 + 2 * r2]
    movu         m3, [r1 + r3]
    lea          r1, [r1 + 4 * r2]

    movu         m4, [r1]
    movu         m5, [r1 + r2]
    movu         m6, [r1 + 2 * r2]
    movu         m7, [r1 + r3]

    punpcklwd    m8, m0, m1                 ;[1 - 4; 9 - 12][1 2]
    punpckhwd    m0, m1                     ;[5 - 8; 13 -16][1 2]

    punpcklwd    m1, m2, m3                 ;[1 - 4; 9 - 12][3 4]
    punpckhwd    m2, m3                     ;[5 - 8; 13 -16][3 4]

    punpcklwd    m3, m4, m5                 ;[1 - 4; 9 - 12][5 6]
    punpckhwd    m4, m5                     ;[5 - 8; 13 -16][5 6]

    punpcklwd    m5, m6, m7                 ;[1 - 4; 9 - 12][7 8]
    punpckhwd    m6, m7                     ;[5 - 8; 13 -16][7 8]

    punpckldq    m7, m8, m1                 ;[1 - 2; 9 -  10][1 2 3 4]
    punpckhdq    m8, m1                     ;[3 - 4; 11 - 12][1 2 3 4]

    punpckldq    m1, m3, m5                 ;[1 - 2; 9 -  10][5 6 7 8]
    punpckhdq    m3, m5                     ;[3 - 4; 11 - 12][5 6 7 8]

    punpckldq    m5, m0, m2                 ;[5 - 6; 13 - 14][1 2 3 4]
    punpckhdq    m0, m2                     ;[7 - 8; 15 - 16][1 2 3 4]

    punpckldq    m2, m4, m6                 ;[5 - 6; 13 - 14][5 6 7 8]
    punpckhdq    m4, m6                     ;[7 - 8; 15 - 16][5 6 7 8]

    punpcklqdq   m6, m7, m1                 ;[1 ; 9 ][1 2 3 4 5 6 7 8]
    punpckhqdq   m7, m1                     ;[2 ; 10][1 2 3 4 5 6 7 8]

    punpcklqdq   m1, m8, m3                 ;[3 ; 11][1 2 3 4 5 6 7 8]
    punpckhqdq   m8, m3                     ;[4 ; 12][1 2 3 4 5 6 7 8]

    punpcklqdq   m3, m5, m2                 ;[5 ; 13][1 2 3 4 5 6 7 8]
    punpckhqdq   m5, m2                     ;[6 ; 14][1 2 3 4 5 6 7 8]

    punpcklqdq   m2, m0, m4                 ;[7 ; 15][1 2 3 4 5 6 7 8]
    punpckhqdq   m0, m4                     ;[8 ; 16][1 2 3 4 5 6 7 8]

    movu         [r0 + 0 * 32], xm6
    vextracti128 [r0 + 8 * 32], m6, 1
    movu         [r0 + 1  * 32], xm7
    vextracti128 [r0 + 9  * 32], m7, 1
    movu         [r0 + 2  * 32], xm1
    vextracti128 [r0 + 10 * 32], m1, 1
    movu         [r0 + 3  * 32], xm8
    vextracti128 [r0 + 11 * 32], m8, 1
    movu         [r0 + 4  * 32], xm3
    vextracti128 [r0 + 12 * 32], m3, 1
    movu         [r0 + 5  * 32], xm5
    vextracti128 [r0 + 13 * 32], m5, 1
    movu         [r0 + 6  * 32], xm2
    vextracti128 [r0 + 14 * 32], m2, 1
    movu         [r0 + 7  * 32], xm0
    vextracti128 [r0 + 15 * 32], m0, 1
    ret

cglobal transpose16, 3, 4, 9
    add          r2, r2
    lea          r3, [r2 * 3]
    call         transpose16x8_internal
    lea          r1, [r1 + 4 * r2]
    add          r0, 16
    call         transpose16x8_internal
    RET
%endif
INIT_XMM sse2
cglobal transpose16, 3, 7, 4, dest, src, stride
    add    r2,    r2
    mov    r3,    r0
    mov    r4,    r1
    mov    r5,    32
    mov    r6,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r4 + 16]
    lea    r0,    [r6 + 8 * r5]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * r5 + 16]
    mov    r3,    r0
    call   transpose8_internal
    RET
%else ;HIGH_BIT_DEPTH == 0
%if ARCH_X86_64 == 1
INIT_YMM avx2
cglobal transpose16, 3, 5, 9
    lea          r3, [r2 * 3]
    lea          r4, [r1 + 8 * r2]

    movu         xm0, [r1]
    movu         xm1, [r1 + r2]
    movu         xm2, [r1 + 2 * r2]
    movu         xm3, [r1 + r3]
    vinserti128  m0,  m0, [r4], 1
    vinserti128  m1,  m1, [r4 + r2], 1
    vinserti128  m2,  m2, [r4 + 2 * r2], 1
    vinserti128  m3,  m3, [r4 + r3], 1
    lea          r1,  [r1 + 4 * r2]
    lea          r4,  [r4 + 4 * r2]

    movu         xm4, [r1]
    movu         xm5, [r1 + r2]
    movu         xm6, [r1 + 2 * r2]
    movu         xm7, [r1 + r3]
    vinserti128  m4,  m4, [r4], 1
    vinserti128  m5,  m5, [r4 + r2], 1
    vinserti128  m6,  m6, [r4 + 2 * r2], 1
    vinserti128  m7,  m7, [r4 + r3], 1

    punpcklbw    m8, m0, m1                 ;[1 - 8 ; 1 - 8 ][1 2  9 10]
    punpckhbw    m0, m1                     ;[9 - 16; 9 - 16][1 2  9 10]

    punpcklbw    m1, m2, m3                 ;[1 - 8 ; 1 - 8 ][3 4 11 12]
    punpckhbw    m2, m3                     ;[9 - 16; 9 - 16][3 4 11 12]

    punpcklbw    m3, m4, m5                 ;[1 - 8 ; 1 - 8 ][5 6 13 14]
    punpckhbw    m4, m5                     ;[9 - 16; 9 - 16][5 6 13 14]

    punpcklbw    m5, m6, m7                 ;[1 - 8 ; 1 - 8 ][7 8 15 16]
    punpckhbw    m6, m7                     ;[9 - 16; 9 - 16][7 8 15 16]

    punpcklwd    m7, m8, m1                 ;[1 - 4 ; 1 - 4][1 2 3 4 9 10 11 12]
    punpckhwd    m8, m1                     ;[5 - 8 ; 5 - 8][1 2 3 4 9 10 11 12]

    punpcklwd    m1, m3, m5                 ;[1 - 4 ; 1 - 4][5 6 7 8 13 14 15 16]
    punpckhwd    m3, m5                     ;[5 - 8 ; 5 - 8][5 6 7 8 13 14 15 16]

    punpcklwd    m5, m0, m2                 ;[9 - 12; 9 -  12][1 2 3 4 9 10 11 12]
    punpckhwd    m0, m2                     ;[13- 16; 13 - 16][1 2 3 4 9 10 11 12]

    punpcklwd    m2, m4, m6                 ;[9 - 12; 9 -  12][5 6 7 8 13 14 15 16]
    punpckhwd    m4, m6                     ;[13- 16; 13 - 16][5 6 7 8 13 14 15 16]

    punpckldq    m6, m7, m1                 ;[1 - 2 ; 1 - 2][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhdq    m7, m1                     ;[3 - 4 ; 3 - 4][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpckldq    m1, m8, m3                 ;[5 - 6 ; 5 - 6][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhdq    m8, m3                     ;[7 - 8 ; 7 - 8][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpckldq    m3, m5, m2                 ;[9 - 10; 9 -  10][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhdq    m5, m2                     ;[11- 12; 11 - 12][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpckldq    m2, m0, m4                 ;[13- 14; 13 - 14][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhdq    m0, m4                     ;[15- 16; 15 - 16][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    vpermq       m6, m6, 0xD8
    vpermq       m7, m7, 0xD8
    vpermq       m1, m1, 0xD8
    vpermq       m8, m8, 0xD8
    vpermq       m3, m3, 0xD8
    vpermq       m5, m5, 0xD8
    vpermq       m2, m2, 0xD8
    vpermq       m0, m0, 0xD8

    movu         [r0 + 0 *  16], m6
    movu         [r0 + 2 *  16], m7
    movu         [r0 + 4 *  16], m1
    movu         [r0 + 6 *  16], m8
    movu         [r0 + 8 *  16], m3
    movu         [r0 + 10 * 16], m5
    movu         [r0 + 12 * 16], m2
    movu         [r0 + 14 * 16], m0
    RET
%endif
INIT_XMM sse2
cglobal transpose16, 3, 5, 8, dest, src, stride
    mov    r3,    r0
    mov    r4,    r1
    TRANSPOSE_8x8 16
    lea    r1,    [r1 + 2 * r2]
    lea    r0,    [r3 + 8]
    TRANSPOSE_8x8 16
    lea    r1,    [r4 + 8]
    lea    r0,    [r3 + 8 * 16]
    TRANSPOSE_8x8 16
    lea    r1,    [r1 + 2 * r2]
    lea    r0,    [r3 + 8 * 16 + 8]
    TRANSPOSE_8x8 16
    RET
%endif

cglobal transpose16_internal
    TRANSPOSE_8x8 r6
    lea    r1,    [r1 + 2 * r2]
    lea    r0,    [r5 + 8]
    TRANSPOSE_8x8 r6
    lea    r1,    [r1 + 2 * r2]
    neg    r2
    lea    r1,    [r1 + r2 * 8]
    lea    r1,    [r1 + r2 * 8 + 8]
    neg    r2
    lea    r0,    [r5 + 8 * r6]
    TRANSPOSE_8x8 r6
    lea    r1,    [r1 + 2 * r2]
    lea    r0,    [r5 + 8 * r6 + 8]
    TRANSPOSE_8x8 r6
    ret

;-----------------------------------------------------------------
; void transpose_32x32(pixel *dst, pixel *src, intptr_t stride)
;-----------------------------------------------------------------
%if HIGH_BIT_DEPTH == 1
%if ARCH_X86_64 == 1
INIT_YMM avx2
cglobal transpose8x32_internal
    movu         m0, [r1]
    movu         m1, [r1 + 32]
    movu         m2, [r1 + r2]
    movu         m3, [r1 + r2 + 32]
    movu         m4, [r1 + 2 * r2]
    movu         m5, [r1 + 2 * r2 + 32]
    movu         m6, [r1 + r3]
    movu         m7, [r1 + r3 + 32]
    lea          r1, [r1 + 4 * r2]

    punpcklwd    m8, m0, m2               ;[1 - 4;  9 - 12][1 2]
    punpckhwd    m0, m2                   ;[5 - 8; 13 - 16][1 2]

    punpcklwd    m2, m4, m6               ;[1 - 4;  9 - 12][3 4]
    punpckhwd    m4, m6                   ;[5 - 8; 13 - 16][3 4]

    punpcklwd    m6, m1, m3               ;[17 - 20; 25 - 28][1 2]
    punpckhwd    m1, m3                   ;[21 - 24; 29 - 32][1 2]

    punpcklwd    m3, m5, m7               ;[17 - 20; 25 - 28][3 4]
    punpckhwd    m5, m7                   ;[21 - 24; 29 - 32][3 4]

    punpckldq    m7, m8, m2               ;[1 - 2;  9 - 10][1 2 3 4]
    punpckhdq    m8, m2                   ;[3 - 4; 11 - 12][1 2 3 4]

    punpckldq    m2, m0, m4               ;[5 - 6; 13 - 14][1 2 3 4]
    punpckhdq    m0, m4                   ;[7 - 8; 15 - 16][1 2 3 4]

    punpckldq    m4, m6, m3               ;[17 - 18; 25 - 26][1 2 3 4]
    punpckhdq    m6, m3                   ;[19 - 20; 27 - 28][1 2 3 4]

    punpckldq    m3, m1, m5               ;[21 - 22; 29 - 30][1 2 3 4]
    punpckhdq    m1, m5                   ;[23 - 24; 31 - 32][1 2 3 4]

    movq         [r0 + 0  * 64], xm7
    movhps       [r0 + 1  * 64], xm7
    vextracti128 xm5, m7, 1
    movq         [r0 + 8 * 64], xm5
    movhps       [r0 + 9 * 64], xm5

    movu         m7,  [r1]
    movu         m9,  [r1 + 32]
    movu         m10, [r1 + r2]
    movu         m11, [r1 + r2 + 32]
    movu         m12, [r1 + 2 * r2]
    movu         m13, [r1 + 2 * r2 + 32]
    movu         m14, [r1 + r3]
    movu         m15, [r1 + r3 + 32]

    punpcklwd    m5, m7, m10              ;[1 - 4;  9 - 12][5 6]
    punpckhwd    m7, m10                  ;[5 - 8; 13 - 16][5 6]

    punpcklwd    m10, m12, m14            ;[1 - 4;  9 - 12][7 8]
    punpckhwd    m12, m14                 ;[5 - 8; 13 - 16][7 8]

    punpcklwd    m14, m9, m11             ;[17 - 20; 25 - 28][5 6]
    punpckhwd    m9, m11                  ;[21 - 24; 29 - 32][5 6]

    punpcklwd    m11, m13, m15            ;[17 - 20; 25 - 28][7 8]
    punpckhwd    m13, m15                 ;[21 - 24; 29 - 32][7 8]

    punpckldq    m15, m5, m10             ;[1 - 2;  9 - 10][5 6 7 8]
    punpckhdq    m5, m10                  ;[3 - 4; 11 - 12][5 6 7 8]

    punpckldq    m10, m7, m12             ;[5 - 6; 13 - 14][5 6 7 8]
    punpckhdq    m7, m12                  ;[7 - 8; 15 - 16][5 6 7 8]

    punpckldq    m12, m14, m11            ;[17 - 18; 25 - 26][5 6 7 8]
    punpckhdq    m14, m11                 ;[19 - 20; 27 - 28][5 6 7 8]

    punpckldq    m11, m9, m13             ;[21 - 22; 29 - 30][5 6 7 8]
    punpckhdq    m9, m13                  ;[23 - 24; 31 - 32][5 6 7 8]

    movq         [r0 + 0 * 64 + 8], xm15
    movhps       [r0 + 1 * 64 + 8], xm15
    vextracti128 xm13, m15, 1
    movq         [r0 + 8 * 64 + 8], xm13
    movhps       [r0 + 9 * 64 + 8], xm13

    punpcklqdq   m13, m8, m5              ;[3 ; 11][1 2 3 4 5 6 7 8]
    punpckhqdq   m8, m5                   ;[4 ; 12][1 2 3 4 5 6 7 8]

    punpcklqdq   m5, m2, m10              ;[5 ; 13][1 2 3 4 5 6 7 8]
    punpckhqdq   m2, m10                  ;[6 ; 14][1 2 3 4 5 6 7 8]

    punpcklqdq   m10, m0, m7              ;[7 ; 15][1 2 3 4 5 6 7 8]
    punpckhqdq   m0, m7                   ;[8 ; 16][1 2 3 4 5 6 7 8]

    punpcklqdq   m7, m4, m12              ;[17 ; 25][1 2 3 4 5 6 7 8]
    punpckhqdq   m4, m12                  ;[18 ; 26][1 2 3 4 5 6 7 8]

    punpcklqdq   m12, m6, m14             ;[19 ; 27][1 2 3 4 5 6 7 8]
    punpckhqdq   m6, m14                  ;[20 ; 28][1 2 3 4 5 6 7 8]

    punpcklqdq   m14, m3, m11             ;[21 ; 29][1 2 3 4 5 6 7 8]
    punpckhqdq   m3, m11                  ;[22 ; 30][1 2 3 4 5 6 7 8]

    punpcklqdq   m11, m1, m9              ;[23 ; 31][1 2 3 4 5 6 7 8]
    punpckhqdq   m1, m9                   ;[24 ; 32][1 2 3 4 5 6 7 8]

    movu         [r0 + 2  * 64], xm13
    vextracti128 [r0 + 10 * 64], m13, 1

    movu         [r0 + 3  * 64], xm8
    vextracti128 [r0 + 11 * 64], m8, 1

    movu         [r0 + 4  * 64], xm5
    vextracti128 [r0 + 12 * 64], m5, 1

    movu         [r0 + 5  * 64], xm2
    vextracti128 [r0 + 13 * 64], m2, 1

    movu         [r0 + 6  * 64], xm10
    vextracti128 [r0 + 14 * 64], m10, 1

    movu         [r0 + 7  * 64], xm0
    vextracti128 [r0 + 15 * 64], m0, 1

    movu         [r0 + 16 * 64], xm7
    vextracti128 [r0 + 24 * 64], m7, 1

    movu         [r0 + 17 * 64], xm4
    vextracti128 [r0 + 25 * 64], m4, 1

    movu         [r0 + 18 * 64], xm12
    vextracti128 [r0 + 26 * 64], m12, 1

    movu         [r0 + 19 * 64], xm6
    vextracti128 [r0 + 27 * 64], m6, 1

    movu         [r0 + 20 * 64], xm14
    vextracti128 [r0 + 28 * 64], m14, 1

    movu         [r0 + 21 * 64], xm3
    vextracti128 [r0 + 29 * 64], m3, 1

    movu         [r0 + 22 * 64], xm11
    vextracti128 [r0 + 30 * 64], m11, 1

    movu         [r0 + 23 * 64], xm1
    vextracti128 [r0 + 31 * 64], m1, 1
    ret

cglobal transpose32, 3, 4, 16
    add          r2, r2
    lea          r3, [r2 * 3]
    call         transpose8x32_internal
    add          r0, 16
    lea          r1, [r1 + 4 * r2]
    call         transpose8x32_internal
    add          r0, 16
    lea          r1, [r1 + 4 * r2]
    call         transpose8x32_internal
    add          r0, 16
    lea          r1, [r1 + 4 * r2]
    call         transpose8x32_internal
    RET
%endif
INIT_XMM sse2
cglobal transpose32, 3, 7, 4, dest, src, stride
    add    r2,    r2
    mov    r3,    r0
    mov    r4,    r1
    mov    r5,    64
    mov    r6,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r4 + 16]
    lea    r0,    [r6 + 8 * 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 64 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 64 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 64 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r4 + 32]
    lea    r0,    [r6 + 16 * 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 64 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 64 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 64 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r4 + 48]
    lea    r0,    [r6 + 24 * 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 64 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 64 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 64 + 48]
    mov    r3,    r0
    call   transpose8_internal
    RET
%else ;HIGH_BIT_DEPTH == 0
INIT_XMM sse2
cglobal transpose32, 3, 7, 8, dest, src, stride
    mov    r3,    r0
    mov    r4,    r1
    mov    r5,    r0
    mov    r6,    32
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 16]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r4 + 16]
    lea    r0,    [r3 + 16 * 32]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 16 * 32 + 16]
    mov    r5,    r0
    call   transpose16_internal
    RET

%if ARCH_X86_64 == 1
INIT_YMM avx2
cglobal transpose32, 3, 5, 16
    lea          r3, [r2 * 3]
    mov          r4d, 2

.loop:
    movu         m0, [r1]
    movu         m1, [r1 + r2]
    movu         m2, [r1 + 2 * r2]
    movu         m3, [r1 + r3]
    lea          r1, [r1 + 4 * r2]

    movu         m4, [r1]
    movu         m5, [r1 + r2]
    movu         m6, [r1 + 2 * r2]
    movu         m7, [r1 + r3]

    punpcklbw    m8, m0, m1                 ;[1 - 8 ; 17 - 24][1 2]
    punpckhbw    m0, m1                     ;[9 - 16; 25 - 32][1 2]

    punpcklbw    m1, m2, m3                 ;[1 - 8 ; 17 - 24][3 4]
    punpckhbw    m2, m3                     ;[9 - 16; 25 - 32][3 4]

    punpcklbw    m3, m4, m5                 ;[1 - 8 ; 17 - 24][5 6]
    punpckhbw    m4, m5                     ;[9 - 16; 25 - 32][5 6]

    punpcklbw    m5, m6, m7                 ;[1 - 8 ; 17 - 24][7 8]
    punpckhbw    m6, m7                     ;[9 - 16; 25 - 32][7 8]

    punpcklwd    m7, m8, m1                 ;[1 - 4 ; 17 - 20][1 2 3 4]
    punpckhwd    m8, m1                     ;[5 - 8 ; 20 - 24][1 2 3 4]

    punpcklwd    m1, m3, m5                 ;[1 - 4 ; 17 - 20][5 6 7 8]
    punpckhwd    m3, m5                     ;[5 - 8 ; 20 - 24][5 6 7 8]

    punpcklwd    m5, m0, m2                 ;[9 - 12; 25 - 28][1 2 3 4]
    punpckhwd    m0, m2                     ;[13- 15; 29 - 32][1 2 3 4]

    punpcklwd    m2, m4, m6                 ;[9 - 12; 25 - 28][5 6 7 8]
    punpckhwd    m4, m6                     ;[13- 15; 29 - 32][5 6 7 8]

    punpckldq    m6, m7, m1                 ;[1 - 2 ; 17 - 18][1 2 3 4 5 6 7 8]
    punpckhdq    m7, m1                     ;[3 - 4 ; 19 - 20][1 2 3 4 5 6 7 8]

    punpckldq    m1, m8, m3                 ;[5 - 6 ; 21 - 22][1 2 3 4 5 6 7 8]
    punpckhdq    m8, m3                     ;[7 - 8 ; 23 - 24][1 2 3 4 5 6 7 8]

    punpckldq    m3, m5, m2                 ;[9 - 10; 25 - 26][1 2 3 4 5 6 7 8]
    punpckhdq    m5, m2                     ;[11- 12; 27 - 28][1 2 3 4 5 6 7 8]

    punpckldq    m2, m0, m4                 ;[13- 14; 29 - 30][1 2 3 4 5 6 7 8]
    punpckhdq    m0, m4                     ;[15- 16; 31 - 32][1 2 3 4 5 6 7 8]

    movq         [r0 + 0  * 32], xm6
    movhps       [r0 + 1  * 32], xm6
    vextracti128 xm4, m6, 1
    movq         [r0 + 16 * 32], xm4
    movhps       [r0 + 17 * 32], xm4

    lea          r1,  [r1 + 4 * r2]
    movu         m9,  [r1]
    movu         m10, [r1 + r2]
    movu         m11, [r1 + 2 * r2]
    movu         m12, [r1 + r3]
    lea          r1,  [r1 + 4 * r2]

    movu         m13, [r1]
    movu         m14, [r1 + r2]
    movu         m15, [r1 + 2 * r2]
    movu         m6,  [r1 + r3]

    punpcklbw    m4, m9, m10                ;[1 - 8 ; 17 - 24][9 10]
    punpckhbw    m9, m10                    ;[9 - 16; 25 - 32][9 10]

    punpcklbw    m10, m11, m12              ;[1 - 8 ; 17 - 24][11 12]
    punpckhbw    m11, m12                   ;[9 - 16; 25 - 32][11 12]

    punpcklbw    m12, m13, m14              ;[1 - 8 ; 17 - 24][13 14]
    punpckhbw    m13, m14                   ;[9 - 16; 25 - 32][13 14]

    punpcklbw    m14, m15, m6               ;[1 - 8 ; 17 - 24][15 16]
    punpckhbw    m15, m6                    ;[9 - 16; 25 - 32][15 16]

    punpcklwd    m6, m4, m10                ;[1 - 4 ; 17 - 20][9 10 11 12]
    punpckhwd    m4, m10                    ;[5 - 8 ; 20 - 24][9 10 11 12]

    punpcklwd    m10, m12, m14              ;[1 - 4 ; 17 - 20][13 14 15 16]
    punpckhwd    m12, m14                   ;[5 - 8 ; 20 - 24][13 14 15 16]

    punpcklwd    m14, m9, m11               ;[9 - 12; 25 - 28][9 10 11 12]
    punpckhwd    m9, m11                    ;[13- 16; 29 - 32][9 10 11 12]

    punpcklwd    m11, m13, m15              ;[9 - 12; 25 - 28][13 14 15 16]
    punpckhwd    m13, m15                   ;[13- 16; 29 - 32][13 14 15 16]

    punpckldq    m15, m6, m10               ;[1 - 2 ; 17 - 18][9 10 11 12 13 14 15 16]
    punpckhdq    m6, m10                    ;[3 - 4 ; 19 - 20][9 10 11 12 13 14 15 16]

    punpckldq    m10, m4, m12               ;[5 - 6 ; 21 - 22][9 10 11 12 13 14 15 16]
    punpckhdq    m4, m12                    ;[7 - 8 ; 23 - 24][9 10 11 12 13 14 15 16]

    punpckldq    m12, m14, m11              ;[9 - 10; 25 - 26][9 10 11 12 13 14 15 16]
    punpckhdq    m14, m11                   ;[11- 12; 27 - 28][9 10 11 12 13 14 15 16]

    punpckldq    m11, m9, m13               ;[13- 14; 29 - 30][9 10 11 12 13 14 15 16]
    punpckhdq    m9, m13                    ;[15- 16; 31 - 32][9 10 11 12 13 14 15 16]


    punpcklqdq   m13, m7, m6                ;[3 ; 19][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m7, m6                     ;[4 ; 20][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m6, m1, m10                ;[5 ; 21][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m1, m10                    ;[6 ; 22][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m10, m8, m4                ;[7 ; 23][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m8, m4                     ;[8 ; 24][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m4, m3, m12                ;[9 ; 25][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m3, m12                    ;[10; 26][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m12, m5, m14               ;[11; 27][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m5, m14                    ;[12; 28][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m14, m2, m11               ;[13; 29][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m2, m11                    ;[14; 30][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m11, m0, m9                ;[15; 31][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m0, m9                     ;[16; 32][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    movq         [r0 + 0  * 32 + 8], xm15
    movhps       [r0 + 1  * 32 + 8], xm15
    vextracti128 xm9, m15, 1
    movq         [r0 + 16 * 32 + 8], xm9
    movhps       [r0 + 17 * 32 + 8], xm9

    movu         [r0 + 2  * 32], xm13
    vextracti128 [r0 + 18 * 32], m13, 1

    movu         [r0 + 3  * 32], xm7
    vextracti128 [r0 + 19 * 32], m7, 1

    movu         [r0 + 4  * 32], xm6
    vextracti128 [r0 + 20 * 32], m6, 1

    movu         [r0 + 5  * 32], xm1
    vextracti128 [r0 + 21 * 32], m1, 1

    movu         [r0 + 6  * 32], xm10
    vextracti128 [r0 + 22 * 32], m10, 1

    movu         [r0 + 7  * 32], xm8
    vextracti128 [r0 + 23 * 32], m8, 1

    movu         [r0 + 8  * 32], xm4
    vextracti128 [r0 + 24 * 32], m4, 1

    movu         [r0 + 9  * 32], xm3
    vextracti128 [r0 + 25 * 32], m3, 1

    movu         [r0 + 10 * 32], xm12
    vextracti128 [r0 + 26 * 32], m12, 1

    movu         [r0 + 11 * 32], xm5
    vextracti128 [r0 + 27 * 32], m5, 1

    movu         [r0 + 12 * 32], xm14
    vextracti128 [r0 + 28 * 32], m14, 1

    movu         [r0 + 13 * 32], xm2
    vextracti128 [r0 + 29 * 32], m2, 1

    movu         [r0 + 14 * 32], xm11
    vextracti128 [r0 + 30 * 32], m11, 1

    movu         [r0 + 15 * 32], xm0
    vextracti128 [r0 + 31 * 32], m0, 1

    add          r0, 16
    lea          r1,  [r1 + 4 * r2]
    dec          r4d
    jnz          .loop
    RET
%endif
%endif

;-----------------------------------------------------------------
; void transpose_64x64(pixel *dst, pixel *src, intptr_t stride)
;-----------------------------------------------------------------
%if HIGH_BIT_DEPTH == 1
%if ARCH_X86_64 == 1
INIT_YMM avx2
cglobal transpose8x32_64_internal
    movu         m0, [r1]
    movu         m1, [r1 + 32]
    movu         m2, [r1 + r2]
    movu         m3, [r1 + r2 + 32]
    movu         m4, [r1 + 2 * r2]
    movu         m5, [r1 + 2 * r2 + 32]
    movu         m6, [r1 + r3]
    movu         m7, [r1 + r3 + 32]
    lea          r1, [r1 + 4 * r2]

    punpcklwd    m8, m0, m2               ;[1 - 4;  9 - 12][1 2]
    punpckhwd    m0, m2                   ;[5 - 8; 13 - 16][1 2]

    punpcklwd    m2, m4, m6               ;[1 - 4;  9 - 12][3 4]
    punpckhwd    m4, m6                   ;[5 - 8; 13 - 16][3 4]

    punpcklwd    m6, m1, m3               ;[17 - 20; 25 - 28][1 2]
    punpckhwd    m1, m3                   ;[21 - 24; 29 - 32][1 2]

    punpcklwd    m3, m5, m7               ;[17 - 20; 25 - 28][3 4]
    punpckhwd    m5, m7                   ;[21 - 24; 29 - 32][3 4]

    punpckldq    m7, m8, m2               ;[1 - 2;  9 - 10][1 2 3 4]
    punpckhdq    m8, m2                   ;[3 - 4; 11 - 12][1 2 3 4]

    punpckldq    m2, m0, m4               ;[5 - 6; 13 - 14][1 2 3 4]
    punpckhdq    m0, m4                   ;[7 - 8; 15 - 16][1 2 3 4]

    punpckldq    m4, m6, m3               ;[17 - 18; 25 - 26][1 2 3 4]
    punpckhdq    m6, m3                   ;[19 - 20; 27 - 28][1 2 3 4]

    punpckldq    m3, m1, m5               ;[21 - 22; 29 - 30][1 2 3 4]
    punpckhdq    m1, m5                   ;[23 - 24; 31 - 32][1 2 3 4]

    movq         [r0 + 0  * 128], xm7
    movhps       [r0 + 1  * 128], xm7
    vextracti128 xm5, m7, 1
    movq         [r0 + 8 * 128], xm5
    movhps       [r0 + 9 * 128], xm5

    movu         m7,  [r1]
    movu         m9,  [r1 + 32]
    movu         m10, [r1 + r2]
    movu         m11, [r1 + r2 + 32]
    movu         m12, [r1 + 2 * r2]
    movu         m13, [r1 + 2 * r2 + 32]
    movu         m14, [r1 + r3]
    movu         m15, [r1 + r3 + 32]

    punpcklwd    m5, m7, m10              ;[1 - 4;  9 - 12][5 6]
    punpckhwd    m7, m10                  ;[5 - 8; 13 - 16][5 6]

    punpcklwd    m10, m12, m14            ;[1 - 4;  9 - 12][7 8]
    punpckhwd    m12, m14                 ;[5 - 8; 13 - 16][7 8]

    punpcklwd    m14, m9, m11             ;[17 - 20; 25 - 28][5 6]
    punpckhwd    m9, m11                  ;[21 - 24; 29 - 32][5 6]

    punpcklwd    m11, m13, m15            ;[17 - 20; 25 - 28][7 8]
    punpckhwd    m13, m15                 ;[21 - 24; 29 - 32][7 8]

    punpckldq    m15, m5, m10             ;[1 - 2;  9 - 10][5 6 7 8]
    punpckhdq    m5, m10                  ;[3 - 4; 11 - 12][5 6 7 8]

    punpckldq    m10, m7, m12             ;[5 - 6; 13 - 14][5 6 7 8]
    punpckhdq    m7, m12                  ;[7 - 8; 15 - 16][5 6 7 8]

    punpckldq    m12, m14, m11            ;[17 - 18; 25 - 26][5 6 7 8]
    punpckhdq    m14, m11                 ;[19 - 20; 27 - 28][5 6 7 8]

    punpckldq    m11, m9, m13             ;[21 - 22; 29 - 30][5 6 7 8]
    punpckhdq    m9, m13                  ;[23 - 24; 31 - 32][5 6 7 8]

    movq         [r0 + 0 * 128 + 8], xm15
    movhps       [r0 + 1 * 128 + 8], xm15
    vextracti128 xm13, m15, 1
    movq         [r0 + 8 * 128 + 8], xm13
    movhps       [r0 + 9 * 128 + 8], xm13

    punpcklqdq   m13, m8, m5              ;[3 ; 11][1 2 3 4 5 6 7 8]
    punpckhqdq   m8, m5                   ;[4 ; 12][1 2 3 4 5 6 7 8]

    punpcklqdq   m5, m2, m10              ;[5 ; 13][1 2 3 4 5 6 7 8]
    punpckhqdq   m2, m10                  ;[6 ; 14][1 2 3 4 5 6 7 8]

    punpcklqdq   m10, m0, m7              ;[7 ; 15][1 2 3 4 5 6 7 8]
    punpckhqdq   m0, m7                   ;[8 ; 16][1 2 3 4 5 6 7 8]

    punpcklqdq   m7, m4, m12              ;[17 ; 25][1 2 3 4 5 6 7 8]
    punpckhqdq   m4, m12                  ;[18 ; 26][1 2 3 4 5 6 7 8]

    punpcklqdq   m12, m6, m14             ;[19 ; 27][1 2 3 4 5 6 7 8]
    punpckhqdq   m6, m14                  ;[20 ; 28][1 2 3 4 5 6 7 8]

    punpcklqdq   m14, m3, m11             ;[21 ; 29][1 2 3 4 5 6 7 8]
    punpckhqdq   m3, m11                  ;[22 ; 30][1 2 3 4 5 6 7 8]

    punpcklqdq   m11, m1, m9              ;[23 ; 31][1 2 3 4 5 6 7 8]
    punpckhqdq   m1, m9                   ;[24 ; 32][1 2 3 4 5 6 7 8]

    movu         [r0 + 2  * 128], xm13
    vextracti128 [r0 + 10 * 128], m13, 1

    movu         [r0 + 3  * 128], xm8
    vextracti128 [r0 + 11 * 128], m8, 1

    movu         [r0 + 4  * 128], xm5
    vextracti128 [r0 + 12 * 128], m5, 1

    movu         [r0 + 5  * 128], xm2
    vextracti128 [r0 + 13 * 128], m2, 1

    movu         [r0 + 6  * 128], xm10
    vextracti128 [r0 + 14 * 128], m10, 1

    movu         [r0 + 7  * 128], xm0
    vextracti128 [r0 + 15 * 128], m0, 1

    movu         [r0 + 16 * 128], xm7
    vextracti128 [r0 + 24 * 128], m7, 1

    movu         [r0 + 17 * 128], xm4
    vextracti128 [r0 + 25 * 128], m4, 1

    movu         [r0 + 18 * 128], xm12
    vextracti128 [r0 + 26 * 128], m12, 1

    movu         [r0 + 19 * 128], xm6
    vextracti128 [r0 + 27 * 128], m6, 1

    movu         [r0 + 20 * 128], xm14
    vextracti128 [r0 + 28 * 128], m14, 1

    movu         [r0 + 21 * 128], xm3
    vextracti128 [r0 + 29 * 128], m3, 1

    movu         [r0 + 22 * 128], xm11
    vextracti128 [r0 + 30 * 128], m11, 1

    movu         [r0 + 23 * 128], xm1
    vextracti128 [r0 + 31 * 128], m1, 1
    ret

cglobal transpose64, 3, 6, 16
    add          r2, r2
    lea          r3, [3 * r2]
    lea          r4, [r1 + 64]
    lea          r5, [r0 + 16]

    call         transpose8x32_64_internal
    mov          r1, r4
    lea          r0, [r0 + 32 * 128]
    call         transpose8x32_64_internal
    mov          r0, r5
    lea          r5, [r0 + 16]
    lea          r4, [r1 + 4 * r2]
    lea          r1, [r4 - 64]
    call         transpose8x32_64_internal
    mov          r1, r4
    lea          r0, [r0 + 32 * 128]
    call         transpose8x32_64_internal
    mov          r0, r5
    lea          r5, [r0 + 16]
    lea          r4, [r1 + 4 * r2]
    lea          r1, [r4 - 64]
    call         transpose8x32_64_internal
    mov          r1, r4
    lea          r0, [r0 + 32 * 128]
    call         transpose8x32_64_internal
    mov          r0, r5
    lea          r5, [r0 + 16]
    lea          r4, [r1 + 4 * r2]
    lea          r1, [r4 - 64]
    call         transpose8x32_64_internal
    mov          r1, r4
    lea          r0, [r0 + 32 * 128]
    call         transpose8x32_64_internal
    mov          r0, r5
    lea          r5, [r0 + 16]
    lea          r4, [r1 + 4 * r2]
    lea          r1, [r4 - 64]
    call         transpose8x32_64_internal
    mov          r1, r4
    lea          r0, [r0 + 32 * 128]
    call         transpose8x32_64_internal
    mov          r0, r5
    lea          r5, [r0 + 16]
    lea          r4, [r1 + 4 * r2]
    lea          r1, [r4 - 64]
    call         transpose8x32_64_internal
    mov          r1, r4
    lea          r0, [r0 + 32 * 128]
    call         transpose8x32_64_internal
    mov          r0, r5
    lea          r5, [r0 + 16]
    lea          r4, [r1 + 4 * r2]
    lea          r1, [r4 - 64]
    call         transpose8x32_64_internal
    mov          r1, r4
    lea          r0, [r0 + 32 * 128]
    call         transpose8x32_64_internal
    mov          r0, r5
    lea          r4, [r1 + 4 * r2]
    lea          r1, [r4 - 64]
    call         transpose8x32_64_internal
    mov          r1, r4
    lea          r0, [r0 + 32 * 128]
    call         transpose8x32_64_internal
    RET
%endif
INIT_XMM sse2
cglobal transpose64, 3, 7, 4, dest, src, stride
    add    r2,    r2
    mov    r3,    r0
    mov    r4,    r1
    mov    r5,    128
    mov    r6,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 80]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 96]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 112]
    mov    r3,    r0
    call   transpose8_internal

    lea    r1,    [r4 + 16]
    lea    r0,    [r6 + 8 * 128]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 128 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 128 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 128 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 128 + 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 128 + 80]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 128 + 96]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 8 * 128 + 112]
    mov    r3,    r0
    call   transpose8_internal

    lea    r1,    [r4 + 32]
    lea    r0,    [r6 + 16 * 128]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 128 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 128 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 128 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 128 + 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 128 + 80]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 128 + 96]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 16 * 128 + 112]
    mov    r3,    r0
    call   transpose8_internal

    lea    r1,    [r4 + 48]
    lea    r0,    [r6 + 24 * 128]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 128 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 128 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 128 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 128 + 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 128 + 80]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 128 + 96]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 24 * 128 + 112]
    mov    r3,    r0
    call   transpose8_internal

    lea    r1,    [r4 + 64]
    lea    r0,    [r6 + 32 * 128]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32 * 128 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32 * 128 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32 * 128 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32 * 128 + 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32 * 128 + 80]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32 * 128 + 96]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 32 * 128 + 112]
    mov    r3,    r0
    call   transpose8_internal

    lea    r1,    [r4 + 80]
    lea    r0,    [r6 + 40 * 128]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 40 * 128 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 40 * 128 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 40 * 128 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 40 * 128 + 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 40 * 128 + 80]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 40 * 128 + 96]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 40 * 128 + 112]
    mov    r3,    r0
    call   transpose8_internal

    lea    r1,    [r4 + 96]
    lea    r0,    [r6 + 48 * 128]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48 * 128 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48 * 128 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48 * 128 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48 * 128 + 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48 * 128 + 80]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48 * 128 + 96]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 48 * 128 + 112]
    mov    r3,    r0
    call   transpose8_internal

    lea    r1,    [r4 + 112]
    lea    r0,    [r6 + 56 * 128]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 56 * 128 + 16]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 56 * 128 + 32]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 56 * 128 + 48]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 56 * 128 + 64]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 56 * 128 + 80]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 56 * 128 + 96]
    mov    r3,    r0
    call   transpose8_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r6 + 56 * 128 + 112]
    mov    r3,    r0
    call   transpose8_internal
    RET
%else ;HIGH_BIT_DEPTH == 0
%if ARCH_X86_64 == 1
INIT_YMM avx2

cglobal transpose16x32_avx2
    movu         m0, [r1]
    movu         m1, [r1 + r2]
    movu         m2, [r1 + 2 * r2]
    movu         m3, [r1 + r3]
    lea          r1, [r1 + 4 * r2]

    movu         m4, [r1]
    movu         m5, [r1 + r2]
    movu         m6, [r1 + 2 * r2]
    movu         m7, [r1 + r3]

    punpcklbw    m8, m0, m1                 ;[1 - 8 ; 17 - 24][1 2]
    punpckhbw    m0, m1                     ;[9 - 16; 25 - 32][1 2]

    punpcklbw    m1, m2, m3                 ;[1 - 8 ; 17 - 24][3 4]
    punpckhbw    m2, m3                     ;[9 - 16; 25 - 32][3 4]

    punpcklbw    m3, m4, m5                 ;[1 - 8 ; 17 - 24][5 6]
    punpckhbw    m4, m5                     ;[9 - 16; 25 - 32][5 6]

    punpcklbw    m5, m6, m7                 ;[1 - 8 ; 17 - 24][7 8]
    punpckhbw    m6, m7                     ;[9 - 16; 25 - 32][7 8]

    punpcklwd    m7, m8, m1                 ;[1 - 4 ; 17 - 20][1 2 3 4]
    punpckhwd    m8, m1                     ;[5 - 8 ; 20 - 24][1 2 3 4]

    punpcklwd    m1, m3, m5                 ;[1 - 4 ; 17 - 20][5 6 7 8]
    punpckhwd    m3, m5                     ;[5 - 8 ; 20 - 24][5 6 7 8]

    punpcklwd    m5, m0, m2                 ;[9 - 12; 25 - 28][1 2 3 4]
    punpckhwd    m0, m2                     ;[12- 15; 29 - 32][1 2 3 4]

    punpcklwd    m2, m4, m6                 ;[9 - 12; 25 - 28][5 6 7 8]
    punpckhwd    m4, m6                     ;[12- 15; 29 - 32][5 6 7 8]

    punpckldq    m6, m7, m1                 ;[1 - 2 ; 17 - 18][1 2 3 4 5 6 7 8]
    punpckhdq    m7, m1                     ;[3 - 4 ; 19 - 20][1 2 3 4 5 6 7 8]

    punpckldq    m1, m8, m3                 ;[5 - 6 ; 21 - 22][1 2 3 4 5 6 7 8]
    punpckhdq    m8, m3                     ;[7 - 8 ; 23 - 24][1 2 3 4 5 6 7 8]

    punpckldq    m3, m5, m2                 ;[9 - 10; 25 - 26][1 2 3 4 5 6 7 8]
    punpckhdq    m5, m2                     ;[11- 12; 27 - 28][1 2 3 4 5 6 7 8]

    punpckldq    m2, m0, m4                 ;[13- 14; 29 - 30][1 2 3 4 5 6 7 8]
    punpckhdq    m0, m4                     ;[15- 16; 31 - 32][1 2 3 4 5 6 7 8]

    movq         [r0 + 0  * 64], xm6
    movhps       [r0 + 1  * 64], xm6
    vextracti128 xm4, m6, 1
    movq         [r0 + 16 * 64], xm4
    movhps       [r0 + 17 * 64], xm4

    lea          r1,  [r1 + 4 * r2]
    movu         m9,  [r1]
    movu         m10, [r1 + r2]
    movu         m11, [r1 + 2 * r2]
    movu         m12, [r1 + r3]
    lea          r1,  [r1 + 4 * r2]

    movu         m13, [r1]
    movu         m14, [r1 + r2]
    movu         m15, [r1 + 2 * r2]
    movu         m6,  [r1 + r3]

    punpcklbw    m4, m9, m10                ;[1 - 8 ; 17 - 24][9 10]
    punpckhbw    m9, m10                    ;[9 - 16; 25 - 32][9 10]

    punpcklbw    m10, m11, m12              ;[1 - 8 ; 17 - 24][11 12]
    punpckhbw    m11, m12                   ;[9 - 16; 25 - 32][11 12]

    punpcklbw    m12, m13, m14              ;[1 - 8 ; 17 - 24][13 14]
    punpckhbw    m13, m14                   ;[9 - 16; 25 - 32][13 14]

    punpcklbw    m14, m15, m6               ;[1 - 8 ; 17 - 24][15 16]
    punpckhbw    m15, m6                    ;[9 - 16; 25 - 32][15 16]

    punpcklwd    m6, m4, m10                ;[1 - 4 ; 17 - 20][9 10 11 12]
    punpckhwd    m4, m10                    ;[5 - 8 ; 20 - 24][9 10 11 12]

    punpcklwd    m10, m12, m14              ;[1 - 4 ; 17 - 20][13 14 15 16]
    punpckhwd    m12, m14                   ;[5 - 8 ; 20 - 24][13 14 15 16]

    punpcklwd    m14, m9, m11               ;[9 - 12; 25 - 28][9 10 11 12]
    punpckhwd    m9, m11                    ;[13- 16; 29 - 32][9 10 11 12]

    punpcklwd    m11, m13, m15              ;[9 - 12; 25 - 28][13 14 15 16]
    punpckhwd    m13, m15                   ;[13- 16; 29 - 32][13 14 15 16]

    punpckldq    m15, m6, m10               ;[1 - 2 ; 17 - 18][9 10 11 12 13 14 15 16]
    punpckhdq    m6, m10                    ;[3 - 4 ; 19 - 20][9 10 11 12 13 14 15 16]

    punpckldq    m10, m4, m12               ;[5 - 6 ; 21 - 22][9 10 11 12 13 14 15 16]
    punpckhdq    m4, m12                    ;[7 - 8 ; 23 - 24][9 10 11 12 13 14 15 16]

    punpckldq    m12, m14, m11              ;[9 - 10; 25 - 26][9 10 11 12 13 14 15 16]
    punpckhdq    m14, m11                   ;[11- 12; 27 - 28][9 10 11 12 13 14 15 16]

    punpckldq    m11, m9, m13               ;[13- 14; 29 - 30][9 10 11 12 13 14 15 16]
    punpckhdq    m9, m13                    ;[15- 16; 31 - 32][9 10 11 12 13 14 15 16]


    punpcklqdq   m13, m7, m6                ;[3 ; 19][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m7, m6                     ;[4 ; 20][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m6, m1, m10                ;[5 ; 21][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m1, m10                    ;[6 ; 22][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m10, m8, m4                ;[7 ; 23][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m8, m4                     ;[8 ; 24][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m4, m3, m12                ;[9 ; 25][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m3, m12                    ;[10; 26][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m12, m5, m14               ;[11; 27][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m5, m14                    ;[12; 28][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m14, m2, m11               ;[13; 29][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m2, m11                    ;[14; 30][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    punpcklqdq   m11, m0, m9                ;[15; 31][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
    punpckhqdq   m0, m9                     ;[16; 32][1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]

    movq         [r0 + 0  * 64 + 8], xm15
    movhps       [r0 + 1  * 64 + 8], xm15
    vextracti128 xm9, m15, 1
    movq         [r0 + 16 * 64 + 8], xm9
    movhps       [r0 + 17 * 64 + 8], xm9

    movu         [r0 + 2  * 64], xm13
    vextracti128 [r0 + 18 * 64], m13, 1

    movu         [r0 + 3  * 64], xm7
    vextracti128 [r0 + 19 * 64], m7, 1

    movu         [r0 + 4  * 64], xm6
    vextracti128 [r0 + 20 * 64], m6, 1

    movu         [r0 + 5  * 64], xm1
    vextracti128 [r0 + 21 * 64], m1, 1

    movu         [r0 + 6  * 64], xm10
    vextracti128 [r0 + 22 * 64], m10, 1

    movu         [r0 + 7  * 64], xm8
    vextracti128 [r0 + 23 * 64], m8, 1

    movu         [r0 + 8  * 64], xm4
    vextracti128 [r0 + 24 * 64], m4, 1

    movu         [r0 + 9  * 64], xm3
    vextracti128 [r0 + 25 * 64], m3, 1

    movu         [r0 + 10 * 64], xm12
    vextracti128 [r0 + 26 * 64], m12, 1

    movu         [r0 + 11 * 64], xm5
    vextracti128 [r0 + 27 * 64], m5, 1

    movu         [r0 + 12 * 64], xm14
    vextracti128 [r0 + 28 * 64], m14, 1

    movu         [r0 + 13 * 64], xm2
    vextracti128 [r0 + 29 * 64], m2, 1

    movu         [r0 + 14 * 64], xm11
    vextracti128 [r0 + 30 * 64], m11, 1

    movu         [r0 + 15 * 64], xm0
    vextracti128 [r0 + 31 * 64], m0, 1
    ret

cglobal transpose64, 3, 6, 16

    lea          r3, [r2 * 3]
    lea          r4, [r0 + 16]

    lea          r5, [r1 + 32]
    call         transpose16x32_avx2
    lea          r0, [r0 + 32 * 64]
    mov          r1, r5
    call         transpose16x32_avx2

    mov          r0, r4
    lea          r5, [r1 + 4 * r2]

    lea          r1, [r5 - 32]
    call         transpose16x32_avx2
    lea          r0, [r0 + 32 * 64]
    mov          r1, r5
    call         transpose16x32_avx2

    lea          r0, [r4 + 16]
    lea          r5, [r1 + 4 * r2]

    lea          r1, [r5 - 32]
    call         transpose16x32_avx2
    lea          r0, [r0 + 32 * 64]
    mov          r1, r5
    call         transpose16x32_avx2

    lea          r5, [r1 + 4 * r2]
    lea          r0, [r4 + 32]

    lea          r1, [r5 - 32]
    call         transpose16x32_avx2
    lea          r0, [r0 + 32 * 64]
    mov          r1, r5
    call         transpose16x32_avx2
    RET
%endif

INIT_XMM sse2
cglobal transpose64, 3, 7, 8, dest, src, stride
    mov    r3,    r0
    mov    r4,    r1
    mov    r5,    r0
    mov    r6,    64
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 16]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 32]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 48]
    mov    r5,    r0
    call   transpose16_internal

    lea    r1,    [r4 + 16]
    lea    r0,    [r3 + 16 * 64]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 16 * 64 + 16]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 16 * 64 + 32]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 16 * 64 + 48]
    mov    r5,    r0
    call   transpose16_internal

    lea    r1,    [r4 + 32]
    lea    r0,    [r3 + 32 * 64]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 32 * 64 + 16]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 32 * 64 + 32]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 32 * 64 + 48]
    mov    r5,    r0
    call   transpose16_internal

    lea    r1,    [r4 + 48]
    lea    r0,    [r3 + 48 * 64]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 48 * 64 + 16]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 48 * 64 + 32]
    mov    r5,    r0
    call   transpose16_internal
    lea    r1,    [r1 - 8 + 2 * r2]
    lea    r0,    [r3 + 48 * 64 + 48]
    mov    r5,    r0
    call   transpose16_internal
    RET
%endif


;=============================================================================
; SSIM
;=============================================================================

;-----------------------------------------------------------------------------
; void pixel_ssim_4x4x2_core( const uint8_t *pix1, intptr_t stride1,
;                             const uint8_t *pix2, intptr_t stride2, int sums[2][4] )
;-----------------------------------------------------------------------------
%macro SSIM_ITER 1
%if HIGH_BIT_DEPTH
    movdqu    m5, [r0+(%1&1)*r1]
    movdqu    m6, [r2+(%1&1)*r3]
%else
    movq      m5, [r0+(%1&1)*r1]
    movq      m6, [r2+(%1&1)*r3]
    punpcklbw m5, m0
    punpcklbw m6, m0
%endif
%if %1==1
    lea       r0, [r0+r1*2]
    lea       r2, [r2+r3*2]
%endif
%if %1==0
    movdqa    m1, m5
    movdqa    m2, m6
%else
    paddw     m1, m5
    paddw     m2, m6
%endif
    pmaddwd   m7, m5, m6
    pmaddwd   m5, m5
    pmaddwd   m6, m6
    ACCUM  paddd, 3, 5, %1
    ACCUM  paddd, 4, 7, %1
    paddd     m3, m6
%endmacro

%macro SSIM 0
cglobal pixel_ssim_4x4x2_core, 4,4,8
    FIX_STRIDES r1, r3
    pxor      m0, m0
    SSIM_ITER 0
    SSIM_ITER 1
    SSIM_ITER 2
    SSIM_ITER 3
    ; PHADDW m1, m2
    ; PHADDD m3, m4
    movdqa    m7, [pw_1]
    pshufd    m5, m3, q2301
    pmaddwd   m1, m7
    pmaddwd   m2, m7
    pshufd    m6, m4, q2301
    packssdw  m1, m2
    paddd     m3, m5
    pshufd    m1, m1, q3120
    paddd     m4, m6
    pmaddwd   m1, m7
    punpckhdq m5, m3, m4
    punpckldq m3, m4

%if UNIX64
    %define t0 r4
%else
    %define t0 rax
    mov t0, r4mp
%endif

    movq      [t0+ 0], m1
    movq      [t0+ 8], m3
    movhps    [t0+16], m1
    movq      [t0+24], m5
    RET

;-----------------------------------------------------------------------------
; float pixel_ssim_end( int sum0[5][4], int sum1[5][4], int width )
;-----------------------------------------------------------------------------
cglobal pixel_ssim_end4, 2,3
    mov       r2d, r2m
    mova      m0, [r0+ 0]
    mova      m1, [r0+16]
    mova      m2, [r0+32]
    mova      m3, [r0+48]
    mova      m4, [r0+64]
    paddd     m0, [r1+ 0]
    paddd     m1, [r1+16]
    paddd     m2, [r1+32]
    paddd     m3, [r1+48]
    paddd     m4, [r1+64]
    paddd     m0, m1
    paddd     m1, m2
    paddd     m2, m3
    paddd     m3, m4
    TRANSPOSE4x4D  0, 1, 2, 3, 4

;   s1=m0, s2=m1, ss=m2, s12=m3
%if BIT_DEPTH >= 10
    cvtdq2ps  m0, m0
    cvtdq2ps  m1, m1
    cvtdq2ps  m2, m2
    cvtdq2ps  m3, m3
    mulps     m4, m0, m1  ; s1*s2
    mulps     m0, m0      ; s1*s1
    mulps     m1, m1      ; s2*s2
    mulps     m2, [pf_64] ; ss*64
    mulps     m3, [pf_128] ; s12*128
    addps     m4, m4      ; s1*s2*2
    addps     m0, m1      ; s1*s1 + s2*s2
    subps     m2, m0      ; vars
    subps     m3, m4      ; covar*2
    movaps    m1, [ssim_c1]
    addps     m4, m1      ; s1*s2*2 + ssim_c1
    addps     m0, m1      ; s1*s1 + s2*s2 + ssim_c1
    movaps    m1, [ssim_c2]
    addps     m2, m1      ; vars + ssim_c2
    addps     m3, m1      ; covar*2 + ssim_c2
%else
    pmaddwd   m4, m1, m0  ; s1*s2
    pslld     m1, 16
    por       m0, m1
    pmaddwd   m0, m0  ; s1*s1 + s2*s2
    pslld     m4, 1
    pslld     m3, 7
    pslld     m2, 6
    psubd     m3, m4  ; covar*2
    psubd     m2, m0  ; vars
    mova      m1, [ssim_c1]
    paddd     m0, m1
    paddd     m4, m1
    mova      m1, [ssim_c2]
    paddd     m3, m1
    paddd     m2, m1
    cvtdq2ps  m0, m0  ; (float)(s1*s1 + s2*s2 + ssim_c1)
    cvtdq2ps  m4, m4  ; (float)(s1*s2*2 + ssim_c1)
    cvtdq2ps  m3, m3  ; (float)(covar*2 + ssim_c2)
    cvtdq2ps  m2, m2  ; (float)(vars + ssim_c2)
%endif
    mulps     m4, m3
    mulps     m0, m2
    divps     m4, m0  ; ssim

    cmp       r2d, 4
    je .skip ; faster only if this is the common case; remove branch if we use ssim on a macroblock level
    neg       r2

%ifdef PIC
    lea       r3, [mask_ff + 16]
    %xdefine %%mask r3
%else
    %xdefine %%mask mask_ff + 16
%endif
%if cpuflag(avx)
    andps     m4, [%%mask + r2*4]
%else
    movups    m0, [%%mask + r2*4]
    andps     m4, m0
%endif

.skip:
    movhlps   m0, m4
    addps     m0, m4
%if cpuflag(ssse3)
    movshdup  m4, m0
%else
     pshuflw   m4, m0, q0032
%endif
    addss     m0, m4
%if ARCH_X86_64 == 0
    movss    r0m, m0
    fld     dword r0m
%endif
    RET
%endmacro ; SSIM

INIT_XMM sse2
SSIM
INIT_XMM avx
SSIM

%macro SCALE1D_128to64_HBD 0
    movu        m0,      [r1]
    palignr     m1,      m0,    2
    movu        m2,      [r1 + 16]
    palignr     m3,      m2,    2
    movu        m4,      [r1 + 32]
    palignr     m5,      m4,    2
    movu        m6,      [r1 + 48]
    pavgw       m0,      m1
    palignr     m1,      m6,    2
    pavgw       m2,      m3
    pavgw       m4,      m5
    pavgw       m6,      m1
    pshufb      m0,      m0,    m7
    pshufb      m2,      m2,    m7
    pshufb      m4,      m4,    m7
    pshufb      m6,      m6,    m7
    punpcklqdq    m0,           m2
    movu          [r0],         m0
    punpcklqdq    m4,           m6
    movu          [r0 + 16],    m4

    movu        m0,      [r1 + 64]
    palignr     m1,      m0,    2
    movu        m2,      [r1 + 80]
    palignr     m3,      m2,    2
    movu        m4,      [r1 + 96]
    palignr     m5,      m4,    2
    movu        m6,      [r1 + 112]
    pavgw       m0,      m1
    palignr     m1,      m6,    2
    pavgw       m2,      m3
    pavgw       m4,      m5
    pavgw       m6,      m1
    pshufb      m0,      m0,    m7
    pshufb      m2,      m2,    m7
    pshufb      m4,      m4,    m7
    pshufb      m6,      m6,    m7
    punpcklqdq    m0,           m2
    movu          [r0 + 32],    m0
    punpcklqdq    m4,           m6
    movu          [r0 + 48],    m4

    movu        m0,      [r1 + 128]
    palignr     m1,      m0,    2
    movu        m2,      [r1 + 144]
    palignr     m3,      m2,    2
    movu        m4,      [r1 + 160]
    palignr     m5,      m4,    2
    movu        m6,      [r1 + 176]
    pavgw       m0,      m1
    palignr     m1,      m6,    2
    pavgw       m2,      m3
    pavgw       m4,      m5
    pavgw       m6,      m1
    pshufb      m0,      m0,    m7
    pshufb      m2,      m2,    m7
    pshufb      m4,      m4,    m7
    pshufb      m6,      m6,    m7

    punpcklqdq    m0,           m2
    movu          [r0 + 64],    m0
    punpcklqdq    m4,           m6
    movu          [r0 + 80],    m4

    movu        m0,      [r1 + 192]
    palignr     m1,      m0,    2
    movu        m2,      [r1 + 208]
    palignr     m3,      m2,    2
    movu        m4,      [r1 + 224]
    palignr     m5,      m4,    2
    movu        m6,      [r1 + 240]
    pavgw       m0,      m1
    palignr     m1,      m6,    2
    pavgw       m2,      m3
    pavgw       m4,      m5
    pavgw       m6,      m1
    pshufb      m0,      m0,    m7
    pshufb      m2,      m2,    m7
    pshufb      m4,      m4,    m7
    pshufb      m6,      m6,    m7

    punpcklqdq    m0,           m2
    movu          [r0 + 96],    m0
    punpcklqdq    m4,           m6
    movu          [r0 + 112],    m4
%endmacro

;-----------------------------------------------------------------
; void scale1D_128to64(pixel *dst, pixel *src, intptr_t /*stride*/)
;-----------------------------------------------------------------
INIT_XMM ssse3
cglobal scale1D_128to64, 2, 2, 8, dest, src1, stride
%if HIGH_BIT_DEPTH
    mova        m7,      [deinterleave_word_shuf]

    ;Top pixel
    SCALE1D_128to64_HBD

    ;Left pixel
    add         r1,      256
    add         r0,      128
    SCALE1D_128to64_HBD

%else
    mova        m7,      [deinterleave_shuf]

    ;Top pixel
    movu        m0,      [r1]
    palignr     m1,      m0,    1
    movu        m2,      [r1 + 16]
    palignr     m3,      m2,    1
    movu        m4,      [r1 + 32]
    palignr     m5,      m4,    1
    movu        m6,      [r1 + 48]

    pavgb       m0,      m1

    palignr     m1,      m6,    1

    pavgb       m2,      m3
    pavgb       m4,      m5
    pavgb       m6,      m1

    pshufb      m0,      m0,    m7
    pshufb      m2,      m2,    m7
    pshufb      m4,      m4,    m7
    pshufb      m6,      m6,    m7

    punpcklqdq    m0,           m2
    movu          [r0],         m0
    punpcklqdq    m4,           m6
    movu          [r0 + 16],    m4

    movu        m0,      [r1 + 64]
    palignr     m1,      m0,    1
    movu        m2,      [r1 + 80]
    palignr     m3,      m2,    1
    movu        m4,      [r1 + 96]
    palignr     m5,      m4,    1
    movu        m6,      [r1 + 112]

    pavgb       m0,      m1

    palignr     m1,      m6,    1

    pavgb       m2,      m3
    pavgb       m4,      m5
    pavgb       m6,      m1

    pshufb      m0,      m0,    m7
    pshufb      m2,      m2,    m7
    pshufb      m4,      m4,    m7
    pshufb      m6,      m6,    m7

    punpcklqdq    m0,           m2
    movu          [r0 + 32],    m0
    punpcklqdq    m4,           m6
    movu          [r0 + 48],    m4

    ;Left pixel
    movu        m0,      [r1 + 128]
    palignr     m1,      m0,    1
    movu        m2,      [r1 + 144]
    palignr     m3,      m2,    1
    movu        m4,      [r1 + 160]
    palignr     m5,      m4,    1
    movu        m6,      [r1 + 176]

    pavgb       m0,      m1

    palignr     m1,      m6,    1

    pavgb       m2,      m3
    pavgb       m4,      m5
    pavgb       m6,      m1

    pshufb      m0,      m0,    m7
    pshufb      m2,      m2,    m7
    pshufb      m4,      m4,    m7
    pshufb      m6,      m6,    m7

    punpcklqdq    m0,           m2
    movu          [r0 + 64],    m0
    punpcklqdq    m4,           m6
    movu          [r0 + 80],    m4

    movu        m0,      [r1 + 192]
    palignr     m1,      m0,    1
    movu        m2,      [r1 + 208]
    palignr     m3,      m2,    1
    movu        m4,      [r1 + 224]
    palignr     m5,      m4,    1
    movu        m6,      [r1 + 240]

    pavgb       m0,      m1

    palignr     m1,      m6,    1

    pavgb       m2,      m3
    pavgb       m4,      m5
    pavgb       m6,      m1

    pshufb      m0,      m0,    m7
    pshufb      m2,      m2,    m7
    pshufb      m4,      m4,    m7
    pshufb      m6,      m6,    m7

    punpcklqdq    m0,           m2
    movu          [r0 + 96],    m0
    punpcklqdq    m4,           m6
    movu          [r0 + 112],   m4
%endif
RET

%if HIGH_BIT_DEPTH == 1
INIT_YMM avx2
cglobal scale1D_128to64, 2, 2, 3
    pxor            m2, m2

    ;Top pixel
    movu            m0, [r1]
    movu            m1, [r1 + 32]
    phaddw          m0, m1
    pavgw           m0, m2
    vpermq          m0, m0, 0xD8
    movu            [r0], m0

    movu            m0, [r1 + 64]
    movu            m1, [r1 + 96]
    phaddw          m0, m1
    pavgw           m0, m2
    vpermq          m0, m0, 0xD8
    movu            [r0 + 32], m0

    movu            m0, [r1 + 128]
    movu            m1, [r1 + 160]
    phaddw          m0, m1
    pavgw           m0, m2
    vpermq          m0, m0, 0xD8
    movu            [r0 + 64], m0

    movu            m0, [r1 + 192]
    movu            m1, [r1 + 224]
    phaddw          m0, m1
    pavgw           m0, m2
    vpermq          m0, m0, 0xD8
    movu            [r0 + 96], m0

    ;Left pixel
    movu            m0, [r1 + 256]
    movu            m1, [r1 + 288]
    phaddw          m0, m1
    pavgw           m0, m2
    vpermq          m0, m0, 0xD8
    movu            [r0 + 128], m0

    movu            m0, [r1 + 320]
    movu            m1, [r1 + 352]
    phaddw          m0, m1
    pavgw           m0, m2
    vpermq          m0, m0, 0xD8
    movu            [r0 + 160], m0

    movu            m0, [r1 + 384]
    movu            m1, [r1 + 416]
    phaddw          m0, m1
    pavgw           m0, m2
    vpermq          m0, m0, 0xD8
    movu            [r0 + 192], m0

    movu            m0, [r1 + 448]
    movu            m1, [r1 + 480]
    phaddw          m0, m1
    pavgw           m0, m2
    vpermq          m0, m0, 0xD8
    movu            [r0 + 224], m0

    RET
%else ; HIGH_BIT_DEPTH == 0
INIT_YMM avx2
cglobal scale1D_128to64, 2, 2, 4
    pxor            m2, m2
    mova            m3, [pb_1]

    ;Top pixel
    movu            m0, [r1]
    pmaddubsw       m0, m0, m3
    pavgw           m0, m2
    movu            m1, [r1 + 32]
    pmaddubsw       m1, m1, m3
    pavgw           m1, m2
    packuswb        m0, m1
    vpermq          m0, m0, 0xD8
    movu            [r0], m0

    movu            m0, [r1 + 64]
    pmaddubsw       m0, m0, m3
    pavgw           m0, m2
    movu            m1, [r1 + 96]
    pmaddubsw       m1, m1, m3
    pavgw           m1, m2
    packuswb        m0, m1
    vpermq          m0, m0, 0xD8
    movu            [r0 + 32], m0

    ;Left pixel
    movu            m0, [r1 + 128]
    pmaddubsw       m0, m0, m3
    pavgw           m0, m2
    movu            m1, [r1 + 160]
    pmaddubsw       m1, m1, m3
    pavgw           m1, m2
    packuswb        m0, m1
    vpermq          m0, m0, 0xD8
    movu            [r0 + 64], m0

    movu            m0, [r1 + 192]
    pmaddubsw       m0, m0, m3
    pavgw           m0, m2
    movu            m1, [r1 + 224]
    pmaddubsw       m1, m1, m3
    pavgw           m1, m2
    packuswb        m0, m1
    vpermq          m0, m0, 0xD8
    movu            [r0 + 96], m0
    RET
%endif

;-----------------------------------------------------------------
; void scale2D_64to32(pixel *dst, pixel *src, intptr_t stride)
;-----------------------------------------------------------------
%if HIGH_BIT_DEPTH
INIT_XMM ssse3
cglobal scale2D_64to32, 3, 4, 8, dest, src, stride
    mov       r3d,    32
    mova      m7,    [deinterleave_word_shuf]
    add       r2,    r2
.loop:
    movu      m0,    [r1]                  ;i
    psrld     m1,    m0,    16             ;j
    movu      m2,    [r1 + r2]             ;k
    psrld     m3,    m2,    16             ;l
    movu      m4,    m0
    movu      m5,    m2
    pxor      m4,    m1                    ;i^j
    pxor      m5,    m3                    ;k^l
    por       m4,    m5                    ;ij|kl
    pavgw     m0,    m1                    ;s
    pavgw     m2,    m3                    ;t
    movu      m5,    m0
    pavgw     m0,    m2                    ;(s+t+1)/2
    pxor      m5,    m2                    ;s^t
    pand      m4,    m5                    ;(ij|kl)&st
    pand      m4,    [hmulw_16p]
    psubw     m0,    m4                    ;Result
    movu      m1,    [r1 + 16]             ;i
    psrld     m2,    m1,    16             ;j
    movu      m3,    [r1 + r2 + 16]        ;k
    psrld     m4,    m3,    16             ;l
    movu      m5,    m1
    movu      m6,    m3
    pxor      m5,    m2                    ;i^j
    pxor      m6,    m4                    ;k^l
    por       m5,    m6                    ;ij|kl
    pavgw     m1,    m2                    ;s
    pavgw     m3,    m4                    ;t
    movu      m6,    m1
    pavgw     m1,    m3                    ;(s+t+1)/2
    pxor      m6,    m3                    ;s^t
    pand      m5,    m6                    ;(ij|kl)&st
    pand      m5,    [hmulw_16p]
    psubw     m1,    m5                    ;Result
    pshufb    m0,    m7
    pshufb    m1,    m7

    punpcklqdq    m0,       m1
    movu          [r0],     m0

    movu      m0,    [r1 + 32]             ;i
    psrld     m1,    m0,    16             ;j
    movu      m2,    [r1 + r2 + 32]        ;k
    psrld     m3,    m2,    16             ;l
    movu      m4,    m0
    movu      m5,    m2
    pxor      m4,    m1                    ;i^j
    pxor      m5,    m3                    ;k^l
    por       m4,    m5                    ;ij|kl
    pavgw     m0,    m1                    ;s
    pavgw     m2,    m3                    ;t
    movu      m5,    m0
    pavgw     m0,    m2                    ;(s+t+1)/2
    pxor      m5,    m2                    ;s^t
    pand      m4,    m5                    ;(ij|kl)&st
    pand      m4,    [hmulw_16p]
    psubw     m0,    m4                    ;Result
    movu      m1,    [r1 + 48]             ;i
    psrld     m2,    m1,    16             ;j
    movu      m3,    [r1 + r2 + 48]        ;k
    psrld     m4,    m3,    16             ;l
    movu      m5,    m1
    movu      m6,    m3
    pxor      m5,    m2                    ;i^j
    pxor      m6,    m4                    ;k^l
    por       m5,    m6                    ;ij|kl
    pavgw     m1,    m2                    ;s
    pavgw     m3,    m4                    ;t
    movu      m6,    m1
    pavgw     m1,    m3                    ;(s+t+1)/2
    pxor      m6,    m3                    ;s^t
    pand      m5,    m6                    ;(ij|kl)&st
    pand      m5,    [hmulw_16p]
    psubw     m1,    m5                    ;Result
    pshufb    m0,    m7
    pshufb    m1,    m7

    punpcklqdq    m0,           m1
    movu          [r0 + 16],    m0

    movu      m0,    [r1 + 64]             ;i
    psrld     m1,    m0,    16             ;j
    movu      m2,    [r1 + r2 + 64]        ;k
    psrld     m3,    m2,    16             ;l
    movu      m4,    m0
    movu      m5,    m2
    pxor      m4,    m1                    ;i^j
    pxor      m5,    m3                    ;k^l
    por       m4,    m5                    ;ij|kl
    pavgw     m0,    m1                    ;s
    pavgw     m2,    m3                    ;t
    movu      m5,    m0
    pavgw     m0,    m2                    ;(s+t+1)/2
    pxor      m5,    m2                    ;s^t
    pand      m4,    m5                    ;(ij|kl)&st
    pand      m4,    [hmulw_16p]
    psubw     m0,    m4                    ;Result
    movu      m1,    [r1 + 80]             ;i
    psrld     m2,    m1,    16             ;j
    movu      m3,    [r1 + r2 + 80]        ;k
    psrld     m4,    m3,    16             ;l
    movu      m5,    m1
    movu      m6,    m3
    pxor      m5,    m2                    ;i^j
    pxor      m6,    m4                    ;k^l
    por       m5,    m6                    ;ij|kl
    pavgw     m1,    m2                    ;s
    pavgw     m3,    m4                    ;t
    movu      m6,    m1
    pavgw     m1,    m3                    ;(s+t+1)/2
    pxor      m6,    m3                    ;s^t
    pand      m5,    m6                    ;(ij|kl)&st
    pand      m5,    [hmulw_16p]
    psubw     m1,    m5                    ;Result
    pshufb    m0,    m7
    pshufb    m1,    m7

    punpcklqdq    m0,           m1
    movu          [r0 + 32],    m0

    movu      m0,    [r1 + 96]             ;i
    psrld     m1,    m0,    16             ;j
    movu      m2,    [r1 + r2 + 96]        ;k
    psrld     m3,    m2,    16             ;l
    movu      m4,    m0
    movu      m5,    m2
    pxor      m4,    m1                    ;i^j
    pxor      m5,    m3                    ;k^l
    por       m4,    m5                    ;ij|kl
    pavgw     m0,    m1                    ;s
    pavgw     m2,    m3                    ;t
    movu      m5,    m0
    pavgw     m0,    m2                    ;(s+t+1)/2
    pxor      m5,    m2                    ;s^t
    pand      m4,    m5                    ;(ij|kl)&st
    pand      m4,    [hmulw_16p]
    psubw     m0,    m4                    ;Result
    movu      m1,    [r1 + 112]            ;i
    psrld     m2,    m1,    16             ;j
    movu      m3,    [r1 + r2 + 112]       ;k
    psrld     m4,    m3,    16             ;l
    movu      m5,    m1
    movu      m6,    m3
    pxor      m5,    m2                    ;i^j
    pxor      m6,    m4                    ;k^l
    por       m5,    m6                    ;ij|kl
    pavgw     m1,    m2                    ;s
    pavgw     m3,    m4                    ;t
    movu      m6,    m1
    pavgw     m1,    m3                    ;(s+t+1)/2
    pxor      m6,    m3                    ;s^t
    pand      m5,    m6                    ;(ij|kl)&st
    pand      m5,    [hmulw_16p]
    psubw     m1,    m5                    ;Result
    pshufb    m0,    m7
    pshufb    m1,    m7

    punpcklqdq    m0,           m1
    movu          [r0 + 48],    m0
    lea    r0,    [r0 + 64]
    lea    r1,    [r1 + 2 * r2]
    dec    r3d
    jnz    .loop
    RET
%else

INIT_XMM ssse3
cglobal scale2D_64to32, 3, 4, 8, dest, src, stride
    mov       r3d,    32
    mova        m7,      [deinterleave_shuf]
.loop:

    movu        m0,      [r1]                  ;i
    psrlw       m1,      m0,    8              ;j
    movu        m2,      [r1 + r2]             ;k
    psrlw       m3,      m2,    8              ;l
    movu        m4,      m0
    movu        m5,      m2

    pxor        m4,      m1                    ;i^j
    pxor        m5,      m3                    ;k^l
    por         m4,      m5                    ;ij|kl

    pavgb       m0,      m1                    ;s
    pavgb       m2,      m3                    ;t
    movu        m5,      m0
    pavgb       m0,      m2                    ;(s+t+1)/2
    pxor        m5,      m2                    ;s^t
    pand        m4,      m5                    ;(ij|kl)&st
    pand        m4,      [hmul_16p]
    psubb       m0,      m4                    ;Result

    movu        m1,      [r1 + 16]             ;i
    psrlw       m2,      m1,    8              ;j
    movu        m3,      [r1 + r2 + 16]        ;k
    psrlw       m4,      m3,    8              ;l
    movu        m5,      m1
    movu        m6,      m3

    pxor        m5,      m2                    ;i^j
    pxor        m6,      m4                    ;k^l
    por         m5,      m6                    ;ij|kl

    pavgb       m1,      m2                    ;s
    pavgb       m3,      m4                    ;t
    movu        m6,      m1
    pavgb       m1,      m3                    ;(s+t+1)/2
    pxor        m6,      m3                    ;s^t
    pand        m5,      m6                    ;(ij|kl)&st
    pand        m5,      [hmul_16p]
    psubb       m1,      m5                    ;Result

    pshufb      m0,      m0,    m7
    pshufb      m1,      m1,    m7

    punpcklqdq    m0,           m1
    movu          [r0],         m0

    movu        m0,      [r1 + 32]             ;i
    psrlw       m1,      m0,    8              ;j
    movu        m2,      [r1 + r2 + 32]        ;k
    psrlw       m3,      m2,    8              ;l
    movu        m4,      m0
    movu        m5,      m2

    pxor        m4,      m1                    ;i^j
    pxor        m5,      m3                    ;k^l
    por         m4,      m5                    ;ij|kl

    pavgb       m0,      m1                    ;s
    pavgb       m2,      m3                    ;t
    movu        m5,      m0
    pavgb       m0,      m2                    ;(s+t+1)/2
    pxor        m5,      m2                    ;s^t
    pand        m4,      m5                    ;(ij|kl)&st
    pand        m4,      [hmul_16p]
    psubb       m0,      m4                    ;Result

    movu        m1,      [r1 + 48]             ;i
    psrlw       m2,      m1,    8              ;j
    movu        m3,      [r1 + r2 + 48]        ;k
    psrlw       m4,      m3,    8              ;l
    movu        m5,      m1
    movu        m6,      m3

    pxor        m5,      m2                    ;i^j
    pxor        m6,      m4                    ;k^l
    por         m5,      m6                    ;ij|kl

    pavgb       m1,      m2                    ;s
    pavgb       m3,      m4                    ;t
    movu        m6,      m1
    pavgb       m1,      m3                    ;(s+t+1)/2
    pxor        m6,      m3                    ;s^t
    pand        m5,      m6                    ;(ij|kl)&st
    pand        m5,      [hmul_16p]
    psubb       m1,      m5                    ;Result

    pshufb      m0,      m0,    m7
    pshufb      m1,      m1,    m7

    punpcklqdq    m0,           m1
    movu          [r0 + 16],    m0

    lea    r0,    [r0 + 32]
    lea    r1,    [r1 + 2 * r2]
    dec    r3d
    jnz    .loop
    RET
%endif

;-----------------------------------------------------------------
; void scale2D_64to32(pixel *dst, pixel *src, intptr_t stride)
;-----------------------------------------------------------------
%if HIGH_BIT_DEPTH
INIT_YMM avx2
cglobal scale2D_64to32, 3, 4, 5, dest, src, stride
    mov         r3d,     32
    add         r2d,     r2d
    mova        m4,      [pw_2000]

.loop:
    movu        m0,      [r1]
    movu        m1,      [r1 + 1 * mmsize]
    movu        m2,      [r1 + r2]
    movu        m3,      [r1 + r2 + 1 * mmsize]

    paddw       m0,      m2
    paddw       m1,      m3
    phaddw      m0,      m1

    pmulhrsw    m0,      m4
    vpermq      m0,      m0, q3120
    movu        [r0],    m0

    movu        m0,      [r1 + 2 * mmsize]
    movu        m1,      [r1 + 3 * mmsize]
    movu        m2,      [r1 + r2 + 2 * mmsize]
    movu        m3,      [r1 + r2 + 3 * mmsize]

    paddw       m0,      m2
    paddw       m1,      m3
    phaddw      m0,      m1

    pmulhrsw    m0,      m4
    vpermq      m0,      m0, q3120
    movu        [r0 + mmsize], m0

    add         r0,      64
    lea         r1,      [r1 + 2 * r2]
    dec         r3d
    jnz         .loop
    RET
%else

INIT_YMM avx2
cglobal scale2D_64to32, 3, 5, 8, dest, src, stride
    mov         r3d,     16
    mova        m7,      [deinterleave_shuf]
.loop:
    movu        m0,      [r1]                  ; i
    lea         r4,      [r1 + r2 * 2]
    psrlw       m1,      m0, 8                 ; j
    movu        m2,      [r1 + r2]             ; k
    psrlw       m3,      m2, 8                 ; l

    pxor        m4,      m0, m1                ; i^j
    pxor        m5,      m2, m3                ; k^l
    por         m4,      m5                    ; ij|kl

    pavgb       m0,      m1                    ; s
    pavgb       m2,      m3                    ; t
    mova        m5,      m0
    pavgb       m0,      m2                    ; (s+t+1)/2
    pxor        m5,      m2                    ; s^t
    pand        m4,      m5                    ; (ij|kl)&st
    pand        m4,      [pb_1]
    psubb       m0,      m4                    ; Result

    movu        m1,      [r1 + 32]             ; i
    psrlw       m2,      m1, 8                 ; j
    movu        m3,      [r1 + r2 + 32]        ; k
    psrlw       m4,      m3, 8                 ; l

    pxor        m5,      m1, m2                ; i^j
    pxor        m6,      m3, m4                ; k^l
    por         m5,      m6                    ; ij|kl

    pavgb       m1,      m2                    ; s
    pavgb       m3,      m4                    ; t
    mova        m6,      m1
    pavgb       m1,      m3                    ; (s+t+1)/2
    pxor        m6,      m3                    ; s^t
    pand        m5,      m6                    ; (ij|kl)&st
    pand        m5,      [pb_1]
    psubb       m1,      m5                    ; Result

    pshufb      m0,      m0, m7
    pshufb      m1,      m1, m7

    punpcklqdq  m0,      m1
    vpermq      m0,      m0, 11011000b
    movu        [r0],    m0

    add         r0,      32

    movu        m0,      [r4]                  ; i
    psrlw       m1,      m0, 8                 ; j
    movu        m2,      [r4 + r2]             ; k
    psrlw       m3,      m2, 8                 ; l

    pxor        m4,      m0, m1                ; i^j
    pxor        m5,      m2, m3                ; k^l
    por         m4,      m5                    ; ij|kl

    pavgb       m0,      m1                    ; s
    pavgb       m2,      m3                    ; t
    mova        m5,      m0
    pavgb       m0,      m2                    ; (s+t+1)/2
    pxor        m5,      m2                    ; s^t
    pand        m4,      m5                    ; (ij|kl)&st
    pand        m4,      [pb_1]
    psubb       m0,      m4                    ; Result

    movu        m1,      [r4 + 32]             ; i
    psrlw       m2,      m1, 8                 ; j
    movu        m3,      [r4 + r2 + 32]        ; k
    psrlw       m4,      m3, 8                 ; l

    pxor        m5,      m1, m2                ; i^j
    pxor        m6,      m3, m4                ; k^l
    por         m5,      m6                    ; ij|kl

    pavgb       m1,      m2                    ; s
    pavgb       m3,      m4                    ; t
    mova        m6,      m1
    pavgb       m1,      m3                    ; (s+t+1)/2
    pxor        m6,      m3                    ; s^t
    pand        m5,      m6                    ; (ij|kl)&st
    pand        m5,      [pb_1]
    psubb       m1,      m5                    ; Result

    pshufb      m0,      m0, m7
    pshufb      m1,      m1, m7

    punpcklqdq  m0,      m1
    vpermq      m0,      m0, 11011000b
    movu        [r0],    m0

    lea         r1,      [r1 + 4 * r2]
    add         r0,      32
    dec         r3d
    jnz         .loop
    RET
%endif

;-----------------------------------------------------------------------------
; void pixel_sub_ps_4x4(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%if HIGH_BIT_DEPTH
INIT_XMM sse2
cglobal pixel_sub_ps_4x4, 6, 6, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    add     r4,     r4
    add     r5,     r5
    add     r1,     r1
    movh    m0,     [r2]
    movh    m2,     [r2 + r4]
    movh    m1,     [r3]
    movh    m3,     [r3 + r5]
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]
    movh    m4,     [r2]
    movh    m6,     [r2 + r4]
    movh    m5,     [r3]
    movh    m7,     [r3 + r5]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movh    [r0],       m0
    movh    [r0 + r1],  m2
    lea     r0,     [r0 + r1 * 2]
    movh    [r0],       m4
    movh    [r0 + r1],  m6

    RET
%else
INIT_XMM sse4
cglobal pixel_sub_ps_4x4, 6, 6, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    add         r1,     r1
    movd        m0,     [r2]
    movd        m2,     [r2 + r4]
    movd        m1,     [r3]
    movd        m3,     [r3 + r5]
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    movd        m4,     [r2]
    movd        m6,     [r2 + r4]
    movd        m5,     [r3]
    movd        m7,     [r3 + r5]
    punpckldq   m0,     m2
    punpckldq   m1,     m3
    punpckldq   m4,     m6
    punpckldq   m5,     m7
    pmovzxbw    m0,     m0
    pmovzxbw    m1,     m1
    pmovzxbw    m4,     m4
    pmovzxbw    m5,     m5

    psubw       m0,     m1
    psubw       m4,     m5

    movh        [r0],           m0
    movhps      [r0 + r1],      m0
    movh        [r0 + r1 * 2],  m4
    lea         r0,     [r0 + r1 * 2]
    movhps      [r0 + r1],      m4

    RET
%endif


;-----------------------------------------------------------------------------
; void pixel_sub_ps_4x%2(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%macro PIXELSUB_PS_W4_H4 2
%if HIGH_BIT_DEPTH
cglobal pixel_sub_ps_4x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov     r6d,    %2/4
    add     r4,     r4
    add     r5,     r5
    add     r1,     r1
.loop:
    movh    m0,     [r2]
    movh    m2,     [r2 + r4]
    movh    m1,     [r3]
    movh    m3,     [r3 + r5]
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]
    movh    m4,     [r2]
    movh    m6,     [r2 + r4]
    movh    m5,     [r3]
    movh    m7,     [r3 + r5]
    dec     r6d
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movh    [r0],           m0
    movh    [r0 + r1],      m2
    movh    [r0 + r1 * 2],  m4
    lea     r0,     [r0 + r1 * 2]
    movh    [r0 + r1],      m6
    lea     r0,     [r0 + r1 * 2]

    jnz     .loop
    RET
%else
cglobal pixel_sub_ps_4x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov         r6d,    %2/4
    add         r1,     r1
.loop:
    movd        m0,     [r2]
    movd        m2,     [r2 + r4]
    movd        m1,     [r3]
    movd        m3,     [r3 + r5]
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    movd        m4,     [r2]
    movd        m6,     [r2 + r4]
    movd        m5,     [r3]
    movd        m7,     [r3 + r5]
    dec         r6d
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    punpckldq   m0,     m2
    punpckldq   m1,     m3
    punpckldq   m4,     m6
    punpckldq   m5,     m7
    pmovzxbw    m0,     m0
    pmovzxbw    m1,     m1
    pmovzxbw    m4,     m4
    pmovzxbw    m5,     m5

    psubw       m0,     m1
    psubw       m4,     m5

    movh        [r0],           m0
    movhps      [r0 + r1],      m0
    movh        [r0 + r1 * 2],  m4
    lea         r0,     [r0 + r1 * 2]
    movhps      [r0 + r1],      m4
    lea         r0,     [r0 + r1 * 2]

    jnz         .loop
    RET
%endif
%endmacro

%if HIGH_BIT_DEPTH
INIT_XMM sse2
PIXELSUB_PS_W4_H4 4, 8
%else
INIT_XMM sse4
PIXELSUB_PS_W4_H4 4, 8
%endif


;-----------------------------------------------------------------------------
; void pixel_sub_ps_8x%2(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%macro PIXELSUB_PS_W8_H4 2
%if HIGH_BIT_DEPTH
cglobal pixel_sub_ps_8x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov     r6d,    %2/4
    add     r4,     r4
    add     r5,     r5
    add     r1,     r1
.loop:
    movu    m0,     [r2]
    movu    m2,     [r2 + r4]
    movu    m1,     [r3]
    movu    m3,     [r3 + r5]
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]
    movu    m4,     [r2]
    movu    m6,     [r2 + r4]
    movu    m5,     [r3]
    movu    m7,     [r3 + r5]
    dec     r6d
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0],           m0
    movu    [r0 + r1],      m2
    movu    [r0 + r1 * 2],  m4
    lea     r0,     [r0 + r1 * 2]
    movu    [r0 + r1],      m6
    lea     r0,     [r0 + r1 * 2]

    jnz     .loop
    RET
%else
cglobal pixel_sub_ps_8x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov         r6d,    %2/4
    add         r1,     r1
.loop:
    movh        m0,     [r2]
    movh        m2,     [r2 + r4]
    movh        m1,     [r3]
    movh        m3,     [r3 + r5]
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    movh        m4,     [r2]
    movh        m6,     [r2 + r4]
    movh        m5,     [r3]
    movh        m7,     [r3 + r5]
    dec         r6d
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    pmovzxbw    m0,     m0
    pmovzxbw    m1,     m1
    pmovzxbw    m2,     m2
    pmovzxbw    m3,     m3
    pmovzxbw    m4,     m4
    pmovzxbw    m5,     m5
    pmovzxbw    m6,     m6
    pmovzxbw    m7,     m7

    psubw       m0,     m1
    psubw       m2,     m3
    psubw       m4,     m5
    psubw       m6,     m7

    movu        [r0],           m0
    movu        [r0 + r1],      m2
    movu        [r0 + r1 * 2],  m4
    lea         r0,     [r0 + r1 * 2]
    movu        [r0 + r1],      m6
    lea         r0,     [r0 + r1 * 2]

    jnz         .loop
    RET
%endif
%endmacro

%if HIGH_BIT_DEPTH
INIT_XMM sse2
PIXELSUB_PS_W8_H4 8, 8
PIXELSUB_PS_W8_H4 8, 16
%else
INIT_XMM sse4
PIXELSUB_PS_W8_H4 8, 8
PIXELSUB_PS_W8_H4 8, 16
%endif


;-----------------------------------------------------------------------------
; void pixel_sub_ps_16x%2(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%macro PIXELSUB_PS_W16_H4 2
%if HIGH_BIT_DEPTH
cglobal pixel_sub_ps_16x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov     r6d,    %2/4
    add     r4,     r4
    add     r5,     r5
    add     r1,     r1
.loop:
    movu    m0,     [r2]
    movu    m2,     [r2 + 16]
    movu    m1,     [r3]
    movu    m3,     [r3 + 16]
    movu    m4,     [r2 + r4]
    movu    m6,     [r2 + r4 + 16]
    movu    m5,     [r3 + r5]
    movu    m7,     [r3 + r5 + 16]
    dec     r6d
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0],           m0
    movu    [r0 + 16],      m2
    movu    [r0 + r1],      m4
    movu    [r0 + r1 + 16], m6

    movu    m0,     [r2]
    movu    m2,     [r2 + 16]
    movu    m1,     [r3]
    movu    m3,     [r3 + 16]
    movu    m4,     [r2 + r4]
    movu    m5,     [r3 + r5]
    movu    m6,     [r2 + r4 + 16]
    movu    m7,     [r3 + r5 + 16]
    lea     r0,     [r0 + r1 * 2]
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0],           m0
    movu    [r0 + 16],      m2
    movu    [r0 + r1],      m4
    movu    [r0 + r1 + 16], m6
    lea     r0,     [r0 + r1 * 2]

    jnz     .loop
    RET
%else
cglobal pixel_sub_ps_16x%2, 6, 7, 7, dest, deststride, src0, src1, srcstride0, srcstride1
    mov         r6d,    %2/4
    pxor        m6,     m6
    add         r1,     r1
.loop:
    movu        m1,     [r2]
    movu        m3,     [r3]
    pmovzxbw    m0,     m1
    pmovzxbw    m2,     m3
    punpckhbw   m1,     m6
    punpckhbw   m3,     m6

    psubw       m0,     m2
    psubw       m1,     m3

    movu        m5,     [r2 + r4]
    movu        m3,     [r3 + r5]
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    pmovzxbw    m4,     m5
    pmovzxbw    m2,     m3
    punpckhbw   m5,     m6
    punpckhbw   m3,     m6

    psubw       m4,     m2
    psubw       m5,     m3

    movu        [r0],           m0
    movu        [r0 + 16],      m1
    movu        [r0 + r1],      m4
    movu        [r0 + r1 + 16], m5

    movu        m1,     [r2]
    movu        m3,     [r3]
    pmovzxbw    m0,     m1
    pmovzxbw    m2,     m3
    punpckhbw   m1,     m6
    punpckhbw   m3,     m6

    psubw       m0,     m2
    psubw       m1,     m3

    movu        m5,     [r2 + r4]
    movu        m3,     [r3 + r5]
    dec         r6d
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    lea         r0,     [r0 + r1 * 2]
    pmovzxbw    m4,     m5
    pmovzxbw    m2,     m3
    punpckhbw   m5,     m6
    punpckhbw   m3,     m6

    psubw       m4,     m2
    psubw       m5,     m3

    movu        [r0],           m0
    movu        [r0 + 16],      m1
    movu        [r0 + r1],      m4
    movu        [r0 + r1 + 16], m5
    lea         r0,     [r0 + r1 * 2]

    jnz         .loop
    RET
%endif
%endmacro

%if HIGH_BIT_DEPTH
INIT_XMM sse2
PIXELSUB_PS_W16_H4 16, 16
PIXELSUB_PS_W16_H4 16, 32
%else
INIT_XMM sse4
PIXELSUB_PS_W16_H4 16, 16
PIXELSUB_PS_W16_H4 16, 32
%endif


;-----------------------------------------------------------------------------
; void pixel_sub_ps_16x16(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%if HIGH_BIT_DEPTH
%macro PIXELSUB_PS_W16_H4_avx2 1
%if ARCH_X86_64
INIT_YMM avx2
cglobal pixel_sub_ps_16x%1, 6, 9, 4, dest, deststride, src0, src1, srcstride0, srcstride1
    add         r1d,    r1d
    add         r4d,    r4d
    add         r5d,    r5d
    lea         r6,     [r1 * 3]
    lea         r7,     [r4 * 3]
    lea         r8,     [r5 * 3]

%rep %1/4
    movu        m0,     [r2]
    movu        m1,     [r3]
    movu        m2,     [r2 + r4]
    movu        m3,     [r3 + r5]

    psubw       m0,     m1
    psubw       m2,     m3

    movu        [r0],            m0
    movu        [r0 + r1],       m2

    movu        m0,     [r2 + r4 * 2]
    movu        m1,     [r3 + r5 * 2]
    movu        m2,     [r2 + r7]
    movu        m3,     [r3 + r8]

    psubw       m0,     m1
    psubw       m2,     m3

    movu        [r0 + r1 * 2],   m0
    movu        [r0 + r6],       m2

    lea         r0,     [r0 + r1 * 4]
    lea         r2,     [r2 + r4 * 4]
    lea         r3,     [r3 + r5 * 4]
%endrep
    RET
%endif
%endmacro
PIXELSUB_PS_W16_H4_avx2 16
PIXELSUB_PS_W16_H4_avx2 32
%else
;-----------------------------------------------------------------------------
; void pixel_sub_ps_16x16(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%macro PIXELSUB_PS_W16_H8_avx2 2
%if ARCH_X86_64
INIT_YMM avx2
cglobal pixel_sub_ps_16x%2, 6, 10, 4, dest, deststride, src0, src1, srcstride0, srcstride1
    add         r1,     r1
    lea         r6,     [r1 * 3]
    mov         r7d,    %2/8

    lea         r9,     [r4 * 3]
    lea         r8,     [r5 * 3]

.loop
    pmovzxbw    m0,     [r2]
    pmovzxbw    m1,     [r3]
    pmovzxbw    m2,     [r2 + r4]
    pmovzxbw    m3,     [r3 + r5]

    psubw       m0,     m1
    psubw       m2,     m3

    movu        [r0],            m0
    movu        [r0 + r1],       m2

    pmovzxbw    m0,     [r2 + 2 * r4]
    pmovzxbw    m1,     [r3 + 2 * r5]
    pmovzxbw    m2,     [r2 + r9]
    pmovzxbw    m3,     [r3 + r8]

    psubw       m0,     m1
    psubw       m2,     m3

    movu        [r0 + r1 * 2],   m0
    movu        [r0 + r6],       m2

    lea         r0,     [r0 + r1 * 4]
    lea         r2,     [r2 + r4 * 4]
    lea         r3,     [r3 + r5 * 4]

    pmovzxbw    m0,     [r2]
    pmovzxbw    m1,     [r3]
    pmovzxbw    m2,     [r2 + r4]
    pmovzxbw    m3,     [r3 + r5]

    psubw       m0,     m1
    psubw       m2,     m3

    movu        [r0],            m0
    movu        [r0 + r1],       m2

    pmovzxbw    m0,     [r2 + 2 * r4]
    pmovzxbw    m1,     [r3 + 2 * r5]
    pmovzxbw    m2,     [r2 + r9]
    pmovzxbw    m3,     [r3 + r8]

    psubw       m0,     m1
    psubw       m2,     m3

    movu        [r0 + r1 * 2],   m0
    movu        [r0 + r6],       m2

    lea         r0,     [r0 + r1 * 4]
    lea         r2,     [r2 + r4 * 4]
    lea         r3,     [r3 + r5 * 4]

    dec         r7d
    jnz         .loop
    RET
%endif
%endmacro

PIXELSUB_PS_W16_H8_avx2 16, 16
PIXELSUB_PS_W16_H8_avx2 16, 32
%endif

;-----------------------------------------------------------------------------
; void pixel_sub_ps_32x%2(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%macro PIXELSUB_PS_W32_H2 2
%if HIGH_BIT_DEPTH
cglobal pixel_sub_ps_32x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov     r6d,    %2/2
    add     r4,     r4
    add     r5,     r5
    add     r1,     r1
.loop:
    movu    m0,     [r2]
    movu    m2,     [r2 + 16]
    movu    m4,     [r2 + 32]
    movu    m6,     [r2 + 48]
    movu    m1,     [r3]
    movu    m3,     [r3 + 16]
    movu    m5,     [r3 + 32]
    movu    m7,     [r3 + 48]
    dec     r6d

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0],       m0
    movu    [r0 + 16],  m2
    movu    [r0 + 32],  m4
    movu    [r0 + 48],  m6

    movu    m0,     [r2 + r4]
    movu    m2,     [r2 + r4 + 16]
    movu    m4,     [r2 + r4 + 32]
    movu    m6,     [r2 + r4 + 48]
    movu    m1,     [r3 + r5]
    movu    m3,     [r3 + r5 + 16]
    movu    m5,     [r3 + r5 + 32]
    movu    m7,     [r3 + r5 + 48]
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0 + r1],      m0
    movu    [r0 + r1 + 16], m2
    movu    [r0 + r1 + 32], m4
    movu    [r0 + r1 + 48], m6
    lea     r0,     [r0 + r1 * 2]

    jnz     .loop
    RET
%else
cglobal pixel_sub_ps_32x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov         r6d,    %2/2
    add         r1,     r1
.loop:
    movh        m0,     [r2]
    movh        m1,     [r2 + 8]
    movh        m2,     [r2 + 16]
    movh        m6,     [r2 + 24]
    movh        m3,     [r3]
    movh        m4,     [r3 + 8]
    movh        m5,     [r3 + 16]
    movh        m7,     [r3 + 24]
    dec         r6d
    pmovzxbw    m0,     m0
    pmovzxbw    m1,     m1
    pmovzxbw    m2,     m2
    pmovzxbw    m6,     m6
    pmovzxbw    m3,     m3
    pmovzxbw    m4,     m4
    pmovzxbw    m5,     m5
    pmovzxbw    m7,     m7

    psubw       m0,     m3
    psubw       m1,     m4
    psubw       m2,     m5
    psubw       m6,     m7

    movu        [r0],       m0
    movu        [r0 + 16],  m1
    movu        [r0 + 32],  m2
    movu        [r0 + 48],  m6

    movh        m0,     [r2 + r4]
    movh        m1,     [r2 + r4 + 8]
    movh        m2,     [r2 + r4 + 16]
    movh        m6,     [r2 + r4 + 24]
    movh        m3,     [r3 + r5]
    movh        m4,     [r3 + r5 + 8]
    movh        m5,     [r3 + r5 + 16]
    movh        m7,     [r3 + r5 + 24]
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    pmovzxbw    m0,     m0
    pmovzxbw    m1,     m1
    pmovzxbw    m2,     m2
    pmovzxbw    m6,     m6
    pmovzxbw    m3,     m3
    pmovzxbw    m4,     m4
    pmovzxbw    m5,     m5
    pmovzxbw    m7,     m7

    psubw       m0,     m3
    psubw       m1,     m4
    psubw       m2,     m5
    psubw       m6,     m7

    movu        [r0 + r1],      m0
    movu        [r0 + r1 + 16], m1
    movu        [r0 + r1 + 32], m2
    movu        [r0 + r1 + 48], m6
    lea         r0,     [r0 + r1 * 2]

    jnz         .loop
    RET
%endif
%endmacro

%if HIGH_BIT_DEPTH
INIT_XMM sse2
PIXELSUB_PS_W32_H2 32, 32
PIXELSUB_PS_W32_H2 32, 64
%else
INIT_XMM sse4
PIXELSUB_PS_W32_H2 32, 32
PIXELSUB_PS_W32_H2 32, 64
%endif


;-----------------------------------------------------------------------------
; void pixel_sub_ps_32x32(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%if HIGH_BIT_DEPTH
%macro PIXELSUB_PS_W32_H4_avx2 1
%if ARCH_X86_64
INIT_YMM avx2
cglobal pixel_sub_ps_32x%1, 6, 10, 4, dest, deststride, src0, src1, srcstride0, srcstride1
    add         r1d,    r1d
    add         r4d,    r4d
    add         r5d,    r5d
    mov         r9d,    %1/4
    lea         r6,     [r1 * 3]
    lea         r7,     [r4 * 3]
    lea         r8,     [r5 * 3]

.loop
    movu        m0,     [r2]
    movu        m1,     [r2 + 32]
    movu        m2,     [r3]
    movu        m3,     [r3 + 32]
    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0],                 m0
    movu        [r0 + 32],            m1

    movu        m0,     [r2 + r4]
    movu        m1,     [r2 + r4 + 32]
    movu        m2,     [r3 + r5]
    movu        m3,     [r3 + r5 + 32]
    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r1],            m0
    movu        [r0 + r1 + 32],       m1

    movu        m0,     [r2 + r4 * 2]
    movu        m1,     [r2 + r4 * 2 + 32]
    movu        m2,     [r3 + r5 * 2]
    movu        m3,     [r3 + r5 * 2 + 32]
    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r1 * 2],        m0
    movu        [r0 + r1 * 2 + 32],   m1

    movu        m0,     [r2 + r7]
    movu        m1,     [r2 + r7 + 32]
    movu        m2,     [r3 + r8]
    movu        m3,     [r3 + r8 + 32]
    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r6],            m0
    movu        [r0 + r6 + 32],       m1

    lea         r0,     [r0 + r1 * 4]
    lea         r2,     [r2 + r4 * 4]
    lea         r3,     [r3 + r5 * 4]
    dec         r9d
    jnz         .loop
    RET
%endif
%endmacro
PIXELSUB_PS_W32_H4_avx2 32
PIXELSUB_PS_W32_H4_avx2 64
%else
;-----------------------------------------------------------------------------
; void pixel_sub_ps_32x32(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%macro PIXELSUB_PS_W32_H8_avx2 2
%if ARCH_X86_64
INIT_YMM avx2
cglobal pixel_sub_ps_32x%2, 6, 10, 4, dest, deststride, src0, src1, srcstride0, srcstride1
    mov        r6d,    %2/8
    add        r1,     r1
    lea         r7,         [r4 * 3]
    lea         r8,         [r5 * 3]
    lea         r9,         [r1 * 3]

.loop:
    pmovzxbw    m0,     [r2]
    pmovzxbw    m1,     [r2 + 16]
    pmovzxbw    m2,     [r3]
    pmovzxbw    m3,     [r3 + 16]

    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0],            m0
    movu        [r0 + 32],       m1

    pmovzxbw    m0,     [r2 + r4]
    pmovzxbw    m1,     [r2 + r4 + 16]
    pmovzxbw    m2,     [r3 + r5]
    pmovzxbw    m3,     [r3 + r5 + 16]

    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r1],       m0
    movu        [r0 + r1 + 32],  m1

    pmovzxbw    m0,     [r2 + 2 * r4]
    pmovzxbw    m1,     [r2 + 2 * r4 + 16]
    pmovzxbw    m2,     [r3 + 2 * r5]
    pmovzxbw    m3,     [r3 + 2 * r5 + 16]

    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r1 * 2 ],           m0
    movu        [r0 + r1 * 2 + 32],       m1

    pmovzxbw    m0,     [r2 + r7]
    pmovzxbw    m1,     [r2 + r7 + 16]
    pmovzxbw    m2,     [r3 + r8]
    pmovzxbw    m3,     [r3 + r8 + 16]


    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r9],           m0
    movu        [r0 + r9 +32],       m1

    lea         r2,     [r2 + r4 * 4]
    lea         r3,     [r3 + r5 * 4]
    lea         r0,     [r0 + r1 * 4]

    pmovzxbw    m0,     [r2]
    pmovzxbw    m1,     [r2 + 16]
    pmovzxbw    m2,     [r3]
    pmovzxbw    m3,     [r3 + 16]

    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 ],           m0
    movu        [r0 + 32],       m1

    pmovzxbw    m0,     [r2 + r4]
    pmovzxbw    m1,     [r2 + r4 + 16]
    pmovzxbw    m2,     [r3 + r5]
    pmovzxbw    m3,     [r3 + r5 + 16]

    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r1],           m0
    movu        [r0 + r1 + 32],       m1

    pmovzxbw    m0,     [r2 + 2 * r4]
    pmovzxbw    m1,     [r2 + 2 * r4 + 16]
    pmovzxbw    m2,     [r3 + 2 * r5]
    pmovzxbw    m3,     [r3 + 2 * r5 + 16]

    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r1 * 2],           m0
    movu        [r0 + r1 * 2 + 32],       m1

    pmovzxbw    m0,     [r2 + r7]
    pmovzxbw    m1,     [r2 + r7 + 16]
    pmovzxbw    m2,     [r3 + r8]
    pmovzxbw    m3,     [r3 + r8 + 16]

    psubw       m0,     m2
    psubw       m1,     m3

    movu        [r0 + r9],           m0
    movu        [r0 + r9 + 32],       m1

    lea         r0,     [r0 + r1 * 4]
    lea         r2,     [r2 + r4 * 4]
    lea         r3,     [r3 + r5 * 4]

    dec         r6d
    jnz         .loop
    RET
%endif
%endmacro

PIXELSUB_PS_W32_H8_avx2 32, 32
PIXELSUB_PS_W32_H8_avx2 32, 64
%endif

;-----------------------------------------------------------------------------
; void pixel_sub_ps_64x%2(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%macro PIXELSUB_PS_W64_H2 2
%if HIGH_BIT_DEPTH
cglobal pixel_sub_ps_64x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov     r6d,    %2/2
    add     r4,     r4
    add     r5,     r5
    add     r1,     r1
.loop:
    movu    m0,     [r2]
    movu    m2,     [r2 + 16]
    movu    m4,     [r2 + 32]
    movu    m6,     [r2 + 48]
    movu    m1,     [r3]
    movu    m3,     [r3 + 16]
    movu    m5,     [r3 + 32]
    movu    m7,     [r3 + 48]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0],       m0
    movu    [r0 + 16],  m2
    movu    [r0 + 32],  m4
    movu    [r0 + 48],  m6

    movu    m0,     [r2 + 64]
    movu    m2,     [r2 + 80]
    movu    m4,     [r2 + 96]
    movu    m6,     [r2 + 112]
    movu    m1,     [r3 + 64]
    movu    m3,     [r3 + 80]
    movu    m5,     [r3 + 96]
    movu    m7,     [r3 + 112]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0 + 64],  m0
    movu    [r0 + 80],  m2
    movu    [r0 + 96],  m4
    movu    [r0 + 112], m6

    movu    m0,     [r2 + r4]
    movu    m2,     [r2 + r4 + 16]
    movu    m4,     [r2 + r4 + 32]
    movu    m6,     [r2 + r4 + 48]
    movu    m1,     [r3 + r5]
    movu    m3,     [r3 + r5 + 16]
    movu    m5,     [r3 + r5 + 32]
    movu    m7,     [r3 + r5 + 48]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0 + r1],      m0
    movu    [r0 + r1 + 16], m2
    movu    [r0 + r1 + 32], m4
    movu    [r0 + r1 + 48], m6

    movu    m0,     [r2 + r4 + 64]
    movu    m2,     [r2 + r4 + 80]
    movu    m4,     [r2 + r4 + 96]
    movu    m6,     [r2 + r4 + 112]
    movu    m1,     [r3 + r5 + 64]
    movu    m3,     [r3 + r5 + 80]
    movu    m5,     [r3 + r5 + 96]
    movu    m7,     [r3 + r5 + 112]
    dec     r6d
    lea     r2,     [r2 + r4 * 2]
    lea     r3,     [r3 + r5 * 2]

    psubw   m0,     m1
    psubw   m2,     m3
    psubw   m4,     m5
    psubw   m6,     m7

    movu    [r0 + r1 + 64],     m0
    movu    [r0 + r1 + 80],     m2
    movu    [r0 + r1 + 96],     m4
    movu    [r0 + r1 + 112],    m6
    lea     r0,     [r0 + r1 * 2]

    jnz     .loop
    RET
%else
cglobal pixel_sub_ps_64x%2, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    mov         r6d,    %2/2
    pxor        m6,     m6
    add         r1,     r1
.loop:
    movu        m1,     [r2]
    movu        m5,     [r2 + 16]
    movu        m3,     [r3]
    movu        m7,     [r3 + 16]

    pmovzxbw    m0,     m1
    pmovzxbw    m4,     m5
    pmovzxbw    m2,     m3
    punpckhbw   m1,     m6
    punpckhbw   m3,     m6
    punpckhbw   m5,     m6

    psubw       m0,     m2
    psubw       m1,     m3
    pmovzxbw    m2,     m7
    punpckhbw   m7,     m6
    psubw       m4,     m2
    psubw       m5,     m7

    movu        m3,     [r2 + 32]
    movu        m7,     [r3 + 32]
    pmovzxbw    m2,     m3
    punpckhbw   m3,     m6

    movu        [r0],       m0
    movu        [r0 + 16],  m1
    movu        [r0 + 32],  m4
    movu        [r0 + 48],  m5

    movu        m1,     [r2 + 48]
    movu        m5,     [r3 + 48]
    pmovzxbw    m0,     m1
    pmovzxbw    m4,     m7
    punpckhbw   m1,     m6
    punpckhbw   m7,     m6

    psubw       m2,     m4
    psubw       m3,     m7

    movu        [r0 + 64],  m2
    movu        [r0 + 80],  m3

    movu        m7,     [r2 + r4]
    movu        m3,     [r3 + r5]
    pmovzxbw    m2,     m5
    pmovzxbw    m4,     m7
    punpckhbw   m5,     m6
    punpckhbw   m7,     m6

    psubw       m0,     m2
    psubw       m1,     m5

    movu        [r0 + 96],  m0
    movu        [r0 + 112], m1

    movu        m2,     [r2 + r4 + 16]
    movu        m5,     [r3 + r5 + 16]
    pmovzxbw    m0,     m3
    pmovzxbw    m1,     m2
    punpckhbw   m3,     m6
    punpckhbw   m2,     m6

    psubw       m4,     m0
    psubw       m7,     m3

    movu        [r0 + r1],      m4
    movu        [r0 + r1 + 16], m7

    movu        m0,     [r2 + r4 + 32]
    movu        m3,     [r3 + r5 + 32]
    dec         r6d
    pmovzxbw    m4,     m5
    pmovzxbw    m7,     m0
    punpckhbw   m5,     m6
    punpckhbw   m0,     m6

    psubw       m1,     m4
    psubw       m2,     m5

    movu        [r0 + r1 + 32], m1
    movu        [r0 + r1 + 48], m2

    movu        m4,     [r2 + r4 + 48]
    movu        m5,     [r3 + r5 + 48]
    lea         r2,     [r2 + r4 * 2]
    lea         r3,     [r3 + r5 * 2]
    pmovzxbw    m1,     m3
    pmovzxbw    m2,     m4
    punpckhbw   m3,     m6
    punpckhbw   m4,     m6

    psubw       m7,     m1
    psubw       m0,     m3

    movu        [r0 + r1 + 64], m7
    movu        [r0 + r1 + 80], m0

    pmovzxbw    m7,     m5
    punpckhbw   m5,     m6
    psubw       m2,     m7
    psubw       m4,     m5

    movu        [r0 + r1 + 96],     m2
    movu        [r0 + r1 + 112],    m4
    lea         r0,     [r0 + r1 * 2]

    jnz         .loop
    RET
%endif
%endmacro

%if HIGH_BIT_DEPTH
INIT_XMM sse2
PIXELSUB_PS_W64_H2 64, 64
%else
INIT_XMM sse4
PIXELSUB_PS_W64_H2 64, 64
%endif


;-----------------------------------------------------------------------------
; void pixel_sub_ps_64x64(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
%if HIGH_BIT_DEPTH
%if ARCH_X86_64
INIT_YMM avx2
cglobal pixel_sub_ps_64x64, 6, 10, 8, dest, deststride, src0, src1, srcstride0, srcstride1
    add         r1d,    r1d
    add         r4d,    r4d
    add         r5d,    r5d
    mov         r9d,    16
    lea         r6,     [r1 * 3]
    lea         r7,     [r4 * 3]
    lea         r8,     [r5 * 3]

.loop
    movu        m0,     [r2]
    movu        m1,     [r2 + 32]
    movu        m2,     [r2 + 64]
    movu        m3,     [r2 + 96]
    movu        m4,     [r3]
    movu        m5,     [r3 + 32]
    movu        m6,     [r3 + 64]
    movu        m7,     [r3 + 96]
    psubw       m0,     m4
    psubw       m1,     m5
    psubw       m2,     m6
    psubw       m3,     m7

    movu        [r0],                 m0
    movu        [r0 + 32],            m1
    movu        [r0 + 64],            m2
    movu        [r0 + 96],            m3

    movu        m0,     [r2 + r4]
    movu        m1,     [r2 + r4 + 32]
    movu        m2,     [r2 + r4 + 64]
    movu        m3,     [r2 + r4 + 96]
    movu        m4,     [r3 + r5]
    movu        m5,     [r3 + r5 + 32]
    movu        m6,     [r3 + r5 + 64]
    movu        m7,     [r3 + r5 + 96]
    psubw       m0,     m4
    psubw       m1,     m5
    psubw       m2,     m6
    psubw       m3,     m7

    movu        [r0 + r1],            m0
    movu        [r0 + r1 + 32],       m1
    movu        [r0 + r1 + 64],       m2
    movu        [r0 + r1 + 96],       m3

    movu        m0,     [r2 + r4 * 2]
    movu        m1,     [r2 + r4 * 2 + 32]
    movu        m2,     [r2 + r4 * 2 + 64]
    movu        m3,     [r2 + r4 * 2 + 96]
    movu        m4,     [r3 + r5 * 2]
    movu        m5,     [r3 + r5 * 2 + 32]
    movu        m6,     [r3 + r5 * 2 + 64]
    movu        m7,     [r3 + r5 * 2 + 96]
    psubw       m0,     m4
    psubw       m1,     m5
    psubw       m2,     m6
    psubw       m3,     m7

    movu        [r0 + r1 * 2],        m0
    movu        [r0 + r1 * 2 + 32],   m1
    movu        [r0 + r1 * 2 + 64],   m2
    movu        [r0 + r1 * 2 + 96],   m3

    movu        m0,     [r2 + r7]
    movu        m1,     [r2 + r7 + 32]
    movu        m2,     [r2 + r7 + 64]
    movu        m3,     [r2 + r7 + 96]
    movu        m4,     [r3 + r8]
    movu        m5,     [r3 + r8 + 32]
    movu        m6,     [r3 + r8 + 64]
    movu        m7,     [r3 + r8 + 96]
    psubw       m0,     m4
    psubw       m1,     m5
    psubw       m2,     m6
    psubw       m3,     m7

    movu        [r0 + r6],            m0
    movu        [r0 + r6 + 32],       m1
    movu        [r0 + r6 + 64],       m2
    movu        [r0 + r6 + 96],       m3

    lea         r0,     [r0 + r1 * 4]
    lea         r2,     [r2 + r4 * 4]
    lea         r3,     [r3 + r5 * 4]
    dec         r9d
    jnz         .loop
    RET
%endif
%else
;-----------------------------------------------------------------------------
; void pixel_sub_ps_64x64(int16_t *dest, intptr_t destride, pixel *src0, pixel *src1, intptr_t srcstride0, intptr_t srcstride1);
;-----------------------------------------------------------------------------
INIT_YMM avx2
cglobal pixel_sub_ps_64x64, 6, 7, 8, dest, deststride, src0, src1, srcstride0, srcstride1
     mov        r6d,    16
     add        r1,     r1

.loop:
    pmovzxbw    m0,     [r2]
    pmovzxbw    m1,     [r2 + 16]
    pmovzxbw    m2,     [r2 + 32]
    pmovzxbw    m3,     [r2 + 48]

    pmovzxbw    m4,     [r3]
    pmovzxbw    m5,     [r3 + 16]
    pmovzxbw    m6,     [r3 + 32]
    pmovzxbw    m7,     [r3 + 48]

    psubw       m0,     m4
    psubw       m1,     m5
    psubw       m2,     m6
    psubw       m3,     m7

    movu        [r0],         m0
    movu        [r0 + 32],    m1
    movu        [r0 + 64],    m2
    movu        [r0 + 96],    m3

    add         r0,     r1
    add         r2,     r4
    add         r3,     r5

    pmovzxbw    m0,     [r2]
    pmovzxbw    m1,     [r2 + 16]
    pmovzxbw    m2,     [r2 + 32]
    pmovzxbw    m3,     [r2 + 48]

    pmovzxbw    m4,     [r3]
    pmovzxbw    m5,     [r3 + 16]
    pmovzxbw    m6,     [r3 + 32]
    pmovzxbw    m7,     [r3 + 48]

    psubw       m0,     m4
    psubw       m1,     m5
    psubw       m2,     m6
    psubw       m3,     m7

    movu        [r0],         m0
    movu        [r0 + 32],    m1
    movu        [r0 + 64],    m2
    movu        [r0 + 96],    m3

    add         r0,     r1
    add         r2,     r4
    add         r3,     r5

    pmovzxbw    m0,     [r2]
    pmovzxbw    m1,     [r2 + 16]
    pmovzxbw    m2,     [r2 + 32]
    pmovzxbw    m3,     [r2 + 48]

    pmovzxbw    m4,     [r3]
    pmovzxbw    m5,     [r3 + 16]
    pmovzxbw    m6,     [r3 + 32]
    pmovzxbw    m7,     [r3 + 48]

    psubw       m0,     m4
    psubw       m1,     m5
    psubw       m2,     m6
    psubw       m3,     m7

    movu        [r0],         m0
    movu        [r0 + 32],    m1
    movu        [r0 + 64],    m2
    movu        [r0 + 96],    m3

    add         r0,     r1
    add         r2,     r4
    add         r3,     r5

    pmovzxbw    m0,     [r2]
    pmovzxbw    m1,     [r2 + 16]
    pmovzxbw    m2,     [r2 + 32]
    pmovzxbw    m3,     [r2 + 48]

    pmovzxbw    m4,     [r3]
    pmovzxbw    m5,     [r3 + 16]
    pmovzxbw    m6,     [r3 + 32]
    pmovzxbw    m7,     [r3 + 48]

    psubw       m0,     m4
    psubw       m1,     m5
    psubw       m2,     m6
    psubw       m3,     m7

    movu        [r0],         m0
    movu        [r0 + 32],    m1
    movu        [r0 + 64],    m2
    movu        [r0 + 96],    m3

    add         r0,     r1
    add         r2,     r4
    add         r3,     r5

    dec         r6d
    jnz         .loop
    RET
%endif
;=============================================================================
; variance
;=============================================================================

%macro VAR_START 1
    pxor  m5, m5    ; sum
    pxor  m6, m6    ; sum squared
%if HIGH_BIT_DEPTH == 0
%if %1
    mova  m7, [pw_00ff]
%elif mmsize < 32
    pxor  m7, m7    ; zero
%endif
%endif ; !HIGH_BIT_DEPTH
%endmacro

%macro VAR_END 2
%if HIGH_BIT_DEPTH
%if mmsize == 8 && %1*%2 == 256
    HADDUW  m5, m2
%else
%if %1 >= 32
    HADDW     m5,    m2
    movd      m7,    r4d
    paddd     m5,    m7
%else
    HADDW   m5, m2
%endif
%endif
%else ; !HIGH_BIT_DEPTH
%if %1 == 64
    HADDW     m5,    m2
    movd      m7,    r4d
    paddd     m5,    m7
%else
    HADDW   m5, m2
%endif
%endif ; HIGH_BIT_DEPTH
    HADDD   m6, m1
%if ARCH_X86_64
    punpckldq m5, m6
    movq   rax, m5
%else
    movd   eax, m5
    movd   edx, m6
%endif
    RET
%endmacro

%macro VAR_END_12bit 2
    HADDD   m5, m1
    HADDD   m6, m1
%if ARCH_X86_64
    punpckldq m5, m6
    movq   rax, m5
%else
    movd   eax, m5
    movd   edx, m6
%endif
    RET
%endmacro

%macro VAR_CORE 0
    paddw     m5, m0
    paddw     m5, m3
    paddw     m5, m1
    paddw     m5, m4
    pmaddwd   m0, m0
    pmaddwd   m3, m3
    pmaddwd   m1, m1
    pmaddwd   m4, m4
    paddd     m6, m0
    paddd     m6, m3
    paddd     m6, m1
    paddd     m6, m4
%endmacro

%macro VAR_2ROW 2
    mov      r2d, %2
%%loop:
%if HIGH_BIT_DEPTH
    movu      m0, [r0]
    movu      m1, [r0+mmsize]
    movu      m3, [r0+%1]
    movu      m4, [r0+%1+mmsize]
%else ; !HIGH_BIT_DEPTH
    mova      m0, [r0]
    punpckhbw m1, m0, m7
    mova      m3, [r0+%1]
    mova      m4, m3
    punpcklbw m0, m7
%endif ; HIGH_BIT_DEPTH
%ifidn %1, r1
    lea       r0, [r0+%1*2]
%else
    add       r0, r1
%endif
%if HIGH_BIT_DEPTH == 0
    punpcklbw m3, m7
    punpckhbw m4, m7
%endif ; !HIGH_BIT_DEPTH
    VAR_CORE
    dec r2d
    jg %%loop
%endmacro

;-----------------------------------------------------------------------------
; int pixel_var_wxh( uint8_t *, intptr_t )
;-----------------------------------------------------------------------------
INIT_MMX mmx2
cglobal pixel_var_16x16, 2,3
    FIX_STRIDES r1
    VAR_START 0
    VAR_2ROW 8*SIZEOF_PIXEL, 16
    VAR_END 16, 16

cglobal pixel_var_8x8, 2,3
    FIX_STRIDES r1
    VAR_START 0
    VAR_2ROW r1, 4
    VAR_END 8, 8

%if HIGH_BIT_DEPTH
%macro VAR 0

%if BIT_DEPTH <= 10
cglobal pixel_var_16x16, 2,3,8
    FIX_STRIDES r1
    VAR_START 0
    VAR_2ROW r1, 8
    VAR_END 16, 16

cglobal pixel_var_32x32, 2,6,8
    FIX_STRIDES r1
    mov       r3,    r0
    VAR_START 0
    VAR_2ROW  r1,    8
    HADDW      m5,    m2
    movd       r4d,   m5
    pxor       m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    lea       r0,    [r3 + 32]
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    VAR_END   32,    32

cglobal pixel_var_64x64, 2,6,8
    FIX_STRIDES r1
    mov       r3,    r0
    VAR_START 0
    VAR_2ROW  r1,    8
    HADDW      m5,    m2
    movd       r4d,   m5
    pxor       m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    lea       r0,    [r3 + 32]
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    lea       r0,    [r3 + 64]
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    lea       r0,    [r3 + 96]
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    VAR_2ROW  r1,    8
    VAR_END   64,    64

%else ; BIT_DEPTH <= 10

cglobal pixel_var_16x16, 2,3,8
    FIX_STRIDES r1
    VAR_START 0
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    mova m7, m5
    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m5, m7
    VAR_END_12bit 16, 16

cglobal pixel_var_32x32, 2,6,8
    FIX_STRIDES r1
    mov r3,    r0
    VAR_START 0

    VAR_2ROW r1, 4
    HADDUWD m5, m1
    mova m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    lea       r0, [r3 + 32]
    pxor      m5, m5
    VAR_2ROW  r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor      m5, m5
    VAR_2ROW  r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m5, m7
    VAR_END_12bit 32, 32

cglobal pixel_var_64x64, 2,6,8
    FIX_STRIDES r1
    mov r3,    r0
    VAR_START 0

    VAR_2ROW r1, 4
    HADDUWD m5, m1
    mova m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    lea       r0, [r3 + 16 * SIZEOF_PIXEL]
    pxor      m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    lea       r0, [r3 + 32 * SIZEOF_PIXEL]
    pxor      m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    lea       r0, [r3 + 48 * SIZEOF_PIXEL]
    pxor      m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m7, m5

    pxor m5, m5
    VAR_2ROW r1, 4
    HADDUWD m5, m1
    paddd m5, m7
    VAR_END_12bit 64, 64

%endif ; BIT_DEPTH <= 10

cglobal pixel_var_8x8, 2,3,8
    lea       r2, [r1*3]
    VAR_START 0
    movu      m0, [r0]
    movu      m1, [r0+r1*2]
    movu      m3, [r0+r1*4]
    movu      m4, [r0+r2*2]
    lea       r0, [r0+r1*8]
    VAR_CORE
    movu      m0, [r0]
    movu      m1, [r0+r1*2]
    movu      m3, [r0+r1*4]
    movu      m4, [r0+r2*2]
    VAR_CORE
    VAR_END 8, 8

%endmacro ; VAR

INIT_XMM sse2
VAR
INIT_XMM avx
VAR
INIT_XMM xop
VAR
%endif ; HIGH_BIT_DEPTH

%if HIGH_BIT_DEPTH == 0
%macro VAR 0
cglobal pixel_var_8x8, 2,3,8
    VAR_START 1
    lea       r2,    [r1 * 3]
    movh      m0,    [r0]
    movh      m3,    [r0 + r1]
    movhps    m0,    [r0 + r1 * 2]
    movhps    m3,    [r0 + r2]
    DEINTB    1, 0, 4, 3, 7
    lea       r0,    [r0 + r1 * 4]
    VAR_CORE
    movh      m0,    [r0]
    movh      m3,    [r0 + r1]
    movhps    m0,    [r0 + r1 * 2]
    movhps    m3,    [r0 + r2]
    DEINTB    1, 0, 4, 3, 7
    VAR_CORE
    VAR_END 8, 8

cglobal pixel_var_16x16_internal
    movu      m0,    [r0]
    movu      m3,    [r0 + r1]
    DEINTB    1, 0, 4, 3, 7
    VAR_CORE
    movu      m0,    [r0 + 2 * r1]
    movu      m3,    [r0 + r2]
    DEINTB    1, 0, 4, 3, 7
    lea       r0,    [r0 + r1 * 4]
    VAR_CORE
    movu      m0,    [r0]
    movu      m3,    [r0 + r1]
    DEINTB    1, 0, 4, 3, 7
    VAR_CORE
    movu      m0,    [r0 + 2 * r1]
    movu      m3,    [r0 + r2]
    DEINTB    1, 0, 4, 3, 7
    lea       r0,    [r0 + r1 * 4]
    VAR_CORE
    movu      m0,    [r0]
    movu      m3,    [r0 + r1]
    DEINTB    1, 0, 4, 3, 7
    VAR_CORE
    movu      m0,    [r0 + 2 * r1]
    movu      m3,    [r0 + r2]
    DEINTB    1, 0, 4, 3, 7
    lea       r0,    [r0 + r1 * 4]
    VAR_CORE
    movu      m0,    [r0]
    movu      m3,    [r0 + r1]
    DEINTB    1, 0, 4, 3, 7
    VAR_CORE
    movu      m0,    [r0 + 2 * r1]
    movu      m3,    [r0 + r2]
    DEINTB    1, 0, 4, 3, 7
    VAR_CORE
    ret

cglobal pixel_var_16x16, 2,3,8
    VAR_START 1
    lea     r2,    [r1 * 3]
    call    pixel_var_16x16_internal
    VAR_END 16, 16

cglobal pixel_var_32x32, 2,4,8
    VAR_START 1
    lea     r2,    [r1 * 3]
    mov     r3,    r0
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r3 + 16]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    VAR_END 32, 32

cglobal pixel_var_64x64, 2,6,8
    VAR_START 1
    lea     r2,    [r1 * 3]
    mov     r3,    r0
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    HADDW     m5,    m2
    movd      r4d,   m5
    pxor      m5,    m5
    lea       r0,    [r3 + 16]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    lea       r0,    [r3 + 32]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r3 + 48]
    HADDW     m5,    m2
    movd      r5d,   m5
    add       r4,    r5
    pxor      m5,    m5
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    lea       r0,    [r0 + r1 * 4]
    call    pixel_var_16x16_internal
    VAR_END 64, 64
%endmacro ; VAR

INIT_XMM sse2
VAR
INIT_XMM avx
VAR
INIT_XMM xop
VAR

INIT_YMM avx2
cglobal pixel_var_16x16, 2,4,7
    VAR_START 0
    mov      r2d, 4
    lea       r3, [r1*3]
.loop:
    pmovzxbw  m0, [r0]
    pmovzxbw  m3, [r0+r1]
    pmovzxbw  m1, [r0+r1*2]
    pmovzxbw  m4, [r0+r3]
    lea       r0, [r0+r1*4]
    VAR_CORE
    dec r2d
    jg .loop
    vextracti128 xm0, m5, 1
    vextracti128 xm1, m6, 1
    paddw  xm5, xm0
    paddd  xm6, xm1
    HADDW  xm5, xm2
    HADDD  xm6, xm1
%if ARCH_X86_64
    punpckldq xm5, xm6
    movq   rax, xm5
%else
    movd   eax, xm5
    movd   edx, xm6
%endif
    RET

INIT_YMM avx2
cglobal pixel_var_32x32, 2,4,7
    VAR_START 0
    mov             r2d, 16

.loop:
    pmovzxbw        m0, [r0]
    pmovzxbw        m3, [r0 + 16]
    pmovzxbw        m1, [r0 + r1]
    pmovzxbw        m4, [r0 + r1 + 16]

    lea             r0, [r0 + r1 * 2]

    VAR_CORE

    dec             r2d
    jg              .loop

    vextracti128   xm0, m5, 1
    vextracti128   xm1, m6, 1
    paddw          xm5, xm0
    paddd          xm6, xm1
    HADDW          xm5, xm2
    HADDD          xm6, xm1

%if ARCH_X86_64
    punpckldq      xm5, xm6
    movq           rax, xm5
%else
    movd           eax, xm5
    movd           edx, xm6
%endif
    RET

INIT_YMM avx2
cglobal pixel_var_64x64, 2,4,7
    VAR_START 0
    mov             r2d, 64

.loop:
    pmovzxbw        m0, [r0]
    pmovzxbw        m3, [r0 + 16]
    pmovzxbw        m1, [r0 + mmsize]
    pmovzxbw        m4, [r0 + mmsize + 16]

    lea             r0, [r0 + r1]

    VAR_CORE

    dec             r2d
    jg              .loop

    pxor            m1, m1
    punpcklwd       m0, m5, m1
    punpckhwd       m5, m1
    paddd           m5, m0
    vextracti128   xm2, m5, 1
    vextracti128   xm1, m6, 1
    paddd          xm5, xm2
    paddd          xm6, xm1
    HADDD          xm5, xm2
    HADDD          xm6, xm1

%if ARCH_X86_64
    punpckldq      xm5, xm6
    movq           rax, xm5
%else
    movd           eax, xm5
    movd           edx, xm6
%endif
    RET
%endif ; !HIGH_BIT_DEPTH

%macro VAR2_END 3
    HADDW   %2, xm1
    movd   r1d, %2
    imul   r1d, r1d
    HADDD   %3, xm1
    shr    r1d, %1
    movd   eax, %3
    movd  [r4], %3
    sub    eax, r1d  ; sqr - (sum * sum >> shift)
    RET
%endmacro

;int scanPosLast(const uint16_t *scan, const coeff_t *coeff, uint16_t *coeffSign, uint16_t *coeffFlag, uint8_t *coeffNum, int numSig, const uint16_t* scanCG4x4, const int trSize)
;{
;    int scanPosLast = 0;
;    do
;    {
;        const uint32_t cgIdx = (uint32_t)scanPosLast >> MLS_CG_SIZE;
;
;        const uint32_t posLast = scan[scanPosLast++];
;
;        const int curCoeff = coeff[posLast];
;        const uint32_t isNZCoeff = (curCoeff != 0);
;        numSig -= isNZCoeff;
;
;        coeffSign[cgIdx] += (uint16_t)(((uint32_t)curCoeff >> 31) << coeffNum[cgIdx]);
;        coeffFlag[cgIdx] = (coeffFlag[cgIdx] << 1) + (uint16_t)isNZCoeff;
;        coeffNum[cgIdx] += (uint8_t)isNZCoeff;
;    }
;    while (numSig > 0);
;    return scanPosLast - 1;
;}

%if ARCH_X86_64 == 1
INIT_XMM avx2,bmi2
cglobal scanPosLast, 7,11,6
    ; convert unit of Stride(trSize) to int16_t
    mov         r7d, r7m
    add         r7d, r7d

    ; loading scan table and convert to Byte
    mova        m0, [r6]
    packuswb    m0, [r6 + mmsize]
    pxor        m1, m0, [pb_15]

    ; clear CG count
    xor         r9d, r9d

    ; m0 - Zigzag scan table
    ; m1 - revert order scan table
    ; m4 - zero
    ; m5 - ones

    pxor        m4, m4
    pcmpeqb     m5, m5
    lea         r8d, [r7d * 3]

.loop:
    ; position of current CG
    movzx       r6d, word [r0]
    lea         r6, [r6 * 2 + r1]
    add         r0, 16 * 2

    ; loading current CG
    movh        m2, [r6]
    movhps      m2, [r6 + r7]
    movh        m3, [r6 + r7 * 2]
    movhps      m3, [r6 + r8]
    packsswb    m2, m3

    ; Zigzag
    pshufb      m3, m2, m0
    pshufb      m2, m1

    ; get sign
    pmovmskb    r6d, m3
    pcmpeqb     m3, m4
    pmovmskb    r10d, m3
    not         r10d
    pext        r6d, r6d, r10d
    mov         [r2 + r9 * 2], r6w

    ; get non-zero flag
    ; TODO: reuse above result with reorder
    pcmpeqb     m2, m4
    pxor        m2, m5
    pmovmskb    r6d, m2
    mov         [r3 + r9 * 2], r6w

    ; get non-zero number, POPCNT is faster
    pabsb       m2, m2
    psadbw      m2, m4
    movhlps     m3, m2
    paddd       m2, m3
    movd        r6d, m2
    mov         [r4 + r9], r6b

    inc         r9d
    sub         r5d, r6d
    jg         .loop

    ; fixup last CG non-zero flag
    dec         r9d
    movzx       r0d, word [r3 + r9 * 2]
;%if cpuflag(bmi1)  ; 2uops?
;    tzcnt       r1d, r0d
;%else
    bsf         r1d, r0d
;%endif
    shrx        r0d, r0d, r1d
    mov         [r3 + r9 * 2], r0w

    ; get last pos
    mov         eax, r9d
    shl         eax, 4
    xor         r1d, 15
    add         eax, r1d
    RET


; t3 must be ecx, since it's used for shift.
%if WIN64
    DECLARE_REG_TMP 3,1,2,0
%elif ARCH_X86_64
    DECLARE_REG_TMP 0,1,2,3
%else ; X86_32
    %error Unsupport platform X86_32
%endif
INIT_CPUFLAGS
cglobal scanPosLast_x64, 5,12
    mov         r10, r3mp
    movifnidn   t0, r0mp
    mov         r5d, r5m
    xor         r11d, r11d                  ; cgIdx
    xor         r7d, r7d                    ; tmp for non-zero flag

.loop:
    xor         r8d, r8d                    ; coeffSign[]
    xor         r9d, r9d                    ; coeffFlag[]
    xor         t3d, t3d                    ; coeffNum[]

%assign x 0
%rep 16
    movzx       r6d, word [t0 + x * 2]
    movsx       r6d, word [t1 + r6 * 2]
    test        r6d, r6d
    setnz       r7b
    shr         r6d, 31
    shl         r6d, t3b
    or          r8d, r6d
    lea         r9, [r9 * 2 + r7]
    add         t3d, r7d
%assign x x+1
%endrep

    ; store latest group data
    mov         [t2 + r11 * 2], r8w
    mov         [r10 + r11 * 2], r9w
    mov         [r4 + r11], t3b
    inc         r11d

    add         t0, 16 * 2
    sub         r5d, t3d
    jnz        .loop

    ; store group data
    bsf         t3d, r9d
    shr         r9d, t3b
    mov         [r10 + (r11 - 1) * 2], r9w

    ; get posLast
    shl         r11d, 4
    sub         r11d, t3d
    lea         eax, [r11d - 1]
    RET
%endif


;-----------------------------------------------------------------------------
; uint32_t[sumSign last first] findPosFirstLast(const int16_t *dstCoeff, const intptr_t trSize, const uint16_t scanTbl[16], uint32_t *absSum)
;-----------------------------------------------------------------------------
INIT_XMM ssse3
cglobal findPosFirstLast, 3,3,4
    ; convert stride to int16_t
    add         r1d, r1d

    ; loading scan table and convert to Byte
    mova        m0, [r2]
    packuswb    m0, [r2 + mmsize]

    ; loading 16 of coeff
    movh        m1, [r0]
    movhps      m1, [r0 + r1]
    movh        m2, [r0 + r1 * 2]
    lea         r1d, [r1 * 3]
    movhps      m2, [r0 + r1]
    pxor        m3, m1, m2
    packsswb    m1, m2

    ; get absSum
    movhlps     m2, m3
    pxor        m3, m2
    pshufd      m2, m3, q2301
    pxor        m3, m2
    movd        r0d, m3
    mov         r2d, r0d
    shr         r2d, 16
    xor         r2d, r0d
    shl         r2d, 31

    ; get non-zero mask
    pxor        m2, m2
    pcmpeqb     m1, m2

    ; reorder by Zigzag scan
    pshufb      m1, m0

    ; get First and Last pos
    pmovmskb    r0d, m1
    not         r0d
    bsr         r1w, r0w
    bsf         eax, r0d    ; side effect: clear AH to Zero
    shl         r1d, 8
    or          eax, r2d    ; merge absSumSign
    or          eax, r1d    ; merge lastNZPosInCG
    RET


; uint32_t costCoeffNxN(uint16_t *scan, coeff_t *coeff, intptr_t trSize, uint16_t *absCoeff, uint8_t *tabSigCtx, uint16_t scanFlagMask, uint8_t *baseCtx, int offset, int subPosBase)
;for (int i = 0; i < MLS_CG_SIZE; i++)
;{
;    tmpCoeff[i * MLS_CG_SIZE + 0] = (uint16_t)abs(coeff[blkPosBase + i * trSize + 0]);
;    tmpCoeff[i * MLS_CG_SIZE + 1] = (uint16_t)abs(coeff[blkPosBase + i * trSize + 1]);
;    tmpCoeff[i * MLS_CG_SIZE + 2] = (uint16_t)abs(coeff[blkPosBase + i * trSize + 2]);
;    tmpCoeff[i * MLS_CG_SIZE + 3] = (uint16_t)abs(coeff[blkPosBase + i * trSize + 3]);
;}
;do
;{
;    uint32_t blkPos, sig, ctxSig;
;    blkPos = g_scan4x4[codingParameters.scanType][scanPosSigOff];
;    const uint32_t posZeroMask = (subPosBase + scanPosSigOff) ? ~0 : 0;
;    sig     = scanFlagMask & 1;
;    scanFlagMask >>= 1;
;    if (scanPosSigOff + (subSet == 0) + numNonZero)
;    {
;        const uint32_t cnt = tabSigCtx[blkPos] + offset + posOffset;
;        ctxSig = cnt & posZeroMask;
;
;        const uint32_t mstate = baseCtx[ctxSig];
;        const uint32_t mps = mstate & 1;
;        const uint32_t stateBits = x265_entropyStateBits[mstate ^ sig];
;        uint32_t nextState = (stateBits >> 24) + mps;
;        if ((mstate ^ sig) == 1)
;            nextState = sig;
;        baseCtx[ctxSig] = (uint8_t)nextState;
;        sum += stateBits;
;    }
;    absCoeff[numNonZero] = tmpCoeff[blkPos];
;    numNonZero += sig;
;    scanPosSigOff--;
;}
;while(scanPosSigOff >= 0);
; sum &= 0xFFFFFF

%if ARCH_X86_64
; uint32_t costCoeffNxN(uint16_t *scan, coeff_t *coeff, intptr_t trSize, uint16_t *absCoeff, uint8_t *tabSigCtx, uint16_t scanFlagMask, uint8_t *baseCtx, int offset, int scanPosSigOff, int subPosBase)
INIT_XMM sse4
cglobal costCoeffNxN, 6,11,6
    add         r2d, r2d

    ; abs(coeff)
    movh        m1, [r1]
    movhps      m1, [r1 + r2]
    movh        m2, [r1 + r2 * 2]
    lea         r2, [r2 * 3]
    movhps      m2, [r1 + r2]
    pabsw       m1, m1
    pabsw       m2, m2
    ; r[1-2] free here

    ; WARNING: beyond-bound read here!
    ; loading scan table
    mov         r2d, r8m
    xor         r2d, 15
    movu        m0, [r0 + r2 * 2]
    movu        m3, [r0 + r2 * 2 + mmsize]
    packuswb    m0, m3
    pxor        m0, [pb_15]
    xchg        r2d, r8m
    ; r[0-1] free here

    ; reorder coeff
    mova        m3, [deinterleave_shuf]
    pshufb      m1, m3
    pshufb      m2, m3
    punpcklqdq  m3, m1, m2
    punpckhqdq  m1, m2
    pshufb      m3, m0
    pshufb      m1, m0
    punpcklbw   m2, m3, m1
    punpckhbw   m3, m1
    ; r[0-1], m[1] free here

    ; loading tabSigCtx (+offset)
    mova        m1, [r4]
    pshufb      m1, m0
    movd        m4, r7m
    pxor        m5, m5
    pshufb      m4, m5
    paddb       m1, m4

    ; register mapping
    ; m0 - Zigzag
    ; m1 - sigCtx
    ; {m3,m2} - abs(coeff)
    ; r0 - x265_entropyStateBits
    ; r1 - baseCtx
    ; r2 - scanPosSigOff
    ; r3 - absCoeff
    ; r4 - nonZero
    ; r5 - scanFlagMask
    ; r6 - sum
    lea         r0, [private_prefix %+ _entropyStateBits]
    mov         r1, r6mp
    xor         r6d, r6d
    xor         r4d, r4d
    xor         r8d, r8d

    test        r2d, r2d
    jz         .idx_zero

.loop:
;   {
;        const uint32_t cnt = tabSigCtx[blkPos] + offset + posOffset;
;        ctxSig = cnt & posZeroMask;
;        const uint32_t mstate = baseCtx[ctxSig];
;        const uint32_t mps = mstate & 1;
;        const uint32_t stateBits = x265_entropyStateBits[mstate ^ sig];
;        uint32_t nextState = (stateBits >> 24) + mps;
;        if ((mstate ^ sig) == 1)
;            nextState = sig;
;        baseCtx[ctxSig] = (uint8_t)nextState;
;        sum += stateBits;
;    }
;    absCoeff[numNonZero] = tmpCoeff[blkPos];
;    numNonZero += sig;
;    scanPosSigOff--;

    pextrw      [r3 + r4 * 2], m2, 0            ; absCoeff[numNonZero] = tmpCoeff[blkPos]
    shr         r5d, 1
    setc        r8b                             ; r8 = sig
    add         r4d, r8d                        ; numNonZero += sig
    palignr     m4, m3, m2, 2
    psrldq      m3, 2
    mova        m2, m4
    movd        r7d, m1                         ; r7 = ctxSig
    movzx       r7d, r7b
    psrldq      m1, 1
    movzx       r9d, byte [r1 + r7]             ; mstate = baseCtx[ctxSig]
    mov         r10d, r9d
    and         r10d, 1                         ; mps = mstate & 1
    xor         r9d, r8d                        ; r9 = mstate ^ sig
    add         r6d, [r0 + r9 * 4]              ; sum += x265_entropyStateBits[mstate ^ sig]
    add         r10b, byte [r0 + r9 * 4 + 3]    ; nextState = (stateBits >> 24) + mps
    cmp         r9b, 1
    cmove       r10d, r8d
    mov    byte [r1 + r7], r10b

    dec         r2d
    jg         .loop

.idx_zero:
    pextrw      [r3 + r4 * 2], m2, 0            ; absCoeff[numNonZero] = tmpCoeff[blkPos]
    add         r4b, r8m
    xor         r2d, r2d
    cmp    word r9m, 0
    sete        r2b
    add         r4b, r2b
    jz         .exit

    dec         r2b
    movd        r3d, m1
    and         r2d, r3d

    movzx       r3d, byte [r1 + r2]             ; mstate = baseCtx[ctxSig]
    mov         r4d, r5d
    xor         r5d, r3d                        ; r0 = mstate ^ sig
    and         r3d, 1                          ; mps = mstate & 1
    add         r6d, [r0 + r5 * 4]              ; sum += x265_entropyStateBits[mstate ^ sig]
    add         r3b, [r0 + r5 * 4 + 3]          ; nextState = (stateBits >> 24) + mps
    cmp         r5b, 1
    cmove       r3d, r4d
    mov    byte [r1 + r2], r3b

.exit:
%ifnidn eax,r6d
    mov         eax, r6d
%endif
    and         eax, 0xFFFFFF
    RET


; uint32_t costCoeffNxN(uint16_t *scan, coeff_t *coeff, intptr_t trSize, uint16_t *absCoeff, uint8_t *tabSigCtx, uint16_t scanFlagMask, uint8_t *baseCtx, int offset, int scanPosSigOff, int subPosBase)
INIT_YMM avx2,bmi2
cglobal costCoeffNxN, 6,10,5
    add             r2d, r2d

    ; abs(coeff)
    movq            xm1, [r1]
    movhps          xm1, [r1 + r2]
    movq            xm2, [r1 + r2 * 2]
    lea             r2, [r2 * 3]
    movhps          xm2, [r1 + r2]
    vinserti128     m1, m1, xm2, 1
    pabsw           m1, m1
    ; r[1-2] free here

    ; loading tabSigCtx
    mova            xm2, [r4]
    ; r[4] free here

    ; WARNING: beyond-bound read here!
    ; loading scan table
    mov             r2d, r8m
    bzhi            r4d, r5d, r2d                   ; clear non-scan mask bits
    mov             r6d, r2d
    xor             r2d, 15
    movu            m0, [r0 + r2 * 2]
    packuswb        m0, m0
    pxor            m0, [pb_15]
    vpermq          m0, m0, q3120
    add             r4d, r2d                        ; r4d = (scanPosSigOff == 15) -> (numNonZero == 0)
    mov             r2d, r6d

    ; reorder tabSigCtx (+offset)
    pshufb          xm2, xm0
    vpbroadcastb    xm3, r7m
    paddb           xm2, xm3
    ; r[0-1] free here

    ; reorder coeff
    pshufb          m1, [deinterleave_shuf]
    vpermq          m1, m1, q3120
    pshufb          m1, m0
    vpermq          m1, m1, q3120
    pshufb          m1, [interleave_shuf]
    ; r[0-1], m[2-3] free here

    ; sig mask
    pxor            xm3, xm3
    movd            xm4, r5d
    vpbroadcastw    m4, xm4
    pandn           m4, m4, [pw_exp2_0_15]
    pcmpeqw         m4, m3

    ; absCoeff[numNonZero] = tmpCoeff[blkPos]
    ; [0-3]
    movq            r0, xm4
    movq            r1, xm1
    pext            r6, r1, r0
    mov       qword [r3], r6
    popcnt          r0, r0
    shr             r0, 3
    add             r3, r0

    ; [4-7]
    pextrq          r0, xm4, 1
    pextrq          r1, xm1, 1
    pext            r6, r1, r0
    mov       qword [r3], r6
    popcnt          r0, r0
    shr             r0, 3
    add             r3, r0

    ; [8-B]
    vextracti128    xm4, m4, 1
    movq            r0, xm4
    vextracti128    xm1, m1, 1
    movq            r1, xm1
    pext            r6, r1, r0
    mov       qword [r3], r6
    popcnt          r0, r0
    shr             r0, 3
    add             r3, r0

    ; [C-F]
    pextrq          r0, xm4, 1
    pextrq          r1, xm1, 1
    pext            r6, r1, r0
    mov       qword [r3], r6
    ; r[0-1,3] free here

    ; register mapping
    ; m0 - Zigzag
    ; m1 - sigCtx
    ; r0 - x265_entropyStateBits
    ; r1 - baseCtx
    ; r2 - scanPosSigOff
    ; r5 - scanFlagMask
    ; r6 - sum
    ; {r3,r4} - ctxSig[15-0]
    ; r8m - (numNonZero != 0) || (subPosBase == 0)
    lea             r0, [private_prefix %+ _entropyStateBits]
    mov             r1, r6mp
    xor             r6d, r6d
    xor             r8d, r8d

    test            r2d, r2d
    jz             .idx_zero

;   {
;        const uint32_t cnt = tabSigCtx[blkPos] + offset + posOffset;
;        ctxSig = cnt & posZeroMask;
;        const uint32_t mstate = baseCtx[ctxSig];
;        const uint32_t mps = mstate & 1;
;        const uint32_t stateBits = x265_entropyStateBits[mstate ^ sig];
;        uint32_t nextState = (stateBits >> 24) + mps;
;        if ((mstate ^ sig) == 1)
;            nextState = sig;
;        baseCtx[ctxSig] = (uint8_t)nextState;
;        sum += stateBits;
;    }
;    absCoeff[numNonZero] = tmpCoeff[blkPos];
;    numNonZero += sig;
;    scanPosSigOff--;
.loop:
    shr             r5d, 1
    setc            r8b                             ; r8 = sig
    movd            r7d, xm2                        ; r7 = ctxSig
    movzx           r7d, r7b
    psrldq          xm2, 1
    movzx           r9d, byte [r1 + r7]             ; mstate = baseCtx[ctxSig]
    mov             r3d, r9d
    and             r3b, 1                          ; mps = mstate & 1
    xor             r9d, r8d                        ; r9 = mstate ^ sig
    add             r6d, [r0 + r9 * 4]              ; sum += entropyStateBits[mstate ^ sig]
    add             r3b, byte [r0 + r9 * 4 + 3]     ; nextState = (stateBits >> 24) + mps
    cmp             r9d, 1
    cmove           r3d, r8d
    mov        byte [r1 + r7], r3b

    dec             r2d
    jg             .loop

.idx_zero:
    xor             r2d, r2d
    cmp        word r9m, 0
    sete            r2b
    add             r4d, r2d                        ; (numNonZero != 0) || (subPosBase == 0)
    jz             .exit

    dec             r2b
    movd            r3d, xm2
    and             r2d, r3d

    movzx           r3d, byte [r1 + r2]             ; mstate = baseCtx[ctxSig]
    mov             r4d, r5d
    xor             r5d, r3d                        ; r0 = mstate ^ sig
    and             r3b, 1                          ; mps = mstate & 1
    add             r6d, [r0 + r5 * 4]              ; sum += x265_entropyStateBits[mstate ^ sig]
    add             r3b, [r0 + r5 * 4 + 3]          ; nextState = (stateBits >> 24) + mps
    cmp             r5b, 1
    cmove           r3d, r4d
    mov        byte [r1 + r2], r3b

.exit:
%ifnidn eax,r6d
    mov             eax, r6d
%endif
    and             eax, 0xFFFFFF
    RET
%endif ; ARCH_X86_64


;uint32_t goRiceParam = 0;
;int firstCoeff2 = 1;
;uint32_t baseLevelN = 0x5555AAAA; // 2-bits encode format baseLevel
;idx = 0;
;do
;{
;    int baseLevel = (baseLevelN & 3) | firstCoeff2;
;    baseLevelN >>= 2;
;    int codeNumber = absCoeff[idx] - baseLevel;
;    if (codeNumber >= 0)
;    {
;        uint32_t length = 0;
;        codeNumber = ((uint32_t)codeNumber >> goRiceParam) - COEF_REMAIN_BIN_REDUCTION;
;        if (codeNumber >= 0)
;        {
;            {
;                unsigned long cidx;
;                CLZ(cidx, codeNumber + 1);
;                length = cidx;
;            }
;            codeNumber = (length + length);
;        }
;        sum += (COEF_REMAIN_BIN_REDUCTION + 1 + goRiceParam + codeNumber);
;        if (absCoeff[idx] > (COEF_REMAIN_BIN_REDUCTION << goRiceParam))
;            goRiceParam = (goRiceParam + 1) - (goRiceParam >> 2);
;    }
;    if (absCoeff[idx] >= 2)
;        firstCoeff2 = 0;
;    idx++;
;}
;while(idx < numNonZero);

; uint32_t costCoeffRemain(uint16_t *absCoeff, int numNonZero, int idx)
INIT_XMM sse4
cglobal costCoeffRemain, 0,7,1
    ; assign RCX to R3
    ; RAX always in R6 and free
  %if WIN64
    DECLARE_REG_TMP 3,1,2,0
    mov         t0, r0
    mov         r4d, r2d
  %elif ARCH_X86_64
    ; *nix x64 didn't do anything
    DECLARE_REG_TMP 0,1,2,3
    mov         r4d, r2d
  %else ; X86_32
    DECLARE_REG_TMP 6,3,2,1
    mov         t0, r0m
    mov         r4d, r2m
  %endif

    xor         t3d, t3d
    xor         r5d, r5d

    lea         t0, [t0 + r4 * 2]
    mov         r2d, 3

    ; register mapping
    ; r2d - baseLevel & tmp
    ; r4d - idx
    ; t3  - goRiceParam
    ; eax - absCoeff[idx] & tmp
    ; r5  - sum

.loop:
    mov         eax, 1
    cmp         r4d, 8
    cmovge      r2d, eax

    movzx       eax, word [t0]
    add         t0, 2
    sub         eax, r2d                ; codeNumber = absCoeff[idx] - baseLevel
    jl         .next

    shr         eax, t3b                ; codeNumber = ((uint32_t)codeNumber >> goRiceParam) - COEF_REMAIN_BIN_REDUCTION

    lea         r2d, [rax - 3 + 1]      ; CLZ(cidx, codeNumber + 1);
    bsr         r2d, r2d
    add         r2d, r2d                ; codeNumber = (length + length)

    sub         eax, 3
    cmovge      eax, r2d

    lea         eax, [3 + 1 + t3 + rax] ; sum += (COEF_REMAIN_BIN_REDUCTION + 1 + goRiceParam + codeNumber)
    add         r5d, eax

    ; if (absCoeff[idx] > (COEF_REMAIN_BIN_REDUCTION << goRiceParam))
    ;     goRiceParam = (goRiceParam + 1) - (goRiceParam >> 2);
    cmp         t3d, 4
    setl        al

    mov         r2d, 3
    shl         r2d, t3b
    cmp         word [t0 - 2], r2w
    setg        r2b
    and         al, r2b
    add         t3b, al

.next:
    inc         r4d
    mov         r2d, 2
    cmp         r4d, r1m
    jl         .loop

    mov         eax, r5d
    RET


; uint32_t costC1C2Flag(uint16_t *absCoeff, intptr_t numC1Flag, uint8_t *baseCtxMod, intptr_t ctxOffset)
;idx = 0;
;do
;{
;    uint32_t symbol1 = absCoeff[idx] > 1;
;    uint32_t symbol2 = absCoeff[idx] > 2;
;    {
;        const uint32_t mstate = baseCtxMod[c1];
;        baseCtxMod[c1] = sbacNext(mstate, symbol1);
;        sum += sbacGetEntropyBits(mstate, symbol1);
;    }
;    if (symbol1)
;        c1Next = 0;
;    if (symbol1 + firstC2Flag == 3)
;        firstC2Flag = symbol2;
;    if (symbol1 + firstC2Idx == 9)
;        firstC2Idx  = idx;
;    c1 = (c1Next & 3);
;    c1Next >>= 2;
;    idx++;
;}
;while(idx < numC1Flag);
;if (!c1)
;{
;    baseCtxMod = &m_contextState[(bIsLuma ? 0 : NUM_ABS_FLAG_CTX_LUMA) + OFF_ABS_FLAG_CTX + ctxSet];
;    {
;        const uint32_t mstate = baseCtxMod[0];
;        baseCtxMod[0] = sbacNext(mstate, firstC2Flag);
;        sum += sbacGetEntropyBits(mstate, firstC2Flag);
;    }
;}
;m_fracBits += (sum & 0xFFFFFF);


; TODO: we need more register, so I writen code as x64 only, but it is easy to portab to x86 platform
%if ARCH_X86_64
INIT_XMM sse2
cglobal costC1C2Flag, 4,12,2

    mova        m0, [r0]
    packsswb    m0, m0

    pcmpgtb     m1, m0, [pb_1]
    pcmpgtb     m0, [pb_2]

    ; get mask for 'X>1'
    pmovmskb    r0d, m1
    mov         r11d, r0d

    ; clear unavailable coeff flags
    xor         r6d, r6d
    bts         r6d, r1d
    dec         r6d
    and         r11d, r6d

    ; calculate firstC2Idx
    or          r11d, 0x100                     ; default value setting to 8
    bsf         r11d, r11d

    lea         r5, [private_prefix %+ _entropyStateBits]
    xor         r6d, r6d
    mov         r4d, 0xFFFFFFF9

    ; register mapping
    ; r4d       - nextC1
    ; r5        - x265_entropyStateBits
    ; r6d       - sum
    ; r[7-10]   - tmp
    ; r11d      - firstC2Idx (not use in loop)

    ; process c1 flag
.loop:
    ; const uint32_t mstate = baseCtx[ctxSig];
    ; const uint32_t mps = mstate & 1;
    ; const uint32_t stateBits = x265_entropyStateBits[mstate ^ sig];
    ; uint32_t nextState = (stateBits >> 24) + mps;
    ; if ((mstate ^ sig) == 1)
    ;     nextState = sig;
    mov         r10d, r4d                       ; c1
    and         r10d, 3
    shr         r4d, 2

    xor         r7d, r7d
    shr         r0d, 1
    cmovc       r4d, r7d                        ; c1 <- 0 when C1Flag=1
    setc        r7b                             ; symbol1

    movzx       r8d, byte [r2 + r10]            ; mstate = baseCtx[c1]
    mov         r9d, r7d                        ; sig = symbol1
    xor         r7d, r8d                        ; mstate ^ sig
    and         r8d, 1                          ; mps = mstate & 1
    add         r6d, [r5 + r7 * 4]              ; sum += x265_entropyStateBits[mstate ^ sig]
    add         r8b, [r5 + r7 * 4 + 3]          ; nextState = (stateBits >> 24) + mps
    cmp         r7b, 1                          ; if ((mstate ^ sig) == 1) nextState = sig;
    cmove       r8d, r9d
    mov    byte [r2 + r10], r8b

    dec         r1d
    jg         .loop

    ; check and generate c1 flag
    shl         r4d, 30
    jnz        .quit

    ; move to c2 ctx
    add         r2, r3

    ; process c2 flag
    pmovmskb    r8d, m0
    bt          r8d, r11d
    setc        r7b

    movzx       r8d, byte [r2]                  ; mstate = baseCtx[c1]
    mov         r1d, r7d                        ; sig = symbol1
    xor         r7d, r8d                        ; mstate ^ sig
    and         r8d, 1                          ; mps = mstate & 1
    add         r6d, [r5 + r7 * 4]              ; sum += x265_entropyStateBits[mstate ^ sig]
    add         r8b, [r5 + r7 * 4 + 3]          ; nextState = (stateBits >> 24) + mps
    cmp         r7b, 1                          ; if ((mstate ^ sig) == 1) nextState = sig;
    cmove       r8d, r1d
    mov    byte [r2], r8b

.quit:
    shrd        r4d, r11d, 4
%ifnidn r6d,eax
    mov         eax, r6d
%endif
    and         eax, 0x00FFFFFF
    or          eax, r4d
    RET
%endif ; ARCH_X86_64
