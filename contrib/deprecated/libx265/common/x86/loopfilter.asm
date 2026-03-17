;*****************************************************************************
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Min Chen <chenm001@163.com>
;*          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
;*          Nabajit Deka <nabajit@multicorewareinc.com>
;*          Dnyaneshwar Gorade <dnyaneshwar@multicorewareinc.com>
;*          Murugan Vairavel <murugan@multicorewareinc.com>
;*          Yuvaraj Venkatesh <yuvaraj@multicorewareinc.com>
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

SECTION .text
cextern pb_1
cextern pb_2
cextern pb_3
cextern pb_4
cextern pb_01
cextern pb_0123
cextern pb_15
cextern pb_31
cextern pb_124
cextern pb_128
cextern pw_1
cextern pw_n1
cextern pw_2
cextern pw_4
cextern pw_pixel_max
cextern pb_movemask
cextern pb_movemask_32
cextern hmul_16p
cextern pw_1_ffff
cextern pb_shuf_off4
cextern pw_shuf_off4

;============================================================================================================
; void saoCuOrgE0(pixel * rec, int8_t * offsetEo, int lcuWidth, int8_t* signLeft, intptr_t stride)
;============================================================================================================
INIT_XMM sse4
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE0, 4,5,9
    mov         r4d, r4m
    movh        m6,  [r1]
    movzx       r1d, byte [r3]
    pxor        m5, m5
    neg         r1b
    movd        m0, r1d
    lea         r1, [r0 + r4 * 2]
    mov         r4d, r2d

.loop:
    movu        m7, [r0]
    movu        m8, [r0 + 16]
    movu        m2, [r0 + 2]
    movu        m1, [r0 + 18]

    pcmpgtw     m3, m7, m2
    pcmpgtw     m2, m7
    pcmpgtw     m4, m8, m1
    pcmpgtw     m1, m8 

    packsswb    m3, m4
    packsswb    m2, m1

    pand        m3, [pb_1]
    por         m3, m2

    palignr     m2, m3, m5, 15
    por         m2, m0

    mova        m4, [pw_pixel_max]
    psignb      m2, [pb_128]                ; m2 = signLeft
    pxor        m0, m0
    palignr     m0, m3, 15
    paddb       m3, m2
    paddb       m3, [pb_2]                  ; m2 = uiEdgeType
    pshufb      m2, m6, m3
    pmovsxbw    m3, m2                      ; offsetEo
    punpckhbw   m2, m2
    psraw       m2, 8
    paddw       m7, m3
    paddw       m8, m2
    pmaxsw      m7, m5
    pmaxsw      m8, m5
    pminsw      m7, m4
    pminsw      m8, m4
    movu        [r0], m7
    movu        [r0 + 16], m8

    add         r0q, 32
    sub         r2d, 16
    jnz        .loop

    movzx       r3d, byte [r3 + 1]
    neg         r3b
    movd        m0, r3d
.loopH:
    movu        m7, [r1]
    movu        m8, [r1 + 16]
    movu        m2, [r1 + 2]
    movu        m1, [r1 + 18]

    pcmpgtw     m3, m7, m2
    pcmpgtw     m2, m7
    pcmpgtw     m4, m8, m1
    pcmpgtw     m1, m8 

    packsswb    m3, m4
    packsswb    m2, m1

    pand        m3, [pb_1]
    por         m3, m2

    palignr     m2, m3, m5, 15
    por         m2, m0

    mova        m4, [pw_pixel_max]
    psignb      m2, [pb_128]                ; m2 = signLeft
    pxor        m0, m0
    palignr     m0, m3, 15
    paddb       m3, m2
    paddb       m3, [pb_2]                  ; m2 = uiEdgeType
    pshufb      m2, m6, m3
    pmovsxbw    m3, m2                      ; offsetEo
    punpckhbw   m2, m2
    psraw       m2, 8
    paddw       m7, m3
    paddw       m8, m2
    pmaxsw      m7, m5
    pmaxsw      m8, m5
    pminsw      m7, m4
    pminsw      m8, m4
    movu        [r1], m7
    movu        [r1 + 16], m8

    add         r1q, 32
    sub         r4d, 16
    jnz        .loopH
    RET

%else ; HIGH_BIT_DEPTH == 1

cglobal saoCuOrgE0, 5, 5, 8, rec, offsetEo, lcuWidth, signLeft, stride

    mov         r4d, r4m
    mova        m4,  [pb_128]                ; m4 = [80]
    pxor        m5,  m5                      ; m5 = 0
    movu        m6,  [r1]                    ; m6 = offsetEo

    movzx       r1d, byte [r3]
    inc         r3
    neg         r1b
    movd        m0, r1d
    lea         r1, [r0 + r4]
    mov         r4d, r2d

.loop:
    movu        m7, [r0]                    ; m7 = rec[x]
    movu        m2, [r0 + 1]                ; m2 = rec[x+1]

    pxor        m1, m7, m4
    pxor        m3, m2, m4
    pcmpgtb     m2, m1, m3
    pcmpgtb     m3, m1
    pand        m2, [pb_1]
    por         m2, m3

    pslldq      m3, m2, 1
    por         m3, m0

    psignb      m3, m4                      ; m3 = signLeft
    pxor        m0, m0
    palignr     m0, m2, 15
    paddb       m2, m3
    paddb       m2, [pb_2]                  ; m2 = uiEdgeType
    pshufb      m3, m6, m2
    pmovzxbw    m2, m7                      ; rec
    punpckhbw   m7, m5
    pmovsxbw    m1, m3                      ; offsetEo
    punpckhbw   m3, m3
    psraw       m3, 8
    paddw       m2, m1
    paddw       m7, m3
    packuswb    m2, m7
    movu        [r0], m2

    add         r0q, 16
    sub         r2d, 16
    jnz        .loop

    movzx       r3d, byte [r3]
    neg         r3b
    movd        m0, r3d
.loopH:
    movu        m7, [r1]                    ; m7 = rec[x]
    movu        m2, [r1 + 1]                ; m2 = rec[x+1]

    pxor        m1, m7, m4
    pxor        m3, m2, m4
    pcmpgtb     m2, m1, m3
    pcmpgtb     m3, m1
    pand        m2, [pb_1]
    por         m2, m3

    pslldq      m3, m2, 1
    por         m3, m0

    psignb      m3, m4                      ; m3 = signLeft
    pxor        m0, m0
    palignr     m0, m2, 15
    paddb       m2, m3
    paddb       m2, [pb_2]                  ; m2 = uiEdgeType
    pshufb      m3, m6, m2
    pmovzxbw    m2, m7                      ; rec
    punpckhbw   m7, m5
    pmovsxbw    m1, m3                      ; offsetEo
    punpckhbw   m3, m3
    psraw       m3, 8
    paddw       m2, m1
    paddw       m7, m3
    packuswb    m2, m7
    movu        [r1], m2

    add         r1q, 16
    sub         r4d, 16
    jnz        .loopH
    RET
%endif ; HIGH_BIT_DEPTH == 0

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE0, 4,4,9
    vbroadcasti128  m6, [r1]
    movzx           r1d, byte [r3]
    neg             r1b
    movd            xm0, r1d
    movzx           r1d, byte [r3 + 1]
    neg             r1b
    movd            xm1, r1d
    vinserti128     m0, m0, xm1, 1
    mova            m5, [pw_pixel_max]
    mov             r1d, r4m
    add             r1d, r1d
    shr             r2d, 4

.loop:
    movu            m7, [r0]
    movu            m8, [r0 + r1]
    movu            m2, [r0 + 2]
    movu            m1, [r0 + r1 + 2]

    pcmpgtw         m3, m7, m2
    pcmpgtw         m2, m7
    pcmpgtw         m4, m8, m1
    pcmpgtw         m1, m8

    packsswb        m3, m4
    packsswb        m2, m1
    vpermq          m3, m3, 11011000b
    vpermq          m2, m2, 11011000b

    pand            m3, [pb_1]
    por             m3, m2

    pslldq          m2, m3, 1
    por             m2, m0

    psignb          m2, [pb_128]                ; m2 = signLeft
    pxor            m0, m0
    palignr         m0, m3, 15
    paddb           m3, m2
    paddb           m3, [pb_2]                  ; m3 = uiEdgeType
    pshufb          m2, m6, m3
    pmovsxbw        m3, xm2                     ; offsetEo
    vextracti128    xm2, m2, 1
    pmovsxbw        m2, xm2
    pxor            m4, m4
    paddw           m7, m3
    paddw           m8, m2
    pmaxsw          m7, m4
    pmaxsw          m8, m4
    pminsw          m7, m5
    pminsw          m8, m5
    movu            [r0], m7
    movu            [r0 + r1], m8

    add             r0q, 32
    dec             r2d
    jnz             .loop
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE0, 5, 5, 7, rec, offsetEo, lcuWidth, signLeft, stride

    mov                 r4d,        r4m
    vbroadcasti128      m4,         [pb_128]                   ; m4 = [80]
    vbroadcasti128      m6,         [r1]                       ; m6 = offsetEo
    movzx               r1d,        byte [r3]
    neg                 r1b
    movd                xm0,        r1d
    movzx               r1d,        byte [r3 + 1]
    neg                 r1b
    movd                xm1,        r1d
    vinserti128         m0,         m0,        xm1,           1

.loop:
    movu                xm5,        [r0]                       ; xm5 = rec[x]
    movu                xm2,        [r0 + 1]                   ; xm2 = rec[x + 1]
    vinserti128         m5,         m5,        [r0 + r4],     1
    vinserti128         m2,         m2,        [r0 + r4 + 1], 1

    pxor                m1,         m5,        m4
    pxor                m3,         m2,        m4
    pcmpgtb             m2,         m1,        m3
    pcmpgtb             m3,         m1
    pand                m2,         [pb_1]
    por                 m2,         m3

    pslldq              m3,         m2,        1
    por                 m3,         m0

    psignb              m3,         m4                         ; m3 = signLeft
    pxor                m0,         m0
    palignr             m0,         m2,        15
    paddb               m2,         m3
    paddb               m2,         [pb_2]                     ; m2 = uiEdgeType
    pshufb              m3,         m6,        m2
    pmovzxbw            m2,         xm5                        ; rec
    vextracti128        xm5,        m5,        1
    pmovzxbw            m5,         xm5
    pmovsxbw            m1,         xm3                        ; offsetEo
    vextracti128        xm3,        m3,        1
    pmovsxbw            m3,         xm3
    paddw               m2,         m1
    paddw               m5,         m3
    packuswb            m2,         m5
    vpermq              m2,         m2,        11011000b
    movu                [r0],       xm2
    vextracti128        [r0 + r4],  m2,        1

    add                 r0q,        16
    sub                 r2d,        16
    jnz                 .loop
    RET
%endif

;==================================================================================================
; void saoCuOrgE1(pixel *pRec, int8_t *m_iUpBuff1, int8_t *m_iOffsetEo, Int iStride, Int iLcuWidth)
;==================================================================================================
INIT_XMM sse4
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE1, 4,5,8
    add         r3d, r3d
    mov         r4d, r4m
    pxor        m0, m0                      ; m0 = 0
    mova        m6, [pb_2]                  ; m6 = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    shr         r4d, 4
.loop
    movu        m7, [r0]
    movu        m5, [r0 + 16]
    movu        m3, [r0 + r3]
    movu        m1, [r0 + r3 + 16]

    pcmpgtw     m2, m7, m3
    pcmpgtw     m3, m7
    pcmpgtw     m4, m5, m1
    pcmpgtw     m1, m5 

    packsswb    m2, m4
    packsswb    m3, m1

    pand        m2, [pb_1]
    por         m2, m3

    movu        m3, [r1]                    ; m3 = m_iUpBuff1

    paddb       m3, m2
    paddb       m3, m6

    movu        m4, [r2]                    ; m4 = m_iOffsetEo
    pshufb      m1, m4, m3

    psubb       m3, m0, m2
    movu        [r1], m3

    pmovsxbw    m3, m1
    punpckhbw   m1, m1
    psraw       m1, 8

    paddw       m7, m3
    paddw       m5, m1

    pmaxsw      m7, m0
    pmaxsw      m5, m0
    pminsw      m7, [pw_pixel_max]
    pminsw      m5, [pw_pixel_max]

    movu        [r0], m7
    movu        [r0 + 16],  m5

    add         r0, 32
    add         r1, 16
    dec         r4d
    jnz         .loop
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE1, 3, 5, 8, pRec, m_iUpBuff1, m_iOffsetEo, iStride, iLcuWidth
    mov         r3d, r3m
    mov         r4d, r4m
    pxor        m0,    m0                      ; m0 = 0
    mova        m6,    [pb_2]                  ; m6 = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    mova        m7,    [pb_128]
    shr         r4d,   4
.loop
    movu        m1,    [r0]                    ; m1 = pRec[x]
    movu        m2,    [r0 + r3]               ; m2 = pRec[x + iStride]

    pxor        m3,    m1,    m7
    pxor        m4,    m2,    m7
    pcmpgtb     m2,    m3,    m4
    pcmpgtb     m4,    m3
    pand        m2,    [pb_1]
    por         m2,    m4

    movu        m3,    [r1]                    ; m3 = m_iUpBuff1

    paddb       m3,    m2
    paddb       m3,    m6

    movu        m4,    [r2]                    ; m4 = m_iOffsetEo
    pshufb      m5,    m4,    m3

    psubb       m3,    m0,    m2
    movu        [r1],  m3

    pmovzxbw    m2,    m1
    punpckhbw   m1,    m0
    pmovsxbw    m3,    m5
    punpckhbw   m5,    m5
    psraw       m5,    8

    paddw       m2,    m3
    paddw       m1,    m5
    packuswb    m2,    m1
    movu        [r0],  m2

    add         r0,    16
    add         r1,    16
    dec         r4d
    jnz         .loop
    RET
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE1, 4,5,6
    add         r3d, r3d
    mov         r4d, r4m
    mova        m4, [pb_2]
    shr         r4d, 4
    mova        m0, [pw_pixel_max]
.loop
    movu        m5, [r0]
    movu        m3, [r0 + r3]

    pcmpgtw     m2, m5, m3
    pcmpgtw     m3, m5

    packsswb    m2, m3
    vpermq      m3, m2, 11011101b
    vpermq      m2, m2, 10001000b

    pand        xm2, [pb_1]
    por         xm2, xm3

    movu        xm3, [r1]       ; m3 = m_iUpBuff1

    paddb       xm3, xm2
    paddb       xm3, xm4

    movu        xm1, [r2]       ; m1 = m_iOffsetEo
    pshufb      xm1, xm3
    pmovsxbw    m3, xm1

    paddw       m5, m3
    pxor        m3, m3
    pmaxsw      m5, m3
    pminsw      m5, m0
    movu        [r0], m5

    psubb       xm3, xm2
    movu        [r1], xm3

    add         r0, 32
    add         r1, 16
    dec         r4d
    jnz         .loop
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE1, 3, 5, 8, pRec, m_iUpBuff1, m_iOffsetEo, iStride, iLcuWidth
    mov           r3d,    r3m
    mov           r4d,    r4m
    movu          xm0,    [r2]                    ; xm0 = m_iOffsetEo
    mova          xm6,    [pb_2]                  ; xm6 = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    mova          xm7,    [pb_128]
    shr           r4d,    4
.loop
    movu          xm1,    [r0]                    ; xm1 = pRec[x]
    movu          xm2,    [r0 + r3]               ; xm2 = pRec[x + iStride]

    pxor          xm3,    xm1,    xm7
    pxor          xm4,    xm2,    xm7
    pcmpgtb       xm2,    xm3,    xm4
    pcmpgtb       xm4,    xm3
    pand          xm2,    [pb_1]
    por           xm2,    xm4

    movu          xm3,    [r1]                    ; xm3 = m_iUpBuff1

    paddb         xm3,    xm2
    paddb         xm3,    xm6

    pshufb        xm5,    xm0,    xm3
    pxor          xm4,    xm4
    psubb         xm3,    xm4,    xm2
    movu          [r1],   xm3

    pmovzxbw      m2,     xm1
    pmovsxbw      m3,     xm5

    paddw         m2,     m3
    vextracti128  xm3,    m2,     1
    packuswb      xm2,    xm3
    movu          [r0],   xm2

    add           r0,     16
    add           r1,     16
    dec           r4d
    jnz           .loop
    RET
%endif

;========================================================================================================
; void saoCuOrgE1_2Rows(pixel *pRec, int8_t *m_iUpBuff1, int8_t *m_iOffsetEo, Int iStride, Int iLcuWidth)
;========================================================================================================
INIT_XMM sse4
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE1_2Rows, 4,7,8
    add         r3d, r3d
    mov         r4d, r4m
    pxor        m0, m0                      ; m0 = 0
    mova        m6, [pw_pixel_max]
    mov         r5d, r4d
    shr         r4d, 4
    mov         r6, r0
.loop
    movu        m7, [r0]
    movu        m5, [r0 + 16]
    movu        m3, [r0 + r3]
    movu        m1, [r0 + r3 + 16]

    pcmpgtw     m2, m7, m3
    pcmpgtw     m3, m7
    pcmpgtw     m4, m5, m1
    pcmpgtw     m1, m5
    packsswb    m2, m4
    packsswb    m3, m1
    pand        m2, [pb_1]
    por         m2, m3

    movu        m3, [r1]                    ; m3 = m_iUpBuff1

    paddb       m3, m2
    paddb       m3, [pb_2]

    movu        m4, [r2]                    ; m4 = m_iOffsetEo
    pshufb      m1, m4, m3

    psubb       m3, m0, m2
    movu        [r1], m3

    pmovsxbw    m3, m1
    punpckhbw   m1, m1
    psraw       m1, 8

    paddw       m7, m3
    paddw       m5, m1

    pmaxsw      m7, m0
    pmaxsw      m5, m0
    pminsw      m7, m6
    pminsw      m5, m6

    movu        [r0], m7
    movu        [r0 + 16],  m5

    add         r0, 32
    add         r1, 16
    dec         r4d
    jnz         .loop

    sub         r1, r5
    shr         r5d, 4
    lea         r0, [r6 + r3]
.loopH:
    movu        m7, [r0]
    movu        m5, [r0 + 16]
    movu        m3, [r0 + r3]
    movu        m1, [r0 + r3 + 16]

    pcmpgtw     m2, m7, m3
    pcmpgtw     m3, m7
    pcmpgtw     m4, m5, m1
    pcmpgtw     m1, m5
    packsswb    m2, m4
    packsswb    m3, m1
    pand        m2, [pb_1]
    por         m2, m3

    movu        m3, [r1]                    ; m3 = m_iUpBuff1

    paddb       m3, m2
    paddb       m3, [pb_2]

    movu        m4, [r2]                    ; m4 = m_iOffsetEo
    pshufb      m1, m4, m3

    psubb       m3, m0, m2
    movu        [r1], m3

    pmovsxbw    m3, m1
    punpckhbw   m1, m1
    psraw       m1, 8

    paddw       m7, m3
    paddw       m5, m1

    pmaxsw      m7, m0
    pmaxsw      m5, m0
    pminsw      m7, m6
    pminsw      m5, m6

    movu        [r0], m7
    movu        [r0 + 16],  m5

    add         r0, 32
    add         r1, 16
    dec         r5d
    jnz         .loopH
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE1_2Rows, 3, 5, 8, pRec, m_iUpBuff1, m_iOffsetEo, iStride, iLcuWidth
    mov         r3d,        r3m
    mov         r4d,        r4m
    pxor        m0,         m0                      ; m0 = 0
    mova        m7,         [pb_128]
    shr         r4d,        4
.loop
    movu        m1,         [r0]                    ; m1 = pRec[x]
    movu        m2,         [r0 + r3]               ; m2 = pRec[x + iStride]

    pxor        m3,         m1,         m7
    pxor        m4,         m2,         m7
    pcmpgtb     m6,         m3,         m4
    pcmpgtb     m5,         m4,         m3
    pand        m6,         [pb_1]
    por         m6,         m5

    movu        m5,         [r0 + r3 * 2]
    pxor        m3,         m5,         m7
    pcmpgtb     m5,         m4,         m3
    pcmpgtb     m3,         m4
    pand        m5,         [pb_1]
    por         m5,         m3

    movu        m3,         [r1]                    ; m3 = m_iUpBuff1
    paddb       m3,         m6
    paddb       m3,         [pb_2]

    movu        m4,         [r2]                    ; m4 = m_iOffsetEo
    pshufb      m4,         m3

    psubb       m3,         m0,         m6
    movu        [r1],       m3

    pmovzxbw    m6,         m1
    punpckhbw   m1,         m0
    pmovsxbw    m3,         m4
    punpckhbw   m4,         m4
    psraw       m4,         8

    paddw       m6,         m3
    paddw       m1,         m4
    packuswb    m6,         m1
    movu        [r0],       m6

    movu        m3,         [r1]                    ; m3 = m_iUpBuff1
    paddb       m3,         m5
    paddb       m3,         [pb_2]

    movu        m4,         [r2]                    ; m4 = m_iOffsetEo
    pshufb      m4,         m3
    psubb       m3,         m0,         m5
    movu        [r1],       m3

    pmovzxbw    m5,         m2
    punpckhbw   m2,         m0
    pmovsxbw    m3,         m4
    punpckhbw   m4,         m4
    psraw       m4,         8

    paddw       m5,         m3
    paddw       m2,         m4
    packuswb    m5,         m2
    movu        [r0 + r3],  m5

    add         r0,         16
    add         r1,         16
    dec         r4d
    jnz         .loop
    RET
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE1_2Rows, 4,5,8
    add             r3d, r3d
    mov             r4d, r4m
    mova            m4, [pw_pixel_max]
    vbroadcasti128  m6, [r2]                ; m6 = m_iOffsetEo
    shr             r4d, 4
.loop
    movu            m7, [r0]
    movu            m5, [r0 + r3]
    movu            m1, [r0 + r3 * 2]

    pcmpgtw         m2, m7, m5
    pcmpgtw         m3, m5, m7
    pcmpgtw         m0, m5, m1
    pcmpgtw         m1, m5

    packsswb        m2, m0
    packsswb        m3, m1
    vpermq          m2, m2, 11011000b
    vpermq          m3, m3, 11011000b

    pand            m2, [pb_1]
    por             m2, m3

    movu            xm3, [r1]               ; m3 = m_iUpBuff1
    pxor            m0, m0
    psubb           m1, m0, m2
    vinserti128     m3, m3, xm1, 1
    vextracti128    [r1], m1, 1

    paddb           m3, m2
    paddb           m3, [pb_2]

    pshufb          m1, m6, m3
    pmovsxbw        m3, xm1
    vextracti128    xm1, m1, 1
    pmovsxbw        m1, xm1

    paddw           m7, m3
    paddw           m5, m1

    pmaxsw          m7, m0
    pmaxsw          m5, m0
    pminsw          m7, m4
    pminsw          m5, m4

    movu            [r0], m7
    movu            [r0 + r3],  m5

    add             r0, 32
    add             r1, 16
    dec             r4d
    jnz             .loop
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE1_2Rows, 3, 5, 7, pRec, m_iUpBuff1, m_iOffsetEo, iStride, iLcuWidth
    mov             r3d,        r3m
    mov             r4d,        r4m
    pxor            m0,         m0                           ; m0 = 0
    vbroadcasti128  m5,         [pb_128]
    vbroadcasti128  m6,         [r2]                         ; m6 = m_iOffsetEo
    shr             r4d,        4
.loop
    movu            xm1,        [r0]                         ; m1 = pRec[x]
    movu            xm2,        [r0 + r3]                    ; m2 = pRec[x + iStride]
    vinserti128     m1,         m1,       xm2,            1
    vinserti128     m2,         m2,       [r0 + r3 * 2],  1

    pxor            m3,         m1,       m5
    pxor            m4,         m2,       m5
    pcmpgtb         m2,         m3,       m4
    pcmpgtb         m4,         m3
    pand            m2,         [pb_1]
    por             m2,         m4

    movu            xm3,        [r1]                         ; xm3 = m_iUpBuff
    psubb           m4,         m0,       m2
    vinserti128     m3,         m3,       xm4,            1
    paddb           m3,         m2
    paddb           m3,         [pb_2]
    pshufb          m2,         m6,       m3
    vextracti128    [r1],       m4,       1

    pmovzxbw        m4,         xm1
    vextracti128    xm3,        m1,       1
    pmovzxbw        m3,         xm3
    pmovsxbw        m1,         xm2
    vextracti128    xm2,        m2,       1
    pmovsxbw        m2,         xm2

    paddw           m4,         m1
    paddw           m3,         m2
    packuswb        m4,         m3
    vpermq          m4,         m4,       11011000b
    movu            [r0],       xm4
    vextracti128    [r0 + r3],  m4,       1

    add             r0,         16
    add             r1,         16
    dec             r4d
    jnz             .loop
    RET
%endif

;======================================================================================================================================================
; void saoCuOrgE2(pixel * rec, int8_t * bufft, int8_t * buff1, int8_t * offsetEo, int lcuWidth, intptr_t stride)
;======================================================================================================================================================
INIT_XMM sse4
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE2, 6,6,8
    mov         r4d, r4m
    add         r5d, r5d
    pxor        m0, m0
    inc         r1
    movh        m6, [r0 + r4 * 2]
    movhps      m6, [r1 + r4]

.loop
    movu        m7, [r0]
    movu        m5, [r0 + 16]
    movu        m3, [r0 + r5 + 2]
    movu        m1, [r0 + r5 + 18]

    pcmpgtw     m2, m7, m3
    pcmpgtw     m3, m7
    pcmpgtw     m4, m5, m1
    pcmpgtw     m1, m5
    packsswb    m2, m4
    packsswb    m3, m1
    pand        m2, [pb_1]
    por         m2, m3

    movu        m3, [r2]

    paddb       m3, m2
    paddb       m3, [pb_2]

    movu        m4, [r3]
    pshufb      m4, m3

    psubb       m3, m0, m2
    movu        [r1], m3

    pmovsxbw    m3, m4
    punpckhbw   m4, m4
    psraw       m4, 8

    paddw       m7, m3
    paddw       m5, m4
    pmaxsw      m7, m0
    pmaxsw      m5, m0
    pminsw      m7, [pw_pixel_max]
    pminsw      m5, [pw_pixel_max]
    movu        [r0], m7
    movu        [r0 + 16], m5

    add         r0, 32
    add         r1, 16
    add         r2, 16
    sub         r4, 16
    jg          .loop

    movh        [r0 + r4 * 2], m6
    movhps      [r1 + r4], m6
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE2, 5, 6, 8, rec, bufft, buff1, offsetEo, lcuWidth
    mov         r4d,   r4m
    mov         r5d,   r5m
    pxor        m0,    m0                      ; m0 = 0
    mova        m6,    [pb_2]                  ; m6 = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    mova        m7,    [pb_128]
    inc         r1
    movh        m5,    [r0 + r4]
    movhps      m5,    [r1 + r4]

.loop
    movu        m1,    [r0]                    ; m1 = rec[x]
    movu        m2,    [r0 + r5 + 1]           ; m2 = rec[x + stride + 1]
    pxor        m3,    m1,    m7
    pxor        m4,    m2,    m7
    pcmpgtb     m2,    m3,    m4
    pcmpgtb     m4,    m3
    pand        m2,    [pb_1]
    por         m2,    m4
    movu        m3,    [r2]                    ; m3 = buff1

    paddb       m3,    m2
    paddb       m3,    m6                      ; m3 = edgeType

    movu        m4,    [r3]                    ; m4 = offsetEo
    pshufb      m4,    m3

    psubb       m3,    m0,    m2
    movu        [r1],  m3

    pmovzxbw    m2,    m1
    punpckhbw   m1,    m0
    pmovsxbw    m3,    m4
    punpckhbw   m4,    m4
    psraw       m4,    8

    paddw       m2,    m3
    paddw       m1,    m4
    packuswb    m2,    m1
    movu        [r0],  m2

    add         r0,    16
    add         r1,    16
    add         r2,    16
    sub         r4,    16
    jg          .loop

    movh        [r0 + r4], m5
    movhps      [r1 + r4], m5
    RET
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE2, 6,6,7
    mov             r4d, r4m
    add             r5d, r5d
    inc             r1
    movq            xm4, [r0 + r4 * 2]
    movhps          xm4, [r1 + r4]
    vbroadcasti128  m5, [r3]
    mova            m6, [pw_pixel_max]
.loop
    movu            m1, [r0]
    movu            m3, [r0 + r5 + 2]

    pcmpgtw         m2, m1, m3
    pcmpgtw         m3, m1

    packsswb        m2, m3
    vpermq          m3, m2, 11011101b
    vpermq          m2, m2, 10001000b

    pand            xm2, [pb_1]
    por             xm2, xm3

    movu            xm3, [r2]

    paddb           xm3, xm2
    paddb           xm3, [pb_2]
    pshufb          xm0, xm5, xm3
    pmovsxbw        m3, xm0

    pxor            m0, m0
    paddw           m1, m3
    pmaxsw          m1, m0
    pminsw          m1, m6
    movu            [r0], m1

    psubb           xm0, xm2
    movu            [r1], xm0

    add             r0, 32
    add             r1, 16
    add             r2, 16
    sub             r4, 16
    jg              .loop

    movq            [r0 + r4 * 2], xm4
    movhps          [r1 + r4], xm4
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE2, 5, 6, 7, rec, bufft, buff1, offsetEo, lcuWidth
    mov            r4d,   r4m
    mov            r5d,   r5m
    pxor           xm0,   xm0                     ; xm0 = 0
    mova           xm5,   [pb_128]
    inc            r1
    movq           xm6,   [r0 + r4]
    movhps         xm6,   [r1 + r4]

    movu           xm1,   [r0]                    ; xm1 = rec[x]
    movu           xm2,   [r0 + r5 + 1]           ; xm2 = rec[x + stride + 1]
    pxor           xm3,   xm1,   xm5
    pxor           xm4,   xm2,   xm5
    pcmpgtb        xm2,   xm3,   xm4
    pcmpgtb        xm4,   xm3
    pand           xm2,   [pb_1]
    por            xm2,   xm4
    movu           xm3,   [r2]                    ; xm3 = buff1

    paddb          xm3,   xm2
    paddb          xm3,   [pb_2]                  ; xm3 = edgeType

    movu           xm4,   [r3]                    ; xm4 = offsetEo
    pshufb         xm4,   xm3

    psubb          xm3,   xm0,   xm2
    movu           [r1],  xm3

    pmovzxbw       m2,    xm1
    pmovsxbw       m3,    xm4

    paddw          m2,    m3
    vextracti128   xm3,   m2,    1
    packuswb       xm2,   xm3
    movu           [r0],  xm2

    movq           [r0 + r4], xm6
    movhps         [r1 + r4], xm6
    RET
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE2_32, 6,6,8
    mov             r4d, r4m
    add             r5d, r5d
    inc             r1
    movq            xm4, [r0 + r4 * 2]
    movhps          xm4, [r1 + r4]
    vbroadcasti128  m5, [r3]

.loop
    movu            m1, [r0]
    movu            m7, [r0 + 32]
    movu            m3, [r0 + r5 + 2]
    movu            m6, [r0 + r5 + 34]

    pcmpgtw         m2, m1, m3
    pcmpgtw         m0, m7, m6
    pcmpgtw         m3, m1
    pcmpgtw         m6, m7

    packsswb        m2, m0
    packsswb        m3, m6
    vpermq          m3, m3, 11011000b
    vpermq          m2, m2, 11011000b

    pand            m2, [pb_1]
    por             m2, m3

    movu            m3, [r2]

    paddb           m3, m2
    paddb           m3, [pb_2]
    pshufb          m0, m5, m3

    pmovsxbw        m3, xm0
    vextracti128    xm0, m0, 1
    pmovsxbw        m6, xm0

    pxor            m0, m0
    paddw           m1, m3
    paddw           m7, m6
    pmaxsw          m1, m0
    pmaxsw          m7, m0
    pminsw          m1, [pw_pixel_max]
    pminsw          m7, [pw_pixel_max]
    movu            [r0], m1
    movu            [r0 + 32], m7

    psubb           m0, m2
    movu            [r1], m0

    add             r0, 64
    add             r1, 32
    add             r2, 32
    sub             r4, 32
    jg              .loop

    movq            [r0 + r4 * 2], xm4
    movhps          [r1 + r4], xm4
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE2_32, 5, 6, 8, rec, bufft, buff1, offsetEo, lcuWidth
    mov             r4d,   r4m
    mov             r5d,   r5m
    pxor            m0,    m0                      ; m0 = 0
    vbroadcasti128  m7,    [pb_128]
    vbroadcasti128  m5,    [r3]                    ; m5 = offsetEo
    inc             r1
    movq            xm6,   [r0 + r4]
    movhps          xm6,   [r1 + r4]

.loop:
    movu            m1,    [r0]                    ; m1 = rec[x]
    movu            m2,    [r0 + r5 + 1]           ; m2 = rec[x + stride + 1]
    pxor            m3,    m1,    m7
    pxor            m4,    m2,    m7
    pcmpgtb         m2,    m3,    m4
    pcmpgtb         m4,    m3
    pand            m2,    [pb_1]
    por             m2,    m4
    movu            m3,    [r2]                    ; m3 = buff1

    paddb           m3,    m2
    paddb           m3,    [pb_2]                  ; m3 = edgeType

    pshufb          m4,    m5,    m3

    psubb           m3,    m0,    m2
    movu            [r1],  m3

    pmovzxbw        m2,    xm1
    vextracti128    xm1,   m1,    1
    pmovzxbw        m1,    xm1
    pmovsxbw        m3,    xm4
    vextracti128    xm4,   m4,    1
    pmovsxbw        m4,    xm4

    paddw           m2,    m3
    paddw           m1,    m4
    packuswb        m2,    m1
    vpermq          m2,    m2,    11011000b
    movu            [r0],  m2

    add             r0,    32
    add             r1,    32
    add             r2,    32
    sub             r4,    32
    jg              .loop

    movq            [r0 + r4], xm6
    movhps          [r1 + r4], xm6
    RET
%endif

;=======================================================================================================
;void saoCuOrgE3(pixel *rec, int8_t *upBuff1, int8_t *m_offsetEo, intptr_t stride, int startX, int endX)
;=======================================================================================================
INIT_XMM sse4
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE3, 4,6,8
    add             r3d, r3d
    mov             r4d, r4m
    mov             r5d, r5m

    ; save latest 2 pixels for case startX=1 or left_endX=15
    movh            m6, [r0 + r5 * 2]
    movhps          m6, [r1 + r5 - 1]

    ; move to startX+1
    inc             r4d
    lea             r0, [r0 + r4 * 2]           ; x = startX + 1
    add             r1, r4
    sub             r5d, r4d
    pxor            m0, m0

.loop:
    movu            m7, [r0]
    movu            m5, [r0 + 16]
    movu            m3, [r0 + r3]
    movu            m1, [r0 + r3 + 16]

    pcmpgtw         m2, m7, m3
    pcmpgtw         m3, m7
    pcmpgtw         m4, m5, m1
    pcmpgtw         m1, m5
    packsswb        m2, m4
    packsswb        m3, m1
    pand            m2, [pb_1]
    por             m2, m3

    movu            m3, [r1]                    ; m3 = m_iUpBuff1

    paddb           m3, m2
    paddb           m3, [pb_2]                  ; m3 = uiEdgeType

    movu            m4, [r2]                    ; m4 = m_iOffsetEo
    pshufb          m4, m3

    psubb           m3, m0, m2
    movu            [r1 - 1], m3

    pmovsxbw        m3, m4
    punpckhbw       m4, m4
    psraw           m4, 8

    paddw           m7, m3
    paddw           m5, m4
    pmaxsw          m7, m0
    pmaxsw          m5, m0
    pminsw          m7, [pw_pixel_max]
    pminsw          m5, [pw_pixel_max]
    movu            [r0], m7
    movu            [r0 + 16], m5

    add             r0, 32
    add             r1, 16

    sub             r5, 16
    jg             .loop

    ; restore last pixels (up to 2)
    movh            [r0 + r5 * 2], m6
    movhps          [r1 + r5 - 1], m6
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE3, 3,6,8
    mov             r3d, r3m
    mov             r4d, r4m
    mov             r5d, r5m

    ; save latest 2 pixels for case startX=1 or left_endX=15
    movh            m7, [r0 + r5]
    movhps          m7, [r1 + r5 - 1]

    ; move to startX+1
    inc             r4d
    add             r0, r4
    add             r1, r4
    sub             r5d, r4d
    pxor            m0, m0                      ; m0 = 0
    movu            m6, [pb_2]                  ; m6 = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]

.loop:
    movu            m1, [r0]                    ; m1 = pRec[x]
    movu            m2, [r0 + r3]               ; m2 = pRec[x + iStride]

    psubusb         m3, m2, m1
    psubusb         m4, m1, m2
    pcmpeqb         m3, m0
    pcmpeqb         m4, m0
    pcmpeqb         m2, m1

    pabsb           m3, m3
    por             m4, m3
    pandn           m2, m4                      ; m2 = iSignDown

    movu            m3, [r1]                    ; m3 = m_iUpBuff1

    paddb           m3, m2
    paddb           m3, m6                      ; m3 = uiEdgeType

    movu            m4, [r2]                    ; m4 = m_iOffsetEo
    pshufb          m5, m4, m3

    psubb           m3, m0, m2
    movu            [r1 - 1], m3

    pmovzxbw        m2, m1
    punpckhbw       m1, m0
    pmovsxbw        m3, m5
    punpckhbw       m5, m5
    psraw           m5, 8

    paddw           m2, m3
    paddw           m1, m5
    packuswb        m2, m1
    movu            [r0], m2

    add             r0, 16
    add             r1, 16

    sub             r5, 16
    jg             .loop

    ; restore last pixels (up to 2)
    movh            [r0 + r5], m7
    movhps          [r1 + r5 - 1], m7
    RET
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE3, 4,6,6
    add             r3d, r3d
    mov             r4d, r4m
    mov             r5d, r5m

    ; save latest 2 pixels for case startX=1 or left_endX=15
    movq            xm5, [r0 + r5 * 2]
    movhps          xm5, [r1 + r5 - 1]

    ; move to startX+1
    inc             r4d
    lea             r0, [r0 + r4 * 2]           ; x = startX + 1
    add             r1, r4
    sub             r5d, r4d
    movu            xm4, [r2]

.loop:
    movu            m1, [r0]
    movu            m0, [r0 + r3]

    pcmpgtw         m2, m1, m0
    pcmpgtw         m0, m1
    packsswb        m2, m0
    vpermq          m0, m2, 11011101b
    vpermq          m2, m2, 10001000b
    pand            m2, [pb_1]
    por             m2, m0

    movu            xm0, [r1]
    paddb           xm0, xm2
    paddb           xm0, [pb_2]

    pshufb          xm3, xm4, xm0
    pmovsxbw        m3, xm3

    paddw           m1, m3
    pxor            m0, m0
    pmaxsw          m1, m0
    pminsw          m1, [pw_pixel_max]
    movu            [r0], m1

    psubb           xm0, xm2
    movu            [r1 - 1], xm0

    add             r0, 32
    add             r1, 16
    sub             r5, 16
    jg             .loop

    ; restore last pixels (up to 2)
    movq            [r0 + r5 * 2], xm5
    movhps          [r1 + r5 - 1], xm5
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE3, 3, 6, 8
    mov             r3d,  r3m
    mov             r4d,  r4m
    mov             r5d,  r5m

    ; save latest 2 pixels for case startX=1 or left_endX=15
    movq            xm7,  [r0 + r5]
    movhps          xm7,  [r1 + r5 - 1]

    ; move to startX+1
    inc             r4d
    add             r0,   r4
    add             r1,   r4
    sub             r5d,  r4d
    pxor            xm0,  xm0                     ; xm0 = 0
    mova            xm6,  [pb_2]                  ; xm6 = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    movu            xm5,  [r2]                    ; xm5 = m_iOffsetEo

.loop:
    movu            xm1,  [r0]                    ; xm1 = pRec[x]
    movu            xm2,  [r0 + r3]               ; xm2 = pRec[x + iStride]

    psubusb         xm3,  xm2,  xm1
    psubusb         xm4,  xm1,  xm2
    pcmpeqb         xm3,  xm0
    pcmpeqb         xm4,  xm0
    pcmpeqb         xm2,  xm1

    pabsb           xm3,  xm3
    por             xm4,  xm3
    pandn           xm2,  xm4                     ; xm2 = iSignDown

    movu            xm3,  [r1]                    ; xm3 = m_iUpBuff1

    paddb           xm3,  xm2
    paddb           xm3,  xm6                     ; xm3 = uiEdgeType

    pshufb          xm4,  xm5,  xm3

    psubb           xm3,  xm0,  xm2
    movu            [r1 - 1],   xm3

    pmovzxbw        m2,   xm1
    pmovsxbw        m3,   xm4

    paddw           m2,   m3
    vextracti128    xm3,  m2,   1
    packuswb        xm2,  xm3
    movu            [r0], xm2

    add             r0,   16
    add             r1,   16

    sub             r5,   16
    jg             .loop

    ; restore last pixels (up to 2)
    movq            [r0 + r5],     xm7
    movhps          [r1 + r5 - 1], xm7
    RET
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal saoCuOrgE3_32, 3,6,8
    add             r3d, r3d
    mov             r4d, r4m
    mov             r5d, r5m

    ; save latest 2 pixels for case startX=1 or left_endX=15
    movq            xm5, [r0 + r5 * 2]
    movhps          xm5, [r1 + r5 - 1]

    ; move to startX+1
    inc             r4d
    lea             r0, [r0 + r4 * 2]           ; x = startX + 1
    add             r1, r4
    sub             r5d, r4d
    vbroadcasti128  m4, [r2]

.loop:
    movu            m1, [r0]
    movu            m7, [r0 + 32]
    movu            m0, [r0 + r3]
    movu            m6, [r0 + r3 + 32]

    pcmpgtw         m2, m1, m0
    pcmpgtw         m3, m7, m6
    pcmpgtw         m0, m1
    pcmpgtw         m6, m7

    packsswb        m2, m3
    packsswb        m0, m6
    vpermq          m2, m2, 11011000b
    vpermq          m0, m0, 11011000b
    pand            m2, [pb_1]
    por             m2, m0

    movu            m0, [r1]
    paddb           m0, m2
    paddb           m0, [pb_2]

    pshufb          m3, m4, m0
    vextracti128    xm6, m3, 1
    pmovsxbw        m3, xm3
    pmovsxbw        m6, xm6

    paddw           m1, m3
    paddw           m7, m6
    pxor            m0, m0
    pmaxsw          m1, m0
    pmaxsw          m7, m0
    pminsw          m1, [pw_pixel_max]
    pminsw          m7, [pw_pixel_max]
    movu            [r0], m1
    movu            [r0 + 32], m7

    psubb           m0, m2
    movu            [r1 - 1], m0

    add             r0, 64
    add             r1, 32
    sub             r5, 32
    jg             .loop

    ; restore last pixels (up to 2)
    movq            [r0 + r5 * 2], xm5
    movhps          [r1 + r5 - 1], xm5
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgE3_32, 3, 6, 8
    mov             r3d,  r3m
    mov             r4d,  r4m
    mov             r5d,  r5m

    ; save latest 2 pixels for case startX=1 or left_endX=15
    movq            xm7,  [r0 + r5]
    movhps          xm7,  [r1 + r5 - 1]

    ; move to startX+1
    inc             r4d
    add             r0,   r4
    add             r1,   r4
    sub             r5d,  r4d
    pxor            m0,   m0                      ; m0 = 0
    mova            m6,   [pb_2]                  ; m6 = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    vbroadcasti128  m5,   [r2]                    ; m5 = m_iOffsetEo

.loop:
    movu            m1,   [r0]                    ; m1 = pRec[x]
    movu            m2,   [r0 + r3]               ; m2 = pRec[x + iStride]

    psubusb         m3,   m2,   m1
    psubusb         m4,   m1,   m2
    pcmpeqb         m3,   m0
    pcmpeqb         m4,   m0
    pcmpeqb         m2,   m1

    pabsb           m3,   m3
    por             m4,   m3
    pandn           m2,   m4                      ; m2 = iSignDown

    movu            m3,   [r1]                    ; m3 = m_iUpBuff1

    paddb           m3,   m2
    paddb           m3,   m6                      ; m3 = uiEdgeType

    pshufb          m4,   m5,   m3

    psubb           m3,   m0,   m2
    movu            [r1 - 1],   m3

    pmovzxbw        m2,   xm1
    vextracti128    xm1,  m1,   1
    pmovzxbw        m1,   xm1
    pmovsxbw        m3,   xm4
    vextracti128    xm4,  m4,   1
    pmovsxbw        m4,   xm4

    paddw           m2,   m3
    paddw           m1,   m4
    packuswb        m2,   m1
    vpermq          m2,   m2,   11011000b
    movu            [r0], m2

    add             r0,   32
    add             r1,   32
    sub             r5,   32
    jg             .loop

    ; restore last pixels (up to 2)
    movq            [r0 + r5],     xm7
    movhps          [r1 + r5 - 1], xm7
    RET
%endif

;=====================================================================================
; void saoCuOrgB0(pixel* rec, const pixel* offset, int lcuWidth, int lcuHeight, int stride)
;=====================================================================================
INIT_XMM sse4
%if HIGH_BIT_DEPTH
cglobal saoCuOrgB0, 5,7,8
    add         r4d, r4d

    shr         r2d, 4
    movu        m3, [r1]            ; offset[0-15]
    movu        m4, [r1 + 16]       ; offset[16-31]
    pxor        m7, m7

.loopH
    mov         r5d, r2d
    xor         r6,  r6

.loopW
    movu        m2, [r0 + r6]
    movu        m5, [r0 + r6 + 16]
    psrlw       m0, m2, (BIT_DEPTH - 5)
    psrlw       m6, m5, (BIT_DEPTH - 5)
    packuswb    m0, m6
    pand        m0, [pb_31]         ; m0 = [index]

    pshufb      m6, m3, m0
    pshufb      m1, m4, m0
    pcmpgtb     m0, [pb_15]         ; m0 = [mask]

    pblendvb    m6, m1, m0

    pmovsxbw    m0, m6              ; offset
    punpckhbw   m6, m6
    psraw       m6, 8

    paddw       m2, m0
    paddw       m5, m6
    pmaxsw      m2, m7
    pmaxsw      m5, m7
    pminsw      m2, [pw_pixel_max]
    pminsw      m5, [pw_pixel_max]

    movu        [r0 + r6], m2
    movu        [r0 + r6 + 16], m5
    add         r6d, 32
    dec         r5d
    jnz         .loopW

    lea         r0, [r0 + r4]

    dec         r3d
    jnz         .loopH
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgB0, 4, 7, 8

    mov         r3d, r3m
    mov         r4d, r4m

    shr         r2d, 4
    movu        m3, [r1 + 0]      ; offset[0-15]
    movu        m4, [r1 + 16]     ; offset[16-31]
    pxor        m7, m7            ; m7 =[0]
.loopH
    mov         r5d, r2d
    xor         r6,  r6

.loopW
    movu        m2, [r0 + r6]     ; m0 = [rec]
    psrlw       m1, m2, 3
    pand        m1, [pb_31]       ; m1 = [index]
    pcmpgtb     m0, m1, [pb_15]   ; m2 = [mask]

    pshufb      m6, m3, m1
    pshufb      m5, m4, m1

    pblendvb    m6, m5, m0

    pmovzxbw    m1, m2            ; rec
    punpckhbw   m2, m7

    pmovsxbw    m0, m6            ; offset
    punpckhbw   m6, m6
    psraw       m6, 8

    paddw       m1, m0
    paddw       m2, m6
    packuswb    m1, m2

    movu        [r0 + r6], m1
    add         r6d, 16
    dec         r5d
    jnz         .loopW

    lea         r0, [r0 + r4]

    dec         r3d
    jnz         .loopH
    RET
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal saoCuOrgB0, 5,7,8
    vbroadcasti128  m3, [r1]
    vbroadcasti128  m4, [r1 + 16]
    add             r4d, r4d
    lea             r1, [r4 * 2]
    sub             r1d, r2d
    sub             r1d, r2d
    shr             r2d, 4
    mova            m7, [pw_pixel_max]

    mov             r6d, r3d
    shr             r3d, 1

.loopH
    mov             r5d, r2d
.loopW
    movu            m2, [r0]
    movu            m5, [r0 + r4]
    psrlw           m0, m2, (BIT_DEPTH - 5)
    psrlw           m6, m5, (BIT_DEPTH - 5)
    packuswb        m0, m6
    vpermq          m0, m0, 11011000b
    pand            m0, [pb_31]         ; m0 = [index]

    pshufb          m6, m3, m0
    pshufb          m1, m4, m0
    pcmpgtb         m0, [pb_15]         ; m0 = [mask]

    pblendvb        m6, m6, m1, m0      ; NOTE: don't use 3 parameters style, x264 macro have some bug!

    pmovsxbw        m0, xm6
    vextracti128    xm6, m6, 1
    pmovsxbw        m6, xm6

    paddw           m2, m0
    paddw           m5, m6
    pxor            m1, m1
    pmaxsw          m2, m1
    pmaxsw          m5, m1
    pminsw          m2, m7
    pminsw          m5, m7

    movu            [r0], m2
    movu            [r0 + r4], m5

    add             r0, 32
    dec             r5d
    jnz             .loopW

    add             r0, r1
    dec             r3d
    jnz             .loopH

    test            r6b, 1
    jz              .end
    xor             r1, r1
.loopW1:
    movu            m2, [r0 + r1]
    psrlw           m0, m2, (BIT_DEPTH - 5)
    packuswb        m0, m0
    vpermq          m0, m0, 10001000b
    pand            m0, [pb_31]         ; m0 = [index]

    pshufb          m6, m3, m0
    pshufb          m1, m4, m0
    pcmpgtb         m0, [pb_15]         ; m0 = [mask]

    pblendvb        m6, m6, m1, m0      ; NOTE: don't use 3 parameters style, x264 macro have some bug!
    pmovsxbw        m0, xm6             ; offset

    paddw           m2, m0
    pxor            m0, m0
    pmaxsw          m2, m0
    pminsw          m2, m7

    movu            [r0 + r1], m2
    add             r1d, 32
    dec             r2d
    jnz             .loopW1
.end:
    RET
%else ; HIGH_BIT_DEPTH
cglobal saoCuOrgB0, 4, 7, 8

    mov             r3d,        r3m
    mov             r4d,        r4m
    mova            m7,         [pb_31]
    vbroadcasti128  m3,         [r1 + 0]            ; offset[0-15]
    vbroadcasti128  m4,         [r1 + 16]           ; offset[16-31]
    lea             r6,         [r4 * 2]
    sub             r6d,        r2d
    shr             r2d,        4
    mov             r1d,        r3d
    shr             r3d,        1
.loopH
    mov             r5d,        r2d
.loopW
    movu            xm2,        [r0]                ; m2 = [rec]
    vinserti128     m2,         m2,  [r0 + r4],  1
    psrlw           m1,         m2,  3
    pand            m1,         m7                  ; m1 = [index]
    pcmpgtb         m0,         m1,  [pb_15]        ; m0 = [mask]

    pshufb          m6,         m3,  m1
    pshufb          m5,         m4,  m1

    pblendvb        m6,         m6,  m5,  m0        ; NOTE: don't use 3 parameters style, x264 macro have some bug!

    pmovzxbw        m1,         xm2                 ; rec
    vextracti128    xm2,        m2,  1
    pmovzxbw        m2,         xm2
    pmovsxbw        m0,         xm6                 ; offset
    vextracti128    xm6,        m6,  1
    pmovsxbw        m6,         xm6

    paddw           m1,         m0
    paddw           m2,         m6
    packuswb        m1,         m2
    vpermq          m1,         m1,  11011000b

    movu            [r0],       xm1
    vextracti128    [r0 + r4],  m1,  1
    add             r0,         16
    dec             r5d
    jnz             .loopW

    add             r0,         r6
    dec             r3d
    jnz             .loopH
    test            r1b,        1
    jz              .end
    mov             r5d,        r2d
.loopW1
    movu            xm2,        [r0]                ; m2 = [rec]
    psrlw           xm1,        xm2, 3
    pand            xm1,        xm7                 ; m1 = [index]
    pcmpgtb         xm0,        xm1, [pb_15]        ; m0 = [mask]

    pshufb          xm6,        xm3, xm1
    pshufb          xm5,        xm4, xm1

    pblendvb        xm6,        xm6, xm5, xm0       ; NOTE: don't use 3 parameters style, x264 macro have some bug!

    pmovzxbw        m1,         xm2                 ; rec
    pmovsxbw        m0,         xm6                 ; offset

    paddw           m1,         m0
    vextracti128    xm0,        m1,  1
    packuswb        xm1,        xm0

    movu            [r0],       xm1
    add             r0,         16
    dec             r5d
    jnz             .loopW1
.end
    RET
%endif

;============================================================================================================
; void calSign(int8_t *dst, const Pixel *src1, const Pixel *src2, const int width)
;============================================================================================================
INIT_XMM sse4
%if HIGH_BIT_DEPTH
cglobal calSign, 4, 7, 5
    mova            m0, [pw_1]
    mov             r4d, r3d
    shr             r3d, 4
    add             r3d, 1
    mov             r5, r0
    movu            m4, [r0 + r4]
.loop
    movu            m1, [r1]        ; m2 = pRec[x]
    movu            m2, [r2]        ; m3 = pTmpU[x]

    pcmpgtw         m3, m1, m2
    pcmpgtw         m2, m1
    pand            m3, m0
    por             m3, m2
    packsswb        m3, m3
    movh            [r0], xm3

    movu            m1, [r1 + 16]   ; m2 = pRec[x]
    movu            m2, [r2 + 16]   ; m3 = pTmpU[x]

    pcmpgtw         m3, m1, m2
    pcmpgtw         m2, m1
    pand            m3, m0
    por             m3, m2
    packsswb        m3, m3
    movh            [r0 + 8], xm3

    add             r0, 16
    add             r1, 32
    add             r2, 32
    dec             r3d
    jnz             .loop

    mov             r6, r0
    sub             r6, r5
    sub             r4, r6
    movu            [r0 + r4], m4
    RET
%else ; HIGH_BIT_DEPTH

cglobal calSign, 4,5,6
    mova        m0,     [pb_128]
    mova        m1,     [pb_1]

    sub         r1,     r0
    sub         r2,     r0

    mov         r4d,    r3d
    shr         r3d,    4
    jz         .next
.loop:
    movu        m2,     [r0 + r1]            ; m2 = pRec[x]
    movu        m3,     [r0 + r2]            ; m3 = pTmpU[x]
    pxor        m4,     m2,     m0
    pxor        m3,     m0
    pcmpgtb     m5,     m4,     m3
    pcmpgtb     m3,     m4
    pand        m5,     m1
    por         m5,     m3
    movu        [r0],   m5

    add         r0,     16
    dec         r3d
    jnz        .loop

    ; process partial
.next:
    and         r4d, 15
    jz         .end

    movu        m2,     [r0 + r1]            ; m2 = pRec[x]
    movu        m3,     [r0 + r2]            ; m3 = pTmpU[x]
    pxor        m4,     m2,     m0
    pxor        m3,     m0
    pcmpgtb     m5,     m4,     m3
    pcmpgtb     m3,     m4
    pand        m5,     m1
    por         m5,     m3

    lea         r3,     [pb_movemask + 16]
    sub         r3,     r4
    movu        xmm0,   [r3]
    movu        m3,     [r0]
    pblendvb    m5,     m3,     xmm0
    movu        [r0],   m5

.end:
    RET
%endif

INIT_YMM avx2
%if HIGH_BIT_DEPTH
cglobal calSign, 4, 7, 5
    mova            m0, [pw_1]
    mov             r4d, r3d
    shr             r3d, 4
    add             r3d, 1
    mov             r5, r0
    movu            m4, [r0 + r4]

.loop
    movu            m1, [r1]        ; m2 = pRec[x]
    movu            m2, [r2]        ; m3 = pTmpU[x]

    pcmpgtw         m3, m1, m2
    pcmpgtw         m2, m1

    pand            m3, m0
    por             m3, m2
    packsswb        m3, m3
    vpermq          m3, m3, q3220
    movu            [r0 ], xm3

    add             r0, 16
    add             r1, 32
    add             r2, 32
    dec             r3d
    jnz             .loop

    mov             r6, r0
    sub             r6, r5
    sub             r4, r6
    movu            [r0 + r4], m4
    RET
%else ; HIGH_BIT_DEPTH

cglobal calSign, 4, 5, 6
    vbroadcasti128  m0,     [pb_128]
    mova            m1,     [pb_1]

    sub             r1,     r0
    sub             r2,     r0

    mov             r4d,    r3d
    shr             r3d,    5
    jz              .next
.loop:
    movu            m2,     [r0 + r1]            ; m2 = pRec[x]
    movu            m3,     [r0 + r2]            ; m3 = pTmpU[x]
    pxor            m4,     m2,     m0
    pxor            m3,     m0
    pcmpgtb         m5,     m4,     m3
    pcmpgtb         m3,     m4
    pand            m5,     m1
    por             m5,     m3
    movu            [r0],   m5

    add             r0,     mmsize
    dec             r3d
    jnz             .loop

    ; process partial
.next:
    and             r4d,    31
    jz              .end

    movu            m2,     [r0 + r1]            ; m2 = pRec[x]
    movu            m3,     [r0 + r2]            ; m3 = pTmpU[x]
    pxor            m4,     m2,     m0
    pxor            m3,     m0
    pcmpgtb         m5,     m4,     m3
    pcmpgtb         m3,     m4
    pand            m5,     m1
    por             m5,     m3

    lea             r3,     [pb_movemask_32 + 32]
    sub             r3,     r4
    movu            m0,     [r3]
    movu            m3,     [r0]
    pblendvb        m5,     m5,     m3,     m0
    movu            [r0],   m5

.end:
    RET
%endif

;--------------------------------------------------------------------------------------------------------------------------
; saoCuStatsBO_c(const int16_t *diff, const pixel *rec, intptr_t stride, int endX, int endY, int32_t *stats, int32_t *count)
;--------------------------------------------------------------------------------------------------------------------------
%if ARCH_X86_64
INIT_XMM sse4
cglobal saoCuStatsBO, 7,13,2
    mova        m0, [pb_124]
    add         r5, 4
    add         r6, 4

.loopH:
    mov         r12, r0
    mov         r11, r1
    mov         r9d, r3d

.loopL:
    movu        m1, [r11]
    psrlw       m1, 1                   ; rec[x] >> boShift
    pand        m1, m0

    cmp         r9d, 8
    jle        .proc8

    movq        r10, m1
%assign x 0
%rep 8
    movzx       r7d, r10b
    shr         r10, 8

    movsx       r8d, word [r12 + x*2]   ; diff[x]
    inc         dword  [r6 + r7]        ; count[classIdx]++
    add         [r5 + r7], r8d          ; stats[classIdx] += (fenc[x] - rec[x]);
%assign x x+1
%endrep
    movhlps     m1, m1
    sub         r9d, 8
    add         r12, 8*2

.proc8:
    movq        r10, m1
%assign x 0
%rep 8
    movzx       r7d, r10b
    shr         r10, 8

    movsx       r8d, word [r12 + x*2]   ; diff[x]
    inc         dword  [r6 + r7]        ; count[classIdx]++
    add         [r5 + r7], r8d          ; stats[classIdx] += (fenc[x] - rec[x]);
    dec         r9d
    jz         .next
%assign x x+1
%endrep

    add         r12, 8*2
    add         r11, 16
    jmp        .loopL

.next:
    add         r0, 64*2                ; MAX_CU_SIZE
    add         r1, r2
    dec         r4d
    jnz        .loopH
    RET
%endif

;-----------------------------------------------------------------------------------------------------------------------
; saoCuStatsE0(const int16_t *diff, const pixel *rec, intptr_t stride, int endX, int endY, int32_t *stats, int32_t *count)
;-----------------------------------------------------------------------------------------------------------------------
%if ARCH_X86_64

%if HIGH_BIT_DEPTH == 1
INIT_XMM sse4
cglobal saoCuStatsE0, 3,10,8, 0-32
    mov         r3d, r3m
    mov         r4d, r4m
    mov         r9, r5mp

    ; clear internal temporary buffer
    pxor        m0, m0
    mova        [rsp], m0
    mova        [rsp + mmsize], m0
    mova        m4, [pw_1]
    mova        m5, [pb_2]
    xor         r7d, r7d

    ; correct stride for diff[] and rec
    mov         r6d, r3d
    and         r6d, ~15
    sub         r2, r6
    lea         r8, [(r6 - 64) * 2]             ; 64 = MAX_CU_SIZE

    FIX_STRIDES r2

.loopH:
    mov         r5d, r3d

    ; calculate signLeft
    mov         r7w, [r1]
    sub         r7w, [r1 - SIZEOF_PIXEL]
    seta        r7b
    setb        r6b
    sub         r7b, r6b
    neg         r7b
    pinsrb      m0, r7d, 15

.loopL:

    movu        m3, [r1]
    movu        m2, [r1 + SIZEOF_PIXEL]
    pcmpgtw     m6, m3, m2
    pcmpgtw     m2, m3
    pand        m6, m4
    por         m2, m6

    movu        m3, [r1 + mmsize]
    movu        m6, [r1 + mmsize + SIZEOF_PIXEL]
    pcmpgtw     m7, m3, m6
    pcmpgtw     m6, m3
    pand        m7, m4
    por         m7, m6

    packsswb    m2, m7                          ; signRight

    palignr     m3, m2, m0, 15

    pxor        m6, m6
    psubb       m6, m3                          ; signLeft

    mova        m0, m2
    paddb       m2, m6
    paddb       m2, m5                          ; edgeType

    ; stats[edgeType]
%assign x 0
%rep 16
    pextrb      r7d, m2, x

    movsx       r6d, word [r0 + x * 2]
    inc         word [rsp + r7 * 2]             ; tmp_count[edgeType]++
    add         [rsp + 5 * 2 + r7 * 4], r6d     ; tmp_stats[edgeType] += (fenc[x] - rec[x])
    dec         r5d
    jz         .next
%assign x x+1
%endrep

    add         r0, 16*2
    add         r1, 16 * SIZEOF_PIXEL
    jmp        .loopL

.next:
    sub         r0, r8
    add         r1, r2

    dec         r4d
    jnz        .loopH

    ; sum to global buffer
    mov         r0, r6mp

    ; s_eoTable = {1, 2, 0, 3, 4}
    pmovzxwd    m0, [rsp + 0 * 2]
    pshufd      m0, m0, q3102
    movu        m1, [r0]
    paddd       m0, m1
    movu        [r0], m0
    movzx       r5d, word [rsp + 4 * 2]
    add         [r0 + 4 * 4], r5d

    movu        m0, [rsp + 5 * 2 + 0 * 4]
    pshufd      m0, m0, q3102
    movu        m1, [r9]
    paddd       m0, m1
    movu        [r9], m0
    mov         r6d, [rsp + 5 * 2 + 4 * 4]
    add         [r9 + 4 * 4], r6d
    RET
%endif ; HIGH_BIT_DEPTH=1


%if HIGH_BIT_DEPTH == 0
INIT_XMM sse4
cglobal saoCuStatsE0, 3,10,6, 0-32
    mov         r3d, r3m
    mov         r4d, r4m
    mov         r9, r5mp

    ; clear internal temporary buffer
    pxor        m0, m0
    mova        [rsp], m0
    mova        [rsp + mmsize], m0
    mova        m4, [pb_128]
    mova        m5, [pb_2]
    xor         r7d, r7d

    ; correct stride for diff[] and rec
    mov         r6d, r3d
    and         r6d, ~15
    sub         r2, r6
    lea         r8, [(r6 - 64) * 2]             ; 64 = MAX_CU_SIZE

.loopH:
    mov         r5d, r3d

    ; calculate signLeft
    mov         r7b, [r1]
    sub         r7b, [r1 - SIZEOF_PIXEL]
    seta        r7b
    setb        r6b
    sub         r7b, r6b
    neg         r7b
    pinsrb      m0, r7d, 15

.loopL:
    movu        m3, [r1]
    movu        m2, [r1 + SIZEOF_PIXEL]

    pxor        m1, m3, m4
    pxor        m2, m4
    pcmpgtb     m3, m1, m2
    pcmpgtb     m2, m1
    pand        m3, [pb_1]

    por         m2, m3                          ; signRight

    palignr     m3, m2, m0, 15
    psignb      m3, m4                          ; signLeft

    mova        m0, m2
    paddb       m2, m3
    paddb       m2, m5                          ; edgeType

    ; stats[edgeType]
%assign x 0
%rep 16
    pextrb      r7d, m2, x

    movsx       r6d, word [r0 + x * 2]
    inc         word [rsp + r7 * 2]             ; tmp_count[edgeType]++
    add         [rsp + 5 * 2 + r7 * 4], r6d     ; tmp_stats[edgeType] += (fenc[x] - rec[x])
    dec         r5d
    jz         .next
%assign x x+1
%endrep

    add         r0, 16*2
    add         r1, 16 * SIZEOF_PIXEL
    jmp        .loopL

.next:
    sub         r0, r8
    add         r1, r2

    dec         r4d
    jnz        .loopH

    ; sum to global buffer
    mov         r0, r6mp

    ; s_eoTable = {1, 2, 0, 3, 4}
    pmovzxwd    m0, [rsp + 0 * 2]
    pshufd      m0, m0, q3102
    movu        m1, [r0]
    paddd       m0, m1
    movu        [r0], m0
    movzx       r5d, word [rsp + 4 * 2]
    add         [r0 + 4 * 4], r5d

    movu        m0, [rsp + 5 * 2 + 0 * 4]
    pshufd      m0, m0, q3102
    movu        m1, [r9]
    paddd       m0, m1
    movu        [r9], m0
    mov         r6d, [rsp + 5 * 2 + 4 * 4]
    add         [r9 + 4 * 4], r6d
    RET
%endif ; HIGH_BIT_DEPTH=0


;-----------------------------------------------------------------------------------------------------------------------
; saoCuStatsE0(const int16_t *diff, const pixel *rec, intptr_t stride, int endX, int endY, int32_t *stats, int32_t *count)
;-----------------------------------------------------------------------------------------------------------------------
INIT_YMM avx2
; spending rbp register to avoid x86inc stack alignment problem
cglobal saoCuStatsE0, 3,11,16
    mov         r3d, r3m
    mov         r4d, r4m
    mov         r9, r5mp

    ; clear internal temporary buffer
    pxor        xm6, xm6                        ; count[0]
    pxor        xm7, xm7                        ; count[1]
    pxor        xm8, xm8                        ; count[2]
    pxor        xm9, xm9                        ; count[3]
    pxor        xm10, xm10                      ; count[4]
    pxor        xm11, xm11                      ; stats[0]
    pxor        xm12, xm12                      ; stats[1]
    pxor        xm13, xm13                      ; stats[2]
    pxor        xm14, xm14                      ; stats[3]
    pxor        xm15, xm15                      ; stats[4]
    xor         r7d, r7d

    ; correct stride for diff[] and rec
    mov         r6d, r3d
    and         r6d, ~15
    sub         r2, r6
    lea         r8, [(r6 - 64) * 2]             ; 64 = MAX_CU_SIZE
    lea         r10, [pb_movemask_32 + 32]

.loopH:
    mov         r5d, r3d

    ; calculate signLeft
    mov         r7b, [r1]
    sub         r7b, [r1 - 1]
    seta        r7b
    setb        r6b
    sub         r7b, r6b
    neg         r7b
    pinsrb      xm0, r7d, 15

.loopL:
    mova        m4, [pb_128]                    ; lower performance, but we haven't enough register for stats[]
    movu        xm3, [r1]
    movu        xm2, [r1 + 1]

    pxor        xm1, xm3, xm4
    pxor        xm2, xm4
    pcmpgtb     xm3, xm1, xm2
    pcmpgtb     xm2, xm1
    pand        xm3, [pb_1]
    por         xm2, xm3                        ; signRight

    palignr     xm3, xm2, xm0, 15
    psignb      xm3, xm4                        ; signLeft

    mova        xm0, xm2
    paddb       xm2, xm3
    paddb       xm2, [pb_2]                     ; edgeType

    ; get current process mask
    mov         r7d, 16
    mov         r6d, r5d
    cmp         r5d, r7d
    cmovge      r6d, r7d
    neg         r6
    movu        xm1, [r10 + r6]

    ; tmp_count[edgeType]++
    ; tmp_stats[edgeType] += (fenc[x] - rec[x])
    pxor        xm3, xm3
    por         xm1, xm2                        ; apply unavailable pixel mask
    movu        m5, [r0]                        ; up to 14bits

    pcmpeqb     xm3, xm1, xm3
    psubb       xm6, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m4, m5, m2
    paddd       m11, m4

    pcmpeqb     xm3, xm1, [pb_1]
    psubb       xm7, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m4, m5, m2
    paddd       m12, m4

    pcmpeqb     xm3, xm1, [pb_2]
    psubb       xm8, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m4, m5, m2
    paddd       m13, m4

    pcmpeqb     xm3, xm1, [pb_3]
    psubb       xm9, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m4, m5, m2
    paddd       m14, m4

    pcmpeqb     xm3, xm1, [pb_4]
    psubb       xm10, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m4, m5, m2
    paddd       m15, m4

    sub         r5d, r7d
    jle        .next

    add         r0, 16*2
    add         r1, 16
    jmp        .loopL

.next:
    sub         r0, r8
    add         r1, r2

    dec         r4d
    jnz        .loopH

    ; sum to global buffer
    mov         r0, r6mp

    ; sum into word
    ; WARNING: There have a ovberflow bug on case Block64x64 with ALL pixels are SAME type (HM algorithm never pass Block64x64 into here)
    pxor        xm0, xm0
    psadbw      xm1, xm6, xm0
    psadbw      xm2, xm7, xm0
    psadbw      xm3, xm8, xm0
    psadbw      xm4, xm9, xm0
    psadbw      xm5, xm10, xm0
    pshufd      xm1, xm1, q3120
    pshufd      xm2, xm2, q3120
    pshufd      xm3, xm3, q3120
    pshufd      xm4, xm4, q3120

    ; sum count[4] only
    movhlps     xm6, xm5
    paddd       xm5, xm6

    ; sum count[s_eoTable]
    ; s_eoTable = {1, 2, 0, 3, 4}
    punpcklqdq  xm3, xm1
    punpcklqdq  xm2, xm4
    phaddd      xm3, xm2
    movu        xm1, [r0]
    paddd       xm3, xm1
    movu        [r0], xm3
    movd        r5d, xm5
    add         [r0 + 4 * 4], r5d

    ; sum stats[s_eoTable]
    vextracti128 xm1, m11, 1
    paddd       xm1, xm11
    vextracti128 xm2, m12, 1
    paddd       xm2, xm12
    vextracti128 xm3, m13, 1
    paddd       xm3, xm13
    vextracti128 xm4, m14, 1
    paddd       xm4, xm14
    vextracti128 xm5, m15, 1
    paddd       xm5, xm15

    ; s_eoTable = {1, 2, 0, 3, 4}
    phaddd      xm3, xm1
    phaddd      xm2, xm4
    phaddd      xm3, xm2
    psubd       xm3, xm0, xm3               ; negtive for compensate PMADDWD sign algorithm problem

    ; sum stats[4] only
    HADDD       xm5, xm6
    psubd       xm5, xm0, xm5

    movu        xm1, [r9]
    paddd       xm3, xm1
    movu        [r9], xm3
    movd        r6d, xm5
    add         [r9 + 4 * 4], r6d
    RET
%endif

;-------------------------------------------------------------------------------------------------------------------------------------------
; saoCuStatsE1_c(const int16_t *diff, const pixel *rec, intptr_t stride, int8_t *upBuff1, int endX, int endY, int32_t *stats, int32_t *count)
;-------------------------------------------------------------------------------------------------------------------------------------------
%if ARCH_X86_64

%if HIGH_BIT_DEPTH
INIT_XMM sse4
cglobal saoCuStatsE1, 4,12,8,0-32    ; Stack: 5 of stats and 5 of count
    mov         r5d, r5m
    mov         r4d, r4m

    ; clear internal temporary buffer
    pxor        m0, m0
    mova        [rsp], m0
    mova        [rsp + mmsize], m0
    mova        m5, [pw_1]
    mova        m6, [pb_2]
    movh        m7, [r3 + r4]

    FIX_STRIDES r2d

.loopH:
    mov         r6d, r4d
    mov         r9, r0
    mov         r10, r1
    mov         r11, r3

.loopW:
    ; signDown
    movu        m1, [r10]
    movu        m2, [r10 + r2]
    pcmpgtw     m3, m1, m2
    pcmpgtw     m2, m1
    pand        m3, m5
    por         m2, m3

    movu        m3, [r10 + mmsize]
    movu        m4, [r10 + mmsize + r2]
    pcmpgtw     m0, m3, m4
    pcmpgtw     m4, m3
    pand        m0, m5
    por         m4, m0
    packsswb    m2, m4

    pxor        m3, m3
    psubb       m3, m2                          ; -signDown

    ; edgeType
    movu        m4, [r11]
    paddb       m4, m6
    paddb       m2, m4

    ; update upBuff1
    movu        [r11], m3

    ; 16 pixels
%assign x 0
%rep 16
    pextrb      r7d, m2, x
    inc         word [rsp + r7 * 2]

    ; stats[edgeType]
    movsx       r8d, word [r9 + x * 2]
    add         [rsp + 5 * 2 + r7 * 4], r8d

    dec         r6d
    jz         .next
%assign x x+1
%endrep

    add         r9, mmsize * 2
    add         r10, mmsize * SIZEOF_PIXEL
    add         r11, mmsize
    jmp        .loopW

.next:
    ; restore pointer upBuff1
    add         r0, 64*2                        ; MAX_CU_SIZE
    add         r1, r2

    dec         r5d
    jg         .loopH

    ; restore unavailable pixels
    movh        [r3 + r4], m7

    ; sum to global buffer
    mov         r1, r6m
    mov         r0, r7m

    ; s_eoTable = {1,2,0,3,4}
    pmovzxwd    m0, [rsp + 0 * 2]
    pshufd      m0, m0, q3102
    movu        m1, [r0]
    paddd       m0, m1
    movu        [r0], m0
    movzx       r5d, word [rsp + 4 * 2]
    add         [r0 + 4 * 4], r5d

    movu        m0, [rsp + 5 * 2 + 0 * 4]
    pshufd      m0, m0, q3102
    movu        m1, [r1]
    paddd       m0, m1
    movu        [r1], m0
    mov         r6d, [rsp + 5 * 2 + 4 * 4]
    add         [r1 + 4 * 4], r6d
    RET

%else ; HIGH_BIT_DEPTH == 1

INIT_XMM sse4
cglobal saoCuStatsE1, 4,12,8,0-32    ; Stack: 5 of stats and 5 of count
    mov         r5d, r5m
    mov         r4d, r4m

    ; clear internal temporary buffer
    pxor        m0, m0
    mova        [rsp], m0
    mova        [rsp + mmsize], m0
    mova        m0, [pb_128]
    mova        m5, [pb_1]
    mova        m6, [pb_2]
    movh        m7, [r3 + r4]

.loopH:
    mov         r6d, r4d
    mov         r9, r0
    mov         r10, r1
    mov         r11, r3

.loopW:
    movu        m1, [r10]
    movu        m2, [r10 + r2]

    ; signDown
    pxor        m1, m0
    pxor        m2, m0
    pcmpgtb     m3, m1, m2
    pcmpgtb     m2, m1
    pand        m3, m5
    por         m2, m3
    pxor        m3, m3
    psubb       m3, m2                          ; -signDown

    ; edgeType
    movu        m4, [r11]
    paddb       m4, m6
    paddb       m2, m4

    ; update upBuff1
    movu        [r11], m3

    ; 16 pixels
%assign x 0
%rep 16
    pextrb      r7d, m2, x
    inc         word [rsp + r7 * 2]

    ; stats[edgeType]
    movsx       r8d, word [r9 + x * 2]
    add         [rsp + 5 * 2 + r7 * 4], r8d

    dec         r6d
    jz         .next
%assign x x+1
%endrep

    add         r9, 16*2
    add         r10, 16
    add         r11, 16
    jmp        .loopW

.next:
    ; restore pointer upBuff1
    add         r0, 64*2                        ; MAX_CU_SIZE
    add         r1, r2

    dec         r5d
    jg         .loopH

    ; restore unavailable pixels
    movh        [r3 + r4], m7

    ; sum to global buffer
    mov         r1, r6m
    mov         r0, r7m

    ; s_eoTable = {1,2,0,3,4}
    pmovzxwd    m0, [rsp + 0 * 2]
    pshufd      m0, m0, q3102
    movu        m1, [r0]
    paddd       m0, m1
    movu        [r0], m0
    movzx       r5d, word [rsp + 4 * 2]
    add         [r0 + 4 * 4], r5d

    movu        m0, [rsp + 5 * 2 + 0 * 4]
    pshufd      m0, m0, q3102
    movu        m1, [r1]
    paddd       m0, m1
    movu        [r1], m0
    mov         r6d, [rsp + 5 * 2 + 4 * 4]
    add         [r1 + 4 * 4], r6d
    RET
%endif ; HIGH_BIT_DEPTH == 0


INIT_YMM avx2
cglobal saoCuStatsE1, 4,13,16       ; Stack: 5 of stats and 5 of count
    mov         r5d, r5m
    mov         r4d, r4m

    ; clear internal temporary buffer
    pxor        xm6, xm6                            ; count[0]
    pxor        xm7, xm7                            ; count[1]
    pxor        xm8, xm8                            ; count[2]
    pxor        xm9, xm9                            ; count[3]
    pxor        xm10, xm10                          ; count[4]
    pxor        xm11, xm11                          ; stats[0]
    pxor        xm12, xm12                          ; stats[1]
    pxor        xm13, xm13                          ; stats[2]
    pxor        xm14, xm14                          ; stats[3]
    pxor        xm15, xm15                          ; stats[4]
    mova        m0, [pb_128]
    mova        m5, [pb_1]

    ; save unavailable bound pixel
    push  qword [r3 + r4]

    ; unavailable mask
    lea         r12, [pb_movemask_32 + 32]

.loopH:
    mov         r6d, r4d
    mov         r9, r0
    mov         r10, r1
    mov         r11, r3

.loopW:
    movu        xm1, [r10]
    movu        xm2, [r10 + r2]

    ; signDown
    pxor        xm1, xm0
    pxor        xm2, xm0
    pcmpgtb     xm3, xm1, xm2
    pcmpgtb     xm2, xm1
    pand        xm3, xm5
    por         xm2, xm3
    psignb      xm3, xm2, xm0                       ; -signDown

    ; edgeType
    movu        xm4, [r11]
    paddb       xm4, [pb_2]
    paddb       xm2, xm4

    ; update upBuff1 (must be delay, above code modify memory[r11])
    movu        [r11], xm3

    ; m[1-4] free in here

    ; get current process group mask
    mov         r7d, 16
    mov         r8d, r6d
    cmp         r6d, r7d
    cmovge      r8d, r7d
    neg         r8
    movu        xm1, [r12 + r8]

    ; tmp_count[edgeType]++
    ; tmp_stats[edgeType] += (fenc[x] - rec[x])
    pxor        xm3, xm3
    por         xm1, xm2                            ; apply unavailable pixel mask
    movu        m4, [r9]                            ; up to 14bits

    pcmpeqb     xm3, xm1, xm3
    psubb       xm6, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m11, m3

    pcmpeqb     xm3, xm1, xm5
    psubb       xm7, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m12, m3

    pcmpeqb     xm3, xm1, [pb_2]
    psubb       xm8, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m13, m3

    pcmpeqb     xm3, xm1, [pb_3]
    psubb       xm9, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m14, m3

    pcmpeqb     xm3, xm1, [pb_4]
    psubb       xm10, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m15, m3

    sub         r6d, r7d
    jle        .next

    add         r9, 16*2
    add         r10, 16
    add         r11, 16
    jmp        .loopW

.next:
    ; restore pointer upBuff1
    add         r0, 64*2                            ; MAX_CU_SIZE
    add         r1, r2

    dec         r5d
    jg         .loopH

    ; restore unavailable pixels
    pop   qword [r3 + r4]

    ; sum to global buffer
    mov         r1, r6m
    mov         r0, r7m

    ; sum into word
    ; WARNING: There have a ovberflow bug on case Block64x64 with ALL pixels are SAME type (HM algorithm never pass Block64x64 into here)
    pxor        xm0, xm0
    psadbw      xm1, xm6, xm0
    psadbw      xm2, xm7, xm0
    psadbw      xm3, xm8, xm0
    psadbw      xm4, xm9, xm0
    psadbw      xm5, xm10, xm0
    pshufd      xm1, xm1, q3120
    pshufd      xm2, xm2, q3120
    pshufd      xm3, xm3, q3120
    pshufd      xm4, xm4, q3120

    ; sum count[4] only
    movhlps     xm6, xm5
    paddd       xm5, xm6

    ; sum count[s_eoTable]
    ; s_eoTable = {1, 2, 0, 3, 4}
    punpcklqdq  xm3, xm1
    punpcklqdq  xm2, xm4
    phaddd      xm3, xm2
    movu        xm1, [r0]
    paddd       xm3, xm1
    movu        [r0], xm3
    movd        r5d, xm5
    add         [r0 + 4 * 4], r5d

    ; sum stats[s_eoTable]
    vextracti128 xm1, m11, 1
    paddd       xm1, xm11
    vextracti128 xm2, m12, 1
    paddd       xm2, xm12
    vextracti128 xm3, m13, 1
    paddd       xm3, xm13
    vextracti128 xm4, m14, 1
    paddd       xm4, xm14
    vextracti128 xm5, m15, 1
    paddd       xm5, xm15

    ; s_eoTable = {1, 2, 0, 3, 4}
    phaddd      xm3, xm1
    phaddd      xm2, xm4
    phaddd      xm3, xm2
    psubd       xm3, xm0, xm3               ; negtive for compensate PMADDWD sign algorithm problem

    ; sum stats[4] only
    HADDD       xm5, xm6
    psubd       xm5, xm0, xm5

    movu        xm1, [r1]
    paddd       xm3, xm1
    movu        [r1], xm3
    movd        r6d, xm5
    add         [r1 + 4 * 4], r6d
    RET
%endif ; ARCH_X86_64


;void saoCuStatsE2_c(const int16_t *fenc, const pixel *rec, intptr_t stride, int8_t *upBuff1, int8_t *upBufft, int endX, int endY, int32_t *stats, int32_t *count)
;{
;    X265_CHECK(endX < MAX_CU_SIZE, "endX check failure\n");
;    X265_CHECK(endY < MAX_CU_SIZE, "endY check failure\n");
;    int x, y;
;    int32_t tmp_stats[SAO::NUM_EDGETYPE];
;    int32_t tmp_count[SAO::NUM_EDGETYPE];
;    memset(tmp_stats, 0, sizeof(tmp_stats));
;    memset(tmp_count, 0, sizeof(tmp_count));
;    for (y = 0; y < endY; y++)
;    {
;        upBufft[0] = signOf(rec[stride] - rec[-1]);
;        for (x = 0; x < endX; x++)
;        {
;            int signDown = signOf2(rec[x], rec[x + stride + 1]);
;            X265_CHECK(signDown == signOf(rec[x] - rec[x + stride + 1]), "signDown check failure\n");
;            uint32_t edgeType = signDown + upBuff1[x] + 2;
;            upBufft[x + 1] = (int8_t)(-signDown);
;            tmp_stats[edgeType] += diff[x];
;            tmp_count[edgeType]++;
;        }
;        std::swap(upBuff1, upBufft);
;        rec += stride;
;        fenc += stride;
;    }
;    for (x = 0; x < SAO::NUM_EDGETYPE; x++)
;    {
;        stats[SAO::s_eoTable[x]] += tmp_stats[x];
;        count[SAO::s_eoTable[x]] += tmp_count[x];
;    }
;}

%if ARCH_X86_64

%if HIGH_BIT_DEPTH == 1
INIT_XMM sse4
cglobal saoCuStatsE2, 5,9,7,0-32    ; Stack: 5 of stats and 5 of count
    mov         r5d, r5m
    FIX_STRIDES r2d

    ; clear internal temporary buffer
    pxor        m0, m0
    mova        [rsp], m0
    mova        [rsp + mmsize], m0
    mova        m5, [pw_1]
    mova        m6, [pb_2]

.loopH:
    ; TODO: merge into SIMD in below
    ; get upBuffX[0]
    mov         r6w, [r1 + r2]
    sub         r6w, [r1 -  1 * SIZEOF_PIXEL]
    seta        r6b
    setb        r7b
    sub         r6b, r7b
    mov         [r4], r6b

    ; backup unavailable pixels
    movh        m0, [r4 + r5 + 1]

    mov         r6d, r5d
.loopW:
    ; signDown
    ; stats[edgeType]
    ; edgeType
    movu        m1, [r1]
    movu        m2, [r1 + r2 + 1 * SIZEOF_PIXEL]
    pcmpgtw     m3, m1, m2
    pcmpgtw     m2, m1
    pand        m2, m5
    por         m3, m2

    movu        m1, [r1 + mmsize]
    movu        m2, [r1 + r2 + 1 * SIZEOF_PIXEL + mmsize]
    pcmpgtw     m4, m1, m2
    pcmpgtw     m2, m1
    pand        m2, m5
    por         m4, m2
    packsswb    m3, m4

    movu        m4, [r3]
    paddb       m4, m6
    psubb       m4, m3

    ; update upBuff1
    movu        [r4 + 1], m3

    ; 16 pixels
%assign x 0
%rep 16
    pextrb      r7d, m4, x
    inc    word [rsp + r7 * 2]

    movsx       r8d, word [r0 + x * 2]
    add         [rsp + 5 * 2 + r7 * 4], r8d

    dec         r6d
    jz         .next
%assign x x+1
%endrep

    add         r0, mmsize * 2
    add         r1, mmsize * SIZEOF_PIXEL
    add         r3, mmsize
    add         r4, mmsize
    jmp        .loopW

.next:
    xchg        r3, r4

    ; restore pointer upBuff1
    mov         r6d, r5d
    and         r6d, ~15
    neg         r6                              ; MUST BE 64-bits, it is Negtive

    ; move to next row

    ; move back to start point
    add         r3, r6
    add         r4, r6

    ; adjust with stride
    lea         r0, [r0 + (r6 + 64) * 2]        ; 64 = MAX_CU_SIZE
    add         r1, r2
    lea         r1, [r1 + r6 * SIZEOF_PIXEL]

    ; restore unavailable pixels
    movh        [r3 + r5 + 1], m0

    dec    byte r6m
    jg         .loopH

    ; sum to global buffer
    mov         r1, r7m
    mov         r0, r8m

    ; s_eoTable = {1,2,0,3,4}
    pmovzxwd    m0, [rsp + 0 * 2]
    pshufd      m0, m0, q3102
    movu        m1, [r0]
    paddd       m0, m1
    movu        [r0], m0
    movzx       r5d, word [rsp + 4 * 2]
    add         [r0 + 4 * 4], r5d

    movu        m0, [rsp + 5 * 2 + 0 * 4]
    pshufd      m0, m0, q3102
    movu        m1, [r1]
    paddd       m0, m1
    movu        [r1], m0
    mov         r6d, [rsp + 5 * 2 + 4 * 4]
    add         [r1 + 4 * 4], r6d
    RET

%else ; HIGH_BIT_DEPTH == 1

; TODO: x64 only because I need temporary register r7,r8, easy portab to x86
INIT_XMM sse4
cglobal saoCuStatsE2, 5,9,8,0-32    ; Stack: 5 of stats and 5 of count
    mov         r5d, r5m

    ; clear internal temporary buffer
    pxor        m0, m0
    mova        [rsp], m0
    mova        [rsp + mmsize], m0
    mova        m0, [pb_128]
    mova        m5, [pb_1]
    mova        m6, [pb_2]

.loopH:
    ; TODO: merge into SIMD in below
    ; get upBuffX[0]
    mov         r6b, [r1 + r2]
    sub         r6b, [r1 -  1]
    seta        r6b
    setb        r7b
    sub         r6b, r7b
    mov         [r4], r6b

    ; backup unavailable pixels
    movh        m7, [r4 + r5 + 1]

    mov         r6d, r5d
.loopW:
    movu        m1, [r1]
    movu        m2, [r1 + r2 + 1]

    ; signDown
    ; stats[edgeType]
    pxor        m1, m0
    pxor        m2, m0
    pcmpgtb     m3, m1, m2
    pand        m3, m5
    pcmpgtb     m2, m1
    por         m2, m3
    pxor        m3, m3
    psubb       m3, m2

    ; edgeType
    movu        m4, [r3]
    paddb       m4, m6
    paddb       m2, m4

    ; update upBuff1
    movu        [r4 + 1], m3

    ; 16 pixels
%assign x 0
%rep 16
    pextrb      r7d, m2, x
    inc    word [rsp + r7 * 2]

    movsx       r8d, word [r0 + x * 2]
    add         [rsp + 5 * 2 + r7 * 4], r8d

    dec         r6d
    jz         .next
%assign x x+1
%endrep

    add         r0, 16*2
    add         r1, 16
    add         r3, 16
    add         r4, 16
    jmp        .loopW

.next:
    xchg        r3, r4

    ; restore pointer upBuff1
    mov         r6d, r5d
    and         r6d, ~15
    neg         r6                              ; MUST BE 64-bits, it is Negtive

    ; move to next row

    ; move back to start point
    add         r3, r6
    add         r4, r6

    ; adjust with stride
    lea         r0, [r0 + (r6 + 64) * 2]        ; 64 = MAX_CU_SIZE
    add         r1, r2
    add         r1, r6

    ; restore unavailable pixels
    movh        [r3 + r5 + 1], m7

    dec    byte r6m
    jg         .loopH

    ; sum to global buffer
    mov         r1, r7m
    mov         r0, r8m

    ; s_eoTable = {1,2,0,3,4}
    pmovzxwd    m0, [rsp + 0 * 2]
    pshufd      m0, m0, q3102
    movu        m1, [r0]
    paddd       m0, m1
    movu        [r0], m0
    movzx       r5d, word [rsp + 4 * 2]
    add         [r0 + 4 * 4], r5d

    movu        m0, [rsp + 5 * 2 + 0 * 4]
    pshufd      m0, m0, q3102
    movu        m1, [r1]
    paddd       m0, m1
    movu        [r1], m0
    mov         r6d, [rsp + 5 * 2 + 4 * 4]
    add         [r1 + 4 * 4], r6d
    RET

%endif ; HIGH_BIT_DEPTH == 0

INIT_YMM avx2
cglobal saoCuStatsE2, 5,10,16                        ; Stack: 5 of stats and 5 of count
    mov         r5d, r5m

    ; clear internal temporary buffer
    pxor        xm6, xm6                            ; count[0]
    pxor        xm7, xm7                            ; count[1]
    pxor        xm8, xm8                            ; count[2]
    pxor        xm9, xm9                            ; count[3]
    pxor        xm10, xm10                          ; count[4]
    pxor        xm11, xm11                          ; stats[0]
    pxor        xm12, xm12                          ; stats[1]
    pxor        xm13, xm13                          ; stats[2]
    pxor        xm14, xm14                          ; stats[3]
    pxor        xm15, xm15                          ; stats[4]
    mova        m0, [pb_128]

    ; unavailable mask
    lea         r9, [pb_movemask_32 + 32]

.loopH:
    ; TODO: merge into SIMD in below
    ; get upBuffX[0]
    mov         r6b, [r1 + r2]
    sub         r6b, [r1 -  1]
    seta        r6b
    setb        r7b
    sub         r6b, r7b
    mov         [r4], r6b

    ; backup unavailable pixels
    movq        xm5, [r4 + r5 + 1]

    mov         r6d, r5d
.loopW:
    movu        m1, [r1]
    movu        m2, [r1 + r2 + 1]

    ; signDown
    ; stats[edgeType]
    pxor        xm1, xm0
    pxor        xm2, xm0
    pcmpgtb     xm3, xm1, xm2
    pand        xm3, [pb_1]
    pcmpgtb     xm2, xm1
    por         xm2, xm3
    psignb      xm3, xm2, xm0

    ; edgeType
    movu        xm4, [r3]
    paddb       xm4, [pb_2]
    paddb       xm2, xm4

    ; update upBuff1
    movu        [r4 + 1], xm3

    ; m[1-4] free in here

    ; get current process group mask
    mov         r7d, 16
    mov         r8d, r6d
    cmp         r6d, r7d
    cmovge      r8d, r7d
    neg         r8
    movu        xm1, [r9 + r8]

    ; tmp_count[edgeType]++
    ; tmp_stats[edgeType] += (fenc[x] - rec[x])
    pxor        xm3, xm3
    por         xm1, xm2                            ; apply unavailable pixel mask
    movu        m4, [r0]                            ; up to 14bits

    pcmpeqb     xm3, xm1, xm3
    psubb       xm6, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m11, m3

    pcmpeqb     xm3, xm1, [pb_1]
    psubb       xm7, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m12, m3

    pcmpeqb     xm3, xm1, [pb_2]
    psubb       xm8, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m13, m3

    pcmpeqb     xm3, xm1, [pb_3]
    psubb       xm9, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m14, m3

    pcmpeqb     xm3, xm1, [pb_4]
    psubb       xm10, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m15, m3

    sub         r6d, r7d
    jle        .next

    add         r0, 16*2
    add         r1, 16
    add         r3, 16
    add         r4, 16
    jmp        .loopW

.next:
    xchg        r3, r4

    ; restore pointer upBuff1
    ; TODO: BZHI
    mov         r6d, r5d
    and         r6d, ~15
    neg         r6                              ; MUST BE 64-bits, it is Negtive

    ; move to next row

    ; move back to start point
    add         r3, r6
    add         r4, r6

    ; adjust with stride
    lea         r0, [r0 + (r6 + 64) * 2]        ; 64 = MAX_CU_SIZE
    add         r1, r2
    add         r1, r6

    ; restore unavailable pixels
    movq        [r3 + r5 + 1], xm5

    dec    byte r6m
    jg         .loopH

    ; sum to global buffer
    mov         r1, r7m
    mov         r0, r8m

    ; sum into word
    ; WARNING: There have a ovberflow bug on case Block64x64 with ALL pixels are SAME type (HM algorithm never pass Block64x64 into here)
    pxor        xm0, xm0
    psadbw      xm1, xm6, xm0
    psadbw      xm2, xm7, xm0
    psadbw      xm3, xm8, xm0
    psadbw      xm4, xm9, xm0
    psadbw      xm5, xm10, xm0
    pshufd      xm1, xm1, q3120
    pshufd      xm2, xm2, q3120
    pshufd      xm3, xm3, q3120
    pshufd      xm4, xm4, q3120

    ; sum count[4] only
    movhlps     xm6, xm5
    paddd       xm5, xm6

    ; sum count[s_eoTable]
    ; s_eoTable = {1, 2, 0, 3, 4}
    punpcklqdq  xm3, xm1
    punpcklqdq  xm2, xm4
    phaddd      xm3, xm2
    movu        xm1, [r0]
    paddd       xm3, xm1
    movu        [r0], xm3
    movd        r5d, xm5
    add         [r0 + 4 * 4], r5d

    ; sum stats[s_eoTable]
    vextracti128 xm1, m11, 1
    paddd       xm1, xm11
    vextracti128 xm2, m12, 1
    paddd       xm2, xm12
    vextracti128 xm3, m13, 1
    paddd       xm3, xm13
    vextracti128 xm4, m14, 1
    paddd       xm4, xm14
    vextracti128 xm5, m15, 1
    paddd       xm5, xm15

    ; s_eoTable = {1, 2, 0, 3, 4}
    phaddd      xm3, xm1
    phaddd      xm2, xm4
    phaddd      xm3, xm2
    psubd       xm3, xm0, xm3               ; negtive for compensate PMADDWD sign algorithm problem

    ; sum stats[4] only
    HADDD       xm5, xm6
    psubd       xm5, xm0, xm5

    movu        xm1, [r1]
    paddd       xm3, xm1
    movu        [r1], xm3
    movd        r6d, xm5
    add         [r1 + 4 * 4], r6d
    RET
%endif ; ARCH_X86_64


;void saoStatE3(const int16_t *diff, const pixel *rec, intptr_t stride, int8_t *upBuff1, int endX, int endY, int32_t *stats, int32_t *count);
;{
;    memset(tmp_stats, 0, sizeof(tmp_stats));
;    memset(tmp_count, 0, sizeof(tmp_count));
;    for (y = startY; y < endY; y++)
;    {
;        for (x = startX; x < endX; x++)
;        {
;            int signDown = signOf2(rec[x], rec[x + stride - 1]);
;            uint32_t edgeType = signDown + upBuff1[x] + 2;
;            upBuff1[x - 1] = (int8_t)(-signDown);
;            tmp_stats[edgeType] += diff[x];
;            tmp_count[edgeType]++;
;        }
;        upBuff1[endX - 1] = signOf(rec[endX - 1 + stride] - rec[endX]);
;        rec += stride;
;        fenc += stride;
;    }
;    for (x = 0; x < NUM_EDGETYPE; x++)
;    {
;        stats[s_eoTable[x]] += tmp_stats[x];
;        count[s_eoTable[x]] += tmp_count[x];
;    }
;}

%if ARCH_X86_64

%if HIGH_BIT_DEPTH == 1
INIT_XMM sse4
cglobal saoCuStatsE3, 4,9,8,0-32    ; Stack: 5 of stats and 5 of count
    mov         r4d, r4m
    mov         r5d, r5m
    FIX_STRIDES r2d

    ; clear internal temporary buffer
    pxor        m0, m0
    mova        [rsp], m0
    mova        [rsp + mmsize], m0
    ;mova        m0, [pb_128]
    mova        m5, [pw_1]
    mova        m6, [pb_2]
    movh        m7, [r3 + r4]

.loopH:
    mov         r6d, r4d

.loopW:
    ; signDown
    movu        m1, [r1]
    movu        m2, [r1 + r2 - 1 * SIZEOF_PIXEL]
    pcmpgtw     m3, m1, m2
    pcmpgtw     m2, m1
    pand        m2, m5
    por         m3, m2

    movu        m1, [r1 + mmsize]
    movu        m2, [r1 + r2 - 1 * SIZEOF_PIXEL + mmsize]
    pcmpgtw     m4, m1, m2
    pcmpgtw     m2, m1
    pand        m2, m5
    por         m4, m2
    packsswb    m3, m4

    ; edgeType
    movu        m4, [r3]
    paddb       m4, m6
    psubb       m4, m3

    ; update upBuff1
    movu        [r3 - 1], m3

    ; stats[edgeType]
    pxor        m1, m0

    ; 16 pixels
%assign x 0
%rep 16
    pextrb      r7d, m4, x
    inc    word [rsp + r7 * 2]

    movsx       r8d, word [r0 + x * 2]
    add         [rsp + 5 * 2 + r7 * 4], r8d

    dec         r6d
    jz         .next
%assign x x+1
%endrep

    add         r0, 16 * 2
    add         r1, 16 * SIZEOF_PIXEL
    add         r3, 16
    jmp         .loopW

.next:
    ; restore pointer upBuff1
    mov         r6d, r4d
    and         r6d, ~15
    neg         r6                              ; MUST BE 64-bits, it is Negtive

    ; move to next row

    ; move back to start point
    add         r3, r6

    ; adjust with stride
    lea         r0, [r0 + (r6 + 64) * 2]        ; 64 = MAX_CU_SIZE
    add         r1, r2
    lea         r1, [r1 + r6 * SIZEOF_PIXEL]

    dec         r5d
    jg         .loopH

    ; restore unavailable pixels
    movh        [r3 + r4], m7

    ; sum to global buffer
    mov         r1, r6m
    mov         r0, r7m

    ; s_eoTable = {1,2,0,3,4}
    pmovzxwd    m0, [rsp + 0 * 2]
    pshufd      m0, m0, q3102
    movu        m1, [r0]
    paddd       m0, m1
    movu        [r0], m0
    movzx       r5d, word [rsp + 4 * 2]
    add         [r0 + 4 * 4], r5d

    movu        m0, [rsp + 5 * 2 + 0 * 4]
    pshufd      m0, m0, q3102
    movu        m1, [r1]
    paddd       m0, m1
    movu        [r1], m0
    mov         r6d, [rsp + 5 * 2 + 4 * 4]
    add         [r1 + 4 * 4], r6d
    RET

%else ; HIGH_BIT_DEPTH == 1

INIT_XMM sse4
cglobal saoCuStatsE3, 4,9,8,0-32    ; Stack: 5 of stats and 5 of count
    mov         r4d, r4m
    mov         r5d, r5m

    ; clear internal temporary buffer
    pxor        m0, m0
    mova        [rsp], m0
    mova        [rsp + mmsize], m0
    mova        m0, [pb_128]
    mova        m5, [pb_1]
    mova        m6, [pb_2]
    movh        m7, [r3 + r4]

.loopH:
    mov         r6d, r4d

.loopW:
    movu        m1, [r1]
    movu        m2, [r1 + r2 - 1]

    ; signDown
    pxor        m1, m0
    pxor        m2, m0
    pcmpgtb     m3, m1, m2
    pand        m3, m5
    pcmpgtb     m2, m1
    por         m2, m3
    pxor        m3, m3
    psubb       m3, m2

    ; edgeType
    movu        m4, [r3]
    paddb       m4, m6
    paddb       m2, m4

    ; update upBuff1
    movu        [r3 - 1], m3

    ; stats[edgeType]
    pxor        m1, m0

    ; 16 pixels
%assign x 0
%rep 16
    pextrb      r7d, m2, x
    inc    word [rsp + r7 * 2]

    movsx       r8d, word [r0 + x * 2]
    add         [rsp + 5 * 2 + r7 * 4], r8d

    dec         r6d
    jz         .next
%assign x x+1
%endrep

    add         r0, 16*2
    add         r1, 16
    add         r3, 16
    jmp         .loopW

.next:
    ; restore pointer upBuff1
    mov         r6d, r4d
    and         r6d, ~15
    neg         r6                              ; MUST BE 64-bits, it is Negtive

    ; move to next row

    ; move back to start point
    add         r3, r6

    ; adjust with stride
    lea         r0, [r0 + (r6 + 64) * 2]        ; 64 = MAX_CU_SIZE
    add         r1, r2
    add         r1, r6

    dec         r5d
    jg         .loopH

    ; restore unavailable pixels
    movh        [r3 + r4], m7

    ; sum to global buffer
    mov         r1, r6m
    mov         r0, r7m

    ; s_eoTable = {1,2,0,3,4}
    pmovzxwd    m0, [rsp + 0 * 2]
    pshufd      m0, m0, q3102
    movu        m1, [r0]
    paddd       m0, m1
    movu        [r0], m0
    movzx       r5d, word [rsp + 4 * 2]
    add         [r0 + 4 * 4], r5d

    movu        m0, [rsp + 5 * 2 + 0 * 4]
    pshufd      m0, m0, q3102
    movu        m1, [r1]
    paddd       m0, m1
    movu        [r1], m0
    mov         r6d, [rsp + 5 * 2 + 4 * 4]
    add         [r1 + 4 * 4], r6d
    RET

%endif ; HIGH_BIT_DEPTH == 0

INIT_YMM avx2
cglobal saoCuStatsE3, 4,10,16           ; Stack: 5 of stats and 5 of count
    mov         r4d, r4m
    mov         r5d, r5m

    ; clear internal temporary buffer
    pxor        xm6, xm6                            ; count[0]
    pxor        xm7, xm7                            ; count[1]
    pxor        xm8, xm8                            ; count[2]
    pxor        xm9, xm9                            ; count[3]
    pxor        xm10, xm10                          ; count[4]
    pxor        xm11, xm11                          ; stats[0]
    pxor        xm12, xm12                          ; stats[1]
    pxor        xm13, xm13                          ; stats[2]
    pxor        xm14, xm14                          ; stats[3]
    pxor        xm15, xm15                          ; stats[4]
    mova        m0, [pb_128]

    ; unavailable mask
    lea         r9, [pb_movemask_32 + 32]
    push  qword [r3 + r4]

.loopH:
    mov         r6d, r4d

.loopW:
    movu        m1, [r1]
    movu        m2, [r1 + r2 - 1]

    ; signDown
    ; stats[edgeType]
    pxor        xm1, xm0
    pxor        xm2, xm0
    pcmpgtb     xm3, xm1, xm2
    pand        xm3, [pb_1]
    pcmpgtb     xm2, xm1
    por         xm2, xm3
    pxor        xm3, xm3
    psubb       xm3, xm2

    ; edgeType
    movu        xm4, [r3]
    paddb       xm4, [pb_2]
    paddb       xm2, xm4

    ; update upBuff1
    movu        [r3 - 1], xm3

    ; m[1-4] free in here

    ; get current process group mask
    mov         r7d, 16
    mov         r8d, r6d
    cmp         r6d, r7d
    cmovge      r8d, r7d
    neg         r8
    movu        xm1, [r9 + r8]

    ; tmp_count[edgeType]++
    ; tmp_stats[edgeType] += (fenc[x] - rec[x])
    pxor        xm3, xm3
    por         xm1, xm2                            ; apply unavailable pixel mask
    movu        m4, [r0]                            ; up to 14bits

    pcmpeqb     xm3, xm1, xm3
    psubb       xm6, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m11, m3

    pcmpeqb     xm3, xm1, [pb_1]
    psubb       xm7, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m12, m3

    pcmpeqb     xm3, xm1, [pb_2]
    psubb       xm8, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m13, m3

    pcmpeqb     xm3, xm1, [pb_3]
    psubb       xm9, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m14, m3

    pcmpeqb     xm3, xm1, [pb_4]
    psubb       xm10, xm3
    pmovsxbw    m2, xm3
    pmaddwd     m3, m4, m2
    paddd       m15, m3

    sub         r6d, r7d
    jle        .next

    add         r0, 16*2
    add         r1, 16
    add         r3, 16
    jmp        .loopW

.next:
    ; restore pointer upBuff1
    mov         r6d, r4d
    and         r6d, ~15
    neg         r6                              ; MUST BE 64-bits, it is Negtive

    ; move to next row

    ; move back to start point
    add         r3, r6

    ; adjust with stride
    lea         r0, [r0 + (r6 + 64) * 2]        ; 64 = MAX_CU_SIZE
    add         r1, r2
    add         r1, r6

    dec         r5d
    jg         .loopH

    ; restore unavailable pixels
    pop   qword [r3 + r4]

    ; sum to global buffer
    mov         r1, r6m
    mov         r0, r7m

    ; sum into word
    ; WARNING: There have a ovberflow bug on case Block64x64 with ALL pixels are SAME type (HM algorithm never pass Block64x64 into here)
    pxor        xm0, xm0
    psadbw      xm1, xm6, xm0
    psadbw      xm2, xm7, xm0
    psadbw      xm3, xm8, xm0
    psadbw      xm4, xm9, xm0
    psadbw      xm5, xm10, xm0
    pshufd      xm1, xm1, q3120
    pshufd      xm2, xm2, q3120
    pshufd      xm3, xm3, q3120
    pshufd      xm4, xm4, q3120

    ; sum count[4] only
    movhlps     xm6, xm5
    paddd       xm5, xm6

    ; sum count[s_eoTable]
    ; s_eoTable = {1, 2, 0, 3, 4}
    punpcklqdq  xm3, xm1
    punpcklqdq  xm2, xm4
    phaddd      xm3, xm2
    movu        xm1, [r0]
    paddd       xm3, xm1
    movu        [r0], xm3
    movd        r5d, xm5
    add         [r0 + 4 * 4], r5d

    ; sum stats[s_eoTable]
    vextracti128 xm1, m11, 1
    paddd       xm1, xm11
    vextracti128 xm2, m12, 1
    paddd       xm2, xm12
    vextracti128 xm3, m13, 1
    paddd       xm3, xm13
    vextracti128 xm4, m14, 1
    paddd       xm4, xm14
    vextracti128 xm5, m15, 1
    paddd       xm5, xm15

    ; s_eoTable = {1, 2, 0, 3, 4}
    phaddd      xm3, xm1
    phaddd      xm2, xm4
    phaddd      xm3, xm2
    psubd       xm3, xm0, xm3               ; negtive for compensate PMADDWD sign algorithm problem

    ; sum stats[4] only
    HADDD       xm5, xm6
    psubd       xm5, xm0, xm5

    movu        xm1, [r1]
    paddd       xm3, xm1
    movu        [r1], xm3
    movd        r6d, xm5
    add         [r1 + 4 * 4], r6d
    RET
%endif ; ARCH_X86_64


%if ARCH_X86_64
;; argument registers used -
; r0    - src
; r1    - srcStep
; r2    - offset
; r3    - tcP
; r4    - tcQ

INIT_XMM sse4
cglobal pelFilterLumaStrong_H, 5,7,10
%if HIGH_BIT_DEPTH
    add             r2d, r2d
%endif
    mov             r1, r2
    neg             r3d
    neg             r4d
    neg             r1

    lea             r5, [r2 * 3]
    lea             r6, [r1 * 3]

%if HIGH_BIT_DEPTH
    movu            m4, [r0]                ; src[0]
    movu            m3, [r0 + r1]           ; src[-offset]
    movu            m2, [r0 + r1 * 2]       ; src[-offset * 2]
    movu            m1, [r0 + r6]           ; src[-offset * 3]
    movu            m0, [r0 + r1 * 4]       ; src[-offset * 4]
    movu            m5, [r0 + r2]           ; src[offset]
    movu            m6, [r0 + r2 * 2]       ; src[offset * 2]
    movu            m7, [r0 + r5]           ; src[offset * 3]
%else
    pmovzxbw        m4, [r0]                ; src[0]
    pmovzxbw        m3, [r0 + r1]           ; src[-offset]
    pmovzxbw        m2, [r0 + r1 * 2]       ; src[-offset * 2]
    pmovzxbw        m1, [r0 + r6]           ; src[-offset * 3]
    pmovzxbw        m0, [r0 + r1 * 4]       ; src[-offset * 4]
    pmovzxbw        m5, [r0 + r2]           ; src[offset]
    pmovzxbw        m6, [r0 + r2 * 2]       ; src[offset * 2]
    pmovzxbw        m7, [r0 + r5]           ; src[offset * 3]
%endif

    paddw           m0, m0                  ; m0*2
    mova            m8, m2
    paddw           m8, m3                  ; m2 + m3
    paddw           m8, m4                  ; m2 + m3 + m4
    mova            m9, m8
    paddw           m9, m9                  ; 2*m2 + 2*m3 + 2*m4
    paddw           m8, m1                  ; m2 + m3 + m4 + m1
    paddw           m0, m8                  ; 2*m0 + m2+ m3 + m4 + m1
    paddw           m9, m1
    paddw           m0, m1
    paddw           m9, m5                  ; m1 + 2*m2 + 2*m3 + 2*m4 + m5
    paddw           m0, m1                  ; 2*m0 + 3*m1 + m2 + m3 + m4

    punpcklqdq      m0, m9
    punpcklqdq      m1, m3

    paddw           m3, m4
    mova            m9, m5
    paddw           m9, m6
    paddw           m7, m7                  ; 2*m7
    paddw           m9, m3                  ; m3 + m4 + m5 + m6
    mova            m3, m9
    paddw           m3, m3                  ; 2*m3 + 2*m4 + 2*m5 + 2*m6
    paddw           m7, m9                  ; 2*m7 + m3 + m4 + m5 + m6
    paddw           m7, m6
    psubw           m3, m6                  ; 2*m3 + 2*m4 + 2*m5 + m6
    paddw           m7, m6                  ; m3 + m4 + m5 + 3*m6 + 2*m7
    paddw           m3, m2                  ; m2 + 2*m3 + 2*m4 + 2*m5 + m6

    punpcklqdq      m9, m8
    punpcklqdq      m3, m7
    punpcklqdq      m5, m2
    punpcklqdq      m4, m6

    movd            m7, r3d                 ; -tcP
    movd            m2, r4d                 ; -tcQ
    pshufb          m7, [pb_01]
    pshufb          m2, [pb_01]
    mova            m6, m2
    punpcklqdq      m6, m7

    paddw           m0, [pw_4]
    paddw           m3, [pw_4]
    paddw           m9, [pw_2]

    psraw           m0, 3
    psraw           m3, 3
    psraw           m9, 2

    psubw           m0, m1
    psubw           m3, m4
    psubw           m9, m5

    pmaxsw          m0, m7
    pmaxsw          m3, m2
    pmaxsw          m9, m6
    psignw          m7, [pw_n1]
    psignw          m2, [pw_n1]
    psignw          m6, [pw_n1]
    pminsw          m0, m7
    pminsw          m3, m2
    pminsw          m9, m6

    paddw           m0, m1
    paddw           m3, m4
    paddw           m9, m5

%if HIGH_BIT_DEPTH
    movh            [r0 + r6], m0
    movhps          [r0 + r1], m0
    movh            [r0], m3
    movhps          [r0 + r2 * 2], m3,
    movh            [r0 + r2 * 1], m9
    movhps          [r0 + r1 * 2], m9
%else
    packuswb        m0, m0
    packuswb        m3, m9

    movd            [r0 + r6], m0
    pextrd          [r0 + r1], m0, 1
    movd            [r0], m3
    pextrd          [r0 + r2 * 2], m3, 1
    pextrd          [r0 + r2 * 1], m3, 2
    pextrd          [r0 + r1 * 2], m3, 3
%endif
    RET

INIT_XMM sse4
cglobal pelFilterLumaStrong_V, 5,5,10
%if HIGH_BIT_DEPTH
    add             r1d, r1d
%endif
    neg             r3d
    neg             r4d
    lea             r2, [r1 * 3]

%if HIGH_BIT_DEPTH
    movu            m0, [r0 - 8]            ; src[-offset * 4] row 0
    movu            m1, [r0 + r1 * 1 - 8]   ; src[-offset * 4] row 1
    movu            m2, [r0 + r1 * 2 - 8]   ; src[-offset * 4] row 2
    movu            m3, [r0 + r2 * 1 - 8]   ; src[-offset * 4] row 3

    punpckhwd       m4, m0, m1              ; [m4 m4 m5 m5 m6 m6 m7 m7]
    punpcklwd       m0, m1                  ; [m0 m0 m1 m1 m2 m2 m3 m3]

    punpckhwd       m5, m2, m3              ; [m4 m4 m5 m5 m6 m6 m7 m7]
    punpcklwd       m2, m3                  ; [m0 m0 m1 m1 m2 m2 m3 m3]

    punpckhdq       m3, m0, m2              ; [m2 m2 m2 m2 m3 m3 m3 m3]
    punpckldq       m0, m2                  ; [m0 m0 m0 m0 m1 m1 m1 m1]
    psrldq          m1, m0, 8               ; [m1 m1 m1 m1 x x x x]
    mova            m2, m3                  ; [m2 m2 m2 m2 x x x x]
    punpckhqdq      m3, m3                  ; [m3 m3 m3 m3 x x x x]

    punpckhdq       m6, m4, m5              ; [m6 m6 m6 m6 m7 m7 m7 m7]
    punpckldq       m4, m5                  ; [m4 m4 m4 m4 m5 m5 m5 m5]
    psrldq          m7, m6, 8
    psrldq          m5, m4, 8
%else
    movh            m0, [r0 - 4]            ; src[-offset * 4] row 0
    movh            m1, [r0 + r1 * 1 - 4]   ; src[-offset * 4] row 1
    movh            m2, [r0 + r1 * 2 - 4]   ; src[-offset * 4] row 2
    movh            m3, [r0 + r2 * 1 - 4]   ; src[-offset * 4] row 3

    punpcklbw       m0, m1
    punpcklbw       m2, m3
    mova            m4, m0
    punpcklwd       m0, m2
    punpckhwd       m4, m2
    mova            m1, m0
    mova            m2, m0
    mova            m3, m0
    pshufd          m0, m0, 0
    pshufd          m1, m1, 1
    pshufd          m2, m2, 2
    pshufd          m3, m3, 3
    mova            m5, m4
    mova            m6, m4
    mova            m7, m4
    pshufd          m4, m4, 0
    pshufd          m5, m5, 1
    pshufd          m6, m6, 2
    pshufd          m7, m7, 3
    pmovzxbw        m0, m0
    pmovzxbw        m1, m1
    pmovzxbw        m2, m2
    pmovzxbw        m3, m3
    pmovzxbw        m4, m4
    pmovzxbw        m5, m5
    pmovzxbw        m6, m6
    pmovzxbw        m7, m7
%endif

    paddw           m0, m0                  ; m0*2
    mova            m8, m2
    paddw           m8, m3                  ; m2 + m3
    paddw           m8, m4                  ; m2 + m3 + m4
    mova            m9, m8
    paddw           m9, m9                  ; 2*m2 + 2*m3 + 2*m4
    paddw           m8, m1                  ; m2 + m3 + m4 + m1
    paddw           m0, m8                  ; 2*m0 + m2+ m3 + m4 + m1
    paddw           m9, m1
    paddw           m0, m1
    paddw           m9, m5                  ; m1 + 2*m2 + 2*m3 + 2*m4 + m5
    paddw           m0, m1                  ; 2*m0 + 3*m1 + m2 + m3 + m4

    punpcklqdq      m0, m9
    punpcklqdq      m1, m3

    paddw           m3, m4
    mova            m9, m5
    paddw           m9, m6
    paddw           m7, m7                  ; 2*m7
    paddw           m9, m3                  ; m3 + m4 + m5 + m6
    mova            m3, m9
    paddw           m3, m3                  ; 2*m3 + 2*m4 + 2*m5 + 2*m6
    paddw           m7, m9                  ; 2*m7 + m3 + m4 + m5 + m6
    paddw           m7, m6
    psubw           m3, m6                  ; 2*m3 + 2*m4 + 2*m5 + m6
    paddw           m7, m6                  ; m3 + m4 + m5 + 3*m6 + 2*m7
    paddw           m3, m2                  ; m2 + 2*m3 + 2*m4 + 2*m5 + m6

    punpcklqdq      m9, m8
    punpcklqdq      m3, m7
    punpcklqdq      m5, m2
    punpcklqdq      m4, m6

    movd            m7, r3d                 ; -tcP
    movd            m2, r4d                 ; -tcQ
    pshufb          m7, [pb_01]
    pshufb          m2, [pb_01]
    mova            m6, m2
    punpcklqdq      m6, m7

    paddw           m0, [pw_4]
    paddw           m3, [pw_4]
    paddw           m9, [pw_2]

    psraw           m0, 3
    psraw           m3, 3
    psraw           m9, 2

    psubw           m0, m1
    psubw           m3, m4
    psubw           m9, m5

    pmaxsw          m0, m7
    pmaxsw          m3, m2
    pmaxsw          m9, m6
    psignw          m7, [pw_n1]
    psignw          m2, [pw_n1]
    psignw          m6, [pw_n1]
    pminsw          m0, m7
    pminsw          m3, m2
    pminsw          m9, m6

    paddw           m0, m1
    paddw           m3, m4
    paddw           m9, m5

%if HIGH_BIT_DEPTH
    ; 4x6 output rows -
    ; m0 - col 0
    ; m3 - col 3

    psrldq           m1, m0, 8
    psrldq           m2, m3, 8

    mova            m4, m9
    psrldq          m5, m9, 8

    ; transpose 4x6 to 6x4
    punpcklwd       m0, m5
    punpcklwd       m1, m3
    punpcklwd       m4, m2

    punpckldq       m9, m0, m1
    punpckhdq       m0, m1

    movh            [r0 + r1 * 0 - 6], m9
    movhps          [r0 + r1 * 1 - 6], m9
    movh            [r0 + r1 * 2 - 6], m0
    movhps          [r0 + r2 * 1 - 6], m0
    pextrd          [r0 + r1 * 0 + 2], m4, 0
    pextrd          [r0 + r1 * 1 + 2], m4, 1
    pextrd          [r0 + r1 * 2 + 2], m4, 2
    pextrd          [r0 + r2 * 1 + 2], m4, 3
%else
    packuswb        m0, m0
    packuswb        m3, m9

    ; 4x6 output rows -
    ; m0 - col 0
    ; m3 - col 3
    mova            m1, m0
    mova            m2, m3
    mova            m4, m3
    mova            m5, m3
    pshufd          m1, m1, 1               ; col 2
    pshufd          m2, m2, 1               ; col 5
    pshufd          m4, m4, 2               ; col 4
    pshufd          m5, m5, 3               ; col 1

    ; transpose 4x6 to 6x4
    punpcklbw       m0, m5
    punpcklbw       m1, m3
    punpcklbw       m4, m2
    punpcklwd       m0, m1

    movd            [r0 + r1 * 0 - 3], m0
    pextrd          [r0 + r1 * 1 - 3], m0, 1
    pextrd          [r0 + r1 * 2 - 3], m0, 2
    pextrd          [r0 + r2 * 1 - 3], m0, 3
    pextrw          [r0 + r1 * 0 + 1], m4, 0
    pextrw          [r0 + r1 * 1 + 1], m4, 1
    pextrw          [r0 + r1 * 2 + 1], m4, 2
    pextrw          [r0 + r2 * 1 + 1], m4, 3
%endif
    RET
%endif ; ARCH_X86_64

%if ARCH_X86_64
INIT_XMM sse4
cglobal pelFilterChroma_H, 6,6,5
%if HIGH_BIT_DEPTH
    add             r2d, r2d
%endif
    mov             r1, r2
    neg             r3d
    neg             r1

%if HIGH_BIT_DEPTH
    movu            m4, [r0]                ; src[0]
    movu            m3, [r0 + r1]           ; src[-offset]
    movu            m0, [r0 + r2]           ; src[offset]
    movu            m2, [r0 + r1 * 2]       ; src[-offset * 2]
%else
    pmovzxbw        m4, [r0]                ; src[0]
    pmovzxbw        m3, [r0 + r1]           ; src[-offset]
    pmovzxbw        m0, [r0 + r2]           ; src[offset]
    pmovzxbw        m2, [r0 + r1 * 2]       ; src[-offset * 2]
%endif

    psubw           m1, m4, m3              ; m4 - m3
    psubw           m2, m0                  ; m2 - m5
    paddw           m2, [pw_4]
    psllw           m1, 2                   ; (m4 - m3) * 4
    paddw           m1, m2
    psraw           m1, 3

    movd            m0, r3d
    pshufb          m0, [pb_01]             ; -tc

    pmaxsw          m1, m0
    psignw          m0, [pw_n1]
    pminsw          m1, m0                  ; delta
    punpcklqdq      m1, m1

    shl             r5d, 16
    or              r5w, r4w
    punpcklqdq      m3, m4
    mova            m2, [pw_1_ffff]

    movd            m0, r5d
    pshufb          m0, [pb_0123]

    pand            m0, m1                  ; (delta & maskP) (delta & maskQ)
    psignw          m0, m2
    paddw           m3, m0

    pxor            m0, m0
    pmaxsw          m3, m0
    pminsw          m3, [pw_pixel_max]

%if HIGH_BIT_DEPTH
    movh            [r0 + r1], m3
    movhps          [r0], m3
%else
    packuswb        m3, m3
    movd            [r0 + r1], m3
    pextrd          [r0], m3, 1
%endif
    RET

INIT_XMM sse4
cglobal pelFilterChroma_V, 6,6,5
%if HIGH_BIT_DEPTH
    add             r1d, r1d
%endif
    neg             r3d
    lea             r2, [r1 * 3]

%if HIGH_BIT_DEPTH
    movu            m4, [r0 + r1 * 0 - 4]   ; src[-offset*2, -offset, 0, offset] [m2 m3 m4 m5]
    movu            m3, [r0 + r1 * 1 - 4]
    movu            m0, [r0 + r1 * 2 - 4]
    movu            m2, [r0 + r2 * 1 - 4]
%else
    pmovzxbw        m4, [r0 + r1 * 0 - 2]   ; src[-offset*2, -offset, 0, offset] [m2 m3 m4 m5]
    pmovzxbw        m3, [r0 + r1 * 1 - 2]
    pmovzxbw        m0, [r0 + r1 * 2 - 2]
    pmovzxbw        m2, [r0 + r2 * 1 - 2]
%endif
    punpcklwd       m4, m3
    punpcklwd       m0, m2
    punpckldq       m2, m4, m0              ; [m2 m2 m2 m2 m3 m3 m3 m3]
    punpckhdq       m4, m0                  ; [m4 m4 m4 m4 m5 m5 m5 m5]
    psrldq          m3, m2, 8
    psrldq          m0, m4, 8

    psubw           m1, m4, m3              ; m4 - m3
    psubw           m2, m0                  ; m2 - m5
    paddw           m2, [pw_4]
    psllw           m1, 2                   ; (m4 - m3) * 4
    paddw           m1, m2
    psraw           m1, 3

    movd            m0, r3d
    pshufb          m0, [pb_01]             ; -tc

    pmaxsw          m1, m0
    psignw          m0, [pw_n1]
    pminsw          m1, m0                  ; delta
    punpcklqdq      m1, m1

    shl             r5d, 16
    or              r5w, r4w
    punpcklqdq      m3, m4
    mova            m2, [pw_1_ffff]

    movd            m0, r5d
    pshufb          m0, [pb_0123]

    pand            m0, m1                  ; (delta & maskP) (delta & maskQ)
    psignw          m0, m2
    paddw           m3, m0

    pxor            m0, m0
    pmaxsw          m3, m0
    pminsw          m3, [pw_pixel_max]

%if HIGH_BIT_DEPTH
    pshufb          m3, [pw_shuf_off4]
    pextrd          [r0 + r1 * 0 - 2], m3, 0
    pextrd          [r0 + r1 * 1 - 2], m3, 1
    pextrd          [r0 + r1 * 2 - 2], m3, 2
    pextrd          [r0 + r2 * 1 - 2], m3, 3
%else
    packuswb        m3, m3
    pshufb          m3, [pb_shuf_off4]
    pextrw          [r0 + r1 * 0 - 1], m3, 0
    pextrw          [r0 + r1 * 1 - 1], m3, 1
    pextrw          [r0 + r1 * 2 - 1], m3, 2
    pextrw          [r0 + r2 * 1 - 1], m3, 3
%endif
    RET
%endif ; ARCH_X86_64
