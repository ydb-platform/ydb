;*****************************************************************************
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Min Chen <chenm003@163.com> <min.chen@multicorewareinc.com>
;*          Praveen Tiwari <praveen@multicorewareinc.com>
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

const allAng4_shuf_mode2,       db  1,  2,  3,  4,  2,  3,  4,  5,  3,  4,  5,  6,  4,  5,  6,  7,  1,  2,  3,  4,  2,  3,  4,  5,  3,  4,  5,  6,  4,  5,  6,  7
const allAng4_shuf_mode3_4,     db  0,  1,  1,  2,  2,  3,  3,  4,  1,  2,  2,  3,  3,  4,  4,  5,  0,  1,  1,  2,  2,  3,  3,  4,  1,  2,  2,  3,  3,  4,  4,  5
                                db  2,  3,  3,  4,  4,  5,  5,  6,  3,  4,  4,  5,  5,  6,  6,  7,  1,  2,  2,  3,  3,  4,  4,  5,  2,  3,  3,  4,  4,  5,  5,  6
const allAng4_shuf_mode5_6,     db  0,  1,  1,  2,  2,  3,  3,  4,  1,  2,  2,  3,  3,  4,  4,  5,  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4
                                db  1,  2,  2,  3,  3,  4,  4,  5,  2,  3,  3,  4,  4,  5,  5,  6,  1,  2,  2,  3,  3,  4,  4,  5,  1,  2,  2,  3,  3,  4,  4,  5
const allAng4_shuf_mode7_8,     db  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4
                                db  0,  1,  1,  2,  2,  3,  3,  4,  1,  2,  2,  3,  3,  4,  4,  5,  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4
const allAng4_shuf_mode10,      db  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3
const allAng4_shuf_mode11_12,   db  0,  9,  9, 10, 10, 11, 11, 12,  0,  9,  9, 10, 10, 11, 11, 12,  0,  9,  9, 10, 10, 11, 11, 12,  0,  9,  9, 10, 10, 11, 11, 12
const allAng4_shuf_mode13_14,   db  0,  9,  9, 10, 10, 11, 11, 12,  4,  0,  0,  9,  9, 10, 10, 11,  2,  0,  0,  9,  9, 10, 10, 11,  2,  0,  0,  9,  9, 10, 10, 11
const allAng4_shuf_mode15_16,   db  0,  9,  9, 10, 10, 11, 11, 12,  2,  0,  0,  9,  9, 10, 10, 11,  0,  9,  9, 10, 10, 11, 11, 12,  2,  0,  0,  9,  9, 10, 10, 11
                                db  2,  0,  0,  9,  9, 10, 10, 11,  4,  2,  2,  0,  0,  9,  9, 10,  2,  0,  0,  9,  9, 10, 10, 11,  3,  2,  2,  0,  0,  9,  9, 10
const allAng4_shuf_mode17,      db  0,  9,  9, 10, 10, 11, 11, 12,  1,  0,  0,  9,  9, 10, 10, 11,  2,  1,  1,  0,  0,  9,  9, 10,  4,  2,  2,  1,  1,  0,  0,  9
                                db  0,  1,  2,  3,  9,  0,  1,  2, 10,  9,  0,  1, 11, 10,  9,  0,  0,  1,  2,  3,  9,  0,  1,  2, 10,  9,  0,  1, 11, 10,  9,  0
const allAng4_shuf_mode18,      db  0,  1,  2,  3,  9,  0,  1,  2, 10,  9,  0,  1, 11, 10,  9,  0,  0,  1,  2,  3,  9,  0,  1,  2, 10,  9,  0,  1, 11, 10,  9,  0
const allAng4_shuf_mode19_20,   db  0,  1,  1,  2,  2,  3,  3,  4,  9,  0,  0,  1,  1,  2,  2,  3,  0,  1,  1,  2,  2,  3,  3,  4, 10,  0,  0,  1,  1,  2,  2,  3
                                db 10,  9,  9,  0,  0,  1,  1,  2, 12, 10, 10,  9,  9,  0,  0,  1, 10,  0,  0,  1,  1,  2,  2,  3, 11, 10, 10,  0,  0,  1,  1,  2
const allAng4_shuf_mode21_22,   db  0,  1,  1,  2,  2,  3,  3,  4, 10,  0,  0,  1,  1,  2,  2,  3,  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4
                                db 10,  0,  0,  1,  1,  2,  2,  3, 12, 10, 10,  0,  0,  1,  1,  2, 10,  0,  0,  1,  1,  2,  2,  3, 10,  0,  0,  1,  1,  2,  2,  3
const allAng4_shuf_mode23_24,   db  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4
                                db  0,  1,  1,  2,  2,  3,  3,  4, 12,  0,  0,  1,  1,  2,  2,  3,  0,  1,  1,  2,  2,  3,  3,  4,  0,  1,  1,  2,  2,  3,  3,  4
const allAng4_shuf_mode26,      db  1,  2,  3,  4,  1,  2,  3,  4,  1,  2,  3,  4,  1,  2,  3,  4,  1,  2,  3,  4,  1,  2,  3,  4,  1,  2,  3,  4,  1,  2,  3,  4
const allAng4_shuf_mode27_28,   db  1,  2,  2,  3,  3,  4,  4,  5,  1,  2,  2,  3,  3,  4,  4,  5,  1,  2,  2,  3,  3,  4,  4,  5,  1,  2,  2,  3,  3,  4,  4,  5
const allAng4_shuf_mode29_30,   db  1,  2,  2,  3,  3,  4,  4,  5,  2,  3,  3,  4,  4,  5,  5,  6,  2,  3,  3,  4,  4,  5,  5,  6,  2,  3,  3,  4,  4,  5,  5,  6
const allAng4_shuf_mode31_32,   db  1,  2,  2,  3,  3,  4,  4,  5,  2,  3,  3,  4,  4,  5,  5,  6,  1,  2,  2,  3,  3,  4,  4,  5,  2,  3,  3,  4,  4,  5,  5,  6
                                db  2,  3,  3,  4,  4,  5,  5,  6,  3,  4,  4,  5,  5,  6,  6,  7,  2,  3,  3,  4,  4,  5,  5,  6,  3,  4,  4,  5,  5,  6,  6,  7
const allAng4_shuf_mode33,      db  1,  2,  2,  3,  3,  4,  4,  5,  2,  3,  3,  4,  4,  5,  5,  6,  3,  4,  4,  5,  5,  6,  6,  7,  4,  5,  5,  6,  6,  7,  7,  8
const allAng4_shuf_mode34,      db  2,  3,  4,  5,  3,  4,  5,  6,  4,  5,  6,  7,  5,  6,  7,  8,  2,  3,  4,  5,  3,  4,  5,  6,  4,  5,  6,  7,  5,  6,  7,  8

const allAng4_fact_mode3_4,     db  6, 26,  6, 26,  6, 26,  6, 26, 12, 20, 12, 20, 12, 20, 12, 20, 11, 21, 11, 21, 11, 21, 11, 21, 22, 10, 22, 10, 22, 10, 22, 10
                                db 18, 14, 18, 14, 18, 14, 18, 14, 24,  8, 24,  8, 24,  8, 24,  8,  1, 31,  1, 31,  1, 31,  1, 31, 12, 20, 12, 20, 12, 20, 12, 20
const allAng4_fact_mode5_6,     db 15, 17, 15, 17, 15, 17, 15, 17, 30,  2, 30,  2, 30,  2, 30,  2, 19, 13, 19, 13, 19, 13, 19, 13,  6, 26,  6, 26,  6, 26,  6, 26
                                db 13, 19, 13, 19, 13, 19, 13, 19, 28,  4, 28,  4, 28,  4, 28,  4, 25,  7, 25,  7, 25,  7, 25,  7, 12, 20, 12, 20, 12, 20, 12, 20
const allAng4_fact_mode7_8,     db 23,  9, 23,  9, 23,  9, 23,  9, 14, 18, 14, 18, 14, 18, 14, 18, 27,  5, 27,  5, 27,  5, 27,  5, 22, 10, 22, 10, 22, 10, 22, 10
                                db  5, 27,  5, 27,  5, 27,  5, 27, 28,  4, 28,  4, 28,  4, 28,  4, 17, 15, 17, 15, 17, 15, 17, 15, 12, 20, 12, 20, 12, 20, 12, 20
const allAng4_fact_mode9,       db 30,  2, 30,  2, 30,  2, 30,  2, 28,  4, 28,  4, 28,  4, 28,  4, 26,  6, 26,  6, 26,  6, 26,  6, 24,  8, 24,  8, 24,  8, 24,  8
const allAng4_fact_mode11_12,   db  2, 30,  2, 30,  2, 30,  2, 30,  4, 28,  4, 28,  4, 28,  4, 28,  5, 27,  5, 27,  5, 27,  5, 27, 10, 22, 10, 22, 10, 22, 10, 22
                                db  6, 26,  6, 26,  6, 26,  6, 26,  8, 24,  8, 24,  8, 24,  8, 24, 15, 17, 15, 17, 15, 17, 15, 17, 20, 12, 20, 12, 20, 12, 20, 12
const allAng4_fact_mode13_14,   db  9, 23,  9, 23,  9, 23,  9, 23, 18, 14, 18, 14, 18, 14, 18, 14, 13, 19, 13, 19, 13, 19, 13, 19, 26,  6, 26,  6, 26,  6, 26,  6
                                db 27,  5, 27,  5, 27,  5, 27,  5,  4, 28,  4, 28,  4, 28,  4, 28,  7, 25,  7, 25,  7, 25,  7, 25, 20, 12, 20, 12, 20, 12, 20, 12
const allAng4_fact_mode15_16,   db 17, 15, 17, 15, 17, 15, 17, 15,  2, 30,  2, 30,  2, 30,  2, 30, 21, 11, 21, 11, 21, 11, 21, 11, 10, 22, 10, 22, 10, 22, 10, 22
                                db 19, 13, 19, 13, 19, 13, 19, 13,  4, 28,  4, 28,  4, 28,  4, 28, 31,  1, 31,  1, 31,  1, 31,  1, 20, 12, 20, 12, 20, 12, 20, 12
const allAng4_fact_mode17,      db 26,  6, 26,  6, 26,  6, 26,  6, 20, 12, 20, 12, 20, 12, 20, 12, 14, 18, 14, 18, 14, 18, 14, 18,  8, 24,  8, 24,  8, 24,  8, 24
const allAng4_fact_mode19_20,   db 26,  6, 26,  6, 26,  6, 26,  6, 20, 12, 20, 12, 20, 12, 20, 12, 21, 11, 21, 11, 21, 11, 21, 11, 10, 22, 10, 22, 10, 22, 10, 22
                                db 14, 18, 14, 18, 14, 18, 14, 18,  8, 24,  8, 24,  8, 24,  8, 24, 31,  1, 31,  1, 31,  1, 31,  1, 20, 12, 20, 12, 20, 12, 20, 12
const allAng4_fact_mode21_22,   db 17, 15, 17, 15, 17, 15, 17, 15,  2, 30,  2, 30,  2, 30,  2, 30, 13, 19, 13, 19, 13, 19, 13, 19, 26,  6, 26,  6, 26,  6, 26,  6
                                db 19, 13, 19, 13, 19, 13, 19, 13,  4, 28,  4, 28,  4, 28,  4, 28,  7, 25,  7, 25,  7, 25,  7, 25, 20, 12, 20, 12, 20, 12, 20, 12
const allAng4_fact_mode23_24,   db  9, 23,  9, 23,  9, 23,  9, 23, 18, 14, 18, 14, 18, 14, 18, 14,  5, 27,  5, 27,  5, 27,  5, 27, 10, 22, 10, 22, 10, 22, 10, 22
                                db 27,  5, 27,  5, 27,  5, 27,  5,  4, 28,  4, 28,  4, 28,  4, 28, 15, 17, 15, 17, 15, 17, 15, 17, 20, 12, 20, 12, 20, 12, 20, 12
const allAng4_fact_mode25,      db  2, 30,  2, 30,  2, 30,  2, 30,  4, 28,  4, 28,  4, 28,  4, 28,  6, 26,  6, 26,  6, 26,  6, 26,  8, 24,  8, 24,  8, 24,  8, 24
const allAng4_fact_mode27_28,   db 30,  2, 30,  2, 30,  2, 30,  2, 28,  4, 28,  4, 28,  4, 28,  4, 27,  5, 27,  5, 27,  5, 27,  5, 22, 10, 22, 10, 22, 10, 22, 10
                                db 26,  6, 26,  6, 26,  6, 26,  6, 24,  8, 24,  8, 24,  8, 24,  8, 17, 15, 17, 15, 17, 15, 17, 15, 12, 20, 12, 20, 12, 20, 12, 20
const allAng4_fact_mode29_30,   db 23,  9, 23,  9, 23,  9, 23,  9, 14, 18, 14, 18, 14, 18, 14, 18, 19, 13, 19, 13, 19, 13, 19, 13,  6, 26,  6, 26,  6, 26,  6, 26
                                db  5, 27,  5, 27,  5, 27,  5, 27, 28,  4, 28,  4, 28,  4, 28,  4, 25,  7, 25,  7, 25,  7, 25,  7, 12, 20, 12, 20, 12, 20, 12, 20
const allAng4_fact_mode31_32,   db 15, 17, 15, 17, 15, 17, 15, 17, 30,  2, 30,  2, 30,  2, 30,  2, 11, 21, 11, 21, 11, 21, 11, 21, 22, 10, 22, 10, 22, 10, 22, 10
                                db 13, 19, 13, 19, 13, 19, 13, 19, 28,  4, 28,  4, 28,  4, 28,  4,  1, 31,  1, 31,  1, 31,  1, 31, 12, 20, 12, 20, 12, 20, 12, 20
const allAng4_fact_mode33,      db  6, 26,  6, 26,  6, 26,  6, 26, 12, 20, 12, 20, 12, 20, 12, 20, 18, 14, 18, 14, 18, 14, 18, 14, 24,  8, 24,  8, 24,  8, 24,  8


SECTION .text

; global constant
cextern pw_1024

; common constant with intrapred8.asm
cextern ang_table
cextern pw_ang_table
cextern tab_S1
cextern tab_S2
cextern tab_Si
cextern pw_16
cextern pb_000000000000000F
cextern pb_0000000000000F0F
cextern pw_FFFFFFFFFFFFFFF0


;-----------------------------------------------------------------------------
; void all_angs_pred_4x4(pixel *dest, pixel *refPix, pixel *filtPix, int bLuma)
;-----------------------------------------------------------------------------
INIT_XMM sse4
cglobal all_angs_pred_4x4, 4, 4, 8

; mode 2

movh      m0,         [r1 + 10]
movd      [r0],       m0

palignr   m1,         m0,      1
movd      [r0 + 4],   m1

palignr   m1,         m0,      2
movd      [r0 + 8],   m1

palignr   m1,         m0,      3
movd      [r0 + 12],  m1

; mode 3

mova          m2,        [pw_1024]

pslldq        m1,        m0,         1
pinsrb        m1,        [r1 + 9],   0
punpcklbw     m1,        m0

lea           r3,        [ang_table]

pmaddubsw     m6,        m1,        [r3 + 26 * 16]
pmulhrsw      m6,        m2
packuswb      m6,        m6
movd          [r0 + 16], m6

palignr       m0,        m1,        2

mova          m7,        [r3 + 20 * 16]

pmaddubsw     m3,        m0,        m7
pmulhrsw      m3,        m2
packuswb      m3,        m3
movd          [r0 + 20], m3

; mode 6 [row 3]
movd          [r0 + 76], m3

palignr       m3,        m1,       4

pmaddubsw     m4,        m3,        [r3 + 14 * 16]
pmulhrsw      m4,        m2
packuswb      m4,        m4
movd          [r0 + 24], m4

palignr       m4,        m1,        6

pmaddubsw     m4,        [r3 + 8 * 16]
pmulhrsw      m4,        m2
packuswb      m4,        m4
movd          [r0 + 28], m4

; mode 4

pmaddubsw     m5,        m1,        [r3 + 21 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 32], m5

pmaddubsw     m5,        m0,        [r3 + 10 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 36], m5

pmaddubsw     m5,        m0,        [r3 + 31 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 40], m5

pmaddubsw     m4,        m3,        m7
pmulhrsw      m4,        m2
packuswb      m4,        m4
movd          [r0 + 44], m4

; mode 5

pmaddubsw     m5,        m1,        [r3 + 17 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 48], m5

pmaddubsw     m5,        m0,        [r3 + 2 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 52], m5

pmaddubsw     m5,        m0,        [r3 + 19 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 56], m5

pmaddubsw     m4,        m3,        [r3 + 4 * 16]
pmulhrsw      m4,        m2
packuswb      m4,        m4
movd          [r0 + 60], m4

; mode 6

pmaddubsw     m5,        m1,        [r3 + 13 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 64], m5

movd          [r0 + 68], m6

pmaddubsw     m5,        m0,        [r3 + 7 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 72], m5

; mode 7

pmaddubsw     m5,        m1,        [r3 + 9 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 80], m5

pmaddubsw     m5,        m1,        [r3 + 18 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 84], m5

pmaddubsw     m5,        m1,        [r3 + 27 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 88], m5

pmaddubsw     m5,        m0,        [r3 + 4 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 92], m5

; mode 8

pmaddubsw     m5,        m1,        [r3 + 5 * 16]
pmulhrsw      m5,        m2
packuswb      m5,        m5
movd          [r0 + 96], m5

pmaddubsw     m5,         m1,       [r3 + 10 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 100], m5

pmaddubsw     m5,         m1,        [r3 + 15 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 104], m5

pmaddubsw     m5,         m1,        [r3 + 20 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 108], m5

; mode 9

pmaddubsw     m5,         m1,        [r3 + 2 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 112], m5

pmaddubsw     m5,         m1,        [r3 + 4 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 116], m5

pmaddubsw     m5,         m1,        [r3 + 6 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 120], m5

pmaddubsw     m5,         m1,        [r3 + 8 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 124], m5

; mode 10

movd         m3,         [r1 + 9]
pshufd       m4,         m3,        0
movu         [r0 + 128], m4

pxor         m5,         m5
movd         m7,         [r1 + 1]
pshufd       m4,         m7,        0
punpcklbw    m4,         m5

pinsrb       m7,         [r1],      0
pshufb       m6,         m7,        m5
punpcklbw    m6,         m5

psubw        m4,         m6
psraw        m4,         1

pshufb       m6,         m3,       m5
punpcklbw    m6,         m5

paddw        m4,         m6
packuswb     m4,         m5

pextrb       [r0 + 128],  m4,    0
pextrb       [r0 + 132],  m4,    1
pextrb       [r0 + 136],  m4,    2
pextrb       [r0 + 140],  m4,    3

; mode 11

pslldq        m1,        m1,         2
pinsrb        m1,        [r1],       0
pinsrb        m1,        [r1 + 9],   1

pmaddubsw     m3,         m1,        [r3 + 30 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 144], m3

pmaddubsw     m3,         m1,        [r3 + 28 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 148], m3

pmaddubsw     m3,         m1,        [r3 + 26 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 152], m3

pmaddubsw     m3,         m1,        [r3 + 24 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 156], m3

; mode 12

pmaddubsw     m3,         m1,        [r3 + 27 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 160], m3

pmaddubsw     m3,         m1,        [r3 + 22 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 164], m3

pmaddubsw     m3,         m1,        [r3 + 17 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 168], m3

pmaddubsw     m3,         m1,        [r3 + 12 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 172], m3

; mode 13

pmaddubsw     m3,         m1,        [r3 + 23 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 176], m3

pmaddubsw     m3,         m1,        [r3 + 14 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 180], m3

pmaddubsw     m3,         m1,        [r3 + 5 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 184], m3

pslldq        m5,         m1,        2
pinsrb        m5,         [r1 + 0],  1
pinsrb        m5,         [r1 + 4],  0

pmaddubsw     m4,         m5,        [r3 + 28 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 188], m4

; mode 14

pmaddubsw     m4,         m1,        [r3 + 19 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 192], m4

pmaddubsw     m7,         m1,        [r3 + 6 * 16]
pmulhrsw      m7,         m2
packuswb      m7,         m7
movd          [r0 + 196], m7

pinsrb        m5,         [r1 + 2],  0

pmaddubsw     m4,         m5,        [r3 + 25 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 200], m4

pmaddubsw     m4,         m5,        [r3 + 12 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 204], m4

; mode 15

pmaddubsw     m4,         m1,        [r3 + 15 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 208], m4

pmaddubsw     m4,         m5,        [r3 + 30 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 212], m4

pmaddubsw     m4,         m5,        [r3 + 13 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 216], m4

pslldq        m4,         m5,         2
pinsrb        m4,         [r1 + 2],   1
pinsrb        m4,         [r1 + 4],   0

pmaddubsw     m6,         m4,         [r3 + 28 * 16]
pmulhrsw      m6,         m2
packuswb      m6,         m6
movd          [r0 + 220], m6

; mode 16

pmaddubsw     m6,         m1,        [r3 + 11 * 16]
pmulhrsw      m6,         m2
packuswb      m6,         m6
movd          [r0 + 224], m6

pmaddubsw     m6,         m5,        [r3 + 22 * 16]
pmulhrsw      m6,         m2
packuswb      m6,         m6
movd          [r0 + 228], m6

pmaddubsw     m6,         m5,        [r3 + 1 * 16]
pmulhrsw      m6,         m2
packuswb      m6,         m6
movd          [r0 + 232], m6

pinsrb        m4,         [r1 + 3],  0

pmaddubsw     m4,         [r3 + 12 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 236], m4

; mode 17

movd          [r0 + 240],  m7

pslldq        m1,         2
pinsrb        m1,         [r1 + 1],  0
pinsrb        m1,         [r1 + 0],  1

pmaddubsw     m3,         m1,        [r3 + 12 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 244], m3

pslldq        m1,         2
pinsrb        m1,         [r1 + 1],  1
pinsrb        m1,         [r1 + 2],  0

pmaddubsw     m3,         m1,        [r3 + 18 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 248], m3

pslldq        m1,         2
pinsrb        m1,         [r1 + 2],  1
pinsrb        m1,         [r1 + 4],  0

pmaddubsw     m1,         [r3 + 24 * 16]
pmulhrsw      m1,         m2
packuswb      m1,         m1
movd          [r0 + 252], m1

; mode 18

movh          m1,         [r1]
movd          [r0 + 256], m1

pslldq        m3,         m1,         1
pinsrb        m3,         [r1 + 9],   0
movd          [r0 + 260], m3

pslldq        m4,         m3,         1
pinsrb        m4,         [r1 + 10],  0
movd          [r0 + 264], m4

pslldq        m4,         1
pinsrb        m4,         [r1 + 11],  0
movd          [r0 + 268], m4

; mode 19

palignr       m3,         m1,        1
punpcklbw     m1,         m3

pmaddubsw     m7,         m1,        [r3 + 6 * 16]
pmulhrsw      m7,         m2
packuswb      m7,         m7
movd          [r0 + 272], m7

pslldq        m3,         m1,         2
pinsrb        m3,         [r1],       1
pinsrb        m3,         [r1 + 9],   0

pmaddubsw     m4,         m3,         [r3 + 12 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 276], m4

pslldq        m4,         m3,         2
pinsrb        m4,         [r1 + 9],   1
pinsrb        m4,         [r1 + 10],  0

pmaddubsw     m5,         m4,         [r3 + 18 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 280], m5

pslldq        m4,         2
pinsrb        m4,         [r1 + 10],  1
pinsrb        m4,         [r1 + 12],  0

pmaddubsw     m4,         [r3 + 24 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 284], m4

; mode 20

pmaddubsw     m4,         m1,        [r3 + 11 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 288], m4

pinsrb        m3,         [r1 + 10],  0

pmaddubsw     m4,         m3,        [r3 + 22 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 292], m4

pmaddubsw     m4,         m3,        [r3 + 1 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 296], m4

pslldq        m6,         m3,        2
pinsrb        m6,         [r1 + 10], 1
pinsrb        m6,         [r1 + 11], 0

pmaddubsw     m5,         m6,        [r3 + 12 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 300], m5

; mode 21

pmaddubsw     m4,         m1,        [r3 + 15 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 304], m4

pmaddubsw     m4,         m3,        [r3 + 30 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 308], m4

pmaddubsw     m4,         m3,        [r3 + 13 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 312], m4

pinsrb        m6,         [r1 + 12],   0

pmaddubsw     m6,         [r3 + 28 * 16]
pmulhrsw      m6,         m2
packuswb      m6,         m6
movd          [r0 + 316], m6

; mode 22

pmaddubsw     m4,         m1,         [r3 + 19 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 320], m4

movd          [r0 + 324], m7

pmaddubsw     m4,         m3,        [r3 + 25 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 328], m4

pmaddubsw     m4,         m3,         [r3 + 12 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 332], m4

; mode 23

pmaddubsw     m4,         m1,         [r3 + 23 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 336], m4

pmaddubsw     m4,         m1,         [r3 + 14 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 340], m4

pmaddubsw     m4,         m1,         [r3 + 5 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 344], m4

pinsrb         m3,        [r1 + 12],   0

pmaddubsw     m3,         [r3 + 28 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 348], m3

; mode 24

pmaddubsw     m3,         m1,         [r3 + 27 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 352], m3

pmaddubsw     m3,         m1,         [r3 + 22 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 356], m3

pmaddubsw     m3,         m1,         [r3 + 17 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 360], m3

pmaddubsw     m3,         m1,         [r3 + 12 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 364], m3

; mode 25

pmaddubsw     m3,         m1,         [r3 + 30 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 368], m3

pmaddubsw     m3,         m1,         [r3 + 28 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 372], m3

pmaddubsw     m3,         m1,         [r3 + 26 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 376], m3

pmaddubsw     m1,         [r3 + 24 * 16]
pmulhrsw      m1,         m2
packuswb      m1,         m1
movd          [r0 + 380], m1

; mode 26

movh         m1,         [r1 + 1]
pshufd       m3,         m1,        0
movu         [r0 + 384], m3

pxor         m4,         m4
movd         m5,         [r1 + 9]
pshufd       m5,         m5,        0
punpcklbw    m5,         m4

pinsrb       m6,         [r1],      0
pshufb       m6,         m4
punpcklbw    m6,         m4

psubw        m5,         m6
psraw        m5,         1

pshufb       m6,         m1,        m4
punpcklbw    m6,         m4

paddw        m5,         m6
packuswb     m5,         m4

pextrb       [r0 + 384], m5,    0
pextrb       [r0 + 388], m5,    1
pextrb       [r0 + 392], m5,    2
pextrb       [r0 + 396], m5,    3

; mode 27

palignr       m3,         m1,     1
punpcklbw     m1,         m3

pmaddubsw     m3,         m1,     [r3 + 2 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 400], m3

pmaddubsw     m3,         m1,     [r3 + 4 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 404], m3

pmaddubsw     m3,         m1,     [r3 + 6 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 408], m3

pmaddubsw     m3,         m1,     [r3 + 8 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 412], m3

; mode 28

pmaddubsw     m3,         m1,     [r3 + 5 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 416], m3

pmaddubsw     m3,         m1,     [r3 + 10 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 420], m3

pmaddubsw     m3,         m1,     [r3 + 15 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 424], m3

pmaddubsw     m3,         m1,     [r3 + 20 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 428], m3

; mode 29

pmaddubsw     m3,         m1,     [r3 + 9 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 432], m3

pmaddubsw     m3,         m1,     [r3 + 18 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 436], m3

pmaddubsw     m3,         m1,     [r3 + 27 * 16]
pmulhrsw      m3,         m2
packuswb      m3,         m3
movd          [r0 + 440], m3

palignr       m3,         m1,     2

pmaddubsw     m4,         m3,     [r3 + 4 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 444], m4

; mode 30

pmaddubsw     m4,         m1,     [r3 + 13 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 448], m4

pmaddubsw     m7,         m1,     [r3 + 26 * 16]
pmulhrsw      m7,         m2
packuswb      m7,         m7
movd          [r0 + 452], m7

pmaddubsw     m5,         m3,     [r3 + 7 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 456], m5

pmaddubsw     m6,         m3,     [r3 + 20 * 16]
pmulhrsw      m6,         m2
packuswb      m6,         m6
movd          [r0 + 460], m6

; mode 31

pmaddubsw     m4,         m1,     [r3 + 17 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 464], m4

pmaddubsw     m5,         m3,     [r3 + 2 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 468], m5

pmaddubsw     m5,         m3,     [r3 + 19 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 472], m5

palignr       m4,         m3,     2

pmaddubsw     m5,         m4,     [r3 + 4 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 476], m5

; mode 32

pmaddubsw     m5,         m1,     [r3 + 21 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 480], m5

pmaddubsw     m5,         m3,     [r3 + 10 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 484], m5

pmaddubsw     m5,         m3,     [r3 + 31 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 488], m5

pmaddubsw     m5,         m4,     [r3 + 20 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 492], m5

; mode 33

movd          [r0 + 496], m7

movd          [r0 + 500], m6

pmaddubsw     m5,         m4,         [r3 + 14 * 16]
pmulhrsw      m5,         m2
packuswb      m5,         m5
movd          [r0 + 504], m5

psrldq        m4,         2

pmaddubsw     m4,         [r3 + 8 * 16]
pmulhrsw      m4,         m2
packuswb      m4,         m4
movd          [r0 + 508], m4

; mode 34

movh      m7,             [r1 + 2]
movd      [r0 + 512],     m7

psrldq    m7,      1
movd      [r0 + 516],     m7

psrldq    m7,      1
movd      [r0 + 520],     m7

psrldq    m7,      1
movd      [r0 + 524],     m7

RET

;------------------------------------------------------------------------------
; void all_angs_pred_8x8(pixel *dest, pixel *refPix, pixel *filtPix, int bLuma)
;------------------------------------------------------------------------------
INIT_XMM sse4
cglobal all_angs_pred_8x8, 3,4,8
    ; mode 2

    movu         m0,          [r2 + 18]
    palignr      m1,          m0,          1
    punpcklqdq   m2,          m0,          m1
    movu         [r0],        m2

    palignr      m1,          m0,          2
    palignr      m2,          m0,          3
    punpcklqdq   m1,          m2
    movu         [r0 + 16],   m1

    palignr      m1,          m0,          4
    palignr      m2,          m0,          5
    punpcklqdq   m1,          m2
    movu         [r0 + 32],   m1

    palignr      m1,          m0,          6
    palignr      m2,          m0,          7
    punpcklqdq   m1,          m2
    movu         [r0 + 48],   m1

    ; mode 3 [row 0, 1]

    mova          m7,         [pw_1024]
    lea           r3,         [ang_table]

    movu          m0,         [r1 + 17]

    palignr       m1,         m0,               1
    palignr       m2,         m0,               2

    punpcklbw     m3,         m0,               m1
    pmaddubsw     m4,         m3,               [r3 + 26 * 16]
    pmulhrsw      m4,         m7

    punpcklbw     m1,         m2
    pmaddubsw     m5,         m1,               [r3 + 20 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5

    movu          [r0 + 64],  m4

    ; mode 6 [row 1]

    movh          [r0 + 264], m4

    ; mode 6 [row 3]

    movhps        [r0 + 280], m4

    ; mode 4 [row 0, 1]

    pmaddubsw     m4,         m3,               [r3 + 21 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m1,               [r3 + 10 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 128], m4

    ; mode 5 [row 0, 1]

    pmaddubsw     m4,         m3,               [r3 + 17 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m1,               [r3 + 2 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 192], m4

    ; mode 6 [row 0]

    pmaddubsw     m4,         m3,               [r3 + 13 * 16]
    pmulhrsw      m4,         m7

    pxor          m5,         m5

    packuswb      m4,         m5
    movh          [r0 + 256], m4

    ; mode 7 [row 0, 1]

    pmaddubsw     m4,         m3,               [r3 + 9 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 18 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 320], m4

    ; mode 8 [row 0, 1]

    pmaddubsw     m4,         m3,               [r3 + 5 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 10 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 384], m4

    ; mode 8 [row 2, 3]

    pmaddubsw     m4,         m3,               [r3 + 15 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 20 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 400], m4

    ; mode 8 [row 4, 5]

    pmaddubsw     m4,         m3,               [r3 + 25 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 30 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 416], m4

    ; mode 8 [row 6, 7]

    pmaddubsw     m4,         m1,               [r3 + 3 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m1,               [r3 + 8 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 432], m4

    ; mode 9 [row 0, 1]

    pmaddubsw     m4,         m3,               [r3 + 2 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 4 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 448], m4

    ; mode 9 [row 2, 3]

    pmaddubsw     m4,         m3,               [r3 + 6 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 8 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 464], m4

    ; mode 9 [row 4, 5]

    pmaddubsw     m4,         m3,               [r3 + 10 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 12 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 480], m4

    ; mode 9 [row 6, 7]

    pmaddubsw     m4,         m3,               [r3 + 14 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 16 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 496], m4

    ; mode 7 [row 2, 3]

    pmaddubsw     m4,         m3,               [r3 + 27 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m1,               [r3 + 4 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 336], m4

    ; mode 7 [row 4, 5]

    pmaddubsw     m4,         m1,               [r3 + 13 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m1,               [r3 + 22 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 352], m4

    ; mode 6 [row 2]

    pmaddubsw     m4,         m1,               [r3 + 7 * 16]
    pmulhrsw      m4,         m7

    pxor           m5,         m5

    packuswb      m4,         m5
    movh          [r0 + 272], m4

    ; mode 3 [row 2, 3]

    palignr       m1,         m0,               3
    palignr       m3,         m0,               4

    punpcklbw     m2,         m1
    pmaddubsw     m5,         m2,               [r3 + 14 * 16]
    pmulhrsw      m5,         m7

    punpcklbw     m1,         m3
    pmaddubsw     m6,         m1,               [r3 + 8 * 16]
    pmulhrsw      m6,         m7

    packuswb      m5,         m6
    movu          [r0 + 80],  m5

    ; mode 6 [row 7]

    movhps        [r0 + 312], m5

    ; mode 6 [row 5]

    movh          [r0 + 296], m5

    ; mode 4 [calculate and store row 4, 5]

    pmaddubsw     m4,         m1,               [r3 + 9 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m1,               [r3 + 30 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 160], m4

    ; mode 5 [row 4, 5]

    pmaddubsw     m4,         m2,               [r3 + 21 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m1,               [r3 + 6 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 224], m4

    ; mode 6 [row 4, 5]

    pmaddubsw     m5,         m2,               [r3 + 1 * 16]
    pmulhrsw      m5,         m7

    pxor           m6,        m6

    packuswb      m5,         m6
    movh          [r0 + 288], m5

    ; mode 6 [row 6, 7]

    pmaddubsw     m5,         m2,               [r3 + 27 * 16]
    pmulhrsw      m5,         m7

    pxor          m6,         m6

    packuswb      m5,         m6
    movh          [r0 + 304], m5

    ; mode 5 [calculate row 6]

    pmaddubsw     m6,         m1,               [r3 + 23 * 16]
    pmulhrsw      m6,         m7

    ; mode 3 [row 4, 5]

    palignr       m1,         m0,               5

    punpcklbw     m3,         m1
    pmaddubsw     m4,         m3,               [r3 + 2 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m3,               [r3 + 28 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 96],  m4

    ; mode 4 [calculate row 7]

    pmaddubsw     m5,         m3,               [r3 + 19 * 16]
    pmulhrsw      m5,         m7

    ; mode 5 [calculate row 6]

    pmaddubsw     m4,         m3,               [r3 + 8 * 16]
    pmulhrsw      m4,         m7

    packuswb      m6,         m4
    movu          [r0 + 240], m6

    ; mode 3 [row 6, 7]

    palignr       m2,         m0,               6
    palignr       m3,         m0,               7

    punpcklbw     m1,         m2
    pmaddubsw     m4,         m1,               [r3 + 22 * 16]
    pmulhrsw      m4,         m7

    punpcklbw     m2,         m3
    pmaddubsw     m2,         [r3 + 16 * 16]
    pmulhrsw      m2,         m7

    packuswb      m4,         m2
    movu          [r0 + 112], m4

    ; mode 4 [calculate row 7]

    pmaddubsw     m2,         m1,               [r3 + 8 * 16]
    pmulhrsw      m2,         m7

    ; mode 4 [store row 6 and 7]

    packuswb      m5,         m2
    movu          [r0 + 176], m5

    ; mode 4 [row 2, 3]

    palignr       m1,         m0,               1
    palignr       m2,         m0,               2
    palignr       m3,         m0,               3

    punpcklbw     m1,         m2
    pmaddubsw     m4,         m1,               [r3 + 31 * 16]
    pmulhrsw      m4,         m7

    punpcklbw     m2,         m3
    pmaddubsw     m5,         m2,               [r3 + 20 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 144], m4

    ; mode 5 [row 2, 3]

    pmaddubsw     m4,         m1,               [r3 + 19 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m2,               [r3 + 4 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 208], m4

    ; mode 7 [row 6, 7]

    pmaddubsw     m4,         m1,               [r3 + 31 * 16]
    pmulhrsw      m4,         m7

    pmaddubsw     m5,         m2,               [r3 + 8 * 16]
    pmulhrsw      m5,         m7

    packuswb      m4,         m5
    movu          [r0 + 368], m4

    ; mode 10

    pshufb       m1,          m0,          [tab_Si]
    movu         [r0 + 512],  m1
    movu         [r0 + 528],  m1
    movu         [r0 + 544],  m1
    movu         [r0 + 560],  m1

    pxor         m0,          m0

    pshufb       m1,          m1,          m0
    punpcklbw    m1,          m0

    movu         m2,          [r1]

    pshufb       m3,          m2,          m0
    punpcklbw    m3,          m0

    psrldq       m4,          m2,          1
    punpcklbw    m4,          m0

    movu         m2,          [r1 + 9]
    punpcklbw    m2,          m0

    psubw        m4,          m3
    psubw        m2,          m3

    psraw        m4,          1
    psraw        m2,          1

    paddw        m4,          m1
    paddw        m2,          m1

    packuswb     m4,          m2

    pextrb       [r0 + 512],  m4,          0
    pextrb       [r0 + 520],  m4,          1
    pextrb       [r0 + 528],  m4,          2
    pextrb       [r0 + 536],  m4,          3
    pextrb       [r0 + 544],  m4,          4
    pextrb       [r0 + 552],  m4,          5
    pextrb       [r0 + 560],  m4,          6
    pextrb       [r0 + 568],  m4,          7

    ; mode 11 [row 0, 1]

    movu         m0,         [r1 + 16]
    pinsrb       m0,         [r1], 0
    palignr      m1,         m0,          1
    punpcklbw    m2,         m0,          m1

    pmaddubsw    m3,         m2,          [r3 + 30 * 16]
    pmulhrsw     m3,         m7

    pmaddubsw    m4,         m2,          [r3 + 28 * 16]
    pmulhrsw     m4,         m7

    packuswb     m3,         m4
    movu         [r0 + 576], m3

    ; mode 11 [row 2, 3]

    pmaddubsw    m3,         m2,          [r3 + 26 * 16]
    pmulhrsw     m3,         m7

    pmaddubsw    m4,         m2,          [r3 + 24 * 16]
    pmulhrsw     m4,         m7

    packuswb     m3,         m4
    movu         [r0 + 592], m3

    ; mode 11 [row 4, 5]

    pmaddubsw    m3,         m2,          [r3 + 22 * 16]
    pmulhrsw     m3,         m7

    pmaddubsw    m4,         m2,          [r3 + 20 * 16]
    pmulhrsw     m4,         m7

    packuswb     m5,         m3,         m4
    movu         [r0 + 608], m5

    ; mode 12 [row 0, 1]

    pmaddubsw    m4,         m2,          [r3 + 27 * 16]
    pmulhrsw     m4,         m7

    packuswb     m4,         m3
    movu         [r0 + 640], m4

    ; mode 11 [row 6, 7]

    pmaddubsw    m3,         m2,          [r3 + 18 * 16]
    pmulhrsw     m3,         m7

    pmaddubsw    m4,         m2,          [r3 + 16 * 16]
    pmulhrsw     m4,         m7

    packuswb     m3,         m4
    movu         [r0 + 624], m3

    ; mode 12 [row 2, 3]

    pmaddubsw    m3,         m2,          [r3 + 17 * 16]
    pmulhrsw     m3,         m7

    pmaddubsw    m4,         m2,          [r3 + 12 * 16]
    pmulhrsw     m4,         m7

    packuswb     m3,         m4
    movu         [r0 + 656], m3

    ; mode 12 [row 4, 5]

    pmaddubsw    m3,         m2,          [r3 + 7 * 16]
    pmulhrsw     m3,         m7

    pmaddubsw    m4,         m2,          [r3 + 2 * 16]
    pmulhrsw     m4,         m7

    packuswb     m3,         m4
    movu         [r0 + 672], m3

    ; mode 12 [row 6, 7]

    pslldq       m3,         m2,          2
    pinsrb       m3,         [r1 + 0],    1
    pinsrb       m3,         [r1 + 6],    0

    pmaddubsw    m4,         m3,          [r3 + 29 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m3,          [r3 + 24 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 688], m4

    ; mode 13 [row 0, 1]

    pmaddubsw    m4,         m2,          [r3 + 23 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m2,          [r3 + 14 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 704], m4

    ; mode 13 [row 2, 3]

    pmaddubsw    m4,         m2,          [r3 + 5 * 16]
    pmulhrsw     m4,         m7

    pinsrb       m3,         [r1 + 4],    0
    pmaddubsw    m5,         m3,          [r3 + 28 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 720], m4

    ; mode 13 [row 4, 5]

    pmaddubsw    m4,         m3,          [r3 + 19 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m3,          [r3 + 10 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 736], m4

    ; mode 13 [row 6, 7]

    pmaddubsw    m4,         m3,          [r3 + 1 * 16]
    pmulhrsw     m4,         m7

    pslldq       m5,         m3,          2
    pinsrb       m5,         [r1 + 4],    1
    pinsrb       m5,         [r1 + 7],    0

    pmaddubsw    m5,         [r3 + 24 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 752], m4

    ; mode 14 [row 0, 1]

    pmaddubsw    m4,         m2,          [r3 + 19 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m2,          [r3 + 6 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 768], m4

    ; mode 14 [row 2, 3]

    pinsrb       m3,         [r1 + 2],    0

    pmaddubsw    m4,         m3,          [r3 + 25 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m3,          [r3 + 12 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 784], m4

    ; mode 14 [row 4, 5]

    pslldq       m1,         m3,          2
    pinsrb       m1,         [r1 + 2],    1
    pinsrb       m1,         [r1 + 5],    0

    pmaddubsw    m4,         m1,          [r3 + 31 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m1,          [r3 + 18 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 800], m4

    ; mode 14 [row 6, 7]

    pmaddubsw    m4,         m1,          [r3 + 5 * 16]
    pmulhrsw     m4,         m7

    pslldq       m1,         2
    pinsrb       m1,         [r1 + 5],    1
    pinsrb       m1,         [r1 + 7],    0

    pmaddubsw    m5,         m1,          [r3 + 24 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 816], m4

    ; mode 15 [row 0, 1]

    pmaddubsw    m4,         m2,          [r3 + 15 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m3,          [r3 + 30 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 832], m4

    ; mode 15 [row 2, 3]

    pmaddubsw    m4,         m3,          [r3 + 13 * 16]
    pmulhrsw     m4,         m7

    pslldq       m1,         m3,          2
    pinsrb       m1,         [r1 + 2],    1
    pinsrb       m1,         [r1 + 4],    0

    pmaddubsw    m5,         m1,          [r3 + 28 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 848], m4

    ; mode 15 [row 4, 5]

    pmaddubsw    m4,         m1,          [r3 + 11 * 16]
    pmulhrsw     m4,         m7

    pslldq       m1,         2
    pinsrb       m1,         [r1 + 4],    1
    pinsrb       m1,         [r1 + 6],    0

    pmaddubsw    m5,         m1,          [r3 + 26 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 864], m4

    ; mode 15 [row 6, 7]

    pmaddubsw    m4,         m1,          [r3 + 9 * 16]
    pmulhrsw     m4,         m7

    pslldq       m1,         2
    pinsrb       m1,         [r1 + 6],    1
    pinsrb       m1,         [r1 + 8],    0

    pmaddubsw    m1,          [r3 + 24 * 16]
    pmulhrsw     m1,         m7

    packuswb     m4,         m1
    movu         [r0 + 880], m4

    ; mode 16 [row 0, 1]

    pmaddubsw    m4,         m2,          [r3 + 11 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m3,          [r3 + 22 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 896], m4

    ; mode 16 [row 2, 3]

    pmaddubsw    m4,         m3,          [r3 + 1 * 16]
    pmulhrsw     m4,         m7

    pslldq       m3,         2
    pinsrb       m3,         [r1 + 2],    1
    pinsrb       m3,         [r1 + 3],    0

    pmaddubsw    m5,         m3,          [r3 + 12 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 912], m4

    ; mode 16 [row 4, 5]

    pslldq       m3,         2
    pinsrb       m3,         [r1 + 3],    1
    pinsrb       m3,         [r1 + 5],    0

    pmaddubsw    m4,         m3,          [r3 + 23 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m5,         m3,          [r3 + 2 * 16]
    pmulhrsw     m5,         m7

    packuswb     m4,         m5
    movu         [r0 + 928], m4

    ; mode 16 [row 6, 7]

    pslldq       m3,         2
    pinsrb       m3,         [r1 + 5],    1
    pinsrb       m3,         [r1 + 6],    0

    pmaddubsw    m4,         m3,          [r3 + 13 * 16]
    pmulhrsw     m4,         m7

    pslldq       m3,         2
    pinsrb       m3,         [r1 + 6],    1
    pinsrb       m3,         [r1 + 8],    0

    pmaddubsw    m3,         [r3 + 24 * 16]
    pmulhrsw     m3,         m7

    packuswb     m4,         m3
    movu         [r0 + 944], m4

    ; mode 17 [row 0, 1]

    pmaddubsw    m4,         m2,          [r3 + 6 * 16]
    pmulhrsw     m4,         m7

    pslldq       m2,         2
    pinsrb       m2,         [r1 + 0],    1
    pinsrb       m2,         [r1 + 1],    0

    pmaddubsw    m3,         m2,          [r3 + 12 * 16]
    pmulhrsw     m3,         m7

    packuswb     m4,         m3
    movu         [r0 + 960], m4

    ; mode 17 [row 2, 3]

    pslldq       m2,         2
    pinsrb       m2,         [r1 + 1],    1
    pinsrb       m2,         [r1 + 2],    0

    pmaddubsw    m4,         m2,          [r3 + 18 * 16]
    pmulhrsw     m4,         m7

    pslldq       m2,         2
    pinsrb       m2,         [r1 + 2],    1
    pinsrb       m2,         [r1 + 4],    0

    pmaddubsw    m3,         m2,          [r3 + 24 * 16]
    pmulhrsw     m3,         m7

    packuswb     m4,         m3
    movu         [r0 + 976], m4

    ; mode 17 [row 4, 5]

    pslldq       m2,         2
    pinsrb       m2,         [r1 + 4],    1
    pinsrb       m2,         [r1 + 5],    0

    pmaddubsw    m4,         m2,          [r3 + 30 * 16]
    pmulhrsw     m4,         m7

    pmaddubsw    m3,         m2,          [r3 + 4 * 16]
    pmulhrsw     m3,         m7

    packuswb     m4,         m3
    movu         [r0 + 992], m4

    ; mode 17 [row 6, 7]

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 5],    1
    pinsrb       m2,          [r1 + 6],    0

    pmaddubsw    m4,          m2,          [r3 + 10 * 16]
    pmulhrsw     m4,          m7

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 6],    1
    pinsrb       m2,          [r1 + 7],    0

    pmaddubsw    m3,          m2,          [r3 + 16 * 16]
    pmulhrsw     m3,          m7

    packuswb     m4,          m3
    movu         [r0 + 1008], m4

    ; mode 18 [row 0, 1, 2, 3, 4, 5, 6, 7]

    movh          m1,          [r2]

    pslldq        m2,          m1,         1
    pinsrb        m2,          [r2 + 1 + 16],   0
    punpcklqdq    m1,          m2
    movu          [r0 + 1024], m1

    pslldq        m2,          1
    pinsrb        m2,          [r2 + 2 + 16],   0

    pslldq        m0,          m2,          1
    pinsrb        m0,          [r2 + 3 + 16],   0
    punpcklqdq    m2,          m0
    movu          [r0 + 1040], m2

    pslldq        m0,          1
    pinsrb        m0,          [r2 + 4 + 16],   0

    pslldq        m2,          m0,              1
    pinsrb        m2,          [r2 + 5 + 16],   0
    punpcklqdq    m0,          m2
    movu          [r0 + 1056], m0

    pslldq        m2,          1
    pinsrb        m2,          [r2 + 6 + 16],   0

    pslldq        m0,           m2,             1
    pinsrb        m0,          [r2 + 7 + 16],   0
    punpcklqdq    m2,          m0
    movu          [r0 + 1072], m2

    ; mode 19 [row 0, 1]

    movu         m0,          [r1]
    palignr      m1,          m0,          1
    punpcklbw    m0,          m1

    pmaddubsw    m1,          m0,          [r3 + 6 * 16]
    pmulhrsw     m1,          m7

    pslldq       m2,          m0,          2
    pinsrb       m2,          [r1],        1
    pinsrb       m2,          [r1 + 1 + 16], 0

    pmaddubsw    m3,          m2,          [r3 + 12 * 16]
    pmulhrsw     m3,          m7

    packuswb     m1,          m3
    movu         [r0 + 1088], m1

    ; mode 19 [row 2, 3]

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 1 + 16], 1
    pinsrb       m2,          [r1 + 2 + 16], 0

    pmaddubsw    m4,          m2,          [r3 + 18 * 16]
    pmulhrsw     m4,          m7

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 2 + 16],    1
    pinsrb       m2,          [r1 + 4 + 16],    0

    pmaddubsw    m5,          m2,          [r3 + 24 * 16]
    pmulhrsw     m5,          m7

    packuswb     m4,          m5
    movu         [r0 + 1104], m4

    ; mode 19 [row 4, 5]

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 4 + 16], 1
    pinsrb       m2,          [r1 + 5 + 16], 0

    pmaddubsw    m4,          m2,          [r3 + 30 * 16]
    pmulhrsw     m4,          m7

    pmaddubsw    m5,          m2,          [r3 + 4 * 16]
    pmulhrsw     m5,          m7

    packuswb     m4,          m5
    movu         [r0 + 1120], m4

    ; mode 19 [row 6, 7]

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 5 + 16], 1
    pinsrb       m2,          [r1 + 6 + 16], 0

    pmaddubsw    m4,          m2,          [r3 + 10 * 16]
    pmulhrsw     m4,          m7

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 6 + 16], 1
    pinsrb       m2,          [r1 + 7 + 16], 0

    pmaddubsw    m2,          [r3 + 16 * 16]
    pmulhrsw     m2,          m7

    packuswb     m4,          m2
    movu         [r0 + 1136], m4

    ; mode 20 [row 0, 1]

    pmaddubsw    m3,          m0,          [r3 + 11 * 16]
    pmulhrsw     m3,          m7

    pslldq       m1,          m0,          2
    pinsrb       m1,          [r1 + 0],    1
    pinsrb       m1,          [r1 + 2 + 16], 0

    pmaddubsw    m4,          m1,          [r3 + 22 * 16]
    pmulhrsw     m4,          m7

    packuswb     m3,          m4
    movu         [r0 + 1152], m3

    ; mode 20 [row 2, 3]

    pmaddubsw    m3,          m1,          [r3 + 1 * 16]
    pmulhrsw     m3,          m7

    pslldq       m2,          m1,          2
    pinsrb       m2,          [r1 + 2 + 16], 1
    pinsrb       m2,          [r1 + 3 + 16], 0

    pmaddubsw    m4,          m2,          [r3 + 12 * 16]
    pmulhrsw     m4,          m7

    packuswb     m3,          m4
    movu         [r0 + 1168], m3

    ; mode 20 [row 4, 5]

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 3 + 16], 1
    pinsrb       m2,          [r1 + 5 + 16], 0

    pmaddubsw    m3,          m2,          [r3 + 23 * 16]
    pmulhrsw     m3,          m7

    pmaddubsw    m4,          m2,          [r3 + 2 * 16]
    pmulhrsw     m4,          m7

    packuswb     m3,          m4
    movu         [r0 + 1184], m3

    ; mode 20 [row 6, 7]

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 5 + 16], 1
    pinsrb       m2,          [r1 + 6 + 16], 0

    pmaddubsw    m3,          m2,          [r3 + 13 * 16]
    pmulhrsw     m3,          m7

    pslldq       m2,          2
    pinsrb       m2,          [r1 + 6 + 16], 1
    pinsrb       m2,          [r1 + 8 + 16], 0

    pmaddubsw    m4,          m2,          [r3 + 24 * 16]
    pmulhrsw     m4,          m7

    packuswb     m3,          m4
    movu         [r0 + 1200], m3

    ; mode 21 [row 0, 1]

    pmaddubsw    m2,          m0,          [r3 + 15 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m1,          [r3 + 30 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1216], m2

    ; mode 21 [row 2, 3]

    pmaddubsw    m2,          m1,          [r3 + 13 * 16]
    pmulhrsw     m2,          m7

    pslldq       m3,          m1,          2
    pinsrb       m3,          [r1 + 2 + 16], 1
    pinsrb       m3,          [r1 + 4 + 16], 0

    pmaddubsw    m4,          m3,          [r3 + 28 * 16]
    pmulhrsw     m4,          m7

    packuswb     m2,          m4
    movu         [r0 + 1232], m2

    ; mode 21 [row 4, 5]

    pmaddubsw    m2,          m3,          [r3 + 11 * 16]
    pmulhrsw     m2,          m7

    pslldq       m3,          2
    pinsrb       m3,          [r1 + 4 + 16], 1
    pinsrb       m3,          [r1 + 6 + 16], 0

    pmaddubsw    m4,          m3,          [r3 + 26 * 16]
    pmulhrsw     m4,          m7

    packuswb     m2,          m4
    movu         [r0 + 1248], m2

    ; mode 21 [row 6, 7]

    pmaddubsw    m2,          m3,          [r3 + 9 * 16]
    pmulhrsw     m2,          m7

    pslldq       m3,          2
    pinsrb       m3,          [r1 + 6 + 16], 1
    pinsrb       m3,          [r1 + 8 + 16], 0

    pmaddubsw    m4,          m3,          [r3 + 24 * 16]
    pmulhrsw     m4,          m7

    packuswb     m2,          m4
    movu         [r0 + 1264], m2

    ; mode 22 [row 0, 1]

    pmaddubsw    m2,          m0,          [r3 + 19 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m4,          m0,          [r3 + 6 * 16]
    pmulhrsw     m4,          m7

    packuswb     m2,          m4
    movu         [r0 + 1280], m2

    ; mode 22 [row 2, 3]

    pmaddubsw    m2,          m1,          [r3 + 25 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m1,          [r3 + 12 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1296], m2

    ; mode 22 [row 4, 5]

    pslldq       m1,          2
    pinsrb       m1,          [r1 + 5 + 16], 0
    pinsrb       m1,          [r1 + 2 + 16], 1

    pmaddubsw    m2,          m1,          [r3 + 31 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m1,          [r3 + 18 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1312], m2

    ; mode 22 [row 6, 7]

    pmaddubsw    m2,          m1,          [r3 + 5 * 16]
    pmulhrsw     m2,          m7

    pslldq       m1,          2
    pinsrb       m1,          [r1 + 5 + 16], 1
    pinsrb       m1,          [r1 + 7 + 16], 0

    pmaddubsw    m1,          [r3 + 24 * 16]
    pmulhrsw     m1,          m7

    packuswb     m2,          m1
    movu         [r0 + 1328], m2

    ; mode 23 [row 0, 1]

    pmaddubsw    m2,          m0,          [r3 + 23 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m0,          [r3 + 14 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1344], m2

    ; mode 23 [row 2, 3]

    pmaddubsw    m2,          m0,          [r3 + 5 * 16]
    pmulhrsw     m2,          m7

    pslldq       m1,          m0,          2
    pinsrb       m1,          [r1], 1
    pinsrb       m1,          [r1 + 4 + 16], 0

    pmaddubsw    m3,          m1,          [r3 + 28 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1360], m2

    ; mode 23 [row 4, 5]

    pmaddubsw    m2,          m1,          [r3 + 19 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m1,          [r3 + 10 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1376], m2

    ; mode 23 [row 6, 7]

    pmaddubsw    m2,          m1,          [r3 + 1 * 16]
    pmulhrsw     m2,          m7

    pslldq       m3,          m1,          2
    pinsrb       m3,          [r1 + 4 + 16], 1
    pinsrb       m3,          [r1 + 7 + 16], 0

    pmaddubsw    m3,          [r3 + 24 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1392], m2

    ; mode 24 [row 0, 1]

    pmaddubsw    m2,          m0,          [r3 + 27 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m5,          m0,          [r3 + 22 * 16]
    pmulhrsw     m5,          m7

    packuswb     m2,          m5
    movu         [r0 + 1408], m2

    ; mode 24 [row 2, 3]

    pmaddubsw    m2,          m0,          [r3 + 17 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m0,          [r3 + 12 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1424], m2

    ; mode 24 [row 4, 5]

    pmaddubsw    m2,          m0,          [r3 + 7 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m0,          [r3 + 2 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1440], m2

    ; mode 24 [row 6, 7]

    pinsrb       m1,          [r1 + 6 + 16], 0

    pmaddubsw    m2,          m1,          [r3 + 29 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m1,          [r3 + 24 * 16]
    pmulhrsw     m1,          m7

    packuswb     m2,          m1
    movu         [r0 + 1456], m2

    ; mode 25 [row 0, 1]

    pmaddubsw    m2,          m0,          [r3 + 30 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m1,          m0,          [r3 + 28 * 16]
    pmulhrsw     m1,          m7

    packuswb     m2,          m1
    movu         [r0 + 1472], m2

    ; mode 25 [row 2, 3]

    pmaddubsw    m2,          m0,          [r3 + 26 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m1,          m0,          [r3 + 24 * 16]
    pmulhrsw     m1,          m7

    packuswb     m2,          m1
    movu         [r0 + 1488], m2

    ; mode 25 [row 4, 5]

    pmaddubsw    m1,          m0,          [r3 + 20 * 16]
    pmulhrsw     m1,          m7

    packuswb     m5,          m1
    movu         [r0 + 1504], m5

    ; mode 25 [row 6, 7]

    pmaddubsw    m2,          m0,          [r3 + 18 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m1,          m0,          [r3 + 16 * 16]
    pmulhrsw     m1,          m7

    packuswb     m2,          m1
    movu         [r0 + 1520], m2

    ; mode 26

    movu         m0,          [r1 + 1]

    pshufb       m1,          m0,          [tab_Si]
    movu         [r0 + 1536], m1
    movu         [r0 + 1552], m1
    movu         [r0 + 1568], m1
    movu         [r0 + 1584], m1

    pxor         m5,          m5

    pshufb       m1,          m1,          m5
    punpcklbw    m1,          m5

    movu         m2,          [r1 + 16]
    pinsrb       m2,          [r1], 0

    pshufb       m3,          m2,          m5
    punpcklbw    m3,          m5

    psrldq       m4,          m2,          1
    punpcklbw    m4,          m5

    movu         m2,          [r1 + 9 + 16]
    punpcklbw    m2,          m5

    psubw        m4,          m3
    psubw        m2,          m3

    psraw        m4,          1
    psraw        m2,          1

    paddw        m4,          m1
    paddw        m2,          m1

    packuswb     m4,          m2

    pextrb       [r0 + 1536], m4,          0
    pextrb       [r0 + 1544], m4,          1
    pextrb       [r0 + 1552], m4,          2
    pextrb       [r0 + 1560], m4,          3
    pextrb       [r0 + 1568], m4,          4
    pextrb       [r0 + 1576], m4,          5
    pextrb       [r0 + 1584], m4,          6
    pextrb       [r0 + 1592], m4,          7

    ; mode 27 [row 0, 1]

    palignr      m6,          m0,          1
    punpcklbw    m4,          m0,          m6

    pmaddubsw    m1,          m4,          [r3 + 2 * 16]
    pmulhrsw     m1,          m7

    pmaddubsw    m2,          m4,          [r3 + 4 * 16]
    pmulhrsw     m2,          m7

    packuswb     m1,          m2
    movu         [r0 + 1600], m1

    ; mode 27 [row 2, 3]

    pmaddubsw    m1,          m4,          [r3 + 6 * 16]
    pmulhrsw     m1,          m7

    pmaddubsw    m2,          m4,          [r3 + 8 * 16]
    pmulhrsw     m2,          m7

    packuswb     m1,          m2
    movu         [r0 + 1616], m1

    ; mode 27 [row 4, 5]

    pmaddubsw    m3,          m4,          [r3 + 10 * 16]
    pmulhrsw     m3,          m7

    pmaddubsw    m2,          m4,          [r3 + 12 * 16]
    pmulhrsw     m2,          m7

    packuswb     m1,          m3,          m2
    movu         [r0 + 1632], m1

    ; mode 27 [row 6, 7]

    pmaddubsw    m1,          m4,          [r3 + 14 * 16]
    pmulhrsw     m1,          m7

    pmaddubsw    m2,          m4,          [r3 + 16 * 16]
    pmulhrsw     m2,          m7

    packuswb     m1,          m2
    movu         [r0 + 1648], m1

    ; mode 28 [row 0, 1]

    pmaddubsw    m1,          m4,          [r3 + 5 * 16]
    pmulhrsw     m1,          m7

    packuswb     m1,          m3
    movu         [r0 + 1664], m1

    ; mode 28 [row 2, 3]

    pmaddubsw    m1,          m4,          [r3 + 15 * 16]
    pmulhrsw     m1,          m7

    pmaddubsw    m2,          m4,          [r3 + 20 * 16]
    pmulhrsw     m2,          m7

    packuswb     m1,          m2
    movu         [r0 + 1680], m1

    ; mode 28 [row 4, 5]

    pmaddubsw    m1,          m4,          [r3 + 25 * 16]
    pmulhrsw     m1,          m7

    pmaddubsw    m2,          m4,          [r3 + 30 * 16]
    pmulhrsw     m2,          m7

    packuswb     m1,          m2
    movu         [r0 + 1696], m1

    ; mode 28 [row 6, 7]

    palignr      m1,          m0,          2
    punpcklbw    m5,          m6,          m1

    pmaddubsw    m2,          m5,          [r3 + 3 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m5,          [r3 + 8 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1712], m2

    ; mode 29 [row 0, 1]

    pmaddubsw    m2,          m4,          [r3 + 9 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m4,          [r3 + 18 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1728], m2

    ; mode 29 [row 2, 3]

    pmaddubsw    m2,          m4,          [r3 + 27 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m5,          [r3 + 4 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1744], m2

    ; mode 29 [row 4, 5]

    pmaddubsw    m2,          m5,          [r3 + 13 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m5,          [r3 + 22 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1760], m2

    ; mode 29 [row 6, 7]

    pmaddubsw    m2,          m5,          [r3 + 31 * 16]
    pmulhrsw     m2,          m7

    palignr      m6,          m0,          3
    punpcklbw    m1,          m6

    pmaddubsw    m3,          m1,          [r3 + 8 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1776], m2

    ; mode 32 [row 2]

    movh         [r0 + 1936], m2

    ; mode 30 [row 0, 1]

    pmaddubsw    m2,          m4,          [r3 + 13 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m4,          [r3 + 26 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1792], m2

    ; mode 30 [row 2, 3]

    pmaddubsw    m2,          m5,          [r3 + 7 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m5,          [r3 + 20 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1808], m2

    ; mode 33 [row 1]

    movhps       [r0 + 1992], m2

    ; mode 30 [row 4, 5]

    pmaddubsw    m2,          m1,          [r3 + 1 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m1,          [r3 + 14 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1824], m2

    ; mode 33 [row 2]

    movhps       [r0 + 2000], m2

    ; mode 30 [row 6, 7]

    pmaddubsw    m2,          m1,          [r3 + 27 * 16]
    pmulhrsw     m2,          m7

    psrldq       m0,          4
    punpcklbw    m6,          m0

    pmaddubsw    m3,          m6,          [r3 + 8 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1840], m2

    ; mode 33 [row 3]

    movhps       [r0 + 2008], m2

    ; mode 31 [row 0, 1]

    pmaddubsw    m2,          m4,          [r3 + 17 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m5,          [r3 + 2 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1856], m2

    ; mode 31 [row 2, 3]

    pmaddubsw    m2,          m5,          [r3 + 19 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m1,          [r3 + 4 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1872], m2

    ; mode 31 [row 4, 5]

    pmaddubsw    m2,          m1,          [r3 + 21 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m6,          [r3 + 6 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1888], m2

    ; mode 31 [row 6, 7]

    pmaddubsw    m2,          m6,          [r3 + 23 * 16]
    pmulhrsw     m2,          m7

    movu         m3,          [r1 + 6]
    punpcklbw    m0,          m3

    pmaddubsw    m3,          m0,          [r3 + 8 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1904], m2

    ; mode 32 [row 0, 1]

    pmaddubsw    m2,          m4,          [r3 + 21 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m5,          [r3 + 10 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1920], m2

    ; mode 32 [row 3]

    pmaddubsw    m2,          m1,          [r3 + 20 * 16]
    pmulhrsw     m2,          m7

    pxor         m3,          m3

    packuswb     m2,          m3
    movh         [r0 + 1944], m2

    ; mode 32 [row 4, 5]

    pmaddubsw    m2,          m6,          [r3 + 9 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m6,          [r3 + 30 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1952], m2

    ; mode 33 [row 4, 5]

    pmaddubsw    m2,          m0,          [r3 + 2 * 16]
    pmulhrsw     m2,          m7

    pmaddubsw    m3,          m0,          [r3 + 28 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 2016], m2

    ; mode 32 [row 6]

    pmaddubsw    m2,          m0,          [r3 + 19 * 16]
    pmulhrsw     m2,          m7

    ; mode 32 [row 7]

    movu         m0,          [r1 + 6]
    palignr      m3,          m0,          1
    punpcklbw    m0,          m3

    pmaddubsw    m3,          m0,          [r3 + 8 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 1968], m2

    ; mode 33 [row 6, 7]

    pmaddubsw    m2,          m0,          [r3 + 22 * 16]
    pmulhrsw     m2,          m7

    movu         m0,          [r1 + 7]
    palignr      m3,          m0,          1
    punpcklbw    m0,          m3

    pmaddubsw    m3,          m0,          [r3 + 16 * 16]
    pmulhrsw     m3,          m7

    packuswb     m2,          m3
    movu         [r0 + 2032], m2

    ; mode 33 [row 0]

    pmaddubsw    m2,          m4,          [r3 + 26 * 16]
    pmulhrsw     m2,          m7

    pxor         m3,          m3

    packuswb     m2,          m3
    movh         [r0 + 1984], m2

    ; mode 34 [row 0, 1, 2, 3, 4, 5, 6, 7]

    movu         m0,          [r2 + 2]
    palignr      m1,          m0,          1
    punpcklqdq   m2,          m0,          m1
    movu         [r0 + 2048], m2

    palignr      m1,          m0,          2
    palignr      m2,          m0,          3
    punpcklqdq   m1,          m2
    movu         [r0 + 2064], m1

    palignr      m1,          m0,          4
    palignr      m2,          m0,          5
    punpcklqdq   m1,          m2
    movu         [r0 + 2080], m1

    palignr      m1,          m0,          6
    palignr      m2,          m0,          7
    punpcklqdq   m1,          m2
    movu         [r0 + 2096], m1
RET

;--------------------------------------------------------------------------------
; void all_angs_pred_16x16(pixel *dest, pixel *refPix, pixel *filtPix, int bLuma)
;--------------------------------------------------------------------------------
INIT_XMM sse4
cglobal all_angs_pred_16x16, 3,4,8
    ; mode 2

    movu      m0,               [r2 + 2 + 32]
    movu      [r0 + 0 * 16],    m0

    movu      m1,               m0

    movu      m6,              [r2 + 18 + 32]
    palignr   m5,              m6,             m0,    1
    movu     [r0 + 1 * 16],    m5

    movu      m4,               m5

    palignr   m5,              m6,             m0,    2
    movu      [r0 + 2 * 16],   m5
    palignr   m5,              m6,             m0,    3
    movu      [r0 + 3 * 16],   m5
    palignr   m5,              m6,             m0,    4
    movu      [r0 + 4 * 16],   m5
    palignr   m5,              m6,             m0,    5
    movu      [r0 + 5 * 16],   m5
    palignr   m5,              m6,             m0,    6
    movu      [r0 + 6 * 16],   m5
    palignr   m5,              m6,             m0,    7
    movu      [r0 + 7 * 16],   m5

    movu      m7,               m5

    palignr   m5,              m6,             m0,    8
    movu      [r0 + 8 * 16],   m5

    movu      m2,              m5

    palignr   m5,              m6,             m0,    9
    movu      [r0 + 9 * 16],   m5

    palignr   m3,              m6,             m0,    10
    movu      [r0 + 10 * 16],  m3
    palignr   m3,              m6,             m0,    11
    movu      [r0 + 11 * 16],  m3
    palignr   m3,              m6,             m0,    12
    movu      [r0 + 12 * 16],  m3

    ; mode 3  [row 15]
    movu      [r0 + (3-2)*16*16 + 15 * 16], m3

    palignr   m3,              m6,             m0,    13
    movu      [r0 + 13 * 16],   m3
    palignr   m3,              m6,             m0,    14
    movu      [r0 + 14 * 16],   m3
    palignr   m3,              m6,             m0,    15
    movu      [r0 + 15 * 16],   m3

    ; mode 3 [row 0]
    lea           r3,    [ang_table]
    movu          m3,    [pw_1024]
    movu          m0,    [r2 + 1 + 32]
    punpcklbw     m0,    m1

    ; mode 17 [row 8 - second half]
    pmaddubsw     m1,                   m0,    [r3 + 22 * 16]
    pmulhrsw      m1,                   m3
    packuswb      m1,                   m1
    movh          [r0 + 248 * 16 + 8],  m1
    ; mode 17 [row 8 - second half] end

    pmaddubsw     m1,    m0,        [r3 + 26 * 16]
    pmulhrsw      m1,    m3
    punpcklbw     m7,    m2
    pmaddubsw     m2,    m7,        [r3 + 26 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 16 * 16],   m1

    ;mode 6 [row 1]
    movu          [r0 + 65 * 16],   m1

    ; mode 4 [row 0]
    pmaddubsw     m1,             m0,         [r3 + 21 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,         [r3 + 21 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 32 * 16], m1

    ; mode 5 [row 0]
    pmaddubsw     m1,             m0,         [r3 + 17 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,         [r3 + 17 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 48 * 16], m1

    ; mode 6 [row 0]
    pmaddubsw     m1,             m0,         [r3 + 13 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,         [r3 + 13 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 64 * 16], m1

    ; mode 7 [row 0]
    pmaddubsw     m1,             m0,        [r3 + 9 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,        [r3 + 9 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 80 * 16], m1

    ; mode 7 [row 1]
    pmaddubsw     m1,             m0,         [r3 + 18 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,         [r3 + 18 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 81 * 16], m1

    ; mode 7 [row 2]
    pmaddubsw     m1,             m0,         [r3 + 27 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,         [r3 + 27 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 82 * 16], m1

    ; mode 8 [row 0]
    pmaddubsw     m1,             m0,        [r3 + 5 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,        [r3 + 5 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 96 * 16], m1

    ; mode 8 [row 1]
    pmaddubsw     m1,             m0,         [r3 + 10 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,         [r3 + 10 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 97 * 16], m1

    ; mode 8 [row 2]
    pmaddubsw     m1,             m0,         [r3 + 15 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,         [r3 + 15 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 98 * 16], m1

    ; mode 8 [row 3]
    pmaddubsw     m1,             m0,         [r3 + 20 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m2,             m7,         [r3 + 20 * 16]
    pmulhrsw      m2,             m3
    packuswb      m1,             m2
    movu          [r0 + 99 * 16], m1

    ; mode 8 [row 4]
    pmaddubsw     m1,              m0,         [r3 + 25 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m2,              m7,         [r3 + 25 * 16]
    pmulhrsw      m2,              m3
    packuswb      m1,              m2
    movu          [r0 + 100 * 16], m1

    ; mode 8 [row 5]
    pmaddubsw     m1,              m0,         [r3 + 30 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m2,              m7,         [r3 + 30 * 16]
    pmulhrsw      m2,              m3
    packuswb      m1,              m2
    movu          [r0 + 101 * 16], m1

    ; mode 15 [row 13 - second half]
    pmaddubsw     m1,                  m0,     [r3 + 18 * 16]
    pmulhrsw      m1,                  m3
    packuswb      m1,                  m1
    movh          [r0 + 221 * 16 + 8], m1
    ; mode 15 [row 13 - second half] end

    ; mode 15 [row 14 - second half]
    pmaddubsw     m1,                  m0,     [r3 + 1 * 16]
    pmulhrsw      m1,                  m3
    packuswb      m1,                  m1
    movh          [r0 + 222 * 16 + 8], m1
    ; mode 15 [row 14 - second half] end

    ; mode 16 [row 10 - second half]
    pmaddubsw     m1,                  m0,    [r3 + 25 * 16]
    pmulhrsw      m1,                  m3
    packuswb      m1,                  m1
    movh          [r0 + 234 * 16 + 8], m1
    ; mode 16 [row 10 - second half] end

    ; mode 16 [row 11 - second half]
    pmaddubsw     m1,                  m0,    [r3 + 4 * 16]
    pmulhrsw      m1,                  m3
    packuswb      m1,                  m1
    movh          [r0 + 235 * 16 + 8], m1
    ; mode 16 [row 11 - second half] end

    ; mode 3 [row 1]
    movu          m6,    [r3 + 20 * 16]
    movu          m0,    [r2 + 2 + 32]
    punpcklbw     m0,    m4

    ; mode 17 [row 7 - second half]
    pmaddubsw     m1,     m0,          [r3 + 16 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                  m1
    movh          [r0 + 247 * 16 + 8], m1

    ; mode 17 [row 7 - second half] end
    pmaddubsw     m1,             m0,          m6
    pmulhrsw      m1,             m3
    movu          m2,             [r2 + 10 + 32]
    punpcklbw     m2,             m5
    pmaddubsw     m4,             m2,          m6
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 17 * 16], m1

    ;mode 6 [row 3]
    movu          [r0 + 67 * 16], m1

    ; mode 4 row [row 1]
    pmaddubsw     m1,             m0,         [r3 + 10 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 10 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 33 * 16], m1

    ; mode 4 row [row 2]
    pmaddubsw     m1,             m0,         [r3 + 31 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 31 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 34 * 16], m1

    ; mode 7 [row 6]
    movu          [r0 + 86 * 16], m1

    ; mode 5 row [row 1]
    pmaddubsw     m1,             m0,        [r3 + 2 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,        [r3 + 2 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 49 * 16], m1

    ; mode 5 row [row 2]
    pmaddubsw     m1,             m0,         [r3 + 19 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 19 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 50 * 16], m1

    ; mode 6 [row 2]
    pmaddubsw     m1,             m0,        [r3 + 7 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,        [r3 + 7 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 66 * 16], m1

    ; mode 7 [row 3]
    pmaddubsw     m1,             m0,        [r3 + 4 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,        [r3 + 4 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 83 * 16], m1

    ; mode 7 [row 4]
    pmaddubsw     m1,             m0,         [r3 + 13 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 13 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 84 * 16], m1

    ; mode 8 [row 8]
    movu          [r0 + 104 * 16], m1

    ; mode 7 [row 5]
    pmaddubsw     m1,             m0,         [r3 + 22 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 22 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 85 * 16], m1

    ; mode 8 [row 6]
    pmaddubsw     m1,              m0,      [r3 + 3 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,      [r3 + 3 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 102 * 16], m1

    ; mode 8 [row 7]
    pmaddubsw     m1,              m0,        [r3 + 8 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,        [r3 + 8 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 103 * 16], m1

    ; mode 8 [row 9]
    pmaddubsw     m1,              m0,         [r3 + 18 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,         [r3 + 18 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 105 * 16], m1

    ; mode 8 [row 10]
    pmaddubsw     m1,              m0,         [r3 + 23 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,         [r3 + 23 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 106 * 16], m1

    ; mode 8 [row 11]
    pmaddubsw     m1,              m0,         [r3 + 28 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,         [r3 + 28 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 107 * 16], m1

    ; mode 3 [row 2]
    movu          m0,    [r2 + 3 + 32]
    movd          m1,    [r2 + 19 + 32]
    palignr       m1,    m0,          1
    punpcklbw     m0,    m1

    ; mode 17 [row 6 - second half]
    pmaddubsw     m1,                  m0,     [r3 + 10 * 16]
    pmulhrsw      m1,                  m3
    packuswb      m1,                  m1
    movh          [r0 + 246 * 16 + 8], m1
    ; mode 17 [row 6 - second half] end

    pmaddubsw     m1,             m0,          [r3 + 14 * 16]
    pmulhrsw      m1,             m3
    movu          m2,             [r2 + 11 + 32]
    movd          m4,             [r2 + 27 + 32]
    palignr       m4,             m2,          1
    punpcklbw     m2,             m4
    pmaddubsw     m4,             m2,          [r3 + 14 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 18 * 16], m1

    ; mode 6 [row 5]
    movu          [r0 + 69 * 16], m1

    ; mode 4 row [row 3]
    pmaddubsw     m1,             m0,         [r3 + 20 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 20 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 35 * 16], m1

    ; mode 5 row [row 3]
    pmaddubsw     m1,             m0,        [r3 + 4 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,        [r3 + 4 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 51 * 16], m1

    ; mode 5 row [row 4]
    pmaddubsw     m1,             m0,         [r3 + 21 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 21 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 52 * 16], m1

    ; mode 6 [row 4]
    pmaddubsw     m1,             m0,        [r3 + 1 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,        [r3 + 1 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 68 * 16], m1

    ; mode 6 [row 6]
    pmaddubsw     m1,             m0,      [r3 + 27 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,      [r3 + 27 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 70 * 16], m1

    ; mode 7 [row 7]
    pmaddubsw     m1,             m0,        [r3 + 8 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,        [r3 + 8 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 87 * 16], m1

    ; mode 7 [row 8]
    pmaddubsw     m1,             m0,         [r3 + 17 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 17 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 88 * 16], m1

    ; mode 7 [row 9]
    pmaddubsw     m1,             m0,       [r3 + 26 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,       [r3 + 26 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 89 * 16], m1

    ; mode 8 [row 12]
    pmaddubsw     m1,              m0,        [r3 + 1 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,        [r3 + 1 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 108 * 16], m1

    ; mode 8 [row 13]
    pmaddubsw     m1,              m0,      [r3 + 6 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,      [r3 + 6 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 109 * 16], m1

    ; mode 8 [row 14]
    pmaddubsw     m1,              m0,         [r3 + 11 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,         [r3 + 11 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 110 * 16], m1

    ; mode 8 [row 15]
    pmaddubsw     m1,              m0,         [r3 + 16 * 16]
    pmulhrsw      m1,              m3
    pmaddubsw     m4,              m2,         [r3 + 16 * 16]
    pmulhrsw      m4,              m3
    packuswb      m1,              m4
    movu          [r0 + 111 * 16], m1

    ; mode 3 [row 3]
    movu          m0,              [r2 + 4 + 32]
    movd          m1,              [r2 + 20 + 32]
    palignr       m1,              m0,          1
    punpcklbw     m0,              m1

    ; mode 17 [row 4 - second half]
    pmaddubsw     m1,                  m0,    [r3 + 30 * 16]
    pmulhrsw      m1,                  m3
    packuswb      m1,                  m1
    movh          [r0 + 244 * 16 + 8], m1
    ; mode 17 [row 4 - second half] end

    ; mode 17 [row 5 - second half]
    pmaddubsw     m1,                  m0,    [r3 + 4 * 16]
    pmulhrsw      m1,                  m3
    packuswb      m1,                  m1
    movh          [r0 + 245 * 16 + 8], m1
    ; mode 17 [row 5 - second half] end

    pmaddubsw     m1,             m0,          [r3 + 8 * 16]
    pmulhrsw      m1,             m3
    movu          m2,             [r2 + 12 + 32]
    movd          m4,             [r2 + 28 + 32]
    palignr       m4,             m2,          1
    punpcklbw     m2,             m4
    pmaddubsw     m4,             m2,          [r3 + 8 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 19 * 16], m1

    ; mode 6 [row 7]
    movu          [r0 + 71 * 16], m1

    ; mode 4 row [row 4]
    pmaddubsw     m1,             m0,        [r3 + 9 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,        [r3 + 9 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 36 * 16], m1

    ; mode 4 row [row 5]
    pmaddubsw     m1,             m0,        [r3 + 30 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 30 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 37 * 16], m1

    ; mode 7 row [row 13]
    movu          [r0 + 93 * 16], m1

    ; mode 5 row [row 5]
    pmaddubsw     m1,             m0,        [r3 + 6 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,        [r3 + 6 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 53 * 16], m1

    ; mode 5 row [row 6]
    pmaddubsw     m1,             m0,         [r3 + 23 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 23 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 54 * 16], m1

    ; mode 6 [row 8]
    pmaddubsw     m1,             m0,         [r3 + 21 * 16]
    pmulhrsw      m1,             m3
    pmaddubsw     m4,             m2,         [r3 + 21 * 16]
    pmulhrsw      m4,             m3
    packuswb      m1,             m4
    movu          [r0 + 72 * 16], m1

    ; mode 7 [row 12]
    movu          [r0 + 92 * 16], m1

    ; mode 7 [row 10]
    pmaddubsw     m1,    m0,      [r3 + 3 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 3 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 90 * 16], m1

    ; mode 7 [row 11]
    pmaddubsw     m1,    m0,      [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 91 * 16], m1

    ; mode 3 [row 4]
    movu          m0,    [r2 + 5 + 32]
    movd          m1,    [r2 + 20 + 32]
    palignr       m1,    m0,         1
    punpcklbw     m0,    m1

    ; mode 17 [row 3 - second half]
    pmaddubsw     m1,     m0,           [r3 + 24 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 243 * 16 + 8],  m1

    ; mode 17 [row 3 - second half] end
    pmaddubsw     m1,    m0,          [r3 + 2 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 13 + 32]
    movd          m4,    [r2 + 29 + 32]
    palignr       m4,    m2,          1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,          [r3 + 2 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 20 * 16], m1

    ;mode 6 [row 9]
    movu          [r0 + 73 * 16], m1

    ; mode 4 row [row 6]
    movu          m6,    [r3 + 19 * 16]
    pmaddubsw     m1,    m0,      m6
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      m6
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 38 * 16], m1

    ; mode 3 [row 5]
    pmaddubsw     m1,    m0,      [r3 + 28 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 21 * 16], m1

    ;mode 6 [row 11]
    movu          [r0 + 75 * 16], m1

    ; mode 5 row [row 7]
    pmaddubsw     m1,    m0,      [r3 + 8 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 8 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 55 * 16], m1

    ; mode 5 row [row 8]
    pmaddubsw     m1,    m0,      [r3 + 25 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 25 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 56 * 16], m1

    ; mode 6 [row 10]
    pmaddubsw     m1,    m0,      [r3 + 15 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 15 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 74 * 16], m1

    ; mode 7 [row 14]
    pmaddubsw     m1,    m0,      [r3 + 7 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 7 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 94 * 16], m1

    ; mode 7 [row 15]
    pmaddubsw     m1,    m0,      [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 95 * 16], m1

    ; mode 3 [row 6]
    movu          m0,    [r2 + 6 + 32]
    movd          m1,    [r2 + 22 + 32]
    palignr       m1,    m0,          1
    punpcklbw     m0,    m1

    ; mode 17 [row 2 - second half]
    pmaddubsw     m1,     m0,          [r3 + 18 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 242 * 16 + 8],  m1
    ; mode 17 [row 2 - second half] end

    pmaddubsw     m1,    m0,          [r3 + 22 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 14 + 32]
    movd          m4,    [r2 + 30 + 32]
    palignr       m4,    m2,          1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,          [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 22 * 16], m1

    ; mode 6 [row 13]
    movu          [r0 + 77 * 16], m1

    ; mode 4 row [row 7]
    pmaddubsw     m1,    m0,      [r3 + 8 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 8 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 39 * 16], m1

    ; mode 4 row [row 8]
    pmaddubsw     m1,    m0,       [r3 + 29 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,       [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 40 * 16], m1

    ; mode 5 row [row 9]
    pmaddubsw     m1,    m0,      [r3 + 10 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 57 * 16], m1

    ; mode 5 row [row 10]
    pmaddubsw     m1,    m0,      [r3 + 27 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 27 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 58 * 16], m1

    ; mode 6 [row 12]
    pmaddubsw     m1,    m0,      [r3 + 9 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 9 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 76 * 16], m1

    ; mode 3 [row 7]
    movu          m0,    [r2 + 7 + 32]
    movd          m1,    [r2 + 27 + 32]
    palignr       m1,    m0,          1
    punpcklbw     m0,    m1

    ; mode 17 [row 1 - second half]
    pmaddubsw     m1,     m0,           [r3 + 12 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 241 * 16 + 8],  m1
    ; mode 17 [row 1 - second half] end

    pmaddubsw     m1,    m0,          [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 15 + 32]
    movd          m4,    [r2 + 25 + 32]
    palignr       m4,    m2,          1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,          [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 23 * 16], m1

    ; mode 6 [row 15]
    movu          [r0 + 79 * 16], m1

    ; mode 4 row [row 9]
    pmaddubsw     m1,    m0,      [r3 + 18 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 18 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 41 * 16], m1

    ; mode 5 row [row 11]
    pmaddubsw     m1,    m0,      [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 59 * 16], m1

    ; mode 5 row [row 12]
    pmaddubsw     m1,    m0,      [r3 + 29 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 60 * 16], m1

    ; mode 6 [row 14]
    pmaddubsw     m1,    m0,      [r3 + 3 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 3 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 78 * 16], m1

    ; mode 3 [row 8]
    movu          m0,    [r2 + 8 + 32]
    movd          m1,    [r2 + 24 + 32]
    palignr       m1,    m0,          1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,          [r3 + 10 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 16 + 32]
    psrldq        m4,    m2,         1
    pinsrb        m4,    [r2 + 32],  15
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,          [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 24 * 16], m1

    ; mode 4 row [row 10]
    pmaddubsw     m1,    m0,      [r3 + 7 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 7 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 42 * 16], m1

    ; mode 4 row [row 11]
    pmaddubsw     m1,    m0,      [r3 + 28 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 43 * 16], m1

    ; mode 5 row [row 13]
    pmaddubsw     m1,    m0,      [r3 + 14 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 61 * 16], m1

    ; mode 5 row [row 14]
    pmaddubsw     m1,    m0,      [r3 + 31 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 31 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 62 * 16], m1

    ; mode 3 [row 9]
    movu          m0,    [r2 +  9 + 32]
    movd          m1,    [r2 + 16 + 32]
    palignr       m1,    m0,         1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,         [r3 + 4 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 17 + 32]
    movd          m4,    [r2 + 33 + 32]
    palignr       m4,    m2,         1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,         [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 25 * 16], m1

    ; mode 4 row [row 12]
    pmaddubsw     m1,    m0,      [r3 + 17 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 17 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 44 * 16], m1

    ; mode 3 [row 10]
    pmaddubsw     m1,    m0,          [r3 + 30 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,          [r3 + 30 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 26 * 16], m1

    ; mode 5 row [row 15]
    pmaddubsw     m1,    m0,      [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 63 * 16], m1

    ; mode 3 [row 11]
    movu          m0,    [r2 + 10 + 32]
    movd          m1,    [r2 + 26 + 32]
    palignr       m1,    m0,          1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,          [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 18 + 32]
    movd          m4,    [r2 + 34 + 32]
    palignr       m4,    m2,         1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,         [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,                 m4
    movu          [r0 + 27 * 16],     m1

    ; mode 4 row [row 13]
    pmaddubsw     m1,    m0,      [r3 + 6 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 6 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 45 * 16], m1

    ; mode 4 row [row 14]
    pmaddubsw     m1,    m0,      [r3 + 27 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 27 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 46 * 16], m1

    ; mode 3 [row 12]
    movu          m0,    [r2 + 11 + 32]
    movd          m1,    [r2 + 27 + 32]
    palignr       m1,    m0,          1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,          [r3 + 18 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 19 + 32]
    movd          m4,    [r2 + 35 + 32]
    palignr       m4,    m2,          1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,          [r3 + 18 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 28 * 16], m1

    ; mode 4 row [row 15]
    pmaddubsw     m1,    m0,      [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m2,      [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 47 * 16], m1

    ; mode 3 [row 13]
    movu          m0,    [r2 + 12 + 32]
    movd          m1,    [r2 + 28 + 32]
    palignr       m1,    m0,          1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,          [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 20 + 32]
    movd          m4,    [r2 + 36 + 32]
    palignr       m4,    m2,          1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,          [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,             m4
    movu          [r0 + 29 * 16], m1

    ; mode 3 [row 14]
    movu          m0,    [r2 + 13 + 32]
    movd          m1,    [r2 + 29 + 32]
    palignr       m1,    m0,         1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,         [r3 + 6 * 16]
    pmulhrsw      m1,    m3
    movu          m2,    [r2 + 21 + 32]
    movd          m4,    [r2 + 37 + 32]
    palignr       m4,    m2,         1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m2,         [r3 + 6 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,                m4
    movu          [r0 + 30 * 16],    m1

    ; mode 9
    movu          m0,    [r1 + 1 + 32]
    movd          m1,    [r1 + 17 + 32]
    palignr       m1,    m0,         1

    ; mode 9 [row 15]
    movu          [r0 + 127 * 16],  m1

    ; mode 9 [row 0]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        [r3 + 2 * 16]
    pmulhrsw      m1,    m3
    movu          m7,    [r1 +  9 + 32]
    movd          m4,    [r2 + 25 + 32]
    palignr       m2,    m7,        1
    punpcklbw     m7,    m2
    pmaddubsw     m2,    m7,        [r3 + 2 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 112 * 16],  m1

    ; mode 9 [row 1]
    pmaddubsw     m1,    m0,        [r3 + 4 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 4 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 113 * 16],  m1

    ; mode 9 [row 2]
    pmaddubsw     m1,    m0,        [r3 + 6 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 6 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 114 * 16],  m1

    ; mode 9 [row 3]
    pmaddubsw     m1,    m0,        [r3 + 8 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 8 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 115 * 16],  m1

    ; mode 9 [row 4]
    pmaddubsw     m1,    m0,        [r3 + 10 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 10 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 116 * 16],  m1

    ; mode 9 [row 5]
    pmaddubsw     m1,    m0,        [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 12 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 117 * 16],  m1

    ; mode 9 [row 6]
    pmaddubsw     m1,    m0,        [r3 + 14 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 14 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 118 * 16],  m1

    ; mode 9 [row 7]
    pmaddubsw     m1,    m0,        [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 16 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 119 * 16],  m1

    ; mode 9 [row 8]
    pmaddubsw     m1,    m0,        [r3 + 18 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 18 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 120 * 16],  m1

    ; mode 9 [row 9]
    pmaddubsw     m1,    m0,        [r3 + 20 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 20 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 121 * 16],  m1

    ; mode 9 [row 10]
    pmaddubsw     m1,    m0,        [r3 + 22 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 22 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 122 * 16],  m1

    ; mode 9 [row 11]
    pmaddubsw     m1,    m0,        [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 24 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 123 * 16],  m1

    ; mode 9 [row 12]
    pmaddubsw     m1,    m0,        [r3 + 26 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 26 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 124 * 16],  m1

    ; mode 9 [row 13]
    pmaddubsw     m1,    m0,         [r3 + 28 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,         [r3 + 28 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 125 * 16],  m1

    ; mode 9 [row 14]
    pmaddubsw     m1,    m0,        [r3 + 30 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 30 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 126 * 16],  m1

    ; mode 10
    movu         m1,               [r1 + 1 + 32]
    movu         [r0 + 128 * 16],  m1
    movu         [r0 + 129 * 16],  m1
    movu         [r0 + 130 * 16],  m1
    movu         [r0 + 131 * 16],  m1
    movu         [r0 + 132 * 16],  m1
    movu         [r0 + 133 * 16],  m1
    movu         [r0 + 134 * 16],  m1
    movu         [r0 + 135 * 16],  m1
    movu         [r0 + 136 * 16],  m1
    movu         [r0 + 137 * 16],  m1
    movu         [r0 + 138 * 16],  m1
    movu         [r0 + 139 * 16],  m1
    movu         [r0 + 140 * 16],  m1
    movu         [r0 + 141 * 16],  m1
    movu         [r0 + 142 * 16],  m1
    movu         [r0 + 143 * 16],  m1

    pxor         m0,          m0
    pshufb       m1,          m1,         m0
    punpcklbw    m1,          m0
    pinsrb       m2,          [r1], 0
    pshufb       m2,          m2,         m0
    punpcklbw    m2,          m0
    movu         m4,          [r1 + 1]
    punpcklbw    m5,          m4,         m0
    punpckhbw    m4,          m0
    psubw        m5,          m2
    psubw        m4,          m2
    psraw        m5,          1
    psraw        m4,          1
    paddw        m5,          m1
    paddw        m4,          m1
    packuswb     m5,          m4

    pextrb       [r0 + 128 * 16],  m5,          0
    pextrb       [r0 + 129 * 16],  m5,          1
    pextrb       [r0 + 130 * 16],  m5,          2
    pextrb       [r0 + 131 * 16],  m5,          3
    pextrb       [r0 + 132 * 16],  m5,          4
    pextrb       [r0 + 133 * 16],  m5,          5
    pextrb       [r0 + 134 * 16],  m5,          6
    pextrb       [r0 + 135 * 16],  m5,          7
    pextrb       [r0 + 136 * 16],  m5,          8
    pextrb       [r0 + 137 * 16],  m5,          9
    pextrb       [r0 + 138 * 16],  m5,          10
    pextrb       [r0 + 139 * 16],  m5,          11
    pextrb       [r0 + 140 * 16],  m5,          12
    pextrb       [r0 + 141 * 16],  m5,          13
    pextrb       [r0 + 142 * 16],  m5,          14
    pextrb       [r0 + 143 * 16],  m5,          15

    ; mode 11
    movu          m0,               [r1 + 32]
    pinsrb        m0,               [r1], 0

    ; mode 11 [row 15]
    movu          [r0 + 159 * 16],  m0

    ; mode 11 [row 0]
    movu          m1,    [r1 + 1 + 32]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        [r3 + 30 * 16]
    pmulhrsw      m1,    m3
    movu          m7,    [r1 + 8 + 32]
    movu          m2,    [r1 + 9 + 32]
    punpcklbw     m7,    m2
    pmaddubsw     m2,    m7,        [r3 + 30 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 144 * 16],  m1

    ; mode 11 [row 1]
    pmaddubsw     m1,    m0,        [r3 + 28 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 28 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 145 * 16],  m1

    ; mode 11 [row 2]
    pmaddubsw     m1,    m0,        [r3 + 26 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 26 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 146 * 16],  m1

    ; mode 11 [row 3]
    pmaddubsw     m1,    m0,         [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 24 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 147 * 16],  m1

    ; mode 11 [row 4]
    pmaddubsw     m1,    m0,        [r3 + 22 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 22 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 148 * 16],  m1

    ; mode 11 [row 5]
    pmaddubsw     m1,    m0,        [r3 + 20 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 20 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 149 * 16],  m1

    ; mode 11 [row 6]
    pmaddubsw     m1,    m0,        [r3 + 18 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 18 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 150 * 16],  m1

    ; mode 11 [row 7]
    pmaddubsw     m1,    m0,        [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 16 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 151 * 16],  m1

    ; mode 11 [row 8]
    pmaddubsw     m1,    m0,        [r3 + 14 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 14 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 152 * 16],  m1

    ; mode 11 [row 9]
    pmaddubsw     m1,    m0,        [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 12 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 153 * 16],  m1

    ; mode 11 [row 10]
    pmaddubsw     m1,    m0,        [r3 + 10 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 10 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 154 * 16],  m1

    ; mode 11 [row 11]
    pmaddubsw     m1,    m0,        [r3 + 8 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 8 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 155 * 16],  m1

    ; mode 11 [row 12]
    pmaddubsw     m1,    m0,        [r3 + 6 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 6 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 156 * 16],  m1

    ; mode 11 [row 13]
    pmaddubsw     m1,    m0,        [r3 + 4 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 4 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 157 * 16],  m1

    ; mode 11 [row 14]
    pmaddubsw     m1,    m0,        [r3 + 2 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 2 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 158 * 16],  m1

    ; mode 12 [row 0]
    movu          m0,    [r2 + 32]
    pinsrb        m0,    [r2], 0
    movu          m1,    [r2 + 1 + 32]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        [r3 + 27 * 16]
    pmulhrsw      m1,    m3
    movu          m7,    [r2 + 8 + 32]
    movd          m2,    [r2 + 24 + 32]
    palignr       m2,    m7,        1
    punpcklbw     m7,    m2
    pmaddubsw     m2,    m7,        [r3 + 27 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 160 * 16],  m1

    ; mode 12 [row 1]
    pmaddubsw     m1,    m0,        [r3 + 22 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 22 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 161 * 16],  m1

    ; mode 12 [row 2]
    pmaddubsw     m1,    m0,        [r3 + 17 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 17 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 162 * 16],  m1

    ; mode 12 [row 3]
    pmaddubsw     m1,    m0,        [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 12 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 163 * 16],  m1

    ; mode 12 [row 4]
    pmaddubsw     m1,    m0,        [r3 + 7 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 7 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 164 * 16],  m1

    ; mode 12 [row 5]
    pmaddubsw     m1,    m0,        [r3 + 2 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 2 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 165 * 16],  m1

    ; mode 13 [row 0]
    pmaddubsw     m1,    m0,        [r3 + 23 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 23 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 176 * 16],  m1

    ; mode 13 [row 1]
    pmaddubsw     m1,    m0,        [r3 + 14 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 14 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 177 * 16],  m1

    ; mode 13 [row 2]
    pmaddubsw     m1,    m0,        [r3 + 5 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 5 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 178 * 16],  m1

    ; mode 14 [row 0]
    pmaddubsw     m1,    m0,        [r3 + 19 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 19 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 192 * 16],  m1

    ; mode 14 [row 1]
    pmaddubsw     m1,    m0,        [r3 + 6 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 6 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 193 * 16],  m1

    ; mode 17 [row 0]
    movu          [r0 + 240 * 16],  m1

    ; mode 15 [row 0]
    pmaddubsw     m1,    m0,        [r3 + 15 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 15 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 208 * 16],  m1

    ; mode 15 [row 15 - second half]
    pmaddubsw     m1,    m0,           [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 223 * 16 + 8], m1
    ; mode 15 [row 15 - second half] end

    ; mode 16 [row 0]
    pmaddubsw     m1,    m0,        [r3 + 11 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m2,    m7,        [r3 + 11 * 16]
    pmulhrsw      m2,    m3
    packuswb      m1,               m2
    movu          [r0 + 224 * 16],  m1

    ; mode 17 [row 9 - second half]
    pmaddubsw     m1,     m0,          [r3 + 28 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 249 * 16 + 8],  m1
    ; mode 17 [row 9 - second half] end

    ; mode 17 [row 10 - second half]
    pmaddubsw     m1,     m0,          [r3 + 2 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 250 * 16 + 8],  m1
    ; mode 17 [row 10 - second half] end

    ; mode 17 [row 1 - first half]
    pslldq        m6,     m0,          2
    pinsrb        m6,     [r2],        1
    pinsrb        m6,     [r2 + 1],    0
    pmaddubsw     m1,     m6,          [r3 + 12 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,               m1
    movh          [r0 + 241 * 16],  m1

    ; mode 17 [row 11 - second half]
    pmaddubsw     m1,     m6,          [r3 + 8 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 251 * 16 + 8],  m1
    ; mode 17 [row 11 - second half] end

    ; mode 17 [row 2 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 1],    1
    pinsrb        m6,     [r2 + 2],    0
    pmaddubsw     m1,     m6,          [r3 + 18 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                  m1
    movh          [r0 + 242 * 16],     m1

    ; mode 17 [row 12 - second half]
    pmaddubsw     m1,     m6,           [r3 + 14 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 252 * 16 + 8],  m1
    ; mode 17 [row 12 - second half] end

    ; mode 17 [row 3 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 2],    1
    pinsrb        m6,     [r2 + 4],    0
    pmaddubsw     m1,     m6,          [r3 + 24 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,               m1
    movh          [r0 + 243 * 16],  m1

    ; mode 17 [row 13 - first half]
    pmaddubsw     m1,     m6,           [r3 + 20 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 253 * 16 + 8],  m1

    ; mode 17 [row 4 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 4],    1
    pinsrb        m6,     [r2 + 5],    0
    pmaddubsw     m1,     m6,          [r3 + 30 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                  m1
    movh          [r0 + 244 * 16],     m1

    ; mode 17 [row 5 - first half]
    pmaddubsw     m1,     m6,          [r3 + 4 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,               m1
    movh          [r0 + 245 * 16],  m1

    ; mode 17 [row 14 - second half]
    pmaddubsw     m1,     m6,          [r3 + 26 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                  m1
    movh          [r0 + 254 * 16 + 8], m1
    ; mode 17 [row 14 - second half] end

    ; mode 17 [row 6 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 5],    1
    pinsrb        m6,     [r2 + 6],    0
    pmaddubsw     m1,     m6,          [r3 + 10 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                  m1
    movh          [r0 + 246 * 16],     m1

    ; mode 17 [row 7 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 6],    1
    pinsrb        m6,     [r2 + 7],    0
    pmaddubsw     m1,     m6,          [r3 + 16 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                  m1
    movh          [r0 + 247 * 16],     m1

    ; mode 17 [row 8 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 7],    1
    pinsrb        m6,     [r2 + 9],    0
    pmaddubsw     m1,     m6,          [r3 + 22 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                  m1
    movh          [r0 + 248 * 16],     m1

    ; mode 17 [row 9 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 +  9],    1
    pinsrb        m6,     [r2 + 10],    0
    pmaddubsw     m1,     m6,           [r3 + 28 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 249 * 16],      m1

    ; mode 17 [row 10 - first half]
    pmaddubsw     m1,     m6,          [r3 + 2 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                  m1
    movh          [r0 + 250 * 16],     m1

    ; mode 17 [row 11 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 10],    1
    pinsrb        m6,     [r2 + 11],    0
    pmaddubsw     m1,     m6,           [r3 + 8 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 251 * 16],      m1

    ; mode 17 [row 12 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 11],    1
    pinsrb        m6,     [r2 + 12],    0
    pmaddubsw     m1,     m6,           [r3 + 14 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 252 * 16],      m1

    ; mode 17 [row 13 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 12],    1
    pinsrb        m6,     [r2 + 14],    0
    pmaddubsw     m1,     m6,           [r3 + 20 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 253 * 16],      m1

    ; mode 17 [row 14 - first half]
    pslldq        m6,     2
    pinsrb        m6,     [r2 + 14],    1
    pinsrb        m6,     [r2 + 15],    0
    pmaddubsw     m1,     m6,           [r3 + 26 * 16]
    pmulhrsw      m1,     m3
    packuswb      m1,                   m1
    movh          [r0 + 254 * 16],      m1

    ; mode 16 [row 12 -  second half]
    pmaddubsw     m1,    m0,            [r3 + 15 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                   m1
    movh          [r0 + 236 * 16 + 8],  m1
    ; mode 16 [row 12 -  second half]

    ; mode 12 [row 6]
    pslldq        m2,    m0,            2
    pinsrb        m2,    [r2], 1
    pinsrb        m2,    [r2 + 6],      0
    pmaddubsw     m1,    m2,            [r3 + 29 * 16]
    pmulhrsw      m1,    m3
    movu          m0,    [r2 + 7 + 32]
    psrldq        m4,    m0,            1
    punpcklbw     m0,    m4
    pmaddubsw     m4,    m0,            [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,                   m4
    movu          [r0 + 166 * 16],      m1

    ; mode 12 [row 7]
    pmaddubsw     m1,    m2,        [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 167 * 16],  m1

    ; mode 12 [row 8]
    pmaddubsw     m1,    m2,        [r3 + 19 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 19 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 168 * 16],  m1

    ; mode 12 [row 9]
    pmaddubsw     m1,    m2,        [r3 + 14 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 169 * 16],  m1

    ; mode 12 [row 10]
    pmaddubsw     m1,    m2,        [r3 + 9 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 9 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 170 * 16],  m1

    ; mode 12 [row 11]
    pmaddubsw     m1,    m2,        [r3 + 4 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,    m4
    movu          [r0 + 171 * 16],  m1

    ; mode 13 [row 3]
    pinsrb        m7,    m2,        [r2 +  4],   0
    pmaddubsw     m1,    m7,        [r3 + 28 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 179 * 16],  m1

    ; mode 13 [row 4]
    pmaddubsw     m1,    m7,        [r3 + 19 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 19 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 180 * 16],  m1

    ; mode 13 [row 5]
    pmaddubsw     m1,    m7,        [r3 + 10 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 181 * 16],  m1

    ; mode 13 [row 6]
    pmaddubsw     m1,    m7,        [r3 + 1 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 1 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 182 * 16],  m1

    ; mode 14 [row 2]
    pinsrb        m5,    m7,        [r2 +  2],   0
    pmaddubsw     m1,    m5,        [r3 + 25 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 25 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 194 * 16],  m1

    ; mode 14 [row 3]
    pmaddubsw     m1,    m5,        [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 195 * 16],  m1

    ; mode 15 [row 1]
    pmaddubsw     m1,    m5,        [r3 + 30 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 30 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 209 * 16],  m1

    ; mode 15 [row 2]
    pmaddubsw     m1,    m5,        [r3 + 13 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 13 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 210 * 16],  m1

    ; mode 16 [row 1]
    pmaddubsw     m1,    m5,        [r3 + 22 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 225 * 16],  m1

    ; mode 16 [row 2]
    pmaddubsw     m1,    m5,        [r3 + 1 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m4,    m0,        [r3 + 1 * 16]
    pmulhrsw      m4,    m3
    packuswb      m1,               m4
    movu          [r0 + 226 * 16],  m1

    ; mode 16 [row 13 - second half]
    pmaddubsw     m1,    m5,           [r3 + 26 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 237 * 16 + 8], m1
    ; mode 16 [row 13 - second half]

    ; mode 16 [row 14 - second half]
    pmaddubsw     m1,    m5,           [r3 + 5 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 238 * 16 + 8], m1
    ; mode 16 [row 14 - second half]

    ; mode 16 [row 3]
    pslldq        m6,    m5,         2
    pinsrb        m6,    [r2 + 2],   1
    pinsrb        m6,    [r2 + 3],   0
    pmaddubsw     m1,    m6,         [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 227 * 16],   m1

    ; mode 16 [row 15 - second half]
    pmaddubsw     m1,    m6,          [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 239 * 16 + 8], m1
    ; mode 16 [row 15 - second half] end

    ; mode 16 [row 4- first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 3],   1
    pinsrb        m6,    [r2 + 5],   0
    pmaddubsw     m1,    m6,         [r3 + 23 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 228 * 16],   m1

    ; mode 16 [row 5- first half]
    pmaddubsw     m1,    m6,        [r3 + 2 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 229 * 16],  m1

    ; mode 16 [row 6- first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 5],   1
    pinsrb        m6,    [r2 + 6],   0
    pmaddubsw     m1,    m6,         [r3 + 13 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 230 * 16],   m1

    ; mode 16 [row 7- first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 6],   1
    pinsrb        m6,    [r2 + 8],   0
    pmaddubsw     m1,    m6,         [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 231 * 16],   m1

    ; mode 16 [row 8- first half]
    pmaddubsw     m1,    m6,        [r3 + 3 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 232 * 16],  m1
    ; mode 19 [row 0 - second half] end

    ; mode 16 [row 9- first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 8],   1
    pinsrb        m6,    [r2 + 9],   0
    pmaddubsw     m1,    m6,        [r3 + 14 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 233 * 16],  m1

    ; mode 16 [row 10 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 +  9], 1
    pinsrb        m6,    [r2 + 11], 0
    pmaddubsw     m1,    m6,        [r3 + 25 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 234 * 16],  m1

    ; mode 16 [row 11 - first half]
    pmaddubsw     m1,    m6,        [r3 + 4 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 235 * 16],  m1

    ; mode 16 [row 12 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 11], 1
    pinsrb        m6,    [r2 + 12], 0
    pmaddubsw     m1,    m6,        [r3 + 15 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 236 * 16],  m1

    ; mode 16 [row 13 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 12],   1
    pinsrb        m6,    [r2 + 14],   0
    pmaddubsw     m1,    m6,        [r3 + 26 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 237 * 16],  m1

    ; mode 16 [row 14 - first half]
    pmaddubsw     m1,    m6,        [r3 + 5 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 238 * 16],  m1

    ; mode 16 [row 15 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 14],   1
    pinsrb        m6,    [r2 + 15],   0
    pmaddubsw     m1,    m6,          [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,               m1
    movh          [r0 + 239 * 16],  m1

    ; mode 14 [row 4]
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 2],   1
    pinsrb        m5,    [r2 + 5],   0
    movu          m4,    [r2 + 6 + 32]
    psrldq        m0,    m4,         1
    punpcklbw     m4,    m0

    ; mode 16 [row 3 - second half]
    pmaddubsw     m1,    m4,        [r3 + 12 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 227 * 16 + 8], m1

    ; mode 16 [row 3 - second half] end
    pmaddubsw     m1,    m5,        [r3 + 31 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m0,    m4,        [r3 + 31 * 16]
    pmulhrsw      m0,    m3
    packuswb      m1,               m0
    movu          [r0 + 196 * 16],  m1

    ; mode 14 [row 5]
    pmaddubsw     m1,    m5,        [r3 + 18 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m0,    m4,        [r3 + 18 * 16]
    pmulhrsw      m0,    m3
    packuswb      m1,               m0
    movu          [r0 + 197 * 16],  m1

    ; mode 14 [row 6]
    pmaddubsw     m1,    m5,         [r3 + 5 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m0,    m4,         [r3 + 5 * 16]
    pmulhrsw      m0,    m3
    packuswb      m1,               m0
    movu          [r0 + 198 * 16],  m1

    ; mode 15 [row 3]
    movu          m6,    m5
    pinsrb        m6,    [r2 + 4],   0
    pmaddubsw     m1,    m6,         [r3 + 28 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m0,    m4,         [r3 + 28 * 16]
    pmulhrsw      m0,    m3
    packuswb      m1,                m0
    movu          [r0 + 211 * 16],   m1

    ; mode 15 [row 4]
    pmaddubsw     m1,    m6,         [r3 + 11 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m0,    m4,         [r3 + 11 * 16]
    pmulhrsw      m0,    m3
    packuswb      m1,                m0
    movu          [r0 + 212 * 16],   m1

    ; mode 15 [row 5 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 4],   1
    pinsrb        m6,    [r2 + 6],   0
    pmaddubsw     m1,    m6,         [r3 + 26 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 213 * 16],   m1

    ; mode 15 [row 6 - first half]
    pmaddubsw     m1,    m6,         [r3 + 9 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 214 * 16],   m1

    ; mode 15 [row 7 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 6],   1
    pinsrb        m6,    [r2 + 8],   0
    pmaddubsw     m1,    m6,         [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 215 * 16],   m1

    ; mode 15 [row 8 - first half]
    pmaddubsw     m1,    m6,         [r3 + 7 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 216 * 16],   m1

    ; mode 15 [row 9 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 8],   1
    pinsrb        m6,    [r2 + 9],   0
    pmaddubsw     m1,    m6,         [r3 + 22 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 217 * 16],   m1

    ; mode 15 [row 10 - first half]
    pmaddubsw     m1,    m6,         [r3 + 5 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 218 * 16],   m1

    ; mode 15 [row 11 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 +  9],   1
    pinsrb        m6,    [r2 + 11],   0
    pmaddubsw     m1,    m6,         [r3 + 20 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 219 * 16],   m1

    ; mode 15 [row 12 - first half]
    pmaddubsw     m1,    m6,         [r3 + 3 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 220 * 16],   m1

    ; mode 15 [row 13 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 11],   1
    pinsrb        m6,    [r2 + 13],   0
    pmaddubsw     m1,    m6,         [r3 + 18 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 221 * 16],   m1

    ; mode 15 [row 14 - first half]
    pmaddubsw     m1,    m6,         [r3 + 1 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 222 * 16],   m1

    ; mode 15 [row 15 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 13],   1
    pinsrb        m6,    [r2 + 15],   0
    pmaddubsw     m1,    m6,         [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                m1
    movh          [r0 + 223 * 16],   m1

    ; mode 14 [row 7]
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 5],   1
    pinsrb        m5,    [r2 + 7],   0
    movu          m0,    [r2 + 5 + 32]
    psrldq        m6,    m0,          1
    punpcklbw     m0,    m6

    ; mode 15 [row 5 - second half]
    pmaddubsw     m1,    m0,           [r3 + 26 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 213 * 16 + 8], m1
    ; mode 15 [row 5 - second half] end

    ; mode 15 [row 6 - second half]
    pmaddubsw     m1,    m0,           [r3 + 9 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 214 * 16 + 8], m1
    ; mode 15 [row 6 - second half] end

    ; mode 16 [row 4 - second half]
    pmaddubsw     m1,    m0,        [r3 + 23 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 228 * 16 + 8], m1
    ; mode 16 [row 4 - second half] end

    ; mode 16 [row 5 - second half]
    pmaddubsw     m1,    m0,        [r3 + 2 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                  m1
    movh          [r0 + 229 * 16 + 8], m1

    ; mode 16 [row 5 - second half] end
    pmaddubsw     m1,    m5,        [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,        [r3 + 24 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,               m6
    movu          [r0 + 199 * 16],  m1

    ; mode 14 [row 8]
    pmaddubsw     m1,    m5,        [r3 + 11 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,        [r3 + 11 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,               m6
    movu          [r0 + 200 * 16],  m1

    ; mode 14 [row 9]
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 7],    1
    pinsrb        m5,    [r2 + 10],   0
    movu          m0,    [r2 + 4 + 32]
    psrldq        m6,    m0,          1
    punpcklbw     m0,    m6

    ; mode 15 [row 7 - second half]
    pmaddubsw     m1,    m0,             [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 215 * 16 + 8],   m1
    ; mode 15 [row 7 - second half] end

    ; mode 15 [row 8 - second half]
    pmaddubsw     m1,    m0,             [r3 + 7 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 216 * 16 + 8],   m1
    ; mode 15 [row 8 - second half] end

    ; mode 16 [row 6 - second half]
    pmaddubsw     m1,    m0,             [r3 + 13 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 230 * 16 + 8],   m1
    ; mode 16 [row 6 - second half] end

    ; mode 15 [row 6 - second half] end
    pmaddubsw     m1,    m5,        [r3 + 30 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,        [r3 + 30 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,               m6
    movu          [r0 + 201 * 16],  m1

    ; mode 14 [row 10]
    pmaddubsw     m1,    m5,        [r3 + 17 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,        [r3 + 17 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,               m6
    movu          [r0 + 202 * 16],  m1

    ; mode 14 [row 11]
    pmaddubsw     m1,    m5,        [r3 + 4 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,        [r3 + 4 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,               m6
    movu          [r0 + 203 * 16],  m1

    ; mode 14 [row 12]
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 10],   1
    pinsrb        m5,    [r2 + 12],   0
    movu          m0,    [r2 + 3 + 32]
    psrldq        m6,    m0,          1
    punpcklbw     m0,    m6

    ; mode 15 [row 9 - second half]
    pmaddubsw     m1,    m0,             [r3 + 22 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 217 * 16 + 8],   m1
    ; mode 15 [row 9 - second half] end

    ; mode 15 [row 10 - second half]
    pmaddubsw     m1,    m0,             [r3 + 5 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 218 * 16 + 8],   m1
    ; mode 15 [row 10 - second half] end

    ; mode 16 [row 7 - second half]
    pmaddubsw     m1,    m0,             [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 231 * 16 + 8],   m1
    ; mode 16 [row 7 - second half] end

    ; mode 16 [row 8 - second half]
    pmaddubsw     m1,    m0,             [r3 + 3 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 232 * 16 + 8],   m1
    ; mode 16 [row 8 - second half] end

    pmaddubsw     m1,    m5,          [r3 + 23 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,          [r3 + 23 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,                 m6
    movu          [r0 + 204 * 16],    m1

    ; mode 14 [row 13]
    pmaddubsw     m1,    m5,          [r3 + 10 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,          [r3 + 10 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,                 m6
    movu          [r0 + 205 * 16],    m1

    ; mode 14 [row 14]
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 12],   1
    pinsrb        m5,    [r2 + 15],   0
    movu          m0,    [r2 + 2 + 32]
    psrldq        m6,    m0,          1
    punpcklbw     m0,    m6

    ; mode 15 [row 11 - second half]
    pmaddubsw     m1,    m0,             [r3 + 20 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 219 * 16 + 8],   m1
    ; mode 15 [row 11 - second half] end

    ; mode 15 [row 12 - second half]
    pmaddubsw     m1,    m0,             [r3 + 3 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 220 * 16 + 8],   m1
    ; mode 15 [row 12 - second half] end

    ; mode 16 [row 9 - second half]
    pmaddubsw     m1,    m0,             [r3 + 14 * 16]
    pmulhrsw      m1,    m3
    packuswb      m1,                    m1
    movh          [r0 + 233 * 16 + 8],   m1

    ; mode 16 [row 9 - second half] end
    pmaddubsw     m1,    m5,          [r3 + 29 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,          [r3 + 29 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,                 m6
    movu          [r0 + 206 * 16],    m1

    ; mode 14 [row 15]
    pmaddubsw     m1,    m5,          [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m6,    m0,          [r3 + 16 * 16]
    pmulhrsw      m6,    m3
    packuswb      m1,                 m6
    movu          [r0 + 207 * 16],    m1

    ; mode 12 [row 12]
    pslldq        m0,    m2,          2
    pinsrb        m0,    [r2 +  6],   1
    pinsrb        m0,    [r2 + 13],   0
    pmaddubsw     m1,    m0,          [r3 + 31 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 31 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 172 * 16],    m1

    ; mode 12 [row 13]
    pmaddubsw     m1,    m0,          [r3 + 26 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 26 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 173 * 16],    m1

    ; mode 12 [row 14]
    pmaddubsw     m1,    m0,          [r3 + 21 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 21 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 174 * 16],    m1

    ; mode 12 [row 15]
    pmaddubsw     m1,    m0,          [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 175 * 16],    m1

    ; mode 13 [row 7]
    pslldq        m7,    2
    pinsrb        m7,    [r2 +  4],   1
    pinsrb        m7,    [r2 +  7],   0
    pmaddubsw     m1,    m7,          [r3 + 24 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 24 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 183 * 16],    m1

    ; mode 13 [row 8]
    pmaddubsw     m1,    m7,          [r3 + 15 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 15 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 184 * 16],    m1

    ; mode 13 [row 9]
    pmaddubsw     m1,    m7,          [r3 + 6 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 6 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 185 * 16],    m1

    ; mode 13 [row 10]
    pslldq        m7,    2
    pinsrb        m7,    [r2 +  7],   1
    pinsrb        m7,    [r2 + 11],   0
    pmaddubsw     m1,    m7,          [r3 + 29 * 16]
    pmulhrsw      m1,    m3
    movu          m4,    [r2 + 5 + 32]
    psrldq        m5,    m4,         1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,          [r3 + 29 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,    m5
    movu          [r0 + 186 * 16],    m1

    ; mode 13 [row 11]
    pmaddubsw     m1,    m7,          [r3 + 20 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 20 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 187 * 16],    m1

    ; mode 13 [row 12]
    pmaddubsw     m1,    m7,          [r3 + 11 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 11 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 188 * 16],    m1

    ; mode 13 [row 13]
    pmaddubsw     m1,    m7,          [r3 + 2 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 2 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 189 * 16],    m1

    ; mode 13 [row 14]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 11],   1
    pinsrb        m7,    [r2 + 14],   0
    pmaddubsw     m1,    m7,          [r3 + 25 * 16]
    pmulhrsw      m1,    m3
    movu          m4,    [r2 + 4 + 32]
    psrldq        m5,    m4,          1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,          [r3 + 25 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 190 * 16],    m1

    ; mode 13 [row 15]
    pmaddubsw     m1,    m7,          [r3 + 16 * 16]
    pmulhrsw      m1,    m3
    pmaddubsw     m5,    m4,          [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m1,                 m5
    movu          [r0 + 191 * 16],    m1

    ; mode 17 [row 15]
    movu         m0,                   [r2]
    pshufb       m1,                   m0,       [tab_S1]
    movu         [r0 + 255 * 16],      m1
    movu         m2,                   [r2 + 32]
    pinsrb       m2,                   [r2], 0
    movd         [r0 + 255 * 16 + 12], m2

    ; mode 18 [row 0]
    movu         [r0 + 256 * 16],      m0

    ; mode 18 [row 1]
    pslldq        m4,              m0,         1
    pinsrb        m4,              [r2 + 1 + 32],   0
    movu          [r0 + 257 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 2 + 32],   0
    movu          [r0 + 258 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 3 + 32],   0
    movu          [r0 + 259 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 4 + 32],   0
    movu          [r0 + 260 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 5 + 32],   0
    movu          [r0 + 261 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 6 + 32],   0
    movu          [r0 + 262 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 7 + 32],   0
    movu          [r0 + 263 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 8 + 32],   0
    movu          [r0 + 264 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 9 + 32],   0
    movu          [r0 + 265 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 10 + 32],   0
    movu          [r0 + 266 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 11 + 32],   0
    movu          [r0 + 267 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 12 + 32],   0
    movu          [r0 + 268 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 13 + 32],   0
    movu          [r0 + 269 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 14 + 32],   0
    movu          [r0 + 270 * 16], m4
    pslldq        m4,              1
    pinsrb        m4,              [r2 + 15 + 32],   0
    movu          [r0 + 271 * 16], m4

    ; mode 19 [row 0]
    psrldq        m2,    m0,           1
    punpcklbw     m0,    m2
    movu          m5,    [r2 + 8]
    psrldq        m6,    m5,           1
    punpcklbw     m5,    m6
    pmaddubsw     m4,    m0,           [r3 + 6 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 6 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 272 * 16],     m4

    ; mode 20 [row 0]
    pmaddubsw     m4,    m0,           [r3 + 11 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 11 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 288 * 16],     m4

    ; mode 21 [row 0]
    pmaddubsw     m4,    m0,            [r3 + 15 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,            [r3 + 15 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 304 * 16],     m4

    ; mode 22 [row 0]
    pmaddubsw     m4,    m0,           [r3 + 19 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 19 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 320 * 16],     m4

    ; mode 22 [row 1]
    pmaddubsw     m4,    m0,           [r3 + 6 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 6 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 321 * 16],     m4

    ; mode 23 [row 0]
    pmaddubsw     m4,    m0,           [r3 + 23 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 23 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 336 * 16],     m4

    ; mode 23 [row 1]
    pmaddubsw     m4,    m0,           [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 14 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 337 * 16],     m4

    ; mode 23 [row 2]
    pmaddubsw     m4,    m0,           [r3 + 5 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 5 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 338 * 16],     m4

    ; mode 24 [row 0]
    pmaddubsw     m4,    m0,           [r3 + 27 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 27 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 352 * 16],     m4

    ; mode 24 [row 1]
    pmaddubsw     m4,    m0,            [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,            [r3 + 22 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 353 * 16],     m4

    ; mode 24 [row 2]
    pmaddubsw     m4,    m0,           [r3 + 17 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 17 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 354 * 16],     m4

    ; mode 24 [row 3]
    pmaddubsw     m4,    m0,           [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 12 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 355 * 16],     m4

    ; mode 24 [row 4]
    pmaddubsw     m4,    m0,            [r3 + 7 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,            [r3 + 7 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 356 * 16],     m4

    ; mode 24 [row 5]
    pmaddubsw     m4,    m0,           [r3 + 2 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,           [r3 + 2 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                  m6
    movu          [r0 + 357 * 16],     m4

    ; mode 24 [row 6 - first half]
    pslldq        m7,    m0,    2
    pinsrb        m7,    [r2 + 0],     1
    pinsrb        m7,    [r2 + 6 + 32],     0
    pmaddubsw     m4,    m7,           [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 358 * 16],     m4

    ; mode 24 [row 7 - first half]
    pmaddubsw     m4,    m7,           [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 359 * 16],     m4

    ; mode 24 [row 8 - first half]
    pmaddubsw     m4,    m7,           [r3 + 19 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 360 * 16],     m4

    ; mode 24 [row 9 - first half]
    pmaddubsw     m4,    m7,           [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 361 * 16],     m4

    ; mode 24 [row 10 - first half]
    pmaddubsw     m4,    m7,           [r3 + 9 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 362 * 16],     m4

    ; mode 24 [row 11 - first half]
    pmaddubsw     m4,    m7,           [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 363 * 16],     m4

    ; mode 24 [row 12 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 +  6 + 32],    1
    pinsrb        m7,    [r2 + 13 + 32],    0
    pmaddubsw     m4,    m7,           [r3 + 31 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 364 * 16],     m4

    ; mode 24 [row 13 - first half]
    pmaddubsw     m4,    m7,           [r3 + 26 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 365 * 16],     m4

    ; mode 24 [row 14 - first half]
    pmaddubsw     m4,    m7,           [r3 + 21 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 366 * 16],     m4

    ; mode 24 [row 15 - first half]
    pmaddubsw     m4,    m7,           [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 367 * 16],     m4

    ; mode 23 [row 3 - first half]
    pslldq        m7,    m0,    2
    pinsrb        m7,    [r2 + 0],     1
    pinsrb        m7,    [r2 + 4 + 32],     0
    pmaddubsw     m4,    m7,           [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 339 * 16],     m4

    ; mode 23 [row 4 - first half]
    pmaddubsw     m4,    m7,           [r3 + 19 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 340 * 16],     m4

    ; mode 23 [row 5 - first half]
    pmaddubsw     m4,    m7,           [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 341 * 16],     m4

    ; mode 23 [row 6 - first half]
    pmaddubsw     m4,    m7,           [r3 + 1 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 342 * 16],     m4

    ; mode 23 [row 7 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 4 + 32],     1
    pinsrb        m7,    [r2 + 7 + 32],     0
    pmaddubsw     m4,    m7,            [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 343 * 16],     m4

    ; mode 23 [row 8 - first half]
    pmaddubsw     m4,    m7,           [r3 + 15 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 344 * 16],     m4

    ; mode 23 [row 9 - first half]
    pmaddubsw     m4,    m7,           [r3 + 6 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 345 * 16],     m4

    ; mode 23 [row 10 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 +  7 + 32],    1
    pinsrb        m7,    [r2 + 11 + 32],    0
    pmaddubsw     m4,    m7,           [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 346 * 16],     m4

    ; mode 23 [row 11 - first half]
    pmaddubsw     m4,    m7,           [r3 + 20 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 347 * 16],     m4

    ; mode 23 [row 12 - first half]
    pmaddubsw     m4,    m7,           [r3 + 11 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 348 * 16],     m4

    ; mode 23 [row 13 - first half]
    pmaddubsw     m4,    m7,           [r3 + 2 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 349 * 16],     m4

    ; mode 23 [row 14 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 11 + 32],   1
    pinsrb        m7,    [r2 + 14 + 32],   0
    pmaddubsw     m4,    m7,           [r3 + 25 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 350 * 16],     m4

    ; mode 23 [row 15 - first half]
    pmaddubsw     m4,    m7,           [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 351 * 16],     m4

    ; mode 21 [row 15 - first half]
    pmaddubsw     m4,    m0,         [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 319 * 16 + 8], m4
    ; mode 21 [row 15 - second half] end

    ; mode 20 [row 1 - first half]
    pslldq        m7,    m0,    2
    pinsrb        m7,    [r2 + 0],   1
    pinsrb        m7,    [r2 + 2 + 32],   0
    pmaddubsw     m4,    m7,           [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 289 * 16],     m4

    ; mode 20 [row 2 - first half]
    pmaddubsw     m4,    m7,           [r3 + 1 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 290 * 16],     m4

    ; mode 21 [row 1 - first half]
    pmaddubsw     m4,    m7,           [r3 + 30 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 305 * 16],     m4

    ; mode 21 [row 2 - first half]
    pmaddubsw     m4,    m7,           [r3 + 13 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 306 * 16],     m4

    ; mode 22 [row 2 - first half]
    pmaddubsw     m4,    m7,           [r3 + 25 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 322 * 16],     m4

    ; mode 22 [row 3 - first half]
    pmaddubsw     m4,    m7,           [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 323 * 16],     m4

    ; mode 22 [row 4 - first half]
    pslldq        m1,    m7,    2
    pinsrb        m1,    [r2 + 2 + 32],     1
    pinsrb        m1,    [r2 + 5 + 32],     0
    pmaddubsw     m4,    m1,           [r3 + 31 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 324 * 16],     m4

    ; mode 22 [row 5 - first half]
    pmaddubsw     m4,    m1,           [r3 + 18 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 325 * 16],     m4

    ; mode 22 [row 6 - first half]
    pmaddubsw     m4,    m1,           [r3 + 5 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 326 * 16],     m4

    ; mode 22 [row 7 - first half]
    pslldq        m1,    2
    pinsrb        m1,    [r2 + 5 + 32],     1
    pinsrb        m1,    [r2 + 7 + 32],     0
    pmaddubsw     m4,    m1,           [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 327 * 16],     m4

    ; mode 22 [row 8 - first half]
    pmaddubsw     m4,    m1,           [r3 + 11 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 328 * 16],     m4

    ; mode 22 [row 9 - first half]
    pslldq        m1,    2
    pinsrb        m1,    [r2 +  7 + 32],    1
    pinsrb        m1,    [r2 + 10 + 32],    0
    pmaddubsw     m4,    m1,           [r3 + 30 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 329 * 16],     m4

    ; mode 22 [row 10 - first half]
    pmaddubsw     m4,    m1,           [r3 + 17 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 330 * 16],     m4

    ; mode 22 [row 11 - first half]
    pmaddubsw     m4,    m1,           [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 331 * 16],     m4

    ; mode 22 [row 12 - first half]
    pslldq        m1,    2
    pinsrb        m1,    [r2 + 10 + 32],    1
    pinsrb        m1,    [r2 + 12 + 32],    0
    pmaddubsw     m4,    m1,           [r3 + 23 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 332 * 16],     m4

    ; mode 22 [row 13 - first half]
    pmaddubsw     m4,    m1,           [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 333 * 16],     m4

    ; mode 22 [row 14 - first half]
    pslldq        m1,    2
    pinsrb        m1,    [r2 + 12 + 32],   1
    pinsrb        m1,    [r2 + 15 + 32],   0
    pmaddubsw     m4,    m1,          [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 334 * 16],     m4

    ; mode 22 [row 15 - first half]
    pmaddubsw     m4,    m1,           [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 335 * 16],     m4

    ; mode 21 [row 3 - first half]
    pslldq        m6,    m7,    2
    pinsrb        m6,    [r2 + 2 + 32],     1
    pinsrb        m6,    [r2 + 4 + 32],     0
    pmaddubsw     m4,    m6,           [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 307 * 16],     m4

    ; mode 21 [row 4 - first half]
    pmaddubsw     m4,    m6,            [r3 + 11 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 308 * 16],     m4

    ; mode 21 [row 5 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 4 + 32],     1
    pinsrb        m6,    [r2 + 6 + 32],     0
    pmaddubsw     m4,    m6,           [r3 + 26 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 309 * 16],     m4

    ; mode 21 [row 6 - first half]
    pmaddubsw     m4,    m6,           [r3 + 9 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 310 * 16],     m4

    ; mode 21 [row 7 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 6 + 32],     1
    pinsrb        m6,    [r2 + 8 + 32],     0
    pmaddubsw     m4,    m6,           [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 311 * 16],     m4

    ; mode 21 [row 8 - first half]
    pmaddubsw     m4,    m6,           [r3 + 7 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 312 * 16],     m4

    ; mode 21 [row 9 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 8 + 32],     1
    pinsrb        m6,    [r2 + 9 + 32],     0
    pmaddubsw     m4,    m6,            [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 313 * 16],     m4

    ; mode 21 [row 10 - first half]
    pmaddubsw     m4,    m6,            [r3 + 5 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 314 * 16],     m4

    ; mode 21 [row 11 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 +  9 + 32],    1
    pinsrb        m6,    [r2 + 11 + 32],    0
    pmaddubsw     m4,    m6,           [r3 + 20 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 315 * 16],     m4

    ; mode 21 [row 12 - first half]
    pmaddubsw     m4,    m6,           [r3 + 3 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 316 * 16],     m4

    ; mode 21 [row 13 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 11 + 32],    1
    pinsrb        m6,    [r2 + 13 + 32],    0
    pmaddubsw     m4,    m6,           [r3 + 18 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 317 * 16],     m4

    ; mode 21 [row 14 - first half]
    pmaddubsw     m4,    m6,           [r3 + 1 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 318 * 16],     m4

    ; mode 21 [row 15 - first half]
    pslldq        m6,    2
    pinsrb        m6,    [r2 + 32 + 13],    1
    pinsrb        m6,    [r2 + 32 + 15],    0
    pmaddubsw     m4,    m6,           [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 319 * 16],     m4

    ; mode 20 [row 13 - second half]
    pmaddubsw     m4,    m7,           [r3 + 26 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 301 * 16 + 8], m4
    ; mode 20 [row 13 - second half]

    ; mode 20 [row 14 - second half]
    pmaddubsw     m4,    m7,           [r3 + 5 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 302 * 16 + 8], m4
    ; mode 20 [row 14 - second half]

    ; mode 20 [row 3 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 + 2],    1
    pinsrb        m7,    [r2 + 32 + 3],    0
    pmaddubsw     m4,    m7,           [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 291 * 16],     m4

    ; mode 20 [row 15 - second half]
    pmaddubsw     m4,    m7,           [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 303 * 16 + 8], m4
    ; mode 20 [row 15 - second half]

    ; mode 20 [row 4 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 + 3],     1
    pinsrb        m7,    [r2 + 32 + 5],     0
    pmaddubsw     m4,    m7,           [r3 + 23 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 292 * 16],     m4

    ; mode 20 [row 5 - first half]
    pmaddubsw     m4,    m7,           [r3 + 2 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 293 * 16],     m4

    ; mode 20 [row 6 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 + 5],     1
    pinsrb        m7,    [r2 + 32 + 6],     0
    pmaddubsw     m4,    m7,           [r3 + 13 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 294 * 16],     m4

    ; mode 20 [row 7 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 + 6],   1
    pinsrb        m7,    [r2 + 32 + 8],   0
    pmaddubsw     m4,    m7,           [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 295 * 16],     m4

    ; mode 20 [row 8 - first half]
    pmaddubsw     m4,    m7,           [r3 + 3 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 296 * 16],     m4

    ; mode 20 [row 9 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 + 8],   1
    pinsrb        m7,    [r2 + 32 + 9],   0
    pmaddubsw     m4,    m7,           [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 297 * 16],     m4

    ; mode 20 [row 10 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 +  9],   1
    pinsrb        m7,    [r2 + 32 + 11],   0
    pmaddubsw     m4,    m7,           [r3 + 25 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 298 * 16],     m4

    ; mode 20 [row 11 - first half]
    pmaddubsw     m4,    m7,           [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 299 * 16],     m4

    ; mode 20 [row 12 - first half]
    movu          m1,    [r3 + 15 * 16]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 + 11],   1
    pinsrb        m7,    [r2 + 32 + 12],   0
    pmaddubsw     m4,    m7,           [r3 + 15 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 300 * 16],     m4

    ; mode 20 [row 13 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 + 12],   1
    pinsrb        m7,    [r2 + 32 + 14],   0
    pmaddubsw     m4,    m7,           [r3 + 26 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 301 * 16],     m4

    ; mode 20 [row 14 - first half]
    pmaddubsw     m4,    m7,           [r3 + 5 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 302 * 16],     m4

    ; mode 20 [row 15 - first half]
    pslldq        m7,    2
    pinsrb        m7,    [r2 + 32 + 14],    1
    pinsrb        m7,    [r2 + 32 + 15],    0
    pmaddubsw     m4,    m7,           [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 303 * 16],     m4

    ; mode 19 [row 1]
    pslldq        m0,    2
    pinsrb        m0,    [r2],            1
    pinsrb        m0,    [r2 + 32 + 1],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 8],   1
    pinsrb        m5,    [r2 + 7],   0

    ; mode 20 [row 1 - second half]
    pmaddubsw     m4,    m5,           [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 289 * 16 + 8], m4
    ; mode 20 [row 1 - second half] end

    ; mode 20 [row 2 - second half]
    pmaddubsw     m4,    m5,           [r3 + 1 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 290 * 16 + 8], m4
    ; mode 20 [row 2 - second half] end

    ; mode 21 [row 2 - second half]
    pmaddubsw     m4,    m5,           [r3 + 30 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 305 * 16 + 8], m4
    ; mode 21 [row 2 - second half] end

    ; mode 21 [row 3 - second half]
    pmaddubsw     m4,    m5,           [r3 + 13 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 306 * 16 + 8], m4
    ; mode 21 [row 3 - second half] end

    ; mode 21 [row 4 - second half]
    pmaddubsw     m4,    m5,           [r3 + 11 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 307 * 16 + 8], m4
    ; mode 21 [row 4 - second half] end

    ; mode 22 [row 2 - second half]
    pmaddubsw     m4,    m5,           [r3 + 25 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 322 * 16 + 8], m4
    ; mode 22 [row 2 - second half] end

    ; mode 22 [row 3 - second half]
    pmaddubsw     m4,    m5,           [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 323 * 16 + 8], m4
    ; mode 22 [row 3 - second half] end

    ; mode 23 [row 3 - second half]
    pmaddubsw     m4,    m5,          [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 339 * 16 + 8], m4
    ; mode 23 [row 3 - second half] end

    ; mode 23 [row 4 - second half]
    pmaddubsw     m4,    m5,          [r3 + 19 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 340 * 16 + 8], m4
    ; mode 23 [row 4 - second half] end

    ; mode 23 [row 5 - second half]
    pmaddubsw     m4,    m5,          [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 341 * 16 + 8], m4
    ; mode 23 [row 5 - second half] end

    ; mode 23 [row 6 - second half]
    pmaddubsw     m4,    m5,          [r3 + 1 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 342 * 16 + 8], m4
    ; mode 23 [row 6 - second half] end

    ; mode 24 [row 6 - second half]
    pmaddubsw     m4,    m5,           [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 358 * 16 + 8], m4
    ; mode 24 [row 6 - second half] end

    ; mode 24 [row 7 - second half]
    pmaddubsw     m4,    m5,           [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 359 * 16 + 8], m4
    ; mode 24 [row 7 - second half] end

    ; mode 24 [row 8 - second half]
    pmaddubsw     m4,    m5,           [r3 + 19 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 360 * 16 + 8], m4
    ; mode 24 [row 8 - second half] end

    ; mode 24 [row 9 - second half]
    pmaddubsw     m4,    m5,           [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 361 * 16 + 8], m4
    ; mode 24 [row 9 - second half] end

    ; mode 24 [row 10 - second half]
    pmaddubsw     m4,    m5,           [r3 + 9 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 362 * 16 + 8], m4
    ; mode 24 [row 10 - second half] end

    ; mode 24 [row 11 - second half]
    pmaddubsw     m4,    m5,           [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 363 * 16 + 8], m4
    ; mode 24 [row 11 - second half] end

    pmaddubsw     m4,    m0,         [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 12 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 273 * 16],   m4

    ; mode 19 [row 2]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 1],   1
    pinsrb        m0,    [r2 + 32 + 2],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 7],   1
    pinsrb        m5,    [r2 + 6],   0

    ; mode 20 [row 3 - second half]
    pmaddubsw     m4,    m5,            [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                   m4
    movh          [r0 + 291 * 16 + 8], m4
    ; mode 20 [row 3 - second half] end

    ; mode 21 [row 3 - second half]
    pmaddubsw     m4,    m5,           [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 307 * 16 + 8], m4
    ; mode 21 [row 3 - second half] end

    ; mode 21 [row 4 - second half]
    pmaddubsw     m4,    m5,           [r3 + 11 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 308 * 16 + 8], m4
    ; mode 21 [row 4 - second half] end

    ; mode 22 [row 4 - second half]
    pmaddubsw     m4,    m5,           [r3 + 31 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 324 * 16 + 8], m4
    ; mode 22 [row 4 - second half] end

    ; mode 22 [row 5 - second half]
    pmaddubsw     m4,    m5,           [r3 + 18 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 325 * 16 + 8], m4
    ; mode 22 [row 5 - second half] end

    ; mode 22 [row 6 - second half]
    pmaddubsw     m4,    m5,           [r3 + 5 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 326 * 16 + 8], m4
    ; mode 22 [row 6 - second half] end

    ; mode 23 [row 7 - second half]
    pmaddubsw     m4,    m5,           [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 343 * 16 + 8], m4
    ; mode 23 [row 7 - second half] end

    ; mode 23 [row 8 - second half]
    pmaddubsw     m4,    m5,           [r3 + 15 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 344 * 16 + 8], m4
    ; mode 23 [row 8 - second half] end

    ; mode 23 [row 9 - second half]
    pmaddubsw     m4,    m5,           [r3 + 6 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 345 * 16 + 8], m4
    ; mode 23 [row 9 - second half] end

    ; mode 24 [row 12 - second half]
    pmaddubsw     m4,    m5,          [r3 + 31 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 364 * 16 + 8], m4
    ; mode 24 [row 12 - second half] end

    ; mode 24 [row 13 - second half]
    pmaddubsw     m4,    m5,          [r3 + 26 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 365 * 16 + 8], m4
    ; mode 24 [row 13 - second half] end

    ; mode 24 [row 14 - second half]
    pmaddubsw     m4,    m5,          [r3 + 21 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 366 * 16 + 8], m4
    ; mode 24 [row 14 - second half] end

    ; mode 24 [row 15 - second half]
    pmaddubsw     m4,    m5,          [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 367 * 16 + 8], m4
    ; mode 24 [row 15 - second half] end

    pmaddubsw     m4,    m0,         [r3 + 18 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 18 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 274 * 16],   m4

    ; mode 19 [row 3]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 2],   1
    pinsrb        m0,    [r2 + 32 + 4],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 6],   1
    pinsrb        m5,    [r2 + 5],   0

    ; mode 20 [row 4 - second half]
    pmaddubsw     m4,    m5,           [r3 + 23 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 292 * 16 + 8], m4
    ; mode 20 [row 4 - second half] end

    ; mode 20 [row 5 - second half]
    pmaddubsw     m4,    m5,           [r3 + 2 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 293 * 16 + 8], m4
    ; mode 20 [row 5 - second half] end

    ; mode 21 [row 5 - second half]
    pmaddubsw     m4,    m5,          [r3 + 26 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 309 * 16 + 8], m4
    ; mode 21 [row 5 - second half] end

    ; mode 21 [row 6 - second half]
    pmaddubsw     m4,    m5,          [r3 + 9 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 310 * 16 + 8], m4
    ; mode 21 [row 6 - second half] end

    ; mode 22 [row 7 - second half]
    pmaddubsw     m4,    m5,           [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 327 * 16 + 8], m4
    ; mode 22 [row 7 - second half] end

    ; mode 22 [row 8 - second half]
    pmaddubsw     m4,    m5,           [r3 + 11 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 328 * 16 + 8], m4
    ; mode 22 [row 7 - second half] end

    ; mode 23 [row 10 - second half]
    pmaddubsw     m4,    m5,           [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 346 * 16 + 8], m4
    ; mode 23 [row 10 - second half] end

    ; mode 23 [row 11 - second half]
    pmaddubsw     m4,    m5,           [r3 + 20 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 347 * 16 + 8], m4
    ; mode 23 [row 11 - second half] end

    ; mode 23 [row 12 - second half]
    pmaddubsw     m4,    m5,           [r3 + 11 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 348 * 16 + 8], m4
    ; mode 23 [row 12 - second half] end

    ; mode 23 [row 13 - second half]
    pmaddubsw     m4,    m5,           [r3 + 2 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 349 * 16 + 8], m4
    ; mode 23 [row 13 - second half] end

    pmaddubsw     m4,    m0,         [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 24 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 275 * 16],   m4

    ; mode 19 [row 4]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 4],   1
    pinsrb        m0,    [r2 + 32 + 5],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 5],   1
    pinsrb        m5,    [r2 + 4],   0

    ; mode 20 [row 6 - second half]
    pmaddubsw     m4,    m5,           [r3 + 13 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 294 * 16 + 8], m4
    ; mode 20 [row 6 - second half] end

    ; mode 21 [row 7 - second half]
    pmaddubsw     m4,    m5,          [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 311 * 16 + 8], m4
    ; mode 21 [row 7 - second half] end

    ; mode 21 [row 8 - second half]
    pmaddubsw     m4,    m5,          [r3 + 7 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 312 * 16 + 8], m4
    ; mode 21 [row 8 - second half] end

    ; mode 22 [row 9 - second half]
    pmaddubsw     m4,    m5,           [r3 + 30 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 329 * 16 + 8], m4
    ; mode 22 [row 9 - second half] end

    ; mode 22 [row 10 - second half]
    pmaddubsw     m4,    m5,           [r3 + 17 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 330 * 16 + 8], m4
    ; mode 22 [row 10 - second half] end

    ; mode 22 [row 11 - second half]
    pmaddubsw     m4,    m5,           [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 331 * 16 + 8], m4
    ; mode 22 [row 11 - second half] end

    ; mode 23 [row 14 - second half]
    pmaddubsw     m4,    m5,           [r3 + 25 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 350 * 16 + 8], m4
    ; mode 23 [row 14 - second half] end

    ; mode 23 [row 15 - second half]
    pmaddubsw     m4,    m5,           [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 351 * 16 + 8], m4

    ; mode 23 [row 15 - second half] end
    pmaddubsw     m4,    m0,         [r3 + 30 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 30 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 276 * 16],   m4

    ; mode 19 [row 5]
    pmaddubsw     m4,    m0,         [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 4 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 277 * 16],   m4

    ; mode 19 [row 6]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 5],   1
    pinsrb        m0,    [r2 + 32 + 6],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 4],   1
    pinsrb        m5,    [r2 + 3],   0

    ; mode 20 [row 7 - second half]
    pmaddubsw     m4,    m5,           [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 295 * 16 + 8], m4
    ; mode 20 [row 7 - second half] end

    ; mode 20 [row 8 - second half]
    pmaddubsw     m4,    m5,           [r3 + 3 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 296 * 16 + 8], m4
    ; mode 20 [row 8 - second half] end

    ; mode 21 [row 9 - second half]
    pmaddubsw     m4,    m5,           [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 313 * 16 + 8], m4
    ; mode 21 [row 9 - second half] end

    ; mode 21 [row 10 - second half]
    pmaddubsw     m4,    m5,           [r3 + 5 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 314 * 16 + 8], m4
    ; mode 21 [row 10 - second half] end

    ; mode 22 [row 12 - second half]
    pmaddubsw     m4,    m5,           [r3 + 23 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 332 * 16 + 8], m4
    ; mode 22 [row 12 - second half] end

    ; mode 22 [row 12 - second half]
    pmaddubsw     m4,    m5,           [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 333 * 16 + 8], m4
    ; mode 22 [row 12 - second half] end

    pmaddubsw     m4,    m0,          [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,          [r3 + 10 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 278 * 16],   m4

    ; mode 19 [row 7]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 6],   1
    pinsrb        m0,    [r2 + 32 + 7],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 3],   1
    pinsrb        m5,    [r2 + 2],   0

    ; mode 20 [row 9 - second half]
    pmaddubsw     m4,    m5,           [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 297 * 16 + 8], m4
    ; mode 20 [row 9 - second half]

    ; mode 21 [row 11 - second half]
    pmaddubsw     m4,    m5,           [r3 + 20 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 315 * 16 + 8], m4
    ; mode 21 [row 11 - second half] end

    ; mode 21 [row 12 - second half]
    pmaddubsw     m4,    m5,           [r3 + 3 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 316 * 16 + 8], m4
    ; mode 21 [row 12 - second half] end

    ; mode 22 [row 14 - second half]
    pmaddubsw     m4,    m5,           [r3 + 29 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 334 * 16 + 8], m4
    ; mode 22 [row 14 - second half] end

    ; mode 22 [row 15 - second half]
    pmaddubsw     m4,    m5,           [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 335 * 16 + 8], m4
    ; mode 22 [row 15 - second half] end

    pmaddubsw     m4,    m0,         [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 16 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 279 * 16],   m4

    ; mode 19 [row 8]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 7],   1
    pinsrb        m0,    [r2 + 32 + 9],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 2],   1
    pinsrb        m5,    [r2 + 1],   0

    ; mode 20 [row 10 - second half]
    pmaddubsw     m4,    m5,           [r3 + 25 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 298 * 16 + 8], m4
    ; mode 20 [row 10 - second half] end

    ; mode 20 [row 11 - second half]
    pmaddubsw     m4,    m5,           [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 299 * 16 + 8], m4
    ; mode 20 [row 11 - second half] end

    ; mode 21 [row 13 - second half]
    pmaddubsw     m4,    m5,           [r3 + 18 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 317 * 16 + 8], m4
    ; mode 21 [row 13 - second half] end

    ; mode 21 [row 14 - second half]
    pmaddubsw     m4,    m5,           [r3 + 1 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 318 * 16 + 8], m4
    ; mode 21 [row 14 - second half] end

    pmaddubsw     m4,    m0,         [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 22 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 280 * 16],   m4

    ; mode 19 [row 9]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 9],   1
    pinsrb        m0,    [r2 + 32 + 10],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 1],   1
    pinsrb        m5,    [r2 + 0],   0

    ; mode 20 [row 12 - second half]
    pmaddubsw     m4,    m5,           [r3 + 15 * 16]
    pmulhrsw      m4,    m3
    packuswb      m4,                  m4
    movh          [r0 + 300 * 16 + 8], m4

    ; mode 20 [row 12 - second half] end
    pmaddubsw     m4,    m0,          [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,          [r3 + 28 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 281 * 16],   m4

    ; mode 19 [row 10]
    pmaddubsw     m4,    m0,         [r3 + 2 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 2 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 282 * 16],   m4

    ; mode 19 [row 11]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 10],   1
    pinsrb        m0,    [r2 + 32 + 11],   0
    pmaddubsw     m4,    m0,         [r3 + 8 * 16]
    pmulhrsw      m4,    m3
    pslldq        m5,    2
    pinsrb        m5,    [r2],            1
    pinsrb        m5,    [r2 + 32 + 1],   0
    pmaddubsw     m6,    m5,         [r3 + 8 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 283 * 16],   m4

    ; mode 19 [row 12]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 11],   1
    pinsrb        m0,    [r2 + 32 + 12],   0
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 32 + 1],   1
    pinsrb        m5,    [r2 + 32 + 2],   0
    pmaddubsw     m4,    m0,         [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m6,    m5,         [r3 + 14 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 284 * 16],   m4

    ; mode 19 [row 13]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 12],   1
    pinsrb        m0,    [r2 + 32 + 14],   0
    pmaddubsw     m4,    m0,         [r3 + 20 * 16]
    pmulhrsw      m4,    m3
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 32 + 2],   1
    pinsrb        m5,    [r2 + 32 + 4],   0
    pmaddubsw     m6,    m5,         [r3 + 20 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 285 * 16],   m4

    ; mode 19 [row 14]
    pslldq        m0,    2
    pinsrb        m0,    [r2 + 32 + 14],   1
    pinsrb        m0,    [r2 + 32 + 15],   0
    pmaddubsw     m4,    m0,         [r3 + 26 * 16]
    pmulhrsw      m4,    m3
    pslldq        m5,    2
    pinsrb        m5,    [r2 + 32 + 4],   1
    pinsrb        m5,    [r2 + 32 + 5],   0
    pmaddubsw     m6,    m5,         [r3 + 26 * 16]
    pmulhrsw      m6,    m3
    packuswb      m4,                m6
    movu          [r0 + 286 * 16],   m4

    ; mode 19 [row 15]
    movu         m0,                   [r2 + 32]
    pshufb       m0,                   [tab_S1]
    movu         [r0 + 287 * 16],      m0
    movd         m1,                   [r2]
    movd         [r0 + 287 * 16 + 12], m1

    ; mode 25
    movu          m1,    [r1]

    ; mode 26 [all rows]
    psrldq        m6,    m1,         1
    pinsrb        m6,    [r1 + 16], 15
    movu          m7,    m6
    movu         [r0 + 384 * 16],   m6
    movu         [r0 + 385 * 16],   m6
    movu         [r0 + 386 * 16],   m6
    movu         [r0 + 387 * 16],   m6
    movu         [r0 + 388 * 16],   m6
    movu         [r0 + 389 * 16],   m6
    movu         [r0 + 390 * 16],   m6
    movu         [r0 + 391 * 16],   m6
    movu         [r0 + 392 * 16],   m6
    movu         [r0 + 393 * 16],   m6
    movu         [r0 + 394 * 16],   m6
    movu         [r0 + 395 * 16],   m6
    movu         [r0 + 396 * 16],   m6
    movu         [r0 + 397 * 16],   m6
    movu         [r0 + 398 * 16],   m6
    movu         [r0 + 399 * 16],   m6

    pxor         m0,          m0
    pshufb       m6,          m6,         m0
    punpcklbw    m6,          m0
    pinsrb       m2,          [r1], 0
    pshufb       m2,          m2,         m0
    punpcklbw    m2,          m0
    movu         m4,          [r1 + 1 + 32]
    punpcklbw    m5,          m4,         m0
    punpckhbw    m4,          m0
    psubw        m5,          m2
    psubw        m4,          m2
    psraw        m5,          1
    psraw        m4,          1
    paddw        m5,          m6
    paddw        m4,          m6
    packuswb     m5,          m4

    pextrb       [r0 + 384 * 16],  m5,          0
    pextrb       [r0 + 385 * 16],  m5,          1
    pextrb       [r0 + 386 * 16],  m5,          2
    pextrb       [r0 + 387 * 16],  m5,          3
    pextrb       [r0 + 388 * 16],  m5,          4
    pextrb       [r0 + 389 * 16],  m5,          5
    pextrb       [r0 + 390 * 16],  m5,          6
    pextrb       [r0 + 391 * 16],  m5,          7
    pextrb       [r0 + 392 * 16],  m5,          8
    pextrb       [r0 + 393 * 16],  m5,          9
    pextrb       [r0 + 394 * 16],  m5,          10
    pextrb       [r0 + 395 * 16],  m5,          11
    pextrb       [r0 + 396 * 16],  m5,          12
    pextrb       [r0 + 397 * 16],  m5,          13
    pextrb       [r0 + 398 * 16],  m5,          14
    pextrb       [r0 + 399 * 16],  m5,          15

    ; mode 25 [row 15]
    movu          [r0 + 383 * 16],     m1

    ; mode 25 [row 0]
    psrldq        m2,    m1,           1
    punpcklbw     m1,    m2
    movu          m2,    [r1 + 8]
    psrldq        m4,    m2,           1
    punpcklbw     m2,    m4
    pmaddubsw     m4,    m1,           [r3 + 30 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,           [r3 + 30 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 368 * 16],     m4

    ; mode 25 [row 1]
    pmaddubsw     m4,    m1,            [r3 + 28 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,            [r3 + 28 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 369 * 16],     m4

    ; mode 25 [row 2]
    pmaddubsw     m4,    m1,            [r3 + 26 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,            [r3 + 26 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 370 * 16],     m4

    ; mode 25 [row 3]
    pmaddubsw     m4,    m1,            [r3 + 24 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,            [r3 + 24 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 371 * 16],     m4

    ; mode 25 [row 4]
    pmaddubsw     m4,    m1,           [r3 + 22 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,           [r3 + 22 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 372 * 16],     m4

    ; mode 25 [row 5]
    pmaddubsw     m4,    m1,           [r3 + 20 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,           [r3 + 20 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 373 * 16],     m4

    ; mode 25 [row 6]
    pmaddubsw     m4,    m1,            [r3 + 18 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,            [r3 + 18 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 374 * 16],     m4

    ; mode 25 [row 7]
    pmaddubsw     m4,    m1,            [r3 + 16 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,            [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 375 * 16],     m4

    ; mode 25 [row 8]
    pmaddubsw     m4,    m1,           [r3 + 14 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,           [r3 + 14 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 376 * 16],     m4

    ; mode 25 [row 9]
    pmaddubsw     m4,    m1,            [r3 + 12 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,            [r3 + 12 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 377 * 16],     m4

    ; mode 25 [row 10]
    pmaddubsw     m4,    m1,           [r3 + 10 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,           [r3 + 10 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 378 * 16],     m4

    ; mode 25 [row 11]
    pmaddubsw     m4,    m1,             [r3 + 8 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,             [r3 + 8 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 379 * 16],     m4

    ; mode 25 [row 12]
    pmaddubsw     m4,    m1,            [r3 + 6 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,            [r3 + 6 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 380 * 16],     m4

    ; mode 25 [row 13]
    pmaddubsw     m4,    m1,           [r3 + 4 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,           [r3 + 4 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 381 * 16],     m4

    ; mode 25 [row 14]
    pmaddubsw     m4,    m1,           [r3 + 2 * 16]
    pmulhrsw      m4,    m3
    pmaddubsw     m5,    m2,           [r3 + 2 * 16]
    pmulhrsw      m5,    m3
    packuswb      m4,                  m5
    movu          [r0 + 382 * 16],     m4

    ; mode 27 [row 15]
    psrldq        m6,    m7,           1
    punpcklbw     m7,    m6
    pinsrb        m6,    [r1 + 17],    15
    movu          [r0 + 415 * 16],     m6

    ; mode 27 [row 0]
    movu          m4,    [r1 + 9]
    psrldq        m5,    m4,           1
    punpcklbw     m4,    m5
    pmaddubsw     m6,    m7,           [r3 + 2 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 2 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 400 * 16],     m6

    ; mode 27 [row 1]
    pmaddubsw     m6,    m7,           [r3 + 4 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 4 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 401 * 16],     m6

    ; mode 27 [row 2]
    pmaddubsw     m6,    m7,           [r3 + 6 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 6 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 402 * 16],     m6

    ; mode 27 [row 3]
    pmaddubsw     m6,    m7,           [r3 + 8 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 8 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 403 * 16],     m6

    ; mode 27 [row 4]
    pmaddubsw     m6,    m7,           [r3 + 10 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 10 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 404 * 16],     m6

    ; mode 27 [row 5]
    pmaddubsw     m6,    m7,           [r3 + 12 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 12 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 405 * 16],     m6

    ; mode 27 [row 6]
    pmaddubsw     m6,    m7,           [r3 + 14 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 14 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 406 * 16],     m6

    ; mode 27 [row 7]
    pmaddubsw     m6,    m7,           [r3 + 16 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 407 * 16],     m6

    ; mode 27 [row 8]
    pmaddubsw     m6,    m7,            [r3 + 18 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,            [r3 + 18 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 408 * 16],     m6

    ; mode 27 [row 9]
    pmaddubsw     m6,    m7,           [r3 + 20 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 20 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 409 * 16],     m6

    ; mode 27 [row 10]
    pmaddubsw     m6,    m7,           [r3 + 22 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 22 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 410 * 16],     m6

    ; mode 27 [row 11]
    pmaddubsw     m6,    m7,           [r3 + 24 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 24 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 411 * 16],     m6

    ; mode 27 [row 12]
    pmaddubsw     m6,    m7,           [r3 + 26 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 26 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 412 * 16],     m6

    ; mode 27 [row 13]
    pmaddubsw     m6,    m7,           [r3 + 28 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 28 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 413 * 16],     m6

    ; mode 27 [row 14]
    pmaddubsw     m6,    m7,           [r3 + 30 * 16]
    pmulhrsw      m6,    m3
    pmaddubsw     m5,    m4,           [r3 + 30 * 16]
    pmulhrsw      m5,    m3
    packuswb      m6,                  m5
    movu          [r0 + 414 * 16],     m6

    ; mode 28 [row 0]
    movu          m1,    [r2 + 1]
    psrldq        m2,    m1,           1
    punpcklbw     m1,    m2
    movu          m4,    [r2 + 9]
    psrldq        m5,    m4,           1
    punpcklbw     m4,    m5
    pmaddubsw     m2,    m1,           [r3 + 5 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 5 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 416 * 16],     m2

    ; mode 28 [row 0]
    pmaddubsw     m2,    m1,            [r3 + 5 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,            [r3 + 5 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 416 * 16],     m2

    ; mode 28 [row 1]
    pmaddubsw     m2,    m1,           [r3 + 10 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 10 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 417 * 16],     m2

    ; mode 28 [row 2]
    pmaddubsw     m2,    m1,            [r3 + 15 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,            [r3 + 15 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 418 * 16],     m2

    ; mode 28 [row 3]
    pmaddubsw     m2,    m1,           [r3 + 20 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 20 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 419 * 16],     m2

    ; mode 28 [row 4]
    pmaddubsw     m2,    m1,           [r3 + 25 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 25 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 420 * 16],     m2

    ; mode 28 [row 5]
    pmaddubsw     m2,    m1,           [r3 + 30 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 30 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 421 * 16],     m2

    ; mode 29 [row 0]
    pmaddubsw     m2,    m1,           [r3 + 9 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 9 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 432 * 16],     m2

    ; mode 29 [row 1]
    pmaddubsw     m2,    m1,           [r3 + 18 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 18 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 433 * 16],     m2

    ; mode 29 [row 2]
    pmaddubsw     m2,    m1,           [r3 + 27 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 27 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 434 * 16],     m2

    ; mode 30 [row 0]
    pmaddubsw     m2,    m1,           [r3 + 13 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 13 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 448 * 16],     m2

    ; mode 30 [row 1]
    pmaddubsw     m2,    m1,           [r3 + 26 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 26 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 449 * 16],     m2

    ; mode 33 [row 0]
    movu     [r0 + 496 * 16],     m2

    ; mode 31 [row 0]
    pmaddubsw     m2,    m1,           [r3 + 17 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 17 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 464 * 16],     m2

    ; mode 32 [row 0]
    pmaddubsw     m2,    m1,           [r3 + 21 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 21 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                  m5
    movu          [r0 + 480 * 16],     m2

    ; mode 28 [row 6]
    movd          m7,    [r2 + 9]
    palignr       m7,    m1,          2
    pmaddubsw     m2,    m7,          [r3 + 3 * 16]
    pmulhrsw      m2,    m3
    movd          m6,    [r2 + 17]
    palignr       m6,    m4,         2
    pmaddubsw     m5,    m6,          [r3 + 3 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,    m5
    movu          [r0 + 422 * 16],   m2

    ; mode 28 [row 7]
    pmaddubsw     m2,    m7,         [r3 + 8 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 8 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 423 * 16],   m2

    ; mode 28 [row 8]
    pmaddubsw     m2,    m7,         [r3 + 13 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 13 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 424 * 16],   m2

    ; mode 28 [row 9]
    pmaddubsw     m2,    m7,         [r3 + 18 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 18 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 425 * 16],   m2

    ; mode 28 [row 10]
    pmaddubsw     m2,    m7,         [r3 + 23 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 23 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 426 * 16],   m2

    ; mode 29 [row 3]
    pmaddubsw     m2,    m7,         [r3 + 4 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 4 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 435 * 16],   m2

    ; mode 29 [row 4]
    pmaddubsw     m2,    m7,         [r3 + 13 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 13 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 436 * 16],   m2

    ; mode 29 [row 5]
    pmaddubsw     m2,    m7,         [r3 + 22 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 22 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 437 * 16],   m2

    ; mode 29 [row 6]
    pmaddubsw     m2,    m7,         [r3 + 31 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 31 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 438 * 16],   m2

    ; mode 32 [row 2]
    movu          [r0 + 482 * 16],   m2

    ; mode 30 [row 2]
    pmaddubsw     m2,    m7,         [r3 + 7 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 7 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 450 * 16],   m2

    ; mode 30 [row 3]
    pmaddubsw     m2,    m7,         [r3 + 20 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 20 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 451 * 16],   m2

    ; mode 33 [row 1]
    movu          [r0 + 497 * 16],   m2

    ; mode 31 [row 1]
    pmaddubsw     m2,    m7,         [r3 + 2 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 2 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 465 * 16],   m2

    ; mode 31 [row 2]
    pmaddubsw     m2,    m7,         [r3 + 19 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 19 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 466 * 16],   m2

    ; mode 32 [row 1]
    pmaddubsw     m2,    m7,         [r3 + 10 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 10 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 481 * 16],   m2

    ; mode 28 [row 11]
    pmaddubsw     m2,    m7,         [r3 + 28 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 28 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 427 * 16],   m2

    ; mode 28 [row 12]
    movd          m1,    [r2 + 10]
    palignr       m1,    m7,        2
    pmaddubsw     m2,    m1,         [r3 + 1 * 16]
    pmulhrsw      m2,    m3
    movd          m4,    [r2 + 18]
    palignr       m4,    m6,        2
    pmaddubsw     m5,    m4,         [r3 + 1 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 428 * 16],   m2

    ; mode 30 [row 4]
    movu          [r0 + 452 * 16],   m2

    ; mode 28 [row 13]
    pmaddubsw     m2,    m1,         [r3 + 6 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 6 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 429 * 16],   m2

    ; mode 28 [row 14]
    pmaddubsw     m2,    m1,         [r3 + 11 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 11 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 430 * 16],   m2

    ; mode 28 [row 15]
    pmaddubsw     m2,    m1,           [r3 + 16 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,           [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 431 * 16],   m2

    ; mode 29 [row 7]
    pmaddubsw     m2,    m1,         [r3 + 8 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 8 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 439 * 16],   m2

    ; mode 29 [row 8]
    pmaddubsw     m2,    m1,         [r3 + 17 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 17 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 440 * 16],   m2

    ; mode 29 [row 9]
    pmaddubsw     m2,    m1,          [r3 + 26 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,          [r3 + 26 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 441 * 16],   m2

    ; mode 30 [row 5]
    pmaddubsw     m2,    m1,         [r3 + 14 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 14 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 453 * 16],   m2

    ; mode 33 [row 2]
    movu          [r0 + 498 * 16],   m2

    ; mode 30 [row 6]
    pmaddubsw     m2,    m1,         [r3 + 27 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 27 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 454 * 16],   m2

    ; mode 31 [row 3]
    pmaddubsw     m2,    m1,         [r3 + 4 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 4 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 467 * 16],   m2

    ; mode 31 [row 4]
    pmaddubsw     m2,    m1,         [r3 + 21 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 21 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 468 * 16],   m2

    ; mode 32 [row 3]
    pmaddubsw     m2,    m1,         [r3 + 20 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 20 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 483 * 16],   m2

    ; mode 29 [row 10]
    movd          m7,     [r2 + 11]
    palignr       m7,     m1,        2
    pmaddubsw     m2,    m7,         [r3 + 3 * 16]
    pmulhrsw      m2,    m3
    movd          m6,     [r2 + 19]
    palignr       m6,     m4,        2
    pmaddubsw     m5,    m6,         [r3 + 3 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 442 * 16],   m2

    ; mode 29 [row 11]
    pmaddubsw     m2,    m7,         [r3 + 12 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 12 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 443 * 16],   m2

    ; mode 29 [row 12]
    pmaddubsw     m2,    m7,         [r3 + 21 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 21 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 444 * 16],   m2

    ; mode 30 [row 8]
    movu          [r0 + 456 * 16],   m2

    ; mode 29 [row 13]
    pmaddubsw     m2,    m7,         [r3 + 30 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 30 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 445 * 16],   m2

    ; mode 32 [row 5]
    movu          [r0 + 485 * 16],   m2

    ; mode 30 [row 7]
    pmaddubsw     m2,    m7,         [r3 + 8 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 8 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 455 * 16],   m2

    ; mode 33 [row 3]
    movu          [r0 + 499 * 16],   m2

    ; mode 31 [row 5]
    pmaddubsw     m2,    m7,         [r3 + 6 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 6 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 469 * 16],   m2

    ; mode 31 [row 6]
    pmaddubsw     m2,    m7,         [r3 + 23 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 23 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 470 * 16],   m2

    ; mode 32 [row 4]
    pmaddubsw     m2,    m7,          [r3 + 9 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,          [r3 + 9 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 484 * 16],   m2

    movu          m1,        m7
    movu          m4,        m6

    ; mode 29 [row 14]
    movu          m1,    [r2 + 12]
    palignr       m1,    m7,         2
    pmaddubsw     m2,    m1,         [r3 + 7 * 16]
    pmulhrsw      m2,    m3
    movd          m4,     [r2 + 20]
    palignr       m4,     m6,        2
    pmaddubsw     m5,    m4,         [r3 + 7 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 446 * 16],   m2

    ; mode 29 [row 15]
    pmaddubsw     m2,    m1,         [r3 + 16 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 447 * 16],   m2

    ; mode 30 [row 9]
    pmaddubsw     m2,    m1,         [r3 + 2 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 2 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 457 * 16],   m2

    ; mode 33 [row 4]
    movu          [r0 + 500 * 16],   m2

    ; mode 30 [row 10]
    pmaddubsw     m2,    m1,         [r3 + 15 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 15 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 458 * 16],   m2

    ; mode 30 [row 11]
    pmaddubsw     m2,    m1,          [r3 + 28 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,          [r3 + 28 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 459 * 16],   m2

    ; mode 33 [row 5]
    movu          [r0 + 501 * 16],   m2

    ; mode 31 [row 7]
    pmaddubsw     m2,    m1,         [r3 + 8 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 8 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 471 * 16],   m2

    ; mode 31 [row 8]
    pmaddubsw     m2,    m1,         [r3 + 25 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 25 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 472 * 16],   m2

    ; mode 32 [row 6]
    pmaddubsw     m2,    m1,         [r3 + 19 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 19 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 486 * 16],   m2

    ; mode 30 [row 12]
    movd          m7,    [r2 + 13]
    palignr       m7,    m1,         2
    pmaddubsw     m2,    m7,         [r3 + 9 * 16]
    pmulhrsw      m2,    m3
    movd          m6,    [r2 + 21]
    palignr       m6,    m4,         2
    pmaddubsw     m5,    m6,         [r3 + 9 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 460 * 16],   m2

    ; mode 30 [row 13]
    pmaddubsw     m2,    m7,          [r3 + 22 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,          [r3 + 22 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 461 * 16],   m2

    ; mode 33 [row 6]
    movu          [r0 + 502 * 16],   m2

    ; mode 31 [row 9]
    pmaddubsw     m2,    m7,          [r3 + 10 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,          [r3 + 10 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 473 * 16],   m2

    ; mode 31 [row 10]
    pmaddubsw     m2,    m7,         [r3 + 27 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 27 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 474 * 16],   m2

    ; mode 32 [row 7]
    pmaddubsw     m2,    m7,         [r3 + 8 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 8 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 487 * 16],   m2

    ; mode 32 [row 8]
    pmaddubsw     m2,    m7,         [r3 + 29 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 29 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 488 * 16],   m2


    movu          m1,                m7
    movu          m4,                m6

    ; mode 30 [row 14]
    movd          m1,    [r2 + 14]
    palignr       m1,    m7,        2
    pmaddubsw     m2,    m1,        [r3 + 3 * 16]
    pmulhrsw      m2,    m3
    movd          m4,    [r2 + 22]
    palignr       m4,    m6,        2
    pmaddubsw     m5,    m4,        [r3 + 3 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,               m5
    movu          [r0 + 462 * 16],  m2

    ; mode 30 [row 15]
    pmaddubsw     m2,    m1,         [r3 + 16 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 463 * 16],   m2

    ; mode 33 [row 7]
    movu          [r0 + 503 * 16],   m2

    ; mode 31 [row 11]
    pmaddubsw     m2,    m1,          [r3 + 12 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,          [r3 + 12 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 475 * 16],   m2

    ; mode 31 [row 12]
    pmaddubsw     m2,    m1,         [r3 + 29 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 29 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 476 * 16],   m2

    ; mode 32 [row 9]
    pmaddubsw     m2,    m1,         [r3 + 18 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 18 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 489 * 16],   m2

    ; mode 31 [row 13]
    movd          m7,    [r2 + 15]
    palignr       m7,    m1,         2
    pmaddubsw     m2,    m7,         [r3 + 14 * 16]
    pmulhrsw      m2,    m3
    movd          m6,    [r2 + 23]
    palignr       m6,    m4,         2
    pmaddubsw     m5,    m6,         [r3 + 14 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 477 * 16],   m2

    ; mode 31 [row 14]
    pmaddubsw     m2,    m7,         [r3 + 31 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 31 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 478 * 16],   m2

    ; mode 32 [row 10]
    pmaddubsw     m2,    m7,         [r3 + 7 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 7 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 490 * 16],   m2

    ; mode 32 [row 11]
    pmaddubsw     m2,    m7,         [r3 + 28 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 28 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 491 * 16],   m2

    ; mode 33 [row 8]
    pmaddubsw     m2,    m7,         [r3 + 10 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 10 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 504 * 16],   m2

    ; mode 31 [row 15]
    movd          m1,    [r2 + 16]
    palignr       m1,    m7,         2
    pmaddubsw     m2,    m1,          [r3 + 16 * 16]
    pmulhrsw      m2,    m3
    movd          m4,    [r2 + 24]
    palignr       m4,    m6,         2
    pmaddubsw     m5,    m4,          [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 479 * 16],   m2

    ; mode 32 [row 12]
    pmaddubsw     m2,    m1,          [r3 + 17 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,          [r3 + 17 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 492 * 16],   m2

    ; mode 33 [row 9]
    pmaddubsw     m2,    m1,         [r3 + 4 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 4 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 505 * 16],   m2

    ; mode 33 [row 10]
    pmaddubsw     m2,    m1,          [r3 + 30 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,          [r3 + 30 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 506 * 16],   m2

    ; mode 33 [row 10]
    pmaddubsw     m2,    m1,          [r3 + 4 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,          [r3 + 4 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 505 * 16],   m2

    ; mode 32 [row 13]
    movd          m7,    [r2 + 17]
    palignr       m7,    m1,         2
    pmaddubsw     m2,    m7,         [r3 + 6 * 16]
    pmulhrsw      m2,    m3

    movd          m6,    [r2 + 25]
    palignr       m6,    m4,         2
    pmaddubsw     m5,    m6,         [r3 + 6 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 493 * 16],   m2

    ; mode 32 [row 14]
    pmaddubsw     m2,    m7,         [r3 + 27 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 27 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 494 * 16],   m2

    ; mode 33 [row 11]
    pmaddubsw     m2,    m7,         [r3 + 24 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m6,         [r3 + 24 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 507 * 16],   m2

    ; mode 32 [row 15]
    movd          m1,    [r2 + 18]
    palignr       m1,    m7,         2
    pmaddubsw     m2,    m1,         [r3 + 16 * 16]
    pmulhrsw      m2,    m3
    psrldq        m4,    2
    pinsrb        m4,    [r2 + 26],  14
    pinsrb        m4,    [r2 + 27],  15
    movd          m4,    [r2 + 26]
    palignr       m4,    m6,         2
    pmaddubsw     m5,    m4,         [r3 + 16 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 495 * 16],   m2

    ; mode 33 [row 12]
    pmaddubsw     m2,    m1,         [r3 + 18 * 16]
    pmulhrsw      m2,    m3
    pmaddubsw     m5,    m4,         [r3 + 18 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 508 * 16],   m2

    ; mode 33 [row 13]
    movd          m7,    [r2 + 19]
    palignr       m7,    m1,         2
    pmaddubsw     m2,    m7,         [r3 + 12 * 16]
    pmulhrsw      m2,    m3
    movd          m6,    [r2 + 27]
    palignr       m6,    m4,         2
    pmaddubsw     m5,    m6,         [r3 + 12 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 509 * 16],   m2

    ; mode 33 [row 14]
    movd          m1,    [r2 + 20]
    palignr       m1,    m7,         2
    pmaddubsw     m2,    m1,         [r3 + 6 * 16]
    pmulhrsw      m2,    m3
    movd          m4,    [r2 + 28]
    palignr       m4,    m6,         2
    pmaddubsw     m5,    m4,         [r3 + 6 * 16]
    pmulhrsw      m5,    m3
    packuswb      m2,                m5
    movu          [r0 + 510 * 16],   m2

    ; mode 34 [row 0]
    movu          m1,                [r2 + 2]
    movu          [r0 + 512 * 16],   m1
    movu          m2,                [r2 + 18]
    palignr       m3,                m2,     m1,    1
    movu          [r0 + 513 * 16],   m3
    palignr       m3,                m2,     m1,    2
    movu          [r0 + 514 * 16],   m3
    palignr       m3,                m2,     m1,    3
    movu          [r0 + 515 * 16],   m3
    palignr       m3,                m2,     m1,    4
    movu          [r0 + 516 * 16],   m3
    palignr       m3,                m2,     m1,    5
    movu          [r0 + 517 * 16],   m3
    palignr       m3,                m2,     m1,    6
    movu          [r0 + 518 * 16],   m3
    palignr       m3,                m2,     m1,    7
    movu          [r0 + 519 * 16],   m3
    palignr       m3,                m2,     m1,    8
    movu          [r0 + 520 * 16],   m3
    palignr       m3,                m2,     m1,    9
    movu          [r0 + 521 * 16],   m3
    palignr       m3,                m2,     m1,   10
    movu          [r0 + 522 * 16],   m3
    palignr       m3,                m2,     m1,   11
    movu          [r0 + 523 * 16],   m3
    palignr       m3,                m2,     m1,   12
    movu          [r0 + 524 * 16],   m3

    ; mode 33 [row 15]
    movu          [r0 + 511 * 16],   m3

    ; mode 34
    palignr       m3,                m2,     m1,   13
    movu          [r0 + 525 * 16],   m3
    palignr       m3,                m2,     m1,   14
    movu          [r0 + 526 * 16],   m3
    palignr       m3,                m2,     m1,   15
    movu          [r0 + 527 * 16],   m3
    RET

;--------------------------------------------------------------------------------
; void all_angs_pred_32x32(pixel *dest, pixel *refPix, pixel *filtPix, int bLuma)
;--------------------------------------------------------------------------------
INIT_XMM sse4
cglobal all_angs_pred_32x32, 3,7,8, 0-4
    mov        r6d, [r1 + 64]
    mov        r3d, [r1]
    mov        [rsp], r6d
    mov        [r1 + 64], r3b
    mov        r3d, [r2]
    mov        r6d, [r2 + 64]
    mov        [r2 + 64], r3b

    lea        r3, [r2]
    lea        r4, [r2 + 64]
    lea        r2, [r1 + 64]

    ;mode 2[row 0]
    movu       m0,              [r4 + 2]
    movu       [r0 + 0 * 16],   m0
    movu       m1,              [r4 + 18]
    movu       [r0 + 1 * 16],   m1

    ;mode 9 [row 15]
    movu       [r0 + 478 * 16],   m0
    movu       [r0 + 479 * 16],   m1

    ;mode 2[row 1]
    movu       m2,              [r4 + 34]
    palignr    m3,              m1,       m0,    1
    movu       [r0 + 2 * 16],   m3
    palignr    m4,              m2,       m1,    1
    movu       [r0 + 3 * 16],   m4

    ; mode 9 [row 31]
    movu       [r0 + 510 * 16], m3
    movu       [r0 + 511 * 16], m4

    ;mode 2[row 17]
    movu       [r0 + 34 * 16],  m4
    movu       m5,              [r4 + 35]
    movu       [r0 + 35 * 16],  m5

    ;mode 2[row 2]
    palignr    m3,              m1,       m0,    2
    movu       [r0 + 4 * 16],   m3
    palignr    m4,              m2,       m1,    2
    movu       [r0 + 5 * 16],   m4

    ;mode 2[row 18]
    movu       [r0 + 36 * 16],  m4
    movu       m6,              [r4 + 51]
    palignr    m7,              m6,       m5,    1
    movu       [r0 + 37 * 16],  m7

    ;mode 2[row 3]
    palignr    m3,              m1,       m0,    3
    movu       [r0 + 6 * 16],   m3
    palignr    m4,              m2,       m1,    3
    movu       [r0 + 7 * 16],   m4

    ;mode 2[row 19]
    movu       [r0 + 38 * 16],  m4
    palignr    m7,              m6,       m5,    2
    movu       [r0 + 39 * 16],  m7

    ;mode 2[row 4]
    palignr    m3,              m1,       m0,    4
    movu       [r0 + 8 * 16],   m3
    palignr    m4,              m2,       m1,    4
    movu       [r0 + 9 * 16],   m4

    ; mode 8 [row 31]
    movu       [r0 + 446 * 16],   m3
    movu       [r0 + 447 * 16],   m4

    ;mode 2[row 20]
    movu       [r0 + 40 * 16],  m4
    palignr    m7,              m6,       m5,    3
    movu       [r0 + 41 * 16],  m7

    ; mode 4 [row 31]
    movu       [r0 + 190 * 16],  m4
    movu       [r0 + 191 * 16],  m7

    ;mode 2[row 5]
    palignr    m3,              m1,       m0,    5
    movu       [r0 + 10 * 16],  m3
    palignr    m4,              m2,       m1,    5
    movu       [r0 + 11 * 16],  m4

    ;mode 2[row 21]
    movu       [r0 + 42 * 16],  m4
    palignr    m7,              m6,       m5,    4
    movu       [r0 + 43 * 16],  m7

    ;mode 2[row 6]
    palignr    m3,              m1,       m0,    6
    movu       [r0 + 12 * 16],  m3
    palignr    m4,              m2,       m1,    6
    movu       [r0 + 13 * 16],  m4

    ;mode 2[row 22]
    movu       [r0 + 44 * 16],  m4
    palignr    m7,              m6,       m5,    5
    movu       [r0 + 45 * 16],  m7

    ;mode 2[row 7]
    palignr    m3,              m1,       m0,    7
    movu       [r0 + 14 * 16],  m3
    palignr    m4,              m2,       m1,    7
    movu       [r0 + 15 * 16],  m4

    ;mode 2[row 23]
    movu       [r0 + 46 * 16],  m4
    palignr    m7,              m6,       m5,    6
    movu       [r0 + 47 * 16],  m7

    ;mode 2[row 8]
    palignr    m3,              m1,       m0,    8
    movu       [r0 + 16 * 16],  m3
    palignr    m4,              m2,       m1,    8
    movu       [r0 + 17 * 16],  m4

    ;mode 7[row 31]
    movu       [r0 + 382 * 16],  m3
    movu       [r0 + 383 * 16],  m4

    ;mode 2[row 24]
    movu       [r0 + 48 * 16],  m4
    palignr    m7,              m6,       m5,    7
    movu       [r0 + 49 * 16],  m7

    ;mode 2[row 9]
    palignr    m3,              m1,       m0,    9
    movu       [r0 + 18 * 16],  m3
    palignr    m4,              m2,       m1,    9
    movu       [r0 + 19 * 16],  m4

    ;mode 2[row 25]
    movu       [r0 + 50 * 16],  m4
    palignr    m7,              m6,       m5,    8
    movu       [r0 + 51 * 16],  m7

    ; mode 3 [row 31]
    movu       [r0 + 126 * 16],  m4
    movu       [r0 + 127 * 16],  m7

    ;mode 2[row 10]
    palignr    m3,              m1,       m0,   10
    movu       [r0 + 20 * 16],  m3
    palignr    m4,              m2,       m1,   10
    movu       [r0 + 21 * 16],  m4

    ;mode 2[row 26]
    movu       [r0 + 52 * 16],  m4
    palignr    m7,              m6,       m5,    9
    movu       [r0 + 53 * 16],  m7

    ;mode 2[row 11]
    palignr    m3,              m1,       m0,   11
    movu       [r0 + 22 * 16],  m3
    palignr    m4,              m2,       m1,   11
    movu       [r0 + 23 * 16],  m4

    ;mode 2[row 27]
    movu       [r0 + 54 * 16],  m4
    palignr    m7,              m6,       m5,   10
    movu       [r0 + 55 * 16],  m7

    ;mode 2[row 12]
    palignr    m3,              m1,       m0,   12
    movu       [r0 + 24 * 16],  m3
    palignr    m4,              m2,       m1,   12
    movu       [r0 + 25 * 16],  m4

    ; mode 6 [row 31]
    movu       [r0 + 318 * 16],  m3
    movu       [r0 + 319 * 16],  m4

    ; mode 3 [row 15]
    movu       [r0 + 94 * 16],  m3
    movu       [r0 + 95 * 16],  m4

    ;mode 2[row 28]
    movu       [r0 + 56 * 16],  m4
    palignr    m7,              m6,       m5,   11
    movu       [r0 + 57 * 16],  m7

    ;mode 2[row 13]
    palignr    m3,              m1,       m0,   13
    movu       [r0 + 26 * 16],  m3
    palignr    m4,              m2,       m1,   13
    movu       [r0 + 27 * 16],  m4

    ;mode 2[row 29]
    movu       [r0 + 58 * 16],  m4
    palignr    m7,              m6,       m5,   12
    movu       [r0 + 59 * 16],  m7

    ;mode 2[row 14]
    palignr    m3,              m1,       m0,   14
    movu       [r0 + 28 * 16],  m3
    palignr    m4,              m2,       m1,   14
    movu       [r0 + 29 * 16],  m4

    ;mode 2[row 30]
    movu       [r0 + 60 * 16],  m4
    palignr    m7,              m6,       m5,   13
    movu       [r0 + 61 * 16],  m7

    ;mode 2[row 15]
    palignr    m3,              m1,       m0,   15
    movu       [r0 + 30 * 16],  m3
    palignr    m4,              m2,       m1,   15
    movu       [r0 + 31 * 16],  m4

    ;mode 2[row 31]
    movu       [r0 + 62 * 16],  m4
    palignr    m7,              m6,       m5,   14
    movu       [r0 + 63 * 16],  m7

    ;mode 2[row 16]
    movu       [r0 + 32 * 16],  m1
    movu       [r0 + 33 * 16],  m2

    ; mode 5[row 31]
    movu       [r0 + 254 * 16],  m1
    movu       [r0 + 255 * 16],  m2

    ; mode 3 [row 0]
    lea           r5,    [ang_table]
    movu          m6,    [r5 + 26 * 16]
    movu          m7,    [pw_1024  ]
    movu          m1,    [r4 + 1   ]
    punpcklbw     m1,    m0
    pmaddubsw     m0,    m1,        m6
    pmulhrsw      m0,    m7
    movu          m2,    [r4 +  9]
    movd          m3,    [r4 + 10]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m0,    m3
    movu          [r0 + 64 * 16],   m0

    ; mode 6 [row 1 - first half]
    movu          [r0 + 258 * 16],  m0

    ; mode 9 [row 12 - first half]
    movu          [r0 + 472 * 16],  m0

    movu          m0,    [r4 + 17]
    movd          m3,    [r4 + 18]
    palignr       m3,    m0,        1
    punpcklbw     m0,    m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 25]
    movd          m5,    [r4 + 26]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 65 * 16],   m3

    ; mode 6 [row 1 - second half]
    movu          [r0 + 259 * 16],  m3

    ; mode 9 [row 12 - second half]
    movu          [r0 + 473 * 16],  m3

    ; mode 4 [row 0]
    movu          m6,    [r5 + 21 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 128 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 129 * 16],  m3

    ; mode 5 [row 0]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 192 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 193 * 16],  m3

    ; mode 6 [row 0]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 256 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 257 * 16],  m3

    ; mode 7 [row 0]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 320 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 321 * 16],  m3

    ; mode 7 [row 1]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 322 * 16],  m3

    ; mode 9 [row 8 - first half]
    movu          [r0 + 464 * 16],  m3

    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 323 * 16],  m3

    ; mode 9 [row 8 - second half]
    movu          [r0 + 465 * 16],  m3

    ; mode 7 [row 2]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 324 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 325 * 16],  m3

    ; mode 8 [row 0]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 384 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 385 * 16],  m3

    ; mode 8 [row 1]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 386 * 16],  m3

    ; mode 9 [row 4 - first half]
    movu          [r0 + 456 * 16],  m3

    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 387 * 16],  m3

    ; mode 9 [row 4 - second half]
    movu          [r0 + 457 * 16],  m3

    ; mode 8 [row 2]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 388 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 389 * 16],  m3

    ; mode 8 [row 3]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 390 * 16],  m3

    ; mode 9 [row 9 - first half]
    movu          [r0 + 466 * 16],  m3

    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 391 * 16],  m3

    ; mode 9 [row 9 - second half]
    movu          [r0 + 467 * 16],  m3

    ; mode 8 [row 4]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 392 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 393 * 16],  m3

    ; mode 8 [row 5]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 394 * 16],  m3

    ; mode 9 [row 14 - first half]
    movu          [r0 + 476 * 16],  m3

    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 395 * 16],  m3

    ; mode 9 [row 14 - second half]
    movu          [r0 + 477 * 16],  m3

    ; mode 9 [row 0]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 448 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 449 * 16],  m3

    ; mode 9 [row 1]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 450 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 451 * 16],  m3

    ; mode 9 [row 2]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 452 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 453 * 16],  m3

    ; mode 9 [row 3]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 454 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 455 * 16],  m3

    ; mode 9 [row 5]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 458 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 459 * 16],  m3

    ; mode 9 [row 6]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 460 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 461 * 16],  m3

    ; mode 9 [row 7]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 462 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 463 * 16],  m3

    ; mode 9 [row 10]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 468 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 469 * 16],  m3

    ; mode 9 [row 11]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 470 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 471 * 16],  m3

    ; mode 9 [row 13]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 474 * 16],  m3
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 475 * 16],  m3

    ; mode 3 [row 1]
    movu          m6,    [r5 + 20 * 16]
    movu          m0,    [r4 + 2]
    movd          m1,    [r4 + 3]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 10]
    movd          m3,    [r4 + 11]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 66 * 16],   m1

    ; mode 6 [row 3 - first half]
    movu          [r0 + 262 * 16],  m1

    ; mode 9 [row 25 - first half]
    movu          [r0 + 498 * 16],  m1

    movu          m1,    [r4 + 18]
    movd          m3,    [r4 + 19]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 26]
    movd          m5,    [r4 + 27]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 67 * 16],   m3

    ; mode 6 [row 3 - second half]
    movu          [r0 + 263 * 16],  m3

    ; mode 9 [row 25 - second half]
    movu          [r0 + 499 * 16],  m3

    ; mode 4 [row 1]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 130 * 16],  m3

    ; mode 9 [row 20 - first half]
    movu          [r0 + 488 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 131 * 16],  m3

    ; mode 9 [row 20 - second half]
    movu          [r0 + 489 * 16],  m3

    ; mode 4 [row 2]
    movu          m6,    [r5 + 31 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 132 * 16],  m3

    ; mode 7 [row 6 - first half]
    movu          [r0 + 332 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 133 * 16],  m3

    ; mode 7 [row 6 - second half]
    movu          [r0 + 333 * 16],  m3

    ; mode 5 [row 1]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 194 * 16],  m3

    ; mode 5 [row 1 - first half]
    movu          [r0 + 480 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 195 * 16],  m3

    ; mode 5 [row 1 - second half]
    movu          [r0 + 481 * 16],  m3

    ; mode 5 [row 2]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 196 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 197 * 16],  m3

    ; mode 6 [row 2]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 260 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 261 * 16],  m3

    ; mode 7 [row 3]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 326 * 16],  m3

    ; mode 9 [row 17 - first half]
    movu          [r0 + 482 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 327 * 16],  m3

    ; mode 9 [row 17 - second half]
    movu          [r0 + 483 * 16],  m3

    ; mode 7 [row 4]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 328 * 16],  m3

    ; mode 8 [row 8 - first half]
    movu          [r0 + 400 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 329 * 16],  m3

    ; mode 8 [row 8 - second half]
    movu          [r0 + 401 * 16],  m3

    ; mode 7 [row 5]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 330 * 16],  m3

    ; mode 9 [row 26 - first half]
    movu          [r0 + 500 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 331 * 16],  m3

    ; mode 9 [row 26 - second half]
    movu          [r0 + 501 * 16],  m3

    ; mode 8 [row 6]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 396 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 397 * 16],  m3

    ; mode 9 [row 18]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 484 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 485 * 16],  m3

    ; mode 9 [row 21]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 490 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 491 * 16],  m3

    ; mode 9 [row 22]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 492 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 493 * 16],  m3

    ; mode 9 [row 23]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 494 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 495 * 16],  m3

    ; mode 9 [row 27]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 502 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 503 * 16],  m3

    ; mode 9 [row 28]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 504 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 505 * 16],  m3

    ; mode 9 [row 30]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 508 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 509 * 16],  m3

    ; mode 8 [row 7]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 398 * 16],  m3

    ; mode 9 [row 19 - first half]
    movu          [r0 + 486 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 399 * 16],  m3

    ; mode 9 [row 19 - second half]
    movu          [r0 + 487 * 16],  m3

    ; mode 8 [row 9]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 402 * 16],  m3

    ; mode 9 [row 24 - first half]
    movu          [r0 + 496 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 403 * 16],  m3

    ; mode 9 [row 24 - second half]
    movu          [r0 + 497 * 16],  m3

    ; mode 8 [row 10]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 404 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 405 * 16],  m3

    ; mode 8 [row 11]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 406 * 16],  m3

    ; mode 9 [row 29 - first half]
    movu          [r0 + 506 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 407 * 16],  m3

    ; mode 9 [row 29 - second half]
    movu          [r0 + 507 * 16],  m3

    ; mode 3 [row 2]
    movu          m6,    [r5 + 14 * 16]
    movu          m0,    [r4 + 3]
    movd          m1,    [r4 + 4]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 11]
    movd          m3,    [r4 + 12]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 68 * 16],   m1

    ; mode 3 [row 2 - first half]
    movu          [r0 + 266 * 16],  m1

    movu          m1,    [r4 + 19]
    movd          m3,    [r4 + 20]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 27]
    movd          m5,    [r4 + 28]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 69 * 16],   m3

    ; mode 3 [row 2 - second half]
    movu          [r0 + 267 * 16],  m3

    ; mode 4 [row 3]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 134 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 135 * 16],  m3

    ; mode 5 [row 3]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 198 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 199 * 16],  m3

    ; mode 5 [row 4]
    movu          m6,    [r5 + 21 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 200 * 16],  m3

    ; mode 8 [row 16 - first half]
    movu          [r0 + 416 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 201 * 16],  m3

    ; mode 8 [row 16 - second half]
    movu          [r0 + 417 * 16],  m3

    ; mode 6 [row 4]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 264 * 16],  m3

    ; mode 6 [row 4 - first half]
    movu          [r0 + 408 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 265 * 16],  m3

    ; mode 6 [row 4 - second half]
    movu          [r0 + 409 * 16],  m3

    ; mode 6 [row 6]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 268 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 269 * 16],  m3

    ; mode 7 [row 7]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 334 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 335 * 16],  m3

    ; mode 7 [row 8]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 336 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 337 * 16],  m3

    ; mode 7 [row 9]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 338 * 16],  m3

    ; mode 8 [row 17 - first half]
    movu          [r0 + 418 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 339 * 16],  m3

    ; mode 8 [row 17 - second half]
    movu          [r0 + 419 * 16],  m3

    ; mode 8 [row 13]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 410 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 411 * 16],  m3

    ; mode 8 [row 14]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 412 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 413 * 16],  m3

    ; mode 8 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 414 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 415 * 16],  m3

    ; mode 8 [row 18]
    movu          m6,    [r5 + 31 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 420 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 421 * 16],  m3

    ; mode 3 [row 3]
    movu          m6,    [r5 + 8 * 16]
    movu          m0,    [r4 + 4]
    movd          m1,    [r4 + 5]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 12]
    movd          m3,    [r4 + 13]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 70 * 16],   m1

    ; mode 6 [row 7 - first half]
    movu          [r0 + 270 * 16],  m1

    movu          m1,    [r4 + 20]
    movd          m3,    [r4 + 21]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 28]
    movd          m5,    [r4 + 29]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 71 * 16],   m3

    ; mode 6 [row 7 - second half]
    movu          [r0 + 271 * 16],  m3

    ; mode 4 [row 4]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 136 * 16],  m3

    ; mode 4 [row 4 - first half]
    movu          [r0 + 424 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 137 * 16],  m3

    ; mode 4 [row 4 - second half]
    movu          [r0 + 425 * 16],  m3

    ; mode 4 [row 5]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 138 * 16],  m3

    ; mode 7 [row 13 - first half]
    movu          [r0 + 346 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 139 * 16],  m3

    ; mode 7 [row 13 - second half]
    movu          [r0 + 347 * 16],  m3

    ; mode 5 [row 5]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 202 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 203 * 16],  m3

    ; mode 5 [row 6]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 204 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 205 * 16],  m3

    ; mode 6 [row 8]
    movu          m6,    [r5 + 21 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 272 * 16],  m3

    ; mode 7 [row 12 - first half]
    movu          [r0 + 344 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 273 * 16],  m3

    ; mode 7 [row 12 - second half]
    movu          [r0 + 345 * 16],  m3

    ; mode 7 [row 10]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 340 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 341 * 16],  m3

    ; mode 7 [row 11]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 342 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 343 * 16],  m3

    ; mode 8 [row 19]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 422 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 423 * 16],  m3

    ; mode 8 [row 21]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 426 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 427 * 16],  m3

    ; mode 8 [row 22]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 428 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 429 * 16],  m3

    ; mode 8 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 430 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 431 * 16],  m3

    ; mode 8 [row 24]
    movu          m6,    [r5 + 29 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 432 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 433 * 16],  m3

    ; mode 3 [row 4]
    movu          m6,    [r5 + 2 * 16]
    movu          m0,    [r4 + 5]
    movd          m1,    [r4 + 6]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 13]
    movd          m3,    [r4 + 14]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 72 * 16],   m1

    ; mode 3 [row 4 - first half]
    movu          [r0 + 274 * 16],  m1

    ; mode 8 [row 25 - first half]
    movu          [r0 + 434 * 16],  m1

    movu          m1,    [r4 + 21]
    movd          m3,    [r4 + 22]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 29]
    movd          m5,    [r4 + 30]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 73 * 16],   m3

    ; mode 3 [row 4 - second half]
    movu          [r0 + 275 * 16],  m3

    ; mode 8 [row 25 - second half]
    movu          [r0 + 435 * 16],  m3

    ; mode 3 [row 5]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 74 * 16],   m3

    ; mode 3 [row 5 - first half]
    movu          [r0 + 278 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 75 * 16],   m3

    ; mode 3 [row 5 - second half]
    movu          [r0 + 279 * 16],  m3

    ; mode 4 [row 6]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 140 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 141 * 16],  m3

    ; mode 5 [row 7]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 206 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 207 * 16],  m3

    ; mode 5 [row 8]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 208 * 16],  m3

    ; mode 7 [row 16 - first half]
    movu          [r0 + 352 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 209 * 16],  m3

    ; mode 7 [row 16 - second half]
    movu          [r0 + 353 * 16],  m3

    ; mode 6 [row 10]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 276 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 277 * 16],  m3

    ; mode 7 [row 14]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 348 * 16],  m3

    ; mode 8 [row 26 - first half]
    movu          [r0 + 436 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 349 * 16],  m3

    ; mode 8 [row 26 - second half]
    movu          [r0 + 437 * 16],  m3

    ; mode 7 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 350 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 351 * 16],  m3

    ; mode 8 [row 27]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 438 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 439 * 16],  m3

    ; mode 8 [row 28]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 440 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 441 * 16],  m3

    ; mode 8 [row 29]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 442 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 443 * 16],  m3

    ; mode 8 [row 30]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 444 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 445 * 16],  m3

    ; mode 3 [row 6]
    movu          m6,    [r5 + 22 * 16]
    movu          m0,    [r4 + 6]
    movd          m1,    [r4 + 7]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 14]
    movd          m3,    [r4 + 15]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 76 * 16],   m1

    ; mode 6 [row 13 - first half]
    movu          [r0 + 282 * 16],  m1

    movu          m1,    [r4 + 22]
    movd          m3,    [r4 + 23]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 30]
    movd          m5,    [r4 + 31]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 77 * 16],   m3

    ; mode 6 [row 13 - second half]
    movu          [r0 + 283 * 16],  m3

    ; mode 4 [row 7]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 142 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 143 * 16],  m3

    ; mode 4 [row 8]
    movu          m6,    [r5 + 29 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 144 * 16],  m3

    ; mode 4 [row 8 - first half]
    movu          [r0 + 360 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 145 * 16],  m3

    ; mode 4 [row 8 - second half]
    movu          [r0 + 361 * 16],  m3

    ; mode 5 [row 9]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 210 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 211 * 16],  m3

    ; mode 5 [row 10]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 212 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 213 * 16],  m3

    ; mode 7 [row 17]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 354 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 355 * 16],  m3

    ; mode 7 [row 18]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 356 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 357 * 16],  m3

    ; mode 7 [row 19]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 358 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 359 * 16],  m3

    ; mode 6 [row 12]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 280 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 281 * 16],  m3

    ; mode 3 [row 7]
    movu          m6,    [r5 + 16 * 16]
    movu          m0,    [r4 + 7]
    movd          m1,    [r4 + 8]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 15]
    movd          m3,    [r4 + 16]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 78 * 16],   m1

    ; mode 6 [row 15 - first half]
    movu          [r0 + 286 * 16],  m1

    movu          m1,    [r4 + 23]
    movd          m3,    [r4 + 24]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 31]
    movd          m5,    [r4 + 32]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 79 * 16],   m3

    ; mode 6 [row 15 - second half]
    movu          [r0 + 287 * 16],  m3

    ; mode 4 [row 9]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 146 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 147 * 16],  m3

    ; mode 5 [row 11]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 214 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 215 * 16],  m3

    ; mode 5 [row 12]
    movu          m6,    [r5 + 29 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 216 * 16],  m3

    ; mode 6 [row 16 - first half]
    movu          [r0 + 288 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 217 * 16],  m3

    ; mode 6 [row 16 - second half]
    movu          [r0 + 289 * 16],  m3

    ; mode 6 [row 14]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 284 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 285 * 16],  m3

    ; mode 7 [row 21]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 362 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 363 * 16],  m3

    ; mode 7 [row 22]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 364 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 365 * 16],  m3

    ; mode 7 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 366 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 367 * 16],  m3

    ; mode 3 [row 8]
    movu          m6,    [r5 + 10 * 16]
    movu          m0,    [r4 + 8]
    movd          m1,    [r4 + 9]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 16]
    movd          m3,    [r4 + 17]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 80 * 16],   m1

    ; mode 7 [row 25 - first half]
    movu          [r0 + 290 * 16],  m1

    ; mode 6 [row 17 - first half]
    movu          [r0 + 370 * 16],  m1

    movu          m1,    [r4 + 24]
    movd          m3,    [r4 + 25]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 32]
    movd          m5,    [r4 + 33]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 81 * 16],   m3

    ; mode 7 [row 25 - second half]
    movu          [r0 + 291 * 16],  m3

    ; mode 6 [row 17 - second half]
    movu          [r0 + 371 * 16],  m3

    ; mode 4 [row 10]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 148 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 149 * 16],  m3

    ; mode 4 [row 11]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 150 * 16],  m3

    ; mode 7 [row 27 - first half]
    movu          [r0 + 374 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 151 * 16],  m3

    ; mode 7 [row 27 - second half]
    movu          [r0 + 375 * 16],  m3

    ; mode 5 [row 13]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 218 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 219 * 16],  m3

    ; mode 5 [row 14]
    movu          m6,    [r5 + 31 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 220 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 221 * 16],  m3

    ; mode 6 [row 18]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 292 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 293 * 16],  m3

    ; mode 7 [row 24]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 368 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 369 * 16],  m3

    ; mode 7 [row 26]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 372 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 373 * 16],  m3

    ; mode 3 [row 9]
    movu          m6,    [r5 + 4 * 16]
    movu          m0,    [r4 + 9]
    movd          m1,    [r4 + 10]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 17]
    movd          m3,    [r4 + 18]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 82 * 16],   m1

    ; mode 6 [row 19 - first half]
    movu          [r0 + 294 * 16],  m1

    movu          m1,    [r4 + 25]
    movd          m3,    [r4 + 26]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 33]
    movd          m5,    [r4 + 34]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 83 * 16],   m3

    ; mode 6 [row 19 - second half]
    movu          [r0 + 295 * 16],  m3

    ; mode 4 [row 12]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 152 * 16],  m3

    ; mode 4 [row 12 - first half]
    movu          [r0 + 296 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 153 * 16],  m3

    ; mode 4 [row 12 - second half]
    movu          [r0 + 297 * 16],  m3

    ; mode 3 [row 10]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 84 * 16],   m3

    ; mode 6 [row 21 - first half]
    movu          [r0 + 298 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 85 * 16],   m3

    ; mode 6 [row 21 - second half]
    movu          [r0 + 299 * 16],  m3

    ; mode 5 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 222 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 223 * 16],  m3

    ; mode 7 [row 28]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 376 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 377 * 16],  m3

    ; mode 7 [row 29]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 378 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 379 * 16],  m3

    ; mode 7 [row 30]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 380 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 381 * 16],  m3

    ; mode 3 [row 11]
    movu          m6,    [r5 + 24 * 16]
    movu          m0,    [r4 + 10]
    movd          m1,    [r4 + 11]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 18]
    movd          m3,    [r4 + 19]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 86 * 16],   m1

    ; mode 6 [row 23 - first half]
    movu          [r0 + 302 * 16],  m1

    movu          m1,    [r4 + 26]
    movd          m3,    [r4 + 27]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 34]
    movd          m5,    [r4 + 35]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 87 * 16],   m3

    ; mode 6 [row 23 - second half]
    movu          [r0 + 303 * 16],  m3

    ; mode 4 [row 13]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 154 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 155 * 16],  m3

    ; mode 4 [row 14]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 156 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 157 * 16],  m3

    ; mode 5 [row 16]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 224 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 225 * 16],  m3

    ; mode 5 [row 17]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 226 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 227 * 16],  m3

    ; mode 6 [row 22]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 300 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 301 * 16],  m3

    ; mode 3 [row 12]
    movu          m6,    [r5 + 18 * 16]
    movu          m0,    [r4 + 11]
    movd          m1,    [r4 + 12]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 19]
    movd          m3,    [r4 + 20]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 88 * 16],   m1

    ; mode 6 [row 25 - first half]
    movu          [r0 + 306 * 16],  m1

    movu          m1,    [r4 + 27]
    movd          m3,    [r4 + 28]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 35]
    movd          m5,    [r4 + 36]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 89 * 16],   m3

    ; mode 6 [row 25 - second half]
    movu          [r0 + 307 * 16],  m3

    ; mode 4 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 158 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 159 * 16],   m3

    ; mode 5 [row 18]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 228 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 229 * 16],  m3

    ; mode 5 [row 19]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 230 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 231 * 16],  m3

    ; mode 6 [row 24]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 304 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 305 * 16],  m3

    ; mode 6 [row 26]
    movu          m6,    [r5 + 31 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 308 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 309 * 16],  m3

    ; mode 3 [row 13]
    movu          m6,    [r5 + 12 * 16]
    movu          m0,    [r4 + 12]
    movd          m1,    [r4 + 13]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 20]
    movd          m3,    [r4 + 21]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 90 * 16],   m1

    movu          m1,    [r4 + 28]
    movd          m3,    [r4 + 29]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 36]
    movd          m5,    [r4 + 37]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 91 * 16],   m3

    ; mode 4 [row 16]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 160 * 16],  m3

    ; mode 5 [row 20 - first half]
    movu          [r0 + 232 * 16],  m3

    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 161 * 16],  m3

    ; mode 5 [row 20 - second half]
    movu          [r0 + 233 * 16],  m3

    ; mode 4 [row 17]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 162 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 163 * 16],  m3

    ; mode 5 [row 21]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 234 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 235 * 16],  m3

    ; mode 6 [row 27]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 310 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 311 * 16],  m3

    ; mode 6 [row 28]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 312 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 313 * 16],  m3

    ; mode 3 [row 14]
    movu          m6,    [r5 + 6 * 16]
    movu          m0,    [r4 + 13]
    movd          m1,    [r4 + 14]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 21]
    movd          m3,    [r4 + 22]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 92 * 16],   m1

    ; mode 6 [row 29 - first half]
    movu          [r0 + 314 * 16],  m1

    movu          m1,    [r4 + 29]
    movd          m3,    [r4 + 30]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 37]
    movd          m5,    [r4 + 38]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 93 * 16],   m3

    ; mode 6 [row 29 - second half]
    movu          [r0 + 315 * 16],  m3

    ; mode 4 [row 18]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 164 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 165 * 16],  m3

    ; mode 5 [row 22]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 236 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 237 * 16],  m3

    ; mode 5 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 238 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 239 * 16],  m3

    ; mode 6 [row 30]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 316 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 317 * 16],  m3

    ; mode 3 [row 16]
    movu          m6,    [r5 + 26 * 16]
    movu          m0,    [r4 + 14]
    movd          m1,    [r4 + 15]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 22]
    movd          m3,    [r4 + 23]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 96 * 16],   m1

    ; mode 5 [row 25 - first half]
    movu          [r0 + 242 * 16],  m1

    movu          m1,    [r4 + 30]
    movd          m3,    [r4 + 31]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 38]
    movd          m5,    [r4 + 39]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 97 * 16],   m3

    ; mode 5 [row 25 - second half]
    movu          [r0 + 243 * 16],  m3

    ; mode 4 [row 19]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 166 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 167 * 16],  m3

    ; mode 4 [row 20]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 168 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 169 * 16],  m3

    ; mode 5 [row 24]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 240 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 241 * 16],  m3

    ; mode 3 [row 17]
    movu          m6,    [r5 + 20 * 16]
    movu          m0,    [r4 + 15]
    movd          m1,    [r4 + 16]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 23]
    movd          m3,    [r4 + 24]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 98 * 16],   m1

    movu          m1,    [r4 + 31]
    movd          m3,    [r4 + 32]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 39]
    movd          m5,    [r4 + 40]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 99 * 16],   m3

    ; mode 4 [row 21]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 170 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 171 * 16],  m3

    ; mode 5 [row 26]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 244 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 245 * 16],  m3

    ; mode 5 [row 27]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 246 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 247 * 16],  m3

    ; mode 3 [row 18]
    movu          m6,    [r5 + 14 * 16]
    movu          m0,    [r4 + 16]
    movd          m1,    [r4 + 17]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 24]
    movd          m3,    [r4 + 25]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 100 * 16],  m1

    movu          m1,    [r4 + 32]
    movd          m3,    [r4 + 33]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 40]
    movd          m5,    [r4 + 41]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 101 * 16],  m3

    ; mode 4 [row 22]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 172 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 173 * 16],  m3

    ; mode 4 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 174 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 175 * 16],  m3

    ; mode 5 [row 28]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 248 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 249 * 16],  m3

    ; mode 5 [row 29]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 250 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 251 * 16],  m3

    ; mode 3 [row 19]
    movu          m6,    [r5 + 8 * 16]
    movu          m0,    [r4 + 17]
    movd          m1,    [r4 + 18]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 25]
    movd          m3,    [r4 + 26]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 102 * 16],  m1

    movu          m1,    [r4 + 33]
    movd          m3,    [r4 + 34]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 41]
    movd          m5,    [r4 + 42]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 103 * 16],  m3

    ; mode 4 [row 24]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 176 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 177 * 16],  m3

    ; mode 5 [row 30]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 252 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 253 * 16],  m3

    ; mode 3 [row 20]
    movu          m6,    [r5 + 2 * 16]
    movu          m0,    [r4 + 18]
    movd          m1,    [r4 + 19]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 26]
    movd          m3,    [r4 + 27]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 104 * 16],  m1

    movu          m1,    [r4 + 34]
    movd          m3,    [r4 + 35]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 42]
    movd          m5,    [r4 + 43]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 105 * 16],  m3

    ; mode 4 [row 25]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 178 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 179 * 16],   m3

    ; mode 4 [row 26]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 180 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 181 * 16],  m3

    ; mode 3 [row 21]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 106 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 107 * 16],  m3

    ; mode 3 [row 22]
    movu          m6,    [r5 + 22 * 16]
    movu          m0,    [r4 + 19]
    movd          m1,    [r4 + 20]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 27]
    movd          m3,    [r4 + 28]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 108 * 16],  m1

    movu          m1,    [r4 + 35]
    movd          m3,    [r4 + 36]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 43]
    movd          m5,    [r4 + 44]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 109 * 16],  m3

    ; mode 4 [row 27]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 182 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 183 * 16],  m3

    ; mode 3 [row 23]
    movu          m6,    [r5 + 16 * 16]
    movu          m0,    [r4 + 20]
    movd          m1,    [r4 + 21]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 28]
    movd          m3,    [r4 + 29]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 110 * 16],  m1

    movu          m1,    [r4 + 36]
    movd          m3,    [r4 + 37]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 44]
    movd          m5,    [r4 + 45]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 111 * 16],  m3

    ; mode 4 [row 28]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 184 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 185 * 16],  m3

    ; mode 4 [row 29]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 186 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 187 * 16],  m3

    ; mode 3 [row 24]
    movu          m6,    [r5 + 10 * 16]
    movu          m0,    [r4 + 21]
    movd          m1,    [r4 + 22]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 29]
    movd          m3,    [r4 + 30]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 112 * 16],  m1

    movu          m1,    [r4 + 37]
    movd          m3,    [r4 + 38]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 45]
    movd          m5,    [r4 + 46]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 113 * 16],  m3

    ; mode 4 [row 30]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 188 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 189 * 16],  m3

    ; mode 3 [row 25]
    movu          m6,    [r5 + 4 * 16]
    movu          m0,    [r4 + 22]
    movd          m1,    [r4 + 23]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 30]
    movd          m3,    [r4 + 31]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 114 * 16],  m1

    movu          m1,    [r4 + 38]
    movd          m3,    [r4 + 39]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 46]
    movd          m5,    [r4 + 47]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 115 * 16],  m3

    ; mode 3 [row 26]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 116 * 16],  m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 117 * 16],  m3

    ; mode 3 [row 27]
    movu          m6,    [r5 + 24 * 16]
    movu          m0,    [r4 + 23]
    movd          m1,    [r4 + 24]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 31]
    movd          m3,    [r4 + 32]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 118 * 16],  m1

    movu          m1,    [r4 + 39]
    movd          m3,    [r4 + 40]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 47]
    movd          m5,    [r4 + 48]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 119 * 16],  m3

    ; mode 3 [row 28]
    movu          m6,    [r5 + 18 * 16]
    movu          m0,    [r4 + 24]
    movd          m1,    [r4 + 25]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 32]
    movd          m3,    [r4 + 33]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 120 * 16],  m1

    movu          m1,    [r4 + 40]
    movd          m3,    [r4 + 41]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 48]
    movd          m5,    [r4 + 49]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 121 * 16],  m3

    ; mode 3 [row 29]
    movu          m6,    [r5 + 12 * 16]
    movu          m0,    [r4 + 25]
    movd          m1,    [r4 + 26]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 33]
    movd          m3,    [r4 + 34]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 122 * 16],  m1

    movu          m1,    [r4 + 41]
    movd          m3,    [r4 + 42]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 49]
    movd          m5,    [r4 + 50]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 123 * 16],  m3

    ; mode 3 [row 30]
    movu          m6,    [r5 + 6 * 16]
    movu          m0,    [r4 + 26]
    movd          m1,    [r4 + 27]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        m6
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 34]
    movd          m3,    [r4 + 35]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        m6
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 124 * 16],  m1

    movu          m1,    [r4 + 42]
    movd          m3,    [r4 + 43]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        m6
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 50]
    movd          m5,    [r4 + 51]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        m6
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 125 * 16],  m3

    ; mode 10
    movu                      m1,  [r2 +  1]
    movu                      m2,  [r2 + 17]
    movu         [r0 + 512 * 16],  m1
    movu         [r0 + 513 * 16],  m2
    movu         [r0 + 514 * 16],  m1
    movu         [r0 + 515 * 16],  m2
    movu         [r0 + 516 * 16],  m1
    movu         [r0 + 517 * 16],  m2
    movu         [r0 + 518 * 16],  m1
    movu         [r0 + 519 * 16],  m2
    movu         [r0 + 520 * 16],  m1
    movu         [r0 + 521 * 16],  m2
    movu         [r0 + 522 * 16],  m1
    movu         [r0 + 523 * 16],  m2
    movu         [r0 + 524 * 16],  m1
    movu         [r0 + 525 * 16],  m2
    movu         [r0 + 526 * 16],  m1
    movu         [r0 + 527 * 16],  m2

    movu         [r0 + 528 * 16],  m1
    movu         [r0 + 529 * 16],  m2
    movu         [r0 + 530 * 16],  m1
    movu         [r0 + 531 * 16],  m2
    movu         [r0 + 532 * 16],  m1
    movu         [r0 + 533 * 16],  m2
    movu         [r0 + 534 * 16],  m1
    movu         [r0 + 535 * 16],  m2
    movu         [r0 + 536 * 16],  m1
    movu         [r0 + 537 * 16],  m2
    movu         [r0 + 538 * 16],  m1
    movu         [r0 + 539 * 16],  m2
    movu         [r0 + 540 * 16],  m1
    movu         [r0 + 541 * 16],  m2
    movu         [r0 + 542 * 16],  m1
    movu         [r0 + 543 * 16],  m2

    movu         [r0 + 544 * 16],  m1
    movu         [r0 + 545 * 16],  m2
    movu         [r0 + 546 * 16],  m1
    movu         [r0 + 547 * 16],  m2
    movu         [r0 + 548 * 16],  m1
    movu         [r0 + 549 * 16],  m2
    movu         [r0 + 550 * 16],  m1
    movu         [r0 + 551 * 16],  m2
    movu         [r0 + 552 * 16],  m1
    movu         [r0 + 553 * 16],  m2
    movu         [r0 + 554 * 16],  m1
    movu         [r0 + 555 * 16],  m2
    movu         [r0 + 556 * 16],  m1
    movu         [r0 + 557 * 16],  m2
    movu         [r0 + 558 * 16],  m1
    movu         [r0 + 559 * 16],  m2

    movu         [r0 + 560 * 16],  m1
    movu         [r0 + 561 * 16],  m2
    movu         [r0 + 562 * 16],  m1
    movu         [r0 + 563 * 16],  m2
    movu         [r0 + 564 * 16],  m1
    movu         [r0 + 565 * 16],  m2
    movu         [r0 + 566 * 16],  m1
    movu         [r0 + 567 * 16],  m2
    movu         [r0 + 568 * 16],  m1
    movu         [r0 + 569 * 16],  m2
    movu         [r0 + 570 * 16],  m1
    movu         [r0 + 571 * 16],  m2
    movu         [r0 + 572 * 16],  m1
    movu         [r0 + 573 * 16],  m2
    movu         [r0 + 574 * 16],  m1
    movu         [r0 + 575 * 16],  m2

    ; mode 11 [row 0]
    movu          m0,    [r4]

    ; mode 11 [row 15 - first half]
    movu          [r0 + 606 * 16],  m0

    movu          [r0 + 606 * 16],  m0

    ; mode 12 [row 31]
    pslldq        m6,    m0,          4
    pinsrb        m6,    [r3 + 26],   0
    pinsrb        m6,    [r3 + 19],   1
    pinsrb        m6,    [r3 + 13],   2
    pinsrb        m6,    [r3 +  6],   3
    movu          [r0 + 702 * 16],    m6
    movu          m6,                 [r4 + 12]
    movu          [r0 + 703 * 16],    m6

    ; mode 11 [row 31]
    pslldq        m6,               m0,           1
    pinsrb        m6,               [r3 + 16],    0
    movu          [r0 + 638 * 16],  m6
    movu          m6,               [r4 + 15]
    movu          [r0 + 639 * 16],  m6

    movd          m1,               [r4 + 1]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,        [r5 + 30 * 16]
    pmulhrsw      m1,    m7
    movu          m2,    [r4 + 8]
    movd          m3,    [r4 + 9]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,        [r5 + 30 * 16]
    pmulhrsw      m3,    m7
    packuswb      m1,               m3
    movu          [r0 + 576 * 16],  m1

    movu          m1,    [r4 + 16]

    ; mode 11 [row 15 - second half]
    movu          [r0 + 607 * 16],  m1

    movd          m3,    [r4 + 17]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,        [r5 + 30 * 16]
    pmulhrsw      m3,    m7
    movu          m4,    [r4 + 24]
    movd          m5,    [r4 + 25]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,        [r5 + 30 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 577 * 16],  m3

    ; mode 11 [row 1]
    pmaddubsw     m3,    m0,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 578 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 579 * 16],  m3

    ; mode 11 [row 2]
    pmaddubsw     m3,    m0,        [r5 + 26 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 26 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 580 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 26 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 26 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 581 * 16],  m3

    ; mode 11 [row 3]
    pmaddubsw     m3,    m0,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 582 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 583 * 16],  m3

    ; mode 11 [row 4]
    pmaddubsw     m3,    m0,        [r5 + 22 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 22 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 584 * 16],  m3

    ; mode 12 [row 1 - first half]
    movu          [r0 + 642 * 16],  m3

    pmaddubsw     m3,    m1,        [r5 + 22 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 22 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 585 * 16],  m3

    ; mode 12 [row 1 - second half]
    movu          [r0 + 643 * 16],  m3

    ; mode 11 [row 5]
    pmaddubsw     m3,    m0,        [r5 + 20 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 20 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 586 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 20 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 20 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 587 * 16],  m3

    ; mode 11 [row 6]
    pmaddubsw     m3,    m0,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 18 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 588 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 18 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 589 * 16],  m3

    ; mode 11 [row 7]
    pmaddubsw     m3,    m0,        [r5 + 16 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 16 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 590 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 16 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 16 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 591 * 16],  m3

    ; mode 11 [row 8]
    pmaddubsw     m3,    m0,        [r5 + 14 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 14 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 592 * 16],  m3

    ; mode 13 [row 1 - first half]
    movu          [r0 + 706 * 16],  m3

    pmaddubsw     m3,    m1,        [r5 + 14 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 14 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 593 * 16],  m3

    ; mode 13 [row 1 - second half]
    movu          [r0 + 707 * 16],  m3

    ; mode 11 [row 9]
    pmaddubsw     m3,    m0,        [r5 + 12 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 12 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 594 * 16],  m3

    ; mode 12 [row 3 - first half]
    movu          [r0 + 646 * 16],  m3

    pmaddubsw     m3,    m1,        [r5 + 12 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 12 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 595 * 16],  m3

    ; mode 12 [row 3 - second half]
    movu          [r0 + 647 * 16],  m3

    ; mode 11 [row 10]
    pmaddubsw     m3,    m0,        [r5 + 10 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 10 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 596 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 10 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 10 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 597 * 16],  m3

    ; mode 11 [row 11]
    pmaddubsw     m3,    m0,        [r5 + 8 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 8 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 598 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 8 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 8 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 599 * 16],  m3

    ; mode 11 [row 12]
    pmaddubsw     m3,    m0,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 6 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 600 * 16],  m3

    ; mode 14 [row 1 - first half]
    movu          [r0 + 770 * 16],  m3

    pmaddubsw     m3,    m1,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 6 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 601 * 16],  m3

    ; mode 14 [row 1 - second half]
    movu          [r0 + 771 * 16],  m3

    ; mode 11 [row 13]
    pmaddubsw     m3,    m0,        [r5 + 4 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 4 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 602 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 4 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 4 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 603 * 16],  m3

    ; mode 11 [row 14]
    pmaddubsw     m3,    m0,        [r5 + 2 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 2 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 604 * 16],  m3

    ; mode 13 [row 5 - first half]
    movu          [r0 + 650 * 16],  m3

    pmaddubsw     m3,    m1,        [r5 + 2 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 2 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 605 * 16],  m3

    ; mode 13 [row 5 - second half]
    movu          [r0 + 651 * 16],  m3

    ; mode 12 [row 0]
    pmaddubsw     m3,    m0,        [r5 + 27 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 27 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 640 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 27 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 27 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 641 * 16],  m3

    ; mode 12 [row 2]
    pmaddubsw     m3,    m0,        [r5 + 17 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 17 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 644 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 17 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 17 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 645 * 16],  m3

    ; mode 12 [row 4]
    pmaddubsw     m3,    m0,        [r5 + 7 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 7 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 648 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 7 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 7 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 649 * 16],  m3

    ; mode 13 [row 0]
    pmaddubsw     m3,    m0,        [r5 + 23 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 23 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 704 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 23 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 23 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 705 * 16],  m3

    ; mode 13 [row 2]
    pmaddubsw     m3,    m0,        [r5 + 5 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 5 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 708 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 5 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 5 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 709 * 16],  m3

    ; mode 14 [row 0]
    pmaddubsw     m3,    m0,        [r5 + 19 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 19 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 768 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 19 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 19 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 769 * 16],  m3

    ; mode 15 [row 0]
    pmaddubsw     m3,    m0,        [r5 + 15 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 15 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 832 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 15 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 15 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 833 * 16],  m3

    ; mode 11 [row 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 +  0],   1
    pinsrb        m0,    [r3 + 16],   0
    pmaddubsw     m3,    m0,        [r5 + 30 * 16]
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 8],   1
    pinsrb        m2,    [r4 + 7],   0
    pmaddubsw     m5,    m2,        [r5 + 30 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 608 * 16],  m3
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 16],   1
    pinsrb        m1,    [r4 + 15],   0
    pmaddubsw     m3,    m1,        [r5 + 30 * 16]
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrb        m4,    [r4 + 24],   1
    pinsrb        m4,    [r4 + 23],   0
    pmaddubsw     m5,    m4,        [r5 + 30 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 609 * 16],  m3

    ; mode 11 [row 17]
    pmaddubsw     m3,    m0,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 610 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 611 * 16],  m3

    ; mode 11 [row 18]
    pmaddubsw     m3,    m0,        [r5 + 26 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 26 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 612 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 26 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 26 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 613 * 16],  m3

    ; mode 11 [row 19]
    pmaddubsw     m3,    m0,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 614 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 615 * 16],  m3

    ; mode 11 [row 20]
    pmaddubsw     m3,    m0,        [r5 + 22 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 22 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 616 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 22 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 22 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 617 * 16],  m3

    ; mode 11 [row 21]
    pmaddubsw     m3,    m0,        [r5 + 20 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 20 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 618 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 20 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 20 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 619 * 16],  m3

    ; mode 11 [row 22]
    pmaddubsw     m3,    m0,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 18 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 620 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 18 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 621 * 16],  m3

    ; mode 11 [row 23]
    pmaddubsw     m3,    m0,        [r5 + 16 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 16 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 622 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 16 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 16 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 623 * 16],  m3

    ; mode 11 [row 24]
    pmaddubsw     m3,    m0,        [r5 + 14 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 14 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 624 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 14 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 14 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 625 * 16],  m3

    ; mode 11 [row 25]
    pmaddubsw     m3,    m0,        [r5 + 12 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 12 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 626 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 12 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 12 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 627 * 16],  m3

    ; mode 11 [row 26]
    pmaddubsw     m3,    m0,        [r5 + 10 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 10 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 628 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 10 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 10 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 629 * 16],  m3

    ; mode 11 [row 27]
    pmaddubsw     m3,    m0,        [r5 + 8 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 8 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 630 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 8 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 8 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 631 * 16],  m3

    ; mode 11 [row 28]
    pmaddubsw     m3,    m0,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 6 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 632 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 6 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 633 * 16],  m3

    ; mode 11 [row 29]
    pmaddubsw     m3,    m0,        [r5 + 4 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 4 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 634 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 4 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 4 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 635 * 16],  m3

    ; mode 11 [row 30]
    pmaddubsw     m3,    m0,        [r5 + 2 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 2 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 636 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 2 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 2 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,               m5
    movu          [r0 + 637 * 16],  m3

    ; mode 12 [row 6]
    pinsrb        m0,    [r3 + 6],  0
    pmaddubsw     m3,    m0,        [r5 + 29 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 29 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 652 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 29 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 29 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 653 * 16],  m3

    ; mode 12 [row 7]
    pmaddubsw     m3,    m0,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 654 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 655 * 16],  m3

    ; mode 12 [row 8]
    pmaddubsw     m3,    m0,        [r5 + 19 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 19 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 656 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 19 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 19 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 657 * 16],  m3

    ; mode 12 [row 9]
    pmaddubsw     m3,    m0,        [r5 + 14 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 14 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 658 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 14 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 14 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 659 * 16],  m3

    ; mode 12 [row 10]
    pmaddubsw     m3,    m0,        [r5 + 9 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 9 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 660 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 9 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 9 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 661 * 16],  m3

    ; mode 12 [row 11]
    pmaddubsw     m3,    m0,        [r5 + 4 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 4 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 662 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 4 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 4 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 663 * 16],  m3

    ; mode 13 [row 3]
    movu          m6,    m0
    pinsrb        m6,    [r3 + 4],  0
    pmaddubsw     m3,    m6,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 710 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 711 * 16],  m3

    ; mode 13 [row 4]
    pmaddubsw     m3,    m6,        [r5 + 19 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 19 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 712 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 19 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 19 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 713 * 16],  m3

    ; mode 13 [row 5]
    pmaddubsw     m3,    m6,        [r5 + 10 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 10 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 714 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 10 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 10 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 715 * 16],  m3

    ; mode 13 [row 6]
    pmaddubsw     m3,    m6,        [r5 + 1 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 1 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 716 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 1 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 1 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 717 * 16],  m3

    ; mode 14 [row 2]
    movu          m6,    m0
    pinsrb        m6,    [r4 +  0],  1
    pinsrb        m6,    [r3 +  2],  0
    pmaddubsw     m3,    m6,         [r5 + 25 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,         [r5 + 25 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 772 * 16],   m3
    pmaddubsw     m3,    m1,         [r5 + 25 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,         [r5 + 25 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 773 * 16],   m3

    ; mode 14 [row 3]
    pmaddubsw     m3,    m6,         [r5 + 12 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,         [r5 + 12 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 774 * 16],   m3
    pmaddubsw     m3,    m1,         [r5 + 12 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,         [r5 + 12 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 775 * 16],   m3

    ; mode 15 [row 1]
    pmaddubsw     m3,    m6,        [r5 + 30 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 30 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 834 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 30 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 30 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 835 * 16],  m3

    ; mode 15 [row 2]
    pmaddubsw     m3,    m6,        [r5 + 13 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 13 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 836 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 13 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 13 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 837 * 16],  m3

    ; mode 15 [row 3]
    pslldq        m6,    2
    pinsrb        m6,    [r3 +  2], 1
    pinsrb        m6,    [r3 +  4], 0
    pmaddubsw     m3,    m6,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 7],  1
    pinsrb        m2,    [r4 + 6],  0
    pmaddubsw     m5,    m2,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 838 * 16],  m3
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 15], 1
    pinsrb        m1,    [r4 + 14], 0
    pmaddubsw     m3,    m1,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrb        m4,    [r4 + 23], 1
    pinsrb        m4,    [r4 + 22], 0
    pmaddubsw     m5,    m4,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 839 * 16],  m3

    ; mode 15 [row 4]
    pmaddubsw     m3,    m6,        [r5 + 11 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 11 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 840 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 11 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 11 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 841 * 16],  m3

    ; mode 15 [row 5, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 +  4], 1
    pinsrb        m6,    [r3 +  6], 0
    pmaddubsw     m3,    m6,        [r5 + 26 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 842 * 16],  m3

    ; mode 15 [row 6, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 9 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 844 * 16],  m3

    ; mode 15 [row 7, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 +  6], 1
    pinsrb        m6,    [r3 +  8], 0
    pmaddubsw     m3,    m6,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 846 * 16],  m3

    ; mode 15 [row 8, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 7 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 848 * 16],  m3

    ; mode 15 [row 9, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 +  8], 1
    pinsrb        m6,    [r3 +  9], 0
    pmaddubsw     m3,    m6,        [r5 + 22 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 850 * 16],  m3

    ; mode 15 [row 10, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 5 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 852 * 16],  m3

    ; mode 15 [row 11, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 +  9], 1
    pinsrb        m6,    [r3 + 11], 0
    pmaddubsw     m3,    m6,        [r5 + 20 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 854 * 16],  m3

    ; mode 15 [row 12, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 3 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 856 * 16],  m3

    ; mode 15 [row 13, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 11], 1
    pinsrb        m6,    [r3 + 13], 0
    pmaddubsw     m3,    m6,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 858 * 16],  m3

    ; mode 15 [row 14, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 1 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 860 * 16],  m3

    ; mode 15 [row 15, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 13], 1
    pinsrb        m6,    [r3 + 15], 0
    pmaddubsw     m3,    m6,        [r5 + 16 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 862 * 16],  m3

    ; mode 15 [row 16, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 15], 1
    pinsrb        m6,    [r3 + 17], 0
    pmaddubsw     m3,    m6,        [r5 + 31 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 864 * 16],  m3

    ; mode 15 [row 17, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 14 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 866 * 16],  m3

    ; mode 15 [row 18, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 17], 1
    pinsrb        m6,    [r3 + 19], 0
    pmaddubsw     m3,    m6,        [r5 + 29 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 868 * 16],  m3

    ; mode 15 [row 19, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 12 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 870 * 16],  m3

    ; mode 15 [row 20, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 19], 1
    pinsrb        m6,    [r3 + 21], 0
    pmaddubsw     m3,    m6,        [r5 + 27 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 872 * 16],  m3

    ; mode 15 [row 21, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 10 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 874 * 16],  m3

    ; mode 15 [row 22, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 21], 1
    pinsrb        m6,    [r3 + 23], 0
    pmaddubsw     m3,    m6,        [r5 + 25 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 876 * 16],  m3

    ; mode 15 [row 23, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 8 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 878 * 16],  m3

    ; mode 15 [row 24, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 23], 1
    pinsrb        m6,    [r3 + 24], 0
    pmaddubsw     m3,    m6,        [r5 + 23 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 880 * 16],  m3

    ; mode 15 [row 25, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 882 * 16],  m3

    ; mode 15 [row 26, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 24], 1
    pinsrb        m6,    [r3 + 26], 0
    pmaddubsw     m3,    m6,        [r5 + 21 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 884 * 16],  m3

    ; mode 15 [row 27, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 4 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 886 * 16],  m3

    ; mode 15 [row 28, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 26], 1
    pinsrb        m6,    [r3 + 28], 0
    pmaddubsw     m3,    m6,        [r5 + 19 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 888 * 16],  m3

    ; mode 15 [row 29, 0-7]
    pmaddubsw     m3,    m6,        [r5 + 2 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 890 * 16],  m3

    ; mode 15 [row 30, 0-7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 28], 1
    pinsrb        m6,    [r3 + 30], 0
    pmaddubsw     m3,    m6,        [r5 + 17 * 16]
    pmulhrsw      m3,    m7
    packuswb      m3,    m3
    movh          [r0 + 892 * 16],  m3

    ; mode 15 [row 31, 0-7]
    pshufb        m3,    m6,           [tab_S2]
    movh          [r0 + 894 * 16],     m3

    ; mode 12 [row 12]
    pslldq        m0,    2
    pinsrb        m0,    [r3 +  6], 1
    pinsrb        m0,    [r3 + 13], 0
    pmaddubsw     m3,    m0,        [r5 + 31 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 31 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 664 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 31 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 31 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 665 * 16],  m3

    ; mode 12 [row 13]
    pmaddubsw     m3,    m0,        [r5 + 26 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 26 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 666 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 26 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 26 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 667 * 16],  m3

    ; mode 12 [row 14]
    pmaddubsw     m3,    m0,        [r5 + 21 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 21 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 668 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 21 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 21 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 669 * 16],  m3

    ; mode 12 [row 15]
    pmaddubsw     m3,    m0,        [r5 + 16 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 16 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 670 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 16 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 16 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 671 * 16],  m3

    ; mode 12 [row 16]
    pmaddubsw     m3,    m0,        [r5 + 11 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 11 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 672 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 11 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 11 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 673 * 16],  m3

    ; mode 12 [row 17]
    pmaddubsw     m3,    m0,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 6 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 674 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 6 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 675 * 16],  m3

    ; mode 12 [row 18]
    pmaddubsw     m3,    m0,        [r5 + 1 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 1 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 676 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 1 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 1 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 677 * 16],  m3

    ; mode 13 [row 7]
    movu          m6,    m0
    pinsrb        m6,    [r3 + 4],  2
    pinsrb        m6,    [r3 + 4],  1
    pinsrb        m6,    [r3 + 7],  0
    pmaddubsw     m3,    m6,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 718 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 719 * 16],  m3

    ; mode 13 [row 8]
    pmaddubsw     m3,    m6,        [r5 + 15 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 15 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 720 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 15 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 15 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 721 * 16],  m3

    ; mode 13 [row 9]
    pmaddubsw     m3,    m6,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 6 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 722 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 6 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 6 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 723 * 16],  m3

    ; mode 14 [row 4]
    pinsrb        m6,    [r3 + 2],  2
    pinsrb        m6,    [r3 + 2],  1
    pinsrb        m6,    [r3 + 5],  0
    pmaddubsw     m3,    m6,        [r5 + 31 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 31 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 776 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 31 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 31 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 777 * 16],  m3

    ; mode 14 [row 5]
    pmaddubsw     m3,    m6,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 18 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 778 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 18 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 779 * 16],  m3

    ; mode 14 [row 6]
    pmaddubsw     m3,    m6,        [r5 + 5 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 5 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 780 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 5 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 5 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 781 * 16],  m3

    ; mode 14 [row 7]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 5], 1
    pinsrb        m6,    [r3 + 7], 0
    pmaddubsw     m3,    m6,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw         m2,    [r4 + 5], 0
    pmaddubsw     m5,    m2,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 782 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 13], 0
    pmaddubsw     m3,    m1,        [r5 + 24 * 16]
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 21], 0
    pmaddubsw     m5,    m4,        [r5 + 24 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 783 * 16],  m3

    ; mode 14 [row 8]
    pmaddubsw     m3,    m6,        [r5 + 11 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 11 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 784 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 11 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 11 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 785 * 16],  m3

    ; mode 15 [row 5, 8-31]
    pmaddubsw     m5,    m2,            [r5 + 26 * 16]
    pmulhrsw      m5,    m7
    packuswb      m5,    m5
    movh          [r0 + 842 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 26 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            [r5 + 26 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 843 * 16],      m3

    ; mode 15 [row 6, 8-31]
    pmaddubsw     m5,    m2,            [r5 + 9 * 16]
    pmulhrsw      m5,    m7
    packuswb      m5,    m5
    movh          [r0 + 844 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 9 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            [r5 + 9 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 845 * 16],      m3

    ; mode 12 [row 19]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 13], 1
    pinsrb        m0,    [r3 + 19], 0
    pmaddubsw     m3,    m0,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 678 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 28 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 28 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 679 * 16],  m3

    ; mode 12 [row 20]
    pmaddubsw     m3,    m0,        [r5 + 23 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 23 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 680 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 23 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 23 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 681 * 16],  m3

    ; mode 12 [row 21]
    pmaddubsw     m3,    m0,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 18 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 682 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 18 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 18 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 683 * 16],  m3

    ; mode 12 [row 22]
    pmaddubsw     m3,    m0,        [r5 + 13 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 13 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 684 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 13 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 13 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 685 * 16],  m3

    ; mode 12 [row 23]
    pmaddubsw     m3,    m0,        [r5 + 8 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 8 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 686 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 8 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 8 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 687 * 16],  m3

    ; mode 12 [row 24]
    pmaddubsw     m3,    m0,        [r5 + 3 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,        [r5 + 3 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 688 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 3 * 16]
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,        [r5 + 3 * 16]
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 689 * 16],  m3

    ; mode 13 [row 10]
    movu          m7,    m6
    movu          m6,    m0
    pinsrb        m6,    [r3 + 4],  4
    pinsrb        m6,    [r3 + 4],  3
    pinsrb        m6,    [r3 + 7],  2
    pinsrb        m6,    [r3 + 7],  1
    pinsrb        m6,    [r3 + 11], 0
    pmaddubsw     m3,    m6,        [r5 + 29 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 29 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 724 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 29 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 29 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 725 * 16],  m3

    ; mode 13 [row 11]
    pmaddubsw     m3,    m6,        [r5 + 20 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 20 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 726 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 20 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 20 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 727 * 16],  m3

    ; mode 13 [row 12]
    pmaddubsw     m3,    m6,        [r5 + 11 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 11 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 728 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 11 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 11 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 729 * 16],  m3

    ; mode 13 [row 13]
    pmaddubsw     m3,    m6,        [r5 + 2 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 2 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 730 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 2 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 2 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 731 * 16],  m3

    ; mode 14 [row 9]
    pslldq        m7,    2
    pinsrb        m7,    [r3 +  7], 1
    pinsrb        m7,    [r3 + 10], 0
    pmaddubsw     m3,    m7,        [r5 + 30 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrw        m2,     [r4 + 4],  0
    pmaddubsw     m5,    m2,        [r5 + 30 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 786 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 12], 0
    pmaddubsw     m3,    m1,        [r5 + 30 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrb        m4,    [r4 + 21], 1
    pinsrb        m4,    [r4 + 20], 0
    pmaddubsw     m5,    m4,        [r5 + 30 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 787 * 16],  m3

    ; mode 14 [row 10]
    pmaddubsw     m3,    m7,        [r5 + 17 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 17 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 788 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 17 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 17 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 789 * 16],  m3

    ; mode 14 [row 11]
    pmaddubsw     m3,    m7,        [r5 + 4 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 4 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 790 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 4 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 4 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 791 * 16],  m3

    movu          m6,    [pw_1024]

    ; mode 15 [row 7, 8-31]
    pmaddubsw     m5,    m2,            [r5 + 24 * 16]
    pmulhrsw      m5,    m6
    packuswb      m5,    m5
    movh          [r0 + 846 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 24 * 16]
    pmulhrsw      m3,    m6
    pmaddubsw     m5,    m4,            [r5 + 24 * 16]
    pmulhrsw      m5,    m6
    packuswb      m3,    m5
    movu          [r0 + 847 * 16],      m3

    ; mode 15 [row 8, 8-31]
    pmaddubsw     m5,    m2,            [r5 + 7 * 16]
    pmulhrsw      m5,    m6
    packuswb      m5,    m5
    movh          [r0 + 848 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 7 * 16]
    pmulhrsw      m3,    m6
    pmaddubsw     m5,    m4,            [r5 + 7 * 16]
    pmulhrsw      m5,    m6
    packuswb      m3,    m5
    movu          [r0 + 849 * 16],      m3

    ; mode 12 [row 25]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 19], 1
    pinsrb        m0,    [r3 + 26], 0
    pmaddubsw     m3,    m0,        [r5 + 30 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 30 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 690 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 30 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 30 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 691 * 16],  m3

    ; mode 12 [row 26]
    pmaddubsw     m3,    m0,        [r5 + 25 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 25 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 692 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 25 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 25 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 693 * 16],  m3

    ; mode 12 [row 27]
    pmaddubsw     m3,    m0,        [r5 + 20 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 20 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 694 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 20 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 20 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 695 * 16],  m3

    ; mode 12 [row 28]
    pmaddubsw     m3,    m0,        [r5 + 15 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 15 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 696 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 15 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 15 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 697 * 16],  m3

    ; mode 12 [row 29]
    pmaddubsw     m3,    m0,        [r5 + 10 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 10 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 698 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 10 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 10 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 699 * 16],  m3

    ; mode 12 [row 30]
    pmaddubsw     m3,    m0,        [r5 + 5 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 5 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 700 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 5 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 5 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 701 * 16],  m3

    ; mode 13 [row 14]
    movu          m6,    m0
    pinsrb        m6,    [r3 +  4], 6
    pinsrb        m6,    [r3 +  4], 5
    pinsrb        m6,    [r3 +  7], 4
    pinsrb        m6,    [r3 +  7], 3
    pinsrb        m6,    [r3 + 11], 2
    pinsrb        m6,    [r3 + 11], 1
    pinsrb        m6,    [r3 + 14], 0
    pmaddubsw     m3,    m6,        [r5 + 25 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 25 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 732 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 25 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 25 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 733 * 16],  m3

    ; mode 13 [row 15]
    pmaddubsw     m3,    m6,        [r5 + 16 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 16 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 734 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 16 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 16 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 735 * 16],  m3

    ; mode 13 [row 16]
    pmaddubsw     m3,    m6,        [r5 + 7 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 7 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 736 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 7 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 7 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 737 * 16],  m3

    ; mode 13 [row 17]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 14],  1
    pinsrb        m6,    [r3 + 18],  0
    pmaddubsw     m3,    m6,         [r5 + 30 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrw        m2,     [r4 + 3],  0
    pmaddubsw     m5,    m2,        [r5 + 30 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 738 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 11], 0
    pmaddubsw     m3,    m1,        [r5 + 30 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 19], 0
    pmaddubsw     m5,    m4,        [r5 + 30 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,               m5
    movu          [r0 + 739 * 16],  m3

    ; mode 13 [row 18]
    pmaddubsw     m3,    m6,        [r5 + 21 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 21 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 740 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 21 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 21 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 741 * 16],  m3

    ; mode 13 [row 19]
    pmaddubsw     m3,    m6,        [r5 + 12 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 12 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 742 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 12 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 12 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 743 * 16],  m3

    ; mode 13 [row 20]
    pmaddubsw     m3,    m6,        [r5 + 3 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 3 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 744 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 3 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 3 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 745 * 16],  m3

    ; mode 14 [row 12]
    pslldq        m7,    2
    pinsrb        m7,    [r3 + 10], 1
    pinsrb        m7,    [r3 + 12], 0
    pmaddubsw     m3,    m7,        [r5 + 23 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 23 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 792 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 23 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 23 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 793 * 16],  m3

    ; mode 14 [row 13]
    pmaddubsw     m3,    m7,        [r5 + 10 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 10 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 794 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 10 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 10 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 795 * 16],  m3

    ; mode 15 [row 9]
    pmaddubsw     m5,    m2,            [r5 + 22 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movu          [r0 + 850 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 22 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 22 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 851 * 16],      m3

    ; mode 15 [row 10]
    pmaddubsw     m5,    m2,            [r5 + 5 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movu          [r0 + 852 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 5 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 5 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 853 * 16],      m3

    ; mode 13 [row 21]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 18],  1
    pinsrb        m6,    [r3 + 21],  0
    pmaddubsw     m3,    m6,         [r5 + 26 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 2],  0
    pmaddubsw     m5,    m2,        [r5 + 26 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 746 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 10], 0
    pmaddubsw     m3,    m1,        [r5 + 26 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 18], 0
    pmaddubsw     m5,    m4,        [r5 + 26 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 747 * 16],  m3

    ; mode 13 [row 22]
    pmaddubsw     m3,    m6,        [r5 + 17 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 17 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 748 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 17 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 17 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 749 * 16],  m3

    ; mode 13 [row 23]
    pmaddubsw     m3,    m6,        [r5 + 8 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 8 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 750 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 8 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 8 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 751 * 16],  m3

    ; mode 14 [row 14]
    pslldq        m7,    2
    pinsrb        m7,    [r3 + 12], 1
    pinsrb        m7,    [r3 + 15], 0
    pmaddubsw     m3,    m7,        [r5 + 29 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 29 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 796 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 29 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 29 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 797 * 16],  m3

    ; mode 14 [row 15]
    pmaddubsw     m3,    m7,        [r5 + 16 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 16 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 798 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 16 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 16 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 799 * 16],  m3

    ; mode 14 [row 16]
    pmaddubsw     m3,    m7,        [r5 + 3 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 3 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 800 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 3 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 3 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 801 * 16],  m3

    ; mode 15 [row 11]
    pmaddubsw     m5,    m2,            [r5 + 20 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 854 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 20 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 20 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 855 * 16],      m3

    ; mode 15 [row 12]
    pmaddubsw     m5,    m2,            [r5 + 3 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 856 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 3 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 3 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 857 * 16],      m3

    ; mode 13 [row 24]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 21],  1
    pinsrb        m6,    [r3 + 25],  0
    pmaddubsw     m3,    m6,         [r5 + 31 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 1],  0
    pmaddubsw     m5,    m2,        [r5 + 31 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 752 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 9],  0
    pmaddubsw     m3,    m1,        [r5 + 31 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 17], 0
    pmaddubsw     m5,    m4,        [r5 + 31 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 753 * 16],  m3

    ; mode 13 [row 25]
    pmaddubsw     m3,    m6,        [r5 + 22 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 22 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 754 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 22 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 22 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 755 * 16],  m3

    ; mode 13 [row 26]
    pmaddubsw     m3,    m6,        [r5 + 13 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 13 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 756 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 13 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 13 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 757 * 16],  m3

    ; mode 13 [row 27]
    pmaddubsw     m3,    m6,        [r5 + 4 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 4 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 758 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 4 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 4 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 759 * 16],  m3

    ; mode 14 [row 17]
    pslldq        m7,    2
    pinsrb        m7,    [r3 + 15], 1
    pinsrb        m7,    [r3 + 17], 0
    pmaddubsw     m3,    m7,        [r5 + 22 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 22 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 802 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 22 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 22 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 803 * 16],  m3

    ; mode 14 [row 18]
    pmaddubsw     m3,    m7,        [r5 + 9 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 9 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 804 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 9 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 9 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 805 * 16],  m3

    ; mode 15 [row 13]
    pmaddubsw     m5,    m2,           [r5 + 18 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 858 * 16 + 8], m5
    pmaddubsw     m3,    m1,           [r5 + 18 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,           [r5 + 18 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 859 * 16],     m3

    ; mode 15 [row 14]
    pmaddubsw     m5,    m2,           [r5 + 1 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 860 * 16 + 8], m5
    pmaddubsw     m3,    m1,           [r5 + 1 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,           [r5 + 1 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 861 * 16],     m3

    ; mode 13 [row 28]
    pslldq        m6,    2
    pinsrb        m6,    [r3 + 25],  1
    pinsrb        m6,    [r3 + 28],  0
    pmaddubsw     m3,    m6,         [r5 + 27 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 0],  0
    pmaddubsw     m5,    m2,         [r5 + 27 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 760 * 16],   m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 8],  0
    pmaddubsw     m3,    m1,         [r5 + 27 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 16],  0
    pmaddubsw     m5,    m4,         [r5 + 27 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 761 * 16],   m3

    ; mode 13 [row 29]
    pmaddubsw     m3,    m6,         [r5 + 18 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,         [r5 + 18 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 762 * 16],   m3
    pmaddubsw     m3,    m1,         [r5 + 18 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,         [r5 + 18 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 763 * 16],   m3

    ; mode 13 [row 30]
    pmaddubsw     m3,    m6,         [r5 + 9 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,         [r5 + 9 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 764 * 16],   m3
    pmaddubsw     m3,    m1,         [r5 + 9 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,         [r5 + 9 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 765 * 16],   m3

    ; mode 14 [row 19]
    pslldq        m7,    2
    pinsrb        m7,    [r3 + 17], 1
    pinsrb        m7,    [r3 + 20], 0
    pmaddubsw     m3,    m7,        [r5 + 28 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 28 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 806 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 28 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 28 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 807 * 16],  m3

    ; mode 14 [row 20]
    pmaddubsw     m3,    m7,        [r5 + 15 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 15 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 808 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 15 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 15 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 809 * 16],  m3

    ; mode 14 [row 21]
    pmaddubsw     m3,    m7,        [r5 + 2 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 2 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 810 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 2 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 2 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 811 * 16],  m3

    ; mode 15 [row 15]
    pmaddubsw     m5,    m2,            [r5 + 16 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 862 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 16 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 16 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 863 * 16],      m3

    ; mode 14 [row 22]
    pslldq        m7,    2
    pinsrb        m7,    [r3 + 20],  1
    pinsrb        m7,    [r3 + 22],  0
    pmaddubsw     m3,    m7,         [r5 + 21 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 0],  1
    pinsrb        m2,    [r3 + 2],  0
    pmaddubsw     m5,    m2,        [r5 + 21 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 812 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 7],  0
    pmaddubsw     m3,    m1,        [r5 + 21 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 15],  0
    pmaddubsw     m5,    m4,        [r5 + 21 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 813 * 16],  m3

    ; mode 14 [row 23]
    pmaddubsw     m3,    m7,        [r5 + 8 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 8 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 814 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 8 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 8 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 815 * 16],  m3

    ; mode 15 [row 16]
    pmaddubsw     m5,    m2,            [r5 + 31 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 864 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 31 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 31 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 865 * 16],  m3

    ; mode 15 [row 17]
    pmaddubsw     m5,    m2,            [r5 + 14 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 866 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 14 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 14 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 867 * 16],      m3

    ; mode 14 [row 24]
    pslldq        m7,    2
    pinsrb        m7,    [r3 + 22],  1
    pinsrb        m7,    [r3 + 25],  0
    pmaddubsw     m3,    m7,         [r5 + 27 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 2],  1
    pinsrb        m2,    [r3 + 5],  0
    pmaddubsw     m5,    m2,        [r5 + 27 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 816 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,     [r4 + 6],  0
    pmaddubsw     m3,    m1,        [r5 + 27 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 14],  0
    pmaddubsw     m5,    m4,        [r5 + 27 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 817 * 16],  m3

    ; mode 14 [row 25]
    pmaddubsw     m3,    m7,        [r5 + 14 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 14 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 818 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 14 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 14 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 819 * 16],  m3

    ; mode 14 [row 26]
    pmaddubsw     m3,    m7,        [r5 + 1 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 1 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 820 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 1 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 1 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 821 * 16],  m3

    ; mode 15 [row 18]
    pinsrb        m2,    [r3 + 4],      0
    pmaddubsw     m5,    m2,            [r5 + 29 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 868 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 29 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 29 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 869 * 16],      m3

    ; mode 15 [row 19]
    pmaddubsw     m5,    m2,            [r5 + 12 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 870 * 16 + 8],  m5
    pmaddubsw     m3,    m1,            [r5 + 12 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,            [r5 + 12 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 871 * 16],      m3

    ; mode 15 [row 20 - 8 to 15]
    pslldq        m3,     m2,           2
    pinsrb        m3,    [r3 + 4],      1
    pinsrb        m3,    [r3 + 6],      0
    pmaddubsw     m5,    m3,            [r5 + 27 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 872 * 16 + 8],  m5

    ; mode 15 [row 21 - 8 to 15]
    pmaddubsw     m5,    m3,            [r5 + 10 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 874 * 16 + 8],  m5

    ; mode 15 [row 22 - 8 to 15]
    pslldq        m3,    2
    pinsrb        m3,    [r3 + 6],      1
    pinsrb        m3,    [r3 + 8],      0
    pmaddubsw     m5,    m3,            [r5 + 25 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 876 * 16 + 8],  m5

    ; mode 15 [row 23 - 8 to 15]
    pmaddubsw     m5,    m3,            [r5 + 8 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 878 * 16 + 8],  m5

    ; mode 15 [row 24 - 8 to 15]
    pslldq        m3,    2
    pinsrb        m3,    [r3 + 8],      1
    pinsrb        m3,    [r3 + 9],      0
    pmaddubsw     m5,    m3,            [r5 + 23 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 880 * 16 + 8],  m5

    ; mode 15 [row 25 - 8 to 15]
    pmaddubsw     m5,    m3,            [r5 + 6 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 882 * 16 + 8],  m5

    ; mode 15 [row 26 - 8 to 15]
    pslldq        m3,    2
    pinsrb        m3,    [r3 +  9],      1
    pinsrb        m3,    [r3 + 11],      0
    pmaddubsw     m5,    m3,             [r5 + 21 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 884 * 16 + 8],   m5

    ; mode 15 [row 27 - 8 to 15]
    pmaddubsw     m5,    m3,             [r5 + 4 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 886 * 16 + 8],   m5

    ; mode 15 [row 28 - 8 to 15]
    pslldq        m3,    2
    pinsrb        m3,    [r3 + 11],      1
    pinsrb        m3,    [r3 + 13],      0
    pmaddubsw     m5,    m3,             [r5 + 19 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 888 * 16 + 8],   m5

    ; mode 15 [row 29 - 8 to 15]
    pmaddubsw     m5,    m3,             [r5 + 2 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 890 * 16 + 8],   m5

    ; mode 15 [row 30 - 8 to 15]
    pslldq        m3,    2
    pinsrb        m3,    [r3 + 13],      1
    pinsrb        m3,    [r3 + 15],      0
    pmaddubsw     m5,    m3,             [r5 + 17 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m5,    m5
    movh          [r0 + 892 * 16 + 8],   m5

    ; mode 15 [row 31, 8 to 15]
    pshufb        m5,    m3,           [tab_S2]
    movh          [r0 + 894 * 16 + 8],     m5

    ; mode 14 [row 27]
    pinsrb        m2,    [r3 + 5],      0
    pslldq        m7,    2
    pinsrb        m7,    [r3 + 25],  1
    pinsrb        m7,    [r3 + 27],  0
    pmaddubsw     m3,    m7,         [r5 + 20 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 5],  1
    pinsrb        m2,    [r3 + 7],  0
    pmaddubsw     m5,    m2,        [r5 + 20 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 822 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 5],  0
    pmaddubsw     m3,    m1,        [r5 + 20 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 13],  0
    pmaddubsw     m5,    m4,        [r5 + 20 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 823 * 16],  m3

    ; mode 15 [row 20 - 16 to 31]
    pmaddubsw     m3,    m1,        [r5 + 27 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 27 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 873 * 16],  m3

    ; mode 15 [row 21 - 16 to 31]
    pmaddubsw     m3,    m1,        [r5 + 10 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 10 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 875 * 16],  m3

    ; mode 14 [row 28]
    pmaddubsw     m3,    m7,        [r5 + 7 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,        [r5 + 7 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 824 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 7 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 7 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 825 * 16],  m3

    ; mode 14 [row 29]
    pslldq        m7,    2
    pinsrb        m7,    [r3 + 27],  1
    pinsrb        m7,    [r3 + 30],  0
    pmaddubsw     m3,    m7,         [r5 + 26 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m2,    2
    pinsrb        m2,    [r3 +  7],  1
    pinsrb        m2,    [r3 + 10],  0
    pmaddubsw     m5,    m2,         [r5 + 26 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 826 * 16],  m3
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 4],  0
    pmaddubsw     m3,    m1,        [r5 + 26 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 12], 0
    pmaddubsw     m5,    m4,        [r5 + 26 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 827 * 16],  m3

    ; mode 14 [row 30]
    pmaddubsw     m3,    m7,         [r5 + 13 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m2,         [r5 + 13 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 828 * 16],  m3
    pmaddubsw     m3,    m1,        [r5 + 13 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 13 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 829 * 16],  m3

    ; mode 15 [row 22]
    pmaddubsw     m3,    m1,        [r5 + 25 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 25 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 877 * 16],  m3

    ; mode 15 [row 23]
    pmaddubsw     m3,    m1,        [r5 + 8 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 8 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 879 * 16],  m3

    ; mode 14 [row 31]
    pshufb        m3,    m7,           [tab_S2]
    movh          [r0 + 830 * 16],     m3
    pshufb        m3,    m2,           [tab_S2]
    movh          [r0 + 830 * 16 + 8], m3
    pshufb        m3,    m1,           [tab_S2]
    movh          [r0 + 831 * 16],     m3
    pshufb        m3,    m4,           [tab_S2]
    movh          [r0 + 831 * 16 + 8], m3

    ; mode 13 [row 31]
    pshufb        m0,    m6,           [tab_S2]
    movh          [r0 + 766 * 16],     m0
    movh          m0,                  [r4]
    movh          [r0 + 766 * 16 + 8], m0
    movu          m0,                  [r4 + 8]
    movu          [r0 + 767 * 16],     m0

    ; mode 15 [row 24]
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 3], 0
    pmaddubsw     m3,    m1,        [r5 + 23 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 11], 0
    pmaddubsw     m5,    m4,        [r5 + 23 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 881 * 16],  m3

    ; mode 15 [row 25]
    pmaddubsw     m3,    m1,        [r5 + 6 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 6 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 883 * 16],  m3

    ; mode 15 [row 26]
    pslldq        m1,    2
    pinsrw        m1,     [r4 + 2], 0
    pmaddubsw     m3,    m1,        [r5 + 21 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 10], 0
    pmaddubsw     m5,    m4,        [r5 + 21 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 885 * 16],  m3

    ; mode 15 [row 27]
    pmaddubsw     m3,    m1,        [r5 + 4 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 4 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 887 * 16],  m3

    ; mode 15 [row 28]
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 1],  0
    pmaddubsw     m3,    m1,        [r5 + 19 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 9],  0
    pmaddubsw     m5,    m4,        [r5 + 19 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 889 * 16],  m3

    ; mode 15 [row 29]
    pmaddubsw     m3,    m1,        [r5 + 2 * 16]
    pmulhrsw      m3,    [pw_1024]
    pmaddubsw     m5,    m4,        [r5 + 2 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 891 * 16],  m3

    ; mode 15 [row 30]
    pslldq        m1,    2
    pinsrw        m1,    [r4 + 0],  0
    pmaddubsw     m3,    m1,        [r5 + 17 * 16]
    pmulhrsw      m3,    [pw_1024]
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 8], 0
    pmaddubsw     m5,    m4,        [r5 + 17 * 16]
    pmulhrsw      m5,    [pw_1024]
    packuswb      m3,    m5
    movu          [r0 + 893 * 16],  m3

    ; mode 15 [row 31]
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 895 * 16],      m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 895 * 16 + 8],  m5

    ; mode 16 [row 0]
    movu          m6,    [r5 + 11 * 16]
    movu          m7,    [pw_1024]
    movh          m0,    [r4     ]
    movh          m1,    [r4 + 1 ]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,            m6
    pmulhrsw      m1,    m7
    movh          m2,    [r4 +  8]
    movh          m3,    [r4 +  9]
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,            m6
    pmulhrsw      m3,    m7
    packuswb      m1,    m3
    movu          [r0 + 896 * 16],      m1

    movh          m1,    [r4 + 16]
    movh          m3,    [r4 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movh          m4,    [r4 + 24]
    movh          m5,    [r4 + 25]
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 897 * 16],      m3

    ; mode16 [row 1]
    movu          m6,    [r5 + 22 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4],          1
    pinsrb        m0,    [r3 + 2],      0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 7],  0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 898 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,     [r4 + 15],    0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 23],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 899 * 16],      m3

    ; mode16 [row 2]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 900 * 16],      m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 901 * 16],      m3

    ; mode16 [row 3]
    movu          m6,    [r5 + 12 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 2],      1
    pinsrb        m0,    [r3 + 3],      0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,     [r4 + 6],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 902 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,     [r4 + 14],    0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 22],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 903 * 16],      m3

    ; mode16 [row 4]
    movu          m6,    [r5 + 23 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 3],      1
    pinsrb        m0,    [r3 + 5],      0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 904 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 13],     0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 21],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 905 * 16],      m3

    ; mode16 [row 5]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 906 * 16],      m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 907 * 16],      m3

    ; mode16 [row 6]
    movu          m6,    [r5 + 13 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 5],      1
    pinsrb        m0,    [r3 + 6],      0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 5],      1
    pinsrb        m2,    [r4 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 908 * 16],      m3
    pslldq        m1,    2
    pinsrw        m1,     [r4 + 12],    0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,     [r4 + 20],    0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 909 * 16],      m3

    ; mode16 [row 7]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 6],      1
    pinsrb        m0,    [r3 + 8],      0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,     [r4 + 3],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 910 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,     [r4 + 11],    0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,     [r4 + 19],    0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 911 * 16],      m3

    ; mode16 [row 8]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 912 * 16],      m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 913 * 16],      m3

    ; mode16 [row 9]
    movu          m6,    [r5 + 14 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 8],      1
    pinsrb        m0,    [r3 + 9],      0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,     [r4 + 2],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 914 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 10],     0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 18],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 915 * 16],      m3

    ; mode16 [row 10]
    movu          m6,    [r5 + 25 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 +  9],      1
    pinsrb        m0,    [r3 + 11],      0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,     [r4 + 1],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 916 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 9],      0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrb        m4,    [r4 + 18],     1
    pinsrb        m4,    [r4 + 17],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 917 * 16],      m3

    ; mode16 [row 11]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 918 * 16],      m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 919 * 16],      m3

    ; mode16 [row 12]
    movu          m6,    [r5 + 15 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 11],     1
    pinsrb        m0,    [r3 + 12],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 0],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 920 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 8],    0
    pmaddubsw     m3,    m1,           m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 16],   0
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 921 * 16],     m3

    ; mode16 [row 13]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 12],     1
    pinsrb        m0,    [r3 + 14],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 0],      1
    pinsrb        m2,    [r3 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 922 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,     [r4 + 7],    0
    pmaddubsw     m3,    m1,           m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,     [r4 + 15],   0
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 923 * 16],     m3

    ; mode16 [row 14]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 924 * 16],      m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 925 * 16],      m3

    ; mode16 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 14],     1
    pinsrb        m0,    [r3 + 15],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 2],      1
    pinsrb        m2,    [r3 + 3],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 926 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 6],      0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 14],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 927 * 16],      m3

    ; mode16 [row 16]
    movu          m6,    [r5 + 27 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 15],     1
    pinsrb        m0,    [r3 + 17],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 3],      1
    pinsrb        m2,    [r3 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 928 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 5],      0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 13],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 929 * 16],      m3

    ; mode16 [row 17]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 930 * 16],      m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 931 * 16],      m3

    ; mode16 [row 18]
    movu          m6,    [r5 + 17 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 17],     1
    pinsrb        m0,    [r3 + 18],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 5],      1
    pinsrb        m2,    [r3 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 932 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 4],      0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 12],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 933 * 16],      m3

    ; mode16 [row 19]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 18],     1
    pinsrb        m0,    [r3 + 20],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 6],      1
    pinsrb        m2,    [r3 + 8],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 934 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 3],      0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 11],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 935 * 16],      m3

    ; mode16 [row 20]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 936 * 16],      m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 937 * 16],      m3

    ; mode16 [row 21]
    movu          m6,    [r5 + 18 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 20],     1
    pinsrb        m0,    [r3 + 21],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 8],      1
    pinsrb        m2,    [r3 + 9],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 938 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 2],      0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 10],     0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 939 * 16],      m3

    ; mode16 [row 22]
    movu          m6,    [r5 + 29 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 21],     1
    pinsrb        m0,    [r3 + 23],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 +  9],     1
    pinsrb        m2,    [r3 + 11],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 940 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 1],      0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 9],      0
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 941 * 16],      m3

    ; mode16 [row 23]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 942 * 16],      m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 943 * 16],      m3

    ; mode16 [row 24]
    movu          m6,    [r5 + 19 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 23],     1
    pinsrb        m0,    [r3 + 24],     0
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 11],     1
    pinsrb        m2,    [r3 + 12],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 944 * 16],      m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 0],      0
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 8],     0
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 945 * 16],     m3

    ; mode16 [row 25]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 24],    1
    pinsrb        m0,    [r3 + 26],    0
    pmaddubsw     m3,    m0,           m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 12],    1
    pinsrb        m2,    [r3 + 14],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 946 * 16],     m3

    pslldq        m1,    2
    pinsrb        m1,    [r4 + 0],     1
    pinsrb        m1,    [r3 + 2],     0
    pmaddubsw     m3,    m1,           m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 7],     0
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 947 * 16],     m3

    ; mode16 [row 26]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m3,    m0,           m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 948 * 16],     m3

    pmaddubsw     m3,    m1,           m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 949 * 16],     m3

    ; mode16 [row 27]
    movu          m6,    [r5 + 20 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 26],    1
    pinsrb        m0,    [r3 + 27],    0
    pmaddubsw     m3,    m0,           m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 14],    1
    pinsrb        m2,    [r3 + 15],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 950 * 16],     m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 2],     1
    pinsrb        m1,    [r3 + 3],     0
    pmaddubsw     m3,    m1,           m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 6],     0
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 951 * 16],     m3

    ; mode16 [row 28]
    movu          m6,    [r5 + 31 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 27],    1
    pinsrb        m0,    [r3 + 29],    0
    pmaddubsw     m3,    m0,           m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 15],    1
    pinsrb        m2,    [r3 + 17],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 952 * 16],     m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 3],     1
    pinsrb        m1,    [r3 + 5],     0
    pmaddubsw     m3,    m1,           m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 5],     0
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 953 * 16],     m3

    ; mode16 [row 29]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m3,    m0,           m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 954 * 16],     m3

    pmaddubsw     m3,    m1,           m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 955 * 16],     m3

    ; mode16 [row 30]
    movu          m6,    [r5 + 21 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 29],    1
    pinsrb        m0,    [r3 + 30],    0
    pmaddubsw     m3,    m0,           m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 17],    1
    pinsrb        m2,    [r3 + 18],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 956 * 16],     m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 5],     1
    pinsrb        m1,    [r3 + 6],     0
    pmaddubsw     m3,    m1,           m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 4],     0
    pmaddubsw     m5,    m4,           m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 957 * 16],     m3

    ; mode16 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 958 * 16],      m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 958 * 16 + 8],  m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 959 * 16],      m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 959 * 16 + 8],  m5

    ; mode 17 [row 0]
    movu          m6,    [r5 + 6 * 16]
    movu          m7,    [pw_1024]
    movh          m0,    [r4     ]
    movh          m1,    [r4 + 1 ]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,            m6
    pmulhrsw      m1,    m7
    movh          m2,    [r4 +  8]
    movh          m3,    [r4 +  9]
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,            m6
    pmulhrsw      m3,    m7
    packuswb      m1,    m3
    movu          [r0 + 960 * 16],      m1

    movh          m1,    [r4 + 16]
    movh          m3,    [r4 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movh          m4,    [r4 + 24]
    movh          m5,    [r4 + 25]
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 961 * 16],      m3

    ; mode17 [row 1]
    movu          m6,    [r5 + 12 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 0],    1
    pinsrb        m0,    [r3 + 1],    0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 7],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 962 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 15],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 23],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 963 * 16],    m3

    ; mode17 [row 2]
    movu          m6,    [r5 + 18 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 1],    1
    pinsrb        m0,    [r3 + 2],    0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 6],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 964 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 14],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 22],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 965 * 16],    m3

    ; mode17 [row 3]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 2],    1
    pinsrb        m0,    [r3 + 4],    0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 5],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 966 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 13],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 21],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 967 * 16],    m3

    ; mode17 [row 4]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 4],    1
    pinsrb        m0,    [r3 + 5],    0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 4],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 968 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 12],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 20],  0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 969 * 16],    m3

    ; mode17 [row 5]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 970 * 16],    m3

    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 971 * 16],    m3

    ; mode17 [row 6]
    movu          m6,    [r5 + 10 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 5],    1
    pinsrb        m0,    [r3 + 6],    0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 3],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 972 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 11],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 19],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 973 * 16],    m3

    ; mode17 [row 7]
    movu          m6,    [r5 + 16 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 6],    1
    pinsrb        m0,    [r3 + 7],    0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 2],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 974 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 10],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 18],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 975 * 16],    m3

    ; mode17 [row 8]
    movu          m6,    [r5 + 22 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 7],    1
    pinsrb        m0,    [r3 + 9],    0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 1],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 976 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 9],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 17],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 977 * 16],    m3

    ; mode17 [row 9]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 +  9],    1
    pinsrb        m0,    [r3 + 10],    0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r4 + 0],     0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 978 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 8],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 16],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 979 * 16],    m3

    ; mode17 [row 10]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 980 * 16],    m3

    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 981 * 16],    m3

    ; mode17 [row 11]
    movu          m6,    [r5 + 8 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 10],   1
    pinsrb        m0,    [r3 + 11],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 0],    1
    pinsrb        m2,    [r3 + 1],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 982 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 7],     0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 15],    0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 983 * 16],    m3

    ; mode17 [row 12]
    movu          m6,    [r5 + 14 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 11],   1
    pinsrb        m0,    [r3 + 12],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 1],    1
    pinsrb        m2,    [r3 + 2],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 984 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 6],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 14],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 985 * 16],    m3

    ; mode17 [row 13]
    movu          m6,    [r5 + 20 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 12],   1
    pinsrb        m0,    [r3 + 14],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 2],    1
    pinsrb        m2,    [r3 + 4],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 986 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 5],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 13],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 987 * 16],    m3

    ; mode17 [row 14]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 14],   1
    pinsrb        m0,    [r3 + 15],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 4],    1
    pinsrb        m2,    [r3 + 5],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 988 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 4],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 12],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 989 * 16],    m3

    ; mode17 [row 15]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 990 * 16],      m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 990 * 16 + 8],  m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 991 * 16],      m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 991 * 16 + 8],  m5

    ; mode17 [row 16]
    movu          m6,    [r5 + 6 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 15],   1
    pinsrb        m0,    [r3 + 16],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 5],    1
    pinsrb        m2,    [r3 + 6],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 992 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 3],     0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 11],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 993 * 16],    m3

    ; mode17 [row 17]
    movu          m6,    [r5 + 12 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 16],   1
    pinsrb        m0,    [r3 + 17],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 6],    1
    pinsrb        m2,    [r3 + 7],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 994 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,     [r4 + 2],   0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 10],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 995 * 16],    m3

    ; mode17 [row 18]
    movu          m6,    [r5 + 18 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 17],   1
    pinsrb        m0,    [r3 + 18],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 7],    1
    pinsrb        m2,    [r3 + 9],    0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 996 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 1],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 9],    0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 997 * 16],    m3

    ; mode17 [row 19]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 18],   1
    pinsrb        m0,    [r3 + 20],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 +  9],   1
    pinsrb        m2,    [r3 + 10],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 998 * 16],    m3

    pslldq        m1,    2
    pinsrw        m1,    [r4 + 0],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 8],    0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 999 * 16],    m3

    ; mode17 [row 20]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 20],   1
    pinsrb        m0,    [r3 + 21],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 10],   1
    pinsrb        m2,    [r3 + 11],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1000 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r4 + 0],    1
    pinsrb        m1,    [r3 + 1],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    ;pinsrb        m4,    [r4 + 8],   1
    ;pinsrb        m4,    [r4 + 7],   0
    pinsrw        m4,     [r4 + 7],  0
    pmaddubsw     m5,    m4,         m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1001 * 16],  m3

    ; mode17 [row 21]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1002 * 16],   m3

    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,         m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1003 * 16],  m3

    ; mode17 [row 22]
    movu          m6,    [r5 + 10 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 21],   1
    pinsrb        m0,    [r3 + 22],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 11],   1
    pinsrb        m2,    [r3 + 12],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1004 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 1],    1
    pinsrb        m1,    [r3 + 2],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 6],   0
    pmaddubsw     m5,    m4,         m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1005 * 16],  m3

    ; mode17 [row 23]
    movu          m6,    [r5 + 16 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 22],   1
    pinsrb        m0,    [r3 + 23],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 12],   1
    pinsrb        m2,    [r3 + 14],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1006 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 2],    1
    pinsrb        m1,    [r3 + 4],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 5],   0
    pmaddubsw     m5,    m4,         m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1007 * 16],  m3

    ; mode17 [row 24]
    movu          m6,    [r5 + 22 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 23],   1
    pinsrb        m0,    [r3 + 25],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 14],   1
    pinsrb        m2,    [r3 + 15],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1008 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 4],    1
    pinsrb        m1,    [r3 + 5],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 4],   0
    pmaddubsw     m5,    m4,         m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1009 * 16],  m3

    ; mode17 [row 25]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 25],   1
    pinsrb        m0,    [r3 + 26],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 15],   1
    pinsrb        m2,    [r3 + 16],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1010 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 5],    1
    pinsrb        m1,    [r3 + 6],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,     [r4 + 3],   0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1011 * 16],   m3

    ; mode17 [row 26]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1012 * 16],   m3

    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1013 * 16],   m3

    ; mode17 [row 27]
    movu          m6,    [r5 + 8 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 26],   1
    pinsrb        m0,    [r3 + 27],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 16],   1
    pinsrb        m2,    [r3 + 17],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1014 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 6],    1
    pinsrb        m1,    [r3 + 7],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 2],    0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1015 * 16],   m3

    ; mode17 [row 28]
    movu          m6,    [r5 + 14 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 27],   1
    pinsrb        m0,    [r3 + 28],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 17],   1
    pinsrb        m2,    [r3 + 18],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1016 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 7],    1
    pinsrb        m1,    [r3 + 9],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 1],    0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1017 * 16],   m3

    ; mode17 [row 29]
    movu          m6,    [r5 + 20 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 28],   1
    pinsrb        m0,    [r3 + 30],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 18],   1
    pinsrb        m2,    [r3 + 20],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1018 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 +  9],    1
    pinsrb        m1,    [r3 + 10],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrw        m4,    [r4 + 0],    0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1019 * 16],   m3

    ; mode17 [row 30]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r3 + 30],   1
    pinsrb        m0,    [r3 + 31],   0
    pmaddubsw     m3,    m0,          m6
    pmulhrsw      m3,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 20],   1
    pinsrb        m2,    [r3 + 21],   0
    pmaddubsw     m5,    m2,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1020 * 16],   m3

    pslldq        m1,    2
    pinsrb        m1,    [r3 + 10],    1
    pinsrb        m1,    [r3 + 11],    0
    pmaddubsw     m3,    m1,          m6
    pmulhrsw      m3,    m7
    pslldq        m4,    2
    pinsrb        m4,    [r4 + 0],    1
    pinsrb        m4,    [r3 + 1],    0
    pmaddubsw     m5,    m4,          m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1021 * 16],   m3

    ; mode17 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1022 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1022 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1023 * 16],     m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 1023 * 16 + 8], m5

    ;mode 18[row 0]
    movu          m0,                   [r3]
    movu          [r0 + 1024 * 16],     m0
    movu          m1,                   [r3 + 16]
    movu          [r0 + 1025 * 16],     m1

    ;mode 18[row 1]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 1],     0
    movu          [r0 + 1026 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 15],    0
    movu          [r0 + 1027 * 16],     m1

    ;mode 18[row 2]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 2],     0
    movu          [r0 + 1028 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 14],    0
    movu          [r0 + 1029 * 16],     m1

    ;mode 18[row 3]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 3],     0
    movu          [r0 + 1030 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 13],    0
    movu          [r0 + 1031 * 16],     m1

    ;mode 18[row 4]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 4],     0
    movu          [r0 + 1032 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 12],    0
    movu          [r0 + 1033 * 16],     m1

    ;mode 18[row 5]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 5],     0
    movu          [r0 + 1034 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 11],    0
    movu          [r0 + 1035 * 16],     m1

    ;mode 18[row 6]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 6],     0
    movu          [r0 + 1036 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 10],    0
    movu          [r0 + 1037 * 16],     m1

    ;mode 18[row 7]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 7],     0
    movu          [r0 + 1038 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 9],     0
    movu          [r0 + 1039 * 16],     m1

    ;mode 18[row 8]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 8],     0
    movu          [r0 + 1040 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 8],     0
    movu          [r0 + 1041 * 16],     m1

    ;mode 18[row 9]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 +  9],    0
    movu          [r0 + 1042 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 7],     0
    movu          [r0 + 1043 * 16],     m1

    ;mode 18[row 10]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 10],    0
    movu          [r0 + 1044 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 6],     0
    movu          [r0 + 1045 * 16],     m1

    ;mode 18[row 11]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 11],    0
    movu          [r0 + 1046 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 5],     0
    movu          [r0 + 1047 * 16],     m1

    ;mode 18[row 12]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 12],    0
    movu          [r0 + 1048 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 4],     0
    movu          [r0 + 1049 * 16],     m1

    ;mode 18[row 13]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 13],    0
    movu          [r0 + 1050 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 3],     0
    movu          [r0 + 1051 * 16],     m1

    ;mode 18[row 14]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 14],    0
    movu          [r0 + 1052 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 2],     0
    movu          [r0 + 1053 * 16],     m1

    ;mode 18[row 15]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 15],    0
    movu          [r0 + 1054 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 1],     0
    movu          [r0 + 1055 * 16],     m1

    ;mode 18[row 16]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 16],    0
    movu          [r0 + 1056 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r3 + 0],     0
    movu          [r0 + 1057 * 16],     m1

    ;mode 18[row 17]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 17],    0
    movu          [r0 + 1058 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 1],     0
    movu          [r0 + 1059 * 16],     m1

    ;mode 18[row 18]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 18],    0
    movu          [r0 + 1060 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 2],     0
    movu          [r0 + 1061 * 16],     m1

    ;mode 18[row 19]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 19],    0
    movu          [r0 + 1062 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 3],     0
    movu          [r0 + 1063 * 16],     m1

    ;mode 18[row 20]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 20],    0
    movu          [r0 + 1064 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 4],     0
    movu          [r0 + 1065 * 16],     m1

    ;mode 18[row 21]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 21],    0
    movu          [r0 + 1066 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 5],     0
    movu          [r0 + 1067 * 16],     m1

    ;mode 18[row 22]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 22],    0
    movu          [r0 + 1068 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 6],     0
    movu          [r0 + 1069 * 16],     m1

    ;mode 18[row 23]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 23],    0
    movu          [r0 + 1070 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 7],     0
    movu          [r0 + 1071 * 16],     m1

    ;mode 18[row 24]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 24],    0
    movu          [r0 + 1072 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 8],     0
    movu          [r0 + 1073 * 16],     m1

    ;mode 18[row 25]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 25],    0
    movu          [r0 + 1074 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 9],     0
    movu          [r0 + 1075 * 16],     m1

    ;mode 18[row 26]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 26],    0
    movu          [r0 + 1076 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 10],    0
    movu          [r0 + 1077 * 16],     m1

    ;mode 18[row 27]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 27],    0
    movu          [r0 + 1078 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 11],    0
    movu          [r0 + 1079 * 16],     m1

    ;mode 18[row 28]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 28],    0
    movu          [r0 + 1080 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 12],    0
    movu          [r0 + 1081 * 16],     m1

    ;mode 18[row 29]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 29],    0
    movu          [r0 + 1082 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 13],    0
    movu          [r0 + 1083 * 16],     m1

    ;mode 18[row 30]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 30],    0
    movu          [r0 + 1084 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 14],    0
    movu          [r0 + 1085 * 16],     m1

    ;mode 18[row 31]
    pslldq        m0,                   1
    pinsrb        m0,                   [r4 + 31],    0
    movu          [r0 + 1086 * 16],     m0
    pslldq        m1,                   1
    pinsrb        m1,                   [r4 + 15],    0
    movu          [r0 + 1087 * 16],     m1

    ; mode 19 [row 0]
    movu          m6,    [r5 + 6 * 16]
    movu          m0,    [r3         ]
    movu          m1,    [r3 + 1     ]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,            m6
    pmulhrsw      m1,    m7
    movu          m2,    [r3 + 8]
    movu          m3,    [r3 + 9]
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,            m6
    pmulhrsw      m3,    m7
    packuswb      m1,    m3
    movu          [r0 + 1088 * 16],     m1

    movu          m1,    [r3 + 16]
    movu          m3,    [r3 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    movu          m3,    [r3 + 24]
    movu          m5,    [r3 + 25]
    punpcklbw     m3,    m5
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1089 * 16],     m4

    ; mode 19 [row 1]
    movu          m6,    [r5 + 12 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 0],      1
    pinsrb        m0,    [r4 + 1],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 7],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1090 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 15],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 23],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1091 * 16],     m4

    ; mode 19 [row 2]
    movu          m6,    [r5 + 18 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 1],      1
    pinsrb        m0,    [r4 + 2],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1092 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 14],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 22],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1093 * 16],     m4

    ; mode 19 [row 3]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 2],      1
    pinsrb        m0,    [r4 + 4],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1094 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 13],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 21],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1095 * 16],     m4

    ; mode 19 [row 4]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 4],      1
    pinsrb        m0,    [r4 + 5],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1096 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 12],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 20],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1097 * 16],     m4

    ; mode 19 [row 5]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1098 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1099 * 16],     m4

    ; mode 19 [row 6]
    movu          m6,    [r5 + 10 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 5],      1
    pinsrb        m0,    [r4 + 6],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 3],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1100 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 11],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 19],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1101 * 16],    m4

    ; mode 19 [row 7]
    movu          m6,    [r5 + 16 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 6],      1
    pinsrb        m0,    [r4 + 7],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 2],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1102 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 10],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 18],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1103 * 16],    m4

    ; mode 19 [row 8]
    movu          m6,    [r5 + 22 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 7],      1
    pinsrb        m0,    [r4 + 9],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 1],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1104 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 9],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 17],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1105 * 16],    m4

    ; mode 19 [row 9]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 +  9],     1
    pinsrb        m0,    [r4 + 10],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 0],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1106 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 8],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 16],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1107 * 16],    m4

    ; mode 19 [row 10]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1108 * 16],    m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1109 * 16],    m4

    ; mode 19 [row 11]
    movu          m6,    [r5 + 8 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 10],     1
    pinsrb        m0,    [r4 + 11],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 0],      1
    pinsrb        m2,    [r4 + 1],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1110 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 7],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 15],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1111 * 16],    m4

    ; mode 19 [row 12]
    movu          m6,    [r5 + 14 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 11],     1
    pinsrb        m0,    [r4 + 12],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 1],      1
    pinsrb        m2,    [r4 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1112 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 6],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 14],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1113 * 16],    m4

    ; mode 19 [row 13]
    movu          m6,    [r5 + 20 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 12],     1
    pinsrb        m0,    [r4 + 14],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 2],      1
    pinsrb        m2,    [r4 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1114 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 5],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 13],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1115 * 16],    m4

    ; mode 19 [row 14]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 14],     1
    pinsrb        m0,    [r4 + 15],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 4],      1
    pinsrb        m2,    [r4 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1116 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 4],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 12],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1117 * 16],    m4

    ; mode19 [row 15]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1118 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1118 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1119 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1119 * 16 + 8], m5

    ; mode 19 [row 16]
    movu          m6,    [r5 + 6 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 15],     1
    pinsrb        m0,    [r4 + 16],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 5],      1
    pinsrb        m2,    [r4 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1120 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 3],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 11],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1121 * 16],    m4

    ; mode 19 [row 17]
    movu          m6,    [r5 + 12 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 16],     1
    pinsrb        m0,    [r4 + 17],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 6],      1
    pinsrb        m2,    [r4 + 7],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1122 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 2],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 10],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1123 * 16],    m4

    ; mode 19 [row 18]
    movu          m6,    [r5 + 18 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 17],     1
    pinsrb        m0,    [r4 + 18],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 7],      1
    pinsrb        m2,    [r4 + 9],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1124 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 1],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 +  9],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1125 * 16],    m4

    ; mode 19 [row 19]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 18],     1
    pinsrb        m0,    [r4 + 20],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 +  9],     1
    pinsrb        m2,    [r4 + 10],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1126 * 16],    m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 0],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 8],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1127 * 16],    m4

    ; mode 19 [row 20]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 20],     1
    pinsrb        m0,    [r4 + 21],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 10],     1
    pinsrb        m2,    [r4 + 11],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1128 * 16],    m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 0],      1
    pinsrb        m1,    [r4 + 1],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrb        m3,    [r3 + 8],     1
    pinsrb        m3,    [r3 + 7],     0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1129 * 16],   m4

    ; mode 19 [row 21]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1130 * 16],   m4
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1131 * 16],   m4

    ; mode 19 [row 22]
    movu          m6,    [r5 + 10 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 21],    1
    pinsrb        m0,    [r4 + 22],    0
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 11],    1
    pinsrb        m2,    [r4 + 12],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1132 * 16],   m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 1],     1
    pinsrb        m1,    [r4 + 2],     0
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 6],      0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1133 * 16],   m4

    ; mode 19 [row 23]
    movu          m6,    [r5 + 16 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 22],    1
    pinsrb        m0,    [r4 + 23],    0
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 12],    1
    pinsrb        m2,    [r4 + 14],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1134 * 16],   m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 2],     1
    pinsrb        m1,    [r4 + 4],     0
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 5],      0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1135 * 16],   m4

    ; mode 19 [row 24]
    movu          m6,    [r5 + 22 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 23],    1
    pinsrb        m0,    [r4 + 25],    0
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 14],    1
    pinsrb        m2,    [r4 + 15],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1136 * 16],   m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 4],     1
    pinsrb        m1,    [r4 + 5],     0
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 4],      0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1137 * 16],   m4

    ; mode 19 [row 25]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 25],    1
    pinsrb        m0,    [r4 + 26],    0
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 15],    1
    pinsrb        m2,    [r4 + 16],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1138 * 16],   m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 5],     1
    pinsrb        m1,    [r4 + 6],     0
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 3],      0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1139 * 16],   m4

    ; mode 19 [row 26]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1140 * 16],   m4
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1141 * 16],   m4

    ; mode 19 [row 27]
    movu          m6,    [r5 + 8 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 26],    1
    pinsrb        m0,    [r4 + 27],    0
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 16],    1
    pinsrb        m2,    [r4 + 17],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1142 * 16],   m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 6],     1
    pinsrb        m1,    [r4 + 7],     0
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 2],      0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1143 * 16],   m4

    ; mode 19 [row 28]
    movu          m6,    [r5 + 14 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 27],    1
    pinsrb        m0,    [r4 + 28],    0
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 17],    1
    pinsrb        m2,    [r4 + 18],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1144 * 16],   m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 7],     1
    pinsrb        m1,    [r4 + 9],     0
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 1],      0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1145 * 16],   m4

    ; mode 19 [row 29]
    movu          m6,    [r5 + 20 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 28],    1
    pinsrb        m0,    [r4 + 30],    0
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 18],    1
    pinsrb        m2,    [r4 + 20],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1146 * 16],   m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 +  9],    1
    pinsrb        m1,    [r4 + 10],    0
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 0],      0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1147 * 16],   m4

    ; mode 19 [row 30]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 30],    1
    pinsrb        m0,    [r4 + 31],    0
    pmaddubsw     m4,    m0,           m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 20],    1
    pinsrb        m2,    [r4 + 21],    0
    pmaddubsw     m5,    m2,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1148 * 16],   m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 10],    1
    pinsrb        m1,    [r4 + 11],    0
    pmaddubsw     m4,    m1,           m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrb        m3,    [r4 + 0],     1
    pinsrb        m3,    [r4 + 1],     0
    pmaddubsw     m5,    m3,           m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 +  1149 * 16],   m4

    ; mode19 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1150 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1150 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1151 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1151 * 16 + 8], m5

    ; mode 20 [row 0]
    movu          m6,    [r5 + 11 * 16]
    movu          m0,    [r3         ]
    movu          m1,    [r3 + 1     ]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,            m6
    pmulhrsw      m1,    m7
    movu          m2,    [r3 + 8]
    movu          m3,    [r3 + 9]
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,            m6
    pmulhrsw      m3,    m7
    packuswb      m1,    m3
    movu          [r0 + 1152 * 16],     m1

    movu          m1,    [r3 + 16]
    movu          m3,    [r3 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    movu          m3,    [r3 + 24]
    movu          m5,    [r3 + 25]
    punpcklbw     m3,    m5
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1153 * 16],     m4

    ; mode 20 [row 1]
    movu          m6,    [r5 + 22 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 0],      1
    pinsrb        m0,    [r4 + 2],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 7],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1154 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 15],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 23],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1155 * 16],     m4

    ; mode 20 [row 2]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1156 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1157 * 16],     m4

    ; mode 20 [row 3]
    movu          m6,    [r5 + 12 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 2],      1
    pinsrb        m0,    [r4 + 3],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1158 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 14],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 22],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1159 * 16],     m4

    ; mode 20 [row 4]
    movu          m6,    [r5 + 23 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 3],      1
    pinsrb        m0,    [r4 + 5],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1160 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 13],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 21],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1161 * 16],     m4

    ; mode 20 [row 5]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1162 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1163 * 16],     m4

    ; mode 20 [row 6]
    movu          m6,    [r5 + 13 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 5],      1
    pinsrb        m0,    [r4 + 6],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1164 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 12],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 20],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1165 * 16],     m4

    ; mode 20 [row 7]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 6],      1
    pinsrb        m0,    [r4 + 8],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 3],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1166 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 11],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 19],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1167 * 16],     m4

    ; mode 20 [row 8]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1168 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1169 * 16],     m4

    ; mode 20 [row 9]
    movu          m6,    [r5 + 14 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 8],      1
    pinsrb        m0,    [r4 + 9],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 3],      1
    pinsrb        m2,    [r3 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1170 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 10],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 18],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1171 * 16],     m4

    ; mode 20 [row 10]
    movu          m6,    [r5 + 25 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 +  9],      1
    pinsrb        m0,    [r4 + 11],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 1],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1172 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 9],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 17],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1173 * 16],     m4

    ; mode 20 [row 11]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1174 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1175 * 16],     m4

    ; mode 20 [row 12]
    movu          m6,    [r5 + 15 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 11],     1
    pinsrb        m0,    [r4 + 12],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r3 + 1],      1
    pinsrb        m2,    [r3 + 0],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1176 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 8],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 16],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1177 * 16],     m4

    ; mode 20 [row 13]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 12],     1
    pinsrb        m0,    [r4 + 14],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 0],      1
    pinsrb        m2,    [r4 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1178 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 7],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 15],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1179 * 16],     m4

    ; mode 20 [row 14]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1180 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1181 * 16],     m4

    ; mode 20 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 14],     1
    pinsrb        m0,    [r4 + 15],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 2],      1
    pinsrb        m2,    [r4 + 3],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1182 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 6],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 14],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1183 * 16],     m4

    ; mode 20 [row 16]
    movu          m6,    [r5 + 27 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 15],     1
    pinsrb        m0,    [r4 + 17],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 3],      1
    pinsrb        m2,    [r4 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1184 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 5],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 13],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1185 * 16],     m4

    ; mode 20 [row 17]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1186 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1187 * 16],     m4

    ; mode 20 [row 18]
    movu          m6,    [r5 + 17 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 17],     1
    pinsrb        m0,    [r4 + 18],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 5],      1
    pinsrb        m2,    [r4 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1188 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 4],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 12],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1189 * 16],     m4

    ; mode 20 [row 19]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 18],     1
    pinsrb        m0,    [r4 + 20],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 6],      1
    pinsrb        m2,    [r4 + 8],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1190 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 3],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 11],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1191 * 16],     m4

    ; mode 20 [row 20]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1192 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1193 * 16],     m4

    ; mode 20 [row 21]
    movu          m6,    [r5 + 18 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 20],     1
    pinsrb        m0,    [r4 + 21],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 8],      1
    pinsrb        m2,    [r4 + 9],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1194 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 2],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 10],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1195 * 16],     m4

    ; mode 20 [row 22]
    movu          m6,    [r5 + 29 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 21],     1
    pinsrb        m0,    [r4 + 23],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 +  9],      1
    pinsrb        m2,    [r4 + 11],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1196 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 1],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 9],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1197 * 16],     m4

    ; mode 20 [row 23]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1198 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1199 * 16],     m4

    ; mode 20 [row 24]
    movu          m6,    [r5 + 19 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 23],     1
    pinsrb        m0,    [r4 + 24],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 11],      1
    pinsrb        m2,    [r4 + 12],      0
    pmaddubsw     m5,    m2,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1200 * 16],      m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 0],      0
    pmaddubsw     m4,    m1,             m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 8],       0
    pmaddubsw     m5,    m3,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1201 * 16],      m4

    ; mode 20 [row 25]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 24],      1
    pinsrb        m0,    [r4 + 26],      0
    pmaddubsw     m4,    m0,             m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 12],      1
    pinsrb        m2,    [r4 + 14],      0
    pmaddubsw     m5,    m2,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1202 * 16],      m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 0],       1
    pinsrb        m1,    [r4 + 2],       0
    pmaddubsw     m4,    m1,             m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 7],      0
    pmaddubsw     m5,    m3,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1203 * 16],      m4

    ; mode 20 [row 26]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m4,    m0,             m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1204 * 16],      m4
    pmaddubsw     m4,    m1,             m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1205 * 16],      m4

    ; mode 20 [row 27]
    movu          m6,    [r5 + 20 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 26],      1
    pinsrb        m0,    [r4 + 27],      0
    pmaddubsw     m4,    m0,             m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 14],      1
    pinsrb        m2,    [r4 + 15],      0
    pmaddubsw     m5,    m2,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1206 * 16],      m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 2],       1
    pinsrb        m1,    [r4 + 3],       0
    pmaddubsw     m4,    m1,             m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 6],      0
    pmaddubsw     m5,    m3,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1207 * 16],      m4

    ; mode 20 [row 28]
    movu          m6,    [r5 + 31 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 27],      1
    pinsrb        m0,    [r4 + 29],      0
    pmaddubsw     m4,    m0,             m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 15],      1
    pinsrb        m2,    [r4 + 17],      0
    pmaddubsw     m5,    m2,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1208 * 16],      m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 3],       1
    pinsrb        m1,    [r4 + 5],       0
    pmaddubsw     m4,    m1,             m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 5],      0
    pmaddubsw     m5,    m3,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1209 * 16],      m4

    ; mode 20 [row 29]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m4,    m0,             m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1210 * 16],      m4
    pmaddubsw     m4,    m1,             m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1211 * 16],      m4

    ; mode 20 [row 30]
    movu          m6,    [r5 + 21 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 29],      1
    pinsrb        m0,    [r4 + 30],      0
    pmaddubsw     m4,    m0,             m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 17],      1
    pinsrb        m2,    [r4 + 18],      0
    pmaddubsw     m5,    m2,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1212 * 16],      m4
    pslldq        m1,    2
    pinsrb        m1,    [r4 + 5],       1
    pinsrb        m1,    [r4 + 6],       0
    pmaddubsw     m4,    m1,             m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 4],      0
    pmaddubsw     m5,    m3,             m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1213 * 16],      m4

    ; mode20 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1214 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1214 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1215 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1215 * 16 + 8], m5

    ; mode 21 [row 0]
    movu          m6,    [r5 + 15 * 16]
    movu          m0,    [r3         ]
    movu          m1,    [r3 + 1     ]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,            m6
    pmulhrsw      m1,    m7
    movu          m2,    [r3 + 8]
    movu          m3,    [r3 + 9]
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,            m6
    pmulhrsw      m3,    m7
    packuswb      m1,    m3
    movu          [r0 + 1216 * 16],     m1

    movu          m1,    [r3 + 16]
    movu          m3,    [r3 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    movu          m3,    [r3 + 24]
    movu          m5,    [r3 + 25]
    punpcklbw     m3,    m5
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1217 * 16],     m4

    ; mode 21 [row 1]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 0],      1
    pinsrb        m0,    [r4 + 2],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 7],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1218 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 15],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 23],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1219 * 16],     m4

    ; mode 21 [row 2]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1220 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1221 * 16],     m4

    ; mode 21 [row 3]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 2],      1
    pinsrb        m0,    [r4 + 4],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1222 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 14],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 22],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1223 * 16],     m4

    ; mode 21 [row 4]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1224 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1225 * 16],     m4

    ; mode 21 [row 5]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 4],      1
    pinsrb        m0,    [r4 + 6],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1226 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 13],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 21],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1227 * 16],     m4

    ; mode 21 [row 6]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1228 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1229 * 16],     m4

    ; mode 21 [row 7]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 6],      1
    pinsrb        m0,    [r4 + 8],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1230 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 12],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 20],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1231 * 16],     m4

    ; mode 21 [row 8]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1232 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1233 * 16],     m4

    ; mode 21 [row 9]
    movu          m6,    [r5 + 22 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 8],      1
    pinsrb        m0,    [r4 + 9],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 3],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1234 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 11],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 19],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1235 * 16],     m4

    ; mode 21 [row 10]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1236 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1237 * 16],     m4

    ; mode 21 [row 11]
    movu          m6,    [r5 + 20 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 +  9],     1
    pinsrb        m0,    [r4 + 11],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1238 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 10],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 18],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1239 * 16],     m4

    ; mode 21 [row 12]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1240 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1241 * 16],     m4

    ; mode 21 [row 13]
    movu          m6,    [r5 + 18 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 11],     1
    pinsrb        m0,    [r4 + 13],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 1],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1242 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 9],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 17],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1243 * 16],     m4

    ; mode 21 [row 14]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1244 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1245 * 16],     m4

    ; mode 21 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 13],     1
    pinsrb        m0,    [r4 + 15],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 0],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1246 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 8],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 16],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1247 * 16],     m4

    ; mode 21 [row 16]
    movu          m6,    [r5 + 31 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 15],     1
    pinsrb        m0,    [r4 + 17],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 0],      1
    pinsrb        m2,    [r4 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1248 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 7],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 15],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1249 * 16],     m4

    ; mode 21 [row 17]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1250 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1251 * 16],     m4

    ; mode 21 [row 18]
    movu          m6,    [r5 + 29 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 17],     1
    pinsrb        m0,    [r4 + 19],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 2],      1
    pinsrb        m2,    [r4 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1252 * 16],     m4
    pslldq        m1,    2
    pinsrb        m1,    [r3 + 7],      1
    pinsrb        m1,    [r3 + 6],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrb        m3,    [r3 + 15],     1
    pinsrb        m3,    [r3 + 14],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1253 * 16],     m4

    ; mode 21 [row 19]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1254 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1255 * 16],     m4

    ; mode 21 [row 20]
    movu          m6,    [r5 + 27 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 19],     1
    pinsrb        m0,    [r4 + 21],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 4],      1
    pinsrb        m2,    [r4 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1256 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 5],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 13],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1257 * 16],     m4

    ; mode 21 [row 21]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1258 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1259 * 16],     m4

    ; mode 21 [row 22]
    movu          m6,    [r5 + 25 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 21],     1
    pinsrb        m0,    [r4 + 23],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 6],      1
    pinsrb        m2,    [r4 + 8],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1260 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 4],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 12],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1261 * 16],     m4

    ; mode 21 [row 23]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1262 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1263 * 16],     m4

    ; mode 21 [row 24]
    movu          m6,    [r5 + 23 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 23],     1
    pinsrb        m0,    [r4 + 24],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 8],      1
    pinsrb        m2,    [r4 + 9],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1264 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 3],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 11],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1265 * 16],     m4

    ; mode 21 [row 25]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1266 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1267 * 16],     m4

    ; mode 21 [row 26]
    movu          m6,    [r5 + 21 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 24],     1
    pinsrb        m0,    [r4 + 26],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 +  9],      1
    pinsrb        m2,    [r4 + 11],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1268 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 2],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 10],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1269 * 16],     m4

    ; mode 21 [row 27]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1270 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1271 * 16],     m4

    ; mode 21 [row 28]
    movu          m6,    [r5 + 19 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 26],     1
    pinsrb        m0,    [r4 + 28],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 11],      1
    pinsrb        m2,    [r4 + 13],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1272 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 1],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 9],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1273 * 16],     m4

    ; mode 21 [row 29]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1274 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1275 * 16],     m4

    ; mode 21 [row 30]
    movu          m6,    [r5 + 17 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 28],     1
    pinsrb        m0,    [r4 + 30],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 13],     1
    pinsrb        m2,    [r4 + 15],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1276 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 0],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 8],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1277 * 16],     m4

    ; mode21 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1278 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1278 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1279 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1279 * 16 + 8], m5

    ; mode 22 [row 0]
    movu          m6,    [r5 + 19 * 16]
    movu          m0,    [r3          ]
    movu          m1,    [r3 + 1      ]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,            m6
    pmulhrsw      m1,    m7
    movu          m2,    [r3 + 8]
    movu          m3,    [r3 + 9]
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,            m6
    pmulhrsw      m3,    m7
    packuswb      m1,    m3
    movu          [r0 + 1280 * 16],     m1

    movu          m1,    [r3 + 16]
    movu          m3,    [r3 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    movu          m3,    [r3 + 24]
    movu          m5,    [r3 + 25]
    punpcklbw     m3,    m5
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1281 * 16],     m4

    ; mode 22 [row 1]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1282 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1283 * 16],     m4

    ; mode 22 [row 2]
    movu          m6,    [r5 + 25 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 0],      1
    pinsrb        m0,    [r4 + 2],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 7],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1284 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 15],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 23],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1285 * 16],     m4

    ; mode 22 [row 3]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1286 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1287 * 16],     m4

    ; mode 22 [row 4]
    movu          m6,    [r5 + 31 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 2],      1
    pinsrb        m0,    [r4 + 5],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1288 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 14],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 22],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1289 * 16],     m4

    ; mode 22 [row 5]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1290 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1291 * 16],     m4

    ; mode 22 [row 6]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1292 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1293 * 16],     m4

    ; mode 22 [row 7]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 5],      1
    pinsrb        m0,    [r4 + 7],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1294 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 13],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 21],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1295 * 16],     m4

    ; mode 22 [row 8]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1296 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1297 * 16],     m4

    ; mode 22 [row 9]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 +  7],      1
    pinsrb        m0,    [r4 + 10],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1298 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 12],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 20],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1299 * 16],     m4

    ; mode 22 [row 10]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1300 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1301 * 16],     m4

    ; mode 22 [row 11]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1302 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1303 * 16],     m4

    ; mode 22 [row 12]
    movu          m6,    [r5 + 23 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 10],     1
    pinsrb        m0,    [r4 + 12],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 3],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1304 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 11],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 19],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1305 * 16],     m4

    ; mode 22 [row 13]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1306 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1307 * 16],     m4

    ; mode 22 [row 14]
    movu          m6,    [r5 + 29 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 12],     1
    pinsrb        m0,    [r4 + 15],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1308 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 10],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 18],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1309 * 16],     m4

    ; mode 22 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1310 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1311 * 16],     m4

    ; mode 22 [row 16]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1312 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1313 * 16],     m4

    ; mode 22 [row 17]
    movu          m6,    [r5 + 22 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 15],     1
    pinsrb        m0,    [r4 + 17],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 1],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1314 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 9],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 17],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1315 * 16],     m4

    ; mode 22 [row 18]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1316 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1317 * 16],     m4

    ; mode 22 [row 19]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 17],     1
    pinsrb        m0,    [r4 + 20],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 0],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1318 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 8],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 16],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1319 * 16],     m4

    ; mode 22 [row 20]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1320 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1321 * 16],     m4

    ; mode 22 [row 21]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1322 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1323 * 16],     m4

    ; mode 22 [row 22]
    movu          m6,    [r5 + 21 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 20],     1
    pinsrb        m0,    [r4 + 22],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 0],      1
    pinsrb        m2,    [r4 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1324 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 7],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 15],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1325 * 16],     m4

    ; mode 22 [row 23]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1326 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1327 * 16],     m4

    ; mode 22 [row 24]
    movu          m6,    [r5 + 27 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 22],     1
    pinsrb        m0,    [r4 + 25],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 2],      1
    pinsrb        m2,    [r4 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1328 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 6],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 14],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1329 * 16],     m4

    ; mode 22 [row 25]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1330 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1331 * 16],     m4

    ; mode 22 [row 26]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1332 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1333 * 16],     m4

    ; mode 22 [row 27]
    movu          m6,    [r5 + 20 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 25],     1
    pinsrb        m0,    [r4 + 27],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 + 5],      1
    pinsrb        m2,    [r4 + 7],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1334 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 +  5],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 13],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1335 * 16],     m4

    ; mode 22 [row 28]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1336 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1337 * 16],     m4

    ; mode 22 [row 29]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 27],     1
    pinsrb        m0,    [r4 + 30],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrb        m2,    [r4 +  7],     1
    pinsrb        m2,    [r4 + 10],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1338 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 +  4],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 12],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1339 * 16],     m4

    ; mode 22 [row 30]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1340 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1341 * 16],     m4

    ; mode22 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1342 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1342 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1343 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1343 * 16 + 8], m5

    ; mode 23 [row 0]
    movu          m6,    [r5 + 23 * 16]
    movu          m0,    [r3          ]
    movu          m1,    [r3 + 1      ]
    punpcklbw     m0,    m1
    pmaddubsw     m1,    m0,            m6
    pmulhrsw      m1,    m7
    movu          m2,    [r3 + 8]
    movu          m3,    [r3 + 9]
    punpcklbw     m2,    m3
    pmaddubsw     m3,    m2,            m6
    pmulhrsw      m3,    m7
    packuswb      m1,    m3
    movu          [r0 + 1344 * 16],     m1

    movu          m1,    [r3 + 16]
    movu          m3,    [r3 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    movu          m3,    [r3 + 24]
    movu          m5,    [r3 + 25]
    punpcklbw     m3,    m5
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1345 * 16],     m4

    ; mode 23 [row 1]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1346 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1347 * 16],     m4

    ; mode 23 [row 2]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1348 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1349 * 16],     m4

    ; mode 23 [row 3]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 0],      1
    pinsrb        m0,    [r4 + 4],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 7],     0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1350 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 15],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 23],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1351 * 16],     m4

    ; mode 23 [row 4]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1352 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1353 * 16],     m4

    ; mode 23 [row 5]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1354 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1355 * 16],     m4

    ; mode 23 [row 6]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1356 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1357 * 16],     m4

    ; mode 23 [row 7]
    movu          m6,    [r5 + 24 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 4],      1
    pinsrb        m0,    [r4 + 7],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1358 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 14],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 22],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1359 * 16],     m4

    ; mode 23 [row 8]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1360 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1361 * 16],     m4

    ; mode 23 [row 9]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1362 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1363 * 16],     m4

    ; mode 23 [row 10]
    movu          m6,    [r5 + 29 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 +  7],      1
    pinsrb        m0,    [r4 + 11],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1364 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 13],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 21],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1365 * 16],     m4

    ; mode 23 [row 11]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1366 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1367 * 16],     m4

    ; mode 23 [row 12]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1368 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1369 * 16],     m4

    ; mode 23 [row 13]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1370 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1371 * 16],     m4

    ; mode 23 [row 14]
    movu          m6,    [r5 + 25 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 11],      1
    pinsrb        m0,    [r4 + 14],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1372 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 12],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 20],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1373 * 16],     m4

    ; mode 23 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1374 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1375 * 16],     m4

    ; mode 23 [row 16]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1376 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1377 * 16],     m4

    ; mode 23 [row 17]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 14],      1
    pinsrb        m0,    [r4 + 18],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 3],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1378 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 11],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 19],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1379 * 16],     m4

    ; mode 23 [row 18]
    movu          m6,    [r5 + 21 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1380 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1381 * 16],     m4

    ; mode 23 [row 19]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1382 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1383 * 16],     m4

    ; mode 23 [row 20]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1384 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1385 * 16],     m4

    ; mode 23 [row 21]
    movu          m6,    [r5 + 26 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 18],      1
    pinsrb        m0,    [r4 + 21],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 2],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1386 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 10],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 18],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1387 * 16],     m4

    ; mode 23 [row 22]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1388 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1389 * 16],     m4

    ; mode 23 [row 23]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1390 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1391 * 16],     m4

    ; mode 23 [row 24]
    movu          m6,    [r5 + 31 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 21],      1
    pinsrb        m0,    [r4 + 25],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 1],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1392 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 9],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 17],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1393 * 16],     m4

    ; mode 23 [row 25]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1394 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1395 * 16],     m4

    ; mode 23 [row 26]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1396 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1397 * 16],     m4

    ; mode 23 [row 27]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1398 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1399 * 16],     m4

    ; mode 23 [row 28]
    movu          m6,    [r5 + 27 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 25],      1
    pinsrb        m0,    [r4 + 28],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 0],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1400 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 8],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 16],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1401 * 16],     m4

    ; mode 23 [row 29]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1402 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1403 * 16],     m4

    ; mode 23 [row 30]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1404 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1405 * 16],     m4

    ; mode23 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1406 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1406 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1407 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1407 * 16 + 8], m5

    ; mode 24 [row 0]
    movu          m6,    [r5 + 27 * 16]
    movu          m0,    [r3          ]
    movu          m1,    [r3 + 1      ]
    punpcklbw     m0,    m1
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    movu          m2,    [r3 + 8]
    movu          m3,    [r3 + 9]
    punpcklbw     m2,    m3
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1408 * 16],     m4

    movu          m1,    [r3 + 16]
    movu          m3,    [r3 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    movu          m3,    [r3 + 24]
    movu          m5,    [r3 + 25]
    punpcklbw     m3,    m5
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1409 * 16],     m4

    ; mode 24 [row 1]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1410 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1411 * 16],     m4

    ; mode 24 [row 2]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1412 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1413 * 16],     m4

    ; mode 24 [row 3]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1414 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1415 * 16],     m4

    ; mode 24 [row 4]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1416 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1417 * 16],     m4

    ; mode 24 [row 5]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1418 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1419 * 16],     m4

    ; mode 24 [row 6]
    movu          m6,    [r5 + 29 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 0],      1
    pinsrb        m0,    [r4 + 6],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 7],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1420 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 15],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 23],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1421 * 16],     m4

    ; mode 24 [row 7]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1422 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1423 * 16],     m4

    ; mode 24 [row 8]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1424 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1425 * 16],     m4

    ; mode 24 [row 9]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1426 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1427 * 16],     m4

    ; mode 24 [row 10]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1428 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1429 * 16],     m4

    ; mode 24 [row 11]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1430 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1431 * 16],     m4

    ; mode 24 [row 12]
    movu          m6,    [r5 + 31 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 +  6],      1
    pinsrb        m0,    [r4 + 13],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 6],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1432 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 14],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 22],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1433 * 16],     m4

    ; mode 24 [row 13]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1434 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1435 * 16],     m4

    ; mode 24 [row 14]
    movu          m6,    [r5 + 21 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1436 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1437 * 16],     m4

    ; mode 24 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1438 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1439 * 16],     m4

    ; mode 24 [row 16]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1440 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1441 * 16],     m4

    ; mode 24 [row 17]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1442 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1443 * 16],     m4

    ; mode 24 [row 18]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1444 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1445 * 16],     m4

    ; mode 24 [row 19]
    movu          m6,    [r5 + 28 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 13],      1
    pinsrb        m0,    [r4 + 19],      0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 5],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1446 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 13],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 21],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1447 * 16],     m4

    ; mode 24 [row 20]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1448 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1449 * 16],     m4

    ; mode 24 [row 21]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1450 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1451 * 16],     m4

    ; mode 24 [row 22]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1452 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1453 * 16],     m4

    ; mode 24 [row 23]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1454 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1455 * 16],     m4

    ; mode 24 [row 24]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1456 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1457 * 16],     m4

    ; mode 24 [row 25]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 19],     1
    pinsrb        m0,    [r4 + 26],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 4],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1458 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 12],      0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 20],      0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1459 * 16],     m4

    ; mode 24 [row 26]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1460 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1461 * 16],     m4

    ; mode 24 [row 27]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1462 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1463 * 16],     m4

    ; mode 24 [row 28]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1464 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1465 * 16],     m4

    ; mode 24 [row 29]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1466 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1467 * 16],     m4

    ; mode 24 [row 30]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1468 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1469 * 16],     m4

    ; mode 24 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1470 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1470 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1471 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1471 * 16 + 8], m5

    ; mode 25 [row 0]
    movu          m6,    [r5 + 30 * 16]
    movu          m0,    [r3          ]
    movu          m1,    [r3 + 1      ]
    punpcklbw     m0,    m1
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    movu          m2,    [r3 + 8]
    movu          m3,    [r3 + 9]
    punpcklbw     m2,    m3
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1472 * 16],     m4

    movu          m1,    [r3 + 16]
    movu          m3,    [r3 + 17]
    punpcklbw     m1,    m3
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    movu          m3,    [r3 + 24]
    movu          m5,    [r3 + 25]
    punpcklbw     m3,    m5
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1473 * 16],     m4

    ; mode 25 [row 1]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1474 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1475 * 16],     m4

    ; mode 25 [row 2]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1476 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1477 * 16],     m4

    ; mode 25 [row 3]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1478 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1479 * 16],     m4

    ; mode 25 [row 4]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1480 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1481 * 16],     m4

    ; mode 25 [row 5]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1482 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1483 * 16],     m4

    ; mode 25 [row 6]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1484 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1485 * 16],     m4

    ; mode 25 [row 7]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1486 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1487 * 16],     m4

    ; mode 25 [row 8]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1488 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1489 * 16],     m4

    ; mode 25 [row 9]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1490 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1491 * 16],     m4

    ; mode 25 [row 10]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1492 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1493 * 16],     m4

    ; mode 25 [row 11]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1494 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1495 * 16],     m4

    ; mode 25 [row 12]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1496 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1497 * 16],     m4

    ; mode 25 [row 13]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1498 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1499 * 16],     m4

    ; mode 25 [row 14]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1500 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1501 * 16],     m4

    ; mode 25 [row 15]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1502 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1502 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1503 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1503 * 16 + 8], m5

    ; mode 25 [row 16]
    movu          m6,    [r5 + 30 * 16]
    pslldq        m0,    2
    pinsrb        m0,    [r4 + 0],      1
    pinsrb        m0,    [r4 + 16],     0
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pslldq        m2,    2
    pinsrw        m2,    [r3 + 7],      0
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1504 * 16],     m4
    pslldq        m1,    2
    pinsrw        m1,    [r3 + 15],     0
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pslldq        m3,    2
    pinsrw        m3,    [r3 + 23],     0
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1505 * 16],     m4

    ; mode 25 [row 17]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1506 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1507 * 16],     m4

    ; mode 25 [row 18]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1508 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1509 * 16],     m4

    ; mode 25 [row 19]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1510 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1511 * 16],     m4

    ; mode 25 [row 20]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1512 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1513 * 16],     m4

    ; mode 25 [row 21]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1514 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1515 * 16],     m4

    ; mode 25 [row 22]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1516 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1517 * 16],     m4

    ; mode 25 [row 23]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1518 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1519 * 16],     m4

    ; mode 25 [row 24]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1520 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1521 * 16],     m4

    ; mode 25 [row 25]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1522 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1523 * 16],     m4

    ; mode 25 [row 26]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1524 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1525 * 16],     m4

    ; mode 25 [row 27]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1526 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1527 * 16],     m4

    ; mode 25 [row 28]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1528 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1529 * 16],     m4

    ; mode 25 [row 29]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1530 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1531 * 16],     m4

    ; mode 25 [row 30]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1532 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1533 * 16],     m4

    ; mode 25 [row 31]
    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1534 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1534 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1535 * 16],     m5
    pshufb        m5,    m3,            [tab_S2]
    movh          [r0 + 1535 * 16 + 8], m5

    ; mode 26
    movu                      m1,   [r1 +  1]
    movu                      m2,   [r1 + 17]
    movu         [r0 + 1536 * 16],  m1
    movu         [r0 + 1537 * 16],  m2
    movu         [r0 + 1538 * 16],  m1
    movu         [r0 + 1539 * 16],  m2
    movu         [r0 + 1540 * 16],  m1
    movu         [r0 + 1541 * 16],  m2
    movu         [r0 + 1542 * 16],  m1
    movu         [r0 + 1543 * 16],  m2
    movu         [r0 + 1544 * 16],  m1
    movu         [r0 + 1545 * 16],  m2
    movu         [r0 + 1546 * 16],  m1
    movu         [r0 + 1547 * 16],  m2
    movu         [r0 + 1548 * 16],  m1
    movu         [r0 + 1549 * 16],  m2
    movu         [r0 + 1550 * 16],  m1
    movu         [r0 + 1551 * 16],  m2

    movu         [r0 + 1552 * 16],  m1
    movu         [r0 + 1553 * 16],  m2
    movu         [r0 + 1554 * 16],  m1
    movu         [r0 + 1555 * 16],  m2
    movu         [r0 + 1556 * 16],  m1
    movu         [r0 + 1557 * 16],  m2
    movu         [r0 + 1558 * 16],  m1
    movu         [r0 + 1559 * 16],  m2
    movu         [r0 + 1560 * 16],  m1
    movu         [r0 + 1561 * 16],  m2
    movu         [r0 + 1562 * 16],  m1
    movu         [r0 + 1563 * 16],  m2
    movu         [r0 + 1564 * 16],  m1
    movu         [r0 + 1565 * 16],  m2
    movu         [r0 + 1566 * 16],  m1
    movu         [r0 + 1567 * 16],  m2

    movu         [r0 + 1568 * 16],  m1
    movu         [r0 + 1569 * 16],  m2
    movu         [r0 + 1570 * 16],  m1
    movu         [r0 + 1571 * 16],  m2
    movu         [r0 + 1572 * 16],  m1
    movu         [r0 + 1573 * 16],  m2
    movu         [r0 + 1574 * 16],  m1
    movu         [r0 + 1575 * 16],  m2
    movu         [r0 + 1576 * 16],  m1
    movu         [r0 + 1577 * 16],  m2
    movu         [r0 + 1578 * 16],  m1
    movu         [r0 + 1579 * 16],  m2
    movu         [r0 + 1580 * 16],  m1
    movu         [r0 + 1581 * 16],  m2
    movu         [r0 + 1582 * 16],  m1
    movu         [r0 + 1583 * 16],  m2

    movu         [r0 + 1584 * 16],  m1
    movu         [r0 + 1585 * 16],  m2
    movu         [r0 + 1586 * 16],  m1
    movu         [r0 + 1587 * 16],  m2
    movu         [r0 + 1588 * 16],  m1
    movu         [r0 + 1589 * 16],  m2
    movu         [r0 + 1590 * 16],  m1
    movu         [r0 + 1591 * 16],  m2
    movu         [r0 + 1592 * 16],  m1
    movu         [r0 + 1593 * 16],  m2
    movu         [r0 + 1594 * 16],  m1
    movu         [r0 + 1595 * 16],  m2
    movu         [r0 + 1596 * 16],  m1
    movu         [r0 + 1597 * 16],  m2
    movu         [r0 + 1598 * 16],  m1
    movu         [r0 + 1599 * 16],  m2

    ; mode 27 [row 0]
    movu          m6,    [r5 + 2 * 16]
    movu          m0,    [r3 + 1     ]
    movu          m1,    [r3 + 2     ]
    punpcklbw     m0,    m1
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    movu          m2,    [r3 +  9]
    movu          m3,    [r3 + 10]
    punpcklbw     m2,    m3
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1600 * 16],     m4

    movu          m1,    [r3 + 17]
    movu          m3,    [r3 + 18]
    punpcklbw     m1,    m3
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    movu          m3,    [r3 + 25]
    movu          m5,    [r3 + 26]
    punpcklbw     m3,    m5
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1601 * 16],     m4

    ; mode 27 [row 1]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1602 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1603 * 16],     m4

    ; mode 27 [row 2]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1604 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1605 * 16],     m4

    ; mode 27 [row 3]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1606 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1607 * 16],     m4

    ; mode 27 [row 4]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1608 * 16],     m4

    ; mode 28 [row 1 -first half]
    movu          [r0 + 1666 * 16],     m4

    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1609 * 16],     m4

    ; mode 28 [row 1 - second half]
    movu          [r0 + 1667 * 16],     m4

    ; mode 27 [row 5]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1610 * 16],     m4

    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1611 * 16],     m4

    ; mode 27 [row 6]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1612 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1613 * 16],     m4

    ; mode 27 [row 7]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1614 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1615 * 16],     m4

    ; mode 27 [row 8]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1616 * 16],     m4

    ; mode 29 [row 1 - first half]
    movu          [r0 + 1730 * 16],     m4

    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1617 * 16],     m4

    ; mode 29 [row 1 - second half]
    movu          [r0 + 1731 * 16],     m4

    ; mode 27 [row 9]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1618 * 16],     m4

    ; mode 28 [row 3 -first half]
    movu          [r0 + 1670 * 16],     m4

    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1619 * 16],     m4

    ; mode 28 [row 3 -second half]
    movu          [r0 + 1671 * 16],     m4

    ; mode 27 [row 10]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1620 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1621 * 16],     m4

    ; mode 27 [row 11]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1622 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1623 * 16],     m4

    ; mode 27 [row 12]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1624 * 16],     m4

    ; mode 30 [row 1 - first half]
    movu          [r0 + 1794 * 16],     m4

    ; mode 33 [row 0 - first half]
    movu          [r0 + 1984 * 16],     m4

    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1625 * 16],     m4

    ; mode 30 [row 1 - second half]
    movu          [r0 + 1795 * 16],     m4

    ; mode 33 [row 0 - second half]
    movu          [r0 + 1985 * 16],     m4

    ; mode 27 [row 13]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1626 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1627 * 16],     m4

    ; mode 27 [row 14]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1628 * 16],     m4

    ; mode 28 [row 5 first half]
    movu          [r0 + 1674 * 16],     m4

    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1629 * 16],     m4

    ; mode 28 [row 5 second half]
    movu          [r0 + 1675 * 16],     m4

    ; mode 28 [row 0]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1664 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1665 * 16],     m4

    ; mode 28 [row 2]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1668 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1669 * 16],     m4

    ; mode 28 [row 4]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1672 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1673 * 16],     m4

    ; mode 30 [row 0]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1792 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1793 * 16],     m4

    ; mode 29 [row 0]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1728 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1729 * 16],     m4

    ; mode 29 [row 2]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1732 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1733 * 16],     m4

    ; mode 31 [row 0]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1856 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1857 * 16],     m4

    ; mode 32 [row 0]
    movu          m6,    [r5 + 21 * 16]
    pmaddubsw     m4,    m0,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1920 * 16],     m4
    pmaddubsw     m4,    m1,            m6
    pmulhrsw      m4,    m7
    pmaddubsw     m5,    m3,            m6
    pmulhrsw      m5,    m7
    packuswb      m4,    m5
    movu          [r0 + 1921 * 16],     m4

    ; mode 27 [row 15]
    movu          m0,    [r3 + 2]
    movd          m1,    [r3 + 3]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    movu          m2,    [r3 + 10]
    movd          m3,    [r3 + 11]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    movu          m1,    [r3 + 18]
    movd          m3,    [r3 + 19]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    movu          m4,    [r3 + 26]
    movd          m5,    [r3 + 27]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5

    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1630 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1630 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1631 * 16],     m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 1631 * 16 + 8], m5

    ; mode 27 [row 16]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1632 * 16],     m3

    ; mode 31 [row 1 - first half]
    movu          [r0 + 1858 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1633 * 16],     m3

    ; mode 31 [row 1 - second half]
    movu          [r0 + 1859 * 16],     m3

    ; mode 27 [row 17]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1634 * 16],     m3

    ; mode 29 [row 3 - first half]
    movu          [r0 + 1734 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1635 * 16],     m3

    ; mode 29 [row 3 - second half]
    movu          [r0 + 1735 * 16],     m3

    ; mode 27 [row 18]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1636 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1637 * 16],     m3

    ; mode 27 [row 19]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1638 * 16],     m3

    ; mode 28 [row 7 - first half]
    movu          [r0 + 1678 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1639 * 16],     m3

    ; mode 28 [row 7 - second half]
    movu          [r0 + 1679 * 16],     m3

    ; mode 27 [row 20]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1640 * 16],     m3

    ; mode 32 [row 1 - first half]
    movu          [r0 + 1922 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1641 * 16],     m3

    ; mode 32 [row 1 - second half]
    movu          [r0 + 1923 * 16],     m3

    ; mode 27 [row 21]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1642 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1643 * 16],     m3

    ; mode 27 [row 22]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1644 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1645 * 16],     m3

    ; mode 27 [row 23]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1646 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1647 * 16],     m3

    ; mode 27 [row 24]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1648 * 16],     m3

    ; mode 28 [row 9 - first half]
    movu          [r0 + 1682 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1649 * 16],     m3

    ; mode 28 [row 9 - second half]
    movu          [r0 + 1683 * 16],     m3

    ; mode 27 [row 25]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1650 * 16],     m3

    ; mode 30 [row 3 - first half]
    movu          [r0 + 1798 * 16],     m3

    ; mode 33 [row 1 - first half]
    movu          [r0 + 1986 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1651 * 16],     m3

    ; mode 30 [row 3 - second half]
    movu          [r0 + 1799 * 16],     m3

    ; mode 33 [row 1 - second half]
    movu          [r0 + 1987 * 16],     m3

    ; mode 27 [row 26]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1652 * 16],     m3

    ; mode 29 [row 5 - first half]
    movu          [r0 + 1738 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1653 * 16],     m3

    ; mode 29 [row 5 - second half]
    movu          [r0 + 1739 * 16],     m3

    ; mode 27 [row 27]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1654 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1655 * 16],     m3

    ; mode 27 [row 28]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1656 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1657 * 16],     m3

    ; mode 27 [row 29]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1658 * 16],     m3

    ; mode 28 [row 11 - first half]
    movu          [r0 + 1686 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1659 * 16],     m3

    ; mode 28 [row 11 - second half]
    movu          [r0 + 1687 * 16],     m3

    ; mode 27 [row 30]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1660 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1661 * 16],     m3

    ; mode 28 [row 6]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1676 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1677 * 16],     m3

    ; mode 28 [row 8]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1680 * 16],     m3

    ; mode 29 [row 4 - first half]
    movu          [r0 + 1736 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1681 * 16],     m3

    ; mode 29 [row 4 - second half]
    movu          [r0 + 1737 * 16],     m3

    ; mode 28 [row 10]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1684 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1685 * 16],     m3

    ; mode 29 [row 6]
    movu          m6,    [r5 + 31 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1740 * 16],     m3

    ; mode 32 [row 2 - first half]
    movu          [r0 + 1924 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1741 * 16],     m3

    ; mode 32 [row 2 - second half]
    movu          [r0 + 1925 * 16],     m3

    ; mode 30 [row 2]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1796 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1797 * 16],     m3

    ; mode 31 [row 2]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1860 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1861 * 16],     m3

    ; mode 27 [row 15]
    movu          m0,    [r3 + 3]
    movd          m1,    [r3 + 4]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    movu          m2,    [r3 + 11]
    movd          m3,    [r3 + 12]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    movu          m1,    [r3 + 19]
    movd          m3,    [r3 + 20]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    movu          m4,    [r3 + 27]
    movd          m5,    [r3 + 28]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5

    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1662 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1662 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1663 * 16],     m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 1663 * 16 + 8], m5

    ; mode 28 [row 12]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1688 * 16],     m3

    ; mode 30 [row 4 - first half]
    movu          [r0 + 1800 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1689 * 16],     m3

    ; mode 30 [row 4 - second half]
    movu          [r0 + 1801 * 16],     m3

    ; mode 28 [row 13]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1690 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1691 * 16],     m3

    ; mode 28 [row 14]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1692 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1693 * 16],     m3

    ; mode 28 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1694 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1695 * 16],     m3

    ; mode 28 [row 16]
    movu          m6,    [r5 + 21 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1696 * 16],     m3

    ; mode 31 [row 4 - first half]
    movu          [r0 + 1864 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1697 * 16],     m3

    ; mode 31 [row 4 - second half]
    movu          [r0 + 1865 * 16],     m3

    ; mode 28 [row 17]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1698 * 16],     m3

    ; mode 29 [row 9 - first half]
    movu          [r0 + 1746 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1699 * 16],     m3

    ; mode 29 [row 9 - second half]
    movu          [r0 + 1747 * 16],     m3

    ; mode 28 [row 18]
    movu          m6,    [r5 + 31 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1700 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1701 * 16],     m3

    ; mode 29 [row 7]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1742 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1743 * 16],     m3

    ; mode 29 [row 8]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1744 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1745 * 16],     m3

    ; mode 30 [row 5]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1802 * 16],     m3

    ; mode 33 [row 2 - first half]
    movu          [r0 + 1988 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1803 * 16],     m3

    ; mode 33 [row 2 - second half]
    movu          [r0 + 1989 * 16],     m3

    ; mode 30 [row 6]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1804 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1805 * 16],     m3

    ; mode 31 [row 3]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1862 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1863 * 16],     m3

    ; mode 32 [row 3]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1926 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1927 * 16],     m3

    ; mode 28 [row 19]
    movu          m6,    [r5 + 4 * 16]
    movu          m0,    [r3 + 4]
    movd          m1,    [r3 + 5]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 12]
    movd          m4,    [r3 + 13]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1702 * 16],     m3

    movu          m1,    [r3 + 20]
    movd          m3,    [r3 + 21]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 28]
    movd          m5,    [r3 + 29]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1703 * 16],     m3

    ; mode 28 [row 20]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1704 * 16],     m3

    ; mode 32 [row 4 - first half]
    movu          [r0 + 1928 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1705 * 16],     m3

    ; mode 32 [row 4 - second half]
    movu          [r0 + 1929 * 16],     m3

    ; mode 28 [row 21]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1706 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1707 * 16],     m3

    ; mode 28 [row 22]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1708 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1709 * 16],     m3

    ; mode 28 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1710 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1711 * 16],     m3

    ; mode 28 [row 24]
    movu          m6,    [r5 + 29 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1712 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1713 * 16],     m3

    ; mode 29 [row 10]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1748 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1749 * 16],     m3

    ; mode 29 [row 11]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1750 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1751 * 16],     m3

    ; mode 29 [row 12]
    movu          m6,    [r5 + 21 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1752 * 16],     m3

    ; mode 30 [row 8 -first half]
    movu          [r0 + 1808 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1753 * 16],     m3

    ; mode 30 [row 8 -second half]
    movu          [r0 + 1809 * 16],     m3

    ; mode 29 [row 13]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1754 * 16],     m3

    ; mode 32 [row 5 - first half]
    movu          [r0 + 1930 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1755 * 16],     m3

    ; mode 32 [row 5 - second half]
    movu          [r0 + 1931 * 16],     m3

    ; mode 30 [row 7]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1806 * 16],     m3

    ; mode 33 [row 3 - first half]
    movu          [r0 + 1990 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1807 * 16],     m3

    ; mode 33 [row 3 - second half]
    movu          [r0 + 1991 * 16],     m3

    ; mode 31 [row 5]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1866 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1867 * 16],     m3

    ; mode 31 [row 6]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1868 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1869 * 16],     m3

    ; mode 28 [row 25]
    movu          m6,    [r5 + 2 * 16]
    movu          m0,    [r3 + 5]
    movd          m1,    [r3 + 6]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 13]
    movd          m4,    [r3 + 14]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1714 * 16],     m3

    movu          m1,    [r3 + 21]
    movd          m3,    [r3 + 22]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 29]
    movd          m5,    [r3 + 30]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1715 * 16],     m3

    ; mode 28 [row 26]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1716 * 16],     m3

    ; mode 29 [row 14 - first half]
    movu          [r0 + 1756 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1717 * 16],     m3

    ; mode 29 [row 14 - second half]
    movu          [r0 + 1757 * 16],     m3

    ; mode 28 [row 27]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1718 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1719 * 16],     m3

    ; mode 28 [row 28]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1720 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1721 * 16],     m3

    ; mode 28 [row 29]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1722 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1723 * 16],     m3

    ; mode 28 [row 30]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1724 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1725 * 16],     m3

    ; mode 29 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1758 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1759 * 16],     m3

    ; mode 29 [row 16]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1760 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1761 * 16],     m3

    ; mode 30 [row 9]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1810 * 16],     m3

    ; mode 33 [row 4 - first half]
    movu          [r0 + 1992 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1811 * 16],     m3

    ; mode 33 [row 4 - second half]
    movu          [r0 + 1993 * 16],     m3

    ; mode 30 [row 10]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1812 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1813 * 16],     m3

    ; mode 31 [row 7]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1870 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1871 * 16],     m3

    ; mode 31 [row 8]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1872 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1873 * 16],     m3

    ; mode 32 [row 6]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1932 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1933 * 16],     m3

    ; mode 30 [row 11]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1814 * 16],     m3

    ; mode 33 [row 5 - first half]
    movu          [r0 + 1994 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1815 * 16],     m3

    ; mode 33 [row 5 - second half]
    movu          [r0 + 1995 * 16],     m3

    ; mode 28 [row 31]
    movu          m0,    [r3 + 6]
    movd          m1,    [r3 + 7]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    movu          m2,    [r3 + 14]
    movd          m3,    [r3 + 15]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    movu          m1,    [r3 + 22]
    movd          m3,    [r3 + 23]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    movu          m4,    [r3 + 30]
    movd          m5,    [r3 + 31]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5

    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1726 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1726 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1727 * 16],     m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 1727 * 16 + 8], m5

    ; mode 29 [row 17]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1762 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1763 * 16],     m3

    ; mode 29 [row 18]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1764 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1765 * 16],     m3

    ; mode 29 [row 19]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1766 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1767 * 16],     m3

    ; mode 29 [row 20]
    movu          m6,    [r5 + 29 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1768 * 16],     m3

    ; mode 32 [row 8 - first halif]
    movu          [r0 + 1936 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1769 * 16],     m3

    ; mode 32 [row 8 - second halif]
    movu          [r0 + 1937 * 16],     m3

    ; mode 30 [row 12]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1816 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1817 * 16],     m3

    ; mode 30 [row 13]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1818 * 16],     m3

    ; mode 33 [row 6 - first half]
    movu          [r0 + 1996 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1819 * 16],     m3

    ; mode 33 [row 6 - second half]
    movu          [r0 + 1997 * 16],     m3

    ; mode 31 [row 9]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1874 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1875 * 16],     m3

    ; mode 31 [row 10]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1876 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1877 * 16],     m3

    ; mode 32 [row 7]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1934 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1935 * 16],     m3

    ; mode 29 [row 21]
    movu          m6,    [r5 + 6 * 16]
    movu          m0,    [r3 + 7]
    movd          m1,    [r3 + 8]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 15]
    movd          m4,    [r3 + 16]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1770 * 16],     m3

    movu          m1,    [r3 + 23]
    movd          m3,    [r3 + 24]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 31]
    movd          m5,    [r3 + 32]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1771 * 16],     m3

    ; mode 29 [row 22]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1772 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1773 * 16],     m3

    ; mode 29 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1774 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1775 * 16],     m3

    ; mode 30 [row 14]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1820 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1821 * 16],     m3

    ; mode 30 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1822 * 16],     m3

    ; mode 33 [row 7 - first half]
    movu          [r0 + 1998 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1823 * 16],     m3

    ; mode 33 [row 7 - second half]
    movu          [r0 + 1999 * 16],     m3

    ; mode 30 [row 16]
    movu          m6,    [r5 + 29 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1824 * 16],     m3

    ; mode 31 [row 12 - first half]
    movu          [r0 + 1880 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1825 * 16],     m3

    ; mode 31 [row 12 - second half]
    movu          [r0 + 1881 * 16],     m3

    ; mode 31 [row 11]
    movu          m6,    [r5 + 12 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1878 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1879 * 16],     m3

    ; mode 32 [row 9]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1938 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1939 * 16],     m3

    ; mode 29 [row 24]
    movu          m6,    [r5 + 1 * 16]
    movu          m0,    [r3 + 8]
    movd          m1,    [r3 + 9]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 16]
    movd          m4,    [r3 + 17]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1776 * 16],     m3

    movu          m1,    [r3 + 24]
    movd          m3,    [r3 + 25]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 32]
    movd          m5,    [r3 + 33]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1777 * 16],     m3

    ; mode 29 [row 25]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1778 * 16],     m3

    ; mode 30 [row 17 - first half]
    movu          [r0 + 1826 * 16],     m3

    ; mode 33 [row 8 - first half]
    movu          [r0 + 2000 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1779 * 16],     m3

    ; mode 30 [row 17 - second half]
    movu          [r0 + 1827 * 16],     m3

    ; mode 33 [row 8 - second half]
    movu          [r0 + 2001 * 16],     m3

    ; mode 29 [row 26]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1780 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1781 * 16],     m3

    ; mode 29 [row 27]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1782 * 16],     m3

    ; mode 32 [row 11 - first half]
    movu          [r0 + 1942 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1783 * 16],     m3

    ; mode 32 [row 11 - second half]
    movu          [r0 + 1943 * 16],     m3

    ; mode 30 [row 18]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1828 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1829 * 16],     m3

    ; mode 31 [row 13]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1882 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1883 * 16],     m3

    ; mode 31 [row 14]
    movu          m6,    [r5 + 31 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1884 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1885 * 16],     m3

    ; mode 32 [row 10]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1940 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1941 * 16],     m3

    ; mode 29 [row 28]
    movu          m6,    [r5 + 5 * 16]
    movu          m0,    [r3 +  9]
    movd          m1,    [r3 + 10]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 17]
    movd          m4,    [r3 + 18]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1784 * 16],     m3

    movu          m1,    [r3 + 25]
    movd          m3,    [r3 + 26]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 33]
    movd          m5,    [r3 + 34]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1785 * 16],     m3

    ; mode 29 [row 29]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1786 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1787 * 16],     m3

    ; mode 29 [row 30]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1788 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1789 * 16],     m3

    ; mode 30 [row 19]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1830 * 16],     m3

    ; mode 33 [row 9 - first half]
    movu          [r0 + 2002 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1831 * 16],     m3

    ; mode 33 [row 9 - second half]
    movu          [r0 + 2003 * 16],     m3

    ; mode 30 [row 20]
    movu          m6,    [r5 + 17 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1832 * 16],     m3

    ; mode 32 [row 12 - first half]
    movu          [r0 + 1944 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1833 * 16],     m3

    ; mode 32 [row 12 - second half]
    movu          [r0 + 1945 * 16],     m3

    ; mode 30 [row 21]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1834 * 16],     m3

    ; mode 33 [row 10 - first half]
    movu          [r0 + 2004 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1835 * 16],     m3

    ; mode 33 [row 10 - second half]
    movu          [r0 + 2005 * 16],     m3

    ; mode 31 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1886 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1887 * 16],     m3

    ; mode 29 [row 31]
    movu          m0,    [r3 + 10]
    movd          m1,    [r3 + 11]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    movu          m2,    [r3 + 18]
    movd          m3,    [r3 + 19]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    movu          m1,    [r3 + 26]
    movd          m3,    [r3 + 27]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    movu          m4,    [r3 + 34]
    movd          m5,    [r3 + 35]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5

    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1790 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1790 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1791 * 16],     m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 1791 * 16 + 8], m5

    ; mode 30 [row 22]
    movu          m6,    [r5 + 11 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1836 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1837 * 16],     m3

    ; mode 30 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1838 * 16],     m3

    ; mode 33 [row 11 - first half]
    movu          [r0 + 2006 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1839 * 16],     m3

    ; mode 33 [row 11 - second half]
    movu          [r0 + 2007 * 16],     m3

    ; mode 31 [row 16]
    movu          m6,    [r5 + 1 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1888 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1889 * 16],     m3

    ; mode 31 [row 17]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1890 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1891 * 16],     m3

    ; mode 32 [row 13]
    movu          m6,    [r5 + 6 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1946 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1947 * 16],     m3

    ; mode 32 [row 14]
    movu          m6,    [r5 + 27 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1948 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1949 * 16],     m3

    ; mode 30 [row 24]
    movu          m6,    [r5 + 5 * 16]
    movu          m0,    [r3 + 11]
    movd          m1,    [r3 + 12]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 19]
    movd          m4,    [r3 + 20]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1840 * 16],     m3

    movu          m1,    [r3 + 27]
    movd          m3,    [r3 + 28]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 35]
    movd          m5,    [r3 + 36]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1841 * 16],     m3

    ; mode 30 [row 25]
    movu          m6,    [r5 + 18 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1842 * 16],     m3

    ; mode 33 [row 12 - first half]
    movu          [r0 + 2008 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1843 * 16],     m3

    ; mode 33 [row 12 - second half]
    movu          [r0 + 2009 * 16],     m3

    ; mode 30 [row 26]
    movu          m6,    [r5 + 31 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1844 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1845 * 16],     m3

    ; mode 31 [row 18]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1892 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1893 * 16],     m3

    ; mode 31 [row 19]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1894 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1895 * 16],     m3

    ; mode 32 [row 15]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1950 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1951 * 16],     m3

    ; mode 30 [row 27]
    movu          m6,    [r5 + 12 * 16]
    movu          m0,    [r3 + 12]
    movd          m1,    [r3 + 13]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 20]
    movd          m4,    [r3 + 21]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1846 * 16],     m3

    ; mode 33 [row 13 - first half]
    movu          [r0 + 2010 * 16],     m3

    movu          m1,    [r3 + 28]
    movd          m3,    [r3 + 29]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 36]
    movd          m5,    [r3 + 37]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1847 * 16],     m3

    ; mode 33 [row 13 - second half]
    movu          [r0 + 2011 * 16],     m3

    ; mode 30 [row 28]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1848 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1849 * 16],     m3

    ; mode 31 [row 20]
    movu          m6,    [r5 + 5 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1896 * 16],     m3

    ; mode 32 [row 16 - first half]
    movu          [r0 + 1952 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1897 * 16],     m3

    ; mode 32 [row 16 - second half]
    movu          [r0 + 1953 * 16],     m3

    ; mode 31 [row 21]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1898 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1899 * 16],     m3

    ; mode 32 [row 17]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1954 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1955 * 16],     m3

    ; mode 30 [row 29]
    movu          m6,    [r5 + 6 * 16]
    movu          m0,    [r3 + 13]
    movd          m1,    [r3 + 14]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 21]
    movd          m4,    [r3 + 22]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1850 * 16],     m3

    ; mode 33 [row 14 - first half]
    movu          [r0 + 2012 * 16],     m3

    movu          m1,    [r3 + 29]
    movd          m3,    [r3 + 30]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 37]
    movd          m5,    [r3 + 38]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1851 * 16],     m3

    ; mode 33 [row 14 - second half]
    movu          [r0 + 2013 * 16],     m3

    ; mode 30 [row 30]
    movu          m6,    [r5 + 19 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1852 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1853 * 16],     m3

    ; mode 31 [row 22]
    movu          m6,    [r5 + 7 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1900 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1901 * 16],     m3

    ; mode 31 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1902 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1903 * 16],     m3

    ; mode 32 [row 18]
    movu          m6,    [r5 + 15 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1956 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1957 * 16],     m3

    ; mode 30 [row 31]
    movu          m0,    [r3 + 14]
    movd          m1,    [r3 + 15]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    movu          m2,    [r3 + 22]
    movd          m3,    [r3 + 23]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    movu          m1,    [r3 + 30]
    movd          m3,    [r3 + 31]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    movu          m4,    [r3 + 38]
    movd          m5,    [r3 + 39]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5

    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1854 * 16],     m5

    ; mode 33 [row 15 - first eight]
    movh          [r0 + 2014 * 16],     m5

    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1854 * 16 + 8], m5

    ; mode 33 [row 15 - second eight]
    movh          [r0 + 2014 * 16 + 8],     m5

    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1855 * 16],     m5

    ; mode 33 [row 15 - third eight]
    movh          [r0 + 2015 * 16],     m5

    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 1855 * 16 + 8], m5

    ; mode 33 [row 15 - fourth eight]
    movh          [r0 + 2015 * 16 + 8], m5

    ; mode 31 [row 24]
    movu          m6,    [r5 + 9 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1904 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1905 * 16],     m3

    ; mode 31 [row 25]
    movu          m6,    [r5 + 26 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1906 * 16],     m3

    ; mode 33 [row 16 - first half]
    movu          [r0 + 2016 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1907 * 16],     m3

    ; mode 33 [row 16 - second half]
    movu          [r0 + 2017 * 16],     m3

    ; mode 32 [row 19]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1958 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1959 * 16],     m3

    ; mode 32 [row 20]
    movu          m6,    [r5 + 25 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1960 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1961 * 16],     m3

    ; mode 31 [row 26]
    movu          m6,    [r5 + 11 * 16]
    movu          m0,    [r3 + 15]
    movd          m1,    [r3 + 16]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 23]
    movd          m4,    [r3 + 24]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1908 * 16],     m3

    movu          m1,    [r3 + 31]
    movd          m3,    [r3 + 32]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 39]
    movd          m5,    [r3 + 40]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1909 * 16],     m3

    ; mode 31 [row 27]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1910 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1911 * 16],     m3

    ; mode 32 [row 21]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1962 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1963 * 16],     m3

    ; mode 33 [row 17]
    movu          m6,    [r5 + 20 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2018 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2019 * 16],     m3

    ; mode 31 [row 28]
    movu          m6,    [r5 + 13 * 16]
    movu          m0,    [r3 + 16]
    movd          m1,    [r3 + 17]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 24]
    movd          m4,    [r3 + 25]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1912 * 16],     m3

    movu          m1,    [r3 + 32]
    movd          m3,    [r3 + 33]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 40]
    movd          m5,    [r3 + 41]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1913 * 16],     m3

    ; mode 31 [row 29]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1914 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1915 * 16],     m3

    ; mode 32 [row 22]
    movu          m6,    [r5 + 3 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1964 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1965 * 16],     m3

    ; mode 32 [row 23]
    movu          m6,    [r5 + 24 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1966 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1967 * 16],     m3

    ; mode 33 [row 18]
    movu          m6,    [r5 + 14 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2020 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2021 * 16],     m3

    ; mode 31 [row 30]
    movu          m6,    [r5 + 15 * 16]
    movu          m0,    [r3 + 17]
    movd          m1,    [r3 + 18]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 25]
    movd          m4,    [r3 + 26]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1916 * 16],     m3

    movu          m1,    [r3 + 33]
    movd          m3,    [r3 + 34]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 41]
    movd          m5,    [r3 + 42]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1917 * 16],     m3

    ; mode 32 [row 24]
    movu          m6,    [r5 + 13 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1968 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1969 * 16],     m3

    ; mode 33 [row 19]
    movu          m6,    [r5 + 8 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2022 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2023 * 16],     m3

    ; mode 31 [row 31]
    movu          m0,    [r3 + 18]
    movd          m1,    [r3 + 19]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    movu          m2,    [r3 + 26]
    movd          m3,    [r3 + 27]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    movu          m1,    [r3 + 34]
    movd          m3,    [r3 + 35]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    movu          m4,    [r3 + 42]
    movd          m5,    [r3 + 43]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5

    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1918 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1918 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1919 * 16],     m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 1919 * 16 + 8], m5

    ; mode 32 [row 25]
    movu          m6,    [r5 + 2 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1970 * 16],     m3

    ; mode 33 [row 20 - first half]
    movu          [r0 + 2024 * 16],     m3

    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1971 * 16],     m3

    ; mode 33 [row 20 - second half]
    movu          [r0 + 2025 * 16],     m3

    ; mode 32 [row 26]
    movu          m6,    [r5 + 23 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1972 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1973 * 16],     m3

    ; mode 33 [row 21]
    movu          m6,    [r5 + 28 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2026 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2027 * 16],     m3

    ; mode 32 [row 27]
    movu          m6,    [r5 + 12 * 16]
    movu          m0,    [r3 + 19]
    movd          m1,    [r3 + 20]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 27]
    movd          m4,    [r3 + 28]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1974 * 16],     m3

    movu          m1,    [r3 + 35]
    movd          m3,    [r3 + 36]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 43]
    movd          m5,    [r3 + 44]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1975 * 16],     m3

    ; mode 33 [row 22]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2028 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2029 * 16],     m3

    ; mode 32 [row 28]
    movu          m6,    [r5 + 1 * 16]
    movu          m0,    [r3 + 20]
    movd          m1,    [r3 + 21]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 28]
    movd          m4,    [r3 + 29]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1976 * 16],     m3

    movu          m1,    [r3 + 36]
    movd          m3,    [r3 + 37]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 44]
    movd          m5,    [r3 + 45]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1977 * 16],     m3

    ; mode 32 [row 29]
    movu          m6,    [r5 + 22 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1978 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1979 * 16],     m3

    ; mode 33 [row 23]
    movu          m6,    [r5 + 16 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2030 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2031 * 16],     m3

    ; mode 32 [row 30]
    movu          m6,    [r5 + 11 * 16]
    movu          m0,    [r3 + 21]
    movd          m1,    [r3 + 22]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 29]
    movd          m4,    [r3 + 30]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1980 * 16],     m3

    movu          m1,    [r3 + 37]
    movd          m3,    [r3 + 38]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 45]
    movd          m5,    [r3 + 46]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 1981 * 16],     m3

    ; mode 33 [row 24]
    movu          m6,    [r5 + 10 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2032 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2033 * 16],     m3

    ; mode 32 [row 31]
    movu          m0,    [r3 + 22]
    movd          m1,    [r3 + 23]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    movu          m2,    [r3 + 30]
    movd          m3,    [r3 + 31]
    palignr       m3,    m2,        1
    punpcklbw     m2,    m3
    movu          m1,    [r3 + 38]
    movd          m3,    [r3 + 39]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    movu          m4,    [r3 + 46]
    movd          m5,    [r3 + 47]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5

    pshufb        m5,    m0,            [tab_S2]
    movh          [r0 + 1982 * 16],     m5
    pshufb        m5,    m2,            [tab_S2]
    movh          [r0 + 1982 * 16 + 8], m5
    pshufb        m5,    m1,            [tab_S2]
    movh          [r0 + 1983 * 16],     m5
    pshufb        m5,    m4,            [tab_S2]
    movh          [r0 + 1983 * 16 + 8], m5

    ; mode 33 [row 25]
    movu          m6,    [r5 + 4 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2034 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2035 * 16],     m3

    ; mode 33 [row 26]
    movu          m6,    [r5 + 30 * 16]
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2036 * 16],     m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2037 * 16],     m3

    ; mode 33 [row 27]
    movu          m6,    [r5 + 24 * 16]
    movu          m0,    [r3 + 23]
    movd          m1,    [r3 + 24]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 31]
    movd          m4,    [r3 + 32]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2038 * 16],     m3

    movu          m1,    [r3 + 39]
    movd          m3,    [r3 + 40]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 47]
    movd          m5,    [r3 + 48]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2039 * 16],     m3

    ; mode 33 [row 28]
    movu          m6,    [r5 + 18 * 16]
    movu          m0,    [r3 + 24]
    movd          m1,    [r3 + 25]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 32]
    movd          m4,    [r3 + 33]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2040 * 16],     m3

    movu          m1,    [r3 + 40]
    movd          m3,    [r3 + 41]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 48]
    movd          m5,    [r3 + 49]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2041 * 16],     m3

    ; mode 33 [row 29]
    movu          m6,    [r5 + 12 * 16]
    movu          m0,    [r3 + 25]
    movd          m1,    [r3 + 26]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 33]
    movd          m4,    [r3 + 34]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2042 * 16],     m3

    movu          m1,    [r3 + 41]
    movd          m3,    [r3 + 42]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 49]
    movd          m5,    [r3 + 50]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2043 * 16],     m3

    ; mode 33 [row 30]
    movu          m6,    [r5 + 6 * 16]
    movu          m0,    [r3 + 26]
    movd          m1,    [r3 + 27]
    palignr       m1,    m0,        1
    punpcklbw     m0,    m1
    pmaddubsw     m3,    m0,            m6
    pmulhrsw      m3,    m7
    movu          m2,    [r3 + 34]
    movd          m4,    [r3 + 35]
    palignr       m4,    m2,        1
    punpcklbw     m2,    m4
    pmaddubsw     m5,    m2,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2044 * 16],     m3

    movu          m1,    [r3 + 42]
    movd          m3,    [r3 + 43]
    palignr       m3,    m1,        1
    punpcklbw     m1,    m3
    pmaddubsw     m3,    m1,            m6
    pmulhrsw      m3,    m7
    movu          m4,    [r3 + 50]
    movd          m5,    [r3 + 51]
    palignr       m5,    m4,        1
    punpcklbw     m4,    m5
    pmaddubsw     m5,    m4,            m6
    pmulhrsw      m5,    m7
    packuswb      m3,    m5
    movu          [r0 + 2045 * 16],     m3

    ; mode 33 [row 31]
    movu          m5,                   [r3 + 27]
    movu          [r0 + 2046 * 16],     m5
    movu          m5,                   [r3 + 43]
    movu          [r0 + 2047 * 16],     m5

    ;mode 34 [row 0]
    movu       m0,              [r3 + 2]
    movu       [r0 + 2048 * 16],   m0
    movu       m1,              [r3 + 18]
    movu       [r0 + 2049 * 16],   m1

    ;mode 34 [row 1]
    movu       m2,              [r3 + 34]
    palignr    m3,              m1,       m0,    1
    movu       [r0 + 2050 * 16],   m3
    palignr    m4,              m2,       m1,    1
    movu       [r0 + 2051 * 16],   m4

    ;mode 34 [row 2]
    palignr    m3,              m1,       m0,    2
    movu       [r0 + 2052 * 16],   m3
    palignr    m4,              m2,       m1,    2
    movu       [r0 + 2053 * 16],   m4

    ;mode 34 [row 3]
    palignr    m3,              m1,       m0,    3
    movu       [r0 + 2054 * 16],   m3
    palignr    m4,              m2,       m1,    3
    movu       [r0 + 2055 * 16],   m4

    ;mode 34 [row 4]
    palignr    m3,              m1,       m0,    4
    movu       [r0 + 2056 * 16],   m3
    palignr    m4,              m2,       m1,    4
    movu       [r0 + 2057 * 16],   m4

    ;mode 34 [row 5]
    palignr    m3,              m1,       m0,    5
    movu       [r0 + 2058 * 16],   m3
    palignr    m4,              m2,       m1,    5
    movu       [r0 + 2059 * 16],   m4

    ;mode 34 [row 6]
    palignr    m3,              m1,       m0,    6
    movu       [r0 + 2060 * 16],   m3
    palignr    m4,              m2,       m1,    6
    movu       [r0 + 2061 * 16],   m4

    ;mode 34 [row 7]
    palignr    m3,              m1,       m0,    7
    movu       [r0 + 2062 * 16],   m3
    palignr    m4,              m2,       m1,    7
    movu       [r0 + 2063 * 16],   m4

    ;mode 34 [row 8]
    palignr    m3,              m1,       m0,    8
    movu       [r0 + 2064 * 16],   m3
    palignr    m4,              m2,       m1,    8
    movu       [r0 + 2065 * 16],   m4

    ;mode 34 [row 9]
    palignr    m3,              m1,       m0,    9
    movu       [r0 + 2066 * 16],   m3
    palignr    m4,              m2,       m1,    9
    movu       [r0 + 2067 * 16],   m4

    ;mode 34 [row 10]
    palignr    m3,              m1,       m0,    10
    movu       [r0 + 2068 * 16],   m3
    palignr    m4,              m2,       m1,    10
    movu       [r0 + 2069 * 16],   m4

    ;mode 34 [row 11]
    palignr    m3,              m1,       m0,    11
    movu       [r0 + 2070 * 16],   m3
    palignr    m4,              m2,       m1,    11
    movu       [r0 + 2071 * 16],   m4

    ;mode 34 [row 12]
    palignr    m3,              m1,       m0,    12
    movu       [r0 + 2072 * 16],   m3
    palignr    m4,              m2,       m1,    12
    movu       [r0 + 2073 * 16],   m4

    ;mode 34 [row 13]
    palignr    m3,              m1,       m0,    13
    movu       [r0 + 2074 * 16],   m3
    palignr    m4,              m2,       m1,    13
    movu       [r0 + 2075 * 16],   m4

    ;mode 34 [row 14]
    palignr    m3,              m1,       m0,    14
    movu       [r0 + 2076 * 16],   m3
    palignr    m4,              m2,       m1,    14
    movu       [r0 + 2077 * 16],   m4

    ;mode 34 [row 15]
    palignr    m3,              m1,       m0,    15
    movu       [r0 + 2078 * 16],   m3
    palignr    m4,              m2,       m1,    15
    movu       [r0 + 2079 * 16],   m4

    ;mode 34 [row 16]
    palignr    m3,              m1,       m0,    16
    movu       [r0 + 2080 * 16],   m3
    palignr    m4,              m2,       m1,    16
    movu       [r0 + 2081 * 16],   m4

    ;mode 34 [row 17]
    movu       m0,                [r3 + 19]
    movu       [r0 + 2082 * 16],   m0
    movu       m1,                [r3 + 35]
    movu       [r0 + 2083 * 16],   m1

    mov        r2d, r6d
    mov        [r4], r2b
    mov        r2d, [rsp]
    mov        [r1 + 64], r2b

    ;mode 34 [row 18]
    movu       m2,              [r3 + 51]
    palignr    m3,              m1,       m0,    1
    movu       [r0 + 2084 * 16],   m3
    palignr    m4,              m2,       m1,    1
    movu       [r0 + 2085 * 16],   m4

    ;mode 34 [row 19]
    palignr    m3,              m1,       m0,    2
    movu       [r0 + 2086 * 16],   m3
    palignr    m4,              m2,       m1,    2
    movu       [r0 + 2087 * 16],   m4

    ;mode 34 [row 20]
    palignr    m3,              m1,       m0,    3
    movu       [r0 + 2088 * 16],   m3
    palignr    m4,              m2,       m1,    3
    movu       [r0 + 2089 * 16],   m4

    ;mode 34 [row 21]
    palignr    m3,              m1,       m0,    4
    movu       [r0 + 2090 * 16],   m3
    palignr    m4,              m2,       m1,    4
    movu       [r0 + 2091 * 16],   m4

    ;mode 34 [row 22]
    palignr    m3,              m1,       m0,    5
    movu       [r0 + 2092 * 16],   m3
    palignr    m4,              m2,       m1,    5
    movu       [r0 + 2093 * 16],   m4

    ;mode 34 [row 23]
    palignr    m3,              m1,       m0,    6
    movu       [r0 + 2094 * 16],   m3
    palignr    m4,              m2,       m1,    6
    movu       [r0 + 2095 * 16],   m4

    ;mode 34 [row 24]
    palignr    m3,              m1,       m0,    7
    movu       [r0 + 2096 * 16],   m3
    palignr    m4,              m2,       m1,    7
    movu       [r0 + 2097 * 16],   m4

    ;mode 34 [row 25]
    palignr    m3,              m1,       m0,    8
    movu       [r0 + 2098 * 16],   m3
    palignr    m4,              m2,       m1,    8
    movu       [r0 + 2099 * 16],   m4

    ;mode 34 [row 26]
    palignr    m3,              m1,       m0,    9
    movu       [r0 + 2100 * 16],   m3
    palignr    m4,              m2,       m1,    9
    movu       [r0 + 2101 * 16],   m4

    ;mode 34 [row 27]
    palignr    m3,              m1,       m0,    10
    movu       [r0 + 2102 * 16],   m3
    palignr    m4,              m2,       m1,    10
    movu       [r0 + 2103 * 16],   m4

    ;mode 34 [row 28]
    palignr    m3,              m1,       m0,    11
    movu       [r0 + 2104 * 16],   m3
    palignr    m4,              m2,       m1,    11
    movu       [r0 + 2105 * 16],   m4

    ;mode 34 [row 29]
    palignr    m3,              m1,       m0,    12
    movu       [r0 + 2106 * 16],   m3
    palignr    m4,              m2,       m1,    12
    movu       [r0 + 2107 * 16],   m4

    ;mode 34 [row 30]
    palignr    m3,              m1,       m0,    13
    movu       [r0 + 2108 * 16],   m3
    palignr    m4,              m2,       m1,    13
    movu       [r0 + 2109 * 16],   m4

    ;mode 34 [row 31]
    palignr    m3,              m1,       m0,    14
    movu       [r0 + 2110 * 16],   m3
    palignr    m4,              m2,       m1,    14
    movu       [r0 + 2111 * 16],   m4
    RET


;-----------------------------------------------------------------------------
; void all_angs_pred_4x4(pixel *dest, pixel *refPix, pixel *filtPix, int bLuma)
;-----------------------------------------------------------------------------
INIT_YMM avx2
cglobal all_angs_pred_4x4, 2, 2, 6

    mova           m5, [pw_1024]

; mode 2

    vbroadcasti128 m0, [r1 + 9]
    pshufb         m1, m0, [allAng4_shuf_mode2]
    movu           [r0], xm1

; mode 3

    pshufb         m1, m0, [allAng4_shuf_mode3_4]
    pmaddubsw      m1, [allAng4_fact_mode3_4]
    pmulhrsw       m1, m5

; mode 4

    pshufb         m2, m0, [allAng4_shuf_mode3_4 + mmsize]
    pmaddubsw      m2, [allAng4_fact_mode3_4 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m1, m2
    movu           [r0 + (3 - 2) * 16], m1

; mode 5

    pshufb         m1, m0, [allAng4_shuf_mode5_6]
    pmaddubsw      m1, [allAng4_fact_mode5_6]
    pmulhrsw       m1, m5

; mode 6

    pshufb         m2, m0, [allAng4_shuf_mode5_6 + mmsize]
    pmaddubsw      m2, [allAng4_fact_mode5_6 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m1, m2
    movu           [r0 + (5 - 2) * 16], m1

; mode 7

    pshufb         m3, m0, [allAng4_shuf_mode7_8]
    pmaddubsw      m1, m3, [allAng4_fact_mode7_8]
    pmulhrsw       m1, m5

; mode 8

    pshufb         m2, m0, [allAng4_shuf_mode7_8 + mmsize]
    pmaddubsw      m2, [allAng4_fact_mode7_8 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m1, m2
    movu           [r0 + (7 - 2) * 16], m1

; mode 9

    pmaddubsw      m3, [allAng4_fact_mode9]
    pmulhrsw       m3, m5
    packuswb       m3, m3
    vpermq         m3, m3, 11011000b
    movu           [r0 + (9 - 2) * 16], xm3

; mode 10

    pshufb         xm1, xm0, [allAng4_shuf_mode10]
    movu           [r0 + (10 - 2) * 16], xm1

    pxor           xm1, xm1
    movd           xm2, [r1 + 1]
    pshufd         xm3, xm2, 0
    punpcklbw      xm3, xm1
    pinsrb         xm2, [r1], 0
    pshufb         xm4, xm2, xm1
    punpcklbw      xm4, xm1
    psubw          xm3, xm4
    psraw          xm3, 1
    pshufb         xm4, xm0, xm1
    punpcklbw      xm4, xm1
    paddw          xm3, xm4
    packuswb       xm3, xm1

    pextrb         [r0 + 128], xm3, 0
    pextrb         [r0 + 132], xm3, 1
    pextrb         [r0 + 136], xm3, 2
    pextrb         [r0 + 140], xm3, 3

; mode 11

    vbroadcasti128 m0, [r1]
    pshufb         m3, m0, [allAng4_shuf_mode11_12]
    pmaddubsw      m1, m3, [allAng4_fact_mode11_12]
    pmulhrsw       m1, m5

; mode 12

    pmaddubsw      m2, m3, [allAng4_fact_mode11_12 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m1, m2
    movu           [r0 + (11 - 2) * 16], m1

; mode 13

    pmaddubsw      m3, [allAng4_fact_mode13_14]
    pmulhrsw       m3, m5

; mode 14

    pshufb         m2, m0, [allAng4_shuf_mode13_14]
    pmaddubsw      m2, [allAng4_fact_mode13_14 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m3, m2
    movu           [r0 + (13 - 2) * 16], m3

; mode 15

    pshufb         m1, m0, [allAng4_shuf_mode15_16]
    pmaddubsw      m1, [allAng4_fact_mode15_16]
    pmulhrsw       m1, m5

; mode 16

    pshufb         m2, m0, [allAng4_shuf_mode15_16 + mmsize]
    pmaddubsw      m2, [allAng4_fact_mode15_16 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m1, m2
    movu           [r0 + (15 - 2) * 16], m1

; mode 17

    pshufb         m1, m0, [allAng4_shuf_mode17]
    pmaddubsw      m1, [allAng4_fact_mode17]
    pmulhrsw       m1, m5
    packuswb       m1, m1
    vpermq         m1, m1, 11011000b

; mode 18

    pshufb         m2, m0, [allAng4_shuf_mode18]
    vinserti128    m1, m1, xm2, 1
    movu           [r0 + (17 - 2) * 16], m1

; mode 19

    pshufb         m1, m0, [allAng4_shuf_mode19_20]
    pmaddubsw      m1, [allAng4_fact_mode19_20]
    pmulhrsw       m1, m5

; mode 20

    pshufb         m2, m0, [allAng4_shuf_mode19_20 + mmsize]
    pmaddubsw      m2, [allAng4_fact_mode19_20 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m1, m2
    movu           [r0 + (19 - 2) * 16], m1

; mode 21

    pshufb         m1, m0, [allAng4_shuf_mode21_22]
    pmaddubsw      m1, [allAng4_fact_mode21_22]
    pmulhrsw       m1, m5

; mode 22

    pshufb         m2, m0, [allAng4_shuf_mode21_22 + mmsize]
    pmaddubsw      m2, [allAng4_fact_mode21_22 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m1, m2
    movu           [r0 + (21 - 2) * 16], m1

; mode 23

    pshufb         m3, m0, [allAng4_shuf_mode23_24]
    pmaddubsw      m1, m3, [allAng4_fact_mode23_24]
    pmulhrsw       m1, m5

; mode 24

    pshufb         m2, m0, [allAng4_shuf_mode23_24 + mmsize]
    pmaddubsw      m2, [allAng4_fact_mode23_24 + mmsize]
    pmulhrsw       m2, m5
    packuswb       m1, m2
    movu           [r0 + (23 - 2) * 16], m1

; mode 25

    pmaddubsw      m3, [allAng4_fact_mode25]
    pmulhrsw       m3, m5
    packuswb       m3, m3
    vpermq         m3, m3, 11011000b
    movu           [r0 + (25 - 2) * 16], xm3

; mode 26

    pshufb         m1, m0, [allAng4_shuf_mode26]
    movu           [r0 + (26 - 2) * 16], xm1

    pxor           xm1, xm1
    movd           xm2, [r1 + 9]
    pshufd         xm3, xm2, 0
    punpcklbw      xm3, xm1
    pinsrb         xm4, [r1 + 0], 0
    pshufb         xm4, xm1
    punpcklbw      xm4, xm1
    psubw          xm3, xm4
    psraw          xm3, 1
    psrldq         xm2, xm0, 1
    pshufb         xm2, xm1
    punpcklbw      xm2, xm1
    paddw          xm3, xm2
    packuswb       xm3, xm1

    pextrb       [r0 + 384], xm3, 0
    pextrb       [r0 + 388], xm3, 1
    pextrb       [r0 + 392], xm3, 2
    pextrb       [r0 + 396], xm3, 3

; mode 27

    pshufb        m3, m0, [allAng4_shuf_mode27_28]
    pmaddubsw     m1, m3, [allAng4_fact_mode27_28]
    pmulhrsw      m1, m5

; mode 28

    pmaddubsw     m2, m3, [allAng4_fact_mode27_28 + mmsize]
    pmulhrsw      m2, m5
    packuswb      m1, m2
    movu          [r0 + (27 - 2) * 16], m1

; mode 29

    pmaddubsw     m3, [allAng4_fact_mode29_30]
    pmulhrsw      m3, m5

; mode 30

    pshufb        m2, m0, [allAng4_shuf_mode29_30]
    pmaddubsw     m2, [allAng4_fact_mode29_30 + mmsize]
    pmulhrsw      m2, m5
    packuswb      m3, m2
    movu          [r0 + (29 - 2) * 16], m3

; mode 31

    pshufb        m1, m0, [allAng4_shuf_mode31_32]
    pmaddubsw     m1, [allAng4_fact_mode31_32]
    pmulhrsw      m1, m5

; mode 32

    pshufb        m2, m0, [allAng4_shuf_mode31_32 + mmsize]
    pmaddubsw     m2, [allAng4_fact_mode31_32 + mmsize]
    pmulhrsw      m2, m5
    packuswb      m1, m2
    movu          [r0 + (31 - 2) * 16], m1

; mode 33

    pshufb        m1, m0, [allAng4_shuf_mode33]
    pmaddubsw     m1, [allAng4_fact_mode33]
    pmulhrsw      m1, m5
    packuswb      m1, m2
    vpermq        m1, m1, 11011000b

; mode 34

    pshufb        m0, [allAng4_shuf_mode34]
    vinserti128   m1, m1, xm0, 1
    movu          [r0 + (33 - 2) * 16], m1
    RET

;-----------------------------------------------------------------------------
; void all_angs_pred_4x4(pixel *dest, pixel *refPix, pixel *filtPix, int bLuma)
;-----------------------------------------------------------------------------
INIT_XMM sse2
cglobal all_angs_pred_4x4, 4, 4, 8

; mode 2

    movh        m6,             [r1 + 9]
    mova        m2,             m6
    psrldq      m2,             1
    movd        [r0],           m2              ;byte[A, B, C, D]
    psrldq      m2,             1
    movd        [r0 + 4],       m2              ;byte[B, C, D, E]
    psrldq      m2,             1
    movd        [r0 + 8],       m2              ;byte[C, D, E, F]
    psrldq      m2,             1
    movd        [r0 + 12],      m2              ;byte[D, E, F, G]

; mode 10/26

    pxor        m7,             m7
    pshufd      m5,             m6,        0
    mova        [r0 + 128],     m5              ;mode 10 byte[9, A, B, C, 9, A, B, C, 9, A, B, C, 9, A, B, C]

    movd        m4,             [r1 + 1]
    pshufd      m4,             m4,        0
    mova        [r0 + 384],     m4              ;mode 26 byte[1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4]

    movd        m1,             [r1]
    punpcklbw   m1,             m7
    pshuflw     m1,             m1,     0x00
    punpcklqdq  m1,             m1              ;m1 = byte[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    punpckldq   m4,             m5
    punpcklbw   m4,             m7              ;m4 = word[1, 2, 3, 4, 9, A, B, C]
    pshuflw     m2,             m4,     0x00
    pshufhw     m2,             m2,     0x00    ;m2 = word[1, 1, 1, 1, 9, 9, 9, 9]

    psubw       m4,             m1
    psraw       m4,             1

    pshufd      m2,             m2,     q1032   ;m2 = word[9, 9, 9, 9, 1, 1, 1, 1]
    paddw       m4,             m2
    packuswb    m4,             m4

%if ARCH_X86_64
    movq        r2,             m4

    mov         [r0 + 128],     r2b              ;mode 10
    shr         r2,             8
    mov         [r0 + 132],     r2b
    shr         r2,             8
    mov         [r0 + 136],     r2b
    shr         r2,             8
    mov         [r0 + 140],     r2b
    shr         r2,             8
    mov         [r0 + 384],     r2b              ;mode 26
    shr         r2d,            8
    mov         [r0 + 388],     r2b
    shr         r2d,            8
    mov         [r0 + 392],     r2b
    shr         r2d,            8
    mov         [r0 + 396],     r2b

%else
    movd        r2d,             m4

    mov         [r0 + 128],     r2b              ;mode 10
    shr         r2d,             8
    mov         [r0 + 132],     r2b
    shr         r2d,             8
    mov         [r0 + 136],     r2b
    shr         r2d,             8
    mov         [r0 + 140],     r2b

    psrldq      m4,             4
    movd        r2d,            m4

    mov         [r0 + 384],     r2b              ;mode 26
    shr         r2d,            8
    mov         [r0 + 388],     r2b
    shr         r2d,            8
    mov         [r0 + 392],     r2b
    shr         r2d,            8
    mov         [r0 + 396],     r2b
%endif

; mode 3

    mova        m2,             [pw_16]
    lea         r3,             [pw_ang_table + 7 * 16]
    lea         r2,             [pw_ang_table + 23 * 16]
    punpcklbw   m6,             m6
    psrldq      m6,             1
    movh        m1,             m6
    psrldq      m6,             2
    movh        m0,             m6
    psrldq      m6,             2
    movh        m3,             m6
    psrldq      m6,             2
    punpcklbw   m1,             m7              ;m1 = word[9, A, A, B, B, C, C, D]
    punpcklbw   m0,             m7              ;m0 = word[A, B, B, C, C, D, D, E]
    punpcklbw   m3,             m7              ;m3 = word[B, C, C, D, D, E, E, F]
    punpcklbw   m6,             m7              ;m6 = word[C, D, D, E, E, F, F, G]

    mova        m7,             [r2 - 3 * 16]

    pmaddwd     m5,             m1,     [r2 + 3 * 16]
    pmaddwd     m4,             m0,     m7

    packssdw    m5,             m4
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m4,             m3,     [r3 + 7 * 16]
    pmaddwd     m6,             [r3 + 1 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 16],      m5
    movd        [r0 + 68],      m5              ;mode 6 row 1
    psrldq      m5,             4
    movd        [r0 + 76],      m5              ;mode 6 row 3

; mode 4

    pmaddwd     m4,             m0,     [r2 + 8 * 16]
    pmaddwd     m6,             m3,     m7

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m5,             m1,     [r2 - 2 * 16]
    pmaddwd     m6,             m0,     [r3 + 3 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    packuswb    m5,             m4
    mova        [r0 + 32],      m5

; mode 5

    pmaddwd     m5,             m1,     [r2 - 6 * 16]
    pmaddwd     m6,             m0,     [r3 - 5 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m4,             m0,     [r2 - 4 * 16]
    pmaddwd     m3,             [r3 - 3 * 16]

    packssdw    m4,             m3
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 48],      m5

; mode 6

    pmaddwd     m5,             m1,     [r3 + 6 * 16]
    pmaddwd     m6,             m0,     [r3 + 0 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    packuswb    m5,             m6
    movd        [r0 + 64],      m5
    psrldq      m5,             4
    movd        [r0 + 72],      m5

; mode 7

    pmaddwd     m5,             m1,     [r3 + 2 * 16]
    pmaddwd     m6,             m1,     [r2 - 5 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    mova        m3,             [r2 + 4 * 16]
    pmaddwd     m4,             m1,     m3
    pmaddwd     m0,             [r3 - 3 * 16]

    packssdw    m4,             m0
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 80],      m5

; mode 8

    mova        m0,             [r3 - 2 * 16]
    pmaddwd     m5,             m1,     m0
    pmaddwd     m6,             m1,     [r3 + 3 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m4,             m1,     [r3 + 8 * 16]
    pmaddwd     m7,             m1

    packssdw    m4,             m7
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 96],      m5

; mode 9

    pmaddwd     m5,             m1,     [r3 - 5 * 16]
    pmaddwd     m6,             m1,     [r3 - 3 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m4,             m1,     [r3 - 1 * 16]
    pmaddwd     m6,             m1,     [r3 + 1 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 112],     m5

; mode 11

    movd        m5,             [r1]
    punpcklwd   m5,             m1
    pand        m5,             [pb_0000000000000F0F]
    pslldq      m1,             4
    por         m1,             m5              ;m1 = word[0, 9, 9, A, A, B, B, C]

    pmaddwd     m5,             m1,     [r2 + 7 * 16]
    pmaddwd     m6,             m1,     [r2 + 5 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m4,             m1,     [r2 + 3 * 16]
    pmaddwd     m6,             m1,     [r2 + 1 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 144],     m5

; mode 12

    pmaddwd     m3,             m1
    pmaddwd     m6,             m1,     [r2 - 1 * 16]

    packssdw    m3,             m6
    paddw       m3,             m2
    psraw       m3,             5

    pmaddwd     m4,             m1,     [r2 - 6 * 16]
    pmaddwd     m6,             m1,     [r3 + 5 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m3,             m4
    mova        [r0 + 160],     m3

; mode 13

    mova        m3,             m1
    movd        m7,             [r1 + 4]
    punpcklwd   m7,             m1
    pand        m7,             [pb_0000000000000F0F]
    pslldq      m3,             4
    por         m3,             m7              ;m3 = word[4, 0, 0, 9, 9, A, A, B]

    pmaddwd     m5,             m1,     [r2 + 0 * 16]
    pmaddwd     m6,             m1,     [r3 + 7 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m4,             m1,     m0
    pmaddwd     m6,             m3,     [r2 + 5 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 176],     m5

; mode 14

    pmaddwd     m5,             m1,     [r2 - 4 * 16]
    pmaddwd     m6,             m1,     [r3 - 1 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    movd        m6,             [r1 + 2]
    pand        m3,             [pw_FFFFFFFFFFFFFFF0]
    pand        m6,             [pb_000000000000000F]
    por         m3,             m6              ;m3 = word[2, 0, 0, 9, 9, A, A, B]

    pmaddwd     m4,             m3,     [r2 + 2 * 16]
    pmaddwd     m6,             m3,     [r3 + 5 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 192],     m5
    psrldq      m5,             4
    movd        [r0 + 240],     m5              ;mode 17 row 0

; mode 15

    pmaddwd     m5,             m1,     [r3 + 8 * 16]
    pmaddwd     m6,             m3,     [r2 + 7 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m6,             m3,     [r3 + 6 * 16]

    mova        m0,             m3
    punpcklwd   m7,             m3
    pslldq      m0,             4
    pand        m7,             [pb_0000000000000F0F]
    por         m0,             m7              ;m0 = word[4, 2, 2, 0, 0, 9, 9, A]

    pmaddwd     m4,             m0,     [r2 + 5 * 16]

    packssdw    m6,             m4
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m5,             m6
    mova        [r0 + 208],     m5

; mode 16

    pmaddwd     m5,             m1,     [r3 + 4 * 16]
    pmaddwd     m6,             m3,     [r2 - 1 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m3,             [r3 - 6 * 16]

    movd        m6,             [r1 + 3]
    pand        m0,             [pw_FFFFFFFFFFFFFFF0]
    pand        m6,             [pb_000000000000000F]
    por         m0,             m6              ;m0 = word[3, 2, 2, 0, 0, 9, 9, A]

    pmaddwd     m0,             [r3 + 5 * 16]
    packssdw    m3,             m0
    paddw       m3,             m2
    psraw       m3,             5

    packuswb    m5,             m3
    mova        [r0 + 224],     m5

; mode 17

    movd        m4,             [r1 + 1]
    punpcklwd   m4,             m1
    pand        m4,             [pb_0000000000000F0F]
    pslldq      m1,             4
    por         m1,             m4              ;m1 = word[1, 0, 0, 9, 9, A, A, B]

    pmaddwd     m6,             m1,     [r3 + 5 * 16]

    packssdw    m6,             m6
    paddw       m6,             m2
    psraw       m6,             5

    movd        m5,             [r1 + 2]
    punpcklwd   m5,             m1
    pand        m5,             [pb_0000000000000F0F]
    pslldq      m1,             4
    por         m1,             m5              ;m1 = word[2, 1, 1, 0, 0, 9, 9, A]

    pmaddwd     m4,             m1,     [r2 - 5 * 16]

    punpcklwd   m7,             m1
    pand        m7,             [pb_0000000000000F0F]
    pslldq      m1,             4
    por         m1,             m7              ;m1 = word[4, 2, 2, 1, 1, 0, 0, 9]

    pmaddwd     m1,             [r2 + 1 * 16]
    packssdw    m4,             m1
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m6,             m4
    movd        [r0 + 244],     m6
    psrldq      m6,             8
    movh        [r0 + 248],     m6

; mode 18

    movh        m1,             [r1]
    movd        [r0 + 256],     m1              ;byte[0, 1, 2, 3]

    movh        m3,             [r1 + 2]
    punpcklqdq  m3,             m1
    psrldq      m3,             7
    movd        [r0 + 260],     m3              ;byte[2, 1, 0, 9]

    movh        m4,             [r1 + 3]
    punpcklqdq  m4,             m3
    psrldq      m4,             7
    movd        [r0 + 264],     m4              ;byte[1, 0, 9, A]

    movh        m0,             [r1 + 4]
    punpcklqdq  m0,             m4
    psrldq      m0,             7
    movd        [r0 + 268],     m0              ;byte[0, 9, A, B]

; mode 19

    pxor        m7,             m7
    punpcklbw   m4,             m3
    punpcklbw   m3,             m1
    punpcklbw   m1,             m1
    punpcklbw   m4,             m7              ;m4 = word[A, 9, 9, 0, 0, 1, 1, 2]
    punpcklbw   m3,             m7              ;m3 = word[9, 0, 0, 1, 1, 2, 2, 3]
    psrldq      m1,             1
    punpcklbw   m1,             m7              ;m1 = word[0, 1, 1, 2, 2, 3, 3, 4]

    pmaddwd     m6,             m1,     [r3 - 1 * 16]
    pmaddwd     m7,             m3,     [r3 + 5 * 16]

    packssdw    m6,             m7
    paddw       m6,             m2
    psraw       m6,             5

    pmaddwd     m5,             m4,     [r2 - 5 * 16]

    movd        m7,             [r1 + 12]
    punpcklwd   m7,             m4
    pand        m7,             [pb_0000000000000F0F]
    pslldq      m4,             4
    por         m4,             m7              ;m4 = word[C, A, A, 9, 9, 0, 0, 1]

    pmaddwd     m4,             [r2 + 1 * 16]
    packssdw    m5,             m4
    paddw       m5,             m2
    psraw       m5,             5

    packuswb    m6,             m5
    mova        [r0 + 272],     m6
    movd        [r0 + 324],     m6              ;mode 22 row 1

; mode 20

    pmaddwd     m5,             m1,     [r3 + 4 * 16]

    movd        m4,             [r1 + 10]
    pand        m3,             [pw_FFFFFFFFFFFFFFF0]
    pand        m4,             [pb_000000000000000F]
    por         m3,             m4              ;m3 = word[A, 0, 0, 1, 1, 2, 2, 3]

    pmaddwd     m6,             m3,     [r2 - 1 * 16]

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    pmaddwd     m4,             m3,     [r3 - 6 * 16]

    punpcklwd   m0,             m3
    pand        m0,             [pb_0000000000000F0F]
    mova        m6,             m3
    pslldq      m6,             4
    por         m0,             m6              ;m0 = word[B, A, A, 0, 0, 1, 1, 2]

    pmaddwd     m6,             m0,     [r3 + 5 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    packuswb    m5,             m4
    mova        [r0 + 288],     m5

; mode 21

    pmaddwd     m4,             m1,     [r3 + 8 * 16]
    pmaddwd     m6,             m3,     [r2 + 7 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m5,             m3,     [r3 + 6 * 16]

    pand        m0,             [pw_FFFFFFFFFFFFFFF0]
    pand        m7,             [pb_000000000000000F]
    por         m0,             m7              ;m0 = word[C, A, A, 0, 0, 1, 1, 2]

    pmaddwd     m0,             [r2 + 5 * 16]
    packssdw    m5,             m0
    paddw       m5,             m2
    psraw       m5,             5

    packuswb    m4,             m5
    mova        [r0 + 304],     m4

; mode 22

    pmaddwd     m4,             m1,     [r2 - 4 * 16]
    packssdw    m4,             m4
    paddw       m4,             m2
    psraw       m4,             5

    mova        m0,             [r3 + 5 * 16]
    pmaddwd     m5,             m3,     [r2 + 2 * 16]
    pmaddwd     m6,             m3,     m0

    packssdw    m5,             m6
    paddw       m5,             m2
    psraw       m5,             5

    packuswb    m4,             m5
    movd        [r0 + 320],     m4
    psrldq      m4,             8
    movh        [r0 + 328],     m4

; mode 23

    pmaddwd     m4,             m1,     [r2 + 0 * 16]
    pmaddwd     m5,             m1,     [r3 + 7 * 16]

    packssdw    m4,             m5
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m6,             m1,     [r3 - 2 * 16]

    pand        m3,             [pw_FFFFFFFFFFFFFFF0]
    por         m3,             m7              ;m3 = word[C, 0, 0, 1, 1, 2, 2, 3]

    pmaddwd     m3,             [r2 + 5 * 16]
    packssdw    m6,             m3
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m4,             m6
    mova        [r0 + 336],     m4

; mode 24

    pmaddwd     m4,             m1,     [r2 + 4 * 16]
    pmaddwd     m5,             m1,     [r2 - 1 * 16]

    packssdw    m4,             m5
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m6,             m1,     [r2 - 6 * 16]
    pmaddwd     m0,             m1

    packssdw    m6,             m0
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m4,             m6
    mova        [r0 + 352],     m4

; mode 25

    pmaddwd     m4,             m1,     [r2 + 7 * 16]
    pmaddwd     m5,             m1,     [r2 + 5 * 16]

    packssdw    m4,             m5
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m6,             m1,     [r2 + 3 * 16]
    pmaddwd     m1,             [r2 + 1 * 16]

    packssdw    m6,             m1
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m4,             m6
    mova        [r0 + 368],     m4

; mode 27

    movh        m0,             [r1 + 1]
    pxor        m7,             m7
    punpcklbw   m0,             m0
    psrldq      m0,             1
    movh        m1,             m0
    psrldq      m0,             2
    movh        m3,             m0
    psrldq      m0,             2
    punpcklbw   m1,             m7              ;m1 = word[1, 2, 2, 3, 3, 4, 4, 5]
    punpcklbw   m3,             m7              ;m3 = word[2, 3, 3, 4, 4, 5, 5, 6]
    punpcklbw   m0,             m7              ;m0 = word[3, 4, 4, 5, 5, 6, 6, 7]

    mova        m7,             [r3 - 3 * 16]

    pmaddwd     m4,             m1,     [r3 - 5 * 16]
    pmaddwd     m5,             m1,     m7

    packssdw    m4,             m5
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m6,             m1,     [r3 - 1 * 16]
    pmaddwd     m5,             m1,     [r3 + 1 * 16]

    packssdw    m6,             m5
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m4,             m6
    mova        [r0 + 400],     m4

; mode 28

    pmaddwd     m4,             m1,     [r3 - 2 * 16]
    pmaddwd     m5,             m1,     [r3 + 3 * 16]

    packssdw    m4,             m5
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m6,             m1,     [r3 + 8 * 16]
    pmaddwd     m5,             m1,     [r2 - 3 * 16]

    packssdw    m6,             m5
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m4,             m6
    mova        [r0 + 416],     m4

; mode 29

    pmaddwd     m4,             m1,     [r3 + 2 * 16]
    pmaddwd     m6,             m1,     [r2 - 5 * 16]

    packssdw    m4,             m6
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m6,             m1,     [r2 + 4 * 16]
    pmaddwd     m5,             m3,     m7

    packssdw    m6,             m5
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m4,             m6
    mova        [r0 + 432],     m4

; mode 30

    pmaddwd     m4,             m1,     [r3 + 6 * 16]
    pmaddwd     m5,             m1,     [r2 + 3 * 16]

    packssdw    m4,             m5
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m6,             m3,     [r3 + 0 * 16]
    pmaddwd     m5,             m3,     [r2 - 3 * 16]

    packssdw    m6,             m5
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m4,             m6
    mova        [r0 + 448],     m4
    psrldq      m4,             4
    movh        [r0 + 496],     m4              ;mode 33 row 0
    psrldq      m4,             8
    movd        [r0 + 500],     m4              ;mode 33 row 1

; mode 31

    pmaddwd     m4,             m1,     [r2 - 6 * 16]
    pmaddwd     m5,             m3,     [r3 - 5 * 16]

    packssdw    m4,             m5
    paddw       m4,             m2
    psraw       m4,             5

    pmaddwd     m6,             m3,     [r2 - 4 * 16]
    pmaddwd     m7,             m0

    packssdw    m6,             m7
    paddw       m6,             m2
    psraw       m6,             5

    packuswb    m4,             m6
    mova        [r0 + 464],     m4

; mode 32

    pmaddwd     m1,             [r2 - 2 * 16]
    pmaddwd     m5,             m3,     [r3 + 3 * 16]

    packssdw    m1,             m5
    paddw       m1,             m2
    psraw       m1,             5

    pmaddwd     m3,             [r2 + 8 * 16]
    pmaddwd     m5,             m0,     [r2 - 3 * 16]
    packssdw    m3,             m5
    paddw       m3,             m2
    psraw       m3,             5

    packuswb    m1,             m3
    mova        [r0 + 480],     m1

; mode 33

    pmaddwd     m0,             [r3 + 7 * 16]
    pxor        m7,             m7
    movh        m4,             [r1 + 4]
    punpcklbw   m4,             m4
    psrldq      m4,             1
    punpcklbw   m4,             m7

    pmaddwd     m4,             [r3 + 1 * 16]

    packssdw    m0,             m4
    paddw       m0,             m2
    psraw       m0,             5

    packuswb    m0,             m0
    movh        [r0 + 504],     m0

; mode 34

    movh        m7,             [r1 + 2]
    movd        [r0 + 512],     m7              ;byte[2, 3, 4, 5]

    psrldq      m7,             1
    movd        [r0 + 516],     m7              ;byte[3, 4, 5, 6]

    psrldq      m7,             1
    movd        [r0 + 520],     m7              ;byte[4, 5, 6, 7]

    psrldq      m7,             1
    movd        [r0 + 524],     m7              ;byte[5, 6, 7, 8]

RET
