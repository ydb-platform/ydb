;*****************************************************************************
;* const-a.asm: x86 global constants
;*****************************************************************************
;* Copyright (C) 2003-2013 x264 project
;* Copyright (C) 2013-2017 MulticoreWare, Inc
;*
;* Authors: Loren Merritt <lorenm@u.washington.edu>
;*          Fiona Glaser <fiona@x264.com>
;*          Min Chen <chenm003@163.com> <min.chen@multicorewareinc.com>
;*          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
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

SECTION_RODATA 32

;; 8-bit constants

const pb_0,                 times 32 db 0
const pb_1,                 times 32 db 1
const pb_2,                 times 32 db 2
const pb_3,                 times 32 db 3
const pb_4,                 times 32 db 4
const pb_8,                 times 32 db 8
const pb_15,                times 32 db 15
const pb_16,                times 32 db 16
const pb_31,                times 32 db 31
const pb_32,                times 32 db 32
const pb_64,                times 32 db 64
const pb_124,               times 32 db 124
const pb_128,               times 32 db 128
const pb_a1,                times 16 db 0xa1

const pb_01,                times  8 db   0,   1
const pb_0123,              times  4 db   0,   1
                            times  4 db   2,   3
const hsub_mul,             times 16 db   1,  -1
const pw_swap,              times  2 db   6,   7,   4,   5,   2,   3,   0,   1
const pb_unpackbd1,         times  2 db   0,   0,   0,   0,   1,   1,   1,   1,   2,   2,   2,   2,   3,   3,   3,   3
const pb_unpackbd2,         times  2 db   4,   4,   4,   4,   5,   5,   5,   5,   6,   6,   6,   6,   7,   7,   7,   7
const pb_unpackwq1,         times  1 db   0,   1,   0,   1,   0,   1,   0,   1,   2,   3,   2,   3,   2,   3,   2,   3
const pb_unpackwq2,         times  1 db   4,   5,   4,   5,   4,   5,   4,   5,   6,   7,   6,   7,   6,   7,   6,   7
const pb_shuf8x8c,          times  1 db   0,   0,   0,   0,   2,   2,   2,   2,   4,   4,   4,   4,   6,   6,   6,   6
const pb_movemask,          times 16 db 0x00
                            times 16 db 0xFF

const pb_movemask_32,       times 32 db 0x00
                            times 32 db 0xFF
                            times 32 db 0x00

const pb_0000000000000F0F,  times  2 db 0xff, 0x00
                            times 12 db 0x00
const pb_000000000000000F,           db 0xff
                            times 15 db 0x00
const pb_shuf_off4,         times  2 db   0,   4,   1,   5,   2,   6,   3,   7
const pw_shuf_off4,         times  1 db   0,   1,   8,   9,   2,   3,  10,  11,   4,   5,  12,  13,   6,   7,  14,  15

;; 16-bit constants

const pw_n1,                times 16 dw -1
const pw_1,                 times 16 dw 1
const pw_2,                 times 16 dw 2
const pw_3,                 times 16 dw 3
const pw_7,                 times 16 dw 7
const pw_m2,                times  8 dw -2
const pw_4,                 times  8 dw 4
const pw_8,                 times  8 dw 8
const pw_16,                times 16 dw 16
const pw_15,                times 16 dw 15
const pw_31,                times 16 dw 31
const pw_32,                times 16 dw 32
const pw_64,                times  8 dw 64
const pw_128,               times 16 dw 128
const pw_256,               times 16 dw 256
const pw_257,               times 16 dw 257
const pw_512,               times 16 dw 512
const pw_1023,              times 16 dw 1023
const pw_1024,              times 16 dw 1024
const pw_2048,              times 16 dw 2048
const pw_4096,              times 16 dw 4096
const pw_8192,              times  8 dw 8192
const pw_00ff,              times 16 dw 0x00ff
const pw_ff00,              times  8 dw 0xff00
const pw_2000,              times 16 dw 0x2000
const pw_8000,              times  8 dw 0x8000
const pw_3fff,              times 16 dw 0x3fff
const pw_32_0,              times  4 dw 32,
                            times  4 dw 0
const pw_pixel_max,         times 16 dw ((1 << BIT_DEPTH)-1)

const pw_0_7,               times  2 dw   0,   1,   2,   3,   4,   5,   6,   7
const pw_ppppmmmm,          times  1 dw   1,   1,   1,   1,  -1,  -1,  -1,  -1
const pw_ppmmppmm,          times  1 dw   1,   1,  -1,  -1,   1,   1,  -1,  -1
const pw_pmpmpmpm,          times 16 dw   1,  -1,   1,  -1,   1,  -1,   1,  -1
const pw_pmmpzzzz,          times  1 dw   1,  -1,  -1,   1,   0,   0,   0,   0
const multi_2Row,           times  1 dw   1,   2,   3,   4,   1,   2,   3,   4
const multiH,               times  1 dw   9,  10,  11,  12,  13,  14,  15,  16
const multiH3,              times  1 dw  25,  26,  27,  28,  29,  30,  31,  32
const multiL,               times  1 dw   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16
const multiH2,              times  1 dw  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32
const pw_planar16_mul,      times  1 dw  15,  14,  13,  12,  11,  10,   9,   8,   7,   6,   5,   4,   3,   2,   1,   0
const pw_planar32_mul,      times  1 dw  31,  30,  29,  28,  27,  26,  25,  24,  23,  22,  21,  20,  19,  18,  17,  16
const pw_FFFFFFFFFFFFFFF0,           dw 0x00
                            times  7 dw 0xff
const hmul_16p,             times 16 db   1
                            times  8 db   1,  -1
const pw_exp2_0_15,                  dw 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768
const pw_1_ffff,            times  4 dw 1
                            times  4 dw 0xFFFF


;; 32-bit constants

const pd_1,                 times  8 dd 1
const pd_2,                 times  8 dd 2
const pd_4,                 times  4 dd 4
const pd_8,                 times  4 dd 8
const pd_15,                times  8 dd 15
const pd_16,                times  8 dd 16
const pd_31,                times  8 dd 31
const pd_32,                times  8 dd 32
const pd_64,                times  4 dd 64
const pd_128,               times  4 dd 128
const pd_256,               times  4 dd 256
const pd_512,               times  4 dd 512
const pd_1024,              times  4 dd 1024
const pd_2048,              times  4 dd 2048
const pd_ffff,              times  4 dd 0xffff
const pd_32767,             times  4 dd 32767
const pd_524416,            times  4 dd 524416
const pd_n32768,            times  8 dd 0xffff8000
const pd_n131072,           times  4 dd 0xfffe0000
const pd_0000ffff,          times  8 dd 0x0000FFFF
const pd_planar16_mul0,     times  1 dd  15,  14,  13,  12,  11,  10,   9,   8,    7,   6,   5,   4,   3,   2,   1,   0
const pd_planar16_mul1,     times  1 dd   1,   2,   3,   4,   5,   6,   7,   8,    9,  10,  11,  12,  13,  14,  15,  16
const pd_planar32_mul1,     times  1 dd  31,  30,  29,  28,  27,  26,  25,  24,   23,  22,  21,  20,  19,  18,  17,  16
const pd_planar32_mul2,     times  1 dd  17,  18,  19,  20,  21,  22,  23,  24,   25,  26,  27,  28,  29,  30,  31,  32
const pd_planar16_mul2,     times  1 dd  15,  14,  13,  12,  11,  10,   9,   8,    7,   6,   5,   4,   3,   2,   1,   0
const trans8_shuf,          times  1 dd   0,   4,   1,   5,   2,   6,   3,   7

;; 64-bit constants

const pq_1,                 times 1 dq 1
