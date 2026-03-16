/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Mandar Gurav <mandar@multicorewareinc.com>
 *          Deepthi Devaki Akkoorath <deepthidevaki@multicorewareinc.com>
 *          Mahesh Pittala <mahesh@multicorewareinc.com>
 *          Rajesh Paulraj <rajesh@multicorewareinc.com>
 *          Min Chen <min.chen@multicorewareinc.com>
 *          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
 *          Nabajit Deka <nabajit@multicorewareinc.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com.
 *****************************************************************************/

#include "common.h"
#include "primitives.h"
#include <xmmintrin.h> // SSE
#include <pmmintrin.h> // SSE3
#include <tmmintrin.h> // SSSE3

#define DCT16_SHIFT1  (3 + X265_DEPTH - 8)
#define DCT16_ADD1    (1 << ((DCT16_SHIFT1) - 1))

#define DCT16_SHIFT2  10
#define DCT16_ADD2    (1 << ((DCT16_SHIFT2) - 1))

#define DCT32_SHIFT1  (DCT16_SHIFT1 + 1)
#define DCT32_ADD1    (1 << ((DCT32_SHIFT1) - 1))

#define DCT32_SHIFT2  (DCT16_SHIFT2 + 1)
#define DCT32_ADD2    (1 << ((DCT32_SHIFT2) - 1))

using namespace X265_NS;

ALIGN_VAR_32(static const int16_t, tab_dct_8[][8]) =
{
    { 0x0100, 0x0F0E, 0x0706, 0x0908, 0x0302, 0x0D0C, 0x0504, 0x0B0A },

    { 64, 64, 64, 64, 64, 64, 64, 64 },
    { 64, -64, 64, -64, 64, -64, 64, -64 },
    { 83, 36, 83, 36, 83, 36, 83, 36 },
    { 36, -83, 36, -83, 36, -83, 36, -83 },
    { 89, 18, 75, 50, 89, 18, 75, 50 },
    { 75, -50, -18, -89, 75, -50, -18, -89 },
    { 50, 75, -89, 18, 50, 75, -89, 18 },
    { 18, -89, -50, 75, 18, -89, -50, 75 },

    { 83, 83, -83, -83, 36, 36, -36, -36 },
    { 36, 36, -36, -36, -83, -83, 83, 83 },
    { 89, -89, 18, -18, 75, -75, 50, -50 },
    { 75, -75, -50, 50, -18, 18, -89, 89 },
    { 50, -50, 75, -75, -89, 89, 18, -18 },
    { 18, -18, -89, 89, -50, 50, 75, -75 },
};

ALIGN_VAR_32(static const int16_t, tab_dct_16_0[][8]) =
{
    { 0x0F0E, 0x0D0C, 0x0B0A, 0x0908, 0x0706, 0x0504, 0x0302, 0x0100 },  // 0
    { 0x0100, 0x0F0E, 0x0706, 0x0908, 0x0302, 0x0D0C, 0x0504, 0x0B0A },  // 1
    { 0x0100, 0x0706, 0x0302, 0x0504, 0x0F0E, 0x0908, 0x0D0C, 0x0B0A },  // 2
    { 0x0F0E, 0x0908, 0x0D0C, 0x0B0A, 0x0100, 0x0706, 0x0302, 0x0504 },  // 3
};

ALIGN_VAR_32(static const int16_t, tab_dct_16_1[][8]) =
{
    { 90, 87, 80, 70, 57, 43, 25,  9 },  //  0
    { 87, 57,  9, -43, -80, -90, -70, -25 },  //  1
    { 80,  9, -70, -87, -25, 57, 90, 43 },  //  2
    { 70, -43, -87,  9, 90, 25, -80, -57 },  //  3
    { 57, -80, -25, 90, -9, -87, 43, 70 },  //  4
    { 43, -90, 57, 25, -87, 70,  9, -80 },  //  5
    { 25, -70, 90, -80, 43,  9, -57, 87 },  //  6
    {  9, -25, 43, -57, 70, -80, 87, -90 },  //  7
    { 83, 83, -83, -83, 36, 36, -36, -36 },  //  8
    { 36, 36, -36, -36, -83, -83, 83, 83 },  //  9
    { 89, 89, 18, 18, 75, 75, 50, 50 },  // 10
    { 75, 75, -50, -50, -18, -18, -89, -89 },  // 11
    { 50, 50, 75, 75, -89, -89, 18, 18 },  // 12
    { 18, 18, -89, -89, -50, -50, 75, 75 },  // 13

#define MAKE_COEF(a0, a1, a2, a3, a4, a5, a6, a7) \
    { (a0), -(a0), (a3), -(a3), (a1), -(a1), (a2), -(a2) \
    }, \
    { (a7), -(a7), (a4), -(a4), (a6), -(a6), (a5), -(a5) },

    MAKE_COEF(90, 87, 80, 70, 57, 43, 25,  9)
    MAKE_COEF(87, 57,  9, -43, -80, -90, -70, -25)
    MAKE_COEF(80,  9, -70, -87, -25, 57, 90, 43)
    MAKE_COEF(70, -43, -87,  9, 90, 25, -80, -57)
    MAKE_COEF(57, -80, -25, 90, -9, -87, 43, 70)
    MAKE_COEF(43, -90, 57, 25, -87, 70,  9, -80)
    MAKE_COEF(25, -70, 90, -80, 43,  9, -57, 87)
    MAKE_COEF(9, -25, 43, -57, 70, -80, 87, -90)
#undef MAKE_COEF
};

static void dct16(const int16_t *src, int16_t *dst, intptr_t stride)
{
    // Const
    __m128i c_4     = _mm_set1_epi32(DCT16_ADD1);
    __m128i c_512   = _mm_set1_epi32(DCT16_ADD2);

    int i;

    ALIGN_VAR_32(int16_t, tmp[16 * 16]);

    __m128i T00A, T01A, T02A, T03A, T04A, T05A, T06A, T07A;
    __m128i T00B, T01B, T02B, T03B, T04B, T05B, T06B, T07B;
    __m128i T10, T11, T12, T13, T14, T15, T16, T17;
    __m128i T20, T21, T22, T23, T24, T25, T26, T27;
    __m128i T30, T31, T32, T33, T34, T35, T36, T37;
    __m128i T40, T41, T42, T43, T44, T45, T46, T47;
    __m128i T50, T51, T52, T53;
    __m128i T60, T61, T62, T63, T64, T65, T66, T67;
    __m128i T70;

    // DCT1
    for (i = 0; i < 16; i += 8)
    {
        T00A = _mm_load_si128((__m128i*)&src[(i + 0) * stride + 0]);    // [07 06 05 04 03 02 01 00]
        T00B = _mm_load_si128((__m128i*)&src[(i + 0) * stride + 8]);    // [0F 0E 0D 0C 0B 0A 09 08]
        T01A = _mm_load_si128((__m128i*)&src[(i + 1) * stride + 0]);    // [17 16 15 14 13 12 11 10]
        T01B = _mm_load_si128((__m128i*)&src[(i + 1) * stride + 8]);    // [1F 1E 1D 1C 1B 1A 19 18]
        T02A = _mm_load_si128((__m128i*)&src[(i + 2) * stride + 0]);    // [27 26 25 24 23 22 21 20]
        T02B = _mm_load_si128((__m128i*)&src[(i + 2) * stride + 8]);    // [2F 2E 2D 2C 2B 2A 29 28]
        T03A = _mm_load_si128((__m128i*)&src[(i + 3) * stride + 0]);    // [37 36 35 34 33 32 31 30]
        T03B = _mm_load_si128((__m128i*)&src[(i + 3) * stride + 8]);    // [3F 3E 3D 3C 3B 3A 39 38]
        T04A = _mm_load_si128((__m128i*)&src[(i + 4) * stride + 0]);    // [47 46 45 44 43 42 41 40]
        T04B = _mm_load_si128((__m128i*)&src[(i + 4) * stride + 8]);    // [4F 4E 4D 4C 4B 4A 49 48]
        T05A = _mm_load_si128((__m128i*)&src[(i + 5) * stride + 0]);    // [57 56 55 54 53 52 51 50]
        T05B = _mm_load_si128((__m128i*)&src[(i + 5) * stride + 8]);    // [5F 5E 5D 5C 5B 5A 59 58]
        T06A = _mm_load_si128((__m128i*)&src[(i + 6) * stride + 0]);    // [67 66 65 64 63 62 61 60]
        T06B = _mm_load_si128((__m128i*)&src[(i + 6) * stride + 8]);    // [6F 6E 6D 6C 6B 6A 69 68]
        T07A = _mm_load_si128((__m128i*)&src[(i + 7) * stride + 0]);    // [77 76 75 74 73 72 71 70]
        T07B = _mm_load_si128((__m128i*)&src[(i + 7) * stride + 8]);    // [7F 7E 7D 7C 7B 7A 79 78]

        T00B = _mm_shuffle_epi8(T00B, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T01B = _mm_shuffle_epi8(T01B, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T02B = _mm_shuffle_epi8(T02B, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T03B = _mm_shuffle_epi8(T03B, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T04B = _mm_shuffle_epi8(T04B, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T05B = _mm_shuffle_epi8(T05B, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T06B = _mm_shuffle_epi8(T06B, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T07B = _mm_shuffle_epi8(T07B, _mm_load_si128((__m128i*)tab_dct_16_0[0]));

        T10  = _mm_add_epi16(T00A, T00B);
        T11  = _mm_add_epi16(T01A, T01B);
        T12  = _mm_add_epi16(T02A, T02B);
        T13  = _mm_add_epi16(T03A, T03B);
        T14  = _mm_add_epi16(T04A, T04B);
        T15  = _mm_add_epi16(T05A, T05B);
        T16  = _mm_add_epi16(T06A, T06B);
        T17  = _mm_add_epi16(T07A, T07B);

        T20  = _mm_sub_epi16(T00A, T00B);
        T21  = _mm_sub_epi16(T01A, T01B);
        T22  = _mm_sub_epi16(T02A, T02B);
        T23  = _mm_sub_epi16(T03A, T03B);
        T24  = _mm_sub_epi16(T04A, T04B);
        T25  = _mm_sub_epi16(T05A, T05B);
        T26  = _mm_sub_epi16(T06A, T06B);
        T27  = _mm_sub_epi16(T07A, T07B);

        T30  = _mm_shuffle_epi8(T10, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T31  = _mm_shuffle_epi8(T11, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T32  = _mm_shuffle_epi8(T12, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T33  = _mm_shuffle_epi8(T13, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T34  = _mm_shuffle_epi8(T14, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T35  = _mm_shuffle_epi8(T15, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T36  = _mm_shuffle_epi8(T16, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T37  = _mm_shuffle_epi8(T17, _mm_load_si128((__m128i*)tab_dct_16_0[1]));

        T40  = _mm_hadd_epi16(T30, T31);
        T41  = _mm_hadd_epi16(T32, T33);
        T42  = _mm_hadd_epi16(T34, T35);
        T43  = _mm_hadd_epi16(T36, T37);
        T44  = _mm_hsub_epi16(T30, T31);
        T45  = _mm_hsub_epi16(T32, T33);
        T46  = _mm_hsub_epi16(T34, T35);
        T47  = _mm_hsub_epi16(T36, T37);

        T50  = _mm_hadd_epi16(T40, T41);
        T51  = _mm_hadd_epi16(T42, T43);
        T52  = _mm_hsub_epi16(T40, T41);
        T53  = _mm_hsub_epi16(T42, T43);

        T60  = _mm_madd_epi16(T50, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T61  = _mm_madd_epi16(T51, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1);
        T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1);
        T70  = _mm_packs_epi32(T60, T61);
        _mm_store_si128((__m128i*)&tmp[0 * 16 + i], T70);

        T60  = _mm_madd_epi16(T50, _mm_load_si128((__m128i*)tab_dct_8[2]));
        T61  = _mm_madd_epi16(T51, _mm_load_si128((__m128i*)tab_dct_8[2]));
        T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1);
        T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1);
        T70  = _mm_packs_epi32(T60, T61);
        _mm_store_si128((__m128i*)&tmp[8 * 16 + i], T70);

        T60  = _mm_madd_epi16(T52, _mm_load_si128((__m128i*)tab_dct_8[3]));
        T61  = _mm_madd_epi16(T53, _mm_load_si128((__m128i*)tab_dct_8[3]));
        T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1);
        T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1);
        T70  = _mm_packs_epi32(T60, T61);
        _mm_store_si128((__m128i*)&tmp[4 * 16 + i], T70);

        T60  = _mm_madd_epi16(T52, _mm_load_si128((__m128i*)tab_dct_8[4]));
        T61  = _mm_madd_epi16(T53, _mm_load_si128((__m128i*)tab_dct_8[4]));
        T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1);
        T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1);
        T70  = _mm_packs_epi32(T60, T61);
        _mm_store_si128((__m128i*)&tmp[12 * 16 + i], T70);

        T60  = _mm_madd_epi16(T44, _mm_load_si128((__m128i*)tab_dct_8[5]));
        T61  = _mm_madd_epi16(T45, _mm_load_si128((__m128i*)tab_dct_8[5]));
        T62  = _mm_madd_epi16(T46, _mm_load_si128((__m128i*)tab_dct_8[5]));
        T63  = _mm_madd_epi16(T47, _mm_load_si128((__m128i*)tab_dct_8[5]));
        T60  = _mm_hadd_epi32(T60, T61);
        T61  = _mm_hadd_epi32(T62, T63);
        T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1);
        T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1);
        T70  = _mm_packs_epi32(T60, T61);
        _mm_store_si128((__m128i*)&tmp[2 * 16 + i], T70);

        T60  = _mm_madd_epi16(T44, _mm_load_si128((__m128i*)tab_dct_8[6]));
        T61  = _mm_madd_epi16(T45, _mm_load_si128((__m128i*)tab_dct_8[6]));
        T62  = _mm_madd_epi16(T46, _mm_load_si128((__m128i*)tab_dct_8[6]));
        T63  = _mm_madd_epi16(T47, _mm_load_si128((__m128i*)tab_dct_8[6]));
        T60  = _mm_hadd_epi32(T60, T61);
        T61  = _mm_hadd_epi32(T62, T63);
        T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1);
        T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1);
        T70  = _mm_packs_epi32(T60, T61);
        _mm_store_si128((__m128i*)&tmp[6 * 16 + i], T70);

        T60  = _mm_madd_epi16(T44, _mm_load_si128((__m128i*)tab_dct_8[7]));
        T61  = _mm_madd_epi16(T45, _mm_load_si128((__m128i*)tab_dct_8[7]));
        T62  = _mm_madd_epi16(T46, _mm_load_si128((__m128i*)tab_dct_8[7]));
        T63  = _mm_madd_epi16(T47, _mm_load_si128((__m128i*)tab_dct_8[7]));
        T60  = _mm_hadd_epi32(T60, T61);
        T61  = _mm_hadd_epi32(T62, T63);
        T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1);
        T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1);
        T70  = _mm_packs_epi32(T60, T61);
        _mm_store_si128((__m128i*)&tmp[10 * 16 + i], T70);

        T60  = _mm_madd_epi16(T44, _mm_load_si128((__m128i*)tab_dct_8[8]));
        T61  = _mm_madd_epi16(T45, _mm_load_si128((__m128i*)tab_dct_8[8]));
        T62  = _mm_madd_epi16(T46, _mm_load_si128((__m128i*)tab_dct_8[8]));
        T63  = _mm_madd_epi16(T47, _mm_load_si128((__m128i*)tab_dct_8[8]));
        T60  = _mm_hadd_epi32(T60, T61);
        T61  = _mm_hadd_epi32(T62, T63);
        T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1);
        T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1);
        T70  = _mm_packs_epi32(T60, T61);
        _mm_store_si128((__m128i*)&tmp[14 * 16 + i], T70);

#define MAKE_ODD(tab, dstPos) \
    T60  = _mm_madd_epi16(T20, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T61  = _mm_madd_epi16(T21, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T62  = _mm_madd_epi16(T22, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T63  = _mm_madd_epi16(T23, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T64  = _mm_madd_epi16(T24, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T65  = _mm_madd_epi16(T25, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T66  = _mm_madd_epi16(T26, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T67  = _mm_madd_epi16(T27, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T60  = _mm_hadd_epi32(T60, T61); \
    T61  = _mm_hadd_epi32(T62, T63); \
    T62  = _mm_hadd_epi32(T64, T65); \
    T63  = _mm_hadd_epi32(T66, T67); \
    T60  = _mm_hadd_epi32(T60, T61); \
    T61  = _mm_hadd_epi32(T62, T63); \
    T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_4), DCT16_SHIFT1); \
    T61  = _mm_srai_epi32(_mm_add_epi32(T61, c_4), DCT16_SHIFT1); \
    T70  = _mm_packs_epi32(T60, T61); \
    _mm_store_si128((__m128i*)&tmp[(dstPos) * 16 + i], T70);

        MAKE_ODD(0, 1);
        MAKE_ODD(1, 3);
        MAKE_ODD(2, 5);
        MAKE_ODD(3, 7);
        MAKE_ODD(4, 9);
        MAKE_ODD(5, 11);
        MAKE_ODD(6, 13);
        MAKE_ODD(7, 15);
#undef MAKE_ODD
    }

    // DCT2
    for (i = 0; i < 16; i += 4)
    {
        T00A = _mm_load_si128((__m128i*)&tmp[(i + 0) * 16 + 0]);    // [07 06 05 04 03 02 01 00]
        T00B = _mm_load_si128((__m128i*)&tmp[(i + 0) * 16 + 8]);    // [0F 0E 0D 0C 0B 0A 09 08]
        T01A = _mm_load_si128((__m128i*)&tmp[(i + 1) * 16 + 0]);    // [17 16 15 14 13 12 11 10]
        T01B = _mm_load_si128((__m128i*)&tmp[(i + 1) * 16 + 8]);    // [1F 1E 1D 1C 1B 1A 19 18]
        T02A = _mm_load_si128((__m128i*)&tmp[(i + 2) * 16 + 0]);    // [27 26 25 24 23 22 21 20]
        T02B = _mm_load_si128((__m128i*)&tmp[(i + 2) * 16 + 8]);    // [2F 2E 2D 2C 2B 2A 29 28]
        T03A = _mm_load_si128((__m128i*)&tmp[(i + 3) * 16 + 0]);    // [37 36 35 34 33 32 31 30]
        T03B = _mm_load_si128((__m128i*)&tmp[(i + 3) * 16 + 8]);    // [3F 3E 3D 3C 3B 3A 39 38]

        T00A = _mm_shuffle_epi8(T00A, _mm_load_si128((__m128i*)tab_dct_16_0[2]));
        T00B = _mm_shuffle_epi8(T00B, _mm_load_si128((__m128i*)tab_dct_16_0[3]));
        T01A = _mm_shuffle_epi8(T01A, _mm_load_si128((__m128i*)tab_dct_16_0[2]));
        T01B = _mm_shuffle_epi8(T01B, _mm_load_si128((__m128i*)tab_dct_16_0[3]));
        T02A = _mm_shuffle_epi8(T02A, _mm_load_si128((__m128i*)tab_dct_16_0[2]));
        T02B = _mm_shuffle_epi8(T02B, _mm_load_si128((__m128i*)tab_dct_16_0[3]));
        T03A = _mm_shuffle_epi8(T03A, _mm_load_si128((__m128i*)tab_dct_16_0[2]));
        T03B = _mm_shuffle_epi8(T03B, _mm_load_si128((__m128i*)tab_dct_16_0[3]));

        T10  = _mm_unpacklo_epi16(T00A, T00B);
        T11  = _mm_unpackhi_epi16(T00A, T00B);
        T12  = _mm_unpacklo_epi16(T01A, T01B);
        T13  = _mm_unpackhi_epi16(T01A, T01B);
        T14  = _mm_unpacklo_epi16(T02A, T02B);
        T15  = _mm_unpackhi_epi16(T02A, T02B);
        T16  = _mm_unpacklo_epi16(T03A, T03B);
        T17  = _mm_unpackhi_epi16(T03A, T03B);

        T20  = _mm_madd_epi16(T10, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T21  = _mm_madd_epi16(T11, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T22  = _mm_madd_epi16(T12, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T23  = _mm_madd_epi16(T13, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T24  = _mm_madd_epi16(T14, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T25  = _mm_madd_epi16(T15, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T26  = _mm_madd_epi16(T16, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T27  = _mm_madd_epi16(T17, _mm_load_si128((__m128i*)tab_dct_8[1]));

        T30  = _mm_add_epi32(T20, T21);
        T31  = _mm_add_epi32(T22, T23);
        T32  = _mm_add_epi32(T24, T25);
        T33  = _mm_add_epi32(T26, T27);

        T30  = _mm_hadd_epi32(T30, T31);
        T31  = _mm_hadd_epi32(T32, T33);

        T40  = _mm_hadd_epi32(T30, T31);
        T41  = _mm_hsub_epi32(T30, T31);
        T40  = _mm_srai_epi32(_mm_add_epi32(T40, c_512), DCT16_SHIFT2);
        T41  = _mm_srai_epi32(_mm_add_epi32(T41, c_512), DCT16_SHIFT2);
        T40  = _mm_packs_epi32(T40, T40);
        T41  = _mm_packs_epi32(T41, T41);
        _mm_storel_epi64((__m128i*)&dst[0 * 16 + i], T40);
        _mm_storel_epi64((__m128i*)&dst[8 * 16 + i], T41);

        T20  = _mm_madd_epi16(T10, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T21  = _mm_madd_epi16(T11, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T22  = _mm_madd_epi16(T12, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T23  = _mm_madd_epi16(T13, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T24  = _mm_madd_epi16(T14, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T25  = _mm_madd_epi16(T15, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T26  = _mm_madd_epi16(T16, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T27  = _mm_madd_epi16(T17, _mm_load_si128((__m128i*)tab_dct_16_1[8]));

        T30  = _mm_add_epi32(T20, T21);
        T31  = _mm_add_epi32(T22, T23);
        T32  = _mm_add_epi32(T24, T25);
        T33  = _mm_add_epi32(T26, T27);

        T30  = _mm_hadd_epi32(T30, T31);
        T31  = _mm_hadd_epi32(T32, T33);

        T40  = _mm_hadd_epi32(T30, T31);
        T40  = _mm_srai_epi32(_mm_add_epi32(T40, c_512), DCT16_SHIFT2);
        T40  = _mm_packs_epi32(T40, T40);
        _mm_storel_epi64((__m128i*)&dst[4 * 16 + i], T40);

        T20  = _mm_madd_epi16(T10, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T21  = _mm_madd_epi16(T11, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T22  = _mm_madd_epi16(T12, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T23  = _mm_madd_epi16(T13, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T24  = _mm_madd_epi16(T14, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T25  = _mm_madd_epi16(T15, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T26  = _mm_madd_epi16(T16, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T27  = _mm_madd_epi16(T17, _mm_load_si128((__m128i*)tab_dct_16_1[9]));

        T30  = _mm_add_epi32(T20, T21);
        T31  = _mm_add_epi32(T22, T23);
        T32  = _mm_add_epi32(T24, T25);
        T33  = _mm_add_epi32(T26, T27);

        T30  = _mm_hadd_epi32(T30, T31);
        T31  = _mm_hadd_epi32(T32, T33);

        T40  = _mm_hadd_epi32(T30, T31);
        T40  = _mm_srai_epi32(_mm_add_epi32(T40, c_512), DCT16_SHIFT2);
        T40  = _mm_packs_epi32(T40, T40);
        _mm_storel_epi64((__m128i*)&dst[12 * 16 + i], T40);

        T20  = _mm_madd_epi16(T10, _mm_load_si128((__m128i*)tab_dct_16_1[10]));
        T21  = _mm_madd_epi16(T11, _mm_load_si128((__m128i*)tab_dct_16_1[10]));
        T22  = _mm_madd_epi16(T12, _mm_load_si128((__m128i*)tab_dct_16_1[10]));
        T23  = _mm_madd_epi16(T13, _mm_load_si128((__m128i*)tab_dct_16_1[10]));
        T24  = _mm_madd_epi16(T14, _mm_load_si128((__m128i*)tab_dct_16_1[10]));
        T25  = _mm_madd_epi16(T15, _mm_load_si128((__m128i*)tab_dct_16_1[10]));
        T26  = _mm_madd_epi16(T16, _mm_load_si128((__m128i*)tab_dct_16_1[10]));
        T27  = _mm_madd_epi16(T17, _mm_load_si128((__m128i*)tab_dct_16_1[10]));

        T30  = _mm_sub_epi32(T20, T21);
        T31  = _mm_sub_epi32(T22, T23);
        T32  = _mm_sub_epi32(T24, T25);
        T33  = _mm_sub_epi32(T26, T27);

        T30  = _mm_hadd_epi32(T30, T31);
        T31  = _mm_hadd_epi32(T32, T33);

        T40  = _mm_hadd_epi32(T30, T31);
        T40  = _mm_srai_epi32(_mm_add_epi32(T40, c_512), DCT16_SHIFT2);
        T40  = _mm_packs_epi32(T40, T40);
        _mm_storel_epi64((__m128i*)&dst[2 * 16 + i], T40);

        T20  = _mm_madd_epi16(T10, _mm_load_si128((__m128i*)tab_dct_16_1[11]));
        T21  = _mm_madd_epi16(T11, _mm_load_si128((__m128i*)tab_dct_16_1[11]));
        T22  = _mm_madd_epi16(T12, _mm_load_si128((__m128i*)tab_dct_16_1[11]));
        T23  = _mm_madd_epi16(T13, _mm_load_si128((__m128i*)tab_dct_16_1[11]));
        T24  = _mm_madd_epi16(T14, _mm_load_si128((__m128i*)tab_dct_16_1[11]));
        T25  = _mm_madd_epi16(T15, _mm_load_si128((__m128i*)tab_dct_16_1[11]));
        T26  = _mm_madd_epi16(T16, _mm_load_si128((__m128i*)tab_dct_16_1[11]));
        T27  = _mm_madd_epi16(T17, _mm_load_si128((__m128i*)tab_dct_16_1[11]));

        T30  = _mm_sub_epi32(T20, T21);
        T31  = _mm_sub_epi32(T22, T23);
        T32  = _mm_sub_epi32(T24, T25);
        T33  = _mm_sub_epi32(T26, T27);

        T30  = _mm_hadd_epi32(T30, T31);
        T31  = _mm_hadd_epi32(T32, T33);

        T40  = _mm_hadd_epi32(T30, T31);
        T40  = _mm_srai_epi32(_mm_add_epi32(T40, c_512), DCT16_SHIFT2);
        T40  = _mm_packs_epi32(T40, T40);
        _mm_storel_epi64((__m128i*)&dst[6 * 16 + i], T40);

        T20  = _mm_madd_epi16(T10, _mm_load_si128((__m128i*)tab_dct_16_1[12]));
        T21  = _mm_madd_epi16(T11, _mm_load_si128((__m128i*)tab_dct_16_1[12]));
        T22  = _mm_madd_epi16(T12, _mm_load_si128((__m128i*)tab_dct_16_1[12]));
        T23  = _mm_madd_epi16(T13, _mm_load_si128((__m128i*)tab_dct_16_1[12]));
        T24  = _mm_madd_epi16(T14, _mm_load_si128((__m128i*)tab_dct_16_1[12]));
        T25  = _mm_madd_epi16(T15, _mm_load_si128((__m128i*)tab_dct_16_1[12]));
        T26  = _mm_madd_epi16(T16, _mm_load_si128((__m128i*)tab_dct_16_1[12]));
        T27  = _mm_madd_epi16(T17, _mm_load_si128((__m128i*)tab_dct_16_1[12]));

        T30  = _mm_sub_epi32(T20, T21);
        T31  = _mm_sub_epi32(T22, T23);
        T32  = _mm_sub_epi32(T24, T25);
        T33  = _mm_sub_epi32(T26, T27);

        T30  = _mm_hadd_epi32(T30, T31);
        T31  = _mm_hadd_epi32(T32, T33);

        T40  = _mm_hadd_epi32(T30, T31);
        T40  = _mm_srai_epi32(_mm_add_epi32(T40, c_512), DCT16_SHIFT2);
        T40  = _mm_packs_epi32(T40, T40);
        _mm_storel_epi64((__m128i*)&dst[10 * 16 + i], T40);

        T20  = _mm_madd_epi16(T10, _mm_load_si128((__m128i*)tab_dct_16_1[13]));
        T21  = _mm_madd_epi16(T11, _mm_load_si128((__m128i*)tab_dct_16_1[13]));
        T22  = _mm_madd_epi16(T12, _mm_load_si128((__m128i*)tab_dct_16_1[13]));
        T23  = _mm_madd_epi16(T13, _mm_load_si128((__m128i*)tab_dct_16_1[13]));
        T24  = _mm_madd_epi16(T14, _mm_load_si128((__m128i*)tab_dct_16_1[13]));
        T25  = _mm_madd_epi16(T15, _mm_load_si128((__m128i*)tab_dct_16_1[13]));
        T26  = _mm_madd_epi16(T16, _mm_load_si128((__m128i*)tab_dct_16_1[13]));
        T27  = _mm_madd_epi16(T17, _mm_load_si128((__m128i*)tab_dct_16_1[13]));

        T30  = _mm_sub_epi32(T20, T21);
        T31  = _mm_sub_epi32(T22, T23);
        T32  = _mm_sub_epi32(T24, T25);
        T33  = _mm_sub_epi32(T26, T27);

        T30  = _mm_hadd_epi32(T30, T31);
        T31  = _mm_hadd_epi32(T32, T33);

        T40  = _mm_hadd_epi32(T30, T31);
        T40  = _mm_srai_epi32(_mm_add_epi32(T40, c_512), DCT16_SHIFT2);
        T40  = _mm_packs_epi32(T40, T40);
        _mm_storel_epi64((__m128i*)&dst[14 * 16 + i], T40);

#define MAKE_ODD(tab, dstPos) \
    T20  = _mm_madd_epi16(T10, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)]));       /* [*O2_0 *O1_0 *O3_0 *O0_0] */ \
    T21  = _mm_madd_epi16(T11, _mm_load_si128((__m128i*)tab_dct_16_1[(tab) + 1]));   /* [*O5_0 *O6_0 *O4_0 *O7_0] */ \
    T22  = _mm_madd_epi16(T12, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T23  = _mm_madd_epi16(T13, _mm_load_si128((__m128i*)tab_dct_16_1[(tab) + 1])); \
    T24  = _mm_madd_epi16(T14, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T25  = _mm_madd_epi16(T15, _mm_load_si128((__m128i*)tab_dct_16_1[(tab) + 1])); \
    T26  = _mm_madd_epi16(T16, _mm_load_si128((__m128i*)tab_dct_16_1[(tab)])); \
    T27  = _mm_madd_epi16(T17, _mm_load_si128((__m128i*)tab_dct_16_1[(tab) + 1])); \
        \
    T30  = _mm_add_epi32(T20, T21); \
    T31  = _mm_add_epi32(T22, T23); \
    T32  = _mm_add_epi32(T24, T25); \
    T33  = _mm_add_epi32(T26, T27); \
        \
    T30  = _mm_hadd_epi32(T30, T31); \
    T31  = _mm_hadd_epi32(T32, T33); \
        \
    T40  = _mm_hadd_epi32(T30, T31); \
    T40  = _mm_srai_epi32(_mm_add_epi32(T40, c_512), DCT16_SHIFT2); \
    T40  = _mm_packs_epi32(T40, T40); \
    _mm_storel_epi64((__m128i*)&dst[(dstPos) * 16 + i], T40);

        MAKE_ODD(14,  1);
        MAKE_ODD(16,  3);
        MAKE_ODD(18,  5);
        MAKE_ODD(20,  7);
        MAKE_ODD(22,  9);
        MAKE_ODD(24, 11);
        MAKE_ODD(26, 13);
        MAKE_ODD(28, 15);
#undef MAKE_ODD
    }
}

ALIGN_VAR_32(static const int16_t, tab_dct_32_0[][8]) =
{
    { 0x0F0E, 0x0100, 0x0908, 0x0706, 0x0D0C, 0x0302, 0x0B0A, 0x0504 },  // 0
};

ALIGN_VAR_32(static const int16_t, tab_dct_32_1[][8]) =
{
    { 89, -89, 18, -18, 75, -75, 50, -50 },          //  0
    { 75, -75, -50, 50, -18, 18, -89, 89 },          //  1
    { 50, -50, 75, -75, -89, 89, 18, -18 },          //  2
    { 18, -18, -89, 89, -50, 50, 75, -75 },          //  3

#define MAKE_COEF8(a0, a1, a2, a3, a4, a5, a6, a7) \
    { (a0), (a7), (a3), (a4), (a1), (a6), (a2), (a5) \
    }, \

    MAKE_COEF8(90, 87, 80, 70, 57, 43, 25,  9)   //  4
    MAKE_COEF8(87, 57,  9, -43, -80, -90, -70, -25)   //  5
    MAKE_COEF8(80,  9, -70, -87, -25, 57, 90, 43)   //  6
    MAKE_COEF8(70, -43, -87,  9, 90, 25, -80, -57)   //  7
    MAKE_COEF8(57, -80, -25, 90, -9, -87, 43, 70)   //  8
    MAKE_COEF8(43, -90, 57, 25, -87, 70,  9, -80)   //  9
    MAKE_COEF8(25, -70, 90, -80, 43,  9, -57, 87)   // 10
    MAKE_COEF8(9, -25, 43, -57, 70, -80, 87, -90)   // 11
#undef MAKE_COEF8

#define MAKE_COEF16(a00, a01, a02, a03, a04, a05, a06, a07, a08, a09, a10, a11, a12, a13, a14, a15) \
    { (a00), (a07), (a03), (a04), (a01), (a06), (a02), (a05) }, \
    { (a15), (a08), (a12), (a11), (a14), (a09), (a13), (a10) },

    MAKE_COEF16(90, 90, 88, 85, 82, 78, 73, 67, 61, 54, 46, 38, 31, 22, 13,  4)    // 12
    MAKE_COEF16(90, 82, 67, 46, 22, -4, -31, -54, -73, -85, -90, -88, -78, -61, -38, -13)    // 14
    MAKE_COEF16(88, 67, 31, -13, -54, -82, -90, -78, -46, -4, 38, 73, 90, 85, 61, 22)    // 16
    MAKE_COEF16(85, 46, -13, -67, -90, -73, -22, 38, 82, 88, 54, -4, -61, -90, -78, -31)    // 18
    MAKE_COEF16(82, 22, -54, -90, -61, 13, 78, 85, 31, -46, -90, -67,  4, 73, 88, 38)    // 20
    MAKE_COEF16(78, -4, -82, -73, 13, 85, 67, -22, -88, -61, 31, 90, 54, -38, -90, -46)    // 22
    MAKE_COEF16(73, -31, -90, -22, 78, 67, -38, -90, -13, 82, 61, -46, -88, -4, 85, 54)    // 24
    MAKE_COEF16(67, -54, -78, 38, 85, -22, -90,  4, 90, 13, -88, -31, 82, 46, -73, -61)    // 26
    MAKE_COEF16(61, -73, -46, 82, 31, -88, -13, 90, -4, -90, 22, 85, -38, -78, 54, 67)    // 28
    MAKE_COEF16(54, -85, -4, 88, -46, -61, 82, 13, -90, 38, 67, -78, -22, 90, -31, -73)    // 30
    MAKE_COEF16(46, -90, 38, 54, -90, 31, 61, -88, 22, 67, -85, 13, 73, -82,  4, 78)    // 32
    MAKE_COEF16(38, -88, 73, -4, -67, 90, -46, -31, 85, -78, 13, 61, -90, 54, 22, -82)    // 34
    MAKE_COEF16(31, -78, 90, -61,  4, 54, -88, 82, -38, -22, 73, -90, 67, -13, -46, 85)    // 36
    MAKE_COEF16(22, -61, 85, -90, 73, -38, -4, 46, -78, 90, -82, 54, -13, -31, 67, -88)    // 38
    MAKE_COEF16(13, -38, 61, -78, 88, -90, 85, -73, 54, -31,  4, 22, -46, 67, -82, 90)    // 40
    MAKE_COEF16(4, -13, 22, -31, 38, -46, 54, -61, 67, -73, 78, -82, 85, -88, 90, -90)    // 42
#undef MAKE_COEF16

    {
        64, 64, 64, 64, 64, 64, 64, 64
    },                                  // 44

    { 64, 64, -64, -64, -64, -64, 64, 64 },  // 45

    { 83, 83, 36, 36, -36, -36, -83, -83 },  // 46
    { -83, -83, -36, -36, 36, 36, 83, 83 },  // 47

    { 36, 36, -83, -83, 83, 83, -36, -36 },  // 48
    { -36, -36, 83, 83, -83, -83, 36, 36 },  // 49

#define MAKE_COEF16(a00, a01, a02, a03, a04, a05, a06, a07, a08, a09, a10, a11, a12, a13, a14, a15) \
    { (a00), (a00), (a01), (a01), (a02), (a02), (a03), (a03) }, \
    { (a04), (a04), (a05), (a05), (a06), (a06), (a07), (a07) }, \
    { (a08), (a08), (a09), (a09), (a10), (a10), (a11), (a11) }, \
    { (a12), (a12), (a13), (a13), (a14), (a14), (a15), (a15) },

    MAKE_COEF16(89, 75, 50, 18, -18, -50, -75, -89, -89, -75, -50, -18, 18, 50, 75, 89) // 50
    MAKE_COEF16(75, -18, -89, -50, 50, 89, 18, -75, -75, 18, 89, 50, -50, -89, -18, 75) // 54

    // TODO: convert below table here
#undef MAKE_COEF16

    {
        50, 50, -89, -89, 18, 18, 75, 75
    },                                  // 58
    { -75, -75, -18, -18, 89, 89, -50, -50 },  // 59
    { -50, -50, 89, 89, -18, -18, -75, -75 },  // 60
    { 75, 75, 18, 18, -89, -89, 50, 50 },  // 61

    { 18, 18, -50, -50, 75, 75, -89, -89 },  // 62
    { 89, 89, -75, -75, 50, 50, -18, -18 },  // 63
    { -18, -18, 50, 50, -75, -75, 89, 89 },  // 64
    { -89, -89, 75, 75, -50, -50, 18, 18 },  // 65

    { 90, 90, 87, 87, 80, 80, 70, 70 },  // 66
    { 57, 57, 43, 43, 25, 25,  9,  9 },  // 67
    { -9, -9, -25, -25, -43, -43, -57, -57 },  // 68
    { -70, -70, -80, -80, -87, -87, -90, -90 },  // 69

    { 87, 87, 57, 57,  9,  9, -43, -43 },  // 70
    { -80, -80, -90, -90, -70, -70, -25, -25 },  // 71
    { 25, 25, 70, 70, 90, 90, 80, 80 },  // 72
    { 43, 43, -9, -9, -57, -57, -87, -87 },  // 73

    { 80, 80,  9,  9, -70, -70, -87, -87 },  // 74
    { -25, -25, 57, 57, 90, 90, 43, 43 },  // 75
    { -43, -43, -90, -90, -57, -57, 25, 25 },  // 76
    { 87, 87, 70, 70, -9, -9, -80, -80 },  // 77

    { 70, 70, -43, -43, -87, -87,  9,  9 },  // 78
    { 90, 90, 25, 25, -80, -80, -57, -57 },  // 79
    { 57, 57, 80, 80, -25, -25, -90, -90 },  // 80
    { -9, -9, 87, 87, 43, 43, -70, -70 },  // 81

    { 57, 57, -80, -80, -25, -25, 90, 90 },  // 82
    { -9, -9, -87, -87, 43, 43, 70, 70 },  // 83
    { -70, -70, -43, -43, 87, 87,  9,  9 },  // 84
    { -90, -90, 25, 25, 80, 80, -57, -57 },  // 85

    { 43, 43, -90, -90, 57, 57, 25, 25 },  // 86
    { -87, -87, 70, 70,  9,  9, -80, -80 },  // 87
    { 80, 80, -9, -9, -70, -70, 87, 87 },  // 88
    { -25, -25, -57, -57, 90, 90, -43, -43 },  // 89

    { 25, 25, -70, -70, 90, 90, -80, -80 },  // 90
    { 43, 43,  9,  9, -57, -57, 87, 87 },  // 91
    { -87, -87, 57, 57, -9, -9, -43, -43 },  // 92
    { 80, 80, -90, -90, 70, 70, -25, -25 },  // 93

    {  9,  9, -25, -25, 43, 43, -57, -57 },  // 94
    { 70, 70, -80, -80, 87, 87, -90, -90 },  // 95
    { 90, 90, -87, -87, 80, 80, -70, -70 },  // 96
    { 57, 57, -43, -43, 25, 25, -9, -9 },  // 97

#define MAKE_COEF16(a00, a01, a02, a03, a04, a05, a06, a07, a08, a09, a10, a11, a12, a13, a14, a15) \
    { (a00), -(a00), (a01), -(a01), (a02), -(a02), (a03), -(a03) }, \
    { (a04), -(a04), (a05), -(a05), (a06), -(a06), (a07), -(a07) }, \
    { (a08), -(a08), (a09), -(a09), (a10), -(a10), (a11), -(a11) }, \
    { (a12), -(a12), (a13), -(a13), (a14), -(a14), (a15), -(a15) },

    MAKE_COEF16(90, 90, 88, 85, 82, 78, 73, 67, 61, 54, 46, 38, 31, 22, 13, 4)    // 98
    MAKE_COEF16(90, 82, 67, 46, 22, -4, -31, -54, -73, -85, -90, -88, -78, -61, -38, -13)     //102
    MAKE_COEF16(88, 67, 31, -13, -54, -82, -90, -78, -46, -4, 38, 73, 90, 85, 61, 22)     //106
    MAKE_COEF16(85, 46, -13, -67, -90, -73, -22, 38, +82, 88, 54, -4, -61, -90, -78, -31)     //110
    MAKE_COEF16(82, 22, -54, -90, -61, 13, 78, 85, +31, -46, -90, -67,  4, 73, 88, 38)     //114
    MAKE_COEF16(78, -4, -82, -73, 13, 85, 67, -22, -88, -61, 31, 90, 54, -38, -90, -46)     //118
    MAKE_COEF16(73, -31, -90, -22, 78, 67, -38, -90, -13, 82, 61, -46, -88, -4, 85, 54)     //122
    MAKE_COEF16(67, -54, -78, 38, 85, -22, -90,  4, +90, 13, -88, -31, 82, 46, -73, -61)     //126
    MAKE_COEF16(61, -73, -46, 82, 31, -88, -13, 90, -4, -90, 22, 85, -38, -78, 54, 67)     //130
    MAKE_COEF16(54, -85, -4, 88, -46, -61, 82, 13, -90, 38, 67, -78, -22, 90, -31, -73)     //134
    MAKE_COEF16(46, -90, 38, 54, -90, 31, 61, -88, +22, 67, -85, 13, 73, -82,  4, 78)     //138
    MAKE_COEF16(38, -88, 73, -4, -67, 90, -46, -31, +85, -78, 13, 61, -90, 54, 22, -82)     //142
    MAKE_COEF16(31, -78, 90, -61,  4, 54, -88, 82, -38, -22, 73, -90, 67, -13, -46, 85)     //146
    MAKE_COEF16(22, -61, 85, -90, 73, -38, -4, 46, -78, 90, -82, 54, -13, -31, 67, -88)     //150
    MAKE_COEF16(13, -38, 61, -78, 88, -90, 85, -73, +54, -31,  4, 22, -46, 67, -82, 90)     //154
    MAKE_COEF16(4, -13, 22, -31, 38, -46, 54, -61, +67, -73, 78, -82, 85, -88, 90, -90)     //158

#undef MAKE_COEF16
};

static void dct32(const int16_t *src, int16_t *dst, intptr_t stride)
{
    // Const
    __m128i c_8     = _mm_set1_epi32(DCT32_ADD1);
    __m128i c_1024  = _mm_set1_epi32(DCT32_ADD2);

    int i;

    __m128i T00A, T01A, T02A, T03A, T04A, T05A, T06A, T07A;
    __m128i T00B, T01B, T02B, T03B, T04B, T05B, T06B, T07B;
    __m128i T00C, T01C, T02C, T03C, T04C, T05C, T06C, T07C;
    __m128i T00D, T01D, T02D, T03D, T04D, T05D, T06D, T07D;
    __m128i T10A, T11A, T12A, T13A, T14A, T15A, T16A, T17A;
    __m128i T10B, T11B, T12B, T13B, T14B, T15B, T16B, T17B;
    __m128i T20, T21, T22, T23, T24, T25, T26, T27;
    __m128i T30, T31, T32, T33, T34, T35, T36, T37;
    __m128i T40, T41, T42, T43, T44, T45, T46, T47;
    __m128i T50, T51, T52, T53;
    __m128i T60, T61, T62, T63, T64, T65, T66, T67;
    __m128i im[32][4];

    // DCT1
    for (i = 0; i < 32 / 8; i++)
    {
        T00A = _mm_load_si128((__m128i*)&src[(i * 8 + 0) * stride + 0]);    // [07 06 05 04 03 02 01 00]
        T00B = _mm_load_si128((__m128i*)&src[(i * 8 + 0) * stride + 8]);    // [15 14 13 12 11 10 09 08]
        T00C = _mm_load_si128((__m128i*)&src[(i * 8 + 0) * stride + 16]);    // [23 22 21 20 19 18 17 16]
        T00D = _mm_load_si128((__m128i*)&src[(i * 8 + 0) * stride + 24]);    // [31 30 29 28 27 26 25 24]
        T01A = _mm_load_si128((__m128i*)&src[(i * 8 + 1) * stride + 0]);
        T01B = _mm_load_si128((__m128i*)&src[(i * 8 + 1) * stride + 8]);
        T01C = _mm_load_si128((__m128i*)&src[(i * 8 + 1) * stride + 16]);
        T01D = _mm_load_si128((__m128i*)&src[(i * 8 + 1) * stride + 24]);
        T02A = _mm_load_si128((__m128i*)&src[(i * 8 + 2) * stride + 0]);
        T02B = _mm_load_si128((__m128i*)&src[(i * 8 + 2) * stride + 8]);
        T02C = _mm_load_si128((__m128i*)&src[(i * 8 + 2) * stride + 16]);
        T02D = _mm_load_si128((__m128i*)&src[(i * 8 + 2) * stride + 24]);
        T03A = _mm_load_si128((__m128i*)&src[(i * 8 + 3) * stride + 0]);
        T03B = _mm_load_si128((__m128i*)&src[(i * 8 + 3) * stride + 8]);
        T03C = _mm_load_si128((__m128i*)&src[(i * 8 + 3) * stride + 16]);
        T03D = _mm_load_si128((__m128i*)&src[(i * 8 + 3) * stride + 24]);
        T04A = _mm_load_si128((__m128i*)&src[(i * 8 + 4) * stride + 0]);
        T04B = _mm_load_si128((__m128i*)&src[(i * 8 + 4) * stride + 8]);
        T04C = _mm_load_si128((__m128i*)&src[(i * 8 + 4) * stride + 16]);
        T04D = _mm_load_si128((__m128i*)&src[(i * 8 + 4) * stride + 24]);
        T05A = _mm_load_si128((__m128i*)&src[(i * 8 + 5) * stride + 0]);
        T05B = _mm_load_si128((__m128i*)&src[(i * 8 + 5) * stride + 8]);
        T05C = _mm_load_si128((__m128i*)&src[(i * 8 + 5) * stride + 16]);
        T05D = _mm_load_si128((__m128i*)&src[(i * 8 + 5) * stride + 24]);
        T06A = _mm_load_si128((__m128i*)&src[(i * 8 + 6) * stride + 0]);
        T06B = _mm_load_si128((__m128i*)&src[(i * 8 + 6) * stride + 8]);
        T06C = _mm_load_si128((__m128i*)&src[(i * 8 + 6) * stride + 16]);
        T06D = _mm_load_si128((__m128i*)&src[(i * 8 + 6) * stride + 24]);
        T07A = _mm_load_si128((__m128i*)&src[(i * 8 + 7) * stride + 0]);
        T07B = _mm_load_si128((__m128i*)&src[(i * 8 + 7) * stride + 8]);
        T07C = _mm_load_si128((__m128i*)&src[(i * 8 + 7) * stride + 16]);
        T07D = _mm_load_si128((__m128i*)&src[(i * 8 + 7) * stride + 24]);

        T00A = _mm_shuffle_epi8(T00A, _mm_load_si128((__m128i*)tab_dct_16_0[1]));    // [05 02 06 01 04 03 07 00]
        T00B = _mm_shuffle_epi8(T00B, _mm_load_si128((__m128i*)tab_dct_32_0[0]));    // [10 13 09 14 11 12 08 15]
        T00C = _mm_shuffle_epi8(T00C, _mm_load_si128((__m128i*)tab_dct_16_0[1]));    // [21 18 22 17 20 19 23 16]
        T00D = _mm_shuffle_epi8(T00D, _mm_load_si128((__m128i*)tab_dct_32_0[0]));    // [26 29 25 30 27 28 24 31]
        T01A = _mm_shuffle_epi8(T01A, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T01B = _mm_shuffle_epi8(T01B, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T01C = _mm_shuffle_epi8(T01C, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T01D = _mm_shuffle_epi8(T01D, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T02A = _mm_shuffle_epi8(T02A, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T02B = _mm_shuffle_epi8(T02B, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T02C = _mm_shuffle_epi8(T02C, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T02D = _mm_shuffle_epi8(T02D, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T03A = _mm_shuffle_epi8(T03A, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T03B = _mm_shuffle_epi8(T03B, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T03C = _mm_shuffle_epi8(T03C, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T03D = _mm_shuffle_epi8(T03D, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T04A = _mm_shuffle_epi8(T04A, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T04B = _mm_shuffle_epi8(T04B, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T04C = _mm_shuffle_epi8(T04C, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T04D = _mm_shuffle_epi8(T04D, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T05A = _mm_shuffle_epi8(T05A, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T05B = _mm_shuffle_epi8(T05B, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T05C = _mm_shuffle_epi8(T05C, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T05D = _mm_shuffle_epi8(T05D, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T06A = _mm_shuffle_epi8(T06A, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T06B = _mm_shuffle_epi8(T06B, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T06C = _mm_shuffle_epi8(T06C, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T06D = _mm_shuffle_epi8(T06D, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T07A = _mm_shuffle_epi8(T07A, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T07B = _mm_shuffle_epi8(T07B, _mm_load_si128((__m128i*)tab_dct_32_0[0]));
        T07C = _mm_shuffle_epi8(T07C, _mm_load_si128((__m128i*)tab_dct_16_0[1]));
        T07D = _mm_shuffle_epi8(T07D, _mm_load_si128((__m128i*)tab_dct_32_0[0]));

        T10A = _mm_add_epi16(T00A, T00D);   // [E05 E02 E06 E01 E04 E03 E07 E00]
        T10B = _mm_add_epi16(T00B, T00C);   // [E10 E13 E09 E14 E11 E12 E08 E15]
        T11A = _mm_add_epi16(T01A, T01D);
        T11B = _mm_add_epi16(T01B, T01C);
        T12A = _mm_add_epi16(T02A, T02D);
        T12B = _mm_add_epi16(T02B, T02C);
        T13A = _mm_add_epi16(T03A, T03D);
        T13B = _mm_add_epi16(T03B, T03C);
        T14A = _mm_add_epi16(T04A, T04D);
        T14B = _mm_add_epi16(T04B, T04C);
        T15A = _mm_add_epi16(T05A, T05D);
        T15B = _mm_add_epi16(T05B, T05C);
        T16A = _mm_add_epi16(T06A, T06D);
        T16B = _mm_add_epi16(T06B, T06C);
        T17A = _mm_add_epi16(T07A, T07D);
        T17B = _mm_add_epi16(T07B, T07C);

        T00A = _mm_sub_epi16(T00A, T00D);   // [O05 O02 O06 O01 O04 O03 O07 O00]
        T00B = _mm_sub_epi16(T00B, T00C);   // [O10 O13 O09 O14 O11 O12 O08 O15]
        T01A = _mm_sub_epi16(T01A, T01D);
        T01B = _mm_sub_epi16(T01B, T01C);
        T02A = _mm_sub_epi16(T02A, T02D);
        T02B = _mm_sub_epi16(T02B, T02C);
        T03A = _mm_sub_epi16(T03A, T03D);
        T03B = _mm_sub_epi16(T03B, T03C);
        T04A = _mm_sub_epi16(T04A, T04D);
        T04B = _mm_sub_epi16(T04B, T04C);
        T05A = _mm_sub_epi16(T05A, T05D);
        T05B = _mm_sub_epi16(T05B, T05C);
        T06A = _mm_sub_epi16(T06A, T06D);
        T06B = _mm_sub_epi16(T06B, T06C);
        T07A = _mm_sub_epi16(T07A, T07D);
        T07B = _mm_sub_epi16(T07B, T07C);

        T20  = _mm_add_epi16(T10A, T10B);   // [EE5 EE2 EE6 EE1 EE4 EE3 EE7 EE0]
        T21  = _mm_add_epi16(T11A, T11B);
        T22  = _mm_add_epi16(T12A, T12B);
        T23  = _mm_add_epi16(T13A, T13B);
        T24  = _mm_add_epi16(T14A, T14B);
        T25  = _mm_add_epi16(T15A, T15B);
        T26  = _mm_add_epi16(T16A, T16B);
        T27  = _mm_add_epi16(T17A, T17B);

        T30  = _mm_madd_epi16(T20, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T31  = _mm_madd_epi16(T21, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T32  = _mm_madd_epi16(T22, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T33  = _mm_madd_epi16(T23, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T34  = _mm_madd_epi16(T24, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T35  = _mm_madd_epi16(T25, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T36  = _mm_madd_epi16(T26, _mm_load_si128((__m128i*)tab_dct_8[1]));
        T37  = _mm_madd_epi16(T27, _mm_load_si128((__m128i*)tab_dct_8[1]));

        T40  = _mm_hadd_epi32(T30, T31);
        T41  = _mm_hadd_epi32(T32, T33);
        T42  = _mm_hadd_epi32(T34, T35);
        T43  = _mm_hadd_epi32(T36, T37);

        T50  = _mm_hadd_epi32(T40, T41);
        T51  = _mm_hadd_epi32(T42, T43);
        T50  = _mm_srai_epi32(_mm_add_epi32(T50, c_8), DCT32_SHIFT1);
        T51  = _mm_srai_epi32(_mm_add_epi32(T51, c_8), DCT32_SHIFT1);
        T60  = _mm_packs_epi32(T50, T51);
        im[0][i] = T60;

        T50  = _mm_hsub_epi32(T40, T41);
        T51  = _mm_hsub_epi32(T42, T43);
        T50  = _mm_srai_epi32(_mm_add_epi32(T50, c_8), DCT32_SHIFT1);
        T51  = _mm_srai_epi32(_mm_add_epi32(T51, c_8), DCT32_SHIFT1);
        T60  = _mm_packs_epi32(T50, T51);
        im[16][i] = T60;

        T30  = _mm_madd_epi16(T20, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T31  = _mm_madd_epi16(T21, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T32  = _mm_madd_epi16(T22, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T33  = _mm_madd_epi16(T23, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T34  = _mm_madd_epi16(T24, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T35  = _mm_madd_epi16(T25, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T36  = _mm_madd_epi16(T26, _mm_load_si128((__m128i*)tab_dct_16_1[8]));
        T37  = _mm_madd_epi16(T27, _mm_load_si128((__m128i*)tab_dct_16_1[8]));

        T40  = _mm_hadd_epi32(T30, T31);
        T41  = _mm_hadd_epi32(T32, T33);
        T42  = _mm_hadd_epi32(T34, T35);
        T43  = _mm_hadd_epi32(T36, T37);

        T50  = _mm_hadd_epi32(T40, T41);
        T51  = _mm_hadd_epi32(T42, T43);
        T50  = _mm_srai_epi32(_mm_add_epi32(T50, c_8), DCT32_SHIFT1);
        T51  = _mm_srai_epi32(_mm_add_epi32(T51, c_8), DCT32_SHIFT1);
        T60  = _mm_packs_epi32(T50, T51);
        im[8][i] = T60;

        T30  = _mm_madd_epi16(T20, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T31  = _mm_madd_epi16(T21, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T32  = _mm_madd_epi16(T22, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T33  = _mm_madd_epi16(T23, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T34  = _mm_madd_epi16(T24, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T35  = _mm_madd_epi16(T25, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T36  = _mm_madd_epi16(T26, _mm_load_si128((__m128i*)tab_dct_16_1[9]));
        T37  = _mm_madd_epi16(T27, _mm_load_si128((__m128i*)tab_dct_16_1[9]));

        T40  = _mm_hadd_epi32(T30, T31);
        T41  = _mm_hadd_epi32(T32, T33);
        T42  = _mm_hadd_epi32(T34, T35);
        T43  = _mm_hadd_epi32(T36, T37);

        T50  = _mm_hadd_epi32(T40, T41);
        T51  = _mm_hadd_epi32(T42, T43);
        T50  = _mm_srai_epi32(_mm_add_epi32(T50, c_8), DCT32_SHIFT1);
        T51  = _mm_srai_epi32(_mm_add_epi32(T51, c_8), DCT32_SHIFT1);
        T60  = _mm_packs_epi32(T50, T51);
        im[24][i] = T60;

#define MAKE_ODD(tab, dstPos) \
    T30  = _mm_madd_epi16(T20, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T31  = _mm_madd_epi16(T21, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T32  = _mm_madd_epi16(T22, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T33  = _mm_madd_epi16(T23, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T34  = _mm_madd_epi16(T24, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T35  = _mm_madd_epi16(T25, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T36  = _mm_madd_epi16(T26, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T37  = _mm_madd_epi16(T27, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
        \
    T40  = _mm_hadd_epi32(T30, T31); \
    T41  = _mm_hadd_epi32(T32, T33); \
    T42  = _mm_hadd_epi32(T34, T35); \
    T43  = _mm_hadd_epi32(T36, T37); \
        \
    T50  = _mm_hadd_epi32(T40, T41); \
    T51  = _mm_hadd_epi32(T42, T43); \
    T50  = _mm_srai_epi32(_mm_add_epi32(T50, c_8), DCT32_SHIFT1); \
    T51  = _mm_srai_epi32(_mm_add_epi32(T51, c_8), DCT32_SHIFT1); \
    T60  = _mm_packs_epi32(T50, T51); \
    im[(dstPos)][i] = T60;

        MAKE_ODD(0, 4);
        MAKE_ODD(1, 12);
        MAKE_ODD(2, 20);
        MAKE_ODD(3, 28);

        T20  = _mm_sub_epi16(T10A, T10B);   // [EO5 EO2 EO6 EO1 EO4 EO3 EO7 EO0]
        T21  = _mm_sub_epi16(T11A, T11B);
        T22  = _mm_sub_epi16(T12A, T12B);
        T23  = _mm_sub_epi16(T13A, T13B);
        T24  = _mm_sub_epi16(T14A, T14B);
        T25  = _mm_sub_epi16(T15A, T15B);
        T26  = _mm_sub_epi16(T16A, T16B);
        T27  = _mm_sub_epi16(T17A, T17B);

        MAKE_ODD(4, 2);
        MAKE_ODD(5, 6);
        MAKE_ODD(6, 10);
        MAKE_ODD(7, 14);
        MAKE_ODD(8, 18);
        MAKE_ODD(9, 22);
        MAKE_ODD(10, 26);
        MAKE_ODD(11, 30);
#undef MAKE_ODD

#define MAKE_ODD(tab, dstPos) \
    T20  = _mm_madd_epi16(T00A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T21  = _mm_madd_epi16(T00B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab) + 1])); \
    T22  = _mm_madd_epi16(T01A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T23  = _mm_madd_epi16(T01B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab) + 1])); \
    T24  = _mm_madd_epi16(T02A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T25  = _mm_madd_epi16(T02B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab) + 1])); \
    T26  = _mm_madd_epi16(T03A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T27  = _mm_madd_epi16(T03B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab) + 1])); \
    T30  = _mm_madd_epi16(T04A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T31  = _mm_madd_epi16(T04B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab) + 1])); \
    T32  = _mm_madd_epi16(T05A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T33  = _mm_madd_epi16(T05B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab) + 1])); \
    T34  = _mm_madd_epi16(T06A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T35  = _mm_madd_epi16(T06B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab) + 1])); \
    T36  = _mm_madd_epi16(T07A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab)])); \
    T37  = _mm_madd_epi16(T07B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab) + 1])); \
        \
    T40  = _mm_hadd_epi32(T20, T21); \
    T41  = _mm_hadd_epi32(T22, T23); \
    T42  = _mm_hadd_epi32(T24, T25); \
    T43  = _mm_hadd_epi32(T26, T27); \
    T44  = _mm_hadd_epi32(T30, T31); \
    T45  = _mm_hadd_epi32(T32, T33); \
    T46  = _mm_hadd_epi32(T34, T35); \
    T47  = _mm_hadd_epi32(T36, T37); \
        \
    T50  = _mm_hadd_epi32(T40, T41); \
    T51  = _mm_hadd_epi32(T42, T43); \
    T52  = _mm_hadd_epi32(T44, T45); \
    T53  = _mm_hadd_epi32(T46, T47); \
        \
    T50  = _mm_hadd_epi32(T50, T51); \
    T51  = _mm_hadd_epi32(T52, T53); \
    T50  = _mm_srai_epi32(_mm_add_epi32(T50, c_8), DCT32_SHIFT1); \
    T51  = _mm_srai_epi32(_mm_add_epi32(T51, c_8), DCT32_SHIFT1); \
    T60  = _mm_packs_epi32(T50, T51); \
    im[(dstPos)][i] = T60;

        MAKE_ODD(12,  1);
        MAKE_ODD(14,  3);
        MAKE_ODD(16,  5);
        MAKE_ODD(18,  7);
        MAKE_ODD(20,  9);
        MAKE_ODD(22, 11);
        MAKE_ODD(24, 13);
        MAKE_ODD(26, 15);
        MAKE_ODD(28, 17);
        MAKE_ODD(30, 19);
        MAKE_ODD(32, 21);
        MAKE_ODD(34, 23);
        MAKE_ODD(36, 25);
        MAKE_ODD(38, 27);
        MAKE_ODD(40, 29);
        MAKE_ODD(42, 31);

#undef MAKE_ODD
    }

    // DCT2
    for (i = 0; i < 32 / 4; i++)
    {
        // OPT_ME: to avoid register spill, I use matrix multiply, have other way?
        T00A = im[i * 4 + 0][0];    // [07 06 05 04 03 02 01 00]
        T00B = im[i * 4 + 0][1];    // [15 14 13 12 11 10 09 08]
        T00C = im[i * 4 + 0][2];    // [23 22 21 20 19 18 17 16]
        T00D = im[i * 4 + 0][3];    // [31 30 29 28 27 26 25 24]
        T01A = im[i * 4 + 1][0];
        T01B = im[i * 4 + 1][1];
        T01C = im[i * 4 + 1][2];
        T01D = im[i * 4 + 1][3];
        T02A = im[i * 4 + 2][0];
        T02B = im[i * 4 + 2][1];
        T02C = im[i * 4 + 2][2];
        T02D = im[i * 4 + 2][3];
        T03A = im[i * 4 + 3][0];
        T03B = im[i * 4 + 3][1];
        T03C = im[i * 4 + 3][2];
        T03D = im[i * 4 + 3][3];

        T00C = _mm_shuffle_epi8(T00C, _mm_load_si128((__m128i*)tab_dct_16_0[0]));    // [16 17 18 19 20 21 22 23]
        T00D = _mm_shuffle_epi8(T00D, _mm_load_si128((__m128i*)tab_dct_16_0[0]));    // [24 25 26 27 28 29 30 31]
        T01C = _mm_shuffle_epi8(T01C, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T01D = _mm_shuffle_epi8(T01D, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T02C = _mm_shuffle_epi8(T02C, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T02D = _mm_shuffle_epi8(T02D, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T03C = _mm_shuffle_epi8(T03C, _mm_load_si128((__m128i*)tab_dct_16_0[0]));
        T03D = _mm_shuffle_epi8(T03D, _mm_load_si128((__m128i*)tab_dct_16_0[0]));

        T10A = _mm_unpacklo_epi16(T00A, T00D);  // [28 03 29 02 30 01 31 00]
        T10B = _mm_unpackhi_epi16(T00A, T00D);  // [24 07 25 06 26 05 27 04]
        T00A = _mm_unpacklo_epi16(T00B, T00C);  // [20 11 21 10 22 09 23 08]
        T00B = _mm_unpackhi_epi16(T00B, T00C);  // [16 15 17 14 18 13 19 12]
        T11A = _mm_unpacklo_epi16(T01A, T01D);
        T11B = _mm_unpackhi_epi16(T01A, T01D);
        T01A = _mm_unpacklo_epi16(T01B, T01C);
        T01B = _mm_unpackhi_epi16(T01B, T01C);
        T12A = _mm_unpacklo_epi16(T02A, T02D);
        T12B = _mm_unpackhi_epi16(T02A, T02D);
        T02A = _mm_unpacklo_epi16(T02B, T02C);
        T02B = _mm_unpackhi_epi16(T02B, T02C);
        T13A = _mm_unpacklo_epi16(T03A, T03D);
        T13B = _mm_unpackhi_epi16(T03A, T03D);
        T03A = _mm_unpacklo_epi16(T03B, T03C);
        T03B = _mm_unpackhi_epi16(T03B, T03C);

#define MAKE_ODD(tab0, tab1, tab2, tab3, dstPos) \
    T20  = _mm_madd_epi16(T10A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab0)])); \
    T21  = _mm_madd_epi16(T10B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab1)])); \
    T22  = _mm_madd_epi16(T00A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab2)])); \
    T23  = _mm_madd_epi16(T00B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab3)])); \
    T24  = _mm_madd_epi16(T11A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab0)])); \
    T25  = _mm_madd_epi16(T11B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab1)])); \
    T26  = _mm_madd_epi16(T01A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab2)])); \
    T27  = _mm_madd_epi16(T01B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab3)])); \
    T30  = _mm_madd_epi16(T12A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab0)])); \
    T31  = _mm_madd_epi16(T12B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab1)])); \
    T32  = _mm_madd_epi16(T02A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab2)])); \
    T33  = _mm_madd_epi16(T02B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab3)])); \
    T34  = _mm_madd_epi16(T13A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab0)])); \
    T35  = _mm_madd_epi16(T13B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab1)])); \
    T36  = _mm_madd_epi16(T03A, _mm_load_si128((__m128i*)tab_dct_32_1[(tab2)])); \
    T37  = _mm_madd_epi16(T03B, _mm_load_si128((__m128i*)tab_dct_32_1[(tab3)])); \
        \
    T60  = _mm_hadd_epi32(T20, T21); \
    T61  = _mm_hadd_epi32(T22, T23); \
    T62  = _mm_hadd_epi32(T24, T25); \
    T63  = _mm_hadd_epi32(T26, T27); \
    T64  = _mm_hadd_epi32(T30, T31); \
    T65  = _mm_hadd_epi32(T32, T33); \
    T66  = _mm_hadd_epi32(T34, T35); \
    T67  = _mm_hadd_epi32(T36, T37); \
        \
    T60  = _mm_hadd_epi32(T60, T61); \
    T61  = _mm_hadd_epi32(T62, T63); \
    T62  = _mm_hadd_epi32(T64, T65); \
    T63  = _mm_hadd_epi32(T66, T67); \
        \
    T60  = _mm_hadd_epi32(T60, T61); \
    T61  = _mm_hadd_epi32(T62, T63); \
        \
    T60  = _mm_hadd_epi32(T60, T61); \
        \
    T60  = _mm_srai_epi32(_mm_add_epi32(T60, c_1024), DCT32_SHIFT2); \
    T60  = _mm_packs_epi32(T60, T60); \
    _mm_storel_epi64((__m128i*)&dst[(dstPos) * 32 + (i * 4) + 0], T60); \

        MAKE_ODD(44, 44, 44, 44,  0);
        MAKE_ODD(45, 45, 45, 45, 16);
        MAKE_ODD(46, 47, 46, 47,  8);
        MAKE_ODD(48, 49, 48, 49, 24);

        MAKE_ODD(50, 51, 52, 53,  4);
        MAKE_ODD(54, 55, 56, 57, 12);
        MAKE_ODD(58, 59, 60, 61, 20);
        MAKE_ODD(62, 63, 64, 65, 28);

        MAKE_ODD(66, 67, 68, 69,  2);
        MAKE_ODD(70, 71, 72, 73,  6);
        MAKE_ODD(74, 75, 76, 77, 10);
        MAKE_ODD(78, 79, 80, 81, 14);

        MAKE_ODD(82, 83, 84, 85, 18);
        MAKE_ODD(86, 87, 88, 89, 22);
        MAKE_ODD(90, 91, 92, 93, 26);
        MAKE_ODD(94, 95, 96, 97, 30);

        MAKE_ODD(98, 99, 100, 101,  1);
        MAKE_ODD(102, 103, 104, 105,  3);
        MAKE_ODD(106, 107, 108, 109,  5);
        MAKE_ODD(110, 111, 112, 113,  7);
        MAKE_ODD(114, 115, 116, 117,  9);
        MAKE_ODD(118, 119, 120, 121, 11);
        MAKE_ODD(122, 123, 124, 125, 13);
        MAKE_ODD(126, 127, 128, 129, 15);
        MAKE_ODD(130, 131, 132, 133, 17);
        MAKE_ODD(134, 135, 136, 137, 19);
        MAKE_ODD(138, 139, 140, 141, 21);
        MAKE_ODD(142, 143, 144, 145, 23);
        MAKE_ODD(146, 147, 148, 149, 25);
        MAKE_ODD(150, 151, 152, 153, 27);
        MAKE_ODD(154, 155, 156, 157, 29);
        MAKE_ODD(158, 159, 160, 161, 31);
#undef MAKE_ODD
    }
}

namespace X265_NS {
void setupIntrinsicDCT_ssse3(EncoderPrimitives &p)
{
    /* Note: We have AVX2 assembly for these two functions, but since AVX2 is
     * still somewhat rare on end-user PCs we still compile and link these SSSE3
     * intrinsic SIMD functions */
    p.cu[BLOCK_16x16].dct = dct16;
    p.cu[BLOCK_32x32].dct = dct32;
}
}
