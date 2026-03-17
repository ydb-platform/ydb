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

using namespace X265_NS;

#define SHIFT1  7
#define ADD1    64

#define SHIFT2  (12 - (X265_DEPTH - 8))
#define ADD2    (1 << ((SHIFT2) - 1))

ALIGN_VAR_32(static const int16_t, tab_idct_8x8[12][8]) =
{
    {  89,  75,  89,  75, 89,  75, 89,  75 },
    {  50,  18,  50,  18, 50,  18, 50,  18 },
    {  75, -18,  75, -18, 75, -18, 75, -18 },
    { -89, -50, -89, -50, -89, -50, -89, -50 },
    {  50, -89,  50, -89, 50, -89, 50, -89 },
    {  18,  75,  18,  75, 18,  75, 18,  75 },
    {  18, -50,  18, -50, 18, -50, 18, -50 },
    {  75, -89,  75, -89, 75, -89, 75, -89 },
    {  64,  64,  64,  64, 64,  64, 64,  64 },
    {  64, -64,  64, -64, 64, -64, 64, -64 },
    {  83,  36,  83,  36, 83,  36, 83,  36 },
    {  36, -83,  36, -83, 36, -83, 36, -83 }
};

static void idct8(const int16_t* src, int16_t* dst, intptr_t stride)
{
    __m128i m128iS0, m128iS1, m128iS2, m128iS3, m128iS4, m128iS5, m128iS6, m128iS7, m128iAdd, m128Tmp0, m128Tmp1, m128Tmp2, m128Tmp3, E0h, E1h, E2h, E3h, E0l, E1l, E2l, E3l, O0h, O1h, O2h, O3h, O0l, O1l, O2l, O3l, EE0l, EE1l, E00l, E01l, EE0h, EE1h, E00h, E01h;
    __m128i T00, T01, T02, T03, T04, T05, T06, T07;

    m128iAdd = _mm_set1_epi32(ADD1);

    m128iS1 = _mm_load_si128((__m128i*)&src[8 + 0]);
    m128iS3 = _mm_load_si128((__m128i*)&src[24 + 0]);
    m128Tmp0 = _mm_unpacklo_epi16(m128iS1, m128iS3);
    E1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[0])));
    m128Tmp1 = _mm_unpackhi_epi16(m128iS1, m128iS3);
    E1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[0])));

    m128iS5 = _mm_load_si128((__m128i*)&src[40 + 0]);
    m128iS7 = _mm_load_si128((__m128i*)&src[56 + 0]);
    m128Tmp2 = _mm_unpacklo_epi16(m128iS5, m128iS7);
    E2l = _mm_madd_epi16(m128Tmp2, _mm_load_si128((__m128i*)(tab_idct_8x8[1])));
    m128Tmp3 = _mm_unpackhi_epi16(m128iS5, m128iS7);
    E2h = _mm_madd_epi16(m128Tmp3, _mm_load_si128((__m128i*)(tab_idct_8x8[1])));
    O0l = _mm_add_epi32(E1l, E2l);
    O0h = _mm_add_epi32(E1h, E2h);

    E1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[2])));
    E1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[2])));
    E2l = _mm_madd_epi16(m128Tmp2, _mm_load_si128((__m128i*)(tab_idct_8x8[3])));
    E2h = _mm_madd_epi16(m128Tmp3, _mm_load_si128((__m128i*)(tab_idct_8x8[3])));

    O1l = _mm_add_epi32(E1l, E2l);
    O1h = _mm_add_epi32(E1h, E2h);

    E1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[4])));
    E1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[4])));
    E2l = _mm_madd_epi16(m128Tmp2, _mm_load_si128((__m128i*)(tab_idct_8x8[5])));
    E2h = _mm_madd_epi16(m128Tmp3, _mm_load_si128((__m128i*)(tab_idct_8x8[5])));
    O2l = _mm_add_epi32(E1l, E2l);
    O2h = _mm_add_epi32(E1h, E2h);

    E1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[6])));
    E1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[6])));
    E2l = _mm_madd_epi16(m128Tmp2, _mm_load_si128((__m128i*)(tab_idct_8x8[7])));
    E2h = _mm_madd_epi16(m128Tmp3, _mm_load_si128((__m128i*)(tab_idct_8x8[7])));
    O3h = _mm_add_epi32(E1h, E2h);
    O3l = _mm_add_epi32(E1l, E2l);

    /*    -------     */

    m128iS0 = _mm_load_si128((__m128i*)&src[0 + 0]);
    m128iS4 = _mm_load_si128((__m128i*)&src[32 + 0]);
    m128Tmp0 = _mm_unpacklo_epi16(m128iS0, m128iS4);
    EE0l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[8])));
    m128Tmp1 = _mm_unpackhi_epi16(m128iS0, m128iS4);
    EE0h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[8])));

    EE1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[9])));
    EE1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[9])));

    /*    -------     */

    m128iS2 = _mm_load_si128((__m128i*)&src[16 + 0]);
    m128iS6 = _mm_load_si128((__m128i*)&src[48 + 0]);
    m128Tmp0 = _mm_unpacklo_epi16(m128iS2, m128iS6);
    E00l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[10])));
    m128Tmp1 = _mm_unpackhi_epi16(m128iS2, m128iS6);
    E00h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[10])));
    E01l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[11])));
    E01h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[11])));
    E0l = _mm_add_epi32(EE0l, E00l);
    E0l = _mm_add_epi32(E0l, m128iAdd);
    E0h = _mm_add_epi32(EE0h, E00h);
    E0h = _mm_add_epi32(E0h, m128iAdd);
    E3l = _mm_sub_epi32(EE0l, E00l);
    E3l = _mm_add_epi32(E3l, m128iAdd);
    E3h = _mm_sub_epi32(EE0h, E00h);
    E3h = _mm_add_epi32(E3h, m128iAdd);

    E1l = _mm_add_epi32(EE1l, E01l);
    E1l = _mm_add_epi32(E1l, m128iAdd);
    E1h = _mm_add_epi32(EE1h, E01h);
    E1h = _mm_add_epi32(E1h, m128iAdd);
    E2l = _mm_sub_epi32(EE1l, E01l);
    E2l = _mm_add_epi32(E2l, m128iAdd);
    E2h = _mm_sub_epi32(EE1h, E01h);
    E2h = _mm_add_epi32(E2h, m128iAdd);
    m128iS0 = _mm_packs_epi32(_mm_srai_epi32(_mm_add_epi32(E0l, O0l), SHIFT1), _mm_srai_epi32(_mm_add_epi32(E0h, O0h), SHIFT1));
    m128iS1 = _mm_packs_epi32(_mm_srai_epi32(_mm_add_epi32(E1l, O1l), SHIFT1), _mm_srai_epi32(_mm_add_epi32(E1h, O1h), SHIFT1));
    m128iS2 = _mm_packs_epi32(_mm_srai_epi32(_mm_add_epi32(E2l, O2l), SHIFT1), _mm_srai_epi32(_mm_add_epi32(E2h, O2h), SHIFT1));
    m128iS3 = _mm_packs_epi32(_mm_srai_epi32(_mm_add_epi32(E3l, O3l), SHIFT1), _mm_srai_epi32(_mm_add_epi32(E3h, O3h), SHIFT1));
    m128iS4 = _mm_packs_epi32(_mm_srai_epi32(_mm_sub_epi32(E3l, O3l), SHIFT1), _mm_srai_epi32(_mm_sub_epi32(E3h, O3h), SHIFT1));
    m128iS5 = _mm_packs_epi32(_mm_srai_epi32(_mm_sub_epi32(E2l, O2l), SHIFT1), _mm_srai_epi32(_mm_sub_epi32(E2h, O2h), SHIFT1));
    m128iS6 = _mm_packs_epi32(_mm_srai_epi32(_mm_sub_epi32(E1l, O1l), SHIFT1), _mm_srai_epi32(_mm_sub_epi32(E1h, O1h), SHIFT1));
    m128iS7 = _mm_packs_epi32(_mm_srai_epi32(_mm_sub_epi32(E0l, O0l), SHIFT1), _mm_srai_epi32(_mm_sub_epi32(E0h, O0h), SHIFT1));
    /*  Invers matrix   */

    E0l = _mm_unpacklo_epi16(m128iS0, m128iS4);
    E1l = _mm_unpacklo_epi16(m128iS1, m128iS5);
    E2l = _mm_unpacklo_epi16(m128iS2, m128iS6);
    E3l = _mm_unpacklo_epi16(m128iS3, m128iS7);
    O0l = _mm_unpackhi_epi16(m128iS0, m128iS4);
    O1l = _mm_unpackhi_epi16(m128iS1, m128iS5);
    O2l = _mm_unpackhi_epi16(m128iS2, m128iS6);
    O3l = _mm_unpackhi_epi16(m128iS3, m128iS7);
    m128Tmp0 = _mm_unpacklo_epi16(E0l, E2l);
    m128Tmp1 = _mm_unpacklo_epi16(E1l, E3l);
    m128iS0  = _mm_unpacklo_epi16(m128Tmp0, m128Tmp1);
    m128iS1  = _mm_unpackhi_epi16(m128Tmp0, m128Tmp1);
    m128Tmp2 = _mm_unpackhi_epi16(E0l, E2l);
    m128Tmp3 = _mm_unpackhi_epi16(E1l, E3l);
    m128iS2  = _mm_unpacklo_epi16(m128Tmp2, m128Tmp3);
    m128iS3  = _mm_unpackhi_epi16(m128Tmp2, m128Tmp3);
    m128Tmp0 = _mm_unpacklo_epi16(O0l, O2l);
    m128Tmp1 = _mm_unpacklo_epi16(O1l, O3l);
    m128iS4  = _mm_unpacklo_epi16(m128Tmp0, m128Tmp1);
    m128iS5  = _mm_unpackhi_epi16(m128Tmp0, m128Tmp1);
    m128Tmp2 = _mm_unpackhi_epi16(O0l, O2l);
    m128Tmp3 = _mm_unpackhi_epi16(O1l, O3l);
    m128iS6  = _mm_unpacklo_epi16(m128Tmp2, m128Tmp3);
    m128iS7  = _mm_unpackhi_epi16(m128Tmp2, m128Tmp3);

    m128iAdd = _mm_set1_epi32(ADD2);

    m128Tmp0 = _mm_unpacklo_epi16(m128iS1, m128iS3);
    E1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[0])));
    m128Tmp1 = _mm_unpackhi_epi16(m128iS1, m128iS3);
    E1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[0])));
    m128Tmp2 = _mm_unpacklo_epi16(m128iS5, m128iS7);
    E2l = _mm_madd_epi16(m128Tmp2, _mm_load_si128((__m128i*)(tab_idct_8x8[1])));
    m128Tmp3 = _mm_unpackhi_epi16(m128iS5, m128iS7);
    E2h = _mm_madd_epi16(m128Tmp3, _mm_load_si128((__m128i*)(tab_idct_8x8[1])));
    O0l = _mm_add_epi32(E1l, E2l);
    O0h = _mm_add_epi32(E1h, E2h);
    E1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[2])));
    E1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[2])));
    E2l = _mm_madd_epi16(m128Tmp2, _mm_load_si128((__m128i*)(tab_idct_8x8[3])));
    E2h = _mm_madd_epi16(m128Tmp3, _mm_load_si128((__m128i*)(tab_idct_8x8[3])));
    O1l = _mm_add_epi32(E1l, E2l);
    O1h = _mm_add_epi32(E1h, E2h);
    E1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[4])));
    E1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[4])));
    E2l = _mm_madd_epi16(m128Tmp2, _mm_load_si128((__m128i*)(tab_idct_8x8[5])));
    E2h = _mm_madd_epi16(m128Tmp3, _mm_load_si128((__m128i*)(tab_idct_8x8[5])));
    O2l = _mm_add_epi32(E1l, E2l);
    O2h = _mm_add_epi32(E1h, E2h);
    E1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[6])));
    E1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[6])));
    E2l = _mm_madd_epi16(m128Tmp2, _mm_load_si128((__m128i*)(tab_idct_8x8[7])));
    E2h = _mm_madd_epi16(m128Tmp3, _mm_load_si128((__m128i*)(tab_idct_8x8[7])));
    O3h = _mm_add_epi32(E1h, E2h);
    O3l = _mm_add_epi32(E1l, E2l);

    m128Tmp0 = _mm_unpacklo_epi16(m128iS0, m128iS4);
    EE0l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[8])));
    m128Tmp1 = _mm_unpackhi_epi16(m128iS0, m128iS4);
    EE0h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[8])));
    EE1l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[9])));
    EE1h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[9])));

    m128Tmp0 = _mm_unpacklo_epi16(m128iS2, m128iS6);
    E00l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[10])));
    m128Tmp1 = _mm_unpackhi_epi16(m128iS2, m128iS6);
    E00h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[10])));
    E01l = _mm_madd_epi16(m128Tmp0, _mm_load_si128((__m128i*)(tab_idct_8x8[11])));
    E01h = _mm_madd_epi16(m128Tmp1, _mm_load_si128((__m128i*)(tab_idct_8x8[11])));
    E0l = _mm_add_epi32(EE0l, E00l);
    E0l = _mm_add_epi32(E0l, m128iAdd);
    E0h = _mm_add_epi32(EE0h, E00h);
    E0h = _mm_add_epi32(E0h, m128iAdd);
    E3l = _mm_sub_epi32(EE0l, E00l);
    E3l = _mm_add_epi32(E3l, m128iAdd);
    E3h = _mm_sub_epi32(EE0h, E00h);
    E3h = _mm_add_epi32(E3h, m128iAdd);
    E1l = _mm_add_epi32(EE1l, E01l);
    E1l = _mm_add_epi32(E1l, m128iAdd);
    E1h = _mm_add_epi32(EE1h, E01h);
    E1h = _mm_add_epi32(E1h, m128iAdd);
    E2l = _mm_sub_epi32(EE1l, E01l);
    E2l = _mm_add_epi32(E2l, m128iAdd);
    E2h = _mm_sub_epi32(EE1h, E01h);
    E2h = _mm_add_epi32(E2h, m128iAdd);

    m128iS0 = _mm_packs_epi32(_mm_srai_epi32(_mm_add_epi32(E0l, O0l), SHIFT2), _mm_srai_epi32(_mm_add_epi32(E0h, O0h), SHIFT2));
    m128iS1 = _mm_packs_epi32(_mm_srai_epi32(_mm_add_epi32(E1l, O1l), SHIFT2), _mm_srai_epi32(_mm_add_epi32(E1h, O1h), SHIFT2));
    m128iS2 = _mm_packs_epi32(_mm_srai_epi32(_mm_add_epi32(E2l, O2l), SHIFT2), _mm_srai_epi32(_mm_add_epi32(E2h, O2h), SHIFT2));
    m128iS3 = _mm_packs_epi32(_mm_srai_epi32(_mm_add_epi32(E3l, O3l), SHIFT2), _mm_srai_epi32(_mm_add_epi32(E3h, O3h), SHIFT2));
    m128iS4 = _mm_packs_epi32(_mm_srai_epi32(_mm_sub_epi32(E3l, O3l), SHIFT2), _mm_srai_epi32(_mm_sub_epi32(E3h, O3h), SHIFT2));
    m128iS5 = _mm_packs_epi32(_mm_srai_epi32(_mm_sub_epi32(E2l, O2l), SHIFT2), _mm_srai_epi32(_mm_sub_epi32(E2h, O2h), SHIFT2));
    m128iS6 = _mm_packs_epi32(_mm_srai_epi32(_mm_sub_epi32(E1l, O1l), SHIFT2), _mm_srai_epi32(_mm_sub_epi32(E1h, O1h), SHIFT2));
    m128iS7 = _mm_packs_epi32(_mm_srai_epi32(_mm_sub_epi32(E0l, O0l), SHIFT2), _mm_srai_epi32(_mm_sub_epi32(E0h, O0h), SHIFT2));

    // [07 06 05 04 03 02 01 00]
    // [17 16 15 14 13 12 11 10]
    // [27 26 25 24 23 22 21 20]
    // [37 36 35 34 33 32 31 30]
    // [47 46 45 44 43 42 41 40]
    // [57 56 55 54 53 52 51 50]
    // [67 66 65 64 63 62 61 60]
    // [77 76 75 74 73 72 71 70]

    T00 = _mm_unpacklo_epi16(m128iS0, m128iS1);     // [13 03 12 02 11 01 10 00]
    T01 = _mm_unpackhi_epi16(m128iS0, m128iS1);     // [17 07 16 06 15 05 14 04]
    T02 = _mm_unpacklo_epi16(m128iS2, m128iS3);     // [33 23 32 22 31 21 30 20]
    T03 = _mm_unpackhi_epi16(m128iS2, m128iS3);     // [37 27 36 26 35 25 34 24]
    T04 = _mm_unpacklo_epi16(m128iS4, m128iS5);     // [53 43 52 42 51 41 50 40]
    T05 = _mm_unpackhi_epi16(m128iS4, m128iS5);     // [57 47 56 46 55 45 54 44]
    T06 = _mm_unpacklo_epi16(m128iS6, m128iS7);     // [73 63 72 62 71 61 70 60]
    T07 = _mm_unpackhi_epi16(m128iS6, m128iS7);     // [77 67 76 66 75 65 74 64]

    __m128i T10, T11;
    T10 = _mm_unpacklo_epi32(T00, T02);                                     // [31 21 11 01 30 20 10 00]
    T11 = _mm_unpackhi_epi32(T00, T02);                                     // [33 23 13 03 32 22 12 02]
    _mm_storel_epi64((__m128i*)&dst[0 * stride +  0], T10);                   // [30 20 10 00]
    _mm_storeh_pi((__m64*)&dst[1 * stride +  0], _mm_castsi128_ps(T10));  // [31 21 11 01]
    _mm_storel_epi64((__m128i*)&dst[2 * stride +  0], T11);                   // [32 22 12 02]
    _mm_storeh_pi((__m64*)&dst[3 * stride +  0], _mm_castsi128_ps(T11));  // [33 23 13 03]

    T10 = _mm_unpacklo_epi32(T04, T06);                                     // [71 61 51 41 70 60 50 40]
    T11 = _mm_unpackhi_epi32(T04, T06);                                     // [73 63 53 43 72 62 52 42]
    _mm_storel_epi64((__m128i*)&dst[0 * stride +  4], T10);
    _mm_storeh_pi((__m64*)&dst[1 * stride +  4], _mm_castsi128_ps(T10));
    _mm_storel_epi64((__m128i*)&dst[2 * stride +  4], T11);
    _mm_storeh_pi((__m64*)&dst[3 * stride +  4], _mm_castsi128_ps(T11));

    T10 = _mm_unpacklo_epi32(T01, T03);                                     // [35 25 15 05 34 24 14 04]
    T11 = _mm_unpackhi_epi32(T01, T03);                                     // [37 27 17 07 36 26 16 06]
    _mm_storel_epi64((__m128i*)&dst[4 * stride +  0], T10);
    _mm_storeh_pi((__m64*)&dst[5 * stride +  0], _mm_castsi128_ps(T10));
    _mm_storel_epi64((__m128i*)&dst[6 * stride +  0], T11);
    _mm_storeh_pi((__m64*)&dst[7 * stride +  0], _mm_castsi128_ps(T11));

    T10 = _mm_unpacklo_epi32(T05, T07);                                     // [75 65 55 45 74 64 54 44]
    T11 = _mm_unpackhi_epi32(T05, T07);                                     // [77 67 57 47 76 56 46 36]
    _mm_storel_epi64((__m128i*)&dst[4 * stride +  4], T10);
    _mm_storeh_pi((__m64*)&dst[5 * stride +  4], _mm_castsi128_ps(T10));
    _mm_storel_epi64((__m128i*)&dst[6 * stride +  4], T11);
    _mm_storeh_pi((__m64*)&dst[7 * stride +  4], _mm_castsi128_ps(T11));
}

static void idct16(const int16_t *src, int16_t *dst, intptr_t stride)
{
#define READ_UNPACKHILO(offset)\
    const __m128i T_00_00A = _mm_unpacklo_epi16(*(__m128i*)&src[1 * 16 + offset], *(__m128i*)&src[3 * 16 + offset]);\
    const __m128i T_00_00B = _mm_unpackhi_epi16(*(__m128i*)&src[1 * 16 + offset], *(__m128i*)&src[3 * 16 + offset]);\
    const __m128i T_00_01A = _mm_unpacklo_epi16(*(__m128i*)&src[5 * 16 + offset], *(__m128i*)&src[7 * 16 + offset]);\
    const __m128i T_00_01B = _mm_unpackhi_epi16(*(__m128i*)&src[5 * 16 + offset], *(__m128i*)&src[7 * 16 + offset]);\
    const __m128i T_00_02A = _mm_unpacklo_epi16(*(__m128i*)&src[9 * 16 + offset], *(__m128i*)&src[11 * 16 + offset]);\
    const __m128i T_00_02B = _mm_unpackhi_epi16(*(__m128i*)&src[9 * 16 + offset], *(__m128i*)&src[11 * 16 + offset]);\
    const __m128i T_00_03A = _mm_unpacklo_epi16(*(__m128i*)&src[13 * 16 + offset], *(__m128i*)&src[15 * 16 + offset]);\
    const __m128i T_00_03B = _mm_unpackhi_epi16(*(__m128i*)&src[13 * 16 + offset], *(__m128i*)&src[15 * 16 + offset]);\
    const __m128i T_00_04A = _mm_unpacklo_epi16(*(__m128i*)&src[2 * 16 + offset], *(__m128i*)&src[6 * 16 + offset]);\
    const __m128i T_00_04B = _mm_unpackhi_epi16(*(__m128i*)&src[2 * 16 + offset], *(__m128i*)&src[6 * 16 + offset]);\
    const __m128i T_00_05A = _mm_unpacklo_epi16(*(__m128i*)&src[10 * 16 + offset], *(__m128i*)&src[14 * 16 + offset]);\
    const __m128i T_00_05B = _mm_unpackhi_epi16(*(__m128i*)&src[10 * 16 + offset], *(__m128i*)&src[14 * 16 + offset]);\
    const __m128i T_00_06A = _mm_unpacklo_epi16(*(__m128i*)&src[4 * 16 + offset], *(__m128i*)&src[12 * 16 + offset]);\
    const __m128i T_00_06B = _mm_unpackhi_epi16(*(__m128i*)&src[4 * 16 + offset], *(__m128i*)&src[12 * 16 + offset]);\
    const __m128i T_00_07A = _mm_unpacklo_epi16(*(__m128i*)&src[0 * 16 + offset], *(__m128i*)&src[8 * 16 + offset]);\
    const __m128i T_00_07B = _mm_unpackhi_epi16(*(__m128i*)&src[0 * 16 + offset], *(__m128i*)&src[8 * 16 + offset]);

#define UNPACKHILO(part) \
    const __m128i T_00_00A = _mm_unpacklo_epi16(in01[part], in03[part]);\
    const __m128i T_00_00B = _mm_unpackhi_epi16(in01[part], in03[part]);\
    const __m128i T_00_01A = _mm_unpacklo_epi16(in05[part], in07[part]);\
    const __m128i T_00_01B = _mm_unpackhi_epi16(in05[part], in07[part]);\
    const __m128i T_00_02A = _mm_unpacklo_epi16(in09[part], in11[part]);\
    const __m128i T_00_02B = _mm_unpackhi_epi16(in09[part], in11[part]);\
    const __m128i T_00_03A = _mm_unpacklo_epi16(in13[part], in15[part]);\
    const __m128i T_00_03B = _mm_unpackhi_epi16(in13[part], in15[part]);\
    const __m128i T_00_04A = _mm_unpacklo_epi16(in02[part], in06[part]);\
    const __m128i T_00_04B = _mm_unpackhi_epi16(in02[part], in06[part]);\
    const __m128i T_00_05A = _mm_unpacklo_epi16(in10[part], in14[part]);\
    const __m128i T_00_05B = _mm_unpackhi_epi16(in10[part], in14[part]);\
    const __m128i T_00_06A = _mm_unpacklo_epi16(in04[part], in12[part]);\
    const __m128i T_00_06B = _mm_unpackhi_epi16(in04[part], in12[part]);\
    const __m128i T_00_07A = _mm_unpacklo_epi16(in00[part], in08[part]);\
    const __m128i T_00_07B = _mm_unpackhi_epi16(in00[part], in08[part]);

#define COMPUTE_ROW(row0103, row0507, row0911, row1315, c0103, c0507, c0911, c1315, row) \
    T00 = _mm_add_epi32(_mm_madd_epi16(row0103, c0103), _mm_madd_epi16(row0507, c0507)); \
    T01 = _mm_add_epi32(_mm_madd_epi16(row0911, c0911), _mm_madd_epi16(row1315, c1315)); \
    row = _mm_add_epi32(T00, T01);

#define TRANSPOSE_8x8_16BIT(I0, I1, I2, I3, I4, I5, I6, I7, O0, O1, O2, O3, O4, O5, O6, O7) \
    tr0_0 = _mm_unpacklo_epi16(I0, I1); \
    tr0_1 = _mm_unpacklo_epi16(I2, I3); \
    tr0_2 = _mm_unpackhi_epi16(I0, I1); \
    tr0_3 = _mm_unpackhi_epi16(I2, I3); \
    tr0_4 = _mm_unpacklo_epi16(I4, I5); \
    tr0_5 = _mm_unpacklo_epi16(I6, I7); \
    tr0_6 = _mm_unpackhi_epi16(I4, I5); \
    tr0_7 = _mm_unpackhi_epi16(I6, I7); \
    tr1_0 = _mm_unpacklo_epi32(tr0_0, tr0_1); \
    tr1_1 = _mm_unpacklo_epi32(tr0_2, tr0_3); \
    tr1_2 = _mm_unpackhi_epi32(tr0_0, tr0_1); \
    tr1_3 = _mm_unpackhi_epi32(tr0_2, tr0_3); \
    tr1_4 = _mm_unpacklo_epi32(tr0_4, tr0_5); \
    tr1_5 = _mm_unpacklo_epi32(tr0_6, tr0_7); \
    tr1_6 = _mm_unpackhi_epi32(tr0_4, tr0_5); \
    tr1_7 = _mm_unpackhi_epi32(tr0_6, tr0_7); \
    O0 = _mm_unpacklo_epi64(tr1_0, tr1_4); \
    O1 = _mm_unpackhi_epi64(tr1_0, tr1_4); \
    O2 = _mm_unpacklo_epi64(tr1_2, tr1_6); \
    O3 = _mm_unpackhi_epi64(tr1_2, tr1_6); \
    O4 = _mm_unpacklo_epi64(tr1_1, tr1_5); \
    O5 = _mm_unpackhi_epi64(tr1_1, tr1_5); \
    O6 = _mm_unpacklo_epi64(tr1_3, tr1_7); \
    O7 = _mm_unpackhi_epi64(tr1_3, tr1_7);

#define PROCESS(part, rnd, shift) \
    __m128i c32_rnd = _mm_set1_epi32(rnd);\
    int nShift = shift;\
\
    __m128i O0A, O1A, O2A, O3A, O4A, O5A, O6A, O7A;\
    __m128i O0B, O1B, O2B, O3B, O4B, O5B, O6B, O7B;\
    {\
        __m128i T00, T01;\
\
        COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, c16_p87_p90, c16_p70_p80, c16_p43_p57, c16_p09_p25, O0A)\
        COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, c16_p57_p87, c16_n43_p09, c16_n90_n80, c16_n25_n70, O1A)\
        COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, c16_p09_p80, c16_n87_n70, c16_p57_n25, c16_p43_p90, O2A)\
        COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, c16_n43_p70, c16_p09_n87, c16_p25_p90, c16_n57_n80, O3A)\
        COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, c16_n80_p57, c16_p90_n25, c16_n87_n09, c16_p70_p43, O4A)\
        COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, c16_n90_p43, c16_p25_p57, c16_p70_n87, c16_n80_p09, O5A)\
        COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, c16_n70_p25, c16_n80_p90, c16_p09_p43, c16_p87_n57, O6A)\
        COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, c16_n25_p09, c16_n57_p43, c16_n80_p70, c16_n90_p87, O7A)\
\
        COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, c16_p87_p90, c16_p70_p80, c16_p43_p57, c16_p09_p25, O0B)\
        COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, c16_p57_p87, c16_n43_p09, c16_n90_n80, c16_n25_n70, O1B)\
        COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, c16_p09_p80, c16_n87_n70, c16_p57_n25, c16_p43_p90, O2B)\
        COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, c16_n43_p70, c16_p09_n87, c16_p25_p90, c16_n57_n80, O3B)\
        COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, c16_n80_p57, c16_p90_n25, c16_n87_n09, c16_p70_p43, O4B)\
        COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, c16_n90_p43, c16_p25_p57, c16_p70_n87, c16_n80_p09, O5B)\
        COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, c16_n70_p25, c16_n80_p90, c16_p09_p43, c16_p87_n57, O6B)\
        COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, c16_n25_p09, c16_n57_p43, c16_n80_p70, c16_n90_p87, O7B)\
    }\
\
    __m128i EO0A, EO1A, EO2A, EO3A;\
    __m128i EO0B, EO1B, EO2B, EO3B;\
    EO0A = _mm_add_epi32(_mm_madd_epi16(T_00_04A, c16_p75_p89), _mm_madd_epi16(T_00_05A, c16_p18_p50));\
    EO0B = _mm_add_epi32(_mm_madd_epi16(T_00_04B, c16_p75_p89), _mm_madd_epi16(T_00_05B, c16_p18_p50));\
    EO1A = _mm_add_epi32(_mm_madd_epi16(T_00_04A, c16_n18_p75), _mm_madd_epi16(T_00_05A, c16_n50_n89));\
    EO1B = _mm_add_epi32(_mm_madd_epi16(T_00_04B, c16_n18_p75), _mm_madd_epi16(T_00_05B, c16_n50_n89));\
    EO2A = _mm_add_epi32(_mm_madd_epi16(T_00_04A, c16_n89_p50), _mm_madd_epi16(T_00_05A, c16_p75_p18));\
    EO2B = _mm_add_epi32(_mm_madd_epi16(T_00_04B, c16_n89_p50), _mm_madd_epi16(T_00_05B, c16_p75_p18));\
    EO3A = _mm_add_epi32(_mm_madd_epi16(T_00_04A, c16_n50_p18), _mm_madd_epi16(T_00_05A, c16_n89_p75));\
    EO3B = _mm_add_epi32(_mm_madd_epi16(T_00_04B, c16_n50_p18), _mm_madd_epi16(T_00_05B, c16_n89_p75));\
\
    __m128i EEO0A, EEO1A;\
    __m128i EEO0B, EEO1B;\
    EEO0A = _mm_madd_epi16(T_00_06A, c16_p36_p83);\
    EEO0B = _mm_madd_epi16(T_00_06B, c16_p36_p83);\
    EEO1A = _mm_madd_epi16(T_00_06A, c16_n83_p36);\
    EEO1B = _mm_madd_epi16(T_00_06B, c16_n83_p36);\
\
    __m128i EEE0A, EEE1A;\
    __m128i EEE0B, EEE1B;\
    EEE0A = _mm_madd_epi16(T_00_07A, c16_p64_p64);\
    EEE0B = _mm_madd_epi16(T_00_07B, c16_p64_p64);\
    EEE1A = _mm_madd_epi16(T_00_07A, c16_n64_p64);\
    EEE1B = _mm_madd_epi16(T_00_07B, c16_n64_p64);\
\
    const __m128i EE0A = _mm_add_epi32(EEE0A, EEO0A);\
    const __m128i EE0B = _mm_add_epi32(EEE0B, EEO0B);\
    const __m128i EE1A = _mm_add_epi32(EEE1A, EEO1A);\
    const __m128i EE1B = _mm_add_epi32(EEE1B, EEO1B);\
    const __m128i EE3A = _mm_sub_epi32(EEE0A, EEO0A);\
    const __m128i EE3B = _mm_sub_epi32(EEE0B, EEO0B);\
    const __m128i EE2A = _mm_sub_epi32(EEE1A, EEO1A);\
    const __m128i EE2B = _mm_sub_epi32(EEE1B, EEO1B);\
\
    const __m128i E0A = _mm_add_epi32(EE0A, EO0A);\
    const __m128i E0B = _mm_add_epi32(EE0B, EO0B);\
    const __m128i E1A = _mm_add_epi32(EE1A, EO1A);\
    const __m128i E1B = _mm_add_epi32(EE1B, EO1B);\
    const __m128i E2A = _mm_add_epi32(EE2A, EO2A);\
    const __m128i E2B = _mm_add_epi32(EE2B, EO2B);\
    const __m128i E3A = _mm_add_epi32(EE3A, EO3A);\
    const __m128i E3B = _mm_add_epi32(EE3B, EO3B);\
    const __m128i E7A = _mm_sub_epi32(EE0A, EO0A);\
    const __m128i E7B = _mm_sub_epi32(EE0B, EO0B);\
    const __m128i E6A = _mm_sub_epi32(EE1A, EO1A);\
    const __m128i E6B = _mm_sub_epi32(EE1B, EO1B);\
    const __m128i E5A = _mm_sub_epi32(EE2A, EO2A);\
    const __m128i E5B = _mm_sub_epi32(EE2B, EO2B);\
    const __m128i E4A = _mm_sub_epi32(EE3A, EO3A);\
    const __m128i E4B = _mm_sub_epi32(EE3B, EO3B);\
\
    const __m128i T10A = _mm_add_epi32(E0A, c32_rnd);\
    const __m128i T10B = _mm_add_epi32(E0B, c32_rnd);\
    const __m128i T11A = _mm_add_epi32(E1A, c32_rnd);\
    const __m128i T11B = _mm_add_epi32(E1B, c32_rnd);\
    const __m128i T12A = _mm_add_epi32(E2A, c32_rnd);\
    const __m128i T12B = _mm_add_epi32(E2B, c32_rnd);\
    const __m128i T13A = _mm_add_epi32(E3A, c32_rnd);\
    const __m128i T13B = _mm_add_epi32(E3B, c32_rnd);\
    const __m128i T14A = _mm_add_epi32(E4A, c32_rnd);\
    const __m128i T14B = _mm_add_epi32(E4B, c32_rnd);\
    const __m128i T15A = _mm_add_epi32(E5A, c32_rnd);\
    const __m128i T15B = _mm_add_epi32(E5B, c32_rnd);\
    const __m128i T16A = _mm_add_epi32(E6A, c32_rnd);\
    const __m128i T16B = _mm_add_epi32(E6B, c32_rnd);\
    const __m128i T17A = _mm_add_epi32(E7A, c32_rnd);\
    const __m128i T17B = _mm_add_epi32(E7B, c32_rnd);\
\
    const __m128i T20A = _mm_add_epi32(T10A, O0A);\
    const __m128i T20B = _mm_add_epi32(T10B, O0B);\
    const __m128i T21A = _mm_add_epi32(T11A, O1A);\
    const __m128i T21B = _mm_add_epi32(T11B, O1B);\
    const __m128i T22A = _mm_add_epi32(T12A, O2A);\
    const __m128i T22B = _mm_add_epi32(T12B, O2B);\
    const __m128i T23A = _mm_add_epi32(T13A, O3A);\
    const __m128i T23B = _mm_add_epi32(T13B, O3B);\
    const __m128i T24A = _mm_add_epi32(T14A, O4A);\
    const __m128i T24B = _mm_add_epi32(T14B, O4B);\
    const __m128i T25A = _mm_add_epi32(T15A, O5A);\
    const __m128i T25B = _mm_add_epi32(T15B, O5B);\
    const __m128i T26A = _mm_add_epi32(T16A, O6A);\
    const __m128i T26B = _mm_add_epi32(T16B, O6B);\
    const __m128i T27A = _mm_add_epi32(T17A, O7A);\
    const __m128i T27B = _mm_add_epi32(T17B, O7B);\
    const __m128i T2FA = _mm_sub_epi32(T10A, O0A);\
    const __m128i T2FB = _mm_sub_epi32(T10B, O0B);\
    const __m128i T2EA = _mm_sub_epi32(T11A, O1A);\
    const __m128i T2EB = _mm_sub_epi32(T11B, O1B);\
    const __m128i T2DA = _mm_sub_epi32(T12A, O2A);\
    const __m128i T2DB = _mm_sub_epi32(T12B, O2B);\
    const __m128i T2CA = _mm_sub_epi32(T13A, O3A);\
    const __m128i T2CB = _mm_sub_epi32(T13B, O3B);\
    const __m128i T2BA = _mm_sub_epi32(T14A, O4A);\
    const __m128i T2BB = _mm_sub_epi32(T14B, O4B);\
    const __m128i T2AA = _mm_sub_epi32(T15A, O5A);\
    const __m128i T2AB = _mm_sub_epi32(T15B, O5B);\
    const __m128i T29A = _mm_sub_epi32(T16A, O6A);\
    const __m128i T29B = _mm_sub_epi32(T16B, O6B);\
    const __m128i T28A = _mm_sub_epi32(T17A, O7A);\
    const __m128i T28B = _mm_sub_epi32(T17B, O7B);\
\
    const __m128i T30A = _mm_srai_epi32(T20A, nShift);\
    const __m128i T30B = _mm_srai_epi32(T20B, nShift);\
    const __m128i T31A = _mm_srai_epi32(T21A, nShift);\
    const __m128i T31B = _mm_srai_epi32(T21B, nShift);\
    const __m128i T32A = _mm_srai_epi32(T22A, nShift);\
    const __m128i T32B = _mm_srai_epi32(T22B, nShift);\
    const __m128i T33A = _mm_srai_epi32(T23A, nShift);\
    const __m128i T33B = _mm_srai_epi32(T23B, nShift);\
    const __m128i T34A = _mm_srai_epi32(T24A, nShift);\
    const __m128i T34B = _mm_srai_epi32(T24B, nShift);\
    const __m128i T35A = _mm_srai_epi32(T25A, nShift);\
    const __m128i T35B = _mm_srai_epi32(T25B, nShift);\
    const __m128i T36A = _mm_srai_epi32(T26A, nShift);\
    const __m128i T36B = _mm_srai_epi32(T26B, nShift);\
    const __m128i T37A = _mm_srai_epi32(T27A, nShift);\
    const __m128i T37B = _mm_srai_epi32(T27B, nShift);\
\
    const __m128i T38A = _mm_srai_epi32(T28A, nShift);\
    const __m128i T38B = _mm_srai_epi32(T28B, nShift);\
    const __m128i T39A = _mm_srai_epi32(T29A, nShift);\
    const __m128i T39B = _mm_srai_epi32(T29B, nShift);\
    const __m128i T3AA = _mm_srai_epi32(T2AA, nShift);\
    const __m128i T3AB = _mm_srai_epi32(T2AB, nShift);\
    const __m128i T3BA = _mm_srai_epi32(T2BA, nShift);\
    const __m128i T3BB = _mm_srai_epi32(T2BB, nShift);\
    const __m128i T3CA = _mm_srai_epi32(T2CA, nShift);\
    const __m128i T3CB = _mm_srai_epi32(T2CB, nShift);\
    const __m128i T3DA = _mm_srai_epi32(T2DA, nShift);\
    const __m128i T3DB = _mm_srai_epi32(T2DB, nShift);\
    const __m128i T3EA = _mm_srai_epi32(T2EA, nShift);\
    const __m128i T3EB = _mm_srai_epi32(T2EB, nShift);\
    const __m128i T3FA = _mm_srai_epi32(T2FA, nShift);\
    const __m128i T3FB = _mm_srai_epi32(T2FB, nShift);\
\
    res00[part]  = _mm_packs_epi32(T30A, T30B);\
    res01[part]  = _mm_packs_epi32(T31A, T31B);\
    res02[part]  = _mm_packs_epi32(T32A, T32B);\
    res03[part]  = _mm_packs_epi32(T33A, T33B);\
    res04[part]  = _mm_packs_epi32(T34A, T34B);\
    res05[part]  = _mm_packs_epi32(T35A, T35B);\
    res06[part]  = _mm_packs_epi32(T36A, T36B);\
    res07[part]  = _mm_packs_epi32(T37A, T37B);\
\
    res08[part]  = _mm_packs_epi32(T38A, T38B);\
    res09[part]  = _mm_packs_epi32(T39A, T39B);\
    res10[part]  = _mm_packs_epi32(T3AA, T3AB);\
    res11[part]  = _mm_packs_epi32(T3BA, T3BB);\
    res12[part]  = _mm_packs_epi32(T3CA, T3CB);\
    res13[part]  = _mm_packs_epi32(T3DA, T3DB);\
    res14[part]  = _mm_packs_epi32(T3EA, T3EB);\
    res15[part]  = _mm_packs_epi32(T3FA, T3FB);

    const __m128i c16_p87_p90   = _mm_set1_epi32(0x0057005A); //row0 87high - 90low address
    const __m128i c16_p70_p80   = _mm_set1_epi32(0x00460050);
    const __m128i c16_p43_p57   = _mm_set1_epi32(0x002B0039);
    const __m128i c16_p09_p25   = _mm_set1_epi32(0x00090019);
    const __m128i c16_p57_p87   = _mm_set1_epi32(0x00390057); //row1
    const __m128i c16_n43_p09   = _mm_set1_epi32(0xFFD50009);
    const __m128i c16_n90_n80   = _mm_set1_epi32(0xFFA6FFB0);
    const __m128i c16_n25_n70   = _mm_set1_epi32(0xFFE7FFBA);
    const __m128i c16_p09_p80   = _mm_set1_epi32(0x00090050); //row2
    const __m128i c16_n87_n70   = _mm_set1_epi32(0xFFA9FFBA);
    const __m128i c16_p57_n25   = _mm_set1_epi32(0x0039FFE7);
    const __m128i c16_p43_p90   = _mm_set1_epi32(0x002B005A);
    const __m128i c16_n43_p70   = _mm_set1_epi32(0xFFD50046); //row3
    const __m128i c16_p09_n87   = _mm_set1_epi32(0x0009FFA9);
    const __m128i c16_p25_p90   = _mm_set1_epi32(0x0019005A);
    const __m128i c16_n57_n80   = _mm_set1_epi32(0xFFC7FFB0);
    const __m128i c16_n80_p57   = _mm_set1_epi32(0xFFB00039); //row4
    const __m128i c16_p90_n25   = _mm_set1_epi32(0x005AFFE7);
    const __m128i c16_n87_n09   = _mm_set1_epi32(0xFFA9FFF7);
    const __m128i c16_p70_p43   = _mm_set1_epi32(0x0046002B);
    const __m128i c16_n90_p43   = _mm_set1_epi32(0xFFA6002B); //row5
    const __m128i c16_p25_p57   = _mm_set1_epi32(0x00190039);
    const __m128i c16_p70_n87   = _mm_set1_epi32(0x0046FFA9);
    const __m128i c16_n80_p09   = _mm_set1_epi32(0xFFB00009);
    const __m128i c16_n70_p25   = _mm_set1_epi32(0xFFBA0019); //row6
    const __m128i c16_n80_p90   = _mm_set1_epi32(0xFFB0005A);
    const __m128i c16_p09_p43   = _mm_set1_epi32(0x0009002B);
    const __m128i c16_p87_n57   = _mm_set1_epi32(0x0057FFC7);
    const __m128i c16_n25_p09   = _mm_set1_epi32(0xFFE70009); //row7
    const __m128i c16_n57_p43   = _mm_set1_epi32(0xFFC7002B);
    const __m128i c16_n80_p70   = _mm_set1_epi32(0xFFB00046);
    const __m128i c16_n90_p87   = _mm_set1_epi32(0xFFA60057);

    const __m128i c16_p75_p89   = _mm_set1_epi32(0x004B0059);
    const __m128i c16_p18_p50   = _mm_set1_epi32(0x00120032);
    const __m128i c16_n18_p75   = _mm_set1_epi32(0xFFEE004B);
    const __m128i c16_n50_n89   = _mm_set1_epi32(0xFFCEFFA7);
    const __m128i c16_n89_p50   = _mm_set1_epi32(0xFFA70032);
    const __m128i c16_p75_p18   = _mm_set1_epi32(0x004B0012);
    const __m128i c16_n50_p18   = _mm_set1_epi32(0xFFCE0012);
    const __m128i c16_n89_p75   = _mm_set1_epi32(0xFFA7004B);

    const __m128i c16_p36_p83   = _mm_set1_epi32(0x00240053);
    const __m128i c16_n83_p36   = _mm_set1_epi32(0xFFAD0024);

    const __m128i c16_n64_p64   = _mm_set1_epi32(0xFFC00040);
    const __m128i c16_p64_p64   = _mm_set1_epi32(0x00400040);

    // DCT1
    __m128i in00[2], in01[2], in02[2], in03[2], in04[2], in05[2], in06[2], in07[2];
    __m128i in08[2], in09[2], in10[2], in11[2], in12[2], in13[2], in14[2], in15[2];
    __m128i res00[2], res01[2], res02[2], res03[2], res04[2], res05[2], res06[2], res07[2];
    __m128i res08[2], res09[2], res10[2], res11[2], res12[2], res13[2], res14[2], res15[2];

    {
        READ_UNPACKHILO(0)
        PROCESS(0, ADD1, SHIFT1)
    }

    {
        READ_UNPACKHILO(8)
        PROCESS(1, ADD1, SHIFT1)
    }
    {
        __m128i tr0_0, tr0_1, tr0_2, tr0_3, tr0_4, tr0_5, tr0_6, tr0_7;
        __m128i tr1_0, tr1_1, tr1_2, tr1_3, tr1_4, tr1_5, tr1_6, tr1_7;
        TRANSPOSE_8x8_16BIT(res00[0], res01[0], res02[0], res03[0], res04[0], res05[0], res06[0], res07[0], in00[0], in01[0], in02[0], in03[0], in04[0], in05[0], in06[0], in07[0])
        TRANSPOSE_8x8_16BIT(res08[0], res09[0], res10[0], res11[0], res12[0], res13[0], res14[0], res15[0], in00[1], in01[1], in02[1], in03[1], in04[1], in05[1], in06[1], in07[1])
        TRANSPOSE_8x8_16BIT(res00[1], res01[1], res02[1], res03[1], res04[1], res05[1], res06[1], res07[1], in08[0], in09[0], in10[0], in11[0], in12[0], in13[0], in14[0], in15[0])
        TRANSPOSE_8x8_16BIT(res08[1], res09[1], res10[1], res11[1], res12[1], res13[1], res14[1], res15[1], in08[1], in09[1], in10[1], in11[1], in12[1], in13[1], in14[1], in15[1])
    }

    {
        UNPACKHILO(0)
        PROCESS(0, ADD2, SHIFT2)
    }
    {
        UNPACKHILO(1)
        PROCESS(1, ADD2, SHIFT2)
    }

    {
        __m128i tr0_0, tr0_1, tr0_2, tr0_3, tr0_4, tr0_5, tr0_6, tr0_7;
        __m128i tr1_0, tr1_1, tr1_2, tr1_3, tr1_4, tr1_5, tr1_6, tr1_7;
        TRANSPOSE_8x8_16BIT(res00[0], res01[0], res02[0], res03[0], res04[0], res05[0], res06[0], res07[0], in00[0], in01[0], in02[0], in03[0], in04[0], in05[0], in06[0], in07[0])
        _mm_store_si128((__m128i*)&dst[0 * stride + 0], in00[0]);
        _mm_store_si128((__m128i*)&dst[1 * stride + 0], in01[0]);
        _mm_store_si128((__m128i*)&dst[2 * stride + 0], in02[0]);
        _mm_store_si128((__m128i*)&dst[3 * stride + 0], in03[0]);
        _mm_store_si128((__m128i*)&dst[4 * stride + 0], in04[0]);
        _mm_store_si128((__m128i*)&dst[5 * stride + 0], in05[0]);
        _mm_store_si128((__m128i*)&dst[6 * stride + 0], in06[0]);
        _mm_store_si128((__m128i*)&dst[7 * stride + 0], in07[0]);
        TRANSPOSE_8x8_16BIT(res08[0], res09[0], res10[0], res11[0], res12[0], res13[0], res14[0], res15[0], in00[1], in01[1], in02[1], in03[1], in04[1], in05[1], in06[1], in07[1])
        _mm_store_si128((__m128i*)&dst[0 * stride + 8], in00[1]);
        _mm_store_si128((__m128i*)&dst[1 * stride + 8], in01[1]);
        _mm_store_si128((__m128i*)&dst[2 * stride + 8], in02[1]);
        _mm_store_si128((__m128i*)&dst[3 * stride + 8], in03[1]);
        _mm_store_si128((__m128i*)&dst[4 * stride + 8], in04[1]);
        _mm_store_si128((__m128i*)&dst[5 * stride + 8], in05[1]);
        _mm_store_si128((__m128i*)&dst[6 * stride + 8], in06[1]);
        _mm_store_si128((__m128i*)&dst[7 * stride + 8], in07[1]);
        TRANSPOSE_8x8_16BIT(res00[1], res01[1], res02[1], res03[1], res04[1], res05[1], res06[1], res07[1], in08[0], in09[0], in10[0], in11[0], in12[0], in13[0], in14[0], in15[0])
        _mm_store_si128((__m128i*)&dst[8 * stride + 0], in08[0]);
        _mm_store_si128((__m128i*)&dst[9 * stride + 0], in09[0]);
        _mm_store_si128((__m128i*)&dst[10 * stride + 0], in10[0]);
        _mm_store_si128((__m128i*)&dst[11 * stride + 0], in11[0]);
        _mm_store_si128((__m128i*)&dst[12 * stride + 0], in12[0]);
        _mm_store_si128((__m128i*)&dst[13 * stride + 0], in13[0]);
        _mm_store_si128((__m128i*)&dst[14 * stride + 0], in14[0]);
        _mm_store_si128((__m128i*)&dst[15 * stride + 0], in15[0]);
        TRANSPOSE_8x8_16BIT(res08[1], res09[1], res10[1], res11[1], res12[1], res13[1], res14[1], res15[1], in08[1], in09[1], in10[1], in11[1], in12[1], in13[1], in14[1], in15[1])
        _mm_store_si128((__m128i*)&dst[8 * stride + 8], in08[1]);
        _mm_store_si128((__m128i*)&dst[9 * stride + 8], in09[1]);
        _mm_store_si128((__m128i*)&dst[10 * stride + 8], in10[1]);
        _mm_store_si128((__m128i*)&dst[11 * stride + 8], in11[1]);
        _mm_store_si128((__m128i*)&dst[12 * stride + 8], in12[1]);
        _mm_store_si128((__m128i*)&dst[13 * stride + 8], in13[1]);
        _mm_store_si128((__m128i*)&dst[14 * stride + 8], in14[1]);
        _mm_store_si128((__m128i*)&dst[15 * stride + 8], in15[1]);
    }
}
#undef PROCESS
#undef TRANSPOSE_8x8_16BIT
#undef COMPUTE_ROW
#undef UNPACKHILO
#undef READ_UNPACKHILO

static void idct32(const int16_t *src, int16_t *dst, intptr_t stride)
{
    //Odd
    const __m128i c16_p90_p90   = _mm_set1_epi32(0x005A005A); //column 0
    const __m128i c16_p85_p88   = _mm_set1_epi32(0x00550058);
    const __m128i c16_p78_p82   = _mm_set1_epi32(0x004E0052);
    const __m128i c16_p67_p73   = _mm_set1_epi32(0x00430049);
    const __m128i c16_p54_p61   = _mm_set1_epi32(0x0036003D);
    const __m128i c16_p38_p46   = _mm_set1_epi32(0x0026002E);
    const __m128i c16_p22_p31   = _mm_set1_epi32(0x0016001F);
    const __m128i c16_p04_p13   = _mm_set1_epi32(0x0004000D);
    const __m128i c16_p82_p90   = _mm_set1_epi32(0x0052005A); //column 1
    const __m128i c16_p46_p67   = _mm_set1_epi32(0x002E0043);
    const __m128i c16_n04_p22   = _mm_set1_epi32(0xFFFC0016);
    const __m128i c16_n54_n31   = _mm_set1_epi32(0xFFCAFFE1);
    const __m128i c16_n85_n73   = _mm_set1_epi32(0xFFABFFB7);
    const __m128i c16_n88_n90   = _mm_set1_epi32(0xFFA8FFA6);
    const __m128i c16_n61_n78   = _mm_set1_epi32(0xFFC3FFB2);
    const __m128i c16_n13_n38   = _mm_set1_epi32(0xFFF3FFDA);
    const __m128i c16_p67_p88   = _mm_set1_epi32(0x00430058); //column 2
    const __m128i c16_n13_p31   = _mm_set1_epi32(0xFFF3001F);
    const __m128i c16_n82_n54   = _mm_set1_epi32(0xFFAEFFCA);
    const __m128i c16_n78_n90   = _mm_set1_epi32(0xFFB2FFA6);
    const __m128i c16_n04_n46   = _mm_set1_epi32(0xFFFCFFD2);
    const __m128i c16_p73_p38   = _mm_set1_epi32(0x00490026);
    const __m128i c16_p85_p90   = _mm_set1_epi32(0x0055005A);
    const __m128i c16_p22_p61   = _mm_set1_epi32(0x0016003D);
    const __m128i c16_p46_p85   = _mm_set1_epi32(0x002E0055); //column 3
    const __m128i c16_n67_n13   = _mm_set1_epi32(0xFFBDFFF3);
    const __m128i c16_n73_n90   = _mm_set1_epi32(0xFFB7FFA6);
    const __m128i c16_p38_n22   = _mm_set1_epi32(0x0026FFEA);
    const __m128i c16_p88_p82   = _mm_set1_epi32(0x00580052);
    const __m128i c16_n04_p54   = _mm_set1_epi32(0xFFFC0036);
    const __m128i c16_n90_n61   = _mm_set1_epi32(0xFFA6FFC3);
    const __m128i c16_n31_n78   = _mm_set1_epi32(0xFFE1FFB2);
    const __m128i c16_p22_p82   = _mm_set1_epi32(0x00160052); //column 4
    const __m128i c16_n90_n54   = _mm_set1_epi32(0xFFA6FFCA);
    const __m128i c16_p13_n61   = _mm_set1_epi32(0x000DFFC3);
    const __m128i c16_p85_p78   = _mm_set1_epi32(0x0055004E);
    const __m128i c16_n46_p31   = _mm_set1_epi32(0xFFD2001F);
    const __m128i c16_n67_n90   = _mm_set1_epi32(0xFFBDFFA6);
    const __m128i c16_p73_p04   = _mm_set1_epi32(0x00490004);
    const __m128i c16_p38_p88   = _mm_set1_epi32(0x00260058);
    const __m128i c16_n04_p78   = _mm_set1_epi32(0xFFFC004E); //column 5
    const __m128i c16_n73_n82   = _mm_set1_epi32(0xFFB7FFAE);
    const __m128i c16_p85_p13   = _mm_set1_epi32(0x0055000D);
    const __m128i c16_n22_p67   = _mm_set1_epi32(0xFFEA0043);
    const __m128i c16_n61_n88   = _mm_set1_epi32(0xFFC3FFA8);
    const __m128i c16_p90_p31   = _mm_set1_epi32(0x005A001F);
    const __m128i c16_n38_p54   = _mm_set1_epi32(0xFFDA0036);
    const __m128i c16_n46_n90   = _mm_set1_epi32(0xFFD2FFA6);
    const __m128i c16_n31_p73   = _mm_set1_epi32(0xFFE10049); //column 6
    const __m128i c16_n22_n90   = _mm_set1_epi32(0xFFEAFFA6);
    const __m128i c16_p67_p78   = _mm_set1_epi32(0x0043004E);
    const __m128i c16_n90_n38   = _mm_set1_epi32(0xFFA6FFDA);
    const __m128i c16_p82_n13   = _mm_set1_epi32(0x0052FFF3);
    const __m128i c16_n46_p61   = _mm_set1_epi32(0xFFD2003D);
    const __m128i c16_n04_n88   = _mm_set1_epi32(0xFFFCFFA8);
    const __m128i c16_p54_p85   = _mm_set1_epi32(0x00360055);
    const __m128i c16_n54_p67   = _mm_set1_epi32(0xFFCA0043); //column 7
    const __m128i c16_p38_n78   = _mm_set1_epi32(0x0026FFB2);
    const __m128i c16_n22_p85   = _mm_set1_epi32(0xFFEA0055);
    const __m128i c16_p04_n90   = _mm_set1_epi32(0x0004FFA6);
    const __m128i c16_p13_p90   = _mm_set1_epi32(0x000D005A);
    const __m128i c16_n31_n88   = _mm_set1_epi32(0xFFE1FFA8);
    const __m128i c16_p46_p82   = _mm_set1_epi32(0x002E0052);
    const __m128i c16_n61_n73   = _mm_set1_epi32(0xFFC3FFB7);
    const __m128i c16_n73_p61   = _mm_set1_epi32(0xFFB7003D); //column 8
    const __m128i c16_p82_n46   = _mm_set1_epi32(0x0052FFD2);
    const __m128i c16_n88_p31   = _mm_set1_epi32(0xFFA8001F);
    const __m128i c16_p90_n13   = _mm_set1_epi32(0x005AFFF3);
    const __m128i c16_n90_n04   = _mm_set1_epi32(0xFFA6FFFC);
    const __m128i c16_p85_p22   = _mm_set1_epi32(0x00550016);
    const __m128i c16_n78_n38   = _mm_set1_epi32(0xFFB2FFDA);
    const __m128i c16_p67_p54   = _mm_set1_epi32(0x00430036);
    const __m128i c16_n85_p54   = _mm_set1_epi32(0xFFAB0036); //column 9
    const __m128i c16_p88_n04   = _mm_set1_epi32(0x0058FFFC);
    const __m128i c16_n61_n46   = _mm_set1_epi32(0xFFC3FFD2);
    const __m128i c16_p13_p82   = _mm_set1_epi32(0x000D0052);
    const __m128i c16_p38_n90   = _mm_set1_epi32(0x0026FFA6);
    const __m128i c16_n78_p67   = _mm_set1_epi32(0xFFB20043);
    const __m128i c16_p90_n22   = _mm_set1_epi32(0x005AFFEA);
    const __m128i c16_n73_n31   = _mm_set1_epi32(0xFFB7FFE1);
    const __m128i c16_n90_p46   = _mm_set1_epi32(0xFFA6002E); //column 10
    const __m128i c16_p54_p38   = _mm_set1_epi32(0x00360026);
    const __m128i c16_p31_n90   = _mm_set1_epi32(0x001FFFA6);
    const __m128i c16_n88_p61   = _mm_set1_epi32(0xFFA8003D);
    const __m128i c16_p67_p22   = _mm_set1_epi32(0x00430016);
    const __m128i c16_p13_n85   = _mm_set1_epi32(0x000DFFAB);
    const __m128i c16_n82_p73   = _mm_set1_epi32(0xFFAE0049);
    const __m128i c16_p78_p04   = _mm_set1_epi32(0x004E0004);
    const __m128i c16_n88_p38   = _mm_set1_epi32(0xFFA80026); //column 11
    const __m128i c16_n04_p73   = _mm_set1_epi32(0xFFFC0049);
    const __m128i c16_p90_n67   = _mm_set1_epi32(0x005AFFBD);
    const __m128i c16_n31_n46   = _mm_set1_epi32(0xFFE1FFD2);
    const __m128i c16_n78_p85   = _mm_set1_epi32(0xFFB20055);
    const __m128i c16_p61_p13   = _mm_set1_epi32(0x003D000D);
    const __m128i c16_p54_n90   = _mm_set1_epi32(0x0036FFA6);
    const __m128i c16_n82_p22   = _mm_set1_epi32(0xFFAE0016);
    const __m128i c16_n78_p31   = _mm_set1_epi32(0xFFB2001F); //column 12
    const __m128i c16_n61_p90   = _mm_set1_epi32(0xFFC3005A);
    const __m128i c16_p54_p04   = _mm_set1_epi32(0x00360004);
    const __m128i c16_p82_n88   = _mm_set1_epi32(0x0052FFA8);
    const __m128i c16_n22_n38   = _mm_set1_epi32(0xFFEAFFDA);
    const __m128i c16_n90_p73   = _mm_set1_epi32(0xFFA60049);
    const __m128i c16_n13_p67   = _mm_set1_epi32(0xFFF30043);
    const __m128i c16_p85_n46   = _mm_set1_epi32(0x0055FFD2);
    const __m128i c16_n61_p22   = _mm_set1_epi32(0xFFC30016); //column 13
    const __m128i c16_n90_p85   = _mm_set1_epi32(0xFFA60055);
    const __m128i c16_n38_p73   = _mm_set1_epi32(0xFFDA0049);
    const __m128i c16_p46_n04   = _mm_set1_epi32(0x002EFFFC);
    const __m128i c16_p90_n78   = _mm_set1_epi32(0x005AFFB2);
    const __m128i c16_p54_n82   = _mm_set1_epi32(0x0036FFAE);
    const __m128i c16_n31_n13   = _mm_set1_epi32(0xFFE1FFF3);
    const __m128i c16_n88_p67   = _mm_set1_epi32(0xFFA80043);
    const __m128i c16_n38_p13   = _mm_set1_epi32(0xFFDA000D); //column 14
    const __m128i c16_n78_p61   = _mm_set1_epi32(0xFFB2003D);
    const __m128i c16_n90_p88   = _mm_set1_epi32(0xFFA60058);
    const __m128i c16_n73_p85   = _mm_set1_epi32(0xFFB70055);
    const __m128i c16_n31_p54   = _mm_set1_epi32(0xFFE10036);
    const __m128i c16_p22_p04   = _mm_set1_epi32(0x00160004);
    const __m128i c16_p67_n46   = _mm_set1_epi32(0x0043FFD2);
    const __m128i c16_p90_n82   = _mm_set1_epi32(0x005AFFAE);
    const __m128i c16_n13_p04   = _mm_set1_epi32(0xFFF30004); //column 15
    const __m128i c16_n31_p22   = _mm_set1_epi32(0xFFE10016);
    const __m128i c16_n46_p38   = _mm_set1_epi32(0xFFD20026);
    const __m128i c16_n61_p54   = _mm_set1_epi32(0xFFC30036);
    const __m128i c16_n73_p67   = _mm_set1_epi32(0xFFB70043);
    const __m128i c16_n82_p78   = _mm_set1_epi32(0xFFAE004E);
    const __m128i c16_n88_p85   = _mm_set1_epi32(0xFFA80055);
    const __m128i c16_n90_p90   = _mm_set1_epi32(0xFFA6005A);

    //EO
    const __m128i c16_p87_p90   = _mm_set1_epi32(0x0057005A); //row0 87high - 90low address
    const __m128i c16_p70_p80   = _mm_set1_epi32(0x00460050);
    const __m128i c16_p43_p57   = _mm_set1_epi32(0x002B0039);
    const __m128i c16_p09_p25   = _mm_set1_epi32(0x00090019);
    const __m128i c16_p57_p87   = _mm_set1_epi32(0x00390057); //row1
    const __m128i c16_n43_p09   = _mm_set1_epi32(0xFFD50009);
    const __m128i c16_n90_n80   = _mm_set1_epi32(0xFFA6FFB0);
    const __m128i c16_n25_n70   = _mm_set1_epi32(0xFFE7FFBA);
    const __m128i c16_p09_p80   = _mm_set1_epi32(0x00090050); //row2
    const __m128i c16_n87_n70   = _mm_set1_epi32(0xFFA9FFBA);
    const __m128i c16_p57_n25   = _mm_set1_epi32(0x0039FFE7);
    const __m128i c16_p43_p90   = _mm_set1_epi32(0x002B005A);
    const __m128i c16_n43_p70   = _mm_set1_epi32(0xFFD50046); //row3
    const __m128i c16_p09_n87   = _mm_set1_epi32(0x0009FFA9);
    const __m128i c16_p25_p90   = _mm_set1_epi32(0x0019005A);
    const __m128i c16_n57_n80   = _mm_set1_epi32(0xFFC7FFB0);
    const __m128i c16_n80_p57   = _mm_set1_epi32(0xFFB00039); //row4
    const __m128i c16_p90_n25   = _mm_set1_epi32(0x005AFFE7);
    const __m128i c16_n87_n09   = _mm_set1_epi32(0xFFA9FFF7);
    const __m128i c16_p70_p43   = _mm_set1_epi32(0x0046002B);
    const __m128i c16_n90_p43   = _mm_set1_epi32(0xFFA6002B); //row5
    const __m128i c16_p25_p57   = _mm_set1_epi32(0x00190039);
    const __m128i c16_p70_n87   = _mm_set1_epi32(0x0046FFA9);
    const __m128i c16_n80_p09   = _mm_set1_epi32(0xFFB00009);
    const __m128i c16_n70_p25   = _mm_set1_epi32(0xFFBA0019); //row6
    const __m128i c16_n80_p90   = _mm_set1_epi32(0xFFB0005A);
    const __m128i c16_p09_p43   = _mm_set1_epi32(0x0009002B);
    const __m128i c16_p87_n57   = _mm_set1_epi32(0x0057FFC7);
    const __m128i c16_n25_p09   = _mm_set1_epi32(0xFFE70009); //row7
    const __m128i c16_n57_p43   = _mm_set1_epi32(0xFFC7002B);
    const __m128i c16_n80_p70   = _mm_set1_epi32(0xFFB00046);
    const __m128i c16_n90_p87   = _mm_set1_epi32(0xFFA60057);
    //EEO
    const __m128i c16_p75_p89   = _mm_set1_epi32(0x004B0059);
    const __m128i c16_p18_p50   = _mm_set1_epi32(0x00120032);
    const __m128i c16_n18_p75   = _mm_set1_epi32(0xFFEE004B);
    const __m128i c16_n50_n89   = _mm_set1_epi32(0xFFCEFFA7);
    const __m128i c16_n89_p50   = _mm_set1_epi32(0xFFA70032);
    const __m128i c16_p75_p18   = _mm_set1_epi32(0x004B0012);
    const __m128i c16_n50_p18   = _mm_set1_epi32(0xFFCE0012);
    const __m128i c16_n89_p75   = _mm_set1_epi32(0xFFA7004B);
    //EEEO
    const __m128i c16_p36_p83   = _mm_set1_epi32(0x00240053);
    const __m128i c16_n83_p36   = _mm_set1_epi32(0xFFAD0024);
    //EEEE
    const __m128i c16_n64_p64   = _mm_set1_epi32(0xFFC00040);
    const __m128i c16_p64_p64   = _mm_set1_epi32(0x00400040);
    __m128i c32_rnd             = _mm_set1_epi32(ADD1);

    int nShift = SHIFT1;

    // DCT1
    __m128i in00[4], in01[4], in02[4], in03[4], in04[4], in05[4], in06[4], in07[4], in08[4], in09[4], in10[4], in11[4], in12[4], in13[4], in14[4], in15[4];
    __m128i in16[4], in17[4], in18[4], in19[4], in20[4], in21[4], in22[4], in23[4], in24[4], in25[4], in26[4], in27[4], in28[4], in29[4], in30[4], in31[4];
    __m128i res00[4], res01[4], res02[4], res03[4], res04[4], res05[4], res06[4], res07[4], res08[4], res09[4], res10[4], res11[4], res12[4], res13[4], res14[4], res15[4];
    __m128i res16[4], res17[4], res18[4], res19[4], res20[4], res21[4], res22[4], res23[4], res24[4], res25[4], res26[4], res27[4], res28[4], res29[4], res30[4], res31[4];

    for (int i = 0; i < 4; i++)
    {
        const int offset = (i << 3);
        in00[i]  = _mm_loadu_si128((const __m128i*)&src[0  * 32 + offset]);
        in01[i]  = _mm_loadu_si128((const __m128i*)&src[1  * 32 + offset]);
        in02[i]  = _mm_loadu_si128((const __m128i*)&src[2  * 32 + offset]);
        in03[i]  = _mm_loadu_si128((const __m128i*)&src[3  * 32 + offset]);
        in04[i]  = _mm_loadu_si128((const __m128i*)&src[4  * 32 + offset]);
        in05[i]  = _mm_loadu_si128((const __m128i*)&src[5  * 32 + offset]);
        in06[i]  = _mm_loadu_si128((const __m128i*)&src[6  * 32 + offset]);
        in07[i]  = _mm_loadu_si128((const __m128i*)&src[7  * 32 + offset]);
        in08[i]  = _mm_loadu_si128((const __m128i*)&src[8  * 32 + offset]);
        in09[i]  = _mm_loadu_si128((const __m128i*)&src[9  * 32 + offset]);
        in10[i]  = _mm_loadu_si128((const __m128i*)&src[10 * 32 + offset]);
        in11[i]  = _mm_loadu_si128((const __m128i*)&src[11 * 32 + offset]);
        in12[i]  = _mm_loadu_si128((const __m128i*)&src[12 * 32 + offset]);
        in13[i]  = _mm_loadu_si128((const __m128i*)&src[13 * 32 + offset]);
        in14[i]  = _mm_loadu_si128((const __m128i*)&src[14 * 32 + offset]);
        in15[i]  = _mm_loadu_si128((const __m128i*)&src[15 * 32 + offset]);
        in16[i]  = _mm_loadu_si128((const __m128i*)&src[16 * 32 + offset]);
        in17[i]  = _mm_loadu_si128((const __m128i*)&src[17 * 32 + offset]);
        in18[i]  = _mm_loadu_si128((const __m128i*)&src[18 * 32 + offset]);
        in19[i]  = _mm_loadu_si128((const __m128i*)&src[19 * 32 + offset]);
        in20[i]  = _mm_loadu_si128((const __m128i*)&src[20 * 32 + offset]);
        in21[i]  = _mm_loadu_si128((const __m128i*)&src[21 * 32 + offset]);
        in22[i]  = _mm_loadu_si128((const __m128i*)&src[22 * 32 + offset]);
        in23[i]  = _mm_loadu_si128((const __m128i*)&src[23 * 32 + offset]);
        in24[i]  = _mm_loadu_si128((const __m128i*)&src[24 * 32 + offset]);
        in25[i]  = _mm_loadu_si128((const __m128i*)&src[25 * 32 + offset]);
        in26[i]  = _mm_loadu_si128((const __m128i*)&src[26 * 32 + offset]);
        in27[i]  = _mm_loadu_si128((const __m128i*)&src[27 * 32 + offset]);
        in28[i]  = _mm_loadu_si128((const __m128i*)&src[28 * 32 + offset]);
        in29[i]  = _mm_loadu_si128((const __m128i*)&src[29 * 32 + offset]);
        in30[i]  = _mm_loadu_si128((const __m128i*)&src[30 * 32 + offset]);
        in31[i]  = _mm_loadu_si128((const __m128i*)&src[31 * 32 + offset]);
    }

    for (int pass = 0; pass < 2; pass++)
    {
        if (pass == 1)
        {
            c32_rnd = _mm_set1_epi32(ADD2);
            nShift  = SHIFT2;
        }

        for (int part = 0; part < 4; part++)
        {
            const __m128i T_00_00A = _mm_unpacklo_epi16(in01[part], in03[part]);       // [33 13 32 12 31 11 30 10]
            const __m128i T_00_00B = _mm_unpackhi_epi16(in01[part], in03[part]);       // [37 17 36 16 35 15 34 14]
            const __m128i T_00_01A = _mm_unpacklo_epi16(in05[part], in07[part]);       // [ ]
            const __m128i T_00_01B = _mm_unpackhi_epi16(in05[part], in07[part]);       // [ ]
            const __m128i T_00_02A = _mm_unpacklo_epi16(in09[part], in11[part]);       // [ ]
            const __m128i T_00_02B = _mm_unpackhi_epi16(in09[part], in11[part]);       // [ ]
            const __m128i T_00_03A = _mm_unpacklo_epi16(in13[part], in15[part]);       // [ ]
            const __m128i T_00_03B = _mm_unpackhi_epi16(in13[part], in15[part]);       // [ ]
            const __m128i T_00_04A = _mm_unpacklo_epi16(in17[part], in19[part]);       // [ ]
            const __m128i T_00_04B = _mm_unpackhi_epi16(in17[part], in19[part]);       // [ ]
            const __m128i T_00_05A = _mm_unpacklo_epi16(in21[part], in23[part]);       // [ ]
            const __m128i T_00_05B = _mm_unpackhi_epi16(in21[part], in23[part]);       // [ ]
            const __m128i T_00_06A = _mm_unpacklo_epi16(in25[part], in27[part]);       // [ ]
            const __m128i T_00_06B = _mm_unpackhi_epi16(in25[part], in27[part]);       // [ ]
            const __m128i T_00_07A = _mm_unpacklo_epi16(in29[part], in31[part]);       //
            const __m128i T_00_07B = _mm_unpackhi_epi16(in29[part], in31[part]);       // [ ]

            const __m128i T_00_08A = _mm_unpacklo_epi16(in02[part], in06[part]);       // [ ]
            const __m128i T_00_08B = _mm_unpackhi_epi16(in02[part], in06[part]);       // [ ]
            const __m128i T_00_09A = _mm_unpacklo_epi16(in10[part], in14[part]);       // [ ]
            const __m128i T_00_09B = _mm_unpackhi_epi16(in10[part], in14[part]);       // [ ]
            const __m128i T_00_10A = _mm_unpacklo_epi16(in18[part], in22[part]);       // [ ]
            const __m128i T_00_10B = _mm_unpackhi_epi16(in18[part], in22[part]);       // [ ]
            const __m128i T_00_11A = _mm_unpacklo_epi16(in26[part], in30[part]);       // [ ]
            const __m128i T_00_11B = _mm_unpackhi_epi16(in26[part], in30[part]);       // [ ]

            const __m128i T_00_12A = _mm_unpacklo_epi16(in04[part], in12[part]);       // [ ]
            const __m128i T_00_12B = _mm_unpackhi_epi16(in04[part], in12[part]);       // [ ]
            const __m128i T_00_13A = _mm_unpacklo_epi16(in20[part], in28[part]);       // [ ]
            const __m128i T_00_13B = _mm_unpackhi_epi16(in20[part], in28[part]);       // [ ]

            const __m128i T_00_14A = _mm_unpacklo_epi16(in08[part], in24[part]);       //
            const __m128i T_00_14B = _mm_unpackhi_epi16(in08[part], in24[part]);       // [ ]
            const __m128i T_00_15A = _mm_unpacklo_epi16(in00[part], in16[part]);       //
            const __m128i T_00_15B = _mm_unpackhi_epi16(in00[part], in16[part]);       // [ ]

            __m128i O00A, O01A, O02A, O03A, O04A, O05A, O06A, O07A, O08A, O09A, O10A, O11A, O12A, O13A, O14A, O15A;
            __m128i O00B, O01B, O02B, O03B, O04B, O05B, O06B, O07B, O08B, O09B, O10B, O11B, O12B, O13B, O14B, O15B;
            {
                __m128i T00, T01, T02, T03;
#define COMPUTE_ROW(r0103, r0507, r0911, r1315, r1719, r2123, r2527, r2931, c0103, c0507, c0911, c1315, c1719, c2123, c2527, c2931, row) \
    T00 = _mm_add_epi32(_mm_madd_epi16(r0103, c0103), _mm_madd_epi16(r0507, c0507)); \
    T01 = _mm_add_epi32(_mm_madd_epi16(r0911, c0911), _mm_madd_epi16(r1315, c1315)); \
    T02 = _mm_add_epi32(_mm_madd_epi16(r1719, c1719), _mm_madd_epi16(r2123, c2123)); \
    T03 = _mm_add_epi32(_mm_madd_epi16(r2527, c2527), _mm_madd_epi16(r2931, c2931)); \
    row = _mm_add_epi32(_mm_add_epi32(T00, T01), _mm_add_epi32(T02, T03));

                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_p90_p90, c16_p85_p88, c16_p78_p82, c16_p67_p73, c16_p54_p61, c16_p38_p46, c16_p22_p31, c16_p04_p13, O00A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_p82_p90, c16_p46_p67, c16_n04_p22, c16_n54_n31, c16_n85_n73, c16_n88_n90, c16_n61_n78, c16_n13_n38, O01A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_p67_p88, c16_n13_p31, c16_n82_n54, c16_n78_n90, c16_n04_n46, c16_p73_p38, c16_p85_p90, c16_p22_p61, O02A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_p46_p85, c16_n67_n13, c16_n73_n90, c16_p38_n22, c16_p88_p82, c16_n04_p54, c16_n90_n61, c16_n31_n78, O03A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_p22_p82, c16_n90_n54, c16_p13_n61, c16_p85_p78, c16_n46_p31, c16_n67_n90, c16_p73_p04, c16_p38_p88, O04A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n04_p78, c16_n73_n82, c16_p85_p13, c16_n22_p67, c16_n61_n88, c16_p90_p31, c16_n38_p54, c16_n46_n90, O05A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n31_p73, c16_n22_n90, c16_p67_p78, c16_n90_n38, c16_p82_n13, c16_n46_p61, c16_n04_n88, c16_p54_p85, O06A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n54_p67, c16_p38_n78, c16_n22_p85, c16_p04_n90, c16_p13_p90, c16_n31_n88, c16_p46_p82, c16_n61_n73, O07A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n73_p61, c16_p82_n46, c16_n88_p31, c16_p90_n13, c16_n90_n04, c16_p85_p22, c16_n78_n38, c16_p67_p54, O08A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n85_p54, c16_p88_n04, c16_n61_n46, c16_p13_p82, c16_p38_n90, c16_n78_p67, c16_p90_n22, c16_n73_n31, O09A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n90_p46, c16_p54_p38, c16_p31_n90, c16_n88_p61, c16_p67_p22, c16_p13_n85, c16_n82_p73, c16_p78_p04, O10A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n88_p38, c16_n04_p73, c16_p90_n67, c16_n31_n46, c16_n78_p85, c16_p61_p13, c16_p54_n90, c16_n82_p22, O11A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n78_p31, c16_n61_p90, c16_p54_p04, c16_p82_n88, c16_n22_n38, c16_n90_p73, c16_n13_p67, c16_p85_n46, O12A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n61_p22, c16_n90_p85, c16_n38_p73, c16_p46_n04, c16_p90_n78, c16_p54_n82, c16_n31_n13, c16_n88_p67, O13A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n38_p13, c16_n78_p61, c16_n90_p88, c16_n73_p85, c16_n31_p54, c16_p22_p04, c16_p67_n46, c16_p90_n82, O14A)
                COMPUTE_ROW(T_00_00A, T_00_01A, T_00_02A, T_00_03A, T_00_04A, T_00_05A, T_00_06A, T_00_07A, \
                            c16_n13_p04, c16_n31_p22, c16_n46_p38, c16_n61_p54, c16_n73_p67, c16_n82_p78, c16_n88_p85, c16_n90_p90, O15A)

                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_p90_p90, c16_p85_p88, c16_p78_p82, c16_p67_p73, c16_p54_p61, c16_p38_p46, c16_p22_p31, c16_p04_p13, O00B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_p82_p90, c16_p46_p67, c16_n04_p22, c16_n54_n31, c16_n85_n73, c16_n88_n90, c16_n61_n78, c16_n13_n38, O01B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_p67_p88, c16_n13_p31, c16_n82_n54, c16_n78_n90, c16_n04_n46, c16_p73_p38, c16_p85_p90, c16_p22_p61, O02B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_p46_p85, c16_n67_n13, c16_n73_n90, c16_p38_n22, c16_p88_p82, c16_n04_p54, c16_n90_n61, c16_n31_n78, O03B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_p22_p82, c16_n90_n54, c16_p13_n61, c16_p85_p78, c16_n46_p31, c16_n67_n90, c16_p73_p04, c16_p38_p88, O04B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n04_p78, c16_n73_n82, c16_p85_p13, c16_n22_p67, c16_n61_n88, c16_p90_p31, c16_n38_p54, c16_n46_n90, O05B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n31_p73, c16_n22_n90, c16_p67_p78, c16_n90_n38, c16_p82_n13, c16_n46_p61, c16_n04_n88, c16_p54_p85, O06B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n54_p67, c16_p38_n78, c16_n22_p85, c16_p04_n90, c16_p13_p90, c16_n31_n88, c16_p46_p82, c16_n61_n73, O07B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n73_p61, c16_p82_n46, c16_n88_p31, c16_p90_n13, c16_n90_n04, c16_p85_p22, c16_n78_n38, c16_p67_p54, O08B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n85_p54, c16_p88_n04, c16_n61_n46, c16_p13_p82, c16_p38_n90, c16_n78_p67, c16_p90_n22, c16_n73_n31, O09B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n90_p46, c16_p54_p38, c16_p31_n90, c16_n88_p61, c16_p67_p22, c16_p13_n85, c16_n82_p73, c16_p78_p04, O10B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n88_p38, c16_n04_p73, c16_p90_n67, c16_n31_n46, c16_n78_p85, c16_p61_p13, c16_p54_n90, c16_n82_p22, O11B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n78_p31, c16_n61_p90, c16_p54_p04, c16_p82_n88, c16_n22_n38, c16_n90_p73, c16_n13_p67, c16_p85_n46, O12B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n61_p22, c16_n90_p85, c16_n38_p73, c16_p46_n04, c16_p90_n78, c16_p54_n82, c16_n31_n13, c16_n88_p67, O13B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n38_p13, c16_n78_p61, c16_n90_p88, c16_n73_p85, c16_n31_p54, c16_p22_p04, c16_p67_n46, c16_p90_n82, O14B)
                COMPUTE_ROW(T_00_00B, T_00_01B, T_00_02B, T_00_03B, T_00_04B, T_00_05B, T_00_06B, T_00_07B, \
                            c16_n13_p04, c16_n31_p22, c16_n46_p38, c16_n61_p54, c16_n73_p67, c16_n82_p78, c16_n88_p85, c16_n90_p90, O15B)

#undef COMPUTE_ROW
            }

            __m128i EO0A, EO1A, EO2A, EO3A, EO4A, EO5A, EO6A, EO7A;
            __m128i EO0B, EO1B, EO2B, EO3B, EO4B, EO5B, EO6B, EO7B;
            {
                __m128i T00, T01;
#define COMPUTE_ROW(row0206, row1014, row1822, row2630, c0206, c1014, c1822, c2630, row) \
    T00 = _mm_add_epi32(_mm_madd_epi16(row0206, c0206), _mm_madd_epi16(row1014, c1014)); \
    T01 = _mm_add_epi32(_mm_madd_epi16(row1822, c1822), _mm_madd_epi16(row2630, c2630)); \
    row = _mm_add_epi32(T00, T01);

                COMPUTE_ROW(T_00_08A, T_00_09A, T_00_10A, T_00_11A, c16_p87_p90, c16_p70_p80, c16_p43_p57, c16_p09_p25, EO0A)
                COMPUTE_ROW(T_00_08A, T_00_09A, T_00_10A, T_00_11A, c16_p57_p87, c16_n43_p09, c16_n90_n80, c16_n25_n70, EO1A)
                COMPUTE_ROW(T_00_08A, T_00_09A, T_00_10A, T_00_11A, c16_p09_p80, c16_n87_n70, c16_p57_n25, c16_p43_p90, EO2A)
                COMPUTE_ROW(T_00_08A, T_00_09A, T_00_10A, T_00_11A, c16_n43_p70, c16_p09_n87, c16_p25_p90, c16_n57_n80, EO3A)
                COMPUTE_ROW(T_00_08A, T_00_09A, T_00_10A, T_00_11A, c16_n80_p57, c16_p90_n25, c16_n87_n09, c16_p70_p43, EO4A)
                COMPUTE_ROW(T_00_08A, T_00_09A, T_00_10A, T_00_11A, c16_n90_p43, c16_p25_p57, c16_p70_n87, c16_n80_p09, EO5A)
                COMPUTE_ROW(T_00_08A, T_00_09A, T_00_10A, T_00_11A, c16_n70_p25, c16_n80_p90, c16_p09_p43, c16_p87_n57, EO6A)
                COMPUTE_ROW(T_00_08A, T_00_09A, T_00_10A, T_00_11A, c16_n25_p09, c16_n57_p43, c16_n80_p70, c16_n90_p87, EO7A)

                COMPUTE_ROW(T_00_08B, T_00_09B, T_00_10B, T_00_11B, c16_p87_p90, c16_p70_p80, c16_p43_p57, c16_p09_p25, EO0B)
                COMPUTE_ROW(T_00_08B, T_00_09B, T_00_10B, T_00_11B, c16_p57_p87, c16_n43_p09, c16_n90_n80, c16_n25_n70, EO1B)
                COMPUTE_ROW(T_00_08B, T_00_09B, T_00_10B, T_00_11B, c16_p09_p80, c16_n87_n70, c16_p57_n25, c16_p43_p90, EO2B)
                COMPUTE_ROW(T_00_08B, T_00_09B, T_00_10B, T_00_11B, c16_n43_p70, c16_p09_n87, c16_p25_p90, c16_n57_n80, EO3B)
                COMPUTE_ROW(T_00_08B, T_00_09B, T_00_10B, T_00_11B, c16_n80_p57, c16_p90_n25, c16_n87_n09, c16_p70_p43, EO4B)
                COMPUTE_ROW(T_00_08B, T_00_09B, T_00_10B, T_00_11B, c16_n90_p43, c16_p25_p57, c16_p70_n87, c16_n80_p09, EO5B)
                COMPUTE_ROW(T_00_08B, T_00_09B, T_00_10B, T_00_11B, c16_n70_p25, c16_n80_p90, c16_p09_p43, c16_p87_n57, EO6B)
                COMPUTE_ROW(T_00_08B, T_00_09B, T_00_10B, T_00_11B, c16_n25_p09, c16_n57_p43, c16_n80_p70, c16_n90_p87, EO7B)
#undef COMPUTE_ROW
            }

            const __m128i EEO0A = _mm_add_epi32(_mm_madd_epi16(T_00_12A, c16_p75_p89), _mm_madd_epi16(T_00_13A, c16_p18_p50)); // EEO0
            const __m128i EEO0B = _mm_add_epi32(_mm_madd_epi16(T_00_12B, c16_p75_p89), _mm_madd_epi16(T_00_13B, c16_p18_p50));
            const __m128i EEO1A = _mm_add_epi32(_mm_madd_epi16(T_00_12A, c16_n18_p75), _mm_madd_epi16(T_00_13A, c16_n50_n89)); // EEO1
            const __m128i EEO1B = _mm_add_epi32(_mm_madd_epi16(T_00_12B, c16_n18_p75), _mm_madd_epi16(T_00_13B, c16_n50_n89));
            const __m128i EEO2A = _mm_add_epi32(_mm_madd_epi16(T_00_12A, c16_n89_p50), _mm_madd_epi16(T_00_13A, c16_p75_p18)); // EEO2
            const __m128i EEO2B = _mm_add_epi32(_mm_madd_epi16(T_00_12B, c16_n89_p50), _mm_madd_epi16(T_00_13B, c16_p75_p18));
            const __m128i EEO3A = _mm_add_epi32(_mm_madd_epi16(T_00_12A, c16_n50_p18), _mm_madd_epi16(T_00_13A, c16_n89_p75)); // EEO3
            const __m128i EEO3B = _mm_add_epi32(_mm_madd_epi16(T_00_12B, c16_n50_p18), _mm_madd_epi16(T_00_13B, c16_n89_p75));

            const __m128i EEEO0A = _mm_madd_epi16(T_00_14A, c16_p36_p83);
            const __m128i EEEO0B = _mm_madd_epi16(T_00_14B, c16_p36_p83);
            const __m128i EEEO1A = _mm_madd_epi16(T_00_14A, c16_n83_p36);
            const __m128i EEEO1B = _mm_madd_epi16(T_00_14B, c16_n83_p36);

            const __m128i EEEE0A = _mm_madd_epi16(T_00_15A, c16_p64_p64);
            const __m128i EEEE0B = _mm_madd_epi16(T_00_15B, c16_p64_p64);
            const __m128i EEEE1A = _mm_madd_epi16(T_00_15A, c16_n64_p64);
            const __m128i EEEE1B = _mm_madd_epi16(T_00_15B, c16_n64_p64);

            const __m128i EEE0A = _mm_add_epi32(EEEE0A, EEEO0A);          // EEE0 = EEEE0 + EEEO0
            const __m128i EEE0B = _mm_add_epi32(EEEE0B, EEEO0B);
            const __m128i EEE1A = _mm_add_epi32(EEEE1A, EEEO1A);          // EEE1 = EEEE1 + EEEO1
            const __m128i EEE1B = _mm_add_epi32(EEEE1B, EEEO1B);
            const __m128i EEE3A = _mm_sub_epi32(EEEE0A, EEEO0A);          // EEE2 = EEEE0 - EEEO0
            const __m128i EEE3B = _mm_sub_epi32(EEEE0B, EEEO0B);
            const __m128i EEE2A = _mm_sub_epi32(EEEE1A, EEEO1A);          // EEE3 = EEEE1 - EEEO1
            const __m128i EEE2B = _mm_sub_epi32(EEEE1B, EEEO1B);

            const __m128i EE0A = _mm_add_epi32(EEE0A, EEO0A);          // EE0 = EEE0 + EEO0
            const __m128i EE0B = _mm_add_epi32(EEE0B, EEO0B);
            const __m128i EE1A = _mm_add_epi32(EEE1A, EEO1A);          // EE1 = EEE1 + EEO1
            const __m128i EE1B = _mm_add_epi32(EEE1B, EEO1B);
            const __m128i EE2A = _mm_add_epi32(EEE2A, EEO2A);          // EE2 = EEE0 + EEO0
            const __m128i EE2B = _mm_add_epi32(EEE2B, EEO2B);
            const __m128i EE3A = _mm_add_epi32(EEE3A, EEO3A);          // EE3 = EEE1 + EEO1
            const __m128i EE3B = _mm_add_epi32(EEE3B, EEO3B);
            const __m128i EE7A = _mm_sub_epi32(EEE0A, EEO0A);          // EE7 = EEE0 - EEO0
            const __m128i EE7B = _mm_sub_epi32(EEE0B, EEO0B);
            const __m128i EE6A = _mm_sub_epi32(EEE1A, EEO1A);          // EE6 = EEE1 - EEO1
            const __m128i EE6B = _mm_sub_epi32(EEE1B, EEO1B);
            const __m128i EE5A = _mm_sub_epi32(EEE2A, EEO2A);          // EE5 = EEE0 - EEO0
            const __m128i EE5B = _mm_sub_epi32(EEE2B, EEO2B);
            const __m128i EE4A = _mm_sub_epi32(EEE3A, EEO3A);          // EE4 = EEE1 - EEO1
            const __m128i EE4B = _mm_sub_epi32(EEE3B, EEO3B);

            const __m128i E0A = _mm_add_epi32(EE0A, EO0A);          // E0 = EE0 + EO0
            const __m128i E0B = _mm_add_epi32(EE0B, EO0B);
            const __m128i E1A = _mm_add_epi32(EE1A, EO1A);          // E1 = EE1 + EO1
            const __m128i E1B = _mm_add_epi32(EE1B, EO1B);
            const __m128i E2A = _mm_add_epi32(EE2A, EO2A);          // E2 = EE2 + EO2
            const __m128i E2B = _mm_add_epi32(EE2B, EO2B);
            const __m128i E3A = _mm_add_epi32(EE3A, EO3A);          // E3 = EE3 + EO3
            const __m128i E3B = _mm_add_epi32(EE3B, EO3B);
            const __m128i E4A = _mm_add_epi32(EE4A, EO4A);          // E4 =
            const __m128i E4B = _mm_add_epi32(EE4B, EO4B);
            const __m128i E5A = _mm_add_epi32(EE5A, EO5A);          // E5 =
            const __m128i E5B = _mm_add_epi32(EE5B, EO5B);
            const __m128i E6A = _mm_add_epi32(EE6A, EO6A);          // E6 =
            const __m128i E6B = _mm_add_epi32(EE6B, EO6B);
            const __m128i E7A = _mm_add_epi32(EE7A, EO7A);          // E7 =
            const __m128i E7B = _mm_add_epi32(EE7B, EO7B);
            const __m128i EFA = _mm_sub_epi32(EE0A, EO0A);          // EF = EE0 - EO0
            const __m128i EFB = _mm_sub_epi32(EE0B, EO0B);
            const __m128i EEA = _mm_sub_epi32(EE1A, EO1A);          // EE = EE1 - EO1
            const __m128i EEB = _mm_sub_epi32(EE1B, EO1B);
            const __m128i EDA = _mm_sub_epi32(EE2A, EO2A);          // ED = EE2 - EO2
            const __m128i EDB = _mm_sub_epi32(EE2B, EO2B);
            const __m128i ECA = _mm_sub_epi32(EE3A, EO3A);          // EC = EE3 - EO3
            const __m128i ECB = _mm_sub_epi32(EE3B, EO3B);
            const __m128i EBA = _mm_sub_epi32(EE4A, EO4A);          // EB =
            const __m128i EBB = _mm_sub_epi32(EE4B, EO4B);
            const __m128i EAA = _mm_sub_epi32(EE5A, EO5A);          // EA =
            const __m128i EAB = _mm_sub_epi32(EE5B, EO5B);
            const __m128i E9A = _mm_sub_epi32(EE6A, EO6A);          // E9 =
            const __m128i E9B = _mm_sub_epi32(EE6B, EO6B);
            const __m128i E8A = _mm_sub_epi32(EE7A, EO7A);          // E8 =
            const __m128i E8B = _mm_sub_epi32(EE7B, EO7B);

            const __m128i T10A = _mm_add_epi32(E0A, c32_rnd);         // E0 + rnd
            const __m128i T10B = _mm_add_epi32(E0B, c32_rnd);
            const __m128i T11A = _mm_add_epi32(E1A, c32_rnd);         // E1 + rnd
            const __m128i T11B = _mm_add_epi32(E1B, c32_rnd);
            const __m128i T12A = _mm_add_epi32(E2A, c32_rnd);         // E2 + rnd
            const __m128i T12B = _mm_add_epi32(E2B, c32_rnd);
            const __m128i T13A = _mm_add_epi32(E3A, c32_rnd);         // E3 + rnd
            const __m128i T13B = _mm_add_epi32(E3B, c32_rnd);
            const __m128i T14A = _mm_add_epi32(E4A, c32_rnd);         // E4 + rnd
            const __m128i T14B = _mm_add_epi32(E4B, c32_rnd);
            const __m128i T15A = _mm_add_epi32(E5A, c32_rnd);         // E5 + rnd
            const __m128i T15B = _mm_add_epi32(E5B, c32_rnd);
            const __m128i T16A = _mm_add_epi32(E6A, c32_rnd);         // E6 + rnd
            const __m128i T16B = _mm_add_epi32(E6B, c32_rnd);
            const __m128i T17A = _mm_add_epi32(E7A, c32_rnd);         // E7 + rnd
            const __m128i T17B = _mm_add_epi32(E7B, c32_rnd);
            const __m128i T18A = _mm_add_epi32(E8A, c32_rnd);         // E8 + rnd
            const __m128i T18B = _mm_add_epi32(E8B, c32_rnd);
            const __m128i T19A = _mm_add_epi32(E9A, c32_rnd);         // E9 + rnd
            const __m128i T19B = _mm_add_epi32(E9B, c32_rnd);
            const __m128i T1AA = _mm_add_epi32(EAA, c32_rnd);         // E10 + rnd
            const __m128i T1AB = _mm_add_epi32(EAB, c32_rnd);
            const __m128i T1BA = _mm_add_epi32(EBA, c32_rnd);         // E11 + rnd
            const __m128i T1BB = _mm_add_epi32(EBB, c32_rnd);
            const __m128i T1CA = _mm_add_epi32(ECA, c32_rnd);         // E12 + rnd
            const __m128i T1CB = _mm_add_epi32(ECB, c32_rnd);
            const __m128i T1DA = _mm_add_epi32(EDA, c32_rnd);         // E13 + rnd
            const __m128i T1DB = _mm_add_epi32(EDB, c32_rnd);
            const __m128i T1EA = _mm_add_epi32(EEA, c32_rnd);         // E14 + rnd
            const __m128i T1EB = _mm_add_epi32(EEB, c32_rnd);
            const __m128i T1FA = _mm_add_epi32(EFA, c32_rnd);         // E15 + rnd
            const __m128i T1FB = _mm_add_epi32(EFB, c32_rnd);

            const __m128i T2_00A = _mm_add_epi32(T10A, O00A);          // E0 + O0 + rnd
            const __m128i T2_00B = _mm_add_epi32(T10B, O00B);
            const __m128i T2_01A = _mm_add_epi32(T11A, O01A);          // E1 + O1 + rnd
            const __m128i T2_01B = _mm_add_epi32(T11B, O01B);
            const __m128i T2_02A = _mm_add_epi32(T12A, O02A);          // E2 + O2 + rnd
            const __m128i T2_02B = _mm_add_epi32(T12B, O02B);
            const __m128i T2_03A = _mm_add_epi32(T13A, O03A);          // E3 + O3 + rnd
            const __m128i T2_03B = _mm_add_epi32(T13B, O03B);
            const __m128i T2_04A = _mm_add_epi32(T14A, O04A);          // E4
            const __m128i T2_04B = _mm_add_epi32(T14B, O04B);
            const __m128i T2_05A = _mm_add_epi32(T15A, O05A);          // E5
            const __m128i T2_05B = _mm_add_epi32(T15B, O05B);
            const __m128i T2_06A = _mm_add_epi32(T16A, O06A);          // E6
            const __m128i T2_06B = _mm_add_epi32(T16B, O06B);
            const __m128i T2_07A = _mm_add_epi32(T17A, O07A);          // E7
            const __m128i T2_07B = _mm_add_epi32(T17B, O07B);
            const __m128i T2_08A = _mm_add_epi32(T18A, O08A);          // E8
            const __m128i T2_08B = _mm_add_epi32(T18B, O08B);
            const __m128i T2_09A = _mm_add_epi32(T19A, O09A);          // E9
            const __m128i T2_09B = _mm_add_epi32(T19B, O09B);
            const __m128i T2_10A = _mm_add_epi32(T1AA, O10A);          // E10
            const __m128i T2_10B = _mm_add_epi32(T1AB, O10B);
            const __m128i T2_11A = _mm_add_epi32(T1BA, O11A);          // E11
            const __m128i T2_11B = _mm_add_epi32(T1BB, O11B);
            const __m128i T2_12A = _mm_add_epi32(T1CA, O12A);          // E12
            const __m128i T2_12B = _mm_add_epi32(T1CB, O12B);
            const __m128i T2_13A = _mm_add_epi32(T1DA, O13A);          // E13
            const __m128i T2_13B = _mm_add_epi32(T1DB, O13B);
            const __m128i T2_14A = _mm_add_epi32(T1EA, O14A);          // E14
            const __m128i T2_14B = _mm_add_epi32(T1EB, O14B);
            const __m128i T2_15A = _mm_add_epi32(T1FA, O15A);          // E15
            const __m128i T2_15B = _mm_add_epi32(T1FB, O15B);
            const __m128i T2_31A = _mm_sub_epi32(T10A, O00A);          // E0 - O0 + rnd
            const __m128i T2_31B = _mm_sub_epi32(T10B, O00B);
            const __m128i T2_30A = _mm_sub_epi32(T11A, O01A);          // E1 - O1 + rnd
            const __m128i T2_30B = _mm_sub_epi32(T11B, O01B);
            const __m128i T2_29A = _mm_sub_epi32(T12A, O02A);          // E2 - O2 + rnd
            const __m128i T2_29B = _mm_sub_epi32(T12B, O02B);
            const __m128i T2_28A = _mm_sub_epi32(T13A, O03A);          // E3 - O3 + rnd
            const __m128i T2_28B = _mm_sub_epi32(T13B, O03B);
            const __m128i T2_27A = _mm_sub_epi32(T14A, O04A);          // E4
            const __m128i T2_27B = _mm_sub_epi32(T14B, O04B);
            const __m128i T2_26A = _mm_sub_epi32(T15A, O05A);          // E5
            const __m128i T2_26B = _mm_sub_epi32(T15B, O05B);
            const __m128i T2_25A = _mm_sub_epi32(T16A, O06A);          // E6
            const __m128i T2_25B = _mm_sub_epi32(T16B, O06B);
            const __m128i T2_24A = _mm_sub_epi32(T17A, O07A);          // E7
            const __m128i T2_24B = _mm_sub_epi32(T17B, O07B);
            const __m128i T2_23A = _mm_sub_epi32(T18A, O08A);          //
            const __m128i T2_23B = _mm_sub_epi32(T18B, O08B);
            const __m128i T2_22A = _mm_sub_epi32(T19A, O09A);          //
            const __m128i T2_22B = _mm_sub_epi32(T19B, O09B);
            const __m128i T2_21A = _mm_sub_epi32(T1AA, O10A);          //
            const __m128i T2_21B = _mm_sub_epi32(T1AB, O10B);
            const __m128i T2_20A = _mm_sub_epi32(T1BA, O11A);          //
            const __m128i T2_20B = _mm_sub_epi32(T1BB, O11B);
            const __m128i T2_19A = _mm_sub_epi32(T1CA, O12A);          //
            const __m128i T2_19B = _mm_sub_epi32(T1CB, O12B);
            const __m128i T2_18A = _mm_sub_epi32(T1DA, O13A);          //
            const __m128i T2_18B = _mm_sub_epi32(T1DB, O13B);
            const __m128i T2_17A = _mm_sub_epi32(T1EA, O14A);          //
            const __m128i T2_17B = _mm_sub_epi32(T1EB, O14B);
            const __m128i T2_16A = _mm_sub_epi32(T1FA, O15A);          //
            const __m128i T2_16B = _mm_sub_epi32(T1FB, O15B);

            const __m128i T3_00A = _mm_srai_epi32(T2_00A, nShift);             // [30 20 10 00]
            const __m128i T3_00B = _mm_srai_epi32(T2_00B, nShift);             // [70 60 50 40]
            const __m128i T3_01A = _mm_srai_epi32(T2_01A, nShift);             // [31 21 11 01]
            const __m128i T3_01B = _mm_srai_epi32(T2_01B, nShift);             // [71 61 51 41]
            const __m128i T3_02A = _mm_srai_epi32(T2_02A, nShift);             // [32 22 12 02]
            const __m128i T3_02B = _mm_srai_epi32(T2_02B, nShift);             // [72 62 52 42]
            const __m128i T3_03A = _mm_srai_epi32(T2_03A, nShift);             // [33 23 13 03]
            const __m128i T3_03B = _mm_srai_epi32(T2_03B, nShift);             // [73 63 53 43]
            const __m128i T3_04A = _mm_srai_epi32(T2_04A, nShift);             // [33 24 14 04]
            const __m128i T3_04B = _mm_srai_epi32(T2_04B, nShift);             // [74 64 54 44]
            const __m128i T3_05A = _mm_srai_epi32(T2_05A, nShift);             // [35 25 15 05]
            const __m128i T3_05B = _mm_srai_epi32(T2_05B, nShift);             // [75 65 55 45]
            const __m128i T3_06A = _mm_srai_epi32(T2_06A, nShift);             // [36 26 16 06]
            const __m128i T3_06B = _mm_srai_epi32(T2_06B, nShift);             // [76 66 56 46]
            const __m128i T3_07A = _mm_srai_epi32(T2_07A, nShift);             // [37 27 17 07]
            const __m128i T3_07B = _mm_srai_epi32(T2_07B, nShift);             // [77 67 57 47]
            const __m128i T3_08A = _mm_srai_epi32(T2_08A, nShift);             // [30 20 10 00] x8
            const __m128i T3_08B = _mm_srai_epi32(T2_08B, nShift);             // [70 60 50 40]
            const __m128i T3_09A = _mm_srai_epi32(T2_09A, nShift);             // [31 21 11 01] x9
            const __m128i T3_09B = _mm_srai_epi32(T2_09B, nShift);             // [71 61 51 41]
            const __m128i T3_10A = _mm_srai_epi32(T2_10A, nShift);             // [32 22 12 02] xA
            const __m128i T3_10B = _mm_srai_epi32(T2_10B, nShift);             // [72 62 52 42]
            const __m128i T3_11A = _mm_srai_epi32(T2_11A, nShift);             // [33 23 13 03] xB
            const __m128i T3_11B = _mm_srai_epi32(T2_11B, nShift);             // [73 63 53 43]
            const __m128i T3_12A = _mm_srai_epi32(T2_12A, nShift);             // [33 24 14 04] xC
            const __m128i T3_12B = _mm_srai_epi32(T2_12B, nShift);             // [74 64 54 44]
            const __m128i T3_13A = _mm_srai_epi32(T2_13A, nShift);             // [35 25 15 05] xD
            const __m128i T3_13B = _mm_srai_epi32(T2_13B, nShift);             // [75 65 55 45]
            const __m128i T3_14A = _mm_srai_epi32(T2_14A, nShift);             // [36 26 16 06] xE
            const __m128i T3_14B = _mm_srai_epi32(T2_14B, nShift);             // [76 66 56 46]
            const __m128i T3_15A = _mm_srai_epi32(T2_15A, nShift);             // [37 27 17 07] xF
            const __m128i T3_15B = _mm_srai_epi32(T2_15B, nShift);             // [77 67 57 47]

            const __m128i T3_16A = _mm_srai_epi32(T2_16A, nShift);             // [30 20 10 00]
            const __m128i T3_16B = _mm_srai_epi32(T2_16B, nShift);             // [70 60 50 40]
            const __m128i T3_17A = _mm_srai_epi32(T2_17A, nShift);             // [31 21 11 01]
            const __m128i T3_17B = _mm_srai_epi32(T2_17B, nShift);             // [71 61 51 41]
            const __m128i T3_18A = _mm_srai_epi32(T2_18A, nShift);             // [32 22 12 02]
            const __m128i T3_18B = _mm_srai_epi32(T2_18B, nShift);             // [72 62 52 42]
            const __m128i T3_19A = _mm_srai_epi32(T2_19A, nShift);             // [33 23 13 03]
            const __m128i T3_19B = _mm_srai_epi32(T2_19B, nShift);             // [73 63 53 43]
            const __m128i T3_20A = _mm_srai_epi32(T2_20A, nShift);             // [33 24 14 04]
            const __m128i T3_20B = _mm_srai_epi32(T2_20B, nShift);             // [74 64 54 44]
            const __m128i T3_21A = _mm_srai_epi32(T2_21A, nShift);             // [35 25 15 05]
            const __m128i T3_21B = _mm_srai_epi32(T2_21B, nShift);             // [75 65 55 45]
            const __m128i T3_22A = _mm_srai_epi32(T2_22A, nShift);             // [36 26 16 06]
            const __m128i T3_22B = _mm_srai_epi32(T2_22B, nShift);             // [76 66 56 46]
            const __m128i T3_23A = _mm_srai_epi32(T2_23A, nShift);             // [37 27 17 07]
            const __m128i T3_23B = _mm_srai_epi32(T2_23B, nShift);             // [77 67 57 47]
            const __m128i T3_24A = _mm_srai_epi32(T2_24A, nShift);             // [30 20 10 00] x8
            const __m128i T3_24B = _mm_srai_epi32(T2_24B, nShift);             // [70 60 50 40]
            const __m128i T3_25A = _mm_srai_epi32(T2_25A, nShift);             // [31 21 11 01] x9
            const __m128i T3_25B = _mm_srai_epi32(T2_25B, nShift);             // [71 61 51 41]
            const __m128i T3_26A = _mm_srai_epi32(T2_26A, nShift);             // [32 22 12 02] xA
            const __m128i T3_26B = _mm_srai_epi32(T2_26B, nShift);             // [72 62 52 42]
            const __m128i T3_27A = _mm_srai_epi32(T2_27A, nShift);             // [33 23 13 03] xB
            const __m128i T3_27B = _mm_srai_epi32(T2_27B, nShift);             // [73 63 53 43]
            const __m128i T3_28A = _mm_srai_epi32(T2_28A, nShift);             // [33 24 14 04] xC
            const __m128i T3_28B = _mm_srai_epi32(T2_28B, nShift);             // [74 64 54 44]
            const __m128i T3_29A = _mm_srai_epi32(T2_29A, nShift);             // [35 25 15 05] xD
            const __m128i T3_29B = _mm_srai_epi32(T2_29B, nShift);             // [75 65 55 45]
            const __m128i T3_30A = _mm_srai_epi32(T2_30A, nShift);             // [36 26 16 06] xE
            const __m128i T3_30B = _mm_srai_epi32(T2_30B, nShift);             // [76 66 56 46]
            const __m128i T3_31A = _mm_srai_epi32(T2_31A, nShift);             // [37 27 17 07] xF
            const __m128i T3_31B = _mm_srai_epi32(T2_31B, nShift);             // [77 67 57 47]

            res00[part]  = _mm_packs_epi32(T3_00A, T3_00B);        // [70 60 50 40 30 20 10 00]
            res01[part]  = _mm_packs_epi32(T3_01A, T3_01B);        // [71 61 51 41 31 21 11 01]
            res02[part]  = _mm_packs_epi32(T3_02A, T3_02B);        // [72 62 52 42 32 22 12 02]
            res03[part]  = _mm_packs_epi32(T3_03A, T3_03B);        // [73 63 53 43 33 23 13 03]
            res04[part]  = _mm_packs_epi32(T3_04A, T3_04B);        // [74 64 54 44 34 24 14 04]
            res05[part]  = _mm_packs_epi32(T3_05A, T3_05B);        // [75 65 55 45 35 25 15 05]
            res06[part]  = _mm_packs_epi32(T3_06A, T3_06B);        // [76 66 56 46 36 26 16 06]
            res07[part]  = _mm_packs_epi32(T3_07A, T3_07B);        // [77 67 57 47 37 27 17 07]
            res08[part]  = _mm_packs_epi32(T3_08A, T3_08B);        // [A0 ... 80]
            res09[part]  = _mm_packs_epi32(T3_09A, T3_09B);        // [A1 ... 81]
            res10[part]  = _mm_packs_epi32(T3_10A, T3_10B);        // [A2 ... 82]
            res11[part]  = _mm_packs_epi32(T3_11A, T3_11B);        // [A3 ... 83]
            res12[part]  = _mm_packs_epi32(T3_12A, T3_12B);        // [A4 ... 84]
            res13[part]  = _mm_packs_epi32(T3_13A, T3_13B);        // [A5 ... 85]
            res14[part]  = _mm_packs_epi32(T3_14A, T3_14B);        // [A6 ... 86]
            res15[part]  = _mm_packs_epi32(T3_15A, T3_15B);        // [A7 ... 87]
            res16[part]  = _mm_packs_epi32(T3_16A, T3_16B);
            res17[part]  = _mm_packs_epi32(T3_17A, T3_17B);
            res18[part]  = _mm_packs_epi32(T3_18A, T3_18B);
            res19[part]  = _mm_packs_epi32(T3_19A, T3_19B);
            res20[part]  = _mm_packs_epi32(T3_20A, T3_20B);
            res21[part]  = _mm_packs_epi32(T3_21A, T3_21B);
            res22[part]  = _mm_packs_epi32(T3_22A, T3_22B);
            res23[part]  = _mm_packs_epi32(T3_23A, T3_23B);
            res24[part]  = _mm_packs_epi32(T3_24A, T3_24B);
            res25[part]  = _mm_packs_epi32(T3_25A, T3_25B);
            res26[part]  = _mm_packs_epi32(T3_26A, T3_26B);
            res27[part]  = _mm_packs_epi32(T3_27A, T3_27B);
            res28[part]  = _mm_packs_epi32(T3_28A, T3_28B);
            res29[part]  = _mm_packs_epi32(T3_29A, T3_29B);
            res30[part]  = _mm_packs_epi32(T3_30A, T3_30B);
            res31[part]  = _mm_packs_epi32(T3_31A, T3_31B);
        }
        //transpose matrix 8x8 16bit.
        {
            __m128i tr0_0, tr0_1, tr0_2, tr0_3, tr0_4, tr0_5, tr0_6, tr0_7;
            __m128i tr1_0, tr1_1, tr1_2, tr1_3, tr1_4, tr1_5, tr1_6, tr1_7;
#define TRANSPOSE_8x8_16BIT(I0, I1, I2, I3, I4, I5, I6, I7, O0, O1, O2, O3, O4, O5, O6, O7) \
    tr0_0 = _mm_unpacklo_epi16(I0, I1); \
    tr0_1 = _mm_unpacklo_epi16(I2, I3); \
    tr0_2 = _mm_unpackhi_epi16(I0, I1); \
    tr0_3 = _mm_unpackhi_epi16(I2, I3); \
    tr0_4 = _mm_unpacklo_epi16(I4, I5); \
    tr0_5 = _mm_unpacklo_epi16(I6, I7); \
    tr0_6 = _mm_unpackhi_epi16(I4, I5); \
    tr0_7 = _mm_unpackhi_epi16(I6, I7); \
    tr1_0 = _mm_unpacklo_epi32(tr0_0, tr0_1); \
    tr1_1 = _mm_unpacklo_epi32(tr0_2, tr0_3); \
    tr1_2 = _mm_unpackhi_epi32(tr0_0, tr0_1); \
    tr1_3 = _mm_unpackhi_epi32(tr0_2, tr0_3); \
    tr1_4 = _mm_unpacklo_epi32(tr0_4, tr0_5); \
    tr1_5 = _mm_unpacklo_epi32(tr0_6, tr0_7); \
    tr1_6 = _mm_unpackhi_epi32(tr0_4, tr0_5); \
    tr1_7 = _mm_unpackhi_epi32(tr0_6, tr0_7); \
    O0 = _mm_unpacklo_epi64(tr1_0, tr1_4); \
    O1 = _mm_unpackhi_epi64(tr1_0, tr1_4); \
    O2 = _mm_unpacklo_epi64(tr1_2, tr1_6); \
    O3 = _mm_unpackhi_epi64(tr1_2, tr1_6); \
    O4 = _mm_unpacklo_epi64(tr1_1, tr1_5); \
    O5 = _mm_unpackhi_epi64(tr1_1, tr1_5); \
    O6 = _mm_unpacklo_epi64(tr1_3, tr1_7); \
    O7 = _mm_unpackhi_epi64(tr1_3, tr1_7); \

            TRANSPOSE_8x8_16BIT(res00[0], res01[0], res02[0], res03[0], res04[0], res05[0], res06[0], res07[0], in00[0], in01[0], in02[0], in03[0], in04[0], in05[0], in06[0], in07[0])
            TRANSPOSE_8x8_16BIT(res00[1], res01[1], res02[1], res03[1], res04[1], res05[1], res06[1], res07[1], in08[0], in09[0], in10[0], in11[0], in12[0], in13[0], in14[0], in15[0])
            TRANSPOSE_8x8_16BIT(res00[2], res01[2], res02[2], res03[2], res04[2], res05[2], res06[2], res07[2], in16[0], in17[0], in18[0], in19[0], in20[0], in21[0], in22[0], in23[0])
            TRANSPOSE_8x8_16BIT(res00[3], res01[3], res02[3], res03[3], res04[3], res05[3], res06[3], res07[3], in24[0], in25[0], in26[0], in27[0], in28[0], in29[0], in30[0], in31[0])

            TRANSPOSE_8x8_16BIT(res08[0], res09[0], res10[0], res11[0], res12[0], res13[0], res14[0], res15[0], in00[1], in01[1], in02[1], in03[1], in04[1], in05[1], in06[1], in07[1])
            TRANSPOSE_8x8_16BIT(res08[1], res09[1], res10[1], res11[1], res12[1], res13[1], res14[1], res15[1], in08[1], in09[1], in10[1], in11[1], in12[1], in13[1], in14[1], in15[1])
            TRANSPOSE_8x8_16BIT(res08[2], res09[2], res10[2], res11[2], res12[2], res13[2], res14[2], res15[2], in16[1], in17[1], in18[1], in19[1], in20[1], in21[1], in22[1], in23[1])
            TRANSPOSE_8x8_16BIT(res08[3], res09[3], res10[3], res11[3], res12[3], res13[3], res14[3], res15[3], in24[1], in25[1], in26[1], in27[1], in28[1], in29[1], in30[1], in31[1])

            TRANSPOSE_8x8_16BIT(res16[0], res17[0], res18[0], res19[0], res20[0], res21[0], res22[0], res23[0], in00[2], in01[2], in02[2], in03[2], in04[2], in05[2], in06[2], in07[2])
            TRANSPOSE_8x8_16BIT(res16[1], res17[1], res18[1], res19[1], res20[1], res21[1], res22[1], res23[1], in08[2], in09[2], in10[2], in11[2], in12[2], in13[2], in14[2], in15[2])
            TRANSPOSE_8x8_16BIT(res16[2], res17[2], res18[2], res19[2], res20[2], res21[2], res22[2], res23[2], in16[2], in17[2], in18[2], in19[2], in20[2], in21[2], in22[2], in23[2])
            TRANSPOSE_8x8_16BIT(res16[3], res17[3], res18[3], res19[3], res20[3], res21[3], res22[3], res23[3], in24[2], in25[2], in26[2], in27[2], in28[2], in29[2], in30[2], in31[2])

            TRANSPOSE_8x8_16BIT(res24[0], res25[0], res26[0], res27[0], res28[0], res29[0], res30[0], res31[0], in00[3], in01[3], in02[3], in03[3], in04[3], in05[3], in06[3], in07[3])
            TRANSPOSE_8x8_16BIT(res24[1], res25[1], res26[1], res27[1], res28[1], res29[1], res30[1], res31[1], in08[3], in09[3], in10[3], in11[3], in12[3], in13[3], in14[3], in15[3])
            TRANSPOSE_8x8_16BIT(res24[2], res25[2], res26[2], res27[2], res28[2], res29[2], res30[2], res31[2], in16[3], in17[3], in18[3], in19[3], in20[3], in21[3], in22[3], in23[3])
            TRANSPOSE_8x8_16BIT(res24[3], res25[3], res26[3], res27[3], res28[3], res29[3], res30[3], res31[3], in24[3], in25[3], in26[3], in27[3], in28[3], in29[3], in30[3], in31[3])

#undef TRANSPOSE_8x8_16BIT
        }
    }

    // Add
    for (int i = 0; i < 2; i++)
    {
#define STORE_LINE(L0, L1, L2, L3, L4, L5, L6, L7, H0, H1, H2, H3, H4, H5, H6, H7, offsetV, offsetH) \
    _mm_storeu_si128((__m128i*)&dst[(0 + (offsetV)) * stride + (offsetH) + 0], L0); \
    _mm_storeu_si128((__m128i*)&dst[(0 + (offsetV)) * stride + (offsetH) + 8], H0); \
    _mm_storeu_si128((__m128i*)&dst[(1 + (offsetV)) * stride + (offsetH) + 0], L1); \
    _mm_storeu_si128((__m128i*)&dst[(1 + (offsetV)) * stride + (offsetH) + 8], H1); \
    _mm_storeu_si128((__m128i*)&dst[(2 + (offsetV)) * stride + (offsetH) + 0], L2); \
    _mm_storeu_si128((__m128i*)&dst[(2 + (offsetV)) * stride + (offsetH) + 8], H2); \
    _mm_storeu_si128((__m128i*)&dst[(3 + (offsetV)) * stride + (offsetH) + 0], L3); \
    _mm_storeu_si128((__m128i*)&dst[(3 + (offsetV)) * stride + (offsetH) + 8], H3); \
    _mm_storeu_si128((__m128i*)&dst[(4 + (offsetV)) * stride + (offsetH) + 0], L4); \
    _mm_storeu_si128((__m128i*)&dst[(4 + (offsetV)) * stride + (offsetH) + 8], H4); \
    _mm_storeu_si128((__m128i*)&dst[(5 + (offsetV)) * stride + (offsetH) + 0], L5); \
    _mm_storeu_si128((__m128i*)&dst[(5 + (offsetV)) * stride + (offsetH) + 8], H5); \
    _mm_storeu_si128((__m128i*)&dst[(6 + (offsetV)) * stride + (offsetH) + 0], L6); \
    _mm_storeu_si128((__m128i*)&dst[(6 + (offsetV)) * stride + (offsetH) + 8], H6); \
    _mm_storeu_si128((__m128i*)&dst[(7 + (offsetV)) * stride + (offsetH) + 0], L7); \
    _mm_storeu_si128((__m128i*)&dst[(7 + (offsetV)) * stride + (offsetH) + 8], H7);

        const int k = i * 2;
        STORE_LINE(in00[k], in01[k], in02[k], in03[k], in04[k], in05[k], in06[k], in07[k], in00[k + 1], in01[k + 1], in02[k + 1], in03[k + 1], in04[k + 1], in05[k + 1], in06[k + 1], in07[k + 1], 0, i * 16)
        STORE_LINE(in08[k], in09[k], in10[k], in11[k], in12[k], in13[k], in14[k], in15[k], in08[k + 1], in09[k + 1], in10[k + 1], in11[k + 1], in12[k + 1], in13[k + 1], in14[k + 1], in15[k + 1], 8, i * 16)
        STORE_LINE(in16[k], in17[k], in18[k], in19[k], in20[k], in21[k], in22[k], in23[k], in16[k + 1], in17[k + 1], in18[k + 1], in19[k + 1], in20[k + 1], in21[k + 1], in22[k + 1], in23[k + 1], 16, i * 16)
        STORE_LINE(in24[k], in25[k], in26[k], in27[k], in28[k], in29[k], in30[k], in31[k], in24[k + 1], in25[k + 1], in26[k + 1], in27[k + 1], in28[k + 1], in29[k + 1], in30[k + 1], in31[k + 1], 24, i * 16)
#undef STORE_LINE
    }
}

namespace X265_NS {
void setupIntrinsicDCT_sse3(EncoderPrimitives &p)
{
    /* Note: We have AVX2 assembly for these functions, but since AVX2 is still
     * somewhat rare on end-user PCs we still compile and link these SSE3
     * intrinsic SIMD functions */
    p.cu[BLOCK_8x8].idct   = idct8;
    p.cu[BLOCK_16x16].idct = idct16;
    p.cu[BLOCK_32x32].idct = idct32;
}
}
