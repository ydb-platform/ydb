/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Mandar Gurav <mandar@multicorewareinc.com>
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
#include "contexts.h"   // costCoeffNxN_c
#include "threading.h"  // CLZ

using namespace X265_NS;

#if _MSC_VER
#pragma warning(disable: 4127) // conditional expression is constant, typical for templated functions
#endif

// Fast DST Algorithm. Full matrix multiplication for DST and Fast DST algorithm
// give identical results
static void fastForwardDst(const int16_t* block, int16_t* coeff, int shift)  // input block, output coeff
{
    int c[4];
    int rnd_factor = 1 << (shift - 1);

    for (int i = 0; i < 4; i++)
    {
        // Intermediate Variables
        c[0] = block[4 * i + 0] + block[4 * i + 3];
        c[1] = block[4 * i + 1] + block[4 * i + 3];
        c[2] = block[4 * i + 0] - block[4 * i + 1];
        c[3] = 74 * block[4 * i + 2];

        coeff[i] =      (int16_t)((29 * c[0] + 55 * c[1]  + c[3] + rnd_factor) >> shift);
        coeff[4 + i] =  (int16_t)((74 * (block[4 * i + 0] + block[4 * i + 1] - block[4 * i + 3]) + rnd_factor) >> shift);
        coeff[8 + i] =  (int16_t)((29 * c[2] + 55 * c[0]  - c[3] + rnd_factor) >> shift);
        coeff[12 + i] = (int16_t)((55 * c[2] - 29 * c[1] + c[3] + rnd_factor) >> shift);
    }
}

static void inversedst(const int16_t* tmp, int16_t* block, int shift)  // input tmp, output block
{
    int i, c[4];
    int rnd_factor = 1 << (shift - 1);

    for (i = 0; i < 4; i++)
    {
        // Intermediate Variables
        c[0] = tmp[i] + tmp[8 + i];
        c[1] = tmp[8 + i] + tmp[12 + i];
        c[2] = tmp[i] - tmp[12 + i];
        c[3] = 74 * tmp[4 + i];

        block[4 * i + 0] = (int16_t)x265_clip3(-32768, 32767, (29 * c[0] + 55 * c[1]     + c[3]               + rnd_factor) >> shift);
        block[4 * i + 1] = (int16_t)x265_clip3(-32768, 32767, (55 * c[2] - 29 * c[1]     + c[3]               + rnd_factor) >> shift);
        block[4 * i + 2] = (int16_t)x265_clip3(-32768, 32767, (74 * (tmp[i] - tmp[8 + i]  + tmp[12 + i])      + rnd_factor) >> shift);
        block[4 * i + 3] = (int16_t)x265_clip3(-32768, 32767, (55 * c[0] + 29 * c[2]     - c[3]               + rnd_factor) >> shift);
    }
}

static void partialButterfly16(const int16_t* src, int16_t* dst, int shift, int line)
{
    int j, k;
    int E[8], O[8];
    int EE[4], EO[4];
    int EEE[2], EEO[2];
    int add = 1 << (shift - 1);

    for (j = 0; j < line; j++)
    {
        /* E and O */
        for (k = 0; k < 8; k++)
        {
            E[k] = src[k] + src[15 - k];
            O[k] = src[k] - src[15 - k];
        }

        /* EE and EO */
        for (k = 0; k < 4; k++)
        {
            EE[k] = E[k] + E[7 - k];
            EO[k] = E[k] - E[7 - k];
        }

        /* EEE and EEO */
        EEE[0] = EE[0] + EE[3];
        EEO[0] = EE[0] - EE[3];
        EEE[1] = EE[1] + EE[2];
        EEO[1] = EE[1] - EE[2];

        dst[0] = (int16_t)((g_t16[0][0] * EEE[0] + g_t16[0][1] * EEE[1] + add) >> shift);
        dst[8 * line] = (int16_t)((g_t16[8][0] * EEE[0] + g_t16[8][1] * EEE[1] + add) >> shift);
        dst[4 * line] = (int16_t)((g_t16[4][0] * EEO[0] + g_t16[4][1] * EEO[1] + add) >> shift);
        dst[12 * line] = (int16_t)((g_t16[12][0] * EEO[0] + g_t16[12][1] * EEO[1] + add) >> shift);

        for (k = 2; k < 16; k += 4)
        {
            dst[k * line] = (int16_t)((g_t16[k][0] * EO[0] + g_t16[k][1] * EO[1] + g_t16[k][2] * EO[2] +
                                       g_t16[k][3] * EO[3] + add) >> shift);
        }

        for (k = 1; k < 16; k += 2)
        {
            dst[k * line] =  (int16_t)((g_t16[k][0] * O[0] + g_t16[k][1] * O[1] + g_t16[k][2] * O[2] + g_t16[k][3] * O[3] +
                                        g_t16[k][4] * O[4] + g_t16[k][5] * O[5] + g_t16[k][6] * O[6] + g_t16[k][7] * O[7] +
                                        add) >> shift);
        }

        src += 16;
        dst++;
    }
}

static void partialButterfly32(const int16_t* src, int16_t* dst, int shift, int line)
{
    int j, k;
    int E[16], O[16];
    int EE[8], EO[8];
    int EEE[4], EEO[4];
    int EEEE[2], EEEO[2];
    int add = 1 << (shift - 1);

    for (j = 0; j < line; j++)
    {
        /* E and O*/
        for (k = 0; k < 16; k++)
        {
            E[k] = src[k] + src[31 - k];
            O[k] = src[k] - src[31 - k];
        }

        /* EE and EO */
        for (k = 0; k < 8; k++)
        {
            EE[k] = E[k] + E[15 - k];
            EO[k] = E[k] - E[15 - k];
        }

        /* EEE and EEO */
        for (k = 0; k < 4; k++)
        {
            EEE[k] = EE[k] + EE[7 - k];
            EEO[k] = EE[k] - EE[7 - k];
        }

        /* EEEE and EEEO */
        EEEE[0] = EEE[0] + EEE[3];
        EEEO[0] = EEE[0] - EEE[3];
        EEEE[1] = EEE[1] + EEE[2];
        EEEO[1] = EEE[1] - EEE[2];

        dst[0] = (int16_t)((g_t32[0][0] * EEEE[0] + g_t32[0][1] * EEEE[1] + add) >> shift);
        dst[16 * line] = (int16_t)((g_t32[16][0] * EEEE[0] + g_t32[16][1] * EEEE[1] + add) >> shift);
        dst[8 * line] = (int16_t)((g_t32[8][0] * EEEO[0] + g_t32[8][1] * EEEO[1] + add) >> shift);
        dst[24 * line] = (int16_t)((g_t32[24][0] * EEEO[0] + g_t32[24][1] * EEEO[1] + add) >> shift);
        for (k = 4; k < 32; k += 8)
        {
            dst[k * line] = (int16_t)((g_t32[k][0] * EEO[0] + g_t32[k][1] * EEO[1] + g_t32[k][2] * EEO[2] +
                                       g_t32[k][3] * EEO[3] + add) >> shift);
        }

        for (k = 2; k < 32; k += 4)
        {
            dst[k * line] = (int16_t)((g_t32[k][0] * EO[0] + g_t32[k][1] * EO[1] + g_t32[k][2] * EO[2] +
                                       g_t32[k][3] * EO[3] + g_t32[k][4] * EO[4] + g_t32[k][5] * EO[5] +
                                       g_t32[k][6] * EO[6] + g_t32[k][7] * EO[7] + add) >> shift);
        }

        for (k = 1; k < 32; k += 2)
        {
            dst[k * line] = (int16_t)((g_t32[k][0] * O[0] + g_t32[k][1] * O[1] + g_t32[k][2] * O[2] + g_t32[k][3] * O[3] +
                                       g_t32[k][4] * O[4] + g_t32[k][5] * O[5] + g_t32[k][6] * O[6] + g_t32[k][7] * O[7] +
                                       g_t32[k][8] * O[8] + g_t32[k][9] * O[9] + g_t32[k][10] * O[10] + g_t32[k][11] *
                                       O[11] + g_t32[k][12] * O[12] + g_t32[k][13] * O[13] + g_t32[k][14] * O[14] +
                                       g_t32[k][15] * O[15] + add) >> shift);
        }

        src += 32;
        dst++;
    }
}

static void partialButterfly8(const int16_t* src, int16_t* dst, int shift, int line)
{
    int j, k;
    int E[4], O[4];
    int EE[2], EO[2];
    int add = 1 << (shift - 1);

    for (j = 0; j < line; j++)
    {
        /* E and O*/
        for (k = 0; k < 4; k++)
        {
            E[k] = src[k] + src[7 - k];
            O[k] = src[k] - src[7 - k];
        }

        /* EE and EO */
        EE[0] = E[0] + E[3];
        EO[0] = E[0] - E[3];
        EE[1] = E[1] + E[2];
        EO[1] = E[1] - E[2];

        dst[0] = (int16_t)((g_t8[0][0] * EE[0] + g_t8[0][1] * EE[1] + add) >> shift);
        dst[4 * line] = (int16_t)((g_t8[4][0] * EE[0] + g_t8[4][1] * EE[1] + add) >> shift);
        dst[2 * line] = (int16_t)((g_t8[2][0] * EO[0] + g_t8[2][1] * EO[1] + add) >> shift);
        dst[6 * line] = (int16_t)((g_t8[6][0] * EO[0] + g_t8[6][1] * EO[1] + add) >> shift);

        dst[line] = (int16_t)((g_t8[1][0] * O[0] + g_t8[1][1] * O[1] + g_t8[1][2] * O[2] + g_t8[1][3] * O[3] + add) >> shift);
        dst[3 * line] = (int16_t)((g_t8[3][0] * O[0] + g_t8[3][1] * O[1] + g_t8[3][2] * O[2] + g_t8[3][3] * O[3] + add) >> shift);
        dst[5 * line] = (int16_t)((g_t8[5][0] * O[0] + g_t8[5][1] * O[1] + g_t8[5][2] * O[2] + g_t8[5][3] * O[3] + add) >> shift);
        dst[7 * line] = (int16_t)((g_t8[7][0] * O[0] + g_t8[7][1] * O[1] + g_t8[7][2] * O[2] + g_t8[7][3] * O[3] + add) >> shift);

        src += 8;
        dst++;
    }
}

static void partialButterflyInverse4(const int16_t* src, int16_t* dst, int shift, int line)
{
    int j;
    int E[2], O[2];
    int add = 1 << (shift - 1);

    for (j = 0; j < line; j++)
    {
        /* Utilizing symmetry properties to the maximum to minimize the number of multiplications */
        O[0] = g_t4[1][0] * src[line] + g_t4[3][0] * src[3 * line];
        O[1] = g_t4[1][1] * src[line] + g_t4[3][1] * src[3 * line];
        E[0] = g_t4[0][0] * src[0] + g_t4[2][0] * src[2 * line];
        E[1] = g_t4[0][1] * src[0] + g_t4[2][1] * src[2 * line];

        /* Combining even and odd terms at each hierarchy levels to calculate the final spatial domain vector */
        dst[0] = (int16_t)(x265_clip3(-32768, 32767, (E[0] + O[0] + add) >> shift));
        dst[1] = (int16_t)(x265_clip3(-32768, 32767, (E[1] + O[1] + add) >> shift));
        dst[2] = (int16_t)(x265_clip3(-32768, 32767, (E[1] - O[1] + add) >> shift));
        dst[3] = (int16_t)(x265_clip3(-32768, 32767, (E[0] - O[0] + add) >> shift));

        src++;
        dst += 4;
    }
}

static void partialButterflyInverse8(const int16_t* src, int16_t* dst, int shift, int line)
{
    int j, k;
    int E[4], O[4];
    int EE[2], EO[2];
    int add = 1 << (shift - 1);

    for (j = 0; j < line; j++)
    {
        /* Utilizing symmetry properties to the maximum to minimize the number of multiplications */
        for (k = 0; k < 4; k++)
        {
            O[k] = g_t8[1][k] * src[line] + g_t8[3][k] * src[3 * line] + g_t8[5][k] * src[5 * line] + g_t8[7][k] * src[7 * line];
        }

        EO[0] = g_t8[2][0] * src[2 * line] + g_t8[6][0] * src[6 * line];
        EO[1] = g_t8[2][1] * src[2 * line] + g_t8[6][1] * src[6 * line];
        EE[0] = g_t8[0][0] * src[0] + g_t8[4][0] * src[4 * line];
        EE[1] = g_t8[0][1] * src[0] + g_t8[4][1] * src[4 * line];

        /* Combining even and odd terms at each hierarchy levels to calculate the final spatial domain vector */
        E[0] = EE[0] + EO[0];
        E[3] = EE[0] - EO[0];
        E[1] = EE[1] + EO[1];
        E[2] = EE[1] - EO[1];
        for (k = 0; k < 4; k++)
        {
            dst[k] = (int16_t)x265_clip3(-32768, 32767, (E[k] + O[k] + add) >> shift);
            dst[k + 4] = (int16_t)x265_clip3(-32768, 32767, (E[3 - k] - O[3 - k] + add) >> shift);
        }

        src++;
        dst += 8;
    }
}

static void partialButterflyInverse16(const int16_t* src, int16_t* dst, int shift, int line)
{
    int j, k;
    int E[8], O[8];
    int EE[4], EO[4];
    int EEE[2], EEO[2];
    int add = 1 << (shift - 1);

    for (j = 0; j < line; j++)
    {
        /* Utilizing symmetry properties to the maximum to minimize the number of multiplications */
        for (k = 0; k < 8; k++)
        {
            O[k] = g_t16[1][k] * src[line] + g_t16[3][k] * src[3 * line] + g_t16[5][k] * src[5 * line] + g_t16[7][k] * src[7 * line] +
                g_t16[9][k] * src[9 * line] + g_t16[11][k] * src[11 * line] + g_t16[13][k] * src[13 * line] + g_t16[15][k] * src[15 * line];
        }

        for (k = 0; k < 4; k++)
        {
            EO[k] = g_t16[2][k] * src[2 * line] + g_t16[6][k] * src[6 * line] + g_t16[10][k] * src[10 * line] + g_t16[14][k] * src[14 * line];
        }

        EEO[0] = g_t16[4][0] * src[4 * line] + g_t16[12][0] * src[12 * line];
        EEE[0] = g_t16[0][0] * src[0] + g_t16[8][0] * src[8 * line];
        EEO[1] = g_t16[4][1] * src[4 * line] + g_t16[12][1] * src[12 * line];
        EEE[1] = g_t16[0][1] * src[0] + g_t16[8][1] * src[8 * line];

        /* Combining even and odd terms at each hierarchy levels to calculate the final spatial domain vector */
        for (k = 0; k < 2; k++)
        {
            EE[k] = EEE[k] + EEO[k];
            EE[k + 2] = EEE[1 - k] - EEO[1 - k];
        }

        for (k = 0; k < 4; k++)
        {
            E[k] = EE[k] + EO[k];
            E[k + 4] = EE[3 - k] - EO[3 - k];
        }

        for (k = 0; k < 8; k++)
        {
            dst[k]   = (int16_t)x265_clip3(-32768, 32767, (E[k] + O[k] + add) >> shift);
            dst[k + 8] = (int16_t)x265_clip3(-32768, 32767, (E[7 - k] - O[7 - k] + add) >> shift);
        }

        src++;
        dst += 16;
    }
}

static void partialButterflyInverse32(const int16_t* src, int16_t* dst, int shift, int line)
{
    int j, k;
    int E[16], O[16];
    int EE[8], EO[8];
    int EEE[4], EEO[4];
    int EEEE[2], EEEO[2];
    int add = 1 << (shift - 1);

    for (j = 0; j < line; j++)
    {
        /* Utilizing symmetry properties to the maximum to minimize the number of multiplications */
        for (k = 0; k < 16; k++)
        {
            O[k] = g_t32[1][k] * src[line] + g_t32[3][k] * src[3 * line] + g_t32[5][k] * src[5 * line] + g_t32[7][k] * src[7 * line] +
                g_t32[9][k] * src[9 * line] + g_t32[11][k] * src[11 * line] + g_t32[13][k] * src[13 * line] + g_t32[15][k] * src[15 * line] +
                g_t32[17][k] * src[17 * line] + g_t32[19][k] * src[19 * line] + g_t32[21][k] * src[21 * line] + g_t32[23][k] * src[23 * line] +
                g_t32[25][k] * src[25 * line] + g_t32[27][k] * src[27 * line] + g_t32[29][k] * src[29 * line] + g_t32[31][k] * src[31 * line];
        }

        for (k = 0; k < 8; k++)
        {
            EO[k] = g_t32[2][k] * src[2 * line] + g_t32[6][k] * src[6 * line] + g_t32[10][k] * src[10 * line] + g_t32[14][k] * src[14 * line] +
                g_t32[18][k] * src[18 * line] + g_t32[22][k] * src[22 * line] + g_t32[26][k] * src[26 * line] + g_t32[30][k] * src[30 * line];
        }

        for (k = 0; k < 4; k++)
        {
            EEO[k] = g_t32[4][k] * src[4 * line] + g_t32[12][k] * src[12 * line] + g_t32[20][k] * src[20 * line] + g_t32[28][k] * src[28 * line];
        }

        EEEO[0] = g_t32[8][0] * src[8 * line] + g_t32[24][0] * src[24 * line];
        EEEO[1] = g_t32[8][1] * src[8 * line] + g_t32[24][1] * src[24 * line];
        EEEE[0] = g_t32[0][0] * src[0] + g_t32[16][0] * src[16 * line];
        EEEE[1] = g_t32[0][1] * src[0] + g_t32[16][1] * src[16 * line];

        /* Combining even and odd terms at each hierarchy levels to calculate the final spatial domain vector */
        EEE[0] = EEEE[0] + EEEO[0];
        EEE[3] = EEEE[0] - EEEO[0];
        EEE[1] = EEEE[1] + EEEO[1];
        EEE[2] = EEEE[1] - EEEO[1];
        for (k = 0; k < 4; k++)
        {
            EE[k] = EEE[k] + EEO[k];
            EE[k + 4] = EEE[3 - k] - EEO[3 - k];
        }

        for (k = 0; k < 8; k++)
        {
            E[k] = EE[k] + EO[k];
            E[k + 8] = EE[7 - k] - EO[7 - k];
        }

        for (k = 0; k < 16; k++)
        {
            dst[k] = (int16_t)x265_clip3(-32768, 32767, (E[k] + O[k] + add) >> shift);
            dst[k + 16] = (int16_t)x265_clip3(-32768, 32767, (E[15 - k] - O[15 - k] + add) >> shift);
        }

        src++;
        dst += 32;
    }
}

static void partialButterfly4(const int16_t* src, int16_t* dst, int shift, int line)
{
    int j;
    int E[2], O[2];
    int add = 1 << (shift - 1);

    for (j = 0; j < line; j++)
    {
        /* E and O */
        E[0] = src[0] + src[3];
        O[0] = src[0] - src[3];
        E[1] = src[1] + src[2];
        O[1] = src[1] - src[2];

        dst[0] = (int16_t)((g_t4[0][0] * E[0] + g_t4[0][1] * E[1] + add) >> shift);
        dst[2 * line] = (int16_t)((g_t4[2][0] * E[0] + g_t4[2][1] * E[1] + add) >> shift);
        dst[line] = (int16_t)((g_t4[1][0] * O[0] + g_t4[1][1] * O[1] + add) >> shift);
        dst[3 * line] = (int16_t)((g_t4[3][0] * O[0] + g_t4[3][1] * O[1] + add) >> shift);

        src += 4;
        dst++;
    }
}

static void dst4_c(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    const int shift_1st = 1 + X265_DEPTH - 8;
    const int shift_2nd = 8;

    ALIGN_VAR_32(int16_t, coef[4 * 4]);
    ALIGN_VAR_32(int16_t, block[4 * 4]);

    for (int i = 0; i < 4; i++)
    {
        memcpy(&block[i * 4], &src[i * srcStride], 4 * sizeof(int16_t));
    }

    fastForwardDst(block, coef, shift_1st);
    fastForwardDst(coef, dst, shift_2nd);
}

static void dct4_c(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    const int shift_1st = 1 + X265_DEPTH - 8;
    const int shift_2nd = 8;

    ALIGN_VAR_32(int16_t, coef[4 * 4]);
    ALIGN_VAR_32(int16_t, block[4 * 4]);

    for (int i = 0; i < 4; i++)
    {
        memcpy(&block[i * 4], &src[i * srcStride], 4 * sizeof(int16_t));
    }

    partialButterfly4(block, coef, shift_1st, 4);
    partialButterfly4(coef, dst, shift_2nd, 4);
}

static void dct8_c(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    const int shift_1st = 2 + X265_DEPTH - 8;
    const int shift_2nd = 9;

    ALIGN_VAR_32(int16_t, coef[8 * 8]);
    ALIGN_VAR_32(int16_t, block[8 * 8]);

    for (int i = 0; i < 8; i++)
    {
        memcpy(&block[i * 8], &src[i * srcStride], 8 * sizeof(int16_t));
    }

    partialButterfly8(block, coef, shift_1st, 8);
    partialButterfly8(coef, dst, shift_2nd, 8);
}

static void dct16_c(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    const int shift_1st = 3 + X265_DEPTH - 8;
    const int shift_2nd = 10;

    ALIGN_VAR_32(int16_t, coef[16 * 16]);
    ALIGN_VAR_32(int16_t, block[16 * 16]);

    for (int i = 0; i < 16; i++)
    {
        memcpy(&block[i * 16], &src[i * srcStride], 16 * sizeof(int16_t));
    }

    partialButterfly16(block, coef, shift_1st, 16);
    partialButterfly16(coef, dst, shift_2nd, 16);
}

static void dct32_c(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    const int shift_1st = 4 + X265_DEPTH - 8;
    const int shift_2nd = 11;

    ALIGN_VAR_32(int16_t, coef[32 * 32]);
    ALIGN_VAR_32(int16_t, block[32 * 32]);

    for (int i = 0; i < 32; i++)
    {
        memcpy(&block[i * 32], &src[i * srcStride], 32 * sizeof(int16_t));
    }

    partialButterfly32(block, coef, shift_1st, 32);
    partialButterfly32(coef, dst, shift_2nd, 32);
}

static void idst4_c(const int16_t* src, int16_t* dst, intptr_t dstStride)
{
    const int shift_1st = 7;
    const int shift_2nd = 12 - (X265_DEPTH - 8);

    ALIGN_VAR_32(int16_t, coef[4 * 4]);
    ALIGN_VAR_32(int16_t, block[4 * 4]);

    inversedst(src, coef, shift_1st); // Forward DST BY FAST ALGORITHM, block input, coef output
    inversedst(coef, block, shift_2nd); // Forward DST BY FAST ALGORITHM, coef input, coeff output

    for (int i = 0; i < 4; i++)
    {
        memcpy(&dst[i * dstStride], &block[i * 4], 4 * sizeof(int16_t));
    }
}

static void idct4_c(const int16_t* src, int16_t* dst, intptr_t dstStride)
{
    const int shift_1st = 7;
    const int shift_2nd = 12 - (X265_DEPTH - 8);

    ALIGN_VAR_32(int16_t, coef[4 * 4]);
    ALIGN_VAR_32(int16_t, block[4 * 4]);

    partialButterflyInverse4(src, coef, shift_1st, 4); // Forward DST BY FAST ALGORITHM, block input, coef output
    partialButterflyInverse4(coef, block, shift_2nd, 4); // Forward DST BY FAST ALGORITHM, coef input, coeff output

    for (int i = 0; i < 4; i++)
    {
        memcpy(&dst[i * dstStride], &block[i * 4], 4 * sizeof(int16_t));
    }
}

static void idct8_c(const int16_t* src, int16_t* dst, intptr_t dstStride)
{
    const int shift_1st = 7;
    const int shift_2nd = 12 - (X265_DEPTH - 8);

    ALIGN_VAR_32(int16_t, coef[8 * 8]);
    ALIGN_VAR_32(int16_t, block[8 * 8]);

    partialButterflyInverse8(src, coef, shift_1st, 8);
    partialButterflyInverse8(coef, block, shift_2nd, 8);

    for (int i = 0; i < 8; i++)
    {
        memcpy(&dst[i * dstStride], &block[i * 8], 8 * sizeof(int16_t));
    }
}

static void idct16_c(const int16_t* src, int16_t* dst, intptr_t dstStride)
{
    const int shift_1st = 7;
    const int shift_2nd = 12 - (X265_DEPTH - 8);

    ALIGN_VAR_32(int16_t, coef[16 * 16]);
    ALIGN_VAR_32(int16_t, block[16 * 16]);

    partialButterflyInverse16(src, coef, shift_1st, 16);
    partialButterflyInverse16(coef, block, shift_2nd, 16);

    for (int i = 0; i < 16; i++)
    {
        memcpy(&dst[i * dstStride], &block[i * 16], 16 * sizeof(int16_t));
    }
}

static void idct32_c(const int16_t* src, int16_t* dst, intptr_t dstStride)
{
    const int shift_1st = 7;
    const int shift_2nd = 12 - (X265_DEPTH - 8);

    ALIGN_VAR_32(int16_t, coef[32 * 32]);
    ALIGN_VAR_32(int16_t, block[32 * 32]);

    partialButterflyInverse32(src, coef, shift_1st, 32);
    partialButterflyInverse32(coef, block, shift_2nd, 32);

    for (int i = 0; i < 32; i++)
    {
        memcpy(&dst[i * dstStride], &block[i * 32], 32 * sizeof(int16_t));
    }
}

static void dequant_normal_c(const int16_t* quantCoef, int16_t* coef, int num, int scale, int shift)
{
#if HIGH_BIT_DEPTH
    X265_CHECK(scale < 32768 || ((scale & 3) == 0 && shift > (X265_DEPTH - 8)), "dequant invalid scale %d\n", scale);
#else
    // NOTE: maximum of scale is (72 * 256)
    X265_CHECK(scale < 32768, "dequant invalid scale %d\n", scale);
#endif
    X265_CHECK(num <= 32 * 32, "dequant num %d too large\n", num);
    X265_CHECK((num % 8) == 0, "dequant num %d not multiple of 8\n", num);
    X265_CHECK(shift <= 10, "shift too large %d\n", shift);
    X265_CHECK(((intptr_t)coef & 31) == 0, "dequant coef buffer not aligned\n");

    int add, coeffQ;

    add = 1 << (shift - 1);

    for (int n = 0; n < num; n++)
    {
        coeffQ = (quantCoef[n] * scale + add) >> shift;
        coef[n] = (int16_t)x265_clip3(-32768, 32767, coeffQ);
    }
}

static void dequant_scaling_c(const int16_t* quantCoef, const int32_t* deQuantCoef, int16_t* coef, int num, int per, int shift)
{
    X265_CHECK(num <= 32 * 32, "dequant num %d too large\n", num);

    int add, coeffQ;

    shift += 4;

    if (shift > per)
    {
        add = 1 << (shift - per - 1);

        for (int n = 0; n < num; n++)
        {
            coeffQ = ((quantCoef[n] * deQuantCoef[n]) + add) >> (shift - per);
            coef[n] = (int16_t)x265_clip3(-32768, 32767, coeffQ);
        }
    }
    else
    {
        for (int n = 0; n < num; n++)
        {
            coeffQ   = x265_clip3(-32768, 32767, quantCoef[n] * deQuantCoef[n]);
            coef[n] = (int16_t)x265_clip3(-32768, 32767, coeffQ << (per - shift));
        }
    }
}

static uint32_t quant_c(const int16_t* coef, const int32_t* quantCoeff, int32_t* deltaU, int16_t* qCoef, int qBits, int add, int numCoeff)
{
    X265_CHECK(qBits >= 8, "qBits less than 8\n");
    X265_CHECK((numCoeff % 16) == 0, "numCoeff must be multiple of 16\n");
    int qBits8 = qBits - 8;
    uint32_t numSig = 0;

    for (int blockpos = 0; blockpos < numCoeff; blockpos++)
    {
        int level = coef[blockpos];
        int sign  = (level < 0 ? -1 : 1);

        int tmplevel = abs(level) * quantCoeff[blockpos];
        level = ((tmplevel + add) >> qBits);
        deltaU[blockpos] = ((tmplevel - (level << qBits)) >> qBits8);
        if (level)
            ++numSig;
        level *= sign;
        qCoef[blockpos] = (int16_t)x265_clip3(-32768, 32767, level);
    }

    return numSig;
}

static uint32_t nquant_c(const int16_t* coef, const int32_t* quantCoeff, int16_t* qCoef, int qBits, int add, int numCoeff)
{
    X265_CHECK((numCoeff % 16) == 0, "number of quant coeff is not multiple of 4x4\n");
    X265_CHECK((uint32_t)add < ((uint32_t)1 << qBits), "2 ^ qBits less than add\n");
    X265_CHECK(((intptr_t)quantCoeff & 31) == 0, "quantCoeff buffer not aligned\n");

    uint32_t numSig = 0;

    for (int blockpos = 0; blockpos < numCoeff; blockpos++)
    {
        int level = coef[blockpos];
        int sign  = (level < 0 ? -1 : 1);

        int tmplevel = abs(level) * quantCoeff[blockpos];
        level = ((tmplevel + add) >> qBits);
        if (level)
            ++numSig;
        level *= sign;

        // TODO: when we limit range to [-32767, 32767], we can get more performance with output change
        //       But nquant is a little percent in rdoQuant, so I keep old dynamic range for compatible
        qCoef[blockpos] = (int16_t)abs(x265_clip3(-32768, 32767, level));
    }

    return numSig;
}
template<int trSize>
int  count_nonzero_c(const int16_t* quantCoeff)
{
    X265_CHECK(((intptr_t)quantCoeff & 15) == 0, "quant buffer not aligned\n");
    int count = 0;
    int numCoeff = trSize * trSize;
    for (int i = 0; i < numCoeff; i++)
    {
        count += quantCoeff[i] != 0;
    }

    return count;
}

template<int trSize>
uint32_t copy_count(int16_t* coeff, const int16_t* residual, intptr_t resiStride)
{
    uint32_t numSig = 0;
    for (int k = 0; k < trSize; k++)
    {
        for (int j = 0; j < trSize; j++)
        {
            coeff[k * trSize + j] = residual[k * resiStride + j];
            numSig += (residual[k * resiStride + j] != 0);
        }
    }

    return numSig;
}

static void denoiseDct_c(int16_t* dctCoef, uint32_t* resSum, const uint16_t* offset, int numCoeff)
{
    for (int i = 0; i < numCoeff; i++)
    {
        int level = dctCoef[i];
        int sign = level >> 31;
        level = (level + sign) ^ sign;
        resSum[i] += level;
        level -= offset[i];
        dctCoef[i] = (int16_t)(level < 0 ? 0 : (level ^ sign) - sign);
    }
}

static int scanPosLast_c(const uint16_t *scan, const coeff_t *coeff, uint16_t *coeffSign, uint16_t *coeffFlag, uint8_t *coeffNum, int numSig, const uint16_t* /*scanCG4x4*/, const int /*trSize*/)
{
    memset(coeffNum, 0, MLS_GRP_NUM * sizeof(*coeffNum));
    memset(coeffFlag, 0, MLS_GRP_NUM * sizeof(*coeffFlag));
    memset(coeffSign, 0, MLS_GRP_NUM * sizeof(*coeffSign));

    int scanPosLast = 0;
    do
    {
        const uint32_t cgIdx = (uint32_t)scanPosLast >> MLS_CG_SIZE;

        const uint32_t posLast = scan[scanPosLast++];

        const int curCoeff = coeff[posLast];
        const uint32_t isNZCoeff = (curCoeff != 0);
        // get L1 sig map
        // NOTE: the new algorithm is complicated, so I keep reference code here
        //uint32_t posy   = posLast >> log2TrSize;
        //uint32_t posx   = posLast - (posy << log2TrSize);
        //uint32_t blkIdx0 = ((posy >> MLS_CG_LOG2_SIZE) << codingParameters.log2TrSizeCG) + (posx >> MLS_CG_LOG2_SIZE);
        //const uint32_t blkIdx = ((posLast >> (2 * MLS_CG_LOG2_SIZE)) & ~maskPosXY) + ((posLast >> MLS_CG_LOG2_SIZE) & maskPosXY);
        //sigCoeffGroupFlag64 |= ((uint64_t)isNZCoeff << blkIdx);
        numSig -= isNZCoeff;

        // TODO: optimize by instruction BTS
        coeffSign[cgIdx] += (uint16_t)(((uint32_t)curCoeff >> 31) << coeffNum[cgIdx]);
        coeffFlag[cgIdx] = (coeffFlag[cgIdx] << 1) + (uint16_t)isNZCoeff;
        coeffNum[cgIdx] += (uint8_t)isNZCoeff;
    }
    while (numSig > 0);
    return scanPosLast - 1;
}

// NOTE: no defined value on lastNZPosInCG & absSumSign when ALL ZEROS block as input
static uint32_t findPosFirstLast_c(const int16_t *dstCoeff, const intptr_t trSize, const uint16_t scanTbl[16])
{
    int n;

    for (n = SCAN_SET_SIZE - 1; n >= 0; n--)
    {
        const uint32_t idx = scanTbl[n];
        const uint32_t idxY = idx / MLS_CG_SIZE;
        const uint32_t idxX = idx % MLS_CG_SIZE;
        if (dstCoeff[idxY * trSize + idxX])
            break;
    }

    X265_CHECK(n >= -1, "non-zero coeff scan failuare!\n");

    uint32_t lastNZPosInCG = (uint32_t)n;

    for (n = 0; n < SCAN_SET_SIZE; n++)
    {
        const uint32_t idx = scanTbl[n];
        const uint32_t idxY = idx / MLS_CG_SIZE;
        const uint32_t idxX = idx % MLS_CG_SIZE;
        if (dstCoeff[idxY * trSize + idxX])
            break;
    }

    uint32_t firstNZPosInCG = (uint32_t)n;

    uint32_t absSumSign = 0;
    for (n = firstNZPosInCG; n <= (int)lastNZPosInCG; n++)
    {
        const uint32_t idx = scanTbl[n];
        const uint32_t idxY = idx / MLS_CG_SIZE;
        const uint32_t idxX = idx % MLS_CG_SIZE;
        absSumSign += dstCoeff[idxY * trSize + idxX];
    }

    // NOTE: when coeff block all ZERO, the lastNZPosInCG is undefined and firstNZPosInCG is 16
    return ((absSumSign << 31) | (lastNZPosInCG << 8) | firstNZPosInCG);
}


static uint32_t costCoeffNxN_c(const uint16_t *scan, const coeff_t *coeff, intptr_t trSize, uint16_t *absCoeff, const uint8_t *tabSigCtx, uint32_t scanFlagMask, uint8_t *baseCtx, int offset, int scanPosSigOff, int subPosBase)
{
    ALIGN_VAR_32(uint16_t, tmpCoeff[SCAN_SET_SIZE]);
    uint32_t numNonZero = (scanPosSigOff < (SCAN_SET_SIZE - 1) ? 1 : 0);
    uint32_t sum = 0;

    // correct offset to match assembly
    absCoeff -= numNonZero;

    for (int i = 0; i < MLS_CG_SIZE; i++)
    {
        tmpCoeff[i * MLS_CG_SIZE + 0] = (uint16_t)abs(coeff[i * trSize + 0]);
        tmpCoeff[i * MLS_CG_SIZE + 1] = (uint16_t)abs(coeff[i * trSize + 1]);
        tmpCoeff[i * MLS_CG_SIZE + 2] = (uint16_t)abs(coeff[i * trSize + 2]);
        tmpCoeff[i * MLS_CG_SIZE + 3] = (uint16_t)abs(coeff[i * trSize + 3]);
    }

    do
    {
        uint32_t blkPos, sig, ctxSig;
        blkPos = scan[scanPosSigOff];
        const uint32_t posZeroMask = (subPosBase + scanPosSigOff) ? ~0 : 0;
        sig     = scanFlagMask & 1;
        scanFlagMask >>= 1;
        X265_CHECK((uint32_t)(tmpCoeff[blkPos] != 0) == sig, "sign bit mistake\n");
        if ((scanPosSigOff != 0) || (subPosBase == 0) || numNonZero)
        {
            const uint32_t cnt = tabSigCtx[blkPos] + offset;
            ctxSig = cnt & posZeroMask;

            //X265_CHECK(ctxSig == Quant::getSigCtxInc(patternSigCtx, log2TrSize, trSize, codingParameters.scan[subPosBase + scanPosSigOff], bIsLuma, codingParameters.firstSignificanceMapContext), "sigCtx mistake!\n");;
            //encodeBin(sig, baseCtx[ctxSig]);
            const uint32_t mstate = baseCtx[ctxSig];
            const uint32_t mps = mstate & 1;
            const uint32_t stateBits = PFX(entropyStateBits)[mstate ^ sig];
            uint32_t nextState = (stateBits >> 24) + mps;
            if ((mstate ^ sig) == 1)
                nextState = sig;
            X265_CHECK(sbacNext(mstate, sig) == nextState, "nextState check failure\n");
            X265_CHECK(sbacGetEntropyBits(mstate, sig) == (stateBits & 0xFFFFFF), "entropyBits check failure\n");
            baseCtx[ctxSig] = (uint8_t)nextState;
            sum += stateBits;
        }
        assert(numNonZero <= 15);
        assert(blkPos <= 15);
        absCoeff[numNonZero] = tmpCoeff[blkPos];
        numNonZero += sig;
        scanPosSigOff--;
    }
    while(scanPosSigOff >= 0);

    return (sum & 0xFFFFFF);
}

static uint32_t costCoeffRemain_c(uint16_t *absCoeff, int numNonZero, int idx)
{
    uint32_t goRiceParam = 0;

    uint32_t sum = 0;
    int baseLevel = 3;
    do
    {
        if (idx >= C1FLAG_NUMBER)
            baseLevel = 1;

        // TODO: the IDX is not really idx, so this check inactive
        //X265_CHECK(baseLevel == ((idx < C1FLAG_NUMBER) ? (2 + firstCoeff2) : 1), "baseLevel check failurr\n");
        int codeNumber = absCoeff[idx] - baseLevel;

        if (codeNumber >= 0)
        {
            //writeCoefRemainExGolomb(absCoeff[idx] - baseLevel, goRiceParam);
            uint32_t length = 0;

            codeNumber = ((uint32_t)codeNumber >> goRiceParam) - COEF_REMAIN_BIN_REDUCTION;
            if (codeNumber >= 0)
            {
                {
                    unsigned long cidx;
                    CLZ(cidx, codeNumber + 1);
                    length = cidx;
                }
                X265_CHECK((codeNumber != 0) || (length == 0), "length check failure\n");

                codeNumber = (length + length);
            }
            sum += (COEF_REMAIN_BIN_REDUCTION + 1 + goRiceParam + codeNumber);

            if (absCoeff[idx] > (COEF_REMAIN_BIN_REDUCTION << goRiceParam))
                goRiceParam = (goRiceParam + 1) - (goRiceParam >> 2);
            X265_CHECK(goRiceParam <= 4, "goRiceParam check failure\n");
        }
        baseLevel = 2;
        idx++;
    }
    while(idx < numNonZero);

    return sum;
}


static uint32_t costC1C2Flag_c(uint16_t *absCoeff, intptr_t numC1Flag, uint8_t *baseCtxMod, intptr_t ctxOffset)
{
    uint32_t sum = 0;
    uint32_t c1 = 1;
    uint32_t firstC2Idx = 8;
    uint32_t firstC2Flag = 2;
    uint32_t c1Next = 0xFFFFFFFE;

    int idx = 0;
    do
    {
        uint32_t symbol1 = absCoeff[idx] > 1;
        uint32_t symbol2 = absCoeff[idx] > 2;
        //encodeBin(symbol1, baseCtxMod[c1]);
        {
            const uint32_t mstate = baseCtxMod[c1];
            baseCtxMod[c1] = sbacNext(mstate, symbol1);
            sum += sbacGetEntropyBits(mstate, symbol1);
        }

        if (symbol1)
            c1Next = 0;

        if (symbol1 + firstC2Flag == 3)
            firstC2Flag = symbol2;

        if (symbol1 + firstC2Idx == 9)
            firstC2Idx  = idx;

        c1 = (c1Next & 3);
        c1Next >>= 2;
        X265_CHECK(c1 <= 3, "c1 check failure\n");
        idx++;
    }
    while(idx < numC1Flag);

    if (!c1)
    {
        X265_CHECK((firstC2Flag <= 1), "firstC2FlagIdx check failure\n");

        baseCtxMod += ctxOffset;

        //encodeBin(firstC2Flag, baseCtxMod[0]);
        {
            const uint32_t mstate = baseCtxMod[0];
            baseCtxMod[0] = sbacNext(mstate, firstC2Flag);
            sum += sbacGetEntropyBits(mstate, firstC2Flag);
        }
    }

    return (sum & 0x00FFFFFF) + (c1 << 26) + (firstC2Idx << 28);
}

namespace X265_NS {
// x265 private namespace

void setupDCTPrimitives_c(EncoderPrimitives& p)
{
    p.dequant_scaling = dequant_scaling_c;
    p.dequant_normal = dequant_normal_c;
    p.quant = quant_c;
    p.nquant = nquant_c;
    p.dst4x4 = dst4_c;
    p.cu[BLOCK_4x4].dct   = dct4_c;
    p.cu[BLOCK_8x8].dct   = dct8_c;
    p.cu[BLOCK_16x16].dct = dct16_c;
    p.cu[BLOCK_32x32].dct = dct32_c;
    p.idst4x4 = idst4_c;
    p.cu[BLOCK_4x4].idct   = idct4_c;
    p.cu[BLOCK_8x8].idct   = idct8_c;
    p.cu[BLOCK_16x16].idct = idct16_c;
    p.cu[BLOCK_32x32].idct = idct32_c;
    p.denoiseDct = denoiseDct_c;
    p.cu[BLOCK_4x4].count_nonzero = count_nonzero_c<4>;
    p.cu[BLOCK_8x8].count_nonzero = count_nonzero_c<8>;
    p.cu[BLOCK_16x16].count_nonzero = count_nonzero_c<16>;
    p.cu[BLOCK_32x32].count_nonzero = count_nonzero_c<32>;

    p.cu[BLOCK_4x4].copy_cnt   = copy_count<4>;
    p.cu[BLOCK_8x8].copy_cnt   = copy_count<8>;
    p.cu[BLOCK_16x16].copy_cnt = copy_count<16>;
    p.cu[BLOCK_32x32].copy_cnt = copy_count<32>;

    p.scanPosLast = scanPosLast_c;
    p.findPosFirstLast = findPosFirstLast_c;
    p.costCoeffNxN = costCoeffNxN_c;
    p.costCoeffRemain = costCoeffRemain_c;
    p.costC1C2Flag = costC1C2Flag_c;
}
}
