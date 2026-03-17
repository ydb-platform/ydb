/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
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
#include "scalinglist.h"

namespace {
// file-anonymous namespace

/* Strings for scaling list file parsing */

static int quantTSDefault4x4[16] =
{
    16, 16, 16, 16,
    16, 16, 16, 16,
    16, 16, 16, 16,
    16, 16, 16, 16
};

static int quantIntraDefault8x8[64] =
{
    16, 16, 16, 16, 17, 18, 21, 24,
    16, 16, 16, 16, 17, 19, 22, 25,
    16, 16, 17, 18, 20, 22, 25, 29,
    16, 16, 18, 21, 24, 27, 31, 36,
    17, 17, 20, 24, 30, 35, 41, 47,
    18, 19, 22, 27, 35, 44, 54, 65,
    21, 22, 25, 31, 41, 54, 70, 88,
    24, 25, 29, 36, 47, 65, 88, 115
};

static int quantInterDefault8x8[64] =
{
    16, 16, 16, 16, 17, 18, 20, 24,
    16, 16, 16, 17, 18, 20, 24, 25,
    16, 16, 17, 18, 20, 24, 25, 28,
    16, 17, 18, 20, 24, 25, 28, 33,
    17, 18, 20, 24, 25, 28, 33, 41,
    18, 20, 24, 25, 28, 33, 41, 54,
    20, 24, 25, 28, 33, 41, 54, 71,
    24, 25, 28, 33, 41, 54, 71, 91
};

}

namespace X265_NS {
// private namespace
    const char ScalingList::MatrixType[4][6][20] =
    {
        {
            "INTRA4X4_LUMA",
            "INTRA4X4_CHROMAU",
            "INTRA4X4_CHROMAV",
            "INTER4X4_LUMA",
            "INTER4X4_CHROMAU",
            "INTER4X4_CHROMAV"
        },
        {
            "INTRA8X8_LUMA",
            "INTRA8X8_CHROMAU",
            "INTRA8X8_CHROMAV",
            "INTER8X8_LUMA",
            "INTER8X8_CHROMAU",
            "INTER8X8_CHROMAV"
        },
        {
            "INTRA16X16_LUMA",
            "INTRA16X16_CHROMAU",
            "INTRA16X16_CHROMAV",
            "INTER16X16_LUMA",
            "INTER16X16_CHROMAU",
            "INTER16X16_CHROMAV"
        },
        {
            "INTRA32X32_LUMA",
            "",
            "",
            "INTER32X32_LUMA",
            "",
            "",
        },
    };
    const char ScalingList::MatrixType_DC[4][12][22] =
    {
        {
        },
        {
        },
        {
            "INTRA16X16_LUMA_DC",
            "INTRA16X16_CHROMAU_DC",
            "INTRA16X16_CHROMAV_DC",
            "INTER16X16_LUMA_DC",
            "INTER16X16_CHROMAU_DC",
            "INTER16X16_CHROMAV_DC"
        },
        {
            "INTRA32X32_LUMA_DC",
            "",
            "",
            "INTER32X32_LUMA_DC",
            "",
            "",
        },
    };

const int     ScalingList::s_numCoefPerSize[NUM_SIZES] = { 16, 64, 256, 1024 };
const int32_t ScalingList::s_quantScales[NUM_REM] = { 26214, 23302, 20560, 18396, 16384, 14564 };
const int32_t ScalingList::s_invQuantScales[NUM_REM] = { 40, 45, 51, 57, 64, 72 };

ScalingList::ScalingList()
{
    memset(m_quantCoef, 0, sizeof(m_quantCoef));
    memset(m_dequantCoef, 0, sizeof(m_dequantCoef));
    memset(m_scalingListCoef, 0, sizeof(m_scalingListCoef));
}

bool ScalingList::init()
{
    bool ok = true;
    for (int sizeId = 0; sizeId < NUM_SIZES; sizeId++)
    {
        for (int listId = 0; listId < NUM_LISTS; listId++)
        {
            m_scalingListCoef[sizeId][listId] = X265_MALLOC(int32_t, X265_MIN(MAX_MATRIX_COEF_NUM, s_numCoefPerSize[sizeId]));
            ok &= !!m_scalingListCoef[sizeId][listId];
            for (int rem = 0; rem < NUM_REM; rem++)
            {
                m_quantCoef[sizeId][listId][rem] = X265_MALLOC(int32_t, s_numCoefPerSize[sizeId]);
                m_dequantCoef[sizeId][listId][rem] = X265_MALLOC(int32_t, s_numCoefPerSize[sizeId]);
                ok &= m_quantCoef[sizeId][listId][rem] && m_dequantCoef[sizeId][listId][rem];
            }
        }
    }
    return ok;
}

ScalingList::~ScalingList()
{
    for (int sizeId = 0; sizeId < NUM_SIZES; sizeId++)
    {
        for (int listId = 0; listId < NUM_LISTS; listId++)
        {
            X265_FREE(m_scalingListCoef[sizeId][listId]);
            for (int rem = 0; rem < NUM_REM; rem++)
            {
                X265_FREE(m_quantCoef[sizeId][listId][rem]);
                X265_FREE(m_dequantCoef[sizeId][listId][rem]);
            }
        }
    }
}

/* returns predicted list index if a match is found, else -1 */ 
int ScalingList::checkPredMode(int size, int list) const
{
    for (int predList = list; predList >= 0; predList--)
    {
        // check DC value
        if (size < BLOCK_16x16 && m_scalingListDC[size][list] != m_scalingListDC[size][predList])
            continue;

        // check value of matrix
        if (!memcmp(m_scalingListCoef[size][list],
                    list == predList ? getScalingListDefaultAddress(size, predList) : m_scalingListCoef[size][predList],
                    sizeof(int32_t) * X265_MIN(MAX_MATRIX_COEF_NUM, s_numCoefPerSize[size])))
            return predList;
    }

    return -1;
}

/* check if use default quantization matrix
 * returns true if default quantization matrix is used in all sizes */
bool ScalingList::checkDefaultScalingList() const
{
    int defaultCounter = 0;

    for (int s = 0; s < NUM_SIZES; s++)
        for (int l = 0; l < NUM_LISTS; l++)
            if (!memcmp(m_scalingListCoef[s][l], getScalingListDefaultAddress(s, l),
                        sizeof(int32_t) * X265_MIN(MAX_MATRIX_COEF_NUM, s_numCoefPerSize[s])) &&
                ((s < BLOCK_16x16) || (m_scalingListDC[s][l] == 16)))
                defaultCounter++;

    return defaultCounter != (NUM_LISTS * NUM_SIZES - 4); // -4 for 32x32
}

/* get address of default quantization matrix */
const int32_t* ScalingList::getScalingListDefaultAddress(int sizeId, int listId) const
{
    switch (sizeId)
    {
    case BLOCK_4x4:
        return quantTSDefault4x4;
    case BLOCK_8x8:
        return (listId < 3) ? quantIntraDefault8x8 : quantInterDefault8x8;
    case BLOCK_16x16:
        return (listId < 3) ? quantIntraDefault8x8 : quantInterDefault8x8;
    case BLOCK_32x32:
        return (listId < 1) ? quantIntraDefault8x8 : quantInterDefault8x8;
    default:
        break;
    }

    X265_CHECK(0, "invalid scaling list size\n");
    return NULL;
}

void ScalingList::processDefaultMarix(int sizeId, int listId)
{
    memcpy(m_scalingListCoef[sizeId][listId], getScalingListDefaultAddress(sizeId, listId), sizeof(int) * X265_MIN(MAX_MATRIX_COEF_NUM, s_numCoefPerSize[sizeId]));
    m_scalingListDC[sizeId][listId] = SCALING_LIST_DC;
}

void ScalingList::setDefaultScalingList()
{
    for (int sizeId = 0; sizeId < NUM_SIZES; sizeId++)
        for (int listId = 0; listId < NUM_LISTS; listId++)
            processDefaultMarix(sizeId, listId);
    m_bEnabled = true;
    m_bDataPresent = false;
}

bool ScalingList::parseScalingList(const char* filename)
{
    FILE *fp = x265_fopen(filename, "r");
    if (!fp)
    {
        x265_log_file(NULL, X265_LOG_ERROR, "can't open scaling list file %s\n", filename);
        return true;
    }

    char line[1024];
    int32_t *src = NULL;
    fseek(fp, 0, 0);

    for (int sizeIdc = 0; sizeIdc < NUM_SIZES; sizeIdc++)
    {
        int size = X265_MIN(MAX_MATRIX_COEF_NUM, s_numCoefPerSize[sizeIdc]);
        for (int listIdc = 0; listIdc < NUM_LISTS;  listIdc += (sizeIdc == 3) ? 3 : 1)
        {
            src = m_scalingListCoef[sizeIdc][listIdc];

            do
            {
                char *ret = fgets(line, 1024, fp);
                if (!ret || (!strstr(line, MatrixType[sizeIdc][listIdc]) && feof(fp)))
                {
                    x265_log_file(NULL, X265_LOG_ERROR, "can't read matrix from %s\n", filename);
                    return true;
                }
            }
            while (!strstr(line, MatrixType[sizeIdc][listIdc]));

            for (int i = 0; i < size; i++)
            {
                int data;
                if (fscanf(fp, "%d,", &data) != 1)
                {
                    x265_log_file(NULL, X265_LOG_ERROR, "can't read matrix from %s\n", filename);
                    return true;
                }
                src[i] = data;
            }

            // set DC value for default matrix check
            m_scalingListDC[sizeIdc][listIdc] = src[0];

            if (sizeIdc > BLOCK_8x8)
            {
                do
                {
                    char *ret = fgets(line, 1024, fp);
                    if (!ret || (!strstr(line, MatrixType_DC[sizeIdc][listIdc]) && feof(fp)))
                    {
                        x265_log_file(NULL, X265_LOG_ERROR, "can't read DC from %s\n", filename);
                        return true;
                    }
                }
                while (!strstr(line, MatrixType_DC[sizeIdc][listIdc]));

                int data;
                if (fscanf(fp, "%d,", &data) != 1)
                {
                    x265_log_file(NULL, X265_LOG_ERROR, "can't read matrix from %s\n", filename);
                    return true;
                }

                // overwrite DC value when size of matrix is larger than 16x16
                m_scalingListDC[sizeIdc][listIdc] = data;
            }
        }
        if (sizeIdc == 3)
        {
            for (int listIdc = 1; listIdc < NUM_LISTS; listIdc++)
            {
                if (listIdc % 3 != 0)
                {
                    src = m_scalingListCoef[sizeIdc][listIdc];
                    const int *srcNextSmallerSize = m_scalingListCoef[sizeIdc - 1][listIdc];
                    for (int i = 0; i < size; i++)
                    {
                        src[i] = srcNextSmallerSize[i];
                    }
                    m_scalingListDC[sizeIdc][listIdc] = m_scalingListDC[sizeIdc - 1][listIdc];
                }
            }
        }
    }

    fclose(fp);

    m_bEnabled = true;
    m_bDataPresent = true;

    return false;
}

/** set quantized matrix coefficient for encode */
void ScalingList::setupQuantMatrices(int internalCsp)
{
    for (int size = 0; size < NUM_SIZES; size++)
    {
        int width = 1 << (size + 2);
        int ratio = width / X265_MIN(MAX_MATRIX_SIZE_NUM, width);
        int stride = X265_MIN(MAX_MATRIX_SIZE_NUM, width);
        int count = s_numCoefPerSize[size];

        for (int list = 0; list < NUM_LISTS; list++)
        {
            int32_t *coeff = m_scalingListCoef[size][list];
            int32_t dc = m_scalingListDC[size][list];

            for (int rem = 0; rem < NUM_REM; rem++)
            {
                int32_t *quantCoeff   = m_quantCoef[size][list][rem];
                int32_t *dequantCoeff = m_dequantCoef[size][list][rem];

                if (m_bEnabled)
                {
                    if (internalCsp == X265_CSP_I444)
                    {
                        for (int i = 0; i < 64; i++)
                        {
                            m_scalingListCoef[BLOCK_32x32][1][i] = m_scalingListCoef[BLOCK_16x16][1][i];
                            m_scalingListCoef[BLOCK_32x32][2][i] = m_scalingListCoef[BLOCK_16x16][2][i];
                            m_scalingListCoef[BLOCK_32x32][4][i] = m_scalingListCoef[BLOCK_16x16][4][i];
                            m_scalingListCoef[BLOCK_32x32][5][i] = m_scalingListCoef[BLOCK_16x16][5][i];
                        }

                        m_scalingListDC[BLOCK_32x32][1] = m_scalingListDC[BLOCK_16x16][1];
                        m_scalingListDC[BLOCK_32x32][2] = m_scalingListDC[BLOCK_16x16][2];
                        m_scalingListDC[BLOCK_32x32][4] = m_scalingListDC[BLOCK_16x16][4];
                        m_scalingListDC[BLOCK_32x32][5] = m_scalingListDC[BLOCK_16x16][5];
                    }
                    processScalingListEnc(coeff, quantCoeff, s_quantScales[rem] << 4, width, width, ratio, stride, dc);
                    processScalingListDec(coeff, dequantCoeff, s_invQuantScales[rem], width, width, ratio, stride, dc);
                }
                else
                {
                    /* flat quant and dequant coefficients */
                    for (int i = 0; i < count; i++)
                    {
                        quantCoeff[i] = s_quantScales[rem];
                        dequantCoeff[i] = s_invQuantScales[rem];
                    }
                }
            }
        }
    }
}

void ScalingList::processScalingListEnc(int32_t *coeff, int32_t *quantcoeff, int32_t quantScales, int height, int width,
                                        int ratio, int stride, int32_t dc)
{
    for (int j = 0; j < height; j++)
        for (int i = 0; i < width; i++)
            quantcoeff[j * width + i] = quantScales / coeff[stride * (j / ratio) + i / ratio];

    if (ratio > 1)
        quantcoeff[0] = quantScales / dc;
}

void ScalingList::processScalingListDec(int32_t *coeff, int32_t *dequantcoeff, int32_t invQuantScales, int height, int width,
                                        int ratio, int stride, int32_t dc)
{
    for (int j = 0; j < height; j++)
        for (int i = 0; i < width; i++)
            dequantcoeff[j * width + i] = invQuantScales * coeff[stride * (j / ratio) + i / ratio];

    if (ratio > 1)
        dequantcoeff[0] = invQuantScales * dc;
}

}
