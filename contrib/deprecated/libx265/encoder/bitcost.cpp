/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Min Chen <chenm003@163.com>
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
#include "bitcost.h"

using namespace X265_NS;

void BitCost::setQP(unsigned int qp)
{
    if (!s_costs[qp])
    {
        ScopedLock s(s_costCalcLock);

        // Now that we have acquired the lock, check again if another thread calculated
        // this row while we were blocked
        if (!s_costs[qp])
        {
            x265_emms(); // just to be safe

            CalculateLogs();
            s_costs[qp] = X265_MALLOC(uint16_t, 4 * BC_MAX_MV + 1) + 2 * BC_MAX_MV;
            if (!s_costs[qp])
            {
                x265_log(NULL, X265_LOG_ERROR, "BitCost s_costs buffer allocation failure\n");
                return;
            }
            double lambda = x265_lambda_tab[qp];

            // estimate same cost for negative and positive MVD
            for (int i = 0; i <= 2 * BC_MAX_MV; i++)
                s_costs[qp][i] = s_costs[qp][-i] = (uint16_t)X265_MIN(s_bitsizes[i] * lambda + 0.5f, (1 << 15) - 1);
        }
    }
    for (int j = 0; j < 4; j++)
    {
        if (!s_fpelMvCosts[qp][j])
        {
            ScopedLock s(s_costCalcLock);
            if (!s_fpelMvCosts[qp][j])
            {
                s_fpelMvCosts[qp][j] = X265_MALLOC(uint16_t, BC_MAX_MV + 1) + (BC_MAX_MV >> 1);
                if (!s_fpelMvCosts[qp][j])
                {
                    x265_log(NULL, X265_LOG_ERROR, "BitCost s_fpelMvCosts buffer allocation failure\n");
                    return;
                }
                for (int i = -(BC_MAX_MV >> 1); i < (BC_MAX_MV >> 1); i++)
                {
                    s_fpelMvCosts[qp][j][i] = s_costs[qp][i * 4 + j];
                }
            }
        }
    }
    m_cost = s_costs[qp];
    for (int j = 0; j < 4; j++)
    {
        m_fpelMvCosts[j] = s_fpelMvCosts[qp][j];
    }
}
/***
 * Class static data and methods
 */

uint16_t *BitCost::s_costs[BC_MAX_QP];

uint16_t* BitCost::s_fpelMvCosts[BC_MAX_QP][4];

float *BitCost::s_bitsizes;

Lock BitCost::s_costCalcLock;

void BitCost::CalculateLogs()
{
    if (!s_bitsizes)
    {
        s_bitsizes = X265_MALLOC(float, 4 * BC_MAX_MV + 1) + 2 * BC_MAX_MV;
        if (!s_bitsizes)
        {
            x265_log(NULL, X265_LOG_ERROR, "BitCost s_bitsizes buffer allocation failure\n");
            return;
        }
        s_bitsizes[0] = 0.718f;
        float log2_2 = 2.0f / log(2.0f);  // 2 x 1/log(2)
        for (int i = 1; i <= 2 * BC_MAX_MV; i++)
            s_bitsizes[i] = s_bitsizes[-i] = log((float)(i + 1)) * log2_2 + 1.718f;
    }
}

void BitCost::destroy()
{
    for (int i = 0; i < BC_MAX_QP; i++)
    {
        if (s_costs[i])
        {
            X265_FREE(s_costs[i] - 2 * BC_MAX_MV);

            s_costs[i] = NULL;
        }
    }
    for (int i = 0; i < BC_MAX_QP; i++)
    {
        for (int j = 0; j < 4; j++)
        {
            if (s_fpelMvCosts[i][j])
            {
                X265_FREE(s_fpelMvCosts[i][j] - (BC_MAX_MV >> 1));
                s_fpelMvCosts[i][j] = NULL;
            }
        }
    }

    if (s_bitsizes)
    {
        X265_FREE(s_bitsizes - 2 * BC_MAX_MV);
        s_bitsizes = NULL;
    }
}
