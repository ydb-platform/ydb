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

#ifndef X265_BITCOST_H
#define X265_BITCOST_H

#include "common.h"
#include "threading.h"
#include "mv.h"

namespace X265_NS {
// private x265 namespace

class BitCost
{
public:

    BitCost() : m_cost_mvx(0), m_cost_mvy(0), m_cost(0), m_mvp(0) {}

    void setQP(unsigned int qp);

    void setMVP(const MV& mvp)                      { m_mvp = mvp; m_cost_mvx = m_cost - mvp.x; m_cost_mvy = m_cost - mvp.y; }

    // return bit cost of motion vector difference, multiplied by lambda
    inline uint16_t mvcost(const MV& mv) const      { return m_cost_mvx[mv.x] + m_cost_mvy[mv.y]; }

    // return bit cost of motion vector difference, without lambda
    inline uint32_t bitcost(const MV& mv) const
    {
        return (uint32_t)(s_bitsizes[mv.x - m_mvp.x] +
                          s_bitsizes[mv.y - m_mvp.y] + 0.5f);
    }

    static inline uint32_t bitcost(const MV& mv, const MV& mvp)
    {
        return (uint32_t)(s_bitsizes[mv.x - mvp.x] +
                          s_bitsizes[mv.y - mvp.y] + 0.5f);
    }

    static void destroy();

protected:

    uint16_t *m_cost_mvx;

    uint16_t *m_cost_mvy;

    uint16_t *m_cost;

    uint16_t *m_fpelMvCosts[4];

    MV        m_mvp;

    BitCost& operator =(const BitCost&);

private:

    /* default log2_max_mv_length_horizontal and log2_max_mv_length_horizontal
     * are 15, specified in quarter-pel luma sample units. making the maximum
     * signaled ful-pel motion distance 4096, max qpel is 32768 */
    enum { BC_MAX_MV = (1 << 15) };

    enum { BC_MAX_QP = 82 };

    static float *s_bitsizes;

    static uint16_t *s_costs[BC_MAX_QP];

    static uint16_t *s_fpelMvCosts[BC_MAX_QP][4];

    static Lock s_costCalcLock;

    static void CalculateLogs();
};
}

#endif // ifndef X265_BITCOST_H
