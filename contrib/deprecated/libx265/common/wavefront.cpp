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
 * For more information, contact us at license @ x265.com
 *****************************************************************************/

#include "threadpool.h"
#include "threading.h"
#include "wavefront.h"
#include "common.h"

namespace X265_NS {
// x265 private namespace

bool WaveFront::init(int numRows)
{
    m_numRows = numRows;

    m_numWords = (numRows + 31) >> 5;
    m_internalDependencyBitmap = X265_MALLOC(uint32_t, m_numWords);
    if (m_internalDependencyBitmap)
        memset((void*)m_internalDependencyBitmap, 0, sizeof(uint32_t) * m_numWords);

    m_externalDependencyBitmap = X265_MALLOC(uint32_t, m_numWords);
    if (m_externalDependencyBitmap)
        memset((void*)m_externalDependencyBitmap, 0, sizeof(uint32_t) * m_numWords);

    m_row_to_idx = X265_MALLOC(uint32_t, m_numRows);
    m_idx_to_row = X265_MALLOC(uint32_t, m_numRows);

    return m_internalDependencyBitmap && m_externalDependencyBitmap;
}

WaveFront::~WaveFront()
{
    x265_free((void*)m_row_to_idx);
    x265_free((void*)m_idx_to_row);

    x265_free((void*)m_internalDependencyBitmap);
    x265_free((void*)m_externalDependencyBitmap);
}

void WaveFront::clearEnabledRowMask()
{
    memset((void*)m_externalDependencyBitmap, 0, sizeof(uint32_t) * m_numWords);
    memset((void*)m_internalDependencyBitmap, 0, sizeof(uint32_t) * m_numWords);
}

void WaveFront::enqueueRow(int row)
{
    uint32_t bit = 1 << (row & 31);
    ATOMIC_OR(&m_internalDependencyBitmap[row >> 5], bit);
}

void WaveFront::enableRow(int row)
{
    uint32_t bit = 1 << (row & 31);
    ATOMIC_OR(&m_externalDependencyBitmap[row >> 5], bit);
}

void WaveFront::enableAllRows()
{
    memset((void*)m_externalDependencyBitmap, ~0, sizeof(uint32_t) * m_numWords);
}

bool WaveFront::dequeueRow(int row)
{
    uint32_t bit = 1 << (row & 31);
    return !!(ATOMIC_AND(&m_internalDependencyBitmap[row >> 5], ~bit) & bit);
}

void WaveFront::findJob(int threadId)
{
    unsigned long id;

    /* Loop over each word until all available rows are finished */
    for (int w = 0; w < m_numWords; w++)
    {
        uint32_t oldval = m_internalDependencyBitmap[w] & m_externalDependencyBitmap[w];
        while (oldval)
        {
            CTZ(id, oldval);

            uint32_t bit = 1 << id;
            if (ATOMIC_AND(&m_internalDependencyBitmap[w], ~bit) & bit)
            {
                /* we cleared the bit, we get to process the row */
                processRow(w * 32 + id, threadId);
                m_helpWanted = true;
                return; /* check for a higher priority task */
            }

            oldval = m_internalDependencyBitmap[w] & m_externalDependencyBitmap[w];
        }
    }

    m_helpWanted = false;
}
}
