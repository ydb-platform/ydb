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

#ifndef X265_WAVEFRONT_H
#define X265_WAVEFRONT_H

#include "common.h"
#include "threadpool.h"

namespace X265_NS {
// x265 private namespace

// Generic wave-front scheduler, manages busy-state of CU rows as a priority
// queue (higher CU rows have priority over lower rows)
//
// Derived classes must implement ProcessRow().
class WaveFront : public JobProvider
{
private:

    // bitmaps of rows queued for processing, uses atomic intrinsics

    // Dependencies are categorized as internal and external. Internal dependencies
    // are caused by neighbor block availability.  External dependencies are generally
    // reference frame reconstructed pixels being available.
    uint32_t volatile *m_internalDependencyBitmap;
    uint32_t volatile *m_externalDependencyBitmap;

    // number of words in the bitmap
    int m_numWords;

    int m_numRows;

protected:
    uint32_t *m_row_to_idx;
    uint32_t *m_idx_to_row;

public:

    WaveFront()
        : m_internalDependencyBitmap(NULL)
        , m_externalDependencyBitmap(NULL)
    {}

    virtual ~WaveFront();

    // If returns false, the frame must be encoded in series.
    bool init(int numRows);

    // Enqueue a row to be processed (mark its internal dependencies as resolved).
    // A worker thread will later call processRow(row).
    // This provider must be enqueued in the pool before enqueuing a row
    void enqueueRow(int row);

    // Mark a row as no longer having internal dependencies resolved. Returns
    // true if bit clear was successful, false otherwise.
    bool dequeueRow(int row);

    // Mark the row's external dependencies as being resolved
    void enableRow(int row);

    // Mark all row external dependencies as being resolved. Some wavefront
    // implementations (lookahead, for instance) have no recon pixel dependencies.
    void enableAllRows();

    // Mark all rows as having external dependencies which must be
    // resolved before each row may proceed.
    void clearEnabledRowMask();

    // WaveFront's implementation of JobProvider::findJob. Consults
    // m_queuedBitmap and calls ProcessRow(row) for lowest numbered queued row
    // processes available rows and returns when no work remains
    void findJob(int threadId);

    // Start or resume encode processing of this row, must be implemented by
    // derived classes.
    virtual void processRow(int row, int threadId) = 0;
};
} // end namespace X265_NS

#endif // ifndef X265_WAVEFRONT_H
