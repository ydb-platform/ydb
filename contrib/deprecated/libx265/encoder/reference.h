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

#ifndef X265_REFERENCE_H
#define X265_REFERENCE_H

#include "primitives.h"
#include "picyuv.h"
#include "lowres.h"
#include "mv.h"

namespace X265_NS {
// private x265 namespace

struct WeightParam;

class MotionReference : public ReferencePlanes
{
public:

    MotionReference();
    ~MotionReference();
    int  init(PicYuv*, WeightParam* wp, const x265_param& p);
    void applyWeight(uint32_t finishedRows, uint32_t maxNumRows, uint32_t maxNumRowsInSlice, uint32_t sliceId);

    pixel*      weightBuffer[3];
    int         numInterpPlanes;
    uint32_t*   numSliceWeightedRows;

protected:

    MotionReference& operator =(const MotionReference&);
};
}

#endif // ifndef X265_REFERENCE_H
