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

#ifndef X265_NAL_H
#define X265_NAL_H

#include "common.h"
#include "x265.h"

namespace X265_NS {
// private namespace

class Bitstream;

class NALList
{
public:
    static const int MAX_NAL_UNITS = 16;

public:

    x265_nal    m_nal[MAX_NAL_UNITS];
    uint32_t    m_numNal;

    uint8_t*    m_buffer;
    uint32_t    m_occupancy;
    uint32_t    m_allocSize;

    uint8_t*    m_extraBuffer;
    uint32_t    m_extraOccupancy;
    uint32_t    m_extraAllocSize;
    bool        m_annexB;

    NALList();
    ~NALList() { X265_FREE(m_buffer); X265_FREE(m_extraBuffer); }

    void takeContents(NALList& other);

    void serialize(NalUnitType nalUnitType, const Bitstream& bs);

    uint32_t serializeSubstreams(uint32_t* streamSizeBytes, uint32_t streamCount, const Bitstream* streams);
};

}

#endif // ifndef X265_NAL_H
