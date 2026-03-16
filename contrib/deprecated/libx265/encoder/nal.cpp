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
#include "bitstream.h"
#include "nal.h"

using namespace X265_NS;

NALList::NALList()
    : m_numNal(0)
    , m_buffer(NULL)
    , m_occupancy(0)
    , m_allocSize(0)
    , m_extraBuffer(NULL)
    , m_extraOccupancy(0)
    , m_extraAllocSize(0)
    , m_annexB(true)
{}

void NALList::takeContents(NALList& other)
{
    /* take other NAL buffer, discard our old one */
    X265_FREE(m_buffer);
    m_buffer = other.m_buffer;
    m_allocSize = other.m_allocSize;
    m_occupancy = other.m_occupancy;

    /* copy packet data */
    m_numNal = other.m_numNal;
    memcpy(m_nal, other.m_nal, sizeof(x265_nal) * m_numNal);

    /* reset other list, re-allocate their buffer with same size */
    other.m_numNal = 0;
    other.m_occupancy = 0;
    other.m_buffer = X265_MALLOC(uint8_t, m_allocSize);
}

void NALList::serialize(NalUnitType nalUnitType, const Bitstream& bs)
{
    static const char startCodePrefix[] = { 0, 0, 0, 1 };

    uint32_t payloadSize = bs.getNumberOfWrittenBytes();
    const uint8_t* bpayload = bs.getFIFO();
    if (!bpayload)
        return;

    uint32_t nextSize = m_occupancy + sizeof(startCodePrefix) + 2 + payloadSize + (payloadSize >> 1) + m_extraOccupancy;
    if (nextSize > m_allocSize)
    {
        uint8_t *temp = X265_MALLOC(uint8_t, nextSize);
        if (temp)
        {
            memcpy(temp, m_buffer, m_occupancy);

            /* fixup existing payload pointers */
            for (uint32_t i = 0; i < m_numNal; i++)
                m_nal[i].payload = temp + (m_nal[i].payload - m_buffer);

            X265_FREE(m_buffer);
            m_buffer = temp;
            m_allocSize = nextSize;
        }
        else
        {
            x265_log(NULL, X265_LOG_ERROR, "Unable to realloc access unit buffer\n");
            return;
        }
    }

    uint8_t *out = m_buffer + m_occupancy;
    uint32_t bytes = 0;

    if (!m_annexB)
    {
        /* Will write size later */
        bytes += 4;
    }
    else if (!m_numNal || nalUnitType == NAL_UNIT_VPS || nalUnitType == NAL_UNIT_SPS || nalUnitType == NAL_UNIT_PPS)
    {
        memcpy(out, startCodePrefix, 4);
        bytes += 4;
    }
    else
    {
        memcpy(out, startCodePrefix + 1, 3);
        bytes += 3;
    }

    /* 16 bit NAL header:
     * forbidden_zero_bit       1-bit
     * nal_unit_type            6-bits
     * nuh_reserved_zero_6bits  6-bits
     * nuh_temporal_id_plus1    3-bits */
    out[bytes++] = (uint8_t)nalUnitType << 1;
    out[bytes++] = 1 + (nalUnitType == NAL_UNIT_CODED_SLICE_TSA_N);

    /* 7.4.1 ...
     * Within the NAL unit, the following three-byte sequences shall not occur at
     * any byte-aligned position:
     *  - 0x000000
     *  - 0x000001
     *  - 0x000002 */
    for (uint32_t i = 0; i < payloadSize; i++)
    {
        if (i > 2 && !out[bytes - 2] && !out[bytes - 3] && out[bytes - 1] <= 0x03)
        {
            /* inject 0x03 to prevent emulating a start code */
            out[bytes] = out[bytes - 1];
            out[bytes - 1] = 0x03;
            bytes++;
        }

        out[bytes++] = bpayload[i];
    }

    X265_CHECK(bytes <= 4 + 2 + payloadSize + (payloadSize >> 1), "NAL buffer overflow\n");

    if (m_extraOccupancy)
    {
        /* these bytes were escaped by serializeSubstreams */
        memcpy(out + bytes, m_extraBuffer, m_extraOccupancy);
        bytes += m_extraOccupancy;
        m_extraOccupancy = 0;
    }

    /* 7.4.1.1
     * ... when the last byte of the RBSP data is equal to 0x00 (which can
     * only occur when the RBSP ends in a cabac_zero_word), a final byte equal
     * to 0x03 is appended to the end of the data.  */
    if (!out[bytes - 1])
        out[bytes++] = 0x03;

    if (!m_annexB)
    {
        uint32_t dataSize = bytes - 4;
        out[0] = (uint8_t)(dataSize >> 24);
        out[1] = (uint8_t)(dataSize >> 16);
        out[2] = (uint8_t)(dataSize >> 8);
        out[3] = (uint8_t)dataSize;
    }

    m_occupancy += bytes;

    X265_CHECK(m_numNal < (uint32_t)MAX_NAL_UNITS, "NAL count overflow\n");

    x265_nal& nal = m_nal[m_numNal++];
    nal.type = nalUnitType;
    nal.sizeBytes = bytes;
    nal.payload = out;
}

/* concatenate and escape WPP sub-streams, return escaped row lengths.
 * These streams will be appended to the next serialized NAL */
uint32_t NALList::serializeSubstreams(uint32_t* streamSizeBytes, uint32_t streamCount, const Bitstream* streams)
{
    uint32_t maxStreamSize = 0;
    uint32_t estSize = 0;
    for (uint32_t s = 0; s < streamCount; s++)
        estSize += streams[s].getNumberOfWrittenBytes();
    estSize += estSize >> 1;

    if (estSize > m_extraAllocSize)
    {
        uint8_t *temp = X265_MALLOC(uint8_t, estSize);
        if (temp)
        {
            X265_FREE(m_extraBuffer);
            m_extraBuffer = temp;
            m_extraAllocSize = estSize;
        }
        else
        {
            x265_log(NULL, X265_LOG_ERROR, "Unable to realloc WPP substream concatenation buffer\n");
            return 0;
        }
    }

    uint32_t bytes = 0;
    uint8_t *out = m_extraBuffer;
    for (uint32_t s = 0; s < streamCount; s++)
    {
        const Bitstream& stream = streams[s];
        uint32_t inSize = stream.getNumberOfWrittenBytes();
        const uint8_t *inBytes = stream.getFIFO();
        uint32_t prevBufSize = bytes;

        if (inBytes)
        {
            for (uint32_t i = 0; i < inSize; i++)
            {
                if (bytes >= 2 && !out[bytes - 2] && !out[bytes - 1] && inBytes[i] <= 0x03)
                {
                    /* inject 0x03 to prevent emulating a start code */
                    out[bytes++] = 3;
                }

                out[bytes++] = inBytes[i];
            }
        }

        if (s < streamCount - 1)
        {
            streamSizeBytes[s] = bytes - prevBufSize;
            if (streamSizeBytes[s] > maxStreamSize)
                maxStreamSize = streamSizeBytes[s];
        }
    }

    m_extraOccupancy = bytes;
    return maxStreamSize;
}
