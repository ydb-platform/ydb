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

#ifndef X265_MV_H
#define X265_MV_H

#include "common.h"
#include "primitives.h"

namespace X265_NS {
// private x265 namespace

#if _MSC_VER
#pragma warning(disable: 4201) // non-standard extension used (nameless struct/union)
#endif

struct MV
{
public:

    union {
        struct { int16_t x, y; };

        int32_t word;
    };

    MV()                                       {}
    MV(int32_t w) : word(w)                    {}
    MV(int16_t _x, int16_t _y) : x(_x), y(_y)  {}

    MV& operator =(uint32_t w)                 { word = w; return *this; }

    MV& operator +=(const MV& other)           { x += other.x; y += other.y; return *this; }

    MV& operator -=(const MV& other)           { x -= other.x; y -= other.y; return *this; }

    MV& operator >>=(int i)                    { x >>= i; y >>= i; return *this; }

#if USING_FTRAPV
    /* avoid signed left-shifts when -ftrapv is enabled */
    MV& operator <<=(int i)                    { x *= (1 << i); y *= (1 << i); return *this; }
    MV operator <<(int i) const                { return MV(x * (1 << i), y * (1 << i)); }
#else
    MV& operator <<=(int i)                    { x <<= i; y <<= i; return *this; }
    MV operator <<(int i) const                { return MV(x << i, y << i); }
#endif

    MV operator >>(int i) const                { return MV(x >> i, y >> i); }

    MV operator *(int16_t i) const             { return MV(x * i, y * i); }

    MV operator -(const MV& other) const       { return MV(x - other.x, y - other.y); }

    MV operator +(const MV& other) const       { return MV(x + other.x, y + other.y); }

    bool operator ==(const MV& other) const    { return word == other.word; }

    bool operator !=(const MV& other) const    { return word != other.word; }

    bool operator !() const                    { return !word; }

    // Scale down a QPEL mv to FPEL mv, rounding up by one HPEL offset
    MV roundToFPel() const                     { return MV((x + 2) >> 2, (y + 2) >> 2); }

    // Scale up an FPEL mv to QPEL by shifting up two bits
    MV toQPel() const                          { return *this << 2; }

    bool inline notZero() const                { return this->word != 0; }

    bool inline isSubpel() const               { return (this->word & 0x00030003) != 0; }

    MV mvmin(const MV& m) const                { return MV(x > m.x ? m.x : x, y > m.y ? m.y : y); }

    MV mvmax(const MV& m) const                { return MV(x < m.x ? m.x : x, y < m.y ? m.y : y); }

    MV clipped(const MV& _min, const MV& _max) const
    {
        MV cl = mvmin(_max);

        return cl.mvmax(_min);
    }

    // returns true if MV is within range (inclusive)
    bool checkRange(const MV& _min, const MV& _max) const
    {
        return x >= _min.x && x <= _max.x && y >= _min.y && y <= _max.y;
    }
};
}

#endif // ifndef X265_MV_H
