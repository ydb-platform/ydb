/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: io/ByteOrderValues.java rev. 1.3 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/io/ByteOrderValues.h>
#include <geos/constants.h>
#include <geos/util.h>

#include <cstring>
#include <cassert>

#if DEBUG_BYTEORDER_VALUES
#include <ios>
#include <iostream>
#endif

namespace geos {
namespace io { // geos.io

int
ByteOrderValues::getInt(const unsigned char* buf, int byteOrder)
{
    if(byteOrder == ENDIAN_BIG) {
        return ((int)(buf[0] & 0xff) << 24) |
               ((int)(buf[1] & 0xff) << 16) |
               ((int)(buf[2] & 0xff) << 8) |
               ((int)(buf[3] & 0xff));
    }
    else { // ENDIAN_LITTLE
        assert(byteOrder == ENDIAN_LITTLE);

        return ((int)(buf[3] & 0xff) << 24) |
               ((int)(buf[2] & 0xff) << 16) |
               ((int)(buf[1] & 0xff) << 8) |
               ((int)(buf[0] & 0xff));
    }
}

void
ByteOrderValues::putInt(int intValue, unsigned char* buf, int byteOrder)
{
    if(byteOrder == ENDIAN_BIG) {
        buf[0] = (unsigned char)(intValue >> 24);
        buf[1] = (unsigned char)(intValue >> 16);
        buf[2] = (unsigned char)(intValue >> 8);
        buf[3] = (unsigned char) intValue;
    }
    else { // ENDIAN_LITTLE
        assert(byteOrder == ENDIAN_LITTLE);

        buf[3] = (unsigned char)(intValue >> 24);
        buf[2] = (unsigned char)(intValue >> 16);
        buf[1] = (unsigned char)(intValue >> 8);
        buf[0] = (unsigned char) intValue;
    }
}

int64
ByteOrderValues::getLong(const unsigned char* buf, int byteOrder)
{
    if(byteOrder == ENDIAN_BIG) {
        return
            (int64)(buf[0]) << 56
            | (int64)(buf[1] & 0xff) << 48
            | (int64)(buf[2] & 0xff) << 40
            | (int64)(buf[3] & 0xff) << 32
            | (int64)(buf[4] & 0xff) << 24
            | (int64)(buf[5] & 0xff) << 16
            | (int64)(buf[6] & 0xff) <<  8
            | (int64)(buf[7] & 0xff);
    }
    else { // ENDIAN_LITTLE
        assert(byteOrder == ENDIAN_LITTLE);

        return
            (int64)(buf[7]) << 56
            | (int64)(buf[6] & 0xff) << 48
            | (int64)(buf[5] & 0xff) << 40
            | (int64)(buf[4] & 0xff) << 32
            | (int64)(buf[3] & 0xff) << 24
            | (int64)(buf[2] & 0xff) << 16
            | (int64)(buf[1] & 0xff) <<  8
            | (int64)(buf[0] & 0xff);
    }
}

void
ByteOrderValues::putLong(int64 longValue, unsigned char* buf, int byteOrder)
{
    if(byteOrder == ENDIAN_BIG) {
        buf[0] = (unsigned char)(longValue >> 56);
        buf[1] = (unsigned char)(longValue >> 48);
        buf[2] = (unsigned char)(longValue >> 40);
        buf[3] = (unsigned char)(longValue >> 32);
        buf[4] = (unsigned char)(longValue >> 24);
        buf[5] = (unsigned char)(longValue >> 16);
        buf[6] = (unsigned char)(longValue >> 8);
        buf[7] = (unsigned char) longValue;
    }
    else { // ENDIAN_LITTLE
        assert(byteOrder == ENDIAN_LITTLE);

        buf[0] = (unsigned char) longValue;
        buf[1] = (unsigned char)(longValue >> 8);
        buf[2] = (unsigned char)(longValue >> 16);
        buf[3] = (unsigned char)(longValue >> 24);
        buf[4] = (unsigned char)(longValue >> 32);
        buf[5] = (unsigned char)(longValue >> 40);
        buf[6] = (unsigned char)(longValue >> 48);
        buf[7] = (unsigned char)(longValue >> 56);
    }
}

double
ByteOrderValues::getDouble(const unsigned char* buf, int byteOrder)
{
    int64 longValue = getLong(buf, byteOrder);
    double ret;
    std::memcpy(&ret, &longValue, sizeof(double));
    return ret;
}

void
ByteOrderValues::putDouble(double doubleValue, unsigned char* buf, int byteOrder)
{
    int64 longValue;
    std::memcpy(&longValue, &doubleValue, sizeof(double));
#if DEBUG_BYTEORDER_VALUES
    std::cout << "ByteOrderValues::putDouble(" << doubleValue <<
              ", order:" << byteOrder
              << ") = " << std::hex;
    for(int i = 0; i < 8; i++) {
        std::cout << "[" << (int)buf[i] << "]";
    }
    std::cout << std::dec << std::endl;
#endif
    putLong(longValue, buf, byteOrder);
}

} // namespace geos.io
} // namespace geos
