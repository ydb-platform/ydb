/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: io/ByteOrderDataInStream.java rev. 1.1 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_IO_BYTEORDERDATAINSTREAM_INL
#define GEOS_IO_BYTEORDERDATAINSTREAM_INL

#include <geos/io/ParseException.h>
#include <geos/io/ByteOrderDataInStream.h>
#include <geos/io/ByteOrderValues.h>
#include <geos/util/Machine.h> // for getMachineByteOrder

#include <iostream> // ostream, istream 

namespace geos {
namespace io {

INLINE
ByteOrderDataInStream::ByteOrderDataInStream(std::istream* s)
    :
    byteOrder(getMachineByteOrder()),
    stream(s)
{
}

INLINE
ByteOrderDataInStream::~ByteOrderDataInStream()
{
}

INLINE void
ByteOrderDataInStream::setInStream(std::istream* s)
{
    stream = s;
}

INLINE void
ByteOrderDataInStream::setOrder(int order)
{
    byteOrder = order;
}

INLINE unsigned char
ByteOrderDataInStream::readByte() // throws ParseException
{
    stream->read(reinterpret_cast<char*>(buf), 1);
    if(stream->eof()) {
        throw  ParseException("Unexpected EOF parsing WKB");
    }
    return buf[0];
}

INLINE int
ByteOrderDataInStream::readInt()
{
    stream->read(reinterpret_cast<char*>(buf), 4);
    if(stream->eof()) {
        throw  ParseException("Unexpected EOF parsing WKB");
    }
    return ByteOrderValues::getInt(buf, byteOrder);
}

INLINE long
ByteOrderDataInStream::readLong()
{
    stream->read(reinterpret_cast<char*>(buf), 8);
    if(stream->eof()) {
        throw  ParseException("Unexpected EOF parsing WKB");
    }
    return static_cast<long>(ByteOrderValues::getLong(buf, byteOrder));
}

INLINE double
ByteOrderDataInStream::readDouble()
{
    stream->read(reinterpret_cast<char*>(buf), 8);
    if(stream->eof()) {
        throw  ParseException("Unexpected EOF parsing WKB");
    }
    return ByteOrderValues::getDouble(buf, byteOrder);
}

} // namespace io
} // namespace geos

#endif // #ifndef GEOS_IO_BYTEORDERDATAINSTREAM_INL
