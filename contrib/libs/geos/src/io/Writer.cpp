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
 * Last port: ORIGINAL WORK to be used like java.io.Writer
 *
 **********************************************************************/

#include <geos/io/Writer.h>
#include <string>

namespace geos {
namespace io { // geos.io

Writer::Writer()
{
}

void
Writer::reserve(std::size_t capacity)
{
    str.reserve(capacity);
}

void
Writer::write(const std::string& txt)
{
    str.append(txt);
}

const std::string&
Writer::toString()
{
    return str;
}

} // namespace geos.io
} // namespace geos
