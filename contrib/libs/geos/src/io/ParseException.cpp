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
 * Last port: io/ParseException.java rev. 1.13 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/io/ParseException.h>
#include <sstream>
#include <string>

using namespace std;

namespace geos {
namespace io { // geos.io

ParseException::ParseException()
    :
    GEOSException("ParseException", "")
{
}

ParseException::ParseException(const string& msg)
    :
    GEOSException("ParseException", msg)
{
}

ParseException::ParseException(const string& msg, const string& var)
    :
    GEOSException("ParseException", msg + ": '" + var + "'")
{
}

ParseException::ParseException(const string& msg, double num)
    :
    GEOSException("ParseException", msg + ": '" + stringify(num) + "'")
{
}

string
ParseException::stringify(double num)
{
    stringstream ss;
    ss << num;
    return ss.str();
}

} // namespace geos.io
} // namespace geos
