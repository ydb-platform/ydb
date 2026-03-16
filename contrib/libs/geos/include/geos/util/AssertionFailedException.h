/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_UTIL_ASSERTIONFAILEDEXCEPTION_H
#define GEOS_UTIL_ASSERTIONFAILEDEXCEPTION_H

#include <geos/export.h>
#include <string>

#include <geos/util/GEOSException.h>

namespace geos {
namespace util { // geos.util

/** \class AssertionFailedException util.h geos.h
 * \brief Indicates a bug in GEOS code.
 */
class GEOS_DLL AssertionFailedException: public GEOSException {

public:

    AssertionFailedException()
        :
        GEOSException("AssertionFailedException", "")
    {}

    AssertionFailedException(const std::string& msg)
        :
        GEOSException("AssertionFailedException", msg)
    {}

    ~AssertionFailedException() noexcept override {}
};

} // namespace geos.util
} // namespace geos


#endif // GEOS_UTIL_ASSERTIONFAILEDEXCEPTION_H
