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

#ifndef GEOS_UTIL_ILLEGALARGUMENTEXCEPTION_H
#define GEOS_UTIL_ILLEGALARGUMENTEXCEPTION_H

#include <geos/export.h>
#include <string>

#include <geos/util/GEOSException.h>

namespace geos {
namespace util { // geos::util

/**
 * \brief Indicates one or more illegal arguments.
 *
 * This exception is thrown - for example - when
 * trying to apply set-theoretic methods to a
 * GeometryCollection object.
 */
class GEOS_DLL IllegalArgumentException: public GEOSException {
public:
    IllegalArgumentException()
        :
        GEOSException("IllegalArgumentException", "")
    {}

    IllegalArgumentException(const std::string& msg)
        :
        GEOSException("IllegalArgumentException", msg)
    {}

    ~IllegalArgumentException() noexcept override {}
};

} // namespace geos::util
} // namespace geos


#endif // GEOS_UTIL_ILLEGALARGUMENTEXCEPTION_H
