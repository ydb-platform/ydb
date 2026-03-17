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

#ifndef GEOS_UTIL_GEOSEXCEPTION_H
#define GEOS_UTIL_GEOSEXCEPTION_H

#include <geos/export.h>
#include <stdexcept>
#include <string>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#pragma warning(disable: 4275) // warning C4275: non-DLL-interface std::exception used as base for DLL-interface GEOSException
#endif

namespace geos {
namespace util { // geos.util

/**
 *
 * \brief Base class for all GEOS exceptions.
 *
 * Use what() to get a readable message.
 */
class GEOS_DLL GEOSException: public std::runtime_error {

public:

    GEOSException()
        :
        std::runtime_error("Unknown error")
    {}

    GEOSException(std::string const& msg)
        :
        std::runtime_error(msg)
    {}

    GEOSException(std::string const& name, std::string const& msg)
        :
        std::runtime_error(name + ": " + msg)
    {}

};

} // namespace geos.util
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_UTIL_GEOSEXCEPTION_H
