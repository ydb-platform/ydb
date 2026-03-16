/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2008 Sean Gillies
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: ORIGINAL WORK
 *
 **********************************************************************/

#ifndef GEOS_IO_CLOCALIZER_H
#define GEOS_IO_CLOCALIZER_H

#include <geos/export.h>

#include <string>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace io {

/**
 * \class CLocalizer io.h geos.h
 */
class GEOS_DLL CLocalizer {
public:

    CLocalizer();
    ~CLocalizer();

private:

    std::string saved_locale;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

} // namespace io
} // namespace geos

#endif // GEOS_IO_CLOCALIZER_H
