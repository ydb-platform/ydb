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

#include <geos/io/CLocalizer.h>

#include <string>
#include <clocale>

using namespace std;

namespace geos {
namespace io {

CLocalizer::CLocalizer()
{
#ifdef _MSC_VER
    // Avoid multithreading issues caused by setlocale
    _configthreadlocale(_ENABLE_PER_THREAD_LOCALE);
#endif
    char* p = std::setlocale(LC_NUMERIC, nullptr);
    if(nullptr != p) {
        saved_locale = p;
    }
    std::setlocale(LC_NUMERIC, "C");
}

CLocalizer::~CLocalizer()
{
    std::setlocale(LC_NUMERIC, saved_locale.c_str());
}

} // namespace geos.io
} // namespace geos

