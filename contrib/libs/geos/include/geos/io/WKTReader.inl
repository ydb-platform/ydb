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
 * Last port: io/WKTReader.java rev. 1.1 (JTS-1.7)
 *
 **********************************************************************/

#ifndef GEOS_IO_WKTREADER_INL
#define GEOS_IO_WKTREADER_INL

#include <geos/io/WKTReader.h>
#include <geos/geom/GeometryFactory.h>

#if GEOS_DEBUG
# include <iostream>
#endif

namespace geos {
namespace io {

INLINE
WKTReader::WKTReader(const geom::GeometryFactory* gf)
    :
    geometryFactory(gf),
    precisionModel(gf->getPrecisionModel())
{
#if GEOS_DEBUG
    std::cerr << "\nGEOS_DEBUG: WKTReader::WKTReader(const GeometryFactory *gf)\n";
#endif
}

INLINE
WKTReader::WKTReader(const geom::GeometryFactory& gf)
    :
    geometryFactory(&gf),
    precisionModel(gf.getPrecisionModel())
{
#if GEOS_DEBUG
    std::cerr << "\nGEOS_DEBUG: WKTReader::WKTReader(const GeometryFactory &gf)\n";
#endif
}

INLINE
WKTReader::WKTReader()
    :
    geometryFactory(geom::GeometryFactory::getDefaultInstance()),
    precisionModel(geometryFactory->getPrecisionModel())
{
#if GEOS_DEBUG
    std::cerr << "\nGEOS_DEBUG: WKTReader::WKTReader()\n";
#endif
}

INLINE
WKTReader::~WKTReader()
{
#if GEOS_DEBUG
    std::cerr << "\nGEOS_DEBUG: WKTReader::~WKTReader()\n";
#endif
}

} // namespace io
} // namespace geos

#endif // #ifndef GEOS_IO_WKTREADER_INL
