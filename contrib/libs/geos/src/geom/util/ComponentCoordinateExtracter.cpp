/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/


#include <geos/geom/util/ComponentCoordinateExtracter.h>
#include <typeinfo>

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

ComponentCoordinateExtracter::ComponentCoordinateExtracter(std::vector<const Coordinate*>& newComps)
    :
    comps(newComps)
{}

void
ComponentCoordinateExtracter::filter_rw(Geometry* geom)
{
    if (geom->isEmpty())
        return;
    if(geom->getGeometryTypeId() == geos::geom::GEOS_LINEARRING
            ||	geom->getGeometryTypeId() == geos::geom::GEOS_LINESTRING
            ||	geom->getGeometryTypeId() == geos::geom::GEOS_POINT) {
        comps.push_back(geom->getCoordinate());
    }
    //if (	typeid( *geom ) == typeid( LineString )
    //	||	typeid( *geom ) == typeid( Point ) )
    //if ( const Coordinate *ls=dynamic_cast<const Coordinate *>(geom) )
    //	comps.push_back(ls);
}

void
ComponentCoordinateExtracter::filter_ro(const Geometry* geom)
{
    if (geom->isEmpty())
        return;
    //if (	typeid( *geom ) == typeid( LineString )
    //	||	typeid( *geom ) == typeid( Point ) )
    if(geom->getGeometryTypeId() == geos::geom::GEOS_LINEARRING
            ||	geom->getGeometryTypeId() == geos::geom::GEOS_LINESTRING
            ||	geom->getGeometryTypeId() == geos::geom::GEOS_POINT) {
        comps.push_back(geom->getCoordinate());
    }
    //if ( const Coordinate *ls=dynamic_cast<const Coordinate *>(geom) )
    //	comps.push_back(ls);
}


void
ComponentCoordinateExtracter::getCoordinates(const Geometry& geom, std::vector<const Coordinate*>& ret)
{
    ComponentCoordinateExtracter cce(ret);
    geom.apply_ro(&cce);
}

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos
