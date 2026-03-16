/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2016 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/strtree/GeometryItemDistance.java (JTS-1.14)
 *
 **********************************************************************/

#include <geos/geom/Geometry.h>
#include <geos/index/strtree/ItemBoundable.h>
#include <geos/index/strtree/GeometryItemDistance.h>

using namespace geos::geom;
using namespace geos::index::strtree;

double
GeometryItemDistance::distance(const ItemBoundable* item1, const ItemBoundable* item2)
{
    const Geometry* g1 = (Geometry*) item1->getItem();
    const Geometry* g2 = (Geometry*) item2->getItem();
    return g1->distance(g2);
}
