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

#ifndef GEOS_INDEX_STRTREE_GEOMETRYITEMDISTANCE_H
#define GEOS_INDEX_STRTREE_GEOMETRYITEMDISTANCE_H

#include <geos/geom/Geometry.h>
#include <geos/index/strtree/ItemDistance.h>

namespace geos {
namespace index {
namespace strtree {
class GEOS_DLL GeometryItemDistance : public ItemDistance {
public:
    /**
     * Computes the distance between two {@link Geometry} items,
     * using the {@link Geometry#distance(Geometry)} method.
     *
     * @param item1 an item which is a Geometry
     * @param item2 an item which is a Geometry
     * @return the distance between the geometries
     * @throws ClassCastException if either item is not a Geometry
     */
    double distance(const ItemBoundable* item1, const ItemBoundable* item2) override;
};
}
}
}

#endif //GEOS_GEOMETRYITEMDISTANCE_H
