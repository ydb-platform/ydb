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
 * Last port: index/strtree/ItemDistance.java (JTS-1.14)
 *
 **********************************************************************/

#ifndef GEOS_INDEX_STRTREE_ITEMDISTANCE_H
#define GEOS_INDEX_STRTREE_ITEMDISTANCE_H

#include <geos/index/strtree/ItemBoundable.h>

namespace geos {
namespace index {
namespace strtree {

/** \brief
 * A function method which computes the distance between two [ItemBoundables](\ref ItemBoundable)
 * in an STRtree. Used for Nearest Neighbour searches.
 *
 * \author Martin Davis
 */
class GEOS_DLL ItemDistance {
public:
    /** \brief
     * Computes the distance between two items.
     *
     * @param item1
     * @param item2
     * @return the distance between the items
     *
     * @throws IllegalArgumentException if the metric is not applicable to the arguments
     */
    virtual double distance(const ItemBoundable* item1, const ItemBoundable* item2) = 0;
    virtual ~ItemDistance() = default;
};
}
}
}

#endif //GEOS_ITEMDISTANCE_H
