/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_OP_UNION_GEOMETRYLISTHOLDER_H
#define GEOS_OP_UNION_GEOMETRYLISTHOLDER_H

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
}
}

namespace geos {
namespace operation { // geos::operation
namespace geounion {  // geos::operation::geounion

/**
 * \brief Helper class holding Geometries, part of which are held by reference
 *        others are held exclusively.
 */
class GeometryListHolder : public std::vector<geom::Geometry*> {
private:
    typedef std::vector<geom::Geometry*> base_type;

public:
    GeometryListHolder() {}
    ~GeometryListHolder()
    {
        std::for_each(ownedItems.begin(), ownedItems.end(),
                      &GeometryListHolder::deleteItem);
    }

    // items need to be deleted in the end
    void
    push_back_owned(geom::Geometry* item)
    {
        this->base_type::push_back(item);
        ownedItems.push_back(item);
    }

    geom::Geometry*
    getGeometry(std::size_t index)
    {
        if(index >= this->base_type::size()) {
            return nullptr;
        }
        return (*this)[index];
    }

private:
    static void deleteItem(geom::Geometry* item);

private:
    std::vector<geom::Geometry*> ownedItems;
};

} // namespace geos::operation::union
} // namespace geos::operation
} // namespace geos

#endif
