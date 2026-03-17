/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geomgraph/TopologyLocation.java r428 (JTS-1.12+)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_TOPOLOGYLOCATION_H
#define GEOS_GEOMGRAPH_TOPOLOGYLOCATION_H
#include <vector>
#include <array>
#include <string>
#include <cassert>
#include <cstdint>

#include <geos/export.h>
#include <geos/inline.h>
#include <geos/geom/Location.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace geomgraph { // geos.geomgraph

/** \brief
 * A TopologyLocation is the labelling of a
 * GraphComponent's topological relationship to a single Geometry.
 *
 * If the parent component is an area edge, each side and the edge itself
 * have a topological location.  These locations are named
 *
 *  - ON: on the edge
 *  - LEFT: left-hand side of the edge
 *  - RIGHT: right-hand side
 *
 * If the parent component is a line edge or node, there is a single
 * topological relationship attribute, ON.
 *
 * The possible values of a topological location are
 * {Location::NONE, Location::EXTERIOR, Location::BOUNDARY, Location::INTERIOR}
 *
 * The labelling is stored in an array location[j] where
 * where j has the values ON, LEFT, RIGHT
 */
class GEOS_DLL TopologyLocation {

public:

    friend std::ostream& operator<< (std::ostream&, const TopologyLocation&);

    TopologyLocation() = default;

    /** \brief
     * Constructs a TopologyLocation specifying how points on, to the
     * left of, and to the right of some GraphComponent relate to some
     * Geometry.
     *
     * Possible values for the
     * parameters are Location::NONE, Location::EXTERIOR, Location::BOUNDARY,
     * and Location::INTERIOR.
     *
     * @see Location
     */
    TopologyLocation(geom::Location on, geom::Location left, geom::Location right);

    TopologyLocation(geom::Location on);

    TopologyLocation(const TopologyLocation& gl);

    TopologyLocation& operator= (const TopologyLocation& gl);

    geom::Location get(std::size_t posIndex) const;

    /**
     * @return true if all locations are Location::NONE
     */
    bool isNull() const;

    /**
     * @return true if any locations is Location::NONE
     */
    bool isAnyNull() const;

    bool isEqualOnSide(const TopologyLocation& le, uint32_t locIndex) const;

    bool isArea() const;

    bool isLine() const;

    void flip();

    void setAllLocations(geom::Location locValue);

    void setAllLocationsIfNull(geom::Location locValue);

    void setLocation(std::size_t locIndex, geom::Location locValue);

    void setLocation(geom::Location locValue);

    /// Warning: returns reference to owned memory
    const std::array<geom::Location, 3>& getLocations() const;

    void setLocations(geom::Location on, geom::Location left, geom::Location right);

    bool allPositionsEqual(geom::Location loc) const;

    /** \brief
     * merge updates only the UNDEF attributes of this object
     * with the attributes of another.
     */
    void merge(const TopologyLocation& gl);

    std::string toString() const;

private:

    std::array<geom::Location, 3> location;
    std::uint8_t locationSize;
};

std::ostream& operator<< (std::ostream&, const TopologyLocation&);

} // namespace geos.geomgraph
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geomgraph/TopologyLocation.inl"
#endif

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ifndef GEOS_GEOMGRAPH_TOPOLOGYLOCATION_H

