/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/LinearRing.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOS_LINEARRING_H
#define GEOS_GEOS_LINEARRING_H

#include <geos/export.h>
#include <string>
#include <vector>
#include <geos/geom/LineString.h>

#include <geos/inline.h>

// Forward declarations
namespace geos {
namespace geom { // geos::geom
class Coordinate;
class CoordinateArraySequence;
}
}

namespace geos {
namespace geom { // geos::geom

/**
 * \brief
 * Models an OGC SFS LinearRing. A LinearRing is a LineString which is both
 * closed and simple.
 *
 * In other words, the first and last coordinate in the ring must be equal,
 * and the interior of the ring must not self-intersect.  Either orientation
 * of the ring is allowed.
 *
 * A ring must have either 0 or 4 or more points. The first and last points
 * must be equal (in 2D). If these conditions are not met, the constructors
 * throw an {@link geos::util::IllegalArgumentException}
 */
class GEOS_DLL LinearRing : public LineString {

public:

    /** \brief
     * The minimum number of vertices allowed in a valid non-empty ring (= 4).
     * Empty rings with 0 vertices are also valid.
     */
    static const unsigned int MINIMUM_VALID_SIZE = 4;

    LinearRing(const LinearRing& lr);

    /**
     * \brief Constructs a LinearRing with the given points.
     *
     * @param  points  points forming a closed and simple linestring, or
     *      <code>null</code> or an empty array to create the empty
     *      geometry.
     *      This array must not contain <code>null</code> elements.
     *	If not null LinearRing will take ownership of points.
     *
     * @param newFactory the GeometryFactory used to create this geometry
     *
     */
    LinearRing(CoordinateSequence* points,
               const GeometryFactory* newFactory);

    /// Hopefully cleaner version of the above
    LinearRing(CoordinateSequence::Ptr && points,
            const GeometryFactory& newFactory);

    std::unique_ptr<Geometry>
    clone() const override
    {
        return std::unique_ptr<Geometry>(new LinearRing(*this));
    }

    ~LinearRing() override = default;

    /** \brief
     * Returns <code>Dimension.FALSE</code>, since by definition
     * LinearRings do not have a boundary.
     *
     * @return Dimension::False
     */
    int getBoundaryDimension() const override;

    bool isClosed() const override;

    std::string getGeometryType() const override;

    GeometryTypeId getGeometryTypeId() const override;

    void setPoints(const CoordinateSequence* cl);

    std::unique_ptr<Geometry> reverse() const override;

protected:

    int
    getSortIndex() const override
    {
        return SORTINDEX_LINEARRING;
    };


private:

    void validateConstruction();
};


} // namespace geos::geom
} // namespace geos

#endif // ndef GEOS_GEOS_LINEARRING_H
