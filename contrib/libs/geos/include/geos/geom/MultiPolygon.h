/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: geom/MultiPolygon.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOS_MULTIPOLYGON_H
#define GEOS_GEOS_MULTIPOLYGON_H

#include <geos/export.h>
#include <string>
#include <vector>
#include <geos/geom/GeometryCollection.h> // for inheritance
#include <geos/geom/Polygon.h> // for inheritance
#include <geos/geom/Dimension.h> // for Dimension::DimensionType

#include <geos/inline.h>

// Forward declarations
namespace geos {
namespace geom { // geos::geom
class Coordinate;
class CoordinateArraySequence;
class MultiPoint;
}
}


namespace geos {
namespace geom { // geos::geom

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4250) // T1 inherits T2 via dominance
#endif

/// Models a collection of {@link Polygon}s.
///
/// As per the OGC SFS specification,
/// the Polygons in a MultiPolygon may not overlap,
/// and may only touch at single points.
/// This allows the topological point-set semantics
/// to be well-defined.
///
class GEOS_DLL MultiPolygon: public GeometryCollection {
public:

    friend class GeometryFactory;

    ~MultiPolygon() override;

    /// Returns surface dimension (2)
    Dimension::DimensionType getDimension() const override;

    bool isDimensionStrict(Dimension::DimensionType d) const override {
        return d == Dimension::A;
    }

    /// Returns 1 (MultiPolygon boundary is MultiLineString)
    int getBoundaryDimension() const override;

    /** \brief
     * Computes the boundary of this geometry
     *
     * @return a lineal geometry (which may be empty)
     * @see Geometry#getBoundary
     */
    std::unique_ptr<Geometry> getBoundary() const override;

    const Polygon* getGeometryN(std::size_t n) const override;

    std::string getGeometryType() const override;

    GeometryTypeId getGeometryTypeId() const override;

    bool equalsExact(const Geometry* other, double tolerance = 0) const override;

    std::unique_ptr<Geometry> clone() const override;

    std::unique_ptr<Geometry> reverse() const override;

protected:

    /**
     * \brief Construct a MultiPolygon
     *
     * @param newPolys
     *	the <code>Polygon</code>s for this <code>MultiPolygon</code>,
     *	or <code>null</code> or an empty array to create the empty
     *	geometry. Elements may be empty <code>Polygon</code>s, but
     *	not <code>null</code>s.
     *	The polygons must conform to the assertions specified in the
     *	<A HREF="http://www.opengis.org/techno/specs.htm">
     *	OpenGIS Simple Features Specification for SQL
     *	</A>.
     *
     *	Constructed object will take ownership of
     *	the vector and its elements.
     *
     * @param newFactory
     * 	The GeometryFactory used to create this geometry
     *	Caller must keep the factory alive for the life-time
     *	of the constructed MultiPolygon.
     */
    MultiPolygon(std::vector<Geometry*>* newPolys, const GeometryFactory* newFactory);

    MultiPolygon(std::vector<std::unique_ptr<Polygon>> && newPolys,
            const GeometryFactory& newFactory);

    MultiPolygon(std::vector<std::unique_ptr<Geometry>> && newPolys,
                 const GeometryFactory& newFactory);

    MultiPolygon(const MultiPolygon& mp);

    int
    getSortIndex() const override
    {
        return SORTINDEX_MULTIPOLYGON;
    };

};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

} // namespace geos::geom
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geom/MultiPolygon.inl"
#endif

#endif // ndef GEOS_GEOS_MULTIPOLYGON_H
