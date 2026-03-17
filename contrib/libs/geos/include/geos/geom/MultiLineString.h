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
 * Last port: geom/MultiLineString.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOS_MULTILINESTRING_H
#define GEOS_GEOS_MULTILINESTRING_H

#include <geos/export.h>
#include <geos/geom/GeometryCollection.h> // for inheritance
#include <geos/geom/Dimension.h>
#include <geos/geom/LineString.h>

#include <string>
#include <vector>

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

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4250) // T1 inherits T2 via dominance
#endif

/// Models a collection of [LineStrings](@ref geom::LineString).
class GEOS_DLL MultiLineString: public GeometryCollection {

public:

    friend class GeometryFactory;

    ~MultiLineString() override = default;

    /// Returns line dimension (1)
    Dimension::DimensionType getDimension() const override;

    bool isDimensionStrict(Dimension::DimensionType d) const override {
        return d == Dimension::L;
    }

    /**
     * \brief
     * Returns Dimension::False if all [LineStrings](@ref geom::LineString) in the collection
     * are closed, 0 otherwise.
     */
    int getBoundaryDimension() const override;

    /// Returns a (possibly empty) [MultiPoint](@ref geom::MultiPoint)
    std::unique_ptr<Geometry> getBoundary() const override;

    const LineString* getGeometryN(std::size_t n) const override;

    std::string getGeometryType() const override;

    GeometryTypeId getGeometryTypeId() const override;

    bool isClosed() const;

    bool equalsExact(const Geometry* other, double tolerance = 0) const override;

    std::unique_ptr<Geometry> clone() const override;

    /**
     * Creates a MultiLineString in the reverse
     * order to this object.
     * Both the order of the component LineStrings
     * and the order of their coordinate sequences
     * are reversed.
     *
     * @return a MultiLineString in the reverse order
     */
    std::unique_ptr<Geometry> reverse() const override;

protected:

    /**
     * \brief Constructs a MultiLineString.
     *
     * @param  newLines The [LineStrings](@ref geom::LineString) for this
     *                  MultiLineString, or `null`
     *                  or an empty array to create the empty geometry.
     *                  Elements may be empty LineString,
     *                  but not `null`s.
     *
     * @param newFactory The GeometryFactory used to create this geometry.
     *                   Caller must keep the factory alive for the life-time
     *                   of the constructed MultiLineString.
     *
     * @note Constructed object will take ownership of
     *       the vector and its elements.
     *
     */
    MultiLineString(std::vector<Geometry*>* newLines,
                    const GeometryFactory* newFactory);

    MultiLineString(std::vector<std::unique_ptr<LineString>> && newLines,
            const GeometryFactory& newFactory);

    MultiLineString(std::vector<std::unique_ptr<Geometry>> && newLines,
                    const GeometryFactory& newFactory);

    MultiLineString(const MultiLineString& mp);

    int
    getSortIndex() const override
    {
        return SORTINDEX_MULTILINESTRING;
    };

};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

} // namespace geos::geom
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geom/MultiLineString.inl"
#endif

#endif // ndef GEOS_GEOS_MULTILINESTRING_H
