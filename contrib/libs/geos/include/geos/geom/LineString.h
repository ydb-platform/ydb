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
 * Last port: geom/LineString.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOS_LINESTRING_H
#define GEOS_GEOS_LINESTRING_H

#include <geos/export.h>
#include <geos/geom/Geometry.h> // for inheritance
#include <geos/geom/CoordinateSequence.h> // for proper use of unique_ptr<>
#include <geos/geom/Envelope.h> // for proper use of unique_ptr<>
#include <geos/geom/Dimension.h> // for Dimension::DimensionType

#include <string>
#include <vector>
#include <memory> // for unique_ptr

#include <geos/inline.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace geom {
class Coordinate;
class CoordinateArraySequence;
class CoordinateSequenceFilter;
}
}

namespace geos {
namespace geom { // geos::geom

/**
 *  Models an OGC-style <code>LineString</code>.
 *
 *  A LineString consists of a sequence of two or more vertices,
 *  along with all points along the linearly-interpolated curves
 *  (line segments) between each
 *  pair of consecutive vertices.
 *  Consecutive vertices may be equal.
 *  The line segments in the line may intersect each other (in other words,
 *  the linestring may "curl back" in itself and self-intersect).
 *  Linestrings with exactly two identical points are invalid.
 *
 *  A linestring must have either 0 or 2 or more points.
 *  If these conditions are not met, the constructors throw
 *  an {@link util::IllegalArgumentException}.
 */
class GEOS_DLL LineString: public Geometry {

public:

    friend class GeometryFactory;

    /// A vector of const LineString pointers
    typedef std::vector<const LineString*> ConstVect;

    ~LineString() override;

    /**
     * \brief
     * Creates and returns a full copy of this {@link LineString} object
     * (including all coordinates contained by it)
     *
     * @return A clone of this instance
     */
    std::unique_ptr<Geometry> clone() const override;

    std::unique_ptr<CoordinateSequence> getCoordinates() const override;

    /// Returns a read-only pointer to internal CoordinateSequence
    const CoordinateSequence* getCoordinatesRO() const;

    virtual const Coordinate& getCoordinateN(size_t n) const;

    /// Returns line dimension (1)
    Dimension::DimensionType getDimension() const override;

    /**
     * \brief
     * Returns Dimension::False for a closed LineString,
     * 0 otherwise (LineString boundary is a MultiPoint)
     */
    int getBoundaryDimension() const override;

    /// Returns coordinate dimension.
    uint8_t getCoordinateDimension() const override;

    /**
     * \brief
     * Returns a MultiPoint.
     * Empty for closed LineString, a Point for each vertex otherwise.
     */
    std::unique_ptr<Geometry> getBoundary() const override;

    bool isEmpty() const override;

    std::size_t getNumPoints() const override;

    virtual std::unique_ptr<Point> getPointN(std::size_t n) const;

    /// \brief
    /// Return the start point of the LineString
    /// or NULL if this is an EMPTY LineString.
    ///
    virtual std::unique_ptr<Point> getStartPoint() const;

    /// \brief
    /// Return the end point of the LineString
    /// or NULL if this is an EMPTY LineString.
    ///
    virtual std::unique_ptr<Point> getEndPoint() const;

    virtual bool isClosed() const;

    virtual bool isRing() const;

    std::string getGeometryType() const override;

    GeometryTypeId getGeometryTypeId() const override;

    virtual bool isCoordinate(Coordinate& pt) const;

    bool equalsExact(const Geometry* other, double tolerance = 0)
    const override;

    void apply_rw(const CoordinateFilter* filter) override;

    void apply_ro(CoordinateFilter* filter) const override;

    void apply_rw(GeometryFilter* filter) override;

    void apply_ro(GeometryFilter* filter) const override;

    void apply_rw(GeometryComponentFilter* filter) override;

    void apply_ro(GeometryComponentFilter* filter) const override;

    void apply_rw(CoordinateSequenceFilter& filter) override;

    void apply_ro(CoordinateSequenceFilter& filter) const override;

    /** \brief
     * Normalizes a LineString.
     *
     * A normalized linestring
     * has the first point which is not equal to its reflected point
     * less than the reflected point.
     */
    void normalize() override;

    //was protected
    int compareToSameClass(const Geometry* ls) const override;

    const Coordinate* getCoordinate() const override;

    double getLength() const override;

    /**
     * Creates a LineString whose coordinates are in the reverse
     * order of this object's
     *
     * @return a LineString with coordinates in the reverse order
     */
    std::unique_ptr<Geometry> reverse() const override;

protected:

    LineString(const LineString& ls);

    /// \brief
    /// Constructs a LineString taking ownership the
    /// given CoordinateSequence.
    LineString(CoordinateSequence* pts, const GeometryFactory* newFactory);

    /// Hopefully cleaner version of the above
    LineString(CoordinateSequence::Ptr && pts,
               const GeometryFactory& newFactory);

    Envelope::Ptr computeEnvelopeInternal() const override;

    CoordinateSequence::Ptr points;

    int
    getSortIndex() const override
    {
        return SORTINDEX_LINESTRING;
    };

private:

    void validateConstruction();
    void normalizeClosed();


};

struct GEOS_DLL  LineStringLT {
    bool
    operator()(const LineString* ls1, const LineString* ls2) const
    {
        return ls1->compareTo(ls2) < 0;
    }
};


inline std::unique_ptr<Geometry>
LineString::clone() const
{
    return std::unique_ptr<Geometry>(new LineString(*this));
}

} // namespace geos::geom
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_GEOS_LINESTRING_H
