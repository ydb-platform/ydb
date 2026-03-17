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
 **********************************************************************
 *
 * Last port: geom/GeometryFactory.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_GEOMETRYFACTORY_H
#define GEOS_GEOM_GEOMETRYFACTORY_H

#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/export.h>
#include <geos/inline.h>
#include <geos/util.h>
#include <atomic>
#include <vector>
#include <memory>
#include <cassert>
#include <geos/util/IllegalArgumentException.h>

namespace geos {
namespace geom {
class CoordinateSequenceFactory;
class Coordinate;
class CoordinateSequence;
class Envelope;
class Geometry;
class GeometryCollection;
class LineString;
class LinearRing;
class MultiLineString;
class MultiPoint;
class MultiPolygon;
class Polygon;
}
}

namespace geos {
namespace geom { // geos::geom

/**
 * \brief
 * Supplies a set of utility methods for building Geometry objects
 * from CoordinateSequence or other Geometry objects.
 *
 * Note that the factory constructor methods do <b>not</b> change the input
 * coordinates in any way.
 * In particular, they are not rounded to the supplied <tt>PrecisionModel</tt>.
 * It is assumed that input Coordinates meet the given precision.
 */
class GEOS_DLL GeometryFactory {
private:

    struct GeometryFactoryDeleter {
        void
        operator()(GeometryFactory* p) const
        {
            p->destroy();
        }
    };

public:

    using Ptr = std::unique_ptr<GeometryFactory, GeometryFactoryDeleter>;

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having a
     * floating PrecisionModel and a spatial-reference ID of 0.
     */
    static GeometryFactory::Ptr create();

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having
     * the given PrecisionModel, spatial-reference ID, and
     * CoordinateSequence implementation.
     *
     * NOTES:
     * (1) the given PrecisionModel is COPIED
     * (2) the CoordinateSequenceFactory is NOT COPIED
     *     and must be available for the whole lifetime
     *     of the GeometryFactory
     */
    static GeometryFactory::Ptr create(const PrecisionModel* pm, int newSRID,
                                       CoordinateSequenceFactory* nCoordinateSequenceFactory);

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having the
     * given CoordinateSequence implementation, a double-precision floating
     * PrecisionModel and a spatial-reference ID of 0.
     */
    static GeometryFactory::Ptr create(CoordinateSequenceFactory* nCoordinateSequenceFactory);

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having
     * the given PrecisionModel and the default CoordinateSequence
     * implementation.
     *
     * @param pm the PrecisionModel to use
     */
    static GeometryFactory::Ptr create(const PrecisionModel* pm);

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having
     * the given {@link PrecisionModel} and spatial-reference ID,
     * and the default CoordinateSequence implementation.
     *
     * @param pm the PrecisionModel to use, will be copied internally
     * @param newSRID the SRID to use
     */
    static GeometryFactory::Ptr create(const PrecisionModel* pm, int newSRID);

    /**
     * \brief Copy constructor
     *
     * @param gf the GeometryFactory to clone from
     */
    static GeometryFactory::Ptr create(const GeometryFactory& gf);

    /**
     * \brief
     * Return a pointer to the default GeometryFactory.
     * This is a global shared object instantiated
     * using default constructor.
     */
    static const GeometryFactory*
    getDefaultInstance();

//Skipped a lot of list to array convertors

    Point* createPointFromInternalCoord(const Coordinate* coord,
                                        const Geometry* exemplar) const;

    /// Converts an Envelope to a Geometry.
    ///
    /// Returned Geometry can be a Point, a Polygon or an EMPTY geom.
    ///
    std::unique_ptr<Geometry> toGeometry(const Envelope* envelope) const;

    /// \brief
    /// Returns the PrecisionModel that Geometries created by this
    /// factory will be associated with.
    const PrecisionModel* getPrecisionModel() const;

    /// Creates an EMPTY Point
    std::unique_ptr<Point> createPoint(std::size_t coordinateDimension = 2) const;

    /// Creates a Point using the given Coordinate
    Point* createPoint(const Coordinate& coordinate) const;

    /// Creates a Point taking ownership of the given CoordinateSequence
    Point* createPoint(CoordinateSequence* coordinates) const;

    /// Creates a Point with a deep-copy of the given CoordinateSequence.
    Point* createPoint(const CoordinateSequence& coordinates) const;

    /// Construct an EMPTY GeometryCollection
    std::unique_ptr<GeometryCollection> createGeometryCollection() const;

    /// Construct the EMPTY Geometry
    std::unique_ptr<Geometry> createEmptyGeometry() const;

    /// Construct a GeometryCollection taking ownership of given arguments
    GeometryCollection* createGeometryCollection(
        std::vector<Geometry*>* newGeoms) const;

    template<typename T>
    std::unique_ptr<GeometryCollection> createGeometryCollection(
            std::vector<std::unique_ptr<T>> && newGeoms) const {
        // Can't use make_unique because constructor is protected
        return std::unique_ptr<GeometryCollection>(new GeometryCollection(Geometry::toGeometryArray(std::move(newGeoms)), *this));
    }

    /// Constructs a GeometryCollection with a deep-copy of args
    GeometryCollection* createGeometryCollection(
        const std::vector<const Geometry*>& newGeoms) const;

    /// Construct an EMPTY MultiLineString
    std::unique_ptr<MultiLineString> createMultiLineString() const;

    /// Construct a MultiLineString taking ownership of given arguments
    MultiLineString* createMultiLineString(
        std::vector<Geometry*>* newLines) const;

    /// Construct a MultiLineString with a deep-copy of given arguments
    MultiLineString* createMultiLineString(
        const std::vector<const Geometry*>& fromLines) const;

    std::unique_ptr<MultiLineString> createMultiLineString(
            std::vector<std::unique_ptr<LineString>> && fromLines) const;

    std::unique_ptr<MultiLineString> createMultiLineString(
            std::vector<std::unique_ptr<Geometry>> && fromLines) const;

    /// Construct an EMPTY MultiPolygon
    std::unique_ptr<MultiPolygon> createMultiPolygon() const;

    /// Construct a MultiPolygon taking ownership of given arguments
    MultiPolygon* createMultiPolygon(std::vector<Geometry*>* newPolys) const;

    /// Construct a MultiPolygon with a deep-copy of given arguments
    MultiPolygon* createMultiPolygon(
        const std::vector<const Geometry*>& fromPolys) const;

    std::unique_ptr<MultiPolygon> createMultiPolygon(
        std::vector<std::unique_ptr<Polygon>> && fromPolys) const;

    std::unique_ptr<MultiPolygon> createMultiPolygon(
            std::vector<std::unique_ptr<Geometry>> && fromPolys) const;

    /// Construct an EMPTY LinearRing
    std::unique_ptr<LinearRing> createLinearRing() const;

    /// Construct a LinearRing taking ownership of given arguments
    LinearRing* createLinearRing(CoordinateSequence* newCoords) const;

    std::unique_ptr<LinearRing> createLinearRing(
        std::unique_ptr<CoordinateSequence> && newCoords) const;

    /// Construct a LinearRing with a deep-copy of given arguments
    LinearRing* createLinearRing(
        const CoordinateSequence& coordinates) const;

    /// Constructs an EMPTY <code>MultiPoint</code>.
    std::unique_ptr<MultiPoint> createMultiPoint() const;

    /// Construct a MultiPoint taking ownership of given arguments
    MultiPoint* createMultiPoint(std::vector<Geometry*>* newPoints) const;

    std::unique_ptr<MultiPoint> createMultiPoint(std::vector<Coordinate> && newPoints) const;

    std::unique_ptr<MultiPoint> createMultiPoint(std::vector<std::unique_ptr<Point>> && newPoints) const;

    std::unique_ptr<MultiPoint> createMultiPoint(std::vector<std::unique_ptr<Geometry>> && newPoints) const;

    /// Construct a MultiPoint with a deep-copy of given arguments
    MultiPoint* createMultiPoint(
        const std::vector<const Geometry*>& fromPoints) const;

    /// \brief
    /// Construct a MultiPoint containing a Point geometry
    /// for each Coordinate in the given list.
    MultiPoint* createMultiPoint(
        const CoordinateSequence& fromCoords) const;

    /// \brief
    /// Construct a MultiPoint containing a Point geometry
    /// for each Coordinate in the given vector.
    MultiPoint* createMultiPoint(
        const std::vector<Coordinate>& fromCoords) const;

    /// Construct an EMPTY Polygon
    std::unique_ptr<Polygon> createPolygon(std::size_t coordinateDimension = 2) const;

    /// Construct a Polygon taking ownership of given arguments
    Polygon* createPolygon(LinearRing* shell,
                           std::vector<LinearRing*>* holes) const;

    std::unique_ptr<Polygon> createPolygon(std::unique_ptr<LinearRing> && shell) const;

    std::unique_ptr<Polygon> createPolygon(std::unique_ptr<LinearRing> && shell,
                                           std::vector<std::unique_ptr<LinearRing>> && holes) const;

    /// Construct a Polygon with a deep-copy of given arguments
    Polygon* createPolygon(const LinearRing& shell,
                           const std::vector<LinearRing*>& holes) const;

    /// Construct an EMPTY LineString
    std::unique_ptr<LineString> createLineString(std::size_t coordinateDimension = 2) const;

    /// Copy a LineString
    std::unique_ptr<LineString> createLineString(const LineString& ls) const;

    /// Construct a LineString taking ownership of given argument
    LineString* createLineString(CoordinateSequence* coordinates) const;

    std::unique_ptr<LineString> createLineString(
        std::unique_ptr<CoordinateSequence> && coordinates) const;

    /// Construct a LineString with a deep-copy of given argument
    LineString* createLineString(
        const CoordinateSequence& coordinates) const;

    /**
    * Creates an empty atomic geometry of the given dimension.
    * If passed a dimension of -1 will create an empty {@link GeometryCollection}.
    *
    * @param dimension the required dimension (-1, 0, 1 or 2)
    * @return an empty atomic geometry of given dimension
    */
    std::unique_ptr<Geometry> createEmpty(int dimension) const;

    /**
     *  Build an appropriate <code>Geometry</code>, <code>MultiGeometry</code>, or
     *  <code>GeometryCollection</code> to contain the <code>Geometry</code>s in
     *  it.
     *
     *  For example:
     *
     *    - If <code>geomList</code> contains a single <code>Polygon</code>,
     *      the <code>Polygon</code> is returned.
     *    - If <code>geomList</code> contains several <code>Polygon</code>s, a
     *      <code>MultiPolygon</code> is returned.
     *    - If <code>geomList</code> contains some <code>Polygon</code>s and
     *      some <code>LineString</code>s, a <code>GeometryCollection</code> is
     *      returned.
     *    - If <code>geomList</code> is empty, an empty
     *      <code>GeometryCollection</code> is returned
     *    .
     *
     * Note that this method does not "flatten" Geometries in the input,
     * and hence if any MultiGeometries are contained in the input a
     * GeometryCollection containing them will be returned.
     *
     * @param  geoms  the <code>Geometry</code>s to combine
     *
     * @return A <code>Geometry</code> of the "smallest", "most type-specific"
     *         class that can contain the elements of <code>geomList</code>.
     *
     * NOTE: the returned Geometry will take ownership of the
     *       given vector AND its elements
     */
    Geometry* buildGeometry(std::vector<Geometry*>* geoms) const;

    std::unique_ptr<Geometry> buildGeometry(std::vector<std::unique_ptr<Geometry>> && geoms) const;

    std::unique_ptr<Geometry> buildGeometry(std::vector<std::unique_ptr<Point>> && geoms) const;

    std::unique_ptr<Geometry> buildGeometry(std::vector<std::unique_ptr<LineString>> && geoms) const;

    std::unique_ptr<Geometry> buildGeometry(std::vector<std::unique_ptr<Polygon>> && geoms) const;

    /// See buildGeometry(std::vector<Geometry *>&) for semantics
    //
    /// Will clone the geometries accessible trough the iterator.
    ///
    /// @tparam T an iterator yelding something which casts to const Geometry*
    /// @param from start iterator
    /// @param toofar end iterator
    ///
    template <class T>
    std::unique_ptr<Geometry>
    buildGeometry(T from, T toofar) const
    {
        bool isHeterogeneous = false;
        size_t count = 0;
        int geomClass = -1;
        for(T i = from; i != toofar; ++i) {
            ++count;
            const Geometry* g = *i;
            if(geomClass < 0) {
                geomClass = g->getSortIndex();
            }
            else if(geomClass != g->getSortIndex()) {
                isHeterogeneous = true;
            }
        }

        // for the empty geometry, return an empty GeometryCollection
        if(count == 0) {
            return std::unique_ptr<Geometry>(createGeometryCollection());
        }

        // for the single geometry, return a clone
        if(count == 1) {
            return (*from)->clone();
        }

        // Now we know it is a collection

        // FIXME:
        // Until we tweak all the createMulti* interfaces
        // to support taking iterators we'll have to build
        // a custom vector here.
        std::vector<std::unique_ptr<Geometry>> fromGeoms;
        for(T i = from; i != toofar; ++i) {
            fromGeoms.push_back((*i)->clone());
        }

        // for an heterogeneous ...
        if(isHeterogeneous) {
            return createGeometryCollection(std::move(fromGeoms));
        }

        // At this point we know the collection is not hetereogenous.
        switch((*from)->getDimension()) {
            case Dimension::A: return createMultiPolygon(std::move(fromGeoms));
            case Dimension::L: return createMultiLineString(std::move(fromGeoms));
            case Dimension::P: return createMultiPoint(std::move(fromGeoms));
            default:
                throw geos::util::IllegalArgumentException(std::string("Invalid geometry type."));
        }
    }

    /** \brief
     * This function does the same thing of the omonimouse function
     * taking vector pointer instead of reference.
     *
     * The difference is that this version will copy needed data
     * leaving ownership to the caller.
     */
    Geometry* buildGeometry(const std::vector<const Geometry*>& geoms) const;

    int getSRID() const;

    /// \brief
    /// Returns the CoordinateSequenceFactory associated
    /// with this GeometryFactory
    const CoordinateSequenceFactory* getCoordinateSequenceFactory() const;

    /// Returns a clone of given Geometry.
    Geometry* createGeometry(const Geometry* g) const;

    /// Destroy a Geometry, or release it
    void destroyGeometry(Geometry* g) const;

    /// Request that the instance is deleted.
    ///
    /// It will really be deleted only after last child Geometry is
    /// deleted. Do not use the instance anymore after calling this function
    /// (unless you're a live child!).
    ///
    void destroy();

protected:

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having a
     * floating PrecisionModel and a spatial-reference ID of 0.
     */
    GeometryFactory();

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having
     * the given PrecisionModel, spatial-reference ID, and
     * CoordinateSequence implementation.
     *
     * NOTES:
     * (1) the given PrecisionModel is COPIED
     * (2) the CoordinateSequenceFactory is NOT COPIED
     *     and must be available for the whole lifetime
     *     of the GeometryFactory
     */
    GeometryFactory(const PrecisionModel* pm, int newSRID,
                    CoordinateSequenceFactory* nCoordinateSequenceFactory);

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having the
     * given CoordinateSequence implementation, a double-precision floating
     * PrecisionModel and a spatial-reference ID of 0.
     */
    GeometryFactory(CoordinateSequenceFactory* nCoordinateSequenceFactory);

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having
     * the given PrecisionModel and the default CoordinateSequence
     * implementation.
     *
     * @param pm the PrecisionModel to use
     */
    GeometryFactory(const PrecisionModel* pm);

    /**
     * \brief
     * Constructs a GeometryFactory that generates Geometries having
     * the given {@link PrecisionModel} and spatial-reference ID,
     * and the default CoordinateSequence implementation.
     *
     * @param pm the PrecisionModel to use, will be copied internally
     * @param newSRID the SRID to use
     */
    GeometryFactory(const PrecisionModel* pm, int newSRID);

    /**
     * \brief Copy constructor
     *
     * @param gf the GeometryFactory to clone from
     */
    GeometryFactory(const GeometryFactory& gf);

    /// Destructor
    virtual ~GeometryFactory();

private:

    PrecisionModel precisionModel;
    int SRID;
    const CoordinateSequenceFactory* coordinateListFactory;

    mutable std::atomic_int _refCount;
    bool _autoDestroy;

    friend class Geometry;

    void addRef() const;
    void dropRef() const;

};

} // namespace geos::geom
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geom/GeometryFactory.inl"
#endif

#endif // ndef GEOS_GEOM_GEOMETRYFACTORY_H
