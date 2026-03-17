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
 * Last port: geom/util/GeometryTransformer.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_UTIL_GEOMETRYTRANSFORMER_H
#define GEOS_GEOM_UTIL_GEOMETRYTRANSFORMER_H


#include <geos/export.h>
#include <geos/geom/Coordinate.h> // destructor visibility for vector
#include <geos/geom/Geometry.h> // destructor visibility for unique_ptr
#include <geos/geom/CoordinateSequence.h> // destructor visibility for unique_ptr

#include <memory> // for unique_ptr
#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class GeometryFactory;
class Point;
class LinearRing;
class LineString;
class Polygon;
class MultiPoint;
class MultiPolygon;
class MultiLineString;
class GeometryCollection;
namespace util {
//class GeometryEditorOperation;
}
}
}


namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/** \brief
 * A framework for processes which transform an input Geometry into
 * an output Geometry, possibly changing its structure and type(s).
 *
 * This class is a framework for implementing subclasses which perform
 * transformations on various different Geometry subclasses.
 * It provides an easy way of applying specific transformations
 * to given geometry types, while allowing unhandled types to be simply copied.
 * Also, the framework ensures that if subcomponents change type
 * the parent geometries types change appropriately to maintain valid structure.
 * Subclasses will override whichever `transformX` methods
 * they need to to handle particular Geometry types.
 *
 * A typically usage would be a transformation that may transform Polygons into
 * Polygons, LineStrings or Points. This class would likely need to override the
 * `GeometryTransformer::transformMultiPolygon(const MultiPolygon* geom,
 * const Geometry* parent)` method to ensure that if input Polygons change type
 * the result is a GeometryCollection, not a MultiPolygon
 *
 * The default behaviour of this class is to simply recursively transform
 * each Geometry component into an identical object by copying.
 *
 * Note that all `transformX` methods may return `null`, to avoid creating
 * empty geometry objects. This will be handled correctly by the transformer.
 * The `GeometryTransformer::transform(const Geometry* nInputGeom)` method
 * itself will always return a geometry object.
 *
 * @see GeometryEditor
 *
 * Possible extensions:
 * getParent() method to return immediate parent e.g. of LinearRings in Polygons
 *
 */
class GEOS_DLL GeometryTransformer {

public:

    GeometryTransformer();

    virtual ~GeometryTransformer() = default;

    std::unique_ptr<Geometry> transform(const Geometry* nInputGeom);

    void setSkipTransformedInvalidInteriorRings(bool b);

protected:

    const GeometryFactory* factory;

    /** \brief
     * Convenience method which provides standard way of
     * creating a CoordinateSequence.
     *
     * @param coords the coordinate array to copy
     * @return a coordinate sequence for the array
     *
     * [final]
     */
    CoordinateSequence::Ptr createCoordinateSequence(
        std::unique_ptr< std::vector<Coordinate> > coords);

    virtual CoordinateSequence::Ptr transformCoordinates(
        const CoordinateSequence* coords,
        const Geometry* parent);

    virtual Geometry::Ptr transformPoint(
        const Point* geom,
        const Geometry* parent);

    virtual Geometry::Ptr transformMultiPoint(
        const MultiPoint* geom,
        const Geometry* parent);

    virtual Geometry::Ptr transformLinearRing(
        const LinearRing* geom,
        const Geometry* parent);

    virtual Geometry::Ptr transformLineString(
        const LineString* geom,
        const Geometry* parent);

    virtual Geometry::Ptr transformMultiLineString(
        const MultiLineString* geom,
        const Geometry* parent);

    virtual Geometry::Ptr transformPolygon(
        const Polygon* geom,
        const Geometry* parent);

    virtual Geometry::Ptr transformMultiPolygon(
        const MultiPolygon* geom,
        const Geometry* parent);

    virtual Geometry::Ptr transformGeometryCollection(
        const GeometryCollection* geom,
        const Geometry* parent);

private:

    const Geometry* inputGeom;

    // these could eventually be exposed to clients
    /**
     * `true` if empty geometries should not be included in the result
     */
    bool pruneEmptyGeometry;

    /**
     * `true` if a homogenous collection result
     * from a {@link GeometryCollection} should still
     * be a general GeometryCollection
     */
    bool preserveGeometryCollectionType;

    /**
     * `true` if the output from a collection argument should still be a collection
     */
    // bool preserveCollections;

    /**
     * `true` if the type of the input should be preserved
     */
    bool preserveType;

    /**
     * `true` if transformed invalid interior rings should be skipped
     */
    bool skipTransformedInvalidInteriorRings;

    // Declare type as noncopyable
    GeometryTransformer(const GeometryTransformer& other) = delete;
    GeometryTransformer& operator=(const GeometryTransformer& rhs) = delete;
};


} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif // GEOS_GEOM_UTIL_GEOMETRYTRANSFORMER_H
