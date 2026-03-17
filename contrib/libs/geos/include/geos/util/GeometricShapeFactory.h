/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: util/GeometricShapeFactory.java rev 1.14 (JTS-1.10+)
 * (2009-03-19)
 *
 **********************************************************************/

#ifndef GEOS_UTIL_GEOMETRICSHAPEFACTORY_H
#define GEOS_UTIL_GEOMETRICSHAPEFACTORY_H

#include <geos/export.h>
#include <cassert>
#include <memory>

#include <geos/geom/Coordinate.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Envelope;
class Polygon;
class GeometryFactory;
class PrecisionModel;
class LineString;
}
}

namespace geos {
namespace util { // geos::util


/** \brief
 * Computes various kinds of common geometric shapes.
 *
 * Allows various ways of specifying the location and extent of the shapes,
 * as well as number of line segments used to form them.
 *
 * Example:
 * <pre>
 *  GeometricShapeFactory gsf(factory);
 *  gsf.setSize(100);
 *  gsf.setNumPoints(100);
 *  gsf.setBase(Coordinate(0, 0));
 *  std::unique_ptr<Polygon> rect ( gsf.createRectangle() );
 * </pre>
 *
 */
class GEOS_DLL GeometricShapeFactory {
protected:
    class Dimensions {
    public:
        Dimensions();
        geom::Coordinate base;
        geom::Coordinate centre;
        double width;
        double height;
        void setBase(const geom::Coordinate& newBase);
        void setCentre(const geom::Coordinate& newCentre);
        void setSize(double size);
        void setWidth(double nWidth);
        void setHeight(double nHeight);

        // Return newly-allocated object, ownership transferred
        std::unique_ptr<geom::Envelope> getEnvelope() const;
    };
    const geom::GeometryFactory* geomFact; // externally owned
    const geom::PrecisionModel* precModel; // externally owned
    Dimensions dim;
    uint32_t nPts;

    geom::Coordinate coord(double x, double y) const;

public:

    /**
     * \brief
     * Create a shape factory which will create shapes using the given
     * GeometryFactory.
     *
     * @param factory the factory to use. You need to keep the factory
     *                alive for the whole GeometricShapeFactory life time.
     */
    GeometricShapeFactory(const geom::GeometryFactory* factory);

    virtual
    ~GeometricShapeFactory() {}

    /**
     * \brief Creates an elliptical arc, as a LineString.
     *
     * The arc is always created in a counter-clockwise direction.
     *
     * @param startAng start angle in radians
     * @param angExtent size of angle in radians
     * @return an elliptical arc
     */
    std::unique_ptr<geom::LineString> createArc(double startAng, double angExtent);

    /**
     * \brief Creates an elliptical arc polygon.
     *
     * The polygon is formed from the specified arc of an ellipse
     * and the two radii connecting the endpoints to the centre of
     * the ellipse.
     *
     * @param startAng start angle in radians
     * @param angExt size of angle in radians
     * @return an elliptical arc polygon
     */
    std::unique_ptr<geom::Polygon> createArcPolygon(double startAng, double angExt);

    /**
     * \brief Creates a circular Polygon.
     *
     * @return a circle
     */
    std::unique_ptr<geom::Polygon> createCircle();

    /**
     * \brief Creates a rectangular Polygon.
     *
     * @return a rectangular Polygon
     */
    std::unique_ptr<geom::Polygon> createRectangle();

    /**
     * \brief
     * Sets the location of the shape by specifying the base coordinate
     * (which in most cases is the * lower left point of the envelope
     * containing the shape).
     *
     * @param base the base coordinate of the shape
     */
    void setBase(const geom::Coordinate& base);

    /**
     * \brief
     * Sets the location of the shape by specifying the centre of
     * the shape's bounding box
     *
     * @param centre the centre coordinate of the shape
     */
    void setCentre(const geom::Coordinate& centre);

    /**
     * \brief Sets the height of the shape.
     *
     * @param height the height of the shape
     */
    void setHeight(double height);

    /**
     * \brief Sets the total number of points in the created Geometry
     */
    void setNumPoints(uint32_t nNPts);

    /**
     * \brief
     * Sets the size of the extent of the shape in both x and y directions.
     *
     * @param size the size of the shape's extent
     */
    void setSize(double size);

    /**
     * \brief Sets the width of the shape.
     *
     * @param width the width of the shape
     */
    void setWidth(double width);

};

} // namespace geos::util
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_UTIL_GEOMETRICSHAPEFACTORY_H
