/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2014 Mika Heiskanen <mika.heiskanen@fmi.fi>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_OP_INTERSECTION_RECTANGLE_H
#define GEOS_OP_INTERSECTION_RECTANGLE_H

#include <geos/export.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class Geometry;
class Polygon;
class LinearRing;
}
}

namespace geos {
namespace operation { // geos::operation
namespace intersection { // geos::operation::intersection

/**
 * \brief Clipping rectangle
 *
 * A clipping rectangle defines the boundaries of the rectangle
 * by defining the limiting x- and y-coordinates. The clipping
 * rectangle must be non-empty. In addition, methods are provided
 * for specifying the location of a given coordinate with respect
 * to the clipping rectangle similarly to the Cohen-Sutherland
 * clipping algorithm.
 *
 */

class GEOS_DLL Rectangle {
public:

    /**
     * \brief Construct a clipping rectangle
     *
     * @param x1 x-coordinate of the left edge
     * @param y1 y-coordinate of the bottom edge
     * @param x2 x-coordinate of the right edge
     * @param y2 y-coordinate of the right edge
     * @throws IllegalArgumentException if the rectangle is empty
     */

    Rectangle(double x1, double y1, double x2, double y2);

    /**
     * @return the minimum x-coordinate of the rectangle
     */
    double
    xmin() const
    {
        return xMin;
    }

    /**
     * @return the minimum y-coordinate of the rectangle
     */

    double
    ymin() const
    {
        return yMin;
    }


    /**
     * @return the maximum x-coordinate of the rectangle
     */

    double
    xmax() const
    {
        return xMax;
    }


    /**
     * @return the maximum y-coordinate of the rectangle
     */

    double
    ymax() const
    {
        return yMax;
    }

    /**
     * @return the rectangle as a polygon geometry
     *
     * @note Ownership transferred to caller
     */
    geom::Polygon* toPolygon(const geom::GeometryFactory& f) const;

    geom::LinearRing* toLinearRing(const geom::GeometryFactory& f) const;

    /**
     * @brief Position with respect to a clipping rectangle
     */

    enum Position {
        Inside    = 1,
        Outside   = 2,

        Left        = 4,
        Top         = 8,
        Right       = 16,
        Bottom      = 32,

        TopLeft     = Top | Left,               // 12
        TopRight    = Top | Right,              // 24
        BottomLeft  = Bottom | Left,            // 36
        BottomRight = Bottom | Right            // 48
    };

    /**
     * @brief Test if the given position is on a Rectangle edge
     * @param pos Rectangle::Position
     * @return `true`, if the position is on an edge
     */

    static bool
    onEdge(Position pos)
    {
        return (pos > Outside);
    }

    /**
     * @brief Test if the given positions are on the same Rectangle edge
     * @param pos1 [Position](@ref Rectangle::Position) of first coordinate
     * @param pos2 [Position](@ref Rectangle::Position) of second coordinate
     * @return `true`, if the positions are on the same edge
     */

    static bool
    onSameEdge(Position pos1, Position pos2)
    {
        return onEdge(Position(pos1 & pos2));
    }

    /**
     * @brief Establish position of coordinate with respect to a Rectangle
     * @param x x-coordinate
     * @param y y-coordinate
     * @return [Position](@ref Rectangle::Position) of the coordinate
     */

    Position
    position(double x, double y) const
    {
        // We assume the point to be inside and test it first
        if(x > xMin && x < xMax && y > yMin && y < yMax) {
            return Inside;
        }
        // Next we assume the point to be outside and test it next
        if(x < xMin || x > xMax || y < yMin || y > yMax) {
            return Outside;
        }
        // Slower cases
        unsigned int pos = 0;
        if(x == xMin) {
            pos |= Left;
        }
        else if(x == xMax) {
            pos |= Right;
        }
        if(y == yMin) {
            pos |= Bottom;
        }
        else if(y == yMax) {
            pos |= Top;
        }
        return Position(pos);
    }

    /**
     * @brief Next edge in clock-wise direction
     * @param pos [Position](@ref Rectangle::Position)
     * @return next [Position](@ref Rectangle::Position) in clock-wise direction
     */

    static Position
    nextEdge(Position pos)
    {
        switch(pos) {
        case BottomLeft:
        case Left:
            return Top;
        case TopLeft:
        case Top:
            return Right;
        case TopRight:
        case Right:
            return Bottom;
        case BottomRight:
        case Bottom:
            return Left;
        /* silences compiler warnings, Inside & Outside are not handled explicitly */
        default:
            return pos;
        }
    }

private:

    Rectangle();
    double xMin;
    double yMin;
    double xMax;
    double yMax;

}; // class RectangleIntersection

} // namespace geos::operation::intersection
} // namespace geos::operation
} // namespace geos

#endif // GEOS_OP_INTERSECTION_RECTANGLE_H
