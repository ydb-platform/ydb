/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Excensus LLC.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: triangulate/quadedge/Vertex.java r705
 *
 **********************************************************************/

#ifndef GEOS_TRIANGULATE_QUADEDGE_VERTEX_H
#define GEOS_TRIANGULATE_QUADEDGE_VERTEX_H

#include <math.h>
#include <memory>

#include <geos/geom/Coordinate.h>
#include <geos/algorithm/HCoordinate.h>
#include <geos/triangulate/quadedge/TrianglePredicate.h>


//fwd declarations
namespace geos {
namespace triangulate {
namespace quadedge {
class QuadEdge;
}
}
}

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

/** \brief
 * Models a site (node) in a QuadEdgeSubdivision.
 *
 * The sites can be points on a line string representing a linear site.
 *
 * The vertex can be considered as a vector with a norm, length, inner product, cross
 * product, etc. Additionally, point relations (e.g., is a point to the left of a line, the circle
 * defined by this point and two others, etc.) are also defined in this class.
 *
 * It is common to want to attach user-defined data to the vertices of a subdivision.
 * One way to do this is to subclass `Vertex` to carry any desired information.
 *
 * @author JTS: David Skea
 * @author JTS: Martin Davis
 * @author Benjamin Campbell
 * */

class GEOS_DLL Vertex {
public:
    static const int LEFT		= 0;
    static const int RIGHT	   = 1;
    static const int BEYOND	  = 2;
    static const int BEHIND	  = 3;
    static const int BETWEEN	 = 4;
    static const int ORIGIN	  = 5;
    static const int DESTINATION = 6;
private:
    geom::Coordinate	  p;

public:
    Vertex(double _x, double _y);

    Vertex(double _x, double _y, double _z);

    Vertex(const geom::Coordinate& _p);

    Vertex();
    ~Vertex() {};

    inline double
    getX() const
    {
        return p.x;
    }

    inline double
    getY() const
    {
        return p.y;
    }

    inline double
    getZ() const
    {
        return p.z;
    }

    inline void
    setZ(double _z)
    {
        p.z = _z;
    }

    inline const geom::Coordinate&
    getCoordinate() const
    {
        return p;
    }

    inline bool
    equals(const Vertex& _x) const
    {
        if(p.x == _x.getX() && p.y == _x.getY()) {
            return true;
        }
        return false;
    }

    inline bool
    equals(const Vertex& _x, double tolerance) const
    {
        if(p.distance(_x.getCoordinate()) < tolerance) {
            return true;
        }
        return false;
    }

    int classify(const Vertex& p0, const Vertex& p1);

    /**
     * Computes the cross product k = u X v.
     *
     * @param v a vertex
     * @return returns the magnitude of u X v
     */
    inline double
    crossProduct(const Vertex& v) const
    {
        return (p.x * v.getY() - p.y * v.getX());
    }

    /**
     * Computes the inner or dot product
     *
     * @param v a vertex
     * @return returns the dot product u.v
     */
    inline double
    dot(Vertex v) const
    {
        return (p.x * v.getX() + p.y * v.getY());
    }

    /**
     * Computes the scalar product c(v)
     *
     * @param c scaling factor
     * @return returns the scaled vector
     */
    inline std::unique_ptr<Vertex>
    times(double c) const
    {
        return std::unique_ptr<Vertex>(new Vertex(c * p.x, c * p.y));
    }

    /* Vector addition */
    inline std::unique_ptr<Vertex>
    sum(Vertex v) const
    {
        return std::unique_ptr<Vertex>(new Vertex(p.x + v.getX(), p.y + v.getY()));
    }

    /* and subtraction */
    inline std::unique_ptr<Vertex>
    sub(const Vertex& v) const
    {
        return std::unique_ptr<Vertex>(new Vertex(p.x - v.getX(), p.y - v.getY()));
    }

    /* magnitude of vector */
    inline double
    magn() const
    {
        return (sqrt(p.x * p.x + p.y * p.y));
    }

    /* returns k X v (cross product). this is a vector perpendicular to v */
    inline std::unique_ptr<Vertex>
    cross() const
    {
        return std::unique_ptr<Vertex>(new Vertex(p.y, -p.x));
    }

    /** ************************************************************* */
    /***********************************************************************************************
     * Geometric primitives /
     **********************************************************************************************/

    /**
     * Tests if the vertex is inside the circle defined by
     * the triangle with vertices a, b, c (oriented counter-clockwise).
     *
     * @param a a vertex of the triangle
     * @param b a vertex of the triangle
     * @param c a vertex of the triangle
     * @return true if this vertex is in the circumcircle of (a,b,c)
     */
    bool isInCircle(const Vertex& a, const Vertex& b, const Vertex& c) const {
        return geom::TrianglePredicate::isInCircleRobust(a.p, b.p, c.p, this->p);
    }

    /**
     * Tests whether the triangle formed by this vertex and two
     * other vertices is in CCW orientation.
     *
     * @param b a vertex
     * @param c a vertex
     * @returns true if the triangle is oriented CCW
     */
    inline bool
    isCCW(const Vertex& b, const Vertex& c) const
    {
        // check if signed area is positive
        return (b.p.x - p.x) * (c.p.y - p.y)
               > (b.p.y - p.y) * (c.p.x - p.x);
    }

    bool rightOf(const QuadEdge& e) const;
    bool leftOf(const QuadEdge& e) const;

private:
    static std::unique_ptr<algorithm::HCoordinate> bisector(const Vertex& a, const Vertex& b);

    inline double
    distance(const Vertex& v1, const Vertex& v2)
    {
        return sqrt(pow(v2.getX() - v1.getX(), 2.0)
                    + pow(v2.getY() - v1.getY(), 2.0));
    }

    /**
     * Computes the value of the ratio of the circumradius to shortest edge. If smaller than some
     * given tolerance B, the associated triangle is considered skinny. For an equal lateral
     * triangle this value is 0.57735. The ratio is related to the minimum triangle angle theta by:
     * circumRadius/shortestEdge = 1/(2sin(theta)).
     *
     * @param b second vertex of the triangle
     * @param c third vertex of the triangle
     * @return ratio of circumradius to shortest edge.
     */
    double circumRadiusRatio(const Vertex& b, const Vertex& c);

    /**
     * returns a new vertex that is mid-way between this vertex and another end point.
     *
     * @param a the other end point.
     * @return the point mid-way between this and that.
     */
    std::unique_ptr<Vertex> midPoint(const Vertex& a);

    /**
     * Computes the centre of the circumcircle of this vertex and two others.
     *
     * @param b
     * @param c
     * @return the Coordinate which is the circumcircle of the 3 points.
     */
    std::unique_ptr<Vertex> circleCenter(const Vertex& b, const Vertex& c) const;

    /**
     * For this vertex enclosed in a triangle defined by three vertices v0, v1 and v2, interpolate
     * a z value from the surrounding vertices.
     */
    double interpolateZValue(const Vertex& v0, const Vertex& v1, const Vertex& v2) const;

    /**
     * Interpolates the Z-value (height) of a point enclosed in a triangle
     * whose vertices all have Z values.
     * The containing triangle must not be degenerate
     * (in other words, the three vertices must enclose a
     * non-zero area).
     *
     * @param p the point to interpolate the Z value of
     * @param v0 a vertex of a triangle containing the p
     * @param v1 a vertex of a triangle containing the p
     * @param v2 a vertex of a triangle containing the p
     * @return the interpolated Z-value (height) of the point
     */
    static double interpolateZ(const geom::Coordinate& p, const geom::Coordinate& v0,
                               const geom::Coordinate& v1, const geom::Coordinate& v2);

    /**
     * Computes the interpolated Z-value for a point p lying on the segment p0-p1
     *
     * @param p
     * @param p0
     * @param p1
     * @return the interpolated Z value
     */
    static double interpolateZ(const geom::Coordinate& p, const geom::Coordinate& p0,
                               const geom::Coordinate& p1);
};

inline bool
operator<(const Vertex& v1, const Vertex& v2)
{
    return v1.getCoordinate() < v2.getCoordinate();
}

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace geos

#endif //GEOS_TRIANGULATE_QUADEDGE_VERTEX_H

