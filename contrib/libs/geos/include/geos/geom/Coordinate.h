/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_COORDINATE_H
#define GEOS_GEOM_COORDINATE_H

#include <geos/export.h>
#include <geos/constants.h> // for DoubleNotANumber
#include <geos/inline.h>
#include <set>
#include <stack>
#include <vector> // for typedefs
#include <string>
#include <limits>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace geom { // geos.geom

struct CoordinateLessThen;

/**
 * \class Coordinate geom.h geos.h
 *
 * \brief
 * Coordinate is the lightweight class used to store coordinates.
 *
 * It is distinct from Point, which is a subclass of Geometry.
 * Unlike objects of type Point (which contain additional
 * information such as an envelope, a precision model, and spatial
 * reference system information), a Coordinate only contains
 * ordinate values and accessor methods.
 *
 * Coordinate objects are two-dimensional points, with an additional
 * z-ordinate. JTS does not support any operations on the z-ordinate except
 * the basic accessor functions.
 *
 * Constructed coordinates will have a z-ordinate of DoubleNotANumber.
 * The standard comparison functions will ignore the z-ordinate.
 *
 */
// Define the following to make assignments and copy constructions
// NON-inline (will let profilers report usages)
//#define PROFILE_COORDINATE_COPIES 1
class GEOS_DLL Coordinate {

private:

    static Coordinate _nullCoord;

public:
    /// A set of const Coordinate pointers
    typedef std::set<const Coordinate*, CoordinateLessThen> ConstSet;

    /// A vector of const Coordinate pointers
    typedef std::vector<const Coordinate*> ConstVect;

    /// A stack of const Coordinate pointers
    typedef std::stack<const Coordinate*> ConstStack;

    /// A vector of Coordinate objects (real object, not pointers)
    typedef std::vector<Coordinate> Vect;

    /// x-coordinate
    double x;

    /// y-coordinate
    double y;

    /// z-coordinate
    double z;

    void setNull();

    static Coordinate& getNull();

    bool isNull() const;

    Coordinate(double xNew = 0.0, double yNew = 0.0, double zNew = DoubleNotANumber);

    bool equals2D(const Coordinate& other) const;

    /// 2D only
    bool equals(const Coordinate& other) const;

    /// TODO: deprecate this, move logic to CoordinateLessThen instead
    int compareTo(const Coordinate& other) const;

    /// 3D comparison
    bool equals3D(const Coordinate& other) const;

    ///  Returns a string of the form <I>(x,y,z)</I> .
    std::string toString() const;

    /// TODO: obsoleted this, can use PrecisionModel::makePrecise(Coordinate*)
    /// instead
    //void makePrecise(const PrecisionModel *pm);

    double distance(const Coordinate& p) const;

    double distanceSquared(const Coordinate& p) const;

    struct GEOS_DLL HashCode {
        size_t operator()(const Coordinate & c) const;
    };

};

/// Strict weak ordering Functor for Coordinate
struct GEOS_DLL CoordinateLessThen {

    bool operator()(const Coordinate* a, const Coordinate* b) const;
    bool operator()(const Coordinate& a, const Coordinate& b) const;

};

/// Strict weak ordering operator for Coordinate
inline bool
operator<(const Coordinate& a, const Coordinate& b)
{
    return CoordinateLessThen()(a, b);
}

/// Output function
GEOS_DLL std::ostream& operator<< (std::ostream& os, const Coordinate& c);

/// Equality operator for Coordinate. 2D only.
GEOS_DLL bool operator==(const Coordinate& a, const Coordinate& b);

/// Inequality operator for Coordinate. 2D only.
GEOS_DLL bool operator!=(const Coordinate& a, const Coordinate& b);



} // namespace geos.geom
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#ifdef GEOS_INLINE
# include "geos/geom/Coordinate.inl"
#endif

#endif // ndef GEOS_GEOM_COORDINATE_H

