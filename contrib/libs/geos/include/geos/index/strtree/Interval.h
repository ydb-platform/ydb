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

#ifndef GEOS_INDEX_STRTREE_INTERVAL_H
#define GEOS_INDEX_STRTREE_INTERVAL_H

#include <geos/export.h>

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree

/// A contiguous portion of 1D-space. Used internally by SIRtree.
//
/// @see SIRtree
///
class GEOS_DLL Interval {
public:
    Interval(double newMin, double newMax);
    double getCentre();
    Interval* expandToInclude(const Interval* other);
    bool intersects(const Interval* other) const;
    bool equals(const Interval* o) const;
private:
    double imin;
    double imax;
};


} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos

#endif // GEOS_INDEX_STRTREE_INTERVAL_H
