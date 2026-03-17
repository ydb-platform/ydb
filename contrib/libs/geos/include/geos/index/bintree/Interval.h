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

#ifndef GEOS_IDX_BINTREE_INTERVAL_H
#define GEOS_IDX_BINTREE_INTERVAL_H

#include <geos/export.h>

namespace geos {
namespace index { // geos::index
namespace bintree { // geos::index::bintree

/// Represents an (1-dimensional) closed interval on the Real number line.
class GEOS_DLL Interval {

public:

    double min, max;

    Interval();

    Interval(double nmin, double nmax);

    /// TODO: drop this, rely on copy ctor
    Interval(const Interval* interval);

    void init(double nmin, double nmax);

    double getMin() const;

    double getMax() const;

    double getWidth() const;

    void expandToInclude(Interval* interval);

    bool overlaps(const Interval* interval) const;

    bool overlaps(double nmin, double nmax) const;

    bool contains(const Interval* interval) const;

    bool contains(double nmin, double nmax) const;

    bool contains(double p) const;
};

} // namespace geos::index::bintree
} // namespace geos::index
} // namespace geos

#endif // GEOS_IDX_BINTREE_INTERVAL_H

