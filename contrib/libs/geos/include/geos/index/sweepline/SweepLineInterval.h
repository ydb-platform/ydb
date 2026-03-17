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

#ifndef GEOS_INDEX_SWEEPLINE_SWEEPLINEINTERVAL_H
#define GEOS_INDEX_SWEEPLINE_SWEEPLINEINTERVAL_H

#include <geos/export.h>

namespace geos {
namespace index { // geos.index
namespace sweepline { // geos:index:sweepline

class GEOS_DLL SweepLineInterval {
public:
    SweepLineInterval(double newMin, double newMax, void* newItem = nullptr);
    double getMin();
    double getMax();
    void* getItem();
private:
    double min, max;
    void* item;
};

} // namespace geos:index:sweepline
} // namespace geos:index
} // namespace geos

#endif // GEOS_INDEX_SWEEPLINE_SWEEPLINEINTERVAL_H
