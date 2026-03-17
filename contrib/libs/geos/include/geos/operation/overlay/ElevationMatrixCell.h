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
 ***********************************************************************
 *
 * Last port: original (by strk)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_ELEVATIONMATRIXCELL_H
#define GEOS_OP_OVERLAY_ELEVATIONMATRIXCELL_H

#include <geos/export.h>

#include <set>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay


class GEOS_DLL ElevationMatrixCell {
public:
    ElevationMatrixCell();
    ~ElevationMatrixCell() = default;
    void add(const geom::Coordinate& c);
    void add(double z);
    double getAvg(void) const;
    double getTotal(void) const;
    std::string print() const;
private:
    std::set<double>zvals;
    double ztot;
};

} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_OVERLAY_ELEVATIONMATRIXCELL_H
