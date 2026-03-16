/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
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

#include <geos/constants.h>
#include <geos/geom/Coordinate.h>
#include <geos/operation/overlay/ElevationMatrixCell.h>

#include <sstream>
#include <string>
#include <vector>
#include <cmath>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay

ElevationMatrixCell::ElevationMatrixCell(): ztot(0)
{
}

void
ElevationMatrixCell::add(const Coordinate& c)
{
    if(!std::isnan(c.z)) {
        if(zvals.insert(c.z).second) {
            ztot += c.z;
        }
    }
}

void
ElevationMatrixCell::add(double z)
{
    if(!std::isnan(z)) {
        if(zvals.insert(z).second) {
            ztot += z;
        }
    }
}

double
ElevationMatrixCell::getTotal() const
{
    return ztot;
}

double
ElevationMatrixCell::getAvg() const
{
    return  zvals.size() ?
            ztot / static_cast<double>(zvals.size()) :
            DoubleNotANumber;
}

string
ElevationMatrixCell::print() const
{
    ostringstream ret;
    //ret<<"["<<ztot<<"/"<<zvals.size()<<"]";
    ret << "[" << getAvg() << "]";
    return ret.str();
}

} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos;
