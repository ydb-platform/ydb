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
#include <geos/operation/overlay/ElevationMatrix.h>
#include <geos/util/IllegalArgumentException.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>

#include <cassert>
#include <cmath>
#include <iostream>
#include <sstream>
#include <string>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif
#define PARANOIA_LEVEL 0

#ifdef _MSC_VER
#pragma warning(disable:4355)
#endif

using namespace std;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay

ElevationMatrixFilter::ElevationMatrixFilter(ElevationMatrix& newEm):
    em(newEm)
{ }

void
ElevationMatrixFilter::filter_rw(Coordinate* c) const
{
#if GEOS_DEBUG
    cerr << "ElevationMatrixFilter::filter_rw(" << c->toString() << ") called"
         << endl;
#endif

    // already has a Z value, nothing to do
    if(! std::isnan(c->z)) {
        return;
    }

    double p_avgElevation = em.getAvgElevation();

    try {
        const ElevationMatrixCell& emc = em.getCell(*c);
        c->z = emc.getAvg();
        if(std::isnan(c->z)) {
            c->z = p_avgElevation;
        }
#if GEOS_DEBUG
        cerr << "  z set to " << c->z << endl;
#endif
    }
    catch(const util::IllegalArgumentException& /* ex */) {
        c->z = avgElevation;
    }
}

void
ElevationMatrixFilter::filter_ro(const Coordinate* c)
{
#if GEOS_DEBUG
    cerr << "ElevationMatrixFilter::filter_ro(" << c->toString() << ") called"
         << endl;
#endif
    em.add(*c);
}


ElevationMatrix::ElevationMatrix(const Envelope& newEnv,
                                 unsigned int newRows, unsigned int newCols):
    filter(*this),
    env(newEnv), cols(newCols), rows(newRows),
    avgElevationComputed(false),
    avgElevation(DoubleNotANumber),
    cells(newRows * newCols)
{
    cellwidth = env.getWidth() / cols;
    cellheight = env.getHeight() / rows;
    if(! cellwidth) {
        cols = 1;
    }
    if(! cellheight) {
        rows = 1;
    }
}

void
ElevationMatrix::add(const Geometry* geom)
{
#if GEOS_DEBUG
    cerr << "ElevationMatrix::add(Geometry *) called" << endl;
#endif // GEOS_DEBUG

    // Cannot add Geometries to an ElevationMatrix after it's average
    // elevation has been computed
    assert(!avgElevationComputed);

    //ElevationMatrixFilter filter(this);
    geom->apply_ro(&filter);

}

#if 0
void
ElevationMatrix::add(const CoordinateSequence* cs)
{
    unsigned int ncoords = cs->getSize();
    for(unsigned int i = 0; i < ncoords; i++) {
        add(cs->getAt(i));
    }
}
#endif

void
ElevationMatrix::add(const Coordinate& c)
{
    if(std::isnan(c.z)) {
        return;
    }
    try {
        ElevationMatrixCell& emc = getCell(c);
        emc.add(c);
    }
    catch(const util::IllegalArgumentException& exp) {
        // coordinate do not overlap matrix
        cerr << "ElevationMatrix::add(" << c.toString()
             << "): Coordinate does not overlap grid extent: "
             << exp.what() << endl;
        return;
    }
}

ElevationMatrixCell&
ElevationMatrix::getCell(const Coordinate& c)
{
    int col, row;

    if(! cellwidth) {
        col = 0;
    }
    else {
        double xoffset = c.x - env.getMinX();
        col = (int)(xoffset / cellwidth);
        if(col == (int)cols) {
            col = cols - 1;
        }
    }
    if(! cellheight) {
        row = 0;
    }
    else {
        double yoffset = c.y - env.getMinY();
        row = (int)(yoffset / cellheight);
        if(row == (int)rows) {
            row = rows - 1;
        }
    }
    int celloffset = (cols * row) + col;

    if(celloffset < 0 || celloffset >= (int)(cols * rows)) {
        ostringstream s;
        s << "ElevationMatrix::getCell got a Coordinate out of grid extent (" << env.toString() << ") - cols:" << cols <<
          " rows:" << rows;
        throw util::IllegalArgumentException(s.str());
    }

    return cells[celloffset];
}

const ElevationMatrixCell&
ElevationMatrix::getCell(const Coordinate& c) const
{
    return (const ElevationMatrixCell&)
           ((ElevationMatrix*)this)->getCell(c);
}

double
ElevationMatrix::getAvgElevation() const
{
    if(avgElevationComputed) {
        return avgElevation;
    }
    double ztot = 0;
    int zvals = 0;
    for(unsigned int r = 0; r < rows; r++) {
        for(unsigned int c = 0; c < cols; c++) {
            const ElevationMatrixCell& cell = cells[(r * cols) + c];
            double e = cell.getAvg();
            if(!std::isnan(e)) {
                zvals++;
                ztot += e;
            }
        }
    }
    if(zvals) {
        avgElevation = ztot / zvals;
    }
    else {
        avgElevation = DoubleNotANumber;
    }

    avgElevationComputed = true;

    return avgElevation;
}

string
ElevationMatrix::print() const
{
    ostringstream ret;
    ret << "Cols:" << cols << " Rows:" << rows << " AvgElevation:" << getAvgElevation() << endl;
    for(unsigned int r = 0; r < rows; r++) {
        for(unsigned int c = 0; c < cols; c++) {
            ret << cells[(r * cols) + c].print() << '\t';
        }
        ret << endl;
    }
    return ret.str();
}

void
ElevationMatrix::elevate(Geometry* g) const
{

    // Nothing to do if no elevation info in matrix
    if(std::isnan(getAvgElevation())) {
        return;
    }

    g->apply_rw(&filter);
}

} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos;
