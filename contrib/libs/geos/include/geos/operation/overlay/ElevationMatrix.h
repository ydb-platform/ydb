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

#ifndef GEOS_OP_OVERLAY_ELEVATIONMATRIX_H
#define GEOS_OP_OVERLAY_ELEVATIONMATRIX_H

#include <geos/export.h>

#include <geos/geom/CoordinateFilter.h> // for inheritance
#include <geos/geom/Envelope.h> // for composition
#include <geos/operation/overlay/ElevationMatrixCell.h> // for composition

#include <vector>
#include <string>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Geometry;
}
namespace operation {
namespace overlay {
class ElevationMatrixFilter;
class ElevationMatrix;
}
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay


/*
 * This is the CoordinateFilter used by ElevationMatrix.
 * filter_ro is used to add Geometry Coordinate's Z
 * values to the matrix.
 * filter_rw is used to actually elevate Geometries.
 */
class GEOS_DLL ElevationMatrixFilter: public geom::CoordinateFilter {
public:
    ElevationMatrixFilter(ElevationMatrix& em);
    ~ElevationMatrixFilter() override = default;
    void filter_rw(geom::Coordinate* c) const override;
    void filter_ro(const geom::Coordinate* c) override;
private:
    ElevationMatrix& em;
    double avgElevation;

    // Declare type as noncopyable
    ElevationMatrixFilter(const ElevationMatrixFilter& other) = delete;
    ElevationMatrixFilter& operator=(const ElevationMatrixFilter& rhs) = delete;
};


/*
 */
class GEOS_DLL ElevationMatrix {
    friend class ElevationMatrixFilter;
public:
    ElevationMatrix(const geom::Envelope& extent, unsigned int rows,
                    unsigned int cols);
    ~ElevationMatrix() = default;
    void add(const geom::Geometry* geom);
    void elevate(geom::Geometry* geom) const;
    // set Z value for each cell w/out one
    double getAvgElevation() const;
    ElevationMatrixCell& getCell(const geom::Coordinate& c);
    const ElevationMatrixCell& getCell(const geom::Coordinate& c) const;
    std::string print() const;
private:
    ElevationMatrixFilter filter;
    void add(const geom::Coordinate& c);
    geom::Envelope env;
    unsigned int cols;
    unsigned int rows;
    double cellwidth;
    double cellheight;
    mutable bool avgElevationComputed;
    mutable double avgElevation;
    std::vector<ElevationMatrixCell>cells;
};

} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_OVERLAY_ELEVATIONMATRIX_H
