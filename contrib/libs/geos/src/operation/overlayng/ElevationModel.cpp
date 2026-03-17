/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last Port: operation/overlayng/ElevationModel.java 4c88fea52
 *
 **********************************************************************/

#include <geos/operation/overlayng/ElevationModel.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/CoordinateSequenceFilter.h>
#include <geos/geom/Envelope.h>

#include <memory> // for unique_ptr

#ifndef GEOS_DEBUG
# define GEOS_DEBUG 0
#endif

#if GEOS_DEBUG
# include <iostream>
#endif

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

namespace {
    template<typename T>
    T clamp(const T& v, const T& low, const T& high) {
        return v < low ? low : high < v ? high : v;
    }
}

using geos::geom::Coordinate;
using geos::geom::CoordinateSequence;
using geos::geom::Geometry;
using geos::geom::Envelope;

/* public static */
std::unique_ptr<ElevationModel>
ElevationModel::create(const geom::Geometry& geom1, const geom::Geometry& geom2)
{
    Envelope extent;
    if (! geom1.isEmpty() ) {
      extent.expandToInclude(geom1.getEnvelopeInternal());
    }
    if (! geom2.isEmpty() ) {
      extent.expandToInclude(geom2.getEnvelopeInternal());
    }
    std::unique_ptr<ElevationModel> model ( new ElevationModel(extent, DEFAULT_CELL_NUM, DEFAULT_CELL_NUM) );
    if (! geom1.isEmpty() ) model->add(geom1);
    if (! geom2.isEmpty() ) model->add(geom2);
    return model;
}

/* public static */
std::unique_ptr<ElevationModel>
ElevationModel::create(const geom::Geometry& geom1)
{
    Envelope extent;
    if (! geom1.isEmpty() ) {
      extent.expandToInclude(geom1.getEnvelopeInternal());
    }
    std::unique_ptr<ElevationModel> model ( new ElevationModel(extent, DEFAULT_CELL_NUM, DEFAULT_CELL_NUM) );
    if (! geom1.isEmpty() ) model->add(geom1);
    return model;
}

const int ElevationModel::DEFAULT_CELL_NUM = 3;

ElevationModel::ElevationModel(const Envelope& nExtent, int nNumCellX, int nNumCellY)
    :
    extent(nExtent),
    numCellX(nNumCellX),
    numCellY(nNumCellY)
{

    cellSizeX = extent.getWidth() / numCellX;
    cellSizeY = extent.getHeight() / numCellY;
    if(cellSizeX <= 0.0) {
        numCellX = 1;
    }
    if(cellSizeY <= 0.0) {
        numCellY = 1;
    }
    cells.resize(numCellX * numCellY);
}

/* public */
void
ElevationModel::add(const Geometry& geom)
{
    class Filter: public geom::CoordinateSequenceFilter {
        ElevationModel& model;
        bool hasZ;
    public:
        Filter(ElevationModel& nModel) : model(nModel), hasZ(true) {}

        void filter_ro(const geom::CoordinateSequence& seq, std::size_t i) override
        {
#if 0
            if (! seq.hasZ()) {
                hasZ = false;;
                return;
            }
#endif
            const Coordinate& c = seq.getAt(i);
#if GEOS_DEBUG
            std::cout << "Coordinate " << i << " of added geom is: "
<< c << std::endl;
#endif
            model.add(c.x, c.y, c.z);
        }

        bool isDone() const override {
            // no need to scan if no Z present
            return ! hasZ;
        }

        bool isGeometryChanged() const override {
            return false;
        }

    };

    Filter filter(*this);
    geom.apply_ro(filter);

}

/* protected */
void
ElevationModel::add(double x, double y, double z)
{
    if (std::isnan(z))
      return;
    hasZValue = true;
    ElevationCell& cell = getCell(x, y); //, true);
    cell.add(z);
}

/* private */
void
ElevationModel::init()
{
    isInitialized = true;
    int numCells = 0;
    double sumZ = 0.0;

#if GEOS_DEBUG
    int offset = 0;
#endif
    for (ElevationCell& cell : cells )
    {
        if (!cell.isNull()) {
          cell.compute();
#if GEOS_DEBUG
          std::cout << "init: cell" << offset
                    << " getZ: " << cell.getZ()
                    << std::endl;
#endif
          numCells++;
          sumZ += cell.getZ();
        }
#if GEOS_DEBUG
        ++offset;
#endif
    }
    averageZ = DoubleNotANumber;
    if (numCells > 0) {
      averageZ = sumZ / numCells;
    }
#if GEOS_DEBUG
    std::cout << "init: numCells: " << numCells
              << " averageZ: " << averageZ << std::endl;
#endif
}

/* public */
double
ElevationModel::getZ(double x, double y)
{
    if (! isInitialized)
    {
      init();
    }
    const ElevationModel::ElevationCell& cell = getCell(x, y); //, false);
    if (cell.isNull())
    {
      return averageZ;
    }
    return cell.getZ();
}

/* public */
void
ElevationModel::populateZ(Geometry& geom)
{
    // short-circuit if no Zs are present in model
    if (! hasZValue)
      return;

    if (! isInitialized)
      init();

    class Filter: public geom::CoordinateFilter {
        ElevationModel& model;
    public:
        Filter(ElevationModel& nModel) : model(nModel) {}

        void filter_rw(geom::Coordinate* c) const override
        {
#if GEOS_DEBUG
            std::cout << "Input coord:  " << *c << std::endl;
#endif
            if (std::isnan( c->z )) {
                c->z = model.getZ(c->x, c->y);
#if GEOS_DEBUG
                std::cout << "New coord: " << *c << std::endl;
#endif
            }
        }

    };

    Filter filter(*this);
#if GEOS_DEBUG
    std::cout << "ElevationModel::poplateZ calling apply_rw(CoordinateSequenceFilter&) against" <<
              //std::type_name<decltype(ci)>()
              typeid(geom).name()
              << std::endl;
#endif
    geom.apply_rw(&filter);
}

/* private */
ElevationModel::ElevationCell&
ElevationModel::getCell(double x, double y) //, bool isCreateIfMissing
{
    int ix = 0;
    if (numCellX > 1) {
      ix = (int) ((x - extent.getMinX()) / cellSizeX);
      ix = clamp(ix, 0, numCellX - 1);
    }
    int iy = 0;
    if (numCellY > 1) {
      iy = (int) ((y - extent.getMinY()) / cellSizeY);
      iy = clamp(iy, 0, numCellY - 1);
    }
    int cellOffset = getCellOffset(ix, iy);
#if GEOS_DEBUG
    std::cout << "Cell of coordinate " << x << "," << y
              << " is " << ix << "," << iy
              << " offset " << cellOffset << std::endl;
#endif
    assert ( cellOffset < numCellX * numCellY );
    ElevationModel::ElevationCell& cell = cells[cellOffset];
    return cell;
}

} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
