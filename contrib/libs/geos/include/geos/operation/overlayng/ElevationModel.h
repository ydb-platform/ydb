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

#pragma once

#include <geos/export.h>

#include <geos/geom/Envelope.h> // for composition

// Forward declarations
namespace geos {
    namespace geom {
        class Geometry;
    }
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/**
 * \brief
 * A simple elevation model used to populate missing Z values
 * in overlay results.
 *
 * The model divides the extent of the input geometry(s)
 * into an NxM grid.
 * The default grid size is 3x3.
 * If the input has no extent in the X or Y dimension,
 * that dimension is given grid size 1.
 * The elevation of each grid cell is computed as the average of the Z
 * values
 * of the input vertices in that cell (if any).
 * If a cell has no input vertices within it, it is assigned
 * the average elevation over all cells.
 *
 * If no input vertices have Z values, the model does not assign a Z
 * value.
 *
 * The elevation of an arbitrary location is determined as the
 * Z value of the nearest grid cell.
 *
 * An elevation model can be used to populate missing Z values
 * in an overlay result geometry.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL ElevationModel {

private:

    class ElevationCell {
    private:

      int numZ = 0;
      double sumZ = 0.0;
      double avgZ;

    public:

      bool isNull() const
      {
        return numZ == 0;
      }

      void add(double z)
      {
        ++numZ;
        sumZ += z;
      }

      void compute()
      {
        avgZ = DoubleNotANumber;
        if (numZ > 0)
          avgZ = sumZ / numZ;
      }

      double getZ() const
      {
        return avgZ;
      }
    };


    static const int DEFAULT_CELL_NUM;
    geom::Envelope extent;
    int numCellX;
    int numCellY;
    double cellSizeX;
    double cellSizeY;
    std::vector<ElevationCell> cells;
    bool isInitialized = false;
    bool hasZValue = false;
    double averageZ = DoubleNotANumber;

    void init();

    ElevationCell& getCell(double x, double y); //, bool isCreateIfMissing);

    int getCellOffset(int ix, int iy) {
        return (numCellX * iy + ix);
    }

protected:

  void add(double x, double y, double z);


public:

    static std::unique_ptr<ElevationModel> create(const geom::Geometry& geom1,
                                                const geom::Geometry& geom2);

    static std::unique_ptr<ElevationModel> create(const geom::Geometry& geom1);

    ElevationModel(const geom::Envelope& extent, int numCellX, int numCellY);

    void add(const geom::Geometry& geom);


    /**
     * Gets the model Z value at a given location.
     * If the location lies outside the model grid extent,
     * this returns the Z value of the nearest grid cell.
     * If the model has no elevation computed (i.e. due
     * to empty input), the value is returned as a double NaN.
     *
     * @param x the x ordinate of the location
     * @param y the y ordinate of the location
     * @return the computed model Z value
     */
    double getZ(double x, double y);


    /**
     * \brief
     * Computes Z values for any missing Z values in a geometry,
     * using the computed model.
     *
     * If the model has no Z value, or the geometry coordinate dimension
     * does not include Z, no action is taken.
     *
     * @param geom the geometry to elevate
     */
    void populateZ(geom::Geometry& geom);


};

} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
