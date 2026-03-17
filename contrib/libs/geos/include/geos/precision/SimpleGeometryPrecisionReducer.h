/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_PRECISION_SIMPLEGEOMETRYPRECISIONREDUCER_H
#define GEOS_PRECISION_SIMPLEGEOMETRYPRECISIONREDUCER_H

#include <geos/export.h>
#include <memory>

// Forward declarations
namespace geos {
namespace geom {
class PrecisionModel;
class Geometry;
}
}

namespace geos {
namespace precision { // geos.precision

/** \brief
 * Reduces the precision of a {@link geom::Geometry}
 * according to the supplied {@link geom::PrecisionModel}, without
 * attempting to preserve valid topology.
 *
 * The topology of the resulting geometry may be invalid if
 * topological collapse occurs due to coordinates being shifted.
 * It is up to the client to check this and handle it if necessary.
 * Collapses may not matter for some uses.  An example
 * is simplifying the input to the buffer algorithm.
 * The buffer algorithm does not depend on the validity of the input geometry.
 *
 */
class GEOS_DLL SimpleGeometryPrecisionReducer {

private:

    const geom::PrecisionModel* newPrecisionModel;

    bool removeCollapsed;

    //bool changePrecisionModel;

public:

    SimpleGeometryPrecisionReducer(const geom::PrecisionModel* pm);

    /**
     * Sets whether the reduction will result in collapsed components
     * being removed completely, or simply being collapsed to an (invalid)
     * Geometry of the same type.
     *
     * @param nRemoveCollapsed if <code>true</code> collapsed
     * components will be removed
     */
    void setRemoveCollapsedComponents(bool nRemoveCollapsed);

    /*
     * Sets whether the {@link PrecisionModel} of the new reduced Geometry
     * will be changed to be the {@link PrecisionModel} supplied to
     * specify the reduction.  The default is to not change the
     * precision model
     *
     * @param changePrecisionModel if <code>true</code> the precision
     * model of the created Geometry will be the
     * the precisionModel supplied in the constructor.
     */
    //void setChangePrecisionModel(bool nChangePrecisionModel);

    const geom::PrecisionModel* getPrecisionModel();

    bool getRemoveCollapsed();
    std::unique_ptr<geom::Geometry> reduce(const geom::Geometry* geom);
};

} // namespace geos.precision
} // namespace geos

#endif // GEOS_PRECISION_SIMPLEGEOMETRYPRECISIONREDUCER_H
