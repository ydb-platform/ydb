/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: precision/PrecisionreducerCoordinateOperation.java r591 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_PRECISION_PRECISIONREDUCERCOORDINATEOPERATION_H
#define GEOS_PRECISION_PRECISIONREDUCERCOORDINATEOPERATION_H

#include <geos/geom/util/CoordinateOperation.h>

// Forward declarations
namespace geos {
namespace geom {
class PrecisionModel;
class CoordinateSequence;
class Geometry;
}
}

namespace geos {
namespace precision { // geos.precision

class PrecisionReducerCoordinateOperation :
    public geom::util::CoordinateOperation {
    using CoordinateOperation::edit;
private:

    const geom::PrecisionModel& targetPM;

    bool removeCollapsed;

    PrecisionReducerCoordinateOperation(PrecisionReducerCoordinateOperation const&); /*= delete*/
    PrecisionReducerCoordinateOperation& operator=(PrecisionReducerCoordinateOperation const&); /*= delete*/

public:

    PrecisionReducerCoordinateOperation(const geom::PrecisionModel& pm,
                                        bool doRemoveCollapsed)
        :
        targetPM(pm),
        removeCollapsed(doRemoveCollapsed)
    {}

    /// Ownership of returned CoordinateSequence to caller
    ///
    /// virtual function
    std::unique_ptr<geom::CoordinateSequence> edit(const geom::CoordinateSequence* coordinates,
                                                   const geom::Geometry* geom) override;
};

} // namespace geos.precision
} // namespace geos

#endif // GEOS_PRECISION_PRECISIONREDUCERCOORDINATEOPERATION_H

