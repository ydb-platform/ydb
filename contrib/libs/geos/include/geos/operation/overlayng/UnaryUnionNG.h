/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/operation/union/UnionStrategy.h>
#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/operation/overlayng/OverlayUtil.h>



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
 * Unions a collection of geometries in an
 * efficient way, using {@link OverlayNG}
 * to ensure robust computation.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL UnaryUnionNG {

private:

    // Members



public:

    /**
    * Strategy class for NG unions.
    */
    class NGUnionStrategy : public operation::geounion::UnionStrategy {

    public:

        NGUnionStrategy(const PrecisionModel& p_pm)
            : pm(p_pm)
            {};

        std::unique_ptr<geom::Geometry>
        Union(const geom::Geometry* g0, const geom::Geometry* g1) override
        {
            return OverlayNG::overlay(g0, g1, OverlayNG::UNION, &pm);
        }

        bool
        isFloatingPrecision() const override
        {
            return OverlayUtil::isFloating(&pm);
        }

    private:

        const geom::PrecisionModel& pm;

    };


    // static methods
    static std::unique_ptr<Geometry> Union(const Geometry* geom, const PrecisionModel& pm);
    static std::unique_ptr<Geometry> Union(const Geometry* geom);


};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

