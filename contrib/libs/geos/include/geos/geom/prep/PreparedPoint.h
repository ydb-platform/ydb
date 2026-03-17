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
 *
 **********************************************************************
 *
 * Last port: geom/prep/PreparedPoint.java rev. 1.2 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDPOINT_H
#define GEOS_GEOM_PREP_PREPAREDPOINT_H

#include <geos/geom/prep/BasicPreparedGeometry.h> // for inheritance

namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep

/**
 * \brief
 * A prepared version of {@link Point} or {@link MultiPoint} geometries.
 *
 * @author Martin Davis
 *
 */
class PreparedPoint: public BasicPreparedGeometry {
private:
protected:
public:
    PreparedPoint(const Geometry* geom)
        : BasicPreparedGeometry(geom)
    { }

    /**
     * Tests whether this point intersects a {@link Geometry}.
     *
     * The optimization here is that computing topology for the test
     * geometry is avoided. This can be significant for large geometries.
     */
    bool intersects(const geom::Geometry* g) const override;

};

} // namespace geos::geom::prep
} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_PREP_PREPAREDPOINT_H
