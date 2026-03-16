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
 * Last port: geom/prep/PreparedGeometryFactory.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDGEOMETRYFACTORY_H
#define GEOS_GEOM_PREP_PREPAREDGEOMETRYFACTORY_H

#include <geos/export.h>
#include <geos/geom/prep/PreparedGeometry.h>

#include <memory>

namespace geos {
namespace geom {
namespace prep {
class PreparedGeometry;
}
}
}


namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep


/**
 * \brief
 * A factory for creating {@link PreparedGeometry}s.
 *
 * It chooses an appropriate implementation of PreparedGeometry
 * based on the geoemtric type of the input geometry.
 * In the future, the factory may accept hints that indicate
 * special optimizations which can be performed.
 *
 * @author Martin Davis
 *
 */
class GEOS_DLL PreparedGeometryFactory {
public:

    /**
     * Creates a new {@link PreparedGeometry} appropriate for the argument {@link Geometry}.
     *
     * @param geom the geometry to prepare
     * @return the prepared geometry
     */
    static std::unique_ptr<PreparedGeometry>
    prepare(const geom::Geometry* geom)
    {
        PreparedGeometryFactory pf;
        return pf.create(geom);
    }

    /**
     * Destroys {@link PreparedGeometry} allocated with the factory.
     *
     * @param geom to be deallocated
     */
    static void
    destroy(const PreparedGeometry* geom)
    {
        delete geom;
    }

    /**
     * Creates a new {@link PreparedGeometry} appropriate for the argument {@link Geometry}.
     *
     * @param geom the geometry to prepare
     * @return the prepared geometry
     */
    std::unique_ptr<PreparedGeometry> create(const geom::Geometry* geom) const;

};

} // namespace geos::geom::prep
} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_PREP_PREPAREDGEOMETRYFACTORY_H
