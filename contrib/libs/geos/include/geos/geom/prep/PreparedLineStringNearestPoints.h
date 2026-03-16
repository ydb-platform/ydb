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
 *
 **********************************************************************
 *
 * Last port: ORIGINAL WORK
 *
 **********************************************************************/

#ifndef GEOS_GEOM_PREP_PREPAREDLINESTRINGNEARESTPOINTS_H
#define GEOS_GEOM_PREP_PREPAREDLINESTRINGNEARESTPOINTS_H

namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep

class PreparedLineStringNearestPoints {
public:

    static std::unique_ptr<geom::CoordinateSequence>
    nearestPoints(const PreparedLineString& prep, const geom::Geometry* geom)
    {
        PreparedLineStringNearestPoints op(prep);
        return op.nearestPoints(geom);
    }

    PreparedLineStringNearestPoints(const PreparedLineString& prep)
        : prepLine(prep)
    { }

    std::unique_ptr<geom::CoordinateSequence> nearestPoints(const geom::Geometry* g) const;

protected:

    const PreparedLineString& prepLine;

    // Declare type as noncopyable
    PreparedLineStringNearestPoints(const PreparedLineStringNearestPoints& other) = delete;
    PreparedLineStringNearestPoints& operator=(const PreparedLineStringNearestPoints& rhs) = delete;
};

} // namespace geos::geom::prep
} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_PREP_PREPAREDLINESTRINGNEARESTPOINTS_H
