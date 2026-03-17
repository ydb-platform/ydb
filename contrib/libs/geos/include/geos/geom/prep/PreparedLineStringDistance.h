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

#ifndef GEOS_GEOM_PREP_PREPAREDLINESTRINGDISTANCE_H
#define GEOS_GEOM_PREP_PREPAREDLINESTRINGDISTANCE_H

namespace geos {
namespace geom { // geos::geom
namespace prep { // geos::geom::prep

class PreparedLineString;

class PreparedLineStringDistance {
public:

    static double distance(const PreparedLineString& prep, const geom::Geometry* geom)
    {
        PreparedLineStringDistance op(prep);
        return op.distance(geom);
    }

    PreparedLineStringDistance(const PreparedLineString& prep)
        : prepLine(prep)
    { }

    double distance(const geom::Geometry* g) const;

protected:

    const PreparedLineString& prepLine;

    // Declare type as noncopyable
    PreparedLineStringDistance(const PreparedLineStringDistance& other) = delete;
    PreparedLineStringDistance& operator=(const PreparedLineStringDistance& rhs) = delete;
};

} // namespace geos::geom::prep
} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_PREP_PREPAREDLINESTRINGDISTANCE_H
