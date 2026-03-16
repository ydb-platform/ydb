/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Martin Davis <mtnclimb@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/InteriorPointArea.java (JTS-1.17+)
 * https://github.com/locationtech/jts/commit/a140ca30cc51be4f65c950a30b0a8f51a6df75ba
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_INTERIORPOINTAREA_H
#define GEOS_ALGORITHM_INTERIORPOINTAREA_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
class Polygon;
}
}

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Computes a point in the interior of an areal geometry.
 * The point will lie in the geometry interior
 * in all except certain pathological cases.
 *
 * <h2>Algorithm</h2>
 * For each input polygon:
 * <ul>
 * <li>Determine a horizontal scan line on which the interior
 * point will be located.
 * To increase the chance of the scan line
 * having non-zero-width intersection with the polygon
 * the scan line Y ordinate is chosen to be near the centre of the polygon's
 * Y extent but distinct from all of vertex Y ordinates.
 * <li>Compute the sections of the scan line
 * which lie in the interior of the polygon.
 * <li>Choose the widest interior section
 * and take its midpoint as the interior point.
 * </ul>
 * The final interior point is chosen as
 * the one occurring in the widest interior section.
 * <p>
 * This algorithm is a tradeoff between performance
 * and point quality (where points further from the geometry
 * boundary are considered to be higher quality)
 * Priority is given to performance.
 * This means that the computed interior point
 * may not be suitable for some uses
 * (such as label positioning).
 * <p>
 * The algorithm handles some kinds of invalid/degenerate geometry,
 * including zero-area and self-intersecting polygons.
 * <p>
 * Empty geometry is handled by returning a <code>null</code> point.
 *
 * <h3>KNOWN BUGS</h3>
 * <ul>
 * <li>If a fixed precision model is used, in some cases this method may return
 * a point which does not lie in the interior.
 * <li>If the input polygon is <i>extremely</i> narrow the computed point
 * may not lie in the interior of the polygon.
 * </ul>
 */
class GEOS_DLL InteriorPointArea {

public:
    /**
     * Creates a new interior point finder
     * for an areal geometry.
     *
     * @param g an areal geometry
     */
    InteriorPointArea(const geom::Geometry* g);

    /**
     * Gets the computed interior point.
     *
     * @return the coordinate of an interior point
     *  or <code>null</code> if the input geometry is empty
     */
    bool getInteriorPoint(geom::Coordinate& ret) const;

private:
    geom::Coordinate interiorPoint;
    double maxWidth;

    void process(const geom::Geometry* geom);

    void processPolygon(const geom::Polygon* polygon);

};

} // namespace geos::algorithm
} // namespace geos

#endif // GEOS_ALGORITHM_INTERIORPOINTAREA_H

