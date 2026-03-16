/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: linearref/ExtractLineByLocation.java rev. 1.10
 *
 **********************************************************************/

#ifndef GEOS_LINEARREF_EXTRACTLINEBYLOCATION_H
#define GEOS_LINEARREF_EXTRACTLINEBYLOCATION_H

#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/linearref/LinearLocation.h>

namespace geos {
namespace linearref { // geos::linearref

/** \brief
 * Extracts the subline of a linear [Geometry](@ref geom::Geometry) between
 * two {@link LinearLocation}s on the line.
 */
class ExtractLineByLocation {

private:
    const geom::Geometry* line;
    std::unique_ptr<geom::Geometry> reverse(const geom::Geometry* linear);

    /**
     * Assumes input is valid (e.g. start <= end)
     *
     * @param start
     * @param end
     * @return a linear geometry
     */
    std::unique_ptr<geom::LineString> computeLine(const LinearLocation& start, const LinearLocation& end);

    /**
     * Assumes input is valid (e.g. start <= end)
     *
     * @param start
     * @param end
     * @return a linear geometry
     */
    std::unique_ptr<geom::Geometry> computeLinear(const LinearLocation& start, const LinearLocation& end);

public:
    /** \brief
     * Computes the subline of a [LineString](@ref geom::LineString) between
     * two {@link LinearLocation}s on the line.
     *
     * If the start location is after the end location,
     * the computed geometry is reversed.
     *
     * @param line the line to use as the baseline
     * @param start the start location
     * @param end the end location
     * @return the extracted subline
     */
    static std::unique_ptr<geom::Geometry> extract(const geom::Geometry* line, const LinearLocation& start, const LinearLocation& end);

    ExtractLineByLocation(const geom::Geometry* line);

    /** \brief
     * Extracts a subline of the input.
     *
     * If `end < start` the linear geometry computed will be reversed.
     *
     * @param start the start location
     * @param end the end location
     * @return a linear geometry
     */
    std::unique_ptr<geom::Geometry> extract(const LinearLocation& start, const LinearLocation& end);

};
}
}
#endif
