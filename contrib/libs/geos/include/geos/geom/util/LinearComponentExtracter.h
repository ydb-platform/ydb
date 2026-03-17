/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_UTIL_LINEARCOMPONENTEXTRACTER_H
#define GEOS_GEOM_UTIL_LINEARCOMPONENTEXTRACTER_H


#include <geos/export.h>
#include <vector>

#include <geos/geom/GeometryComponentFilter.h>
#include <geos/geom/Geometry.h> // to be removed when we have the .inl
#include <geos/geom/LineString.h> // to be removed when we have the .inl

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/**
 * Extracts all the 1-dimensional (LineString) components from a Geometry.
 */
class GEOS_DLL LinearComponentExtracter: public GeometryComponentFilter {

private:

    LineString::ConstVect& comps;

    // Declare type as noncopyable
    LinearComponentExtracter(const LinearComponentExtracter& other) = delete;
    LinearComponentExtracter& operator=(const LinearComponentExtracter& rhs) = delete;

public:
    /**
     * Push the linear components from a single geometry into
     * the provided vector.
     * If more than one geometry is to be processed, it is more
     * efficient to create a single LinearComponentExtracterFilter instance
     * and pass it to multiple geometries.
     */
    static void getLines(const Geometry& geom, std::vector<const LineString*>& ret);
    /**
     * Constructs a LinearComponentExtracterFilter with a list in which
     * to store LineStrings found.
     */
    LinearComponentExtracter(std::vector<const LineString*>& newComps);

    void filter_rw(Geometry* geom) override;

    void filter_ro(const Geometry* geom) override;

};

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif
