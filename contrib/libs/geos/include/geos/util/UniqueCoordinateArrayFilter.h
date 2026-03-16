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

#ifndef GEOS_UTIL_UNIQUECOORDINATEARRAYFILTER_H
#define GEOS_UTIL_UNIQUECOORDINATEARRAYFILTER_H

#include <geos/export.h>
#include <cassert>
#include <set>
#include <vector>

#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Coordinate.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

namespace geos {
namespace util { // geos::util

/*
 *  A CoordinateFilter that fills a vector of Coordinate const pointers.
 *  The set of coordinates contains no duplicate points.
 *
 *  Last port: util/UniqueCoordinateArrayFilter.java rev. 1.17
 */
class GEOS_DLL UniqueCoordinateArrayFilter: public geom::CoordinateFilter {
public:
    /**
     * Constructs a CoordinateArrayFilter.
     *
     * @param target The destination set.
     */
    UniqueCoordinateArrayFilter(geom::Coordinate::ConstVect& target)
        : pts(target)
    {}

    /**
     * Destructor.
     * Virtual dctor promises appropriate behaviour when someone will
     * delete a derived-class object via a base-class pointer.
     * http://www.parashift.com/c++-faq-lite/virtual-functions.html#faq-20.7
     */
    ~UniqueCoordinateArrayFilter() override {}

    /**
     * Performs a filtering operation with or on coord in "read-only" mode.
     * @param coord The "read-only" Coordinate to which
     * 				the filter is applied.
     */
    void
    filter_ro(const geom::Coordinate* coord) override
    {
        if(uniqPts.insert(coord).second) {
            pts.push_back(coord);
        }
    }

private:
    geom::Coordinate::ConstVect& pts;	// target set reference
    geom::Coordinate::ConstSet uniqPts; 	// unique points set

    // Declare type as noncopyable
    UniqueCoordinateArrayFilter(const UniqueCoordinateArrayFilter& other) = delete;
    UniqueCoordinateArrayFilter& operator=(const UniqueCoordinateArrayFilter& rhs) = delete;
};

} // namespace geos::util
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_UTIL_UNIQUECOORDINATEARRAYFILTER_H
