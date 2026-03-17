/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: simplify/LineSegmentIndex.java rev. 1.1 (JTS-1.7.1)
 *
 **********************************************************************
 *
 * NOTES
 *
 **********************************************************************/

#ifndef GEOS_SIMPLIFY_LINESEGMENTINDEX_H
#define GEOS_SIMPLIFY_LINESEGMENTINDEX_H

#include <geos/export.h>
#include <geos/geom/Envelope.h>
#include <geos/index/quadtree/Quadtree.h>
#include <vector>
#include <memory> // for unique_ptr

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class LineSegment;
}
namespace simplify {
class TaggedLineString;
}
}

namespace geos {
namespace simplify { // geos::simplify

class GEOS_DLL LineSegmentIndex {

public:

    LineSegmentIndex() = default;

    ~LineSegmentIndex() = default;

    void add(const TaggedLineString& line);

    void add(const geom::LineSegment* seg);

    void remove(const geom::LineSegment* seg);

    std::unique_ptr< std::vector<geom::LineSegment*> >
    query(const geom::LineSegment* seg);


private:

    index::quadtree::Quadtree index;

    std::vector<std::unique_ptr<geom::Envelope>> newEnvelopes;

    /**
     * Disable copy construction and assignment. Apparently needed to make this
     * class compile under MSVC. (See https://stackoverflow.com/q/29565299)
     */
    LineSegmentIndex(const LineSegmentIndex&) = delete;
    LineSegmentIndex& operator=(const LineSegmentIndex&) = delete;
};

} // namespace geos::simplify
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_SIMPLIFY_LINESEGMENTINDEX_H
