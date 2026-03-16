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
 * Last port: simplify/TaggedLineSegment.java rev. 1.1 (JTS-1.7)
 *
 **********************************************************************
 *
 * NOTES: Use of this class by DP simplification algorithms
 * makes it useless for a TaggedLineSegment to store copies
 * of coordinates. Using pointers would be good enough here.
 * We don't do it to avoid having to break inheritance from
 * LineSegment, which has copies intead. Wheter LineSegment
 * itself should be refactored can be discussed.
 *  --strk 2006-04-12
 *
 **********************************************************************/

#ifndef GEOS_SIMPLIFY_TAGGEDLINESEGMENT_H
#define GEOS_SIMPLIFY_TAGGEDLINESEGMENT_H

#include <geos/export.h>
#include <geos/geom/LineSegment.h> // for inheritance


// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Geometry;
}
}

namespace geos {
namespace simplify { // geos::simplify


/** \brief
 * A geom::LineSegment which is tagged with its location in a geom::Geometry.
 *
 * Used to index the segments in a geometry and recover the segment locations
 * from the index.
 */
class GEOS_DLL TaggedLineSegment: public geom::LineSegment {

public:

    TaggedLineSegment(const geom::Coordinate& p0,
                      const geom::Coordinate& p1,
                      const geom::Geometry* parent,
                      size_t index);

    TaggedLineSegment(const geom::Coordinate& p0,
                      const geom::Coordinate& p1);

    TaggedLineSegment(const TaggedLineSegment& ls);

    const geom::Geometry* getParent() const;

    size_t getIndex() const;

private:

    const geom::Geometry* parent;

    size_t index;

};



} // namespace geos::simplify
} // namespace geos

#endif // GEOS_SIMPLIFY_TAGGEDLINESEGMENT_H
