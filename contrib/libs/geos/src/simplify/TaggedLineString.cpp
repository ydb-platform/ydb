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
 * Last port: simplify/TaggedLineString.java rev. 1.2 (JTS-1.7.1)
 *
 **********************************************************************/

#include <geos/simplify/TaggedLineString.h>
#include <geos/simplify/TaggedLineSegment.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Geometry.h> // for unique_ptr destructor
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>

#include <cassert>
#include <memory>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace geos::geom;
using namespace std;

namespace geos {
namespace simplify { // geos::simplify

/*public*/
TaggedLineString::TaggedLineString(const geom::LineString* nParentLine,
                                   std::size_t nMinimumSize)
    :
    parentLine(nParentLine),
    minimumSize(nMinimumSize)
{
    init();
}

/*public*/
TaggedLineString::~TaggedLineString()
{
#if GEOS_DEBUG
    cerr << "TaggedLineString[" << this << "] destructor"
         << endl;
#endif

    for(std::size_t i = 0, n = segs.size(); i < n; i++) {
        delete segs[i];
    }

    for(std::size_t i = 0, n = resultSegs.size(); i < n; i++) {
        delete resultSegs[i];
    }
}

/*private*/
void
TaggedLineString::init()
{
    assert(parentLine);
    const CoordinateSequence* pts = parentLine->getCoordinatesRO();

#if GEOS_DEBUG
    cerr << "TaggedLineString[" << this << "] pts.size() " << pts->size()
         << endl;
#endif

    if(!pts->isEmpty()) {

        segs.reserve(pts->size() - 1);

        for(std::size_t i = 0, n = pts->size() - 1; i < n; i++) {
            TaggedLineSegment* seg = new TaggedLineSegment(
                pts->getAt(i),
                pts->getAt(i + 1),
                parentLine, i);

            segs.push_back(seg);
        }

    }

#if GEOS_DEBUG
    cerr << "TaggedLineString[" << this << "] segs.size " << segs.size()
         << endl;
    cerr << "TaggedLineString[" << this << "] resultSegs.size " << resultSegs.size()
         << endl;
#endif
}

/*public*/
std::size_t
TaggedLineString::getMinimumSize() const
{
    return minimumSize;
}

/*public*/
const geom::LineString*
TaggedLineString::getParent() const
{
    return parentLine;
}

/*public*/
const CoordinateSequence*
TaggedLineString::getParentCoordinates() const
{
    assert(parentLine);
    return parentLine->getCoordinatesRO();
}

/*public*/
CoordinateSequence::Ptr
TaggedLineString::getResultCoordinates() const
{

#if GEOS_DEBUG
    cerr << __FUNCTION__ << " resultSegs.size: "
         << resultSegs.size() << endl;
#endif

    CoordVectPtr pts = extractCoordinates(resultSegs);

#if GEOS_DEBUG
    cerr << __FUNCTION__ << " extracted Coords.size: "
         << pts->size() << endl;
#endif


    CoordVect* v = pts.release();
    return CoordinateSequence::Ptr(parentLine->getFactory()->getCoordinateSequenceFactory()->create(v));

}

/*private static*/
TaggedLineString::CoordVectPtr
TaggedLineString::extractCoordinates(
    const std::vector<TaggedLineSegment*>& segs)
{
    CoordVectPtr pts(new CoordVect());

#if GEOS_DEBUG
    cerr << __FUNCTION__ << " segs.size: " << segs.size() << endl;
#endif

    std::size_t i = 0, size = segs.size();

    if(size) {
        for(; i < size; i++) {
            TaggedLineSegment* seg = segs[i];
            assert(seg);
            pts->push_back(seg->p0);
        }

        // add last point
        pts->push_back(segs[size - 1]->p1);
    }

    return pts;
}

/*public*/
std::size_t
TaggedLineString::getResultSize() const
{
    auto resultSegsSize = resultSegs.size();
    return resultSegsSize == 0 ? 0 : resultSegsSize + 1;
}

/*public*/
TaggedLineSegment*
TaggedLineString::getSegment(std::size_t i)
{
    return segs[i];
}

/*public*/
const TaggedLineSegment*
TaggedLineString::getSegment(std::size_t i) const
{
    return segs[i];
}

/*public*/
vector<TaggedLineSegment*>&
TaggedLineString::getSegments()
{
    assert(0);
    return segs;
}

/*public*/
const vector<TaggedLineSegment*>&
TaggedLineString::getSegments() const
{
    return segs;
}

/*public*/
unique_ptr<Geometry>
TaggedLineString::asLineString() const
{
    return parentLine->getFactory()->createLineString(
               getResultCoordinates());
}

/*public*/
unique_ptr<Geometry>
TaggedLineString::asLinearRing() const
{
    return std::unique_ptr<Geometry>(parentLine->getFactory()->createLinearRing(
               getResultCoordinates()));
}

/*public*/
void
TaggedLineString::addToResult(unique_ptr<TaggedLineSegment> seg)
{
#if GEOS_DEBUG
    cerr << "TaggedLineString[" << this << "] adding "
         << " seg " << seg.get() << " to result"
         << endl;
#endif
    resultSegs.push_back(seg.release());
#if GEOS_DEBUG
    cerr << "TaggedLineString[" << this << "] adding "
         << " seg " << seg.get() << " to result"
         << endl;
#endif
}

} // namespace geos::simplify
} // namespace geos
