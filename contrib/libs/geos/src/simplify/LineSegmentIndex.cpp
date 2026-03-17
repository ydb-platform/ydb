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
 **********************************************************************/

#include <geos/simplify/LineSegmentIndex.h>
#include <geos/simplify/TaggedLineSegment.h>
#include <geos/simplify/TaggedLineString.h>
#include <geos/index/quadtree/Quadtree.h>
#include <geos/index/ItemVisitor.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/Envelope.h>

#include <vector>
#include <memory> // for unique_ptr
#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace std;
using namespace geos::geom;
using namespace geos::index::quadtree;

namespace geos {
namespace simplify { // geos::simplify

/**
 * ItemVisitor subclass to reduce volume of query results.
 */
class LineSegmentVisitor: public index::ItemVisitor {

// MD - only seems to make about a 10% difference in overall time.

private:

    const LineSegment* querySeg;

    unique_ptr< vector<LineSegment*> > items;

public:

    LineSegmentVisitor(const LineSegment* s)
        :
        ItemVisitor(),
        querySeg(s),
        items(new vector<LineSegment*>())
    {}

    ~LineSegmentVisitor() override
    {
        // nothing to do, LineSegments are not owned by us
    }

    LineSegmentVisitor(const LineSegmentVisitor& o)
        :
        ItemVisitor(),
        querySeg(o.querySeg),
        items(new vector<LineSegment*>(*(o.items.get())))
    {
    }

    LineSegmentVisitor&
    operator=(const LineSegmentVisitor& o)
    {
        if(this == &o) {
            return *this;
        }
        querySeg = o.querySeg;
        items.reset(new vector<LineSegment*>(*(o.items.get())));
        return *this;
    }

    void
    visitItem(void* item) override
    {
        LineSegment* seg = (LineSegment*) item;
        if(Envelope::intersects(seg->p0, seg->p1,
                                querySeg->p0, querySeg->p1)) {
            items->push_back(seg);
        }
    }

    unique_ptr< vector<LineSegment*> >
    getItems()
    {
        // NOTE: Apparently, this is 'source' method giving up the object resource.
        return std::move(items);
    }


};


/*public*/
void
LineSegmentIndex::add(const TaggedLineString& line)
{
    for(const LineSegment* seg : line.getSegments()) {
        add(seg);
    }
}

/*public*/
void
LineSegmentIndex::add(const LineSegment* seg)
{
    std::unique_ptr<Envelope> env{new Envelope(seg->p0, seg->p1)};

    // We need a cast because index wants a non-const,
    // although it won't change the argument
    index.insert(env.get(), const_cast<LineSegment*>(seg));

    newEnvelopes.push_back(std::move(env));
}

/*public*/
void
LineSegmentIndex::remove(const LineSegment* seg)
{
    Envelope env(seg->p0, seg->p1);

    // We need a cast because index wants a non-const
    // although it won't change the argument
    index.remove(&env, const_cast<LineSegment*>(seg));
}

/*public*/
unique_ptr< vector<LineSegment*> >
LineSegmentIndex::query(const LineSegment* querySeg)
{
    Envelope env(querySeg->p0, querySeg->p1);

    LineSegmentVisitor visitor(querySeg);
    index.query(&env, visitor);

    unique_ptr< vector<LineSegment*> > itemsFound = visitor.getItems();

    return itemsFound;
}

} // namespace geos::simplify
} // namespace geos
