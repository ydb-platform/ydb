/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2018 Daniel Baston <dbaston@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/


#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/util/LinearComponentExtracter.h>
#include <geos/index/intervalrtree/SortedPackedIntervalRTree.h>
#include <geos/util.h>
#include <geos/algorithm/RayCrossingCounter.h>
#include <geos/index/ItemVisitor.h>

#include <algorithm>
#include <typeinfo>

namespace geos {
namespace algorithm {
namespace locate {
//
// private:
//
IndexedPointInAreaLocator::IntervalIndexedGeometry::IntervalIndexedGeometry(const geom::Geometry& g)
    : isEmpty(0)
{
    if (g.isEmpty())
        isEmpty = true;
    else
        init(g);
}

void
IndexedPointInAreaLocator::IntervalIndexedGeometry::init(const geom::Geometry& g)
{
    geom::LineString::ConstVect lines;
    geom::util::LinearComponentExtracter::getLines(g, lines);

    // pre-compute size of segment vector
    size_t nsegs = 0;
    for(const geom::LineString* line : lines) {
        //-- only include rings of Polygons or LinearRings
        if (! line->isClosed())
          continue;

        nsegs += line->getCoordinatesRO()->size() - 1;
    }
    segments.reserve(nsegs);

    for(const geom::LineString* line : lines) {
        //-- only include rings of Polygons or LinearRings
        if (! line->isClosed())
          continue;

        addLine(line->getCoordinatesRO());
    }

    index = index::intervalrtree::SortedPackedIntervalRTree(segments.size());

    for(geom::LineSegment& seg : segments) {
        index.insert(
            std::min(seg.p0.y, seg.p1.y),
            std::max(seg.p0.y, seg.p1.y),
            &seg);
    }
}

void
IndexedPointInAreaLocator::IntervalIndexedGeometry::addLine(const geom::CoordinateSequence* pts)
{
    for(size_t i = 1, ni = pts->size(); i < ni; i++) {
        segments.emplace_back((*pts)[i - 1], (*pts)[i]);
    }
}


void
IndexedPointInAreaLocator::buildIndex(const geom::Geometry& g)
{
    index = detail::make_unique<IntervalIndexedGeometry>(g);
}


//
// protected:
//

//
// public:
//
IndexedPointInAreaLocator::IndexedPointInAreaLocator(const geom::Geometry& g)
    :	areaGeom(g)
{
}

geom::Location
IndexedPointInAreaLocator::locate(const geom::Coordinate* /*const*/ p)
{
    if (index == nullptr) {
        buildIndex(areaGeom);
    }

    algorithm::RayCrossingCounter rcc(*p);

    IndexedPointInAreaLocator::SegmentVisitor visitor(&rcc);

    index->query(p->y, p->y, &visitor);

    return rcc.getLocation();
}

void
IndexedPointInAreaLocator::SegmentVisitor::visitItem(void* item)
{
    geom::LineSegment* seg = static_cast<geom::LineSegment*>(item);

    counter->countSegment(seg->p0, seg->p1);
}

void
IndexedPointInAreaLocator::IntervalIndexedGeometry::query(double min, double max, index::ItemVisitor* visitor)
{
    if (isEmpty)
        return;
    index.query(min, max, visitor);
}


} // geos::algorithm::locate
} // geos::algorithm
} // geos
