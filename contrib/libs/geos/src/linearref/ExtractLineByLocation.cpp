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
 * Last port: linearref/ExtractLineByLocation.java rev. 1.35
 *
 **********************************************************************/

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/LineString.h>
#include <geos/linearref/ExtractLineByLocation.h>
#include <geos/linearref/LinearIterator.h>
#include <geos/linearref/LinearLocation.h>
#include <geos/linearref/LengthLocationMap.h>
#include <geos/linearref/LengthIndexOfPoint.h>
#include <geos/linearref/LinearGeometryBuilder.h>

#include <cassert>
#include <typeinfo>

using namespace std;

using namespace geos::geom;

namespace geos {
namespace linearref { // geos.linearref


std::unique_ptr<Geometry>
ExtractLineByLocation::extract(const Geometry* line, const LinearLocation& start, const LinearLocation& end)
{
    ExtractLineByLocation ls(line);
    return ls.extract(start, end);
}

ExtractLineByLocation::ExtractLineByLocation(const Geometry* p_line) :
    line(p_line) {}


std::unique_ptr<Geometry>
ExtractLineByLocation::extract(const LinearLocation& start, const LinearLocation& end)
{
    if(end.compareTo(start) < 0) {
        auto backwards = computeLinear(end, start);
        auto forwards = reverse(backwards.get());
        return forwards;
    }
    return computeLinear(start, end);
}

std::unique_ptr<Geometry>
ExtractLineByLocation::reverse(const Geometry* linear)
{
    const LineString* ls = dynamic_cast<const LineString*>(linear);
    if(ls) {
        return ls->reverse();
    }
    else {
        const MultiLineString* mls = dynamic_cast<const MultiLineString*>(linear);
        if(mls) {
            return mls->reverse();
        }
        else {
            assert(!static_cast<bool>("non-linear geometry encountered"));
            return nullptr;
        }
    }
}

std::unique_ptr<LineString>
ExtractLineByLocation::computeLine(const LinearLocation& start, const LinearLocation& end)
{
    auto coordinates = line->getCoordinates();
    CoordinateArraySequence newCoordinateArray;

    const size_t indexStep = 1;
    auto startSegmentIndex = start.getSegmentIndex();

    if(start.getSegmentFraction() > 0.0) {
        startSegmentIndex += indexStep;
    }

    auto lastSegmentIndex = end.getSegmentIndex();
    if(end.getSegmentFraction() == 1.0) {
        lastSegmentIndex += indexStep;
    }

    if(lastSegmentIndex >= coordinates->size()) {
        assert(!coordinates->isEmpty());
        lastSegmentIndex = coordinates->size() - indexStep;
    }

    if(! start.isVertex()) {
        newCoordinateArray.add(start.getCoordinate(line));
    }

    for(auto i = startSegmentIndex; i <= lastSegmentIndex; i++) {
        newCoordinateArray.add((*coordinates)[i]);
    }

    if(! end.isVertex()) {
        newCoordinateArray.add(end.getCoordinate(line));
    }

    // ensure there is at least one coordinate in the result
    if(newCoordinateArray.isEmpty()) {
        newCoordinateArray.add(start.getCoordinate(line));
    }

    /*
     * Ensure there is enough coordinates to build a valid line.
     * Make a 2-point line with duplicate coordinates, if necessary.
     * There will always be at least one coordinate in the coordList.
     */
    if(newCoordinateArray.size() <= 1) {
        newCoordinateArray.add(newCoordinateArray[0]);
    }

    return std::unique_ptr<LineString>(line->getFactory()->createLineString(newCoordinateArray));
}

std::unique_ptr<Geometry>
ExtractLineByLocation::computeLinear(const LinearLocation& start, const LinearLocation& end)
{
    LinearGeometryBuilder builder(line->getFactory());
    builder.setFixInvalidLines(true);

    if(! start.isVertex()) {
        builder.add(start.getCoordinate(line));
    }

    for(LinearIterator it(line, start); it.hasNext(); it.next()) {
        if(end.compareLocationValues(it.getComponentIndex(), it.getVertexIndex(), 0.0) < 0) {
            break;
        }
        Coordinate pt = it.getSegmentStart();
        builder.add(pt);
        if(it.isEndOfLine()) {
            builder.endLine();
        }
    }
    if(! end.isVertex()) {
        builder.add(end.getCoordinate(line));
    }
    return std::unique_ptr<Geometry>(builder.getGeometry());
}
}
}
