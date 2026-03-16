/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <sstream>

#include <geos/geomgraph/index/SweepLineEvent.h>
#include <geos/geomgraph/index/SweepLineEventObj.h>

namespace geos {
namespace geomgraph { // geos.geomgraph
namespace index { // geos.geomgraph.index

SweepLineEvent::SweepLineEvent(void* newEdgeSet, double x,
                               SweepLineEvent* newInsertEvent, SweepLineEventOBJ* newObj):
    edgeSet(newEdgeSet),
    obj(newObj),
    xValue(x),
    insertEvent(newInsertEvent),
    deleteEventIndex(0)
{
}

/**
 * ProjectionEvents are ordered first by their x-value, and then by their
 * eventType.
 * It is important that Insert events are sorted before Delete events, so that
 * items whose Insert and Delete events occur at the same x-value will be
 * correctly handled.
 */
int
SweepLineEvent::compareTo(SweepLineEvent* sle)
{
    if(xValue < sle->xValue) {
        return -1;
    }
    if(xValue > sle->xValue) {
        return 1;
    }
    if(eventType() < sle->eventType()) {
        return -1;
    }
    if(eventType() > sle->eventType()) {
        return 1;
    }
    return 0;
}

std::string
SweepLineEvent::print()
{
    std::ostringstream s;

    s << "SweepLineEvent:";
    s << " xValue=" << xValue << " deleteEventIndex=" << deleteEventIndex;
    s << ((eventType() == INSERT_EVENT) ? " INSERT_EVENT" : " DELETE_EVENT");
    s << std::endl << "\tinsertEvent=";
    if(insertEvent) {
        s << insertEvent->print();
    }
    else {
        s << "NULL";
    }
    return s.str();
}

} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos
