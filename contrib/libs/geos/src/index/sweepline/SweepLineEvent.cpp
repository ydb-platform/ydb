/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/index/sweepline/SweepLineEvent.h>

namespace geos {
namespace index { // geos.index
namespace sweepline { // geos.index.sweepline

SweepLineEvent::SweepLineEvent(double x, SweepLineEvent* newInsertEvent,
                               SweepLineInterval* newSweepInt)
    :
    xValue(x),
    eventType(SweepLineEvent::INSERT_EVENT),
    insertEvent(newInsertEvent),
    sweepInt(newSweepInt)
{
    if(insertEvent != nullptr) {
        eventType = SweepLineEvent::DELETE_EVENT;
    }
}

bool
SweepLineEvent::isInsert()
{
    return insertEvent == nullptr;
}

bool
SweepLineEvent::isDelete()
{
    return insertEvent != nullptr;
}

SweepLineEvent*
SweepLineEvent::getInsertEvent()
{
    return insertEvent;
}

size_t
SweepLineEvent::getDeleteEventIndex()
{
    return deleteEventIndex;
}

void
SweepLineEvent::setDeleteEventIndex(size_t newDeleteEventIndex)
{
    deleteEventIndex = newDeleteEventIndex;
}

SweepLineInterval*
SweepLineEvent::getInterval()
{
    return sweepInt;
}

int
SweepLineEvent::compareTo(const SweepLineEvent* pe) const
{
    if(xValue < pe->xValue) {
        return -1;
    }
    if(xValue > pe->xValue) {
        return 1;
    }
    if(eventType < pe->eventType) {
        return -1;
    }
    if(eventType > pe->eventType) {
        return 1;
    }
    return 0;
}

#if 0
int
SweepLineEvent::compareTo(void* o) const
{
    SweepLineEvent* pe = (SweepLineEvent*) o;
    if(xValue < pe->xValue) {
        return -1;
    }
    if(xValue > pe->xValue) {
        return 1;
    }
    if(eventType < pe->eventType) {
        return -1;
    }
    if(eventType > pe->eventType) {
        return 1;
    }
    return 0;
}
#endif // 0

bool
SweepLineEventLessThen::operator()(const SweepLineEvent* first, const SweepLineEvent* second) const
{
    if(first->compareTo(second) < 0) {
        return true;
    }
    else {
        return false;
    }
}


} // namespace geos.index.sweepline
} // namespace geos.index
} // namespace geos

