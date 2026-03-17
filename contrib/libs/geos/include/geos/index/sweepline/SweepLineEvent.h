/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_INDEX_SWEEPLINE_SWEEPLINEEVENT_H
#define GEOS_INDEX_SWEEPLINE_SWEEPLINEEVENT_H

#include <cstddef>
#include <geos/export.h>

// Forward declarations
namespace geos {
namespace index {
namespace sweepline {
class SweepLineInterval;
}
}
}

namespace geos {
namespace index { // geos.index
namespace sweepline { // geos:index:sweepline

class GEOS_DLL SweepLineEvent {

public:

    enum {
        INSERT_EVENT = 1,
        DELETE_EVENT
    };

    SweepLineEvent(double x, SweepLineEvent* newInsertEvent,
                   SweepLineInterval* newSweepInt);

    bool isInsert();

    bool isDelete();

    SweepLineEvent* getInsertEvent();

    size_t getDeleteEventIndex();

    void setDeleteEventIndex(size_t newDeleteEventIndex);

    SweepLineInterval* getInterval();

    /**
     * ProjectionEvents are ordered first by their x-value, and then by their eventType.
     * It is important that Insert events are sorted before Delete events, so that
     * items whose Insert and Delete events occur at the same x-value will be
     * correctly handled.
     */
    int compareTo(const SweepLineEvent* pe) const;

    //int compareTo(void *o) const;

private:

    double xValue;

    int eventType;

    /// null if this is an INSERT_EVENT event
    SweepLineEvent* insertEvent;

    size_t deleteEventIndex;

    SweepLineInterval* sweepInt;

};

// temp typedefs for backward compatibility
//typedef SweepLineEvent indexSweepLineEvent;

struct GEOS_DLL  SweepLineEventLessThen {
    bool operator()(const SweepLineEvent* first, const SweepLineEvent* second) const;
};

//bool isleLessThen(SweepLineEvent *first, SweepLineEvent *second);


} // namespace geos:index:sweepline
} // namespace geos:index
} // namespace geos

#endif // GEOS_INDEX_SWEEPLINE_SWEEPLINEEVENT_H
