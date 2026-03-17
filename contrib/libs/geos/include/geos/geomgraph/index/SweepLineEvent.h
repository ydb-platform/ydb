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
 **********************************************************************/

#ifndef GEOS_GEOMGRAPH_INDEX_SWEEPLINEEVENT_H
#define GEOS_GEOMGRAPH_INDEX_SWEEPLINEEVENT_H


#include <geos/export.h>
#include <string>

// Forward declarations
namespace geos {
namespace geomgraph {
namespace index {
class SweepLineEventOBJ;
}
}
}

namespace geos {
namespace geomgraph { // geos::geomgraph
namespace index { // geos::geomgraph::index

//class SweepLineEventLessThen; // needed ??

class GEOS_DLL SweepLineEvent final {
    friend class SweepLineEventLessThen;

public:

    enum {
        INSERT_EVENT = 1,
        DELETE_EVENT
    };

    SweepLineEvent(void* newEdgeSet, double x,
                   SweepLineEvent* newInsertEvent,
                   SweepLineEventOBJ* newObj);

    ~SweepLineEvent() = default;

    bool
    isInsert()
    {
        return insertEvent == nullptr;
    }

    bool
    isDelete()
    {
        return insertEvent != nullptr;
    }

    int
    eventType()
    {
        return insertEvent == nullptr ? INSERT_EVENT : DELETE_EVENT;
    }

    SweepLineEvent*
    getInsertEvent()
    {
        return insertEvent;
    }

    size_t
    getDeleteEventIndex()
    {
        return deleteEventIndex;
    }

    void
    setDeleteEventIndex(size_t newDeleteEventIndex)
    {
        deleteEventIndex = newDeleteEventIndex;
    }

    SweepLineEventOBJ*
    getObject() const
    {
        return obj;
    }

    int compareTo(SweepLineEvent* sle);

    std::string print();

    void* edgeSet;    // used for red-blue intersection detection

protected:

    SweepLineEventOBJ* obj;

private:

    double xValue;

    SweepLineEvent* insertEvent; // null if this is an INSERT_EVENT event

    size_t deleteEventIndex;
};

class GEOS_DLL SweepLineEventLessThen {
public:
    template<typename T>
    bool
    operator()(const T& f, const T& s) const
    {
        if(f->xValue < s->xValue) {
            return true;
        }
        if(f->xValue > s->xValue) {
            return false;
        }
        if(f->eventType() < s->eventType()) {
            return true;
        }
        return false;
    }
};



} // namespace geos.geomgraph.index
} // namespace geos.geomgraph
} // namespace geos

#endif

