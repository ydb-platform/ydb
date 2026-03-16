/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
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
 * Last port: geomgraph/GraphComponent.java r428 (JTS-1.12+)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_GRAPHCOMPONENT_H
#define GEOS_GEOMGRAPH_GRAPHCOMPONENT_H

#include <geos/export.h>
#include <geos/inline.h>

#include <geos/geomgraph/Label.h>

// Forward declarations
namespace geos {
namespace geom {
class IntersectionMatrix;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph


/** \brief
 * A GraphComponent is the parent class for the objects'
 * that form a graph.
 *
 * Each GraphComponent can carry a Label.
 */
class GEOS_DLL GraphComponent {
public:
    GraphComponent();

    /*
     * GraphComponent copies the given Label.
     */
    GraphComponent(const Label& newLabel);

    virtual ~GraphComponent() = default;

    Label&
    getLabel()
    {
        return label;
    }
    const Label&
    getLabel() const
    {
        return label;
    }
    void
    setLabel(const Label& newLabel)
    {
        label = newLabel;
    }

    virtual void
    setInResult(bool p_isInResult)
    {
        isInResultVar = p_isInResult;
    }
    virtual bool
    isInResult() const
    {
        return isInResultVar;
    }
    virtual void setCovered(bool isCovered);
    virtual bool
    isCovered() const
    {
        return isCoveredVar;
    }
    virtual bool
    isCoveredSet() const
    {
        return isCoveredSetVar;
    }
    virtual bool
    isVisited() const
    {
        return isVisitedVar;
    }
    virtual void
    setVisited(bool p_isVisited)
    {
        isVisitedVar = p_isVisited;
    }
    virtual bool isIsolated() const = 0;
    virtual void updateIM(geom::IntersectionMatrix& im);
protected:
    Label label;
    virtual void computeIM(geom::IntersectionMatrix& im) = 0;
private:
    bool isInResultVar;
    bool isCoveredVar;
    bool isCoveredSetVar;
    bool isVisitedVar;
};

} // namespace geos.geomgraph
} // namespace geos

#endif // ifndef GEOS_GEOMGRAPH_GRAPHCOMPONENT_H
