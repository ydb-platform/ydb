/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/


#pragma once

#include <geos/geom/Coordinate.h>

#include <geos/export.h>
#include <string>
#include <cassert>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
}

namespace geos {
namespace edgegraph { // geos.edgegraph

class GEOS_DLL MarkHalfEdge : public HalfEdge {

private:

    bool m_isMarked;

public:

    /**
    * Creates a new marked edge.
    *
    * @param orig the coordinate of the edge origin
    */
    MarkHalfEdge(const geom::Coordinate& p_orig) :
        HalfEdge(p_orig),
        m_isMarked(false)
    {};

    /**
    * Tests whether the given edge is marked.
    *
    * @param e the edge to test
    * @return true if the edge is marked
    */
    static bool isMarked(HalfEdge* e);

    /**
    * Marks the given edge.
    *
    * @param e the edge to mark
    */
    static void mark(HalfEdge* e);

    /**
    * Sets the mark for the given edge to a boolean value.
    *
    * @param e the edge to set
    * @param isMarked the mark value
    */
    static void setMark(HalfEdge* e, bool isMarked);

    /**
    * Sets the mark for the given edge pair to a boolean value.
    *
    * @param e an edge of the pair to update
    * @param isMarked the mark value to set
    */
    static void setMarkBoth(HalfEdge* e, bool isMarked);

    /**
    * Marks the edges in a pair.
    *
    * @param e an edge of the pair to mark
    */
    static void markBoth(HalfEdge* e);

    /**
    * Tests whether this edge is marked.
    *
    * @return true if this edge is marked
    */
    bool isMarked() const { return m_isMarked; }

    /**
    * Marks this edge.
    *
    */
    void mark() { m_isMarked = true; }

    /**
    * Sets the value of the mark on this edge.
    *
    * @param isMarked the mark value to set
    */
    void setMark(bool p_isMarked) { m_isMarked = p_isMarked; }

};


} // namespace geos.edgegraph
} // namespace geos



