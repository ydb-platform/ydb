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

#ifdef _MSC_VER
#pragma warning(disable:4355)
#endif

#include <cassert>
#include <string>
#include <sstream>

#include <geos/algorithm/Orientation.h>
#include <geos/edgegraph/HalfEdge.h>
#include <geos/geom/Quadrant.h>
#include <geos/geom/Coordinate.h>
#include <geos/util/Assert.h>

using namespace geos::geom;

namespace geos {
namespace edgegraph { // geos.edgegraph

/*public static*/
HalfEdge*
HalfEdge::create(const Coordinate& p0, const Coordinate& p1)
{
    HalfEdge* e0 = new HalfEdge(p0);
    HalfEdge* e1 = new HalfEdge(p1);
    e0->link(e1);
    return e0;
}

/*public*/
void
HalfEdge::link(HalfEdge* p_sym)
{
    setSym(p_sym);
    p_sym->setSym(this);
    // set next ptrs for a single segment
    setNext(p_sym);
    p_sym->setNext(this);
}

/*public*/
HalfEdge*
HalfEdge::find(const Coordinate& p_dest)
{
    HalfEdge* oNxt = this;
    do {
        if (oNxt == nullptr) {
            return nullptr;
        }
        if (oNxt->dest().equals2D(p_dest)) {
            return oNxt;
        }
        oNxt = oNxt->oNext();
    } while (oNxt != this);
    return nullptr;
}

/*public*/
bool
HalfEdge::equals(const Coordinate& p0, const Coordinate& p1) const
{
    return m_orig.equals2D(p0) && m_sym->m_orig.equals2D(p1);
}

/*public*/
void
HalfEdge::insert(HalfEdge* eAdd)
{
    // If this is only edge at origin, insert it after this
    if (oNext() == this) {
        // set linkage so ring is correct
        insertAfter(eAdd);
        return;
    }

    // Scan edges
    // until insertion point is found
    HalfEdge* ePrev = insertionEdge(eAdd);
    ePrev->insertAfter(eAdd);
    return;
}

/*private*/
HalfEdge*
HalfEdge::insertionEdge(HalfEdge* eAdd)
{
    HalfEdge* ePrev = this;
    do {
        HalfEdge* eNext = ePrev->oNext();
        /**
        * Case 1: General case,
        * with eNext higher than ePrev.
        *
        * Insert edge here if it lies between ePrev and eNext.
        */
        if (eNext->compareTo(ePrev) > 0
            && eAdd->compareTo(ePrev) >= 0
            && eAdd->compareTo(eNext) <= 0) {
            return ePrev;
        }
        /**
        * Case 2: Origin-crossing case,
        * indicated by eNext <= ePrev.
        *
        * Insert edge here if it lies
        * in the gap between ePrev and eNext across the origin.
        */
        if (eNext->compareTo(ePrev) <= 0
            && (eAdd->compareTo(eNext) <= 0 || eAdd->compareTo(ePrev) >= 0)) {
            return ePrev;
        }
        ePrev = eNext;
    } while (ePrev != this);

    util::Assert::shouldNeverReachHere();
    return nullptr;
}

/*private*/
void
HalfEdge::insertAfter(HalfEdge* e)
{
    assert(m_orig == e->orig());
    HalfEdge* save = oNext();
    m_sym->setNext(e);
    e->sym()->setNext(save);
}

/*public*/
bool
HalfEdge::isEdgesSorted() const
{
    // find lowest edge at origin
    const HalfEdge* lowest = findLowest();
    const HalfEdge* e = lowest;
    // check that all edges are sorted
    do {
        HalfEdge* eNext = e->oNext();
        if (eNext == lowest) break;
        bool isSorted = (eNext->compareTo(e) > 0);
        if (!isSorted) {
            return false;
        }
        e = eNext;
    } while (e != lowest);
    return true;
}

/*private*/
const HalfEdge*
HalfEdge::findLowest() const
{
    const HalfEdge* lowest = this;
    HalfEdge* e = this->oNext();
    do {
        if (e->compareTo(lowest) < 0) {
            lowest = e;
        }
        e = e->oNext();
    } while (e != this);
    return lowest;
}

/*public*/
int
HalfEdge::compareAngularDirection(const HalfEdge* e) const
{
    double dx = directionX();
    double dy = directionY();
    double dx2 = e->directionX();
    double dy2 = e->directionY();

    // same vector
    if (dx == dx2 && dy == dy2)
        return 0;

    int quadrant = geom::Quadrant::quadrant(dx, dy);
    int quadrant2 = geom::Quadrant::quadrant(dx2, dy2);

    /**
    * If the direction vectors are in different quadrants,
    * that determines the ordering
    */
    if (quadrant > quadrant2) return 1;
    if (quadrant < quadrant2) return -1;

    /**
    * Check relative orientation of direction vectors
    * this is > e if it is CCW of e
    */
    const Coordinate& dir1 = directionPt();
    const Coordinate& dir2 = e->directionPt();
    return algorithm::Orientation::index(e->m_orig, dir2, dir1);
}

/*public*/
int
HalfEdge::degree()
{
    int deg = 0;
    HalfEdge* e = this;
    do {
        deg++;
        e = e->oNext();
    } while (e != this);
    return deg;
}

/*public*/
HalfEdge*
HalfEdge::prevNode()
{
    HalfEdge* e = this;
    while (e->degree() == 2) {
        e = e->prev();
        if (e == this)
            return nullptr;
    }
    return e;
}

std::ostream&
operator<< (std::ostream& os, const HalfEdge& e)
{
    os << "HE(" << e.m_orig.x << " " << e.m_orig.y << ", "
       << e.m_sym->m_orig.x << " " << e.m_sym->m_orig.y << ")";
    return os;
}

/*public static*/
void
HalfEdge::toStringNode(const HalfEdge* he, std::ostream& os)
{
    os << "Node( " << he->orig() << " )" << std::endl;
    const HalfEdge* e = he;
    do {
        os << "  -> " << e << std::endl;
        e = e->oNext();
    } while (e != he);
    return;
}




} // namespace geos.edgegraph
} // namespace geos


