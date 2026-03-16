/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Excensus LLC.
 * Copyright (C) 2019 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: triangulate/quadedge/QuadEdge.java r524
 *
 **********************************************************************/

#include <geos/triangulate/quadedge/QuadEdge.h>
#include <geos/triangulate/quadedge/QuadEdgeQuartet.h>

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

using namespace geos::geom;

QuadEdge* QuadEdge::makeEdge(const Vertex& o, const Vertex& d, std::deque<QuadEdgeQuartet> & edges) {
    return &QuadEdgeQuartet::makeEdge(o, d, edges);
}

QuadEdge*
QuadEdge::connect(QuadEdge& a, QuadEdge& b, std::deque<QuadEdgeQuartet> & edges)
{
    QuadEdge* q0 = makeEdge(a.dest(), b.orig(), edges);
    splice(*q0, a.lNext());
    splice(q0->sym(), b);
    return q0;
}

void
QuadEdge::splice(QuadEdge& a, QuadEdge& b)
{
    QuadEdge& alpha = a.oNext().rot();
    QuadEdge& beta = b.oNext().rot();

    QuadEdge& t1 = b.oNext();
    QuadEdge& t2 = a.oNext();
    QuadEdge& t3 = beta.oNext();
    QuadEdge& t4 = alpha.oNext();

    a.setNext(&t1);
    b.setNext(&t2);
    alpha.setNext(&t3);
    beta.setNext(&t4);
}

void
QuadEdge::swap(QuadEdge& e)
{
    QuadEdge& a = e.oPrev();
    QuadEdge& b = e.sym().oPrev();
    splice(e, a);
    splice(e.sym(), b);
    splice(e, a.lNext());
    splice(e.sym(), b.lNext());
    e.setOrig(a.dest());
    e.setDest(b.dest());
}

const QuadEdge&
QuadEdge::getPrimary()
{
    if(orig().getCoordinate().compareTo(dest().getCoordinate()) <= 0) {
        return *this;
    }
    else {
        return sym();
    }
}

void
QuadEdge::remove()
{
    rot().rot().rot().isAlive = false;
    rot().rot().isAlive = false;
    rot().isAlive = false;
    isAlive = false;
}

bool
QuadEdge::equalsNonOriented(const QuadEdge& qe) const
{
    if(equalsOriented(qe)) {
        return true;
    }
    if(equalsOriented(qe.sym())) {
        return true;
    }
    return false;
}

bool
QuadEdge::equalsOriented(const QuadEdge& qe) const
{
    if(orig().getCoordinate().equals2D(qe.orig().getCoordinate())
            && dest().getCoordinate().equals2D(qe.dest().getCoordinate())) {
        return true;
    }
    return false;
}

std::unique_ptr<LineSegment>
QuadEdge::toLineSegment() const
{
    return std::unique_ptr<geom::LineSegment>(
               new geom::LineSegment(vertex.getCoordinate(), dest().getCoordinate()));
}

std::ostream&
operator<< (std::ostream& os, const QuadEdge* e)
{
    os << "( " << e->orig().getCoordinate() << ", " << e->dest().getCoordinate() << " )";
    return os;
}

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace goes
