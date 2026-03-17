/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_TRIANGULATE_QUADEDGE_QUADEDGEQUARTET_H
#define GEOS_TRIANGULATE_QUADEDGE_QUADEDGEQUARTET_H

#include <geos/triangulate/quadedge/QuadEdge.h>


namespace geos {
namespace triangulate {
namespace quadedge {

class GEOS_DLL QuadEdgeQuartet {

public:
    QuadEdgeQuartet() : e{QuadEdge(0), QuadEdge(1), QuadEdge(2), QuadEdge(3)} {
        e[0].next = &(e[0]);
        e[1].next = &(e[3]);
        e[2].next = &(e[2]);
        e[3].next = &(e[1]);
    };

    static QuadEdge& makeEdge(const Vertex& o, const Vertex & d, std::deque<QuadEdgeQuartet> & edges) {
        edges.emplace_back();
        auto& qe = edges.back();
        qe.base().setOrig(o);
        qe.base().setDest(d);

        return qe.base();
    }

    QuadEdge& base() {
        return e[0];
    }

    const QuadEdge& base() const {
        return e[0];
    }

    void setVisited(bool status) {
        for (auto& edge : e) {
            edge.setVisited(status);
        }
    }

private:
    std::array<QuadEdge, 4> e;
};

}
}
}


#endif
