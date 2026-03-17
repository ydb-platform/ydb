/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Daniel Baston <dbaston@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/polygonize/HoleAssigner.java 0b3c7e3eb0d3e
 *
 **********************************************************************/

#ifndef GEOS_OP_POLYGONIZE_HOLEASSIGNER_H
#define GEOS_OP_POLYGONIZE_HOLEASSIGNER_H

#include <geos/operation/polygonize/EdgeRing.h>
#include <geos/index/strtree/STRtree.h>

#include <vector>

namespace geos {
namespace operation {
namespace polygonize {

/** \brief
 * Assigns hole rings to shell rings during polygonization.
 *
 * Uses spatial indexing to improve performance of shell lookup.
 *
 * @author mdavis
 */
class GEOS_DLL HoleAssigner {
public:
    /**
     * Assigns hole rings to shell rings
     * @param holes list of hole rings to assign
     * @param shells list of shell rings
     */
    static void assignHolesToShells(std::vector<EdgeRing*> & holes, std::vector<EdgeRing*> & shells);

private:
    explicit HoleAssigner(std::vector<EdgeRing*> & shells) : m_shells(shells) {
        buildIndex();
    }

    void assignHolesToShells(std::vector<EdgeRing*> & holes);
    void assignHoleToShell(EdgeRing* holeER);
    std::vector<EdgeRing*> findShells(const geom::Envelope & ringEnv);

    EdgeRing* findEdgeRingContaining(EdgeRing* testER);

    void buildIndex();

    std::vector<EdgeRing*>& m_shells;
    geos::index::strtree::STRtree m_shellIndex;
};
}
}
}

#endif
