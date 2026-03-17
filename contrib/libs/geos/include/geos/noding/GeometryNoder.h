/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * NOTE: this is not in JTS. JTS has a snapround/GeometryNoder though
 *
 **********************************************************************/

#ifndef GEOS_NODING_GEOMETRYNODER_H
#define GEOS_NODING_GEOMETRYNODER_H

#include <geos/export.h>
#include <geos/noding/SegmentString.h> // for NonConstVect

#include <memory> // for unique_ptr

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
}
namespace noding {
class Noder;
}
}

namespace geos {
namespace noding { // geos.noding

class GEOS_DLL GeometryNoder {
public:

    static std::unique_ptr<geom::Geometry> node(const geom::Geometry& geom);

    GeometryNoder(const geom::Geometry& g);

    std::unique_ptr<geom::Geometry> getNoded();

private:

    const geom::Geometry& argGeom;

    SegmentString::NonConstVect lineList;

    static void extractSegmentStrings(const geom::Geometry& g,
                                      SegmentString::NonConstVect& to);

    Noder& getNoder();

    std::unique_ptr<Noder> noder;

    std::unique_ptr<geom::Geometry> toGeometry(SegmentString::NonConstVect& noded);

    GeometryNoder(GeometryNoder const&); /*= delete*/
    GeometryNoder& operator=(GeometryNoder const&); /*= delete*/
};

} // namespace geos.noding
} // namespace geos

#endif // GEOS_NODING_NODER_H
