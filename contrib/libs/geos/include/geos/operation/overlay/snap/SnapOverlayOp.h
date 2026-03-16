/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009  Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/snap/SnapOverlayOp.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_OVERLAY_SNAP_SNAPOVERLAYOP_H
#define GEOS_OP_OVERLAY_SNAP_SNAPOVERLAYOP_H

#include <geos/operation/overlay/OverlayOp.h> // for enums
#include <geos/precision/CommonBitsRemover.h> // for dtor visibility by unique_ptr

#include <memory> // for unique_ptr

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Geometry;
struct GeomPtrPair;
}
}

namespace geos {
namespace operation { // geos::operation
namespace overlay { // geos::operation::overlay
namespace snap { // geos::operation::overlay::snap

/** \brief
 * Performs an overlay operation using snapping and enhanced precision
 * to improve the robustness of the result.
 *
 * This class <i>always</i> uses snapping.
 * This is less performant than the standard JTS overlay code,
 * and may even introduce errors which were not present in the original data.
 * For this reason, this class should only be used
 * if the standard overlay code fails to produce a correct result.
 *
 */
class GEOS_DLL SnapOverlayOp {

public:

    static std::unique_ptr<geom::Geometry>
    overlayOp(const geom::Geometry& g0, const geom::Geometry& g1,
              OverlayOp::OpCode opCode)
    {
        SnapOverlayOp op(g0, g1);
        return op.getResultGeometry(opCode);
    }

    static std::unique_ptr<geom::Geometry>
    intersection(const geom::Geometry& g0, const geom::Geometry& g1)
    {
        return overlayOp(g0, g1, OverlayOp::opINTERSECTION);
    }

    static std::unique_ptr<geom::Geometry>
    Union(const geom::Geometry& g0, const geom::Geometry& g1)
    {
        return overlayOp(g0, g1, OverlayOp::opUNION);
    }

    static std::unique_ptr<geom::Geometry>
    difference(const geom::Geometry& g0, const geom::Geometry& g1)
    {
        return overlayOp(g0, g1, OverlayOp::opDIFFERENCE);
    }

    static std::unique_ptr<geom::Geometry>
    symDifference(const geom::Geometry& g0, const geom::Geometry& g1)
    {
        return overlayOp(g0, g1, OverlayOp::opSYMDIFFERENCE);
    }

    SnapOverlayOp(const geom::Geometry& g1, const geom::Geometry& g2)
        :
        geom0(g1),
        geom1(g2)
    {
        computeSnapTolerance();
    }


    typedef std::unique_ptr<geom::Geometry> GeomPtr;

    GeomPtr getResultGeometry(OverlayOp::OpCode opCode);

private:

    void computeSnapTolerance();

    void snap(geom::GeomPtrPair& ret);

    void removeCommonBits(const geom::Geometry& geom0,
                          const geom::Geometry& geom1,
                          geom::GeomPtrPair& ret);

    // re-adds common bits to the given geom
    void prepareResult(geom::Geometry& geom);


    const geom::Geometry& geom0;
    const geom::Geometry& geom1;

    double snapTolerance;

    std::unique_ptr<precision::CommonBitsRemover> cbr;

    // Declare type as noncopyable
    SnapOverlayOp(const SnapOverlayOp& other) = delete;
    SnapOverlayOp& operator=(const SnapOverlayOp& rhs) = delete;
};

} // namespace geos::operation::overlay::snap
} // namespace geos::operation::overlay
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_OP_OVERLAY_SNAP_SNAPOVERLAYOP_H
