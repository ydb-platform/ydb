/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006      Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/snapround/HotPixel.java r320 (JTS-1.12)
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>

#include <geos/inline.h>

#include <geos/geom/Coordinate.h> // for composition
#include <geos/geom/Envelope.h> // for unique_ptr
#include <geos/io/WKTWriter.h>


#include <array>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Envelope;
}
namespace algorithm {
class LineIntersector;
}
namespace noding {
class NodedSegmentString;
}
}

namespace geos {
namespace noding { // geos::noding
namespace snapround { // geos::noding::snapround

/** \brief
 * Implements a "hot pixel" as used in the Snap Rounding algorithm.
 *
 * A hot pixel contains the interior of the tolerance square and
 * the boundary minus the top and right segments.
 *
 * The hot pixel operations are all computed in the integer domain
 * to avoid rounding problems.
 */
class GEOS_DLL HotPixel {

private:

    static constexpr double TOLERANCE = 0.5;

    static constexpr int UPPER_RIGHT = 0;
    static constexpr int UPPER_LEFT  = 1;
    static constexpr int LOWER_LEFT  = 2;
    static constexpr int LOWER_RIGHT = 3;

    geom::Coordinate originalPt;
    double scaleFactor;

    /* Indicates if this hot pixel must be a node in the output. */
    bool hpIsNode;

    /* The scaled ordinates of the hot pixel point */
    double hpx;
    double hpy;

    double scaleRound(double val) const;

    double scale(double val) const;

    bool intersectsPixelClosure(const geom::Coordinate& p0,
                                const geom::Coordinate& p1) const;

    bool intersectsScaled(double p0x, double p0y, double p1x, double p1y) const;

    // Declare type as noncopyable
    HotPixel(const HotPixel& other) = delete;
    HotPixel& operator=(const HotPixel& rhs) = delete;

public:

    /**
    * Gets the width of the hot pixel in the original coordinate system.
    */
    double getWidth() const { return 1.0 / scaleFactor; };

    double getScaleFactor() const { return scaleFactor; };

    /**
     * Creates a new hot pixel.
     *
     * @param pt the coordinate at the centre of the pixel.
     *           Will be kept by reference, so make sure to keep it alive.
     * @param scaleFactor the scaleFactor determining the pixel size     */
    HotPixel(const geom::Coordinate& pt, double scaleFactor);

    /*
    * Gets the coordinate this hot pixel is based at.
    *
    * @return the coordinate of the pixel
    */
    const geom::Coordinate& getCoordinate() const;

    /**
     * Tests whether the line segment (p0-p1) intersects this hot pixel.
     *
     * @param p0 the first coordinate of the line segment to test
     * @param p1 the second coordinate of the line segment to test
     * @return true if the line segment intersects this hot pixel
     */
    bool intersects(const geom::Coordinate& p0,
                    const geom::Coordinate& p1) const;

    /**
    * Tests whether a coordinate lies in (intersects) this hot pixel.
    *
    * @param p the coordinate to test
    * @return true if the coordinate intersects this hot pixel
    */
    bool intersects(const geom::Coordinate& p) const;

    bool isNode() const { return hpIsNode; };
    void setToNode() { hpIsNode = true; };

    std::ostream& operator<< (std::ostream& os);
};

} // namespace geos::noding::snapround
} // namespace geos::noding
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#ifdef GEOS_INLINE
# include "geos/noding/snapround/HotPixel.inl"
#endif

