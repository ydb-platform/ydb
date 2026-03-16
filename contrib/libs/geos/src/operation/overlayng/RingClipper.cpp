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

#include <geos/operation/overlayng/RingClipper.h>


namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


/*public*/
std::unique_ptr<CoordinateArraySequence>
RingClipper::clip(const CoordinateSequence* cs) const
{
    std::unique_ptr<CoordinateArraySequence> pts;
    for (int edgeIndex = 0; edgeIndex < 4; edgeIndex++) {
        bool closeRing = (edgeIndex == 3);
        pts = clipToBoxEdge(cs, edgeIndex, closeRing);
        if (pts->size() == 0)
            return pts;
        cs = pts.get();
    }
    return pts;
}

/*private*/
std::unique_ptr<CoordinateArraySequence>
RingClipper::clipToBoxEdge(const CoordinateSequence* pts, int edgeIndex, bool closeRing) const
{
    // TODO: is it possible to avoid copying array 4 times?
    std::unique_ptr<CoordinateArraySequence> ptsClip(new CoordinateArraySequence());

    Coordinate p0;
    pts->getAt(pts->size() - 1, p0);
    for (std::size_t i = 0; i < pts->size(); i++) {
        Coordinate p1;
        pts->getAt(i, p1);
        if (isInsideEdge(p1, edgeIndex)) {
            if (!isInsideEdge(p0, edgeIndex)) {
                Coordinate intPt;
                intersection(p0, p1, edgeIndex, intPt);
                ptsClip->add(intPt, false);
            }
            // TODO: avoid copying so much?
            ptsClip->add(p1, false);

        }
        else if (isInsideEdge(p0, edgeIndex)) {
            Coordinate intPt;
            intersection(p0, p1, edgeIndex, intPt);
            ptsClip->add(intPt, false);
        }

        // else p0-p1 is outside box, so it is dropped
        p0 = p1;
    }

    // add closing point if required
    if (closeRing && ptsClip->size() > 0) {
        const Coordinate& start = ptsClip->getAt(0);
        if (!start.equals2D(ptsClip->getAt(ptsClip->size() - 1))) {
            ptsClip->add(start);
        }
    }
    return ptsClip;
}

/*private*/
void
RingClipper::intersection(const Coordinate& a, const Coordinate& b, int edgeIndex, Coordinate& rsltPt) const
{
    switch (edgeIndex) {
    case BOX_BOTTOM:
        rsltPt = Coordinate(intersectionLineY(a, b, clipEnvMinY), clipEnvMinY);
        break;
    case BOX_RIGHT:
        rsltPt = Coordinate(clipEnvMaxX, intersectionLineX(a, b, clipEnvMaxX));
        break;
    case BOX_TOP:
        rsltPt = Coordinate(intersectionLineY(a, b, clipEnvMaxY), clipEnvMaxY);
        break;
    case BOX_LEFT:
    default:
        rsltPt = Coordinate(clipEnvMinX, intersectionLineX(a, b, clipEnvMinX));
    }
    return;
}

/*private*/
double
RingClipper::intersectionLineY(const Coordinate& a, const Coordinate& b, double y) const
{
    double m = (b.x - a.x) / (b.y - a.y);
    double intercept = (y - a.y) * m;
    return a.x + intercept;
}

/*private*/
double
RingClipper::intersectionLineX(const Coordinate& a, const Coordinate& b, double x) const
{
    double m = (b.y - a.y) / (b.x - a.x);
    double intercept = (x - a.x) * m;
    return a.y + intercept;
}

/*private*/
bool
RingClipper::isInsideEdge(const Coordinate& p, int edgeIndex) const
{
    bool isInside = false;
    switch (edgeIndex) {
    case BOX_BOTTOM: // bottom
        isInside = p.y > clipEnvMinY;
        break;
    case BOX_RIGHT: // right
        isInside = p.x < clipEnvMaxX;
        break;
    case BOX_TOP: // top
        isInside = p.y < clipEnvMaxY;
        break;
    case BOX_LEFT:
    default: // left
        isInside = p.x > clipEnvMinX;
    }
    return isInside;
}



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
