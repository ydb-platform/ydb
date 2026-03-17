/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009-2010  Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/snap/GeometrySnapper.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/operation/overlay/snap/GeometrySnapper.h>
#include <geos/operation/overlay/snap/LineStringSnapper.h>
#include <geos/geom/util/GeometryTransformer.h> // inherit. of SnapTransformer
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/util/UniqueCoordinateArrayFilter.h>
#include <geos/util.h>

#include <vector>
#include <memory>
#include <algorithm>

//using namespace std;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay
namespace snap { // geos.operation.overlay.snap

const double GeometrySnapper::snapPrecisionFactor = 1e-9;

class SnapTransformer: public geos::geom::util::GeometryTransformer {

private:

    double snapTol;

    const Coordinate::ConstVect& snapPts;

    CoordinateSequence::Ptr
    snapLine(
        const CoordinateSequence* srcPts)
    {
        using std::unique_ptr;

        assert(srcPts);
        std::vector<Coordinate> coords;
        srcPts->toVector(coords);
        LineStringSnapper snapper(coords, snapTol);
        unique_ptr<Coordinate::Vect> newPts = snapper.snapTo(snapPts);

        const CoordinateSequenceFactory* cfact = factory->getCoordinateSequenceFactory();
        return unique_ptr<CoordinateSequence>(cfact->create(newPts.release()));
    }

public:

    SnapTransformer(double nSnapTol,
                    const Coordinate::ConstVect& nSnapPts)
        :
        snapTol(nSnapTol),
        snapPts(nSnapPts)
    {
    }

    CoordinateSequence::Ptr
    transformCoordinates(
        const CoordinateSequence* coords,
        const Geometry* parent) override
    {
        ::geos::ignore_unused_variable_warning(parent);
        return snapLine(coords);
    }


};

/*private*/
std::unique_ptr<Coordinate::ConstVect>
GeometrySnapper::extractTargetCoordinates(const Geometry& g)
{
    auto snapPts = detail::make_unique<Coordinate::ConstVect>();
    util::UniqueCoordinateArrayFilter filter(*snapPts);
    g.apply_ro(&filter);
    // integrity check
    assert(snapPts->size() <= g.getNumPoints());
    return snapPts;
}

/*public*/
std::unique_ptr<geom::Geometry>
GeometrySnapper::snapTo(const geom::Geometry& g, double snapTolerance)
{

    using std::unique_ptr;
    using geom::util::GeometryTransformer;

    // Get snap points
    unique_ptr<Coordinate::ConstVect> snapPts = extractTargetCoordinates(g);

    // Apply a SnapTransformer to source geometry
    // (we need a pointer for dynamic polymorphism)
    unique_ptr<GeometryTransformer> snapTrans(new SnapTransformer(snapTolerance, *snapPts));
    return snapTrans->transform(&srcGeom);
}

/*public*/
std::unique_ptr<geom::Geometry>
GeometrySnapper::snapToSelf(double snapTolerance, bool cleanResult)
{

    using std::unique_ptr;
    using geom::util::GeometryTransformer;

    // Get snap points
    unique_ptr<Coordinate::ConstVect> snapPts = extractTargetCoordinates(srcGeom);

    // Apply a SnapTransformer to source geometry
    // (we need a pointer for dynamic polymorphism)
    unique_ptr<GeometryTransformer> snapTrans(new SnapTransformer(snapTolerance, *snapPts));

    GeomPtr result = snapTrans->transform(&srcGeom);

    if(cleanResult && (dynamic_cast<const Polygon*>(result.get()) ||
                       dynamic_cast<const MultiPolygon*>(result.get()))) {
        // TODO: use better cleaning approach
        result = result->buffer(0);
    }

    return result;
}

/*public static*/
double
GeometrySnapper::computeSizeBasedSnapTolerance(const geom::Geometry& g)
{
    const Envelope* env = g.getEnvelopeInternal();
    double minDimension = std::min(env->getHeight(), env->getWidth());
    double snapTol = minDimension * snapPrecisionFactor;
    return snapTol;
}

/*public static*/
double
GeometrySnapper::computeOverlaySnapTolerance(const geom::Geometry& g)
{
    double snapTolerance = computeSizeBasedSnapTolerance(g);

    /*
     * Overlay is carried out in the precision model
     * of the two inputs.
     * If this precision model is of type FIXED, then the snap tolerance
     * must reflect the precision grid size.
     * Specifically, the snap tolerance should be at least
     * the distance from a corner of a precision grid cell
     * to the centre point of the cell.
     */
    assert(g.getPrecisionModel());
    const PrecisionModel& pm = *(g.getPrecisionModel());
    if(pm.getType() == PrecisionModel::FIXED) {
        double fixedSnapTol = (1 / pm.getScale()) * 2 / 1.415;
        if(fixedSnapTol > snapTolerance) {
            snapTolerance = fixedSnapTol;
        }
    }
    return snapTolerance;
}

/*public static*/
double
GeometrySnapper::computeOverlaySnapTolerance(const geom::Geometry& g1,
        const geom::Geometry& g2)
{
    return std::min(computeOverlaySnapTolerance(g1), computeOverlaySnapTolerance(g2));
}

/* public static */
void
GeometrySnapper::snap(const geom::Geometry& g0,
                      const geom::Geometry& g1,
                      double snapTolerance,
                      geom::GeomPtrPair& snapGeom)
{
    GeometrySnapper snapper0(g0);
    snapGeom.first = snapper0.snapTo(g1, snapTolerance);

    /*
     * Snap the second geometry to the snapped first geometry
     * (this strategy minimizes the number of possible different
     * points in the result)
     */
    GeometrySnapper snapper1(g1);
    snapGeom.second = snapper1.snapTo(*snapGeom.first, snapTolerance);

//	cout << *snapGeom.first << endl;
//	cout << *snapGeom.second << endl;

}

/* public static */
GeometrySnapper::GeomPtr
GeometrySnapper::snapToSelf(const geom::Geometry& g,
                            double snapTolerance,
                            bool cleanResult)
{
    GeometrySnapper snapper0(g);
    return snapper0.snapToSelf(snapTolerance, cleanResult);
}

} // namespace geos.operation.snap
} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

