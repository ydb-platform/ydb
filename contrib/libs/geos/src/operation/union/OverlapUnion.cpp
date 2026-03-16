/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/operation/union/OverlapUnion.h>

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequenceFilter.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/util/GeometryCombiner.h>
#include <geos/util/TopologyException.h>

namespace geos {
namespace operation {
namespace geounion {

// https://github.com/locationtech/jts/blob/master/modules/core/src/main/java/org/locationtech/jts/operation/union/OverlapUnion.java

using namespace geom;
using namespace geom::util;

/* public */
std::unique_ptr<Geometry>
OverlapUnion::doUnion()
{
    Envelope overlapEnv = overlapEnvelope(g0, g1);
    /*
     * If no overlap, can just combine the geometries
     */
    if (overlapEnv.isNull()) {
        // Geometry* g0Copy = g0->clone().get();
        // Geometry* g1Copy = g1->clone().get();
        return GeometryCombiner::combine(g0, g1);
    }

    std::vector<std::unique_ptr<Geometry>> disjointPolys;

    std::unique_ptr<Geometry> g0Overlap = extractByEnvelope(overlapEnv, g0, disjointPolys);
    std::unique_ptr<Geometry> g1Overlap = extractByEnvelope(overlapEnv, g1, disjointPolys);

    // std::out << "# geoms in common: " << intersectingPolys.size() << std::endl;
    std::unique_ptr<Geometry> theUnion(unionFull(g0Overlap.get(), g1Overlap.get()));
    isUnionSafe = isBorderSegmentsSame(theUnion.get(), overlapEnv);
    if (!isUnionSafe) {
        // overlap union changed border segments... need to do full union
        // std::out <<  "OverlapUnion: Falling back to full union" << std::endl;
        return unionFull(g0, g1);
    }
    else {
        // std::out << "OverlapUnion: fast path" << std::endl;
        return combine(theUnion, disjointPolys);
    }
}

/* private */
Envelope
OverlapUnion::overlapEnvelope(const Geometry* geom0, const Geometry* geom1)
{
    const Envelope* g0Env = geom0->getEnvelopeInternal();
    const Envelope* g1Env = geom1->getEnvelopeInternal();
    Envelope overlapEnv;
    g0Env->intersection(*g1Env, overlapEnv);
    return overlapEnv;
}

/* private */
std::unique_ptr<Geometry>
OverlapUnion::combine(std::unique_ptr<Geometry>& unionGeom, std::vector<std::unique_ptr<Geometry>>& disjointPolys)
{
    if (disjointPolys.size() <= 0)
        return std::move(unionGeom);

    disjointPolys.push_back(std::move(unionGeom));
    return GeometryCombiner::combine(disjointPolys);
}

/* private */
std::unique_ptr<Geometry>
OverlapUnion::extractByEnvelope(const Envelope& env, const Geometry* geom, std::vector<std::unique_ptr<Geometry>>& disjointGeoms)
{
    std::vector<const Geometry*> intersectingGeoms;
    for (std::size_t i = 0; i < geom->getNumGeometries(); i++) {
        const Geometry* elem = geom->getGeometryN(i);
        if (elem->getEnvelopeInternal()->intersects(env)) {
            intersectingGeoms.push_back(elem);
        }
        else {
            disjointGeoms.push_back(elem->clone());
        }
    }
    return std::unique_ptr<Geometry>(geomFactory->buildGeometry(intersectingGeoms));
}

/* private */
std::unique_ptr<Geometry>
OverlapUnion::unionFull(const Geometry* geom0, const Geometry* geom1)
{
    // try {
    //     return geom0->Union(geom1);
    // }
    // catch (geos::util::TopologyException &) {
    //      // If the overlay union fails,
    //      // try a buffer union, which often succeeds
    //     return unionBuffer(geom0, geom1);
    // }
    if (geom0->getNumGeometries() == 0 && geom1->getNumGeometries() == 0) {
        return geom0->clone();
    }

    return unionFunction->Union(geom0, geom1);
}

/* private */
std::unique_ptr<Geometry>
OverlapUnion::unionBuffer(const Geometry* geom0, const Geometry* geom1)
{
    const GeometryFactory* factory = geom0->getFactory();
    std::unique_ptr<Geometry> copy0 = geom0->clone();
    std::unique_ptr<Geometry> copy1 = geom1->clone();
    std::vector<std::unique_ptr<Geometry>> geoms;
    geoms.push_back(std::move(copy0));
    geoms.push_back(std::move(copy1));
    std::unique_ptr<GeometryCollection> gColl(factory->createGeometryCollection(std::move(geoms)));
    return gColl->buffer(0.0);
}

/* private */
bool
OverlapUnion::isBorderSegmentsSame(const Geometry* result, const Envelope& env)
{
    std::vector<LineSegment> segsBefore = extractBorderSegments(g0, g1, env);
    std::vector<LineSegment> segsAfter;
    extractBorderSegments(result, env, segsAfter);
    bool eq = isEqual(segsBefore, segsAfter);

    return eq;
}

static bool lineSegmentPtrCmp(const LineSegment& a, const LineSegment& b)
{
    return a.compareTo(b) < 0;
}

/* private */
bool
OverlapUnion::isEqual(std::vector<LineSegment>& segs0, std::vector<LineSegment>& segs1)
{
    if (segs0.size() != segs1.size())
        return false;

    std::sort(segs0.begin(), segs0.end(), lineSegmentPtrCmp);
    std::sort(segs1.begin(), segs1.end(), lineSegmentPtrCmp);

    size_t sz = segs0.size();
    for (std::size_t i = 0; i < sz; i++) {
        if (segs0[i].p0.x != segs1[i].p0.x ||
            segs0[i].p0.y != segs1[i].p0.y ||
            segs0[i].p1.x != segs1[i].p1.x ||
            segs0[i].p1.y != segs1[i].p1.y)
        {
            return false;
        }
    }

    return true;
}

/* private */
std::vector<LineSegment>
OverlapUnion::extractBorderSegments(const Geometry* geom0, const Geometry* geom1, const Envelope& env)
{
    std::vector<LineSegment> segs;
    extractBorderSegments(geom0, env, segs);
    if (geom1 != nullptr)
        extractBorderSegments(geom1, env, segs);
    return segs;
}

/* static */
static bool
intersects(const Envelope& env, const Coordinate& p0, const Coordinate& p1)
{
    return env.intersects(p0) || env.intersects(p1);
}

/* static */
static bool
containsProperly(const Envelope& env, const Coordinate& p)
{
    if (env.isNull()) return false;
    return p.x > env.getMinX() &&
           p.x < env.getMaxX() &&
           p.y > env.getMinY() &&
           p.y < env.getMaxY();
}

/* static */
static bool
containsProperly(const Envelope& env, const Coordinate& p0, const Coordinate& p1)
{
    return containsProperly(env, p0) && containsProperly(env, p1);
}

/* privatef */
void
OverlapUnion::extractBorderSegments(const Geometry* geom, const Envelope& penv, std::vector<LineSegment>& psegs)
{
    class BorderSegmentFilter : public CoordinateSequenceFilter {

    private:
        const Envelope env;
        std::vector<LineSegment>* segs;

    public:

        BorderSegmentFilter(const Envelope& penv, std::vector<LineSegment>* psegs)
            : env(penv),
              segs(psegs) {};

        bool
        isDone() const override { return false; }

        bool
        isGeometryChanged() const override  { return false; }

        void
        filter_ro(const CoordinateSequence& seq, std::size_t i) override
        {
            if (i <= 0) return;

            // extract LineSegment
            const Coordinate& p0 = seq.getAt(i-1);
            const Coordinate& p1 = seq.getAt(i  );
            bool isBorder = intersects(env, p0, p1) && ! containsProperly(env, p0, p1);
            if (isBorder) {
                segs->emplace_back(p0, p1);
            }
        };

    };

    BorderSegmentFilter bsf(penv, &psegs);
    geom->apply_ro(bsf);

}



}
}
}
