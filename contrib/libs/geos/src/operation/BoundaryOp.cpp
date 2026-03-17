/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2022 ISciences LLC
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/BoundaryOp.java fd5aebb
 *
 **********************************************************************/

#include <geos/operation/BoundaryOp.h>
#include <geos/algorithm/BoundaryNodeRule.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LineString.h>
#include <geos/geom/MultiLineString.h>
#include <map>

using geos::geom::Coordinate;
using geos::geom::Dimension;
using geos::geom::Geometry;
using geos::geom::LineString;
using geos::geom::MultiLineString;
using geos::geom::Point;
using geos::algorithm::BoundaryNodeRule;

namespace geos {
namespace operation {

BoundaryOp::BoundaryOp(const Geometry& geom) :
    m_geom(geom),
    m_geomFact(*geom.getFactory()),
    m_bnRule(BoundaryNodeRule::getBoundaryRuleMod2())
{}

BoundaryOp::BoundaryOp(const geom::Geometry& geom, const algorithm::BoundaryNodeRule& bnRule) :
    m_geom(geom),
    m_geomFact(*geom.getFactory()),
    m_bnRule(bnRule)
{}

std::unique_ptr<geom::Geometry>
BoundaryOp::getBoundary()
{
    if (auto ls = dynamic_cast<const LineString*>(&m_geom)) {
        return boundaryLineString(*ls);
    }

    if (auto mls = dynamic_cast<const MultiLineString*>(&m_geom)) {
        return boundaryMultiLineString(*mls);
    }

    return m_geom.getBoundary();
}

std::unique_ptr<geom::Geometry>
BoundaryOp::getBoundary(const geom::Geometry& g)
{
    BoundaryOp bop(g);
    return bop.getBoundary();
}

std::unique_ptr<geom::Geometry>
BoundaryOp::getBoundary(const geom::Geometry& g, const algorithm::BoundaryNodeRule& bnRule)
{
    BoundaryOp bop(g, bnRule);
    return bop.getBoundary();
}

bool
BoundaryOp::hasBoundary(const geom::Geometry& geom, const algorithm::BoundaryNodeRule& boundaryNodeRule)
{
    // Note that this does not handle geometry collections with a non-empty linear element
    if (geom.isEmpty()) {
        return false;
    }

    switch (geom.getDimension()) {
        case Dimension::P: return false;
        /**
         * Linear geometries might have an empty boundary due to boundary node rule.
         */
        case Dimension::L:
            {

            auto boundary = getBoundary(geom, boundaryNodeRule);
            return !boundary->isEmpty();
            }
        default:
            return true;
        }
}

std::unique_ptr<Geometry>
BoundaryOp::boundaryLineString(const geom::LineString& line)
{
    if (m_geom.isEmpty()) {
        return m_geomFact.createMultiPoint();
    }

    if (line.isClosed()) {
        // check whether endpoints of valence 2 are on the boundary or not
        bool closedEndpointOnBoundary = m_bnRule.isInBoundary(2);
        if (closedEndpointOnBoundary) {
            return line.getStartPoint();
        }
        else {
            return m_geomFact.createMultiPoint();
        }
    }

    std::vector<std::unique_ptr<Point>> pts(2);
    pts[0] = line.getStartPoint();
    pts[1] = line.getEndPoint();

    return m_geomFact.createMultiPoint(std::move(pts));
}

std::unique_ptr<Geometry>
BoundaryOp::boundaryMultiLineString(const geom::MultiLineString& mLine)
{
    if (m_geom.isEmpty()) {
        return m_geomFact.createMultiPoint();
    }

    auto bdyPts = computeBoundaryCoordinates(mLine);

    // return Point or MultiPoint
    if (bdyPts.size() == 1) {
        return std::unique_ptr<Geometry>(m_geomFact.createPoint(bdyPts[0]));
    }
    // this handles 0 points case as well
    return m_geomFact.createMultiPoint(std::move(bdyPts));
}

std::vector<geom::Coordinate>
BoundaryOp::computeBoundaryCoordinates(const geom::MultiLineString& mLine)
{
    std::vector<Coordinate> bdyPts;
    std::map<Coordinate, int> endpointMap;

    for (std::size_t i = 0; i < mLine.getNumGeometries(); i++) {
      const LineString* line = mLine.getGeometryN(i);

      if (line->getNumPoints() == 0) {
        continue;
      }

      endpointMap[line->getCoordinateN(0)]++;
      endpointMap[line->getCoordinateN(line->getNumPoints() - 1)]++;
    }

    for (const auto& entry: endpointMap) {
        auto valence = entry.second;
        if (m_bnRule.isInBoundary(valence)) {
            bdyPts.push_back(entry.first);
        }
    }

    return bdyPts;
}


}
}
