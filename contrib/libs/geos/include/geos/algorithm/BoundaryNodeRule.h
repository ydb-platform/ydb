/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2009      Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/BoundaryNodeRule.java rev 1.4 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_BOUNDARYNODERULE_H
#define GEOS_ALGORITHM_BOUNDARYNODERULE_H

#include <geos/export.h>

// Forward declarations
// ...

namespace geos {
namespace algorithm { // geos::algorithm


/** \brief
 * An interface for rules which determine whether node points which are
 * in boundaries of lineal geometry components
 * are in the boundary of the parent geometry collection.
 *
 * The SFS specifies a single kind of boundary node rule,
 * the `Mod2BoundaryNodeRule` rule.
 * However, other kinds of Boundary Node Rules are appropriate
 * in specific situations (for instance, linear network topology
 * usually follows the `EndPointBoundaryNodeRule`.)
 * Some JTS operations allow the BoundaryNodeRule to be specified,
 * and respect this rule when computing the results of the operation.
 *
 * @author Martin Davis
 * @version 1.7
 *
 * @see operation::relate::RelateOp
 * @see operation::IsSimpleOp
 * @see algorithm::PointLocator
 */
class GEOS_DLL BoundaryNodeRule {

public:

    // virtual classes should always have a virtual destructor..
    virtual
    ~BoundaryNodeRule() {}

    /** \brief
     * Tests whether a point that lies in `boundaryCount`
     * geometry component boundaries is considered to form part of
     * the boundary of the parent geometry.
     *
     * @param boundaryCount the number of component boundaries that
     *                      this point occurs in
     * @return `true` if points in this number of boundaries lie in
     *         the parent boundary
     */
    virtual bool isInBoundary(int boundaryCount) const = 0;

    /** \brief
     * The Mod-2 Boundary Node Rule (which is the rule specified
     * in the OGC SFS).
     *
     * A BoundaryNodeRule specifies that points are in the boundary of
     * a lineal geometry iff the point lies on the boundary of an odd number
     * of components. Under this rule LinearRings and closed LineStrings have
     * an empty boundary.
     *
     * This is the rule specified by the OGC SFS, and is the default
     * rule used in JTS.
     */
    //static const BoundaryNodeRule& MOD2_BOUNDARY_RULE;
    static const BoundaryNodeRule& getBoundaryRuleMod2();

    /** \brief
     * The Endpoint Boundary Node Rule.
     *
     * A BoundaryNodeRule which specifies that any points which are endpoints
     * of lineal components are in the boundary of the parent geometry. This
     * corresponds to the "intuitive" topological definition of boundary. Under
     * this rule LinearRings have a non-empty boundary (the common endpoint
     * of the underlying LineString).
     *
     * This rule is useful when dealing with linear networks. For example,
     * it can be used to check whether linear networks are correctly noded.
     * The usual network topology constraint is that linear segments may
     * touch only at endpoints. In the case of a segment touching a closed
     * segment (ring) at one point, the Mod2 rule cannot distinguish between
     * the permitted case of touching at the node point and the invalid case
     * of touching at some other interior (non-node) point. The EndPoint rule
     * does distinguish between these cases, so is more appropriate for use.
     */
    //static const BoundaryNodeRule& ENDPOINT_BOUNDARY_RULE;
    static const BoundaryNodeRule& getBoundaryEndPoint();

    /** \brief
     * The MultiValent Endpoint Boundary Node Rule.
     *
     * A BoundaryNodeRule which determines that only endpoints with valency
     * greater than 1 are on the boundary. This corresponds to the boundary
     * of a MultiLineString being all the "attached" endpoints, but not
     * the "unattached" ones.
     */
    //static const BoundaryNodeRule& MULTIVALENT_ENDPOINT_BOUNDARY_RULE;
    static const BoundaryNodeRule& getBoundaryMultivalentEndPoint();

    /** \brief
     * The Monovalent Endpoint Boundary Node Rule.
     *
     * A BoundaryNodeRule which determines that only endpoints with valency of
     * exactly 1 are on the boundary. This corresponds to the boundary of
     * a MultiLineString being all the "unattached" endpoints.
     */
    //static const BoundaryNodeRule& MONOVALENT_ENDPOINT_BOUNDARY_RULE;
    static const BoundaryNodeRule& getBoundaryMonovalentEndPoint();

    /** \brief
     * The Boundary Node Rule specified by the OGC Simple Features
     * Specification, which is the same as the Mod-2 rule.
     *
     * A BoundaryNodeRule which determines that only endpoints with valency
     * greater than 1 are on the boundary. This corresponds to the boundary
     * of a MultiLineString being all the "attached" endpoints, but not the
     * "unattached" ones.
     */
    //static const BoundaryNodeRule& OGC_SFS_BOUNDARY_RULE;
    static const BoundaryNodeRule& getBoundaryOGCSFS();
};

} // namespace geos::algorithm
} // namespace geos

#endif // GEOS_ALGORITHM_BOUNDARYNODERULE_H

