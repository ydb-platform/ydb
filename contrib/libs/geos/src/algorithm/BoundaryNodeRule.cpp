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

#include <geos/algorithm/BoundaryNodeRule.h>

namespace geos {
namespace algorithm { // geos.algorithm

// These classes are public in JTS, and under the BoundaryNodeRule
// namespace. In GEOS they are hidden and only accessible as singleton
// from BoundaryNodeRule static const references
//
namespace {

/**
 * A {@link BoundaryNodeRule} specifies that points are in the
 * boundary of a lineal geometry iff
 * the point lies on the boundary of an odd number
 * of components.
 * Under this rule {@link LinearRing}s and closed
 * {@link LineString}s have an empty boundary.
 * <p>
 * This is the rule specified by the <i>OGC SFS</i>,
 * and is the default rule used in JTS.
 *
 * @author Martin Davis
 * @version 1.7
 */
class Mod2BoundaryNodeRule : public BoundaryNodeRule {
public:
    bool
    isInBoundary(int boundaryCount) const override
    {
        // the "Mod-2 Rule"
        return boundaryCount % 2 == 1;
    }
};


/**
 * A {@link BoundaryNodeRule} which specifies that any points
 * which are endpoints
 * of lineal components are in the boundary of the
 * parent geometry.
 * This corresponds to the "intuitive" topological definition
 * of boundary.
 * Under this rule {@link LinearRing}s have a non-empty boundary
 * (the common endpoint of the underlying LineString).
 * <p>
 * This rule is useful when dealing with linear networks.
 * For example, it can be used to check
 * whether linear networks are correctly noded.
 * The usual network topology constraint is that linear segments may
 * touch only at endpoints.
 * In the case of a segment touching a closed segment (ring) at one
 * point,
 * the Mod2 rule cannot distinguish between the permitted case of
 * touching at the
 * node point and the invalid case of touching at some other interior
 * (non-node) point.
 * The EndPoint rule does distinguish between these cases,
 * so is more appropriate for use.
 *
 * @author Martin Davis
 * @version 1.7
 */
class EndPointBoundaryNodeRule : public BoundaryNodeRule {
    bool
    isInBoundary(int boundaryCount) const override
    {
        return boundaryCount > 0;
    }
};

/**
 * A {@link BoundaryNodeRule} which determines that only
 * endpoints with valency greater than 1 are on the boundary.
 * This corresponds to the boundary of a {@link MultiLineString}
 * being all the "attached" endpoints, but not
 * the "unattached" ones.
 *
 * @author Martin Davis
 * @version 1.7
 */
class MultiValentEndPointBoundaryNodeRule : public BoundaryNodeRule {
    bool
    isInBoundary(int boundaryCount) const override
    {
        return boundaryCount > 1;
    }
};

/**
 * A {@link BoundaryNodeRule} which determines that only
 * endpoints with valency of exactly 1 are on the boundary.
 * This corresponds to the boundary of a {@link MultiLineString}
 * being all the "unattached" endpoints.
 *
 * @author Martin Davis
 * @version 1.7
 */
class MonoValentEndPointBoundaryNodeRule : public BoundaryNodeRule {
    bool
    isInBoundary(int boundaryCount) const override
    {
        return boundaryCount == 1;
    }
};

Mod2BoundaryNodeRule mod2Rule;
EndPointBoundaryNodeRule endPointRule;
MultiValentEndPointBoundaryNodeRule multiValentRule;
MonoValentEndPointBoundaryNodeRule monoValentRule;

} // anonymous namespace

//const BoundaryNodeRule& BoundaryNodeRule::MOD2_BOUNDARY_RULE = mod2Rule;
const BoundaryNodeRule&
BoundaryNodeRule::getBoundaryRuleMod2()
{
    return mod2Rule;
}

//const BoundaryNodeRule& BoundaryNodeRule::ENDPOINT_BOUNDARY_RULE = endPointRule;
const BoundaryNodeRule&
BoundaryNodeRule::getBoundaryEndPoint()
{
    return endPointRule;
}

//const BoundaryNodeRule& BoundaryNodeRule::MULTIVALENT_ENDPOINT_BOUNDARY_RULE = multiValentRule;
const BoundaryNodeRule&
BoundaryNodeRule::getBoundaryMultivalentEndPoint()
{
    return multiValentRule;
}

//const BoundaryNodeRule& BoundaryNodeRule::MONOVALENT_ENDPOINT_BOUNDARY_RULE = monoValentRule;
const BoundaryNodeRule&
BoundaryNodeRule::getBoundaryMonovalentEndPoint()
{
    return monoValentRule;
}

//const BoundaryNodeRule& BoundaryNodeRule::OGC_SFS_BOUNDARY_RULE = BoundaryNodeRule::MOD2_BOUNDARY_RULE;
const BoundaryNodeRule&
BoundaryNodeRule::getBoundaryOGCSFS()
{
    return getBoundaryRuleMod2();
}

} // namespace geos.algorithm
} // namespace geos

