/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/relate/RelateOp.java rev. 1.19 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_RELATE_RELATEOP_H
#define GEOS_OP_RELATE_RELATEOP_H

#include <geos/export.h>

#include <geos/geom/IntersectionMatrix.h>
#include <geos/operation/GeometryGraphOperation.h> // for inheritance
#include <geos/operation/relate/RelateComputer.h> // for composition

// Forward declarations
namespace geos {
namespace algorithm {
class BoundaryNodeRule;
}
namespace geom {
class Geometry;
}
}


namespace geos {
namespace operation { // geos::operation
namespace relate { // geos::operation::relate

/** \brief
 * Implements the SFS `relate()` operation on two
 * geom::Geometry objects.
 *
 * This class supports specifying a custom algorithm::BoundaryNodeRule
 * to be used during the relate computation.
 *
 * @note Custom Boundary Node Rules do not (currently)
 * affect the results of other Geometry methods (such
 * as [Geometry::getBoundary()](@ref geom::Geometry::getBoundary() const)).
 * The results of these methods may not be consistent with the relationship
 * computed by a custom Boundary Node Rule.
 *
 */
class GEOS_DLL RelateOp: public GeometryGraphOperation {

public:

    /** \brief
     * Computes the geom::IntersectionMatrix for the spatial relationship
     * between two geom::Geometry objects, using the default (OGC SFS)
     * Boundary Node Rule
     *
     * @param a a Geometry to test. Ownership left to caller.
     * @param b a Geometry to test. Ownership left to caller.
     *
     * @return the IntersectonMatrix for the spatial relationship
     *         between the geometries. Ownership transferred.
     */
    static std::unique_ptr<geom::IntersectionMatrix> relate(
        const geom::Geometry* a,
        const geom::Geometry* b);

    /** \brief
     * Computes the geom::IntersectionMatrix for the spatial relationship
     * between two geom::Geometry objects, using a specified
     * Boundary Node Rule.
     *
     * @param a a Geometry to test. Ownership left to caller.
     * @param b a Geometry to test. Ownership left to caller.
     * @param boundaryNodeRule the Boundary Node Rule to use.
     *
     * @return the IntersectonMatrix for the spatial relationship
     *         between the geometries. Ownership transferred.
     */
    static std::unique_ptr<geom::IntersectionMatrix> relate(
        const geom::Geometry* a,
        const geom::Geometry* b,
        const algorithm::BoundaryNodeRule& boundaryNodeRule);

    /** \brief
     * Creates a new Relate operation, using the default (OGC SFS)
     * Boundary Node Rule.
     *
     * @param g0 a Geometry to relate. Ownership left to caller.
     * @param g1 another Geometry to relate. Ownership to caller.
     */
    RelateOp(const geom::Geometry* g0,
             const geom::Geometry* g1);

    /** \brief
     * Creates a new Relate operation with a specified
     * Boundary Node Rule.
     *
     * @param g0 a Geometry to relate. Ownership left to caller.
     * @param g1 another Geometry to relate. Ownership to caller.
     * @param boundaryNodeRule the Boundary Node Rule to use
     */
    RelateOp(const geom::Geometry* g0,
             const geom::Geometry* g1,
             const algorithm::BoundaryNodeRule& boundaryNodeRule);

    ~RelateOp() override = default;

    /** \brief
     * Gets the IntersectionMatrix for the spatial relationship
     * between the input geometries.
     *
     * @return the geom::IntersectionMatrix for the spatial
     *         relationship between the input geometries.
     *         Ownership transferred.
     */
    std::unique_ptr<geom::IntersectionMatrix> getIntersectionMatrix();

private:

    RelateComputer relateComp;
};


} // namespace geos:operation:relate
} // namespace geos:operation
} // namespace geos

#endif // GEOS_OP_RELATE_RELATEOP_H
