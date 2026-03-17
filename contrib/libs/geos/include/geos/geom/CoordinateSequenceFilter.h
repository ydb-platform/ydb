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
 **********************************************************************
 *
 * Last port: geom/CoordinateSequenceFilter.java rev. 1.3 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_COORDINATESEQUENCEFILTER_H
#define GEOS_GEOM_COORDINATESEQUENCEFILTER_H

#include <geos/export.h>
#include <geos/inline.h>

#include <cassert>

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
}
}

namespace geos {
namespace geom { // geos::geom

/** \brief
 * Interface for classes which provide operations that can be applied to
 * the coordinates in a CoordinateSequence.
 *
 * A CoordinateSequence filter can either record information about each
 * coordinate or change the coordinate in some way.
 * CoordinateSequence filters can be used to implement such things as
 * coordinate transformations, centroid and envelope computation, and many
 * other functions.
 * For maximum efficiency, the execution of filters can be short-circuited.
 * Geometry classes support the concept of applying a CoordinateSequenceFilter
 * to each CoordinateSequences they contain.
 *
 * CoordinateSequenceFilter is an example of the Gang-of-Four Visitor pattern.
 *
 * @see Geometry::apply_ro(CoordinateSequenceFilter& filter) const
 * @see Geometry::apply_rw(CoordinateSequenceFilter& filter)
 * @author Martin Davis
 *
 */
class GEOS_DLL CoordinateSequenceFilter {

public:

    virtual
    ~CoordinateSequenceFilter() {}

    /** \brief
     * Performs an operation on a coordinate in a CoordinateSequence.
     *
     * **param** `seq`  the CoordinateSequence to which the filter is applied
     * **param** `i` the index of the coordinate to apply the filter to
     */
    virtual void
    filter_rw(CoordinateSequence& /*seq*/, std::size_t /*i*/)
    {
        assert(0);
    }

    /** \brief
     * Performs an operation on a coordinate in a CoordinateSequence.
     *
     * **param** `seq`  the CoordinateSequence to which the filter is applied
     * **param** `i` the index of the coordinate to apply the filter to
     */
    virtual void
    filter_ro(const CoordinateSequence& /*seq*/, std::size_t /*i*/)
    {
        assert(0);
    }

    /** \brief
     * Reports whether the application of this filter can be terminated.
     *
     * Once this method returns `false`, it should continue to return
     * `false` on every subsequent call.
     *
     * @return `true` if the application of this filter can be terminated.
     */
    virtual bool isDone() const = 0;


    /** \brief
     * Reports whether the execution of this filter has modified
     * the coordinates of the geometry.
     *
     * If so, Geometry::geometryChanged() will be executed after this
     * filter has finished being executed.
     *
     * Most filters can simply return a constant value reflecting whether
     * they are able to change the coordinates.
     *
     * @return `true` if this filter has changed the coordinates of the geometry
     */
    virtual bool isGeometryChanged() const = 0;

};

} // namespace geos::geom
} // namespace geos


#endif // ndef GEOS_GEOM_COORDINATESEQUENCEFILTER_H

