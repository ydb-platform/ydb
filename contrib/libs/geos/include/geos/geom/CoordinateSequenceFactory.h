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
 * Last port: geom/CoordinateSequenceFactory.java r591 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_COORDINATESEQUENCEFACTORY_H
#define GEOS_GEOM_COORDINATESEQUENCEFACTORY_H


#include <geos/export.h>
#include <memory>
#include <vector>

//#include <geos/geom/Coordinate.h>
#include <geos/inline.h>

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
class Coordinate;
}
}

namespace geos {
namespace geom { // geos::geom

/**
 * \brief
 * A factory to create concrete instances of {@link CoordinateSequence}s.
 *
 * Used to configure {@link GeometryFactory}s
 * to provide specific kinds of CoordinateSequences.
 */
class GEOS_DLL CoordinateSequenceFactory {
public:

    /** \brief
     * Returns an empty CoordinateSequence, the dimensions will be autodetected
     * when it is populated.
     */
    virtual std::unique_ptr<CoordinateSequence> create() const = 0;

    /** \brief
     * Returns a CoordinateSequence based on the given array.
     *
     * Whether the array is copied or simply referenced
     * is implementation-dependent.
     * For this reason caller does give up ownership of it.
     * Implementations that will not copy it will need take care
     * of deleting it.
     *
     * This method must handle null arguments by creating
     * an empty sequence.
     *
     * @param coordinates the coordinates
     * @param dimension 0, 2 or 3 with 0 indicating unknown at this time.
     */
    virtual std::unique_ptr<CoordinateSequence> create(
        std::vector<Coordinate>* coordinates,
        std::size_t dimension = 0) const = 0;

    /** \brief
     * Returns a CoordinateSequence based on the given array.
     *
     * @param coordinates the coordinates
     * @param dimension 0, 2 or 3 with 0 indicating unknown at this time.
     */
    virtual std::unique_ptr<CoordinateSequence> create(
            std::vector<Coordinate> && coordinates,
            std::size_t dimension = 0) const = 0;

    /** \brief
     * Creates a CoordinateSequence of the specified size and dimension.
     *
     * For this to be useful, the CoordinateSequence implementation must
     * be mutable.
     *
     * @param size the number of coordinates in the sequence
     * @param dimension the dimension of the coordinates in the sequence
     * 	(0=unknown, 2, or 3 - ignored if not user specifiable)
     */
    virtual std::unique_ptr<CoordinateSequence> create(std::size_t size,
                                                       std::size_t dimension = 0) const = 0;

    /** \brief
     * Creates a CoordinateSequence which is a copy of the given one.
     *
     * This method must handle null arguments by creating an empty sequence.
     *
     * @param coordSeq the coordinate sequence to copy
     */
    virtual std::unique_ptr<CoordinateSequence> create(const CoordinateSequence& coordSeq) const = 0;

    virtual ~CoordinateSequenceFactory() = default;
};

} // namespace geos::geom
} // namespace geos

//#ifdef GEOS_INLINE
//# include "geos/geom/CoordinateSequenceFactory.inl"
//#endif

#endif // ndef GEOS_GEOM_COORDINATESEQUENCEFACTORY_H
