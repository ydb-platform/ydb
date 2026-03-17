/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: linearref/LinearIterator.java r463
 *
 **********************************************************************/

#ifndef GEOS_LINEARREF_LINEARITERATOR_H
#define GEOS_LINEARREF_LINEARITERATOR_H

#include <string>

#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/LineSegment.h>
#include <geos/linearref/LinearLocation.h>

namespace geos {
namespace linearref {

/** \brief
 * An iterator over the components and coordinates of a linear geometry
 * (LineString or MultiLineString).
 *
 * The standard usage pattern for a LinearIterator is:
 *
 * ~~~~~~
 * for (LinearIterator it = new LinearIterator(...); it.hasNext(); it.next()) {
 *   ...
 *   int ci = it.getComponentIndex();   // for example
 *   int vi = it.getVertexIndex();      // for example
 *   ...
 * }
 * ~~~~~~
 *
 * @version 1.7
 */
class LinearIterator {
public:
    /** \brief
     * Creates an iterator initialized to the start of a linear Geometry
     *
     * @param linear the linear geometry to iterate over
     */
    LinearIterator(const geom::Geometry* linear);

    /** \brief
     * Creates an iterator starting at
     * a LinearLocation on a linear geom::Geometry
     *
     * @param linear the linear geometry to iterate over
     * @param start the location to start at
     */
    LinearIterator(const geom::Geometry* linear, const LinearLocation& start);

    /** \brief
     * Creates an iterator starting at a component and vertex in a
     * linear geom::Geometry
     *
     * @param linear the linear geometry to iterate over
     * @param componentIndex the component to start at
     * @param vertexIndex the vertex to start at
     */
    LinearIterator(const geom::Geometry* linear, size_t componentIndex, size_t vertexIndex);

    /** \brief
     * Tests whether there are any vertices left to iterator over.
     * @return `true` if there are more vertices to scan
     */
    bool hasNext() const;


    /** \brief
     * Moves the iterator ahead to the next vertex and (possibly) linear component.
     */
    void next();

    /** \brief
     * Checks whether the iterator cursor is pointing to the
     * endpoint of a component geom::LineString.
     *
     * @return `true` if the iterator is at an endpoint
     */
    bool isEndOfLine() const;

    /** \brief
     * The component index of the vertex the iterator is currently at.
     * @return the current component index
     */
    size_t getComponentIndex() const;

    /** \brief
     * The vertex index of the vertex the iterator is currently at.
     * @return the current vertex index
     */
    size_t getVertexIndex() const;

    /** \brief
     * Gets the geom::LineString component the iterator is current at.
     * @return a linestring
     */
    const geom::LineString* getLine() const;

    /** \brief
     * Gets the first geom::Coordinate of the current segment.
     * (the coordinate of the current vertex).
     * @return a Coordinate
     */
    geom::Coordinate getSegmentStart() const;

    /** \brief
     * Gets the second geom::Coordinate of the current segment.
     * (the coordinate of the next vertex).
     * If the iterator is at the end of a line, `null` is returned.
     *
     * @return a Coordinate or `null`
     */
    geom::Coordinate getSegmentEnd() const;

private:

    static size_t segmentEndVertexIndex(const LinearLocation& loc);

    const geom::LineString* currentLine;
    size_t vertexIndex;
    size_t componentIndex;
    const geom::Geometry* linear;
    const size_t numLines;

    /**
     * Invariant: currentLine <> null if the iterator is pointing
     *            at a valid coordinate
     *
     * @throws IllegalArgumentException if linearGeom is not {@link Lineal}
     */
    void loadCurrentLine();

    // Declare type as noncopyable
    LinearIterator(const LinearIterator& other) = delete;
    LinearIterator& operator=(const LinearIterator& rhs) = delete;
};

}
} // namespace geos::linearref

#endif // GEOS_LINEARREF_LINEARITERATOR_H
