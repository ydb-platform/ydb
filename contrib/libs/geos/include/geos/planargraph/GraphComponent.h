/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: planargraph/GraphComponent.java rev. 1.7 (JTS-1.7)
 *
 **********************************************************************/

#ifndef GEOS_PLANARGRAPH_GRAPHCOMPONENT_H
#define GEOS_PLANARGRAPH_GRAPHCOMPONENT_H

#include <geos/export.h>

namespace geos {
namespace planargraph { // geos.planargraph

/**
 * \brief The base class for all graph component classes.
 *
 * Maintains flags of use in generic graph algorithms.
 * Provides two flags:
 *
 *  - <b>marked</b> - typically this is used to indicate a state that
 *    persists for the course of the graph's lifetime.  For instance,
 *    it can be used to indicate that a component has been logically
 *    deleted from the graph.
 *  - <b>visited</b> - this is used to indicate that a component has been
 *    processed or visited by an single graph algorithm.  For instance,
 *    a breadth-first traversal of the graph might use this to indicate
 *    that a node has already been traversed.
 *    The visited flag may be set and cleared many times during the
 *    lifetime of a graph.
 *
 */
class GEOS_DLL GraphComponent {

protected:

    /// Variable holding ''marked'' status
    bool isMarkedVar;

    /// Variable holding ''visited'' status
    bool isVisitedVar;

public:

    GraphComponent()
        :
        isMarkedVar(false),
        isVisitedVar(false)
    {}

    virtual
    ~GraphComponent() {}

    /** \brief
     * Tests if a component has been visited during the course
     * of a graph algorithm.
     *
     * @return <code>true</code> if the component has been visited
     */
    virtual bool
    isVisited() const
    {
        return isVisitedVar;
    }

    /** \brief
     * Sets the visited flag for this component.
     * @param p_isVisited the desired value of the visited flag
     */
    virtual void
    setVisited(bool p_isVisited)
    {
        isVisitedVar = p_isVisited;
    }

    /** \brief
     * Sets the Visited state for the elements of a container,
     * from start to end iterator.
     *
     * @param start the start element
     * @param end one past the last element
     * @param visited the state to set the visited flag to
     */
    template <typename T>
    static void
    setVisited(T start, T end, bool visited)
    {
        for(T i = start; i != end; ++i) {
            (*i)->setVisited(visited);
        }
    }

    /** \brief
     * Sets the Visited state for the values of each map
     * container element, from start to end iterator.
     *
     * @param start the start element
     * @param end one past the last element
     * @param visited the state to set the visited flag to
     */
    template <typename T>
    static void
    setVisitedMap(T start, T end, bool visited)
    {
        for(T i = start; i != end; ++i) {
            i->second->setVisited(visited);
        }
    }

    /** \brief
     * Sets the Marked state for the elements of a container,
     * from start to end iterator.
     *
     * @param start the start element
     * @param end one past the last element
     * @param marked the state to set the marked flag to
     */
    template <typename T>
    static void
    setMarked(T start, T end, bool marked)
    {
        for(T i = start; i != end; ++i) {
            (*i)->setMarked(marked);
        }
    }


    /** \brief
     * Sets the Marked state for the values of each map
     * container element, from start to end iterator.
     *
     * @param start the start element
     * @param end one past the last element
     * @param marked the state to set the visited flag to
     */
    template <typename T>
    static void
    setMarkedMap(T start, T end, bool marked)
    {
        for(T i = start; i != end; ++i) {
            i->second->setMarked(marked);
        }
    }

    /** \brief
     * Tests if a component has been marked at some point
     * during the processing involving this graph.
     * @return <code>true</code> if the component has been marked
     */
    virtual bool
    isMarked() const
    {
        return isMarkedVar;
    }

    /** \brief
     * Sets the marked flag for this component.
     * @param p_isMarked the desired value of the marked flag
     */
    virtual void
    setMarked(bool p_isMarked)
    {
        isMarkedVar = p_isMarked;
    }

};

// For backward compatibility
//typedef GraphComponent planarGraphComponent;

} // namespace geos::planargraph
} // namespace geos

#endif // GEOS_PLANARGRAPH_GRAPHCOMPONENT_H
