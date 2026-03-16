/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2010 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/CoordinateList.java ?? (never been in complete sync)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_COORDINATELIST_H
#define GEOS_GEOM_COORDINATELIST_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h>

#include <list>
#include <ostream> // for operator<<
#include <memory> // for unique_ptr

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
//class Coordinate;
}
}


namespace geos {
namespace geom { // geos::geom

/** \brief
 * A list of {@link Coordinate}s, which may
 * be set to prevent repeated coordinates from occuring in the list.
 *
 * Use this class when fast insertions and removal at arbitrary
 * position is needed.
 * The class keeps ownership of the Coordinates.
 *
 */
class GEOS_DLL CoordinateList {

public:

    typedef std::list<Coordinate>::iterator iterator;
    typedef std::list<Coordinate>::const_iterator const_iterator;

    friend std::ostream& operator<< (std::ostream& os,
                                     const CoordinateList& cl);

    /** \brief
     * Constructs a new list from an array of Coordinates, allowing
     * repeated points.
     *
     * (I.e. this constructor produces a {@link CoordinateList} with
     * exactly the same set of points as the input array.)
     *
     * @param v the initial coordinates
     */
    CoordinateList(const std::vector<Coordinate>& v)
        :
        coords(v.begin(), v.end())
    {
    }

    CoordinateList()
        :
        coords()
    {
    }

    size_t
    size() const
    {
        return coords.size();
    }

    bool
    empty() const
    {
        return coords.empty();
    }

    iterator
    begin()
    {
        return coords.begin();
    }

    iterator
    end()
    {
        return coords.end();
    }

    const_iterator
    begin() const
    {
        return coords.begin();
    }

    const_iterator
    end() const
    {
        return coords.end();
    }

    /** \brief
     * Inserts the specified coordinate at the specified position in this list.
     *
     * @param pos the position at which to insert
     * @param c the coordinate to insert
     * @param allowRepeated if set to false, repeated coordinates are collapsed
     *
     * @return an iterator to the newly installed coordinate
     *         (or previous, if equal and repeated are not allowed)
     *
     * NOTE: when allowRepeated is false _next_ point is not checked
     *       this matches JTS behavior
     */
    iterator
    insert(iterator pos, const Coordinate& c, bool allowRepeated)
    {
        if(!allowRepeated && pos != coords.begin()) {
            iterator prev = pos;
            --prev;
            if(c.equals2D(*prev)) {
                return prev;
            }
        }
        return coords.insert(pos, c);
    }

    iterator
    insert(iterator pos, const Coordinate& c)
    {
        return coords.insert(pos, c);
    }

    iterator
    erase(iterator pos)
    {
        return coords.erase(pos);
    }

    iterator
    erase(iterator first, iterator last)
    {
        return coords.erase(first, last);
    }

    std::unique_ptr<Coordinate::Vect>
    toCoordinateArray() const
    {
        std::unique_ptr<Coordinate::Vect> ret(new Coordinate::Vect);
        ret->assign(coords.begin(), coords.end());
        return ret;
    }
    void
    closeRing()
    {
        if(!coords.empty() && !(*(coords.begin())).equals(*(coords.rbegin()))) {
            const Coordinate& c = *(coords.begin());
            coords.insert(coords.end(), c);
        }
    }


private:

    std::list<Coordinate> coords;
};

inline
std::ostream&
operator<< (std::ostream& os, const CoordinateList& cl)
{
    os << "(";
    for(CoordinateList::const_iterator
            it = cl.begin(), end = cl.end();
            it != end;
            ++it) {
        const Coordinate& c = *it;
        if(it != cl.begin()) {
            os << ", ";
        }
        os << c;
    }
    os << ")";

    return os;
}

} // namespace geos::geom
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ndef GEOS_GEOM_COORDINATELIST_H
