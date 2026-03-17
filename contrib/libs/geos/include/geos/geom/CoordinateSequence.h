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
 **********************************************************************/

#ifndef GEOS_GEOM_COORDINATESEQUENCE_H
#define GEOS_GEOM_COORDINATESEQUENCE_H

#include <geos/export.h>
#include <geos/inline.h>

#include <geos/geom/Coordinate.h> // for applyCoordinateFilter

#include <vector>
#include <iosfwd> // ostream
#include <memory> // for unique_ptr typedef

// Forward declarations
namespace geos {
namespace geom {
class Envelope;
class CoordinateFilter;
class Coordinate;
}
}

namespace geos {
namespace geom { // geos::geom

/**
 * \class CoordinateSequence geom.h geos.h
 *
 * \brief
 * The internal representation of a list of coordinates inside a Geometry.
 *
 * There are some cases in which you might want Geometries to store their
 * points using something other than the GEOS Coordinate class. For example, you
 * may want to experiment with another implementation, such as an array of Xs
 * and an array of Ys. or you might want to use your own coordinate class, one
 * that supports extra attributes like M-values.
 *
 * You can do this by implementing the CoordinateSequence and
 * CoordinateSequenceFactory interfaces. You would then create a
 * GeometryFactory parameterized by your CoordinateSequenceFactory, and use
 * this GeometryFactory to create new Geometries. All of these new Geometries
 * will use your CoordinateSequence implementation.
 *
 */
class GEOS_DLL CoordinateSequence {

protected:

    CoordinateSequence() {}

    CoordinateSequence(const CoordinateSequence&) {}

public:

    typedef std::unique_ptr<CoordinateSequence> Ptr;

    virtual
    ~CoordinateSequence() {}

    /** \brief
     * Returns a deep copy of this collection.
     */
    virtual std::unique_ptr<CoordinateSequence> clone() const = 0;

    /** \brief
     * Returns a read-only reference to Coordinate at position i.
     *
     * Whether or not the Coordinate returned is the actual underlying
     * Coordinate or merely a copy depends on the implementation.
     */
    virtual const Coordinate& getAt(std::size_t i) const = 0;

    /// Return last Coordinate in the sequence
    const Coordinate&
    back() const
    {
        return getAt(size() - 1);
    }

    /// Return first Coordinate in the sequence
    const Coordinate&
    front() const
    {
        return getAt(0);
    }

    const Coordinate&
    operator[](std::size_t i) const
    {
        return getAt(i);
    }

    virtual Envelope getEnvelope() const;

    /** \brief
     * Write Coordinate at position i to given Coordinate.
     */
    virtual void getAt(std::size_t i, Coordinate& c) const = 0;

    /** \brief
     * Returns the number of Coordinates (actual or otherwise, as
     * this implementation may not store its data in Coordinate objects).
     */
    virtual std::size_t getSize() const = 0;

    size_t
    size() const
    {
        return getSize();
    }

    /// Pushes all Coordinates of this sequence into the provided vector.
    ///
    /// This method is a port of the toCoordinateArray() method of JTS.
    ///
    virtual void toVector(std::vector<Coordinate>& coords) const = 0;

    /// Returns <code>true</code> it list contains no coordinates.
    virtual bool isEmpty() const = 0;

    /// Copy Coordinate c to position pos
    virtual void setAt(const Coordinate& c, std::size_t pos) = 0;

    /// Get a string representation of CoordinateSequence
    std::string toString() const;

    /// Substitute Coordinate list with a copy of the given vector
    virtual void setPoints(const std::vector<Coordinate>& v) = 0;

    /// Returns true if contains any two consecutive points
    bool hasRepeatedPoints() const;

    /// Returns lower-left Coordinate in list
    const Coordinate* minCoordinate() const;

    /** \brief
     *  Returns true if given CoordinateSequence contains
     *  any two consecutive Coordinate
     */
    static bool hasRepeatedPoints(const CoordinateSequence* cl);

    /** \brief
     *  Returns either the given CoordinateSequence if its length
     *  is greater than the given amount, or an empty CoordinateSequence.
     */
    static CoordinateSequence* atLeastNCoordinatesOrNothing(std::size_t n,
            CoordinateSequence* c);

    /// Return position of a Coordinate
    //
    /// or numeric_limits<std::size_t>::max() if not found
    ///
    static size_t indexOf(const Coordinate* coordinate,
                          const CoordinateSequence* cl);

    /**
     * \brief
     * Returns true if the two arrays are identical, both null,
     * or pointwise equal
     */
    static bool equals(const CoordinateSequence* cl1,
                       const CoordinateSequence* cl2);

    /// Scroll given CoordinateSequence so to start with given Coordinate.
    static void scroll(CoordinateSequence* cl, const Coordinate* firstCoordinate);

    /** \brief
     * Determines which orientation of the {@link Coordinate} array
     * is (overall) increasing.
     *
     * In other words, determines which end of the array is "smaller"
     * (using the standard ordering on {@link Coordinate}).
     * Returns an integer indicating the increasing direction.
     * If the sequence is a palindrome, it is defined to be
     * oriented in a positive direction.
     *
     * @param pts the array of Coordinates to test
     * @return <code>1</code> if the array is smaller at the start
     * or is a palindrome,
     * <code>-1</code> if smaller at the end
     *
     * NOTE: this method is found in CoordinateArrays class for JTS
     */
    static int increasingDirection(const CoordinateSequence& pts);


    /** \brief
    * Tests whether an array of {@link Coordinate}s forms a ring,
    * by checking length and closure.
    * Self-intersection is not checked.
    *
    * @param pts an array of Coordinates
    * @return true if the coordinate form a ring.
    */
    static bool isRing(const CoordinateSequence *pts);

    /// Reverse Coordinate order in given CoordinateSequence
    static void reverse(CoordinateSequence* cl);

    /// Standard ordinate index values
    enum { X, Y, Z, M };

    /**
     * Returns the dimension (number of ordinates in each coordinate)
     * for this sequence.
     *
     * @return the dimension of the sequence.
     */
    virtual std::size_t getDimension() const = 0;

    bool hasZ() const {
        return getDimension() > 2;
    }

    /**
     * Returns the ordinate of a coordinate in this sequence.
     * Ordinate indices 0 and 1 are assumed to be X and Y.
     * Ordinates indices greater than 1 have user-defined semantics
     * (for instance, they may contain other dimensions or measure values).
     *
     * @param index  the coordinate index in the sequence
     * @param ordinateIndex the ordinate index in the coordinate
     *                      (in range [0, dimension-1])
     */
    virtual double getOrdinate(std::size_t index, std::size_t ordinateIndex) const;

    /**
     * Returns ordinate X (0) of the specified coordinate.
     *
     * @param index
     * @return the value of the X ordinate in the index'th coordinate
     */
    virtual double
    getX(std::size_t index) const
    {
        return getOrdinate(index, X);
    }

    /**
     * Returns ordinate Y (1) of the specified coordinate.
     *
     * @param index
     * @return the value of the Y ordinate in the index'th coordinate
     */
    virtual double
    getY(std::size_t index) const
    {
        return getOrdinate(index, Y);
    }


    /**
     * Sets the value for a given ordinate of a coordinate in this sequence.
     *
     * @param index  the coordinate index in the sequence
     * @param ordinateIndex the ordinate index in the coordinate
     *                      (in range [0, dimension-1])
     * @param value  the new ordinate value
     */
    virtual void setOrdinate(std::size_t index, std::size_t ordinateIndex, double value) = 0;

    /**
     * Expands the given Envelope to include the coordinates in the
     * sequence.
     * Allows implementing classes to optimize access to coordinate values.
     *
     * @param env the envelope to expand
     */
    virtual void expandEnvelope(Envelope& env) const;

    virtual void apply_rw(const CoordinateFilter* filter) = 0; //Abstract
    virtual void apply_ro(CoordinateFilter* filter) const = 0; //Abstract

    /** \brief
     * Apply a filter to each Coordinate of this sequence.
     * The filter is expected to provide a .filter(Coordinate&)
     * method.
     *
     * TODO: accept a Functor instead, will be more flexible.
     *       actually, define iterators on Geometry
     */
    template <class T>
    void
    applyCoordinateFilter(T& f)
    {
        Coordinate c;
        for(std::size_t i = 0, n = size(); i < n; ++i) {
            getAt(i, c);
            f.filter(c);
            setAt(c, i);
        }
    }

};

GEOS_DLL std::ostream& operator<< (std::ostream& os, const CoordinateSequence& cs);

GEOS_DLL bool operator== (const CoordinateSequence& s1, const CoordinateSequence& s2);

GEOS_DLL bool operator!= (const CoordinateSequence& s1, const CoordinateSequence& s2);

} // namespace geos::geom
} // namespace geos

//#ifdef GEOS_INLINE
//# include "geos/geom/CoordinateSequence.inl"
//#endif

#endif // ndef GEOS_GEOM_COORDINATESEQUENCE_H
