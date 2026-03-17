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
 * Last port: geom/IntersectionMatrix.java rev. 1.18
 *
 **********************************************************************/

#ifndef GEOS_GEOM_INTERSECTIONMATRIX_H
#define GEOS_GEOM_INTERSECTIONMATRIX_H

#include <geos/geom/Location.h>

#include <geos/export.h>
#include <array>
#include <string>

#include <geos/inline.h>

namespace geos {
namespace geom { // geos::geom

/** \brief
 * Implementation of Dimensionally Extended Nine-Intersection Model
 * (DE-9IM) matrix.
 *
 * Dimensionally Extended Nine-Intersection Model (DE-9IM) matrix.
 * This class can used to represent both computed DE-9IM's (like 212FF1FF2)
 * as well as patterns for matching them (like T*T******).
 *
 * Methods are provided to:
 *
 *  - set and query the elements of the matrix in a convenient fashion
 *  - convert to and from the standard string representation
 *    (specified in SFS Section 2.1.13.2).
 *  - test to see if a matrix matches a given pattern string.
 *
 * For a description of the DE-9IM, see the
 * <a href="http://www.opengis.org/techno/specs.htm">OpenGIS Simple
 * Features Specification for SQL.</a>
 *
 * \todo Suggestion: add equal and not-equal operator to this class.
 */
class GEOS_DLL IntersectionMatrix {

public:

    /** \brief
     * Default constructor.
     *
     * Creates an IntersectionMatrix with Dimension::False
     * dimension values ('F').
     */
    IntersectionMatrix();

    /** \brief
     * Overriden constructor.
     *
     * Creates an IntersectionMatrix with the given dimension symbols.
     *
     * @param elements - reference to string containing pattern
     *                   of dimension values for elements.
     */
    IntersectionMatrix(const std::string& elements);

    /** \brief
     * Copy constructor.
     *
     * Creates an IntersectionMatrix with the same elements as other.
     *
     * \todo Add assignment operator to make this class fully copyable.
     */
    IntersectionMatrix(const IntersectionMatrix& other);

    /** \brief
     * Returns whether the elements of this IntersectionMatrix
     * satisfies the required dimension symbols.
     *
     * @param requiredDimensionSymbols - nine dimension symbols with
     *        which to compare the elements of this IntersectionMatrix.
     *        Possible values are {T, F, * , 0, 1, 2}.
     * @return true if this IntersectionMatrix matches the required
     *         dimension symbols.
     */
    bool matches(const std::string& requiredDimensionSymbols) const;

    /** \brief
     * Tests if given dimension value satisfies the dimension symbol.
     *
     * @param actualDimensionValue - valid dimension value stored in
     *        the IntersectionMatrix.
     *        Possible values are {TRUE, FALSE, DONTCARE, 0, 1, 2}.
     * @param requiredDimensionSymbol - a character used in the string
     *        representation of an IntersectionMatrix.
     *        Possible values are {T, F, * , 0, 1, 2}.
     * @return true if the dimension symbol encompasses the
     *         dimension value.
     */
    static bool matches(int actualDimensionValue,
                        char requiredDimensionSymbol);

    /** \brief
     * Returns true if each of the actual dimension symbols satisfies
     * the corresponding required dimension symbol.
     *
     * @param actualDimensionSymbols - nine dimension symbols to validate.
     *        Possible values are {T, F, * , 0, 1, 2}.
     * @param requiredDimensionSymbols - nine dimension symbols to
     *        validate against.
     *        Possible values are {T, F, * , 0, 1, 2}.
     * @return true if each of the required dimension symbols encompass
     *         the corresponding actual dimension symbol.
     */
    static bool matches(const std::string& actualDimensionSymbols,
                        const std::string& requiredDimensionSymbols);

    /** \brief
     * Adds one matrix to another.
     *
     * Addition is defined by taking the maximum dimension value
     * of each position in the summand matrices.
     *
     * @param other - the matrix to add.
     *
     * \todo Why the 'other' matrix is not passed by const-reference?
     */
    void add(IntersectionMatrix* other);

    /** \brief
     * Changes the value of one of this IntersectionMatrixs elements.
     *
     * @param row - the row of this IntersectionMatrix, indicating
     *        the interior, boundary or exterior of the first Geometry.
     * @param column - the column of this IntersectionMatrix,
     *        indicating the interior, boundary or exterior of the
     *        second Geometry.
     * @param dimensionValue - the new value of the element.
     */
    void set(Location row, Location column, int dimensionValue);

    /** \brief
     * Changes the elements of this IntersectionMatrix to the dimension
     * symbols in dimensionSymbols.
     *
     * @param dimensionSymbols - nine dimension symbols to which to
     *        set this IntersectionMatrix elements.
     *        Possible values are {T, F, * , 0, 1, 2}.
     */
    void set(const std::string& dimensionSymbols);

    /** \brief
     * Changes the specified element to minimumDimensionValue if the
     * element is less.
     *
     * @param row - the row of this IntersectionMatrix, indicating
     *        the interior, boundary or exterior of the first Geometry.
     * @param column -  the column of this IntersectionMatrix, indicating
     *        the interior, boundary or exterior of the second Geometry.
     * @param minimumDimensionValue - the dimension value with which
     *        to compare the element.  The order of dimension values
     *        from least to greatest is {DONTCARE, TRUE, FALSE, 0, 1, 2}.
     */
    void setAtLeast(Location row, Location column, int minimumDimensionValue);

    /** \brief
     * If row >= 0 and column >= 0, changes the specified element
     * to minimumDimensionValue if the element is less.
     * Does nothing if row <0 or column < 0.
     *
     * @param row -
     *        the row of this IntersectionMatrix,
     *        indicating the interior, boundary or exterior of the
     *        first Geometry.
     *
     * @param column -
     *        the column of this IntersectionMatrix,
     *        indicating the interior, boundary or exterior of the
     *        second Geometry.
     *
     * @param minimumDimensionValue -
     *        the dimension value with which
     *        to compare the element. The order of dimension values
     *        from least to greatest is {DONTCARE, TRUE, FALSE, 0, 1, 2}.
     */
    void setAtLeastIfValid(Location row, Location column, int minimumDimensionValue);

    /** \brief
     * For each element in this IntersectionMatrix, changes the element to
     * the corresponding minimum dimension symbol if the element is less.
     *
     * @param minimumDimensionSymbols -
     *        nine dimension symbols with which
     *        to compare the elements of this IntersectionMatrix.
     *        The order of dimension values from least to greatest is
     *        {DONTCARE, TRUE, FALSE, 0, 1, 2}  .
     */
    void setAtLeast(std::string minimumDimensionSymbols);

    /** \brief
     * Changes the elements of this IntersectionMatrix to dimensionValue.
     *
     * @param dimensionValue -
     *        the dimension value to which to set this
     *        IntersectionMatrix elements. Possible values {TRUE,
     *        FALSE, DONTCARE, 0, 1, 2}.
     */
    void setAll(int dimensionValue);

    /** \brief
     * Returns the value of one of this IntersectionMatrixs elements.
     *
     * @param row -
     *        the row of this IntersectionMatrix, indicating the
     *        interior, boundary or exterior of the first Geometry.
     *
     * @param column -
     *        the column of this IntersectionMatrix, indicating the
     *        interior, boundary or exterior of the second Geometry.
     *
     * @return the dimension value at the given matrix position.
     */
    int get(geom::Location row, geom::Location column) const {
        return matrix[static_cast<size_t>(row)][static_cast<size_t>(column)];
    }

    /** \brief
     * Returns true if this IntersectionMatrix is FF*FF****.
     *
     * @return true if the two Geometrys related by this
     *         IntersectionMatrix are disjoint.
     */
    bool isDisjoint() const;

    /** \brief
     * Returns true if isDisjoint returns false.
     *
     * @return true if the two Geometrys related by this
     *         IntersectionMatrix intersect.
     */
    bool isIntersects() const;

    /** \brief
     * Returns true if this IntersectionMatrix is FT*******, F**T*****
     * or F***T****.
     *
     * @param dimensionOfGeometryA - the dimension of the first Geometry.
     *
     * @param dimensionOfGeometryB - the dimension of the second Geometry.
     *
     * @return true if the two Geometry's related by this
     *         IntersectionMatrix touch, false if both Geometrys
     *         are points.
     */
    bool isTouches(int dimensionOfGeometryA, int dimensionOfGeometryB)
    const;

    /** \brief
     * Returns true if this IntersectionMatrix is:
     * - T*T****** (for a point and a curve, a point and an area or
     *   a line and an area)
     * - 0******** (for two curves)
     *
     * @param dimensionOfGeometryA - he dimension of the first Geometry.
     *
     * @param dimensionOfGeometryB - the dimension of the second Geometry.
     *
     * @return true if the two Geometry's related by this
     *         IntersectionMatrix cross.
     *
     * For this function to return true, the Geometrys must be a point
     * and a curve; a point and a surface; two curves; or a curve and
     * a surface.
     */
    bool isCrosses(int dimensionOfGeometryA, int dimensionOfGeometryB)
    const;

    /** \brief
     * Returns true if this IntersectionMatrix is T*F**F***.
     *
     * @return true if the first Geometry is within the second.
     */
    bool isWithin() const;

    /** \brief
     * Returns true if this IntersectionMatrix is T*****FF*.
     *
     * @return true if the first Geometry contains the second.
     */
    bool isContains() const;

    /** \brief
     * Returns true if this IntersectionMatrix is T*F**FFF*.
     *
     * @param dimensionOfGeometryA - he dimension of the first Geometry.
     * @param dimensionOfGeometryB - the dimension of the second Geometry.
     * @return true if the two Geometry's related by this
     *         IntersectionMatrix are equal; the Geometrys must have
     *         the same dimension for this function to return true
     */
    bool isEquals(int dimensionOfGeometryA, int dimensionOfGeometryB)
    const;

    /** \brief
     * Returns true if this IntersectionMatrix is:
     * - T*T***T** (for two points or two surfaces)
     * - 1*T***T** (for two curves)
     *
     * @param dimensionOfGeometryA - he dimension of the first Geometry.
     * @param dimensionOfGeometryB - the dimension of the second Geometry.
     * @return true if the two Geometry's related by this
     *         IntersectionMatrix overlap.
     *
     * For this function to return true, the Geometrys must be two points,
     * two curves or two surfaces.
     */
    bool isOverlaps(int dimensionOfGeometryA, int dimensionOfGeometryB)
    const;

    /** \brief
     * Returns true if this IntersectionMatrix is <code>T*****FF*</code>
     * or <code>*T****FF*</code> or <code>***T**FF*</code>
     * or <code>****T*FF*</code>
     *
     * @return <code>true</code> if the first Geometry covers the
     * second
     */
    bool isCovers() const;


    /** \brief
     * Returns true if this IntersectionMatrix is <code>T*F**F***</code>
     * <code>*TF**F***</code> or <code>**FT*F***</code>
     * or <code>**F*TF***</code>
     *
     * @return <code>true</code> if the first Geometry is covered by
     * the second
     */
    bool isCoveredBy() const;

    /** \brief
     * Transposes this IntersectionMatrix.
     *
     * @return this IntersectionMatrix as a convenience.
     *
     * \todo It returns 'this' pointer so why not to return const-pointer?
     * \todo May be it would be better to return copy of transposed matrix?
     */
    IntersectionMatrix* transpose();

    /** \brief
     * Returns a nine-character String representation of this
     * IntersectionMatrix.
     *
     * @return the nine dimension symbols of this IntersectionMatrix
     * in row-major order.
     */
    std::string toString() const;

private:

    static const int firstDim; // = 3;

    static const int secondDim; // = 3;

    // Internal buffer for 3x3 matrix.
    std::array<std::array<int, 3>, 3> matrix;

}; // class IntersectionMatrix

GEOS_DLL std::ostream& operator<< (std::ostream& os, const IntersectionMatrix& im);


} // namespace geos::geom
} // namespace geos

#endif // ndef GEOS_GEOM_INTERSECTIONMATRIX_H
