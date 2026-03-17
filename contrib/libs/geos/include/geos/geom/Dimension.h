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

#ifndef GEOS_GEOM_DIMENSION_H
#define GEOS_GEOM_DIMENSION_H

#include <geos/export.h>
#include <geos/inline.h>

namespace geos {
namespace geom { // geos::geom

/// Constants representing the dimensions of a point, a curve and a surface.
///
/// Also, constants representing the dimensions of the empty geometry and
/// non-empty geometries, and a wildcard dimension meaning "any dimension".
///
class GEOS_DLL Dimension {
public:
    enum DimensionType {
        /// Dimension value for any dimension (= {FALSE, TRUE}).
        DONTCARE = -3,

        /// Dimension value of non-empty geometries (= {P, L, A}).
        True = -2,

        /// Dimension value of the empty geometry (-1).
        False = -1,

        /// Dimension value of a point (0).
        P = 0,

        /// Dimension value of a curve (1).
        L = 1,

        /// Dimension value of a surface (2).
        A = 2
    };

    //static const int P = 0;			/// Dimension value of a point (0).
    //static const int L = 1;			/// Dimension value of a curve (1).
    //static const int A = 2;			/// Dimension value of a surface (2).
    //static const int False = -1;	/// Dimension value of the empty geometry (-1).
    //static const int True = -2;		/// Dimension value of non-empty geometries (= {P, L, A}).
    //static const int DONTCARE = -3;	/// Dimension value for any dimension (= {FALSE, TRUE}).
    static char toDimensionSymbol(int dimensionValue);

    static int toDimensionValue(char dimensionSymbol);

};

} // namespace geos::geom
} // namespace geos

#ifdef GEOS_INLINE
# include "geos/geom/Envelope.inl"
#endif

#endif // ndef GEOS_GEOM_DIMENSION_H
