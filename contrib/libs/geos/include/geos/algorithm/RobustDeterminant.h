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
 * Last port: algorithm/RobustDeterminant.java 1.15 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_ALGORITHM_ROBUSTDETERMINANT_H
#define GEOS_ALGORITHM_ROBUSTDETERMINANT_H

#include <geos/export.h>

namespace geos {
namespace algorithm { // geos::algorithm

/** \brief
 * Implements an algorithm to compute the
 * sign of a 2x2 determinant for double precision values robustly.
 *
 * It is a direct translation of code developed by Olivier Devillers.
 *
 * The original code carries the following copyright notice:
 *
 * <pre>
 *************************************************************************
 * The original code carries the following copyright notice:
 * Author : Olivier Devillers
 * Olivier.Devillers@sophia.inria.fr
 * http:/www.inria.fr:/prisme/personnel/devillers/anglais/determinant.html
 *
 **************************************************************************
 *              Copyright (c) 1995  by  INRIA Prisme Project
 *                  BP 93 06902 Sophia Antipolis Cedex, France.
 *                           All rights reserved
 **********************************************************************
 * </pre>
 *
 */
class GEOS_DLL RobustDeterminant {
public:

    /** \brief
     * Computes the sign of the determinant of the 2x2 matrix
     * with the given entries, in a robust way.
     *
     * @return -1 if the determinant is negative,
     * @return  1 if the determinant is positive,
     * @return  0 if the determinant is 0.
     */
    static int signOfDet2x2(double x1, double y1, double x2, double y2);
};

} // namespace geos::algorithm
} // namespace geos



#endif // GEOS_ALGORITHM_ROBUSTDETERMINANT_H
