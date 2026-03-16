/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/geom/Dimension.h>
#include <geos/util/IllegalArgumentException.h>

#include <sstream>

using namespace std;

namespace geos {
namespace geom { // geos::geom

/**
 *  Converts the dimension value to a dimension symbol, for example, <code>TRUE => 'T'</code>.
 *
 *@param  dimensionValue  a number that can be stored in the <code>IntersectionMatrix</code>.
 *			Possible values are <code>{TRUE, FALSE, DONTCARE, 0, 1, 2}</code>.
 *@return   a character for use in the string representation of
 *      an <code>IntersectionMatrix</code>. Possible values are <code>{T, F, * , 0, 1, 2}</code>.
 */
char
Dimension::toDimensionSymbol(int dimensionValue)
{
    switch(dimensionValue) {
    case False:
        return 'F';
    case True:
        return 'T';
    case DONTCARE:
        return '*';
    case P:
        return '0';
    case L:
        return '1';
    case A:
        return '2';
    default:
        ostringstream s;
        s << "Unknown dimension value: " << dimensionValue << endl;
        throw  util::IllegalArgumentException(s.str());
    }
}

/**
 *  Converts the dimension symbol to a dimension value, for example, <code>'*' => DONTCARE</code>.
 *
 *@param  dimensionSymbol  a character for use in the string representation of
 *      an <code>IntersectionMatrix</code>. Possible values are <code>{T, F, * , 0, 1, 2}</code>.
 *@return       a number that can be stored in the <code>IntersectionMatrix</code>.
 *				Possible values are <code>{TRUE, FALSE, DONTCARE, 0, 1, 2}</code>.
 */
int
Dimension::toDimensionValue(char dimensionSymbol)
{
    switch(dimensionSymbol) {
    case 'F':
    case 'f':
        return False;
    case 'T':
    case 't':
        return True;
    case '*':
        return DONTCARE;
    case '0':
        return P;
    case '1':
        return L;
    case '2':
        return A;
    default:
        ostringstream s;
        s << "Unknown dimension symbol: " << dimensionSymbol << endl;
        throw  util::IllegalArgumentException(s.str());
    }
}

} // namespace geos::geom
} // namespace geos
