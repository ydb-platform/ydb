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

#include <string>

#include <geos/util/Assert.h>
#include <geos/util/AssertionFailedException.h>
#include <geos/geom/Coordinate.h>

using std::string;
using namespace geos::geom;

namespace geos {
namespace util { // geos.util

void
Assert::isTrue(bool assertion, const string& message)
{
    if(!assertion) {
        if(message.empty()) {
            throw  AssertionFailedException();
        }
        else {
            throw  AssertionFailedException(message);
        }
    }
}

void
Assert::equals(const Coordinate& expectedValue,
               const Coordinate& actualValue, const string& message)
{
    if(!(actualValue == expectedValue)) {
        throw  AssertionFailedException("Expected " + expectedValue.toString() + " but encountered "
                                        + actualValue.toString() + (!message.empty() ? ": " + message : ""));
    }
}


void
Assert::shouldNeverReachHere(const string& message)
{
    throw  AssertionFailedException("Should never reach here"
                                    + (!message.empty() ? ": " + message : ""));
}

} // namespace geos.util
} // namespace geos
