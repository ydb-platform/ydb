/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Even Rouault <even dot rouault at spatialys dot com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

#ifndef FROM_PROJ_CPP
#error This file should only be included from a PROJ cpp file
#endif

#ifndef COORDINATESYSTEM_INTERNAL_HH_INCLUDED
#define COORDINATESYSTEM_INTERNAL_HH_INCLUDED

#include "proj/util.hpp"

#include <map>
#include <set>
#include <string>

//! @cond Doxygen_Suppress

NS_PROJ_START

namespace cs {

// ---------------------------------------------------------------------------

class AxisDirectionWKT1 : public util::CodeList {
  public:
    static const AxisDirectionWKT1 *valueOf(const std::string &nameIn);

    static const AxisDirectionWKT1 NORTH;
    static const AxisDirectionWKT1 SOUTH;
    static const AxisDirectionWKT1 EAST;
    static const AxisDirectionWKT1 WEST;
    static const AxisDirectionWKT1 UP;
    static const AxisDirectionWKT1 DOWN;
    static const AxisDirectionWKT1 OTHER;

  private:
    explicit AxisDirectionWKT1(const std::string &nameIn);

    static std::map<std::string, const AxisDirectionWKT1 *> registry;
};

// ---------------------------------------------------------------------------

class AxisName {
  public:
    static const std::string Longitude;
    static const std::string Latitude;
    static const std::string Easting;
    static const std::string Northing;
    static const std::string Westing;
    static const std::string Southing;
    static const std::string Ellipsoidal_height;
    static const std::string Geocentric_X;
    static const std::string Geocentric_Y;
    static const std::string Geocentric_Z;
};

// ---------------------------------------------------------------------------

class AxisAbbreviation {
  public:
    static const std::string lon;
    static const std::string lat;
    static const std::string E;
    static const std::string N;
    static const std::string h;
    static const std::string X;
    static const std::string Y;
    static const std::string Z;
};

} // namespace cs

NS_PROJ_END

//! @endcond

#endif // COORDINATESYSTEM_INTERNAL_HH_INCLUDED
