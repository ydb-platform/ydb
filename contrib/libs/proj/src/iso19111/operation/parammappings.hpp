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

#ifndef PARAMMAPPINGS_HPP
#define PARAMMAPPINGS_HPP

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include "proj/coordinateoperation.hpp"
#include "proj/util.hpp"

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

extern const char *WKT1_LATITUDE_OF_ORIGIN;
extern const char *WKT1_CENTRAL_MERIDIAN;
extern const char *WKT1_SCALE_FACTOR;
extern const char *WKT1_FALSE_EASTING;
extern const char *WKT1_FALSE_NORTHING;
extern const char *WKT1_STANDARD_PARALLEL_1;
extern const char *WKT1_STANDARD_PARALLEL_2;
extern const char *WKT1_LATITUDE_OF_CENTER;
extern const char *WKT1_LONGITUDE_OF_CENTER;
extern const char *WKT1_AZIMUTH;
extern const char *WKT1_RECTIFIED_GRID_ANGLE;

struct ParamMapping {
    const char *wkt2_name;
    const int epsg_code;
    const char *wkt1_name;
    const common::UnitOfMeasure::Type unit_type;
    const char *proj_name;
};

struct MethodMapping {
    const char *wkt2_name;
    const int epsg_code;
    const char *wkt1_name;
    const char *proj_name_main;
    const char *proj_name_aux;
    const ParamMapping *const *params;
};

extern const ParamMapping paramLatitudeNatOrigin;

const MethodMapping *getProjectionMethodMappings(size_t &nElts);
const MethodMapping *getOtherMethodMappings(size_t &nElts);

struct MethodNameCode {
    const char *name;
    int epsg_code;
};

const MethodNameCode *getMethodNameCodes(size_t &nElts);

struct ParamNameCode {
    const char *name;
    int epsg_code;
};

const ParamNameCode *getParamNameCodes(size_t &nElts);

const MethodMapping *getMapping(int epsg_code) noexcept;
const MethodMapping *getMappingFromWKT1(const std::string &wkt1_name) noexcept;
const MethodMapping *getMapping(const char *wkt2_name) noexcept;
const MethodMapping *getMapping(const OperationMethod *method) noexcept;
std::vector<const MethodMapping *>
getMappingsFromPROJName(const std::string &projName);
const ParamMapping *getMapping(const MethodMapping *mapping,
                               const OperationParameterNNPtr &param);
const ParamMapping *getMappingFromWKT1(const MethodMapping *mapping,
                                       const std::string &wkt1_name);

//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END

#endif // PARAMMAPPINGS_HPP
