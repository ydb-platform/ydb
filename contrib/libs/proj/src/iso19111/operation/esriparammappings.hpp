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

#ifndef ESRIPARAMMAPPINGS_HPP
#define ESRIPARAMMAPPINGS_HPP

#include "proj/coordinateoperation.hpp"
#include "proj/util.hpp"

#include "esriparammappings.hpp"
#include <vector>

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

struct ESRIParamMapping {
    const char *esri_name;
    const char *wkt2_name;
    int epsg_code;
    const char *fixed_value;
    bool is_fixed_value;
};

struct ESRIMethodMapping {
    const char *esri_name;
    const char *wkt2_name;
    int epsg_code;
    const ESRIParamMapping *const params;
};

extern const ESRIParamMapping paramsESRI_Plate_Carree[];
extern const ESRIParamMapping paramsESRI_Equidistant_Cylindrical[];
extern const ESRIParamMapping paramsESRI_Gauss_Kruger[];
extern const ESRIParamMapping paramsESRI_Transverse_Mercator[];
extern const ESRIParamMapping
    paramsESRI_Hotine_Oblique_Mercator_Azimuth_Natural_Origin[];
extern const ESRIParamMapping
    paramsESRI_Rectified_Skew_Orthomorphic_Natural_Origin[];
extern const ESRIParamMapping
    paramsESRI_Hotine_Oblique_Mercator_Azimuth_Center[];
extern const ESRIParamMapping paramsESRI_Rectified_Skew_Orthomorphic_Center[];

const ESRIMethodMapping *getEsriMappings(size_t &nElts);

std::vector<const ESRIMethodMapping *>
getMappingsFromESRI(const std::string &esri_name);

//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END

#endif // ESRIPARAMMAPPINGS_HPP
