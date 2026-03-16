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

#ifndef OPUTILS_HPP
#define OPUTILS_HPP

#include "proj/coordinateoperation.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

extern const common::Measure nullMeasure;

extern const std::string INVERSE_OF;

extern const char *BALLPARK_GEOCENTRIC_TRANSLATION;
extern const char *NULL_GEOGRAPHIC_OFFSET;
extern const char *NULL_GEOCENTRIC_TRANSLATION;
extern const char *BALLPARK_GEOGRAPHIC_OFFSET;
extern const char *BALLPARK_VERTICAL_TRANSFORMATION;
extern const char *BALLPARK_VERTICAL_TRANSFORMATION_NO_ELLIPSOID_VERT_HEIGHT;

extern const std::string AXIS_ORDER_CHANGE_2D_NAME;
extern const std::string AXIS_ORDER_CHANGE_3D_NAME;

OperationParameterNNPtr createOpParamNameEPSGCode(int code);

util::PropertyMap createMethodMapNameEPSGCode(int code);

util::PropertyMap createMapNameEPSGCode(const std::string &name, int code);

util::PropertyMap createMapNameEPSGCode(const char *name, int code);

util::PropertyMap &addDomains(util::PropertyMap &map,
                              const common::ObjectUsage *obj);

std::string buildOpName(const char *opType, const crs::CRSPtr &source,
                        const crs::CRSPtr &target);

void addModifiedIdentifier(util::PropertyMap &map,
                           const common::IdentifiedObject *obj, bool inverse,
                           bool derivedFrom);
util::PropertyMap
createPropertiesForInverse(const OperationMethodNNPtr &method);

util::PropertyMap createPropertiesForInverse(const CoordinateOperation *op,
                                             bool derivedFrom,
                                             bool approximateInversion);

util::PropertyMap addDefaultNameIfNeeded(const util::PropertyMap &properties,
                                         const std::string &defaultName);

bool areEquivalentParameters(const std::string &a, const std::string &b);

bool isTimeDependent(const std::string &methodName);

std::string computeConcatenatedName(
    const std::vector<CoordinateOperationNNPtr> &flattenOps);

metadata::ExtentPtr getExtent(const std::vector<CoordinateOperationNNPtr> &ops,
                              bool conversionExtentIsWorld,
                              bool &emptyIntersection);

metadata::ExtentPtr getExtent(const CoordinateOperationNNPtr &op,
                              bool conversionExtentIsWorld,
                              bool &emptyIntersection);

const metadata::ExtentPtr &getExtent(const crs::CRSNNPtr &crs);

const metadata::ExtentPtr getExtentPossiblySynthetized(const crs::CRSNNPtr &crs,
                                                       bool &approxOut);

double getAccuracy(const CoordinateOperationNNPtr &op);

double getAccuracy(const std::vector<CoordinateOperationNNPtr> &ops);

void exportSourceCRSAndTargetCRSToWKT(const CoordinateOperation *co,
                                      io::WKTFormatter *formatter);

//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END

#endif // OPUTILS_HPP
