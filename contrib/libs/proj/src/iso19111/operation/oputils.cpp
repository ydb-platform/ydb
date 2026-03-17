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
#define FROM_PROJ_CPP
#endif

#include <string.h>

#include "proj/coordinateoperation.hpp"
#include "proj/crs.hpp"
#include "proj/util.hpp"

#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

#include "oputils.hpp"
#include "parammappings.hpp"

#include "proj_constants.h"

// ---------------------------------------------------------------------------

NS_PROJ_START

using namespace internal;

namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

const char *BALLPARK_GEOCENTRIC_TRANSLATION = "Ballpark geocentric translation";
const char *NULL_GEOGRAPHIC_OFFSET = "Null geographic offset";
const char *NULL_GEOCENTRIC_TRANSLATION = "Null geocentric translation";
const char *BALLPARK_GEOGRAPHIC_OFFSET = "Ballpark geographic offset";
const char *BALLPARK_VERTICAL_TRANSFORMATION =
    "ballpark vertical transformation";
const char *BALLPARK_VERTICAL_TRANSFORMATION_NO_ELLIPSOID_VERT_HEIGHT =
    "ballpark vertical transformation, without ellipsoid height to vertical "
    "height correction";

// ---------------------------------------------------------------------------

OperationParameterNNPtr createOpParamNameEPSGCode(int code) {
    const char *name = OperationParameter::getNameForEPSGCode(code);
    assert(name);
    return OperationParameter::create(createMapNameEPSGCode(name, code));
}

// ---------------------------------------------------------------------------

util::PropertyMap createMethodMapNameEPSGCode(int code) {
    const char *name = nullptr;
    size_t nMethodNameCodes = 0;
    const auto methodNameCodes = getMethodNameCodes(nMethodNameCodes);
    for (size_t i = 0; i < nMethodNameCodes; ++i) {
        const auto &tuple = methodNameCodes[i];
        if (tuple.epsg_code == code) {
            name = tuple.name;
            break;
        }
    }
    assert(name);
    return createMapNameEPSGCode(name, code);
}

// ---------------------------------------------------------------------------

util::PropertyMap createMapNameEPSGCode(const std::string &name, int code) {
    return util::PropertyMap()
        .set(common::IdentifiedObject::NAME_KEY, name)
        .set(metadata::Identifier::CODESPACE_KEY, metadata::Identifier::EPSG)
        .set(metadata::Identifier::CODE_KEY, code);
}

// ---------------------------------------------------------------------------

util::PropertyMap createMapNameEPSGCode(const char *name, int code) {
    return util::PropertyMap()
        .set(common::IdentifiedObject::NAME_KEY, name)
        .set(metadata::Identifier::CODESPACE_KEY, metadata::Identifier::EPSG)
        .set(metadata::Identifier::CODE_KEY, code);
}

// ---------------------------------------------------------------------------

util::PropertyMap &addDomains(util::PropertyMap &map,
                              const common::ObjectUsage *obj) {

    auto ar = util::ArrayOfBaseObject::create();
    for (const auto &domain : obj->domains()) {
        ar->add(domain);
    }
    if (!ar->empty()) {
        map.set(common::ObjectUsage::OBJECT_DOMAIN_KEY, ar);
    }
    return map;
}

// ---------------------------------------------------------------------------

static const char *getCRSQualifierStr(const crs::CRSPtr &crs) {
    auto geod = dynamic_cast<crs::GeodeticCRS *>(crs.get());
    if (geod) {
        if (geod->isGeocentric()) {
            return " (geocentric)";
        }
        auto geog = dynamic_cast<crs::GeographicCRS *>(geod);
        if (geog) {
            if (geog->coordinateSystem()->axisList().size() == 2) {
                return " (geog2D)";
            } else {
                return " (geog3D)";
            }
        }
    }
    return "";
}

// ---------------------------------------------------------------------------

std::string buildOpName(const char *opType, const crs::CRSPtr &source,
                        const crs::CRSPtr &target) {
    std::string res(opType);
    const auto &srcName = source->nameStr();
    const auto &targetName = target->nameStr();
    const char *srcQualifier = "";
    const char *targetQualifier = "";
    if (srcName == targetName) {
        srcQualifier = getCRSQualifierStr(source);
        targetQualifier = getCRSQualifierStr(target);
        if (strcmp(srcQualifier, targetQualifier) == 0) {
            srcQualifier = "";
            targetQualifier = "";
        }
    }
    res += " from ";
    res += srcName;
    res += srcQualifier;
    res += " to ";
    res += targetName;
    res += targetQualifier;
    return res;
}

// ---------------------------------------------------------------------------

void addModifiedIdentifier(util::PropertyMap &map,
                           const common::IdentifiedObject *obj, bool inverse,
                           bool derivedFrom) {
    // If original operation is AUTH:CODE, then assign INVERSE(AUTH):CODE
    // as identifier.

    auto ar = util::ArrayOfBaseObject::create();
    for (const auto &idSrc : obj->identifiers()) {
        auto authName = *(idSrc->codeSpace());
        const auto &srcCode = idSrc->code();
        if (derivedFrom) {
            authName = concat("DERIVED_FROM(", authName, ")");
        }
        if (inverse) {
            if (starts_with(authName, "INVERSE(") && authName.back() == ')') {
                authName = authName.substr(strlen("INVERSE("));
                authName.resize(authName.size() - 1);
            } else {
                authName = concat("INVERSE(", authName, ")");
            }
        }
        auto idsProp = util::PropertyMap().set(
            metadata::Identifier::CODESPACE_KEY, authName);
        ar->add(metadata::Identifier::create(srcCode, idsProp));
    }
    if (!ar->empty()) {
        map.set(common::IdentifiedObject::IDENTIFIERS_KEY, ar);
    }
}

// ---------------------------------------------------------------------------

util::PropertyMap
createPropertiesForInverse(const OperationMethodNNPtr &method) {
    util::PropertyMap map;

    const std::string &forwardName = method->nameStr();
    if (!forwardName.empty()) {
        if (starts_with(forwardName, INVERSE_OF)) {
            map.set(common::IdentifiedObject::NAME_KEY,
                    forwardName.substr(INVERSE_OF.size()));
        } else {
            map.set(common::IdentifiedObject::NAME_KEY,
                    INVERSE_OF + forwardName);
        }
    }

    addModifiedIdentifier(map, method.get(), true, false);

    return map;
}

// ---------------------------------------------------------------------------

util::PropertyMap createPropertiesForInverse(const CoordinateOperation *op,
                                             bool derivedFrom,
                                             bool approximateInversion) {
    assert(op);
    util::PropertyMap map;

    // The domain(s) are unchanged by the inverse operation
    addDomains(map, op);

    const std::string &forwardName = op->nameStr();

    // Forge a name for the inverse, either from the forward name, or
    // from the source and target CRS names
    const char *opType;
    if (starts_with(forwardName, BALLPARK_GEOCENTRIC_TRANSLATION)) {
        opType = BALLPARK_GEOCENTRIC_TRANSLATION;
    } else if (starts_with(forwardName, BALLPARK_GEOGRAPHIC_OFFSET)) {
        opType = BALLPARK_GEOGRAPHIC_OFFSET;
    } else if (starts_with(forwardName, NULL_GEOGRAPHIC_OFFSET)) {
        opType = NULL_GEOGRAPHIC_OFFSET;
    } else if (starts_with(forwardName, NULL_GEOCENTRIC_TRANSLATION)) {
        opType = NULL_GEOCENTRIC_TRANSLATION;
    } else if (dynamic_cast<const Transformation *>(op) ||
               starts_with(forwardName, "Transformation from ")) {
        opType = "Transformation";
    } else if (dynamic_cast<const Conversion *>(op)) {
        opType = "Conversion";
    } else {
        opType = "Operation";
    }

    auto sourceCRS = op->sourceCRS();
    auto targetCRS = op->targetCRS();
    std::string name;
    if (!forwardName.empty()) {
        if (dynamic_cast<const Transformation *>(op) == nullptr &&
            dynamic_cast<const ConcatenatedOperation *>(op) == nullptr &&
            (starts_with(forwardName, INVERSE_OF) ||
             forwardName.find(" + ") != std::string::npos)) {
            std::vector<std::string> tokens;
            std::string curToken;
            bool inString = false;
            for (size_t i = 0; i < forwardName.size(); ++i) {
                if (inString) {
                    curToken += forwardName[i];
                    if (forwardName[i] == '\'') {
                        inString = false;
                    }
                } else if (i + 3 < forwardName.size() &&
                           memcmp(&forwardName[i], " + ", 3) == 0) {
                    tokens.push_back(curToken);
                    curToken.clear();
                    i += 2;
                } else if (forwardName[i] == '\'') {
                    inString = true;
                    curToken += forwardName[i];
                } else {
                    curToken += forwardName[i];
                }
            }
            if (!curToken.empty()) {
                tokens.push_back(std::move(curToken));
            }
            for (size_t i = tokens.size(); i > 0;) {
                i--;
                if (!name.empty()) {
                    name += " + ";
                }
                if (starts_with(tokens[i], INVERSE_OF)) {
                    name += tokens[i].substr(INVERSE_OF.size());
                } else if (tokens[i] == AXIS_ORDER_CHANGE_2D_NAME ||
                           tokens[i] == AXIS_ORDER_CHANGE_3D_NAME) {
                    name += tokens[i];
                } else {
                    name += INVERSE_OF + tokens[i];
                }
            }
        } else if (!sourceCRS || !targetCRS ||
                   forwardName != buildOpName(opType, sourceCRS, targetCRS)) {
            if (forwardName.find(" + ") != std::string::npos) {
                name = INVERSE_OF + '\'' + forwardName + '\'';
            } else {
                name = INVERSE_OF + forwardName;
            }
        }
    }
    if (name.empty() && sourceCRS && targetCRS) {
        name = buildOpName(opType, targetCRS, sourceCRS);
    }
    if (approximateInversion) {
        name += " (approx. inversion)";
    }

    if (!name.empty()) {
        map.set(common::IdentifiedObject::NAME_KEY, name);
    }

    const std::string &remarks = op->remarks();
    if (!remarks.empty()) {
        map.set(common::IdentifiedObject::REMARKS_KEY, remarks);
    }

    addModifiedIdentifier(map, op, true, derivedFrom);

    const auto so = dynamic_cast<const SingleOperation *>(op);
    if (so) {
        const int soMethodEPSGCode = so->method()->getEPSGCode();
        if (soMethodEPSGCode > 0) {
            map.set("OPERATION_METHOD_EPSG_CODE", soMethodEPSGCode);
        }
    }

    return map;
}

// ---------------------------------------------------------------------------

util::PropertyMap addDefaultNameIfNeeded(const util::PropertyMap &properties,
                                         const std::string &defaultName) {
    if (!properties.get(common::IdentifiedObject::NAME_KEY)) {
        return util::PropertyMap(properties)
            .set(common::IdentifiedObject::NAME_KEY, defaultName);
    } else {
        return properties;
    }
}

// ---------------------------------------------------------------------------

static std::string createEntryEqParam(const std::string &a,
                                      const std::string &b) {
    return a < b ? a + b : b + a;
}

static std::set<std::string> buildSetEquivalentParameters() {

    std::set<std::string> set;

    const char *const listOfEquivalentParameterNames[][7] = {
        {"latitude_of_point_1", "Latitude_Of_1st_Point", nullptr},
        {"longitude_of_point_1", "Longitude_Of_1st_Point", nullptr},
        {"latitude_of_point_2", "Latitude_Of_2nd_Point", nullptr},
        {"longitude_of_point_2", "Longitude_Of_2nd_Point", nullptr},

        {"satellite_height", "height", nullptr},

        {EPSG_NAME_PARAMETER_FALSE_EASTING,
         EPSG_NAME_PARAMETER_EASTING_FALSE_ORIGIN,
         EPSG_NAME_PARAMETER_EASTING_PROJECTION_CENTRE, nullptr},

        {EPSG_NAME_PARAMETER_FALSE_NORTHING,
         EPSG_NAME_PARAMETER_NORTHING_FALSE_ORIGIN,
         EPSG_NAME_PARAMETER_NORTHING_PROJECTION_CENTRE, nullptr},

        {EPSG_NAME_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN, WKT1_SCALE_FACTOR,
         EPSG_NAME_PARAMETER_SCALE_FACTOR_INITIAL_LINE,
         EPSG_NAME_PARAMETER_SCALE_FACTOR_PROJECTION_CENTRE,
         EPSG_NAME_PARAMETER_SCALE_FACTOR_PSEUDO_STANDARD_PARALLEL, nullptr},

        {WKT1_LATITUDE_OF_ORIGIN, WKT1_LATITUDE_OF_CENTER,
         EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN,
         EPSG_NAME_PARAMETER_LATITUDE_FALSE_ORIGIN,
         EPSG_NAME_PARAMETER_LATITUDE_PROJECTION_CENTRE, "Central_Parallel",
         nullptr},

        {WKT1_CENTRAL_MERIDIAN, WKT1_LONGITUDE_OF_CENTER,
         EPSG_NAME_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN,
         EPSG_NAME_PARAMETER_LONGITUDE_FALSE_ORIGIN,
         EPSG_NAME_PARAMETER_LONGITUDE_PROJECTION_CENTRE,
         EPSG_NAME_PARAMETER_LONGITUDE_OF_ORIGIN, nullptr},

        {EPSG_NAME_PARAMETER_AZIMUTH_INITIAL_LINE,
         EPSG_NAME_PARAMETER_AZIMUTH_PROJECTION_CENTRE, nullptr},

        {"pseudo_standard_parallel_1", WKT1_STANDARD_PARALLEL_1, nullptr},
    };

    for (const auto &paramList : listOfEquivalentParameterNames) {
        for (size_t i = 0; paramList[i]; i++) {
            auto a = metadata::Identifier::canonicalizeName(paramList[i]);
            for (size_t j = i + 1; paramList[j]; j++) {
                auto b = metadata::Identifier::canonicalizeName(paramList[j]);
                set.insert(createEntryEqParam(a, b));
            }
        }
    }
    return set;
}

bool areEquivalentParameters(const std::string &a, const std::string &b) {

    static const std::set<std::string> setEquivalentParameters =
        buildSetEquivalentParameters();

    auto a_can = metadata::Identifier::canonicalizeName(a);
    auto b_can = metadata::Identifier::canonicalizeName(b);
    return setEquivalentParameters.find(createEntryEqParam(a_can, b_can)) !=
           setEquivalentParameters.end();
}

// ---------------------------------------------------------------------------

bool isTimeDependent(const std::string &methodName) {
    return ci_find(methodName, "Time dependent") != std::string::npos ||
           ci_find(methodName, "Time-dependent") != std::string::npos;
}

// ---------------------------------------------------------------------------

std::string computeConcatenatedName(
    const std::vector<CoordinateOperationNNPtr> &flattenOps) {
    std::string name;
    for (const auto &subOp : flattenOps) {
        if (!name.empty()) {
            name += " + ";
        }
        const auto &l_name = subOp->nameStr();
        if (l_name.empty()) {
            name += "unnamed";
        } else {
            name += l_name;
        }
    }
    return name;
}

// ---------------------------------------------------------------------------

metadata::ExtentPtr getExtent(const CoordinateOperationNNPtr &op,
                              bool conversionExtentIsWorld,
                              bool &emptyIntersection) {
    auto conv = dynamic_cast<const Conversion *>(op.get());
    if (conv) {
        emptyIntersection = false;
        return metadata::Extent::WORLD;
    }
    const auto &domains = op->domains();
    if (!domains.empty()) {
        emptyIntersection = false;
        return domains[0]->domainOfValidity();
    }
    auto concatenated = dynamic_cast<const ConcatenatedOperation *>(op.get());
    if (!concatenated) {
        emptyIntersection = false;
        return nullptr;
    }
    return getExtent(concatenated->operations(), conversionExtentIsWorld,
                     emptyIntersection);
}

// ---------------------------------------------------------------------------

static const metadata::ExtentPtr nullExtent{};

const metadata::ExtentPtr &getExtent(const crs::CRSNNPtr &crs) {
    const auto &domains = crs->domains();
    if (!domains.empty()) {
        return domains[0]->domainOfValidity();
    }
    const auto *boundCRS = dynamic_cast<const crs::BoundCRS *>(crs.get());
    if (boundCRS) {
        return getExtent(boundCRS->baseCRS());
    }
    return nullExtent;
}

const metadata::ExtentPtr getExtentPossiblySynthetized(const crs::CRSNNPtr &crs,
                                                       bool &approxOut) {
    const auto &rawExtent(getExtent(crs));
    approxOut = false;
    if (rawExtent)
        return rawExtent;
    const auto compoundCRS = dynamic_cast<const crs::CompoundCRS *>(crs.get());
    if (compoundCRS) {
        // For a compoundCRS, take the intersection of the extent of its
        // components.
        const auto &components = compoundCRS->componentReferenceSystems();
        metadata::ExtentPtr extent;
        approxOut = true;
        for (const auto &component : components) {
            const auto &componentExtent(getExtent(component));
            if (extent && componentExtent)
                extent = extent->intersection(NN_NO_CHECK(componentExtent));
            else if (componentExtent)
                extent = componentExtent;
        }
        return extent;
    }
    return rawExtent;
}

// ---------------------------------------------------------------------------

metadata::ExtentPtr getExtent(const std::vector<CoordinateOperationNNPtr> &ops,
                              bool conversionExtentIsWorld,
                              bool &emptyIntersection) {
    metadata::ExtentPtr res = nullptr;
    for (const auto &subop : ops) {

        const auto &subExtent =
            getExtent(subop, conversionExtentIsWorld, emptyIntersection);
        if (!subExtent) {
            if (emptyIntersection) {
                return nullptr;
            }
            continue;
        }
        if (res == nullptr) {
            res = subExtent;
        } else {
            res = res->intersection(NN_NO_CHECK(subExtent));
            if (!res) {
                emptyIntersection = true;
                return nullptr;
            }
        }
    }
    emptyIntersection = false;
    return res;
}

// ---------------------------------------------------------------------------

// Returns the accuracy of an operation, or -1 if unknown
double getAccuracy(const CoordinateOperationNNPtr &op) {

    if (dynamic_cast<const Conversion *>(op.get())) {
        // A conversion is perfectly accurate.
        return 0.0;
    }

    double accuracy = -1.0;
    const auto &accuracies = op->coordinateOperationAccuracies();
    if (!accuracies.empty()) {
        try {
            accuracy = c_locale_stod(accuracies[0]->value());
        } catch (const std::exception &) {
        }
    } else {
        auto concatenated =
            dynamic_cast<const ConcatenatedOperation *>(op.get());
        if (concatenated) {
            accuracy = getAccuracy(concatenated->operations());
        }
    }
    return accuracy;
}

// ---------------------------------------------------------------------------

// Returns the accuracy of a set of concatenated operations, or -1 if unknown
double getAccuracy(const std::vector<CoordinateOperationNNPtr> &ops) {
    double accuracy = -1.0;
    for (const auto &subop : ops) {
        const double subops_accuracy = getAccuracy(subop);
        if (subops_accuracy < 0.0) {
            return -1.0;
        }
        if (accuracy < 0.0) {
            accuracy = 0.0;
        }
        accuracy += subops_accuracy;
    }
    return accuracy;
}

// ---------------------------------------------------------------------------

void exportSourceCRSAndTargetCRSToWKT(const CoordinateOperation *co,
                                      io::WKTFormatter *formatter) {
    auto l_sourceCRS = co->sourceCRS();
    assert(l_sourceCRS);
    auto l_targetCRS = co->targetCRS();
    assert(l_targetCRS);
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    const bool canExportCRSId =
        (isWKT2 && formatter->use2019Keywords() &&
         !(formatter->idOnTopLevelOnly() && formatter->topLevelHasId()));

    const bool hasDomains = !co->domains().empty();
    if (hasDomains) {
        formatter->pushDisableUsage();
    }

    formatter->startNode(io::WKTConstants::SOURCECRS, false);
    if (canExportCRSId && !l_sourceCRS->identifiers().empty()) {
        // fake that top node has no id, so that the sourceCRS id is
        // considered
        formatter->pushHasId(false);
        l_sourceCRS->_exportToWKT(formatter);
        formatter->popHasId();
    } else {
        l_sourceCRS->_exportToWKT(formatter);
    }
    formatter->endNode();

    formatter->startNode(io::WKTConstants::TARGETCRS, false);
    if (canExportCRSId && !l_targetCRS->identifiers().empty()) {
        // fake that top node has no id, so that the targetCRS id is
        // considered
        formatter->pushHasId(false);
        l_targetCRS->_exportToWKT(formatter);
        formatter->popHasId();
    } else {
        l_targetCRS->_exportToWKT(formatter);
    }
    formatter->endNode();

    if (hasDomains) {
        formatter->popDisableUsage();
    }
}

//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END
