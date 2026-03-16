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

//! @cond Doxygen_Suppress
#define DO_NOT_DEFINE_EXTERN_DERIVED_CRS_TEMPLATE
//! @endcond

#include "proj/crs.hpp"
#include "proj/common.hpp"
#include "proj/coordinateoperation.hpp"
#include "proj/coordinatesystem.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/coordinatesystem_internal.hpp"
#include "proj/internal/crs_internal.hpp"
#include "proj/internal/datum_internal.hpp"
#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

#include "operation/oputils.hpp"

#include "proj_constants.h"
#include "proj_json_streaming_writer.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace NS_PROJ::internal;

#if 0
namespace dropbox{ namespace oxygen {
template<> nn<NS_PROJ::crs::CRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::SingleCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::GeodeticCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::GeographicCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::DerivedCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::ProjectedCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::VerticalCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::CompoundCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::TemporalCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::EngineeringCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::ParametricCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::BoundCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::DerivedGeodeticCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::DerivedGeographicCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::DerivedProjectedCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::DerivedVerticalCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::DerivedTemporalCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::DerivedEngineeringCRSPtr>::~nn() = default;
template<> nn<NS_PROJ::crs::DerivedParametricCRSPtr>::~nn() = default;
}}
#endif

NS_PROJ_START

namespace crs {

//! @cond Doxygen_Suppress
constexpr const char *PROMOTED_TO_3D_PRELUDE = "Promoted to 3D from ";
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct CRS::Private {
    BoundCRSPtr canonicalBoundCRS_{};
    std::string extensionProj4_{};
    bool implicitCS_ = false;
    bool over_ = false;

    bool allowNonConformantWKT1Export_ = false;
    // for what was initially a COMPD_CS with a VERT_CS with a datum type ==
    // ellipsoidal height / 2002
    CompoundCRSPtr originalCompoundCRS_{};

    void setNonStandardProperties(const util::PropertyMap &properties) {
        {
            const auto pVal = properties.get("IMPLICIT_CS");
            if (pVal) {
                if (const auto genVal =
                        dynamic_cast<const util::BoxedValue *>(pVal->get())) {
                    if (genVal->type() == util::BoxedValue::Type::BOOLEAN &&
                        genVal->booleanValue()) {
                        implicitCS_ = true;
                    }
                }
            }
        }

        {
            const auto pVal = properties.get("OVER");
            if (pVal) {
                if (const auto genVal =
                        dynamic_cast<const util::BoxedValue *>(pVal->get())) {
                    if (genVal->type() == util::BoxedValue::Type::BOOLEAN &&
                        genVal->booleanValue()) {
                        over_ = true;
                    }
                }
            }
        }
    }
};
//! @endcond

// ---------------------------------------------------------------------------

CRS::CRS() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

CRS::CRS(const CRS &other)
    : ObjectUsage(other), d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CRS::~CRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

/** \brief Return whether the CRS has an implicit coordinate system
 * (e.g from ESRI WKT) */
bool CRS::hasImplicitCS() const { return d->implicitCS_; }

/** \brief Return whether the CRS has a +over flag */
bool CRS::hasOver() const { return d->over_; }

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the BoundCRS potentially attached to this CRS.
 *
 * In the case this method is called on a object returned by
 * BoundCRS::baseCRSWithCanonicalBoundCRS(), this method will return this
 * BoundCRS
 *
 * @return a BoundCRSPtr, that might be null.
 */
const BoundCRSPtr &CRS::canonicalBoundCRS() PROJ_PURE_DEFN {
    return d->canonicalBoundCRS_;
}

// ---------------------------------------------------------------------------

/** \brief Return whether a CRS is a dynamic CRS.
 *
 * A dynamic CRS is a CRS that contains a geodetic CRS whose geodetic reference
 * frame is dynamic, or a vertical CRS whose vertical reference frame is
 * dynamic.
 * @param considerWGS84AsDynamic set to true to consider the WGS 84 / EPSG:6326
 *                               datum ensemble as dynamic.
 * @since 9.2
 */
bool CRS::isDynamic(bool considerWGS84AsDynamic) const {

    if (auto raw = extractGeodeticCRSRaw()) {
        const auto &l_datum = raw->datum();
        if (l_datum) {
            if (dynamic_cast<datum::DynamicGeodeticReferenceFrame *>(
                    l_datum.get())) {
                return true;
            }
            if (considerWGS84AsDynamic &&
                l_datum->nameStr() == "World Geodetic System 1984") {
                return true;
            }
        }
        if (considerWGS84AsDynamic) {
            const auto &l_datumEnsemble = raw->datumEnsemble();
            if (l_datumEnsemble && l_datumEnsemble->nameStr() ==
                                       "World Geodetic System 1984 ensemble") {
                return true;
            }
        }
    }

    if (auto vertCRS = extractVerticalCRS()) {
        const auto &l_datum = vertCRS->datum();
        if (l_datum && dynamic_cast<datum::DynamicVerticalReferenceFrame *>(
                           l_datum.get())) {
            return true;
        }
    }

    return false;
}

// ---------------------------------------------------------------------------

/** \brief Return the GeodeticCRS of the CRS.
 *
 * Returns the GeodeticCRS contained in a CRS. This works currently with
 * input parameters of type GeodeticCRS or derived, ProjectedCRS,
 * CompoundCRS or BoundCRS.
 *
 * @return a GeodeticCRSPtr, that might be null.
 */
GeodeticCRSPtr CRS::extractGeodeticCRS() const {
    auto raw = extractGeodeticCRSRaw();
    if (raw) {
        return std::dynamic_pointer_cast<GeodeticCRS>(
            raw->shared_from_this().as_nullable());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const GeodeticCRS *CRS::extractGeodeticCRSRaw() const {
    auto geodCRS = dynamic_cast<const GeodeticCRS *>(this);
    if (geodCRS) {
        return geodCRS;
    }
    auto projCRS = dynamic_cast<const ProjectedCRS *>(this);
    if (projCRS) {
        return projCRS->baseCRS()->extractGeodeticCRSRaw();
    }
    auto compoundCRS = dynamic_cast<const CompoundCRS *>(this);
    if (compoundCRS) {
        for (const auto &subCrs : compoundCRS->componentReferenceSystems()) {
            auto retGeogCRS = subCrs->extractGeodeticCRSRaw();
            if (retGeogCRS) {
                return retGeogCRS;
            }
        }
    }
    auto boundCRS = dynamic_cast<const BoundCRS *>(this);
    if (boundCRS) {
        return boundCRS->baseCRS()->extractGeodeticCRSRaw();
    }
    auto derivedProjectedCRS = dynamic_cast<const DerivedProjectedCRS *>(this);
    if (derivedProjectedCRS) {
        return derivedProjectedCRS->baseCRS()->extractGeodeticCRSRaw();
    }
    return nullptr;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const std::string &CRS::getExtensionProj4() const noexcept {
    return d->extensionProj4_;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the GeographicCRS of the CRS.
 *
 * Returns the GeographicCRS contained in a CRS. This works currently with
 * input parameters of type GeographicCRS or derived, ProjectedCRS,
 * CompoundCRS or BoundCRS.
 *
 * @return a GeographicCRSPtr, that might be null.
 */
GeographicCRSPtr CRS::extractGeographicCRS() const {
    auto raw = extractGeodeticCRSRaw();
    if (raw) {
        return std::dynamic_pointer_cast<GeographicCRS>(
            raw->shared_from_this().as_nullable());
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static util::PropertyMap
createPropertyMap(const common::IdentifiedObject *obj) {
    auto props = util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                         obj->nameStr());
    if (obj->isDeprecated()) {
        props.set(common::IdentifiedObject::DEPRECATED_KEY, true);
    }
    return props;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CRSNNPtr CRS::alterGeodeticCRS(const GeodeticCRSNNPtr &newGeodCRS) const {
    if (dynamic_cast<const GeodeticCRS *>(this)) {
        return newGeodCRS;
    }

    if (auto projCRS = dynamic_cast<const ProjectedCRS *>(this)) {
        return ProjectedCRS::create(createPropertyMap(this), newGeodCRS,
                                    projCRS->derivingConversion(),
                                    projCRS->coordinateSystem());
    }

    if (auto derivedProjCRS = dynamic_cast<const DerivedProjectedCRS *>(this)) {
        auto newProjCRS =
            NN_CHECK_ASSERT(util::nn_dynamic_pointer_cast<ProjectedCRS>(
                derivedProjCRS->baseCRS()->alterGeodeticCRS(newGeodCRS)));

        return DerivedProjectedCRS::create(createPropertyMap(this), newProjCRS,
                                           derivedProjCRS->derivingConversion(),
                                           derivedProjCRS->coordinateSystem());
    }

    if (auto compoundCRS = dynamic_cast<const CompoundCRS *>(this)) {
        std::vector<CRSNNPtr> components;
        for (const auto &subCrs : compoundCRS->componentReferenceSystems()) {
            components.emplace_back(subCrs->alterGeodeticCRS(newGeodCRS));
        }
        return CompoundCRS::create(createPropertyMap(this), components);
    }

    return NN_NO_CHECK(
        std::dynamic_pointer_cast<CRS>(shared_from_this().as_nullable()));
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CRSNNPtr CRS::alterCSLinearUnit(const common::UnitOfMeasure &unit) const {
    {
        auto projCRS = dynamic_cast<const ProjectedCRS *>(this);
        if (projCRS) {
            return ProjectedCRS::create(
                createPropertyMap(this), projCRS->baseCRS(),
                projCRS->derivingConversion(),
                projCRS->coordinateSystem()->alterUnit(unit));
        }
    }

    {
        auto geodCRS = dynamic_cast<const GeodeticCRS *>(this);
        if (geodCRS && geodCRS->isGeocentric()) {
            auto cs = dynamic_cast<const cs::CartesianCS *>(
                geodCRS->coordinateSystem().get());
            assert(cs);
            return GeodeticCRS::create(
                createPropertyMap(this), geodCRS->datum(),
                geodCRS->datumEnsemble(), cs->alterUnit(unit));
        }
    }

    {
        auto geogCRS = dynamic_cast<const GeographicCRS *>(this);
        if (geogCRS && geogCRS->coordinateSystem()->axisList().size() == 3) {
            return GeographicCRS::create(
                createPropertyMap(this), geogCRS->datum(),
                geogCRS->datumEnsemble(),
                geogCRS->coordinateSystem()->alterLinearUnit(unit));
        }
    }

    {
        auto vertCRS = dynamic_cast<const VerticalCRS *>(this);
        if (vertCRS) {
            return VerticalCRS::create(
                createPropertyMap(this), vertCRS->datum(),
                vertCRS->datumEnsemble(),
                vertCRS->coordinateSystem()->alterUnit(unit));
        }
    }

    {
        auto engCRS = dynamic_cast<const EngineeringCRS *>(this);
        if (engCRS) {
            auto cartCS = util::nn_dynamic_pointer_cast<cs::CartesianCS>(
                engCRS->coordinateSystem());
            if (cartCS) {
                return EngineeringCRS::create(createPropertyMap(this),
                                              engCRS->datum(),
                                              cartCS->alterUnit(unit));
            } else {
                auto vertCS = util::nn_dynamic_pointer_cast<cs::VerticalCS>(
                    engCRS->coordinateSystem());
                if (vertCS) {
                    return EngineeringCRS::create(createPropertyMap(this),
                                                  engCRS->datum(),
                                                  vertCS->alterUnit(unit));
                }
            }
        }
    }

    {
        auto derivedProjCRS = dynamic_cast<const DerivedProjectedCRS *>(this);
        if (derivedProjCRS) {
            auto cs = derivedProjCRS->coordinateSystem();
            auto cartCS = util::nn_dynamic_pointer_cast<cs::CartesianCS>(cs);
            if (cartCS) {
                cs = cartCS->alterUnit(unit);
            }
            return DerivedProjectedCRS::create(
                createPropertyMap(this), derivedProjCRS->baseCRS(),
                derivedProjCRS->derivingConversion(), cs);
        }
    }

    {
        auto compoundCRS = dynamic_cast<const CompoundCRS *>(this);
        if (compoundCRS) {
            std::vector<CRSNNPtr> components;
            for (const auto &subCrs :
                 compoundCRS->componentReferenceSystems()) {
                components.push_back(subCrs->alterCSLinearUnit(unit));
            }
            return CompoundCRS::create(createPropertyMap(this), components);
        }
    }

    {
        auto boundCRS = dynamic_cast<const BoundCRS *>(this);
        if (boundCRS) {
            return BoundCRS::create(
                createPropertyMap(this),
                boundCRS->baseCRS()->alterCSLinearUnit(unit),
                boundCRS->hubCRS(), boundCRS->transformation());
        }
    }

    return NN_NO_CHECK(
        std::dynamic_pointer_cast<CRS>(shared_from_this().as_nullable()));
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the VerticalCRS of the CRS.
 *
 * Returns the VerticalCRS contained in a CRS. This works currently with
 * input parameters of type VerticalCRS or derived, CompoundCRS or BoundCRS.
 *
 * @return a VerticalCRSPtr, that might be null.
 */
VerticalCRSPtr CRS::extractVerticalCRS() const {
    auto vertCRS = dynamic_cast<const VerticalCRS *>(this);
    if (vertCRS) {
        return std::dynamic_pointer_cast<VerticalCRS>(
            shared_from_this().as_nullable());
    }
    auto compoundCRS = dynamic_cast<const CompoundCRS *>(this);
    if (compoundCRS) {
        for (const auto &subCrs : compoundCRS->componentReferenceSystems()) {
            auto retVertCRS = subCrs->extractVerticalCRS();
            if (retVertCRS) {
                return retVertCRS;
            }
        }
    }
    auto boundCRS = dynamic_cast<const BoundCRS *>(this);
    if (boundCRS) {
        return boundCRS->baseCRS()->extractVerticalCRS();
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

/** \brief Returns potentially
 * a BoundCRS, with a transformation to EPSG:4326, wrapping this CRS
 *
 * If no such BoundCRS is possible, the object will be returned.
 *
 * The purpose of this method is to be able to format a PROJ.4 string with
 * a +towgs84 parameter or a WKT1:GDAL string with a TOWGS node.
 *
 * This method will fetch the GeographicCRS of this CRS and find a
 * transformation to EPSG:4326 using the domain of the validity of the main CRS,
 * and there's only one Helmert transformation.
 *
 * @return a CRS.
 */
CRSNNPtr CRS::createBoundCRSToWGS84IfPossible(
    const io::DatabaseContextPtr &dbContext,
    operation::CoordinateOperationContext::IntermediateCRSUse
        allowIntermediateCRSUse) const {
    auto thisAsCRS = NN_NO_CHECK(
        std::static_pointer_cast<CRS>(shared_from_this().as_nullable()));
    auto boundCRS = util::nn_dynamic_pointer_cast<BoundCRS>(thisAsCRS);
    if (!boundCRS) {
        boundCRS = canonicalBoundCRS();
    }
    if (boundCRS) {
        if (boundCRS->hubCRS()->_isEquivalentTo(
                GeographicCRS::EPSG_4326.get(),
                util::IComparable::Criterion::EQUIVALENT, dbContext)) {
            return NN_NO_CHECK(boundCRS);
        }
    }

    auto compoundCRS = dynamic_cast<const CompoundCRS *>(this);
    if (compoundCRS) {
        const auto &comps = compoundCRS->componentReferenceSystems();
        if (comps.size() == 2) {
            auto horiz = comps[0]->createBoundCRSToWGS84IfPossible(
                dbContext, allowIntermediateCRSUse);
            auto vert = comps[1]->createBoundCRSToWGS84IfPossible(
                dbContext, allowIntermediateCRSUse);
            if (horiz.get() != comps[0].get() || vert.get() != comps[1].get()) {
                return CompoundCRS::create(createPropertyMap(this),
                                           {std::move(horiz), std::move(vert)});
            }
        }
        return thisAsCRS;
    }

    if (!dbContext) {
        return thisAsCRS;
    }

    const auto &l_domains = domains();
    metadata::ExtentPtr extent;
    if (!l_domains.empty()) {
        if (l_domains.size() == 2) {
            // Special case for the UTM ETRS89 CRS that have 2 domains since
            // EPSG v11.009. The "Pan-European conformal mapping at scales
            // larger than 1:500,000" one includes a slightly smaller one
            // "Engineering survey, topographic mapping" valid for some
            // countries. Let's take the larger one to get a transformation
            // valid for all domains.
            auto extent0 = l_domains[0]->domainOfValidity();
            auto extent1 = l_domains[1]->domainOfValidity();
            if (extent0 && extent1) {
                if (extent0->contains(NN_NO_CHECK(extent1))) {
                    extent = std::move(extent0);
                } else if (extent1->contains(NN_NO_CHECK(extent0))) {
                    extent = std::move(extent1);
                }
            }
        }
        if (!extent) {
            if (l_domains.size() > 1) {
                // If there are several domains of validity, then it is
                // extremely unlikely, we could get a single transformation
                // valid for all. At least, in the current state of the code of
                // createOperations() which returns a single extent, this can't
                // happen.
                return thisAsCRS;
            }
            extent = l_domains[0]->domainOfValidity();
        }
    }

    std::string crs_authority;
    const auto &l_identifiers = identifiers();
    // If the object has an authority, restrict the transformations to
    // come from that codespace too. This avoids for example EPSG:4269
    // (NAD83) to use a (dubious) ESRI transformation.
    if (!l_identifiers.empty()) {
        crs_authority = *(l_identifiers[0]->codeSpace());
    }

    auto authorities = dbContext->getAllowedAuthorities(
        crs_authority, metadata::Identifier::EPSG);
    if (authorities.empty()) {
        authorities.emplace_back();
    }

    // Vertical CRS ?
    auto vertCRS = dynamic_cast<const VerticalCRS *>(this);
    if (vertCRS) {
        auto hubCRS =
            util::nn_static_pointer_cast<CRS>(GeographicCRS::EPSG_4979);
        for (const auto &authority : authorities) {
            try {

                auto authFactory = io::AuthorityFactory::create(
                    NN_NO_CHECK(dbContext),
                    authority == "any" ? std::string() : authority);
                auto ctxt = operation::CoordinateOperationContext::create(
                    authFactory, extent, 0.0);
                ctxt->setAllowUseIntermediateCRS(allowIntermediateCRSUse);
                // ctxt->setSpatialCriterion(
                //    operation::CoordinateOperationContext::SpatialCriterion::PARTIAL_INTERSECTION);
                auto list = operation::CoordinateOperationFactory::create()
                                ->createOperations(hubCRS, thisAsCRS, ctxt);
                CRSPtr candidateBoundCRS;
                for (const auto &op : list) {
                    auto transf = util::nn_dynamic_pointer_cast<
                        operation::Transformation>(op);
                    // Only keep transformations that use a known grid
                    if (transf && !transf->hasBallparkTransformation()) {
                        auto gridsNeeded = transf->gridsNeeded(dbContext, true);
                        bool gridsKnown = !gridsNeeded.empty();
                        for (const auto &gridDesc : gridsNeeded) {
                            if (gridDesc.packageName.empty() &&
                                !(!gridDesc.url.empty() &&
                                  gridDesc.openLicense) &&
                                !gridDesc.available) {
                                gridsKnown = false;
                                break;
                            }
                        }
                        if (gridsKnown) {
                            if (candidateBoundCRS) {
                                candidateBoundCRS = nullptr;
                                break;
                            }
                            candidateBoundCRS =
                                BoundCRS::create(thisAsCRS, hubCRS,
                                                 NN_NO_CHECK(transf))
                                    .as_nullable();
                        }
                    }
                }
                if (candidateBoundCRS) {
                    return NN_NO_CHECK(candidateBoundCRS);
                }
            } catch (const std::exception &) {
            }
        }
        return thisAsCRS;
    }

    // Geodetic/geographic CRS ?
    auto geodCRS = util::nn_dynamic_pointer_cast<GeodeticCRS>(thisAsCRS);
    auto geogCRS = extractGeographicCRS();
    auto hubCRS = util::nn_static_pointer_cast<CRS>(GeographicCRS::EPSG_4326);
    if (geodCRS && !geogCRS) {
        if (geodCRS->datumNonNull(dbContext)->nameStr() != "unknown" &&
            geodCRS->_isEquivalentTo(GeographicCRS::EPSG_4978.get(),
                                     util::IComparable::Criterion::EQUIVALENT,
                                     dbContext)) {
            return thisAsCRS;
        }
        hubCRS = util::nn_static_pointer_cast<CRS>(GeodeticCRS::EPSG_4978);
    } else if (!geogCRS ||
               (geogCRS->datumNonNull(dbContext)->nameStr() != "unknown" &&
                geogCRS->_isEquivalentTo(
                    GeographicCRS::EPSG_4326.get(),
                    util::IComparable::Criterion::EQUIVALENT, dbContext))) {
        return thisAsCRS;
    } else {
        geodCRS = geogCRS;
    }

    for (const auto &authority : authorities) {
        try {

            auto authFactory = io::AuthorityFactory::create(
                NN_NO_CHECK(dbContext),
                authority == "any" ? std::string() : authority);
            metadata::ExtentPtr extentResolved(extent);
            if (!extent) {
                getResolvedCRS(thisAsCRS, authFactory, extentResolved);
            }
            auto ctxt = operation::CoordinateOperationContext::create(
                authFactory, extentResolved, 0.0);
            ctxt->setAllowUseIntermediateCRS(allowIntermediateCRSUse);
            // ctxt->setSpatialCriterion(
            //    operation::CoordinateOperationContext::SpatialCriterion::PARTIAL_INTERSECTION);
            auto list =
                operation::CoordinateOperationFactory::create()
                    ->createOperations(NN_NO_CHECK(geodCRS), hubCRS, ctxt);
            CRSPtr candidateBoundCRS;
            int candidateCount = 0;

            const auto takeIntoAccountCandidate =
                [&](const operation::TransformationNNPtr &transf) {
                    if (transf->getTOWGS84Parameters(false).empty()) {
                        return;
                    }

                    candidateCount++;
                    if (candidateBoundCRS == nullptr) {
                        candidateCount = 1;
                        candidateBoundCRS =
                            BoundCRS::create(thisAsCRS, hubCRS, transf)
                                .as_nullable();
                    }
                };

            for (const auto &op : list) {
                auto transf =
                    util::nn_dynamic_pointer_cast<operation::Transformation>(
                        op);
                if (transf && !starts_with(transf->nameStr(), "Ballpark geo")) {
                    takeIntoAccountCandidate(NN_NO_CHECK(transf));
                } else {
                    auto concatenated =
                        dynamic_cast<const operation::ConcatenatedOperation *>(
                            op.get());
                    if (concatenated) {
                        // Case for EPSG:4807 / "NTF (Paris)" that is made of a
                        // longitude rotation followed by a Helmert
                        // The prime meridian shift will be accounted elsewhere
                        const auto &subops = concatenated->operations();
                        if (subops.size() == 2) {
                            auto firstOpIsTransformation =
                                dynamic_cast<const operation::Transformation *>(
                                    subops[0].get());
                            auto firstOpIsConversion =
                                dynamic_cast<const operation::Conversion *>(
                                    subops[0].get());
                            if ((firstOpIsTransformation &&
                                 firstOpIsTransformation
                                     ->isLongitudeRotation()) ||
                                (dynamic_cast<DerivedCRS *>(thisAsCRS.get()) &&
                                 firstOpIsConversion)) {
                                transf = util::nn_dynamic_pointer_cast<
                                    operation::Transformation>(subops[1]);
                                if (transf && !starts_with(transf->nameStr(),
                                                           "Ballpark geo")) {
                                    takeIntoAccountCandidate(
                                        NN_NO_CHECK(transf));
                                }
                            }
                        }
                    }
                }
            }
            if (candidateCount == 1 && candidateBoundCRS) {
                return NN_NO_CHECK(candidateBoundCRS);
            }
        } catch (const std::exception &) {
        }
    }
    return thisAsCRS;
}

// ---------------------------------------------------------------------------

/** \brief Returns a CRS whose coordinate system does not contain a vertical
 * component.
 *
 * As of PROJ 9.5, this method is an alias of demoteTo2D(std::string(),
 * nullptr), which deals with all potential CRS types.
 *
 * demoteTo2D() is a preferred alternative, especially when invoked with a
 * non-null database context, to perform a look-up in the database for
 * already registered 2D CRS.
 *
 * @return a CRS.
 */
CRSNNPtr CRS::stripVerticalComponent() const {
    return demoteTo2D(std::string(), nullptr);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

/** \brief Return a shallow clone of this object. */
CRSNNPtr CRS::shallowClone() const { return _shallowClone(); }

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

CRSNNPtr CRS::allowNonConformantWKT1Export() const {
    const auto boundCRS = dynamic_cast<const BoundCRS *>(this);
    if (boundCRS) {
        return BoundCRS::create(
            boundCRS->baseCRS()->allowNonConformantWKT1Export(),
            boundCRS->hubCRS(), boundCRS->transformation());
    }
    auto crs(shallowClone());
    crs->d->allowNonConformantWKT1Export_ = true;
    return crs;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

CRSNNPtr
CRS::attachOriginalCompoundCRS(const CompoundCRSNNPtr &compoundCRS) const {

    const auto boundCRS = dynamic_cast<const BoundCRS *>(this);
    if (boundCRS) {
        return BoundCRS::create(
            boundCRS->baseCRS()->attachOriginalCompoundCRS(compoundCRS),
            boundCRS->hubCRS(), boundCRS->transformation());
    }

    auto crs(shallowClone());
    crs->d->originalCompoundCRS_ = compoundCRS.as_nullable();
    return crs;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

CRSNNPtr CRS::alterName(const std::string &newName) const {
    auto crs = shallowClone();
    auto newNameMod(newName);
    auto props = util::PropertyMap();
    if (ends_with(newNameMod, " (deprecated)")) {
        newNameMod.resize(newNameMod.size() - strlen(" (deprecated)"));
        props.set(common::IdentifiedObject::DEPRECATED_KEY, true);
    }
    props.set(common::IdentifiedObject::NAME_KEY, newNameMod);
    crs->setProperties(props);
    return crs;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

CRSNNPtr CRS::alterId(const std::string &authName,
                      const std::string &code) const {
    auto crs = shallowClone();
    auto props = util::PropertyMap();
    props.set(metadata::Identifier::CODESPACE_KEY, authName)
        .set(metadata::Identifier::CODE_KEY, code);
    crs->setProperties(props);
    return crs;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static bool mustAxisOrderBeSwitchedForVisualizationInternal(
    const std::vector<cs::CoordinateSystemAxisNNPtr> &axisList) {
    const auto &dir0 = axisList[0]->direction();
    const auto &dir1 = axisList[1]->direction();
    if (&dir0 == &cs::AxisDirection::NORTH &&
        &dir1 == &cs::AxisDirection::EAST) {
        return true;
    }

    // Address EPSG:32661 "WGS 84 / UPS North (N,E)"
    if (&dir0 == &cs::AxisDirection::SOUTH &&
        &dir1 == &cs::AxisDirection::SOUTH) {
        const auto &meridian0 = axisList[0]->meridian();
        const auto &meridian1 = axisList[1]->meridian();
        return meridian0 != nullptr && meridian1 != nullptr &&
               std::abs(meridian0->longitude().convertToUnit(
                            common::UnitOfMeasure::DEGREE) -
                        180.0) < 1e-10 &&
               std::abs(meridian1->longitude().convertToUnit(
                            common::UnitOfMeasure::DEGREE) -
                        90.0) < 1e-10;
    }

    if (&dir0 == &cs::AxisDirection::NORTH &&
        &dir1 == &cs::AxisDirection::NORTH) {
        const auto &meridian0 = axisList[0]->meridian();
        const auto &meridian1 = axisList[1]->meridian();
        return meridian0 != nullptr && meridian1 != nullptr &&
               ((
                    // Address EPSG:32761 "WGS 84 / UPS South (N,E)"
                    std::abs(meridian0->longitude().convertToUnit(
                                 common::UnitOfMeasure::DEGREE) -
                             0.0) < 1e-10 &&
                    std::abs(meridian1->longitude().convertToUnit(
                                 common::UnitOfMeasure::DEGREE) -
                             90.0) < 1e-10) ||
                // Address EPSG:5482 "RSRGD2000 / RSPS2000"
                (std::abs(meridian0->longitude().convertToUnit(
                              common::UnitOfMeasure::DEGREE) -
                          180) < 1e-10 &&
                 std::abs(meridian1->longitude().convertToUnit(
                              common::UnitOfMeasure::DEGREE) -
                          -90.0) < 1e-10));
    }

    return false;
}
// ---------------------------------------------------------------------------

bool CRS::mustAxisOrderBeSwitchedForVisualization() const {

    if (const CompoundCRS *compoundCRS =
            dynamic_cast<const CompoundCRS *>(this)) {
        const auto &comps = compoundCRS->componentReferenceSystems();
        if (!comps.empty()) {
            return comps[0]->mustAxisOrderBeSwitchedForVisualization();
        }
    }

    if (const GeographicCRS *geogCRS =
            dynamic_cast<const GeographicCRS *>(this)) {
        return mustAxisOrderBeSwitchedForVisualizationInternal(
            geogCRS->coordinateSystem()->axisList());
    }

    if (const ProjectedCRS *projCRS =
            dynamic_cast<const ProjectedCRS *>(this)) {
        return mustAxisOrderBeSwitchedForVisualizationInternal(
            projCRS->coordinateSystem()->axisList());
    }

    if (const DerivedProjectedCRS *derivedProjCRS =
            dynamic_cast<const DerivedProjectedCRS *>(this)) {
        return mustAxisOrderBeSwitchedForVisualizationInternal(
            derivedProjCRS->coordinateSystem()->axisList());
    }

    return false;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

void CRS::setProperties(
    const util::PropertyMap &properties) // throw(InvalidValueTypeException)
{
    std::string l_remarks;
    std::string extensionProj4;
    properties.getStringValue(IdentifiedObject::REMARKS_KEY, l_remarks);
    properties.getStringValue("EXTENSION_PROJ4", extensionProj4);

    const char *PROJ_CRS_STRING_PREFIX = "PROJ CRS string: ";
    const char *PROJ_CRS_STRING_SUFFIX = ". ";
    const auto beginOfProjStringPos = l_remarks.find(PROJ_CRS_STRING_PREFIX);
    if (beginOfProjStringPos == std::string::npos && extensionProj4.empty()) {
        ObjectUsage::setProperties(properties);
        return;
    }

    util::PropertyMap newProperties(properties);

    // Parse remarks and extract EXTENSION_PROJ4 from it
    if (extensionProj4.empty()) {
        if (beginOfProjStringPos != std::string::npos) {
            const auto endOfProjStringPos =
                l_remarks.find(PROJ_CRS_STRING_SUFFIX, beginOfProjStringPos);
            if (endOfProjStringPos == std::string::npos) {
                extensionProj4 = l_remarks.substr(
                    beginOfProjStringPos + strlen(PROJ_CRS_STRING_PREFIX));
            } else {
                extensionProj4 = l_remarks.substr(
                    beginOfProjStringPos + strlen(PROJ_CRS_STRING_PREFIX),
                    endOfProjStringPos - beginOfProjStringPos -
                        strlen(PROJ_CRS_STRING_PREFIX));
            }
        }
    }

    if (!extensionProj4.empty()) {
        if (beginOfProjStringPos == std::string::npos) {
            // Add EXTENSION_PROJ4 to remarks
            l_remarks =
                PROJ_CRS_STRING_PREFIX + extensionProj4 +
                (l_remarks.empty() ? std::string()
                                   : PROJ_CRS_STRING_SUFFIX + l_remarks);
        }
    }

    newProperties.set(IdentifiedObject::REMARKS_KEY, l_remarks);

    ObjectUsage::setProperties(newProperties);

    d->extensionProj4_ = std::move(extensionProj4);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

CRSNNPtr CRS::applyAxisOrderReversal(const char *nameSuffix) const {

    const auto createProperties =
        [this, nameSuffix](const std::string &newNameIn = std::string()) {
            std::string newName(newNameIn);
            if (newName.empty()) {
                newName = nameStr();
                if (ends_with(newName, NORMALIZED_AXIS_ORDER_SUFFIX_STR)) {
                    newName.resize(newName.size() -
                                   strlen(NORMALIZED_AXIS_ORDER_SUFFIX_STR));
                } else if (ends_with(newName, AXIS_ORDER_REVERSED_SUFFIX_STR)) {
                    newName.resize(newName.size() -
                                   strlen(AXIS_ORDER_REVERSED_SUFFIX_STR));
                } else {
                    newName += nameSuffix;
                }
            }
            auto props = util::PropertyMap().set(
                common::IdentifiedObject::NAME_KEY, newName);
            const auto &l_domains = domains();
            if (!l_domains.empty()) {
                auto array = util::ArrayOfBaseObject::create();
                for (const auto &domain : l_domains) {
                    array->add(domain);
                }
                if (!array->empty()) {
                    props.set(common::ObjectUsage::OBJECT_DOMAIN_KEY, array);
                }
            }
            const auto &l_identifiers = identifiers();
            const auto &l_remarks = remarks();
            if (l_identifiers.size() == 1) {
                std::string remarks("Axis order reversed compared to ");
                if (!starts_with(l_remarks, remarks)) {
                    remarks += *(l_identifiers[0]->codeSpace());
                    remarks += ':';
                    remarks += l_identifiers[0]->code();
                    if (!l_remarks.empty()) {
                        remarks += ". ";
                        remarks += l_remarks;
                    }
                    props.set(common::IdentifiedObject::REMARKS_KEY, remarks);
                }
            } else if (!l_remarks.empty()) {
                props.set(common::IdentifiedObject::REMARKS_KEY, l_remarks);
            }
            return props;
        };

    if (const CompoundCRS *compoundCRS =
            dynamic_cast<const CompoundCRS *>(this)) {
        const auto &comps = compoundCRS->componentReferenceSystems();
        if (!comps.empty()) {
            std::vector<CRSNNPtr> newComps;
            newComps.emplace_back(comps[0]->applyAxisOrderReversal(nameSuffix));
            std::string l_name = newComps.back()->nameStr();
            for (size_t i = 1; i < comps.size(); i++) {
                newComps.emplace_back(comps[i]);
                l_name += " + ";
                l_name += newComps.back()->nameStr();
            }
            return util::nn_static_pointer_cast<CRS>(
                CompoundCRS::create(createProperties(l_name), newComps));
        }
    }

    if (const GeographicCRS *geogCRS =
            dynamic_cast<const GeographicCRS *>(this)) {
        const auto &axisList = geogCRS->coordinateSystem()->axisList();
        auto cs =
            axisList.size() == 2
                ? cs::EllipsoidalCS::create(util::PropertyMap(), axisList[1],
                                            axisList[0])
                : cs::EllipsoidalCS::create(util::PropertyMap(), axisList[1],
                                            axisList[0], axisList[2]);
        return util::nn_static_pointer_cast<CRS>(
            GeographicCRS::create(createProperties(), geogCRS->datum(),
                                  geogCRS->datumEnsemble(), cs));
    }

    if (const ProjectedCRS *projCRS =
            dynamic_cast<const ProjectedCRS *>(this)) {
        const auto &axisList = projCRS->coordinateSystem()->axisList();
        auto cs =
            axisList.size() == 2
                ? cs::CartesianCS::create(util::PropertyMap(), axisList[1],
                                          axisList[0])
                : cs::CartesianCS::create(util::PropertyMap(), axisList[1],
                                          axisList[0], axisList[2]);
        return util::nn_static_pointer_cast<CRS>(
            ProjectedCRS::create(createProperties(), projCRS->baseCRS(),
                                 projCRS->derivingConversion(), cs));
    }

    if (const DerivedProjectedCRS *derivedProjCRS =
            dynamic_cast<const DerivedProjectedCRS *>(this)) {
        const auto &axisList = derivedProjCRS->coordinateSystem()->axisList();
        auto cs =
            axisList.size() == 2
                ? cs::CartesianCS::create(util::PropertyMap(), axisList[1],
                                          axisList[0])
                : cs::CartesianCS::create(util::PropertyMap(), axisList[1],
                                          axisList[0], axisList[2]);
        return util::nn_static_pointer_cast<CRS>(DerivedProjectedCRS::create(
            createProperties(), derivedProjCRS->baseCRS(),
            derivedProjCRS->derivingConversion(), cs));
    }

    throw util::UnsupportedOperationException(
        "axis order reversal not supported on this type of CRS");
}

// ---------------------------------------------------------------------------

CRSNNPtr CRS::normalizeForVisualization() const {

    if (const CompoundCRS *compoundCRS =
            dynamic_cast<const CompoundCRS *>(this)) {
        const auto &comps = compoundCRS->componentReferenceSystems();
        if (!comps.empty() &&
            comps[0]->mustAxisOrderBeSwitchedForVisualization()) {
            return applyAxisOrderReversal(NORMALIZED_AXIS_ORDER_SUFFIX_STR);
        }
    }

    if (const GeographicCRS *geogCRS =
            dynamic_cast<const GeographicCRS *>(this)) {
        const auto &axisList = geogCRS->coordinateSystem()->axisList();
        if (mustAxisOrderBeSwitchedForVisualizationInternal(axisList)) {
            return applyAxisOrderReversal(NORMALIZED_AXIS_ORDER_SUFFIX_STR);
        }
    }

    if (const ProjectedCRS *projCRS =
            dynamic_cast<const ProjectedCRS *>(this)) {
        const auto &axisList = projCRS->coordinateSystem()->axisList();
        if (mustAxisOrderBeSwitchedForVisualizationInternal(axisList)) {
            return applyAxisOrderReversal(NORMALIZED_AXIS_ORDER_SUFFIX_STR);
        }
    }

    if (const DerivedProjectedCRS *derivedProjCRS =
            dynamic_cast<const DerivedProjectedCRS *>(this)) {
        const auto &axisList = derivedProjCRS->coordinateSystem()->axisList();
        if (mustAxisOrderBeSwitchedForVisualizationInternal(axisList)) {
            return applyAxisOrderReversal(NORMALIZED_AXIS_ORDER_SUFFIX_STR);
        }
    }

    if (const BoundCRS *boundCRS = dynamic_cast<const BoundCRS *>(this)) {
        auto baseNormCRS = boundCRS->baseCRS()->normalizeForVisualization();
        return BoundCRS::create(baseNormCRS, boundCRS->hubCRS(),
                                boundCRS->transformation());
    }

    return NN_NO_CHECK(
        std::static_pointer_cast<CRS>(shared_from_this().as_nullable()));
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Identify the CRS with reference CRSs.
 *
 * The candidate CRSs are either hard-coded, or looked in the database when
 * authorityFactory is not null.
 *
 * Note that the implementation uses a set of heuristics to have a good
 * compromise of successful identifications over execution time. It might miss
 * legitimate matches in some circumstances.
 *
 * The method returns a list of matching reference CRS, and the percentage
 * (0-100) of confidence in the match. The list is sorted by decreasing
 * confidence.
 * <ul>
 * <li>100% means that the name of the reference entry
 * perfectly matches the CRS name, and both are equivalent. In which case a
 * single result is returned.
 * Note: in the case of a GeographicCRS whose axis
 * order is implicit in the input definition (for example ESRI WKT), then axis
 * order is ignored for the purpose of identification. That is the CRS built
 * from
 * GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],
 * PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]
 * will be identified to EPSG:4326, but will not pass a
 * isEquivalentTo(EPSG_4326, util::IComparable::Criterion::EQUIVALENT) test,
 * but rather isEquivalentTo(EPSG_4326,
 * util::IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS)
 * </li>
 * <li>90% means that CRS are equivalent, but the names are not exactly the
 * same.</li>
 * <li>70% means that CRS are equivalent), but the names do not match at
 * all.</li>
 * <li>25% means that the CRS are not equivalent, but there is some similarity
 * in
 * the names.</li>
 * </ul>
 * Other confidence values may be returned by some specialized implementations.
 *
 * This is implemented for GeodeticCRS, ProjectedCRS, VerticalCRS and
 * CompoundCRS.
 *
 * @param authorityFactory Authority factory (or null, but degraded
 * functionality)
 * @return a list of matching reference CRS, and the percentage (0-100) of
 * confidence in the match.
 */
std::list<std::pair<CRSNNPtr, int>>
CRS::identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    return _identify(authorityFactory);
}

// ---------------------------------------------------------------------------

/** \brief Return CRSs that are non-deprecated substitutes for the current CRS.
 */
std::list<CRSNNPtr>
CRS::getNonDeprecated(const io::DatabaseContextNNPtr &dbContext) const {
    std::list<CRSNNPtr> res;
    const auto &l_identifiers = identifiers();
    if (l_identifiers.empty()) {
        return res;
    }
    const char *tableName = nullptr;
    if (dynamic_cast<const GeodeticCRS *>(this)) {
        tableName = "geodetic_crs";
    } else if (dynamic_cast<const ProjectedCRS *>(this)) {
        tableName = "projected_crs";
    } else if (dynamic_cast<const VerticalCRS *>(this)) {
        tableName = "vertical_crs";
    } else if (dynamic_cast<const CompoundCRS *>(this)) {
        tableName = "compound_crs";
    }
    if (!tableName) {
        return res;
    }
    const auto &id = l_identifiers[0];
    auto tmpRes =
        dbContext->getNonDeprecated(tableName, *(id->codeSpace()), id->code());
    for (const auto &pair : tmpRes) {
        res.emplace_back(io::AuthorityFactory::create(dbContext, pair.first)
                             ->createCoordinateReferenceSystem(pair.second));
    }
    return res;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

/** \brief Return the authority name to which this object is registered, or
 * has an indirect provenance.
 *
 * Typically this method called on EPSG:4269 (NAD83) promoted to 3D will return
 * "EPSG".
 *
 * Returns empty string if more than an authority or no originating authority is
 * found.
 */
std::string CRS::getOriginatingAuthName() const {
    const auto &ids = identifiers();
    if (ids.size() == 1) {
        return *(ids[0]->codeSpace());
    }
    if (ids.size() > 1) {
        return std::string();
    }
    const auto &l_remarks = remarks();
    if (starts_with(l_remarks, PROMOTED_TO_3D_PRELUDE)) {
        const auto pos = l_remarks.find(':');
        if (pos != std::string::npos) {
            return l_remarks.substr(strlen(PROMOTED_TO_3D_PRELUDE),
                                    pos - strlen(PROMOTED_TO_3D_PRELUDE));
        }
    }
    return std::string();
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return a variant of this CRS "promoted" to a 3D one, if not already
 * the case.
 *
 * The new axis will be ellipsoidal height, oriented upwards, and with metre
 * units.
 *
 * @param newName Name of the new CRS. If empty, nameStr() will be used.
 * @param dbContext Database context to look for potentially already registered
 *                  3D CRS. May be nullptr.
 * @return a new CRS promoted to 3D, or the current one if already 3D or not
 * applicable.
 * @since 6.3
 */
CRSNNPtr CRS::promoteTo3D(const std::string &newName,
                          const io::DatabaseContextPtr &dbContext) const {
    auto upAxis = cs::CoordinateSystemAxis::create(
        util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                cs::AxisName::Ellipsoidal_height),
        cs::AxisAbbreviation::h, cs::AxisDirection::UP,
        common::UnitOfMeasure::METRE);
    return promoteTo3D(newName, dbContext, upAxis);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

CRSNNPtr CRS::promoteTo3D(const std::string &newName,
                          const io::DatabaseContextPtr &dbContext,
                          const cs::CoordinateSystemAxisNNPtr
                              &verticalAxisIfNotAlreadyPresent) const {

    const auto createProperties = [this, &newName]() {
        auto props =
            util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                    !newName.empty() ? newName : nameStr());
        const auto &l_domains = domains();
        if (!l_domains.empty()) {
            auto array = util::ArrayOfBaseObject::create();
            for (const auto &domain : l_domains) {
                auto extent = domain->domainOfValidity();
                if (extent) {
                    // Propagate only the extent, not the scope, as it might
                    // imply more that we can guarantee with the promotion to
                    // 3D.
                    auto newDomain = common::ObjectDomain::create(
                        util::optional<std::string>(), extent);
                    array->add(newDomain);
                }
            }
            if (!array->empty()) {
                props.set(common::ObjectUsage::OBJECT_DOMAIN_KEY, array);
            }
        }
        const auto &l_identifiers = identifiers();
        const auto &l_remarks = remarks();
        if (l_identifiers.size() == 1) {
            std::string remarks(PROMOTED_TO_3D_PRELUDE);
            remarks += *(l_identifiers[0]->codeSpace());
            remarks += ':';
            remarks += l_identifiers[0]->code();
            if (!l_remarks.empty()) {
                remarks += ". ";
                remarks += l_remarks;
            }
            props.set(common::IdentifiedObject::REMARKS_KEY, remarks);
        } else if (!l_remarks.empty()) {
            props.set(common::IdentifiedObject::REMARKS_KEY, l_remarks);
        }
        return props;
    };

    if (auto derivedGeogCRS =
            dynamic_cast<const DerivedGeographicCRS *>(this)) {
        const auto &axisList = derivedGeogCRS->coordinateSystem()->axisList();
        if (axisList.size() == 2) {
            auto cs = cs::EllipsoidalCS::create(
                util::PropertyMap(), axisList[0], axisList[1],
                verticalAxisIfNotAlreadyPresent);
            auto baseGeog3DCRS = util::nn_dynamic_pointer_cast<GeodeticCRS>(
                derivedGeogCRS->baseCRS()->promoteTo3D(
                    std::string(), dbContext, verticalAxisIfNotAlreadyPresent));
            return util::nn_static_pointer_cast<CRS>(
                DerivedGeographicCRS::create(
                    createProperties(),
                    NN_CHECK_THROW(std::move(baseGeog3DCRS)),
                    derivedGeogCRS->derivingConversion(), std::move(cs)));
        }
    }

    else if (auto derivedProjCRS =
                 dynamic_cast<const DerivedProjectedCRS *>(this)) {
        const auto &axisList = derivedProjCRS->coordinateSystem()->axisList();
        if (axisList.size() == 2) {
            auto cs = cs::CartesianCS::create(util::PropertyMap(), axisList[0],
                                              axisList[1],
                                              verticalAxisIfNotAlreadyPresent);
            auto baseProj3DCRS = util::nn_dynamic_pointer_cast<ProjectedCRS>(
                derivedProjCRS->baseCRS()->promoteTo3D(
                    std::string(), dbContext, verticalAxisIfNotAlreadyPresent));
            return util::nn_static_pointer_cast<CRS>(
                DerivedProjectedCRS::create(
                    createProperties(),
                    NN_CHECK_THROW(std::move(baseProj3DCRS)),
                    derivedProjCRS->derivingConversion(), std::move(cs)));
        }
    }

    else if (auto geogCRS = dynamic_cast<const GeographicCRS *>(this)) {
        const auto &axisList = geogCRS->coordinateSystem()->axisList();
        if (axisList.size() == 2) {
            const auto &l_identifiers = identifiers();
            // First check if there is a Geographic 3D CRS in the database
            // of the same name.
            // This is the common practice in the EPSG dataset.
            if (dbContext && l_identifiers.size() == 1) {
                auto authFactory = io::AuthorityFactory::create(
                    NN_NO_CHECK(dbContext), *(l_identifiers[0]->codeSpace()));
                auto res = authFactory->createObjectsFromName(
                    nameStr(),
                    {io::AuthorityFactory::ObjectType::GEOGRAPHIC_3D_CRS},
                    false);
                if (!res.empty()) {
                    const auto &firstRes = res.front();
                    const auto firstResGeog =
                        dynamic_cast<GeographicCRS *>(firstRes.get());
                    const auto &firstResAxisList =
                        firstResGeog->coordinateSystem()->axisList();
                    if (firstResAxisList[2]->_isEquivalentTo(
                            verticalAxisIfNotAlreadyPresent.get(),
                            util::IComparable::Criterion::EQUIVALENT) &&
                        geogCRS->is2DPartOf3D(NN_NO_CHECK(firstResGeog),
                                              dbContext)) {
                        return NN_NO_CHECK(
                            util::nn_dynamic_pointer_cast<CRS>(firstRes));
                    }
                }
            }

            auto cs = cs::EllipsoidalCS::create(
                util::PropertyMap(), axisList[0], axisList[1],
                verticalAxisIfNotAlreadyPresent);
            return util::nn_static_pointer_cast<CRS>(
                GeographicCRS::create(createProperties(), geogCRS->datum(),
                                      geogCRS->datumEnsemble(), std::move(cs)));
        }
    }

    else if (auto projCRS = dynamic_cast<const ProjectedCRS *>(this)) {
        const auto &axisList = projCRS->coordinateSystem()->axisList();
        if (axisList.size() == 2) {
            auto base3DCRS =
                projCRS->baseCRS()->promoteTo3D(std::string(), dbContext);
            auto cs = cs::CartesianCS::create(util::PropertyMap(), axisList[0],
                                              axisList[1],
                                              verticalAxisIfNotAlreadyPresent);
            return util::nn_static_pointer_cast<CRS>(ProjectedCRS::create(
                createProperties(),
                NN_NO_CHECK(
                    util::nn_dynamic_pointer_cast<GeodeticCRS>(base3DCRS)),
                projCRS->derivingConversion(), std::move(cs)));
        }
    }

    else if (auto boundCRS = dynamic_cast<const BoundCRS *>(this)) {
        auto base3DCRS = boundCRS->baseCRS()->promoteTo3D(
            newName, dbContext, verticalAxisIfNotAlreadyPresent);
        auto transf = boundCRS->transformation();
        if (!transf->getTOWGS84Parameters(false).empty()) {
            return BoundCRS::create(
                createProperties(), base3DCRS,
                boundCRS->hubCRS()->promoteTo3D(std::string(), dbContext),
                transf->promoteTo3D(std::string(), dbContext));
        } else {
            return BoundCRS::create(base3DCRS, boundCRS->hubCRS(),
                                    std::move(transf));
        }
    }

    return NN_NO_CHECK(
        std::static_pointer_cast<CRS>(shared_from_this().as_nullable()));
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return a variant of this CRS "demoted" to a 2D one, if not already
 * the case.
 *
 *
 * @param newName Name of the new CRS. If empty, nameStr() will be used.
 * @param dbContext Database context to look for potentially already registered
 *                  2D CRS. May be nullptr.
 * @return a new CRS demoted to 2D, or the current one if already 2D or not
 * applicable.
 * @since 6.3
 */
CRSNNPtr CRS::demoteTo2D(const std::string &newName,
                         const io::DatabaseContextPtr &dbContext) const {

    if (auto derivedGeogCRS =
            dynamic_cast<const DerivedGeographicCRS *>(this)) {
        return derivedGeogCRS->demoteTo2D(newName, dbContext);
    }

    else if (auto derivedProjCRS =
                 dynamic_cast<const DerivedProjectedCRS *>(this)) {
        return derivedProjCRS->demoteTo2D(newName, dbContext);
    }

    else if (auto geogCRS = dynamic_cast<const GeographicCRS *>(this)) {
        return geogCRS->demoteTo2D(newName, dbContext);
    }

    else if (auto projCRS = dynamic_cast<const ProjectedCRS *>(this)) {
        return projCRS->demoteTo2D(newName, dbContext);
    }

    else if (auto boundCRS = dynamic_cast<const BoundCRS *>(this)) {
        auto base2DCRS = boundCRS->baseCRS()->demoteTo2D(newName, dbContext);
        auto transf = boundCRS->transformation();
        if (!transf->getTOWGS84Parameters(false).empty()) {
            return BoundCRS::create(
                base2DCRS,
                boundCRS->hubCRS()->demoteTo2D(std::string(), dbContext),
                transf->demoteTo2D(std::string(), dbContext));
        } else {
            return BoundCRS::create(base2DCRS, boundCRS->hubCRS(), transf);
        }
    }

    else if (auto compoundCRS = dynamic_cast<const CompoundCRS *>(this)) {
        const auto &components = compoundCRS->componentReferenceSystems();
        if (components.size() >= 2) {
            return components[0];
        }
    }

    return NN_NO_CHECK(
        std::static_pointer_cast<CRS>(shared_from_this().as_nullable()));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
CRS::_identify(const io::AuthorityFactoryPtr &) const {
    return {};
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct SingleCRS::Private {
    datum::DatumPtr datum{};
    datum::DatumEnsemblePtr datumEnsemble{};
    cs::CoordinateSystemNNPtr coordinateSystem;

    Private(const datum::DatumPtr &datumIn,
            const datum::DatumEnsemblePtr &datumEnsembleIn,
            const cs::CoordinateSystemNNPtr &csIn)
        : datum(datumIn), datumEnsemble(datumEnsembleIn),
          coordinateSystem(csIn) {
        if ((datum ? 1 : 0) + (datumEnsemble ? 1 : 0) != 1) {
            throw util::Exception("datum or datumEnsemble should be set");
        }
    }
};
//! @endcond

// ---------------------------------------------------------------------------

SingleCRS::SingleCRS(const datum::DatumPtr &datumIn,
                     const datum::DatumEnsemblePtr &datumEnsembleIn,
                     const cs::CoordinateSystemNNPtr &csIn)
    : d(std::make_unique<Private>(datumIn, datumEnsembleIn, csIn)) {}

// ---------------------------------------------------------------------------

SingleCRS::SingleCRS(const SingleCRS &other)
    : CRS(other), d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
SingleCRS::~SingleCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the datum::Datum associated with the CRS.
 *
 * This might be null, in which case datumEnsemble() return will not be null.
 *
 * @return a Datum that might be null.
 */
const datum::DatumPtr &SingleCRS::datum() PROJ_PURE_DEFN { return d->datum; }

// ---------------------------------------------------------------------------

/** \brief Return the datum::DatumEnsemble associated with the CRS.
 *
 * This might be null, in which case datum() return will not be null.
 *
 * @return a DatumEnsemble that might be null.
 */
const datum::DatumEnsemblePtr &SingleCRS::datumEnsemble() PROJ_PURE_DEFN {
    return d->datumEnsemble;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
/** \brief Return the real datum or a synthesized one if a datumEnsemble.
 */
const datum::DatumNNPtr
SingleCRS::datumNonNull(const io::DatabaseContextPtr &dbContext) const {
    return d->datum ? NN_NO_CHECK(d->datum)
                    : d->datumEnsemble->asDatum(dbContext);
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the cs::CoordinateSystem associated with the CRS.
 *
 * @return a CoordinateSystem.
 */
const cs::CoordinateSystemNNPtr &SingleCRS::coordinateSystem() PROJ_PURE_DEFN {
    return d->coordinateSystem;
}

// ---------------------------------------------------------------------------

bool SingleCRS::baseIsEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherSingleCRS = dynamic_cast<const SingleCRS *>(other);
    if (otherSingleCRS == nullptr ||
        (criterion == util::IComparable::Criterion::STRICT &&
         !ObjectUsage::_isEquivalentTo(other, criterion, dbContext))) {
        return false;
    }

    // Check datum
    const auto &thisDatum = d->datum;
    const auto &otherDatum = otherSingleCRS->d->datum;
    const auto &thisDatumEnsemble = d->datumEnsemble;
    const auto &otherDatumEnsemble = otherSingleCRS->d->datumEnsemble;
    if (thisDatum && otherDatum) {
        if (!thisDatum->_isEquivalentTo(otherDatum.get(), criterion,
                                        dbContext)) {
            return false;
        }
    } else if (thisDatumEnsemble && otherDatumEnsemble) {
        if (!thisDatumEnsemble->_isEquivalentTo(otherDatumEnsemble.get(),
                                                criterion, dbContext)) {
            return false;
        }
    }

    if (criterion == util::IComparable::Criterion::STRICT) {
        if ((thisDatum != nullptr) ^ (otherDatum != nullptr)) {
            return false;
        }
        if ((thisDatumEnsemble != nullptr) ^ (otherDatumEnsemble != nullptr)) {
            return false;
        }
    } else {
        if (!datumNonNull(dbContext)->_isEquivalentTo(
                otherSingleCRS->datumNonNull(dbContext).get(), criterion,
                dbContext)) {
            return false;
        }
    }

    // Check coordinate system
    if (!(d->coordinateSystem->_isEquivalentTo(
            otherSingleCRS->d->coordinateSystem.get(), criterion, dbContext))) {
        return false;
    }

    // Now compare PROJ4 extensions

    const auto &thisProj4 = getExtensionProj4();
    const auto &otherProj4 = otherSingleCRS->getExtensionProj4();

    if (thisProj4.empty() && otherProj4.empty()) {
        return true;
    }

    if (!(thisProj4.empty() ^ otherProj4.empty())) {
        return true;
    }

    // Asks for a "normalized" output during toString(), aimed at comparing two
    // strings for equivalence.
    auto formatter1 = io::PROJStringFormatter::create();
    formatter1->setNormalizeOutput();
    formatter1->ingestPROJString(thisProj4);

    auto formatter2 = io::PROJStringFormatter::create();
    formatter2->setNormalizeOutput();
    formatter2->ingestPROJString(otherProj4);

    return formatter1->toString() == formatter2->toString();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void SingleCRS::exportDatumOrDatumEnsembleToWkt(
    io::WKTFormatter *formatter) const // throw(io::FormattingException)
{
    const auto &l_datum = d->datum;
    if (l_datum) {
        l_datum->_exportToWKT(formatter);
    } else {
        const auto &l_datumEnsemble = d->datumEnsemble;
        assert(l_datumEnsemble);
        l_datumEnsemble->_exportToWKT(formatter);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct GeodeticCRS::Private {
    std::vector<operation::PointMotionOperationNNPtr> velocityModel{};
    datum::GeodeticReferenceFramePtr datum_;

    explicit Private(const datum::GeodeticReferenceFramePtr &datumIn)
        : datum_(datumIn) {}
};

// ---------------------------------------------------------------------------

static const datum::DatumEnsemblePtr &
checkEnsembleForGeodeticCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                            const datum::DatumEnsemblePtr &ensemble) {
    const char *msg = "One of Datum or DatumEnsemble should be defined";
    if (datumIn) {
        if (!ensemble) {
            return ensemble;
        }
        msg = "Datum and DatumEnsemble should not be defined";
    } else if (ensemble) {
        const auto &datums = ensemble->datums();
        assert(!datums.empty());
        auto grfFirst =
            dynamic_cast<datum::GeodeticReferenceFrame *>(datums[0].get());
        if (grfFirst) {
            return ensemble;
        }
        msg = "Ensemble should contain GeodeticReferenceFrame";
    }
    throw util::Exception(msg);
}

//! @endcond

// ---------------------------------------------------------------------------

GeodeticCRS::GeodeticCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                         const datum::DatumEnsemblePtr &datumEnsembleIn,
                         const cs::EllipsoidalCSNNPtr &csIn)
    : SingleCRS(datumIn, checkEnsembleForGeodeticCRS(datumIn, datumEnsembleIn),
                csIn),
      d(std::make_unique<Private>(datumIn)) {}

// ---------------------------------------------------------------------------

GeodeticCRS::GeodeticCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                         const datum::DatumEnsemblePtr &datumEnsembleIn,
                         const cs::SphericalCSNNPtr &csIn)
    : SingleCRS(datumIn, checkEnsembleForGeodeticCRS(datumIn, datumEnsembleIn),
                csIn),
      d(std::make_unique<Private>(datumIn)) {}

// ---------------------------------------------------------------------------

GeodeticCRS::GeodeticCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                         const datum::DatumEnsemblePtr &datumEnsembleIn,
                         const cs::CartesianCSNNPtr &csIn)
    : SingleCRS(datumIn, checkEnsembleForGeodeticCRS(datumIn, datumEnsembleIn),
                csIn),
      d(std::make_unique<Private>(datumIn)) {}

// ---------------------------------------------------------------------------

GeodeticCRS::GeodeticCRS(const GeodeticCRS &other)
    : SingleCRS(other), d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
GeodeticCRS::~GeodeticCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

CRSNNPtr GeodeticCRS::_shallowClone() const {
    auto crs(GeodeticCRS::nn_make_shared<GeodeticCRS>(*this));
    crs->assignSelf(crs);
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the datum::GeodeticReferenceFrame associated with the CRS.
 *
 * @return a GeodeticReferenceFrame or null (in which case datumEnsemble()
 * should return a non-null pointer.)
 */
const datum::GeodeticReferenceFramePtr &GeodeticCRS::datum() PROJ_PURE_DEFN {
    return d->datum_;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
/** \brief Return the real datum or a synthesized one if a datumEnsemble.
 */
const datum::GeodeticReferenceFrameNNPtr
GeodeticCRS::datumNonNull(const io::DatabaseContextPtr &dbContext) const {
    return NN_NO_CHECK(
        d->datum_
            ? d->datum_
            : util::nn_dynamic_pointer_cast<datum::GeodeticReferenceFrame>(
                  SingleCRS::getPrivate()->datumEnsemble->asDatum(dbContext)));
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static datum::GeodeticReferenceFrame *oneDatum(const GeodeticCRS *crs) {
    const auto &l_datumEnsemble = crs->datumEnsemble();
    assert(l_datumEnsemble);
    const auto &l_datums = l_datumEnsemble->datums();
    return static_cast<datum::GeodeticReferenceFrame *>(l_datums[0].get());
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the PrimeMeridian associated with the GeodeticReferenceFrame
 * or with one of the GeodeticReferenceFrame of the datumEnsemble().
 *
 * @return the PrimeMeridian.
 */
const datum::PrimeMeridianNNPtr &GeodeticCRS::primeMeridian() PROJ_PURE_DEFN {
    if (d->datum_) {
        return d->datum_->primeMeridian();
    }
    return oneDatum(this)->primeMeridian();
}

// ---------------------------------------------------------------------------

/** \brief Return the ellipsoid associated with the GeodeticReferenceFrame
 * or with one of the GeodeticReferenceFrame of the datumEnsemble().
 *
 * @return the PrimeMeridian.
 */
const datum::EllipsoidNNPtr &GeodeticCRS::ellipsoid() PROJ_PURE_DEFN {
    if (d->datum_) {
        return d->datum_->ellipsoid();
    }
    return oneDatum(this)->ellipsoid();
}

// ---------------------------------------------------------------------------

/** \brief Return the velocity model associated with the CRS.
 *
 * @return a velocity model. might be null.
 */
const std::vector<operation::PointMotionOperationNNPtr> &
GeodeticCRS::velocityModel() PROJ_PURE_DEFN {
    return d->velocityModel;
}

// ---------------------------------------------------------------------------

/** \brief Return whether the CRS is a Cartesian geocentric one.
 *
 * A geocentric CRS is a geodetic CRS that has a Cartesian coordinate system
 * with three axis, whose direction is respectively
 * cs::AxisDirection::GEOCENTRIC_X,
 * cs::AxisDirection::GEOCENTRIC_Y and cs::AxisDirection::GEOCENTRIC_Z.
 *
 * @return true if the CRS is a geocentric CRS.
 */
bool GeodeticCRS::isGeocentric() PROJ_PURE_DEFN {
    const auto &cs = coordinateSystem();
    const auto &axisList = cs->axisList();
    return axisList.size() == 3 &&
           dynamic_cast<cs::CartesianCS *>(cs.get()) != nullptr &&
           &axisList[0]->direction() == &cs::AxisDirection::GEOCENTRIC_X &&
           &axisList[1]->direction() == &cs::AxisDirection::GEOCENTRIC_Y &&
           &axisList[2]->direction() == &cs::AxisDirection::GEOCENTRIC_Z;
}

// ---------------------------------------------------------------------------

/** \brief Return whether the CRS is a Spherical planetocentric one.
 *
 * A Spherical planetocentric CRS is a geodetic CRS that has a spherical
 * (angular) coordinate system with 2 axis, which represent geocentric latitude/
 * longitude or longitude/geocentric latitude.
 *
 * Such CRS are typically used in use case that apply to non-Earth bodies.
 *
 * @return true if the CRS is a Spherical planetocentric CRS.
 *
 * @since 8.2
 */
bool GeodeticCRS::isSphericalPlanetocentric() PROJ_PURE_DEFN {
    const auto &cs = coordinateSystem();
    const auto &axisList = cs->axisList();
    return axisList.size() == 2 &&
           dynamic_cast<cs::SphericalCS *>(cs.get()) != nullptr &&
           ((ci_equal(axisList[0]->nameStr(), "planetocentric latitude") &&
             ci_equal(axisList[1]->nameStr(), "planetocentric longitude")) ||
            (ci_equal(axisList[0]->nameStr(), "planetocentric longitude") &&
             ci_equal(axisList[1]->nameStr(), "planetocentric latitude")));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeodeticCRS from a datum::GeodeticReferenceFrame and a
 * cs::SphericalCS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datum The datum of the CRS.
 * @param cs a SphericalCS.
 * @return new GeodeticCRS.
 */
GeodeticCRSNNPtr
GeodeticCRS::create(const util::PropertyMap &properties,
                    const datum::GeodeticReferenceFrameNNPtr &datum,
                    const cs::SphericalCSNNPtr &cs) {
    return create(properties, datum.as_nullable(), nullptr, cs);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeodeticCRS from a datum::GeodeticReferenceFrame or
 * datum::DatumEnsemble and a cs::SphericalCS.
 *
 * One and only one of datum or datumEnsemble should be set to a non-null value.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datum The datum of the CRS, or nullptr
 * @param datumEnsemble The datum ensemble of the CRS, or nullptr.
 * @param cs a SphericalCS.
 * @return new GeodeticCRS.
 */
GeodeticCRSNNPtr
GeodeticCRS::create(const util::PropertyMap &properties,
                    const datum::GeodeticReferenceFramePtr &datum,
                    const datum::DatumEnsemblePtr &datumEnsemble,
                    const cs::SphericalCSNNPtr &cs) {
    auto crs(
        GeodeticCRS::nn_make_shared<GeodeticCRS>(datum, datumEnsemble, cs));
    crs->assignSelf(crs);
    crs->setProperties(properties);

    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeodeticCRS from a datum::GeodeticReferenceFrame and a
 * cs::CartesianCS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datum The datum of the CRS.
 * @param cs a CartesianCS.
 * @return new GeodeticCRS.
 */
GeodeticCRSNNPtr
GeodeticCRS::create(const util::PropertyMap &properties,
                    const datum::GeodeticReferenceFrameNNPtr &datum,
                    const cs::CartesianCSNNPtr &cs) {
    return create(properties, datum.as_nullable(), nullptr, cs);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeodeticCRS from a datum::GeodeticReferenceFrame or
 * datum::DatumEnsemble and a cs::CartesianCS.
 *
 * One and only one of datum or datumEnsemble should be set to a non-null value.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datum The datum of the CRS, or nullptr
 * @param datumEnsemble The datum ensemble of the CRS, or nullptr.
 * @param cs a CartesianCS
 * @return new GeodeticCRS.
 */
GeodeticCRSNNPtr
GeodeticCRS::create(const util::PropertyMap &properties,
                    const datum::GeodeticReferenceFramePtr &datum,
                    const datum::DatumEnsemblePtr &datumEnsemble,
                    const cs::CartesianCSNNPtr &cs) {
    auto crs(
        GeodeticCRS::nn_make_shared<GeodeticCRS>(datum, datumEnsemble, cs));
    crs->assignSelf(crs);
    crs->setProperties(properties);

    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

// Try to format a Geographic/ProjectedCRS 3D CRS as a
// GEOGCS[]/PROJCS[],VERTCS[...,DATUM[],...] if we find corresponding objects
static bool exportAsESRIWktCompoundCRSWithEllipsoidalHeight(
    const CRS *self, const GeodeticCRS *geodCRS, io::WKTFormatter *formatter) {
    const auto &dbContext = formatter->databaseContext();
    if (!dbContext) {
        return false;
    }
    const auto l_datum = geodCRS->datumNonNull(formatter->databaseContext());
    auto l_esri_name = dbContext->getAliasFromOfficialName(
        l_datum->nameStr(), "geodetic_datum", "ESRI");
    if (l_esri_name.empty()) {
        l_esri_name = l_datum->nameStr();
    }
    auto authFactory =
        io::AuthorityFactory::create(NN_NO_CHECK(dbContext), std::string());
    auto list = authFactory->createObjectsFromName(
        l_esri_name,
        {io::AuthorityFactory::ObjectType::GEODETIC_REFERENCE_FRAME},
        false /* approximate=false*/);
    if (list.empty()) {
        return false;
    }
    auto gdatum = util::nn_dynamic_pointer_cast<datum::Datum>(list.front());
    if (gdatum == nullptr || gdatum->identifiers().empty()) {
        return false;
    }
    const auto &gdatum_ids = gdatum->identifiers();
    auto vertCRSList = authFactory->createVerticalCRSFromDatum(
        "ESRI", "from_geogdatum_" + *gdatum_ids[0]->codeSpace() + '_' +
                    gdatum_ids[0]->code());
    self->demoteTo2D(std::string(), dbContext)->_exportToWKT(formatter);
    if (vertCRSList.size() == 1) {
        vertCRSList.front()->_exportToWKT(formatter);
    } else {
        // This will not be recognized properly by ESRI software
        // See https://github.com/OSGeo/PROJ/issues/2757

        const auto &axisList = geodCRS->coordinateSystem()->axisList();
        assert(axisList.size() == 3U);

        formatter->startNode(io::WKTConstants::VERTCS, false);
        auto vertcs_name = std::move(l_esri_name);
        if (starts_with(vertcs_name.c_str(), "GCS_"))
            vertcs_name = vertcs_name.substr(4);
        formatter->addQuotedString(vertcs_name);

        gdatum->_exportToWKT(formatter);

        // Seems to be a constant value...
        formatter->startNode(io::WKTConstants::PARAMETER, false);
        formatter->addQuotedString("Vertical_Shift");
        formatter->add(0.0);
        formatter->endNode();

        formatter->startNode(io::WKTConstants::PARAMETER, false);
        formatter->addQuotedString("Direction");
        formatter->add(
            axisList[2]->direction() == cs::AxisDirection::UP ? 1.0 : -1.0);
        formatter->endNode();

        axisList[2]->unit()._exportToWKT(formatter);
        formatter->endNode();
    }
    return true;
}

// ---------------------------------------------------------------------------

// Try to format a Geographic/ProjectedCRS 3D CRS as a
// GEOGCS[]/PROJCS[],VERTCS["Ellipsoid (metre)",DATUM["Ellipsoid",2002],...]
static void exportAsWKT1CompoundCRSWithEllipsoidalHeight(
    const CRSNNPtr &base2DCRS,
    const cs::CoordinateSystemAxisNNPtr &verticalAxis,
    io::WKTFormatter *formatter) {
    std::string verticalCRSName = "Ellipsoid (";
    verticalCRSName += verticalAxis->unit().name();
    verticalCRSName += ')';
    auto vertDatum = datum::VerticalReferenceFrame::create(
        util::PropertyMap()
            .set(common::IdentifiedObject::NAME_KEY, "Ellipsoid")
            .set("VERT_DATUM_TYPE", "2002"));
    auto vertCRS = VerticalCRS::create(
        util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                verticalCRSName),
        vertDatum.as_nullable(), nullptr,
        cs::VerticalCS::create(util::PropertyMap(), verticalAxis));
    formatter->startNode(io::WKTConstants::COMPD_CS, false);
    formatter->addQuotedString(base2DCRS->nameStr() + " + " + verticalCRSName);
    base2DCRS->_exportToWKT(formatter);
    vertCRS->_exportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeodeticCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    const bool isGeographic =
        dynamic_cast<const GeographicCRS *>(this) != nullptr;

    const auto &cs = coordinateSystem();
    const auto &axisList = cs->axisList();
    const bool isGeographic3D = isGeographic && axisList.size() == 3;
    const auto oldAxisOutputRule = formatter->outputAxis();
    std::string l_name = nameStr();
    const auto &dbContext = formatter->databaseContext();

    const bool isESRIExport = !isWKT2 && formatter->useESRIDialect();
    const auto &l_identifiers = identifiers();

    if (isESRIExport && axisList.size() == 3) {
        if (!isGeographic) {
            io::FormattingException::Throw(
                "Geocentric CRS not supported in WKT1_ESRI");
        }
        if (!formatter->isAllowedLINUNITNode()) {
            // Try to format the Geographic 3D CRS as a
            // GEOGCS[],VERTCS[...,DATUM[]] if we find corresponding objects
            if (dbContext) {
                if (exportAsESRIWktCompoundCRSWithEllipsoidalHeight(
                        this, this, formatter)) {
                    return;
                }
            }
            io::FormattingException::Throw(
                "Cannot export this Geographic 3D CRS in WKT1_ESRI");
        }
    }

    if (!isWKT2 && !isESRIExport && formatter->isStrict() && isGeographic &&
        axisList.size() == 3 &&
        oldAxisOutputRule != io::WKTFormatter::OutputAxisRule::NO) {

        auto geogCRS2D = demoteTo2D(std::string(), dbContext);
        if (dbContext) {
            const auto res = geogCRS2D->identify(io::AuthorityFactory::create(
                NN_NO_CHECK(dbContext), metadata::Identifier::EPSG));
            if (res.size() == 1) {
                const auto &front = res.front();
                if (front.second == 100) {
                    geogCRS2D = front.first;
                }
            }
        }

        if (CRS::getPrivate()->allowNonConformantWKT1Export_) {
            formatter->startNode(io::WKTConstants::COMPD_CS, false);
            formatter->addQuotedString(l_name + " + " + l_name);
            geogCRS2D->_exportToWKT(formatter);
            const std::vector<double> oldTOWGSParameters(
                formatter->getTOWGS84Parameters());
            formatter->setTOWGS84Parameters({});
            geogCRS2D->_exportToWKT(formatter);
            formatter->setTOWGS84Parameters(oldTOWGSParameters);
            formatter->endNode();
            return;
        }

        auto &originalCompoundCRS = CRS::getPrivate()->originalCompoundCRS_;
        if (originalCompoundCRS) {
            originalCompoundCRS->_exportToWKT(formatter);
            return;
        }

        if (formatter->isAllowedEllipsoidalHeightAsVerticalCRS()) {
            exportAsWKT1CompoundCRSWithEllipsoidalHeight(geogCRS2D, axisList[2],
                                                         formatter);
            return;
        }

        io::FormattingException::Throw(
            "WKT1 does not support Geographic 3D CRS.");
    }

    formatter->startNode(isWKT2
                             ? ((formatter->use2019Keywords() && isGeographic)
                                    ? io::WKTConstants::GEOGCRS
                                    : io::WKTConstants::GEODCRS)
                         : isGeocentric() ? io::WKTConstants::GEOCCS
                                          : io::WKTConstants::GEOGCS,
                         !l_identifiers.empty());

    if (isESRIExport) {
        std::string l_esri_name;
        if (l_name == "WGS 84") {
            l_esri_name = isGeographic3D ? "WGS_1984_3D" : "GCS_WGS_1984";
        } else {
            if (dbContext) {
                const auto tableName =
                    isGeographic3D ? "geographic_3D_crs" : "geodetic_crs";
                if (!l_identifiers.empty()) {
                    // Try to find the ESRI alias from the CRS identified by its
                    // id
                    const auto aliases =
                        dbContext->getAliases(*(l_identifiers[0]->codeSpace()),
                                              l_identifiers[0]->code(),
                                              std::string(), // officialName,
                                              tableName, "ESRI");
                    if (aliases.size() == 1)
                        l_esri_name = aliases.front();
                }
                if (l_esri_name.empty()) {
                    // Then find the ESRI alias from the CRS name
                    l_esri_name = dbContext->getAliasFromOfficialName(
                        l_name, tableName, "ESRI");
                }
                if (l_esri_name.empty()) {
                    // Then try to build an ESRI CRS from the CRS name, and if
                    // there's one, the ESRI name is the CRS name
                    auto authFactory = io::AuthorityFactory::create(
                        NN_NO_CHECK(dbContext), "ESRI");
                    const bool found = authFactory
                                           ->createObjectsFromName(
                                               l_name,
                                               {io::AuthorityFactory::
                                                    ObjectType::GEODETIC_CRS},
                                               false // approximateMatch
                                               )
                                           .size() == 1;
                    if (found)
                        l_esri_name = l_name;
                }
            }
            if (l_esri_name.empty()) {
                // For now, there's no ESRI alias for this CRS. Fallback to
                // ETRS89
                if (l_name == "ETRS89-NOR [EUREF89]") {
                    l_esri_name = "GCS_ETRS_1989";
                } else {
                    l_esri_name = io::WKTFormatter::morphNameToESRI(l_name);
                    if (!starts_with(l_esri_name, "GCS_")) {
                        l_esri_name = "GCS_" + l_esri_name;
                    }
                }
            }
        }
        const std::string &l_esri_name_ref(l_esri_name);
        l_name = l_esri_name_ref;
    } else if (!isWKT2 && isDeprecated()) {
        l_name += " (deprecated)";
    }
    formatter->addQuotedString(l_name);

    const auto &unit = axisList[0]->unit();
    formatter->pushAxisAngularUnit(common::UnitOfMeasure::create(unit));
    exportDatumOrDatumEnsembleToWkt(formatter);
    primeMeridian()->_exportToWKT(formatter);
    formatter->popAxisAngularUnit();
    if (!isWKT2) {
        unit._exportToWKT(formatter);
    }
    if (isGeographic3D && isESRIExport) {
        axisList[2]->unit()._exportToWKT(formatter, io::WKTConstants::LINUNIT);
    }

    if (oldAxisOutputRule ==
            io::WKTFormatter::OutputAxisRule::WKT1_GDAL_EPSG_STYLE &&
        isGeocentric()) {
        formatter->setOutputAxis(io::WKTFormatter::OutputAxisRule::YES);
    }
    cs->_exportToWKT(formatter);
    formatter->setOutputAxis(oldAxisOutputRule);

    ObjectUsage::baseExportToWKT(formatter);

    if (!isWKT2 && !isESRIExport) {
        const auto &extensionProj4 = CRS::getPrivate()->extensionProj4_;
        if (!extensionProj4.empty()) {
            formatter->startNode(io::WKTConstants::EXTENSION, false);
            formatter->addQuotedString("PROJ4");
            formatter->addQuotedString(extensionProj4);
            formatter->endNode();
        }
    }

    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeodeticCRS::addGeocentricUnitConversionIntoPROJString(
    io::PROJStringFormatter *formatter) const {

    const auto &axisList = coordinateSystem()->axisList();
    const auto &unit = axisList[0]->unit();
    if (!unit._isEquivalentTo(common::UnitOfMeasure::METRE,
                              util::IComparable::Criterion::EQUIVALENT)) {
        if (formatter->getCRSExport()) {
            io::FormattingException::Throw(
                "GeodeticCRS::exportToPROJString() only "
                "supports metre unit");
        }
        formatter->addStep("unitconvert");
        formatter->addParam("xy_in", "m");
        formatter->addParam("z_in", "m");
        {
            auto projUnit = unit.exportToPROJString();
            if (!projUnit.empty()) {
                formatter->addParam("xy_out", projUnit);
                formatter->addParam("z_out", projUnit);
                return;
            }
        }

        const auto &toSI = unit.conversionToSI();
        formatter->addParam("xy_out", toSI);
        formatter->addParam("z_out", toSI);
    } else if (formatter->getCRSExport()) {
        formatter->addParam("units", "m");
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeodeticCRS::addAxisSwap(io::PROJStringFormatter *formatter) const {
    const auto &axisList = coordinateSystem()->axisList();

    const char *order[2] = {nullptr, nullptr};
    const char *one = "1";
    const char *two = "2";
    for (int i = 0; i < 2; i++) {
        const auto &dir = axisList[i]->direction();
        if (&dir == &cs::AxisDirection::WEST) {
            order[i] = "-1";
        } else if (&dir == &cs::AxisDirection::EAST) {
            order[i] = one;
        } else if (&dir == &cs::AxisDirection::SOUTH) {
            order[i] = "-2";
        } else if (&dir == &cs::AxisDirection::NORTH) {
            order[i] = two;
        }
    }
    if (order[0] && order[1] && (order[0] != one || order[1] != two)) {
        formatter->addStep("axisswap");
        char orderStr[10];
        snprintf(orderStr, sizeof(orderStr), "%.2s,%.2s", order[0], order[1]);
        formatter->addParam("order", orderStr);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeodeticCRS::addAngularUnitConvertAndAxisSwap(
    io::PROJStringFormatter *formatter) const {
    const auto &axisList = coordinateSystem()->axisList();

    formatter->addStep("unitconvert");
    formatter->addParam("xy_in", "rad");
    if (axisList.size() == 3 && !formatter->omitZUnitConversion()) {
        formatter->addParam("z_in", "m");
    }
    {
        const auto &unitHoriz = axisList[0]->unit();
        const auto projUnit = unitHoriz.exportToPROJString();
        if (projUnit.empty()) {
            formatter->addParam("xy_out", unitHoriz.conversionToSI());
        } else {
            formatter->addParam("xy_out", projUnit);
        }
    }
    if (axisList.size() == 3 && !formatter->omitZUnitConversion()) {
        const auto &unitZ = axisList[2]->unit();
        auto projVUnit = unitZ.exportToPROJString();
        if (projVUnit.empty()) {
            formatter->addParam("z_out", unitZ.conversionToSI());
        } else {
            formatter->addParam("z_out", projVUnit);
        }
    }

    addAxisSwap(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeodeticCRS::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(io::FormattingException)
{
    const auto &extensionProj4 = CRS::getPrivate()->extensionProj4_;
    if (!extensionProj4.empty()) {
        formatter->ingestPROJString(
            replaceAll(extensionProj4, " +type=crs", ""));
        formatter->addNoDefs(false);
        return;
    }

    if (isGeocentric()) {
        if (!formatter->getCRSExport()) {
            formatter->addStep("cart");
        } else {
            formatter->addStep("geocent");
        }

        addDatumInfoToPROJString(formatter);
        addGeocentricUnitConversionIntoPROJString(formatter);
    } else if (isSphericalPlanetocentric()) {
        if (!formatter->getCRSExport()) {

            if (!formatter->omitProjLongLatIfPossible() ||
                primeMeridian()->longitude().getSIValue() != 0.0 ||
                !ellipsoid()->isSphere() ||
                !formatter->getTOWGS84Parameters().empty() ||
                !formatter->getHDatumExtension().empty()) {
                formatter->addStep("geoc");
                addDatumInfoToPROJString(formatter);
            }

            addAngularUnitConvertAndAxisSwap(formatter);
        } else {
            io::FormattingException::Throw(
                "GeodeticCRS::exportToPROJString() not supported on spherical "
                "planetocentric coordinate systems");
            // The below code now works as input to PROJ, but I'm not sure we
            // want to propagate this, given that we got cs2cs doing conversion
            // in the wrong direction in past versions.
            /*formatter->addStep("longlat");
            formatter->addParam("geoc");

            addDatumInfoToPROJString(formatter);*/
        }
    } else {
        io::FormattingException::Throw(
            "GeodeticCRS::exportToPROJString() only "
            "supports geocentric or spherical planetocentric "
            "coordinate systems");
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeodeticCRS::addDatumInfoToPROJString(
    io::PROJStringFormatter *formatter) const // throw(io::FormattingException)
{
    const auto &TOWGS84Params = formatter->getTOWGS84Parameters();
    bool datumWritten = false;
    const auto &nadgrids = formatter->getHDatumExtension();
    const auto l_datum = datumNonNull(formatter->databaseContext());
    if (formatter->getCRSExport() && TOWGS84Params.empty() &&
        nadgrids.empty() && l_datum->nameStr() != "unknown") {
        if (l_datum->_isEquivalentTo(
                datum::GeodeticReferenceFrame::EPSG_6326.get(),
                util::IComparable::Criterion::EQUIVALENT)) {
            datumWritten = true;
            formatter->addParam("datum", "WGS84");
        } else if (l_datum->_isEquivalentTo(
                       datum::GeodeticReferenceFrame::EPSG_6267.get(),
                       util::IComparable::Criterion::EQUIVALENT)) {
            datumWritten = true;
            formatter->addParam("datum", "NAD27");
        } else if (l_datum->_isEquivalentTo(
                       datum::GeodeticReferenceFrame::EPSG_6269.get(),
                       util::IComparable::Criterion::EQUIVALENT)) {
            datumWritten = true;
            if (formatter->getLegacyCRSToCRSContext()) {
                // We do not want datum=NAD83 to cause a useless towgs84=0,0,0
                formatter->addParam("ellps", "GRS80");
            } else {
                formatter->addParam("datum", "NAD83");
            }
        }
    }
    if (!datumWritten) {
        ellipsoid()->_exportToPROJString(formatter);
        primeMeridian()->_exportToPROJString(formatter);
    }
    if (TOWGS84Params.size() == 7) {
        formatter->addParam("towgs84", TOWGS84Params);
    }
    if (!nadgrids.empty()) {
        formatter->addParam("nadgrids", nadgrids);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

void GeodeticCRS::_exportToJSONInternal(
    io::JSONFormatter *formatter,
    const char *objectName) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext(objectName, !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    const auto &l_datum(datum());
    if (l_datum) {
        writer->AddObjKey("datum");
        l_datum->_exportToJSON(formatter);
    } else {
        writer->AddObjKey("datum_ensemble");
        formatter->setOmitTypeInImmediateChild();
        datumEnsemble()->_exportToJSON(formatter);
    }

    writer->AddObjKey("coordinate_system");
    formatter->setOmitTypeInImmediateChild();
    coordinateSystem()->_exportToJSON(formatter);

    if (const auto dynamicGRF =
            dynamic_cast<datum::DynamicGeodeticReferenceFrame *>(
                l_datum.get())) {
        const auto &deformationModel = dynamicGRF->deformationModelName();
        if (deformationModel.has_value()) {
            writer->AddObjKey("deformation_models");
            auto arrayContext(writer->MakeArrayContext(false));
            auto objectContext2(formatter->MakeObjectContext(nullptr, false));
            writer->AddObjKey("name");
            writer->Add(*deformationModel);
        }
    }

    ObjectUsage::baseExportToJSON(formatter);
}

void GeodeticCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    _exportToJSONInternal(formatter, "GeodeticCRS");
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static util::IComparable::Criterion
getStandardCriterion(util::IComparable::Criterion criterion) {
    return criterion == util::IComparable::Criterion::
                            EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS
               ? util::IComparable::Criterion::EQUIVALENT
               : criterion;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool GeodeticCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    if (other == nullptr || !util::isOfExactType<GeodeticCRS>(*other)) {
        return false;
    }
    return _isEquivalentToNoTypeCheck(other, criterion, dbContext);
}

bool GeodeticCRS::_isEquivalentToNoTypeCheck(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    const auto standardCriterion = getStandardCriterion(criterion);

    // TODO test velocityModel
    return SingleCRS::baseIsEquivalentTo(other, standardCriterion, dbContext);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static util::PropertyMap createMapNameEPSGCode(const char *name, int code) {
    return util::PropertyMap()
        .set(common::IdentifiedObject::NAME_KEY, name)
        .set(metadata::Identifier::CODESPACE_KEY, metadata::Identifier::EPSG)
        .set(metadata::Identifier::CODE_KEY, code);
}
//! @endcond

// ---------------------------------------------------------------------------

GeodeticCRSNNPtr GeodeticCRS::createEPSG_4978() {
    return create(
        createMapNameEPSGCode("WGS 84", 4978),
        datum::GeodeticReferenceFrame::EPSG_6326,
        cs::CartesianCS::createGeocentric(common::UnitOfMeasure::METRE));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static bool hasCodeCompatibleOfAuthorityFactory(
    const common::IdentifiedObject *obj,
    const io::AuthorityFactoryPtr &authorityFactory) {
    const auto &ids = obj->identifiers();
    if (!ids.empty() && authorityFactory->getAuthority().empty()) {
        return true;
    }
    for (const auto &id : ids) {
        if (*(id->codeSpace()) == authorityFactory->getAuthority()) {
            return true;
        }
    }
    return false;
}

static bool hasCodeCompatibleOfAuthorityFactory(
    const metadata::IdentifierNNPtr &id,
    const io::AuthorityFactoryPtr &authorityFactory) {
    if (authorityFactory->getAuthority().empty()) {
        return true;
    }
    return *(id->codeSpace()) == authorityFactory->getAuthority();
}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Identify the CRS with reference CRSs.
 *
 * The candidate CRSs are either hard-coded, or looked in the database when
 * authorityFactory is not null.
 *
 * Note that the implementation uses a set of heuristics to have a good
 * compromise of successful identifications over execution time. It might miss
 * legitimate matches in some circumstances.
 *
 * The method returns a list of matching reference CRS, and the percentage
 * (0-100) of confidence in the match:
 * <ul>
 * <li>100% means that the name of the reference entry
 * perfectly matches the CRS name, and both are equivalent. In which case a
 * single result is returned.
 * Note: in the case of a GeographicCRS whose axis
 * order is implicit in the input definition (for example ESRI WKT), then axis
 * order is ignored for the purpose of identification. That is the CRS built
 * from
 * GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],
 * PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]
 * will be identified to EPSG:4326, but will not pass a
 * isEquivalentTo(EPSG_4326, util::IComparable::Criterion::EQUIVALENT) test,
 * but rather isEquivalentTo(EPSG_4326,
 * util::IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS)
 * </li>
 * <li>90% means that CRS are equivalent, but the names are not exactly the
 * same.
 * <li>70% means that CRS are equivalent (equivalent datum and coordinate
 * system),
 * but the names are not equivalent.</li>
 * <li>60% means that ellipsoid, prime meridian and coordinate systems are
 * equivalent, but the CRS and datum names do not match.</li>
 * <li>25% means that the CRS are not equivalent, but there is some similarity
 * in
 * the names.</li>
 * </ul>
 *
 * @param authorityFactory Authority factory (or null, but degraded
 * functionality)
 * @return a list of matching reference CRS, and the percentage (0-100) of
 * confidence in the match.
 */
std::list<std::pair<GeodeticCRSNNPtr, int>>
GeodeticCRS::identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<GeodeticCRSNNPtr, int> Pair;
    std::list<Pair> res;
    const auto &thisName(nameStr());

    io::DatabaseContextPtr dbContext =
        authorityFactory ? authorityFactory->databaseContext().as_nullable()
                         : nullptr;
    const bool l_implicitCS = hasImplicitCS();
    const auto crsCriterion =
        l_implicitCS
            ? util::IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS
            : util::IComparable::Criterion::EQUIVALENT;

    if (authorityFactory == nullptr ||
        authorityFactory->getAuthority().empty() ||
        authorityFactory->getAuthority() == metadata::Identifier::EPSG) {
        const GeographicCRSNNPtr candidatesCRS[] = {GeographicCRS::EPSG_4326,
                                                    GeographicCRS::EPSG_4267,
                                                    GeographicCRS::EPSG_4269};
        for (const auto &crs : candidatesCRS) {
            const bool nameEquivalent = metadata::Identifier::isEquivalentName(
                thisName.c_str(), crs->nameStr().c_str());
            const bool nameEqual = thisName == crs->nameStr();
            const bool isEq =
                _isEquivalentTo(crs.get(), crsCriterion, dbContext);
            if (nameEquivalent && isEq && (!authorityFactory || nameEqual)) {
                res.emplace_back(util::nn_static_pointer_cast<GeodeticCRS>(crs),
                                 nameEqual ? 100 : 90);
                return res;
            } else if (nameEqual && !isEq && !authorityFactory) {
                res.emplace_back(util::nn_static_pointer_cast<GeodeticCRS>(crs),
                                 25);
                return res;
            } else if (isEq && !authorityFactory) {
                res.emplace_back(util::nn_static_pointer_cast<GeodeticCRS>(crs),
                                 70);
                return res;
            }
        }
    }

    std::string geodetic_crs_type;
    if (isGeocentric()) {
        geodetic_crs_type = "geocentric";
    } else {
        auto geogCRS = dynamic_cast<const GeographicCRS *>(this);
        if (geogCRS) {
            if (coordinateSystem()->axisList().size() == 2) {
                geodetic_crs_type = "geographic 2D";
            } else {
                geodetic_crs_type = "geographic 3D";
            }
        }
    }

    if (authorityFactory) {

        const auto thisDatum(datumNonNull(dbContext));

        auto searchByDatumCode =
            [this, &authorityFactory, &res, &geodetic_crs_type, crsCriterion,
             &dbContext](const common::IdentifiedObjectNNPtr &l_datum) {
                bool resModified = false;
                for (const auto &id : l_datum->identifiers()) {
                    try {
                        auto tempRes =
                            authorityFactory->createGeodeticCRSFromDatum(
                                *id->codeSpace(), id->code(),
                                geodetic_crs_type);
                        for (const auto &crs : tempRes) {
                            if (_isEquivalentTo(crs.get(), crsCriterion,
                                                dbContext)) {
                                res.emplace_back(crs, 70);
                                resModified = true;
                            }
                        }
                    } catch (const std::exception &) {
                    }
                }
                return resModified;
            };

        auto searchByEllipsoid = [this, &authorityFactory, &res, &thisDatum,
                                  &geodetic_crs_type, l_implicitCS,
                                  &dbContext]() {
            const auto &thisEllipsoid = thisDatum->ellipsoid();
            const std::list<datum::EllipsoidNNPtr> ellipsoids(
                thisEllipsoid->identifiers().empty()
                    ? authorityFactory->createEllipsoidFromExisting(
                          thisEllipsoid)
                    : std::list<datum::EllipsoidNNPtr>{thisEllipsoid});
            for (const auto &ellps : ellipsoids) {
                for (const auto &id : ellps->identifiers()) {
                    try {
                        auto tempRes =
                            authorityFactory->createGeodeticCRSFromEllipsoid(
                                *id->codeSpace(), id->code(),
                                geodetic_crs_type);
                        for (const auto &crs : tempRes) {
                            const auto crsDatum(crs->datumNonNull(dbContext));
                            if (crsDatum->ellipsoid()->_isEquivalentTo(
                                    ellps.get(),
                                    util::IComparable::Criterion::EQUIVALENT,
                                    dbContext) &&
                                crsDatum->primeMeridian()->_isEquivalentTo(
                                    thisDatum->primeMeridian().get(),
                                    util::IComparable::Criterion::EQUIVALENT,
                                    dbContext) &&
                                (l_implicitCS ||
                                 coordinateSystem()->_isEquivalentTo(
                                     crs->coordinateSystem().get(),
                                     util::IComparable::Criterion::EQUIVALENT,
                                     dbContext))) {
                                res.emplace_back(crs, 60);
                            }
                        }
                    } catch (const std::exception &) {
                    }
                }
            }
        };

        const auto searchByDatumOrEllipsoid = [&authorityFactory, &thisDatum,
                                               searchByDatumCode,
                                               searchByEllipsoid]() {
            if (!thisDatum->identifiers().empty()) {
                searchByDatumCode(thisDatum);
            } else {
                auto candidateDatums = authorityFactory->createObjectsFromName(
                    thisDatum->nameStr(),
                    {io::AuthorityFactory::ObjectType::
                         GEODETIC_REFERENCE_FRAME},
                    false);
                bool resModified = false;
                for (const auto &candidateDatum : candidateDatums) {
                    if (searchByDatumCode(candidateDatum))
                        resModified = true;
                }
                if (!resModified) {
                    searchByEllipsoid();
                }
            }
        };

        const bool insignificantName = thisName.empty() ||
                                       ci_equal(thisName, "unknown") ||
                                       ci_equal(thisName, "unnamed");

        if (insignificantName) {
            searchByDatumOrEllipsoid();
        } else if (hasCodeCompatibleOfAuthorityFactory(this,
                                                       authorityFactory)) {
            // If the CRS has already an id, check in the database for the
            // official object, and verify that they are equivalent.
            for (const auto &id : identifiers()) {
                if (hasCodeCompatibleOfAuthorityFactory(id, authorityFactory)) {
                    try {
                        auto crs = io::AuthorityFactory::create(
                                       authorityFactory->databaseContext(),
                                       *id->codeSpace())
                                       ->createGeodeticCRS(id->code());
                        bool match =
                            _isEquivalentTo(crs.get(), crsCriterion, dbContext);
                        res.emplace_back(crs, match ? 100 : 25);
                        return res;
                    } catch (const std::exception &) {
                    }
                }
            }
        } else {
            bool gotAbove25Pct = false;
            for (int ipass = 0; ipass < 2; ipass++) {
                const bool approximateMatch = ipass == 1;
                auto objects = authorityFactory->createObjectsFromName(
                    thisName, {io::AuthorityFactory::ObjectType::GEODETIC_CRS},
                    approximateMatch);
                for (const auto &obj : objects) {
                    auto crs = util::nn_dynamic_pointer_cast<GeodeticCRS>(obj);
                    assert(crs);
                    auto crsNN = NN_NO_CHECK(crs);
                    if (_isEquivalentTo(crs.get(), crsCriterion, dbContext)) {
                        if (crs->nameStr() == thisName) {
                            res.clear();
                            res.emplace_back(crsNN, 100);
                            return res;
                        }
                        const bool eqName =
                            metadata::Identifier::isEquivalentName(
                                thisName.c_str(), crs->nameStr().c_str());
                        res.emplace_back(crsNN, eqName ? 90 : 70);
                        gotAbove25Pct = true;
                    } else {
                        res.emplace_back(crsNN, 25);
                    }
                }
                if (!res.empty()) {
                    break;
                }
            }
            if (!gotAbove25Pct) {
                searchByDatumOrEllipsoid();
            }
        }

        const auto &thisCS(coordinateSystem());
        // Sort results
        res.sort([&thisName, &thisDatum, &thisCS, &dbContext](const Pair &a,
                                                              const Pair &b) {
            // First consider confidence
            if (a.second > b.second) {
                return true;
            }
            if (a.second < b.second) {
                return false;
            }

            // Then consider exact name matching
            const auto &aName(a.first->nameStr());
            const auto &bName(b.first->nameStr());
            if (aName == thisName && bName != thisName) {
                return true;
            }
            if (bName == thisName && aName != thisName) {
                return false;
            }

            // Then datum matching
            const auto aDatum(a.first->datumNonNull(dbContext));
            const auto bDatum(b.first->datumNonNull(dbContext));
            const auto thisEquivADatum(thisDatum->_isEquivalentTo(
                aDatum.get(), util::IComparable::Criterion::EQUIVALENT,
                dbContext));
            const auto thisEquivBDatum(thisDatum->_isEquivalentTo(
                bDatum.get(), util::IComparable::Criterion::EQUIVALENT,
                dbContext));

            if (thisEquivADatum && !thisEquivBDatum) {
                return true;
            }
            if (!thisEquivADatum && thisEquivBDatum) {
                return false;
            }

            // Then coordinate system matching
            const auto &aCS(a.first->coordinateSystem());
            const auto &bCS(b.first->coordinateSystem());
            const auto thisEquivACs(thisCS->_isEquivalentTo(
                aCS.get(), util::IComparable::Criterion::EQUIVALENT,
                dbContext));
            const auto thisEquivBCs(thisCS->_isEquivalentTo(
                bCS.get(), util::IComparable::Criterion::EQUIVALENT,
                dbContext));
            if (thisEquivACs && !thisEquivBCs) {
                return true;
            }
            if (!thisEquivACs && thisEquivBCs) {
                return false;
            }

            // Then dimension of the coordinate system matching
            const auto thisCSAxisListSize = thisCS->axisList().size();
            const auto aCSAxistListSize = aCS->axisList().size();
            const auto bCSAxistListSize = bCS->axisList().size();
            if (thisCSAxisListSize == aCSAxistListSize &&
                thisCSAxisListSize != bCSAxistListSize) {
                return true;
            }
            if (thisCSAxisListSize != aCSAxistListSize &&
                thisCSAxisListSize == bCSAxistListSize) {
                return false;
            }

            // Favor the CRS whole ellipsoid names matches the ellipsoid
            // name (WGS84...)
            const bool aEllpsNameEqCRSName =
                metadata::Identifier::isEquivalentName(
                    aDatum->ellipsoid()->nameStr().c_str(),
                    a.first->nameStr().c_str());
            const bool bEllpsNameEqCRSName =
                metadata::Identifier::isEquivalentName(
                    bDatum->ellipsoid()->nameStr().c_str(),
                    b.first->nameStr().c_str());
            if (aEllpsNameEqCRSName && !bEllpsNameEqCRSName) {
                return true;
            }
            if (bEllpsNameEqCRSName && !aEllpsNameEqCRSName) {
                return false;
            }

            // Arbitrary final sorting criterion
            return aName < bName;
        });

        // If there are results with 90% confidence, only keep those
        if (res.size() >= 2 && res.front().second == 90) {
            std::list<Pair> newRes;
            for (const auto &pair : res) {
                if (pair.second == 90) {
                    newRes.push_back(pair);
                } else {
                    break;
                }
            }
            return newRes;
        }
    }
    return res;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
GeodeticCRS::_identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<CRSNNPtr, int> Pair;
    std::list<Pair> res;
    auto resTemp = identify(authorityFactory);
    for (const auto &pair : resTemp) {
        res.emplace_back(pair.first, pair.second);
    }
    return res;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct GeographicCRS::Private {
    cs::EllipsoidalCSNNPtr coordinateSystem_;

    explicit Private(const cs::EllipsoidalCSNNPtr &csIn)
        : coordinateSystem_(csIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

GeographicCRS::GeographicCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                             const datum::DatumEnsemblePtr &datumEnsembleIn,
                             const cs::EllipsoidalCSNNPtr &csIn)
    : SingleCRS(datumIn, datumEnsembleIn, csIn),
      GeodeticCRS(datumIn,
                  checkEnsembleForGeodeticCRS(datumIn, datumEnsembleIn), csIn),
      d(std::make_unique<Private>(csIn)) {}

// ---------------------------------------------------------------------------

GeographicCRS::GeographicCRS(const GeographicCRS &other)
    : SingleCRS(other), GeodeticCRS(other),
      d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
GeographicCRS::~GeographicCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

CRSNNPtr GeographicCRS::_shallowClone() const {
    auto crs(GeographicCRS::nn_make_shared<GeographicCRS>(*this));
    crs->assignSelf(crs);
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the cs::EllipsoidalCS associated with the CRS.
 *
 * @return a EllipsoidalCS.
 */
const cs::EllipsoidalCSNNPtr &GeographicCRS::coordinateSystem() PROJ_PURE_DEFN {
    return d->coordinateSystem_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeographicCRS from a datum::GeodeticReferenceFrameNNPtr
 * and a
 * cs::EllipsoidalCS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datum The datum of the CRS.
 * @param cs a EllipsoidalCS.
 * @return new GeographicCRS.
 */
GeographicCRSNNPtr
GeographicCRS::create(const util::PropertyMap &properties,
                      const datum::GeodeticReferenceFrameNNPtr &datum,
                      const cs::EllipsoidalCSNNPtr &cs) {
    return create(properties, datum.as_nullable(), nullptr, cs);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeographicCRS from a datum::GeodeticReferenceFramePtr
 * or
 * datum::DatumEnsemble and a
 * cs::EllipsoidalCS.
 *
 * One and only one of datum or datumEnsemble should be set to a non-null value.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datum The datum of the CRS, or nullptr
 * @param datumEnsemble The datum ensemble of the CRS, or nullptr.
 * @param cs a EllipsoidalCS.
 * @return new GeographicCRS.
 */
GeographicCRSNNPtr
GeographicCRS::create(const util::PropertyMap &properties,
                      const datum::GeodeticReferenceFramePtr &datum,
                      const datum::DatumEnsemblePtr &datumEnsemble,
                      const cs::EllipsoidalCSNNPtr &cs) {
    GeographicCRSNNPtr crs(
        GeographicCRS::nn_make_shared<GeographicCRS>(datum, datumEnsemble, cs));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    crs->CRS::getPrivate()->setNonStandardProperties(properties);
    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

/** \brief Return whether the current GeographicCRS is the 2D part of the
 * other 3D GeographicCRS.
 */
bool GeographicCRS::is2DPartOf3D(util::nn<const GeographicCRS *> other,
                                 const io::DatabaseContextPtr &dbContext)
    PROJ_PURE_DEFN {
    const auto &axis = d->coordinateSystem_->axisList();
    const auto &otherAxis = other->d->coordinateSystem_->axisList();
    if (!(axis.size() == 2 && otherAxis.size() == 3)) {
        return false;
    }
    const auto &firstAxis = axis[0];
    const auto &secondAxis = axis[1];
    const auto &otherFirstAxis = otherAxis[0];
    const auto &otherSecondAxis = otherAxis[1];
    if (!(firstAxis->_isEquivalentTo(
              otherFirstAxis.get(), util::IComparable::Criterion::EQUIVALENT) &&
          secondAxis->_isEquivalentTo(
              otherSecondAxis.get(),
              util::IComparable::Criterion::EQUIVALENT))) {
        return false;
    }
    try {
        const auto thisDatum = datumNonNull(dbContext);
        const auto otherDatum = other->datumNonNull(dbContext);
        return thisDatum->_isEquivalentTo(
            otherDatum.get(), util::IComparable::Criterion::EQUIVALENT);
    } catch (const util::InvalidValueTypeException &) {
        // should not happen really, but potentially thrown by
        // Identifier::Private::setProperties()
        assert(false);
        return false;
    }
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool GeographicCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    if (other == nullptr || !util::isOfExactType<GeographicCRS>(*other)) {
        return false;
    }

    const auto standardCriterion = getStandardCriterion(criterion);
    const auto otherGeogCRS = dynamic_cast<const GeographicCRS *>(other);
    if (GeodeticCRS::_isEquivalentToNoTypeCheck(other, standardCriterion,
                                                dbContext)) {
        // Make sure GeoPackage "Undefined geographic SRS" != EPSG:4326
        if ((nameStr() == "Undefined geographic SRS" ||
             otherGeogCRS->nameStr() == "Undefined geographic SRS") &&
            otherGeogCRS->nameStr() != nameStr()) {
            return false;
        }
        return true;
    }

    // In EPSG v12.025, Norway projected systems based on ETRS89 (EPSG:4258)
    // have swiched to use ETRS89-NOR [EUREF89] (EPSG:10875). There's no way
    // from the current content of the database to infer both CRS are equivalent
    if (criterion != util::IComparable::Criterion::STRICT) {
        if (((nameStr() == "ETRS89" &&
              otherGeogCRS->nameStr() == "ETRS89-NOR [EUREF89]") ||
             (nameStr() == "ETRS89-NOR [EUREF89]" &&
              otherGeogCRS->nameStr() == "ETRS89")) &&
            ellipsoid()->_isEquivalentTo(otherGeogCRS->ellipsoid().get(),
                                         criterion) &&
            datumNonNull(dbContext)->primeMeridian()->_isEquivalentTo(
                otherGeogCRS->datumNonNull(dbContext)->primeMeridian().get(),
                criterion)) {
            auto thisCS = coordinateSystem();
            auto otherCS = otherGeogCRS->coordinateSystem();
            if (thisCS->_isEquivalentTo(otherCS.get(), criterion)) {
                return true;
            } else if (criterion == util::IComparable::Criterion::
                                        EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS) {
                const auto &otherAxisList = otherCS->axisList();
                return thisCS->axisList().size() == 2 &&
                       otherAxisList.size() == 2 &&
                       thisCS->_isEquivalentTo(
                           cs::EllipsoidalCS::create(util::PropertyMap(),
                                                     otherAxisList[1],
                                                     otherAxisList[0])
                               .get(),
                           criterion);
            }
        }
    }

    if (criterion !=
        util::IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS) {
        return false;
    }

    const auto axisOrder = coordinateSystem()->axisOrder();
    if (axisOrder == cs::EllipsoidalCS::AxisOrder::LONG_EAST_LAT_NORTH ||
        axisOrder == cs::EllipsoidalCS::AxisOrder::LAT_NORTH_LONG_EAST) {
        const auto &unit = coordinateSystem()->axisList()[0]->unit();
        return GeographicCRS::create(
                   util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                           nameStr()),
                   datum(), datumEnsemble(),
                   axisOrder ==
                           cs::EllipsoidalCS::AxisOrder::LONG_EAST_LAT_NORTH
                       ? cs::EllipsoidalCS::createLatitudeLongitude(unit)
                       : cs::EllipsoidalCS::createLongitudeLatitude(unit))
            ->GeodeticCRS::_isEquivalentToNoTypeCheck(other, standardCriterion,
                                                      dbContext);
    }
    if (axisOrder ==
            cs::EllipsoidalCS::AxisOrder::LONG_EAST_LAT_NORTH_HEIGHT_UP ||
        axisOrder ==
            cs::EllipsoidalCS::AxisOrder::LAT_NORTH_LONG_EAST_HEIGHT_UP) {
        const auto &angularUnit = coordinateSystem()->axisList()[0]->unit();
        const auto &linearUnit = coordinateSystem()->axisList()[2]->unit();
        return GeographicCRS::create(
                   util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                           nameStr()),
                   datum(), datumEnsemble(),
                   axisOrder == cs::EllipsoidalCS::AxisOrder::
                                    LONG_EAST_LAT_NORTH_HEIGHT_UP
                       ? cs::EllipsoidalCS::
                             createLatitudeLongitudeEllipsoidalHeight(
                                 angularUnit, linearUnit)
                       : cs::EllipsoidalCS::
                             createLongitudeLatitudeEllipsoidalHeight(
                                 angularUnit, linearUnit))
            ->GeodeticCRS::_isEquivalentToNoTypeCheck(other, standardCriterion,
                                                      dbContext);
    }
    return false;
}
//! @endcond

// ---------------------------------------------------------------------------

GeographicCRSNNPtr GeographicCRS::createEPSG_4267() {
    return create(createMapNameEPSGCode("NAD27", 4267),
                  datum::GeodeticReferenceFrame::EPSG_6267,
                  cs::EllipsoidalCS::createLatitudeLongitude(
                      common::UnitOfMeasure::DEGREE));
}

// ---------------------------------------------------------------------------

GeographicCRSNNPtr GeographicCRS::createEPSG_4269() {
    return create(createMapNameEPSGCode("NAD83", 4269),
                  datum::GeodeticReferenceFrame::EPSG_6269,
                  cs::EllipsoidalCS::createLatitudeLongitude(
                      common::UnitOfMeasure::DEGREE));
}

// ---------------------------------------------------------------------------

GeographicCRSNNPtr GeographicCRS::createEPSG_4326() {
    return create(createMapNameEPSGCode("WGS 84", 4326),
                  datum::GeodeticReferenceFrame::EPSG_6326,
                  cs::EllipsoidalCS::createLatitudeLongitude(
                      common::UnitOfMeasure::DEGREE));
}

// ---------------------------------------------------------------------------

GeographicCRSNNPtr GeographicCRS::createOGC_CRS84() {
    util::PropertyMap propertiesCRS;
    propertiesCRS
        .set(metadata::Identifier::CODESPACE_KEY, metadata::Identifier::OGC)
        .set(metadata::Identifier::CODE_KEY, "CRS84")
        .set(common::IdentifiedObject::NAME_KEY, "WGS 84 (CRS84)");
    return create(propertiesCRS, datum::GeodeticReferenceFrame::EPSG_6326,
                  cs::EllipsoidalCS::createLongitudeLatitude( // Long Lat !
                      common::UnitOfMeasure::DEGREE));
}

// ---------------------------------------------------------------------------

GeographicCRSNNPtr GeographicCRS::createEPSG_4979() {
    return create(
        createMapNameEPSGCode("WGS 84", 4979),
        datum::GeodeticReferenceFrame::EPSG_6326,
        cs::EllipsoidalCS::createLatitudeLongitudeEllipsoidalHeight(
            common::UnitOfMeasure::DEGREE, common::UnitOfMeasure::METRE));
}

// ---------------------------------------------------------------------------

GeographicCRSNNPtr GeographicCRS::createEPSG_4807() {
    auto ellps(datum::Ellipsoid::createFlattenedSphere(
        createMapNameEPSGCode("Clarke 1880 (IGN)", 7011),
        common::Length(6378249.2), common::Scale(293.4660212936269)));

    auto cs(cs::EllipsoidalCS::createLatitudeLongitude(
        common::UnitOfMeasure::GRAD));

    auto datum(datum::GeodeticReferenceFrame::create(
        createMapNameEPSGCode("Nouvelle Triangulation Francaise (Paris)", 6807),
        ellps, util::optional<std::string>(), datum::PrimeMeridian::PARIS));

    return create(createMapNameEPSGCode("NTF (Paris)", 4807), datum, cs);
}

// ---------------------------------------------------------------------------

/** \brief Return a variant of this CRS "demoted" to a 2D one, if not already
 * the case.
 *
 *
 * @param newName Name of the new CRS. If empty, nameStr() will be used.
 * @param dbContext Database context to look for potentially already registered
 *                  2D CRS. May be nullptr.
 * @return a new CRS demoted to 2D, or the current one if already 2D or not
 * applicable.
 * @since 6.3
 */
GeographicCRSNNPtr
GeographicCRS::demoteTo2D(const std::string &newName,
                          const io::DatabaseContextPtr &dbContext) const {

    const auto &axisList = coordinateSystem()->axisList();
    if (axisList.size() == 3) {
        const auto &l_identifiers = identifiers();
        // First check if there is a Geographic 2D CRS in the database
        // of the same name.
        // This is the common practice in the EPSG dataset.
        if (dbContext && l_identifiers.size() == 1) {
            auto authFactory = io::AuthorityFactory::create(
                NN_NO_CHECK(dbContext), *(l_identifiers[0]->codeSpace()));
            auto res = authFactory->createObjectsFromName(
                nameStr(),
                {io::AuthorityFactory::ObjectType::GEOGRAPHIC_2D_CRS}, false);
            if (!res.empty()) {
                const auto &firstRes = res.front();
                auto firstResAsGeogCRS =
                    util::nn_dynamic_pointer_cast<GeographicCRS>(firstRes);
                if (firstResAsGeogCRS && firstResAsGeogCRS->is2DPartOf3D(
                                             NN_NO_CHECK(this), dbContext)) {
                    return NN_NO_CHECK(firstResAsGeogCRS);
                }
            }
        }

        auto cs = cs::EllipsoidalCS::create(util::PropertyMap(), axisList[0],
                                            axisList[1]);
        return GeographicCRS::create(
            util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                    !newName.empty() ? newName : nameStr()),
            datum(), datumEnsemble(), cs);
    }

    return NN_NO_CHECK(std::dynamic_pointer_cast<GeographicCRS>(
        shared_from_this().as_nullable()));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeographicCRS::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(io::FormattingException)
{
    const auto &extensionProj4 = CRS::getPrivate()->extensionProj4_;
    if (!extensionProj4.empty()) {
        formatter->ingestPROJString(
            replaceAll(extensionProj4, " +type=crs", ""));
        formatter->addNoDefs(false);
        return;
    }

    if (!formatter->omitProjLongLatIfPossible() ||
        primeMeridian()->longitude().getSIValue() != 0.0 ||
        !formatter->getTOWGS84Parameters().empty() ||
        !formatter->getHDatumExtension().empty()) {

        formatter->addStep("longlat");
        bool done = false;
        if (formatter->getLegacyCRSToCRSContext() &&
            formatter->getHDatumExtension().empty() &&
            formatter->getTOWGS84Parameters().empty()) {
            const auto l_datum = datumNonNull(formatter->databaseContext());
            if (l_datum->_isEquivalentTo(
                    datum::GeodeticReferenceFrame::EPSG_6326.get(),
                    util::IComparable::Criterion::EQUIVALENT)) {
                done = true;
                formatter->addParam("ellps", "WGS84");
            } else if (l_datum->_isEquivalentTo(
                           datum::GeodeticReferenceFrame::EPSG_6269.get(),
                           util::IComparable::Criterion::EQUIVALENT)) {
                done = true;
                // We do not want datum=NAD83 to cause a useless towgs84=0,0,0
                formatter->addParam("ellps", "GRS80");
            }
        }
        if (!done) {
            addDatumInfoToPROJString(formatter);
        }
    }
    if (!formatter->getCRSExport()) {
        addAngularUnitConvertAndAxisSwap(formatter);
    }
    if (hasOver()) {
        formatter->addParam("over");
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeographicCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    _exportToJSONInternal(formatter, "GeographicCRS");
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct VerticalCRS::Private {
    std::vector<operation::TransformationNNPtr> geoidModel{};
    std::vector<operation::PointMotionOperationNNPtr> velocityModel{};
};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static const datum::DatumEnsemblePtr &
checkEnsembleForVerticalCRS(const datum::VerticalReferenceFramePtr &datumIn,
                            const datum::DatumEnsemblePtr &ensemble) {
    const char *msg = "One of Datum or DatumEnsemble should be defined";
    if (datumIn) {
        if (!ensemble) {
            return ensemble;
        }
        msg = "Datum and DatumEnsemble should not be defined";
    } else if (ensemble) {
        const auto &datums = ensemble->datums();
        assert(!datums.empty());
        auto grfFirst =
            dynamic_cast<datum::VerticalReferenceFrame *>(datums[0].get());
        if (grfFirst) {
            return ensemble;
        }
        msg = "Ensemble should contain VerticalReferenceFrame";
    }
    throw util::Exception(msg);
}
//! @endcond

// ---------------------------------------------------------------------------

VerticalCRS::VerticalCRS(const datum::VerticalReferenceFramePtr &datumIn,
                         const datum::DatumEnsemblePtr &datumEnsembleIn,
                         const cs::VerticalCSNNPtr &csIn)
    : SingleCRS(datumIn, checkEnsembleForVerticalCRS(datumIn, datumEnsembleIn),
                csIn),
      d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

VerticalCRS::VerticalCRS(const VerticalCRS &other)
    : SingleCRS(other), d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
VerticalCRS::~VerticalCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

CRSNNPtr VerticalCRS::_shallowClone() const {
    auto crs(VerticalCRS::nn_make_shared<VerticalCRS>(*this));
    crs->assignSelf(crs);
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the datum::VerticalReferenceFrame associated with the CRS.
 *
 * @return a VerticalReferenceFrame.
 */
const datum::VerticalReferenceFramePtr VerticalCRS::datum() const {
    return std::static_pointer_cast<datum::VerticalReferenceFrame>(
        SingleCRS::getPrivate()->datum);
}

// ---------------------------------------------------------------------------

/** \brief Return the geoid model associated with the CRS.
 *
 * Geoid height model or height correction model linked to a geoid-based
 * vertical CRS.
 *
 * @return a geoid model. might be null
 */
const std::vector<operation::TransformationNNPtr> &
VerticalCRS::geoidModel() PROJ_PURE_DEFN {
    return d->geoidModel;
}

// ---------------------------------------------------------------------------

/** \brief Return the velocity model associated with the CRS.
 *
 * @return a velocity model. might be null.
 */
const std::vector<operation::PointMotionOperationNNPtr> &
VerticalCRS::velocityModel() PROJ_PURE_DEFN {
    return d->velocityModel;
}

// ---------------------------------------------------------------------------

/** \brief Return the cs::VerticalCS associated with the CRS.
 *
 * @return a VerticalCS.
 */
const cs::VerticalCSNNPtr VerticalCRS::coordinateSystem() const {
    return util::nn_static_pointer_cast<cs::VerticalCS>(
        SingleCRS::getPrivate()->coordinateSystem);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
/** \brief Return the real datum or a synthesized one if a datumEnsemble.
 */
const datum::VerticalReferenceFrameNNPtr
VerticalCRS::datumNonNull(const io::DatabaseContextPtr &dbContext) const {
    return NN_NO_CHECK(
        util::nn_dynamic_pointer_cast<datum::VerticalReferenceFrame>(
            SingleCRS::datumNonNull(dbContext)));
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void VerticalCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    formatter->startNode(isWKT2 ? io::WKTConstants::VERTCRS
                         : formatter->useESRIDialect()
                             ? io::WKTConstants::VERTCS
                             : io::WKTConstants::VERT_CS,
                         !identifiers().empty());

    std::string l_name(nameStr());
    const auto &dbContext = formatter->databaseContext();
    if (formatter->useESRIDialect()) {
        bool aliasFound = false;
        if (dbContext) {
            auto l_alias = dbContext->getAliasFromOfficialName(
                l_name, "vertical_crs", "ESRI");
            if (!l_alias.empty()) {
                l_name = std::move(l_alias);
                aliasFound = true;
            }
        }
        if (!aliasFound && dbContext) {
            auto authFactory =
                io::AuthorityFactory::create(NN_NO_CHECK(dbContext), "ESRI");
            aliasFound =
                authFactory
                    ->createObjectsFromName(
                        l_name,
                        {io::AuthorityFactory::ObjectType::VERTICAL_CRS},
                        false // approximateMatch
                        )
                    .size() == 1;
        }
        if (!aliasFound) {
            l_name = io::WKTFormatter::morphNameToESRI(l_name);
        }
    }

    formatter->addQuotedString(l_name);

    const auto l_datum = datum();
    if (formatter->useESRIDialect() && l_datum &&
        l_datum->getWKT1DatumType() == "2002") {
        bool foundMatch = false;
        if (dbContext) {
            auto authFactory = io::AuthorityFactory::create(
                NN_NO_CHECK(dbContext), std::string());
            auto list = authFactory->createObjectsFromName(
                l_datum->nameStr(),
                {io::AuthorityFactory::ObjectType::GEODETIC_REFERENCE_FRAME},
                false /* approximate=false*/);
            if (!list.empty()) {
                auto gdatum =
                    util::nn_dynamic_pointer_cast<datum::Datum>(list.front());
                if (gdatum) {
                    gdatum->_exportToWKT(formatter);
                    foundMatch = true;
                }
            }
        }
        if (!foundMatch) {
            // We should export a geodetic datum, but we cannot really do better
            l_datum->_exportToWKT(formatter);
        }
    } else {
        exportDatumOrDatumEnsembleToWkt(formatter);
    }
    const auto &cs = SingleCRS::getPrivate()->coordinateSystem;
    const auto &axisList = cs->axisList();

    if (formatter->useESRIDialect()) {
        // Seems to be a constant value...
        formatter->startNode(io::WKTConstants::PARAMETER, false);
        formatter->addQuotedString("Vertical_Shift");
        formatter->add(0.0);
        formatter->endNode();

        formatter->startNode(io::WKTConstants::PARAMETER, false);
        formatter->addQuotedString("Direction");
        formatter->add(
            axisList[0]->direction() == cs::AxisDirection::UP ? 1.0 : -1.0);
        formatter->endNode();
    }

    if (!isWKT2) {
        axisList[0]->unit()._exportToWKT(formatter);
    }

    const auto oldAxisOutputRule = formatter->outputAxis();
    if (oldAxisOutputRule ==
        io::WKTFormatter::OutputAxisRule::WKT1_GDAL_EPSG_STYLE) {
        formatter->setOutputAxis(io::WKTFormatter::OutputAxisRule::YES);
    }
    cs->_exportToWKT(formatter);
    formatter->setOutputAxis(oldAxisOutputRule);

    if (isWKT2 && formatter->use2019Keywords() && !d->geoidModel.empty()) {
        for (const auto &model : d->geoidModel) {
            formatter->startNode(io::WKTConstants::GEOIDMODEL, false);
            formatter->addQuotedString(model->nameStr());
            model->formatID(formatter);
            formatter->endNode();
        }
    }

    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void VerticalCRS::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(io::FormattingException)
{
    const auto &geoidgrids = formatter->getVDatumExtension();
    if (!geoidgrids.empty()) {
        formatter->addParam("geoidgrids", geoidgrids);
    }
    const auto &geoidCRS = formatter->getGeoidCRSValue();
    if (!geoidCRS.empty()) {
        formatter->addParam("geoid_crs", geoidCRS);
    }

    auto &axisList = coordinateSystem()->axisList();
    if (!axisList.empty()) {
        auto projUnit = axisList[0]->unit().exportToPROJString();
        if (projUnit.empty()) {
            formatter->addParam("vto_meter",
                                axisList[0]->unit().conversionToSI());
        } else {
            formatter->addParam("vunits", projUnit);
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void VerticalCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("VerticalCRS", !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    const auto &l_datum(datum());
    if (l_datum) {
        writer->AddObjKey("datum");
        l_datum->_exportToJSON(formatter);
    } else {
        writer->AddObjKey("datum_ensemble");
        formatter->setOmitTypeInImmediateChild();
        datumEnsemble()->_exportToJSON(formatter);
    }

    writer->AddObjKey("coordinate_system");
    formatter->setOmitTypeInImmediateChild();
    coordinateSystem()->_exportToJSON(formatter);

    const auto geoidModelExport =
        [&writer, &formatter](const operation::TransformationNNPtr &model) {
            auto objectContext2(formatter->MakeObjectContext(nullptr, false));
            writer->AddObjKey("name");
            writer->Add(model->nameStr());

            if (model->identifiers().empty()) {
                const auto &interpCRS = model->interpolationCRS();
                if (interpCRS) {
                    writer->AddObjKey("interpolation_crs");
                    interpCRS->_exportToJSON(formatter);
                }
            }

            model->formatID(formatter);
        };

    if (d->geoidModel.size() == 1) {
        writer->AddObjKey("geoid_model");
        geoidModelExport(d->geoidModel[0]);
    } else if (d->geoidModel.size() > 1) {
        writer->AddObjKey("geoid_models");
        auto geoidModelsArrayContext(writer->MakeArrayContext(false));
        for (const auto &model : d->geoidModel) {
            geoidModelExport(model);
        }
    }

    if (const auto dynamicVRF =
            dynamic_cast<datum::DynamicVerticalReferenceFrame *>(
                l_datum.get())) {
        const auto &deformationModel = dynamicVRF->deformationModelName();
        if (deformationModel.has_value()) {
            writer->AddObjKey("deformation_models");
            auto arrayContext(writer->MakeArrayContext(false));
            auto objectContext2(formatter->MakeObjectContext(nullptr, false));
            writer->AddObjKey("name");
            writer->Add(*deformationModel);
        }
    }

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void VerticalCRS::addLinearUnitConvert(
    io::PROJStringFormatter *formatter) const {
    auto &axisList = coordinateSystem()->axisList();

    if (!axisList.empty()) {
        if (axisList[0]->unit().conversionToSI() != 1.0) {
            formatter->addStep("unitconvert");
            formatter->addParam("z_in", "m");
            auto projVUnit = axisList[0]->unit().exportToPROJString();
            if (projVUnit.empty()) {
                formatter->addParam("z_out",
                                    axisList[0]->unit().conversionToSI());
            } else {
                formatter->addParam("z_out", projVUnit);
            }
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a VerticalCRS from a datum::VerticalReferenceFrame and a
 * cs::VerticalCS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined. The GEOID_MODEL property can be set
 * to a TransformationNNPtr object.
 * @param datumIn The datum of the CRS.
 * @param csIn a VerticalCS.
 * @return new VerticalCRS.
 */
VerticalCRSNNPtr
VerticalCRS::create(const util::PropertyMap &properties,
                    const datum::VerticalReferenceFrameNNPtr &datumIn,
                    const cs::VerticalCSNNPtr &csIn) {
    return create(properties, datumIn.as_nullable(), nullptr, csIn);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a VerticalCRS from a datum::VerticalReferenceFrame or
 * datum::DatumEnsemble and a cs::VerticalCS.
 *
 * One and only one of datum or datumEnsemble should be set to a non-null value.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined. The GEOID_MODEL property can be set
 * to a TransformationNNPtr object.
 * @param datumIn The datum of the CRS, or nullptr
 * @param datumEnsembleIn The datum ensemble of the CRS, or nullptr.
 * @param csIn a VerticalCS.
 * @return new VerticalCRS.
 */
VerticalCRSNNPtr
VerticalCRS::create(const util::PropertyMap &properties,
                    const datum::VerticalReferenceFramePtr &datumIn,
                    const datum::DatumEnsemblePtr &datumEnsembleIn,
                    const cs::VerticalCSNNPtr &csIn) {
    auto crs(VerticalCRS::nn_make_shared<VerticalCRS>(datumIn, datumEnsembleIn,
                                                      csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    const auto geoidModelPtr = properties.get("GEOID_MODEL");
    if (geoidModelPtr) {
        if (auto array = util::nn_dynamic_pointer_cast<util::ArrayOfBaseObject>(
                *geoidModelPtr)) {
            for (const auto &item : *array) {
                auto transf =
                    util::nn_dynamic_pointer_cast<operation::Transformation>(
                        item);
                if (transf) {
                    crs->d->geoidModel.emplace_back(NN_NO_CHECK(transf));
                }
            }
        } else if (auto transf =
                       util::nn_dynamic_pointer_cast<operation::Transformation>(
                           *geoidModelPtr)) {
            crs->d->geoidModel.emplace_back(NN_NO_CHECK(transf));
        }
    }
    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool VerticalCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherVertCRS = dynamic_cast<const VerticalCRS *>(other);
    if (otherVertCRS == nullptr ||
        !util::isOfExactType<VerticalCRS>(*otherVertCRS)) {
        return false;
    }
    // TODO test geoidModel and velocityModel
    return SingleCRS::baseIsEquivalentTo(other, criterion, dbContext);
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Identify the CRS with reference CRSs.
 *
 * The candidate CRSs are looked in the database when
 * authorityFactory is not null.
 *
 * Note that the implementation uses a set of heuristics to have a good
 * compromise of successful identifications over execution time. It might miss
 * legitimate matches in some circumstances.
 *
 * The method returns a list of matching reference CRS, and the percentage
 * (0-100) of confidence in the match.
 * 100% means that the name of the reference entry
 * perfectly matches the CRS name, and both are equivalent. In which case a
 * single result is returned.
 * 90% means that CRS are equivalent, but the names are not exactly the same.
 * 70% means that CRS are equivalent (equivalent datum and coordinate system),
 * but the names are not equivalent.
 * 25% means that the CRS are not equivalent, but there is some similarity in
 * the names.
 *
 * @param authorityFactory Authority factory (if null, will return an empty
 * list)
 * @return a list of matching reference CRS, and the percentage (0-100) of
 * confidence in the match.
 */
std::list<std::pair<VerticalCRSNNPtr, int>>
VerticalCRS::identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<VerticalCRSNNPtr, int> Pair;
    std::list<Pair> res;

    const auto &thisName(nameStr());

    if (authorityFactory) {
        const io::DatabaseContextNNPtr &dbContext =
            authorityFactory->databaseContext();

        const bool insignificantName = thisName.empty() ||
                                       ci_equal(thisName, "unknown") ||
                                       ci_equal(thisName, "unnamed");
        if (hasCodeCompatibleOfAuthorityFactory(this, authorityFactory)) {
            // If the CRS has already an id, check in the database for the
            // official object, and verify that they are equivalent.
            for (const auto &id : identifiers()) {
                if (hasCodeCompatibleOfAuthorityFactory(id, authorityFactory)) {
                    try {
                        auto crs = io::AuthorityFactory::create(
                                       dbContext, *id->codeSpace())
                                       ->createVerticalCRS(id->code());
                        bool match = _isEquivalentTo(
                            crs.get(), util::IComparable::Criterion::EQUIVALENT,
                            dbContext);
                        res.emplace_back(crs, match ? 100 : 25);
                        return res;
                    } catch (const std::exception &) {
                    }
                }
            }
        } else if (!insignificantName) {
            for (int ipass = 0; ipass < 2; ipass++) {
                const bool approximateMatch = ipass == 1;
                auto objects = authorityFactory->createObjectsFromName(
                    thisName, {io::AuthorityFactory::ObjectType::VERTICAL_CRS},
                    approximateMatch);
                for (const auto &obj : objects) {
                    auto crs = util::nn_dynamic_pointer_cast<VerticalCRS>(obj);
                    assert(crs);
                    auto crsNN = NN_NO_CHECK(crs);
                    if (_isEquivalentTo(
                            crs.get(), util::IComparable::Criterion::EQUIVALENT,
                            dbContext)) {
                        if (crs->nameStr() == thisName) {
                            res.clear();
                            res.emplace_back(crsNN, 100);
                            return res;
                        }
                        res.emplace_back(crsNN, 90);
                    } else {
                        res.emplace_back(crsNN, 25);
                    }
                }
                if (!res.empty()) {
                    break;
                }
            }
        }

        // Sort results
        res.sort([&thisName](const Pair &a, const Pair &b) {
            // First consider confidence
            if (a.second > b.second) {
                return true;
            }
            if (a.second < b.second) {
                return false;
            }

            // Then consider exact name matching
            const auto &aName(a.first->nameStr());
            const auto &bName(b.first->nameStr());
            if (aName == thisName && bName != thisName) {
                return true;
            }
            if (bName == thisName && aName != thisName) {
                return false;
            }

            // Arbitrary final sorting criterion
            return aName < bName;
        });

        // Keep only results of the highest confidence
        if (res.size() >= 2) {
            const auto highestConfidence = res.front().second;
            std::list<Pair> newRes;
            for (const auto &pair : res) {
                if (pair.second == highestConfidence) {
                    newRes.push_back(pair);
                } else {
                    break;
                }
            }
            return newRes;
        }
    }

    return res;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
VerticalCRS::_identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<CRSNNPtr, int> Pair;
    std::list<Pair> res;
    auto resTemp = identify(authorityFactory);
    for (const auto &pair : resTemp) {
        res.emplace_back(pair.first, pair.second);
    }
    return res;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DerivedCRS::Private {
    SingleCRSNNPtr baseCRS_;
    operation::ConversionNNPtr derivingConversion_;

    Private(const SingleCRSNNPtr &baseCRSIn,
            const operation::ConversionNNPtr &derivingConversionIn)
        : baseCRS_(baseCRSIn), derivingConversion_(derivingConversionIn) {}

    // For the conversion make a _shallowClone(), so that we can later set
    // its targetCRS to this.
    Private(const Private &other)
        : baseCRS_(other.baseCRS_),
          derivingConversion_(other.derivingConversion_->shallowClone()) {}
};

//! @endcond

// ---------------------------------------------------------------------------

// DerivedCRS is an abstract class, that virtually inherits from SingleCRS
// Consequently the base constructor in SingleCRS will never be called by
// that constructor. clang -Wabstract-vbase-init and VC++ underline this, but
// other
// compilers will complain if we don't call the base constructor.

DerivedCRS::DerivedCRS(const SingleCRSNNPtr &baseCRSIn,
                       const operation::ConversionNNPtr &derivingConversionIn,
                       const cs::CoordinateSystemNNPtr &
#if !defined(COMPILER_WARNS_ABOUT_ABSTRACT_VBASE_INIT)
                           cs
#endif
                       )
    :
#if !defined(COMPILER_WARNS_ABOUT_ABSTRACT_VBASE_INIT)
      SingleCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), cs),
#endif
      d(std::make_unique<Private>(baseCRSIn, derivingConversionIn)) {
}

// ---------------------------------------------------------------------------

DerivedCRS::DerivedCRS(const DerivedCRS &other)
    :
#if !defined(COMPILER_WARNS_ABOUT_ABSTRACT_VBASE_INIT)
      SingleCRS(other),
#endif
      d(std::make_unique<Private>(*other.d)) {
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DerivedCRS::~DerivedCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the base CRS of a DerivedCRS.
 *
 * @return the base CRS.
 */
const SingleCRSNNPtr &DerivedCRS::baseCRS() PROJ_PURE_DEFN {
    return d->baseCRS_;
}

// ---------------------------------------------------------------------------

/** \brief Return the deriving conversion from the base CRS to this CRS.
 *
 * @return the deriving conversion.
 */
const operation::ConversionNNPtr DerivedCRS::derivingConversion() const {
    return d->derivingConversion_->shallowClone();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const operation::ConversionNNPtr &
DerivedCRS::derivingConversionRef() PROJ_PURE_DEFN {
    return d->derivingConversion_;
}
//! @endcond

// ---------------------------------------------------------------------------

bool DerivedCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherDerivedCRS = dynamic_cast<const DerivedCRS *>(other);
    const auto standardCriterion = getStandardCriterion(criterion);
    if (otherDerivedCRS == nullptr ||
        !SingleCRS::baseIsEquivalentTo(other, standardCriterion, dbContext)) {
        return false;
    }
    return d->baseCRS_->_isEquivalentTo(otherDerivedCRS->d->baseCRS_.get(),
                                        criterion, dbContext) &&
           d->derivingConversion_->_isEquivalentTo(
               otherDerivedCRS->d->derivingConversion_.get(), standardCriterion,
               dbContext);
}

// ---------------------------------------------------------------------------

void DerivedCRS::setDerivingConversionCRS() {
    derivingConversionRef()->setWeakSourceTargetCRS(
        baseCRS().as_nullable(),
        std::static_pointer_cast<CRS>(shared_from_this().as_nullable()));
}

// ---------------------------------------------------------------------------

void DerivedCRS::baseExportToWKT(io::WKTFormatter *formatter,
                                 const std::string &keyword,
                                 const std::string &baseKeyword) const {
    formatter->startNode(keyword, !identifiers().empty());
    formatter->addQuotedString(nameStr());

    const auto &l_baseCRS = d->baseCRS_;
    formatter->startNode(baseKeyword, formatter->use2019Keywords() &&
                                          !l_baseCRS->identifiers().empty());
    formatter->addQuotedString(l_baseCRS->nameStr());
    l_baseCRS->exportDatumOrDatumEnsembleToWkt(formatter);
    if (formatter->use2019Keywords() &&
        !(formatter->idOnTopLevelOnly() && formatter->topLevelHasId())) {
        l_baseCRS->formatID(formatter);
    }
    formatter->endNode();

    formatter->setUseDerivingConversion(true);
    derivingConversionRef()->_exportToWKT(formatter);
    formatter->setUseDerivingConversion(false);

    coordinateSystem()->_exportToWKT(formatter);
    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DerivedCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext(className(), !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    writer->AddObjKey("base_crs");
    baseCRS()->_exportToJSON(formatter);

    writer->AddObjKey("conversion");
    formatter->setOmitTypeInImmediateChild();
    derivingConversionRef()->_exportToJSON(formatter);

    writer->AddObjKey("coordinate_system");
    formatter->setOmitTypeInImmediateChild();
    coordinateSystem()->_exportToJSON(formatter);

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct ProjectedCRS::Private {
    GeodeticCRSNNPtr baseCRS_;
    cs::CartesianCSNNPtr cs_;
    Private(const GeodeticCRSNNPtr &baseCRSIn, const cs::CartesianCSNNPtr &csIn)
        : baseCRS_(baseCRSIn), cs_(csIn) {}

    inline const GeodeticCRSNNPtr &baseCRS() const { return baseCRS_; }

    inline const cs::CartesianCSNNPtr &coordinateSystem() const { return cs_; }
};
//! @endcond

// ---------------------------------------------------------------------------

ProjectedCRS::ProjectedCRS(
    const GeodeticCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::CartesianCSNNPtr &csIn)
    : SingleCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      DerivedCRS(baseCRSIn, derivingConversionIn, csIn),
      d(std::make_unique<Private>(baseCRSIn, csIn)) {}

// ---------------------------------------------------------------------------

ProjectedCRS::ProjectedCRS(const ProjectedCRS &other)
    : SingleCRS(other), DerivedCRS(other),
      d(std::make_unique<Private>(other.baseCRS(), other.coordinateSystem())) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ProjectedCRS::~ProjectedCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

CRSNNPtr ProjectedCRS::_shallowClone() const {
    auto crs(ProjectedCRS::nn_make_shared<ProjectedCRS>(*this));
    crs->assignSelf(crs);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the base CRS (a GeodeticCRS, which is generally a
 * GeographicCRS) of the ProjectedCRS.
 *
 * @return the base CRS.
 */
const GeodeticCRSNNPtr &ProjectedCRS::baseCRS() PROJ_PURE_DEFN {
    return d->baseCRS();
}

// ---------------------------------------------------------------------------

/** \brief Return the cs::CartesianCS associated with the CRS.
 *
 * @return a CartesianCS
 */
const cs::CartesianCSNNPtr &ProjectedCRS::coordinateSystem() PROJ_PURE_DEFN {
    return d->coordinateSystem();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ProjectedCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;

    const auto &l_identifiers = identifiers();
    // Try to perfectly round-trip ESRI projectedCRS if the current object
    // perfectly matches the database definition
    const auto &dbContext = formatter->databaseContext();

    std::string l_name(nameStr());
    const auto &l_coordinateSystem = d->coordinateSystem();
    const auto &axisList = l_coordinateSystem->axisList();
    if (axisList.size() == 3 && !(isWKT2 && formatter->use2019Keywords())) {
        auto projCRS2D = demoteTo2D(std::string(), dbContext);
        if (dbContext) {
            const auto res = projCRS2D->identify(io::AuthorityFactory::create(
                NN_NO_CHECK(dbContext), metadata::Identifier::EPSG));
            if (res.size() == 1) {
                const auto &front = res.front();
                if (front.second == 100) {
                    projCRS2D = front.first;
                }
            }
        }

        if (formatter->useESRIDialect() && dbContext) {
            // Try to format the ProjecteD 3D CRS as a
            // PROJCS[],VERTCS[...,DATUM[]]
            // if we find corresponding objects
            if (exportAsESRIWktCompoundCRSWithEllipsoidalHeight(
                    this, baseCRS().as_nullable().get(), formatter)) {
                return;
            }
        }

        if (!formatter->useESRIDialect() &&
            CRS::getPrivate()->allowNonConformantWKT1Export_) {
            formatter->startNode(io::WKTConstants::COMPD_CS, false);
            formatter->addQuotedString(l_name + " + " + baseCRS()->nameStr());
            projCRS2D->_exportToWKT(formatter);
            baseCRS()
                ->demoteTo2D(std::string(), dbContext)
                ->_exportToWKT(formatter);
            formatter->endNode();
            return;
        }

        auto &originalCompoundCRS = CRS::getPrivate()->originalCompoundCRS_;
        if (!formatter->useESRIDialect() && originalCompoundCRS) {
            originalCompoundCRS->_exportToWKT(formatter);
            return;
        }

        if (!formatter->useESRIDialect() &&
            formatter->isAllowedEllipsoidalHeightAsVerticalCRS()) {
            exportAsWKT1CompoundCRSWithEllipsoidalHeight(projCRS2D, axisList[2],
                                                         formatter);
            return;
        }

        io::FormattingException::Throw(
            "Projected 3D CRS can only be exported since WKT2:2019");
    }

    std::string l_esri_name;
    if (formatter->useESRIDialect() && dbContext) {

        if (!l_identifiers.empty()) {
            // Try to find the ESRI alias from the CRS identified by its id
            const auto aliases = dbContext->getAliases(
                *(l_identifiers[0]->codeSpace()), l_identifiers[0]->code(),
                std::string(), // officialName,
                "projected_crs", "ESRI");
            if (aliases.size() == 1)
                l_esri_name = aliases.front();
        }
        if (l_esri_name.empty()) {
            // Then find the ESRI alias from the CRS name
            l_esri_name = dbContext->getAliasFromOfficialName(
                l_name, "projected_crs", "ESRI");
        }
        if (l_esri_name.empty()) {
            // Then try to build an ESRI CRS from the CRS name, and if there's
            // one, the ESRI name is the CRS name
            auto authFactory =
                io::AuthorityFactory::create(NN_NO_CHECK(dbContext), "ESRI");
            const bool found =
                authFactory
                    ->createObjectsFromName(
                        l_name,
                        {io::AuthorityFactory::ObjectType::PROJECTED_CRS},
                        false // approximateMatch
                        )
                    .size() == 1;
            if (found)
                l_esri_name = l_name;
        }

        if (!isWKT2 && !l_identifiers.empty() &&
            *(l_identifiers[0]->codeSpace()) == "ESRI") {
            try {
                // If the id of the object is in the ESRI namespace, then
                // try to find the full ESRI WKT from the database
                const auto definition = dbContext->getTextDefinition(
                    "projected_crs", "ESRI", l_identifiers[0]->code());
                if (starts_with(definition, "PROJCS")) {
                    auto crsFromFromDef = io::WKTParser()
                                              .attachDatabaseContext(dbContext)
                                              .createFromWKT(definition);
                    if (_isEquivalentTo(
                            dynamic_cast<IComparable *>(crsFromFromDef.get()),
                            util::IComparable::Criterion::EQUIVALENT)) {
                        formatter->ingestWKTNode(
                            io::WKTNode::createFrom(definition));
                        return;
                    }
                }
            } catch (const std::exception &) {
            }
        } else if (!isWKT2 && !l_esri_name.empty()) {
            try {
                auto res =
                    io::AuthorityFactory::create(NN_NO_CHECK(dbContext), "ESRI")
                        ->createObjectsFromName(
                            l_esri_name,
                            {io::AuthorityFactory::ObjectType::PROJECTED_CRS},
                            false);
                if (res.size() == 1) {
                    const auto definition = dbContext->getTextDefinition(
                        "projected_crs", "ESRI",
                        res.front()->identifiers()[0]->code());
                    if (starts_with(definition, "PROJCS")) {
                        if (_isEquivalentTo(
                                dynamic_cast<IComparable *>(res.front().get()),
                                util::IComparable::Criterion::EQUIVALENT)) {
                            formatter->ingestWKTNode(
                                io::WKTNode::createFrom(definition));
                            return;
                        }
                    }
                }
            } catch (const std::exception &) {
            }
        }
    }

    const auto exportAxis = [&l_coordinateSystem, &axisList, &formatter]() {
        const auto oldAxisOutputRule = formatter->outputAxis();
        if (oldAxisOutputRule ==
            io::WKTFormatter::OutputAxisRule::WKT1_GDAL_EPSG_STYLE) {
            if (&axisList[0]->direction() == &cs::AxisDirection::EAST &&
                &axisList[1]->direction() == &cs::AxisDirection::NORTH) {
                formatter->setOutputAxis(io::WKTFormatter::OutputAxisRule::YES);
            }
        }
        l_coordinateSystem->_exportToWKT(formatter);
        formatter->setOutputAxis(oldAxisOutputRule);
    };

    if (!isWKT2 && !formatter->useESRIDialect() &&
        starts_with(nameStr(), "Popular Visualisation CRS / Mercator")) {
        formatter->startNode(io::WKTConstants::PROJCS, !l_identifiers.empty());
        formatter->addQuotedString(nameStr());
        formatter->setTOWGS84Parameters({0, 0, 0, 0, 0, 0, 0});
        baseCRS()->_exportToWKT(formatter);
        formatter->setTOWGS84Parameters({});

        formatter->startNode(io::WKTConstants::PROJECTION, false);
        formatter->addQuotedString("Mercator_1SP");
        formatter->endNode();

        formatter->startNode(io::WKTConstants::PARAMETER, false);
        formatter->addQuotedString("central_meridian");
        formatter->add(0.0);
        formatter->endNode();

        formatter->startNode(io::WKTConstants::PARAMETER, false);
        formatter->addQuotedString("scale_factor");
        formatter->add(1.0);
        formatter->endNode();

        formatter->startNode(io::WKTConstants::PARAMETER, false);
        formatter->addQuotedString("false_easting");
        formatter->add(0.0);
        formatter->endNode();

        formatter->startNode(io::WKTConstants::PARAMETER, false);
        formatter->addQuotedString("false_northing");
        formatter->add(0.0);
        formatter->endNode();

        axisList[0]->unit()._exportToWKT(formatter);
        exportAxis();
        derivingConversionRef()->addWKTExtensionNode(formatter);
        ObjectUsage::baseExportToWKT(formatter);
        formatter->endNode();
        return;
    }

    formatter->startNode(isWKT2 ? io::WKTConstants::PROJCRS
                                : io::WKTConstants::PROJCS,
                         !l_identifiers.empty());

    if (formatter->useESRIDialect()) {
        if (l_esri_name.empty()) {
            l_name = io::WKTFormatter::morphNameToESRI(l_name);
        } else {
            const std::string &l_esri_name_ref(l_esri_name);
            l_name = l_esri_name_ref;
        }
    }
    if (!isWKT2 && !formatter->useESRIDialect() && isDeprecated()) {
        l_name += " (deprecated)";
    }
    formatter->addQuotedString(l_name);

    const auto &l_baseCRS = d->baseCRS();
    const auto &geodeticCRSAxisList = l_baseCRS->coordinateSystem()->axisList();

    if (isWKT2) {
        formatter->startNode(
            (formatter->use2019Keywords() &&
             dynamic_cast<const GeographicCRS *>(l_baseCRS.get()))
                ? io::WKTConstants::BASEGEOGCRS
                : io::WKTConstants::BASEGEODCRS,
            formatter->use2019Keywords() && !l_baseCRS->identifiers().empty());
        formatter->addQuotedString(l_baseCRS->nameStr());
        l_baseCRS->exportDatumOrDatumEnsembleToWkt(formatter);
        // insert ellipsoidal cs unit when the units of the map
        // projection angular parameters are not explicitly given within those
        // parameters. See
        // http://docs.opengeospatial.org/is/12-063r5/12-063r5.html#61
        if (formatter->primeMeridianOrParameterUnitOmittedIfSameAsAxis()) {
            geodeticCRSAxisList[0]->unit()._exportToWKT(formatter);
        }
        l_baseCRS->primeMeridian()->_exportToWKT(formatter);
        if (formatter->use2019Keywords() &&
            !(formatter->idOnTopLevelOnly() && formatter->topLevelHasId())) {
            l_baseCRS->formatID(formatter);
        }
        formatter->endNode();
    } else {
        const auto oldAxisOutputRule = formatter->outputAxis();
        formatter->setOutputAxis(io::WKTFormatter::OutputAxisRule::NO);
        l_baseCRS->_exportToWKT(formatter);
        formatter->setOutputAxis(oldAxisOutputRule);
    }

    formatter->pushAxisLinearUnit(
        common::UnitOfMeasure::create(axisList[0]->unit()));

    formatter->pushAxisAngularUnit(
        common::UnitOfMeasure::create(geodeticCRSAxisList[0]->unit()));

    derivingConversionRef()->_exportToWKT(formatter);

    formatter->popAxisAngularUnit();

    formatter->popAxisLinearUnit();

    if (!isWKT2) {
        axisList[0]->unit()._exportToWKT(formatter);
    }

    exportAxis();

    if (!isWKT2 && !formatter->useESRIDialect()) {
        const auto &extensionProj4 = CRS::getPrivate()->extensionProj4_;
        if (!extensionProj4.empty()) {
            formatter->startNode(io::WKTConstants::EXTENSION, false);
            formatter->addQuotedString("PROJ4");
            formatter->addQuotedString(extensionProj4);
            formatter->endNode();
        } else {
            derivingConversionRef()->addWKTExtensionNode(formatter);
        }
    }

    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
    return;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ProjectedCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("ProjectedCRS", !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    writer->AddObjKey("base_crs");
    formatter->setAllowIDInImmediateChild();
    baseCRS()->_exportToJSON(formatter);

    writer->AddObjKey("conversion");
    formatter->setOmitTypeInImmediateChild();
    derivingConversionRef()->_exportToJSON(formatter);

    writer->AddObjKey("coordinate_system");
    formatter->setOmitTypeInImmediateChild();
    coordinateSystem()->_exportToJSON(formatter);

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

void ProjectedCRS::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(io::FormattingException)
{
    const auto &extensionProj4 = CRS::getPrivate()->extensionProj4_;
    if (!extensionProj4.empty()) {
        formatter->ingestPROJString(
            replaceAll(extensionProj4, " +type=crs", ""));
        formatter->addNoDefs(false);
        return;
    }

    derivingConversionRef()->_exportToPROJString(formatter);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ProjectedCRS from a base CRS, a deriving
 * operation::Conversion
 * and a coordinate system.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param baseCRSIn The base CRS, a GeodeticCRS that is generally a
 * GeographicCRS.
 * @param derivingConversionIn The deriving operation::Conversion (typically
 * using a map
 * projection method)
 * @param csIn The coordniate system.
 * @return new ProjectedCRS.
 */
ProjectedCRSNNPtr
ProjectedCRS::create(const util::PropertyMap &properties,
                     const GeodeticCRSNNPtr &baseCRSIn,
                     const operation::ConversionNNPtr &derivingConversionIn,
                     const cs::CartesianCSNNPtr &csIn) {
    auto crs = ProjectedCRS::nn_make_shared<ProjectedCRS>(
        baseCRSIn, derivingConversionIn, csIn);
    crs->assignSelf(crs);
    crs->setProperties(properties);
    crs->setDerivingConversionCRS();
    crs->CRS::getPrivate()->setNonStandardProperties(properties);
    return crs;
}

// ---------------------------------------------------------------------------

bool ProjectedCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherProjCRS = dynamic_cast<const ProjectedCRS *>(other);
    if (otherProjCRS != nullptr &&
        criterion == util::IComparable::Criterion::EQUIVALENT &&
        (d->baseCRS_->hasImplicitCS() ||
         otherProjCRS->d->baseCRS_->hasImplicitCS())) {
        // If one of the 2 base CRS has implicit coordinate system, then
        // relax the check. The axis order of the base CRS doesn't matter
        // for most purposes.
        criterion =
            util::IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS;
    }
    return other != nullptr && util::isOfExactType<ProjectedCRS>(*other) &&
           DerivedCRS::_isEquivalentTo(other, criterion, dbContext);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ProjectedCRSNNPtr
ProjectedCRS::alterParametersLinearUnit(const common::UnitOfMeasure &unit,
                                        bool convertToNewUnit) const {
    return create(
        createPropertyMap(this), baseCRS(),
        derivingConversion()->alterParametersLinearUnit(unit, convertToNewUnit),
        coordinateSystem());
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ProjectedCRS::addUnitConvertAndAxisSwap(io::PROJStringFormatter *formatter,
                                             bool axisSpecFound) const {
    ProjectedCRS::addUnitConvertAndAxisSwap(d->coordinateSystem()->axisList(),
                                            formatter, axisSpecFound);
}

void ProjectedCRS::addUnitConvertAndAxisSwap(
    const std::vector<cs::CoordinateSystemAxisNNPtr> &axisListIn,
    io::PROJStringFormatter *formatter, bool axisSpecFound) {
    const auto &unit = axisListIn[0]->unit();
    const auto *zUnit =
        axisListIn.size() == 3 ? &(axisListIn[2]->unit()) : nullptr;
    if (!unit._isEquivalentTo(common::UnitOfMeasure::METRE,
                              util::IComparable::Criterion::EQUIVALENT) ||
        (zUnit &&
         !zUnit->_isEquivalentTo(common::UnitOfMeasure::METRE,
                                 util::IComparable::Criterion::EQUIVALENT))) {
        auto projUnit = unit.exportToPROJString();
        const double toSI = unit.conversionToSI();
        if (!formatter->getCRSExport()) {
            formatter->addStep("unitconvert");
            formatter->addParam("xy_in", "m");
            if (zUnit)
                formatter->addParam("z_in", "m");

            if (projUnit.empty()) {
                formatter->addParam("xy_out", toSI);
            } else {
                formatter->addParam("xy_out", projUnit);
            }
            if (zUnit) {
                auto projZUnit = zUnit->exportToPROJString();
                const double zToSI = zUnit->conversionToSI();
                if (projZUnit.empty()) {
                    formatter->addParam("z_out", zToSI);
                } else {
                    formatter->addParam("z_out", projZUnit);
                }
            }
        } else {
            if (projUnit.empty()) {
                formatter->addParam("to_meter", toSI);
            } else {
                formatter->addParam("units", projUnit);
            }
        }
    } else if (formatter->getCRSExport() &&
               !formatter->getLegacyCRSToCRSContext()) {
        formatter->addParam("units", "m");
    }

    if (!axisSpecFound &&
        (!formatter->getCRSExport() || formatter->getLegacyCRSToCRSContext())) {
        const auto &dir0 = axisListIn[0]->direction();
        const auto &dir1 = axisListIn[1]->direction();
        if (!(&dir0 == &cs::AxisDirection::EAST &&
              &dir1 == &cs::AxisDirection::NORTH) &&
            // For polar projections, that have south+south direction,
            // we don't want to mess with axes.
            dir0 != dir1) {

            const char *order[2] = {nullptr, nullptr};
            for (int i = 0; i < 2; i++) {
                const auto &dir = axisListIn[i]->direction();
                if (&dir == &cs::AxisDirection::WEST)
                    order[i] = "-1";
                else if (&dir == &cs::AxisDirection::EAST)
                    order[i] = "1";
                else if (&dir == &cs::AxisDirection::SOUTH)
                    order[i] = "-2";
                else if (&dir == &cs::AxisDirection::NORTH)
                    order[i] = "2";
            }

            if (order[0] && order[1]) {
                formatter->addStep("axisswap");
                char orderStr[10];
                snprintf(orderStr, sizeof(orderStr), "%.2s,%.2s", order[0],
                         order[1]);
                formatter->addParam("order", orderStr);
            }
        } else {
            const auto &name0 = axisListIn[0]->nameStr();
            const auto &name1 = axisListIn[1]->nameStr();
            const bool northingEasting = ci_starts_with(name0, "northing") &&
                                         ci_starts_with(name1, "easting");
            // case of EPSG:32661 ["WGS 84 / UPS North (N,E)]"
            // case of EPSG:32761 ["WGS 84 / UPS South (N,E)]"
            if (((&dir0 == &cs::AxisDirection::SOUTH &&
                  &dir1 == &cs::AxisDirection::SOUTH) ||
                 (&dir0 == &cs::AxisDirection::NORTH &&
                  &dir1 == &cs::AxisDirection::NORTH)) &&
                northingEasting) {
                formatter->addStep("axisswap");
                formatter->addParam("order", "2,1");
            }
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Identify the CRS with reference CRSs.
 *
 * The candidate CRSs are either hard-coded, or looked in the database when
 * authorityFactory is not null.
 *
 * Note that the implementation uses a set of heuristics to have a good
 * compromise of successful identifications over execution time. It might miss
 * legitimate matches in some circumstances.
 *
 * The method returns a list of matching reference CRS, and the percentage
 * (0-100) of confidence in the match. The list is sorted by decreasing
 * confidence.
 *
 * 100% means that the name of the reference entry
 * perfectly matches the CRS name, and both are equivalent. In which case a
 * single result is returned.
 * 90% means that CRS are equivalent, but the names are not exactly the same.
 * 70% means that CRS are equivalent (equivalent base CRS, conversion and
 * coordinate system), but the names are not equivalent.
 * 60% means that CRS have strong similarity (equivalent base datum, conversion
 * and coordinate system), but the names are not equivalent.
 * 50% means that CRS have similarity (equivalent base ellipsoid and
 * conversion),
 * but the coordinate system do not match (e.g. different axis ordering or
 * axis unit).
 * 25% means that the CRS are not equivalent, but there is some similarity in
 * the names.
 *
 * For the purpose of this function, equivalence is tested with the
 * util::IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS, that is
 * to say that the axis order of the base GeographicCRS is ignored.
 *
 * @param authorityFactory Authority factory (or null, but degraded
 * functionality)
 * @return a list of matching reference CRS, and the percentage (0-100) of
 * confidence in the match.
 */
std::list<std::pair<ProjectedCRSNNPtr, int>>
ProjectedCRS::identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<ProjectedCRSNNPtr, int> Pair;
    std::list<Pair> res;

    const auto &thisName(nameStr());
    io::DatabaseContextPtr dbContext =
        authorityFactory ? authorityFactory->databaseContext().as_nullable()
                         : nullptr;

    std::list<std::pair<GeodeticCRSNNPtr, int>> baseRes;
    const auto &l_baseCRS(baseCRS());
    const auto l_datum = l_baseCRS->datumNonNull(dbContext);
    const bool significantNameForDatum =
        !ci_starts_with(l_datum->nameStr(), "unknown") &&
        l_datum->nameStr() != "unnamed";
    const auto &ellipsoid = l_baseCRS->ellipsoid();
    auto geogCRS = dynamic_cast<const GeographicCRS *>(l_baseCRS.get());
    if (geogCRS && geogCRS->coordinateSystem()->axisOrder() ==
                       cs::EllipsoidalCS::AxisOrder::LONG_EAST_LAT_NORTH) {
        baseRes =
            GeographicCRS::create(
                util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                        geogCRS->nameStr()),
                geogCRS->datum(), geogCRS->datumEnsemble(),
                cs::EllipsoidalCS::createLatitudeLongitude(
                    geogCRS->coordinateSystem()->axisList()[0]->unit()))
                ->identify(authorityFactory);
    } else {
        baseRes = l_baseCRS->identify(authorityFactory);
    }

    int zone = 0;
    bool north = false;

    auto computeConfidence = [&thisName](const std::string &crsName) {
        return crsName == thisName ? 100
               : metadata::Identifier::isEquivalentName(crsName.c_str(),
                                                        thisName.c_str())
                   ? 90
                   : 70;
    };

    const auto &conv = derivingConversionRef();
    const auto &cs = coordinateSystem();

    if (baseRes.size() == 1 && baseRes.front().second >= 70 &&
        (authorityFactory == nullptr ||
         authorityFactory->getAuthority().empty() ||
         authorityFactory->getAuthority() == metadata::Identifier::EPSG) &&
        conv->isUTM(zone, north) &&
        cs->_isEquivalentTo(
            cs::CartesianCS::createEastingNorthing(common::UnitOfMeasure::METRE)
                .get(),
            util::IComparable::Criterion::EQUIVALENT, dbContext)) {

        auto computeUTMCRSName = [](const char *base, int l_zone,
                                    bool l_north) {
            return base + toString(l_zone) + (l_north ? "N" : "S");
        };

        if (baseRes.front().first->_isEquivalentTo(
                GeographicCRS::EPSG_4326.get(),
                util::IComparable::Criterion::EQUIVALENT, dbContext)) {
            std::string crsName(
                computeUTMCRSName("WGS 84 / UTM zone ", zone, north));
            res.emplace_back(
                ProjectedCRS::create(
                    createMapNameEPSGCode(crsName.c_str(),
                                          (north ? 32600 : 32700) + zone),
                    GeographicCRS::EPSG_4326, conv->identify(), cs),
                computeConfidence(crsName));
            return res;
        } else if (((zone >= 1 && zone <= 22) || zone == 59 || zone == 60) &&
                   north &&
                   baseRes.front().first->_isEquivalentTo(
                       GeographicCRS::EPSG_4267.get(),
                       util::IComparable::Criterion::EQUIVALENT, dbContext)) {
            std::string crsName(
                computeUTMCRSName("NAD27 / UTM zone ", zone, north));
            res.emplace_back(
                ProjectedCRS::create(
                    createMapNameEPSGCode(crsName.c_str(),
                                          (zone >= 59) ? 3370 + zone - 59
                                                       : 26700 + zone),
                    GeographicCRS::EPSG_4267, conv->identify(), cs),
                computeConfidence(crsName));
            return res;
        } else if (((zone >= 1 && zone <= 23) || zone == 59 || zone == 60) &&
                   north &&
                   baseRes.front().first->_isEquivalentTo(
                       GeographicCRS::EPSG_4269.get(),
                       util::IComparable::Criterion::EQUIVALENT, dbContext)) {
            std::string crsName(
                computeUTMCRSName("NAD83 / UTM zone ", zone, north));
            res.emplace_back(
                ProjectedCRS::create(
                    createMapNameEPSGCode(crsName.c_str(),
                                          (zone >= 59) ? 3372 + zone - 59
                                                       : 26900 + zone),
                    GeographicCRS::EPSG_4269, conv->identify(), cs),
                computeConfidence(crsName));
            return res;
        }
    }

    const bool l_implicitCS = hasImplicitCS();
    const auto addCRS = [&](const ProjectedCRSNNPtr &crs, const bool eqName,
                            bool hasNonMatchingId) -> Pair {
        const auto &l_unit = cs->axisList()[0]->unit();
        if ((_isEquivalentTo(crs.get(),
                             util::IComparable::Criterion::
                                 EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS,
                             dbContext) ||
             (l_implicitCS &&
              l_unit._isEquivalentTo(
                  crs->coordinateSystem()->axisList()[0]->unit(),
                  util::IComparable::Criterion::EQUIVALENT) &&
              l_baseCRS->_isEquivalentTo(
                  crs->baseCRS().get(),
                  util::IComparable::Criterion::
                      EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS,
                  dbContext) &&
              derivingConversionRef()->_isEquivalentTo(
                  crs->derivingConversionRef().get(),
                  util::IComparable::Criterion::EQUIVALENT, dbContext))) &&
            !((baseCRS()->datumNonNull(dbContext)->nameStr() == "unknown" &&
               crs->baseCRS()->datumNonNull(dbContext)->nameStr() !=
                   "unknown") ||
              (baseCRS()->datumNonNull(dbContext)->nameStr() != "unknown" &&
               crs->baseCRS()->datumNonNull(dbContext)->nameStr() ==
                   "unknown"))) {
            if (crs->nameStr() == thisName) {
                res.clear();
                res.emplace_back(crs, hasNonMatchingId ? 70 : 100);
            } else {
                res.emplace_back(crs, eqName ? 90 : 70);
            }
        } else if (ellipsoid->_isEquivalentTo(
                       crs->baseCRS()->ellipsoid().get(),
                       util::IComparable::Criterion::EQUIVALENT, dbContext) &&
                   derivingConversionRef()->_isEquivalentTo(
                       crs->derivingConversionRef().get(),
                       util::IComparable::Criterion::EQUIVALENT, dbContext)) {
            if ((l_implicitCS &&
                 l_unit._isEquivalentTo(
                     crs->coordinateSystem()->axisList()[0]->unit(),
                     util::IComparable::Criterion::EQUIVALENT)) ||
                cs->_isEquivalentTo(crs->coordinateSystem().get(),
                                    util::IComparable::Criterion::EQUIVALENT,
                                    dbContext)) {
                if (!significantNameForDatum ||
                    l_datum->_isEquivalentTo(
                        crs->baseCRS()->datumNonNull(dbContext).get(),
                        util::IComparable::Criterion::EQUIVALENT)) {
                    res.emplace_back(crs, 70);
                } else {
                    res.emplace_back(crs, 60);
                }
            } else {
                res.emplace_back(crs, 50);
            }
        } else {
            res.emplace_back(crs, 25);
        }
        return res.back();
    };

    if (authorityFactory) {

        const bool insignificantName = thisName.empty() ||
                                       ci_equal(thisName, "unknown") ||
                                       ci_equal(thisName, "unnamed");
        bool foundEquivalentName = false;

        bool hasNonMatchingId = false;
        if (hasCodeCompatibleOfAuthorityFactory(this, authorityFactory)) {
            // If the CRS has already an id, check in the database for the
            // official object, and verify that they are equivalent.
            for (const auto &id : identifiers()) {
                if (hasCodeCompatibleOfAuthorityFactory(id, authorityFactory)) {
                    try {
                        auto crs = io::AuthorityFactory::create(
                                       authorityFactory->databaseContext(),
                                       *id->codeSpace())
                                       ->createProjectedCRS(id->code());
                        bool match = _isEquivalentTo(
                            crs.get(),
                            util::IComparable::Criterion::
                                EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS,
                            dbContext);
                        res.emplace_back(crs, match ? 100 : 25);
                        if (match) {
                            return res;
                        }
                    } catch (const std::exception &) {
                    }
                }
            }
            hasNonMatchingId = true;
        } else if (!insignificantName) {
            for (int ipass = 0; ipass < 2; ipass++) {
                const bool approximateMatch = ipass == 1;
                auto objects = authorityFactory->createObjectsFromNameEx(
                    thisName, {io::AuthorityFactory::ObjectType::PROJECTED_CRS},
                    approximateMatch);
                for (const auto &pairObjName : objects) {
                    auto crs = util::nn_dynamic_pointer_cast<ProjectedCRS>(
                        pairObjName.first);
                    assert(crs);
                    auto crsNN = NN_NO_CHECK(crs);
                    const bool eqName = metadata::Identifier::isEquivalentName(
                        thisName.c_str(), pairObjName.second.c_str());
                    foundEquivalentName |= eqName;

                    if (addCRS(crsNN, eqName, false).second == 100) {
                        return res;
                    }
                }
                if (!res.empty()) {
                    break;
                }
            }
        }

        const auto lambdaSort = [&thisName](const Pair &a, const Pair &b) {
            // First consider confidence
            if (a.second > b.second) {
                return true;
            }
            if (a.second < b.second) {
                return false;
            }

            // Then consider exact name matching
            const auto &aName(a.first->nameStr());
            const auto &bName(b.first->nameStr());
            if (aName == thisName && bName != thisName) {
                return true;
            }
            if (bName == thisName && aName != thisName) {
                return false;
            }

            // Arbitrary final sorting criterion
            return aName < bName;
        };

        // Sort results
        res.sort(lambdaSort);

        if (!foundEquivalentName && (res.empty() || res.front().second < 50)) {
            std::set<std::pair<std::string, std::string>> alreadyKnown;
            for (const auto &pair : res) {
                const auto &ids = pair.first->identifiers();
                assert(!ids.empty());
                alreadyKnown.insert(std::pair<std::string, std::string>(
                    *(ids[0]->codeSpace()), ids[0]->code()));
            }

            auto self = NN_NO_CHECK(std::dynamic_pointer_cast<ProjectedCRS>(
                shared_from_this().as_nullable()));
            auto candidates =
                authorityFactory->createProjectedCRSFromExisting(self);
            for (const auto &crs : candidates) {
                const auto &ids = crs->identifiers();
                assert(!ids.empty());
                if (alreadyKnown.find(std::pair<std::string, std::string>(
                        *(ids[0]->codeSpace()), ids[0]->code())) !=
                    alreadyKnown.end()) {
                    continue;
                }

                addCRS(crs, insignificantName, hasNonMatchingId);
            }

            res.sort(lambdaSort);
        }

        // Keep only results of the highest confidence
        if (res.size() >= 2) {
            const auto highestConfidence = res.front().second;
            std::list<Pair> newRes;
            for (const auto &pair : res) {
                if (pair.second == highestConfidence) {
                    newRes.push_back(pair);
                } else {
                    break;
                }
            }
            return newRes;
        }
    }

    return res;
}

// ---------------------------------------------------------------------------

/** \brief Return a variant of this CRS "demoted" to a 2D one, if not already
 * the case.
 *
 *
 * @param newName Name of the new CRS. If empty, nameStr() will be used.
 * @param dbContext Database context to look for potentially already registered
 *                  2D CRS. May be nullptr.
 * @return a new CRS demoted to 2D, or the current one if already 2D or not
 * applicable.
 * @since 6.3
 */
ProjectedCRSNNPtr
ProjectedCRS::demoteTo2D(const std::string &newName,
                         const io::DatabaseContextPtr &dbContext) const {

    const auto &axisList = coordinateSystem()->axisList();
    if (axisList.size() == 3) {
        auto cs = cs::CartesianCS::create(util::PropertyMap(), axisList[0],
                                          axisList[1]);
        const auto &l_baseCRS = baseCRS();
        const auto geogCRS =
            dynamic_cast<const GeographicCRS *>(l_baseCRS.get());
        const auto newBaseCRS =
            geogCRS ? util::nn_static_pointer_cast<GeodeticCRS>(
                          geogCRS->demoteTo2D(std::string(), dbContext))
                    : l_baseCRS;
        return ProjectedCRS::create(
            util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                    !newName.empty() ? newName : nameStr()),
            newBaseCRS, derivingConversion(), cs);
    }

    return NN_NO_CHECK(std::dynamic_pointer_cast<ProjectedCRS>(
        shared_from_this().as_nullable()));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
ProjectedCRS::_identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<CRSNNPtr, int> Pair;
    std::list<Pair> res;
    auto resTemp = identify(authorityFactory);
    for (const auto &pair : resTemp) {
        res.emplace_back(pair.first, pair.second);
    }
    return res;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
InvalidCompoundCRSException::InvalidCompoundCRSException(const char *message)
    : Exception(message) {}

// ---------------------------------------------------------------------------

InvalidCompoundCRSException::InvalidCompoundCRSException(
    const std::string &message)
    : Exception(message) {}

// ---------------------------------------------------------------------------

InvalidCompoundCRSException::~InvalidCompoundCRSException() = default;

// ---------------------------------------------------------------------------

InvalidCompoundCRSException::InvalidCompoundCRSException(
    const InvalidCompoundCRSException &) = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct CompoundCRS::Private {
    std::vector<CRSNNPtr> components_{};
};
//! @endcond

// ---------------------------------------------------------------------------

CompoundCRS::CompoundCRS(const std::vector<CRSNNPtr> &components)
    : CRS(), d(std::make_unique<Private>()) {
    d->components_ = components;
}

// ---------------------------------------------------------------------------

CompoundCRS::CompoundCRS(const CompoundCRS &other)
    : CRS(other), d(std::make_unique<Private>(*other.d)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CompoundCRS::~CompoundCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

CRSNNPtr CompoundCRS::_shallowClone() const {
    auto crs(CompoundCRS::nn_make_shared<CompoundCRS>(*this));
    crs->assignSelf(crs);
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the components of a CompoundCRS.
 *
 * @return the components.
 */
const std::vector<CRSNNPtr> &
CompoundCRS::componentReferenceSystems() PROJ_PURE_DEFN {
    return d->components_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CompoundCRS from a vector of CRS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param components the component CRS of the CompoundCRS.
 * @return new CompoundCRS.
 * @throw InvalidCompoundCRSException if the object cannot be constructed.
 */
CompoundCRSNNPtr CompoundCRS::create(const util::PropertyMap &properties,
                                     const std::vector<CRSNNPtr> &components) {

    if (components.size() < 2) {
        throw InvalidCompoundCRSException(
            "compound CRS should have at least 2 components");
    }

    auto comp0 = components[0].get();
    auto comp0Bound = dynamic_cast<const BoundCRS *>(comp0);
    if (comp0Bound) {
        comp0 = comp0Bound->baseCRS().get();
    }
    auto comp0Geog = dynamic_cast<const GeographicCRS *>(comp0);
    auto comp0Proj = dynamic_cast<const ProjectedCRS *>(comp0);
    auto comp0DerPr = dynamic_cast<const DerivedProjectedCRS *>(comp0);
    auto comp0Eng = dynamic_cast<const EngineeringCRS *>(comp0);

    auto comp1 = components[1].get();
    auto comp1Bound = dynamic_cast<const BoundCRS *>(comp1);
    if (comp1Bound) {
        comp1 = comp1Bound->baseCRS().get();
    }
    auto comp1Vert = dynamic_cast<const VerticalCRS *>(comp1);
    auto comp1Eng = dynamic_cast<const EngineeringCRS *>(comp1);
    // Loose validation based on
    // http://docs.opengeospatial.org/as/18-005r5/18-005r5.html#34
    bool ok = false;
    const bool comp1IsVertOrEng1 =
        comp1Vert ||
        (comp1Eng && comp1Eng->coordinateSystem()->axisList().size() == 1);
    if ((comp0Geog && comp0Geog->coordinateSystem()->axisList().size() == 2 &&
         comp1IsVertOrEng1) ||
        (comp0Proj && comp0Proj->coordinateSystem()->axisList().size() == 2 &&
         comp1IsVertOrEng1) ||
        (comp0DerPr && comp0DerPr->coordinateSystem()->axisList().size() == 2 &&
         comp1IsVertOrEng1) ||
        (comp0Eng && comp0Eng->coordinateSystem()->axisList().size() <= 2 &&
         comp1Vert)) {
        // Spatial compound coordinate reference system
        ok = true;
    } else {
        bool isComp0Spatial = comp0Geog || comp0Proj || comp0DerPr ||
                              comp0Eng ||
                              dynamic_cast<const GeodeticCRS *>(comp0) ||
                              dynamic_cast<const VerticalCRS *>(comp0);
        if (isComp0Spatial && dynamic_cast<const TemporalCRS *>(comp1)) {
            // Spatio-temporal compound coordinate reference system
            ok = true;
        } else if (isComp0Spatial &&
                   dynamic_cast<const ParametricCRS *>(comp1)) {
            // Spatio-parametric compound coordinate reference system
            ok = true;
        }
    }
    if (!ok) {
        throw InvalidCompoundCRSException(
            "components of the compound CRS do not belong to one of the "
            "allowed combinations of "
            "http://docs.opengeospatial.org/as/18-005r5/18-005r5.html#34");
    }

    auto compoundCRS(CompoundCRS::nn_make_shared<CompoundCRS>(components));
    compoundCRS->assignSelf(compoundCRS);
    compoundCRS->setProperties(properties);
    if (!properties.get(common::IdentifiedObject::NAME_KEY)) {
        std::string name;
        for (const auto &crs : components) {
            if (!name.empty()) {
                name += " + ";
            }
            const auto &l_name = crs->nameStr();
            if (!l_name.empty()) {
                name += l_name;
            } else {
                name += "unnamed";
            }
        }
        util::PropertyMap propertyName;
        propertyName.set(common::IdentifiedObject::NAME_KEY, name);
        compoundCRS->setProperties(propertyName);
    }

    return compoundCRS;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

/** \brief Instantiate a CompoundCRS, a Geographic 3D CRS or a Projected CRS
 * from a vector of CRS.
 *
 * Be a bit "lax", in allowing formulations like EPSG:4326+4326 or
 * EPSG:32631+4326 to express Geographic 3D CRS / Projected3D CRS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param components the component CRS of the CompoundCRS.
 * @return new CRS.
 * @throw InvalidCompoundCRSException if the object cannot be constructed.
 */
CRSNNPtr CompoundCRS::createLax(const util::PropertyMap &properties,
                                const std::vector<CRSNNPtr> &components,
                                const io::DatabaseContextPtr &dbContext) {

    if (components.size() == 2) {
        auto comp0 = components[0].get();
        auto comp1 = components[1].get();
        auto comp0Geog = dynamic_cast<const GeographicCRS *>(comp0);
        auto comp0Proj = dynamic_cast<const ProjectedCRS *>(comp0);
        auto comp0Bound = dynamic_cast<const BoundCRS *>(comp0);
        if (comp0Geog == nullptr && comp0Proj == nullptr) {
            if (comp0Bound) {
                const auto *baseCRS = comp0Bound->baseCRS().get();
                comp0Geog = dynamic_cast<const GeographicCRS *>(baseCRS);
                comp0Proj = dynamic_cast<const ProjectedCRS *>(baseCRS);
            }
        }
        auto comp1Geog = dynamic_cast<const GeographicCRS *>(comp1);
        if ((comp0Geog != nullptr || comp0Proj != nullptr) &&
            comp1Geog != nullptr) {
            const auto horizGeog =
                (comp0Proj != nullptr)
                    ? comp0Proj->baseCRS().as_nullable().get()
                    : comp0Geog;
            if (horizGeog->_isEquivalentTo(
                    comp1Geog->demoteTo2D(std::string(), dbContext).get())) {
                return components[0]
                    ->promoteTo3D(std::string(), dbContext)
                    ->allowNonConformantWKT1Export();
            }
            throw InvalidCompoundCRSException(
                "The 'vertical' geographic CRS is not equivalent to the "
                "geographic CRS of the horizontal part");
        }

        // Detect a COMPD_CS whose VERT_CS is for ellipoidal heights
        auto comp1Vert =
            util::nn_dynamic_pointer_cast<VerticalCRS>(components[1]);
        if (comp1Vert != nullptr && comp1Vert->datum() &&
            comp1Vert->datum()->getWKT1DatumType() == "2002") {
            const auto &axis = comp1Vert->coordinateSystem()->axisList()[0];
            std::string name(components[0]->nameStr());
            if (!(axis->unit()._isEquivalentTo(
                      common::UnitOfMeasure::METRE,
                      util::IComparable::Criterion::EQUIVALENT) &&
                  &(axis->direction()) == &(cs::AxisDirection::UP))) {
                name += " (" + comp1Vert->nameStr() + ')';
            }
            auto newVertAxis = cs::CoordinateSystemAxis::create(
                util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                        cs::AxisName::Ellipsoidal_height),
                cs::AxisAbbreviation::h, axis->direction(), axis->unit());
            return components[0]
                ->promoteTo3D(name, dbContext, newVertAxis)
                ->attachOriginalCompoundCRS(create(
                    properties,
                    comp0Bound ? std::vector<CRSNNPtr>{comp0Bound->baseCRS(),
                                                       components[1]}
                               : components));
        }
    }

    return create(properties, components);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void CompoundCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    const auto &l_components = componentReferenceSystems();
    if (!isWKT2 && formatter->useESRIDialect() && l_components.size() == 2) {
        l_components[0]->_exportToWKT(formatter);
        l_components[1]->_exportToWKT(formatter);
    } else {
        formatter->startNode(isWKT2 ? io::WKTConstants::COMPOUNDCRS
                                    : io::WKTConstants::COMPD_CS,
                             !identifiers().empty());
        formatter->addQuotedString(nameStr());
        if (!l_components.empty()) {
            formatter->setGeogCRSOfCompoundCRS(
                l_components[0]->extractGeographicCRS());
        }
        for (const auto &crs : l_components) {
            crs->_exportToWKT(formatter);
        }
        formatter->setGeogCRSOfCompoundCRS(nullptr);
        ObjectUsage::baseExportToWKT(formatter);
        formatter->endNode();
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void CompoundCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("CompoundCRS", !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    writer->AddObjKey("components");
    {
        auto componentsContext(writer->MakeArrayContext(false));
        for (const auto &crs : componentReferenceSystems()) {
            crs->_exportToJSON(formatter);
        }
    }

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

void CompoundCRS::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(io::FormattingException)
{
    const auto &l_components = componentReferenceSystems();
    if (!l_components.empty()) {
        formatter->setGeogCRSOfCompoundCRS(
            l_components[0]->extractGeographicCRS());
    }
    for (const auto &crs : l_components) {
        auto crs_exportable =
            dynamic_cast<const IPROJStringExportable *>(crs.get());
        if (crs_exportable) {
            crs_exportable->_exportToPROJString(formatter);
        }
    }
    formatter->setGeogCRSOfCompoundCRS(nullptr);
}

// ---------------------------------------------------------------------------

bool CompoundCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherCompoundCRS = dynamic_cast<const CompoundCRS *>(other);
    if (otherCompoundCRS == nullptr ||
        (criterion == util::IComparable::Criterion::STRICT &&
         !ObjectUsage::_isEquivalentTo(other, criterion, dbContext))) {
        return false;
    }
    const auto &components = componentReferenceSystems();
    const auto &otherComponents = otherCompoundCRS->componentReferenceSystems();
    if (components.size() != otherComponents.size()) {
        return false;
    }
    for (size_t i = 0; i < components.size(); i++) {
        if (!components[i]->_isEquivalentTo(otherComponents[i].get(), criterion,
                                            dbContext)) {
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

/** \brief Identify the CRS with reference CRSs.
 *
 * The candidate CRSs are looked in the database when
 * authorityFactory is not null.
 *
 * Note that the implementation uses a set of heuristics to have a good
 * compromise of successful identifications over execution time. It might miss
 * legitimate matches in some circumstances.
 *
 * The method returns a list of matching reference CRS, and the percentage
 * (0-100) of confidence in the match. The list is sorted by decreasing
 * confidence.
 *
 * 100% means that the name of the reference entry
 * perfectly matches the CRS name, and both are equivalent. In which case a
 * single result is returned.
 * 90% means that CRS are equivalent, but the names are not exactly the same.
 * 70% means that CRS are equivalent (equivalent horizontal and vertical CRS),
 * but the names are not equivalent.
 * 25% means that the CRS are not equivalent, but there is some similarity in
 * the names.
 *
 * @param authorityFactory Authority factory (if null, will return an empty
 * list)
 * @return a list of matching reference CRS, and the percentage (0-100) of
 * confidence in the match.
 */
std::list<std::pair<CompoundCRSNNPtr, int>>
CompoundCRS::identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<CompoundCRSNNPtr, int> Pair;
    std::list<Pair> res;

    const auto &thisName(nameStr());

    const auto &components = componentReferenceSystems();
    bool l_implicitCS = components[0]->hasImplicitCS();
    if (!l_implicitCS) {
        const auto projCRS =
            dynamic_cast<const ProjectedCRS *>(components[0].get());
        if (projCRS) {
            l_implicitCS = projCRS->baseCRS()->hasImplicitCS();
        }
    }
    const auto crsCriterion =
        l_implicitCS
            ? util::IComparable::Criterion::EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS
            : util::IComparable::Criterion::EQUIVALENT;

    if (authorityFactory) {
        const io::DatabaseContextNNPtr &dbContext =
            authorityFactory->databaseContext();

        const bool insignificantName = thisName.empty() ||
                                       ci_equal(thisName, "unknown") ||
                                       ci_equal(thisName, "unnamed");
        bool foundEquivalentName = false;

        if (hasCodeCompatibleOfAuthorityFactory(this, authorityFactory)) {
            // If the CRS has already an id, check in the database for the
            // official object, and verify that they are equivalent.
            for (const auto &id : identifiers()) {
                if (hasCodeCompatibleOfAuthorityFactory(id, authorityFactory)) {
                    try {
                        auto crs = io::AuthorityFactory::create(
                                       dbContext, *id->codeSpace())
                                       ->createCompoundCRS(id->code());
                        bool match =
                            _isEquivalentTo(crs.get(), crsCriterion, dbContext);
                        res.emplace_back(crs, match ? 100 : 25);
                        return res;
                    } catch (const std::exception &) {
                    }
                }
            }
        } else if (!insignificantName) {
            for (int ipass = 0; ipass < 2; ipass++) {
                const bool approximateMatch = ipass == 1;
                auto objects = authorityFactory->createObjectsFromName(
                    thisName, {io::AuthorityFactory::ObjectType::COMPOUND_CRS},
                    approximateMatch);
                for (const auto &obj : objects) {
                    auto crs = util::nn_dynamic_pointer_cast<CompoundCRS>(obj);
                    assert(crs);
                    auto crsNN = NN_NO_CHECK(crs);
                    const bool eqName = metadata::Identifier::isEquivalentName(
                        thisName.c_str(), crs->nameStr().c_str());
                    foundEquivalentName |= eqName;
                    if (_isEquivalentTo(crs.get(), crsCriterion, dbContext)) {
                        if (crs->nameStr() == thisName) {
                            res.clear();
                            res.emplace_back(crsNN, 100);
                            return res;
                        }
                        res.emplace_back(crsNN, eqName ? 90 : 70);
                    } else {

                        res.emplace_back(crsNN, 25);
                    }
                }
                if (!res.empty()) {
                    break;
                }
            }
        }

        const auto lambdaSort = [&thisName](const Pair &a, const Pair &b) {
            // First consider confidence
            if (a.second > b.second) {
                return true;
            }
            if (a.second < b.second) {
                return false;
            }

            // Then consider exact name matching
            const auto &aName(a.first->nameStr());
            const auto &bName(b.first->nameStr());
            if (aName == thisName && bName != thisName) {
                return true;
            }
            if (bName == thisName && aName != thisName) {
                return false;
            }

            // Arbitrary final sorting criterion
            return aName < bName;
        };

        // Sort results
        res.sort(lambdaSort);

        if (identifiers().empty() && !foundEquivalentName &&
            (res.empty() || res.front().second < 50)) {
            std::set<std::pair<std::string, std::string>> alreadyKnown;
            for (const auto &pair : res) {
                const auto &ids = pair.first->identifiers();
                assert(!ids.empty());
                alreadyKnown.insert(std::pair<std::string, std::string>(
                    *(ids[0]->codeSpace()), ids[0]->code()));
            }

            auto self = NN_NO_CHECK(std::dynamic_pointer_cast<CompoundCRS>(
                shared_from_this().as_nullable()));
            auto candidates =
                authorityFactory->createCompoundCRSFromExisting(self);
            for (const auto &crs : candidates) {
                const auto &ids = crs->identifiers();
                assert(!ids.empty());
                if (alreadyKnown.find(std::pair<std::string, std::string>(
                        *(ids[0]->codeSpace()), ids[0]->code())) !=
                    alreadyKnown.end()) {
                    continue;
                }

                if (_isEquivalentTo(crs.get(), crsCriterion, dbContext)) {
                    res.emplace_back(crs, insignificantName ? 90 : 70);
                } else {
                    res.emplace_back(crs, 25);
                }
            }

            res.sort(lambdaSort);

            // If there's a single candidate at 90% confidence with same name,
            // then promote it to 100%
            if (res.size() == 1 && res.front().second == 90 &&
                thisName == res.front().first->nameStr()) {
                res.front().second = 100;
            }
        }

        // If we didn't find a match for the CompoundCRS, check if the
        // horizontal and vertical parts are not themselves well known.
        if (identifiers().empty() && res.empty() && components.size() == 2) {
            auto candidatesHorizCRS = components[0]->identify(authorityFactory);
            auto candidatesVertCRS = components[1]->identify(authorityFactory);
            if (candidatesHorizCRS.size() == 1 &&
                candidatesVertCRS.size() == 1 &&
                candidatesHorizCRS.front().second >= 70 &&
                candidatesVertCRS.front().second >= 70) {
                auto newCRS = CompoundCRS::create(
                    util::PropertyMap().set(
                        common::IdentifiedObject::NAME_KEY,
                        candidatesHorizCRS.front().first->nameStr() + " + " +
                            candidatesVertCRS.front().first->nameStr()),
                    {candidatesHorizCRS.front().first,
                     candidatesVertCRS.front().first});
                const bool eqName = metadata::Identifier::isEquivalentName(
                    thisName.c_str(), newCRS->nameStr().c_str());
                res.emplace_back(
                    newCRS,
                    std::min(thisName == newCRS->nameStr() ? 100
                             : eqName                      ? 90
                                                           : 70,
                             std::min(candidatesHorizCRS.front().second,
                                      candidatesVertCRS.front().second)));
            }
        }

        // Keep only results of the highest confidence
        if (res.size() >= 2) {
            const auto highestConfidence = res.front().second;
            std::list<Pair> newRes;
            for (const auto &pair : res) {
                if (pair.second == highestConfidence) {
                    newRes.push_back(pair);
                } else {
                    break;
                }
            }
            return newRes;
        }
    }

    return res;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
CompoundCRS::_identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<CRSNNPtr, int> Pair;
    std::list<Pair> res;
    auto resTemp = identify(authorityFactory);
    for (const auto &pair : resTemp) {
        res.emplace_back(pair.first, pair.second);
    }
    return res;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct PROJ_INTERNAL BoundCRS::Private {
    CRSNNPtr baseCRS_;
    CRSNNPtr hubCRS_;
    operation::TransformationNNPtr transformation_;

    Private(const CRSNNPtr &baseCRSIn, const CRSNNPtr &hubCRSIn,
            const operation::TransformationNNPtr &transformationIn);

    inline const CRSNNPtr &baseCRS() const { return baseCRS_; }
    inline const CRSNNPtr &hubCRS() const { return hubCRS_; }
    inline const operation::TransformationNNPtr &transformation() const {
        return transformation_;
    }
};

BoundCRS::Private::Private(
    const CRSNNPtr &baseCRSIn, const CRSNNPtr &hubCRSIn,
    const operation::TransformationNNPtr &transformationIn)
    : baseCRS_(baseCRSIn), hubCRS_(hubCRSIn),
      transformation_(transformationIn) {}

//! @endcond

// ---------------------------------------------------------------------------

BoundCRS::BoundCRS(const CRSNNPtr &baseCRSIn, const CRSNNPtr &hubCRSIn,
                   const operation::TransformationNNPtr &transformationIn)
    : d(std::make_unique<Private>(baseCRSIn, hubCRSIn, transformationIn)) {}

// ---------------------------------------------------------------------------

BoundCRS::BoundCRS(const BoundCRS &other)
    : CRS(other),
      d(std::make_unique<Private>(other.d->baseCRS(), other.d->hubCRS(),
                                  other.d->transformation())) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
BoundCRS::~BoundCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

BoundCRSNNPtr BoundCRS::shallowCloneAsBoundCRS() const {
    auto crs(BoundCRS::nn_make_shared<BoundCRS>(*this));
    crs->assignSelf(crs);
    return crs;
}

// ---------------------------------------------------------------------------

CRSNNPtr BoundCRS::_shallowClone() const { return shallowCloneAsBoundCRS(); }

// ---------------------------------------------------------------------------

/** \brief Return the base CRS.
 *
 * This is the CRS into which coordinates of the BoundCRS are expressed.
 *
 * @return the base CRS.
 */
const CRSNNPtr &BoundCRS::baseCRS() PROJ_PURE_DEFN { return d->baseCRS_; }

// ---------------------------------------------------------------------------

// The only legit caller is BoundCRS::baseCRSWithCanonicalBoundCRS()
void CRS::setCanonicalBoundCRS(const BoundCRSNNPtr &boundCRS) {

    d->canonicalBoundCRS_ = boundCRS;
}

// ---------------------------------------------------------------------------

/** \brief Return a shallow clone of the base CRS that points to a
 * shallow clone of this BoundCRS.
 *
 * The base CRS is the CRS into which coordinates of the BoundCRS are expressed.
 *
 * The returned CRS will actually be a shallow clone of the actual base CRS,
 * with the extra property that CRS::canonicalBoundCRS() will point to a
 * shallow clone of this BoundCRS. Use this only if you want to work with
 * the base CRS object rather than the BoundCRS, but wanting to be able to
 * retrieve the BoundCRS later.
 *
 * @return the base CRS.
 */
CRSNNPtr BoundCRS::baseCRSWithCanonicalBoundCRS() const {
    auto baseCRSClone = baseCRS()->_shallowClone();
    baseCRSClone->setCanonicalBoundCRS(shallowCloneAsBoundCRS());
    return baseCRSClone;
}

// ---------------------------------------------------------------------------

/** \brief Return the target / hub CRS.
 *
 * @return the hub CRS.
 */
const CRSNNPtr &BoundCRS::hubCRS() PROJ_PURE_DEFN { return d->hubCRS_; }

// ---------------------------------------------------------------------------

/** \brief Return the transformation to the hub RS.
 *
 * @return transformation.
 */
const operation::TransformationNNPtr &
BoundCRS::transformation() PROJ_PURE_DEFN {
    return d->transformation_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a BoundCRS from a base CRS, a hub CRS and a
 * transformation.
 *
 * @param properties See \ref general_properties.
 * @param baseCRSIn base CRS.
 * @param hubCRSIn hub CRS.
 * @param transformationIn transformation from base CRS to hub CRS.
 * @return new BoundCRS.
 * @since PROJ 8.2
 */
BoundCRSNNPtr
BoundCRS::create(const util::PropertyMap &properties, const CRSNNPtr &baseCRSIn,
                 const CRSNNPtr &hubCRSIn,
                 const operation::TransformationNNPtr &transformationIn) {
    auto crs = BoundCRS::nn_make_shared<BoundCRS>(baseCRSIn, hubCRSIn,
                                                  transformationIn);
    crs->assignSelf(crs);
    const auto &l_name = baseCRSIn->nameStr();
    if (properties.get(common::IdentifiedObject::NAME_KEY) == nullptr &&
        !l_name.empty()) {
        auto newProperties(properties);
        newProperties.set(common::IdentifiedObject::NAME_KEY, l_name);
        crs->setProperties(newProperties);
    } else {
        crs->setProperties(properties);
    }
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a BoundCRS from a base CRS, a hub CRS and a
 * transformation.
 *
 * @param baseCRSIn base CRS.
 * @param hubCRSIn hub CRS.
 * @param transformationIn transformation from base CRS to hub CRS.
 * @return new BoundCRS.
 */
BoundCRSNNPtr
BoundCRS::create(const CRSNNPtr &baseCRSIn, const CRSNNPtr &hubCRSIn,
                 const operation::TransformationNNPtr &transformationIn) {
    return create(util::PropertyMap(), baseCRSIn, hubCRSIn, transformationIn);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a BoundCRS from a base CRS and TOWGS84 parameters
 *
 * @param baseCRSIn base CRS.
 * @param TOWGS84Parameters a vector of 3 or 7 double values representing WKT1
 * TOWGS84 parameter.
 * @return new BoundCRS.
 */
BoundCRSNNPtr
BoundCRS::createFromTOWGS84(const CRSNNPtr &baseCRSIn,
                            const std::vector<double> &TOWGS84Parameters) {
    auto transf =
        operation::Transformation::createTOWGS84(baseCRSIn, TOWGS84Parameters);
    return create(baseCRSIn, transf->targetCRS(), transf);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a BoundCRS from a base CRS and nadgrids parameters
 *
 * @param baseCRSIn base CRS.
 * @param filename Horizontal grid filename
 * @return new BoundCRS.
 */
BoundCRSNNPtr BoundCRS::createFromNadgrids(const CRSNNPtr &baseCRSIn,
                                           const std::string &filename) {
    const auto sourceGeographicCRS = baseCRSIn->extractGeographicCRS();
    auto transformationSourceCRS =
        sourceGeographicCRS
            ? NN_NO_CHECK(std::static_pointer_cast<CRS>(sourceGeographicCRS))
            : baseCRSIn;
    if (sourceGeographicCRS != nullptr &&
        sourceGeographicCRS->primeMeridian()->longitude().getSIValue() != 0.0) {
        transformationSourceCRS = GeographicCRS::create(
            util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                    sourceGeographicCRS->nameStr() +
                                        " (with Greenwich prime meridian)"),
            datum::GeodeticReferenceFrame::create(
                util::PropertyMap().set(
                    common::IdentifiedObject::NAME_KEY,
                    sourceGeographicCRS->datumNonNull(nullptr)->nameStr() +
                        " (with Greenwich prime meridian)"),
                sourceGeographicCRS->datumNonNull(nullptr)->ellipsoid(),
                util::optional<std::string>(), datum::PrimeMeridian::GREENWICH),
            cs::EllipsoidalCS::createLatitudeLongitude(
                common::UnitOfMeasure::DEGREE));
    }
    std::string transformationName = transformationSourceCRS->nameStr();
    transformationName += " to WGS84";

    return create(
        baseCRSIn, GeographicCRS::EPSG_4326,
        operation::Transformation::createNTv2(
            util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                    transformationName),
            transformationSourceCRS, GeographicCRS::EPSG_4326, filename,
            std::vector<metadata::PositionalAccuracyNNPtr>()));
}

// ---------------------------------------------------------------------------

bool BoundCRS::isTOWGS84Compatible() const {
    return dynamic_cast<GeodeticCRS *>(d->hubCRS().get()) != nullptr &&
           ci_equal(d->hubCRS()->nameStr(), "WGS 84");
}

// ---------------------------------------------------------------------------

std::string BoundCRS::getHDatumPROJ4GRIDS(
    const io::DatabaseContextPtr &databaseContext) const {
    if (ci_equal(d->hubCRS()->nameStr(), "WGS 84")) {
        if (databaseContext) {
            return d->transformation()
                ->substitutePROJAlternativeGridNames(
                    NN_NO_CHECK(databaseContext))
                ->getPROJ4NadgridsCompatibleFilename();
        }
        return d->transformation()->getPROJ4NadgridsCompatibleFilename();
    }
    return std::string();
}

// ---------------------------------------------------------------------------

std::string
BoundCRS::getVDatumPROJ4GRIDS(const crs::GeographicCRS *geogCRSOfCompoundCRS,
                              const char **outGeoidCRSValue) const {
    // When importing from WKT1 PROJ4_GRIDS extension, we used to hardcode
    // "WGS 84" as the hub CRS, so let's test that for backward compatibility.
    if (dynamic_cast<VerticalCRS *>(d->baseCRS().get()) &&
        ci_equal(d->hubCRS()->nameStr(), "WGS 84")) {
        if (outGeoidCRSValue)
            *outGeoidCRSValue = "WGS84";
        return d->transformation()->getHeightToGeographic3DFilename();
    } else if (geogCRSOfCompoundCRS &&
               dynamic_cast<VerticalCRS *>(d->baseCRS().get()) &&
               ci_equal(d->hubCRS()->nameStr(),
                        geogCRSOfCompoundCRS->nameStr())) {
        if (outGeoidCRSValue)
            *outGeoidCRSValue = "horizontal_crs";
        return d->transformation()->getHeightToGeographic3DFilename();
    }
    return std::string();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void BoundCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (isWKT2) {
        formatter->startNode(io::WKTConstants::BOUNDCRS, false);
        formatter->startNode(io::WKTConstants::SOURCECRS, false);
        d->baseCRS()->_exportToWKT(formatter);
        formatter->endNode();
        formatter->startNode(io::WKTConstants::TARGETCRS, false);
        d->hubCRS()->_exportToWKT(formatter);
        formatter->endNode();
        formatter->setAbridgedTransformation(true);
        d->transformation()->_exportToWKT(formatter);
        formatter->setAbridgedTransformation(false);
        ObjectUsage::baseExportToWKT(formatter);
        formatter->endNode();
    } else {

        auto vdatumProj4GridName = getVDatumPROJ4GRIDS(
            formatter->getGeogCRSOfCompoundCRS().get(), nullptr);
        if (!vdatumProj4GridName.empty()) {
            formatter->setVDatumExtension(vdatumProj4GridName);
            d->baseCRS()->_exportToWKT(formatter);
            formatter->setVDatumExtension(std::string());
            return;
        }

        auto hdatumProj4GridName =
            getHDatumPROJ4GRIDS(formatter->databaseContext());
        if (!hdatumProj4GridName.empty()) {
            formatter->setHDatumExtension(hdatumProj4GridName);
            d->baseCRS()->_exportToWKT(formatter);
            formatter->setHDatumExtension(std::string());
            return;
        }

        if (!isTOWGS84Compatible()) {
            io::FormattingException::Throw(
                "Cannot export BoundCRS with non-WGS 84 hub CRS in WKT1");
        }
        auto params = d->transformation()->getTOWGS84Parameters(true);
        if (!formatter->useESRIDialect()) {
            formatter->setTOWGS84Parameters(params);
        }
        d->baseCRS()->_exportToWKT(formatter);
        formatter->setTOWGS84Parameters(std::vector<double>());
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void BoundCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    const auto &l_name = nameStr();

    auto objectContext(formatter->MakeObjectContext("BoundCRS", false));

    const auto &l_sourceCRS = d->baseCRS();

    if (!l_name.empty() && l_name != l_sourceCRS->nameStr()) {
        writer->AddObjKey("name");
        writer->Add(l_name);
    }

    writer->AddObjKey("source_crs");
    l_sourceCRS->_exportToJSON(formatter);

    writer->AddObjKey("target_crs");
    const auto &l_targetCRS = d->hubCRS();
    l_targetCRS->_exportToJSON(formatter);

    writer->AddObjKey("transformation");
    formatter->setOmitTypeInImmediateChild();
    formatter->setAbridgedTransformation(true);
    // Only write the source_crs of the transformation if it is different from
    // the source_crs of the BoundCRS. But don't do it for projectedCRS if its
    // base CRS matches the source_crs of the transformation and the targetCRS
    // is geographic
    const auto sourceCRSAsProjectedCRS =
        dynamic_cast<const ProjectedCRS *>(l_sourceCRS.get());
    if (!l_sourceCRS->_isEquivalentTo(
            d->transformation()->sourceCRS().get(),
            util::IComparable::Criterion::EQUIVALENT) &&
        (sourceCRSAsProjectedCRS == nullptr ||
         (dynamic_cast<GeographicCRS *>(l_targetCRS.get()) &&
          !sourceCRSAsProjectedCRS->baseCRS()->_isEquivalentTo(
              d->transformation()->sourceCRS().get(),
              util::IComparable::Criterion::EQUIVALENT)))) {
        formatter->setAbridgedTransformationWriteSourceCRS(true);
    }
    d->transformation()->_exportToJSON(formatter);
    formatter->setAbridgedTransformation(false);
    formatter->setAbridgedTransformationWriteSourceCRS(false);

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

void BoundCRS::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(io::FormattingException)
{
    auto crs_exportable =
        dynamic_cast<const io::IPROJStringExportable *>(d->baseCRS_.get());
    if (!crs_exportable) {
        io::FormattingException::Throw(
            "baseCRS of BoundCRS cannot be exported as a PROJ string");
    }

    const char *geoidCRSValue = "";
    auto vdatumProj4GridName = getVDatumPROJ4GRIDS(
        formatter->getGeogCRSOfCompoundCRS().get(), &geoidCRSValue);
    if (!vdatumProj4GridName.empty()) {
        formatter->setVDatumExtension(vdatumProj4GridName, geoidCRSValue);
        crs_exportable->_exportToPROJString(formatter);
        formatter->setVDatumExtension(std::string(), std::string());
    } else {
        auto hdatumProj4GridName =
            getHDatumPROJ4GRIDS(formatter->databaseContext());
        if (!hdatumProj4GridName.empty()) {
            formatter->setHDatumExtension(hdatumProj4GridName);
            crs_exportable->_exportToPROJString(formatter);
            formatter->setHDatumExtension(std::string());
        } else {
            if (isTOWGS84Compatible()) {
                auto params = transformation()->getTOWGS84Parameters(true);
                formatter->setTOWGS84Parameters(params);
            }
            crs_exportable->_exportToPROJString(formatter);
            formatter->setTOWGS84Parameters(std::vector<double>());
        }
    }
}

// ---------------------------------------------------------------------------

bool BoundCRS::_isEquivalentTo(const util::IComparable *other,
                               util::IComparable::Criterion criterion,
                               const io::DatabaseContextPtr &dbContext) const {
    auto otherBoundCRS = dynamic_cast<const BoundCRS *>(other);
    if (otherBoundCRS == nullptr ||
        (criterion == util::IComparable::Criterion::STRICT &&
         !ObjectUsage::_isEquivalentTo(other, criterion, dbContext))) {
        return false;
    }
    const auto standardCriterion = getStandardCriterion(criterion);
    return d->baseCRS_->_isEquivalentTo(otherBoundCRS->d->baseCRS_.get(),
                                        criterion, dbContext) &&
           d->hubCRS_->_isEquivalentTo(otherBoundCRS->d->hubCRS_.get(),
                                       criterion, dbContext) &&
           d->transformation_->_isEquivalentTo(
               otherBoundCRS->d->transformation_.get(), standardCriterion,
               dbContext);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
BoundCRS::_identify(const io::AuthorityFactoryPtr &authorityFactory) const {
    typedef std::pair<CRSNNPtr, int> Pair;
    std::list<Pair> res;
    if (!authorityFactory)
        return res;
    std::list<Pair> resMatchOfTransfToWGS84;
    const io::DatabaseContextNNPtr &dbContext =
        authorityFactory->databaseContext();
    if (d->hubCRS_->_isEquivalentTo(GeographicCRS::EPSG_4326.get(),
                                    util::IComparable::Criterion::EQUIVALENT,
                                    dbContext)) {
        auto resTemp = d->baseCRS_->identify(authorityFactory);

        std::string refTransfPROJString;
        bool refTransfPROJStringValid = false;
        auto refTransf = d->transformation_->normalizeForVisualization();
        try {
            refTransfPROJString = refTransf->exportToPROJString(
                io::PROJStringFormatter::create().get());
            refTransfPROJString = replaceAll(
                refTransfPROJString,
                " +rx=0 +ry=0 +rz=0 +s=0 +convention=position_vector", "");
            refTransfPROJStringValid = true;
        } catch (const std::exception &) {
        }
        bool refIsNullTransform = false;
        if (isTOWGS84Compatible()) {
            auto params = transformation()->getTOWGS84Parameters(true);
            if (params == std::vector<double>{0, 0, 0, 0, 0, 0, 0}) {
                refIsNullTransform = true;
            }
        }

        for (const auto &pair : resTemp) {
            const auto &candidateBaseCRS = pair.first;
            auto projCRS =
                dynamic_cast<const ProjectedCRS *>(candidateBaseCRS.get());
            auto geodCRS = projCRS ? projCRS->baseCRS().as_nullable()
                                   : util::nn_dynamic_pointer_cast<GeodeticCRS>(
                                         candidateBaseCRS);
            if (geodCRS) {
                auto context = operation::CoordinateOperationContext::create(
                    authorityFactory, nullptr, 0.0);
                context->setSpatialCriterion(
                    operation::CoordinateOperationContext::SpatialCriterion::
                        PARTIAL_INTERSECTION);
                auto ops =
                    operation::CoordinateOperationFactory::create()
                        ->createOperations(NN_NO_CHECK(geodCRS),
                                           GeographicCRS::EPSG_4326, context);

                bool foundOp = false;
                for (const auto &op : ops) {
                    auto opNormalized = op->normalizeForVisualization();
                    std::string opTransfPROJString;
                    bool opTransfPROJStringValid = false;
                    const auto &opName = op->nameStr();
                    if (starts_with(
                            opName,
                            operation::BALLPARK_GEOCENTRIC_TRANSLATION) ||
                        starts_with(opName,
                                    operation::NULL_GEOGRAPHIC_OFFSET)) {
                        if (refIsNullTransform) {
                            res.emplace_back(create(candidateBaseCRS,
                                                    d->hubCRS_,
                                                    transformation()),
                                             pair.second);
                            foundOp = true;
                            break;
                        }
                        continue;
                    }
                    try {
                        opTransfPROJString = opNormalized->exportToPROJString(
                            io::PROJStringFormatter::create().get());
                        opTransfPROJStringValid = true;
                        opTransfPROJString =
                            replaceAll(opTransfPROJString,
                                       " +rx=0 +ry=0 +rz=0 +s=0 "
                                       "+convention=position_vector",
                                       "");
                    } catch (const std::exception &) {
                    }
                    if ((refTransfPROJStringValid && opTransfPROJStringValid &&
                         refTransfPROJString == opTransfPROJString) ||
                        opNormalized->_isEquivalentTo(
                            refTransf.get(),
                            util::IComparable::Criterion::EQUIVALENT,
                            dbContext)) {
                        auto transf = util::nn_dynamic_pointer_cast<
                            operation::Transformation>(op);
                        if (transf) {
                            resMatchOfTransfToWGS84.emplace_back(
                                create(candidateBaseCRS, d->hubCRS_,
                                       NN_NO_CHECK(transf)),
                                pair.second);
                            foundOp = true;
                            break;
                        } else {
                            auto concatenated = dynamic_cast<
                                const operation::ConcatenatedOperation *>(
                                op.get());
                            if (concatenated) {
                                // Case for EPSG:4807 / "NTF (Paris)" that is
                                // made of a longitude rotation followed by a
                                // Helmert The prime meridian shift will be
                                // accounted elsewhere
                                const auto &subops = concatenated->operations();
                                if (subops.size() == 2) {
                                    auto firstOpIsTransformation = dynamic_cast<
                                        const operation::Transformation *>(
                                        subops[0].get());
                                    auto firstOpIsConversion = dynamic_cast<
                                        const operation::Conversion *>(
                                        subops[0].get());
                                    if ((firstOpIsTransformation &&
                                         firstOpIsTransformation
                                             ->isLongitudeRotation()) ||
                                        (dynamic_cast<DerivedCRS *>(
                                             candidateBaseCRS.get()) &&
                                         firstOpIsConversion)) {
                                        transf = util::nn_dynamic_pointer_cast<
                                            operation::Transformation>(
                                            subops[1]);
                                        if (transf &&
                                            !starts_with(transf->nameStr(),
                                                         "Ballpark geo")) {
                                            resMatchOfTransfToWGS84
                                                .emplace_back(
                                                    create(candidateBaseCRS,
                                                           d->hubCRS_,
                                                           NN_NO_CHECK(transf)),
                                                    pair.second);
                                            foundOp = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if (!foundOp) {
                    res.emplace_back(
                        create(candidateBaseCRS, d->hubCRS_, transformation()),
                        std::min(70, pair.second));
                }
            }
        }
    }
    return !resMatchOfTransfToWGS84.empty() ? resMatchOfTransfToWGS84 : res;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DerivedGeodeticCRS::Private {};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DerivedGeodeticCRS::~DerivedGeodeticCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

DerivedGeodeticCRS::DerivedGeodeticCRS(
    const GeodeticCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::CartesianCSNNPtr &csIn)
    : SingleCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      GeodeticCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      DerivedCRS(baseCRSIn, derivingConversionIn, csIn), d(nullptr) {}

// ---------------------------------------------------------------------------

DerivedGeodeticCRS::DerivedGeodeticCRS(
    const GeodeticCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::SphericalCSNNPtr &csIn)
    : SingleCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      GeodeticCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      DerivedCRS(baseCRSIn, derivingConversionIn, csIn), d(nullptr) {}

// ---------------------------------------------------------------------------

DerivedGeodeticCRS::DerivedGeodeticCRS(const DerivedGeodeticCRS &other)
    : SingleCRS(other), GeodeticCRS(other), DerivedCRS(other), d(nullptr) {}

// ---------------------------------------------------------------------------

CRSNNPtr DerivedGeodeticCRS::_shallowClone() const {
    auto crs(DerivedGeodeticCRS::nn_make_shared<DerivedGeodeticCRS>(*this));
    crs->assignSelf(crs);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the base CRS (a GeodeticCRS) of a DerivedGeodeticCRS.
 *
 * @return the base CRS.
 */
const GeodeticCRSNNPtr DerivedGeodeticCRS::baseCRS() const {
    return NN_NO_CHECK(util::nn_dynamic_pointer_cast<GeodeticCRS>(
        DerivedCRS::getPrivate()->baseCRS_));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a DerivedGeodeticCRS from a base CRS, a deriving
 * conversion and a cs::CartesianCS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param baseCRSIn base CRS.
 * @param derivingConversionIn the deriving conversion from the base CRS to this
 * CRS.
 * @param csIn the coordinate system.
 * @return new DerivedGeodeticCRS.
 */
DerivedGeodeticCRSNNPtr DerivedGeodeticCRS::create(
    const util::PropertyMap &properties, const GeodeticCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::CartesianCSNNPtr &csIn) {
    auto crs(DerivedGeodeticCRS::nn_make_shared<DerivedGeodeticCRS>(
        baseCRSIn, derivingConversionIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a DerivedGeodeticCRS from a base CRS, a deriving
 * conversion and a cs::SphericalCS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param baseCRSIn base CRS.
 * @param derivingConversionIn the deriving conversion from the base CRS to this
 * CRS.
 * @param csIn the coordinate system.
 * @return new DerivedGeodeticCRS.
 */
DerivedGeodeticCRSNNPtr DerivedGeodeticCRS::create(
    const util::PropertyMap &properties, const GeodeticCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::SphericalCSNNPtr &csIn) {
    auto crs(DerivedGeodeticCRS::nn_make_shared<DerivedGeodeticCRS>(
        baseCRSIn, derivingConversionIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DerivedGeodeticCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {
        io::FormattingException::Throw(
            "DerivedGeodeticCRS can only be exported to WKT2");
    }
    formatter->startNode(io::WKTConstants::GEODCRS, !identifiers().empty());
    formatter->addQuotedString(nameStr());

    auto l_baseCRS = baseCRS();
    formatter->startNode((formatter->use2019Keywords() &&
                          dynamic_cast<const GeographicCRS *>(l_baseCRS.get()))
                             ? io::WKTConstants::BASEGEOGCRS
                             : io::WKTConstants::BASEGEODCRS,
                         !baseCRS()->identifiers().empty());
    formatter->addQuotedString(l_baseCRS->nameStr());
    auto l_datum = l_baseCRS->datum();
    if (l_datum) {
        l_datum->_exportToWKT(formatter);
    } else {
        auto l_datumEnsemble = datumEnsemble();
        assert(l_datumEnsemble);
        l_datumEnsemble->_exportToWKT(formatter);
    }
    l_baseCRS->primeMeridian()->_exportToWKT(formatter);
    if (formatter->use2019Keywords() &&
        !(formatter->idOnTopLevelOnly() && formatter->topLevelHasId())) {
        l_baseCRS->formatID(formatter);
    }
    formatter->endNode();

    formatter->setUseDerivingConversion(true);
    derivingConversionRef()->_exportToWKT(formatter);
    formatter->setUseDerivingConversion(false);

    coordinateSystem()->_exportToWKT(formatter);
    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

void DerivedGeodeticCRS::_exportToPROJString(
    io::PROJStringFormatter *) const // throw(io::FormattingException)
{
    throw io::FormattingException(
        "DerivedGeodeticCRS cannot be exported to PROJ string");
}

// ---------------------------------------------------------------------------

bool DerivedGeodeticCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherDerivedCRS = dynamic_cast<const DerivedGeodeticCRS *>(other);
    return otherDerivedCRS != nullptr &&
           DerivedCRS::_isEquivalentTo(other, criterion, dbContext);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
DerivedGeodeticCRS::_identify(const io::AuthorityFactoryPtr &factory) const {
    return CRS::_identify(factory);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DerivedGeographicCRS::Private {};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DerivedGeographicCRS::~DerivedGeographicCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

DerivedGeographicCRS::DerivedGeographicCRS(
    const GeodeticCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::EllipsoidalCSNNPtr &csIn)
    : SingleCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      GeographicCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      DerivedCRS(baseCRSIn, derivingConversionIn, csIn), d(nullptr) {}

// ---------------------------------------------------------------------------

DerivedGeographicCRS::DerivedGeographicCRS(const DerivedGeographicCRS &other)
    : SingleCRS(other), GeographicCRS(other), DerivedCRS(other), d(nullptr) {}

// ---------------------------------------------------------------------------

CRSNNPtr DerivedGeographicCRS::_shallowClone() const {
    auto crs(DerivedGeographicCRS::nn_make_shared<DerivedGeographicCRS>(*this));
    crs->assignSelf(crs);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the base CRS (a GeodeticCRS) of a DerivedGeographicCRS.
 *
 * @return the base CRS.
 */
const GeodeticCRSNNPtr DerivedGeographicCRS::baseCRS() const {
    return NN_NO_CHECK(util::nn_dynamic_pointer_cast<GeodeticCRS>(
        DerivedCRS::getPrivate()->baseCRS_));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a DerivedGeographicCRS from a base CRS, a deriving
 * conversion and a cs::EllipsoidalCS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param baseCRSIn base CRS.
 * @param derivingConversionIn the deriving conversion from the base CRS to this
 * CRS.
 * @param csIn the coordinate system.
 * @return new DerivedGeographicCRS.
 */
DerivedGeographicCRSNNPtr DerivedGeographicCRS::create(
    const util::PropertyMap &properties, const GeodeticCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::EllipsoidalCSNNPtr &csIn) {
    auto crs(DerivedGeographicCRS::nn_make_shared<DerivedGeographicCRS>(
        baseCRSIn, derivingConversionIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DerivedGeographicCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {
        io::FormattingException::Throw(
            "DerivedGeographicCRS can only be exported to WKT2");
    }
    formatter->startNode(formatter->use2019Keywords()
                             ? io::WKTConstants::GEOGCRS
                             : io::WKTConstants::GEODCRS,
                         !identifiers().empty());
    formatter->addQuotedString(nameStr());

    auto l_baseCRS = baseCRS();
    formatter->startNode((formatter->use2019Keywords() &&
                          dynamic_cast<const GeographicCRS *>(l_baseCRS.get()))
                             ? io::WKTConstants::BASEGEOGCRS
                             : io::WKTConstants::BASEGEODCRS,
                         !l_baseCRS->identifiers().empty());
    formatter->addQuotedString(l_baseCRS->nameStr());
    l_baseCRS->exportDatumOrDatumEnsembleToWkt(formatter);
    l_baseCRS->primeMeridian()->_exportToWKT(formatter);
    if (formatter->use2019Keywords() &&
        !(formatter->idOnTopLevelOnly() && formatter->topLevelHasId())) {
        l_baseCRS->formatID(formatter);
    }
    formatter->endNode();

    formatter->setUseDerivingConversion(true);
    derivingConversionRef()->_exportToWKT(formatter);
    formatter->setUseDerivingConversion(false);

    coordinateSystem()->_exportToWKT(formatter);
    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

void DerivedGeographicCRS::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(io::FormattingException)
{
    const auto &l_conv = derivingConversionRef();
    const auto &methodName = l_conv->method()->nameStr();

    for (const char *substr :
         {"PROJ ob_tran o_proj=longlat", "PROJ ob_tran o_proj=lonlat",
          "PROJ ob_tran o_proj=latlon", "PROJ ob_tran o_proj=latlong"}) {
        if (starts_with(methodName, substr)) {
            l_conv->_exportToPROJString(formatter);
            return;
        }
    }

    if (ci_equal(methodName,
                 PROJ_WKT2_NAME_METHOD_POLE_ROTATION_GRIB_CONVENTION) ||
        ci_equal(methodName,
                 PROJ_WKT2_NAME_METHOD_POLE_ROTATION_NETCDF_CF_CONVENTION)) {
        l_conv->_exportToPROJString(formatter);
        return;
    }

    throw io::FormattingException(
        "DerivedGeographicCRS cannot be exported to PROJ string");
}

// ---------------------------------------------------------------------------

bool DerivedGeographicCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherDerivedCRS = dynamic_cast<const DerivedGeographicCRS *>(other);
    return otherDerivedCRS != nullptr &&
           DerivedCRS::_isEquivalentTo(other, criterion, dbContext);
}

// ---------------------------------------------------------------------------

/** \brief Return a variant of this CRS "demoted" to a 2D one, if not already
 * the case.
 *
 *
 * @param newName Name of the new CRS. If empty, nameStr() will be used.
 * @param dbContext Database context to look for potentially already registered
 *                  2D CRS. May be nullptr.
 * @return a new CRS demoted to 2D, or the current one if already 2D or not
 * applicable.
 * @since 8.1.1
 */
DerivedGeographicCRSNNPtr DerivedGeographicCRS::demoteTo2D(
    const std::string &newName, const io::DatabaseContextPtr &dbContext) const {

    const auto &axisList = coordinateSystem()->axisList();
    if (axisList.size() == 3) {
        auto cs = cs::EllipsoidalCS::create(util::PropertyMap(), axisList[0],
                                            axisList[1]);
        auto baseGeog2DCRS = util::nn_dynamic_pointer_cast<GeodeticCRS>(
            baseCRS()->demoteTo2D(std::string(), dbContext));
        return DerivedGeographicCRS::create(
            util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                    !newName.empty() ? newName : nameStr()),
            NN_CHECK_THROW(std::move(baseGeog2DCRS)), derivingConversion(),
            std::move(cs));
    }

    return NN_NO_CHECK(std::dynamic_pointer_cast<DerivedGeographicCRS>(
        shared_from_this().as_nullable()));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
DerivedGeographicCRS::_identify(const io::AuthorityFactoryPtr &factory) const {
    return CRS::_identify(factory);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DerivedProjectedCRS::Private {};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DerivedProjectedCRS::~DerivedProjectedCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

DerivedProjectedCRS::DerivedProjectedCRS(
    const ProjectedCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::CoordinateSystemNNPtr &csIn)
    : SingleCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      DerivedCRS(baseCRSIn, derivingConversionIn, csIn), d(nullptr) {}

// ---------------------------------------------------------------------------

DerivedProjectedCRS::DerivedProjectedCRS(const DerivedProjectedCRS &other)
    : SingleCRS(other), DerivedCRS(other), d(nullptr) {}

// ---------------------------------------------------------------------------

CRSNNPtr DerivedProjectedCRS::_shallowClone() const {
    auto crs(DerivedProjectedCRS::nn_make_shared<DerivedProjectedCRS>(*this));
    crs->assignSelf(crs);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the base CRS (a ProjectedCRS) of a DerivedProjectedCRS.
 *
 * @return the base CRS.
 */
const ProjectedCRSNNPtr DerivedProjectedCRS::baseCRS() const {
    return NN_NO_CHECK(util::nn_dynamic_pointer_cast<ProjectedCRS>(
        DerivedCRS::getPrivate()->baseCRS_));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a DerivedProjectedCRS from a base CRS, a deriving
 * conversion and a cs::CS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param baseCRSIn base CRS.
 * @param derivingConversionIn the deriving conversion from the base CRS to this
 * CRS.
 * @param csIn the coordinate system.
 * @return new DerivedProjectedCRS.
 */
DerivedProjectedCRSNNPtr DerivedProjectedCRS::create(
    const util::PropertyMap &properties, const ProjectedCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::CoordinateSystemNNPtr &csIn) {
    auto crs(DerivedProjectedCRS::nn_make_shared<DerivedProjectedCRS>(
        baseCRSIn, derivingConversionIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return a variant of this CRS "demoted" to a 2D one, if not already
 * the case.
 *
 *
 * @param newName Name of the new CRS. If empty, nameStr() will be used.
 * @param dbContext Database context to look for potentially already registered
 *                  2D CRS. May be nullptr.
 * @return a new CRS demoted to 2D, or the current one if already 2D or not
 * applicable.
 * @since 9.1.1
 */
DerivedProjectedCRSNNPtr
DerivedProjectedCRS::demoteTo2D(const std::string &newName,
                                const io::DatabaseContextPtr &dbContext) const {

    const auto &axisList = coordinateSystem()->axisList();
    if (axisList.size() == 3) {
        auto cs = cs::CartesianCS::create(util::PropertyMap(), axisList[0],
                                          axisList[1]);
        auto baseProj2DCRS = util::nn_dynamic_pointer_cast<ProjectedCRS>(
            baseCRS()->demoteTo2D(std::string(), dbContext));
        return DerivedProjectedCRS::create(
            util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                    !newName.empty() ? newName : nameStr()),
            NN_CHECK_THROW(std::move(baseProj2DCRS)), derivingConversion(),
            std::move(cs));
    }

    return NN_NO_CHECK(std::dynamic_pointer_cast<DerivedProjectedCRS>(
        shared_from_this().as_nullable()));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DerivedProjectedCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2 || !formatter->use2019Keywords()) {
        io::FormattingException::Throw(
            "DerivedProjectedCRS can only be exported to WKT2:2019");
    }
    formatter->startNode(io::WKTConstants::DERIVEDPROJCRS,
                         !identifiers().empty());
    formatter->addQuotedString(nameStr());

    {
        auto l_baseProjCRS = baseCRS();
        formatter->startNode(io::WKTConstants::BASEPROJCRS,
                             !l_baseProjCRS->identifiers().empty());
        formatter->addQuotedString(l_baseProjCRS->nameStr());

        auto l_baseGeodCRS = l_baseProjCRS->baseCRS();
        auto &geodeticCRSAxisList =
            l_baseGeodCRS->coordinateSystem()->axisList();

        formatter->startNode(
            dynamic_cast<const GeographicCRS *>(l_baseGeodCRS.get())
                ? io::WKTConstants::BASEGEOGCRS
                : io::WKTConstants::BASEGEODCRS,
            !l_baseGeodCRS->identifiers().empty());
        formatter->addQuotedString(l_baseGeodCRS->nameStr());
        l_baseGeodCRS->exportDatumOrDatumEnsembleToWkt(formatter);
        // insert ellipsoidal cs unit when the units of the map
        // projection angular parameters are not explicitly given within those
        // parameters. See
        // http://docs.opengeospatial.org/is/12-063r5/12-063r5.html#61
        if (formatter->primeMeridianOrParameterUnitOmittedIfSameAsAxis() &&
            !geodeticCRSAxisList.empty()) {
            geodeticCRSAxisList[0]->unit()._exportToWKT(formatter);
        }
        l_baseGeodCRS->primeMeridian()->_exportToWKT(formatter);
        formatter->endNode();

        l_baseProjCRS->derivingConversionRef()->_exportToWKT(formatter);

        const auto &baseCSAxisList =
            l_baseProjCRS->coordinateSystem()->axisList();
        // Current WKT grammar (as of WKT2 18-010r11) does not allow a
        // BASEPROJCRS.CS node, but in situations where this is ambiguous, emit
        // one. Cf WKTParser::Private::buildProjectedCRS() for more details
        if (!baseCSAxisList.empty() &&
            baseCSAxisList[0]->unit() != common::UnitOfMeasure::METRE &&
            l_baseProjCRS->identifiers().empty()) {
            bool knownBaseCRS = false;
            auto &dbContext = formatter->databaseContext();
            if (dbContext) {
                auto authFactory = io::AuthorityFactory::create(
                    NN_NO_CHECK(dbContext), std::string());
                auto res = authFactory->createObjectsFromName(
                    l_baseProjCRS->nameStr(),
                    {io::AuthorityFactory::ObjectType::PROJECTED_CRS}, false,
                    2);
                if (res.size() == 1) {
                    knownBaseCRS = true;
                }
            }
            if (!knownBaseCRS) {
                l_baseProjCRS->coordinateSystem()->_exportToWKT(formatter);
            }
        }

        if (identifiers().empty() && !l_baseProjCRS->identifiers().empty()) {
            l_baseProjCRS->formatID(formatter);
        }

        formatter->endNode();
    }

    formatter->setUseDerivingConversion(true);
    derivingConversionRef()->_exportToWKT(formatter);
    formatter->setUseDerivingConversion(false);

    coordinateSystem()->_exportToWKT(formatter);
    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

bool DerivedProjectedCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherDerivedCRS = dynamic_cast<const DerivedProjectedCRS *>(other);
    return otherDerivedCRS != nullptr &&
           DerivedCRS::_isEquivalentTo(other, criterion, dbContext);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DerivedProjectedCRS::addUnitConvertAndAxisSwap(
    io::PROJStringFormatter *formatter) const {
    ProjectedCRS::addUnitConvertAndAxisSwap(coordinateSystem()->axisList(),
                                            formatter, false);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct TemporalCRS::Private {};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
TemporalCRS::~TemporalCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

TemporalCRS::TemporalCRS(const datum::TemporalDatumNNPtr &datumIn,
                         const cs::TemporalCSNNPtr &csIn)
    : SingleCRS(datumIn.as_nullable(), nullptr, csIn), d(nullptr) {}

// ---------------------------------------------------------------------------

TemporalCRS::TemporalCRS(const TemporalCRS &other)
    : SingleCRS(other), d(nullptr) {}

// ---------------------------------------------------------------------------

CRSNNPtr TemporalCRS::_shallowClone() const {
    auto crs(TemporalCRS::nn_make_shared<TemporalCRS>(*this));
    crs->assignSelf(crs);
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the datum::TemporalDatum associated with the CRS.
 *
 * @return a TemporalDatum
 */
const datum::TemporalDatumNNPtr TemporalCRS::datum() const {
    return NN_NO_CHECK(std::static_pointer_cast<datum::TemporalDatum>(
        SingleCRS::getPrivate()->datum));
}

// ---------------------------------------------------------------------------

/** \brief Return the cs::TemporalCS associated with the CRS.
 *
 * @return a TemporalCS
 */
const cs::TemporalCSNNPtr TemporalCRS::coordinateSystem() const {
    return util::nn_static_pointer_cast<cs::TemporalCS>(
        SingleCRS::getPrivate()->coordinateSystem);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a TemporalCRS from a datum and a coordinate system.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datumIn the datum.
 * @param csIn the coordinate system.
 * @return new TemporalCRS.
 */
TemporalCRSNNPtr TemporalCRS::create(const util::PropertyMap &properties,
                                     const datum::TemporalDatumNNPtr &datumIn,
                                     const cs::TemporalCSNNPtr &csIn) {
    auto crs(TemporalCRS::nn_make_shared<TemporalCRS>(datumIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void TemporalCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {
        io::FormattingException::Throw(
            "TemporalCRS can only be exported to WKT2");
    }
    formatter->startNode(io::WKTConstants::TIMECRS, !identifiers().empty());
    formatter->addQuotedString(nameStr());
    datum()->_exportToWKT(formatter);
    coordinateSystem()->_exportToWKT(formatter);
    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void TemporalCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("TemporalCRS", !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    writer->AddObjKey("datum");
    formatter->setOmitTypeInImmediateChild();
    datum()->_exportToJSON(formatter);

    writer->AddObjKey("coordinate_system");
    formatter->setOmitTypeInImmediateChild();
    coordinateSystem()->_exportToJSON(formatter);

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

bool TemporalCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherTemporalCRS = dynamic_cast<const TemporalCRS *>(other);
    return otherTemporalCRS != nullptr &&
           SingleCRS::baseIsEquivalentTo(other, criterion, dbContext);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct EngineeringCRS::Private {};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
EngineeringCRS::~EngineeringCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

EngineeringCRS::EngineeringCRS(const datum::EngineeringDatumNNPtr &datumIn,
                               const cs::CoordinateSystemNNPtr &csIn)
    : SingleCRS(datumIn.as_nullable(), nullptr, csIn),
      d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

EngineeringCRS::EngineeringCRS(const EngineeringCRS &other)
    : SingleCRS(other), d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

CRSNNPtr EngineeringCRS::_shallowClone() const {
    auto crs(EngineeringCRS::nn_make_shared<EngineeringCRS>(*this));
    crs->assignSelf(crs);
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the datum::EngineeringDatum associated with the CRS.
 *
 * @return a EngineeringDatum
 */
const datum::EngineeringDatumNNPtr EngineeringCRS::datum() const {
    return NN_NO_CHECK(std::static_pointer_cast<datum::EngineeringDatum>(
        SingleCRS::getPrivate()->datum));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a EngineeringCRS from a datum and a coordinate system.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datumIn the datum.
 * @param csIn the coordinate system.
 * @return new EngineeringCRS.
 */
EngineeringCRSNNPtr
EngineeringCRS::create(const util::PropertyMap &properties,
                       const datum::EngineeringDatumNNPtr &datumIn,
                       const cs::CoordinateSystemNNPtr &csIn) {
    auto crs(EngineeringCRS::nn_make_shared<EngineeringCRS>(datumIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);

    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void EngineeringCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    formatter->startNode(isWKT2 ? io::WKTConstants::ENGCRS
                                : io::WKTConstants::LOCAL_CS,
                         !identifiers().empty());
    formatter->addQuotedString(nameStr());
    const auto &datumName = datum()->nameStr();
    if (isWKT2 ||
        (!datumName.empty() && datumName != UNKNOWN_ENGINEERING_DATUM)) {
        datum()->_exportToWKT(formatter);
    }
    if (!isWKT2) {
        coordinateSystem()->axisList()[0]->unit()._exportToWKT(formatter);
    }

    const auto oldAxisOutputRule = formatter->outputAxis();
    formatter->setOutputAxis(io::WKTFormatter::OutputAxisRule::YES);
    coordinateSystem()->_exportToWKT(formatter);
    formatter->setOutputAxis(oldAxisOutputRule);

    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void EngineeringCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("EngineeringCRS", !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    writer->AddObjKey("datum");
    formatter->setOmitTypeInImmediateChild();
    datum()->_exportToJSON(formatter);

    writer->AddObjKey("coordinate_system");
    formatter->setOmitTypeInImmediateChild();
    coordinateSystem()->_exportToJSON(formatter);

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

bool EngineeringCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherEngineeringCRS = dynamic_cast<const EngineeringCRS *>(other);
    if (otherEngineeringCRS == nullptr ||
        (criterion == util::IComparable::Criterion::STRICT &&
         !ObjectUsage::_isEquivalentTo(other, criterion, dbContext))) {
        return false;
    }

    // Check datum
    const auto &thisDatum = datum();
    const auto &otherDatum = otherEngineeringCRS->datum();
    if (!thisDatum->_isEquivalentTo(otherDatum.get(), criterion, dbContext)) {
        return false;
    }

    // Check coordinate system
    const auto &thisCS = coordinateSystem();
    const auto &otherCS = otherEngineeringCRS->coordinateSystem();
    if (!(thisCS->_isEquivalentTo(otherCS.get(), criterion, dbContext))) {
        const auto thisCartCS = dynamic_cast<cs::CartesianCS *>(thisCS.get());
        const auto otherCartCS = dynamic_cast<cs::CartesianCS *>(otherCS.get());
        const auto &thisAxisList = thisCS->axisList();
        const auto &otherAxisList = otherCS->axisList();
        // Check particular case of
        // https://github.com/r-spatial/sf/issues/2049#issuecomment-1486600723
        if (criterion != util::IComparable::Criterion::STRICT && thisCartCS &&
            otherCartCS && thisAxisList.size() == 2 &&
            otherAxisList.size() == 2 &&
            ((&thisAxisList[0]->direction() ==
                  &cs::AxisDirection::UNSPECIFIED &&
              &thisAxisList[1]->direction() ==
                  &cs::AxisDirection::UNSPECIFIED) ||
             (&otherAxisList[0]->direction() ==
                  &cs::AxisDirection::UNSPECIFIED &&
              &otherAxisList[1]->direction() ==
                  &cs::AxisDirection::UNSPECIFIED)) &&
            ((thisAxisList[0]->nameStr() == "X" &&
              otherAxisList[0]->nameStr() == "Easting" &&
              thisAxisList[1]->nameStr() == "Y" &&
              otherAxisList[1]->nameStr() == "Northing") ||
             (otherAxisList[0]->nameStr() == "X" &&
              thisAxisList[0]->nameStr() == "Easting" &&
              otherAxisList[1]->nameStr() == "Y" &&
              thisAxisList[1]->nameStr() == "Northing"))) {
            return true;
        }
        return false;
    }

    return true;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct ParametricCRS::Private {};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ParametricCRS::~ParametricCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

ParametricCRS::ParametricCRS(const datum::ParametricDatumNNPtr &datumIn,
                             const cs::ParametricCSNNPtr &csIn)
    : SingleCRS(datumIn.as_nullable(), nullptr, csIn), d(nullptr) {}

// ---------------------------------------------------------------------------

ParametricCRS::ParametricCRS(const ParametricCRS &other)
    : SingleCRS(other), d(nullptr) {}

// ---------------------------------------------------------------------------

CRSNNPtr ParametricCRS::_shallowClone() const {
    auto crs(ParametricCRS::nn_make_shared<ParametricCRS>(*this));
    crs->assignSelf(crs);
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the datum::ParametricDatum associated with the CRS.
 *
 * @return a ParametricDatum
 */
const datum::ParametricDatumNNPtr ParametricCRS::datum() const {
    return NN_NO_CHECK(std::static_pointer_cast<datum::ParametricDatum>(
        SingleCRS::getPrivate()->datum));
}

// ---------------------------------------------------------------------------

/** \brief Return the cs::TemporalCS associated with the CRS.
 *
 * @return a TemporalCS
 */
const cs::ParametricCSNNPtr ParametricCRS::coordinateSystem() const {
    return util::nn_static_pointer_cast<cs::ParametricCS>(
        SingleCRS::getPrivate()->coordinateSystem);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParametricCRS from a datum and a coordinate system.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datumIn the datum.
 * @param csIn the coordinate system.
 * @return new ParametricCRS.
 */
ParametricCRSNNPtr
ParametricCRS::create(const util::PropertyMap &properties,
                      const datum::ParametricDatumNNPtr &datumIn,
                      const cs::ParametricCSNNPtr &csIn) {
    auto crs(ParametricCRS::nn_make_shared<ParametricCRS>(datumIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ParametricCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {
        io::FormattingException::Throw(
            "ParametricCRS can only be exported to WKT2");
    }
    formatter->startNode(io::WKTConstants::PARAMETRICCRS,
                         !identifiers().empty());
    formatter->addQuotedString(nameStr());
    datum()->_exportToWKT(formatter);
    coordinateSystem()->_exportToWKT(formatter);
    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ParametricCRS::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("ParametricCRS", !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    writer->AddObjKey("datum");
    formatter->setOmitTypeInImmediateChild();
    datum()->_exportToJSON(formatter);

    writer->AddObjKey("coordinate_system");
    formatter->setOmitTypeInImmediateChild();
    coordinateSystem()->_exportToJSON(formatter);

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

bool ParametricCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherParametricCRS = dynamic_cast<const ParametricCRS *>(other);
    return otherParametricCRS != nullptr &&
           SingleCRS::baseIsEquivalentTo(other, criterion, dbContext);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DerivedVerticalCRS::Private {};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DerivedVerticalCRS::~DerivedVerticalCRS() = default;
//! @endcond

// ---------------------------------------------------------------------------

DerivedVerticalCRS::DerivedVerticalCRS(
    const VerticalCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::VerticalCSNNPtr &csIn)
    : SingleCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      VerticalCRS(baseCRSIn->datum(), baseCRSIn->datumEnsemble(), csIn),
      DerivedCRS(baseCRSIn, derivingConversionIn, csIn), d(nullptr) {}

// ---------------------------------------------------------------------------

DerivedVerticalCRS::DerivedVerticalCRS(const DerivedVerticalCRS &other)
    : SingleCRS(other), VerticalCRS(other), DerivedCRS(other), d(nullptr) {}

// ---------------------------------------------------------------------------

CRSNNPtr DerivedVerticalCRS::_shallowClone() const {
    auto crs(DerivedVerticalCRS::nn_make_shared<DerivedVerticalCRS>(*this));
    crs->assignSelf(crs);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

/** \brief Return the base CRS (a VerticalCRS) of a DerivedVerticalCRS.
 *
 * @return the base CRS.
 */
const VerticalCRSNNPtr DerivedVerticalCRS::baseCRS() const {
    return NN_NO_CHECK(util::nn_dynamic_pointer_cast<VerticalCRS>(
        DerivedCRS::getPrivate()->baseCRS_));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a DerivedVerticalCRS from a base CRS, a deriving
 * conversion and a cs::VerticalCS.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param baseCRSIn base CRS.
 * @param derivingConversionIn the deriving conversion from the base CRS to this
 * CRS.
 * @param csIn the coordinate system.
 * @return new DerivedVerticalCRS.
 */
DerivedVerticalCRSNNPtr DerivedVerticalCRS::create(
    const util::PropertyMap &properties, const VerticalCRSNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const cs::VerticalCSNNPtr &csIn) {
    auto crs(DerivedVerticalCRS::nn_make_shared<DerivedVerticalCRS>(
        baseCRSIn, derivingConversionIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DerivedVerticalCRS::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {

        bool useBaseMethod = true;
        const DerivedVerticalCRS *dvcrs = this;
        while (true) {
            // If the derived vertical CRS is obtained through simple conversion
            // methods that just do unit change or height/depth reversal, export
            // it as a regular VerticalCRS
            const int methodCode =
                dvcrs->derivingConversionRef()->method()->getEPSGCode();
            if (methodCode == EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT ||
                methodCode ==
                    EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR ||
                methodCode == EPSG_CODE_METHOD_HEIGHT_DEPTH_REVERSAL) {
                dvcrs = dynamic_cast<DerivedVerticalCRS *>(baseCRS().get());
                if (dvcrs == nullptr) {
                    break;
                }
            } else {
                useBaseMethod = false;
                break;
            }
        }
        if (useBaseMethod) {
            VerticalCRS::_exportToWKT(formatter);
            return;
        }

        io::FormattingException::Throw(
            "DerivedVerticalCRS can only be exported to WKT2");
    }
    baseExportToWKT(formatter, io::WKTConstants::VERTCRS,
                    io::WKTConstants::BASEVERTCRS);
}
//! @endcond

// ---------------------------------------------------------------------------

void DerivedVerticalCRS::_exportToPROJString(
    io::PROJStringFormatter *) const // throw(io::FormattingException)
{
    throw io::FormattingException(
        "DerivedVerticalCRS cannot be exported to PROJ string");
}

// ---------------------------------------------------------------------------

bool DerivedVerticalCRS::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherDerivedCRS = dynamic_cast<const DerivedVerticalCRS *>(other);
    return otherDerivedCRS != nullptr &&
           DerivedCRS::_isEquivalentTo(other, criterion, dbContext);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::list<std::pair<CRSNNPtr, int>>
DerivedVerticalCRS::_identify(const io::AuthorityFactoryPtr &factory) const {
    return CRS::_identify(factory);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
template <class DerivedCRSTraits>
struct DerivedCRSTemplate<DerivedCRSTraits>::Private {};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
template <class DerivedCRSTraits>
DerivedCRSTemplate<DerivedCRSTraits>::~DerivedCRSTemplate() = default;
//! @endcond

// ---------------------------------------------------------------------------

template <class DerivedCRSTraits>
DerivedCRSTemplate<DerivedCRSTraits>::DerivedCRSTemplate(
    const BaseNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn, const CSNNPtr &csIn)
    : SingleCRS(baseCRSIn->datum().as_nullable(), nullptr, csIn),
      BaseType(baseCRSIn->datum(), csIn),
      DerivedCRS(baseCRSIn, derivingConversionIn, csIn), d(nullptr) {}

// ---------------------------------------------------------------------------

template <class DerivedCRSTraits>
DerivedCRSTemplate<DerivedCRSTraits>::DerivedCRSTemplate(
    const DerivedCRSTemplate &other)
    : SingleCRS(other), BaseType(other), DerivedCRS(other), d(nullptr) {}

// ---------------------------------------------------------------------------

template <class DerivedCRSTraits>
const typename DerivedCRSTemplate<DerivedCRSTraits>::BaseNNPtr
DerivedCRSTemplate<DerivedCRSTraits>::baseCRS() const {
    auto l_baseCRS = DerivedCRS::getPrivate()->baseCRS_;
    return NN_NO_CHECK(util::nn_dynamic_pointer_cast<BaseType>(l_baseCRS));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

template <class DerivedCRSTraits>
CRSNNPtr DerivedCRSTemplate<DerivedCRSTraits>::_shallowClone() const {
    auto crs(DerivedCRSTemplate::nn_make_shared<DerivedCRSTemplate>(*this));
    crs->assignSelf(crs);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

template <class DerivedCRSTraits>
typename DerivedCRSTemplate<DerivedCRSTraits>::NNPtr
DerivedCRSTemplate<DerivedCRSTraits>::create(
    const util::PropertyMap &properties, const BaseNNPtr &baseCRSIn,
    const operation::ConversionNNPtr &derivingConversionIn,
    const CSNNPtr &csIn) {
    auto crs(DerivedCRSTemplate::nn_make_shared<DerivedCRSTemplate>(
        baseCRSIn, derivingConversionIn, csIn));
    crs->assignSelf(crs);
    crs->setProperties(properties);
    crs->setDerivingConversionCRS();
    return crs;
}

// ---------------------------------------------------------------------------

template <class DerivedCRSTraits>
const char *DerivedCRSTemplate<DerivedCRSTraits>::className() const {
    return DerivedCRSTraits::CRSName().c_str();
}

// ---------------------------------------------------------------------------

static void DerivedCRSTemplateCheckExportToWKT(io::WKTFormatter *formatter,
                                               const std::string &crsName,
                                               bool wkt2_2019_only) {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2 || (wkt2_2019_only && !formatter->use2019Keywords())) {
        io::FormattingException::Throw(crsName +
                                       " can only be exported to WKT2" +
                                       (wkt2_2019_only ? ":2019" : ""));
    }
}

// ---------------------------------------------------------------------------

template <class DerivedCRSTraits>
void DerivedCRSTemplate<DerivedCRSTraits>::_exportToWKT(
    io::WKTFormatter *formatter) const {
    DerivedCRSTemplateCheckExportToWKT(formatter, DerivedCRSTraits::CRSName(),
                                       DerivedCRSTraits::wkt2_2019_only);
    baseExportToWKT(formatter, DerivedCRSTraits::WKTKeyword(),
                    DerivedCRSTraits::WKTBaseKeyword());
}

// ---------------------------------------------------------------------------

template <class DerivedCRSTraits>
bool DerivedCRSTemplate<DerivedCRSTraits>::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherDerivedCRS = dynamic_cast<const DerivedCRSTemplate *>(other);
    return otherDerivedCRS != nullptr &&
           DerivedCRS::_isEquivalentTo(other, criterion, dbContext);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static const std::string STRING_DerivedEngineeringCRS("DerivedEngineeringCRS");
const std::string &DerivedEngineeringCRSTraits::CRSName() {
    return STRING_DerivedEngineeringCRS;
}
const std::string &DerivedEngineeringCRSTraits::WKTKeyword() {
    return io::WKTConstants::ENGCRS;
}
const std::string &DerivedEngineeringCRSTraits::WKTBaseKeyword() {
    return io::WKTConstants::BASEENGCRS;
}

template class DerivedCRSTemplate<DerivedEngineeringCRSTraits>;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static const std::string STRING_DerivedParametricCRS("DerivedParametricCRS");
const std::string &DerivedParametricCRSTraits::CRSName() {
    return STRING_DerivedParametricCRS;
}
const std::string &DerivedParametricCRSTraits::WKTKeyword() {
    return io::WKTConstants::PARAMETRICCRS;
}
const std::string &DerivedParametricCRSTraits::WKTBaseKeyword() {
    return io::WKTConstants::BASEPARAMCRS;
}

template class DerivedCRSTemplate<DerivedParametricCRSTraits>;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static const std::string STRING_DerivedTemporalCRS("DerivedTemporalCRS");
const std::string &DerivedTemporalCRSTraits::CRSName() {
    return STRING_DerivedTemporalCRS;
}
const std::string &DerivedTemporalCRSTraits::WKTKeyword() {
    return io::WKTConstants::TIMECRS;
}
const std::string &DerivedTemporalCRSTraits::WKTBaseKeyword() {
    return io::WKTConstants::BASETIMECRS;
}

template class DerivedCRSTemplate<DerivedTemporalCRSTraits>;
//! @endcond

// ---------------------------------------------------------------------------

} // namespace crs
NS_PROJ_END
