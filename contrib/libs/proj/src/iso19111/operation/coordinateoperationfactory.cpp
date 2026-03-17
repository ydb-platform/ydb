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

#include "proj/common.hpp"
#include "proj/coordinateoperation.hpp"
#include "proj/coordinates.hpp"
#include "proj/crs.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/datum_internal.hpp"
#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"
#include "proj/internal/tracing.hpp"

#include "coordinateoperation_internal.hpp"
#include "coordinateoperation_private.hpp"
#include "oputils.hpp"
#include "vectorofvaluesparams.hpp"

// PROJ include order is sensitive
// clang-format off
#include "proj.h"
#include "proj_internal.h" // M_PI
// clang-format on
#include "proj_constants.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <vector>

// #define TRACE_CREATE_OPERATIONS
// #define DEBUG_SORT
// #define DEBUG_CONCATENATED_OPERATION
#if defined(DEBUG_SORT) || defined(DEBUG_CONCATENATED_OPERATION)
#include <iostream>

void dumpWKT(const NS_PROJ::crs::CRS *crs);
void dumpWKT(const NS_PROJ::crs::CRS *crs) {
    auto f(NS_PROJ::io::WKTFormatter::create(
        NS_PROJ::io::WKTFormatter::Convention::WKT2_2019));
    std::cerr << crs->exportToWKT(f.get()) << std::endl;
}

void dumpWKT(const NS_PROJ::crs::CRSPtr &crs);
void dumpWKT(const NS_PROJ::crs::CRSPtr &crs) { dumpWKT(crs.get()); }

void dumpWKT(const NS_PROJ::crs::CRSNNPtr &crs);
void dumpWKT(const NS_PROJ::crs::CRSNNPtr &crs) {
    dumpWKT(crs.as_nullable().get());
}

void dumpWKT(const NS_PROJ::crs::GeographicCRSPtr &crs);
void dumpWKT(const NS_PROJ::crs::GeographicCRSPtr &crs) { dumpWKT(crs.get()); }

void dumpWKT(const NS_PROJ::crs::GeographicCRSNNPtr &crs);
void dumpWKT(const NS_PROJ::crs::GeographicCRSNNPtr &crs) {
    dumpWKT(crs.as_nullable().get());
}

#endif

using namespace NS_PROJ::internal;

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

#ifdef TRACE_CREATE_OPERATIONS

//! @cond Doxygen_Suppress

static std::string objectAsStr(const common::IdentifiedObject *obj) {
    std::string ret(obj->nameStr());
    const auto &ids = obj->identifiers();
    if (const auto *geogCRS = dynamic_cast<const crs::GeographicCRS *>(obj)) {
        if (geogCRS->coordinateSystem()->axisList().size() == 3U)
            ret += " (geographic3D)";
        else
            ret += " (geographic2D)";
    }
    if (const auto *geodCRS = dynamic_cast<const crs::GeodeticCRS *>(obj)) {
        if (geodCRS->isGeocentric()) {
            ret += " (geocentric)";
        }
    }
    if (!ids.empty()) {
        ret += " (";
        ret += (*ids[0]->codeSpace()) + ":" + ids[0]->code();
        ret += ")";
    }
    return ret;
}
//! @endcond

#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static double getPseudoArea(const metadata::ExtentPtr &extent) {
    if (!extent)
        return 0.0;
    const auto &geographicElements = extent->geographicElements();
    if (geographicElements.empty())
        return 0.0;
    auto bbox = dynamic_cast<const metadata::GeographicBoundingBox *>(
        geographicElements[0].get());
    if (!bbox)
        return 0;
    double w = bbox->westBoundLongitude();
    double s = bbox->southBoundLatitude();
    double e = bbox->eastBoundLongitude();
    double n = bbox->northBoundLatitude();
    if (w > e) {
        e += 360.0;
    }
    // Integrate cos(lat) between south_lat and north_lat
    return (e - w) * (std::sin(common::Angle(n).getSIValue()) -
                      std::sin(common::Angle(s).getSIValue()));
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct CoordinateOperationContext::Private {
    io::AuthorityFactoryPtr authorityFactory_{};
    metadata::ExtentPtr extent_{};
    double accuracy_ = 0.0;
    SourceTargetCRSExtentUse sourceAndTargetCRSExtentUse_ =
        CoordinateOperationContext::SourceTargetCRSExtentUse::SMALLEST;
    SpatialCriterion spatialCriterion_ =
        CoordinateOperationContext::SpatialCriterion::STRICT_CONTAINMENT;
    bool usePROJNames_ = true;
    GridAvailabilityUse gridAvailabilityUse_ =
        GridAvailabilityUse::USE_FOR_SORTING;
    IntermediateCRSUse allowUseIntermediateCRS_ = CoordinateOperationContext::
        IntermediateCRSUse::IF_NO_DIRECT_TRANSFORMATION;
    std::vector<std::pair<std::string, std::string>>
        intermediateCRSAuthCodes_{};
    bool discardSuperseded_ = true;
    bool allowBallpark_ = true;
    std::shared_ptr<util::optional<common::DataEpoch>> sourceCoordinateEpoch_{
        std::make_shared<util::optional<common::DataEpoch>>()};
    std::shared_ptr<util::optional<common::DataEpoch>> targetCoordinateEpoch_{
        std::make_shared<util::optional<common::DataEpoch>>()};

    Private() = default;
    Private(const Private &) = default;
};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CoordinateOperationContext::~CoordinateOperationContext() = default;
//! @endcond

// ---------------------------------------------------------------------------

CoordinateOperationContext::CoordinateOperationContext()
    : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

CoordinateOperationContext::CoordinateOperationContext(
    const CoordinateOperationContext &other)
    : d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

/** \brief Return the authority factory, or null */
const io::AuthorityFactoryPtr &
CoordinateOperationContext::getAuthorityFactory() const {
    return d->authorityFactory_;
}

// ---------------------------------------------------------------------------

/** \brief Return the desired area of interest, or null */
const metadata::ExtentPtr &
CoordinateOperationContext::getAreaOfInterest() const {
    return d->extent_;
}

// ---------------------------------------------------------------------------

/** \brief Set the desired area of interest, or null */
void CoordinateOperationContext::setAreaOfInterest(
    const metadata::ExtentPtr &extent) {
    d->extent_ = extent;
}

// ---------------------------------------------------------------------------

/** \brief Return the desired accuracy (in metre), or 0 */
double CoordinateOperationContext::getDesiredAccuracy() const {
    return d->accuracy_;
}

// ---------------------------------------------------------------------------

/** \brief Set the desired accuracy (in metre), or 0 */
void CoordinateOperationContext::setDesiredAccuracy(double accuracy) {
    d->accuracy_ = accuracy;
}

// ---------------------------------------------------------------------------

/** \brief Return whether ballpark transformations are allowed */
bool CoordinateOperationContext::getAllowBallparkTransformations() const {
    return d->allowBallpark_;
}

// ---------------------------------------------------------------------------

/** \brief Set whether ballpark transformations are allowed */
void CoordinateOperationContext::setAllowBallparkTransformations(bool allow) {
    d->allowBallpark_ = allow;
}

// ---------------------------------------------------------------------------

/** \brief Set how source and target CRS extent should be used
 * when considering if a transformation can be used (only takes effect if
 * no area of interest is explicitly defined).
 *
 * The default is
 * CoordinateOperationContext::SourceTargetCRSExtentUse::SMALLEST.
 */
void CoordinateOperationContext::setSourceAndTargetCRSExtentUse(
    SourceTargetCRSExtentUse use) {
    d->sourceAndTargetCRSExtentUse_ = use;
}

// ---------------------------------------------------------------------------

/** \brief Return how source and target CRS extent should be used
 * when considering if a transformation can be used (only takes effect if
 * no area of interest is explicitly defined).
 *
 * The default is
 * CoordinateOperationContext::SourceTargetCRSExtentUse::SMALLEST.
 */
CoordinateOperationContext::SourceTargetCRSExtentUse
CoordinateOperationContext::getSourceAndTargetCRSExtentUse() const {
    return d->sourceAndTargetCRSExtentUse_;
}

// ---------------------------------------------------------------------------

/** \brief Set the spatial criterion to use when comparing the area of
 * validity
 * of coordinate operations with the area of interest / area of validity of
 * source and target CRS.
 *
 * The default is STRICT_CONTAINMENT.
 */
void CoordinateOperationContext::setSpatialCriterion(
    SpatialCriterion criterion) {
    d->spatialCriterion_ = criterion;
}

// ---------------------------------------------------------------------------

/** \brief Return the spatial criterion to use when comparing the area of
 * validity
 * of coordinate operations with the area of interest / area of validity of
 * source and target CRS.
 *
 * The default is STRICT_CONTAINMENT.
 */
CoordinateOperationContext::SpatialCriterion
CoordinateOperationContext::getSpatialCriterion() const {
    return d->spatialCriterion_;
}

// ---------------------------------------------------------------------------

/** \brief Set whether PROJ alternative grid names should be substituted to
 * the official authority names.
 *
 * This only has effect is an authority factory with a non-null database context
 * has been attached to this context.
 *
 * If set to false, it is still possible to
 * obtain later the substitution by using io::PROJStringFormatter::create()
 * with a non-null database context.
 *
 * The default is true.
 */
void CoordinateOperationContext::setUsePROJAlternativeGridNames(
    bool usePROJNames) {
    d->usePROJNames_ = usePROJNames;
}

// ---------------------------------------------------------------------------

/** \brief Return whether PROJ alternative grid names should be substituted to
 * the official authority names.
 *
 * The default is true.
 */
bool CoordinateOperationContext::getUsePROJAlternativeGridNames() const {
    return d->usePROJNames_;
}

// ---------------------------------------------------------------------------

/** \brief Return whether transformations that are superseded (but not
 * deprecated)
 * should be discarded.
 *
 * The default is true.
 */
bool CoordinateOperationContext::getDiscardSuperseded() const {
    return d->discardSuperseded_;
}

// ---------------------------------------------------------------------------

/** \brief Set whether transformations that are superseded (but not deprecated)
 * should be discarded.
 *
 * The default is true.
 */
void CoordinateOperationContext::setDiscardSuperseded(bool discard) {
    d->discardSuperseded_ = discard;
}

// ---------------------------------------------------------------------------

/** \brief Set how grid availability is used.
 *
 * The default is USE_FOR_SORTING.
 */
void CoordinateOperationContext::setGridAvailabilityUse(
    GridAvailabilityUse use) {
    d->gridAvailabilityUse_ = use;
}

// ---------------------------------------------------------------------------

/** \brief Return how grid availability is used.
 *
 * The default is USE_FOR_SORTING.
 */
CoordinateOperationContext::GridAvailabilityUse
CoordinateOperationContext::getGridAvailabilityUse() const {
    return d->gridAvailabilityUse_;
}

// ---------------------------------------------------------------------------

/** \brief Set whether an intermediate pivot CRS can be used for researching
 * coordinate operations between a source and target CRS.
 *
 * Concretely if in the database there is an operation from A to C
 * (or C to A), and another one from C to B (or B to C), but no direct
 * operation between A and B, setting this parameter to
 * ALWAYS/IF_NO_DIRECT_TRANSFORMATION, allow chaining both operations.
 *
 * The current implementation is limited to researching one intermediate
 * step.
 *
 * By default, with the IF_NO_DIRECT_TRANSFORMATION strategy, all potential
 * C candidates will be used if there is no direct transformation.
 */
void CoordinateOperationContext::setAllowUseIntermediateCRS(
    IntermediateCRSUse use) {
    d->allowUseIntermediateCRS_ = use;
}

// ---------------------------------------------------------------------------

/** \brief Return whether an intermediate pivot CRS can be used for researching
 * coordinate operations between a source and target CRS.
 *
 * Concretely if in the database there is an operation from A to C
 * (or C to A), and another one from C to B (or B to C), but no direct
 * operation between A and B, setting this parameter to
 * ALWAYS/IF_NO_DIRECT_TRANSFORMATION, allow chaining both operations.
 *
 * The default is IF_NO_DIRECT_TRANSFORMATION.
 */
CoordinateOperationContext::IntermediateCRSUse
CoordinateOperationContext::getAllowUseIntermediateCRS() const {
    return d->allowUseIntermediateCRS_;
}

// ---------------------------------------------------------------------------

/** \brief Restrict the potential pivot CRSs that can be used when trying to
 * build a coordinate operation between two CRS that have no direct operation.
 *
 * @param intermediateCRSAuthCodes a vector of (auth_name, code) that can be
 * used as potential pivot RS
 */
void CoordinateOperationContext::setIntermediateCRS(
    const std::vector<std::pair<std::string, std::string>>
        &intermediateCRSAuthCodes) {
    d->intermediateCRSAuthCodes_ = intermediateCRSAuthCodes;
}

// ---------------------------------------------------------------------------

/** \brief Return the potential pivot CRSs that can be used when trying to
 * build a coordinate operation between two CRS that have no direct operation.
 *
 */
const std::vector<std::pair<std::string, std::string>> &
CoordinateOperationContext::getIntermediateCRS() const {
    return d->intermediateCRSAuthCodes_;
}

// ---------------------------------------------------------------------------

/** \brief Set the source coordinate epoch.
 */
void CoordinateOperationContext::setSourceCoordinateEpoch(
    const util::optional<common::DataEpoch> &epoch) {

    d->sourceCoordinateEpoch_ =
        std::make_shared<util::optional<common::DataEpoch>>(epoch);
}

// ---------------------------------------------------------------------------

/** \brief Return the source coordinate epoch.
 */
const util::optional<common::DataEpoch> &
CoordinateOperationContext::getSourceCoordinateEpoch() const {
    return *(d->sourceCoordinateEpoch_);
}

// ---------------------------------------------------------------------------

/** \brief Set the target coordinate epoch.
 */
void CoordinateOperationContext::setTargetCoordinateEpoch(
    const util::optional<common::DataEpoch> &epoch) {

    d->targetCoordinateEpoch_ =
        std::make_shared<util::optional<common::DataEpoch>>(epoch);
}

// ---------------------------------------------------------------------------

/** \brief Return the target coordinate epoch.
 */
const util::optional<common::DataEpoch> &
CoordinateOperationContext::getTargetCoordinateEpoch() const {
    return *(d->targetCoordinateEpoch_);
}

// ---------------------------------------------------------------------------

/** \brief Creates a context for a coordinate operation.
 *
 * If a non null authorityFactory is provided, the resulting context should
 * not be used simultaneously by more than one thread.
 *
 * If authorityFactory->getAuthority() is the empty string, then coordinate
 * operations from any authority will be searched, with the restrictions set
 * in the authority_to_authority_preference database table.
 * If authorityFactory->getAuthority() is set to "any", then coordinate
 * operations from any authority will be searched
 * If authorityFactory->getAuthority() is a non-empty string different of "any",
 * then coordinate operations will be searched only in that authority namespace.
 *
 * @param authorityFactory Authority factory, or null if no database lookup
 * is allowed.
 * Use io::authorityFactory::create(context, std::string()) to allow all
 * authorities to be used.
 * @param extent Area of interest, or null if none is known.
 * @param accuracy Maximum allowed accuracy in metre, as specified in or
 * 0 to get best accuracy.
 * @return a new context.
 */
CoordinateOperationContextNNPtr CoordinateOperationContext::create(
    const io::AuthorityFactoryPtr &authorityFactory,
    const metadata::ExtentPtr &extent, double accuracy) {
    auto ctxt = NN_NO_CHECK(
        CoordinateOperationContext::make_unique<CoordinateOperationContext>());
    ctxt->d->authorityFactory_ = authorityFactory;
    ctxt->d->extent_ = extent;
    ctxt->d->accuracy_ = accuracy;
    return ctxt;
}

// ---------------------------------------------------------------------------

/** \brief Clone a coordinate operation context.
 *
 * @return a new context.
 * @since 9.2
 */
CoordinateOperationContextNNPtr CoordinateOperationContext::clone() const {
    return NN_NO_CHECK(
        CoordinateOperationContext::make_unique<CoordinateOperationContext>(
            *this));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct CoordinateOperationFactory::Private {

    struct Context {
        // This is the extent of the source CRS and target CRS of the initial
        // CoordinateOperationFactory::createOperations() public call, not
        // necessarily the ones of intermediate
        // CoordinateOperationFactory::Private::createOperations() calls.
        // This is used to compare transformations area of use against the
        // area of use of the source & target CRS.
        const metadata::ExtentPtr &extent1;
        const metadata::ExtentPtr &extent2;
        const CoordinateOperationContextNNPtr &context;
        bool inCreateOperationsWithDatumPivotAntiRecursion = false;
        bool inCreateOperationsGeogToVertWithAlternativeGeog = false;
        bool inCreateOperationsGeogToVertWithIntermediateVert = false;
        bool skipHorizontalTransformation = false;
        int nRecLevelCreateOperations = 0;
        std::map<std::pair<io::AuthorityFactory::ObjectType, std::string>,
                 std::list<std::pair<std::string, std::string>>>
            cacheNameToCRS{};

        // Normally there should be at most one element in this stack
        // This is set when computing a CompoundCRS to GeogCRS operation to
        // relate the VerticalCRS of the CompoundCRS to the geographicCRS of
        // the horizontal component of the CompoundCRS. This is especially
        // used if the VerticalCRS is a DerivedVerticalCRS using a
        // (non-standard) "Ellipsoid" vertical datum
        std::vector<crs::GeographicCRSNNPtr> geogCRSOfVertCRSStack{};

        Context(const metadata::ExtentPtr &extent1In,
                const metadata::ExtentPtr &extent2In,
                const CoordinateOperationContextNNPtr &contextIn)
            : extent1(extent1In), extent2(extent2In), context(contextIn) {}
    };

    static std::vector<CoordinateOperationNNPtr>
    createOperations(const crs::CRSNNPtr &sourceCRS,
                     const util::optional<common::DataEpoch> &sourceEpoch,
                     const crs::CRSNNPtr &targetCRS,
                     const util::optional<common::DataEpoch> &targetEpoch,
                     Context &context);

  private:
    static constexpr bool disallowEmptyIntersection = true;

    static void
    buildCRSIds(const crs::CRSNNPtr &crs, Private::Context &context,
                std::list<std::pair<std::string, std::string>> &ids);

    static std::vector<CoordinateOperationNNPtr> findOpsInRegistryDirect(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, bool &resNonEmptyBeforeFiltering);

    static std::vector<CoordinateOperationNNPtr>
    findOpsInRegistryDirectTo(const crs::CRSNNPtr &targetCRS,
                              Private::Context &context);

    static std::vector<CoordinateOperationNNPtr>
    findsOpsInRegistryWithIntermediate(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context,
        bool useCreateBetweenGeodeticCRSWithDatumBasedIntermediates);

    static void createOperationsFromProj4Ext(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        const crs::BoundCRS *boundSrc, const crs::BoundCRS *boundDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static bool createOperationsFromDatabase(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::GeodeticCRS *geodSrc,
        const crs::GeodeticCRS *geodDst, const crs::GeographicCRS *geogSrc,
        const crs::GeographicCRS *geogDst, const crs::VerticalCRS *vertSrc,
        const crs::VerticalCRS *vertDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static std::vector<CoordinateOperationNNPtr>
    createOperationsGeogToVertFromGeoid(const crs::CRSNNPtr &sourceCRS,
                                        const crs::CRSNNPtr &targetCRS,
                                        const crs::VerticalCRS *vertDst,
                                        Context &context);

    static std::vector<CoordinateOperationNNPtr>
    createOperationsGeogToVertWithIntermediateVert(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        const crs::VerticalCRS *vertDst, Context &context);

    static std::vector<CoordinateOperationNNPtr>
    createOperationsGeogToVertWithAlternativeGeog(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Context &context);

    static void createOperationsFromDatabaseWithVertCRS(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::GeographicCRS *geogSrc,
        const crs::GeographicCRS *geogDst, const crs::VerticalCRS *vertSrc,
        const crs::VerticalCRS *vertDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsGeodToGeod(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::GeodeticCRS *geodSrc,
        const crs::GeodeticCRS *geodDst,
        std::vector<CoordinateOperationNNPtr> &res, bool forceBallpark);

    static void createOperationsFromSphericalPlanetocentric(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::GeodeticCRS *geodSrc,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsFromBoundOfSphericalPlanetocentric(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::BoundCRS *boundSrc,
        const crs::GeodeticCRSNNPtr &geodSrcBase,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsDerivedTo(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::DerivedCRS *derivedSrc,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsBoundToGeog(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::BoundCRS *boundSrc,
        const crs::GeographicCRS *geogDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsBoundToVert(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::BoundCRS *boundSrc,
        const crs::VerticalCRS *vertDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsVertToVert(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::VerticalCRS *vertSrc,
        const crs::VerticalCRS *vertDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsVertToGeog(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::VerticalCRS *vertSrc,
        const crs::GeographicCRS *geogDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsVertToGeogSynthetized(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::VerticalCRS *vertSrc,
        const crs::GeographicCRS *geogDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsBoundToBound(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::BoundCRS *boundSrc,
        const crs::BoundCRS *boundDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsCompoundToGeog(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::CompoundCRS *compoundSrc,
        const crs::GeographicCRS *geogDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void
    createOperationsToGeod(const crs::CRSNNPtr &sourceCRS,
                           const util::optional<common::DataEpoch> &sourceEpoch,
                           const crs::CRSNNPtr &targetCRS,
                           const util::optional<common::DataEpoch> &targetEpoch,
                           Private::Context &context,
                           const crs::GeodeticCRS *geodDst,
                           std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsCompoundToCompound(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::CompoundCRS *compoundSrc,
        const crs::CompoundCRS *compoundDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static void createOperationsBoundToCompound(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::BoundCRS *boundSrc,
        const crs::CompoundCRS *compoundDst,
        std::vector<CoordinateOperationNNPtr> &res);

    static std::vector<CoordinateOperationNNPtr> createOperationsGeogToGeog(
        std::vector<CoordinateOperationNNPtr> &res,
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::GeographicCRS *geogSrc,
        const crs::GeographicCRS *geogDst, bool forceBallpark);

    static void createOperationsWithDatumPivot(
        std::vector<CoordinateOperationNNPtr> &res,
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        const crs::GeodeticCRS *geodSrc, const crs::GeodeticCRS *geodDst,
        Context &context);

    static bool
    hasPerfectAccuracyResult(const std::vector<CoordinateOperationNNPtr> &res,
                             const Context &context);

    static void setCRSs(CoordinateOperation *co, const crs::CRSNNPtr &sourceCRS,
                        const crs::CRSNNPtr &targetCRS);
};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CoordinateOperationFactory::~CoordinateOperationFactory() = default;
//! @endcond

// ---------------------------------------------------------------------------

CoordinateOperationFactory::CoordinateOperationFactory() : d(nullptr) {}

// ---------------------------------------------------------------------------

/** \brief Find a CoordinateOperation from sourceCRS to targetCRS.
 *
 * This is a helper of createOperations(), using a coordinate operation
 * context
 * with no authority factory (so no catalog searching is done), no desired
 * accuracy and no area of interest.
 * This returns the first operation of the result set of createOperations(),
 * or null if none found.
 *
 * @param sourceCRS source CRS.
 * @param targetCRS source CRS.
 * @return a CoordinateOperation or nullptr.
 */
CoordinateOperationPtr CoordinateOperationFactory::createOperation(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS) const {
    auto res = createOperations(
        sourceCRS, targetCRS,
        CoordinateOperationContext::create(nullptr, nullptr, 0.0));
    if (!res.empty()) {
        return res[0];
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

struct PrecomputedOpCharacteristics {
    double area_{};
    double accuracy_{};
    bool isPROJExportable_ = false;
    bool hasGrids_ = false;
    bool gridsAvailable_ = false;
    bool gridsKnown_ = false;
    size_t stepCount_ = 0;
    size_t projStepCount_ = 0;
    bool isApprox_ = false;
    bool hasBallparkVertical_ = false;
    bool isNullTransformation_ = false;

    PrecomputedOpCharacteristics() = default;
    PrecomputedOpCharacteristics(double area, double accuracy,
                                 bool isPROJExportable, bool hasGrids,
                                 bool gridsAvailable, bool gridsKnown,
                                 size_t stepCount, size_t projStepCount,
                                 bool isApprox, bool hasBallparkVertical,
                                 bool isNullTransformation)
        : area_(area), accuracy_(accuracy), isPROJExportable_(isPROJExportable),
          hasGrids_(hasGrids), gridsAvailable_(gridsAvailable),
          gridsKnown_(gridsKnown), stepCount_(stepCount),
          projStepCount_(projStepCount), isApprox_(isApprox),
          hasBallparkVertical_(hasBallparkVertical),
          isNullTransformation_(isNullTransformation) {}
};

// ---------------------------------------------------------------------------

// We could have used a lambda instead of this old-school way, but
// filterAndSort() is already huge.
struct SortFunction {

    explicit SortFunction(const std::map<CoordinateOperation *,
                                         PrecomputedOpCharacteristics> &mapIn)
        : map(mapIn), BALLPARK_GEOGRAPHIC_OFFSET_FROM(
                          std::string(BALLPARK_GEOGRAPHIC_OFFSET) + " from ") {}

    // Sorting function
    // Return true if a < b
    bool compare(const CoordinateOperationNNPtr &a,
                 const CoordinateOperationNNPtr &b) const {
        auto iterA = map.find(a.get());
        assert(iterA != map.end());
        auto iterB = map.find(b.get());
        assert(iterB != map.end());

        // CAUTION: the order of the comparisons is extremely important
        // to get the intended result.

        if (iterA->second.isPROJExportable_ &&
            !iterB->second.isPROJExportable_) {
            return true;
        }
        if (!iterA->second.isPROJExportable_ &&
            iterB->second.isPROJExportable_) {
            return false;
        }

        if (!iterA->second.isApprox_ && iterB->second.isApprox_) {
            return true;
        }
        if (iterA->second.isApprox_ && !iterB->second.isApprox_) {
            return false;
        }

        if (!iterA->second.hasBallparkVertical_ &&
            iterB->second.hasBallparkVertical_) {
            return true;
        }
        if (iterA->second.hasBallparkVertical_ &&
            !iterB->second.hasBallparkVertical_) {
            return false;
        }

        if (!iterA->second.isNullTransformation_ &&
            iterB->second.isNullTransformation_) {
            return true;
        }
        if (iterA->second.isNullTransformation_ &&
            !iterB->second.isNullTransformation_) {
            return false;
        }

        // Operations where grids are all available go before other
        if (iterA->second.gridsAvailable_ && !iterB->second.gridsAvailable_) {
            return true;
        }
        if (iterB->second.gridsAvailable_ && !iterA->second.gridsAvailable_) {
            return false;
        }

        // Operations where grids are all known in our DB go before other
        if (iterA->second.gridsKnown_ && !iterB->second.gridsKnown_) {
            return true;
        }
        if (iterB->second.gridsKnown_ && !iterA->second.gridsKnown_) {
            return false;
        }

        // Operations with known accuracy go before those with unknown accuracy
        const double accuracyA = iterA->second.accuracy_;
        const double accuracyB = iterB->second.accuracy_;
        if (accuracyA >= 0 && accuracyB < 0) {
            return true;
        }
        if (accuracyB >= 0 && accuracyA < 0) {
            return false;
        }

        if (accuracyA < 0 && accuracyB < 0) {
            // unknown accuracy ? then prefer operations with grids, which
            // are likely to have best practical accuracy
            if (iterA->second.hasGrids_ && !iterB->second.hasGrids_) {
                return true;
            }
            if (!iterA->second.hasGrids_ && iterB->second.hasGrids_) {
                return false;
            }
        }

        // Operations with larger non-zero area of use go before those with
        // lower one
        const double areaA = iterA->second.area_;
        const double areaB = iterB->second.area_;
        if (areaA > 0) {
            if (areaA > areaB) {
                return true;
            }
            if (areaA < areaB) {
                return false;
            }
        } else if (areaB > 0) {
            return false;
        }

        // Operations with better accuracy go before those with worse one
        if (accuracyA >= 0 && accuracyA < accuracyB) {
            return true;
        }
        if (accuracyB >= 0 && accuracyB < accuracyA) {
            return false;
        }

        if (accuracyA >= 0 && accuracyA == accuracyB) {
            // same accuracy ? then prefer operations without grids
            if (!iterA->second.hasGrids_ && iterB->second.hasGrids_) {
                return true;
            }
            if (iterA->second.hasGrids_ && !iterB->second.hasGrids_) {
                return false;
            }
        }

        // The less intermediate steps, the better
        if (iterA->second.stepCount_ < iterB->second.stepCount_) {
            return true;
        }
        if (iterB->second.stepCount_ < iterA->second.stepCount_) {
            return false;
        }

        // Compare number of steps in PROJ pipeline, and prefer the ones
        // with less operations.
        if (iterA->second.projStepCount_ != 0 &&
            iterB->second.projStepCount_ != 0) {
            if (iterA->second.projStepCount_ < iterB->second.projStepCount_) {
                return true;
            }
            if (iterB->second.projStepCount_ < iterA->second.projStepCount_) {
                return false;
            }
        }

        const auto &a_name = a->nameStr();
        const auto &b_name = b->nameStr();

        // Make sure that
        // "Ballpark geographic offset from NAD83(CSRS)v6 to NAD83(CSRS)"
        // has more priority than
        // "Ballpark geographic offset from ITRF2008 to NAD83(CSRS)"
        const auto posA = a_name.find(BALLPARK_GEOGRAPHIC_OFFSET_FROM);
        const auto posB = b_name.find(BALLPARK_GEOGRAPHIC_OFFSET_FROM);
        if (posA != std::string::npos && posB != std::string::npos) {
            const auto pos2A = a_name.find(" to ", posA);
            const auto pos2B = b_name.find(" to ", posB);
            if (pos2A != std::string::npos && pos2B != std::string::npos) {
                const auto pos3A = a_name.find(" + ", pos2A);
                const auto pos3B = b_name.find(" + ", pos2B);
                const std::string fromA = a_name.substr(
                    posA + BALLPARK_GEOGRAPHIC_OFFSET_FROM.size(),
                    pos2A - (posA + BALLPARK_GEOGRAPHIC_OFFSET_FROM.size()));
                const std::string toA =
                    a_name.substr(pos2A + strlen(" to "),
                                  pos3A == std::string::npos
                                      ? pos3A
                                      : pos3A - (pos2A + strlen(" to ")));
                const std::string fromB = b_name.substr(
                    posB + BALLPARK_GEOGRAPHIC_OFFSET_FROM.size(),
                    pos2B - (posB + BALLPARK_GEOGRAPHIC_OFFSET_FROM.size()));
                const std::string toB =
                    b_name.substr(pos2B + strlen(" to "),
                                  pos3B == std::string::npos
                                      ? pos3B
                                      : pos3B - (pos2B + strlen(" to ")));
                const bool similarCRSInA =
                    (fromA.find(toA) == 0 || toA.find(fromA) == 0);
                const bool similarCRSInB =
                    (fromB.find(toB) == 0 || toB.find(fromB) == 0);
                if (similarCRSInA && !similarCRSInB) {
                    return true;
                }
                if (!similarCRSInA && similarCRSInB) {
                    return false;
                }
            }
        }

        // The shorter name, the better ?
        if (a_name.size() < b_name.size()) {
            return true;
        }
        if (b_name.size() < a_name.size()) {
            return false;
        }

        // Arbitrary final criterion. We actually return the greater element
        // first, so that "Amersfoort to WGS 84 (4)" is presented before
        // "Amersfoort to WGS 84 (3)", which is probably a better guess.

        // Except for French NTF (Paris) to NTF, where the (1) conversion
        // should be preferred because in the remarks of (2), it is mentioned
        // OGP prefers value from IGN Paris (code 1467)...
        if (a_name.find("NTF (Paris) to NTF (1)") != std::string::npos &&
            b_name.find("NTF (Paris) to NTF (2)") != std::string::npos) {
            return true;
        }
        if (a_name.find("NTF (Paris) to NTF (2)") != std::string::npos &&
            b_name.find("NTF (Paris) to NTF (1)") != std::string::npos) {
            return false;
        }
        if (a_name.find("NTF (Paris) to RGF93 v1 (1)") != std::string::npos &&
            b_name.find("NTF (Paris) to RGF93 v1 (2)") != std::string::npos) {
            return true;
        }
        if (a_name.find("NTF (Paris) to RGF93 v1 (2)") != std::string::npos &&
            b_name.find("NTF (Paris) to RGF93 v1 (1)") != std::string::npos) {
            return false;
        }

        return a_name > b_name;
    }

    bool operator()(const CoordinateOperationNNPtr &a,
                    const CoordinateOperationNNPtr &b) const {
        const bool ret = compare(a, b);
#if 0
        std::cerr << a->nameStr() << " < " << b->nameStr() << " : " << ret << std::endl;
#endif
        return ret;
    }

    SortFunction(const SortFunction &) = default;
    SortFunction &operator=(const SortFunction &) = delete;
    SortFunction(SortFunction &&) = default;
    SortFunction &operator=(SortFunction &&) = delete;

  private:
    const std::map<CoordinateOperation *, PrecomputedOpCharacteristics> &map;
    const std::string BALLPARK_GEOGRAPHIC_OFFSET_FROM;
};

// ---------------------------------------------------------------------------

static size_t getStepCount(const CoordinateOperationNNPtr &op) {
    auto concat = dynamic_cast<const ConcatenatedOperation *>(op.get());
    size_t stepCount = 1;
    if (concat) {
        stepCount = concat->operations().size();
    }
    return stepCount;
}

// ---------------------------------------------------------------------------

// Return number of steps that are transformations (and not conversions)
static size_t getTransformationStepCount(const CoordinateOperationNNPtr &op) {
    auto concat = dynamic_cast<const ConcatenatedOperation *>(op.get());
    size_t stepCount = 1;
    if (concat) {
        stepCount = 0;
        for (const auto &subOp : concat->operations()) {
            if (dynamic_cast<const Conversion *>(subOp.get()) == nullptr) {
                stepCount++;
            }
        }
    }
    return stepCount;
}

// ---------------------------------------------------------------------------

static bool isNullTransformation(const std::string &name) {
    if (name.find(" + ") != std::string::npos)
        return false;
    return starts_with(name, BALLPARK_GEOCENTRIC_TRANSLATION) ||
           starts_with(name, BALLPARK_GEOGRAPHIC_OFFSET) ||
           starts_with(name, NULL_GEOGRAPHIC_OFFSET) ||
           starts_with(name, NULL_GEOCENTRIC_TRANSLATION);
}

// ---------------------------------------------------------------------------

struct FilterResults {

    FilterResults(const std::vector<CoordinateOperationNNPtr> &sourceListIn,
                  const CoordinateOperationContextNNPtr &contextIn,
                  const metadata::ExtentPtr &extent1In,
                  const metadata::ExtentPtr &extent2In,
                  bool forceStrictContainmentTest)
        : sourceList(sourceListIn), context(contextIn), extent1(extent1In),
          extent2(extent2In), areaOfInterest(context->getAreaOfInterest()),
          areaOfInterestUserSpecified(areaOfInterest != nullptr),
          desiredAccuracy(context->getDesiredAccuracy()),
          sourceAndTargetCRSExtentUse(
              context->getSourceAndTargetCRSExtentUse()) {

        computeAreaOfInterest();
        filterOut(forceStrictContainmentTest);
    }

    FilterResults &andSort() {
        sort();

        // And now that we have a sorted list, we can remove uninteresting
        // results
        // ...
        removeSyntheticNullTransforms();
        removeUninterestingOps();
        removeDuplicateOps();
        removeSyntheticNullTransforms();
        return *this;
    }

    // ----------------------------------------------------------------------

    // cppcheck-suppress functionStatic
    const std::vector<CoordinateOperationNNPtr> &getRes() { return res; }

    // ----------------------------------------------------------------------
  private:
    const std::vector<CoordinateOperationNNPtr> &sourceList;
    const CoordinateOperationContextNNPtr &context;
    const metadata::ExtentPtr &extent1;
    const metadata::ExtentPtr &extent2;
    metadata::ExtentPtr areaOfInterest;
    const bool areaOfInterestUserSpecified;
    const double desiredAccuracy = context->getDesiredAccuracy();
    const CoordinateOperationContext::SourceTargetCRSExtentUse
        sourceAndTargetCRSExtentUse;

    bool hasOpThatContainsAreaOfInterestAndNoGrid = false;
    std::vector<CoordinateOperationNNPtr> res{};

    // ----------------------------------------------------------------------
    void computeAreaOfInterest() {

        // Compute an area of interest from the CRS extent if the user did
        // not specify one
        if (!areaOfInterest) {
            if (sourceAndTargetCRSExtentUse ==
                CoordinateOperationContext::SourceTargetCRSExtentUse::
                    INTERSECTION) {
                if (extent1 && extent2) {
                    areaOfInterest =
                        extent1->intersection(NN_NO_CHECK(extent2));
                }
            } else if (sourceAndTargetCRSExtentUse ==
                       CoordinateOperationContext::SourceTargetCRSExtentUse::
                           SMALLEST) {
                if (extent1 && extent2) {
                    if (getPseudoArea(extent1) < getPseudoArea(extent2)) {
                        areaOfInterest = extent1;
                    } else {
                        areaOfInterest = extent2;
                    }
                } else if (extent1) {
                    areaOfInterest = extent1;
                } else {
                    areaOfInterest = extent2;
                }
            }
        }
    }

    // ---------------------------------------------------------------------------

    void filterOut(bool forceStrictContainmentTest) {

        // Filter out operations that do not match the expected accuracy
        // and area of use.
        const auto spatialCriterion =
            forceStrictContainmentTest
                ? CoordinateOperationContext::SpatialCriterion::
                      STRICT_CONTAINMENT
                : context->getSpatialCriterion();
        bool hasOnlyBallpark = true;
        bool hasNonBallparkWithoutExtent = false;
        bool hasNonBallparkOpWithExtent = false;
        const bool allowBallpark = context->getAllowBallparkTransformations();

        bool foundExtentWithExpectedDescription = false;
        if (areaOfInterestUserSpecified && areaOfInterest &&
            areaOfInterest->description().has_value()) {
            for (const auto &op : sourceList) {
                bool emptyIntersection = false;
                auto extent = getExtent(op, true, emptyIntersection);
                if (extent && extent->description().has_value()) {
                    if (*(areaOfInterest->description()) ==
                        *(extent->description())) {
                        foundExtentWithExpectedDescription = true;
                        break;
                    }
                }
            }
        }

        for (const auto &op : sourceList) {
            if (desiredAccuracy != 0) {
                const double accuracy = getAccuracy(op);
                if (accuracy < 0 || accuracy > desiredAccuracy) {
                    continue;
                }
            }
            if (!allowBallpark && op->hasBallparkTransformation()) {
                continue;
            }
            if (areaOfInterest) {
                bool emptyIntersection = false;
                auto extent = getExtent(op, true, emptyIntersection);
                if (!extent) {
                    if (!op->hasBallparkTransformation()) {
                        hasNonBallparkWithoutExtent = true;
                    }
                    continue;
                }
                if (foundExtentWithExpectedDescription &&
                    (!extent->description().has_value() ||
                     *(areaOfInterest->description()) !=
                         *(extent->description()))) {
                    continue;
                }
                if (!op->hasBallparkTransformation()) {
                    hasNonBallparkOpWithExtent = true;
                }
                bool extentContains =
                    extent->contains(NN_NO_CHECK(areaOfInterest));
                if (!hasOpThatContainsAreaOfInterestAndNoGrid &&
                    extentContains) {
                    if (!op->hasBallparkTransformation() &&
                        op->gridsNeeded(nullptr, true).empty()) {
                        hasOpThatContainsAreaOfInterestAndNoGrid = true;
                    }
                }
                if (spatialCriterion ==
                        CoordinateOperationContext::SpatialCriterion::
                            STRICT_CONTAINMENT &&
                    !extentContains) {
                    continue;
                }
                if (spatialCriterion ==
                        CoordinateOperationContext::SpatialCriterion::
                            PARTIAL_INTERSECTION &&
                    !extent->intersects(NN_NO_CHECK(areaOfInterest))) {
                    continue;
                }
            } else if (sourceAndTargetCRSExtentUse ==
                       CoordinateOperationContext::SourceTargetCRSExtentUse::
                           BOTH) {
                bool emptyIntersection = false;
                auto extent = getExtent(op, true, emptyIntersection);
                if (!extent) {
                    if (!op->hasBallparkTransformation()) {
                        hasNonBallparkWithoutExtent = true;
                    }
                    continue;
                }
                if (!op->hasBallparkTransformation()) {
                    hasNonBallparkOpWithExtent = true;
                }
                bool extentContainsExtent1 =
                    !extent1 || extent->contains(NN_NO_CHECK(extent1));
                bool extentContainsExtent2 =
                    !extent2 || extent->contains(NN_NO_CHECK(extent2));
                if (!hasOpThatContainsAreaOfInterestAndNoGrid &&
                    extentContainsExtent1 && extentContainsExtent2) {
                    if (!op->hasBallparkTransformation() &&
                        op->gridsNeeded(nullptr, true).empty()) {
                        hasOpThatContainsAreaOfInterestAndNoGrid = true;
                    }
                }
                if (spatialCriterion ==
                    CoordinateOperationContext::SpatialCriterion::
                        STRICT_CONTAINMENT) {
                    if (!extentContainsExtent1 || !extentContainsExtent2) {
                        continue;
                    }
                } else if (spatialCriterion ==
                           CoordinateOperationContext::SpatialCriterion::
                               PARTIAL_INTERSECTION) {
                    bool extentIntersectsExtent1 =
                        !extent1 || extent->intersects(NN_NO_CHECK(extent1));
                    bool extentIntersectsExtent2 =
                        extent2 && extent->intersects(NN_NO_CHECK(extent2));
                    if (!extentIntersectsExtent1 || !extentIntersectsExtent2) {
                        continue;
                    }
                }
            }
            if (!op->hasBallparkTransformation()) {
                hasOnlyBallpark = false;
            }
            res.emplace_back(op);
        }

        // In case no operation has an extent and no result is found,
        // retain all initial operations that match accuracy criterion.
        if ((res.empty() && !hasNonBallparkOpWithExtent) ||
            (hasOnlyBallpark && hasNonBallparkWithoutExtent)) {
            for (const auto &op : sourceList) {
                if (desiredAccuracy != 0) {
                    const double accuracy = getAccuracy(op);
                    if (accuracy < 0 || accuracy > desiredAccuracy) {
                        continue;
                    }
                }
                if (!allowBallpark && op->hasBallparkTransformation()) {
                    continue;
                }
                res.emplace_back(op);
            }
        }
    }

    // ----------------------------------------------------------------------

    void sort() {

        // Precompute a number of parameters for each operation that will be
        // useful for the sorting.
        std::map<CoordinateOperation *, PrecomputedOpCharacteristics> map;
        const auto gridAvailabilityUse = context->getGridAvailabilityUse();
        for (const auto &op : res) {
            bool dummy = false;
            auto extentOp = getExtent(op, true, dummy);
            double area = 0.0;
            if (extentOp) {
                if (areaOfInterest) {
                    area = getPseudoArea(
                        extentOp->intersection(NN_NO_CHECK(areaOfInterest)));
                } else if (extent1 && extent2) {
                    auto x = extentOp->intersection(NN_NO_CHECK(extent1));
                    auto y = extentOp->intersection(NN_NO_CHECK(extent2));
                    area = getPseudoArea(x) + getPseudoArea(y) -
                           ((x && y)
                                ? getPseudoArea(x->intersection(NN_NO_CHECK(y)))
                                : 0.0);
                } else if (extent1) {
                    area = getPseudoArea(
                        extentOp->intersection(NN_NO_CHECK(extent1)));
                } else if (extent2) {
                    area = getPseudoArea(
                        extentOp->intersection(NN_NO_CHECK(extent2)));
                } else {
                    area = getPseudoArea(extentOp);
                }
            }

            bool hasGrids = false;
            bool gridsAvailable = true;
            bool gridsKnown = true;
            if (context->getAuthorityFactory()) {
                const auto gridsNeeded = op->gridsNeeded(
                    context->getAuthorityFactory()->databaseContext(),
                    gridAvailabilityUse ==
                        CoordinateOperationContext::GridAvailabilityUse::
                            KNOWN_AVAILABLE);
                for (const auto &gridDesc : gridsNeeded) {
                    hasGrids = true;
                    if (gridAvailabilityUse ==
                            CoordinateOperationContext::GridAvailabilityUse::
                                USE_FOR_SORTING &&
                        !gridDesc.available) {
                        gridsAvailable = false;
                    }
                    if (gridDesc.packageName.empty() &&
                        !(!gridDesc.url.empty() && gridDesc.openLicense) &&
                        !gridDesc.available) {
                        gridsKnown = false;
                    }
                }
            }

            const auto stepCount = getStepCount(op);

            bool isPROJExportable = false;
            auto formatter = io::PROJStringFormatter::create();
            size_t projStepCount = 0;
            try {
                const auto str = op->exportToPROJString(formatter.get());
                // Grids might be missing, but at least this is something
                // PROJ could potentially process
                isPROJExportable = true;

                // We exclude pipelines with +proj=xyzgridshift as they
                // generate more steps, but are more precise.
                if (str.find("+proj=xyzgridshift") == std::string::npos) {
                    auto formatter2 = io::PROJStringFormatter::create();
                    formatter2->ingestPROJString(str);
                    projStepCount = formatter2->getStepCount();
                }
            } catch (const std::exception &) {
            }

#if 0
            std::cerr << "name=" << op->nameStr() << " ";
            std::cerr << "area=" << area << " ";
            std::cerr << "accuracy=" << getAccuracy(op) << " ";
            std::cerr << "isPROJExportable=" << isPROJExportable << " ";
            std::cerr << "hasGrids=" << hasGrids << " ";
            std::cerr << "gridsAvailable=" << gridsAvailable << " ";
            std::cerr << "gridsKnown=" << gridsKnown << " ";
            std::cerr << "stepCount=" << stepCount << " ";
            std::cerr << "projStepCount=" << projStepCount << " ";
            std::cerr << "ballpark=" << op->hasBallparkTransformation() << " ";
            std::cerr << "vertBallpark="
                      << (op->nameStr().find(
                              BALLPARK_VERTICAL_TRANSFORMATION) !=
                          std::string::npos)
                      << " ";
            std::cerr << "isNull=" << isNullTransformation(op->nameStr())
                      << " ";
            std::cerr << std::endl;
#endif
            map[op.get()] = PrecomputedOpCharacteristics(
                area, getAccuracy(op), isPROJExportable, hasGrids,
                gridsAvailable, gridsKnown, stepCount, projStepCount,
                op->hasBallparkTransformation(),
                op->nameStr().find(BALLPARK_VERTICAL_TRANSFORMATION) !=
                    std::string::npos,
                isNullTransformation(op->nameStr()));
        }

        // Sort !
        std::sort(res.begin(), res.end(), SortFunction(map));

// Debug code to check consistency of the sort function
#ifdef DEBUG_SORT
        constexpr bool debugSort = true;
#elif !defined(NDEBUG)
        const bool debugSort = getenv("PROJ_DEBUG_SORT_FUNCT") != nullptr;
#endif
#if defined(DEBUG_SORT) || !defined(NDEBUG)
        if (debugSort) {
            SortFunction sortFunc(map);
            const bool assertIfIssue =
                !(getenv("PROJ_DEBUG_SORT_FUNCT_ASSERT") != nullptr);
            for (size_t i = 0; i < res.size(); ++i) {
                for (size_t j = i + 1; j < res.size(); ++j) {
                    if (sortFunc(res[j], res[i])) {
#ifdef DEBUG_SORT
                        std::cerr << "Sorting issue with entry " << i << "("
                                  << res[i]->nameStr() << ") and " << j << "("
                                  << res[j]->nameStr() << ")" << std::endl;
#endif
                        if (assertIfIssue) {
                            assert(false);
                        }
                    }
                }
            }
        }
#endif
    }

    // ----------------------------------------------------------------------

    void removeSyntheticNullTransforms() {

        // If we have more than one result, and than the last result is the
        // default "Ballpark geographic offset" or "Ballpark geocentric
        // translation" operations we have synthesized, and that at least one
        // operation has the desired area of interest and does not require the
        // use of grids, remove it as all previous results are necessarily
        // better
        if (hasOpThatContainsAreaOfInterestAndNoGrid && res.size() > 1) {
            const auto &opLast = res.back();
            if (opLast->hasBallparkTransformation() ||
                isNullTransformation(opLast->nameStr())) {
                std::vector<CoordinateOperationNNPtr> resTemp;
                for (size_t i = 0; i < res.size() - 1; i++) {
                    resTemp.emplace_back(res[i]);
                }
                res = std::move(resTemp);
            }
        }
    }

    // ----------------------------------------------------------------------

    void removeUninterestingOps() {

        // Eliminate operations that bring nothing, ie for a given area of use,
        // do not keep operations that have similar or worse accuracy, but
        // involve more (non conversion) steps
        std::vector<CoordinateOperationNNPtr> resTemp;
        metadata::ExtentPtr lastExtent;
        double lastAccuracy = -1;
        size_t lastStepCount = 0;
        CoordinateOperationPtr lastOp;

        bool first = true;
        for (const auto &op : res) {
            const auto curAccuracy = getAccuracy(op);
            bool dummy = false;
            auto curExtent = getExtent(op, true, dummy);
            // If a concatenated operation has an identifier, consider it as
            // a single step (to be opposed to synthesized concatenated
            // operations). Helps for example to get EPSG:8537,
            // "Egypt 1907 to WGS 84 (2)"
            const auto curStepCount =
                op->identifiers().empty() ? getTransformationStepCount(op) : 1;

            if (first) {
                resTemp.emplace_back(op);
                first = false;
            } else {
                if (lastOp->_isEquivalentTo(op.get())) {
                    continue;
                }
                const bool sameExtent =
                    ((!curExtent && !lastExtent) ||
                     (curExtent && lastExtent &&
                      curExtent->contains(NN_NO_CHECK(lastExtent)) &&
                      lastExtent->contains(NN_NO_CHECK(curExtent))));
                if (((curAccuracy >= lastAccuracy && lastAccuracy >= 0) ||
                     (curAccuracy < 0 && lastAccuracy >= 0)) &&
                    sameExtent && curStepCount > lastStepCount) {
                    continue;
                }

                resTemp.emplace_back(op);
            }

            lastOp = op.as_nullable();
            lastStepCount = curStepCount;
            lastExtent = std::move(curExtent);
            lastAccuracy = curAccuracy;
        }
        res = std::move(resTemp);
    }

    // ----------------------------------------------------------------------

    // cppcheck-suppress functionStatic
    void removeDuplicateOps() {

        if (res.size() <= 1) {
            return;
        }

        // When going from EPSG:4807 (NTF Paris) to EPSG:4171 (RGC93), we get
        // EPSG:7811, NTF (Paris) to RGF93 (2), 1 m
        // and unknown id, NTF (Paris) to NTF (1) + Inverse of RGF93 to NTF (2),
        // 1 m
        // both have same PROJ string and extent
        // Do not keep the later (that has more steps) as it adds no value.

        std::set<std::string> setPROJPlusExtent;
        std::vector<CoordinateOperationNNPtr> resTemp;
        for (const auto &op : res) {
            auto formatter = io::PROJStringFormatter::create();
            try {
                std::string key(op->exportToPROJString(formatter.get()));
                bool dummy = false;
                auto extentOp = getExtent(op, true, dummy);
                if (extentOp) {
                    const auto &geogElts = extentOp->geographicElements();
                    if (geogElts.size() == 1) {
                        auto bbox = dynamic_cast<
                            const metadata::GeographicBoundingBox *>(
                            geogElts[0].get());
                        if (bbox) {
                            double w = bbox->westBoundLongitude();
                            double s = bbox->southBoundLatitude();
                            double e = bbox->eastBoundLongitude();
                            double n = bbox->northBoundLatitude();
                            key += "-";
                            key += toString(w);
                            key += "-";
                            key += toString(s);
                            key += "-";
                            key += toString(e);
                            key += "-";
                            key += toString(n);
                        }
                    }
                }

                if (setPROJPlusExtent.find(key) == setPROJPlusExtent.end()) {
                    resTemp.emplace_back(op);
                    setPROJPlusExtent.insert(std::move(key));
                }
            } catch (const std::exception &) {
                resTemp.emplace_back(op);
            }
        }
        res = std::move(resTemp);
    }
};

// ---------------------------------------------------------------------------

/** \brief Filter operations and sort them given context.
 *
 * If a desired accuracy is specified, only keep operations whose accuracy
 * is at least the desired one.
 * If an area of interest is specified, only keep operations whose area of
 * use include the area of interest.
 * Then sort remaining operations by descending area of use, and increasing
 * accuracy.
 */
static std::vector<CoordinateOperationNNPtr>
filterAndSort(const std::vector<CoordinateOperationNNPtr> &sourceList,
              const CoordinateOperationContextNNPtr &context,
              const metadata::ExtentPtr &extent1,
              const metadata::ExtentPtr &extent2) {
#ifdef TRACE_CREATE_OPERATIONS
    ENTER_FUNCTION();
    logTrace("number of results before filter and sort: " +
             toString(static_cast<int>(sourceList.size())));
#endif
    FilterResults filterResults(sourceList, context, extent1, extent2, false);
    filterResults.andSort();
    const auto &resFiltered = filterResults.getRes();
#ifdef TRACE_CREATE_OPERATIONS
    logTrace("number of results after filter and sort: " +
             toString(static_cast<int>(resFiltered.size())));
#endif
    return resFiltered;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
// Apply the inverse() method on all elements of the input list
static std::vector<CoordinateOperationNNPtr>
applyInverse(const std::vector<CoordinateOperationNNPtr> &list) {
    auto res = list;
    for (auto &op : res) {
#ifdef DEBUG
        auto opNew = op->inverse();
        assert(opNew->targetCRS()->isEquivalentTo(op->sourceCRS().get()));
        assert(opNew->sourceCRS()->isEquivalentTo(op->targetCRS().get()));
        op = opNew;
#else
        op = op->inverse();
#endif
    }
    return res;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

void CoordinateOperationFactory::Private::buildCRSIds(
    const crs::CRSNNPtr &crs, Private::Context &context,
    std::list<std::pair<std::string, std::string>> &ids) {
    const auto &authFactory = context.context->getAuthorityFactory();
    assert(authFactory);
    for (const auto &id : crs->identifiers()) {
        const auto &authName = *(id->codeSpace());
        const auto &code = id->code();
        if (!authName.empty()) {
            const auto tmpAuthFactory = io::AuthorityFactory::create(
                authFactory->databaseContext(), authName);
            try {
                // Consistency check for the ID attached to the object.
                // See https://github.com/OSGeo/PROJ/issues/1982 where EPSG:4656
                // is attached to a GeographicCRS whereas it is a ProjectedCRS
                if (tmpAuthFactory->createCoordinateReferenceSystem(code)
                        ->_isEquivalentTo(
                            crs.get(),
                            util::IComparable::Criterion::
                                EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS)) {
                    ids.emplace_back(authName, code);
                } else {
                    // TODO? log this inconsistency
                }
            } catch (const std::exception &) {
                // TODO? log this inconsistency
            }
        }
    }
    if (ids.empty() && !ci_equal(crs->nameStr(), "unknown")) {
        std::vector<io::AuthorityFactory::ObjectType> allowedObjects;
        auto geogCRS = dynamic_cast<const crs::GeographicCRS *>(crs.get());
        if (geogCRS) {
            if (geogCRS->datumNonNull(authFactory->databaseContext())
                    ->nameStr() == "unknown") {
                return;
            }
            allowedObjects.push_back(
                geogCRS->coordinateSystem()->axisList().size() == 2
                    ? io::AuthorityFactory::ObjectType::GEOGRAPHIC_2D_CRS
                    : io::AuthorityFactory::ObjectType::GEOGRAPHIC_3D_CRS);
        } else if (dynamic_cast<crs::ProjectedCRS *>(crs.get())) {
            allowedObjects.push_back(
                io::AuthorityFactory::ObjectType::PROJECTED_CRS);
        } else if (dynamic_cast<crs::VerticalCRS *>(crs.get())) {
            allowedObjects.push_back(
                io::AuthorityFactory::ObjectType::VERTICAL_CRS);
        }
        if (!allowedObjects.empty()) {

            const std::pair<io::AuthorityFactory::ObjectType, std::string> key(
                allowedObjects[0], crs->nameStr());
            auto iter = context.cacheNameToCRS.find(key);
            if (iter != context.cacheNameToCRS.end()) {
                ids = iter->second;
                return;
            }

            const auto &authFactoryName = authFactory->getAuthority();
            try {
                const auto tmpAuthFactory = io::AuthorityFactory::create(
                    authFactory->databaseContext(),
                    (authFactoryName.empty() || authFactoryName == "any")
                        ? std::string()
                        : authFactoryName);

                auto matches = tmpAuthFactory->createObjectsFromName(
                    crs->nameStr(), allowedObjects, false, 2);
                if (matches.size() == 1 &&
                    crs->_isEquivalentTo(
                        matches.front().get(),
                        util::IComparable::Criterion::EQUIVALENT) &&
                    !matches.front()->identifiers().empty()) {
                    const auto &tmpIds = matches.front()->identifiers();
                    ids.emplace_back(*(tmpIds[0]->codeSpace()),
                                     tmpIds[0]->code());
                }
            } catch (const std::exception &) {
            }
            context.cacheNameToCRS[key] = ids;
        }
    }
}

// ---------------------------------------------------------------------------

static std::vector<std::string>
getCandidateAuthorities(const io::AuthorityFactoryPtr &authFactory,
                        const std::string &srcAuthName,
                        const std::string &targetAuthName) {
    const auto &authFactoryName = authFactory->getAuthority();
    std::vector<std::string> authorities;
    if (authFactoryName.empty()) {
        for (const std::string &authName :
             authFactory->databaseContext()->getAllowedAuthorities(
                 srcAuthName, targetAuthName)) {
            authorities.emplace_back(authName == "any" ? std::string()
                                                       : authName);
        }
        if (authorities.empty()) {
            authorities.emplace_back();
        }
    } else {
        authorities.emplace_back(authFactoryName == "any" ? std::string()
                                                          : authFactoryName);
    }
    return authorities;
}

// ---------------------------------------------------------------------------

// Look in the authority registry for operations from sourceCRS to targetCRS
std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::Private::findOpsInRegistryDirect(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    Private::Context &context, bool &resNonEmptyBeforeFiltering) {
    const auto &authFactory = context.context->getAuthorityFactory();
    assert(authFactory);

    if ((sourceCRS->identifiers().empty() &&
         ci_equal(sourceCRS->nameStr(), "unknown")) ||
        (targetCRS->identifiers().empty() &&
         ci_equal(targetCRS->nameStr(), "unknown"))) {
        return {};
    }

#ifdef TRACE_CREATE_OPERATIONS
    ENTER_BLOCK("findOpsInRegistryDirect(" + objectAsStr(sourceCRS.get()) +
                " --> " + objectAsStr(targetCRS.get()) + ")");
#endif

    resNonEmptyBeforeFiltering = false;
    std::list<std::pair<std::string, std::string>> sourceIds;
    std::list<std::pair<std::string, std::string>> targetIds;
    buildCRSIds(sourceCRS, context, sourceIds);
    buildCRSIds(targetCRS, context, targetIds);

    const auto gridAvailabilityUse = context.context->getGridAvailabilityUse();
    for (const auto &idSrc : sourceIds) {
        const auto &srcAuthName = idSrc.first;
        const auto &srcCode = idSrc.second;
        for (const auto &idTarget : targetIds) {
            const auto &targetAuthName = idTarget.first;
            const auto &targetCode = idTarget.second;

            const auto authorities(getCandidateAuthorities(
                authFactory, srcAuthName, targetAuthName));
            std::vector<CoordinateOperationNNPtr> res;
            for (const auto &authName : authorities) {
                const auto tmpAuthFactory = io::AuthorityFactory::create(
                    authFactory->databaseContext(), authName);
                auto resTmp =
                    tmpAuthFactory->createFromCoordinateReferenceSystemCodes(
                        srcAuthName, srcCode, targetAuthName, targetCode,
                        context.context->getUsePROJAlternativeGridNames(),
                        gridAvailabilityUse ==
                                CoordinateOperationContext::
                                    GridAvailabilityUse::
                                        DISCARD_OPERATION_IF_MISSING_GRID ||
                            gridAvailabilityUse ==
                                CoordinateOperationContext::
                                    GridAvailabilityUse::KNOWN_AVAILABLE,
                        gridAvailabilityUse ==
                            CoordinateOperationContext::GridAvailabilityUse::
                                KNOWN_AVAILABLE,
                        context.context->getDiscardSuperseded(), true, false,
                        context.extent1, context.extent2);
                res.insert(res.end(), resTmp.begin(), resTmp.end());
                if (authName == "PROJ") {
                    // Do not stop at the first transformations available in
                    // the PROJ namespace, but allow the next authority to
                    // continue
                    continue;
                }
                if (!res.empty()) {
                    resNonEmptyBeforeFiltering = true;
                    auto resFiltered =
                        FilterResults(res, context.context, context.extent1,
                                      context.extent2, false)
                            .getRes();
#ifdef TRACE_CREATE_OPERATIONS
                    logTrace("filtering reduced from " +
                             toString(static_cast<int>(res.size())) + " to " +
                             toString(static_cast<int>(resFiltered.size())));
#endif
                    return resFiltered;
                }
            }
        }
    }
    return std::vector<CoordinateOperationNNPtr>();
}

// ---------------------------------------------------------------------------

// Look in the authority registry for operations to targetCRS
std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::Private::findOpsInRegistryDirectTo(
    const crs::CRSNNPtr &targetCRS, Private::Context &context) {
#ifdef TRACE_CREATE_OPERATIONS
    ENTER_BLOCK("findOpsInRegistryDirectTo({any} -->" +
                objectAsStr(targetCRS.get()) + ")");
#endif

    const auto &authFactory = context.context->getAuthorityFactory();
    assert(authFactory);

    std::list<std::pair<std::string, std::string>> ids;
    buildCRSIds(targetCRS, context, ids);

    const auto gridAvailabilityUse = context.context->getGridAvailabilityUse();
    for (const auto &id : ids) {
        const auto &targetAuthName = id.first;
        const auto &targetCode = id.second;

        const auto authorities(getCandidateAuthorities(
            authFactory, targetAuthName, targetAuthName));
        std::vector<CoordinateOperationNNPtr> res;
        for (const auto &authName : authorities) {
            const auto tmpAuthFactory = io::AuthorityFactory::create(
                authFactory->databaseContext(), authName);
            auto resTmp =
                tmpAuthFactory->createFromCoordinateReferenceSystemCodes(
                    std::string(), std::string(), targetAuthName, targetCode,
                    context.context->getUsePROJAlternativeGridNames(),

                    gridAvailabilityUse ==
                            CoordinateOperationContext::GridAvailabilityUse::
                                DISCARD_OPERATION_IF_MISSING_GRID ||
                        gridAvailabilityUse ==
                            CoordinateOperationContext::GridAvailabilityUse::
                                KNOWN_AVAILABLE,
                    gridAvailabilityUse ==
                        CoordinateOperationContext::GridAvailabilityUse::
                            KNOWN_AVAILABLE,
                    context.context->getDiscardSuperseded(), true, true,
                    context.extent1, context.extent2);
            res.insert(res.end(), resTmp.begin(), resTmp.end());
            if (authName == "PROJ") {
                // Do not stop at the first transformations available in
                // the PROJ namespace, but allow the next authority to continue
                continue;
            }
            if (!res.empty()) {
                auto resFiltered =
                    FilterResults(res, context.context, context.extent1,
                                  context.extent2, false)
                        .getRes();
#ifdef TRACE_CREATE_OPERATIONS
                logTrace("filtering reduced from " +
                         toString(static_cast<int>(res.size())) + " to " +
                         toString(static_cast<int>(resFiltered.size())));
#endif
                return resFiltered;
            }
        }
    }
    return std::vector<CoordinateOperationNNPtr>();
}

//! @endcond

// ---------------------------------------------------------------------------

static std::list<crs::GeodeticCRSNNPtr>
findCandidateGeodCRSForDatum(const io::AuthorityFactoryPtr &authFactory,
                             const crs::GeodeticCRS *crs,
                             const datum::GeodeticReferenceFrameNNPtr &datum) {
    std::string preferredAuthName;
    const auto &crsIds = crs->identifiers();
    if (crsIds.size() == 1)
        preferredAuthName = *(crsIds.front()->codeSpace());
    return authFactory->createGeodeticCRSFromDatum(datum, preferredAuthName,
                                                   std::string());
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static bool
isSameGeodeticDatum(const datum::GeodeticReferenceFrameNNPtr &datum1,
                    const datum::GeodeticReferenceFrameNNPtr &datum2,
                    const io::DatabaseContextPtr &dbContext) {
    if (datum1->nameStr() == "unknown" && datum2->nameStr() != "unknown")
        return false;
    if (datum2->nameStr() == "unknown" && datum1->nameStr() != "unknown")
        return false;
    return datum1->_isEquivalentTo(
        datum2.get(), util::IComparable::Criterion::EQUIVALENT, dbContext);
}

// ---------------------------------------------------------------------------

// Look in the authority registry for operations from sourceCRS to targetCRS
// using an intermediate pivot
std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::Private::findsOpsInRegistryWithIntermediate(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    Private::Context &context,
    bool useCreateBetweenGeodeticCRSWithDatumBasedIntermediates) {

#ifdef TRACE_CREATE_OPERATIONS
    ENTER_BLOCK("findsOpsInRegistryWithIntermediate(" +
                objectAsStr(sourceCRS.get()) + " --> " +
                objectAsStr(targetCRS.get()) + ")");
#endif
    const auto &authFactory = context.context->getAuthorityFactory();
    assert(authFactory);

    const auto dbContext = authFactory->databaseContext().as_nullable();
    const auto geodSrc = dynamic_cast<crs::GeodeticCRS *>(sourceCRS.get());
    const auto datumSrc =
        geodSrc ? geodSrc->datumNonNull(dbContext).as_nullable() : nullptr;
    // Optimization: check if the source/target CRS have no chance to match
    // a database entry
    if (datumSrc && datumSrc->identifiers().empty() &&
        (ci_equal(datumSrc->nameStr(), "unknown") ||
         ci_starts_with(datumSrc->nameStr(), UNKNOWN_BASED_ON)))
        return {};
    if (!geodSrc && dynamic_cast<crs::BoundCRS *>(sourceCRS.get()))
        return {};
    const auto geodDst = dynamic_cast<crs::GeodeticCRS *>(targetCRS.get());
    const auto datumDst =
        geodDst ? geodDst->datumNonNull(dbContext).as_nullable() : nullptr;
    if (datumDst && datumDst->identifiers().empty() &&
        (ci_equal(datumDst->nameStr(), "unknown") ||
         ci_starts_with(datumDst->nameStr(), UNKNOWN_BASED_ON)))
        return {};
    if (!geodDst && dynamic_cast<crs::BoundCRS *>(targetCRS.get()))
        return {};

    std::list<std::pair<std::string, std::string>> sourceIds;
    buildCRSIds(sourceCRS, context, sourceIds);
    if (sourceIds.empty()) {
        if (geodSrc) {
            const std::string originatingAuthSrc =
                geodSrc->getOriginatingAuthName();
            const auto candidatesSrcGeod(findCandidateGeodCRSForDatum(
                authFactory, geodSrc, NN_NO_CHECK(datumSrc)));
            std::vector<CoordinateOperationNNPtr> res;
            for (const auto &candidateSrcGeod : candidatesSrcGeod) {
                // Restrict to using only objects that have the same authority
                // as geodSrc.
                const auto &candidateIds = candidateSrcGeod->identifiers();
                if ((originatingAuthSrc.empty() ||
                     (candidateIds.size() == 1 &&
                      *(candidateIds[0]->codeSpace()) == originatingAuthSrc) ||
                     candidateIds.size() > 1) &&
                    candidateSrcGeod->coordinateSystem()->axisList().size() ==
                        geodSrc->coordinateSystem()->axisList().size() &&
                    ((dynamic_cast<crs::GeographicCRS *>(sourceCRS.get()) !=
                      nullptr) ==
                     (dynamic_cast<crs::GeographicCRS *>(
                          candidateSrcGeod.get()) != nullptr))) {
                    if (geodDst) {
                        const auto candidateSrcDatum =
                            candidateSrcGeod->datumNonNull(dbContext);
                        const bool sameGeodeticDatum = isSameGeodeticDatum(
                            candidateSrcDatum, NN_NO_CHECK(datumDst),
                            dbContext);
                        if (sameGeodeticDatum) {
                            continue;
                        }
                    }

                    const auto opsWithIntermediate =
                        findsOpsInRegistryWithIntermediate(
                            candidateSrcGeod, targetCRS, context,
                            useCreateBetweenGeodeticCRSWithDatumBasedIntermediates);
                    if (!opsWithIntermediate.empty()) {
                        const auto opsFirst = createOperations(
                            sourceCRS, util::optional<common::DataEpoch>(),
                            candidateSrcGeod,
                            util::optional<common::DataEpoch>(), context);
                        for (const auto &opFirst : opsFirst) {
                            for (const auto &opSecond : opsWithIntermediate) {
                                try {
                                    res.emplace_back(
                                        ConcatenatedOperation::
                                            createComputeMetadata(
                                                {opFirst, opSecond},
                                                disallowEmptyIntersection));
                                } catch (
                                    const InvalidOperationEmptyIntersection &) {
                                }
                            }
                        }
                        if (!res.empty())
                            return res;
                    }
                }
            }
        }
        return std::vector<CoordinateOperationNNPtr>();
    }

    std::list<std::pair<std::string, std::string>> targetIds;
    buildCRSIds(targetCRS, context, targetIds);
    if (targetIds.empty()) {
        return applyInverse(findsOpsInRegistryWithIntermediate(
            targetCRS, sourceCRS, context,
            useCreateBetweenGeodeticCRSWithDatumBasedIntermediates));
    }

    const auto gridAvailabilityUse = context.context->getGridAvailabilityUse();
    for (const auto &idSrc : sourceIds) {
        const auto &srcAuthName = idSrc.first;
        const auto &srcCode = idSrc.second;
        for (const auto &idTarget : targetIds) {
            const auto &targetAuthName = idTarget.first;
            const auto &targetCode = idTarget.second;

            const auto authorities(getCandidateAuthorities(
                authFactory, srcAuthName, targetAuthName));
            assert(!authorities.empty());

            const auto tmpAuthFactory = io::AuthorityFactory::create(
                authFactory->databaseContext(),
                (authFactory->getAuthority() == "any" || authorities.size() > 1)
                    ? std::string()
                    : authorities.front());

            std::vector<CoordinateOperationNNPtr> res;
            if (useCreateBetweenGeodeticCRSWithDatumBasedIntermediates) {
                res =
                    tmpAuthFactory
                        ->createBetweenGeodeticCRSWithDatumBasedIntermediates(
                            sourceCRS, srcAuthName, srcCode, targetCRS,
                            targetAuthName, targetCode,
                            context.context->getUsePROJAlternativeGridNames(),
                            gridAvailabilityUse ==
                                    CoordinateOperationContext::
                                        GridAvailabilityUse::
                                            DISCARD_OPERATION_IF_MISSING_GRID ||
                                gridAvailabilityUse ==
                                    CoordinateOperationContext::
                                        GridAvailabilityUse::KNOWN_AVAILABLE,
                            gridAvailabilityUse ==
                                CoordinateOperationContext::
                                    GridAvailabilityUse::KNOWN_AVAILABLE,
                            context.context->getDiscardSuperseded(),
                            authFactory->getAuthority() != "any" &&
                                    authorities.size() > 1
                                ? authorities
                                : std::vector<std::string>(),
                            context.extent1, context.extent2);
            } else {
                io::AuthorityFactory::ObjectType intermediateObjectType =
                    io::AuthorityFactory::ObjectType::CRS;

                // If doing GeogCRS --> GeogCRS, only use GeogCRS as
                // intermediate CRS
                // Avoid weird behavior when doing NAD83 -> NAD83(2011)
                // that would go through NAVD88 otherwise.
                if (context.context->getIntermediateCRS().empty() &&
                    dynamic_cast<const crs::GeographicCRS *>(sourceCRS.get()) &&
                    dynamic_cast<const crs::GeographicCRS *>(targetCRS.get())) {
                    intermediateObjectType =
                        io::AuthorityFactory::ObjectType::GEOGRAPHIC_CRS;
                }
                res = tmpAuthFactory->createFromCRSCodesWithIntermediates(
                    srcAuthName, srcCode, targetAuthName, targetCode,
                    context.context->getUsePROJAlternativeGridNames(),
                    gridAvailabilityUse ==
                            CoordinateOperationContext::GridAvailabilityUse::
                                DISCARD_OPERATION_IF_MISSING_GRID ||
                        gridAvailabilityUse ==
                            CoordinateOperationContext::GridAvailabilityUse::
                                KNOWN_AVAILABLE,
                    gridAvailabilityUse ==
                        CoordinateOperationContext::GridAvailabilityUse::
                            KNOWN_AVAILABLE,
                    context.context->getDiscardSuperseded(),
                    context.context->getIntermediateCRS(),
                    intermediateObjectType,
                    authFactory->getAuthority() != "any" &&
                            authorities.size() > 1
                        ? authorities
                        : std::vector<std::string>(),
                    context.extent1, context.extent2);
            }
            if (!res.empty()) {

                auto resFiltered =
                    FilterResults(res, context.context, context.extent1,
                                  context.extent2, false)
                        .getRes();
#ifdef TRACE_CREATE_OPERATIONS
                logTrace("filtering reduced from " +
                         toString(static_cast<int>(res.size())) + " to " +
                         toString(static_cast<int>(resFiltered.size())));
#endif
                return resFiltered;
            }
        }
    }
    return std::vector<CoordinateOperationNNPtr>();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static TransformationNNPtr createBallparkGeographicOffset(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const io::DatabaseContextPtr &dbContext, bool forceBallpark) {

    const crs::GeographicCRS *geogSrc =
        dynamic_cast<const crs::GeographicCRS *>(sourceCRS.get());
    const crs::GeographicCRS *geogDst =
        dynamic_cast<const crs::GeographicCRS *>(targetCRS.get());
    const bool isSameDatum =
        !forceBallpark && geogSrc && geogDst &&
        isSameGeodeticDatum(geogSrc->datumNonNull(dbContext),
                            geogDst->datumNonNull(dbContext), dbContext);

    auto name = buildOpName(isSameDatum ? NULL_GEOGRAPHIC_OFFSET
                                        : BALLPARK_GEOGRAPHIC_OFFSET,
                            sourceCRS, targetCRS);

    const auto &sourceCRSExtent = getExtent(sourceCRS);
    const auto &targetCRSExtent = getExtent(targetCRS);
    const bool sameExtent =
        sourceCRSExtent && targetCRSExtent &&
        sourceCRSExtent->_isEquivalentTo(
            targetCRSExtent.get(), util::IComparable::Criterion::EQUIVALENT);

    util::PropertyMap map;
    map.set(common::IdentifiedObject::NAME_KEY, name)
        .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
             sameExtent ? NN_NO_CHECK(sourceCRSExtent)
                        : metadata::Extent::WORLD);
    const common::Angle angle0(0);

    std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
    if (isSameDatum) {
        accuracies.emplace_back(metadata::PositionalAccuracy::create("0"));
    }

    const auto singleSourceCRS =
        dynamic_cast<const crs::SingleCRS *>(sourceCRS.get());
    const auto singleTargetCRS =
        dynamic_cast<const crs::SingleCRS *>(targetCRS.get());
    if ((singleSourceCRS &&
         singleSourceCRS->coordinateSystem()->axisList().size() == 3) ||
        (singleTargetCRS &&
         singleTargetCRS->coordinateSystem()->axisList().size() == 3)) {
        return Transformation::createGeographic3DOffsets(
            map, sourceCRS, targetCRS, angle0, angle0, common::Length(0),
            accuracies);
    } else {
        return Transformation::createGeographic2DOffsets(
            map, sourceCRS, targetCRS, angle0, angle0, accuracies);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

struct MyPROJStringExportableGeodToGeod final
    : public io::IPROJStringExportable {
    crs::GeodeticCRSPtr geodSrc{};
    crs::GeodeticCRSPtr geodDst{};

    MyPROJStringExportableGeodToGeod(const crs::GeodeticCRSPtr &geodSrcIn,
                                     const crs::GeodeticCRSPtr &geodDstIn)
        : geodSrc(geodSrcIn), geodDst(geodDstIn) {}

    ~MyPROJStringExportableGeodToGeod() override;

    void
    // cppcheck-suppress functionStatic
    _exportToPROJString(io::PROJStringFormatter *formatter) const override {

        formatter->startInversion();
        geodSrc->_exportToPROJString(formatter);
        formatter->stopInversion();
        geodDst->_exportToPROJString(formatter);
    }
};

MyPROJStringExportableGeodToGeod::~MyPROJStringExportableGeodToGeod() = default;

// ---------------------------------------------------------------------------

struct MyPROJStringExportableHorizVertical final
    : public io::IPROJStringExportable {
    CoordinateOperationPtr horizTransform{};
    CoordinateOperationPtr verticalTransform{};
    crs::GeographicCRSPtr geogDst{};

    MyPROJStringExportableHorizVertical(
        const CoordinateOperationPtr &horizTransformIn,
        const CoordinateOperationPtr &verticalTransformIn,
        const crs::GeographicCRSPtr &geogDstIn)
        : horizTransform(horizTransformIn),
          verticalTransform(verticalTransformIn), geogDst(geogDstIn) {}

    ~MyPROJStringExportableHorizVertical() override;

    void
    // cppcheck-suppress functionStatic
    _exportToPROJString(io::PROJStringFormatter *formatter) const override {

        horizTransform->_exportToPROJString(formatter);

        formatter->startInversion();
        geogDst->addAngularUnitConvertAndAxisSwap(formatter);
        formatter->stopInversion();

        formatter->pushOmitHorizontalConversionInVertTransformation();
        verticalTransform->_exportToPROJString(formatter);
        formatter->popOmitHorizontalConversionInVertTransformation();

        formatter->pushOmitZUnitConversion();
        geogDst->addAngularUnitConvertAndAxisSwap(formatter);
        formatter->popOmitZUnitConversion();
    }
};

MyPROJStringExportableHorizVertical::~MyPROJStringExportableHorizVertical() =
    default;

// ---------------------------------------------------------------------------

struct MyPROJStringExportableHorizVerticalHorizPROJBased final
    : public io::IPROJStringExportable {
    CoordinateOperationPtr opSrcCRSToGeogCRS{};
    CoordinateOperationPtr verticalTransform{};
    CoordinateOperationPtr opGeogCRStoDstCRS{};
    crs::SingleCRSPtr interpolationCRS{};

    MyPROJStringExportableHorizVerticalHorizPROJBased(
        const CoordinateOperationPtr &opSrcCRSToGeogCRSIn,
        const CoordinateOperationPtr &verticalTransformIn,
        const CoordinateOperationPtr &opGeogCRStoDstCRSIn,
        const crs::SingleCRSPtr &interpolationCRSIn)
        : opSrcCRSToGeogCRS(opSrcCRSToGeogCRSIn),
          verticalTransform(verticalTransformIn),
          opGeogCRStoDstCRS(opGeogCRStoDstCRSIn),
          interpolationCRS(interpolationCRSIn) {}

    ~MyPROJStringExportableHorizVerticalHorizPROJBased() override;

    void
    // cppcheck-suppress functionStatic
    _exportToPROJString(io::PROJStringFormatter *formatter) const override {

        bool saveHorizontalCoords = false;
        const auto transf =
            dynamic_cast<Transformation *>(opSrcCRSToGeogCRS.get());
        if (transf && opSrcCRSToGeogCRS->sourceCRS()->_isEquivalentTo(
                          opGeogCRStoDstCRS->targetCRS()
                              ->demoteTo2D(std::string(), nullptr)
                              .get(),
                          util::IComparable::Criterion::EQUIVALENT)) {
            int methodEPSGCode = transf->method()->getEPSGCode();
            if (methodEPSGCode == 0) {
                // If the transformation is actually an inverse transformation,
                // we will not get the EPSG code. So get the forward
                // transformation.
                const auto invTrans = transf->inverse();
                const auto invTransAsTrans =
                    dynamic_cast<Transformation *>(invTrans.get());
                if (invTransAsTrans)
                    methodEPSGCode = invTransAsTrans->method()->getEPSGCode();
            }

            const bool bGeocentricTranslation =
                methodEPSGCode ==
                    EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOCENTRIC ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_2D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_3D;
            if ((bGeocentricTranslation &&
                 !(transf->parameterValueNumericAsSI(
                       EPSG_CODE_PARAMETER_X_AXIS_TRANSLATION) == 0 &&
                   transf->parameterValueNumericAsSI(
                       EPSG_CODE_PARAMETER_Y_AXIS_TRANSLATION) == 0 &&
                   transf->parameterValueNumericAsSI(
                       EPSG_CODE_PARAMETER_Z_AXIS_TRANSLATION) == 0)) ||

                methodEPSGCode ==
                    EPSG_CODE_METHOD_COORDINATE_FRAME_GEOCENTRIC ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOCENTRIC ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_COORDINATE_FRAME_GEOGRAPHIC_2D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOGRAPHIC_2D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_COORDINATE_FRAME_GEOGRAPHIC_3D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_COORDINATE_FRAME_GEOG3D_TO_COMPOUND ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOGRAPHIC_3D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOCENTRIC ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_2D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_3D ||
                methodEPSGCode == EPSG_CODE_METHOD_POSITION_VECTOR_GEOCENTRIC ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_POSITION_VECTOR_GEOGRAPHIC_2D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_POSITION_VECTOR_GEOGRAPHIC_3D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOCENTRIC ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_2D ||
                methodEPSGCode ==
                    EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_3D) {
                saveHorizontalCoords = true;
            }
        }

        if (saveHorizontalCoords) {
            formatter->addStep("push");
            formatter->addParam("v_1");
            formatter->addParam("v_2");
        }

        auto interpolationCRSGeog =
            dynamic_cast<const crs::GeographicCRS *>(interpolationCRS.get());
        auto interpolationCRSProj =
            dynamic_cast<const crs::ProjectedCRS *>(interpolationCRS.get());

        formatter->pushOmitZUnitConversion();

        opSrcCRSToGeogCRS->_exportToPROJString(formatter);

        formatter->startInversion();
        if (interpolationCRSGeog)
            interpolationCRSGeog->addAngularUnitConvertAndAxisSwap(formatter);
        else if (interpolationCRSProj)
            interpolationCRSProj->addUnitConvertAndAxisSwap(formatter, false);
        formatter->stopInversion();

        formatter->popOmitZUnitConversion();

        formatter->pushOmitHorizontalConversionInVertTransformation();
        verticalTransform->_exportToPROJString(formatter);
        formatter->popOmitHorizontalConversionInVertTransformation();

        formatter->pushOmitZUnitConversion();

        if (interpolationCRSGeog)
            interpolationCRSGeog->addAngularUnitConvertAndAxisSwap(formatter);
        else if (interpolationCRSProj)
            interpolationCRSProj->addUnitConvertAndAxisSwap(formatter, false);

        opGeogCRStoDstCRS->_exportToPROJString(formatter);

        formatter->popOmitZUnitConversion();

        if (saveHorizontalCoords) {
            formatter->addStep("pop");
            formatter->addParam("v_1");
            formatter->addParam("v_2");
        }
    }
};

MyPROJStringExportableHorizVerticalHorizPROJBased::
    ~MyPROJStringExportableHorizVerticalHorizPROJBased() = default;

// ---------------------------------------------------------------------------

struct MyPROJStringExportableHorizNullVertical final
    : public io::IPROJStringExportable {
    CoordinateOperationPtr horizTransform{};

    explicit MyPROJStringExportableHorizNullVertical(
        const CoordinateOperationPtr &horizTransformIn)
        : horizTransform(horizTransformIn) {}

    ~MyPROJStringExportableHorizNullVertical() override;

    void
    // cppcheck-suppress functionStatic
    _exportToPROJString(io::PROJStringFormatter *formatter) const override {

        horizTransform->_exportToPROJString(formatter);
    }
};

MyPROJStringExportableHorizNullVertical::
    ~MyPROJStringExportableHorizNullVertical() = default;

//! @endcond

} // namespace operation
NS_PROJ_END

#if 0
namespace dropbox{ namespace oxygen {
template<> nn<std::shared_ptr<NS_PROJ::operation::MyPROJStringExportableGeodToGeod>>::~nn() = default;
template<> nn<std::shared_ptr<NS_PROJ::operation::MyPROJStringExportableHorizVertical>>::~nn() = default;
template<> nn<std::shared_ptr<NS_PROJ::operation::MyPROJStringExportableHorizVerticalHorizPROJBased>>::~nn() = default;
template<> nn<std::shared_ptr<NS_PROJ::operation::MyPROJStringExportableHorizNullVertical>>::~nn() = default;
}}
#endif

NS_PROJ_START
namespace operation {

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

static std::string buildTransfName(const std::string &srcName,
                                   const std::string &targetName) {
    std::string name("Transformation from ");
    name += srcName;
    name += " to ";
    name += targetName;
    return name;
}

// ---------------------------------------------------------------------------

static std::string buildConvName(const std::string &srcName,
                                 const std::string &targetName) {
    std::string name("Conversion from ");
    name += srcName;
    name += " to ";
    name += targetName;
    return name;
}

// ---------------------------------------------------------------------------

static SingleOperationNNPtr createPROJBased(
    const util::PropertyMap &properties,
    const io::IPROJStringExportableNNPtr &projExportable,
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const crs::CRSPtr &interpolationCRS,
    const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies,
    bool hasBallparkTransformation) {
    return util::nn_static_pointer_cast<SingleOperation>(
        PROJBasedOperation::create(properties, projExportable, false, sourceCRS,
                                   targetCRS, interpolationCRS, accuracies,
                                   hasBallparkTransformation));
}

// ---------------------------------------------------------------------------

static CoordinateOperationNNPtr
createGeodToGeodPROJBased(const crs::CRSNNPtr &geodSrc,
                          const crs::CRSNNPtr &geodDst) {

    auto exportable = util::nn_make_shared<MyPROJStringExportableGeodToGeod>(
        util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(geodSrc),
        util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(geodDst));

    auto properties =
        util::PropertyMap()
            .set(common::IdentifiedObject::NAME_KEY,
                 buildTransfName(geodSrc->nameStr(), geodDst->nameStr()))
            .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                 metadata::Extent::WORLD);
    return createPROJBased(properties, exportable, geodSrc, geodDst, nullptr,
                           {}, false);
}

// ---------------------------------------------------------------------------

static std::string
getRemarks(const std::vector<operation::CoordinateOperationNNPtr> &ops) {
    std::string remarks;
    for (const auto &op : ops) {
        const auto &opRemarks = op->remarks();
        if (!opRemarks.empty()) {
            if (!remarks.empty()) {
                remarks += '\n';
            }

            std::string opName(op->nameStr());
            if (starts_with(opName, INVERSE_OF)) {
                opName = opName.substr(INVERSE_OF.size());
            }

            remarks += "For ";
            remarks += opName;

            const auto &ids = op->identifiers();
            if (!ids.empty()) {
                std::string authority(*ids.front()->codeSpace());
                if (starts_with(authority, "INVERSE(") &&
                    authority.back() == ')') {
                    authority = authority.substr(strlen("INVERSE("),
                                                 authority.size() - 1 -
                                                     strlen("INVERSE("));
                }
                if (starts_with(authority, "DERIVED_FROM(") &&
                    authority.back() == ')') {
                    authority = authority.substr(strlen("DERIVED_FROM("),
                                                 authority.size() - 1 -
                                                     strlen("DERIVED_FROM("));
                }

                remarks += " (";
                remarks += authority;
                remarks += ':';
                remarks += ids.front()->code();
                remarks += ')';
            }
            remarks += ": ";
            remarks += opRemarks;
        }
    }
    return remarks;
}

// ---------------------------------------------------------------------------

static CoordinateOperationNNPtr createHorizVerticalPROJBased(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const operation::CoordinateOperationNNPtr &horizTransform,
    const operation::CoordinateOperationNNPtr &verticalTransform,
    bool checkExtent) {

    auto geogDst = util::nn_dynamic_pointer_cast<crs::GeographicCRS>(targetCRS);
    assert(geogDst);

    auto exportable = util::nn_make_shared<MyPROJStringExportableHorizVertical>(
        horizTransform, verticalTransform, geogDst);

    const bool horizTransformIsNoOp =
        starts_with(horizTransform->nameStr(), NULL_GEOGRAPHIC_OFFSET) &&
        horizTransform->nameStr().find(" + ") == std::string::npos;
    if (horizTransformIsNoOp) {
        auto properties = util::PropertyMap();
        properties.set(common::IdentifiedObject::NAME_KEY,
                       verticalTransform->nameStr());
        bool dummy = false;
        auto extent = getExtent(verticalTransform, true, dummy);
        if (extent) {
            properties.set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                           NN_NO_CHECK(extent));
        }
        const auto &remarks = verticalTransform->remarks();
        if (!remarks.empty()) {
            properties.set(common::IdentifiedObject::REMARKS_KEY, remarks);
        }
        auto accuracies = verticalTransform->coordinateOperationAccuracies();
        if (accuracies.empty() &&
            dynamic_cast<const Conversion *>(verticalTransform.get())) {
            accuracies.emplace_back(metadata::PositionalAccuracy::create("0"));
        }
        return createPROJBased(properties, exportable, sourceCRS, targetCRS,
                               nullptr, accuracies,
                               verticalTransform->hasBallparkTransformation());
    } else {
        bool emptyIntersection = false;
        auto ops = std::vector<CoordinateOperationNNPtr>{horizTransform,
                                                         verticalTransform};
        auto extent = getExtent(ops, true, emptyIntersection);
        if (checkExtent && emptyIntersection) {
            std::string msg(
                "empty intersection of area of validity of concatenated "
                "operations");
            throw InvalidOperationEmptyIntersection(msg);
        }
        auto properties = util::PropertyMap();
        properties.set(common::IdentifiedObject::NAME_KEY,
                       computeConcatenatedName(ops));

        if (extent) {
            properties.set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                           NN_NO_CHECK(extent));
        }

        const auto remarks = getRemarks(ops);
        if (!remarks.empty()) {
            properties.set(common::IdentifiedObject::REMARKS_KEY, remarks);
        }

        std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
        const double accuracy = getAccuracy(ops);
        if (accuracy >= 0.0) {
            accuracies.emplace_back(
                metadata::PositionalAccuracy::create(toString(accuracy)));
        }

        return createPROJBased(
            properties, exportable, sourceCRS, targetCRS, nullptr, accuracies,
            horizTransform->hasBallparkTransformation() ||
                verticalTransform->hasBallparkTransformation());
    }
}

// ---------------------------------------------------------------------------

static CoordinateOperationNNPtr createHorizVerticalHorizPROJBased(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const operation::CoordinateOperationNNPtr &opSrcCRSToGeogCRS,
    const operation::CoordinateOperationNNPtr &verticalTransform,
    const operation::CoordinateOperationNNPtr &opGeogCRStoDstCRS,
    const crs::SingleCRSPtr &interpolationCRS, bool checkExtent) {

    auto exportable =
        util::nn_make_shared<MyPROJStringExportableHorizVerticalHorizPROJBased>(
            opSrcCRSToGeogCRS, verticalTransform, opGeogCRStoDstCRS,
            interpolationCRS);

    std::vector<CoordinateOperationNNPtr> ops;
    if (!(starts_with(opSrcCRSToGeogCRS->nameStr(), NULL_GEOGRAPHIC_OFFSET) &&
          opSrcCRSToGeogCRS->nameStr().find(" + ") == std::string::npos)) {
        ops.emplace_back(opSrcCRSToGeogCRS);
    }
    ops.emplace_back(verticalTransform);
    if (!(starts_with(opGeogCRStoDstCRS->nameStr(), NULL_GEOGRAPHIC_OFFSET) &&
          opGeogCRStoDstCRS->nameStr().find(" + ") == std::string::npos)) {
        ops.emplace_back(opGeogCRStoDstCRS);
    }

    std::vector<CoordinateOperationNNPtr> opsForRemarks;
    std::vector<CoordinateOperationNNPtr> opsForAccuracy;
    std::string opName;
    if (ops.size() == 3 && opGeogCRStoDstCRS->inverse()->_isEquivalentTo(
                               opSrcCRSToGeogCRS.get(),
                               util::IComparable::Criterion::EQUIVALENT)) {
        opsForRemarks.emplace_back(opSrcCRSToGeogCRS);
        opsForRemarks.emplace_back(verticalTransform);

        // Only taking into account the accuracy of the vertical transform when
        // opSrcCRSToGeogCRS and opGeogCRStoDstCRS are reversed and cancel
        // themselves would make sense. Unfortunately it causes
        // EPSG:4313+5710 (BD72 + Ostend height) to EPSG:9707
        // (WGS 84 + EGM96 height) to use a non-ideal pipeline.
        // opsForAccuracy.emplace_back(verticalTransform);
        opsForAccuracy = ops;

        opName = verticalTransform->nameStr() + " using ";
        if (!starts_with(opSrcCRSToGeogCRS->nameStr(), "Inverse of"))
            opName += opSrcCRSToGeogCRS->nameStr();
        else
            opName += opGeogCRStoDstCRS->nameStr();
    } else {
        opsForRemarks = ops;
        opsForAccuracy = ops;
        opName = computeConcatenatedName(ops);
    }

    bool hasBallparkTransformation = false;
    for (const auto &op : ops) {
        hasBallparkTransformation |= op->hasBallparkTransformation();
    }
    bool emptyIntersection = false;
    auto extent = getExtent(ops, false, emptyIntersection);
    if (checkExtent && emptyIntersection) {
        std::string msg(
            "empty intersection of area of validity of concatenated "
            "operations");
        throw InvalidOperationEmptyIntersection(msg);
    }
    auto properties = util::PropertyMap();
    properties.set(common::IdentifiedObject::NAME_KEY, opName);

    if (extent) {
        properties.set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                       NN_NO_CHECK(extent));
    }

    const auto remarks = getRemarks(opsForRemarks);
    if (!remarks.empty()) {
        properties.set(common::IdentifiedObject::REMARKS_KEY, remarks);
    }

    std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
    const double accuracy = getAccuracy(opsForAccuracy);
    if (accuracy >= 0.0) {
        accuracies.emplace_back(
            metadata::PositionalAccuracy::create(toString(accuracy)));
    }

    return createPROJBased(properties, exportable, sourceCRS, targetCRS,
                           nullptr, accuracies, hasBallparkTransformation);
}

// ---------------------------------------------------------------------------

static CoordinateOperationNNPtr createHorizNullVerticalPROJBased(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const operation::CoordinateOperationNNPtr &horizTransform,
    const operation::CoordinateOperationNNPtr &verticalTransform) {

    auto exportable =
        util::nn_make_shared<MyPROJStringExportableHorizNullVertical>(
            horizTransform);

    std::vector<CoordinateOperationNNPtr> ops = {horizTransform,
                                                 verticalTransform};
    const std::string opName = computeConcatenatedName(ops);

    auto properties = util::PropertyMap();
    properties.set(common::IdentifiedObject::NAME_KEY, opName);

    bool emptyIntersection = false;
    auto extent = getExtent(ops, false, emptyIntersection);
    if (extent) {
        properties.set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                       NN_NO_CHECK(extent));
    }

    const auto remarks = getRemarks(ops);
    if (!remarks.empty()) {
        properties.set(common::IdentifiedObject::REMARKS_KEY, remarks);
    }

    return createPROJBased(properties, exportable, sourceCRS, targetCRS,
                           nullptr, {}, /*hasBallparkTransformation=*/true);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::Private::createOperationsGeogToGeog(
    std::vector<CoordinateOperationNNPtr> &res, const crs::CRSNNPtr &sourceCRS,
    const crs::CRSNNPtr &targetCRS, Private::Context &context,
    const crs::GeographicCRS *geogSrc, const crs::GeographicCRS *geogDst,
    bool forceBallpark) {

    assert(sourceCRS.get() == geogSrc);
    assert(targetCRS.get() == geogDst);

    const auto &src_pm = geogSrc->primeMeridian()->longitude();
    const auto &dst_pm = geogDst->primeMeridian()->longitude();
    const common::Angle offset_pm(
        (src_pm.unit() == dst_pm.unit())
            ? common::Angle(src_pm.value() - dst_pm.value(), src_pm.unit())
            : common::Angle(
                  src_pm.convertToUnit(common::UnitOfMeasure::DEGREE) -
                      dst_pm.convertToUnit(common::UnitOfMeasure::DEGREE),
                  common::UnitOfMeasure::DEGREE));

    double vconvSrc = 1.0;
    const auto &srcCS = geogSrc->coordinateSystem();
    const auto &srcAxisList = srcCS->axisList();
    if (srcAxisList.size() == 3) {
        vconvSrc = srcAxisList[2]->unit().conversionToSI();
    }
    double vconvDst = 1.0;
    const auto &dstCS = geogDst->coordinateSystem();
    const auto &dstAxisList = dstCS->axisList();
    if (dstAxisList.size() == 3) {
        vconvDst = dstAxisList[2]->unit().conversionToSI();
    }

    std::string name(buildTransfName(geogSrc->nameStr(), geogDst->nameStr()));

    const auto &authFactory = context.context->getAuthorityFactory();
    const auto dbContext =
        authFactory ? authFactory->databaseContext().as_nullable() : nullptr;

    const bool sameDatum =
        !forceBallpark &&
        isSameGeodeticDatum(geogSrc->datumNonNull(dbContext),
                            geogDst->datumNonNull(dbContext), dbContext);

    // Do the CRS differ by their axis order ?
    bool axisReversal2D = false;
    bool axisReversal3D = false;
    if (!srcCS->_isEquivalentTo(dstCS.get(),
                                util::IComparable::Criterion::EQUIVALENT)) {
        auto srcOrder = srcCS->axisOrder();
        auto dstOrder = dstCS->axisOrder();
        if (((srcOrder == cs::EllipsoidalCS::AxisOrder::LAT_NORTH_LONG_EAST ||
              srcOrder == cs::EllipsoidalCS::AxisOrder::
                              LAT_NORTH_LONG_EAST_HEIGHT_UP) &&
             (dstOrder == cs::EllipsoidalCS::AxisOrder::LONG_EAST_LAT_NORTH ||
              dstOrder == cs::EllipsoidalCS::AxisOrder::
                              LONG_EAST_LAT_NORTH_HEIGHT_UP)) ||
            ((srcOrder == cs::EllipsoidalCS::AxisOrder::LONG_EAST_LAT_NORTH ||
              srcOrder == cs::EllipsoidalCS::AxisOrder::
                              LONG_EAST_LAT_NORTH_HEIGHT_UP) &&
             (dstOrder == cs::EllipsoidalCS::AxisOrder::LAT_NORTH_LONG_EAST ||
              dstOrder == cs::EllipsoidalCS::AxisOrder::
                              LAT_NORTH_LONG_EAST_HEIGHT_UP))) {
            if (srcAxisList.size() == 3 || dstAxisList.size() == 3)
                axisReversal3D = true;
            else
                axisReversal2D = true;
        }
    }

    // Do they differ by vertical units ?
    if (vconvSrc != vconvDst && geogSrc->ellipsoid()->_isEquivalentTo(
                                    geogDst->ellipsoid().get(),
                                    util::IComparable::Criterion::EQUIVALENT)) {
        if (offset_pm.value() == 0 && !axisReversal2D && !axisReversal3D) {
            // If only by vertical units, use a Change of Vertical
            // Unit transformation
            if (vconvDst == 0)
                throw InvalidOperation("Conversion factor of target unit is 0");
            const double factor = vconvSrc / vconvDst;
            auto conv = Conversion::createChangeVerticalUnit(
                util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                        name),
                common::Scale(factor));
            conv->setCRSs(sourceCRS, targetCRS, nullptr);
            conv->setHasBallparkTransformation(!sameDatum);
            res.push_back(conv);
            return res;
        } else {
            auto op = createGeodToGeodPROJBased(sourceCRS, targetCRS);
            op->setHasBallparkTransformation(!sameDatum);
            res.emplace_back(op);
            return res;
        }
    }

    // Do the CRS differ only by their axis order ?
    if (sameDatum && (axisReversal2D || axisReversal3D)) {
        auto conv = Conversion::createAxisOrderReversal(axisReversal3D);
        conv->setCRSs(sourceCRS, targetCRS, nullptr);
        res.emplace_back(conv);
        return res;
    }

    std::vector<CoordinateOperationNNPtr> steps;
    // If both are geographic and only differ by their prime
    // meridian,
    // apply a longitude rotation transformation.
    if (geogSrc->ellipsoid()->_isEquivalentTo(
            geogDst->ellipsoid().get(),
            util::IComparable::Criterion::EQUIVALENT) &&
        src_pm.getSIValue() != dst_pm.getSIValue()) {

        steps.emplace_back(Transformation::createLongitudeRotation(
            util::PropertyMap()
                .set(common::IdentifiedObject::NAME_KEY, name)
                .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                     metadata::Extent::WORLD),
            sourceCRS, targetCRS, offset_pm));
        // If only the target has a non-zero prime meridian, chain a
        // null geographic offset and then the longitude rotation
    } else if (src_pm.getSIValue() == 0 && dst_pm.getSIValue() != 0) {
        auto datum = datum::GeodeticReferenceFrame::create(
            util::PropertyMap(), geogDst->ellipsoid(),
            util::optional<std::string>(), geogSrc->primeMeridian());
        std::string interm_crs_name(geogDst->nameStr());
        interm_crs_name += " altered to use prime meridian of ";
        interm_crs_name += geogSrc->nameStr();
        auto interm_crs =
            util::nn_static_pointer_cast<crs::CRS>(crs::GeographicCRS::create(
                util::PropertyMap()
                    .set(common::IdentifiedObject::NAME_KEY, interm_crs_name)
                    .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                         metadata::Extent::WORLD),
                datum, dstCS));

        steps.emplace_back(createBallparkGeographicOffset(
            sourceCRS, interm_crs, dbContext, forceBallpark));

        steps.emplace_back(Transformation::createLongitudeRotation(
            util::PropertyMap()
                .set(common::IdentifiedObject::NAME_KEY,
                     buildTransfName(geogSrc->nameStr(), interm_crs->nameStr()))
                .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                     metadata::Extent::WORLD),
            interm_crs, targetCRS, offset_pm));

    } else {
        // If the prime meridians are different, chain a longitude
        // rotation and the null geographic offset.
        if (src_pm.getSIValue() != dst_pm.getSIValue()) {
            auto datum = datum::GeodeticReferenceFrame::create(
                util::PropertyMap(), geogSrc->ellipsoid(),
                util::optional<std::string>(), geogDst->primeMeridian());
            std::string interm_crs_name(geogSrc->nameStr());
            interm_crs_name += " altered to use prime meridian of ";
            interm_crs_name += geogDst->nameStr();
            auto interm_crs = util::nn_static_pointer_cast<crs::CRS>(
                crs::GeographicCRS::create(
                    util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                            interm_crs_name),
                    datum, srcCS));

            steps.emplace_back(Transformation::createLongitudeRotation(
                util::PropertyMap()
                    .set(common::IdentifiedObject::NAME_KEY,
                         buildTransfName(geogSrc->nameStr(),
                                         interm_crs->nameStr()))
                    .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                         metadata::Extent::WORLD),
                sourceCRS, interm_crs, offset_pm));
            steps.emplace_back(createBallparkGeographicOffset(
                interm_crs, targetCRS, dbContext, forceBallpark));
        } else {
            steps.emplace_back(createBallparkGeographicOffset(
                sourceCRS, targetCRS, dbContext, forceBallpark));
        }
    }

    auto op = ConcatenatedOperation::createComputeMetadata(
        steps, disallowEmptyIntersection);
    op->setHasBallparkTransformation(!sameDatum);
    res.emplace_back(op);
    return res;
}

// ---------------------------------------------------------------------------

static bool hasIdentifiers(const CoordinateOperationNNPtr &op) {
    if (!op->identifiers().empty()) {
        return true;
    }
    auto concatenated = dynamic_cast<const ConcatenatedOperation *>(op.get());
    if (concatenated) {
        for (const auto &subOp : concatenated->operations()) {
            if (hasIdentifiers(subOp)) {
                return true;
            }
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::setCRSs(
    CoordinateOperation *co, const crs::CRSNNPtr &sourceCRS,
    const crs::CRSNNPtr &targetCRS) {

    co->setCRSsUpdateInverse(sourceCRS, targetCRS, co->interpolationCRS());
}

// ---------------------------------------------------------------------------

static bool hasResultSetOnlyResultsWithPROJStep(
    const std::vector<CoordinateOperationNNPtr> &res) {
    for (const auto &op : res) {
        auto concat = dynamic_cast<const ConcatenatedOperation *>(op.get());
        if (concat) {
            bool hasPROJStep = false;
            const auto &steps = concat->operations();
            for (const auto &step : steps) {
                const auto &ids = step->identifiers();
                if (!ids.empty()) {
                    const auto &opAuthority = *(ids.front()->codeSpace());
                    if (opAuthority == "PROJ" ||
                        opAuthority == "INVERSE(PROJ)" ||
                        opAuthority == "DERIVED_FROM(PROJ)") {
                        hasPROJStep = true;
                        break;
                    }
                }
            }
            if (!hasPROJStep) {
                return false;
            }
        } else {
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsWithDatumPivot(
    std::vector<CoordinateOperationNNPtr> &res, const crs::CRSNNPtr &sourceCRS,
    const util::optional<common::DataEpoch> &sourceEpoch,
    const crs::CRSNNPtr &targetCRS,
    const util::optional<common::DataEpoch> &targetEpoch,
    const crs::GeodeticCRS *geodSrc, const crs::GeodeticCRS *geodDst,
    Private::Context &context) {

#ifdef TRACE_CREATE_OPERATIONS
    ENTER_BLOCK("createOperationsWithDatumPivot(" +
                objectAsStr(sourceCRS.get()) + "," +
                objectAsStr(targetCRS.get()) + ")");
#endif

    struct CreateOperationsWithDatumPivotAntiRecursion {
        Context &context;

        explicit CreateOperationsWithDatumPivotAntiRecursion(Context &contextIn)
            : context(contextIn) {
            assert(!context.inCreateOperationsWithDatumPivotAntiRecursion);
            context.inCreateOperationsWithDatumPivotAntiRecursion = true;
        }

        ~CreateOperationsWithDatumPivotAntiRecursion() {
            context.inCreateOperationsWithDatumPivotAntiRecursion = false;
        }
    };
    CreateOperationsWithDatumPivotAntiRecursion guard(context);

    const auto &authFactory = context.context->getAuthorityFactory();
    const auto &dbContext = authFactory->databaseContext();

    const auto srcDatum = geodSrc->datumNonNull(dbContext.as_nullable());
    if (srcDatum->identifiers().empty() &&
        (ci_equal(srcDatum->nameStr(), "unknown") ||
         ci_starts_with(srcDatum->nameStr(), UNKNOWN_BASED_ON)))
        return;
    const auto dstDatum = geodDst->datumNonNull(dbContext.as_nullable());
    if (dstDatum->identifiers().empty() &&
        (ci_equal(dstDatum->nameStr(), "unknown") ||
         ci_starts_with(dstDatum->nameStr(), UNKNOWN_BASED_ON)))
        return;

    const auto candidatesSrcGeod(
        findCandidateGeodCRSForDatum(authFactory, geodSrc, srcDatum));
    const auto candidatesDstGeod(
        findCandidateGeodCRSForDatum(authFactory, geodDst, dstDatum));

    const bool sourceAndTargetAre3D =
        geodSrc->coordinateSystem()->axisList().size() == 3 &&
        geodDst->coordinateSystem()->axisList().size() == 3;

    auto createTransformations = [&](const crs::CRSNNPtr &candidateSrcGeod,
                                     const crs::CRSNNPtr &candidateDstGeod,
                                     const CoordinateOperationNNPtr &opFirst,
                                     bool isNullFirst,
                                     bool useOnlyDirectRegistryOp) {
        bool resNonEmptyBeforeFiltering;

        // Deal with potential epoch change
        std::vector<CoordinateOperationNNPtr> opsEpochChangeSrc;
        std::vector<CoordinateOperationNNPtr> opsEpochChangeDst;
        if (sourceEpoch.has_value() && targetEpoch.has_value() &&
            !sourceEpoch->coordinateEpoch()._isEquivalentTo(
                targetEpoch->coordinateEpoch())) {
            const auto pmoSrc =
                context.context->getAuthorityFactory()
                    ->getPointMotionOperationsFor(
                        NN_NO_CHECK(
                            util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
                                candidateSrcGeod)),
                        true);
            if (!pmoSrc.empty()) {
                opsEpochChangeSrc =
                    createOperations(candidateSrcGeod, sourceEpoch,
                                     candidateSrcGeod, targetEpoch, context);
            } else {
                const auto pmoDst =
                    context.context->getAuthorityFactory()
                        ->getPointMotionOperationsFor(
                            NN_NO_CHECK(
                                util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
                                    candidateDstGeod)),
                            true);
                if (!pmoDst.empty()) {
                    opsEpochChangeDst = createOperations(
                        candidateDstGeod, sourceEpoch, candidateDstGeod,
                        targetEpoch, context);
                }
            }
        }

        const std::vector<CoordinateOperationNNPtr> opsSecond(
            useOnlyDirectRegistryOp
                ? findOpsInRegistryDirect(candidateSrcGeod, candidateDstGeod,
                                          context, resNonEmptyBeforeFiltering)
                : createOperations(candidateSrcGeod, targetEpoch,
                                   candidateDstGeod, targetEpoch, context));
        const auto opsThird = createOperations(
            sourceAndTargetAre3D
                ? candidateDstGeod->promoteTo3D(std::string(), dbContext)
                : candidateDstGeod,
            targetEpoch, targetCRS, targetEpoch, context);
        assert(!opsThird.empty());
        const CoordinateOperationNNPtr &opThird(opsThird[0]);

        const auto nIters = std::max<size_t>(
            1, std::max(opsEpochChangeSrc.size(), opsEpochChangeDst.size()));
        for (size_t iEpochChange = 0; iEpochChange < nIters; ++iEpochChange) {
            for (auto &opSecond : opsSecond) {
                // Check that it is not a transformation synthesized by
                // ourselves
                if (!hasIdentifiers(opSecond)) {
                    continue;
                }
                // And even if it is a referenced transformation, check that
                // it is not a trivial one
                auto so = dynamic_cast<const SingleOperation *>(opSecond.get());
                if (so && isAxisOrderReversal(so->method()->getEPSGCode())) {
                    continue;
                }

                std::vector<CoordinateOperationNNPtr> subOps;
                const bool isNullThird =
                    isNullTransformation(opThird->nameStr());
                CoordinateOperationNNPtr opSecondCloned(
                    (isNullFirst || isNullThird || sourceAndTargetAre3D)
                        ? opSecond->shallowClone()
                        : opSecond);
                if (isNullFirst || isNullThird) {
                    if (opSecondCloned->identifiers().size() == 1 &&
                        (*opSecondCloned->identifiers()[0]->codeSpace())
                                .find("DERIVED_FROM") == std::string::npos) {
                        {
                            util::PropertyMap map;
                            addModifiedIdentifier(map, opSecondCloned.get(),
                                                  false, true);
                            opSecondCloned->setProperties(map);
                        }
                        auto invCO = dynamic_cast<InverseCoordinateOperation *>(
                            opSecondCloned.get());
                        if (invCO) {
                            auto invCOForward = invCO->forwardOperation().get();
                            if (invCOForward->identifiers().size() == 1 &&
                                (*invCOForward->identifiers()[0]->codeSpace())
                                        .find("DERIVED_FROM") ==
                                    std::string::npos) {
                                util::PropertyMap map;
                                addModifiedIdentifier(map, invCOForward, false,
                                                      true);
                                invCOForward->setProperties(map);
                            }
                        }
                    }
                }
                if (sourceAndTargetAre3D) {

                    // Force Helmert operations to use the 3D domain, even if
                    // the ones we found in EPSG are advertised for the 2D
                    // domain.
                    auto concat = dynamic_cast<ConcatenatedOperation *>(
                        opSecondCloned.get());
                    if (concat) {
                        std::vector<CoordinateOperationNNPtr> newSteps;
                        for (const auto &step : concat->operations()) {
                            auto newStep = step->shallowClone();
                            setCRSs(newStep.get(),
                                    newStep->sourceCRS()->promoteTo3D(
                                        std::string(), dbContext),
                                    newStep->targetCRS()->promoteTo3D(
                                        std::string(), dbContext));
                            newSteps.emplace_back(newStep);
                        }
                        opSecondCloned =
                            ConcatenatedOperation::createComputeMetadata(
                                newSteps, disallowEmptyIntersection);
                    } else {
                        setCRSs(opSecondCloned.get(),
                                opSecondCloned->sourceCRS()->promoteTo3D(
                                    std::string(), dbContext),
                                opSecondCloned->targetCRS()->promoteTo3D(
                                    std::string(), dbContext));
                    }
                }
                if (!isNullFirst) {
                    subOps.emplace_back(opFirst);
                }
                if (!opsEpochChangeSrc.empty()) {
                    subOps.emplace_back(opsEpochChangeSrc[iEpochChange]);
                }
                subOps.emplace_back(opSecondCloned);
                if (!opsEpochChangeDst.empty()) {
                    subOps.emplace_back(opsEpochChangeDst[iEpochChange]);
                }
                if (!isNullThird) {
                    subOps.emplace_back(opThird);
                }

                subOps[0] = subOps[0]->shallowClone();
                if (subOps[0]->targetCRS())
                    setCRSs(subOps[0].get(), sourceCRS,
                            NN_NO_CHECK(subOps[0]->targetCRS()));
                subOps.back() = subOps.back()->shallowClone();
                if (subOps[0]->sourceCRS())
                    setCRSs(subOps.back().get(),
                            NN_NO_CHECK(subOps.back()->sourceCRS()), targetCRS);

#ifdef TRACE_CREATE_OPERATIONS
                std::string debugStr;
                for (const auto &op : subOps) {
                    if (!debugStr.empty()) {
                        debugStr += " + ";
                    }
                    debugStr += objectAsStr(op.get());
                    debugStr += " (";
                    debugStr += objectAsStr(op->sourceCRS().get());
                    debugStr += "->";
                    debugStr += objectAsStr(op->targetCRS().get());
                    debugStr += ")";
                }
                logTrace("transformation " + debugStr);
#endif
                try {
                    res.emplace_back(
                        ConcatenatedOperation::createComputeMetadata(
                            subOps, disallowEmptyIntersection));
                } catch (const InvalidOperationEmptyIntersection &) {
                }
            }
        }
    };

    // The below logic is thus quite fragile, and attempts at changing it
    // result in degraded results for other use cases...
    //
    // Start in priority with candidates that have exactly the same name as
    // the sourceCRS and targetCRS (Typically for the case of init=IGNF:XXXX);
    // and then attempt candidate geodetic CRS with different names
    //
    // Transformation from IGNF:NTFP to IGNF:RGF93G,
    // using
    // NTF geographiques Paris (gr) vers NTF GEOGRAPHIQUES GREENWICH (DMS) +
    // NOUVELLE TRIANGULATION DE LA FRANCE (NTF) vers RGF93 (ETRS89)
    // that is using ntf_r93.gsb, is horribly dependent
    // of IGNF:RGF93G being returned before IGNF:RGF93GEO in candidatesDstGeod.
    // If RGF93GEO is returned before then we go through WGS84 and use
    // instead a Helmert transformation.
    //
    // Actually, in the general case, we do the lookup in 3 passes with the 2
    // above steps in each pass:
    // - one first pass where we only consider direct transformations (no
    //   other intermediate CRS)
    // - a second pass where we allow transformation through another
    //   intermediate CRS, but we make sure the candidate geodetic CRS are of
    //   the same type
    // - a third where we allow transformation through another
    //   intermediate CRS, where the candidate geodetic CRS are of different
    //   type.
    // ... but when transforming between 2 IGNF CRS, we do just one single pass
    // by allowing directly all transformation. There is no strong reason for
    // that particular case, except that otherwise we'd get different results
    // for test/cli/test_cs2cs_ignf.yaml when transforming a point outside
    // the area of validity... Not totally sure the behavior we try to preserve
    // here with the particular case is fundamentally better than the general
    // case. The general case is needed typically for the RGNC91-93 -> RGNC15
    // transformation where we we need to actually use a transformation between
    // RGNC91-93 (lon-lat) -> RGNC15 (lon-lat), and not chain 2 no-op
    // transformations RGNC91-93 -> WGS 84 and RGNC15 -> WGS84.

    const auto isIGNF = [](const crs::CRSNNPtr &crs) {
        const auto &ids = crs->identifiers();
        return !ids.empty() && *(ids.front()->codeSpace()) == "IGNF";
    };
    const int nIters = (isIGNF(sourceCRS) && isIGNF(targetCRS)) ? 1 : 3;

    const auto getType = [](const crs::GeodeticCRSNNPtr &crs) {
        if (auto geogCRS =
                dynamic_cast<const crs::GeographicCRS *>(crs.get())) {
            if (geogCRS->coordinateSystem()->axisList().size() == 3)
                return 1;
            return 0;
        }
        return 2;
    };

    for (int iter = 0; iter < nIters; ++iter) {
        const bool useOnlyDirectRegistryOp = (iter == 0 && nIters == 3);
        for (const auto &candidateSrcGeod : candidatesSrcGeod) {
            if (candidateSrcGeod->nameStr() == sourceCRS->nameStr()) {
                const auto typeSource =
                    (iter >= 1) ? getType(candidateSrcGeod) : -1;
                auto sourceSrcGeodModified(sourceAndTargetAre3D
                                               ? candidateSrcGeod->promoteTo3D(
                                                     std::string(), dbContext)
                                               : candidateSrcGeod);
                for (const auto &candidateDstGeod : candidatesDstGeod) {
                    if (candidateDstGeod->nameStr() == targetCRS->nameStr()) {
                        if (iter == 1) {
                            if (typeSource != getType(candidateDstGeod)) {
                                continue;
                            }
                        } else if (iter == 2) {
                            if (typeSource == getType(candidateDstGeod)) {
                                continue;
                            }
                        }
#ifdef TRACE_CREATE_OPERATIONS
                        ENTER_BLOCK("iter=" + toString(iter) + ", try " +
                                    objectAsStr(sourceCRS.get()) + "->" +
                                    objectAsStr(candidateSrcGeod.get()) + "->" +
                                    objectAsStr(candidateDstGeod.get()) + "->" +
                                    objectAsStr(targetCRS.get()) + ")");
#endif
                        const auto opsFirst = createOperations(
                            sourceCRS, sourceEpoch, sourceSrcGeodModified,
                            sourceEpoch, context);
                        assert(!opsFirst.empty());
                        const bool isNullFirst =
                            isNullTransformation(opsFirst[0]->nameStr());
                        createTransformations(
                            candidateSrcGeod, candidateDstGeod, opsFirst[0],
                            isNullFirst, useOnlyDirectRegistryOp);
                        if (!res.empty()) {
                            if (hasResultSetOnlyResultsWithPROJStep(res)) {
                                continue;
                            }
                            return;
                        }
                    }
                }
            }
        }

        for (const auto &candidateSrcGeod : candidatesSrcGeod) {
            const bool bSameSrcName =
                candidateSrcGeod->nameStr() == sourceCRS->nameStr();
#ifdef TRACE_CREATE_OPERATIONS
            ENTER_BLOCK("");
#endif
            auto sourceSrcGeodModified(
                sourceAndTargetAre3D
                    ? candidateSrcGeod->promoteTo3D(std::string(), dbContext)
                    : candidateSrcGeod);
            const auto opsFirst =
                createOperations(sourceCRS, sourceEpoch, sourceSrcGeodModified,
                                 sourceEpoch, context);
            assert(!opsFirst.empty());
            const bool isNullFirst =
                isNullTransformation(opsFirst[0]->nameStr());

            for (const auto &candidateDstGeod : candidatesDstGeod) {
                if (bSameSrcName &&
                    candidateDstGeod->nameStr() == targetCRS->nameStr()) {
                    continue;
                }

#ifdef TRACE_CREATE_OPERATIONS
                ENTER_BLOCK("try " + objectAsStr(sourceCRS.get()) + "->" +
                            objectAsStr(candidateSrcGeod.get()) + "->" +
                            objectAsStr(candidateDstGeod.get()) + "->" +
                            objectAsStr(targetCRS.get()) + ")");
#endif
                createTransformations(candidateSrcGeod, candidateDstGeod,
                                      opsFirst[0], isNullFirst,
                                      useOnlyDirectRegistryOp);
                if (!res.empty() && !hasResultSetOnlyResultsWithPROJStep(res)) {
                    return;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------

static CoordinateOperationNNPtr
createBallparkGeocentricTranslation(const crs::CRSNNPtr &sourceCRS,
                                    const crs::CRSNNPtr &targetCRS) {
    std::string name(BALLPARK_GEOCENTRIC_TRANSLATION);
    name += " from ";
    name += sourceCRS->nameStr();
    name += " to ";
    name += targetCRS->nameStr();

    return util::nn_static_pointer_cast<CoordinateOperation>(
        Transformation::createGeocentricTranslations(
            util::PropertyMap()
                .set(common::IdentifiedObject::NAME_KEY, name)
                .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                     metadata::Extent::WORLD),
            sourceCRS, targetCRS, 0.0, 0.0, 0.0, {}));
}

// ---------------------------------------------------------------------------

bool CoordinateOperationFactory::Private::hasPerfectAccuracyResult(
    const std::vector<CoordinateOperationNNPtr> &res, const Context &context) {
    auto resTmp = FilterResults(res, context.context, context.extent1,
                                context.extent2, true)
                      .getRes();
    for (const auto &op : resTmp) {
        const double acc = getAccuracy(op);
        if (acc == 0.0) {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::Private::createOperations(
    const crs::CRSNNPtr &sourceCRS,
    const util::optional<common::DataEpoch> &sourceEpoch,
    const crs::CRSNNPtr &targetCRS,
    const util::optional<common::DataEpoch> &targetEpoch,
    Private::Context &context) {

#ifdef TRACE_CREATE_OPERATIONS
    ENTER_BLOCK("createOperations(" + objectAsStr(sourceCRS.get()) + " --> " +
                objectAsStr(targetCRS.get()) + ")");
#endif

#ifndef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
    // 10 is arbitrary and hopefully large enough for all transformations PROJ
    // can handle.
    // At time of writing 7 is the maximum known to be required by a few tests
    // like
    // operation.compoundCRS_to_compoundCRS_with_bound_crs_in_horiz_and_vert_WKT1_same_geoidgrids_context
    // We don't enable that check for fuzzing, to be able to detect
    // the root cause of recursions.
    if (context.nRecLevelCreateOperations == 10) {
        throw InvalidOperation("Too deep recursion in createOperations()");
    }
#endif

    struct RecLevelIncrementer {
        Private::Context &context_;

        explicit inline RecLevelIncrementer(Private::Context &contextIn)
            : context_(contextIn) {
            ++context_.nRecLevelCreateOperations;
        }

        inline ~RecLevelIncrementer() { --context_.nRecLevelCreateOperations; }
    };
    RecLevelIncrementer recLevelIncrementer(context);

    std::vector<CoordinateOperationNNPtr> res;

    auto boundSrc = dynamic_cast<const crs::BoundCRS *>(sourceCRS.get());
    auto boundDst = dynamic_cast<const crs::BoundCRS *>(targetCRS.get());

    const auto &sourceProj4Ext = boundSrc
                                     ? boundSrc->baseCRS()->getExtensionProj4()
                                     : sourceCRS->getExtensionProj4();
    const auto &targetProj4Ext = boundDst
                                     ? boundDst->baseCRS()->getExtensionProj4()
                                     : targetCRS->getExtensionProj4();
    if (!sourceProj4Ext.empty() || !targetProj4Ext.empty()) {
        createOperationsFromProj4Ext(sourceCRS, targetCRS, boundSrc, boundDst,
                                     res);
        return res;
    }

    auto geodSrc = dynamic_cast<const crs::GeodeticCRS *>(sourceCRS.get());
    auto geodDst = dynamic_cast<const crs::GeodeticCRS *>(targetCRS.get());
    auto geogSrc = dynamic_cast<const crs::GeographicCRS *>(sourceCRS.get());
    auto geogDst = dynamic_cast<const crs::GeographicCRS *>(targetCRS.get());
    auto vertSrc = dynamic_cast<const crs::VerticalCRS *>(sourceCRS.get());
    auto vertDst = dynamic_cast<const crs::VerticalCRS *>(targetCRS.get());

    // First look-up if the registry provide us with operations.
    auto derivedSrc = dynamic_cast<const crs::DerivedCRS *>(sourceCRS.get());
    auto derivedDst = dynamic_cast<const crs::DerivedCRS *>(targetCRS.get());
    const auto &authFactory = context.context->getAuthorityFactory();
    if (authFactory &&
        (derivedSrc == nullptr ||
         !derivedSrc->baseCRS()->_isEquivalentTo(
             targetCRS.get(), util::IComparable::Criterion::EQUIVALENT)) &&
        (derivedDst == nullptr ||
         !derivedDst->baseCRS()->_isEquivalentTo(
             sourceCRS.get(), util::IComparable::Criterion::EQUIVALENT))) {

        if (createOperationsFromDatabase(
                sourceCRS, sourceEpoch, targetCRS, targetEpoch, context,
                geodSrc, geodDst, geogSrc, geogDst, vertSrc, vertDst, res)) {
            return res;
        }
    }

    if (geodSrc && geodSrc->isSphericalPlanetocentric()) {
        createOperationsFromSphericalPlanetocentric(sourceCRS, sourceEpoch,
                                                    targetCRS, targetEpoch,
                                                    context, geodSrc, res);
        return res;
    } else if (geodDst && geodDst->isSphericalPlanetocentric()) {
        return applyInverse(createOperations(targetCRS, targetEpoch, sourceCRS,
                                             sourceEpoch, context));
    }

    // Special case if both CRS are geodetic
    if (geodSrc && geodDst && !derivedSrc && !derivedDst) {
        createOperationsGeodToGeod(sourceCRS, targetCRS, context, geodSrc,
                                   geodDst, res, /*forceBallpark=*/false);
        return res;
    }

    if (boundSrc) {
        auto geodSrcBase = util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
            boundSrc->baseCRS());
        if (geodSrcBase && geodSrcBase->isSphericalPlanetocentric()) {
            createOperationsFromBoundOfSphericalPlanetocentric(
                sourceCRS, targetCRS, context, boundSrc,
                NN_NO_CHECK(geodSrcBase), res);
            return res;
        }
    } else if (boundDst) {
        auto geodDstBase = util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
            boundDst->baseCRS());
        if (geodDstBase && geodDstBase->isSphericalPlanetocentric()) {
            return applyInverse(createOperations(
                targetCRS, targetEpoch, sourceCRS, sourceEpoch, context));
        }
    }

    // If the source is a derived CRS, then chain the inverse of its
    // deriving conversion, with transforms from its baseCRS to the
    // targetCRS
    if (derivedSrc) {
        createOperationsDerivedTo(sourceCRS, sourceEpoch, targetCRS,
                                  targetEpoch, context, derivedSrc, res);
        return res;
    }

    // reverse of previous case
    if (derivedDst) {
        return applyInverse(createOperations(targetCRS, targetEpoch, sourceCRS,
                                             sourceEpoch, context));
    }

    // Order of comparison between the geogDst vs geodDst is important
    if (boundSrc && geogDst) {
        createOperationsBoundToGeog(sourceCRS, targetCRS, context, boundSrc,
                                    geogDst, res);
        return res;
    } else if (boundSrc && geodDst) {
        createOperationsToGeod(sourceCRS, sourceEpoch, targetCRS, targetEpoch,
                               context, geodDst, res);
        return res;
    }

    // reverse of previous case
    if (geodSrc && boundDst) {
        return applyInverse(createOperations(targetCRS, targetEpoch, sourceCRS,
                                             sourceEpoch, context));
    }

    // vertCRS (as boundCRS with transformation to target vertCRS) to
    // vertCRS
    if (boundSrc && vertDst) {
        createOperationsBoundToVert(sourceCRS, targetCRS, context, boundSrc,
                                    vertDst, res);
        return res;
    }

    // reverse of previous case
    if (boundDst && vertSrc) {
        return applyInverse(createOperations(targetCRS, targetEpoch, sourceCRS,
                                             sourceEpoch, context));
    }

    if (vertSrc && vertDst) {
        createOperationsVertToVert(sourceCRS, targetCRS, context, vertSrc,
                                   vertDst, res);
        return res;
    }

    // A bit odd case as we are comparing apples to oranges, but in case
    // the vertical unit differ, do something useful.
    if (vertSrc && geogDst) {
        createOperationsVertToGeog(sourceCRS, sourceEpoch, targetCRS,
                                   targetEpoch, context, vertSrc, geogDst, res);
        return res;
    }

    // reverse of previous case
    if (vertDst && geogSrc) {
        return applyInverse(createOperations(targetCRS, targetEpoch, sourceCRS,
                                             sourceEpoch, context));
    }

    // boundCRS to boundCRS
    if (boundSrc && boundDst) {
        createOperationsBoundToBound(sourceCRS, targetCRS, context, boundSrc,
                                     boundDst, res);
        return res;
    }

    auto compoundSrc = dynamic_cast<crs::CompoundCRS *>(sourceCRS.get());
    // Order of comparison between the geogDst vs geodDst is important
    if (compoundSrc && geogDst) {
        createOperationsCompoundToGeog(sourceCRS, sourceEpoch, targetCRS,
                                       targetEpoch, context, compoundSrc,
                                       geogDst, res);
        return res;
    } else if (compoundSrc && geodDst) {
        createOperationsToGeod(sourceCRS, sourceEpoch, targetCRS, targetEpoch,
                               context, geodDst, res);
        return res;
    }

    // reverse of previous cases
    auto compoundDst = dynamic_cast<const crs::CompoundCRS *>(targetCRS.get());
    if (geodSrc && compoundDst) {
        return applyInverse(createOperations(targetCRS, targetEpoch, sourceCRS,
                                             sourceEpoch, context));
    }

    if (compoundSrc && compoundDst) {
        createOperationsCompoundToCompound(sourceCRS, sourceEpoch, targetCRS,
                                           targetEpoch, context, compoundSrc,
                                           compoundDst, res);
        return res;
    }

    // '+proj=longlat +ellps=GRS67 +nadgrids=@foo.gsb +type=crs' to
    // '+proj=longlat +ellps=GRS80 +nadgrids=@bar.gsb +geoidgrids=@bar.gtx
    // +type=crs'
    if (boundSrc && compoundDst) {
        createOperationsBoundToCompound(sourceCRS, targetCRS, context, boundSrc,
                                        compoundDst, res);
        return res;
    }

    // reverse of previous case
    if (boundDst && compoundSrc) {
        return applyInverse(createOperations(targetCRS, targetEpoch, sourceCRS,
                                             sourceEpoch, context));
    }

    if (dynamic_cast<const crs::EngineeringCRS *>(sourceCRS.get()) &&
        sourceCRS->_isEquivalentTo(targetCRS.get(),
                                   util::IComparable::Criterion::EQUIVALENT)) {
        std::string name("Identity transformation from ");
        name += sourceCRS->nameStr();
        name += " to ";
        name += targetCRS->nameStr();
        res.push_back(Transformation::create(
            util::PropertyMap()
                .set(common::IdentifiedObject::NAME_KEY, name)
                .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                     metadata::Extent::WORLD),
            sourceCRS, targetCRS, nullptr,
            createMethodMapNameEPSGCode(
                EPSG_CODE_METHOD_CARTESIAN_GRID_OFFSETS),
            VectorOfParameters{
                createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_EASTING_OFFSET),
                createOpParamNameEPSGCode(EPSG_CODE_PARAMETER_NORTHING_OFFSET),
            },
            VectorOfValues{
                common::Length(0),
                common::Length(0),
            },
            {metadata::PositionalAccuracy::create("0")}));
    }

    return res;
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsFromProj4Ext(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const crs::BoundCRS *boundSrc, const crs::BoundCRS *boundDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    auto sourceProjExportable = dynamic_cast<const io::IPROJStringExportable *>(
        boundSrc ? boundSrc : sourceCRS.get());
    auto targetProjExportable = dynamic_cast<const io::IPROJStringExportable *>(
        boundDst ? boundDst : targetCRS.get());
    if (!sourceProjExportable) {
        throw InvalidOperation("Source CRS is not PROJ exportable");
    }
    if (!targetProjExportable) {
        throw InvalidOperation("Target CRS is not PROJ exportable");
    }
    auto projFormatter = io::PROJStringFormatter::create();
    projFormatter->setCRSExport(true);
    projFormatter->setLegacyCRSToCRSContext(true);
    projFormatter->startInversion();
    sourceProjExportable->_exportToPROJString(projFormatter.get());
    auto geogSrc = dynamic_cast<const crs::GeographicCRS *>(
        boundSrc ? boundSrc->baseCRS().get() : sourceCRS.get());
    if (geogSrc) {
        auto tmpFormatter = io::PROJStringFormatter::create();
        geogSrc->addAngularUnitConvertAndAxisSwap(tmpFormatter.get());
        projFormatter->ingestPROJString(tmpFormatter->toString());
    }

    projFormatter->stopInversion();

    targetProjExportable->_exportToPROJString(projFormatter.get());
    auto geogDst = dynamic_cast<const crs::GeographicCRS *>(
        boundDst ? boundDst->baseCRS().get() : targetCRS.get());
    if (geogDst) {
        auto tmpFormatter = io::PROJStringFormatter::create();
        geogDst->addAngularUnitConvertAndAxisSwap(tmpFormatter.get());
        projFormatter->ingestPROJString(tmpFormatter->toString());
    }

    auto properties = util::PropertyMap().set(
        common::IdentifiedObject::NAME_KEY,
        buildTransfName(sourceCRS->nameStr(), targetCRS->nameStr()));
    res.emplace_back(SingleOperation::createPROJBased(
        properties, projFormatter->toString(), sourceCRS, targetCRS, {}));
}

// ---------------------------------------------------------------------------

bool CoordinateOperationFactory::Private::createOperationsFromDatabase(
    const crs::CRSNNPtr &sourceCRS,
    const util::optional<common::DataEpoch> &sourceEpoch,
    const crs::CRSNNPtr &targetCRS,
    const util::optional<common::DataEpoch> &targetEpoch,
    Private::Context &context, const crs::GeodeticCRS *geodSrc,
    const crs::GeodeticCRS *geodDst, const crs::GeographicCRS *geogSrc,
    const crs::GeographicCRS *geogDst, const crs::VerticalCRS *vertSrc,
    const crs::VerticalCRS *vertDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    if (geogSrc && vertDst) {
        createOperationsFromDatabase(targetCRS, targetEpoch, sourceCRS,
                                     sourceEpoch, context, geodDst, geodSrc,
                                     geogDst, geogSrc, vertDst, vertSrc, res);
        res = applyInverse(res);
    } else if (geogDst && vertSrc) {
        res = applyInverse(createOperationsGeogToVertFromGeoid(
            targetCRS, sourceCRS, vertSrc, context));
        if (!res.empty()) {
            createOperationsVertToGeogSynthetized(sourceCRS, targetCRS, context,
                                                  vertSrc, geogDst, res);
        }
    }

    if (!res.empty()) {
        return true;
    }

    // Use PointMotionOperations if appropriate and available
    if (geodSrc && geodDst && sourceEpoch.has_value() &&
        targetEpoch.has_value() &&
        !sourceEpoch->coordinateEpoch()._isEquivalentTo(
            targetEpoch->coordinateEpoch())) {
        const auto pmoSrc =
            context.context->getAuthorityFactory()->getPointMotionOperationsFor(
                NN_NO_CHECK(
                    util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(sourceCRS)),
                true);
        if (!pmoSrc.empty()) {
            const auto pmoDst =
                context.context->getAuthorityFactory()
                    ->getPointMotionOperationsFor(
                        NN_NO_CHECK(
                            util::nn_dynamic_pointer_cast<crs::GeodeticCRS>(
                                targetCRS)),
                        true);
            if (pmoDst.size() == pmoSrc.size()) {
                bool ok = true;
                for (size_t i = 0; i < pmoSrc.size(); ++i) {
                    if (pmoSrc[i]->_isEquivalentTo(
                            pmoDst[i].get(),
                            util::IComparable::Criterion::EQUIVALENT)) {
                        auto pmo = pmoSrc[i]->cloneWithEpochs(*sourceEpoch,
                                                              *targetEpoch);
                        std::vector<operation::CoordinateOperationNNPtr> ops;
                        if (!pmo->sourceCRS()->_isEquivalentTo(
                                sourceCRS.get(),
                                util::IComparable::Criterion::EQUIVALENT)) {
                            auto tmp = createOperations(sourceCRS, sourceEpoch,
                                                        pmo->sourceCRS(),
                                                        sourceEpoch, context);
                            assert(!tmp.empty());
                            ops.emplace_back(tmp.front());
                        }
                        ops.emplace_back(pmo);
                        // pmo->sourceCRS() == pmo->targetCRS() by definition
                        if (!pmo->sourceCRS()->_isEquivalentTo(
                                targetCRS.get(),
                                util::IComparable::Criterion::EQUIVALENT)) {
                            auto tmp = createOperations(pmo->sourceCRS(),
                                                        targetEpoch, targetCRS,
                                                        targetEpoch, context);
                            assert(!tmp.empty());
                            ops.emplace_back(tmp.front());
                        }
                        res.emplace_back(
                            ConcatenatedOperation::createComputeMetadata(
                                ops, disallowEmptyIntersection));
                    } else {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    std::vector<CoordinateOperationNNPtr> resTmp;
                    createOperationsGeodToGeod(sourceCRS, targetCRS, context,
                                               geodSrc, geodDst, resTmp,
                                               /*forceBallpark=*/true);
                    res.insert(res.end(), resTmp.begin(), resTmp.end());
                    return true;
                }
            }
        }
    }

    bool resFindDirectNonEmptyBeforeFiltering = false;
    res = findOpsInRegistryDirect(sourceCRS, targetCRS, context,
                                  resFindDirectNonEmptyBeforeFiltering);

    // If we get at least a result with perfect accuracy, do not
    // bother generating synthetic transforms.
    if (hasPerfectAccuracyResult(res, context)) {
        return true;
    }

    bool doFilterAndCheckPerfectOp = false;

    bool sameGeodeticDatum = false;

    if (vertSrc || vertDst) {
        if (res.empty()) {
            if (geogSrc &&
                geogSrc->coordinateSystem()->axisList().size() == 2 &&
                vertDst) {
                auto dbContext =
                    context.context->getAuthorityFactory()->databaseContext();
                auto resTmp = findOpsInRegistryDirect(
                    sourceCRS->promoteTo3D(std::string(), dbContext), targetCRS,
                    context, resFindDirectNonEmptyBeforeFiltering);
                for (auto &op : resTmp) {
                    auto newOp = op->shallowClone();
                    setCRSs(newOp.get(), sourceCRS, targetCRS);
                    res.emplace_back(newOp);
                }
            } else if (geogDst &&
                       geogDst->coordinateSystem()->axisList().size() == 2 &&
                       vertSrc) {
                auto dbContext =
                    context.context->getAuthorityFactory()->databaseContext();
                auto resTmp = findOpsInRegistryDirect(
                    sourceCRS, targetCRS->promoteTo3D(std::string(), dbContext),
                    context, resFindDirectNonEmptyBeforeFiltering);
                for (auto &op : resTmp) {
                    auto newOp = op->shallowClone();
                    setCRSs(newOp.get(), sourceCRS, targetCRS);
                    res.emplace_back(newOp);
                }
            }
        }
        if (res.empty()) {
            createOperationsFromDatabaseWithVertCRS(
                sourceCRS, sourceEpoch, targetCRS, targetEpoch, context,
                geogSrc, geogDst, vertSrc, vertDst, res);
        }
    } else if (geodSrc && geodDst) {

        const auto &authFactory = context.context->getAuthorityFactory();
        const auto dbContext = authFactory->databaseContext().as_nullable();

        const auto srcDatum = geodSrc->datumNonNull(dbContext);
        const auto dstDatum = geodDst->datumNonNull(dbContext);

        sameGeodeticDatum = isSameGeodeticDatum(srcDatum, dstDatum, dbContext);

        if (res.empty() && !sameGeodeticDatum &&
            !context.inCreateOperationsWithDatumPivotAntiRecursion) {
            // If we still didn't find a transformation, and that the source
            // and target are GeodeticCRS, then go through their underlying
            // datum to find potential transformations between other
            // GeodeticCRSs
            // that are made of those datum
            // The typical example is if transforming between two
            // GeographicCRS,
            // but transformations are only available between their
            // corresponding geocentric CRS.
            createOperationsWithDatumPivot(res, sourceCRS, sourceEpoch,
                                           targetCRS, targetEpoch, geodSrc,
                                           geodDst, context);
            doFilterAndCheckPerfectOp = !res.empty();
        }
    }

    bool foundInstantiableOp = false;
    // FIXME: the limitation to .size() == 1 is just for the
    // -s EPSG:4959+5759 -t "EPSG:4959+7839" case
    // finding EPSG:7860 'NZVD2016 height to Auckland 1946
    // height (1)', which uses the EPSG:1071 'Vertical Offset by Grid
    // Interpolation (NZLVD)' method which is not currently implemented by PROJ
    // (cannot deal with .csv files)
    // Initially the test was written to iterate over for all operations of a
    // non-empty res, but this causes failures in the test suite when no grids
    // are installed at all. Ideally we should tweak the test suite to be
    // robust to that, or skip some tests.
    if (res.size() == 1) {
        try {
            res.front()->exportToPROJString(
                io::PROJStringFormatter::create().get());
            foundInstantiableOp = true;
        } catch (const std::exception &) {
        }
        if (!foundInstantiableOp) {
            resFindDirectNonEmptyBeforeFiltering = false;
        }
    } else if (res.size() > 1) {
        foundInstantiableOp = true;
    }

    // NAD27 to NAD83 has tens of results already. No need to look
    // for a pivot
    if (!sameGeodeticDatum &&
        (((res.empty() || !foundInstantiableOp) &&
          !resFindDirectNonEmptyBeforeFiltering &&
          context.context->getAllowUseIntermediateCRS() ==
              CoordinateOperationContext::IntermediateCRSUse::
                  IF_NO_DIRECT_TRANSFORMATION) ||
         context.context->getAllowUseIntermediateCRS() ==
             CoordinateOperationContext::IntermediateCRSUse::ALWAYS ||
         getenv("PROJ_FORCE_SEARCH_PIVOT"))) {
        auto resWithIntermediate = findsOpsInRegistryWithIntermediate(
            sourceCRS, targetCRS, context, false);
        res.insert(res.end(), resWithIntermediate.begin(),
                   resWithIntermediate.end());
        doFilterAndCheckPerfectOp = !res.empty();
    }

    // Browse through candidate operations and check if their area of use
    // is sufficiently large compared to the area of use of the
    // source/target CRS. If it is not, we might need to extend the lookup
    // to using intermediate CRSs.
    // But only do that if we have a relatively small number of solutions.
    // Otherwise we might just spend time appending more dubious solutions.
    bool tooSmallAreas = false;
    // 10: arbitrary threshold so that particular case triggers for
    // EPSG:9989 (ITRF2000) to EPSG:4937 (ETRS89), but not for
    // "WGS 84" to "NAD83(CSRS)v2", to avoid excessive computation time.
    if (!res.empty() && res.size() < 10) {
        const auto &areaOfInterest = context.context->getAreaOfInterest();
        double targetArea = 0.0;
        if (areaOfInterest) {
            targetArea = getPseudoArea(NN_NO_CHECK(areaOfInterest));
        } else if (context.extent1) {
            if (context.extent2) {
                auto intersection =
                    context.extent1->intersection(NN_NO_CHECK(context.extent2));
                if (intersection)
                    targetArea = getPseudoArea(NN_NO_CHECK(intersection));
            } else {
                targetArea = getPseudoArea(NN_NO_CHECK(context.extent1));
            }
        } else if (context.extent2) {
            targetArea = getPseudoArea(NN_NO_CHECK(context.extent2));
        }
        if (targetArea > 0) {
            tooSmallAreas = true;
            for (const auto &op : res) {
                bool dummy = false;
                auto extentOp = getExtent(op, true, dummy);
                double area = 0.0;
                if (extentOp) {
                    if (areaOfInterest) {
                        area = getPseudoArea(extentOp->intersection(
                            NN_NO_CHECK(areaOfInterest)));
                    } else if (context.extent1 && context.extent2) {
                        auto x = extentOp->intersection(
                            NN_NO_CHECK(context.extent1));
                        auto y = extentOp->intersection(
                            NN_NO_CHECK(context.extent2));
                        area = getPseudoArea(x) + getPseudoArea(y) -
                               ((x && y) ? getPseudoArea(
                                               x->intersection(NN_NO_CHECK(y)))
                                         : 0.0);
                    } else if (context.extent1) {
                        area = getPseudoArea(extentOp->intersection(
                            NN_NO_CHECK(context.extent1)));
                    } else if (context.extent2) {
                        area = getPseudoArea(extentOp->intersection(
                            NN_NO_CHECK(context.extent2)));
                    } else {
                        area = getPseudoArea(extentOp);
                    }
                }

                constexpr double HALF_RATIO = 0.5;
                if (area > HALF_RATIO * targetArea) {
                    tooSmallAreas = false;
                    break;
                }
            }
        }
    }

    bool allRequiresPerCoordinateInputTime = false;
    for (const auto &op : res) {
        allRequiresPerCoordinateInputTime =
            op->requiresPerCoordinateInputTime();
        if (!allRequiresPerCoordinateInputTime) {
            break;
        }
    }

    if (!context.inCreateOperationsWithDatumPivotAntiRecursion &&
        !resFindDirectNonEmptyBeforeFiltering && geodSrc && geodDst &&
        !sameGeodeticDatum && context.context->getIntermediateCRS().empty() &&
        context.context->getAllowUseIntermediateCRS() !=
            CoordinateOperationContext::IntermediateCRSUse::NEVER &&
        (res.empty() || tooSmallAreas ||
         // If all coordinate operations are time-dependent and none of the
         // source or target CRS are dynamic, try through an intermediate CRS
         // (we go here between ETRS89 and ETRS89-NO that would otherwise only
         // use NKG time-dependent transformations)
         (allRequiresPerCoordinateInputTime && !geodSrc->isDynamic() &&
          !geodDst->isDynamic()))) {
        // Currently triggered by "IG05/12 Intermediate CRS" to ITRF2014

        std::set<std::string> oSetNames;
        if (tooSmallAreas) {
            for (const auto &op : res) {
                oSetNames.insert(op->nameStr());
            }
        }

        auto resWithIntermediate = findsOpsInRegistryWithIntermediate(
            sourceCRS, targetCRS, context, true);
        if (tooSmallAreas && !res.empty()) {
            // Only insert operations we didn't already find
            for (const auto &op : resWithIntermediate) {
                if (oSetNames.find(op->nameStr()) == oSetNames.end()) {
                    res.emplace_back(op);
                }
            }
        } else {
            res.insert(res.end(), resWithIntermediate.begin(),
                       resWithIntermediate.end());
        }
        doFilterAndCheckPerfectOp = !res.empty();
    }

    if (doFilterAndCheckPerfectOp) {
        // If we get at least a result with perfect accuracy, do not bother
        // generating synthetic transforms.
        if (hasPerfectAccuracyResult(res, context)) {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

static std::vector<crs::CRSNNPtr>
findCandidateVertCRSForDatum(const io::AuthorityFactoryPtr &authFactory,
                             const datum::VerticalReferenceFrame *datum) {
    std::vector<crs::CRSNNPtr> candidates;
    assert(datum);
    const auto &ids = datum->identifiers();
    const auto &datumName = datum->nameStr();
    if (!ids.empty()) {
        for (const auto &id : ids) {
            const auto &authName = *(id->codeSpace());
            const auto &code = id->code();
            if (!authName.empty()) {
                auto l_candidates =
                    authFactory->createVerticalCRSFromDatum(authName, code);
                for (const auto &candidate : l_candidates) {
                    candidates.emplace_back(candidate);
                }
            }
        }
    } else if (datumName != "unknown" && datumName != "unnamed") {
        auto matches = authFactory->createObjectsFromName(
            datumName,
            {io::AuthorityFactory::ObjectType::VERTICAL_REFERENCE_FRAME}, false,
            2);
        if (matches.size() == 1) {
            const auto &match = matches.front();
            if (datum->_isEquivalentTo(
                    match.get(), util::IComparable::Criterion::EQUIVALENT,
                    authFactory->databaseContext().as_nullable()) &&
                !match->identifiers().empty()) {
                return findCandidateVertCRSForDatum(
                    authFactory,
                    dynamic_cast<const datum::VerticalReferenceFrame *>(
                        match.get()));
            }
        }
    }
    return candidates;
}

// ---------------------------------------------------------------------------

std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::Private::createOperationsGeogToVertFromGeoid(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const crs::VerticalCRS *vertDst, Private::Context &context) {

    ENTER_FUNCTION();

    const auto useTransf = [&sourceCRS, &targetCRS, &context,
                            vertDst](const CoordinateOperationNNPtr &op) {
        // If the source geographic CRS has a non-metre vertical unit, we need
        // to create an intermediate and operation to do the vertical unit
        // conversion from that vertical unit to the one of the geographic CRS
        // of the source of the operation
        const auto geogCRS =
            dynamic_cast<const crs::GeographicCRS *>(sourceCRS.get());
        assert(geogCRS);
        const auto &srcAxisList = geogCRS->coordinateSystem()->axisList();
        CoordinateOperationPtr opPtr;
        const auto opSourceCRSGeog =
            dynamic_cast<const crs::GeographicCRS *>(op->sourceCRS().get());
        // I assume opSourceCRSGeog should always be null in practice...
        if (opSourceCRSGeog && srcAxisList.size() == 3 &&
            srcAxisList[2]->unit().conversionToSI() != 1) {
            const auto &authFactory = context.context->getAuthorityFactory();
            const auto dbContext =
                authFactory ? authFactory->databaseContext().as_nullable()
                            : nullptr;
            auto tmpCRSWithSrcZ =
                opSourceCRSGeog->demoteTo2D(std::string(), dbContext)
                    ->promoteTo3D(std::string(), dbContext, srcAxisList[2]);

            std::vector<CoordinateOperationNNPtr> opsUnitConvert;
            createOperationsGeogToGeog(
                opsUnitConvert, tmpCRSWithSrcZ, NN_NO_CHECK(op->sourceCRS()),
                context,
                dynamic_cast<const crs::GeographicCRS *>(tmpCRSWithSrcZ.get()),
                opSourceCRSGeog, /*forceBallpark=*/false);
            assert(opsUnitConvert.size() == 1);
            opPtr = opsUnitConvert.front().as_nullable();
        }

        std::vector<CoordinateOperationNNPtr> ops;
        if (opPtr)
            ops.emplace_back(NN_NO_CHECK(opPtr));
        ops.emplace_back(op);

        const auto targetOp =
            dynamic_cast<const crs::VerticalCRS *>(op->targetCRS().get());
        assert(targetOp);
        if (targetOp->_isEquivalentTo(
                vertDst, util::IComparable::Criterion::EQUIVALENT)) {
            auto ret = ConcatenatedOperation::createComputeMetadata(
                ops, disallowEmptyIntersection);
            return ret;
        }
        std::vector<CoordinateOperationNNPtr> tmp;
        createOperationsVertToVert(NN_NO_CHECK(op->targetCRS()), targetCRS,
                                   context, targetOp, vertDst, tmp);
        assert(!tmp.empty());
        ops.emplace_back(tmp.front());
        auto ret = ConcatenatedOperation::createComputeMetadata(
            ops, disallowEmptyIntersection);
        return ret;
    };

    const auto getProjGeoidTransformation =
        [&sourceCRS, &targetCRS, &vertDst,
         &context](const CoordinateOperationNNPtr &model,
                   const std::string &projFilename) {
            const auto getNameVertCRSMetre = [](const std::string &name) {
                if (name.empty())
                    return std::string("unnamed");
                auto ret(name);
                bool haveOriginalUnit = false;
                if (name.back() == ')') {
                    const auto pos = ret.rfind(" (");
                    if (pos != std::string::npos) {
                        haveOriginalUnit = true;
                        ret = ret.substr(0, pos);
                    }
                }
                const auto pos = ret.rfind(" depth");
                if (pos != std::string::npos) {
                    ret = ret.substr(0, pos) + " height";
                }
                if (!haveOriginalUnit) {
                    ret += " (metre)";
                }
                return ret;
            };

            const auto &axis = vertDst->coordinateSystem()->axisList()[0];
            const auto &authFactory = context.context->getAuthorityFactory();
            const auto dbContext =
                authFactory ? authFactory->databaseContext().as_nullable()
                            : nullptr;

            const auto geogSrcCRS =
                dynamic_cast<crs::GeographicCRS *>(
                    model->interpolationCRS().get())
                    ? NN_NO_CHECK(model->interpolationCRS())
                    : sourceCRS->demoteTo2D(std::string(), dbContext)
                          ->promoteTo3D(std::string(), dbContext);
            const auto vertCRSMetre =
                axis->unit() == common::UnitOfMeasure::METRE &&
                        axis->direction() == cs::AxisDirection::UP
                    ? targetCRS
                    : util::nn_static_pointer_cast<crs::CRS>(
                          crs::VerticalCRS::create(
                              util::PropertyMap().set(
                                  common::IdentifiedObject::NAME_KEY,
                                  getNameVertCRSMetre(targetCRS->nameStr())),
                              vertDst->datum(), vertDst->datumEnsemble(),
                              cs::VerticalCS::createGravityRelatedHeight(
                                  common::UnitOfMeasure::METRE)));
            auto properties = util::PropertyMap().set(
                common::IdentifiedObject::NAME_KEY,
                buildOpName("Transformation", vertCRSMetre, geogSrcCRS));

            // Try to find a representative value for the accuracy of this grid
            // from the registered transformations.
            std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
            const auto &modelAccuracies =
                model->coordinateOperationAccuracies();
            std::vector<CoordinateOperationNNPtr> transformationsForGrid;
            if (authFactory) {
                transformationsForGrid =
                    io::DatabaseContext::getTransformationsForGridName(
                        authFactory->databaseContext(), projFilename);
            }

            // Only select transformations whose datum of the target vertical
            // CRS match the one of the target vertical CRS of interest (when
            // there's such match) Helps for example if specifying GEOID
            // g2012bp0 whose has a record for Puerto Rico and another one for
            // Virgin Islands.
            {
                std::vector<CoordinateOperationNNPtr>
                    transformationsForGridMatchingDatum;
                for (const auto &op : transformationsForGrid) {
                    const auto opTargetCRS =
                        dynamic_cast<const crs::VerticalCRS *>(
                            op->targetCRS().get());
                    if (opTargetCRS &&
                        opTargetCRS->datumNonNull(dbContext)->_isEquivalentTo(
                            vertDst->datumNonNull(dbContext).get(),
                            util::IComparable::Criterion::EQUIVALENT)) {
                        transformationsForGridMatchingDatum.push_back(op);
                    }
                }
                if (!transformationsForGridMatchingDatum.empty()) {
                    transformationsForGrid =
                        std::move(transformationsForGridMatchingDatum);
                }
            }

            double accuracy = -1;
            size_t idx = static_cast<size_t>(-1);
            if (modelAccuracies.empty()) {
                if (authFactory) {
                    for (size_t i = 0; i < transformationsForGrid.size(); ++i) {
                        const auto &transf = transformationsForGrid[i];
                        const double transfAcc = getAccuracy(transf);
                        if (transfAcc - accuracy > 1e-10) {
                            accuracy = transfAcc;
                            idx = i;
                        }
                    }
                    if (accuracy >= 0) {
                        accuracies.emplace_back(
                            metadata::PositionalAccuracy::create(
                                toString(accuracy)));
                    }
                }
            }

            // Set extent
            bool dummy = false;
            // Use in priority the one of the geoid model transformation
            auto extent = getExtent(model, true, dummy);
            // Otherwise fallback to the extent of a transformation using
            // the grid.
            if (extent == nullptr && authFactory != nullptr) {
                if (idx != static_cast<size_t>(-1)) {
                    const auto &transf = transformationsForGrid[idx];
                    extent = getExtent(transf, true, dummy);
                } else if (!transformationsForGrid.empty()) {
                    const auto &transf = transformationsForGrid.front();
                    extent = getExtent(transf, true, dummy);
                }
            }
            if (extent) {
                properties.set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                               NN_NO_CHECK(extent));
            }

            return Transformation::createGravityRelatedHeightToGeographic3D(
                properties, vertCRSMetre, geogSrcCRS, nullptr, projFilename,
                !modelAccuracies.empty() ? modelAccuracies : accuracies);
        };

    std::vector<CoordinateOperationNNPtr> res;
    const auto &authFactory = context.context->getAuthorityFactory();
    if (authFactory) {
        const auto &models = vertDst->geoidModel();
        for (const auto &model : models) {
            const auto &modelName = model->nameStr();
            const auto &modelIds = model->identifiers();
            const std::vector<CoordinateOperationNNPtr> transformations(
                !modelIds.empty()
                    ? std::vector<
                          CoordinateOperationNNPtr>{io::AuthorityFactory::create(
                                                        authFactory
                                                            ->databaseContext(),
                                                        *(modelIds[0]
                                                              ->codeSpace()))
                                                        ->createCoordinateOperation(
                                                            modelIds[0]->code(),
                                                            true)}
                : starts_with(modelName, "PROJ ")
                    ? std::vector<
                          CoordinateOperationNNPtr>{getProjGeoidTransformation(
                          model, modelName.substr(strlen("PROJ ")))}
                    : authFactory->getTransformationsForGeoid(
                          modelName,
                          context.context->getUsePROJAlternativeGridNames()));
            for (const auto &transf : transformations) {
                if (dynamic_cast<crs::GeographicCRS *>(
                        transf->sourceCRS().get()) &&
                    dynamic_cast<crs::VerticalCRS *>(
                        transf->targetCRS().get())) {
                    res.push_back(useTransf(transf));
                } else if (dynamic_cast<crs::GeographicCRS *>(
                               transf->targetCRS().get()) &&
                           dynamic_cast<crs::VerticalCRS *>(
                               transf->sourceCRS().get())) {
                    res.push_back(useTransf(transf->inverse()));
                }
            }
        }
    }

    return res;
}

// ---------------------------------------------------------------------------

std::vector<CoordinateOperationNNPtr> CoordinateOperationFactory::Private::
    createOperationsGeogToVertWithIntermediateVert(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        const crs::VerticalCRS *vertDst, Private::Context &context) {

    ENTER_FUNCTION();

    std::vector<CoordinateOperationNNPtr> res;

    struct AntiRecursionGuard {
        Context &context;

        explicit AntiRecursionGuard(Context &contextIn) : context(contextIn) {
            assert(!context.inCreateOperationsGeogToVertWithIntermediateVert);
            context.inCreateOperationsGeogToVertWithIntermediateVert = true;
        }

        ~AntiRecursionGuard() {
            context.inCreateOperationsGeogToVertWithIntermediateVert = false;
        }
    };
    AntiRecursionGuard guard(context);
    const auto &authFactory = context.context->getAuthorityFactory();
    const auto dbContext = authFactory->databaseContext().as_nullable();

    auto candidatesVert = findCandidateVertCRSForDatum(
        authFactory, vertDst->datumNonNull(dbContext).get());
    for (const auto &candidateVert : candidatesVert) {
        auto resTmp = createOperations(sourceCRS, sourceEpoch, candidateVert,
                                       sourceEpoch, context);
        if (!resTmp.empty()) {
            const auto opsSecond = createOperations(
                candidateVert, sourceEpoch, targetCRS, targetEpoch, context);
            if (!opsSecond.empty()) {
                // The transformation from candidateVert to targetCRS should
                // be just a unit change typically, so take only the first one,
                // which is likely/hopefully the only one.
                for (const auto &opFirst : resTmp) {
                    if (hasIdentifiers(opFirst)) {
                        if (candidateVert->_isEquivalentTo(
                                targetCRS.get(),
                                util::IComparable::Criterion::EQUIVALENT)) {
                            res.emplace_back(opFirst);
                        } else {
                            res.emplace_back(
                                ConcatenatedOperation::createComputeMetadata(
                                    {opFirst, opsSecond.front()},
                                    disallowEmptyIntersection));
                        }
                    }
                }
                if (!res.empty())
                    break;
            }
        }
    }

    return res;
}

// ---------------------------------------------------------------------------

std::vector<CoordinateOperationNNPtr> CoordinateOperationFactory::Private::
    createOperationsGeogToVertWithAlternativeGeog(
        const crs::CRSNNPtr &sourceCRS, // geographic CRS
        const crs::CRSNNPtr &targetCRS, // vertical CRS
        Private::Context &context) {

    ENTER_FUNCTION();

    std::vector<CoordinateOperationNNPtr> res;

    struct AntiRecursionGuard {
        Context &context;

        explicit AntiRecursionGuard(Context &contextIn) : context(contextIn) {
            assert(!context.inCreateOperationsGeogToVertWithAlternativeGeog);
            context.inCreateOperationsGeogToVertWithAlternativeGeog = true;
        }

        ~AntiRecursionGuard() {
            context.inCreateOperationsGeogToVertWithAlternativeGeog = false;
        }
    };
    AntiRecursionGuard guard(context);

    // Generally EPSG has operations from GeogCrs to VertCRS
    auto ops = findOpsInRegistryDirectTo(targetCRS, context);

    const auto geogCRS =
        dynamic_cast<const crs::GeographicCRS *>(sourceCRS.get());
    assert(geogCRS);
    const auto &srcAxisList = geogCRS->coordinateSystem()->axisList();
    for (const auto &op : ops) {
        const auto tmpCRS =
            dynamic_cast<const crs::GeographicCRS *>(op->sourceCRS().get());
        if (tmpCRS) {
            if (srcAxisList.size() == 3 &&
                srcAxisList[2]->unit().conversionToSI() != 1) {

                const auto &authFactory =
                    context.context->getAuthorityFactory();
                const auto dbContext =
                    authFactory->databaseContext().as_nullable();
                auto tmpCRSWithSrcZ =
                    tmpCRS->demoteTo2D(std::string(), dbContext)
                        ->promoteTo3D(std::string(), dbContext, srcAxisList[2]);

                std::vector<CoordinateOperationNNPtr> opsUnitConvert;
                createOperationsGeogToGeog(
                    opsUnitConvert, tmpCRSWithSrcZ,
                    NN_NO_CHECK(op->sourceCRS()), context,
                    dynamic_cast<const crs::GeographicCRS *>(
                        tmpCRSWithSrcZ.get()),
                    tmpCRS, /*forceBallpark=*/false);
                assert(opsUnitConvert.size() == 1);
                auto concat = ConcatenatedOperation::createComputeMetadata(
                    {opsUnitConvert.front(), op}, disallowEmptyIntersection);
                res.emplace_back(concat);
            } else {
                res.emplace_back(op);
            }
        }
    }

    return res;
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::
    createOperationsFromDatabaseWithVertCRS(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::GeographicCRS *geogSrc,
        const crs::GeographicCRS *geogDst, const crs::VerticalCRS *vertSrc,
        const crs::VerticalCRS *vertDst,
        std::vector<CoordinateOperationNNPtr> &res) {

    // Typically to transform from "NAVD88 height (ftUS)" to a geog CRS
    // by using transformations of "NAVD88 height" (metre) to that geog CRS
    if (res.empty() &&
        !context.inCreateOperationsGeogToVertWithIntermediateVert && geogSrc &&
        vertDst) {
        res = createOperationsGeogToVertWithIntermediateVert(
            sourceCRS, sourceEpoch, targetCRS, targetEpoch, vertDst, context);
    } else if (res.empty() &&
               !context.inCreateOperationsGeogToVertWithIntermediateVert &&
               geogDst && vertSrc) {
        res = applyInverse(createOperationsGeogToVertWithIntermediateVert(
            targetCRS, targetEpoch, sourceCRS, sourceEpoch, vertSrc, context));
    }

    // NAD83 only exists in 2D version in EPSG, so if it has been
    // promoted to 3D, when researching a vertical to geog
    // transformation, try to down cast to 2D.
    const auto geog3DToVertTryThroughGeog2D =
        [&res, &context](const crs::GeographicCRS *geogSrcIn,
                         const crs::VerticalCRS *vertDstIn,
                         const crs::CRSNNPtr &targetCRSIn) {
            const auto &authFactory = context.context->getAuthorityFactory();
            if (res.empty() && geogSrcIn && vertDstIn && authFactory &&
                geogSrcIn->coordinateSystem()->axisList().size() == 3) {
                const auto &dbContext = authFactory->databaseContext();
                const auto candidatesSrcGeod(findCandidateGeodCRSForDatum(
                    authFactory, geogSrcIn,
                    geogSrcIn->datumNonNull(dbContext)));
                for (const auto &candidate : candidatesSrcGeod) {
                    auto geogCandidate =
                        util::nn_dynamic_pointer_cast<crs::GeographicCRS>(
                            candidate);
                    if (geogCandidate &&
                        geogCandidate->coordinateSystem()->axisList().size() ==
                            2) {
                        bool ignored;
                        res = findOpsInRegistryDirect(
                            NN_NO_CHECK(geogCandidate), targetCRSIn, context,
                            ignored);
                        break;
                    }
                }
                return true;
            }
            return false;
        };

    if (geog3DToVertTryThroughGeog2D(geogSrc, vertDst, targetCRS)) {
        // do nothing
    } else if (geog3DToVertTryThroughGeog2D(geogDst, vertSrc, sourceCRS)) {
        res = applyInverse(res);
    }

    // There's no direct transformation from NAVD88 height to WGS84,
    // so try to research all transformations from NAVD88 to another
    // intermediate GeographicCRS.
    if (res.empty() &&
        !context.inCreateOperationsGeogToVertWithAlternativeGeog && geogSrc &&
        vertDst) {
        res = createOperationsGeogToVertWithAlternativeGeog(sourceCRS,
                                                            targetCRS, context);
    } else if (res.empty() &&
               !context.inCreateOperationsGeogToVertWithAlternativeGeog &&
               geogDst && vertSrc) {
        res = applyInverse(createOperationsGeogToVertWithAlternativeGeog(
            targetCRS, sourceCRS, context));
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsGeodToGeod(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    Private::Context &context, const crs::GeodeticCRS *geodSrc,
    const crs::GeodeticCRS *geodDst, std::vector<CoordinateOperationNNPtr> &res,
    bool forceBallpark) {

    ENTER_FUNCTION();

    const auto &srcEllps = geodSrc->ellipsoid();
    const auto &dstEllps = geodDst->ellipsoid();
    if (srcEllps->celestialBody() == dstEllps->celestialBody() &&
        srcEllps->celestialBody() != NON_EARTH_BODY) {
        // Same celestial body, with a known name (that is not the generic
        // NON_EARTH_BODY used when we can't guess it) ==> compatible
    } else if ((srcEllps->celestialBody() != dstEllps->celestialBody() &&
                srcEllps->celestialBody() != NON_EARTH_BODY &&
                dstEllps->celestialBody() != NON_EARTH_BODY) ||
               std::fabs(srcEllps->semiMajorAxis().getSIValue() -
                         dstEllps->semiMajorAxis().getSIValue()) >
                   REL_ERROR_FOR_SAME_CELESTIAL_BODY *
                       dstEllps->semiMajorAxis().getSIValue()) {
        const char *envVarVal = getenv("PROJ_IGNORE_CELESTIAL_BODY");
        if (envVarVal == nullptr || ci_equal(envVarVal, "NO") ||
            ci_equal(envVarVal, "FALSE") || ci_equal(envVarVal, "OFF")) {
            std::string osMsg(
                "Source and target ellipsoid do not belong to the same "
                "celestial body (");
            osMsg += srcEllps->celestialBody();
            osMsg += " vs ";
            osMsg += dstEllps->celestialBody();
            osMsg += ").";
            if (envVarVal == nullptr) {
                osMsg += " You may override this check by setting the "
                         "PROJ_IGNORE_CELESTIAL_BODY environment variable "
                         "to YES.";
            }
            throw util::UnsupportedOperationException(osMsg.c_str());
        }
    }

    auto geogSrc = dynamic_cast<const crs::GeographicCRS *>(geodSrc);
    auto geogDst = dynamic_cast<const crs::GeographicCRS *>(geodDst);

    if (geogSrc && geogDst) {
        createOperationsGeogToGeog(res, sourceCRS, targetCRS, context, geogSrc,
                                   geogDst, forceBallpark);
        return;
    }

    const bool isSrcGeocentric = geodSrc->isGeocentric();
    const bool isSrcGeographic = geogSrc != nullptr;
    const bool isTargetGeocentric = geodDst->isGeocentric();
    const bool isTargetGeographic = geogDst != nullptr;

    const auto IsSameDatum = [&context, &geodSrc, &geodDst]() {
        const auto &authFactory = context.context->getAuthorityFactory();
        const auto dbContext =
            authFactory ? authFactory->databaseContext().as_nullable()
                        : nullptr;

        return geodSrc->datumNonNull(dbContext)->_isEquivalentTo(
            geodDst->datumNonNull(dbContext).get(),
            util::IComparable::Criterion::EQUIVALENT);
    };

    if (((isSrcGeocentric && isTargetGeographic) ||
         (isSrcGeographic && isTargetGeocentric))) {

        // Same datum ?
        if (IsSameDatum()) {
            if (forceBallpark) {
                auto op = createGeodToGeodPROJBased(sourceCRS, targetCRS);
                op->setHasBallparkTransformation(true);
                res.emplace_back(op);
            } else {
                res.emplace_back(Conversion::createGeographicGeocentric(
                    sourceCRS, targetCRS));
            }
        } else if (isSrcGeocentric && geogDst) {
#if 0
            // The below logic was used between PROJ >= 6.0 and < 9.2
            // It assumed that the geocentric origin of the 2 datums
            // matched.
            std::string interm_crs_name(geogDst->nameStr());
            interm_crs_name += " (geocentric)";
            auto interm_crs =
                util::nn_static_pointer_cast<crs::CRS>(crs::GeodeticCRS::create(
                    addDomains(util::PropertyMap().set(
                                   common::IdentifiedObject::NAME_KEY,
                                   interm_crs_name),
                               geogDst),
                    geogDst->datum(), geogDst->datumEnsemble(),
                    NN_CHECK_ASSERT(
                        util::nn_dynamic_pointer_cast<cs::CartesianCS>(
                            geodSrc->coordinateSystem()))));
            auto opFirst =
                createBallparkGeocentricTranslation(sourceCRS, interm_crs);
            auto opSecond =
                Conversion::createGeographicGeocentric(interm_crs, targetCRS);
#else
            // The below logic is used since PROJ >= 9.2. It emulates the
            // behavior of PROJ < 6 by converting from the source geocentric CRS
            // to its corresponding geographic CRS, and then doing a null
            // geographic offset between that CRS and the target geographic CRS
            std::string interm_crs_name(geodSrc->nameStr());
            interm_crs_name += " (geographic)";
            auto interm_crs = util::nn_static_pointer_cast<crs::CRS>(
                crs::GeographicCRS::create(
                    addDomains(util::PropertyMap().set(
                                   common::IdentifiedObject::NAME_KEY,
                                   interm_crs_name),
                               geodSrc),
                    geodSrc->datum(), geodSrc->datumEnsemble(),
                    cs::EllipsoidalCS::createLongitudeLatitudeEllipsoidalHeight(
                        common::UnitOfMeasure::DEGREE,
                        common::UnitOfMeasure::METRE)));
            auto opFirst =
                Conversion::createGeographicGeocentric(sourceCRS, interm_crs);
            auto opsSecond = createOperations(
                interm_crs, util::optional<common::DataEpoch>(), targetCRS,
                util::optional<common::DataEpoch>(), context);
            for (const auto &opSecond : opsSecond) {
                try {
                    res.emplace_back(
                        ConcatenatedOperation::createComputeMetadata(
                            {opFirst, opSecond}, disallowEmptyIntersection));
                } catch (const InvalidOperationEmptyIntersection &) {
                }
            }
#endif
        } else {
            // Apply previous case in reverse way
            std::vector<CoordinateOperationNNPtr> resTmp;
            createOperationsGeodToGeod(targetCRS, sourceCRS, context, geodDst,
                                       geodSrc, resTmp, forceBallpark);
            resTmp = applyInverse(resTmp);
            res.insert(res.end(), resTmp.begin(), resTmp.end());
        }

        return;
    }

    if (isSrcGeocentric && isTargetGeocentric) {
        if (!forceBallpark &&
            (sourceCRS->_isEquivalentTo(
                 targetCRS.get(), util::IComparable::Criterion::EQUIVALENT) ||
             IsSameDatum())) {
            std::string name(NULL_GEOCENTRIC_TRANSLATION);
            name += " from ";
            name += sourceCRS->nameStr();
            name += " to ";
            name += targetCRS->nameStr();
            res.emplace_back(Transformation::createGeocentricTranslations(
                util::PropertyMap()
                    .set(common::IdentifiedObject::NAME_KEY, name)
                    .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                         metadata::Extent::WORLD),
                sourceCRS, targetCRS, 0.0, 0.0, 0.0,
                {metadata::PositionalAccuracy::create("0")}));
        } else {
            res.emplace_back(
                createBallparkGeocentricTranslation(sourceCRS, targetCRS));
        }
        return;
    }

    // Transformation between two geodetic systems of unknown type
    // This should normally not be triggered with "standard" CRS
    res.emplace_back(createGeodToGeodPROJBased(sourceCRS, targetCRS));
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::
    createOperationsFromSphericalPlanetocentric(
        const crs::CRSNNPtr &sourceCRS,
        const util::optional<common::DataEpoch> &sourceEpoch,
        const crs::CRSNNPtr &targetCRS,
        const util::optional<common::DataEpoch> &targetEpoch,
        Private::Context &context, const crs::GeodeticCRS *geodSrc,
        std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    const auto IsSameDatum = [&context,
                              &geodSrc](const crs::GeodeticCRS *geodDst) {
        const auto &authFactory = context.context->getAuthorityFactory();
        const auto dbContext =
            authFactory ? authFactory->databaseContext().as_nullable()
                        : nullptr;

        return geodSrc->datumNonNull(dbContext)->_isEquivalentTo(
            geodDst->datumNonNull(dbContext).get(),
            util::IComparable::Criterion::EQUIVALENT);
    };
    auto geogDst = dynamic_cast<const crs::GeographicCRS *>(targetCRS.get());
    if (geogDst && IsSameDatum(geogDst)) {
        res.emplace_back(Conversion::createGeographicGeocentricLatitude(
            sourceCRS, targetCRS));
        return;
    }

    // Create an intermediate geographic CRS with the same datum as the
    // source spherical planetocentric one
    std::string interm_crs_name(geodSrc->nameStr());
    interm_crs_name += " (geographic)";
    auto interm_crs =
        util::nn_static_pointer_cast<crs::CRS>(crs::GeographicCRS::create(
            addDomains(util::PropertyMap().set(
                           common::IdentifiedObject::NAME_KEY, interm_crs_name),
                       geodSrc),
            geodSrc->datum(), geodSrc->datumEnsemble(),
            cs::EllipsoidalCS::createLatitudeLongitude(
                common::UnitOfMeasure::DEGREE)));

    auto opFirst =
        Conversion::createGeographicGeocentricLatitude(sourceCRS, interm_crs);
    auto opsSecond = createOperations(interm_crs, sourceEpoch, targetCRS,
                                      targetEpoch, context);
    for (const auto &opSecond : opsSecond) {
        try {
            res.emplace_back(ConcatenatedOperation::createComputeMetadata(
                {opFirst, opSecond}, disallowEmptyIntersection));
        } catch (const InvalidOperationEmptyIntersection &) {
        }
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::
    createOperationsFromBoundOfSphericalPlanetocentric(
        const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
        Private::Context &context, const crs::BoundCRS *boundSrc,
        const crs::GeodeticCRSNNPtr &geodSrcBase,
        std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    // Create an intermediate geographic CRS with the same datum as the
    // source spherical planetocentric one
    std::string interm_crs_name(geodSrcBase->nameStr());
    interm_crs_name += " (geographic)";
    auto intermGeog =
        util::nn_static_pointer_cast<crs::CRS>(crs::GeographicCRS::create(
            addDomains(util::PropertyMap().set(
                           common::IdentifiedObject::NAME_KEY, interm_crs_name),
                       geodSrcBase.get()),
            geodSrcBase->datum(), geodSrcBase->datumEnsemble(),
            cs::EllipsoidalCS::createLatitudeLongitude(
                common::UnitOfMeasure::DEGREE)));

    // Create an intermediate boundCRS wrapping the above intermediate
    // geographic CRS
    auto transf = boundSrc->transformation()->shallowClone();
    // keep a reference to the target before patching it with itself
    // (this is due to our abuse of passing shared_ptr by reference
    auto transfTarget = transf->targetCRS();
    setCRSs(transf.get(), intermGeog, transfTarget);

    auto intermBoundCRS =
        crs::BoundCRS::create(intermGeog, boundSrc->hubCRS(), transf);

    auto opFirst =
        Conversion::createGeographicGeocentricLatitude(geodSrcBase, intermGeog);
    setCRSs(opFirst.get(), sourceCRS, intermBoundCRS);
    auto opsSecond = createOperations(
        intermBoundCRS, util::optional<common::DataEpoch>(), targetCRS,
        util::optional<common::DataEpoch>(), context);
    for (const auto &opSecond : opsSecond) {
        try {
            auto opSecondClone = opSecond->shallowClone();
            // In theory, we should not need that setCRSs() forcing, but due
            // how BoundCRS transformations are implemented currently, we
            // need it in practice.
            setCRSs(opSecondClone.get(), intermBoundCRS, targetCRS);
            res.emplace_back(ConcatenatedOperation::createComputeMetadata(
                {opFirst, std::move(opSecondClone)},
                disallowEmptyIntersection));
        } catch (const InvalidOperationEmptyIntersection &) {
        }
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsDerivedTo(
    const crs::CRSNNPtr & /*sourceCRS*/,
    const util::optional<common::DataEpoch> &sourceEpoch,
    const crs::CRSNNPtr &targetCRS,
    const util::optional<common::DataEpoch> &targetEpoch,
    Private::Context &context, const crs::DerivedCRS *derivedSrc,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    auto opFirst = derivedSrc->derivingConversion()->inverse();
    // Small optimization if the targetCRS is the baseCRS of the source
    // derivedCRS.
    if (derivedSrc->baseCRS()->_isEquivalentTo(
            targetCRS.get(), util::IComparable::Criterion::EQUIVALENT)) {
        res.emplace_back(opFirst);
        return;
    }

    auto opsSecond = createOperations(derivedSrc->baseCRS(), sourceEpoch,
                                      targetCRS, targetEpoch, context);

    // Optimization to remove a no-op "Conversion from WGS 84 to WGS 84"
    // when transforming from EPSG:4979 to a CompoundCRS whose vertical CRS
    // is a DerivedVerticalCRS of a datum with ellipsoid height.
    if (opsSecond.size() == 1 &&
        !opsSecond.front()->hasBallparkTransformation() &&
        dynamic_cast<const crs::VerticalCRS *>(derivedSrc->baseCRS().get()) &&
        !dynamic_cast<const crs::DerivedCRS *>(derivedSrc->baseCRS().get()) &&
        dynamic_cast<const crs::GeographicCRS *>(targetCRS.get()) &&
        !dynamic_cast<const crs::DerivedCRS *>(targetCRS.get())) {
        auto conv = dynamic_cast<const Conversion *>(opsSecond.front().get());
        if (conv &&
            conv->nameStr() ==
                buildConvName(targetCRS->nameStr(), targetCRS->nameStr()) &&
            conv->method()->getEPSGCode() ==
                EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT &&
            conv->parameterValueNumericAsSI(
                EPSG_CODE_PARAMETER_UNIT_CONVERSION_SCALAR) == 1.0) {
            res.emplace_back(opFirst);
            return;
        }
    }

    for (const auto &opSecond : opsSecond) {
        try {
            res.emplace_back(ConcatenatedOperation::createComputeMetadata(
                {opFirst, opSecond}, disallowEmptyIntersection));
        } catch (const InvalidOperationEmptyIntersection &) {
        }
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsBoundToGeog(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    Private::Context &context, const crs::BoundCRS *boundSrc,
    const crs::GeographicCRS *geogDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    const auto &hubSrc = boundSrc->hubCRS();
    auto hubSrcGeog = dynamic_cast<const crs::GeographicCRS *>(hubSrc.get());
    auto geogCRSOfBaseOfBoundSrc = boundSrc->baseCRS()->extractGeographicCRS();
    {
        // If geogCRSOfBaseOfBoundSrc is a DerivedGeographicCRS, use its base
        // instead (if it is a GeographicCRS)
        auto derivedGeogCRS =
            std::dynamic_pointer_cast<crs::DerivedGeographicCRS>(
                geogCRSOfBaseOfBoundSrc);
        if (derivedGeogCRS) {
            auto baseCRS = std::dynamic_pointer_cast<crs::GeographicCRS>(
                derivedGeogCRS->baseCRS().as_nullable());
            if (baseCRS) {
                geogCRSOfBaseOfBoundSrc = std::move(baseCRS);
            }
        }
    }

    const auto &authFactory = context.context->getAuthorityFactory();
    const auto dbContext =
        authFactory ? authFactory->databaseContext().as_nullable() : nullptr;

    const auto geogDstDatum = geogDst->datumNonNull(dbContext);

    // If the underlying datum of the source is the same as the target, do
    // not consider the boundCRS at all, but just its base
    if (geogCRSOfBaseOfBoundSrc) {
        auto geogCRSOfBaseOfBoundSrcDatum =
            geogCRSOfBaseOfBoundSrc->datumNonNull(dbContext);
        if (geogCRSOfBaseOfBoundSrcDatum->_isEquivalentTo(
                geogDstDatum.get(), util::IComparable::Criterion::EQUIVALENT,
                dbContext)) {
            res = createOperations(
                boundSrc->baseCRS(), util::optional<common::DataEpoch>(),
                targetCRS, util::optional<common::DataEpoch>(), context);
            return;
        }
    }

    bool triedBoundCrsToGeogCRSSameAsHubCRS = false;
    // Is it: boundCRS to a geogCRS that is the same as the hubCRS ?
    if (hubSrcGeog && geogCRSOfBaseOfBoundSrc &&
        (hubSrcGeog->_isEquivalentTo(
             geogDst, util::IComparable::Criterion::EQUIVALENT) ||
         hubSrcGeog->is2DPartOf3D(NN_NO_CHECK(geogDst), dbContext))) {
        triedBoundCrsToGeogCRSSameAsHubCRS = true;

        CoordinateOperationPtr opIntermediate;
        if (!geogCRSOfBaseOfBoundSrc->_isEquivalentTo(
                boundSrc->transformation()->sourceCRS().get(),
                util::IComparable::Criterion::EQUIVALENT)) {
            auto opsIntermediate =
                createOperations(NN_NO_CHECK(geogCRSOfBaseOfBoundSrc),
                                 util::optional<common::DataEpoch>(),
                                 boundSrc->transformation()->sourceCRS(),
                                 util::optional<common::DataEpoch>(), context);
            assert(!opsIntermediate.empty());
            opIntermediate = opsIntermediate.front();
        }

        if (boundSrc->baseCRS() == geogCRSOfBaseOfBoundSrc) {
            if (opIntermediate) {
                try {
                    res.emplace_back(
                        ConcatenatedOperation::createComputeMetadata(
                            {NN_NO_CHECK(opIntermediate),
                             boundSrc->transformation()},
                            disallowEmptyIntersection));
                } catch (const InvalidOperationEmptyIntersection &) {
                }
            } else {
                // Optimization to avoid creating a useless concatenated
                // operation
                res.emplace_back(boundSrc->transformation());
            }
            return;
        }
        auto opsFirst = createOperations(
            boundSrc->baseCRS(), util::optional<common::DataEpoch>(),
            NN_NO_CHECK(geogCRSOfBaseOfBoundSrc),
            util::optional<common::DataEpoch>(), context);
        if (!opsFirst.empty()) {
            for (const auto &opFirst : opsFirst) {
                try {
                    std::vector<CoordinateOperationNNPtr> subops;
                    subops.emplace_back(opFirst);
                    if (opIntermediate) {
                        subops.emplace_back(NN_NO_CHECK(opIntermediate));
                    }
                    subops.emplace_back(boundSrc->transformation());
                    res.emplace_back(
                        ConcatenatedOperation::createComputeMetadata(
                            subops, disallowEmptyIntersection));
                } catch (const InvalidOperationEmptyIntersection &) {
                }
            }
            if (!res.empty()) {
                return;
            }
        }
        // If the datum are equivalent, this is also fine
    } else if (geogCRSOfBaseOfBoundSrc && hubSrcGeog &&
               hubSrcGeog->datumNonNull(dbContext)->_isEquivalentTo(
                   geogDstDatum.get(),
                   util::IComparable::Criterion::EQUIVALENT)) {
        auto opsFirst = createOperations(
            boundSrc->baseCRS(), util::optional<common::DataEpoch>(),
            NN_NO_CHECK(geogCRSOfBaseOfBoundSrc),
            util::optional<common::DataEpoch>(), context);
        auto opsLast = createOperations(
            hubSrc, util::optional<common::DataEpoch>(), targetCRS,
            util::optional<common::DataEpoch>(), context);
        if (!opsFirst.empty() && !opsLast.empty()) {
            CoordinateOperationPtr opIntermediate;
            if (!geogCRSOfBaseOfBoundSrc->_isEquivalentTo(
                    boundSrc->transformation()->sourceCRS().get(),
                    util::IComparable::Criterion::EQUIVALENT)) {
                auto opsIntermediate = createOperations(
                    NN_NO_CHECK(geogCRSOfBaseOfBoundSrc),
                    util::optional<common::DataEpoch>(),
                    boundSrc->transformation()->sourceCRS(),
                    util::optional<common::DataEpoch>(), context);
                assert(!opsIntermediate.empty());
                opIntermediate = opsIntermediate.front();
            }
            for (const auto &opFirst : opsFirst) {
                for (const auto &opLast : opsLast) {
                    try {
                        std::vector<CoordinateOperationNNPtr> subops;
                        subops.emplace_back(opFirst);
                        if (opIntermediate) {
                            subops.emplace_back(NN_NO_CHECK(opIntermediate));
                        }
                        subops.emplace_back(boundSrc->transformation());
                        subops.emplace_back(opLast);
                        res.emplace_back(
                            ConcatenatedOperation::createComputeMetadata(
                                subops, disallowEmptyIntersection));
                    } catch (const InvalidOperationEmptyIntersection &) {
                    }
                }
            }
            if (!res.empty()) {
                return;
            }
        }
        // Consider WGS 84 and NAD83 as equivalent in that context if the
        // geogCRSOfBaseOfBoundSrc ellipsoid is Clarke66 (for NAD27)
        // Case of "+proj=latlong +ellps=clrk66
        // +nadgrids=ntv1_can.dat,conus"
        // to "+proj=latlong +datum=NAD83"
    } else if (geogCRSOfBaseOfBoundSrc && hubSrcGeog &&
               geogCRSOfBaseOfBoundSrc->ellipsoid()->_isEquivalentTo(
                   datum::Ellipsoid::CLARKE_1866.get(),
                   util::IComparable::Criterion::EQUIVALENT, dbContext) &&
               hubSrcGeog->datumNonNull(dbContext)->_isEquivalentTo(
                   datum::GeodeticReferenceFrame::EPSG_6326.get(),
                   util::IComparable::Criterion::EQUIVALENT, dbContext) &&
               geogDstDatum->_isEquivalentTo(
                   datum::GeodeticReferenceFrame::EPSG_6269.get(),
                   util::IComparable::Criterion::EQUIVALENT, dbContext)) {
        auto nnGeogCRSOfBaseOfBoundSrc = NN_NO_CHECK(geogCRSOfBaseOfBoundSrc);
        if (boundSrc->baseCRS()->_isEquivalentTo(
                nnGeogCRSOfBaseOfBoundSrc.get(),
                util::IComparable::Criterion::EQUIVALENT)) {
            auto transf = boundSrc->transformation()->shallowClone();
            transf->setProperties(util::PropertyMap().set(
                common::IdentifiedObject::NAME_KEY,
                buildTransfName(boundSrc->baseCRS()->nameStr(),
                                targetCRS->nameStr())));
            transf->setCRSs(boundSrc->baseCRS(), targetCRS, nullptr);
            res.emplace_back(transf);
            return;
        } else {
            auto opsFirst = createOperations(
                boundSrc->baseCRS(), util::optional<common::DataEpoch>(),
                nnGeogCRSOfBaseOfBoundSrc, util::optional<common::DataEpoch>(),
                context);
            auto transf = boundSrc->transformation()->shallowClone();
            transf->setProperties(util::PropertyMap().set(
                common::IdentifiedObject::NAME_KEY,
                buildTransfName(nnGeogCRSOfBaseOfBoundSrc->nameStr(),
                                targetCRS->nameStr())));
            transf->setCRSs(nnGeogCRSOfBaseOfBoundSrc, targetCRS, nullptr);
            if (!opsFirst.empty()) {
                for (const auto &opFirst : opsFirst) {
                    try {
                        res.emplace_back(
                            ConcatenatedOperation::createComputeMetadata(
                                {opFirst, transf}, disallowEmptyIntersection));
                    } catch (const InvalidOperationEmptyIntersection &) {
                    }
                }
                if (!res.empty()) {
                    return;
                }
            }
        }
    }

    if (hubSrcGeog &&
        hubSrcGeog->_isEquivalentTo(geogDst,
                                    util::IComparable::Criterion::EQUIVALENT) &&
        dynamic_cast<const crs::VerticalCRS *>(boundSrc->baseCRS().get())) {
        auto transfSrc = boundSrc->transformation()->sourceCRS();
        if (dynamic_cast<const crs::VerticalCRS *>(transfSrc.get()) &&
            !boundSrc->baseCRS()->_isEquivalentTo(
                transfSrc.get(), util::IComparable::Criterion::EQUIVALENT)) {
            auto opsFirst = createOperations(
                boundSrc->baseCRS(), util::optional<common::DataEpoch>(),
                transfSrc, util::optional<common::DataEpoch>(), context);
            for (const auto &opFirst : opsFirst) {
                try {
                    res.emplace_back(
                        ConcatenatedOperation::createComputeMetadata(
                            {opFirst, boundSrc->transformation()},
                            disallowEmptyIntersection));
                } catch (const InvalidOperationEmptyIntersection &) {
                }
            }
            return;
        }

        res.emplace_back(boundSrc->transformation());
        return;
    }

    if (!triedBoundCrsToGeogCRSSameAsHubCRS && hubSrcGeog &&
        geogCRSOfBaseOfBoundSrc) {
        // This one should go to the above 'Is it: boundCRS to a geogCRS
        // that is the same as the hubCRS ?' case
        auto opsFirst = createOperations(
            sourceCRS, util::optional<common::DataEpoch>(), hubSrc,
            util::optional<common::DataEpoch>(), context);
        auto opsLast = createOperations(
            hubSrc, util::optional<common::DataEpoch>(), targetCRS,
            util::optional<common::DataEpoch>(), context);
        if (!opsFirst.empty() && !opsLast.empty()) {
            for (const auto &opFirst : opsFirst) {
                for (const auto &opLast : opsLast) {
                    // Exclude artificial transformations from the hub
                    // to the target CRS, if it is the only one.
                    if (opsLast.size() > 1 ||
                        !opLast->hasBallparkTransformation()) {
                        try {
                            res.emplace_back(
                                ConcatenatedOperation::createComputeMetadata(
                                    {opFirst, opLast},
                                    disallowEmptyIntersection));
                        } catch (const InvalidOperationEmptyIntersection &) {
                        }
                    } else {
                        // std::cerr << "excluded " << opLast->nameStr() <<
                        // std::endl;
                    }
                }
            }
            if (!res.empty()) {
                return;
            }
        }
    }

    auto vertCRSOfBaseOfBoundSrc =
        dynamic_cast<const crs::VerticalCRS *>(boundSrc->baseCRS().get());
    // The test for hubSrcGeog not being a DerivedCRS is to avoid infinite
    // recursion in a scenario involving a
    // BoundCRS[SourceCRS[VertCRS],TargetCRS[DerivedGeographicCRS]] to a
    // GeographicCRS
    if (vertCRSOfBaseOfBoundSrc && hubSrcGeog &&
        dynamic_cast<const crs::DerivedCRS *>(hubSrcGeog) == nullptr) {
        auto opsFirst = createOperations(
            sourceCRS, util::optional<common::DataEpoch>(), hubSrc,
            util::optional<common::DataEpoch>(), context);
        if (context.skipHorizontalTransformation) {
            if (!opsFirst.empty()) {
                const auto &hubAxisList =
                    hubSrcGeog->coordinateSystem()->axisList();
                const auto &targetAxisList =
                    geogDst->coordinateSystem()->axisList();
                if (hubAxisList.size() == 3 && targetAxisList.size() == 3 &&
                    !hubAxisList[2]->_isEquivalentTo(
                        targetAxisList[2].get(),
                        util::IComparable::Criterion::EQUIVALENT)) {

                    const auto &srcAxis = hubAxisList[2];
                    const double convSrc = srcAxis->unit().conversionToSI();
                    const auto &dstAxis = targetAxisList[2];
                    const double convDst = dstAxis->unit().conversionToSI();
                    const bool srcIsUp =
                        srcAxis->direction() == cs::AxisDirection::UP;
                    const bool srcIsDown =
                        srcAxis->direction() == cs::AxisDirection::DOWN;
                    const bool dstIsUp =
                        dstAxis->direction() == cs::AxisDirection::UP;
                    const bool dstIsDown =
                        dstAxis->direction() == cs::AxisDirection::DOWN;
                    const bool heightDepthReversal =
                        ((srcIsUp && dstIsDown) || (srcIsDown && dstIsUp));

                    if (convDst == 0)
                        throw InvalidOperation(
                            "Conversion factor of target unit is 0");
                    const double factor = convSrc / convDst;
                    auto conv = Conversion::createChangeVerticalUnit(
                        util::PropertyMap().set(
                            common::IdentifiedObject::NAME_KEY,
                            "Change of vertical unit"),
                        common::Scale(heightDepthReversal ? -factor : factor));

                    conv->setCRSs(
                        hubSrc,
                        hubSrc->demoteTo2D(std::string(), dbContext)
                            ->promoteTo3D(std::string(), dbContext, dstAxis),
                        nullptr);

                    for (const auto &op : opsFirst) {
                        try {
                            res.emplace_back(
                                ConcatenatedOperation::createComputeMetadata(
                                    {op, conv}, disallowEmptyIntersection));
                        } catch (const InvalidOperationEmptyIntersection &) {
                        }
                    }
                } else {
                    res = std::move(opsFirst);
                }
            }
            return;
        } else {
            auto opsSecond = createOperations(
                hubSrc, util::optional<common::DataEpoch>(), targetCRS,
                util::optional<common::DataEpoch>(), context);
            if (!opsFirst.empty() && !opsSecond.empty()) {
                for (const auto &opFirst : opsFirst) {
                    for (const auto &opLast : opsSecond) {
                        // Exclude artificial transformations from the hub
                        // to the target CRS
                        if (!opLast->hasBallparkTransformation()) {
                            try {
                                res.emplace_back(
                                    ConcatenatedOperation::
                                        createComputeMetadata(
                                            {opFirst, opLast},
                                            disallowEmptyIntersection));
                            } catch (
                                const InvalidOperationEmptyIntersection &) {
                            }
                        } else {
                            // std::cerr << "excluded " << opLast->nameStr() <<
                            // std::endl;
                        }
                    }
                }
                if (!res.empty()) {
                    return;
                }
            }
        }
    }

    res = createOperations(boundSrc->baseCRS(),
                           util::optional<common::DataEpoch>(), targetCRS,
                           util::optional<common::DataEpoch>(), context);
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsBoundToVert(
    const crs::CRSNNPtr & /*sourceCRS*/, const crs::CRSNNPtr &targetCRS,
    Private::Context &context, const crs::BoundCRS *boundSrc,
    const crs::VerticalCRS *vertDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    auto baseSrcVert =
        dynamic_cast<const crs::VerticalCRS *>(boundSrc->baseCRS().get());
    const auto &hubSrc = boundSrc->hubCRS();
    auto hubSrcVert = dynamic_cast<const crs::VerticalCRS *>(hubSrc.get());
    if (baseSrcVert && hubSrcVert &&
        vertDst->_isEquivalentTo(hubSrcVert,
                                 util::IComparable::Criterion::EQUIVALENT)) {
        res.emplace_back(boundSrc->transformation());
        return;
    }

    res = createOperations(boundSrc->baseCRS(),
                           util::optional<common::DataEpoch>(), targetCRS,
                           util::optional<common::DataEpoch>(), context);
}

// ---------------------------------------------------------------------------

static std::string
getBallparkTransformationVertToVert(const crs::CRSNNPtr &sourceCRS,
                                    const crs::CRSNNPtr &targetCRS) {
    auto name = buildTransfName(sourceCRS->nameStr(), targetCRS->nameStr());
    name += " (";
    name += BALLPARK_VERTICAL_TRANSFORMATION;
    name += ')';
    return name;
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsVertToVert(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    Private::Context &context, const crs::VerticalCRS *vertSrc,
    const crs::VerticalCRS *vertDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    const auto &authFactory = context.context->getAuthorityFactory();
    const auto dbContext =
        authFactory ? authFactory->databaseContext().as_nullable() : nullptr;

    const auto srcDatum = vertSrc->datumNonNull(dbContext);
    const auto dstDatum = vertDst->datumNonNull(dbContext);
    const bool equivalentVDatum = srcDatum->_isEquivalentTo(
        dstDatum.get(), util::IComparable::Criterion::EQUIVALENT, dbContext);

    const auto &srcAxis = vertSrc->coordinateSystem()->axisList()[0];
    const double convSrc = srcAxis->unit().conversionToSI();
    const auto &dstAxis = vertDst->coordinateSystem()->axisList()[0];
    const double convDst = dstAxis->unit().conversionToSI();
    const bool srcIsUp = srcAxis->direction() == cs::AxisDirection::UP;
    const bool srcIsDown = srcAxis->direction() == cs::AxisDirection::DOWN;
    const bool dstIsUp = dstAxis->direction() == cs::AxisDirection::UP;
    const bool dstIsDown = dstAxis->direction() == cs::AxisDirection::DOWN;
    const bool heightDepthReversal =
        ((srcIsUp && dstIsDown) || (srcIsDown && dstIsUp));

    if (convDst == 0)
        throw InvalidOperation("Conversion factor of target unit is 0");
    const double factor = convSrc / convDst;

    const auto &sourceCRSExtent = getExtent(sourceCRS);
    const auto &targetCRSExtent = getExtent(targetCRS);
    const bool sameExtent =
        sourceCRSExtent && targetCRSExtent &&
        sourceCRSExtent->_isEquivalentTo(
            targetCRSExtent.get(), util::IComparable::Criterion::EQUIVALENT);

    util::PropertyMap map;
    map.set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
            sameExtent ? NN_NO_CHECK(sourceCRSExtent)
                       : metadata::Extent::WORLD);

    if (!equivalentVDatum) {
        const auto name =
            getBallparkTransformationVertToVert(sourceCRS, targetCRS);
        auto conv = Transformation::createChangeVerticalUnit(
            map.set(common::IdentifiedObject::NAME_KEY, name), sourceCRS,
            targetCRS,
            // In case of a height depth reversal, we should probably have
            // 2 steps instead of putting a negative factor...
            common::Scale(heightDepthReversal ? -factor : factor), {});
        conv->setHasBallparkTransformation(true);
        res.push_back(conv);
    } else if (convSrc != convDst || !heightDepthReversal) {
        auto name = buildConvName(sourceCRS->nameStr(), targetCRS->nameStr());
        auto conv = Conversion::createChangeVerticalUnit(
            map.set(common::IdentifiedObject::NAME_KEY, name),
            // In case of a height depth reversal, we should probably have
            // 2 steps instead of putting a negative factor...
            common::Scale(heightDepthReversal ? -factor : factor));
        conv->setCRSs(sourceCRS, targetCRS, nullptr);
        res.push_back(conv);
    } else {
        auto name = buildConvName(sourceCRS->nameStr(), targetCRS->nameStr());
        auto conv = Conversion::createHeightDepthReversal(
            map.set(common::IdentifiedObject::NAME_KEY, name));
        conv->setCRSs(sourceCRS, targetCRS, nullptr);
        res.push_back(conv);
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsVertToGeog(
    const crs::CRSNNPtr &sourceCRS,
    const util::optional<common::DataEpoch> &sourceEpoch,
    const crs::CRSNNPtr &targetCRS,
    const util::optional<common::DataEpoch> &targetEpoch,
    Private::Context &context, const crs::VerticalCRS *vertSrc,
    const crs::GeographicCRS *geogDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    if (vertSrc->identifiers().empty()) {
        const auto &vertSrcName = vertSrc->nameStr();
        const auto &authFactory = context.context->getAuthorityFactory();
        if (authFactory != nullptr && vertSrcName != "unnamed" &&
            vertSrcName != "unknown") {
            auto matches = authFactory->createObjectsFromName(
                vertSrcName, {io::AuthorityFactory::ObjectType::VERTICAL_CRS},
                false, 2);
            if (matches.size() == 1) {
                const auto &match = matches.front();
                if (vertSrc->_isEquivalentTo(
                        match.get(),
                        util::IComparable::Criterion::EQUIVALENT) &&
                    !match->identifiers().empty()) {
                    auto resTmp = createOperations(
                        NN_NO_CHECK(
                            util::nn_dynamic_pointer_cast<crs::VerticalCRS>(
                                match)),
                        sourceEpoch, targetCRS, targetEpoch, context);
                    res.insert(res.end(), resTmp.begin(), resTmp.end());
                    return;
                }
            }
        }
    }

    createOperationsVertToGeogSynthetized(sourceCRS, targetCRS, context,
                                          vertSrc, geogDst, res);
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsVertToGeogSynthetized(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    Private::Context &context, const crs::VerticalCRS *vertSrc,
    const crs::GeographicCRS *geogDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    const auto &srcAxis = vertSrc->coordinateSystem()->axisList()[0];
    const double convSrc = srcAxis->unit().conversionToSI();
    double convDst = 1.0;
    const auto &geogAxis = geogDst->coordinateSystem()->axisList();
    bool dstIsUp = true;
    bool dstIsDown = false;
    if (geogAxis.size() == 3) {
        const auto &dstAxis = geogAxis[2];
        convDst = dstAxis->unit().conversionToSI();
        dstIsUp = dstAxis->direction() == cs::AxisDirection::UP;
        dstIsDown = dstAxis->direction() == cs::AxisDirection::DOWN;
    }
    const bool srcIsUp = srcAxis->direction() == cs::AxisDirection::UP;
    const bool srcIsDown = srcAxis->direction() == cs::AxisDirection::DOWN;
    const bool heightDepthReversal =
        ((srcIsUp && dstIsDown) || (srcIsDown && dstIsUp));

    if (convDst == 0)
        throw InvalidOperation("Conversion factor of target unit is 0");
    const double factor = convSrc / convDst;

    const auto &sourceCRSExtent = getExtent(sourceCRS);
    const auto &targetCRSExtent = getExtent(targetCRS);
    const bool sameExtent =
        sourceCRSExtent && targetCRSExtent &&
        sourceCRSExtent->_isEquivalentTo(
            targetCRSExtent.get(), util::IComparable::Criterion::EQUIVALENT);

    const auto &authFactory = context.context->getAuthorityFactory();
    const auto dbContext =
        authFactory ? authFactory->databaseContext().as_nullable() : nullptr;

    const auto vertDatum = vertSrc->datumNonNull(dbContext);
    const auto &vertDatumName = vertDatum->nameStr();
    const auto geogDstDatum = geogDst->datumNonNull(dbContext);
    const auto &geogDstDatumName = geogDstDatum->nameStr();
    // We accept a vertical CRS whose datum name is the same datum name as the
    // source geographic CRS, or whose datum name is "Ellipsoid" if it is part
    // of a CompoundCRS whose horizontal CRS has a geodetic datum of the same
    // datum name as the source geographic CRS, to mean an ellipsoidal height.
    // This is against OGC Topic 2, and an extension needed for use case of
    // https://github.com/OSGeo/PROJ/issues/4175
    const bool bIsSameDatum = vertDatumName != "unknown" &&
                              (vertDatumName == geogDstDatumName ||
                               (vertDatumName == "Ellipsoid" &&
                                !context.geogCRSOfVertCRSStack.empty() &&
                                context.geogCRSOfVertCRSStack.back()
                                        ->datumNonNull(dbContext)
                                        ->nameStr() == geogDstDatumName));

    std::string transfName;

    if (bIsSameDatum) {
        transfName = buildConvName(factor != 1.0 ? sourceCRS->nameStr()
                                                 : targetCRS->nameStr(),
                                   targetCRS->nameStr());
    } else {
        transfName =
            buildTransfName(sourceCRS->nameStr(), targetCRS->nameStr());
        transfName += " (";
        transfName += BALLPARK_VERTICAL_TRANSFORMATION_NO_ELLIPSOID_VERT_HEIGHT;
        transfName += ')';
    }

    util::PropertyMap map;
    map.set(common::IdentifiedObject::NAME_KEY, transfName)
        .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
             sameExtent ? NN_NO_CHECK(sourceCRSExtent)
                        : metadata::Extent::WORLD);

    if (bIsSameDatum) {
        auto conv = Conversion::createChangeVerticalUnit(
            map, common::Scale(heightDepthReversal ? -factor : factor));
        conv->setCRSs(sourceCRS, targetCRS, nullptr);
        res.push_back(conv);
    } else {
        auto transf = Transformation::createChangeVerticalUnit(
            map, sourceCRS, targetCRS,
            common::Scale(heightDepthReversal ? -factor : factor), {});
        transf->setHasBallparkTransformation(true);
        res.push_back(transf);
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsBoundToBound(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    Private::Context &context, const crs::BoundCRS *boundSrc,
    const crs::BoundCRS *boundDst, std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    // BoundCRS to BoundCRS of horizontal CRS using the same (geographic) hub
    const auto &hubSrc = boundSrc->hubCRS();
    auto hubSrcGeog = dynamic_cast<const crs::GeographicCRS *>(hubSrc.get());
    const auto &hubDst = boundDst->hubCRS();
    auto hubDstGeog = dynamic_cast<const crs::GeographicCRS *>(hubDst.get());
    if (hubSrcGeog && hubDstGeog &&
        hubSrcGeog->_isEquivalentTo(hubDstGeog,
                                    util::IComparable::Criterion::EQUIVALENT)) {
        auto opsFirst = createOperations(
            sourceCRS, util::optional<common::DataEpoch>(), hubSrc,
            util::optional<common::DataEpoch>(), context);
        auto opsLast = createOperations(
            hubSrc, util::optional<common::DataEpoch>(), targetCRS,
            util::optional<common::DataEpoch>(), context);
        for (const auto &opFirst : opsFirst) {
            for (const auto &opLast : opsLast) {
                try {
                    std::vector<CoordinateOperationNNPtr> ops;
                    ops.push_back(opFirst);
                    ops.push_back(opLast);
                    res.emplace_back(
                        ConcatenatedOperation::createComputeMetadata(
                            ops, disallowEmptyIntersection));
                } catch (const InvalidOperationEmptyIntersection &) {
                }
            }
        }
        if (!res.empty()) {
            return;
        }
    }

    // BoundCRS to BoundCRS of vertical CRS using the same vertical datum
    // ==> ignore the bound transformation
    auto baseOfBoundSrcAsVertCRS =
        dynamic_cast<crs::VerticalCRS *>(boundSrc->baseCRS().get());
    auto baseOfBoundDstAsVertCRS =
        dynamic_cast<crs::VerticalCRS *>(boundDst->baseCRS().get());
    if (baseOfBoundSrcAsVertCRS && baseOfBoundDstAsVertCRS) {

        const auto &authFactory = context.context->getAuthorityFactory();
        const auto dbContext =
            authFactory ? authFactory->databaseContext().as_nullable()
                        : nullptr;

        const auto datumSrc = baseOfBoundSrcAsVertCRS->datumNonNull(dbContext);
        const auto datumDst = baseOfBoundDstAsVertCRS->datumNonNull(dbContext);
        if (datumSrc->nameStr() == datumDst->nameStr() &&
            (datumSrc->nameStr() != "unknown" ||
             boundSrc->transformation()->_isEquivalentTo(
                 boundDst->transformation().get(),
                 util::IComparable::Criterion::EQUIVALENT))) {
            res = createOperations(
                boundSrc->baseCRS(), util::optional<common::DataEpoch>(),
                boundDst->baseCRS(), util::optional<common::DataEpoch>(),
                context);
            return;
        }
    }

    // BoundCRS to BoundCRS of vertical CRS
    auto vertCRSOfBaseOfBoundSrc = boundSrc->baseCRS()->extractVerticalCRS();
    auto vertCRSOfBaseOfBoundDst = boundDst->baseCRS()->extractVerticalCRS();
    if (hubSrcGeog && hubDstGeog &&
        hubSrcGeog->_isEquivalentTo(hubDstGeog,
                                    util::IComparable::Criterion::EQUIVALENT) &&
        vertCRSOfBaseOfBoundSrc && vertCRSOfBaseOfBoundDst) {
        auto opsFirst = createOperations(
            sourceCRS, util::optional<common::DataEpoch>(), hubSrc,
            util::optional<common::DataEpoch>(), context);
        auto opsLast = createOperations(
            hubSrc, util::optional<common::DataEpoch>(), targetCRS,
            util::optional<common::DataEpoch>(), context);
        if (!opsFirst.empty() && !opsLast.empty()) {
            for (const auto &opFirst : opsFirst) {
                for (const auto &opLast : opsLast) {
                    try {
                        res.emplace_back(
                            ConcatenatedOperation::createComputeMetadata(
                                {opFirst, opLast}, disallowEmptyIntersection));
                    } catch (const InvalidOperationEmptyIntersection &) {
                    }
                }
            }
            if (!res.empty()) {
                return;
            }
        }
    }

    res = createOperations(
        boundSrc->baseCRS(), util::optional<common::DataEpoch>(),
        boundDst->baseCRS(), util::optional<common::DataEpoch>(), context);
}

// ---------------------------------------------------------------------------

static std::vector<CoordinateOperationNNPtr>
getOps(const CoordinateOperationNNPtr &op) {
    auto concatenated = dynamic_cast<const ConcatenatedOperation *>(op.get());
    if (concatenated)
        return concatenated->operations();
    return {op};
}

// ---------------------------------------------------------------------------

static std::string normalize2D3DInName(const std::string &s) {
    std::string out = s;
    const char *const patterns[] = {
        " (2D)",
        " (geographic3D horizontal)",
        " (geog2D)",
        " (geog3D)",
    };
    for (const char *pattern : patterns) {
        out = replaceAll(out, pattern, "");
    }
    return out;
}

// ---------------------------------------------------------------------------

static bool useCompatibleTransformationsForSameSourceTarget(
    const CoordinateOperationNNPtr &opA, const CoordinateOperationNNPtr &opB) {
    const auto subOpsA = getOps(opA);
    const auto subOpsB = getOps(opB);

    for (const auto &subOpA : subOpsA) {
        if (!dynamic_cast<const Transformation *>(subOpA.get()))
            continue;
        const auto subOpAName = normalize2D3DInName(subOpA->nameStr());
        const auto &subOpASourceCRSName = subOpA->sourceCRS()->nameStr();
        const auto &subOpATargetCRSName = subOpA->targetCRS()->nameStr();
        if (subOpASourceCRSName == "unknown" ||
            subOpATargetCRSName == "unknown")
            continue;
        for (const auto &subOpB : subOpsB) {
            if (!dynamic_cast<const Transformation *>(subOpB.get()))
                continue;
            const auto &subOpBSourceCRSName = subOpB->sourceCRS()->nameStr();
            const auto &subOpBTargetCRSName = subOpB->targetCRS()->nameStr();
            if (subOpBSourceCRSName == "unknown" ||
                subOpBTargetCRSName == "unknown")
                continue;

            if (subOpASourceCRSName == subOpBSourceCRSName &&
                subOpATargetCRSName == subOpBTargetCRSName) {
                const auto &subOpBName = normalize2D3DInName(subOpB->nameStr());
                if (starts_with(subOpAName, NULL_GEOGRAPHIC_OFFSET) &&
                    starts_with(subOpB->nameStr(), NULL_GEOGRAPHIC_OFFSET)) {
                    continue;
                }
                if (subOpAName != subOpBName) {
                    return false;
                }
            } else if (subOpASourceCRSName == subOpBTargetCRSName &&
                       subOpATargetCRSName == subOpBSourceCRSName) {
                const auto &subOpBName = subOpB->nameStr();
                if (starts_with(subOpAName, NULL_GEOGRAPHIC_OFFSET) &&
                    starts_with(subOpBName, NULL_GEOGRAPHIC_OFFSET)) {
                    continue;
                }

                if (subOpAName !=
                    normalize2D3DInName(subOpB->inverse()->nameStr())) {
                    return false;
                }
            }
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

static crs::GeographicCRSPtr
getInterpolationGeogCRS(const CoordinateOperationNNPtr &verticalTransform,
                        const io::DatabaseContextPtr &dbContext) {
    crs::GeographicCRSPtr interpolationGeogCRS;
    auto transformationVerticalTransform =
        dynamic_cast<const Transformation *>(verticalTransform.get());
    if (transformationVerticalTransform == nullptr) {
        const auto concat = dynamic_cast<const ConcatenatedOperation *>(
            verticalTransform.get());
        if (concat) {
            const auto &steps = concat->operations();
            // Is this change of unit and/or height depth reversal +
            // transformation ?
            for (const auto &step : steps) {
                const auto transf =
                    dynamic_cast<const Transformation *>(step.get());
                if (transf) {
                    // Only support a single Transformation in the steps
                    if (transformationVerticalTransform != nullptr) {
                        transformationVerticalTransform = nullptr;
                        break;
                    }
                    transformationVerticalTransform = transf;
                }
            }
        }
    }
    if (transformationVerticalTransform &&
        !transformationVerticalTransform->hasBallparkTransformation()) {
        auto interpTransformCRS =
            transformationVerticalTransform->interpolationCRS();
        if (interpTransformCRS) {
            interpolationGeogCRS =
                std::dynamic_pointer_cast<crs::GeographicCRS>(
                    interpTransformCRS);
        } else {
            // If no explicit interpolation CRS, then
            // this will be the geographic CRS of the
            // vertical to geog transformation
            interpolationGeogCRS =
                std::dynamic_pointer_cast<crs::GeographicCRS>(
                    transformationVerticalTransform->targetCRS().as_nullable());
        }
    }

    if (interpolationGeogCRS) {
        if (interpolationGeogCRS->coordinateSystem()->axisList().size() == 3) {
            // We need to force the interpolation CRS, which
            // will
            // frequently be 3D, to 2D to avoid transformations
            // between source CRS and interpolation CRS to have
            // 3D terms.
            interpolationGeogCRS =
                interpolationGeogCRS->demoteTo2D(std::string(), dbContext)
                    .as_nullable();
        }
    }

    return interpolationGeogCRS;
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsCompoundToGeog(
    const crs::CRSNNPtr &sourceCRS,
    const util::optional<common::DataEpoch> &sourceEpoch,
    const crs::CRSNNPtr &targetCRS,
    const util::optional<common::DataEpoch> &targetEpoch,
    Private::Context &context, const crs::CompoundCRS *compoundSrc,
    const crs::GeographicCRS *geogDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    ENTER_FUNCTION();

    const auto &authFactory = context.context->getAuthorityFactory();
    const auto &componentsSrc = compoundSrc->componentReferenceSystems();
    if (!componentsSrc.empty()) {

        const auto dbContext =
            authFactory ? authFactory->databaseContext().as_nullable()
                        : nullptr;

        if (componentsSrc.size() == 2) {
            auto derivedHSrc =
                dynamic_cast<const crs::DerivedCRS *>(componentsSrc[0].get());
            if (derivedHSrc) {
                std::vector<crs::CRSNNPtr> intermComponents{
                    derivedHSrc->baseCRS(), componentsSrc[1]};
                auto properties = util::PropertyMap().set(
                    common::IdentifiedObject::NAME_KEY,
                    intermComponents[0]->nameStr() + " + " +
                        intermComponents[1]->nameStr());
                auto intermCompound =
                    crs::CompoundCRS::create(properties, intermComponents);
                auto opsFirst =
                    createOperations(sourceCRS, sourceEpoch, intermCompound,
                                     sourceEpoch, context);
                assert(!opsFirst.empty());
                auto opsLast =
                    createOperations(intermCompound, sourceEpoch, targetCRS,
                                     targetEpoch, context);
                for (const auto &opLast : opsLast) {
                    try {
                        res.emplace_back(
                            ConcatenatedOperation::createComputeMetadata(
                                {opsFirst.front(), opLast},
                                disallowEmptyIntersection));
                    } catch (const std::exception &) {
                    }
                }
                return;
            }

            auto geogSrc = dynamic_cast<const crs::GeographicCRS *>(
                componentsSrc[0].get());
            // Sorry for this hack... aimed at transforming
            // "NAD83(CSRS)v7 + CGVD2013a(1997) height @ 1997" to "NAD83(CSRS)v7
            // @ 1997" to "NAD83(CSRS)v3 + CGVD2013a(1997) height" to
            // "NAD83(CSRS)v3" OR "NAD83(CSRS)v7 + CGVD2013a(2002) height @
            // 2002" to "NAD83(CSRS)v7 @ 2002" to "NAD83(CSRS)v4 +
            // CGVD2013a(2002) height" to "NAD83(CSRS)v4"
            if (dbContext && geogSrc && geogSrc->nameStr() == "NAD83(CSRS)v7" &&
                sourceEpoch.has_value() &&
                geogDst->coordinateSystem()->axisList().size() == 3U &&
                geogDst->nameStr() == geogSrc->nameStr() &&
                targetEpoch.has_value() &&
                sourceEpoch->coordinateEpoch()._isEquivalentTo(
                    targetEpoch->coordinateEpoch())) {
                const bool is1997 =
                    std::abs(sourceEpoch->coordinateEpoch().convertToUnit(
                                 common::UnitOfMeasure::YEAR) -
                             1997) < 1e-10;
                const bool is2002 =
                    std::abs(sourceEpoch->coordinateEpoch().convertToUnit(
                                 common::UnitOfMeasure::YEAR) -
                             2002) < 1e-10;
                try {
                    auto authFactoryEPSG = io::AuthorityFactory::create(
                        authFactory->databaseContext(), "EPSG");
                    if (geogSrc->_isEquivalentTo(
                            authFactoryEPSG
                                ->createCoordinateReferenceSystem("8255")
                                .get(),
                            util::IComparable::Criterion::
                                EQUIVALENT) && // NAD83(CSRS)v7
                                               // 2D
                        geogDst->_isEquivalentTo(
                            authFactoryEPSG
                                ->createCoordinateReferenceSystem("8254")
                                .get(),
                            util::IComparable::Criterion::
                                EQUIVALENT) && // NAD83(CSRS)v7
                                               // 3D
                        ((is1997 && componentsSrc[1]->nameStr() ==
                                        "CGVD2013a(1997) height") ||
                         (is2002 && componentsSrc[1]->nameStr() ==
                                        "CGVD2013a(2002) height"))) {
                        std::vector<crs::CRSNNPtr> intermComponents{
                            authFactoryEPSG->createCoordinateReferenceSystem(
                                is1997 ? "8240" : // NAD83(CSRS)v3 2D
                                    "8246"        // NAD83(CSRS)v4 2D
                                ),
                            componentsSrc[1]};
                        auto properties = util::PropertyMap().set(
                            common::IdentifiedObject::NAME_KEY,
                            intermComponents[0]->nameStr() + " + " +
                                intermComponents[1]->nameStr());
                        auto newCompound = crs::CompoundCRS::create(
                            properties, intermComponents);
                        auto ops = createOperations(
                            newCompound, sourceEpoch,
                            authFactoryEPSG->createCoordinateReferenceSystem(
                                is1997 ? "8239" : // NAD83(CSRS)v3 3D
                                    "8244"        // NAD83(CSRS)v4 3D
                                ),
                            sourceEpoch, context);
                        for (const auto &op : ops) {
                            auto opClone = op->shallowClone();
                            setCRSs(opClone.get(), sourceCRS, targetCRS);
                            res.emplace_back(opClone);
                        }
                        return;
                    }
                } catch (const std::exception &) {
                }
            }

            auto boundSrc =
                dynamic_cast<const crs::BoundCRS *>(componentsSrc[0].get());
            if (boundSrc) {
                derivedHSrc = dynamic_cast<const crs::DerivedCRS *>(
                    boundSrc->baseCRS().get());
                if (derivedHSrc) {
                    std::vector<crs::CRSNNPtr> intermComponents{
                        crs::BoundCRS::create(derivedHSrc->baseCRS(),
                                              boundSrc->hubCRS(),
                                              boundSrc->transformation()),
                        componentsSrc[1]};
                    auto properties = util::PropertyMap().set(
                        common::IdentifiedObject::NAME_KEY,
                        intermComponents[0]->nameStr() + " + " +
                            intermComponents[1]->nameStr());
                    auto intermCompound =
                        crs::CompoundCRS::create(properties, intermComponents);
                    auto opsFirst =
                        createOperations(sourceCRS, sourceEpoch, intermCompound,
                                         sourceEpoch, context);
                    assert(!opsFirst.empty());
                    auto opsLast =
                        createOperations(intermCompound, sourceEpoch, targetCRS,
                                         targetEpoch, context);
                    for (const auto &opLast : opsLast) {
                        try {
                            res.emplace_back(
                                ConcatenatedOperation::createComputeMetadata(
                                    {opsFirst.front(), opLast},
                                    disallowEmptyIntersection));
                        } catch (const std::exception &) {
                        }
                    }
                    return;
                }
            }
        }

        // Deal with "+proj=something +geoidgrids +nadgrids/+towgs84" to
        // another CRS whose datum is not the same as the horizontal datum
        // of the source
        if (componentsSrc.size() == 2) {
            auto comp0Bound =
                dynamic_cast<crs::BoundCRS *>(componentsSrc[0].get());
            auto comp1Bound =
                dynamic_cast<crs::BoundCRS *>(componentsSrc[1].get());
            auto comp0Geog = componentsSrc[0]->extractGeographicCRS();
            auto dstGeog = targetCRS->extractGeographicCRS();
            if (comp0Bound && comp1Bound && comp0Geog &&
                comp1Bound->hubCRS()
                    ->demoteTo2D(std::string(), dbContext)
                    ->isEquivalentTo(
                        comp0Geog.get(),
                        util::IComparable::Criterion::EQUIVALENT) &&
                dstGeog &&
                !comp0Geog->datumNonNull(dbContext)->isEquivalentTo(
                    dstGeog->datumNonNull(dbContext).get(),
                    util::IComparable::Criterion::EQUIVALENT)) {

                const auto &op1Dest = comp1Bound->hubCRS();
                const auto ops1 = createOperations(
                    crs::CompoundCRS::create(
                        util::PropertyMap().set(
                            common::IdentifiedObject::NAME_KEY, std::string()),
                        {comp0Bound->baseCRS(), componentsSrc[1]}),
                    util::optional<common::DataEpoch>(), op1Dest,
                    util::optional<common::DataEpoch>(), context);

                const auto op2Dest =
                    comp0Bound->hubCRS()->promoteTo3D(std::string(), dbContext);
                const auto ops2 = createOperations(
                    crs::BoundCRS::create(
                        util::PropertyMap().set(
                            common::IdentifiedObject::NAME_KEY, std::string()),
                        NN_NO_CHECK(comp0Geog), comp0Bound->hubCRS(),
                        comp0Bound->transformation())
                        ->promoteTo3D(std::string(), dbContext),
                    util::optional<common::DataEpoch>(), op2Dest,
                    util::optional<common::DataEpoch>(), context);

                const auto ops3 = createOperations(
                    op2Dest, util::optional<common::DataEpoch>(), targetCRS,
                    util::optional<common::DataEpoch>(), context);

                for (const auto &op1 : ops1) {
                    auto op1Clone = op1->shallowClone();
                    setCRSs(op1Clone.get(), sourceCRS, op1Dest);
                    for (const auto &op2 : ops2) {
                        auto op2Clone = op2->shallowClone();
                        setCRSs(op2Clone.get(), op1Dest, op2Dest);
                        for (const auto &op3 : ops3) {
                            try {
                                res.emplace_back(
                                    ConcatenatedOperation::
                                        createComputeMetadata(
                                            {op1Clone, op2Clone, op3},
                                            disallowEmptyIntersection));
                            } catch (const std::exception &) {
                            }
                        }
                    }
                }
                if (!res.empty()) {
                    return;
                }
            }
        }

        // Only do a vertical transformation if the target CRS is 3D.
        const auto dstSingle = dynamic_cast<crs::SingleCRS *>(targetCRS.get());
        if (dstSingle &&
            dstSingle->coordinateSystem()->axisList().size() == 2) {
            auto tmp = createOperations(componentsSrc[0], sourceEpoch,
                                        targetCRS, targetEpoch, context);
            for (const auto &op : tmp) {
                auto opClone = op->shallowClone();
                setCRSs(opClone.get(), sourceCRS, targetCRS);
                res.emplace_back(opClone);
            }
            return;
        }

        std::vector<CoordinateOperationNNPtr> horizTransforms;
        auto srcGeogCRS = componentsSrc[0]->extractGeographicCRS();
        if (srcGeogCRS) {
            horizTransforms = createOperations(componentsSrc[0], sourceEpoch,
                                               targetCRS, targetEpoch, context);
        }
        std::vector<CoordinateOperationNNPtr> verticalTransforms;

        if (componentsSrc.size() >= 2 &&
            componentsSrc[1]->extractVerticalCRS()) {

            struct SetSkipHorizontalTransform {
                Context &context;

                explicit SetSkipHorizontalTransform(Context &contextIn)
                    : context(contextIn) {
                    assert(!context.skipHorizontalTransformation);
                    context.skipHorizontalTransformation = true;
                }

                ~SetSkipHorizontalTransform() {
                    context.skipHorizontalTransformation = false;
                }
            };
            SetSkipHorizontalTransform setSkipHorizontalTransform(context);

            struct SetGeogCRSOfVertCRS {
                Context &context;
                const bool hasPushed;

                explicit SetGeogCRSOfVertCRS(
                    Context &contextIn, const crs::GeographicCRSPtr &geogCRS)
                    : context(contextIn), hasPushed(geogCRS != nullptr) {
                    if (geogCRS)
                        context.geogCRSOfVertCRSStack.push_back(
                            NN_NO_CHECK(geogCRS));
                }

                ~SetGeogCRSOfVertCRS() {
                    if (hasPushed)
                        context.geogCRSOfVertCRSStack.pop_back();
                }
            };
            SetGeogCRSOfVertCRS setGeogCRSOfVertCRS(context, srcGeogCRS);

            verticalTransforms = createOperations(
                componentsSrc[1], util::optional<common::DataEpoch>(),
                targetCRS->promoteTo3D(std::string(), dbContext),
                util::optional<common::DataEpoch>(), context);
            bool foundRegisteredTransformWithAllGridsAvailable = false;
            const auto gridAvailabilityUse =
                context.context->getGridAvailabilityUse();
            const bool ignoreMissingGrids =
                gridAvailabilityUse ==
                CoordinateOperationContext::GridAvailabilityUse::
                    IGNORE_GRID_AVAILABILITY;
            for (const auto &op : verticalTransforms) {
                if (hasIdentifiers(op) && dbContext) {
                    bool missingGrid = false;
                    if (!ignoreMissingGrids) {
                        const auto gridsNeeded = op->gridsNeeded(
                            dbContext,
                            gridAvailabilityUse ==
                                CoordinateOperationContext::
                                    GridAvailabilityUse::KNOWN_AVAILABLE);
                        for (const auto &gridDesc : gridsNeeded) {
                            if (!gridDesc.available) {
                                missingGrid = true;
                                break;
                            }
                        }
                    }
                    if (!missingGrid) {
                        foundRegisteredTransformWithAllGridsAvailable = true;
                        break;
                    }
                }
            }
            if (srcGeogCRS &&
                !srcGeogCRS->_isEquivalentTo(
                    geogDst, util::IComparable::Criterion::EQUIVALENT) &&
                !srcGeogCRS->is2DPartOf3D(NN_NO_CHECK(geogDst), dbContext)) {
                auto geogCRSTmp =
                    NN_NO_CHECK(srcGeogCRS)
                        ->demoteTo2D(std::string(), dbContext)
                        ->promoteTo3D(
                            std::string(), dbContext,
                            geogDst->coordinateSystem()->axisList().size() == 3
                                ? geogDst->coordinateSystem()->axisList()[2]
                                : cs::VerticalCS::createGravityRelatedHeight(
                                      common::UnitOfMeasure::METRE)
                                      ->axisList()[0]);
                auto verticalTransformsTmp = createOperations(
                    componentsSrc[1], util::optional<common::DataEpoch>(),
                    geogCRSTmp, util::optional<common::DataEpoch>(), context);
                bool foundRegisteredTransform = false;
                bool foundRegisteredTransformWithAllGridsAvailable2 = false;
                for (const auto &op : verticalTransformsTmp) {
                    if (hasIdentifiers(op) && dbContext) {
                        bool missingGrid = false;
                        if (!ignoreMissingGrids) {
                            const auto gridsNeeded = op->gridsNeeded(
                                dbContext,
                                gridAvailabilityUse ==
                                    CoordinateOperationContext::
                                        GridAvailabilityUse::KNOWN_AVAILABLE);
                            for (const auto &gridDesc : gridsNeeded) {
                                if (!gridDesc.available) {
                                    missingGrid = true;
                                    break;
                                }
                            }
                        }
                        foundRegisteredTransform = true;
                        if (!missingGrid) {
                            foundRegisteredTransformWithAllGridsAvailable2 =
                                true;
                            break;
                        }
                    }
                }
                if (foundRegisteredTransformWithAllGridsAvailable2 &&
                    !foundRegisteredTransformWithAllGridsAvailable) {
                    verticalTransforms = std::move(verticalTransformsTmp);
                } else if (foundRegisteredTransform) {
                    verticalTransforms.insert(verticalTransforms.end(),
                                              verticalTransformsTmp.begin(),
                                              verticalTransformsTmp.end());
                }
            }
        }

        if (horizTransforms.empty() || verticalTransforms.empty()) {
            res = std::move(horizTransforms);
            return;
        }

        typedef std::pair<std::vector<CoordinateOperationNNPtr>,
                          std::vector<CoordinateOperationNNPtr>>
            PairOfTransforms;
        std::map<std::string, PairOfTransforms>
            cacheHorizToInterpAndInterpToTarget;

        bool hasVerticalTransformWithInterpGeogCRS = false;
        for (const auto &verticalTransform : verticalTransforms) {
#ifdef TRACE_CREATE_OPERATIONS
            ENTER_BLOCK("Considering vertical transform " +
                        objectAsStr(verticalTransform.get()));
#endif
            crs::GeographicCRSPtr interpolationGeogCRS =
                getInterpolationGeogCRS(verticalTransform, dbContext);
            if (interpolationGeogCRS) {
#ifdef TRACE_CREATE_OPERATIONS
                logTrace("Using " + objectAsStr(interpolationGeogCRS.get()) +
                         " as interpolation CRS");
#endif
                std::vector<CoordinateOperationNNPtr> srcToInterpOps;
                std::vector<CoordinateOperationNNPtr> interpToTargetOps;

                std::string key;
                const auto &ids = interpolationGeogCRS->identifiers();
                if (!ids.empty()) {
                    key =
                        (*ids.front()->codeSpace()) + ':' + ids.front()->code();
                }

                const auto computeOpsToInterp = [&srcToInterpOps,
                                                 &interpToTargetOps,
                                                 &componentsSrc,
                                                 &interpolationGeogCRS,
                                                 &targetCRS, &geogDst,
                                                 &dbContext, &context]() {
                    // Do the sourceCRS to interpolation CRS in 2D only
                    // to avoid altering the orthometric elevation
                    srcToInterpOps = createOperations(
                        componentsSrc[0], util::optional<common::DataEpoch>(),
                        NN_NO_CHECK(interpolationGeogCRS),
                        util::optional<common::DataEpoch>(), context);

                    // e.g when doing COMPOUND_CRS[
                    //      NAD83(CRS)+TOWGS84[0,0,0],
                    //      CGVD28 height + EXTENSION["PROJ4_GRIDS","HT2_0.gtx"]
                    // to NAD83(CRS) 3D
                    const auto boundSrc =
                        dynamic_cast<crs::BoundCRS *>(componentsSrc[0].get());
                    if (boundSrc &&
                        boundSrc->baseCRS()->isEquivalentTo(
                            targetCRS->demoteTo2D(std::string(), dbContext)
                                .get(),
                            util::IComparable::Criterion::EQUIVALENT) &&
                        boundSrc->hubCRS()->isEquivalentTo(
                            interpolationGeogCRS
                                ->demoteTo2D(std::string(), dbContext)
                                .get(),
                            util::IComparable::Criterion::EQUIVALENT)) {
                        // Make sure to use the same horizontal transformation
                        // (likely a null shift)
                        interpToTargetOps = applyInverse(srcToInterpOps);
                        return;
                    }

                    // But do the interpolation CRS to targetCRS in 3D
                    // to have proper ellipsoid height transformation.
                    // We need to force the vertical axis of this 3D'ified
                    // interpolation CRS to be the same as the target CRS,
                    // to avoid potential double vertical unit conversion,
                    // as the vertical transformation already takes care of
                    // that.
                    auto interp3D =
                        interpolationGeogCRS
                            ->demoteTo2D(std::string(), dbContext)
                            ->promoteTo3D(
                                std::string(), dbContext,
                                geogDst->coordinateSystem()
                                            ->axisList()
                                            .size() == 3
                                    ? geogDst->coordinateSystem()->axisList()[2]
                                    : cs::VerticalCS::
                                          createGravityRelatedHeight(
                                              common::UnitOfMeasure::METRE)
                                              ->axisList()[0]);
                    interpToTargetOps = createOperations(
                        interp3D, util::optional<common::DataEpoch>(),
                        targetCRS, util::optional<common::DataEpoch>(),
                        context);
                };

                if (!key.empty()) {
                    auto iter = cacheHorizToInterpAndInterpToTarget.find(key);
                    if (iter == cacheHorizToInterpAndInterpToTarget.end()) {
#ifdef TRACE_CREATE_OPERATIONS
                        ENTER_BLOCK("looking for horizontal transformation "
                                    "from source to interpCRS and interpCRS to "
                                    "target");
#endif
                        computeOpsToInterp();
                        cacheHorizToInterpAndInterpToTarget[key] =
                            PairOfTransforms(srcToInterpOps, interpToTargetOps);
                    } else {
                        srcToInterpOps = iter->second.first;
                        interpToTargetOps = iter->second.second;
                    }
                } else {
#ifdef TRACE_CREATE_OPERATIONS
                    ENTER_BLOCK("looking for horizontal transformation "
                                "from source to interpCRS and interpCRS to "
                                "target");
#endif
                    computeOpsToInterp();
                }

#ifdef TRACE_CREATE_OPERATIONS
                ENTER_BLOCK("creating HorizVerticalHorizPROJBased operations");
#endif
                const bool srcAndTargetGeogAreSame =
                    componentsSrc[0]->isEquivalentTo(
                        targetCRS->demoteTo2D(std::string(), dbContext).get(),
                        util::IComparable::Criterion::EQUIVALENT) &&
                    // Kind of a hack for EPSG:4937 to EPSG:9883 to work
                    // properly
                    !((componentsSrc[0]->nameStr() == "ETRS89" &&
                       targetCRS->nameStr() == "ETRS89-NOR [EUREF89]") ||
                      (componentsSrc[0]->nameStr() == "ETRS89-NOR [EUREF89]" &&
                       targetCRS->nameStr() == "ETRS89"));

                // Lambda to add to the set the name of geodetic datum of the
                // CRS
                const auto addDatumOfToSet = [&dbContext](
                                                 std::set<std::string> &set,
                                                 const crs::CRSNNPtr &crs) {
                    auto geodCRS = crs->extractGeodeticCRS();
                    if (geodCRS) {
                        set.insert(geodCRS->datumNonNull(dbContext)->nameStr());
                    }
                };

                // Lambda to return the set of names of geodetic datums used
                // by the source and target CRS of a list of operations.
                const auto makeDatumSet =
                    [&addDatumOfToSet](
                        const std::vector<CoordinateOperationNNPtr> &ops) {
                        std::set<std::string> datumSetOps;
                        for (const auto &subOp : ops) {
                            if (!dynamic_cast<const Transformation *>(
                                    subOp.get()))
                                continue;
                            addDatumOfToSet(datumSetOps,
                                            NN_NO_CHECK(subOp->sourceCRS()));
                            addDatumOfToSet(datumSetOps,
                                            NN_NO_CHECK(subOp->targetCRS()));
                        }
                        return datumSetOps;
                    };

                std::map<CoordinateOperation *, std::set<std::string>>
                    mapSetDatumsUsed;
                if (srcAndTargetGeogAreSame) {
                    // When the geographic CRS of the source and target, we
                    // want to make sure that the transformation from the
                    // source to the interpolation CRS uses the same datums as
                    // the one from the interpolation CRS to the target CRS.
                    // A simplistic view would be that the srcToInterp and
                    // interpToTarget should be the same, but they are
                    // subtelties, like interpToTarget being done in 3D, so with
                    // additional conversion steps, slightly different names in
                    // operations between 2D and 3D. The initial filter on
                    // checking that we use the same set of datum enable us
                    // to be confident we reject upfront geodetically-dubious
                    // operations.
                    for (const auto &op : srcToInterpOps) {
                        mapSetDatumsUsed[op.get()] = makeDatumSet(getOps(op));
                    }
                    for (const auto &op : interpToTargetOps) {
                        mapSetDatumsUsed[op.get()] = makeDatumSet(getOps(op));
                    }
                }

                const bool hasOnlyOneOp =
                    srcToInterpOps.size() == 1 && interpToTargetOps.size() == 1;
                for (const auto &srcToInterp : srcToInterpOps) {
                    for (const auto &interpToTarget : interpToTargetOps) {
                        if (!hasOnlyOneOp &&
                            ((srcAndTargetGeogAreSame &&
                              mapSetDatumsUsed[srcToInterp.get()] !=
                                  mapSetDatumsUsed[interpToTarget.get()]) ||
                             !useCompatibleTransformationsForSameSourceTarget(
                                 srcToInterp, interpToTarget))) {
#ifdef TRACE_CREATE_OPERATIONS
                            logTrace(
                                "Considering that '" + srcToInterp->nameStr() +
                                "' and '" + interpToTarget->nameStr() +
                                "' do not use consistent operations in the pre "
                                "and post-vertical transformation steps");
#endif
                            continue;
                        }

                        try {
                            auto op = createHorizVerticalHorizPROJBased(
                                sourceCRS, targetCRS, srcToInterp,
                                verticalTransform, interpToTarget,
                                interpolationGeogCRS, true);
                            res.emplace_back(op);
                            hasVerticalTransformWithInterpGeogCRS = true;
                        } catch (const std::exception &) {
                        }
                    }
                }
            }
        }

        for (const auto &verticalTransform : verticalTransforms) {
            crs::GeographicCRSPtr interpolationGeogCRS =
                getInterpolationGeogCRS(verticalTransform, dbContext);
            if (!interpolationGeogCRS &&
                (!hasVerticalTransformWithInterpGeogCRS ||
                 verticalTransform->hasBallparkTransformation())) {
                // This case is probably only correct if
                // verticalTransform and horizTransform are independent
                // and in particular that verticalTransform does not
                // involve a grid, because of the rather arbitrary order
                // horizontal then vertical applied
                for (const auto &horizTransform : horizTransforms) {
                    try {
                        auto op = createHorizVerticalPROJBased(
                            sourceCRS, targetCRS, horizTransform,
                            verticalTransform, disallowEmptyIntersection);
                        res.emplace_back(op);
                    } catch (const std::exception &) {
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsToGeod(
    const crs::CRSNNPtr &sourceCRS,
    const util::optional<common::DataEpoch> &sourceEpoch,
    const crs::CRSNNPtr &targetCRS,
    const util::optional<common::DataEpoch> &targetEpoch,
    Private::Context &context, const crs::GeodeticCRS *geodDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    auto cs = cs::EllipsoidalCS::createLatitudeLongitudeEllipsoidalHeight(
        common::UnitOfMeasure::DEGREE, common::UnitOfMeasure::METRE);
    auto intermGeog3DCRS =
        util::nn_static_pointer_cast<crs::CRS>(crs::GeographicCRS::create(
            util::PropertyMap()
                .set(common::IdentifiedObject::NAME_KEY, geodDst->nameStr())
                .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                     metadata::Extent::WORLD),
            geodDst->datum(), geodDst->datumEnsemble(), cs));
    auto sourceToGeog3DOps = createOperations(
        sourceCRS, sourceEpoch, intermGeog3DCRS, sourceEpoch, context);
    auto geog3DToTargetOps = createOperations(intermGeog3DCRS, targetEpoch,
                                              targetCRS, targetEpoch, context);
    if (!geog3DToTargetOps.empty()) {
        for (const auto &op : sourceToGeog3DOps) {
            auto newOp = op->shallowClone();
            setCRSs(newOp.get(), sourceCRS, intermGeog3DCRS);
            try {
                res.emplace_back(ConcatenatedOperation::createComputeMetadata(
                    {std::move(newOp), geog3DToTargetOps.front()},
                    disallowEmptyIntersection));
            } catch (const InvalidOperationEmptyIntersection &) {
            }
        }
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsCompoundToCompound(
    const crs::CRSNNPtr &sourceCRS,
    const util::optional<common::DataEpoch> &sourceEpoch,
    const crs::CRSNNPtr &targetCRS,
    const util::optional<common::DataEpoch> &targetEpoch,
    Private::Context &context, const crs::CompoundCRS *compoundSrc,
    const crs::CompoundCRS *compoundDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    const auto &componentsSrc = compoundSrc->componentReferenceSystems();
    const auto &componentsDst = compoundDst->componentReferenceSystems();
    if (componentsSrc.empty() || componentsSrc.size() != componentsDst.size()) {
        return;
    }
    const auto srcGeog = componentsSrc[0]->extractGeographicCRS();
    const auto dstGeog = componentsDst[0]->extractGeographicCRS();
    if (srcGeog == nullptr || dstGeog == nullptr) {
        return;
    }
    const bool srcGeogIsSameAsDstGeog = srcGeog->_isEquivalentTo(
        dstGeog.get(), util::IComparable::Criterion::EQUIVALENT);
    const auto &authFactory = context.context->getAuthorityFactory();
    auto dbContext =
        authFactory ? authFactory->databaseContext().as_nullable() : nullptr;
    const auto intermGeogSrc = srcGeog->promoteTo3D(std::string(), dbContext);
    const auto intermGeogDst =
        srcGeogIsSameAsDstGeog ? intermGeogSrc
                               : dstGeog->promoteTo3D(std::string(), dbContext);
    const auto opsGeogSrcToGeogDst = createOperations(
        intermGeogSrc, sourceEpoch, intermGeogDst, targetEpoch, context);

    // Use PointMotionOperations if appropriate and available
    if (authFactory && sourceEpoch.has_value() && targetEpoch.has_value() &&
        !sourceEpoch->coordinateEpoch()._isEquivalentTo(
            targetEpoch->coordinateEpoch()) &&
        srcGeogIsSameAsDstGeog) {
        const auto pmoSrc = authFactory->getPointMotionOperationsFor(
            NN_NO_CHECK(srcGeog), true);
        if (!pmoSrc.empty()) {
            auto geog3D = srcGeog->promoteTo3D(
                std::string(), authFactory->databaseContext().as_nullable());
            auto opsFirst = createOperations(sourceCRS, sourceEpoch, geog3D,
                                             sourceEpoch, context);
            auto pmoOps = createOperations(geog3D, sourceEpoch, geog3D,
                                           targetEpoch, context);
            auto opsLast = createOperations(geog3D, targetEpoch, targetCRS,
                                            targetEpoch, context);
            for (const auto &opFirst : opsFirst) {
                if (!opFirst->hasBallparkTransformation()) {
                    for (const auto &opMiddle : pmoOps) {
                        if (!opMiddle->hasBallparkTransformation()) {
                            for (const auto &opLast : opsLast) {
                                if (!opLast->hasBallparkTransformation()) {
                                    try {
                                        res.emplace_back(
                                            ConcatenatedOperation::
                                                createComputeMetadata(
                                                    {opFirst, opMiddle, opLast},
                                                    disallowEmptyIntersection));
                                    } catch (const std::exception &) {
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (!res.empty()) {
                return;
            }
        }
    }

    // Deal with "+proj=something +geoidgrids +nadgrids/+towgs84" to
    // "+proj=something +geoidgrids +nadgrids/+towgs84", using WGS 84 as an
    // intermediate.
    if (componentsSrc.size() == 2 && componentsDst.size() == 2) {
        auto comp0SrcBound =
            dynamic_cast<crs::BoundCRS *>(componentsSrc[0].get());
        auto comp1SrcBound =
            dynamic_cast<crs::BoundCRS *>(componentsSrc[1].get());
        auto comp0DstBound =
            dynamic_cast<crs::BoundCRS *>(componentsDst[0].get());
        auto comp1DstBound =
            dynamic_cast<crs::BoundCRS *>(componentsDst[1].get());
        if (comp0SrcBound && comp1SrcBound && comp0DstBound && comp1DstBound &&
            comp0SrcBound->hubCRS()->isEquivalentTo(
                comp0DstBound->hubCRS().get(),
                util::IComparable::Criterion::EQUIVALENT) &&
            !comp1SrcBound->isEquivalentTo(
                comp1DstBound, util::IComparable::Criterion::EQUIVALENT)) {
            auto hub3D =
                comp0SrcBound->hubCRS()->promoteTo3D(std::string(), dbContext);
            const auto ops1 = createOperations(sourceCRS, sourceEpoch, hub3D,
                                               sourceEpoch, context);
            const auto ops2 = createOperations(hub3D, targetEpoch, targetCRS,
                                               targetEpoch, context);
            for (const auto &op1 : ops1) {
                for (const auto &op2 : ops2) {
                    try {
                        res.emplace_back(
                            ConcatenatedOperation::createComputeMetadata(
                                {op1, op2}, disallowEmptyIntersection));
                    } catch (const std::exception &) {
                    }
                }
            }
            return;
        }
    }

    std::vector<CoordinateOperationNNPtr> verticalTransforms;
    bool bHasTriedVerticalTransforms = false;
    bool bTryThroughIntermediateGeogCRS = false;
    if (componentsSrc.size() >= 2) {
        const auto vertSrc = componentsSrc[1]->extractVerticalCRS();
        const auto vertDst = componentsDst[1]->extractVerticalCRS();
        if (vertSrc && vertDst &&
            !componentsSrc[1]->_isEquivalentTo(
                componentsDst[1].get(),
                util::IComparable::Criterion::EQUIVALENT)) {
            if ((!vertSrc->geoidModel().empty() ||
                 !vertDst->geoidModel().empty()) &&
                // To be able to use "CGVD28 height to
                // CGVD2013a(1997|2002|2010)" single grid
                !(vertSrc->nameStr() == "CGVD28 height" &&
                  !vertSrc->geoidModel().empty() &&
                  vertSrc->geoidModel().front()->nameStr() == "HT2_1997" &&
                  vertDst->nameStr() == "CGVD2013a(1997) height" &&
                  vertDst->geoidModel().empty()) &&
                !(vertSrc->nameStr() == "CGVD28 height" &&
                  !vertSrc->geoidModel().empty() &&
                  vertSrc->geoidModel().front()->nameStr() == "HT2_2002" &&
                  vertDst->nameStr() == "CGVD2013a(2002) height" &&
                  vertDst->geoidModel().empty()) &&
                !(vertSrc->nameStr() == "CGVD28 height" &&
                  !vertSrc->geoidModel().empty() &&
                  vertSrc->geoidModel().front()->nameStr() == "HT2_2010" &&
                  vertDst->nameStr() == "CGVD2013a(2010) height" &&
                  vertDst->geoidModel().empty()) &&
                !(vertDst->nameStr() == "CGVD28 height" &&
                  !vertDst->geoidModel().empty() &&
                  vertDst->geoidModel().front()->nameStr() == "HT2_1997" &&
                  vertSrc->nameStr() == "CGVD2013a(1997) height" &&
                  vertSrc->geoidModel().empty()) &&
                !(vertDst->nameStr() == "CGVD28 height" &&
                  !vertDst->geoidModel().empty() &&
                  vertDst->geoidModel().front()->nameStr() == "HT2_2002" &&
                  vertSrc->nameStr() == "CGVD2013a(2002) height" &&
                  vertSrc->geoidModel().empty()) &&
                !(vertDst->nameStr() == "CGVD28 height" &&
                  !vertDst->geoidModel().empty() &&
                  vertDst->geoidModel().front()->nameStr() == "HT2_2010" &&
                  vertSrc->nameStr() == "CGVD2013a(2010) height" &&
                  vertSrc->geoidModel().empty())) {
                // If we have a geoid model, force using through it
                bTryThroughIntermediateGeogCRS = true;
            } else {
                bHasTriedVerticalTransforms = true;
                verticalTransforms =
                    createOperations(componentsSrc[1], sourceEpoch,
                                     componentsDst[1], targetEpoch, context);
                // If we didn't find a non-ballpark transformation between
                // the 2 vertical CRS, then try through intermediate geographic
                // CRS
                // Do that although when the geographic CRS of the source and
                // target CRS are not the same, but only if they have a 3D
                // known version, and there is a non-ballpark transformation
                // between them.
                // This helps for "GDA94 + AHD height" to "GDA2020 + AVWS
                // height" going through GDA94 3D and GDA2020 3D
                bTryThroughIntermediateGeogCRS =
                    (verticalTransforms.size() == 1 &&
                     verticalTransforms.front()->hasBallparkTransformation()) ||
                    (!srcGeogIsSameAsDstGeog &&
                     !intermGeogSrc->identifiers().empty() &&
                     !intermGeogDst->identifiers().empty() &&
                     !opsGeogSrcToGeogDst.empty() &&
                     !opsGeogSrcToGeogDst.front()->hasBallparkTransformation());
            }
        }
    }

    if (bTryThroughIntermediateGeogCRS) {
        const auto createWithIntermediateCRS =
            [&sourceCRS, &sourceEpoch, &targetCRS, &targetEpoch, &context,
             &res](const crs::CRSNNPtr &intermCRS) {
                const auto ops1 = createOperations(
                    sourceCRS, sourceEpoch, intermCRS, sourceEpoch, context);
                const auto ops2 = createOperations(
                    intermCRS, targetEpoch, targetCRS, targetEpoch, context);
                for (const auto &op1 : ops1) {
                    for (const auto &op2 : ops2) {
                        try {
                            res.emplace_back(
                                ConcatenatedOperation::createComputeMetadata(
                                    {op1, op2}, disallowEmptyIntersection));
                        } catch (const std::exception &) {
                        }
                    }
                }
            };

        const auto boundSrcHorizontal =
            dynamic_cast<const crs::BoundCRS *>(componentsSrc[0].get());
        const auto boundDstHorizontal =
            dynamic_cast<const crs::BoundCRS *>(componentsDst[0].get());

        // CompoundCRS[something_with_TOWGS84, ...] to CompoundCRS[WGS84, ...]
        if (boundSrcHorizontal != nullptr && boundDstHorizontal == nullptr &&
            boundSrcHorizontal->hubCRS()->_isEquivalentTo(
                componentsDst[0].get(),
                util::IComparable::Criterion::EQUIVALENT)) {
            createWithIntermediateCRS(
                componentsDst[0]->promoteTo3D(std::string(), dbContext));
            if (!res.empty()) {
                return;
            }
        }
        // Inverse of above
        else if (boundDstHorizontal != nullptr &&
                 boundSrcHorizontal == nullptr &&
                 boundDstHorizontal->hubCRS()->_isEquivalentTo(
                     componentsSrc[0].get(),
                     util::IComparable::Criterion::EQUIVALENT)) {
            createWithIntermediateCRS(
                componentsSrc[0]->promoteTo3D(std::string(), dbContext));
            if (!res.empty()) {
                return;
            }
        }

        // Deal with situation like
        // WGS 84 + EGM96 --> ETRS89 + Belfast height where
        // there is a geoid model for EGM96 referenced to WGS 84
        // and a geoid model for Belfast height referenced to ETRS89
        const auto opsSrcToGeog = createOperations(
            sourceCRS, sourceEpoch, intermGeogSrc, sourceEpoch, context);
        const auto opsGeogToTarget = createOperations(
            intermGeogDst, targetEpoch, targetCRS, targetEpoch, context);

        // We preferably accept using an intermediate if the operations
        // to it do not include ballpark operations, but if they do include
        // grids, using such operations result will be better than nothing.
        // This will help for example for "NAD83(CSRS) + CGVD28 height" to
        // "NAD83(CSRS) + CGVD2013(CGG2013) height" where the transformation
        // between "CGVD2013(CGG2013) height" and "NAD83(CSRS)" actually
        // involves going through NAD83(CSRS)v6
        const auto hasKnownGrid =
            [&dbContext](const CoordinateOperationNNPtr &op) {
                const auto grids = op->gridsNeeded(dbContext, true);
                if (grids.empty()) {
                    return false;
                }
                for (const auto &grid : grids) {
                    if (grid.available) {
                        return true;
                    }
                }
                return false;
            };

        const auto hasNonTrivialTransf =
            [&hasKnownGrid](const std::vector<CoordinateOperationNNPtr> &ops) {
                return !ops.empty() &&
                       (!ops.front()->hasBallparkTransformation() ||
                        hasKnownGrid(ops.front()));
            };

        const bool hasNonTrivialSrcTransf = hasNonTrivialTransf(opsSrcToGeog);
        const bool hasNonTrivialTargetTransf =
            hasNonTrivialTransf(opsGeogToTarget);
        double bestAccuracy = -1;
        size_t bestStepCount = 0;
        if (hasNonTrivialSrcTransf && hasNonTrivialTargetTransf) {
            // In first pass, exclude (horizontal) ballpark operations, but
            // accept them in second pass.
            for (int pass = 0; pass <= 1 && res.empty(); pass++) {
                for (const auto &op1 : opsSrcToGeog) {
                    if (pass == 0 && op1->hasBallparkTransformation()) {
                        // std::cerr << "excluded " << op1->nameStr() <<
                        // std::endl;
                        continue;
                    }
                    if (op1->nameStr().find(BALLPARK_VERTICAL_TRANSFORMATION) !=
                        std::string::npos) {
                        continue;
                    }
                    for (const auto &op2 : opsGeogSrcToGeogDst) {
                        for (const auto &op3 : opsGeogToTarget) {
                            if (pass == 0 && op3->hasBallparkTransformation()) {
                                // std::cerr << "excluded " << op3->nameStr() <<
                                // std::endl;
                                continue;
                            }
                            if (op3->nameStr().find(
                                    BALLPARK_VERTICAL_TRANSFORMATION) !=
                                std::string::npos) {
                                continue;
                            }
                            try {
                                res.emplace_back(
                                    ConcatenatedOperation::createComputeMetadata(
                                        srcGeogIsSameAsDstGeog
                                            ? std::vector<
                                                  CoordinateOperationNNPtr>{op1,
                                                                            op3}
                                            : std::vector<
                                                  CoordinateOperationNNPtr>{op1,
                                                                            op2,
                                                                            op3},
                                        disallowEmptyIntersection));
                                const double accuracy = getAccuracy(res.back());
                                const size_t stepCount =
                                    getStepCount(res.back());
                                if (accuracy >= 0 &&
                                    (bestAccuracy < 0 ||
                                     (accuracy < bestAccuracy ||
                                      (accuracy == bestAccuracy &&
                                       stepCount < bestStepCount)))) {
                                    bestAccuracy = accuracy;
                                    bestStepCount = stepCount;
                                }
                            } catch (const std::exception &) {
                            }
                        }
                    }
                }
            }
        }

        const auto createOpsInTwoSteps =
            [&res, bestAccuracy,
             bestStepCount](const std::vector<CoordinateOperationNNPtr> &ops1,
                            const std::vector<CoordinateOperationNNPtr> &ops2) {
                std::vector<CoordinateOperationNNPtr> res2;
                double bestAccuracy2 = -1;
                size_t bestStepCount2 = 0;

                // In first pass, exclude (horizontal) ballpark operations, but
                // accept them in second pass.
                for (int pass = 0; pass <= 1 && res2.empty(); pass++) {
                    for (const auto &op1 : ops1) {
                        if (pass == 0 && op1->hasBallparkTransformation()) {
                            // std::cerr << "excluded " << op1->nameStr() <<
                            // std::endl;
                            continue;
                        }
                        if (op1->nameStr().find(
                                BALLPARK_VERTICAL_TRANSFORMATION) !=
                            std::string::npos) {
                            continue;
                        }
                        for (const auto &op2 : ops2) {
                            if (pass == 0 && op2->hasBallparkTransformation()) {
                                // std::cerr << "excluded " << op2->nameStr() <<
                                // std::endl;
                                continue;
                            }
                            if (op2->nameStr().find(
                                    BALLPARK_VERTICAL_TRANSFORMATION) !=
                                std::string::npos) {
                                continue;
                            }
                            try {
                                res2.emplace_back(
                                    ConcatenatedOperation::
                                        createComputeMetadata(
                                            {op1, op2},
                                            disallowEmptyIntersection));
                                const double accuracy =
                                    getAccuracy(res2.back());
                                const size_t stepCount =
                                    getStepCount(res2.back());
                                if (accuracy >= 0 &&
                                    (bestAccuracy2 < 0 ||
                                     (accuracy < bestAccuracy2 ||
                                      (accuracy == bestAccuracy2 &&
                                       stepCount < bestStepCount2)))) {
                                    bestAccuracy2 = accuracy;
                                    bestStepCount2 = stepCount;
                                }
                            } catch (const std::exception &) {
                            }
                        }
                    }
                }

                // Keep the results of this new attempt, if there are better
                // than the previous ones
                if (bestAccuracy2 >= 0 &&
                    (bestAccuracy < 0 || (bestAccuracy2 < bestAccuracy ||
                                          (bestAccuracy2 == bestAccuracy &&
                                           bestStepCount2 < bestStepCount)))) {
                    res = std::move(res2);
                }
            };

        // If the promoted-to-3D source geographic CRS is not a known object,
        // transformations from it to another 3D one may not be relevant,
        // so try doing source -> geogDst 3D -> dest, if geogDst 3D is a known
        // object
        if (!srcGeog->identifiers().empty() &&
            intermGeogSrc->identifiers().empty() &&
            !intermGeogDst->identifiers().empty() &&
            hasNonTrivialTargetTransf) {
            const auto opsSrcToIntermGeog = createOperations(
                sourceCRS, util::optional<common::DataEpoch>(), intermGeogDst,
                util::optional<common::DataEpoch>(), context);
            if (hasNonTrivialTransf(opsSrcToIntermGeog)) {
                createOpsInTwoSteps(opsSrcToIntermGeog, opsGeogToTarget);
            }
        }
        // Symmetrical situation with the promoted-to-3D target geographic CRS
        else if (!dstGeog->identifiers().empty() &&
                 intermGeogDst->identifiers().empty() &&
                 !intermGeogSrc->identifiers().empty() &&
                 hasNonTrivialSrcTransf) {
            const auto opsIntermGeogToDst = createOperations(
                intermGeogSrc, util::optional<common::DataEpoch>(), targetCRS,
                util::optional<common::DataEpoch>(), context);
            if (hasNonTrivialTransf(opsIntermGeogToDst)) {
                createOpsInTwoSteps(opsSrcToGeog, opsIntermGeogToDst);
            }
        }

        if (!res.empty()) {
            return;
        }

        if (!bHasTriedVerticalTransforms) {
            verticalTransforms =
                createOperations(componentsSrc[1], sourceEpoch,
                                 componentsDst[1], targetEpoch, context);
        }
    }

    for (const auto &verticalTransform : verticalTransforms) {

        auto interpolationCRS =
            NN_NO_CHECK(std::static_pointer_cast<crs::SingleCRS>(srcGeog));
        if (auto interpTransformCRS = verticalTransform->interpolationCRS()) {
            auto interpTransformSingleCRS =
                std::static_pointer_cast<crs::SingleCRS>(interpTransformCRS);
            if (interpTransformSingleCRS) {
                interpolationCRS = NN_NO_CHECK(interpTransformSingleCRS);
            }
        } else {
            auto compSrc0BoundCrs =
                dynamic_cast<crs::BoundCRS *>(componentsSrc[0].get());
            auto compDst0BoundCrs =
                dynamic_cast<crs::BoundCRS *>(componentsDst[0].get());
            if (compSrc0BoundCrs && compDst0BoundCrs &&
                dynamic_cast<crs::GeographicCRS *>(
                    compSrc0BoundCrs->hubCRS().get()) &&
                compSrc0BoundCrs->hubCRS()->_isEquivalentTo(
                    compDst0BoundCrs->hubCRS().get(),
                    util::IComparable::Criterion::EQUIVALENT)) {
                interpolationCRS =
                    NN_NO_CHECK(util::nn_dynamic_pointer_cast<crs::SingleCRS>(
                        compSrc0BoundCrs->hubCRS()));
            } else if (compSrc0BoundCrs && !compDst0BoundCrs &&
                       compSrc0BoundCrs->hubCRS()->_isEquivalentTo(
                           dstGeog.get(),
                           util::IComparable::Criterion::EQUIVALENT)) {
                interpolationCRS = NN_NO_CHECK(
                    std::static_pointer_cast<crs::SingleCRS>(dstGeog));
            }
        }

        // Hack for
        // NAD83_CSRS_1997_xxxx_HT2_1997 to NAD83_CSRS_1997_yyyy_CGVD2013_1997
        // NAD83_CSRS_2002_xxxx_HT2_2002 to NAD83_CSRS_2002_yyyy_CGVD2013_2002
        if (dbContext && sourceEpoch.has_value() && targetEpoch.has_value() &&
            sourceEpoch->coordinateEpoch()._isEquivalentTo(
                targetEpoch->coordinateEpoch()) &&
            srcGeog->_isEquivalentTo(
                dstGeog.get(), util::IComparable::Criterion::EQUIVALENT) &&
            srcGeog->nameStr() == "NAD83(CSRS)v7") {
            const bool is1997 =
                std::abs(sourceEpoch->coordinateEpoch().convertToUnit(
                             common::UnitOfMeasure::YEAR) -
                         1997) < 1e-10;
            const bool is2002 =
                std::abs(sourceEpoch->coordinateEpoch().convertToUnit(
                             common::UnitOfMeasure::YEAR) -
                         2002) < 1e-10;
            try {
                auto authFactoryEPSG = io::AuthorityFactory::create(
                    NN_NO_CHECK(dbContext), "EPSG");
                auto nad83CSRSv7 = authFactoryEPSG->createGeographicCRS("8255");
                if (srcGeog->_isEquivalentTo(nad83CSRSv7.get(),
                                             util::IComparable::Criterion::
                                                 EQUIVALENT) && // NAD83(CSRS)v7
                                                                // 2D
                    ((is1997 &&
                      verticalTransform->nameStr().find(
                          "CGVD28 height to CGVD2013a(1997) height (1)") !=
                          std::string::npos) ||
                     (is2002 &&
                      verticalTransform->nameStr().find(
                          "CGVD28 height to CGVD2013a(2002) height (1)") !=
                          std::string::npos))) {
                    interpolationCRS = std::move(nad83CSRSv7);
                }
            } catch (const std::exception &) {
            }
        }

        const auto opSrcCRSToGeogCRS = createOperations(
            componentsSrc[0], util::optional<common::DataEpoch>(),
            interpolationCRS, util::optional<common::DataEpoch>(), context);
        const auto opGeogCRStoDstCRS = createOperations(
            interpolationCRS, util::optional<common::DataEpoch>(),
            componentsDst[0], util::optional<common::DataEpoch>(), context);
        for (const auto &opSrc : opSrcCRSToGeogCRS) {
            for (const auto &opDst : opGeogCRStoDstCRS) {

                try {
                    auto op = createHorizVerticalHorizPROJBased(
                        sourceCRS, targetCRS, opSrc, verticalTransform, opDst,
                        interpolationCRS, true);
                    res.emplace_back(op);
                } catch (const InvalidOperationEmptyIntersection &) {
                } catch (const io::FormattingException &) {
                }
            }
        }

        if (verticalTransforms.size() == 1U &&
            verticalTransform->hasBallparkTransformation() &&
            context.context->getAuthorityFactory() &&
            dynamic_cast<crs::ProjectedCRS *>(componentsSrc[0].get()) &&
            dynamic_cast<crs::ProjectedCRS *>(componentsDst[0].get()) &&
            verticalTransform->nameStr() ==
                getBallparkTransformationVertToVert(componentsSrc[1],
                                                    componentsDst[1])) {
            // e.g EPSG:3912+EPSG:5779 to EPSG:3794+EPSG:8690
            // "MGI 1901 / Slovene National Grid + SVS2000 height" to
            // "Slovenia 1996 / Slovene National Grid + SVS2010 height"
            // using the "D48/GK to D96/TM (xx)" family of horizontal
            // transformatoins between the projected CRS
            // Cf
            // https://github.com/OSGeo/PROJ/issues/3854#issuecomment-1689964773
            // We restrict to a ballpark vertical transformation for now,
            // but ideally we should deal with a regular vertical transformation
            // but that would involve doing a transformation to its
            // interpolation CRS and we don't have such cases for now.

            std::vector<CoordinateOperationNNPtr> opsHoriz;
            createOperationsFromDatabase(
                componentsSrc[0], util::optional<common::DataEpoch>(),
                componentsDst[0], util::optional<common::DataEpoch>(), context,
                srcGeog.get(), dstGeog.get(), srcGeog.get(), dstGeog.get(),
                /*vertSrc=*/nullptr, /*vertDst=*/nullptr, opsHoriz);
            for (const auto &opHoriz : opsHoriz) {
                res.emplace_back(createHorizNullVerticalPROJBased(
                    sourceCRS, targetCRS, opHoriz, verticalTransform));
            }
        }
    }

    if (verticalTransforms.empty()) {
        auto resTmp = createOperations(
            componentsSrc[0], util::optional<common::DataEpoch>(),
            componentsDst[0], util::optional<common::DataEpoch>(), context);
        for (const auto &op : resTmp) {
            auto opClone = op->shallowClone();
            setCRSs(opClone.get(), sourceCRS, targetCRS);
            res.emplace_back(opClone);
        }
    }
}

// ---------------------------------------------------------------------------

void CoordinateOperationFactory::Private::createOperationsBoundToCompound(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    Private::Context &context, const crs::BoundCRS *boundSrc,
    const crs::CompoundCRS *compoundDst,
    std::vector<CoordinateOperationNNPtr> &res) {

    const auto &authFactory = context.context->getAuthorityFactory();
    const auto dbContext =
        authFactory ? authFactory->databaseContext().as_nullable() : nullptr;

    const auto &componentsDst = compoundDst->componentReferenceSystems();

    // Case of BOUND[NAD83_3D,TOWGS84] to
    // COMPOUND[BOUND[NAD83_2D,TOWGS84],BOUND[VERT[NAVD88],HUB=NAD83_3D,GRID]]
    // ==> We can ignore the TOWGS84 BOUND aspect and just ask for
    // NAD83_3D to COMPOUND[NAD83_2D,BOUND[VERT[NAVD88],HUB=NAD83_3D,GRID]]
    if (componentsDst.size() >= 2) {
        auto srcGeogCRS = boundSrc->baseCRS()->extractGeodeticCRS();
        auto compDst0BoundCrs =
            util::nn_dynamic_pointer_cast<crs::BoundCRS>(componentsDst[0]);
        auto compDst1BoundCrs =
            dynamic_cast<crs::BoundCRS *>(componentsDst[1].get());
        if (srcGeogCRS && compDst0BoundCrs && compDst1BoundCrs) {
            auto compDst0Geog = compDst0BoundCrs->extractGeographicCRS();
            auto compDst1Vert = dynamic_cast<const crs::VerticalCRS *>(
                compDst1BoundCrs->baseCRS().get());
            auto compDst1BoundCrsHubCrsGeog =
                dynamic_cast<const crs::GeographicCRS *>(
                    compDst1BoundCrs->hubCRS().get());
            if (compDst1BoundCrsHubCrsGeog) {
                auto hubDst1Datum =
                    compDst1BoundCrsHubCrsGeog->datumNonNull(dbContext);
                if (compDst0Geog && compDst1Vert &&
                    srcGeogCRS->datumNonNull(dbContext)->isEquivalentTo(
                        hubDst1Datum.get(),
                        util::IComparable::Criterion::EQUIVALENT) &&
                    compDst0Geog->datumNonNull(dbContext)->isEquivalentTo(
                        hubDst1Datum.get(),
                        util::IComparable::Criterion::EQUIVALENT)) {
                    auto srcNew = boundSrc->baseCRS();
                    auto properties = util::PropertyMap().set(
                        common::IdentifiedObject::NAME_KEY,
                        compDst0BoundCrs->nameStr() + " + " +
                            componentsDst[1]->nameStr());
                    auto dstNew = crs::CompoundCRS::create(
                        properties,
                        {NN_NO_CHECK(compDst0BoundCrs), componentsDst[1]});
                    auto tmpRes = createOperations(
                        srcNew, util::optional<common::DataEpoch>(), dstNew,
                        util::optional<common::DataEpoch>(), context);
                    for (const auto &op : tmpRes) {
                        auto opClone = op->shallowClone();
                        setCRSs(opClone.get(), sourceCRS, targetCRS);
                        res.emplace_back(opClone);
                    }
                    return;
                }
            }
        }
    }

    if (!componentsDst.empty()) {
        auto compDst0BoundCrs =
            dynamic_cast<crs::BoundCRS *>(componentsDst[0].get());
        if (compDst0BoundCrs) {
            auto boundSrcHubAsGeogCRS =
                dynamic_cast<crs::GeographicCRS *>(boundSrc->hubCRS().get());
            auto compDst0BoundCrsHubAsGeogCRS =
                dynamic_cast<crs::GeographicCRS *>(
                    compDst0BoundCrs->hubCRS().get());
            if (boundSrcHubAsGeogCRS && compDst0BoundCrsHubAsGeogCRS) {
                const auto boundSrcHubAsGeogCRSDatum =
                    boundSrcHubAsGeogCRS->datumNonNull(dbContext);
                const auto compDst0BoundCrsHubAsGeogCRSDatum =
                    compDst0BoundCrsHubAsGeogCRS->datumNonNull(dbContext);
                if (boundSrcHubAsGeogCRSDatum->_isEquivalentTo(
                        compDst0BoundCrsHubAsGeogCRSDatum.get(),
                        util::IComparable::Criterion::EQUIVALENT)) {
                    auto cs = cs::EllipsoidalCS::
                        createLatitudeLongitudeEllipsoidalHeight(
                            common::UnitOfMeasure::DEGREE,
                            common::UnitOfMeasure::METRE);
                    auto intermGeog3DCRS = util::nn_static_pointer_cast<
                        crs::CRS>(crs::GeographicCRS::create(
                        util::PropertyMap()
                            .set(common::IdentifiedObject::NAME_KEY,
                                 boundSrcHubAsGeogCRS->nameStr())
                            .set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                                 metadata::Extent::WORLD),
                        boundSrcHubAsGeogCRS->datum(),
                        boundSrcHubAsGeogCRS->datumEnsemble(), cs));
                    auto sourceToGeog3DOps = createOperations(
                        sourceCRS, util::optional<common::DataEpoch>(),
                        intermGeog3DCRS, util::optional<common::DataEpoch>(),
                        context);
                    auto geog3DToTargetOps = createOperations(
                        intermGeog3DCRS, util::optional<common::DataEpoch>(),
                        targetCRS, util::optional<common::DataEpoch>(),
                        context);
                    for (const auto &opSrc : sourceToGeog3DOps) {
                        for (const auto &opDst : geog3DToTargetOps) {
                            if (opSrc->targetCRS() && opDst->sourceCRS() &&
                                !opSrc->targetCRS()->_isEquivalentTo(
                                    opDst->sourceCRS().get(),
                                    util::IComparable::Criterion::EQUIVALENT)) {
                                // Shouldn't happen normally, but typically
                                // one of them can be 2D and the other 3D
                                // due to above createOperations() not
                                // exactly setting the expected source and
                                // target CRS.
                                // So create an adapter operation...
                                auto intermOps = createOperations(
                                    NN_NO_CHECK(opSrc->targetCRS()),
                                    util::optional<common::DataEpoch>(),
                                    NN_NO_CHECK(opDst->sourceCRS()),
                                    util::optional<common::DataEpoch>(),
                                    context);
                                if (!intermOps.empty()) {
                                    res.emplace_back(
                                        ConcatenatedOperation::
                                            createComputeMetadata(
                                                {opSrc, intermOps.front(),
                                                 opDst},
                                                disallowEmptyIntersection));
                                }
                            } else {
                                res.emplace_back(
                                    ConcatenatedOperation::
                                        createComputeMetadata(
                                            {opSrc, opDst},
                                            disallowEmptyIntersection));
                            }
                        }
                    }
                    return;
                }
            }
        }

        // e.g transforming from a Bound[something_not_WGS84_but_with_TOWGS84]
        // to a CompoundCRS[WGS84, ...]
        const auto srcBaseSingleCRS =
            dynamic_cast<const crs::SingleCRS *>(boundSrc->baseCRS().get());
        const auto srcGeogCRS = boundSrc->extractGeographicCRS();
        const auto boundSrcHubAsGeogCRS =
            dynamic_cast<const crs::GeographicCRS *>(boundSrc->hubCRS().get());
        const auto comp0Geog = componentsDst[0]->extractGeographicCRS();
        if (srcBaseSingleCRS &&
            srcBaseSingleCRS->coordinateSystem()->axisList().size() == 3 &&
            srcGeogCRS && boundSrcHubAsGeogCRS && comp0Geog &&
            boundSrcHubAsGeogCRS->coordinateSystem()->axisList().size() == 3 &&
            !srcGeogCRS->datumNonNull(dbContext)->isEquivalentTo(
                comp0Geog->datumNonNull(dbContext).get(),
                util::IComparable::Criterion::EQUIVALENT) &&
            boundSrcHubAsGeogCRS &&
            boundSrcHubAsGeogCRS->datumNonNull(dbContext)->isEquivalentTo(
                comp0Geog->datumNonNull(dbContext).get(),
                util::IComparable::Criterion::EQUIVALENT)) {
            const auto ops1 =
                createOperations(sourceCRS, util::optional<common::DataEpoch>(),
                                 boundSrc->hubCRS(),
                                 util::optional<common::DataEpoch>(), context);
            const auto ops2 = createOperations(
                boundSrc->hubCRS(), util::optional<common::DataEpoch>(),
                targetCRS, util::optional<common::DataEpoch>(), context);
            for (const auto &op1 : ops1) {
                for (const auto &op2 : ops2) {
                    try {
                        res.emplace_back(
                            ConcatenatedOperation::createComputeMetadata(
                                {op1, op2}, disallowEmptyIntersection));
                    } catch (const std::exception &) {
                    }
                }
            }
            if (!res.empty()) {
                return;
            }
        }
    }

    // There might be better things to do, but for now just ignore the
    // transformation of the bound CRS
    res = createOperations(boundSrc->baseCRS(),
                           util::optional<common::DataEpoch>(), targetCRS,
                           util::optional<common::DataEpoch>(), context);
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Find a list of CoordinateOperation from sourceCRS to targetCRS.
 *
 * The operations are sorted with the most relevant ones first: by
 * descending
 * area (intersection of the transformation area with the area of interest,
 * or intersection of the transformation with the area of use of the CRS),
 * and
 * by increasing accuracy. Operations with unknown accuracy are sorted last,
 * whatever their area.
 *
 * When one of the source or target CRS has a vertical component but not the
 * other one, the one that has no vertical component is automatically promoted
 * to a 3D version, where its vertical axis is the ellipsoidal height in metres,
 * using the ellipsoid of the base geodetic CRS.
 *
 * @param sourceCRS source CRS.
 * @param targetCRS target CRS.
 * @param context Search context.
 * @return a list
 */
std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::createOperations(
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const CoordinateOperationContextNNPtr &context) const {

#ifdef TRACE_CREATE_OPERATIONS
    ENTER_FUNCTION();
#endif
    // Look if we are called on CRS that have a link to a 'canonical'
    // BoundCRS
    // If so, use that one as input
    const auto &srcBoundCRS = sourceCRS->canonicalBoundCRS();
    const auto &targetBoundCRS = targetCRS->canonicalBoundCRS();
    auto l_sourceCRS = srcBoundCRS ? NN_NO_CHECK(srcBoundCRS) : sourceCRS;
    auto l_targetCRS = targetBoundCRS ? NN_NO_CHECK(targetBoundCRS) : targetCRS;
    const auto &authFactory = context->getAuthorityFactory();

    metadata::ExtentPtr sourceCRSExtent;
    auto l_resolvedSourceCRS =
        crs::CRS::getResolvedCRS(l_sourceCRS, authFactory, sourceCRSExtent);
    metadata::ExtentPtr targetCRSExtent;
    auto l_resolvedTargetCRS =
        crs::CRS::getResolvedCRS(l_targetCRS, authFactory, targetCRSExtent);
    if (context->getSourceAndTargetCRSExtentUse() ==
        CoordinateOperationContext::SourceTargetCRSExtentUse::NONE) {
        // Make sure *not* to use CRS extent if requested to ignore it
        sourceCRSExtent.reset();
        targetCRSExtent.reset();
    }
    Private::Context contextPrivate(sourceCRSExtent, targetCRSExtent, context);

    if (context->getSourceAndTargetCRSExtentUse() ==
        CoordinateOperationContext::SourceTargetCRSExtentUse::INTERSECTION) {
        if (sourceCRSExtent && targetCRSExtent &&
            !sourceCRSExtent->intersects(NN_NO_CHECK(targetCRSExtent))) {
            return std::vector<CoordinateOperationNNPtr>();
        }
    }

    auto resFiltered = filterAndSort(
        Private::createOperations(
            l_resolvedSourceCRS, context->getSourceCoordinateEpoch(),
            l_resolvedTargetCRS, context->getTargetCoordinateEpoch(),
            contextPrivate),
        context, sourceCRSExtent, targetCRSExtent);
    if (context->getSourceCoordinateEpoch().has_value() ||
        context->getTargetCoordinateEpoch().has_value()) {
        std::vector<CoordinateOperationNNPtr> res;
        res.reserve(resFiltered.size());
        for (const auto &op : resFiltered) {
            auto opClone = op->shallowClone();
            opClone->setSourceCoordinateEpoch(
                context->getSourceCoordinateEpoch());
            opClone->setTargetCoordinateEpoch(
                context->getTargetCoordinateEpoch());
            res.emplace_back(opClone);
        }
        return res;
    }
    return resFiltered;
}

// ---------------------------------------------------------------------------

/** \brief Find a list of CoordinateOperation from a source coordinate metadata
 * to targetCRS.
 * @param sourceCoordinateMetadata source CoordinateMetadata.
 * @param targetCRS target CRS.
 * @param context Search context.
 * @return a list
 * @since 9.2
 */
std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::createOperations(
    const coordinates::CoordinateMetadataNNPtr &sourceCoordinateMetadata,
    const crs::CRSNNPtr &targetCRS,
    const CoordinateOperationContextNNPtr &context) const {
    auto newContext = context->clone();
    newContext->setSourceCoordinateEpoch(
        sourceCoordinateMetadata->coordinateEpoch());
    return createOperations(sourceCoordinateMetadata->crs(), targetCRS,
                            newContext);
}

// ---------------------------------------------------------------------------

/** \brief Find a list of CoordinateOperation from a source CRS to a target
 * coordinate metadata.
 * @param sourceCRS source CRS.
 * @param targetCoordinateMetadata target CoordinateMetadata.
 * @param context Search context.
 * @return a list
 * @since 9.2
 */
std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::createOperations(
    const crs::CRSNNPtr &sourceCRS,
    const coordinates::CoordinateMetadataNNPtr &targetCoordinateMetadata,
    const CoordinateOperationContextNNPtr &context) const {
    auto newContext = context->clone();
    newContext->setTargetCoordinateEpoch(
        targetCoordinateMetadata->coordinateEpoch());
    return createOperations(sourceCRS, targetCoordinateMetadata->crs(),
                            newContext);
}

// ---------------------------------------------------------------------------

/** \brief Find a list of CoordinateOperation from a source coordinate metadata
 * to a target coordinate metadata.
 *
 * Both source_crs and target_crs can be a CoordinateMetadata
 * with an associated coordinate epoch, to perform changes of coordinate epochs.
 * Note however than this is in practice limited to use of velocity grids inside
 * the same dynamic CRS.
 *
 * @param sourceCoordinateMetadata source CoordinateMetadata.
 * @param targetCoordinateMetadata target CoordinateMetadata.
 * @param context Search context.
 * @return a list
 * @since 9.4
 */
std::vector<CoordinateOperationNNPtr>
CoordinateOperationFactory::createOperations(
    const coordinates::CoordinateMetadataNNPtr &sourceCoordinateMetadata,
    const coordinates::CoordinateMetadataNNPtr &targetCoordinateMetadata,
    const CoordinateOperationContextNNPtr &context) const {
    auto newContext = context->clone();
    newContext->setSourceCoordinateEpoch(
        sourceCoordinateMetadata->coordinateEpoch());
    newContext->setTargetCoordinateEpoch(
        targetCoordinateMetadata->coordinateEpoch());
    return createOperations(sourceCoordinateMetadata->crs(),
                            targetCoordinateMetadata->crs(), newContext);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CoordinateOperationFactory.
 */
CoordinateOperationFactoryNNPtr CoordinateOperationFactory::create() {
    return NN_NO_CHECK(
        CoordinateOperationFactory::make_unique<CoordinateOperationFactory>());
}

// ---------------------------------------------------------------------------

} // namespace operation

namespace crs {
// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

crs::CRSNNPtr CRS::getResolvedCRS(const crs::CRSNNPtr &crs,
                                  const io::AuthorityFactoryPtr &authFactory,
                                  metadata::ExtentPtr &extentOut) {
    if (crs->hasOver()) {
        return crs;
    }
    const auto &ids = crs->identifiers();
    const auto &name = crs->nameStr();

    bool approxExtent;
    extentOut = operation::getExtentPossiblySynthetized(crs, approxExtent);

    // We try to "identify" the provided CRS with the ones of the database,
    // but in a more restricted way that what identify() does.
    // If we get a match from id in priority, and from name as a fallback, and
    // that they are equivalent to the input CRS, then use the identified CRS.
    // Even if they aren't equivalent, we update extentOut with the one of the
    // identified CRS if our input one is absent/not reliable.

    const auto tryToIdentifyByName =
        [&crs, &name, &authFactory, approxExtent,
         &extentOut](io::AuthorityFactory::ObjectType objectType) {
            if (name != "unknown" && name != "unnamed") {
                auto matches = authFactory->createObjectsFromName(
                    name, {objectType}, false, 2);
                if (matches.size() == 1) {
                    auto match =
                        util::nn_static_pointer_cast<crs::CRS>(matches.front());
                    if (approxExtent || !extentOut) {
                        extentOut = operation::getExtent(match);
                    }
                    if (match->isEquivalentTo(
                            crs.get(),
                            util::IComparable::Criterion::EQUIVALENT)) {
                        return match;
                    }
                }
            }
            return crs;
        };

    auto geogCRS = dynamic_cast<crs::GeographicCRS *>(crs.get());
    if (geogCRS && authFactory) {
        if (!ids.empty()) {
            const auto tmpAuthFactory = io::AuthorityFactory::create(
                authFactory->databaseContext(), *ids.front()->codeSpace());
            try {
                auto resolvedCrs(
                    tmpAuthFactory->createGeographicCRS(ids.front()->code()));
                if (approxExtent || !extentOut) {
                    extentOut = operation::getExtent(resolvedCrs);
                }
                if (resolvedCrs->isEquivalentTo(
                        crs.get(), util::IComparable::Criterion::EQUIVALENT)) {
                    return util::nn_static_pointer_cast<crs::CRS>(resolvedCrs);
                }
            } catch (const std::exception &) {
            }
        } else {
            return tryToIdentifyByName(
                geogCRS->coordinateSystem()->axisList().size() == 2
                    ? io::AuthorityFactory::ObjectType::GEOGRAPHIC_2D_CRS
                    : io::AuthorityFactory::ObjectType::GEOGRAPHIC_3D_CRS);
        }
    }

    auto projectedCrs = dynamic_cast<crs::ProjectedCRS *>(crs.get());
    if (projectedCrs && authFactory) {
        if (!ids.empty()) {
            const auto tmpAuthFactory = io::AuthorityFactory::create(
                authFactory->databaseContext(), *ids.front()->codeSpace());
            try {
                auto resolvedCrs(
                    tmpAuthFactory->createProjectedCRS(ids.front()->code()));
                if (approxExtent || !extentOut) {
                    extentOut = operation::getExtent(resolvedCrs);
                }
                if (resolvedCrs->isEquivalentTo(
                        crs.get(), util::IComparable::Criterion::EQUIVALENT)) {
                    return util::nn_static_pointer_cast<crs::CRS>(resolvedCrs);
                }
            } catch (const std::exception &) {
            }
        } else {
            return tryToIdentifyByName(
                io::AuthorityFactory::ObjectType::PROJECTED_CRS);
        }
    }

    auto compoundCrs = dynamic_cast<crs::CompoundCRS *>(crs.get());
    if (compoundCrs && authFactory) {
        if (!ids.empty()) {
            const auto tmpAuthFactory = io::AuthorityFactory::create(
                authFactory->databaseContext(), *ids.front()->codeSpace());
            try {
                auto resolvedCrs(
                    tmpAuthFactory->createCompoundCRS(ids.front()->code()));
                if (approxExtent || !extentOut) {
                    extentOut = operation::getExtent(resolvedCrs);
                }
                if (resolvedCrs->isEquivalentTo(
                        crs.get(), util::IComparable::Criterion::EQUIVALENT)) {
                    return util::nn_static_pointer_cast<crs::CRS>(resolvedCrs);
                }
            } catch (const std::exception &) {
            }
        } else {
            auto outCrs = tryToIdentifyByName(
                io::AuthorityFactory::ObjectType::COMPOUND_CRS);
            const auto &components = compoundCrs->componentReferenceSystems();
            if (outCrs.get() != crs.get()) {
                bool hasGeoid = false;
                if (components.size() == 2) {
                    auto vertCRS =
                        dynamic_cast<crs::VerticalCRS *>(components[1].get());
                    if (vertCRS && !vertCRS->geoidModel().empty()) {
                        hasGeoid = true;
                    }
                }
                if (!hasGeoid) {
                    return outCrs;
                }
            }
            if (approxExtent || !extentOut) {
                // If we still did not get a reliable extent, then try to
                // resolve the components of the compoundCRS, and take the
                // intersection of their extent.
                extentOut = metadata::ExtentPtr();
                for (const auto &component : components) {
                    metadata::ExtentPtr componentExtent;
                    getResolvedCRS(component, authFactory, componentExtent);
                    if (extentOut && componentExtent)
                        extentOut = extentOut->intersection(
                            NN_NO_CHECK(componentExtent));
                    else if (componentExtent)
                        extentOut = std::move(componentExtent);
                }
            }
        }
    }
    return crs;
}

//! @endcond

} // namespace crs
NS_PROJ_END
