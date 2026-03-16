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
#include "proj/crs.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/crs_internal.hpp"
#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

#include "coordinateoperation_internal.hpp"
#include "oputils.hpp"

// PROJ include order is sensitive
// clang-format off
#include "proj.h"
#include "proj_internal.h" // M_PI
// clang-format on
#include "proj_constants.h"

#include "proj_json_streaming_writer.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <vector>

using namespace NS_PROJ::internal;

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct ConcatenatedOperation::Private {
    std::vector<CoordinateOperationNNPtr> operations_{};
    bool computedName_ = false;

    explicit Private(const std::vector<CoordinateOperationNNPtr> &operationsIn)
        : operations_(operationsIn) {}
    Private(const Private &) = default;
};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ConcatenatedOperation::~ConcatenatedOperation() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ConcatenatedOperation::ConcatenatedOperation(const ConcatenatedOperation &other)
    : CoordinateOperation(other), d(std::make_unique<Private>(*(other.d))) {}
//! @endcond

// ---------------------------------------------------------------------------

ConcatenatedOperation::ConcatenatedOperation(
    const std::vector<CoordinateOperationNNPtr> &operationsIn)
    : CoordinateOperation(), d(std::make_unique<Private>(operationsIn)) {
    for (const auto &op : operationsIn) {
        if (op->requiresPerCoordinateInputTime()) {
            setRequiresPerCoordinateInputTime(true);
            break;
        }
    }
}

// ---------------------------------------------------------------------------

/** \brief Return the operation steps of the concatenated operation.
 *
 * @return the operation steps.
 */
const std::vector<CoordinateOperationNNPtr> &
ConcatenatedOperation::operations() const {
    return d->operations_;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static bool areCRSMoreOrLessEquivalent(const crs::CRS *a, const crs::CRS *b) {
    const auto &aIds = a->identifiers();
    const auto &bIds = b->identifiers();
    if (aIds.size() == 1 && bIds.size() == 1 &&
        aIds[0]->code() == bIds[0]->code() &&
        *aIds[0]->codeSpace() == *bIds[0]->codeSpace()) {
        return true;
    }
    if (a->_isEquivalentTo(b, util::IComparable::Criterion::EQUIVALENT)) {
        return true;
    }
    // This is for example for EPSG:10146 which is EPSG:9471
    // (INAGeoid2020 v1 height)
    // to EPSG:20036 (INAGeoid2020 v2 height), but chains
    // EPSG:9629 (SRGI2013 to SRGI2013 + INAGeoid2020 v1 height (1))
    // with EPSG:10145 (SRGI2013 to SRGI2013 + INAGeoid2020 v2 height (1))
    const auto compoundA = dynamic_cast<const crs::CompoundCRS *>(a);
    const auto compoundB = dynamic_cast<const crs::CompoundCRS *>(b);
    if (compoundA && !compoundB)
        return areCRSMoreOrLessEquivalent(
            compoundA->componentReferenceSystems()[1].get(), b);
    else if (!compoundA && compoundB)
        return areCRSMoreOrLessEquivalent(
            a, compoundB->componentReferenceSystems()[1].get());
    return false;
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a ConcatenatedOperation
 *
 * @param properties See \ref general_properties. At minimum the name should
 * be
 * defined.
 * @param operationsIn Vector of the CoordinateOperation steps.
 * @param accuracies Vector of positional accuracy (might be empty).
 * @return new Transformation.
 * @throws InvalidOperation if the object cannot be constructed.
 */
ConcatenatedOperationNNPtr ConcatenatedOperation::create(
    const util::PropertyMap &properties,
    const std::vector<CoordinateOperationNNPtr> &operationsIn,
    const std::vector<metadata::PositionalAccuracyNNPtr>
        &accuracies) // throw InvalidOperation
{
    if (operationsIn.size() < 2) {
        throw InvalidOperation(
            "ConcatenatedOperation must have at least 2 operations");
    }
    crs::CRSPtr lastTargetCRS;

    crs::CRSPtr interpolationCRS;
    bool interpolationCRSValid = true;
    for (size_t i = 0; i < operationsIn.size(); i++) {
        auto l_sourceCRS = operationsIn[i]->sourceCRS();
        auto l_targetCRS = operationsIn[i]->targetCRS();

        if (interpolationCRSValid) {
            auto subOpInterpCRS = operationsIn[i]->interpolationCRS();
            if (interpolationCRS == nullptr)
                interpolationCRS = std::move(subOpInterpCRS);
            else if (subOpInterpCRS == nullptr ||
                     !(subOpInterpCRS->isEquivalentTo(
                         interpolationCRS.get(),
                         util::IComparable::Criterion::EQUIVALENT))) {
                interpolationCRS = nullptr;
                interpolationCRSValid = false;
            }
        }

        if (l_sourceCRS == nullptr || l_targetCRS == nullptr) {
            throw InvalidOperation("At least one of the operation lacks a "
                                   "source and/or target CRS");
        }
        if (i >= 1) {
            if (!areCRSMoreOrLessEquivalent(l_sourceCRS.get(),
                                            lastTargetCRS.get())) {
#ifdef DEBUG_CONCATENATED_OPERATION
                std::cerr << "Step " << i - 1 << ": "
                          << operationsIn[i - 1]->nameStr() << std::endl;
                std::cerr << "Step " << i << ": " << operationsIn[i]->nameStr()
                          << std::endl;
                {
                    auto f(io::WKTFormatter::create(
                        io::WKTFormatter::Convention::WKT2_2019));
                    std::cerr << "Source CRS of step " << i << ":" << std::endl;
                    std::cerr << l_sourceCRS->exportToWKT(f.get()) << std::endl;
                }
                {
                    auto f(io::WKTFormatter::create(
                        io::WKTFormatter::Convention::WKT2_2019));
                    std::cerr << "Target CRS of step " << i - 1 << ":"
                              << std::endl;
                    std::cerr << lastTargetCRS->exportToWKT(f.get())
                              << std::endl;
                }
#endif
                throw InvalidOperation(
                    "Inconsistent chaining of CRS in operations");
            }
        }
        lastTargetCRS = std::move(l_targetCRS);
    }

    // When chaining VerticalCRS -> GeographicCRS -> VerticalCRS, use
    // GeographicCRS as the interpolationCRS
    const auto l_sourceCRS = NN_NO_CHECK(operationsIn[0]->sourceCRS());
    const auto l_targetCRS = NN_NO_CHECK(operationsIn.back()->targetCRS());
    if (operationsIn.size() == 2 && interpolationCRS == nullptr &&
        dynamic_cast<const crs::VerticalCRS *>(l_sourceCRS.get()) != nullptr &&
        dynamic_cast<const crs::VerticalCRS *>(l_targetCRS.get()) != nullptr) {
        const auto geog1 = dynamic_cast<crs::GeographicCRS *>(
            operationsIn[0]->targetCRS().get());
        const auto geog2 = dynamic_cast<crs::GeographicCRS *>(
            operationsIn[1]->sourceCRS().get());
        if (geog1 != nullptr && geog2 != nullptr &&
            geog1->_isEquivalentTo(geog2,
                                   util::IComparable::Criterion::EQUIVALENT)) {
            interpolationCRS = operationsIn[0]->targetCRS();
        }
    }

    auto op = ConcatenatedOperation::nn_make_shared<ConcatenatedOperation>(
        operationsIn);
    op->assignSelf(op);
    op->setProperties(properties);
    op->setCRSs(l_sourceCRS, l_targetCRS, interpolationCRS);
    op->setAccuracies(accuracies);
#ifdef DEBUG_CONCATENATED_OPERATION
    {
        auto f(
            io::WKTFormatter::create(io::WKTFormatter::Convention::WKT2_2019));
        std::cerr << "ConcatenatedOperation::create()" << std::endl;
        std::cerr << op->exportToWKT(f.get()) << std::endl;
    }
#endif
    return op;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

/* static */ void
ConcatenatedOperation::setCRSsUpdateInverse(CoordinateOperation *co,
                                            const crs::CRSNNPtr &sourceCRS,
                                            const crs::CRSNNPtr &targetCRS) {

    co->setCRSsUpdateInverse(sourceCRS, targetCRS, co->interpolationCRS());
}

// ---------------------------------------------------------------------------

void ConcatenatedOperation::fixSteps(
    const crs::CRSNNPtr &concatOpSourceCRS,
    const crs::CRSNNPtr &concatOpTargetCRS,
    std::vector<CoordinateOperationNNPtr> &operationsInOut,
    const io::DatabaseContextPtr & /*dbContext*/, bool fixDirectionAllowed) {

    // Set of heuristics to assign CRS to steps, and possibly reverse them.

    const auto isGeographic = [](const crs::CRS *crs) -> bool {
        return dynamic_cast<const crs::GeographicCRS *>(crs) != nullptr;
    };

    const auto isGeocentric = [](const crs::CRS *crs) -> bool {
        const auto geodCRS = dynamic_cast<const crs::GeodeticCRS *>(crs);
        return (geodCRS && geodCRS->isGeocentric());
    };

    // Apply axis order reversal operation on first operation if needed
    // to set CRSs on it
    if (operationsInOut.size() >= 1) {
        auto &op = operationsInOut.front();
        auto l_sourceCRS = op->sourceCRS();
        auto l_targetCRS = op->targetCRS();
        auto conv = dynamic_cast<const Conversion *>(op.get());
        if (conv && !l_sourceCRS && !l_targetCRS &&
            isAxisOrderReversal(conv->method()->getEPSGCode())) {
            auto reversedCRS = concatOpSourceCRS->applyAxisOrderReversal(
                NORMALIZED_AXIS_ORDER_SUFFIX_STR);
            setCRSsUpdateInverse(op.get(), concatOpSourceCRS, reversedCRS);
        }
    }

    // Apply axis order reversal operation on last operation if needed
    // to set CRSs on it
    if (operationsInOut.size() >= 2) {
        auto &op = operationsInOut.back();
        auto l_sourceCRS = op->sourceCRS();
        auto l_targetCRS = op->targetCRS();
        auto conv = dynamic_cast<const Conversion *>(op.get());
        if (conv && !l_sourceCRS && !l_targetCRS &&
            isAxisOrderReversal(conv->method()->getEPSGCode())) {
            auto reversedCRS = concatOpTargetCRS->applyAxisOrderReversal(
                NORMALIZED_AXIS_ORDER_SUFFIX_STR);
            setCRSsUpdateInverse(op.get(), reversedCRS, concatOpTargetCRS);
        }
    }

    // If the first operation is a transformation whose target CRS matches the
    // source CRS of the concatenated operation, then reverse it.
    if (fixDirectionAllowed && operationsInOut.size() >= 2) {
        auto &op = operationsInOut.front();
        auto l_sourceCRS = op->sourceCRS();
        auto l_targetCRS = op->targetCRS();
        if (l_sourceCRS && l_targetCRS &&
            !areCRSMoreOrLessEquivalent(l_sourceCRS.get(),
                                        concatOpSourceCRS.get()) &&
            areCRSMoreOrLessEquivalent(l_targetCRS.get(),
                                       concatOpSourceCRS.get())) {
            op = op->inverse();
        }
    }

    // If the last operation is a transformation whose source CRS matches the
    // target CRS of the concatenated operation, then reverse it.
    if (fixDirectionAllowed && operationsInOut.size() >= 2) {
        auto &op = operationsInOut.back();
        auto l_sourceCRS = op->sourceCRS();
        auto l_targetCRS = op->targetCRS();
        if (l_sourceCRS && l_targetCRS &&
            !areCRSMoreOrLessEquivalent(l_targetCRS.get(),
                                        concatOpTargetCRS.get()) &&
            areCRSMoreOrLessEquivalent(l_sourceCRS.get(),
                                       concatOpTargetCRS.get())) {
            op = op->inverse();
        }
    }

    const auto extractDerivedCRS =
        [](const crs::CRS *crs) -> const crs::DerivedCRS * {
        auto derivedCRS = dynamic_cast<const crs::DerivedCRS *>(crs);
        if (derivedCRS)
            return derivedCRS;
        auto compoundCRS = dynamic_cast<const crs::CompoundCRS *>(crs);
        if (compoundCRS) {
            derivedCRS = dynamic_cast<const crs::DerivedCRS *>(
                compoundCRS->componentReferenceSystems().front().get());
            if (derivedCRS)
                return derivedCRS;
        }
        return nullptr;
    };

    for (size_t i = 0; i < operationsInOut.size(); ++i) {
        auto &op = operationsInOut[i];
        auto l_sourceCRS = op->sourceCRS();
        auto l_targetCRS = op->targetCRS();
        auto conv = dynamic_cast<const Conversion *>(op.get());
        if (conv && i == 0 && !l_sourceCRS && !l_targetCRS) {
            if (auto derivedCRS = extractDerivedCRS(concatOpSourceCRS.get())) {
                if (i + 1 < operationsInOut.size()) {
                    // use the sourceCRS of the next operation as our target CRS
                    l_targetCRS = operationsInOut[i + 1]->sourceCRS();
                    // except if it looks like the next operation should
                    // actually be reversed !!!
                    if (l_targetCRS &&
                        !areCRSMoreOrLessEquivalent(
                            l_targetCRS.get(), derivedCRS->baseCRS().get()) &&
                        operationsInOut[i + 1]->targetCRS() &&
                        areCRSMoreOrLessEquivalent(
                            operationsInOut[i + 1]->targetCRS().get(),
                            derivedCRS->baseCRS().get())) {
                        l_targetCRS = operationsInOut[i + 1]->targetCRS();
                    }
                }
                if (!l_targetCRS) {
                    l_targetCRS = derivedCRS->baseCRS().as_nullable();
                }
                auto invConv =
                    util::nn_dynamic_pointer_cast<InverseConversion>(op);
                auto nn_targetCRS = NN_NO_CHECK(l_targetCRS);
                if (invConv) {
                    setCRSsUpdateInverse(op.get(), concatOpSourceCRS,
                                         nn_targetCRS);
                } else if (fixDirectionAllowed) {
                    op->setCRSs(nn_targetCRS, concatOpSourceCRS, nullptr);
                    op = op->inverse();
                }
            } else if (i + 1 < operationsInOut.size()) {
                /* coverity[copy_paste_error] */
                l_targetCRS = operationsInOut[i + 1]->sourceCRS();
                if (l_targetCRS) {
                    setCRSsUpdateInverse(op.get(), concatOpSourceCRS,
                                         NN_NO_CHECK(l_targetCRS));
                }
            }
        } else if (conv && i + 1 == operationsInOut.size() && !l_sourceCRS &&
                   !l_targetCRS) {
            auto derivedCRS = extractDerivedCRS(concatOpTargetCRS.get());
            if (derivedCRS) {
                if (i >= 1) {
                    // use the targetCRS of the previous operation as our source
                    // CRS
                    l_sourceCRS = operationsInOut[i - 1]->targetCRS();
                    // except if it looks like the previous operation should
                    // actually be reversed !!!
                    if (l_sourceCRS &&
                        !areCRSMoreOrLessEquivalent(
                            l_sourceCRS.get(), derivedCRS->baseCRS().get()) &&
                        operationsInOut[i - 1]->sourceCRS() &&
                        areCRSMoreOrLessEquivalent(
                            operationsInOut[i - 1]->sourceCRS().get(),
                            derivedCRS->baseCRS().get())) {
                        l_sourceCRS = operationsInOut[i - 1]->sourceCRS();
                        operationsInOut[i - 1] =
                            operationsInOut[i - 1]->inverse();
                    }
                }
                if (!l_sourceCRS) {
                    l_sourceCRS = derivedCRS->baseCRS().as_nullable();
                }
                setCRSsUpdateInverse(op.get(), NN_NO_CHECK(l_sourceCRS),
                                     concatOpTargetCRS);
            } else if (i >= 1) {
                l_sourceCRS = operationsInOut[i - 1]->targetCRS();
                if (l_sourceCRS) {
                    derivedCRS = extractDerivedCRS(l_sourceCRS.get());
                    if (fixDirectionAllowed && derivedCRS &&
                        conv->isEquivalentTo(
                            derivedCRS->derivingConversion().get(),
                            util::IComparable::Criterion::EQUIVALENT)) {
                        op = op->inverse();
                    }
                    setCRSsUpdateInverse(op.get(), NN_NO_CHECK(l_sourceCRS),
                                         concatOpTargetCRS);
                }
            }
        } else if (conv && i > 0 && i < operationsInOut.size() - 1) {

            l_sourceCRS = operationsInOut[i - 1]->targetCRS();
            l_targetCRS = operationsInOut[i + 1]->sourceCRS();
            // For an intermediate conversion, use the target CRS of the
            // previous step and the source CRS of the next step
            if (l_sourceCRS && l_targetCRS) {
                // If the sourceCRS is a projectedCRS and the target a
                // geographic one, then we must inverse the operation. See
                // https://github.com/OSGeo/PROJ/issues/2817
                if (fixDirectionAllowed &&
                    dynamic_cast<const crs::ProjectedCRS *>(
                        l_sourceCRS.get()) &&
                    dynamic_cast<const crs::GeographicCRS *>(
                        l_targetCRS.get())) {
                    op = op->inverse();
                    setCRSsUpdateInverse(op.get(), NN_NO_CHECK(l_sourceCRS),
                                         NN_NO_CHECK(l_targetCRS));
                } else {
                    setCRSsUpdateInverse(op.get(), NN_NO_CHECK(l_sourceCRS),
                                         NN_NO_CHECK(l_targetCRS));

                    // Deal with special case of
                    // https://github.com/OSGeo/PROJ/issues/4116 where EPSG:7989
                    // -- NAVD88 height to NAVD88 depth conversion is chained
                    // with "NAD83(FBN)+LMSL to NAD83(FBN)+NAVD88 depth" The
                    // latter must thus be inversed
                    const auto nPosTo = conv->nameStr().find(" to ");
                    const auto nPosToNextOp =
                        operationsInOut[i + 1]->nameStr().find(" to ");
                    if (fixDirectionAllowed && nPosTo != std::string::npos &&
                        nPosToNextOp != std::string::npos) {
                        const std::string convTo =
                            conv->nameStr().substr(nPosTo + strlen(" to "));
                        const std::string nextOpFrom =
                            operationsInOut[i + 1]->nameStr().substr(
                                0, nPosToNextOp);
                        const std::string nextOpTo =
                            operationsInOut[i + 1]->nameStr().substr(
                                nPosToNextOp + strlen(" to "));
                        if (nextOpTo.find(convTo) != std::string::npos &&
                            nextOpFrom.find(convTo) == std::string::npos &&
                            operationsInOut[i + 1]->sourceCRS()) {
                            operationsInOut[i + 1] =
                                operationsInOut[i + 1]->inverse();

                            setCRSsUpdateInverse(
                                op.get(), NN_NO_CHECK(l_sourceCRS),
                                NN_NO_CHECK(
                                    operationsInOut[i + 1]->sourceCRS()));
                        }
                    }
                }
            } else if (l_sourceCRS && l_targetCRS == nullptr &&
                       conv->method()->getEPSGCode() ==
                           EPSG_CODE_METHOD_HEIGHT_DEPTH_REVERSAL) {
                // Needed for EPSG:7987 e.g.
                auto vertCRS =
                    dynamic_cast<const crs::VerticalCRS *>(l_sourceCRS.get());
                if (vertCRS && ends_with(l_sourceCRS->nameStr(), " height") &&
                    &vertCRS->coordinateSystem()->axisList()[0]->direction() ==
                        &cs::AxisDirection::UP) {
                    setCRSsUpdateInverse(
                        op.get(), NN_NO_CHECK(l_sourceCRS),
                        crs::VerticalCRS::create(
                            util::PropertyMap().set(
                                common::IdentifiedObject::NAME_KEY,
                                l_sourceCRS->nameStr().substr(
                                    0, l_sourceCRS->nameStr().size() -
                                           strlen(" height")) +
                                    " depth"),
                            vertCRS->datum(), vertCRS->datumEnsemble(),
                            cs::VerticalCS::create(
                                util::PropertyMap(),
                                cs::CoordinateSystemAxis::create(
                                    util::PropertyMap().set(
                                        common::IdentifiedObject::NAME_KEY,
                                        "Gravity-related depth"),
                                    "D", cs::AxisDirection::DOWN,
                                    vertCRS->coordinateSystem()
                                        ->axisList()[0]
                                        ->unit()))));
                }
            }
        } else if (!conv && l_sourceCRS && l_targetCRS) {

            // Transformations might be mentioned in their forward directions,
            // whereas we should instead use the reverse path.
            auto prevOpTarget = (i == 0) ? concatOpSourceCRS.as_nullable()
                                         : operationsInOut[i - 1]->targetCRS();
            if (prevOpTarget == nullptr) {
                throw InvalidOperation(
                    "Cannot determine targetCRS of operation at step " +
                    toString(static_cast<int>(i)));
            }
            if (areCRSMoreOrLessEquivalent(l_sourceCRS.get(),
                                           prevOpTarget.get())) {
                // do nothing
            } else if (fixDirectionAllowed &&
                       areCRSMoreOrLessEquivalent(l_targetCRS.get(),
                                                  prevOpTarget.get())) {
                op = op->inverse();
            }
            // Below is needed for EPSG:9103 which chains NAD83(2011) geographic
            // 2D with NAD83(2011) geocentric
            else if (l_sourceCRS->nameStr() == prevOpTarget->nameStr() &&
                     ((isGeographic(l_sourceCRS.get()) &&
                       isGeocentric(prevOpTarget.get())) ||
                      (isGeocentric(l_sourceCRS.get()) &&
                       isGeographic(prevOpTarget.get())))) {
                auto newOp(Conversion::createGeographicGeocentric(
                    NN_NO_CHECK(prevOpTarget), NN_NO_CHECK(l_sourceCRS)));
                operationsInOut.insert(operationsInOut.begin() + i, newOp);
            } else if (l_targetCRS->nameStr() == prevOpTarget->nameStr() &&
                       ((isGeographic(l_targetCRS.get()) &&
                         isGeocentric(prevOpTarget.get())) ||
                        (isGeocentric(l_targetCRS.get()) &&
                         isGeographic(prevOpTarget.get())))) {
                auto newOp(Conversion::createGeographicGeocentric(
                    NN_NO_CHECK(prevOpTarget), NN_NO_CHECK(l_targetCRS)));
                operationsInOut.insert(operationsInOut.begin() + i, newOp);
                // Particular case for https://github.com/OSGeo/PROJ/issues/3819
                // where the antepenultimate transformation goes to A
                // (geographic 3D) // and the last transformation is a NADCON 3D
                // but between A (geographic 2D) to B (geographic 2D), and the
                // concatenated transformation target CRS is B (geographic 3D)
                // This is due to an oddity of the EPSG database that registers
                // the NADCON 3D transformation between the 2D geographic CRS
                // and not the 3D ones.
            } else if (i + 1 == operationsInOut.size() &&
                       l_sourceCRS->nameStr() == prevOpTarget->nameStr() &&
                       l_targetCRS->nameStr() == concatOpTargetCRS->nameStr() &&
                       isGeographic(l_targetCRS.get()) &&
                       isGeographic(concatOpTargetCRS.get()) &&
                       isGeographic(l_sourceCRS.get()) &&
                       isGeographic(prevOpTarget.get()) &&
                       dynamic_cast<const crs::GeographicCRS *>(
                           prevOpTarget.get())
                               ->coordinateSystem()
                               ->axisList()
                               .size() == 3 &&
                       dynamic_cast<const crs::GeographicCRS *>(
                           l_sourceCRS.get())
                               ->coordinateSystem()
                               ->axisList()
                               .size() == 2 &&
                       dynamic_cast<const crs::GeographicCRS *>(
                           l_targetCRS.get())
                               ->coordinateSystem()
                               ->axisList()
                               .size() == 2) {
                const auto transf =
                    dynamic_cast<const Transformation *>(op.get());
                if (transf &&
                    (transf->method()->getEPSGCode() ==
                         EPSG_CODE_METHOD_NADCON5_3D ||
                     transf->parameterValue(
                         PROJ_WKT2_PARAMETER_LATITUDE_LONGITUDE_ELLIPOISDAL_HEIGHT_DIFFERENCE_FILE,
                         0))) {
                    setCRSsUpdateInverse(op.get(), NN_NO_CHECK(prevOpTarget),
                                         concatOpTargetCRS);
                }
            }
        }
    }

    if (!operationsInOut.empty()) {
        auto l_sourceCRS = operationsInOut.front()->sourceCRS();
        if (l_sourceCRS && !areCRSMoreOrLessEquivalent(
                               l_sourceCRS.get(), concatOpSourceCRS.get())) {
            throw InvalidOperation("The source CRS of the first step of "
                                   "concatenated operation is not the same "
                                   "as the source CRS of the concatenated "
                                   "operation itself");
        }

        auto l_targetCRS = operationsInOut.back()->targetCRS();
        if (l_targetCRS && !areCRSMoreOrLessEquivalent(
                               l_targetCRS.get(), concatOpTargetCRS.get())) {
            if (l_targetCRS->nameStr() == concatOpTargetCRS->nameStr() &&
                ((isGeographic(l_targetCRS.get()) &&
                  isGeocentric(concatOpTargetCRS.get())) ||
                 (isGeocentric(l_targetCRS.get()) &&
                  isGeographic(concatOpTargetCRS.get())))) {
                auto newOp(Conversion::createGeographicGeocentric(
                    NN_NO_CHECK(l_targetCRS), concatOpTargetCRS));
                operationsInOut.push_back(newOp);
            } else {
                throw InvalidOperation("The target CRS of the last step of "
                                       "concatenated operation is not the same "
                                       "as the target CRS of the concatenated "
                                       "operation itself");
            }
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a ConcatenatedOperation, or return a single
 * coordinate
 * operation.
 *
 * This computes its accuracy from the sum of its member operations, its
 * extent
 *
 * @param operationsIn Vector of the CoordinateOperation steps.
 * @param checkExtent Whether we should check the non-emptiness of the
 * intersection
 * of the extents of the operations
 * @throws InvalidOperation if the object cannot be constructed.
 */
CoordinateOperationNNPtr ConcatenatedOperation::createComputeMetadata(
    const std::vector<CoordinateOperationNNPtr> &operationsIn,
    bool checkExtent) // throw InvalidOperation
{
    util::PropertyMap properties;

    if (operationsIn.size() == 1) {
        return operationsIn[0];
    }

    std::vector<CoordinateOperationNNPtr> flattenOps;
    bool hasBallparkTransformation = false;
    for (const auto &subOp : operationsIn) {
        hasBallparkTransformation |= subOp->hasBallparkTransformation();
        auto subOpConcat =
            dynamic_cast<const ConcatenatedOperation *>(subOp.get());
        if (subOpConcat) {
            auto subOps = subOpConcat->operations();
            for (const auto &subSubOp : subOps) {
                flattenOps.emplace_back(subSubOp);
            }
        } else {
            flattenOps.emplace_back(subOp);
        }
    }

    // Remove consecutive inverse operations
    if (flattenOps.size() > 2) {
        std::vector<size_t> indices;
        for (size_t i = 0; i < flattenOps.size(); ++i)
            indices.push_back(i);
        while (true) {
            bool bHasChanged = false;
            for (size_t i = 0; i + 1 < indices.size(); ++i) {
                if (flattenOps[indices[i]]->_isEquivalentTo(
                        flattenOps[indices[i + 1]]->inverse().get(),
                        util::IComparable::Criterion::EQUIVALENT) &&
                    flattenOps[indices[i]]->sourceCRS()->_isEquivalentTo(
                        flattenOps[indices[i + 1]]->targetCRS().get(),
                        util::IComparable::Criterion::EQUIVALENT)) {
                    indices.erase(indices.begin() + i, indices.begin() + i + 2);
                    bHasChanged = true;
                    break;
                }
            }
            // We bail out if indices.size() == 2, because potentially
            // the last 2 remaining ones could auto-cancel, and we would have
            // to have a special case for that (and this happens in practice).
            if (!bHasChanged || indices.size() <= 2)
                break;
        }
        if (indices.size() < flattenOps.size()) {
            std::vector<CoordinateOperationNNPtr> flattenOpsNew;
            for (size_t i = 0; i < indices.size(); ++i) {
                flattenOpsNew.emplace_back(flattenOps[indices[i]]);
            }
            flattenOps = std::move(flattenOpsNew);
        }
    }

    if (flattenOps.size() == 1) {
        return flattenOps[0];
    }

    properties.set(common::IdentifiedObject::NAME_KEY,
                   computeConcatenatedName(flattenOps));

    bool emptyIntersection = false;
    auto extent = getExtent(flattenOps, false, emptyIntersection);
    if (checkExtent && emptyIntersection) {
        std::string msg(
            "empty intersection of area of validity of concatenated "
            "operations");
        throw InvalidOperationEmptyIntersection(msg);
    }
    if (extent) {
        properties.set(common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY,
                       NN_NO_CHECK(extent));
    }

    std::vector<metadata::PositionalAccuracyNNPtr> accuracies;
    const double accuracy = getAccuracy(flattenOps);
    if (accuracy >= 0.0) {
        accuracies.emplace_back(
            metadata::PositionalAccuracy::create(toString(accuracy)));
    }

    auto op = create(properties, flattenOps, accuracies);
    op->setHasBallparkTransformation(hasBallparkTransformation);
    op->d->computedName_ = true;
    return op;
}

// ---------------------------------------------------------------------------

CoordinateOperationNNPtr ConcatenatedOperation::inverse() const {
    std::vector<CoordinateOperationNNPtr> inversedOperations;
    auto l_operations = operations();
    inversedOperations.reserve(l_operations.size());
    for (const auto &operation : l_operations) {
        inversedOperations.emplace_back(operation->inverse());
    }
    std::reverse(inversedOperations.begin(), inversedOperations.end());

    auto properties = createPropertiesForInverse(this, false, false);
    if (d->computedName_) {
        properties.set(common::IdentifiedObject::NAME_KEY,
                       computeConcatenatedName(inversedOperations));
    }

    auto op =
        create(properties, inversedOperations, coordinateOperationAccuracies());
    op->d->computedName_ = d->computedName_;
    op->setHasBallparkTransformation(hasBallparkTransformation());
    op->setSourceCoordinateEpoch(targetCoordinateEpoch());
    op->setTargetCoordinateEpoch(sourceCoordinateEpoch());
    return op;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ConcatenatedOperation::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2 || !formatter->use2019Keywords()) {
        throw io::FormattingException(
            "Transformation can only be exported to WKT2:2019");
    }

    formatter->startNode(io::WKTConstants::CONCATENATEDOPERATION,
                         !identifiers().empty());
    formatter->addQuotedString(nameStr());

    if (formatter->use2019Keywords()) {
        const auto &version = operationVersion();
        if (version.has_value()) {
            formatter->startNode(io::WKTConstants::VERSION, false);
            formatter->addQuotedString(*version);
            formatter->endNode();
        }
    }

    exportSourceCRSAndTargetCRSToWKT(this, formatter);

    const bool canExportOperationId =
        !(formatter->idOnTopLevelOnly() && formatter->topLevelHasId());

    const bool hasDomains = !domains().empty();
    if (hasDomains) {
        formatter->pushDisableUsage();
    }

    for (const auto &operation : operations()) {
        formatter->startNode(io::WKTConstants::STEP, false);
        if (canExportOperationId && !operation->identifiers().empty()) {
            // fake that top node has no id, so that the operation id is
            // considered
            formatter->pushHasId(false);
            operation->_exportToWKT(formatter);
            formatter->popHasId();
        } else {
            operation->_exportToWKT(formatter);
        }
        formatter->endNode();
    }

    if (hasDomains) {
        formatter->popDisableUsage();
    }

    if (!coordinateOperationAccuracies().empty()) {
        formatter->startNode(io::WKTConstants::OPERATIONACCURACY, false);
        formatter->add(coordinateOperationAccuracies()[0]->value());
        formatter->endNode();
    }

    ObjectUsage::baseExportToWKT(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ConcatenatedOperation::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(formatter->MakeObjectContext("ConcatenatedOperation",
                                                    !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    writer->AddObjKey("source_crs");
    formatter->setAllowIDInImmediateChild();
    sourceCRS()->_exportToJSON(formatter);

    writer->AddObjKey("target_crs");
    formatter->setAllowIDInImmediateChild();
    targetCRS()->_exportToJSON(formatter);

    writer->AddObjKey("steps");
    {
        auto parametersContext(writer->MakeArrayContext(false));
        for (const auto &operation : operations()) {
            formatter->setAllowIDInImmediateChild();
            operation->_exportToJSON(formatter);
        }
    }

    if (!coordinateOperationAccuracies().empty()) {
        writer->AddObjKey("accuracy");
        writer->Add(coordinateOperationAccuracies()[0]->value());
    }

    ObjectUsage::baseExportToJSON(formatter);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CoordinateOperationNNPtr ConcatenatedOperation::_shallowClone() const {
    auto op =
        ConcatenatedOperation::nn_make_shared<ConcatenatedOperation>(*this);
    std::vector<CoordinateOperationNNPtr> ops;
    for (const auto &subOp : d->operations_) {
        ops.emplace_back(subOp->shallowClone());
    }
    op->d->operations_ = std::move(ops);
    op->assignSelf(op);
    op->setCRSs(this, false);
    return util::nn_static_pointer_cast<CoordinateOperation>(op);
}
//! @endcond

// ---------------------------------------------------------------------------

void ConcatenatedOperation::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(FormattingException)
{
    double sourceYear =
        sourceCoordinateEpoch().has_value()
            ? getRoundedEpochInDecimalYear(
                  sourceCoordinateEpoch()->coordinateEpoch().convertToUnit(
                      common::UnitOfMeasure::YEAR))
            : 0;
    double targetYear =
        targetCoordinateEpoch().has_value()
            ? getRoundedEpochInDecimalYear(
                  targetCoordinateEpoch()->coordinateEpoch().convertToUnit(
                      common::UnitOfMeasure::YEAR))
            : 0;
    if (sourceYear > 0 && targetYear == 0)
        targetYear = sourceYear;
    else if (targetYear > 0 && sourceYear == 0)
        sourceYear = targetYear;
    if (sourceYear > 0) {
        formatter->addStep("set");
        formatter->addParam("v_4", sourceYear);
    }
    for (const auto &operation : operations()) {
        operation->_exportToPROJString(formatter);
    }
    if (targetYear > 0) {
        formatter->addStep("set");
        formatter->addParam("v_4", targetYear);
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool ConcatenatedOperation::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherCO = dynamic_cast<const ConcatenatedOperation *>(other);
    if (otherCO == nullptr ||
        (criterion == util::IComparable::Criterion::STRICT &&
         !ObjectUsage::_isEquivalentTo(other, criterion, dbContext))) {
        return false;
    }
    const auto &steps = operations();
    const auto &otherSteps = otherCO->operations();
    if (steps.size() != otherSteps.size()) {
        return false;
    }
    for (size_t i = 0; i < steps.size(); i++) {
        if (!steps[i]->_isEquivalentTo(otherSteps[i].get(), criterion,
                                       dbContext)) {
            return false;
        }
    }
    return true;
}
//! @endcond

// ---------------------------------------------------------------------------

std::set<GridDescription> ConcatenatedOperation::gridsNeeded(
    const io::DatabaseContextPtr &databaseContext,
    bool considerKnownGridsAsAvailable) const {
    std::set<GridDescription> res;
    for (const auto &operation : operations()) {
        const auto l_gridsNeeded = operation->gridsNeeded(
            databaseContext, considerKnownGridsAsAvailable);
        for (const auto &gridDesc : l_gridsNeeded) {
            res.insert(gridDesc);
        }
    }
    return res;
}

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END
