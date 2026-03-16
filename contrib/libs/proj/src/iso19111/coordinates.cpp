/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2023, Even Rouault <even dot rouault at spatialys dot com>
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

#include "proj/coordinates.hpp"
#include "proj/common.hpp"
#include "proj/crs.hpp"
#include "proj/io.hpp"

#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

#include "proj_json_streaming_writer.hpp"

#include <cmath>
#include <limits>

using namespace NS_PROJ::internal;

NS_PROJ_START

namespace coordinates {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct CoordinateMetadata::Private {
    crs::CRSNNPtr crs_;
    util::optional<common::DataEpoch> coordinateEpoch_{};

    explicit Private(const crs::CRSNNPtr &crs) : crs_(crs) {}
    Private(const crs::CRSNNPtr &crs, const common::DataEpoch &coordinateEpoch)
        : crs_(crs), coordinateEpoch_(coordinateEpoch) {}
};
//! @endcond

// ---------------------------------------------------------------------------

CoordinateMetadata::CoordinateMetadata(const crs::CRSNNPtr &crsIn)
    : d(std::make_unique<Private>(crsIn)) {}

// ---------------------------------------------------------------------------

CoordinateMetadata::CoordinateMetadata(const crs::CRSNNPtr &crsIn,
                                       double coordinateEpochAsDecimalYearIn)
    : d(std::make_unique<Private>(crsIn, common::DataEpoch(common::Measure(
                                             coordinateEpochAsDecimalYearIn,
                                             common::UnitOfMeasure::YEAR)))) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CoordinateMetadata::~CoordinateMetadata() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a CoordinateMetadata from a static CRS.
 * @param crsIn a static CRS
 * @return new CoordinateMetadata.
 * @throw util::Exception if crsIn is a dynamic CRS.
 */
CoordinateMetadataNNPtr CoordinateMetadata::create(const crs::CRSNNPtr &crsIn) {

    if (crsIn->isDynamic(/*considerWGS84AsDynamic=*/false)) {
        throw util::Exception(
            "Coordinate epoch should be provided for a dynamic CRS");
    }

    auto coordinateMetadata(
        CoordinateMetadata::nn_make_shared<CoordinateMetadata>(crsIn));
    coordinateMetadata->assignSelf(coordinateMetadata);
    return coordinateMetadata;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CoordinateMetadata from a dynamic CRS and an associated
 * coordinate epoch.
 *
 * @param crsIn a dynamic CRS
 * @param coordinateEpochIn coordinate epoch expressed in decimal year.
 * @return new CoordinateMetadata.
 * @throw util::Exception if crsIn is a static CRS.
 */
CoordinateMetadataNNPtr CoordinateMetadata::create(const crs::CRSNNPtr &crsIn,
                                                   double coordinateEpochIn) {

    return create(crsIn, coordinateEpochIn, nullptr);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CoordinateMetadata from a dynamic CRS and an associated
 * coordinate epoch.
 *
 * @param crsIn a dynamic CRS
 * @param coordinateEpochIn coordinate epoch expressed in decimal year.
 * @param dbContext Database context (may be null)
 * @return new CoordinateMetadata.
 * @throw util::Exception if crsIn is a static CRS.
 */
CoordinateMetadataNNPtr
CoordinateMetadata::create(const crs::CRSNNPtr &crsIn, double coordinateEpochIn,
                           const io::DatabaseContextPtr &dbContext) {

    if (!crsIn->isDynamic(/*considerWGS84AsDynamic=*/true)) {
        bool ok = false;
        if (dbContext) {
            auto geodCrs = crsIn->extractGeodeticCRS();
            if (geodCrs) {
                auto factory = io::AuthorityFactory::create(
                    NN_NO_CHECK(dbContext), std::string());
                ok = !factory
                          ->getPointMotionOperationsFor(NN_NO_CHECK(geodCrs),
                                                        false)
                          .empty();
            }
        }
        if (!ok) {
            throw util::Exception(
                "Coordinate epoch should not be provided for a static CRS");
        }
    }

    auto coordinateMetadata(
        CoordinateMetadata::nn_make_shared<CoordinateMetadata>(
            crsIn, coordinateEpochIn));
    coordinateMetadata->assignSelf(coordinateMetadata);
    return coordinateMetadata;
}

// ---------------------------------------------------------------------------

/** \brief Get the CRS associated with this CoordinateMetadata object.
 */
const crs::CRSNNPtr &CoordinateMetadata::crs() PROJ_PURE_DEFN {
    return d->crs_;
}

// ---------------------------------------------------------------------------

/** \brief Get the coordinate epoch associated with this CoordinateMetadata
 * object.
 *
 * The coordinate epoch is mandatory for a dynamic CRS,
 * and forbidden for a static CRS.
 */
const util::optional<common::DataEpoch> &
CoordinateMetadata::coordinateEpoch() PROJ_PURE_DEFN {
    return d->coordinateEpoch_;
}

// ---------------------------------------------------------------------------

/** \brief Get the coordinate epoch associated with this CoordinateMetadata
 * object, as decimal year.
 *
 * The coordinate epoch is mandatory for a dynamic CRS,
 * and forbidden for a static CRS.
 */
double CoordinateMetadata::coordinateEpochAsDecimalYear() PROJ_PURE_DEFN {
    if (d->coordinateEpoch_.has_value()) {
        return getRoundedEpochInDecimalYear(
            d->coordinateEpoch_->coordinateEpoch().convertToUnit(
                common::UnitOfMeasure::YEAR));
    }
    return std::numeric_limits<double>::quiet_NaN();
}

// ---------------------------------------------------------------------------

/** \brief Return a variant of this CoordinateMetadata "promoted" to a 3D one,
 * if not already the case.
 *
 * @param newName Name of the new underlying CRS. If empty, nameStr() will be
 * used.
 * @param dbContext Database context to look for potentially already registered
 *                  3D CRS. May be nullptr.
 * @return a new CoordinateMetadata object promoted to 3D, or the current one if
 * already 3D or not applicable.
 */
CoordinateMetadataNNPtr
CoordinateMetadata::promoteTo3D(const std::string &newName,
                                const io::DatabaseContextPtr &dbContext) const {
    auto crs = d->crs_->promoteTo3D(newName, dbContext);
    if (d->coordinateEpoch_.has_value()) {
        auto coordinateMetadata(
            CoordinateMetadata::nn_make_shared<CoordinateMetadata>(
                crs, coordinateEpochAsDecimalYear()));
        coordinateMetadata->assignSelf(coordinateMetadata);
        return coordinateMetadata;
    } else {
        auto coordinateMetadata(
            CoordinateMetadata::nn_make_shared<CoordinateMetadata>(crs));
        coordinateMetadata->assignSelf(coordinateMetadata);
        return coordinateMetadata;
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void CoordinateMetadata::_exportToWKT(io::WKTFormatter *formatter) const {
    if (formatter->version() != io::WKTFormatter::Version::WKT2 ||
        !formatter->use2019Keywords()) {
        io::FormattingException::Throw(
            "CoordinateMetadata can only be exported since WKT2:2019");
    }
    formatter->startNode(io::WKTConstants::COORDINATEMETADATA, false);

    crs()->_exportToWKT(formatter);

    if (d->coordinateEpoch_.has_value()) {
        formatter->startNode(io::WKTConstants::EPOCH, false);
        formatter->add(coordinateEpochAsDecimalYear());
        formatter->endNode();
    }

    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void CoordinateMetadata::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(io::FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("CoordinateMetadata", false));

    writer->AddObjKey("crs");
    crs()->_exportToJSON(formatter);

    if (d->coordinateEpoch_.has_value()) {
        writer->AddObjKey("coordinateEpoch");
        writer->Add(coordinateEpochAsDecimalYear());
    }
}
//! @endcond

} // namespace coordinates

NS_PROJ_END
