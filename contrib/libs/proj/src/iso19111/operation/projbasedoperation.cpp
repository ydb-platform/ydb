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

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

PROJBasedOperation::~PROJBasedOperation() = default;

// ---------------------------------------------------------------------------

PROJBasedOperation::PROJBasedOperation(const OperationMethodNNPtr &methodIn)
    : SingleOperation(methodIn) {}

// ---------------------------------------------------------------------------

PROJBasedOperationNNPtr PROJBasedOperation::create(
    const util::PropertyMap &properties, const std::string &PROJString,
    const crs::CRSPtr &sourceCRS, const crs::CRSPtr &targetCRS,
    const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies) {
    auto method = OperationMethod::create(
        util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                "PROJ-based operation method: " + PROJString),
        std::vector<GeneralOperationParameterNNPtr>{});
    auto op = PROJBasedOperation::nn_make_shared<PROJBasedOperation>(method);
    op->assignSelf(op);
    op->projString_ = PROJString;
    if (sourceCRS && targetCRS) {
        op->setCRSs(NN_NO_CHECK(sourceCRS), NN_NO_CHECK(targetCRS), nullptr);
    }
    op->setProperties(
        addDefaultNameIfNeeded(properties, "PROJ-based coordinate operation"));
    op->setAccuracies(accuracies);

    auto formatter = io::PROJStringFormatter::create();
    try {
        formatter->ingestPROJString(PROJString);
        op->setRequiresPerCoordinateInputTime(
            formatter->requiresPerCoordinateInputTime());
    } catch (const io::ParsingException &e) {
        throw util::UnsupportedOperationException(
            std::string("PROJBasedOperation::create() failed: ") + e.what());
    }

    return op;
}

// ---------------------------------------------------------------------------

PROJBasedOperationNNPtr PROJBasedOperation::create(
    const util::PropertyMap &properties,
    const io::IPROJStringExportableNNPtr &projExportable, bool inverse,
    const crs::CRSNNPtr &sourceCRS, const crs::CRSNNPtr &targetCRS,
    const crs::CRSPtr &interpolationCRS,
    const std::vector<metadata::PositionalAccuracyNNPtr> &accuracies,
    bool hasBallparkTransformation) {

    auto formatter = io::PROJStringFormatter::create();
    if (inverse) {
        formatter->startInversion();
    }
    projExportable->_exportToPROJString(formatter.get());
    if (inverse) {
        formatter->stopInversion();
    }
    const auto &projString = formatter->toString();

    auto method = OperationMethod::create(
        util::PropertyMap().set(common::IdentifiedObject::NAME_KEY,
                                "PROJ-based operation method (approximate): " +
                                    projString),
        std::vector<GeneralOperationParameterNNPtr>{});
    auto op = PROJBasedOperation::nn_make_shared<PROJBasedOperation>(method);
    op->assignSelf(op);
    op->projString_ = projString;
    op->setCRSs(sourceCRS, targetCRS, interpolationCRS);
    op->setProperties(
        addDefaultNameIfNeeded(properties, "PROJ-based coordinate operation"));
    op->setAccuracies(accuracies);
    op->projStringExportable_ = projExportable.as_nullable();
    op->inverse_ = inverse;
    op->setHasBallparkTransformation(hasBallparkTransformation);
    op->setRequiresPerCoordinateInputTime(
        formatter->requiresPerCoordinateInputTime());

    return op;
}

// ---------------------------------------------------------------------------

CoordinateOperationNNPtr PROJBasedOperation::inverse() const {

    if (projStringExportable_ && sourceCRS() && targetCRS()) {
        return util::nn_static_pointer_cast<CoordinateOperation>(
            PROJBasedOperation::create(
                createPropertiesForInverse(this, false, false),
                NN_NO_CHECK(projStringExportable_), !inverse_,
                NN_NO_CHECK(targetCRS()), NN_NO_CHECK(sourceCRS()),
                interpolationCRS(), coordinateOperationAccuracies(),
                hasBallparkTransformation()));
    }

    auto formatter = io::PROJStringFormatter::create();
    formatter->startInversion();
    try {
        formatter->ingestPROJString(projString_);
    } catch (const io::ParsingException &e) {
        throw util::UnsupportedOperationException(
            std::string("PROJBasedOperation::inverse() failed: ") + e.what());
    }
    formatter->stopInversion();

    auto op = PROJBasedOperation::create(
        createPropertiesForInverse(this, false, false), formatter->toString(),
        targetCRS(), sourceCRS(), coordinateOperationAccuracies());
    if (sourceCRS() && targetCRS()) {
        op->setCRSs(NN_NO_CHECK(targetCRS()), NN_NO_CHECK(sourceCRS()),
                    interpolationCRS());
    }
    op->setHasBallparkTransformation(hasBallparkTransformation());
    op->setRequiresPerCoordinateInputTime(
        formatter->requiresPerCoordinateInputTime());
    return util::nn_static_pointer_cast<CoordinateOperation>(op);
}

// ---------------------------------------------------------------------------

void PROJBasedOperation::_exportToWKT(io::WKTFormatter *formatter) const {

    if (sourceCRS() && targetCRS()) {
        exportTransformationToWKT(formatter);
        return;
    }

    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {
        throw io::FormattingException(
            "PROJBasedOperation can only be exported to WKT2");
    }

    formatter->startNode(io::WKTConstants::CONVERSION, false);
    formatter->addQuotedString(nameStr());
    method()->_exportToWKT(formatter);

    for (const auto &paramValue : parameterValues()) {
        paramValue->_exportToWKT(formatter);
    }
    formatter->endNode();
}

// ---------------------------------------------------------------------------

void PROJBasedOperation::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(formatter->MakeObjectContext(
        (sourceCRS() && targetCRS()) ? "Transformation" : "Conversion",
        !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    if (sourceCRS() && targetCRS()) {
        writer->AddObjKey("source_crs");
        formatter->setAllowIDInImmediateChild();
        sourceCRS()->_exportToJSON(formatter);

        writer->AddObjKey("target_crs");
        formatter->setAllowIDInImmediateChild();
        targetCRS()->_exportToJSON(formatter);
    }

    writer->AddObjKey("method");
    formatter->setOmitTypeInImmediateChild();
    formatter->setAllowIDInImmediateChild();
    method()->_exportToJSON(formatter);

    const auto &l_parameterValues = parameterValues();
    writer->AddObjKey("parameters");
    {
        auto parametersContext(writer->MakeArrayContext(false));
        for (const auto &genOpParamvalue : l_parameterValues) {
            formatter->setAllowIDInImmediateChild();
            formatter->setOmitTypeInImmediateChild();
            genOpParamvalue->_exportToJSON(formatter);
        }
    }
}

// ---------------------------------------------------------------------------

void PROJBasedOperation::_exportToPROJString(
    io::PROJStringFormatter *formatter) const {
    if (projStringExportable_) {
        if (inverse_) {
            formatter->startInversion();
        }
        projStringExportable_->_exportToPROJString(formatter);
        if (inverse_) {
            formatter->stopInversion();
        }
        return;
    }

    try {
        formatter->ingestPROJString(projString_);
    } catch (const io::ParsingException &e) {
        throw io::FormattingException(
            std::string("PROJBasedOperation::exportToPROJString() failed: ") +
            e.what());
    }
}

// ---------------------------------------------------------------------------

CoordinateOperationNNPtr PROJBasedOperation::_shallowClone() const {
    auto op = PROJBasedOperation::nn_make_shared<PROJBasedOperation>(*this);
    op->assignSelf(op);
    op->setCRSs(this, false);
    return util::nn_static_pointer_cast<CoordinateOperation>(op);
}

// ---------------------------------------------------------------------------

std::set<GridDescription>
PROJBasedOperation::gridsNeeded(const io::DatabaseContextPtr &databaseContext,
                                bool considerKnownGridsAsAvailable) const {
    std::set<GridDescription> res;

    try {
        auto formatterOut = io::PROJStringFormatter::create();
        auto formatter = io::PROJStringFormatter::create();
        formatter->ingestPROJString(exportToPROJString(formatterOut.get()));
        const auto usedGridNames = formatter->getUsedGridNames();
        for (const auto &shortName : usedGridNames) {
            GridDescription desc;
            desc.shortName = shortName;
            if (databaseContext) {
                databaseContext->lookForGridInfo(
                    desc.shortName, considerKnownGridsAsAvailable,
                    desc.fullName, desc.packageName, desc.url,
                    desc.directDownload, desc.openLicense, desc.available);
            }
            res.insert(std::move(desc));
        }
    } catch (const io::ParsingException &) {
    }

    return res;
}

//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation

NS_PROJ_END
