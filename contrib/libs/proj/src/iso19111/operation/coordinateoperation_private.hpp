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

#ifndef COORDINATEROPERATION_PRIVATE_HPP
#define COORDINATEROPERATION_PRIVATE_HPP

#include "proj/coordinateoperation.hpp"
#include "proj/util.hpp"

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct CoordinateOperation::Private {
    util::optional<std::string> operationVersion_{};
    std::vector<metadata::PositionalAccuracyNNPtr>
        coordinateOperationAccuracies_{};
    std::weak_ptr<crs::CRS> sourceCRSWeak_{};
    std::weak_ptr<crs::CRS> targetCRSWeak_{};
    crs::CRSPtr interpolationCRS_{};
    std::shared_ptr<util::optional<common::DataEpoch>> sourceCoordinateEpoch_{
        std::make_shared<util::optional<common::DataEpoch>>()};
    std::shared_ptr<util::optional<common::DataEpoch>> targetCoordinateEpoch_{
        std::make_shared<util::optional<common::DataEpoch>>()};
    bool hasBallparkTransformation_ = false;
    bool requiresPerCoordinateInputTime_ = false;

    // do not set this for a ProjectedCRS.definingConversion
    struct CRSStrongRef {
        crs::CRSNNPtr sourceCRS_;
        crs::CRSNNPtr targetCRS_;
        CRSStrongRef(const crs::CRSNNPtr &sourceCRSIn,
                     const crs::CRSNNPtr &targetCRSIn)
            : sourceCRS_(sourceCRSIn), targetCRS_(targetCRSIn) {}
    };
    std::unique_ptr<CRSStrongRef> strongRef_{};

    Private() = default;
    Private(const Private &other)
        : operationVersion_(other.operationVersion_),
          coordinateOperationAccuracies_(other.coordinateOperationAccuracies_),
          sourceCRSWeak_(other.sourceCRSWeak_),
          targetCRSWeak_(other.targetCRSWeak_),
          interpolationCRS_(other.interpolationCRS_),
          sourceCoordinateEpoch_(other.sourceCoordinateEpoch_),
          targetCoordinateEpoch_(other.targetCoordinateEpoch_),
          hasBallparkTransformation_(other.hasBallparkTransformation_),
          requiresPerCoordinateInputTime_(
              other.requiresPerCoordinateInputTime_),
          strongRef_(other.strongRef_
                         ? std::make_unique<CRSStrongRef>(*(other.strongRef_))
                         : nullptr) {}

    Private &operator=(const Private &) = delete;
};
//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END

#endif // COORDINATEROPERATION_PRIVATE_HPP
