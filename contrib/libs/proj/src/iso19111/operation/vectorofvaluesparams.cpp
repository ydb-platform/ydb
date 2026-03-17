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

#include "vectorofvaluesparams.hpp"

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

static std::vector<ParameterValueNNPtr> buildParameterValueFromMeasure(
    const std::initializer_list<common::Measure> &list) {
    std::vector<ParameterValueNNPtr> res;
    for (const auto &v : list) {
        res.emplace_back(ParameterValue::create(v));
    }
    return res;
}

VectorOfValues::VectorOfValues(std::initializer_list<common::Measure> list)
    : std::vector<ParameterValueNNPtr>(buildParameterValueFromMeasure(list)) {}

// This way, we disable inlining of destruction, and save a lot of space
VectorOfValues::~VectorOfValues() = default;

VectorOfValues createParams(const common::Measure &m1,
                            const common::Measure &m2,
                            const common::Measure &m3) {
    return VectorOfValues{ParameterValue::create(m1),
                          ParameterValue::create(m2),
                          ParameterValue::create(m3)};
}

VectorOfValues createParams(const common::Measure &m1,
                            const common::Measure &m2,
                            const common::Measure &m3,
                            const common::Measure &m4) {
    return VectorOfValues{
        ParameterValue::create(m1), ParameterValue::create(m2),
        ParameterValue::create(m3), ParameterValue::create(m4)};
}

VectorOfValues createParams(const common::Measure &m1,
                            const common::Measure &m2,
                            const common::Measure &m3,
                            const common::Measure &m4,
                            const common::Measure &m5) {
    return VectorOfValues{
        ParameterValue::create(m1), ParameterValue::create(m2),
        ParameterValue::create(m3), ParameterValue::create(m4),
        ParameterValue::create(m5),
    };
}

VectorOfValues
createParams(const common::Measure &m1, const common::Measure &m2,
             const common::Measure &m3, const common::Measure &m4,
             const common::Measure &m5, const common::Measure &m6) {
    return VectorOfValues{
        ParameterValue::create(m1), ParameterValue::create(m2),
        ParameterValue::create(m3), ParameterValue::create(m4),
        ParameterValue::create(m5), ParameterValue::create(m6),
    };
}

VectorOfValues
createParams(const common::Measure &m1, const common::Measure &m2,
             const common::Measure &m3, const common::Measure &m4,
             const common::Measure &m5, const common::Measure &m6,
             const common::Measure &m7) {
    return VectorOfValues{
        ParameterValue::create(m1), ParameterValue::create(m2),
        ParameterValue::create(m3), ParameterValue::create(m4),
        ParameterValue::create(m5), ParameterValue::create(m6),
        ParameterValue::create(m7),
    };
}

// This way, we disable inlining of destruction, and save a lot of space
VectorOfParameters::~VectorOfParameters() = default;

//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END
