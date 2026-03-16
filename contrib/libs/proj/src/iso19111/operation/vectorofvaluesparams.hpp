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

#ifndef VECTOROFVALUESPARAMS_HPP
#define VECTOROFVALUESPARAMS_HPP

#include "proj/coordinateoperation.hpp"
#include "proj/util.hpp"

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

struct VectorOfValues : public std::vector<ParameterValueNNPtr> {
    VectorOfValues() : std::vector<ParameterValueNNPtr>() {}
    explicit VectorOfValues(std::initializer_list<ParameterValueNNPtr> list)
        : std::vector<ParameterValueNNPtr>(list) {}

    explicit VectorOfValues(std::initializer_list<common::Measure> list);
    VectorOfValues(const VectorOfValues &) = delete;
    VectorOfValues(VectorOfValues &&) = default;

    ~VectorOfValues();
};

VectorOfValues createParams(const common::Measure &m1,
                            const common::Measure &m2,
                            const common::Measure &m3);

VectorOfValues createParams(const common::Measure &m1,
                            const common::Measure &m2,
                            const common::Measure &m3,
                            const common::Measure &m4);

VectorOfValues createParams(const common::Measure &m1,
                            const common::Measure &m2,
                            const common::Measure &m3,
                            const common::Measure &m4,
                            const common::Measure &m5);

VectorOfValues
createParams(const common::Measure &m1, const common::Measure &m2,
             const common::Measure &m3, const common::Measure &m4,
             const common::Measure &m5, const common::Measure &m6);

VectorOfValues
createParams(const common::Measure &m1, const common::Measure &m2,
             const common::Measure &m3, const common::Measure &m4,
             const common::Measure &m5, const common::Measure &m6,
             const common::Measure &m7);

// ---------------------------------------------------------------------------

struct VectorOfParameters : public std::vector<OperationParameterNNPtr> {
    VectorOfParameters() : std::vector<OperationParameterNNPtr>() {}
    explicit VectorOfParameters(
        std::initializer_list<OperationParameterNNPtr> list)
        : std::vector<OperationParameterNNPtr>(list) {}
    VectorOfParameters(const VectorOfParameters &) = delete;

    ~VectorOfParameters();
};

//! @endcond

// ---------------------------------------------------------------------------

} // namespace operation
NS_PROJ_END

#endif // VECTOROFVALUESPARAMS_HPP
