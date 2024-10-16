// Copyright 2018 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: mock_distributions.h
// -----------------------------------------------------------------------------
//
// This file contains mock distribution functions for use alongside an
// `y_absl::MockingBitGen` object within the Googletest testing framework. Such
// mocks are useful to provide deterministic values as return values within
// (otherwise random) Abseil distribution functions.
//
// The return type of each function is a mock expectation object which
// is used to set the match result.
//
// More information about the Googletest testing framework is available at
// https://github.com/google/googletest
//
// EXPECT_CALL and ON_CALL need to be made within the same DLL component as
// the call to y_absl::Uniform and related methods, otherwise mocking will fail
// since the  underlying implementation creates a type-specific pointer which
// will be distinct across different DLL boundaries.
//
// Example:
//
//   y_absl::MockingBitGen mock;
//   EXPECT_CALL(y_absl::MockUniform<int>(), Call(mock, 1, 1000))
//     .WillRepeatedly(testing::ReturnRoundRobin({20, 40}));
//
//   EXPECT_EQ(y_absl::Uniform<int>(gen, 1, 1000), 20);
//   EXPECT_EQ(y_absl::Uniform<int>(gen, 1, 1000), 40);
//   EXPECT_EQ(y_absl::Uniform<int>(gen, 1, 1000), 20);
//   EXPECT_EQ(y_absl::Uniform<int>(gen, 1, 1000), 40);

#ifndef Y_ABSL_RANDOM_MOCK_DISTRIBUTIONS_H_
#define Y_ABSL_RANDOM_MOCK_DISTRIBUTIONS_H_

#include "y_absl/base/config.h"
#include "y_absl/random/bernoulli_distribution.h"
#include "y_absl/random/beta_distribution.h"
#include "y_absl/random/distributions.h"
#include "y_absl/random/exponential_distribution.h"
#include "y_absl/random/gaussian_distribution.h"
#include "y_absl/random/internal/mock_overload_set.h"
#include "y_absl/random/internal/mock_validators.h"
#include "y_absl/random/log_uniform_int_distribution.h"
#include "y_absl/random/mocking_bit_gen.h"
#include "y_absl/random/poisson_distribution.h"
#include "y_absl/random/zipf_distribution.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN

// -----------------------------------------------------------------------------
// y_absl::MockUniform
// -----------------------------------------------------------------------------
//
// Matches calls to y_absl::Uniform.
//
// `y_absl::MockUniform` is a class template used in conjunction with Googletest's
// `ON_CALL()` and `EXPECT_CALL()` macros. To use it, default-construct an
// instance of it inside `ON_CALL()` or `EXPECT_CALL()`, and use `Call(...)` the
// same way one would define mocks on a Googletest `MockFunction()`.
//
// Example:
//
//  y_absl::MockingBitGen mock;
//  EXPECT_CALL(y_absl::MockUniform<uint32_t>(), Call(mock))
//     .WillOnce(Return(123456));
//  auto x = y_absl::Uniform<uint32_t>(mock);
//  assert(x == 123456)
//
template <typename R>
using MockUniform = random_internal::MockOverloadSetWithValidator<
    random_internal::UniformDistributionWrapper<R>,
    random_internal::UniformDistributionValidator<R>,
    R(IntervalClosedOpenTag, MockingBitGen&, R, R),
    R(IntervalClosedClosedTag, MockingBitGen&, R, R),
    R(IntervalOpenOpenTag, MockingBitGen&, R, R),
    R(IntervalOpenClosedTag, MockingBitGen&, R, R), R(MockingBitGen&, R, R),
    R(MockingBitGen&)>;

// -----------------------------------------------------------------------------
// y_absl::MockBernoulli
// -----------------------------------------------------------------------------
//
// Matches calls to y_absl::Bernoulli.
//
// `y_absl::MockBernoulli` is a class used in conjunction with Googletest's
// `ON_CALL()` and `EXPECT_CALL()` macros. To use it, default-construct an
// instance of it inside `ON_CALL()` or `EXPECT_CALL()`, and use `Call(...)` the
// same way one would define mocks on a Googletest `MockFunction()`.
//
// Example:
//
//  y_absl::MockingBitGen mock;
//  EXPECT_CALL(y_absl::MockBernoulli(), Call(mock, testing::_))
//     .WillOnce(Return(false));
//  assert(y_absl::Bernoulli(mock, 0.5) == false);
//
using MockBernoulli =
    random_internal::MockOverloadSet<y_absl::bernoulli_distribution,
                                     bool(MockingBitGen&, double)>;

// -----------------------------------------------------------------------------
// y_absl::MockBeta
// -----------------------------------------------------------------------------
//
// Matches calls to y_absl::Beta.
//
// `y_absl::MockBeta` is a class used in conjunction with Googletest's `ON_CALL()`
// and `EXPECT_CALL()` macros. To use it, default-construct an instance of it
// inside `ON_CALL()` or `EXPECT_CALL()`, and use `Call(...)` the same way one
// would define mocks on a Googletest `MockFunction()`.
//
// Example:
//
//  y_absl::MockingBitGen mock;
//  EXPECT_CALL(y_absl::MockBeta(), Call(mock, 3.0, 2.0))
//     .WillOnce(Return(0.567));
//  auto x = y_absl::Beta<double>(mock, 3.0, 2.0);
//  assert(x == 0.567);
//
template <typename RealType>
using MockBeta =
    random_internal::MockOverloadSet<y_absl::beta_distribution<RealType>,
                                     RealType(MockingBitGen&, RealType,
                                              RealType)>;

// -----------------------------------------------------------------------------
// y_absl::MockExponential
// -----------------------------------------------------------------------------
//
// Matches calls to y_absl::Exponential.
//
// `y_absl::MockExponential` is a class template used in conjunction with
// Googletest's `ON_CALL()` and `EXPECT_CALL()` macros. To use it,
// default-construct an instance of it inside `ON_CALL()` or `EXPECT_CALL()`,
// and use `Call(...)` the same way one would define mocks on a
// Googletest `MockFunction()`.
//
// Example:
//
//  y_absl::MockingBitGen mock;
//  EXPECT_CALL(y_absl::MockExponential<double>(), Call(mock, 0.5))
//     .WillOnce(Return(12.3456789));
//  auto x = y_absl::Exponential<double>(mock, 0.5);
//  assert(x == 12.3456789)
//
template <typename RealType>
using MockExponential =
    random_internal::MockOverloadSet<y_absl::exponential_distribution<RealType>,
                                     RealType(MockingBitGen&, RealType)>;

// -----------------------------------------------------------------------------
// y_absl::MockGaussian
// -----------------------------------------------------------------------------
//
// Matches calls to y_absl::Gaussian.
//
// `y_absl::MockGaussian` is a class template used in conjunction with
// Googletest's `ON_CALL()` and `EXPECT_CALL()` macros. To use it,
// default-construct an instance of it inside `ON_CALL()` or `EXPECT_CALL()`,
// and use `Call(...)` the same way one would define mocks on a
// Googletest `MockFunction()`.
//
// Example:
//
//  y_absl::MockingBitGen mock;
//  EXPECT_CALL(y_absl::MockGaussian<double>(), Call(mock, 16.3, 3.3))
//     .WillOnce(Return(12.3456789));
//  auto x = y_absl::Gaussian<double>(mock, 16.3, 3.3);
//  assert(x == 12.3456789)
//
template <typename RealType>
using MockGaussian =
    random_internal::MockOverloadSet<y_absl::gaussian_distribution<RealType>,
                                     RealType(MockingBitGen&, RealType,
                                              RealType)>;

// -----------------------------------------------------------------------------
// y_absl::MockLogUniform
// -----------------------------------------------------------------------------
//
// Matches calls to y_absl::LogUniform.
//
// `y_absl::MockLogUniform` is a class template used in conjunction with
// Googletest's `ON_CALL()` and `EXPECT_CALL()` macros. To use it,
// default-construct an instance of it inside `ON_CALL()` or `EXPECT_CALL()`,
// and use `Call(...)` the same way one would define mocks on a
// Googletest `MockFunction()`.
//
// Example:
//
//  y_absl::MockingBitGen mock;
//  EXPECT_CALL(y_absl::MockLogUniform<int>(), Call(mock, 10, 10000, 10))
//     .WillOnce(Return(1221));
//  auto x = y_absl::LogUniform<int>(mock, 10, 10000, 10);
//  assert(x == 1221)
//
template <typename IntType>
using MockLogUniform = random_internal::MockOverloadSet<
    y_absl::log_uniform_int_distribution<IntType>,
    IntType(MockingBitGen&, IntType, IntType, IntType)>;

// -----------------------------------------------------------------------------
// y_absl::MockPoisson
// -----------------------------------------------------------------------------
//
// Matches calls to y_absl::Poisson.
//
// `y_absl::MockPoisson` is a class template used in conjunction with Googletest's
// `ON_CALL()` and `EXPECT_CALL()` macros. To use it, default-construct an
// instance of it inside `ON_CALL()` or `EXPECT_CALL()`, and use `Call(...)` the
// same way one would define mocks on a Googletest `MockFunction()`.
//
// Example:
//
//  y_absl::MockingBitGen mock;
//  EXPECT_CALL(y_absl::MockPoisson<int>(), Call(mock, 2.0))
//     .WillOnce(Return(1221));
//  auto x = y_absl::Poisson<int>(mock, 2.0);
//  assert(x == 1221)
//
template <typename IntType>
using MockPoisson =
    random_internal::MockOverloadSet<y_absl::poisson_distribution<IntType>,
                                     IntType(MockingBitGen&, double)>;

// -----------------------------------------------------------------------------
// y_absl::MockZipf
// -----------------------------------------------------------------------------
//
// Matches calls to y_absl::Zipf.
//
// `y_absl::MockZipf` is a class template used in conjunction with Googletest's
// `ON_CALL()` and `EXPECT_CALL()` macros. To use it, default-construct an
// instance of it inside `ON_CALL()` or `EXPECT_CALL()`, and use `Call(...)` the
// same way one would define mocks on a Googletest `MockFunction()`.
//
// Example:
//
//  y_absl::MockingBitGen mock;
//  EXPECT_CALL(y_absl::MockZipf<int>(), Call(mock, 1000000, 2.0, 1.0))
//     .WillOnce(Return(1221));
//  auto x = y_absl::Zipf<int>(mock, 1000000, 2.0, 1.0);
//  assert(x == 1221)
//
template <typename IntType>
using MockZipf =
    random_internal::MockOverloadSet<y_absl::zipf_distribution<IntType>,
                                     IntType(MockingBitGen&, IntType, double,
                                             double)>;

Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_RANDOM_MOCK_DISTRIBUTIONS_H_
