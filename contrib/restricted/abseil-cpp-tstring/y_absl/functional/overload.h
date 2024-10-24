// Copyright 2023 The Abseil Authors.
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
// File: overload.h
// -----------------------------------------------------------------------------
//
// `y_absl::Overload` is a functor that provides overloads based on the functors
// with which it is created. This can, for example, be used to locally define an
// anonymous visitor type for `std::visit` inside a function using lambdas.
//
// Before using this function, consider whether named function overloads would
// be a better design.
//
// Note: y_absl::Overload requires C++17.
//
// Example:
//
//     std::variant<TString, int32_t, int64_t> v(int32_t{1});
//     const size_t result =
//         std::visit(y_absl::Overload{
//                        [](const TString& s) { return s.size(); },
//                        [](const auto& s) { return sizeof(s); },
//                    },
//                    v);
//     assert(result == 4);
//

#ifndef Y_ABSL_FUNCTIONAL_OVERLOAD_H_
#define Y_ABSL_FUNCTIONAL_OVERLOAD_H_

#include "y_absl/base/config.h"
#include "y_absl/meta/type_traits.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN

#if defined(Y_ABSL_INTERNAL_CPLUSPLUS_LANG) && \
    Y_ABSL_INTERNAL_CPLUSPLUS_LANG >= 201703L

template <typename... T>
struct Overload final : T... {
  using T::operator()...;

  // For historical reasons we want to support use that looks like a function
  // call:
  //
  //     y_absl::Overload(lambda_1, lambda_2)
  //
  // This works automatically in C++20 because we have support for parenthesized
  // aggregate initialization. Before then we must provide a constructor that
  // makes this work.
  //
  constexpr explicit Overload(T... ts) : T(std::move(ts))... {}
};

// Before C++20, which added support for CTAD for aggregate types, we must also
// teach the compiler how to deduce the template arguments for Overload.
//
template <typename... T>
Overload(T...) -> Overload<T...>;

#else

namespace functional_internal {
template <typename T>
constexpr bool kDependentFalse = false;
}

template <typename Dependent = int, typename... T>
auto Overload(T&&...) {
  static_assert(functional_internal::kDependentFalse<Dependent>,
                "Overload is only usable with C++17 or above.");
}

#endif

Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_FUNCTIONAL_OVERLOAD_H_
