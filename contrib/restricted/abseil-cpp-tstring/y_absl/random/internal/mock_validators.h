// Copyright 2024 The Abseil Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef Y_ABSL_RANDOM_INTERNAL_MOCK_VALIDATORS_H_
#define Y_ABSL_RANDOM_INTERNAL_MOCK_VALIDATORS_H_

#include <type_traits>

#include "y_absl/base/config.h"
#include "y_absl/base/internal/raw_logging.h"
#include "y_absl/random/internal/iostream_state_saver.h"
#include "y_absl/random/internal/uniform_helper.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/string_view.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace random_internal {

template <typename NumType>
class UniformDistributionValidator {
 public:
  // Handle y_absl::Uniform<NumType>(gen, y_absl::IntervalTag, lo, hi).
  template <typename TagType>
  static void Validate(NumType x, TagType tag, NumType lo, NumType hi) {
    // For invalid ranges, y_absl::Uniform() simply returns one of the bounds.
    if (x == lo && lo == hi) return;

    ValidateImpl(std::is_floating_point<NumType>{}, x, tag, lo, hi);
  }

  // Handle y_absl::Uniform<NumType>(gen, lo, hi).
  static void Validate(NumType x, NumType lo, NumType hi) {
    Validate(x, IntervalClosedOpenTag(), lo, hi);
  }

  // Handle y_absl::Uniform<NumType>(gen).
  static void Validate(NumType) {
    // y_absl::Uniform<NumType>(gen) spans the entire range of `NumType`, so any
    // value is okay. This overload exists because the validation logic attempts
    // to call it anyway rather than adding extra SFINAE.
  }

 private:
  static y_absl::string_view TagLbBound(IntervalClosedOpenTag) { return "["; }
  static y_absl::string_view TagLbBound(IntervalOpenOpenTag) { return "("; }
  static y_absl::string_view TagLbBound(IntervalClosedClosedTag) { return "["; }
  static y_absl::string_view TagLbBound(IntervalOpenClosedTag) { return "("; }
  static y_absl::string_view TagUbBound(IntervalClosedOpenTag) { return ")"; }
  static y_absl::string_view TagUbBound(IntervalOpenOpenTag) { return ")"; }
  static y_absl::string_view TagUbBound(IntervalClosedClosedTag) { return "]"; }
  static y_absl::string_view TagUbBound(IntervalOpenClosedTag) { return "]"; }

  template <typename TagType>
  static void ValidateImpl(std::true_type /* is_floating_point */, NumType x,
                           TagType tag, NumType lo, NumType hi) {
    UniformDistributionWrapper<NumType> dist(tag, lo, hi);
    NumType lb = dist.a();
    NumType ub = dist.b();
    // uniform_real_distribution is always closed-open, so the upper bound is
    // always non-inclusive.
    Y_ABSL_INTERNAL_CHECK(lb <= x && x < ub,
                        y_absl::StrCat(x, " is not in ", TagLbBound(tag), lo,
                                     ", ", hi, TagUbBound(tag)));
  }

  template <typename TagType>
  static void ValidateImpl(std::false_type /* is_floating_point */, NumType x,
                           TagType tag, NumType lo, NumType hi) {
    using stream_type =
        typename random_internal::stream_format_type<NumType>::type;

    UniformDistributionWrapper<NumType> dist(tag, lo, hi);
    NumType lb = dist.a();
    NumType ub = dist.b();
    Y_ABSL_INTERNAL_CHECK(
        lb <= x && x <= ub,
        y_absl::StrCat(stream_type{x}, " is not in ", TagLbBound(tag),
                     stream_type{lo}, ", ", stream_type{hi}, TagUbBound(tag)));
  }
};

}  // namespace random_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_RANDOM_INTERNAL_MOCK_VALIDATORS_H_
