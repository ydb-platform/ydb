// Copyright 2017 The Abseil Authors.
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

#ifndef Y_ABSL_TIME_INTERNAL_TEST_UTIL_H_
#define Y_ABSL_TIME_INTERNAL_TEST_UTIL_H_

#include <util/generic/string.h>

#include "y_absl/time/time.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace time_internal {

// Loads the named timezone, but dies on any failure.
y_absl::TimeZone LoadTimeZone(const TString& name);

}  // namespace time_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_TIME_INTERNAL_TEST_UTIL_H_
