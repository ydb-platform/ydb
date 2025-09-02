// Copyright 2022 The Abseil Authors.
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
// File: log/internal/test_helpers.h
// -----------------------------------------------------------------------------
//
// This file declares testing helpers for the logging library.

#ifndef Y_ABSL_LOG_INTERNAL_TEST_HELPERS_H_
#define Y_ABSL_LOG_INTERNAL_TEST_HELPERS_H_

#include "gtest/gtest.h"
#include "y_absl/base/config.h"
#include "y_absl/base/log_severity.h"
#include "y_absl/log/globals.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace log_internal {

// `Y_ABSL_MIN_LOG_LEVEL` can't be used directly since it is not always defined.
constexpr auto kAbslMinLogLevel =
#ifdef Y_ABSL_MIN_LOG_LEVEL
    static_cast<y_absl::LogSeverityAtLeast>(Y_ABSL_MIN_LOG_LEVEL);
#else
    y_absl::LogSeverityAtLeast::kInfo;
#endif

// Returns false if the specified severity level is disabled by
// `Y_ABSL_MIN_LOG_LEVEL` or `y_absl::MinLogLevel()`.
bool LoggingEnabledAt(y_absl::LogSeverity severity);

// -----------------------------------------------------------------------------
// Googletest Death Test Predicates
// -----------------------------------------------------------------------------

#if GTEST_HAS_DEATH_TEST

bool DiedOfFatal(int exit_status);
bool DiedOfQFatal(int exit_status);

#endif

// -----------------------------------------------------------------------------
// Helper for Log initialization in test
// -----------------------------------------------------------------------------

class LogTestEnvironment : public ::testing::Environment {
 public:
  ~LogTestEnvironment() override = default;

  void SetUp() override;
};

}  // namespace log_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_LOG_INTERNAL_TEST_HELPERS_H_
