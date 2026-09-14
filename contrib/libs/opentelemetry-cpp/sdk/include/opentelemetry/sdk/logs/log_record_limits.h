// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <limits>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{

/**
 * LogRecordLimits carries the SDK-level attribute limits applied to every
 * LogRecord emitted through a LoggerProvider. The defaults match the
 * specification: at most 128 attributes per record, attribute value length
 * unlimited (no truncation).
 *
 * See https://opentelemetry.io/docs/specs/otel/logs/sdk/#logrecord-limits.
 */
struct LogRecordLimits
{
  static constexpr std::size_t kDefaultAttributeCountLimit = 128;
  static constexpr std::size_t kDefaultAttributeValueLengthLimit =
      (std::numeric_limits<std::size_t>::max)();

  std::size_t attribute_count_limit        = kDefaultAttributeCountLimit;
  std::size_t attribute_value_length_limit = kDefaultAttributeValueLengthLimit;

  /**
   * Limits that impose no cap. A recordable uses these until a LoggerProvider
   * injects the configured limits, so a recordable created outside a provider
   * does not enforce any limit on its own.
   */
  static constexpr LogRecordLimits NoLimits() noexcept
  {
    return LogRecordLimits{(std::numeric_limits<std::size_t>::max)(),
                           (std::numeric_limits<std::size_t>::max)()};
  }
};

}  // namespace logs
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
