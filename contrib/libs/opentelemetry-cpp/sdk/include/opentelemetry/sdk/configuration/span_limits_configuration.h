// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#include "opentelemetry/sdk/configuration/optional_value.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/tracer_provider.json
// YAML-NODE: SpanLimits
class SpanLimitsConfiguration
{
public:
  static constexpr std::size_t kDefaultAttributeValueLengthLimit =
      (std::numeric_limits<std::size_t>::max)();
  static constexpr std::uint32_t kDefaultAttributeCountLimit      = 128;
  static constexpr std::uint32_t kDefaultEventCountLimit          = 128;
  static constexpr std::uint32_t kDefaultLinkCountLimit           = 128;
  static constexpr std::uint32_t kDefaultEventAttributeCountLimit = 128;
  static constexpr std::uint32_t kDefaultLinkAttributeCountLimit  = 128;

  OptionalValue<std::size_t> attribute_value_length_limit;
  OptionalValue<std::uint32_t> attribute_count_limit;
  OptionalValue<std::uint32_t> event_count_limit;
  OptionalValue<std::uint32_t> link_count_limit;
  OptionalValue<std::uint32_t> event_attribute_count_limit;
  OptionalValue<std::uint32_t> link_attribute_count_limit;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
