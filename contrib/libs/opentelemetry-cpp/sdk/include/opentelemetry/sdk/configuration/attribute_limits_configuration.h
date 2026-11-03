// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <limits>

#include "opentelemetry/sdk/configuration/optional_value.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/opentelemetry_configuration.json
// YAML-NODE: AttributeLimits
class AttributeLimitsConfiguration
{
public:
  static constexpr std::size_t kDefaultAttributeValueLengthLimit =
      (std::numeric_limits<std::size_t>::max)();
  static constexpr std::size_t kDefaultAttributeCountLimit = 128;

  OptionalValue<std::size_t> attribute_value_length_limit;
  OptionalValue<std::size_t> attribute_count_limit;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
