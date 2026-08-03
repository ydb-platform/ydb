// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>

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
  // TODO: spec default is no limit, using 4096 to preserve original behavior
  static constexpr std::size_t kDefaultAttributeValueLengthLimit = 4096;
  static constexpr std::size_t kDefaultAttributeCountLimit       = 128;

  std::size_t attribute_value_length_limit{kDefaultAttributeValueLengthLimit};
  std::size_t attribute_count_limit{kDefaultAttributeCountLimit};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
