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

// YAML-SCHEMA: schema/meter_provider.yaml
// YAML-NODE: CardinalityLimits
class CardinalityLimitsConfiguration
{
public:
  static constexpr std::size_t kDefaultLimit = 2000;
  // 0 is a sentinel: inherit from default_limit (schema exclusiveMinimum: 0)
  static constexpr std::size_t kInheritDefault = 0;

  std::size_t default_limit{kDefaultLimit};
  // For all limits, kInheritDefault means unset, use default_limit
  std::size_t counter{kInheritDefault};
  std::size_t gauge{kInheritDefault};
  std::size_t histogram{kInheritDefault};
  std::size_t observable_counter{kInheritDefault};
  std::size_t observable_gauge{kInheritDefault};
  std::size_t observable_up_down_counter{kInheritDefault};
  std::size_t up_down_counter{kInheritDefault};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
