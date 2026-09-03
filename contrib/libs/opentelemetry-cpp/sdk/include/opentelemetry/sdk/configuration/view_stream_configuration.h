// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "opentelemetry/sdk/configuration/aggregation_configuration.h"
#include "opentelemetry/sdk/configuration/include_exclude_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/meter_provider.json
// YAML-NODE: ViewStream
class ViewStreamConfiguration
{
public:
  // 0 is a sentinel: inherit cardinality limit from the metric reader (schema exclusiveMinimum: 0)
  static constexpr std::size_t kInheritFromReader = 0;

  std::string name;
  std::string description;
  std::unique_ptr<AggregationConfiguration> aggregation;
  std::size_t aggregation_cardinality_limit{kInheritFromReader};
  std::unique_ptr<IncludeExcludeConfiguration> attribute_keys;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
