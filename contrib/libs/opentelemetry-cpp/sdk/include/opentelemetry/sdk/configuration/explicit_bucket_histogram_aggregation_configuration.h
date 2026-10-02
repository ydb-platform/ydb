// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <vector>

#include "opentelemetry/sdk/configuration/aggregation_configuration.h"
#include "opentelemetry/sdk/configuration/aggregation_configuration_visitor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/meter_provider.json
// YAML-NODE: explicit_bucket_histogram
class ExplicitBucketHistogramAggregationConfiguration : public AggregationConfiguration
{
public:
  // empty boundaries means use the SDK spec default ([0, 5, 10, ...])
  static constexpr bool kDefaultRecordMinMax = true;

  void Accept(AggregationConfigurationVisitor *visitor) const override
  {
    visitor->VisitExplicitBucketHistogram(this);
  }

  std::vector<double> boundaries;
  bool record_min_max{kDefaultRecordMinMax};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
