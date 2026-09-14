// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>

#include "opentelemetry/sdk/configuration/aggregation_configuration.h"
#include "opentelemetry/sdk/configuration/aggregation_configuration_visitor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/meter_provider.json
// YAML-NODE: base2_exponential_bucket_histogram
class Base2ExponentialBucketHistogramAggregationConfiguration : public AggregationConfiguration
{
public:
  // TODO: max_scale range is [-10, 20]. Negative values require GetSignedInteger in the parser and
  // changing the type of max_scale to an int32_t to match the
  // Base2ExponentialHistogramAggregationConfig.
  static constexpr std::size_t kDefaultMaxScale = 20;  // schema: minimum -10, maximum 20
  static constexpr std::size_t kDefaultMaxSize  = 160;
  static constexpr bool kDefaultRecordMinMax    = true;

  void Accept(AggregationConfigurationVisitor *visitor) const override
  {
    visitor->VisitBase2ExponentialBucketHistogram(this);
  }

  std::size_t max_scale{kDefaultMaxScale};
  std::size_t max_size{kDefaultMaxSize};
  bool record_min_max{kDefaultRecordMinMax};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
