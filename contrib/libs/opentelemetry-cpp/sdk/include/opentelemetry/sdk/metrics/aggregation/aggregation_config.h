// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <vector>

#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/sdk/metrics/state/attributes_hashmap.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class AggregationConfig
{
public:
  AggregationConfig(size_t cardinality_limit = kAggregationCardinalityLimit)
      : cardinality_limit_(cardinality_limit)
  {}

  AggregationConfig(const AggregationConfig &)            = default;
  AggregationConfig(AggregationConfig &&)                 = default;
  AggregationConfig &operator=(const AggregationConfig &) = default;
  AggregationConfig &operator=(AggregationConfig &&)      = default;

  virtual AggregationType GetType() const noexcept { return AggregationType::kDefault; }

  static const AggregationConfig *GetOrDefault(const AggregationConfig *config)
  {
    if (config)
    {
      return config;
    }
    static const AggregationConfig default_config{};
    return &default_config;
  }

  size_t cardinality_limit_;
  virtual ~AggregationConfig() = default;
};

class HistogramAggregationConfig : public AggregationConfig
{
public:
  HistogramAggregationConfig(size_t cardinality_limit = kAggregationCardinalityLimit)
      : AggregationConfig(cardinality_limit)
  {}

  AggregationType GetType() const noexcept override { return AggregationType::kHistogram; }

  // The SDK-specified default bucket boundaries, used when no boundaries are configured.
  static const std::vector<double> &DefaultBoundaries()
  {
    static const std::vector<double> boundaries = {0.0,    5.0,    10.0,   25.0,   50.0,
                                                   75.0,   100.0,  250.0,  500.0,  750.0,
                                                   1000.0, 2500.0, 5000.0, 7500.0, 10000.0};
    return boundaries;
  }

  std::vector<double> boundaries_;
  bool record_min_max_ = true;
};

// Valid ranges per the declarative configuration schema; the schema defines no maximum for
// max_size.
// https://github.com/open-telemetry/opentelemetry-configuration/blob/main/schema/meter_provider.yaml
constexpr std::int32_t kMaxScaleMin = -10;
constexpr std::int32_t kMaxScaleMax = 20;
constexpr std::size_t kMaxSizeMin   = 2;

class Base2ExponentialHistogramAggregationConfig : public AggregationConfig
{
public:
  Base2ExponentialHistogramAggregationConfig(
      size_t cardinality_limit = kAggregationCardinalityLimit)
      : AggregationConfig(cardinality_limit)
  {}

  AggregationType GetType() const noexcept override
  {
    return AggregationType::kBase2ExponentialHistogram;
  }

  size_t max_size_     = 160;
  int32_t max_scale_   = 20;
  bool record_min_max_ = true;
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
