// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/open_census_metric_producer_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class MetricProducer;
}  // namespace metrics

namespace configuration
{

class OpenCensusMetricProducerBuilder
{
public:
  OpenCensusMetricProducerBuilder()                                              = default;
  OpenCensusMetricProducerBuilder(OpenCensusMetricProducerBuilder &&)            = default;
  OpenCensusMetricProducerBuilder(const OpenCensusMetricProducerBuilder &)       = default;
  OpenCensusMetricProducerBuilder &operator=(OpenCensusMetricProducerBuilder &&) = default;
  OpenCensusMetricProducerBuilder &operator=(const OpenCensusMetricProducerBuilder &other) =
      default;
  virtual ~OpenCensusMetricProducerBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::metrics::MetricProducer> Build(
      const opentelemetry::sdk::configuration::OpenCensusMetricProducerConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
