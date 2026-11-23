// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/extension_metric_producer_configuration.h"
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

class ExtensionMetricProducerBuilder
{
public:
  ExtensionMetricProducerBuilder()                                                       = default;
  ExtensionMetricProducerBuilder(ExtensionMetricProducerBuilder &&)                      = default;
  ExtensionMetricProducerBuilder(const ExtensionMetricProducerBuilder &)                 = default;
  ExtensionMetricProducerBuilder &operator=(ExtensionMetricProducerBuilder &&)           = default;
  ExtensionMetricProducerBuilder &operator=(const ExtensionMetricProducerBuilder &other) = default;
  virtual ~ExtensionMetricProducerBuilder()                                              = default;

  virtual std::unique_ptr<opentelemetry::sdk::metrics::MetricProducer> Build(
      const opentelemetry::sdk::configuration::ExtensionMetricProducerConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
