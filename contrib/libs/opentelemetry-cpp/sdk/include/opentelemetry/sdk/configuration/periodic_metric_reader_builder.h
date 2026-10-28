// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/periodic_metric_reader_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class MetricReader;
class PushMetricExporter;
}  // namespace metrics

namespace configuration
{

class PeriodicMetricReaderBuilder
{
public:
  PeriodicMetricReaderBuilder()                                                    = default;
  PeriodicMetricReaderBuilder(PeriodicMetricReaderBuilder &&)                      = default;
  PeriodicMetricReaderBuilder(const PeriodicMetricReaderBuilder &)                 = default;
  PeriodicMetricReaderBuilder &operator=(PeriodicMetricReaderBuilder &&)           = default;
  PeriodicMetricReaderBuilder &operator=(const PeriodicMetricReaderBuilder &other) = default;
  virtual ~PeriodicMetricReaderBuilder()                                           = default;

  virtual std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> Build(
      const opentelemetry::sdk::configuration::PeriodicMetricReaderConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> &&exporter) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
