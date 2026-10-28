// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/pull_metric_reader_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class MetricReader;
}  // namespace metrics

namespace configuration
{

class PullMetricReaderBuilder
{
public:
  PullMetricReaderBuilder()                                                = default;
  PullMetricReaderBuilder(PullMetricReaderBuilder &&)                      = default;
  PullMetricReaderBuilder(const PullMetricReaderBuilder &)                 = default;
  PullMetricReaderBuilder &operator=(PullMetricReaderBuilder &&)           = default;
  PullMetricReaderBuilder &operator=(const PullMetricReaderBuilder &other) = default;
  virtual ~PullMetricReaderBuilder()                                       = default;

  virtual std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> Build(
      const opentelemetry::sdk::configuration::PullMetricReaderConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> &&exporter) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
