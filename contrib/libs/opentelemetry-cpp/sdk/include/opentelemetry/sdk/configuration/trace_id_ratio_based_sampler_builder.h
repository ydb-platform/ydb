// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/trace_id_ratio_based_sampler_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
class Sampler;
}  // namespace trace

namespace configuration
{

class TraceIdRatioBasedSamplerBuilder
{
public:
  TraceIdRatioBasedSamplerBuilder()                                              = default;
  TraceIdRatioBasedSamplerBuilder(TraceIdRatioBasedSamplerBuilder &&)            = default;
  TraceIdRatioBasedSamplerBuilder(const TraceIdRatioBasedSamplerBuilder &)       = default;
  TraceIdRatioBasedSamplerBuilder &operator=(TraceIdRatioBasedSamplerBuilder &&) = default;
  TraceIdRatioBasedSamplerBuilder &operator=(const TraceIdRatioBasedSamplerBuilder &other) =
      default;
  virtual ~TraceIdRatioBasedSamplerBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::Sampler> Build(
      const opentelemetry::sdk::configuration::TraceIdRatioBasedSamplerConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
