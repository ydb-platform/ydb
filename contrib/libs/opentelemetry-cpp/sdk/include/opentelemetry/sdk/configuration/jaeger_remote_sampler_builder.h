// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/jaeger_remote_sampler_configuration.h"
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

class JaegerRemoteSamplerBuilder
{
public:
  JaegerRemoteSamplerBuilder()                                                   = default;
  JaegerRemoteSamplerBuilder(JaegerRemoteSamplerBuilder &&)                      = default;
  JaegerRemoteSamplerBuilder(const JaegerRemoteSamplerBuilder &)                 = default;
  JaegerRemoteSamplerBuilder &operator=(JaegerRemoteSamplerBuilder &&)           = default;
  JaegerRemoteSamplerBuilder &operator=(const JaegerRemoteSamplerBuilder &other) = default;
  virtual ~JaegerRemoteSamplerBuilder()                                          = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::Sampler> Build(
      const opentelemetry::sdk::configuration::JaegerRemoteSamplerConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::trace::Sampler> &&initial_sampler) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
