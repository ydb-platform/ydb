// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/always_on_sampler_configuration.h"
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

class AlwaysOnSamplerBuilder
{
public:
  AlwaysOnSamplerBuilder()                                               = default;
  AlwaysOnSamplerBuilder(AlwaysOnSamplerBuilder &&)                      = default;
  AlwaysOnSamplerBuilder(const AlwaysOnSamplerBuilder &)                 = default;
  AlwaysOnSamplerBuilder &operator=(AlwaysOnSamplerBuilder &&)           = default;
  AlwaysOnSamplerBuilder &operator=(const AlwaysOnSamplerBuilder &other) = default;
  virtual ~AlwaysOnSamplerBuilder()                                      = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::Sampler> Build(
      const opentelemetry::sdk::configuration::AlwaysOnSamplerConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
