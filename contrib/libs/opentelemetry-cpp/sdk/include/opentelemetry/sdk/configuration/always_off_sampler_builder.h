// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/always_off_sampler_configuration.h"
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

class AlwaysOffSamplerBuilder
{
public:
  AlwaysOffSamplerBuilder()                                                = default;
  AlwaysOffSamplerBuilder(AlwaysOffSamplerBuilder &&)                      = default;
  AlwaysOffSamplerBuilder(const AlwaysOffSamplerBuilder &)                 = default;
  AlwaysOffSamplerBuilder &operator=(AlwaysOffSamplerBuilder &&)           = default;
  AlwaysOffSamplerBuilder &operator=(const AlwaysOffSamplerBuilder &other) = default;
  virtual ~AlwaysOffSamplerBuilder()                                       = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::Sampler> Build(
      const opentelemetry::sdk::configuration::AlwaysOffSamplerConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
