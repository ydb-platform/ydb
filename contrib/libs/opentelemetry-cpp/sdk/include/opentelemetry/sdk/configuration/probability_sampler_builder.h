// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/probability_sampler_configuration.h"
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

class ProbabilitySamplerBuilder
{
public:
  ProbabilitySamplerBuilder()                                                  = default;
  ProbabilitySamplerBuilder(ProbabilitySamplerBuilder &&)                      = default;
  ProbabilitySamplerBuilder(const ProbabilitySamplerBuilder &)                 = default;
  ProbabilitySamplerBuilder &operator=(ProbabilitySamplerBuilder &&)           = default;
  ProbabilitySamplerBuilder &operator=(const ProbabilitySamplerBuilder &other) = default;
  virtual ~ProbabilitySamplerBuilder()                                         = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::Sampler> Build(
      const opentelemetry::sdk::configuration::ProbabilitySamplerConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
