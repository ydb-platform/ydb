// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/parent_based_sampler_configuration.h"
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

class ParentBasedSamplerBuilder
{
public:
  ParentBasedSamplerBuilder()                                                  = default;
  ParentBasedSamplerBuilder(ParentBasedSamplerBuilder &&)                      = default;
  ParentBasedSamplerBuilder(const ParentBasedSamplerBuilder &)                 = default;
  ParentBasedSamplerBuilder &operator=(ParentBasedSamplerBuilder &&)           = default;
  ParentBasedSamplerBuilder &operator=(const ParentBasedSamplerBuilder &other) = default;
  virtual ~ParentBasedSamplerBuilder()                                         = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::Sampler> Build(
      const opentelemetry::sdk::configuration::ParentBasedSamplerConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::trace::Sampler> &&root,
      std::unique_ptr<opentelemetry::sdk::trace::Sampler> &&remote_parent_sampled,
      std::unique_ptr<opentelemetry::sdk::trace::Sampler> &&remote_parent_not_sampled,
      std::unique_ptr<opentelemetry::sdk::trace::Sampler> &&local_parent_sampled,
      std::unique_ptr<opentelemetry::sdk::trace::Sampler> &&local_parent_not_sampled) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
