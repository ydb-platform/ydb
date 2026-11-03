// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
class ComposableSampler;
class Sampler;
}  // namespace trace

namespace configuration
{

class CompositeSamplerBuilder
{
public:
  CompositeSamplerBuilder()                                                = default;
  CompositeSamplerBuilder(CompositeSamplerBuilder &&)                      = default;
  CompositeSamplerBuilder(const CompositeSamplerBuilder &)                 = default;
  CompositeSamplerBuilder &operator=(CompositeSamplerBuilder &&)           = default;
  CompositeSamplerBuilder &operator=(const CompositeSamplerBuilder &other) = default;
  virtual ~CompositeSamplerBuilder()                                       = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::Sampler> Build(
      std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler> &&sampler) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
