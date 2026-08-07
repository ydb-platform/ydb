// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/trace/samplers/composite_sampler_factory.h"

#include <memory>
#include <utility>

#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/samplers/composable_sampler.h"
#include "opentelemetry/sdk/trace/samplers/composite_sampler.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

std::unique_ptr<Sampler> CompositeSamplerFactory::Create(
    std::shared_ptr<ComposableSampler> delegate)
{
  return std::unique_ptr<Sampler>(new CompositeSampler(std::move(delegate)));
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
