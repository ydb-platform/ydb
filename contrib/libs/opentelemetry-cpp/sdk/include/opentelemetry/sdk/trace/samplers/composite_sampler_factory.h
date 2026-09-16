// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/samplers/composable_sampler.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * Factory for CompositeSampler, the Sampler that drives a ComposableSampler.
 */
class CompositeSamplerFactory
{
public:
  static std::unique_ptr<Sampler> Create(std::shared_ptr<ComposableSampler> delegate);
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
