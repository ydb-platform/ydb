// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/trace/samplers/probability_factory.h"
#include "opentelemetry/sdk/trace/samplers/probability.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

std::unique_ptr<Sampler> ProbabilitySamplerFactory::Create(double ratio)
{
  std::unique_ptr<Sampler> sampler(new ProbabilitySampler(ratio));
  return sampler;
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
