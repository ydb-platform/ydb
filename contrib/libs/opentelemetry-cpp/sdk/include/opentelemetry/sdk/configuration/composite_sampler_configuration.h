// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/composable_sampler_configuration.h"
#include "opentelemetry/sdk/configuration/sampler_configuration.h"
#include "opentelemetry/sdk/configuration/sampler_configuration_visitor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

class CompositeSamplerConfiguration : public SamplerConfiguration
{
public:
  CompositeSamplerConfiguration() = default;

  std::unique_ptr<ComposableSamplerConfiguration> composable_sampler;

  void Accept(SamplerConfigurationVisitor *visitor) const override
  {
    visitor->VisitComposite(this);
  }
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
