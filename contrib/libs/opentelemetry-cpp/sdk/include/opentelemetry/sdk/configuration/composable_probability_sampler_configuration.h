// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/configuration/composable_sampler_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

class ComposableProbabilitySamplerConfiguration : public ComposableSamplerConfiguration
{
public:
  static constexpr double kDefaultRatio = 1.0;

  ComposableProbabilitySamplerConfiguration() = default;
  double ratio{kDefaultRatio};
  void Accept(SamplerConfigurationVisitor *visitor) const override;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
