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
  static constexpr double kDefaultRatio = 1.0;  // schema: minimum 0, maximum 1
  static constexpr double kMinRatio     = 0.0;
  static constexpr double kMaxRatio     = 1.0;

  ComposableProbabilitySamplerConfiguration() = default;
  double ratio{kDefaultRatio};
  void Accept(ComposableSamplerConfigurationVisitor *visitor) const override;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
