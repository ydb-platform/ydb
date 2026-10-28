// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/configuration/sampler_configuration.h"
#include "opentelemetry/sdk/configuration/sampler_configuration_visitor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/tracer_provider.json
// YAML-NODE: trace_id_ratio_based
class TraceIdRatioBasedSamplerConfiguration : public SamplerConfiguration
{
public:
  static constexpr double kDefaultRatio = 1.0;

  void Accept(SamplerConfigurationVisitor *visitor) const override
  {
    visitor->VisitTraceIdRatioBased(this);
  }

  double ratio{kDefaultRatio};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
