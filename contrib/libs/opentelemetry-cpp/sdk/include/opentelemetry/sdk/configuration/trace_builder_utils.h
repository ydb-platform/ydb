// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/registry.h"
#include "opentelemetry/sdk/configuration/sampler_configuration.h"
#include "opentelemetry/sdk/configuration/span_exporter_configuration.h"
#include "opentelemetry/sdk/configuration/span_processor_configuration.h"
#include "opentelemetry/sdk/configuration/tracer_configurator_configuration.h"
#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/processor.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/tracer_config.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

/**
 * Common utility functions for building trace SDK components from configuration models
 */
class TraceBuilderUtils
{
public:
  static std::unique_ptr<opentelemetry::sdk::trace::Sampler> CreateSampler(
      const Registry *registry,
      const std::unique_ptr<SamplerConfiguration> &model);

  static std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> CreateSpanExporter(
      const Registry *registry,
      const std::unique_ptr<SpanExporterConfiguration> &model);

  static std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> CreateSpanProcessor(
      const Registry *registry,
      const std::unique_ptr<SpanProcessorConfiguration> &model);

  static std::unique_ptr<opentelemetry::sdk::instrumentationscope::ScopeConfigurator<
      opentelemetry::sdk::trace::TracerConfig>>
  CreateTracerConfigurator(const Registry *registry,
                           const std::unique_ptr<TracerConfiguratorConfiguration> &model);
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
