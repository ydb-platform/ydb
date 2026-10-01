// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/metrics/meter_config.h"
#include "opentelemetry/sdk/metrics/meter_context.h"
#include "opentelemetry/sdk/metrics/view/view_registry.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/version.h"

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
#  include "opentelemetry/sdk/metrics/exemplar/filter_type.h"
#endif

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

/**
 * Factory class for MeterContext.
 */
class OPENTELEMETRY_EXPORT MeterContextFactory
{
public:
  /**
   * Create a MeterContext with valid defaults.
   * @return A unique pointer to the created MeterContext object.
   */
  static std::unique_ptr<MeterContext> Create();

  /**
   * Create a MeterContext with specified views.
   * @param views ViewRegistry containing OpenTelemetry views registered with this meter context.
   */
  static std::unique_ptr<MeterContext> Create(std::unique_ptr<ViewRegistry> views);

  /**
   * Create a MeterContext with specified views and resource.
   * @param views ViewRegistry containing OpenTelemetry views registered with this meter context.
   * @param resource The OpenTelemetry resource associated with this meter context.
   * @return A unique pointer to the created MeterContext object.
   */
  static std::unique_ptr<MeterContext> Create(
      std::unique_ptr<ViewRegistry> views,
      const opentelemetry::sdk::resource::Resource &resource);

  /**
   * Create a MeterContext with specified views, resource and meter scope configurator.
   * @param views ViewRegistry containing OpenTelemetry views registered with this meter context.
   * @param resource The OpenTelemetry resource associated with this meter context.
   * @param meter_configurator A scope configurator defining the behavior of a meter associated with
   * this meter context.
   * @return A unique pointer to the created MeterContext object.
   */
  static std::unique_ptr<MeterContext> Create(
      std::unique_ptr<ViewRegistry> views,
      const opentelemetry::sdk::resource::Resource &resource,
      std::unique_ptr<instrumentationscope::ScopeConfigurator<MeterConfig>> meter_configurator);

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
  /**
   * Create a MeterContext with an explicitly configured exemplar filter.
   * @param views ViewRegistry containing OpenTelemetry views registered with this meter context.
   * @param resource The OpenTelemetry resource associated with this meter context.
   * @param meter_configurator A scope configurator defining the behavior of a meter associated with
   * this meter context.
   * @param exemplar_filter_type The exemplar filter for this meter context.
   * @return A unique pointer to the created MeterContext object.
   */
  static std::unique_ptr<MeterContext> Create(
      std::unique_ptr<ViewRegistry> views,
      const opentelemetry::sdk::resource::Resource &resource,
      std::unique_ptr<instrumentationscope::ScopeConfigurator<MeterConfig>> meter_configurator,
      ExemplarFilterType exemplar_filter_type);
#endif
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
