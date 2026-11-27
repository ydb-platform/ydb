// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>

#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/metrics/meter_provider.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/metrics/export/metric_filter.h"
#include "opentelemetry/sdk/metrics/meter_context.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/sdk/metrics/view/instrument_selector.h"
#include "opentelemetry/sdk/metrics/view/meter_selector.h"
#include "opentelemetry/sdk/metrics/view/view.h"
#include "opentelemetry/sdk/metrics/view/view_registry.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/version.h"

#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/metrics/meter.h"

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
#  include "opentelemetry/sdk/metrics/exemplar/filter_type.h"
#endif

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class OPENTELEMETRY_EXPORT MeterProvider final : public opentelemetry::metrics::MeterProvider
{
public:
  /**
   * Initialize a new meter provider with default configuration.
   * This will create a MeterProvider with a default ViewRegistry,
   * a Resource using the OTELResourceDetector, and
   * a default ScopeConfigurator that applies the default MeterConfig to all Meters.
   */
  MeterProvider();

  /**
   * Initialize a new meter provider.
   * @param views The views for this meter provider
   * @param resource  The resources for this meter provider.
   * @param meter_configurator Provides access to a function that computes the MeterConfig for
   * Meters provided by this MeterProvider.
   */
  explicit MeterProvider(
      std::unique_ptr<ViewRegistry> views,
      const sdk::resource::Resource &resource = sdk::resource::Resource::Create({}),
      std::unique_ptr<instrumentationscope::ScopeConfigurator<MeterConfig>> meter_configurator =
          std::make_unique<instrumentationscope::ScopeConfigurator<MeterConfig>>(
              instrumentationscope::ScopeConfigurator<MeterConfig>::Builder(MeterConfig::Default())
                  .Build()));

  /**
   * Initialize a new meter provider with a specified context
   * @param context The owned meter configuration/pipeline for this provider.
   */
  explicit MeterProvider(std::unique_ptr<MeterContext> context);

  MeterProvider(const MeterProvider &)            = delete;
  MeterProvider(MeterProvider &&)                 = delete;
  MeterProvider &operator=(const MeterProvider &) = delete;
  MeterProvider &operator=(MeterProvider &&)      = delete;

  /*
    Make sure GetMeter() helpers from the API are seen in overload resolution.
  */
  using opentelemetry::metrics::MeterProvider::GetMeter;

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  nostd::shared_ptr<opentelemetry::metrics::Meter> GetMeter(
      nostd::string_view name,
      nostd::string_view version,
      nostd::string_view schema_url,
      const opentelemetry::common::KeyValueIterable *attributes) noexcept override;
#else
  nostd::shared_ptr<opentelemetry::metrics::Meter> GetMeter(
      nostd::string_view name,
      nostd::string_view version    = "",
      nostd::string_view schema_url = "") noexcept override;
#endif

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  void RemoveMeter(nostd::string_view name,
                   nostd::string_view version,
                   nostd::string_view schema_url) noexcept override;
#endif

  /**
   * Obtain the resource associated with this meter provider.
   * @return The resource for this meter provider.
   */
  const sdk::resource::Resource &GetResource() const noexcept;

  /**
   * Create a MetricCollector from a MetricReader using the MeterContext of this MeterProvider and
   * add it to the list of configured collectors.
   * @param reader The MetricReader for which a MetricCollector is to be created. This must not be a
   * nullptr.
   * @param metric_filter The optional MetricFilter used when creating the MetricCollector.
   *
   * Note: This reader may not receive any in-flight meter data, but will get newly created meter
   * data.
   * Note: This method is not thread safe, and should ideally be called from main thread.
   */
  void AddMetricReader(std::shared_ptr<MetricReader> reader,
                       std::unique_ptr<MetricFilter> metric_filter = nullptr) noexcept;

  /**
   * Attaches a View to list of configured Views for this Meter provider.
   * @param instrument_selector The instrument selector for this view.
   * @param meter_selector The meter selector for this view.
   * @param view The Views for this meter provider. This
   * must not be a nullptr.
   *
   * Note: This view may not receive any in-flight meter data, but will get newly created meter
   * data. Note: This method is not thread safe, and should ideally be called from main thread.
   */
  void AddView(std::unique_ptr<InstrumentSelector> instrument_selector,
               std::unique_ptr<MeterSelector> meter_selector,
               std::unique_ptr<View> view) noexcept;

  /**
   * Update the MeterConfigurator for this provider, recreate and propagate the resulting
   * MeterConfig to all existing Meters while new Meters will use the updated configuration.
   *
   * @param meter_configurator The new configurator.
   *
   * @note Calling MeterProvider::GetMeter from within the
   * ScopeConfigurator<MeterConfig>::ComputeConfig function (as a scope_matcher callback set with
   * ScopeConfigurator<MeterConfig>::AddCondition) is not supported and will result in a deadlock.
   */
  void UpdateMeterConfigurator(std::unique_ptr<instrumentationscope::ScopeConfigurator<MeterConfig>>
                                   meter_configurator) noexcept;

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW

  void SetExemplarFilter(metrics::ExemplarFilterType exemplar_filter_type =
                             metrics::ExemplarFilterType::kTraceBased) noexcept;

#endif

  /**
   * Shutdown the meter provider.
   */
  bool Shutdown(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept;

  /**
   * Force flush the meter provider.
   */
  bool ForceFlush(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept;

  ~MeterProvider() override;

private:
  std::shared_ptr<MeterContext> context_;
  std::mutex lock_;
  // Allocated during provider construction so GetMeter can return it without allocating.
  nostd::shared_ptr<opentelemetry::metrics::Meter> noop_meter_;
  // Cleared on the first GetMeter construction failure. Later uncached GetMeter
  // calls return the preallocated noop without retrying construction.
  bool initialized_{false};

#if defined(__cpp_lib_atomic_value_initialization) && \
    __cpp_lib_atomic_value_initialization >= 201911L
  std::atomic_flag shutdown_latch_{};
#else
  std::atomic_flag shutdown_latch_ = ATOMIC_FLAG_INIT;
#endif
};
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
