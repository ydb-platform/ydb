// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <atomic>
#include <chrono>
#include <exception>
#include <mutex>
#include <ostream>
#include <utility>

#include "opentelemetry/common/key_value_iterable.h"  // IWYU pragma: keep
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/metrics/noop.h"
#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/instrumentationscope/instrumentation_scope.h"
#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/metrics/export/metric_filter.h"
#include "opentelemetry/sdk/metrics/meter.h"
#include "opentelemetry/sdk/metrics/meter_config.h"
#include "opentelemetry/sdk/metrics/meter_context.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/sdk/metrics/view/instrument_selector.h"
#include "opentelemetry/sdk/metrics/view/meter_selector.h"
#include "opentelemetry/sdk/metrics/view/view.h"
#include "opentelemetry/sdk/metrics/view/view_registry.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/version.h"

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
#  include "opentelemetry/sdk/metrics/exemplar/filter_type.h"
#endif  // ENABLE_METRICS_EXEMPLAR_PREVIEW

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
namespace resource    = opentelemetry::sdk::resource;
namespace metrics_api = opentelemetry::metrics;

namespace
{

nostd::shared_ptr<metrics_api::Meter> CreateNoopMeterFallback()
{
  return nostd::shared_ptr<metrics_api::Meter>(new metrics_api::NoopMeter());
}

void LogGetMeterConstructionFailure(const char *detail) noexcept
{
#if OPENTELEMETRY_HAVE_EXCEPTIONS
  try
  {
#endif
    OTEL_INTERNAL_LOG_ERROR("[MeterProvider::GetMeter] Failed to construct meter: "
                            << detail << "; returning noop meter.");
#if OPENTELEMETRY_HAVE_EXCEPTIONS
  }
  catch (const std::exception &)  // NOLINT(bugprone-empty-catch)
  {
    // Logging can throw (typically std::bad_alloc from the string stream).
    // Swallow it so the noexcept GetMeter path cannot throw.
  }
#endif
}

}  // namespace

MeterProvider::MeterProvider()
    : context_(std::make_shared<MeterContext>(
          std::make_unique<ViewRegistry>(),
          resource::Resource::Create({}),
          std::make_unique<instrumentationscope::ScopeConfigurator<MeterConfig>>(
              instrumentationscope::ScopeConfigurator<MeterConfig>::Builder(MeterConfig::Default())
                  .Build()))),
      noop_meter_(CreateNoopMeterFallback()),
      initialized_(true)
{}

MeterProvider::MeterProvider(std::unique_ptr<MeterContext> context)
    : context_(std::move(context)), noop_meter_(CreateNoopMeterFallback()), initialized_(true)
{}

MeterProvider::MeterProvider(
    std::unique_ptr<ViewRegistry> views,
    const sdk::resource::Resource &resource,
    std::unique_ptr<instrumentationscope::ScopeConfigurator<MeterConfig>> meter_configurator)
    : context_(std::make_shared<MeterContext>(std::move(views),
                                              resource,
                                              std::move(meter_configurator))),
      noop_meter_(CreateNoopMeterFallback())
{
  OTEL_INTERNAL_LOG_DEBUG("[MeterProvider] MeterProvider created.");
  initialized_ = true;
}

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
nostd::shared_ptr<metrics_api::Meter> MeterProvider::GetMeter(
    nostd::string_view name,
    nostd::string_view version,
    nostd::string_view schema_url,
    const opentelemetry::common::KeyValueIterable *attributes) noexcept
#else
nostd::shared_ptr<metrics_api::Meter> MeterProvider::GetMeter(
    nostd::string_view name,
    nostd::string_view version,
    nostd::string_view schema_url) noexcept
#endif
{
#if OPENTELEMETRY_ABI_VERSION_NO < 2
  const opentelemetry::common::KeyValueIterable *attributes = nullptr;
#endif

  if (name.data() == nullptr || name == "")
  {
    OTEL_INTERNAL_LOG_WARN("[MeterProvider::GetMeter] Library name is empty.");
    name = "";
  }

#if OPENTELEMETRY_HAVE_EXCEPTIONS
  try
  {
#endif
    // Release the lock before construction-failure diagnostics can re-enter the provider.
    const std::lock_guard<std::mutex> guard(lock_);

    for (auto &meter : context_->GetMeters())
    {
      auto meter_lib = meter->GetInstrumentationScope();
      if (meter_lib->equal(name, version, schema_url, attributes))
      {
        return nostd::shared_ptr<metrics_api::Meter>{meter};
      }
    }

    // GetMeter is noexcept and does not surface an error, so callers cannot
    // recover after a construction failure. Retrying would re-throw, catch, and
    // log on later GetMeter calls. After the first failure, treat the provider
    // as non-functional for new meters.
    if (!initialized_)
    {
      return noop_meter_;
    }

    initialized_ = false;
    instrumentationscope::InstrumentationScopeAttributes attrs_map(attributes);
    auto scope =
        instrumentationscope::InstrumentationScope::Create(name, version, schema_url, attrs_map);

    auto meter = std::make_shared<Meter>(context_, std::move(scope));
    context_->AddMeter(meter);
    nostd::shared_ptr<metrics_api::Meter> result{meter};
    initialized_ = true;
    return result;
#if OPENTELEMETRY_HAVE_EXCEPTIONS
  }
  catch (const std::exception &ex)
  {
    LogGetMeterConstructionFailure(ex.what());
    return noop_meter_;
  }
  // User-provided scope configurators can throw any exception type, not just
  // std::exception. Catch everything so GetMeter stays noexcept.
  catch (...)
  {
    LogGetMeterConstructionFailure("unknown exception");
    return noop_meter_;
  }
#endif
}

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
void MeterProvider::RemoveMeter(nostd::string_view name,
                                nostd::string_view version,
                                nostd::string_view schema_url) noexcept
{
  if (name.data() == nullptr || name == "")
  {
    OTEL_INTERNAL_LOG_WARN("[MeterProvider::RemoveMeter] Library name is empty.");
    name = "";
  }

  const std::lock_guard<std::mutex> guard(lock_);

  context_->RemoveMeter(name, version, schema_url);
}
#endif

const resource::Resource &MeterProvider::GetResource() const noexcept
{
  return context_->GetResource();
}

void MeterProvider::AddMetricReader(std::shared_ptr<MetricReader> reader,
                                    std::unique_ptr<MetricFilter> metric_filter) noexcept
{
  context_->AddMetricReader(std::move(reader), std::move(metric_filter));
}

void MeterProvider::AddView(std::unique_ptr<InstrumentSelector> instrument_selector,
                            std::unique_ptr<MeterSelector> meter_selector,
                            std::unique_ptr<View> view) noexcept
{
  context_->AddView(std::move(instrument_selector), std::move(meter_selector), std::move(view));
}

void MeterProvider::UpdateMeterConfigurator(
    std::unique_ptr<instrumentationscope::ScopeConfigurator<MeterConfig>>
        meter_configurator) noexcept
{
  if (!meter_configurator)
  {
    OTEL_INTERNAL_LOG_ERROR(
        "[MeterProvider::UpdateMeterConfigurator] meter_configurator must not be null, "
        "ignoring.");
    return;
  }

  // Shares the lock with GetMeter so a Meter is never returned while its MeterConfig is out of
  // date with the provider configurator.
  const std::lock_guard<std::mutex> guard(lock_);
  context_->SetMeterConfigurator(std::move(meter_configurator));

  // Takes meter_lock_ after lock_, the same order as GetMeter and RemoveMeter.
  const auto &configurator = context_->GetMeterConfigurator();
  context_->ForEachMeter([&configurator](std::shared_ptr<Meter> &meter) {
    meter->UpdateMeterConfig(configurator.ComputeConfig(*meter->GetInstrumentationScope()));
    return true;
  });
}

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW

void MeterProvider::SetExemplarFilter(metrics::ExemplarFilterType exemplar_filter_type) noexcept
{
  context_->SetExemplarFilter(exemplar_filter_type);
}

#endif  // ENABLE_METRICS_EXEMPLAR_PREVIEW

/**
 * Shutdown the meter provider.
 */
bool MeterProvider::Shutdown(std::chrono::microseconds timeout) noexcept
{
  // Shutdown only once
  if (shutdown_latch_.test_and_set(std::memory_order_acquire))
  {
    return true;
  }
  return context_->Shutdown(timeout);
}

/**
 * Force flush the meter provider.
 */
bool MeterProvider::ForceFlush(std::chrono::microseconds timeout) noexcept
{
  return context_->ForceFlush(timeout);
}

/**
 * Shutdown MeterContext when MeterProvider is destroyed.
 *
 */
MeterProvider::~MeterProvider()
{
  if (context_)
  {
    Shutdown();
  }
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
