// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "opentelemetry/common/macros.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/metrics/noop.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/unique_ptr.h"
#include "opentelemetry/sdk/common/attributemap_hash.h"
#include "opentelemetry/sdk/instrumentationscope/instrumentation_scope.h"
#include "opentelemetry/sdk/metrics/instrument_metadata_validator.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/sdk/metrics/meter_config.h"
#include "opentelemetry/sdk/metrics/meter_context.h"
#include "opentelemetry/sdk/metrics/meter_enabled_state.h"
#include "opentelemetry/sdk/metrics/state/async_metric_storage.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk_config.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class MetricStorage;
class SyncWritableMetricStorage;
class AsyncWritableMetricsStorge;
class ObservableRegistry;

class Meter final : public opentelemetry::metrics::Meter
{
public:
  /** Construct a new Meter with the given  pipeline. */
  explicit Meter(
      std::weak_ptr<sdk::metrics::MeterContext> meter_context,
      std::unique_ptr<opentelemetry::sdk::instrumentationscope::InstrumentationScope> scope =
          opentelemetry::sdk::instrumentationscope::InstrumentationScope::Create(""));

  nostd::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> CreateUInt64Counter(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::unique_ptr<opentelemetry::metrics::Counter<double>> CreateDoubleCounter(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument> CreateInt64ObservableCounter(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument> CreateDoubleObservableCounter(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> CreateUInt64Histogram(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::unique_ptr<opentelemetry::metrics::Histogram<double>> CreateDoubleHistogram(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  nostd::unique_ptr<opentelemetry::metrics::Gauge<int64_t>> CreateInt64Gauge(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::unique_ptr<opentelemetry::metrics::Gauge<double>> CreateDoubleGauge(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;
#endif

  nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument> CreateInt64ObservableGauge(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument> CreateDoubleObservableGauge(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::unique_ptr<opentelemetry::metrics::UpDownCounter<int64_t>> CreateInt64UpDownCounter(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::unique_ptr<opentelemetry::metrics::UpDownCounter<double>> CreateDoubleUpDownCounter(
      nostd::string_view name,
      nostd::string_view description = "",
      nostd::string_view unit        = "") noexcept override;

  nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
  CreateInt64ObservableUpDownCounter(nostd::string_view name,
                                     nostd::string_view description = "",
                                     nostd::string_view unit        = "") noexcept override;

  nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
  CreateDoubleObservableUpDownCounter(nostd::string_view name,
                                      nostd::string_view description = "",
                                      nostd::string_view unit        = "") noexcept override;

  /** Returns the associated instrumentation scope */
  const sdk::instrumentationscope::InstrumentationScope *GetInstrumentationScope() const noexcept;

  OPENTELEMETRY_DEPRECATED_MESSAGE("Please use GetInstrumentationScope instead")
  const sdk::instrumentationscope::InstrumentationScope *GetInstrumentationLibrary() const noexcept
  {
    return GetInstrumentationScope();
  }

  /** collect metrics across all the instruments configured for the meter **/
  std::vector<MetricData> Collect(CollectorHandle *collector,
                                  opentelemetry::common::SystemTimestamp collect_ts) noexcept;

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  uintptr_t RegisterCallback(
      opentelemetry::metrics::MultiObservableCallbackPtr callback,
      void *state,
      nostd::span<opentelemetry::metrics::ObservableInstrument *> instruments) noexcept override;

  void DeregisterCallback(uintptr_t callback_id) noexcept override;
#endif
private:
  // MeterProvider needs access to UpdateMeterConfig to propagate configuration updates to
  // existing meters.
  friend class MeterProvider;

  /**
   * Update this meter's MeterConfig. Called on construction, and by
   * MeterProvider::UpdateMeterConfigurator when the provider-level MeterConfigurator is
   * replaced at runtime.
   */
  void UpdateMeterConfig(MeterConfig config) noexcept;

  /** Returns whether this meter is enabled by its current MeterConfig. */
  bool IsEnabled() const noexcept { return meter_enabled_state_->IsEnabled(); }

  // order of declaration is important here - instrumentation scope should destroy after
  // meter-context.
  std::unique_ptr<sdk::instrumentationscope::InstrumentationScope> scope_;
  std::weak_ptr<sdk::metrics::MeterContext> meter_context_;
  // Mapping between instrument descriptor and Aggregation Storage.
  using MetricStorageMap = std::unordered_map<InstrumentDescriptor,
                                              std::shared_ptr<MetricStorage>,
                                              InstrumentDescriptorHash,
                                              InstrumentEqualNameCaseInsensitive>;
  MetricStorageMap storage_registry_;
  std::shared_ptr<ObservableRegistry> observable_registry_;
  // Shared with this meter's instruments so updates reach them; atomic so recording never blocks.
  std::shared_ptr<MeterEnabledState> meter_enabled_state_{new MeterEnabledState()};
  std::unique_ptr<SyncWritableMetricStorage> RegisterSyncMetricStorage(
      InstrumentDescriptor &instrument_descriptor);
  std::unique_ptr<AsyncWritableMetricStorage> RegisterAsyncMetricStorage(
      InstrumentDescriptor &instrument_descriptor);
  std::mutex storage_lock_;

  static nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument>
  GetNoopObservableInsrument()
  {
    static nostd::shared_ptr<opentelemetry::metrics::ObservableInstrument> noop_instrument(
        new opentelemetry::metrics::NoopObservableInstrument("", "", ""));
    return noop_instrument;
  }

  static bool ValidateInstrument(nostd::string_view name,
                                 nostd::string_view description,
                                 nostd::string_view unit)
  {
    const static InstrumentMetaDataValidator instrument_validator;
    return instrument_validator.ValidateName(name) && instrument_validator.ValidateUnit(unit) &&
           instrument_validator.ValidateDescription(description);
  }

  // This function checks if the instrument is a duplicate of an existing one
  // and emits a warning through the internal logger.
  static void WarnOnDuplicateInstrument(
      const sdk::instrumentationscope::InstrumentationScope *scope,
      const MetricStorageMap &storage_registry,
      const InstrumentDescriptor &new_instrument);

  // This function checks if the instrument has a name case conflict with an existing one
  // and emits a warning through the internal logger.
  static void WarnOnNameCaseConflict(const sdk::instrumentationscope::InstrumentationScope *scope,
                                     const InstrumentDescriptor &existing_instrument,
                                     const InstrumentDescriptor &new_instrument);

  // Emits a warning that the view will create a semantic error and will be ignored.
  static void WarnOnViewSemanticError(const sdk::instrumentationscope::InstrumentationScope *scope,
                                      const InstrumentDescriptor &existing_instrument,
                                      const InstrumentDescriptor &stream,
                                      const View &view);
};
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
