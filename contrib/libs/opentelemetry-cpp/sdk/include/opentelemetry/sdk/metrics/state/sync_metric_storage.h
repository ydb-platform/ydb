// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "opentelemetry/common/key_value_iterable.h"
#include "opentelemetry/common/spin_lock_mutex.h"
#include "opentelemetry/common/timestamp.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/common/attributemap_hash.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation_config.h"
#include "opentelemetry/sdk/metrics/aggregation/default_aggregation.h"
#include "opentelemetry/sdk/metrics/aggregation/histogram_aggregation.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/sdk/metrics/state/attributes_hashmap.h"
#include "opentelemetry/sdk/metrics/state/metric_collector.h"
#include "opentelemetry/sdk/metrics/state/metric_storage.h"
#include "opentelemetry/sdk/metrics/state/temporal_metric_storage.h"
#include "opentelemetry/sdk/metrics/view/attributes_processor.h"
#include "opentelemetry/version.h"

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
#  include <unordered_set>
#endif

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
#  include "opentelemetry/sdk/metrics/exemplar/filter_type.h"
#  include "opentelemetry/sdk/metrics/exemplar/reservoir.h"
#endif

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class SyncMetricStorage : public MetricStorage, public SyncWritableMetricStorage
{

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW

  static inline bool EnableExamplarFilter(ExemplarFilterType filter_type,
                                          const opentelemetry::context::Context &context)
  {
    return filter_type == ExemplarFilterType::kAlwaysOn ||
           (filter_type == ExemplarFilterType::kTraceBased &&
            opentelemetry::trace::GetSpan(context)->GetContext().IsValid() &&
            opentelemetry::trace::GetSpan(context)->GetContext().IsSampled());
  }

#endif  // ENABLE_METRICS_EXEMPLAR_PREVIEW

public:
  SyncMetricStorage(const InstrumentDescriptor &instrument_descriptor,
                    const AggregationType aggregation_type,
                    std::shared_ptr<const AttributesProcessor> attributes_processor,
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
                    ExemplarFilterType exempler_filter_type,
                    nostd::shared_ptr<ExemplarReservoir> &&exemplar_reservoir,
#endif
                    const AggregationConfig *aggregation_config)
      : instrument_descriptor_(instrument_descriptor),
        aggregation_config_(AggregationConfig::GetOrDefault(aggregation_config)),
        attributes_hashmap_(
            std::make_unique<AttributesHashMap>(aggregation_config_->cardinality_limit_)),
        attributes_processor_(std::move(attributes_processor)),
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
        exemplar_filter_type_(exempler_filter_type),
        exemplar_reservoir_(std::move(exemplar_reservoir)),
#endif
        temporal_metric_storage_(instrument_descriptor, aggregation_type, aggregation_config)
  {
    create_default_aggregation_ = [&, aggregation_type,
                                   aggregation_config]() -> std::unique_ptr<Aggregation> {
      return DefaultAggregation::CreateAggregation(aggregation_type, instrument_descriptor_,
                                                   aggregation_config);
    };
  }

  void RecordLong(int64_t value,
                  const opentelemetry::context::Context &context
                  OPENTELEMETRY_MAYBE_UNUSED) noexcept override
  {
    if (instrument_descriptor_.value_type_ != InstrumentValueType::kLong)
    {
      return;
    }
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
    if (EnableExamplarFilter(exemplar_filter_type_, context))
    {
      exemplar_reservoir_->OfferMeasurement(value, {}, context, std::chrono::system_clock::now());
    }
#endif
    static MetricAttributes attr = MetricAttributes{};
    std::lock_guard<opentelemetry::common::SpinLockMutex> guard(attribute_hashmap_lock_);
#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
    MetricAttributes resolved = ResolveCardinality(attr);
    attributes_hashmap_->GetOrSetDefault(std::move(resolved), create_default_aggregation_)
        ->Aggregate(value);
#else
    attributes_hashmap_->GetOrSetDefault(attr, create_default_aggregation_)->Aggregate(value);
#endif
  }

  void RecordLong(int64_t value,
                  const opentelemetry::common::KeyValueIterable &attributes,
                  const opentelemetry::context::Context &context
                  OPENTELEMETRY_MAYBE_UNUSED) noexcept override
  {
    if (instrument_descriptor_.value_type_ != InstrumentValueType::kLong)
    {
      return;
    }
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
    if (EnableExamplarFilter(exemplar_filter_type_, context))
    {
      exemplar_reservoir_->OfferMeasurement(value, attributes, context,
                                            std::chrono::system_clock::now());
    }
#endif

    MetricAttributes attr{attributes, attributes_processor_.get()};
    std::lock_guard<opentelemetry::common::SpinLockMutex> guard(attribute_hashmap_lock_);
#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
    // Resolve via the unified cardinality policy so unbound and bound paths
    // share one combined limit (see ResolveCardinality()).
    MetricAttributes resolved = ResolveCardinality(std::move(attr));
    // cppcheck-suppress accessMoved
    attributes_hashmap_->GetOrSetDefault(std::move(resolved), create_default_aggregation_)
        ->Aggregate(value);
#else
    // cppcheck-suppress accessMoved
    attributes_hashmap_->GetOrSetDefault(std::move(attr), create_default_aggregation_)
        ->Aggregate(value);
#endif
  }

  void RecordDouble(double value,
                    const opentelemetry::context::Context &context
                    OPENTELEMETRY_MAYBE_UNUSED) noexcept override
  {
    if (instrument_descriptor_.value_type_ != InstrumentValueType::kDouble)
    {
      return;
    }
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
    if (EnableExamplarFilter(exemplar_filter_type_, context))
    {
      exemplar_reservoir_->OfferMeasurement(value, {}, context, std::chrono::system_clock::now());
    }
#endif
    static MetricAttributes attr = MetricAttributes{};
    std::lock_guard<opentelemetry::common::SpinLockMutex> guard(attribute_hashmap_lock_);
#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
    MetricAttributes resolved = ResolveCardinality(attr);
    attributes_hashmap_->GetOrSetDefault(std::move(resolved), create_default_aggregation_)
        ->Aggregate(value);
#else
    attributes_hashmap_->GetOrSetDefault(attr, create_default_aggregation_)->Aggregate(value);
#endif
  }

  void RecordDouble(double value,
                    const opentelemetry::common::KeyValueIterable &attributes,
                    const opentelemetry::context::Context &context
                    OPENTELEMETRY_MAYBE_UNUSED) noexcept override
  {
    if (instrument_descriptor_.value_type_ != InstrumentValueType::kDouble)
    {
      return;
    }
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
    if (EnableExamplarFilter(exemplar_filter_type_, context))
    {
      exemplar_reservoir_->OfferMeasurement(value, attributes, context,
                                            std::chrono::system_clock::now());
    }
#endif
    MetricAttributes attr{attributes, attributes_processor_.get()};
    std::lock_guard<opentelemetry::common::SpinLockMutex> guard(attribute_hashmap_lock_);
#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
    MetricAttributes resolved = ResolveCardinality(std::move(attr));
    // cppcheck-suppress accessMoved
    attributes_hashmap_->GetOrSetDefault(std::move(resolved), create_default_aggregation_)
        ->Aggregate(value);
#else
    // cppcheck-suppress accessMoved
    attributes_hashmap_->GetOrSetDefault(std::move(attr), create_default_aggregation_)
        ->Aggregate(value);
#endif
  }

  bool Collect(CollectorHandle *collector,
               nostd::span<std::shared_ptr<CollectorHandle>> collectors,
               opentelemetry::common::SystemTimestamp sdk_start_ts,
               opentelemetry::common::SystemTimestamp collection_ts,
               nostd::function_ref<bool(MetricData)> callback) noexcept override;

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
  std::shared_ptr<BoundSyncWritableMetricStorage> Bind(
      const opentelemetry::common::KeyValueIterable &attributes) noexcept override;

  // Internal: stable bound entry. Self-contained: owns its own spinlock and
  // aggregation so the user-held handle stays safe to call even if the parent
  // SyncMetricStorage is destroyed first (writes simply have no observer).
  // Collect() rotates current_ when dirty so bound + unbound writes for the
  // same post-filter attribute set merge into one delta datapoint via the
  // existing TemporalMetricStorage pipeline.
  //
  // Exemplar note: the bound fast path has no per-call Context, so it does not
  // offer measurements to the exemplar reservoir. Callers that need exemplars
  // should use the unbound RecordLong/RecordDouble path.
  class BoundEntry : public BoundSyncWritableMetricStorage
  {
  public:
    BoundEntry(InstrumentValueType value_type,
               MetricAttributes attributes,
               std::unique_ptr<Aggregation> initial_aggregation) noexcept
        : value_type_(value_type),
          attributes_(std::move(attributes)),
          current_(std::move(initial_aggregation))
    {}

    void RecordLong(int64_t value) noexcept override;
    void RecordDouble(double value) noexcept override;

  private:
    friend class SyncMetricStorage;
    InstrumentValueType value_type_;
    MetricAttributes attributes_;
    // Protected by lock_.
    opentelemetry::common::SpinLockMutex lock_;
    std::unique_ptr<Aggregation> current_;
    bool dirty_ = false;
  };
#endif

private:
#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
  // Unified cardinality resolver. Returns either `filtered` unchanged or
  // kOverflowAttributes. Existing keys (already present in active_keys_) pass
  // through unchanged so we never retroactively reroute admitted keys to
  // overflow. New keys are admitted or routed to overflow using the same
  // two-branch logic as AttributesHashMap::IsOverflowAttributes() so the
  // bound and unbound streams share one coherent O(1) cardinality limit
  // without scanning the hashmap.
  //
  // active_keys_ is the union of:
  //   - unbound attribute keys admitted in the current collection interval, and
  //   - retained bound entry keys.
  // It is reset to bound entry keys at every Collect().
  //
  // Must be called with attribute_hashmap_lock_ held.
  MetricAttributes ResolveCardinality(const MetricAttributes &filtered) noexcept
  {
    if (filtered == GetOverflowAttributes())
    {
      active_keys_.insert(GetOverflowAttributes());
      return GetOverflowAttributes();
    }
    if (active_keys_.find(filtered) != active_keys_.end())
    {
      return filtered;
    }
    const size_t limit      = aggregation_config_->cardinality_limit_;
    const bool has_overflow = active_keys_.find(GetOverflowAttributes()) != active_keys_.end();
    // Mirror AttributesHashMap::IsOverflowAttributes() exactly. The configured
    // limit applies to non-overflow attribute sets, while overflow is reserved.
    const size_t non_overflow_size = active_keys_.size() - (has_overflow ? 1 : 0);
    const bool would_overflow      = non_overflow_size >= limit;
    if (would_overflow)
    {
      active_keys_.insert(GetOverflowAttributes());
      return GetOverflowAttributes();
    }
    active_keys_.insert(filtered);
    return filtered;
  }
#endif

  InstrumentDescriptor instrument_descriptor_;
  // hashmap to maintain the metrics for delta collection (i.e, collection since last Collect call)
  const AggregationConfig *aggregation_config_;
  std::unique_ptr<AttributesHashMap> attributes_hashmap_;
  std::function<std::unique_ptr<Aggregation>()> create_default_aggregation_;
  std::shared_ptr<const AttributesProcessor> attributes_processor_;
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
  ExemplarFilterType exemplar_filter_type_;
  nostd::shared_ptr<ExemplarReservoir> exemplar_reservoir_;
#endif
  TemporalMetricStorage temporal_metric_storage_;
  opentelemetry::common::SpinLockMutex attribute_hashmap_lock_;
#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
  // NOTE: ENABLE_METRICS_BOUND_INSTRUMENTS_PREVIEW changes the layout and
  // vtable of SyncMetricStorage (these conditional members and the virtual
  // Bind() method on SyncWritableMetricStorage). It MUST be defined
  // consistently across the SDK library build and every consumer translation
  // unit, otherwise ODR violations and ABI mismatches will result.
  // Bound entries deduped by post-filter attribute set. Lifetime of entries is
  // tied to user-held shared_ptrs returned by Bind() plus this storage. The
  // storage retains a shared_ptr so collect-time rotation always finds them.
  std::unordered_map<MetricAttributes, std::shared_ptr<BoundEntry>, AttributeHashGenerator>
      bound_entries_;
  // Active union of admitted unbound + bound attribute keys for O(1)
  // cardinality decisions. Intentionally duplicates keys also stored in
  // attributes_hashmap_ and bound_entries_ so ResolveCardinality() avoids
  // scanning either container. Guarded by attribute_hashmap_lock_. Reset to
  // bound entry keys at every Collect(), mirroring the per-interval reset of
  // attributes_hashmap_ while retaining bound-entry cardinality cost.
  std::unordered_set<MetricAttributes, AttributeHashGenerator> active_keys_;
#endif
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
