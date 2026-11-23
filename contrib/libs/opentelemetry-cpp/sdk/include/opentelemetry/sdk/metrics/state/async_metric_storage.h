// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/sdk/common/attributemap_hash.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation_config.h"
#include "opentelemetry/sdk/metrics/aggregation/default_aggregation.h"

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
#  include "opentelemetry/sdk/metrics/exemplar/filter_predicate.h"
#  include "opentelemetry/sdk/metrics/exemplar/filter_type.h"
#  include "opentelemetry/sdk/metrics/exemplar/reservoir.h"
#endif

#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/sdk/metrics/observer_result.h"
#include "opentelemetry/sdk/metrics/state/attributes_hashmap.h"
#include "opentelemetry/sdk/metrics/state/metric_collector.h"
#include "opentelemetry/sdk/metrics/state/metric_storage.h"
#include "opentelemetry/sdk/metrics/state/temporal_metric_storage.h"
#include "opentelemetry/sdk/metrics/view/attributes_processor.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class AsyncMetricStorage : public MetricStorage, public AsyncWritableMetricStorage
{
public:
  AsyncMetricStorage(const InstrumentDescriptor &instrument_descriptor,
                     const AggregationType aggregation_type,
                     std::shared_ptr<const AttributesProcessor> attributes_processor,
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
                     ExemplarFilterType exemplar_filter_type,
                     nostd::shared_ptr<ExemplarReservoir> &&exemplar_reservoir,
#endif
                     const AggregationConfig *aggregation_config)
      : instrument_descriptor_(instrument_descriptor),
        aggregation_type_{aggregation_type},
        aggregation_config_{AggregationConfig::GetOrDefault(aggregation_config)},
        attributes_processor_{std::move(attributes_processor)},
        is_monotonic_sum_{IsMonotonicSum(aggregation_type, instrument_descriptor)},
        last_observed_hash_map_(
            std::make_unique<AttributesHashMap>(aggregation_config_->cardinality_limit_)),
        delta_hash_map_(
            std::make_unique<AttributesHashMap>(aggregation_config_->cardinality_limit_)),
        round_hash_map_(
            std::make_unique<AttributesHashMap>(aggregation_config_->cardinality_limit_)),
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
        exemplar_filter_type_(exemplar_filter_type),
        exemplar_reservoir_(std::move(exemplar_reservoir)),
#endif
        temporal_metric_storage_(instrument_descriptor, aggregation_type, aggregation_config)
  {
    create_default_aggregation_ = [this]() -> std::unique_ptr<Aggregation> {
      return DefaultAggregation::CreateAggregation(aggregation_type_, instrument_descriptor_);
    };
  }

  /**
   * Converts the absolute values reported by the callbacks into the deltas the temporal storage
   * consumes. Runs once per callback registered on the instrument, so what it accumulates stays
   * until the next Collect().
   */
  template <class T>
  void Record(const std::unordered_map<MetricAttributes, T, AttributeHashGenerator> &measurements,
              opentelemetry::common::SystemTimestamp /* observation_time */) noexcept
  {
    std::lock_guard<std::mutex> guard(hashmap_lock_);
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
    const bool offer_exemplars =
        ExemplarFilterEnabled(exemplar_filter_type_, opentelemetry::context::Context{});
#endif
    for (auto &measurement : measurements)
    {
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
      if (offer_exemplars)
      {
        exemplar_reservoir_->OfferMeasurement(measurement.second, measurement.first, {});
      }
#endif
      if (is_monotonic_sum_)
      {
        RecordMonotonicSum(measurement.first, measurement.second);
      }
      else
      {
        round_hash_map_
            ->GetOrSetDefault(FilterAttributes(measurement.first), create_default_aggregation_)
            ->Aggregate(measurement.second);
      }
    }
  }

  void RecordLong(
      const std::unordered_map<MetricAttributes, int64_t, AttributeHashGenerator> &measurements,
      opentelemetry::common::SystemTimestamp observation_time) noexcept override
  {
    if (instrument_descriptor_.value_type_ != InstrumentValueType::kLong)
    {
      return;
    }
    Record<int64_t>(measurements, observation_time);
  }

  void RecordDouble(
      const std::unordered_map<MetricAttributes, double, AttributeHashGenerator> &measurements,
      opentelemetry::common::SystemTimestamp observation_time) noexcept override
  {
    if (instrument_descriptor_.value_type_ != InstrumentValueType::kDouble)
    {
      return;
    }
    Record<double>(measurements, observation_time);
  }

  bool Collect(CollectorHandle *collector,
               nostd::span<std::shared_ptr<CollectorHandle>> collectors,
               opentelemetry::common::SystemTimestamp sdk_start_ts,
               opentelemetry::common::SystemTimestamp collection_ts,
               nostd::function_ref<bool(MetricData)> metric_collection_callback) noexcept override
  {

    std::shared_ptr<AttributesHashMap> delta_metrics = nullptr;
    {
      std::lock_guard<std::mutex> guard(hashmap_lock_);
      if (!is_monotonic_sum_)
      {
        BuildDeltaFromRound();
      }
      delta_metrics = std::move(delta_hash_map_);
      delta_hash_map_ =
          std::make_unique<AttributesHashMap>(aggregation_config_->cardinality_limit_);
    }

    auto status =
        temporal_metric_storage_.buildMetrics(collector, collectors, sdk_start_ts, collection_ts,
                                              delta_metrics, metric_collection_callback);
    return status;
  }

private:
  /**
   * Differences a monotonic sum per source series - keyed by the unfiltered attributes - before
   * the view merges anything. Diffing the merged group instead would turn a series which stops
   * being reported into a spurious decrease.
   */
  template <class T>
  void RecordMonotonicSum(const MetricAttributes &attributes, T value)
  {
    auto observed = create_default_aggregation_();
    observed->Aggregate(value);

    std::unique_ptr<Aggregation> delta;
    auto previous = last_observed_hash_map_->Get(attributes);
    if (previous)
    {
      delta = previous->Diff(*observed);
    }
    else
    {
      delta = DefaultAggregation::CloneAggregation(aggregation_type_, instrument_descriptor_,
                                                   *observed);
    }
    last_observed_hash_map_->Set(attributes, std::move(observed));

    AccumulateDelta(FilterAttributes(attributes), *delta);
  }

  /**
   * Adds one series' delta to its output point. Merging, not overwriting, is what lets the series
   * a view collapses together and the observations from different callbacks all contribute.
   * Over-the-limit attribute sets resolve to the same otel.metric.overflow entry, which merges
   * too.
   */
  void AccumulateDelta(const MetricAttributes &attributes, const Aggregation &delta)
  {
    auto merged =
        delta_hash_map_->GetOrSetDefault(attributes, create_default_aggregation_)->Merge(delta);
    delta_hash_map_->Set(attributes, std::move(merged));
  }

  /**
   * Differences this round's absolute observations and starts a fresh round, for everything but
   * monotonic sums. An attribute set missing from this round keeps its baseline and contributes
   * no delta; suppressing the stale output is the temporal storage's job.
   */
  void BuildDeltaFromRound() noexcept
  {
    round_hash_map_->GetAllEntries([this](const MetricAttributes &attributes,
                                          Aggregation &aggregation) {
      auto observed = DefaultAggregation::CloneAggregation(aggregation_type_,
                                                           instrument_descriptor_, aggregation);
      auto previous = last_observed_hash_map_->Get(attributes);
      if (previous)
      {
        delta_hash_map_->Set(attributes, previous->Diff(*observed));
      }
      else
      {
        delta_hash_map_->Set(attributes, DefaultAggregation::CloneAggregation(
                                             aggregation_type_, instrument_descriptor_, *observed));
      }
      last_observed_hash_map_->Set(attributes, std::move(observed));
      return true;
    });

    round_hash_map_ = std::make_unique<AttributesHashMap>(aggregation_config_->cardinality_limit_);
  }

  /**
   * Returns a copy of the observed attributes with the attributes dropped by the view removed.
   */
  MetricAttributes FilterAttributes(const MetricAttributes &attributes) const
  {
    MetricAttributes filtered(attributes);
    if (!attributes_processor_)
    {
      return filtered;
    }

    bool dropped = false;
    for (auto iter = filtered.begin(); iter != filtered.end();)
    {
      if (attributes_processor_->isPresent(iter->first))
      {
        ++iter;
      }
      else
      {
        iter    = filtered.erase(iter);
        dropped = true;
      }
    }

    if (dropped)
    {
      filtered.UpdateHash();
    }
    return filtered;
  }

  /**
   * Whether this storage aggregates into a monotonic sum, matching what
   * DefaultAggregation::CreateAggregation() would create.
   */
  static bool IsMonotonicSum(AggregationType aggregation_type,
                             const InstrumentDescriptor &instrument_descriptor) noexcept
  {
    bool is_monotonic = true;
    if (aggregation_type == AggregationType::kDefault)
    {
      const AggregationType resolved =
          DefaultAggregation::GetDefaultAggregationType(instrument_descriptor.type_, is_monotonic);
      return resolved == AggregationType::kSum && is_monotonic;
    }
    if (aggregation_type != AggregationType::kSum)
    {
      return false;
    }
    return instrument_descriptor.type_ != InstrumentType::kUpDownCounter &&
           instrument_descriptor.type_ != InstrumentType::kObservableUpDownCounter &&
           instrument_descriptor.type_ != InstrumentType::kHistogram;
  }

  InstrumentDescriptor instrument_descriptor_;
  AggregationType aggregation_type_;
  const AggregationConfig *aggregation_config_;
  std::shared_ptr<const AttributesProcessor> attributes_processor_;
  bool is_monotonic_sum_;
  std::function<std::unique_ptr<Aggregation>()> create_default_aggregation_;

  std::unique_ptr<AttributesHashMap> last_observed_hash_map_;
  std::unique_ptr<AttributesHashMap> delta_hash_map_;
  std::unique_ptr<AttributesHashMap> round_hash_map_;

  std::mutex hashmap_lock_;
#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW
  ExemplarFilterType exemplar_filter_type_;
  nostd::shared_ptr<ExemplarReservoir> exemplar_reservoir_;
#endif
  TemporalMetricStorage temporal_metric_storage_;
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
