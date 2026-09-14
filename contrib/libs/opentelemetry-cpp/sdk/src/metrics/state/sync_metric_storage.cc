// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <memory>
#include <mutex>
#include <utility>

#include "opentelemetry/common/spin_lock_mutex.h"
#include "opentelemetry/common/timestamp.h"
#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation_config.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/state/attributes_hashmap.h"
#include "opentelemetry/sdk/metrics/state/sync_metric_storage.h"
#include "opentelemetry/sdk/metrics/state/temporal_metric_storage.h"
#include "opentelemetry/version.h"

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
#  include <stdint.h>
#  include <functional>
#  include <unordered_map>
#  include <unordered_set>
#  include <vector>

#  include "opentelemetry/sdk/common/global_log_handler.h"
#  include "opentelemetry/sdk/metrics/aggregation/aggregation.h"
#  include "opentelemetry/sdk/metrics/data/exemplar_data.h"
#  include "opentelemetry/sdk/metrics/instruments.h"
#endif

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

bool SyncMetricStorage::Collect(CollectorHandle *collector,
                                nostd::span<std::shared_ptr<CollectorHandle>> collectors,
                                opentelemetry::common::SystemTimestamp sdk_start_ts,
                                opentelemetry::common::SystemTimestamp collection_ts,
                                nostd::function_ref<bool(MetricData)> callback) noexcept
{
  // Add the current delta metrics to `unreported metrics stash` for all the collectors,
  // this will also empty the delta metrics hashmap, and make it available for
  // recordings
  std::shared_ptr<AttributesHashMap> delta_metrics = nullptr;
#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
  // Snapshot of bound entries (under map lock) that we will rotate without
  // holding the map lock. Each entry has its own spinlock for the swap.
  std::vector<std::shared_ptr<BoundEntry>> entry_snapshot;
#endif
  {
    std::lock_guard<opentelemetry::common::SpinLockMutex> guard(attribute_hashmap_lock_);
    delta_metrics = std::move(attributes_hashmap_);
    attributes_hashmap_.reset(new AttributesHashMap(aggregation_config_->cardinality_limit_));
#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
    // Garbage-collect entries the user has dropped that have no pending data.
    // Cleanup happens during Collect(); if no collection runs, dropped bound
    // entries remain until storage destruction. We only erase entries the user
    // has released (use_count == 1). `dirty_` must be read under the entry's
    // own lock_ because BoundEntry::RecordLong/RecordDouble and collect-time
    // rotation mutate `dirty_` only while holding that lock.
    for (auto it = bound_entries_.begin(); it != bound_entries_.end();)
    {
      bool can_erase = false;
      if (it->second.use_count() == 1)
      {
        std::lock_guard<opentelemetry::common::SpinLockMutex> g(it->second->lock_);
        can_erase = !it->second->dirty_;
      }
      if (can_erase)
      {
        it = bound_entries_.erase(it);
      }
      else
      {
        ++it;
      }
    }
    // Rebuild the active key set after GC so dropped bound keys no longer
    // count toward cardinality. attributes_hashmap_ has just been reset, so
    // only retained bound entry keys should remain.
    active_keys_.clear();
    for (auto &kv : bound_entries_)
    {
      active_keys_.insert(kv.first);
    }
    entry_snapshot.reserve(bound_entries_.size());
    for (auto &kv : bound_entries_)
    {
      entry_snapshot.push_back(kv.second);
    }
#endif
  }

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
  // Rotate dirty bound entries: under each entry's own spinlock, swap out the
  // current aggregation and merge it into delta_metrics so bound + unbound
  // writes for the same post-filter attribute set produce one datapoint.
  for (auto &entry : entry_snapshot)
  {
    std::unique_ptr<Aggregation> rotated;
    MetricAttributes attrs_copy;
    {
      std::lock_guard<opentelemetry::common::SpinLockMutex> g(entry->lock_);
      if (!entry->dirty_)
      {
        continue;
      }
      rotated         = std::move(entry->current_);
      entry->current_ = create_default_aggregation_();
      entry->dirty_   = false;
      attrs_copy      = entry->attributes_;
    }
    auto *existing = delta_metrics->Get(attrs_copy);
    if (existing)
    {
      delta_metrics->Set(attrs_copy, existing->Merge(*rotated));
    }
    else
    {
      delta_metrics->Set(std::move(attrs_copy), std::move(rotated));
    }
  }

  // Targeted post-rotation cleanup. The pre-rotation GC pass cannot remove
  // entries that were dropped by the user but still had pending data, since
  // they were dirty at that point. After rotation cleared their dirty_ flag,
  // walk the snapshot once more and erase any entry the user has now fully
  // released (use_count == 1, owned only by bound_entries_) and that has not
  // been redirtied. The map lock is briefly released between snapshot and
  // rotation, so concurrent unbound writes may have legitimately added the
  // same key into attributes_hashmap_; in that case we must NOT remove the
  // key from active_keys_ or we would drop their cardinality slot.
  std::vector<MetricAttributes> snapshot_keys;
  snapshot_keys.reserve(entry_snapshot.size());
  for (auto &entry : entry_snapshot)
  {
    snapshot_keys.push_back(entry->attributes_);
  }
  // Drop snapshot refs so use_count reflects only storage + user holders.
  entry_snapshot.clear();
  {
    std::lock_guard<opentelemetry::common::SpinLockMutex> guard(attribute_hashmap_lock_);
    for (const auto &k : snapshot_keys)
    {
      auto it = bound_entries_.find(k);
      if (it == bound_entries_.end())
      {
        continue;
      }
      // User still holds the handle (or rebound during rotation): keep.
      if (it->second.use_count() != 1)
      {
        continue;
      }
      // use_count == 1 means no other thread can be calling RecordLong/Double
      // on this entry, so reading dirty_ is race-free here. Lock anyway to
      // satisfy the documented invariant on dirty_.
      bool entry_dirty = false;
      {
        std::lock_guard<opentelemetry::common::SpinLockMutex> g(it->second->lock_);
        entry_dirty = it->second->dirty_;
      }
      if (entry_dirty)
      {
        continue;
      }
      // Only remove from active_keys_ if no concurrent unbound write has
      // claimed the same key in this interval.
      if (!attributes_hashmap_->Has(k))
      {
        active_keys_.erase(k);
      }
      bound_entries_.erase(it);
    }
  }
#endif

  return temporal_metric_storage_.buildMetrics(collector, collectors, sdk_start_ts, collection_ts,
                                               delta_metrics, callback);
}

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
std::shared_ptr<BoundSyncWritableMetricStorage> SyncMetricStorage::Bind(
    const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  // Filter attributes once, at bind time.
  MetricAttributes filtered{attributes, attributes_processor_.get()};

  std::lock_guard<opentelemetry::common::SpinLockMutex> guard(attribute_hashmap_lock_);

  // Dedupe: same post-filter attribute set returns the same bound entry.
  auto it = bound_entries_.find(filtered);
  if (it != bound_entries_.end())
  {
    return it->second;
  }

  // Apply the unified cardinality policy. ResolveCardinality returns either
  // `filtered` (already-admitted or capacity available) or kOverflowAttributes,
  // updating active_keys_ as needed.
  MetricAttributes key = ResolveCardinality(filtered);

  // If we ended up at overflow, dedupe against an existing overflow entry.
  if (key == GetOverflowAttributes())
  {
    auto ov_it = bound_entries_.find(key);
    if (ov_it != bound_entries_.end())
    {
      return ov_it->second;
    }
  }

  auto entry = std::make_shared<BoundEntry>(instrument_descriptor_.value_type_, key,
                                            create_default_aggregation_());
  bound_entries_.emplace(std::move(key), entry);
  return entry;
}

void SyncMetricStorage::BoundEntry::RecordLong(int64_t value) noexcept
{
  if (value_type_ != InstrumentValueType::kLong)
  {
    OTEL_INTERNAL_LOG_ERROR(
        "[SyncMetricStorage::BoundEntry::RecordLong] Value not recorded - storage value type "
        "is not long");
    return;
  }
  std::lock_guard<opentelemetry::common::SpinLockMutex> guard(lock_);
  current_->Aggregate(value);
  dirty_ = true;
}

void SyncMetricStorage::BoundEntry::RecordDouble(double value) noexcept
{
  if (value_type_ != InstrumentValueType::kDouble)
  {
    OTEL_INTERNAL_LOG_ERROR(
        "[SyncMetricStorage::BoundEntry::RecordDouble] Value not recorded - storage value type "
        "is not double");
    return;
  }
  std::lock_guard<opentelemetry::common::SpinLockMutex> guard(lock_);
  current_->Aggregate(value);
  dirty_ = true;
}
#endif

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
