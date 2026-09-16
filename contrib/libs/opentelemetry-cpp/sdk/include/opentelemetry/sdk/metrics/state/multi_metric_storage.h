// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stdint.h>
#include <memory>
#include <unordered_map>
#include <vector>

#include "opentelemetry/common/key_value_iterable.h"
#include "opentelemetry/common/timestamp.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/sdk/metrics/state/attributes_hashmap.h"
#include "opentelemetry/sdk/metrics/state/metric_storage.h"
#include "opentelemetry/sdk/metrics/view/attributes_processor.h"
#include "opentelemetry/version.h"

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
#  include <utility>
#endif

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

class SyncMultiMetricStorage : public SyncWritableMetricStorage
{
public:
  void AddStorage(const std::shared_ptr<SyncWritableMetricStorage> &storage)
  {
    storages_.push_back(storage);
  }

  void RecordLong(int64_t value, const opentelemetry::context::Context &context) noexcept override
  {
    for (auto &s : storages_)
    {
      s->RecordLong(value, context);
    }
  }

  void RecordLong(int64_t value,
                  const opentelemetry::common::KeyValueIterable &attributes,
                  const opentelemetry::context::Context &context) noexcept override
  {
    for (auto &s : storages_)
    {
      s->RecordLong(value, attributes, context);
    }
  }

  void RecordDouble(double value, const opentelemetry::context::Context &context) noexcept override
  {
    for (auto &s : storages_)
    {
      s->RecordDouble(value, context);
    }
  }

  void RecordDouble(double value,
                    const opentelemetry::common::KeyValueIterable &attributes,
                    const opentelemetry::context::Context &context) noexcept override
  {
    for (auto &s : storages_)
    {
      s->RecordDouble(value, attributes, context);
    }
  }

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
  std::shared_ptr<BoundSyncWritableMetricStorage> Bind(
      const opentelemetry::common::KeyValueIterable &attributes) noexcept override;
#endif

private:
  std::vector<std::shared_ptr<SyncWritableMetricStorage>> storages_;
};

class AsyncMultiMetricStorage : public AsyncWritableMetricStorage
{
public:
  void AddStorage(const std::shared_ptr<AsyncWritableMetricStorage> &storage)
  {
    storages_.push_back(storage);
  }

  void RecordLong(
      const std::unordered_map<MetricAttributes, int64_t, AttributeHashGenerator> &measurements,
      opentelemetry::common::SystemTimestamp observation_time) noexcept override
  {
    for (auto &s : storages_)
    {
      s->RecordLong(measurements, observation_time);
    }
  }

  void RecordDouble(
      const std::unordered_map<MetricAttributes, double, AttributeHashGenerator> &measurements,
      opentelemetry::common::SystemTimestamp observation_time) noexcept override
  {
    for (auto &s : storages_)
    {
      s->RecordDouble(measurements, observation_time);
    }
  }

private:
  std::vector<std::shared_ptr<AsyncWritableMetricStorage>> storages_;
};

#ifdef OPENTELEMETRY_HAVE_METRICS_BOUND_INSTRUMENTS_PREVIEW
// Bound handle that fans out to per-view children that support binding.
// Children whose Bind() returns nullptr (no-op storages) are skipped, matching
// the behavior of their unbound RecordLong/RecordDouble.
class MultiBoundEntry : public BoundSyncWritableMetricStorage
{
public:
  explicit MultiBoundEntry(
      std::vector<std::shared_ptr<BoundSyncWritableMetricStorage>> children) noexcept
      : children_(std::move(children))
  {}

  void RecordLong(int64_t value) noexcept override
  {
    for (auto &c : children_)
    {
      c->RecordLong(value);
    }
  }

  void RecordDouble(double value) noexcept override
  {
    for (auto &c : children_)
    {
      c->RecordDouble(value);
    }
  }

private:
  std::vector<std::shared_ptr<BoundSyncWritableMetricStorage>> children_;
};

inline std::shared_ptr<BoundSyncWritableMetricStorage> SyncMultiMetricStorage::Bind(
    const opentelemetry::common::KeyValueIterable &attributes) noexcept
{
  std::vector<std::shared_ptr<BoundSyncWritableMetricStorage>> children;
  children.reserve(storages_.size());
  for (auto &s : storages_)
  {
    auto child = s->Bind(attributes);
    if (child)
    {
      children.push_back(std::move(child));
    }
  }
  // If no child supports binding, return nullptr. The instrument layer will
  // return a no-op bound handle, matching no-op storage behavior.
  if (children.empty())
  {
    return nullptr;
  }
  return std::make_shared<MultiBoundEntry>(std::move(children));
}
#endif

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
