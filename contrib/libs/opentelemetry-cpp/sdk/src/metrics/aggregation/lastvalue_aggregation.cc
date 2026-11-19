// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>

#include "opentelemetry/common/spin_lock_mutex.h"
#include "opentelemetry/common/timestamp.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation.h"
#include "opentelemetry/sdk/metrics/aggregation/lastvalue_aggregation.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/data/point_data.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
namespace
{

// Returns whichever of `curr` and `other` has the newer sample, or `curr` if `other` is not a
// LastValue aggregation.
LastValuePointData GetLastValuePointData(const LastValuePointData &curr,
                                         const Aggregation &other) noexcept
{
  const PointType other_point = other.ToPoint();
  const auto *other_data      = nostd::get_if<LastValuePointData>(&other_point);
  if (other_data == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("LastValueAggregation - Loss of type");
    return curr;
  }
  return curr.sample_ts_.time_since_epoch() > other_data->sample_ts_.time_since_epoch()
             ? curr
             : *other_data;
}

}  // namespace

LongLastValueAggregation::LongLastValueAggregation()
{
  point_data_.is_lastvalue_valid_ = false;
  point_data_.value_              = static_cast<int64_t>(0);
}

LongLastValueAggregation::LongLastValueAggregation(const LastValuePointData &data)
    : point_data_{data}
{}

void LongLastValueAggregation::Aggregate(int64_t value,
                                         const PointAttributes & /* attributes */) noexcept
{
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  point_data_.is_lastvalue_valid_ = true;
  point_data_.value_              = value;
  point_data_.sample_ts_          = std::chrono::system_clock::now();
}

std::unique_ptr<Aggregation> LongLastValueAggregation::Merge(
    const Aggregation &delta) const noexcept
{
  LastValuePointData curr_data;
  {
    const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
    curr_data = point_data_;
  }
  return std::unique_ptr<Aggregation>(
      new LongLastValueAggregation(GetLastValuePointData(curr_data, delta)));
}

std::unique_ptr<Aggregation> LongLastValueAggregation::Diff(const Aggregation &next) const noexcept
{
  LastValuePointData curr_data;
  {
    const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
    curr_data = point_data_;
  }
  return std::unique_ptr<Aggregation>(
      new LongLastValueAggregation(GetLastValuePointData(curr_data, next)));
}

PointType LongLastValueAggregation::ToPoint() const noexcept
{
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  return point_data_;
}

DoubleLastValueAggregation::DoubleLastValueAggregation()
{
  point_data_.is_lastvalue_valid_ = false;
  point_data_.value_              = 0.0;
}

DoubleLastValueAggregation::DoubleLastValueAggregation(const LastValuePointData &data)
    : point_data_{data}
{}

void DoubleLastValueAggregation::Aggregate(double value,
                                           const PointAttributes & /* attributes */) noexcept
{
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  point_data_.is_lastvalue_valid_ = true;
  point_data_.value_              = value;
  point_data_.sample_ts_          = std::chrono::system_clock::now();
}

std::unique_ptr<Aggregation> DoubleLastValueAggregation::Merge(
    const Aggregation &delta) const noexcept
{
  LastValuePointData curr_data;
  {
    const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
    curr_data = point_data_;
  }
  return std::unique_ptr<Aggregation>(
      new DoubleLastValueAggregation(GetLastValuePointData(curr_data, delta)));
}

std::unique_ptr<Aggregation> DoubleLastValueAggregation::Diff(
    const Aggregation &next) const noexcept
{
  LastValuePointData curr_data;
  {
    const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
    curr_data = point_data_;
  }
  return std::unique_ptr<Aggregation>(
      new DoubleLastValueAggregation(GetLastValuePointData(curr_data, next)));
}

PointType DoubleLastValueAggregation::ToPoint() const noexcept
{
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  return point_data_;
}
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
