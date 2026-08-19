// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <stdint.h>
#include <memory>
#include <mutex>
#include <ostream>

#include "opentelemetry/common/spin_lock_mutex.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation.h"
#include "opentelemetry/sdk/metrics/aggregation/sum_aggregation.h"
#include "opentelemetry/sdk/metrics/data/metric_data.h"
#include "opentelemetry/sdk/metrics/data/point_data.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

LongSumAggregation::LongSumAggregation(bool is_monotonic)
{
  point_data_.value_        = static_cast<int64_t>(0);
  point_data_.is_monotonic_ = is_monotonic;
}

LongSumAggregation::LongSumAggregation(const SumPointData &data) : point_data_{data} {}

void LongSumAggregation::Aggregate(int64_t value, const PointAttributes & /* attributes */) noexcept
{
  if (point_data_.is_monotonic_ && value < 0)
  {
    OTEL_INTERNAL_LOG_WARN(
        " LongSumAggregation::Aggregate Negative value ignored for Monotonic increasing "
        "measurement. Value"
        << value);
    return;
  }
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  point_data_.value_ = nostd::get<int64_t>(point_data_.value_) + value;
}

std::unique_ptr<Aggregation> LongSumAggregation::Merge(const Aggregation &delta) const noexcept
{
  auto delta_point = static_cast<const LongSumAggregation &>(delta).ToPoint();
  auto curr_point  = ToPoint();

  const auto *delta_sum = nostd::get_if<SumPointData>(&delta_point);
  const auto *curr_sum  = nostd::get_if<SumPointData>(&curr_point);
  if (delta_sum == nullptr || curr_sum == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("LongSumAggregation::Merge - Loss of type");
    return std::unique_ptr<Aggregation>(new LongSumAggregation(point_data_.is_monotonic_));
  }

  const auto *delta_val = nostd::get_if<int64_t>(&delta_sum->value_);
  const auto *curr_val  = nostd::get_if<int64_t>(&curr_sum->value_);
  if (delta_val == nullptr || curr_val == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("LongSumAggregation::Merge - Loss of type");
    return std::unique_ptr<Aggregation>(new LongSumAggregation(point_data_.is_monotonic_));
  }

  std::unique_ptr<Aggregation> aggr(new LongSumAggregation(point_data_.is_monotonic_));
  static_cast<LongSumAggregation *>(aggr.get())->point_data_.value_ = *delta_val + *curr_val;
  return aggr;
}

std::unique_ptr<Aggregation> LongSumAggregation::Diff(const Aggregation &next) const noexcept
{
  auto next_point = static_cast<const LongSumAggregation &>(next).ToPoint();
  auto curr_point = ToPoint();

  const auto *next_sum = nostd::get_if<SumPointData>(&next_point);
  const auto *curr_sum = nostd::get_if<SumPointData>(&curr_point);
  if (next_sum == nullptr || curr_sum == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("LongSumAggregation::Diff - Loss of type");
    return std::unique_ptr<Aggregation>(new LongSumAggregation(point_data_.is_monotonic_));
  }

  const auto *next_val = nostd::get_if<int64_t>(&next_sum->value_);
  const auto *curr_val = nostd::get_if<int64_t>(&curr_sum->value_);
  if (next_val == nullptr || curr_val == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("LongSumAggregation::Diff - Loss of type");
    return std::unique_ptr<Aggregation>(new LongSumAggregation(point_data_.is_monotonic_));
  }

  std::unique_ptr<Aggregation> aggr(new LongSumAggregation(point_data_.is_monotonic_));
  static_cast<LongSumAggregation *>(aggr.get())->point_data_.value_ = *next_val - *curr_val;
  return aggr;
}

PointType LongSumAggregation::ToPoint() const noexcept
{
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  return point_data_;
}

DoubleSumAggregation::DoubleSumAggregation(bool is_monotonic)
{
  point_data_.value_        = 0.0;
  point_data_.is_monotonic_ = is_monotonic;
}

DoubleSumAggregation::DoubleSumAggregation(const SumPointData &data) : point_data_(data) {}

void DoubleSumAggregation::Aggregate(double value,
                                     const PointAttributes & /* attributes */) noexcept
{
  if (point_data_.is_monotonic_ && value < 0)
  {
    OTEL_INTERNAL_LOG_WARN(
        " DoubleSumAggregation::Aggregate Negative value ignored for Monotonic increasing "
        "measurement. Value"
        << value);
    return;
  }
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  point_data_.value_ = nostd::get<double>(point_data_.value_) + value;
}

std::unique_ptr<Aggregation> DoubleSumAggregation::Merge(const Aggregation &delta) const noexcept
{
  auto delta_point = static_cast<const DoubleSumAggregation &>(delta).ToPoint();
  auto curr_point  = ToPoint();

  const auto *delta_sum = nostd::get_if<SumPointData>(&delta_point);
  const auto *curr_sum  = nostd::get_if<SumPointData>(&curr_point);
  if (delta_sum == nullptr || curr_sum == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("DoubleSumAggregation::Merge - Loss of type");
    return std::unique_ptr<Aggregation>(new DoubleSumAggregation(point_data_.is_monotonic_));
  }

  const auto *delta_val = nostd::get_if<double>(&delta_sum->value_);
  const auto *curr_val  = nostd::get_if<double>(&curr_sum->value_);
  if (delta_val == nullptr || curr_val == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("DoubleSumAggregation::Merge - Loss of type");
    return std::unique_ptr<Aggregation>(new DoubleSumAggregation(point_data_.is_monotonic_));
  }

  std::unique_ptr<Aggregation> aggr(new DoubleSumAggregation(point_data_.is_monotonic_));
  static_cast<DoubleSumAggregation *>(aggr.get())->point_data_.value_ = *delta_val + *curr_val;
  return aggr;
}

std::unique_ptr<Aggregation> DoubleSumAggregation::Diff(const Aggregation &next) const noexcept
{
  auto next_point = static_cast<const DoubleSumAggregation &>(next).ToPoint();
  auto curr_point = ToPoint();

  const auto *next_sum = nostd::get_if<SumPointData>(&next_point);
  const auto *curr_sum = nostd::get_if<SumPointData>(&curr_point);
  if (next_sum == nullptr || curr_sum == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("DoubleSumAggregation::Diff - Loss of type");
    return std::unique_ptr<Aggregation>(new DoubleSumAggregation(point_data_.is_monotonic_));
  }

  const auto *next_val = nostd::get_if<double>(&next_sum->value_);
  const auto *curr_val = nostd::get_if<double>(&curr_sum->value_);
  if (next_val == nullptr || curr_val == nullptr)
  {
    OTEL_INTERNAL_LOG_ERROR("DoubleSumAggregation::Diff - Loss of type");
    return std::unique_ptr<Aggregation>(new DoubleSumAggregation(point_data_.is_monotonic_));
  }

  std::unique_ptr<Aggregation> aggr(new DoubleSumAggregation(point_data_.is_monotonic_));
  static_cast<DoubleSumAggregation *>(aggr.get())->point_data_.value_ = *next_val - *curr_val;
  return aggr;
}

PointType DoubleSumAggregation::ToPoint() const noexcept
{
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  return point_data_;
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
