// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <utility>

#include "opentelemetry/common/spin_lock_mutex.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation.h"
#include "opentelemetry/sdk/metrics/aggregation/aggregation_config.h"
#include "opentelemetry/sdk/metrics/aggregation/base2_exponential_histogram_aggregation.h"
#include "opentelemetry/sdk/metrics/aggregation/base2_exponential_histogram_indexer.h"
#include "opentelemetry/sdk/metrics/data/circular_buffer.h"
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

uint32_t GetScaleReduction(int32_t start_index, int32_t end_index, size_t max_buckets) noexcept
{
  uint32_t scale_reduction = 0;
  while (static_cast<int64_t>(end_index) - start_index + 1 > static_cast<int64_t>(max_buckets))
  {
    start_index >>= 1;
    end_index >>= 1;
    scale_reduction++;
  }
  return scale_reduction;
}

uint32_t GetScaleReductionForUnion(const AdaptingCircularBufferCounter &low,
                                   const AdaptingCircularBufferCounter &high,
                                   size_t max_buckets) noexcept
{
  if (low.Empty() || high.Empty())
  {
    return 0;
  }
  return GetScaleReduction((std::min)(low.StartIndex(), high.StartIndex()),
                           (std::max)(low.EndIndex(), high.EndIndex()), max_buckets);
}

void DiffBuckets(const AdaptingCircularBufferCounter &left,
                 const AdaptingCircularBufferCounter &right,
                 AdaptingCircularBufferCounter &out) noexcept
{
  if (left.Empty() && right.Empty())
  {
    return;
  }
  const int32_t min_index = left.Empty()    ? right.StartIndex()
                            : right.Empty() ? left.StartIndex()
                                            : (std::min)(left.StartIndex(), right.StartIndex());
  const int32_t max_index = left.Empty()    ? right.EndIndex()
                            : right.Empty() ? left.EndIndex()
                                            : (std::max)(left.EndIndex(), right.EndIndex());
  for (int32_t i = min_index; i <= max_index; ++i)
  {
    const uint64_t l_cnt = left.Get(i);
    const uint64_t r_cnt = right.Get(i);
    if (r_cnt > l_cnt)
    {
      if (!out.Increment(i, r_cnt - l_cnt))
      {
        OTEL_INTERNAL_LOG_ERROR("[Base2ExponentialHistogramAggregation::DiffBuckets] bucket index "
                                << i << " out of range; count " << (r_cnt - l_cnt)
                                << " dropped. SDK invariant violation");
        assert(false && "DiffBuckets: bucket index out of range");
      }
    }
  }
}

void DownscaleBuckets(std::unique_ptr<AdaptingCircularBufferCounter> &buckets, uint32_t by) noexcept
{
  // Downscaling is done in place, which preserves the existing bucket size
  // optimisation (integer width) and avoids allocating a replacement buffer.
  buckets->Downscale(by);
}

}  // namespace

Base2ExponentialHistogramAggregation::Base2ExponentialHistogramAggregation(
    const AggregationConfig *aggregation_config)
{
  const Base2ExponentialHistogramAggregationConfig default_config;
  auto ac = static_cast<const Base2ExponentialHistogramAggregationConfig *>(aggregation_config);
  if (!ac)
  {
    ac = &default_config;
  }

  size_t max_size = ac->max_size_;
  if (max_size < kMaxSizeMin)
  {
    OTEL_INTERNAL_LOG_WARN("[Base2ExponentialHistogramAggregation] max_size "
                           << max_size << " is less than " << kMaxSizeMin << "; using default "
                           << default_config.max_size_);
    max_size = default_config.max_size_;
  }

  int32_t max_scale = ac->max_scale_;
  if (max_scale < kMaxScaleMin || max_scale > kMaxScaleMax)
  {
    OTEL_INTERNAL_LOG_WARN("[Base2ExponentialHistogramAggregation] max_scale "
                           << max_scale << " is out of range [" << kMaxScaleMin << ", "
                           << kMaxScaleMax << "]; using default " << default_config.max_scale_);
    max_scale = default_config.max_scale_;
  }

  point_data_.max_buckets_    = max_size;
  point_data_.scale_          = max_scale;
  point_data_.record_min_max_ = ac->record_min_max_;
  point_data_.min_            = (std::numeric_limits<double>::max)();
  point_data_.max_            = (std::numeric_limits<double>::min)();

  // Initialize buckets
  point_data_.positive_buckets_ =
      std::make_unique<AdaptingCircularBufferCounter>(point_data_.max_buckets_);
  point_data_.negative_buckets_ =
      std::make_unique<AdaptingCircularBufferCounter>(point_data_.max_buckets_);

  indexer_ = Base2ExponentialHistogramIndexer(point_data_.scale_);
}

Base2ExponentialHistogramAggregation::Base2ExponentialHistogramAggregation(
    const Base2ExponentialHistogramPointData &point_data)
    : point_data_{}, indexer_(point_data.scale_), record_min_max_{point_data.record_min_max_}
{
  point_data_.sum_            = point_data.sum_;
  point_data_.min_            = point_data.min_;
  point_data_.max_            = point_data.max_;
  point_data_.zero_threshold_ = point_data.zero_threshold_;
  point_data_.count_          = point_data.count_;
  point_data_.zero_count_     = point_data.zero_count_;
  point_data_.max_buckets_    = point_data.max_buckets_;
  point_data_.scale_          = point_data.scale_;
  point_data_.record_min_max_ = point_data.record_min_max_;

  // Deep copy the unique_ptr members
  if (point_data.positive_buckets_)
  {
    point_data_.positive_buckets_ =
        std::make_unique<AdaptingCircularBufferCounter>(*point_data.positive_buckets_);
  }
  if (point_data.negative_buckets_)
  {
    point_data_.negative_buckets_ =
        std::make_unique<AdaptingCircularBufferCounter>(*point_data.negative_buckets_);
  }
}

Base2ExponentialHistogramAggregation::Base2ExponentialHistogramAggregation(
    Base2ExponentialHistogramPointData &&point_data)
    : point_data_{std::move(point_data)},
      indexer_(point_data_.scale_),
      record_min_max_{point_data_.record_min_max_}
{}

void Base2ExponentialHistogramAggregation::Aggregate(
    int64_t value,
    const PointAttributes & /* attributes */) noexcept
{
  Aggregate(static_cast<double>(value));
}

void Base2ExponentialHistogramAggregation::Aggregate(
    double value,
    const PointAttributes & /* attributes */) noexcept
{
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
  point_data_.sum_ += value;
  point_data_.count_++;

  if (record_min_max_)
  {
    point_data_.min_ = (std::min)(point_data_.min_, value);
    point_data_.max_ = (std::max)(point_data_.max_, value);
  }

  if (value == 0)
  {
    point_data_.zero_count_++;
    return;
  }
  else if (value > 0)
  {
    if (point_data_.positive_buckets_)
    {
      AggregateIntoBuckets(point_data_.positive_buckets_, value);
    }
  }
  else
  {
    if (point_data_.negative_buckets_)
    {
      AggregateIntoBuckets(point_data_.negative_buckets_, -value);
    }
  }
}

void Base2ExponentialHistogramAggregation::AggregateIntoBuckets(
    std::unique_ptr<AdaptingCircularBufferCounter> &buckets,
    double value) noexcept
{
  if (!buckets)
  {
    buckets = std::make_unique<AdaptingCircularBufferCounter>(point_data_.max_buckets_);
  }

  if (buckets->MaxSize() == 0)
  {
    buckets = std::make_unique<AdaptingCircularBufferCounter>(point_data_.max_buckets_);
  }

  const int32_t index = indexer_.ComputeIndex(value);
  if (!buckets->Increment(index, 1))
  {
    const int32_t start_index = (std::min)(buckets->StartIndex(), index);
    const int32_t end_index   = (std::max)(buckets->EndIndex(), index);
    const uint32_t scale_reduction =
        GetScaleReduction(start_index, end_index, point_data_.max_buckets_);
    Downscale(scale_reduction);

    buckets->Increment(index >> scale_reduction, 1);
  }
}

void Base2ExponentialHistogramAggregation::Downscale(uint32_t by) noexcept
{
  if (by == 0)
  {
    return;
  }

  if (point_data_.positive_buckets_)
  {
    DownscaleBuckets(point_data_.positive_buckets_, by);
  }
  if (point_data_.negative_buckets_)
  {
    DownscaleBuckets(point_data_.negative_buckets_, by);
  }

  point_data_.scale_ -= static_cast<int32_t>(by);
  indexer_ = Base2ExponentialHistogramIndexer(point_data_.scale_);
}

// Merge A and B into a new circular buffer C.
// Caller must ensure that A and B are used as buckets at the same scale.
static AdaptingCircularBufferCounter MergeBuckets(size_t max_buckets,
                                                  const AdaptingCircularBufferCounter &A,
                                                  const AdaptingCircularBufferCounter &B)
{
  AdaptingCircularBufferCounter C = AdaptingCircularBufferCounter(max_buckets);
  C.Clear();

  if (A.Empty() && B.Empty())
  {
    return C;
  }
  if (A.Empty())
  {
    return B;
  }
  if (B.Empty())
  {
    return A;
  }

  auto min_index = (std::min)(A.StartIndex(), B.StartIndex());
  auto max_index = (std::max)(A.EndIndex(), B.EndIndex());

  for (int i = min_index; i <= max_index; i++)
  {
    auto count = A.Get(i) + B.Get(i);
    if (count > 0)
    {
      if (!C.Increment(i, count))
      {
        OTEL_INTERNAL_LOG_ERROR("[Base2ExponentialHistogramAggregation::MergeBuckets] bucket index "
                                << i << " out of range; count " << count
                                << " dropped. SDK invariant violation");
        assert(false && "MergeBuckets: bucket index out of range");
      }
    }
  }

  return C;
}

std::unique_ptr<Aggregation> Base2ExponentialHistogramAggregation::Merge(
    const Aggregation &delta) const noexcept
{
  auto left  = nostd::get<Base2ExponentialHistogramPointData>(ToPoint());
  auto right = nostd::get<Base2ExponentialHistogramPointData>(
      (static_cast<const Base2ExponentialHistogramAggregation &>(delta).ToPoint()));

  if (left.count_ == 0)
  {
    return std::make_unique<Base2ExponentialHistogramAggregation>(std::move(right));
  }

  if (right.count_ == 0)
  {
    return std::make_unique<Base2ExponentialHistogramAggregation>(std::move(left));
  }

  auto &low_res  = left.scale_ < right.scale_ ? left : right;
  auto &high_res = left.scale_ < right.scale_ ? right : left;

  Base2ExponentialHistogramPointData result_value;
  result_value.count_      = low_res.count_ + high_res.count_;
  result_value.sum_        = low_res.sum_ + high_res.sum_;
  result_value.zero_count_ = low_res.zero_count_ + high_res.zero_count_;
  result_value.scale_      = (std::min)(low_res.scale_, high_res.scale_);
  result_value.max_buckets_ =
      low_res.max_buckets_ >= high_res.max_buckets_ ? low_res.max_buckets_ : high_res.max_buckets_;
  result_value.record_min_max_ = low_res.record_min_max_ && high_res.record_min_max_;

  if (result_value.record_min_max_)
  {
    result_value.min_ = (std::min)(low_res.min_, high_res.min_);
    result_value.max_ = (std::max)(low_res.max_, high_res.max_);
  }

  {
    auto scale_reduction = high_res.scale_ - low_res.scale_;

    if (scale_reduction > 0)
    {
      DownscaleBuckets(high_res.positive_buckets_, scale_reduction);
      DownscaleBuckets(high_res.negative_buckets_, scale_reduction);
      high_res.scale_ -= scale_reduction;
    }
  }

  // positive_buckets_ and negative_buckets_ share a single scale_; apply
  // the maximum required reduction across both bucket types.
  const uint32_t scale_reduction =
      (std::max)(GetScaleReductionForUnion(*low_res.positive_buckets_, *high_res.positive_buckets_,
                                           result_value.max_buckets_),
                 GetScaleReductionForUnion(*low_res.negative_buckets_, *high_res.negative_buckets_,
                                           result_value.max_buckets_));

  if (scale_reduction > 0)
  {
    DownscaleBuckets(low_res.positive_buckets_, scale_reduction);
    DownscaleBuckets(high_res.positive_buckets_, scale_reduction);
    DownscaleBuckets(low_res.negative_buckets_, scale_reduction);
    DownscaleBuckets(high_res.negative_buckets_, scale_reduction);
    low_res.scale_ -= static_cast<int32_t>(scale_reduction);
    high_res.scale_ -= static_cast<int32_t>(scale_reduction);
    result_value.scale_ -= static_cast<int32_t>(scale_reduction);
  }

  result_value.positive_buckets_ = std::make_unique<AdaptingCircularBufferCounter>(MergeBuckets(
      result_value.max_buckets_, *low_res.positive_buckets_, *high_res.positive_buckets_));
  result_value.negative_buckets_ = std::make_unique<AdaptingCircularBufferCounter>(MergeBuckets(
      result_value.max_buckets_, *low_res.negative_buckets_, *high_res.negative_buckets_));

  return std::unique_ptr<Base2ExponentialHistogramAggregation>{
      new Base2ExponentialHistogramAggregation(std::move(result_value))};
}

std::unique_ptr<Aggregation> Base2ExponentialHistogramAggregation::Diff(
    const Aggregation &next) const noexcept
{
  auto left  = nostd::get<Base2ExponentialHistogramPointData>(ToPoint());
  auto right = nostd::get<Base2ExponentialHistogramPointData>(
      (static_cast<const Base2ExponentialHistogramAggregation &>(next).ToPoint()));

  auto &low_res  = left.scale_ < right.scale_ ? left : right;
  auto &high_res = left.scale_ < right.scale_ ? right : left;

  {
    const auto scale_reduction = high_res.scale_ - low_res.scale_;

    if (scale_reduction > 0)
    {
      if (high_res.positive_buckets_)
      {
        DownscaleBuckets(high_res.positive_buckets_, scale_reduction);
      }

      if (high_res.negative_buckets_)
      {
        DownscaleBuckets(high_res.negative_buckets_, scale_reduction);
      }

      high_res.scale_ -= scale_reduction;
    }
  }

  // positive_buckets_ and negative_buckets_ share a single scale_; apply
  // the maximum required reduction across both bucket types.
  const uint32_t scale_reduction =
      (std::max)(GetScaleReductionForUnion(*low_res.positive_buckets_, *high_res.positive_buckets_,
                                           low_res.max_buckets_),
                 GetScaleReductionForUnion(*low_res.negative_buckets_, *high_res.negative_buckets_,
                                           low_res.max_buckets_));

  if (scale_reduction > 0)
  {
    DownscaleBuckets(low_res.positive_buckets_, scale_reduction);
    DownscaleBuckets(high_res.positive_buckets_, scale_reduction);
    DownscaleBuckets(low_res.negative_buckets_, scale_reduction);
    DownscaleBuckets(high_res.negative_buckets_, scale_reduction);
    low_res.scale_ -= static_cast<int32_t>(scale_reduction);
    high_res.scale_ -= static_cast<int32_t>(scale_reduction);
  }

  Base2ExponentialHistogramPointData result_value;
  result_value.scale_          = low_res.scale_;
  result_value.max_buckets_    = low_res.max_buckets_;
  result_value.record_min_max_ = false;
  result_value.count_          = (right.count_ >= left.count_) ? (right.count_ - left.count_) : 0;
  result_value.sum_            = (right.sum_ >= left.sum_) ? (right.sum_ - left.sum_) : 0.0;
  result_value.zero_count_ =
      (right.zero_count_ >= left.zero_count_) ? (right.zero_count_ - left.zero_count_) : 0;

  result_value.positive_buckets_ =
      std::make_unique<AdaptingCircularBufferCounter>(right.max_buckets_);
  result_value.negative_buckets_ =
      std::make_unique<AdaptingCircularBufferCounter>(right.max_buckets_);

  if (!left.positive_buckets_->Empty() || !right.positive_buckets_->Empty())
  {
    DiffBuckets(*left.positive_buckets_, *right.positive_buckets_, *result_value.positive_buckets_);
  }

  if (!left.negative_buckets_->Empty() || !right.negative_buckets_->Empty())
  {
    DiffBuckets(*left.negative_buckets_, *right.negative_buckets_, *result_value.negative_buckets_);
  }

  return std::unique_ptr<Base2ExponentialHistogramAggregation>{
      new Base2ExponentialHistogramAggregation(std::move(result_value))};
}

PointType Base2ExponentialHistogramAggregation::ToPoint() const noexcept
{
  const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);

  Base2ExponentialHistogramPointData copy;
  copy.sum_            = point_data_.sum_;
  copy.min_            = point_data_.min_;
  copy.max_            = point_data_.max_;
  copy.zero_threshold_ = point_data_.zero_threshold_;
  copy.count_          = point_data_.count_;
  copy.zero_count_     = point_data_.zero_count_;
  copy.max_buckets_    = point_data_.max_buckets_;
  copy.scale_          = point_data_.scale_;
  copy.record_min_max_ = point_data_.record_min_max_;

  if (point_data_.positive_buckets_)
  {
    copy.positive_buckets_ =
        std::make_unique<AdaptingCircularBufferCounter>(*point_data_.positive_buckets_);
  }
  if (point_data_.negative_buckets_)
  {
    copy.negative_buckets_ =
        std::make_unique<AdaptingCircularBufferCounter>(*point_data_.negative_buckets_);
  }

  return copy;
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
