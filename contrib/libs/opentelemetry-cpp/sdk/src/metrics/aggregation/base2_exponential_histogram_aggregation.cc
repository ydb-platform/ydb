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
  // Both indices have collapsed to -1 or 0 after 31 shifts, so further iterations cannot narrow
  // the span; the bound keeps a degenerate max_buckets from spinning forever.
  while (scale_reduction < 31 &&
         static_cast<int64_t>(end_index) - start_index + 1 > static_cast<int64_t>(max_buckets))
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
  if (low.Empty() && high.Empty())
  {
    return 0;
  }
  // The populated side alone can exceed a result sized for the narrower operand's budget.
  if (low.Empty() || high.Empty())
  {
    const AdaptingCircularBufferCounter &populated = low.Empty() ? high : low;
    return GetScaleReduction(populated.StartIndex(), populated.EndIndex(), max_buckets);
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

// Folds `high_res` onto `target_scale`. The bucket shift has to match the scale delta exactly, so
// the runtime floor is deliberately not applied here: it bounds the reductions the SDK chooses,
// not the alignment of an operand that already sits lower.
void AlignToScale(Base2ExponentialHistogramPointData &high_res, int32_t target_scale) noexcept
{
  if (high_res.scale_ <= target_scale)
  {
    return;
  }

  // AdaptingCircularBufferCounter::Downscale() saturates at 31, which is idempotent for int32_t
  // indices, so a larger delta needs no special handling.
  const int64_t delta = static_cast<int64_t>(high_res.scale_) - target_scale;
  const uint32_t by   = delta > 31 ? 31u : static_cast<uint32_t>(delta);

  if (high_res.positive_buckets_)
  {
    DownscaleBuckets(high_res.positive_buckets_, by);
  }
  if (high_res.negative_buckets_)
  {
    DownscaleBuckets(high_res.negative_buckets_, by);
  }
  high_res.scale_ = target_scale;
}

// Guards point data that arrives through the public constructors with a smaller budget than the
// configuration validator would ever produce. A configured max_size is at least kMaxSizeMin, so
// this never allocates more buckets than the user asked for.
size_t BucketCapacity(size_t max_buckets) noexcept
{
  return (std::max)(max_buckets, kMaxSizeMin);
}

// Point data handed to the public constructors carries buffers the caller sized, which can be
// narrower than the capacity this class guarantees; move the counts into a wide enough buffer.
void EnsureBucketCapacity(std::unique_ptr<AdaptingCircularBufferCounter> &buckets,
                          size_t capacity) noexcept
{
  if (!buckets)
  {
    buckets = std::make_unique<AdaptingCircularBufferCounter>(capacity);
    return;
  }

  if (buckets->MaxSize() >= capacity)
  {
    return;
  }

  auto widened = std::make_unique<AdaptingCircularBufferCounter>(capacity);
  if (!buckets->Empty())
  {
    for (int64_t index = buckets->StartIndex(); index <= buckets->EndIndex(); ++index)
    {
      const uint64_t count = buckets->Get(static_cast<int32_t>(index));
      if (count > 0 && !widened->Increment(static_cast<int32_t>(index), count))
      {
        OTEL_INTERNAL_LOG_ERROR(
            "[Base2ExponentialHistogramAggregation::EnsureBucketCapacity] bucket index "
            << index << " out of range; count " << count << " dropped. SDK invariant violation");
        assert(false && "EnsureBucketCapacity: bucket index out of range");
      }
    }
  }
  buckets = std::move(widened);
}

bool BucketIndicesMatchScale(const AdaptingCircularBufferCounter *buckets,
                             int32_t min_index,
                             int32_t max_index) noexcept
{
  return buckets == nullptr || buckets->Empty() ||
         (buckets->StartIndex() >= min_index && buckets->EndIndex() <= max_index);
}

void NormalizeSuppliedPointData(Base2ExponentialHistogramPointData &point_data) noexcept
{
  // Index bounds above kMaxScaleMax overflow int32_t; folding keeps every supplied count.
  AlignToScale(point_data, kMaxScaleMax);

  const size_t capacity = BucketCapacity(point_data.max_buckets_);
  const Base2ExponentialHistogramIndexer indexer(point_data.scale_);
  const int32_t min_index = indexer.ComputeIndex((std::numeric_limits<double>::denorm_min)());
  const int32_t max_index = indexer.ComputeIndex((std::numeric_limits<double>::max)());
  if (BucketIndicesMatchScale(point_data.positive_buckets_.get(), min_index, max_index) &&
      BucketIndicesMatchScale(point_data.negative_buckets_.get(), min_index, max_index))
  {
    EnsureBucketCapacity(point_data.positive_buckets_, capacity);
    EnsureBucketCapacity(point_data.negative_buckets_, capacity);
    return;
  }

  OTEL_INTERNAL_LOG_ERROR(
      "[Base2ExponentialHistogramAggregation] supplied bucket indexes do not match scale "
      << point_data.scale_ << "; point data discarded");
  point_data.sum_        = 0.0;
  point_data.min_        = (std::numeric_limits<double>::max)();
  point_data.max_        = (std::numeric_limits<double>::min)();
  point_data.count_      = 0;
  point_data.zero_count_ = 0;
  // Fresh counters rather than Clear(): a moved-in counter may be null.
  point_data.positive_buckets_ = std::make_unique<AdaptingCircularBufferCounter>(capacity);
  point_data.negative_buckets_ = std::make_unique<AdaptingCircularBufferCounter>(capacity);
}

// Truncates `requested` to the reduction that can be applied without pushing `current_scale` below
// kMinRuntimeScale. Returns 0 once the floor is reached.
uint32_t ClampScaleReduction(int32_t current_scale, uint32_t requested) noexcept
{
  const int64_t headroom = static_cast<int64_t>(current_scale) - kMinRuntimeScale;
  if (headroom <= 0)
  {
    return 0;
  }
  return static_cast<uint32_t>((std::min)(static_cast<int64_t>(requested), headroom));
}

// Single entry point for scale reduction: clamps to the runtime floor, folds both bucket arrays by
// the clamped amount and moves scale_ by exactly that amount. Returns the reduction applied.
uint32_t ApplyDownscale(Base2ExponentialHistogramPointData &point_data, uint32_t requested) noexcept
{
  const uint32_t applied = ClampScaleReduction(point_data.scale_, requested);
  if (applied == 0)
  {
    return 0;
  }

  if (point_data.positive_buckets_)
  {
    DownscaleBuckets(point_data.positive_buckets_, applied);
  }
  if (point_data.negative_buckets_)
  {
    DownscaleBuckets(point_data.negative_buckets_, applied);
  }
  point_data.scale_ -= static_cast<int32_t>(applied);
  return applied;
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
      std::make_unique<AdaptingCircularBufferCounter>(BucketCapacity(point_data_.max_buckets_));
  point_data_.negative_buckets_ =
      std::make_unique<AdaptingCircularBufferCounter>(BucketCapacity(point_data_.max_buckets_));

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

  NormalizeSuppliedPointData(point_data_);
  // Normalization may have folded the supplied scale.
  indexer_ = Base2ExponentialHistogramIndexer(point_data_.scale_);
}

Base2ExponentialHistogramAggregation::Base2ExponentialHistogramAggregation(
    Base2ExponentialHistogramPointData &&point_data)
    : point_data_{std::move(point_data)},
      indexer_(point_data_.scale_),
      record_min_max_{point_data_.record_min_max_}
{
  NormalizeSuppliedPointData(point_data_);
  // Normalization may have folded the supplied scale.
  indexer_ = Base2ExponentialHistogramIndexer(point_data_.scale_);
}

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
    buckets =
        std::make_unique<AdaptingCircularBufferCounter>(BucketCapacity(point_data_.max_buckets_));
  }

  if (buckets->MaxSize() == 0)
  {
    buckets =
        std::make_unique<AdaptingCircularBufferCounter>(BucketCapacity(point_data_.max_buckets_));
  }

  const int32_t index = indexer_.ComputeIndex(value);
  if (buckets->Increment(index, 1))
  {
    return;
  }

  // A configured max_size is never below kMaxSizeMin, so the buffer capacity equals max_buckets_
  // and the failure above already means the span exceeds the budget.
  const uint32_t scale_reduction =
      GetScaleReduction((std::min)(buckets->StartIndex(), index),
                        (std::max)(buckets->EndIndex(), index), point_data_.max_buckets_);

  // Downscale() may stop short of the request at the floor, so shift the index by what was
  // actually applied.
  const uint32_t applied = Downscale(scale_reduction);
  if (!buckets->Increment(index >> applied, 1))
  {
    OTEL_INTERNAL_LOG_ERROR(
        "[Base2ExponentialHistogramAggregation::AggregateIntoBuckets] bucket index "
        << (index >> applied) << " out of range; count dropped. SDK invariant violation");
    assert(false && "AggregateIntoBuckets: bucket index out of range");
  }
}

uint32_t Base2ExponentialHistogramAggregation::Downscale(uint32_t by) noexcept
{
  if (by == 0)
  {
    return 0;
  }

  const uint32_t applied = ApplyDownscale(point_data_, by);

  if (applied < by && !floor_warning_emitted_)
  {
    floor_warning_emitted_ = true;
    OTEL_INTERNAL_LOG_WARN("[Base2ExponentialHistogramAggregation] scale "
                           << point_data_.scale_ << " reached the runtime minimum "
                           << kMinRuntimeScale
                           << "; recorded values now share buckets instead of downscaling further");
  }

  if (applied == 0)
  {
    return 0;
  }

  indexer_ = Base2ExponentialHistogramIndexer(point_data_.scale_);
  return applied;
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
  result_value.max_buckets_ =
      low_res.max_buckets_ >= high_res.max_buckets_ ? low_res.max_buckets_ : high_res.max_buckets_;
  result_value.record_min_max_ = low_res.record_min_max_ && high_res.record_min_max_;

  if (result_value.record_min_max_)
  {
    result_value.min_ = (std::min)(low_res.min_, high_res.min_);
    result_value.max_ = (std::max)(low_res.max_, high_res.max_);
  }

  AlignToScale(high_res, low_res.scale_);

  // positive_buckets_ and negative_buckets_ share a single scale_; apply
  // the maximum required reduction across both bucket types.
  const uint32_t scale_reduction =
      (std::max)(GetScaleReductionForUnion(*low_res.positive_buckets_, *high_res.positive_buckets_,
                                           result_value.max_buckets_),
                 GetScaleReductionForUnion(*low_res.negative_buckets_, *high_res.negative_buckets_,
                                           result_value.max_buckets_));

  // Both operands share a scale after the alignment above, so one clamped amount applies to both.
  const uint32_t applied = ClampScaleReduction(low_res.scale_, scale_reduction);
  ApplyDownscale(low_res, applied);
  ApplyDownscale(high_res, applied);
  result_value.scale_ = low_res.scale_;

  result_value.positive_buckets_ = std::make_unique<AdaptingCircularBufferCounter>(
      MergeBuckets(BucketCapacity(result_value.max_buckets_), *low_res.positive_buckets_,
                   *high_res.positive_buckets_));
  result_value.negative_buckets_ = std::make_unique<AdaptingCircularBufferCounter>(
      MergeBuckets(BucketCapacity(result_value.max_buckets_), *low_res.negative_buckets_,
                   *high_res.negative_buckets_));

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

  AlignToScale(high_res, low_res.scale_);

  // positive_buckets_ and negative_buckets_ share a single scale_; apply
  // the maximum required reduction across both bucket types.
  const uint32_t scale_reduction =
      (std::max)(GetScaleReductionForUnion(*low_res.positive_buckets_, *high_res.positive_buckets_,
                                           low_res.max_buckets_),
                 GetScaleReductionForUnion(*low_res.negative_buckets_, *high_res.negative_buckets_,
                                           low_res.max_buckets_));

  // Both operands share a scale after the alignment above, so one clamped amount applies to both.
  const uint32_t applied = ClampScaleReduction(low_res.scale_, scale_reduction);
  ApplyDownscale(low_res, applied);
  ApplyDownscale(high_res, applied);

  Base2ExponentialHistogramPointData result_value;
  result_value.scale_          = low_res.scale_;
  result_value.max_buckets_    = low_res.max_buckets_;
  result_value.record_min_max_ = false;
  result_value.count_          = (right.count_ >= left.count_) ? (right.count_ - left.count_) : 0;
  result_value.sum_            = (right.sum_ >= left.sum_) ? (right.sum_ - left.sum_) : 0.0;
  result_value.zero_count_ =
      (right.zero_count_ >= left.zero_count_) ? (right.zero_count_ - left.zero_count_) : 0;

  result_value.positive_buckets_ =
      std::make_unique<AdaptingCircularBufferCounter>(BucketCapacity(result_value.max_buckets_));
  result_value.negative_buckets_ =
      std::make_unique<AdaptingCircularBufferCounter>(BucketCapacity(result_value.max_buckets_));

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
