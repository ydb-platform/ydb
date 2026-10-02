// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/sdk/metrics/data/circular_buffer.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

namespace
{

struct AdaptingIntegerArrayIncrement
{
  size_t index;
  uint64_t count;

  template <typename T>
  uint64_t operator()(std::vector<T> &backing)
  {
    const uint64_t result = backing[index] + count;
    if OPENTELEMETRY_LIKELY_CONDITION (result <= uint64_t(std::numeric_limits<T>::max()))
    {
      backing[index] = static_cast<T>(result);
      return 0;
    }
    return result;
  }
};

struct AdaptingIntegerArrayGet
{
  size_t index;

  template <typename T>
  uint64_t operator()(const std::vector<T> &backing)
  {
    return backing[index];
  }
};

struct AdaptingIntegerArraySize
{
  template <typename T>
  size_t operator()(const std::vector<T> &backing)
  {
    return backing.size();
  }
};

struct AdaptingIntegerArrayClear
{
  template <typename T>
  void operator()(std::vector<T> &backing)
  {
    backing.assign(backing.size(), static_cast<T>(0));
  }
};

struct AdaptingIntegerArrayFold
{
  size_t from;
  size_t to;

  template <typename T>
  uint64_t operator()(std::vector<T> &backing)
  {
    const uint64_t count = backing[from];
    if (count == 0)
    {
      return 0;
    }

    const uint64_t result = backing[to] + count;
    if OPENTELEMETRY_LIKELY_CONDITION (result <= uint64_t(std::numeric_limits<T>::max()))
    {
      backing[to]   = static_cast<T>(result);
      backing[from] = static_cast<T>(0);
      return 0;
    }
    // Leaves the source untouched so the retry after widening sees it again.
    return result;
  }
};

struct AdaptingIntegerArrayCopy
{
  template <class T1, class T2>
  void operator()(const std::vector<T1> &from, std::vector<T2> &to)
  {
    for (size_t i = 0; i < from.size(); i++)
    {
      to[i] = static_cast<T2>(from[i]);
    }
  }
};

}  // namespace

void AdaptingIntegerArray::Increment(size_t index, uint64_t count)
{
  /* May or may not fit */
  const uint64_t result = nostd::visit(AdaptingIntegerArrayIncrement{index, count}, backing_);
  if OPENTELEMETRY_LIKELY_CONDITION (result == 0)
  {
    return;
  }
  EnlargeToFit(result);
  /* Must fit, buffer was enlarged for the value to store */
  OPENTELEMETRY_MAYBE_UNUSED const uint64_t result2 =
      nostd::visit(AdaptingIntegerArrayIncrement{index, count}, backing_);
  assert(result2 == 0);
}

uint64_t AdaptingIntegerArray::Get(size_t index) const
{
  return nostd::visit(AdaptingIntegerArrayGet{index}, backing_);
}

size_t AdaptingIntegerArray::Size() const
{
  return nostd::visit(AdaptingIntegerArraySize{}, backing_);
}

void AdaptingIntegerArray::Clear()
{
  nostd::visit(AdaptingIntegerArrayClear{}, backing_);
}

void AdaptingIntegerArray::Fold(size_t from, size_t to)
{
  if (from == to)
  {
    return;
  }

  /* May or may not fit */
  const uint64_t result = nostd::visit(AdaptingIntegerArrayFold{from, to}, backing_);
  if OPENTELEMETRY_LIKELY_CONDITION (result == 0)
  {
    return;
  }
  EnlargeToFit(result);
  /* Must fit, buffer was enlarged for the value to store */
  OPENTELEMETRY_MAYBE_UNUSED const uint64_t result2 =
      nostd::visit(AdaptingIntegerArrayFold{from, to}, backing_);
  assert(result2 == 0);
}

void AdaptingIntegerArray::EnlargeToFit(uint64_t value)
{
  const size_t backing_size = Size();
  decltype(backing_) backing;
  if (value <= std::numeric_limits<uint16_t>::max())
  {
    backing = std::vector<uint16_t>(backing_size, 0);
  }
  else if (value <= std::numeric_limits<uint32_t>::max())
  {
    backing = std::vector<uint32_t>(backing_size, 0);
  }
  else
  {
    backing = std::vector<uint64_t>(backing_size, 0);
  }
  nostd::visit(AdaptingIntegerArrayCopy{}, backing_, backing);
  backing_ = std::move(backing);
}

void AdaptingCircularBufferCounter::Clear()
{
  start_index_ = kNullIndex;
  end_index_   = kNullIndex;
  base_index_  = kNullIndex;
  backing_.Clear();
}

bool AdaptingCircularBufferCounter::Increment(int32_t index, uint64_t delta)
{
  if (Empty())
  {
    start_index_ = index;
    end_index_   = index;
    base_index_  = index;
    backing_.Increment(0, delta);
    return true;
  }

  if (index > end_index_)
  {
    // Move end, check max size.
    if (static_cast<int64_t>(index) - start_index_ + 1 > static_cast<int64_t>(backing_.Size()))
    {
      return false;
    }
    end_index_ = index;
  }
  else if (index < start_index_)
  {
    // Move end, check max size.
    if (static_cast<int64_t>(end_index_) - index + 1 > static_cast<int64_t>(backing_.Size()))
    {
      return false;
    }
    start_index_ = index;
  }
  backing_.Increment(ToBufferIndex(index), delta);
  return true;
}

uint64_t AdaptingCircularBufferCounter::Get(int32_t index) const
{
  if (index < start_index_ || index > end_index_)
  {
    return 0;
  }
  return backing_.Get(ToBufferIndex(index));
}

void AdaptingCircularBufferCounter::Downscale(uint32_t by)
{
  if (by == 0 || Empty())
  {
    return;
  }

  // Right shifting a negative index floors towards minus infinity on every
  // supported compiler, which is exactly the mapping the caller applies to the
  // indices it computes for the reduced scale.
  constexpr uint32_t kMaxShift = static_cast<uint32_t>(std::numeric_limits<int32_t>::digits);
  const uint32_t shift         = by > kMaxShift ? kMaxShift : by;
  const int32_t new_base_index = base_index_ >> shift;

  // Buckets [base_index_, end_index_] sit at the front of the backing array in
  // ascending slot order. Folding never moves one of them to a higher slot, so
  // walking up means every destination slot has already been consumed.
  for (int64_t index = base_index_; index <= end_index_; ++index)
  {
    FoldBucket(static_cast<int32_t>(index), new_base_index, shift);
  }

  // Buckets [start_index_, base_index_) sit at the tail of the backing array,
  // again in ascending slot order. Folding never moves one of them to a lower
  // slot, so this half is walked down instead. Any of them that folds into the
  // new base index lands in the front half, which is already fully consumed.
  for (int64_t index = static_cast<int64_t>(base_index_) - 1; index >= start_index_; --index)
  {
    FoldBucket(static_cast<int32_t>(index), new_base_index, shift);
  }

  start_index_ = start_index_ >> shift;
  end_index_   = end_index_ >> shift;
  base_index_  = new_base_index;
}

void AdaptingCircularBufferCounter::FoldBucket(int32_t index,
                                               int32_t new_base_index,
                                               uint32_t shift)
{
  // The destination is expressed against the post-downscale base index, which
  // cannot be installed yet because the remaining slots still hold buckets of
  // the current scale. Downscale() guarantees that destination slot has already
  // been read, or is the source slot itself.
  backing_.Fold(ToBufferIndex(index), ToBufferIndex(index >> shift, new_base_index));
}

size_t AdaptingCircularBufferCounter::ToBufferIndex(int32_t index) const
{
  return ToBufferIndex(index, base_index_);
}

size_t AdaptingCircularBufferCounter::ToBufferIndex(int32_t index, int32_t base_index) const
{
  // Figure out the index relative to the start of the circular buffer.
  if (index < base_index)
  {
    // If index is before the base one, wrap around.
    return static_cast<size_t>(index + backing_.Size() - base_index);
  }
  return static_cast<size_t>(index - base_index);
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
