#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/types.h>

#include <cmath>

namespace NKikimr {
namespace NOptimizerHistograms {
// Helper functions to work with histogram values.
template <typename T>
T LoadFrom(const ui8 *storage) {
  T val;
  std::memcpy(&val, storage, sizeof(T));
  return val;
}
template <typename T>
void StoreTo(ui8 *storage, T value) {
  std::memcpy(storage, &value, sizeof(T));
}
template <typename T>
static bool CmpEqual(T a, T b) {
  return a == b;
}
template <>
[[maybe_unused]] bool CmpEqual(double a, double b) {
  return std::fabs(a - b) < std::numeric_limits<double>::epsilon();
}
template <typename T>
bool CmpLess(T a, T b) {
  return a < b;
}

// Represents value types supported by histogram.
enum class EHistogramValueType : ui8 { Int32, Int64, Uint32, Uint64, Double, NotSupported };

// This class represents an `Equal-width` histogram.
// Each bucket represents a range of contiguous values of equal width, and the
// aggregate summary stored in the bucket is the number of rows whose value lies
// within that range.
class TEqWidthHistogram {
 public:
  struct TBucket {
    // The number of values in a bucket.
    ui64 count{0};
    // The `start` value of a bucket, the `end` of the bucket is a next start.
    // [start = start[i], end = start[i + 1])
    ui8 start[8];
  };
  struct TBucketRange {
    ui8 start[8];
    ui8 end[8];
  };

  // Have to specify the number of buckets and type of the values.
  TEqWidthHistogram(ui32 numBuckets = 1, EHistogramValueType type = EHistogramValueType::Int32);
  // From serialized data.
  TEqWidthHistogram(const char *str, ui64 size);

  // Adds the given `val` to a histogram.
  template <typename T>
  void AddElement(T val) {
    const auto index = FindBucketIndex(val);
    // The given `index` in range [0, numBuckets - 1].
    const T bucketValue = LoadFrom<T>(buckets[index].start);
    if (!index || ((CmpEqual<T>(bucketValue, val) || CmpLess<T>(bucketValue, val)))) {
      buckets[index].count++;
    } else {
      buckets[index - 1].count++;
    }
  }

  // Returns an index of the bucket which stores the given `val`.
  // Returned index in range [0, numBuckets - 1].
  // Not using `std::lower_bound()` here because need an index to map to `suffix` and `prefix` sum.
  template <typename T>
  ui32 FindBucketIndex(T val) const {
    ui32 start = 0;
    ui32 end = GetNumBuckets() - 1;
    while (start < end) {
      auto it = start + (end - start) / 2;
      if (CmpLess<T>(LoadFrom<T>(buckets[it].start), val)) {
        start = it + 1;
      } else {
        end = it;
      }
    }
    return start;
  }

  // Returns a number of buckets in a histogram.
  ui32 GetNumBuckets() const { return buckets.size(); }
  // Returns histogram type.
  EHistogramValueType GetType() const { return valueType; }
  // Returns a number of elements in a bucket by the given `index`.
  ui64 GetNumElementsInBucket(ui32 index) const { return buckets[index].count; }

  // Initializes buckets with a given `range`.
  template <typename T>
  void InitializeBuckets(const TBucketRange &range) {
    // TODO: Proper diff calculation for types like `string`, `datetime`, etc.
    Y_ASSERT(CmpLess<T>(LoadFrom<T>(range.start), LoadFrom<T>(range.end)));
    T rangeLen = LoadFrom<T>(range.end) - LoadFrom<T>(range.start);
    std::memcpy(buckets[0].start, range.start, sizeof(range.start));
    for (ui32 i = 1; i < GetNumBuckets(); ++i) {
      const T prevStart = LoadFrom<T>(buckets[i - 1].start);
      StoreTo<T>(buckets[i].start, prevStart + rangeLen);
    }
  }

  // Seriailizes to a binary representation
  std::unique_ptr<char> Serialize(ui64 &binSize) const;
  // Returns buckets.
  TVector<TBucket> &GetBuckets() { return buckets; }

 private:
  // Returns binary size of the histogram.
  ui64 GetBinarySize(ui32 nBuckets) const;
  EHistogramValueType valueType;
  TVector<TBucket> buckets;
};

// This class represents a machinery to estimate a value in a histogram.
class TEqWidthHistogramEstimator {
 public:
  TEqWidthHistogramEstimator(std::shared_ptr<TEqWidthHistogram> histogram);

  // Methods to estimate values.
  template <typename T>
  ui64 EstimateLessOrEqual(T val) const {
    return EstimateOrEqual<T>(val, prefixSum);
  }

  template <typename T>
  ui64 EstimateGreaterOrEqual(T val) const {
    return EstimateOrEqual<T>(val, suffixSum);
  }

  template <typename T>
  ui64 EstimateLess(T val) const {
    return EstimateNotEqual<T>(val, prefixSum);
  }

  template <typename T>
  ui64 EstimateGreater(T val) const {
    return EstimateNotEqual<T>(val, suffixSum);
  }

  // Returns the total number elements in histogram.
  // Could be used to adjust scale.
  ui64 GetNumElements() const { return prefixSum.back(); }

 private:
  template <typename T>
  ui64 EstimateOrEqual(T val, const TVector<ui64> &sumArray) const {
    const auto index = histogram->FindBucketIndex(val);
    return sumArray[index];
  }

  template <typename T>
  ui64 EstimateNotEqual(T val, const TVector<ui64> &sumArray) const {
    const auto index = histogram->FindBucketIndex(val);
    // Take the previous backet if it's not the first one.
    if (!index) {
      return sumArray[index];
    }
    return sumArray[index - 1];
  }

  void CreatePrefixSum(ui32 numBuckets);
  void CreateSuffixSum(ui32 numBuckets);
  std::shared_ptr<TEqWidthHistogram> histogram;
  TVector<ui64> prefixSum;
  TVector<ui64> suffixSum;
};
}  // namespace NOptimizerHistograms
}  // namespace NKikimr