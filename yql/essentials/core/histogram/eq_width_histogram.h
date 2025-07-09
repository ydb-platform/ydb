#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/types.h>
#include <cmath>

namespace NKikimr {

  // Helper functions to work with histogram values.
template <typename T>
inline T LoadFrom(const ui8 *storage) {
  T val;
  std::memcpy(&val, storage, sizeof(T));
  return val;
}
template <typename T>
inline void StoreTo(ui8 *storage, T value) {
  std::memcpy(storage, &value, sizeof(T));
}
template <typename T>
inline bool CmpEqual(T left, T right) {
  return left == right;
}
template <>
inline bool CmpEqual(double left, double right) {
  return std::fabs(left - right) < std::numeric_limits<double>::epsilon();
}
template <typename T>
inline bool CmpLess(T left, T right) {
  return left < right;
}

// Represents value types supported by histogram.
enum class EHistogramValueType : ui8 { Int16, Int32, Int64, Uint16, Uint32, Uint64, Double, NotSupported };

// Bucket storage size for Equal width histogram.
constexpr const ui32 EqWidthHistogramBucketStorageSize = 8;

// This class represents an `Equal-width` histogram.
// Each bucket represents a range of contiguous values of equal width, and the
// aggregate summary stored in the bucket is the number of rows whose value lies
// within that range.
class TEqWidthHistogram {
 public:
#pragma pack(push, 1)
  struct TBucket {
    // The number of values in a bucket.
    ui64 Count{0};
    // The `start` value of a bucket, the `end` of the bucket is a next start.
    // [start = start[i], end = start[i + 1])
    ui8 Start[EqWidthHistogramBucketStorageSize];
  };
  struct TBucketRange {
    ui8 Start[EqWidthHistogramBucketStorageSize];
    ui8 End[EqWidthHistogramBucketStorageSize];
  };
#pragma pack(pop)

  // Have to specify the number of buckets and type of the values.
  TEqWidthHistogram(ui32 numBuckets = 1, EHistogramValueType type = EHistogramValueType::Int32);
  // From serialized data.
  TEqWidthHistogram(const char *str, ui64 size);

  // Adds the given `val` to a histogram.
  template <typename T>
  void AddElement(T val) {
    const auto index = FindBucketIndex(val);
    // The given `index` in range [0, numBuckets - 1].
    const T bucketValue = LoadFrom<T>(Buckets_[index].Start);
    if (!index || ((CmpEqual<T>(bucketValue, val) || CmpLess<T>(bucketValue, val)))) {
      Buckets_[index].Count++;
    } else {
      Buckets_[index - 1].Count++;
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
      if (CmpLess<T>(LoadFrom<T>(Buckets_[it].Start), val)) {
        start = it + 1;
      } else {
        end = it;
      }
    }
    return start;
  }

  // Returns a number of buckets in a histogram.
  ui32 GetNumBuckets() const { return Buckets_.size(); }

  template <typename T>
  ui32 GetBucketWidth() const {
    Y_ASSERT(GetNumBuckets());
    if (GetNumBuckets() == 1) {
      return std::max(static_cast<ui32>(LoadFrom<T>(Buckets_.front().Start)), 1U);
    } else {
      return std::max(static_cast<ui32>(LoadFrom<T>(Buckets_[1].Start) - LoadFrom<T>(Buckets_[0].Start)), 1U);
    }
  }

  template <>
  ui32 GetBucketWidth<double>() const {
    return 1;
  }

  // Returns histogram type.
  EHistogramValueType GetType() const { return ValueType_; }
  // Returns a number of elements in a bucket by the given `index`.
  ui64 GetNumElementsInBucket(ui32 index) const { return Buckets_[index].Count; }

  // Initializes buckets with a given `range`.
  template <typename T>
  void InitializeBuckets(const TBucketRange &range) {
    Y_ASSERT(CmpLess<T>(LoadFrom<T>(range.Start), LoadFrom<T>(range.End)));
    T rangeLen = LoadFrom<T>(range.End) - LoadFrom<T>(range.Start);
    std::memcpy(Buckets_[0].Start, range.Start, sizeof(range.Start));
    for (ui32 i = 1; i < GetNumBuckets(); ++i) {
      const T prevStart = LoadFrom<T>(Buckets_[i - 1].Start);
      StoreTo<T>(Buckets_[i].Start, prevStart + rangeLen);
    }
  }

  // Seriailizes to a binary representation
  std::unique_ptr<char> Serialize(ui64 &binSize) const;
  // Returns buckets.
  const TVector<TBucket> &GetBuckets() const { return Buckets_; }

  template <typename T>
  void Aggregate(const TEqWidthHistogram &other) {
    if ((this->ValueType_ != other.GetType()) || (!BucketsEqual<T>(other))) {
      // Should we fail?
      return;
    }
    for (ui32 i = 0; i < Buckets_.size(); ++i) {
      Buckets_[i].Count += other.GetBuckets()[i].Count;
    }
  }

 private:
  template <typename T>
  bool BucketsEqual(const TEqWidthHistogram &other) {
    if (Buckets_.size() != other.GetNumBuckets()) {
      return false;
    }
    for (ui32 i = 0; i < Buckets_.size(); ++i) {
      if (!CmpEqual<T>(LoadFrom<T>(Buckets_[i].Start), LoadFrom<T>(GetBuckets()[i].Start))) {
        return false;
      }
    }
    return true;
  }

  // Returns binary size of the histogram.
  ui64 GetBinarySize(ui32 nBuckets) const;
  EHistogramValueType ValueType_;
  TVector<TBucket> Buckets_;
};

// This class represents a machinery to estimate a value in a histogram.
class TEqWidthHistogramEstimator {
 public:
  TEqWidthHistogramEstimator(std::shared_ptr<TEqWidthHistogram> histogram);

  // Methods to estimate values.
  template <typename T>
  ui64 EstimateLessOrEqual(T val) const {
    return EstimateOrEqual<T>(val, PrefixSum_);
  }

  template <typename T>
  ui64 EstimateGreaterOrEqual(T val) const {
    return EstimateOrEqual<T>(val, SuffixSum_);
  }

  template <typename T>
  ui64 EstimateLess(T val) const {
    return EstimateNotEqual<T>(val, PrefixSum_);
  }

  template <typename T>
  ui64 EstimateGreater(T val) const {
    return EstimateNotEqual<T>(val, SuffixSum_);
  }

  template <typename T>
  ui64 EstimateEqual(T val) const {
    const auto index = Histogram_->FindBucketIndex(val);
    // Assuming uniform distribution.
    return std::max(1U, static_cast<ui32>(Histogram_->GetNumElementsInBucket(index) / Histogram_->template GetBucketWidth<T>()));
  }

  // Returns the total number elements in histogram.
  // Could be used to adjust scale.
  ui64 GetNumElements() const { return PrefixSum_.back(); }

 private:
  template <typename T>
  ui64 EstimateOrEqual(T val, const TVector<ui64> &sumArray) const {
    const auto index = Histogram_->FindBucketIndex(val);
    return sumArray[index];
  }

  template <typename T>
  ui64 EstimateNotEqual(T val, const TVector<ui64> &sumArray) const {
    const auto index = Histogram_->FindBucketIndex(val);
    // Take the previous backet if it's not the first one.
    if (!index) {
      return sumArray[index];
    }
    return sumArray[index - 1];
  }

  void CreatePrefixSum(ui32 numBuckets);
  void CreateSuffixSum(ui32 numBuckets);
  std::shared_ptr<TEqWidthHistogram> Histogram_;
  TVector<ui64> PrefixSum_;
  TVector<ui64> SuffixSum_;
};
}  // namespace NKikimr
