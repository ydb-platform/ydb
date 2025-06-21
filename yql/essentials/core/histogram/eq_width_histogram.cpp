#include "eq_width_histogram.h"

namespace NKikimr {

TEqWidthHistogram::TEqWidthHistogram(ui32 numBuckets, EHistogramValueType valueType)
    : ValueType_(valueType), Buckets_(numBuckets) {
  // Exptected at least one bucket for histogram.
  Y_ASSERT(numBuckets >= 1);
}

TEqWidthHistogram::TEqWidthHistogram(const char *str, ui64 size) {
  Y_ASSERT(str && size);
  const ui32 numBuckets = *reinterpret_cast<const ui32 *>(str);
  Y_ABORT_UNLESS(GetBinarySize(numBuckets) == size);
  ui32 offset = sizeof(ui32);
  ValueType_ = *reinterpret_cast<const EHistogramValueType *>(str + offset);
  offset += sizeof(EHistogramValueType);
  Buckets_ = TVector<TBucket>(numBuckets);
  for (ui32 i = 0; i < numBuckets; ++i) {
    std::memcpy(&Buckets_[i], reinterpret_cast<const char *>(str + offset), sizeof(TBucket));
    offset += sizeof(TBucket);
  }
}

ui64 TEqWidthHistogram::GetBinarySize(ui32 nBuckets) const {
  return sizeof(ui32) + sizeof(EHistogramValueType) + sizeof(TBucket) * nBuckets;
}

// Binary layout:
// [4 byte: number of buckets][1 byte: value type]
// [sizeof(Bucket)[0]... sizeof(Bucket)[n]].
std::unique_ptr<char> TEqWidthHistogram::Serialize(ui64 &binarySize) const {
  binarySize = GetBinarySize(GetNumBuckets());
  std::unique_ptr<char> binaryData(new char[binarySize]);
  ui32 offset = 0;
  const ui32 numBuckets = GetNumBuckets();
  // 4 byte - number of buckets.
  std::memcpy(binaryData.get(), &numBuckets, sizeof(ui32));
  offset += sizeof(ui32);
  // 1 byte - values type.
  std::memcpy(binaryData.get() + offset, &ValueType_, sizeof(EHistogramValueType));
  offset += sizeof(EHistogramValueType);
  // Buckets.
  for (ui32 i = 0; i < numBuckets; ++i) {
    std::memcpy(binaryData.get() + offset, &Buckets_[i], sizeof(TBucket));
    offset += sizeof(TBucket);
  }
  return binaryData;
}

TEqWidthHistogramEstimator::TEqWidthHistogramEstimator(std::shared_ptr<TEqWidthHistogram> histogram)
    : Histogram_(histogram) {
  const auto numBuckets = Histogram_->GetNumBuckets();
  PrefixSum_ = TVector<ui64>(numBuckets);
  SuffixSum_ = TVector<ui64>(numBuckets);
  CreatePrefixSum(numBuckets);
  CreateSuffixSum(numBuckets);
}

void TEqWidthHistogramEstimator::CreatePrefixSum(ui32 numBuckets) {
  PrefixSum_[0] = Histogram_->GetNumElementsInBucket(0);
  for (ui32 i = 1; i < numBuckets; ++i) {
    PrefixSum_[i] = PrefixSum_[i - 1] + Histogram_->GetNumElementsInBucket(i);
  }
}

void TEqWidthHistogramEstimator::CreateSuffixSum(ui32 numBuckets) {
  SuffixSum_[numBuckets - 1] = Histogram_->GetNumElementsInBucket(numBuckets - 1);
  for (i32 i = static_cast<i32>(numBuckets) - 2; i >= 0; --i) {
    SuffixSum_[i] = SuffixSum_[i + 1] + Histogram_->GetNumElementsInBucket(i);
  }
}
}  // namespace NKikimr
