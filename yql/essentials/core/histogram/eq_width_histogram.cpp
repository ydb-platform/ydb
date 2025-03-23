#include "eq_width_histogram.h"

namespace NKikimr {
namespace NOptimizerHistograms {

TEqWidthHistogram::TEqWidthHistogram(ui32 numBuckets, EHistogramValueType valueType)
    : valueType(valueType), buckets(numBuckets) {
  // Exptected at least one bucket for histogram.
  Y_ASSERT(numBuckets >= 1);
}

TEqWidthHistogram::TEqWidthHistogram(const char *str, ui64 size) {
  Y_ASSERT(str && size);
  const ui32 numBuckets = *reinterpret_cast<const ui32 *>(str);
  Y_ABORT_UNLESS(GetBinarySize(numBuckets) == size);
  ui32 offset = sizeof(ui32);
  valueType = *reinterpret_cast<const EHistogramValueType *>(str + offset);
  offset += sizeof(EHistogramValueType);
  buckets = TVector<TBucket>(numBuckets);
  for (ui32 i = 0; i < numBuckets; ++i) {
    std::memcpy(&buckets[i], reinterpret_cast<const char *>(str + offset), sizeof(TBucket));
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
  std::memcpy(binaryData.get() + offset, &valueType, sizeof(EHistogramValueType));
  offset += sizeof(EHistogramValueType);
  // Buckets.
  for (ui32 i = 0; i < numBuckets; ++i) {
    std::memcpy(binaryData.get() + offset, &buckets[i], sizeof(TBucket));
    offset += sizeof(TBucket);
  }
  return binaryData;
}

TEqWidthHistogramEstimator::TEqWidthHistogramEstimator(std::shared_ptr<TEqWidthHistogram> histogram)
    : histogram(histogram) {
  const auto numBuckets = histogram->GetNumBuckets();
  prefixSum = TVector<ui64>(numBuckets);
  suffixSum = TVector<ui64>(numBuckets);
  CreatePrefixSum(numBuckets);
  CreateSuffixSum(numBuckets);
}

void TEqWidthHistogramEstimator::CreatePrefixSum(ui32 numBuckets) {
  prefixSum[0] = histogram->GetNumElementsInBucket(0);
  for (ui32 i = 1; i < numBuckets; ++i) {
    prefixSum[i] = prefixSum[i - 1] + histogram->GetNumElementsInBucket(i);
  }
}

void TEqWidthHistogramEstimator::CreateSuffixSum(ui32 numBuckets) {
  suffixSum[numBuckets - 1] = histogram->GetNumElementsInBucket(numBuckets - 1);
  for (i32 i = static_cast<i32>(numBuckets) - 2; i >= 0; --i) {
    suffixSum[i] = suffixSum[i + 1] + histogram->GetNumElementsInBucket(i);
  }
}
}  // namespace NOptimizerHistograms
}  // namespace NKikimr