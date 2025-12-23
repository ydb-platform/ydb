#include "eq_width_histogram.h"

namespace NKikimr {

TEqWidthHistogram::TEqWidthHistogram(ui32 numBuckets, EHistogramValueType valueType)
    : ValueType_(valueType)
    , Buckets_(numBuckets)
{
    // Exptected at least one bucket for histogram.
    Y_ENSURE(numBuckets >= 1);
    Y_ENSURE(ValueType_ != EHistogramValueType::NotSupported, "Unsupported histogram type");
}

TEqWidthHistogram::TEqWidthHistogram(const char* str, size_t size) {
    Y_ENSURE(str && size);
    const ui32 numBuckets = ReadUnaligned<ui32>(str);
    Y_ENSURE(GetBinarySize(numBuckets) == size);
    ui32 offset = sizeof(ui32);
    ValueType_ = ReadUnaligned<EHistogramValueType>(str + offset);
    Y_ENSURE(ValueType_ != EHistogramValueType::NotSupported, "Unsupported histogram type");
    offset += sizeof(EHistogramValueType);
    Buckets_ = TVector<TBucket>(numBuckets);
    for (ui32 i = 0; i < numBuckets; ++i) {
        Buckets_[i] = ReadUnaligned<TBucket>(str + offset);
        offset += sizeof(TBucket);
    }
}

void TEqWidthHistogram::Aggregate(const TEqWidthHistogram& other) {
    switch (ValueType_) {
#define HIST_TYPE_CHECK(layout, type)        \
    case EHistogramValueType::layout: {      \
        Y_ENSURE(BucketsEqual<type>(other)); \
    }
        KNOWN_FIXED_HISTOGRAM_TYPES(HIST_TYPE_CHECK)
#undef HIST_TYPE_CHECK

        default:
            Y_ENSURE(ValueType_ != EHistogramValueType::NotSupported, "Unsupported histogram type");
    }
    for (ui32 i = 0; i < Buckets_.size(); ++i) {
        Buckets_[i].Count += other.GetNumElementsInBucket(i);
    }
}

ui64 TEqWidthHistogram::GetBinarySize(ui32 nBuckets) const {
    return sizeof(ui32) + sizeof(EHistogramValueType) + sizeof(TBucket) * nBuckets;
}

// Binary layout:
// [4 byte: number of buckets][1 byte: value type]
// [sizeof(Bucket)[0]... sizeof(Bucket)[n]].
TString TEqWidthHistogram::Serialize() const {
    const ui32 numBuckets = GetNumBuckets();
    const ui64 binarySize = GetBinarySize(GetNumBuckets());
    TString result = TString::Uninitialized(binarySize);
    char* out = result.Detach();
    ui32 offset = 0;
    // 4 byte - number of buckets.
    WriteUnaligned<ui32>(out + offset, numBuckets);
    offset += sizeof(numBuckets);
    // 1 byte - values type.
    WriteUnaligned<EHistogramValueType>(out + offset, ValueType_);
    offset += sizeof(EHistogramValueType);
    // Buckets.
    for (ui32 i = 0; i < numBuckets; ++i) {
        WriteUnaligned<TBucket>(out + offset, Buckets_[i]);
        offset += sizeof(TBucket);
    }
    Y_ENSURE(offset == binarySize);
    return result;
}

TEqWidthHistogramEstimator::TEqWidthHistogramEstimator(std::shared_ptr<TEqWidthHistogram> histogram)
    : Histogram_(histogram)
{
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
    for (ui32 i = numBuckets - 1; i > 0; --i) {
        SuffixSum_[i - 1] = SuffixSum_[i] + Histogram_->GetNumElementsInBucket(i - 1);
    };
}
} // namespace NKikimr
