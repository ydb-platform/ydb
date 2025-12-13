#include "eq_width_histogram.h"

namespace NKikimr {

TEqWidthHistogram::TEqWidthHistogram(ui32 numBuckets, EHistogramValueType valueType)
    : ValueType_(valueType)
    , Buckets_(numBuckets)
{
    // Exptected at least one bucket for histogram.
    Y_ASSERT(numBuckets >= 1);
    Y_ABORT_UNLESS(ValueType_ != EHistogramValueType::NotSupported);
}

TEqWidthHistogram::TEqWidthHistogram(const char* str, ui64 size) {
    Y_ASSERT(str && size);
    const ui32 numBuckets = ReadUnaligned<ui32>(str);
    Y_ABORT_UNLESS(GetBinarySize(numBuckets) == size);
    ui32 offset = sizeof(ui32);
    ValueType_ = *reinterpret_cast<const EHistogramValueType*>(str + offset);
    Y_ABORT_UNLESS(ValueType_ != EHistogramValueType::NotSupported);
    offset += sizeof(EHistogramValueType);
    Buckets_ = TVector<TBucket>(numBuckets);
    for (ui32 i = 0; i < numBuckets; ++i) {
        std::memcpy(&Buckets_[i], reinterpret_cast<const char*>(str + offset), sizeof(TBucket));
        offset += sizeof(TBucket);
    }
}

void TEqWidthHistogram::Aggregate(const TEqWidthHistogram& other) {
    switch (ValueType_) {
        case EHistogramValueType::Int16: {
            if (!BucketsEqual<i16>(other)) {
                return;
            }
            break;
        }
        case EHistogramValueType::Int32: {
            if (!BucketsEqual<i32>(other)) {
                return;
            }
            break;
        }
        case EHistogramValueType::Int64: {
            if (!BucketsEqual<i64>(other)) {
                return;
            }
            break;
        }
        case EHistogramValueType::Uint16: {
            if (!BucketsEqual<ui16>(other)) {
                return;
            }
            break;
        }
        case EHistogramValueType::Uint32: {
            if (!BucketsEqual<ui32>(other)) {
                return;
            }
            break;
        }
        case EHistogramValueType::Uint64: {
            if (!BucketsEqual<ui64>(other)) {
                return;
            }
            break;
        }
        case EHistogramValueType::Double: {
            if (!BucketsEqual<double>(other)) {
                return;
            }
            break;
        }
        default:
            Y_ABORT_UNLESS(ValueType_ != EHistogramValueType::NotSupported);
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
    std::memcpy(out + offset, &numBuckets, sizeof(numBuckets));
    offset += sizeof(numBuckets);
    // 1 byte - values type.
    std::memcpy(out + offset, &ValueType_, sizeof(EHistogramValueType));
    offset += sizeof(EHistogramValueType);
    // Buckets.
    for (ui32 i = 0; i < numBuckets; ++i) {
        std::memcpy(out + offset, &Buckets_[i], sizeof(TBucket));
        offset += sizeof(TBucket);
    }
    Y_ASSERT(offset == binarySize);
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
