#include "eq_width_histogram.h"

namespace NKikimr {

TEqWidthHistogram::TEqWidthHistogram(ui64 numBuckets, EHistogramValueType valueType)
    : ValueType_(valueType)
    , Buckets_(numBuckets, 0)
{
    // Exptected at least one bucket for histogram.
    Y_ASSERT(numBuckets >= 1);
    Y_ABORT_UNLESS(ValueType_ != EHistogramValueType::NotSupported);
}

TEqWidthHistogram::TEqWidthHistogram(const char* str, ui64 size) {
    Y_ASSERT(str && size);
    const ui64 numBuckets = *reinterpret_cast<const ui64*>(str);
    Y_ABORT_UNLESS(GetBinarySize(numBuckets) == size);
    ui64 offset = sizeof(ui64);
    ValueType_ = *reinterpret_cast<const EHistogramValueType*>(str + offset);
    Y_ABORT_UNLESS(ValueType_ != EHistogramValueType::NotSupported);
    offset += sizeof(EHistogramValueType);
    Buckets_ = TVector<TBucket>(numBuckets);
    for (ui64 i = 0; i < numBuckets; ++i) {
        std::memcpy(&Buckets_[i], reinterpret_cast<const char*>(str + offset), sizeof(TBucket));
        offset += sizeof(TBucket);
    }
}

void TEqWidthHistogram::AddElement(const char* data, size_t size) {
    switch (ValueType_) {
        case EHistogramValueType::Int16: {
            i16 val; memcpy(&val, data, size); AddElementTyped(val); break;
        }
        case EHistogramValueType::Int32: {
            i32 val; memcpy(&val, data, size); AddElementTyped(val); break;
        }
        case EHistogramValueType::Int64: {
            i64 val; memcpy(&val, data, size); AddElementTyped(val); break;
        }
        case EHistogramValueType::Uint16: {
            ui16 val; memcpy(&val, data, size); AddElementTyped(val); break;
        }
        case EHistogramValueType::Uint32: {
            ui32 val; memcpy(&val, data, size); AddElementTyped(val); break;
        }
        case EHistogramValueType::Uint64: {
            ui64 val; memcpy(&val, data, size); AddElementTyped(val); break;
        }
        case EHistogramValueType::Double: {
            double val; memcpy(&val, data, size); AddElementTyped(val); break;
        }
        default: Y_ABORT("Unsupported histogram type");
    }
}

void TEqWidthHistogram::Aggregate(const TEqWidthHistogram& other) {
    switch (ValueType_) {
        case EHistogramValueType::Int16:  { if (!BucketsEqual<i16>(other)) return; break; }
        case EHistogramValueType::Int32:  { if (!BucketsEqual<i32>(other)) return; break; }
        case EHistogramValueType::Int64:  { if (!BucketsEqual<i64>(other)) return; break; }
        case EHistogramValueType::Uint16: { if (!BucketsEqual<ui16>(other)) return; break; }
        case EHistogramValueType::Uint32: { if (!BucketsEqual<ui32>(other)) return; break; }
        case EHistogramValueType::Uint64: { if (!BucketsEqual<ui64>(other)) return; break; }
        case EHistogramValueType::Double: { if (!BucketsEqual<double>(other)) return; break; }
        default:
            Y_ABORT("Unsupported histogram type");
    }
    for (ui64 i = 0; i < Buckets_.size(); ++i) {
        Buckets_[i].Count += other.GetNumElementsInBucket(i);
    }
}

ui64 TEqWidthHistogram::GetBinarySize(ui64 nBuckets) const {
    return sizeof(ui64) + sizeof(EHistogramValueType) + sizeof(TBucket) * nBuckets;
}

// Binary layout:
// [4 byte: number of buckets][1 byte: value type]
// [sizeof(Bucket)[0]... sizeof(Bucket)[n]].
std::pair<std::unique_ptr<char>, ui64> TEqWidthHistogram::Serialize() const {
    ui64 binarySize = GetBinarySize(GetNumBuckets());
    std::unique_ptr<char> binaryData(new char[binarySize]);
    ui64 offset = 0;
    const ui64 numBuckets = GetNumBuckets();
    // 4 byte - number of buckets.
    std::memcpy(binaryData.get(), &numBuckets, sizeof(ui64));
    offset += sizeof(ui64);
    // 1 byte - values type.
    WriteUnaligned<EHistogramValueType>(out + offset, ValueType_);
    offset += sizeof(EHistogramValueType);
    // 16 byte - domain range.
    for (size_t i = 0; i < EqWidthHistogramBucketStorageSize; ++i) {
        WriteUnaligned<ui8>(out + offset, DomainRange_.Start[i]);
        offset += sizeof(ui8);
    }
    for (size_t i = 0; i < EqWidthHistogramBucketStorageSize; ++i) {
        WriteUnaligned<ui8>(out + offset, DomainRange_.End[i]);
        offset += sizeof(ui8);
    }
    // Buckets.
    for (ui64 i = 0; i < numBuckets; ++i) {
        std::memcpy(binaryData.get() + offset, &Buckets_[i], sizeof(TBucket));
        offset += sizeof(TBucket);
    }
    return std::make_pair(std::move(binaryData), binarySize);
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

void TEqWidthHistogramEstimator::CreatePrefixSum(ui64 numBuckets) {
    PrefixSum_[0] = Histogram_->GetNumElementsInBucket(0);
    for (ui64 i = 1; i < numBuckets; ++i) {
        PrefixSum_[i] = PrefixSum_[i - 1] + Histogram_->GetNumElementsInBucket(i);
    }
}

void TEqWidthHistogramEstimator::CreateSuffixSum(ui64 numBuckets) {
    SuffixSum_[numBuckets - 1] = Histogram_->GetNumElementsInBucket(numBuckets - 1);
    for (ui64 i = numBuckets - 1; i > 0; --i) {
        SuffixSum_[i - 1] = SuffixSum_[i] + Histogram_->GetNumElementsInBucket(i - 1);
    };
}
} // namespace NKikimr
