#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/types.h>
#include <util/generic/maybe.h>
#include <util/system/unaligned_mem.h>
#include <util/generic/yexception.h>
#include <cmath>

namespace NKikimr {

// Helper functions to work with histogram values.
template <typename T>
inline T LoadFrom(const ui8* storage) {
    T val;
    std::memcpy(&val, storage, sizeof(T));
    return val;
}
template <typename T>
inline void StoreTo(ui8* storage, T value) {
    std::memcpy(storage, &value, sizeof(T));
}
template <typename T>
inline bool CmpEqual(T left, T right) {
    return left == right;
}
template <>
inline bool CmpEqual(float left, float right) {
    if (std::isinf(left) || std::isinf(right)) {
        return left == right;
    }
    if (std::isnan(left) || std::isnan(right)) {
        return false;
    }
    float diff = std::fabs(left - right);
    float scale = std::max({1.0f, std::fabs(left), std::fabs(right)});
    return diff <= std::numeric_limits<float>::epsilon() * scale;
}
template <>
inline bool CmpEqual(double left, double right) {
    if (std::isinf(left) || std::isinf(right)) {
        return left == right;
    }
    if (std::isnan(left) || std::isnan(right)) {
        return false;
    }
    double diff = std::fabs(left - right);
    double scale = std::max({1.0, std::fabs(left), std::fabs(right)});
    return diff <= std::numeric_limits<double>::epsilon() * scale;
}
template <typename T>
inline bool CmpLess(T left, T right) {
    return left < right;
}

// Represents value types supported by histogram.
enum class EHistogramValueType: ui8 { Int8,
                                      Int16,
                                      Int32,
                                      Int64,
                                      Uint8,
                                      Uint16,
                                      Uint32,
                                      Uint64,
                                      Float,
                                      Double,
                                      NotSupported };

// clang-format off
#define KNOWN_FIXED_HISTOGRAM_TYPES(xx) \
xx(Int8, i8)                            \
xx(Int16, i16)                          \
xx(Int32, i32)                          \
xx(Int64, i64)                          \
xx(Uint8, ui8)                          \
xx(Uint16, ui16)                        \
xx(Uint32, ui32)                        \
xx(Uint64, ui64)                        \
xx(Float, float)                        \
xx(Double, double)
// clang-format on

template <typename T>
inline TMaybe<EHistogramValueType> GetHistogramValueType() {
    return Nothing();
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<i8>() {
    return EHistogramValueType::Int8;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<i16>() {
    return EHistogramValueType::Int16;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<i32>() {
    return EHistogramValueType::Int32;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<i64>() {
    return EHistogramValueType::Int64;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<ui8>() {
    return EHistogramValueType::Uint8;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<ui16>() {
    return EHistogramValueType::Uint16;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<ui32>() {
    return EHistogramValueType::Uint32;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<ui64>() {
    return EHistogramValueType::Uint64;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<float>() {
    return EHistogramValueType::Float;
}

template <>
inline TMaybe<EHistogramValueType> GetHistogramValueType<double>() {
    return EHistogramValueType::Double;
}

// Bucket storage size for Equal width histogram.
constexpr const ui32 EqWidthHistogramBucketStorageSize = 8;

// This class represents an `Equi-width` histogram.
// Each bucket represents a range of contiguous values of equal width, and the
// aggregate summary stored in the bucket is the number of rows whose value lies
// within that range.
class TEqWidthHistogram {
public:
#pragma pack(push, 1)
    struct THistValue {
        ui8 Value[EqWidthHistogramBucketStorageSize];
    };
    struct TDomainRange {
        ui8 Start[EqWidthHistogramBucketStorageSize];
        ui8 End[EqWidthHistogramBucketStorageSize];
    };
#pragma pack(pop)

    // Have to specify the number of buckets and type of the values.
    explicit TEqWidthHistogram(ui32 numBuckets = 1, EHistogramValueType type = EHistogramValueType::Int32);
    // From serialized data.
    TEqWidthHistogram(const char* str, size_t size);

    // Adds the given `val` to a histogram.
    // Values which exceed the domain min/max are ignored.
    template <typename T>
    void AddElement(T val) {
        const T domainStart = LoadFrom<T>(GetDomainRange().Start);
        const T domainEnd = LoadFrom<T>(GetDomainRange().End);
        if (CmpLess<T>(val, domainStart) || CmpLess<T>(domainEnd, val)) {
            return;
        }

        const auto index = FindBucketIndex(val);
        Buckets_[index]++;
    }

    // To prevent large positive value due to e.g. AddElement<ui32>(-5).
    template <typename T>
    void AddElement(std::type_identity_t<T> val) = delete;

    // Returns an index of the bucket which stores the given `val`.
    // Returned index in range [0, numBuckets - 1].
    // Not using `std::lower_bound()` here because need an index to map to `suffix` and `prefix` sum.
    template <typename T>
    ui32 FindBucketIndex(T val) const {
        const T domainStart = LoadFrom<T>(DomainRange_.Start);
        const T domainEnd = LoadFrom<T>(DomainRange_.End);
        if (CmpLess<T>(val, domainStart)) {
            return 0;
        }
        if (CmpLess<T>(domainEnd, val)) {
            return NumBuckets_ - 1;
        }
        const THistValue bucketWidth = GetBucketWidth<T>();
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            using UT = std::make_unsigned_t<T>;
            const UT start = static_cast<UT>(domainStart);
            const UT value = static_cast<UT>(val);
            const UT diff = value - start;
            UT bucketIndex = diff / LoadFrom<UT>(bucketWidth.Value);
            bucketIndex = std::floor<UT>(bucketIndex);
            bucketIndex = std::min<UT>(NumBuckets_ - 1, bucketIndex);
            return static_cast<ui32>(bucketIndex);
        }
        T bucketIndex = std::floor((val - domainStart) / LoadFrom<T>(bucketWidth.Value));
        bucketIndex = std::min<T>(NumBuckets_ - 1, bucketIndex);
        return static_cast<ui32>(bucketIndex);
    }

    // To prevent large positive value due to e.g. FindBucketIndex<ui32>(-5).
    template <typename T>
    ui32 FindBucketIndex(std::type_identity_t<T> val) = delete;

    // Returns bucket width based on domain range and number of buckets.
    template <typename T>
    THistValue GetBucketWidth() const {
        THistValue returnValue;
        const T start = LoadFrom<T>(DomainRange_.Start);
        const T end = LoadFrom<T>(DomainRange_.End);
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            using UT = std::make_unsigned_t<T>;
            const UT rangeLen = static_cast<UT>(end) - static_cast<UT>(start);
            // width is truncated after division.
            const UT bucketWidth = rangeLen / static_cast<UT>(NumBuckets_);
            StoreTo<UT>(returnValue.Value, bucketWidth);
            return returnValue;
        }
        const T rangeLen = end - start;
        const T bucketWidth = rangeLen / static_cast<T>(NumBuckets_);
        StoreTo<T>(returnValue.Value, bucketWidth);
        return returnValue;
    }

    // Returns border value of targeted bucket.
    template <typename T>
    T GetBorderValue(i64 index) const {
        Y_ENSURE(CmpLess<i64>(-1, index));
        Y_ENSURE(CmpLess<i64>(index, NumBuckets_));

        const TEqWidthHistogram::THistValue bucketWidth = GetBucketWidth<T>();
        const T width = LoadFrom<T>(bucketWidth.Value);
        const T domainStart = LoadFrom<T>(GetDomainRange().Start);
        const T border = domainStart + static_cast<T>(index) * width;
        return border;
    }

    // Initializes buckets with a given `range`.
    // NOTE: buckets can be less than range (i.e. end - start)
    template <typename T>
    void InitializeBuckets(T rangeStart, T rangeEnd) {
        // class invariant: start < end.
        Y_ENSURE(CmpLess<T>(rangeStart, rangeEnd));
        DomainRange_ = {};
        StoreTo<T>(DomainRange_.Start, rangeStart);
        StoreTo<T>(DomainRange_.End, rangeEnd);

        // class invariant: bucket width is non-zero positive.
        const THistValue bucketWidth = GetBucketWidth<T>();
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            using UT = std::make_unsigned_t<T>;
            Y_ENSURE(CmpLess<UT>(0, LoadFrom<UT>(bucketWidth.Value)), "Domain range is too close");
        } else {
            Y_ENSURE(CmpLess<T>(0, LoadFrom<T>(bucketWidth.Value)), "Domain range is too close");
        }
    }

    // To prevent large positive value due to e.g. InitializeBuckets<ui32>(-5, -1).
    template <typename T>
    void InitializeBuckets(std::type_identity_t<T> rangeStart, std::type_identity_t<T> rangeEnd) = delete;

    // Checks whether two histograms have same parameters.
    template <typename T>
    bool BucketsEqual(const TEqWidthHistogram& other) {
        if (NumBuckets_ != other.GetNumBuckets()) {
            return false;
        } else if (ValueType_ != other.GetType()) {
            return false;
        } else if (!CmpEqual<T>(LoadFrom<T>(DomainRange_.Start), LoadFrom<T>(other.GetDomainRange().Start))) {
            return false;
        } else if (!CmpEqual<T>(LoadFrom<T>(DomainRange_.End), LoadFrom<T>(other.GetDomainRange().End))) {
            return false;
        }
        return true;
    }

    // Seriailizes to a binary representation.
    TString Serialize() const;

    // Merge two histograms given their parameters match.
    void Aggregate(const TEqWidthHistogram& other);

    // Returns a number of buckets in histogram.
    ui32 GetNumBuckets() const {
        return NumBuckets_;
    }

    // Returns histogram type.
    EHistogramValueType GetType() const {
        return ValueType_;
    }

    // Returns a number of elements in a bucket by the given `index`.
    ui64 GetNumElementsInBucket(i64 index) const {
        Y_ENSURE(CmpLess<i64>(-1, index));
        Y_ENSURE(CmpLess<i64>(index, NumBuckets_));
        return Buckets_[index];
    }

    // Returns domain range.
    TDomainRange GetDomainRange() const {
        return DomainRange_;
    }

    // Returns binary size of the histogram.
    size_t GetSize() const {
        return GetBinarySize(NumBuckets_);
    }

    static ui64 GetBinarySize(ui32 nBuckets) {
        return sizeof(VersionNumber_) + sizeof(nBuckets) +
               sizeof(EHistogramValueType) + sizeof(TDomainRange) + sizeof(ui64) * nBuckets;
    }

private:
    ui8 VersionNumber_ = 0;
    ui32 NumBuckets_;
    EHistogramValueType ValueType_;
    TDomainRange DomainRange_;
    TVector<ui64> Buckets_;
};

// This class represents a machinery to estimate a value in a histogram.
class TEqWidthHistogramEstimator {
public:
    explicit TEqWidthHistogramEstimator(std::shared_ptr<TEqWidthHistogram> histogram);

    // all values <= `val`.
    template <typename T>
    ui64 EstimateLessOrEqual(T val) const {
        // Due to values which exceed the domain min/max.
        const T domainStart = LoadFrom<T>(Histogram_->GetDomainRange().Start);
        const T domainEnd = LoadFrom<T>(Histogram_->GetDomainRange().End);
        if (CmpLess<T>(val, domainStart)) {
            return 0;
        } else if (CmpLess<T>(domainEnd, val)) {
            return PrefixSum_.back();
        }

        const auto index = Histogram_->FindBucketIndex(val);
        if (!index) {
            return EstimateEqual(val);
        }
        return PrefixSum_[index - 1] + EstimateEqual(val);
    }

    // all values >= `val`.
    template <typename T>
    ui64 EstimateGreaterOrEqual(T val) const {
        // Due to values which exceed the domain min/max.
        const T domainStart = LoadFrom<T>(Histogram_->GetDomainRange().Start);
        const T domainEnd = LoadFrom<T>(Histogram_->GetDomainRange().End);
        if (CmpLess<T>(domainEnd, val)) {
            return 0;
        } else if (CmpLess<T>(val, domainStart)) {
            return SuffixSum_.front();
        }

        const auto index = Histogram_->FindBucketIndex(val);
        const auto numBuckets = Histogram_->GetNumBuckets();
        if (index + 1 == numBuckets) {
            return EstimateEqual(val);
        }
        return SuffixSum_[index + 1] + EstimateEqual(val);
    }

    // all values < `val`.
    template <typename T>
    ui64 EstimateLess(T val) const {
        // Due to values which exceed the domain min/max.
        const T domainStart = LoadFrom<T>(Histogram_->GetDomainRange().Start);
        const T domainEnd = LoadFrom<T>(Histogram_->GetDomainRange().End);
        if (CmpLess<T>(val, domainStart)) {
            return 0;
        } else if (CmpLess<T>(domainEnd, val)) {
            return PrefixSum_.back();
        }

        const auto index = Histogram_->FindBucketIndex(val);
        const auto border = Histogram_->GetBorderValue<T>(index);
        if (val == border) {
            if (!index) {
                return 0;
            }
            return PrefixSum_[index - 1];
        }

        if (!index) {
            return EstimateEqual(val);
        }
        return PrefixSum_[index - 1] + EstimateEqual(val);
    }

    // all values > `val`.
    template <typename T>
    ui64 EstimateGreater(T val) const {
        // Due to values which exceed the domain min/max.
        const T domainStart = LoadFrom<T>(Histogram_->GetDomainRange().Start);
        const T domainEnd = LoadFrom<T>(Histogram_->GetDomainRange().End);
        if (CmpLess<T>(domainEnd, val)) {
            return 0;
        } else if (CmpLess<T>(val, domainStart)) {
            return SuffixSum_.front();
        }

        const auto index = Histogram_->FindBucketIndex(val);
        const auto numBuckets = Histogram_->GetNumBuckets();
        // TODO: handle the case at the border
        // const auto border = Histogram_->GetBorderValue<T>(index);
        // if (val == border) {
        //     if (index + 1 == numBuckets) {
        //         return 0;
        //     }
        //     return SuffixSum_[index + 1];
        // }

        if (index + 1 == numBuckets) {
            return EstimateEqual(val);
        }
        return SuffixSum_[index + 1] + EstimateEqual(val);
    }

    template <typename T>
    ui64 EstimateEqual(T val) const {
        const auto index = Histogram_->FindBucketIndex(val);
        const auto count = Histogram_->GetNumElementsInBucket(index);
        const TEqWidthHistogram::THistValue bucketWidth = Histogram_->GetBucketWidth<T>();
        // Assuming uniform distribution.
        // Final estimated values are truncated after division.
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            const ui64 width = LoadFrom<ui64>(bucketWidth.Value);
            return count / width;
        }
        // TODO: currenty return count due to close-to-zero width thus count / width generates large value
        // const T width = LoadFrom<T>(bucketWidth.Value);
        // return static_cast<ui64>(count / width);
        return count;
    }

    // Returns the total number elements in histogram.
    // Could be used to adjust scale.
    ui64 GetNumElements() const {
        return PrefixSum_.back();
    }

    // Returns cardinality of overlapping keys based on PK domain bucket counts.
    // NOTE: number of buckets and widths may differ (e.g. PK-FK) due to different min/max values.
    // Also, max value can be large than the last bucket border value.
    TMaybe<ui64> GetOverlappingCardinality(const TEqWidthHistogramEstimator& other) const {
        Y_ENSURE(Histogram_->GetType() == other.Histogram_->GetType(), "Histogram value types must match");
        switch (Histogram_->GetType()) {
#define HIST_TYPE_CHECK(type, layout)                          \
    case EHistogramValueType::type: {                          \
        return GetOverlappingCardinalityHelper<layout>(other); \
    }
            KNOWN_FIXED_HISTOGRAM_TYPES(HIST_TYPE_CHECK)
#undef HIST_TYPE_CHECK
            default:
                Y_ENSURE(false, "Unsupported histogram data type");
                return Nothing();
        }
    }

    // Assumes that domain is PK, otherDomain is FK (i.e. FK is a subset of PK)
    template <typename T>
    ui64 GetOverlappingCardinalityHelper(const TEqWidthHistogramEstimator& other) const {
        const T otherDomainStart = LoadFrom<T>(other.Histogram_->GetDomainRange().Start);
        const T otherDomainEnd = LoadFrom<T>(other.Histogram_->GetDomainRange().End);

        ui32 leftIndex = Histogram_->FindBucketIndex(otherDomainStart);
        ui32 rightIndex = Histogram_->FindBucketIndex(otherDomainEnd);

        ui64 cardinality = 0;
        for (size_t i = leftIndex; i < rightIndex + 1; ++i) {
            cardinality += Histogram_->GetNumElementsInBucket(i);
        }
        return cardinality;
    }

private:
    void CreatePrefixSum(ui32 numBuckets);
    void CreateSuffixSum(ui32 numBuckets);
    std::shared_ptr<TEqWidthHistogram> Histogram_;
    TVector<ui64> PrefixSum_;
    TVector<ui64> SuffixSum_;
};
} // namespace NKikimr
