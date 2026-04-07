#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/types.h>
#include <util/generic/maybe.h>
#include <util/system/unaligned_mem.h>
#include <util/generic/yexception.h>

#include <array>
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
        std::array<ui8, EqWidthHistogramBucketStorageSize> Value;
    };
    struct TDomainRange {
        std::array<ui8, EqWidthHistogramBucketStorageSize> Start;
        std::array<ui8, EqWidthHistogramBucketStorageSize> End;
    };
#pragma pack(pop)

    // Have to specify the number of buckets and type of the values.
    explicit TEqWidthHistogram(ui32 numBuckets = 1, EHistogramValueType type = EHistogramValueType::Int32);
    // From serialized data.
    TEqWidthHistogram(const char* str, size_t size);

    // Adds the given `val` to a histogram.
    template <typename T>
    void AddElement(T val) {
        const auto index = FindBucketIndex(val);
        Buckets_[index]++;
    }

    // Returns an index of the bucket which stores the given `val`.
    // Returned index in range [0, numBuckets - 1].
    // Not using `std::lower_bound()` here because need an index to map to `suffix` and `prefix` sum.
    template <typename T>
    ui32 FindBucketIndex(T val) const {
        const T domainStart = LoadFrom<T>(DomainRange_.Start.data());
        const T domainEnd = LoadFrom<T>(DomainRange_.End.data());
        if (CmpLess<T>(val, domainStart)) {
            return 0;
        }
        if (CmpLess<T>(domainEnd, val)) {
            return GetNumBuckets() - 1;
        }
        const THistValue bucketWidth = GetBucketWidth<T>();
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            using UT = std::make_unsigned_t<T>;
            const UT start = static_cast<UT>(domainStart);
            const UT value = static_cast<UT>(val);
            const UT diff = value - start;
            UT bucketIndex = diff / LoadFrom<UT>(bucketWidth.Value.data());
            bucketIndex = std::floor<UT>(bucketIndex);
            bucketIndex = std::min<UT>(GetNumBuckets() - 1, bucketIndex);
            return static_cast<ui32>(bucketIndex);
        }
        T bucketIndex = std::floor((val - domainStart) / LoadFrom<T>(bucketWidth.Value.data()));
        bucketIndex = std::min<T>(GetNumBuckets() - 1, bucketIndex);
        return static_cast<ui32>(bucketIndex);
    }

    // Returns bucket width based on domain range and number of buckets.
    template <typename T>
    THistValue GetBucketWidth() const {
        THistValue returnValue;
        const T start = LoadFrom<T>(DomainRange_.Start.data());
        const T end = LoadFrom<T>(DomainRange_.End.data());
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            using UT = std::make_unsigned_t<T>;
            const UT rangeLen = static_cast<UT>(end) - static_cast<UT>(start);
            const UT bucketWidth = rangeLen / static_cast<UT>(GetNumBuckets());
            StoreTo<UT>(returnValue.Value.data(), bucketWidth);
            return returnValue;
        }
        const T rangeLen = end - start;
        const T bucketWidth = rangeLen / static_cast<T>(GetNumBuckets());
        StoreTo<T>(returnValue.Value.data(), bucketWidth);
        return returnValue;
    }

    // Initializes buckets with a given `range`.
    template <typename T>
    void InitializeBuckets(T rangeStart, T rangeEnd) {
        Y_ENSURE(CmpLess<T>(rangeStart, rangeEnd));
        DomainRange_ = {};
        StoreTo<T>(DomainRange_.Start.data(), rangeStart);
        StoreTo<T>(DomainRange_.End.data(), rangeEnd);
        const THistValue bucketWidth = GetBucketWidth<T>(); // non-zero positive width of each bucket
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            using UT = std::make_unsigned_t<T>;
            Y_ENSURE(CmpLess<UT>(0, LoadFrom<UT>(bucketWidth.Value.data())), "Domain range is too close");
        } else {
            Y_ENSURE(CmpLess<T>(0, LoadFrom<T>(bucketWidth.Value.data())), "Domain range is too close");
        }
    }

    // Checks whether two histograms have same parameters.
    template <typename T>
    bool BucketsEqual(const TEqWidthHistogram& other) {
        if (GetNumBuckets() != other.GetNumBuckets()) {
            return false;
        } else if (ValueType_ != other.GetType()) {
            return false;
        } else if (!CmpEqual<T>(LoadFrom<T>(DomainRange_.Start.data()), LoadFrom<T>(other.GetDomainRange().Start.data()))) {
            return false;
        } else if (!CmpEqual<T>(LoadFrom<T>(DomainRange_.End.data()), LoadFrom<T>(other.GetDomainRange().End.data()))) {
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
        return Buckets_.size();
    }

    // Returns histogram type.
    EHistogramValueType GetType() const {
        return ValueType_;
    }

    // Returns a number of elements in a bucket by the given `index`.
    ui64 GetNumElementsInBucket(ui32 index) const {
        Y_ENSURE(index < GetNumBuckets());
        return Buckets_[index];
    }

    // Returns domain range.
    TDomainRange GetDomainRange() const {
        return DomainRange_;
    }

private:
    // Returns binary size of the histogram.
    ui64 GetBinarySize(ui32 nBuckets) const;

    ui8 VersionNumber_ = 0;
    EHistogramValueType ValueType_;
    TDomainRange DomainRange_;
    TVector<ui64> Buckets_;
};

// This class represents a machinery to estimate a value in a histogram.
class TEqWidthHistogramEstimator {
public:
    explicit TEqWidthHistogramEstimator(std::shared_ptr<TEqWidthHistogram> histogram);

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
        const auto count = Histogram_->GetNumElementsInBucket(index);
        const TEqWidthHistogram::THistValue bucketWidth = Histogram_->GetBucketWidth<T>();
        // Assuming uniform distribution.
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            const ui64 width = LoadFrom<ui64>(bucketWidth.Value.data());
            return count / width;
        }
        const T width = LoadFrom<T>(bucketWidth.Value.data());
        return static_cast<ui64>(count / width);
    }

    // Returns the total number elements in histogram.
    // Could be used to adjust scale.
    ui64 GetNumElements() const {
        return PrefixSum_.back();
    }

private:
    template <typename T>
    ui64 EstimateOrEqual(T val, const TVector<ui64>& sumArray) const {
        const auto index = Histogram_->FindBucketIndex(val);
        return sumArray[index];
    }

    template <typename T>
    ui64 EstimateNotEqual(T val, const TVector<ui64>& sumArray) const {
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
} // namespace NKikimr
