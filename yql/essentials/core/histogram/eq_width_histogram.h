#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/types.h>
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
    float diff = std::fabs(left - right);
    float scale = std::max({1.0f, std::fabs(left), std::fabs(right)});
    return diff <= std::numeric_limits<float>::epsilon() * scale;
}
template <>
inline bool CmpEqual(double left, double right) {
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
inline std::optional<EHistogramValueType> GetHistogramValueType() {
    return std::nullopt;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<i8>() {
    return EHistogramValueType::Int8;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<i16>() {
    return EHistogramValueType::Int16;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<i32>() {
    return EHistogramValueType::Int32;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<i64>() {
    return EHistogramValueType::Int64;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<ui8>() {
    return EHistogramValueType::Uint8;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<ui16>() {
    return EHistogramValueType::Uint16;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<ui32>() {
    return EHistogramValueType::Uint32;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<ui64>() {
    return EHistogramValueType::Uint64;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<float>() {
    return EHistogramValueType::Float;
}

template <>
inline std::optional<EHistogramValueType> GetHistogramValueType<double>() {
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
    TEqWidthHistogram(ui32 numBuckets = 1, EHistogramValueType type = EHistogramValueType::Int32);
    // From serialized data.
    TEqWidthHistogram(const char* str, size_t size);

    // Adds the given `val` to a histogram.
    template <typename T>
    void AddElement(T val) {
        const auto index = FindBucketIndex(val);
        // The given `index` in range [0, numBuckets - 1].
        const T bucketValue = LoadFrom<T>(Buckets_[index].Start);
        if (!index || (CmpEqual<T>(bucketValue, val) || CmpLess<T>(bucketValue, val))) {
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
            auto it = start + (end - start + 1) / 2;
            if (CmpLess<T>(val, LoadFrom<T>(Buckets_[it].Start))) {
                end = it - 1;
            } else {
                start = it;
            }
        }
        return start;
    }

    // Returns a number of buckets in a histogram.
    ui32 GetNumBuckets() const {
        return Buckets_.size();
    }

    template <typename T>
    ui32 GetBucketWidth() const {
        if (ValueType_ == EHistogramValueType::Float || ValueType_ == EHistogramValueType::Double) {
            return 1;
        }
        if (GetNumBuckets() == 1) {
            auto val = LoadFrom<T>(Buckets_.front().Start);
            // to avoid returning zero value and casting negative values
            return val > 0 ? static_cast<ui32>(val) : 1;
        } else {
            return static_cast<ui32>(LoadFrom<T>(Buckets_[1].Start) - LoadFrom<T>(Buckets_[0].Start));
        }
        T bucketIndex = std::floor((val - domainStart) / LoadFrom<T>(bucketWidth.Value));
        bucketIndex = std::min<T>(GetNumBuckets() - 1, bucketIndex);
        return static_cast<ui32>(bucketIndex);
    }

    // Returns histogram type.
    EHistogramValueType GetType() const {
        return ValueType_;
    }

    // Returns a number of elements in a bucket by the given `index`.
    ui64 GetNumElementsInBucket(ui32 index) const {
        Y_ENSURE(index < GetNumBuckets());
        return Buckets_[index].Count;
    }

    // Returns the start boundary value of a bucket by the given `index`.
    template <typename T>
    T GetBucketStartBoundary(ui32 index) const {
        Y_ENSURE(index < GetNumBuckets());
        return LoadFrom<T>(Buckets_[index].Start);
    }

    // Initializes buckets with a given `range`.
    template <typename T>
    void InitializeBuckets(T rangeStart, T rangeEnd) {
        TEqWidthHistogram::TBucketRange range;
        StoreTo<T>(range.Start, rangeStart);
        StoreTo<T>(range.End, rangeEnd);
        const T start = LoadFrom<T>(range.Start);
        const T end = LoadFrom<T>(range.End);
        Y_ENSURE(CmpLess<T>(start, end));
        const T rangeLen = end - start;
        T bucketWidth = rangeLen / static_cast<T>(GetNumBuckets());
        Y_ENSURE(start + bucketWidth > start); // non-zero positive width of each bucket
        WriteUnaligned<ui8[EqWidthHistogramBucketStorageSize]>(Buckets_[0].Start, range.Start);
        for (ui32 i = 1; i < GetNumBuckets(); ++i) {
            const T prevStart = LoadFrom<T>(Buckets_[i - 1].Start);
            StoreTo<T>(Buckets_[i].Start, prevStart + bucketWidth);
        }
    }

    // Seriailizes to a binary representation
    TString Serialize() const;

    void Aggregate(const TEqWidthHistogram& other);

private:
    template <typename T>
    bool BucketsEqual(const TEqWidthHistogram& other) {
        if (GetNumBuckets() != other.GetNumBuckets()) {
            return false;
        } else if (ValueType_ != other.GetType()) {
            return false;
        } else if (!CmpEqual<T>(LoadFrom<T>(DomainRange_.Start), LoadFrom<T>(other.GetDomainRange().Start))) {
            return false;
        } else if (!CmpEqual<T>(LoadFrom<T>(DomainRange_.End), LoadFrom<T>(other.GetDomainRange().End))) {
            return false;
        }
        for (ui32 i = 0; i < Buckets_.size(); ++i) {
            if (!CmpEqual<T>(LoadFrom<T>(Buckets_[i].Start), other.GetBucketStartBoundary<T>(i))) {
                return false;
            }
        }
        return true;
    }

    // Returns binary size of the histogram.
    ui64 GetBinarySize(ui32 nBuckets) const;
    EHistogramValueType ValueType_;
    TDomainRange DomainRange_;
    TVector<ui64> Buckets_;
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
        const auto count = Histogram_->GetNumElementsInBucket(index);
        const TEqWidthHistogram::THistValue bucketWidth = Histogram_->GetBucketWidth<T>();
        // Assuming uniform distribution.
        if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
            const ui64 width = LoadFrom<ui64>(bucketWidth.Value);
            return count / width;
        }
        const T width = LoadFrom<T>(bucketWidth.Value);
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
