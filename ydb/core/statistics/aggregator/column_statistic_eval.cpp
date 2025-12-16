#include "column_statistic_eval.h"
#include "select_builder.h"

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <numbers>

namespace NKikimr::NStat {

class TCMSEval : public IColumnStatisticEval {
    ui64 Width;
    ui64 Depth = DEFAULT_DEPTH;
    std::optional<ui32> Seq;

    static constexpr ui64 MIN_WIDTH = 256;
    static constexpr ui64 DEFAULT_DEPTH = 8;

public:
    TCMSEval(ui64 width) : Width(width) {}

    static TPtr MaybeCreate(
            const NKikimrStat::TSimpleColumnStatistics& simpleStats,
            const NScheme::TTypeInfo&) {
        if (simpleStats.GetCount() == 0 || simpleStats.GetCountDistinct() == 0) {
            // Empty table
            return TPtr{};
        }

        const double n = simpleStats.GetCount();
        const double ndv = simpleStats.GetCountDistinct();

        if (ndv >= 0.8 * n) {
            return TPtr{};
        }

        const double c = 10;
        const double eps = (c - 1) * (1 + std::log10(n / ndv)) / ndv;
        const ui64 cmsWidth = std::max((ui64)MIN_WIDTH, (ui64)ceil(std::numbers::e_v<double> / eps));
        return std::make_unique<TCMSEval>(cmsWidth);
    }

    EStatType GetType() const final { return EStatType::COUNT_MIN_SKETCH; }

    size_t EstimateSize() const final { return Width * Depth * sizeof(ui32); }

    void AddAggregations(const TString& columnName, TSelectBuilder& builder) final {
        Seq = builder.AddUDAFAggregation(columnName, "CountMinSketch", Width, Depth);
    }

    TString ExtractData(const TVector<NYdb::TValue>& aggColumns) const final {
        NYdb::TValueParser val(aggColumns.at(Seq.value()));
        val.OpenOptional();
        if (!val.IsNull()) {
            const auto& bytes = val.GetBytes();
            return TString(bytes.data(), bytes.size());
        } else {
            auto defaultVal = std::unique_ptr<TCountMinSketch>(
                TCountMinSketch::Create(Width, Depth));
            auto bytes = defaultVal->AsStringBuf();
            return TString(bytes.data(), bytes.size());
        }
    }
};

struct TBorder {
    std::variant<ui64, i64, double> Val;

    template<typename T>
    explicit TBorder(T val) : Val(val) {}

    static std::pair<TBorder, TBorder> GetBucketRange(
            EHistogramValueType type,
            const Ydb::Value& minVal, const Ydb::Value& maxVal, ui32* numBuckets) {
        switch (type) {
        default:
            Y_ENSURE(false, "Unexpected type: " << static_cast<ui8>(type));
        case EHistogramValueType::Uint16:
        case EHistogramValueType::Uint32:
        case EHistogramValueType::Uint64: {
            const ui64 min = (type == EHistogramValueType::Uint64 ?
                minVal.uint64_value() : minVal.uint32_value());
            const ui64 max = (type == EHistogramValueType::Uint64 ?
                maxVal.uint64_value() : maxVal.uint32_value());
            if (min >= max || *numBuckets <= 1) {
                *numBuckets = 1;
                return {TBorder(min), TBorder(min)};
            }
            const ui64 dist = max - min;
            if (*numBuckets > dist) {
                *numBuckets = dist + 1;
            }
            const ui64 bucketLen = dist / (*numBuckets - 1);
            return {TBorder(min), TBorder(min + bucketLen)};
        }
        case EHistogramValueType::Int16:
        case EHistogramValueType::Int32:
        case EHistogramValueType::Int64: {
            const i64 min = (type == EHistogramValueType::Int64 ?
                minVal.int64_value() : minVal.int32_value());
            const i64 max = (type == EHistogramValueType::Int64 ?
                maxVal.int64_value() : maxVal.int32_value());
            if (min >= max || *numBuckets <= 1) {
                *numBuckets = 1;
                return {TBorder(min), TBorder(min)};
            }
            const ui64 dist = -((ui64)(-max) + (ui64)min);
            if (*numBuckets > dist) {
                *numBuckets = dist + 1;
            }
            const ui64 bucketLen = dist / (*numBuckets - 1);
            return {TBorder(min), TBorder((i64)(min + bucketLen))};
        }
        case EHistogramValueType::Double: {
            const double min = minVal.double_value();
            const double max = maxVal.double_value();
            if (min >= max || *numBuckets <= 1) {
                *numBuckets = 1;
                return {TBorder(min), TBorder(min)};
            }
            const double bucketLen = (max - min) / (*numBuckets - 1);
            return {TBorder(min), TBorder(min + bucketLen)};
        }
        }
    }
};

class TEWHEval : public IColumnStatisticEval {
    ui64 NumBuckets;
    TBorder RangeStart;
    TBorder RangeEnd;

    std::optional<ui32> Seq;

public:
    TEWHEval(ui64 numBuckets, TBorder rangeStart, TBorder rangeEnd)
        : NumBuckets(numBuckets)
        , RangeStart(std::move(rangeStart))
        , RangeEnd(std::move(rangeEnd))
    {}

    static std::optional<EHistogramValueType> GetHistogramType(NScheme::TTypeId typeId) {
        using namespace NYql;
        switch (typeId) {
#define MAKE_PRIMITIVE_VISITOR(type, layout)                                 \
        case NUdf::TDataType<type>::Id: {                                    \
            return GetHistogramValueType<layout>();                          \
        }
        KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_VISITOR)
#undef MAKE_PRIMITIVE_VISITOR
        default:
            return std::nullopt;
        }
    }

    static TPtr MaybeCreate(
            const NKikimrStat::TSimpleColumnStatistics& simpleStats,
            const NScheme::TTypeInfo& type) {
        if (simpleStats.GetCount() == 0 || simpleStats.GetCountDistinct() == 0) {
            // Empty table
            return TPtr{};
        }

        std::optional<EHistogramValueType> histType = GetHistogramType(type.GetTypeId());
        if (!histType) {
            // Unsupported column type
            return TPtr{};
        }

        const double n = simpleStats.GetCount();
        const double ndv = simpleStats.GetCountDistinct();

        if (ndv >= 0.8 * n) {
            // Too many distinct values
            return TPtr{};
        }

        const double cbrtN = std::cbrt(n);
        const double numBucketsEstimate = std::ceil(
            std::min(std::sqrt(n), cbrtN * n / ndv));
        ui32 numBuckets = (numBucketsEstimate <= std::numeric_limits<ui32>::max()
            ? numBucketsEstimate
            : std::numeric_limits<ui32>::max());

        auto [rangeStart, rangeEnd] = TBorder::GetBucketRange(
            *histType, simpleStats.GetMin(), simpleStats.GetMax(), &numBuckets);
        return std::make_unique<TEWHEval>(numBuckets, rangeStart, rangeEnd);
    }

    EStatType GetType() const final { return EStatType::EQ_WIDTH_HISTOGRAM; }

    size_t EstimateSize() const final { return NumBuckets * sizeof(TEqWidthHistogram::TBucket); }

    void AddAggregations(const TString& columnName, TSelectBuilder& builder) final {
        Seq = builder.AddUDAFAggregation(
            columnName, "EquiWidthHistogram", NumBuckets, RangeStart, RangeEnd);
    }

    TString ExtractData(const TVector<NYdb::TValue>& aggColumns) const final {
        NYdb::TValueParser val(aggColumns.at(Seq.value()));
        val.OpenOptional();
        Y_ENSURE(!val.IsNull()); // Makes no sense to calculate histograms for empty tables.
        const auto& bytes = val.GetBytes();
        return TString(bytes.data(), bytes.size());
    }
};

TVector<EStatType> IColumnStatisticEval::SupportedTypes() {
    return {
        EStatType::COUNT_MIN_SKETCH,
        EStatType::EQ_WIDTH_HISTOGRAM,
    };
}

IColumnStatisticEval::TPtr IColumnStatisticEval::MaybeCreate(
        EStatType statType,
        const NKikimrStat::TSimpleColumnStatistics& simpleStats,
        const NScheme::TTypeInfo& columnType) {
    switch (statType) {
    case EStatType::COUNT_MIN_SKETCH:
        return TCMSEval::MaybeCreate(simpleStats, columnType);
    case EStatType::EQ_WIDTH_HISTOGRAM:
        return TEWHEval::MaybeCreate(simpleStats, columnType);
    default:
        return TPtr{};
    }
}

bool IColumnStatisticEval::AreMinMaxNeeded(const NScheme::TTypeInfo& typeInfo) {
    return TEWHEval::GetHistogramType(typeInfo.GetTypeId()).has_value();
}

} // NKikimr::NStat

template<>
void Out<NKikimr::NStat::TBorder>(IOutputStream& os, const NKikimr::NStat::TBorder& x) {
    struct TVisitor {
        IOutputStream& Os;
        void operator()(ui64 val) { Os << val; }
        void operator()(i64 val) { Os << val; }
        void operator()(double val) { Os << val; }
    };
    std::visit(TVisitor{os}, x.Val);
}
