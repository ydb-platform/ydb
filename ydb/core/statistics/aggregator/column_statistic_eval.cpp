#include "column_statistic_eval.h"
#include "select_builder.h"

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <numbers>

namespace NKikimr::NStat {

class TCMSEval : public IColumnStatisticEval {
    ui64 Width;
    ui64 Depth = DEFAULT_DEPTH;
    std::optional<ui32> Seq;

    // current upper limit is 4_MB per columnar statistics
    static constexpr ui64 MAX_WIDTH = 131072;
    static constexpr ui64 MIN_WIDTH = 4096;
    static constexpr ui64 DEFAULT_DEPTH = 8;
    static constexpr double NDV_N_RATIO = 0.8;
    static constexpr double RELATIVE_ERROR = 10;

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

        if (ndv >= NDV_N_RATIO * n) {
            return TPtr{};
        }

        const double eps = (RELATIVE_ERROR - 1) * (1 + std::log10(n / ndv)) / ndv;
        ui64 cmsWidth = std::max(MIN_WIDTH, (ui64)ceil(std::numbers::e_v<double> / eps));
        if (cmsWidth >= MAX_WIDTH) {
            cmsWidth = MAX_WIDTH - 1;
        }
        return std::make_unique<TCMSEval>(cmsWidth);
    }

    EStatType GetType() const final { return EStatType::COUNT_MIN_SKETCH; }

    size_t EstimateSize() const final { return TCountMinSketch::StaticSize(Width, Depth); }

    void AddAggregations(const TString& columnName, TSelectBuilder& builder) final {
        Seq = builder.AddUDAFAggregation(columnName, "CMS", Width, Depth);
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
    std::variant<ui64, i64, float, double> Val;

    template<typename T>
    explicit TBorder(T val) : Val(val) {}
};

class TEWHEval : public IColumnStatisticEval {
    ui32 NumBuckets;
    TBorder RangeStart;
    TBorder RangeEnd;

    std::optional<ui32> Seq;

    // current upper limit is 4_MB per columnar statistics
    static constexpr ui32 MAX_BUCKETS = 524288;
    static constexpr ui32 MIN_BUCKETS = 1;
    static constexpr double NDV_N_RATIO = 0.8;

public:
    TEWHEval(ui32 numBuckets, TBorder rangeStart, TBorder rangeEnd)
        : NumBuckets(numBuckets)
        , RangeStart(std::move(rangeStart))
        , RangeEnd(std::move(rangeEnd))
    {}

    static TMaybe<EHistogramValueType> GetHistogramType(NScheme::TTypeId typeId) {
        using namespace NYql;
        switch (typeId) {
#define MAKE_PRIMITIVE_VISITOR(type, layout)                                 \
        case NUdf::TDataType<type>::Id: {                                    \
            return GetHistogramValueType<layout>();                          \
        }
        KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_VISITOR)
#undef MAKE_PRIMITIVE_VISITOR
        default:
            return Nothing();
        }
    }

    static std::optional<std::pair<TBorder, TBorder>> GetDomainRange(
            EHistogramValueType type,
            const Ydb::Value& minVal, const Ydb::Value& maxVal, ui32* numBuckets) {
        auto getIntDomainRange = [&](auto min, auto max)
                -> std::optional<std::pair<TBorder, TBorder>> {
            if (min >= max) {
                return std::nullopt;
            }
            const ui64 dist = (ui64)max - (ui64)min;
            if (*numBuckets > dist) {
                *numBuckets = dist;
            }
            return { {TBorder(min), TBorder(max)} };
        };

        auto getFloatDomainRange = [&](auto min, auto max)
                -> std::optional<std::pair<TBorder, TBorder>> {
            if (!CmpLess(min, max)) {
                return std::nullopt;
            }
            return { {TBorder(min), TBorder(max)} };
        };

        switch (type) {
        default:
            Y_ENSURE(false, "Unexpected type: " << static_cast<ui8>(type));
        case EHistogramValueType::Uint8:
        case EHistogramValueType::Uint16:
        case EHistogramValueType::Uint32:
        case EHistogramValueType::Uint64: {
            const ui64 min = (type == EHistogramValueType::Uint64 ?
                minVal.uint64_value() : minVal.uint32_value());
            const ui64 max = (type == EHistogramValueType::Uint64 ?
                maxVal.uint64_value() : maxVal.uint32_value());
            return getIntDomainRange(min, max);
        }
        case EHistogramValueType::Int8:
        case EHistogramValueType::Int16:
        case EHistogramValueType::Int32:
        case EHistogramValueType::Int64: {
            const i64 min = (type == EHistogramValueType::Int64 ?
                minVal.int64_value() : minVal.int32_value());
            const i64 max = (type == EHistogramValueType::Int64 ?
                maxVal.int64_value() : maxVal.int32_value());
            return getIntDomainRange(min, max);
        }
        case EHistogramValueType::Float: {
            return getFloatDomainRange(minVal.float_value(), maxVal.float_value());
        }
        case EHistogramValueType::Double: {
            return getFloatDomainRange(minVal.double_value(), maxVal.double_value());
        }
        }
    }

    static TPtr MaybeCreate(
            const NKikimrStat::TSimpleColumnStatistics& simpleStats,
            const NScheme::TTypeInfo& type) {
        if (simpleStats.GetCount() == 0 || simpleStats.GetCountDistinct() == 0) {
            // Empty table
            return TPtr{};
        }

        TMaybe<EHistogramValueType> histType = GetHistogramType(type.GetTypeId());
        if (!histType) {
            // Unsupported column type
            return TPtr{};
        }

        const double n = simpleStats.GetCount();
        const double ndv = simpleStats.GetCountDistinct();

        //if (ndv >= NDV_N_RATIO * n) {
        //    // Too many distinct values
        //    return TPtr{};
        //}

        const double cbrtN = std::cbrt(n);
        const double numBucketsEstimate = std::ceil(
            std::min(std::sqrt(n), cbrtN * n / ndv));
        ui32 numBuckets = (numBucketsEstimate <= std::numeric_limits<ui32>::max()
            ? numBucketsEstimate
            : std::numeric_limits<ui32>::max());
        if (numBuckets == 0) {
            numBuckets = MIN_BUCKETS;
        } else if (numBuckets >= MAX_BUCKETS) {
            numBuckets = MAX_BUCKETS - 1;
        }

        auto domainRange = GetDomainRange(
            *histType, simpleStats.GetMin(), simpleStats.GetMax(), &numBuckets);
        if (!domainRange) {
            return TPtr{};
        }
        return std::make_unique<TEWHEval>(
            numBuckets, domainRange->first, domainRange->second);
    }

    EStatType GetType() const final { return EStatType::EQ_WIDTH_HISTOGRAM; }

    size_t EstimateSize() const final { return TEqWidthHistogram::GetBinarySize(NumBuckets); }

    void AddAggregations(const TString& columnName, TSelectBuilder& builder) final {
        Seq = builder.AddUDAFAggregation(columnName, "EWH", NumBuckets, RangeStart, RangeEnd);
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
    return TEWHEval::GetHistogramType(typeInfo.GetTypeId()).Defined();
}

} // NKikimr::NStat

template<>
void Out<NKikimr::NStat::TBorder>(IOutputStream& os, const NKikimr::NStat::TBorder& x) {
    // Serialize as a YQL literal with a correct type.
    struct TVisitor {
        IOutputStream& Os;
        void operator()(ui64 val) { Os << val << "ul"; }
        void operator()(i64 val) { Os << val << "l"; }
        void operator()(float val) { Os << "CAST(" << val << " AS Float)"; }
        void operator()(double val) { Os << "CAST(" << val << " AS Double)"; }
    };
    std::visit(TVisitor{os}, x.Val);
}
