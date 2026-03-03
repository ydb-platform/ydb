#include "column_statistic_eval.h"
#include "select_builder.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/library/yql/udfs/statistics_internal/all_agg_funcs.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <numbers>

namespace NKikimr::NStat {

struct TSimpleColumnStatisticEval::TState {
    using THLLState = NAggFuncs::THybridHyperLogLog<std::allocator>;

    THLLState Hll;
    std::optional<NYdb::TValue> Min;
    std::optional<NYdb::TValue> Max;

    TState()
        : Hll(THLLState::Create(NAggFuncs::THLLAggFunc::DEFAULT_PRECISION))
    {}
};

TSimpleColumnStatisticEval::TSimpleColumnStatisticEval(NScheme::TTypeInfo type, TString pgTypeMod)
    : Type(std::move(type))
    , PgTypeMod(std::move(pgTypeMod))
{}

TSimpleColumnStatisticEval::~TSimpleColumnStatisticEval() = default;

EStatType TSimpleColumnStatisticEval::GetType() const {
    return EStatType::SIMPLE_COLUMN;
}

size_t TSimpleColumnStatisticEval::EstimateSize() const {
    return 1u << NAggFuncs::THLLAggFunc::DEFAULT_PRECISION;
}

void TSimpleColumnStatisticEval::AddAggregations(
        const TString& columnName, TSelectBuilder& builder) {
    if (builder.Final()) {
        CountDistinctSeq = builder.AddBuiltinAggregation(columnName, "HLL");
    } else {
        State = std::make_unique<TState>();
        CountDistinctSeq = builder.AddUDAFAggregation(columnName, "HLL");
    }

    if (IStage2ColumnStatisticEval::AreMinMaxNeeded(Type)) {
        MinSeq = builder.AddBuiltinAggregation(columnName, "min");
        MaxSeq = builder.AddBuiltinAggregation(columnName, "max");
    }
}

template<template<typename> class TCmp>
static void UpdateMinmax(std::optional<NYdb::TValue>& left, const NYdb::TValue& right) {
    if (!left) {
        left = right;
        return;
    }

    TCmp cmp;
    auto& leftProto = left->GetProto();
    const auto& rightProto = right.GetProto();
    switch (leftProto.value_case()) {
    case Ydb::Value::kInt32Value:
        if (cmp(rightProto.int32_value(), leftProto.int32_value())) {
            leftProto.set_int32_value(rightProto.int32_value());
        }
        break;
    case Ydb::Value::kUint32Value:
        if (cmp(rightProto.uint32_value(), leftProto.uint32_value())) {
            leftProto.set_uint32_value(rightProto.uint32_value());
        }
        break;
    case Ydb::Value::kInt64Value:
        if (cmp(rightProto.int64_value(), leftProto.int64_value())) {
            leftProto.set_int64_value(rightProto.int64_value());
        }
        break;
    case Ydb::Value::kUint64Value:
        if (cmp(rightProto.uint64_value(), leftProto.uint64_value())) {
            leftProto.set_uint64_value(rightProto.uint64_value());
        }
        break;
    case Ydb::Value::kFloatValue:
        if (cmp(rightProto.float_value(), leftProto.float_value())) {
            leftProto.set_float_value(rightProto.float_value());
        }
        break;
    case Ydb::Value::kDoubleValue:
        if (cmp(rightProto.double_value(), leftProto.double_value())) {
            leftProto.set_double_value(rightProto.double_value());
        }
        break;
    default:
        Y_ENSURE(false, "unexpected value type");
    }
};

void TSimpleColumnStatisticEval::Merge(const TVector<NYdb::TValue>& aggColumns) {
    Y_ENSURE(State);

    {
        NYdb::TValueParser hllVal(aggColumns.at(CountDistinctSeq.value()));
        hllVal.OpenOptional();
        if (hllVal.IsNull()) {
            return;
        }
        TMemoryInput is(hllVal.GetBytes().data(), hllVal.GetBytes().size());
        auto hll = TState::THLLState::Load(is);
        State->Hll.Merge(hll);
    }

    if (MinSeq) {
        UpdateMinmax<std::less>(State->Min, aggColumns.at(*MinSeq));
    }
    if (MaxSeq) {
        UpdateMinmax<std::greater>(State->Max, aggColumns.at(*MaxSeq));
    }
}

NKikimrStat::TSimpleColumnStatistics TSimpleColumnStatisticEval::Extract(
        ui64 rowCount, const TVector<NYdb::TValue>& aggColumns) {
    NKikimrStat::TSimpleColumnStatistics result;
    result.SetCount(rowCount);

    result.SetTypeId(Type.GetTypeId());
    if (NScheme::NTypeIds::IsParametrizedType(Type.GetTypeId())) {
        NScheme::ProtoFromTypeInfo(Type, PgTypeMod, *result.MutableTypeInfo());
    }

    if (State) {
        Merge(aggColumns);
        result.SetCountDistinct(State->Hll.Estimate());
        if (State->Min) {
            result.MutableMin()->CopyFrom(State->Min->GetProto());
        }
        if (State->Max) {
            result.MutableMax()->CopyFrom(State->Max->GetProto());
        }
    } else {
        NYdb::TValueParser hllVal(aggColumns.at(CountDistinctSeq.value()));
        ui64 countDistinct = hllVal.GetOptionalUint64().value_or(0);
        result.SetCountDistinct(countDistinct);

        if (MinSeq) {
            result.MutableMin()->CopyFrom(aggColumns.at(*MinSeq).GetProto());
        }
        if (MaxSeq) {
            result.MutableMax()->CopyFrom(aggColumns.at(*MaxSeq).GetProto());
        }
    }

    return result;
}

class TCMSEval : public IStage2ColumnStatisticEval {
    ui64 Width;
    ui64 Depth = DEFAULT_DEPTH;

    std::optional<ui32> Seq;
    std::unique_ptr<TCountMinSketch> State;

    static constexpr ui64 MIN_WIDTH = 4096;
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
        Seq = builder.AddUDAFAggregation(columnName, "CMS", Width, Depth);

        if (!builder.Final()) {
            State = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create(Width, Depth));
        }
    }

    void Merge(const TVector<NYdb::TValue>& aggColumns) final {
        Y_ENSURE(State);
        NYdb::TValueParser val(aggColumns.at(Seq.value()));
        val.OpenOptional();
        if (val.IsNull()) {
            return;
        }
        auto cms = std::unique_ptr<TCountMinSketch>(TCountMinSketch::FromString(
            val.GetBytes().data(), val.GetBytes().size()));
        *State += *cms;
    }

    TString ExtractData(const TVector<NYdb::TValue>& aggColumns) final {
        if (State) {
            Merge(aggColumns);
            auto bytes = State->AsStringBuf();
            return TString(bytes.data(), bytes.size());
        }

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
    NYql::NUdf::TUnboxedValuePod YqlVal;

    template<typename T>
    explicit TBorder(T val) : Val(val), YqlVal(val) {}
};

class TEWHEval : public IStage2ColumnStatisticEval {
    NScheme::TTypeInfo ColumnType;

    ui32 NumBuckets;
    TBorder RangeStart;
    TBorder RangeEnd;

    std::optional<ui32> Seq;
    std::unique_ptr<TEqWidthHistogram> State;

private:
    TEqWidthHistogram CreateEmpty() const {
        std::array<NYql::NUdf::TUnboxedValuePod, 3> params {
            NYql::NUdf::TUnboxedValuePod(NumBuckets),
            RangeStart.YqlVal,
            RangeEnd.YqlVal,
        };
        return NAggFuncs::TEWHAggFunc::CreateState(ColumnType.GetTypeId(), params);
    }

public:
    TEWHEval(NScheme::TTypeInfo columnType, ui32 numBuckets, TBorder rangeStart, TBorder rangeEnd)
        : ColumnType(columnType)
        , NumBuckets(numBuckets)
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
        if (numBuckets == 0) {
            numBuckets = 1;
        }

        auto domainRange = GetDomainRange(
            *histType, simpleStats.GetMin(), simpleStats.GetMax(), &numBuckets);
        if (!domainRange) {
            return TPtr{};
        }
        return std::make_unique<TEWHEval>(
            type, numBuckets, domainRange->first, domainRange->second);
    }

    EStatType GetType() const final { return EStatType::EQ_WIDTH_HISTOGRAM; }

    size_t EstimateSize() const final { return NumBuckets * sizeof(ui64); }

    void AddAggregations(const TString& columnName, TSelectBuilder& builder) final {
        Seq = builder.AddUDAFAggregation(columnName, "EWH", NumBuckets, RangeStart, RangeEnd);

        if (!builder.Final()) {
            State = std::make_unique<TEqWidthHistogram>(CreateEmpty());
        }
    }

    void Merge(const TVector<NYdb::TValue>& aggColumns) final {
        Y_ENSURE(State);

        NYdb::TValueParser val(aggColumns.at(Seq.value()));
        val.OpenOptional();
        if (val.IsNull()) {
            return;
        }

        const auto& bytes = val.GetBytes();
        TEqWidthHistogram merged(bytes.data(), bytes.size());
        State->Aggregate(merged);
    }

    TString ExtractData(const TVector<NYdb::TValue>& aggColumns) final {
        if (State) {
            Merge(aggColumns);
            return State->Serialize();
        }

        NYdb::TValueParser val(aggColumns.at(Seq.value()));
        val.OpenOptional();
        if (!val.IsNull()) {
            const auto& bytes = val.GetBytes();
            return TString(bytes.data(), bytes.size());
        } else {
            auto empty = CreateEmpty();
            return empty.Serialize();
        }
    }
};

TVector<EStatType> IStage2ColumnStatisticEval::SupportedTypes() {
    return {
        EStatType::COUNT_MIN_SKETCH,
        EStatType::EQ_WIDTH_HISTOGRAM,
    };
}

IStage2ColumnStatisticEval::TPtr IStage2ColumnStatisticEval::MaybeCreate(
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

bool IStage2ColumnStatisticEval::AreMinMaxNeeded(const NScheme::TTypeInfo& typeInfo) {
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
