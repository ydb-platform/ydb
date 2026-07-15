#include "column_statistic_eval.h"
#include "select_builder.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/library/yql/udfs/statistics_internal/all_agg_funcs.h>
#include <ydb/library/actors/core/log.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <numbers>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TSimpleColumnStatisticEval::TIntermediateState {
    using THLLState = NAggFuncs::THybridHyperLogLog<std::allocator>;

    THLLState Hll;
    std::optional<NYdb::TValue> Min;
    std::optional<NYdb::TValue> Max;

    TIntermediateState()
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
    if (builder.IsIntermediateAggregation()) {
        IntermediateState = std::make_unique<TIntermediateState>();
        CountDistinctSeq = builder.AddUDAFAggregation(columnName, "HLL");
    } else {
        CountDistinctSeq = builder.AddBuiltinAggregation(columnName, "HLL");
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
    Y_ENSURE(IntermediateState);

    {
        NYdb::TValueParser hllVal(aggColumns.at(CountDistinctSeq.value()));
        hllVal.OpenOptional();
        if (hllVal.IsNull()) {
            return;
        }
        TMemoryInput is(hllVal.GetBytes().data(), hllVal.GetBytes().size());
        auto hll = TIntermediateState::THLLState::Load(is);
        IntermediateState->Hll.Merge(hll);
    }

    if (MinSeq) {
        UpdateMinmax<std::less>(IntermediateState->Min, aggColumns.at(*MinSeq));
    }
    if (MaxSeq) {
        UpdateMinmax<std::greater>(IntermediateState->Max, aggColumns.at(*MaxSeq));
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

    if (IntermediateState) {
        Merge(aggColumns);
        result.SetCountDistinct(IntermediateState->Hll.Estimate());
        if (IntermediateState->Min) {
            result.MutableMin()->CopyFrom(IntermediateState->Min->GetProto());
        }
        if (IntermediateState->Max) {
            result.MutableMax()->CopyFrom(IntermediateState->Max->GetProto());
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

// Shared count-min sketch bookkeeping for both the single-column (TCMSEval) and multi-column
// (TMultiColumnCountMinSketchEval) evaluators: owns the sketch dimensions, the aggregation
// column slot, and (for cross-shard OLAP scans) the intermediate merge accumulator.
class TCountMinSketchState {
public:
    // current upper limit is 4_MB per columnar statistics
    static constexpr ui64 MAX_WIDTH = 131072;
    static constexpr ui64 MIN_WIDTH = 4096;
    static constexpr ui64 DEFAULT_DEPTH = 8;
    static constexpr double RELATIVE_ERROR = 10;

    explicit TCountMinSketchState(ui64 width) : Width(width) {}

    // Sketch width for the given per-element error, clamped to [MIN_WIDTH, MAX_WIDTH - 1].
    // (MAX_WIDTH - 1 leaves room for the other class variables' memory consumption.)
    static ui64 WidthForEps(double eps) {
        ui64 width = std::max((ui64)MIN_WIDTH, (ui64)ceil(std::numbers::e_v<double> / eps));
        return std::min(width, MAX_WIDTH - 1);
    }

    ui64 GetWidth() const { return Width; }
    ui64 GetDepth() const { return Depth; }

    size_t EstimateSize() const { return TCountMinSketch::StaticSize(Width, Depth); }

    // Records the aggregation column slot; for intermediate (cross-shard) aggregation also
    // allocates the accumulator that Merge() folds partial sketches into.
    void OnAdded(ui32 seq, bool intermediateAggregation) {
        Seq = seq;
        if (intermediateAggregation) {
            IntermediateState = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create(Width, Depth));
        }
    }

    void Merge(const TVector<NYdb::TValue>& aggColumns) {
        Y_ENSURE(IntermediateState);
        NYdb::TValueParser val(aggColumns.at(Seq.value()));
        val.OpenOptional();
        if (val.IsNull()) {
            return;
        }
        auto cms = std::unique_ptr<TCountMinSketch>(TCountMinSketch::FromString(
            val.GetBytes().data(), val.GetBytes().size()));
        *IntermediateState += *cms;
    }

    TString ExtractData(const TVector<NYdb::TValue>& aggColumns) {
        if (IntermediateState) {
            Merge(aggColumns);
            auto bytes = IntermediateState->AsStringBuf();
            return TString(bytes.data(), bytes.size());
        }

        NYdb::TValueParser val(aggColumns.at(Seq.value()));
        val.OpenOptional();
        if (!val.IsNull()) {
            const auto& bytes = val.GetBytes();
            return TString(bytes.data(), bytes.size());
        }
        auto defaultVal = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create(Width, Depth));
        auto bytes = defaultVal->AsStringBuf();
        return TString(bytes.data(), bytes.size());
    }

private:
    ui64 Width;
    ui64 Depth = DEFAULT_DEPTH;
    std::optional<ui32> Seq;
    std::unique_ptr<TCountMinSketch> IntermediateState;
};

class TCMSEval : public IStage2ColumnStatisticEval {
    TCountMinSketchState Sketch;

public:
    explicit TCMSEval(ui64 width) : Sketch(width) {}

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
            // Too many distinct values i.e. domain is close to be PK
            return TPtr{};
        }

        const double eps = (TCountMinSketchState::RELATIVE_ERROR - 1) * (1 + std::log10(n / ndv)) / ndv;
        return std::make_unique<TCMSEval>(TCountMinSketchState::WidthForEps(eps));
    }

    EStatType GetType() const final { return EStatType::COUNT_MIN_SKETCH; }

    size_t EstimateSize() const final { return Sketch.EstimateSize(); }

    void AddAggregations(const TString& columnName, TSelectBuilder& builder) final {
        Sketch.OnAdded(
            builder.AddUDAFAggregation(columnName, "CMS", Sketch.GetWidth(), Sketch.GetDepth()),
            builder.IsIntermediateAggregation());
    }

    void Merge(const TVector<NYdb::TValue>& aggColumns) final { Sketch.Merge(aggColumns); }

    TString ExtractData(const TVector<NYdb::TValue>& aggColumns) final { return Sketch.ExtractData(aggColumns); }
};

class TMultiColumnCountMinSketchEval : public IMultiColumnStatisticEval {
    std::vector<TString> ColumnNames;
    std::vector<ui32> ColumnIds;
    TCountMinSketchState Sketch;

public:
    TMultiColumnCountMinSketchEval(std::vector<TString> columnNames, std::vector<ui32> columnIds, ui64 width)
        : ColumnNames(std::move(columnNames))
        , ColumnIds(std::move(columnIds))
        , Sketch(width)
    {}

    static TPtr MaybeCreate(std::vector<TString> columnNames, std::vector<ui32> columnIds, ui64 rowCount) {
        if (rowCount == 0) {
            // Empty table
            return TPtr{};
        }

        // There is no distinct-tuple-count estimate for a multi-column tuple,
        // so size as if every tuple were unique (ndv == n).
        const double n = rowCount;
        const double eps = (TCountMinSketchState::RELATIVE_ERROR - 1) / n;
        const ui64 width = TCountMinSketchState::WidthForEps(eps);
        if (width >= TCountMinSketchState::MAX_WIDTH - 1) {
            YDB_LOG_WARN("[TMultiColumnCountMinSketchEval] Multi-column CMS sized at max width due to ndv==n assumption"
                    " (no NDV estimate is available for column tuples); sketch may provide low selectivity value"
                    " if the tuple is not actually high-cardinality",
                {"rowCount", rowCount},
                {"width", width});
        }
        return std::make_unique<TMultiColumnCountMinSketchEval>(
            std::move(columnNames), std::move(columnIds), width);
    }

    EStatType GetType() const final { return EStatType::COUNT_MIN_SKETCH; }

    const std::vector<ui32>& GetColumnIds() const final { return ColumnIds; }

    size_t EstimateSize() const final { return Sketch.EstimateSize(); }

    void AddAggregations(TSelectBuilder& builder) final {
        Sketch.OnAdded(
            builder.AddUDAFAggregationTuple(ColumnNames, "CMS", Sketch.GetWidth(), Sketch.GetDepth()),
            builder.IsIntermediateAggregation());
    }

    void Merge(const TVector<NYdb::TValue>& aggColumns) final { Sketch.Merge(aggColumns); }

    TString ExtractData(const TVector<NYdb::TValue>& aggColumns) final { return Sketch.ExtractData(aggColumns); }
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
    std::unique_ptr<TEqWidthHistogram> IntermediateState;

private:
    TEqWidthHistogram CreateEmpty() const {
        std::array<NYql::NUdf::TUnboxedValuePod, 3> params {
            NYql::NUdf::TUnboxedValuePod(NumBuckets),
            RangeStart.YqlVal,
            RangeEnd.YqlVal,
        };
        return NAggFuncs::TEWHAggFunc::CreateState(ColumnType.GetTypeId(), params);
    }

    // current upper limit is 4_MB per columnar statistics
    static constexpr ui32 MAX_BUCKETS = 524288;
    static constexpr ui32 MIN_BUCKETS = 1;

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

        const double cbrtN = std::cbrt(n);
        const double numBucketsEstimate = std::ceil(
            std::min(std::sqrt(n), cbrtN * n / ndv));
        ui32 numBuckets = (numBucketsEstimate <= std::numeric_limits<ui32>::max()
            ? numBucketsEstimate
            : std::numeric_limits<ui32>::max());
        if (numBuckets == 0) {
            numBuckets = MIN_BUCKETS;
        } else if (numBuckets > MAX_BUCKETS - 24) {
            // to accommodate for the other class variables' memory consumption.
            numBuckets = MAX_BUCKETS - 24;
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

    size_t EstimateSize() const final { return TEqWidthHistogram::GetBinarySize(NumBuckets); }

    void AddAggregations(const TString& columnName, TSelectBuilder& builder) final {
        Seq = builder.AddUDAFAggregation(columnName, "EWH", NumBuckets, RangeStart, RangeEnd);

        if (builder.IsIntermediateAggregation()) {
            IntermediateState = std::make_unique<TEqWidthHistogram>(CreateEmpty());
        }
    }

    void Merge(const TVector<NYdb::TValue>& aggColumns) final {
        Y_ENSURE(IntermediateState);

        NYdb::TValueParser val(aggColumns.at(Seq.value()));
        val.OpenOptional();
        if (val.IsNull()) {
            return;
        }

        const auto& bytes = val.GetBytes();
        TEqWidthHistogram merged(bytes.data(), bytes.size());
        IntermediateState->Aggregate(merged);
    }

    TString ExtractData(const TVector<NYdb::TValue>& aggColumns) final {
        if (IntermediateState) {
            Merge(aggColumns);
            return IntermediateState->Serialize();
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

TVector<EStatType> IMultiColumnStatisticEval::SupportedMultiColumnTypes() {
    return {
        EStatType::COUNT_MIN_SKETCH,
    };
}

IMultiColumnStatisticEval::TPtr IMultiColumnStatisticEval::MaybeCreate(
        EStatType statType,
        std::vector<TString> columnNames,
        std::vector<ui32> columnIds,
        ui64 rowCount) {
    switch (statType) {
    case EStatType::COUNT_MIN_SKETCH:
        return TMultiColumnCountMinSketchEval::MaybeCreate(
            std::move(columnNames), std::move(columnIds), rowCount);
    default:
        return TPtr{};
    }
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