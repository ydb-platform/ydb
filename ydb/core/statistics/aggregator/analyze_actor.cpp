#include "analyze_actor.h"
#include "select_builder.h"

#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/yql/udfs/statistics_internal/all_agg_funcs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>
#include <util/generic/size_literals.h>
#include <util/string/vector.h>

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

class TAnalyzeActor::TScanActor : public TQueryBase {
public:
    TScanActor(TActorId parent, TString Database, TString query, size_t columnCount)
        : TQueryBase(NKikimrServices::STATISTICS, {}, std::move(Database))
        , Parent(parent)
        , Query(std::move(query))
        , ColumnCount(columnCount)
    {}

    void OnRunQuery() override {
        RunStreamQuery(Query);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser result(std::move(resultSet));
        if (!result.TryNextRow()) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "unexpected query result: expected row");
            return;
        }
        if (result.ColumnsCount() != ColumnCount) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR,
                TStringBuilder() << "unexpected query result: expected "
                << ColumnCount << " columns, got " << result.ColumnsCount());
            return;
        }

        TVector<NYdb::TValue> resultColumns;
        resultColumns.reserve(ColumnCount);
        for (size_t i = 0; i < ColumnCount; ++i) {
            resultColumns.push_back(result.GetValue(i));
        }

        auto response = std::make_unique<TEvPrivate::TEvAnalyzeScanResult>(
            std::move(resultColumns));
        Send(Parent, response.release());
        ResponseSent = true;
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Y_ABORT_UNLESS(ResponseSent);
            return;
        }

        Y_ABORT_UNLESS(!ResponseSent);
        auto response = std::make_unique<TEvPrivate::TEvAnalyzeScanResult>(
            status, std::move(issues));
        Send(Parent, response.release());
        ResponseSent = true;
    }

private:
    TActorId Parent;
    TString Query;
    size_t ColumnCount;
    bool ResponseSent = false;
};

void TAnalyzeActor::Bootstrap() {
    Become(&TThis::StateNavigate);

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    TNavigate::TEntry entry;
    entry.TableId = PathId;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
    entry.Operation = TNavigate::OpTable;

    auto request = std::make_unique<TNavigate>();
    request->DatabaseName = DatabaseName;
    request->ResultSet.emplace_back(entry);

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
}

void TAnalyzeActor::FinishWithFailure(TEvStatistics::TEvFinishTraversal::EStatus status) {
    auto response = std::make_unique<TEvStatistics::TEvFinishTraversal>(status);
    Send(Parent, response.release());
    PassAway();
}

void TAnalyzeActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    const auto& request = *ev->Get()->Request;
    Y_ABORT_UNLESS(request.ResultSet.size() == 1);
    const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = request.ResultSet.front();

    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        SA_LOG_W("Navigate request failed with " << entry.Status
            << ", operationId: " << OperationId
            << ", PathId: " << PathId
            << ", DatabaseName: " << DatabaseName);

        FinishWithFailure(
            entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown
            ? TEvStatistics::TEvFinishTraversal::EStatus::TableNotFound
            : TEvStatistics::TEvFinishTraversal::EStatus::InternalError);
        return;
    }

    // TODO: escape table path
    TableName = "/" + JoinVectorIntoString(entry.Path, "/");

    THashMap<ui32, TSysTables::TTableColumnInfo> tag2Column;
    for (const auto& col : entry.Columns) {
        tag2Column[col.second.Id] = col.second;
    }

    TSelectBuilder stage1Builder;

    CountSeq = stage1Builder.AddBuiltinAggregation({}, "count");

    auto addColumn = [&](const TSysTables::TTableColumnInfo& colInfo) {
        auto& col = Columns.emplace_back(colInfo.Id, colInfo.PType, colInfo.PTypeMod, colInfo.Name);
        // TODO: escape column names
        col.CountDistinctSeq = stage1Builder.AddBuiltinAggregation(colInfo.Name, "HLL");
        if (TEWHEval::GetHistogramType(colInfo.PType.GetTypeId())) {
            col.MinSeq = stage1Builder.AddBuiltinAggregation(colInfo.Name, "min");
            col.MaxSeq = stage1Builder.AddBuiltinAggregation(colInfo.Name, "max");
        }
    };
    if (!RequestedColumnTags.empty()) {
        for (const auto& colTag : RequestedColumnTags) {
            auto colIt = tag2Column.find(colTag);
            if (colIt == tag2Column.end()) {
                // Column probably already dropped, skip it.
                continue;
            }
            addColumn(colIt->second);
        }
    } else {
        for (const auto& [tag, info] : tag2Column) {
            addColumn(info);
        }
    }

    if (Columns.empty()) {
        // All requested columns were already dropped. Send empty response right away.
        auto response = std::make_unique<TEvStatistics::TEvFinishTraversal>(std::move(Results));
        Send(Parent, response.release());
        PassAway();
        return;
    }

    Become(&TThis::StateQueryStage1);
    auto actor = std::make_unique<TScanActor>(
        SelfId(), DatabaseName, stage1Builder.Build(TableName), stage1Builder.ColumnCount());
    Register(actor.release());
}

NKikimrStat::TSimpleColumnStatistics TAnalyzeActor::TColumnDesc::ExtractSimpleStats(
        ui64 count, const TVector<NYdb::TValue>& aggColumns) const {
    NKikimrStat::TSimpleColumnStatistics result;
    result.SetCount(count);

    NYdb::TValueParser hllVal(aggColumns.at(CountDistinctSeq.value()));
    ui64 countDistinct = hllVal.GetOptionalUint64().value_or(0);
    result.SetCountDistinct(countDistinct);

    result.SetTypeId(Type.GetTypeId());
    if (NScheme::NTypeIds::IsParametrizedType(Type.GetTypeId())) {
        NScheme::ProtoFromTypeInfo(Type, PgTypeMod, *result.MutableTypeInfo());
    }

    if (MinSeq) {
        result.MutableMin()->CopyFrom(aggColumns.at(*MinSeq).GetProto());
    }
    if (MaxSeq) {
        result.MutableMax()->CopyFrom(aggColumns.at(*MaxSeq).GetProto());
    }

    return result;
}

void TAnalyzeActor::HandleStage1(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev) {
    const auto& result = *ev->Get();
    if (result.Status != Ydb::StatusIds::SUCCESS) {
        FinishWithFailure(TEvStatistics::TEvFinishTraversal::EStatus::InternalError);
        return;
    }

    NYdb::TValueParser val(result.AggColumns.at(CountSeq.value()));
    ui64 rowCount = val.GetUint64();

    auto supportedStatTypes = IColumnStatisticEval::SupportedTypes();

    TSelectBuilder stage2Builder;
    for (auto& col : Columns) {
        auto simpleStats = col.ExtractSimpleStats(rowCount, result.AggColumns);
        Results.emplace_back(
            col.Tag,
            NKikimr::NStat::SIMPLE_COLUMN,
            simpleStats.SerializeAsString());

        for (auto type : supportedStatTypes) {
            auto statEval = IColumnStatisticEval::MaybeCreate(type, simpleStats, col.Type);
            if (!statEval) {
                continue;
            }
            if (statEval->EstimateSize() >= 4_MB) {
                continue;
            }
            statEval->AddAggregations(col.Name, stage2Builder);
            col.Statistics.push_back(std::move(statEval));
        }
    }

    if (stage2Builder.ColumnCount() == 0) {
        // Second stage is not needed, return results right away.
        auto response = std::make_unique<TEvStatistics::TEvFinishTraversal>(std::move(Results));
        Send(Parent, response.release());
        PassAway();
        return;
    }

    Become(&TThis::StateQueryStage2);
    auto actor = std::make_unique<TScanActor>(
        SelfId(), DatabaseName, stage2Builder.Build(TableName), stage2Builder.ColumnCount());
    Register(actor.release());
}

void TAnalyzeActor::HandleStage2(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev) {
    const auto& result = *ev->Get();
    if (result.Status != Ydb::StatusIds::SUCCESS) {
        FinishWithFailure(TEvStatistics::TEvFinishTraversal::EStatus::InternalError);
        return;
    }

    for (const auto& col : Columns) {
        for (const auto& statEval : col.Statistics) {
            Results.emplace_back(
                col.Tag,
                statEval->GetType(),
                statEval->ExtractData(result.AggColumns));
        }
    }

    auto response = std::make_unique<TEvStatistics::TEvFinishTraversal>(std::move(Results));
    Send(Parent, response.release());
    PassAway();
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
