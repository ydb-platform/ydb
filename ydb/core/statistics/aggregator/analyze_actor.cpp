#include "analyze_actor.h"
#include "select_builder.h"

#include <ydb/library/query_actor/query_actor.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>
#include <util/generic/size_literals.h>
#include <util/string/vector.h>

namespace NKikimr::NStat {


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

        if (!ResponseSent) {
            auto response = std::make_unique<TEvPrivate::TEvAnalyzeScanResult>(
                status, std::move(issues));
            Send(Parent, response.release());
            ResponseSent = true;
        }
    }

private:
    STFUNC(StateFunc) final {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoison, Handle);
            default:
                TQueryBase::StateFunc(ev);
        }
    }

    void Handle(TEvents::TEvPoison::TPtr&) {
        SA_LOG_D("Got TEvPoison");
        Finish(Ydb::StatusIds::ABORTED, "Query aborted");
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

void TAnalyzeActor::FinishWithFailure(
        TEvStatistics::TEvFinishTraversal::EStatus status,
        NYql::TIssue issue) {
    auto response = std::make_unique<TEvStatistics::TEvFinishTraversal>(status);
    if (status != TEvStatistics::TEvFinishTraversal::EStatus::Success) {
        TStringBuilder errMsg;
        errMsg << "Analyzing table ";
        if (!TableName.empty()) {
            errMsg << TableName;
        } else {
            errMsg << "id: " << PathId.LocalPathId;
        }
        NYql::TIssue error(errMsg);
        error.AddSubIssue(MakeIntrusive<NYql::TIssue>(std::move(issue)));
        response->Issues.AddIssue(std::move(error));
    }
    Send(Parent, response.release());
    PassAway();
}

void TAnalyzeActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    const auto& request = *ev->Get()->Request;
    Y_ABORT_UNLESS(request.ResultSet.size() == 1);
    const NSchemeCache::TSchemeCacheNavigate::TEntry& entry = request.ResultSet.front();

    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        SA_LOG_W("Navigate request failed with " << entry.Status
            << ", operationId: " << OperationId.Quote()
            << ", PathId: " << PathId
            << ", DatabaseName: " << DatabaseName);

        FinishWithFailure(
            entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown
            ? TEvStatistics::TEvFinishTraversal::EStatus::TableNotFound
            : TEvStatistics::TEvFinishTraversal::EStatus::InternalError,
            NYql::TIssue(TStringBuilder() << "Navigate request failed with " << entry.Status));
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
        if (IColumnStatisticEval::AreMinMaxNeeded(col.Type)) {
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
    ScanActorId = Register(actor.release());
}

void TAnalyzeActor::Handle(TEvents::TEvPoison::TPtr&) {
    if (ScanActorId) {
        Send(ScanActorId, new TEvents::TEvPoison());
    }

    PassAway();
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
    ScanActorId = {};
    auto& result = *ev->Get();
    if (result.Status != Ydb::StatusIds::SUCCESS) {
        NYql::TIssue error(TStringBuilder() << "Stage 1 SELECT failed with " << result.Status);
        FinishWithFailure(
            TEvStatistics::TEvFinishTraversal::EStatus::InternalError,
            std::move(error));
        return;
    }

    NYdb::TValueParser val(result.AggColumns.at(CountSeq.value()));
    ui64 rowCount = val.GetUint64();

    NKikimrStat::TTableSummaryStatistics tableSummary;
    tableSummary.SetRowCount(rowCount);
    Results.emplace_back(std::nullopt, EStatType::TABLE_SUMMARY, tableSummary.SerializeAsString());

    auto supportedStatTypes = IColumnStatisticEval::SupportedTypes();

    TSelectBuilder stage2Builder;
    for (auto& col : Columns) {
        auto simpleStats = col.ExtractSimpleStats(rowCount, result.AggColumns);
        Results.emplace_back(
            col.Tag,
            EStatType::SIMPLE_COLUMN,
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
    ScanActorId = Register(actor.release());
}

void TAnalyzeActor::HandleStage2(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev) {
    ScanActorId = {};
    const auto& result = *ev->Get();
    if (result.Status != Ydb::StatusIds::SUCCESS) {
        NYql::TIssue error(TStringBuilder() << "Stage 2 SELECT failed with " << result.Status);
        FinishWithFailure(
            TEvStatistics::TEvFinishTraversal::EStatus::InternalError,
            std::move(error));
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
