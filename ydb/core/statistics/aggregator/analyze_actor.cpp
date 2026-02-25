#include "analyze_actor.h"
#include "select_builder.h"

#include <ydb/library/query_actor/query_actor.h>
#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>
#include <util/generic/size_literals.h>
#include <util/string/vector.h>

namespace NKikimr::NStat {

static constexpr ui64 MAX_STATISTIC_SIZE = 8_MB;
static constexpr ui64 MAX_STATISTICS_SIZE_IN_SINGLE_SCAN = 40_MB;

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
        TEvStatistics::TEvAnalyzeActorResult::EStatus status,
        NYql::TIssue issue) {
    auto response = std::make_unique<TEvStatistics::TEvAnalyzeActorResult>(status);

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
            ? TEvStatistics::TEvAnalyzeActorResult::EStatus::TableNotFound
            : TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError,
            NYql::TIssue(TStringBuilder() << "Navigate request failed with " << entry.Status));
        return;
    }

    // TODO: escape table path
    TableName = "/" + JoinVectorIntoString(entry.Path, "/");

    THashMap<ui32, TSysTables::TTableColumnInfo> tag2Column;
    for (const auto& col : entry.Columns) {
        tag2Column[col.second.Id] = col.second;
    }

    auto addColumn = [&](const TSysTables::TTableColumnInfo& colInfo) {
        Columns.emplace_back(colInfo.Id, colInfo.PType, colInfo.PTypeMod, colInfo.Name);
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

    // Add tasks to calculate simple column statistics.
    for (size_t i = 0; i < Columns.size(); ++i) {
        const auto& col = Columns[i];
        PendingTasks.push(TEvalTask{
            .ColumnIdx = i,
            .SimpleStatEval = std::make_unique<TSimpleColumnStatisticEval>(
                col.Type, col.PgTypeMod),
        });
    }

    if (PendingTasks.empty()) {
        // All requested columns were already dropped. Send empty response right away.
        auto response = std::make_unique<TEvStatistics::TEvAnalyzeActorResult>(
            std::vector<TStatisticsItem>{}, /*final=*/ true);
        Send(Parent, response.release());
        PassAway();
        return;
    }

    Become(&TThis::StateQuery);
    DispatchScanActor();
}

void TAnalyzeActor::DispatchScanActor() {
    Y_ENSURE(!ScanActorId);
    Y_ENSURE(InProgressTasks.empty());
    Y_ENSURE(!PendingTasks.empty());

    TSelectBuilder selectBuilder;
    size_t totalSize = 0;

    if (!CountSeq) {
        // Calculate total row count in the first scan we dispatch
        CountSeq = selectBuilder.AddBuiltinAggregation({}, "count");
    }

    while (!PendingTasks.empty()) {
        auto& task = PendingTasks.front();
        Y_ENSURE(!task.SimpleStatEval != !task.Stage2StatEval);
        size_t resultSize = task.SimpleStatEval
            ? task.SimpleStatEval->EstimateSize()
            : task.Stage2StatEval->EstimateSize();
        if (totalSize + resultSize > MAX_STATISTICS_SIZE_IN_SINGLE_SCAN) {
            break;
        }

        const auto& col = Columns.at(task.ColumnIdx);
        totalSize += resultSize;
        if (task.SimpleStatEval) {
            task.SimpleStatEval->AddAggregations(col.Name, selectBuilder);
        } else if (task.Stage2StatEval) {
            task.Stage2StatEval->AddAggregations(col.Name, selectBuilder);
        }
        InProgressTasks.push_back(std::move(task));
        PendingTasks.pop();
    }

    auto actor = std::make_unique<TScanActor>(
        SelfId(), DatabaseName, selectBuilder.Build(TableName), selectBuilder.ColumnCount());
    ScanActorId = Register(actor.release());
}

void TAnalyzeActor::Handle(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev) {
    ScanActorId = {};
    auto& result = *ev->Get();
    if (result.Status != Ydb::StatusIds::SUCCESS) {
        NYql::TIssue error(TStringBuilder() << "Statistics calculation query failed with " << result.Status);
        FinishWithFailure(
            TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError,
            std::move(error));
        return;
    }

    std::vector<TStatisticsItem> resultItems;

    if (!RowCount) {
        NYdb::TValueParser val(result.AggColumns.at(CountSeq.value()));
        RowCount = val.GetUint64();

        NKikimrStat::TTableSummaryStatistics tableSummary;
        tableSummary.SetRowCount(*RowCount);
        resultItems.emplace_back(
            std::nullopt, EStatType::TABLE_SUMMARY, tableSummary.SerializeAsString());
    }

    auto supportedStatTypes = IStage2ColumnStatisticEval::SupportedTypes();

    for (const auto& task : InProgressTasks) {
        const auto& col = Columns.at(task.ColumnIdx);
        Y_ENSURE(!task.SimpleStatEval != !task.Stage2StatEval);

        if (task.SimpleStatEval) {
            auto simpleStats = task.SimpleStatEval->Extract(*RowCount, result.AggColumns);
            resultItems.emplace_back(
                col.Tag,
                task.SimpleStatEval->GetType(),
                simpleStats.SerializeAsString());

            for (auto type : supportedStatTypes) {
                auto statEval = IStage2ColumnStatisticEval::MaybeCreate(type, simpleStats, col.Type);
                if (!statEval) {
                    continue;
                }
                if (statEval->EstimateSize() > MAX_STATISTIC_SIZE) {
                    continue;
                }
                PendingTasks.push(TEvalTask{
                    .ColumnIdx = task.ColumnIdx,
                    .Stage2StatEval = std::move(statEval),
                });
            }
        } else if (task.Stage2StatEval) {
            resultItems.emplace_back(
                col.Tag,
                task.Stage2StatEval->GetType(),
                task.Stage2StatEval->ExtractData(result.AggColumns));
        }
    }
    InProgressTasks.clear();

    const bool isFinalResult = PendingTasks.empty();
    auto response = std::make_unique<TEvStatistics::TEvAnalyzeActorResult>(
        std::move(resultItems), isFinalResult);
    Send(Parent, response.release());

    if (isFinalResult) {
        PassAway();
    } else {
        DispatchScanActor();
    }
}

void TAnalyzeActor::Handle(TEvents::TEvPoison::TPtr&) {
    if (ScanActorId) {
        Send(ScanActorId, new TEvents::TEvPoison());
    }

    PassAway();
}

} // NKikimr::NStat
