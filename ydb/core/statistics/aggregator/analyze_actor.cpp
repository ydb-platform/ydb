#include "analyze_actor.h"

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
        SA_LOG_D("[" << SelfId() << "]: Got TEvPoison");
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
        SA_LOG_W("[" << SelfId() << "]: Navigate request failed with " << entry.Status
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

    HiveId = entry.DomainInfo->ExtractHive();
    if (!HiveId) {
        HiveId = AppData()->DomainsInfo->GetHive();
    }
    if (!HiveId) {
        FinishWithFailure(
            TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError,
            NYql::TIssue("Could not determine Hive ID."));
        return;
    }

    // TODO: escape table path
    TableName = "/" + JoinVectorIntoString(entry.Path, "/");
    IsColumnTable = !!entry.ColumnTableInfo;

    THashMap<ui32, TSysTables::TTableColumnInfo> tag2Column;
    for (const auto& col : entry.Columns) {
        tag2Column[col.second.Id] = col.second;

        if (col.second.KeyOrder >= 0) {
            KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), col.second.KeyOrder + 1));
            KeyColumnTypes[col.second.KeyOrder] = col.second.PType;
        }
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
        PendingTasks.push(TColumnStatEvalTask{
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

    if (IsColumnTable) {
        // Resolve table shard ids.
        TVector<TCell> minusInf(KeyColumnTypes.size());
        TVector<TCell> plusInf;
        TTableRange range(minusInf, true, plusInf, true, false);
        auto keyDesc = MakeHolder<TKeyDesc>(
            PathId, range, TKeyDesc::ERowOperation::Unknown, KeyColumnTypes, TVector<TKeyDesc::TColumnOp>{});

        auto resolveRequest = std::make_unique<NSchemeCache::TSchemeCacheRequest>();
        resolveRequest->DatabaseName = DatabaseName;
        resolveRequest->ResultSet.emplace_back(std::move(keyDesc));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(resolveRequest.release()));
    } else {
        StartColumnStatEvalTasks();
        Become(&TThis::StateScan);
    }
}

void TAnalyzeActor::Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
    const auto& request = *ev->Get()->Request;
    Y_ABORT_UNLESS(request.ResultSet.size() == 1);
    const NSchemeCache::TSchemeCacheRequest::TEntry& entry = request.ResultSet.front();

    if (entry.Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
        SA_LOG_W("[" << SelfId() << "]: Resolve request failed with " << entry.Status
            << ", operationId: " << OperationId.Quote()
            << ", PathId: " << PathId
            << ", DatabaseName: " << DatabaseName);

        FinishWithFailure(
            entry.Status == NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist
            ? TEvStatistics::TEvAnalyzeActorResult::EStatus::TableNotFound
            : TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError,
            NYql::TIssue(TStringBuilder() << "Resolve request failed with " << entry.Status));
        return;
    }

    for (const auto& part : entry.KeyDescription->GetPartitions()) {
        TabletIdsToLocate.insert(part.ShardId);
    }

    Send(SelfId(), new TEvPrivate::TEvRequestTableDistribution);
    Become(&TThis::StateLocateTablets);
}

void TAnalyzeActor::Handle(TEvPrivate::TEvRequestTableDistribution::TPtr&) {
    auto req = std::make_unique<TEvHive::TEvRequestTabletDistribution>();
    req->Record.MutableTabletIds()->Add(TabletIdsToLocate.begin(), TabletIdsToLocate.end());
    Send(MakePipePerNodeCacheID(EPipePerNodeCache::Leader),
        new TEvPipeCache::TEvForward(req.release(), HiveId, true));
}

void TAnalyzeActor::Handle(TEvHive::TEvResponseTabletDistribution::TPtr& ev) {
    const auto& msg = ev->Get()->Record;
    for (const auto& node : msg.GetNodes()) {
        if (node.GetNodeId()) {
            for (auto tabletId : node.GetTabletIds()) {
                TabletId2NodeId[tabletId] = node.GetNodeId();
                TabletIdsToLocate.erase(tabletId);
            }
        }
    }

    if (!TabletIdsToLocate.empty()) {
        SA_LOG_W("[" << SelfId() << "]: unable to locate " << TabletIdsToLocate.size() << " tablets");
        TryScheduleHiveRetry("Unable to locate some tablets.");
        return;
    }

    Send(MakePipePerNodeCacheID(EPipePerNodeCache::Leader), new TEvPipeCache::TEvUnlink(0));
    StartColumnStatEvalTasks();
    Become(&TThis::StateScan);
}

void TAnalyzeActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
    SA_LOG_W("[" << SelfId() << "]: got TEvDeliveryProblem");
    TryScheduleHiveRetry("Problem connecting to Hive");
}

void TAnalyzeActor::TryScheduleHiveRetry(const TStringBuf& issue) {
    if (HiveRetryCount >= MaxHiveRetryCount) {
        FinishWithFailure(
            TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError,
            NYql::TIssue(issue));
        return;
    }

    ++HiveRetryCount;
    Schedule(HiveRetryInterval, new TEvPrivate::TEvRequestTableDistribution);
}

void TAnalyzeActor::StartColumnStatEvalTasks() {
    Y_ENSURE(ScanActorsInFlight.empty());
    Y_ENSURE(NodeId2State.empty());
    Y_ENSURE(InProgressTasks.empty());
    Y_ENSURE(!PendingTasks.empty());

    if (IsColumnTable) {
        for (const auto& [tabletId, nodeId] : TabletId2NodeId) {
            NodeId2State.try_emplace(nodeId, nodeId).first->second.PendingTablets.push_back(tabletId);
        }
    }

    SelectBuilder.emplace(/*isIntermediateAggregation=*/IsColumnTable);
    size_t totalSize = 0;

    if (!CountSeq && !RowCount) {
        // Calculate total row count in the first scan we dispatch
        CountSeq = SelectBuilder->AddBuiltinAggregation({}, "count");
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
            task.SimpleStatEval->AddAggregations(col.Name, *SelectBuilder);
        } else if (task.Stage2StatEval) {
            task.Stage2StatEval->AddAggregations(col.Name, *SelectBuilder);
        }
        InProgressTasks.push_back(std::move(task));
        PendingTasks.pop();
    }

    DispatchSomeScanActors();
}

void TAnalyzeActor::DispatchSomeScanActors() {
    auto dispatchActor = [&](ui32 nodeId, std::optional<ui64> tabletId) {
        auto actor = std::make_unique<TScanActor>(
            SelfId(), DatabaseName,
            SelectBuilder->Build(TableName, tabletId), SelectBuilder->ColumnCount());
        ScanActorsInFlight[Register(actor.release())] = TScanActorInfo{
            .TabletNodeId = nodeId,
        };
    };

    if (!IsColumnTable) {
        Y_ENSURE(ScanActorsInFlight.empty());
        SA_LOG_D("[" << SelfId() << "]: Dispatching scan actor for the whole table");
        dispatchActor(0, std::nullopt);
        return;
    }

    // Run a simple scheduling algorithm, dispatching scans fairly among nodes hosting tablets,
    // and for nodes with the same number of in-flight scans, favoring nodes with the
    // longer tablet ids queue.

    auto isSchedulable = [&](const TNodeState& node) {
        return !node.PendingTablets.empty()
            && node.TabletsInFlight < Config.MaxPerNodeScanActorsInFlight;
    };

    auto nodeCmp = [](TNodeState* left, TNodeState* right) {
        return std::tuple(-left->TabletsInFlight, left->PendingTablets.size())
            < std::tuple(-right->TabletsInFlight, right->PendingTablets.size());
    };

    std::priority_queue<TNodeState*, std::vector<TNodeState*>, decltype(nodeCmp)> schedulableQueue;
    for (auto& [id, node] : NodeId2State) {
        if (isSchedulable(node)) {
            schedulableQueue.emplace(&node);
        }
    }

    while (!schedulableQueue.empty()
            && ScanActorsInFlight.size() < Config.MaxTotalScanActorsInFlight) {
        auto* node = schedulableQueue.top();
        schedulableQueue.pop();
        Y_ENSURE(!node->PendingTablets.empty());
        ui64 tabletId = node->PendingTablets.back();

        SA_LOG_D("[" << SelfId() << "]: Dispatching scan actor"
            << ", tabletId: " << tabletId << ", nodeId: " << node->Id);
        dispatchActor(node->Id, tabletId);
        node->PendingTablets.pop_back();
        ++node->TabletsInFlight;

        if (isSchedulable(*node)) {
            schedulableQueue.push(node);
        }
    }
}

void TAnalyzeActor::HandleImpl(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev) {
    auto actorIt = ScanActorsInFlight.find(ev->Sender);
    Y_ENSURE(actorIt != ScanActorsInFlight.end());
    const ui32 tabletNodeId = actorIt->second.TabletNodeId;
    ScanActorsInFlight.erase(actorIt);

    auto& result = *ev->Get();
    if (result.Status != Ydb::StatusIds::SUCCESS) {
        NYql::TIssue error(TStringBuilder() << "Statistics calculation query failed with " << result.Status);
        FinishWithFailure(
            TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError,
            std::move(error));
        return;
    }

    if (tabletNodeId) {
        auto nodeIt = NodeId2State.find(tabletNodeId);
        Y_ENSURE(nodeIt != NodeId2State.end());
        --nodeIt->second.TabletsInFlight;
        if (!nodeIt->second.TabletsInFlight && nodeIt->second.PendingTablets.empty()) {
            NodeId2State.erase(nodeIt);
        }

        DispatchSomeScanActors();
    }

    if (CountSeq) {
        NYdb::TValueParser val(result.AggColumns.at(CountSeq.value()));
        ui64 count = val.GetUint64();
        RowCount = count + RowCount.value_or(0);
    }

    if (!ScanActorsInFlight.empty()) {
        // More scan results coming for the current column tasks batch, merge the current one
        // and wait for more.
        for (const auto& task : InProgressTasks) {
            Y_ENSURE(!task.SimpleStatEval != !task.Stage2StatEval);
            if (task.SimpleStatEval) {
                task.SimpleStatEval->Merge(result.AggColumns);
            } else {
                task.Stage2StatEval->Merge(result.AggColumns);
            }
        }
        return;
    }

    // This is the last scan result for the current column tasks batch, finalize the result.

    std::vector<TStatisticsItem> resultItems;

    if (CountSeq) {
        NKikimrStat::TTableSummaryStatistics tableSummary;
        tableSummary.SetRowCount(*RowCount);
        resultItems.emplace_back(
            std::nullopt, EStatType::TABLE_SUMMARY, tableSummary.SerializeAsString());
        CountSeq.reset();
    }

    auto supportedStatTypes = IStage2ColumnStatisticEval::SupportedTypes();

    for (const auto& task : InProgressTasks) {
        const auto& col = Columns.at(task.ColumnIdx);
        Y_ENSURE(!task.SimpleStatEval != !task.Stage2StatEval);

        if (task.SimpleStatEval) {
            auto simpleStats = task.SimpleStatEval->Extract(RowCount.value(), result.AggColumns);
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
                PendingTasks.push(TColumnStatEvalTask{
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
        StartColumnStatEvalTasks();
    }
}

void TAnalyzeActor::Handle(TEvPrivate::TEvAnalyzeScanResult::TPtr& ev) {
    try {
        HandleImpl(ev);
    } catch (const std::exception& ex) {
        NYql::TIssue error(TStringBuilder()
            << "Processing statistics scan results failed with " << ex.what());
        FinishWithFailure(
            TEvStatistics::TEvAnalyzeActorResult::EStatus::InternalError,
            std::move(error));
    }
}

void TAnalyzeActor::PassAway() {
    for (const auto& [id, info] : ScanActorsInFlight){
        Send(id, new TEvents::TEvPoison());
    }

    Send(MakePipePerNodeCacheID(EPipePerNodeCache::Leader), new TEvPipeCache::TEvUnlink(0));

    TActorBootstrapped::PassAway();
}

} // NKikimr::NStat
