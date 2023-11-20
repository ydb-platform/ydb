#include "kqp_executer_stats.h"


namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;

namespace {

TTableStat operator - (const TTableStat& l, const TTableStat& r) {
    return {.Rows = l.Rows - r.Rows, .Bytes = l.Bytes - r.Bytes};
}

TProgressStatEntry operator - (const TProgressStatEntry& l, const TProgressStatEntry& r) {
    return TProgressStatEntry {
        .ComputeTime = l.ComputeTime - r.ComputeTime,
        .ReadIOStat = l.ReadIOStat - r.ReadIOStat
    };
}

void UpdateAggr(NDqProto::TDqStatsAggr* aggr, ui64 value) noexcept {
    if (aggr->GetMin() == 0) {
        aggr->SetMin(value);
    } else {
        aggr->SetMin(std::min(aggr->GetMin(), value));
    }
    aggr->SetMax(std::max(aggr->GetMax(), value));
    aggr->SetSum(aggr->GetSum() + value);
    aggr->SetCnt(aggr->GetCnt() + 1);
}

struct TAsyncGroupStat {
    ui64 Bytes = 0;
    ui64 Rows = 0;
    ui64 Chunks = 0;
    ui64 Splits = 0;
    ui64 FirstMessageMs = 0;
    ui64 PauseMessageMs = 0;
    ui64 ResumeMessageMs = 0;
    ui64 LastMessageMs = 0;
    ui64 WaitTimeUs = 0;
    ui64 WaitPeriods = 0;
    ui64 Count = 0;
};

void UpdateAsyncAggr(NDqProto::TDqAsyncStatsAggr& asyncAggr, const NDqProto::TDqAsyncBufferStats& asyncStat) noexcept {
    UpdateAggr(asyncAggr.MutableBytes(), asyncStat.GetBytes());
    UpdateAggr(asyncAggr.MutableRows(), asyncStat.GetRows());
    UpdateAggr(asyncAggr.MutableChunks(), asyncStat.GetChunks());
    UpdateAggr(asyncAggr.MutableSplits(), asyncStat.GetSplits());

    auto firstMessageMs = asyncStat.GetFirstMessageMs();
    if (firstMessageMs) {
        UpdateAggr(asyncAggr.MutableFirstMessageMs(), firstMessageMs);
    }
    if (asyncStat.GetPauseMessageMs()) {
        UpdateAggr(asyncAggr.MutablePauseMessageMs(), asyncStat.GetPauseMessageMs());
    }
    if (asyncStat.GetResumeMessageMs()) {
        UpdateAggr(asyncAggr.MutableResumeMessageMs(), asyncStat.GetResumeMessageMs());
    }
    auto lastMessageMs = asyncStat.GetLastMessageMs();
    if (lastMessageMs) {
        UpdateAggr(asyncAggr.MutableLastMessageMs(), lastMessageMs);
    }

    UpdateAggr(asyncAggr.MutableWaitTimeUs(), asyncStat.GetWaitTimeUs());
    UpdateAggr(asyncAggr.MutableWaitPeriods(), asyncStat.GetWaitPeriods());

    if (firstMessageMs && lastMessageMs >= firstMessageMs) {
        UpdateAggr(asyncAggr.MutableActiveTimeUs(), (lastMessageMs - firstMessageMs) * 1000);
    }
}

void UpdateMinMax(NDqProto::TDqStatsMinMax* minMax, ui64 value) noexcept {
    if (minMax->GetMin() == 0) {
        minMax->SetMin(value);
    } else {
        minMax->SetMin(std::min(minMax->GetMin(), value));
    }
    minMax->SetMax(std::max(minMax->GetMax(), value));
}

NDqProto::TDqStageStats* GetOrCreateStageStats(const NYql::NDqProto::TDqTaskStats& taskStats,
    const TKqpTasksGraph& tasksGraph, NDqProto::TDqExecutionStats& execStats)
{
    auto& task = tasksGraph.GetTask(taskStats.GetTaskId());
    auto& stageId = task.StageId;
    auto& stageInfo = tasksGraph.GetStageInfo(stageId);
    auto& stageProto = stageInfo.Meta.Tx.Body->GetStages(stageId.StageId);

    for (auto& stage : *execStats.MutableStages()) {
        if (stage.GetStageGuid() == stageProto.GetStageGuid()) {
            return &stage;
        }
    }

    auto* newStage = execStats.AddStages();
    newStage->SetStageId(stageId.StageId);
    newStage->SetStageGuid(stageProto.GetStageGuid());
    newStage->SetProgram(stageProto.GetProgramAst());
    return newStage;
}

NDqProto::TDqTableAggrStats* GetOrCreateTableAggrStats(NDqProto::TDqStageStats* stage, const TString& tablePath) {
    for(auto& table : *stage->MutableTables()) {
        if (table.GetTablePath() == tablePath) {
            return &table;
        }
    }
    auto table = stage->AddTables();
    table->SetTablePath(tablePath);
    return table;
}

} // anonymous namespace

NYql::NDqProto::EDqStatsMode GetDqStatsMode(Ydb::Table::QueryStatsCollection::Mode mode) {
    switch (mode) {
        // Always collect basic stats for system views / request unit computation.
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE:
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
            return NYql::NDqProto::DQ_STATS_MODE_BASIC;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
            return NYql::NDqProto::DQ_STATS_MODE_FULL;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE:
            return NYql::NDqProto::DQ_STATS_MODE_PROFILE;
        default:
            return NYql::NDqProto::DQ_STATS_MODE_NONE;
    }
}

NYql::NDqProto::EDqStatsMode GetDqStatsModeShard(Ydb::Table::QueryStatsCollection::Mode mode) {
    switch (mode) {
        // Collect only minimal required stats to improve datashard performance.
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE:
            return NYql::NDqProto::DQ_STATS_MODE_NONE;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
            return NYql::NDqProto::DQ_STATS_MODE_BASIC;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
            return NYql::NDqProto::DQ_STATS_MODE_FULL;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE:
            return NYql::NDqProto::DQ_STATS_MODE_PROFILE;
        default:
            return NYql::NDqProto::DQ_STATS_MODE_NONE;
    }
}

bool CollectFullStats(Ydb::Table::QueryStatsCollection::Mode statsMode) {
    return statsMode >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL;
}

bool CollectProfileStats(Ydb::Table::QueryStatsCollection::Mode statsMode) {
    return statsMode >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE;
}

void TQueryExecutionStats::AddComputeActorFullStatsByTask(
        const NYql::NDqProto::TDqTaskStats& task,
        const NYql::NDqProto::TDqComputeActorStats& stats
    ) {
    auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);

    stageStats->SetTotalTasksCount(stageStats->GetTotalTasksCount() + 1);
    UpdateAggr(stageStats->MutableMaxMemoryUsage(), stats.GetMaxMemoryUsage()); // only 1 task per CA now
    UpdateAggr(stageStats->MutableCpuTimeUs(), task.GetCpuTimeUs());
    UpdateAggr(stageStats->MutableSourceCpuTimeUs(), task.GetSourceCpuTimeUs());
    UpdateAggr(stageStats->MutableInputRows(), task.GetInputRows());
    UpdateAggr(stageStats->MutableInputBytes(), task.GetInputBytes());
    UpdateAggr(stageStats->MutableOutputRows(), task.GetOutputRows());
    UpdateAggr(stageStats->MutableOutputBytes(), task.GetOutputBytes());

    UpdateMinMax(stageStats->MutableStartTimeMs(), task.GetStartTimeMs());       // to be reviewed
    UpdateMinMax(stageStats->MutableFirstRowTimeMs(), task.GetFirstRowTimeMs()); // to be reviewed
    UpdateMinMax(stageStats->MutableFinishTimeMs(), task.GetFinishTimeMs());     // to be reviewed

    stageStats->SetDurationUs((stageStats->GetFinishTimeMs().GetMax() - stageStats->GetStartTimeMs().GetMin()) * 1'000);

    for (auto& sourcesStat : task.GetSources()) {
        UpdateAsyncAggr(*(*stageStats->MutableIngress())[sourcesStat.GetIngressName()].MutableIngress(), sourcesStat.GetIngress());
        UpdateAsyncAggr(*(*stageStats->MutableIngress())[sourcesStat.GetIngressName()].MutablePush(),   sourcesStat.GetPush());
        UpdateAsyncAggr(*(*stageStats->MutableIngress())[sourcesStat.GetIngressName()].MutablePop(),  sourcesStat.GetPop());
    }
    for (auto& inputChannelStat : task.GetInputChannels()) {
        UpdateAsyncAggr(*(*stageStats->MutableInput())[inputChannelStat.GetSrcStageId()].MutablePush(), inputChannelStat.GetPush());
        UpdateAsyncAggr(*(*stageStats->MutableInput())[inputChannelStat.GetSrcStageId()].MutablePop(), inputChannelStat.GetPop());
    }
    for (auto& outputChannelStat : task.GetOutputChannels()) {
        UpdateAsyncAggr(*(*stageStats->MutableOutput())[outputChannelStat.GetDstStageId()].MutablePush(), outputChannelStat.GetPush());
        UpdateAsyncAggr(*(*stageStats->MutableOutput())[outputChannelStat.GetDstStageId()].MutablePop(), outputChannelStat.GetPop());
    }
    for (auto& sinksStat : task.GetSinks()) {
        UpdateAsyncAggr(*(*stageStats->MutableEgress())[sinksStat.GetEgressName()].MutablePush(),   sinksStat.GetPush());
        UpdateAsyncAggr(*(*stageStats->MutableEgress())[sinksStat.GetEgressName()].MutablePop(),    sinksStat.GetPop());
        UpdateAsyncAggr(*(*stageStats->MutableEgress())[sinksStat.GetEgressName()].MutableEgress(), sinksStat.GetEgress());
    }
}

void TQueryExecutionStats::AddComputeActorProfileStatsByTask(
        const NYql::NDqProto::TDqTaskStats& task, const NYql::NDqProto::TDqComputeActorStats& stats) {
    auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);
    stageStats->AddComputeActors()->CopyFrom(stats);
}

void TQueryExecutionStats::AddComputeActorStats(ui32 /* nodeId */, NYql::NDqProto::TDqComputeActorStats&& stats,
        TDuration collectLongTaskStatsTimeout) {
//    Cerr << (TStringBuilder() << "::AddComputeActorStats " << stats.DebugString() << Endl);

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + stats.GetCpuTimeUs());
    TotalTasks += stats.GetTasks().size();

    UpdateAggr(ExtraStats.MutableComputeCpuTimeUs(), stats.GetCpuTimeUs());

    auto longTasks = TVector<NYql::NDqProto::TDqTaskStats*>(Reserve(stats.GetTasks().size()));

    for (auto& task : *stats.MutableTasks()) {
        for (auto& table : task.GetTables()) {
            NYql::NDqProto::TDqTableStats* tableAggr = nullptr;
            if (auto it = TableStats.find(table.GetTablePath()); it != TableStats.end()) {
                tableAggr = it->second;
            } else {
                tableAggr = Result->AddTables();
                tableAggr->SetTablePath(table.GetTablePath());
                TableStats.emplace(table.GetTablePath(), tableAggr);
            }

            tableAggr->SetReadRows(tableAggr->GetReadRows() + table.GetReadRows());
            tableAggr->SetReadBytes(tableAggr->GetReadBytes() + table.GetReadBytes());
            tableAggr->SetWriteRows(tableAggr->GetWriteRows() + table.GetWriteRows());
            tableAggr->SetWriteBytes(tableAggr->GetWriteBytes() + table.GetWriteBytes());
            tableAggr->SetEraseRows(tableAggr->GetEraseRows() + table.GetEraseRows());
            tableAggr->SetAffectedPartitions(tableAggr->GetAffectedPartitions() + table.GetAffectedPartitions());

            NKqpProto::TKqpTableExtraStats tableExtraStats;
            if (table.GetExtra().UnpackTo(&tableExtraStats)) {
                for (const auto& shardId : tableExtraStats.GetReadActorTableAggrExtraStats().GetAffectedShards()) {
                    AffectedShards.insert(shardId);
                }
            }

            // TODO: the following code is for backward compatibility, remove it after ydb release
            {
                NKqpProto::TKqpReadActorTableAggrExtraStats tableExtraStats;
                if (table.GetExtra().UnpackTo(&tableExtraStats)) {
                    for (const auto& shardId : tableExtraStats.GetAffectedShards()) {
                        AffectedShards.insert(shardId);
                    }
                }
            }
        }

        // checking whether the task is long
        auto taskDuration = TDuration::MilliSeconds(task.GetFinishTimeMs() - task.GetStartTimeMs());
        bool longTask = taskDuration > collectLongTaskStatsTimeout;
        if (longTask) {
            CollectStatsByLongTasks = true;
            longTasks.push_back(&task);
        }
    }

    if (CollectFullStats(StatsMode)) {
        for (const auto& task : stats.GetTasks()) {
            AddComputeActorFullStatsByTask(task, stats);
        }
    }

    if (CollectProfileStats(StatsMode)) {
        for (const auto& task : stats.GetTasks()) {
            AddComputeActorProfileStatsByTask(task, stats);
        }
    } else {
        for (const auto* task : longTasks) {
            AddComputeActorProfileStatsByTask(*task, stats);
        }
    }
}

void TQueryExecutionStats::AddDatashardPrepareStats(NKikimrQueryStats::TTxStats&& txStats) {
//    Cerr << (TStringBuilder() << "::AddDatashardPrepareStats " << txStats.DebugString() << Endl);

    ui64 cpuUs = txStats.GetComputeCpuTimeUsec();
    for (const auto& perShard : txStats.GetPerShardStats()) {
        AffectedShards.emplace(perShard.GetShardId());
        cpuUs += perShard.GetCpuTimeUsec();
    }

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + cpuUs);
}

void TQueryExecutionStats::AddDatashardFullStatsByTask(
        const NYql::NDqProto::TDqTaskStats& task, ui64 datashardCpuTimeUs) {
    auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);

    stageStats->SetTotalTasksCount(stageStats->GetTotalTasksCount() + 1);
    UpdateAggr(stageStats->MutableCpuTimeUs(), task.GetCpuTimeUs());
    UpdateAggr(stageStats->MutableInputRows(), task.GetInputRows());
    UpdateAggr(stageStats->MutableInputBytes(), task.GetInputBytes());
    UpdateAggr(stageStats->MutableOutputRows(), task.GetOutputRows());
    UpdateAggr(stageStats->MutableOutputBytes(), task.GetOutputBytes());

    UpdateMinMax(stageStats->MutableStartTimeMs(), task.GetStartTimeMs());       // to be reviewed
    UpdateMinMax(stageStats->MutableFirstRowTimeMs(), task.GetFirstRowTimeMs()); // to be reviewed
    UpdateMinMax(stageStats->MutableFinishTimeMs(), task.GetFinishTimeMs());     // to be reviewed

    stageStats->SetDurationUs((stageStats->GetFinishTimeMs().GetMax() - stageStats->GetStartTimeMs().GetMin()) * 1'000);

    for (auto& tableStats: task.GetTables()) {
        auto* tableAggrStats = GetOrCreateTableAggrStats(stageStats, tableStats.GetTablePath());

        UpdateAggr(tableAggrStats->MutableReadRows(), tableStats.GetReadRows());
        UpdateAggr(tableAggrStats->MutableReadBytes(), tableStats.GetReadBytes());
        UpdateAggr(tableAggrStats->MutableWriteRows(), tableStats.GetWriteRows());
        UpdateAggr(tableAggrStats->MutableWriteBytes(), tableStats.GetWriteBytes());
        UpdateAggr(tableAggrStats->MutableEraseRows(), tableStats.GetEraseRows());

        NKqpProto::TKqpShardTableExtraStats tableExtraStats;

        if (tableStats.GetExtra().UnpackTo(&tableExtraStats)) {
            NKqpProto::TKqpShardTableAggrExtraStats tableAggrExtraStats;
            if (tableAggrStats->HasExtra()) {
                bool ok = tableAggrStats->MutableExtra()->UnpackTo(&tableAggrExtraStats);
                YQL_ENSURE(ok);
            }

            tableAggrExtraStats.SetAffectedShards(TableShards[tableStats.GetTablePath()].size());
            UpdateAggr(tableAggrExtraStats.MutableShardCpuTimeUs(), datashardCpuTimeUs);

            tableAggrStats->MutableExtra()->PackFrom(tableAggrExtraStats);
        }
    }

    NKqpProto::TKqpStageExtraStats stageExtraStats;
    if (stageStats->HasExtra()) {
        bool ok = stageStats->GetExtra().UnpackTo(&stageExtraStats);
        YQL_ENSURE(ok);
    }
    stageExtraStats.AddDatashardTasks()->CopyFrom(task);
    stageStats->MutableExtra()->PackFrom(stageExtraStats);
}

void TQueryExecutionStats::AddDatashardStats(NYql::NDqProto::TDqComputeActorStats&& stats,
    NKikimrQueryStats::TTxStats&& txStats, TDuration collectLongTaskStatsTimeout)
{
//    Cerr << (TStringBuilder() << "::AddDatashardStats " << stats.DebugString() << ", " << txStats.DebugString() << Endl);

    ui64 datashardCpuTimeUs = 0;
    for (const auto& perShard : txStats.GetPerShardStats()) {
        AffectedShards.emplace(perShard.GetShardId());

        datashardCpuTimeUs += perShard.GetCpuTimeUsec();
        UpdateAggr(ExtraStats.MutableShardsCpuTimeUs(), perShard.GetCpuTimeUsec());
    }

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + datashardCpuTimeUs);
    TotalTasks += stats.GetTasks().size();

    auto longTasks = TVector<NYql::NDqProto::TDqTaskStats*>(Reserve(stats.GetTasks().size()));

    for (auto& task : *stats.MutableTasks()) {
        for (auto& table : task.GetTables()) {
            NYql::NDqProto::TDqTableStats* tableAggr = nullptr;
            if (auto it = TableStats.find(table.GetTablePath()); it != TableStats.end()) {
                tableAggr = it->second;
            } else {
                tableAggr = Result->AddTables();
                tableAggr->SetTablePath(table.GetTablePath());
                TableStats.emplace(table.GetTablePath(), tableAggr);
            }

            tableAggr->SetReadRows(tableAggr->GetReadRows() + table.GetReadRows());
            tableAggr->SetReadBytes(tableAggr->GetReadBytes() + table.GetReadBytes());
            tableAggr->SetWriteRows(tableAggr->GetWriteRows() + table.GetWriteRows());
            tableAggr->SetWriteBytes(tableAggr->GetWriteBytes() + table.GetWriteBytes());
            tableAggr->SetEraseRows(tableAggr->GetEraseRows() + table.GetEraseRows());

            auto& shards = TableShards[table.GetTablePath()];
            for (const auto& perShard : txStats.GetPerShardStats()) {
                shards.insert(perShard.GetShardId());
            }
            tableAggr->SetAffectedPartitions(shards.size());
        }

        // checking whether the task is long
        auto taskDuration = TDuration::MilliSeconds(task.GetFinishTimeMs() - task.GetStartTimeMs());
        bool longTask = taskDuration > collectLongTaskStatsTimeout;
        if (longTask) {
            CollectStatsByLongTasks = true;
            longTasks.push_back(&task);
        }
    }

    if (CollectFullStats(StatsMode)) {
        for (auto& task : stats.GetTasks()) {
            AddDatashardFullStatsByTask(task, datashardCpuTimeUs);
        }
        DatashardStats.emplace_back(std::move(txStats));
    } else {
        if (!longTasks.empty()) {
            DatashardStats.emplace_back(std::move(txStats));
        }
    }

    if (CollectProfileStats(StatsMode)) {
        for (const auto& task : stats.GetTasks()) {
            AddComputeActorProfileStatsByTask(task, stats);
        }
    } else {
        for (const auto* task : longTasks) {
            AddComputeActorProfileStatsByTask(*task, stats);
        }
    }
}

void TQueryExecutionStats::Finish() {
//    Cerr << (TStringBuilder() << "-- finish: executerTime: " << ExecuterCpuTime.MicroSeconds() << Endl);

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + ExecuterCpuTime.MicroSeconds());
    Result->SetDurationUs(FinishTs.MicroSeconds() - StartTs.MicroSeconds());

    Result->SetResultBytes(ResultBytes);
    Result->SetResultRows(ResultRows);

    ExtraStats.SetAffectedShards(AffectedShards.size());
    if (CollectStatsByLongTasks || CollectProfileStats(StatsMode)) {
        for (auto&& s : UseLlvmByStageId) {
            for (auto&& pbs : *Result->MutableStages()) {
                if (pbs.GetStageId() == s.first) {
                    pbs.SetUseLlvm(s.second);
                    break;
                }
            }
        }

        for (auto&& s : ShardsCountByNode) {
            for (auto&& pbs : *Result->MutableStages()) {
                if (pbs.GetStageId() == s.first) {
                    NKqpProto::TKqpStageExtraStats pbStats;
                    if (pbs.HasExtra() && !pbs.GetExtra().UnpackTo(&pbStats)) {
                        break;
                    }
                    for (auto&& i : s.second) {
                        auto& nodeStat = *pbStats.AddNodeStats();
                        nodeStat.SetNodeId(i.first);
                        nodeStat.SetShardsCount(i.second);
                    }
                    pbs.MutableExtra()->PackFrom(pbStats);
                    break;
                }
            }
        }
    }
    Result->MutableExtra()->PackFrom(ExtraStats);

    if (CollectStatsByLongTasks || CollectFullStats(StatsMode)) {
        Result->SetExecuterCpuTimeUs(ExecuterCpuTime.MicroSeconds());

        Result->SetStartTimeMs(StartTs.MilliSeconds());
        Result->SetFinishTimeMs(FinishTs.MilliSeconds());
    }

 //   Cerr << (TStringBuilder() << "TQueryExecutionStats::Finish" << Endl << Result->DebugString() << Endl);
}

TTableStat& TTableStat::operator +=(const TTableStat& rhs) {
    Rows += rhs.Rows;
    Bytes += rhs.Bytes;
    return *this;
}

TTableStat& TTableStat::operator -=(const TTableStat& rhs) {
    Rows -= rhs.Rows;
    Bytes -= rhs.Bytes;
    return *this;
}

TProgressStatEntry& TProgressStatEntry::operator +=(const TProgressStatEntry& rhs) {
    ComputeTime += rhs.ComputeTime;
    ReadIOStat += rhs.ReadIOStat;

    return *this;
}

TTableStat CalcSumTableReadStat(const TProgressStatEntry& entry) {
    return entry.ReadIOStat;
}

TDuration CalcCumComputeTime(const TProgressStatEntry& entry) {
    return entry.ComputeTime;
}

void TProgressStatEntry::Out(IOutputStream& o) const {
    o << "ComputeTime: " << ComputeTime << " ReadRows: " << ReadIOStat.Rows << " ReadBytes: " << ReadIOStat.Bytes;
}

void TProgressStat::Set(const NDqProto::TDqComputeActorStats& stats) {
    Cur.ComputeTime += TDuration::MicroSeconds(stats.GetCpuTimeUs());
    for (auto& task : stats.GetTasks()) {
        for (auto& table: task.GetTables()) {
            Cur.ReadIOStat.Rows += table.GetReadRows();
            Cur.ReadIOStat.Bytes += table.GetReadBytes();
        }
    }
}

TProgressStat::TEntry TProgressStat::GetLastUsage() const {
    return Cur - Total;
}

void TProgressStat::Update() {
    Total = Cur;
    Cur = TEntry();
}

} // namespace NKikimr::NKqp
