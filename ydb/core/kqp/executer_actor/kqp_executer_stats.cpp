#include "kqp_executer_stats.h"


namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;

void TAsyncStats::Resize(ui32 taskCount) {
    Bytes.resize(taskCount);
    Rows.resize(taskCount);
    Chunks.resize(taskCount);
    Splits.resize(taskCount);
    FirstMessageMs.resize(taskCount);
    PauseMessageMs.resize(taskCount);
    ResumeMessageMs.resize(taskCount);
    LastMessageMs.resize(taskCount);
    WaitTimeUs.resize(taskCount);
    WaitPeriods.resize(taskCount);
    ActiveTimeUs.resize(taskCount);
}

void TAsyncBufferStats::Resize(ui32 taskCount) {
    Ingress.Resize(taskCount);
    Push.Resize(taskCount);
    Pop.Resize(taskCount);
    Egress.Resize(taskCount);
}

void TTableStats::Resize(ui32 taskCount) {
    ReadRows.resize(taskCount);
    ReadBytes.resize(taskCount);
    WriteRows.resize(taskCount);
    WriteBytes.resize(taskCount);
    EraseRows.resize(taskCount);
    EraseBytes.resize(taskCount);
    AffectedPartitions.resize(taskCount);
}

void TStageExecutionStats::Resize(ui32 taskCount) {
    CpuTimeUs.resize(taskCount);
    SourceCpuTimeUs.resize(taskCount);

    InputRows.resize(taskCount);
    InputBytes.resize(taskCount);
    OutputRows.resize(taskCount);
    OutputBytes.resize(taskCount);
    ResultRows.resize(taskCount);
    ResultBytes.resize(taskCount);
    IngressRows.resize(taskCount);
    IngressBytes.resize(taskCount);
    EgressRows.resize(taskCount);
    EgressBytes.resize(taskCount);

    FinishTimeMs.resize(taskCount);
    StartTimeMs.resize(taskCount);
    DurationUs.resize(taskCount);
    WaitInputTimeUs.resize(taskCount);
    WaitOutputTimeUs.resize(taskCount);

    for (auto& p : Ingress) p.second.Resize(taskCount);
    for (auto& p : Egress) p.second.Resize(taskCount);
    for (auto& p : Input) p.second.Resize(taskCount);
    for (auto& p : Output) p.second.Resize(taskCount);

    MaxMemoryUsage.resize(taskCount);
}

void SetNonZero(ui64& target, ui64 source) {
    if (source) {
        target = source;
    }
}

void TStageExecutionStats::UpdateAsyncStats(i32 index, TAsyncStats& aggrAsyncStats, const NYql::NDqProto::TDqAsyncBufferStats& asyncStats) {
    SetNonZero(aggrAsyncStats.Bytes[index], asyncStats.GetBytes());
    SetNonZero(aggrAsyncStats.Rows[index], asyncStats.GetRows());
    SetNonZero(aggrAsyncStats.Chunks[index], asyncStats.GetChunks());
    SetNonZero(aggrAsyncStats.Splits[index], asyncStats.GetSplits());

    auto firstMessageMs = asyncStats.GetFirstMessageMs();
    SetNonZero(aggrAsyncStats.FirstMessageMs[index], firstMessageMs);
    SetNonZero(aggrAsyncStats.PauseMessageMs[index], asyncStats.GetPauseMessageMs());
    SetNonZero(aggrAsyncStats.ResumeMessageMs[index], asyncStats.GetResumeMessageMs());
    auto lastMessageMs = asyncStats.GetLastMessageMs();
    SetNonZero(aggrAsyncStats.LastMessageMs[index], lastMessageMs);
    SetNonZero(aggrAsyncStats.WaitTimeUs[index], asyncStats.GetWaitTimeUs());
    SetNonZero(aggrAsyncStats.WaitPeriods[index], asyncStats.GetWaitPeriods());
    if (firstMessageMs && lastMessageMs > firstMessageMs) {
        aggrAsyncStats.ActiveTimeUs[index] = lastMessageMs - firstMessageMs;
    }
}

void TStageExecutionStats::UpdateStats(const NYql::NDqProto::TDqTaskStats& taskStats, ui64 maxMemoryUsage, ui64 durationUs) {
    auto taskId = taskStats.GetTaskId();
    auto it = Task2Index.find(taskId);

    ui32 taskCount = Task2Index.size();

    ui32 index;
    if (it == Task2Index.end()) {
        index = taskCount++;
        Task2Index.emplace(taskId, index);
        Resize(taskCount);
    } else {
        index = it->second;
    }

    SetNonZero(CpuTimeUs[index], taskStats.GetCpuTimeUs());
    SetNonZero(SourceCpuTimeUs[index], taskStats.GetSourceCpuTimeUs());

    SetNonZero(InputRows[index], taskStats.GetInputRows());
    SetNonZero(InputBytes[index], taskStats.GetInputBytes());
    SetNonZero(OutputRows[index], taskStats.GetOutputRows());
    SetNonZero(OutputBytes[index], taskStats.GetOutputBytes());
    SetNonZero(ResultRows[index], taskStats.GetResultRows());
    SetNonZero(ResultBytes[index], taskStats.GetResultBytes());
    SetNonZero(IngressRows[index], taskStats.GetIngressRows());
    SetNonZero(IngressBytes[index], taskStats.GetIngressBytes());
    SetNonZero(EgressRows[index], taskStats.GetEgressRows());
    SetNonZero(EgressBytes[index], taskStats.GetEgressBytes());

    SetNonZero(StartTimeMs[index], taskStats.GetStartTimeMs());
    SetNonZero(FinishTimeMs[index], taskStats.GetFinishTimeMs());
    SetNonZero(DurationUs[index], durationUs);
    SetNonZero(WaitInputTimeUs[index], taskStats.GetWaitInputTimeUs());
    SetNonZero(WaitOutputTimeUs[index], taskStats.GetWaitOutputTimeUs());

    for (auto& tableStat : taskStats.GetTables()) {
        auto tablePath = tableStat.GetTablePath();
        auto [it, inserted] = Tables.try_emplace(tablePath, taskCount);
        auto& aggrTableStats = it->second;
        SetNonZero(aggrTableStats.ReadRows[index], tableStat.GetReadRows());
        SetNonZero(aggrTableStats.ReadBytes[index], tableStat.GetReadBytes());
        SetNonZero(aggrTableStats.WriteRows[index], tableStat.GetWriteRows());
        SetNonZero(aggrTableStats.WriteBytes[index], tableStat.GetWriteBytes());
        SetNonZero(aggrTableStats.EraseRows[index], tableStat.GetEraseRows());
        SetNonZero(aggrTableStats.EraseBytes[index], tableStat.GetEraseBytes());
        SetNonZero(aggrTableStats.AffectedPartitions[index], tableStat.GetAffectedPartitions());
    }

    for (auto& sourceStat : taskStats.GetSources()) {
        auto ingressName = sourceStat.GetIngressName();
        if (ingressName) {
            auto [it, inserted] = Ingress.try_emplace(ingressName, taskCount);
            auto& asyncBufferStats = it->second;
            UpdateAsyncStats(index, asyncBufferStats.Ingress, sourceStat.GetIngress());
            UpdateAsyncStats(index, asyncBufferStats.Push, sourceStat.GetPush());
            UpdateAsyncStats(index, asyncBufferStats.Pop, sourceStat.GetPop());
        }
    }

    for (auto& inputChannelStat : taskStats.GetInputChannels()) {
        auto stageId = inputChannelStat.GetSrcStageId();
        auto [it, inserted] = Input.try_emplace(stageId, taskCount);
        auto& asyncBufferStats = it->second;
        UpdateAsyncStats(index, asyncBufferStats.Push, inputChannelStat.GetPush());
        UpdateAsyncStats(index, asyncBufferStats.Pop, inputChannelStat.GetPop());
    }

    for (auto& outputChannelStat : taskStats.GetOutputChannels()) {
        auto stageId = outputChannelStat.GetDstStageId();
        auto [it, inserted] = Output.try_emplace(stageId, taskCount);
        auto& asyncBufferStats = it->second;
        UpdateAsyncStats(index, asyncBufferStats.Push, outputChannelStat.GetPush());
        UpdateAsyncStats(index, asyncBufferStats.Pop, outputChannelStat.GetPop());
    }

    for (auto& sinkStat : taskStats.GetSinks()) {
        auto egressName = sinkStat.GetEgressName();
        if (egressName) {
            auto [it, inserted] = Egress.try_emplace(egressName, taskCount);
            auto& asyncBufferStats = it->second;
            UpdateAsyncStats(index, asyncBufferStats.Push, sinkStat.GetPush());
            UpdateAsyncStats(index, asyncBufferStats.Pop, sinkStat.GetPop());
            UpdateAsyncStats(index, asyncBufferStats.Ingress, sinkStat.GetEgress());
        }
    }

    SetNonZero(MaxMemoryUsage[index], maxMemoryUsage);
}

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
    if (value) {
        if (aggr->GetMin() == 0) {
            aggr->SetMin(value);
        } else {
            aggr->SetMin(std::min(aggr->GetMin(), value));
        }
        aggr->SetMax(std::max(aggr->GetMax(), value));
        aggr->SetSum(aggr->GetSum() + value);
        aggr->SetCnt(aggr->GetCnt() + 1);
    }
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

NDqProto::TDqStageStats* GetOrCreateStageStats(const NYql::NDq::TStageId& stageId,
    const TKqpTasksGraph& tasksGraph, NDqProto::TDqExecutionStats& execStats)
{
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

NDqProto::TDqStageStats* GetOrCreateStageStats(const NYql::NDqProto::TDqTaskStats& taskStats,
    const TKqpTasksGraph& tasksGraph, NDqProto::TDqExecutionStats& execStats)
{
    auto& task = tasksGraph.GetTask(taskStats.GetTaskId());
    return GetOrCreateStageStats(task.StageId, tasksGraph, execStats);
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

void TQueryExecutionStats::FillStageDurationUs(NYql::NDqProto::TDqStageStats& stats) {
    if (stats.HasStartTimeMs() && stats.HasFinishTimeMs()) {
        auto startTimeMs = stats.GetStartTimeMs().GetMin();
        auto finishTimeMs = stats.GetFinishTimeMs().GetMax();
        if (startTimeMs && finishTimeMs > startTimeMs) {
            stats.SetStageDurationUs((finishTimeMs - startTimeMs) * 1'000);
        }
    }
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
    UpdateAggr(stageStats->MutableResultRows(), task.GetResultRows());
    UpdateAggr(stageStats->MutableResultBytes(), task.GetResultBytes());
    UpdateAggr(stageStats->MutableIngressRows(), task.GetIngressRows());
    UpdateAggr(stageStats->MutableIngressBytes(), task.GetIngressBytes());
    UpdateAggr(stageStats->MutableEgressRows(), task.GetEgressRows());
    UpdateAggr(stageStats->MutableEgressBytes(), task.GetEgressBytes());

    UpdateAggr(stageStats->MutableStartTimeMs(), task.GetStartTimeMs());
    UpdateAggr(stageStats->MutableFinishTimeMs(), task.GetFinishTimeMs());
    UpdateAggr(stageStats->MutableDurationUs(), stats.GetDurationUs());
    UpdateAggr(stageStats->MutableWaitInputTimeUs(), task.GetWaitInputTimeUs());
    UpdateAggr(stageStats->MutableWaitOutputTimeUs(), task.GetWaitOutputTimeUs());
    FillStageDurationUs(*stageStats);

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
        ResultBytes += task.GetResultBytes();
        ResultRows += task.GetResultRows();
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

    UpdateAggr(stageStats->MutableStartTimeMs(), task.GetStartTimeMs());
    UpdateAggr(stageStats->MutableFinishTimeMs(), task.GetFinishTimeMs());
    // UpdateAggr(stageStats->MutableDurationUs(), ??? );
    UpdateAggr(stageStats->MutableWaitInputTimeUs(), task.GetWaitInputTimeUs());
    UpdateAggr(stageStats->MutableWaitOutputTimeUs(), task.GetWaitOutputTimeUs());
    FillStageDurationUs(*stageStats);

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

void TQueryExecutionStats::UpdateTaskStats(ui64 taskId, const NYql::NDqProto::TDqComputeActorStats& stats) {
    Y_ASSERT(stats.GetTasks().size() == 1);
    const NYql::NDqProto::TDqTaskStats& taskStats = stats.GetTasks(0);
    Y_ASSERT(taskStats.GetTaskId() == taskId);
    auto stageId = taskStats.GetStageId();
    auto [it, inserted] = StageStats.try_emplace(stageId);
    if (inserted) {
        it->second.StageId = TasksGraph->GetTask(taskStats.GetTaskId()).StageId;
    }
    it->second.UpdateStats(taskStats, stats.GetMaxMemoryUsage(), stats.GetDurationUs());
}

void ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsMinMax& stats) {
    ui64 count = 0;
    ui64 min = 0;
    ui64 max = 0;
    for (auto d : data) {
        if (d) {
            if (count) {
                if (min > d) min = d;
                if (max < d) max = d;
            } else {
                min = max = d;
            }
            count++;
        }
    }
    if (count) {
        stats.SetMin(min);
        stats.SetMax(max);
    }
}

void ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats) {
    ui64 count = 0;
    ui64 min = 0;
    ui64 max = 0;
    ui64 sum = 0;
    for (auto d : data) {
        if (d) {
            if (count) {
                if (min > d) min = d;
                if (max < d) max = d;
            } else {
                min = max = d;
            }
            sum += d;
            count++;
        }
    }
    if (count) {
        stats.SetMin(min);
        stats.SetMax(max);
        stats.SetSum(sum);
        stats.SetCnt(count);
    }
}

ui64 ExportAggStats(std::vector<ui64>& data) {
    ui64 sum = 0;
    for (auto d : data) {
        sum += d;
    }
    return sum;
}

void ExportAggAsyncStats(TAsyncStats& data, NYql::NDqProto::TDqAsyncStatsAggr& stats) {
    ExportAggStats(data.Bytes, *stats.MutableBytes());
    ExportAggStats(data.Rows, *stats.MutableRows());
    ExportAggStats(data.Chunks, *stats.MutableChunks());
    ExportAggStats(data.Splits, *stats.MutableSplits());
    ExportAggStats(data.FirstMessageMs, *stats.MutableFirstMessageMs());
    ExportAggStats(data.PauseMessageMs, *stats.MutablePauseMessageMs());
    ExportAggStats(data.ResumeMessageMs, *stats.MutableResumeMessageMs());
    ExportAggStats(data.LastMessageMs, *stats.MutableLastMessageMs());
    ExportAggStats(data.WaitTimeUs, *stats.MutableWaitTimeUs());
    ExportAggStats(data.WaitPeriods, *stats.MutableWaitPeriods());
    ExportAggStats(data.ActiveTimeUs, *stats.MutableActiveTimeUs());
}

void ExportAggAsyncBufferStats(TAsyncBufferStats& data, NYql::NDqProto::TDqAsyncBufferStatsAggr& stats) {
    ExportAggAsyncStats(data.Ingress, *stats.MutableIngress());
    ExportAggAsyncStats(data.Push, *stats.MutablePush());
    ExportAggAsyncStats(data.Pop, *stats.MutablePop());
    ExportAggAsyncStats(data.Egress, *stats.MutableEgress());
}

void TQueryExecutionStats::ExportExecStats(NYql::NDqProto::TDqExecutionStats& stats) {

    THashMap<ui32, NDqProto::TDqStageStats*> protoStages;
    for (auto& [stageId, stagetype] : TasksGraph->GetStagesInfo()) {
        protoStages.emplace(stageId.StageId, GetOrCreateStageStats(stageId, *TasksGraph, stats));
    }

    for (auto& p : StageStats) {
        auto& stageStats = *protoStages[p.second.StageId.StageId];
        stageStats.SetTotalTasksCount(p.second.Task2Index.size());

        ExportAggStats(p.second.CpuTimeUs, *stageStats.MutableCpuTimeUs());
        ExportAggStats(p.second.SourceCpuTimeUs, *stageStats.MutableSourceCpuTimeUs());

        ExportAggStats(p.second.InputRows, *stageStats.MutableInputRows());
        ExportAggStats(p.second.InputBytes, *stageStats.MutableInputBytes());
        ExportAggStats(p.second.OutputRows, *stageStats.MutableOutputRows());
        ExportAggStats(p.second.OutputBytes, *stageStats.MutableOutputBytes());
        ExportAggStats(p.second.ResultRows, *stageStats.MutableResultRows());
        ExportAggStats(p.second.ResultBytes, *stageStats.MutableResultBytes());
        ExportAggStats(p.second.IngressRows, *stageStats.MutableIngressRows());
        ExportAggStats(p.second.IngressBytes, *stageStats.MutableIngressBytes());
        ExportAggStats(p.second.EgressRows, *stageStats.MutableEgressRows());
        ExportAggStats(p.second.EgressBytes, *stageStats.MutableEgressBytes());

        ExportAggStats(p.second.StartTimeMs, *stageStats.MutableStartTimeMs());
        ExportAggStats(p.second.FinishTimeMs, *stageStats.MutableFinishTimeMs());
        ExportAggStats(p.second.DurationUs, *stageStats.MutableDurationUs());
        ExportAggStats(p.second.WaitInputTimeUs, *stageStats.MutableWaitInputTimeUs());
        ExportAggStats(p.second.WaitOutputTimeUs, *stageStats.MutableWaitOutputTimeUs());
        FillStageDurationUs(stageStats);

        for (auto& p2 : p.second.Tables) {
            auto& table = *stageStats.AddTables();
            table.SetTablePath(p2.first);
            ExportAggStats(p2.second.ReadRows, *table.MutableReadRows());
            ExportAggStats(p2.second.ReadBytes, *table.MutableReadBytes());
            ExportAggStats(p2.second.WriteRows, *table.MutableWriteRows());
            ExportAggStats(p2.second.WriteBytes, *table.MutableWriteBytes());
            ExportAggStats(p2.second.EraseRows, *table.MutableEraseRows());
            ExportAggStats(p2.second.EraseBytes, *table.MutableEraseBytes());
            table.SetAffectedPartitions(ExportAggStats(p2.second.AffectedPartitions));
        }
        for (auto& p2 : p.second.Ingress) {
            ExportAggAsyncBufferStats(p2.second, (*stageStats.MutableIngress())[p2.first]);
        }
        for (auto& p2 : p.second.Input) {
            ExportAggAsyncBufferStats(p2.second, (*stageStats.MutableInput())[p2.first]);
        }
        for (auto& p2 : p.second.Output) {
            ExportAggAsyncBufferStats(p2.second, (*stageStats.MutableOutput())[p2.first]);
        }
        for (auto& p2 : p.second.Egress) {
            ExportAggAsyncBufferStats(p2.second, (*stageStats.MutableEgress())[p2.first]);
        }
    }
}

void TQueryExecutionStats::Finish() {
//    Cerr << (TStringBuilder() << "-- finish: executerTime: " << ExecuterCpuTime.MicroSeconds() << Endl);

    THashMap<ui32, NDqProto::TDqStageStats*> protoStages;
    for (auto& [stageId, stagetype] : TasksGraph->GetStagesInfo()) {
        GetOrCreateStageStats(stageId, *TasksGraph, *Result);
    }

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + ExecuterCpuTime.MicroSeconds());
    Result->SetDurationUs(FinishTs.MicroSeconds() - StartTs.MicroSeconds());

    // Result->Result* feilds are (temporary?) commented out in proto due to lack of use
    //
    // Result->SetResultBytes(ResultBytes);
    // Result->SetResultRows(ResultRows);

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
    if (Cur.Defined) {
        Cur = TEntry();
    }

    Cur.Defined = true;
    Cur.ComputeTime += TDuration::MicroSeconds(stats.GetCpuTimeUs());
    for (auto& task : stats.GetTasks()) {
        for (auto& table: task.GetTables()) {
            Cur.ReadIOStat.Rows += table.GetReadRows();
            Cur.ReadIOStat.Bytes += table.GetReadBytes();
        }
    }
}

TProgressStat::TEntry TProgressStat::GetLastUsage() const {
    return Cur.Defined ? Cur - Total : Cur;
}

void TProgressStat::Update() {
    Total = Cur;
    Cur = TEntry();
}

} // namespace NKikimr::NKqp
