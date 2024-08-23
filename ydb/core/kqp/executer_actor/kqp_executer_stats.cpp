#include "kqp_executer_stats.h"


namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;

void ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats);

ui64 NonZeroMin(ui64 a, ui64 b) {
    return (b == 0) ? a : ((a == 0 || a > b) ? b : a);
}

void TTimeSeriesStats::ExportAggStats(ui64 baseTimeMs, NYql::NDqProto::TDqStatsAggr& stats) {
    NKikimr::NKqp::ExportAggStats(Values, stats);
    ExportHistory(baseTimeMs, stats);
}

void TTimeSeriesStats::ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqStatsAggr& stats) {
    Pack();
    if (!History.empty()) {
        for (auto& h : History) {
            auto& item = *stats.AddHistory();
            item.SetTimeMs((h.first <= baseTimeMs) ? 0 : (h.first - baseTimeMs));
            item.SetValue(h.second);
        }
    }
}

void TTimeSeriesStats::Resize(ui32 taskCount) {
    Values.resize(taskCount);
}

void TTimeSeriesStats::SetNonZero(ui32 taskIndex, ui64 value) {
    if (value) {
        Sum += value;
        Sum -= Values[taskIndex];
        Values[taskIndex] = value;
    }
    if (HistorySampleCount) {
        auto nowMs = Now().MilliSeconds();

        if (!History.empty() && History.back().first == nowMs) {
            History.back().second = Sum;
            return;
        }

        if (History.size() > 1 && History.back().second == Sum && History[History.size() - 2].second == Sum) {
            History.back().first = nowMs;
            return;
        }

        History.emplace_back(nowMs, Sum);
        if (History.size() >= HistorySampleCount * 2) {
            Pack();
        }
    }
}

void TTimeSeriesStats::Pack() {
    if (HistorySampleCount == 0) {
        History.clear();
        return;
    }
    if (History.size() > HistorySampleCount) {

        if (HistorySampleCount == 1) {
            History.front() = History.back();
            return;
        }
        if (HistorySampleCount == 2) {
            History[1] = History.back();
            History.resize(2);
            return;
        }

        std::vector<std::pair<ui64, ui64>> history;
        ui32 count = History.size();
        ui32 delta = count - HistorySampleCount;
        ui64 minTime = History.front().first;
        ui64 maxTime = History.back().first;
        ui64 deltaTime = (maxTime - minTime) / (HistorySampleCount - 1);
        bool first = true;
        ui64 nextTime = minTime;
        for (auto& h : History) {
            if (!first && delta && ((h.first < nextTime) || (delta + 1 == count))) {
                delta--;
            } else {
                history.push_back(h);
                nextTime += deltaTime;
                first = false;
            }
            count--;
        }
        History.swap(history);
    }
}

void TAsyncStats::Resize(ui32 taskCount) {
    Bytes.Resize(taskCount);
    DecompressedBytes.resize(taskCount);
    Rows.resize(taskCount);
    Chunks.resize(taskCount);
    Splits.resize(taskCount);
    FirstMessageMs.resize(taskCount);
    PauseMessageMs.resize(taskCount);
    ResumeMessageMs.resize(taskCount);
    LastMessageMs.resize(taskCount);
    WaitTimeUs.Resize(taskCount);
    WaitPeriods.resize(taskCount);
    ActiveTimeUs.resize(taskCount);
}

void TAsyncStats::SetHistorySampleCount(ui32 historySampleCount) {
    Bytes.HistorySampleCount = historySampleCount;
    WaitTimeUs.HistorySampleCount = historySampleCount;
}

void TAsyncStats::ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqAsyncStatsAggr& stats) {
    if (stats.HasBytes()) {
        Bytes.ExportHistory(baseTimeMs, *stats.MutableBytes());
    }
    if (stats.HasWaitTimeUs()) {
        WaitTimeUs.ExportHistory(baseTimeMs, *stats.MutableWaitTimeUs());
    }
}

void TAsyncBufferStats::Resize(ui32 taskCount) {
    Ingress.Resize(taskCount);
    Push.Resize(taskCount);
    Pop.Resize(taskCount);
    Egress.Resize(taskCount);
}

void TAsyncBufferStats::SetHistorySampleCount(ui32 historySampleCount) {
    Ingress.SetHistorySampleCount(historySampleCount);
    Push.SetHistorySampleCount(historySampleCount);
    Pop.SetHistorySampleCount(historySampleCount);
    Egress.SetHistorySampleCount(historySampleCount);
}

void TAsyncBufferStats::ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqAsyncBufferStatsAggr& stats) {
    if (stats.HasIngress()) {
        Ingress.ExportHistory(baseTimeMs, *stats.MutableIngress());
    }
    if (stats.HasPush()) {
        Push.ExportHistory(baseTimeMs, *stats.MutablePush());
    }
    if (stats.HasPop()) {
        Pop.ExportHistory(baseTimeMs, *stats.MutablePop());
    }
    if (stats.HasEgress()) {
        Egress.ExportHistory(baseTimeMs, *stats.MutableEgress());
    }
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
    CpuTimeUs.Resize(taskCount);
    SourceCpuTimeUs.resize(taskCount);

    InputRows.resize(taskCount);
    InputBytes.resize(taskCount);
    OutputRows.resize(taskCount);
    OutputBytes.resize(taskCount);
    ResultRows.resize(taskCount);
    ResultBytes.resize(taskCount);
    IngressRows.resize(taskCount);
    IngressBytes.resize(taskCount);
    IngressDecompressedBytes.resize(taskCount);
    EgressRows.resize(taskCount);
    EgressBytes.resize(taskCount);

    FinishTimeMs.resize(taskCount);
    StartTimeMs.resize(taskCount);
    DurationUs.resize(taskCount);
    WaitInputTimeUs.resize(taskCount);
    WaitOutputTimeUs.resize(taskCount);

    SpillingComputeBytes.Resize(taskCount);
    SpillingChannelBytes.Resize(taskCount);
    SpillingComputeTimeUs.Resize(taskCount);
    SpillingChannelTimeUs.Resize(taskCount);

    for (auto& p : Ingress) p.second.Resize(taskCount);
    for (auto& p : Input)   p.second.Resize(taskCount);
    for (auto& p : Output)  p.second.Resize(taskCount);
    for (auto& p : Egress)  p.second.Resize(taskCount);

    MaxMemoryUsage.Resize(taskCount);
}

void TStageExecutionStats::SetHistorySampleCount(ui32 historySampleCount) {
    HistorySampleCount = historySampleCount;
    CpuTimeUs.HistorySampleCount = historySampleCount;
    MaxMemoryUsage.HistorySampleCount = historySampleCount;
    SpillingComputeBytes.HistorySampleCount = historySampleCount;
    SpillingChannelBytes.HistorySampleCount = historySampleCount;
    SpillingComputeTimeUs.HistorySampleCount = historySampleCount;
    SpillingChannelTimeUs.HistorySampleCount = historySampleCount;
}

void TStageExecutionStats::ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqStageStats& stageStats) {
    if (stageStats.HasCpuTimeUs()) {
        CpuTimeUs.ExportHistory(baseTimeMs, *stageStats.MutableCpuTimeUs());
    }
    for (auto& p : *stageStats.MutableIngress()) {
        auto it = Ingress.find(p.first);
        if (it != Ingress.end()) {
            it->second.ExportHistory(baseTimeMs, p.second);
        }
    }
    for (auto& p : *stageStats.MutableInput()) {
        auto it = Input.find(p.first);
        if (it != Input.end()) {
            it->second.ExportHistory(baseTimeMs, p.second);
        }
    }
    for (auto& p : *stageStats.MutableOutput()) {
        auto it = Output.find(p.first);
        if (it != Output.end()) {
            it->second.ExportHistory(baseTimeMs, p.second);
        }
    }
    for (auto& p : *stageStats.MutableEgress()) {
        auto it = Egress.find(p.first);
        if (it != Egress.end()) {
            it->second.ExportHistory(baseTimeMs, p.second);
        }
    }
    if (stageStats.HasMaxMemoryUsage()) {
        MaxMemoryUsage.ExportHistory(baseTimeMs, *stageStats.MutableMaxMemoryUsage());
    }
    if (stageStats.HasSpillingComputeBytes()) {
        SpillingComputeBytes.ExportHistory(baseTimeMs, *stageStats.MutableSpillingComputeBytes());
    }
    if (stageStats.HasSpillingChannelBytes()) {
        SpillingChannelBytes.ExportHistory(baseTimeMs, *stageStats.MutableSpillingChannelBytes());
    }
    if (stageStats.HasSpillingComputeTimeUs()) {
        SpillingComputeTimeUs.ExportHistory(baseTimeMs, *stageStats.MutableSpillingComputeTimeUs());
    }
    if (stageStats.HasSpillingChannelTimeUs()) {
        SpillingChannelTimeUs.ExportHistory(baseTimeMs, *stageStats.MutableSpillingChannelTimeUs());
    }
}

void SetNonZero(ui64& target, ui64 source) {
    if (source) {
        target = source;
    }
}

ui64 TStageExecutionStats::UpdateAsyncStats(i32 index, TAsyncStats& aggrAsyncStats, const NYql::NDqProto::TDqAsyncBufferStats& asyncStats) {
    ui64 baseTimeMs = 0;

    aggrAsyncStats.Bytes.SetNonZero(index, asyncStats.GetBytes());
    SetNonZero(aggrAsyncStats.DecompressedBytes[index], asyncStats.GetDecompressedBytes());
    SetNonZero(aggrAsyncStats.Rows[index], asyncStats.GetRows());
    SetNonZero(aggrAsyncStats.Chunks[index], asyncStats.GetChunks());
    SetNonZero(aggrAsyncStats.Splits[index], asyncStats.GetSplits());

    auto firstMessageMs = asyncStats.GetFirstMessageMs();
    SetNonZero(aggrAsyncStats.FirstMessageMs[index], firstMessageMs);
    baseTimeMs = NonZeroMin(baseTimeMs, firstMessageMs);

    auto pauseMessageMs = asyncStats.GetPauseMessageMs();
    SetNonZero(aggrAsyncStats.PauseMessageMs[index], pauseMessageMs);
    baseTimeMs = NonZeroMin(baseTimeMs, pauseMessageMs);

    auto resumeMessageMs = asyncStats.GetResumeMessageMs();
    SetNonZero(aggrAsyncStats.ResumeMessageMs[index], resumeMessageMs);
    baseTimeMs = NonZeroMin(baseTimeMs, resumeMessageMs);

    auto lastMessageMs = asyncStats.GetLastMessageMs();
    SetNonZero(aggrAsyncStats.LastMessageMs[index], lastMessageMs);
    baseTimeMs = NonZeroMin(baseTimeMs, lastMessageMs);

    aggrAsyncStats.WaitTimeUs.SetNonZero(index, asyncStats.GetWaitTimeUs());
    SetNonZero(aggrAsyncStats.WaitPeriods[index], asyncStats.GetWaitPeriods());
    if (firstMessageMs && lastMessageMs > firstMessageMs) {
        aggrAsyncStats.ActiveTimeUs[index] = lastMessageMs - firstMessageMs;
    }

    return baseTimeMs;
}

ui64 TStageExecutionStats::UpdateStats(const NYql::NDqProto::TDqTaskStats& taskStats, ui64 maxMemoryUsage, ui64 durationUs) {
    auto taskId = taskStats.GetTaskId();
    auto it = Task2Index.find(taskId);
    ui64 baseTimeMs = 0;

    ui32 taskCount = Task2Index.size();

    ui32 index;
    if (it == Task2Index.end()) {
        index = taskCount++;
        Task2Index.emplace(taskId, index);
        Resize(taskCount);
    } else {
        index = it->second;
    }

    CpuTimeUs.SetNonZero(index, taskStats.GetCpuTimeUs());
    SetNonZero(SourceCpuTimeUs[index], taskStats.GetSourceCpuTimeUs());

    SetNonZero(InputRows[index], taskStats.GetInputRows());
    SetNonZero(InputBytes[index], taskStats.GetInputBytes());
    SetNonZero(OutputRows[index], taskStats.GetOutputRows());
    SetNonZero(OutputBytes[index], taskStats.GetOutputBytes());
    SetNonZero(ResultRows[index], taskStats.GetResultRows());
    SetNonZero(ResultBytes[index], taskStats.GetResultBytes());
    SetNonZero(IngressRows[index], taskStats.GetIngressRows());
    SetNonZero(IngressBytes[index], taskStats.GetIngressBytes());
    SetNonZero(IngressDecompressedBytes[index], taskStats.GetIngressDecompressedBytes());
    SetNonZero(EgressRows[index], taskStats.GetEgressRows());
    SetNonZero(EgressBytes[index], taskStats.GetEgressBytes());

    auto startTimeMs = taskStats.GetStartTimeMs();
    SetNonZero(StartTimeMs[index], startTimeMs);
    baseTimeMs = NonZeroMin(baseTimeMs, startTimeMs);

    auto finishTimeMs = taskStats.GetFinishTimeMs();
    SetNonZero(FinishTimeMs[index], finishTimeMs);
    baseTimeMs = NonZeroMin(baseTimeMs, finishTimeMs);

    SetNonZero(DurationUs[index], durationUs);
    SetNonZero(WaitInputTimeUs[index], taskStats.GetWaitInputTimeUs());
    SetNonZero(WaitOutputTimeUs[index], taskStats.GetWaitOutputTimeUs());

    SpillingComputeBytes.SetNonZero(index, taskStats.GetSpillingComputeWriteBytes());
    SpillingChannelBytes.SetNonZero(index, taskStats.GetSpillingChannelWriteBytes());
    SpillingComputeTimeUs.SetNonZero(index, taskStats.GetSpillingComputeReadTimeUs() + taskStats.GetSpillingComputeWriteTimeUs());
    SpillingChannelTimeUs.SetNonZero(index, taskStats.GetSpillingChannelReadTimeUs() + taskStats.GetSpillingChannelWriteTimeUs());

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
            if (inserted) {
                asyncBufferStats.SetHistorySampleCount(HistorySampleCount);
            }
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Ingress, sourceStat.GetIngress()));
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Push, sourceStat.GetPush()));
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Pop, sourceStat.GetPop()));
        }
    }

    for (auto& inputChannelStat : taskStats.GetInputChannels()) {
        auto stageId = inputChannelStat.GetSrcStageId();
        auto [it, inserted] = Input.try_emplace(stageId, taskCount);
        auto& asyncBufferStats = it->second;
        if (inserted) {
            asyncBufferStats.SetHistorySampleCount(HistorySampleCount);
        }
        baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Push, inputChannelStat.GetPush()));
        baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Pop, inputChannelStat.GetPop()));
    }

    for (auto& outputChannelStat : taskStats.GetOutputChannels()) {
        auto stageId = outputChannelStat.GetDstStageId();
        auto [it, inserted] = Output.try_emplace(stageId, taskCount);
        auto& asyncBufferStats = it->second;
        if (inserted) {
            asyncBufferStats.SetHistorySampleCount(HistorySampleCount);
        }
        baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Push, outputChannelStat.GetPush()));
        baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Pop, outputChannelStat.GetPop()));
    }

    for (auto& sinkStat : taskStats.GetSinks()) {
        auto egressName = sinkStat.GetEgressName();
        if (egressName) {
            auto [it, inserted] = Egress.try_emplace(egressName, taskCount);
            auto& asyncBufferStats = it->second;
            if (inserted) {
                asyncBufferStats.SetHistorySampleCount(HistorySampleCount);
            }
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Push, sinkStat.GetPush()));
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Pop, sinkStat.GetPop()));
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Ingress, sinkStat.GetEgress()));
        }
    }

    MaxMemoryUsage.SetNonZero(index, maxMemoryUsage);

    return baseTimeMs;
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
    ui64 DecompressedBytes = 0;
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

ui64 UpdateAsyncAggr(NDqProto::TDqAsyncStatsAggr& asyncAggr, const NDqProto::TDqAsyncBufferStats& asyncStat) noexcept {
    ui64 baseTimeMs = 0;

    UpdateAggr(asyncAggr.MutableBytes(), asyncStat.GetBytes());
    UpdateAggr(asyncAggr.MutableDecompressedBytes(), asyncStat.GetDecompressedBytes());
    UpdateAggr(asyncAggr.MutableRows(), asyncStat.GetRows());
    UpdateAggr(asyncAggr.MutableChunks(), asyncStat.GetChunks());
    UpdateAggr(asyncAggr.MutableSplits(), asyncStat.GetSplits());

    auto firstMessageMs = asyncStat.GetFirstMessageMs();
    if (firstMessageMs) {
        UpdateAggr(asyncAggr.MutableFirstMessageMs(), firstMessageMs);
        baseTimeMs = NonZeroMin(baseTimeMs, firstMessageMs);
    }

    auto pauseMessageMs = asyncStat.GetPauseMessageMs();
    if (pauseMessageMs) {
        UpdateAggr(asyncAggr.MutablePauseMessageMs(), pauseMessageMs);
        baseTimeMs = NonZeroMin(baseTimeMs, pauseMessageMs);
    }

    auto resumeMessageMs = asyncStat.GetResumeMessageMs();
    if (resumeMessageMs) {
        UpdateAggr(asyncAggr.MutableResumeMessageMs(), resumeMessageMs);
        baseTimeMs = NonZeroMin(baseTimeMs, resumeMessageMs);
    }

    auto lastMessageMs = asyncStat.GetLastMessageMs();
    if (lastMessageMs) {
        UpdateAggr(asyncAggr.MutableLastMessageMs(), lastMessageMs);
        baseTimeMs = NonZeroMin(baseTimeMs, lastMessageMs);
    }

    UpdateAggr(asyncAggr.MutableWaitTimeUs(), asyncStat.GetWaitTimeUs());
    UpdateAggr(asyncAggr.MutableWaitPeriods(), asyncStat.GetWaitPeriods());

    if (firstMessageMs && lastMessageMs >= firstMessageMs) {
        UpdateAggr(asyncAggr.MutableActiveTimeUs(), (lastMessageMs - firstMessageMs) * 1000);
    }

    return baseTimeMs;
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
    UpdateAggr(stageStats->MutableIngressDecompressedBytes(), task.GetIngressDecompressedBytes());
    UpdateAggr(stageStats->MutableEgressRows(), task.GetEgressRows());
    UpdateAggr(stageStats->MutableEgressBytes(), task.GetEgressBytes());

    auto startTimeMs = task.GetStartTimeMs();
    UpdateAggr(stageStats->MutableStartTimeMs(), startTimeMs);
    BaseTimeMs = NonZeroMin(BaseTimeMs, startTimeMs);

    auto finishTimeMs = task.GetFinishTimeMs();
    UpdateAggr(stageStats->MutableFinishTimeMs(), finishTimeMs);
    BaseTimeMs = NonZeroMin(BaseTimeMs, finishTimeMs);

    UpdateAggr(stageStats->MutableDurationUs(), stats.GetDurationUs());
    UpdateAggr(stageStats->MutableWaitInputTimeUs(), task.GetWaitInputTimeUs());
    UpdateAggr(stageStats->MutableWaitOutputTimeUs(), task.GetWaitOutputTimeUs());

    UpdateAggr(stageStats->MutableSpillingComputeBytes(), task.GetSpillingComputeWriteBytes());
    UpdateAggr(stageStats->MutableSpillingChannelBytes(), task.GetSpillingChannelWriteBytes());
    UpdateAggr(stageStats->MutableSpillingComputeTimeUs(), task.GetSpillingComputeReadTimeUs() + task.GetSpillingComputeWriteTimeUs());
    UpdateAggr(stageStats->MutableSpillingChannelTimeUs(), task.GetSpillingChannelReadTimeUs() + task.GetSpillingChannelWriteTimeUs());

    FillStageDurationUs(*stageStats);

    for (auto& sourcesStat : task.GetSources()) {
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableIngress())[sourcesStat.GetIngressName()].MutableIngress(), sourcesStat.GetIngress()));
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableIngress())[sourcesStat.GetIngressName()].MutablePush(),   sourcesStat.GetPush()));
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableIngress())[sourcesStat.GetIngressName()].MutablePop(),  sourcesStat.GetPop()));
    }
    for (auto& inputChannelStat : task.GetInputChannels()) {
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableInput())[inputChannelStat.GetSrcStageId()].MutablePush(), inputChannelStat.GetPush()));
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableInput())[inputChannelStat.GetSrcStageId()].MutablePop(), inputChannelStat.GetPop()));
    }
    for (auto& outputChannelStat : task.GetOutputChannels()) {
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableOutput())[outputChannelStat.GetDstStageId()].MutablePush(), outputChannelStat.GetPush()));
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableOutput())[outputChannelStat.GetDstStageId()].MutablePop(), outputChannelStat.GetPop()));
    }
    for (auto& sinksStat : task.GetSinks()) {
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableEgress())[sinksStat.GetEgressName()].MutablePush(),   sinksStat.GetPush()));
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableEgress())[sinksStat.GetEgressName()].MutablePop(),    sinksStat.GetPop()));
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*(*stageStats->MutableEgress())[sinksStat.GetEgressName()].MutableEgress(), sinksStat.GetEgress()));
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

    // TODO: dedup with AddComputeActorFullStatsByTask

    stageStats->SetTotalTasksCount(stageStats->GetTotalTasksCount() + 1);
    UpdateAggr(stageStats->MutableCpuTimeUs(), task.GetCpuTimeUs());
    UpdateAggr(stageStats->MutableInputRows(), task.GetInputRows());
    UpdateAggr(stageStats->MutableInputBytes(), task.GetInputBytes());
    UpdateAggr(stageStats->MutableOutputRows(), task.GetOutputRows());
    UpdateAggr(stageStats->MutableOutputBytes(), task.GetOutputBytes());

    auto startTimeMs = task.GetStartTimeMs();
    UpdateAggr(stageStats->MutableStartTimeMs(), startTimeMs);
    BaseTimeMs = NonZeroMin(BaseTimeMs, startTimeMs);

    auto finishTimeMs = task.GetFinishTimeMs();
    UpdateAggr(stageStats->MutableFinishTimeMs(), finishTimeMs);
    BaseTimeMs = NonZeroMin(BaseTimeMs, finishTimeMs);

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

void TQueryExecutionStats::AddDatashardStats(NKikimrQueryStats::TTxStats&& txStats) {
    ui64 datashardCpuTimeUs = 0;
    for (const auto& perShard : txStats.GetPerShardStats()) {
        AffectedShards.emplace(perShard.GetShardId());

        datashardCpuTimeUs += perShard.GetCpuTimeUsec();
        UpdateAggr(ExtraStats.MutableShardsCpuTimeUs(), perShard.GetCpuTimeUsec());
    }

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + datashardCpuTimeUs);

    if (CollectFullStats(StatsMode)) {
        DatashardStats.emplace_back(std::move(txStats));
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
        it->second.SetHistorySampleCount(HistorySampleCount);
    }
    BaseTimeMs = NonZeroMin(BaseTimeMs, it->second.UpdateStats(taskStats, stats.GetMaxMemoryUsage(), stats.GetDurationUs()));
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

void ExportOffsetAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats, ui64 offset) {
    ui64 count = 0;
    ui64 min = 0;
    ui64 max = 0;
    ui64 sum = 0;
    for (auto d : data) {
        d = (d <= offset) ? 0 : (d - offset);
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

void ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats) {
    ExportOffsetAggStats(data, stats, 0);
}

ui64 ExportAggStats(std::vector<ui64>& data) {
    ui64 sum = 0;
    for (auto d : data) {
        sum += d;
    }
    return sum;
}

void TQueryExecutionStats::ExportAggAsyncStats(TAsyncStats& data, NYql::NDqProto::TDqAsyncStatsAggr& stats) {
    data.Bytes.ExportAggStats(BaseTimeMs, *stats.MutableBytes());
    ExportAggStats(data.Rows, *stats.MutableRows());
    ExportAggStats(data.Chunks, *stats.MutableChunks());
    ExportAggStats(data.Splits, *stats.MutableSplits());
    ExportOffsetAggStats(data.FirstMessageMs, *stats.MutableFirstMessageMs(), BaseTimeMs);
    ExportOffsetAggStats(data.PauseMessageMs, *stats.MutablePauseMessageMs(), BaseTimeMs);
    ExportOffsetAggStats(data.ResumeMessageMs, *stats.MutableResumeMessageMs(), BaseTimeMs);
    ExportOffsetAggStats(data.LastMessageMs, *stats.MutableLastMessageMs(), BaseTimeMs);
    data.WaitTimeUs.ExportAggStats(BaseTimeMs, *stats.MutableWaitTimeUs());
    ExportAggStats(data.WaitPeriods, *stats.MutableWaitPeriods());
    ExportAggStats(data.ActiveTimeUs, *stats.MutableActiveTimeUs());
}

void TQueryExecutionStats::ExportAggAsyncBufferStats(TAsyncBufferStats& data, NYql::NDqProto::TDqAsyncBufferStatsAggr& stats) {
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

        stageStats.SetBaseTimeMs(BaseTimeMs);
        p.second.CpuTimeUs.ExportAggStats(BaseTimeMs, *stageStats.MutableCpuTimeUs());
        ExportAggStats(p.second.SourceCpuTimeUs, *stageStats.MutableSourceCpuTimeUs());
        p.second.MaxMemoryUsage.ExportAggStats(BaseTimeMs, *stageStats.MutableMaxMemoryUsage());

        ExportAggStats(p.second.InputRows, *stageStats.MutableInputRows());
        ExportAggStats(p.second.InputBytes, *stageStats.MutableInputBytes());
        ExportAggStats(p.second.OutputRows, *stageStats.MutableOutputRows());
        ExportAggStats(p.second.OutputBytes, *stageStats.MutableOutputBytes());
        ExportAggStats(p.second.ResultRows, *stageStats.MutableResultRows());
        ExportAggStats(p.second.ResultBytes, *stageStats.MutableResultBytes());
        ExportAggStats(p.second.IngressRows, *stageStats.MutableIngressRows());
        ExportAggStats(p.second.IngressBytes, *stageStats.MutableIngressBytes());
        ExportAggStats(p.second.IngressDecompressedBytes, *stageStats.MutableIngressDecompressedBytes());
        ExportAggStats(p.second.EgressRows, *stageStats.MutableEgressRows());
        ExportAggStats(p.second.EgressBytes, *stageStats.MutableEgressBytes());

        ExportOffsetAggStats(p.second.StartTimeMs, *stageStats.MutableStartTimeMs(), BaseTimeMs);
        ExportOffsetAggStats(p.second.FinishTimeMs, *stageStats.MutableFinishTimeMs(), BaseTimeMs);
        ExportAggStats(p.second.DurationUs, *stageStats.MutableDurationUs());
        ExportAggStats(p.second.WaitInputTimeUs, *stageStats.MutableWaitInputTimeUs());
        ExportAggStats(p.second.WaitOutputTimeUs, *stageStats.MutableWaitOutputTimeUs());

        p.second.SpillingComputeBytes.ExportAggStats(BaseTimeMs, *stageStats.MutableComputeSpillingBytes());
        p.second.SpillingChannelBytes.ExportAggStats(BaseTimeMs, *stageStats.MutableChannelSpillingBytes());
        p.second.SpillingComputeTimeUs.ExportAggStats(BaseTimeMs, *stageStats.MutableComputeSpillingTimeUs());
        p.second.SpillingChannelTimeUs.ExportAggStats(BaseTimeMs, *stageStats.MutableChannelSpillingTimeUs());

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

void TQueryExecutionStats::AdjustAsyncAggr(NYql::NDqProto::TDqAsyncStatsAggr& stats) {
    if (stats.HasFirstMessageMs()) {
        AdjustDqStatsAggr(*stats.MutableFirstMessageMs());
    }
    if (stats.HasPauseMessageMs()) {
        AdjustDqStatsAggr(*stats.MutablePauseMessageMs());
    }
    if (stats.HasResumeMessageMs()) {
        AdjustDqStatsAggr(*stats.MutableResumeMessageMs());
    }
    if (stats.HasLastMessageMs()) {
        AdjustDqStatsAggr(*stats.MutableLastMessageMs());
    }
}

void TQueryExecutionStats::AdjustAsyncBufferAggr(NYql::NDqProto::TDqAsyncBufferStatsAggr& stats) {
    if (stats.HasIngress()) {
        AdjustAsyncAggr(*stats.MutableIngress());
    }
    if (stats.HasPush()) {
        AdjustAsyncAggr(*stats.MutablePush());
    }
    if (stats.HasPop()) {
        AdjustAsyncAggr(*stats.MutablePop());
    }
    if (stats.HasEgress()) {
        AdjustAsyncAggr(*stats.MutableEgress());
    }
}

void TQueryExecutionStats::AdjustDqStatsAggr(NYql::NDqProto::TDqStatsAggr& stats) {
    if (auto min = stats.GetMin()) {
        stats.SetMin(min - BaseTimeMs);
    }
    if (auto max = stats.GetMax()) {
        stats.SetMax(max - BaseTimeMs);
    }
    if (auto cnt = stats.GetCnt()) {
        stats.SetSum(stats.GetSum() - BaseTimeMs * cnt);
    }
}

void TQueryExecutionStats::AdjustBaseTime(NDqProto::TDqStageStats* stageStats) {
    if (stageStats->HasStartTimeMs()) {
        AdjustDqStatsAggr(*stageStats->MutableStartTimeMs());
    }
    if (stageStats->HasFinishTimeMs()) {
        AdjustDqStatsAggr(*stageStats->MutableFinishTimeMs());
    }
    for (auto& p : *stageStats->MutableIngress()) {
        AdjustAsyncBufferAggr(p.second);
    }
    for (auto& p : *stageStats->MutableInput()) {
        AdjustAsyncBufferAggr(p.second);
    }
    for (auto& p : *stageStats->MutableOutput()) {
        AdjustAsyncBufferAggr(p.second);
    }
    for (auto& p : *stageStats->MutableEgress()) {
        AdjustAsyncBufferAggr(p.second);
    }
}

void TQueryExecutionStats::Finish() {
//    Cerr << (TStringBuilder() << "-- finish: executerTime: " << ExecuterCpuTime.MicroSeconds() << Endl);
    THashMap<ui32, NDqProto::TDqStageStats*> protoStages;

    for (auto& [stageId, stagetype] : TasksGraph->GetStagesInfo()) {
        auto stageStats = GetOrCreateStageStats(stageId, *TasksGraph, *Result);
        stageStats->SetBaseTimeMs(BaseTimeMs);
        AdjustBaseTime(stageStats);
        auto it = StageStats.find(stageId.StageId);
        if (it != StageStats.end()) {
            it->second.ExportHistory(BaseTimeMs, *stageStats);
        }
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
