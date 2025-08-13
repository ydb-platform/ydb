#include "kqp_executer_stats.h"


namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;

void ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats);

ui64 NonZeroMin(ui64 a, ui64 b) {
    return (b == 0) ? a : ((a == 0 || a > b) ? b : a);
}

ui64 ExportMinStats(std::vector<ui64>& data);
ui64 ExportMaxStats(std::vector<ui64>& data);

void TMinStats::Resize(ui32 count) {
    Values.resize(count);
}

void TMinStats::Set(ui32 index, ui64 value) {
    AFL_ENSURE(index < Values.size());
    auto maybeMin = Values[index] == MinValue;
    Values[index] = value;
    if (maybeMin) {
        MinValue = ExportMinStats(Values);
    }
}

void TMaxStats::Resize(ui32 count) {
    Values.resize(count);
}

void TMaxStats::Set(ui32 index, ui64 value) {
    AFL_ENSURE(index < Values.size());
    auto isMonotonic = value >= Values[index];
    Values[index] = value;
    MaxValue = isMonotonic ? (value > MaxValue ? value : MaxValue) : ExportMaxStats(Values);
}

void TTimeSeriesStats::ExportAggStats(NYql::NDqProto::TDqStatsAggr& stats) {
    NKikimr::NKqp::ExportAggStats(Values, stats);
}

void TTimeSeriesStats::ExportAggStats(ui64 baseTimeMs, NYql::NDqProto::TDqStatsAggr& stats) {
    ExportAggStats(stats);
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

void TTimeSeriesStats::Resize(ui32 count) {
    Values.resize(count);
}

void TTimeSeriesStats::SetNonZero(ui32 index, ui64 value) {
    if (value) {
        AFL_ENSURE(index < Values.size());
        Sum += value;
        Sum -= Values[index];
        Values[index] = value;
        AppendHistory();
    }
}

void TTimeSeriesStats::AppendHistory() {
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

void TPartitionedStats::ResizeByTasks(ui32 taskCount) {
    for (auto& p : Parts) {
        p.resize(taskCount);
    }
}

void TPartitionedStats::ResizeByParts(ui32 partCount, ui32 taskCount) {
    auto oldPartCount = Parts.size();
    Parts.resize(partCount);
    for(auto i = oldPartCount; i < partCount; i++) {
        Parts[i].resize(taskCount);
    }
    Resize(partCount);
}

void TPartitionedStats::SetNonZeroAggSum(ui32 taskIndex, ui32 partIndex, ui64 value, bool recordTimeSeries) {
    if (value) {
        AFL_ENSURE(partIndex < Parts.size());
        auto& part = Parts[partIndex];
        auto delta = value - part[taskIndex];
        AFL_ENSURE(taskIndex < part.size());
        part[taskIndex] = value;
        AFL_ENSURE(partIndex < Values.size());
        Values[partIndex] += delta;
        Sum += delta;
        if (recordTimeSeries) {
            AppendHistory();
        }
    }
}

void TPartitionedStats::SetNonZeroAggMin(ui32 taskIndex, ui32 partIndex, ui64 value, bool recordTimeSeries) {
    if (value) {
        AFL_ENSURE(partIndex < Parts.size());
        auto& part = Parts[partIndex];
        AFL_ENSURE(taskIndex < part.size());
        part[taskIndex] = value;
        AFL_ENSURE(partIndex < Values.size());
        if (Values[partIndex] == 0 || value < Values[partIndex]) {
            // Min/Max is related to Parts[] only, Values[] should kepp count Sum as well
            Sum = Sum + value - Values[partIndex];
            Values[partIndex] = value;
            if (recordTimeSeries) {
                AppendHistory();
            }
        }
    }
}

void TPartitionedStats::SetNonZeroAggMax(ui32 taskIndex, ui32 partIndex, ui64 value, bool recordTimeSeries) {
    if (value) {
        AFL_ENSURE(partIndex < Parts.size());
        auto& part = Parts[partIndex];
        AFL_ENSURE(taskIndex < part.size());
        part[taskIndex] = value;
        AFL_ENSURE(partIndex < Values.size());
        if (value > Values[partIndex]) {
            // Min/Max is related to Parts[] only, Values[] should kepp count Sum as well
            Sum = Sum + value - Values[partIndex];
            Values[partIndex] = value;
            if (recordTimeSeries) {
                AppendHistory();
            }
        }
    }
}

void TTimeMultiSeriesStats::SetNonZero(TPartitionedStats& stats, ui32 taskIndex, const TString& key, ui64 value, bool recordTimeSeries, EPartitionedAggKind aggKind) {
    auto [it, inserted] = Indices.try_emplace(key);
    if (inserted) {
        it->second = Indices.size() - 1;
        if (PartCount < Indices.size()) {
            PartCount += 4;
        }
    }
    if (stats.Parts.size() < PartCount) {
        stats.ResizeByParts(PartCount, TaskCount);
    }

    switch (aggKind) {
        case EPartitionedAggKind::PartitionedAggSum:
            stats.SetNonZeroAggSum(taskIndex, it->second, value, recordTimeSeries);
            break;
        case EPartitionedAggKind::PartitionedAggMin:
            stats.SetNonZeroAggMin(taskIndex, it->second, value, recordTimeSeries);
            break;
        case EPartitionedAggKind::PartitionedAggMax:
            stats.SetNonZeroAggMax(taskIndex, it->second, value, recordTimeSeries);
            break;
    }
}

void TExternalStats::Resize(ui32 taskCount) {
    ExternalRows.ResizeByTasks(taskCount);
    ExternalBytes.ResizeByTasks(taskCount);
    FirstMessageMs.ResizeByTasks(taskCount);
    LastMessageMs.ResizeByTasks(taskCount);
    CpuTimeUs.ResizeByTasks(taskCount);
    WaitInputTimeUs.ResizeByTasks(taskCount);
    WaitOutputTimeUs.ResizeByTasks(taskCount);
    TaskCount = taskCount;
}

void TExternalStats::SetHistorySampleCount(ui32 historySampleCount) {
    ExternalBytes.HistorySampleCount = historySampleCount;
    CpuTimeUs.HistorySampleCount = historySampleCount;
}

void TExternalStats::ExportHistory(ui64 baseTimeMs, NDqProto::TDqExternalAggrStats& stats) {
    if (stats.HasExternalBytes()) {
        ExternalBytes.ExportHistory(baseTimeMs, *stats.MutableExternalBytes());
    }
    if (stats.HasCpuTimeUs()) {
        CpuTimeUs.ExportHistory(baseTimeMs, *stats.MutableCpuTimeUs());
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
    External.Resize(taskCount);
    Ingress.Resize(taskCount);
    Push.Resize(taskCount);
    Pop.Resize(taskCount);
    Egress.Resize(taskCount);
}

void TAsyncBufferStats::SetHistorySampleCount(ui32 historySampleCount) {
    External.SetHistorySampleCount(historySampleCount);
    Ingress.SetHistorySampleCount(historySampleCount);
    Push.SetHistorySampleCount(historySampleCount);
    Pop.SetHistorySampleCount(historySampleCount);
    Egress.SetHistorySampleCount(historySampleCount);
}

void TAsyncBufferStats::ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqAsyncBufferStatsAggr& stats) {
    if (stats.HasExternal()) {
        External.ExportHistory(baseTimeMs, *stats.MutableExternal());
    }
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

void TOperatorStats::Resize(ui32 taskCount) {
    Rows.resize(taskCount);
    Bytes.resize(taskCount);
}

void TStageExecutionStats::Resize(ui32 taskCount) {

    AFL_ENSURE((taskCount & 3) == 0);

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

    WaitInputTimeUs.Resize(taskCount);
    WaitOutputTimeUs.Resize(taskCount);
    CurrentWaitInputTimeUs.Resize(taskCount);
    CurrentWaitOutputTimeUs.Resize(taskCount);

    SpillingComputeBytes.Resize(taskCount);
    SpillingChannelBytes.Resize(taskCount);
    SpillingComputeTimeUs.Resize(taskCount);
    SpillingChannelTimeUs.Resize(taskCount);

    for (auto& [_, t] : Tables) t.Resize(taskCount);

    for (auto& [_, i] : Ingress) i.Resize(taskCount);
    for (auto& [_, i] : Input)   i.Resize(taskCount);
    for (auto& [_, o] : Output)  o.Resize(taskCount);
    for (auto& [_, e] : Egress)  e.Resize(taskCount);

    for (auto& [_, j] : Joins) j.Resize(taskCount);
    for (auto& [_, f] : Filters) f.Resize(taskCount);
    for (auto& [_, a] : Aggregations) a.Resize(taskCount);

    MaxMemoryUsage.Resize(taskCount);
    Finished.resize(taskCount);
}

void TStageExecutionStats::SetHistorySampleCount(ui32 historySampleCount) {
    HistorySampleCount = historySampleCount;
    CpuTimeUs.HistorySampleCount = historySampleCount;
    MaxMemoryUsage.HistorySampleCount = historySampleCount;

    WaitInputTimeUs.HistorySampleCount = historySampleCount;
    WaitOutputTimeUs.HistorySampleCount = historySampleCount;

    SpillingComputeBytes.HistorySampleCount = historySampleCount;
    SpillingChannelBytes.HistorySampleCount = historySampleCount;
    SpillingComputeTimeUs.HistorySampleCount = historySampleCount;
    SpillingChannelTimeUs.HistorySampleCount = historySampleCount;

    for (auto& [_, i] : Ingress) i.SetHistorySampleCount(historySampleCount);
    for (auto& [_, i] : Input)   i.SetHistorySampleCount(historySampleCount);
    for (auto& [_, o] : Output)  o.SetHistorySampleCount(historySampleCount);
    for (auto& [_, e] : Egress)  e.SetHistorySampleCount(historySampleCount);
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
    if (stageStats.HasWaitInputTimeUs()) {
        WaitInputTimeUs.ExportHistory(baseTimeMs, *stageStats.MutableWaitInputTimeUs());
    }
    if (stageStats.HasWaitOutputTimeUs()) {
        WaitOutputTimeUs.ExportHistory(baseTimeMs, *stageStats.MutableWaitOutputTimeUs());
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

inline void SetNonZero(ui64& target, ui64 source) {
    if (source) {
        target = source;
    }
}

inline void SetNonZero(std::vector<ui64>& vector, ui32 index, ui64 value) {
    AFL_ENSURE(index < vector.size());
    SetNonZero(vector[index], value);
}

ui64 TStageExecutionStats::UpdateAsyncStats(ui32 index, TAsyncStats& aggrAsyncStats, const NYql::NDqProto::TDqAsyncBufferStats& asyncStats) {
    ui64 baseTimeMs = 0;

    aggrAsyncStats.Bytes.SetNonZero(index, asyncStats.GetBytes());
    SetNonZero(aggrAsyncStats.DecompressedBytes, index, asyncStats.GetDecompressedBytes());
    SetNonZero(aggrAsyncStats.Rows, index, asyncStats.GetRows());
    SetNonZero(aggrAsyncStats.Chunks, index, asyncStats.GetChunks());
    SetNonZero(aggrAsyncStats.Splits, index, asyncStats.GetSplits());

    auto firstMessageMs = asyncStats.GetFirstMessageMs();
    SetNonZero(aggrAsyncStats.FirstMessageMs, index, firstMessageMs);
    baseTimeMs = NonZeroMin(baseTimeMs, firstMessageMs);

    auto pauseMessageMs = asyncStats.GetPauseMessageMs();
    SetNonZero(aggrAsyncStats.PauseMessageMs, index, pauseMessageMs);
    baseTimeMs = NonZeroMin(baseTimeMs, pauseMessageMs);

    auto resumeMessageMs = asyncStats.GetResumeMessageMs();
    SetNonZero(aggrAsyncStats.ResumeMessageMs, index, resumeMessageMs);
    baseTimeMs = NonZeroMin(baseTimeMs, resumeMessageMs);

    auto lastMessageMs = asyncStats.GetLastMessageMs();
    SetNonZero(aggrAsyncStats.LastMessageMs, index, lastMessageMs);
    baseTimeMs = NonZeroMin(baseTimeMs, lastMessageMs);

    aggrAsyncStats.WaitTimeUs.SetNonZero(index, asyncStats.GetWaitTimeUs());
    SetNonZero(aggrAsyncStats.WaitPeriods, index, asyncStats.GetWaitPeriods());
    if (firstMessageMs && lastMessageMs > firstMessageMs) {
        AFL_ENSURE(index < aggrAsyncStats.ActiveTimeUs.size());
        aggrAsyncStats.ActiveTimeUs[index] = lastMessageMs - firstMessageMs;
    }

    return baseTimeMs;
}

ui64 TStageExecutionStats::UpdateStats(const NYql::NDqProto::TDqTaskStats& taskStats, NYql::NDqProto::EComputeState state, ui64 maxMemoryUsage, ui64 durationUs) {
    auto taskId = taskStats.GetTaskId();
    auto it = Task2Index.find(taskId);
    ui64 baseTimeMs = 0;

    AFL_ENSURE(TaskCount >= Task2Index.size());

    ui32 index;
    if (it == Task2Index.end()) {
        if (TaskCount == Task2Index.size()) {
            TaskCount += 4;
            Resize(TaskCount);
        }
        index = Task2Index.size();
        Task2Index.emplace(taskId, index);
    } else {
        index = it->second;
    }

    if (state == NYql::NDqProto::COMPUTE_STATE_FINISHED) {
        if (!Finished[index]) {
            Finished[index] = true;
            FinishedCount++;
        }
    }

    CpuTimeUs.SetNonZero(index, taskStats.GetCpuTimeUs());
    SetNonZero(SourceCpuTimeUs, index, taskStats.GetSourceCpuTimeUs());

    SetNonZero(InputRows, index, taskStats.GetInputRows());
    SetNonZero(InputBytes, index, taskStats.GetInputBytes());
    SetNonZero(OutputRows, index, taskStats.GetOutputRows());
    SetNonZero(OutputBytes, index, taskStats.GetOutputBytes());
    SetNonZero(ResultRows, index, taskStats.GetResultRows());
    SetNonZero(ResultBytes, index, taskStats.GetResultBytes());
    SetNonZero(IngressRows, index, taskStats.GetIngressRows());
    SetNonZero(IngressBytes, index, taskStats.GetIngressBytes());
    SetNonZero(IngressDecompressedBytes, index, taskStats.GetIngressDecompressedBytes());
    SetNonZero(EgressRows, index, taskStats.GetEgressRows());
    SetNonZero(EgressBytes, index, taskStats.GetEgressBytes());

    auto startTimeMs = taskStats.GetStartTimeMs();
    SetNonZero(StartTimeMs, index, startTimeMs);
    baseTimeMs = NonZeroMin(baseTimeMs, startTimeMs);

    auto finishTimeMs = taskStats.GetFinishTimeMs();
    SetNonZero(FinishTimeMs, index, finishTimeMs);
    baseTimeMs = NonZeroMin(baseTimeMs, finishTimeMs);

    SetNonZero(DurationUs, index, durationUs);
    WaitInputTimeUs.SetNonZero(index, taskStats.GetWaitInputTimeUs());
    WaitOutputTimeUs.SetNonZero(index, taskStats.GetWaitOutputTimeUs());
    CurrentWaitInputTimeUs.Set(index, taskStats.GetCurrentWaitInputTimeUs());
    CurrentWaitOutputTimeUs.Set(index, taskStats.GetCurrentWaitOutputTimeUs());

    auto updateTimeMs = taskStats.GetUpdateTimeMs();
    UpdateTimeMs = std::max(UpdateTimeMs, updateTimeMs);
    baseTimeMs = NonZeroMin(baseTimeMs, updateTimeMs);

    SpillingComputeBytes.SetNonZero(index, taskStats.GetSpillingComputeWriteBytes());
    SpillingChannelBytes.SetNonZero(index, taskStats.GetSpillingChannelWriteBytes());
    SpillingComputeTimeUs.SetNonZero(index, taskStats.GetSpillingComputeReadTimeUs() + taskStats.GetSpillingComputeWriteTimeUs());
    SpillingChannelTimeUs.SetNonZero(index, taskStats.GetSpillingChannelReadTimeUs() + taskStats.GetSpillingChannelWriteTimeUs());

    for (auto& tableStat : taskStats.GetTables()) {
        auto tablePath = tableStat.GetTablePath();
        auto [it, inserted] = Tables.try_emplace(tablePath, TaskCount);
        auto& aggrTableStats = it->second;
        SetNonZero(aggrTableStats.ReadRows, index, tableStat.GetReadRows());
        SetNonZero(aggrTableStats.ReadBytes, index, tableStat.GetReadBytes());
        SetNonZero(aggrTableStats.WriteRows, index, tableStat.GetWriteRows());
        SetNonZero(aggrTableStats.WriteBytes, index, tableStat.GetWriteBytes());
        SetNonZero(aggrTableStats.EraseRows, index, tableStat.GetEraseRows());
        SetNonZero(aggrTableStats.EraseBytes, index, tableStat.GetEraseBytes());
        SetNonZero(aggrTableStats.AffectedPartitions, index, tableStat.GetAffectedPartitions());
    }

    for (auto& sourceStat : taskStats.GetSources()) {
        auto ingressName = sourceStat.GetIngressName();
        if (ingressName) {
            auto [it, inserted] = Ingress.try_emplace(ingressName, TaskCount);
            auto& asyncBufferStats = it->second;
            if (inserted) {
                asyncBufferStats.SetHistorySampleCount(HistorySampleCount);
            }
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Ingress, sourceStat.GetIngress()));
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Push, sourceStat.GetPush()));
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Pop, sourceStat.GetPop()));
            for (auto& partitionStat : sourceStat.GetExternalPartitions()) {
                auto key = partitionStat.GetPartitionId();
                asyncBufferStats.External.SetNonZero(asyncBufferStats.External.ExternalRows,
                    index, key, partitionStat.GetExternalRows(), false, EPartitionedAggKind::PartitionedAggSum);
                asyncBufferStats.External.SetNonZero(asyncBufferStats.External.ExternalBytes,
                    index, key, partitionStat.GetExternalBytes(), true, EPartitionedAggKind::PartitionedAggSum);
                asyncBufferStats.External.SetNonZero(asyncBufferStats.External.FirstMessageMs,
                    index, key, partitionStat.GetFirstMessageMs(), false, EPartitionedAggKind::PartitionedAggMin);
                asyncBufferStats.External.SetNonZero(asyncBufferStats.External.LastMessageMs,
                    index, key, partitionStat.GetLastMessageMs(), false, EPartitionedAggKind::PartitionedAggMax);
                asyncBufferStats.External.SetNonZero(asyncBufferStats.External.CpuTimeUs,
                    index, key, partitionStat.GetCpuTimeUs(), true, EPartitionedAggKind::PartitionedAggMax);
                asyncBufferStats.External.SetNonZero(asyncBufferStats.External.WaitInputTimeUs,
                    index, key, partitionStat.GetWaitInputTimeUs(), false, EPartitionedAggKind::PartitionedAggMax);
                asyncBufferStats.External.SetNonZero(asyncBufferStats.External.WaitOutputTimeUs,
                    index, key, partitionStat.GetWaitOutputTimeUs(), false, EPartitionedAggKind::PartitionedAggMax);
                asyncBufferStats.External.SetNonZero(asyncBufferStats.External.Finished,
                    index, key, partitionStat.GetFinished(), false, EPartitionedAggKind::PartitionedAggMax);
            }
        }
    }

    for (auto& inputChannelStat : taskStats.GetInputChannels()) {
        auto stageId = inputChannelStat.GetSrcStageId();
        auto [it, inserted] = Input.try_emplace(stageId, TaskCount);
        auto& asyncBufferStats = it->second;
        if (inserted) {
            asyncBufferStats.SetHistorySampleCount(HistorySampleCount);
        }
        baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Push, inputChannelStat.GetPush()));
        baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Pop, inputChannelStat.GetPop()));
    }

    for (auto& outputChannelStat : taskStats.GetOutputChannels()) {
        auto stageId = outputChannelStat.GetDstStageId();
        auto [it, inserted] = Output.try_emplace(stageId, TaskCount);
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
            auto [it, inserted] = Egress.try_emplace(egressName, TaskCount);
            auto& asyncBufferStats = it->second;
            if (inserted) {
                asyncBufferStats.SetHistorySampleCount(HistorySampleCount);
            }
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Push, sinkStat.GetPush()));
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Pop, sinkStat.GetPop()));
            baseTimeMs = NonZeroMin(baseTimeMs, UpdateAsyncStats(index, asyncBufferStats.Ingress, sinkStat.GetEgress()));
        }
    }

    for (auto& operatorStat : taskStats.GetOperators()) {
        auto operatorId = operatorStat.GetOperatorId();
        if (operatorId) {
            switch (operatorStat.GetTypeCase()) {
                case NYql::NDqProto::TDqOperatorStats::kJoin: {
                    auto [it, inserted] = Joins.try_emplace(operatorId, TaskCount);
                    auto& joinStats = it->second;
                    SetNonZero(joinStats.Rows, index, operatorStat.GetRows());
                    SetNonZero(joinStats.Bytes, index, operatorStat.GetBytes());
                    break;
                }
                case NYql::NDqProto::TDqOperatorStats::kFilter: {
                    auto [it, inserted] = Filters.try_emplace(operatorId, TaskCount);
                    auto& filterStats = it->second;
                    SetNonZero(filterStats.Rows, index, operatorStat.GetRows());
                    SetNonZero(filterStats.Bytes, index, operatorStat.GetBytes());
                    break;
                }
                case NYql::NDqProto::TDqOperatorStats::kAggregation: {
                    auto [it, inserted] = Aggregations.try_emplace(operatorId, TaskCount);
                    auto& aggStats = it->second;
                    SetNonZero(aggStats.Rows, index, operatorStat.GetRows());
                    SetNonZero(aggStats.Bytes, index, operatorStat.GetBytes());
                    break;
                }
                default:
                    break;
            }
        }
    }

    MaxMemoryUsage.SetNonZero(index, maxMemoryUsage);

    return baseTimeMs;
}

bool TStageExecutionStats::IsDeadlocked(ui64 deadline) {
    if (CurrentWaitInputTimeUs.MinValue < deadline || InputStages.empty()) {
        return false;
    }

    for (auto stat : InputStages) {
        if (stat->IsFinished()) {
            if (stat->MaxFinishTimeMs == 0) {
                stat->MaxFinishTimeMs = ExportMaxStats(stat->FinishTimeMs);
            }
            if (stat->UpdateTimeMs < stat->MaxFinishTimeMs || stat->UpdateTimeMs - stat->MaxFinishTimeMs < deadline) {
                return false;
            }
        } else {
            if (stat->CurrentWaitOutputTimeUs.MinValue < deadline) {
                return false;
            }
        }
    }
    return true;
}

bool TStageExecutionStats::IsFinished() {
    return FinishedCount == Task2Index.size();
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

void MergeAggr(NDqProto::TDqStatsAggr& aggr, const NDqProto::TDqStatsAggr& stat) noexcept {
    aggr.SetMin(NonZeroMin(aggr.GetMin(), stat.GetMin()));
    aggr.SetMax(std::max(aggr.GetMax(), stat.GetMax()));
    aggr.SetSum(aggr.GetSum() + stat.GetSum());
    aggr.SetCnt(aggr.GetCnt() + stat.GetCnt());
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

void MergeExternal(NDqProto::TDqExternalAggrStats& asyncAggr, const NDqProto::TDqExternalAggrStats& asyncStat) noexcept {
    MergeAggr(*asyncAggr.MutableExternalRows(), asyncStat.GetExternalRows());
    MergeAggr(*asyncAggr.MutableExternalBytes(), asyncStat.GetExternalBytes());
    MergeAggr(*asyncAggr.MutableStorageRows(), asyncStat.GetStorageRows());
    MergeAggr(*asyncAggr.MutableStorageBytes(), asyncStat.GetStorageBytes());
    MergeAggr(*asyncAggr.MutableCpuTimeUs(), asyncStat.GetCpuTimeUs());
    MergeAggr(*asyncAggr.MutableWaitInputTimeUs(), asyncStat.GetWaitInputTimeUs());
    MergeAggr(*asyncAggr.MutableWaitOutputTimeUs(), asyncStat.GetWaitOutputTimeUs());
    MergeAggr(*asyncAggr.MutableFirstMessageMs(), asyncStat.GetFirstMessageMs());
    MergeAggr(*asyncAggr.MutableLastMessageMs(), asyncStat.GetLastMessageMs());
    asyncAggr.SetPartitionCount(asyncAggr.GetPartitionCount() + asyncStat.GetExternalRows().GetCnt());
    asyncAggr.SetFinishedPartitionCount(asyncAggr.GetFinishedPartitionCount() + asyncStat.GetFinishedPartitionCount());
}

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
    for (const auto& intro: stageInfo.Introspections) {
        newStage->AddIntrospections(intro);
    }
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

void TQueryExecutionStats::Prepare() {
    if (CollectFullStats(StatsMode)) {
        // stages
        for (auto& [stageId, info] : TasksGraph->GetStagesInfo()) {
            auto [it, inserted] = StageStats.try_emplace(stageId);
            Y_ENSURE(inserted);
            it->second.StageId = stageId;
            if (info.Meta.ColumnTableInfoPtr) {
                it->second.Ingress["CS"].External.PartitionCount = info.Meta.ColumnTableInfoPtr->Description.GetColumnShardCount();
            }
        }
        // tasks
        for (auto& task : TasksGraph->GetTasks()) {
            auto& stageStats = StageStats[task.StageId];
            stageStats.Task2Index.emplace(task.Id, stageStats.Task2Index.size());
        }
        // connections
        for (auto& [_, stageStats] : StageStats) {
            auto& info = TasksGraph->GetStageInfo(stageStats.StageId);
            auto& stage = info.Meta.GetStage(info.Id);
            for (const auto& input : stage.GetInputs()) {
                auto& peerStageStats = StageStats[NYql::NDq::TStageId(stageStats.StageId.TxId, input.GetStageIndex())];
                stageStats.InputStages.push_back(&peerStageStats);
                stageStats.Input.emplace(peerStageStats.StageId.StageId, 0);
                peerStageStats.OutputStages.push_back(&stageStats);
                peerStageStats.Output.emplace(stageStats.StageId.StageId, 0);
            }
        }
        // stages postprocessing
        for (auto& [_, stageStats] : StageStats) {
            stageStats.TaskCount = (stageStats.Task2Index.size() + 3) & ~3;
            stageStats.Resize(stageStats.TaskCount);
            stageStats.SetHistorySampleCount(HistorySampleCount);
        }
    }
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

ui64 TQueryExecutionStats::EstimateCollectMem() {
    ui64 result = 0;
    for (auto& [_, stageStat] : StageStats) {
        result += stageStat.EstimateMem();
    }
    return result;
}

ui64 TQueryExecutionStats::EstimateFinishMem() {
    return Result->ByteSizeLong();
}

void TQueryExecutionStats::AddComputeActorFullStatsByTask(
        const NYql::NDqProto::TDqTaskStats& task,
        const NYql::NDqProto::TDqComputeActorStats& stats,
        NYql::NDqProto::EComputeState state
    ) {
    auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);

    stageStats->SetTotalTasksCount(stageStats->GetTotalTasksCount() + 1);
    if (state == NYql::NDqProto::COMPUTE_STATE_FINISHED) {
        stageStats->SetFinishedTasksCount(stageStats->GetFinishedTasksCount() + 1);
    }
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

    auto updateTimeMs = task.GetUpdateTimeMs();
    stageStats->SetUpdateTimeMs(std::max(stageStats->GetUpdateTimeMs(), updateTimeMs));
    BaseTimeMs = NonZeroMin(BaseTimeMs, updateTimeMs);

    UpdateAggr(stageStats->MutableSpillingComputeBytes(), task.GetSpillingComputeWriteBytes());
    UpdateAggr(stageStats->MutableSpillingChannelBytes(), task.GetSpillingChannelWriteBytes());
    UpdateAggr(stageStats->MutableSpillingComputeTimeUs(), task.GetSpillingComputeReadTimeUs() + task.GetSpillingComputeWriteTimeUs());
    UpdateAggr(stageStats->MutableSpillingChannelTimeUs(), task.GetSpillingChannelReadTimeUs() + task.GetSpillingChannelWriteTimeUs());

    FillStageDurationUs(*stageStats);

    for (auto& sourcesStat : task.GetSources()) {
        auto& ingress = (*stageStats->MutableIngress())[sourcesStat.GetIngressName()];
        MergeExternal(*ingress.MutableExternal(), sourcesStat.GetExternal());

        const auto& [it, inserted] = ExternalPartitionStats.emplace(stageStats->GetStageId(), sourcesStat.GetIngressName());
        auto& externalPartitionStat = it->second;

        for (auto& externalPartition : sourcesStat.GetExternalPartitions()) {
            const auto& [it, inserted] = externalPartitionStat.Stat.emplace(externalPartition.GetPartitionId(),
                TExternalPartitionStat(externalPartition.GetExternalRows(), externalPartition.GetExternalBytes(),
                externalPartition.GetFirstMessageMs(), externalPartition.GetLastMessageMs(), externalPartition.GetCpuTimeUs(),
                externalPartition.GetWaitInputTimeUs(), externalPartition.GetWaitOutputTimeUs(), externalPartition.GetFinished()));
            if (!inserted) {
                it->second.ExternalRows += externalPartition.GetExternalRows();
                it->second.ExternalBytes += externalPartition.GetExternalBytes();
                it->second.FirstMessageMs = NonZeroMin(it->second.FirstMessageMs, externalPartition.GetFirstMessageMs());
                it->second.LastMessageMs = std::max(it->second.LastMessageMs, externalPartition.GetLastMessageMs());
                it->second.CpuTimeUs = std::max(it->second.CpuTimeUs, externalPartition.GetCpuTimeUs());
                it->second.WaitInputTimeUs = std::max(it->second.WaitInputTimeUs, externalPartition.GetWaitInputTimeUs());
                it->second.WaitOutputTimeUs = std::max(it->second.WaitOutputTimeUs, externalPartition.GetWaitOutputTimeUs());
                it->second.Finished |= externalPartition.GetFinished();
            }
        }

        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*ingress.MutableIngress(), sourcesStat.GetIngress()));
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*ingress.MutablePush(),   sourcesStat.GetPush()));
        BaseTimeMs = NonZeroMin(BaseTimeMs, UpdateAsyncAggr(*ingress.MutablePop(),  sourcesStat.GetPop()));
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
    for (auto& operatorStat : task.GetOperators()) {
        switch (operatorStat.GetTypeCase()) {
            case NYql::NDqProto::TDqOperatorStats::kJoin: {
                auto& joinStats = (*stageStats->MutableOperatorJoin())[operatorStat.GetOperatorId()];
                joinStats.SetOperatorId(operatorStat.GetOperatorId());
                UpdateAggr(joinStats.MutableBytes(), operatorStat.GetBytes());
                UpdateAggr(joinStats.MutableRows(), operatorStat.GetRows());
                break;
            }
            case NYql::NDqProto::TDqOperatorStats::kFilter: {
                auto& filterStats = (*stageStats->MutableOperatorFilter())[operatorStat.GetOperatorId()];
                filterStats.SetOperatorId(operatorStat.GetOperatorId());
                UpdateAggr(filterStats.MutableBytes(), operatorStat.GetBytes());
                UpdateAggr(filterStats.MutableRows(), operatorStat.GetRows());
                break;
            }
            case NYql::NDqProto::TDqOperatorStats::kAggregation: {
                auto& aggrStats = (*stageStats->MutableOperatorAggregation())[operatorStat.GetOperatorId()];
                aggrStats.SetOperatorId(operatorStat.GetOperatorId());
                UpdateAggr(aggrStats.MutableBytes(), operatorStat.GetBytes());
                UpdateAggr(aggrStats.MutableRows(), operatorStat.GetRows());
                break;
            }
            default:
                break;
        }

    }
    for (auto& tableStat : task.GetTables()) {
        auto& tableStats = *GetOrCreateTableAggrStats(stageStats, tableStat.GetTablePath());
        UpdateAggr(tableStats.MutableReadRows(), tableStat.GetReadRows());
        UpdateAggr(tableStats.MutableReadBytes(), tableStat.GetReadBytes());
    }
}

void TQueryExecutionStats::AddComputeActorProfileStatsByTask(
        const NYql::NDqProto::TDqTaskStats& task, const NYql::NDqProto::TDqComputeActorStats& stats,
        bool keepOnlyLastTask) {
    auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);
    if (keepOnlyLastTask) {
        stageStats->MutableComputeActors()->Clear();
    }
    stageStats->AddComputeActors()->CopyFrom(stats);
}

void TQueryExecutionStats::AddComputeActorStats(ui32 /* nodeId */, NYql::NDqProto::TDqComputeActorStats&& stats,
    NYql::NDqProto::EComputeState state, TDuration collectLongTaskStatsTimeout) {
//    Cerr << (TStringBuilder() << "::AddComputeActorStats " << stats.DebugString() << Endl);

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + stats.GetCpuTimeUs());

    TotalTasks += stats.GetTasks().size();

    UpdateAggr(ExtraStats.MutableComputeCpuTimeUs(), stats.GetCpuTimeUs());

    NYql::NDqProto::TDqTaskStats * longTask = nullptr;

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

        // TODO(ilezhankin): investigate - for some reason `task.FinishTimeMs` may be large (or small?)
        //      enough to result in an enormous duration - triggering the "long tasks" mode.

        auto taskDuration = TDuration::MilliSeconds(
            task.GetStartTimeMs() != 0 && task.GetFinishTimeMs() >= task.GetStartTimeMs()
            ? task.GetFinishTimeMs() - task.GetStartTimeMs()
            : 0);
        auto& longestTaskDuration = LongestTaskDurations[task.GetStageId()];
        if (taskDuration > Max(collectLongTaskStatsTimeout, longestTaskDuration)) {
            CollectStatsByLongTasks = true;
            longTask = &task;
            longestTaskDuration = taskDuration;
        }
    }

    if (CollectFullStats(StatsMode)) {
        for (const auto& task : stats.GetTasks()) {
            AddComputeActorFullStatsByTask(task, stats, state);
        }
    }

    if (CollectProfileStats(StatsMode)) {
        for (const auto& task : stats.GetTasks()) {
            AddComputeActorProfileStatsByTask(task, stats, false);
        }
    } else {
        if (longTask) {
            AddComputeActorProfileStatsByTask(*longTask, stats, true);
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

    FillStageDurationUs(*stageStats);
    UpdateAggr(stageStats->MutableWaitInputTimeUs(), task.GetWaitInputTimeUs());
    UpdateAggr(stageStats->MutableWaitOutputTimeUs(), task.GetWaitOutputTimeUs());

    UpdateAggr(stageStats->MutableSpillingComputeBytes(), task.GetSpillingComputeWriteBytes());
    UpdateAggr(stageStats->MutableSpillingChannelBytes(), task.GetSpillingChannelWriteBytes());
    UpdateAggr(stageStats->MutableSpillingComputeTimeUs(), task.GetSpillingComputeReadTimeUs() + task.GetSpillingComputeWriteTimeUs());
    UpdateAggr(stageStats->MutableSpillingChannelTimeUs(), task.GetSpillingChannelReadTimeUs() + task.GetSpillingChannelWriteTimeUs());

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

    NYql::NDqProto::TDqTaskStats* longTask = nullptr;

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

        auto taskDuration = TDuration::MilliSeconds(
            task.GetStartTimeMs() != 0 && task.GetFinishTimeMs() >= task.GetStartTimeMs()
            ? task.GetFinishTimeMs() - task.GetStartTimeMs()
            : 0);
        auto& longestTaskDuration = LongestTaskDurations[task.GetStageId()];
        if (taskDuration > Max(collectLongTaskStatsTimeout, longestTaskDuration)) {
            CollectStatsByLongTasks = true;
            longTask = &task;
            longestTaskDuration = taskDuration;
        }
    }

    if (CollectFullStats(StatsMode)) {
        for (auto& task : stats.GetTasks()) {
            AddDatashardFullStatsByTask(task, datashardCpuTimeUs);
        }
        DatashardStats.emplace_back(std::move(txStats));
    } else {
        if (longTask) {
            DatashardStats.emplace_back(std::move(txStats));
        }
    }

    if (CollectProfileStats(StatsMode)) {
        for (const auto& task : stats.GetTasks()) {
            AddComputeActorProfileStatsByTask(task, stats, false);
        }
    } else {
        if (longTask) {
            AddComputeActorProfileStatsByTask(*longTask, stats, true);
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

void TQueryExecutionStats::AddBufferStats(NYql::NDqProto::TDqTaskStats&& taskStats) {
    for (auto& table : taskStats.GetTables()) {
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
    }
}

void TQueryExecutionStats::UpdateTaskStats(ui64 taskId, const NYql::NDqProto::TDqComputeActorStats& stats, NYql::NDqProto::EComputeState state) {
    AFL_ENSURE(stats.GetTasks().size() == 1);
    const NYql::NDqProto::TDqTaskStats& taskStats = stats.GetTasks(0);
    AFL_ENSURE(taskStats.GetTaskId() == taskId);
    auto stageId = TasksGraph->GetTask(taskId).StageId;
    auto [it, inserted] = StageStats.try_emplace(stageId);
    if (inserted) {
        it->second.StageId = stageId;
        it->second.SetHistorySampleCount(HistorySampleCount);
    }
    BaseTimeMs = NonZeroMin(BaseTimeMs, it->second.UpdateStats(taskStats, state, stats.GetMaxMemoryUsage(), stats.GetDurationUs()));

    constexpr ui64 deadline = 600'000'000; // 10m
    if (it->second.CurrentWaitOutputTimeUs.MinValue > deadline) {
        for (auto stat : it->second.OutputStages) {
            if (stat->IsDeadlocked(deadline)) {
                DeadlockedStageId = stat->StageId.StageId;
                break;
            }
        }
    } else if (it->second.IsDeadlocked(deadline)) {
        DeadlockedStageId = it->second.StageId.StageId;
    }
}

// SIMD-friendly aggregations are below. Compiler is able to vectorize sum/count, but needs help with min/max

ui64 ExportMinStats(std::vector<ui64>& data) {

    AFL_ENSURE((data.size() & 3) == 0);

    ui64 min4[4] = {0, 0, 0, 0};

    for (auto it = data.begin(); it < data.end(); it += 4) {
        min4[0] = min4[0] ? (it[0] ? (min4[0] < it[0] ? min4[0] : it[0]) : min4[0]) : it[0];
        min4[1] = min4[1] ? (it[1] ? (min4[1] < it[1] ? min4[1] : it[1]) : min4[1]) : it[1];
        min4[2] = min4[2] ? (it[2] ? (min4[2] < it[2] ? min4[2] : it[2]) : min4[2]) : it[2];
        min4[3] = min4[3] ? (it[3] ? (min4[3] < it[3] ? min4[3] : it[3]) : min4[3]) : it[3];
    }

    ui64 min01 = min4[0] ? (min4[1] ? (min4[0] < min4[1] ? min4[0] : min4[1]) : min4[0]) : min4[1];
    ui64 min23 = min4[2] ? (min4[3] ? (min4[2] < min4[3] ? min4[2] : min4[3]) : min4[2]) : min4[3];

    return min01 ? (min23 ? (min01 < min23 ? min01 : min23) : min01) : min23;
}

ui64 ExportMaxStats(std::vector<ui64>& data) {

    AFL_ENSURE((data.size() & 3) == 0);

    ui64 max4[4] = {0, 0, 0, 0};

    for (auto it = data.begin(); it < data.end(); it += 4) {
        max4[0] = max4[0] > it[0] ? max4[0] : it[0];
        max4[1] = max4[1] > it[1] ? max4[1] : it[1];
        max4[2] = max4[2] > it[2] ? max4[2] : it[2];
        max4[3] = max4[3] > it[3] ? max4[3] : it[3];
    }

    ui64 max01 = max4[0] > max4[1] ? max4[0] : max4[1];
    ui64 max23 = max4[2] > max4[3] ? max4[2] : max4[3];

    return max01 > max23 ? max01 : max23;
}

void ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsMinMax& stats) {

    AFL_ENSURE((data.size() & 3) == 0);

    ui64 count = 0;
    ui64 min4[4] = {0, 0, 0, 0};
    ui64 max4[4] = {0, 0, 0, 0};

    for (auto it = data.begin(); it < data.end(); it += 4) {
        count += it[0] != 0;
        count += it[1] != 0;
        count += it[2] != 0;
        count += it[3] != 0;
        min4[0] = min4[0] ? (it[0] ? (min4[0] < it[0] ? min4[0] : it[0]) : min4[0]) : it[0];
        min4[1] = min4[1] ? (it[1] ? (min4[1] < it[1] ? min4[1] : it[1]) : min4[1]) : it[1];
        min4[2] = min4[2] ? (it[2] ? (min4[2] < it[2] ? min4[2] : it[2]) : min4[2]) : it[2];
        min4[3] = min4[3] ? (it[3] ? (min4[3] < it[3] ? min4[3] : it[3]) : min4[3]) : it[3];
        max4[0] = max4[0] > it[0] ? max4[0] : it[0];
        max4[1] = max4[1] > it[1] ? max4[1] : it[1];
        max4[2] = max4[2] > it[2] ? max4[2] : it[2];
        max4[3] = max4[3] > it[3] ? max4[3] : it[3];
    }

    if (count) {
        ui64 min01 = min4[0] ? (min4[1] ? (min4[0] < min4[1] ? min4[0] : min4[1]) : min4[0]) : min4[1];
        ui64 min23 = min4[2] ? (min4[3] ? (min4[2] < min4[3] ? min4[2] : min4[3]) : min4[2]) : min4[3];
        stats.SetMin(min01 ? (min23 ? (min01 < min23 ? min01 : min23) : min01) : min23);
        ui64 max01 = max4[0] > max4[1] ? max4[0] : max4[1];
        ui64 max23 = max4[2] > max4[3] ? max4[2] : max4[3];
        stats.SetMax(max01 > max23 ? max01 : max23);
    }
}

void ExportOffsetAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats, ui64 offset) {

    AFL_ENSURE((data.size() & 3) == 0);

    ui64 count = 0;
    ui64 sum = 0;
    ui64 min4[4] = {0, 0, 0, 0};
    ui64 max4[4] = {0, 0, 0, 0};

    for (auto ito = data.begin(); ito < data.end(); ito += 4) {
        ui64 it[4];
        it[0] = ito[0] <= offset ? 0 : ito[0] - offset;
        it[1] = ito[1] <= offset ? 0 : ito[1] - offset;
        it[2] = ito[2] <= offset ? 0 : ito[2] - offset;
        it[3] = ito[3] <= offset ? 0 : ito[3] - offset;
        count += it[0] != 0;
        count += it[1] != 0;
        count += it[2] != 0;
        count += it[3] != 0;
        sum += it[0];
        sum += it[1];
        sum += it[2];
        sum += it[3];
        min4[0] = min4[0] ? (it[0] ? (min4[0] < it[0] ? min4[0] : it[0]) : min4[0]) : it[0];
        min4[1] = min4[1] ? (it[1] ? (min4[1] < it[1] ? min4[1] : it[1]) : min4[1]) : it[1];
        min4[2] = min4[2] ? (it[2] ? (min4[2] < it[2] ? min4[2] : it[2]) : min4[2]) : it[2];
        min4[3] = min4[3] ? (it[3] ? (min4[3] < it[3] ? min4[3] : it[3]) : min4[3]) : it[3];
        max4[0] = max4[0] > it[0] ? max4[0] : it[0];
        max4[1] = max4[1] > it[1] ? max4[1] : it[1];
        max4[2] = max4[2] > it[2] ? max4[2] : it[2];
        max4[3] = max4[3] > it[3] ? max4[3] : it[3];
    }

    if (count) {
        stats.SetCnt(count);
        stats.SetSum(sum);
        ui64 min01 = min4[0] ? (min4[1] ? (min4[0] < min4[1] ? min4[0] : min4[1]) : min4[0]) : min4[1];
        ui64 min23 = min4[2] ? (min4[3] ? (min4[2] < min4[3] ? min4[2] : min4[3]) : min4[2]) : min4[3];
        stats.SetMin(min01 ? (min23 ? (min01 < min23 ? min01 : min23) : min01) : min23);
        ui64 max01 = max4[0] > max4[1] ? max4[0] : max4[1];
        ui64 max23 = max4[2] > max4[3] ? max4[2] : max4[3];
        stats.SetMax(max01 > max23 ? max01 : max23);
    }
}

void ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats) {

    AFL_ENSURE((data.size() & 3) == 0);

    ui64 count = 0;
    ui64 sum = 0;
    ui64 min4[4] = {0, 0, 0, 0};
    ui64 max4[4] = {0, 0, 0, 0};

    for (auto it = data.begin(); it < data.end(); it += 4) {
        count += it[0] != 0;
        count += it[1] != 0;
        count += it[2] != 0;
        count += it[3] != 0;
        sum += it[0];
        sum += it[1];
        sum += it[2];
        sum += it[3];
        min4[0] = min4[0] ? (it[0] ? (min4[0] < it[0] ? min4[0] : it[0]) : min4[0]) : it[0];
        min4[1] = min4[1] ? (it[1] ? (min4[1] < it[1] ? min4[1] : it[1]) : min4[1]) : it[1];
        min4[2] = min4[2] ? (it[2] ? (min4[2] < it[2] ? min4[2] : it[2]) : min4[2]) : it[2];
        min4[3] = min4[3] ? (it[3] ? (min4[3] < it[3] ? min4[3] : it[3]) : min4[3]) : it[3];
        max4[0] = max4[0] > it[0] ? max4[0] : it[0];
        max4[1] = max4[1] > it[1] ? max4[1] : it[1];
        max4[2] = max4[2] > it[2] ? max4[2] : it[2];
        max4[3] = max4[3] > it[3] ? max4[3] : it[3];
    }

    if (count) {
        stats.SetCnt(count);
        stats.SetSum(sum);
        ui64 min01 = min4[0] ? (min4[1] ? (min4[0] < min4[1] ? min4[0] : min4[1]) : min4[0]) : min4[1];
        ui64 min23 = min4[2] ? (min4[3] ? (min4[2] < min4[3] ? min4[2] : min4[3]) : min4[2]) : min4[3];
        stats.SetMin(min01 ? (min23 ? (min01 < min23 ? min01 : min23) : min01) : min23);
        ui64 max01 = max4[0] > max4[1] ? max4[0] : max4[1];
        ui64 max23 = max4[2] > max4[3] ? max4[2] : max4[3];
        stats.SetMax(max01 > max23 ? max01 : max23);
    }
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
    auto& external = *stats.MutableExternal();
    data.External.ExternalRows.ExportAggStats(*external.MutableExternalRows());
    data.External.ExternalBytes.ExportAggStats(BaseTimeMs, *external.MutableExternalBytes());
    ExportOffsetAggStats(data.External.FirstMessageMs.Values, *external.MutableFirstMessageMs(), BaseTimeMs);
    ExportOffsetAggStats(data.External.LastMessageMs.Values, *external.MutableLastMessageMs(), BaseTimeMs);
    data.External.CpuTimeUs.ExportAggStats(BaseTimeMs, *external.MutableCpuTimeUs());
    data.External.WaitInputTimeUs.ExportAggStats(BaseTimeMs, *external.MutableWaitInputTimeUs());
    data.External.WaitOutputTimeUs.ExportAggStats(BaseTimeMs, *external.MutableWaitOutputTimeUs());
    external.SetPartitionCount(std::max<ui32>(data.External.PartitionCount, data.External.Indices.size()));
    external.SetFinishedPartitionCount(data.External.Finished.Sum);
    ExportAggAsyncStats(data.Ingress, *stats.MutableIngress());
    ExportAggAsyncStats(data.Push, *stats.MutablePush());
    ExportAggAsyncStats(data.Pop, *stats.MutablePop());
    ExportAggAsyncStats(data.Egress, *stats.MutableEgress());
}

void TQueryExecutionStats::ExportExecStats(NYql::NDqProto::TDqExecutionStats& stats) {

    THashMap<ui32, NDqProto::TDqStageStats*> protoStages;

    if (CollectFullStats(StatsMode)) {
        for (auto& [stageId, stagetype] : TasksGraph->GetStagesInfo()) {
            protoStages.emplace(stageId.StageId, GetOrCreateStageStats(stageId, *TasksGraph, stats));
        }
    }

    std::unordered_map<TString, NYql::NDqProto::TDqTableStats*> currentTableStats;

    for (auto& [stageId, stageStat] : StageStats) {
        for (auto& [path, t] : stageStat.Tables) {
            NYql::NDqProto::TDqTableStats* tableAggr = nullptr;
            if (auto it = currentTableStats.find(path); it != currentTableStats.end()) {
                tableAggr = it->second;
            } else {
                tableAggr = stats.AddTables();
                tableAggr->SetTablePath(path);
                currentTableStats.emplace(path, tableAggr);
            }

            tableAggr->SetReadRows(tableAggr->GetReadRows() + ExportAggStats(t.ReadRows));
            tableAggr->SetReadBytes(tableAggr->GetReadBytes() + ExportAggStats(t.ReadBytes));
            tableAggr->SetWriteRows(tableAggr->GetWriteRows() + ExportAggStats(t.WriteRows));
            tableAggr->SetWriteBytes(tableAggr->GetWriteBytes() + ExportAggStats(t.WriteBytes));
            tableAggr->SetEraseRows(tableAggr->GetEraseRows() + ExportAggStats(t.EraseRows));
            tableAggr->SetAffectedPartitions(tableAggr->GetAffectedPartitions() + ExportAggStats(t.AffectedPartitions));

        }


        if (CollectFullStats(StatsMode)) {
            auto& stageStats = *protoStages[stageStat.StageId.StageId];
            stageStats.SetTotalTasksCount(stageStat.Task2Index.size());
            stageStats.SetFinishedTasksCount(stageStat.FinishedCount);

            stageStats.SetBaseTimeMs(BaseTimeMs);
            stageStat.CpuTimeUs.ExportAggStats(BaseTimeMs, *stageStats.MutableCpuTimeUs());
            ExportAggStats(stageStat.SourceCpuTimeUs, *stageStats.MutableSourceCpuTimeUs());
            stageStat.MaxMemoryUsage.ExportAggStats(BaseTimeMs, *stageStats.MutableMaxMemoryUsage());

            ExportAggStats(stageStat.InputRows, *stageStats.MutableInputRows());
            ExportAggStats(stageStat.InputBytes, *stageStats.MutableInputBytes());
            ExportAggStats(stageStat.OutputRows, *stageStats.MutableOutputRows());
            ExportAggStats(stageStat.OutputBytes, *stageStats.MutableOutputBytes());
            ExportAggStats(stageStat.ResultRows, *stageStats.MutableResultRows());
            ExportAggStats(stageStat.ResultBytes, *stageStats.MutableResultBytes());
            ExportAggStats(stageStat.IngressRows, *stageStats.MutableIngressRows());
            ExportAggStats(stageStat.IngressBytes, *stageStats.MutableIngressBytes());
            ExportAggStats(stageStat.IngressDecompressedBytes, *stageStats.MutableIngressDecompressedBytes());
            ExportAggStats(stageStat.EgressRows, *stageStats.MutableEgressRows());
            ExportAggStats(stageStat.EgressBytes, *stageStats.MutableEgressBytes());

            ExportOffsetAggStats(stageStat.StartTimeMs, *stageStats.MutableStartTimeMs(), BaseTimeMs);
            ExportOffsetAggStats(stageStat.FinishTimeMs, *stageStats.MutableFinishTimeMs(), BaseTimeMs);
            ExportAggStats(stageStat.DurationUs, *stageStats.MutableDurationUs());
            stageStat.WaitInputTimeUs.ExportAggStats(BaseTimeMs, *stageStats.MutableWaitInputTimeUs());
            stageStat.WaitOutputTimeUs.ExportAggStats(BaseTimeMs, *stageStats.MutableWaitOutputTimeUs());
            stageStats.SetUpdateTimeMs(stageStat.UpdateTimeMs > BaseTimeMs ? stageStat.UpdateTimeMs - BaseTimeMs : 0);

            stageStat.SpillingComputeBytes.ExportAggStats(BaseTimeMs, *stageStats.MutableSpillingComputeBytes());
            stageStat.SpillingChannelBytes.ExportAggStats(BaseTimeMs, *stageStats.MutableSpillingChannelBytes());
            stageStat.SpillingComputeTimeUs.ExportAggStats(BaseTimeMs, *stageStats.MutableSpillingComputeTimeUs());
            stageStat.SpillingChannelTimeUs.ExportAggStats(BaseTimeMs, *stageStats.MutableSpillingChannelTimeUs());

            FillStageDurationUs(stageStats);

            for (auto& [path, t] : stageStat.Tables) {
                auto& table = *stageStats.AddTables();
                table.SetTablePath(path);
                ExportAggStats(t.ReadRows, *table.MutableReadRows());
                ExportAggStats(t.ReadBytes, *table.MutableReadBytes());
                ExportAggStats(t.WriteRows, *table.MutableWriteRows());
                ExportAggStats(t.WriteBytes, *table.MutableWriteBytes());
                ExportAggStats(t.EraseRows, *table.MutableEraseRows());
                ExportAggStats(t.EraseBytes, *table.MutableEraseBytes());
                table.SetAffectedPartitions(ExportAggStats(t.AffectedPartitions));
            }
            for (auto& [id, i] : stageStat.Ingress) {
                ExportAggAsyncBufferStats(i, (*stageStats.MutableIngress())[id]);
            }
            for (auto& [id, i] : stageStat.Input) {
                ExportAggAsyncBufferStats(i, (*stageStats.MutableInput())[id]);
            }
            for (auto& [id, o] : stageStat.Output) {
                ExportAggAsyncBufferStats(o, (*stageStats.MutableOutput())[id]);
            }
            for (auto& [id, e] : stageStat.Egress) {
                ExportAggAsyncBufferStats(e, (*stageStats.MutableEgress())[id]);
            }
            for (auto& [id, j] : stageStat.Joins) {
                auto& joinStat = (*stageStats.MutableOperatorJoin())[id];
                joinStat.SetOperatorId(id);
                ExportAggStats(j.Bytes, *joinStat.MutableBytes());
                ExportAggStats(j.Rows, *joinStat.MutableRows());
            }
            for (auto& [id, f] : stageStat.Filters) {
                auto& filterStat = (*stageStats.MutableOperatorFilter())[id];
                filterStat.SetOperatorId(id);
                ExportAggStats(f.Bytes, *filterStat.MutableBytes());
                ExportAggStats(f.Rows, *filterStat.MutableRows());
            }
            for (auto& [id, a] : stageStat.Aggregations) {
                auto& aggrStat = (*stageStats.MutableOperatorAggregation())[id];
                aggrStat.SetOperatorId(id);
                ExportAggStats(a.Bytes, *aggrStat.MutableBytes());
                ExportAggStats(a.Rows, *aggrStat.MutableRows());
            }
        }
    }

    stats.SetDurationUs(TInstant::Now().MicroSeconds() - StartTs.MicroSeconds());
}

void TQueryExecutionStats::AdjustExternalAggr(NYql::NDqProto::TDqExternalAggrStats& stats) {
    if (stats.HasFirstMessageMs()) {
        AdjustDqStatsAggr(*stats.MutableFirstMessageMs());
    }
    if (stats.HasLastMessageMs()) {
        AdjustDqStatsAggr(*stats.MutableLastMessageMs());
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
    if (stats.HasExternal()) {
        AdjustExternalAggr(*stats.MutableExternal());
    }
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
        stats.SetMin(min > BaseTimeMs ? min - BaseTimeMs : 0);
    }
    if (auto max = stats.GetMax()) {
        stats.SetMax(max > BaseTimeMs ? max - BaseTimeMs : 0);
    }
    if (auto cnt = stats.GetCnt()) {
        auto sum = stats.GetSum();
        auto baseSum = BaseTimeMs * cnt;
        stats.SetSum(sum > baseSum ? sum - baseSum : 0);
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
    auto updateTimeMs = stageStats->GetUpdateTimeMs();
    stageStats->SetUpdateTimeMs(updateTimeMs > BaseTimeMs ? updateTimeMs - BaseTimeMs : 0);
}

void TQueryExecutionStats::Finish() {
//    Cerr << (TStringBuilder() << "-- finish: executerTime: " << ExecuterCpuTime.MicroSeconds() << Endl);
    THashMap<ui32, NDqProto::TDqStageStats*> protoStages;

    for (auto& [stageId, stagetype] : TasksGraph->GetStagesInfo()) {
        auto stageStats = GetOrCreateStageStats(stageId, *TasksGraph, *Result);
        stageStats->SetBaseTimeMs(BaseTimeMs);

        if (ExternalPartitionStats.contains(stageStats->GetStageId())) {
            auto& externalPartitionStat = ExternalPartitionStats[stageStats->GetStageId()];
            auto& ingress = (*stageStats->MutableIngress())[externalPartitionStat.Name];
            auto& external = *ingress.MutableExternal();
            ui32 finishedPartitions = 0;
            for (auto& [partitionId, partitionStat] : externalPartitionStat.Stat) {
                if (partitionStat.ExternalRows) {
                    auto& externalRows = *external.MutableExternalRows();
                    externalRows.SetMin(NonZeroMin(externalRows.GetMin(), partitionStat.ExternalRows));
                    externalRows.SetMax(std::max(externalRows.GetMax(), partitionStat.ExternalRows));
                    externalRows.SetSum(externalRows.GetSum() + partitionStat.ExternalRows);
                    externalRows.SetCnt(externalRows.GetCnt() + 1);
                }

                if (partitionStat.ExternalBytes) {
                    auto& externalBytes = *external.MutableExternalBytes();
                    externalBytes.SetMin(NonZeroMin(externalBytes.GetMin(), partitionStat.ExternalBytes));
                    externalBytes.SetMax(std::max(externalBytes.GetMax(), partitionStat.ExternalBytes));
                    externalBytes.SetSum(externalBytes.GetSum() + partitionStat.ExternalBytes);
                    externalBytes.SetCnt(externalBytes.GetCnt() + 1);
                }

                if (partitionStat.FirstMessageMs) {
                    auto& firstMessageMs = *external.MutableFirstMessageMs();
                    firstMessageMs.SetMin(NonZeroMin(firstMessageMs.GetMin(), partitionStat.FirstMessageMs));
                    firstMessageMs.SetMax(std::max(firstMessageMs.GetMax(), partitionStat.FirstMessageMs));
                    firstMessageMs.SetSum(firstMessageMs.GetSum() + partitionStat.FirstMessageMs);
                    firstMessageMs.SetCnt(firstMessageMs.GetCnt() + 1);
                }

                if (partitionStat.LastMessageMs) {
                    auto& lastMessageMs = *external.MutableLastMessageMs();
                    lastMessageMs.SetMin(NonZeroMin(lastMessageMs.GetMin(), partitionStat.LastMessageMs));
                    lastMessageMs.SetMax(std::max(lastMessageMs.GetMax(), partitionStat.LastMessageMs));
                    lastMessageMs.SetSum(lastMessageMs.GetSum() + partitionStat.LastMessageMs);
                    lastMessageMs.SetCnt(lastMessageMs.GetCnt() + 1);
                }

                if (partitionStat.CpuTimeUs) {
                    auto& cpuTimeUs = *external.MutableCpuTimeUs();
                    cpuTimeUs.SetMin(NonZeroMin(cpuTimeUs.GetMin(), partitionStat.CpuTimeUs));
                    cpuTimeUs.SetMax(std::max(cpuTimeUs.GetMax(), partitionStat.CpuTimeUs));
                    cpuTimeUs.SetSum(cpuTimeUs.GetSum() + partitionStat.CpuTimeUs);
                    cpuTimeUs.SetCnt(cpuTimeUs.GetCnt() + 1);
                }

                if (partitionStat.WaitInputTimeUs) {
                    auto& waitInputTimeUs = *external.MutableWaitInputTimeUs();
                    waitInputTimeUs.SetMin(NonZeroMin(waitInputTimeUs.GetMin(), partitionStat.WaitInputTimeUs));
                    waitInputTimeUs.SetMax(std::max(waitInputTimeUs.GetMax(), partitionStat.WaitInputTimeUs));
                    waitInputTimeUs.SetSum(waitInputTimeUs.GetSum() + partitionStat.WaitInputTimeUs);
                    waitInputTimeUs.SetCnt(waitInputTimeUs.GetCnt() + 1);
                }

                if (partitionStat.WaitOutputTimeUs) {
                    auto& waitOutputTimeUs = *external.MutableWaitOutputTimeUs();
                    waitOutputTimeUs.SetMin(NonZeroMin(waitOutputTimeUs.GetMin(), partitionStat.WaitOutputTimeUs));
                    waitOutputTimeUs.SetMax(std::max(waitOutputTimeUs.GetMax(), partitionStat.WaitOutputTimeUs));
                    waitOutputTimeUs.SetSum(waitOutputTimeUs.GetSum() + partitionStat.WaitOutputTimeUs);
                    waitOutputTimeUs.SetCnt(waitOutputTimeUs.GetCnt() + 1);
                }

                finishedPartitions += partitionStat.Finished;
            }
            external.SetPartitionCount(external.GetPartitionCount() + externalPartitionStat.Stat.size());
            external.SetFinishedPartitionCount(external.GetFinishedPartitionCount() + finishedPartitions);

            auto it = StageStats.find(stageId);
            if (it != StageStats.end()) {
                auto it1 = it->second.Ingress.find(externalPartitionStat.Name);
                if (it1 != it->second.Ingress.end()) {
                    external.SetPartitionCount(std::max<ui32>(it1->second.External.PartitionCount, external.GetPartitionCount()));
                }
            }
        }

        AdjustBaseTime(stageStats);

        auto it = StageStats.find(stageId);
        if (it != StageStats.end()) {
            it->second.ExportHistory(BaseTimeMs, *stageStats);
        }
    }

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + ExecuterCpuTime.MicroSeconds());
    Result->SetDurationUs(FinishTs.MicroSeconds() - StartTs.MicroSeconds());

    // Result->Result* fields are (temporary?) commented out in proto due to lack of use
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
