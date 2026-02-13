#include "kqp_executer_stats.h"

#include <ydb/core/protos/kqp_stats.pb.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;

ui64 ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats);

ui64 NonZeroMin(ui64 a, ui64 b) {
    return (b == 0) ? a : ((a == 0 || a > b) ? b : a);
}

ui64 ExportMinStats(std::vector<ui64>& data);
ui64 ExportMaxStats(std::vector<ui64>& data);

void TMinStats::Resize(ui32 count) {
    Values.resize(count);
}

void TMinStats::SetNonZero(ui32 index, ui64 value) {
    if (value) {
        AFL_ENSURE(index < Values.size());
        auto maybeMin = Values[index] == MinValue;
        Values[index] = value;
        if (maybeMin) {
            MinValue = ExportMinStats(Values);
        }
    }
}

void TMaxStats::Resize(ui32 count) {
    Values.resize(count);
}

void TMaxStats::SetNonZero(ui32 index, ui64 value) {
    if (value) {
        AFL_ENSURE(index < Values.size());
        auto isMonotonic = value >= Values[index];
        Values[index] = value;
        MaxValue = isMonotonic ? (value > MaxValue ? value : MaxValue) : ExportMaxStats(Values);
    }
}

void TSumStats::Resize(ui32 count) {
    Values.resize(count);
}

void TSumStats::SetNonZero(ui32 index, ui64 value) {
    if (value) {
        AFL_ENSURE(index < Values.size());
        Sum += value;
        Sum -= Values[index];
        Values[index] = value;
    }
}

ui64 TSumStats::ExportAggStats(NYql::NDqProto::TDqStatsAggr& stats) {
    return NKikimr::NKqp::ExportAggStats(Values, stats);
}

ui64 TTimeSeriesStats::ExportAggStats(ui64 baseTimeMs, NYql::NDqProto::TDqStatsAggr& stats) {
    auto result = ExportAggStats(stats);
    ExportHistory(baseTimeMs, stats);
    return result;
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

void TTimeSeriesStats::SetNonZero(ui32 index, ui64 value) {
    if (value) {
        TSumStats::SetNonZero(index, value);
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
    LocalBytes.resize(taskCount);
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

    for (auto& [_, m] : Mkql) m.resize(taskCount);

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
    CurrentWaitInputTimeUs.SetNonZero(index, taskStats.GetCurrentWaitInputTimeUs());
    CurrentWaitOutputTimeUs.SetNonZero(index, taskStats.GetCurrentWaitOutputTimeUs());

    auto updateTimeMs = taskStats.GetUpdateTimeMs();
    UpdateTimeMs = std::max(UpdateTimeMs, updateTimeMs);
    baseTimeMs = NonZeroMin(baseTimeMs, updateTimeMs);

    SpillingComputeBytes.SetNonZero(index, taskStats.GetSpillingComputeWriteBytes());
    SpillingChannelBytes.SetNonZero(index, taskStats.GetSpillingChannelWriteBytes());
    SpillingComputeTimeUs.SetNonZero(index, taskStats.GetSpillingComputeReadTimeUs() + taskStats.GetSpillingComputeWriteTimeUs());
    SpillingChannelTimeUs.SetNonZero(index, taskStats.GetSpillingChannelReadTimeUs() + taskStats.GetSpillingChannelWriteTimeUs());

    for (auto& tableStat : taskStats.GetTables()) {
        auto tablePath = tableStat.GetTablePath();
        auto [it, _] = Tables.try_emplace(tablePath, TaskCount);
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
        SetNonZero(asyncBufferStats.LocalBytes, index, inputChannelStat.GetLocalBytes());
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
        SetNonZero(asyncBufferStats.LocalBytes, index, outputChannelStat.GetLocalBytes());
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

    for (const auto& mkqlStat : taskStats.GetMkqlStats()) {
        if (const auto& name = mkqlStat.GetName()) {
            std::vector<ui64>* stats = nullptr;
            const auto value = mkqlStat.GetValue();
            if (value) {
                stats = &Mkql.emplace(name, TaskCount).first->second;
            } else if (auto it = Mkql.find(name); it != Mkql.end()) {
                stats = &it->second;
            } else {
                continue;
            }

            AFL_ENSURE(stats);
            AFL_ENSURE(index < stats->size());
            (*stats)[index] = value;
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

void TQueryTableStats::Resize(ui32 taskCount) {
    ReadRows.Resize(taskCount);
    ReadBytes.Resize(taskCount);
    WriteRows.Resize(taskCount);
    WriteBytes.Resize(taskCount);
    EraseRows.Resize(taskCount);
    EraseBytes.Resize(taskCount);
    AffectedPartitions.Resize(taskCount);
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

    // TODO: respect stats level: full, profile
    auto intros = tasksGraph.GetStageIntrospection(stageId);
    newStage->MutableIntrospections()->Add(intros.begin(), intros.end());

    return newStage;
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

bool CollectBasicStats(Ydb::Table::QueryStatsCollection::Mode statsMode) {
    return statsMode >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC;
}

bool CollectFullStats(Ydb::Table::QueryStatsCollection::Mode statsMode) {
    return statsMode >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL;
}

bool CollectProfileStats(Ydb::Table::QueryStatsCollection::Mode statsMode) {
    return statsMode >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE;
}

void TQueryExecutionStats::Prepare() {
    TaskCount = TasksGraph->GetTasks().size();
    TaskCount4 = (TaskCount + 3) & ~3;
    switch (StatsMode) {
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE:
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
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
            [[fallthrough]];
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE:
        default:
            ComputeCpuTimeUs.Resize(TaskCount4);
            break;
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

void TQueryExecutionStats::CollectLockStats(const NKikimrQueryStats::TTxStats& txStats) {
    LocksBrokenAsBreaker += txStats.GetLocksBrokenAsBreaker();
    LocksBrokenAsVictim += txStats.GetLocksBrokenAsVictim();
    if (txStats.GetLocksBrokenAsBreaker() > 0) {
        for (ui64 id : txStats.GetBreakerQuerySpanIds()) {
            BreakerQuerySpanIds.push_back(id);
        }
    }
}

void TQueryExecutionStats::AddDatashardPrepareStats(NKikimrQueryStats::TTxStats&& txStats) {
    CollectLockStats(txStats);

    ui64 cpuUs = txStats.GetComputeCpuTimeUsec();
    for (const auto& perShard : txStats.GetPerShardStats()) {
        AffectedShards.emplace(perShard.GetShardId());
        cpuUs += perShard.GetCpuTimeUsec();
    }

    StorageCpuTimeUs += cpuUs;
}

void TQueryExecutionStats::AddDatashardStats(NYql::NDqProto::TDqComputeActorStats&& stats,
    NKikimrQueryStats::TTxStats&& txStats, TDuration collectLongTaskStatsTimeout)
{
    CollectLockStats(txStats);

    UpdateTaskStats(0, stats, &txStats, NYql::NDqProto::COMPUTE_STATE_FINISHED, collectLongTaskStatsTimeout);
}

void TQueryExecutionStats::AddDatashardStats(NKikimrQueryStats::TTxStats&& txStats) {
    CollectLockStats(txStats);

    ui64 datashardCpuTimeUs = 0;
    for (const auto& perShard : txStats.GetPerShardStats()) {
        AffectedShards.emplace(perShard.GetShardId());

        datashardCpuTimeUs += perShard.GetCpuTimeUsec();
        UpdateAggr(ExtraStats.MutableShardsCpuTimeUs(), perShard.GetCpuTimeUsec());
    }

    StorageCpuTimeUs += datashardCpuTimeUs;
}

void TQueryExecutionStats::AddBufferStats(NYql::NDqProto::TDqTaskStats&& taskStats) {
    NKqpProto::TKqpTaskExtraStats extraStats;
    if (taskStats.GetExtra().UnpackTo(&extraStats)) {
        LocksBrokenAsBreaker += extraStats.GetLockStats().GetBrokenAsBreaker();
        LocksBrokenAsVictim += extraStats.GetLockStats().GetBrokenAsVictim();
        for (auto id : extraStats.GetLockStats().GetBreakerQuerySpanIds()) {
            if (id != 0) {
                BreakerQuerySpanIds.push_back(id);
            }
        }
    }
    UpdateStorageTables(taskStats, nullptr);
}

void TQueryExecutionStats::UpdateQueryTables(const NYql::NDqProto::TDqTaskStats& taskStats, NKikimrQueryStats::TTxStats* txStats) {
    auto index = taskStats.GetTaskId() - 1;
    AFL_ENSURE(index < TaskCount);
    for (auto& tableStat : taskStats.GetTables()) {
        auto tablePath = tableStat.GetTablePath();
        auto [it, _] = Tables.try_emplace(tablePath, TaskCount4);
        auto& queryTableStats = it->second;
        queryTableStats.ReadRows.SetNonZero(index, tableStat.GetReadRows());
        queryTableStats.ReadBytes.SetNonZero(index, tableStat.GetReadBytes());
        queryTableStats.WriteRows.SetNonZero(index, tableStat.GetWriteRows());
        queryTableStats.WriteBytes.SetNonZero(index, tableStat.GetWriteBytes());
        queryTableStats.EraseRows.SetNonZero(index, tableStat.GetEraseRows());
        queryTableStats.EraseBytes.SetNonZero(index, tableStat.GetEraseBytes());

        if (txStats) {
            auto& tableShards = TableShards[tablePath];
            for (const auto& perShard : txStats->GetPerShardStats()) {
                tableShards.insert(perShard.GetShardId());
                AffectedShards.insert(perShard.GetShardId());
            }
            queryTableStats.AffectedPartitionsUniqueCount = tableShards.size();
            queryTableStats.AffectedPartitions.SetNonZero(index, txStats->GetPerShardStats().size());
        } else {
            queryTableStats.AffectedPartitions.SetNonZero(index, tableStat.GetAffectedPartitions());
        }

        NKqpProto::TKqpShardTableExtraStats shardExtraStats;
        if (tableStat.GetExtra().UnpackTo(&shardExtraStats)) {
            AffectedShards.insert(shardExtraStats.GetShardId());
        }

        NKqpProto::TKqpTableExtraStats tableExtraStats;
        if (tableStat.GetExtra().UnpackTo(&tableExtraStats)) {
            for (const auto& shardId : tableExtraStats.GetReadActorTableAggrExtraStats().GetAffectedShards()) {
                AffectedShards.insert(shardId);
            }
        }

        // TODO: the following code is for backward compatibility, remove it after ydb release
        {
            NKqpProto::TKqpReadActorTableAggrExtraStats tableExtraStats;
            if (tableStat.GetExtra().UnpackTo(&tableExtraStats)) {
                for (const auto& shardId : tableExtraStats.GetAffectedShards()) {
                    AffectedShards.insert(shardId);
                }
            }
        }
    }
}

void TQueryExecutionStats::UpdateStorageTables(const NYql::NDqProto::TDqTaskStats& taskStats, NKikimrQueryStats::TTxStats* txStats) {
    for (auto& tableStat : taskStats.GetTables()) {
        auto tablePath = tableStat.GetTablePath();
        auto [it, _] = Tables.try_emplace(tablePath, TaskCount4);
        auto& queryTableStats = it->second;
        queryTableStats.StorageStats.ReadRows += tableStat.GetReadRows();
        queryTableStats.StorageStats.ReadBytes += tableStat.GetReadBytes();
        queryTableStats.StorageStats.WriteRows += tableStat.GetWriteRows();
        queryTableStats.StorageStats.WriteBytes += tableStat.GetWriteBytes();
        queryTableStats.StorageStats.EraseRows += tableStat.GetEraseRows();
        queryTableStats.StorageStats.EraseBytes += tableStat.GetEraseBytes();
        if (txStats) {
            auto& tableShards = TableShards[tablePath];
            for (const auto& perShard : txStats->GetPerShardStats()) {
                tableShards.insert(perShard.GetShardId());
            }
            queryTableStats.StorageStats.AffectedPartitions = tableShards.size();
        } else {
            queryTableStats.StorageStats.AffectedPartitions += tableStat.GetAffectedPartitions();
        }
    }
}

void TQueryExecutionStats::UpdateTaskStats(ui64 taskId, const NYql::NDqProto::TDqComputeActorStats& stats, NKikimrQueryStats::TTxStats* txStats,
    NYql::NDqProto::EComputeState state, TDuration collectLongTaskStatsTimeout) {

    if (taskId) {
        AFL_ENSURE(stats.GetTasks().size() == 1);
        AFL_ENSURE(stats.GetTasks(0).GetTaskId() == taskId);
    }

    for (auto& taskStats : stats.GetTasks()) {
        auto taskId = taskStats.GetTaskId();
        // AFL_ENSURE(taskId > 0);
        auto index = taskId ? taskId - 1 : 0;
        AFL_ENSURE(index < TaskCount);

        ComputeCpuTimeUs.SetNonZero(index, taskStats.GetCpuTimeUs());
        if (taskId == 0) {
            UpdateStorageTables(taskStats, txStats);
        } else {
            UpdateQueryTables(taskStats, txStats);
        }

        // Extract lock stats from task extra stats (populated by read actors for broken locks)
        if (taskStats.HasExtra()) {
            NKqpProto::TKqpTaskExtraStats extraStats;
            if (taskStats.GetExtra().UnpackTo(&extraStats)) {
                LocksBrokenAsBreaker += extraStats.GetLockStats().GetBrokenAsBreaker();
                LocksBrokenAsVictim += extraStats.GetLockStats().GetBrokenAsVictim();
                for (auto id : extraStats.GetLockStats().GetBreakerQuerySpanIds()) {
                    if (id != 0) {
                        BreakerQuerySpanIds.push_back(id);
                    }
                }
            }
        }

        if (CollectBasicStats(StatsMode)) {
            if (CollectFullStats(StatsMode)) {
                auto stageId = TasksGraph->GetTask(taskId).StageId;
                auto [it, inserted] = StageStats.try_emplace(stageId);
                TStageExecutionStats& stageStats = it->second;
                if (inserted) {
                    stageStats.StageId = stageId;
                    stageStats.SetHistorySampleCount(HistorySampleCount);
                }
                if (taskId == 0 && stageStats.TaskCount == 0) {
                    stageStats.Task2Index.emplace(0, 0);
                    stageStats.TaskCount = 4;
                    stageStats.Resize(4);
                }
                BaseTimeMs = NonZeroMin(BaseTimeMs, stageStats.UpdateStats(taskStats, state, stats.GetMaxMemoryUsage(), stats.GetDurationUs()));

                constexpr ui64 deadline = 600'000'000; // 10m
                if (stageStats.CurrentWaitOutputTimeUs.MinValue > deadline) {
                    for (auto stat : stageStats.OutputStages) {
                        if (stat->IsDeadlocked(deadline)) {
                            DeadlockedStageId = stat->StageId.StageId;
                            break;
                        }
                    }
                } else if (stageStats.IsDeadlocked(deadline)) {
                    DeadlockedStageId = stageStats.StageId.StageId;
                }

                if (CollectProfileStats(StatsMode)) {
                    stageStats.ComputeActors[taskId].CopyFrom(stats);
                } else {
                    auto taskDuration = TDuration::MilliSeconds(
                        taskStats.GetStartTimeMs() != 0 && taskStats.GetFinishTimeMs() >= taskStats.GetStartTimeMs()
                        ? taskStats.GetFinishTimeMs() - taskStats.GetStartTimeMs()
                        : 0);
                    auto& longestTaskDuration = LongestTaskDurations[taskStats.GetStageId()];
                    if (taskDuration > Max(collectLongTaskStatsTimeout, longestTaskDuration)) {
                        CollectStatsByLongTasks = true;
                        longestTaskDuration = taskDuration;
                        stageStats.ComputeActors.clear();
                        stageStats.ComputeActors[taskId].CopyFrom(stats);
                    }
                }
            }
        }
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

ui64 ExportAggStats(std::vector<ui64>& data, NYql::NDqProto::TDqStatsAggr& stats) {

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

    return sum;
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
    stats.SetLocalBytes(ExportAggStats(data.LocalBytes));
}

void TQueryExecutionStats::ExportExecStats(NYql::NDqProto::TDqExecutionStats& stats) {
    switch (StatsMode) {
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE:
            [[fallthrough]];
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL: {
            THashMap<ui32, NDqProto::TDqStageStats*> protoStages;

            for (auto& [stageId, stagetype] : TasksGraph->GetStagesInfo()) {
                if (stageId.TxId == 0) {
                    protoStages.emplace(stageId.StageId, GetOrCreateStageStats(stageId, *TasksGraph, stats));
                }
            }

            for (auto& [stageId, stageStat] : StageStats) {
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
                for (auto& [id, m] : stageStat.Mkql) {
                    ExportAggStats(m, (*stageStats.MutableMkql())[id]);
                }
                for (auto& [id, caStats] : stageStat.ComputeActors) {
                    stageStats.AddComputeActors()->Swap(&caStats);
                }

                if (CollectProfileStats(StatsMode)) {
                    auto it = ShardsCountByNode.find(stageId.StageId);
                    if (it != ShardsCountByNode.end()) {
                        NKqpProto::TKqpStageExtraStats extraStats;
                        for (auto&& n : it->second) {
                            auto& nodeStat = *extraStats.AddNodeStats();
                            nodeStat.SetNodeId(n.first);
                            nodeStat.SetShardsCount(n.second);
                        }
                        stageStats.MutableExtra()->PackFrom(extraStats);
                    }
                }
            }

            stats.SetExecuterCpuTimeUs(ExecuterCpuTime.MicroSeconds());
            stats.SetStartTimeMs(StartTs.MilliSeconds());
            stats.SetFinishTimeMs(FinishTs.MilliSeconds());
        }
            [[fallthrough]];
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
            stats.SetCpuTimeUs(StorageCpuTimeUs + ComputeCpuTimeUs.Sum);
            stats.SetDurationUs(TInstant::Now().MicroSeconds() - StartTs.MicroSeconds());
            [[fallthrough]];
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE:
            ComputeCpuTimeUs.ExportAggStats(*ExtraStats.MutableComputeCpuTimeUs());
            break;
        default:
            break;
    }

    for (auto& [path, t] : Tables) {
        auto& tableAggr = *stats.AddTables();
        tableAggr.SetTablePath(path);
        tableAggr.SetReadRows(t.StorageStats.ReadRows + t.ReadRows.Sum);
        tableAggr.SetReadBytes(t.StorageStats.ReadBytes + t.ReadBytes.Sum);
        tableAggr.SetWriteRows(t.StorageStats.WriteRows + t.WriteRows.Sum);
        tableAggr.SetWriteBytes(t.StorageStats.WriteBytes + t.WriteBytes.Sum);
        tableAggr.SetEraseRows(t.StorageStats.EraseRows + t.EraseRows.Sum);
        tableAggr.SetEraseBytes(t.StorageStats.EraseBytes + t.EraseBytes.Sum);
        tableAggr.SetAffectedPartitions(t.StorageStats.AffectedPartitions +
            (t.AffectedPartitionsUniqueCount ? t.AffectedPartitionsUniqueCount : t.AffectedPartitions.Sum)
        );
    }

    ExtraStats.SetAffectedShards(AffectedShards.size());
    stats.MutableExtra()->PackFrom(ExtraStats);
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

TBatchOperationExecutionStats::TBatchOperationExecutionStats(Ydb::Table::QueryStatsCollection::Mode statsMode)
    : StatsMode(statsMode) {}

void TBatchOperationExecutionStats::TakeExecStats(NYql::NDqProto::TDqExecutionStats&& stats) {
    for (const auto& tableStat : stats.GetTables()) {
        auto& tableStats = TableStats[tableStat.GetTablePath()];
        tableStats.ReadRows += tableStat.GetReadRows();
        tableStats.ReadBytes += tableStat.GetReadBytes();
        tableStats.WriteRows += tableStat.GetWriteRows();
        tableStats.WriteBytes += tableStat.GetWriteBytes();
        tableStats.EraseRows += tableStat.GetEraseRows();
        tableStats.EraseBytes += tableStat.GetEraseBytes();
    }

    CpuTimeUs += stats.GetCpuTimeUs();
    DurationUs += stats.GetDurationUs();
    ExecutersCpuTimeUs += stats.GetExecuterCpuTimeUs();
}

void TBatchOperationExecutionStats::ExportExecStats(NYql::NDqProto::TDqExecutionStats& stats) const {
    switch (StatsMode) {
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE:
            [[fallthrough]];
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
            stats.SetExecuterCpuTimeUs(ExecutersCpuTimeUs);
            stats.SetStartTimeMs(StartTs.MilliSeconds());
            stats.SetFinishTimeMs(FinishTs.MilliSeconds());
            [[fallthrough]];
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
            stats.SetCpuTimeUs(CpuTimeUs);
            stats.SetDurationUs(DurationUs);
            [[fallthrough]];
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE:
            [[fallthrough]];
        default:
            break;
    }

    for (const auto& [tablePath, tableStats] : TableStats) {
        auto& tableAggr = *stats.AddTables();
        tableAggr.SetTablePath(tablePath.c_str());
        tableAggr.SetReadRows(tableStats.ReadRows);
        tableAggr.SetReadBytes(tableStats.ReadBytes);
        tableAggr.SetWriteRows(tableStats.WriteRows);
        tableAggr.SetWriteBytes(tableStats.WriteBytes);
        tableAggr.SetEraseRows(tableStats.EraseRows);
        tableAggr.SetEraseBytes(tableStats.EraseBytes);

        // TODO: it is not correct for indexImplTables
        tableAggr.SetAffectedPartitions(AffectedPartitions.size());
    }
}

} // namespace NKikimr::NKqp
