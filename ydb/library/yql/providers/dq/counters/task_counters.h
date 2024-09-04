#pragma once

#include "counters.h"
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <util/string/split.h>

namespace NYql {

struct TTaskCounters : public TCounters {

    void AddAsyncStats(const NDqProto::TDqAsyncBufferStats stats, const std::map<TString, TString>& l, const TString& p) {
        if (auto v = stats.GetBytes();  v) SetCounter(GetCounterName("TaskRunner", l, p + "Bytes"), v);
        if (auto v = stats.GetDecompressedBytes(); v) SetCounter(GetCounterName("TaskRunner", l, p + "DecompressedBytes"), v);
        if (auto v = stats.GetRows();   v) SetCounter(GetCounterName("TaskRunner", l, p + "Rows"), v);
        if (auto v = stats.GetChunks(); v) SetCounter(GetCounterName("TaskRunner", l, p + "Chunks"), v);
        if (auto v = stats.GetSplits(); v) SetCounter(GetCounterName("TaskRunner", l, p + "Splits"), v);

        auto firstMessageMs = stats.GetFirstMessageMs();
        auto lastMessageMs = stats.GetLastMessageMs();

        if (firstMessageMs)                              SetCounter(GetCounterName("TaskRunner", l, p + "FirstMessageMs"), firstMessageMs);
        if (auto v = stats.GetPauseMessageMs();  v) SetCounter(GetCounterName("TaskRunner", l, p + "PauseMessageMs"), v);
        if (auto v = stats.GetResumeMessageMs(); v) SetCounter(GetCounterName("TaskRunner", l, p + "ResumeMessageMs"), v);
        if (lastMessageMs)                               SetCounter(GetCounterName("TaskRunner", l, p + "LastMessageMs"), lastMessageMs);
        
        if (auto v = stats.GetWaitTimeUs();  v) SetCounter(GetCounterName("TaskRunner", l, p + "WaitTimeUs"), v);
        if (auto v = stats.GetWaitPeriods(); v) SetCounter(GetCounterName("TaskRunner", l, p + "WaitPeriods"), v);

        if (firstMessageMs && lastMessageMs && (firstMessageMs < lastMessageMs)) {
            SetCounter(GetCounterName("TaskRunner", l, p + "ActiveTimeUs"),
                (TInstant::MilliSeconds(lastMessageMs) - TInstant::MilliSeconds(firstMessageMs)).MicroSeconds()
            );
        }
    }

    void AddAsyncStats(const NDq::TDqAsyncStats stats, const std::map<TString, TString>& l, const TString& p) {
        if (stats.Bytes)  SetCounter(GetCounterName("TaskRunner", l, p + "Bytes"),  stats.Bytes);
        if (stats.Rows)   SetCounter(GetCounterName("TaskRunner", l, p + "Rows"),   stats.Rows);
        if (stats.Chunks) SetCounter(GetCounterName("TaskRunner", l, p + "Chunks"), stats.Chunks);
        if (stats.Splits) SetCounter(GetCounterName("TaskRunner", l, p + "Splits"), stats.Splits);

        if (stats.FirstMessageTs)  SetCounter(GetCounterName("TaskRunner", l, p + "FirstMessageMs"),  stats.FirstMessageTs.MilliSeconds());
        if (stats.PauseMessageTs)  SetCounter(GetCounterName("TaskRunner", l, p + "PauseMessageMs"),  stats.PauseMessageTs.MilliSeconds());
        if (stats.ResumeMessageTs) SetCounter(GetCounterName("TaskRunner", l, p + "ResumeMessageMs"), stats.ResumeMessageTs.MilliSeconds());
        if (stats.LastMessageTs)   SetCounter(GetCounterName("TaskRunner", l, p + "LastMessageMs"),   stats.LastMessageTs.MilliSeconds());
        
        if (stats.WaitTime)        SetCounter(GetCounterName("TaskRunner", l, p + "WaitTimeUs"), stats.WaitTime.MicroSeconds());
        if (stats.WaitPeriods)     SetCounter(GetCounterName("TaskRunner", l, p + "WaitPeriods"), stats.WaitPeriods);

        auto activeTime = stats.LastMessageTs - stats.FirstMessageTs;
        if (activeTime) {
            SetCounter(GetCounterName("TaskRunner", l, p + "ActiveTimeUs"), activeTime.MicroSeconds());
        }
    }

    void AddInputChannelStats(
        const NDq::TDqInputChannelStats& pushStats,
        const NDq::TDqInputStats& popStats,
        ui64 taskId, ui32 stageId)
    {
        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)},
            {"Stage", ToString(stageId)},
            {"InputChannel", ToString(pushStats.ChannelId)},
            {"SrcStageId", ToString(pushStats.SrcStageId)}
        };
        AddAsyncStats(pushStats, labels, "Push");
        AddAsyncStats(popStats, labels, "Pop");
        if (pushStats.DeserializationTime) SetCounter(GetCounterName("TaskRunner", labels, "DeserializationTime"), pushStats.DeserializationTime.MicroSeconds());
        if (pushStats.MaxMemoryUsage)      SetCounter(GetCounterName("TaskRunner", labels, "MaxMemoryUsage"),      pushStats.MaxMemoryUsage);
        if (pushStats.RowsInMemory)        SetCounter(GetCounterName("TaskRunner", labels, "RowsInMemory"),        pushStats.RowsInMemory);
    }

    void AddSourceStats(
        const NDq::TDqAsyncInputBufferStats& pushStats,
        const NDq::TDqInputStats& popStats,
        ui64 taskId, ui32 stageId)
    {
        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)},
            {"Stage", ToString(stageId)},
            {"SourceIndex", ToString(pushStats.InputIndex)}
        };
        AddAsyncStats(pushStats, labels, "Push");
        AddAsyncStats(popStats, labels, "Pop");
        if (pushStats.MaxMemoryUsage)      SetCounter(GetCounterName("TaskRunner", labels, "MaxMemoryUsage"),      pushStats.MaxMemoryUsage);
        if (pushStats.RowsInMemory)        SetCounter(GetCounterName("TaskRunner", labels, "RowsInMemory"),        pushStats.RowsInMemory);
    }

    void AddOutputChannelStats(
        const NDq::TDqOutputStats& pushStats,
        const NDq::TDqOutputChannelStats& popStats,
        ui64 taskId, ui32 stageId)
    {
        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)},
            {"Stage", ToString(stageId)},
            {"OutputChannel", ToString(popStats.ChannelId)},
            {"DstStageId", ToString(popStats.DstStageId)}
        };
        AddAsyncStats(pushStats, labels, "Push");
        AddAsyncStats(popStats, labels, "Pop");
        if (popStats.MaxMemoryUsage)    SetCounter(GetCounterName("TaskRunner", labels, "MaxMemoryUsage"),    popStats.MaxMemoryUsage);
        if (popStats.MaxRowsInMemory)   SetCounter(GetCounterName("TaskRunner", labels, "MaxRowsInMemory"),   popStats.MaxRowsInMemory);
        if (popStats.SerializationTime) SetCounter(GetCounterName("TaskRunner", labels, "SerializationTime"), popStats.SerializationTime.MicroSeconds());
        if (popStats.SpilledBlobs)      SetCounter(GetCounterName("TaskRunner", labels, "SpilledBlobs"),      popStats.SpilledBlobs);
        if (popStats.SpilledBytes)      SetCounter(GetCounterName("TaskRunner", labels, "SpilledBytes"),      popStats.SpilledBytes);
        if (popStats.SpilledRows)       SetCounter(GetCounterName("TaskRunner", labels, "SpilledRows"),       popStats.SpilledRows);
    }

    void AddTaskRunnerStats(
        const NDq::TDqTaskRunnerStats& stats,
        ui64 taskId, ui32 stageId)
    {
        std::map<TString, TString> labels = {
            {"Task", ToString(taskId)},
            {"Stage", ToString(stageId)}
        };
        if (stats.ComputeCpuTime) SetCounter(GetCounterName("TaskRunner", labels, "ComputeCpuTime"), stats.ComputeCpuTime.MicroSeconds());
        if (stats.BuildCpuTime)   SetCounter(GetCounterName("TaskRunner", labels, "BuildCpuTime"),   stats.BuildCpuTime.MicroSeconds());
        if (stats.WaitInputTime)  SetCounter(GetCounterName("TaskRunner", labels, "WaitInputTime"),  stats.WaitInputTime.MicroSeconds());
        if (stats.WaitOutputTime) SetCounter(GetCounterName("TaskRunner", labels, "WaitOutputTime"), stats.WaitOutputTime.MicroSeconds());
    }

    void TakeDeltaCounters(TTaskCounters& CurrentMetrics, TTaskCounters& PrevMetrics) {
        for (const auto& [name, entry] : CurrentMetrics.Counters) {
            auto it = PrevMetrics.Counters.find(name);
            if (it == PrevMetrics.Counters.end()) {
                AddCounter(name, entry.Sum);
                PrevMetrics.AddCounter(name, entry.Sum);
            } else {
                auto delta = entry.Sum - it->second.Sum;
                if (delta) {
                    it->second.Sum = entry.Sum;
                    AddCounter(name, delta);
                }
            }
        }
    }
};

TTaskCounters AggregateQueryStatsByStage(TTaskCounters& queryStat, const THashMap<ui64, ui64>& task2Stage, bool collectFull = true);

} // namespace NYql
