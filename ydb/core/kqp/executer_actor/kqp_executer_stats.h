#pragma once

#include "kqp_tasks_graph.h"
#include <util/generic/vector.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/core/protos/query_stats.pb.h>

namespace NKikimr {
namespace NKqp {

NYql::NDqProto::EDqStatsMode GetDqStatsMode(Ydb::Table::QueryStatsCollection::Mode mode);
NYql::NDqProto::EDqStatsMode GetDqStatsModeShard(Ydb::Table::QueryStatsCollection::Mode mode);

bool CollectFullStats(Ydb::Table::QueryStatsCollection::Mode statsMode);
bool CollectProfileStats(Ydb::Table::QueryStatsCollection::Mode statsMode);

struct TTimeSeriesStats {
    std::vector<ui64> Values;
    ui32 HistorySampleCount = 0;
    ui64 Sum = 0;
    std::vector<std::pair<ui64, ui64>> History;

    void ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqStatsAggr& stats);
    void ExportAggStats(ui64 baseTimeMs, NYql::NDqProto::TDqStatsAggr& stats);
    void Resize(ui32 taskCount);
    void SetNonZero(ui32 taskIndex, ui64 value);
    void Pack();
};

struct TAsyncStats {
    // Data
    TTimeSeriesStats Bytes;
    std::vector<ui64> DecompressedBytes;
    std::vector<ui64> Rows;
    std::vector<ui64> Chunks;
    std::vector<ui64> Splits;
    // Time
    std::vector<ui64> FirstMessageMs;
    std::vector<ui64> PauseMessageMs;
    std::vector<ui64> ResumeMessageMs;
    std::vector<ui64> LastMessageMs;
    TTimeSeriesStats WaitTimeUs;
    std::vector<ui64> WaitPeriods;
    std::vector<ui64> ActiveTimeUs;

    void Resize(ui32 taskCount);
    void SetHistorySampleCount(ui32 historySampleCount);
    void ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqAsyncStatsAggr& stats);
};

struct TAsyncBufferStats {

    TAsyncBufferStats() = default;
    TAsyncBufferStats(ui32 taskCount) {
        Resize(taskCount);
    }

    TAsyncStats Ingress;
    TAsyncStats Push;
    TAsyncStats Pop;
    TAsyncStats Egress;

    void Resize(ui32 taskCount);
    void SetHistorySampleCount(ui32 historySampleCount);
    void ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqAsyncBufferStatsAggr& stats);
};

struct TTableStats {

    TTableStats() = default;
    TTableStats(ui32 taskCount) {
        Resize(taskCount);
    }

    std::vector<ui64> ReadRows;
    std::vector<ui64> ReadBytes;
    std::vector<ui64> WriteRows;
    std::vector<ui64> WriteBytes;
    std::vector<ui64> EraseRows;
    std::vector<ui64> EraseBytes;

    std::vector<ui64> AffectedPartitions;

    void Resize(ui32 taskCount);
};

struct TStageExecutionStats {

    NYql::NDq::TStageId StageId;

    std::map<ui32, ui32> Task2Index;

    TTimeSeriesStats CpuTimeUs;
    std::vector<ui64> SourceCpuTimeUs;

    std::vector<ui64> InputRows;
    std::vector<ui64> InputBytes;
    std::vector<ui64> OutputRows;
    std::vector<ui64> OutputBytes;
    std::vector<ui64> ResultRows;
    std::vector<ui64> ResultBytes;
    std::vector<ui64> IngressRows;
    std::vector<ui64> IngressBytes;
    std::vector<ui64> IngressDecompressedBytes;
    std::vector<ui64> EgressRows;
    std::vector<ui64> EgressBytes;

    std::vector<ui64> FinishTimeMs;
    std::vector<ui64> StartTimeMs;
    std::vector<ui64> DurationUs;
    std::vector<ui64> WaitInputTimeUs;
    std::vector<ui64> WaitOutputTimeUs;

    TTimeSeriesStats SpillingBytes;
    TTimeSeriesStats SpillingTimeUs;

    std::map<TString, TTableStats> Tables;
    std::map<TString, TAsyncBufferStats> Ingress;
    std::map<TString, TAsyncBufferStats> Egress;
    std::map<ui32, TAsyncBufferStats> Input;
    std::map<ui32, TAsyncBufferStats> Output;

    TTimeSeriesStats MaxMemoryUsage;
    ui32 HistorySampleCount;

    void Resize(ui32 taskCount);
    void SetHistorySampleCount(ui32 historySampleCount);
    void ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqStageStats& stageStats);
    ui64 UpdateAsyncStats(i32 index, TAsyncStats& aggrAsyncStats, const NYql::NDqProto::TDqAsyncBufferStats& asyncStats);
    ui64 UpdateStats(const NYql::NDqProto::TDqTaskStats& taskStats, ui64 maxMemoryUsage, ui64 durationUs);
};

struct TQueryExecutionStats {
private:
    std::map<ui32, std::map<ui32, ui32>> ShardsCountByNode;
    std::map<ui32, bool> UseLlvmByStageId;
    std::map<ui32, TStageExecutionStats> StageStats;
    ui64 BaseTimeMs = 0;
    void ExportAggAsyncStats(TAsyncStats& data, NYql::NDqProto::TDqAsyncStatsAggr& stats);
    void ExportAggAsyncBufferStats(TAsyncBufferStats& data, NYql::NDqProto::TDqAsyncBufferStatsAggr& stats);
    void AdjustAsyncAggr(NYql::NDqProto::TDqAsyncStatsAggr& stats);
    void AdjustAsyncBufferAggr(NYql::NDqProto::TDqAsyncBufferStatsAggr& stats);
    void AdjustDqStatsAggr(NYql::NDqProto::TDqStatsAggr& stats);
    void AdjustBaseTime(NYql::NDqProto::TDqStageStats* stageStats);
public:
    const Ydb::Table::QueryStatsCollection::Mode StatsMode;
    const TKqpTasksGraph* const TasksGraph = nullptr;
    NYql::NDqProto::TDqExecutionStats* const Result;

    // basic stats
    std::unordered_set<ui64> AffectedShards;
    ui32 HistorySampleCount = 0;
    ui32 TotalTasks = 0;
    ui64 ResultBytes = 0;
    ui64 ResultRows = 0;
    TDuration ExecuterCpuTime;

    TInstant StartTs;
    TInstant FinishTs;

    std::unordered_map<TString, NYql::NDqProto::TDqTableStats*> TableStats;
    std::unordered_map<TString, std::unordered_set<ui64>> TableShards;

    NKqpProto::TKqpExecutionExtraStats ExtraStats;

    // profile stats
    TDuration ResolveCpuTime;
    TDuration ResolveWallTime;
    TVector<NKikimrQueryStats::TTxStats> DatashardStats;

    bool CollectStatsByLongTasks = false;

    TQueryExecutionStats(Ydb::Table::QueryStatsCollection::Mode statsMode, const TKqpTasksGraph* const tasksGraph,
        NYql::NDqProto::TDqExecutionStats* const result)
        : StatsMode(statsMode)
        , TasksGraph(tasksGraph)
        , Result(result)
    {
        HistorySampleCount = 32;
    }

    void AddComputeActorStats(
        ui32 nodeId,
        NYql::NDqProto::TDqComputeActorStats&& stats,
        TDuration collectLongTaskStatsTimeout = TDuration::Max()
    );
    void AddNodeShardsCount(const ui32 stageId, const ui32 nodeId, const ui32 shardsCount) {
        Y_ABORT_UNLESS(ShardsCountByNode[stageId].emplace(nodeId, shardsCount).second);
    }
    void SetUseLlvm(const ui32 stageId, const bool value) {
        Y_ABORT_UNLESS(UseLlvmByStageId.emplace(stageId, value).second);
    }

    void AddDatashardPrepareStats(NKikimrQueryStats::TTxStats&& txStats);
    void AddDatashardStats(
        NYql::NDqProto::TDqComputeActorStats&& stats,
        NKikimrQueryStats::TTxStats&& txStats,
        TDuration collectLongTaskStatsTimeout = TDuration::Max()
    );
    void AddDatashardStats(NKikimrQueryStats::TTxStats&& txStats);

    void UpdateTaskStats(ui64 taskId, const NYql::NDqProto::TDqComputeActorStats& stats);
    void ExportExecStats(NYql::NDqProto::TDqExecutionStats& stats);
    void FillStageDurationUs(NYql::NDqProto::TDqStageStats& stats);

    void Finish();

private:
    void AddComputeActorFullStatsByTask(
        const NYql::NDqProto::TDqTaskStats& task,
        const NYql::NDqProto::TDqComputeActorStats& stats);
    void AddComputeActorProfileStatsByTask(
        const NYql::NDqProto::TDqTaskStats& task,
        const NYql::NDqProto::TDqComputeActorStats& stats);
    void AddDatashardFullStatsByTask(
        const NYql::NDqProto::TDqTaskStats& task,
        ui64 datashardCpuTimeUs);
};

struct TTableStat {
    ui64 Rows = 0;
    ui64 Bytes = 0;

    TTableStat& operator+=(const TTableStat& rhs);
    TTableStat& operator-=(const TTableStat& rhs);
};

struct TProgressStatEntry {
    TDuration ComputeTime;
    TTableStat ReadIOStat;
    bool Defined = false;

    TProgressStatEntry& operator+=(const TProgressStatEntry& rhs);

    void Out(IOutputStream& o) const;
};

TTableStat CalcSumTableReadStat(const TProgressStatEntry& entry);
TDuration CalcCumComputeTime(const TProgressStatEntry& entry);

class TProgressStat {
public:
    using TEntry = TProgressStatEntry;

    TProgressStat() = default;

    void Set(const NYql::NDqProto::TDqComputeActorStats& stats);

    void Update();

    TEntry GetLastUsage() const;

private:
    TEntry Total;
    TEntry Cur;
};

} // namespace NKqp
} // namespace NKikimr

template<>
inline void Out<NKikimr::NKqp::TProgressStatEntry>(IOutputStream& o, const NKikimr::NKqp::TProgressStatEntry& x) {
    return x.Out(o);
}
