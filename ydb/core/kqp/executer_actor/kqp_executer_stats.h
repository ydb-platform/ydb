#pragma once

#include "kqp_tasks_graph.h"

#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_counters.h>

namespace NKikimr {
namespace NKqp {

NYql::NDqProto::EDqStatsMode GetDqStatsMode(Ydb::Table::QueryStatsCollection::Mode mode);
NYql::NDqProto::EDqStatsMode GetDqStatsModeShard(Ydb::Table::QueryStatsCollection::Mode mode);

bool CollectBasicStats(Ydb::Table::QueryStatsCollection::Mode statsMode);
bool CollectFullStats(Ydb::Table::QueryStatsCollection::Mode statsMode);
bool CollectProfileStats(Ydb::Table::QueryStatsCollection::Mode statsMode);

struct TMinStats {
    std::vector<ui64> Values;
    ui64 MinValue = 0;

    void Resize(ui32 count);
    void SetNonZero(ui32 index, ui64 value);
};

struct TMaxStats {
    std::vector<ui64> Values;
    ui64 MaxValue = 0;

    void Resize(ui32 count);
    void SetNonZero(ui32 index, ui64 value);
};

struct TSumStats {
    std::vector<ui64> Values;
    ui64 Sum = 0;

    void Resize(ui32 count);
    void SetNonZero(ui32 index, ui64 value);
    ui64 ExportAggStats(NYql::NDqProto::TDqStatsAggr& stats);
};

struct TTimeSeriesStats : public TSumStats {
    using TSumStats::ExportAggStats;

    ui32 HistorySampleCount = 0;
    std::vector<std::pair<ui64, ui64>> History;

    void ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqStatsAggr& stats);
    ui64 ExportAggStats(ui64 baseTimeMs, NYql::NDqProto::TDqStatsAggr& stats);
    void SetNonZero(ui32 index, ui64 value);
    void Pack();
    void AppendHistory();
};

struct TPartitionedStats : public TTimeSeriesStats {
    std::vector<std::vector<ui64>> Parts;

    void ResizeByTasks(ui32 taskCount);
    void ResizeByParts(ui32 partCount, ui32 taskCount);
    void SetNonZeroAggSum(ui32 taskIndex, ui32 partIndex, ui64 value, bool recordTimeSeries);
    void SetNonZeroAggMin(ui32 taskIndex, ui32 partIndex, ui64 value, bool recordTimeSeries);
    void SetNonZeroAggMax(ui32 taskIndex, ui32 partIndex, ui64 value, bool recordTimeSeries);
};

enum EPartitionedAggKind {
    PartitionedAggSum,
    PartitionedAggMin,
    PartitionedAggMax,
};

struct TTimeMultiSeriesStats {
    std::unordered_map<TString, ui32> Indices;
    ui32 TaskCount = 0;
    ui32 PartCount = 0;

    void SetNonZero(TPartitionedStats& stats, ui32 taskIndex, const TString& key, ui64 value, bool recordTimeSeries, EPartitionedAggKind aggKind);
};

struct TExternalStats : public TTimeMultiSeriesStats {
    TPartitionedStats ExternalRows;
    TPartitionedStats ExternalBytes;
    TPartitionedStats FirstMessageMs;
    TPartitionedStats LastMessageMs;
    TPartitionedStats CpuTimeUs;
    TPartitionedStats WaitInputTimeUs;
    TPartitionedStats WaitOutputTimeUs;
    TPartitionedStats Finished;
    ui32 PartitionCount = 0;

    void Resize(ui32 taskCount);
    void SetHistorySampleCount(ui32 historySampleCount);
    void ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqExternalAggrStats& stats);
};

struct TMetricInfo {

    TMetricInfo(ui32 sc = 0, ui32 ts = 0) : ScalarCount(sc), TimeSeriesCount(ts) {}

    TMetricInfo operator+(TMetricInfo other) {
        return TMetricInfo(ScalarCount + other.ScalarCount, TimeSeriesCount + other.TimeSeriesCount);
    }
    TMetricInfo& operator+=(TMetricInfo other) {
        ScalarCount += other.ScalarCount;
        TimeSeriesCount += other.TimeSeriesCount;
        return *this;
    }
    TMetricInfo operator*(ui32 m) {
        return TMetricInfo(ScalarCount * m, TimeSeriesCount * m);
    }

    ui32 ScalarCount;
    ui32 TimeSeriesCount;
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
    static TMetricInfo EstimateMem() {
        return TMetricInfo(10, 2);
    }
    void SetHistorySampleCount(ui32 historySampleCount);
    void ExportHistory(ui64 baseTimeMs, NYql::NDqProto::TDqAsyncStatsAggr& stats);
};

struct TAsyncBufferStats {

    TAsyncBufferStats() = default;
    TAsyncBufferStats(ui32 taskCount) {
        Resize(taskCount);
    }

    TExternalStats External;
    TAsyncStats Ingress;
    TAsyncStats Push;
    TAsyncStats Pop;
    TAsyncStats Egress;
    std::vector<ui64> LocalBytes;

    void Resize(ui32 taskCount);
    static TMetricInfo EstimateMem() {
        return TAsyncStats::EstimateMem() * 4;
    }
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
    static TMetricInfo EstimateMem() {
        return TMetricInfo(7);
    }
};

struct TOperatorStats {

    TOperatorStats() = default;

    TOperatorStats(ui32 taskCount) {
        Resize(taskCount);
    }

    std::vector<ui64> Rows;
    std::vector<ui64> Bytes;

    NYql::NDq::TOperatorType OperatorType;

    void Resize(ui32 taskCount);
    static TMetricInfo EstimateMem() {
        return TMetricInfo(2);
    }
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
    TTimeSeriesStats WaitInputTimeUs;
    TTimeSeriesStats WaitOutputTimeUs;
    TMinStats CurrentWaitInputTimeUs;
    TMinStats CurrentWaitOutputTimeUs;
    ui64 UpdateTimeMs = 0;
    ui64 MaxFinishTimeMs = 0;

    TTimeSeriesStats SpillingComputeBytes;
    TTimeSeriesStats SpillingChannelBytes;
    TTimeSeriesStats SpillingComputeTimeUs;
    TTimeSeriesStats SpillingChannelTimeUs;

    std::map<TString, TTableStats> Tables;
    std::map<TString, TAsyncBufferStats> Ingress;
    std::map<TString, TAsyncBufferStats> Egress;
    std::unordered_map<ui32, TAsyncBufferStats> Input;
    std::unordered_map<ui32, TAsyncBufferStats> Output;

    std::map<TString, TOperatorStats> Joins;
    std::map<TString, TOperatorStats> Filters;
    std::map<TString, TOperatorStats> Aggregations;

    std::unordered_map<TString, std::vector<ui64>> Mkql;

    TTimeSeriesStats MaxMemoryUsage;

    ui32 HistorySampleCount = 0;
    ui32 TaskCount = 0; // up rounded to multiple of 4, actual is Task2Index.size()
    std::vector<bool> Finished;
    ui32 FinishedCount = 0;
    std::vector<TStageExecutionStats*> InputStages;
    std::vector<TStageExecutionStats*> OutputStages;
    std::unordered_map<ui32, NYql::NDqProto::TDqComputeActorStats> ComputeActors;

    void Resize(ui32 taskCount);
    ui32 EstimateMem() {
        TMetricInfo info(15, 8);
        info += TAsyncBufferStats::EstimateMem() * (Ingress.size() + Egress.size() + Input.size() + Output.size());
        info += TTableStats::EstimateMem() * Tables.size();
        info += TOperatorStats::EstimateMem() * (Joins.size() + Filters.size() + Aggregations.size());
        return (info.ScalarCount * TaskCount + info.TimeSeriesCount * HistorySampleCount * 2) * sizeof(ui64);
    }
    void SetHistorySampleCount(ui32 historySampleCount);
    ui64 UpdateAsyncStats(ui32 index, TAsyncStats& aggrAsyncStats, const NYql::NDqProto::TDqAsyncBufferStats& asyncStats);
    ui64 UpdateStats(const NYql::NDqProto::TDqTaskStats& taskStats, NYql::NDqProto::EComputeState state, ui64 maxMemoryUsage, ui64 durationUs);
    bool IsDeadlocked(ui64 deadline);
    bool IsFinished();
};

struct TExternalPartitionStat {
    ui64 ExternalRows = 0;
    ui64 ExternalBytes = 0;
    ui64 FirstMessageMs = 0;
    ui64 LastMessageMs = 0;
    ui64 CpuTimeUs;
    ui64 WaitInputTimeUs;
    ui64 WaitOutputTimeUs;
    bool Finished = false;
    TExternalPartitionStat() = default;
    TExternalPartitionStat(ui64 externalRows, ui64 externalBytes, ui64 firstMessageMs, ui64 lastMessageMs, ui64 cpuTimeUs, ui64 waitInputTimeUs, ui64 waitOutputTimeUs, bool finished)
    : ExternalRows(externalRows), ExternalBytes(externalBytes), FirstMessageMs(firstMessageMs), LastMessageMs(lastMessageMs), CpuTimeUs(cpuTimeUs), WaitInputTimeUs(waitInputTimeUs), WaitOutputTimeUs(waitOutputTimeUs), Finished(finished)
    {}
};

struct TIngressExternalPartitionStat {
    TString Name;
    std::map<TString, TExternalPartitionStat> Stat;
    TIngressExternalPartitionStat() = default;
    TIngressExternalPartitionStat(const TString& name) : Name(name) {}
};

struct TStorageTableStats {
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    ui64 WriteRows = 0;
    ui64 WriteBytes = 0;
    ui64 EraseRows = 0;
    ui64 EraseBytes = 0;
    ui64 AffectedPartitions = 0;
};

struct TQueryTableStats {

    TQueryTableStats() = default;
    TQueryTableStats(ui32 taskCount) {
        Resize(taskCount);
    }

    TSumStats ReadRows;
    TSumStats ReadBytes;
    TSumStats WriteRows;
    TSumStats WriteBytes;
    TSumStats EraseRows;
    TSumStats EraseBytes;
    TSumStats AffectedPartitions;
    TStorageTableStats StorageStats;
    ui64 AffectedPartitionsUniqueCount = 0;

    void Resize(ui32 taskCount);
};

struct TQueryExecutionStats {
private:
    std::unordered_map<ui32, std::map<ui32, ui32>> ShardsCountByNode;
    std::unordered_map<ui32, bool> UseLlvmByStageId;
    THashMap<NYql::NDq::TStageId, TStageExecutionStats> StageStats;
    std::unordered_map<ui32, TIngressExternalPartitionStat> ExternalPartitionStats; // FIXME: several ingresses
    ui64 BaseTimeMs = 0;
    std::unordered_map<ui32, TDuration> LongestTaskDurations;
    void ExportAggAsyncStats(TAsyncStats& data, NYql::NDqProto::TDqAsyncStatsAggr& stats);
    void ExportAggAsyncBufferStats(TAsyncBufferStats& data, NYql::NDqProto::TDqAsyncBufferStatsAggr& stats);
public:
    const Ydb::Table::QueryStatsCollection::Mode StatsMode;
    const TKqpTasksGraph* const TasksGraph = nullptr;
    NYql::NDqProto::TDqExecutionStats* const Result;
    std::optional<ui32> DeadlockedStageId;

    // common stats
    ui64 StorageCpuTimeUs = 0;
    TSumStats ComputeCpuTimeUs;

    // basic stats
    ui32 TaskCount = 0;
    ui32 TaskCount4 = 0;
    std::map<TString, TQueryTableStats> Tables;

    std::unordered_set<ui64> AffectedShards;
    ui64 ResultBytes = 0;
    ui64 ResultRows = 0;
    TDuration ExecuterCpuTime;

    TInstant StartTs;
    TInstant FinishTs;

    std::unordered_map<TString, NYql::NDqProto::TDqTableStats*> TableStats;
    std::unordered_map<TString, std::unordered_set<ui64>> TableShards;

    NKqpProto::TKqpExecutionExtraStats ExtraStats;

    // full stats
    ui32 HistorySampleCount = 0;

    // profile stats
    TDuration ResolveCpuTime;
    TDuration ResolveWallTime;

    bool CollectStatsByLongTasks = false;

    TQueryExecutionStats(Ydb::Table::QueryStatsCollection::Mode statsMode, const TKqpTasksGraph* const tasksGraph,
        NYql::NDqProto::TDqExecutionStats* const result)
        : StatsMode(statsMode)
        , TasksGraph(tasksGraph)
        , Result(result)
    {
        HistorySampleCount = 32;
    }

    void Prepare();

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
    void AddBufferStats(NYql::NDqProto::TDqTaskStats&& taskStats);

    ui64 LocksBrokenAsBreaker = 0;
    ui64 LocksBrokenAsVictim = 0;
    TVector<ui64> BreakerQuerySpanIds;

    void CollectLockStats(const NKikimrQueryStats::TTxStats& txStats);

    void UpdateQueryTables(const NYql::NDqProto::TDqTaskStats& taskStats, NKikimrQueryStats::TTxStats* txStats);
    void UpdateStorageTables(const NYql::NDqProto::TDqTaskStats& taskStats, NKikimrQueryStats::TTxStats* txStats);
    void UpdateTaskStats(ui64 taskId, const NYql::NDqProto::TDqComputeActorStats& stats, NKikimrQueryStats::TTxStats* txStats,
        NYql::NDqProto::EComputeState state, TDuration collectLongTaskStatsTimeout);
    void ExportExecStats(NYql::NDqProto::TDqExecutionStats& stats);
    void FillStageDurationUs(NYql::NDqProto::TDqStageStats& stats);
    ui64 EstimateCollectMem();
    ui64 EstimateFinishMem();
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

struct TBatchOperationTableStats {
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    ui64 WriteRows = 0;
    ui64 WriteBytes = 0;
    ui64 EraseRows = 0;
    ui64 EraseBytes = 0;
};

struct TBatchOperationExecutionStats {
public:
    explicit TBatchOperationExecutionStats(Ydb::Table::QueryStatsCollection::Mode statsMode);

    void TakeExecStats(NYql::NDqProto::TDqExecutionStats&& stats);

    void ExportExecStats(NYql::NDqProto::TDqExecutionStats& stats) const;

public:
    const Ydb::Table::QueryStatsCollection::Mode StatsMode;

    // Local stats
    TInstant StartTs = TInstant::Max();
    TInstant FinishTs = TInstant::Max();
    std::unordered_set<ui64> AffectedPartitions;

    // Per-table accumulated stats from child executers
    std::unordered_map<std::string, TBatchOperationTableStats> TableStats;

    // Common accumulated stats from child executers
    ui64 CpuTimeUs = 0;
    ui64 DurationUs = 0;
    ui64 ExecutersCpuTimeUs = 0;
};

} // namespace NKqp
} // namespace NKikimr

template<>
inline void Out<NKikimr::NKqp::TProgressStatEntry>(IOutputStream& o, const NKikimr::NKqp::TProgressStatEntry& x) {
    return x.Out(o);
}
