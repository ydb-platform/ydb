#pragma once

#include "kqp_tasks_graph.h" 
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NKqp {

struct TQueryExecutionStats {
    const NYql::NDqProto::EDqStatsMode StatsMode;
    const TKqpTasksGraph* const TasksGraph = nullptr;
    NYql::NDqProto::TDqExecutionStats* const Result;

    // basic stats
    std::unordered_set<ui64> AffectedShards;
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

    TQueryExecutionStats(NYql::NDqProto::EDqStatsMode statsMode, const TKqpTasksGraph* const tasksGraph,
        NYql::NDqProto::TDqExecutionStats* const result)
        : StatsMode(statsMode)
        , TasksGraph(tasksGraph)
        , Result(result)
    {
        YQL_ENSURE(StatsMode >= NYql::NDqProto::DQ_STATS_MODE_BASIC);
    }

    void AddComputeActorStats(ui32 nodeId, NYql::NDqProto::TDqComputeActorStats&& stats);

    void AddDatashardPrepareStats(NKikimrQueryStats::TTxStats&& txStats);
    void AddDatashardStats(NYql::NDqProto::TDqComputeActorStats&& stats, NKikimrQueryStats::TTxStats&& txStats);

    void Finish();
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
