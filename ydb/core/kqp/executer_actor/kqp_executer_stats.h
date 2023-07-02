#pragma once

#include "kqp_tasks_graph.h"
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NKqp {

NYql::NDqProto::EDqStatsMode GetDqStatsMode(Ydb::Table::QueryStatsCollection::Mode mode);
NYql::NDqProto::EDqStatsMode GetDqStatsModeShard(Ydb::Table::QueryStatsCollection::Mode mode);

bool CollectFullStats(Ydb::Table::QueryStatsCollection::Mode statsMode);
bool CollectProfileStats(Ydb::Table::QueryStatsCollection::Mode statsMode);

struct TQueryExecutionStats {
private:
    std::map<ui32, std::map<ui32, ui32>> ShardsCountByNode;
    std::map<ui32, bool> UseLlvmByStageId;
public:
    const Ydb::Table::QueryStatsCollection::Mode StatsMode;
    const TKqpTasksGraph* const TasksGraph = nullptr;
    NYql::NDqProto::TDqExecutionStats* const Result;

    // basic stats
    std::unordered_set<ui64> AffectedShards;
    ui32 TotalTasks = 0;
    std::atomic<ui64> ResultBytes = 0;
    std::atomic<ui64> ResultRows = 0;    
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

    TQueryExecutionStats(Ydb::Table::QueryStatsCollection::Mode statsMode, const TKqpTasksGraph* const tasksGraph,
        NYql::NDqProto::TDqExecutionStats* const result)
        : StatsMode(statsMode)
        , TasksGraph(tasksGraph)
        , Result(result)
    {
    }

    void AddComputeActorStats(ui32 nodeId, NYql::NDqProto::TDqComputeActorStats&& stats);
    void AddNodeShardsCount(const ui32 stageId, const ui32 nodeId, const ui32 shardsCount) {
        Y_VERIFY(ShardsCountByNode[stageId].emplace(nodeId, shardsCount).second);
    }
    void SetUseLlvm(const ui32 stageId, const bool value) {
        Y_VERIFY(UseLlvmByStageId.emplace(stageId, value).second);
    }

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
