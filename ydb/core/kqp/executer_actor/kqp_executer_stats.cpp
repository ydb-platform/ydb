#include "kqp_executer_stats.h"

#include <ydb/core/protos/kqp.pb.h>

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
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
            return NYql::NDqProto::DQ_STATS_MODE_BASIC;
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
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
            return NYql::NDqProto::DQ_STATS_MODE_BASIC;
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

void TQueryExecutionStats::AddComputeActorStats(ui32 /* nodeId */, NYql::NDqProto::TDqComputeActorStats&& stats) {
//    Cerr << (TStringBuilder() << "::AddComputeActorStats " << stats.DebugString() << Endl);

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + stats.GetCpuTimeUs());
    TotalTasks += stats.GetTasks().size();

    UpdateAggr(ExtraStats.MutableComputeCpuTimeUs(), stats.GetCpuTimeUs());

    for (auto& task : stats.GetTasks()) {
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
    }

    if (CollectFullStats(StatsMode)) {
        for (auto& task : stats.GetTasks()) {
            auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);

            stageStats->SetTotalTasksCount(stageStats->GetTotalTasksCount() + 1);
            UpdateAggr(stageStats->MutableCpuTimeUs(), task.GetCpuTimeUs());
            UpdateAggr(stageStats->MutableInputRows(), task.GetInputRows());
            UpdateAggr(stageStats->MutableInputBytes(), task.GetInputBytes());
            UpdateAggr(stageStats->MutableOutputRows(), task.GetOutputRows());
            UpdateAggr(stageStats->MutableOutputBytes(), task.GetOutputBytes());

            UpdateMinMax(stageStats->MutableFirstRowTimeMs(), task.GetFirstRowTimeMs());
            UpdateMinMax(stageStats->MutableFinishTimeMs(), task.GetFinishTimeMs());

            stageStats->SetDurationUs((stageStats->GetFinishTimeMs().GetMax() - stageStats->GetFirstRowTimeMs().GetMin()) * 1'000);
        }
    }

    if (CollectProfileStats(StatsMode)) {
        for (auto& task : stats.GetTasks()) {
            auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);
            stageStats->AddComputeActors()->CopyFrom(stats);
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

void TQueryExecutionStats::AddDatashardStats(NYql::NDqProto::TDqComputeActorStats&& stats,
    NKikimrQueryStats::TTxStats&& txStats)
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

    for (auto& task : stats.GetTasks()) {
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
    }

    if (CollectFullStats(StatsMode)) {
        for (auto& task : stats.GetTasks()) {
            auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);

            stageStats->SetTotalTasksCount(stageStats->GetTotalTasksCount() + 1);
            UpdateAggr(stageStats->MutableCpuTimeUs(), task.GetCpuTimeUs());
            UpdateAggr(stageStats->MutableInputRows(), task.GetInputRows());
            UpdateAggr(stageStats->MutableInputBytes(), task.GetInputBytes());
            UpdateAggr(stageStats->MutableOutputRows(), task.GetOutputRows());
            UpdateAggr(stageStats->MutableOutputBytes(), task.GetOutputBytes());

            UpdateMinMax(stageStats->MutableFirstRowTimeMs(), task.GetFirstRowTimeMs());
            UpdateMinMax(stageStats->MutableFinishTimeMs(), task.GetFinishTimeMs());

            stageStats->SetDurationUs((stageStats->GetFinishTimeMs().GetMax() - stageStats->GetFirstRowTimeMs().GetMin()) * 1'000);

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

        if (CollectProfileStats(StatsMode)) {
            for (auto& task : stats.GetTasks()) {
                auto* stageStats = GetOrCreateStageStats(task, *TasksGraph, *Result);
                stageStats->AddComputeActors()->CopyFrom(stats);
            }
        }

        DatashardStats.emplace_back(std::move(txStats));
    }
}

void TQueryExecutionStats::Finish() {
//    Cerr << (TStringBuilder() << "-- finish: executerTime: " << ExecuterCpuTime.MicroSeconds() << Endl);

    Result->SetCpuTimeUs(Result->GetCpuTimeUs() + ExecuterCpuTime.MicroSeconds());
    Result->SetDurationUs(FinishTs.MicroSeconds() - StartTs.MicroSeconds());

    Result->SetResultBytes(ResultBytes);
    Result->SetResultRows(ResultRows);

    ExtraStats.SetAffectedShards(AffectedShards.size());
    if (CollectProfileStats(StatsMode)) {
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

    if (CollectFullStats(StatsMode)) {
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
