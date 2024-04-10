#include "rpc_kqp_base.h"

#include <ydb/core/protos/kqp_stats.pb.h>

namespace NKikimr {
namespace NGRpcService {



void FillQueryStats(Ydb::TableStats::QueryStats& queryStats, const NKqpProto::TKqpStatsQuery& kqpStats) {
    uint64_t totalCpuTimeUs = 0;

    for (auto& exec : kqpStats.GetExecutions()) {
        auto durationUs = exec.GetDurationUs();
        auto cpuTimeUs = exec.GetCpuTimeUs();

        totalCpuTimeUs += cpuTimeUs;

        auto& toPhase = *queryStats.add_query_phases();
        toPhase.set_duration_us(durationUs);
        toPhase.set_cpu_time_us(cpuTimeUs);

        for (auto& table : exec.GetTables()) {
            auto& toTable = *toPhase.add_table_access();
            toTable.set_name(table.GetTablePath());

            if (table.GetReadRows() > 0) {
                toTable.mutable_reads()->set_rows(table.GetReadRows());
                toTable.mutable_reads()->set_bytes(table.GetReadBytes());
            }

            if (table.GetWriteRows() > 0) {
                toTable.mutable_updates()->set_rows(table.GetWriteRows());
                toTable.mutable_updates()->set_bytes(table.GetWriteBytes());
            }

            if (table.GetEraseRows() > 0) {
                toTable.mutable_deletes()->set_rows(table.GetEraseRows());
            }

            toTable.set_partitions_count(table.GetAffectedPartitions());
        }

        std::sort(toPhase.mutable_table_access()->begin(), toPhase.mutable_table_access()->end(),
            [](const Ydb::TableStats::TableAccessStats& a, const Ydb::TableStats::TableAccessStats& b) {
                return a.name() < b.name();
            });

        NKqpProto::TKqpExecutionExtraStats executionExtraStats;
        if (exec.HasExtra() && exec.GetExtra().UnpackTo(&executionExtraStats)) {
            toPhase.set_affected_shards(executionExtraStats.GetAffectedShards());
        }
    }

    totalCpuTimeUs += kqpStats.GetWorkerCpuTimeUs();

    if (kqpStats.HasCompilation()) {
        auto& compilation = kqpStats.GetCompilation();
        auto& toCompilation = *queryStats.mutable_compilation();
        toCompilation.set_from_cache(compilation.GetFromCache());
        toCompilation.set_duration_us(compilation.GetDurationUs());
        toCompilation.set_cpu_time_us(compilation.GetCpuTimeUs());

        totalCpuTimeUs += compilation.GetCpuTimeUs();
    }

    queryStats.set_process_cpu_time_us(kqpStats.GetWorkerCpuTimeUs());
    queryStats.set_total_cpu_time_us(totalCpuTimeUs);
    queryStats.set_total_duration_us(kqpStats.GetDurationUs());
}

void FillQueryStats(Ydb::TableStats::QueryStats& queryStats, const NKikimrKqp::TQueryResponse& kqpResponse) {
    FillQueryStats(queryStats, kqpResponse.GetQueryStats());
    queryStats.set_query_plan(kqpResponse.GetQueryPlan());
}

Ydb::Table::QueryStatsCollection::Mode GetCollectStatsMode(Ydb::Query::StatsMode mode) {
    switch (mode) {
        case Ydb::Query::StatsMode::STATS_MODE_NONE:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
        case Ydb::Query::StatsMode::STATS_MODE_BASIC:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC;
        case Ydb::Query::StatsMode::STATS_MODE_FULL:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL;
        case Ydb::Query::StatsMode::STATS_MODE_PROFILE:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE;
        default:
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_UNSPECIFIED;
    }
}

} // namespace NGRpcService
} // namespace NKikimr
