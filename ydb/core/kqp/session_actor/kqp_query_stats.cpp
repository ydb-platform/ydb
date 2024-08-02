#include <ydb/core/kqp/session_actor/kqp_query_stats.h>

#include <library/cpp/time_provider/time_provider.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/sys_view/common/common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

const TVector<NYql::NDqProto::TDqExecutionStats>& TKqpQueryStats::GetExecutions() const {
    return Executions;
}

ui64 TKqpQueryStats::GetWorkerCpuTimeUs() const {
    return WorkerCpuTimeUs;
}

constexpr size_t QUERY_TEXT_LIMIT = 4096;

template <typename T>
void CollectQueryStatsImpl(const TActorContext& ctx, const T* queryStats,
    TDuration queryDuration, const TString& queryText,
    const TString& userSID, ui64 parametersSize, const TString& database,
    const NKikimrKqp::EQueryType type, ui64 requestUnits)
{
    if (!AppData()->FeatureFlags.GetEnableSystemViews()) {
        return;
    }

    auto collectEv = MakeHolder<NSysView::TEvSysView::TEvCollectQueryStats>();
    collectEv->Database = database;

    auto& stats = collectEv->QueryStats;
    auto& dataStats = *stats.MutableStats();
    auto& shardsCpuTime = *stats.MutableShardsCpuTimeUs();
    auto& computeCpuTime = *stats.MutableComputeCpuTimeUs();

    auto nodeId = ctx.SelfID.NodeId();
    stats.SetNodeId(nodeId);

    stats.SetEndTimeMs(TInstant::Now().MilliSeconds());
    stats.SetDurationMs(queryDuration.MilliSeconds());

    stats.SetQueryTextHash(MurmurHash<ui64>(queryText.data(), queryText.size()));
    if (queryText.size() > QUERY_TEXT_LIMIT) {
        auto limitedText = queryText.substr(0, QUERY_TEXT_LIMIT);
        stats.SetQueryText(limitedText);
    } else {
        stats.SetQueryText(queryText);
    }

    if (userSID) {
        stats.SetUserSID(userSID);
    }
    stats.SetParametersSize(parametersSize);

    TString strType;
    switch (type) {
    case NKikimrKqp::QUERY_TYPE_SQL_DML:
    case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
        strType = "data";
        break;
    case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
        strType = "scan";
        break;
    case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
    case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        strType = "script";
        break;
    default:
        break;
    }
    stats.SetType(strType);

    if (!queryStats || queryStats->GetExecutions().empty()) {
        ctx.Send(NSysView::MakeSysViewServiceID(nodeId), std::move(collectEv));
        return;
    }

    shardsCpuTime.SetMin(Max<ui64>());
    computeCpuTime.SetMin(Max<ui64>());

    auto aggregate = [] (NKikimrSysView::TStatsAggr& to, const NYql::NDqProto::TDqStatsAggr& from) {
        to.SetMin(std::min(to.GetMin(), from.GetMin()));
        to.SetMax(std::max(to.GetMax(), from.GetMax()));
        to.SetSum(to.GetSum() + from.GetSum());
        to.SetCnt(to.GetCnt() + from.GetCnt());
    };

    for (const NYql::NDqProto::TDqExecutionStats& exec : queryStats->GetExecutions()) {
        NKqpProto::TKqpExecutionExtraStats execExtra;
        if (exec.HasExtra()) {
            bool ok = exec.GetExtra().UnpackTo(&execExtra);
            Y_UNUSED(ok);
        }

        dataStats.SetPartitionCount(dataStats.GetPartitionCount() + execExtra.GetAffectedShards());

        for (auto& table : exec.GetTables()) {
            dataStats.SetReadRows(dataStats.GetReadRows() + table.GetReadRows());
            dataStats.SetReadBytes(dataStats.GetReadBytes() + table.GetReadBytes());
            dataStats.SetUpdateRows(dataStats.GetUpdateRows() + table.GetWriteRows());
            dataStats.SetUpdateBytes(dataStats.GetUpdateBytes() + table.GetWriteBytes());
            dataStats.SetDeleteRows(dataStats.GetDeleteRows() + table.GetEraseRows());
        }

        aggregate(shardsCpuTime, execExtra.GetShardsCpuTimeUs());
        aggregate(computeCpuTime, execExtra.GetComputeCpuTimeUs());
    }

    if constexpr (std::is_same_v<T, NKqpProto::TKqpStatsQuery>) {
        if (queryStats->HasCompilation()) {
            const auto& compileStats = queryStats->GetCompilation();
            stats.SetFromQueryCache(compileStats.GetFromCache());
            stats.SetCompileDurationMs(compileStats.GetDurationUs() / 1'000);
            stats.SetCompileCpuTimeUs(compileStats.GetCpuTimeUs());
        }
    } else {
        if (queryStats->Compilation) {
            const auto& compileStats = *queryStats->Compilation;
            stats.SetFromQueryCache(compileStats.FromCache);
            stats.SetCompileDurationMs(compileStats.DurationUs / 1'000);
            stats.SetCompileCpuTimeUs(compileStats.CpuTimeUs);
        }
    }

    stats.SetProcessCpuTimeUs(queryStats->GetWorkerCpuTimeUs());
    stats.SetTotalCpuTimeUs(
        stats.GetProcessCpuTimeUs() +
        stats.GetCompileCpuTimeUs() +
        stats.GetShardsCpuTimeUs().GetSum() +
        stats.GetComputeCpuTimeUs().GetSum()
    );

    stats.SetRequestUnits(requestUnits);

    ctx.Send(NSysView::MakeSysViewServiceID(nodeId), std::move(collectEv));
}

void CollectQueryStats(const TActorContext& ctx, const NKqpProto::TKqpStatsQuery* queryStats,
    TDuration queryDuration, const TString& queryText,
    const TString& userSID, ui64 parametersSize, const TString& database,
    const NKikimrKqp::EQueryType type, ui64 requestUnits)
{
    CollectQueryStatsImpl(
        ctx, queryStats, queryDuration, queryText, userSID,
        parametersSize, database, type, requestUnits);
}

void CollectQueryStats(const TActorContext& ctx, const TKqpQueryStats* queryStats,
    TDuration queryDuration, const TString& queryText,
    const TString& userSID, ui64 parametersSize, const TString& database,
    const NKikimrKqp::EQueryType type, ui64 requestUnits)
{
    CollectQueryStatsImpl(
        ctx, queryStats, queryDuration, queryText, userSID,
        parametersSize, database, type, requestUnits);
}

template <typename T>
ui64 CalcRequestUnitImpl(const T& stats) {
    TDuration totalCpuTime;
    NRuCalc::TIoReadStat totalReadStat;
    NRuCalc::TIoWriteStat totalWriteStat;

    for (const auto& exec : stats.GetExecutions()) {
        totalCpuTime += TDuration::MicroSeconds(exec.GetCpuTimeUs());

        for (auto& table : exec.GetTables()) {
            totalReadStat.Add(table);
        }
    }

    if constexpr (std::is_same_v<T, NKqpProto::TKqpStatsQuery>) {
        if (stats.HasCompilation()) {
            totalCpuTime += TDuration::MicroSeconds(stats.GetCompilation().GetCpuTimeUs());
        }
    } else {
        if (stats.Compilation) {
            totalCpuTime += TDuration::MicroSeconds(stats.Compilation->CpuTimeUs);
        }
    }

    totalCpuTime += TDuration::MicroSeconds(stats.GetWorkerCpuTimeUs());

    auto totalIoRu = totalReadStat.CalcRu() + totalWriteStat.CalcRu();

    return std::max(std::max(NRuCalc::CpuTimeToUnit(totalCpuTime), totalIoRu), (ui64)1);
}

ui64 CalcRequestUnit(const NKqpProto::TKqpStatsQuery& stats) {
    return CalcRequestUnitImpl(stats);
}

ui64 CalcRequestUnit(const TKqpQueryStats& stats) {
    return CalcRequestUnitImpl(stats);
}

NKqpProto::TKqpStatsQuery TKqpQueryStats::ToProto() const {
    NKqpProto::TKqpStatsQuery result;
    result.SetDurationUs(DurationUs);
    result.SetQueuedTimeUs(QueuedTimeUs);

    if (Compilation) {
        result.MutableCompilation()->SetFromCache(Compilation->FromCache);
        result.MutableCompilation()->SetDurationUs(Compilation->DurationUs);
        result.MutableCompilation()->SetCpuTimeUs(Compilation->CpuTimeUs);
    }

    result.SetWorkerCpuTimeUs(WorkerCpuTimeUs);
    result.SetReadSetsCount(ReadSetsCount);
    result.SetMaxShardProgramSize(MaxShardProgramSize);
    result.SetMaxShardReplySize(MaxShardReplySize);

    result.MutableExecutions()->Add(std::begin(Executions), std::end(Executions));
    return result;
}

}
}
