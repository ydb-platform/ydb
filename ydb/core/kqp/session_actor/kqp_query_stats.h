#pragma  once

#include <ydb/core/kqp/common/compilation/result.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/protos/kqp_stats.pb.h>

namespace NKikimr::NKqp {

struct TKqpQueryStats {
    ui64 DurationUs = 0;
    ui64 QueuedTimeUs = 0;
    std::optional<TKqpStatsCompile> Compilation;

    ui64 WorkerCpuTimeUs = 0;
    ui64 ReadSetsCount = 0;
    ui64 MaxShardProgramSize = 0;
    ui64 MaxShardReplySize = 0;

    TVector<NYql::NDqProto::TDqExecutionStats> Executions;

    const TVector<NYql::NDqProto::TDqExecutionStats>& GetExecutions() const;
    ui64 GetWorkerCpuTimeUs() const;

    NKqpProto::TKqpStatsQuery ToProto() const;
};


void CollectQueryStats(const TActorContext& ctx, const NKqpProto::TKqpStatsQuery* queryStats,
    TDuration queryDuration, const TString& queryText,
    const TString& userSID, ui64 parametersSize, const TString& database,
    const NKikimrKqp::EQueryType type, ui64 requestUnits);

void CollectQueryStats(const TActorContext& ctx, const TKqpQueryStats* queryStats,
    TDuration queryDuration, const TString& queryText,
    const TString& userSID, ui64 parametersSize, const TString& database,
    const NKikimrKqp::EQueryType type, ui64 requestUnits);

ui64 CalcRequestUnit(const NKqpProto::TKqpStatsQuery& stats);
ui64 CalcRequestUnit(const TKqpQueryStats& stats);

}
