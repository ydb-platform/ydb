#pragma  once

#include <ydb/core/kqp/common/compilation/result.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/protos/kqp_stats.pb.h>

namespace NKikimr::NKqp {

struct TKqpQueryStats {
    ui64 DurationUs;
    std::optional<TKqpStatsCompile> Compilation;

    ui64 WorkerCpuTimeUs;
    ui64 ReadSetsCount;
    ui64 MaxShardProgramSize;
    ui64 MaxShardReplySize;

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
