#pragma  once

#include <ydb/core/kqp/common/simple/query_stats.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NKqp {



void CollectQueryStats(const TActorContext& ctx, const NKqpProto::TKqpStatsQuery* queryStats,
    TDuration queryDuration, const TString& queryText,
    const TString& userSID, ui64 parametersSize, const TString& database,
    const NKikimrKqp::EQueryType type, ui64 requestUnits, const TString& traceId);

void CollectQueryStats(const TActorContext& ctx, const TKqpQueryStats* queryStats,
    TDuration queryDuration, const TString& queryText,
    const TString& userSID, ui64 parametersSize, const TString& database,
    const NKikimrKqp::EQueryType type, ui64 requestUnits, const TString& traceId);

void SendVictimStats(const TActorContext& ctx, ui64 locksBrokenAsVictim,
    const TString& victimQueryText, const TString& database);

ui64 CalcRequestUnit(const NKqpProto::TKqpStatsQuery& stats);
ui64 CalcRequestUnit(const TKqpQueryStats& stats);

}
