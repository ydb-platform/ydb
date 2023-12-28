#pragma once

#include "ext_counters.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>

namespace NKikimr {
namespace NSysView {

inline TActorId MakeSysViewServiceID(ui32 node) {
    const char x[12] = "SysViewSvc!";
    return TActorId(node, TStringBuf(x, 12));
}

void CollectQueryStats(const TActorContext& ctx, const NKqpProto::TKqpStatsQuery* queryStats,
    TDuration queryDuration, const TString& queryText,
    const TString& userSID, ui64 parametersSize, const TString& database,
    const NKikimrKqp::EQueryType type, ui64 requestUnits);

THolder<NActors::IActor> CreateSysViewService(
    TExtCountersConfig&& config, bool hasExternalCounters);

THolder<NActors::IActor> CreateSysViewServiceForTests();

} // NSysView
} // NKikimr
