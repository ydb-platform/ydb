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

THolder<NActors::IActor> CreateSysViewService(
    TExtCountersConfig&& config, bool hasExternalCounters);

THolder<NActors::IActor> CreateSysViewServiceForTests();

} // NSysView
} // NKikimr
