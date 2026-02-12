#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NRetroTracing {

NActors::IActor* CreateRetroCollector();

inline NActors::TActorId MakeRetroCollectorId() {
    return NActors::TActorId(0, TStringBuf("RetroCollect", 12));
}

void DemandTrace(const NWilson::TTraceId& traceId);

// TODO: hide behind friend classes
void DemandAllTraces();

} // namespace NRetroTracing
