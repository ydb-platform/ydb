#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/retro_tracing/retro_span_base.h>

namespace NRetro {

inline NActors::TActorId MakeRetroUploaderId() {
    return NActors::TActorId(0, TStringBuf("retro_upload", 12));
}

void DemandTrace(TTraceId traceId);
 
NActors::IActor* CreateRetroUploader(TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

} // namespace NRetro