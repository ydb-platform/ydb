#include "startup_probes.h"

namespace NKikimr::NColumnShard {

LWTRACE_DEFINE_PROVIDER(YDB_CS_STARTUP);

LWTRACE_USING(YDB_CS_STARTUP);

void TraceStartupStarted(NLWTrace::TOrbit& orbit, const ui64 tabletId) {
    LWTRACK(StartupStarted, orbit, tabletId);
}

void TraceStartupStageFinished(NLWTrace::TOrbit& orbit, const ui64 tabletId, const TString& stage, const TDuration duration) {
    LWTRACK(StartupStageFinished, orbit, tabletId, stage, duration);
}

void TraceStartupTxStageFinished(NLWTrace::TOrbit& orbit, const ui64 tabletId, const TString& stage, const TDuration duration,
    const TDuration execution, const ui32 attempts) {
    LWTRACK(StartupTxStageFinished, orbit, tabletId, stage, duration, execution, duration - execution, attempts);
}

}   // namespace NKikimr::NColumnShard
