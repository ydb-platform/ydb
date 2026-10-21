#include "startup_probes.h"

namespace NKikimr::NColumnShard {

LWTRACE_DEFINE_PROVIDER(YDB_CS_STARTUP);

LWTRACE_USING(YDB_CS_STARTUP);

void TraceStartupStageFinished(const ui64 tabletId, const TString& stage, const TDuration duration) {
    LWPROBE(StartupStageFinished, tabletId, stage, duration);
}

void TraceStartupTxStageFinished(
    const ui64 tabletId, const TString& stage, const TDuration duration, const TDuration execution, const ui32 attempts) {
    LWPROBE(StartupTxStageFinished, tabletId, stage, duration, execution, duration - execution, attempts);
}

}   // namespace NKikimr::NColumnShard
