#pragma once

#include <library/cpp/lwtrace/all.h>
#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimr::NColumnShard {

#define YDB_CS_STARTUP(PROBE, EVENT, GROUPS, TYPES, NAMES)                                                                    \
    PROBE(StartupStarted, GROUPS("Orbit", "Startup"), TYPES(ui64), NAMES("tabletId"))                                         \
    PROBE(StartupStageFinished, GROUPS("Startup"), TYPES(ui64, TString, TDuration), NAMES("tabletId", "stage", "durationMs")) \
    PROBE(StartupTxStageFinished, GROUPS("Startup"), TYPES(ui64, TString, TDuration, TDuration, TDuration, ui32),             \
        NAMES("tabletId", "stage", "durationMs", "executionMs", "waitingMs", "attempts"))

LWTRACE_DECLARE_PROVIDER(YDB_CS_STARTUP)

void TraceStartupStarted(NLWTrace::TOrbit& orbit, const ui64 tabletId);

void TraceStartupStageFinished(NLWTrace::TOrbit& orbit, const ui64 tabletId, const TString& stage, const TDuration duration);

void TraceStartupTxStageFinished(NLWTrace::TOrbit& orbit, const ui64 tabletId, const TString& stage, const TDuration duration,
    const TDuration execution, const ui32 attempts);

}   // namespace NKikimr::NColumnShard
