#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NConveyorComposite {

#define YDB_CONVEYOR_COMPOSITE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(NewTask, \
        GROUPS("ConveyorCompositeRequest"), \
        TYPES(TString, TString, ui64, TDuration), \
        NAMES("conveyorName", "category", "internalProcessId", "elapsedMs")) \
    PROBE(TaskProcessedResult, \
        GROUPS("ConveyorCompositeRequest"), \
        TYPES(TString, TString, TString, ui64, TDuration), \
        NAMES("conveyorName", "category", "scope", "internalProcessId", "elapsedMs")) \
    PROBE(RegisterProcess, \
        GROUPS("ConveyorCompositeRequest"), \
        TYPES(TString, TString, TString, ui64), \
        NAMES("conveyorName", "category", "scope", "internalProcessId")) \
    PROBE(UnregisterProcess, \
        GROUPS("ConveyorCompositeRequest"), \
        TYPES(TString, TString, ui64), \
        NAMES("conveyorName", "category", "internalProcessId")) \

LWTRACE_DECLARE_PROVIDER(YDB_CONVEYOR_COMPOSITE_PROVIDER)

}