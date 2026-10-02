#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NConveyor {

#define YDB_CONVEYOR_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(NewTask, \
        GROUPS("ConveyorRequest"), \
        TYPES(TString, ui64, TString, ui64), \
        NAMES("conveyorName", "processId", "taskClass", "queueSize")) \
    PROBE(TaskProcessedResult, \
        GROUPS("ConveyorRequest"), \
        TYPES(TString, ui64, TDuration, ui64, ui64), \
        NAMES("conveyorName", "processId", "elapsedMs", "queueSize", "requestSize")) \
    PROBE(RegisterProcess, \
        GROUPS("ConveyorRequest"), \
        TYPES(TString, ui64, ui64), \
        NAMES("conveyorName", "processId", "queueSize")) \
    PROBE(UnregisterProcess, \
        GROUPS("ConveyorRequest"), \
        TYPES(TString, ui64, ui64), \
        NAMES("conveyorName", "processId", "queueSize")) \

LWTRACE_DECLARE_PROVIDER(YDB_CONVEYOR_PROVIDER)

}