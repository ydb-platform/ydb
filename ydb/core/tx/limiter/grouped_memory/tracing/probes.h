#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

#define YDB_GROUPED_MEMORY_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(StartTask, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64, ui64, ui32, ui64, ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "externalScopeId", "externalGroupId", "stageFeaturesIdx", "allocationId", "memory", "allocationsSize", "load")) \
    PROBE(FinishTask, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "externalScopeId", "allocationId", "load")) \
    PROBE(TaskUpdated, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "externalScopeId", "allocationId", "load")) \
    PROBE(FinishGroup, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "externalScopeId", "externalGroupId", "load")) \
    PROBE(StartGroup, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "externalScopeId", "externalGroupId", "load")) \
    PROBE(FinishProcess, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "load")) \
    PROBE(StartProcess, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "load")) \
    PROBE(FinishProcessScope, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "externalScopeId", "load")) \
    PROBE(StartProcessScope, \
        GROUPS("GroupedMemoryManagerRequest"), \
        TYPES(ui64, ui64, ui64, ui64), \
        NAMES("managerId", "externalProcessId", "externalScopeId", "load")) \
    PROBE(Allocated, \
        GROUPS("GroupedMemoryManagerResponse"), \
        TYPES(TString, ui64, TString, ui64, ui64, ui64, ui64, TDuration, bool, bool), \
        NAMES("type", "allocationId", "stage",  "limit", "hardLimit", "usage", "waiting", "elapsed", "force", "success")) \

LWTRACE_DECLARE_PROVIDER(YDB_GROUPED_MEMORY_PROVIDER)

}