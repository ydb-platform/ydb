#pragma once

#include "defs.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {
namespace NSchemeBoard {

struct TTestConfig {
    ::NMonitoring::TDynamicCounterPtr Counters;

    ui64 Dirs;
    ui64 ObjectsPerDir;
    ui32 SubscriberMulti;
    // modifications of existent objects
    ui32 InFlightModifications;
    // creations/deletions
    ui32 InFlightChanges;

    explicit TTestConfig(::NMonitoring::TDynamicCounterPtr counters)
        : Counters(counters)
        , Dirs(10)
        , ObjectsPerDir(10000)
        , SubscriberMulti(1)
        , InFlightModifications(100)
        , InFlightChanges(100)
    {
    }

}; // TTestConfig

} // NSchemeBoard

IActor* CreateSchemeBoardLoadProducer(ui64 owner, const NSchemeBoard::TTestConfig& config);
IActor* CreateSchemeBoardLoadConsumer(ui64 owner, const NSchemeBoard::TTestConfig& config);

} // NKikimr
