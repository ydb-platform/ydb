#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/string.h>

namespace NYql {

struct TNodeIdCleanerOptions {
    TString Prefix;
    TDuration CheckPeriod = TDuration::MilliSeconds(10000);
    TDuration Timeout = TDuration::MilliSeconds(600000);
    TDuration RetryPeriod = TDuration::MilliSeconds(15000);
};

NActors::IActor* CreateNodeIdCleaner(NActors::TActorId ytWrapper, const TNodeIdCleanerOptions& options);

} // namespace NYql
