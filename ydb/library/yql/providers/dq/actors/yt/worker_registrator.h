#pragma once

#include <library/cpp/actors/core/actor.h>

#include <util/datetime/base.h>

namespace NYql
{

struct TWorkerRegistratorOptions {
    TString Prefix;
    TString NodeName;

    TDuration PingPeriod = TDuration::MilliSeconds(5000);
    TDuration RetryPeriod = TDuration::MilliSeconds(5000);
};

NActors::IActor* CreateWorkerRegistrator(NActors::TActorId ytWrapper, const TWorkerRegistratorOptions& options);

} // namespace NYql

