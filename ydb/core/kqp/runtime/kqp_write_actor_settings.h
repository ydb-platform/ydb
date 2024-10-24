#pragma once

#include <util/generic/ptr.h>
#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NKqp {

struct TWriteActorSettings : TAtomicRefCount<TWriteActorSettings> {
    i64 InFlightMemoryLimitPerActorBytes = 64_MB;
    i64 MemoryLimitPerMessageBytes = 64_MB;
    i64 MaxBatchesPerMessage = 1000;

    TDuration StartRetryDelay = TDuration::Seconds(1);
    TDuration MaxRetryDelay = TDuration::Seconds(10);
    double UnsertaintyRatio = 0.5;
    double Multiplier = 2.0;

    ui64 MaxWriteAttempts = 100;
    ui64 MaxResolveAttempts = 5;
};

TWriteActorSettings GetWriteActorSettings();
void SetWriteActorSettings(TIntrusivePtr<TWriteActorSettings> ptr);

}
}
