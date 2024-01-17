#pragma once
#include <ydb/library/actors/core/actor.h>

namespace NActors {

TActorId CreateLongTimer(const TActorContext &ctx, TDuration delta, TAutoPtr<IEventHandle> ev, ui32 poolId = 0, ISchedulerCookie *cookie = nullptr);

// uses TlsActivationContext, note that by default we use current pool
TActorId CreateLongTimer(TDuration delta, TAutoPtr<IEventHandle> ev, ui32 poolId = Max<ui32>(), ISchedulerCookie *cookie = nullptr);

}
