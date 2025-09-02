#pragma once

#include "schemeshard_identificators.h"  // for TTabletId

#include <ydb/core/tablet/tablet_counters.h>  // for TTabletCountersBase

#include <ydb/library/actors/core/event.h>  // for TEventHandle

#include <util/datetime/base.h>
#include <util/generic/ptr.h>


namespace NActors {
    struct TActorContext;
}

namespace NKikimr::NSchemeShard::TEvSchemeShard {
    struct TEvMeasureSelfResponseTime;
    using TEvMeasureSelfResponseTime__HandlePtr = TAutoPtr<NActors::TEventHandle<TEvMeasureSelfResponseTime>>;

    struct TEvWakeupToMeasureSelfResponseTime;
    using TEvWakeupToMeasureSelfResponseTime__HandlePtr = TAutoPtr<NActors::TEventHandle<TEvWakeupToMeasureSelfResponseTime>>;
}

namespace NKikimr::NSchemeShard {

class TSelfPinger {
private:
    static constexpr TDuration SELF_PING_INTERVAL = TDuration::MilliSeconds(1000);

public:
    TSelfPinger(TTabletId id, TTabletCountersBase* counters)
        : TabletId(id)
        , TabletCounters(counters)
        , SelfPingInFlight(false)
        , SelfPingWakeupScheduled(false)
    {}

    void Handle(TEvSchemeShard::TEvMeasureSelfResponseTime__HandlePtr &ev, const TActorContext &ctx);
    void Handle(TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime__HandlePtr &ev, const TActorContext &ctx);
    void OnAnyEvent(const TActorContext &ctx);
    void DoSelfPing(const TActorContext &ctx);
    void ScheduleSelfPingWakeup(const TActorContext &ctx);

private:
    const TTabletId TabletId;
    TTabletCountersBase * const TabletCounters;

    TDuration LastResponseTime;
    TInstant SelfPingSentTime;
    bool SelfPingInFlight;
    TInstant SelfPingWakeupScheduledTime;
    bool SelfPingWakeupScheduled;
};

}  // namespace NKikimr::NSchemeShard
