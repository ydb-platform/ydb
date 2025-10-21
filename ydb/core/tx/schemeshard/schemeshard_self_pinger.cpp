#include "schemeshard_self_pinger.h"

#include "schemeshard.h"  // for TEvMeasureSelfResponseTime and TEvWakeupToMeasureSelfResponseTime

#include <ydb/core/protos/counters_schemeshard.pb.h>

namespace NKikimr::NSchemeShard {

void TSelfPinger::Handle(TEvSchemeShard::TEvMeasureSelfResponseTime::TPtr &ev, const NActors::TActorContext &ctx) {
    Y_UNUSED(ev);
    TInstant now = AppData(ctx)->TimeProvider->Now();
    TDuration responseTime = now - SelfPingSentTime;
    LastResponseTime = responseTime;
    TabletCounters->Simple()[COUNTER_RESPONSE_TIME_USEC].Set(LastResponseTime.MicroSeconds());
    if (responseTime.MilliSeconds() > 1000) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Schemeshard " << TabletId << " response time is " << responseTime.MilliSeconds() << " msec");
    }
    SelfPingInFlight = false;
    if (responseTime > SELF_PING_INTERVAL) {
        DoSelfPing(ctx);
    } else {
        ScheduleSelfPingWakeup(ctx);
    }
}

void TSelfPinger::Handle(TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime::TPtr &ev, const NActors::TActorContext &ctx) {
    Y_UNUSED(ev);
    SelfPingWakeupScheduled = false;
    DoSelfPing(ctx);
}

void TSelfPinger::OnAnyEvent(const NActors::TActorContext &ctx) {
    TInstant now = AppData(ctx)->TimeProvider->Now();
    if (SelfPingInFlight) {
        TDuration responseTime = now - SelfPingSentTime;
        // Increase measured response time is ping is taking longer than then the previous one
        LastResponseTime = Max(LastResponseTime, responseTime);
        TabletCounters->Simple()[COUNTER_RESPONSE_TIME_USEC].Set(LastResponseTime.MicroSeconds());
    } else if ((now - SelfPingWakeupScheduledTime) > SELF_PING_INTERVAL) {
        DoSelfPing(ctx);
    }
}

void TSelfPinger::DoSelfPing(const NActors::TActorContext &ctx) {
    if (SelfPingInFlight)
        return;

    ctx.Send(ctx.SelfID, new TEvSchemeShard::TEvMeasureSelfResponseTime);
    SelfPingSentTime = AppData(ctx)->TimeProvider->Now();
    SelfPingInFlight = true;
}

void TSelfPinger::ScheduleSelfPingWakeup(const NActors::TActorContext &ctx) {
    if (SelfPingWakeupScheduled)
        return;

    ctx.Schedule(SELF_PING_INTERVAL, new TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime);
    SelfPingWakeupScheduled = true;
    SelfPingWakeupScheduledTime = AppData(ctx)->TimeProvider->Now();
}

}  // namespace NKikimr::NSchemeShard
