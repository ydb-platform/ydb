#include "tablet_responsiveness_pinger.h"
#include <ydb/core/base/appdata.h>
#include <library/cpp/time_provider/time_provider.h>


namespace NKikimr {

TTabletResponsivenessPinger::TTabletResponsivenessPinger(TTabletSimpleCounter &counter, TDuration pingInterval)
    : Counter(counter)
    , PingInterval(pingInterval)
    , LastResponseTime(TDuration::Zero())
    , SelfPingSentTime(TInstant::Zero())
{}

void TTabletResponsivenessPinger::Bootstrap(const TActorContext &ctx) {
    SelfPingSentTime = TInstant::Zero();
    Become(&TThis::StateWait, ctx, PingInterval, new TEvents::TEvWakeup());
}

void TTabletResponsivenessPinger::OnAnyEvent() {
    if (SelfPingSentTime) {
        const TInstant now = AppData()->TimeProvider->Now();
        const TDuration responseTime = now - SelfPingSentTime;
        LastResponseTime = Max(LastResponseTime, responseTime);
        Counter.Set(LastResponseTime.MicroSeconds());
    }
}

void TTabletResponsivenessPinger::Detach(const TActorContext &ctx) {
    Die(ctx);
}

void TTabletResponsivenessPinger::DoPing(const TActorContext &ctx) {
    ctx.Send(ctx.SelfID, new TEvents::TEvPing());
    SelfPingSentTime = AppData(ctx)->TimeProvider->Now();
    Become(&TThis::StatePing);
}

void TTabletResponsivenessPinger::ReceivePing(const TActorContext &ctx) {
    const TInstant now = AppData(ctx)->TimeProvider->Now();
    const TDuration responseTime = now - SelfPingSentTime;
    LastResponseTime = responseTime;
    Counter.Set(LastResponseTime.MicroSeconds());
    if (responseTime > PingInterval)
        DoPing(ctx);
    else
        Bootstrap(ctx);
}

STFUNC(TTabletResponsivenessPinger::StateWait) {
    switch (ev->GetTypeRewrite()) {
        CFunc(TEvents::TEvWakeup::EventType, DoPing);
    }
}

STFUNC(TTabletResponsivenessPinger::StatePing) {
    switch (ev->GetTypeRewrite()) {
        CFunc(TEvents::TEvPing::EventType, ReceivePing);
    }
}

}
