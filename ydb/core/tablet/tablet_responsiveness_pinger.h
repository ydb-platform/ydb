#pragma once
#include "defs.h"
#include "tablet_counters.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>


namespace NKikimr {

class TTabletResponsivenessPinger : public TActorBootstrapped<TTabletResponsivenessPinger> {
    TTabletSimpleCounter &Counter;
    const TDuration PingInterval;
    TDuration LastResponseTime;
    TInstant SelfPingSentTime;

    STFUNC(StateWait);
    STFUNC(StatePing);

    void DoPing(const TActorContext &ctx);
    void ReceivePing(const TActorContext &ctx);
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_RESPONSIVENESS_PINGER;
    }

    TTabletResponsivenessPinger(TTabletSimpleCounter &counter, TDuration pingInterval);

    void Bootstrap(const TActorContext &ctx);

    void OnAnyEvent();
    void Detach(const TActorContext &ctx);
};

}
