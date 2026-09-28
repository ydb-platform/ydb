#pragma once

#include "columnshard_private_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NColumnShard {

class TColumnShard;

// Owns the MoveData loop: its own cadence, the metadata re-arm and the response gate. TColumnShard
// only starts it and reseeds it; it does not drive the move from the tablet's periodic wakeup.
//
// Registered with RegisterWithSameMailbox, like TSpaceWatcher, so its events are serialized with the
// tablet's and it can reach tablet state through Self without a snapshot protocol. That also means
// this is not CPU offload: the heavy passes still run on the tablet's thread.
class TMoveDataDriver: public TActorBootstrapped<TMoveDataDriver> {
private:
    TColumnShard* Self;
    // A lower bound between gate checks, not a period: a poke may check sooner.
    static constexpr TDuration Cadence = TDuration::Seconds(5);

    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(Cadence, new TEvPrivate::TEvMoveDataWakeup());
    }

    void Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMoveDataReseed::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx);

public:
    explicit TMoveDataDriver(TColumnShard* self)
        : Self(self)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);
        // The vacuum leg and the first batch are started by the tablet before we are registered,
        // so the first thing we owe is a gate check on our own turn.
        ctx.Send(ctx.SelfID, new TEvPrivate::TEvMoveDataPoke());
        ScheduleWakeup(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvMoveDataWakeup, Handle);
            HFunc(TEvPrivate::TEvMoveDataReseed, Handle);
            HFunc(TEvPrivate::TEvMoveDataPoke, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                break;
        }
    }
};

}   // namespace NKikimr::NColumnShard
