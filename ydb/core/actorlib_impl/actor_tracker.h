#pragma once

#include "defs.h"
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <util/generic/set.h>

namespace NActors {

    // TEvTrackActor is sent by child actor in Register[Local]Subactor to an actor tracker to notify it about new
    // tracked subactor in pool.
    struct TEvTrackActor : TEventLocal<TEvTrackActor, TEvents::TSystem::TrackActor> {
        const TActorId NewSubactorId;

        TEvTrackActor(const TActorId& newSubactorId)
            : NewSubactorId(newSubactorId)
        {}
    };

    struct TEvUntrackActor : TEventLocal<TEvUntrackActor, TEvents::TSystem::UntrackActor>
    {};

    class TTrackedActorBase;

    class TActorTracker {
        // our killer -- the one who sent TEvPoisonPill here
        TActorId KillerActorId;

        // a set of registered child actors we are tracking
        TSet<TActorId> RegisteredActors;

        // number of in flight TEvTrackActor messages coming to this->ActorContext()->CurrentContext()r, but not yet processed; high bit indicates if
        // we are stopping and can't register new actors
        TAtomic NumInFlightTracks = 0;

        // actor id for this tracker
        TActorId ActorId;

        TActorContext ActorContext() const {
            return TActivationContext::ActorContextFor(ActorId);
        }

    public:
        void BindToActor(const TActorContext& ctx);

        // HandleTracking should be called from _ANY_ state function of containing actor in the following manner:
        //
        // STFUNC(...) {
        //     if (Tracker.HandleTracking(ev, ctx)) {
        //         return;
        //     }
        //     switch (ev->GetTypeRewrite()) {
        //         ...
        //     }
        // }
        //
        // Inside it handles TEvPoisonPill messages, controls RegisteredActors set, and dies when TEvPoisonPill
        // processing is finished.
        //
        // Since receiving TEvPoisonPill, the containing actor may continue to receive usual messages.
        bool HandleTracking(TAutoPtr<IEventHandle>& ev);

        // register subactor inside this tracker on a separate mailbox; should be called instead of ExecutorThread's
        // method
        TActorId RegisterSubactor(THolder<TTrackedActorBase>&& subactor, const TActorContext& ctx,
                TMailboxType::EType mailboxType = TMailboxType::Simple, ui32 poolId = Max<ui32>());

        // register subactor inside the same mailbox as of the caller
        TActorId RegisterLocalSubactor(THolder<TTrackedActorBase>&& subactor, const TActorContext& ctx);

    private:
        bool PreRegister();
        void Handle(TEvTrackActor::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvUntrackActor::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvents::TEvPoisonTaken::TPtr& ev, const TActorContext& ctx);
        void RemoveActorFromTrackList(const TActorId& subactorId, const TActorContext& ctx);
        void CheckIfPoisonPillDone(const TActorContext& ctx);

    private:
        friend class TTrackedActorBase;
        void SendUntrack(const TActorContext& ctx);
    };

    class TTrackedActorBase : public IActorCallback {
        // plain pointer to tracker; tracker always lives longer than the tracked actors, so it is safe to reference
        // it in this way; the reference is filled in when RegisterSubactor is called
        TActorTracker *Tracker = nullptr;

    protected:
        TTrackedActorBase()
            : IActorCallback(static_cast<TReceiveFunc>(&TTrackedActorBase::InitialReceiveFunc))
        {}

        // subactor registration helpers
        TActorId RegisterSubactor(THolder<TTrackedActorBase>&& subactor, const TActorContext& ctx,
                TMailboxType::EType mailboxType = TMailboxType::Simple, ui32 poolId = Max<ui32>());
        TActorId RegisterLocalSubactor(THolder<TTrackedActorBase>&& subactor, const TActorContext& ctx);

        // an override for tracked actors that also informs tracker about the death of tracked actor
        void Die(const TActorContext& ctx) override;

        // HandlePoison should be called from _ANY_ state function of derived actor in response to TEvPoisonPill event;
        // internally it calls Die() to terminate this actor
        void HandlePoison(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& /*parent*/) override;
        virtual void AfterBootstrap(const TActorContext& ctx) = 0;

    private:
        void InitialReceiveFunc(TAutoPtr<IEventHandle>& ev);

    private:
        friend class TActorTracker;
        void BindToTracker(TActorTracker *tracker);
    };

    template<typename TDerived>
    class TTrackedActorBootstrapped : public TTrackedActorBase {
        void AfterBootstrap(const TActorContext& ctx) {
            static_cast<TDerived&>(*this).Bootstrap(ctx);
        }
    };

    template<typename TDerived>
    class TTrackedActor : public TTrackedActorBase {
        using TDerivedReceiveFunc = void (TDerived::*)(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        TDerivedReceiveFunc UserReceiveFunc;

        void AfterBootstrap(const TActorContext& /*ctx*/) {
            Become(UserReceiveFunc);
        }

    public:
        TTrackedActor(TDerivedReceiveFunc userReceiveFunc)
            : UserReceiveFunc(userReceiveFunc)
        {}
    };

} // NKikimr
