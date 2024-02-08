#include "actor_tracker.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NActors {

    static constexpr TAtomicBase StopBit = TAtomicBase(1) << (sizeof(TAtomicBase) * CHAR_BIT - 1);

    void TActorTracker::BindToActor(const TActorContext& ctx) {
        ActorId = ctx.SelfID;
    }

    void TActorTracker::Handle(TEvTrackActor::TPtr& ev, const TActorContext& ctx) {
        // insert newly created actor into registered actors set and ensure it is not duplicate one
        const TActorId& newSubactorId = ev->Get()->NewSubactorId;
        const bool inserted = RegisteredActors.insert(newSubactorId).second;
        Y_ABORT_UNLESS(inserted);

        // check if we are not dying now; we can do this in non-atomic way, because this bit can be set only in
        // PoisonPill handler and this can'be done concurrently
        TAtomicBase status = AtomicDecrement(NumInFlightTracks);
        if (status & StopBit) {
            // we're stopping, so we have to issue TEvPoisonPill for newly created actor; we do it in safe manner --
            // request tracking, because that actor might die before processing this message
            ctx.Send(newSubactorId, new TEvents::TEvPoisonPill, IEventHandle::MakeFlags(0, IEventHandle::FlagTrackDelivery));
        }
    }

    void TActorTracker::Handle(TEvUntrackActor::TPtr& ev, const TActorContext& ctx) {
        // just remove actor from the list and do nothing more
        RemoveActorFromTrackList(ev->Sender, ctx);
    }

    void TActorTracker::Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        // ensure we haven't received PoisonPill yet, then remember the id of actor who requested the poison
        Y_ABORT_UNLESS(!KillerActorId);
        KillerActorId = ev->Sender;

        // set StopBit in thread-safe manner
        TAtomicBase newValue = AtomicAdd(NumInFlightTracks, StopBit);
        Y_ABORT_UNLESS(newValue & StopBit);

        // propagate TEvPoisonPill to all currently registered actors; request delivery tracking as each actor may die
        // before processing TEvPosionPill message
        for (const TActorId& subactorId : RegisteredActors) {
            ctx.Send(subactorId, new TEvents::TEvPoisonPill, IEventHandle::MakeFlags(0, IEventHandle::FlagTrackDelivery));
        }

        // check if we can respond to killer
        CheckIfPoisonPillDone(ctx);
    }

    void TActorTracker::Handle(TEvents::TEvPoisonTaken::TPtr& ev, const TActorContext& ctx) {
        // we have received response from child actor to TEvPoisonPill -- remove actor from list and check if the
        // TEvPoisonPill processing done
        RemoveActorFromTrackList(ev->Sender, ctx);
    }

    void TActorTracker::RemoveActorFromTrackList(const TActorId& subactorId, const TActorContext& ctx) {
        // erase subactor from set and check if we have finished processing PoisonPill (if we are processing it)
        const ui32 numErased = RegisteredActors.erase(subactorId);
        Y_ABORT_UNLESS(numErased == 1);
        if (KillerActorId) {
            CheckIfPoisonPillDone(ctx);
        }
    }

    void TActorTracker::CheckIfPoisonPillDone(const TActorContext& ctx) {
        // ensure we have killing actor
        Y_ABORT_UNLESS(KillerActorId);

        // ensure we have stop bit set
        TAtomicBase currentValue = AtomicGet(NumInFlightTracks);
        Y_ABORT_UNLESS(currentValue & StopBit);

        // check if we have no more registered actors left and no more TEvTrackActor queries in flight -- in this case
        // we have finished processing the query; send TEvPoisonTaken message and Die
        if (currentValue == StopBit && RegisteredActors.empty()) {
            ctx.Send(KillerActorId, new TEvents::TEvPoisonTaken);
            ctx.ExecutorThread.UnregisterActor(&ctx.Mailbox, ctx.SelfID);
        }
    }

    bool TActorTracker::HandleTracking(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTrackActor, Handle);
            HFunc(TEvUntrackActor, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);
            HFunc(TEvents::TEvPoisonTaken, Handle);

            default:
                return false;
        }

        return true;
    }

    bool TActorTracker::PreRegister() {
        // check if we are stopping; in this case we return false meaning that no actor should be registered and the new
        // actor is simply destroyed
        TAtomicBase currentValue, newValue;
        do {
            currentValue = AtomicGet(NumInFlightTracks);
            if (currentValue & StopBit) {
                return false; // no actor going to be registered -- we're dying; good reaction is to call Die() too in caller code
            }
            newValue = currentValue + 1; // simply add one to current in flight TEvTrackActor message count
        } while (!AtomicCas(&NumInFlightTracks, newValue, currentValue));

        return true;
    }

    TActorId TActorTracker::RegisterSubactor(THolder<TTrackedActorBase>&& subactor, const TActorContext& ctx,
            TMailboxType::EType mailboxType, ui32 poolId) {
        if (!PreRegister()) {
            return TActorId();
        }

        // bind new actor to this tracker
        subactor->BindToTracker(this);

        // we create new actor and register it in pool now
        TActorId subactorId = ctx.Register(subactor.Release(), mailboxType, poolId);

        // send TEvTrackActor message to tracker actor
        ctx.Send(ActorId, new TEvTrackActor(subactorId));

        return subactorId;
    }

    TActorId TActorTracker::RegisterLocalSubactor(THolder<TTrackedActorBase>&& subactor, const TActorContext& ctx) {
        if (!PreRegister()) {
            return TActorId();
        }

        // bind new actor to this tracker
        subactor->BindToTracker(this);

        // we create new actor and register it in local pool
        TActorId subactorId = ctx.RegisterWithSameMailbox(subactor.Release());

        // send TEvTrackActor message to tracker actor
        Y_ABORT_UNLESS(ActorId);
        ctx.Send(ActorId, new TEvTrackActor(subactorId));

        return subactorId;
    }

    void TActorTracker::SendUntrack(const TActorContext& ctx) {
        Y_ABORT_UNLESS(ActorId);
        ctx.Send(ActorId, new TEvUntrackActor);
    }



    TActorId TTrackedActorBase::RegisterSubactor(THolder<TTrackedActorBase>&& subactor, const TActorContext& ctx,
            TMailboxType::EType mailboxType, ui32 poolId) {
        Y_ABORT_UNLESS(Tracker);
        return Tracker->RegisterSubactor(std::move(subactor), ctx, mailboxType, poolId);
    }

    TActorId TTrackedActorBase::RegisterLocalSubactor(THolder<TTrackedActorBase>&& subactor, const TActorContext& ctx) {
        Y_ABORT_UNLESS(Tracker);
        return Tracker->RegisterLocalSubactor(std::move(subactor), ctx);
    }

    void TTrackedActorBase::Die(const TActorContext& ctx) {
        // notify tracker
        Tracker->SendUntrack(ctx);
        // call default implementation to unregister actor from mailbox
        IActor::Die(ctx);
    }

    void TTrackedActorBase::HandlePoison(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        // notify sender
        ctx.Send(ev->Sender, new TEvents::TEvPoisonTaken);
        // unregister from mailbox
        IActor::Die(ctx);
    }

    TAutoPtr<IEventHandle> TTrackedActorBase::AfterRegister(const TActorId& self, const TActorId& parent) {
        // send TEvBootstrap event locally
        return new IEventHandle(self, parent, new TEvents::TEvBootstrap);
    }

    void TTrackedActorBase::InitialReceiveFunc(TAutoPtr<IEventHandle>& ev) {
        // the first event MUST be the TEvBootstrap one
        Y_ABORT_UNLESS(ev->GetTypeRewrite() == TEvents::TEvBootstrap::EventType);
        // notify implementation
        AfterBootstrap(this->ActorContext());
    }

    void TTrackedActorBase::BindToTracker(TActorTracker *tracker) {
        Y_ABORT_UNLESS(!Tracker);
        Tracker = tracker;
    }

} // NKikimr
