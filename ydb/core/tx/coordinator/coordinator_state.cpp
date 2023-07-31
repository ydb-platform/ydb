#include "coordinator_state.h"
#include "coordinator_impl.h"

#include <library/cpp/actors/core/interconnect.h>

namespace NKikimr::NFlatTxCoordinator {

TCoordinatorStateActor::TCoordinatorStateActor(TTxCoordinator* owner, const TActorId& prevStateActorId)
    : TActor(&TThis::StateWork)
    , Owner(owner)
    , PrevStateActorId(prevStateActorId)
{}

TCoordinatorStateActor::~TCoordinatorStateActor() {
    if (Owner) {
        Owner->DetachStateActor();
        Owner = nullptr;
    }
}

void TCoordinatorStateActor::ConfirmPersistent() {
    if (PrevStateActorId) {
        Send(PrevStateActorId, new TEvents::TEvPoison);
        PrevStateActorId = {};
    }
}

void TCoordinatorStateActor::OnTabletDestroyed() {
    Y_VERIFY(Owner, "Unexpected OnTabletDestroyed from detached tablet");
    PreserveState();
    Owner = nullptr;
}

void TCoordinatorStateActor::OnTabletDead() {
    Y_VERIFY(Owner, "Unexpected OnTabletDead from detached tablet");
    PreserveState();
    Owner = nullptr;

    TInstant deadline = TInstant::MilliSeconds(LastBlockedStep + 1);
    Schedule(deadline, new TEvents::TEvPoison);
}

void TCoordinatorStateActor::PreserveState() {
    Y_VERIFY(Owner);
    LastSentStep = Owner->VolatileState.LastSentStep;
    LastAcquiredStep = Owner->VolatileState.LastAcquired;
    LastConfirmedStep = Owner->VolatileState.LastConfirmedStep;
    LastBlockedStep = Max(
        Owner->VolatileState.LastBlockedPending,
        Owner->VolatileState.LastBlockedCommitted);
}

STFUNC(TCoordinatorStateActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvTxCoordinator::TEvCoordinatorStateRequest, Handle);
        sFunc(TEvents::TEvPoison, HandlePoison);
    }
}

void TCoordinatorStateActor::Handle(TEvTxCoordinator::TEvCoordinatorStateRequest::TPtr& ev) {
    // We could receive this request only when newer generation found us in persistent storage
    ConfirmPersistent();

    if (Owner) {
        // New generation may request state before current tablet dies
        PreserveState();
    }

    auto res = std::make_unique<TEvTxCoordinator::TEvCoordinatorStateResponse>();
    res->Record.SetLastSentStep(LastSentStep);
    res->Record.SetLastAcquiredStep(LastAcquiredStep);
    res->Record.SetLastConfirmedStep(LastConfirmedStep);
    Send(ev->Sender, res.release());
}

void TCoordinatorStateActor::HandlePoison() {
    if (Owner) {
        // Note: we may have been killed by a newer generation before tablet died
        Owner->DetachStateActor();
        Owner = nullptr;
    }

    PassAway();
}

bool TTxCoordinator::StartStateActor() {
    // Note: we will start state actor at most once. When state detaches (e.g.
    // killed by newer generation), actor id signals it was previously started.
    if (!CoordinatorStateActorId) {
        CoordinatorStateActor = new TCoordinatorStateActor(this, PrevStateActorId);
        CoordinatorStateActorId = RegisterWithSameMailbox(CoordinatorStateActor);
        return true;
    }

    // Already started
    return false;
}

void TTxCoordinator::ConfirmStateActorPersistent() {
    if (CoordinatorStateActor) {
        CoordinatorStateActor->ConfirmPersistent();
    }
}

void TTxCoordinator::DetachStateActor() {
    Y_VERIFY(CoordinatorStateActor);
    CoordinatorStateActor = nullptr;
}

class TTxCoordinator::TRestoreStateActor
    : public TActorBootstrapped<TRestoreStateActor>
{
public:
    TRestoreStateActor(const TActorId& owner, ui32 generation, const TActorId& prevStateActorId, ui64 lastBlockedStep)
        : Owner(owner)
        , Generation(generation)
        , PrevStateActorId(prevStateActorId)
        , LastBlockedStep(lastBlockedStep)
        , Deadline(TInstant::MilliSeconds(LastBlockedStep + 1))
    { }

    void PassAway() override {
        if (Subscribed) {
            auto proxy = TActivationContext::InterconnectProxy(PrevStateActorId.NodeId());
            Send(proxy, new TEvents::TEvUnsubscribe);
        }
        TActorBootstrapped::PassAway();
    }

    void Bootstrap() {
        if (CheckDeadline()) {
            return;
        }

        SendRequest();
        Become(&TThis::StateWork);
    }

    void StateMissing() {
        Send(Owner, new TEvPrivate::TEvRestoredStateMissing(LastBlockedStep));
        PassAway();
    }

    bool CheckDeadline() {
        TInstant now = TAppData::TimeProvider->Now();
        if (Deadline <= now) {
            StateMissing();
            return true;
        }

        TDuration delay = Min(Deadline - now, TDuration::Seconds(1));
        Schedule(delay, new TEvents::TEvWakeup);
        return false;
    }

    void SendRequest() {
        ui32 flags = IEventHandle::FlagTrackDelivery;
        if (PrevStateActorId.NodeId() != SelfId().NodeId()) {
            flags |= IEventHandle::FlagSubscribeOnSession;
            Subscribed = true;
        }
        Send(PrevStateActorId, new TEvTxCoordinator::TEvCoordinatorStateRequest(Generation), flags);
    }

    void NodeDisconnected() {
        // We just retry without backoff
        SendRequest();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->Reason) {
            case TEvents::TEvUndelivered::ReasonActorUnknown:
                // State actor no longer exists
                StateMissing();
                return;
            default:
                // We retry on disconnect
                break;
        }
    }

    void Handle(TEvTxCoordinator::TEvCoordinatorStateResponse::TPtr& ev) {
        Send(Owner, new TEvPrivate::TEvRestoredState(std::move(ev->Get()->Record)));
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoison, PassAway);
            sFunc(TEvents::TEvWakeup, CheckDeadline);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            sFunc(TEvInterconnect::TEvNodeDisconnected, NodeDisconnected);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvTxCoordinator::TEvCoordinatorStateResponse, Handle);
        }
    }

private:
    const TActorId Owner;
    const ui32 Generation;
    const TActorId PrevStateActorId;
    const ui64 LastBlockedStep;
    const TInstant Deadline;
    bool Subscribed = false;
};

void TTxCoordinator::RestoreState(const TActorId& prevStateActorId, ui64 lastBlockedStep) {
    PrevStateActorId = prevStateActorId;
    RestoreStateActorId = RegisterWithSameMailbox(
        new TRestoreStateActor(
            SelfId(),
            Executor()->Generation(),
            prevStateActorId,
            lastBlockedStep));
}

void TTxCoordinator::Handle(TEvPrivate::TEvRestoredStateMissing::TPtr& ev) {
    RestoreStateActorId = { };

    // Assume the last blocked step was also planned and observed
    ui64 lastBlockedStep = ev->Get()->LastBlockedStep;
    if (VolatileState.LastPlanned < lastBlockedStep) {
        VolatileState.LastPlanned = lastBlockedStep;
        VolatileState.LastSentStep = lastBlockedStep;
        VolatileState.LastAcquired = lastBlockedStep;
        VolatileState.LastConfirmedStep = lastBlockedStep;
    }

    Execute(CreateTxRestoreTransactions());
}

void TTxCoordinator::Handle(TEvPrivate::TEvRestoredState::TPtr& ev) {
    RestoreStateActorId = { };

    auto& record = ev->Get()->Record;

    // We assume we cannot get inconsistent results, because newer generations
    // would either move LastPlanned beyond LastBlocked and restore will not
    // start, or they will persist LastBlocked with a new state actor.
    VolatileState.LastSentStep = record.GetLastSentStep();
    VolatileState.LastAcquired = record.GetLastAcquiredStep();
    VolatileState.LastConfirmedStep = record.GetLastConfirmedStep();

    // Last planned step is the maximum observed
    VolatileState.LastPlanned = Max(
        VolatileState.LastPlanned,
        VolatileState.LastSentStep,
        VolatileState.LastAcquired,
        VolatileState.LastConfirmedStep);

    Execute(CreateTxRestoreTransactions());
}

} // namespace NKikimr::NFlatTxCoordinator
