#include "coordinator_state.h"
#include "coordinator_impl.h"

#include <ydb/library/actors/core/interconnect.h>
#include <library/cpp/time_provider/time_provider.h>


namespace NKikimr::NFlatTxCoordinator {

static constexpr size_t MaxSerializedStateChunk = 8 * 1024 * 1024;

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
    Y_ABORT_UNLESS(Owner, "Unexpected OnTabletDestroyed from detached tablet");
    Y_ABORT_UNLESS(Owner->CoordinatorStateActor == nullptr, "OnTabletDestroyed called with state actor still attached");
    PreserveState();
    Owner = nullptr;
}

void TCoordinatorStateActor::OnTabletDead() {
    Y_ABORT_UNLESS(Owner, "Unexpected OnTabletDead from detached tablet");
    Y_ABORT_UNLESS(Owner->CoordinatorStateActor == nullptr, "OnTabletDead called with state actor still attached");
    PreserveState();
    Owner = nullptr;

    TInstant deadline = TInstant::MilliSeconds(LastBlockedStep + 1);
    Schedule(deadline, new TEvents::TEvPoison);
}

void TCoordinatorStateActor::PreserveState() {
    Y_ABORT_UNLESS(Owner);
    LastSentStep = Owner->VolatileState.LastSentStep;
    LastAcquiredStep = Owner->VolatileState.LastAcquired;
    LastConfirmedStep = Owner->VolatileState.LastConfirmedStep;
    LastBlockedStep = Max(
        Owner->VolatileState.LastBlockedPending,
        Owner->VolatileState.LastBlockedCommitted);

    if (!Owner->VolatileTransactions.empty()) {
        google::protobuf::Arena arena;
        auto* state = google::protobuf::Arena::CreateMessage<NKikimrTxCoordinator::TEvCoordinatorStateResponse::TSerializedState>(&arena);
        auto* pTxs = state->MutableVolatileTxs();
        pTxs->Reserve(Owner->VolatileTransactions.size());
        for (auto& tx : Owner->VolatileTransactions) {
            auto* pTx = pTxs->Add();
            pTx->SetTxId(tx.first);
            pTx->SetPlanStep(tx.second.PlanOnStep);
            auto* pMediators = pTx->MutableMediators();
            pMediators->Reserve(tx.second.UnconfirmedAffectedSet.size());
            for (auto& m : tx.second.UnconfirmedAffectedSet) {
                auto* pMediator = pMediators->Add();
                pMediator->SetMediatorId(m.first);
                auto* pAffectedSet = pMediator->MutableAffectedSet();
                pAffectedSet->Reserve(m.second.size());
                for (ui64 tabletId : m.second) {
                    pAffectedSet->Add(tabletId);
                }
            }
        }
        bool ok = state->SerializeToString(&SerializedState);
        Y_ABORT_UNLESS(ok);
    }

    Owner->VolatileState.Preserved = true;
}

STFUNC(TCoordinatorStateActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvTxCoordinator::TEvCoordinatorStateRequest, Handle);
        sFunc(TEvents::TEvPoison, HandlePoison);
    }
}

void TCoordinatorStateActor::Handle(TEvTxCoordinator::TEvCoordinatorStateRequest::TPtr& ev) {
    auto* msg = ev->Get();

    // We could receive this request only when newer generation found us in persistent storage
    ConfirmPersistent();

    if (Owner) {
        // New generation may request state before current tablet dies
        Owner->DetachStateActor();
        OnTabletDead();
    }

    auto res = std::make_unique<TEvTxCoordinator::TEvCoordinatorStateResponse>();
    if (!msg->Record.HasContinuationToken()) {
        res->Record.SetLastSentStep(LastSentStep);
        res->Record.SetLastAcquiredStep(LastAcquiredStep);
        res->Record.SetLastConfirmedStep(LastConfirmedStep);
    }

    if (!SerializedState.empty()) {
        NKikimrTxCoordinator::TEvCoordinatorStateResponse::TContinuationToken token;
        if (msg->Record.HasContinuationToken()) {
            bool ok = token.ParseFromString(msg->Record.GetContinuationToken());
            Y_DEBUG_ABORT_UNLESS(ok);
        }
        size_t offset = token.GetOffset();
        if (offset < SerializedState.size()) {
            size_t left = SerializedState.size() - offset;
            if (offset == 0 && left <= MaxSerializedStateChunk) {
                res->Record.SetSerializedState(SerializedState);
            } else if (left <= MaxSerializedStateChunk) {
                res->Record.SetSerializedState(SerializedState.substr(offset));
            } else {
                res->Record.SetSerializedState(SerializedState.substr(offset, MaxSerializedStateChunk));
                token.SetOffset(offset + MaxSerializedStateChunk);
                bool ok = token.SerializeToString(res->Record.MutableContinuationToken());
                Y_DEBUG_ABORT_UNLESS(ok);
            }
        }
    }

    Send(ev->Sender, res.release(), 0, ev->Cookie);
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
    Y_ABORT_UNLESS(CoordinatorStateActor);
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
        if (Cookie == 0) {
            Send(Owner, new TEvPrivate::TEvRestoredStateMissing(LastBlockedStep));
        } else {
            // Use partial state from the first response
            Response.ClearSerializedState();
            Send(Owner, new TEvPrivate::TEvRestoredState(std::move(Response)));
        }
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
        Send(PrevStateActorId,
            new TEvTxCoordinator::TEvCoordinatorStateRequest(Generation, ContinuationToken),
            flags, Cookie);
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
        if (ev->Cookie != Cookie) {
            // This is not the response we are waiting for
            return;
        }

        ContinuationToken = ev->Get()->Record.GetContinuationToken();

        if (Cookie == 0) {
            Response = std::move(ev->Get()->Record);
            Response.ClearContinuationToken();
        } else {
            Response.MutableSerializedState()->append(ev->Get()->Record.GetSerializedState());
        }

        // Request the next chunk if we have continuation token
        if (!ContinuationToken.empty()) {
            ++Cookie;
            SendRequest();
            return;
        }

        Send(Owner, new TEvPrivate::TEvRestoredState(std::move(Response)));
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
    TString ContinuationToken;
    ui64 Cookie = 0;
    NKikimrTxCoordinator::TEvCoordinatorStateResponse Response;
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

    if (record.HasSerializedState()) {
        google::protobuf::Arena arena;
        auto* state = google::protobuf::Arena::CreateMessage<NKikimrTxCoordinator::TEvCoordinatorStateResponse::TSerializedState>(&arena);
        bool ok = state->ParseFromString(record.GetSerializedState());
        if (ok) {
            for (auto& protoTx : state->GetVolatileTxs()) {
                auto& tx = VolatileTransactions[protoTx.GetTxId()];
                tx.PlanOnStep = protoTx.GetPlanStep();
                for (auto& protoMediator : protoTx.GetMediators()) {
                    auto& unconfirmed = tx.UnconfirmedAffectedSet[protoMediator.GetMediatorId()];
                    unconfirmed.reserve(protoMediator.AffectedSetSize());
                    for (ui64 tabletId : protoMediator.GetAffectedSet()) {
                        unconfirmed.insert(tabletId);
                    }
                }
            }
        }
    }

    Execute(CreateTxRestoreTransactions());
}

} // namespace NKikimr::NFlatTxCoordinator
