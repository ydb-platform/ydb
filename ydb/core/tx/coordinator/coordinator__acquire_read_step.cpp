#include "coordinator_impl.h"

namespace NKikimr {
namespace NFlatTxCoordinator {

static constexpr ui64 MaxAcquireReadStepInFlight = 4;

struct TTxCoordinator::TTxAcquireReadStep : public TTransactionBase<TTxCoordinator> {
    TVector<TAcquireReadStepRequest> Requests;
    TMonotonic StartTimeStamp;
    ui64 Step;

    TTxAcquireReadStep(TTxCoordinator* self, TMonotonic startTimeStamp)
        : TTransactionBase(self)
        , StartTimeStamp(startTimeStamp)
    { }

    TTxType GetTxType() const override { return TXTYPE_ACQUIRE_READ_STEP; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        Y_ABORT_UNLESS(Self->VolatileState.AcquireReadStepStarting);
        Requests.swap(Self->VolatileState.AcquireReadStepPending);
        Self->VolatileState.AcquireReadStepStarting = false;

        // This is the maximum step that we may have sent to mediators.
        // It is linearizable with all committed transactions, because this is
        // also the maximum step of any transaction in the database that may
        // have committed successfully: even if some other coordinator has
        // planned and sent a larger step value, it would not be executed until
        // all coordinators plan and broadcast that step, us included.
        Step = Max(Self->VolatileState.LastSentStep, Self->VolatileState.LastAcquired);
        Self->VolatileState.LastAcquired = Step;

        // FIXME: this is very inefficient
        // We must return the maximum step that may have been sent to mediators
        // at the start of the request. There may be a newer coordinator that
        // is running right now, and which has planned and sent transactions
        // with a newer step. By making a write transaction we ensure that we
        // committed all pending changes and that there was no newer coordinator
        // running at the start of the request. However this means we must
        // wait for an additional round trip time of this commit.
        // It would be better to introduce some kind of temporary leases here,
        // updated asynchronously, that would make sure restarted coordinators
        // would not break guarantees for older coordinators.
        NIceDb::TNiceDb db(txc.DB);
        Schema::SaveState(db, Schema::State::AcquireReadStepLast, Step);

        Self->SchedulePlanTickAligned(Step + 1);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ui64 latencyUs = (ctx.Monotonic() - StartTimeStamp).MicroSeconds();

        for (const auto& req : Requests) {
            ctx.Send(req.Sender, new TEvTxProxy::TEvAcquireReadStepResult(Self->TabletID(), Step), 0, req.Cookie);
        }

        Self->VolatileState.AcquireReadStepLatencyUs.Push(latencyUs);
        Y_ABORT_UNLESS(Self->VolatileState.AcquireReadStepInFlight > 0);
        --Self->VolatileState.AcquireReadStepInFlight;

        Self->MaybeFlushAcquireReadStep(ctx);
    }
};

void TTxCoordinator::MaybeFlushAcquireReadStep(const TActorContext& ctx) {
    if (VolatileState.AcquireReadStepFlushing || VolatileState.AcquireReadStepStarting) {
        // Another flush is already in progress, must wait for it to complete
        return;
    }

    if (VolatileState.AcquireReadStepInFlight >= MaxAcquireReadStepInFlight) {
        // There are too many transactions inflight already
        return;
    }

    if (VolatileState.AcquireReadStepPending.empty()) {
        // We have no requests, nothing to flush
        return;
    }

    TMonotonic deadline = ctx.Monotonic();
    if (VolatileState.AcquireReadStepInFlight > 0) {
        // We want to spread acquire transactions evenly using the average latency
        deadline = VolatileState.AcquireReadStepLast + TDuration::MicroSeconds(VolatileState.AcquireReadStepLatencyUs.GetValue() / MaxAcquireReadStepInFlight);
    }

    VolatileState.AcquireReadStepFlushing = true;
    ctx.Schedule(deadline, new TEvPrivate::TEvAcquireReadStepFlush);
}

void TTxCoordinator::Handle(TEvTxProxy::TEvAcquireReadStep::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID()
        << " HANDLE TEvAcquireReadStep");

    IncCounter(COUNTER_REQ_ACQUIRE_READ_STEP);

    if (Y_UNLIKELY(Stopping)) {
        // We won't be able to commit anyway
        return;
    }

    // Note: when volatile state is preserved we don't want to update the last
    // acquired step, because the new generation might miss that and invariants
    // not read-step not going back would be violated. Run the code below using
    // the normal tx, which will almost certainly fail (the storage is supposed
    // to be blocked already), or successfully persist the new read step.
    if (ReadOnlyLeaseEnabled() && !VolatileState.Preserved) {
        // We acquire read step using a read-only lease from executor
        // It is guaranteed that any future generation was not running at
        // the time ConfirmReadOnlyLease was called.
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;
        ui64 step = Max(VolatileState.LastSentStep, VolatileState.LastAcquired);
        VolatileState.LastAcquired = step;
        Executor()->ConfirmReadOnlyLease([this, sender, cookie, step]() {
            Send(sender, new TEvTxProxy::TEvAcquireReadStepResult(TabletID(), step), 0, cookie);
        });
        SchedulePlanTickAligned(step + 1);
        return;
    }

    VolatileState.AcquireReadStepPending.emplace_back(ev->Sender, ev->Cookie);
    MaybeFlushAcquireReadStep(ctx);
}

void TTxCoordinator::Handle(TEvPrivate::TEvAcquireReadStepFlush::TPtr&, const TActorContext& ctx) {
    Y_ABORT_UNLESS(VolatileState.AcquireReadStepFlushing);
    VolatileState.AcquireReadStepFlushing = false;

    Y_ABORT_UNLESS(!VolatileState.AcquireReadStepStarting);
    VolatileState.AcquireReadStepStarting = true;
    ++VolatileState.AcquireReadStepInFlight;
    VolatileState.AcquireReadStepLast = ctx.Monotonic();
    Execute(new TTxAcquireReadStep(this, VolatileState.AcquireReadStepLast), ctx);
}

} // namespace NFlatTxCoordinator
} // namespace NKikimr
