#include "flow_control_manager_actor.h"
#include "flow_control_manager_service.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/utility.h>

#include <cmath>

namespace NKikimr::NColumnShard::NFlowControl {

TFlowControlManager::TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
    : TActor(&TThis::StateMain)
    , Counters(countersGroup)
    , Drain(Counters)
{
    // The seed is only a starting point: PrepareDrainCycle re-reads the bounds every drain cycle,
    // so FlowControl config merged after construction still takes effect.
    Drain.Seed(TFlowControlManagerServiceOperator::GetDrainRateParams());
    Drain.PublishCounters();
}

TDrainState TFlowControlManager::MakeDrainState(TInstant now) const {
    TDrainState state;
    state.Now = now;
    state.AnyHotNode = NodeState.AnyHot();
    state.QueueEmpty = WaitQueue.Empty();
    state.FrontWaiterBatchSize = FrontWaiterBatchSize(now);
    return state;
}

double TFlowControlManager::FrontWaiterBatchSize(TInstant now) const {
    for (const auto& waiter : WaitQueue.GetOrder()) {
        if (waiter.DrainScheduled) {
            continue;
        }
        // Expired heads are skipped by ScheduleDrainEligible; do not let their BatchSize pin a cap
        // below a still-eligible waiter further back in the FIFO.
        if (now >= waiter.WaitDeadline) {
            continue;
        }
        // Same filter as ScheduleDrainEligible: a hot head is not drainable, so pinning SoftCap to
        // it would strand a later cool waiter that could actually pay.
        if (!NodeState.IsAdmitAllowed(waiter.TabletIds)) {
            continue;
        }
        return static_cast<double>(waiter.BatchSize);
    }
    return 0.0;
}

void TFlowControlManager::PublishMapSizes() const {
    Counters.SetHotNodesCount(NodeState.HotCount());
    Counters.SetTabletToNodeCount(NodeState.TabletCount());
    Counters.SetWaitQueueCount(WaitQueue.Size());
    Counters.SetDelayedRejectQueueCount(DelayedRejects.Size());
}

void TFlowControlManager::MaybeStartNodeRechecks(const TVector<ui64>& tabletIds) {
    const TInstant now = TActivationContext::Now();
    for (const ui64 tabletId : NodeState.PickTabletsForRecheck(tabletIds, now, NodeRecheckPeriod)) {
        Counters.OnNodeRecheck();
        TEvTabletResolver::TEvForward::TResolveFlags flags;
        flags.SetAllowFollower(false);
        Send(MakeTabletResolverID(), new TEvTabletResolver::TEvForward(tabletId, nullptr, flags));
    }
}

void TFlowControlManager::RefundDrainToken(TWaiter& waiter) {
    if (waiter.TokenReserved) {
        Drain.Refund(waiter.BatchSize);
        waiter.TokenReserved = false;
    }
}

void TFlowControlManager::EraseWaiter(ui64 waiterId) {
    auto waiter = WaitQueue.Erase(waiterId);
    if (!waiter) {
        return;
    }
    RefundDrainToken(*waiter);
    // This is the single choke point for waiter removal, so it is where we notice the queue
    // draining back to empty and reopen the observation window.
    if (WaitQueue.Empty()) {
        Drain.NoteQueueEmpty();
    }
    PublishMapSizes();
    Drain.PublishCounters();
}

void TFlowControlManager::ScheduleDrainEligible(const TActorContext& ctx) {
    const TInstant now = TActivationContext::Now();
    Drain.PrepareDrainCycle(MakeDrainState(now), TFlowControlManagerServiceOperator::GetDrainRateParams());

    bool moreEligibleWithoutToken = false;
    // Bytes still missing before the first waiter that could not pay may go; drives the pacing
    // wakeup below.
    double bytesDeficit = 0.0;
    TVector<ui64> expiredIds;
    for (auto& waiter : WaitQueue.MutableOrder()) {
        if (waiter.DrainScheduled) {
            continue;
        }
        if (now >= waiter.WaitDeadline) {
            // The helper normally owns RejectNow via CancelWait, but if it never learned the
            // waiter id (lost Wait result, admit fail-open) the entry would sit here forever and
            // consume queue capacity. Expire it ourselves.
            expiredIds.push_back(waiter.WaiterId);
            continue;
        }
        if (!NodeState.IsAdmitAllowed(waiter.TabletIds)) {
            continue;
        }

        if (!Drain.TryReserve(waiter.BatchSize)) {
            moreEligibleWithoutToken = true;
            bytesDeficit = Max(0.0, static_cast<double>(waiter.BatchSize) - Drain.GetTokensBytes());
            break;
        }

        waiter.DrainScheduled = true;
        waiter.TokenReserved = true;
        TDuration jitter = TFlowControlManagerServiceOperator::PickDrainJitter();
        // Never schedule past the waiter's deadline: jitter > remaining time makes every
        // DrainWaiter miss, refund, and retry until the helper times out (Drained=0).
        if (jitter != TDuration::Zero() && waiter.WaitDeadline > now) {
            const TDuration remaining = waiter.WaitDeadline - now;
            if (jitter >= remaining) {
                jitter = remaining > TDuration::MilliSeconds(1) ? remaining - TDuration::MilliSeconds(1) : TDuration::Zero();
            }
        }
        if (jitter == TDuration::Zero()) {
            ctx.Send(ctx.SelfID, new TEvDrainWaiter(waiter.WaiterId));
        } else {
            ctx.Schedule(jitter, new TEvDrainWaiter(waiter.WaiterId));
        }
    }
    for (const ui64 waiterId : expiredIds) {
        Counters.OnWaitQueueTimedOut();
        EraseWaiter(waiterId);
    }

    // While a node is hot nothing is drainable, so the pacing wakeup above never fires — keep a
    // slow tick alive to integrate the decay. Stop once both rates sit on their floors: there is
    // nothing left to decay and the timer would run forever.
    const bool hotTick = NodeState.AnyHot() && !Drain.IsAtRateFloor();
    if ((moreEligibleWithoutToken || hotTick) && !DrainWakeupScheduled) {
        DrainWakeupScheduled = true;
        // Wake when the *more depleted* bucket will next admit the front waiter: the time for one
        // count token, or the time to accrue that waiter's bytes deficit. Cap the delay so a
        // floor-rate / large-batch deficit cannot park ContinueDrain for hours while the queue
        // only ages into timeouts.
        constexpr ui64 MaxContinueDrainDelayMs = 1000;
        const double rateCount = Drain.GetRateCount();
        const double rateBytes = Drain.GetRateBytes();
        ui64 delayCountMs = 100;
        if (rateCount > 0) {
            delayCountMs = Max<ui64>(1, static_cast<ui64>(std::llround(1000.0 / rateCount)));
        }
        ui64 delayBytesMs = 1;
        if (rateBytes > 0 && bytesDeficit > 0) {
            delayBytesMs = Max<ui64>(1, static_cast<ui64>(std::llround(1000.0 * bytesDeficit / rateBytes)));
        }
        const ui64 delayMs = moreEligibleWithoutToken ? Min(MaxContinueDrainDelayMs, Max(delayCountMs, delayBytesMs)) : HotDecayTickMs;
        ctx.Schedule(TDuration::MilliSeconds(delayMs), new TEvContinueDrain());
    }

    Drain.PublishCounters();
}

void TFlowControlManager::Handle(const NFlowControl::TEvTryAdmit::TPtr& ev, const TActorContext& ctx) {
    const auto& tabletIds = ev->Get()->GetTabletIds();
    const ui64 batchSize = ev->Get()->GetBatchSize();
    const TInstant now = TActivationContext::Now();
    const TVector<ui32> targetNodes = NodeState.CollectTargetNodes(tabletIds);

    // Admit straight through only when no target node is overloaded AND nobody is already queued
    // for one of them: overtaking a waiter would starve the queue under steady load.
    if (NodeState.IsAdmitAllowed(tabletIds) && !WaitQueue.HasWaitersOnAnyNode(targetNodes)) {
        Counters.OnAdmitAllowed();
        // Fast path: the queue is empty, so this is the "observation window". Fold this admit's
        // spacing and size into the EWMA that will seed the drain rates when the queue first fills.
        const TDrainState state = MakeDrainState(now);
        Drain.NoteFastPathAdmit(state, batchSize);
        Drain.NoteAdmitted(state, batchSize);
        ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::Allow));
        return;
    }

    MaybeStartNodeRechecks(tabletIds);

    const TInstant waitDeadline = ev->Get()->GetWaitDeadline();
    if (now >= waitDeadline) {
        Counters.OnAdmitRejected();
        Counters.OnWaitQueueRejectDeadlineAtAdmit();
        ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::RejectNow));
        return;
    }

    // Read the caps live (matching GetMaxWaitQueueSize) so UT/config overrides applied after FCM
    // construction take effect.
    // MaxWaitQueueSize == 0 means "do not wait": reject immediately. Size() >= 0 is always true,
    // so without this branch every gated admit would fall into delayed-reject instead.
    if (TFlowControlManagerServiceOperator::GetMaxWaitQueueSize() == 0) {
        Counters.OnAdmitRejected();
        Counters.OnWaitQueueRejectFull();
        ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::RejectNow));
        return;
    }
    if (WaitQueue.Size() >= TFlowControlManagerServiceOperator::GetMaxWaitQueueSize()) {
        // Wait queue is full; fall back to the delayed-reject queue if it still has room.
        if (DelayedRejects.Size() >= TFlowControlManagerServiceOperator::GetMaxDelayedRejectQueueSize()) {
            Counters.OnAdmitRejected();
            Counters.OnWaitQueueRejectFull();
            Counters.OnDelayedRejectQueueFull();
            ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::RejectNow));
            return;
        }

        // Enqueue for delayed reject: the Arrow batch is dropped, OVERLOADED is sent after a delay.
        // The instant is derived from the client's operation start, not from now, so the share of
        // the budget the client keeps for its retry does not shrink by whatever the request already
        // spent upstream. Already in the past (little budget left) means reject immediately.
        const TInstant rejectAt = ev->Get()->GetDelayedRejectAt();
        const ui64 rejectId = DelayedRejects.Enqueue(ev->Sender, rejectAt);
        ctx.Schedule(rejectAt > now ? rejectAt - now : TDuration::Zero(), new TEvFireDelayedReject(rejectId));

        Counters.OnAdmitRejected();
        Counters.OnDelayedRejectEnqueue();
        PublishMapSizes();
        ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::DelayedReject, 0, TInstant::Zero(), rejectId));
        return;
    }

    // Empty -> non-empty transition: the incoming rate has outrun the fast-path throughput, so seed
    // the drain rates from what we just observed (before adding the first waiter).
    Drain.NoteQueueBecameNonEmpty(MakeDrainState(now), batchSize);

    TWaiter waiter;
    waiter.Helper = ev->Sender;
    waiter.TabletIds = tabletIds;
    waiter.TargetNodes = targetNodes;
    waiter.WaitDeadline = waitDeadline;
    waiter.EnqueuedAt = now;
    waiter.BatchSize = batchSize;
    const ui64 waiterId = WaitQueue.Enqueue(std::move(waiter));
    Counters.OnWaitQueueEnqueue();
    PublishMapSizes();
    ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::Wait, waiterId, waitDeadline));
    // Kick the drain loop: without this a newly enqueued waiter only moves when some other event
    // (outcome / prior DrainWaiter / READY) happens to call ScheduleDrainEligible, so tokens and
    // the refill rate could climb while the queue sits idle.
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvCancelWait::TPtr& ev, const TActorContext& ctx) {
    const ui64 waiterId = ev->Get()->GetWaiterId();
    // Only count against the wait-queue derivatives if the waiter was actually present.
    if (!WaitQueue.Contains(waiterId)) {
        return;
    }
    if (ev->Get()->GetDeadlineExpired()) {
        Counters.OnWaitQueueTimedOut();
    } else {
        Counters.OnWaitQueueCancelled();
    }
    // EraseWaiter refunds a reserved drain token if any; re-run eligibility so another waiter can
    // take that budget (and so a timed-out DrainScheduled waiter does not stall the drain chain
    // until an unrelated outcome arrives).
    EraseWaiter(waiterId);
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvContinueDrain::TPtr& /*ev*/, const TActorContext& ctx) {
    DrainWakeupScheduled = false;
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvDrainWaiter::TPtr& ev, const TActorContext& ctx) {
    const ui64 waiterId = ev->Get()->GetWaiterId();
    auto* waiter = WaitQueue.Find(waiterId);
    if (!waiter) {
        // Waiter was cancelled/timed out after Schedule(jitter); the reserved token was already
        // refunded in EraseWaiter. Still wake the drain loop — otherwise each timed-out in-flight
        // DrainWaiter permanently drops a wakeup.
        ScheduleDrainEligible(ctx);
        return;
    }

    const TInstant now = TActivationContext::Now();
    if (now >= waiter->WaitDeadline) {
        Counters.OnWaitQueueTimedOut();
        EraseWaiter(waiterId);
        ScheduleDrainEligible(ctx);
        return;
    }

    if (!NodeState.IsAdmitAllowed(waiter->TabletIds)) {
        // A target node went hot after we reserved the token: try the next eligible waiter.
        RefundDrainToken(*waiter);
        waiter->DrainScheduled = false;
        Drain.PublishCounters();
        ScheduleDrainEligible(ctx);
        return;
    }

    const TActorId helper = waiter->Helper;
    const TDuration waited = now - waiter->EnqueuedAt;
    const ui64 batchSize = waiter->BatchSize;
    // The token was reserved at schedule time and is being spent now: clear the flag before erase
    // so EraseWaiter does not refund it.
    waiter->TokenReserved = false;
    EraseWaiter(waiterId);
    Drain.NoteAdmitted(MakeDrainState(now), batchSize);
    Drain.NoteWaiterReleased();
    Counters.OnWaitQueueDrain(waited);
    Counters.OnAdmitAllowed();
    Counters.OnDrainAllowed();
    ctx.Send(helper, new TEvTryAdmitResult(EAdmitDecision::Allow));
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvWriteOutcome::TPtr& ev, const TActorContext& ctx) {
    // Closed-loop feedback, together with the hot-node edges in Handle(TEvNodeOverloadStatus).
    const TInstant now = TActivationContext::Now();
    Drain.NoteWriteOutcome(MakeDrainState(now), TFlowControlManagerServiceOperator::GetDrainRateParams(), ev->Get()->GetOutcome());
    // A cohort close may have raised the rate, so re-evaluate eligibility.
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvNodeOverloadStatus::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    // The publisher is the sender. OM is looked up as a node-0 local service, but its SelfId
    // (and therefore Sender after delivery) is node-scoped, which is what the rest of the product
    // relies on. Sender.NodeId()==0 is empty-sender / local-service delivery to this FCM: us.
    ui32 nodeId = ev->Sender.NodeId();
    if (!nodeId) {
        nodeId = ctx.SelfID.NodeId();
    }
    if (!nodeId) {
        return;
    }
    const ui64 generation = record.GetGeneration();
    const TInstant now = TActivationContext::Now();

    switch (record.GetStatus()) {
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED: {
            // Gate admits to this node, and on the empty -> non-empty edge also cut the drain
            // rate. Compaction overload is published here even when writes complete successfully
            // (no / high in-flight limit), so write outcomes alone would never cut.
            const bool firstHot = NodeState.MarkHot(nodeId, generation);
            Counters.OnStatusOverloaded();
            Drain.NoteHotNode(now);
            if (firstHot) {
                Drain.NoteFirstHotNode(MakeDrainState(now), TFlowControlManagerServiceOperator::GetDrainRateParams());
            }
            // Start the decay integrator and keep a tick alive while hot.
            ScheduleDrainEligible(ctx);
            break;
        }
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY: {
            const bool allReady = NodeState.MarkReady(nodeId, generation);
            Counters.OnStatusReady();
            if (allReady) {
                Drain.NoteAllNodesReady(MakeDrainState(now));
            }
            ScheduleDrainEligible(ctx);
            break;
        }
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_UNSPECIFIED:
            break;
    }
    PublishMapSizes();
}

void TFlowControlManager::Handle(const NFlowControl::TEvTabletLocationUpdated::TPtr& ev, const TActorContext& ctx) {
    NodeState.SetTabletNode(ev->Get()->GetTabletId(), ev->Get()->GetNodeId());
    PublishMapSizes();
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvTabletLocationInvalidated::TPtr& ev, const TActorContext& ctx) {
    NodeState.ForgetTablet(ev->Get()->GetTabletId());
    PublishMapSizes();
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvFireDelayedReject::TPtr& ev, const TActorContext& ctx) {
    auto reject = DelayedRejects.Erase(ev->Get()->GetRejectId());
    if (!reject) {
        return;   // already cancelled or fired
    }

    Counters.OnDelayedRejectFired();
    PublishMapSizes();

    // The helper actor owns the client's TIssues and attaches the reason before forwarding.
    ctx.Send(reject->ReplyTo, new NActors::TEvents::TEvCompleted(0, Ydb::StatusIds::OVERLOADED));
}

void TFlowControlManager::Handle(const TEvTabletResolver::TEvForwardResult::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    NodeState.FinishRecheck(msg->TabletID);
    if (msg->Status != NKikimrProto::OK || !msg->TabletActor) {
        return;
    }
    NodeState.SetTabletNode(msg->TabletID, msg->TabletActor.NodeId());
    PublishMapSizes();
    ScheduleDrainEligible(ctx);
}

}   // namespace NKikimr::NColumnShard::NFlowControl
