#include "flow_control_manager_actor.h"
#include "flow_control_manager_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/utility.h>

#include <cmath>

namespace NKikimr::NColumnShard::NFlowControl {

namespace {

TIntrusivePtr<::NMonitoring::TDynamicCounters> FlowControlCountersGroup;

class TParsedBatchData: public NEvWrite::IShardsSplitter::IEvWriteDataAccessor {
private:
    using TBase = NEvWrite::IShardsSplitter::IEvWriteDataAccessor;
    std::shared_ptr<arrow::RecordBatch> Batch;

public:
    explicit TParsedBatchData(std::shared_ptr<arrow::RecordBatch> batch)
        : TBase(NArrow::GetBatchMemorySize(batch))
        , Batch(std::move(batch))
    {
    }

    std::shared_ptr<arrow::RecordBatch> GetDeserializedBatch() const override {
        return Batch;
    }

    TString GetSerializedData() const override {
        return NArrow::SerializeBatchNoCompression(Batch);
    }
};

bool TryCollectTargetTablets(const TLongTxWrite& tx, TVector<ui64>* tabletIds) {
    Y_ABORT_UNLESS(tabletIds);
    tabletIds->clear();

    const auto& navigate = tx.GetNavigateResult();
    if (!navigate || navigate->ErrorCount > 0 || navigate->ResultSet.empty()) {
        return false;
    }

    const auto& entry = navigate->ResultSet[0];
    auto shardsSplitter = NEvWrite::IShardsSplitter::BuildSplitter(entry);
    if (!shardsSplitter) {
        return false;
    }

    TParsedBatchData accessor(tx.GetBatch());
    const auto initStatus = shardsSplitter->SplitData(entry, accessor);
    if (!initStatus.Ok()) {
        return false;
    }

    for (const auto& [tabletId, _] : shardsSplitter->GetSplitData().GetShardsInfo()) {
        tabletIds->push_back(tabletId);
    }
    return true;
}

TIntrusivePtr<::NMonitoring::TDynamicCounters> CountersGroupOrNull() {
    if (FlowControlCountersGroup) {
        return FlowControlCountersGroup;
    }
    if (HasAppData() && AppData()->Counters) {
        return AppData()->Counters;
    }
    return nullptr;
}

enum class EHelperWakeup: ui64 {
    WaitDeadline = 1,
};

// Runs on the caller's mailbox (BulkUpsert / DoLongTxWriteSameMailbox). Does data split + FCM admit
// RPC here, then starts TLongTxWriteInternal on the same mailbox (forceNoFlowControl).
// On Wait: hold until Allow (READY drain) or wait-deadline / RejectNow → OVERLOADED.
class TLongTxWriteFlowControlled: public NActors::TActorBootstrapped<TLongTxWriteFlowControlled> {
    TLongTxWrite Tx;
    TCSFlowControlManagerCounters Counters;
    TInstant WaitAdmitStartedAt;
    ui64 WaiterId = 0;
    bool Queued = false;

public:
    explicit TLongTxWriteFlowControlled(TLongTxWrite&& tx)
        : Tx(std::move(tx))
        , Counters(CountersGroupOrNull())
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Counters.OnRequestStart();

        const TInstant splitStartedAt = TActivationContext::Now();
        TVector<ui64> tabletIds;
        const bool splitOk = TryCollectTargetTablets(Tx, &tabletIds);
        Counters.OnSplitFinished(TActivationContext::Now() - splitStartedAt);

        if (!splitOk) {
            // Same as legacy FCM path: cannot admit without targets → fail-open into write actor
            // (it will reply with the real navigate/split error).
            Counters.OnAdmitSkippedNoSplit();
            StartWrite(ctx);
            return Finish(ctx);
        }

        WaitAdmitStartedAt = TActivationContext::Now();
        Counters.OnWaitingAdmitStart();
        ctx.Send(TFlowControlManagerServiceOperator::MakeServiceId(ctx.SelfID.NodeId()),
            std::make_unique<TEvTryAdmit>(std::move(tabletIds), Tx.GetDeadline(), Tx.GetOperationTimeout()));
        Become(&TThis::StateWaitAdmit);
    }

private:
    STRICT_STFUNC(
        StateWaitAdmit, HFunc(TEvTryAdmitResult, HandleAdmitResult) HFunc(NActors::TEvents::TEvCompleted, HandleDelayedRejectCompleted))
    STRICT_STFUNC(StateQueued, HFunc(TEvTryAdmitResult, HandleQueuedResult) HFunc(NActors::TEvents::TEvWakeup, HandleWaitDeadlineWakeup))

    void HandleDelayedRejectCompleted(NActors::TEvents::TEvCompleted::TPtr& ev, const TActorContext& ctx) {
        // Forward the OVERLOADED response to the original client
        ctx.Send(Tx.GetReplyTo(), ev->Release().Release());
        Finish(ctx);
    }

    void HandleAdmitResult(TEvTryAdmitResult::TPtr& ev, const TActorContext& ctx) {
        Counters.OnWaitingAdmitFinish(TActivationContext::Now() - WaitAdmitStartedAt);

        switch (ev->Get()->GetDecision()) {
            case EAdmitDecision::Allow:
                StartWrite(ctx);
                Finish(ctx);
                break;
            case EAdmitDecision::RejectNow:
                ReplyOverloaded(ctx, "destination node is overloaded");
                Finish(ctx);
                break;
            case EAdmitDecision::Wait:
                EnterQueued(ctx, ev->Get()->GetWaiterId(), ev->Get()->GetWaitDeadline());
                break;
            case EAdmitDecision::DelayedReject:
                // FCM will send TEvCompleted(OVERLOADED) after a delay.
                // Drop Arrow batch now to free memory, but stay alive to forward the response.
                Tx.DetachBatch();
                // Stay in current state and wait for TEvCompleted from FCM
                break;
        }
    }

    void EnterQueued(const TActorContext& ctx, ui64 waiterId, TInstant waitDeadline) {
        WaiterId = waiterId;
        Queued = true;
        Become(&TThis::StateQueued);

        const TInstant now = TActivationContext::Now();
        if (waitDeadline <= now) {
            CancelAndReject(ctx, "destination node is overloaded");
            return;
        }
        ctx.Schedule(waitDeadline - now, new NActors::TEvents::TEvWakeup(static_cast<ui64>(EHelperWakeup::WaitDeadline)));
    }

    void HandleQueuedResult(TEvTryAdmitResult::TPtr& ev, const TActorContext& ctx) {
        switch (ev->Get()->GetDecision()) {
            case EAdmitDecision::Allow:
                Queued = false;
                StartWrite(ctx);
                Finish(ctx);
                break;
            case EAdmitDecision::RejectNow:
                Queued = false;
                ReplyOverloaded(ctx, "destination node is overloaded");
                Finish(ctx);
                break;
            case EAdmitDecision::DelayedReject:
                // FCM will send OVERLOADED after a delay. Drop Arrow batch now to free memory.
                // We don't need to wait for anything - FCM handles the delayed response.
                // Just finish this helper actor; FCM has all the info it needs to send OVERLOADED.
                Queued = false;
                Finish(ctx);
                break;
            case EAdmitDecision::Wait:
                // Should not be re-issued while already queued.
                break;
        }
    }

    void HandleWaitDeadlineWakeup(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Tag != static_cast<ui64>(EHelperWakeup::WaitDeadline)) {
            return;
        }
        if (!Queued) {
            return;
        }
        CancelAndReject(ctx, "destination node is overloaded");
    }

    void CancelAndReject(const TActorContext& ctx, const TString& message) {
        if (Queued && WaiterId) {
            ctx.Send(TFlowControlManagerServiceOperator::MakeServiceId(ctx.SelfID.NodeId()), std::make_unique<TEvCancelWait>(WaiterId));
        }
        Queued = false;
        ReplyOverloaded(ctx, message);
        Finish(ctx);
    }

    void StartWrite(const TActorContext& ctx) {
        NTxProxy::DoLongTxWriteSameMailbox(ctx, Tx.GetReplyTo(), Tx.GetLongTxId(), Tx.GetDedupId(), Tx.GetDatabaseName(), Tx.GetPath(),
            Tx.GetNavigateResult(), Tx.GetBatch(), Tx.GetIssues(), Tx.GetUserCtx(), /*forceNoFlowControl=*/true);
    }

    void ReplyOverloaded(const TActorContext& ctx, const TString& message) {
        if (!message.empty() && Tx.GetIssues()) {
            Tx.GetIssues()->AddIssue(NYql::TIssue(message));
        }
        ctx.Send(Tx.GetReplyTo(), new NActors::TEvents::TEvCompleted(0, Ydb::StatusIds::OVERLOADED));
    }

    void Finish(const TActorContext& /*ctx*/) {
        Counters.OnRequestFinish();
        PassAway();
    }
};

}   // namespace

TFlowControlManager::TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
    : TActor(&TThis::StateMain)
    , Counters(countersGroup)
{
    FlowControlCountersGroup = countersGroup;
    const auto params = TFlowControlManagerServiceOperator::GetDrainRateParams();
    RMin = params.RMin;
    RMax = params.RMax;
    RefillRateR = params.RStart;
    Burst = params.Burst;
    AimdAdd = params.AimdAdd;
    AimdBeta = params.AimdBeta;
    AimdGrow = params.AimdGrow;
    AimdHold = params.AimdHold;
    AimdFeedback = params.AimdFeedback;
    Tokens = Burst;
    PublishDrainGauges();
}

void TFlowControlManager::PublishMapSizes() const {
    Counters.SetHotNodesCount(HotNodes.size());
    Counters.SetTabletToNodeCount(TabletToNode.size());
    Counters.SetWaitQueueCount(Waiters.size());
    Counters.SetDelayedRejectQueueCount(DelayedRejects.size());
}

void TFlowControlManager::PublishDrainGauges() const {
    Counters.SetDrainRefillRate(static_cast<ui64>(std::llround(RefillRateR)));
    Counters.SetDrainTokens(static_cast<ui64>(std::llround(Tokens)));
}

void TFlowControlManager::RecomputeBurst() {
    Burst = Max(1.0, 2.0 * RefillRateR);
    Tokens = Min(Tokens, Burst);
}

TVector<ui32> TFlowControlManager::CollectDestinationNodes(const TVector<ui64>& tabletIds) const {
    THashSet<ui32> nodes;
    TVector<ui32> result;
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId) {
            continue;
        }
        if (nodes.insert(*nodeId).second) {
            result.push_back(*nodeId);
        }
    }
    return result;
}

bool TFlowControlManager::IsAdmitAllowed(const TVector<ui64>& tabletIds) const {
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId) {
            continue;   // fail-open for unknown location
        }
        if (HotNodes.contains(*nodeId)) {
            return false;
        }
    }
    return true;
}

bool TFlowControlManager::HasWaitersOnDestinations(const TVector<ui64>& tabletIds) const {
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId) {
            continue;
        }
        if (const auto* count = WaiterCountByNode.FindPtr(*nodeId)) {
            if (*count > 0) {
                return true;
            }
        }
    }
    return false;
}

void TFlowControlManager::IncWaiterCounts(const TVector<ui32>& nodes) {
    for (const ui32 nodeId : nodes) {
        ++WaiterCountByNode[nodeId];
    }
}

void TFlowControlManager::DecWaiterCounts(const TVector<ui32>& nodes) {
    for (const ui32 nodeId : nodes) {
        auto it = WaiterCountByNode.find(nodeId);
        if (it == WaiterCountByNode.end()) {
            continue;
        }
        if (it->second <= 1) {
            WaiterCountByNode.erase(it);
        } else {
            --it->second;
        }
    }
}

void TFlowControlManager::MaybeStartLocationRechecks(const TVector<ui64>& tabletIds) {
    const TInstant now = TActivationContext::Now();
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId || !HotNodes.contains(*nodeId)) {
            continue;
        }
        if (LocationRecheckInFlight.contains(tabletId)) {
            continue;
        }
        if (const auto* last = LastLocationRecheck.FindPtr(tabletId)) {
            if (now - *last < LocationRecheckPeriod) {
                continue;
            }
        }

        LastLocationRecheck[tabletId] = now;
        LocationRecheckInFlight.insert(tabletId);
        Counters.OnLocationRecheck();

        TEvTabletResolver::TEvForward::TResolveFlags flags;
        flags.SetAllowFollower(false);
        Send(MakeTabletResolverID(), new TEvTabletResolver::TEvForward(tabletId, nullptr, flags));
    }
}

void TFlowControlManager::RefundDrainToken(TWaiter& waiter) {
    if (waiter.TokenReserved) {
        Tokens = Min(Burst, Tokens + 1.0);
        waiter.TokenReserved = false;
    }
}

void TFlowControlManager::EraseWaiter(ui64 waiterId, bool countCancel) {
    auto it = Waiters.find(waiterId);
    if (it == Waiters.end()) {
        return;
    }
    RefundDrainToken(it->second);
    DecWaiterCounts(it->second.DestinationNodes);
    Waiters.erase(it);
    for (auto qIt = WaitQueueOrder.begin(); qIt != WaitQueueOrder.end(); ++qIt) {
        if (*qIt == waiterId) {
            WaitQueueOrder.erase(qIt);
            break;
        }
    }
    if (countCancel) {
        Counters.OnWaitQueueCancel();
    }
    PublishMapSizes();
    PublishDrainGauges();
}

void TFlowControlManager::RefillTokens(TInstant now) {
    if (LastRefillAt == TInstant::Zero()) {
        LastRefillAt = now;
        return;
    }
    const double dt = (now - LastRefillAt).SecondsFloat();
    if (dt > 0) {
        Tokens = Min(Burst, Tokens + RefillRateR * dt);
        LastRefillAt = now;
    }
    MaybeGrowRate(now);
}

void TFlowControlManager::MaybeGrowRate(TInstant now) {
    if (now < HoldUntil) {
        return;
    }
    if (LastOverloadAt && now - LastOverloadAt < AimdGrow) {
        return;
    }
    if (LastGrowAt && now - LastGrowAt < AimdGrow) {
        return;
    }
    if (RefillRateR >= RMax) {
        return;
    }
    const double prev = RefillRateR;
    RefillRateR = Min(RMax, RefillRateR + AimdAdd);
    RecomputeBurst();
    LastGrowAt = now;
    if (RefillRateR > prev) {
        Counters.OnDrainRateGrow();
    }
}

void TFlowControlManager::CutRateOnOverload(TInstant now) {
    LastOverloadAt = now;
    if (!LastDrainActivityAt || now - LastDrainActivityAt > AimdFeedback) {
        return;
    }
    const double prev = RefillRateR;
    RefillRateR = Max(RMin, RefillRateR * AimdBeta);
    RecomputeBurst();
    HoldUntil = now + AimdHold;
    if (RefillRateR < prev) {
        Counters.OnDrainRateCut();
    }
    PublishDrainGauges();
}

void TFlowControlManager::ScheduleDrainEligible(const TActorContext& ctx) {
    const TInstant now = TActivationContext::Now();
    RefillTokens(now);

    bool moreEligibleWithoutToken = false;
    for (const ui64 waiterId : WaitQueueOrder) {
        auto* waiter = Waiters.FindPtr(waiterId);
        if (!waiter || waiter->DrainScheduled) {
            continue;
        }
        if (now >= waiter->WaitDeadline) {
            continue;   // helper deadline timer owns RejectNow
        }
        if (!IsAdmitAllowed(waiter->TabletIds)) {
            continue;
        }

        if (Tokens < 1.0) {
            moreEligibleWithoutToken = true;
            break;
        }

        Tokens -= 1.0;
        waiter->DrainScheduled = true;
        waiter->TokenReserved = true;
        const TDuration jitter = TFlowControlManagerServiceOperator::PickDrainJitter();
        if (jitter == TDuration::Zero()) {
            ctx.Send(ctx.SelfID, new TEvDrainWaiter(waiterId));
        } else {
            ctx.Schedule(jitter, new TEvDrainWaiter(waiterId));
        }
    }

    if (!moreEligibleWithoutToken) {
        for (const ui64 waiterId : WaitQueueOrder) {
            auto* waiter = Waiters.FindPtr(waiterId);
            if (!waiter || waiter->DrainScheduled) {
                continue;
            }
            if (now >= waiter->WaitDeadline) {
                continue;
            }
            if (!IsAdmitAllowed(waiter->TabletIds)) {
                continue;
            }
            moreEligibleWithoutToken = true;
            break;
        }
    }

    if (moreEligibleWithoutToken && !DrainWakeupScheduled) {
        DrainWakeupScheduled = true;
        ui64 delayMs = 100;
        if (RefillRateR > 0) {
            delayMs = Max<ui64>(1, static_cast<ui64>(std::llround(1000.0 / RefillRateR)));
        }
        ctx.Schedule(TDuration::MilliSeconds(delayMs), new TEvContinueDrain());
    }

    PublishDrainGauges();
}

void TFlowControlManager::Handle(const NFlowControl::TEvLongTxWrite::TPtr& ev, const TActorContext& ctx) {
    // Compatibility path: do not run split/write on FCM mailbox. Schedule helper on a separate mailbox.
    auto tx = ev->Get()->DetachLongTxWrite();
    ctx.Register(new TLongTxWriteFlowControlled(std::move(tx)));
}

void TFlowControlManager::Handle(const NFlowControl::TEvTryAdmit::TPtr& ev, const TActorContext& ctx) {
    const TInstant startedAt = TActivationContext::Now();
    const auto& tabletIds = ev->Get()->GetTabletIds();
    const TDuration duration = TActivationContext::Now() - startedAt;

    if (IsAdmitAllowed(tabletIds) && !HasWaitersOnDestinations(tabletIds)) {
        Counters.OnAdmitAllowed(duration);
        ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::Allow));
        return;
    }

    MaybeStartLocationRechecks(tabletIds);

    const TInstant waitDeadline = ev->Get()->GetWaitDeadline();
    const TInstant now = TActivationContext::Now();
    if (now >= waitDeadline) {
        Counters.OnAdmitRejected(duration);
        Counters.OnWaitQueueRejectDeadlineAtAdmit();
        ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::RejectNow));
        return;
    }

    if (Waiters.size() >= TFlowControlManagerServiceOperator::GetMaxWaitQueueSize()) {
        // Wait queue is full. Check if we can use delayed-reject queue instead.
        // Read the cap live (matches GetMaxWaitQueueSize) so UT/config overrides applied
        // after FCM construction take effect.
        if (DelayedRejects.size() >= TFlowControlManagerServiceOperator::GetMaxDelayedRejectQueueSize()) {
            // Both queues full → immediate reject
            Counters.OnAdmitRejected(duration);
            Counters.OnWaitQueueRejectFull();
            Counters.OnDelayedRejectQueueFull();
            ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::RejectNow));
            return;
        }

        // Enqueue for delayed reject: drop Arrow batch, send OVERLOADED after delay
        const ui64 rejectId = NextRejectId++;
        const TInstant rejectAt =
            now + (ev->Get()->GetOperationTimeout() * TFlowControlManagerServiceOperator::GetDelayedRejectTimeoutPercent() / 100);

        TDelayedReject reject;
        reject.RejectId = rejectId;
        reject.ReplyTo = ev->Sender;
        reject.Issues = std::make_shared<NYql::TIssues>();
        reject.Issues->AddIssue(NYql::TIssue("destination node is overloaded; wait queue full"));
        reject.RejectAt = rejectAt;

        DelayedRejects.emplace(rejectId, std::move(reject));
        DelayedRejectOrder.push_back(rejectId);

        const TDuration delay = rejectAt > now ? rejectAt - now : TDuration::Zero();
        ctx.Schedule(delay, new TEvFireDelayedReject(rejectId));

        Counters.OnAdmitRejected(duration);
        Counters.OnDelayedRejectEnqueue();
        PublishMapSizes();
        ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::DelayedReject, 0, TInstant::Zero(), rejectId));
        return;
    }

    const ui64 waiterId = NextWaiterId++;
    TWaiter waiter;
    waiter.WaiterId = waiterId;
    waiter.Helper = ev->Sender;
    waiter.TabletIds = tabletIds;
    waiter.DestinationNodes = CollectDestinationNodes(tabletIds);
    waiter.WaitDeadline = waitDeadline;
    waiter.EnqueuedAt = now;
    IncWaiterCounts(waiter.DestinationNodes);
    Waiters.emplace(waiterId, std::move(waiter));
    WaitQueueOrder.push_back(waiterId);
    Counters.OnWaitQueueEnqueue();
    PublishMapSizes();
    ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::Wait, waiterId, waitDeadline));
}

void TFlowControlManager::Handle(const NFlowControl::TEvCancelWait::TPtr& ev, const TActorContext& /*ctx*/) {
    EraseWaiter(ev->Get()->GetWaiterId(), /*countCancel=*/true);
}

void TFlowControlManager::Handle(const NFlowControl::TEvContinueDrain::TPtr& /*ev*/, const TActorContext& ctx) {
    DrainWakeupScheduled = false;
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvDrainWaiter::TPtr& ev, const TActorContext& ctx) {
    const ui64 waiterId = ev->Get()->GetWaiterId();
    auto* waiter = Waiters.FindPtr(waiterId);
    if (!waiter) {
        return;
    }

    const TInstant now = TActivationContext::Now();
    if (now >= waiter->WaitDeadline) {
        // Leave for helper deadline / cancel; clear drain flag so we don't loop.
        RefundDrainToken(*waiter);
        waiter->DrainScheduled = false;
        PublishDrainGauges();
        ScheduleDrainEligible(ctx);
        return;
    }

    if (!IsAdmitAllowed(waiter->TabletIds)) {
        RefundDrainToken(*waiter);
        waiter->DrainScheduled = false;
        PublishDrainGauges();
        return;
    }

    const TActorId helper = waiter->Helper;
    const TDuration waited = now - waiter->EnqueuedAt;
    // Token already reserved at schedule time; clear flag before erase so EraseWaiter does not refund.
    waiter->TokenReserved = false;
    EraseWaiter(waiterId, /*countCancel=*/false);
    LastDrainActivityAt = now;
    Counters.OnWaitQueueDrain(waited);
    Counters.OnAdmitAllowed(TDuration::Zero());
    Counters.OnDrainAllowed();
    ctx.Send(helper, new TEvTryAdmitResult(EAdmitDecision::Allow));
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvNodeOverloadStatus::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    const ui32 nodeId = record.GetNodeId();
    const ui64 generation = record.GetGeneration();
    const TInstant now = TActivationContext::Now();

    switch (record.GetStatus()) {
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED:
            HotNodes[nodeId] = Max(HotNodes[nodeId], generation);
            Counters.OnStatusOverloaded();
            CutRateOnOverload(now);
            break;
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY: {
            auto it = HotNodes.find(nodeId);
            if (it != HotNodes.end() && generation >= it->second) {
                HotNodes.erase(it);
            }
            Counters.OnStatusReady();
            ScheduleDrainEligible(ctx);
            break;
        }
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_UNSPECIFIED:
            break;
    }
    PublishMapSizes();
}

void TFlowControlManager::Handle(const NFlowControl::TEvTabletLocationUpdated::TPtr& ev, const TActorContext& ctx) {
    TabletToNode[ev->Get()->GetTabletId()] = ev->Get()->GetNodeId();
    PublishMapSizes();
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvTabletLocationInvalidated::TPtr& ev, const TActorContext& ctx) {
    TabletToNode.erase(ev->Get()->GetTabletId());
    PublishMapSizes();
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvFireDelayedReject::TPtr& ev, const TActorContext& ctx) {
    const ui64 rejectId = ev->Get()->GetRejectId();
    auto it = DelayedRejects.find(rejectId);
    if (it == DelayedRejects.end()) {
        // Already cancelled or fired
        return;
    }

    TDelayedReject reject = std::move(it->second);
    DelayedRejects.erase(it);

    // Remove from order queue
    auto orderIt = std::find(DelayedRejectOrder.begin(), DelayedRejectOrder.end(), rejectId);
    if (orderIt != DelayedRejectOrder.end()) {
        DelayedRejectOrder.erase(orderIt);
    }

    Counters.OnDelayedRejectFired();
    PublishMapSizes();

    // Send OVERLOADED to the client
    if (reject.Issues && !reject.Issues->Empty()) {
        // Issues already set during enqueue
    }
    ctx.Send(reject.ReplyTo, new NActors::TEvents::TEvCompleted(0, Ydb::StatusIds::OVERLOADED));
}

void TFlowControlManager::Handle(const TEvTabletResolver::TEvForwardResult::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    LocationRecheckInFlight.erase(msg->TabletID);
    if (msg->Status != NKikimrProto::OK || !msg->TabletActor) {
        return;
    }
    TabletToNode[msg->TabletID] = msg->TabletActor.NodeId();
    PublishMapSizes();
    ScheduleDrainEligible(ctx);
}

void TFlowControlManagerServiceOperator::StartLongTxWrite(const TActorContext& ctx, TLongTxWrite&& longTxWrite) {
    // Keep split + LongTx write on the caller's mailbox (BulkUpsert upload actor).
    ctx.RegisterWithSameMailbox(new TLongTxWriteFlowControlled(std::move(longTxWrite)));
}

}   // namespace NKikimr::NColumnShard::NFlowControl
