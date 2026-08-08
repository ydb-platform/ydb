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

bool TryCollectTargetTablets(const TLongTxWrite& tx, TVector<ui64>* tabletIds, ui64* batchSize = nullptr) {
    Y_ABORT_UNLESS(tabletIds);
    tabletIds->clear();
    if (batchSize) {
        *batchSize = 0;
    }

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

    if (batchSize) {
        // Deserialized batch memory size, already computed by TParsedBatchData's base ctor.
        *batchSize = accessor.GetSize();
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
        ui64 batchSize = 0;
        const bool splitOk = TryCollectTargetTablets(Tx, &tabletIds, &batchSize);
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
            std::make_unique<TEvTryAdmit>(std::move(tabletIds), Tx.GetDeadline(), Tx.GetOperationTimeout(), batchSize));
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
            CancelAndReject(ctx, "destination node is overloaded", /*deadlineExpired=*/true);
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
        CancelAndReject(ctx, "destination node is overloaded", /*deadlineExpired=*/true);
    }

    void CancelAndReject(const TActorContext& ctx, const TString& message, bool deadlineExpired) {
        if (Queued && WaiterId) {
            ctx.Send(TFlowControlManagerServiceOperator::MakeServiceId(ctx.SelfID.NodeId()),
                std::make_unique<TEvCancelWait>(WaiterId, deadlineExpired));
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
    AimdBeta = params.AimdBeta;
    CubicRecoveryTargetSec = params.CubicRecoveryTargetSec;
    CubicProbePercent = params.CubicProbePercent;
    // Bytes bucket seed (mirrors count bucket).
    RMinBytes = params.RMinBytes;
    RMaxBytes = params.RMaxBytes;
    RefillRateBytesR = params.RStartBytes;
    AimdBetaBytes = params.AimdBetaBytes;
    // Start in probe phase at the seed rates (no recovery curve until a meaningful cut).
    WmaxCount = RefillRateR;
    WmaxBytes = RefillRateBytesR;
    CubicCCount = 0.0;
    CubicCBytes = 0.0;
    CubicEpochStart = TInstant::Zero();
    // Seed both buckets to the soft one-cohort cap (ceil(rate)) rather than a tunable Burst:
    // the first cohort may release immediately, then pacing takes over. This is bounded (never
    // more than one cohort) so it is not an idle-accumulated burst, and it avoids adding a full
    // 1/rate of latency to the first drained request after every idle period.
    Tokens = Max(1.0, std::ceil(RefillRateR));
    TokensBytes = Max(1.0, std::ceil(RefillRateBytesR));
    LastRefillAt = TInstant::Zero();
    LastRefillBytesAt = TInstant::Zero();
    // Observation starts fresh; the queue is empty at construction.
    WasQueueEmpty = true;
    ObservedOverload = false;
    LastObserveAt = TInstant::Zero();
    ObservedRateCount = 0.0;
    ObservedRateBytes = 0.0;
    // The bounds above are only a seed: if FlowControl config is merged after
    // construction, SyncDrainBounds() (called each drain cycle) will pick it up.
    PublishDrainGauges();
}

void TFlowControlManager::SyncDrainBounds() {
    const auto params = TFlowControlManagerServiceOperator::GetDrainRateParams();
    RMin = params.RMin;
    RMax = params.RMax;
    AimdBeta = params.AimdBeta;
    CubicRecoveryTargetSec = params.CubicRecoveryTargetSec;
    CubicProbePercent = params.CubicProbePercent;
    // Keep the live rate inside the (possibly updated) bounds. RMax* of 0 means no limit
    // (+inf via EffectiveRMax*); RMin* of 0 keeps a tiny UT floor via EffectiveRMin*.
    RefillRateR = Min(EffectiveRMax(), Max(EffectiveRMin(), RefillRateR));

    RMinBytes = params.RMinBytes;
    RMaxBytes = params.RMaxBytes;
    AimdBetaBytes = params.AimdBetaBytes;
    RefillRateBytesR = Min(EffectiveRMaxBytes(), Max(EffectiveRMinBytes(), RefillRateBytesR));
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
    Counters.SetDrainRefillRateBytes(static_cast<ui64>(std::llround(RefillRateBytesR)));
    Counters.SetDrainTokensBytes(static_cast<ui64>(std::llround(TokensBytes)));
    Counters.SetObservedRateCount(static_cast<ui64>(std::llround(ObservedRateCount)));
    Counters.SetObservedRateBytes(static_cast<ui64>(std::llround(ObservedRateBytes)));
    Counters.SetServedRateCount(static_cast<ui64>(std::llround(ServedRateCount)));
    Counters.SetServedRateBytes(static_cast<ui64>(std::llround(ServedRateBytes)));
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
        // No Burst cap anymore; the soft one-cohort cap in RefillTokens bounds accrual.
        Tokens += 1.0;
        TokensBytes += static_cast<double>(waiter.BatchSize);
        waiter.TokenReserved = false;
    }
}

void TFlowControlManager::EraseWaiter(ui64 waiterId) {
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
    // EraseWaiter is the single choke point for waiter removal (cancel, drain, deadline),
    // so this is where we notice the queue draining back to empty and reopen observation.
    MaybeMarkQueueEmpty();
    PublishMapSizes();
    PublishDrainGauges();
}

void TFlowControlManager::RefillTokens(TInstant now) {
    // Pick up any FlowControl config merged after construction (e.g. dynamic config)
    // and clamp the live rate into the current bounds before refilling tokens.
    SyncDrainBounds();

    // Count bucket. Soft cap = one cohort's worth (ceil(RefillRateR)) instead of a tunable
    // Burst: it only matters if all eligible waiters were blocked (hot nodes) and then become
    // ready at once, and it prevents releasing more than a cohort in that single instant.
    if (LastRefillAt == TInstant::Zero()) {
        LastRefillAt = now;
    } else {
        const double dt = (now - LastRefillAt).SecondsFloat();
        if (dt > 0) {
            const double cap = Max(1.0, std::ceil(RefillRateR));
            Tokens = Min(cap, Tokens + RefillRateR * dt);
            LastRefillAt = now;
        }
    }

    // Bytes bucket. Soft cap = one second of bytes, raised to the FIFO head's BatchSize so a
    // single large request can never permanently deadlock the queue.
    if (LastRefillBytesAt == TInstant::Zero()) {
        LastRefillBytesAt = now;
    } else {
        const double dtBytes = (now - LastRefillBytesAt).SecondsFloat();
        if (dtBytes > 0) {
            const double capBytes = BytesSoftCap();
            TokensBytes = Min(capBytes, TokensBytes + RefillRateBytesR * dtBytes);
            LastRefillBytesAt = now;
        }
    }
}

double TFlowControlManager::FrontWaiterBatchSize() const {
    const TInstant now = TActivationContext::Now();
    for (const ui64 waiterId : WaitQueueOrder) {
        const auto* waiter = Waiters.FindPtr(waiterId);
        if (!waiter || waiter->DrainScheduled) {
            continue;
        }
        // Expired heads are skipped by ScheduleDrainEligible; do not let their BatchSize
        // pin a cap below a still-eligible waiter further back in the FIFO.
        if (now >= waiter->WaitDeadline) {
            continue;
        }
        return static_cast<double>(waiter->BatchSize);
    }
    return 0.0;
}

double TFlowControlManager::BytesSoftCap() const {
    return Max(Max(1.0, std::ceil(RefillRateBytesR)), FrontWaiterBatchSize());
}

double TFlowControlManager::GrowthPeriodSec() const {
    // Growth is clocked by wall time, not by outcome arrivals: one TEvWriteOutcome is
    // emitted per *shard* write, so a single client request feeding N shards closes a
    // cohort N times faster than the loop assumes.
    return Max(0.1, CubicRecoveryTargetSec / 10.0);
}

double TFlowControlManager::HotCooldownSec() const {
    // Quiet period the system must sustain (no hot node, no overloaded outcome) before
    // growth is allowed again — plain hysteresis around the READY flap.
    return Max(0.2, CubicRecoveryTargetSec / 5.0);
}

double TFlowControlManager::HotDecayTauSec() const {
    return Max(0.1, CubicRecoveryTargetSec / 10.0);
}

void TFlowControlManager::NoteAdmitted(TInstant now, ui64 batchSize) {
    AccrueBusyTime(now);
    if (ServedWindowStart == TInstant::Zero()) {
        ServedWindowStart = now;
    }
    ServedAccumCount += 1.0;
    ServedAccumBytes += static_cast<double>(batchSize);
    CloseServedWindow(now);
}

void TFlowControlManager::AccrueBusyTime(TInstant now) {
    const TInstant last = LastBusySampleAt;
    LastBusySampleAt = now;
    if (last == TInstant::Zero() || now <= last) {
        return;
    }
    // Demand present and admits permitted: whatever we deliver in this interval is the
    // rate FCM itself is imposing, which is what the anchor must track.
    if (!Waiters.empty() && HotNodes.empty()) {
        ServedBusySec += (now - last).SecondsFloat();
    }
}

void TFlowControlManager::CloseServedWindow(TInstant now) {
    if (ServedWindowStart == TInstant::Zero()) {
        ServedWindowStart = now;
        return;
    }
    const double dt = (now - ServedWindowStart).SecondsFloat();
    if (dt < ServedWindowSec) {
        return;
    }
    const double busy = Min(dt, ServedBusySec);
    const double count = ServedAccumCount;
    const double bytes = ServedAccumBytes;
    ServedAccumCount = 0.0;
    ServedAccumBytes = 0.0;
    ServedBusySec = 0.0;
    ServedWindowStart = now;
    if (busy < dt * ServedBusyMinFraction || busy <= 0.0) {
        // Mostly idle or mostly gated: no capacity information, keep the previous estimate.
        return;
    }
    const double alpha = 1.0 - std::exp(-dt / ServedTauSec);
    ServedRateCount = alpha * (count / busy) + (1.0 - alpha) * ServedRateCount;
    ServedRateBytes = alpha * (bytes / busy) + (1.0 - alpha) * ServedRateBytes;
}

double TFlowControlManager::AnchorMaxCount() const {
    if (ServedRateCount <= 0.0) {
        return std::numeric_limits<double>::infinity();
    }
    return Max(EffectiveRMin(), AnchorFactor * ServedRateCount);
}

double TFlowControlManager::AnchorMaxBytes() const {
    if (ServedRateBytes <= 0.0) {
        return std::numeric_limits<double>::infinity();
    }
    return Max(EffectiveRMinBytes(), AnchorFactor * ServedRateBytes);
}

void TFlowControlManager::ApplyHotDecay(TInstant now) {
    if (HotNodes.empty()) {
        LastHotDecayAt = TInstant::Zero();
        return;
    }
    LastHotAt = now;
    if (LastHotDecayAt == TInstant::Zero()) {
        LastHotDecayAt = now;
        return;
    }
    const double dt = (now - LastHotDecayAt).SecondsFloat();
    if (dt <= 0.0) {
        return;
    }
    LastHotDecayAt = now;
    // The empty→non-empty hot edge cuts once; if the node stays hot that single cut was
    // evidently not enough, so keep applying it continuously (AimdBeta per tau) until the
    // pressure clears or the configured floor is reached.
    const double beta = Min(0.999, Max(0.01, AimdBeta));
    const double factor = std::pow(beta, dt / HotDecayTauSec());
    const double prev = RefillRateR;
    const double prevBytes = RefillRateBytesR;
    RefillRateR = Max(EffectiveRMin(), RefillRateR * factor);
    RefillRateBytesR = Max(EffectiveRMinBytes(), RefillRateBytesR * factor);
    if (RefillRateR < prev || RefillRateBytesR < prevBytes) {
        // Keep Wmax at the decayed level: the pre-cut peak is known-bad under this pressure.
        WmaxCount = Min(WmaxCount, RefillRateR);
        WmaxBytes = Min(WmaxBytes, RefillRateBytesR);
        Counters.OnDrainRateDecay();
        ClampTokensToSoftCap();
        PublishDrainGauges();
    }
}

void TFlowControlManager::MaybeApplyAnchor(TInstant now) {
    // Pull a runaway rate back toward measured throughput. This is deliberately not tied
    // to cohort completion: the cohort target is ceil(RefillRateR), so the more inflated
    // the rate is, the longer a cohort takes to close — exactly when the correction is
    // most needed. One step per GrowthPeriodSec, on its own clock.
    if (LastAnchorAt != TInstant::Zero() && (now - LastAnchorAt).SecondsFloat() < GrowthPeriodSec()) {
        return;
    }
    LastAnchorAt = now;
    const double capCount = AnchorMaxCount();
    const double capBytes = AnchorMaxBytes();
    bool gaveBack = false;
    if (RefillRateR > capCount) {
        RefillRateR = Max(capCount, RefillRateR * AnchorGiveBackFactor);
        gaveBack = true;
    }
    if (RefillRateBytesR > capBytes) {
        RefillRateBytesR = Max(capBytes, RefillRateBytesR * AnchorGiveBackFactor);
        gaveBack = true;
    }
    if (gaveBack) {
        WmaxCount = Min(WmaxCount, RefillRateR);
        WmaxBytes = Min(WmaxBytes, RefillRateBytesR);
        Counters.OnDrainAnchorGiveBack();
        ClampTokensToSoftCap();
        PublishDrainGauges();
    }
}

bool TFlowControlManager::IsQuietSinceHot(TInstant now) const {
    if (!HotNodes.empty()) {
        return false;
    }
    if (LastHotAt != TInstant::Zero() && (now - LastHotAt).SecondsFloat() < HotCooldownSec()) {
        return false;
    }
    if (LastOverloadOutcomeAt != TInstant::Zero() && (now - LastOverloadOutcomeAt).SecondsFloat() < HotCooldownSec()) {
        return false;
    }
    return true;
}

bool TFlowControlManager::CanGrowNow(TInstant now) const {
    if (!IsQuietSinceHot(now)) {
        return false;
    }
    if (LastGrowthAt != TInstant::Zero() && (now - LastGrowthAt).SecondsFloat() < GrowthPeriodSec()) {
        return false;
    }
    return true;
}

void TFlowControlManager::ClampTokensAfterReady() {
    Tokens = Min(Tokens, Max(1.0, std::ceil(RefillRateR * ReadyDumpFraction)));
    TokensBytes = Min(TokensBytes, Max(FrontWaiterBatchSize(), RefillRateBytesR * ReadyDumpFraction));
    // Restart the refill clock: the time the node spent hot must not be credited back as
    // tokens right after the clamp, or the drain loop immediately undoes it.
    const TInstant now = TActivationContext::Now();
    LastRefillAt = now;
    LastRefillBytesAt = now;
    PublishDrainGauges();
}

void TFlowControlManager::UpdateObservedThroughput(TInstant now, ui64 batchSize) {
    // Called only on the fast path (empty queue): the throughput we see here is what the
    // system currently sustains without FCM pushing back. EWMA it so a later empty→non-empty
    // transition can seed the drain rates from reality instead of a config nail.
    if (LastObserveAt == TInstant::Zero()) {
        LastObserveAt = now;
        return;
    }
    const double dt = (now - LastObserveAt).SecondsFloat();
    if (dt <= 0.001) {
        return;   // ignore sub-millisecond spacing (burst noise), avoids huge 1/dt spikes
    }
    const double alpha = 1.0 - std::exp(-dt / ObserveTauSec);
    const double instantCountRate = 1.0 / dt;
    const double instantBytesRate = static_cast<double>(batchSize) / dt;
    ObservedRateCount = alpha * instantCountRate + (1.0 - alpha) * ObservedRateCount;
    ObservedRateBytes = alpha * instantBytesRate + (1.0 - alpha) * ObservedRateBytes;
    LastObserveAt = now;
}

void TFlowControlManager::InitializeRatesFromObservation(ui64 firstBatchSize) {
    // The queue just went non-empty: the incoming rate has exceeded what we sustained on the
    // fast path. Seed the drain rates from the observed throughput (× safety factor), or more
    // cautiously if we saw any overload while the queue was still empty. With no observation
    // yet (cold start) keep the current seeded rate.
    // The fast-path EWMA is spacing-based (1/dt), so bursty arrivals read high; cap the seed
    // with the anchor, which is measured over whole windows while the queue was actually
    // busy. Cold start has no anchor yet and keeps the raw observation.
    // Seeding may only raise the rate in a quiet window: right after a hot episode the
    // decayed rate is the current verdict, and re-seeding from fast-path traffic would
    // hand the pressure right back.
    const double factor = ObservedOverload ? ObserveOverloadFactor : ObserveSafetyFactor;
    const bool mayRaise = IsQuietSinceHot(TActivationContext::Now());
    if (ObservedRateCount > 0.0) {
        const double cap = Min(EffectiveRMax(), AnchorMaxCount());
        const double seed = Min(cap, Max(EffectiveRMin(), ObservedRateCount * factor));
        if (mayRaise || seed < RefillRateR) {
            RefillRateR = seed;
        }
    }
    if (ObservedRateBytes > 0.0) {
        const double capBytes = Min(EffectiveRMaxBytes(), AnchorMaxBytes());
        const double seedBytes = Min(capBytes, Max(EffectiveRMinBytes(), ObservedRateBytes * factor));
        if (mayRaise || seedBytes < RefillRateBytesR) {
            RefillRateBytesR = seedBytes;
        }
    }
    // Seed to the soft one-cohort cap so the first waiter can drain immediately, then pace.
    // Raise the bytes seed to firstBatchSize so a single large first waiter is not stuck
    // waiting for tokens that the soft cap would otherwise refuse to accumulate.
    Tokens = Max(1.0, std::ceil(RefillRateR));
    TokensBytes = Max(Max(1.0, std::ceil(RefillRateBytesR)), static_cast<double>(firstBatchSize));
    LastRefillAt = TActivationContext::Now();
    LastRefillBytesAt = LastRefillAt;
    // A real observation seed replaces the recovery curve. If we have no EWMA yet (cold
    // empty→non-empty with no fast-path samples), keep any in-flight CUBIC epoch so a brief
    // empty queue between drain rounds does not throw away Wmax / KTarget progress.
    if (ObservedRateCount > 0.0 || ObservedRateBytes > 0.0) {
        WmaxCount = RefillRateR;
        WmaxBytes = RefillRateBytesR;
        CubicCCount = 0.0;
        CubicCBytes = 0.0;
        CubicEpochStart = TInstant::Zero();
    }
    ObservedOverload = false;
    Counters.OnObservationTransition();
    PublishDrainGauges();
}

void TFlowControlManager::MaybeMarkQueueEmpty() {
    if (!WasQueueEmpty && Waiters.empty()) {
        WasQueueEmpty = true;
        // Reopen the observation window. Do NOT reset the drain rates — AIMD keeps its learned
        // value; observation only re-seeds the *starting* rate at the next empty→non-empty edge.
        LastObserveAt = TInstant::Zero();
        ObservedRateCount = 0.0;
        ObservedRateBytes = 0.0;
        ObservedOverload = false;
    }
}

void TFlowControlManager::NoteCohortRelease() {
    // A cohort targets one full rate-worth of releases: exactly the "send RefillRateR
    // requests, then judge the result" round.
    if (!CohortOpen) {
        CohortOpen = true;
        CohortTarget = Max<ui64>(1, static_cast<ui64>(std::ceil(RefillRateR)));
        CohortReleased = 0;
        CohortOkCount = 0;
        CohortOverloadCount = 0;
    }
    ++CohortReleased;
}

void TFlowControlManager::NoteCohortOutcome(bool overloaded) {
    Counters.OnWriteOutcome(overloaded);
    // Outcomes drive growth/cuts together with HotNodes edges (see Handle NodeOverloadStatus).
    // Re-read bounds here: otherwise RMax/RMin/CUBIC knobs stay at construction-time seed.
    SyncDrainBounds();
    if (overloaded) {
        // Opens the same cooldown a hot node does: growth must wait for a quiet window.
        LastOverloadOutcomeAt = TActivationContext::Now();
    }
    if (!CohortOpen) {
        // Outcome of a write that was not part of an open cohort (e.g. a fast-path admit
        // with no queueing, or an in-flight write that finished between cohorts). Overload
        // still matters as a cut signal, but a *single* stray overload must not apply the
        // full AimdBeta: treat it as 1/notionalCohort of a dirty round so severity matches
        // in-cohort proportional cuts.
        if (overloaded) {
            const double notional = Max(1.0, std::ceil(RefillRateR));
            CutRateByOverloadFraction(1.0 / notional);
        }
        return;
    }
    if (overloaded) {
        ++CohortOverloadCount;
    } else {
        ++CohortOkCount;
    }
    if (CohortOkCount + CohortOverloadCount >= CohortTarget) {
        CloseCohort();
    }
}

double TFlowControlManager::CubicW(double c, double wmax, double tSec, double kTarget) {
    const double dt = tSec - kTarget;
    return c * dt * dt * dt + wmax;
}

void TFlowControlManager::EnsureCubicProbeEpoch(TInstant now) {
    // No recovery curve yet: treat current rates as Wmax and start in the probe region
    // (t >= K) so clean cohorts add ProbePercent * Wmax without a convex climb from zero.
    if (CubicEpochStart != TInstant::Zero()) {
        return;
    }
    WmaxCount = Max(WmaxCount, RefillRateR);
    WmaxBytes = Max(WmaxBytes, RefillRateBytesR);
    CubicCCount = 0.0;
    CubicCBytes = 0.0;
    const double k = Max(0.001, CubicRecoveryTargetSec);
    CubicEpochStart = now - TDuration::Seconds(k);
}

void TFlowControlManager::StartCubicEpoch(TInstant now, double prevCount, double newCount, double prevBytes, double newBytes) {
    WmaxCount = prevCount;
    WmaxBytes = prevBytes;
    const double k = Max(0.001, CubicRecoveryTargetSec);
    const double k3 = k * k * k;
    // C from the actual drop so W(0) == post-cut rate and W(K) == Wmax (handles partial cuts).
    CubicCCount = k3 > 0.0 ? Max(0.0, WmaxCount - newCount) / k3 : 0.0;
    CubicCBytes = k3 > 0.0 ? Max(0.0, WmaxBytes - newBytes) / k3 : 0.0;
    CubicEpochStart = now;
}

void TFlowControlManager::CloseCohort() {
    const ui64 total = CohortOkCount + CohortOverloadCount;
    const ui64 overloads = CohortOverloadCount;

    CohortOpen = false;
    CohortTarget = 0;
    CohortReleased = 0;
    CohortOkCount = 0;
    CohortOverloadCount = 0;

    if (!total) {
        return;
    }

    if (!overloads) {
        // Clean round: CUBIC recovery toward Wmax, then fractional probe above it.
        // A clean cohort is necessary but not sufficient — growth is additionally clocked
        // (one step per GrowthPeriodSec) and gated on a quiet window, because outcomes
        // arrive per shard write and would otherwise fire growth at the fan-out rate.
        const TInstant now = TActivationContext::Now();
        if (!CanGrowNow(now)) {
            Counters.OnDrainGrowthBlocked();
            PublishDrainGauges();
            return;
        }
        LastGrowthAt = now;

        // Never grow past what the system actually takes from us (MaybeApplyAnchor pulls
        // the rate back down when it is already above).
        const double capCount = Min(EffectiveRMax(), AnchorMaxCount());
        const double capBytes = Min(EffectiveRMaxBytes(), AnchorMaxBytes());

        EnsureCubicProbeEpoch(now);
        const double k = Max(0.001, CubicRecoveryTargetSec);
        const double t = Max(0.0, (now - CubicEpochStart).SecondsFloat());
        bool grew = false;

        if (RefillRateR < capCount) {
            const double prev = RefillRateR;
            if (t < k && CubicCCount > 0.0) {
                RefillRateR = Min(capCount, Max(EffectiveRMin(), CubicW(CubicCCount, WmaxCount, t, k)));
            } else {
                // Post-Wmax (or no recovery curve): lift to Wmax, then add ProbePercent of that peak.
                // Do not raise Wmax here — it is the last loss peak until the next meaningful cut.
                if (WmaxCount <= 0.0) {
                    WmaxCount = RefillRateR;
                }
                RefillRateR = Max(RefillRateR, Min(capCount, WmaxCount));
                if (CubicProbePercent > 0.0) {
                    RefillRateR = Min(capCount, RefillRateR + CubicProbePercent / 100.0 * WmaxCount);
                }
            }
            grew = grew || (RefillRateR > prev);
        }
        if (RefillRateBytesR < capBytes) {
            const double prevBytes = RefillRateBytesR;
            if (t < k && CubicCBytes > 0.0) {
                RefillRateBytesR = Min(capBytes, Max(EffectiveRMinBytes(), CubicW(CubicCBytes, WmaxBytes, t, k)));
            } else {
                if (WmaxBytes <= 0.0) {
                    WmaxBytes = RefillRateBytesR;
                }
                RefillRateBytesR = Max(RefillRateBytesR, Min(capBytes, WmaxBytes));
                if (CubicProbePercent > 0.0) {
                    RefillRateBytesR = Min(capBytes, RefillRateBytesR + CubicProbePercent / 100.0 * WmaxBytes);
                }
            }
            grew = grew || (RefillRateBytesR > prevBytes);
        }
        if (grew) {
            Counters.OnDrainRateGrow();
        }
        PublishDrainGauges();
        return;
    }

    Counters.OnDrainCohortAborted();
    CutRateByOverloadFraction(static_cast<double>(overloads) / static_cast<double>(total));
}

void TFlowControlManager::CutRateByOverloadFraction(double overloadFraction) {
    // Proportional multiplicative decrease: a single overloaded write out of many need
    // not halve the rate, while an all-overloaded round applies the full AimdBeta.
    // Both buckets are cut by the same effectiveBeta. A meaningful drop resets the CUBIC
    // epoch (Wmax = pre-cut rates); tiny out-of-cohort nicks do not.
    const double fraction = Min(1.0, Max(0.0, overloadFraction));
    if (fraction <= 0.0) {
        return;
    }
    const double effectiveBeta = 1.0 - fraction * (1.0 - AimdBeta);
    const double prev = RefillRateR;
    RefillRateR = Max(EffectiveRMin(), RefillRateR * effectiveBeta);

    const double effectiveBetaBytes = 1.0 - fraction * (1.0 - AimdBetaBytes);
    const double prevBytes = RefillRateBytesR;
    RefillRateBytesR = Max(EffectiveRMinBytes(), RefillRateBytesR * effectiveBetaBytes);

    const bool meaningfulCount = prev > 0.0 && (prev / Max(RefillRateR, EffectiveRMin())) >= MeaningfulCutRatio;
    const bool meaningfulBytes = prevBytes > 0.0 && (prevBytes / Max(RefillRateBytesR, EffectiveRMinBytes())) >= MeaningfulCutRatio;
    if (meaningfulCount || meaningfulBytes) {
        StartCubicEpoch(TActivationContext::Now(), prev, RefillRateR, prevBytes, RefillRateBytesR);
    }

    if (RefillRateR < prev || RefillRateBytesR < prevBytes) {
        Counters.OnDrainRateCut();
    }
    ClampTokensToSoftCap();
    PublishDrainGauges();
}

void TFlowControlManager::ClampTokensToSoftCap() {
    Tokens = Min(Max(1.0, std::ceil(RefillRateR)), Tokens);
    TokensBytes = Min(BytesSoftCap(), TokensBytes);
}

void TFlowControlManager::ScheduleDrainEligible(const TActorContext& ctx) {
    const TInstant now = TActivationContext::Now();
    AccrueBusyTime(now);
    CloseServedWindow(now);
    ApplyHotDecay(now);
    MaybeApplyAnchor(now);
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

        // Release only when BOTH buckets can pay: one count token AND enough bytes tokens
        // for this batch. This is what makes small batches gate on count and large batches
        // gate on bytes. BatchSize==0 (unknown) charges nothing to the bytes bucket.
        const bool countOk = Tokens >= 1.0;
        const bool bytesOk = TokensBytes >= static_cast<double>(waiter->BatchSize);
        if (!countOk || !bytesOk) {
            moreEligibleWithoutToken = true;
            break;
        }

        Tokens -= 1.0;
        TokensBytes -= static_cast<double>(waiter->BatchSize);
        waiter->DrainScheduled = true;
        waiter->TokenReserved = true;
        TDuration jitter = TFlowControlManagerServiceOperator::PickDrainJitter();
        // Never schedule past the waiter's deadline: jitter > remaining time makes every
        // DrainWaiter miss, refund, and retry until the helper times out (Drained=0).
        if (jitter != TDuration::Zero() && waiter->WaitDeadline > now) {
            const TDuration remaining = waiter->WaitDeadline - now;
            if (jitter >= remaining) {
                jitter = remaining > TDuration::MilliSeconds(1) ? remaining - TDuration::MilliSeconds(1) : TDuration::Zero();
            }
        }
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

    // While a node is hot nothing is drainable, so the pacing wakeup above never fires —
    // keep a slow tick alive to integrate the decay. Stop once both rates sit on their
    // floors: there is nothing left to decay and the timer would run forever.
    const bool hotTick = !HotNodes.empty() && (RefillRateR > EffectiveRMin() || RefillRateBytesR > EffectiveRMinBytes());
    if ((moreEligibleWithoutToken || hotTick) && !DrainWakeupScheduled) {
        DrainWakeupScheduled = true;
        // Wake when the *more depleted* bucket will next admit the front waiter: the time for
        // one count token, or the time to accrue the bytes deficit of the front waiter.
        // Cap the delay so a floor-rate / large-batch deficit cannot park ContinueDrain for
        // hours while the queue only ages into timeouts.
        constexpr ui64 MaxContinueDrainDelayMs = 1000;
        ui64 delayCountMs = 100;
        if (RefillRateR > 0) {
            delayCountMs = Max<ui64>(1, static_cast<ui64>(std::llround(1000.0 / RefillRateR)));
        }
        ui64 delayBytesMs = 1;
        if (RefillRateBytesR > 0) {
            for (const ui64 waiterId : WaitQueueOrder) {
                const auto* waiter = Waiters.FindPtr(waiterId);
                if (!waiter || waiter->DrainScheduled) {
                    continue;
                }
                if (now >= waiter->WaitDeadline || !IsAdmitAllowed(waiter->TabletIds)) {
                    continue;
                }
                const double deficit = static_cast<double>(waiter->BatchSize) - TokensBytes;
                if (deficit > 0) {
                    delayBytesMs = Max<ui64>(1, static_cast<ui64>(std::llround(1000.0 * deficit / RefillRateBytesR)));
                }
                break;   // first eligible waiter is the one the pacing timer must serve
            }
        }
        const ui64 delayMs = moreEligibleWithoutToken ? Min(MaxContinueDrainDelayMs, Max(delayCountMs, delayBytesMs)) : HotDecayTickMs;
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
    const ui64 batchSize = ev->Get()->GetBatchSize();
    const TDuration duration = TActivationContext::Now() - startedAt;

    if (IsAdmitAllowed(tabletIds) && !HasWaitersOnDestinations(tabletIds)) {
        Counters.OnAdmitAllowed(duration);
        // Fast path: the queue is empty, so this is the "observation window". Fold this
        // admit's spacing and size into the EWMA that will seed the drain rates when the
        // queue first fills.
        UpdateObservedThroughput(TActivationContext::Now(), batchSize);
        NoteAdmitted(TActivationContext::Now(), batchSize);
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

    // Empty → non-empty transition: incoming rate has outrun the fast-path throughput, so
    // seed the drain rates from what we just observed (before adding the first waiter).
    if (WasQueueEmpty) {
        WasQueueEmpty = false;
        InitializeRatesFromObservation(batchSize);
    }

    const ui64 waiterId = NextWaiterId++;
    TWaiter waiter;
    waiter.WaiterId = waiterId;
    waiter.Helper = ev->Sender;
    waiter.TabletIds = tabletIds;
    waiter.DestinationNodes = CollectDestinationNodes(tabletIds);
    waiter.WaitDeadline = waitDeadline;
    waiter.EnqueuedAt = now;
    waiter.BatchSize = batchSize;
    IncWaiterCounts(waiter.DestinationNodes);
    Waiters.emplace(waiterId, std::move(waiter));
    WaitQueueOrder.push_back(waiterId);
    Counters.OnWaitQueueEnqueue();
    PublishMapSizes();
    ctx.Send(ev->Sender, new TEvTryAdmitResult(EAdmitDecision::Wait, waiterId, waitDeadline));
    // Kick the drain loop: without this, a newly enqueued waiter only moves when some other
    // event (outcome / prior DrainWaiter / READY) happens to call ScheduleDrainEligible —
    // so Tokens/RefillRate can climb while the queue sits idle.
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvCancelWait::TPtr& ev, const TActorContext& ctx) {
    const ui64 waiterId = ev->Get()->GetWaiterId();
    // Only count against the wait-queue gauge/derivatives if the waiter was
    // actually present (EraseWaiter is a no-op for an unknown id).
    if (!Waiters.contains(waiterId)) {
        return;
    }
    if (ev->Get()->GetDeadlineExpired()) {
        Counters.OnWaitQueueTimedOut();
    } else {
        Counters.OnWaitQueueCancelled();
    }
    // EraseWaiter refunds a reserved drain token if any; re-run eligibility so another
    // waiter can take that budget (and so a timed-out DrainScheduled waiter does not stall
    // the drain chain until an unrelated outcome arrives).
    EraseWaiter(waiterId);
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvContinueDrain::TPtr& /*ev*/, const TActorContext& ctx) {
    DrainWakeupScheduled = false;
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvDrainWaiter::TPtr& ev, const TActorContext& ctx) {
    const ui64 waiterId = ev->Get()->GetWaiterId();
    auto* waiter = Waiters.FindPtr(waiterId);
    if (!waiter) {
        // Waiter was cancelled/timed out after Schedule(jitter); the reserved token was
        // already refunded in EraseWaiter. Still wake the drain loop — otherwise each
        // timed-out in-flight DrainWaiter permanently drops a wakeup.
        ScheduleDrainEligible(ctx);
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
        // Destination went hot after we reserved the token: try the next eligible waiter.
        ScheduleDrainEligible(ctx);
        return;
    }

    const TActorId helper = waiter->Helper;
    const TDuration waited = now - waiter->EnqueuedAt;
    const ui64 batchSize = waiter->BatchSize;
    // Token already reserved at schedule time; clear flag before erase so EraseWaiter does not refund.
    waiter->TokenReserved = false;
    EraseWaiter(waiterId);
    NoteAdmitted(now, batchSize);
    NoteCohortRelease();
    Counters.OnWaitQueueDrain(waited);
    Counters.OnAdmitAllowed(TDuration::Zero());
    Counters.OnDrainAllowed();
    ctx.Send(helper, new TEvTryAdmitResult(EAdmitDecision::Allow));
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvWriteOutcome::TPtr& ev, const TActorContext& ctx) {
    // Closed-loop feedback together with HotNodes edges in Handle(TEvNodeOverloadStatus).
    // If it arrives while the queue is empty (fast-path traffic), remember that the
    // observed throughput was already causing overload, so the next empty→non-empty
    // transition seeds the rates more cautiously.
    if (WasQueueEmpty && ev->Get()->GetOverloaded()) {
        ObservedOverload = true;
    }
    NoteCohortOutcome(ev->Get()->GetOverloaded());
    // A cohort close may have raised the rate, so re-evaluate eligibility.
    ScheduleDrainEligible(ctx);
}

void TFlowControlManager::Handle(const NFlowControl::TEvNodeOverloadStatus::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    const ui32 nodeId = record.GetNodeId();
    const ui64 generation = record.GetGeneration();

    switch (record.GetStatus()) {
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED: {
            // Gate admits to this node, and when HotNodes goes empty→non-empty also cut the
            // drain rate. Compaction overload is published here even when writes complete
            // successfully (no / high in-flight limit), so write outcomes alone never cut.
            const bool firstHot = HotNodes.empty();
            HotNodes[nodeId] = Max(HotNodes[nodeId], generation);
            Counters.OnStatusOverloaded();
            LastHotAt = TActivationContext::Now();
            if (WasQueueEmpty) {
                ObservedOverload = true;
            }
            if (firstHot) {
                SyncDrainBounds();
                // Drop an in-flight cohort: its clean OK outcomes are not a valid sample of
                // the post-cut rate under compaction pressure.
                CohortOpen = false;
                CohortTarget = 0;
                CohortReleased = 0;
                CohortOkCount = 0;
                CohortOverloadCount = 0;
                const double prev = RefillRateR;
                const double prevBytes = RefillRateBytesR;
                CutRateByOverloadFraction(1.0);
                // Compaction hot is not a discovered link limit — do not CUBIC-recover to
                // the pre-cut peak (with β≈0.8 that undoes the cut within KTarget and then
                // probes above it, recreating the sawtooth). Pin Wmax at the post-cut rate
                // so the next cool window only probes from here. Skip when RMin absorbed the
                // cut (rate unchanged) so a write-outcome CUBIC epoch is preserved.
                if (RefillRateR < prev || RefillRateBytesR < prevBytes) {
                    WmaxCount = RefillRateR;
                    WmaxBytes = RefillRateBytesR;
                    CubicCCount = 0.0;
                    CubicCBytes = 0.0;
                    const double k = Max(0.001, CubicRecoveryTargetSec);
                    CubicEpochStart = TActivationContext::Now() - TDuration::Seconds(k);
                }
            }
            // Start the decay integrator and keep a tick alive while hot.
            ScheduleDrainEligible(ctx);
            break;
        }
        case NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY: {
            auto it = HotNodes.find(nodeId);
            if (it != HotNodes.end() && generation >= it->second) {
                HotNodes.erase(it);
            }
            Counters.OnStatusReady();
            if (HotNodes.empty()) {
                // Hot → cool edge: waiters piled up while admits were gated, and tokens kept
                // accruing to the soft cap. Releasing all of that in one instant is exactly
                // what drives the next compaction overload, so trim the carried-over budget.
                LastHotAt = TActivationContext::Now();
                ClampTokensAfterReady();
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
