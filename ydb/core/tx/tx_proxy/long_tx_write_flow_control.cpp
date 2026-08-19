#include "long_tx_write_flow_control.h"
#include "upload_rows_common_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_counters.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_events.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_service.h>
#include <ydb/core/tx/data_events/shards_splitter.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NTxProxy {

using namespace NColumnShard;
using namespace NColumnShard::NFlowControl;

namespace {

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

// Admission needs the target tablet ids and the batch size, and the only thing that knows the
// sharding is the splitter — so the batch is partitioned here and then a second time in the write
// actor after admission. The duplicate pass is deliberate for now: threading a split result through
// TLongTxWrite would tie the flow control API to the shards-splitter output, and a delayed-reject
// or timed-out request throws that work away anyway. Revisit if this shows up in write-path CPU.
bool TryCollectTargetTablets(const TLongTxWrite& tx, TVector<ui64>& tabletIds, ui64& batchSize) {
    tabletIds.clear();
    batchSize = 0;

    // A bad navigate result or a table with no columnshard splitter are both ordinary here — the
    // write actor reports the former and the latter just means the table is not our business — so
    // neither is logged. A split that fails on data we are about to hand to that same write actor is
    // the one case worth a line, since it means the fail-open path is about to produce an error.
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
        AFL_WARN(NKikimrServices::LONG_TX_SERVICE)("event", "flow_control_split_failed")("path", tx.GetPath())(
            "status", Ydb::StatusIds::StatusCode_Name(initStatus.GetStatus()))("reason", initStatus.GetErrorMessage());
        return false;
    }

    // Deserialized batch memory size, already computed by TParsedBatchData's base ctor.
    batchSize = accessor.GetSize();

    for (const auto& [tabletId, _] : shardsSplitter->GetSplitData().GetShardsInfo()) {
        tabletIds.push_back(tabletId);
    }
    return true;
}

// Helper actors run on the caller's mailbox and have no handle on the FCM instance, so they
// resolve the counters group from AppData instead of from a process-global TIntrusivePtr whose
// refcount several TFlowControlManager constructors would race on in multi-node tests.
TIntrusivePtr<::NMonitoring::TDynamicCounters> CountersGroupOrNull() {
    if (!HasAppData()) {
        return nullptr;
    }
    return TFlowControlManagerServiceOperator::BuildCountersGroup(AppData()->Counters);
}

// The FCM fires a delayed reject after OperationTimeout * DelayedRejectTimeoutPercent / 100,
// i.e. never later than OperationTimeout. If that TEvCompleted is lost (FCM restart, mailbox
// drop) the helper would hold the client forever, so it arms its own fallback this much later.
constexpr TDuration DelayedRejectFallbackMargin = TDuration::Seconds(1);

// The FCM answers TEvTryAdmit on every branch of its handler without ever deferring, so this bound
// is about the service being alive at all, not about how loaded it is — nothing legitimate takes
// seconds. Deliberately far above any plausible mailbox delay so it can never pre-empt a real answer.
constexpr TDuration AdmitRpcTimeout = TDuration::Seconds(5);

constexpr TStringBuf OverloadedMessage = "destination node is overloaded";
constexpr TStringBuf QueueFullMessage = "destination node is overloaded; wait queue full";
constexpr TStringBuf TimeoutBeforeWriteMessage = "operation timeout exhausted before shard writes started";

// Runs on the caller's mailbox (BulkUpsert / DoLongTxWriteSameMailbox). Does data split + FCM admit
// RPC here, then starts TLongTxWriteInternal on the same mailbox (forceNoFlowControl).
// On Wait: hold until Allow (READY drain) or wait-deadline / RejectNow → OVERLOADED.
class TLongTxWriteFlowControlled: public NActors::TActorBootstrapped<TLongTxWriteFlowControlled> {
    // Each state arms its own timer, and a timer outlives the state that armed it: leaving
    // StateWaitAdmit does not cancel its wakeup. Tags let a stale one be ignored instead of being
    // mistaken for the current state's deadline.
    enum EWakeupTag : ui64 {
        WakeupAdmitTimeout = 1,
        WakeupWaitDeadline = 2,
        WakeupDelayedRejectFallback = 3,
    };

    TLongTxWrite Tx;
    TCSFlowControlManagerCounters Counters;
    TInstant StartedAt;
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

        StartedAt = TActivationContext::Now();
        TVector<ui64> tabletIds;
        ui64 batchSize = 0;
        const bool splitOk = TryCollectTargetTablets(Tx, tabletIds, batchSize);
        Counters.OnSplitFinished(TActivationContext::Now() - StartedAt);

        if (!splitOk) {
            // Fail open: without target tablets there is nothing to admit against, so hand the
            // request to the normal write actor, which produces the real navigate/split error.
            Counters.OnAdmitSkippedNoSplit();
            StartWrite(ctx);
            Finish(ctx);
            return;
        }

        WaitAdmitStartedAt = TActivationContext::Now();
        Counters.OnWaitingAdmitStart();
        // Deadline is the client's absolute cut-off, OperationTimeout its relative budget: the FCM
        // needs both, one for the wait-queue cut-off and one for the percentage-based windows.
        // FlagTrackDelivery turns "no FCM on this node" into an immediate TEvUndelivered instead of
        // silence; without it this is the one wait in this actor that nothing would ever end.
        ctx.Send(TFlowControlManagerServiceOperator::MakeServiceId(ctx.SelfID.NodeId()),
            std::make_unique<TEvTryAdmit>(std::move(tabletIds), Tx.GetDeadline(), Tx.GetOperationTimeout(), batchSize),
            NActors::IEventHandle::FlagTrackDelivery);
        Become(&TThis::StateWaitAdmit);
        ctx.Schedule(AdmitRpcTimeoutLeft(), new NActors::TEvents::TEvWakeup(WakeupAdmitTimeout));
    }

private:
    // clang-format off
    // TEvUndelivered means there is no FCM to admit against; TEvWakeup covers a reply that was
    // accepted but never came back. Both fail open, see FailOpenUnadmitted.
    STRICT_STFUNC(StateWaitAdmit,
                  HFunc(TEvTryAdmitResult, HandleAdmitResult)
                  HFunc(NActors::TEvents::TEvUndelivered, HandleAdmitUndelivered)
                  HFunc(NActors::TEvents::TEvWakeup, HandleAdmitTimeout)
    )
    STRICT_STFUNC(StateQueued,
                  HFunc(TEvTryAdmitResult, HandleQueuedResult)
                  HFunc(NActors::TEvents::TEvWakeup, HandleWaitDeadlineWakeup)
    )
    // Waiting for the FCM to fire the delayed reject; TEvWakeup is the lost-event fallback.
    STRICT_STFUNC(StateDelayedReject,
                  HFunc(NActors::TEvents::TEvCompleted, HandleDelayedRejectCompleted)
                  HFunc(NActors::TEvents::TEvWakeup, HandleDelayedRejectFallback)
    )
    // clang-format on

    // How long the admit round trip may take before the FCM is presumed absent. Capped by the
    // wait-queue cut-off so a request with a short budget is never held past the point where being
    // admitted would have stopped being useful.
    TDuration AdmitRpcTimeoutLeft() const {
        const TInstant waitDeadline = TFlowControlManagerServiceOperator::ComputeWaitDeadline(Tx.GetDeadline(), Tx.GetOperationTimeout());
        const TInstant firesAt = Min(StartedAt + AdmitRpcTimeout, waitDeadline);
        const TInstant now = TActivationContext::Now();
        return firesAt > now ? firesAt - now : TDuration::Zero();
    }

    void HandleAdmitUndelivered(NActors::TEvents::TEvUndelivered::TPtr& /*ev*/, const TActorContext& ctx) {
        FailOpenUnadmitted(ctx, "flow_control_manager_unavailable");
    }

    void HandleAdmitTimeout(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Tag != WakeupAdmitTimeout) {
            return;
        }
        FailOpenUnadmitted(ctx, "flow_control_admit_timeout");
    }

    // No admission decision is coming. Fail open into the plain write path, as the missing-split
    // branch does: the FCM never defers a reply, so silence means the service is gone rather than
    // busy, and rejecting the client on our own inability to ask would be inventing backpressure.
    void FailOpenUnadmitted(const TActorContext& ctx, TStringBuf reason) {
        Counters.OnWaitingAdmitFinish(TActivationContext::Now() - WaitAdmitStartedAt);
        Counters.OnAdmitSkippedUnavailable();
        AFL_WARN(NKikimrServices::LONG_TX_SERVICE)("event", "flow_control_admit_skipped")("reason", reason)("path", Tx.GetPath());
        StartWrite(ctx);
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
                ReplyOverloaded(ctx, OverloadedMessage);
                Finish(ctx);
                break;
            case EAdmitDecision::Wait:
                EnterQueued(ctx, ev->Get()->GetWaiterId(), ev->Get()->GetWaitDeadline());
                break;
            case EAdmitDecision::DelayedReject:
                // FCM will send TEvCompleted(OVERLOADED) after a delay.
                // Drop Arrow batch now to free memory, but stay alive to forward the response.
                Tx.DetachBatch();
                EnterDelayedReject(ctx);
                break;
        }
    }

    void EnterDelayedReject(const TActorContext& ctx) {
        Become(&TThis::StateDelayedReject);
        // Anchor to the client's absolute Deadline, not to StartedAt: the helper only boots after
        // navigate/split upstream, so StartedAt + OperationTimeout can land after the client has
        // already given up. The margin covers a lost FCM TEvCompleted past that cut-off.
        const TInstant fallbackAt = Tx.GetDeadline() + DelayedRejectFallbackMargin;
        const TInstant now = TActivationContext::Now();
        ctx.Schedule(fallbackAt > now ? fallbackAt - now : TDuration::Zero(),
            new NActors::TEvents::TEvWakeup(WakeupDelayedRejectFallback));
    }

    void HandleDelayedRejectCompleted(NActors::TEvents::TEvCompleted::TPtr& ev, const TActorContext& ctx) {
        // The FCM only knows this actor's id, not the client's TIssues, so the reason for the
        // delayed OVERLOADED has to be attached here — otherwise this one reject path would give
        // the client a bare status with no explanation.
        AddIssue(QueueFullMessage);
        ctx.Send(Tx.GetReplyTo(), ev->Release().Release());
        Finish(ctx);
    }

    void HandleDelayedRejectFallback(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Tag != WakeupDelayedRejectFallback) {
            return;   // the admit-timeout wakeup, outliving the state that armed it
        }
        ReplyOverloaded(ctx, QueueFullMessage);
        Finish(ctx);
    }

    void EnterQueued(const TActorContext& ctx, ui64 waiterId, TInstant waitDeadline) {
        WaiterId = waiterId;
        Queued = true;
        Become(&TThis::StateQueued);

        const TInstant now = TActivationContext::Now();
        if (waitDeadline <= now) {
            CancelAndReject(ctx);
            return;
        }
        ctx.Schedule(waitDeadline - now, new NActors::TEvents::TEvWakeup(WakeupWaitDeadline));
    }

    void HandleQueuedResult(TEvTryAdmitResult::TPtr& ev, const TActorContext& ctx) {
        // A queued waiter leaves the wait queue only through the drain path, which always answers
        // Allow; every other decision is taken before the waiter is enqueued.
        AFL_VERIFY(ev->Get()->GetDecision() == EAdmitDecision::Allow)("decision", (ui64)ev->Get()->GetDecision());
        Queued = false;
        StartWrite(ctx);
        Finish(ctx);
    }

    void HandleWaitDeadlineWakeup(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Tag != WakeupWaitDeadline) {
            return;   // the admit-timeout wakeup, outliving the state that armed it
        }
        // StateQueued is entered only from EnterQueued, which sets Queued and arms this wakeup, and
        // every path that clears Queued also passes away.
        AFL_VERIFY(Queued);
        CancelAndReject(ctx);
    }

    void CancelAndReject(const TActorContext& ctx) {
        if (Queued && WaiterId) {
            ctx.Send(TFlowControlManagerServiceOperator::MakeServiceId(ctx.SelfID.NodeId()),
                std::make_unique<TEvCancelWait>(WaiterId, /*deadlineExpired=*/true));
        }
        Queued = false;
        ReplyOverloaded(ctx, OverloadedMessage);
        Finish(ctx);
    }

    void StartWrite(const TActorContext& ctx) {
        // Time already burned in the admit RPC and the wait queue belongs to the client's budget,
        // so the write gets what is left of it rather than a fresh full timeout. Callers always
        // own Finish()/PassAway — this method must not call them, or an early return double-decrements
        // RequestsInFlight and double-PassesAway.
        const TInstant now = TActivationContext::Now();
        const TDuration untilDeadline = Tx.GetDeadline() > now ? Tx.GetDeadline() - now : TDuration::Zero();
        // StartedAt is helper-boot time (after navigate/split upstream), so OperationTimeout - elapsed
        // can still exceed Deadline - now. Cap by the absolute cut-off.
        const TDuration elapsed = now - StartedAt;
        TDuration remainingTimeout = Tx.GetOperationTimeout() > elapsed ? Tx.GetOperationTimeout() - elapsed : TDuration::Zero();
        remainingTimeout = Min(remainingTimeout, untilDeadline);
        if (remainingTimeout == TDuration::Zero()) {
            // Matching TLongTxWriteBase: no budget left is a timeout, not backpressure.
            ReplyTimeout(ctx);
            return;
        }
        // forceNoFlowControl stops the write from re-entering flow control here.
        DoLongTxWriteSameMailbox(ctx, Tx.GetReplyTo(), Tx.GetLongTxId(), Tx.GetDedupId(), Tx.GetDatabaseName(), Tx.GetPath(),
            Tx.GetNavigateResult(), Tx.GetBatch(), Tx.GetIssues(), Tx.GetUserCtx(), /*forceNoFlowControl=*/true, Tx.GetDeadline(),
            remainingTimeout);
    }

    void AddIssue(TStringBuf message) {
        // Every entry point into the write path owns an issues container; without one the client
        // would silently lose the reject reason.
        AFL_VERIFY(Tx.GetIssues());
        Tx.GetIssues()->AddIssue(NYql::TIssue(TString(message)));
    }

    void ReplyOverloaded(const TActorContext& ctx, TStringBuf message) {
        AddIssue(message);
        ctx.Send(Tx.GetReplyTo(), new NActors::TEvents::TEvCompleted(0, Ydb::StatusIds::OVERLOADED));
    }

    void ReplyTimeout(const TActorContext& ctx) {
        AddIssue(TimeoutBeforeWriteMessage);
        ctx.Send(Tx.GetReplyTo(), new NActors::TEvents::TEvCompleted(0, Ydb::StatusIds::TIMEOUT));
    }

    void Finish(const TActorContext& /*ctx*/) {
        Counters.OnRequestFinish();
        PassAway();
    }
};

}   // namespace

void StartLongTxWriteFlowControlled(const TActorContext& ctx, TLongTxWrite&& longTxWrite) {
    // Keep split + LongTx write on the caller's mailbox (BulkUpsert upload actor).
    ctx.RegisterWithSameMailbox(new TLongTxWriteFlowControlled(std::move(longTxWrite)));
}

}   // namespace NKikimr::NTxProxy
