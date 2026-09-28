#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_service.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_pb.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NKikimr::NColumnShard::NFlowControl {

enum EEvFlowControl {
    EvTryAdmit = EventSpaceBegin(TKikimrEvents::ES_FLOW_CONTROL_MANAGER),
    EvTryAdmitResult,
    EvCancelWait,
    EvDrainWaiter,
    EvContinueDrain,
    EvNodeOverloadStatus,
    EvTabletLocationUpdated,
    EvTabletLocationInvalidated,
    EvFireDelayedReject,
    EvWriteOutcome,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLOW_CONTROL_MANAGER), "expect EvEnd < EventSpaceEnd(ES_FLOW_CONTROL_MANAGER)");

class TEvTryAdmit: public NActors::TEventLocal<TEvTryAdmit, EvTryAdmit> {
    YDB_READONLY_DEF(TVector<ui64>, TabletIds);
    YDB_READONLY_DEF(TInstant, Deadline);
    YDB_READONLY_DEF(TDuration, OperationTimeout);
    // Deserialized Arrow batch memory size in bytes; feeds the bytes-rate token bucket.
    // 0 when unknown (older senders) → bytes bucket is inert for that request.
    YDB_READONLY(ui64, BatchSize, 0);

public:
    TEvTryAdmit(TVector<ui64> tabletIds, TInstant deadline, TDuration operationTimeout, ui64 batchSize = 0)
        : TabletIds(std::move(tabletIds))
        , Deadline(deadline)
        , OperationTimeout(operationTimeout)
        , BatchSize(batchSize)
    {
    }

    TInstant GetWaitDeadline() const {
        return TFlowControlManagerServiceOperator::ComputeWaitDeadline(Deadline, OperationTimeout);
    }

    TInstant GetDelayedRejectAt() const {
        return TFlowControlManagerServiceOperator::ComputeDelayedRejectAt(Deadline, OperationTimeout);
    }
};

class TEvTryAdmitResult: public NActors::TEventLocal<TEvTryAdmitResult, EvTryAdmitResult> {
    YDB_READONLY_DEF(EAdmitDecision, Decision);
    YDB_READONLY(ui64, WaiterId, 0);
    YDB_READONLY_DEF(TInstant, WaitDeadline);
    YDB_READONLY(ui64, RejectId, 0);   // For DelayedReject decision

public:
    explicit TEvTryAdmitResult(EAdmitDecision decision, ui64 waiterId = 0, TInstant waitDeadline = TInstant::Zero(), ui64 rejectId = 0)
        : Decision(decision)
        , WaiterId(waiterId)
        , WaitDeadline(waitDeadline)
        , RejectId(rejectId)
    {
    }
};

class TEvCancelWait: public NActors::TEventLocal<TEvCancelWait, EvCancelWait> {
    YDB_READONLY(ui64, WaiterId, 0);
    // True when the wait was aborted because the waiter's WaitDeadline expired,
    // false for a genuine client-side cancel. Lets FCM count timeouts distinctly.
    YDB_READONLY(bool, DeadlineExpired, false);

public:
    explicit TEvCancelWait(ui64 waiterId, bool deadlineExpired)
        : WaiterId(waiterId)
        , DeadlineExpired(deadlineExpired)
    {
    }
};

class TEvDrainWaiter: public NActors::TEventLocal<TEvDrainWaiter, EvDrainWaiter> {
    YDB_READONLY(ui64, WaiterId, 0);

public:
    explicit TEvDrainWaiter(ui64 waiterId)
        : WaiterId(waiterId)
    {
    }
};

// Wake FCM to continue paced wait-queue drain when tokens refill.
class TEvContinueDrain: public NActors::TEventLocal<TEvContinueDrain, EvContinueDrain> {};

struct TEvNodeOverloadStatus
    : public NActors::TEventPB<TEvNodeOverloadStatus, NKikimrTxColumnShard::TEvNodeOverloadStatus, EvNodeOverloadStatus> {
    TEvNodeOverloadStatus() = default;

    TEvNodeOverloadStatus(NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status, ui64 generation) {
        Record.SetStatus(status);
        Record.SetGeneration(generation);
    }
};

class TEvTabletLocationUpdated: public NActors::TEventLocal<TEvTabletLocationUpdated, EvTabletLocationUpdated> {
    YDB_READONLY(ui64, TabletId, 0);
    YDB_READONLY(ui32, NodeId, 0);

public:
    TEvTabletLocationUpdated(ui64 tabletId, ui32 nodeId)
        : TabletId(tabletId)
        , NodeId(nodeId)
    {
    }
};

class TEvTabletLocationInvalidated: public NActors::TEventLocal<TEvTabletLocationInvalidated, EvTabletLocationInvalidated> {
    YDB_READONLY(ui64, TabletId, 0);

public:
    explicit TEvTabletLocationInvalidated(ui64 tabletId)
        : TabletId(tabletId)
    {
    }
};

// Fire a delayed reject: send OVERLOADED to ReplyTo after a delay.
// This event is scheduled by FCM and sent to itself.
class TEvFireDelayedReject: public NActors::TEventLocal<TEvFireDelayedReject, EvFireDelayedReject> {
    YDB_READONLY(ui64, RejectId, 0);

public:
    explicit TEvFireDelayedReject(ui64 rejectId)
        : RejectId(rejectId)
    {
    }
};

// Terminal per-request write outcome, reported by TShardWriter once per shard write.
// This is the closed-loop feedback that drives drain-rate growth: FCM counts outcomes
// into a "cohort" and grows the rate only when a full cohort completed with no
// overload at all. Unlike the node-level TEvNodeOverloadStatus this is per request,
// immediate and exactly attributable, which is what makes timer-free growth possible.
//
// The outcome must be Overloaded if the write was EVER overloaded, even when a later retry
// succeeded: otherwise retry-by-subscription would launder overload into success and
// the rate would grow exactly when it should not.
class TEvWriteOutcome: public NActors::TEventLocal<TEvWriteOutcome, EvWriteOutcome> {
    YDB_READONLY(ui64, TabletId, 0);
    YDB_READONLY(ui32, NodeId, 0);
    YDB_READONLY(EWriteOutcome, Outcome, EWriteOutcome::Ok);
    YDB_READONLY(ui32, Retries, 0);

public:
    TEvWriteOutcome(ui64 tabletId, ui32 nodeId, EWriteOutcome outcome, ui32 retries)
        : TabletId(tabletId)
        , NodeId(nodeId)
        , Outcome(outcome)
        , Retries(retries)
    {
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
