#pragma once

#include "message_stream_defs.h"

#include <library/cpp/threading/future/core/future.h>

#include <memory>
#include <variant>

namespace NFq {

// A control belongs to one assignment of one partition and is never rebound to
// another assignment, even if PartitionId is reused. Keep the event's control
// when acknowledging processed data. Calls on a session and all its controls
// must be externally serialized, including Close. Futures can complete on
// arbitrary threads; callbacks must schedule work rather than reenter methods.
// YDB: wraps an SDK partition session and its confirmation events. QYT: adapter
// ownership of a queue tablet; pull APIs alone do not provide this lifecycle.
// Kafka: one assignment epoch; serialize client operations on the consumer owner
// thread and invalidate controls on revocation/loss.
// https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html
class IMessageStreamPartitionControl {
public:
    virtual ~IMessageStreamPartitionControl() = default;

    // YDB: topic partition ID. QYT: tablet_index. Kafka: topic partition number.
    // The control identity, not this number alone, distinguishes assignments.
    virtual TMessageStreamPartitionId GetPartitionId() const = 0;
    // Confirm StartRequested once before reading data. startOffset is inclusive; nullopt
    // resumes at the consumer position with the session's OffsetResetPolicy. maxOffset is an optional
    // inclusive last-record bound. Neither argument persists consumer progress.
    // ReadFromWriteTime still applies. Unsupported bounds must be rejected explicitly.
    // YDB: SDK start confirmation. QYT: initialize the pull cursor. Kafka: seek
    // and enable delivery for the assignment. QYT/Kafka need adapter enforcement
    // of maxOffset; this call is not a native ownership acknowledgement.
    virtual void ConfirmStart(std::optional<ui64> startOffset, std::optional<ui64> maxOffset) = 0;
    // Confirm StopRequested after finishing/acknowledging outstanding work. Commands
    // after this confirmation or Closed cannot affect another assignment.
    // Late confirmations and status requests on a closed control are no-ops.
    // YDB: SDK stop confirmation. QYT: finish adapter/coordinator handover.
    // Kafka: release local assignment work within the rebalance protocol; this
    // does not create a right to commit after ownership has already been lost.
    virtual void ConfirmStop() = 0;
    // Confirm Exhausted after processing the parent's delivered records. YDB:
    // releases child partitions after split/merge. QYT/Kafka: adapter-specific
    // acknowledgement of permanent exhaustion, if supported. It does not commit
    // progress or revoke this control. Late confirmation after Closed is a no-op.
    virtual void ConfirmExhausted() = 0;
    // Requests an asynchronous PartitionStatus event while the control is live.
    // YDB: SDK status request. QYT/Kafka: gather available cursor/commit/bound
    // information; unsupported fields stay absent rather than being fabricated.
    virtual void RequestStatus() = 0;
    // Acknowledge exactly [startOffset, endOffset) for this assignment; endpoints
    // must satisfy startOffset < endOffset. Does not acknowledge earlier holes.
    // Adapters storing a cumulative offset advance only past processed records;
    // offsets skipped by the backend (e.g. compaction) are not unprocessed records.
    // true means accepted locally, NOT persisted. false means assignment closed
    // or revoked; the caller must allow replay. Invalid ranges throw InvalidArgument.
    // Async failures are reported through PartitionClosed/SessionClosed events;
    // PartitionStatus.CommittedOffset reports persisted progress when available.
    // YDB: SDK range commit. QYT/Kafka: track acknowledged ranges locally and
    // persist only a safe cumulative next offset. QYT advance_queue_consumer and
    // Kafka group commits do not natively store arbitrary disjoint ranges.
    // No backend transaction with application writes is represented here.
    virtual bool AcknowledgeRange(ui64 startOffset, ui64 endOffset) = 0;
};

// YDB: TDataReceivedEvent. QYT: pulled rows. Kafka: polled records.
// QYT/Kafka adapters group records by partition assignment; one backend response
// can yield several events. PartitionControl identifies that exact assignment.
struct TMessageStreamDataEvent {
    std::shared_ptr<IMessageStreamPartitionControl> PartitionControl;
    std::vector<TMessageStreamRecord> Records;
};

// YDB: TStartPartitionSessionEvent. QYT: adapter-created partition ownership.
// Kafka: adapter event after assignment (including explicit assign); confirmation
// gates local delivery, not acceptance of Kafka group assignment.
// CommittedOffset/EndOffset use the consumer-position/readable-bound semantics
// below; unknown offsets remain absent, including for consumerless reads.
struct TMessageStreamPartitionStartRequestedEvent {
    std::shared_ptr<IMessageStreamPartitionControl> PartitionControl;
    // nullopt means that consumer progress is unavailable or has never been stored.
    std::optional<ui64> CommittedOffset;
    std::optional<ui64> EndOffset;
};

// YDB: TStopPartitionSessionEvent. QYT: adapter/coordinator handover request.
// Kafka: graceful revocation notification; ConfirmStop cannot postpone Kafka
// revocation indefinitely. If ownership is already lost, report Closed directly.
struct TMessageStreamPartitionStopRequestedEvent {
    std::shared_ptr<IMessageStreamPartitionControl> PartitionControl;
};

// YDB: TEndPartitionSessionEvent for a partition ended by topology changes.
// QYT/Kafka: emit only if the adapter can prove permanent partition exhaustion.
// An empty pull/poll or reaching the current end offset is not exhaustion.
// Confirm through PartitionControl after processing this partition. Backend
// topology stays inside the adapter; this is not a rebalance event.
struct TMessageStreamPartitionExhaustedEvent {
    std::shared_ptr<IMessageStreamPartitionControl> PartitionControl;
};

// YDB: TPartitionSessionStatusEvent. QYT: adapter read cursor plus consumer/queue
// state. Kafka: read position, committed offset and visible end offset.
// These values may come from different observations; this is not an atomic snapshot.
struct TMessageStreamPartitionStatusEvent {
    std::shared_ptr<IMessageStreamPartitionControl> PartitionControl;
    // nullopt means that consumer progress is unavailable or has never been stored.
    std::optional<ui64> CommittedOffset;
    // Next position to be read by the session; not proof of application processing.
    // QYT/Kafka adapters must account for their own prefetch/delivery buffering.
    std::optional<ui64> ReadOffset;
    std::optional<ui64> EndOffset;
    // YDB: lower bound on write timestamps of future records. QYT/Kafka: absent
    // unless the adapter can establish that same guarantee. A Kafka offset high
    // watermark or the latest observed record timestamp is not such a guarantee.
    std::optional<TInstant> WriteTimeHighWatermark;
};

// YDB: TPartitionSessionClosedEvent. QYT: adapter ownership/pull lifetime ended.
// Kafka: assignment revoked/lost. All three invalidate this assignment control;
// this does not delete the partition or imply that the whole session has closed.
struct TMessageStreamPartitionClosedEvent {
    std::shared_ptr<IMessageStreamPartitionControl> PartitionControl;
};

// YDB: terminal SDK TSessionClosedEvent. QYT/Kafka: adapter terminal outcome
// after an unrecoverable error or completed shutdown as defined by the session.
// Normalize Status and retain native diagnostics in Issues. A temporary empty
// read or a recoverable backend error must not be mistaken for session closure.
struct TMessageStreamSessionClosedEvent {
    EMessageStreamStatus Status = EMessageStreamStatus::Unknown;
    NYql::TIssues Issues;
};

// Common event envelope for YDB/QYT/Kafka, not a backend wire event. Adapters
// translate native notifications and synthesize missing lifecycle events; SDK
// commit acknowledgements are internal and do not become record/data events.
using TMessageStreamReadEvent = std::variant<
    TMessageStreamDataEvent,
    TMessageStreamPartitionStartRequestedEvent,
    TMessageStreamPartitionStopRequestedEvent,
    TMessageStreamPartitionExhaustedEvent,
    TMessageStreamPartitionStatusEvent,
    TMessageStreamPartitionClosedEvent,
    TMessageStreamSessionClosedEvent>;

// Per assignment: StartRequested -> confirmation -> Data/Status -> optional Exhausted ->
// StopRequested -> confirmation -> Closed. Closed may also interrupt any state.
// Exhausted means exhaustion of this partition (e.g. a split), not revocation: pending
// data may still be acknowledged. StopRequested requests handover; Closed revokes it.
// Events for one assignment preserve backend order. Different assignments may
// interleave; an old control must never acknowledge data for a new assignment.
// A returned batch may already contain Closed for an earlier event's control;
// callers must tolerate rejected acknowledgements and late confirmations.
// YDB: SDK read session. QYT: adapter-owned pull loop and partition controls.
// Kafka: consumer polling loop plus assignment management. QYT/Kafka adapters
// supply this event protocol; their native APIs are not session-event equivalents.
class IMessageStreamReadSession {
public:
    virtual ~IMessageStreamReadSession() = default;

    // A readiness hint, not a reservation: GetEvents may return empty because of
    // filtered internal events or a zero budget. Ready after closure; stop polling
    // after SessionClosed or explicit Close. Close unblocks pending WaitEvent.
    // YDB: SDK readiness. QYT/Kafka: readiness of the adapter event queue;
    // polling/pulls must continue as required by the backend, independently of
    // whether the caller is currently waiting on this future.
    virtual NThreading::TFuture<void> WaitEvent() = 0;
    // SessionClosed is terminal, delivered at most once. No later events are
    // delivered; subsequent calls return empty. Respect both limits in settings.
    // Empty results do not mean EOF. Callers must not busy-loop on readiness.
    // YDB: convert SDK events and filter internal acknowledgements. QYT/Kafka:
    // drain converted pull/poll results and synthesized lifecycle notifications.
    virtual std::vector<TMessageStreamReadEvent> GetEvents(const TMessageStreamGetEventsSettings& settings) = 0;
    // Idempotent local shutdown: rejects further reading/acknowledgements and
    // discards buffered events. Completion means local resources are closed; it
    // does not promise to flush/persist acknowledgements. Shutdown errors surface
    // as an exception or failed future. No SessionClosed event is required here.
    // YDB: close the SDK session. QYT: stop pulls and release adapter ownership.
    // Kafka: stop polling/close the consumer; configure auto-commit and shutdown
    // callbacks so they cannot commit records merely fetched but not acknowledged.
    virtual NThreading::TFuture<void> Close() = 0;
    // Diagnostic identity: YDB SDK read-session ID; QYT/Kafka adapter session ID
    // when no native equivalent exists. Not a durable consumer identity, QYT
    // producer session ID, or Kafka group.id; never use it as an offset owner.
    virtual TString GetSessionId() const = 0;
};

} // namespace NFq
