#include "deferred_publication_ack_tracker.h"

#include "transaction.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>

namespace NYdb::inline Dev::NTopic {

namespace {

std::shared_ptr<TDeferredPublicationAckState> MakeAckState() {
    return std::make_shared<TDeferredPublicationAckState>();
}

TStatus MakeDeferredFinalizeAbortedError() {
    return TStatus(
        EStatus::SESSION_EXPIRED,
        NYdb::NIssue::TIssues{NYdb::NIssue::TIssue(
            "Cannot finalize deferred publication: associated write session was aborted")});
}

} // namespace

TDeferredPublication::TDeferredPublication()
    : AckState_(MakeAckState())
{
}

TDeferredPublication::TDeferredPublication(uint64_t intPublicationId)
    : IntPublicationId(intPublicationId)
    , AckState_(MakeAckState())
{
}

TDeferredPublication::TDeferredPublication(uint64_t intPublicationId, std::string extPublicationId)
    : IntPublicationId(intPublicationId)
    , ExtPublicationId(std::move(extPublicationId))
    , AckState_(MakeAckState())
{
}

TDeferredPublication::TDeferredPublication(const TDeferredPublication& other)
    : IntPublicationId(other.IntPublicationId)
    , ExtPublicationId(other.ExtPublicationId)
    , AckState_(other.AckState_)
{
}

TDeferredPublication::TDeferredPublication(TDeferredPublication&& other) noexcept
    : IntPublicationId(other.IntPublicationId)
    , ExtPublicationId(std::move(other.ExtPublicationId))
    , AckState_(std::move(other.AckState_))
{
    other.IntPublicationId = 0;
    other.ExtPublicationId.reset();
    // Leave AckState_ null: avoid heap traffic on write-pipeline variant moves.
    // A fresh empty state is created on the next TAccess::AckState call if needed.
}

TDeferredPublication& TDeferredPublication::operator=(const TDeferredPublication& other) {
    if (this != &other) {
        IntPublicationId = other.IntPublicationId;
        ExtPublicationId = other.ExtPublicationId;
        AckState_ = other.AckState_;
    }
    return *this;
}

TDeferredPublication& TDeferredPublication::operator=(TDeferredPublication&& other) noexcept {
    if (this != &other) {
        IntPublicationId = other.IntPublicationId;
        ExtPublicationId = std::move(other.ExtPublicationId);
        AckState_ = std::move(other.AckState_);
        other.IntPublicationId = 0;
        other.ExtPublicationId.reset();
        // See move constructor: no MakeAckState() for the moved-from object.
    }
    return *this;
}

const std::shared_ptr<TDeferredPublicationAckState>& TDeferredPublication::TAccess::AckState(
    const TDeferredPublication& publication)
{
    if (!publication.AckState_) {
        publication.AckState_ = MakeAckState();
    }
    return publication.AckState_;
}

bool TDeferredPublicationAckState::TryOnWrite() {
    with_lock (Lock_) {
        if (Sealed_) {
            return false;
        }
        ++WriteCount_;
        return true;
    }
}

std::pair<ui64, ui64> TDeferredPublicationAckState::OnAck() {
    std::optional<NThreading::TPromise<TStatus>> promiseToComplete;
    TStatus statusToSet = MakeCommitTransactionSuccess();
    ui64 writeCount = 0;
    ui64 ackCount = 0;
    with_lock (Lock_) {
        ++AckCount_;
        Y_ABORT_UNLESS(AckCount_ <= WriteCount_);
        writeCount = WriteCount_;
        ackCount = AckCount_;
        if (WriteCount_ == AckCount_ && WaitCalled_) {
            promiseToComplete = AllAcksReceived_;
            statusToSet = MakeCommitTransactionSuccess();
        }
    }
    if (promiseToComplete) {
        promiseToComplete->TrySetValue(statusToSet);
    }
    return {writeCount, ackCount};
}

void TDeferredPublicationAckState::OnUnackedAbort(ui64 unackedCount) {
    if (unackedCount == 0) {
        return;
    }

    std::optional<NThreading::TPromise<TStatus>> promiseToComplete;
    TStatus statusToSet = MakeDeferredFinalizeAbortedError();
    with_lock (Lock_) {
        Y_ABORT_UNLESS(WriteCount_ >= AckCount_ + unackedCount);
        if (WaitCalled_) {
            promiseToComplete = AllAcksReceived_;
            // Failed finalize: allow subsequent Write + Publish/Cancel on this handle.
            Sealed_ = false;
            WaitCalled_ = false;
        }
        WriteCount_ -= unackedCount;
    }
    if (promiseToComplete) {
        promiseToComplete->TrySetValue(statusToSet);
    }
}

NThreading::TFuture<TStatus> TDeferredPublicationAckState::WaitAllAcks() {
    std::optional<NThreading::TPromise<TStatus>> promiseToComplete;
    TStatus statusToSet = MakeCommitTransactionSuccess();
    NThreading::TFuture<TStatus> future;

    with_lock (Lock_) {
        if (WriteCount_ == 0) {
            // No local writes on this handle: do not seal, do not wait.
            return NThreading::MakeFuture(MakeCommitTransactionSuccess());
        }

        if (WaitCalled_) {
            auto existing = AllAcksReceived_.GetFuture();
            if (!existing.HasValue()) {
                return existing;
            }
            // Previous wait finished successfully; allow another finalize wait (e.g. repeat Publish).
        }

        Sealed_ = true;
        WaitCalled_ = true;
        AllAcksReceived_ = NThreading::NewPromise<TStatus>();
        future = AllAcksReceived_.GetFuture();
        if (WriteCount_ == AckCount_) {
            promiseToComplete = AllAcksReceived_;
        }
    }

    if (promiseToComplete) {
        promiseToComplete->TrySetValue(statusToSet);
    }
    return future;
}

bool TDeferredPublicationAckState::IsSealed() const {
    with_lock (Lock_) {
        return Sealed_;
    }
}

} // namespace NYdb::NTopic
