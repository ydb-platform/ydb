#include "deferred_publish_ack_tracker.h"

#include "transaction.h"

namespace NYdb::inline Dev::NTopic {

void TDeferredPublicationAckState::OnWrite() {
    with_lock (Lock_) {
        ++WriteCount_;
    }
}

void TDeferredPublicationAckState::OnAck() {
    std::optional<NThreading::TPromise<TStatus>> promiseToComplete;
    TStatus statusToSet = MakeCommitTransactionSuccess();
    with_lock (Lock_) {
        ++AckCount_;
        Y_ABORT_UNLESS(AckCount_ <= WriteCount_);
        if (WriteCount_ == AckCount_ && WaitCalled_) {
            promiseToComplete = AllAcksReceived_;
            statusToSet = MakeCommitTransactionSuccess();
        }
    }
    if (promiseToComplete) {
        promiseToComplete->TrySetValue(statusToSet);
    }
}

void TDeferredPublicationAckState::OnUnackedAbort(ui64 unackedCount) {
    if (unackedCount == 0) {
        return;
    }

    std::optional<NThreading::TPromise<TStatus>> promiseToComplete;
    TStatus statusToSet = MakeSessionExpiredError();
    with_lock (Lock_) {
        Y_ABORT_UNLESS(WriteCount_ >= AckCount_ + unackedCount);
        if (WaitCalled_) {
            promiseToComplete = AllAcksReceived_;
        }
        WriteCount_ -= unackedCount;
        if (WriteCount_ == AckCount_ && WaitCalled_) {
            // Wait already failed above; nothing else to complete.
        }
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
        if (WaitCalled_) {
            auto existing = AllAcksReceived_.GetFuture();
            if (!existing.HasValue()) {
                // In-flight wait: Publish/Cancel callers share the same future.
                return existing;
            }
            // Previous wait finished (success or abort). Allow retry for the current backlog.
        }

        WaitCalled_ = true;
        AllAcksReceived_ = NThreading::NewPromise<TStatus>();
        future = AllAcksReceived_.GetFuture();
        if (WriteCount_ == AckCount_) {
            promiseToComplete = AllAcksReceived_;
        }
    }

    if (promiseToComplete) {
        promiseToComplete->SetValue(statusToSet);
    }
    return future;
}

} // namespace NYdb::NTopic
