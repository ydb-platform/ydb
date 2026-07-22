#include "deferred_publish_ack_tracker.h"

#include "transaction.h"

#include <mutex>
#include <unordered_map>

namespace NYdb::inline Dev::NTopic {

namespace {

std::mutex& RegistryMutex() {
    static std::mutex mutex;
    return mutex;
}

std::unordered_map<const void*, std::shared_ptr<TDeferredPublishAckTracker>>& Registry() {
    static std::unordered_map<const void*, std::shared_ptr<TDeferredPublishAckTracker>> registry;
    return registry;
}

} // namespace

TDeferredPublishAckTracker& TDeferredPublishAckTracker::For(const void* dbDriverState) {
    Y_ABORT_UNLESS(dbDriverState);
    std::lock_guard guard(RegistryMutex());
    auto& entry = Registry()[dbDriverState];
    if (!entry) {
        entry = std::make_shared<TDeferredPublishAckTracker>();
    }
    return *entry;
}

TDeferredPublishAckTracker::TPublicationInfoPtr TDeferredPublishAckTracker::GetOrCreate(ui64 intPublicationId) {
    with_lock (MapLock_) {
        auto& ptr = Publications_[intPublicationId];
        if (!ptr) {
            ptr = std::make_shared<TPublicationInfo>();
        }
        return ptr;
    }
}

void TDeferredPublishAckTracker::EraseIfReconciled(ui64 intPublicationId, const TPublicationInfoPtr& info) {
    // Lock order: MapLock_ then info->Lock (never hold info->Lock while taking MapLock_).
    with_lock (MapLock_) {
        auto it = Publications_.find(intPublicationId);
        if (it == Publications_.end() || it->second != info) {
            return;
        }
        with_lock (info->Lock) {
            if (info->WriteCount != info->AckCount) {
                return;
            }
        }
        Publications_.erase(it);
    }
}

void TDeferredPublishAckTracker::OnWrite(ui64 intPublicationId) {
    auto info = GetOrCreate(intPublicationId);
    with_lock (info->Lock) {
        ++info->WriteCount;
    }
}

void TDeferredPublishAckTracker::OnAck(ui64 intPublicationId) {
    auto info = GetOrCreate(intPublicationId);
    bool tryErase = false;
    with_lock (info->Lock) {
        ++info->AckCount;
        Y_ABORT_UNLESS(info->AckCount <= info->WriteCount);
        if (info->WriteCount != info->AckCount) {
            return;
        }
        if (info->WaitCalled) {
            info->AllAcksReceived.TrySetValue(MakeCommitTransactionSuccess());
        }
        tryErase = true;
    }
    if (tryErase) {
        EraseIfReconciled(intPublicationId, info);
    }
}

void TDeferredPublishAckTracker::OnUnackedAbort(ui64 intPublicationId, ui64 unackedCount) {
    if (unackedCount == 0) {
        return;
    }

    auto info = GetOrCreate(intPublicationId);
    bool tryErase = false;
    with_lock (info->Lock) {
        Y_ABORT_UNLESS(info->WriteCount >= info->AckCount + unackedCount);
        if (info->WaitCalled) {
            info->AllAcksReceived.TrySetValue(MakeSessionExpiredError());
        }
        info->WriteCount -= unackedCount;
        tryErase = (info->WriteCount == info->AckCount);
    }
    if (tryErase) {
        EraseIfReconciled(intPublicationId, info);
    }
}

NThreading::TFuture<TStatus> TDeferredPublishAckTracker::WaitAllAcks(ui64 intPublicationId) {
    auto info = GetOrCreate(intPublicationId);
    bool tryErase = false;
    NThreading::TFuture<TStatus> future;
    with_lock (info->Lock) {
        if (info->WaitCalled) {
            return info->AllAcksReceived.GetFuture();
        }

        info->WaitCalled = true;
        info->AllAcksReceived = NThreading::NewPromise<TStatus>();
        if (info->WriteCount == info->AckCount) {
            info->AllAcksReceived.SetValue(MakeCommitTransactionSuccess());
            tryErase = true;
        }
        future = info->AllAcksReceived.GetFuture();
    }
    if (tryErase) {
        EraseIfReconciled(intPublicationId, info);
    }
    return future;
}

} // namespace NYdb::NTopic
