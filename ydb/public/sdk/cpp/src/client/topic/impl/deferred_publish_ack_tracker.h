#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/threading/future/future.h>

#include <util/system/spinlock.h>
#include <util/system/types.h>

#include <memory>
#include <unordered_map>

namespace NYdb::inline Dev::NTopic {

// Tracks in-flight deferred StreamWrite acks per int_publication_id for one driver state.
// Publish/Cancel wait here (same idea as topic+tx Precommit) before calling the server.
// Key is TDbDriverState* (opaque here to avoid internal headers in this header).
class TDeferredPublishAckTracker {
public:
    static TDeferredPublishAckTracker& For(const void* dbDriverState);

    void OnWrite(ui64 intPublicationId);
    void OnAck(ui64 intPublicationId);

    // Drop unacked writes from an aborted write session; fails WaitAllAcks if already waiting.
    void OnUnackedAbort(ui64 intPublicationId, ui64 unackedCount);

    NThreading::TFuture<TStatus> WaitAllAcks(ui64 intPublicationId);

private:
    struct TPublicationInfo {
        TSpinLock Lock;
        ui64 WriteCount = 0;
        ui64 AckCount = 0;
        bool WaitCalled = false;
        NThreading::TPromise<TStatus> AllAcksReceived;
    };

    using TPublicationInfoPtr = std::shared_ptr<TPublicationInfo>;

    TPublicationInfoPtr GetOrCreate(ui64 intPublicationId);
    void EraseIfReconciled(ui64 intPublicationId, const TPublicationInfoPtr& info);

    TSpinLock MapLock_;
    std::unordered_map<ui64, TPublicationInfoPtr> Publications_;
};

} // namespace NYdb::NTopic
