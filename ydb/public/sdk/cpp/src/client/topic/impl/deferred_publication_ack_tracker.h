#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/threading/future/future.h>

#include <util/system/spinlock.h>
#include <util/system/types.h>

#include <memory>

namespace NYdb::inline Dev::NTopic {

// Per-publication in-flight write ack state. Shared by TDeferredPublication copies.
class TDeferredPublicationAckState {
public:
    // Returns false if finalize (Publish/Cancel wait) has sealed this state.
    bool TryOnWrite();

    // Returns WriteCount and AckCount after applying this ack.
    std::pair<ui64, ui64> OnAck();

    // Drop unacked writes from an aborted write session; fails WaitAllAcks if already waiting.
    // Clears seal on wait failure so the client may write and finalize again.
    void OnUnackedAbort(ui64 unackedCount);

    // If there were no writes, returns success without sealing.
    // Otherwise seals and completes when WriteCount == AckCount.
    // Concurrent callers share one in-flight wait; a completed wait can be started again
    // for a later finalize attempt on the same handle.
    NThreading::TFuture<TStatus> WaitAllAcks();

    bool IsSealed() const;

private:
    mutable TSpinLock Lock_;
    ui64 WriteCount_ = 0;
    ui64 AckCount_ = 0;
    bool WaitCalled_ = false;
    bool Sealed_ = false;
    NThreading::TPromise<TStatus> AllAcksReceived_;
};

struct TDeferredPublication::TAccess {
    static const std::shared_ptr<TDeferredPublicationAckState>& AckState(const TDeferredPublication& publication);
};

} // namespace NYdb::NTopic
