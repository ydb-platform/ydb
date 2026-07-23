#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/threading/future/future.h>

#include <util/system/spinlock.h>
#include <util/system/types.h>

namespace NYdb::inline Dev::NTopic {

// Per-publication in-flight write ack state. Shared by TDeferredPublication (hot) copies.
class TDeferredPublicationAckState {
public:
    void OnWrite();
    void OnAck();

    // Drop unacked writes from an aborted write session; fails WaitAllAcks if already waiting.
    void OnUnackedAbort(ui64 unackedCount);

    // Completes when WriteCount == AckCount. Safe to call with no prior writes.
    // If a previous wait already completed, starts a new wait for the current backlog.
    // Concurrent callers share one in-flight wait until it completes.
    NThreading::TFuture<TStatus> WaitAllAcks();

private:
    TSpinLock Lock_;
    ui64 WriteCount_ = 0;
    ui64 AckCount_ = 0;
    bool WaitCalled_ = false;
    NThreading::TPromise<TStatus> AllAcksReceived_;
};

} // namespace NYdb::NTopic
