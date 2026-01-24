#pragma once

#include <util/datetime/base.h>
#include <library/cpp/threading/future/future.h>
#include <memory>
#include <atomic>

namespace NYdb::NConsoleClient {

// Shared stop flag type
using TStopFlag = std::shared_ptr<std::atomic<bool>>;

// Helper to create a stop flag
inline TStopFlag CreateStopFlag() {
    return std::make_shared<std::atomic<bool>>(false);
}

// Helper to signal stop
inline void SignalStop(TStopFlag& flag) {
    if (flag) {
        *flag = true;
    }
}

// Helper to wait for future with cancellation check
template<typename T>
inline bool WaitFor(const NThreading::TFuture<T>& future, const TStopFlag& stopFlag, TDuration timeout) {
    TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        if (stopFlag && *stopFlag) return false; // Cancelled
        if (future.Wait(TDuration::MilliSeconds(100))) return true; // Ready
    }
    return false; // Timeout
}

} // namespace NYdb::NConsoleClient
