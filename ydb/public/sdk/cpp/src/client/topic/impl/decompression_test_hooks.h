#pragma once

#include <atomic>
#include <functional>

namespace NYdb::inline Dev::NTopic::NTesting {

// Test-only hooks to synchronize Cleanup/StartDecompressionTasks for race detection (TSan).
inline std::atomic<std::function<void()>*>& CleanupPopHook() {
    static std::atomic<std::function<void()>*> hook{nullptr};
    return hook;
}

inline std::atomic<std::function<void()>*>& StartTasksPopHook() {
    static std::atomic<std::function<void()>*> hook{nullptr};
    return hook;
}

inline void SetDecompressionTasksPopHooks(std::function<void()> beforeCleanupPop,
                                          std::function<void()> beforeStartTasksPop) {
    static std::function<void()> cleanupHook;
    static std::function<void()> startHook;
    cleanupHook = std::move(beforeCleanupPop);
    startHook = std::move(beforeStartTasksPop);
    CleanupPopHook().store(cleanupHook ? &cleanupHook : nullptr, std::memory_order_release);
    StartTasksPopHook().store(startHook ? &startHook : nullptr, std::memory_order_release);
}

inline void ClearDecompressionTasksPopHooks() {
    CleanupPopHook().store(nullptr, std::memory_order_release);
    StartTasksPopHook().store(nullptr, std::memory_order_release);
}

} // namespace NYdb::NTopic::NTesting
