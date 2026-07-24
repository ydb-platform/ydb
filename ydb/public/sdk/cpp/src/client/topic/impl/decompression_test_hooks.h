#pragma once

#include <util/system/compiler.h>

#include <atomic>
#include <functional>

namespace NYdb::inline Dev::NTopic::NTesting {

#if !defined(NDEBUG) || defined(_san_enabled_)
#define YDB_TOPIC_DECOMPRESSION_TEST_HOOKS_ENABLED 1
#else
#define YDB_TOPIC_DECOMPRESSION_TEST_HOOKS_ENABLED 0
#endif

#if YDB_TOPIC_DECOMPRESSION_TEST_HOOKS_ENABLED

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

inline void InvokeCleanupPopHook() {
    if (auto* hook = CleanupPopHook().load(std::memory_order_acquire)) {
        (*hook)();
    }
}

inline void InvokeStartTasksPopHook() {
    if (auto* hook = StartTasksPopHook().load(std::memory_order_acquire)) {
        (*hook)();
    }
}

#else

inline void SetDecompressionTasksPopHooks(std::function<void()>, std::function<void()>) {
}

inline void ClearDecompressionTasksPopHooks() {
}

inline void InvokeCleanupPopHook() {
}

inline void InvokeStartTasksPopHook() {
}

#endif

} // namespace NYdb::inline Dev::NTopic::NTesting
