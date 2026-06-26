#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>

#include <memory>
#include <stop_token>

namespace NYdb {
namespace NCoordination {

class TCoordinationSessionPool;

struct TYdbLockException : public TYdbException {
    TYdbLockException(const std::string& message) : TYdbException(message) {}
};

struct TDistributedLockSettings {
    using TSelf = TDistributedLockSettings;

    FLUENT_SETTING(std::string, Path);
    FLUENT_SETTING(std::string, Name);
    FLUENT_SETTING_DEFAULT(TDuration, Timeout, TDuration::Seconds(5));
};

// Distributed exclusive lock backed by a YDB coordination semaphore.
// Provides lock/unlock for std::lock_guard-style usage with bounded acquire failures.
class TDistributedLock {
public:
    TDistributedLock(TClient& client, const TDistributedLockSettings& settings);
    ~TDistributedLock();

    TDistributedLock(const TDistributedLock&) = delete;
    TDistributedLock& operator=(const TDistributedLock&) = delete;
    TDistributedLock(TDistributedLock&&) = delete;
    TDistributedLock& operator=(TDistributedLock&&) = delete;

    // Throws TYdbLockException on session start failure, acquire timeout, transport
    // error, or contention timeout (timeout bounds the acquire wait).
    void lock();

    // Same as lock()
    void Acquire();

    // noexcept. Undefined behavior if called when the lock is not held (same as std::mutex).
    void unlock() noexcept;

    // Same as unlock()
    void Release() noexcept;

    // noexcept. Returns false on any failure without throwing.
    bool try_lock() noexcept;

    // Signals lock loss for the current hold; refreshed on successful acquire.
    std::stop_token GetStopToken() const;

private:
    struct TImpl;

    friend class TCoordinationSessionPool;

    TDistributedLock(TCoordinationSessionPool pool, const TDistributedLockSettings& settings);
    explicit TDistributedLock(std::unique_ptr<TImpl> impl);

private:
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NCoordination
} // namespace NYdb
