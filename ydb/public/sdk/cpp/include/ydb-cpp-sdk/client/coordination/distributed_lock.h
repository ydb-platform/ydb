#pragma once
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <stop_token>
namespace NYdb {
namespace NCoordination {
    struct TYdbLockException : public TYdbException {
        TYdbLockException(const std::string& message) : TYdbException(message) {}
    };
    // Distributed exclusive lock backed by a YDB coordination semaphore.
    // Satisfies BasicLockable (lock/unlock) for std::lock_guard; not a blocking Lockable.
    class TDistributedLock {
    public:
        TDistributedLock(TClient& client, std::string_view path, std::string_view name, TDuration timeout);
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
        // Signals lock loss for the current hold; refreshed on successful acquire — call again after re-lock.
        std::stop_token getStopToken() const;
    private:
        struct TImpl;
        std::unique_ptr<TImpl> impl_;
    };
}
}
