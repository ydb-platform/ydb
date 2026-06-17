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
        // noexcept. Undefined behavior if called when the lock is not held (same as std::mutex).
        void unlock() noexcept;
        // noexcept. Returns false on any failure without throwing.
        bool try_lock() noexcept;
        // Becomes stopped when the distributed lock is lost (session expiry, or failed release
        // while holding the lock). Once stopped, remains stopped for the lifetime of this object.
        std::stop_token getStopToken() const;
    private:
        struct TImpl;
        std::unique_ptr<TImpl> impl_;
    };
}
}
