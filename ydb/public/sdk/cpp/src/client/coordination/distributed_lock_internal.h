#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace NYdb {
namespace NCoordination {
namespace NPrivate {

class TPooledSessionState {
public:
    void SetOnLost(std::function<void()> onLost) {
        std::lock_guard guard(Lock_);
        OnLost_ = std::move(onLost);
    }

    void ClearOnLost() {
        std::lock_guard guard(Lock_);
        OnLost_ = {};
    }

    void NotifyLost() {
        std::function<void()> onLost;
        {
            std::lock_guard guard(Lock_);
            onLost = OnLost_;
        }
        if (onLost) {
            onLost();
        }
    }

private:
    std::mutex Lock_;
    std::function<void()> OnLost_;
};

struct TSessionLease {
    TSession Session;
    std::shared_ptr<TPooledSessionState> PooledState;
};

class ISessionSource {
public:
    virtual ~ISessionSource() = default;
    virtual bool Checkout(TSessionLease& lease, std::function<void()> onLost) noexcept = 0;
    virtual bool Replace(TSessionLease& lease, std::function<void()> onLost) noexcept = 0;
    virtual void Checkin(TSessionLease&& lease, bool close) noexcept = 0;
    virtual bool IsPersistent() const noexcept = 0;
};

inline void CloseSession(TSession& session) noexcept {
    try {
        if (session) {
            session.Close().GetValueSync();
        }
    } catch (...) {
    }
    session = {};
}

} // namespace NPrivate

struct TDistributedLock::TImpl {
    enum class EAcquireResult {
        Acquired,
        NotAcquired,
        NoSession,
        Failed,
    };

    std::stop_token GetStopToken() const;
    void lock();
    void unlock() noexcept;
    bool try_lock() noexcept;

    TImpl(std::unique_ptr<NPrivate::ISessionSource> source,
        const TDistributedLockSettings& lockSettings, bool startSession);
    ~TImpl();

private:
    std::function<void()> MakeOnLost();
    void RequestStop();
    void ResetStopSource();
    bool TryStartSession() noexcept;
    bool EnsureSession() noexcept;
    bool ResetSession(bool lockLost = false) noexcept;
    void ReturnSession() noexcept;
    void ReturnSessionIfNeeded() noexcept;
    EAcquireResult TryAcquire() noexcept;

private:
    mutable std::mutex StopSourceLock_;
    std::stop_source StopSource_;
    std::atomic<bool> SuppressLockLossNotify_{false};
    std::atomic<bool> SessionReady_{false};
    NPrivate::TSessionLease Session_;
    TAcquireSemaphoreSettings Settings_;
    std::string Name_;
    TDuration Timeout_;
    std::unique_ptr<NPrivate::ISessionSource> SessionSource_;
    bool Locked_ = false;
};

} // namespace NCoordination
} // namespace NYdb
