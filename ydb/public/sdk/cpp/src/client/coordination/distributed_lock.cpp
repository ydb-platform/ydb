#include "distributed_lock_internal.h"

#include <util/system/hostname.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

namespace NYdb {
namespace NCoordination {

using NPrivate::ISessionSource;
using NPrivate::TSessionLease;

namespace {

TSessionSettings MakeLockSessionSettings(TDuration timeout, std::function<void()> onLost) {
    return TSessionSettings()
        .Timeout(timeout)
        .OnStateChanged([onLost](ESessionState state) {
            if (state == ESessionState::EXPIRED) {
                onLost();
            }
        })
        .OnStopped(std::move(onLost));
}

class TClientSessionSource final : public ISessionSource {
public:
    TClientSessionSource(TClient& client, std::string path, TDuration timeout)
        : Client_(client)
        , Path_(std::move(path))
        , Timeout_(timeout)
    {
    }

    bool Checkout(TSessionLease& lease, std::function<void()> onLost) noexcept override {
        return StartSession(lease, std::move(onLost));
    }

    bool Replace(TSessionLease& lease, std::function<void()> onLost) noexcept override {
        NPrivate::CloseSession(lease.Session);
        return StartSession(lease, std::move(onLost));
    }

    void Checkin(TSessionLease&& lease, bool close) noexcept override {
        if (close || lease.Session) {
            NPrivate::CloseSession(lease.Session);
        }
    }

    bool IsPersistent() const noexcept override {
        return true;
    }

private:
    bool StartSession(TSessionLease& lease, std::function<void()> onLost) noexcept {
        try {
            auto result = Client_.StartSession(Path_, MakeLockSessionSettings(Timeout_, std::move(onLost))).GetValueSync();
            if (!result.IsSuccess()) {
                lease = {};
                return false;
            }
            lease.Session = result.GetResult();
            lease.PooledState.reset();
            return true;
        } catch (...) {
            lease = {};
            return false;
        }
    }

    TClient Client_;
    std::string Path_;
    TDuration Timeout_;
};

} // namespace

std::stop_token TDistributedLock::TImpl::GetStopToken() const {
    std::lock_guard guard(StopSourceLock_);
    return StopSource_.get_token();
}

std::function<void()> TDistributedLock::TImpl::MakeOnLost() {
    return [this] {
        SessionReady_.store(false);
        if (!SuppressLockLossNotify_.load()) {
            RequestStop();
        }
    };
}

void TDistributedLock::TImpl::RequestStop() {
    std::lock_guard guard(StopSourceLock_);
    StopSource_.request_stop();
}

void TDistributedLock::TImpl::ResetStopSource() {
    std::lock_guard guard(StopSourceLock_);
    StopSource_ = std::stop_source{};
}

bool TDistributedLock::TImpl::TryStartSession() noexcept {
    if (!SessionSource_->Checkout(Session_, MakeOnLost())) {
        SessionReady_.store(false);
        return false;
    }
    SessionReady_.store(true);
    return true;
}

bool TDistributedLock::TImpl::EnsureSession() noexcept {
    return SessionReady_.load() || TryStartSession();
}

bool TDistributedLock::TImpl::ResetSession(bool lockLost) noexcept {
    if (lockLost) {
        RequestStop();
    }
    SuppressLockLossNotify_.store(true);
    SessionReady_.store(false);
    if (!SessionSource_->Replace(Session_, MakeOnLost())) {
        SuppressLockLossNotify_.store(false);
        return false;
    }
    SessionReady_.store(true);
    SuppressLockLossNotify_.store(false);
    return true;
}

void TDistributedLock::TImpl::ReturnSession() noexcept {
    if (!Session_.Session) {
        return;
    }
    SuppressLockLossNotify_.store(true);
    SessionReady_.store(false);
    SessionSource_->Checkin(std::move(Session_), false);
    Session_ = {};
    SuppressLockLossNotify_.store(false);
}

void TDistributedLock::TImpl::ReturnSessionIfNeeded() noexcept {
    if (!SessionSource_->IsPersistent()) {
        ReturnSession();
    }
}

TDistributedLock::TImpl::EAcquireResult TDistributedLock::TImpl::TryAcquire() noexcept try {
    if (!EnsureSession()) {
        return EAcquireResult::NoSession;
    }

    auto acquireFuture = Session_.Session.AcquireSemaphore(Name_, Settings_);
    if (!acquireFuture.Wait(Timeout_)) {
        ResetSession();
        ReturnSessionIfNeeded();
        return EAcquireResult::Failed;
    }

    const auto result = acquireFuture.GetValue();
    if (!result.IsSuccess()) {
        ResetSession();
        ReturnSessionIfNeeded();
        return EAcquireResult::Failed;
    }

    if (!result.GetResult()) {
        ReturnSessionIfNeeded();
        return EAcquireResult::NotAcquired;
    }

    ResetStopSource();
    Locked_ = true;
    return EAcquireResult::Acquired;
} catch (...) {
    ReturnSessionIfNeeded();
    return EAcquireResult::Failed;
}

TDistributedLock::TImpl::~TImpl() {
    SuppressLockLossNotify_.store(true);
    if (Session_.Session) {
        SessionSource_->Checkin(std::move(Session_), Locked_ || SessionSource_->IsPersistent());
    }
}

TDistributedLock::TImpl::TImpl(
        std::unique_ptr<ISessionSource> source,
        const TDistributedLockSettings& lockSettings,
        bool startSession)
    : Name_(lockSettings.Name_)
    , Timeout_(lockSettings.Timeout_)
    , SessionSource_(std::move(source))
{
    Settings_ = TAcquireSemaphoreSettings()
        .Exclusive()
        .Data(FQDNHostName())
        .Ephemeral()
        .Timeout(Timeout_);
    if (startSession && !TryStartSession()) {
        throw TYdbLockException("Failed to start session");
    }
}

bool TDistributedLock::TImpl::try_lock() noexcept {
    return TryAcquire() == EAcquireResult::Acquired;
}

void TDistributedLock::TImpl::lock() {
    switch (TryAcquire()) {
        case EAcquireResult::Acquired:
            return;
        case EAcquireResult::NoSession:
            throw TYdbLockException("Failed to start session");
        case EAcquireResult::NotAcquired:
        case EAcquireResult::Failed:
            throw TYdbLockException("Failed to acquire semaphore");
    }
}

void TDistributedLock::TImpl::unlock() noexcept try {
    auto releaseFuture = Session_.Session.ReleaseSemaphore(Name_);
    if (releaseFuture.Wait(Timeout_)) {
        const auto result = releaseFuture.GetValue();
        if (!result.IsSuccess() || !result.GetResult()) {
            ResetSession(true);
        }
    } else {
        ResetSession(true);
    }
    Locked_ = false;
    ReturnSessionIfNeeded();
} catch (...) {
    ResetSession(true);
    Locked_ = false;
    ReturnSessionIfNeeded();
}

TDistributedLock::TDistributedLock(TClient& client, const TDistributedLockSettings& settings)
    : TDistributedLock(std::make_unique<TImpl>(
        std::make_unique<TClientSessionSource>(client, settings.Path_, settings.Timeout_),
        settings,
        true))
{
}

TDistributedLock::TDistributedLock(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{
}

    TDistributedLock TClient::CreateDistributedLock(const TDistributedLockSettings& settings) {
        return TDistributedLock(*this, settings);
    }

TDistributedLock::~TDistributedLock() = default;

void TDistributedLock::lock() {
    Impl_->lock();
}

void TDistributedLock::unlock() noexcept {
    Impl_->unlock();
}

void TDistributedLock::Acquire() {
    Impl_->lock();
}

void TDistributedLock::Release() noexcept {
    Impl_->unlock();
}

bool TDistributedLock::try_lock() noexcept {
    return Impl_->try_lock();
}

std::stop_token TDistributedLock::GetStopToken() const {
    return Impl_->GetStopToken();
}
}
}
