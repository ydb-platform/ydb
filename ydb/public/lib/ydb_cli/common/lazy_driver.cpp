#include "lazy_driver.h"

#include <ydb/public/lib/ydb_cli/common/log.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NYdb::NConsoleClient {

TLazyDriver::TLazyDriver(TFactory factory, TDuration idleTimeout)
    : Factory_(std::move(factory))
    , IdleTimeout_(idleTimeout)
{
    Y_VALIDATE(Factory_, "TLazyDriver factory must not be empty");
    if (IdleTimeout_ != TDuration::Zero()) {
        Watcher_.emplace([this] { WatcherLoop(); });
    }
}

TLazyDriver::~TLazyDriver() {
    {
        TGuard<TMutex> guard(Mutex_);
        Shutdown_ = true;
    }
    CondVar_.Signal();
    if (Watcher_ && Watcher_->joinable()) {
        Watcher_->join();
    }
    TGuard<TMutex> guard(Mutex_);
    StopLocked(/* wait = */ true);
}

void TLazyDriver::Init() {
    TGuard<TMutex> guard(Mutex_);
    InitLocked();
}

void TLazyDriver::InitLocked() {
    if (!Driver_) {
        Driver_.emplace(Factory_());
        // Wake the watcher so it starts tracking the freshly created driver.
        CondVar_.Signal();
    }
    LastUseAt_ = TInstant::Now();
}

const TDriver& TLazyDriver::Get() {
    TGuard<TMutex> guard(Mutex_);
    InitLocked();
    return *Driver_;
}

bool TLazyDriver::IsInitialized() const noexcept {
    TGuard<TMutex> guard(Mutex_);
    return Driver_.has_value();
}

void TLazyDriver::Stop(bool wait) noexcept {
    TGuard<TMutex> guard(Mutex_);
    StopLocked(wait);
}

void TLazyDriver::StopLocked(bool wait) noexcept {
    if (!Driver_) {
        return;
    }
    try {
        Driver_->Stop(wait);
    } catch (const std::exception& ex) {
        YDB_CLI_LOG(Warning, "TLazyDriver::Stop failed: " << ex.what());
    } catch (...) {
        YDB_CLI_LOG(Warning, "TLazyDriver::Stop failed with unknown exception");
    }
    Driver_.reset();
}

void TLazyDriver::WatcherLoop() {
    TGuard<TMutex> guard(Mutex_);
    while (!Shutdown_) {
        if (!Driver_) {
            CondVar_.WaitI(Mutex_);
            continue;
        }
        const TInstant deadline = LastUseAt_ + IdleTimeout_;
        if (TInstant::Now() >= deadline) {
            StopLocked(/* wait = */ true);
            continue;
        }
        CondVar_.WaitD(Mutex_, deadline);
    }
}

} // namespace NYdb::NConsoleClient
