#include "lazy_driver.h"

#include <ydb/public/lib/ydb_cli/common/log.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NYdb::NConsoleClient {

namespace {

void StopDriver(TDriver& driver, bool wait) noexcept {
    try {
        driver.Stop(wait);
    } catch (const std::exception& ex) {
        YDB_CLI_LOG(Warning, "TLazyDriver::Stop failed: " << ex.what());
    } catch (...) {
        YDB_CLI_LOG(Warning, "TLazyDriver::Stop failed with unknown exception");
    }
}

} // anonymous namespace

TLazyDriver::TLazyDriver(TFactory factory, TDuration idleTimeout)
    : Factory_(std::move(factory))
    , IdleTimeout_(idleTimeout)
{
    Y_VALIDATE(Factory_, "TLazyDriver factory must not be empty");
    if (IdleTimeout_ > TDuration::Zero()) {
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

TDriver TLazyDriver::Get() {
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
    if (Driver_) {
        StopDriver(*Driver_, wait);
        Driver_.reset();
    }
}

void TLazyDriver::WatcherLoop() {
    while (true) {
        std::optional<TDriver> dying;
        {
            TGuard<TMutex> guard(Mutex_);
            while (!Shutdown_ && !dying) {
                if (!Driver_) {
                    CondVar_.WaitI(Mutex_);
                    continue;
                }
                // Recomputed on every wakeup: a Get()/Init() since the last
                // sleep may have moved LastUseAt_ forward, so a driver in active
                // use is never stopped before a full idle period has elapsed.
                const TInstant deadline = LastUseAt_ + IdleTimeout_;
                if (TInstant::Now() >= deadline) {
                    // Hand the driver off and stop it outside the lock, so a
                    // concurrent Get() is not blocked by driver shutdown.
                    Driver_.swap(dying);
                    continue;
                }
                CondVar_.WaitD(Mutex_, deadline);
            }
            if (Shutdown_) {
                return;
            }
        }
        StopDriver(*dying, /* wait = */ true);
    }
}

} // namespace NYdb::NConsoleClient
