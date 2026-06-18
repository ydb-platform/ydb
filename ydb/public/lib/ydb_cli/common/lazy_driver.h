#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <util/datetime/base.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

#include <functional>
#include <memory>
#include <optional>
#include <thread>

namespace NYdb::NConsoleClient {

// Lazy wrapper around TDriver:
//   * Init() — create the driver via the factory if it has not been created yet.
//   * Get()  — same as Init() plus returns a reference to the driver.
//   * Stop() — stop the underlying driver (if any) and clear the wrapper;
//              the next Init()/Get() builds a fresh driver via the factory.
//
// Idle timeout: when a non-zero IdleTimeout is configured, a background watcher
// stops the driver after that period without any Init()/Get() call. Every
// Init()/Get() extends the deadline, so an actively used driver stays alive and
// is reused, while an idle one is released (and its discovery loop with it). The
// next Init()/Get() transparently recreates it.
//
// Thread-safe.
class TLazyDriver {
public:
    using TPtr = std::shared_ptr<TLazyDriver>;
    using TFactory = std::function<TDriver()>;

    explicit TLazyDriver(TFactory factory, TDuration idleTimeout = TDuration::Zero());
    ~TLazyDriver();

    void Init();
    const TDriver& Get();
    bool IsInitialized() const noexcept;
    void Stop(bool wait = true) noexcept;

private:
    void InitLocked();
    void StopLocked(bool wait) noexcept;
    void WatcherLoop();

    TFactory Factory_;
    TDuration IdleTimeout_;

    mutable TMutex Mutex_;
    TCondVar CondVar_;
    std::optional<TDriver> Driver_;
    TInstant LastUseAt_;
    bool Shutdown_ = false;
    std::optional<std::thread> Watcher_;
};

} // namespace NYdb::NConsoleClient
