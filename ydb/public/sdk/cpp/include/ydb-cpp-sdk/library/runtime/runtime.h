#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>

#include <chrono>
#include <functional>
#include <utility>

namespace NYdb::inline Dev {

// Process-wide services. Tasks and timers are independent of driver lifetimes.
class TRuntime final {
public:
    using TTask = std::function<void()>;

    // Submit to the shared background pool. Tasks may run concurrently and are
    // never invoked inline. The pool is independent of driver response executors.
    void Post(TTask task) const;

    // Submit to the background pool once the deadline expires.
    void Schedule(TDeadline deadline, TTask task) const;
    template <class Rep, class Period>
    void Schedule(std::chrono::duration<Rep, Period> delay, TTask task) const {
        Schedule(TDeadline::AfterDuration(delay), std::move(task));
    }
    void Schedule(TDuration delay, TTask task) const;

private:
    TRuntime() = default;
    TRuntime(const TRuntime&) = delete;
    TRuntime& operator=(const TRuntime&) = delete;

    friend TRuntime& GetRuntime();
};

// The runtime remains available until process exit, including before the first
// driver is constructed. It does not use the driver response executor.
TRuntime& GetRuntime();

} // namespace NYdb
