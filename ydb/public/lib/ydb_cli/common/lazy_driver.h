#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <functional>
#include <memory>
#include <optional>

namespace NYdb::NConsoleClient {

// Lazy wrapper around TDriver:
//   * Init() — create the driver via the factory if it has not been created yet.
//   * Get()  — same as Init() plus returns a reference to the driver. The
//              reference is invalidated by the next Stop() call, so callers
//              must use it within a single synchronous scope and never store
//              it across a Stop() boundary.
//   * Stop() — stop the underlying driver (if any) and clear the wrapper;
//              the next Init()/Get() builds a fresh driver via the factory.
//
// Not thread-safe; all access must be serialized by the caller.
class TLazyDriver {
public:
    using TPtr = std::shared_ptr<TLazyDriver>;
    using TFactory = std::function<TDriver()>;

    explicit TLazyDriver(TFactory factory);

    void Init();
    const TDriver& Get();
    bool IsInitialized() const noexcept;
    void Stop(bool wait = true) noexcept;

private:
    TFactory Factory_;
    std::optional<TDriver> Driver_;
};

} // namespace NYdb::NConsoleClient
