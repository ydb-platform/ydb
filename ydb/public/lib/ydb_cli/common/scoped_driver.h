#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <optional>

namespace NYdb::NConsoleClient {

// Gracefully stops a driver; used by TScopedDriver and TLazyDriver.
void StopDriver(TDriver& driver, bool wait = true) noexcept;

// Owns a TDriver and calls Stop(true) on destruction or move-assignment.
// Implicitly converts to const TDriver& / TDriver& for SDK client constructors.
class TScopedDriver {
public:
    TScopedDriver() = default;

    explicit TScopedDriver(TDriver driver);

    TScopedDriver(TScopedDriver&& other) noexcept;
    TScopedDriver& operator=(TScopedDriver&& other) noexcept;

    TScopedDriver(const TScopedDriver&) = delete;
    TScopedDriver& operator=(const TScopedDriver&) = delete;

    ~TScopedDriver();

    const TDriver& Get() const;
    TDriver& Get();

    operator const TDriver&() const;
    operator TDriver&();

    explicit operator bool() const noexcept;

    void Stop(bool wait = true) noexcept;

    // Transfers ownership without stopping the driver.
    TDriver Release();

private:
    std::optional<TDriver> Driver_;
};

} // namespace NYdb::NConsoleClient
