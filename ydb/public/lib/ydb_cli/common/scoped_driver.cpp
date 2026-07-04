#include "scoped_driver.h"

#include <ydb/public/lib/ydb_cli/common/log.h>

namespace NYdb::NConsoleClient {

void StopDriver(TDriver& driver, bool wait) noexcept {
    try {
        driver.Stop(wait);
    } catch (const std::exception& ex) {
        YDB_CLI_LOG(Warning, "StopDriver failed: " << ex.what());
    } catch (...) {
        YDB_CLI_LOG(Warning, "StopDriver failed with unknown exception");
    }
}

TScopedDriver::TScopedDriver(TDriver driver)
    : Driver_(std::move(driver))
{}

TScopedDriver::TScopedDriver(TScopedDriver&& other) noexcept
    : Driver_(std::move(other.Driver_))
{
    other.Driver_.reset();
}

TScopedDriver& TScopedDriver::operator=(TScopedDriver&& other) noexcept {
    if (this != &other) {
        if (Driver_) {
            StopDriver(*Driver_, true);
        }
        Driver_ = std::move(other.Driver_);
        other.Driver_.reset();
    }
    return *this;
}

TScopedDriver::~TScopedDriver() {
    if (Driver_) {
        StopDriver(*Driver_, true);
    }
}

const TDriver& TScopedDriver::Get() const {
    return *Driver_;
}

TDriver& TScopedDriver::Get() {
    return *Driver_;
}

TScopedDriver::operator const TDriver&() const {
    return Get();
}

TScopedDriver::operator TDriver&() {
    return Get();
}

TScopedDriver::operator bool() const noexcept {
    return Driver_.has_value();
}

void TScopedDriver::Stop(bool wait) noexcept {
    if (Driver_) {
        StopDriver(*Driver_, wait);
        Driver_.reset();
    }
}

TDriver TScopedDriver::Release() {
    TDriver driver = std::move(*Driver_);
    Driver_.reset();
    return driver;
}

} // namespace NYdb::NConsoleClient
