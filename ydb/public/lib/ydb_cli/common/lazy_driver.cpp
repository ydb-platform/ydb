#include "lazy_driver.h"

#include <ydb/public/lib/ydb_cli/common/log.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NYdb::NConsoleClient {

TLazyDriver::TLazyDriver(TFactory factory)
    : Factory_(std::move(factory))
{
    Y_VALIDATE(Factory_, "TLazyDriver factory must not be empty");
}

void TLazyDriver::Init() {
    if (!Driver_) {
        Driver_.emplace(Factory_());
    }
}

const TDriver& TLazyDriver::Get() {
    Init();
    return *Driver_;
}

bool TLazyDriver::IsInitialized() const noexcept {
    return Driver_.has_value();
}

void TLazyDriver::Stop(bool wait) noexcept {
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

} // namespace NYdb::NConsoleClient
