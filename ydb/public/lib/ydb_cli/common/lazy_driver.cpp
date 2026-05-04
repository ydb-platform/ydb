#include "lazy_driver.h"

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

void TLazyDriver::Stop(bool wait) {
    if (Driver_) {
        Driver_->Stop(wait);
        Driver_.reset();
    }
}

} // namespace NYdb::NConsoleClient
