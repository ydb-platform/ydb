#pragma once

#include <util/system/types.h>
#include <functional>
#include <memory>

namespace NYdb {
class IClientImplCommon;
}

namespace NYdb::NRetry {

struct TBackoffSettings;
ui32 CalcBackoffTime(const TBackoffSettings& settings, ui32 retryNumber);
void Backoff(const NRetry::TBackoffSettings& settings, ui32 retryNumber);
void AsyncBackoff(std::shared_ptr<IClientImplCommon> client, const TBackoffSettings& settings,
    ui32 retryNumber, const std::function<void()>& fn);

}
