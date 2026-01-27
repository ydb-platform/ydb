#pragma once

#include "consts.h"

#include <aws/core/utils/memory/stl/AWSString.h>
#include <util/system/types.h>

namespace NYdb::NConsoleClient {

    inline ui64 ExtractSendTimestamp(const Aws::String& messageBody) {
        auto sepIndex = messageBody.find(SQS_MESSAGE_START_TIME_SEPARATOR);
        if (sepIndex == std::string::npos) {
            return 0;
        }
        return std::stoull(messageBody.substr(0, sepIndex));
    }

} // namespace NYdb::NConsoleClient