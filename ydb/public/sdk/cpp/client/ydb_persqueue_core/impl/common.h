#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>

namespace NYdb::NPersQueue {
    using NTopic::GetRetryErrorClass;
    using NTopic::GetRetryErrorClassV2;
}
