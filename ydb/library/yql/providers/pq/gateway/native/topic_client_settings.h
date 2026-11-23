#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NYql {

NYdb::NTopic::TTopicClientSettings MakeCommonTopicClientSettings(ui64 handlersExecutorThreadsNum, ui64 compressionExecutorThreadsNum);

}   // namespace NYql
