#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

namespace NYdb::inline Dev::NTopic {

IExecutor::TPtr CreateSyncExecutor();

} // namespace NYdb::NTopic
