#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/duration.h>

namespace NYdb::NConsoleClient {

inline void ThrowOnError(const NYdb::TOperation& operation) {
    if (!operation.Ready()) {
        return;
    }
    NStatusHelpers::ThrowOnError(operation.Status());
}

} // namespace NYdb::NConsoleClient
