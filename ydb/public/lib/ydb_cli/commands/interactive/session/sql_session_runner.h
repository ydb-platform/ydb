#pragma once

#include "session_runner_interface.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::NConsoleClient {

struct TSqlSessionSettings {
    TDriver Driver;
    TString Database;
    bool EnableAiInteractive = false;
};

ISessionRunner::TPtr CreateSqlSessionRunner(const TSqlSessionSettings& settings);

} // namespace NYdb::NConsoleClient
