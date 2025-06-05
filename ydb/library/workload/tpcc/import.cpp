#include "import.h"

#include "constants.h"
#include "log.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/logger/log.h>

#include <format>

namespace NYdb::NTPCC {

void ImportSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    auto log = std::make_unique<TLog>(CreateLogBackend("cerr", runConfig.LogPriority, true));
    auto* Log = log.get(); // to make LOG_* macros working

    auto connectionConfigCopy = connectionConfig;
    auto driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);
    NQuery::TQueryClient client(driver);

    // TODO: Implement data import logic

    driver.Stop(true);

    LOG_I("TPC-C data import completed");
}

} // namespace NYdb::NTPCC
