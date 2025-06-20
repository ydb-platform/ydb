#include "clean.h"

#include "constants.h"
#include "log.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/logger/log.h>

#include <format>

namespace NYdb::NTPCC {

namespace {

void DropTable(NQuery::TQueryClient& client, const TString& tableName, const TString& path, TLog* Log) {
    TString sql = std::format(R"(
        PRAGMA TablePathPrefix("{}");
        DROP TABLE `{}`;
    )", path.c_str(), tableName.c_str());

    LOG_T("Dropping table " << tableName << ":\n" << sql);

    auto result = client.RetryQuerySync([&sql](NQuery::TSession session) {
        return session.ExecuteQuery(
            sql,
            NQuery::TTxControl::NoTx()
        ).GetValueSync();
    });

    if (!result.IsSuccess()) {
        // Log warning but don't throw - table might not exist
        LOG_W("Failed to drop table " << tableName << ": " << result.GetIssues().ToOneLineString());
    } else {
        LOG_I("Table " << tableName << " dropped successfully");
    }
}

} // anonymous

void CleanSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    auto log = std::make_unique<TLog>(CreateLogBackend("cerr", runConfig.LogPriority, true));
    auto* Log = log.get(); // to make LOG_* macros working

    auto connectionConfigCopy = connectionConfig;
    auto driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);
    NQuery::TQueryClient client(driver);

    LOG_I("Starting to drop TPC-C tables");

    // Drop tables in reverse order to avoid potential dependency issues
    DropTable(client, TABLE_ORDER_LINE, runConfig.Path, Log);
    DropTable(client, TABLE_NEW_ORDER, runConfig.Path, Log);
    DropTable(client, TABLE_OORDER, runConfig.Path, Log);
    DropTable(client, TABLE_HISTORY, runConfig.Path, Log);
    DropTable(client, TABLE_CUSTOMER, runConfig.Path, Log);
    DropTable(client, TABLE_DISTRICT, runConfig.Path, Log);
    DropTable(client, TABLE_STOCK, runConfig.Path, Log);
    DropTable(client, TABLE_ITEM, runConfig.Path, Log);
    DropTable(client, TABLE_WAREHOUSE, runConfig.Path, Log);

    driver.Stop(true);

    LOG_I("All TPC-C tables dropped successfully");
}
}
