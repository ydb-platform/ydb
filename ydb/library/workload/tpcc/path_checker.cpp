#include "path_checker.h"

#include "constants.h"
#include "util.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/generic/hash_set.h>

namespace NYdb::NTPCC {

namespace {

THashSet<std::string> ListPath(TDriver& driver, const TString& path) {
    NScheme::TSchemeClient schemeClient(driver);
    auto listResult = schemeClient.ListDirectory(path, {}).GetValueSync();
    ExitIfError(listResult, TStringBuilder() << "Failed to list path '" << path << "'");

    THashSet<std::string> entries;

    const auto& children = listResult.GetChildren();
    for (const auto& child: children) {
        entries.insert(child.Name);
    }

    return entries;
}

void CheckTablesExist(TDriver& driver, const TString& path, const char* what) {
    auto entries = ListPath(driver, path);

    for (const char* table: TPCC_TABLES) {
        if (!entries.contains(table)) {
            Cerr << "TPC-C table '" << table << "' is missing in " << path << ". " << what << Endl;
            std::exit(1);
        }
    }
}

void CheckNoTablesExist(TDriver& driver, const TString& path, const char* what) {
    auto entries = ListPath(driver, path);

    for (const char* table: TPCC_TABLES) {
        if (entries.contains(table)) {
            Cerr << "TPC-C table '" << table << "' already exists in " << path << ". " << what << Endl;
            std::exit(1);
        }
    }
}

NTable::TTableDescription DescribeTable(TDriver& driver, const TString& path, const TString& tableName) {
    TString fullPath = path + "/" + tableName;
    NTable::TTableClient client(driver);
    NTable::TCreateSessionResult sessionResult = client.GetSession(NTable::TCreateSessionSettings()).GetValueSync();
    ExitIfError(sessionResult, "Failed to obtain a session to check the tables");
    auto session = sessionResult.GetSession();
    auto result = session.DescribeTable(fullPath).GetValueSync();
    ExitIfError(result, TStringBuilder() << "Failed to describe the table '" << fullPath << "'");

    return result.GetTableDescription();
}

void CheckNoIndexExists(TDriver& driver, const TString& path, const TString& tableName) {
    TString fullPath = path + "/" + tableName;
    auto desc = DescribeTable(driver, path, tableName);
    const std::vector<NTable::TIndexDescription>& indexes = desc.GetIndexDescriptions();
    if (!indexes.empty()) {
        Cerr << "Table '" << fullPath
             << "' already has index/indices. Are you importing to the existing data?" << Endl;
        std::exit(1);
    }
}

void CheckIndexExistsAndReady(TDriver& driver, const TString& path, const TString& tableName) {
    TString fullPath = path + "/" + tableName;
    auto desc = DescribeTable(driver, path, tableName);
    const std::vector<NTable::TIndexDescription>& indexes = desc.GetIndexDescriptions();
    if (indexes.empty()) {
        Cerr << "Table '" << fullPath << "' has no index" << Endl;
        std::exit(1);
    }

    if (indexes.size() > 1) {
        Cerr << "Table '" << fullPath << "' has more than one index: " << indexes.size() << Endl;
        std::exit(1);
    }

    const auto& index = indexes[0];
    const auto& indexName = index.GetIndexName();
    auto indexType = index.GetIndexType();
    if (indexType != NTable::EIndexType::GlobalSync) {
        Cerr << "Index '" << indexName << "' has unexpected type: " << int(indexType) << Endl;
        std::exit(1);
    }

    if (tableName == TABLE_CUSTOMER) {
        if (indexName != INDEX_CUSTOMER_NAME) {
            Cerr << "Table '" << fullPath << "' is expected to have index '" << INDEX_CUSTOMER_NAME
                << ", but index '" << indexName << "' found" << Endl;
            std::exit(1);
        }
    } else if (tableName == TABLE_OORDER) {
        if (indexName != INDEX_ORDER) {
            Cerr << "Table '" << fullPath << "' is expected to have index '" << INDEX_ORDER
                << ", but index '" << indexName << "' found" << Endl;
            std::exit(1);
        }
    } else {
        Cerr << "Checking index for unexpected table '" << fullPath << "'" << Endl;
        std::exit(1);
    }
}

int GetWarehouseCount(TDriver& driver, const TString& path) {
    // just check the warehouse table and assume that the rest is consistent

    TString query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT COUNT(*) FROM `{}`;
    )", path.c_str(), TABLE_WAREHOUSE);

    NQuery::TQueryClient queryClient(driver);
    auto result = queryClient.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
    ExitIfError(result, TStringBuilder() << "Failed to count warehouses in " << path << "/" << TABLE_WAREHOUSE);

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        return 0;
    }

    try {
        return parser.ColumnParser("column0").GetUint64();
    } catch (std::exception& e) {
        Cerr << "Failed to count warehouses in " << path << "/" << TABLE_WAREHOUSE << ": " << e.what() << Endl;
        std::exit(1);
    }
}

} // anonymous

void CheckPathForInit(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TString& path) noexcept
{
    auto connectionConfigCopy = connectionConfig;
    TDriver driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);

    CheckNoTablesExist(driver, path, "Already inited or forgot to clean?");
}

void CheckPathForImport(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TString& path) noexcept
{
    auto connectionConfigCopy = connectionConfig;
    TDriver driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);

    // 1. Check tables exist

    CheckTablesExist(driver, path, "Run 'init' first.");

    // 2. Check no wh populated

    int whCount = GetWarehouseCount(driver, path);
    if (whCount != 0) {
        Cerr << path << " already has " << whCount << " warehouses. Are you importing to the existing data?" << Endl;
        std::exit(1);
    }

    // 3. Check no indices

    CheckNoIndexExists(driver, path, TABLE_CUSTOMER);
    CheckNoIndexExists(driver, path, TABLE_OORDER);
}

void CheckPathForRun(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TString& path,
    int expectedWhCount) noexcept
{
    auto connectionConfigCopy = connectionConfig;
    TDriver driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);

    // 1. Check tables exist

    CheckTablesExist(driver, path, "Import the data first.");

    // 2. Check indices exist

    CheckIndexExistsAndReady(driver, path, TABLE_CUSTOMER);
    CheckIndexExistsAndReady(driver, path, TABLE_OORDER);

    // 3. Check the number of warehouses

    int whCount = GetWarehouseCount(driver, path);
    if (whCount != expectedWhCount) {
        if (whCount == 0) {
            Cerr << "Empty warehouse table (and maybe missing other TPC-C data), run import first" << Endl;
            std::exit(1);
        }
        if (whCount < expectedWhCount) {
            Cerr << "Expected data for " << expectedWhCount << " warehouses, but found for " << whCount << Endl;
            std::exit(1);
        }
    }
}

} // namespace NYdb::NTPCC
