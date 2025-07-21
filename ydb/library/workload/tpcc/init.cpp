#include "init.h"

#include "constants.h"
#include "data_splitter.h"
#include "log.h"
#include "path_checker.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/logger/log.h>

#include <util/string/builder.h>

#include <format>

namespace NYdb::NTPCC {

namespace {

void CreateTable(NQuery::TQueryClient& client, const TString& sql, const TString& tableName, TLog* Log) {
    LOG_T("Creating table " << tableName << ":\n" << sql);

    auto result = client.RetryQuerySync([&sql](NQuery::TSession session) {
        return session.ExecuteQuery(
            sql,
            NQuery::TTxControl::NoTx()
        ).GetValueSync();
    });

    if (!result.IsSuccess()) {
        throw std::runtime_error(
            std::format("Failed to create table {}: {}",
                tableName.c_str(), result.GetIssues().ToOneLineString().c_str()));
    }

    LOG_I("Table " << tableName << " created successfully");
}

TString BuildPartitionClause(
    const std::vector<int>& splitKeys,
    int minPartCount,
    int maxPartCount,
    size_t partitionSizeMB = DEFAULT_SHARD_SIZE_MB)
{
    TStringBuilder clause;
    clause << "WITH (\n";
    clause << "    AUTO_PARTITIONING_BY_LOAD = DISABLED";

    if (partitionSizeMB > 0) {
        clause << ",\n    AUTO_PARTITIONING_PARTITION_SIZE_MB = " << partitionSizeMB;
    }

    clause << ",\n    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << maxPartCount;
    clause << ",\n    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << minPartCount;

    if (!splitKeys.empty()) {
        TStringBuilder keysStr;
        for (size_t i = 0; i < splitKeys.size(); ++i) {
            if (i > 0) {
                keysStr << ", ";
            }
            keysStr << splitKeys[i];
        }
        clause << ",\n    PARTITION_AT_KEYS = (" << keysStr << ")";
    }

    clause << "\n)";
    return clause;
}

} // anonymous

void InitSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    auto log = std::make_unique<TLog>(CreateLogBackend("cerr", runConfig.LogPriority, true));
    auto* Log = log.get(); // to make LOG_* macros working

    auto connectionConfigCopy = connectionConfig;
    auto driver = NConsoleClient::TYdbCommand::CreateDriver(connectionConfigCopy);
    NQuery::TQueryClient client(driver);

    CheckPathForInit(connectionConfig, runConfig.Path);

    TDataSplitter dataSplitter(runConfig.WarehouseCount);

    // Get split keys for different table types
    auto warehouseSplitKeys = dataSplitter.GetSplitKeys(TABLE_WAREHOUSE);
    auto itemSplitKeys = dataSplitter.GetSplitKeys(TABLE_ITEM);
    auto stockSplitKeys = dataSplitter.GetSplitKeys(TABLE_STOCK);
    auto customerSplitKeys = dataSplitter.GetSplitKeys(TABLE_CUSTOMER);
    auto historySplitKeys = dataSplitter.GetSplitKeys(TABLE_HISTORY);
    auto oorderSplitKeys = dataSplitter.GetSplitKeys(TABLE_OORDER);
    auto orderLineSplitKeys = dataSplitter.GetSplitKeys(TABLE_ORDER_LINE);

    int minPartCount = TDataSplitter::CalcMinParts(runConfig.WarehouseCount);

    // Calculate partition counts

    // TABLE_WAREHOUSE is small and for simplicity all small tables follow its pattern
    int smallTableShardCount = std::max(minPartCount, static_cast<int>(warehouseSplitKeys.size()) + 1);
    int smallTableMaxShardCount = smallTableShardCount * 2;

    int itemShardCount = std::max(minPartCount, static_cast<int>(itemSplitKeys.size()) + 1);
    int itemMaxShardCount = itemShardCount * 2;

    int stockShardCount = std::max(minPartCount, static_cast<int>(stockSplitKeys.size()) + 1);
    int stockMaxShardCount = stockShardCount * 2;

    int customerShardCount = std::max(minPartCount, static_cast<int>(customerSplitKeys.size()) + 1);
    int customerMaxShardCount = customerShardCount * 2;

    int historyShardCount = std::max(minPartCount, static_cast<int>(historySplitKeys.size()) + 1);
    int historyMaxShardCount = historyShardCount * 2;

    int oorderShardCount = std::max(minPartCount, static_cast<int>(oorderSplitKeys.size()) + 1);
    int oorderMaxShardCount = oorderShardCount * 2;

    int orderLineShardCount = std::max(minPartCount, static_cast<int>(orderLineSplitKeys.size()) + 1);
    int orderLineMaxShardCount = orderLineShardCount * 2;

    // Build partition clauses

    auto smallTablePartitionClause = BuildPartitionClause(warehouseSplitKeys, smallTableShardCount, smallTableMaxShardCount);
    auto itemPartitionClause = BuildPartitionClause(itemSplitKeys, itemShardCount, itemMaxShardCount);
    auto stockPartitionClause = BuildPartitionClause(stockSplitKeys, stockShardCount, stockMaxShardCount);
    auto customerPartitionClause = BuildPartitionClause(customerSplitKeys, customerShardCount, customerMaxShardCount);
    auto historyPartitionClause = BuildPartitionClause(historySplitKeys, historyShardCount, historyMaxShardCount);
    auto oorderPartitionClause = BuildPartitionClause(oorderSplitKeys, oorderShardCount, oorderMaxShardCount);
    auto orderLinePartitionClause = BuildPartitionClause(orderLineSplitKeys, orderLineShardCount, orderLineMaxShardCount);

    // Create tables

    try {

        TString warehouseSql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                W_ID       Int32          NOT NULL,
                W_YTD      Double,
                W_TAX      Double,
                W_NAME     Utf8,
                W_STREET_1 Utf8,
                W_STREET_2 Utf8,
                W_CITY     Utf8,
                W_STATE    Utf8,
                W_ZIP      Utf8,

                PRIMARY KEY (W_ID)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_WAREHOUSE, smallTablePartitionClause.c_str());
        CreateTable(client, warehouseSql, TABLE_WAREHOUSE, Log);

        TString itemSql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                I_ID    Int32           NOT NULL,
                I_NAME  Utf8,
                I_PRICE Double,
                I_DATA  Utf8,
                I_IM_ID Int32,

                PRIMARY KEY (I_ID)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_ITEM, itemPartitionClause.c_str());
        CreateTable(client, itemSql, TABLE_ITEM, Log);

        TString stockSql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                S_W_ID       Int32           NOT NULL,
                S_I_ID       Int32           NOT NULL,
                S_QUANTITY   Int32,
                S_YTD        Double,
                S_ORDER_CNT  Int32,
                S_REMOTE_CNT Int32,
                S_DATA       Utf8,
                S_DIST_01    Utf8,
                S_DIST_02    Utf8,
                S_DIST_03    Utf8,
                S_DIST_04    Utf8,
                S_DIST_05    Utf8,
                S_DIST_06    Utf8,
                S_DIST_07    Utf8,
                S_DIST_08    Utf8,
                S_DIST_09    Utf8,
                S_DIST_10    Utf8,

                PRIMARY KEY (S_W_ID, S_I_ID)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_STOCK, stockPartitionClause.c_str());
        CreateTable(client, stockSql, TABLE_STOCK, Log);

        TString districtSql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                D_W_ID      Int32            NOT NULL,
                D_ID        Int32            NOT NULL,
                D_YTD       Double,
                D_TAX       Double,
                D_NEXT_O_ID Int32,
                D_NAME      Utf8,
                D_STREET_1  Utf8,
                D_STREET_2  Utf8,
                D_CITY      Utf8,
                D_STATE     Utf8,
                D_ZIP       Utf8,

                PRIMARY KEY (D_W_ID, D_ID)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_DISTRICT, smallTablePartitionClause.c_str());
        CreateTable(client, districtSql, TABLE_DISTRICT, Log);

        TString customerSql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                C_W_ID         Int32            NOT NULL,
                C_D_ID         Int32            NOT NULL,
                C_ID           Int32            NOT NULL,
                C_DISCOUNT     Double,
                C_CREDIT       Utf8,
                C_LAST         Utf8,
                C_FIRST        Utf8,
                C_CREDIT_LIM   Double,
                C_BALANCE      Double,
                C_YTD_PAYMENT  Double,
                C_PAYMENT_CNT  Int32,
                C_DELIVERY_CNT Int32,
                C_STREET_1     Utf8,
                C_STREET_2     Utf8,
                C_CITY         Utf8,
                C_STATE        Utf8,
                C_ZIP          Utf8,
                C_PHONE        Utf8,
                C_SINCE        Timestamp,
                C_MIDDLE       Utf8,
                C_DATA         Utf8,

                PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_CUSTOMER, customerPartitionClause.c_str());
        CreateTable(client, customerSql, TABLE_CUSTOMER, Log);

        TString historySql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                H_C_W_ID    Int32,
                H_C_ID      Int32,
                H_C_D_ID    Int32,
                H_D_ID      Int32,
                H_W_ID      Int32,
                H_DATE      Timestamp,
                H_AMOUNT    Double,
                H_DATA      Utf8,
                H_C_NANO_TS Int64        NOT NULL,

                PRIMARY KEY (H_C_W_ID, H_C_NANO_TS)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_HISTORY, historyPartitionClause.c_str());
        CreateTable(client, historySql, TABLE_HISTORY, Log);

        TString oorderSql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                O_W_ID       Int32       NOT NULL,
                O_D_ID       Int32       NOT NULL,
                O_ID         Int32       NOT NULL,
                O_C_ID       Int32,
                O_CARRIER_ID Int32,
                O_OL_CNT     Int32,
                O_ALL_LOCAL  Int32,
                O_ENTRY_D    Timestamp,

                PRIMARY KEY (O_W_ID, O_D_ID, O_ID)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_OORDER, oorderPartitionClause.c_str());
        CreateTable(client, oorderSql, TABLE_OORDER, Log);

        TString newOrderSql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                NO_W_ID Int32 NOT NULL,
                NO_D_ID Int32 NOT NULL,
                NO_O_ID Int32 NOT NULL,

                PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_NEW_ORDER, smallTablePartitionClause.c_str());
        CreateTable(client, newOrderSql, TABLE_NEW_ORDER, Log);

        TString orderLineSql = std::format(R"(
            PRAGMA TablePathPrefix("{}");

            CREATE TABLE {} (
                OL_W_ID        Int32           NOT NULL,
                OL_D_ID        Int32           NOT NULL,
                OL_O_ID        Int32           NOT NULL,
                OL_NUMBER      Int32           NOT NULL,
                OL_I_ID        Int32,
                OL_DELIVERY_D  Timestamp,
                OL_AMOUNT      Double,
                OL_SUPPLY_W_ID Int32,
                OL_QUANTITY    Double,
                OL_DIST_INFO   Utf8,

                PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
            )
            {};
        )", runConfig.Path.c_str(), TABLE_ORDER_LINE, orderLinePartitionClause.c_str());
        CreateTable(client, orderLineSql, TABLE_ORDER_LINE, Log);

        Cout << "All TPC-C tables created successfully" << Endl;

        driver.Stop(true);

    } catch (const std::exception& e) {
        Cout << "Failed to create TPC-C tables. " << e.what() << Endl;
        Cout << "After fixing the reason, you might need to run `tpcc clean`." << Endl;

        // we want to stop it before calling exit to avoid crash
        driver.Stop(true);

        std::exit(1);
    }
}

} // namespace NYdb::NTPCC
