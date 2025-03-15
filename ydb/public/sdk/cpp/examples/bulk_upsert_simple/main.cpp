#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/getopt/last_getopt.h>

#include <filesystem>

constexpr size_t BATCH_SIZE = 1000;

struct TLogMessage {
    std::string App;
    std::string Host;
    TInstant Timestamp;
    uint32_t HttpCode;
    std::string Message;
};

void GetLogBatch(uint64_t logOffset, std::vector<TLogMessage>& logBatch) {
    logBatch.clear();
    for (size_t i = 0; i < BATCH_SIZE; ++i) {
        TLogMessage message;
        message.App = "App_" + std::to_string(logOffset % 10);
        message.Host = "192.168.0." + std::to_string(logOffset % 11);
        message.Timestamp = TInstant::Now() + TDuration::MilliSeconds(i % 1000);
        message.HttpCode = 200;
        message.Message = i % 2 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1";
        logBatch.emplace_back(message);
    }
}

bool WriteLogBatch(NYdb::NTable::TTableClient& tableClient, const std::string& table, const std::vector<TLogMessage>& logBatch,
                   const NYdb::NTable::TRetryOperationSettings& retrySettings)
{
    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (const auto& message : logBatch) {
        rows.AddListItem()
                .BeginStruct()
                .AddMember("App").Utf8(message.App)
                .AddMember("Host").Utf8(message.Host)
                .AddMember("Timestamp").Timestamp(message.Timestamp)
                .AddMember("HttpCode").Uint32(message.HttpCode)
                .AddMember("Message").Utf8(message.Message)
                .EndStruct();
    }
    rows.EndList();

    auto bulkUpsertOperation = [table, rowsValue = rows.Build()](NYdb::NTable::TTableClient& tableClient) {
        NYdb::TValue r = rowsValue;
        auto status = tableClient.BulkUpsert(table, std::move(r));
        return status.GetValueSync();
    };

    auto status = tableClient.RetryOperationSync(bulkUpsertOperation, retrySettings);

    if (!status.IsSuccess()) {
        std::cerr << std::endl << "Write failed with status: " << ToString(status) << std::endl;
        return false;
    }
    return true;
}

bool CreateLogTable(NYdb::NTable::TTableClient& client, const std::string& table) {
    std::cerr << "Create table " << table << "\n";

    NYdb::NTable::TRetryOperationSettings settings;
    auto status = client.RetryOperationSync([&table](NYdb::NTable::TSession session) {
            auto tableDesc = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("App", NYdb::EPrimitiveType::Utf8)
                .AddNullableColumn("Timestamp", NYdb::EPrimitiveType::Timestamp)
                .AddNullableColumn("Host", NYdb::EPrimitiveType::Utf8)
                .AddNullableColumn("HttpCode", NYdb::EPrimitiveType::Uint32)
                .AddNullableColumn("Message", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumns({"App", "Timestamp", "Host"})
                .Build();

            return session.CreateTable(table, std::move(tableDesc)).GetValueSync();
        }, settings);

    if (!status.IsSuccess()) {
        std::cerr << "Create table failed with status: " << ToString(status) << std::endl;
        return false;
    }
    return true;
}

bool Run(const NYdb::TDriver &driver, const std::string &table, uint32_t batchCount) {
    NYdb::NTable::TTableClient client(driver);
    if (!CreateLogTable(client, table)) {
        return false;
    }

    NYdb::NTable::TRetryOperationSettings writeRetrySettings;
    writeRetrySettings
            .Idempotent(true)
            .MaxRetries(20);

    std::vector<TLogMessage> logBatch;
    for (uint32_t offset = 0; offset < batchCount; ++offset) {
        GetLogBatch(offset, logBatch);
        if (!WriteLogBatch(client, table, logBatch, writeRetrySettings)) {
            return false;
        }
        std::cerr << ".";
    }

    std::cerr << std::endl << "Done." << std::endl;
    return true;
}

std::string JoinPath(const std::string& basePath, const std::string& path) {
    if (basePath.empty()) {
        return path;
    }

    std::filesystem::path prefixPathSplit(basePath);
    prefixPathSplit /= path;

    return prefixPathSplit;
}

int main(int argc, char** argv) {
    using namespace NLastGetopt;

    TOpts opts = TOpts::Default();

    std::string endpoint;
    std::string database;
    std::string table = "bulk_upsert_example";
    uint32_t count = 1000;

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
        .StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database name").Required().RequiredArgument("PATH")
        .StoreResult(&database);
    opts.AddLongOption('p', "table", "Path for table").Optional().RequiredArgument("PATH")
        .StoreResult(&table);
    opts.AddLongOption('c', "count", "count requests").Optional().RequiredArgument("NUM")
        .StoreResult(&count).DefaultValue(count);

    [[maybe_unused]] TOptsParseResult res(&opts, argc, argv);

    table = JoinPath(database, table);

    auto driverConfig = NYdb::TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");

    NYdb::TDriver driver(driverConfig);

    if (!Run(driver, table, count)) {
        driver.Stop(true);
        return 2;
    }

    driver.Stop(true);

    return 0;
}
