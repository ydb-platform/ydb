#include "bulk_upsert.h"

#include <filesystem>
#include <format>

static constexpr size_t BATCH_SIZE = 1000;

TRunArgs GetRunArgs() {
    std::string endpoint = std::getenv("YDB_ENDPOINT");
    std::string database = std::getenv("YDB_DATABASE");

    auto driverConfig = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");

    TDriver driver(driverConfig);
    return {driver, database + "/" + std::string(std::getenv("YDB_TEST_ROOT")) + "/bulk"};
}

TStatus CreateTable(TTableClient& client, const std::string& table) {
    TRetryOperationSettings settings;
    auto status = client.RetryOperationSync([&table](TSession session) {
            auto tableDesc = TTableBuilder()
                .AddNullableColumn("App", EPrimitiveType::Utf8)
                .AddNullableColumn("Timestamp", EPrimitiveType::Timestamp)
                .AddNullableColumn("Host", EPrimitiveType::Utf8)
                .AddNonNullableColumn("Id", EPrimitiveType::Uint64)
                .AddNullableColumn("HttpCode", EPrimitiveType::Uint32)
                .AddNullableColumn("Message", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumns({"App", "Timestamp", "Host", "Id"})
                .Build();

            return session.CreateTable(table, std::move(tableDesc)).GetValueSync();
        }, settings);

    return status;
}

TStatistic GetLogBatch(uint64_t logOffset, std::vector<TLogMessage>& logBatch, uint32_t lastNumber) {
    logBatch.clear();
    uint32_t correctSumApp = 0;
    uint32_t correctSumHost = 0;
    uint32_t correctRowCount = 0;

    for (size_t i = 0; i < BATCH_SIZE; ++i) {
        TLogMessage message;
        message.Pk.Id = correctRowCount + lastNumber;        
        message.Pk.App = "App_" + std::to_string(logOffset % 10);
        message.Pk.Host = "192.168.0." + std::to_string(logOffset % 11);
        message.Pk.Timestamp = TInstant::Now() + TDuration::MilliSeconds(i % 1000);
        message.HttpCode = 200;
        message.Message = i % 2 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1";
        logBatch.emplace_back(message);

        correctSumApp += logOffset % 10;
        correctSumHost += logOffset % 11;
        ++correctRowCount;
        
    }
    return {correctSumApp, correctSumHost, correctRowCount};
}

TStatus WriteLogBatch(TTableClient& tableClient, const std::string& table, const std::vector<TLogMessage>& logBatch,
                   const TRetryOperationSettings& retrySettings) {
    TValueBuilder rows;
    rows.BeginList();
    for (const auto& message : logBatch) {
        rows.AddListItem()
                .BeginStruct()
                .AddMember("Id").Uint64(message.Pk.Id)
                .AddMember("App").Utf8(message.Pk.App)
                .AddMember("Host").Utf8(message.Pk.Host)
                .AddMember("Timestamp").Timestamp(message.Pk.Timestamp)
                .AddMember("HttpCode").Uint32(message.HttpCode)
                .AddMember("Message").Utf8(message.Message)
                .EndStruct();
    }
    rows.EndList();
    auto bulkUpsertOperation = [table, rowsValue = rows.Build()](TTableClient& tableClient) {
        TValue r = rowsValue;
        auto status = tableClient.BulkUpsert(table, std::move(r));
        return status.GetValueSync();
    };

    auto status = tableClient.RetryOperationSync(bulkUpsertOperation, retrySettings);
    return status;
}

static TStatus SelectTransaction(TSession session, const std::string& path,
    std::optional<TResultSet>& resultSet) {
    std::filesystem::path filesystemPath(path);
    auto query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT
        SUM(CAST(SUBSTRING(CAST(App as string), 4) as Int32)),
        SUM(CAST(SUBSTRING(CAST(Host as string), 10) as Int32)),
        COUNT(*)
        FROM {}
    )", filesystemPath.parent_path().string(), filesystemPath.filename().string());

    auto txControl =
        TTxControl::BeginTx(TTxSettings::SerializableRW())
        .CommitTx();

    auto result = session.ExecuteDataQuery(query, txControl).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}

TStatistic Select(TTableClient& client, const std::string& path) {
    std::optional<TResultSet> resultSet;
    NStatusHelpers::ThrowOnError(client.RetryOperationSync([path, &resultSet](TSession session) {
        return SelectTransaction(session, path, resultSet);
    }));

    TResultSetParser parser(*resultSet);

    uint64_t sumApp = 0;
    uint64_t sumHost = 0;
    uint64_t rowCount = 0;

    if (parser.ColumnsCount() != 3 || parser.RowsCount() != 1) {
        throw NStatusHelpers::TYdbErrorException(TStatus(EStatus::GENERIC_ERROR,
        {NYdb::NIssue::TIssue("The number of columns should be: 3.\nThe number of rows should be: 1")}));
    }

    if (parser.TryNextRow()) {
        sumApp = *parser.ColumnParser("column0").GetOptionalInt64();
        sumHost = *parser.ColumnParser("column1").GetOptionalInt64();
        rowCount = parser.ColumnParser("column2").GetUint64();
    }

    return {sumApp, sumHost, rowCount};
}

void DropTable(TTableClient& client, const std::string& path) {
    NStatusHelpers::ThrowOnError(client.RetryOperationSync([path](TSession session) {
        return session.DropTable(path).ExtractValueSync();
    }));
}
