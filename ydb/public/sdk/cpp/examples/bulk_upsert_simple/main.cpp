#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/system/env.h>
#include <util/folder/pathsplit.h>

Y_DECLARE_OUT_SPEC(, NYdb::TStatus, stream, value) {
    stream << "Status: " << value.GetStatus() << Endl;
    value.GetIssues().PrintTo(stream);
}

constexpr size_t BATCH_SIZE = 1000;

struct TLogMessage {
    TString App;
    TString Host;
    TInstant Timestamp;
    ui32 HttpCode;
    TString Message;
};

void GetLogBatch(ui64 logOffset, TVector<TLogMessage>& logBatch) {
    logBatch.clear();
    for (size_t i = 0; i < BATCH_SIZE; ++i) {
        TLogMessage message;
        message.App = "App_" + ToString(logOffset % 10);
        message.Host = "192.168.0." + ToString(logOffset % 11);
        message.Timestamp = TInstant::Now() + TDuration::MilliSeconds(i % 1000);
        message.HttpCode = 200;
        message.Message = i % 2 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1";
        logBatch.emplace_back(message);
    }
}

bool WriteLogBatch(NYdb::NTable::TTableClient& tableClient, const TString& table, const TVector<TLogMessage>& logBatch,
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
        Cerr << Endl << "Write failed with status: " << (const NYdb::TStatus&)status << Endl;
        return false;
    }
    return true;
}

bool CreateLogTable(NYdb::NTable::TTableClient& client, const TString& table) {
    Cerr << "Create table " << table << "\n";

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
        Cerr << "Create table failed with status: " << status << Endl;
        return false;
    }
    return true;
}

bool Run(const NYdb::TDriver &driver, const TString &table, ui32 batchCount) {
    NYdb::NTable::TTableClient client(driver);
    if (!CreateLogTable(client, table)) {
        return false;
    }

    NYdb::NTable::TRetryOperationSettings writeRetrySettings;
    writeRetrySettings
            .Idempotent(true)
            .MaxRetries(20);

    TVector<TLogMessage> logBatch;
    for (ui32 offset = 0; offset < batchCount; ++offset) {
        GetLogBatch(offset, logBatch);
        if (!WriteLogBatch(client, table, logBatch, writeRetrySettings)) {
            return false;
        }
        Cerr << ".";
    }

    Cerr << Endl << "Done." << Endl;
    return true;
}

TString JoinPath(const TString& basePath, const TString& path) {
    if (basePath.empty()) {
        return path;
    }

    TPathSplitUnix prefixPathSplit(basePath);
    prefixPathSplit.AppendComponent(path);

    return prefixPathSplit.Reconstruct();
}

int main(int argc, char** argv) {
    using namespace NLastGetopt;

    TOpts opts = TOpts::Default();

    TString endpoint;
    TString database;
    TString table = "bulk_upsert_example";
    ui32 count = 1000;

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
        .StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database name").Required().RequiredArgument("PATH")
        .StoreResult(&database);
    opts.AddLongOption('p', "table", "Path for table").Optional().RequiredArgument("PATH")
        .StoreResult(&table);
    opts.AddLongOption('c', "count", "count requests").Optional().RequiredArgument("NUM")
        .StoreResult(&count).DefaultValue(count);

    TOptsParseResult res(&opts, argc, argv);
    Y_UNUSED(res);

    table = JoinPath(database, table);

    auto driverConfig = NYdb::TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(GetEnv("YDB_TOKEN"));

    NYdb::TDriver driver(driverConfig);

    if (!Run(driver, table, count)) {
        driver.Stop(true);
        return 2;
    }

    driver.Stop(true);

    return 0;
}
