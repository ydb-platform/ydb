#include "ydb_common_ut.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

using namespace NYdb;

namespace {

THashMap<EPrimitiveType, TString> allowedTypes = {
    //EPrimitiveType::Bool,
    {EPrimitiveType::Uint8, "Uint8"},
    {EPrimitiveType::Int32, "Int32"},
    {EPrimitiveType::Uint32, "Uint32"},
    {EPrimitiveType::Int64, "Int64"},
    {EPrimitiveType::Uint64, "Uint64"},
    //{EPrimitiveType::Float, "Float"},
    //{EPrimitiveType::Double, "Double"},
    {EPrimitiveType::Date, "Date"},
    {EPrimitiveType::Datetime, "Datetime"},
    {EPrimitiveType::Timestamp, "Timestamp"},
    //{EPrimitiveType::Interval, "Interval"},
    {EPrimitiveType::String, "String"},
    {EPrimitiveType::Utf8, "Utf8"}
    //EPrimitiveType::Yson,
    //EPrimitiveType::Json,
    //EPrimitiveType::JsonDocument,
    //EPrimitiveType::DyNumber,
};

static constexpr const char* testShardingVariants[] = {
    R"(["timestamp", "uid"])",
    R"(["timestamp", "resource_type", "resource_id", "uid"])"
};
}

Y_UNIT_TEST_SUITE(YdbOlapStore) {

    void EnableDebugLogs(TKikimrWithGrpcAndRootSchema& server) {
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_SESSION, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::MSGBUS_REQUEST, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NActors::NLog::PRI_DEBUG);
    }

    NYdb::TDriver ConnectToServer(TKikimrWithGrpcAndRootSchema& server, const TString& token = {}) {
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(token));
        return connection;
    }

    struct TTestOlapTableOptions {
        EPrimitiveType TsType = EPrimitiveType::Timestamp;
        ui32 NumShards = 2;
        TString Sharding = testShardingVariants[0];
        TString HashFunction = "HASH_FUNCTION_CLOUD_LOGS";
    };

    template<bool NotNull>
    void CreateOlapTable(const TServerSettings& settings, const TString& tableName, TTestOlapTableOptions opts = {})
    {
        auto notNullStr = (NotNull ? "NotNull: true" : "");
        TString tableDescr = Sprintf(R"(
            Name: "OlapStore"
            ColumnShardCount: 4
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "message" Type: "Utf8" }
                    Columns { Name: "json_payload" Type: "JsonDocument" }
                    Columns { Name: "resource_id" Type: "Utf8" %s }
                    Columns { Name: "uid" Type: "Utf8" %s }
                    Columns { Name: "timestamp" Type: "%s" %s }
                    Columns { Name: "resource_type" Type: "Utf8" %s }
                    Columns { Name: "level" Type: "Int32" }
                    Columns { Name: "ingested_at" Type: "Timestamp" }
                    Columns { Name: "saved_at" Type: "Timestamp" }
                    Columns { Name: "request_id" Type: "Utf8" }
                    KeyColumnNames: ["timestamp", "resource_type", "resource_id", "uid"]
                    Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                }
            }
        )", notNullStr, notNullStr, allowedTypes[opts.TsType].c_str(), notNullStr, notNullStr);

        TClient annoyingClient(settings);
        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateOlapStore("/Root", tableDescr);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);

        status = annoyingClient.CreateColumnTable("/Root/OlapStore", Sprintf(R"(
            Name: "%s"
            ColumnShardCount : %d
            Sharding {
                HashSharding {
                    Function: %s
                    Columns: %s
                }
            }
        )", tableName.c_str(), opts.NumShards, opts.HashFunction.c_str(), opts.Sharding.c_str()));

        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);
    }

    template<bool NotNull>
    void CreateTable(const TServerSettings& settings, const TString& tableName) {
        auto notNullStr = (NotNull ? "NotNull: true" : "");
        TString tableDescr =  Sprintf(R"(
                Name: "%s"
                Columns { Name: "uid" Type: "Utf8" %s }
                Columns { Name: "message" Type: "Utf8" }
                Columns { Name: "json_payload" Type: "JsonDocument" }
                Columns { Name: "resource_id" Type: "Utf8" %s }
                Columns { Name: "ingested_at" Type: "Timestamp" }
                Columns { Name: "timestamp" Type: "Timestamp" %s }
                Columns { Name: "resource_type" Type: "Utf8" %s }
                Columns { Name: "level" Type: "Int32" }
                Columns { Name: "saved_at" Type: "Timestamp" }
                Columns { Name: "request_id" Type: "Utf8" }
                KeyColumnNames: ["timestamp", "resource_type", "resource_id", "uid"]
            )", tableName.c_str(), notNullStr, notNullStr, notNullStr, notNullStr);

        TClient annoyingClient(settings);
        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateTable("/Root", tableDescr);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);
    }

    NYdb::NTable::TAsyncBulkUpsertResult SendBatch(NYdb::NTable::TTableClient& client, const TString& tableName,
        const ui64 batchSize, const ui32 baseUserId, std::pair<EPrimitiveType, i64>& value)
    {
        i64 ts = value.second;

        TValueBuilder rows;
        rows.BeginList();
        for (ui64 i = 0; i < batchSize; ++i, ts += 1000) {
            const ui32 userId = baseUserId + (i % 100);
            auto& row = rows.AddListItem()
                .BeginStruct();
            switch (value.first) {
                case EPrimitiveType::Uint8:
                    row.AddMember("timestamp").Uint8(ts);
                    break;
                case EPrimitiveType::Int32:
                    row.AddMember("timestamp").Int32(ts);
                    break;
                case EPrimitiveType::Uint32:
                    row.AddMember("timestamp").Uint32(ts);
                    break;
                case EPrimitiveType::Int64:
                    row.AddMember("timestamp").Int64(ts);
                    break;
                case EPrimitiveType::Uint64:
                    row.AddMember("timestamp").Uint64(ts);
                    break;
                case EPrimitiveType::Float:
                    row.AddMember("timestamp").Float(ts);
                    break;
                case EPrimitiveType::Double:
                    row.AddMember("timestamp").Double(ts);
                    break;
                case EPrimitiveType::Timestamp:
                    row.AddMember("timestamp").Timestamp(TInstant::MicroSeconds(ts % NYql::NUdf::MAX_TIMESTAMP));
                    break;
                case EPrimitiveType::Date:
                    row.AddMember("timestamp").Date(TInstant::Days((ts / 1000) % NYql::NUdf::MAX_DATE));
                    break;
                case EPrimitiveType::Datetime:
                    row.AddMember("timestamp").Datetime(TInstant::Seconds((ts / 1000) % NYql::NUdf::MAX_DATETIME));
                    break;
                case EPrimitiveType::Interval:
                    row.AddMember("timestamp").Interval(ts);
                    break;
                case EPrimitiveType::String:
                    row.AddMember("timestamp").String(ToString(ts));
                    break;
                case EPrimitiveType::Utf8:
                    row.AddMember("timestamp").Utf8(ToString(ts));
                    break;
                default:
                    UNIT_ASSERT(false);
            }
            row.AddMember("resource_type").Utf8(i%2 ? "app" : "nginx")
                    .AddMember("resource_id").Utf8("resource_" + ToString((i+13) % 7))
                    .AddMember("uid").Utf8(ToString(i % 23))
                    .AddMember("level").Int32(i % 10)
                    .AddMember("message").Utf8("message")
                    .AddMember("json_payload").JsonDocument(
                        Sprintf(R"({
                                "auth":{
                                    "user":{
                                        "ip":"257.257.257.257",
                                        "is_cloud":"false",
                                        "id":%)" PRIu32 R"(
                                    },
                                    "type":"token",
                                    "org_id":7704,
                                    "service":{
                                        "ip":"258.258.258.258",
                                        "internal":"false"
                                    }
                                }
                            })", userId))
                    .AddMember("ingested_at").Timestamp(TInstant::MicroSeconds(ts) + TDuration::MilliSeconds(342))
                    .AddMember("saved_at").Timestamp(TInstant::MicroSeconds(ts) + TDuration::MilliSeconds(600))
                    .AddMember("request_id").Utf8(Sprintf("%x", (unsigned int)i))
                .EndStruct();
        }
        rows.EndList();

        return client.BulkUpsert(tableName, rows.Build());
    }

    size_t WriteRows(NYdb::TDriver& connection, const TString& tableName, i64 startTs, const size_t batchCount, const size_t batchSize, const TString& token = {}) {
        NYdb::NTable::TTableClient client(connection, NYdb::NTable::TClientSettings().AuthToken(token));

        TInstant start = TInstant::Now();
        auto ts = std::make_pair<EPrimitiveType, i64>(EPrimitiveType::Timestamp, (i64)startTs);

        const ui32 baseUserId = 1000000;
        TVector<NYdb::NTable::TAsyncBulkUpsertResult> results;
        for (ui64 b = 0; b < batchCount; ++b) {
            auto res = SendBatch(client, tableName, batchSize, baseUserId, ts);
            results.emplace_back(std::move(res));
        }

        for (auto& asyncResult : results) {
            auto res = asyncResult.GetValueSync();
            Cerr << ".";
            if (res.GetStatus() != EStatus::SUCCESS) {
                Cerr << res.GetStatus();
            }
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }
        Cerr << Endl << tableName << ": " << batchCount * batchSize << " rows upserted in " << TInstant::Now() - start << Endl;

        return batchCount * batchSize;
    }

    template <class TStatsProto>
    void PrintQueryStats(IOutputStream& out, const TStatsProto& stats) {
        out << "total CPU: " << stats.process_cpu_time_us() << "\n";
        for (const auto& qp : stats.query_phases()) {
            out << "  duration: " << qp.duration_us() << " usec\n"
                << "  cpu: " << qp.cpu_time_us() << " usec\n";
            for (const auto& ta : qp.table_access()) {
                out << "    " << ta << "\n";
            }
        }
    }

    TString RunQuerySimple(TDriver& connection, const TString& query) {
        auto client = NYdb::NTable::TTableClient(connection);

        NYdb::NTable::TStreamExecScanQuerySettings execSettings;
        execSettings.CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);
        auto it = client.StreamExecuteScanQuery(query, execSettings).GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto result = NKikimr::NKqp::CollectStreamResult(it);
        Cerr << "RESULT:\n" << result.ResultSetYson << "\n---------------------\nSTATS:\n";
        UNIT_ASSERT(result.QueryStats);
        PrintQueryStats(Cerr, *result.QueryStats);
        Cerr << "\n";
        return result.ResultSetYson;
    }

    TString RunQuery(TDriver& connection, const TString& query) {
        for(ui32 iter = 0; iter < 3; iter++) {
            try {
                return RunQuerySimple(connection, query);
            } catch(...) {
                /// o_O
            }
        }

        return RunQuerySimple(connection, query);
    }

    // Create OLTP and OLAP tables with the same set of columns and same PK
    template<bool NotNull>
    void CreateTestTables(const TServerSettings& settings, const TString& tableName, const TString& sharding) {
        TTestOlapTableOptions opts;
        opts.Sharding = sharding;
        CreateOlapTable<NotNull>(settings, tableName, opts);
        CreateTable<NotNull>(settings, "oltp_" + tableName);
    }

    // Write the same set or rows to OLTP and OLAP table
    size_t WriteTestRows(NYdb::TDriver& connection, const TString& tableName, i64 startTs, const size_t batchCount, const size_t batchSize, const TString& token = {}) {
        size_t rowCount = WriteRows(connection, "/Root/OlapStore/" + tableName, startTs, batchCount, batchSize, token);
        size_t rowCount2 = WriteRows(connection, "/Root/oltp_" + tableName, startTs, batchCount, batchSize, token);
        UNIT_ASSERT_VALUES_EQUAL(rowCount, rowCount2);
        return rowCount;
    }

    // Run query against OLTP and OLAP table and check that results are equal
    TString CompareQueryResults(TDriver& connection, const TString& tableName, const TString& query) {
        Cerr << "QUERY:\n" << query << "\n\n";

        TString oltpQuery = query;
        SubstGlobal(oltpQuery, "<TABLE>", "`/Root/oltp_" + tableName + "`");
        TString expectedResult = RunQuery(connection, oltpQuery);

        TString olapQuery = query;
        SubstGlobal(olapQuery, "<TABLE>", "`/Root/OlapStore/" + tableName + "`");
        TString result = RunQuery(connection, olapQuery);

        UNIT_ASSERT_VALUES_EQUAL(result, expectedResult);
        return result;
    }

    template<bool NotNull>
    void TestBulkUpsert(EPrimitiveType pkFirstType) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        EnableDebugLogs(server);

        auto connection = ConnectToServer(server);

        TTestOlapTableOptions opts;
        opts.TsType = pkFirstType;
        opts.HashFunction = "HASH_FUNCTION_MODULO_N";
        CreateOlapTable<NotNull>(*server.ServerSettings, "log1", opts);

        TClient annoyingClient(*server.ServerSettings);
        annoyingClient.ModifyOwner("/Root/OlapStore", "log1", "alice@builtin");

        {
            NYdb::NTable::TTableClient client(connection, NYdb::NTable::TClientSettings().AuthToken("bob@builtin"));

            std::pair<EPrimitiveType, i64> ts(pkFirstType, 1000);
            auto res = SendBatch(client, "/Root/OlapStore/log1", 100, 1, ts).GetValueSync();
            Cerr << __FILE__ << ":" << __LINE__ << " Issues: " << res.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(),
                "Access denied for bob@builtin with access UpdateRow to table '/Root/OlapStore/log1'");

            TString result = RunQuery(connection, "SELECT count(*) FROM `/Root/OlapStore/log1`;");
            UNIT_ASSERT_VALUES_EQUAL(result, "[[0u]]");
        }

        {
            NYdb::NTable::TTableClient client(connection, NYdb::NTable::TClientSettings().AuthToken("alice@builtin"));
            std::pair<EPrimitiveType, i64> ts(pkFirstType, 1000);
            auto res = SendBatch(client, "log1", 100, 1, ts).GetValueSync();
            Cerr << __FILE__ << ":" << __LINE__ << " Issues: " << res.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "unknown database");

            TString result = RunQuery(connection, "SELECT count(*) FROM `/Root/OlapStore/log1`;");
            UNIT_ASSERT_VALUES_EQUAL(result, "[[0u]]");
        }

        {
            NYdb::NTable::TTableClient client(connection, NYdb::NTable::TClientSettings().AuthToken("alice@builtin"));
            std::pair<EPrimitiveType, i64> ts(pkFirstType, 1000);
            auto res = SendBatch(client, "/Root/OlapStore/log1", 100, 1, ts).GetValueSync();
            Cerr << __FILE__ << ":" << __LINE__ << " Issues: " << res.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

            TString result = RunQuery(connection, "SELECT count(*) FROM `/Root/OlapStore/log1`;");
            UNIT_ASSERT_VALUES_EQUAL(result, "[[100u]]");
        }
    }

    Y_UNIT_TEST(BulkUpsert) {
        for (auto& [type, name] : allowedTypes) {
            TestBulkUpsert<true>(type);
        }
    }

    template<bool NotNull>
    void TestManyTables(const TString& sharding) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        EnableDebugLogs(server);

        auto connection = ConnectToServer(server);

        CreateTestTables<NotNull>(*server.ServerSettings, "log1", sharding);
        CreateTestTables<NotNull>(*server.ServerSettings, "log2", sharding);
        CreateTestTables<NotNull>(*server.ServerSettings, "log3", sharding);

        size_t rowCount = WriteTestRows(connection, "log1", 0, 1, 50);
        UNIT_ASSERT_VALUES_EQUAL(rowCount, 50);

        CompareQueryResults(connection, "log2", "SELECT count(*) FROM <TABLE>;");
        CompareQueryResults(connection, "log3", "SELECT count(*) FROM <TABLE>;");
        CompareQueryResults(connection, "log1", "SELECT count(*) FROM <TABLE>;");

        WriteTestRows(connection, "log2", 0, 10, 15);

        CompareQueryResults(connection, "log2", "SELECT count(*) FROM <TABLE>;");
        CompareQueryResults(connection, "log3", "SELECT count(*) FROM <TABLE>;");
        CompareQueryResults(connection, "log1", "SELECT count(*) FROM <TABLE>;");

        WriteTestRows(connection, "log3", 0, 10, 100);

        CompareQueryResults(connection, "log2", "SELECT count(*) FROM <TABLE>;");
        CompareQueryResults(connection, "log3", "SELECT count(*) FROM <TABLE>;");
        CompareQueryResults(connection, "log1", "SELECT count(*) FROM <TABLE>;");
    }

    Y_UNIT_TEST(ManyTables) {
        for (auto& sharding : testShardingVariants) {
            TestManyTables<true>(sharding);
        }
    }

    template<bool NotNull>
    void TestDuplicateRows(const TString& sharding) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        EnableDebugLogs(server);

        auto connection = ConnectToServer(server);
        NYdb::NTable::TTableClient client(connection);

        TTestOlapTableOptions opts;
        opts.Sharding = sharding;
        CreateOlapTable<NotNull>(*server.ServerSettings, "log1", opts);

        const ui64 batchCount = 100;
        const ui64 batchSize = 1000;

        for (ui64 batch = 0; batch < batchCount; ++batch) {
            TValueBuilder rows;
            rows.BeginList();
            i64 ts = 1000; // Same for all rows!
            for (ui64 i = 0; i < batchSize; ++i) {
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("timestamp").Timestamp(TInstant::MicroSeconds(ts))
                        .AddMember("resource_type").Utf8(i%2 ? "app" : "nginx")
                        .AddMember("resource_id").Utf8("resource_" + ToString((i+13) % 7))
                        .AddMember("uid").Utf8(ToString(i % 23))
                        .AddMember("level").Int32(i % 10)
                        .AddMember("message").Utf8(TString(1000, 'a'))
                        .AddMember("json_payload").JsonDocument("{}")
                        .AddMember("ingested_at").Timestamp(TInstant::MicroSeconds(ts) + TDuration::MilliSeconds(342))
                        .AddMember("saved_at").Timestamp(TInstant::MicroSeconds(ts) + TDuration::MilliSeconds(600))
                        .AddMember("request_id").Utf8(Sprintf("%x", (unsigned int)i))
                    .EndStruct();
            }
            rows.EndList();

            auto res = client.BulkUpsert("/Root/OlapStore/log1", rows.Build()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }

        {
            TString result = RunQuery(connection, "SELECT count(*) FROM `/Root/OlapStore/log1`;");
            UNIT_ASSERT_VALUES_EQUAL(result, "[[322u]]"); // 2 * 7 * 23
        }
    }

    Y_UNIT_TEST(DuplicateRows) {
        for (auto& sharding : testShardingVariants) {
            TestDuplicateRows<true>(sharding);
        }
    }

    template<bool NotNull>
    void TestQuery(const TString& query, const TString& sharding) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig, {}, {}, false, nullptr);

        auto connection = ConnectToServer(server);

        CreateTestTables<NotNull>(*server.ServerSettings, "log1", sharding);

        EnableDebugLogs(server);
        Cerr << "Run on empty table:" << Endl;
        // Run with empty tables first
        CompareQueryResults(connection, "log1", query);

        size_t batchCount = 100;
        size_t batchSize = 50;//500;
        size_t rowCount = WriteTestRows(connection, "log1", 0, batchCount, batchSize);
        UNIT_ASSERT_VALUES_EQUAL(rowCount, batchCount * batchSize);

        EnableDebugLogs(server);

        Cerr << "Run on filled table:" << Endl;
        CompareQueryResults(connection, "log1", query);
    }

    Y_UNIT_TEST(LogLast50) {
        TString query(R"(
            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`
              FROM <TABLE>
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogLast50ByResource) {
        TString query(R"(
            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`
              FROM <TABLE>
              WHERE resource_type == 'app' AND resource_id == 'resource_1'
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogGrepNonExisting) {
        TString query(R"(
            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`
              FROM <TABLE>
              WHERE message LIKE '%non-exisiting string%'
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogGrepExisting) {
        TString query(R"(
            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`
              FROM <TABLE>
              WHERE message LIKE '%message%'
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogNonExistingRequest) {
        TString query(R"(
            $request_id = '0xfaceb00c';

            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`, `request_id`
              FROM <TABLE>
              WHERE request_id == $request_id
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogExistingRequest) {
        TString query(R"(
            $request_id = '1f';

            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`, `request_id`
              FROM <TABLE>
              WHERE request_id == $request_id
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogNonExistingUserId) {
        // Should be fixed after Arrow kernel implementation for JSON_VALUE
        // https://st.yandex-team.ru/KIKIMR-17903
        TString query(R"(
            PRAGMA Kikimr.OptEnableOlapPushdown = "false";
            $user_id = '111';

            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`, `json_payload`
              FROM <TABLE>
              WHERE JSON_VALUE(json_payload, '$.auth.user.id') == $user_id
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogExistingUserId) {
        // Should be fixed after Arrow kernel implementation for JSON_VALUE
        // https://st.yandex-team.ru/KIKIMR-17903
        TString query(R"(
            PRAGMA Kikimr.OptEnableOlapPushdown = "false";
            $user_id = '1000042';

            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`, `json_payload`
              FROM <TABLE>
              WHERE JSON_VALUE(json_payload, '$.auth.user.id') == $user_id
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogPagingBefore) {
        TString query(R"(
            PRAGMA kikimr.OptEnablePredicateExtract = "true";

            $ts = CAST(3000000 AS Timestamp);
            $res_type = CAST('nginx' AS Utf8);
            $res_id = CAST('resource_)' AS Utf8);
            $uid = CAST('10' AS Utf8);

            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`
              FROM <TABLE>
              WHERE resource_type == 'app' AND resource_id == 'resource_1'
                AND (`timestamp`, `resource_type`, `resource_id`, `uid`) < ($ts, $res_type, $res_id, $uid)
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogPagingBetween) {
        TString query(R"(
            PRAGMA kikimr.OptEnablePredicateExtract = "true";

            $ts1 = CAST(2500000 AS Timestamp);
            $res_type1 = CAST('nginx' AS Utf8);
            $res_id1 = CAST('resource_)' AS Utf8);
            $uid1 = CAST('10' AS Utf8);

            $ts2 = CAST(3500000 AS Timestamp);
            $res_type2 = CAST('nginx' AS Utf8);
            $res_id2 = CAST('resource_)' AS Utf8);
            $uid2 = CAST('10' AS Utf8);

            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`
              FROM <TABLE>
              WHERE
                  (`timestamp`, `resource_type`, `resource_id`, `uid`) > ($ts1, $res_type1, $res_id1, $uid1)
                  AND (`timestamp`, `resource_type`, `resource_id`, `uid`) < ($ts2, $res_type2, $res_id2, $uid2)
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogPagingAfter) {
        TString query(R"(
            PRAGMA kikimr.OptEnablePredicateExtract = "true";

            $ts = CAST(3000000 AS Timestamp);
            $res_type = CAST('nginx' AS Utf8);
            $res_id = CAST('resource_)' AS Utf8);
            $uid = CAST('10' AS Utf8);

            $next50 = (
                SELECT *
                FROM <TABLE>
                WHERE resource_type == 'app' AND resource_id == 'resource_1'
                    AND (`timestamp`, `resource_type`, `resource_id`, `uid`) > ($ts, $res_type, $res_id, $uid)
                ORDER BY `timestamp`, `resource_type`, `resource_id`, `uid`
                LIMIT 50
            );

            SELECT `timestamp`, `resource_type`, `resource_id`, `uid`, `level`, `message`
              FROM $next50
              ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC;
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogCountByResource) {
        TString query(R"(
            SELECT count(*)
              FROM <TABLE>
              WHERE resource_type == 'app' AND resource_id == 'resource_1'
              LIMIT 50
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogWithUnionAllAscending) {
        TString query(R"(
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                $until = CAST(4100000 AS Timestamp);
                $uidUntil = CAST(3 AS Utf8);
                $resourceTypeUntil = CAST('app' AS Utf8);
                $resourceIDUntil = CAST('resource_5' AS Utf8);
                $since = CAST(4000000 AS Timestamp);
                $uidSince = CAST(1 AS Utf8);
                $resourceTypeSince = CAST('app' AS Utf8);
                $resourceIDSince = CAST('resource_2' AS Utf8);
                $level0 = CAST(1 AS Int64);
                $level1 = CAST(3 AS Int64);
                $limit = 50;

                $part0 = (SELECT * FROM <TABLE> WHERE `timestamp` < $until AND `timestamp` > $since AND `level` IN ($level0, $level1) ORDER BY `timestamp` ASC, `resource_type` ASC, `resource_id` ASC, `uid` ASC LIMIT $limit);
                $part1 = (SELECT * FROM <TABLE> WHERE `timestamp` = $until AND `resource_type` < $resourceTypeUntil AND `level` IN ($level0, $level1) ORDER BY `timestamp` ASC, `resource_type` ASC, `resource_id` ASC, `uid` ASC LIMIT $limit);
                $part2 = (SELECT * FROM <TABLE> WHERE `timestamp` = $until AND `resource_type` = $resourceTypeUntil AND `resource_id` < $resourceIDUntil AND `level` IN ($level0, $level1) ORDER BY `timestamp` ASC, `resource_type` ASC, `resource_id` ASC, `uid` ASC LIMIT $limit);
                $part3 = (SELECT * FROM <TABLE> WHERE `timestamp` = $until AND `resource_type` = $resourceTypeUntil AND `resource_id` = $resourceIDUntil AND `uid` < $uidUntil AND `level` IN ($level0, $level1) ORDER BY `timestamp` ASC, `resource_type` ASC, `resource_id` ASC, `uid` ASC LIMIT $limit);
                $part4 = (SELECT * FROM <TABLE> WHERE `timestamp` = $since AND `resource_type` > $resourceTypeSince AND `level` IN ($level0, $level1) ORDER BY `timestamp` ASC, `resource_type` ASC, `resource_id` ASC, `uid` ASC LIMIT $limit);
                $part5 = (SELECT * FROM <TABLE> WHERE `timestamp` = $since AND `resource_type` = $resourceTypeSince AND `resource_id` > $resourceIDSince AND `level` IN ($level0, $level1) ORDER BY `timestamp` ASC, `resource_type` ASC, `resource_id` ASC, `uid` ASC LIMIT $limit);
                $part6 = (SELECT * FROM <TABLE> WHERE `timestamp` = $since AND `resource_type` = $resourceTypeSince AND `resource_id` = $resourceIDSince AND `uid` > $uidSince AND `level` IN ($level0, $level1) ORDER BY `timestamp` ASC, `resource_type` ASC, `resource_id` ASC, `uid` ASC LIMIT $limit);
                $data = (SELECT * FROM $part0 UNION ALL SELECT * FROM $part1 UNION ALL SELECT * FROM $part2 UNION ALL SELECT * FROM $part3 UNION ALL SELECT * FROM $part4 UNION ALL SELECT * FROM $part5 UNION ALL SELECT * FROM $part6);
                SELECT * FROM $data ORDER BY `timestamp` ASC, `resource_type` ASC, `resource_id` ASC, `uid` ASC LIMIT $limit;
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogWithUnionAllDescending) {
        TString query(R"(
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                $until = CAST(4093000 AS Timestamp);
                $uidUntil = CAST(3 AS Utf8);
                $resourceTypeUntil = CAST('app' AS Utf8);
                $resourceIDUntil = CAST('resource_5' AS Utf8);
                $since = CAST(4000000 AS Timestamp);
                $uidSince = CAST(1 AS Utf8);
                $resourceTypeSince = CAST('app' AS Utf8);
                $resourceIDSince = CAST('resource_2' AS Utf8);
                $level0 = CAST(1 AS Int64);
                $level1 = CAST(3 AS Int64);
                $limit = 50;

                $part0 = (SELECT * FROM <TABLE> WHERE `timestamp` < $until AND `timestamp` > $since AND `level` IN ($level0, $level1) ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit);
                $part1 = (SELECT * FROM <TABLE> WHERE `timestamp` = $until AND `resource_type` < $resourceTypeUntil AND `level` IN ($level0, $level1) ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit);
                $part2 = (SELECT * FROM <TABLE> WHERE `timestamp` = $until AND `resource_type` = $resourceTypeUntil AND `resource_id` < $resourceIDUntil AND `level` IN ($level0, $level1) ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit);
                $part3 = (SELECT * FROM <TABLE> WHERE `timestamp` = $until AND `resource_type` = $resourceTypeUntil AND `resource_id` = $resourceIDUntil AND `uid` < $uidUntil AND `level` IN ($level0, $level1) ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit);
                $part4 = (SELECT * FROM <TABLE> WHERE `timestamp` = $since AND `resource_type` > $resourceTypeSince AND `level` IN ($level0, $level1) ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit);
                $part5 = (SELECT * FROM <TABLE> WHERE `timestamp` = $since AND `resource_type` = $resourceTypeSince AND `resource_id` > $resourceIDSince AND `level` IN ($level0, $level1) ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit);
                $part6 = (SELECT * FROM <TABLE> WHERE `timestamp` = $since AND `resource_type` = $resourceTypeSince AND `resource_id` = $resourceIDSince AND `uid` > $uidSince AND `level` IN ($level0, $level1) ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit);
                $data = (SELECT * FROM $part0 UNION ALL SELECT * FROM $part1 UNION ALL SELECT * FROM $part2 UNION ALL SELECT * FROM $part3 UNION ALL SELECT * FROM $part4 UNION ALL SELECT * FROM $part5 UNION ALL SELECT * FROM $part6);
                SELECT * FROM $data ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit;
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }

    Y_UNIT_TEST(LogTsRangeDescending) {
        TString query(R"(
                --PRAGMA AnsiInForEmptyOrNullableItemsCollections;

                $until = CAST(4093000 AS Timestamp);
                $since = CAST(4000000 AS Timestamp);

                $limit = 50;

                SELECT *
                FROM <TABLE>
                WHERE
                    `timestamp` <= $until AND
                    `timestamp` >= $since
                ORDER BY `timestamp` DESC, `resource_type` DESC, `resource_id` DESC, `uid` DESC LIMIT $limit;
            )");

        for (auto& sharding : testShardingVariants) {
            TestQuery<true>(query, sharding);
        }
    }
}
