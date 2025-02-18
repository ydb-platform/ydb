#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(YdbTableBulkUpsert) {

    Y_UNIT_TEST(Simple) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Shard", EPrimitiveType::Uint64)
                .AddNullableColumn("App", EPrimitiveType::Utf8)
                .AddNullableColumn("Timestamp", EPrimitiveType::Int64)
                .AddNullableColumn("HttpCode", EPrimitiveType::Uint32)
                .AddNullableColumn("Message", EPrimitiveType::Utf8)
                .AddNullableColumn("Ratio", EPrimitiveType::Double)
                .AddNullableColumn("Binary", EPrimitiveType::String)
                .AddNullableColumn("Empty", EPrimitiveType::Uint32);
            tableBuilder.SetPrimaryKeyColumns({"Shard", "App", "Timestamp"});
            NYdb::NTable::TCreateTableSettings tableSettings;
            tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(32));
            auto result = session.CreateTable("/Root/Logs", tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        const size_t BATCH_COUNT = 1;//1000;
        const size_t BATCH_SIZE = 3;//100;

        TInstant start = TInstant::Now();

        for (ui64 b = 0; b < BATCH_COUNT; ++b) {
            TValueBuilder rows;
            rows.BeginList();
            for (ui64 i = 0; i < BATCH_SIZE; ++i) {
                ui64 shard = (i % 8) << 61;
                i64 ts = i % 23;
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Shard").Uint64(shard)
                        .AddMember("App").Utf8("app_" + ToString(b))
                        .AddMember("Timestamp").Int64(ts)
                        .AddMember("HttpCode").Uint32(200)
                        .AddMember("Message").Utf8("message")
                        .AddMember("Ratio").OptionalDouble(0.33)
                        .AddMember("Binary").OptionalString("\1\1\1\1")
                        .AddMember("Empty").EmptyOptional(EPrimitiveType::Uint32)
                    .EndStruct();
            }
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Logs", rows.Build()).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }
        Cerr << BATCH_COUNT * BATCH_SIZE << " rows in " << TInstant::Now() - start << Endl;

        auto res = session.ExecuteDataQuery(
                        "SELECT count(*) AS __count FROM `/Root/Logs`;",
                        NYdb::NTable::TTxControl::BeginTx().CommitTx()
                    ).ExtractValueSync();

        Cerr << res.GetStatus() << Endl;
        UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);

        auto rs = NYdb::TResultSetParser(res.GetResultSet(0));
        UNIT_ASSERT(rs.TryNextRow());
        ui64 count = rs.ColumnParser("__count").GetUint64();
        Cerr << "count returned " << count << " rows" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(count, BATCH_COUNT * BATCH_SIZE);
    }

    Y_UNIT_TEST(ValidRetry) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Shard", EPrimitiveType::Uint64)
                .AddNullableColumn("Message", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumns({"Shard"});
            NYdb::NTable::TCreateTableSettings tableSettings;
            tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(32));
            auto result = session.CreateTable("/Root/Logs", tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem()
            .BeginStruct()
                .AddMember("Shard").Uint64(1)
                .AddMember("Message").Utf8("message")
            .EndStruct();
        rows.EndList();

        auto v1 = rows.Build();
        auto v2 = v1;

        ui32 limit = 100;
        while (true && --limit) {
            NYdb::NTable::TBulkUpsertSettings upsertSettings;
            upsertSettings.ClientTimeout(TDuration::MicroSeconds(10));
            auto res = client.BulkUpsert("/Root/Logs", std::move(v2), upsertSettings).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            if (res.GetStatus() == EStatus::CLIENT_DEADLINE_EXCEEDED)
                break;
        }

        UNIT_ASSERT_C(limit, "Unable to get client error response");

        {
            auto res = client.BulkUpsert("/Root/Logs", std::move(v1)).GetValueSync();
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = session.ExecuteDataQuery(
                "SELECT Message as col FROM `/Root/Logs`;",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();

            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);

            auto rs = NYdb::TResultSetParser(res.GetResultSet(0));
            UNIT_ASSERT(rs.TryNextRow());
            auto msg = rs.ColumnParser("col").GetOptionalUtf8();
            UNIT_ASSERT_VALUES_EQUAL(msg, "message");
        }
    }

    void TestNull(NYdb::TDriver& connection, EPrimitiveType valueType, bool inKey) {
        TString tableName = Sprintf("/Root/TestNulls_0x%04x", valueType);

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", valueType);
            if (inKey) {
                tableBuilder.SetPrimaryKeyColumns({"Key", "Value"});
            } else {
                tableBuilder.SetPrimaryKeyColumns({"Key"});
            }
            auto result = session.CreateTable(tableName, tableBuilder.Build()).ExtractValueSync();

            Cerr << result.GetIssues().ToString();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                    .BeginStruct()
                    .AddMember("Key").Uint64(22)
                    .AddMember("Value").EmptyOptional(valueType)
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert(tableName, rows.Build()).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = session.ExecuteDataQuery(
                        "SELECT count(*) AS __count FROM `" + tableName + "` WHERE Value IS NOT NULL;",
                        NYdb::NTable::TTxControl::BeginTx().CommitTx()
                    ).ExtractValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);

            auto rs = NYdb::TResultSetParser(res.GetResultSet(0));
            UNIT_ASSERT(rs.TryNextRow());
            ui64 count = rs.ColumnParser("__count").GetUint64();
            Cerr << "count returned " << count << " rows" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(count, 0);
        }

        {
            auto res = session.ExecuteDataQuery(
                        "SELECT count(*) AS __count FROM `" + tableName + "` WHERE Value IS NULL;",
                        NYdb::NTable::TTxControl::BeginTx().CommitTx()
                    ).ExtractValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);

            auto rs = NYdb::TResultSetParser(res.GetResultSet(0));
            UNIT_ASSERT(rs.TryNextRow());
            ui64 count = rs.ColumnParser("__count").GetUint64();
            Cerr << "count returned " << count << " rows" << Endl;
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
        }

        {
            auto result = session.DropTable(tableName).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(Nulls) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        EPrimitiveType ydbTypes[] = {
            EPrimitiveType::Bool,
            EPrimitiveType::Uint8,
            EPrimitiveType::Int32, EPrimitiveType::Uint32,
            EPrimitiveType::Int64, EPrimitiveType::Uint64,
            EPrimitiveType::Float,
            EPrimitiveType::Double,
            EPrimitiveType::Date,
            EPrimitiveType::Datetime,
            EPrimitiveType::Timestamp,
            EPrimitiveType::Interval,
            EPrimitiveType::String,
            EPrimitiveType::Utf8,
            EPrimitiveType::Yson,
            EPrimitiveType::Json,
            EPrimitiveType::JsonDocument,
            EPrimitiveType::DyNumber
        };

        for (EPrimitiveType t : ydbTypes) {
            TestNull(connection, t, false);
        }

        EPrimitiveType ydbKeyTypes[] = {
            EPrimitiveType::Bool,
            EPrimitiveType::Uint8,
            EPrimitiveType::Int32, EPrimitiveType::Uint32,
            EPrimitiveType::Int64, EPrimitiveType::Uint64,
            EPrimitiveType::Date,
            EPrimitiveType::Datetime,
            EPrimitiveType::Timestamp,
            EPrimitiveType::Interval,
            EPrimitiveType::String,
            EPrimitiveType::Utf8,
            EPrimitiveType::DyNumber
        };

        for (EPrimitiveType t : ydbKeyTypes) {
            TestNull(connection, t, true);
        }
    }

    Y_UNIT_TEST(NotNulls) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        TString tableName = "/Root/TestNotNullColumns";

        {  /* create table with not null data column */
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                    .AddNonNullableColumn("Key", EPrimitiveType::Uint64)
                    .AddNonNullableColumn("Value", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable(tableName, tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {  /* create table with not null primary key column */
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                    .AddNonNullableColumn("Key", EPrimitiveType::Uint64)
                    .AddNullableColumn("Value", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable(tableName, tableBuilder.Build()).ExtractValueSync();

            Cerr << result.GetIssues().ToString();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(1)
                    .AddMember("Value").Uint64(10)
                .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert(tableName, rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString();
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }

        {  /* missing not null primary key column */
            TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Value").Uint64(20)
                .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert(tableName, rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString();
            Cerr << res.GetStatus();
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Missing key columns: Key");
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {  /* set null to not null primary key column */
            TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").EmptyOptional(EPrimitiveType::Uint64)
                    .AddMember("Value").Uint64(20)
                .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert(tableName, rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString();
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Received NULL value for not null column");
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = session.DropTable(tableName).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(Errors) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Shard", EPrimitiveType::Uint64)
                .AddNullableColumn("App", EPrimitiveType::Utf8)
                .AddNullableColumn("Timestamp", EPrimitiveType::Int64)
                .AddNullableColumn("Message", EPrimitiveType::Utf8)
                .AddNullableColumn("Ratio", EPrimitiveType::Double);
            tableBuilder.SetPrimaryKeyColumns({"Shard", "App", "Timestamp"});
            NYdb::NTable::TCreateTableSettings tableSettings;
            tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(32));
            auto result = session.CreateTable("/Root/Logs", tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Unknown table
        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Shard").Uint64(42)
                        .AddMember("App").Utf8("app_")
                        .AddMember("Message").OptionalUtf8("message")
                        .AddMember("Ratio").Double(0.33)
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Traces", rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "unknown table");
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
        }

        // Missing key column
        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Shard").Uint64(42)
                        .AddMember("App").Utf8("app_")
                        .AddMember("Message").OptionalUtf8("message")
                        .AddMember("Ratio").Double(0.33)
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Logs", rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Missing key columns: Timestamp");
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("App").Utf8("app_")
                        .AddMember("Timestamp").Int64(-3)
                        .AddMember("Message").Utf8("message")
                        .AddMember("Ratio").Double(0.33)
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Logs", rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Missing key columns: Shard");
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
        }

        // Invalid key column type
        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Shard").Uint64(42)
                        .AddMember("App").Uint64(3)
                        .AddMember("Timestamp").Int64(-3)
                        .AddMember("Message").Utf8("message")
                        .AddMember("Ratio").Double(0.33)
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Logs", rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Type mismatch, got type Uint64 for column App, but expected Utf8");
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
        }

        // Invalid value column type
        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Shard").Uint64(42)
                        .AddMember("App").Utf8("app")
                        .AddMember("Timestamp").Int64(-3)
                        .AddMember("Message").Uint64(3)
                        .AddMember("Ratio").OptionalDouble(0.33)
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Logs", rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Type mismatch, got type Uint64 for column Message, but expected Utf8");
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
        }

        // Missing value column - it's ok
        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Shard").Uint64(42)
                        .AddMember("App").Utf8("app")
                        .AddMember("Timestamp").Int64(-3)
                        .AddMember("Ratio").OptionalDouble(0.33)
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Logs", rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT(res.GetIssues().ToString().empty());
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }

        // Unknown column
        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Shard").Uint64(42)
                        .AddMember("App").Utf8("app_")
                        .AddMember("Timestamp").Int64(-3)
                        .AddMember("HttpCode").Uint32(200)
                        .AddMember("Message").Utf8("message")
                        .AddMember("Ratio").Double(0.33)
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Logs", rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Unknown column: HttpCode");
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(Types) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                    .AddNullableColumn("Key", EPrimitiveType::Uint64)

                    .AddNullableColumn("Column_Bool",  EPrimitiveType::Bool)
                    .AddNullableColumn("Column_Uint8",  EPrimitiveType::Uint8)
                    .AddNullableColumn("Column_Int32",  EPrimitiveType::Int32)
                    .AddNullableColumn("Column_Uint32",  EPrimitiveType::Uint32)
                    .AddNullableColumn("Column_Int64",  EPrimitiveType::Int64)
                    .AddNullableColumn("Column_Uint64",  EPrimitiveType::Uint64)
                    .AddNullableColumn("Column_Float",  EPrimitiveType::Float)
                    .AddNullableColumn("Column_Double",  EPrimitiveType::Double)
                    .AddNullableColumn("Column_Date",  EPrimitiveType::Date)
                    .AddNullableColumn("Column_Datetime",  EPrimitiveType::Datetime)
                    .AddNullableColumn("Column_Timestamp",  EPrimitiveType::Timestamp)
                    .AddNullableColumn("Column_Interval",  EPrimitiveType::Interval)
                    .AddNullableColumn("Column_String",  EPrimitiveType::String)
                    .AddNullableColumn("Column_Utf8",  EPrimitiveType::Utf8)
                    .AddNullableColumn("Column_Yson",  EPrimitiveType::Yson)
                    .AddNullableColumn("Column_Json",  EPrimitiveType::Json)
                    .AddNullableColumn("Column_JsonDocument", EPrimitiveType::JsonDocument)
                    .AddNullableColumn("Column_DyNumber", EPrimitiveType::DyNumber)
                    .AddNullableColumn("Column_Decimal",  TDecimalType(22, 9))
                    .AddNullableColumn("Column_Decimal35",  TDecimalType(35, 10))
// These types are not currently supported for table columns
//                    .AddNullableColumn("Column_Int8",  EPrimitiveType::Int8)
//                    .AddNullableColumn("Column_Int16",  EPrimitiveType::Int16)
//                    .AddNullableColumn("Column_Uint16",  EPrimitiveType::Uint16)
//                    .AddNullableColumn("Column_TzDate",  EPrimitiveType::TzDate)
//                    .AddNullableColumn("Column_TzDatetime",  EPrimitiveType::TzDatetime)
//                    .AddNullableColumn("Column_TzTimestamp",  EPrimitiveType::TzTimestamp)
//                    .AddNullableColumn("Column_Uuid",  EPrimitiveType::Uuid)
                    ;
            tableBuilder.SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable("/Root/Types", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(1)

                        .AddMember("Column_Bool").Bool(0)
//                        .AddMember("Column_Int8").Int8(0)
                        .AddMember("Column_Uint8").Uint8(0)
//                        .AddMember("Column_Int16").Int16(0)
//                        .AddMember("Column_Uint16").Uint16(0)
                        .AddMember("Column_Int32").Int32(0)
                        .AddMember("Column_Uint32").Uint32(0)
                        .AddMember("Column_Int64").Int64(0)
                        .AddMember("Column_Uint64").Uint64(0)
                        .AddMember("Column_Float").Float(0)
                        .AddMember("Column_Double").Double(0)
                        .AddMember("Column_Date").Date(TInstant())
                        .AddMember("Column_Datetime").Datetime(TInstant::ParseRfc822("Fri, 29 Aug 1997 02:14:00 EST"))
                        .AddMember("Column_Timestamp").Timestamp(TInstant())
                        .AddMember("Column_Interval").Interval(0)
//                        .AddMember("Column_TzDate").TzDate("")
//                        .AddMember("Column_TzDatetime").TzDatetime("")
//                        .AddMember("Column_TzTimestamp").TzTimestamp("")
                        .AddMember("Column_String").String("")
                        .AddMember("Column_Utf8").Utf8("")
                        .AddMember("Column_Yson").Yson("{ \"a\" = [ { \"b\" = 1; } ]; }")
                        .AddMember("Column_Json").Json("{}")
                        .AddMember("Column_JsonDocument").JsonDocument("{}")
                        .AddMember("Column_DyNumber").DyNumber("123")
//                        .AddMember("Column_Uuid").Uuid("")
                        .AddMember("Column_Decimal").Decimal(TDecimalValue("99.95", 22, 9))
                        .AddMember("Column_Decimal35").Decimal(TDecimalValue("555555555555555.95", 35, 10))
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Types", rows.Build()).GetValueSync();
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }

        // With Optionals
        {
            TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Key").OptionalUint64(1)

                        .AddMember("Column_Bool").OptionalBool(0)
//                        .AddMember("Column_Int8").OptionalInt8(0)
                        .AddMember("Column_Uint8").OptionalUint8(0)
//                        .AddMember("Column_Int16").OptionalInt16(0)
//                        .AddMember("Column_Uint16").OptionalUint16(0)
                        .AddMember("Column_Int32").OptionalInt32(0)
                        .AddMember("Column_Uint32").OptionalUint32(0)
                        .AddMember("Column_Int64").OptionalInt64(0)
                        .AddMember("Column_Uint64").OptionalUint64(0)
                        .AddMember("Column_Float").OptionalFloat(0)
                        .AddMember("Column_Double").OptionalDouble(0)
                        .AddMember("Column_Date").OptionalDate(TInstant())
                        .AddMember("Column_Datetime").OptionalDatetime(TInstant::ParseRfc822("Fri, 29 Aug 1997 02:14:00 EST"))
                        .AddMember("Column_Timestamp").OptionalTimestamp(TInstant())
                        .AddMember("Column_Interval").OptionalInterval(0)
//                        .AddMember("Column_TzDate").OptionalTzDate("")
//                        .AddMember("Column_TzDatetime").OptionalTzDatetime("")
//                        .AddMember("Column_TzTimestamp").OptionalTzTimestamp("")
                        .AddMember("Column_String").OptionalString("")
                        .AddMember("Column_Utf8").OptionalUtf8("")
                        .AddMember("Column_Yson").OptionalYson("{ \"aaa\" = 1; }")
                        .AddMember("Column_Json").OptionalJson("{}")
                        .AddMember("Column_JsonDocument").OptionalJsonDocument("{}")
                        .AddMember("Column_DyNumber").OptionalDyNumber("42")
//                        .AddMember("Column_Uuid").OptionalUuid("")
                        .AddMember("Column_Decimal").Decimal(TDecimalValue("99.95", 22, 9))
                        .AddMember("Column_Decimal35").Decimal(TDecimalValue("555555555555555.95", 35, 10))
                    .EndStruct();
            rows.EndList();

            auto res = client.BulkUpsert("/Root/Types", rows.Build()).GetValueSync();
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }
    }

    struct TTestRow {
        TString Key1;
        TString Key2;
        TString Key3;
        TString Value1;
        TString Value2;
        TString Value3;
    };

    NYdb::NTable::TBulkUpsertResult TestUpsertRow(NYdb::NTable::TTableClient& client, const TTestRow& row) {
        TValueBuilder rows;
        rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key1").Utf8(row.Key1)
                    .AddMember("Key2").String(row.Key2)
                    .AddMember("Key3").Utf8(row.Key3)
                    .AddMember("Value1").String(row.Value1)
                    .AddMember("Value2").Utf8(row.Value2)
                    .AddMember("Value3").String(row.Value3)
                .EndStruct();
        rows.EndList();

        auto res = client.BulkUpsert("/Root/Limits", rows.Build()).GetValueSync();
        Cerr << res.GetIssues().ToString() << Endl;
        return res;
    }

    Y_UNIT_TEST(Limits) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key1", EPrimitiveType::Utf8)
                .AddNullableColumn("Key2", EPrimitiveType::String)
                .AddNullableColumn("Key3", EPrimitiveType::Utf8)

                .AddNullableColumn("Value1", EPrimitiveType::String)
                .AddNullableColumn("Value2", EPrimitiveType::Utf8)
                .AddNullableColumn("Value3", EPrimitiveType::String);

            tableBuilder.SetPrimaryKeyColumns({"Key1", "Key2", "Key3"});
            auto result = session.CreateTable("/Root/Limits", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = TestUpsertRow(client, {TString(1100000, 'a'), "bb", "", "val1", "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto res = TestUpsertRow(client, {"aa", TString(1100000, 'b'), "", "val1", "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto res = TestUpsertRow(client, {TString(600000, 'a'), TString(500000, 'b'), "", "val1", "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto res = TestUpsertRow(client, {TString(500000, 'a'), TString(500000, 'b'), "", TString(17*1000000, '1'), "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto res = TestUpsertRow(client, {TString(500000, 'a'), TString(500000, 'b'), "", TString(15.9*1000000, '1'), "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = TestUpsertRow(client, {TString(500000, 'a'), "",  TString(500000, 'c'),
                                              TString(15.9*1000000, '1'), TString(15.9*1000000, '2'), TString(15.9*1000000, '3')});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }
    }

    NYdb::NTable::TBulkUpsertResult TestUpsertRow(NYdb::NTable::TTableClient& client, const TString& table, ui8 key, ui8 val) {
        TValueBuilder rows;
        rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint8(key)
                    .AddMember("Value").Uint8(val)
                .EndStruct();
        rows.EndList();

        auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
        Cerr << res.GetIssues().ToString() << Endl;
        return res;
    }

    Y_UNIT_TEST(DataValidation) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                    .AddNullableColumn("Key", EPrimitiveType::Uint32)
                    .AddNullableColumn("Value_Decimal",     TDecimalType(22, 9))
                    .AddNullableColumn("Value_Decimal35",     TDecimalType(35, 10))
                    .AddNullableColumn("Value_Date",        EPrimitiveType::Date)
                    .AddNullableColumn("Value_DateTime",    EPrimitiveType::Datetime)
                    .AddNullableColumn("Value_Timestamp",   EPrimitiveType::Timestamp)
                    .AddNullableColumn("Value_Interval",    EPrimitiveType::Interval)
                    .AddNullableColumn("Value_Utf8",        EPrimitiveType::Utf8)
                    .AddNullableColumn("Value_Yson",        EPrimitiveType::Yson)
                    .AddNullableColumn("Value_Json",        EPrimitiveType::Json)
                    .AddNullableColumn("Value_JsonDocument",EPrimitiveType::JsonDocument)
                    .AddNullableColumn("Value_DyNumber",    EPrimitiveType::DyNumber)
                    ;

            tableBuilder.SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable("/Root/TestInvalidData", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            TString table = "/Root/TestInvalidData";
            TDecimalValue val("0", 22, 9);
            val.Low_ = 0;
            val.Hi_ =  11000000000000000000ULL;
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_Decimal").Decimal(val).EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_Date").Date(TInstant::Days(50000)).EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_DateTime").Datetime(TInstant::Seconds(Max<ui32>())).EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_Timestamp").Timestamp(TInstant::Days(50000)).EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_Interval").Interval(TDuration::Days(50000).MicroSeconds()).EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_Utf8").Utf8("\xff").EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            // NOTE: in this case the protobuf serialization doesn't allow to set invalid utf8 value in a 'string' field
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::CLIENT_INTERNAL_ERROR);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_Yson").Yson("]][").EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_Json").Json("]]]").EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_JsonDocument").JsonDocument("]]]").EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            TString table = "/Root/TestInvalidData";
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).AddMember("Value_DyNumber").DyNumber("[[[]]]").EndStruct().EndList();

            auto res = client.BulkUpsert(table, rows.Build()).GetValueSync();
            Cerr << res.GetStatus() << Endl;
            Cerr << res.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(Uint8) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint8)
                .AddNullableColumn("Value", EPrimitiveType::Uint8);

            tableBuilder.SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable("/Root/ui8", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        for (ui32 i = 0; i < 256; ++i) {
            {
                auto res = TestUpsertRow(client, "/Root/ui8", i, 42);
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SUCCESS);
            }

            {
                auto res = TestUpsertRow(client, "/Root/ui8", 42, i);
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SUCCESS);
            }
        }
    }

    void Index(NYdb::NTable::EIndexType indexType, bool enableBulkUpsertToAsyncIndexedTables = false) {
        auto server = TKikimrWithGrpcAndRootSchema({}, {}, {}, false, nullptr, [=](auto& settings) {
            settings.SetEnableBulkUpsertToAsyncIndexedTables(enableBulkUpsertToAsyncIndexedTables);
        });
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint8)
                .AddNullableColumn("Value", EPrimitiveType::Uint8)
                .AddSecondaryIndex("Value_index", indexType, "Value");

            tableBuilder.SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable("/Root/ui8", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = TestUpsertRow(client, "/Root/ui8", 1, 2);

            if (indexType == NYdb::NTable::EIndexType::GlobalAsync) {
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), enableBulkUpsertToAsyncIndexedTables
                    ? EStatus::SUCCESS : EStatus::SCHEME_ERROR);

                while (enableBulkUpsertToAsyncIndexedTables) {
                    auto it = session.ReadTable("/Root/ui8/Value_index/indexImplTable").ExtractValueSync();
                    auto streamPart = it.ReadNext().GetValueSync();
                    auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
                    if (str == "[[[2u];[1u]]]") {
                        break;
                    }

                    Sleep(TDuration::Seconds(1));
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
            }
        }

        {
            auto res = TestUpsertRow(client, "/Root/ui8/Value_index/indexImplTable", 1, 2);
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(SyncIndexShouldSucceed) {
        Index(NYdb::NTable::EIndexType::GlobalSync);
    }

    Y_UNIT_TEST(AsyncIndexShouldFail) {
        Index(NYdb::NTable::EIndexType::GlobalAsync, false);
    }

    Y_UNIT_TEST(AsyncIndexShouldSucceed) {
        Index(NYdb::NTable::EIndexType::GlobalAsync, true);
    }

    Y_UNIT_TEST(Timeout) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Uint8);

            tableBuilder.SetPrimaryKeyColumns({"Key"});
            NYdb::NTable::TCreateTableSettings tableSettings;
            tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(32));
            auto result = session.CreateTable("/Root/ui32", tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        bool gotTimeout = false;
        bool gotSuccess = false;
        for (ui64 usec = 1; (!gotTimeout || !gotSuccess) && usec < 15*1000*1000; usec *= 2) {
            TValueBuilder rows;
            rows.BeginList();
            for (ui32 k = 0; k < 300; ++k) {
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint32(NumericHash(k))
                        .AddMember("Value").Uint8(k)
                    .EndStruct();
            }
            rows.EndList();

            Cerr << usec << " usec" << Endl;
            auto res = client.BulkUpsert("/Root/ui32", rows.Build(),
                                         NYdb::NTable::TBulkUpsertSettings().OperationTimeout(TDuration::MicroSeconds(usec))
                                         ).GetValueSync();
            Cerr << res.GetIssues().ToString() << Endl;
            if (res.GetStatus() == EStatus::TIMEOUT) {
                gotTimeout = true;
            } else if (res.GetStatus() == EStatus::SUCCESS) {
                gotSuccess = true;
            }
        }
        UNIT_ASSERT(gotTimeout);
        UNIT_ASSERT(gotSuccess);
    }

    Y_UNIT_TEST(Overload) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        {
            TClient annoyingClient(*server.ServerSettings);

            // Create a table with crazy compaction policy that can easily get
            // overloaded
            const char * tableDescr = R"___(
                Name: "kv"
                Columns { Name: "Key"    Type: "Uint64"}
                Columns { Name: "Value"  Type: "Utf8"}
                KeyColumnNames: ["Key"]
                UniformPartitionsCount: 5

                PartitionConfig {
                    CompactionPolicy {
                      InMemSizeToSnapshot: 100
                      InMemStepsToSnapshot: 1
                      InMemForceStepsToSnapshot: 1
                      InMemForceSizeToSnapshot: 200
                      InMemCompactionBrokerQueue: 0
                      MinDataPageSize: 7168
                        Generation {
                            GenerationId: 0
                            SizeToCompact: 100
                            CountToCompact: 1
                            ForceCountToCompact: 1
                            ForceSizeToCompact: 200
                            CompactionBrokerQueue: 1
                            KeepInCache: false
                        }
                    }
                }
            )___";

            NMsgBusProxy::EResponseStatus status = annoyingClient.CreateTable("/Root", tableDescr);
            UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);
        }

        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);

        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::MSGBUS_REQUEST, NActors::NLog::PRI_DEBUG);

        bool gotOverload = false;
        bool gotSuccess = false;
        TString blob(100, 'a');
        TMonotonic start = TMonotonic::Now();
        for (ui64 count = 0; (!gotOverload || !gotSuccess) && count < 100000; count++) {
            TValueBuilder rows;

            rows.BeginList();

            // Add a row for shard 0
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(0)
                    .AddMember("Value").Utf8("")
                .EndStruct();

            // Add some random rows for other shards
            for (int i = 0; i < 10; ++i) {
                ui64 key = NumericHash(count + i) | (1ull << 63);
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(key)
                        .AddMember("Value").Utf8(blob)
                    .EndStruct();
            }

            rows.EndList();

            auto res = client.BulkUpsert("/Root/kv", rows.Build()).GetValueSync();

            if (res.GetStatus() == EStatus::SUCCESS) {
                gotSuccess = true;
                Cerr << ".";
            } else {
                Cerr << Endl << res.GetIssues().ToString() << Endl;
                if (res.GetStatus() == EStatus::OVERLOADED && gotSuccess) {
                    gotOverload = true;
                }
            }
            auto elapsed = TMonotonic::Now() - start;
            if (elapsed >= TDuration::Seconds(5)) {
                break;
            }
        }
        UNIT_ASSERT(gotSuccess);
        UNIT_ASSERT(!gotOverload);
    }

    void CreateTestTable(NYdb::NTable::TTableClient& client) {
        auto session = client.GetSession().ExtractValueSync().GetSession();

        auto tableBuilder = client.GetTableBuilder();
        tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Int32);

        tableBuilder.SetPrimaryKeyColumns({"Key"});
        auto result = session.CreateTable("/Root/Test", tableBuilder.Build()).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        Cerr << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    // Returns the specified status first N times and then returns SUCCESS;
    class TFailStatusInjector {
        const NYdb::EStatus StatusCode;
        const ui32 FailCount;
        ui32 CallCount;
    public:
        TFailStatusInjector(NYdb::EStatus statusCode, ui32 failCount)
            : StatusCode(statusCode)
            , FailCount(failCount)
            , CallCount(0)
        {}

        NYdb::EStatus GetInjectedStatus() {
            ++CallCount;
            if (CallCount > FailCount) {
                return EStatus::SUCCESS;
            }
            return StatusCode;
        }

        ui32 GetCallCount() const {
            return CallCount;
        }
    };

    const EStatus InjectedFailStatuses[] = {
        EStatus::ABORTED,
        EStatus::OVERLOADED,
        EStatus::CLIENT_RESOURCE_EXHAUSTED,
        EStatus::UNAVAILABLE,
        EStatus::BAD_SESSION,
        EStatus::SESSION_BUSY,
        EStatus::NOT_FOUND,
        EStatus::UNDETERMINED,
        EStatus::TRANSPORT_UNAVAILABLE
    };

    NYdb::NTable::TRetryOperationSettings GetTestRetrySettings() {
        NYdb::NTable::TRetryOperationSettings retrySettings;
        retrySettings
            .Idempotent(true)
            .FastBackoffSettings(NYdb::NTable::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(1)).Ceiling(3))
            .SlowBackoffSettings(NYdb::NTable::TBackoffSettings().SlotDuration(TDuration::MilliSeconds(1)).Ceiling(3))
            .MaxRetries(5);
        return retrySettings;
    }

    Y_UNIT_TEST(RetryOperationSync) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient db(connection);

        CreateTestTable(db);

        NYdb::NTable::TRetryOperationSettings retrySettings = GetTestRetrySettings();

        for (EStatus injectedStatus : InjectedFailStatuses) {
            for (ui32 injectCount : {10, 6, 5, 3, 0}) {
                TFailStatusInjector failInjector(injectedStatus, injectCount);

                Cerr << "Injecting " << injectedStatus << " " << injectCount << " times" << Endl;

                auto status = db.RetryOperationSync([&failInjector](NYdb::NTable::TTableClient& db) {
                        EStatus injected = failInjector.GetInjectedStatus();
                        if (injected != EStatus::SUCCESS) {
                            return NYdb::NTable::TBulkUpsertResult(TStatus(injected, NYql::TIssues()));
                        }

                        NYdb::TValueBuilder rows;
                        rows.BeginList()
                            .AddListItem()
                                .BeginStruct()
                                    .AddMember("Key").Uint32(1)
                                    .AddMember("Value").Int32(42)
                                .EndStruct()
                        .EndList();
                        auto status = db.BulkUpsert("Root/Test", rows.Build()).GetValueSync();
                        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
                        return status;
                }, retrySettings);

                Cerr << "Result: " << status.GetStatus() << Endl;

                if (injectCount < retrySettings.MaxRetries_ + 1) {
                    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL(failInjector.GetCallCount(), injectCount + 1);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), injectedStatus, status.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL(failInjector.GetCallCount(), retrySettings.MaxRetries_ + 1);
                }
            }
        }
    }

    Y_UNIT_TEST(RetryOperation) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient db(connection);

        CreateTestTable(db);

        NYdb::NTable::TRetryOperationSettings retrySettings = GetTestRetrySettings();

        for (EStatus injectedStatus : InjectedFailStatuses) {
            for (ui32 injectCount : {10, 6, 5, 3, 0}) {
                TFailStatusInjector failInjector(injectedStatus, injectCount);

                Cerr << "Injecting " << injectedStatus << " " << injectCount << " times" << Endl;

                std::function<NYdb::NTable::TAsyncBulkUpsertResult(NYdb::NTable::TTableClient& tableClient)> bulkUpsertOp =
                    [&failInjector](NYdb::NTable::TTableClient& db) {
                        EStatus injected = failInjector.GetInjectedStatus();
                        if (injected != EStatus::SUCCESS) {
                            return NThreading::MakeFuture<NYdb::NTable::TBulkUpsertResult>(NYdb::NTable::TBulkUpsertResult(TStatus(injected, NYql::TIssues())));
                        }

                        NYdb::TValueBuilder rows;
                        rows.BeginList()
                            .AddListItem()
                                .BeginStruct()
                                    .AddMember("Key").Uint32(1)
                                    .AddMember("Value").Int32(42)
                                .EndStruct()
                        .EndList();
                        return db.BulkUpsert("Root/Test", rows.Build());
                    };

                auto status = db.RetryOperation(bulkUpsertOp, retrySettings).GetValueSync();

                Cerr << "Result: " << status.GetStatus() << Endl;

                if (injectCount < retrySettings.MaxRetries_ + 1) {
                    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL(failInjector.GetCallCount(), injectCount + 1);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), injectedStatus, status.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL(failInjector.GetCallCount(), retrySettings.MaxRetries_ + 1);
                }
            }
        }
    }

    Y_UNIT_TEST(ZeroRows) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        NYdb::NTable::TTableClient db(connection);

        CreateTestTable(db);

        NYdb::TValueBuilder rows;
        rows.BeginList()
            .EndList();
        auto status = db.BulkUpsert("Root/Test", rows.Build()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
    }
}
