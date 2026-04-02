#include "ydb_common_ut.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <cmath>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

NYdb::NTable::TBulkUpsertSettings BulkUpsertSettings(const Ydb::Formats::CsvSettings& csvSettings) {
    TString formatSettings;
    UNIT_ASSERT(csvSettings.SerializeToString(&formatSettings));

    NYdb::NTable::TBulkUpsertSettings upsertSettings;
    upsertSettings.FormatSettings(formatSettings);
    return upsertSettings;
}

Ydb::Formats::CsvSettings CsvSettings(
    const TString& delimiter = ",",
    const TString& nullValue = TString(),
    ui32 skipRows = 0,
    bool header = true)
{
    Ydb::Formats::CsvSettings csvSettings;
    csvSettings.set_header(header);
    csvSettings.set_delimiter(delimiter);
    csvSettings.set_skip_rows(skipRows);
    if (!nullValue.empty()) {
        csvSettings.set_null_value(nullValue);
    }
    return csvSettings;
}

void CreateKeyValueTestTable(NYdb::NTable::TTableClient& client, EPrimitiveType keyType = EPrimitiveType::Uint32, EPrimitiveType valueType = EPrimitiveType::Int32) {
    auto session = client.GetSession().ExtractValueSync().GetSession();

    auto tableBuilder = client.GetTableBuilder();
    tableBuilder
        .AddNullableColumn("Key", keyType)
        .AddNullableColumn("Value", valueType);

    tableBuilder.SetPrimaryKeyColumns({"Key"});
    auto result = session.CreateTable("/Root/Test", tableBuilder.Build()).ExtractValueSync();

    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
}

TString StreamQueryToYson(NYdb::NTable::TTableClient& client, const TString& query) {
    auto it = client.StreamExecuteScanQuery(query).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    return NKqp::StreamResultToYson(it);
}

} // namespace

Y_UNIT_TEST_SUITE(YdbTableBulkUpsertCsv) {
    Y_UNIT_TEST(Simple) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

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

        TStringBuilder csv;
        csv << "Shard,App,Timestamp,HttpCode,Message,Ratio,Binary,Empty,AdditionalUnknownColumn\n";
        csv << "0,app_0,0,200,message,0.33,\"\",,thrash\n";
        csv << "2305843009213693952,app_0,1,200,message,0.33,,,thrash\n";
        csv << "4611686018427387904,\"app_0\",2,200,message,0.33,bin_data,,\n";

        auto upsert = client.BulkUpsert(
            "/Root/Logs",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings()))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[0u];["app_0"];[0];[200u];["message"];[0.33];[""];#];
            [[2305843009213693952u];["app_0"];[1];[200u];["message"];[0.33];#;#];
            [[4611686018427387904u];["app_0"];[2];[200u];["message"];[0.33];["bin_data"];#]
        ])", StreamQueryToYson(client, R"(
            SELECT Shard, App, Timestamp, HttpCode, Message, Ratio, Binary, Empty
            FROM `/Root/Logs`
            ORDER BY Shard, App, Timestamp;
        )"));
    }

    Y_UNIT_TEST(SkipRows) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        CreateKeyValueTestTable(client);

        TStringBuilder csv;
        csv << "ignore,ignore\n";
        csv << "ignore,ignore\n";
        csv << "Key,Value\n";
        csv << "1,10\n";
        csv << "2,20\n";

        auto upsert = client.BulkUpsert(
            "/Root/Test",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings(",", {}, 2)))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[1u];[10]];
            [[2u];[20]]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Value
            FROM `/Root/Test`
            ORDER BY Key;
        )"));
    }

    Y_UNIT_TEST(Delimiter) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        CreateKeyValueTestTable(client);

        TStringBuilder csv;
        csv << "Key@Value\n";
        csv << "1@10\n";
        csv << "2@20\n";

        auto upsert = client.BulkUpsert(
            "/Root/Test",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings("@", {})))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[1u];[10]];
            [[2u];[20]]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Value
            FROM `/Root/Test`
            ORDER BY Key;
        )"));
    }

    Y_UNIT_TEST(Header) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        CreateKeyValueTestTable(client);

        TStringBuilder csv;
        csv << "1,10\n";
        csv << "2,20\n";

        auto upsert = client.BulkUpsert(
            "/Root/Test",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings(",", {}, 0, false)))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[1u];[10]];
            [[2u];[20]]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Value
            FROM `/Root/Test`
            ORDER BY Key;
        )"));
    }

    Y_UNIT_TEST(QuotingDisabled) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        CreateKeyValueTestTable(client, EPrimitiveType::Uint32, EPrimitiveType::Utf8);

        TStringBuilder csv;
        csv << "Key,Value\n";
        csv << "1,\"text\"\n";
        csv << "2,'text'\n";

        auto settings = CsvSettings();
        settings.mutable_quoting()->set_disabled(true);

        auto upsert = client.BulkUpsert(
            "/Root/Test",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(settings))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[1u];["\"text\""]];
            [[2u];["'text'"]]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Value
            FROM `/Root/Test`
            ORDER BY Key;
        )"));
    }

    Y_UNIT_TEST(QuoteChar) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        CreateKeyValueTestTable(client, EPrimitiveType::Uint32, EPrimitiveType::Utf8);

        TStringBuilder csv;
        csv << "Key,Value\n";
        csv << "1,*hello,world*\n";

        auto settings = CsvSettings();
        settings.mutable_quoting()->set_quote_char("*");

        auto upsert = client.BulkUpsert(
            "/Root/Test",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(settings))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[1u];["hello,world"]]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Value
            FROM `/Root/Test`
            ORDER BY Key;
        )"));
    }

    Y_UNIT_TEST(DoubleQuoteDisabled) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        CreateKeyValueTestTable(client, EPrimitiveType::Uint32, EPrimitiveType::Utf8);

        TStringBuilder csv;
        csv << "Key,Value\n";
        csv << "1,\"ab\"\"cd\"\n";
        csv << "2,|\"abcd\"|\n";

        auto settings = CsvSettings();
        settings.mutable_quoting()->set_quote_char("|");
        settings.mutable_quoting()->set_double_quote_disabled(true);

        auto upsert = client.BulkUpsert(
            "/Root/Test",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(settings))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[1u];["\"ab\"\"cd\""]];
            [[2u];["\"abcd\""]]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Value
            FROM `/Root/Test`
            ORDER BY Key;
        )"));
    }

    Y_UNIT_TEST(NullValueSetting) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNonNullableColumn("Key", EPrimitiveType::String)
                .AddNonNullableColumn("Key2", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumns({"Key", "Key2"});
            auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        TStringBuilder csv;
        csv << "Key,Key2,Value\n";
        csv << ",0,0\n";
        csv << ",1,1\n";
        csv << ",2,Null\n";

        auto upsert = client.BulkUpsert(
            "/Root/TestTable",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings(",", "Null")))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            ["";0u;[0u]];
            ["";1u;[1u]];
            ["";2u;#]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Key2, Value
            FROM `/Root/TestTable`
            ORDER BY Key, Key2;
        )"));
    }

    Y_UNIT_TEST(ValidateNullsInNotNullColumns) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNonNullableColumn("Key", EPrimitiveType::String)
                .AddNonNullableColumn("Key2", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumns({"Key", "Key2"});
            auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            TStringBuilder csv;
            csv << "Key2,Value\n";
            csv << "0,0\n";
            csv << "1,1\n";

            auto upsert = client.BulkUpsert(
                "/Root/TestTable",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_C(!upsert.IsSuccess(), upsert.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Missing not null column: Key");
        }

        {
            TStringBuilder csv;
            csv << "Key,Key2,Value\n";
            csv << ",0,0\n";
            csv << ",1,1\n";

            auto upsert = client.BulkUpsert(
                "/Root/TestTable",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_C(!upsert.IsSuccess(), upsert.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Received NULL value for not null column: Key");
        }
    }

    Y_UNIT_TEST(ValidateExplicitDefaultValueColumns) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        {
            NQuery::TQueryClient client(connection);

            auto result = client.ExecuteQuery(R"sql(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64 NOT NULL,
                    Value_Default Uint64 DEFAULT 42,
                    PRIMARY KEY (Key)
                );
            )sql", NQuery::TTxControl::NoTx()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NYdb::NTable::TTableClient client(connection);

        {
            // We must specify columns explicitly, regardless of whether they have default values
            TStringBuilder csv;
            csv << "Key\n";
            csv << "0\n";
            csv << "1\n";

            auto upsert = client.BulkUpsert(
                "/Root/TestTable",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_C(!upsert.IsSuccess(), upsert.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Error: Bulk upsert to table '/Root/TestTable' No column 'Value_Default'");
        }

        {
            TStringBuilder csv;
            csv << "Key,Value_Default\n";
            csv << "0,\n";
            csv << "1,15\n";

            auto upsert = client.BulkUpsert(
                "/Root/TestTable",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        }

        // No default values were applied
        NKqp::CompareYson(R"([
            [0u;#];
            [1u;[15u]]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Value_Default
            FROM `/Root/TestTable`
            ORDER BY Key;
        )"));
    }

    Y_UNIT_TEST(Errors) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Shard", EPrimitiveType::Uint64)
                .AddNullableColumn("App", EPrimitiveType::Utf8)
                .AddNullableColumn("Timestamp", EPrimitiveType::Int64)
                .AddNullableColumn("Message", EPrimitiveType::Utf8)
                .AddNullableColumn("Ratio", TDecimalType(35, 10));
            tableBuilder.SetPrimaryKeyColumns({"Shard", "App", "Timestamp"});
            NYdb::NTable::TCreateTableSettings tableSettings;
            tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(32));
            auto result = session.CreateTable("/Root/Logs", tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Unknown table
        {
            TStringBuilder csv;
            csv << "Shard,App,Timestamp,Message,Ratio\n";
            csv << "42,app_,1,message,0.33\n";

            auto upsert = client.BulkUpsert(
                "/Root/Traces",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT(!upsert.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "unknown table");
            UNIT_ASSERT_EQUAL(upsert.GetStatus(), EStatus::SCHEME_ERROR);
        }

        // Missing key column
        {
            TStringBuilder csv;
            csv << "Shard,App,Message,Ratio\n";
            csv << "42,app_,message,0.33\n";

            auto upsert = client.BulkUpsert(
                "/Root/Logs",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT(!upsert.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "No column 'Timestamp' in source batch");
        }
    }

    Y_UNIT_TEST(DecimalPK) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key_Decimal22", TDecimalType(22, 9))
                .AddNonNullableColumn("Key_Decimal35", TDecimalType(35, 10))
                .AddNullableColumn("Value_Decimal22", TDecimalType(22, 9))
                .AddNullableColumn("Value_Decimal35", TDecimalType(35, 10));

            tableBuilder.SetPrimaryKeyColumns({"Key_Decimal22", "Key_Decimal35"});
            auto result = session.CreateTable("/Root/Decimal", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        TStringBuilder csv;
        csv << "Key_Decimal22,Key_Decimal35,Value_Decimal22,Value_Decimal35\n";
        csv << "1.1,555555555555555.55,10,666666666666666.66\n";

        auto upsert = client.BulkUpsert(
            "/Root/Decimal",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings()))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [["1.1"];"555555555555555.55";["10"];["666666666666666.66"]]
        ])", StreamQueryToYson(client, R"(
            SELECT
                CAST(Key_Decimal22 AS String),
                CAST(Key_Decimal35 AS String),
                CAST(Value_Decimal22 AS String),
                CAST(Value_Decimal35 AS String)
            FROM `/Root/Decimal`
            ORDER BY Key_Decimal22, Key_Decimal35;
        )"));
    }

    Y_UNIT_TEST(Types) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Column_Bool", EPrimitiveType::Bool)
                .AddNullableColumn("Column_Uint8", EPrimitiveType::Uint8)
                .AddNullableColumn("Column_Int32", EPrimitiveType::Int32)
                .AddNullableColumn("Column_Uint32", EPrimitiveType::Uint32)
                .AddNullableColumn("Column_Int64", EPrimitiveType::Int64)
                .AddNullableColumn("Column_Uint64", EPrimitiveType::Uint64)
                .AddNullableColumn("Column_Float", EPrimitiveType::Float)
                .AddNullableColumn("Column_Double", EPrimitiveType::Double)
                .AddNullableColumn("Column_Date", EPrimitiveType::Date)
                .AddNullableColumn("Column_Datetime", EPrimitiveType::Datetime)
                .AddNullableColumn("Column_Timestamp", EPrimitiveType::Timestamp)
                .AddNullableColumn("Column_String", EPrimitiveType::String)
                .AddNullableColumn("Column_Utf8", EPrimitiveType::Utf8)
                .AddNullableColumn("Column_Yson", EPrimitiveType::Yson)
                .AddNullableColumn("Column_Json", EPrimitiveType::Json)
                .AddNullableColumn("Column_JsonDocument", EPrimitiveType::JsonDocument)
                .AddNullableColumn("Column_DyNumber", EPrimitiveType::DyNumber)
                .AddNullableColumn("Column_Decimal", TDecimalType(22, 9))
                .AddNullableColumn("Column_Decimal35", TDecimalType(35, 10));
            tableBuilder.SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable("/Root/Types", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        TStringBuilder csv;
        csv << "Key,Column_Bool,Column_Uint8,Column_Int32,Column_Uint32,Column_Int64,Column_Uint64,Column_Float,Column_Double,Column_Date,Column_Datetime,Column_Timestamp,Column_String,Column_Utf8,Column_Yson,Column_Json,Column_JsonDocument,Column_DyNumber,Column_Decimal,Column_Decimal35\n";
        csv << "1,0,1,2,3,4,5,123.7,456.5,1970-01-01,1997-08-29T07:14:00Z,1970-01-01T00:00:00Z,\"string\",\"utf8\",\"{ \"\"a\"\" = [ { \"\"b\"\" = 1; } ]; }\",\"{}\",\"{}\",123,99.95,555555555555555.95\n";

        auto upsert = client.BulkUpsert(
            "/Root/Types",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings()))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[1u];[%false];[1u];[2];[3u];[4];[5u];[123.6999969];[456.5];["1970-01-01"];["1997-08-29T07:14:00Z"];["1970-01-01T00:00:00Z"];["string"];["utf8"];["{ \"a\" = [ { \"b\" = 1; } ]; }"];["{}"];["{}"];[".123e3"];["99.95"];["555555555555555.95"]]
        ])", StreamQueryToYson(client, R"(
            SELECT
                Key,
                Column_Bool,
                Column_Uint8,
                Column_Int32,
                Column_Uint32,
                Column_Int64,
                Column_Uint64,
                Column_Float,
                Column_Double,
                CAST(Column_Date AS String),
                CAST(Column_Datetime AS String),
                CAST(Column_Timestamp AS String),
                Column_String,
                Column_Utf8,
                Column_Yson,
                Column_Json,
                Column_JsonDocument,
                Column_DyNumber,
                CAST(Column_Decimal AS String),
                CAST(Column_Decimal35 AS String)
            FROM `/Root/Types`;
        )"));
    }

    Y_UNIT_TEST(FloatsParsing) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Int32)
                .AddNullableColumn("Value_Double", EPrimitiveType::Double)
                .AddNonNullableColumn("Value_Decimal", TDecimalType(35, 10));

            tableBuilder.SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable("/Root/Floats", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        TStringBuilder csv;
        csv << "Key,Value_Double,Value_Decimal\n";
        csv << "1,1.175494351e-38,1.123456789e-1\n";
        csv << "2,3.402823466e+38,1.123456789e+15\n";
        csv << "3,-inf,3\n";
        csv << "4,inf,4\n";
        csv << "5,nan,5\n";

        auto upsert = client.BulkUpsert(
            "/Root/Floats",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings()))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[1];[1.175494351e-38];"0.1123456789"];
            [[2];[3.402823466e+38];"1123456789000000"]
        ])", StreamQueryToYson(client, R"(
            SELECT Key, Value_Double, Value_Decimal
            FROM `/Root/Floats`
            WHERE Key <= 2 -- infinity has problems with parsing to yson
            ORDER BY Key;
        )"));

        auto read = [&](int key) -> double {
            TStringBuilder query;
            query << "SELECT Value_Double FROM `/Root/Floats` WHERE Key = " << key;
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto part = it.ReadNext().GetValueSync();
            UNIT_ASSERT_C(part.IsSuccess(), part.GetIssues().ToString());
            auto rs = part.ExtractResultSet();
            UNIT_ASSERT_VALUES_EQUAL(rs.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rs.RowsCount(), 1);
            TResultSetParser rsp(rs);
            UNIT_ASSERT(rsp.TryNextRow());
            auto od = rsp.ColumnParser(0).GetOptionalDouble();
            UNIT_ASSERT(od);
            return *od;
        };

        const double v3 = read(3);
        UNIT_ASSERT_C(std::isinf(v3), v3);
        UNIT_ASSERT_C(v3 < 0.0, v3);

        const double v4 = read(4);
        UNIT_ASSERT_C(std::isinf(v4), v4);
        UNIT_ASSERT_C(v4 > 0.0, v4);

        const double v5 = read(5);
        UNIT_ASSERT_C(std::isnan(v5), v5);
    }

    Y_UNIT_TEST(UuidParsing) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNonNullableColumn("Key", EPrimitiveType::Int32)
                .AddNullableColumn("Value_Uuid", EPrimitiveType::Uuid);

            tableBuilder.SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable("/Root/Uuid", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            TStringBuilder csv;
            csv << "Key,Value_Uuid\n";
            csv << "1,65df1ec1-a97d-47b2-ae56-3c023da6ee8c\n";
            csv << "2,65df1ec1a97d47b2ae563c023da6ee8c\n";
            csv << "3,\n";

            auto upsert = client.BulkUpsert(
                "/Root/Uuid",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

            NKqp::CompareYson(R"([
                [1;["65df1ec1-a97d-47b2-ae56-3c023da6ee8c"]];
                [2;["65df1ec1-a97d-47b2-ae56-3c023da6ee8c"]];
                [3;#]
            ])", StreamQueryToYson(client, R"(
                SELECT Key, CAST(Value_Uuid AS String)
                FROM `/Root/Uuid`
                ORDER BY Key;
            )"));
        }

        {
            TStringBuilder csv;
            csv << "Key,Value_Uuid\n";
            csv << "4,not-a-uuid\n";

            auto upsert = client.BulkUpsert(
                "/Root/Uuid",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(upsert.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Failed to convert string 'not-a-uuid' to Uuid");
        }
    }

    struct TLimitCsvRow {
        TString Key1;
        TString Key2;
        TString Key3;
        TString Value1;
        TString Value2;
        TString Value3;
    };

    NYdb::NTable::TBulkUpsertResult BulkUpsertCsvLimitRow(NYdb::NTable::TTableClient& client, const TLimitCsvRow& row) {
        TStringBuilder csv;
        csv << "Key1,Key2,Key3,Value1,Value2,Value3\n";
        csv << row.Key1 << "," << row.Key2 << "," << row.Key3 << "," << row.Value1 << "," << row.Value2 << "," << row.Value3 << "\n";

        auto upsert = client.BulkUpsert(
            "/Root/Limits",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings()))
            .GetValueSync();
        return upsert;
    }

    Y_UNIT_TEST(Limits) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

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
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = BulkUpsertCsvLimitRow(client, {TString(1100000, 'a'), "bb", "", "val1", "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "is larger than the allowed threshold");
        }

        {
            auto res = BulkUpsertCsvLimitRow(client, {"aa", TString(1100000, 'b'), "", "val1", "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "is larger than the allowed threshold");
        }

        {
            auto res = BulkUpsertCsvLimitRow(client, {TString(600000, 'a'), TString(500000, 'b'), "", "val1", "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "is larger than the allowed threshold");
        }

        {
            auto res = BulkUpsertCsvLimitRow(client, {TString(500000, 'a'), TString(500000, 'b'), "", TString(17 * 1000000, '1'), "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "is larger than the allowed threshold");
        }

        {
            auto res = BulkUpsertCsvLimitRow(client, {TString(500000, 'a'), TString(500000, 'b'), "", TString(16 * 1000000, '1'), "val2", "val3"});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = BulkUpsertCsvLimitRow(client, {TString(500000, 'a'), "", TString(500000, 'c'),
                                                      TString(16 * 1000000, '1'), TString(16 * 1000000, '2'), TString(16 * 1000000, '3')});
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }
    }

    TString MakeInvalidDataCsv(
        const TString& valueDecimal = "0",
        const TString& valueDecimal35 = "0",
        const TString& valueDate = "1970-01-01",
        const TString& valueDateTime = "1970-01-01T00:00:00Z",
        const TString& valueTimestamp = "1970-01-01T00:00:00Z",
        const TString& valueUtf8 = "",
        const TString& valueYson = "{}",
        const TString& valueJson = "{}",
        const TString& valueJsonDocument = "{}",
        const TString& valueDyNumber = "0")
    {
        TStringBuilder csv;
        csv << "Key,Value_Decimal,Value_Decimal35,Value_Date,Value_DateTime,Value_Timestamp,Value_Utf8,Value_Yson,Value_Json,Value_JsonDocument,Value_DyNumber\n";
        csv << "1," << valueDecimal
            << "," << valueDecimal35
            << "," << valueDate
            << "," << valueDateTime
            << "," << valueTimestamp
            << ",\"" << valueUtf8 << "\""
            << ",\"" << valueYson << "\""
            << ",\"" << valueJson << "\""
            << ",\"" << valueJsonDocument << "\""
            << "," << valueDyNumber
            << "\n";
        return csv;
    }

    Y_UNIT_TEST(DataValidation) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value_Decimal", TDecimalType(22, 9))
                .AddNullableColumn("Value_Decimal35", TDecimalType(35, 10))
                .AddNullableColumn("Value_Date", EPrimitiveType::Date)
                .AddNullableColumn("Value_DateTime", EPrimitiveType::Datetime)
                .AddNullableColumn("Value_Timestamp", EPrimitiveType::Timestamp)
                .AddNullableColumn("Value_Utf8", EPrimitiveType::Utf8)
                .AddNullableColumn("Value_Yson", EPrimitiveType::Yson)
                .AddNullableColumn("Value_Json", EPrimitiveType::Json)
                .AddNullableColumn("Value_JsonDocument", EPrimitiveType::JsonDocument)
                .AddNullableColumn("Value_DyNumber", EPrimitiveType::DyNumber);

            tableBuilder.SetPrimaryKeyColumns({"Key"});
            auto result = session.CreateTable("/Root/TestInvalidData", tableBuilder.Build()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("10000000000000000000000"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Cannot read CSV: Invalid: In CSV column #1: Row #2: Error converting '10000000000000000000000' to decimal128(22, 9): precision not supported by type");
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("0", "0", "not-a-date"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Cannot read CSV: Invalid: In CSV column #3: Row #2: CSV conversion error to timestamp[s]: invalid value 'not-a-date'");
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("0", "0", "1970-01-01", "not-a-datetime"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Cannot read CSV: Invalid: In CSV column #4: Row #2: CSV conversion error to timestamp[s]: invalid value 'not-a-datetime'");
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("0", "0", "1970-01-01", "1970-01-01T00:00:00Z", "not-a-timestamp"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Cannot read CSV: Invalid: In CSV column #5: Row #2: CSV conversion error to timestamp[us]: invalid value 'not-a-timestamp'");
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("0", "0", "1970-01-01", "1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z", "", "{}", "{}", "]]]"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Invalid JSON for JsonDocument provided: UNEXPECTED_ERROR: Unexpected error, consider reporting this problem as you may have found a bug in simdjson");
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("0", "0", "1970-01-01", "1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z", "", "{}", "{}", "{}", "[[[]]]"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Invalid DyNumber string representation");
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("0", "not decimal"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Cannot read CSV: Invalid: In CSV column #2: Row #2: The string 'not decimal' is not a valid decimal number");
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("123456789012345678901234567890.1234567890123"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Cannot read CSV: Invalid: In CSV column #1: Row #2: Error converting '123456789012345678901234567890.1234567890123' to decimal128(22, 9): precision not supported by type");
        }

        {
            auto res = client.BulkUpsert(
                "/Root/TestInvalidData",
                EDataFormat::CSV,
                MakeInvalidDataCsv("0", "0", "1970-01-02", "1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z", "\xff"),
                {},
                BulkUpsertSettings(CsvSettings()))
                .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(res.GetIssues().ToString(), "Invalid UTF-8 data in column Value_Utf8: Invalid UTF8 sequence at string index 0");
        }
    }

    void WaitAndCompareQueryYson(NYdb::NTable::TTableClient& client, const TString& query, const TString& expectedYson) {
        TString actualYson;
        for (ui32 attempt = 0; attempt != 30; ++attempt) {
            actualYson = StreamQueryToYson(client, query);
            if (actualYson == expectedYson) {
                break;
            }
            Sleep(TDuration::Seconds(1));
        }

        NKqp::CompareYson(expectedYson, actualYson);
    }

    NYdb::NTable::TBulkUpsertResult BulkUpsertCsvUint8Row(NYdb::NTable::TTableClient& client, const TString& path, ui32 key, ui32 value) {
        TStringBuilder csv;
        csv << "Key,Value\n";
        csv << key << "," << value << "\n";

        auto upsert = client.BulkUpsert(
            path,
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettings()))
            .GetValueSync();
        return upsert;
    }

    void IndexTestImpl(NYdb::NTable::EIndexType indexType, bool enableBulkUpsertToAsyncIndexedTables = false) {
        auto server = TKikimrWithGrpcAndRootSchema({}, {}, {}, false, nullptr, [=](auto& settings) {
            settings.SetEnableBulkUpsertToAsyncIndexedTables(enableBulkUpsertToAsyncIndexedTables);
        });
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

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
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto res = BulkUpsertCsvUint8Row(client, "/Root/ui8", 1, 2);

            if (indexType == NYdb::NTable::EIndexType::GlobalAsync) {
                UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), enableBulkUpsertToAsyncIndexedTables
                    ? EStatus::SUCCESS : EStatus::SCHEME_ERROR, res.GetIssues().ToString());

                if (enableBulkUpsertToAsyncIndexedTables) {
                    WaitAndCompareQueryYson(client, R"(
                        SELECT Key, Value
                        FROM `/Root/ui8`
                        ORDER BY Key;
                    )", R"([
                        [[1u];[2u]]
                    ])");

                    WaitAndCompareQueryYson(client, R"(
                        SELECT Value, Key
                        FROM `/Root/ui8` VIEW Value_index
                        ORDER BY Value, Key;
                    )", R"([
                        [[2u];[1u]]
                    ])");
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SCHEME_ERROR, res.GetIssues().ToString());
            }
        }

        {
            auto res = BulkUpsertCsvUint8Row(client, "/Root/ui8/Value_index/indexImplTable", 1, 2);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::BAD_REQUEST, res.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(res.GetIssues().ToString(), "Writing to index implementation tables is not allowed", res.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SyncIndexShouldSucceed) {
        IndexTestImpl(NYdb::NTable::EIndexType::GlobalSync);
    }

    Y_UNIT_TEST(AsyncIndexShouldFail) {
        IndexTestImpl(NYdb::NTable::EIndexType::GlobalAsync, false);
    }

    Y_UNIT_TEST(AsyncIndexShouldSucceed) {
        IndexTestImpl(NYdb::NTable::EIndexType::GlobalAsync, true);
    }

    Y_UNIT_TEST(ZeroRows) {
        TKikimrWithGrpcAndRootSchema server;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        CreateKeyValueTestTable(client);

        auto status = client.BulkUpsert(
            "/Root/Test",
            EDataFormat::CSV,
            TString("Key,Value\n"),
            {},
            BulkUpsertSettings(CsvSettings()))
            .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::BAD_REQUEST, status.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(status.GetIssues().ToString(), "Cannot read CSV", status.GetIssues().ToString());

        NKqp::CompareYson(R"([])", StreamQueryToYson(client, R"(
            SELECT Key, Value
            FROM `/Root/Test`
            ORDER BY Key;
        )"));
    }
}
