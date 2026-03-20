#include "ydb_common_ut.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

using namespace NYdb;

using namespace NYdb;
using namespace NYdb::NTable;

NYdb::NTable::TBulkUpsertSettings BulkUpsertSettings(const Ydb::Formats::CsvSettings& csvSettings) {
    TString formatSettings;
    UNIT_ASSERT(csvSettings.SerializeToString(&formatSettings));

    NYdb::NTable::TBulkUpsertSettings upsertSettings;
    upsertSettings.FormatSettings(formatSettings);
    return upsertSettings;
}

Ydb::Formats::CsvSettings CsvSettingsWithHeader(const TString& delimiter = ",", const TString& nullValue = TString()) {
    Ydb::Formats::CsvSettings csvSettings;
    csvSettings.set_header(true);
    csvSettings.set_delimiter(delimiter);
    if (!nullValue.empty()) {
        csvSettings.set_null_value(nullValue);
    }
    return csvSettings;
}

TString StreamQueryToYson(NYdb::NTable::TTableClient& client, const TString& query) {
    auto it = client.StreamExecuteScanQuery(query).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    return NKqp::StreamResultToYson(it);
}

Y_UNIT_TEST_SUITE(YdbTableBulkUpsertCsv) {
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
            BulkUpsertSettings(CsvSettingsWithHeader()))
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

    Y_UNIT_TEST(NullValueSetting) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

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
            BulkUpsertSettings(CsvSettingsWithHeader(",", "Null")))
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
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

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
                BulkUpsertSettings(CsvSettingsWithHeader()))
                .GetValueSync();
            UNIT_ASSERT_C(!upsert.IsSuccess(), upsert.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Missing key columns: Key");
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
                BulkUpsertSettings(CsvSettingsWithHeader()))
                .GetValueSync();
            UNIT_ASSERT_C(!upsert.IsSuccess(), upsert.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Received NULL value for not null column: Key");
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
            TStringBuilder csv;
            csv << "Shard,App,Timestamp,Message,Ratio\n";
            csv << "42,app_,1,message,0.33\n";

            auto upsert = client.BulkUpsert(
                "/Root/Traces",
                EDataFormat::CSV,
                csv,
                {},
                BulkUpsertSettings(CsvSettingsWithHeader()))
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
                BulkUpsertSettings(CsvSettingsWithHeader()))
                .GetValueSync();
            UNIT_ASSERT(!upsert.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "No column 'Timestamp' in source batch");
        }
    }

    Y_UNIT_TEST(DecimalPK) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

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
        csv << "1.1,555555555555555.55,2.2,666666666666666.66\n";

        auto upsert = client.BulkUpsert(
            "/Root/Decimal",
            EDataFormat::CSV,
            csv,
            {},
            BulkUpsertSettings(CsvSettingsWithHeader()))
            .GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [["1.1"];"555555555555555.55";["2.2"];["666666666666666.66"]]
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
}
