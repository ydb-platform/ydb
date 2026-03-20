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

Y_UNIT_TEST_SUITE(YdbTableBulkUpsertCsv) {
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

        Ydb::Formats::CsvSettings csvSettings;
        csvSettings.set_header(true);
        csvSettings.set_delimiter(",");
        csvSettings.set_null_value("Null");

        auto upsert = client.BulkUpsert("/Root/TestTable", EDataFormat::CSV, csv, {}, BulkUpsertSettings(csvSettings)).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT Key, Key2, Value FROM `/Root/TestTable`;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        NKqp::CompareYson(R"([
            ["";0u;[0u]];
            ["";1u;[1u]];
            ["";2u;#]
        ])", NKqp::StreamResultToYson(it));
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

        Ydb::Formats::CsvSettings csvSettings;
        csvSettings.set_header(true);
        csvSettings.set_delimiter(",");

        {
            TStringBuilder csv;
            csv << "Key2,Value\n";
            csv << "0,0\n";
            csv << "1,1\n";

            auto upsert = client.BulkUpsert("/Root/TestTable", EDataFormat::CSV, csv, {}, BulkUpsertSettings(csvSettings)).GetValueSync();
            UNIT_ASSERT_C(!upsert.IsSuccess(), upsert.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Missing key columns: Key");
        }

        {
            TStringBuilder csv;
            csv << "Key,Key2,Value\n";
            csv << ",0,0\n";
            csv << ",1,1\n";

            auto upsert = client.BulkUpsert("/Root/TestTable", EDataFormat::CSV, csv, {}, BulkUpsertSettings(csvSettings)).GetValueSync();
            UNIT_ASSERT_C(!upsert.IsSuccess(), upsert.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Received NULL value for not null column: Key");
        }
    }
}
