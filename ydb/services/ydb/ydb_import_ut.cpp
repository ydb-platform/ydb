#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(YdbImport) {

    /// @sa YdbTableBulkUpsert::Simple
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
            tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(1)); // TODO: 1 -> N
            auto result = session.CreateTable("/Root/Logs", tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        NYdb::NImport::TImportClient importClient(connection);
        NYdb::NImport::TImportYdbDumpDataSettings importSettings;
        importSettings.AppendColumns("Shard");
        importSettings.AppendColumns("App");
        importSettings.AppendColumns("Timestamp");
        importSettings.AppendColumns("HttpCode");
        importSettings.AppendColumns("Message");
        importSettings.AppendColumns("Ratio");
        importSettings.AppendColumns("Binary");
        importSettings.AppendColumns("Empty");

        const size_t BATCH_COUNT = 1;//1000;
        const size_t BATCH_SIZE = 3;//100;

        TInstant start = TInstant::Now();

        for (ui64 b = 0; b < BATCH_COUNT; ++b) {
            TStringBuilder ss;
            for (ui64 i = 0; i < BATCH_SIZE; ++i) {
                ui64 shard = (i % 8) << 61;
                i64 ts = i % 23;

                ss << shard << ","
                    << "\"app_" + ToString(b) << "\","
                    << ts << ","
                    << 200 << ","
                    << "\"message\"" << ","
                    << (double)0.33 << ","
                    << "\"\x01\x01\x01\x01\"" << ","
                    << "null\n";
            }

            auto res = importClient.ImportData("/Root/Logs", ss, importSettings).GetValueSync();

            Cerr << res.GetStatus() << Endl;
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());
        }
        Cerr << BATCH_COUNT * BATCH_SIZE << " rows in " << TInstant::Now() - start << Endl;

        auto res = session.ExecuteDataQuery(
                        "SELECT count(*) AS __count FROM `/Root/Logs`;",
                        NYdb::NTable::TTxControl::BeginTx().CommitTx()
                    ).ExtractValueSync();

        Cerr << res.GetStatus() << Endl;
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

        auto rs = NYdb::TResultSetParser(res.GetResultSet(0));
        UNIT_ASSERT(rs.TryNextRow());
        ui64 count = rs.ColumnParser("__count").GetUint64();
        Cerr << "count returned " << count << " rows" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(count, BATCH_COUNT * BATCH_SIZE);
    }

    Y_UNIT_TEST(EmptyData) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(TStringBuilder()
            << "localhost:" << server.GetPort()));

        {
            NYdb::NTable::TTableClient client(driver);
            auto session = client.GetSession().ExtractValueSync().GetSession();

            auto builder = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable("/Root/Table", builder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            NYdb::NImport::TImportClient client(driver);
            NYdb::NImport::TImportYdbDumpDataSettings settings;
            settings.AppendColumns("Key");
            settings.AppendColumns("Value");

            auto result = client.ImportData("/Root/Table", "", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

}
