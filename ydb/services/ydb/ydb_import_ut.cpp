#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/backup/backup.h>
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

Y_UNIT_TEST_SUITE(BackupRestore) {

    using namespace NYdb::NTable;

    void ExecuteDataDefinitionQuery(TSession& session, const TString& script) {
        const auto result = session.ExecuteSchemeQuery(script).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "script:\n" << script << "\nissues:\n" << result.GetIssues().ToString());
    }

    TDataQueryResult ExecuteDataModificationQuery(TSession& session,
                                                  const TString& script,
                                                  const TExecDataQuerySettings& settings = {}
    ) {
        const auto result = session.ExecuteDataQuery(
            script,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            settings
        ).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "script:\n" << script << "\nissues:\n" << result.GetIssues().ToString());

        return result;
    }

    TValue GetSingleResult(const TDataQueryResult& rawResults) {
        auto resultSetParser = rawResults.GetResultSetParser(0);
        UNIT_ASSERT(resultSetParser.TryNextRow());
        return resultSetParser.GetValue(0);
    }

    ui64 GetUint64(const TValue& value) {
        return TValueParser(value).GetUint64();
    }
        
    void Restore(NDump::TClient& client, const TFsPath& sourceFile, const TString& dbPath) {
        auto result = client.Restore(sourceFile, dbPath);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    auto CreateMinPartitionsChecker(ui64 expectedMinPartitions) {
        return [=](const TTableDescription& tableDescription) {
            return tableDescription.GetPartitioningSettings().GetMinPartitionsCount() == expectedMinPartitions;
        };
    }

    void CheckTableDescription(TSession& session, const TString& path, auto&& checker) {
        auto describeResult = session.DescribeTable(path).ExtractValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        auto tableDescription = describeResult.GetTableDescription();
        Ydb::Table::CreateTableRequest descriptionProto;
        // The purpose of translating to CreateTableRequest is solely to produce a clearer error message.
        tableDescription.SerializeTo(descriptionProto);
        UNIT_ASSERT_C(
            checker(tableDescription),
            descriptionProto.DebugString()
        );
    }

    Y_UNIT_TEST(Basic) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%d", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();

        constexpr const char* table = "/Root/table";
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key)
                );
            )",
            table
        ));
        ExecuteDataModificationQuery(session, Sprintf(R"(
                UPSERT INTO `%s` (
                    Key,
                    Value
                )
                VALUES
                    (1, "one"),
                    (2, "two"),
                    (3, "three"),
                    (4, "four"),
                    (5, "five");
            )",
            table
        ));

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();
        // TO DO: implement NDump::TClient::Dump and call it instead of BackupFolder
        NYdb::NBackup::BackupFolder(driver, "/Root", ".", pathToBackup, {}, false, false);
        
        NDump::TClient backupClient(driver);

        // restore deleted rows in an existing table
        ExecuteDataModificationQuery(session, Sprintf(R"(
                DELETE FROM `%s` WHERE Key > 3;
            )", table
        ));
        Restore(backupClient, pathToBackup, "/Root");
        {
            auto result = ExecuteDataModificationQuery(session, Sprintf(R"(
                    SELECT COUNT(*) FROM `%s`;
                )", table
            ));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(GetSingleResult(result)), 5ull);
        }

        // restore deleted table
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        Restore(backupClient, pathToBackup, "/Root");
        {
            auto result = ExecuteDataModificationQuery(session, Sprintf(R"(
                    SELECT COUNT(*) FROM `%s`;
                )", table
            ));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(GetSingleResult(result)), 5ull);
        }
    }
    
    Y_UNIT_TEST(RestoreTablePartitioningSettings) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%d", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();

        constexpr const char* table = "/Root/table";
        constexpr int minPartitions = 10;
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key)
                )
                WITH (
                    AUTO_PARTITIONING_BY_LOAD = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d
                );
            )",
            table, minPartitions
        ));

        CheckTableDescription(session, table, CreateMinPartitionsChecker(minPartitions));

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();
        // TO DO: implement NDump::TClient::Dump and call it instead of BackupFolder
        NYdb::NBackup::BackupFolder(driver, "/Root", ".", pathToBackup, {}, false, false);
        
        NDump::TClient backupClient(driver);

        // restore deleted table
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        Restore(backupClient, pathToBackup, "/Root");
        CheckTableDescription(session, table, CreateMinPartitionsChecker(minPartitions));
    }

    Y_UNIT_TEST(RestoreIndexTablePartitioningSettings) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%d", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();

        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        const TString indexTablePath = JoinFsPaths(table, index, "indexImplTable");
        constexpr int minPartitions = 10;
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Uint32,
                    PRIMARY KEY (Key),
                    INDEX %s GLOBAL ON (Value)
                );
            )",
            table, index
        ));
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                ALTER TABLE `%s` ALTER INDEX %s SET (
                    AUTO_PARTITIONING_BY_LOAD = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d
                );
            )", table, index, minPartitions
        ));

        CheckTableDescription(session, indexTablePath, CreateMinPartitionsChecker(minPartitions));
                
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();
        // TO DO: implement NDump::TClient::Dump and call it instead of BackupFolder
        NYdb::NBackup::BackupFolder(driver, "/Root", ".", pathToBackup, {}, false, false);
        
        NDump::TClient backupClient(driver);

        // restore deleted table
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        Restore(backupClient, pathToBackup, "/Root");
        CheckTableDescription(session, indexTablePath, CreateMinPartitionsChecker(minPartitions));
    }

}
