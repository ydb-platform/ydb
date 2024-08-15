#include "ydb_common_ut.h"

#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <ydb/library/backup/backup.h>

#include <library/cpp/testing/hook/hook.h>

#include <aws/core/Aws.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

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

}

Y_UNIT_TEST_SUITE(BackupRestore) {
        
    void Restore(NDump::TClient& client, const TFsPath& sourceFile, const TString& dbPath) {
        auto result = client.Restore(sourceFile, dbPath);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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

Y_UNIT_TEST_SUITE(BackupRestoreS3) {

    Aws::SDKOptions Options;

    Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
        Aws::InitAPI(Options);
    }

    Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
        Aws::ShutdownAPI(Options);
    }

    using NKikimr::NWrappers::NTestHelpers::TS3Mock;

    class TS3TestEnv {
        TKikimrWithGrpcAndRootSchema server;
        TDriver driver;
        TTableClient tableClient;
        TSession session;
        ui16 s3Port;
        TS3Mock s3Mock;
        // required for exports to function
        TDataShardExportFactory dataShardExportFactory;

    public:
        TS3TestEnv()
            : driver(TDriverConfig().SetEndpoint(Sprintf("localhost:%d", server.GetPort())))
            , tableClient(driver)
            , session(tableClient.CreateSession().ExtractValueSync().GetSession())
            , s3Port(server.GetPortManager().GetPort())
            , s3Mock({}, TS3Mock::TSettings(s3Port))
        {
            UNIT_ASSERT_C(s3Mock.Start(), s3Mock.GetError());

            auto& runtime = *server.GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::EPriority::PRI_DEBUG);
            runtime.GetAppData().DataShardExportFactory = &dataShardExportFactory;
        }

        TKikimrWithGrpcAndRootSchema& GetServer() {
            return server;
        }

        const TDriver& GetDriver() const {
            return driver;
        }

        TSession& GetSession() {
            return session;
        }

        ui16 GetS3Port() const {
            return s3Port;
        }
    };

    template <typename TOperation>
    bool WaitForOperation(NOperation::TOperationClient& client, NOperationId::TOperationId id,
        int retries = 10, TDuration sleepDuration = TDuration::MilliSeconds(100)
    ) {
        for (int retry = 0; retry <= retries; ++retry) {
            auto result = client.Get<TOperation>(id).ExtractValueSync();
            if (result.Ready()) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    result.Status().GetStatus(), EStatus::SUCCESS,
                    result.Status().GetIssues().ToString()
                );
                return true;
            }
            Sleep(sleepDuration *= 2);
        }
        return false;
    }

    void ExportToS3(NExport::TExportClient& exportClient, ui16 s3Port, NOperation::TOperationClient& operationClient,
        const TString& source, const TString& destination
   ) {
        // The exact values for Bucket, AccessKey and SecretKey do not matter if the S3 backend is TS3Mock.
        // Any non-empty strings should do.
        const auto exportSettings = NExport::TExportToS3Settings()
            .Endpoint(Sprintf("localhost:%d", s3Port))
            .Scheme(ES3Scheme::HTTP)
            .Bucket("test_bucket")
            .AccessKey("test_key")
            .SecretKey("test_secret")
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = source, .Dst = destination});

        auto response = exportClient.ExportToS3(exportSettings).ExtractValueSync();
        UNIT_ASSERT_C(WaitForOperation<NExport::TExportToS3Response>(operationClient, response.Id()),
            Sprintf("The export from %s to %s did not complete within the allocated time.",
                source.c_str(), destination.c_str()
            )
        );
    }

    void ImportFromS3(NImport::TImportClient& importClient, ui16 s3Port, NOperation::TOperationClient& operationClient,
        const TString& source, const TString& destination
    ) {
        // The exact values for Bucket, AccessKey and SecretKey do not matter if the S3 backend is TS3Mock. 
        // Any non-empty strings should do.
        const auto importSettings = NImport::TImportFromS3Settings()
            .Endpoint(Sprintf("localhost:%d", s3Port))
            .Scheme(ES3Scheme::HTTP)
            .Bucket("test_bucket")
            .AccessKey("test_key")
            .SecretKey("test_secret")
            .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = source, .Dst = destination});

        auto response = importClient.ImportFromS3(importSettings).ExtractValueSync();
        UNIT_ASSERT_C(WaitForOperation<NImport::TImportFromS3Response>(operationClient, response.Id()),
            Sprintf("The import from %s to %s did not complete within the allocated time.",
                source.c_str(), destination.c_str()
            )
        );
    }

    Y_UNIT_TEST(Basic) {
        TS3TestEnv testEnv;

        constexpr const char* table = "/Root/table";
        ExecuteDataDefinitionQuery(testEnv.GetSession(), Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key)
                );
            )",
            table
        ));
        ExecuteDataModificationQuery(testEnv.GetSession(), Sprintf(R"(
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

        NExport::TExportClient exportClient(testEnv.GetDriver());
        NImport::TImportClient importClient(testEnv.GetDriver());
        NOperation::TOperationClient operationClient(testEnv.GetDriver());

        ExportToS3(exportClient, testEnv.GetS3Port(), operationClient, table, "table");

        // The table needs to be dropped before importing from S3 can proceed successfully.
        ExecuteDataDefinitionQuery(testEnv.GetSession(), Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));

        ImportFromS3(importClient, testEnv.GetS3Port(), operationClient, "table", table);
        {
            auto result = ExecuteDataModificationQuery(testEnv.GetSession(), Sprintf(R"(
                    SELECT COUNT(*) FROM `%s`;
                )", table
            ));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(GetSingleResult(result)), 5ull);
        }
    }

    Y_UNIT_TEST(RestoreTablePartitioningSettings) {
        TS3TestEnv testEnv;

        constexpr const char* table = "/Root/table";
        constexpr int minPartitions = 10;
        ExecuteDataDefinitionQuery(testEnv.GetSession(), Sprintf(R"(
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

        CheckTableDescription(testEnv.GetSession(), table, CreateMinPartitionsChecker(minPartitions));

        NExport::TExportClient exportClient(testEnv.GetDriver());
        NImport::TImportClient importClient(testEnv.GetDriver());
        NOperation::TOperationClient operationClient(testEnv.GetDriver());

        ExportToS3(exportClient, testEnv.GetS3Port(), operationClient, table, "table");

        // The table needs to be dropped before importing from S3 can proceed successfully.
        ExecuteDataDefinitionQuery(testEnv.GetSession(), Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));

        ImportFromS3(importClient, testEnv.GetS3Port(), operationClient, "table", table);
        CheckTableDescription(testEnv.GetSession(), table, CreateMinPartitionsChecker(minPartitions));
    }

    Y_UNIT_TEST(RestoreIndexTablePartitioningSettings) {
        TS3TestEnv testEnv;

        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        const TString indexTablePath = JoinFsPaths(table, index, "indexImplTable");
        constexpr int minPartitions = 10;
        ExecuteDataDefinitionQuery(testEnv.GetSession(), Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Uint32,
                    PRIMARY KEY (Key),
                    INDEX %s GLOBAL ON (Value)
                );
            )",
            table, index
        ));
        ExecuteDataDefinitionQuery(testEnv.GetSession(), Sprintf(R"(
                ALTER TABLE `%s` ALTER INDEX %s SET (
                    AUTO_PARTITIONING_BY_LOAD = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d
                );
            )", table, index, minPartitions
        ));

        CheckTableDescription(testEnv.GetSession(), indexTablePath, CreateMinPartitionsChecker(minPartitions));

        NExport::TExportClient exportClient(testEnv.GetDriver());
        NImport::TImportClient importClient(testEnv.GetDriver());
        NOperation::TOperationClient operationClient(testEnv.GetDriver());

        ExportToS3(exportClient, testEnv.GetS3Port(), operationClient, table, "table");

        // The table needs to be dropped before importing from S3 can proceed successfully.
        ExecuteDataDefinitionQuery(testEnv.GetSession(), Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));

        ImportFromS3(importClient, testEnv.GetS3Port(), operationClient, "table", table);
        CheckTableDescription(testEnv.GetSession(), indexTablePath, CreateMinPartitionsChecker(minPartitions));
    }

}
