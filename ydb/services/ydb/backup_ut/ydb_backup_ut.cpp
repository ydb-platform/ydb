#include "ydb_common_ut.h"

#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <ydb/library/backup/backup.h>

#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <aws/core/Aws.h>
#include <google/protobuf/util/message_differencer.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace NYdb::NTable {

bool operator==(const TValue& lhs, const TValue& rhs) {
    return google::protobuf::util::MessageDifferencer::Equals(lhs.GetProto(), rhs.GetProto());
}

bool operator==(const TKeyBound& lhs, const TKeyBound& rhs) {
    return lhs.GetValue() == rhs.GetValue() && lhs.IsInclusive() == rhs.IsInclusive();
}

bool operator==(const TKeyRange& lhs, const TKeyRange& rhs) {
    return lhs.From() == lhs.From() && lhs.To() == rhs.To();
}

}

namespace {

#define DEBUG_HINT (TStringBuilder() << "at line " << __LINE__)

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

TDataQueryResult GetTableContent(TSession& session, const char* table) {
    return ExecuteDataModificationQuery(session, Sprintf(R"(
            SELECT * FROM `%s` ORDER BY Key;
        )", table
    ));
}

void CompareResults(const TDataQueryResult& first, const TDataQueryResult& second) {
    const auto& firstResults = first.GetResultSets();
    const auto& secondResults = second.GetResultSets();

    UNIT_ASSERT_VALUES_EQUAL(firstResults.size(), secondResults.size());
    for (size_t i = 0; i < firstResults.size(); ++i) {
        UNIT_ASSERT_STRINGS_EQUAL(
            FormatResultSetYson(firstResults[i]),
            FormatResultSetYson(secondResults[i])
        );
    }
}

TTableDescription GetTableDescription(TSession& session, const TString& path,
    const TDescribeTableSettings& settings = {}
) {
    auto describeResult = session.DescribeTable(path, settings).ExtractValueSync();
    UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
    return describeResult.GetTableDescription();
}

auto CreateMinPartitionsChecker(ui32 expectedMinPartitions, const TString& debugHint = "") {
    return [=](const TTableDescription& tableDescription) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            tableDescription.GetPartitioningSettings().GetMinPartitionsCount(),
            expectedMinPartitions,
            debugHint
        );
    };
}

void CheckTableDescription(TSession& session, const TString& path, auto&& checker,
    const TDescribeTableSettings& settings = {}
) {
    checker(GetTableDescription(session, path, settings));
}

using TBackupFunction = std::function<void(const char*)>;
using TRestoreFunction = std::function<void(const char*)>;

void TestTableContentIsPreserved(
    const char* table, TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
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
    const auto originalContent = GetTableContent(session, table);

    backup(table);

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore(table);
    CompareResults(GetTableContent(session, table), originalContent);
}

void TestTablePartitioningSettingsArePreserved(
    const char* table, ui32 minPartitions, TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %u
            );
        )",
        table, minPartitions
    ));
    CheckTableDescription(session, table, CreateMinPartitionsChecker(minPartitions, DEBUG_HINT));

    backup(table);

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore(table);
    CheckTableDescription(session, table, CreateMinPartitionsChecker(minPartitions, DEBUG_HINT));
}

void TestIndexTablePartitioningSettingsArePreserved(
    const char* table, const char* index, ui32 minIndexPartitions, TSession& session,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    const TString indexTablePath = JoinFsPaths(table, index, "indexImplTable");

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
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %u
            );
        )", table, index, minIndexPartitions
    ));
    CheckTableDescription(session, indexTablePath, CreateMinPartitionsChecker(minIndexPartitions, DEBUG_HINT));

    backup(table);

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore(table);
    CheckTableDescription(session, indexTablePath, CreateMinPartitionsChecker(minIndexPartitions, DEBUG_HINT));
}

void TestTableSplitBoundariesArePreserved(
    const char* table, ui64 partitions, TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            )
            WITH (
                PARTITION_AT_KEYS = (1, 2, 4, 8, 16, 32, 64, 128, 256)
            );
        )",
        table
    ));
    const auto describeSettings = TDescribeTableSettings()
            .WithTableStatistics(true)
            .WithKeyShardBoundary(true);
    const auto originalTableDescription = GetTableDescription(session, table, describeSettings);
    UNIT_ASSERT_VALUES_EQUAL(originalTableDescription.GetPartitionsCount(), partitions);
    const auto& originalKeyRanges = originalTableDescription.GetKeyRanges();
    UNIT_ASSERT_VALUES_EQUAL(originalKeyRanges.size(), partitions);

    backup(table);

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore(table);
    const auto restoredTableDescription = GetTableDescription(session, table, describeSettings);
    UNIT_ASSERT_VALUES_EQUAL(restoredTableDescription.GetPartitionsCount(), partitions);
    const auto& restoredKeyRanges = restoredTableDescription.GetKeyRanges();
    UNIT_ASSERT_VALUES_EQUAL(restoredKeyRanges.size(), partitions);
    UNIT_ASSERT_EQUAL(restoredTableDescription.GetKeyRanges(), originalKeyRanges);
}

void TestIndexTableSplitBoundariesArePreserved(
    const char* table, const char* index, ui64 indexPartitions, TSession& session,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    const TString indexTablePath = JoinFsPaths(table, index, "indexImplTable");

    {
        TExplicitPartitions indexPartitionBoundaries;
        for (ui32 boundary : {1, 2, 4, 8, 16, 32, 64, 128, 256}) {
            indexPartitionBoundaries.AppendSplitPoints(
                // split boundary is technically always a tuple
                TValueBuilder().BeginTuple().AddElement().OptionalUint32(boundary).EndTuple().Build()
            );
        }
        // By default indexImplTable has auto partitioning by size enabled,
        // so you must set min partition count for partitions to not merge immediately after indexImplTable is built.
        TPartitioningSettingsBuilder partitioningSettingsBuilder;
        partitioningSettingsBuilder
            .SetMinPartitionsCount(indexPartitions)
            .SetMaxPartitionsCount(indexPartitions);

        const auto indexSettings = TGlobalIndexSettings{
            .PartitioningSettings = partitioningSettingsBuilder.Build(),
            .Partitions = std::move(indexPartitionBoundaries)
        };

        auto tableBuilder = TTableBuilder()
            .AddNullableColumn("Key", EPrimitiveType::Uint32)
            .AddNullableColumn("Value", EPrimitiveType::Uint32)
            .SetPrimaryKeyColumn("Key")
            .AddSecondaryIndex(TIndexDescription("byValue", EIndexType::GlobalSync, { "Value" }, {}, { indexSettings }));

        const auto result = session.CreateTable(table, tableBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    const auto describeSettings = TDescribeTableSettings()
            .WithTableStatistics(true)
            .WithKeyShardBoundary(true);
    const auto originalIndexTableDescription = GetTableDescription(
        session, indexTablePath, describeSettings
    );
    UNIT_ASSERT_VALUES_EQUAL(originalIndexTableDescription.GetPartitionsCount(), indexPartitions);
    const auto& originalKeyRanges = originalIndexTableDescription.GetKeyRanges();
    UNIT_ASSERT_VALUES_EQUAL(originalKeyRanges.size(), indexPartitions);

    backup(table);

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore(table);
    const auto restoredIndexTableDescription = GetTableDescription(
        session, indexTablePath, describeSettings
    );
    UNIT_ASSERT_VALUES_EQUAL(restoredIndexTableDescription.GetPartitionsCount(), indexPartitions);
    const auto& restoredKeyRanges = restoredIndexTableDescription.GetKeyRanges();
    UNIT_ASSERT_VALUES_EQUAL(restoredKeyRanges.size(), indexPartitions);
    UNIT_ASSERT_EQUAL(restoredKeyRanges, originalKeyRanges);
}

}

Y_UNIT_TEST_SUITE(BackupRestore) {

    void Restore(NDump::TClient& client, const TFsPath& sourceFile, const TString& dbPath) {
        auto result = client.Restore(sourceFile, dbPath);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    auto CreateBackupLambda(const TDriver& driver, const TFsPath& pathToBackup, bool schemaOnly = false) {
        return [&driver, &pathToBackup, schemaOnly](const char* table) {
            Y_UNUSED(table);
            // TO DO: implement NDump::TClient::Dump and call it instead of BackupFolder
            NBackup::BackupFolder(driver, "/Root", ".", pathToBackup, {}, schemaOnly, false);
        };
    }

    auto CreateRestoreLambda(const TDriver& driver, const TFsPath& pathToBackup) {
        return [&driver, &pathToBackup](const char* table) {
            Y_UNUSED(table);
            NDump::TClient backupClient(driver);
            Restore(backupClient, pathToBackup, "/Root");
        };
    }

    Y_UNIT_TEST(RestoreTableContent) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* table = "/Root/table";

        TestTableContentIsPreserved(
            table,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreTablePartitioningSettings) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* table = "/Root/table";
        constexpr ui32 minPartitions = 10;

        TestTablePartitioningSettingsArePreserved(
            table,
            minPartitions,
            session,
            CreateBackupLambda(driver, pathToBackup, true),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreIndexTablePartitioningSettings) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        constexpr ui32 minIndexPartitions = 10;

        TestIndexTablePartitioningSettingsArePreserved(
            table,
            index,
            minIndexPartitions,
            session,
            CreateBackupLambda(driver, pathToBackup, true),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreTableSplitBoundaries) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* table = "/Root/table";
        constexpr ui64 partitions = 10;

        TestTableSplitBoundariesArePreserved(
            table,
            partitions,
            session,
            CreateBackupLambda(driver, pathToBackup, true),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreIndexTableSplitBoundaries) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        constexpr ui64 indexPartitions = 10;

        TestIndexTableSplitBoundariesArePreserved(
            table,
            index,
            indexPartitions,
            session,
            CreateBackupLambda(driver, pathToBackup, true),
            CreateRestoreLambda(driver, pathToBackup)
        );
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
            : driver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())))
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
            .Endpoint(Sprintf("localhost:%u", s3Port))
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
            .Endpoint(Sprintf("localhost:%u", s3Port))
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

    auto CreateBackupLambda(const TDriver& driver, ui16 s3Port) {
        return [&driver, s3Port](const char* table) {
            NExport::TExportClient exportClient(driver);
            NOperation::TOperationClient operationClient(driver);
            ExportToS3(exportClient, s3Port, operationClient, table, "table");
        };
    }

    auto CreateRestoreLambda(const TDriver& driver, ui16 s3Port) {
        return [&driver, s3Port](const char* table) {
            NImport::TImportClient importClient(driver);
            NOperation::TOperationClient operationClient(driver);
            ImportFromS3(importClient, s3Port, operationClient, "table", table);
        };
    }

    Y_UNIT_TEST(RestoreTableContent) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";

        TestTableContentIsPreserved(
            table,
            testEnv.GetSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

    Y_UNIT_TEST(RestoreTablePartitioningSettings) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr ui32 minPartitions = 10;

        TestTablePartitioningSettingsArePreserved(
            table,
            minPartitions,
            testEnv.GetSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

    Y_UNIT_TEST(RestoreIndexTablePartitioningSettings) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        constexpr ui32 minIndexPartitions = 10;

        TestIndexTablePartitioningSettingsArePreserved(
            table,
            index,
            minIndexPartitions,
            testEnv.GetSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

    Y_UNIT_TEST(RestoreTableSplitBoundaries) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr ui64 partitions = 10;

        TestTableSplitBoundariesArePreserved(
            table,
            partitions,
            testEnv.GetSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

    Y_UNIT_TEST(RestoreIndexTableSplitBoundaries) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        constexpr ui64 indexPartitions = 10;

        TestIndexTableSplitBoundariesArePreserved(
            table,
            index,
            indexPartitions,
            testEnv.GetSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

}
