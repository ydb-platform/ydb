#include "ydb_common_ut.h"

#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <ydb/library/backup/backup.h>

#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

using namespace NYdb;
using namespace NYdb::NTable;
using NYdb::NScheme::TSchemeClient;

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

TDataQueryResult GetTableContent(TSession& session, const char* table,
    const char* keyColumn = "Key"
) {
    return ExecuteDataModificationQuery(session, Sprintf(R"(
            SELECT * FROM `%s` ORDER BY %s;
        )", table, keyColumn
    ));
}

void CompareResults(const std::vector<TResultSet>& first, const std::vector<TResultSet>& second) {
    UNIT_ASSERT_VALUES_EQUAL(first.size(), second.size());
    for (size_t i = 0; i < first.size(); ++i) {
        UNIT_ASSERT_STRINGS_EQUAL(
            FormatResultSetYson(first[i]),
            FormatResultSetYson(second[i])
        );
    }
}

void CompareResults(const TDataQueryResult& first, const TDataQueryResult& second) {
    CompareResults(first.GetResultSets(), second.GetResultSets());
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

// whole database backup
using TBackupFunction = std::function<void(void)>;
// whole database restore
using TRestoreFunction = std::function<void(void)>;

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

    backup();

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();
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

    backup();

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();
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

    backup();

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();
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

    backup();

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();
    const auto restoredTableDescription = GetTableDescription(session, table, describeSettings);
    UNIT_ASSERT_VALUES_EQUAL(restoredTableDescription.GetPartitionsCount(), partitions);
    const auto& restoredKeyRanges = restoredTableDescription.GetKeyRanges();
    UNIT_ASSERT_VALUES_EQUAL(restoredKeyRanges.size(), partitions);
    UNIT_ASSERT_EQUAL(restoredTableDescription.GetKeyRanges(), originalKeyRanges);
}

void TestIndexTableSplitBoundariesArePreserved(
    const char* table, const char* index, ui64 indexPartitions, TSession& session, TTableBuilder& tableBuilder,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    {
        const auto result = session.CreateTable(table, tableBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    const TString indexTablePath = JoinFsPaths(table, index, "indexImplTable");
    const auto describeSettings = TDescribeTableSettings()
            .WithTableStatistics(true)
            .WithKeyShardBoundary(true);

    const auto originalDescription = GetTableDescription(
        session, indexTablePath, describeSettings
    );
    UNIT_ASSERT_VALUES_EQUAL(originalDescription.GetPartitionsCount(), indexPartitions);
    const auto& originalKeyRanges = originalDescription.GetKeyRanges();
    UNIT_ASSERT_VALUES_EQUAL(originalKeyRanges.size(), indexPartitions);

    backup();

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();
    const auto restoredDescription = GetTableDescription(
        session, indexTablePath, describeSettings
    );
    UNIT_ASSERT_VALUES_EQUAL(restoredDescription.GetPartitionsCount(), indexPartitions);
    const auto& restoredKeyRanges = restoredDescription.GetKeyRanges();
    UNIT_ASSERT_VALUES_EQUAL(restoredKeyRanges.size(), indexPartitions);
    UNIT_ASSERT_EQUAL(restoredKeyRanges, originalKeyRanges);
}

}

Y_UNIT_TEST_SUITE(BackupRestore) {

    auto CreateBackupLambda(const TDriver& driver, const TFsPath& pathToBackup, bool schemaOnly = false) {
        return [&driver, &pathToBackup, schemaOnly]() {
            // TO DO: implement NDump::TClient::Dump and call it instead of BackupFolder
            NBackup::BackupFolder(driver, "/Root", ".", pathToBackup, {}, schemaOnly, false);
        };
    }

    auto CreateRestoreLambda(const TDriver& driver, const TFsPath& fsPath, const TString& dbPath = "/Root") {
        return [&]() {
            NDump::TClient backupClient(driver);
            const auto result = backupClient.Restore(fsPath, dbPath);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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
            CreateBackupLambda(driver, pathToBackup),
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
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    // TO DO: test index impl table split boundaries restoration from a backup
}

Y_UNIT_TEST_SUITE(BackupRestoreS3) {

    Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
        NKikimr::InitAwsAPI();
    }

    Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
        NKikimr::ShutdownAwsAPI();
    }

    using NKikimr::NWrappers::NTestHelpers::TS3Mock;

    class TS3TestEnv {
        TKikimrWithGrpcAndRootSchema Server;
        TDriver Driver;
        TTableClient TableClient;
        TSession TableSession;
        NQuery::TQueryClient QueryClient;
        NQuery::TSession QuerySession;
        ui16 S3Port;
        TS3Mock S3Mock;
        // required for exports to function
        TDataShardExportFactory DataShardExportFactory;

    public:
        TS3TestEnv()
            : Driver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", Server.GetPort())))
            , TableClient(Driver)
            , TableSession(TableClient.CreateSession().ExtractValueSync().GetSession())
            , QueryClient(Driver)
            , QuerySession(QueryClient.GetSession().ExtractValueSync().GetSession())
            , S3Port(Server.GetPortManager().GetPort())
            , S3Mock({}, TS3Mock::TSettings(S3Port))
        {
            UNIT_ASSERT_C(S3Mock.Start(), S3Mock.GetError());

            auto& runtime = *Server.GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::EPriority::PRI_DEBUG);
            runtime.GetAppData().DataShardExportFactory = &DataShardExportFactory;
        }

        TKikimrWithGrpcAndRootSchema& GetServer() {
            return Server;
        }

        const TDriver& GetDriver() const {
            return Driver;
        }

        TSession& GetTableSession() {
            return TableSession;
        }

        NQuery::TSession& GetQuerySession() {
            return QuerySession;
        }

        ui16 GetS3Port() const {
            return S3Port;
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

    bool FilterSupportedSchemeObjects(const NYdb::NScheme::TSchemeEntry& entry) {
        return IsIn({
            NYdb::NScheme::ESchemeEntryType::Table,
            NYdb::NScheme::ESchemeEntryType::View,
        }, entry.Type);
    }

    void RecursiveListSourceToItems(TSchemeClient& schemeClient, const TString& source, const TString& destination,
        NExport::TExportToS3Settings& exportSettings
    ) {
        const auto listSettings = NConsoleClient::TRecursiveListSettings().Filter(FilterSupportedSchemeObjects);
        const auto sourceListing = NConsoleClient::RecursiveList(schemeClient, source, listSettings);
        UNIT_ASSERT_C(sourceListing.Status.IsSuccess(), sourceListing.Status.GetIssues());

        for (const auto& entry : sourceListing.Entries) {
            exportSettings.AppendItem({
                .Src = entry.Name,
                .Dst = TStringBuilder() << destination << TStringBuf(entry.Name).RNextTok(source)
            });
        }
    }

    void ExportToS3(
        TSchemeClient& schemeClient,
        NExport::TExportClient& exportClient,
        ui16 s3Port,
        NOperation::TOperationClient& operationClient,
        const TString& source,
        const TString& destination
   ) {
        // The exact values for Bucket, AccessKey and SecretKey do not matter if the S3 backend is TS3Mock.
        // Any non-empty strings should do.
        auto exportSettings = NExport::TExportToS3Settings()
            .Endpoint(Sprintf("localhost:%u", s3Port))
            .Scheme(ES3Scheme::HTTP)
            .Bucket("test_bucket")
            .AccessKey("test_key")
            .SecretKey("test_secret");

        RecursiveListSourceToItems(schemeClient, source, destination, exportSettings);

        const auto response = exportClient.ExportToS3(exportSettings).ExtractValueSync();
        UNIT_ASSERT_C(response.Status().IsSuccess(), response.Status().GetIssues().ToString());
        UNIT_ASSERT_C(WaitForOperation<NExport::TExportToS3Response>(operationClient, response.Id()),
            Sprintf("The export from %s to %s did not complete within the allocated time.",
                source.c_str(), destination.c_str()
            )
        );
    }

    const TString DefaultS3Prefix = "";

    auto CreateBackupLambda(const TDriver& driver, ui16 s3Port, const TString& source = "/Root") {
        return [&, s3Port]() {
            const auto clientSettings = TCommonClientSettings().Database(source);
            TSchemeClient schemeClient(driver, clientSettings);
            NExport::TExportClient exportClient(driver, clientSettings);
            NOperation::TOperationClient operationClient(driver, clientSettings);
            ExportToS3(schemeClient, exportClient, s3Port, operationClient, source, DefaultS3Prefix);
        };
    }

    void ImportFromS3(NImport::TImportClient& importClient, ui16 s3Port, NOperation::TOperationClient& operationClient,
        TVector<NImport::TImportFromS3Settings::TItem>&& items
    ) {
        // The exact values for Bucket, AccessKey and SecretKey do not matter if the S3 backend is TS3Mock.
        // Any non-empty strings should do.
        auto importSettings = NImport::TImportFromS3Settings()
            .Endpoint(Sprintf("localhost:%u", s3Port))
            .Scheme(ES3Scheme::HTTP)
            .Bucket("test_bucket")
            .AccessKey("test_key")
            .SecretKey("test_secret");

        // to do: implement S3 list objects command for TS3Mock to use it here to list the source
        importSettings.Item_ = std::move(items);

        const auto response = importClient.ImportFromS3(importSettings).ExtractValueSync();
        UNIT_ASSERT_C(response.Status().IsSuccess(), response.Status().GetIssues().ToString());
        UNIT_ASSERT_C(WaitForOperation<NImport::TImportFromS3Response>(operationClient, response.Id()),
            "The import did not complete within the allocated time."
        );
    }

    // to do: implement source item list expansion
    auto CreateRestoreLambda(const TDriver& driver, ui16 s3Port, const TVector<TString>& sourceItems, const TString& destinationPrefix = "/Root") {
        return [&, s3Port]() {
            const auto clientSettings = TCommonClientSettings().Database(destinationPrefix);
            NImport::TImportClient importClient(driver, clientSettings);
            NOperation::TOperationClient operationClient(driver, clientSettings);
            using TItem = NImport::TImportFromS3Settings::TItem;
            TVector<TItem> items;
            for (const auto& item : sourceItems) {
                items.emplace_back(TItem{
                    .Src = item,
                    .Dst = TStringBuilder() << destinationPrefix << '/' << item
                });
            }
            ImportFromS3(importClient, s3Port, operationClient, std::move(items));
        };
    }

    Y_UNIT_TEST(RestoreTableContent) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";

        TestTableContentIsPreserved(
            table,
            testEnv.GetTableSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
        );
    }

    Y_UNIT_TEST(RestoreTablePartitioningSettings) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr ui32 minPartitions = 10;

        TestTablePartitioningSettingsArePreserved(
            table,
            minPartitions,
            testEnv.GetTableSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
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
            testEnv.GetTableSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
        );
    }

    Y_UNIT_TEST(RestoreTableSplitBoundaries) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr ui64 partitions = 10;

        TestTableSplitBoundariesArePreserved(
            table,
            partitions,
            testEnv.GetTableSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
        );
    }

    Y_UNIT_TEST(RestoreIndexTableSplitBoundaries) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        constexpr ui64 indexPartitions = 10;

        TExplicitPartitions indexPartitionBoundaries;
        for (ui32 i = 1; i < indexPartitions; ++i) {
            indexPartitionBoundaries.AppendSplitPoints(
                // split boundary is technically always a tuple
                TValueBuilder().BeginTuple().AddElement().OptionalUint32(i * 10).EndTuple().Build()
            );
        }
        // By default indexImplTables have auto partitioning by size enabled.
        // If you don't want the partitions to merge immediately after the indexImplTable is built,
        // you must set the min partition count for the table.
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
            .AddSecondaryIndex(TIndexDescription(index, EIndexType::GlobalSync, { "Value" }, {}, { indexSettings }));

        TestIndexTableSplitBoundariesArePreserved(
            table,
            index,
            indexPartitions,
            testEnv.GetTableSession(),
            tableBuilder,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
        );
    }

    Y_UNIT_TEST(RestoreIndexTableDecimalSplitBoundaries) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        constexpr ui64 indexPartitions = 10;

        constexpr ui8 decimalPrecision = 22;
        constexpr ui8 decimalScale = 9;

        TExplicitPartitions indexPartitionBoundaries;
        for (ui32 i = 1; i < indexPartitions; ++i) {
            TDecimalValue boundary(ToString(i * 10), decimalPrecision, decimalScale);
            indexPartitionBoundaries.AppendSplitPoints(
                // split boundary is technically always a tuple
                TValueBuilder()
                    .BeginTuple().AddElement()
                        .BeginOptional().Decimal(boundary).EndOptional()
                    .EndTuple().Build()
            );
        }
        // By default indexImplTables have auto partitioning by size enabled.
        // If you don't want the partitions to merge immediately after the indexImplTable is built,
        // you must set the min partition count for the table.
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
            .AddNullableColumn("Value", TDecimalType(decimalPrecision, decimalScale))
            .SetPrimaryKeyColumn("Key")
            .AddSecondaryIndex(TIndexDescription(index, EIndexType::GlobalSync, { "Value" }, {}, { indexSettings }));

        TestIndexTableSplitBoundariesArePreserved(
            table,
            index,
            indexPartitions,
            testEnv.GetTableSession(),
            tableBuilder,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
        );
    }

}
