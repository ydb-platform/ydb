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

#include <library/cpp/regex/pcre/regexp.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <aws/core/Aws.h>
#include <google/protobuf/util/message_differencer.h>

using namespace NYdb;
using namespace NYdb::NOperation;
using namespace NYdb::NScheme;
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

#define Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(N, ENUM_TYPE) \
    template <ENUM_TYPE Value> \
    struct TTestCase##N : public TCurrentTestCase { \
        TString ParametrizedTestName = #N "-" + ENUM_TYPE##_Name(Value); \
\
        TTestCase##N() : TCurrentTestCase() { \
            Name_ = ParametrizedTestName.c_str(); \
        } \
\
        static THolder<NUnitTest::TBaseTestCase> Create()  { return ::MakeHolder<TTestCase##N<Value>>();  } \
        void Execute_(NUnitTest::TTestContext&) override; \
    }; \
    struct TTestRegistration##N { \
        template <int I, int End> \
        static constexpr void AddTestsForEnumRange() { \
            if constexpr (I < End) { \
                TCurrentTest::AddTest(TTestCase##N<static_cast<ENUM_TYPE>(I)>::Create); \
                AddTestsForEnumRange<I + 1, End>(); \
            } \
        } \
\
        TTestRegistration##N() { \
            AddTestsForEnumRange<0, ENUM_TYPE##_ARRAYSIZE>(); \
        } \
    }; \
    static TTestRegistration##N testRegistration##N; \
    template <ENUM_TYPE Value> \
    void TTestCase##N<Value>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

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
        return true;
    };
}

auto CreateHasIndexChecker(const TString& indexName, EIndexType indexType) {
    return [=](const TTableDescription& tableDescription) {
        for (const auto& indexDesc : tableDescription.GetIndexDescriptions()) {
            if (indexDesc.GetIndexName() == indexName && indexDesc.GetIndexType() == indexType) {
                return true;
            }
        }
        return false;
    };
}

auto CreateHasSerialChecker(i64 nextValue, bool nextUsed) {
    return [=](const TTableDescription& tableDescription) {
        for (const auto& column : tableDescription.GetTableColumns()) {
            if (column.Name == "Key") {
                UNIT_ASSERT(column.SequenceDescription.has_value());
                UNIT_ASSERT(column.SequenceDescription->SetVal.has_value());
                UNIT_ASSERT_VALUES_EQUAL(column.SequenceDescription->SetVal->NextValue, nextValue);
                UNIT_ASSERT_VALUES_EQUAL(column.SequenceDescription->SetVal->NextUsed, nextUsed);
                return true;
            }
        }
        return false;
    };
}

void CheckTableDescription(TSession& session, const TString& path, auto&& checker,
    const TDescribeTableSettings& settings = {}
) {
    UNIT_ASSERT(checker(GetTableDescription(session, path, settings)));
}

void CheckBuildIndexOperationsCleared(TDriver& driver) {
    TOperationClient operationClient(driver);
    const auto result = operationClient.List<TBuildIndexOperation>().GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "issues:\n" << result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetList().empty(), "Build index operations aren't cleared:\n" << result.ToJsonString());
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

    backup(table);

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore(table);
    const auto restoredDescription = GetTableDescription(
        session, indexTablePath, describeSettings
    );
    UNIT_ASSERT_VALUES_EQUAL(restoredDescription.GetPartitionsCount(), indexPartitions);
    const auto& restoredKeyRanges = restoredDescription.GetKeyRanges();
    UNIT_ASSERT_VALUES_EQUAL(restoredKeyRanges.size(), indexPartitions);
    UNIT_ASSERT_EQUAL(restoredKeyRanges, originalKeyRanges);
}

void TestRestoreTableWithSerial(
    const char* table, TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            CREATE TABLE `%s` (
                Key Serial,
                Value Uint32,
                PRIMARY KEY (Key)
            );
        )",
        table
    ));
    ExecuteDataModificationQuery(session, Sprintf(R"(
            UPSERT INTO `%s` (
                Value
            )
            VALUES (1), (2), (3), (4), (5), (6), (7);
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

    CheckTableDescription(session, table, CreateHasSerialChecker(8, false), TDescribeTableSettings().WithSetVal(true));
    CompareResults(GetTableContent(session, table), originalContent);
}

const char* ConvertIndexTypeToSQL(NKikimrSchemeOp::EIndexType indexType) {
    switch (indexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
            return "GLOBAL";
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            return "GLOBAL ASYNC";
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            return "GLOBAL UNIQUE";
        default:
            UNIT_FAIL("No conversion to SQL for this index type");
            return nullptr;
    }
}

NYdb::NTable::EIndexType ConvertIndexTypeToAPI(NKikimrSchemeOp::EIndexType indexType) {
    switch (indexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
            return NYdb::NTable::EIndexType::GlobalSync;
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            return NYdb::NTable::EIndexType::GlobalAsync;
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            return NYdb::NTable::EIndexType::GlobalUnique;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
            return NYdb::NTable::EIndexType::GlobalVectorKMeansTree;
        default:
            UNIT_FAIL("No conversion to API for this index type");
            return NYdb::NTable::EIndexType::Unknown;
    }
}

void TestRestoreTableWithIndex(
    const char* table, const char* index, NKikimrSchemeOp::EIndexType indexType, TSession& session,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key),
                INDEX %s %s ON (Value)
            );
        )",
        table, index, ConvertIndexTypeToSQL(indexType)
    ));

    backup(table);

    // restore deleted table
    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore(table);

    CheckTableDescription(session, table, CreateHasIndexChecker(index, ConvertIndexTypeToAPI(indexType)));
}

void TestRestoreDirectory(const char* directory, TSchemeClient& client, TBackupFunction&& backup, TRestoreFunction&& restore) {
    {
        const auto result = client.MakeDirectory(directory).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    backup(directory);

    {
        const auto result = client.RemoveDirectory(directory).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    restore(directory);

    {
        const auto result = client.DescribePath(directory).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetEntry().Type, ESchemeEntryType::Directory);
    }
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

    // TO DO: test index impl table split boundaries restoration from a backup

    void TestTableBackupRestore() {
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

    void TestTableWithIndexBackupRestore(NKikimrSchemeOp::EIndexType indexType = NKikimrSchemeOp::EIndexTypeGlobal) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%d", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();
        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";

        TestRestoreTableWithIndex(
            table,
            index,
            indexType,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
        CheckBuildIndexOperationsCleared(driver);
    }

    void TestTableWithSerialBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%d", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();
        constexpr const char* table = "/Root/table";

        TestRestoreTableWithSerial(
            table,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    void TestDirectoryBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%d", server.GetPort())));
        TSchemeClient schemeClient(driver);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();
        constexpr const char* directory = "/Root/dir";

        TestRestoreDirectory(
            directory,
            schemeClient,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(TestAllSchemeObjectTypes, NKikimrSchemeOp::EPathType) {
        using namespace NKikimrSchemeOp;

        switch (Value) {
            case EPathTypeTable:
                TestTableBackupRestore();
                break;
            case EPathTypeTableIndex:
                TestTableWithIndexBackupRestore();
                break;
            case EPathTypeSequence:
                TestTableWithSerialBackupRestore();
                break;
            case EPathTypeDir:
                TestDirectoryBackupRestore();
                break;
            case EPathTypePersQueueGroup:
                break; // https://github.com/ydb-platform/ydb/issues/10431
            case EPathTypeSubDomain:
            case EPathTypeExtSubDomain:
                break; // https://github.com/ydb-platform/ydb/issues/10432
            case EPathTypeView:
                break; // https://github.com/ydb-platform/ydb/issues/10433
            case EPathTypeCdcStream:
                break; // https://github.com/ydb-platform/ydb/issues/7054
            case EPathTypeReplication:
            case EPathTypeTransfer:
                break; // https://github.com/ydb-platform/ydb/issues/10436
            case EPathTypeExternalTable:
                break; // https://github.com/ydb-platform/ydb/issues/10438
            case EPathTypeExternalDataSource:
                break; // https://github.com/ydb-platform/ydb/issues/10439
            case EPathTypeResourcePool:
                break; // https://github.com/ydb-platform/ydb/issues/10440
            case EPathTypeKesus:
                break; // https://github.com/ydb-platform/ydb/issues/10444
            case EPathTypeColumnStore:
            case EPathTypeColumnTable:
                break; // https://github.com/ydb-platform/ydb/issues/10459
            case EPathTypeInvalid:
            case EPathTypeBackupCollection:
            case EPathTypeBlobDepot:
                break; // not applicable
            case EPathTypeRtmrVolume:
            case EPathTypeBlockStoreVolume:
            case EPathTypeSolomonVolume:
            case EPathTypeFileStore:
                break; // other projects
            default:
                UNIT_FAIL("Client backup/restore were not implemented for this scheme object");
        }
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(TestAllIndexTypes, NKikimrSchemeOp::EIndexType) {
        using namespace NKikimrSchemeOp;

        switch (Value) {
            case EIndexTypeGlobal:
            case EIndexTypeGlobalAsync:
                TestTableWithIndexBackupRestore(Value);
                break;
            case EIndexTypeGlobalUnique:
                break; // https://github.com/ydb-platform/ydb/issues/10468
            case EIndexTypeGlobalVectorKmeansTree:
                break; // https://github.com/ydb-platform/ydb/issues/10469
            case EIndexTypeInvalid:
                break; // not applicable
            default:
                UNIT_FAIL("Client backup/restore were not implemented for this index type");
        }
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
            testEnv.GetSession(),
            tableBuilder,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
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
            testEnv.GetSession(),
            tableBuilder,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

    void TestTableBackupRestore() {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";

        TestTableContentIsPreserved(
            table,
            testEnv.GetSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

    void TestTableWithIndexBackupRestore(NKikimrSchemeOp::EIndexType indexType = NKikimrSchemeOp::EIndexTypeGlobal) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr const char* index = "value_idx";

        TestRestoreTableWithIndex(
            table,
            index,
            indexType,
            testEnv.GetSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

    void TestTableWithSerialBackupRestore() {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";

        TestRestoreTableWithSerial(
            table,
            testEnv.GetSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port())
        );
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(TestAllSchemeObjectTypes, NKikimrSchemeOp::EPathType) {
        using namespace NKikimrSchemeOp;

        switch (Value) {
            case EPathTypeTable:
                TestTableBackupRestore();
                break;
            case EPathTypeTableIndex:
                TestTableWithIndexBackupRestore();
                break;
            case EPathTypeSequence:
                TestTableWithSerialBackupRestore();
                break;
            case EPathTypeDir:
                break; // https://github.com/ydb-platform/ydb/issues/10430
            case EPathTypePersQueueGroup:
                break; // https://github.com/ydb-platform/ydb/issues/10431
            case EPathTypeSubDomain:
            case EPathTypeExtSubDomain:
                break; // https://github.com/ydb-platform/ydb/issues/10432
            case EPathTypeView:
                break; // https://github.com/ydb-platform/ydb/issues/10433
            case EPathTypeCdcStream:
                break; // https://github.com/ydb-platform/ydb/issues/7054
            case EPathTypeReplication:
            case EPathTypeTransfer:
                break; // https://github.com/ydb-platform/ydb/issues/10436
            case EPathTypeExternalTable:
                break; // https://github.com/ydb-platform/ydb/issues/10438
            case EPathTypeExternalDataSource:
                break; // https://github.com/ydb-platform/ydb/issues/10439
            case EPathTypeResourcePool:
                break; // https://github.com/ydb-platform/ydb/issues/10440
            case EPathTypeKesus:
                break; // https://github.com/ydb-platform/ydb/issues/10444
            case EPathTypeColumnStore:
            case EPathTypeColumnTable:
                break; // https://github.com/ydb-platform/ydb/issues/10459
            case EPathTypeInvalid:
            case EPathTypeBackupCollection:
            case EPathTypeBlobDepot:
                break; // not applicable
            case EPathTypeRtmrVolume:
            case EPathTypeBlockStoreVolume:
            case EPathTypeSolomonVolume:
            case EPathTypeFileStore:
                break; // other projects
            default:
                UNIT_FAIL("S3 backup/restore were not implemented for this scheme object");
        }
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(TestAllIndexTypes, NKikimrSchemeOp::EIndexType) {
        using namespace NKikimrSchemeOp;

        switch (Value) {
            case EIndexTypeGlobal:
            case EIndexTypeGlobalAsync:
                TestTableWithIndexBackupRestore(Value);
                break;
            case EIndexTypeGlobalUnique:
                break; // https://github.com/ydb-platform/ydb/issues/10468
            case EIndexTypeGlobalVectorKmeansTree:
                break; // https://github.com/ydb-platform/ydb/issues/10469
            case EIndexTypeInvalid:
                break; // not applicable
            default:
                UNIT_FAIL("S3 backup/restore were not implemented for this index type");
        }
    }
}
