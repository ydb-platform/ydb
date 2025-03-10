#include "ydb_common_ut.h"

#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/draft/ydb_view.pb.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb-cpp-sdk/client/draft/ydb_view.h>
#include <ydb-cpp-sdk/client/export/export.h>
#include <ydb-cpp-sdk/client/import/import.h>
#include <ydb-cpp-sdk/client/operation/operation.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <ydb-cpp-sdk/client/value/value.h>

#include <ydb/library/backup/backup.h>

#include <library/cpp/regex/pcre/regexp.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

using namespace NYdb;
using namespace NYdb::NOperation;
using namespace NYdb::NRateLimiter;
using namespace NYdb::NScheme;
using namespace NYdb::NTable;
using namespace NYdb::NView;

namespace Ydb::Table {

bool operator==(const DescribeExternalDataSourceResult& lhs, const DescribeExternalDataSourceResult& rhs) {
    google::protobuf::util::MessageDifferencer differencer;
    differencer.IgnoreField(DescribeExternalDataSourceResult::GetDescriptor()->FindFieldByName("self"));
    return differencer.Compare(lhs, rhs);
}

bool operator==(const DescribeExternalTableResult& lhs, const DescribeExternalTableResult& rhs) {
    google::protobuf::util::MessageDifferencer differencer;
    differencer.IgnoreField(DescribeExternalTableResult::GetDescriptor()->FindFieldByName("self"));
    return differencer.Compare(lhs, rhs);
}

}

namespace Ydb::RateLimiter {

bool operator==(const HierarchicalDrrSettings& lhs, const HierarchicalDrrSettings& rhs) {
    return google::protobuf::util::MessageDifferencer::Equals(lhs, rhs);
}

bool operator==(const MeteringConfig& lhs, const MeteringConfig& rhs) {
    return google::protobuf::util::MessageDifferencer::Equals(lhs, rhs);
}

}

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

namespace NYdb::NRateLimiter {

bool operator==(
    const TDescribeResourceResult::THierarchicalDrrProps& lhs,
    const TDescribeResourceResult::THierarchicalDrrProps& rhs
) {
    Ydb::RateLimiter::HierarchicalDrrSettings left;
    lhs.SerializeTo(left);
    Ydb::RateLimiter::HierarchicalDrrSettings right;
    rhs.SerializeTo(right);
    return left == right;
}

bool operator==(const TMeteringConfig& lhs, const TMeteringConfig& rhs) {
    Ydb::RateLimiter::MeteringConfig left;
    lhs.SerializeTo(left);
    Ydb::RateLimiter::MeteringConfig right;
    rhs.SerializeTo(right);
    return left == right;
}

bool operator==(const TDescribeResourceResult& lhs, const TDescribeResourceResult& rhs) {
    UNIT_ASSERT_C(lhs.IsSuccess(), lhs.GetIssues().ToString());
    UNIT_ASSERT_C(rhs.IsSuccess(), rhs.GetIssues().ToString());
    return lhs.GetHierarchicalDrrProps() == rhs.GetHierarchicalDrrProps()
        && lhs.GetMeteringConfig() == rhs.GetMeteringConfig();
}

}

namespace NYdb {

struct TTenantsTestSettings : TKikimrTestSettings {
    static constexpr bool PrecreatePools = false;
};

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

NQuery::TExecuteQueryResult ExecuteQuery(NQuery::TSession& session, const TString& script, bool isDDL = false) {
    const auto result = session.ExecuteQuery(
        script,
        isDDL ? NQuery::TTxControl::NoTx() : NQuery::TTxControl::BeginTx().CommitTx()
    ).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "query:\n" << script << "\nissues:\n" << result.GetIssues().ToString());
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

NQuery::TExecuteQueryResult GetTableContent(NQuery::TSession& session, const char* table,
    const char* keyColumn = "Key"
) {
    return ExecuteQuery(session, Sprintf(R"(
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

void CompareResults(const NQuery::TExecuteQueryResult& first, const NQuery::TExecuteQueryResult& second) {
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
        return true;
    };
}

auto CreateHasIndexChecker(const TString& indexName, EIndexType indexType) {
    return [=](const TTableDescription& tableDescription) {
        for (const auto& indexDesc : tableDescription.GetIndexDescriptions()) {
            if (indexDesc.GetIndexName() != indexName) {
                continue;
            }
            if (indexDesc.GetIndexType() != indexType) {
                continue;
            }
            if (indexDesc.GetIndexColumns().size() != 1) {
                continue;
            }
            if (indexDesc.GetDataColumns().size() != 0) {
                continue;
            }
            if (indexDesc.GetIndexColumns()[0] != "Value") {
                continue;
            }
            if (indexType != NYdb::NTable::EIndexType::GlobalVectorKMeansTree) {
                return true;
            }
            auto* settings = std::get_if<TKMeansTreeSettings>(&indexDesc.GetIndexSettings());
            UNIT_ASSERT(settings);
            if (settings->Settings.Metric != NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct) {
                continue;
            }
            if (settings->Settings.VectorType != NYdb::NTable::TVectorIndexSettings::EVectorType::Float) {
                continue;
            }
            if (settings->Settings.VectorDimension != 768) {
                continue;
            }
            if (settings->Levels != 2) {
                continue;
            }
            if (settings->Clusters != 80) {
                continue;
            }
            return true;
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

TViewDescription DescribeView(TViewClient& viewClient, const TString& path) {
    const auto describeResult = viewClient.DescribeView(path).ExtractValueSync();
    UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
    return describeResult.GetViewDescription();
}

NTopic::TTopicDescription DescribeTopic(NTopic::TTopicClient& topicClient, const TString& path) {
    const auto describeResult = topicClient.DescribeTopic(path).ExtractValueSync();
    UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
    return describeResult.GetTopicDescription();
}

std::vector<TChangefeedDescription> DescribeChangefeeds(TSession& session, const TString& tablePath) {
    auto describeResult = session.DescribeTable(tablePath).ExtractValueSync();
    UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
    return describeResult.GetTableDescription().GetChangefeedDescriptions();
}

// note: the storage pool kind must be preconfigured in the server
void CreateDatabase(TTenants& tenants, TStringBuf path, TStringBuf storagePoolKind) {
    Ydb::Cms::CreateDatabaseRequest request;
    request.set_path(path);
    auto& storage = *request.mutable_resources()->add_storage_units();
    storage.set_unit_kind(storagePoolKind);
    storage.set_count(1);

    tenants.CreateTenant(std::move(request));
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

    backup();

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();

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
    TString query;
    if (indexType == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree) {
        query = Sprintf(R"(CREATE TABLE `%s` (
            Key Uint32,
            Value String,
            PRIMARY KEY (Key),
            INDEX %s GLOBAL USING vector_kmeans_tree
                ON (Value)
                WITH (similarity=inner_product, vector_type=float, vector_dimension=768, levels=2, clusters=80)
        );)", table, index);
    } else {
        query = Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key),
                INDEX %s %s ON (Value)
            );
        )", table, index, ConvertIndexTypeToSQL(indexType));
    }
    ExecuteDataDefinitionQuery(session, query);

    backup();

    // restore deleted table
    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();

    CheckTableDescription(session, table, CreateHasIndexChecker(index, ConvertIndexTypeToAPI(indexType)));
}

void TestRestoreDirectory(const char* directory, TSchemeClient& client, TBackupFunction&& backup, TRestoreFunction&& restore) {
    {
        const auto result = client.MakeDirectory(directory).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    backup();

    {
        const auto result = client.RemoveDirectory(directory).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    restore();

    {
        const auto result = client.DescribePath(directory).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetEntry().Type, ESchemeEntryType::Directory);
    }
}

void TestViewOutputIsPreserved(
    const char* view, NQuery::TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    constexpr const char* viewQuery = R"(
        SELECT 1 AS Key
        UNION
        SELECT 2 AS Key
        UNION
        SELECT 3 AS Key;
    )";
    ExecuteQuery(session, Sprintf(R"(
                CREATE VIEW `%s` WITH security_invoker = TRUE AS %s;
            )", view, viewQuery
        ), true
    );
    const auto originalContent = GetTableContent(session, view);

    backup();

    ExecuteQuery(session, Sprintf(R"(
                DROP VIEW `%s`;
            )", view
        ), true
    );

    restore();
    CompareResults(GetTableContent(session, view), originalContent);
}

void TestViewQueryTextIsPreserved(
    const char* view, TViewClient& viewClient, NQuery::TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    constexpr const char* viewQuery = "SELECT 42";
    ExecuteQuery(session, Sprintf(R"(
                CREATE VIEW `%s` WITH security_invoker = TRUE AS %s;
            )", view, viewQuery
        ), true
    );
    const auto originalText = DescribeView(viewClient, view).GetQueryText();
    UNIT_ASSERT_STRINGS_EQUAL(originalText, viewQuery);

    backup();

    ExecuteQuery(session, Sprintf(R"(
                DROP VIEW `%s`;
            )", view
        ), true
    );

    restore();
    UNIT_ASSERT_STRINGS_EQUAL(
        DescribeView(viewClient, view).GetQueryText(),
        originalText
    );
}

// The view might be restored to a different path from the original.
void TestViewReferenceTableIsPreserved(
    const char* view, const char* table, const char* restoredView, NQuery::TSession& session,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    ExecuteQuery(session, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key)
                );
            )", table
        ), true
    );
    ExecuteQuery(session, Sprintf(R"(
            UPSERT INTO `%s` (
                Key,
                Value
            )
            VALUES
                (1, "one"),
                (2, "two"),
                (3, "three");
        )",
        table
    ));

    const TString viewQuery = Sprintf(R"(
            SELECT * FROM `%s`
        )", table
    );
    ExecuteQuery(session, Sprintf(R"(
                CREATE VIEW `%s` WITH security_invoker = TRUE AS %s;
            )", view, viewQuery.c_str()
        ), true
    );
    const auto originalContent = GetTableContent(session, view);

    backup();

    ExecuteQuery(session, Sprintf(R"(
                DROP VIEW `%s`;
            )", view
        ), true
    );
    ExecuteQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ), true
    );

    restore();
    CompareResults(GetTableContent(session, restoredView), originalContent);
}

void TestViewReferenceTableIsPreserved(
    const char* view, const char* table, NQuery::TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    // view is restored to the original path
    TestViewReferenceTableIsPreserved(view, table, view, session, std::move(backup), std::move(restore));
}

void TestViewDependentOnAnotherViewIsRestored(
    const char* baseView, const char* dependentView, NQuery::TSession& session,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    ExecuteQuery(session, Sprintf(R"(
                CREATE VIEW `%s` WITH security_invoker = TRUE AS SELECT 1 AS Key;
            )", baseView
        ), true
    );
    ExecuteQuery(session, Sprintf(R"(
                CREATE VIEW `%s` WITH security_invoker = TRUE AS SELECT * FROM `%s`;
            )", dependentView, baseView
        ), true
    );
    const auto originalContent = GetTableContent(session, dependentView);

    backup();

    ExecuteQuery(session, Sprintf(R"(
                DROP VIEW `%s`;
                DROP VIEW `%s`;
            )", baseView, dependentView
        ), true
    );

    restore();
    CompareResults(GetTableContent(session, dependentView), originalContent);
}

std::pair<std::vector<TString>, std::vector<TString>> 
GetChangefeedAndTopicDescriptions(const char* table, TSession& session, NTopic::TTopicClient& topicClient) {
    auto describeChangefeeds = DescribeChangefeeds(session, table);
    const auto vectorSize = describeChangefeeds.size();

    std::vector<TString> changefeedsStr(vectorSize);
    std::transform(describeChangefeeds.begin(), describeChangefeeds.end(), changefeedsStr.begin(), [=](TChangefeedDescription changefeedDesc){
        return changefeedDesc.ToString();
    });

    std::vector<TString> topicsStr(vectorSize);
    std::transform(describeChangefeeds.begin(), describeChangefeeds.end(), topicsStr.begin(), [table, &topicClient](TChangefeedDescription changefeedDesc){
        TString protoStr;
        auto proto = TProtoAccessor::GetProto(
            DescribeTopic(topicClient, TStringBuilder() << table << "/" << changefeedDesc.GetName())
        );
        proto.clear_self();
        proto.clear_topic_stats();

        google::protobuf::TextFormat::PrintToString(
            proto, &protoStr
        );
        return protoStr;
    });
    
    return {changefeedsStr, topicsStr};
}

void TestChangefeedAndTopicDescriptionsIsPreserved(
    const char* table, TSession& session, NTopic::TTopicClient& topicClient,
    TBackupFunction&& backup, TRestoreFunction&& restore, const TVector<TString>& changefeeds
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

    for (const auto& changefeed : changefeeds) {
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                ALTER TABLE `%s` ADD CHANGEFEED `%s` WITH (
                    FORMAT = 'JSON',
                    MODE = 'UPDATES'
                );
            )",
            table,
            changefeed.c_str()
        ));
    }

    Cerr << "GetChangefeedAndTopicDescriptions: " << Endl;
    auto changefeedsAndTopicsBefore = GetChangefeedAndTopicDescriptions(table, session, topicClient);
    backup();

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();
    auto changefeedsAndTopicsAfter = GetChangefeedAndTopicDescriptions(table, session, topicClient);

    UNIT_ASSERT_EQUAL(changefeedsAndTopicsBefore, changefeedsAndTopicsAfter);
}

void TestTopicSettingsArePreserved(
    const char* topic, NQuery::TSession& session, NTopic::TTopicClient& topicClient,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    constexpr int minPartitions = 2;
    constexpr int maxPartitions = 5;
    constexpr const char* autoPartitioningStrategy = "scale_up";
    constexpr int retentionPeriodDays = 7;

    ExecuteQuery(session, Sprintf(R"(
            CREATE TOPIC `%s` (
                CONSUMER basic_consumer,
                CONSUMER important_consumer WITH (important = TRUE)
            ) WITH (
                min_active_partitions = %d,
                max_active_partitions = %d,
                auto_partitioning_strategy = '%s',
                retention_period = Interval('%s')
            );
        )",
        topic, minPartitions, maxPartitions, autoPartitioningStrategy, Sprintf("P%dD", retentionPeriodDays).c_str()
    ), true);

    const auto checkDescription = [&](const NTopic::TTopicDescription& description, const TString& debugHint) {
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetConsumers().at(0).GetConsumerName(), "basic_consumer", debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetConsumers().at(0).GetImportant(), false, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetConsumers().at(1).GetConsumerName(), "important_consumer", debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetConsumers().at(1).GetImportant(), true, debugHint);

        UNIT_ASSERT_VALUES_EQUAL_C(description.GetPartitioningSettings().GetMinActivePartitions(), minPartitions, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetPartitioningSettings().GetMaxActivePartitions(), maxPartitions, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy(), NTopic::EAutoPartitioningStrategy::ScaleUp, debugHint);

        UNIT_ASSERT_VALUES_EQUAL_C(description.GetPartitions().size(), 2, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetPartitions().at(0).GetActive(), true, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetPartitions().at(1).GetActive(), true, debugHint);

        UNIT_ASSERT_VALUES_EQUAL_C(description.GetRetentionPeriod(), TDuration::Days(retentionPeriodDays), debugHint);
    };
    checkDescription(DescribeTopic(topicClient, topic), DEBUG_HINT);

    backup();

    ExecuteQuery(session, Sprintf(R"(
            DROP TOPIC `%s`;
        )", topic
    ), true);

    restore();
    checkDescription(DescribeTopic(topicClient, topic), DEBUG_HINT);
}

void CreateCoordinationNode(
    NCoordination::TClient& client, const std::string& path, const NCoordination::TCreateNodeSettings& settings
) {
    const auto result = client.CreateNode(path, settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

NCoordination::TNodeDescription DescribeCoordinationNode(NCoordination::TClient& client, const std::string& path) {
    const auto result = client.DescribeNode(path).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetResult();
}

void DropCoordinationNode(NCoordination::TClient& client, const std::string& path) {
    const auto result = client.DropNode(path).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void TestCoordinationNodeSettingsArePreserved(
    const std::string& path,
    NCoordination::TClient& nodeClient,
    TBackupFunction&& backup,
    TRestoreFunction&& restore
) {
    const auto settings = NCoordination::TCreateNodeSettings()
        .SelfCheckPeriod(TDuration::Seconds(2))
        .SessionGracePeriod(TDuration::Seconds(30))
        .ReadConsistencyMode(NCoordination::EConsistencyMode::STRICT_MODE)
        .AttachConsistencyMode(NCoordination::EConsistencyMode::STRICT_MODE)
        .RateLimiterCountersMode(NCoordination::ERateLimiterCountersMode::DETAILED);

    CreateCoordinationNode(nodeClient, path, settings);

    const auto checkDescription = [&](const NCoordination::TNodeDescription& description, const TString& debugHint) {
        UNIT_ASSERT_VALUES_EQUAL_C(*description.GetSelfCheckPeriod(), *settings.SelfCheckPeriod_, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(*description.GetSessionGracePeriod(), *settings.SessionGracePeriod_, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetReadConsistencyMode(), settings.ReadConsistencyMode_, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetAttachConsistencyMode(), settings.AttachConsistencyMode_, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(description.GetRateLimiterCountersMode(), settings.RateLimiterCountersMode_, debugHint);
    };
    checkDescription(DescribeCoordinationNode(nodeClient, path), DEBUG_HINT);

    backup();

    DropCoordinationNode(nodeClient, path);

    restore();
    checkDescription(DescribeCoordinationNode(nodeClient, path), DEBUG_HINT);
}

void CreateRateLimiter(
    TRateLimiterClient& client,
    const std::string& coordinationNodePath,
    const std::string& rateLimiterPath,
    const TCreateResourceSettings& settings = {}
) {
    const auto result = client.CreateResource(coordinationNodePath, rateLimiterPath, settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TDescribeResourceResult DescribeRateLimiter(
    TRateLimiterClient& client,
    const std::string& coordinationNodePath,
    const std::string& rateLimiterPath
) {
    const auto result = client.DescribeResource(coordinationNodePath, rateLimiterPath).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result;
}

void TestCoordinationNodeResourcesArePreserved(
    const std::string& path,
    NCoordination::TClient& nodeClient,
    TRateLimiterClient& rateLimiterClient,
    TBackupFunction&& backup,
    TRestoreFunction&& restore
) {
    const std::vector<std::pair<std::string, TCreateResourceSettings>> rateLimiters = {
        {
            "root",
            TCreateResourceSettings()
                .MaxUnitsPerSecond(5)
                .MaxBurstSizeCoefficient(2)
                .PrefetchCoefficient(0.5)
                .PrefetchWatermark(0.8)
                .ImmediatelyFillUpTo(-10)
        },
        {
            "root/firstChild",
            TCreateResourceSettings()
                .MaxUnitsPerSecond(10)
                .LeafBehavior(
                    TReplicatedBucketSettings()
                        .ReportInterval(std::chrono::milliseconds(10000))
                )
        },
        {
            "root/secondChild",
            TCreateResourceSettings()
                .MaxUnitsPerSecond(20)
                .MeteringConfig(
                    TMeteringConfig()
                        .Enabled(true)
                        .ReportPeriod(std::chrono::milliseconds(10000))
                        .MeterPeriod(std::chrono::milliseconds(5000))
                        .CollectPeriod(std::chrono::seconds(20))
                        .ProvisionedUnitsPerSecond(100)
                        .ProvisionedCoefficient(50)
                        .OvershootCoefficient(1.2)
                        .Provisioned(
                            TMetric()
                                .Enabled(true)
                                .BillingPeriod(std::chrono::seconds(30))
                                .Labels({{"k", "v"}})
                        )
                )
        },
    };

    CreateCoordinationNode(nodeClient, path, {});
    for (const auto& [resource, settings] : rateLimiters) {
        CreateRateLimiter(rateLimiterClient, path, resource, settings);
    }

    std::vector<TDescribeResourceResult> original;
    for (const auto& [resource, _] : rateLimiters) {
        original.emplace_back(DescribeRateLimiter(rateLimiterClient, path, resource));
    }

    backup();

    DropCoordinationNode(nodeClient, path);

    restore();
    for (size_t i = 0; i < rateLimiters.size(); ++i) {
        UNIT_ASSERT_EQUAL(DescribeRateLimiter(rateLimiterClient, path, rateLimiters[i].first), original[i]);
    }
}

void WaitReplicationInit(NReplication::TReplicationClient& client, const TString& path) {
    int retry = 0;
    do {
        auto result = client.DescribeReplication(path).ExtractValueSync();
        const auto& desc = result.GetReplicationDescription();
        if (desc.GetItems().empty()) {
            Sleep(TDuration::Seconds(1));
        } else {
            break;
        }
    } while (++retry < 10);
    UNIT_ASSERT(retry < 10);
}

void TestReplicationSettingsArePreserved(
        const TString& endpoint,
        NQuery::TSession& session,
        NReplication::TReplicationClient& client,
        TBackupFunction&& backup,
        TRestoreFunction&& restore)
{
    ExecuteQuery(session, "CREATE OBJECT `secret` (TYPE SECRET) WITH (value = 'root@builtin');", true);
    ExecuteQuery(session, "CREATE TABLE `/Root/table` (k Uint32, v Utf8, PRIMARY KEY (k));", true);
    ExecuteQuery(session, Sprintf(R"(
        CREATE ASYNC REPLICATION `/Root/replication` FOR
            `/Root/table` AS `/Root/replica`
        WITH (
            CONNECTION_STRING = 'grpc://%s/?database=/Root',
            TOKEN_SECRET_NAME = 'secret'
        );)", endpoint.c_str()), true
    );

    auto checkDescription = [&client, &endpoint]() {
        auto result = client.DescribeReplication("/Root/replication").ExtractValueSync();
        const auto& desc = result.GetReplicationDescription();

        const auto& params = desc.GetConnectionParams();
        UNIT_ASSERT_VALUES_EQUAL(params.GetDiscoveryEndpoint(), endpoint);
        UNIT_ASSERT_VALUES_EQUAL(params.GetDatabase(), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(params.GetOAuthCredentials().TokenSecretName, "secret");

        const auto& items = desc.GetItems();
        UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(items.at(0).SrcPath, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(items.at(0).DstPath, "/Root/replica");
    };

    WaitReplicationInit(client, "/Root/replication");
    checkDescription();
    backup();
    ExecuteQuery(session, "DROP ASYNC REPLICATION `/Root/replication` CASCADE;", true);
    restore();
    WaitReplicationInit(client, "/Root/replication");
    checkDescription();
}

Ydb::Table::DescribeExternalDataSourceResult DescribeExternalDataSource(TSession& session, const TString& path) {
    auto result = session.DescribeExternalDataSource(path).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return TProtoAccessor::GetProto(result.GetExternalDataSourceDescription());;
}

void TestExternalDataSourceSettingsArePreserved(
    const char* path, TSession& tableSession, NQuery::TSession& querySession, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    ExecuteQuery(querySession, Sprintf(R"(
                CREATE EXTERNAL DATA SOURCE `%s` WITH (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION = "192.168.1.1:8123",
                    AUTH_METHOD = "NONE"
                );
            )", path
        ), true
    );
    const auto originalDescription = DescribeExternalDataSource(tableSession, path);

    backup();

    ExecuteQuery(querySession, Sprintf(R"(
                DROP EXTERNAL DATA SOURCE `%s`;
            )", path
        ), true
    );

    restore();
    UNIT_ASSERT_VALUES_EQUAL(
        DescribeExternalDataSource(tableSession, path),
        originalDescription
    );
}

Ydb::Table::DescribeExternalTableResult DescribeExternalTable(TSession& session, const TString& path) {
    auto result = session.DescribeExternalTable(path).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return TProtoAccessor::GetProto(result.GetExternalTableDescription());;
}

void TestExternalTableSettingsArePreserved(
    const char* path, const char* externalDataSource, TSession& tableSession, NQuery::TSession& querySession, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    ExecuteQuery(querySession, Sprintf(R"(
                CREATE EXTERNAL DATA SOURCE `%s` WITH (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION = "192.168.1.1:8123",
                    AUTH_METHOD = "NONE"
                );

                CREATE EXTERNAL TABLE `%s` (
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                ) WITH (
                    DATA_SOURCE = "%s",
                    LOCATION = "folder",
                    FORMAT = "csv_with_names",
                    COMPRESSION = "gzip"
                );
            )", externalDataSource, path, externalDataSource
        ), true
    );
    const auto originalDescription = DescribeExternalTable(tableSession, path);

    backup();

    ExecuteQuery(querySession, Sprintf(R"(
                DROP EXTERNAL TABLE `%s`;
                DROP EXTERNAL DATA SOURCE `%s`;
            )", path, externalDataSource
        ), true
    );

    restore();
    UNIT_ASSERT_VALUES_EQUAL(
        DescribeExternalTable(tableSession, path),
        originalDescription
    );
}

}

Y_UNIT_TEST_SUITE(BackupRestore) {
    auto CreateBackupLambda(const TDriver& driver, const TFsPath& fsPath, const TString& dbPath = "/Root") {
        return [&]() {
            NDump::TClient backupClient(driver);
            const auto result = backupClient.Dump(dbPath, fsPath, NDump::TDumpSettings().Database(dbPath));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
    }

    auto CreateRestoreLambda(const TDriver& driver, const TFsPath& fsPath, const TString& dbPath = "/Root") {
        return [&]() {
            NDump::TClient backupClient(driver);
            const auto result = backupClient.Restore(fsPath, dbPath);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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
            CreateBackupLambda(driver, pathToBackup),
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

    Y_UNIT_TEST(ImportDataShouldHandleErrors) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* dbPath = "/Root";
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
                UPSERT INTO `%s` (Key, Value)
                VALUES (1, "one");
            )",
            table
        ));

        NDump::TClient backupClient(driver);
        {
            const auto result = backupClient.Dump(dbPath, pathToBackup, NDump::TDumpSettings().Database(dbPath));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto opts = NDump::TRestoreSettings().Mode(NDump::TRestoreSettings::EMode::ImportData);
        using TYdbErrorException = V3::NStatusHelpers::TYdbErrorException;

        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Utf8,
                    Value Uint32,
                    PRIMARY KEY (Key)
                );
            )", table
        ));
        UNIT_ASSERT_EXCEPTION_SATISFIES(backupClient.Restore(pathToBackup, dbPath, opts), TYdbErrorException,
            [](const TYdbErrorException& e) { return e.GetStatus().GetStatus() == EStatus::BAD_REQUEST; });

        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    PRIMARY KEY (Key)
                );
            )", table
        ));
        UNIT_ASSERT_EXCEPTION_SATISFIES(backupClient.Restore(pathToBackup, dbPath, opts), TYdbErrorException,
            [](const TYdbErrorException& e) { return e.GetStatus().GetStatus() == EStatus::BAD_REQUEST; });

        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key),
                    INDEX Idx GLOBAL SYNC ON (Value)
                );
            )", table
        ));
        UNIT_ASSERT_EXCEPTION_SATISFIES(backupClient.Restore(pathToBackup, dbPath, opts), TYdbErrorException,
            [](const TYdbErrorException& e) { return e.GetStatus().GetStatus() == EStatus::SCHEME_ERROR; });
    }

    // TO DO: test index impl table split boundaries restoration from a backup

    Y_UNIT_TEST(RestoreViewQueryText) {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableViews(true);
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TViewClient viewClient(driver);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "/Root/view";

        TestViewQueryTextIsPreserved(
            view,
            viewClient,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreViewReferenceTable) {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableViews(true);
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "/Root/view";
        constexpr const char* table = "/Root/a/b/c/table";

        TestViewReferenceTableIsPreserved(
            view,
            table,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreViewToDifferentDatabase) {
        TBasicKikimrWithGrpcAndRootSchema<TTenantsTestSettings> server;

        constexpr const char* alice = "/Root/tenants/alice";
        constexpr const char* bob = "/Root/tenants/bob";
        CreateDatabase(*server.Tenants_, alice, "ssd");
        CreateDatabase(*server.Tenants_, bob, "hdd");

        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        // query client lives on the node 0, so it is enough to enable the views only on it
        server.GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(true);

        const TString view = JoinFsPaths(alice, "view");
        const TString table = JoinFsPaths(alice, "a", "b", "c", "table");
        const TString restoredView = JoinFsPaths(bob, "view");

        TestViewReferenceTableIsPreserved(
            view.c_str(),
            table.c_str(),
            restoredView.c_str(),
            session,
            CreateBackupLambda(driver, pathToBackup, alice),
            CreateRestoreLambda(driver, pathToBackup, bob)
        );
    }

    Y_UNIT_TEST(RestoreViewDependentOnAnotherView) {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableViews(true);
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* baseView = "/Root/baseView";
        constexpr const char* dependentView = "/Root/dependentView";

        TestViewDependentOnAnotherViewIsRestored(
            baseView,
            dependentView,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreKesusResources) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        NCoordination::TClient nodeClient(driver);
        TRateLimiterClient rateLimiterClient(driver);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        const std::string kesus = "/Root/kesus";

        TestCoordinationNodeResourcesArePreserved(
            kesus,
            nodeClient,
            rateLimiterClient,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

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
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableVectorIndex(true);
        TKikimrWithGrpcAndRootSchema server{std::move(appConfig)};

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

    void TestViewBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableViews(true);
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "/Root/view";

        TestViewOutputIsPreserved(
            view,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    void TestTopicBackupRestoreWithoutData() {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        NTopic::TTopicClient topicClient(driver);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* topic = "/Root/topic";

        TestTopicSettingsArePreserved(
            topic,
            session,
            topicClient,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    void TestKesusBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        NCoordination::TClient nodeClient(driver);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        const std::string kesus = "/Root/kesus";

        TestCoordinationNodeSettingsArePreserved(
            kesus,
            nodeClient,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    void TestChangefeedBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        NTopic::TTopicClient topicClient(driver);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        const auto table = "/Root/table";

        TestChangefeedAndTopicDescriptionsIsPreserved(
            table,
            session,
            topicClient,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup),
            {"a", "b", "c"}
        );
    }

    void TestReplicationBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driver = TDriver(TDriverConfig().SetEndpoint(endpoint).SetAuthToken("root@builtin"));

        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        NReplication::TReplicationClient replicationClient(driver);

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        TestReplicationSettingsArePreserved(
            endpoint, session, replicationClient,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreReplicationWithoutSecret) {
        TKikimrWithGrpcAndRootSchema server;

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driver = TDriver(TDriverConfig().SetEndpoint(endpoint).SetAuthToken("root@builtin"));

        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        NReplication::TReplicationClient replicationClient(driver);

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        ExecuteQuery(session, "CREATE OBJECT `secret` (TYPE SECRET) WITH (value = 'root@builtin');", true);
        ExecuteQuery(session, "CREATE TABLE `/Root/table` (k Uint32, v Utf8, PRIMARY KEY (k));", true);
        ExecuteQuery(session, Sprintf(R"(
            CREATE ASYNC REPLICATION `/Root/replication` FOR
                `/Root/table` AS `/Root/replica`
            WITH (
                CONNECTION_STRING = 'grpc://%s/?database=/Root',
                TOKEN_SECRET_NAME = 'secret'
            );)", endpoint.c_str()), true
        );
        WaitReplicationInit(replicationClient, "/Root/replication");

        NDump::TClient backupClient(driver);
        {
            const auto result = backupClient.Dump("/Root", pathToBackup, NDump::TDumpSettings().Database("/Root"));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        ExecuteQuery(session, "DROP OBJECT `secret` (TYPE SECRET);", true);
        ExecuteQuery(session, "DROP ASYNC REPLICATION `/Root/replication` CASCADE;", true);
        {
            const auto result = backupClient.Restore(pathToBackup, "/Root");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    void TestExternalDataSourceBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* path = "/Root/externalDataSource";

        TestExternalDataSourceSettingsArePreserved(
            path,
            tableSession,
            querySession,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreExternalDataSourceWithoutSecret) {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);

        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();

        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        ExecuteQuery(querySession, "CREATE OBJECT `secret` (TYPE SECRET) WITH (value = 'secret');", true);
        ExecuteQuery(querySession, R"(
            CREATE EXTERNAL DATA SOURCE `/Root/externalDataSource` WITH (
                SOURCE_TYPE = "PostgreSQL",
                DATABASE_NAME = "db",
                LOCATION = "192.168.1.1:8123",
                AUTH_METHOD = "BASIC",
                LOGIN = "user",
                PASSWORD_SECRET_NAME = "secret"
            );
        )", true);

        NDump::TClient backupClient(driver);
        {
            const auto result = backupClient.Dump("/Root", pathToBackup, NDump::TDumpSettings().Database("/Root"));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        ExecuteQuery(querySession, "DROP OBJECT `secret` (TYPE SECRET);", true);
        ExecuteQuery(querySession, "DROP EXTERNAL DATA SOURCE `/Root/externalDataSource`;", true);
        {
            const auto result = backupClient.Restore(pathToBackup, "/Root");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    void TestExternalTableBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())));
        TTableClient tableClient(driver);
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* path = "/Root/externalTable";
        constexpr const char* externalDataSource = "/Root/externalDataSource";

        TestExternalTableSettingsArePreserved(
            path,
            externalDataSource,
            tableSession,
            querySession,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(TestAllSchemeObjectTypes, NKikimrSchemeOp::EPathType) {
        using namespace NKikimrSchemeOp;

        switch (Value) {
            case EPathTypeTable:
                return TestTableBackupRestore();
            case EPathTypeTableIndex:
                return TestTableWithIndexBackupRestore();
            case EPathTypeSequence:
                return TestTableWithSerialBackupRestore();
            case EPathTypeDir:
                return TestDirectoryBackupRestore();
            case EPathTypePersQueueGroup:
                return TestTopicBackupRestoreWithoutData();
            case EPathTypeSubDomain:
            case EPathTypeExtSubDomain:
                break; // https://github.com/ydb-platform/ydb/issues/10432
            case EPathTypeView:
                return TestViewBackupRestore();
            case EPathTypeCdcStream:
                return TestChangefeedBackupRestore();
            case EPathTypeReplication:
                return TestReplicationBackupRestore();
            case EPathTypeTransfer:
                break; // https://github.com/ydb-platform/ydb/issues/10436
            case EPathTypeExternalTable:
                return TestExternalTableBackupRestore();
            case EPathTypeExternalDataSource:
                return TestExternalDataSourceBackupRestore();
            case EPathTypeResourcePool:
                break; // https://github.com/ydb-platform/ydb/issues/10440
            case EPathTypeKesus:
                return TestKesusBackupRestore();
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
            case EIndexTypeGlobalVectorKmeansTree:
                return TestTableWithIndexBackupRestore(Value);
            case EIndexTypeGlobalUnique:
                break; // https://github.com/ydb-platform/ydb/issues/10468
            case EIndexTypeInvalid:
                break; // not applicable
            default:
                UNIT_FAIL("Client backup/restore were not implemented for this index type");
        }
    }
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
            : Server([&] {
                    NKikimrConfig::TAppConfig appConfig;
                    appConfig.MutableFeatureFlags()->SetEnableVectorIndex(true);
                    return appConfig;
                }())
            , Driver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", Server.GetPort())))
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
            runtime.SetLogPriority(NKikimrServices::EXPORT, NLog::EPriority::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::IMPORT, NLog::EPriority::PRI_DEBUG);
            runtime.GetAppData().DataShardExportFactory = &DataShardExportFactory;
            runtime.GetAppData().FeatureFlags.SetEnableViews(true);
            runtime.GetAppData().FeatureFlags.SetEnableViewExport(true);
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

    Y_UNIT_TEST(RestoreViewQueryText) {
        TS3TestEnv testEnv;
        TViewClient viewClient(testEnv.GetDriver());
        constexpr const char* view = "/Root/view";

        TestViewQueryTextIsPreserved(
            view,
            viewClient,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "view" })
        );
    }

    Y_UNIT_TEST(RestoreViewReferenceTable) {
        TS3TestEnv testEnv;
        constexpr const char* view = "/Root/view";
        constexpr const char* table = "/Root/a/b/c/table";

        TestViewReferenceTableIsPreserved(
            view,
            table,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "view", "a/b/c/table" })
        );
    }

    Y_UNIT_TEST(RestoreViewDependentOnAnotherView) {
        TS3TestEnv testEnv;
        constexpr const char* baseView = "/Root/baseView";
        constexpr const char* dependentView = "/Root/dependentView";

        TestViewDependentOnAnotherViewIsRestored(
            baseView,
            dependentView,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "baseView", "dependentView" })
        );
    }


    // TO DO: test view restoration to a different database

    void TestTableBackupRestore() {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";

        TestTableContentIsPreserved(
            table,
            testEnv.GetTableSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
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
            testEnv.GetTableSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
        );
    }

    void TestTableWithSerialBackupRestore() {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";

        TestRestoreTableWithSerial(
            table,
            testEnv.GetTableSession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" })
        );
    }

    void TestViewBackupRestore() {
        TS3TestEnv testEnv;
        constexpr const char* view = "/Root/view";

        TestViewOutputIsPreserved(
            view,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "view" })
        );
    }

    void TestChangefeedBackupRestore() {
        TS3TestEnv testEnv;
        NTopic::TTopicClient topicClient(testEnv.GetDriver());

        constexpr const char* table = "/Root/table";
        testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableChangefeedsExport(true);
        testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableChangefeedsImport(true);

        TestChangefeedAndTopicDescriptionsIsPreserved(
            table,
            testEnv.GetTableSession(),
            topicClient,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" }),
            {"a", "b", "c"}
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
                TestViewBackupRestore();
                break;
            case EPathTypeCdcStream:
                TestChangefeedBackupRestore();
                break;
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
            case EIndexTypeGlobalVectorKmeansTree:
                TestTableWithIndexBackupRestore(Value);
                break;
            case EIndexTypeGlobalUnique:
                break; // https://github.com/ydb-platform/ydb/issues/10468
            case EIndexTypeInvalid:
                break; // not applicable
            default:
                UNIT_FAIL("S3 backup/restore were not implemented for this index type");
        }
    }
}
