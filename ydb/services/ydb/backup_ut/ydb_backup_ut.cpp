#include "ydb_common_ut.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/draft/ydb_view.pb.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_view.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <ydb/library/backup/backup.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/regex/pcre/regexp.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

#include <contrib/libs/fmt/include/fmt/format.h>

using namespace NYdb;
using namespace NYdb::NOperation;
using namespace NYdb::NRateLimiter;
using namespace NYdb::NReplication;
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

enum class ESecretType {
    SecretTypeOld,
    SecretTypeScheme,
};

enum class EAuthType {
    AuthTypeNone,
    AuthTypeToken,
    AuthTypePassword,
    AuthTypeAws,
};

struct TTransferTestConfig {
    TString TablePath = "/Root/test_table";
    TString TopicPath = "/Root/test_topic";
    TString TransferPath = "/Root/test_transfer";
    bool FakeTopicPath = false;
    TMaybe<ESecretType> SecretType = ESecretType::SecretTypeOld;
    EAuthType AuthType = EAuthType::AuthTypeToken;
    ui64 BatchSizeBytes = 1024;
    TDuration FlushInterval = TDuration::Seconds(55);
    bool Replace = false;
    bool ComplexLambda = false;
    bool UseConnectionString = true;
    bool LambdaInsideUsing = false;
    ui32 BackupRestoreAttemptsCount = 1;
};

void AssertAuthAndSecretTypes(const TTransferTestConfig& config) {
    UNIT_ASSERT_C(
        (config.AuthType == EAuthType::AuthTypeNone && !config.SecretType) ||
            (config.AuthType != EAuthType::AuthTypeNone && config.SecretType),
        "AuthType and SecretType should be consistent"
    );
}

}

namespace {

void ArePermissionsEqual(const THashMap<TString, THashSet<TString>>& lhs, const THashMap<TString, THashSet<TString>>& rhs) {
    UNIT_ASSERT_VALUES_EQUAL(lhs.size(), rhs.size());

    for (const auto& [subject, permissions] : lhs) {
        auto it = rhs.find(subject);
        UNIT_ASSERT_C(it != rhs.end(), TStringBuilder() << "Subject '" << subject << "' not found in rhs");
        UNIT_ASSERT_VALUES_EQUAL_C(permissions.size(), it->second.size(),
            TStringBuilder() << "Permission count mismatch for subject '" << subject << "'");

        for (const auto& permission : permissions) {
            UNIT_ASSERT_C(it->second.contains(permission),
                TStringBuilder() << "Permission '" << permission << "' not found in rhs for subject '" << subject << "'");
        }
    }
}

#define Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES_WITH_FLAG(N, ENUM_TYPE, BOOL_VALUE) \
    struct TTestCase##N : public TCurrentTestCase { \
        ENUM_TYPE Value; \
        bool BOOL_VALUE; \
        TString ParametrizedTestName; \
\
        TTestCase##N(ENUM_TYPE value, bool value2) : TCurrentTestCase(), Value(value), BOOL_VALUE(value2), ParametrizedTestName(#N "-" + ENUM_TYPE##_Name(Value) + (BOOL_VALUE ? "+" : "-") + #BOOL_VALUE) { \
            Name_ = ParametrizedTestName.c_str(); \
        } \
\
        static THolder<NUnitTest::TBaseTestCase> Create(ENUM_TYPE value, bool value2) { return ::MakeHolder<TTestCase##N>(value, value2); } \
        void Execute_(NUnitTest::TTestContext&) override; \
    }; \
    struct TTestRegistration##N { \
        TTestRegistration##N() { \
            const auto* enumDescriptor = google::protobuf::GetEnumDescriptor<ENUM_TYPE>(); \
            for (int i = 0; i < enumDescriptor->value_count(); ++i) { \
                const auto* valueDescriptor = enumDescriptor->value(i); \
                const auto value = static_cast<ENUM_TYPE>(valueDescriptor->number()); \
                for (bool flag : {false, true}) { \
                    TCurrentTest::AddTest([value, flag] { return TTestCase##N::Create(value, flag); }); \
                } \
            } \
        } \
    }; \
    static TTestRegistration##N testRegistration##N; \
    void TTestCase##N::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)


#define Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(N, ENUM_TYPE) \
    struct TTestCase##N : public TCurrentTestCase { \
        ENUM_TYPE Value; \
        TString ParametrizedTestName; \
\
        TTestCase##N(ENUM_TYPE value) : TCurrentTestCase(), Value(value), ParametrizedTestName(#N "-" + ENUM_TYPE##_Name(Value)) { \
            Name_ = ParametrizedTestName.c_str(); \
        } \
\
        static THolder<NUnitTest::TBaseTestCase> Create(ENUM_TYPE value) { return ::MakeHolder<TTestCase##N>(value); } \
        void Execute_(NUnitTest::TTestContext&) override; \
    }; \
    struct TTestRegistration##N { \
        TTestRegistration##N() { \
            const auto* enumDescriptor = google::protobuf::GetEnumDescriptor<ENUM_TYPE>(); \
            for (int i = 0; i < enumDescriptor->value_count(); ++i) { \
                const auto* valueDescriptor = enumDescriptor->value(i); \
                const auto value = static_cast<ENUM_TYPE>(valueDescriptor->number()); \
                TCurrentTest::AddTest([value] { return TTestCase##N::Create(value); }); \
            } \
        } \
    }; \
    static TTestRegistration##N testRegistration##N; \
    void TTestCase##N::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

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
    const char* keyColumn = "Key", const TMaybe<TString>& pathPrefix = Nothing()
) {
    return ExecuteQuery(session, Sprintf(R"(
            %s
            SELECT * FROM `%s` ORDER BY %s;
        )", pathPrefix.GetOrElse("").c_str(), table, keyColumn
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

auto CreateHasIndexChecker(const TString& indexName, EIndexType indexType, bool prefix) {
    return [=](const TTableDescription& tableDescription) {
        for (const auto& indexDesc : tableDescription.GetIndexDescriptions()) {
            if (indexDesc.GetIndexName() != indexName) {
                continue;
            }
            if (indexDesc.GetIndexType() != indexType) {
                continue;
            }
            if (indexDesc.GetIndexColumns().size() != (prefix ? 2 : 1)) {
                continue;
            }
            if (indexDesc.GetDataColumns().size() != 0) {
                continue;
            }
            if (prefix && indexDesc.GetIndexColumns().front() != "Group") {
                continue;
            }
            if (indexDesc.GetIndexColumns().back() != "Value") {
                continue;
            }
            switch (indexType) { // check settings
                case EIndexType::GlobalSync:
                case EIndexType::GlobalAsync:
                case EIndexType::GlobalUnique:
                    UNIT_ASSERT(std::holds_alternative<std::monostate>(indexDesc.GetIndexSettings()));
                    break;
                case EIndexType::GlobalVectorKMeansTree: {
                    Ydb::Table::KMeansTreeSettings settings;
                    std::get<TKMeansTreeSettings>(indexDesc.GetIndexSettings()).SerializeTo(settings);
                    Ydb::Table::KMeansTreeSettings expected;
                    expected.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
                    expected.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
                    expected.mutable_settings()->set_vector_dimension(768);
                    expected.set_levels(2);
                    expected.set_clusters(80);
                    expected.set_overlap_clusters(3);
                    expected.set_overlap_ratio(1.2);
                    if (!google::protobuf::util::MessageDifferencer::Equals(settings, expected)) {
                        continue;
                    }
                    break;
                }
                case EIndexType::GlobalFulltextPlain: {
                    Ydb::Table::FulltextIndexSettings settings;
                    std::get<TFulltextIndexSettings>(indexDesc.GetIndexSettings()).SerializeTo(settings);
                    Ydb::Table::FulltextIndexSettings expected;
                    expected.set_layout(Ydb::Table::FulltextIndexSettings::FLAT);
                    auto column = expected.add_columns();
                    column->set_column("Value");
                    column->mutable_analyzers()->set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
                    column->mutable_analyzers()->set_use_filter_lowercase(true);
                    column->mutable_analyzers()->set_use_filter_length(true);
                    column->mutable_analyzers()->set_filter_length_max(42);
                    if (!google::protobuf::util::MessageDifferencer::Equals(settings, expected)) {
                        continue;
                    }
                    break;
                }
                case EIndexType::GlobalFulltextRelevance: {
                    Ydb::Table::FulltextIndexSettings settings;
                    std::get<TFulltextIndexSettings>(indexDesc.GetIndexSettings()).SerializeTo(settings);
                    Ydb::Table::FulltextIndexSettings expected;
                    expected.set_layout(Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE);
                    auto column = expected.add_columns();
                    column->set_column("Value");
                    column->mutable_analyzers()->set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
                    column->mutable_analyzers()->set_use_filter_lowercase(true);
                    column->mutable_analyzers()->set_use_filter_length(true);
                    column->mutable_analyzers()->set_filter_length_max(42);
                    if (!google::protobuf::util::MessageDifferencer::Equals(settings, expected)) {
                        continue;
                    }
                    break;
                }
                case EIndexType::Unknown: {
                    UNIT_ASSERT(false);
                }
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

auto CreateReadReplicasSettingsChecker(const NYdb::NTable::TReadReplicasSettings::EMode expectedMode, const ui64 expectedCount, const TString& debugHint = "") {
    return [=](const TTableDescription& tableDescription) {
        UNIT_ASSERT_C(tableDescription.GetReadReplicasSettings(), debugHint);
        UNIT_ASSERT_C(tableDescription.GetReadReplicasSettings()->GetMode() == expectedMode, debugHint);
        UNIT_ASSERT_VALUES_EQUAL_C(
            tableDescription.GetReadReplicasSettings()->GetReadReplicasCount(),
            expectedCount,
            debugHint
        );
        return true;
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

NQuery::TSession CreateSession(NQuery::TQueryClient& client) {
    auto sessionCreator = client.GetSession().ExtractValueSync();
    UNIT_ASSERT_C(sessionCreator.IsSuccess(), sessionCreator.GetIssues().ToString());
    return sessionCreator.GetSession();
}

// whole database backup
using TBackupFunction = std::function<void(void)>;
// whole database restore
using TRestoreFunction = std::function<void(void)>;

void TestTableContentIsPreserved(
    const char* table, NQuery::TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore, const bool isOlap, const NDump::TRestoreSettings& restorationSettings = {}
) {
    using namespace fmt::literals;
    ExecuteQuery(session, fmt::format(R"(
            CREATE TABLE `{table}` (
                Key Uint32 {not_null},
                Value Utf8,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = {store}
            );
        )",
        "table"_a = table, "store"_a = isOlap ? "COLUMN" : "ROW", "not_null"_a = isOlap ? "NOT NULL" : ""
    ), true);
    ExecuteQuery(session, Sprintf(R"(
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

    if (!restorationSettings.Replace_) {
        ExecuteQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ), true);
    }

    restore();
    CompareResults(GetTableContent(session, table), originalContent);
}

void TestTablePartitioningSettingsArePreserved(
    const char* table, ui32 minPartitions, TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    using namespace fmt::literals;
    ExecuteDataDefinitionQuery(session, fmt::format(R"(
            CREATE TABLE `{table}` (
                Key Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {min_partitions}
            );
        )",
        "table"_a = table, "min_partitions"_a = minPartitions
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
    using namespace fmt::literals;
    const TString indexTablePath = JoinFsPaths(table, index, "indexImplTable");

    ExecuteDataDefinitionQuery(session, fmt::format(R"(
            CREATE TABLE `{table}` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key),
                INDEX {index} GLOBAL ON (Value)
            )
        )",
        "table"_a = table, "index"_a = index
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

void TestIndexTableReadReplicasSettingsArePreserved(
    const char* table, const char* index, NYdb::NTable::TReadReplicasSettings::EMode readReplicasMode, const ui64 readReplicasCount, TSession& session,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    using namespace fmt::literals;
    const TString indexTablePath = JoinFsPaths(table, index, "indexImplTable");
    TString readReplicasModeAsString;
    switch (readReplicasMode) {
        case NYdb::NTable::TReadReplicasSettings::EMode::PerAz:
            readReplicasModeAsString = "PER_AZ";
            break;
        case NYdb::NTable::TReadReplicasSettings::EMode::AnyAz:
            readReplicasModeAsString = "ANY_AZ";
            break;
        default:
            UNIT_FAIL(TString::Join("Unsupported readReplicasMode"));
    }

    ExecuteDataDefinitionQuery(session, fmt::format(R"(
            CREATE TABLE `{table}` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key),
                INDEX {index} GLOBAL ON (Value)
            )
        )",
        "table"_a = table, "index"_a = index
    ));
    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            ALTER TABLE `%s` ALTER INDEX %s SET (
                READ_REPLICAS_SETTINGS = "%s:%)" PRIu64 R"("
            );
        )", table, index, readReplicasModeAsString.c_str(), readReplicasCount
    ));
    CheckTableDescription(session, indexTablePath, CreateReadReplicasSettingsChecker(readReplicasMode, readReplicasCount, DEBUG_HINT));

    backup();

    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();
    CheckTableDescription(session, indexTablePath, CreateReadReplicasSettingsChecker(readReplicasMode, readReplicasCount, DEBUG_HINT));
}

void TestTableSplitBoundariesArePreserved(
    const char* table, ui64 partitions, TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    using namespace fmt::literals;
    ExecuteDataDefinitionQuery(session, fmt::format(R"(
            CREATE TABLE `{table}` (
                Key Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            )
            WITH (
                PARTITION_AT_KEYS = (1, 2, 4, 8, 16, 32, 64, 128, 256)
            );
        )",
        "table"_a = table
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
    using namespace fmt::literals;
    ExecuteDataDefinitionQuery(session, fmt::format(R"(
            CREATE TABLE `{table}` (
                Key Serial,
                Value Uint32,
                PRIMARY KEY (Key)
            )
        )",
        "table"_a = table
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
            return "GLOBAL UNIQUE SYNC";
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
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            return NYdb::NTable::EIndexType::GlobalFulltextPlain;
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            return NYdb::NTable::EIndexType::GlobalFulltextRelevance;
        default:
            UNIT_FAIL("No conversion to API for this index type");
            return NYdb::NTable::EIndexType::Unknown;
    }
}

void TestRestoreTableWithIndex(
    const char* table, const char* index, NKikimrSchemeOp::EIndexType indexType, bool prefix, TSession& session,
    TBackupFunction&& backup, TRestoreFunction&& restore
) {
    using namespace fmt::literals;
    TString query;
    switch (indexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            query = fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key Uint32,
                    Group Uint32,
                    Value Uint32,
                    PRIMARY KEY (Key),
                    INDEX {index} {index_type} ON (Value)
                )
            )", "table"_a = table, "index"_a = index, "index_type"_a = ConvertIndexTypeToSQL(indexType));
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
            if (prefix) {
                query = fmt::format(R"(CREATE TABLE `{table}` (
                    Key Uint32,
                    Group Uint32,
                    Value String,
                    PRIMARY KEY (Key),
                    INDEX {index} GLOBAL USING vector_kmeans_tree
                        ON (Group, Value)
                        WITH (similarity=inner_product, vector_type=float, vector_dimension=768, levels=2, clusters=80, overlap_clusters=3, overlap_ratio="1.2")
                    ))", "table"_a = table, "index"_a = index);
            } else {
                query = fmt::format(R"(CREATE TABLE `{table}` (
                    Key Uint32,
                    Group Uint32,
                    Value String,
                    PRIMARY KEY (Key),
                    INDEX {index} GLOBAL USING vector_kmeans_tree
                        ON (Value)
                        WITH (similarity=inner_product, vector_type=float, vector_dimension=768, levels=2, clusters=80, overlap_clusters=3, overlap_ratio="1.2")
                    ))", "table"_a = table, "index"_a = index);
            }
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            query = fmt::format(R"(CREATE TABLE `{table}` (
                Key Uint32,
                Group Uint32,
                Value String,
                PRIMARY KEY (Key),
                INDEX {index} GLOBAL USING fulltext_plain
                    ON (Value)
                    WITH (tokenizer=standard, use_filter_lowercase=true, use_filter_length=true, filter_length_max=42)
                ))", "table"_a = table, "index"_a = index);
            break;
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            query = fmt::format(R"(CREATE TABLE `{table}` (
                Key Uint32,
                Group Uint32,
                Value String,
                PRIMARY KEY (Key),
                INDEX {index} GLOBAL USING fulltext_relevance
                    ON (Value)
                    WITH (tokenizer=standard, use_filter_lowercase=true, use_filter_length=true, filter_length_max=42)
                ))", "table"_a = table, "index"_a = index);
            break;
        default:
            UNIT_FAIL("No creation this index type");
            break;
    };

    ExecuteDataDefinitionQuery(session, query);

    backup();

    // restore deleted table
    ExecuteDataDefinitionQuery(session, Sprintf(R"(
            DROP TABLE `%s`;
        )", table
    ));

    restore();

    CheckTableDescription(session, table, CreateHasIndexChecker(index, ConvertIndexTypeToAPI(indexType), prefix));
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
    const char* view, NQuery::TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore, const NDump::TRestoreSettings& restorationSettings = {}
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

    if (!restorationSettings.Replace_) {
        ExecuteQuery(session, Sprintf(R"(
                    DROP VIEW `%s`;
                )", view
            ), true
        );
    }

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

void TestViewWithNamedExpressions(
        const char* view, const char* table, NQuery::TSession& session,
        TBackupFunction&& backup, TRestoreFunction&& restore)
{
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
            VALUES (1, "one");
        )", table
    ));

    ExecuteQuery(session, Sprintf(R"(
        $named_exp = (SELECT `Value` FROM `%s` WHERE Key = 1);
        $named_exp_2 = (SELECT * FROM $named_exp);
        CREATE VIEW `%s` WITH security_invoker = TRUE AS
            SELECT * FROM `%s` WHERE `Value` = $named_exp_2 AND `Value` = $named_exp;
    )", table, view, table), true);
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

void TestViewSelectFromIndex(
        const char* view, const char* table, NQuery::TSession& session,
        TBackupFunction&& backup, TRestoreFunction&& restore)
{
    ExecuteQuery(session, Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key),
                    INDEX indexByValue GLOBAL SYNC ON (Value)
                );
            )", table
        ), true
    );
    ExecuteQuery(session, Sprintf(R"(
            UPSERT INTO `%s` (
                Key,
                Value
            )
            VALUES (1, "one");
        )", table
    ));


    ExecuteQuery(session, Sprintf(R"(
        CREATE VIEW `%s` WITH security_invoker = TRUE AS
            SELECT * FROM `%s` VIEW `indexByValue` WHERE `Value` = "one";
    )", view, table), true);
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

// The view might be restored to a different path from the original. The path might be in a different database.
void TestViewReferenceTableIsPreserved(
    const char* view, const char* table, const char* restoredView,
    NQuery::TSession& aliceSession, // first tenant's session
    NQuery::TSession& bobSession, // second tenant's session (might be different from the first one)
    TBackupFunction&& backup, TRestoreFunction&& restore, const bool isOlap
) {
    using namespace fmt::literals;
    ExecuteQuery(aliceSession, fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key Uint32 {not_null},
                    Value Utf8,
                    PRIMARY KEY (Key)
                ) WITH (
                    STORE = {store}
                );
            )", "table"_a = table, "store"_a = isOlap ? "COLUMN" : "ROW", "not_null"_a = isOlap ? "NOT NULL" : ""
        ), true
    );
    ExecuteQuery(aliceSession, Sprintf(R"(
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
    ExecuteQuery(aliceSession, Sprintf(R"(
                CREATE VIEW `%s` WITH security_invoker = TRUE AS %s;
            )", view, viewQuery.c_str()
        ), true
    );
    const auto originalContent = GetTableContent(aliceSession, view);

    backup();

    ExecuteQuery(aliceSession, Sprintf(R"(
                DROP VIEW `%s`;
            )", view
        ), true
    );
    ExecuteQuery(aliceSession, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ), true
    );

    restore();
    CompareResults(GetTableContent(bobSession, restoredView), originalContent);
}

void TestViewReferenceTableIsPreserved(
    const char* view, const char* table, NQuery::TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore, const bool isOlap
) {
    // view is restored to the original path in the original database
    TestViewReferenceTableIsPreserved(view, table, view, session, session, std::move(backup), std::move(restore), isOlap);
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

void TestViewRelativeReferencesArePreserved(
    const char* view, const char* table, const char* restoredView, NQuery::TSession& session,
    TBackupFunction&& backup, TRestoreFunction&& restore, const bool isOlap, const TMaybe<TString>& pathPrefix = Nothing()
) {
    using namespace fmt::literals;
    ExecuteQuery(session, fmt::format(R"(
                {path_prefix}
                CREATE TABLE `{table}` (
                    Key Uint32 {not_null},
                    Value Utf8,
                    PRIMARY KEY (Key)
                ) WITH (
                    STORE = {store}
                );
            )", "path_prefix"_a = pathPrefix.GetOrElse("").c_str(), "table"_a = table, "store"_a = isOlap ? "COLUMN" : "ROW", "not_null"_a = isOlap ? "NOT NULL" : ""
        ), true
    );
    ExecuteQuery(session, Sprintf(R"(
            %s
            UPSERT INTO `%s` (
                Key,
                Value
            )
            VALUES
                (1, "one"),
                (2, "two"),
                (3, "three");
        )", pathPrefix.GetOrElse("").c_str(), table
    ));

    const TString viewQuery = Sprintf(R"(
            SELECT * FROM `%s`
        )", table
    );
    ExecuteQuery(session, Sprintf(R"(
                %s
                CREATE VIEW `%s` WITH security_invoker = TRUE AS %s;
            )", pathPrefix.GetOrElse("").c_str(), view, viewQuery.c_str()
        ), true
    );
    const auto originalContent = GetTableContent(session, view, "Key", pathPrefix);

    backup();

    ExecuteQuery(session, Sprintf(R"(
                %s
                DROP VIEW `%s`;
            )", pathPrefix.GetOrElse("").c_str(), view
        ), true
    );
    ExecuteQuery(session, Sprintf(R"(
                %s
                DROP TABLE `%s`;
            )", pathPrefix.GetOrElse("").c_str(), table
        ), true
    );

    restore();
    CompareResults(GetTableContent(session, restoredView), originalContent);
}

void TestReplaceSystemDirectoryACL(
    const char* systemDirectory, TSchemeClient& client, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    {
        TPermissions permissions("user2@builtin",
            {"ydb.granular.describe_schema", "ydb.granular.select_row"}
        );

        auto result = client.ModifyPermissions(systemDirectory,
            TModifyPermissionsSettings().AddGrantPermissions(permissions).AddChangeOwner("user1@builtin")
        ).ExtractValueSync();
    }

    auto result = client.DescribePath(systemDirectory).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetEntry().Type, ESchemeEntryType::Directory);
    const auto dumpedSystemDirOwner = result.GetEntry().Owner;
    THashMap<TString, THashSet<TString>> dumpedSystemDirPermissions;
    for (const auto& permission : result.GetEntry().Permissions) {
        dumpedSystemDirPermissions[permission.Subject].insert(permission.PermissionNames.begin(), permission.PermissionNames.end());
    }

    backup();

    {
        auto result = client.ModifyPermissions(systemDirectory,
            TModifyPermissionsSettings().AddClearAcl().AddChangeOwner("user3@builtin")
        ).ExtractValueSync();
    }

    restore();

    result = client.DescribePath(systemDirectory).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetEntry().Type, ESchemeEntryType::Directory);
    const auto replacedSystemDirOwner = result.GetEntry().Owner;
    THashMap<TString, THashSet<TString>> replacedSystemDirPermissions;
    for (const auto& permission : result.GetEntry().Permissions) {
        replacedSystemDirPermissions[permission.Subject].insert(permission.PermissionNames.begin(), permission.PermissionNames.end());

    }

    ArePermissionsEqual(replacedSystemDirPermissions, dumpedSystemDirPermissions);
    UNIT_ASSERT_VALUES_EQUAL(replacedSystemDirOwner, dumpedSystemDirOwner);
}

void TestReplaceSystemViewACL(
    const char* systemView, TSession& session, TSchemeClient& client, TBackupFunction&& backup, TRestoreFunction&& restore
) {
    {
        TPermissions permissions("user2@builtin",
            {"ydb.granular.describe_schema", "ydb.granular.select_row"}
        );

        auto result = client.ModifyPermissions(systemView,
            TModifyPermissionsSettings().AddGrantPermissions(permissions).AddChangeOwner("user1@builtin")
        ).ExtractValueSync();
    }

    auto result = session.DescribeSystemView(systemView).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetEntry().Type, ESchemeEntryType::SysView);
    const auto dumpedSysViewOwner = result.GetEntry().Owner;
    THashMap<TString, THashSet<TString>> dumpedSysViewPermissions;
    for (const auto& permission : result.GetEntry().Permissions) {
        dumpedSysViewPermissions[permission.Subject].insert(permission.PermissionNames.begin(), permission.PermissionNames.end());
    }

    backup();

    {
        auto result = client.ModifyPermissions(systemView,
            TModifyPermissionsSettings().AddClearAcl().AddChangeOwner("user3@builtin")
        ).ExtractValueSync();
    }

    restore();

    result = session.DescribeSystemView(systemView).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetEntry().Type, ESchemeEntryType::SysView);
    const auto replacedSysViewOwner = result.GetEntry().Owner;
    THashMap<TString, THashSet<TString>> replacedSysViewPermissions;
    for (const auto& permission : result.GetEntry().Permissions) {
        replacedSysViewPermissions[permission.Subject].insert(permission.PermissionNames.begin(), permission.PermissionNames.end());
    }

    ArePermissionsEqual(replacedSysViewPermissions, dumpedSysViewPermissions);
    UNIT_ASSERT_VALUES_EQUAL(replacedSysViewOwner, dumpedSysViewOwner);
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
    using namespace fmt::literals;
    ExecuteDataDefinitionQuery(session, fmt::format(R"(
            CREATE TABLE `{table}` (
                Key Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            )
        )",
        "table"_a = table
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
    TBackupFunction&& backup, TRestoreFunction&& restore, const NDump::TRestoreSettings& restorationSettings = {}
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

    if (!restorationSettings.Replace_) {
        ExecuteQuery(session, Sprintf(R"(
                DROP TOPIC `%s`;
            )", topic
        ), true);
    }

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
    TRestoreFunction&& restore,
    const NDump::TRestoreSettings& restorationSettings = {}
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

    if (!restorationSettings.Replace_) {
        DropCoordinationNode(nodeClient, path);
    }

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

void WaitReplicationInit(TReplicationClient& client, const TString& path) {
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
        TReplicationClient& client,
        TBackupFunction&& backup,
        TRestoreFunction&& restore,
        const TMaybe<ESecretType> tokenSecretType,
        const NDump::TRestoreSettings& restorationSettings = {})
{
    using namespace fmt::literals;
    if (tokenSecretType == ESecretType::SecretTypeScheme) {
        ExecuteQuery(session, "CREATE SECRET `replication_secret` WITH (value = 'root@builtin');", true);
    } else if (tokenSecretType == ESecretType::SecretTypeOld) {
        ExecuteQuery(session, "CREATE OBJECT `replication_secret` (TYPE SECRET) WITH (value = 'root@builtin');", true);
    }
    ExecuteQuery(session, "CREATE TABLE `/Root/table` (k Uint32, v Utf8, PRIMARY KEY (k));", true);
    ExecuteQuery(session, Sprintf(R"(
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = 'grpc://%s/?database=/Root'
                    %s
                    %s
                );
            )",
            endpoint.c_str(),
            (tokenSecretType == ESecretType::SecretTypeOld ? ", TOKEN_SECRET_NAME = 'replication_secret'" : ""),
            (tokenSecretType == ESecretType::SecretTypeScheme ? ", TOKEN_SECRET_PATH = 'replication_secret'" : "")
        ), true
    );

    auto checkDescription = [&]() {
        auto result = client.DescribeReplication("/Root/replication").ExtractValueSync();
        const auto& desc = result.GetReplicationDescription();

        const auto& params = desc.GetConnectionParams();
        UNIT_ASSERT_VALUES_EQUAL(params.GetDiscoveryEndpoint(), endpoint);
        UNIT_ASSERT_VALUES_EQUAL(params.GetDatabase(), "/Root");
        if (tokenSecretType == ESecretType::SecretTypeScheme) {
            UNIT_ASSERT_VALUES_EQUAL(params.GetOAuthCredentials().TokenSecretName, "/Root/replication_secret");
        } else if (tokenSecretType == ESecretType::SecretTypeOld) {
            UNIT_ASSERT_VALUES_EQUAL(params.GetOAuthCredentials().TokenSecretName, "replication_secret");
        }

        const auto& items = desc.GetItems();
        UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(items.at(0).SrcPath, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(items.at(0).DstPath, "/Root/replica");
    };

    WaitReplicationInit(client, "/Root/replication");
    checkDescription();
    backup();
    if (!restorationSettings.Replace_) {
        ExecuteQuery(session, "DROP ASYNC REPLICATION `/Root/replication` CASCADE;", true);
    }
    restore();
    WaitReplicationInit(client, "/Root/replication");
    checkDescription();
}

TString CanonizeString(const TString& sourceString) {
    TString canonizedString;
    canonizedString.reserve(sourceString.size());

    for (size_t i = 0; i < sourceString.size(); ++i) {
        if (const char c = sourceString[i]; !IsSpace(c)) {
            canonizedString += ToUpper(c);
        }
    }

    return canonizedString;
}

void WaitTransferInit(TReplicationClient& client, const TString& path) {
    int retry = 0;
    do {
        auto result = client.DescribeTransfer(path).ExtractValueSync();
        const auto& desc = result.GetTransferDescription();
        if (!result.IsSuccess() || desc.GetConsumerName().empty()) {
            Sleep(TDuration::Seconds(1));
        } else {
            break;
        }
    } while (++retry < 10);
    UNIT_ASSERT(retry < 10);
}

TString GetTransformationLambdaCreateQuery(TReplicationClient& client, const TString& path) {
    auto result = client.DescribeTransfer(path).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    const auto& desc = result.GetTransferDescription();
    return desc.GetTransformationLambda().c_str();
}

TString MakeAbsolutePath(const TString& path, const TString& db = "/Root") {
    if (path.empty()) {
        return db;
    }
    if (path.StartsWith('/')) {
        return path;
    }
    return TStringBuilder() << db << (db.EndsWith('/') ? "" : "/") << path;
}

void TestTransferSettingsArePreserved(
    const TString& endpoint,
    NQuery::TSession& session,
    TReplicationClient& client,
    TBackupFunction&& backup,
    TRestoreFunction&& restore,
    TFsPath pathToBackup,
    const TTransferTestConfig& config = {}
) {
    AssertAuthAndSecretTypes(config);
    UNIT_ASSERT_C(config.BackupRestoreAttemptsCount < 10, "backup-restore attempts number must be less than 10");

    if (config.AuthType != EAuthType::AuthTypeNone) {
        if (config.SecretType == ESecretType::SecretTypeScheme) {
            ExecuteQuery(session, "CREATE SECRET `transfer_secret` WITH (value = 'root@builtin');", true);
        } else {
            ExecuteQuery(session, "CREATE OBJECT `transfer_secret` (TYPE SECRET) WITH (value = 'root@builtin');", true);
        }
        if (config.AuthType == EAuthType::AuthTypePassword) {
            ExecuteQuery(session, "CREATE USER `transferuser` PASSWORD 'root@builtin';", true);
        }
    }

    const TString& lambdaBodyQuery = !config.ComplexLambda ?
            "$test_lambda = ($msg) -> { return [<| k:CAST($msg._offset AS Uint32), v: CAST($msg._data AS Utf8) |>]; };" :
            R"(
                $f = ($data) -> {
                    return CAST($data AS Utf8);
                };

                $test_lambda = ($msg) -> {
                    return [<| k:CAST($msg._offset AS Uint32), v: $f($msg._data) |>];
                };
            )";

    ExecuteQuery(session,
        Sprintf("CREATE TABLE `%s` (k Uint32, v Utf8, PRIMARY KEY (k));", config.TablePath.c_str())
        , true);
    ExecuteQuery(session, Sprintf("CREATE TOPIC `%s`;", config.TopicPath.c_str()), true);
    const auto createTransferResult = session.ExecuteQuery(Sprintf(R"(
                %s
                CREATE TRANSFER `%s` FROM `%s` TO `%s` USING %s
                WITH (
                    %s
                    BATCH_SIZE_BYTES = %lu,
                    FLUSH_INTERVAL = Interval('PT%luS')
                    %s -- TOKEN_SECRET_NAME if needed
                    %s -- TOKEN_SECRET_PATH if needed
                    %s -- USER and PASSWORD_SECRET_NAME if needed
                    %s -- USER and PASSWORD_SECRET_PATH if needed
                );
            )",
            lambdaBodyQuery.c_str(),
            config.TransferPath.c_str(),
            ((config.FakeTopicPath ? "/Fake" : "") + config.TopicPath).c_str(),
            config.TablePath.c_str(),
            (config.LambdaInsideUsing ? "($msg) -> { return $test_lambda($msg); }" : "$test_lambda"),
            (config.UseConnectionString ? Sprintf("CONNECTION_STRING = 'grpc://%s/?database=/Root',", endpoint.c_str()).c_str() : ""),
            config.BatchSizeBytes,
            config.FlushInterval.Seconds(),
            (config.AuthType == EAuthType::AuthTypeToken && config.SecretType == ESecretType::SecretTypeOld ? ", TOKEN_SECRET_NAME = 'transfer_secret'" : ""),
            (config.AuthType == EAuthType::AuthTypeToken && config.SecretType == ESecretType::SecretTypeScheme ? ", TOKEN_SECRET_PATH = 'transfer_secret'" : ""),
            (config.AuthType == EAuthType::AuthTypePassword && config.SecretType == ESecretType::SecretTypeOld ? ", USER = 'transferuser', PASSWORD_SECRET_NAME = 'transfer_secret'" : ""),
            (config.AuthType == EAuthType::AuthTypePassword && config.SecretType == ESecretType::SecretTypeScheme ? ", USER = 'transferuser', PASSWORD_SECRET_PATH = 'transfer_secret'" : "")
        ), NQuery::TTxControl::NoTx()
    ).ExtractValueSync();

    if (config.FakeTopicPath) {
        UNIT_ASSERT_C(!createTransferResult.IsSuccess(), createTransferResult.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS_C(createTransferResult.GetIssues().ToOneLineString().c_str(), "not in database", "Fake topic path");
        return;
    }

    const TString& absoluteTopicPath = MakeAbsolutePath(config.TopicPath);
    const TString& absoluteTransferPath = MakeAbsolutePath(config.TransferPath);
    const TString& absoluteTablePath = MakeAbsolutePath(config.TablePath);

    WaitTransferInit(client, absoluteTransferPath);
    const TString& originalLambdaCanonized = CanonizeString(GetTransformationLambdaCreateQuery(client, absoluteTransferPath));

    auto checkDescription = [&]() {
        auto result = client.DescribeTransfer(absoluteTransferPath).ExtractValueSync();
        const auto& desc = result.GetTransferDescription();
        const auto& params = desc.GetConnectionParams();
        if (config.UseConnectionString) {
            UNIT_ASSERT_VALUES_EQUAL(params.GetDatabase(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(params.GetDiscoveryEndpoint(), endpoint);
        }

        if (config.AuthType == EAuthType::AuthTypeToken) {
            if (config.SecretType == ESecretType::SecretTypeOld) {
                UNIT_ASSERT_VALUES_EQUAL(params.GetOAuthCredentials().TokenSecretName, "transfer_secret");
            } else if (config.SecretType == ESecretType::SecretTypeScheme) {
                UNIT_ASSERT_VALUES_EQUAL(params.GetOAuthCredentials().TokenSecretName, "/Root/transfer_secret");
            }
        } else if (config.AuthType == EAuthType::AuthTypePassword) {
            UNIT_ASSERT_VALUES_EQUAL(params.GetStaticCredentials().User, "transferuser");
            if (config.SecretType == ESecretType::SecretTypeOld) {
                UNIT_ASSERT_VALUES_EQUAL(params.GetStaticCredentials().PasswordSecretName, "transfer_secret");
            } else if (config.SecretType == ESecretType::SecretTypeScheme) {
                UNIT_ASSERT_VALUES_EQUAL(params.GetStaticCredentials().PasswordSecretName, "/Root/transfer_secret");
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(desc.GetSrcPath(), config.TopicPath);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetDstPath(), absoluteTablePath);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetBatchingSettings().SizeBytes, config.BatchSizeBytes);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetBatchingSettings().FlushInterval, config.FlushInterval);
        UNIT_ASSERT_VALUES_EQUAL(CanonizeString(desc.GetTransformationLambda().c_str()), originalLambdaCanonized.c_str());
    };

    auto cleanBackupFolder = [&pathToBackup] {
        TVector<TFsPath> children;
        pathToBackup.List(children);
        for (auto& i : children) {
            i.ForceDelete();
        }
    };

    checkDescription();

    size_t attempt = 0;
    do {
        backup();

        if (!config.Replace) {
            ExecuteQuery(session, Sprintf("DROP TRANSFER `%s`;", absoluteTransferPath.c_str()), true);
            // we MUST drop topic, because consumer will be dropped during droping transfer
            // to do: create transfer's option to enable dropping transfer witout dropping consumer
            ExecuteQuery(session, Sprintf("DROP TOPIC `%s`;", absoluteTopicPath.c_str()), true);
        }

        restore();

        WaitTransferInit(client, absoluteTransferPath.c_str());
        checkDescription();

        cleanBackupFolder();
    } while (++attempt < config.BackupRestoreAttemptsCount);
}

Ydb::Table::DescribeExternalDataSourceResult DescribeExternalDataSource(TSession& session, const TString& path) {
    auto result = session.DescribeExternalDataSource(path).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return TProtoAccessor::GetProto(result.GetExternalDataSourceDescription());;
}

void TestExternalDataSourceSettingsArePreserved(
    const char* path, TSession& tableSession, NQuery::TSession& querySession, TBackupFunction&& backup,
    TRestoreFunction&& restore, const NDump::TRestoreSettings& restorationSettings, const TMaybe<ESecretType>& secretType,
    EAuthType authType
) {
    if (secretType == ESecretType::SecretTypeScheme) {
        ExecuteQuery(querySession, "CREATE SECRET `eds_secret` WITH (value = 'secret');", true);
    } else if (secretType == ESecretType::SecretTypeOld) {
        ExecuteQuery(querySession, "CREATE OBJECT `eds_secret` (TYPE SECRET) WITH (value = 'secret');", true);
    }

    TString createEdsQuery;
    switch (authType) {
        case EAuthType::AuthTypeToken: /* fallthrough */
        case EAuthType::AuthTypeNone: {
            createEdsQuery = Sprintf(
                R"(
                    CREATE EXTERNAL DATA SOURCE `%s` WITH (
                        SOURCE_TYPE = "Ydb",
                        LOCATION = "192.168.1.1:8123",
                        DATABASE_NAME = "/Root/test/",
                        AUTH_METHOD = "%s"
                        %s -- TOKEN_SECRET_NAME if needed
                        %s -- TOKEN_SECRET_PATH if needed
                    );
                )",
                path,
                (!secretType ? "NONE" : "TOKEN"),
                (secretType == ESecretType::SecretTypeScheme ? ", TOKEN_SECRET_PATH = 'eds_secret'" : ""),
                (secretType == ESecretType::SecretTypeOld ? ", TOKEN_SECRET_NAME = 'eds_secret'" : "")
            );
            break;
        }
        case EAuthType::AuthTypeAws: { // checks more than one secret
            createEdsQuery = Sprintf(
                R"(
                    CREATE EXTERNAL DATA SOURCE `%s` WITH (
                        SOURCE_TYPE="ObjectStorage",
                        LOCATION = "192.168.1.1:8123",
                        AUTH_METHOD="AWS",
                        AWS_REGION="ru-central-1",
                        %s="eds_secret",
                        %s="eds_secret"
                    );
                )",
                path,
                (secretType == ESecretType::SecretTypeScheme ? "AWS_ACCESS_KEY_ID_SECRET_PATH" : "AWS_ACCESS_KEY_ID_SECRET_NAME"),
                (secretType == ESecretType::SecretTypeScheme ? "AWS_SECRET_ACCESS_KEY_SECRET_PATH" : "AWS_SECRET_ACCESS_KEY_SECRET_NAME")
            );
            break;
        } default: {
            UNIT_ASSERT_C(false, "Unsupported test setting");
        }
    }

    ExecuteQuery(querySession, createEdsQuery, true);
    const auto originalDescription = DescribeExternalDataSource(tableSession, path);

    backup();

    if (!restorationSettings.Replace_) {
        ExecuteQuery(querySession, Sprintf(R"(
                    DROP EXTERNAL DATA SOURCE `%s`;
                )", path
            ), true
        );
    }

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
    const char* path, const char* externalDataSource, TSession& tableSession, NQuery::TSession& querySession, TBackupFunction&& backup, TRestoreFunction&& restore, const NDump::TRestoreSettings& restorationSettings = {}
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

    if (!restorationSettings.Replace_) {
        ExecuteQuery(querySession, Sprintf(R"(
                    DROP EXTERNAL TABLE `%s`;
                    DROP EXTERNAL DATA SOURCE `%s`;
                )", path, externalDataSource
            ), true
        );
    }

    restore();
    UNIT_ASSERT_VALUES_EQUAL(
        DescribeExternalTable(tableSession, path),
        originalDescription
    );
}

// transform the type to the string usable in CREATE TABLE YQL statement
std::string_view GetYqlType(Ydb::Type::PrimitiveTypeId type) {
    switch (type) {
        case Ydb::Type_PrimitiveTypeId_BOOL: return "Bool";
        case Ydb::Type_PrimitiveTypeId_INT8: return "Int8";
        case Ydb::Type_PrimitiveTypeId_UINT8: return "Uint8";
        case Ydb::Type_PrimitiveTypeId_INT16: return "Int16";
        case Ydb::Type_PrimitiveTypeId_UINT16: return "Uint16";
        case Ydb::Type_PrimitiveTypeId_INT32: return "Int32";
        case Ydb::Type_PrimitiveTypeId_UINT32: return "Uint32";
        case Ydb::Type_PrimitiveTypeId_INT64: return "Int64";
        case Ydb::Type_PrimitiveTypeId_UINT64: return "Uint64";
        case Ydb::Type_PrimitiveTypeId_FLOAT: return "Float";
        case Ydb::Type_PrimitiveTypeId_DOUBLE: return "Double";
        case Ydb::Type_PrimitiveTypeId_DATE: return "Date";
        case Ydb::Type_PrimitiveTypeId_DATETIME: return "Datetime";
        case Ydb::Type_PrimitiveTypeId_TIMESTAMP: return "Timestamp";
        case Ydb::Type_PrimitiveTypeId_INTERVAL: return "Interval";
        case Ydb::Type_PrimitiveTypeId_TZ_DATE: return "TzDate";
        case Ydb::Type_PrimitiveTypeId_TZ_DATETIME: return "TzDatetime";
        case Ydb::Type_PrimitiveTypeId_TZ_TIMESTAMP: return "TzTimestamp";
        case Ydb::Type_PrimitiveTypeId_DATE32: return "Date32";
        case Ydb::Type_PrimitiveTypeId_DATETIME64: return "Datetime64";
        case Ydb::Type_PrimitiveTypeId_TIMESTAMP64: return "Timestamp64";
        case Ydb::Type_PrimitiveTypeId_INTERVAL64: return "Interval64";
        case Ydb::Type_PrimitiveTypeId_STRING: return "String";
        case Ydb::Type_PrimitiveTypeId_UTF8: return "Utf8";
        case Ydb::Type_PrimitiveTypeId_YSON: return "Yson";
        case Ydb::Type_PrimitiveTypeId_JSON: return "Json";
        case Ydb::Type_PrimitiveTypeId_UUID: return "Uuid";
        case Ydb::Type_PrimitiveTypeId_JSON_DOCUMENT: return "JsonDocument";
        case Ydb::Type_PrimitiveTypeId_DYNUMBER: return "DyNumber";
        case Ydb::Type_PrimitiveTypeId_PRIMITIVE_TYPE_ID_UNSPECIFIED:
        case Ydb::Type_PrimitiveTypeId_Type_PrimitiveTypeId_INT_MIN_SENTINEL_DO_NOT_USE_:
        case Ydb::Type_PrimitiveTypeId_Type_PrimitiveTypeId_INT_MAX_SENTINEL_DO_NOT_USE_:
            UNIT_FAIL("Unimplemented");
            return "";
    }
}

// sample values to insert into a table
std::string_view GetSampleValue(Ydb::Type::PrimitiveTypeId type) {
    // date types need type casts
    #define TYPE_CAST(Type, Initializer) "CAST(" #Initializer " AS " #Type ")"
    #define TYPE_CONSTRUCTOR(Type, Initializer) #Type "(" #Initializer ")"
    switch (type) {
        case Ydb::Type_PrimitiveTypeId_BOOL: return "false";
        case Ydb::Type_PrimitiveTypeId_INT8: return "0";
        case Ydb::Type_PrimitiveTypeId_UINT8: return "0";
        case Ydb::Type_PrimitiveTypeId_INT16: return "0";
        case Ydb::Type_PrimitiveTypeId_UINT16: return "0";
        case Ydb::Type_PrimitiveTypeId_INT32: return "0";
        case Ydb::Type_PrimitiveTypeId_UINT32: return "0";
        case Ydb::Type_PrimitiveTypeId_INT64: return "0";
        case Ydb::Type_PrimitiveTypeId_UINT64: return "0";
        case Ydb::Type_PrimitiveTypeId_FLOAT: return "0.0f";
        case Ydb::Type_PrimitiveTypeId_DOUBLE: return "0.0";
        case Ydb::Type_PrimitiveTypeId_DATE: return TYPE_CAST(Date, "2020-01-01");
        case Ydb::Type_PrimitiveTypeId_DATETIME: return TYPE_CAST(Datetime, "2020-01-01T00:00:00Z");
        case Ydb::Type_PrimitiveTypeId_TIMESTAMP: return TYPE_CAST(Timestamp, "2020-01-01T00:00:00Z");
        case Ydb::Type_PrimitiveTypeId_INTERVAL: return TYPE_CAST(Interval, "PT1H");
        case Ydb::Type_PrimitiveTypeId_TZ_DATE: return TYPE_CAST(TzDate, "2020-01-01");
        case Ydb::Type_PrimitiveTypeId_TZ_DATETIME: return TYPE_CAST(TzDatetime, "2020-01-01T00:00:00Z");
        case Ydb::Type_PrimitiveTypeId_TZ_TIMESTAMP: return TYPE_CAST(TzTimestamp, "2020-01-01T00:00:00Z");
        case Ydb::Type_PrimitiveTypeId_DATE32: return TYPE_CAST(Date32, "2020-01-01");
        case Ydb::Type_PrimitiveTypeId_DATETIME64: return TYPE_CAST(Datetime64, "2020-01-01T00:00:00Z");
        case Ydb::Type_PrimitiveTypeId_TIMESTAMP64: return TYPE_CAST(Timestamp64, "2020-01-01T00:00:00Z");
        case Ydb::Type_PrimitiveTypeId_INTERVAL64: return TYPE_CAST(Interval64, "PT1H");
        case Ydb::Type_PrimitiveTypeId_STRING: return "\"foo\"";
        case Ydb::Type_PrimitiveTypeId_UTF8: return "\"foo\"u";
        case Ydb::Type_PrimitiveTypeId_YSON: return TYPE_CONSTRUCTOR(Yson, "{ foo = bar }");
        case Ydb::Type_PrimitiveTypeId_JSON: return TYPE_CONSTRUCTOR(Json, "{ \"foo\": \"bar\" }");
        case Ydb::Type_PrimitiveTypeId_UUID: return "RandomUuid(1)";
        case Ydb::Type_PrimitiveTypeId_JSON_DOCUMENT: return TYPE_CONSTRUCTOR(JsonDocument, "{ \"foo\": \"bar\" }");
        case Ydb::Type_PrimitiveTypeId_DYNUMBER: return TYPE_CONSTRUCTOR(DyNumber, "1");
        case Ydb::Type_PrimitiveTypeId_PRIMITIVE_TYPE_ID_UNSPECIFIED:
        case Ydb::Type_PrimitiveTypeId_Type_PrimitiveTypeId_INT_MIN_SENTINEL_DO_NOT_USE_:
        case Ydb::Type_PrimitiveTypeId_Type_PrimitiveTypeId_INT_MAX_SENTINEL_DO_NOT_USE_:
            UNIT_FAIL("Unimplemented");
            return "";
    }
    #undef TYPE_CAST
    #undef TYPE_CONSTRUCTOR
}

bool CanBePrimaryKey(Ydb::Type::PrimitiveTypeId type) {
    switch (type) {
        case Ydb::Type_PrimitiveTypeId_BOOL:
        case Ydb::Type_PrimitiveTypeId_INT8:
        case Ydb::Type_PrimitiveTypeId_UINT8:
        case Ydb::Type_PrimitiveTypeId_INT16:
        case Ydb::Type_PrimitiveTypeId_UINT16:
        case Ydb::Type_PrimitiveTypeId_INT32:
        case Ydb::Type_PrimitiveTypeId_UINT32:
        case Ydb::Type_PrimitiveTypeId_INT64:
        case Ydb::Type_PrimitiveTypeId_UINT64:
        case Ydb::Type_PrimitiveTypeId_DATE:
        case Ydb::Type_PrimitiveTypeId_DATETIME:
        case Ydb::Type_PrimitiveTypeId_TIMESTAMP:
        case Ydb::Type_PrimitiveTypeId_INTERVAL:
        case Ydb::Type_PrimitiveTypeId_TZ_DATE:
        case Ydb::Type_PrimitiveTypeId_TZ_DATETIME:
        case Ydb::Type_PrimitiveTypeId_TZ_TIMESTAMP:
        case Ydb::Type_PrimitiveTypeId_DATE32:
        case Ydb::Type_PrimitiveTypeId_DATETIME64:
        case Ydb::Type_PrimitiveTypeId_TIMESTAMP64:
        case Ydb::Type_PrimitiveTypeId_INTERVAL64:
        case Ydb::Type_PrimitiveTypeId_STRING:
        case Ydb::Type_PrimitiveTypeId_UTF8:
        case Ydb::Type_PrimitiveTypeId_UUID:
        case Ydb::Type_PrimitiveTypeId_DYNUMBER:
            return true;
        case Ydb::Type_PrimitiveTypeId_FLOAT:
        case Ydb::Type_PrimitiveTypeId_DOUBLE:
        case Ydb::Type_PrimitiveTypeId_YSON:
        case Ydb::Type_PrimitiveTypeId_JSON:
        case Ydb::Type_PrimitiveTypeId_JSON_DOCUMENT:
            return false;
        case Ydb::Type_PrimitiveTypeId_PRIMITIVE_TYPE_ID_UNSPECIFIED:
        case Ydb::Type_PrimitiveTypeId_Type_PrimitiveTypeId_INT_MIN_SENTINEL_DO_NOT_USE_:
        case Ydb::Type_PrimitiveTypeId_Type_PrimitiveTypeId_INT_MAX_SENTINEL_DO_NOT_USE_:
            UNIT_FAIL("Unimplemented");
            return false;
    }
}

bool DontTestThisType(Ydb::Type::PrimitiveTypeId type, const bool isOlap = false) {
    switch (type) {
        case Ydb::Type_PrimitiveTypeId_TZ_DATE:
        case Ydb::Type_PrimitiveTypeId_TZ_DATETIME:
        case Ydb::Type_PrimitiveTypeId_TZ_TIMESTAMP:
            // CREATE TABLE with a column of this type is not supported by storage
            return true;
        case Ydb::Type_PrimitiveTypeId_UUID:
        case Ydb::Type_PrimitiveTypeId_INTERVAL:
        case Ydb::Type_PrimitiveTypeId_DYNUMBER:
        case Ydb::Type_PrimitiveTypeId_BOOL:
            return isOlap; // these types aren't supported for column tables
        case Ydb::Type_PrimitiveTypeId_PRIMITIVE_TYPE_ID_UNSPECIFIED:
        case Ydb::Type_PrimitiveTypeId_Type_PrimitiveTypeId_INT_MIN_SENTINEL_DO_NOT_USE_:
        case Ydb::Type_PrimitiveTypeId_Type_PrimitiveTypeId_INT_MAX_SENTINEL_DO_NOT_USE_:
            // helper types
            return true;
        default:
            return false;
    }
}

auto GetTableName(std::string_view yqlType, std::string_view database = "/Root/") {
    return std::format("{}{}Table", database, yqlType);
}

void TestPrimitiveType(
    Ydb::Type::PrimitiveTypeId type, NQuery::TSession& session, TBackupFunction&& backup, TRestoreFunction&& restore, const bool isOlap
) {
    using namespace fmt::literals;
    const auto yqlType = GetYqlType(type);
    const auto tableName = GetTableName(yqlType);
    const TString sampleValue = isOlap ? TStringBuilder{} << "Unwrap(" << GetSampleValue(type) << ")": TString{GetSampleValue(type)};

    std::string_view key = sampleValue;
    std::string_view value = "1";
    if (CanBePrimaryKey(type)) {
        ExecuteQuery(session, std::format(R"(
                CREATE TABLE `{}` (Key {} {}, Value Int32, PRIMARY KEY (Key)) WITH (STORE={});
            )", tableName, yqlType, isOlap ? "NOT NULL" : "", isOlap ? "COLUMN" : "ROW"
        ), true);
    } else {
        {
            // test if the type cannot in fact be a primary key to future-proof the test suite
            const auto result = session.ExecuteQuery(std::format(R"(
                    CREATE TABLE `{}` (Key {} {}, Value Int32, PRIMARY KEY (Key)) WITH (STORE={});
                )", tableName, yqlType, isOlap ? "NOT NULL" : "", isOlap ? "COLUMN" : "ROW"
            ), NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }
        ExecuteQuery(session, std::format(R"(
                CREATE TABLE `{}` (Key Int32 {}, Value {}, PRIMARY KEY (Key)) WITH (STORE={});
            )", tableName, isOlap ? "NOT NULL" : "", yqlType, isOlap ? "COLUMN" : "ROW"
        ), true);
        std::swap(key, value);
    }
    ExecuteQuery(session, std::format(R"(
            UPSERT INTO `{}` (Key, Value) VALUES ({}, {});
        )", tableName, key, value
    ));
    const auto originalTableContent = GetTableContent(session, tableName.c_str());

    backup();

    ExecuteQuery(session, std::format(R"(
            DROP TABLE `{}`;
        )", tableName
    ), true);

    restore();

    CompareResults(GetTableContent(session, tableName.c_str()), originalTableContent);
}

}

Y_UNIT_TEST_SUITE(BackupRestore) {
    auto CreateBackupLambda(const TDriver& driver, const TFsPath& fsPath, const TString& dbPath = "/Root", const TString& db = "/Root") {
        return [=, &driver]() {
            NDump::TClient backupClient(driver);
            const auto result = backupClient.Dump(dbPath, fsPath, NDump::TDumpSettings().Database(db));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
    }

    auto CreateRestoreLambda(const TDriver& driver, const TFsPath& fsPath, const TString& dbPath = "/Root", const NDump::TRestoreSettings& settings = NDump::TRestoreSettings()) {
        return [=, &driver]() {
            NDump::TClient backupClient(driver);
            const auto result = backupClient.Restore(fsPath, dbPath, settings);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
    }

    Y_UNIT_TEST(RestoreTablePartitioningSettings) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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

    Y_UNIT_TEST(RestoreIndexTableReadReplicasSettings) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        constexpr auto readReplicasMode = NYdb::NTable::TReadReplicasSettings::EMode::PerAz;
        constexpr ui64 readReplicasCount = 1;

        TestIndexTableReadReplicasSettingsArePreserved(
            table,
            index,
            readReplicasMode,
            readReplicasCount,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreTableSplitBoundaries) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
        using namespace fmt::literals;
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* dbPath = "/Root";
        constexpr const char* table = "/Root/table";

        ExecuteDataDefinitionQuery(session, fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key)
                )
            )",
            "table"_a = table
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
        using TYdbErrorException = ::NYdb::Dev::NStatusHelpers::TYdbErrorException;

        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        ExecuteDataDefinitionQuery(session, fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key Utf8,
                    Value Uint32,
                    PRIMARY KEY (Key)
                );
            )", "table"_a = table
        ));
        UNIT_ASSERT_EXCEPTION_SATISFIES(backupClient.Restore(pathToBackup, dbPath, opts), TYdbErrorException,
            [](const TYdbErrorException& e) { return e.GetStatus().GetStatus() == EStatus::BAD_REQUEST; });

        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        ExecuteDataDefinitionQuery(session, fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key Uint32,
                    PRIMARY KEY (Key)
                )
            )", "table"_a = table
        ));
        UNIT_ASSERT_EXCEPTION_SATISFIES(backupClient.Restore(pathToBackup, dbPath, opts), TYdbErrorException,
            [](const TYdbErrorException& e) { return e.GetStatus().GetStatus() == EStatus::BAD_REQUEST; });

        ExecuteDataDefinitionQuery(session, Sprintf(R"(
                DROP TABLE `%s`;
            )", table
        ));
        ExecuteDataDefinitionQuery(session, fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key),
                    INDEX Idx GLOBAL SYNC ON (Value)
                )
            )", "table"_a = table
        ));
        UNIT_ASSERT_EXCEPTION_SATISFIES(backupClient.Restore(pathToBackup, dbPath, opts), TYdbErrorException,
            [](const TYdbErrorException& e) { return e.GetStatus().GetStatus() == EStatus::SCHEME_ERROR; });
    }

    Y_UNIT_TEST(BackupUuid) {
        using namespace fmt::literals;
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* dbPath = "/Root";
        constexpr const char* table = "/Root/table";

        ExecuteDataDefinitionQuery(session, fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key Uuid,
                    Value Utf8,
                    PRIMARY KEY (Key)
                )
            )",
            "table"_a = table
        ));

        std::vector<std::string> uuids = {
            "5b99a330-04ef-4f1a-9b64-ba6d5f44eafe",
            "706cca52-b00a-4cbd-a21e-6538de188271",
            "81b1e345-f2ae-4c9e-8d1a-75447be314f2",
            "be2765f2-9f4c-4a22-8d2c-a1b77d84f4fb",
            "d3f9e0a2-5871-4afe-a23a-8db160b449cd",
            "d3f9e0a2-0000-0000-0000-8db160b449cd"
        };

        ExecuteDataModificationQuery(session, Sprintf(R"(
                UPSERT INTO `%s` (Key, Value)
                VALUES
                    (Uuid("%s"), "one"),
                    (Uuid("%s"), "two"),
                    (Uuid("%s"), "three"),
                    (Uuid("%s"), "four"),
                    (Uuid("%s"), "five"),
                    (Uuid("%s"), "six");
            )", table, uuids[0].c_str(), uuids[1].c_str(), uuids[2].c_str(), uuids[3].c_str(), uuids[4].c_str(), uuids[5].c_str()
        ));

        const auto originalContent = GetTableContent(session, table);

        NDump::TClient backupClient(driver);
        {
            const auto result = backupClient.Dump(dbPath, pathToBackup, NDump::TDumpSettings().Database(dbPath));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Check that backup file contains all uuids as strings, making sure we stringify UUIDs correctly in backups
        TString backupFileContent = TFileInput(pathToBackup.GetPath() + "/table/data_00.csv").ReadAll();
        for (const auto& uuid : uuids) {
            UNIT_ASSERT_C(backupFileContent.find(uuid) != TString::npos, "UUID not found in backup file");
        }

        for (auto backupMode : {NDump::TRestoreSettings::EMode::BulkUpsert, NDump::TRestoreSettings::EMode::ImportData, NDump::TRestoreSettings::EMode::Yql}) {
            auto opts = NDump::TRestoreSettings().Mode(backupMode);

            ExecuteDataDefinitionQuery(session, Sprintf(R"(
                    DROP TABLE `%s`;
                )", table
            ));

            auto result = backupClient.Restore(pathToBackup, dbPath, opts);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const auto newContent = GetTableContent(session, table);

            CompareResults(newContent, originalContent);
        }
    }

    // TO DO: test index impl table split boundaries restoration from a backup

    Y_UNIT_TEST(RestoreViewQueryText) {
        TBasicKikimrWithGrpcAndRootSchema<TTenantsTestSettings> server;
        // note: tenant is needed to work around the issue of "/Root" having a dir scheme entry type when described on restore
        CreateDatabase(*server.Tenants_, "/Root/tenant", "ssd");
        auto driver = TDriver(TDriverConfig()
            .SetEndpoint(Sprintf("localhost:%u", server.GetPort()))
            .SetDatabase("/Root/tenant")
            .SetDiscoveryMode(EDiscoveryMode::Off) // workaround to enable tenant's sessions
        );
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TViewClient viewClient(driver);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "/Root/tenant/view";

        TestViewQueryTextIsPreserved(
            view,
            viewClient,
            session,
            CreateBackupLambda(driver, pathToBackup, "/Root/tenant", "/Root/tenant"),
            CreateRestoreLambda(driver, pathToBackup, "/Root/tenant")
        );
    }

    Y_UNIT_TEST(RestoreViewWithNamedExpressions) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "/Root/view";
        constexpr const char* table = "/Root/a/b/c/table";

        TestViewWithNamedExpressions(
            view,
            table,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreViewSelectFromIndex) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "/Root/view";
        constexpr const char* table = "/Root/a/b/c/table";

        TestViewSelectFromIndex(
            view,
            table,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
    }

    Y_UNIT_TEST(RestoreViewReferenceTable) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
            CreateRestoreLambda(driver, pathToBackup),
            false
        );
    }

    Y_UNIT_TEST(RestoreViewToDifferentDatabase) {
        TBasicKikimrWithGrpcAndRootSchema<TTenantsTestSettings> server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableShowCreate(true);

        constexpr const char* alice = "/Root/tenants/alice";
        constexpr const char* bob = "/Root/tenants/bob";
        CreateDatabase(*server.Tenants_, alice, "ssd");
        CreateDatabase(*server.Tenants_, bob, "hdd");

        auto aliceDriver = TDriver(TDriverConfig()
            .SetEndpoint(Sprintf("localhost:%u", server.GetPort()))
            .SetDatabase(alice)
            .SetDiscoveryMode(EDiscoveryMode::Off) // workaround to enable tenant's sessions
        );
        NQuery::TQueryClient aliceQueryClient(aliceDriver);
        auto aliceSessionCreator = aliceQueryClient.GetSession().ExtractValueSync();
        UNIT_ASSERT_C(aliceSessionCreator.IsSuccess(), aliceSessionCreator.GetIssues().ToString());
        auto aliceSession = aliceSessionCreator.GetSession();
        auto bobDriver = TDriver(TDriverConfig()
            .SetEndpoint(Sprintf("localhost:%u", server.GetPort()))
            .SetDatabase(bob)
            .SetDiscoveryMode(EDiscoveryMode::Off) // workaround to enable tenant's sessions
        );
        NQuery::TQueryClient bobQueryClient(bobDriver);
        auto bobSessionCreator = bobQueryClient.GetSession().ExtractValueSync();
        UNIT_ASSERT_C(bobSessionCreator.IsSuccess(), bobSessionCreator.GetIssues().ToString());
        auto bobSession = bobSessionCreator.GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        const TString view = JoinFsPaths(alice, "view");
        const TString table = JoinFsPaths(alice, "a", "b", "c", "table");
        const TString restoredView = JoinFsPaths(bob, "view");

        TestViewReferenceTableIsPreserved(
            view.c_str(),
            table.c_str(),
            restoredView.c_str(),
            aliceSession,
            bobSession,
            CreateBackupLambda(aliceDriver, pathToBackup, alice, alice),
            CreateRestoreLambda(bobDriver, pathToBackup, bob),
            false
        );
    }

    Y_UNIT_TEST(RestoreViewDependentOnAnotherView) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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

    Y_UNIT_TEST(RestoreViewRelativeReferences) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        NQuery::TQueryClient queryClient(driver);
        auto session = CreateSession(queryClient);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "a/b/c/view";
        constexpr const char* table = "a/b/c/table";
        constexpr const char* restoredView = "restore/point/view";

        TestViewRelativeReferencesArePreserved(
            view,
            table,
            restoredView,
            session,
            CreateBackupLambda(driver, pathToBackup, "/Root/a/b/c"),
            CreateRestoreLambda(driver, pathToBackup, "/Root/restore/point"),
            false
        );
    }

    Y_UNIT_TEST(RestoreViewTablePathPrefix) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        NQuery::TQueryClient queryClient(driver);
        auto session = CreateSession(queryClient);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "view";
        constexpr const char* table = "table";
        constexpr const char* restoredView = "restore/point/a/b/c/view";

        TestViewRelativeReferencesArePreserved(
            view,
            table,
            restoredView,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup, "/Root/restore/point"),
            false, "PRAGMA TablePathPrefix = '/Root/a/b/c';\n"
        );
    }

    Y_UNIT_TEST(RestoreViewTablePathPrefixLowercase) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        NQuery::TQueryClient queryClient(driver);
        auto session = CreateSession(queryClient);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* view = "view";
        constexpr const char* table = "table";
        constexpr const char* restoredView = "restore/point/a/b/c/view";

        TestViewRelativeReferencesArePreserved(
            view,
            table,
            restoredView,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup, "/Root/restore/point"),
            false, "pragma tablepathprefix = '/Root/a/b/c';\n" // note the lowercase
        );
    }

    Y_UNIT_TEST(RestoreKesusResources) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        NQuery::TQueryClient queryClient(driver);
        auto session = CreateSession(queryClient);
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* table = "/Root/table";

        TestTableContentIsPreserved(
            table,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup),
            false
        );
    }

    void TestTableWithIndexBackupRestore(NKikimrSchemeOp::EIndexType indexType = NKikimrSchemeOp::EIndexTypeGlobal, bool prefix = false) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableVectorIndex(true);
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);
        appConfig.MutableFeatureFlags()->SetEnableFulltextIndex(true);
        TKikimrWithGrpcAndRootSchema server{std::move(appConfig)};

        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
            prefix,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup)
        );
        CheckBuildIndexOperationsCleared(driver);
    }

    void TestTableWithSerialBackupRestore() {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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

    void TestReplicationBackupRestore(const TMaybe<ESecretType>& tokenSecretType) {
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableSchemaSecrets(tokenSecretType == ESecretType::SecretTypeScheme);

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driverConfig = TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root");
        if (tokenSecretType) {
            driverConfig.SetAuthToken("root@builtin");
        }
        auto driver = TDriver(driverConfig);
        TSchemeClient schemeClient(driver);
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TReplicationClient replicationClient(driver);

        if (tokenSecretType) {
            TPermissions permissions("root@builtin", {"ydb.generic.full"});
            const auto result = schemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        TestReplicationSettingsArePreserved(
            endpoint, session, replicationClient,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup),
            tokenSecretType
        );
    }

    void TestTransferBackupRestore(const TTransferTestConfig& config = {}) {
        AssertAuthAndSecretTypes(config);
        TKikimrWithGrpcAndRootSchema server;
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicTransfer(true);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableSchemaSecrets(config.SecretType == ESecretType::SecretTypeScheme);

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driverConfig = TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root");
        if (config.AuthType != EAuthType::AuthTypeNone) {
            driverConfig.SetAuthToken("root@builtin");
        }

        auto driver = TDriver(driverConfig);
        TSchemeClient schemeClient(driver);
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TReplicationClient replicationClient(driver);

        if (config.AuthType != EAuthType::AuthTypeNone) {
            TPermissions permissionsToken("root@builtin", {"ydb.generic.full"});
            TPermissions permissionsUser("transferuser", {"ydb.generic.full"});
            const auto result = schemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings()
                    .AddGrantPermissions(permissionsToken)
                    .AddGrantPermissions(permissionsUser)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();
        const auto restorationSettings = NDump::TRestoreSettings().Replace(config.Replace);

        TestTransferSettingsArePreserved(
            endpoint, session, replicationClient,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings),
            pathToBackup, config
        );
    }


    Y_UNIT_TEST_TWIN(RestoreReplicationWithoutSecret, UseSchemeSecret) {
        using namespace fmt::literals;
        TKikimrWithGrpcAndRootSchema server;

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driver = TDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root").SetAuthToken("root@builtin"));

        TSchemeClient schemeClient(driver);
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TReplicationClient replicationClient(driver);

        TPermissions permissions("root@builtin", {"ydb.generic.full"});
        const auto result = schemeClient.ModifyPermissions("/Root",
            TModifyPermissionsSettings().AddGrantPermissions(permissions)
        ).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        if (UseSchemeSecret) {
            ExecuteQuery(session, "CREATE SECRET `secret` WITH (value = 'root@builtin');", true);
        } else {
            ExecuteQuery(session, "CREATE OBJECT `secret` (TYPE SECRET) WITH (value = 'root@builtin');", true);
        }
        ExecuteQuery(session, "CREATE TABLE `/Root/table` (k Uint32, v Utf8, PRIMARY KEY (k));", true);
        ExecuteQuery(session,
            Sprintf(R"(
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = 'grpc://%s/?database=/Root',
                    %s = 'secret'
                );)",
                endpoint.c_str(),
                UseSchemeSecret ? "TOKEN_SECRET_PATH" : "TOKEN_SECRET_NAME"
            ),
            true
        );
        WaitReplicationInit(replicationClient, "/Root/replication");

        NDump::TClient backupClient(driver);
        {
            const auto result = backupClient.Dump("/Root", pathToBackup, NDump::TDumpSettings().Database("/Root"));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        if (UseSchemeSecret) {
            ExecuteQuery(session, "DROP SECRET `secret`;", true);
        } else {
            ExecuteQuery(session, "DROP OBJECT `secret` (TYPE SECRET);", true);
        }
        ExecuteQuery(session, "DROP ASYNC REPLICATION `/Root/replication` CASCADE;", true);
        {
            const auto result = backupClient.Restore(pathToBackup, "/Root");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    void TestExternalDataSourceBackupRestore(const TMaybe<ESecretType>& secretType, EAuthType authType) {
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TKikimrWithGrpcAndRootSchema server(config);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableSchemaSecrets(secretType == ESecretType::SecretTypeScheme);

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driverConfig = TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root");
        if (secretType) {
            driverConfig.SetAuthToken("root@builtin");
        }
        auto driver = TDriver(driverConfig);
        TTableClient tableClient(driver);
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();
        TSchemeClient schemeClient(driver);
        if (secretType) {
            TPermissions permissions("root@builtin", {"ydb.generic.full"});
            const auto result = schemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        constexpr const char* path = "/Root/externalDataSource";

        TestExternalDataSourceSettingsArePreserved(
            path,
            tableSession,
            querySession,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup),
            NDump::TRestoreSettings{},
            secretType,
            authType
        );
    }

    Y_UNIT_TEST_TWIN(RestoreExternalDataSourceWithoutSecret, UseSchemeSecret) {
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TKikimrWithGrpcAndRootSchema server(config);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableSchemaSecrets(UseSchemeSecret);

        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        TTableClient tableClient(driver);
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();

        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        if (UseSchemeSecret) {
            ExecuteQuery(querySession, "CREATE SECRET `secret` WITH (value = 'secret');", true);
        } else {
            ExecuteQuery(querySession, "CREATE OBJECT `secret` (TYPE SECRET) WITH (value = 'secret');", true);
        }
        ExecuteQuery(
            querySession,
            Sprintf(
                R"(
                    CREATE EXTERNAL DATA SOURCE `/Root/externalDataSource` WITH (
                        SOURCE_TYPE = "PostgreSQL",
                        DATABASE_NAME = "db",
                        LOCATION = "192.168.1.1:8123",
                        AUTH_METHOD = "BASIC",
                        LOGIN = "user",
                        %s = "secret"
                    );
                )",
                UseSchemeSecret ? "PASSWORD_SECRET_PATH" : "PASSWORD_SECRET_NAME"),
                true);

        NDump::TClient backupClient(driver);
        {
            const auto result = backupClient.Dump("/Root", pathToBackup, NDump::TDumpSettings().Database("/Root"));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        if (UseSchemeSecret) {
            ExecuteQuery(querySession, "DROP SECRET `secret`;", true);
        } else {
            ExecuteQuery(querySession, "DROP OBJECT `secret` (TYPE SECRET);", true);
        }
        ExecuteQuery(querySession, "DROP EXTERNAL DATA SOURCE `/Root/externalDataSource`;", true);
        {
            const auto result = backupClient.Restore(pathToBackup, "/Root");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    void TestExternalTableBackupRestore() {
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TKikimrWithGrpcAndRootSchema server(config);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
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

    void TestSystemViewBackupRestore() {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableShowCreate(true);
        TKikimrWithGrpcAndRootSchema server(config);
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        TSchemeClient schemeClient(driver);
        TTableClient tableClient(driver);
        auto session = tableClient.GetSession().ExtractValueSync().GetSession();

        {
            constexpr const char* systemDirectory = "/Root/.sys";
            const TTempDir tempDir;
            const auto& pathToBackup = tempDir.Path();
            TestReplaceSystemDirectoryACL(
                systemDirectory,
                schemeClient,
                CreateBackupLambda(driver, pathToBackup),
                CreateRestoreLambda(driver, pathToBackup)
            );
        }

        {
            constexpr const char* sysView = "/Root/.sys/partition_stats";
            const TTempDir tempDir;
            const auto& pathToBackup = tempDir.Path();
            TestReplaceSystemViewACL(
                sysView,
                session,
                schemeClient,
                CreateBackupLambda(driver, pathToBackup),
                CreateRestoreLambda(driver, pathToBackup)
            );
        }
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
            case EPathTypeReplication: {
                TestReplicationBackupRestore(ESecretType::SecretTypeOld);
                TestReplicationBackupRestore(ESecretType::SecretTypeScheme);
                return;
            }
            case EPathTypeTransfer:
                return TestTransferBackupRestore();
            case EPathTypeExternalTable:
                return TestExternalTableBackupRestore();
            case EPathTypeExternalDataSource: {
                TestExternalDataSourceBackupRestore(/* secretType */ Nothing(), EAuthType::AuthTypeNone);
                TestExternalDataSourceBackupRestore(ESecretType::SecretTypeOld, EAuthType::AuthTypeToken);
                TestExternalDataSourceBackupRestore(ESecretType::SecretTypeScheme, EAuthType::AuthTypeToken);
                TestExternalDataSourceBackupRestore(ESecretType::SecretTypeOld, EAuthType::AuthTypeAws);
                TestExternalDataSourceBackupRestore(ESecretType::SecretTypeScheme, EAuthType::AuthTypeAws);
                return;
            }
            case EPathTypeResourcePool:
                break; // https://github.com/ydb-platform/ydb/issues/10440
            case EPathTypeKesus:
                return TestKesusBackupRestore();
            case EPathTypeColumnStore:
            case EPathTypeColumnTable:
                break; // https://github.com/ydb-platform/ydb/issues/10459
            case EPathTypeSysView:
                return TestSystemViewBackupRestore();
            case EPathTypeSecret:
                break; // TODO(yurikiselev): Support backups [issue:23489]
            case EPathTypeInvalid:
            case EPathTypeBackupCollection:
            case EPathTypeBlobDepot:
                break; // not applicable
            case EPathTypeRtmrVolume:
            case EPathTypeBlockStoreVolume:
            case EPathTypeSolomonVolume:
            case EPathTypeFileStore:
                break; // other projects
            case EPathTypeStreamingQuery:
                break; // https://github.com/ydb-platform/ydb/issues/22571
            default:
                UNIT_FAIL("Client backup/restore were not implemented for this scheme object");
                // please don't forget to add:
                // - a dedicated TestReplaceRestoreOption test
        }
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(TestAllIndexTypes, NKikimrSchemeOp::EIndexType) {
        using namespace NKikimrSchemeOp;

        switch (Value) {
            case EIndexTypeGlobal:
            case EIndexTypeGlobalAsync:
            case EIndexTypeGlobalUnique:
            case EIndexTypeGlobalVectorKmeansTree:
            case EIndexTypeGlobalFulltextPlain:
            case EIndexTypeGlobalFulltextRelevance:
                return TestTableWithIndexBackupRestore(Value);
            case EIndexTypeInvalid:
                break; // not applicable
            default:
                UNIT_FAIL("Client backup/restore were not implemented for this index type");
        }
    }

    Y_UNIT_TEST_TWIN(TestReplaceRestoreOption, IsOlap) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableShowCreate(true);
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TKikimrWithGrpcAndRootSchema server(config);

        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicTransfer(true);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableSchemaSecrets(true);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NLog::EPriority::PRI_DEBUG);
        server.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::EPriority::PRI_DEBUG);
        server.GetRuntime()->SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::EPriority::PRI_DEBUG);

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driver = TDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root").SetAuthToken("root@builtin"));

        TSchemeClient schemeClient(driver);
        TTableClient tableClient(driver);
        auto tableSession = tableClient.GetSession().ExtractValueSync().GetSession();
        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();
        NTopic::TTopicClient topicClient(driver);
        TReplicationClient replicationClient(driver);
        TReplicationClient transferClient(driver);
        NCoordination::TClient nodeClient(driver);

        TPermissions permissions("root@builtin", {"ydb.generic.full"});
        const auto result = schemeClient.ModifyPermissions("/Root",
            TModifyPermissionsSettings().AddGrantPermissions(permissions)
        ).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        auto cleanup = [&pathToBackup, &driver] {
            TVector<TFsPath> children;
            pathToBackup.List(children);
            for (auto& i : children) {
                i.ForceDelete();
            }

            const auto settings = NConsoleClient::TRemoveDirectoryRecursiveSettings().RemoveSelf(false);
            RemoveDirectoryRecursive(driver, "/Root", settings);
        };

        constexpr const char* table = "/Root/table";
        constexpr const char* topic = "/Root/topic";
        constexpr const char* view = "/Root/view";
        constexpr const char* externalTable = "/Root/externalTable";
        constexpr const char* externalDataSource = "/Root/externalDataSource";
        const std::string kesus = "/Root/kesus";
        constexpr const char* systemDirectory = "/Root/.sys";
        constexpr const char* sysView = "/Root/.sys/partition_stats";

        const auto restorationSettings = NDump::TRestoreSettings().Replace(true);

        cleanup();
        TestTableContentIsPreserved(table, querySession,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), IsOlap, restorationSettings
        );

        cleanup();
        TestTopicSettingsArePreserved(topic, querySession, topicClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), restorationSettings
        );

        cleanup();
        TestTransferSettingsArePreserved(endpoint, querySession, transferClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), pathToBackup,
            TTransferTestConfig { .Replace = restorationSettings.Replace_ }
        );

        cleanup();
        TestViewOutputIsPreserved(view, querySession,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), restorationSettings
        );

        cleanup();
        TestExternalDataSourceSettingsArePreserved(externalDataSource, tableSession, querySession,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), restorationSettings,
            ESecretType::SecretTypeOld, EAuthType::AuthTypeToken
        );

        cleanup();
        TestExternalTableSettingsArePreserved(externalTable, externalDataSource, tableSession, querySession,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), restorationSettings
        );

        cleanup();
        TestCoordinationNodeSettingsArePreserved(kesus, nodeClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), restorationSettings
        );

        cleanup();
        TestReplicationSettingsArePreserved(endpoint, querySession, replicationClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), ESecretType::SecretTypeOld, restorationSettings
        );

        cleanup();
        TestReplaceSystemDirectoryACL(systemDirectory, schemeClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings)
        );

        cleanup();
        TestReplaceSystemViewACL(sysView, tableSession, schemeClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings)
        );
    }

    Y_UNIT_TEST(TestReplaceRestoreOptionOnNonExistingSchemeObjects) {
        NKikimrConfig::TAppConfig config;
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        config.MutableFeatureFlags()->SetEnableShowCreate(true);
        TKikimrWithGrpcAndRootSchema server(config);

        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicTransfer(true);
        server.GetRuntime()->GetAppData().FeatureFlags.SetEnableSchemaSecrets(true);

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driver = TDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root").SetAuthToken("root@builtin"));

        TSchemeClient schemeClient(driver);
        TTableClient tableClient(driver);
        auto tableSession = tableClient.GetSession().ExtractValueSync().GetSession();
        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();
        NTopic::TTopicClient topicClient(driver);
        TReplicationClient replicationClient(driver);
        TReplicationClient transferClient(driver);
        NCoordination::TClient nodeClient(driver);

        TPermissions permissions("root@builtin", {"ydb.generic.full"});
        const auto result = schemeClient.ModifyPermissions("/Root",
            TModifyPermissionsSettings().AddGrantPermissions(permissions)
        ).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        auto cleanup = [&pathToBackup, &driver] {
            TVector<TFsPath> children;
            pathToBackup.List(children);
            for (auto& i : children) {
                i.ForceDelete();
            }

            const auto settings = NConsoleClient::TRemoveDirectoryRecursiveSettings().RemoveSelf(false);
            RemoveDirectoryRecursive(driver, "/Root", settings);
        };

        constexpr const char* table = "/Root/table";
        constexpr const char* topic = "/Root/topic";
        constexpr const char* view = "/Root/view";
        constexpr const char* externalTable = "/Root/externalTable";
        constexpr const char* externalDataSource = "/Root/externalDataSource";
        const std::string kesus = "/Root/kesus";

        const auto restorationSettings = NDump::TRestoreSettings().Replace(true);

        cleanup();
        TestTableContentIsPreserved(table, querySession,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), false
        );

        cleanup();
        TestTopicSettingsArePreserved(topic, querySession, topicClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings)
        );

        cleanup();
        TestTransferSettingsArePreserved(endpoint, querySession, transferClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), pathToBackup,
            TTransferTestConfig { .Replace = restorationSettings.Replace_ }
        );

        cleanup();
        TestViewOutputIsPreserved(view, querySession,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings)
        );

        cleanup();
        TestExternalDataSourceSettingsArePreserved(externalDataSource, tableSession, querySession,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings),
            NDump::TRestoreSettings{}, ESecretType::SecretTypeOld, EAuthType::AuthTypeToken
        );

        cleanup();
        TestExternalTableSettingsArePreserved(externalTable, externalDataSource, tableSession, querySession,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings)
        );

        cleanup();
        TestCoordinationNodeSettingsArePreserved(kesus, nodeClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings)
        );

        cleanup();
        TestReplicationSettingsArePreserved(endpoint, querySession, replicationClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), ESecretType::SecretTypeOld
        );

        cleanup();
        TestReplicationSettingsArePreserved(endpoint, querySession, replicationClient,
            CreateBackupLambda(driver, pathToBackup), CreateRestoreLambda(driver, pathToBackup, "/Root", restorationSettings), ESecretType::SecretTypeScheme
        );
    }

    Y_UNIT_TEST(PrefixedVectorIndex) {
        TestTableWithIndexBackupRestore(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, true);
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(TestAllPrimitiveTypes, Ydb::Type::PrimitiveTypeId) {
        if (DontTestThisType(Value)) {
            return;
        }
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        TestPrimitiveType(
            Value,
            session,
            CreateBackupLambda(driver, pathToBackup),
            CreateRestoreLambda(driver, pathToBackup),
            false
        );
    }

    Y_UNIT_TEST(RestoreReplicationThatDoesNotUseSecret) {
        TestReplicationBackupRestore(/* tokenSecretType */ Nothing());
    }

    Y_UNIT_TEST(BackupRestoreTransfer_UseTokenWithOldSecret) {
        TestTransferBackupRestore(TTransferTestConfig {
            .SecretType = ESecretType::SecretTypeOld,
            .AuthType = EAuthType::AuthTypeToken,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_UseTokenWithSchemaSecret) {
        TestTransferBackupRestore(TTransferTestConfig {
            .SecretType = ESecretType::SecretTypeScheme,
            .AuthType = EAuthType::AuthTypeToken,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_UseUserPasswordWithOldSecret) {
        TestTransferBackupRestore(TTransferTestConfig {
            .SecretType = ESecretType::SecretTypeOld,
            .AuthType = EAuthType::AuthTypePassword,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_UseUserPasswordWithSchemaSecret) {
        TestTransferBackupRestore(TTransferTestConfig {
            .SecretType = ESecretType::SecretTypeScheme,
            .AuthType = EAuthType::AuthTypePassword,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_NoTokenNoUserPassword) {
        TestTransferBackupRestore(TTransferTestConfig {
            .SecretType = Nothing(),
            .AuthType = EAuthType::AuthTypeNone,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_NoConnectionStringFakeTopicPath) {
        TestTransferBackupRestore(TTransferTestConfig {
            .FakeTopicPath = true,
            .UseConnectionString = false,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_WithConnectionStringFakeTopicPath) {
        TestTransferBackupRestore(TTransferTestConfig {
            .FakeTopicPath = true,
            .UseConnectionString = true,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_NoConnectionStringRelativeTableTopicTransferPath) {
        TestTransferBackupRestore(TTransferTestConfig {
            .TablePath = "test_table",
            .TopicPath = "test_topic",
            .TransferPath = "test_transfer",
            .UseConnectionString = false,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_NoConnectionStringRelativeTopicAbsoluteTransferPath) {
        TestTransferBackupRestore(TTransferTestConfig {
            .TopicPath = "test_topic",
            .TransferPath = "/Root/test_transfer",
            .UseConnectionString = false,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_NoConnectionStringAbsoluteTopicRelativeTransferPath) {
        TestTransferBackupRestore(TTransferTestConfig {
            .TopicPath = "/Root/test_topic",
            .TransferPath = "test_transfer",
            .UseConnectionString = false,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_NoConnectionStringAbsolutePath) {
        TestTransferBackupRestore(TTransferTestConfig {
            .UseConnectionString = false,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_Replace) {
        TestTransferBackupRestore(TTransferTestConfig {
            .FlushInterval = TDuration::Seconds(1),
            .Replace = true,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_ComplexLambdaDropRestore) {
        TestTransferBackupRestore(TTransferTestConfig {
            .BatchSizeBytes = 100,
            .FlushInterval = TDuration::Seconds(86399),
            .Replace = false,
            .ComplexLambda = true,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_ComplexLambdaReplace) {
        TestTransferBackupRestore(TTransferTestConfig {
            .BatchSizeBytes = 10241024,
            .FlushInterval = TDuration::Seconds(5),
            .Replace = true,
            .ComplexLambda = true,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_ComplexLambdaInsideUsingDropRestore) {
        TestTransferBackupRestore(TTransferTestConfig {
            .BatchSizeBytes = 100,
            .FlushInterval = TDuration::Seconds(86399),
            .Replace = false,
            .ComplexLambda = true,
            .LambdaInsideUsing = true,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_ComplexLambdaInsideUsingReplace) {
        TestTransferBackupRestore(TTransferTestConfig {
            .BatchSizeBytes = 10241024,
            .FlushInterval = TDuration::Seconds(5),
            .Replace = true,
            .ComplexLambda = true,
            .LambdaInsideUsing = true,
            .BackupRestoreAttemptsCount = 3,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_SubFoldersReplace) {
        TestTransferBackupRestore(TTransferTestConfig {
            .TablePath = "/Root/table_folder/test_table",
            .TopicPath = "/Root/topic_folder/test_topic",
            .FlushInterval = TDuration::Seconds(86399),
            .Replace =  true,
            .ComplexLambda = true,
            .BackupRestoreAttemptsCount = 5,
        });
    }

    Y_UNIT_TEST(BackupRestoreTransfer_SubFoldersDropRestore) {
        TestTransferBackupRestore(TTransferTestConfig {
            .TablePath = "/Root/table_folder/test_table",
            .TopicPath = "/Root/topic_folder/test_topic",
            .FlushInterval = TDuration::Seconds(5),
            .Replace =  false,
            .ComplexLambda = true,
            .BackupRestoreAttemptsCount = 5,
        });
    }

    Y_UNIT_TEST_TWIN(ReplicasAreNotBackedUp, UseSchemeSecret) {
        using namespace fmt::literals;
        TKikimrWithGrpcAndRootSchema server;

        const auto endpoint = Sprintf("localhost:%u", server.GetPort());
        auto driver = TDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root").SetAuthToken("root@builtin"));

        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TReplicationClient replicationClient(driver);
        TSchemeClient schemeClient(driver);

        TPermissions permissions("root@builtin", {"ydb.generic.full"});
        const auto result = schemeClient.ModifyPermissions("/Root",
            TModifyPermissionsSettings().AddGrantPermissions(permissions)
        ).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();

        if (UseSchemeSecret) {
            ExecuteQuery(session, "CREATE SECRET `secret` WITH (value = 'root@builtin');", true);
        } else {
            ExecuteQuery(session, "CREATE OBJECT `secret` (TYPE SECRET) WITH (value = 'root@builtin');", true);
        }
        ExecuteQuery(session, fmt::format("CREATE TABLE `/Root/table` (k Uint32, v Utf8, PRIMARY KEY (k));"), true);
        ExecuteQuery(session,
            Sprintf(R"(
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = 'grpc://%s/?database=/Root',
                    %s = 'secret'
                );)",
                endpoint.c_str(),
                UseSchemeSecret ? "TOKEN_SECRET_PATH" : "TOKEN_SECRET_NAME"
            ),
            true
        );
        WaitReplicationInit(replicationClient, "/Root/replication");

        NDump::TClient backupClient(driver);
        {
            const auto result = backupClient.Dump("/Root", pathToBackup, NDump::TDumpSettings().Database("/Root"));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            {
                TVector<TString> children;
                pathToBackup.ListNames(children);

                UNIT_ASSERT_C(FindPtr(children, "table"), JoinSeq(", ", children));
                UNIT_ASSERT_C(FindPtr(children, "replication"), JoinSeq(", ", children));
                UNIT_ASSERT_C(!FindPtr(children, "replica"), JoinSeq(", ", children));
            }

            {
                auto source = pathToBackup.Child("table");
                TVector<TFsPath> children;
                source.List(children);

                for (const auto& child : children) {
                    UNIT_ASSERT(!child.IsDirectory());
                }
            }
        }
    }

    Y_UNIT_TEST(SkipEmptyDirsOnRestore) {
        TKikimrWithGrpcAndRootSchema server;
        auto driver = TDriver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", server.GetPort())).SetDatabase("/Root"));
        TTempDir tempDir;
        const auto& pathToBackup = tempDir.Path();
        pathToBackup.Child("empty").MkDir();
        auto dir = pathToBackup.Child("with_one_file");
        dir.MkDir();
        dir.Child("file").Touch();
        pathToBackup.Child("with_one_dir").Child("empty").MkDirs();

        NDump::TClient backupClient(driver);
        {
            const auto result = backupClient.Restore(pathToBackup, "/Root");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TSchemeClient schemeClient(driver);
        {
            auto [entries, status] = NConsoleClient::RecursiveList(schemeClient, "/Root", {}, false);
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(entries.size(), 2, JoinSeq(", ", entries));
            Sort(entries, [](const auto& lhs, const auto& rhs) {
                return lhs.Name < rhs.Name;
            });
            UNIT_ASSERT_STRINGS_EQUAL_C(entries[0].Name, "/Root/with_one_dir", entries[0]);
            UNIT_ASSERT_STRINGS_EQUAL_C(entries[1].Name, "/Root/with_one_file", entries[1]);
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
                    appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);
                    appConfig.MutableFeatureFlags()->SetEnableFulltextIndex(true);
                    appConfig.MutableFeatureFlags()->SetEnableColumnTablesBackup(true);
                    return appConfig;
                }())
            , Driver(TDriverConfig().SetEndpoint(Sprintf("localhost:%u", Server.GetPort())).SetDatabase("/Root"))
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
            runtime.GetAppData().FeatureFlags.SetEnableViewExport(true);
            runtime.GetAppData().FeatureFlags.SetEnableSysViewPermissionsExport(true);
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
            NYdb::NScheme::ESchemeEntryType::ColumnTable,
            NYdb::NScheme::ESchemeEntryType::Table,
            NYdb::NScheme::ESchemeEntryType::View,
            NYdb::NScheme::ESchemeEntryType::Topic,
            NYdb::NScheme::ESchemeEntryType::Replication,
            NYdb::NScheme::ESchemeEntryType::Transfer,
            NYdb::NScheme::ESchemeEntryType::ExternalDataSource,
            NYdb::NScheme::ESchemeEntryType::ExternalTable,
            NYdb::NScheme::ESchemeEntryType::SysView,
        }, entry.Type) && entry.Name != "replica"; // Hack to avoid replica table export
    }

    void RecursiveListSourceToItems(TSchemeClient& schemeClient, const TString& source, const TString& destination,
        NExport::TExportToS3Settings& exportSettings
    ) {
        const auto listSettings = NConsoleClient::TRecursiveListSettings()
            .Filter(FilterSupportedSchemeObjects)
            .SkipSys(false);

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
        return [=, &driver]() {
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
        return [=, &driver]() {
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

    Y_UNIT_TEST(RestoreIndexTableReadReplicasSettings) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr const char* index = "byValue";
        constexpr auto readReplicasMode = NYdb::NTable::TReadReplicasSettings::EMode::PerAz;
        constexpr ui64 readReplicasCount = 1;

        TestIndexTableReadReplicasSettingsArePreserved(
            table,
            index,
            readReplicasMode,
            readReplicasCount,
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

    Y_UNIT_TEST(RestoreViewWithNamedExpressions) {
        TS3TestEnv testEnv;
        constexpr const char* view = "/Root/view";
        constexpr const char* table = "/Root/a/b/c/table";

        TestViewWithNamedExpressions(
            view,
            table,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "view" })
        );
    }

    Y_UNIT_TEST(RestoreViewSelectFromIndex) {
        TS3TestEnv testEnv;
        constexpr const char* view = "/Root/view";
        constexpr const char* table = "/Root/a/b/c/table";

        TestViewSelectFromIndex(
            view,
            table,
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
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "view", "a/b/c/table" }),
            false
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

    Y_UNIT_TEST(RestoreViewTablePathPrefix) {
        TS3TestEnv testEnv;
        constexpr const char* view = "view";
        constexpr const char* table = "table";
        constexpr const char* restoredView = "a/b/c/view";

        TestViewRelativeReferencesArePreserved(
            view,
            table,
            restoredView,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "a/b/c/view", "a/b/c/table" }),
            false, "PRAGMA TablePathPrefix = '/Root/a/b/c';\n"
        );
    }

    Y_UNIT_TEST(RestoreViewTablePathPrefixLowercase) {
        TS3TestEnv testEnv;
        constexpr const char* view = "view";
        constexpr const char* table = "table";
        constexpr const char* restoredView = "a/b/c/view";

        TestViewRelativeReferencesArePreserved(
            view,
            table,
            restoredView,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "a/b/c/view", "a/b/c/table" }),
            false, "pragma tablepathprefix = '/Root/a/b/c';\n" // note the lowercase
        );
    }

    // TO DO: test view restoration to a different database

    void TestTableBackupRestore() {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";

        TestTableContentIsPreserved(
            table,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "table" }),
            false
        );
    }

    void TestTableWithIndexBackupRestore(NKikimrSchemeOp::EIndexType indexType = NKikimrSchemeOp::EIndexTypeGlobal, bool prefix = false) {
        TS3TestEnv testEnv;
        constexpr const char* table = "/Root/table";
        constexpr const char* index = "value_idx";

        TestRestoreTableWithIndex(
            table,
            index,
            indexType,
            prefix,
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

    void TestSystemViewBackupRestore() {
        TS3TestEnv testEnv;
        TSchemeClient schemeClient(testEnv.GetDriver());
        constexpr const char* sysView = "/Root/.sys/partition_stats";

        TestReplaceSystemViewACL(
            sysView,
            testEnv.GetTableSession(),
            schemeClient,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), {".sys/partition_stats"})
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

    void TestReplicationBackupRestore( const TMaybe<ESecretType>& tokenSecretType) {
        TS3TestEnv testEnv;
        auto& featureFlags = testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags;
        featureFlags.SetEnableSchemaSecrets(tokenSecretType == ESecretType::SecretTypeScheme);

        const auto endpoint = Sprintf("localhost:%u", testEnv.GetServer().GetPort());
        auto driverConfig = TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root");
        if (tokenSecretType) {
            driverConfig.SetAuthToken("root@builtin");
        }
        auto driver = TDriver(driverConfig);
        TSchemeClient schemeClient(driver);
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TReplicationClient replicationClient(driver);

        if (tokenSecretType) {
            TPermissions permissions("root@builtin", {"ydb.generic.full"});
            const auto result = schemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TestReplicationSettingsArePreserved(
            endpoint,
            session,
            replicationClient,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), {"replication"}),
            tokenSecretType
        );

    }

    void TestTransferBackupRestore(const TMaybe<ESecretType>& tokenSecretType) {
        TS3TestEnv testEnv;
        auto& featureFlags = testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags;
        featureFlags.SetEnableSchemaSecrets(tokenSecretType == ESecretType::SecretTypeScheme);

        const auto endpoint = Sprintf("localhost:%u", testEnv.GetServer().GetPort());
        auto driverConfig = TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root");
        if (tokenSecretType) {
            driverConfig.SetAuthToken("root@builtin");
        }
        auto driver = TDriver(driverConfig);
        TSchemeClient schemeClient(driver);
        NQuery::TQueryClient queryClient(driver);
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();
        TReplicationClient replicationClient(driver);

        if (tokenSecretType) {
            TPermissions permissions("root@builtin", {"ydb.generic.full"});
            const auto result = schemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TTransferTestConfig config;
        config.SecretType = tokenSecretType;

        TTempDir tempDir;

        TestTransferSettingsArePreserved(
            endpoint,
            session,
            replicationClient,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), {"test_transfer", "test_topic"}),
            tempDir.Path(),
            config
        );
    }

    void TestExternalTableBackupRestore() {
        TS3TestEnv testEnv;
        auto& featureFlags = testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags;
        featureFlags.SetEnableExternalDataSources(true);

        const auto endpoint = Sprintf("localhost:%u", testEnv.GetServer().GetPort());
        auto driverConfig = TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root");

        auto driver = TDriver(driverConfig);
        TTableClient tableClient(driver);
        auto tableSession = tableClient.GetSession().ExtractValueSync().GetSession();
        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();

        TestExternalTableSettingsArePreserved(
            "/Root/externalTable",
            "/Root/externalDataSource",
            tableSession,
            querySession,
            CreateBackupLambda(driver, testEnv.GetS3Port()),
            CreateRestoreLambda(driver, testEnv.GetS3Port(), {"externalTable", "externalDataSource"})
        );
    }

    void TestExternalDataSourceBackupRestore(const TMaybe<ESecretType>& secretType, EAuthType authType) {
        TS3TestEnv testEnv;
        auto& featureFlags = testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags;
        featureFlags.SetEnableExternalDataSources(true);
        featureFlags.SetEnableSchemaSecrets(secretType == ESecretType::SecretTypeScheme);

        const auto endpoint = Sprintf("localhost:%u", testEnv.GetServer().GetPort());
        auto driverConfig = TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root");
        if (secretType) {
            driverConfig.SetAuthToken("root@builtin");
        }

        auto driver = TDriver(driverConfig);
        TSchemeClient schemeClient(driver);
        TTableClient tableClient(driver);
        auto tableSession = tableClient.GetSession().ExtractValueSync().GetSession();
        NQuery::TQueryClient queryClient(driver);
        auto querySession = queryClient.GetSession().ExtractValueSync().GetSession();

        if (secretType) {
            TPermissions permissions("root@builtin", {"ydb.generic.full"});
            const auto result = schemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TestExternalDataSourceSettingsArePreserved(
            "/Root/externalDataSource",
            tableSession,
            querySession,
            CreateBackupLambda(driver, testEnv.GetS3Port()),
            CreateRestoreLambda(driver, testEnv.GetS3Port(), {"externalDataSource"}),
            NDump::TRestoreSettings{},
            secretType,
            authType
        );
    }

    void TestTopicBackupRestoreWithoutData() {
        TS3TestEnv testEnv;
        NTopic::TTopicClient topicClient(testEnv.GetDriver());
        constexpr const char* topic = "/Root/topic";

        TestTopicSettingsArePreserved(
            topic,
            testEnv.GetQuerySession(),
            topicClient,
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { "topic" })
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
                TestTopicBackupRestoreWithoutData();
                break;
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
                TestReplicationBackupRestore(ESecretType::SecretTypeOld);
                TestReplicationBackupRestore(ESecretType::SecretTypeScheme);
                return;
            case EPathTypeTransfer:
                TestTransferBackupRestore(ESecretType::SecretTypeOld);
                TestTransferBackupRestore(ESecretType::SecretTypeScheme);
                break;
            case EPathTypeSysView:
                TestSystemViewBackupRestore();
                break;
            case EPathTypeExternalTable:
                return TestExternalTableBackupRestore();
            case EPathTypeExternalDataSource: {
                TestExternalDataSourceBackupRestore(/* secretType */ Nothing(), EAuthType::AuthTypeNone);
                TestExternalDataSourceBackupRestore(ESecretType::SecretTypeOld, EAuthType::AuthTypeToken);
                TestExternalDataSourceBackupRestore(ESecretType::SecretTypeScheme, EAuthType::AuthTypeToken);
                TestExternalDataSourceBackupRestore(ESecretType::SecretTypeOld, EAuthType::AuthTypeAws);
                TestExternalDataSourceBackupRestore(ESecretType::SecretTypeScheme, EAuthType::AuthTypeAws);
                return;
            }
            case EPathTypeResourcePool:
                break; // https://github.com/ydb-platform/ydb/issues/10440
            case EPathTypeKesus:
                break; // https://github.com/ydb-platform/ydb/issues/10444
            case EPathTypeColumnStore:
            case EPathTypeColumnTable:
                break; // https://github.com/ydb-platform/ydb/issues/10459
            case EPathTypeSecret:
                break; // TODO(yurikiselev): Support backups [issue:23489]
            case EPathTypeInvalid:
            case EPathTypeBackupCollection:
            case EPathTypeBlobDepot:
            case EPathTypeRtmrVolume:
            case EPathTypeBlockStoreVolume:
            case EPathTypeSolomonVolume:
            case EPathTypeFileStore:
                break; // other projects
            case EPathTypeStreamingQuery:
                break; // https://github.com/ydb-platform/ydb/issues/22571
            default:
                UNIT_FAIL("S3 backup/restore were not implemented for this scheme object");
        }
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES(TestAllIndexTypes, NKikimrSchemeOp::EIndexType) {
        using namespace NKikimrSchemeOp;

        switch (Value) {
            case EIndexTypeGlobal:
            case EIndexTypeGlobalAsync:
            case EIndexTypeGlobalUnique:
            case EIndexTypeGlobalVectorKmeansTree:
            case EIndexTypeGlobalFulltextPlain:
            case EIndexTypeGlobalFulltextRelevance:
                TestTableWithIndexBackupRestore(Value);
                break;
            case EIndexTypeInvalid:
                break; // not applicable
            default:
                UNIT_FAIL("S3 backup/restore were not implemented for this index type");
        }
    }

    Y_UNIT_TEST(PrefixedVectorIndex) {
        TestTableWithIndexBackupRestore(NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, true);
    }

    Y_UNIT_TEST_ALL_PROTO_ENUM_VALUES_WITH_FLAG(TestAllPrimitiveTypes, Ydb::Type::PrimitiveTypeId, IsOlap) {
        if (DontTestThisType(Value, IsOlap)) {
            return;
        }
        TS3TestEnv testEnv;

        TestPrimitiveType(
            Value,
            testEnv.GetQuerySession(),
            CreateBackupLambda(testEnv.GetDriver(), testEnv.GetS3Port()),
            CreateRestoreLambda(testEnv.GetDriver(), testEnv.GetS3Port(), { GetTableName(GetYqlType(Value), "") } ),
            IsOlap
        );
    }

    Y_UNIT_TEST(ExcludeNonSupportedObjectsInExport) {
        TS3TestEnv testEnv;
        testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableExternalDataSources(true);
        // Disable views export
        testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableViewExport(false);
        // Disable external data source export
        TControlBoard::SetValue(
            0,
            testEnv.GetServer().GetRuntime()->GetAppData().Icb->BackupControls.S3Controls.EnableExternalDataSourceExport
        );

        // Enable destination prefix in rpc_export
        testEnv.GetServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableEncryptedExport(true);

        TSchemeClient schemeClient(testEnv.GetDriver());
        auto result = schemeClient.MakeDirectory("/Root/Dir").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto& querySession = testEnv.GetQuerySession();

        ExecuteQuery(
            querySession,
            "CREATE VIEW `View` WITH security_invoker = TRUE AS SELECT 1;",
            true
        );

        ExecuteQuery(
            querySession,
            "CREATE VIEW `Dir/View` WITH security_invoker = TRUE AS SELECT 1;",
            true
        );

        ExecuteQuery(
            querySession,
            R"(
                CREATE TABLE `Table` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key)
                );
            )",
            true
        );

        ExecuteQuery(
            querySession,
            R"(
                CREATE TABLE `Dir/Table` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key)
                );
            )",
            true
        );

        ExecuteQuery(
            querySession,
            R"(
                CREATE EXTERNAL DATA SOURCE `DataSource` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="https://object_storage_domain/bucket/",
                    AUTH_METHOD="NONE"
                );
            )",
            true
        );

        ExecuteQuery(
            querySession,
            R"(
                CREATE EXTERNAL DATA SOURCE `Dir/DataSource` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="https://object_storage_domain/bucket/",
                    AUTH_METHOD="NONE"
                );
            )",
            true
        );

        auto exportSettings = NExport::TExportToS3Settings()
            .Endpoint(Sprintf("localhost:%u", testEnv.GetS3Port()))
            .Scheme(ES3Scheme::HTTP)
            .Bucket("test_bucket")
            .AccessKey("test_key")
            .SecretKey("test_secret")
            .DestinationPrefix("Dest")
            .SourcePath("/Root");

        const auto clientSettings = TCommonClientSettings().Database("/Root");
        NExport::TExportClient exportClient(testEnv.GetDriver(), clientSettings);
        auto response = exportClient.ExportToS3(exportSettings).ExtractValueSync();

        NOperation::TOperationClient operationClient(testEnv.GetDriver());
        UNIT_ASSERT_C(response.Status().IsSuccess(), response.Status().GetIssues().ToString());
        UNIT_ASSERT_C(WaitForOperation<NExport::TExportToS3Response>(operationClient, response.Id()),
            "The export did not complete within the allocated time."
        );

        auto opResponse = operationClient.Get<NExport::TExportToS3Response>(response.Id()).ExtractValueSync();
        auto& items = opResponse.Metadata().Settings.Item_;

        UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(items[0].Src, "/Root/Dir/Table");
        UNIT_ASSERT_VALUES_EQUAL(items[0].Dst, "Dest/Dir/Table");
        UNIT_ASSERT_VALUES_EQUAL(items[1].Src, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(items[1].Dst, "Dest/Table");
    }
}
