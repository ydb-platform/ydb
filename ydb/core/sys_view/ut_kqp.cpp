#include "ut_common.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/sys_view/show_create/create_view_formatter.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/library/testlib/common/test_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/yson/node/node_io.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NDump;
using namespace NYdb::NScheme;
using namespace NYdb::NTable;
using namespace NTestUtils;

namespace {

void FillRootTable(TTestEnv& env, ui16 tableNum = 0) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(Sprintf(R"(
        REPLACE INTO `Root/Table%u` (Key, Value) VALUES
            (0u, "X"),
            (1u, "Y"),
            (2u, "Z");
    )", tableNum), TTxControl::BeginTx().CommitTx()).GetValueSync());
}

void CreateRootTable(TTestEnv& env, ui64 partitionCount = 1, bool fillTable = false, ui16 tableNum = 0) {
    env.GetClient().CreateTable("/Root", Sprintf(R"(
        Name: "Table%u"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Value", Type: "String" }
        KeyColumnNames: ["Key"]
        UniformPartitionsCount: %lu
    )", tableNum, partitionCount));

    if (fillTable)
        FillRootTable(env, tableNum);
}

void CreateRootColumnTable(TTestEnv& env, ui64 partitionCount = 1, bool fillTable = false, ui16 tableNum = 0) {
    NQuery::TQueryClient client(env.GetDriver());
    auto result = client.ExecuteQuery(Sprintf(R"(
        CREATE TABLE `/Root/Table%u` (
            Key Int32 NOT NULL,
            Value Utf8,
            PRIMARY KEY(Key)
        ) WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %lu);
    )", tableNum, partitionCount), NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    if (fillTable)
        FillRootTable(env, tableNum);
}

void BreakLock(TSession& session, const TString& tableName) {
    std::optional<TTransaction> tx1;

    {  // tx0: write test data
        auto result = session.ExecuteDataQuery(TStringBuilder() <<
            "UPSERT INTO `" << tableName << "` (Key, Value) VALUES (55u, \"Fifty five\")",
        TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {  // tx0: read all data
        auto result = session.ExecuteDataQuery(TStringBuilder() <<
            "SELECT * FROM `" << tableName << "`",
        TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    while (!tx1) {
    // tx1: start reading
        auto result = session.ExecuteDataQuery(TStringBuilder() <<
            "SELECT * FROM `" << tableName << "` WHERE Key = 55u",
        TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        TString yson = FormatResultSetYson(result.GetResultSet(0));
        if (yson == "[]") {
            continue;
        }

        NKqp::CompareYson(R"([
            [[55u];["Fifty five"]];
        ])", yson);
        tx1 = result.GetTransaction();
        UNIT_ASSERT(tx1);
    }

    {  // tx2: write + commit
        auto result = session.ExecuteDataQuery(TStringBuilder() <<
            "UPSERT INTO `" << tableName << "` (Key, Value) VALUES (55u, \"NewValue1\")",
        TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {  // tx1: try to commit
        auto result = tx1->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

size_t GetRowCount(TTableClient& client, const TString& tableName, const TString& condition = {}) {
    TStringBuilder query;
    query << "SELECT * FROM `" << tableName << "`";
    if (!condition.empty())
        query << " WHERE " << condition;
    auto it = client.StreamExecuteScanQuery(query).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    auto ysonString = NKqp::StreamResultToYson(it);
    auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
    UNIT_ASSERT(node.IsList());
    return node.AsList().size();
}

ui64 GetIntervalEnd(TTableClient& client, const TString& name) {
    TStringBuilder query;
    query << "SELECT MAX(IntervalEnd) FROM `" << name << "`";
    auto it = client.StreamExecuteScanQuery(query).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    auto ysonString = NKqp::StreamResultToYson(it);
    auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
    UNIT_ASSERT(node.IsList());
    UNIT_ASSERT(node.AsList().size() == 1);
    auto row = node.AsList()[0];
    UNIT_ASSERT(row.IsList());
    UNIT_ASSERT(row.AsList().size() == 1);
    auto value = row.AsList()[0];
    UNIT_ASSERT(value.IsList());
    UNIT_ASSERT(value.AsList().size() == 1);
    return value.AsList()[0].AsUint64();
}

void WaitForStats(TTableClient& client, const TString& tableName, const TString& condition = {}) {
    size_t rowCount = 0;
    for (size_t iter = 0; iter < 30; ++iter) {
        if (rowCount = GetRowCount(client, tableName, condition))
            break;
        Sleep(TDuration::Seconds(5));
    }
    UNIT_ASSERT_GE(rowCount, 0);
}

NQuery::TExecuteQueryResult ExecuteQuery(NQuery::TSession& session, const std::string& query) {
    auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result;
}

NKikimrSchemeOp::TPathDescription DescribePath(TTestActorRuntime& runtime, TString&& path) {
    if (!IsStartWithSlash(path)) {
        path = CanonizePath(JoinPath({"/Root", path}));
    }
    auto sender = runtime.AllocateEdgeActor();
    TAutoPtr<IEventHandle> handle;

    auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(path);
    request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
    request->Record.MutableDescribePath()->MutableOptions()->SetReturnBoundaries(true);
    request->Record.MutableDescribePath()->MutableOptions()->SetReturnSetVal(true);
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    return runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle)->GetRecord().GetPathDescription();
}

class TShowCreateChecker {
public:

    explicit TShowCreateChecker(TTestEnv& env)
        : Env(env)
        , Runtime(*Env.GetServer().GetRuntime())
        , QueryClient(NQuery::TQueryClient(Env.GetDriver()))
        , Session(QueryClient.GetSession().GetValueSync().GetSession())
    {
        CreateTier("tier1");
        CreateTier("tier2");
    }

    void WaitForCdcStreamReady(const std::string& streamPath) {
        for (int i = 0; i < 60; ++i) {
            auto pathDesc = DescribePath(Runtime, TString(streamPath));
            if (pathDesc.HasCdcStreamDescription()
                && pathDesc.GetCdcStreamDescription().GetState() == NKikimrSchemeOp::ECdcStreamStateReady)
            {
                return;
            }
            Sleep(TDuration::MilliSeconds(500));
        }
        UNIT_FAIL("Timed out waiting for CDC stream to become ready: " << streamPath);
    }

    std::string ShowCreateTable(NQuery::TSession& session, const std::string& tableName) {
        return ShowCreate(session, "TABLE", tableName);
    }

    void CheckShowCreateTable(const std::string& query, const std::string& tableName, TString formatQuery = "", bool temporary = false, bool initialScan = false) {
        auto session = QueryClient.GetSession().GetValueSync().GetSession();

        ExecuteQuery(session, query);
        auto showCreateTableQuery = ShowCreateTable(session, tableName);

        if (formatQuery) {
            UNIT_ASSERT_VALUES_EQUAL_C(UnescapeC(formatQuery), UnescapeC(showCreateTableQuery), UnescapeC(showCreateTableQuery));
        }

        if (initialScan) {
            return;
        }

        std::optional<TString> tempDir = std::nullopt;
        if (temporary) {
            auto res = Env.GetClient().Ls("/Root/.tmp/sessions");
            UNIT_ASSERT(res);
            UNIT_ASSERT(res->Record.HasPathDescription());
            UNIT_ASSERT(res->Record.GetPathDescription().ChildrenSize() == 1);
            tempDir = res->Record.GetPathDescription().GetChildren(0).GetName();
        }

        auto describeResultOrig = DescribeTable(tableName, tempDir);

        DropTable(session, tableName);

        ExecuteQuery(session, showCreateTableQuery);
        auto describeResultNew = DescribeTable(tableName, tempDir);

        DropTable(session, tableName);

        CompareDescriptions(describeResultOrig, describeResultNew, showCreateTableQuery);
    }

    // Checks that the view created from the description provided by the `SHOW CREATE VIEW` statement
    // can be used to create a view with a description equal to the original.
    void CheckShowCreateView(const std::string& query, const std::string& viewName, const std::string& expectedResult = "") {
        ExecuteQuery(Session, query);
        auto showCreateViewResult = ShowCreateView(Session, viewName);

        if (!expectedResult.empty()) {
            UNIT_ASSERT_STRINGS_EQUAL(UnescapeC(showCreateViewResult), UnescapeC(expectedResult));
        }

        const auto originalDescription = CanonizeViewDescription(DescribeView(viewName));

        DropView(Session, viewName);
        ExecuteQuery(Session, showCreateViewResult);

        const auto newDescription = CanonizeViewDescription(DescribeView(viewName));

        CompareDescriptions(originalDescription, newDescription, showCreateViewResult);
        DropView(Session, viewName);
    }

private:

    void CreateTier(const std::string& tierName) {
        ExecuteQuery(Session, std::format(R"(
            UPSERT OBJECT `accessKey` (TYPE SECRET) WITH (value = `secretAccessKey`);
            UPSERT OBJECT `secretKey` (TYPE SECRET) WITH (value = `fakeSecret`);
            CREATE EXTERNAL DATA SOURCE `{}` WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "http://fake.fake/olap-{}",
                AUTH_METHOD = "AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME = "accessKey",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME = "secretKey",
                AWS_REGION = "ru-central1"
            );
        )", tierName, tierName));
    }

    Ydb::Table::DescribeTableResult DescribeTable(const std::string& tableName, std::optional<TString> sessionId = std::nullopt) {

        auto describeTable = [this](TString&& path) {
            auto pathDescription = DescribePath(Runtime, std::move(path));

            if (pathDescription.HasColumnTableDescription()) {
                const auto& tableDescription = pathDescription.GetColumnTableDescription();
                return *GetScheme(tableDescription);
            }

            if (!pathDescription.HasTable()) {
                UNIT_FAIL("Invalid path type: " << pathDescription.GetSelf().GetPathType());
            }

            const auto& tableDescription = pathDescription.GetTable();
            return *GetScheme(tableDescription);
        };

        auto tablePath = TString(tableName);
        if (!IsStartWithSlash(tablePath)) {
            tablePath = CanonizePath(JoinPath({"/Root", tablePath}));
        }
        if (sessionId.has_value()) {
            tablePath = NKqp::GetTempTablePath("Root", sessionId.value(), tablePath);
        }
        auto tableDesc = describeTable(std::move(tablePath));

        return tableDesc;
    }

    NKikimrSchemeOp::TViewDescription DescribeView(const std::string& viewName) {
        auto pathDescription = DescribePath(Runtime, TString(viewName));
        UNIT_ASSERT_C(pathDescription.HasViewDescription(), pathDescription.DebugString());
        return pathDescription.GetViewDescription();
    }

    NKikimrSchemeOp::TViewDescription CanonizeViewDescription(NKikimrSchemeOp::TViewDescription&& description) {
        description.ClearVersion();
        description.ClearPathId();

        TString queryText;
        NYql::TIssues issues;
        UNIT_ASSERT_C(NDump::Format(description.GetQueryText(), queryText, issues), issues.ToString());
        *description.MutableQueryText() = queryText;

        return description;
    }

    std::string ShowCreate(NQuery::TSession& session, std::string_view type, const std::string& path) {
        const auto result = ExecuteQuery(session, std::format("SHOW CREATE {} `{}`;", type, path));

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        auto resultSet = result.GetResultSet(0);
        auto columnsMeta = resultSet.GetColumnsMeta();
        UNIT_ASSERT_VALUES_EQUAL(columnsMeta.size(), 3);

        TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());

        TString createQuery = "";

        for (const auto& column : columnsMeta) {
            TValueParser parserValue(parser.GetValue(column.Name));
            parserValue.OpenOptional();
            const auto& value = parserValue.GetUtf8();

            if (column.Name == "Path") {
                UNIT_ASSERT_VALUES_EQUAL(value, path);
            } else if (column.Name == "PathType") {
                auto actualType = to_upper(TString(value));
                UNIT_ASSERT_VALUES_EQUAL(actualType, type);
            } else if (column.Name == "CreateQuery") {
                createQuery = value;
            } else {
                UNIT_FAIL("Invalid column name: " << column.Name);
            }
        }
        UNIT_ASSERT(createQuery);

        return createQuery;
    }

    std::string ShowCreateView(NQuery::TSession& session, const std::string& viewName) {
        return ShowCreate(session, "VIEW", viewName);
    }

    void DropTable(NQuery::TSession& session, const std::string& tableName) {
        ExecuteQuery(session, std::format("DROP TABLE `{}`;", tableName));
    }

    void DropView(NQuery::TSession& session, const std::string& viewName) {
        ExecuteQuery(session, std::format("DROP VIEW `{}`;", viewName));
    }

    template <typename TProtobufDescription>
    void CompareDescriptions(const TProtobufDescription& describeResultOrig, const TProtobufDescription& describeResultNew, const std::string& showCreateTableQuery) {
        TString first;
        ::google::protobuf::TextFormat::PrintToString(describeResultOrig, &first);
        TString second;
        ::google::protobuf::TextFormat::PrintToString(describeResultNew, &second);

        UNIT_ASSERT_VALUES_EQUAL_C(first, second, showCreateTableQuery);
    }

    TMaybe<Ydb::Table::DescribeTableResult> GetScheme(const NKikimrSchemeOp::TTableDescription& tableDesc) {
        Ydb::Table::DescribeTableResult scheme;

        NKikimrMiniKQL::TType mkqlKeyType;

        try {
            FillColumnDescription(scheme, mkqlKeyType, tableDesc);
        } catch (const yexception&) {
            return Nothing();
        }

        scheme.mutable_primary_key()->CopyFrom(tableDesc.GetKeyColumnNames());

        try {
            FillTableBoundary(scheme, tableDesc, mkqlKeyType);
            FillIndexDescription(scheme, tableDesc);
        } catch (const yexception&) {
            return Nothing();
        }

        FillChangefeedDescription(scheme, tableDesc);

        FillStorageSettings(scheme, tableDesc);
        FillColumnFamilies(scheme, tableDesc);
        FillPartitioningSettings(scheme, tableDesc);
        FillKeyBloomFilter(scheme, tableDesc);
        FillReadReplicasSettings(scheme, tableDesc);

        TString error;
        Ydb::StatusIds::StatusCode status;
        if (!FillSequenceDescription(scheme, tableDesc, status, error)) {
            return Nothing();
        }

        return scheme;
    }

    TMaybe<Ydb::Table::DescribeTableResult> GetScheme(const NKikimrSchemeOp::TColumnTableDescription& tableDesc) {
        Ydb::Table::DescribeTableResult scheme;

        FillColumnDescription(scheme, tableDesc);
        FillColumnFamilies(scheme, tableDesc);

        return scheme;
    }

private:
    TTestEnv& Env;
    TTestActorRuntime& Runtime;
    NQuery::TQueryClient QueryClient;
    NQuery::TSession Session;
};

class TYsonFieldChecker {
    NYT::TNode Root;
    NYT::TNode::TListType::const_iterator RowIterator;

private:
    const NYT::TNode& ExtractOptional(const NYT::TNode& opt) {
        UNIT_ASSERT(opt.IsList());
        UNIT_ASSERT_VALUES_EQUAL(opt.AsList().size(), 1);
        return opt.AsList().front();
    };

public:
    TYsonFieldChecker(const TString& ysonString, size_t fieldCount) {
        Root = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
        UNIT_ASSERT(Root.IsList());
        UNIT_ASSERT_VALUES_EQUAL(Root.AsList().size(), 1);

        const auto& rowNode = Root.AsList().front();
        UNIT_ASSERT(rowNode.IsList());

        const auto& row = rowNode.AsList();
        UNIT_ASSERT_VALUES_EQUAL(row.size(), fieldCount);

        RowIterator = row.begin();
    }

    bool SkipNull() {
        if (RowIterator->IsNull()) {
            ++RowIterator;
            return true;
        } else {
            return false;
        }
    }

    void Null() {
        const auto& value = *RowIterator++;
        UNIT_ASSERT(value.IsNull());
    }

    void Bool(bool expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsBool());
        UNIT_ASSERT_VALUES_EQUAL(value.AsBool(), expected);
    }

    void Uint64(ui64 expected, bool orNull = false) {
        if (!orNull || !SkipNull()) {
            const auto& value = ExtractOptional(*RowIterator++);
            UNIT_ASSERT(value.IsUint64());
            UNIT_ASSERT_VALUES_EQUAL(value.AsUint64(), expected);
        }
    }

    void Uint64Greater(ui64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsUint64());
        UNIT_ASSERT_GT(value.AsUint64(), expected);
    }

    void Uint64GreaterOrEquals(ui64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsUint64());
        UNIT_ASSERT_GE(value.AsUint64(), expected);
    }

    void Uint64LessOrEquals(ui64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsUint64());
        UNIT_ASSERT_LE(value.AsUint64(), expected);
    }

    void Int64(i64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsInt64());
        UNIT_ASSERT_VALUES_EQUAL(value.AsInt64(), expected);
    }

    void Int64Greater(i64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsInt64());
        UNIT_ASSERT_GT(value.AsInt64(), expected);
    }

    void Int64GreaterOrEquals(i64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsInt64());
        UNIT_ASSERT_GE(value.AsInt64(), expected);
    }

    void Double(double expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsDouble());
        UNIT_ASSERT_VALUES_EQUAL(value.AsDouble(), expected);
    }

    void DoubleGreaterOrEquals(double expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsDouble());
        UNIT_ASSERT_GE(value.AsDouble(), expected);
    }

    void String(const TString& expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsString());
        UNIT_ASSERT_STRINGS_EQUAL(value.AsString(), expected);
    }

    void StringContains(const TString& substr) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsString());
        UNIT_ASSERT(value.AsString().Contains(substr));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(SystemView) {

    Y_UNIT_TEST(PartitionStatsOneSchemeShard) {
        TTestEnv env;
        CreateTenantsAndTables(env, true);
        auto describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "Root/Table0");
        const auto table0PathId = describeResult.GetPathId();

        describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "Root/Tenant1/Table1");
        const auto table1PathId = describeResult.GetPathId();

        describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "Root/Tenant2/Table2");
        const auto table2PathId = describeResult.GetPathId();

        TTableClient client(env.GetDriver());
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Table0"]]
            ])", table0PathId), NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant1/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Tenant1/Table1"]]
            ])", table1PathId), NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant2/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Tenant2/Table2"]]
            ])", table2PathId), NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(PartitionStatsOneSchemeShardDataQuery) {
        TTestEnv env;
        CreateTenantsAndTables(env, true);
        auto describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "Root/Table0");
        const auto table0PathId = describeResult.GetPathId();

        describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "Root/Tenant1/Table1");
        const auto table1PathId = describeResult.GetPathId();

        describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "Root/Tenant2/Table2");
        const auto table2PathId = describeResult.GetPathId();

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Table0"]]
            ])", table0PathId), FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant1/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT(result.IsSuccess());
            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Tenant1/Table1"]]
            ])", table1PathId), FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant2/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT(result.IsSuccess());
            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Tenant2/Table2"]]
            ])", table2PathId), FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PgTablesOneSchemeShardDataQuery) {
        TTestEnv env;
        CreateRootTable(env, 1, false, 0);
        CreateRootTable(env, 2, false, 1);

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity FROM `Root/.sys/pg_tables` WHERE tablename = PgName("Table0") OR tablename = PgName("Table1") ORDER BY tablename;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NKqp::CompareYson(R"([
                ["public";"Table0";"root@builtin";#;"t";"f";"f";"f"];
                ["public";"Table1";"root@builtin";#;"t";"f";"f";"f"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ShowCreateTableDefaultLiteral) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Bool DEFAULT true,
                PRIMARY KEY (Key)
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint32,
    `Value` Bool DEFAULT TRUE,
    PRIMARY KEY (`Key`)
);
)"
        );

        checker.CheckShowCreateTable(
            R"(CREATE TABLE `/Root/test_show_create` (
                Key Uint32 DEFAULT 1,
                Value Int32 DEFAULT -100,
                PRIMARY KEY (Key)
            );
            )", "test_show_create"
        );

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint64 DEFAULT 100,
                Value Int64 DEFAULT -100,
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Double DEFAULT 0.5,
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Float DEFAULT CAST(4.0 AS FLOAT),
                PRIMARY KEY (Key)
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint32,
    `Value` Float DEFAULT 4,
    PRIMARY KEY (`Key`)
);
)"
        );

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Double DEFAULT 0.075,
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Date DEFAULT CAST('2000-01-02' as DATE),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Datetime DEFAULT CAST('2000-01-02T02:26:51Z' as DATETIME),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Timestamp DEFAULT CAST('2000-01-02T02:26:50.999900Z' as TIMESTAMP),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Uuid DEFAULT Uuid("afcbef30-9ac3-481a-aa6a-8d9b785dbb0a"),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Json DEFAULT "[12]",
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Yson DEFAULT "[13]",
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value String DEFAULT "string",
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Utf8 DEFAULT "utf8",
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Interval DEFAULT Interval("P10D"),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Date32 DEFAULT Date32('1970-01-05'),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Datetime64 DEFAULT Datetime64('1970-01-01T00:00:00Z'),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Timestamp64 DEFAULT Timestamp64('1970-01-01T00:00:00Z'),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Interval64 DEFAULT Interval64('P222D'),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Decimal(22, 15) DEFAULT CAST("11.11" AS Decimal(22, 15)),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Decimal(35, 10) DEFAULT CAST("110.111" AS Decimal(35, 10)),
                PRIMARY KEY (Key)
            );
        )", "test_show_create");
    }

    Y_UNIT_TEST(ShowCreateTablePartitionAtKeys) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Uint64,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = ((10), (100, "123"), (1000, "cde"))
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Uint64,
                Key2 String,
                Key3 Utf8,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = (10)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Uint64,
                Key2 String,
                Key3 Utf8,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = (10, 20, 30)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Uint64,
                Key2 String,
                Key3 Utf8,
                PRIMARY KEY (Key1, Key2, Key3)
            )
            WITH (
                PARTITION_AT_KEYS = ((10, "str"), (10, "str", "utf"))
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                BoolValue Bool,
                Int32Value Int32,
                Uint32Value Uint32,
                Int64Value Int64,
                Uint64Value Uint64,
                StringValue String,
                Utf8Value Utf8,
                Value1 Int32 Family family1,
                Value2 Int64 Family family1,
                FAMILY family1 (),
                PRIMARY KEY (BoolValue, Int32Value, Uint32Value, Int64Value, Uint64Value, StringValue, Utf8Value)
            ) WITH (
                PARTITION_AT_KEYS = ((false), (false, 1, 2), (true, 1, 1, 1, 1, "str"), (true, 1, 1, 100, 0, "str", "utf"))
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `BoolValue` Bool,
    `Int32Value` Int32,
    `Uint32Value` Uint32,
    `Int64Value` Int64,
    `Uint64Value` Uint64,
    `StringValue` String,
    `Utf8Value` Utf8,
    `Value1` Int32 FAMILY `family1`,
    `Value2` Int64 FAMILY `family1`,
    FAMILY `family1` (),
    PRIMARY KEY (`BoolValue`, `Int32Value`, `Uint32Value`, `Int64Value`, `Uint64Value`, `StringValue`, `Utf8Value`)
)
WITH (PARTITION_AT_KEYS = ((FALSE), (FALSE, 1, 2), (TRUE, 1, 1, 1, 1, 'str'), (TRUE, 1, 1, 100, 0, 'str', 'utf')));
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTablePartitionByHash) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Uint64 NOT NULL,
                Key2 String NOT NULL,
                Value String,
                PRIMARY KEY (Key1, Key2)
            )
            PARTITION BY HASH(Key1, Key2)
            WITH (
                STORE = COLUMN
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Uint64 NOT NULL,
    `Key2` String NOT NULL,
    `Value` String,
    PRIMARY KEY (`Key1`, `Key2`)
)
PARTITION BY HASH (`Key1`, `Key2`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64
);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableColumn) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .EnableOlapCompression = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Uint64 NOT NULL,
                Key2 Utf8 NOT NULL COMPRESSION (),
                Key3 Int32 NOT NULL COMPRESSION (algorithm = off),
                Value1 Utf8 COMPRESSION (algorithm = lz4),
                Value2 Int16 COMPRESSION (algorithm = zstd),
                Value3 String COMPRESSION (algorithm = zstd, level = 10),
                PRIMARY KEY (Key1, Key2, Key3),
            )
            PARTITION BY HASH(`Key1`, `Key2`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100,
                TTL =
                    Interval("PT10S") TO EXTERNAL DATA SOURCE `/Root/tier1`,
                    Interval("PT1H") DELETE
                    ON Key1 AS SECONDS
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Uint64 NOT NULL,
    `Key2` Utf8 NOT NULL,
    `Key3` Int32 NOT NULL COMPRESSION (algorithm = off),
    `Value1` Utf8 COMPRESSION (algorithm = lz4),
    `Value2` Int16 COMPRESSION (algorithm = zstd, level = 1),
    `Value3` String COMPRESSION (algorithm = zstd, level = 10),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
)
PARTITION BY HASH (`Key1`, `Key2`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100,
    TTL =
        INTERVAL('PT10S') TO EXTERNAL DATA SOURCE `/Root/tier1`,
        INTERVAL('PT1H') DELETE
    ON Key1 AS SECONDS
);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTablePartitionSettings) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value1 String NOT NULL,
                Value2 Int32 NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                UNIFORM_PARTITIONS = 10,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );
        )", "test_show_create");
    }

    Y_UNIT_TEST(ShowCreateTableReadReplicas) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                READ_REPLICAS_SETTINGS = "PER_AZ:2"
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                READ_REPLICAS_SETTINGS = "ANY_AZ:3"
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint64 NOT NULL,
    `Value` String NOT NULL,
    PRIMARY KEY (`Key`)
)
WITH (READ_REPLICAS_SETTINGS = 'ANY_AZ:3');
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableKeyBloomFilter) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                KEY_BLOOM_FILTER = ENABLED
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                KEY_BLOOM_FILTER = DISABLED
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint64 NOT NULL,
    `Value` String NOT NULL,
    PRIMARY KEY (`Key`)
)
WITH (KEY_BLOOM_FILTER = DISABLED);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableTtlSettings) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Timestamp NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                TTL = Interval("P1D") DELETE ON Key
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                TTL =
                    Interval("PT1H") DELETE ON Key AS SECONDS
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint32 NOT NULL,
    PRIMARY KEY (`Key`)
)
WITH (TTL = INTERVAL('PT1H') DELETE ON Key AS SECONDS);
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(`Key`)
            WITH (
                STORE = COLUMN,
                TTL = INTERVAL('PT1H') DELETE ON Key AS MILLISECONDS
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(`Key`)
            WITH (
                STORE = COLUMN,
                TTL =
                    INTERVAL('PT1H') TO EXTERNAL DATA SOURCE `/Root/tier2`,
                    INTERVAL('PT3H') DELETE
                ON Key AS NANOSECONDS
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(`Key`)
            WITH (
                STORE = COLUMN,
                TTL = INTERVAL('PT1H') TO EXTERNAL DATA SOURCE `/Root/tier2` ON Key AS MICROSECONDS
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Timestamp NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(`Key`)
            WITH (
                STORE = COLUMN,
                TTL =
                    Interval("PT10S") TO EXTERNAL DATA SOURCE `/Root/tier1`,
                    Interval("PT1M") TO EXTERNAL DATA SOURCE `/Root/tier2`,
                    Interval("PT1H") DELETE
                    ON Key
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Timestamp NOT NULL,
    `Value` String,
    PRIMARY KEY (`Key`)
)
PARTITION BY HASH (`Key`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64,
    TTL =
        INTERVAL('PT10S') TO EXTERNAL DATA SOURCE `/Root/tier1`,
        INTERVAL('PT1M') TO EXTERNAL DATA SOURCE `/Root/tier2`,
        INTERVAL('PT1H') DELETE
    ON Key
);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableTemporary) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TEMPORARY TABLE test_show_create (
                Key Int32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
        )", "test_show_create",
R"(CREATE TEMPORARY TABLE `test_show_create` (
    `Key` Int32 NOT NULL,
    `Value` String,
    PRIMARY KEY (`Key`)
);
)"
        , true);
    }

    Y_UNIT_TEST(ShowCreateTable) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .EnableFulltextIndex = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(
            R"(CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            );
            )", "test_show_create"
        );

        checker.CheckShowCreateTable(
            R"(CREATE TABLE test_show_create (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL,
                Key2 Utf8 NOT NULL,
                Key3 PgInt2 NOT NULL,
                Value1 Utf8,
                Value2 Bool,
                Value3 String,
                PRIMARY KEY (Key1, Key2, Key3),
                INDEX Index1 GLOBAL USING vector_kmeans_tree ON (`Value3`) WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2)
            );
            ALTER TABLE test_show_create ADD INDEX Index2 GLOBAL SYNC ON (Key2, Value1, Value2);
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key),
                INDEX fulltext_idx GLOBAL USING fulltext_plain ON (Text) WITH (tokenizer=standard, use_filter_lowercase=true, use_filter_length=true, filter_length_min=3)
            );
            ALTER TABLE test_show_create ADD INDEX Index2 GLOBAL SYNC ON (Data);
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                BoolValue Bool,
                Int32Value Int32,
                Uint32Value Uint32,
                Int64Value Int64,
                Uint64Value Uint64,
                FloatValue Float,
                DoubleValue Double,
                StringValue String,
                Utf8Value Utf8,
                DateValue Date,
                DatetimeValue Datetime,
                TimestampValue Timestamp,
                IntervalValue Interval,
                DecimalValue1 Decimal(22,9),
                DecimalValue2 Decimal(35,10),
                JsonValue Json,
                YsonValue Yson,
                JsonDocumentValue JsonDocument,
                DyNumberValue DyNumber,
                Int32NotNullValue Int32 NOT NULL,
                PRIMARY KEY (Key)
            );
        )", "test_show_create");

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL DEFAULT -100,
                Key2 Utf8 NOT NULL,
                Key3 BigSerial NOT NULL,
                Value1 Utf8 FAMILY Family1,
                Value2 Bool FAMILY Family2,
                Value3 String FAMILY Family2,
                INDEX Index1 GLOBAL USING vector_kmeans_tree ON (`Value3`) WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2),
                PRIMARY KEY (Key1, Key2, Key3),
                FAMILY Family1 (
                    DATA = "test0",
                    COMPRESSION = "off"
                ),
                FAMILY Family2 (
                    DATA = "test1",
                    COMPRESSION = "lz4"
                )
            ) WITH (
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
            );
            ALTER TABLE test_show_create ADD INDEX Index2 GLOBAL ASYNC ON (Key2, Value1, Value2);
            ALTER TABLE test_show_create ADD INDEX Index3 GLOBAL ASYNC ON (Key3, Value2) COVER (Value1, Value3);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Int64 NOT NULL DEFAULT -100,
    `Key2` Utf8 NOT NULL,
    `Key3` Serial8 NOT NULL,
    `Value1` Utf8 FAMILY `Family1`,
    `Value2` Bool FAMILY `Family2`,
    `Value3` String FAMILY `Family2`,
    INDEX `Index1` GLOBAL USING vector_kmeans_tree ON (`Value3`) WITH (distance = cosine, vector_type = 'uint8', vector_dimension = 2, clusters = 2, levels = 1),
    INDEX `Index2` GLOBAL ASYNC ON (`Key2`, `Value1`, `Value2`),
    INDEX `Index3` GLOBAL ASYNC ON (`Key3`, `Value2`) COVER (`Value1`, `Value3`),
    FAMILY `Family1` (DATA = 'test0', COMPRESSION = 'off'),
    FAMILY `Family2` (DATA = 'test1', COMPRESSION = 'lz4'),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
);
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL DEFAULT -100,
                Key2 Utf8 NOT NULL,
                Key3 BigSerial NOT NULL,
                Value1 Utf8,
                Value2 Bool,
                Value3 STRING,
                Value4 Timestamp DEFAULT CAST('2000-01-02T02:26:50.999900Z' as TIMESTAMP),
                Value5 String,
                INDEX Index2 GLOBAL USING vector_kmeans_tree ON (Value5) COVER (Value1, Value3) WITH (distance=manhattan, vector_type=float, vector_dimension=2, clusters=2, levels=1),
                PRIMARY KEY (Key1, Key2, Key3),
            ) WITH (
                TTL = Interval("PT1H") DELETE ON Value4,
                KEY_BLOOM_FILTER = ENABLED,
                PARTITION_AT_KEYS = ((10), (100, "123"), (1000, "cde")),
                AUTO_PARTITIONING_BY_LOAD = ENABLED
            );
            ALTER TABLE test_show_create ADD INDEX Index1 GLOBAL ASYNC ON (Key2, Value1, Value2) COVER (Value5, Value3);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Int64 NOT NULL DEFAULT -100,
    `Key2` Utf8 NOT NULL,
    `Key3` Serial8 NOT NULL,
    `Value1` Utf8,
    `Value2` Bool,
    `Value3` String,
    `Value4` Timestamp DEFAULT TIMESTAMP('2000-01-02T02:26:50.999900Z'),
    `Value5` String,
    INDEX `Index1` GLOBAL ASYNC ON (`Key2`, `Value1`, `Value2`) COVER (`Value5`, `Value3`),
    INDEX `Index2` GLOBAL USING vector_kmeans_tree ON (`Value5`) COVER (`Value1`, `Value3`) WITH (distance = manhattan, vector_type = 'float', vector_dimension = 2, clusters = 2, levels = 1),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    PARTITION_AT_KEYS = ((10), (100, '123'), (1000, 'cde')),
    KEY_BLOOM_FILTER = ENABLED,
    TTL = INTERVAL('PT1H') DELETE ON Value4
);
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Uint32,
                Key2 BigSerial,
                Key3 SmallSerial,
                Value1 Serial,
                Value2 String,
                PRIMARY KEY (Key1, Key2, Key3)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = Interval("PT1H"));
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 10, RETENTION_PERIOD = Interval("PT3H"), VIRTUAL_TIMESTAMPS = TRUE);
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', TOPIC_MIN_ACTIVE_PARTITIONS = 3, FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT30M"));
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                START WITH 150
                INCREMENT BY 300;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                INCREMENT 1;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key3`
                RESTART WITH 5;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                START WITH 101;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                INCREMENT 404
                RESTART;
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Uint32,
    `Key2` Serial8 NOT NULL,
    `Key3` Serial2 NOT NULL,
    `Value1` Serial4 NOT NULL,
    `Value2` String,
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
);

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = INTERVAL('PT1H'), TOPIC_MIN_ACTIVE_PARTITIONS = 1)
;

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE, RETENTION_PERIOD = INTERVAL('PT3H'), TOPIC_MIN_ACTIVE_PARTITIONS = 10)
;

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT30M'), TOPIC_MIN_ACTIVE_PARTITIONS = 3)
;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key2` START WITH 150;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key3` RESTART WITH 5;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Value1` START WITH 101 INCREMENT BY 404 RESTART;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 BigSerial,
                Key2 SmallSerial,
                Value1 Serial,
                Value2 String,
                PRIMARY KEY (Key1, Key2)
            ) WITH (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                PARTITION_AT_KEYS = ((10), (100, 1000), (1000, 20))
            );
            ALTER TABLE test_show_create ADD CHANGEFEED `feed1` WITH (
                MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT1H")
            );
            ALTER TABLE test_show_create ADD CHANGEFEED `feed2` WITH (
                MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT2H")
            );
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                START WITH 150
                INCREMENT BY 300;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                INCREMENT 1;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                RESTART WITH 5;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                START WITH 101;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                INCREMENT 404
                RESTART;
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Serial8 NOT NULL,
    `Key2` Serial2 NOT NULL,
    `Value1` Serial4 NOT NULL,
    `Value2` String,
    PRIMARY KEY (`Key1`, `Key2`)
)
WITH (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    PARTITION_AT_KEYS = ((10), (100, 1000), (1000, 20))
);

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed1` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT1H'))
;

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed2` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT2H'))
;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key1` START WITH 150;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key2` RESTART WITH 5;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Value1` START WITH 101 INCREMENT BY 404 RESTART;
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableChangefeeds) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create ADD CHANGEFEED `feed` WITH (
                MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT1H")
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint64,
    `Value` String,
    PRIMARY KEY (`Key`)
);

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT1H'), TOPIC_MIN_ACTIVE_PARTITIONS = 1)
;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = Interval("PT1H"));
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 10, RETENTION_PERIOD = Interval("PT3H"), VIRTUAL_TIMESTAMPS = TRUE);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint64,
    `Value` String,
    PRIMARY KEY (`Key`)
);

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = INTERVAL('PT1H'), TOPIC_MIN_ACTIVE_PARTITIONS = 1)
;

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE, RETENTION_PERIOD = INTERVAL('PT3H'), TOPIC_MIN_ACTIVE_PARTITIONS = 10)
;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key String,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON');
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` String,
    `Value` String,
    PRIMARY KEY (`Key`)
);

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('P1D'))
;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key String,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', SCHEMA_CHANGES = TRUE);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` String,
    `Value` String,
    PRIMARY KEY (`Key`)
);

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', SCHEMA_CHANGES = TRUE, RETENTION_PERIOD = INTERVAL('P1D'))
;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = Interval("PT1H"));
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 10, RETENTION_PERIOD = Interval("PT3H"), VIRTUAL_TIMESTAMPS = TRUE);
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', TOPIC_MIN_ACTIVE_PARTITIONS = 3, FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT30M"), INITIAL_SCAN = TRUE);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint64,
    `Value` String,
    PRIMARY KEY (`Key`)
);

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = INTERVAL('PT1H'), TOPIC_MIN_ACTIVE_PARTITIONS = 1)
;

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE, RETENTION_PERIOD = INTERVAL('PT3H'), TOPIC_MIN_ACTIVE_PARTITIONS = 10)
;

ALTER TABLE `test_show_create`
    ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT30M'), TOPIC_MIN_ACTIVE_PARTITIONS = 3, INITIAL_SCAN = TRUE)
;
)"
        , false, true
        );
    }

    Y_UNIT_TEST(ShowCreateTableChangefeedAfterInitialScan) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        auto session = NQuery::TQueryClient(env.GetDriver()).GetSession().GetValueSync().GetSession();

        ExecuteQuery(session, R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT30M"), INITIAL_SCAN = TRUE);
        )");

        // Wait for the initial scan to complete (state transitions from ECdcStreamStateScan to ECdcStreamStateReady)
        checker.WaitForCdcStreamReady("/Root/test_show_create/feed");

        // After the scan is complete, SHOW CREATE TABLE must still include INITIAL_SCAN = TRUE
        auto showCreateTableQuery = checker.ShowCreateTable(session, "test_show_create");
        UNIT_ASSERT_C(showCreateTableQuery.contains("INITIAL_SCAN = TRUE"),
            "INITIAL_SCAN = TRUE must be present in SHOW CREATE TABLE output after initial scan completes: "
            << showCreateTableQuery);

        ExecuteQuery(session, "DROP TABLE `test_show_create`;");
    }

    Y_UNIT_TEST(ShowCreateTableSequences) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key Serial,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key`
                START 50
                INCREMENT BY 11;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key`
                RESTART;
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Serial4 NOT NULL,
    `Value` String,
    PRIMARY KEY (`Key`)
);

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key` START WITH 50 INCREMENT BY 11 RESTART;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 BigSerial,
                Key2 SmallSerial,
                Value String,
                PRIMARY KEY (Key1, Key2)
            );
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                START WITH 50
                INCREMENT BY 11;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                RESTART WITH 5;
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Serial8 NOT NULL,
    `Key2` Serial2 NOT NULL,
    `Value` String,
    PRIMARY KEY (`Key1`, `Key2`)
);

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key1` START WITH 50 INCREMENT BY 11;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key2` RESTART WITH 5;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 BigSerial,
                Key2 SmallSerial,
                Value1 Serial,
                Value2 String,
                PRIMARY KEY (Key1, Key2)
            );
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                START WITH 150
                INCREMENT BY 300;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                INCREMENT 1;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                RESTART WITH 5;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                START WITH 101;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                INCREMENT 404
                RESTART;
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Serial8 NOT NULL,
    `Key2` Serial2 NOT NULL,
    `Value1` Serial4 NOT NULL,
    `Value2` String,
    PRIMARY KEY (`Key1`, `Key2`)
);

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key1` START WITH 150;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key2` RESTART WITH 5;

ALTER SEQUENCE `/Root/test_show_create/_serial_column_Value1` START WITH 101 INCREMENT BY 404 RESTART;
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTablePartitionPolicyIndexTable) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL,
                Key2 Utf8 NOT NULL,
                Key3 PgInt2 NOT NULL,
                Value1 Utf8,
                Value2 Bool,
                Value3 String,
                INDEX Index1 GLOBAL SYNC ON (Key2, Value1, Value2),
                PRIMARY KEY (Key1, Key2, Key3)
            );
            ALTER TABLE test_show_create ALTER INDEX Index1 SET (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Int64 NOT NULL,
    `Key2` Utf8 NOT NULL,
    `Key3` pgint2 NOT NULL,
    `Value1` Utf8,
    `Value2` Bool,
    `Value3` String,
    INDEX `Index1` GLOBAL SYNC ON (`Key2`, `Value1`, `Value2`),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
);

ALTER TABLE `test_show_create`
    ALTER INDEX `Index1` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000)
;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL DEFAULT -100,
                Key2 Utf8 NOT NULL,
                Key3 BigSerial NOT NULL,
                Value1 Utf8 FAMILY Family1,
                Value2 Bool FAMILY Family2,
                Value3 String FAMILY Family2,
                INDEX Index1 GLOBAL ASYNC ON (Key2, Value1, Value2),
                INDEX Index2 GLOBAL ASYNC ON (Key3, Value2) COVER (Value1, Value3),
                PRIMARY KEY (Key1, Key2, Key3),
                FAMILY Family1 (
                    DATA = "test0",
                    COMPRESSION = "off"
                ),
                FAMILY Family2 (
                    DATA = "test1",
                    COMPRESSION = "lz4"
                )
            ) WITH (
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
            );
            ALTER TABLE test_show_create ALTER INDEX Index1 SET (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2000
            );
            ALTER TABLE test_show_create ALTER INDEX Index2 SET (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Int64 NOT NULL DEFAULT -100,
    `Key2` Utf8 NOT NULL,
    `Key3` Serial8 NOT NULL,
    `Value1` Utf8 FAMILY `Family1`,
    `Value2` Bool FAMILY `Family2`,
    `Value3` String FAMILY `Family2`,
    INDEX `Index1` GLOBAL ASYNC ON (`Key2`, `Value1`, `Value2`),
    INDEX `Index2` GLOBAL ASYNC ON (`Key3`, `Value2`) COVER (`Value1`, `Value3`),
    FAMILY `Family1` (DATA = 'test0', COMPRESSION = 'off'),
    FAMILY `Family2` (DATA = 'test1', COMPRESSION = 'lz4'),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
);

ALTER TABLE `test_show_create`
    ALTER INDEX `Index1` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2000)
;

ALTER TABLE `test_show_create`
    ALTER INDEX `Index2` SET (AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100)
;
)"
        );

        checker.CheckShowCreateTable(R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL,
                Key2 Utf8 NOT NULL,
                Key3 PgInt2 NOT NULL,
                Value1 Utf8,
                Value2 Bool,
                Value3 String,
                INDEX Index1 GLOBAL SYNC ON (Key2, Value1, Value2),
                INDEX Index2 GLOBAL ASYNC ON (Key3, Value1) COVER (Value2, Value3),
                INDEX Index3 GLOBAL SYNC ON (Key1, Key2, Value1),
                PRIMARY KEY (Key1, Key2, Key3)
            );
            ALTER TABLE test_show_create ALTER INDEX Index1 SET (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000
            );
            ALTER TABLE test_show_create ALTER INDEX Index2 SET (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 10000,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 2700
            );
            ALTER TABLE test_show_create ALTER INDEX Index3 SET (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3500
            );
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key1` Int64 NOT NULL,
    `Key2` Utf8 NOT NULL,
    `Key3` pgint2 NOT NULL,
    `Value1` Utf8,
    `Value2` Bool,
    `Value3` String,
    INDEX `Index1` GLOBAL SYNC ON (`Key2`, `Value1`, `Value2`),
    INDEX `Index2` GLOBAL ASYNC ON (`Key3`, `Value1`) COVER (`Value2`, `Value3`),
    INDEX `Index3` GLOBAL SYNC ON (`Key1`, `Key2`, `Value1`),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
);

ALTER TABLE `test_show_create`
    ALTER INDEX `Index1` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000)
;

ALTER TABLE `test_show_create`
    ALTER INDEX `Index2` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 10000, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 2700)
;

ALTER TABLE `test_show_create`
    ALTER INDEX `Index3` SET (AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3500)
;
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableColumnAlterColumn) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .AlterObjectEnabled = true, .EnableSparsedColumns = true, .EnableOlapCompression = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE `/Root/test_show_create` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                Col3 Uint32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `FORCE_SIMD_PARSING`=`true`, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0.5`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `ENCODING.DICTIONARY.ENABLED`=`true`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col3, `DEFAULT_VALUE`=`5`);
            ALTER TABLE `/Root/test_show_create` ALTER COLUMN Col2 SET COMPRESSION (algorithm=zstd, level=4);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Col1` Uint64 NOT NULL,
    `Col2` JsonDocument COMPRESSION (algorithm = zstd, level = 4),
    `Col3` Uint32,
    PRIMARY KEY (`Col1`)
)
PARTITION BY HASH (`Col1`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = ALTER_COLUMN, NAME = Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME` = `SUB_COLUMNS`, `SPARSED_DETECTOR_KFF` = `20`, `COLUMNS_LIMIT` = `1024`, `MEM_LIMIT_CHUNK` = `52428800`, `OTHERS_ALLOWED_FRACTION` = `0.5`, `DATA_EXTRACTOR_CLASS_NAME` = `JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY` = `false`, `FORCE_SIMD_PARSING` = `true`, `ENCODING.DICTIONARY.ENABLED` = `true`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = ALTER_COLUMN, NAME = Col3, `DEFAULT_VALUE` = `5`);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableColumnUpsertOptions) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .AlterObjectEnabled = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE `/Root/test_show_create` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                `COMPACTION_PLANNER.FEATURES`=`{"levels" : [{"class_name" : "Zero", "portions_live_duration" : "5s", "expected_blobs_size" : 1000000000000, "portions_count_available" : 2},
                                {"class_name" : "Zero"}]}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `METADATA_MEMORY_MANAGER.CLASS_NAME`=`local_db`,
                    `METADATA_MEMORY_MANAGER.FEATURES`=`{"memory_cache_size" : 0}`);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Col1` Uint64 NOT NULL,
    `Col2` JsonDocument,
    PRIMARY KEY (`Col1`)
)
PARTITION BY HASH (`Col1`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME` = 'SIMPLE', `COMPACTION_PLANNER.CLASS_NAME` = 'lc-buckets', `COMPACTION_PLANNER.FEATURES` = `{"levels":[{"portions_count_available":2,"portions_live_duration":"5.000000s","class_name":"Zero","expected_blobs_size":1000000000000},{"class_name":"Zero"}]}`, `METADATA_MEMORY_MANAGER.CLASS_NAME` = 'local_db', `METADATA_MEMORY_MANAGER.FEATURES` = `{"memory_cache_size":0,"fetch_on_start":false}`);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableColumnUpsertIndex) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .AlterObjectEnabled = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE `/Root/test_show_create` (
                Col1 Uint64 NOT NULL,
                Col2 Uint32 NOT NULL,
                Col3 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=count_min_sketch_index, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['Col2']}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=bloom_ngramm_filter_index, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "Col3", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                           "records_count" : 1024, "case_sensitive" : false, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : '"b.c.d"'}}`);
            ALTER OBJECT `Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=bloom_filter_index, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01, "bits_storage_type": "BITSET"}`);
            ALTER OBJECT `Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=max_index, TYPE=MAX, FEATURES=`{"column_name": "Col2"}`);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Col1` Uint64 NOT NULL,
    `Col2` Uint32 NOT NULL,
    `Col3` JsonDocument,
    PRIMARY KEY (`Col1`)
)
PARTITION BY HASH (`Col1`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = max_index, TYPE = MAX, FEATURES = `{"column_name":"Col2"}`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = count_min_sketch_index, TYPE = COUNT_MIN_SKETCH, FEATURES = `{"column_names":["Col2"]}`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = bloom_ngramm_filter_index, TYPE = BLOOM_NGRAMM_FILTER, FEATURES = `{"bits_storage_type":"SIMPLE_STRING","records_count":1024,"case_sensitive":false,"ngramm_size":3,"filter_size_bytes":4096,"data_extractor":{"class_name":"SUB_COLUMN","sub_column_name":"\\\"b.c.d\\\""},"hashes_count":2,"column_name":"Col3"}`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = bloom_filter_index, TYPE = BLOOM_FILTER, FEATURES = `{"false_positive_probability":0.01,"data_extractor":{"class_name":"DEFAULT"},"bits_storage_type":"BITSET","column_name":"Col2"}`);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableColumnAlterObject) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .AlterObjectEnabled = true, .EnableSparsedColumns = true, .EnableOlapCompression = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(R"(
            CREATE TABLE `/Root/test_show_create` (
                Col1 Uint64 NOT NULL,
                Col2 Uint32 NOT NULL,
                Col3 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=count_min_sketch_index, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['Col2']}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=bloom_ngramm_filter_index, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                           "records_count" : 1024, "case_sensitive" : true, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : "a"}}`);
            ALTER OBJECT `Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=bloom_filter_index, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`);
            ALTER OBJECT `Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=max_index, TYPE=MAX, FEATURES=`{"column_name": "Col2"}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                `COMPACTION_PLANNER.FEATURES`=`{"levels" : [{"class_name" : "Zero", "portions_live_duration" : "180s", "expected_blobs_size" : 2048000},
                               {"class_name" : "Zero", "expected_blobs_size" : 2048000}, {"class_name" : "Zero"}]}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `METADATA_MEMORY_MANAGER.CLASS_NAME`=`local_db`,
                    `METADATA_MEMORY_MANAGER.FEATURES`=`{"memory_cache_size" : 0}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col3, `FORCE_SIMD_PARSING`=`true`, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0.5`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col3, `ENCODING.DICTIONARY.ENABLED`=`true`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DEFAULT_VALUE`=`100`);
        )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Col1` Uint64 NOT NULL,
    `Col2` Uint32 NOT NULL,
    `Col3` JsonDocument,
    PRIMARY KEY (`Col1`)
)
PARTITION BY HASH (`Col1`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = ALTER_COLUMN, NAME = Col2, `DEFAULT_VALUE` = `100`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = ALTER_COLUMN, NAME = Col3, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME` = `SUB_COLUMNS`, `SPARSED_DETECTOR_KFF` = `20`, `COLUMNS_LIMIT` = `1024`, `MEM_LIMIT_CHUNK` = `52428800`, `OTHERS_ALLOWED_FRACTION` = `0.5`, `DATA_EXTRACTOR_CLASS_NAME` = `JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY` = `false`, `FORCE_SIMD_PARSING` = `true`, `ENCODING.DICTIONARY.ENABLED` = `true`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = max_index, TYPE = MAX, FEATURES = `{"column_name":"Col2"}`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = count_min_sketch_index, TYPE = COUNT_MIN_SKETCH, FEATURES = `{"column_names":["Col2"]}`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = bloom_ngramm_filter_index, TYPE = BLOOM_NGRAMM_FILTER, FEATURES = `{"bits_storage_type":"SIMPLE_STRING","records_count":1024,"case_sensitive":true,"ngramm_size":3,"filter_size_bytes":4096,"data_extractor":{"class_name":"SUB_COLUMN","sub_column_name":"a"},"hashes_count":2,"column_name":"Col2"}`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = bloom_filter_index, TYPE = BLOOM_FILTER, FEATURES = `{"false_positive_probability":0.01,"data_extractor":{"class_name":"DEFAULT"},"bits_storage_type":"SIMPLE_STRING","column_name":"Col2"}`);

ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME` = 'SIMPLE', `COMPACTION_PLANNER.CLASS_NAME` = 'lc-buckets', `COMPACTION_PLANNER.FEATURES` = `{"levels":[{"portions_live_duration":"180.000000s","class_name":"Zero","expected_blobs_size":2048000},{"class_name":"Zero","expected_blobs_size":2048000},{"class_name":"Zero"}]}`, `METADATA_MEMORY_MANAGER.CLASS_NAME` = 'local_db', `METADATA_MEMORY_MANAGER.FEATURES` = `{"memory_cache_size":0,"fetch_on_start":false}`);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableFamilyParameters) {
        TTestEnv env(1, 4, {.StoragePools = 4, .ShowCreateTable = true, .EnableTableCacheModes = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateChecker checker(env);

        checker.CheckShowCreateTable(
            R"(CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value0 String,
                Value1 String FAMILY Family1,
                Value2 String FAMILY Family2,
                Value3 String FAMILY Family3,
                Value4 String FAMILY Family4,
                Value5 String FAMILY Family5,
                Value6 String FAMILY Family6,
                PRIMARY KEY (Key),
                FAMILY default (
                    DATA = "test0",
                    COMPRESSION = "lz4",
                    CACHE_MODE = "in_memory"
                ),
                FAMILY Family1 (
                    COMPRESSION = "off",
                    CACHE_MODE = "regular"
                ),
                FAMILY Family2 (
                    DATA = "test1",
                    CACHE_MODE = "in_memory"
                ),
                FAMILY Family3 (
                    DATA = "test2",
                    COMPRESSION = "lz4"
                ),
                FAMILY Family4 (
                    DATA = "test3"
                ),
                FAMILY Family5 (
                    COMPRESSION = "off"
                ),
                FAMILY Family6 (
                    CACHE_MODE = "regular"
                )
            );
            )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint32,
    `Value0` String,
    `Value1` String FAMILY `Family1`,
    `Value2` String FAMILY `Family2`,
    `Value3` String FAMILY `Family3`,
    `Value4` String FAMILY `Family4`,
    `Value5` String FAMILY `Family5`,
    `Value6` String FAMILY `Family6`,
    FAMILY `default` (DATA = 'test0', COMPRESSION = 'lz4', CACHE_MODE = 'in_memory'),
    FAMILY `Family1` (COMPRESSION = 'off', CACHE_MODE = 'regular'),
    FAMILY `Family2` (DATA = 'test1', CACHE_MODE = 'in_memory'),
    FAMILY `Family3` (DATA = 'test2', COMPRESSION = 'lz4'),
    FAMILY `Family4` (DATA = 'test3'),
    FAMILY `Family5` (COMPRESSION = 'off'),
    FAMILY `Family6` (CACHE_MODE = 'regular'),
    PRIMARY KEY (`Key`)
);
)"
        );

        checker.CheckShowCreateTable(
            R"(CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value0 String,
                PRIMARY KEY (Key),
                FAMILY default (
                    DATA = "test0"
                )
            );
            )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint32,
    `Value0` String,
    FAMILY `default` (DATA = 'test0'),
    PRIMARY KEY (`Key`)
);
)"
        );

        checker.CheckShowCreateTable(
            R"(CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value0 String,
                PRIMARY KEY (Key),
                FAMILY default (
                    COMPRESSION = "lz4"
                )
            );
            )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint32,
    `Value0` String,
    FAMILY `default` (COMPRESSION = 'lz4'),
    PRIMARY KEY (`Key`)
);
)"
        );

        checker.CheckShowCreateTable(
            R"(CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value0 String,
                PRIMARY KEY (Key),
                FAMILY default (
                    CACHE_MODE = "in_memory"
                )
            );
            )", "test_show_create",
R"(CREATE TABLE `test_show_create` (
    `Key` Uint32,
    `Value0` String,
    FAMILY `default` (CACHE_MODE = 'in_memory'),
    PRIMARY KEY (`Key`)
);
)"
        );
    }

    Y_UNIT_TEST(ShowCreateTableSystemTableWithEmptyKeyColumnIds) {
        // This test reproduces the crash from issue #30332
        // When trying to SHOW CREATE TABLE on a system table that has empty key column IDs,
        // the formatter crashes at line 347 with: Y_ENSURE(!tableDesc.GetKeyColumnIds().empty())
        //
        // The issue specifically mentions `.sys/tables` as causing the crash.
        // This test verifies that SHOW CREATE TABLE on system tables either succeeds
        // or returns a proper error status, but does not crash the server.

        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        NQuery::TQueryClient queryClient(env.GetDriver());
        auto session = queryClient.GetSession().GetValueSync().GetSession();

        // The issue specifically mentions .sys/tables as the problematic table
        // We test this and a few other system tables to ensure robustness
        TVector<TString> systemTablesToTest = {
            "/Root/.sys/tables",  // The specific table mentioned in issue #30332
            "/Root/.sys/partition_stats",
            "/Root/.sys/nodes"
        };

        for (const auto& systemTable : systemTablesToTest) {
            Cerr << "Testing SHOW CREATE TABLE on " << systemTable << Endl;

            // Try to execute SHOW CREATE TABLE on the system table
            // Before the fix, this would crash with Y_ENSURE at line 347
            // After the fix, this should either succeed or return a proper error status
            auto result = session.ExecuteQuery(
                TStringBuilder() << "SHOW CREATE TABLE `" << systemTable << "`;",
                NQuery::TTxControl::NoTx()
            ).GetValueSync();

            if (!result.IsSuccess()) {
                // If it fails, verify it's a proper error status, not an internal error from a crash
                // The formatter should handle empty key column IDs gracefully and return UNSUPPORTED
                // or SCHEME_ERROR, not crash with an internal error
                UNIT_ASSERT_C(
                    result.GetStatus() == EStatus::SCHEME_ERROR ||
                    result.GetStatus() == EStatus::BAD_REQUEST,
                    "SHOW CREATE TABLE on " << systemTable
                    << " should return a proper error status (SCHEME_ERROR or BAD_REQUEST), "
                    << "not an internal error from a crash. Got status: " << result.GetStatus()
                    << ", issues: " << result.GetIssues().ToString()
                );

                // Verify the error message is meaningful
                UNIT_ASSERT_C(
                    !result.GetIssues().ToString().empty(),
                    "Error message should not be empty for " << systemTable
                );

                Cerr << "SHOW CREATE TABLE on " << systemTable << " returned expected error: "
                     << result.GetStatus() << " - " << result.GetIssues().ToString() << Endl;
            } else {
                // If it succeeds, verify we got a valid result with the expected structure
                UNIT_ASSERT_C(
                    result.GetResultSets().size() > 0,
                    "SHOW CREATE TABLE on " << systemTable << " should return at least one result set"
                );

                auto resultSet = result.GetResultSet(0);
                auto columnsMeta = resultSet.GetColumnsMeta();
                UNIT_ASSERT_C(
                    columnsMeta.size() == 3,
                    "SHOW CREATE TABLE result should have 3 columns (Path, PathType, CreateQuery), got: "
                    << columnsMeta.size()
                );

                Cerr << "SHOW CREATE TABLE on " << systemTable << " succeeded" << Endl;
            }
        }
    }

    Y_UNIT_TEST(Nodes) {
        TTestEnv env;
        CreateTenantsAndTables(env, false);
        TTableClient client(env.GetDriver());
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `Root/Tenant1/.sys/nodes`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            ui32 offset = env.GetServer().GetRuntime()->GetNodeId(0);
            auto expected = Sprintf(R"([
                [["::1"];[%du]];
                [["::1"];[%du]];
            ])", offset + 3, offset + 4);

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `Root/Tenant2/.sys/nodes`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            ui32 offset = env.GetServer().GetRuntime()->GetNodeId(0);
            auto expected = Sprintf(R"([
                [["::1"];[%du]];
                [["::1"];[%du]];
            ])", offset + 1, offset + 2);

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `Root/.sys/nodes`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            ui32 offset = env.GetServer().GetRuntime()->GetNodeId(0);
            auto expected = Sprintf(R"([
                [["::1"];[%du]];
            ])", offset);

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(QueryStats) {
        TTestEnv env;
        CreateTenants(env);

        auto* runtime = env.GetServer().GetRuntime();
        runtime ->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_DEBUG);

        auto oneMinute = TDuration::Minutes(1);
        auto oneMinuteUs = oneMinute.MicroSeconds();

        auto instant = TAppData::TimeProvider->Now() + oneMinute;
        auto instantRounded = instant.MicroSeconds() / oneMinuteUs * oneMinuteUs;

        std::vector<ui64> buckets;
        for (size_t i : xrange(4)) {
            Y_UNUSED(i);
            buckets.push_back(instantRounded);
            instantRounded += oneMinuteUs;
        }

        auto& tenant1Nodes = env.GetTenants().List("/Root/Tenant1");
        auto& tenant2Nodes = env.GetTenants().List("/Root/Tenant2");

        UNIT_ASSERT_EQUAL(tenant1Nodes.size(), 2);
        UNIT_ASSERT_EQUAL(tenant2Nodes.size(), 2);

        auto tenant1Node0 = tenant1Nodes[0];
        auto tenant1Node1 = tenant1Nodes[1];
        auto tenant2Node = tenant2Nodes.front();
        ui32 staticNode = 0;

        auto makeQueryEvent = [&runtime] (ui32 nodeIdx, ui64 endTimeUs, const TString& queryText, ui64 readBytes) {
            auto stats = MakeHolder<NSysView::TEvSysView::TEvCollectQueryStats>();
            stats->QueryStats.MutableStats()->SetReadBytes(readBytes);
            stats->QueryStats.SetQueryText(queryText);
            stats->QueryStats.SetQueryTextHash(MurmurHash<ui64>(queryText.data(), queryText.size()));
            stats->QueryStats.SetDurationMs(1);
            stats->QueryStats.SetEndTimeMs(endTimeUs / 1000);

            auto serviceId = MakeSysViewServiceID(runtime->GetNodeId(nodeIdx));
            runtime->Send(new IEventHandle(serviceId, TActorId(), stats.Release()), nodeIdx);
        };

        makeQueryEvent(tenant1Node0, buckets[0], "a", 100);
        makeQueryEvent(tenant1Node1, buckets[1], "b", 200);
        makeQueryEvent(tenant1Node0, buckets[1], "c", 300);
        makeQueryEvent(tenant1Node0, buckets[1], "d", 400);
        makeQueryEvent(tenant1Node1, buckets[2], "e", 500);

        makeQueryEvent(tenant2Node, buckets[0], "f", 600);

        makeQueryEvent(staticNode, buckets[0], "g", 700);

        TTableClient client(env.GetDriver());
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `Root/.sys/top_queries_by_read_bytes_one_minute`;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"g\"];[1u];[700u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute`;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"a\"];[1u];[100u]];";
            result << "[[" << buckets[2] << "u];[\"d\"];[1u];[400u]];";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "[[" << buckets[2] << "u];[\"b\"];[3u];[200u]];";
            result << "[[" << buckets[3] << "u];[\"e\"];[1u];[500u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `Root/Tenant2/.sys/top_queries_by_read_bytes_one_minute`;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"f\"];[1u];[600u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, QueryText, Rank, ReadBytes ";
            query << "FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd >= CAST(" << buckets[1] << "ul as Timestamp) ";
            query << "AND IntervalEnd < CAST(" << buckets[3] << "ul as Timestamp);";

            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"a\"];[1u];[100u]];";
            result << "[[" << buckets[2] << "u];[\"d\"];[1u];[400u]];";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "[[" << buckets[2] << "u];[\"b\"];[3u];[200u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, QueryText, Rank, ReadBytes ";
            query << "FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd > CAST(" << buckets[1] << "ul as Timestamp) ";
            query << "AND IntervalEnd <= CAST(" << buckets[3] << "ul as Timestamp);";

            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[2] << "u];[\"d\"];[1u];[400u]];";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "[[" << buckets[2] << "u];[\"b\"];[3u];[200u]];";
            result << "[[" << buckets[3] << "u];[\"e\"];[1u];[500u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, QueryText, Rank, ReadBytes ";
            query << "FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << buckets[2] << "ul as Timestamp) ";
            query << "AND Rank >= 1u AND Rank < 3u";

            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[2] << "u];[\"d\"];[1u];[400u]];";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, QueryText, Rank, ReadBytes ";
            query << "FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << buckets[2] << "ul as Timestamp) ";
            query << "AND Rank > 1u AND Rank <= 3u";

            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "[[" << buckets[2] << "u];[\"b\"];[3u];[200u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(QueryStatsFields) {
        TTestEnv env;
        CreateRootTable(env, 3);

        auto nowUs = TInstant::Now().MicroSeconds();

        TString queryText("SELECT * FROM `Root/Table0`");

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            queryText, TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                CPUTime,
                CompileCPUTime,
                CompileDuration,
                ComputeNodesCount,
                DeleteBytes,
                DeleteRows,
                Duration,
                EndTime,
                FromQueryCache,
                IntervalEnd,
                MaxComputeCPUTime,
                MaxShardCPUTime,
                MinComputeCPUTime,
                MinShardCPUTime,
                ParametersSize,
                Partitions,
                ProcessCPUTime,
                QueryText,
                Rank,
                ReadBytes,
                ReadRows,
                RequestUnits,
                ShardCount,
                SumComputeCPUTime,
                SumShardCPUTime,
                Type,
                UpdateBytes,
                UpdateRows,
                UserSID
            FROM `Root/.sys/top_queries_by_read_bytes_one_minute`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 29);

        check.Uint64GreaterOrEquals(0); // CPUTime
        check.Uint64GreaterOrEquals(0); // CompileCPUTime
        check.Int64GreaterOrEquals(0); // CompileDuration
        check.Uint64(2); // ComputeNodesCount
        check.Uint64(0); // DeleteBytes
        check.Uint64(0); // DeleteRows
        check.Int64Greater(0); // Duration
        check.Uint64Greater(nowUs); // EndTime
        check.Bool(false); // FromQueryCache
        check.Uint64Greater(nowUs); // IntervalEnd
        check.Uint64GreaterOrEquals(0); // MaxComputeCPUTime
        check.Uint64GreaterOrEquals(0); // MaxShardCPUTime
        check.Uint64GreaterOrEquals(0); // MinComputeCPUTime
        check.Uint64GreaterOrEquals(0); // MinShardCPUTime
        check.Uint64(0); // ParametersSize
        check.Uint64(3); // Partitions
        check.Uint64GreaterOrEquals(0); // ProcessCPUTime
        check.String(queryText); // QueryText
        check.Uint64(1); // Rank
        check.Uint64(0); // ReadBytes
        check.Uint64(0); // ReadRows
        check.Uint64Greater(0); // RequestUnits

        // https://a.yandex-team.ru/arcadia/ydb/core/sys_view/query_stats/query_stats.cpp?rev=r9637451#L356
        check.Uint64(0); // ShardCount
        check.Uint64GreaterOrEquals(0); // SumComputeCPUTime
        check.Uint64GreaterOrEquals(0); // SumShardCPUTime
        check.String("data"); // Type
        check.Uint64(0); // UpdateBytes
        check.Uint64(0); // UpdateRows
        check.Null(); // UserSID
    }

    Y_UNIT_TEST(PartitionStatsTtlFields) {
        TTestEnv env;
        env.GetClient().CreateTable("/Root", R"(
            Name: "Table0"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "CreatedAt", Type: "Timestamp" }
            KeyColumnNames: ["Key"]
            TTLSettings {
              Enabled {
                ColumnName: "CreatedAt"
              }
            }
        )");

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "REPLACE INTO `Root/Table0` (Key, CreatedAt) VALUES (0u, CAST(0 AS Timestamp));",
            TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        // wait for conditional erase
        for (size_t iter = 0; iter < 70; ++iter) {
            auto result = session.ExecuteDataQuery(
                "SELECT * FROM `Root/Table0`;", TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto ysonString = FormatResultSetYson(result.GetResultSet(0));
            if (ysonString == "[[#]]") {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                LastTtlRunTime,
                LastTtlRowsProcessed,
                LastTtlRowsErased
            FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 3);

        check.Uint64LessOrEquals(TInstant::Now().MicroSeconds()); // LastTtlRunTime
        check.Uint64(1u); // LastTtlRowsProcessed
        check.Uint64(1u); // LastTtlRowsErased
    }

    Y_UNIT_TEST_TWIN(PartitionStatsAfterDropTable, UseColumnTable) {
        TTestEnv env({.DataShardStatsReportIntervalSeconds = 0});
        if (UseColumnTable)
            CreateRootColumnTable(env);
        else
            CreateRootTable(env);

        TTableClient client(env.GetDriver());

        WaitForStats(client, "/Root/.sys/partition_stats", "Path = '/Root/Table0'");

        auto session = client.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(R"(
            DROP TABLE `Root/Table0`;
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Verify Table0 is removed from partition_stats
        auto table0Count = GetRowCount(client, "/Root/.sys/partition_stats", "Path = '/Root/Table0'");
        UNIT_ASSERT_VALUES_EQUAL(table0Count, 0);
    }

    Y_UNIT_TEST(PartitionStatsLocksFields) {
        TTestEnv env({.DataShardStatsReportIntervalSeconds = 0});
        CreateRootTable(env, /* partitionCount */ 1, /* fillTable */ true);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        BreakLock(session, "/Root/Table0");

        WaitForStats(client, "/Root/.sys/partition_stats", "LocksBroken != 0");

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                LocksAcquired,
                LocksWholeShard,
                LocksBroken
            FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);
        TYsonFieldChecker check(ysonString, 3);

        check.Uint64(1); // LocksAcquired
        check.Uint64(0); // LocksWholeShard
        check.Uint64(1); // LocksBroken
    }

    Y_UNIT_TEST_TWIN(PartitionStatsAfterRenameTable, UseColumnTable) {
        TTestEnv env({.DataShardStatsReportIntervalSeconds = 0});
        if (UseColumnTable)
            CreateRootColumnTable(env);
        else
            CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        WaitForStats(client, "/Root/.sys/partition_stats", "Path = '/Root/Table0'");

        auto result = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `Root/Table0` RENAME TO `Root/Table1`;
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        WaitForStats(client, "/Root/.sys/partition_stats", "Path = '/Root/Table1'");

        // Verify Table0 is no longer in partition_stats
        auto table0Count = GetRowCount(client, "/Root/.sys/partition_stats", "Path = '/Root/Table0'");
        UNIT_ASSERT_VALUES_EQUAL(table0Count, 0);

        // Verify Table1 exists in partition_stats
        auto table1Count = GetRowCount(client, "/Root/.sys/partition_stats", "Path = '/Root/Table1'");
        UNIT_ASSERT_VALUES_EQUAL(table1Count, 1);
    }

    Y_UNIT_TEST(PartitionStatsFields) {
        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env({.DataShardStatsReportIntervalSeconds = 0});
        CreateRootTable(env);
        const auto describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "Root/Table0");
        const auto tablePathId = describeResult.GetPathId();

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "REPLACE INTO `Root/Table0` (Key, Value) VALUES (0u, \"A\");",
            TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        // wait for stats
        for (size_t iter = 0; iter < 30; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT AccessTime FROM `/Root/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto ysonString = NKqp::StreamResultToYson(it);
            if (ysonString != "[[#]]") {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                AccessTime,
                CPUCores,
                CoordinatedTxCompleted,
                DataSize,
                ImmediateTxCompleted,
                IndexSize,
                InFlightTxCount,
                NodeId,
                OwnerId,
                PartIdx,
                Path,
                PathId,
                RangeReadRows,
                RangeReads,
                RowCount,
                RowDeletes,
                RowReads,
                RowUpdates,
                StartTime,
                TabletId,
                TxRejectedByOutOfStorage,
                TxRejectedByOverload,
                TxCompleteLag,
                FollowerId,
                LocksAcquired,
                LocksWholeShard,
                LocksBroken,
                UpdateTime
            FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);
        TYsonFieldChecker check(ysonString, 28);

        check.Uint64GreaterOrEquals(nowUs); // AccessTime
        check.DoubleGreaterOrEquals(0.0); // CPUCores
        check.Uint64(1u); // CoordinatedTxCompleted
        check.Uint64(584u); // DataSize
        check.Uint64(1u); // ImmediateTxCompleted
        check.Uint64(0u); // IndexSize
        check.Uint64(0u); // InFlightTxCount
        check.Uint64Greater(0u); // NodeId
        check.Uint64(72057594046644480ull); // OwnerId
        check.Uint64(0u); // PartIdx
        check.String("/Root/Table0"); // Path
        check.Uint64(tablePathId); // PathId
        check.Uint64(0u); // RangeReadRows
        check.Uint64(0u); // RangeReads
        check.Uint64(1u); // RowCount
        check.Uint64(0u); // RowDeletes
        check.Uint64(0u); // RowReads
        check.Uint64(1u); // RowUpdates
        check.Uint64GreaterOrEquals(nowUs); // StartTime
        check.Uint64Greater(0u); // TabletId
        check.Uint64(0u); // TxRejectedByOutOfStorage
        check.Uint64(0u); // TxRejectedByOverload
        check.Int64(0); // TxCompleteLag
        check.Uint64(0u); // FollowerId
        check.Uint64(0u); // LocksAcquired
        check.Uint64(0u); // LocksWholeShard
        check.Uint64(0u); // LocksBroken
        check.Uint64GreaterOrEquals(nowUs); // UpdateTime
    }

    Y_UNIT_TEST(QueryStatsAllTables) {
        auto check = [&] (const TString& queryText) {
            TTestEnv env;
            CreateRootTable(env);

            TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            NKqp::AssertSuccessResult(session.ExecuteDataQuery(
                "SELECT * FROM `Root/Table0`", TTxControl::BeginTx().CommitTx()
            ).GetValueSync());

            auto it = client.StreamExecuteScanQuery(queryText).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[0u]]
            ])", NKqp::StreamResultToYson(it));
        };

        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_read_bytes_one_minute`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_read_bytes_one_hour`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_duration_one_minute`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_duration_one_hour`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_cpu_time_one_minute`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_cpu_time_one_hour`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_request_units_one_minute`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_request_units_one_hour`");
    }

    Y_UNIT_TEST(SysViewScanBackPressure) {
        NKikimrConfig::TTableServiceConfig tableServiceConfig;
        tableServiceConfig.MutableResourceManager()->SetChannelBufferSize(1_KB);

        TTestEnv env({
            .EnableSVP = true,
            .ShowCreateTable = true,
            .TableServiceConfig = tableServiceConfig,
        });
        CreateTenant(env, "Tenant1", true, /* nodesCount */ 1);

        TDriver driver(TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1"));
        NQuery::TQueryClient queryClient(driver);

        // PAYLOAD_SIZE * NUMBER_OF_QUERIES should be greater than BatchSizeLimit in TSysViewProcessor (4 MB)
        constexpr ui64 PAYLOAD_SIZE = 100_KB;
        constexpr ui64 NUMBER_OF_QUERIES = 100;

        auto& actorSystem = *env.GetServer().GetRuntime()->GetActorSystem(env.GetTenants().List("/Root/Tenant1")[0]);
        ui64 amountSize = 0;
        const std::string payload(PAYLOAD_SIZE, 'X');
        for (ui64 i = 0; i < NUMBER_OF_QUERIES; ++i) {
            const auto queryText = TStringBuilder() << "SELECT * FROM `/Root/Tenant1/Table0` /* " << i << " = " << payload << " */";
            amountSize += queryText.size();

            auto collectEv = std::make_unique<NSysView::TEvSysView::TEvCollectQueryStats>();
            collectEv->Database = "/Root/Tenant1";
            collectEv->QueryStats.SetQueryTextHash(i);
            collectEv->QueryStats.SetQueryText(queryText);
            collectEv->QueryStats.SetEndTimeMs(TInstant::Now().MilliSeconds());
            actorSystem.Send(NSysView::MakeSysViewServiceID(actorSystem.NodeId), collectEv.release());
        }

        WaitFor(TDuration::Minutes(1), "statistics delivery", [&](TString& error) {
            const auto result = queryClient.ExecuteQuery(
                "SELECT SUM(Len(QueryText)) FROM `/Root/Tenant1/.sys/query_metrics_one_minute`",
                NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Minutes(1))
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            auto resultSet = result.GetResultSetParser(0);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());

            const auto currentSum = resultSet.ColumnParser(0).GetOptionalUint64().value_or(0);
            error = TStringBuilder() << "currentSum = " << currentSum << ", expectedSum = " << amountSize;
            return currentSum >= amountSize;
        });
    }

    Y_UNIT_TEST(QueryStatsRetries) {
        TTestEnv env;
        CreateRootTable(env);

        TString queryText("SELECT * FROM `Root/Table0`");

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            queryText, TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        auto serviceToKill = MakeSysViewServiceID(env.GetServer().GetRuntime()->GetNodeId(2));
        env.GetServer().GetRuntime()->Send(new IEventHandle(serviceToKill, TActorId(), new TEvents::TEvPoison()));

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                ReadBytes
            FROM `Root/.sys/top_queries_by_read_bytes_one_minute`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        NKqp::CompareYson(R"([
            [[0u]]
        ])", NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(ConcurrentScans) {
        TTestEnv env;
        CreateRootTable(env);
        TTableClient client(env.GetDriver());

        TVector<TAsyncScanQueryPartIterator> futures;
        for (size_t i = 0; i < 20; ++i) {
            auto future0 = client.StreamExecuteScanQuery(R"(
                SELECT
                    ReadBytes
                FROM `Root/.sys/top_queries_by_read_bytes_one_minute`;
            )");
            futures.push_back(future0);
        }

        for (auto& future0 : futures) {
            auto it = future0.GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto streamPart = it.ReadNext().GetValueSync();
            if (streamPart.IsSuccess()) {
                UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SUCCESS);
                Cerr << "SUCCESS" << Endl;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::OVERLOADED);
                Cerr << "FAIL " << streamPart.GetIssues().ToString() << Endl;
            }
        }
    }

    Y_UNIT_TEST(PDisksFields) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT
                    AvailableSize,
                    BoxId,
                    DecommitStatus,
                    ExpectedSlotCount,
                    Guid,
                    Kind,
                    NodeId,
                    NumActiveSlots,
                    Path,
                    PDiskId,
                    ReadCentric,
                    SharedWithOS,
                    SlotSizeInUnits,
                    State,
                    Status,
                    StatusChangeTimestamp,
                    TotalSize,
                    Type
                FROM `/Root/.sys/ds_pdisks`
                WHERE BoxId IS NOT NULL;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }

        TYsonFieldChecker check(ysonString, 18);

        check.Uint64(0u); // AvailableSize
        check.Uint64(999u); // BoxId
        check.String("DECOMMIT_NONE"); // DecommitStatus
        check.Uint64(16); // ExpectedSlotCount
        check.Uint64(123u); // Guid
        check.Uint64(0u); // Kind
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(2); // NumActiveSlots
        check.StringContains("pdisk_1.dat"); // Path
        check.Uint64(1u); // PDiskId
        check.Bool(false); // ReadCentric
        check.Bool(false); // SharedWithOS
        check.Uint64(0u); // SlotSizeInUnits
        check.String("Initial"); // State
        check.String("ACTIVE"); // Status
        check.Null(); // StatusChangeTimestamp
        check.Uint64(0u); // TotalSize
        check.String("ROT"); // Type
    }

    Y_UNIT_TEST(VSlotsFields) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT
                    AllocatedSize,
                    AvailableSize,
                    DiskSpace,
                    FailDomain,
                    FailRealm,
                    GroupGeneration,
                    GroupId,
                    Kind,
                    NodeId,
                    PDiskId,
                    Replicated,
                    State,
                    Status,
                    VDisk,
                    VSlotId
                FROM `/Root/.sys/ds_vslots` WHERE GroupId >= 0x80000000;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }

        TYsonFieldChecker check(ysonString, 15);

        check.Uint64(0u, true); // AllocatedSize
        check.Uint64(0u, true); // AvailableSize
        check.Null(); // DiskSpace
        check.Uint64(0u); // FailDomain
        check.Uint64(0u); // FailRealm
        check.Uint64(1u); // GroupGeneration
        check.Uint64(2181038080u); // GroupId
        check.String("Default"); // Kind
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(1u); // PDiskId
        check.Null(); // Replicated
        check.Null(); // State
        check.Null(); // Status
        check.Uint64(0u); // VDisk
        check.Uint64(1000u); // VSlotId
    }

    Y_UNIT_TEST(GroupsFields) {
        TTestEnv env;

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT
                    AllocatedSize,
                    AvailableSize,
                    BoxId,
                    EncryptionMode,
                    ErasureSpecies,
                    Generation,
                    GetFastLatency,
                    GroupId,
                    GroupSizeInUnits,
                    LifeCyclePhase,
                    PutTabletLogLatency,
                    PutUserDataLatency,
                    StoragePoolId,
                    LayoutCorrect,
                    OperatingStatus,
                    ExpectedStatus
                FROM `/Root/.sys/ds_groups` WHERE GroupId >= 0x80000000;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }

        TYsonFieldChecker check(ysonString, 16);

        check.Uint64(0u); // AllocatedSize
        check.Uint64GreaterOrEquals(0u); // AvailableSize
        check.Uint64(999u); // BoxId
        check.Uint64(0u); // EncryptionMode
        check.String("none"); // ErasureSpecies
        check.Uint64(1u); // Generation
        check.Null(); // GetFastLatency
        check.Uint64(2181038080u); // GroupId
        check.Uint64(0u); // GroupSizeInUnits
        check.Uint64(0u); // LifeCyclePhase
        check.Null(); // PutTabletLogLatency
        check.Null(); // PutUserDataLatency
        check.Uint64(2u); // StoragePoolId
        check.Bool(true); // LayoutCorrect
        check.String("DISINTEGRATED"); // OperatingStatus
        check.String("DISINTEGRATED"); // ExpectedStatus
    }

    Y_UNIT_TEST(StoragePoolsFields) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT
                    BoxId,
                    DefaultGroupSizeInUnits,
                    EncryptionMode,
                    ErasureSpecies,
                    Generation,
                    Kind,
                    Name,
                    NumGroups,
                    PathId,
                    SchemeshardId,
                    StoragePoolId,
                    VDiskKind
                FROM `/Root/.sys/ds_storage_pools`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }

        TYsonFieldChecker check(ysonString, 12);

        check.Uint64(999u); // BoxId
        check.Uint64(0u); // DefaultGroupSizeInUnits
        check.Uint64(0u); // EncryptionMode
        check.String("none"); // ErasureSpecies
        check.Uint64(1u); // Generation
        check.String("test"); // Kind
        check.String("/Root:test"); // Name
        check.Uint64(1u); // NumGroups
        check.Null(); // PathId
        check.Null(); // SchemeshardId
        check.Uint64(2u); // StoragePoolId
        check.String("Default"); // VDiskKind
    }

    Y_UNIT_TEST(StoragePoolsRanges) {
        TTestEnv env(1, 0, {.StoragePools = 3});

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId FROM `/Root/.sys/ds_storage_pools`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto ysonString = NKqp::StreamResultToYson(it);
            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId, StoragePoolId
                FROM `/Root/.sys/ds_storage_pools`
                WHERE BoxId = 999u AND StoragePoolId > 3u;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[999u];[4u]];
                [[999u];[5u]];
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId, StoragePoolId
                FROM `/Root/.sys/ds_storage_pools`
                WHERE BoxId = 999u AND StoragePoolId >= 3u;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[999u];[3u]];
                [[999u];[4u]];
                [[999u];[5u]];
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId, StoragePoolId
                FROM `/Root/.sys/ds_storage_pools`
                WHERE BoxId = 999u AND StoragePoolId < 4u;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[999u];[2u]];
                [[999u];[3u]];
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId, StoragePoolId
                FROM `/Root/.sys/ds_storage_pools`
                WHERE BoxId = 999u AND StoragePoolId <= 4u;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[999u];[2u]];
                [[999u];[3u]];
                [[999u];[4u]];
            ])", NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(TopPartitionsByCpuFields) {
        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env(1, 4, {.EnableSVP = true, .DataShardStatsReportIntervalSeconds = 0});
        CreateTenantsAndTables(env);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            rowCount = GetRowCount(client, "/Root/Tenant1/.sys/top_partitions_one_minute");
            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }
        ui64 intervalEnd = GetIntervalEnd(client, "/Root/Tenant1/.sys/top_partitions_one_minute");

        TStringBuilder query;
        query << R"(
            SELECT
                IntervalEnd,
                Rank,
                TabletId,
                Path,
                PeakTime,
                CPUCores,
                NodeId,
                DataSize,
                RowCount,
                IndexSize,
                InFlightTxCount
            FROM `/Root/Tenant1/.sys/top_partitions_one_minute`)"
            << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp)";
        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 11);
        check.Uint64(intervalEnd); // IntervalEnd
        check.Uint64(1); // Rank
        check.Uint64Greater(0); // TabletId
        check.String("/Root/Tenant1/Table1"); // Path
        check.Uint64GreaterOrEquals(nowUs); // PeakTime
        check.DoubleGreaterOrEquals(0.); // CPUCores
        check.Uint64Greater(0); // NodeId
        check.Uint64Greater(0); // DataSize
        check.Uint64(3); // RowCount
        check.Uint64(0); // IndexSize
        check.Uint64(0); // InFlightTxCount
    }

    Y_UNIT_TEST(TopPartitionsByCpuTables) {
        constexpr ui64 partitionCount = 5;

        TTestEnv env(1, 4, {.EnableSVP = true, .DataShardStatsReportIntervalSeconds = 0});
        CreateTenantsAndTables(env, true, partitionCount);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30 && rowCount < partitionCount; ++iter) {
            rowCount = GetRowCount(client, "/Root/Tenant1/.sys/top_partitions_one_minute");
            if (rowCount < partitionCount) {
                Sleep(TDuration::Seconds(1));
            }
        }
        auto check = [&] (const TString& name) {
            ui64 intervalEnd = GetIntervalEnd(client, name);
            TStringBuilder query;
            query << "SELECT Rank ";
            query << "FROM `" << name << "` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson("[[[1u]];[[2u]];[[3u]];[[4u]];[[5u]]]", NKqp::StreamResultToYson(it));
        };
        check("/Root/Tenant1/.sys/top_partitions_one_minute");
        check("/Root/Tenant1/.sys/top_partitions_one_hour");
    }

    Y_UNIT_TEST(TopPartitionsByCpuRanges) {
        constexpr ui64 partitionCount = 5;

        TTestEnv env(1, 4, {.EnableSVP = true, .DataShardStatsReportIntervalSeconds = 0});
        CreateTenantsAndTables(env, true, partitionCount);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30 && rowCount < partitionCount; ++iter) {
            rowCount = GetRowCount(client, "/Root/Tenant1/.sys/top_partitions_one_minute");
            if (rowCount < partitionCount) {
                Sleep(TDuration::Seconds(5));
            }
        }
        ui64 intervalEnd = GetIntervalEnd(client, "/Root/Tenant1/.sys/top_partitions_one_minute");
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, Rank ";
            query << "FROM `/Root/Tenant1/.sys/top_partitions_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            query << "AND Rank > 3u";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TStringBuilder result;
            result << "[";
            result << "[[" << intervalEnd << "u];[4u]];";
            result << "[[" << intervalEnd << "u];[5u]];";
            result << "]";
            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, Rank ";
            query << "FROM `/Root/Tenant1/.sys/top_partitions_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            query << "AND Rank >= 3u";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TStringBuilder result;
            result << "[";
            result << "[[" << intervalEnd << "u];[3u]];";
            result << "[[" << intervalEnd << "u];[4u]];";
            result << "[[" << intervalEnd << "u];[5u]];";
            result << "]";
            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, Rank ";
            query << "FROM `/Root/Tenant1/.sys/top_partitions_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            query << "AND Rank < 3u";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TStringBuilder result;
            result << "[";
            result << "[[" << intervalEnd << "u];[1u]];";
            result << "[[" << intervalEnd << "u];[2u]];";
            result << "]";
            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, Rank ";
            query << "FROM `/Root/Tenant1/.sys/top_partitions_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            query << "AND Rank <= 3u";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TStringBuilder result;
            result << "[";
            result << "[[" << intervalEnd << "u];[1u]];";
            result << "[[" << intervalEnd << "u];[2u]];";
            result << "[[" << intervalEnd << "u];[3u]];";
            result << "]";
            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(TopPartitionsByCpuFollowers) {
        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env(1, 4, {
            .EnableSVP = true,
            .EnableForceFollowers = true,
            .DataShardStatsReportIntervalSeconds = 0,
        });

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_TRACE);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        CreateTenant(env, "Tenant1", true);
        auto desc = TTableBuilder()
            .AddNullableColumn("Key", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("Key")
            .Build();

        auto settings = TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(3));

        auto result = session.CreateTable("/Root/Tenant1/Table1",
            std::move(desc), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        Cerr << "... UPSERT" << Endl;
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
            UPSERT INTO `Root/Tenant1/Table1` (Key) VALUES (1u), (2u), (3u);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());

        Cerr << "... SELECT from leader" << Endl;
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `Root/Tenant1/Table1` WHERE Key = 1;
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            NKqp::AssertSuccessResult(result);

            TString actual = FormatResultSetYson(result.GetResultSet(0));
            NKqp::CompareYson(R"([
                [[1u]]
            ])", actual);
        }

        Cerr << "... SELECT from follower" << Endl;
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `Root/Tenant1/Table1` WHERE Key = 2;
            )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
            NKqp::AssertSuccessResult(result);

            TString actual = FormatResultSetYson(result.GetResultSet(0));
            NKqp::CompareYson(R"([
                [[2u]]
            ])", actual);
        }

        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30; ++iter) {
            if (rowCount = GetRowCount(client, "/Root/Tenant1/.sys/top_partitions_one_minute", "FollowerId != 0"))
                break;
            Sleep(TDuration::Seconds(5));
        }
        UNIT_ASSERT_GE(rowCount, 0);

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT
                    IntervalEnd,
                    Rank,
                    TabletId,
                    Path,
                    PeakTime,
                    CPUCores,
                    NodeId,
                    DataSize,
                    RowCount,
                    IndexSize,
                    InFlightTxCount,
                    FollowerId,
                    IF(FollowerId = 0, 'L', 'F') AS LeaderFollower
                FROM `/Root/Tenant1/.sys/top_partitions_one_minute`
                ORDER BY IntervalEnd, Rank;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            NKqp::AssertSuccessResult(result);

            auto rs = result.GetResultSet(0);

            TString actual = FormatResultSetYson(rs);
            Cerr << "\n\n\n\n\n\n\n\n" << actual << "\n\n\n\n\n\n\n\n" << Endl;
        }

        ui64 intervalEnd = GetIntervalEnd(client, "/Root/Tenant1/.sys/top_partitions_one_minute");



        Cerr << "... SELECT leader from .sys/top_partitions_one_minute" << Endl;
        {
            TStringBuilder query;
            query << R"(
                SELECT
                    IntervalEnd,
                    Rank,
                    TabletId,
                    Path,
                    PeakTime,
                    CPUCores,
                    NodeId,
                    DataSize,
                    RowCount,
                    IndexSize,
                    InFlightTxCount,
                    FollowerId
                FROM `/Root/Tenant1/.sys/top_partitions_one_minute`)"
                << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) AND FollowerId = 0";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto ysonString = NKqp::StreamResultToYson(it);

            TYsonFieldChecker check(ysonString, 12);
            check.Uint64(intervalEnd); // IntervalEnd
            check.Uint64(1); // Rank
            check.Uint64Greater(0); // TabletId
            check.String("/Root/Tenant1/Table1"); // Path
            check.Uint64GreaterOrEquals(nowUs); // PeakTime
            check.DoubleGreaterOrEquals(0.); // CPUCores
            check.Uint64Greater(0); // NodeId
            check.Uint64Greater(0); // DataSize
            check.Uint64(3); // RowCount
            check.Uint64(0); // IndexSize
            check.Uint64(0); // InFlightTxCount
            check.Uint64(0); // FollowerId
        }

        Cerr << "... SELECT follower from .sys/top_partitions_one_minute" << Endl;
        {
            TStringBuilder query;
            query << R"(
                SELECT
                    IntervalEnd,
                    Rank,
                    TabletId,
                    Path,
                    PeakTime,
                    CPUCores,
                    NodeId,
                    DataSize,
                    RowCount,
                    IndexSize,
                    InFlightTxCount,
                    FollowerId
                FROM `/Root/Tenant1/.sys/top_partitions_one_minute`)"
                << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) AND FollowerId != 0";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto ysonString = NKqp::StreamResultToYson(it);

            TYsonFieldChecker check(ysonString, 12);
            check.Uint64(intervalEnd); // IntervalEnd
            check.Uint64(2); // Rank
            check.Uint64Greater(0); // TabletId
            check.String("/Root/Tenant1/Table1"); // Path
            check.Uint64GreaterOrEquals(nowUs); // PeakTime
            check.DoubleGreaterOrEquals(0.); // CPUCores
            check.Uint64Greater(0); // NodeId
            check.Uint64Greater(0); // DataSize
            check.Uint64(3); // RowCount
            check.Uint64(0); // IndexSize
            check.Uint64(0); // InFlightTxCount
            check.Uint64Greater(0); // FollowerId
        }
    }

    Y_UNIT_TEST(TopPartitionsByTliFields) {
        TTestEnv env(1, 4, {.EnableSVP = true, .DataShardStatsReportIntervalSeconds = 0});
        CreateTenantsAndTables(env);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        const TString tableName = "/Root/Tenant1/Table1";
        const TString viewName = "/Root/Tenant1/.sys/top_partitions_by_tli_one_minute";

        BreakLock(session, tableName);

        WaitForStats(client, viewName, "LocksAcquired != 0");

        ui64 intervalEnd = GetIntervalEnd(client, viewName);

        TStringBuilder query;
        query << R"(
            SELECT
                IntervalEnd,
                Rank,
                TabletId,
                Path,
                LocksAcquired,
                LocksWholeShard,
                LocksBroken,
                NodeId,
                DataSize,
                RowCount,
                IndexSize)"
            << " FROM `" << viewName << "`"
            << " WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp)"
            << " AND Path=\"" << tableName << "\"";
        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);
        TYsonFieldChecker check(ysonString, 11);
        check.Uint64(intervalEnd); // IntervalEnd
        check.Uint64(1); // Rank
        check.Uint64Greater(0); // TabletId
        check.String(tableName); // Path
        check.Uint64GreaterOrEquals(1); // LocksAcquired
        check.Uint64(0); // LocksWholeShard
        check.Uint64GreaterOrEquals(1); // LocksBroken
        check.Uint64Greater(0); // NodeId
        check.Uint64Greater(0); // DataSize
        check.Uint64(4); // RowCount
        check.Uint64(0); // IndexSize
    }

    Y_UNIT_TEST_TWIN(Describe, EnableRealSystemViewPaths) {
        TTestEnv env({ .EnableRealSystemViewPaths = EnableRealSystemViewPaths });
        CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            if (EnableRealSystemViewPaths) {
                auto result = session.DescribeSystemView("/Root/.sys/partition_stats").GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                const auto& systemView = result.GetSystemViewDescription();

                UNIT_ASSERT_VALUES_EQUAL(systemView.GetSysViewId(), 1);
                UNIT_ASSERT_VALUES_EQUAL(systemView.GetSysViewName(), "partition_stats");

                const auto& columns = systemView.GetTableColumns();
                UNIT_ASSERT_VALUES_EQUAL(columns.size(), 31);
                UNIT_ASSERT_STRINGS_EQUAL(columns[0].Name, "OwnerId");
                UNIT_ASSERT_STRINGS_EQUAL(FormatType(columns[0].Type), "Uint64?");

                const auto& keyColumns = systemView.GetPrimaryKeyColumns();
                UNIT_ASSERT_VALUES_EQUAL(keyColumns.size(), 4);
                UNIT_ASSERT_STRINGS_EQUAL(keyColumns[0], "OwnerId");
            } else {
                auto settings = TDescribeTableSettings()
                    .WithKeyShardBoundary(true)
                    .WithTableStatistics(true)
                    .WithPartitionStatistics(true);

                auto result = session.DescribeTable("/Root/.sys/partition_stats", settings).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                const auto& table = result.GetTableDescription();
                const auto& columns = table.GetTableColumns();
                const auto& keyColumns = table.GetPrimaryKeyColumns();

                UNIT_ASSERT_VALUES_EQUAL(columns.size(), 31);
                UNIT_ASSERT_STRINGS_EQUAL(columns[0].Name, "OwnerId");
                UNIT_ASSERT_STRINGS_EQUAL(FormatType(columns[0].Type), "Uint64?");

                UNIT_ASSERT_VALUES_EQUAL(keyColumns.size(), 4);
                UNIT_ASSERT_STRINGS_EQUAL(keyColumns[0], "OwnerId");

                UNIT_ASSERT_VALUES_EQUAL(table.GetPartitionStats().size(), 0);
                UNIT_ASSERT_VALUES_EQUAL(table.GetPartitionsCount(), 0);
            }
        }

        TSchemeClient schemeClient(env.GetDriver());
        {
            auto result = schemeClient.DescribePath("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "partition_stats");

            if (EnableRealSystemViewPaths) {
                UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::SysView);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Table);
            }
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "partition_stats");
            if (EnableRealSystemViewPaths) {
                UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::SysView);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Table);
            }
        }
    }

    Y_UNIT_TEST_TWIN(SystemViewFailOps, EnableRealSystemViewPaths) {
        TTestEnv env({ .EnableRealSystemViewPaths = EnableRealSystemViewPaths });
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);

        // Make AdministrationAllowedSIDs non-empty to deny any user cluster admin privilege.
        // That can cause side effects, especially when dealing with system reserved names.
        // Using an authorized non-admin user helps avoid these side effects.
        env.GetServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back("root@builtin");

        TTableClient adminClient(env.GetDriver(), TClientSettings().AuthToken("root@builtin"));
        auto adminSession = adminClient.CreateSession().GetValueSync().GetSession();

        TTableClient userClient(env.GetDriver(), TClientSettings().AuthToken("user@builtin"));
        auto userSession = userClient.CreateSession().GetValueSync().GetSession();

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                GRANT 'ydb.generic.full' ON `/Root` TO `user@builtin`;
                )";
            auto result = adminSession.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto desc = TTableBuilder()
                .AddNullableColumn("Column1", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumn("Column1")
                .Build();

            auto result = userSession.CreateTable("/Root/.sys/partition_stats", std::move(desc)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSession.CopyTable("/Root/.sys/partition_stats", "/Root/Table0").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto settings = TAlterTableSettings()
                .AppendDropColumns("OwnerId");

            auto result = userSession.AlterTable("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSession.DropTable("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSession.ExecuteSchemeQuery(R"(
                DROP TABLE `/Root/.sys/partition_stats`;
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSession.ReadTable("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TReadTableResultPart streamPart = result.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SCHEME_ERROR);
            streamPart.GetIssues().PrintTo(Cerr);
        }
        {
            TValueBuilder rows;
            rows.BeginList().EndList();
            auto result = userClient.BulkUpsert("/Root/.sys/partition_stats", rows.Build()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }

        auto driverConfig = env.GetDriver().GetConfig();
        driverConfig.SetAuthToken("user@builtin");
        const auto driver = TDriver(driverConfig);
        auto userSchemeClient = TSchemeClient(driver);
        {
            auto result = userSchemeClient.MakeDirectory("/Root/.sys").GetValueSync();
            if (EnableRealSystemViewPaths) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                    "path exist", result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            }
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSchemeClient.MakeDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSchemeClient.RemoveDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSchemeClient.RemoveDirectory("/Root/.sys/partition_stats").GetValueSync();
            if (EnableRealSystemViewPaths) {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            }
            result.GetIssues().PrintTo(Cerr);
        }
        {
            TModifyPermissionsSettings settings;
            auto result = userSchemeClient.ModifyPermissions("/Root/.sys/partition_stats", settings).GetValueSync();
            if (EnableRealSystemViewPaths) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            }
            result.GetIssues().PrintTo(Cerr);
        }
    }

    Y_UNIT_TEST_TWIN(DescribeSystemFolder, EnableRealSystemViewPaths) {
        TTestEnv env({ .EnableRealSystemViewPaths = EnableRealSystemViewPaths });
        CreateTenantsAndTables(env, true);

        TSchemeClient schemeClient(env.GetDriver());
        {
            auto result = schemeClient.ListDirectory("/Root").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_STRINGS_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();
            SortBy(children, [](const auto& entry) { return entry.Name; });
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 5);
            UNIT_ASSERT_STRINGS_EQUAL(children[0].Name, ".metadata");
            UNIT_ASSERT_STRINGS_EQUAL(children[1].Name, ".sys");
            UNIT_ASSERT_STRINGS_EQUAL(children[2].Name, "Table0");
            UNIT_ASSERT_STRINGS_EQUAL(children[3].Name, "Tenant1");
            UNIT_ASSERT_STRINGS_EQUAL(children[4].Name, "Tenant2");
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "Root/Tenant1");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::SubDomain);

            auto children = result.GetChildren();
            SortBy(children, [](const auto& entry) { return entry.Name; });
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(children[0].Name, ".sys");
            UNIT_ASSERT_STRINGS_EQUAL(children[1].Name, "Table1");
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 35);

            THashSet<TString> names;
            for (const auto& child : children) {
                names.insert(TString{child.Name});
                if (EnableRealSystemViewPaths) {
                    UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::SysView);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::Table);
                }
            }
            UNIT_ASSERT(names.contains("partition_stats"));
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1/.sys").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();

            UNIT_ASSERT_VALUES_EQUAL(children.size(), 29);

            THashSet<TString> names;
            for (const auto& child : children) {
                names.insert(TString{child.Name});
                if (EnableRealSystemViewPaths) {
                    UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::SysView);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::Table);
                }
            }
            UNIT_ASSERT(names.contains("partition_stats"));
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1/Table1/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
    }

    Y_UNIT_TEST(DescribeAccessDenied) {
        TTestEnv env;
        CreateTenantsAndTables(env, false);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetAuthToken("user0@builtin");
        auto driver = TDriver(driverConfig);

        TSchemeClient schemeClient(driver);
        {
            auto result = schemeClient.ListDirectory("/Root").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.DescribePath("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.DescribePath("/Root/Tenant1/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
    }

    Y_UNIT_TEST(TabletsFields) {
        TTestEnv env(1, 0);
        CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                BootState,
                CPU,
                Generation,
                Memory,
                Network,
                NodeId,
                FollowerId,
                State,
                TabletId,
                Type,
                VolatileState
            FROM `/Root/.sys/hive_tablets`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 11);

        check.String("Running"); // BootState
        check.DoubleGreaterOrEquals(0.0); // CPU
        check.Uint64(1u); // Generation
        check.Uint64GreaterOrEquals(0u); // Memory
        check.Uint64(0u); // Network
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(0u); // FollowerId
        check.String("ReadyToWork"); // State
        check.Uint64(72075186224037888ul); // TabletId
        check.String("DataShard"); // Type
        check.String("Running"); // VolatileState
    }

    Y_UNIT_TEST(TabletsShards) {
        TTestEnv env(1, 0);
        CreateRootTable(env, 3);

        TTableClient client(env.GetDriver());
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT FollowerId, TabletId, Type
            FROM `/Root/.sys/hive_tablets`;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[0u];[72075186224037888u];["DataShard"]];
            [[0u];[72075186224037889u];["DataShard"]];
            [[0u];[72075186224037890u];["DataShard"]];
        ])", NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(TabletsFollowers) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto desc = TTableBuilder()
            .AddNullableColumn("Column1", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("Column1")
            .Build();

        auto settings = TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(3));

        auto result = session.CreateTable("/Root/Table0",
            std::move(desc), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT FollowerId, TabletId, Type
            FROM `/Root/.sys/hive_tablets`;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[0u];[72075186224037888u];["DataShard"]];
            [[1u];[72075186224037888u];["DataShard"]];
            [[2u];[72075186224037888u];["DataShard"]];
            [[3u];[72075186224037888u];["DataShard"]];
        ])", NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(TabletsRanges) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto desc = TTableBuilder()
            .AddNullableColumn("Column1", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("Column1")
            .Build();

        auto settings = TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(3))
            .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(3));

        auto result = session.CreateTable("/Root/Table0",
            std::move(desc), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        std::vector<std::pair<TString, TString>> testData = {
            {
                "TabletId = 72075186224037888ul AND FollowerId > 1u",
                R"([
                    [[2u];[72075186224037888u]];
                    [[3u];[72075186224037888u]];
                ])"
            },
            {
                "TabletId = 72075186224037888ul AND FollowerId >= 1u",
                R"([
                    [[1u];[72075186224037888u]];
                    [[2u];[72075186224037888u]];
                    [[3u];[72075186224037888u]];
                ])"
            },
            {
                "TabletId = 72075186224037888ul AND FollowerId < 2u",
                R"([
                    [[0u];[72075186224037888u]];
                    [[1u];[72075186224037888u]];
                ])"
            },
            {
                "TabletId = 72075186224037888ul AND FollowerId <= 2u",
                R"([
                    [[0u];[72075186224037888u]];
                    [[1u];[72075186224037888u]];
                    [[2u];[72075186224037888u]];
                ])"
            },
            {
                "TabletId > 72075186224037888ul AND TabletId < 72075186224037890ul",
                R"([
                    [[0u];[72075186224037889u]];
                    [[1u];[72075186224037889u]];
                    [[2u];[72075186224037889u]];
                    [[3u];[72075186224037889u]];
                ])"
            }
        };

        TString enablePredicateExtractor = R"(
            PRAGMA Kikimr.OptEnablePredicateExtract = "true";
        )";

        for (auto& data: testData) {
            TString query = R"(
                SELECT FollowerId, TabletId
                FROM `/Root/.sys/hive_tablets`
                WHERE <PREDICATE>;
            )";

            SubstGlobal(query, "<PREDICATE>", data.first);

            auto it = client.StreamExecuteScanQuery(enablePredicateExtractor + query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto streamed = NKqp::StreamResultToYson(it);

            it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto expected = NKqp::StreamResultToYson(it);

            // Compare two ways of execution
            NKqp::CompareYson(expected, streamed);
            // And check with expected result from test description
            NKqp::CompareYson(data.second, streamed);
        }
    }

    Y_UNIT_TEST(TabletsRangesPredicateExtractDisabled) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto desc = TTableBuilder()
            .AddNullableColumn("Column1", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("Column1")
            .Build();

        auto settings = TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(3))
            .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(3));

        auto result = session.CreateTable("/Root/Table0",
            std::move(desc), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TString query = R"(
            SELECT FollowerId, TabletId
            FROM `/Root/.sys/hive_tablets`
            WHERE TabletId <= 72075186224037888ul OR TabletId >= 72075186224037890ul
            ORDER BY TabletId, FollowerId
        )";

        TString expected = R"([
            [[0u];[72075186224037888u]];
            [[1u];[72075186224037888u]];
            [[2u];[72075186224037888u]];
            [[3u];[72075186224037888u]];
            [[0u];[72075186224037890u]];
            [[1u];[72075186224037890u]];
            [[2u];[72075186224037890u]];
            [[3u];[72075186224037890u]];
        ])";

        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        // System view dows not support multiple ranges, thus here will be an error if
        // predicate extraction occurs.
        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    void TestQueryType(
        std::function<void(const TTestEnv&, const TString&)> execQuery,
        const TString& type)
    {
        TTestEnv env(1, 0);
        CreateRootTable(env, 1, /* fillTable */ true);

        TString query("SELECT * FROM `Root/Table0`");
        execQuery(env, query);

        TTableClient client(env.GetDriver());
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT QueryText, Type, ReadRows
            FROM `Root/.sys/top_queries_by_read_bytes_one_minute`
            ORDER BY ReadRows DESC
            LIMIT 1
            ;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        NKqp::CompareYson(
            Sprintf("[[[\"%s\"];[\"%s\"];[3u]]]", query.c_str(), type.c_str()),
            NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(CollectPreparedQueries) {
        TestQueryType([](const TTestEnv& env, const TString& query) {
            TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto prepareResult = session.PrepareDataQuery(query).GetValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
            auto prepared = prepareResult.GetQuery();
            auto result = prepared.Execute(TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }, "data");
    }

    Y_UNIT_TEST(CollectScanQueries) {
        TestQueryType([](const TTestEnv& env, const TString& query) {
            TTableClient client(env.GetDriver());
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::StreamResultToYson(it);
        }, "scan");
    }

    Y_UNIT_TEST(CollectScriptingQueries) {
        TestQueryType([](const TTestEnv& env, const TString& query) {
            auto scriptingClient = NYdb::NScripting::TScriptingClient(env.GetDriver());
            auto result = scriptingClient.ExecuteYqlScript(query).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }, "script");
    }

    Y_UNIT_TEST(QueryMetricsSimple) {
        TTestEnv env(1, 2, {.EnableSVP = true});
        CreateTenant(env, "Tenant1", true);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto session = client.CreateSession().GetValueSync().GetSession();

        NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Tenant1/Table1` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "SELECT * FROM `/Root/Tenant1/Table1`", TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT SumReadBytes
                FROM `/Root/Tenant1/.sys/query_metrics_one_minute`
                WHERE QueryText = 'SELECT * FROM `/Root/Tenant1/Table1`';
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(5));
            }
        }

        UNIT_ASSERT_GE(rowCount, 0);
        NKqp::CompareYson(R"([
            [[0u]];
        ])", ysonString);
    }
}

Y_UNIT_TEST_SUITE(ShowCreateView) {

Y_UNIT_TEST(Basic) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    checker.CheckShowCreateView(R"(
            CREATE VIEW `test_view` WITH security_invoker = TRUE AS SELECT 1;
        )",
        "test_view",
R"(CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
SELECT
    1
;
)"
    );
}

Y_UNIT_TEST(FromTable) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE t (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(R"(
            CREATE VIEW test_view WITH security_invoker = TRUE AS
                SELECT * FROM t;
        )",
        "test_view",
R"(CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
SELECT
    *
FROM
    t
;
)"
    );
}

Y_UNIT_TEST(WithTablePathPrefix) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE `a/b/c/t` (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(R"(
            PRAGMA TablePathPrefix = "/Root/a/b/c";
            CREATE VIEW test_view WITH security_invoker = TRUE AS
                SELECT * FROM t;
        )",
        "a/b/c/test_view",
R"(PRAGMA TablePathPrefix = '/Root/a/b/c';

CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
SELECT
    *
FROM
    t
;
)"
    );
}

Y_UNIT_TEST(WithSingleQuotedTablePathPrefix) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE `a/b/c/t` (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(R"(
            -- the case of the pragma identifier does not matter, but is preserved
            pragma tabLEpathPRefix = '/Root/a/b';
            CREATE VIEW `../../test_view` WITH security_invoker = TRUE AS
                SELECT * FROM `c/t`;
        )",
        "test_view",
R"(-- the case of the pragma identifier does not matter, but is preserved
PRAGMA tabLEpathPRefix = '/Root/a/b';

CREATE VIEW `../../test_view` WITH (security_invoker = TRUE) AS
SELECT
    *
FROM
    `c/t`
;
)"
    );
}

Y_UNIT_TEST(WithPairedTablePathPrefix) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE `a/b/c/t` (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(R"(
            PRAGMA TablePathPrefix ("db", "/Root/a/b/c");
            CREATE VIEW `test_view` WITH security_invoker = TRUE AS
                SELECT * FROM t;
        )",
        "a/b/c/test_view",
R"(PRAGMA TablePathPrefix('db', '/Root/a/b/c');

CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
SELECT
    *
FROM
    t
;
)"
    );
}

Y_UNIT_TEST(WithTwoTablePathPrefixes) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE `some/other/folder/t` (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(R"(
            PRAGMA TablePathPrefix = "/Root/a/b/c";
            PRAGMA TablePathPrefix = "/Root/some/other/folder";
            CREATE VIEW `test_view` WITH security_invoker = TRUE AS
                SELECT * FROM t;
        )",
        "some/other/folder/test_view",
R"(PRAGMA TablePathPrefix = '/Root/a/b/c';
PRAGMA TablePathPrefix = '/Root/some/other/folder';

CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
SELECT
    *
FROM
    t
;
)"
    );
}

}

Y_UNIT_TEST_SUITE(ViewQuerySplit) {

Y_UNIT_TEST(Basic) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery("select 1", split, issues), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation, "");
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select 1");
}

Y_UNIT_TEST(WithPragmaTablePathPrefix) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery(
        "pragma tablepathprefix = \"/foo/bar\";\n"
        "select 1",
        split, issues
    ), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation, "pragma tablepathprefix = \"/foo/bar\";\n");
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select 1");
}

Y_UNIT_TEST(WithPairedPragmaTablePathPrefix) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery(
        "pragma tablepathprefix (\"foo\", \"/bar/baz\");\n"
        "select 1",
        split, issues
    ), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation, "pragma tablepathprefix (\"foo\", \"/bar/baz\");\n");
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select 1");
}

Y_UNIT_TEST(WithComments) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery(
        "-- what does the fox say?\n"
        "pragma tablepathprefix = \"/foo/bar\";\n"
        "select * from t",
        split, issues
    ), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation,
        "-- what does the fox say?\n"
        "pragma tablepathprefix = \"/foo/bar\";\n"
    );
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select * from t");
}

Y_UNIT_TEST(Joins) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery(
        "$x = \"/t\";\n"
        "$y = \"/tt\";\n"
        "select * from $x as x join $y as y on x.key == y.key",
        split, issues
    ), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation,
        "$x = \"/t\";\n"
        "$y = \"/tt\";\n"
    );
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select * from $x as x join $y as y on x.key == y.key");
}

}

} // NSysView
} // NKikimr
