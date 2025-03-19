#include "ut_common.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/yson/node/node_io.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

void CreateTenant(TTestEnv& env, const TString& tenantName, bool extSchemeShard = true) {
    auto subdomain = GetSubDomainDeclareSettings(tenantName);
    if (extSchemeShard) {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().CreateExtSubdomain("/Root", subdomain));
    } else {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().CreateSubdomain("/Root", subdomain));
    }

    env.GetTenants().Run("/Root/" + tenantName, 2);

    auto subdomainSettings = GetSubDomainDefaultSettings(tenantName, env.GetPools());
    subdomainSettings.SetExternalSysViewProcessor(true);

    if (extSchemeShard) {
        subdomainSettings.SetExternalSchemeShard(true);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
    } else {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterSubdomain("/Root", subdomainSettings));
    }
}

void CreateTenants(TTestEnv& env, bool extSchemeShard = true) {
    CreateTenant(env, "Tenant1", extSchemeShard);
    CreateTenant(env, "Tenant2", extSchemeShard);
}

void CreateTable(auto& session, const TString& name, ui64 partitionCount = 1) {
    auto desc = TTableBuilder()
        .AddNullableColumn("Key", EPrimitiveType::Uint64)
        .AddNullableColumn("Value", EPrimitiveType::String)
        .SetPrimaryKeyColumns({"Key"})
        .Build();

    auto settings = TCreateTableSettings();
    settings.PartitioningPolicy(TPartitioningPolicy().UniformPartitions(partitionCount));

    session.CreateTable(name, std::move(desc), std::move(settings)).GetValueSync();
}

void CreateTables(TTestEnv& env, ui64 partitionCount = 1) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    CreateTable(session, "Root/Table0", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Table0` (Key, Value) VALUES
            (0u, "Z");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());

    CreateTable(session, "Root/Tenant1/Table1", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Tenant1/Table1` (Key, Value) VALUES
            (1u, "A"),
            (2u, "B"),
            (3u, "C");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());

    CreateTable(session, "Root/Tenant2/Table2", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Tenant2/Table2` (Key, Value) VALUES
            (4u, "D"),
            (5u, "E");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());
}

void CreateTenantsAndTables(TTestEnv& env, bool extSchemeShard = true, ui64 partitionCount = 1) {
    CreateTenants(env, extSchemeShard);
    CreateTables(env, partitionCount);
}

void CreateRootTable(TTestEnv& env, ui64 partitionCount = 1, bool fillTable = false, ui16 tableNum = 0) {
    env.GetClient().CreateTable("/Root", Sprintf(R"(
        Name: "Table%u"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Value", Type: "String" }
        KeyColumnNames: ["Key"]
        UniformPartitionsCount: %lu
    )", tableNum, partitionCount));

    if (fillTable) {
        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
            REPLACE INTO `Root/Table0` (Key, Value) VALUES
                (0u, "X"),
                (1u, "Y"),
                (2u, "Z");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());
    }
}

void SetupAuthEnvironment(TTestEnv& env) {
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_TRACE);
    CreateTenantsAndTables(env, true);
}

void SetupAuthAccessEnvironment(TTestEnv& env) {
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back("root@builtin");
    env.GetServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back("user1rootadmin");
    env.GetServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableDatabaseAdmin(true);
    env.GetClient().SetSecurityToken("root@builtin");
    CreateTenantsAndTables(env, true);

    env.GetClient().CreateUser("/Root", "user1rootadmin", "password1");
    env.GetClient().CreateUser("/Root", "user2", "password2");
    env.GetClient().CreateUser("/Root/Tenant1", "user3", "password3");
    env.GetClient().CreateUser("/Root/Tenant1", "user4", "password4");
    env.GetClient().CreateUser("/Root/Tenant2", "user5", "password5");

    // Note: in real scenarios user6tenant1admin should be created in /Root/Tenant1
    // but it isn't supported by test framework
    env.GetClient().CreateUser("/Root", "user6tenant1admin", "password6");
    env.GetClient().ModifyOwner("/Root", "Tenant1", "user6tenant1admin");

    {
        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1rootadmin");
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user2");
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user6tenant1admin");
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, "root@builtin");
        env.GetClient().ModifyACL("", "Root", acl.SerializeAsString());
    }
}

void CheckAuthAdministratorAccessIsRequired(TScanQueryPartIterator& it) {
    NKqp::StreamResultToYson(it, false, EStatus::UNAUTHORIZED,
        "Administrator access is required");
}

void CheckEmpty(TScanQueryPartIterator& it) {
    auto expected = R"([

    ])";
    NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
}

class TShowCreateTableChecker {
public:

    explicit TShowCreateTableChecker(TTestEnv& env)
        : Env(env)
        , QueryClient(NQuery::TQueryClient(Env.GetDriver()))
        , TableClient(TTableClient(Env.GetDriver()))
    {}

    void CheckShowCreateTable(const std::string& query, const std::string& tableName, TString formatQuery = "", bool temporary = false) {
        auto session = QueryClient.GetSession().GetValueSync().GetSession();

        std::optional<TString> sessionId = std::nullopt;
        if (temporary) {
            sessionId = session.GetId();
        }

        CreateTable(session, query);
        auto showCreateTableQuery = ShowCreateTable(session, tableName);

        if (formatQuery) {
            UNIT_ASSERT_VALUES_EQUAL_C(UnescapeC(formatQuery), UnescapeC(showCreateTableQuery), UnescapeC(showCreateTableQuery));
        }

        auto tableDescOrig = DescribeTable(tableName, sessionId);

        DropTable(session, tableName);

        CreateTable(session, showCreateTableQuery);
        auto tableDescNew = DescribeTable(tableName, sessionId);

        DropTable(session, tableName);

        CompareDescriptions(std::move(tableDescOrig), std::move(tableDescNew), showCreateTableQuery);
    }

private:

    void CreateTable(NYdb::NQuery::TSession& session, const std::string& query) {
        auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    NKikimrSchemeOp::TTableDescription DescribeTable(const std::string& tableName,
            std::optional<TString> sessionId = std::nullopt) {

        auto describeTable = [this](const TString& path) {
            auto& runtime = *(this->Env.GetServer().GetRuntime());
            auto sender = runtime.AllocateEdgeActor();
            TAutoPtr<IEventHandle> handle;

            auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            request->Record.MutableDescribePath()->SetPath(path);
            request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
            request->Record.MutableDescribePath()->MutableOptions()->SetReturnBoundaries(true);
            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle);

            return reply->GetRecord().GetPathDescription().GetTable();
        };

        TString tablePath = TString(tableName);
        if (!IsStartWithSlash(tablePath)) {
            tablePath = CanonizePath(JoinPath({"/Root", tablePath}));
        }
        if (sessionId.has_value()) {
            auto pos = sessionId.value().find("&id=");
            tablePath = NKqp::GetTempTablePath("Root", sessionId.value().substr(pos + 4), tablePath);
        }
        auto tableDesc = describeTable(tablePath);

        return tableDesc;
    }

    std::string ShowCreateTable(NYdb::NQuery::TSession& session, const std::string& tableName) {
        auto result = session.ExecuteQuery(TStringBuilder() << R"(
            SHOW CREATE TABLE `)" << tableName << R"(`;
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        auto resultSet = result.GetResultSet(0);
        auto columnsMeta = resultSet.GetColumnsMeta();
        UNIT_ASSERT(columnsMeta.size() == 3);

        NYdb::TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());

        TString tablePath = TString(tableName);

        TString statement = "";

        for (size_t i = 0; i < columnsMeta.size(); i++) {
            const auto& column = columnsMeta[i];
            if (column.Name == "Path") {
                TValueParser parserValue(parser.GetValue(i));
                parserValue.OpenOptional();
                UNIT_ASSERT_VALUES_EQUAL(parserValue.GetUtf8(), std::string(tablePath));
                continue;
            } else if (column.Name == "PathType") {
                TValueParser parserValue(parser.GetValue(i));
                parserValue.OpenOptional();
                UNIT_ASSERT_VALUES_EQUAL(parserValue.GetUtf8(), "Table");
                continue;
            } else if (column.Name == "Statement") {
                TValueParser parserValue(parser.GetValue(i));
                parserValue.OpenOptional();
                statement = parserValue.GetUtf8();
            } else {
                UNIT_ASSERT_C(false, "Invalid column name");
            }
        }
        UNIT_ASSERT(statement);

        return statement;
    }

    void DropTable(NYdb::NQuery::TSession& session, const std::string& tableName) {
        auto result = session.ExecuteQuery(TStringBuilder() << R"(
            DROP TABLE `)" << tableName << R"(`;
        )",  NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void CompareDescriptions(NKikimrSchemeOp::TTableDescription origDesc, NKikimrSchemeOp::TTableDescription newDesc, const std::string& showCreateTableQuery) {
        Ydb::Table::CreateTableRequest requestFirst = *GetCreateTableRequest(origDesc);
        Ydb::Table::CreateTableRequest requestSecond = *GetCreateTableRequest(newDesc);

        TString first;
        ::google::protobuf::TextFormat::PrintToString(requestFirst, &first);
        TString second;
        ::google::protobuf::TextFormat::PrintToString(requestSecond, &second);

        UNIT_ASSERT_VALUES_EQUAL_C(first, second, showCreateTableQuery);
    }

    TMaybe<Ydb::Table::CreateTableRequest> GetCreateTableRequest(const NKikimrSchemeOp::TTableDescription& tableDesc) {
        Ydb::Table::CreateTableRequest scheme;

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

private:
    TTestEnv& Env;
    NQuery::TQueryClient QueryClient;
    TTableClient TableClient;
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
        CreateTenantsAndTables(env, false);
        TTableClient client(env.GetDriver());
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(R"([
                [[4u];[0u];["/Root/Table0"]]
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant1/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(R"([
                [[9u];[0u];["/Root/Tenant1/Table1"]]
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant2/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(R"([
                [[10u];[0u];["/Root/Tenant2/Table2"]]
            ])", NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(PartitionStatsOneSchemeShardDataQuery) {
        TTestEnv env;
        CreateTenantsAndTables(env, false);

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[4u];[0u];["/Root/Table0"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant1/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT(result.IsSuccess());
            NKqp::CompareYson(R"([
                [[9u];[0u];["/Root/Tenant1/Table1"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant2/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT(result.IsSuccess());
            NKqp::CompareYson(R"([
                [[10u];[0u];["/Root/Tenant2/Table2"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
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

        TShowCreateTableChecker checker(env);

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

        TShowCreateTableChecker checker(env);

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
)",
        true);
    }

    Y_UNIT_TEST(ShowCreateTablePartitionSettings) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateTableChecker checker(env);

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

        TShowCreateTableChecker checker(env);

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

        TShowCreateTableChecker checker(env);

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

        TShowCreateTableChecker checker(env);

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
    }

    Y_UNIT_TEST(ShowCreateTableTemporary) {
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateTableChecker checker(env);

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
        TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TShowCreateTableChecker checker(env);

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

    Y_UNIT_TEST(PartitionStatsFields) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env;
        CreateRootTable(env);

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
                FollowerId,
                UpdateTime
            FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);
        TYsonFieldChecker check(ysonString, 24);

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
        check.Uint64(2u); // PathId
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
        check.Uint64(0u); // FollowerId
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
                    Guid,
                    Kind,
                    NodeId,
                    PDiskId,
                    Path,
                    ReadCentric,
                    SharedWithOS,
                    State,
                    Status,
                    StatusChangeTimestamp,
                    TotalSize,
                    Type,
                    ExpectedSlotCount,
                    NumActiveSlots,
                    DecommitStatus
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

        TYsonFieldChecker check(ysonString, 17);

        check.Uint64(0u); // AvailableSize
        check.Uint64(999u); // BoxId
        check.Uint64(123u); // Guid
        check.Uint64(0u); // Kind
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(1u); // PDiskId
        check.StringContains("pdisk_1.dat"); // Path
        check.Bool(false); // ReadCentric
        check.Bool(false); // SharedWithOS
        check.String("Initial"); // State
        check.String("ACTIVE"); // Status
        check.Null(); // StatusChangeTimestamp
        check.Uint64(0u); // TotalSize
        check.String("ROT"); // Type
        check.Uint64(16); // ExpectedSlotCount
        check.Uint64(2); // NumActiveSlots
        check.String("DECOMMIT_NONE"); // DecommitStatus
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
                    LifeCyclePhase,
                    PutTabletLogLatency,
                    PutUserDataLatency,
                    StoragePoolId,
                    LayoutCorrect
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

        TYsonFieldChecker check(ysonString, 13);

        check.Uint64(0u); // AllocatedSize
        check.Uint64GreaterOrEquals(0u); // AvailableSize
        check.Uint64(999u); // BoxId
        check.Uint64(0u); // EncryptionMode
        check.String("none"); // ErasureSpecies
        check.Uint64(1u); // Generation
        check.Null(); // GetFastLatency
        check.Uint64(2181038080u); // GroupId
        check.Uint64(0u); // LifeCyclePhase
        check.Null(); // PutTabletLogLatency
        check.Null(); // PutUserDataLatency
        check.Uint64(2u); // StoragePoolId
        check.Bool(true); // LayoutCorrect
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

        TYsonFieldChecker check(ysonString, 11);

        check.Uint64(999u); // BoxId
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

    Y_UNIT_TEST(TopPartitionsFields) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env(1, 4, {.EnableSVP = true});
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

    Y_UNIT_TEST(TopPartitionsTables) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        constexpr ui64 partitionCount = 5;

        TTestEnv env(1, 4, {.EnableSVP = true});
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

    Y_UNIT_TEST(TopPartitionsRanges) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        constexpr ui64 partitionCount = 5;

        TTestEnv env(1, 4, {.EnableSVP = true});
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

    Y_UNIT_TEST(TopPartitionsFollowers) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env(1, 4, {.EnableSVP = true, .EnableForceFollowers = true});

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

    Y_UNIT_TEST(Describe) {
        TTestEnv env;
        CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            auto settings = TDescribeTableSettings()
                .WithKeyShardBoundary(true)
                .WithTableStatistics(true)
                .WithPartitionStatistics(true);

            auto result = session.DescribeTable("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const auto& table = result.GetTableDescription();
            const auto& columns = table.GetTableColumns();
            const auto& keyColumns = table.GetPrimaryKeyColumns();

            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 27);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0].Name, "OwnerId");
            UNIT_ASSERT_STRINGS_EQUAL(FormatType(columns[0].Type), "Uint64?");

            UNIT_ASSERT_VALUES_EQUAL(keyColumns.size(), 4);
            UNIT_ASSERT_STRINGS_EQUAL(keyColumns[0], "OwnerId");

            UNIT_ASSERT_VALUES_EQUAL(table.GetPartitionStats().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.GetPartitionsCount(), 0);
        }

        TSchemeClient schemeClient(env.GetDriver());

        {
            auto result = schemeClient.DescribePath("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "partition_stats");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Table);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "partition_stats");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Table);
        }
    }

    Y_UNIT_TEST(SystemViewFailOps) {
        TTestEnv env;

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            auto desc = TTableBuilder()
                .AddNullableColumn("Column1", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumn("Column1")
                .Build();

            auto result = session.CreateTable("/Root/.sys/partition_stats", std::move(desc)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = session.CopyTable("/Root/.sys/partition_stats", "/Root/Table0").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto settings = TAlterTableSettings()
                .AppendDropColumns("OwnerId");

            auto result = session.AlterTable("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = session.DropTable("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = session.ExecuteSchemeQuery(R"(
                DROP TABLE `/Root/.sys/partition_stats`;
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = session.ReadTable("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TReadTableResultPart streamPart = result.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SCHEME_ERROR);
            streamPart.GetIssues().PrintTo(Cerr);
        }
        {
            TValueBuilder rows;
            rows.BeginList().EndList();
            auto result = client.BulkUpsert("/Root/.sys/partition_stats", rows.Build()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }

        TSchemeClient schemeClient(env.GetDriver());

        {
            auto result = schemeClient.MakeDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.MakeDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.RemoveDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.RemoveDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            TModifyPermissionsSettings settings;
            auto result = schemeClient.ModifyPermissions("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
    }

    Y_UNIT_TEST(DescribeSystemFolder) {
        TTestEnv env;
        CreateTenantsAndTables(env, false);

        TSchemeClient schemeClient(env.GetDriver());
        {
            auto result = schemeClient.ListDirectory("/Root").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_STRINGS_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 5);
            UNIT_ASSERT_STRINGS_EQUAL(children[0].Name, ".metadata");
            UNIT_ASSERT_STRINGS_EQUAL(children[1].Name, "Table0");
            UNIT_ASSERT_STRINGS_EQUAL(children[2].Name, "Tenant1");
            UNIT_ASSERT_STRINGS_EQUAL(children[3].Name, "Tenant2");
            UNIT_ASSERT_STRINGS_EQUAL(children[4].Name, ".sys");
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "Tenant1");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::SubDomain);

            auto children = result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(children[0].Name, "Table1");
            UNIT_ASSERT_STRINGS_EQUAL(children[1].Name, ".sys");
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();

            UNIT_ASSERT_VALUES_EQUAL(children.size(), 31);

            THashSet<TString> names;
            for (const auto& child : children) {
                names.insert(TString{child.Name});
                UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::Table);
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

            UNIT_ASSERT_VALUES_EQUAL(children.size(), 25);

            THashSet<TString> names;
            for (const auto& child : children) {
                names.insert(TString{child.Name});
                UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::Table);
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
            WHERE TabletId <= 72075186224037888ul OR TabletId >= 72075186224037890ul;
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

    // TODO: make a test when tenant support is provided
    void QueryMetricsSimple() {
        TTestEnv env(1, 2);
        CreateTenant(env, "Tenant1", true);
        {
            TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();

            NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
                CREATE TABLE `Root/Tenant1/Table1` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync());
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "SELECT * FROM `Root/Tenant1/Table1`", TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT SumReadBytes FROM `Root/Tenant1/.sys/query_metrics`;
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

        NKqp::CompareYson(R"([
            [[0u]];
        ])", ysonString);
    }

    Y_UNIT_TEST(AuthUsers) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
        env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
        env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root/Tenant1", "group2");
        env.GetClient().CreateGroup("/Root/Tenant2", "group3");
        env.GetClient().CreateGroup("/Root/Tenant2", "group4");

        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root").DebugString() << Endl;

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, LastSuccessfulAttemptAt, LastFailedAttemptAt, FailedAttemptCount
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user1"];[%true];[%false];#;#;[0u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PasswordHash
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto actual = NKqp::StreamResultToYson(it);
            UNIT_ASSERT_STRING_CONTAINS(actual, "hash");
            UNIT_ASSERT_STRING_CONTAINS(actual, "salt");
            UNIT_ASSERT_STRING_CONTAINS(actual, "type");
            UNIT_ASSERT_STRING_CONTAINS(actual, "argon2id");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, LastSuccessfulAttemptAt, LastFailedAttemptAt, FailedAttemptCount
                FROM `Root/Tenant1/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user2"];[%true];[%false];#;#;[0u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, LastSuccessfulAttemptAt, LastFailedAttemptAt, FailedAttemptCount
                FROM `Root/Tenant2/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user3"];[%true];[%false];#;#;[0u]];
                [["user4"];[%true];[%false];#;#;[0u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthUsers_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);
        TTableClient client(env.GetDriver());

        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant1").DebugString() << Endl;

        { // anonymous login doesn't give administrative access as `AdministrationAllowedSIDs` isn't empty
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint());
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }
        }

        { // user1rootadmin is /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user1rootadmin"]];
                    [["user2"]];
                    [["user6tenant1admin"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user3"]];
                    [["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user5"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // user2 isn't /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user2",
                    .Password = "password2",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user2"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }
        }

        { // user6tenant1admin is /Root/Tenant1 admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user6tenant1admin",
                    .Password = "password6",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user6tenant1admin"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user3"]];
                    [["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }
        }
    }

    Y_UNIT_TEST(AuthUsers_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto user : {
            "user3",
            "user1",
            "user2",
            "user",
            "user33",
            "user21",
            "user22",
            "userrr",
            "u",
            "asdf",
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT Sid
            FROM `Root/.sys/auth_users`
        )").GetValueSync();

        auto expected = R"([
            [["asdf"]];
            [["u"]];
            [["user"]];
            [["user1"]];
            [["user2"]];
            [["user21"]];
            [["user22"]];
            [["user3"]];
            [["user33"]];
            [["userrr"]];
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(AuthUsers_TableRange) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto user : {
            "user1",
            "user2",
            "user3",
            "user4"
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user1"]];
                [["user2"]];
                [["user3"]];
                [["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid >= "user2"
            )").GetValueSync();

            auto expected = R"([
                [["user2"]];
                [["user3"]];
                [["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid > "user2"
            )").GetValueSync();

            auto expected = R"([
                [["user3"]];
                [["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid <= "user3"
            )").GetValueSync();

            auto expected = R"([
                [["user1"]];
                [["user2"]];
                [["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid < "user3"
            )").GetValueSync();

            auto expected = R"([
                [["user1"]];
                [["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid > "user1" AND Sid <= "user3"
            )").GetValueSync();

            auto expected = R"([
                [["user2"]];
                [["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid >= "user2" AND Sid < "user3"
            )").GetValueSync();

            auto expected = R"([
                [["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthGroups) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
        env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
        env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root/Tenant1", "group2");
        env.GetClient().CreateGroup("/Root/Tenant2", "group3");
        env.GetClient().CreateGroup("/Root/Tenant2", "group4");

        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root").DebugString() << Endl;

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_groups`
            )").GetValueSync();

            auto expected = R"([
                [["group1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant1/.sys/auth_groups`
            )").GetValueSync();

            auto expected = R"([
                [["group2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant2/.sys/auth_groups`
            )").GetValueSync();

            auto expected = R"([
                [["group3"]];
                [["group4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthGroups_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root", "group2");
        env.GetClient().CreateGroup("/Root/Tenant1", "group3");
        env.GetClient().CreateGroup("/Root/Tenant1", "group4");
        env.GetClient().CreateGroup("/Root/Tenant2", "group5");

        { // anonymous login doesn't give administrative access as `AdministrationAllowedSIDs` isn't empty
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint());
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }

        { // user1rootadmin is /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_groups`
                )").GetValueSync();

                auto expected = R"([
                    [["group1"]];
                    [["group2"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_groups`
                )").GetValueSync();

                auto expected = R"([
                    [["group3"]];
                    [["group4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_groups`
                )").GetValueSync();

                auto expected = R"([
                    [["group5"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // user2 isn't /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user2",
                    .Password = "password2",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }

        { // user6tenant1admin is /Root/Tenant1 admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user6tenant1admin",
                    .Password = "password6",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_groups`
                )").GetValueSync();

                auto expected = R"([
                    [["group3"]];
                    [["group4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }
    }

    Y_UNIT_TEST(AuthGroups_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto group : {
            "group3",
            "group1",
            "group2",
            "group",
            "group33",
            "group21",
            "group22",
            "grouprr",
            "g",
            "asdf",
        }) {
            env.GetClient().CreateGroup("/Root", group);
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `Root/.sys/auth_groups`
        )").GetValueSync();

        auto expected = R"([
            [["asdf"]];
            [["g"]];
            [["group"]];
            [["group1"]];
            [["group2"]];
            [["group21"]];
            [["group22"]];
            [["group3"]];
            [["group33"]];
            [["grouprr"]];
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(AuthGroups_TableRange) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto group : {
            "group1",
            "group2",
            "group3",
            "group4",
        }) {
            env.GetClient().CreateGroup("/Root", group);
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_groups`
                WHERE Sid > "group1" AND Sid <= "group3"
            )").GetValueSync();

            auto expected = R"([
                [["group2"]];
                [["group3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthGroupMembers) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
        env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
        env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root/Tenant1", "group2");
        env.GetClient().CreateGroup("/Root/Tenant2", "group3");
        env.GetClient().CreateGroup("/Root/Tenant2", "group4");
        env.GetClient().CreateGroup("/Root/Tenant2", "group5");

        env.GetClient().AddGroupMembership("/Root", "group1", "user1");
        env.GetClient().AddGroupMembership("/Root/Tenant1", "group2", "user2");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group3", "user4");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group4", "user3");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group4", "user4");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group4", "group3");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group4", "group4");

        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root").DebugString() << Endl;
        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant2").DebugString() << Endl;

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
            )").GetValueSync();

            auto expected = R"([
                [["group1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant1/.sys/auth_group_members`
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant2/.sys/auth_group_members`
            )").GetValueSync();

            auto expected = R"([
                [["group3"];["user4"]];
                [["group4"];["group3"]];
                [["group4"];["group4"]];
                [["group4"];["user3"]];
                [["group4"];["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthGroupMembers_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root", "group2");
        env.GetClient().CreateGroup("/Root/Tenant1", "group3");
        env.GetClient().CreateGroup("/Root/Tenant1", "group4");
        env.GetClient().CreateGroup("/Root/Tenant2", "group5");

        env.GetClient().AddGroupMembership("/Root", "group1", "user1rootadmin");
        env.GetClient().AddGroupMembership("/Root", "group2", "user2");
        env.GetClient().AddGroupMembership("/Root/Tenant1", "group3", "user3");
        env.GetClient().AddGroupMembership("/Root/Tenant1", "group4", "user4");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group5", "user5");

        { // anonymous login doesn't give administrative access as `AdministrationAllowedSIDs` isn't empty
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint());
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant2/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }

        { // user1rootadmin is /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_group_members`
                )").GetValueSync();

                auto expected = R"([
                    [["group1"];["user1rootadmin"]];
                    [["group2"];["user2"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_group_members`
                )").GetValueSync();

                auto expected = R"([
                    [["group3"];["user3"]];
                    [["group4"];["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant2/.sys/auth_group_members`
                )").GetValueSync();

                auto expected = R"([
                    [["group5"];["user5"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // user2 isn't /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user2",
                    .Password = "password2",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant2/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }

        { // user6tenant1admin is /Root/Tenant1 admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user6tenant1admin",
                    .Password = "password6",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_group_members`
                )").GetValueSync();

                auto expected = R"([
                    [["group3"];["user3"]];
                    [["group4"];["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant2/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }
    }

    Y_UNIT_TEST(AuthGroupMembers_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto group : {
            "group3",
            "group1",
            "group2",
            "group",
        }) {
            env.GetClient().CreateGroup("/Root", group);
        }

        for (auto user : {
            "user1",
            "user2",
            "user"
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        for (auto membership : TVector<std::pair<TString, TString>>{
            {"group3", "user1"},
            {"group3", "user2"},
            {"group2", "user"},
            {"group2", "user1"},
            {"group2", "user2"},
            {"group", "user2"},
        }) {
            env.GetClient().AddGroupMembership("/Root", membership.first, membership.second);
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `Root/.sys/auth_group_members`
        )").GetValueSync();

        auto expected = R"([
            [["group"];["user2"]];
            [["group2"];["user"]];
            [["group2"];["user1"]];
            [["group2"];["user2"]];
            [["group3"];["user1"]];
            [["group3"];["user2"]];
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(AuthGroupMembers_TableRange) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto group : {
            "group1",
            "group2",
            "group3",
        }) {
            env.GetClient().CreateGroup("/Root", group);
        }

        for (auto user : {
            "user1",
            "user2",
            "user3"
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        for (auto membership : TVector<std::pair<TString, TString>>{
            {"group1", "user1"},
            {"group1", "user2"},
            {"group2", "user1"},
            {"group2", "user2"},
            {"group2", "user3"},
            {"group3", "user1"},
            {"group3", "user2"},
        }) {
            env.GetClient().AddGroupMembership("/Root", membership.first, membership.second);
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
            )").GetValueSync();

            auto expected = R"([
                [["group1"];["user1"]];
                [["group1"];["user2"]];
                [["group2"];["user1"]];
                [["group2"];["user2"]];
                [["group2"];["user3"]];
                [["group3"];["user1"]];
                [["group3"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid > "group1" AND GroupSid <= "group3"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user1"]];
                [["group2"];["user2"]];
                [["group2"];["user3"]];
                [["group3"];["user1"]];
                [["group3"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid >= "group2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user1"]];
                [["group2"];["user2"]];
                [["group2"];["user3"]];
                [["group3"];["user1"]];
                [["group3"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid > "group2"
            )").GetValueSync();

            auto expected = R"([
                [["group3"];["user1"]];
                [["group3"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid <= "group2"
            )").GetValueSync();

            auto expected = R"([
                [["group1"];["user1"]];
                [["group1"];["user2"]];
                [["group2"];["user1"]];
                [["group2"];["user2"]];
                [["group2"];["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid < "group2"
            )").GetValueSync();

            auto expected = R"([
                [["group1"];["user1"]];
                [["group1"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid = "group2" AND MemberSid >= "user2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user2"]];
                [["group2"];["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid = "group2" AND MemberSid > "user2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid = "group2" AND MemberSid <= "user2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user1"]];
                [["group2"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid = "group2" AND MemberSid < "user2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthOwners) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
        env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
        env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
        env.GetClient().CreateGroup("/Root/Tenant2", "group1");

        env.GetClient().MkDir("/Root", "Dir1/SubDir1");
        env.GetClient().ModifyOwner("/Root", "Dir1", "user1");
        env.GetClient().ModifyOwner("/Root/Dir1", "SubDir1", "user1");

        env.GetClient().MkDir("/Root/Tenant1", "Dir2/SubDir2");
        env.GetClient().ModifyOwner("/Root/Tenant1", "Dir2", "user2");
        env.GetClient().ModifyOwner("/Root/Tenant1/Dir2", "SubDir2", "user2");

        env.GetClient().MkDir("/Root/Tenant2", "Dir3/SubDir33");
        env.GetClient().MkDir("/Root/Tenant2", "Dir3/SubDir34");
        env.GetClient().MkDir("/Root/Tenant2", "Dir4/SubDir45");
        env.GetClient().MkDir("/Root/Tenant2", "Dir4/SubDir46");
        env.GetClient().ModifyOwner("/Root/Tenant2", "Dir3", "user3");
        env.GetClient().ModifyOwner("/Root/Tenant2", "Dir4", "user4");
        env.GetClient().ModifyOwner("/Root/Tenant2/Dir3", "SubDir33", "group1");
        env.GetClient().ModifyOwner("/Root/Tenant2/Dir4", "SubDir46", "user4");

        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root").DebugString() << Endl;
        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant2").DebugString() << Endl;

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["root@builtin"]];
                [["/Root/.metadata"];["metadata@system"]];
                [["/Root/.metadata/workload_manager"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                [["/Root/Dir1"];["user1"]];
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Table0"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant1/.sys/auth_owners`
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Tenant1"];["root@builtin"]];
                [["/Root/Tenant1/Dir2"];["user2"]];
                [["/Root/Tenant1/Dir2/SubDir2"];["user2"]];
                [["/Root/Tenant1/Table1"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant2/.sys/auth_owners`
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Tenant2"];["root@builtin"]];
                [["/Root/Tenant2/Dir3"];["user3"]];
                [["/Root/Tenant2/Dir3/SubDir33"];["group1"]];
                [["/Root/Tenant2/Dir3/SubDir34"];["root@builtin"]];
                [["/Root/Tenant2/Dir4"];["user4"]];
                [["/Root/Tenant2/Dir4/SubDir45"];["root@builtin"]];
                [["/Root/Tenant2/Dir4/SubDir46"];["user4"]];
                [["/Root/Tenant2/Table2"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthOwners_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().MkDir("/Root", "Dir1");
        env.GetClient().MkDir("/Root", "Dir2");
        env.GetClient().MkDir("/Root/Tenant1", "Dir3");
        env.GetClient().MkDir("/Root/Tenant1", "Dir4");
        env.GetClient().ModifyOwner("/Root", "Dir1", "user1rootadmin");
        env.GetClient().ModifyOwner("/Root/Tenant1", "Dir3", "user3");

        { // anonymous login gives `ydb.granular.describe_schema` access
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint());
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["root@builtin"]];
                [["/Root/.metadata"];["metadata@system"]];
                [["/Root/.metadata/workload_manager"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                [["/Root/Dir1"];["user1rootadmin"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Table0"];["root@builtin"]]
            ])";
            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        { // user1rootadmin has /Root GenericUse access
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_owners`
                )").GetValueSync();

                auto expected = R"([
                    [["/Root"];["root@builtin"]];
                    [["/Root/.metadata"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/Dir1"];["user1rootadmin"]];
                    [["/Root/Dir2"];["root@builtin"]];
                    [["/Root/Table0"];["root@builtin"]]
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_owners`
                )").GetValueSync();

                auto expected = R"([
                    [["/Root/Tenant1"];["user6tenant1admin"]];
                    [["/Root/Tenant1/Dir3"];["user3"]];
                    [["/Root/Tenant1/Dir4"];["root@builtin"]];
                    [["/Root/Tenant1/Table1"];["root@builtin"]]
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // revoke user1rootadmin /Root/Dir2 GenericUse access
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "user1rootadmin");
            env.GetClient().ModifyACL("/Root", "Dir2", acl.SerializeAsString());

            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["root@builtin"]];
                [["/Root/.metadata"];["metadata@system"]];
                [["/Root/.metadata/workload_manager"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                [["/Root/Dir1"];["user1rootadmin"]];
                [["/Root/Table0"];["root@builtin"]]
            ])";
            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthOwners_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto path : {
            "Dir2/SubDir2",
            "Dir1/SubDir1",
            "Dir2/SubDir1",
            "Dir1/SubDir2",
            "Dir2/SubDir3",
            "Dir1/SubDir3",
            "Dir11/SubDir",
            "Dir/SubDir",
        }) {
            env.GetClient().MkDir("/Root", path);
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `Root/.sys/auth_owners`
        )").GetValueSync();

        auto expected = R"([
            [["/Root"];["root@builtin"]];
            [["/Root/.metadata"];["metadata@system"]];
            [["/Root/.metadata/workload_manager"];["metadata@system"]];
            [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
            [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
            [["/Root/Dir"];["root@builtin"]];
            [["/Root/Dir/SubDir"];["root@builtin"]];
            [["/Root/Dir1"];["root@builtin"]];
            [["/Root/Dir1/SubDir1"];["root@builtin"]];
            [["/Root/Dir1/SubDir2"];["root@builtin"]];
            [["/Root/Dir1/SubDir3"];["root@builtin"]];
            [["/Root/Dir11"];["root@builtin"]];
            [["/Root/Dir11/SubDir"];["root@builtin"]];
            [["/Root/Dir2"];["root@builtin"]];
            [["/Root/Dir2/SubDir1"];["root@builtin"]];
            [["/Root/Dir2/SubDir2"];["root@builtin"]];
            [["/Root/Dir2/SubDir3"];["root@builtin"]];
            [["/Root/Table0"];["root@builtin"]]
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(AuthOwners_TableRange) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto path : {
            "Dir0/SubDir0",
            "Dir0/SubDir1",
            "Dir0/SubDir2",
            "Dir1/SubDir0",
            "Dir1/SubDir1",
            "Dir1/SubDir2",
            "Dir2/SubDir0",
            "Dir2/SubDir1",
            "Dir2/SubDir2",
            "Dir3/SubDir0",
            "Dir3/SubDir1",
            "Dir3/SubDir2",
        }) {
            env.GetClient().MkDir("/Root", path);
        }
        env.GetClient().CreateUser("/Root", "user0", "password0");
        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root", "user2", "password2");
        env.GetClient().ModifyOwner("/Root/Dir1", "SubDir0", "user0");
        env.GetClient().ModifyOwner("/Root/Dir1", "SubDir1", "user1");
        env.GetClient().ModifyOwner("/Root/Dir1", "SubDir2", "user2");

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["root@builtin"]];
                [["/Root/.metadata"];["metadata@system"]];
                [["/Root/.metadata/workload_manager"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                [["/Root/Dir0"];["root@builtin"]];
                [["/Root/Dir0/SubDir0"];["root@builtin"]];
                [["/Root/Dir0/SubDir1"];["root@builtin"]];
                [["/Root/Dir0/SubDir2"];["root@builtin"]];
                [["/Root/Dir1"];["root@builtin"]];
                [["/Root/Dir1/SubDir0"];["user0"]];
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
                [["/Root/Dir2/SubDir1"];["root@builtin"]];
                [["/Root/Dir2/SubDir2"];["root@builtin"]];
                [["/Root/Dir3"];["root@builtin"]];
                [["/Root/Dir3/SubDir0"];["root@builtin"]];
                [["/Root/Dir3/SubDir1"];["root@builtin"]];
                [["/Root/Dir3/SubDir2"];["root@builtin"]];
                [["/Root/Table0"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path >= "/A" AND Path <= "/Z"
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["root@builtin"]];
                [["/Root/.metadata"];["metadata@system"]];
                [["/Root/.metadata/workload_manager"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                [["/Root/Dir0"];["root@builtin"]];
                [["/Root/Dir0/SubDir0"];["root@builtin"]];
                [["/Root/Dir0/SubDir1"];["root@builtin"]];
                [["/Root/Dir0/SubDir2"];["root@builtin"]];
                [["/Root/Dir1"];["root@builtin"]];
                [["/Root/Dir1/SubDir0"];["user0"]];
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
                [["/Root/Dir2/SubDir1"];["root@builtin"]];
                [["/Root/Dir2/SubDir2"];["root@builtin"]];
                [["/Root/Dir3"];["root@builtin"]];
                [["/Root/Dir3/SubDir0"];["root@builtin"]];
                [["/Root/Dir3/SubDir1"];["root@builtin"]];
                [["/Root/Dir3/SubDir2"];["root@builtin"]];
                [["/Root/Table0"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path >= "/Root/Dir1" AND Path < "/Root/Dir3"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1"];["root@builtin"]];
                [["/Root/Dir1/SubDir0"];["user0"]];
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
                [["/Root/Dir2/SubDir1"];["root@builtin"]];
                [["/Root/Dir2/SubDir2"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path >= "/Root/Dir1/SubDir1" AND Path <= "/Root/Dir2/SubDir1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
                [["/Root/Dir2/SubDir1"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path > "/Root/Dir1/SubDir1" AND Path < "/Root/Dir2/SubDir1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path >= "/Root/Dir1/SubDir0" AND Sid >= "user1" AND Path < "/Root/Dir2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid > "user0"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid < "user2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid >= "user1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid <= "user1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid > "user1"
            )").GetValueSync();

            auto expected = R"([

            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid < "user1"
            )").GetValueSync();

            auto expected = R"([

            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid = "user1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, Path
                FROM `Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid >= "user1"
            )").GetValueSync();

            auto expected = R"([
                [["user1"];["/Root/Dir1/SubDir1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthPermissions) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
        env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
        env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
        env.GetClient().CreateGroup("/Root/Tenant2", "group1");

        env.GetClient().MkDir("/Root", "Dir1/SubDir1");
        env.GetClient().MkDir("/Root/Tenant1", "Dir2/SubDir2");
        env.GetClient().MkDir("/Root/Tenant2", "Dir3/SubDir3");
        env.GetClient().MkDir("/Root/Tenant2", "Dir4/SubDir4");

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
            env.GetClient().ModifyACL("/", "Root", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root", "Dir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1");
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EraseRow, "user1");
            env.GetClient().ModifyACL("/Root/Dir1", "SubDir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Deny, NACLib::UpdateRow, "user1");
            env.GetClient().ModifyACL("/Root/Dir1", "SubDir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user2");
            env.GetClient().ModifyACL("/Root", "Tenant1", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root/Tenant1/Dir2", "SubDir2", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user3");
            env.GetClient().ModifyACL("/Root", "Tenant2", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root/Tenant2", "Dir3", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user4");
            env.GetClient().ModifyACL("/Root/Tenant2/Dir4", "SubDir4", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "group1");
            env.GetClient().ModifyACL("/Root/Tenant2", "Dir4", acl.SerializeAsString());
        }

        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant2/Dir4").DebugString() << Endl;

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["ydb.generic.use"];["user1"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                [["/Root/Dir1"];["ydb.generic.use"];["user1"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user1"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant1/.sys/auth_permissions`
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Tenant1"];["ydb.generic.use"];["user2"]];
                [["/Root/Tenant1/Dir2/SubDir2"];["ydb.generic.use"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant2/.sys/auth_permissions`
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Tenant2"];["ydb.generic.use"];["user3"]];
                [["/Root/Tenant2/Dir3"];["ydb.generic.use"];["user3"]];
                [["/Root/Tenant2/Dir4"];["ydb.generic.use"];["group1"]];
                [["/Root/Tenant2/Dir4/SubDir4"];["ydb.generic.use"];["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthPermissions_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().MkDir("/Root", "Dir1");
        env.GetClient().MkDir("/Root", "Dir2");
        env.GetClient().MkDir("/Root/Tenant1", "Dir3");
        env.GetClient().MkDir("/Root/Tenant1", "Dir4");

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1rootadmin");
            env.GetClient().ModifyACL("/Root", "Dir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EraseRow, "user2");
            env.GetClient().ModifyACL("/Root", "Dir2", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user3");
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EraseRow, "user4");
            env.GetClient().ModifyACL("/Root/Tenant1", "Dir3", acl.SerializeAsString());
        }

        { // anonymous login gives `ydb.granular.describe_schema` access
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint());
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["ydb.generic.full"];["root@builtin"]];
                [["/Root"];["ydb.generic.use"];["user1rootadmin"]];
                [["/Root"];["ydb.generic.use"];["user2"]];
                [["/Root"];["ydb.generic.use"];["user6tenant1admin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.full"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.full"];["user1rootadmin"]];
                [["/Root/Dir1"];["ydb.granular.select_row"];["user1rootadmin"]];
                [["/Root/Dir2"];["ydb.granular.erase_row"];["user2"]];
            ])";
            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        { // user1rootadmin has /Root GenericUse access
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_permissions`
                )").GetValueSync();

                auto expected = R"([
                    [["/Root"];["ydb.generic.full"];["root@builtin"]];
                    [["/Root"];["ydb.generic.use"];["user1rootadmin"]];
                    [["/Root"];["ydb.generic.use"];["user2"]];
                    [["/Root"];["ydb.generic.use"];["user6tenant1admin"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.full"];["root@builtin"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.full"];["user1rootadmin"]];
                    [["/Root/Dir1"];["ydb.granular.select_row"];["user1rootadmin"]];
                    [["/Root/Dir2"];["ydb.granular.erase_row"];["user2"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_permissions`
                )").GetValueSync();

                auto expected = R"([
                    [["/Root/Tenant1/Dir3"];["ydb.granular.select_row"];["user3"]];
                    [["/Root/Tenant1/Dir3"];["ydb.granular.erase_row"];["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // revoke user1rootadmin /Root/Dir2 GenericUse access
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "user1rootadmin");
            env.GetClient().ModifyACL("/Root", "Dir2", acl.SerializeAsString());

            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["ydb.generic.full"];["root@builtin"]];
                [["/Root"];["ydb.generic.use"];["user1rootadmin"]];
                [["/Root"];["ydb.generic.use"];["user2"]];
                [["/Root"];["ydb.generic.use"];["user6tenant1admin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.full"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.full"];["user1rootadmin"]];
                [["/Root/Dir1"];["ydb.granular.select_row"];["user1rootadmin"]];
            ])";
            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthPermissions_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto user : {
            "user1",
            "user2",
            "user"
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        for (auto dir : {
            "Dir",
            "Dir1",
            "Dir2",
            "Dir/SubDir1",
            "Dir/SubDir2"
        }) {
            env.GetClient().MkDir("/Root", dir);
        }

        for (auto acl : TVector<std::tuple<TString, TString, TString, NACLib::EAccessRights>>{
            {"/", "Root", "user1", NACLib::SelectRow},
            {"/", "Root", "user1", NACLib::EraseRow},
            {"/", "Root", "user1", NACLib::AlterSchema},
            {"/", "Root", "user2", NACLib::GenericUse},
            {"/Root", "Dir1", "user2", NACLib::GenericUse},
            {"/Root", "Dir1", "user1", NACLib::GenericUse},
            {"/Root", "Dir2", "user2", NACLib::GenericUse},
            {"/Root", "Dir2", "user", NACLib::GenericUse},
            {"/Root", "Dir2", "user1", NACLib::GenericUse},
            {"/Root", "Dir", "user1", NACLib::GenericUse},
            {"/Root", "Dir1", "user1", NACLib::AlterSchema},
            {"/Root/Dir1", "SubDir1", "user1", NACLib::AlterSchema},
            {"/Root/Dir1", "SubDir2", "user2", NACLib::AlterSchema},
            {"/Root/Dir1", "SubDir2", "user1", NACLib::AlterSchema}
        }) {
            NACLib::TDiffACL diffAcl;
            diffAcl.AddAccess(NACLib::EAccessType::Allow, std::get<3>(acl), std::get<2>(acl));
            env.GetClient().ModifyACL(std::get<0>(acl), std::get<1>(acl), diffAcl.SerializeAsString());
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT Path, Sid, Permission
            FROM `Root/.sys/auth_permissions`
        )").GetValueSync();

        auto expected = R"([
            [["/Root"];["user1"];["ydb.granular.alter_schema"]];
            [["/Root"];["user1"];["ydb.granular.erase_row"]];
            [["/Root"];["user1"];["ydb.granular.select_row"]];
            [["/Root"];["user2"];["ydb.generic.use"]];
            [["/Root/.metadata/workload_manager/pools/default"];["all-users@well-known"];["ydb.granular.describe_schema"]];
            [["/Root/.metadata/workload_manager/pools/default"];["all-users@well-known"];["ydb.granular.select_row"]];
            [["/Root/.metadata/workload_manager/pools/default"];["root@builtin"];["ydb.granular.describe_schema"]];
            [["/Root/.metadata/workload_manager/pools/default"];["root@builtin"];["ydb.granular.select_row"]];
            [["/Root/Dir"];["user1"];["ydb.generic.use"]];
            [["/Root/Dir1"];["user1"];["ydb.generic.use"]];
            [["/Root/Dir1"];["user1"];["ydb.granular.alter_schema"]];
            [["/Root/Dir1"];["user2"];["ydb.generic.use"]];
            [["/Root/Dir2"];["user"];["ydb.generic.use"]];
            [["/Root/Dir2"];["user1"];["ydb.generic.use"]];
            [["/Root/Dir2"];["user2"];["ydb.generic.use"]];
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(AuthEffectivePermissions) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");

        env.GetClient().MkDir("/Root", "Dir1");
        env.GetClient().MkDir("/Root/Tenant1", "Dir2");

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
            env.GetClient().ModifyACL("/", "Root", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user2");
            env.GetClient().ModifyACL("/Root/Tenant1", "Dir2", acl.SerializeAsString());
        }

        // Cerr << env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant2/Dir4").DebugString() << Endl;

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_effective_permissions`
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["ydb.generic.use"];["user1"]];
                [["/Root/.metadata"];["ydb.generic.use"];["user1"]];
                [["/Root/.metadata/workload_manager"];["ydb.generic.use"];["user1"]];
                [["/Root/.metadata/workload_manager/pools"];["ydb.generic.use"];["user1"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.use"];["user1"]];
                [["/Root/Dir1"];["ydb.generic.use"];["user1"]];
                [["/Root/Table0"];["ydb.generic.use"];["user1"]]
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant1/.sys/auth_effective_permissions`
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Tenant1"];["ydb.generic.use"];["user1"]];
                [["/Root/Tenant1/Dir2"];["ydb.generic.use"];["user1"]];
                [["/Root/Tenant1/Dir2"];["ydb.granular.select_row"];["user2"]];
                [["/Root/Tenant1/Table1"];["ydb.generic.use"];["user1"]]
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthPermissions_Selects) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root", "user2", "password2");

        env.GetClient().MkDir("/Root", "Dir1/SubDir1");
        env.GetClient().MkDir("/Root", "Dir1/SubDir2");

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
            env.GetClient().ModifyACL("/", "Root", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root", "Dir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user2");
            env.GetClient().ModifyACL("/Root", "Dir1", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root/Dir1", "SubDir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EraseRow, "user2");
            env.GetClient().ModifyACL("/Root/Dir1", "SubDir1", acl.SerializeAsString());
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1"];["ydb.generic.use"];["user1"]];
                [["/Root/Dir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
                WHERE Sid = "user2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1"];["ydb.granular.select_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid >= "user2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid = "user2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid = "user2" AND Permission >= "ydb.granular.erase_row"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid = "user2" AND Permission > "ydb.granular.erase_row"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }
}

} // NSysView
} // NKikimr
