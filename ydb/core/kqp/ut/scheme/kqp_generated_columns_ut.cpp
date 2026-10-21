#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

static NKikimrConfig::TAppConfig GeneratedColumnsAppConfig(bool enableIndexStreamWrite = true) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableGeneratedStored(true);
    appConfig.MutableFeatureFlags()->SetEnableGeneratedVirtual(true);
    appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(enableIndexStreamWrite);
    return appConfig;
}

static NKikimrPQ::TPQConfig GeneratedColumnsPQConfig() {
    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.SetEnabled(true);
    pqConfig.SetEnableProtoSourceIdInfo(true);
    pqConfig.SetTopicsAreFirstClassCitizen(true);
    pqConfig.SetRequireCredentialsInNewProtocol(false);
    pqConfig.AddClientServiceType()->SetName("data-streams");
    return pqConfig;
}

std::string GetShowCreateTable(NYdb::NQuery::TSession& session, const std::string& path) {
    const std::string query = "SHOW CREATE TABLE `" + path + "`;";
    auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TResultSetParser parser(result.GetResultSet(0));
    UNIT_ASSERT(parser.TryNextRow());
    auto ddl = parser.ColumnParser("CreateQuery").GetOptionalUtf8();
    UNIT_ASSERT_C(ddl.has_value(), "SHOW CREATE TABLE returned an empty CreateQuery");
    return *ddl;
}

void CheckGeneratedColumnAlterRejections(const std::string& modifier) {
    auto appConfig = GeneratedColumnsAppConfig();
    TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    {
        const std::string query = R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                depA Int32 NOT NULL,
                depB Int32,
                g Int32 GENERATED ALWAYS AS (k + depA + COALESCE(depB, 0)) )" +
                                  modifier + R"(,
                PRIMARY KEY (k),
                FAMILY fam ()
            );
        )";
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    auto rejects = [&](const std::string& alter, const TString& expectedError) {
        auto result = session.ExecuteQuery(alter, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "expected ALTER to be rejected: " << alter);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), expectedError);
    };

    // NOT NULL on the generated column and on its dependency columns (SET and DROP)
    rejects("ALTER TABLE TestTable ALTER COLUMN g DROP NOT NULL;", "GENERATED column");
    rejects("ALTER TABLE TestTable ALTER COLUMN g SET NOT NULL;", "GENERATED column");
    rejects("ALTER TABLE TestTable ALTER COLUMN depA DROP NOT NULL;", "referenced by a GENERATED column");
    rejects("ALTER TABLE TestTable ALTER COLUMN depB SET NOT NULL;", "referenced by a GENERATED column");

    // DEFAULT on the generated column (SET and DROP)
    rejects("ALTER TABLE TestTable ALTER COLUMN g SET DEFAULT 5;", "DEFAULT of GENERATED column");
    rejects("ALTER TABLE TestTable ALTER COLUMN g DROP DEFAULT;", "DEFAULT of GENERATED column");

    // Column family only makes sense for a materialized column, so it is rejected for VIRTUAL
    if (modifier == "VIRTUAL") {
        rejects("ALTER TABLE TestTable ALTER COLUMN g SET FAMILY fam;", "VIRTUAL GENERATED column");
    }
}

void CheckGeneratedColumnRejected(const std::string& createTable, const TString& expectedError) {
    auto appConfig = GeneratedColumnsAppConfig();
    TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    auto result = session.ExecuteQuery(createTable, TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), "expected the generated column to be rejected");
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), expectedError);
}

std::string GeneratedColumnDDL(const std::string& expr, const std::string& prefix = "", const std::string& modifier = "STORED") {
    return prefix + R"(
        CREATE TABLE TestTable (
            k Int32 NOT NULL,
            a Int32,
            s String,
            v Int32 GENERATED ALWAYS AS ()" +
           expr + ") " + modifier + R"(,
            PRIMARY KEY (k)
        );
    )";
}

void CheckGeneratedColumnsRejected(const std::vector<std::pair<std::string, TString>>& cases) {
    auto appConfig = GeneratedColumnsAppConfig();
    TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    for (const auto& [query, expectedError] : cases) {
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "expected the generated column to be rejected: " << query);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), expectedError, "query: " << query);
    }
}

void CheckGeneratedColumnsAccepted(const std::vector<std::pair<std::string, std::string>>& exprAndPrefix) {
    auto appConfig = GeneratedColumnsAppConfig();
    TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    for (const auto& [expr, prefix] : exprAndPrefix) {
        const std::string query = GeneratedColumnDDL(expr, prefix);
        auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query: " << query << "\n" << result.GetIssues().ToString());

        auto drop = session.ExecuteQuery("DROP TABLE TestTable;", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());
    }
}

void CheckGeneratedColumnPersisted(const std::string& createTable, bool expectStored) {
    auto appConfig = GeneratedColumnsAppConfig();
    TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

    auto db = kikimr.GetQueryClient();
    auto session = db.GetSession().GetValueSync().GetSession();

    auto result = session.ExecuteQuery(createTable, TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto describe = kikimr.GetTestClient().Ls("/Root/TestTable");
    const auto& table = describe->Record.GetPathDescription().GetTable();

    bool found = false;
    for (const auto& col : table.GetColumns()) {
        if (col.GetName() != "v") {
            continue;
        }

        found = true;
        UNIT_ASSERT_C(col.HasDefaultFromExpression(), "generated payload not persisted");
        const auto& generated = col.GetDefaultFromExpression();
        UNIT_ASSERT_VALUES_EQUAL(generated.GetStored(), expectStored);

        UNIT_ASSERT_VALUES_EQUAL(generated.GetExprText(), "k + 1");

        Cout << "EXPR:" << Endl << generated.GetExprText() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(generated.DependencyColumnNamesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(generated.GetDependencyColumnNames(0), "k");
    }

    UNIT_ASSERT_C(found, "generated column v not found in describe");
}

constexpr const char* MultiGeneratedTableDDL = R"(
    CREATE TABLE TestTable (
        k Int32 NOT NULL,
        a Int32,
        b Int32,
        c Int32,
        d Int32,
        g1 Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED,
        g2 Int32 GENERATED ALWAYS AS (COALESCE(b, 0) * 10 + COALESCE(c, 0)) STORED,
        PRIMARY KEY (k)
    );
)";
constexpr const char* MultiGeneratedSeed = R"(
    UPSERT INTO TestTable (k, a, b, c, d) VALUES (1, 1, 2, 3, 4), (2, 10, 20, 30, 40);
)";
constexpr const char* MultiGeneratedSelect = "SELECT k, a, b, c, d, g1, g2 FROM TestTable ORDER BY k;";
constexpr const char* MultiGeneratedUntouchedRow = "[2;[10];[20];[30];[40];[30];[230]]";
constexpr const char* MultiGeneratedSeed3 = R"(
    UPSERT INTO TestTable (k, a, b, c, d) VALUES
        (1, 1, 2, 3, 100),
        (2, 4, 5, 6, 100),
        (3, 7, 8, 9, 200);
)";
constexpr const char* MultiGeneratedRow1 = "[1;[1];[2];[3];[100];[3];[23]]";
constexpr const char* MultiGeneratedRow2 = "[2;[4];[5];[6];[100];[9];[56]]";
constexpr const char* MultiGeneratedRow3 = "[3;[7];[8];[9];[200];[15];[89]]";
constexpr const char* MultiGeneratedStarOrderSelect = "SELECT a, b, c, d, g1, g2, k FROM TestTable WHERE k < 3 ORDER BY k;";
constexpr const char* IndexedGeneratedTableDDL = R"(
    CREATE TABLE TestTable (
        k Int32 NOT NULL,
        a Int32,
        b Int32,
        g1 Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED,
        PRIMARY KEY (k),
        INDEX idx_g1 GLOBAL ON (g1)
    );
)";

constexpr const char* VirtualReadTableDDL = R"(
    CREATE TABLE VRead (
        a Int32,
        b Int32,
        grp Int32 NOT NULL,
        k Int32 NOT NULL,
        stored_sum Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED,
        v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) VIRTUAL,
        PRIMARY KEY (k),
        INDEX idx_grp GLOBAL ON (grp)
    );
)";

constexpr const char* VirtualReadSeed = R"(
    UPSERT INTO VRead (k, grp, a, b) VALUES
        (1, 1, 1, 1),
        (2, 1, 2, 2),
        (3, 2, NULL, 3);
)";

TString RowsYson(std::initializer_list<const char*> rows) {
    TStringBuilder result;
    result << "[";
    for (const auto* row : rows) {
        if (row != *rows.begin()) {
            result << ";";
        }
        result << row;
    }
    result << "]";
    return result;
}

class TTestFixture {
public:
    explicit TTestFixture(const std::string& createTable, const std::string& seed = "",
        bool enableIndexStreamWrite = true)
        : TTestFixture(createTable, seed, GeneratedColumnsAppConfig(enableIndexStreamWrite))
    {}

    TTestFixture(const std::string& createTable, const std::string& seed,
        const NKikimrConfig::TAppConfig& appConfig)
        : Kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false))
        , Db(Kikimr.GetQueryClient())
        , Session(Db.GetSession().GetValueSync().GetSession())
    {
        Exec(createTable);
        if (!seed.empty()) {
            Exec(seed);
        }
    }

    void Exec(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
    }

    void Rejects(const std::string& query, const TString& expectedError) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "expected the query to be rejected: " << query);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), expectedError);
    }

    void Check(const std::string& query, const TString& expected) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
    }

    TString QueryYson(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    }

    void CheckStaleEventually(const std::string& query, const TString& expected) {
        const TString normalizedExpected = ReformatYson(expected);
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        TString actual;

        do {
            auto result = Session.ExecuteQuery(
                                     query, TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n"
                                                               << result.GetIssues().ToString());
            actual = FormatResultSetYson(result.GetResultSet(0));
            if (ReformatYson(actual) == normalizedExpected) {
                return;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);

        CompareYson(expected, actual, TStringBuilder() << "eventual result mismatch for: " << query);
    }

    void CheckUnordered(const std::string& query, const TString& expected) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        CompareYsonUnordered(expected, FormatResultSetYson(result.GetResultSet(0)),
            TStringBuilder() << "unexpected rows for: " << query);
    }

    void CheckReturning(const std::string& update, const std::string& selectBack, const TString& expected) {
        auto result = Session.ExecuteQuery(update, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << update << "\n" << result.GetIssues().ToString());
        const TString returned = FormatResultSetYson(result.GetResultSet(0));

        CompareYsonUnordered(expected, returned, TStringBuilder() << "unexpected RETURNING rows for: " << update);

        auto after = Session.ExecuteQuery(selectBack, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(after.IsSuccess(), "query failed: " << selectBack << "\n" << after.GetIssues().ToString());

        CompareYsonUnordered(returned, FormatResultSetYson(after.GetResultSet(0)),
            TStringBuilder() << "RETURNING disagrees with a subsequent SELECT for: " << update);
    }

    TString ExplainAst(const std::string& query) {
        auto settings = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "explain failed: " << query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats().has_value(), "no stats for: " << query);
        const auto ast = result.GetStats()->GetAst();
        UNIT_ASSERT_C(ast.has_value(), "no AST for: " << query);
        return TString(*ast);
    }

    NJson::TJsonValue ExplainPlan(const std::string& query) {
        auto settings = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "explain failed: " << query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats().has_value(), "no stats for: " << query);
        const auto serializedPlan = result.GetStats()->GetPlan();
        UNIT_ASSERT_C(serializedPlan.has_value(), "no plan for: " << query);

        NJson::TJsonValue plan;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*serializedPlan, &plan, true),
            "invalid JSON plan for: " << query << "\n" << *serializedPlan);
        return plan;
    }

    void CheckStreamLookup(const std::string& query, bool expected) {
        const TString ast = ExplainAst(query);
        const bool has = ast.Contains("KqpCnStreamLookup");
        UNIT_ASSERT_C(has == expected,
            "stream lookup expectation mismatch for: " << query
                << "\n  expected stream lookup: " << (expected ? "yes" : "no")
                << ", found: " << (has ? "yes" : "no")
                << "\nAST:\n" << ast);
    }

    void RestartSchemeShard(const std::string& tablePath) {
        auto& runtime = *Kikimr.GetTestServer().GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();

        auto connect = [&](TDuration timeout) {
            const auto pipe = runtime.Register(
                NTabletPipe::CreateClient(sender, Tests::SchemeRoot, pipeConfig));
            auto connected = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientConnected>(
                sender, timeout);
            UNIT_ASSERT_C(connected, "timed out connecting to SchemeShard");
            UNIT_ASSERT_VALUES_EQUAL(connected->Get()->TabletId, Tests::SchemeRoot);
            UNIT_ASSERT_VALUES_EQUAL(connected->Get()->ClientId, pipe);
            UNIT_ASSERT_VALUES_EQUAL(connected->Get()->Status, NKikimrProto::OK);
            return std::pair(pipe, connected->Get()->Generation);
        };

        const auto [oldPipe, oldGeneration] = connect(TDuration::Seconds(30));
        runtime.Send(MakePipePerNodeCacheID(false), sender,
            new TEvPipeCache::TEvForward(
                new TEvents::TEvPoisonPill(), Tests::SchemeRoot, false));

        auto destroyed = runtime.GrabEdgeEventRethrow<TEvTabletPipe::TEvClientDestroyed>(
            sender, TDuration::Seconds(30));
        UNIT_ASSERT_C(destroyed, "timed out waiting for SchemeShard shutdown");
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Get()->TabletId, Tests::SchemeRoot);
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Get()->ClientId, oldPipe);

        const auto [newPipe, newGeneration] = connect(TDuration::Seconds(30));
        UNIT_ASSERT_C(newGeneration > oldGeneration,
            "SchemeShard generation did not change after restart: " << oldGeneration);
        runtime.Send(new IEventHandle(newPipe, sender, new TEvents::TEvPoisonPill()));

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(TString(tablePath));
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = true;
        runtime.Send(MakeSchemeCacheID(), sender,
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));

        auto response = runtime.GrabEdgeEventRethrow<
            TEvTxProxySchemeCache::TEvNavigateKeySetResult>(sender, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Request->ResultSet.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Request->ResultSet.front().Status,
            NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
    }

    std::string ShowCreateTable(const std::string& tablePath) {
        return GetShowCreateTable(Session, tablePath);
    }

    NYdb::NQuery::TSession& QuerySession() {
        return Session;
    }

private:
    TKikimrRunner Kikimr;
    NYdb::NQuery::TQueryClient Db;
    NYdb::NQuery::TSession Session;
};

void CollectPhysicalReadColumns(const NJson::TJsonValue& node, TStringBuf targetTable,
    const TString& inheritedTable, TVector<TVector<TString>>& result)
{
    if (node.IsArray()) {
        for (const auto& child : node.GetArraySafe()) {
            CollectPhysicalReadColumns(child, targetTable, inheritedTable, result);
        }
        return;
    }

    if (!node.IsMap()) {
        return;
    }

    const auto& map = node.GetMapSafe();
    TString currentTable = inheritedTable;
    if (const auto it = map.find("Table"); it != map.end() && it->second.IsString()) {
        currentTable = it->second.GetStringSafe();
    }

    const auto nameIt = map.find("Name");
    const auto columnsIt = map.find("ReadColumns");
    if (currentTable == targetTable && nameIt != map.end() && nameIt->second.IsString()
        && nameIt->second.GetStringSafe().StartsWith("Table")
        && columnsIt != map.end() && columnsIt->second.IsArray())
    {
        auto& columns = result.emplace_back();
        for (const auto& column : columnsIt->second.GetArraySafe()) {
            UNIT_ASSERT_C(column.IsString(), node.GetStringRobust());
            columns.push_back(column.GetStringSafe());
        }
    }

    for (const auto& [_, child] : map) {
        CollectPhysicalReadColumns(child, targetTable, currentTable, result);
    }
}

TVector<TString> GetSinglePhysicalReadColumns(const NJson::TJsonValue& plan, TStringBuf table) {
    TVector<TVector<TString>> reads;
    const auto& canonicalPlan = plan.GetMapSafe().at("Plan");
    CollectPhysicalReadColumns(canonicalPlan, table, {}, reads);
    UNIT_ASSERT_VALUES_EQUAL_C(reads.size(), 1u,
        "expected one physical read of " << table << "\n" << plan.GetStringRobust());
    return reads.front();
}

bool HasPlanOperator(const NJson::TJsonValue& plan, TStringBuf name) {
    return CountPlanNodesByKv(plan, "Node Type", TString(name)) > 0
        || CountPlanNodesByKv(plan, "Name", TString(name)) > 0;
}

struct TVirtualReturningProjection {
    TString Sql;
    TVector<TStringBuf> Columns;
};

const TVector<TVirtualReturningProjection>& VirtualReturningProjections() {
    static const TVector<TVirtualReturningProjection> projections = {
        {"*", {"a", "k", "v"}},
        {"k", {"k"}},
        {"a", {"a"}},
        {"v", {"v"}},
        {"k, a", {"k", "a"}},
        {"k, v", {"k", "v"}},
        {"a, k", {"a", "k"}},
        {"a, v", {"a", "v"}},
        {"v, k", {"v", "k"}},
        {"v, a", {"v", "a"}},
        {"k, a, v", {"k", "a", "v"}},
        {"k, v, a", {"k", "v", "a"}},
        {"a, k, v", {"a", "k", "v"}},
        {"a, v, k", {"a", "v", "k"}},
        {"v, k, a", {"v", "k", "a"}},
        {"v, a, k", {"v", "a", "k"}},
    };
    return projections;
}

TString ExpectedVirtualReturningRow(const TVirtualReturningProjection& projection, i32 k, i32 a) {
    TStringBuilder expected;
    expected << "[[";
    for (size_t i = 0; i < projection.Columns.size(); ++i) {
        if (i) {
            expected << ";";
        }

        if (projection.Columns[i] == "k") {
            expected << k;
        } else if (projection.Columns[i] == "a") {
            expected << "[" << a << "]";
        } else {
            UNIT_ASSERT_VALUES_EQUAL(projection.Columns[i], "v");
            expected << a * 10;
        }
    }
    expected << "]]";
    return expected;
}

enum class EVirtualReturningDml {
    Insert,
    InsertOrRevert,
    Upsert,
    Replace,
    UpdateWhere,
    UpdateOn,
    DeleteWhere,
    DeleteOn,
};

TStringBuf VirtualReturningDmlName(EVirtualReturningDml dml) {
    switch (dml) {
        case EVirtualReturningDml::Insert:
            return "INSERT";
        case EVirtualReturningDml::InsertOrRevert:
            return "INSERT OR REVERT";
        case EVirtualReturningDml::Upsert:
            return "UPSERT";
        case EVirtualReturningDml::Replace:
            return "REPLACE";
        case EVirtualReturningDml::UpdateWhere:
            return "UPDATE WHERE";
        case EVirtualReturningDml::UpdateOn:
            return "UPDATE ON";
        case EVirtualReturningDml::DeleteWhere:
            return "DELETE WHERE";
        case EVirtualReturningDml::DeleteOn:
            return "DELETE ON";
    }
    Y_UNREACHABLE();
}

bool VirtualReturningDmlNeedsSeed(EVirtualReturningDml dml) {
    return dml != EVirtualReturningDml::Insert
        && dml != EVirtualReturningDml::InsertOrRevert;
}

i32 VirtualReturningFinalA(EVirtualReturningDml dml, i32 initialA) {
    switch (dml) {
        case EVirtualReturningDml::Replace:
        case EVirtualReturningDml::UpdateWhere:
        case EVirtualReturningDml::UpdateOn:
            return initialA + 100;
        default:
            return initialA;
    }
}

TString BuildVirtualReturningDml(EVirtualReturningDml dml, i32 k, i32 initialA,
    const TVirtualReturningProjection& projection)
{
    const i32 finalA = VirtualReturningFinalA(dml, initialA);
    TStringBuilder query;
    switch (dml) {
        case EVirtualReturningDml::Insert:
            query << "INSERT INTO VReturningMatrix (k, a) VALUES (" << k << ", " << initialA << ")";
            break;
        case EVirtualReturningDml::InsertOrRevert:
            query << "INSERT OR REVERT INTO VReturningMatrix (k, a) VALUES (" << k << ", " << initialA << ")";
            break;
        case EVirtualReturningDml::Upsert:
            // Omitting a for an existing row checks that RETURNING uses its preserved value.
            query << "UPSERT INTO VReturningMatrix (k) VALUES (" << k << ")";
            break;
        case EVirtualReturningDml::Replace:
            query << "REPLACE INTO VReturningMatrix (k, a) VALUES (" << k << ", " << finalA << ")";
            break;
        case EVirtualReturningDml::UpdateWhere:
            query << "UPDATE VReturningMatrix SET a = " << finalA
                  << " WHERE k = " << k << " AND v = " << initialA * 10;
            break;
        case EVirtualReturningDml::UpdateOn:
            query << "UPDATE VReturningMatrix ON (k, a) VALUES (" << k << ", " << finalA << ")";
            break;
        case EVirtualReturningDml::DeleteWhere:
            query << "DELETE FROM VReturningMatrix WHERE k = " << k << " AND v = " << initialA * 10;
            break;
        case EVirtualReturningDml::DeleteOn:
            query << "DELETE FROM VReturningMatrix ON (k) VALUES (" << k << ")";
            break;
    }
    query << " RETURNING " << projection.Sql << ";";
    return query;
}

void SeedVirtualReturningDml(TTestFixture& fixture, i32 firstKey) {
    TStringBuilder query;
    query << "UPSERT INTO VReturningMatrix (k, a) VALUES ";
    for (size_t i = 0; i < VirtualReturningProjections().size(); ++i) {
        if (i) {
            query << ", ";
        }
        const i32 k = firstKey + i;
        query << "(" << k << ", " << k + 10 << ")";
    }
    query << ";";
    fixture.Exec(query);
}

TString ExpectedVirtualReturningRange(EVirtualReturningDml dml, i32 firstKey) {
    if (dml == EVirtualReturningDml::DeleteWhere || dml == EVirtualReturningDml::DeleteOn) {
        return "[]";
    }

    TStringBuilder expected;
    expected << "[";
    for (size_t i = 0; i < VirtualReturningProjections().size(); ++i) {
        if (i) {
            expected << ";";
        }
        const i32 k = firstKey + i;
        const i32 a = VirtualReturningFinalA(dml, k + 10);
        expected << "[" << k << ";[" << a << "];" << a * 10 << "]";
    }
    expected << "]";
    return expected;
}

void CheckVirtualReturningProjectionMatrix(bool enableStreamWrite) {
    auto appConfig = GeneratedColumnsAppConfig();
    appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(enableStreamWrite);

    TTestFixture fixture(R"(
        CREATE TABLE VReturningMatrix (
            k Int32 NOT NULL,
            a Int32,
            v Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10) VIRTUAL,
            PRIMARY KEY (k)
        );
    )", "", appConfig);

    UNIT_ASSERT_VALUES_EQUAL(VirtualReturningProjections().size(), 16u);

    const TVector<EVirtualReturningDml> dmls = {
        EVirtualReturningDml::Insert,
        EVirtualReturningDml::InsertOrRevert,
        EVirtualReturningDml::Upsert,
        EVirtualReturningDml::Replace,
        EVirtualReturningDml::UpdateWhere,
        EVirtualReturningDml::UpdateOn,
        EVirtualReturningDml::DeleteWhere,
        EVirtualReturningDml::DeleteOn,
    };

    for (size_t dmlIndex = 0; dmlIndex < dmls.size(); ++dmlIndex) {
        const auto dml = dmls[dmlIndex];
        const i32 firstKey = 1000 * (dmlIndex + 1);
        if (VirtualReturningDmlNeedsSeed(dml)) {
            SeedVirtualReturningDml(fixture, firstKey);
        }

        for (size_t projectionIndex = 0; projectionIndex < VirtualReturningProjections().size(); ++projectionIndex) {
            const auto& projection = VirtualReturningProjections()[projectionIndex];
            const i32 k = firstKey + projectionIndex;
            const i32 initialA = k + 10;
            const i32 expectedA = VirtualReturningFinalA(dml, initialA);
            const TString query = BuildVirtualReturningDml(dml, k, initialA, projection);
            const TString actual = fixture.QueryYson(query);
            CompareYson(
                ExpectedVirtualReturningRow(projection, k, expectedA),
                actual,
                TStringBuilder() << VirtualReturningDmlName(dml)
                    << " with RETURNING " << projection.Sql << ": " << query);
        }

        const TString select = TStringBuilder()
            << "SELECT k, a, v FROM VReturningMatrix WHERE k >= " << firstKey
            << " AND k < " << firstKey + VirtualReturningProjections().size() << " ORDER BY k;";
        fixture.Check(select, ExpectedVirtualReturningRange(dml, firstKey));
    }

    fixture.CheckReturning(
        "INSERT OR ABORT INTO VReturningMatrix (k, a) VALUES (9000, 91) RETURNING v, k, a;",
        "SELECT v, k, a FROM VReturningMatrix WHERE k = 9000;",
        "[[910;9000;[91]]]");
}

void CheckVirtualGeneratedReturning(bool enableStreamWrite) {
    auto appConfig = GeneratedColumnsAppConfig();
    appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(enableStreamWrite);

    TTestFixture fixture(R"(
        CREATE TABLE VReturning (
            a Int32,
            b Int32,
            k Int32 NOT NULL,
            v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) VIRTUAL,
            PRIMARY KEY (k),
            INDEX idx_b GLOBAL ON (b)
        );
    )", "", appConfig);

    fixture.CheckReturning(
        "INSERT INTO VReturning (k, a, b) VALUES (1, 1, 2) RETURNING k, v;",
        "SELECT k, v FROM VReturning WHERE k = 1;",
        "[[1;[12]]]");
    fixture.CheckReturning(
        "UPSERT INTO VReturning (k, a) VALUES (1, 3) RETURNING k, a, b, v;",
        "SELECT k, a, b, v FROM VReturning WHERE k = 1;",
        "[[1;[3];[2];[32]]]");
    fixture.CheckReturning(
        "REPLACE INTO VReturning (k, a) VALUES (2, 4) RETURNING k, a, b, v;",
        "SELECT k, a, b, v FROM VReturning WHERE k = 2;",
        "[[2;[4];#;[40]]]");
    fixture.CheckReturning(
        "UPDATE VReturning SET b = 5 WHERE v = 32 RETURNING k, v;",
        "SELECT k, v FROM VReturning WHERE k = 1;",
        "[[1;[35]]]");
    fixture.CheckReturning(
        "UPDATE VReturning SET a = 8 WHERE v = 35 RETURNING *;",
        "SELECT a, b, k, v FROM VReturning WHERE k = 1;",
        "[[[8];[5];1;[85]]]");
    CompareYson("[[1;[85]]]",
        fixture.QueryYson("DELETE FROM VReturning WHERE v = 85 RETURNING k, v;"));
    fixture.Check("SELECT k FROM VReturning WHERE k = 1;", "[]");

    fixture.CheckReturning(
        "INSERT INTO VReturning (k, a, b) VALUES (3, 6, 7) RETURNING *;",
        "SELECT a, b, k, v FROM VReturning WHERE k = 3;",
        "[[[6];[7];3;[67]]]");
    fixture.CheckReturning(
        "UPDATE VReturning ON (k, a) VALUES (3, 9) RETURNING k, b, v;",
        "SELECT k, b, v FROM VReturning WHERE k = 3;",
        "[[3;[7];[97]]]");
    CompareYson("[[3;[97]]]",
        fixture.QueryYson("DELETE FROM VReturning ON (k) VALUES (3) RETURNING k, v;"));
    fixture.Check("SELECT k FROM VReturning WHERE k = 3;", "[]");

    fixture.Exec(R"(
        CREATE TABLE VReturningConst (
            c Int32 GENERATED ALWAYS AS (5) VIRTUAL,
            k Int32 NOT NULL,
            PRIMARY KEY (k)
        );
    )");
    fixture.CheckReturning(
        "INSERT INTO VReturningConst (k) VALUES (10) RETURNING c, k;",
        "SELECT c, k FROM VReturningConst WHERE k = 10;",
        "[[[5];10]]");

    fixture.Exec(R"(
        CREATE TABLE VReturningNamedNewOld (
            `new` Int32,
            `old` Int32,
            k Int32 NOT NULL,
            v Int32 GENERATED ALWAYS AS (COALESCE(`new`, 0) + COALESCE(`old`, 0)) VIRTUAL,
            PRIMARY KEY (k)
        );
    )");
    fixture.CheckReturning(
        "INSERT INTO VReturningNamedNewOld (k, `new`, `old`) VALUES (20, 4, 6) RETURNING `new`, `old`, v;",
        "SELECT `new`, `old`, v FROM VReturningNamedNewOld WHERE k = 20;",
        "[[[4];[6];[10]]]");
}

void CheckVirtualReturningWithFulltextIndex(bool compact) {
    auto appConfig = GeneratedColumnsAppConfig();
    appConfig.MutableFeatureFlags()->SetEnableFulltextIndex(true);
    appConfig.MutableFeatureFlags()->SetEnableCompactFulltextIndex(compact);
    appConfig.MutableTableServiceConfig()->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TTestFixture fixture(R"(
        CREATE TABLE VFulltext (
            a Int32 NOT NULL,
            k Int32 NOT NULL,
            text String NOT NULL,
            v Int32 NOT NULL GENERATED ALWAYS AS (a + 1) VIRTUAL,
            PRIMARY KEY (k),
            INDEX idx_text
                GLOBAL USING fulltext_plain
                ON (text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )", "UPSERT INTO VFulltext (k, text, a) VALUES (1, \"Cats love naps.\", 10);",
        appConfig);

    fixture.CheckReturning(
        R"(
            UPSERT INTO VFulltext (k, text, a)
            VALUES (1, "Cats love naps.", 20)
            RETURNING k, v;
        )",
        "SELECT k, v FROM VFulltext WHERE k = 1;",
        "[[1;21]]");
    fixture.Check(
        "SELECT k, v FROM VFulltext VIEW idx_text WHERE FulltextMatch(text, \"cats\");",
        "[[1;21]]");
}

}   // namespace

Y_UNIT_TEST_SUITE(GeneratedStored) {
    Y_UNIT_TEST(Basic) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32 NOT NULL,
                v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec("UPSERT INTO TestTable (k, v2) VALUES (1, 1);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[4]]]");

        fixture.Exec("UPSERT INTO TestTable (k, v2) VALUES (1, 2);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[5]]]");

        fixture.Exec("UPSERT INTO TestTable (k, v2, v1) VALUES (1, 3, 3);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[8]]]");

        fixture.Exec("UPSERT INTO TestTable (k, v2) VALUES (1, 5);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[10]]]");
    }

    Y_UNIT_TEST(WithIndex) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32 NOT NULL,
                v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL ON (v)
            );
        )");

        fixture.Exec("UPSERT INTO TestTable (k, v2) VALUES (1, 1);");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 4;", "[[1;[4]]]");

        fixture.Exec("UPSERT INTO TestTable (k, v2, v1) VALUES (1, 1, 3);");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 4;", "[]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 6;", "[[1;[6]]]");

        fixture.Exec("UPSERT INTO TestTable (k, v2) VALUES (1, 5);");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 10;", "[[1;[10]]]");
    }

    Y_UNIT_TEST(DependsOnDefault) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                c Int32 NOT NULL DEFAULT 7,
                g Int32 GENERATED ALWAYS AS (k + c) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec("UPSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, c, g FROM TestTable ORDER BY k;", "[[1;7;[8]]]");
    }

    Y_UNIT_TEST(Insert) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32 NOT NULL,
                v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                PRIMARY KEY (k)
            );
        )");

        // Omit v1: new row stores v1 = NULL. v = 1*2 + 1 + COALESCE(NULL, 1) = 4
        fixture.Exec("INSERT INTO TestTable (k, v2) VALUES (1, 1);");

        // Supply every dependency. v = 2*2 + 3 + COALESCE(5, 1) = 12
        fixture.Exec("INSERT INTO TestTable (k, v2, v1) VALUES (2, 3, 5);");

        fixture.Check("SELECT k, v1, v FROM TestTable ORDER BY k;", "[[1;#;[4]];[2;[5];[12]]]");
    }

    Y_UNIT_TEST(Replace) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32 NOT NULL,
                v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                PRIMARY KEY (k)
            );
        )");

        // Seed a row with a non-null v1. v = 1*2 + 1 + COALESCE(5, 1) = 8
        fixture.Exec("INSERT INTO TestTable (k, v2, v1) VALUES (1, 1, 5);");

        // REPLACE the existing row omitting v1: v1 is reset to NULL. v = 1*2 + 3 + COALESCE(NULL, 1) = 6
        fixture.Exec("REPLACE INTO TestTable (k, v2) VALUES (1, 3);");

        // REPLACE inserting a new row: v1 = NULL. v = 2*2 + 1 + COALESCE(NULL, 1) = 6
        fixture.Exec("REPLACE INTO TestTable (k, v2) VALUES (2, 1);");

        fixture.Check("SELECT k, v1, v FROM TestTable ORDER BY k;", "[[1;#;[6]];[2;#;[6]]]");
    }

    Y_UNIT_TEST(Returning) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32 NOT NULL,
                v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                PRIMARY KEY (k)
            );
        )");

        // UPSERT a new row (v1 read back as NULL). v = 1*2 + 1 + COALESCE(NULL, 1) = 4
        fixture.CheckReturning("UPSERT INTO TestTable (k, v2) VALUES (1, 1) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 1;", "[[1;[4]]]");

        // UPSERT the existing row supplying v1. v = 1*2 + 3 + COALESCE(5, 1) = 10
        fixture.CheckReturning("UPSERT INTO TestTable (k, v2, v1) VALUES (1, 3, 5) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 1;", "[[1;[10]]]");

        // UPSERT the existing row omitting v1 (== 5): it is read back, not treated as NULL
        // v = 1*2 + 4 + COALESCE(5, 1) = 11
        fixture.CheckReturning("UPSERT INTO TestTable (k, v2) VALUES (1, 4) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 1;", "[[1;[11]]]");

        // INSERT a new row (v1 = NULL). v = 2*2 + 3 + COALESCE(NULL, 1) = 8
        fixture.CheckReturning("INSERT INTO TestTable (k, v2) VALUES (2, 3) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 2;", "[[2;[8]]]");

        // REPLACE a new row (v1 = NULL). v = 3*2 + 1 + COALESCE(NULL, 1) = 8
        fixture.CheckReturning("REPLACE INTO TestTable (k, v2) VALUES (3, 1) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 3;", "[[3;[8]]]");
    }

    Y_UNIT_TEST(ReturningWithIndex) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32 NOT NULL,
                v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL ON (v)
            );
        )");

        auto viaIndex = [&](const std::string& value, const TString& expected) {
            fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = " + value + " ORDER BY k;", expected);
        };

        // UPSERT supplying v1. v = 1*2 + 1 + COALESCE(3, 1) = 6
        fixture.CheckReturning("UPSERT INTO TestTable (k, v2, v1) VALUES (1, 1, 3) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 1;", "[[1;[6]]]");
        viaIndex("6", "[[1;[6]]]");

        // Partial UPSERT omitting v1 (== 3): read back, index updated. v = 1*2 + 5 + COALESCE(3, 1) = 10
        fixture.CheckReturning("UPSERT INTO TestTable (k, v2) VALUES (1, 5) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 1;", "[[1;[10]]]");
        viaIndex("6", "[]");
        viaIndex("10", "[[1;[10]]]");

        // INSERT (v1 = NULL). v = 2*2 + 3 + COALESCE(NULL, 1) = 8
        fixture.CheckReturning("INSERT INTO TestTable (k, v2) VALUES (2, 3) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 2;", "[[2;[8]]]");

        // REPLACE (v1 = NULL). v = 3*2 + 1 + COALESCE(NULL, 1) = 8
        fixture.CheckReturning("REPLACE INTO TestTable (k, v2) VALUES (3, 1) RETURNING k, v;",
            "SELECT k, v FROM TestTable WHERE k = 3;", "[[3;[8]]]");

        // Both k=2 and k=3 land on v == 8 in the index
        viaIndex("8", "[[2;[8]];[3;[8]]]");
    }

    Y_UNIT_TEST(DependsOnSerial) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                id Serial,
                name String,
                g Int32 GENERATED ALWAYS AS (id * 10) STORED,
                PRIMARY KEY (id)
            );
        )");

        fixture.Exec(R"(INSERT INTO TestTable (name) VALUES ("a"), ("b");)");
        fixture.Check("SELECT id, g FROM TestTable ORDER BY id;", "[[1;[10]];[2;[20]]]");
    }

    Y_UNIT_TEST(NotNull) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(v1, 0) + v2) STORED,
                PRIMARY KEY (k)
            );
        )");

        // Inline path: every dependency supplied. g = COALESCE(5, 0) + 1 = 6
        fixture.Exec("UPSERT INTO TestTable (k, v1, v2) VALUES (1, 5, 1);");

        // Stream-lookup path: partial UPSERT omitting v1 (== 5), which is read back. g = 5 + 3 = 8
        fixture.Exec("UPSERT INTO TestTable (k, v2) VALUES (1, 3);");

        // INSERT omitting the nullable v1: it is stored as NULL, and COALESCE keeps g non-NULL
        // g = COALESCE(NULL, 0) + 3 = 3.
        fixture.Exec("INSERT INTO TestTable (k, v2) VALUES (2, 3);");

        // g is NOT NULL, so it comes back non-optional
        fixture.Check("SELECT k, g FROM TestTable ORDER BY k;", "[[1;8];[2;3]]");
    }

    Y_UNIT_TEST(NotNullWithIndex) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(v1, 0) + v2) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL ON (g)
            );
        )");

        // g = COALESCE(3, 0) + 1 = 4
        fixture.Exec("UPSERT INTO TestTable (k, v1, v2) VALUES (1, 3, 1);");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 4;", "[[1;4]]");

        // Partial UPSERT omitting v1 (== 3): read back, index updated. g = 3 + 5 = 8
        fixture.Exec("UPSERT INTO TestTable (k, v2) VALUES (1, 5);");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 4;", "[]");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 8;", "[[1;8]]");
    }

    Y_UNIT_TEST(NotNullPartialUpsertUntouchedColumn) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v2 Int32,
                v3 Int32,
                g1 Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(v1, 0) + COALESCE(v2, 0)) STORED,
                g2 Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(v3, 0) + COALESCE(v2, 0)) STORED,
                PRIMARY KEY (k)
            );
        )");

        // Insert a new row touching only g1's dependency
        fixture.Exec("UPSERT INTO TestTable (k, v1) VALUES (1, 10);");
        fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", "[[1;10;0]]");

        // Partial UPSERT touching only g2's dependency (v3) on the existing row
        fixture.Exec("UPSERT INTO TestTable (k, v3) VALUES (1, 5);");
        fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", "[[1;10;5]]");
    }

    Y_UNIT_TEST(NotNullUpdateOnPartial) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                c Int32,
                note Int32,
                g1 Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED,
                g2 Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(c, 0) * 10) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TestTable (k, a, b, c, note) VALUES (1, 1, 2, 3, 100);");

        // Seed: g1 = 1 + 2 = 3, g2 = 3 * 10 = 30
        fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", "[[1;3;30]]");

        // UPDATE ON touching no generated dependency (only note). Both generated columns keep
        // their stored value; the row is still updated
        fixture.Exec("UPDATE TestTable ON (k, note) VALUES (1, 999);");
        fixture.Check("SELECT k, note, g1, g2 FROM TestTable ORDER BY k;", "[[1;[999];3;30]]");

        // UPDATE ON touching a dependency of g1 only (a). g1 is recomputed from the new a and the
        // looked-up b; g2 is untouched and keeps its value
        // g1 = 5 + 2 = 7, g2 = 30
        fixture.Exec("UPDATE TestTable ON (k, a) VALUES (1, 5);");
        fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", "[[1;7;30]]");

        // UPDATE ON never inserts: a non-existent key is a no-op
        fixture.Exec("UPDATE TestTable ON (k, note) VALUES (42, 1);");
        fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", "[[1;7;30]]");
    }

    Y_UNIT_TEST(NotNullUpdateOnLookedUpDependency) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Uint32 NOT NULL,
                a Uint32 NOT NULL,
                b Uint32 NOT NULL,
                s Uint32 NOT NULL GENERATED ALWAYS AS (a + b) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TestTable (k, a, b) VALUES (1u, 1u, 2u);");

        // Seed: s = 1 + 2 = 3
        fixture.Check("SELECT k, s FROM TestTable ORDER BY k;", "[[1u;3u]]");

        // UPDATE ... SET recomputes s from the new a and the stored b
        fixture.Exec("UPDATE TestTable SET a = 5u WHERE k = 1u;");
        fixture.Check("SELECT k, a, b, s FROM TestTable ORDER BY k;", "[[1u;5u;2u;7u]]");

        // UPDATE ... ON supplying only a: b is read back from the table. Since UPDATE ON never
        // inserts, the looked-up row is always present, so s stays NOT NULL
        fixture.Exec("UPDATE TestTable ON (SELECT 8u AS a, 1u AS k);");
        fixture.Check("SELECT k, a, b, s FROM TestTable ORDER BY k;", "[[1u;8u;2u;10u]]");

        // Listing every dependency keeps working (no read-back at all)
        fixture.Exec("UPDATE TestTable ON (SELECT 1u AS k, 3u AS a, 4u AS b);");
        fixture.Check("SELECT k, a, b, s FROM TestTable ORDER BY k;", "[[1u;3u;4u;7u]]");

        // A non-existent key is still a no-op
        fixture.Exec("UPDATE TestTable ON (SELECT 42u AS k, 1u AS a);");
        fixture.Check("SELECT k, a, b, s FROM TestTable ORDER BY k;", "[[1u;3u;4u;7u]]");
    }

    Y_UNIT_TEST(NotNullUpdateOnLookedUpDependencyWithIndex) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Uint32 NOT NULL,
                a Uint32 NOT NULL,
                b Uint32 NOT NULL,
                s Uint32 NOT NULL GENERATED ALWAYS AS (a + b) STORED,
                PRIMARY KEY (k),
                INDEX idx_s GLOBAL ON (s)
            );
        )", "UPSERT INTO TestTable (k, a, b) VALUES (1u, 1u, 2u);");

        // Seed: s = 1 + 2 = 3
        fixture.Check("SELECT k, s FROM TestTable VIEW idx_s WHERE s = 3u;", "[[1u;3u]]");

        // Only a is supplied, b is read back from the table: s = 8 + 2 = 10, and the index follows
        fixture.Exec("UPDATE TestTable ON (SELECT 8u AS a, 1u AS k);");
        fixture.Check("SELECT k, a, b, s FROM TestTable ORDER BY k;", "[[1u;8u;2u;10u]]");
        fixture.Check("SELECT k, s FROM TestTable VIEW idx_s WHERE s = 3u;", "[]");
        fixture.Check("SELECT k, s FROM TestTable VIEW idx_s WHERE s = 10u;", "[[1u;10u]]");
    }

    Y_UNIT_TEST(NotNullUpsertPartial) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                c Int32,
                note Int32,
                g1 Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED,
                g2 Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(c, 0) * 10) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TestTable (k, a, b, c, note) VALUES (1, 1, 2, 3, 100);");

        // Seed: g1 = 1 + 2 = 3, g2 = 3 * 10 = 30
        fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", "[[1;3;30]]");

        // UPDATE existing row touching no generated dependency (only note). Both generated
        // columns are recomputed from looked-up deps and stay the same
        fixture.Exec("UPSERT INTO TestTable (k, note) VALUES (1, 777);");
        fixture.Check("SELECT k, note, g1, g2 FROM TestTable ORDER BY k;", "[[1;[777];3;30]]");

        // UPDATE existing row touching a dependency of g1 only (a). g1 recomputed from new a and
        // looked-up b; g2 recomputed from looked-up c and stays the same
        // g1 = 10 + 2 = 12, g2 = 30
        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 10);");
        fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", "[[1;12;30]]");

        // INSERT a new row touching no generated dependency (only note). Missing deps default to
        // NULL; both NOT NULL generated columns are computed from defaults
        // g1 = 0 + 0 = 0, g2 = 0 * 10 = 0
        fixture.Exec("UPSERT INTO TestTable (k, note) VALUES (2, 5);");

        // INSERT a new row touching a dependency of g1 only (a)
        // g1 = 7 + 0 = 7, g2 = 0
        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (3, 7);");

        fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", "[[1;12;30];[2;0;0];[3;7;0]]");
    }

    Y_UNIT_TEST(NotNullDependsOnSerial) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                id Serial,
                name String,
                g Int32 NOT NULL GENERATED ALWAYS AS (id * 10) STORED,
                PRIMARY KEY (id)
            );
        )");

        fixture.Exec(R"(INSERT INTO TestTable (name) VALUES ("a");)");
        fixture.Check("SELECT id, g FROM TestTable ORDER BY id;", "[[1;10]]");
    }

    Y_UNIT_TEST(ShowCreateTable) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE `/Root/ShowCreateGenerated` (
                    k Int32 NOT NULL,
                    st Int32 GENERATED ALWAYS AS (k * 2) STORED,
                    vt Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
                    nn Int32 NOT NULL GENERATED ALWAYS AS (k + 5) STORED,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const std::string ddl = GetShowCreateTable(session, "/Root/ShowCreateGenerated");

        UNIT_ASSERT_STRING_CONTAINS_C(ddl, "GENERATED ALWAYS AS (k * 2) STORED", ddl.c_str());
        UNIT_ASSERT_STRING_CONTAINS_C(ddl, "GENERATED ALWAYS AS (k + 1) VIRTUAL", ddl.c_str());
        UNIT_ASSERT_STRING_CONTAINS_C(ddl, "NOT NULL GENERATED ALWAYS AS (k + 5) STORED", ddl.c_str());
    }

    Y_UNIT_TEST(ShowCreateTableReplay) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                PRAGMA classic_division = "0";

                CREATE TABLE `/Root/Origin` (
                    k Int32 NOT NULL,
                    v1 Int32,
                    st Int32 GENERATED ALWAYS AS (k * 2 + COALESCE(v1, 1)) STORED,
                    vt Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto generatedOf = [&](const TString& column) {
            auto describe = kikimr.GetTestClient().Ls("/Root/Origin");
            const auto& table = describe->Record.GetPathDescription().GetTable();
            for (const auto& col : table.GetColumns()) {
                if (col.GetName() == column) {
                    UNIT_ASSERT_C(col.HasDefaultFromExpression(), "column " << column << " has no generated definition");
                    return col.GetDefaultFromExpression();
                }
            }
            UNIT_FAIL("column " << column << " not found");
            return NKikimrSchemeOp::TDefaultExpressionColumnDescription();
        };

        const auto originSt = generatedOf("st");
        const auto originVt = generatedOf("vt");

        const std::string ddl = GetShowCreateTable(session, "/Root/Origin");
        UNIT_ASSERT_C(ddl.find("CREATE TABLE") == 0, ddl.c_str());
        UNIT_ASSERT_C(ddl.find("PRAGMA") == std::string::npos, ddl.c_str());

        // Replay the printed statement over the dropped original: it must recreate it as it was.
        {
            auto result = session.ExecuteQuery("DROP TABLE `/Root/Origin`;", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteQuery(ddl, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "replaying the printed statement failed: " << result.GetIssues().ToString() << "\nstatement:\n"
                                                                                         << ddl.c_str());
        }

        const auto replayedSt = generatedOf("st");
        UNIT_ASSERT_VALUES_EQUAL(replayedSt.GetStored(), true);
        UNIT_ASSERT_VALUES_EQUAL(replayedSt.GetStored(), originSt.GetStored());
        UNIT_ASSERT_VALUES_EQUAL(replayedSt.GetExprText(), originSt.GetExprText());
        UNIT_ASSERT_VALUES_EQUAL(replayedSt.DependencyColumnNamesSize(), originSt.DependencyColumnNamesSize());

        const auto replayedVt = generatedOf("vt");
        UNIT_ASSERT_VALUES_EQUAL(replayedVt.GetStored(), false);
        UNIT_ASSERT_VALUES_EQUAL(replayedVt.GetStored(), originVt.GetStored());
        UNIT_ASSERT_VALUES_EQUAL(replayedVt.GetExprText(), originVt.GetExprText());
        UNIT_ASSERT_VALUES_EQUAL(replayedVt.DependencyColumnNamesSize(), originVt.DependencyColumnNamesSize());
    }

    Y_UNIT_TEST(AlterRejected) {
        CheckGeneratedColumnAlterRejections("STORED");
    }

    Y_UNIT_TEST(FeatureFlagDisabled) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableGeneratedStored(false);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session
                              .ExecuteQuery(R"(
                CREATE TABLE TStored (
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k + 1) STORED,
                    PRIMARY KEY (k)
                );
            )",
                                  TTxControl::NoTx())
                              .GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "STORED generated column must be rejected when the flag is off");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "STORED GENERATED columns are disabled");
        }

        {
            auto result = session
                              .ExecuteQuery(R"(
                CREATE TABLE TVirtual (
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )",
                                  TTxControl::NoTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(IndexStreamWriteDisabled) {
        auto appConfig = GeneratedColumnsAppConfig(
            /* enableIndexStreamWrite */ false);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(
            CREATE TABLE TGenerated (
                k Int32 NOT NULL,
                v Int32 GENERATED ALWAYS AS (k + 1) STORED,
                PRIMARY KEY (k)
            );
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(),
            "STORED generated column must be rejected when index stream writes are disabled");
        UNIT_ASSERT_STRING_CONTAINS(
            result.GetIssues().ToString(),
            "Generated columns require EnableIndexStreamWrite");
    }

    Y_UNIT_TEST(NonDeterministicAccepted) {
        CheckGeneratedColumnsAccepted({
            {"CAST(RandomNumber(k) AS Int32)", ""},
            {"CAST(Random(k) * 100 AS Int32)", ""},
            {"k + CAST(RandomNumber(k) AS Int32)", ""},
        });
    }

    Y_UNIT_TEST(SelfReferenceRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32,
                v Int32 GENERATED ALWAYS AS (v + 1) STORED,
                PRIMARY KEY (k)
            );
        )",
            "can not reference itself");
    }

    Y_UNIT_TEST(UnknownColumnRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32,
                v Int32 GENERATED ALWAYS AS (missing + 1) STORED,
                PRIMARY KEY (k)
            );
        )",
            "unknown column");
    }

    Y_UNIT_TEST(ReferencesGeneratedRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32,
                a Int32 GENERATED ALWAYS AS (k + 1) STORED,
                b Int32 GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k)
            );
        )",
            "references another generated column");
    }

    Y_UNIT_TEST(AggregateFunctionRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("SUM(k)"), "aggregate function"},
            {GeneratedColumnDDL("COUNT(*)"), "aggregate function"},
            {GeneratedColumnDDL("MAX(a) + 1"), "aggregate function"},
            {GeneratedColumnDDL("ListLength(AGGREGATE_LIST(k))"), "aggregate function"},
        });
    }

    Y_UNIT_TEST(WindowFunctionRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("SUM(k) OVER ()"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("ROW_NUMBER() OVER (ORDER BY k)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("LAG(k) OVER (PARTITION BY a)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("SUM(k) OVER w"), "Failed to compile the expression of generated column v"},
        });
    }

    Y_UNIT_TEST(SubqueryRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("IF(k IN (SELECT a FROM OtherTable), 1, 0)"), "subquery"},
            {GeneratedColumnDDL("IF(EXISTS (SELECT a FROM OtherTable), 1, 0)"), "subquery"},
        });
    }

    Y_UNIT_TEST(NamedExpressionWithReadRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("k + $x", "$x = SELECT MAX(a) FROM OtherTable;\n"), "Unknown name: $x"},
            {GeneratedColumnDDL("k + (SELECT COUNT(*) FROM $s())",
                "DEFINE SUBQUERY $s() AS SELECT * FROM OtherTable; END DEFINE;\n"),
                "Failed to compile the expression of generated column v"},
        });
    }

    Y_UNIT_TEST(ParameterRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("k + $p", "DECLARE $p AS Int32;\n"), "Unknown name: $p"},
        });
    }

    Y_UNIT_TEST(UnrelatedDeclareAccepted) {
        CheckGeneratedColumnsAccepted({
            {"k + 1", "DECLARE $p AS Int32;\n"},
        });
    }

    Y_UNIT_TEST(NonRowDependentRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("CAST(TablePath() AS Int32)"), "TablePath"},
            {GeneratedColumnDDL("CAST(TableName() AS Int32)"), "TableName"},
            {GeneratedColumnDDL("CAST(TableRecordIndex() AS Int32)"), "TableRecord"},
            {GeneratedColumnDDL("CAST(FileContent(\"f\") AS Int32)"), "FileContent"},
            {GeneratedColumnDDL("EvaluateExpr(1 + 2)"), "EvaluateExpr"},
            {GeneratedColumnDDL("CAST(CurrentAuthenticatedUser() AS Int32)"), "CurrentAuthenticatedUser"},
            {GeneratedColumnDDL("CAST(SecureParam(\"token\") AS Int32)"), "SecureParam"},
        });
    }

    Y_UNIT_TEST(WholeRowReferenceRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("TableRow().a"), "uses the whole row"},
            {GeneratedColumnDDL("TableRow().a * 10"), "uses the whole row"},
            {GeneratedColumnDDL("JoinTableRow().a"), "uses the whole row"},
            {GeneratedColumnDDL("k + TableRow().a"), "uses the whole row"},
            {GeneratedColumnDDL("TableRow().a", "PRAGMA EnableSystemColumns='false';\n"), "uses the whole row"},
            {GeneratedColumnDDL("CAST(TableRow() AS String)"), "uses the whole row"},
            {GeneratedColumnDDL("CAST(ListLength(StructMembers(TableRow())) AS Int32)"), "uses the whole row"},
            {GeneratedColumnDDL("CAST(Yson::SerializeText(Yson::From(TableRow())) AS Int32)"), "uses the whole row"},
        });
    }

    Y_UNIT_TEST(WholeRowSelfReferenceRejected) {
        CheckGeneratedColumnsRejected({
            {R"(
                CREATE TABLE TestTable (
                    k Int32,
                    v Int32 GENERATED ALWAYS AS (TableRow().v) STORED,
                    PRIMARY KEY (k)
                );
            )",
                "uses the whole row"},
            {R"(
                CREATE TABLE TestTable (
                    k Int32,
                    g1 Int32 GENERATED ALWAYS AS (k + 1) STORED,
                    g2 Int32 GENERATED ALWAYS AS (TableRow().g1 + 1) STORED,
                    PRIMARY KEY (k)
                );
            )",
                "uses the whole row"},
            {R"(
                CREATE TABLE TestTable (
                    k Int32,
                    a Int32,
                    v Int32 GENERATED ALWAYS AS (TableRow().a) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )",
                "uses the whole row"},
        });
    }

    Y_UNIT_TEST(AggregateFunctionVariantsRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("MIN(a)"), "aggregate function"},
            {GeneratedColumnDDL("AVG(a)"), "aggregate function"},
            {GeneratedColumnDDL("COUNT(a)"), "aggregate function"},
            {GeneratedColumnDDL("COUNT(DISTINCT a)"), "aggregate function"},
            {GeneratedColumnDDL("SUM(DISTINCT a)"), "aggregate function"},
            {GeneratedColumnDDL("SOME(a)"), "aggregate function"},
            {GeneratedColumnDDL("MAX_BY(a, k)"), "aggregate function"},
            {GeneratedColumnDDL("PERCENTILE(a, 0.5)"), "aggregate function"},
            {GeneratedColumnDDL("CORRELATION(a, k)"), "aggregate function"},
            {GeneratedColumnDDL("VARIANCE(a)"), "aggregate function"},
            {GeneratedColumnDDL("ListLength(AGGREGATE_LIST_DISTINCT(a))"), "aggregate function"},
        });
    }

    Y_UNIT_TEST(WindowFunctionVariantsRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("RANK() OVER (ORDER BY a)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("DENSE_RANK() OVER (ORDER BY a)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("LEAD(a) OVER (ORDER BY k)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("FIRST_VALUE(a) OVER (ORDER BY k)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("LAST_VALUE(a) OVER (ORDER BY k)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("NTILE(4) OVER (ORDER BY k)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("CUME_DIST() OVER (ORDER BY a)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("AVG(a) OVER (PARTITION BY k)"), "Window and aggregation functions are not allowed"},
            {GeneratedColumnDDL("COUNT(*) OVER ()"), "Window and aggregation functions are not allowed"},
        });
    }

    Y_UNIT_TEST(SubqueryVariantsRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("k + (SELECT COUNT(*) FROM OtherTable)"), "subquery"},
            {GeneratedColumnDDL("IF(k NOT IN (SELECT a FROM OtherTable), 1, 0)"), "subquery"},
            {GeneratedColumnDDL("IF(NOT EXISTS (SELECT a FROM OtherTable), 1, 0)"), "subquery"},
        });
    }

    Y_UNIT_TEST(NamedExpressionSubqueryVariantsRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("IF(k IN $ids, 1, 0)", "$ids = SELECT a FROM OtherTable;\n"), "Unknown name: $ids"},
            {GeneratedColumnDDL("k + $doubled",
                 "$base = SELECT MAX(a) FROM OtherTable;\n$doubled = $base * 2;\n"),
                "Unknown name: $doubled"},
        });
    }

    Y_UNIT_TEST(ParameterVariantsRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("$p * a", "DECLARE $p AS Int32;\n"), "Unknown name: $p"},
            {GeneratedColumnDDL("COALESCE($p, k)", "DECLARE $p AS Int32;\n"), "Unknown name: $p"},
            {GeneratedColumnDDL("k + $p + $q", "DECLARE $p AS Int32;\nDECLARE $q AS Int32;\n"), "Unknown name"},
        });
    }

    Y_UNIT_TEST(NamedExpressionRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("k + $c", "$c = 5;\n"), "Unknown name: $c"},
            {GeneratedColumnDDL("k + $c", "$c = 5;\n", "VIRTUAL"), "Unknown name: $c"},
        });
    }

    Y_UNIT_TEST(SingleRowExpressionsAccepted) {
        CheckGeneratedColumnsAccepted({
            {"k + 1", ""},
            {"CASE WHEN k > 0 THEN COALESCE(a, 0) ELSE -1 END", ""},
            {"CAST(ListLength(ListMap(AsList(k, k + 1), ($e) -> { RETURN $e * 2 })) AS Int32)", ""},
            {"CAST(Unicode::ToLower(CAST(s AS Utf8)) AS Int32)", ""},
            {"k + 1", "PRAGMA AnsiInForEmptyOrNullableItemsCollections;\n"},
            {"k + 1", "$unused = SELECT MAX(a) FROM OtherTable;\n"},
        });
    }

    Y_UNIT_TEST(TypeMismatchRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32,
                v Int32 GENERATED ALWAYS AS (CAST(k AS String)) STORED,
                PRIMARY KEY (k)
            );
        )",
            "type mismatch");
    }

    Y_UNIT_TEST(NotNullOptionalExprRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v1 Int32,
                v Int32 NOT NULL GENERATED ALWAYS AS (v1 + 1) STORED,
                PRIMARY KEY (k)
            );
        )",
            "is declared NOT NULL, but its expression can evaluate to NULL");
    }

    Y_UNIT_TEST(NotNullJsonExistsRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Json,
                hasKey Bool NOT NULL GENERATED ALWAYS AS (JSON_EXISTS(v, "$.key" UNKNOWN ON ERROR)) STORED,
                PRIMARY KEY (k)
            );
        )",
            "is declared NOT NULL, but its expression can evaluate to NULL");
    }

    Y_UNIT_TEST(NotNullFailingCastRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                s String NOT NULL,
                n Int32 NOT NULL GENERATED ALWAYS AS (CAST(s AS Int32)) STORED,
                PRIMARY KEY (k)
            );
        )",
            "is declared NOT NULL, but its expression can evaluate to NULL");
    }
    Y_UNIT_TEST(NullableOptionalExprAccepted) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Json,
                hasKey Bool GENERATED ALWAYS AS (JSON_EXISTS(v, "$.key" UNKNOWN ON ERROR)) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec(R"(
            UPSERT INTO TestTable (k, v) VALUES
                (1, CAST(@@{"key": 1}@@ AS Json)),
                (2, CAST(@@{"other": 1}@@ AS Json)),
                (3, NULL);
        )");

        // A NULL document yields a NULL value, which a nullable column stores
        fixture.Check("SELECT k, hasKey FROM TestTable ORDER BY k;", "[[1;[%true]];[2;[%false]];[3;#]]");
    }

    Y_UNIT_TEST(GeneratedColumnStoredPersisted) {
        CheckGeneratedColumnPersisted(R"(
            CREATE TABLE TestTable (
                k Int32,
                v Int32 GENERATED ALWAYS AS (k + 1) STORED,
                PRIMARY KEY (k)
            );
        )",
            /* expectStored */ true);
    }

    Y_UNIT_TEST(SuppliedValueRejected) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32,
                v Int32 GENERATED ALWAYS AS (k + 1) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Rejects("UPSERT INTO TestTable (k, v) VALUES (1, 99);", "cannot be set explicitly");
    }

    Y_UNIT_TEST(TtlRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32,
                base Timestamp,
                ts Timestamp GENERATED ALWAYS AS (base) STORED,
                PRIMARY KEY (k)
            ) WITH (TTL = Interval("PT1H") ON ts);
        )",
            "can not be a GENERATED column");
    }

    Y_UNIT_TEST(BulkUpsertRejected) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto queryClient = kikimr.GetQueryClient();
        auto tableClient = kikimr.GetTableClient();

        {
            auto result = queryClient
                              .ExecuteQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k + 1) STORED,
                    PRIMARY KEY (k)
                );
            )",
                                  TTxControl::NoTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto rowsBuilder = NYdb::TValueBuilder();
            rowsBuilder.BeginList();
            rowsBuilder.AddListItem().BeginStruct().AddMember("k").Int32(1).EndStruct();
            rowsBuilder.EndList();

            auto result = tableClient.BulkUpsert("/Root/TestTable", rowsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "bulk upsert on a STORED generated table must be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "STORED generated");
        }
    }

    Y_UNIT_TEST(DependencyDropRejected) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32,
                a Int32,
                v Int32 GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Rejects("ALTER TABLE TestTable DROP COLUMN a;", "used by generated column");
    }

    Y_UNIT_TEST(UpdateSetGeneratedRejected) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        fixture.Rejects("UPDATE TestTable SET g1 = 5 WHERE k = 1;", "cannot be set explicitly");
        fixture.Rejects("UPDATE TestTable SET g2 = 5 WHERE k = 1;", "cannot be set explicitly");

        // Nothing was written
        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[3];[4];[3];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetGeneratedWithDependencyRejected) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        fixture.Rejects("UPDATE TestTable SET a = 5, g1 = 5 WHERE k = 1;", "cannot be set explicitly");
        fixture.Rejects("UPDATE TestTable SET d = 5, g2 = 5 WHERE k = 1;", "cannot be set explicitly");
        fixture.Rejects("UPDATE TestTable SET g1 = 1, g2 = 2 WHERE k = 1;", "cannot be set explicitly");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[3];[4];[3];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetOneDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = COALESCE(10, 0) + COALESCE(2, 0) = 12, g2 untouched (b, c unchanged) = 23
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[10];[2];[3];[4];[12];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetAllDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 5 + 6 = 11, g2 = 6*10 + 7 = 67
        fixture.Exec("UPDATE TestTable SET a = 5, b = 6, c = 7 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[5];[6];[7];[4];[11];[67]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetSharedDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 1 + 7 = 8, g2 = 7*10 + 3 = 73
        fixture.Exec("UPDATE TestTable SET b = 7 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[7];[3];[4];[8];[73]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetIndependentDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 4 + 2 = 6, g2 = 2*10 + 9 = 29
        fixture.Exec("UPDATE TestTable SET a = 4, c = 9 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[4];[2];[9];[4];[6];[29]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetNonDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        fixture.Exec("UPDATE TestTable SET d = 42 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[3];[42];[3];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetDependencyToNull) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 1 + 0 = 1, g2 = 0*10 + 3 = 3
        fixture.Exec("UPDATE TestTable SET b = NULL WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];#;[3];[4];[1];[3]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // Matches k=1 only. g1 = 10 + 2 = 12, g2 = 23
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE b = 2;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[10];[2];[3];[4];[12];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGenerated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 == 3 matches k=1 only. g1 = 10 + 2 = 12, g2 = 23
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE g1 = 3;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[10];[2];[3];[4];[12];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGeneratedAndDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 3 unchanged (a, b untouched), g2 = 2*10 + 9 = 29
        fixture.Exec("UPDATE TestTable SET c = 9 WHERE g1 = 3 AND b = 2;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[9];[4];[3];[29]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGeneratedSetSharedDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // WHERE sees the old g2 == 23; after the write g1 = 1 + 7 = 8, g2 = 7*10 + 3 = 73
        fixture.Exec("UPDATE TestTable SET b = 7 WHERE g2 = 23;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[7];[3];[4];[8];[73]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGeneratedNoMatch) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        fixture.Exec("UPDATE TestTable SET a = 10 WHERE g1 = 999;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[3];[4];[3];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWithIndexOnGenerated) {
        TTestFixture fixture(IndexedGeneratedTableDDL);
        fixture.Exec("UPSERT INTO TestTable (k, a, b) VALUES (1, 1, 2);");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[[1;[3]]]");

        // g1 = 10 + 2 = 12
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE k = 1;");

        fixture.Check("SELECT k, a, b, g1 FROM TestTable ORDER BY k;", "[[1;[10];[2];[12]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 12;", "[[1;[12]]]");
    }

    Y_UNIT_TEST(UpdateReturningStarNoGeneratedUpdate) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET d = 55 WHERE k < 3 RETURNING *;",
            MultiGeneratedStarOrderSelect,
            "[[[1];[2];[3];[55];[3];[23];1];[[4];[5];[6];[55];[9];[56];2]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];[2];[3];[55];[3];[23]];[2;[4];[5];[6];[55];[9];[56]];" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateReturningStarWithGeneratedUpdate) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 100 + b, g2 untouched
        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100 WHERE k < 3 RETURNING *;",
            MultiGeneratedStarOrderSelect,
            "[[[100];[2];[3];[100];[102];[23];1];[[100];[5];[6];[100];[105];[56];2]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[100];[2];[3];[100];[102];[23]];[2;[100];[5];[6];[100];[105];[56]];"
            << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateReturningAllColumnsListed) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // b feeds both: g1 = a + 50, g2 = 50*10 + c
        fixture.CheckReturning(
            "UPDATE TestTable SET b = 50 WHERE k < 3 RETURNING k, a, b, c, d, g1, g2;",
            "SELECT k, a, b, c, d, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[1];[50];[3];[100];[51];[503]];[2;[4];[50];[6];[100];[54];[506]]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];[50];[3];[100];[51];[503]];[2;[4];[50];[6];[100];[54];[506]];"
            << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateReturningGeneratedUpdated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100 WHERE k < 3 RETURNING k, g1;",
            "SELECT k, g1 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[102]];[2;[105]]]");
    }

    Y_UNIT_TEST(UpdateReturningGeneratedNotUpdated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET d = 55 WHERE k < 3 RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[3];[23]];[2;[9];[56]]]");
    }

    Y_UNIT_TEST(UpdateReturningDependenciesUpdatedAndNot) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100 WHERE k < 3 RETURNING k, a, b, g1;",
            "SELECT k, a, b, g1 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[100];[2];[102]];[2;[100];[5];[105]]]");
    }

    Y_UNIT_TEST(UpdateReturningOneOfTwoGenerated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100 WHERE k < 3 RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[102];[23]];[2;[105];[56]]]");
    }

    Y_UNIT_TEST(UpdateReturningBothGeneratedSharedDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET b = 50 WHERE k < 3 RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[51];[503]];[2;[54];[506]]]");
    }

    Y_UNIT_TEST(UpdateReturningBothGeneratedIndependentDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100, c = 77 WHERE k < 3 RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[102];[97]];[2;[105];[127]]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[100];[2];[77];[100];[102];[97]];[2;[100];[5];[77];[100];[105];[127]];"
            << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateReturningNoGeneratedColumns) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET d = 55 WHERE k < 3 RETURNING k, d;",
            "SELECT k, d FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[55]];[2;[55]]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];[2];[3];[55];[3];[23]];[2;[4];[5];[6];[55];[9];[56]];" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateWhereGeneratedWithIndex) {
        TTestFixture fixture(IndexedGeneratedTableDDL);
        fixture.Exec("UPSERT INTO TestTable (k, a, b) VALUES (1, 1, 2), (2, 5, 5);");

        fixture.Exec("UPDATE TestTable SET b = 7 WHERE g1 = 3;");

        fixture.Check("SELECT k, a, b, g1 FROM TestTable ORDER BY k;", "[[1;[1];[7];[8]];[2;[5];[5];[10]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 8;", "[[1;[8]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 10;", "[[2;[10]]]");
    }

    Y_UNIT_TEST(UpdateOnGeneratedRejected) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Rejects("UPDATE TestTable ON (k, g1) VALUES (1, 5);", "cannot be set explicitly");
        fixture.Rejects("UPDATE TestTable ON (k, a, g2) VALUES (1, 5, 5);", "cannot be set explicitly");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow2, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(UpdateOnOneDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 10 + 2 = 12, g2 = 2*10 + 3 = 23
        fixture.Exec("UPDATE TestTable ON (k, a) VALUES (1, 10);");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[10];[2];[3];[100];[12];[23]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnAllDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 5 + 6 = 11, g2 = 6*10 + 7 = 67
        fixture.Exec("UPDATE TestTable ON (k, a, b, c) VALUES (1, 5, 6, 7);");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[5];[6];[7];[100];[11];[67]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnSharedDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 1 + 7 = 8, g2 = 7*10 + 3 = 73
        fixture.Exec("UPDATE TestTable ON (k, b) VALUES (1, 7);");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];[7];[3];[100];[8];[73]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnIndependentDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 4 + 2 = 6, g2 = 2*10 + 9 = 29
        fixture.Exec("UPDATE TestTable ON (k, a, c) VALUES (1, 4, 9);");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[4];[2];[9];[100];[6];[29]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnNonDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("UPDATE TestTable ON (k, d) VALUES (1, 42);");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];[2];[3];[42];[3];[23]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnDependencyToNull) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 1 + 0 = 1, g2 = 0*10 + 3 = 3
        fixture.Exec("UPDATE TestTable ON (k, b) VALUES (1, NULL);");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];#;[3];[100];[1];[3]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnMultipleRows) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // k=1: g1 = 10 + 2 = 12, k=2: g1 = 20 + 5 = 25
        fixture.Exec("UPDATE TestTable ON (k, a) VALUES (1, 10), (2, 20);");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[10];[2];[3];[100];[12];[23]];[2;[20];[5];[6];[100];[25];[56]];" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnMissingRow) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("UPDATE TestTable ON (k, a) VALUES (99, 1);");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow2, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(UpdateOnMissingAndExistingRows) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // Only k=1 exists: g1 = 10 + 2 = 12
        fixture.Exec("UPDATE TestTable ON (k, a) VALUES (1, 10), (99, 1);");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[10];[2];[3];[100];[12];[23]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnViaSelect) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 10 + 2 = 12
        fixture.Exec("UPDATE TestTable ON SELECT 1 AS k, 10 AS a;");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[10];[2];[3];[100];[12];[23]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpdateOnReturningGenerated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 10 + 2 = 12, g2 unchanged at 23
        fixture.CheckReturning(
            "UPDATE TestTable ON (k, a) VALUES (1, 10) RETURNING k, a, g1, g2;",
            "SELECT k, a, g1, g2 FROM TestTable WHERE k = 1;",
            "[[1;[10];[12];[23]]]");
    }

    Y_UNIT_TEST(UpdateOnReturningBothGenerated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // k=1: g1 = 1 + 7 = 8,  g2 = 7*10 + 3 = 73
        // k=2: g1 = 4 + 7 = 11, g2 = 7*10 + 6 = 76
        fixture.CheckReturning(
            "UPDATE TestTable ON (k, b) VALUES (1, 7), (2, 7) RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[8];[73]];[2;[11];[76]]]");
    }

    Y_UNIT_TEST(UpdateOnWithIndexOnGenerated) {
        TTestFixture fixture(IndexedGeneratedTableDDL,
            "UPSERT INTO TestTable (k, a, b) VALUES (1, 1, 2), (2, 4, 5), (3, 7, 8);");

        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[[1;[3]]]");

        // g1 = 10 + 2 = 12
        fixture.Exec("UPDATE TestTable ON (k, a) VALUES (1, 10);");

        fixture.Check("SELECT k, a, b, g1 FROM TestTable ORDER BY k;",
            "[[1;[10];[2];[12]];[2;[4];[5];[9]];[3;[7];[8];[15]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 12;", "[[1;[12]]]");
    }

    Y_UNIT_TEST(DeleteOnByKey) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable ON (k) VALUES (2);");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteOnMultipleRows) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable ON (k) VALUES (1), (3);");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow2}));
    }

    Y_UNIT_TEST(DeleteOnMissingRow) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable ON (k) VALUES (99);");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow2, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteOnViaSelect) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable ON SELECT 2 AS k;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteOnReturningGenerated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckUnordered("DELETE FROM TestTable ON (k) VALUES (2) RETURNING k, a, b, g1, g2;",
            "[[2;[4];[5];[9];[56]]]");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteOnWithIndexOnGenerated) {
        TTestFixture fixture(IndexedGeneratedTableDDL,
            "UPSERT INTO TestTable (k, a, b) VALUES (1, 1, 2), (2, 4, 5), (3, 7, 8);");

        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 9;", "[[2;[9]]]");

        fixture.Exec("DELETE FROM TestTable ON (k) VALUES (2);");

        fixture.Check("SELECT k, a, b, g1 FROM TestTable ORDER BY k;", "[[1;[1];[2];[3]];[3;[7];[8];[15]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 9;", "[]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[[1;[3]]]");
    }

    Y_UNIT_TEST(DeleteByPrimaryKey) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE k = 2;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteByGeneratedColumn) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE g1 = 9;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteByGeneratedColumnRange) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE g2 > 50;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1}));
    }

    Y_UNIT_TEST(DeleteByDependencyColumn) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE b = 5;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteByGeneratedAndDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE g1 = 9 AND b = 5;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteByGeneratedOrDependency) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE g1 = 3 OR c = 9;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow2}));
    }

    Y_UNIT_TEST(DeleteByBothGeneratedColumns) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE g1 = 9 AND g2 = 56;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteByGeneratedColumnIn) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE g1 IN (3, 15);");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow2}));
    }

    Y_UNIT_TEST(DeleteByGeneratedNoMatch) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE g1 = 999;");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow2, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteAllByGeneratedPredicate) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.Exec("DELETE FROM TestTable WHERE g1 > 0;");

        fixture.Check(MultiGeneratedSelect, RowsYson({}));
    }

    Y_UNIT_TEST(DeleteReturningStar) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckUnordered("DELETE FROM TestTable WHERE g1 = 9 RETURNING *;",
            "[[[4];[5];[6];[100];[9];[56];2]]");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteReturningGenerated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckUnordered("DELETE FROM TestTable WHERE b = 5 RETURNING k, g1, g2;", "[[2;[9];[56]]]");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteReturningDependenciesAndGenerated) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckUnordered("DELETE FROM TestTable WHERE g1 = 15 RETURNING k, a, b, g1;", "[[3;[7];[8];[15]]]");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow2}));
    }

    Y_UNIT_TEST(DeleteReturningMultipleRows) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckUnordered("DELETE FROM TestTable WHERE d = 100 RETURNING k, g1, g2;",
            "[[1;[3];[23]];[2;[9];[56]]]");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteReturningNoMatch) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckUnordered("DELETE FROM TestTable WHERE g1 = 999 RETURNING k, g1;", "[]");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1, MultiGeneratedRow2, MultiGeneratedRow3}));
    }

    Y_UNIT_TEST(DeleteWithIndexOnGenerated) {
        TTestFixture fixture(IndexedGeneratedTableDDL,
            "UPSERT INTO TestTable (k, a, b) VALUES (1, 1, 2), (2, 4, 5), (3, 7, 8);");

        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[[1;[3]]]");

        fixture.Exec("DELETE FROM TestTable WHERE k = 1;");

        fixture.Check("SELECT k, a, b, g1 FROM TestTable ORDER BY k;", "[[2;[4];[5];[9]];[3;[7];[8];[15]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 9;", "[[2;[9]]]");
    }

    Y_UNIT_TEST(DeleteByGeneratedWithIndex) {
        TTestFixture fixture(IndexedGeneratedTableDDL,
            "UPSERT INTO TestTable (k, a, b) VALUES (1, 1, 2), (2, 4, 5), (3, 7, 8);");

        fixture.Exec("DELETE FROM TestTable WHERE g1 = 9;");

        fixture.Check("SELECT k, a, b, g1 FROM TestTable ORDER BY k;", "[[1;[1];[2];[3]];[3;[7];[8];[15]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 9;", "[]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 15;", "[[3;[15]]]");
    }

    Y_UNIT_TEST(UpsertReturningStar) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // Partial UPSERT of an existing row: omitted b, c, d are read back. g1 = 100 + 2 = 102, g2 unchanged
        fixture.CheckReturning(
            "UPSERT INTO TestTable (k, a) VALUES (1, 100) RETURNING *;",
            "SELECT a, b, c, d, g1, g2, k FROM TestTable WHERE k = 1 ORDER BY k;",
            "[[[100];[2];[3];[100];[102];[23];1]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[100];[2];[3];[100];[102];[23]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(UpsertReturningNewRow) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // Brand-new row with every dependency supplied. g1 = 1 + 2 = 3, g2 = 2*10 + 3 = 23
        fixture.CheckReturning(
            "UPSERT INTO TestTable (k, a, b, c, d) VALUES (4, 1, 2, 3, 4) RETURNING *;",
            "SELECT a, b, c, d, g1, g2, k FROM TestTable WHERE k = 4 ORDER BY k;",
            "[[[1];[2];[3];[4];[3];[23];4]]");

        fixture.Check(MultiGeneratedSelect,
            RowsYson({MultiGeneratedRow1, MultiGeneratedRow2, MultiGeneratedRow3, "[4;[1];[2];[3];[4];[3];[23]]"}));
    }

    Y_UNIT_TEST(UpsertReturningAllColumns) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // b feeds both: g1 = 1 + 50 = 51, g2 = 50*10 + 3 = 503 (a, c, d read back)
        fixture.CheckReturning(
            "UPSERT INTO TestTable (k, b) VALUES (1, 50) RETURNING k, a, b, c, d, g1, g2;",
            "SELECT k, a, b, c, d, g1, g2 FROM TestTable WHERE k = 1;",
            "[[1;[1];[50];[3];[100];[51];[503]]]");
    }

    Y_UNIT_TEST(UpsertReturningGeneratedWithDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 100 + 2 = 102 (b read back)
        fixture.CheckReturning(
            "UPSERT INTO TestTable (k, a) VALUES (1, 100) RETURNING k, a, b, g1;",
            "SELECT k, a, b, g1 FROM TestTable WHERE k = 1;",
            "[[1;[100];[2];[102]]]");
    }

    Y_UNIT_TEST(UpsertReturningGeneratedWithoutDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // Only the independent d changes, so both generated columns keep their seeded values
        fixture.CheckReturning(
            "UPSERT INTO TestTable (k, d) VALUES (1, 55) RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k = 1;",
            "[[1;[3];[23]]]");
    }

    Y_UNIT_TEST(UpsertReturningIndependentColumn) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPSERT INTO TestTable (k, d) VALUES (1, 55) RETURNING k, d;",
            "SELECT k, d FROM TestTable WHERE k = 1;",
            "[[1;[55]]]");
    }

    Y_UNIT_TEST(UpsertReturningMultipleRows) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // k=1: g1 = 100 + 2 = 102, k=2: g1 = 200 + 5 = 205 (each b read back)
        fixture.CheckReturning(
            "UPSERT INTO TestTable (k, a) VALUES (1, 100), (2, 200) RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[102];[23]];[2;[205];[56]]]");
    }

    Y_UNIT_TEST(InsertReturningStar) {
        TTestFixture fixture(MultiGeneratedTableDDL);

        // g1 = 1 + 2 = 3, g2 = 2*10 + 3 = 23
        fixture.CheckReturning(
            "INSERT INTO TestTable (k, a, b, c, d) VALUES (1, 1, 2, 3, 100) RETURNING *;",
            "SELECT a, b, c, d, g1, g2, k FROM TestTable WHERE k = 1 ORDER BY k;",
            "[[[1];[2];[3];[100];[3];[23];1]]");

        fixture.Check(MultiGeneratedSelect, RowsYson({MultiGeneratedRow1}));
    }

    Y_UNIT_TEST(InsertReturningAllColumns) {
        TTestFixture fixture(MultiGeneratedTableDDL);

        fixture.CheckReturning(
            "INSERT INTO TestTable (k, a, b, c, d) VALUES (1, 1, 2, 3, 100) RETURNING k, a, b, c, d, g1, g2;",
            "SELECT k, a, b, c, d, g1, g2 FROM TestTable WHERE k = 1;",
            "[[1;[1];[2];[3];[100];[3];[23]]]");
    }

    Y_UNIT_TEST(InsertReturningGeneratedWithDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL);

        fixture.CheckReturning(
            "INSERT INTO TestTable (k, a, b, c, d) VALUES (1, 1, 2, 3, 100) RETURNING k, a, b, g1;",
            "SELECT k, a, b, g1 FROM TestTable WHERE k = 1;",
            "[[1;[1];[2];[3]]]");
    }

    Y_UNIT_TEST(InsertReturningGeneratedWithoutDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL);

        // b, c omitted -> NULL. g1 = COALESCE(5, 0) + 0 = 5, g2 = 0*10 + 0 = 0
        fixture.CheckReturning(
            "INSERT INTO TestTable (k, a, d) VALUES (1, 5, 100) RETURNING k, a, b, c, g1, g2;",
            "SELECT k, a, b, c, g1, g2 FROM TestTable WHERE k = 1;",
            "[[1;[5];#;#;[5];[0]]]");
    }

    Y_UNIT_TEST(InsertReturningIndependentColumn) {
        TTestFixture fixture(MultiGeneratedTableDDL);

        fixture.CheckReturning(
            "INSERT INTO TestTable (k, a, b, c, d) VALUES (1, 1, 2, 3, 100) RETURNING k, d;",
            "SELECT k, d FROM TestTable WHERE k = 1;",
            "[[1;[100]]]");
    }

    Y_UNIT_TEST(InsertReturningMultipleRows) {
        TTestFixture fixture(MultiGeneratedTableDDL);

        // Row1 g1 = 3, g2 = 23; Row2 g1 = 9, g2 = 56
        fixture.CheckReturning(
            "INSERT INTO TestTable (k, a, b, c, d) VALUES (1, 1, 2, 3, 100), (2, 4, 5, 6, 100) RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[3];[23]];[2;[9];[56]]]");
    }

    Y_UNIT_TEST(ReplaceReturningStar) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // REPLACE of an existing row resets omitted b, c, d to NULL. g1 = COALESCE(100, 0) + 0 = 100, g2 = 0
        fixture.CheckReturning(
            "REPLACE INTO TestTable (k, a) VALUES (1, 100) RETURNING *;",
            "SELECT a, b, c, d, g1, g2, k FROM TestTable WHERE k = 1 ORDER BY k;",
            "[[[100];#;#;#;[100];[0];1]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[100];#;#;#;[100];[0]];" << MultiGeneratedRow2 << ";" << MultiGeneratedRow3 << "]");
    }

    Y_UNIT_TEST(ReplaceReturningNewRow) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // Brand-new row. g1 = 1 + 2 = 3, g2 = 2*10 + 3 = 23
        fixture.CheckReturning(
            "REPLACE INTO TestTable (k, a, b, c, d) VALUES (4, 1, 2, 3, 4) RETURNING *;",
            "SELECT a, b, c, d, g1, g2, k FROM TestTable WHERE k = 4 ORDER BY k;",
            "[[[1];[2];[3];[4];[3];[23];4]]");

        fixture.Check(MultiGeneratedSelect,
            RowsYson({MultiGeneratedRow1, MultiGeneratedRow2, MultiGeneratedRow3, "[4;[1];[2];[3];[4];[3];[23]]"}));
    }

    Y_UNIT_TEST(ReplaceReturningAllColumns) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // Full row replaced. g1 = 5 + 6 = 11, g2 = 6*10 + 7 = 67
        fixture.CheckReturning(
            "REPLACE INTO TestTable (k, a, b, c, d) VALUES (1, 5, 6, 7, 8) RETURNING k, a, b, c, d, g1, g2;",
            "SELECT k, a, b, c, d, g1, g2 FROM TestTable WHERE k = 1;",
            "[[1;[5];[6];[7];[8];[11];[67]]]");
    }

    Y_UNIT_TEST(ReplaceReturningGeneratedWithDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "REPLACE INTO TestTable (k, a, b, c, d) VALUES (1, 5, 6, 7, 8) RETURNING k, a, b, g1;",
            "SELECT k, a, b, g1 FROM TestTable WHERE k = 1;",
            "[[1;[5];[6];[11]]]");
    }

    Y_UNIT_TEST(ReplaceReturningGeneratedWithoutDependencies) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // b, c reset to NULL. g1 = 5 + 0 = 5, g2 = 0*10 + 0 = 0
        fixture.CheckReturning(
            "REPLACE INTO TestTable (k, a) VALUES (1, 5) RETURNING k, a, b, c, g1, g2;",
            "SELECT k, a, b, c, g1, g2 FROM TestTable WHERE k = 1;",
            "[[1;[5];#;#;[5];[0]]]");
    }

    Y_UNIT_TEST(ReplaceReturningIndependentColumn) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "REPLACE INTO TestTable (k, a, b, c, d) VALUES (1, 5, 6, 7, 8) RETURNING k, d;",
            "SELECT k, d FROM TestTable WHERE k = 1;",
            "[[1;[8]]]");
    }

    Y_UNIT_TEST(ReplaceReturningMultipleRows) {
        TTestFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // Row1 g1 = 11, g2 = 67; Row2 g1 = 19, g2 = 10*10 + 11 = 111
        fixture.CheckReturning(
            "REPLACE INTO TestTable (k, a, b, c, d) VALUES (1, 5, 6, 7, 8), (2, 9, 10, 11, 12) RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[11];[67]];[2;[19];[111]]]");
    }

    Y_UNIT_TEST(AlterAddNotNullDefaultColumn) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                g Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 10);");
        fixture.Check("SELECT k, a, g FROM TestTable ORDER BY k;", "[[1;[10];[11]]]");

        fixture.Exec("ALTER TABLE TestTable ADD COLUMN c Int32 NOT NULL DEFAULT 7;");
        fixture.Check("SELECT k, a, g, c FROM TestTable ORDER BY k;", "[[1;[10];[11];7]]");

        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (2, 20);");
        fixture.Check("SELECT k, a, g, c FROM TestTable ORDER BY k;", "[[1;[10];[11];7];[2;[20];[21];7]]");
    }

    Y_UNIT_TEST(AlterAddCoveringIndexOverGenerated) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec("UPSERT INTO TestTable (k, a, b) VALUES (1, 10, 100), (2, 20, 200);");

        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_a GLOBAL SYNC ON (a) COVER (g);");

        fixture.Check("SELECT a, g FROM TestTable VIEW idx_a ORDER BY a;", "[[[10];[110]];[[20];[220]]]");
        fixture.Check("SELECT a, g FROM TestTable VIEW idx_a WHERE a = 10;", "[[[10];[110]]]");

        fixture.Exec("UPSERT INTO TestTable (k, b) VALUES (1, 500);");
        fixture.Check("SELECT a, g FROM TestTable VIEW idx_a WHERE a = 10;", "[[[10];[510]]]");

        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (2, 25);");
        fixture.Check("SELECT a, g FROM TestTable VIEW idx_a WHERE a = 20;", "[]");
        fixture.Check("SELECT a, g FROM TestTable VIEW idx_a WHERE a = 25;", "[[[25];[225]]]");

        fixture.Check("SELECT k, a, g FROM TestTable ORDER BY k;", "[[1;[10];[510]];[2;[25];[225]]]");
        fixture.Check("SELECT a, g FROM TestTable VIEW idx_a ORDER BY a;", "[[[10];[510]];[[25];[225]]]");
    }

    Y_UNIT_TEST(RandomGeneratedConsistentWithIndex) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                r Uint64 GENERATED ALWAYS AS (RandomNumber(1)) STORED,
                PRIMARY KEY (k),
                INDEX idx_a GLOBAL SYNC ON (a) COVER (r)
            );
        )");

        fixture.Exec("INSERT INTO TestTable (k, a) VALUES (1, 10), (2, 20), (3, 30);");

        const TString fromTable = fixture.QueryYson("SELECT a, r FROM TestTable ORDER BY a;");
        const TString fromIndex = fixture.QueryYson("SELECT a, r FROM TestTable VIEW idx_a ORDER BY a;");

        UNIT_ASSERT_C(!fromTable.Contains("#"), "random generated column is NULL in base table: " << fromTable);
        UNIT_ASSERT_VALUES_EQUAL_C(fromIndex, fromTable,
            "non-deterministic generated value diverged between the base table and the covering index");
    }

    Y_UNIT_TEST(GeneratedInPrimaryKeyRejected) {
        // A generated column cannot be part of the primary key
        CheckGeneratedColumnsRejected({
            {R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    g Int32 GENERATED ALWAYS AS (k + 1) STORED,
                    PRIMARY KEY (g)
                );
            )", "cannot be part of the primary key"},
        });
    }

    Y_UNIT_TEST(IndexOnGeneratedKeyUpdatesEntry) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                g Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )");

        // New row: g = 10 + 1 = 11, present in the index
        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 10);");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 11;", "[[1;[11]]]");
        fixture.Check("SELECT g FROM TestTable VIEW idx_g ORDER BY g;", "[[[11]]]");

        // Update the dependency so the generated index key changes: g = 20 + 1 = 21
        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 20);");

        // The stale key is gone, the new key is present, and the index holds exactly one row
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 11;", "[]");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 21;", "[[1;[21]]]");
        fixture.Check("SELECT g FROM TestTable VIEW idx_g ORDER BY g;", "[[[21]]]");
    }

    Y_UNIT_TEST(UniqueIndexOnGeneratedColumnRejectsCollisionsAtomically) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);

        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL UNIQUE SYNC ON (g)
            );
        )", R"(
            INSERT INTO TestTable (k, a) VALUES (1, 10), (2, 20), (3, 30);
        )", appConfig);

        const auto checkUnchanged = [&] {
            fixture.Check("SELECT k, a, g FROM TestTable ORDER BY k;", "[[1;10;10];[2;20;20];[3;30;30]]");
            fixture.Check("SELECT k, a, g FROM TestTable VIEW idx_g ORDER BY g;", "[[1;10;10];[2;20;20];[3;30;30]]");
        };
        checkUnchanged();

        // One row is valid and one collides with an existing generated key. Neither may persist.
        auto result = fixture.QuerySession().ExecuteQuery(R"(
            INSERT INTO TestTable (k, a) VALUES (4, 40), (5, 10);
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        checkUnchanged();

        // A collision produced entirely inside one input batch must also revert the full batch.
        result = fixture.QuerySession().ExecuteQuery(R"(
            INSERT INTO TestTable (k, a) VALUES (4, 40), (5, 40);
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        checkUnchanged();

        // k=2 collides with k=1 while k=3 would move to a free key. The whole UPDATE rolls back.
        result = fixture.QuerySession().ExecuteQuery(R"(
            UPDATE TestTable
            SET a = CASE k WHEN 2 THEN 10 ELSE 40 END
            WHERE k IN (2, 3);
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        checkUnchanged();
    }

    Y_UNIT_TEST(UniqueIndexOnNullableGeneratedColumnTracksNullTransitions) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);

        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                g Int32 GENERATED ALWAYS AS (a) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL UNIQUE SYNC ON (g)
            );
        )", "", appConfig);

        // SQL unique semantics permit more than one NULL generated key.
        fixture.Exec("INSERT INTO TestTable (k, a) VALUES (1, NULL), (2, NULL);");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g IS NULL ORDER BY k;", "[[1;#];[2;#]]");

        // NULL -> value creates the unique entry.
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE k = 1;");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g ORDER BY k;", "[[1;[10]];[2;#]]");

        auto result = fixture.QuerySession().ExecuteQuery("INSERT INTO TestTable (k, a) VALUES (3, 10);", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        fixture.Check("SELECT k, a, g FROM TestTable ORDER BY k;", "[[1;[10];[10]];[2;#;#]]");

        // value -> NULL removes the unique key, which can then be claimed by another row.
        fixture.Exec("UPDATE TestTable SET a = NULL WHERE k = 1;");
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE k = 2;");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g ORDER BY k;", "[[1;#];[2;[10]]]");

        fixture.Exec("UPDATE TestTable SET a = NULL WHERE k = 2;");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g IS NULL ORDER BY k;", "[[1;#];[2;#]]");
    }

    Y_UNIT_TEST(AsyncIndexesUseGeneratedColumnAsKeyAndCover) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                tag String NOT NULL,
                payload String NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL ASYNC ON (g) COVER (payload),
                INDEX idx_tag GLOBAL ASYNC ON (tag) COVER (g)
            );
        )");

        fixture.Exec(R"(
            INSERT INTO TestTable (k, a, tag, payload) VALUES
                (1, 10, "x", "one"),
                (2, 20, "y", "two");
        )");
        fixture.Exec("UPDATE TestTable SET a = 15, payload = \"one-updated\" WHERE k = 1;");
        fixture.Exec(R"(
            REPLACE INTO TestTable (k, a, tag, payload) VALUES (2, 25, "z", "replaced");
        )");
        fixture.Exec("DELETE FROM TestTable WHERE k = 1;");
        fixture.Exec(R"(
            INSERT INTO TestTable (k, a, tag, payload) VALUES (3, 30, "w", "three");
        )");

        fixture.Check("SELECT k, g, tag, payload FROM TestTable ORDER BY k;", R"([[2;26;"z";"replaced"];[3;31;"w";"three"]])");
        fixture.CheckStaleEventually("SELECT k, g, payload FROM TestTable VIEW idx_g ORDER BY g;", R"([[2;26;"replaced"];[3;31;"three"]])");
        fixture.CheckStaleEventually("SELECT k, tag, g FROM TestTable VIEW idx_tag ORDER BY tag;", R"([[3;"w";31];[2;"z";26]])");
    }

    Y_UNIT_TEST(AlterAddIndexBuildsGeneratedKeyForExistingRows) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                b Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + b) STORED,
                PRIMARY KEY (k)
            );
        )", R"(
            INSERT INTO TestTable (k, a, b) VALUES (1, 10, 1), (2, 20, 2);
        )");

        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_g GLOBAL SYNC ON (g) COVER (a, b);");
        fixture.Check("SELECT k, a, b, g FROM TestTable VIEW idx_g ORDER BY g;", "[[1;10;1;11];[2;20;2;22]]");

        fixture.Exec("UPDATE TestTable SET b = 15 WHERE k = 1;");
        fixture.Exec("DELETE FROM TestTable WHERE k = 2;");
        fixture.Exec("INSERT INTO TestTable (k, a, b) VALUES (3, 30, 3);");

        fixture.Check("SELECT k, a, b, g FROM TestTable VIEW idx_g ORDER BY g;", "[[1;10;15;25];[3;30;3;33]]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_g WHERE g IN (11, 22);", "[]");
    }

    Y_UNIT_TEST(CompositeGeneratedIndexWithCompositePrimaryKey) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                tenant Int32 NOT NULL,
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                b Int32 NOT NULL,
                g_sum Int32 NOT NULL GENERATED ALWAYS AS (a + b) STORED,
                g_delta Int32 NOT NULL GENERATED ALWAYS AS (a - b) STORED,
                PRIMARY KEY (tenant, k),
                INDEX idx_pair GLOBAL SYNC ON (g_sum, g_delta)
            );
        )", R"(
            INSERT INTO TestTable (tenant, k, a, b) VALUES
                (1, 1, 3, 1),
                (1, 2, 4, 2),
                (2, 1, 5, 3);
        )");

        fixture.Check("SELECT tenant, k, g_sum, g_delta FROM TestTable VIEW idx_pair ORDER BY g_sum, g_delta, tenant, k;", "[[1;1;4;2];[1;2;6;2];[2;1;8;2]]");

        fixture.Exec("UPDATE TestTable SET a = 10 WHERE tenant = 1 AND k = 2;");
        fixture.Check("SELECT tenant, k, g_sum, g_delta FROM TestTable VIEW idx_pair ORDER BY g_sum, g_delta, tenant, k;", "[[1;1;4;2];[2;1;8;2];[1;2;12;8]]");
        fixture.Check("SELECT tenant, k FROM TestTable VIEW idx_pair WHERE g_sum = 12 AND g_delta = 8;", "[[1;2]]");
        fixture.Check("SELECT tenant, k FROM TestTable VIEW idx_pair WHERE g_sum = 6 AND g_delta = 2;", "[]");
    }

    Y_UNIT_TEST(MultipleIndexesStayConsistentWithOneGeneratedColumn) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                b Int32 NOT NULL,
                bucket Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + b) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g),
                INDEX idx_g_bucket GLOBAL SYNC ON (g, bucket),
                INDEX idx_bucket GLOBAL SYNC ON (bucket) COVER (g)
            );
        )", R"(
            INSERT INTO TestTable (k, a, b, bucket) VALUES
                (1, 1, 2, 100),
                (2, 5, 6, 200);
        )");

        fixture.Exec(R"(
            UPDATE TestTable
            SET a = CASE k WHEN 1 THEN 20 ELSE a END,
                b = CASE k WHEN 2 THEN 8 ELSE b END
            WHERE k IN (1, 2);
        )");

        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g ORDER BY g;", "[[2;13];[1;22]]");
        fixture.Check("SELECT k, g, bucket FROM TestTable VIEW idx_g_bucket ORDER BY g, bucket;", "[[2;13;200];[1;22;100]]");
        fixture.Check("SELECT k, bucket, g FROM TestTable VIEW idx_bucket ORDER BY bucket;", "[[1;100;22];[2;200;13]]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_g WHERE g IN (3, 11);", "[]");
    }

    Y_UNIT_TEST(ChangefeedIncludesStoredAndExcludesVirtualColumns) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrSettings settings(appConfig);
        settings.SetWithSampleTables(false).SetPQConfig(GeneratedColumnsPQConfig());
        TKikimrRunner kikimr(settings);
        auto queryClient = kikimr.GetQueryClient();

        const auto exec = [&](const std::string& query) {
            auto result = queryClient.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        };

        exec(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                payload String,
                value Int32,
                stored_value Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(value, 0) * 10) STORED,
                virtual_value Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(value, 0) * 10 + 1) VIRTUAL,
                PRIMARY KEY (k)
            );
        )");
        exec(R"(
            ALTER TABLE `/Root/TestTable` ADD CHANGEFEED `feed` WITH (
                MODE = 'NEW_AND_OLD_IMAGES', FORMAT = 'JSON'
            );
        )");
        exec("ALTER TOPIC `/Root/TestTable/feed` ADD CONSUMER `test_consumer`;");

        exec(R"(
            INSERT INTO `/Root/TestTable` (k, payload, value) VALUES (1, "one", 10);
        )");
        exec(R"(
            UPDATE `/Root/TestTable` SET payload = "updated", value = 20 WHERE k = 1;
        )");

        auto check = queryClient.ExecuteQuery(R"(
            SELECT k, value, stored_value, virtual_value FROM `/Root/TestTable`;
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(check.IsSuccess(), check.GetIssues().ToString());
        CompareYson("[[1;[20];200;201]]", FormatResultSetYson(check.GetResultSet(0)));

        exec("DELETE FROM `/Root/TestTable` WHERE k = 1;");

        NYdb::NTopic::TTopicClient topicClient(kikimr.GetDriver());
        NYdb::NTopic::TReadSessionSettings readSettings;
        readSettings.ConsumerName("test_consumer");
        readSettings.AppendTopics(NYdb::NTopic::TTopicReadSettings().Path("/Root/TestTable/feed"));
        auto readSession = topicClient.CreateReadSession(readSettings);

        TVector<TString> messages;
        bool sawPartitionStart = false;
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (messages.size() < 3 && TInstant::Now() < deadline) {
            if (!readSession->WaitEvent().Wait(TDuration::Seconds(1))) {
                continue;
            }

            for (auto& event : readSession->GetEvents(false)) {
                if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                    for (auto& message : data->GetMessages()) {
                        messages.emplace_back(message.GetData());
                    }
                    data->Commit();
                } else if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                    start->Confirm();
                    sawPartitionStart = true;
                } else if (auto* stop = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                    stop->Confirm();
                } else if (auto* end = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
                    end->Confirm();
                } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
                    UNIT_FAIL("topic read session closed before all CDC messages arrived");
                } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
                    UNIT_FAIL("topic partition session closed before all CDC messages arrived");
                }
            }
        }

        UNIT_ASSERT_C(sawPartitionStart, "topic partition session did not start before the deadline");
        UNIT_ASSERT_VALUES_EQUAL_C(messages.size(), 3u, JoinSeq("\n", messages));

        bool sawInsert = false;
        bool sawUpdate = false;
        bool sawDelete = false;
        for (const auto& message : messages) {
            UNIT_ASSERT_C(!message.Contains("virtual_value"), message);

            NJson::TJsonValue json;
            UNIT_ASSERT_C(NJson::ReadJsonTree(message, &json), message);
            UNIT_ASSERT_C(json.Has("key"), message);
            UNIT_ASSERT_VALUES_EQUAL_C(json["key"][0].GetInteger(), 1, message);

            const bool hasNewImage = json.Has("newImage") && json["newImage"].IsMap();
            const bool hasOldImage = json.Has("oldImage") && json["oldImage"].IsMap();
            if (hasNewImage) {
                UNIT_ASSERT_C(json["newImage"].Has("stored_value"), message);
                UNIT_ASSERT_C(!json["newImage"].Has("virtual_value"), message);
            }
            if (hasOldImage) {
                UNIT_ASSERT_C(json["oldImage"].Has("stored_value"), message);
                UNIT_ASSERT_C(!json["oldImage"].Has("virtual_value"), message);
            }

            if (json.Has("erase")) {
                UNIT_ASSERT_C(!hasNewImage && hasOldImage, message);
                UNIT_ASSERT_VALUES_EQUAL_C(json["oldImage"]["value"].GetInteger(), 20, message);
                UNIT_ASSERT_VALUES_EQUAL_C(json["oldImage"]["stored_value"].GetInteger(), 200, message);
                UNIT_ASSERT_C(!sawDelete, message);
                sawDelete = true;
            } else {
                UNIT_ASSERT_C(json.Has("update") && hasNewImage, message);
                if (hasOldImage) {
                    UNIT_ASSERT_VALUES_EQUAL_C(json["oldImage"]["value"].GetInteger(), 10, message);
                    UNIT_ASSERT_VALUES_EQUAL_C(json["oldImage"]["stored_value"].GetInteger(), 100, message);
                    UNIT_ASSERT_VALUES_EQUAL_C(json["newImage"]["value"].GetInteger(), 20, message);
                    UNIT_ASSERT_VALUES_EQUAL_C(json["newImage"]["stored_value"].GetInteger(), 200, message);
                    UNIT_ASSERT_C(!sawUpdate, message);
                    sawUpdate = true;
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(json["newImage"]["value"].GetInteger(), 10, message);
                    UNIT_ASSERT_VALUES_EQUAL_C(json["newImage"]["stored_value"].GetInteger(), 100, message);
                    UNIT_ASSERT_C(!sawInsert, message);
                    sawInsert = true;
                }
            }
        }

        UNIT_ASSERT(sawInsert);
        UNIT_ASSERT(sawUpdate);
        UNIT_ASSERT(sawDelete);
        exec("ALTER TABLE `/Root/TestTable` DROP CHANGEFEED `feed`;");
    }

    Y_UNIT_TEST(CompileTimeDefaultsPartialUpsertAndReplace) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(true);

        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                dep Int32 DEFAULT 7,
                g Int32 NOT NULL GENERATED ALWAYS AS (k * 100 + COALESCE(dep, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TestTable (k, dep) VALUES (1, 5);", appConfig);

        // The existing row preserves dep=5 while the new row receives DEFAULT 7 in one request.
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;[5];105];[2;[7];207]]");

        // REPLACE constructs the complete row, so an omitted dependency receives its default
        // for both an existing and a new row.
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1), (3);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;[7];107];[2;[7];207];[3;[7];307]]");
    }

    Y_UNIT_TEST(CompileTimeDefaultsAlterDependencySetAndDrop) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(true);

        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                dep Int32,
                g Int32 NOT NULL
                    GENERATED ALWAYS AS (COALESCE(dep, 0) + 1) STORED,
                PRIMARY KEY (k)
            );
        )", "", appConfig);

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN dep SET DEFAULT 40;");
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;[40];41];[2;[40];41]]");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN dep DROP DEFAULT;");
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (3);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;#;1];[2;[40];41];[3;#;1]]");
    }

    Y_UNIT_TEST(CompileTimeDefaultsNonKeySerialDependency) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(true);

        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                dep Serial,
                g Int32 NOT NULL GENERATED ALWAYS AS (dep * 10) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TestTable (k, dep) VALUES (1, 50);", appConfig);

        // A generated sequence value is consumed for each input row. The existing row keeps dep=50,
        // while the new row receives the second sequence value.
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (3);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;50;500];[2;2;20];[3;3;30]]");
    }

    Y_UNIT_TEST(UpdateExpressionsUseOldValuesAndRecomputeStoredColumn) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                b Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a * 10 + b) STORED,
                PRIMARY KEY (k)
            );
        )", R"(
            INSERT INTO TestTable (k, a, b) VALUES
                (1, 1, 2),
                (2, 3, 4),
                (3, 5, 6);
        )");

        fixture.Exec("UPDATE TestTable SET a = a + 1 WHERE k = 1;");
        fixture.Exec("UPDATE TestTable SET a = b, b = a WHERE k = 2;");
        fixture.Exec("UPDATE TestTable SET a = g WHERE k = 3;");

        // Every RHS uses the old row. g is then evaluated once from the final dependency values.
        fixture.Check("SELECT k, a, b, g FROM TestTable ORDER BY k;", "[[1;2;2;22];[2;4;3;43];[3;56;6;566]]");
    }

    Y_UNIT_TEST(InplaceUpdateRecomputesStoredColumn) {
        auto appConfig = GeneratedColumnsAppConfig();
        auto unsafeCommitSetting = NKikimrKqp::TKqpSetting();
        unsafeCommitSetting.SetName("_KqpAllowUnsafeCommit");
        unsafeCommitSetting.SetValue("true");

        auto settings = TKikimrSettings(appConfig)
            .SetWithSampleTables(false)
            .SetKqpSettings({unsafeCommitSetting});
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/InplaceGenerated` (
                Key Uint64 NOT NULL,
                a Uint64 NOT NULL,
                g Uint64 NOT NULL GENERATED ALWAYS AS (a + 1ul) STORED,
                PRIMARY KEY (Key)
            ) WITH (
                PARTITION_AT_KEYS = (10)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scheme.GetStatus(), EStatus::SUCCESS,
            scheme.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/InplaceGenerated` (Key, a) VALUES
                (1u, 100u),
                (20u, 200u);
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            result.GetIssues().ToString());

        const TString update = R"(
            PRAGMA kikimr.OptEnableInplaceUpdate = 'true';
            DECLARE $key AS Uint64;

            UPDATE `/Root/InplaceGenerated` SET a = a + 1ul WHERE Key = $key;
        )";

        auto explain = session.ExplainDataQuery(update).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(explain.GetAst(), "Inplace", explain.GetAst());

        auto params = db.GetParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .Build();
        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        result = session.ExecuteDataQuery(update, NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats().has_value(), "inplace UPDATE returned no query stats");

        const auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases().size(), 1, stats.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(0).table_access().size(), 1, stats.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(0).table_access(0).name(), "/Root/InplaceGenerated", stats.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(0).table_access(0).reads().rows(), 1, stats.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(0).table_access(0).updates().rows(), 1, stats.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(stats.query_phases(0).table_access(0).partitions_count(), 2, stats.DebugString());

        result = session.ExecuteDataQuery(R"(
            SELECT Key, a, g FROM `/Root/InplaceGenerated` ORDER BY Key;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson("[[1u;101u;102u];[20u;200u;201u]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ExplicitTransactionReadYourWritesAndCommit) {
        TTestFixture fixture(R"(
            CREATE TABLE TxStored (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )");
        auto& session = fixture.QuerySession();

        auto insert = session.ExecuteQuery(R"(
            INSERT INTO TxStored (k, a, b) VALUES (1, 1, 2), (2, 2, 3);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());

        auto tx = insert.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto inserted = session.ExecuteQuery(R"(
            SELECT k, g FROM TxStored VIEW idx_g WHERE g IN (12, 23) ORDER BY g;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
        CompareYson("[[1;12];[2;23]]", FormatResultSetYson(inserted.GetResultSet(0)));

        tx = inserted.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto update = session.ExecuteQuery(R"(
            UPSERT INTO TxStored (k, a) VALUES (1, 5);
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(update.IsSuccess(), update.GetIssues().ToString());

        tx = update.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto updated = session.ExecuteQuery(R"(
            SELECT k, a, b, g FROM TxStored WHERE k = 1;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(updated.IsSuccess(), updated.GetIssues().ToString());
        CompareYson("[[1;[5];[2];52]]", FormatResultSetYson(updated.GetResultSet(0)));

        tx = updated.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto erase = session.ExecuteQuery(R"(
            DELETE FROM TxStored WHERE g = 23;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(erase.IsSuccess(), erase.GetIssues().ToString());

        tx = erase.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto commit = session.ExecuteQuery(R"(
            SELECT k, g FROM TxStored VIEW idx_g ORDER BY g;
        )", TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        CompareYson("[[1;52]]", FormatResultSetYson(commit.GetResultSet(0)));

        fixture.Check("SELECT k, a, b, g FROM TxStored ORDER BY k;", "[[1;[5];[2];52]]");
        fixture.Check("SELECT k, g FROM TxStored VIEW idx_g ORDER BY g;", "[[1;52]]");
    }

    Y_UNIT_TEST(ExplicitTransactionRollbackRestoresGeneratedState) {
        TTestFixture fixture(R"(
            CREATE TABLE TxStored (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )", "UPSERT INTO TxStored (k, a, b) VALUES (1, 1, 2);");
        auto& session = fixture.QuerySession();

        auto write = session.ExecuteQuery(R"(
            UPSERT INTO TxStored (k, a) VALUES (1, 3), (2, 4);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());

        auto tx = write.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto visible = session.ExecuteQuery(R"(
            SELECT k, a, b, g FROM TxStored ORDER BY k;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(visible.IsSuccess(), visible.GetIssues().ToString());
        CompareYson("[[1;[3];[2];32];[2;[4];#;40]]", FormatResultSetYson(visible.GetResultSet(0)));

        tx = visible.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto rollback = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT_C(rollback.IsSuccess(), rollback.GetIssues().ToString());

        fixture.Check("SELECT k, a, b, g FROM TxStored ORDER BY k;", "[[1;[1];[2];12]]");
        fixture.Check("SELECT k, g FROM TxStored VIEW idx_g ORDER BY g;", "[[1;12]]");
    }

    Y_UNIT_TEST(ExplicitTransactionPrimaryKeyConflictIsAtomic) {
        TTestFixture fixture(R"(
            CREATE TABLE TxStored (
                k Int32 NOT NULL,
                a Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )");
        auto& session = fixture.QuerySession();

        auto insert = session.ExecuteQuery(R"(
            INSERT INTO TxStored (k, a) VALUES (1, 1);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());

        auto tx = insert.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto visible = session.ExecuteQuery(R"(
            SELECT k, g FROM TxStored VIEW idx_g WHERE g = 10;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(visible.IsSuccess(), visible.GetIssues().ToString());
        CompareYson("[[1;10]]", FormatResultSetYson(visible.GetResultSet(0)));

        tx = visible.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto conflict = session.ExecuteQuery(R"(
            INSERT INTO TxStored (k, a) VALUES (1, 2);
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(conflict.GetStatus(), EStatus::PRECONDITION_FAILED, conflict.GetIssues().ToString());
        UNIT_ASSERT(!conflict.GetTransaction() || !conflict.GetTransaction()->IsActive());

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::NOT_FOUND, commit.GetIssues().ToString());
        UNIT_ASSERT_C(HasIssue(commit.GetIssues(), NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND), commit.GetIssues().ToString());

        fixture.Check("SELECT k, a, g FROM TxStored;", "[]");
        fixture.Check("SELECT k, g FROM TxStored VIEW idx_g;", "[]");
    }

    Y_UNIT_TEST(MultipleDmlStatementsCommitAndAbortAtomically) {
        TTestFixture fixture(R"(
            CREATE TABLE TxStored (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )");
        auto& session = fixture.QuerySession();

        auto success = session.ExecuteQuery(R"(
            INSERT INTO TxStored (k, a, b) VALUES (1, 1, 2), (2, 2, 3);
            UPDATE TxStored SET a = 5 WHERE k = 1;
            DELETE FROM TxStored WHERE g = 23;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(success.IsSuccess(), success.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TxStored ORDER BY k;", "[[1;[5];[2];52]]");

        auto failure = session.ExecuteQuery(R"(
            INSERT INTO TxStored (k, a, b) VALUES (3, 3, 4);
            INSERT INTO TxStored (k, a, b) VALUES (1, 9, 9);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(failure.GetStatus(), EStatus::PRECONDITION_FAILED, failure.GetIssues().ToString());
        UNIT_ASSERT(!failure.GetTransaction() || !failure.GetTransaction()->IsActive());
        fixture.Check("SELECT k, a, b, g FROM TxStored ORDER BY k;", "[[1;[5];[2];52]]");
    }

    Y_UNIT_TEST(StoredSourceSelectInsertUpsertReplace) {
        TTestFixture fixture(R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (2, 2, 7), (4, 4, 9);");
        fixture.Exec(R"(
            CREATE TABLE SourceRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec(R"(
            UPSERT INTO SourceRows (k, a, b) VALUES
                (1, 1, 2), (2, 5, 90), (3, 6, 80), (4, 8, 70);
        )");

        fixture.Exec(R"(
            INSERT INTO TargetRows (k, a, b)
            SELECT k, a, b FROM SourceRows WHERE k = 1;
        )");
        fixture.Exec(R"(
            UPSERT INTO TargetRows (k, a)
            SELECT k, a FROM SourceRows WHERE k IN (2, 3);
        )");
        fixture.Exec(R"(
            REPLACE INTO TargetRows (k, a)
            SELECT k, a FROM SourceRows WHERE k = 4;
        )");

        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;", "[[1;[1];[2];12];[2;[5];[7];57];[3;[6];#;60];[4;[8];#;80]]");
    }

    Y_UNIT_TEST(StoredSourceAsTableInsertUpsertReplaceAndMixedRows) {
        TTestFixture fixture(R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (11, 2, 7), (13, 4, 9);");
        auto& session = fixture.QuerySession();

        const auto params = TParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(10)
                        .AddMember("a").Int32(1)
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(11)
                        .AddMember("a").Int32(5)
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(12)
                        .AddMember("a").Int32(6)
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(13)
                        .AddMember("a").Int32(8)
                    .EndStruct()
                .EndList()
            .Build()
            .Build();

        auto insert = session.ExecuteQuery(R"(
            DECLARE $rows AS List<Struct<k:Int32,a:Int32>>;
            INSERT INTO TargetRows (k, a)
            SELECT k, a FROM AS_TABLE($rows) WHERE k = 10;
        )", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());

        auto upsert = session.ExecuteQuery(R"(
            DECLARE $rows AS List<Struct<k:Int32,a:Int32>>;
            UPSERT INTO TargetRows (k, a)
            SELECT k, a FROM AS_TABLE($rows) WHERE k IN (11, 12);
        )", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        auto replace = session.ExecuteQuery(R"(
            DECLARE $rows AS List<Struct<k:Int32,a:Int32>>;
            REPLACE INTO TargetRows (k, a)
            SELECT k, a FROM AS_TABLE($rows) WHERE k = 13;
        )", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(replace.IsSuccess(), replace.GetIssues().ToString());

        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;",
            "[[10;[1];#;10];[11;[5];[7];57];[12;[6];#;60];[13;[8];#;80]]");
    }

    Y_UNIT_TEST(StoredSourceScalarParameters) {
        TTestFixture fixture(R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (2, 2, 7), (3, 3, 9);");
        auto& session = fixture.QuerySession();

        auto insertParams = TParamsBuilder()
            .AddParam("$k").Int32(1).Build()
            .AddParam("$a").Int32(1).Build()
            .AddParam("$b").Int32(2).Build()
            .Build();
        auto insert = session.ExecuteQuery(R"(
            DECLARE $k AS Int32;
            DECLARE $a AS Int32;
            DECLARE $b AS Int32;
            INSERT INTO TargetRows (k, a, b) VALUES ($k, $a, $b);
        )", TTxControl::NoTx(), insertParams).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());

        auto upsertParams = TParamsBuilder()
            .AddParam("$k").Int32(2).Build()
            .AddParam("$a").Int32(5).Build()
            .Build();
        auto upsert = session.ExecuteQuery(R"(
            DECLARE $k AS Int32;
            DECLARE $a AS Int32;
            UPSERT INTO TargetRows (k, a) VALUES ($k, $a);
        )", TTxControl::NoTx(), upsertParams).ExtractValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        auto replaceParams = TParamsBuilder()
            .AddParam("$k").Int32(3).Build()
            .AddParam("$a").Int32(8).Build()
            .Build();
        auto replace = session.ExecuteQuery(R"(
            DECLARE $k AS Int32;
            DECLARE $a AS Int32;
            REPLACE INTO TargetRows (k, a) VALUES ($k, $a);
        )", TTxControl::NoTx(), replaceParams).ExtractValueSync();
        UNIT_ASSERT_C(replace.IsSuccess(), replace.GetIssues().ToString());

        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;", "[[1;[1];[2];12];[2;[5];[7];57];[3;[8];#;80]]");
    }

    Y_UNIT_TEST(StoredSourceEmptyInputsAreNoOp) {
        TTestFixture fixture(R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (1, 1, 2);");
        fixture.Exec(R"(
            CREATE TABLE SourceRows (
                k Int32 NOT NULL,
                a Int32,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("UPSERT INTO SourceRows (k, a) VALUES (2, 2);");

        fixture.Exec(R"(
            INSERT INTO TargetRows (k, a) SELECT k, a FROM SourceRows WHERE k < 0;
        )");
        fixture.Exec(R"(
            UPSERT INTO TargetRows (k, a) SELECT k, a FROM SourceRows WHERE k < 0;
        )");
        fixture.Exec(R"(
            REPLACE INTO TargetRows (k, a) SELECT k, a FROM SourceRows WHERE k < 0;
        )");

        auto& session = fixture.QuerySession();
        const auto params = TParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(3)
                        .AddMember("a").Int32(3)
                    .EndStruct()
                .EndList()
            .Build()
            .Build();
        for (const TStringBuf operation : {"INSERT", "UPSERT", "REPLACE"}) {
            const TString query = TStringBuilder() << R"(
                DECLARE $rows AS List<Struct<k:Int32,a:Int32>>;
            )" << operation << R"( INTO TargetRows (k, a)
                SELECT k, a FROM AS_TABLE($rows) WHERE k < 0;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), operation << ": " << result.GetIssues().ToString());
        }

        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;", "[[1;[1];[2];12]]");
    }

    Y_UNIT_TEST(StoredSourceDuplicateKeysAndInsertConflictAreAtomic) {
        TTestFixture fixture(R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (1, 1, 2);");
        auto& session = fixture.QuerySession();

        auto duplicateInsert = session.ExecuteQuery(R"(
            INSERT INTO TargetRows (k, a) VALUES (2, 2), (2, 3);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(duplicateInsert.GetStatus(), EStatus::PRECONDITION_FAILED, duplicateInsert.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;", "[[1;[1];[2];12]]");

        fixture.Exec("UPSERT INTO TargetRows (k, a) VALUES (2, 3), (2, 4);");
        fixture.Check("SELECT k, a, b, g FROM TargetRows WHERE k = 2;", "[[2;[4];#;40]]");

        auto existingConflict = session.ExecuteQuery(R"(
            INSERT INTO TargetRows (k, a, b) VALUES (3, 3, 4), (1, 9, 9);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(existingConflict.GetStatus(), EStatus::PRECONDITION_FAILED, existingConflict.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;",
            "[[1;[1];[2];12];[2;[4];#;40]]");
    }

    Y_UNIT_TEST(StoredSourceInsertOrRevertSuccessAndConflicts) {
        TTestFixture fixture(R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (1, 1, 2);");
        auto& session = fixture.QuerySession();

        auto success = session.ExecuteQuery(R"(
            INSERT OR REVERT INTO TargetRows (k, a, b) VALUES (2, 2, 3), (3, 3, 4);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(success.IsSuccess(), success.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TargetRows WHERE k IN (2, 3) ORDER BY k;", "[[2;[2];[3];23];[3;[3];[4];34]]");

        auto duplicate = session.ExecuteQuery(R"(
            INSERT OR REVERT INTO TargetRows (k, a) VALUES (4, 4), (5, 5), (4, 6);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(duplicate.IsSuccess(), duplicate.GetIssues().ToString());
        fixture.Check("SELECT k, g FROM TargetRows WHERE k IN (4, 5) ORDER BY k;", "[]");

        auto conflict = session.ExecuteQuery(R"(
            INSERT OR REVERT INTO TargetRows (k, a) VALUES (1, 9), (6, 6);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(conflict.IsSuccess(), conflict.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TargetRows WHERE k IN (1, 6) ORDER BY k;", "[[1;[1];[2];12]]");
    }

    Y_UNIT_TEST(StoredMultiShardDmlMatrix) {
        TTestFixture fixture(R"(
            CREATE TABLE MultiShardStored (
                k Uint32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED,
                UNIFORM_PARTITIONS = 4
            );
        )");

        fixture.Exec(R"(
            INSERT INTO MultiShardStored (k, a, b) VALUES
                (1u, 1, 1),
                (1000000001u, 2, 1),
                (3000000001u, 3, 1),
                (4000000000u, 4, 1);
        )");
        fixture.Check("SELECT k, g FROM MultiShardStored VIEW idx_g ORDER BY g;", "[[1u;11];[1000000001u;21];[3000000001u;31];[4000000000u;41]]");

        // Partial UPSERT preserves omitted dependencies for existing rows and uses NULL for new rows.
        fixture.Exec(R"(
            UPSERT INTO MultiShardStored (k, a) VALUES
                (1u, 5), (1000000002u, 6), (3000000001u, 7);
        )");
        fixture.Check("SELECT k, a, b, g FROM MultiShardStored WHERE k IN (1u, 1000000002u, 3000000001u) ORDER BY k;", "[[1u;[5];[1];51];[1000000002u;[6];#;60];[3000000001u;[7];[1];71]]");

        // REPLACE resets omitted dependencies on two different shards.
        fixture.Exec(R"(
            REPLACE INTO MultiShardStored (k, a) VALUES
                (1000000001u, 8), (4000000000u, 9);
        )");
        fixture.Check("SELECT k, a, b, g FROM MultiShardStored WHERE k IN (1000000001u, 4000000000u) ORDER BY k;", "[[1000000001u;[8];#;80];[4000000000u;[9];#;90]]");

        fixture.Exec(R"(
            UPDATE MultiShardStored SET b = 5 WHERE k IN (1u, 3000000001u);
        )");
        fixture.Exec(R"(
            UPDATE MultiShardStored ON (k, a) VALUES
                (1000000001u, 10), (4000000000u, 11);
        )");
        fixture.Check("SELECT k, a, b, g FROM MultiShardStored ORDER BY k;",
            "[[1u;[5];[5];55];[1000000001u;[10];#;100];[1000000002u;[6];#;60];"
            "[3000000001u;[7];[5];75];[4000000000u;[11];#;110]]");

        // A conflict on the last shard rolls back a new row routed to the first shard.
        auto conflict = fixture.QuerySession().ExecuteQuery(R"(
            INSERT INTO MultiShardStored (k, a, b) VALUES
                (2u, 2, 2), (4000000000u, 12, 12);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(conflict.GetStatus(), EStatus::PRECONDITION_FAILED, conflict.GetIssues().ToString());
        fixture.Check("SELECT k FROM MultiShardStored WHERE k = 2u;", "[]");

        fixture.Exec("DELETE FROM MultiShardStored WHERE g IN (55, 75);");
        fixture.Check("SELECT k, a, b, g FROM MultiShardStored ORDER BY k;", "[[1000000001u;[10];#;100];[1000000002u;[6];#;60];[4000000000u;[11];#;110]]");
        fixture.Check("SELECT k, g FROM MultiShardStored VIEW idx_g ORDER BY g;", "[[1000000002u;60];[1000000001u;100];[4000000000u;110]]");
    }

    Y_UNIT_TEST(StoredMultiShardBatchUpdateRecomputesGeneratedColumn) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->MutableBatchOperationSettings()->SetMaxBatchSize(2);
        appConfig.MutableTableServiceConfig()->MutableBatchOperationSettings()->SetPartitionExecutionLimit(2);
        TTestFixture fixture(R"(
            CREATE TABLE BatchStored (
                k Uint32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED,
                UNIFORM_PARTITIONS = 4
            );
        )", R"(
            UPSERT INTO BatchStored (k, a, b) VALUES
                (1u, 1, 1), (2u, 2, 2),
                (1100000000u, 3, 3), (1100000001u, 4, 4),
                (2200000000u, 5, 5), (2200000001u, 6, 6),
                (3300000000u, 7, 7), (3300000001u, 8, 8);
        )", appConfig);

        fixture.Exec(R"(
            BATCH UPDATE BatchStored SET a = 9 WHERE a % 2 = 1;
        )");
        const TString expected =
            "[[1u;[9];[1];91];[2u;[2];[2];22];"
            "[1100000000u;[9];[3];93];[1100000001u;[4];[4];44];"
            "[2200000000u;[9];[5];95];[2200000001u;[6];[6];66];"
            "[3300000000u;[9];[7];97];[3300000001u;[8];[8];88]]";
        fixture.Check("SELECT k, a, b, g FROM BatchStored ORDER BY k;", expected);
        fixture.Check("SELECT k, g FROM BatchStored VIEW idx_g ORDER BY g;",
            "[[2u;22];[1100000001u;44];[2200000001u;66];[3300000001u;88];"
            "[1u;91];[1100000000u;93];[2200000000u;95];[3300000000u;97]]");
    }

    Y_UNIT_TEST(StoredMultiShardBatchDeleteMaintainsGeneratedIndex) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->MutableBatchOperationSettings()->SetMaxBatchSize(2);
        appConfig.MutableTableServiceConfig()->MutableBatchOperationSettings()->SetPartitionExecutionLimit(2);
        TTestFixture fixture(R"(
            CREATE TABLE BatchStored (
                k Uint32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED,
                UNIFORM_PARTITIONS = 4
            );
        )", R"(
            UPSERT INTO BatchStored (k, a, b) VALUES
                (1u, 1, 1), (2u, 2, 2),
                (1100000000u, 3, 3), (1100000001u, 4, 4),
                (2200000000u, 5, 5), (2200000001u, 6, 6),
                (3300000000u, 7, 7), (3300000001u, 8, 8);
        )", appConfig);

        fixture.Exec("BATCH DELETE FROM BatchStored WHERE a % 2 = 1;");
        fixture.Check("SELECT k, a, b, g FROM BatchStored ORDER BY k;", "[[2u;[2];[2];22];[1100000001u;[4];[4];44];" "[2200000001u;[6];[6];66];[3300000001u;[8];[8];88]]");
        fixture.Check("SELECT k, g FROM BatchStored VIEW idx_g ORDER BY g;", "[[2u;22];[1100000001u;44];[2200000001u;66];[3300000001u;88]]");
    }

    Y_UNIT_TEST(StoredMultiShardSplitRetriesGeneratedWrites) {
        auto appConfig = GeneratedColumnsAppConfig();
        auto& writeActorSettings = *appConfig.MutableTableServiceConfig()->MutableWriteActorSettings();
        writeActorSettings.SetStartRetryDelayMs(100);
        writeActorSettings.SetMaxRetryDelayMs(1000);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false).SetUseRealThreads(false));
        auto client = kikimr.GetQueryClient();

        auto create = kikimr.RunCall([&] {
            return client.ExecuteQuery(R"(
                CREATE TABLE `/Root/SplitStored` (
                    k Uint32 NOT NULL,
                    a Int32,
                    b Int32,
                    g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                    PRIMARY KEY (k)
                ) WITH (
                    AUTO_PARTITIONING_BY_SIZE = DISABLED,
                    AUTO_PARTITIONING_BY_LOAD = DISABLED
                );
            )", TTxControl::NoTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();
        const auto initialShards = GetTableShards(&kikimr.GetTestServer(), edgeActor, "/Root/SplitStored");

        UNIT_ASSERT_VALUES_EQUAL_C(initialShards.size(), 1u, "expected one shard before the split");
        const ui64 splitShard = initialShards.front();

        std::unique_ptr<IEventHandle> heldWrite;
        std::atomic<ui64> writesToNewShards{0};
        THashSet<ui64> replacementShards;

        bool queryRequestPatched = false;
        bool interceptOriginalWrite = true;

        auto observer = [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            if (!queryRequestPatched && ev->GetTypeRewrite() == TEvKqp::TEvQueryRequest::EventType) {
                queryRequestPatched = true;
                auto* request = ev->Get<TEvKqp::TEvQueryRequest>();
                auto userContext = MakeIntrusive<TUserRequestContext>("", "/Root", "");
                userContext->IsStreamingQuery = true;
                request->SetUserRequestContext(std::move(userContext));
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            if (ev->GetTypeRewrite() == TEvPipeCache::EvForward) {
                auto* forward = ev->Get<TEvPipeCache::TEvForward>();
                if (forward->Ev && forward->Ev->Type() == NEvents::TDataEvents::TEvWrite::EventType) {
                    if (interceptOriginalWrite && forward->TabletId == splitShard && !heldWrite) {
                        heldWrite.reset(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    if (replacementShards.contains(forward->TabletId)) {
                        ++writesToNewShards;
                    }
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        auto savedObserver = runtime.SetObserverFunc(observer);
        Y_DEFER { runtime.SetObserverFunc(savedObserver); };

        auto session = kikimr.RunCall([&] {
            return client.GetSession().GetValueSync().GetSession();
        });

        auto future = kikimr.RunInThreadPool([&] {
            return session.ExecuteQuery(R"(
                UPSERT INTO `/Root/SplitStored` (k, a, b) VALUES
                    (1u, 1, 1), (5u, 5, 1), (9u, 9, 1),
                    (10u, 10, 1), (15u, 15, 1), (20u, 20, 1);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        });

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back([&](IEventHandle&) {
                return heldWrite.get() != nullptr;
            });
            runtime.DispatchEvents(options, TDuration::Seconds(30));
        }

        UNIT_ASSERT_C(heldWrite, "no in-flight write to the original shard was intercepted");

        SetSplitMergePartCountLimit(&runtime, -1);
        ui64 splitTxId = 0;
        for (ui32 attempt = 0; attempt < 120 && splitTxId == 0; ++attempt) {
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            request->Record.SetExecTimeoutPeriod(Max<ui64>());

            auto& modifyScheme = *request->Record.MutableTransaction()->MutableModifyScheme();
            modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);

            auto& split = *modifyScheme.MutableSplitMergeTablePartitions();
            split.SetTablePath("/Root/SplitStored");
            split.AddSourceTabletId(splitShard);
            split.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetUint32(10u);

            runtime.Send(new IEventHandle(MakeTxProxyID(), edgeActor, request.Release()), 0, true);
            auto response = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(edgeActor);
            if (response->Get()->Record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
                splitTxId = response->Get()->Record.GetTxId();
                break;
            }

            TDispatchOptions options;
            options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
            runtime.DispatchEvents(options, TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(splitTxId != 0, "the split was not accepted within the retry budget");

        auto notification = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        notification->Record.SetTxId(splitTxId);

        const auto schemeShard = NKikimr::Tests::ChangeStateStorage(NKikimr::Tests::SchemeRoot, kikimr.GetTestServer().GetSettings().Domain);
        runtime.SendToPipe(schemeShard, edgeActor, notification.Release(), 0, GetPipeConfigWithRetries());
        runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(edgeActor);

        const auto splitShards = GetTableShards(&kikimr.GetTestServer(), edgeActor, "/Root/SplitStored");
        UNIT_ASSERT_VALUES_EQUAL_C(splitShards.size(), 2u, "expected two shards after the split");
        UNIT_ASSERT_C(std::find(splitShards.begin(), splitShards.end(), splitShard) == splitShards.end(), "the split must replace the original shard");
        replacementShards.insert(splitShards.begin(), splitShards.end());

        interceptOriginalWrite = false;
        writesToNewShards = 0;
        runtime.Send(heldWrite.release());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back([&](IEventHandle&) {
                return writesToNewShards > 0;
            });
            runtime.DispatchEvents(options, TDuration::Seconds(30));
        }

        UNIT_ASSERT_C(writesToNewShards > 0, "the in-flight batch was not retried against the shards created by the split");

        auto write = runtime.WaitFuture(future, TDuration::Seconds(60));
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());

        auto check = kikimr.RunCall([&] {
            return session.ExecuteQuery(R"(
                SELECT k, g FROM `/Root/SplitStored` ORDER BY k;
            )", TTxControl::NoTx()).ExtractValueSync();
        });

        UNIT_ASSERT_C(check.IsSuccess(), check.GetIssues().ToString());
        CompareYson("[[1u;11];[5u;51];[9u;91];[10u;101];[15u;151];[20u;201]]", FormatResultSetYson(check.GetResultSet(0)));
    }

    Y_UNIT_TEST(DropGeneratedUnlocksDependencyAlterAndDrop) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TestTable (k, a) VALUES (1, 10);");

        fixture.Exec("ALTER TABLE TestTable DROP COLUMN g;");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN a DROP NOT NULL;");
        fixture.Exec("UPDATE TestTable SET a = NULL WHERE k = 1;");
        fixture.Check("SELECT k, a FROM TestTable;", "[[1;#]]");

        fixture.Exec("ALTER TABLE TestTable DROP COLUMN a;");
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k FROM TestTable ORDER BY k;", "[[1];[2]]");
    }

    Y_UNIT_TEST(TtlOnDependencyRejected) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                created Timestamp NOT NULL,
                expires Timestamp NOT NULL GENERATED ALWAYS AS (created) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Rejects(R"(
            ALTER TABLE TestTable
            SET (TTL = Interval("PT1H") ON created);
        )", "used by generated column 'expires'");

        fixture.Exec(R"(
            UPSERT INTO TestTable (k, created)
            VALUES (1, Timestamp("2021-01-01T00:00:00Z"));
        )");
        fixture.Check("SELECT k, created = expires FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(TtlOnIndependentColumnWorks) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto exec = [&](const std::string& query) {
            auto result = querySession.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        };

        exec(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                ttl_at Timestamp NOT NULL,
                PRIMARY KEY (k)
            );
        )");
        exec(R"(
            ALTER TABLE `/Root/TestTable`
            SET (TTL = Interval("PT1H") ON ttl_at);
        )");
        exec(R"(
            UPSERT INTO `/Root/TestTable` (k, a, ttl_at)
            VALUES (1, 10, Timestamp("2099-01-01T00:00:00Z"));
        )");

        auto selected = querySession.ExecuteQuery(
            "SELECT k, a, g FROM `/Root/TestTable`;", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(selected.IsSuccess(), selected.GetIssues().ToString());
        CompareYson("[[1;10;11]]", FormatResultSetYson(selected.GetResultSet(0)));

        auto tableSession = kikimr.GetTableClient().CreateSession().ExtractValueSync().GetSession();
        auto describe = tableSession.DescribeTable("/Root/TestTable").ExtractValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
        const auto ttl = describe.GetTableDescription().GetTtlSettings();
        UNIT_ASSERT_C(ttl, "TTL metadata is missing");
        UNIT_ASSERT_VALUES_EQUAL(ttl->GetDateTypeColumn().GetColumnName(), "ttl_at");
        UNIT_ASSERT_VALUES_EQUAL(ttl->GetDateTypeColumn().GetExpireAfter(), TDuration::Hours(1));
    }

    Y_UNIT_TEST(StoredGeneratedColumnFamilyLifecycle) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto exec = [&](const std::string& query) {
            auto result = querySession.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        };
        auto check = [&](const std::string& query, const TString& expected) {
            auto result = querySession.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        };
        auto checkFamily = [&](const TString& expected) {
            auto tableSession = kikimr.GetTableClient().CreateSession().ExtractValueSync().GetSession();
            auto describe = tableSession.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            for (const auto& column : describe.GetTableDescription().GetTableColumns()) {
                if (column.Name == "g") {
                    UNIT_ASSERT_VALUES_EQUAL(column.Family, expected);
                    return;
                }
            }
            UNIT_FAIL("generated column g is missing from DescribeTable");
        };

        exec(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 FAMILY Family1 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k),
                FAMILY Family1 (),
                FAMILY Family2 ()
            );
        )");
        exec("UPSERT INTO `/Root/TestTable` (k, a) VALUES (1, 10);");
        check("SELECT k, a, g FROM `/Root/TestTable`;", "[[1;10;11]]");
        checkFamily("Family1");

        exec("ALTER TABLE `/Root/TestTable` ALTER COLUMN g SET FAMILY Family2;");
        checkFamily("Family2");
        exec("UPDATE `/Root/TestTable` SET a = 20 WHERE k = 1;");
        check("SELECT k, a, g FROM `/Root/TestTable`;", "[[1;20;21]]");
    }

    Y_UNIT_TEST(TruncateGeneratedIndexedTable) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )");

        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 10), (2, 20);");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g ORDER BY g;", "[[1;11];[2;21]]");

        fixture.Exec("TRUNCATE TABLE TestTable;");
        fixture.Check("SELECT k, g FROM TestTable;", "[]");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g;", "[]");

        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (3, 30);");
        fixture.Check("SELECT k, a, g FROM TestTable;", "[[3;30;31]]");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 31;", "[[3;31]]");
    }

    Y_UNIT_TEST(CopyTablePreservesGeneratedColumn) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto exec = [&](const std::string& query) {
            auto result = querySession.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        };
        auto check = [&](const std::string& query, const TString& expected) {
            auto result = querySession.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        };

        exec(R"(
            CREATE TABLE `/Root/Source` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )");
        exec("UPSERT INTO `/Root/Source` (k, a) VALUES (1, 10);");

        auto tableSession = kikimr.GetTableClient().CreateSession().ExtractValueSync().GetSession();
        auto copy = tableSession.CopyTable("/Root/Source", "/Root/Copy").ExtractValueSync();
        UNIT_ASSERT_C(copy.IsSuccess(), copy.GetIssues().ToString());

        check("SELECT k, a, g FROM `/Root/Copy`;", "[[1;10;11]]");
        check("SELECT k, g FROM `/Root/Copy` VIEW idx_g WHERE g = 11;", "[[1;11]]");
        const auto ddl = GetShowCreateTable(querySession, "/Root/Copy");
        UNIT_ASSERT_STRING_CONTAINS_C(ddl, "GENERATED ALWAYS AS (a + 1) STORED", ddl);

        exec(R"(
            UPSERT INTO `/Root/Copy` (k, a) VALUES (2, 20);
            UPDATE `/Root/Copy` SET a = 30 WHERE k = 1;
        )");
        check("SELECT k, a, g FROM `/Root/Copy` ORDER BY k;", "[[1;30;31];[2;20;21]]");
        check("SELECT k, g FROM `/Root/Copy` VIEW idx_g WHERE g = 31;", "[[1;31]]");
        check("SELECT k, g FROM `/Root/Copy` VIEW idx_g WHERE g = 11;", "[]");
        check("SELECT k, a, g FROM `/Root/Source`;", "[[1;10;11]]");
        check("SELECT k, g FROM `/Root/Source` VIEW idx_g WHERE g = 11;", "[[1;11]]");
        check("SELECT k, g FROM `/Root/Source` VIEW idx_g WHERE g = 31;", "[]");
    }

    Y_UNIT_TEST(StoredGeneratedRejectedForColumnTable) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                g Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) STORED,
                PRIMARY KEY (k)
            ) WITH (STORE = COLUMN);
        )", "Generated columns are not supported in column tables");
    }

    Y_UNIT_TEST(StoredTypesPgDecimalStringUtf8) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                pg_value PgText NOT NULL,
                decimal_value Decimal(22, 9) NOT NULL,
                string_value String NOT NULL,
                utf8_value Utf8 NOT NULL,
                g_pg PgText NOT NULL GENERATED ALWAYS AS (pg_value) STORED,
                g_decimal Decimal(22, 9) NOT NULL GENERATED ALWAYS AS (decimal_value) STORED,
                g_string String NOT NULL GENERATED ALWAYS AS (string_value) STORED,
                g_utf8 Utf8 NOT NULL GENERATED ALWAYS AS (utf8_value) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec(R"(
            INSERT INTO TestTable (k, pg_value, decimal_value, string_value, utf8_value)
            VALUES (1, 'pg-one'pt, Decimal("12.34", 22, 9), "bytes-one", Utf8("Utf8-One"));
        )");
        fixture.Check(R"(
            SELECT g_pg, g_decimal, g_string, g_utf8 FROM TestTable;
        )", R"([["pg-one";"12.34";"bytes-one";"Utf8-One"]])");

        fixture.Exec(R"(
            UPDATE TestTable SET
                pg_value = 'pg-two'pt,
                decimal_value = Decimal("98.765", 22, 9),
                string_value = "bytes-two",
                utf8_value = Utf8("Utf8-Two")
            WHERE k = 1;
        )");
        fixture.Check(R"(
            SELECT g_pg, g_decimal, g_string, g_utf8 FROM TestTable;
        )", R"([["pg-two";"98.765";"bytes-two";"Utf8-Two"]])");
    }

    Y_UNIT_TEST(StoredTypesTemporal) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                date_value Date NOT NULL,
                datetime_value Datetime NOT NULL,
                timestamp_value Timestamp NOT NULL,
                interval_value Interval NOT NULL,
                g_date Date NOT NULL GENERATED ALWAYS AS (date_value) STORED,
                g_datetime Datetime NOT NULL GENERATED ALWAYS AS (datetime_value) STORED,
                g_timestamp Timestamp NOT NULL GENERATED ALWAYS AS (timestamp_value) STORED,
                g_interval Interval NOT NULL GENERATED ALWAYS AS (interval_value) STORED,
                PRIMARY KEY (k)
            );
        )");

        auto check = [&](const TString& date, const TString& datetime, const TString& timestamp, i64 intervalMicros) {
            auto result = fixture.QuerySession().ExecuteQuery(R"(
                SELECT g_date, g_datetime, g_timestamp, g_interval FROM TestTable;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g_date").GetDate(), TInstant::ParseIso8601(date));
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g_datetime").GetDatetime(), TInstant::ParseIso8601(datetime));
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g_timestamp").GetTimestamp(), TInstant::ParseIso8601(timestamp));
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g_interval").GetInterval(), intervalMicros);
            UNIT_ASSERT(!parser.TryNextRow());
        };

        fixture.Exec(R"(
            INSERT INTO TestTable (k, date_value, datetime_value, timestamp_value, interval_value)
            VALUES (
                1, Date("2007-07-07"), Datetime("2008-08-08T08:08:08Z"),
                Timestamp("2009-09-09T09:09:09.09Z"), Interval("P10D")
            );
        )");
        check("2007-07-07", "2008-08-08T08:08:08Z", "2009-09-09T09:09:09.09Z", TDuration::Days(10).MicroSeconds());

        fixture.Exec(R"(
            UPDATE TestTable SET
                date_value = Date("2010-10-10"),
                datetime_value = Datetime("2011-11-11T11:11:11Z"),
                timestamp_value = Timestamp("2012-12-12T12:12:12.123456Z"),
                interval_value = Interval("PT2H")
            WHERE k = 1;
        )");
        check("2010-10-10", "2011-11-11T11:11:11Z", "2012-12-12T12:12:12.123456Z", TDuration::Hours(2).MicroSeconds());
    }

    Y_UNIT_TEST(StoredTypesUuidDyNumberJsonDocument) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                uuid_value Uuid NOT NULL,
                dynumber_value DyNumber NOT NULL,
                document_value JsonDocument NOT NULL,
                g_uuid Uuid NOT NULL GENERATED ALWAYS AS (uuid_value) STORED,
                g_dynumber DyNumber NOT NULL GENERATED ALWAYS AS (dynumber_value) STORED,
                g_json JsonDocument NOT NULL GENERATED ALWAYS AS (document_value) STORED,
                PRIMARY KEY (k)
            );
        )");

        auto check = [&](const TString& uuid, const TString& dynumber, const TString& json) {
            auto result = fixture.QuerySession().ExecuteQuery(R"(
                SELECT g_uuid, g_dynumber, g_json FROM TestTable;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g_uuid").GetUuid().ToString(), uuid);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g_dynumber").GetDyNumber(), dynumber);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g_json").GetJsonDocument(), json);
            UNIT_ASSERT(!parser.TryNextRow());
        };

        fixture.Exec(R"(
            INSERT INTO TestTable (k, uuid_value, dynumber_value, document_value)
            VALUES (
                1, Uuid("550e8400-e29b-41d4-a716-446655440000"),
                DyNumber("15.15"), JsonDocument("[14]")
            );
        )");
        check("550e8400-e29b-41d4-a716-446655440000", ".1515e2", "[14]");

        fixture.Exec(R"(
            UPDATE TestTable SET
                uuid_value = Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe"),
                dynumber_value = DyNumber("60.5"),
                document_value = JsonDocument("[24]")
            WHERE k = 1;
        )");
        check("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe", ".605e2", "[24]");
    }

    Y_UNIT_TEST(StoredNullableResultWithIndex) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                enabled Bool NOT NULL,
                g Int32 GENERATED ALWAYS AS (
                    CASE WHEN enabled THEN a ELSE NULL END
                ) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )");

        fixture.Exec(R"(
            INSERT INTO TestTable (k, a, enabled) VALUES
                (1, 10, false),
                (2, 20, false),
                (3, 30, true);
        )");
        fixture.Check("SELECT k, g FROM TestTable ORDER BY k;", "[[1;#];[2;#];[3;[30]]]");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 30;", "[[3;[30]]]");

        fixture.Exec("UPDATE TestTable SET enabled = true WHERE k = 1;");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 10;", "[[1;[10]]]");

        fixture.Exec("UPDATE TestTable SET enabled = false WHERE k = 1;");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 10;", "[]");
        fixture.Check("SELECT k, g FROM TestTable WHERE k <= 2 ORDER BY k;", "[[1;#];[2;#]]");

        fixture.Exec("UPDATE TestTable SET enabled = true WHERE k = 2;");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g WHERE g = 20;", "[[2;[20]]]");
    }

    Y_UNIT_TEST(StoredComplexExpressionsMaterialize) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                n Int32 NOT NULL,
                utf8_value Utf8 NOT NULL,
                g_case Int32 NOT NULL GENERATED ALWAYS AS (
                    CASE WHEN n > 0 THEN n * 10 ELSE -1 END
                ) STORED,
                g_unicode Utf8 NOT NULL GENERATED ALWAYS AS (Unicode::ToLower(utf8_value)) STORED,
                g_list Int32 NOT NULL GENERATED ALWAYS AS (
                    COALESCE(
                        ListSum(ListMap(AsList(n, n + 1), ($item) -> {
                            RETURN $item * 2;
                        })),
                        0
                    )
                ) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec(R"(
            INSERT INTO TestTable (k, n, utf8_value) VALUES (1, 2, Utf8("MiXeD"));
        )");
        fixture.Check(R"(
            SELECT n, g_case, g_unicode, g_list FROM TestTable;
        )", R"([[2;20;"mixed";10]])");

        fixture.Exec(R"(
            UPDATE TestTable SET n = -1, utf8_value = Utf8("UPdAtEd") WHERE k = 1;
        )");
        fixture.Check(R"(
            SELECT n, g_case, g_unicode, g_list FROM TestTable;
        )", R"([[-1;-1;"updated";-2]])");
    }

    Y_UNIT_TEST(TableApiExecuteDataQueryMaterializesStoredColumn) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().ExtractValueSync().GetSession();

        auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TestTable` (k, a) VALUES (1, 10), (2, 20);
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            UPDATE `/Root/TestTable` SET a = 30 WHERE k = 1;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT k, a, g FROM `/Root/TestTable` ORDER BY k;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[1;30;31];[2;20;21]]", FormatResultSetYson(result.GetResultSet(0)));

        result = session.ExecuteDataQuery(R"(
            SELECT k, g FROM `/Root/TestTable` VIEW idx_g WHERE g = 31;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[1;31]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ReadTableReturnsStoredGeneratedColumn) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().ExtractValueSync().GetSession();

        auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a * 10) STORED,
                PRIMARY KEY (k)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/TestTable` (k, a) VALUES (1, 1), (2, 2);
            UPDATE `/Root/TestTable` SET a = 3 WHERE k = 1;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto settings = NYdb::NTable::TReadTableSettings()
            .Ordered()
            .AppendColumns("k")
            .AppendColumns("a")
            .AppendColumns("g");
        auto iterator = session.ReadTable("/Root/TestTable", settings).ExtractValueSync();
        UNIT_ASSERT_C(iterator.IsSuccess(), iterator.GetIssues().ToString());

        ui32 rowIndex = 0;
        for (;;) {
            auto part = iterator.ReadNext().ExtractValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            TResultSetParser parser(part.ExtractPart());
            while (parser.TryNextRow()) {
                if (rowIndex == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("k").GetOptionalInt32().value(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("a").GetOptionalInt32().value(), 3);
                    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g").GetOptionalInt32().value(), 30);
                } else if (rowIndex == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("k").GetOptionalInt32().value(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("a").GetOptionalInt32().value(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("g").GetOptionalInt32().value(), 20);
                } else {
                    UNIT_FAIL("ReadTable returned an unexpected extra row");
                }
                ++rowIndex;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(rowIndex, 2u);
    }

    Y_UNIT_TEST(PreparedQueryRecompilesAfterSchemaChange) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().ExtractValueSync().GetSession();

        auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        auto prepare = session.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            DECLARE $a AS Int32;
            UPSERT INTO `/Root/TestTable` (k, a) VALUES ($k, $a);
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepare.IsSuccess(), prepare.GetIssues().ToString());
        auto prepared = prepare.GetQuery();

        auto executePrepared = [&](i32 k, i32 a) {
            auto params = tableClient.GetParamsBuilder()
                .AddParam("$k").Int32(k).Build()
                .AddParam("$a").Int32(a).Build()
                .Build();
            for (ui32 attempt = 0; attempt < 5; ++attempt) {
                auto result = prepared.Execute(NYdb::NTable::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
                if (result.IsSuccess()) {
                    return;
                }
                UNIT_ASSERT_C(result.GetStatus() == EStatus::UNAVAILABLE || result.GetStatus() == EStatus::ABORTED, result.GetIssues().ToString());
            }
            UNIT_FAIL("prepared generated-column query did not recover after schema change");
        };

        executePrepared(1, 10);
        TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
        const auto recompilesBeforeAlter = counters.RecompileRequestGet()->Val();

        auto alter = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/TestTable` ADD COLUMN extra String;
        )").ExtractValueSync();
        UNIT_ASSERT_C(alter.IsSuccess(), alter.GetIssues().ToString());

        executePrepared(2, 20);
        UNIT_ASSERT_VALUES_EQUAL(counters.RecompileRequestGet()->Val(), recompilesBeforeAlter + 1);

        auto result = session.ExecuteDataQuery(R"(
            SELECT k, a, g FROM `/Root/TestTable` ORDER BY k;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[1;10;11];[2;20;21]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(CachedQueryInvalidatesAfterSchemaChange) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().ExtractValueSync().GetSession();

        auto scheme = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        const TString query = R"(
            DECLARE $k AS Int32;
            DECLARE $a AS Int32;
            UPSERT INTO `/Root/TestTable` (k, a) VALUES ($k, $a);
        )";
        auto executeCached = [&](i32 k, i32 a) {
            auto params = tableClient.GetParamsBuilder()
                .AddParam("$k").Int32(k).Build()
                .AddParam("$a").Int32(a).Build()
                .Build();
            auto settings = NYdb::NTable::TExecDataQuerySettings()
                .KeepInQueryCache(true)
                .CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetStats().has_value(), "cached query returned no compilation stats");
            return NYdb::TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache();
        };

        UNIT_ASSERT_VALUES_EQUAL(executeCached(1, 10), false);
        UNIT_ASSERT_VALUES_EQUAL(executeCached(1, 20), true);

        auto alter = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/TestTable` ADD COLUMN extra String;
        )").ExtractValueSync();
        UNIT_ASSERT_C(alter.IsSuccess(), alter.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(executeCached(1, 30), false);
        auto result = session.ExecuteDataQuery(R"(
            SELECT k, a, g FROM `/Root/TestTable`;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[1;30;31]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_TWIN(StreamWriteModesMaterializeStoredColumn, EnableStreamWrite) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (a * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL SYNC ON (g)
            );
        )", "", appConfig);

        fixture.Exec("INSERT INTO TestTable (k, a, b) VALUES (1, 10, 1);");
        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 20), (2, 30);");
        fixture.Exec("UPDATE TestTable SET b = 2 WHERE k = 2;");

        fixture.Check("SELECT k, a, b, g FROM TestTable ORDER BY k;", "[[1;20;[1];201];[2;30;[2];302]]");
        fixture.Check("SELECT k, g FROM TestTable VIEW idx_g ORDER BY g;", "[[1;201];[2;302]]");
    }

    Y_UNIT_TEST(StreamExecuteQueryMaterializesStoredColumn) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto queryClient = kikimr.GetQueryClient();

        auto setup = queryClient.ExecuteQuery(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k)
            );
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(setup.IsSuccess(), setup.GetIssues().ToString());

        setup = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/TestTable` (k, a) VALUES (1, 10);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(setup.IsSuccess(), setup.GetIssues().ToString());

        auto iterator = queryClient.StreamExecuteQuery(R"(
            UPDATE `/Root/TestTable` SET a = 20 WHERE k = 1 RETURNING k, a, g;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(iterator.IsSuccess(), iterator.GetIssues().ToString());
        CompareYson("[[1;20;21]]", StreamResultToYson(iterator));

        auto selected = queryClient.ExecuteQuery(R"(
            SELECT k, a, g FROM `/Root/TestTable`;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(selected.IsSuccess(), selected.GetIssues().ToString());
        CompareYson("[[1;20;21]]", FormatResultSetYson(selected.GetResultSet(0)));
    }

    Y_UNIT_TEST(ExecuteScriptMaterializesStoredColumn) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto queryClient = kikimr.GetQueryClient();

        auto setup = queryClient.ExecuteQuery(R"(
            CREATE TABLE `/Root/TestTable` (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + 1) STORED,
                PRIMARY KEY (k)
            );
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(setup.IsSuccess(), setup.GetIssues().ToString());

        setup = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/TestTable` (k, a) VALUES (1, 10);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(setup.IsSuccess(), setup.GetIssues().ToString());

        auto operation = queryClient.ExecuteScript(R"(
            UPDATE `/Root/TestTable` SET a = 40 WHERE k = 1;
            SELECT k, a, g FROM `/Root/TestTable`;
        )").ExtractValueSync();
        UNIT_ASSERT_C(operation.Status().IsSuccess(), operation.Status().GetIssues().ToString());

        NOperation::TOperationClient operationClient(kikimr.GetDriver());
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        while (!operation.Ready() && TInstant::Now() < deadline) {
            operation = operationClient.Get<TScriptExecutionOperation>(operation.Id()).ExtractValueSync();
            UNIT_ASSERT_C(operation.Status().IsSuccess(), operation.Status().GetIssues().ToString());
            if (!operation.Ready()) {
                Sleep(TDuration::MilliSeconds(10));
            }
        }
        UNIT_ASSERT_C(operation.Ready(), "script execution did not finish within the retry budget");
        UNIT_ASSERT_VALUES_EQUAL_C(operation.Metadata().ExecStatus, EExecStatus::Completed, operation.Status().GetIssues().ToString());

        auto fetched = queryClient.FetchScriptResults(operation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(fetched.IsSuccess(), fetched.GetIssues().ToString());
        CompareYson("[[1;40;41]]", FormatResultSetYson(fetched.GetResultSet()));

        auto selected = queryClient.ExecuteQuery(R"(
            SELECT k, a, g FROM `/Root/TestTable`;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(selected.IsSuccess(), selected.GetIssues().ToString());
        CompareYson("[[1;40;41]]", FormatResultSetYson(selected.GetResultSet(0)));
    }
}

Y_UNIT_TEST_SUITE(GeneratedStoredStreamLookup) {
    static constexpr const char* StreamLookupDDL = R"(
        CREATE TABLE GcTable (
            k Int32 NOT NULL,
            a Int32,
            b Int32,
            g Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED,
            PRIMARY KEY (k)
        );
    )";

    Y_UNIT_TEST(Insert) {
        TTestFixture fixture(StreamLookupDDL);
        // INSERT materializes a brand new row; missing dependencies default to NULL, never read back
        fixture.CheckStreamLookup("INSERT INTO GcTable (k, a) VALUES (1, 2);", /* expected */ false);
    }

    Y_UNIT_TEST(Replace) {
        TTestFixture fixture(StreamLookupDDL);
        // REPLACE overwrites the whole row; omitted dependencies become NULL, never read back
        fixture.CheckStreamLookup("REPLACE INTO GcTable (k, a) VALUES (1, 2);", /* expected */ false);
    }

    Y_UNIT_TEST(Upsert) {
        TTestFixture fixture(StreamLookupDDL);
        // Every dependency supplied -> generated value computed inline, no read-back
        fixture.CheckStreamLookup("UPSERT INTO GcTable (k, a, b) VALUES (1, 2, 3);", /* expected */ false);
        // Dependency b omitted -> its current value is read back via a stream lookup
        fixture.CheckStreamLookup("UPSERT INTO GcTable (k, a) VALUES (1, 2);", /* expected */ true);
    }

    Y_UNIT_TEST(UpdateOn) {
        TTestFixture fixture(StreamLookupDDL);
        // Every dependency supplied -> generated value computed inline, no read-back
        fixture.CheckStreamLookup("UPDATE GcTable ON (k, a, b) VALUES (1, 2, 3);", /* expected */ false);
        // Dependency b omitted -> its current value is read back via a stream lookup
        fixture.CheckStreamLookup("UPDATE GcTable ON (k, a) VALUES (1, 2);", /* expected */ true);
    }

    Y_UNIT_TEST(Update) {
        TTestFixture fixture(StreamLookupDDL);
        // UPDATE ... WHERE already reads the full row to apply the filter,
        // so the generated column is recomputed inline from those values
        fixture.CheckStreamLookup("UPDATE GcTable SET a = 2, b = 3 WHERE k = 1;", /* expected */ false);
        fixture.CheckStreamLookup("UPDATE GcTable SET a = 2 WHERE k = 1;", /* expected */ false);
    }

    Y_UNIT_TEST(DependenciesSurviveSchemeShardRestart) {
        TTestFixture fixture(StreamLookupDDL);

        fixture.CheckStreamLookup("UPSERT INTO GcTable (k, a) VALUES (1, 2);", /* expected */ true);
        fixture.Exec("UPSERT INTO GcTable (k, a, b) VALUES (5, 10, 100);");

        fixture.RestartSchemeShard("/Root/GcTable");

        fixture.CheckStreamLookup("UPSERT INTO GcTable (k, a) VALUES (3, 4) /* after restart */;", /* expected */ true);

        fixture.Exec("UPSERT INTO GcTable (k, a) VALUES (5, 20);");
        fixture.Check("SELECT k, a, b, g FROM GcTable WHERE k = 5;", "[[5;[20];[100];[120]]]");
    }
}

    Y_UNIT_TEST_SUITE(GeneratedVirtual) {
        Y_UNIT_TEST_TWIN(ReturningProjectionMatrix, EnableStreamWrite) {
            CheckVirtualReturningProjectionMatrix(EnableStreamWrite);
        }

        Y_UNIT_TEST_TWIN(ReturningSelectSourceForms, EnableStreamWrite) {
            auto appConfig = GeneratedColumnsAppConfig();
            appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);

            TTestFixture fixture(R"(
                CREATE TABLE VReturningSource (
                    k Int32 NOT NULL,
                    a Int32,
                    PRIMARY KEY (k)
                );
                CREATE TABLE VReturningTarget (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", R"(
                UPSERT INTO VReturningSource (k, a) VALUES
                    (1, 11), (2, 22), (3, 33), (4, 44), (5, 55);
                UPSERT INTO VReturningTarget (k, a) VALUES (4, 4), (5, 5);
            )", appConfig);

            fixture.CheckReturning(
                R"(
                    INSERT INTO VReturningTarget (k, a)
                    SELECT k, a FROM VReturningSource WHERE k = 1
                    RETURNING *;
                )",
                "SELECT a, k, v FROM VReturningTarget WHERE k = 1;",
                "[[[11];1;110]]");
            fixture.CheckReturning(
                R"(
                    INSERT OR REVERT INTO VReturningTarget (k, a)
                    SELECT k, a FROM VReturningSource WHERE k = 2
                    RETURNING v, a, k;
                )",
                "SELECT v, a, k FROM VReturningTarget WHERE k = 2;",
                "[[220;[22];2]]");
            fixture.CheckReturning(
                R"(
                    UPSERT INTO VReturningTarget (k, a)
                    SELECT k, a FROM VReturningSource WHERE k = 3
                    RETURNING k, v;
                )",
                "SELECT k, v FROM VReturningTarget WHERE k = 3;",
                "[[3;330]]");
            fixture.CheckReturning(
                R"(
                    REPLACE INTO VReturningTarget (k, a)
                    SELECT k, a FROM VReturningSource WHERE k = 4
                    RETURNING a, v, k;
                )",
                "SELECT a, v, k FROM VReturningTarget WHERE k = 4;",
                "[[[44];440;4]]");
            fixture.CheckReturning(
                R"(
                    UPDATE VReturningTarget ON
                    SELECT k, a FROM VReturningSource WHERE k = 5
                    RETURNING v, k, a;
                )",
                "SELECT v, k, a FROM VReturningTarget WHERE k = 5;",
                "[[550;5;[55]]]");
            CompareYson(
                "[[220;2]]",
                fixture.QueryYson(R"(
                    DELETE FROM VReturningTarget ON
                    SELECT k FROM VReturningSource WHERE k = 2
                    RETURNING v, k;
                )"));
            fixture.Check("SELECT k FROM VReturningTarget WHERE k = 2;", "[]");
        }

        Y_UNIT_TEST_TWIN(ReturningMultipleDmlStatements, EnableStreamWrite) {
            auto appConfig = GeneratedColumnsAppConfig();
            appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);

            TTestFixture fixture(R"(
                CREATE TABLE VReturningStatements (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "", appConfig);

            auto result = fixture.QuerySession().ExecuteQuery(R"(
                INSERT INTO VReturningStatements (k, a) VALUES (1, 10) RETURNING *;
                UPDATE VReturningStatements SET a = 20 WHERE v = 100 RETURNING v, k, a;
                DELETE FROM VReturningStatements WHERE v = 200 RETURNING k, v;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3u);
            CompareYson("[[[10];1;100]]", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson("[[200;1;[20]]]", FormatResultSetYson(result.GetResultSet(1)));
            CompareYson("[[1;200]]", FormatResultSetYson(result.GetResultSet(2)));
            fixture.Check("SELECT k FROM VReturningStatements;", "[]");
        }

        Y_UNIT_TEST_TWIN(ReturningAcrossInteractiveTransaction, EnableStreamWrite) {
            auto appConfig = GeneratedColumnsAppConfig();
            appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);

            TTestFixture fixture(R"(
                CREATE TABLE VReturningTx (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "", appConfig);
            auto& session = fixture.QuerySession();

            auto insert = session.ExecuteQuery(R"(
                INSERT INTO VReturningTx (k, a) VALUES (1, 10), (2, 20) RETURNING v, k;
            )", TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
            CompareYsonUnordered("[[100;1];[200;2]]", FormatResultSetYson(insert.GetResultSet(0)));
            auto insertTx = insert.GetTransaction();
            UNIT_ASSERT(insertTx && insertTx->IsActive());

            auto update = session.ExecuteQuery(R"(
                UPDATE VReturningTx SET a = 30 WHERE v = 100 RETURNING *;
            )", TTxControl::Tx(*insertTx)).ExtractValueSync();
            UNIT_ASSERT_C(update.IsSuccess(), update.GetIssues().ToString());
            CompareYson("[[[30];1;300]]", FormatResultSetYson(update.GetResultSet(0)));
            auto updateTx = update.GetTransaction();
            UNIT_ASSERT(updateTx && updateTx->IsActive());

            auto erase = session.ExecuteQuery(R"(
                DELETE FROM VReturningTx WHERE v = 200 RETURNING a, v, k;
            )", TTxControl::Tx(*updateTx).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(erase.IsSuccess(), erase.GetIssues().ToString());
            CompareYson("[[[20];200;2]]", FormatResultSetYson(erase.GetResultSet(0)));
            fixture.Check("SELECT k, a, v FROM VReturningTx ORDER BY k;", "[[1;[30];300]]");

            auto pendingInsert = session.ExecuteQuery(R"(
                UPSERT INTO VReturningTx (k, a) VALUES (3, 40) RETURNING *;
            )", TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(pendingInsert.IsSuccess(), pendingInsert.GetIssues().ToString());
            CompareYson("[[[40];3;400]]", FormatResultSetYson(pendingInsert.GetResultSet(0)));
            auto rollbackTx = pendingInsert.GetTransaction();
            UNIT_ASSERT(rollbackTx && rollbackTx->IsActive());

            auto pendingUpdate = session.ExecuteQuery(R"(
                UPDATE VReturningTx SET a = 50 WHERE v = 400 RETURNING v, k;
            )", TTxControl::Tx(*rollbackTx)).ExtractValueSync();
            UNIT_ASSERT_C(pendingUpdate.IsSuccess(), pendingUpdate.GetIssues().ToString());
            CompareYson("[[500;3]]", FormatResultSetYson(pendingUpdate.GetResultSet(0)));
            auto activeTx = pendingUpdate.GetTransaction();
            UNIT_ASSERT(activeTx && activeTx->IsActive());

            auto rollback = activeTx->Rollback().ExtractValueSync();
            UNIT_ASSERT_C(rollback.IsSuccess(), rollback.GetIssues().ToString());
            fixture.Check("SELECT k FROM VReturningTx WHERE k = 3;", "[]");
        }

        Y_UNIT_TEST_TWIN(ReturningAfterSchemaChanges, EnableStreamWrite) {
            auto appConfig = GeneratedColumnsAppConfig();
            appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);

            TTestFixture fixture(R"(
                CREATE TABLE VReturningDdl (
                    k Int32 NOT NULL,
                    a Int32,
                    tag String,
                    v Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "", appConfig);

            fixture.CheckReturning(
                "INSERT INTO VReturningDdl (k, a, tag) VALUES (1, 10, \"one\") RETURNING k, v;",
                "SELECT k, v FROM VReturningDdl WHERE k = 1;",
                "[[1;100]]");

            fixture.Exec("ALTER TABLE VReturningDdl ADD INDEX idx_a GLOBAL SYNC ON (a) COVER (tag);");
            fixture.CheckReturning(
                "UPDATE VReturningDdl SET a = 20 WHERE v = 100 RETURNING v, k, a;",
                "SELECT v, k, a FROM VReturningDdl WHERE k = 1;",
                "[[200;1;[20]]]");
            fixture.Check(
                "SELECT k, v FROM VReturningDdl VIEW idx_a WHERE a = 20;",
                "[[1;200]]");

            fixture.Exec("ALTER TABLE VReturningDdl ADD INDEX idx_tag GLOBAL UNIQUE ON (tag);");
            fixture.CheckReturning(
                "UPSERT INTO VReturningDdl (k, a, tag) VALUES (2, 25, \"two\") RETURNING *;",
                "SELECT a, k, tag, v FROM VReturningDdl WHERE k = 2;",
                "[[[25];2;[\"two\"];250]]");
            fixture.Check(
                "SELECT k, v FROM VReturningDdl VIEW idx_tag WHERE tag = \"two\";",
                "[[2;250]]");

            fixture.Exec("ALTER TABLE VReturningDdl DROP INDEX idx_a;");
            fixture.Exec("ALTER TABLE VReturningDdl DROP INDEX idx_tag;");
            fixture.Exec("ALTER TABLE VReturningDdl ADD INDEX idx_async GLOBAL ASYNC ON (tag) COVER (a);");
            fixture.CheckReturning(
                "REPLACE INTO VReturningDdl (k, a, tag) VALUES (2, 30, \"two-new\") RETURNING k, a, v;",
                "SELECT k, a, v FROM VReturningDdl WHERE k = 2;",
                "[[2;[30];300]]");
            fixture.CheckStaleEventually(
                "SELECT k, a, v FROM VReturningDdl VIEW idx_async WHERE tag = \"two-new\";",
                "[[2;[30];300]]");
            fixture.Exec("ALTER TABLE VReturningDdl DROP INDEX idx_async;");

            fixture.Exec("ALTER TABLE VReturningDdl ADD COLUMN extra Int32;");
            fixture.CheckReturning(
                "UPSERT INTO VReturningDdl (k, a, tag, extra) VALUES (3, 30, \"three\", 7) RETURNING extra, v, k;",
                "SELECT extra, v, k FROM VReturningDdl WHERE k = 3;",
                "[[[7];300;3]]");
            fixture.Exec("ALTER TABLE VReturningDdl DROP COLUMN extra;");

            fixture.Exec("ALTER TABLE `/Root/VReturningDdl` RENAME TO `/Root/VReturningDdlRenamed`;");
            fixture.CheckReturning(
                "UPDATE VReturningDdlRenamed SET a = 40 WHERE v = 300 RETURNING v, k;",
                "SELECT v, k FROM VReturningDdlRenamed WHERE k IN (2, 3);",
                "[[400;2];[400;3]]");

            fixture.Exec("TRUNCATE TABLE VReturningDdlRenamed;");
            fixture.Check("SELECT k FROM VReturningDdlRenamed;", "[]");
            fixture.CheckReturning(
                "INSERT INTO VReturningDdlRenamed (k, a, tag) VALUES (5, 5, \"after\") RETURNING *;",
                "SELECT a, k, tag, v FROM VReturningDdlRenamed WHERE k = 5;",
                "[[[5];5;[\"after\"];50]]");

            const auto ddl = fixture.ShowCreateTable("/Root/VReturningDdlRenamed");
            UNIT_ASSERT_STRING_CONTAINS_C(ddl,
                "GENERATED ALWAYS AS (COALESCE(a, 0) * 10) VIRTUAL", ddl);
        }

        Y_UNIT_TEST_TWIN(UpdateByVirtualReturningProjections, EnableStreamWrite) {
            auto appConfig = GeneratedColumnsAppConfig();
            appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);

            TTestFixture fixture(R"(
                CREATE TABLE VUpdateReturning (
                    k Uint32 NOT NULL,
                    v1 Uint32,
                    v2 Uint32 AS (v1 * 10u),
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VUpdateReturning (k, v1) VALUES (1, 10);", appConfig);

            fixture.CheckReturning(
                "UPDATE VUpdateReturning SET v1 = 20 WHERE v2 = 100 RETURNING *;",
                "SELECT k, v1, v2 FROM VUpdateReturning WHERE k = 1;",
                "[[1u;[20u];[200u]]]");
            fixture.CheckReturning(
                "UPDATE VUpdateReturning SET v1 = 30 WHERE v2 = 200 RETURNING k;",
                "SELECT k FROM VUpdateReturning WHERE k = 1;",
                "[[1u]]");
            fixture.CheckReturning(
                "UPDATE VUpdateReturning SET v1 = 40 WHERE v2 = 300 RETURNING v1;",
                "SELECT v1 FROM VUpdateReturning WHERE k = 1;",
                "[[[40u]]]");
            fixture.CheckReturning(
                "UPDATE VUpdateReturning SET v1 = 50 WHERE v2 = 400 RETURNING v2;",
                "SELECT v2 FROM VUpdateReturning WHERE k = 1;",
                "[[[500u]]]");
            fixture.CheckReturning(
                "UPDATE VUpdateReturning SET v1 = 60 WHERE v2 = 500 RETURNING v2, k, v1;",
                "SELECT v2, k, v1 FROM VUpdateReturning WHERE k = 1;",
                "[[[600u];1u;[60u]]]");
        }

        Y_UNIT_TEST(ReturningUsesStreamingSinkWithoutPrecompute) {
            auto appConfig = GeneratedColumnsAppConfig();
            appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(true);

            TTestFixture fixture(R"(
                CREATE TABLE VStreamSource (
                    k Int32 NOT NULL,
                    a Int32,
                    PRIMARY KEY (k)
                );
                CREATE TABLE VStreamTarget (
                    k Int32 NOT NULL,
                    a Int32,
                    b Int32 DEFAULT 7,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VStreamSource (k, a) VALUES (1, 1), (2, 2);", appConfig);

            const auto ast = fixture.ExplainAst(R"(
                UPSERT INTO VStreamTarget (k, a)
                SELECT k, a FROM VStreamSource
                RETURNING k, v;
            )");

            UNIT_ASSERT_STRING_CONTAINS(ast, "ReturningSink");
            UNIT_ASSERT_C(!ast.Contains("DqPrecompute") && !ast.Contains("DqPhyPrecompute"), ast);
        }

        Y_UNIT_TEST_TWIN(ReturningDmlMatrix, EnableStreamWrite) {
            CheckVirtualGeneratedReturning(EnableStreamWrite);
        }

        Y_UNIT_TEST(IndexStreamWriteDisabled) {
            TTestFixture fixture(R"(
                CREATE TABLE BaseTable (
                    k Int32 NOT NULL,
                    PRIMARY KEY (k)
                );
            )", "", GeneratedColumnsAppConfig(/* enableIndexStreamWrite */ false));

            fixture.Rejects(R"(
                CREATE TABLE VGenerated (
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "Generated columns require EnableIndexStreamWrite");
        }

        Y_UNIT_TEST(ReturningReplaceExistingRowUsesPostReplaceDefault) {
            TTestFixture fixture(R"(
                CREATE TABLE VReturningReplace (
                    a Int32,
                    b Int32 DEFAULT 7,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) VIRTUAL,
                    PRIMARY KEY (k),
                    INDEX idx_b GLOBAL ON (b)
                );
            )", "UPSERT INTO VReturningReplace (k, a, b) VALUES (1, 1, 9);");

            fixture.CheckReturning(
                "REPLACE INTO VReturningReplace (k, a) VALUES (1, 4) RETURNING k, b, v;",
                "SELECT k, b, v FROM VReturningReplace WHERE k = 1;",
                "[[1;[7];[47]]]");
        }

        Y_UNIT_TEST(ReturningRegressionMissingNullableDependency) {
            TTestFixture fixture(R"(
                CREATE TABLE VReturningNullable (
                    a Int32,
                    b Int32,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )");

            fixture.CheckReturning(
                "UPSERT INTO VReturningNullable (k, a) VALUES (1, 3) RETURNING k, v;",
                "SELECT k, v FROM VReturningNullable WHERE k = 1;",
                "[[1;[30]]]");
        }

        Y_UNIT_TEST(ReturningRegressionMixedExistingAndNew) {
            TTestFixture fixture(R"(
                CREATE TABLE VReturningMixed (
                    a Int32,
                    b Int32,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VReturningMixed (k, a, b) VALUES (1, 1, 2);");

            fixture.CheckReturning(
                "UPSERT INTO VReturningMixed (k, a) VALUES (1, 3), (2, 4) RETURNING k, v;",
                "SELECT k, v FROM VReturningMixed WHERE k IN (1, 2);",
                "[[1;[32]];[2;[40]]]");
        }

        Y_UNIT_TEST(ReturningRegressionLiteralDefaultDependency) {
            TTestFixture fixture(R"(
                CREATE TABLE VReturningDefault (
                    d Int32 DEFAULT 7,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (COALESCE(d, 0) * 10) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VReturningDefault (k, d) VALUES (1, 5);");

            fixture.CheckReturning(
                "UPSERT INTO VReturningDefault (k) VALUES (1) RETURNING k, d, v;",
                "SELECT k, d, v FROM VReturningDefault WHERE k = 1;",
                "[[1;[5];[50]]]");
            fixture.CheckReturning(
                "UPSERT INTO VReturningDefault (k) VALUES (2) RETURNING k, d, v;",
                "SELECT k, d, v FROM VReturningDefault WHERE k = 2;",
                "[[2;[7];[70]]]");
        }

        Y_UNIT_TEST(ReturningRegressionSequenceDefaultDependency) {
            TTestFixture fixture(R"(
                CREATE TABLE VReturningSequence (
                    k Int32 NOT NULL,
                    d Serial,
                    v Int32 GENERATED ALWAYS AS (d) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VReturningSequence (k, d) VALUES (1, 50);");

            // The generated sequence candidate is consumed once, but the old value wins on conflict.
            fixture.CheckReturning(
                "UPSERT INTO VReturningSequence (k) VALUES (1) RETURNING k, d, v;",
                "SELECT k, d, v FROM VReturningSequence WHERE k = 1;",
                "[[1;50;[50]]]");
            fixture.CheckReturning(
                "UPSERT INTO VReturningSequence (k) VALUES (2) RETURNING k, d, v;",
                "SELECT k, d, v FROM VReturningSequence WHERE k = 2;",
                "[[2;2;[2]]]");
            fixture.CheckReturning(
                "UPSERT INTO VReturningSequence (k) VALUES (3) RETURNING k, d, v;",
                "SELECT k, d, v FROM VReturningSequence WHERE k = 3;",
                "[[3;3;[3]]]");
        }

        Y_UNIT_TEST(ReturningRegressionVolatileDependency) {
            TTestFixture fixture(R"(
                CREATE TABLE VReturningVolatile (
                    k Int32 NOT NULL,
                    payload String,
                    v String GENERATED ALWAYS AS (payload) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )");

            const auto returned = fixture.QueryYson(R"(
                UPSERT INTO VReturningVolatile (k, payload)
                VALUES (1, CAST(RandomUuid(1) AS String))
                RETURNING k, v;
            )");
            const auto selected = fixture.QueryYson("SELECT k, v FROM VReturningVolatile WHERE k = 1;");
            CompareYson(returned, selected);
        }

        Y_UNIT_TEST(ReturningRegressionStoredAndVirtualColumns) {
            TTestFixture fixture(R"(
                CREATE TABLE VReturningStoredAndVirtual (
                    k Int32 NOT NULL,
                    a Int32,
                    stored Int32 GENERATED ALWAYS AS (a + 1) STORED,
                    virtual Int32 GENERATED ALWAYS AS (a + 2) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VReturningStoredAndVirtual (k, a) VALUES (1, 1);");

            fixture.CheckReturning(
                "UPSERT INTO VReturningStoredAndVirtual (k, a) VALUES (1, 10) RETURNING k, stored, virtual;",
                "SELECT k, stored, virtual FROM VReturningStoredAndVirtual WHERE k = 1;",
                "[[1;[11];[12]]]");
        }

        Y_UNIT_TEST(ReturningRegressionDuplicateKeys) {
            TTestFixture fixture(R"(
                CREATE TABLE VReturningDuplicates (
                    k Int32 NOT NULL,
                    a Int32,
                    b Int32,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VReturningDuplicates (k, a, b) VALUES (1, 1, 2);");

            const auto returned = fixture.QueryYson(
                "UPSERT INTO VReturningDuplicates (k, a) VALUES (1, 10), (1, 20) RETURNING k, v;");
            CompareYsonUnordered("[[1;[102]];[1;[202]]]", returned);
            fixture.Check("SELECT k, v FROM VReturningDuplicates WHERE k = 1;", "[[1;[202]]]");
        }

        Y_UNIT_TEST(ReadProjectionAndStar) {
            TTestFixture fixture(VirtualReadTableDDL, VirtualReadSeed);

            fixture.Check("SELECT k, v FROM VRead ORDER BY k;",
                          "[[1;[11]];[2;[22]];[3;[3]]]");
            fixture.Check("SELECT * FROM VRead ORDER BY k;",
                          "[[[1];[1];1;1;[2];[11]];[[2];[2];1;2;[4];[22]];[#;[3];2;3;[3];[3]]]");
        }

        Y_UNIT_TEST(ReadFilterGroupHavingAndOrder) {
            TTestFixture fixture(VirtualReadTableDDL, VirtualReadSeed);

            fixture.Check("SELECT k FROM VRead WHERE v = 22;", "[[2]]");
            fixture.Check("SELECT k FROM VRead ORDER BY v;", "[[3];[1];[2]]");
            fixture.Check("SELECT grp, SUM(v) FROM VRead GROUP BY grp ORDER BY grp;",
                          "[[1;[33]];[2;[3]]]");
            fixture.Check("SELECT grp FROM VRead GROUP BY grp HAVING SUM(v) > 10 ORDER BY grp;",
                          "[[1]]");
        }

        Y_UNIT_TEST(ReadWindowAndSubquery) {
            TTestFixture fixture(VirtualReadTableDDL, VirtualReadSeed);

            fixture.Check(R"(
                SELECT k, ROW_NUMBER() OVER (ORDER BY v) AS rn
                FROM VRead
                ORDER BY k;
            )", "[[1;2u];[2;3u];[3;1u]]");

            fixture.Check(R"(
                SELECT k
                FROM VRead
                WHERE v IN (SELECT v FROM VRead WHERE grp = 1)
                ORDER BY k;
            )", "[[1];[2]]");
        }

        Y_UNIT_TEST(ReadJoinAndConditionalDml) {
            TTestFixture fixture(VirtualReadTableDDL, VirtualReadSeed);

            fixture.Check(R"(
                SELECT l.k, r.k
                FROM VRead AS l
                INNER JOIN VRead AS r ON l.v = r.v
                WHERE l.k = 1;
            )", "[[1;1]]");

            fixture.Exec("UPDATE VRead SET b = v WHERE v = 11;");
            fixture.Check("SELECT k, b, v FROM VRead WHERE k = 1;", "[[1;[11];[21]]]");

            fixture.Exec("DELETE FROM VRead WHERE v = 22;");
            fixture.Check("SELECT k FROM VRead ORDER BY k;", "[[1];[3]]");
        }

        Y_UNIT_TEST(ReadDependencyFreeVirtualColumn) {
            TTestFixture fixture(R"(
                CREATE TABLE VConst (
                    c Int32 GENERATED ALWAYS AS (5) VIRTUAL,
                    k Int32 NOT NULL,
                    PRIMARY KEY (k)
                );
            )");

            fixture.Exec("UPSERT INTO VConst (k) VALUES (1), (2);");
            // Reading only a dependency-free virtual column must anchor the physical read by PK.
            fixture.CheckUnordered("SELECT c FROM VConst;", "[[[5]];[[5]]]");
            fixture.CheckReturning(
                "UPSERT INTO VConst (k) VALUES (3) RETURNING c;",
                "SELECT c FROM VConst WHERE k = 3;",
                "[[[5]]]");
        }

        Y_UNIT_TEST(ReadMultipleVirtualColumnsPreservesRequestedOrder) {
            TTestFixture fixture(R"(
                CREATE TABLE VMultiple (
                    a Int32 NOT NULL,
                    b Int32 NOT NULL,
                    k Int32 NOT NULL,
                    physical Int32 NOT NULL,
                    v Int32 NOT NULL GENERATED ALWAYS AS (a + b) VIRTUAL,
                    v1 Int32 NOT NULL GENERATED ALWAYS AS (a + b) VIRTUAL,
                    v2 Int32 NOT NULL GENERATED ALWAYS AS (b + physical) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VMultiple (k, a, b, physical) VALUES (1, 2, 3, 7);");

            fixture.Check("SELECT v, k FROM VMultiple;", "[[5;1]]");
            fixture.Check("SELECT v2, physical, v1 FROM VMultiple;", "[[10;7;5]]");
        }

        Y_UNIT_TEST(ReadVirtualExpressionReturningNull) {
            TTestFixture fixture(R"(
                CREATE TABLE VNull (
                    a Int32,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (a + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VNull (k, a) VALUES (1, NULL), (2, 4);");

            fixture.Check("SELECT v FROM VNull WHERE k = 1;", "[[#]]");
            fixture.Check("SELECT v FROM VNull WHERE k = 2;", "[[[5]]]");
        }

        Y_UNIT_TEST(ReadVirtualColumnsWithParameterizedAndPgTypes) {
            TTestFixture fixture(R"(
                CREATE TABLE VTypes (
                    k Int32 NOT NULL,
                    nullable_int Int32,
                    nonnull_int Int32 NOT NULL,
                    pg_payload PgText NOT NULL,
                    decimal_value Decimal(22, 9) NOT NULL,
                    string_value String NOT NULL,
                    date_value Date NOT NULL,
                    datetime_value Datetime NOT NULL,
                    timestamp_value Timestamp NOT NULL,
                    v_nullable Int32 GENERATED ALWAYS AS (nullable_int + 1) VIRTUAL,
                    v_nonnull Int32 NOT NULL GENERATED ALWAYS AS (nonnull_int + 1) VIRTUAL,
                    v_pg PgText NOT NULL GENERATED ALWAYS AS (pg_payload) VIRTUAL,
                    v_decimal Decimal(22, 9) NOT NULL GENERATED ALWAYS AS (decimal_value) VIRTUAL,
                    v_string String NOT NULL GENERATED ALWAYS AS (string_value) VIRTUAL,
                    v_date Date NOT NULL GENERATED ALWAYS AS (date_value) VIRTUAL,
                    v_datetime Datetime NOT NULL GENERATED ALWAYS AS (datetime_value) VIRTUAL,
                    v_timestamp Timestamp NOT NULL GENERATED ALWAYS AS (timestamp_value) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", R"(
                UPSERT INTO VTypes (
                    k, nullable_int, nonnull_int, pg_payload, decimal_value, string_value,
                    date_value, datetime_value, timestamp_value
                ) VALUES (
                    1, NULL, 7, 'pg-value'pt, Decimal("12.34", 22, 9), "bytes",
                    Date("2021-01-01"), Datetime("2021-01-01T01:02:03Z"),
                    Timestamp("2021-01-01T01:02:03.123456Z")
                );
            )");

            fixture.Check(R"(
                SELECT
                    v_nullable, v_nonnull, v_pg, v_decimal, v_string,
                    v_date, v_datetime, v_timestamp
                FROM VTypes;
            )", R"([[#;8;"pg-value";"12.34";"bytes";18628u;1609462923u;1609462923123456u]])");
        }

        Y_UNIT_TEST(ReturningNoMatchDoesNotSynthesizeVirtualRows) {
            TTestFixture fixture(R"(
                CREATE TABLE VNoMatch (
                    a Int32,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VNoMatch (k, a) VALUES (1, 10);");

            fixture.CheckUnordered(
                "UPDATE VNoMatch SET a = 20 WHERE k = 999 RETURNING v;", "[]");
            fixture.CheckUnordered(
                "DELETE FROM VNoMatch WHERE k = 999 RETURNING v;", "[]");
            fixture.CheckUnordered(
                "UPDATE VNoMatch ON (k, a) VALUES (998, 30) RETURNING v;", "[]");
            fixture.CheckUnordered(
                "DELETE FROM VNoMatch ON (k) VALUES (997) RETURNING v;", "[]");
            fixture.Check("SELECT k, a, v FROM VNoMatch;", "[[1;[10];[11]]]");
        }

        Y_UNIT_TEST(SchemeShardRestartFirstActionSelectsVirtualColumn) {
            TTestFixture fixture(R"(
                CREATE TABLE VRestartSelect (
                    a Int32,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VRestartSelect (k, a) VALUES (1, 10);");

            fixture.RestartSchemeShard("/Root/VRestartSelect");
            fixture.Check("SELECT v FROM VRestartSelect WHERE k = 1;", "[[[11]]]");
        }

        Y_UNIT_TEST(SchemeShardRestartFirstActionDeletesByVirtualColumn) {
            TTestFixture fixture(R"(
                CREATE TABLE VRestartDelete (
                    a Int32,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VRestartDelete (k, a) VALUES (1, 10), (2, 20);");

            fixture.RestartSchemeShard("/Root/VRestartDelete");
            fixture.Exec("DELETE FROM VRestartDelete WHERE v = 11;");
            fixture.Check("SELECT k, v FROM VRestartDelete;", "[[2;[21]]]");
        }

        Y_UNIT_TEST(SchemeShardRestartFirstActionReturnsVirtualColumn) {
            TTestFixture fixture(R"(
                CREATE TABLE VRestartReturning (
                    a Int32,
                    k Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )", "UPSERT INTO VRestartReturning (k, a) VALUES (1, 10);");

            fixture.RestartSchemeShard("/Root/VRestartReturning");
            fixture.CheckReturning(
                "UPDATE VRestartReturning SET a = 30 WHERE k = 1 RETURNING v;",
                "SELECT v FROM VRestartReturning WHERE k = 1;",
                "[[[31]]]");
        }

        Y_UNIT_TEST(ExplainVirtualReadExpansionAndIndexSelection) {
            TTestFixture fixture(R"(
                CREATE TABLE VExplain (
                    a Int32 NOT NULL,
                    b Int32 NOT NULL,
                    grp Int32 NOT NULL,
                    k Int32 NOT NULL,
                    v_covered Int32 NOT NULL GENERATED ALWAYS AS (a + 1) VIRTUAL,
                    v_same Int32 NOT NULL GENERATED ALWAYS AS (a + 2) VIRTUAL,
                    v_uncovered Int32 NOT NULL GENERATED ALWAYS AS (b + 1) VIRTUAL,
                    PRIMARY KEY (k),
                    INDEX idx_grp GLOBAL ON (grp) COVER (a)
                );
            )", "UPSERT INTO VExplain (k, grp, a, b) VALUES (1, 1, 10, 20), (2, 1, 30, 40);");

            const auto point = fixture.ExplainPlan(
                "SELECT v_covered FROM VExplain WHERE k = 1;");
            UNIT_ASSERT_C(HasPlanOperator(point, "TablePointLookup"), point.GetStringRobust());

            const auto range = fixture.ExplainPlan(
                "SELECT v_covered FROM VExplain WHERE k >= 1 AND k < 3;");
            UNIT_ASSERT_C(HasPlanOperator(range, "TableRangeScan"), range.GetStringRobust());

            const auto covered = fixture.ExplainPlan(
                "SELECT v_covered FROM VExplain WHERE grp = 1;");
            UNIT_ASSERT_C(CountPlanNodesByKv(covered, "Table", "VExplain/idx_grp/indexImplTable") > 0,
                covered.GetStringRobust());
            UNIT_ASSERT_VALUES_EQUAL_C(CountPlanNodesByKv(covered, "Table", "VExplain"), 0u,
                covered.GetStringRobust());

            const auto uncovered = fixture.ExplainPlan(
                "SELECT v_uncovered FROM VExplain WHERE grp = 1;");
            UNIT_ASSERT_C(CountPlanNodesByKv(uncovered, "Table", "VExplain/idx_grp/indexImplTable") > 0,
                uncovered.GetStringRobust());
            UNIT_ASSERT_C(CountPlanNodesByKv(uncovered, "Table", "VExplain") > 0,
                uncovered.GetStringRobust());

            const auto deduplicated = fixture.ExplainPlan(R"(
                SELECT v_covered, v_same, v_uncovered
                FROM VExplain
                WHERE k = 1;
            )");
            const auto readColumns = GetSinglePhysicalReadColumns(deduplicated, "VExplain");
            THashMap<TString, ui32> columnCounts;
            for (const auto& column : readColumns) {
                ++columnCounts[column];
            }
            UNIT_ASSERT_VALUES_EQUAL_C(columnCounts["a"], 1u, deduplicated.GetStringRobust());
            UNIT_ASSERT_VALUES_EQUAL_C(columnCounts["b"], 1u, deduplicated.GetStringRobust());
            UNIT_ASSERT_VALUES_EQUAL_C(columnCounts["v_covered"], 0u, deduplicated.GetStringRobust());
            UNIT_ASSERT_VALUES_EQUAL_C(columnCounts["v_same"], 0u, deduplicated.GetStringRobust());
            UNIT_ASSERT_VALUES_EQUAL_C(columnCounts["v_uncovered"], 0u, deduplicated.GetStringRobust());
        }

        Y_UNIT_TEST(ReadVirtualColumnFromMultiUseNamedExpression) {
            TTestFixture fixture(VirtualReadTableDDL, VirtualReadSeed);

            fixture.Check(R"(
                $rows = SELECT k, v FROM VRead;
                SELECT l.k, l.v, r.v
                FROM $rows AS l
                INNER JOIN $rows AS r ON l.k = r.k
                ORDER BY l.k;
            )", "[[1;[11];[11]];[2;[22];[22]];[3;[3];[3]]]");
        }

        Y_UNIT_TEST(ReturningWithGlobalUniqueIndex) {
            TTestFixture fixture(R"(
                CREATE TABLE VUnique (
                    k Int32 NOT NULL,
                    payload Int32 NOT NULL,
                    unique_value String NOT NULL,
                    v Int32 NOT NULL GENERATED ALWAYS AS (payload + 1) VIRTUAL,
                    PRIMARY KEY (k),
                    INDEX idx_unique GLOBAL UNIQUE SYNC ON (unique_value) COVER (payload)
                );
            )", R"(
                UPSERT INTO VUnique (k, unique_value, payload) VALUES (1, "one", 10);
            )");

            fixture.CheckReturning(
                R"(
                    UPSERT INTO VUnique (k, unique_value, payload)
                    VALUES (1, "one", 20), (2, "two", 30)
                    RETURNING k, v;
                )",
                "SELECT k, v FROM VUnique WHERE k IN (1, 2);",
                "[[1;21];[2;31]]");
            fixture.Check("SELECT k, v FROM VUnique VIEW idx_unique ORDER BY unique_value;",
                "[[1;21];[2;31]]");
        }

        Y_UNIT_TEST(ReturningWithPlainFulltextIndex) {
            CheckVirtualReturningWithFulltextIndex(/* compact */ false);
        }

        Y_UNIT_TEST(ReturningWithCompactFulltextIndexStreamWrite) {
            // Compact fulltext is itself a stream-only index implementation.
            CheckVirtualReturningWithFulltextIndex(/* compact */ true);
        }

        Y_UNIT_TEST(ReadVirtualColumnThroughSecondaryIndex) {
            TTestFixture fixture(VirtualReadTableDDL, VirtualReadSeed);

            fixture.Check("SELECT k, v FROM VRead VIEW idx_grp WHERE grp = 1 ORDER BY k;",
                          "[[1;[11]];[2;[22]]]");
        }

        Y_UNIT_TEST(DmlAndSchemaChangesIgnoreVirtualColumn) {
            TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                note String,
                stored_sum Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) STORED,
                virtual_diff Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) - COALESCE(b, 0)) VIRTUAL,
                PRIMARY KEY (k)
            );
        )");

            fixture.Exec(R"(
            INSERT INTO TestTable (k, a, b, note) VALUES
                (1, 10, 1, "one"),
                (2, 20, 2, "two");
        )");
            fixture.Check("SELECT k, a, b, note, stored_sum FROM TestTable ORDER BY k;",
                          R"([[1;[10];[1];["one"];[11]];[2;[20];[2];["two"];[22]]])");

            fixture.Exec("INSERT INTO TestTable (k, a, note) VALUES (3, 30, \"nullable\");");
            fixture.Check("SELECT k, a, b, note, stored_sum FROM TestTable WHERE k = 3;",
                          R"([[3;[30];#;["nullable"];[30]]])");
            fixture.Exec("DELETE FROM TestTable WHERE k = 3;");

            fixture.Exec("UPSERT INTO TestTable (k, b) VALUES (1, 5);");
            fixture.Exec("REPLACE INTO TestTable (k, a, b, note) VALUES (2, 7, 8, \"replaced\");");
            fixture.Exec("UPDATE TestTable SET a = 30 WHERE k = 1;");
            fixture.Exec("DELETE FROM TestTable WHERE k = 2;");

            fixture.Exec("ALTER TABLE TestTable ADD COLUMN tag Int32;");
            fixture.Exec("UPSERT INTO TestTable (k, tag) VALUES (1, 9);");
            fixture.Exec("ALTER TABLE `/Root/TestTable` RENAME TO `/Root/RenamedTable`;");

            const auto renamedDdl = fixture.ShowCreateTable("/Root/RenamedTable");
            UNIT_ASSERT_STRING_CONTAINS_C(renamedDdl, "virtual_diff", renamedDdl);
            UNIT_ASSERT_STRING_CONTAINS_C(renamedDdl, "VIRTUAL", renamedDdl);

            fixture.Exec("ALTER TABLE `/Root/RenamedTable` DROP COLUMN virtual_diff;");
            fixture.Exec("ALTER TABLE `/Root/RenamedTable` ADD COLUMN virtual_diff Int32;");
            fixture.Exec("UPSERT INTO `/Root/RenamedTable` (k, virtual_diff) VALUES (1, 42);");
            fixture.Exec("ALTER TABLE `/Root/RenamedTable` ADD INDEX idx_recreated GLOBAL SYNC ON (virtual_diff);");
            fixture.Check("SELECT k FROM `/Root/RenamedTable` VIEW idx_recreated WHERE virtual_diff = 42;", "[[1]]");

            fixture.Check("SELECT k, a, b, note, stored_sum, tag FROM `/Root/RenamedTable` ORDER BY k;",
                          R"([[1;[30];[5];["one"];[35];[9]]])");
        }

        Y_UNIT_TEST(BulkUpsertAndReadTableIgnoreVirtualColumn) {
            auto appConfig = GeneratedColumnsAppConfig();
            TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
            auto queryClient = kikimr.GetQueryClient();
            auto tableClient = kikimr.GetTableClient();

            {
                auto result = queryClient.ExecuteQuery(R"(
                    CREATE TABLE `/Root/TestTable` (
                        k Int32 NOT NULL,
                        payload String,
                        virtual_value Int32 NOT NULL GENERATED ALWAYS AS (k + 1) VIRTUAL,
                        PRIMARY KEY (k)
                        );
                )", TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            NYdb::TValueBuilder rowsBuilder;
            rowsBuilder.BeginList();
            rowsBuilder.AddListItem()
                .BeginStruct()
                .AddMember("k")
                .Int32(1)
                .AddMember("payload")
                .String("one")
                .EndStruct();
            rowsBuilder.AddListItem()
                .BeginStruct()
                .AddMember("k")
                .Int32(2)
                .AddMember("payload")
                .String("two")
                .EndStruct();
            rowsBuilder.EndList();

            {
                auto result = tableClient.BulkUpsert("/Root/TestTable", rowsBuilder.Build()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            NYdb::TValueBuilder invalidRowsBuilder;
            invalidRowsBuilder.BeginList();
            invalidRowsBuilder.AddListItem()
                .BeginStruct()
                .AddMember("k")
                .Int32(3)
                .AddMember("payload")
                .String("three")
                .AddMember("virtual_value")
                .Int32(4)
                .EndStruct();
            invalidRowsBuilder.EndList();

            {
                auto result = tableClient.BulkUpsert("/Root/TestTable", invalidRowsBuilder.Build()).ExtractValueSync();
                UNIT_ASSERT_C(!result.IsSuccess(), "bulk upsert must not accept an explicit VIRTUAL generated column");
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "cannot be set explicitly");
            }

            {
                Ydb::Formats::CsvSettings csvSettings;
                csvSettings.set_header(true);
                csvSettings.set_delimiter(",");
                TString serializedCsvSettings;
                UNIT_ASSERT(csvSettings.SerializeToString(&serializedCsvSettings));

                auto result = tableClient.BulkUpsert("/Root/TestTable", NYdb::NTable::EDataFormat::CSV, "k,payload\n3,three\n",
                    {}, NYdb::NTable::TBulkUpsertSettings().FormatSettings(serializedCsvSettings)).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            {
                auto result = queryClient.ExecuteQuery("SELECT k, payload FROM `/Root/TestTable` ORDER BY k;", TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                CompareYson(R"([[1;["one"]];[2;["two"]];[3;["three"]]])", FormatResultSetYson(result.GetResultSet(0)));
            }

            auto session = tableClient.CreateSession().ExtractValueSync().GetSession();
            auto settings = NYdb::NTable::TReadTableSettings()
                .Ordered()
                .AppendColumns("k")
                .AppendColumns("payload");
            auto iterator = session.ReadTable("/Root/TestTable", settings).ExtractValueSync();
            UNIT_ASSERT_C(iterator.IsSuccess(), iterator.GetIssues().ToString());

            TVector<std::pair<i32, TString>> rows;
            for (;;) {
                auto part = iterator.ReadNext().ExtractValueSync();
                if (!part.IsSuccess()) {
                    UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                    break;
                }

                NYdb::TResultSetParser parser(part.ExtractPart());
                while (parser.TryNextRow()) {
                    const auto key = parser.ColumnParser("k").GetOptionalInt32();
                    const auto payload = parser.ColumnParser("payload").GetOptionalString();
                    UNIT_ASSERT(key);
                    UNIT_ASSERT(payload);
                    rows.emplace_back(*key, *payload);
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].first, 1);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].second, "one");
            UNIT_ASSERT_VALUES_EQUAL(rows[1].first, 2);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].second, "two");
            UNIT_ASSERT_VALUES_EQUAL(rows[2].first, 3);
            UNIT_ASSERT_VALUES_EQUAL(rows[2].second, "three");

            auto virtualIterator = session.ReadTable("/Root/TestTable", NYdb::NTable::TReadTableSettings().AppendColumns("virtual_value"))
                .ExtractValueSync();
            auto virtualPart = virtualIterator.ReadNext().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(virtualPart.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }

        Y_UNIT_TEST(VirtualColumnCannotBeIndexKeyOrCover) {
            TTestFixture fixture(R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )");

            fixture.Rejects("ALTER TABLE TestTable ADD INDEX idx_v GLOBAL SYNC ON (v);", "VIRTUAL generated column");
            fixture.Rejects("ALTER TABLE TestTable ADD INDEX idx_v_async GLOBAL ASYNC ON (v);", "VIRTUAL generated column");
            fixture.Rejects("ALTER TABLE TestTable ADD INDEX idx_v_unique GLOBAL UNIQUE ON (v);", "VIRTUAL generated column");
            fixture.Rejects("ALTER TABLE TestTable ADD INDEX idx_cover GLOBAL SYNC ON (a) COVER (v);", "VIRTUAL generated column");
            fixture.Rejects(R"(
                CREATE TABLE VirtualIndexKey (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k),
                    INDEX idx GLOBAL SYNC ON (v)
                );
            )", "VIRTUAL generated column");
            fixture.Rejects(R"(
                CREATE TABLE VirtualIndexCover (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k),
                    INDEX idx GLOBAL SYNC ON (a) COVER (v)
                );
            )", "VIRTUAL generated column");
        }

        Y_UNIT_TEST(SyncAndUniqueIndexBuildsIgnoreVirtualColumn) {
            TTestFixture fixture(R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    a Int32,
                    b Int32,
                    payload String,
                    virtual_value Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + COALESCE(b, 0)) VIRTUAL,
                    PRIMARY KEY (k),
                    INDEX idx_inline GLOBAL SYNC ON (a) COVER (payload)
                );
            )");

            fixture.Exec(R"(
                UPSERT INTO TestTable (k, a, b, payload) VALUES
                    (1, 10, 100, "one"),
                    (2, 20, 200, "two");
            )");
            fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_b GLOBAL SYNC ON (b) COVER (payload);");
            fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_payload GLOBAL UNIQUE ON (payload);");

            fixture.Check("SELECT k, a, payload FROM TestTable VIEW idx_inline WHERE a = 10;",
                          R"([[1;[10];["one"]]])");
            fixture.Check("SELECT k, b, payload FROM TestTable VIEW idx_b WHERE b = 200;",
                          R"([[2;[200];["two"]]])");
            fixture.Check("SELECT k, payload FROM TestTable VIEW idx_payload WHERE payload = \"two\";",
                          R"([[2;["two"]]])");

            fixture.Exec("REPLACE INTO TestTable (k, a, b, payload) VALUES (1, 30, 300, \"uno\");");
            fixture.Check("SELECT k, payload FROM TestTable VIEW idx_inline WHERE a = 10;", "[]");
            fixture.Check("SELECT k, a, payload FROM TestTable VIEW idx_inline WHERE a = 30;",
                          R"([[1;[30];["uno"]]])");
            fixture.Check("SELECT k, b, payload FROM TestTable VIEW idx_b WHERE b = 300;",
                          R"([[1;[300];["uno"]]])");
            fixture.Check("SELECT k, payload FROM TestTable VIEW idx_payload WHERE payload = \"uno\";",
                          R"([[1;["uno"]]])");

            fixture.Exec("ALTER TABLE TestTable DROP COLUMN virtual_value;");
            fixture.Exec("UPSERT INTO TestTable (k, a, b, payload) VALUES (3, 40, 400, \"three\");");
            fixture.Check("SELECT k, b, payload FROM TestTable VIEW idx_b WHERE b = 400;",
                          R"([[3;[400];["three"]]])");
        }

        Y_UNIT_TEST(AsyncIndexWritesIgnoreVirtualColumn) {
            TTestFixture fixture(R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    index_key String,
                    payload String,
                    virtual_value Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )");

            fixture.Exec("UPSERT INTO TestTable (k, index_key, payload) VALUES (1, \"a\", \"one\");");
            fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_async GLOBAL ASYNC ON (index_key) COVER (payload);");

            fixture.Exec("UPSERT INTO TestTable (k, index_key, payload) VALUES (2, \"b\", \"two\");");
            fixture.Exec("UPDATE TestTable SET payload = \"updated\" WHERE k = 1;");
            fixture.Exec("REPLACE INTO TestTable (k, index_key, payload) VALUES (2, \"c\", \"replaced\");");
            fixture.Exec("DELETE FROM TestTable WHERE k = 1;");
            fixture.Exec("ALTER TABLE TestTable DROP COLUMN virtual_value;");
            fixture.Exec("UPSERT INTO TestTable (k, index_key, payload) VALUES (3, \"d\", \"after-drop\");");

            fixture.Check("SELECT k, index_key, payload FROM TestTable ORDER BY k;",
                          R"([[2;["c"];["replaced"]];[3;["d"];["after-drop"]]])");
            fixture.CheckStaleEventually("SELECT k, index_key, payload FROM TestTable VIEW idx_async ORDER BY index_key;",
                                         R"([[2;["c"];["replaced"]];[3;["d"];["after-drop"]]])");

            const auto ddl = fixture.ShowCreateTable("/Root/TestTable");
            UNIT_ASSERT_STRING_CONTAINS_C(ddl, "INDEX `idx_async` GLOBAL ASYNC", ddl);
            UNIT_ASSERT_C(ddl.find("virtual_value") == std::string::npos, ddl);
        }

        Y_UNIT_TEST(MetadataPersistsAcrossSchemeShardRestart) {
            TTestFixture fixture(R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    a Int32,
                    stored_value Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) STORED,
                    virtual_value Int32 GENERATED ALWAYS AS (COALESCE(a, 0) - 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )");

            fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 10);");

            const auto before = fixture.ShowCreateTable("/Root/TestTable");
            UNIT_ASSERT_STRING_CONTAINS_C(before, "GENERATED ALWAYS AS (COALESCE(a, 0) + 1) STORED", before);
            UNIT_ASSERT_STRING_CONTAINS_C(before, "GENERATED ALWAYS AS (COALESCE(a, 0) - 1) VIRTUAL", before);

            fixture.RestartSchemeShard("/Root/TestTable");

            const auto after = fixture.ShowCreateTable("/Root/TestTable");
            UNIT_ASSERT_STRING_CONTAINS_C(after, "GENERATED ALWAYS AS (COALESCE(a, 0) + 1) STORED", after);
            UNIT_ASSERT_STRING_CONTAINS_C(after, "GENERATED ALWAYS AS (COALESCE(a, 0) - 1) VIRTUAL", after);
            fixture.Check("SELECT k, a, stored_value FROM TestTable;", "[[1;[10];[11]]]");

            fixture.Exec("ALTER TABLE TestTable DROP COLUMN virtual_value;");
            fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (2, 20);");
            fixture.Check("SELECT k, a, stored_value FROM TestTable ORDER BY k;",
                          "[[1;[10];[11]];[2;[20];[21]]]");
        }

        Y_UNIT_TEST(AlterAddGeneratedColumnIsRejected) {
            TTestFixture fixture(R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    a Int32,
                    PRIMARY KEY (k)
                );
            )");

            const TString expectedError =
                "Column addition with a GENERATED ALWAYS AS expression is not supported";

            fixture.Rejects(R"(
                ALTER TABLE TestTable ADD COLUMN virtual_value
                    Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL;
            )", expectedError);
            fixture.Rejects(R"(
                ALTER TABLE TestTable ADD COLUMN stored_value
                    Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) STORED;
            )", expectedError);
        }

        Y_UNIT_TEST(ChangefeedExcludesVirtualColumn) {
            auto appConfig = GeneratedColumnsAppConfig();
            TKikimrSettings settings(appConfig);
            settings.SetWithSampleTables(false).SetPQConfig(GeneratedColumnsPQConfig());
            TKikimrRunner kikimr(settings);
            auto queryClient = kikimr.GetQueryClient();

            auto exec = [&](const std::string& query) {
                auto result = queryClient.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n"
                                                                   << result.GetIssues().ToString());
            };
            auto returning = [&](const std::string& query, const TString& expected) {
                auto result = queryClient.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n"
                                                                   << result.GetIssues().ToString());
                CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
            };

            exec(R"(
                CREATE TABLE `/Root/TestTable` (
                    k Int32 NOT NULL,
                    payload String,
                    value Int32,
                    virtual_value Int32 NOT NULL
                        GENERATED ALWAYS AS (COALESCE(value, 0) * 10) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )");
            exec(R"(
                ALTER TABLE `/Root/TestTable` ADD CHANGEFEED `feed` WITH (
                    MODE = 'NEW_AND_OLD_IMAGES', FORMAT = 'JSON'
                );
            )");
            exec("ALTER TOPIC `/Root/TestTable/feed` ADD CONSUMER `test_consumer`;");

            returning(R"(
                UPSERT INTO `/Root/TestTable` (k, payload, value)
                VALUES (1, "one", 10)
                RETURNING virtual_value, k, value;
            )", "[[100;1;[10]]]");
            returning(R"(
                UPDATE `/Root/TestTable`
                SET payload = "updated", value = 20
                WHERE virtual_value = 100
                RETURNING virtual_value, payload, k;
            )", R"([[200;["updated"];1]])");
            returning(R"(
                DELETE FROM `/Root/TestTable` WHERE virtual_value = 200 RETURNING *;
            )", R"([[1;["updated"];[20];200]])");

            NYdb::NTopic::TTopicClient topicClient(kikimr.GetDriver());
            NYdb::NTopic::TReadSessionSettings readSettings;
            readSettings.ConsumerName("test_consumer");
            readSettings.AppendTopics(NYdb::NTopic::TTopicReadSettings().Path("/Root/TestTable/feed"));
            auto readSession = topicClient.CreateReadSession(readSettings);

            TVector<TString> messages;
            bool sawPartitionStart = false;
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (messages.size() < 3 && TInstant::Now() < deadline) {
                if (!readSession->WaitEvent().Wait(TDuration::Seconds(1))) {
                    continue;
                }

                for (auto& event : readSession->GetEvents(false)) {
                    if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                        for (auto& message : data->GetMessages()) {
                            messages.emplace_back(message.GetData());
                        }
                        data->Commit();
                    } else if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                        start->Confirm();
                        sawPartitionStart = true;
                    } else if (auto* stop = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                        stop->Confirm();
                    } else if (auto* end = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
                        end->Confirm();
                    } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
                        UNIT_FAIL("topic read session closed before all CDC messages arrived");
                    } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
                        UNIT_FAIL("topic partition session closed before all CDC messages arrived");
                    }
                }
            }

            UNIT_ASSERT_C(sawPartitionStart, "topic partition session did not start before the deadline");
            UNIT_ASSERT_VALUES_EQUAL_C(messages.size(), 3u, JoinSeq("\n", messages));
            bool sawInsert = false;
            bool sawUpdate = false;
            bool sawDelete = false;
            for (const auto& message : messages) {
                UNIT_ASSERT_C(!message.Contains("virtual_value"), message);

                NJson::TJsonValue json;
                UNIT_ASSERT_C(NJson::ReadJsonTree(message, &json), message);
                UNIT_ASSERT_C(json.Has("key"), message);
                UNIT_ASSERT_VALUES_EQUAL_C(json["key"][0].GetInteger(), 1, message);

                const bool hasNewImage = json.Has("newImage") && json["newImage"].IsMap();
                const bool hasOldImage = json.Has("oldImage") && json["oldImage"].IsMap();
                if (hasNewImage) {
                    UNIT_ASSERT_C(!json["newImage"].Has("virtual_value"), message);
                    UNIT_ASSERT_C(json["newImage"].Has("value"), message);
                }
                if (hasOldImage) {
                    UNIT_ASSERT_C(!json["oldImage"].Has("virtual_value"), message);
                    UNIT_ASSERT_C(json["oldImage"].Has("value"), message);
                }

                if (json.Has("erase")) {
                    UNIT_ASSERT_C(!hasNewImage && hasOldImage, message);
                    UNIT_ASSERT_VALUES_EQUAL_C(json["oldImage"]["value"].GetInteger(), 20, message);
                    UNIT_ASSERT_C(!sawDelete, message);
                    sawDelete = true;
                } else {
                    UNIT_ASSERT_C(json.Has("update") && hasNewImage, message);
                    if (hasOldImage) {
                        UNIT_ASSERT_VALUES_EQUAL_C(json["oldImage"]["value"].GetInteger(), 10, message);
                        UNIT_ASSERT_VALUES_EQUAL_C(json["newImage"]["value"].GetInteger(), 20, message);
                        UNIT_ASSERT_C(!sawUpdate, message);
                        sawUpdate = true;
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL_C(json["newImage"]["value"].GetInteger(), 10, message);
                        UNIT_ASSERT_C(!sawInsert, message);
                        sawInsert = true;
                    }
                }
            }
            UNIT_ASSERT(sawInsert);
            UNIT_ASSERT(sawUpdate);
            UNIT_ASSERT(sawDelete);

            exec("ALTER TABLE `/Root/TestTable` DROP CHANGEFEED `feed`;");
            returning(R"(
                UPSERT INTO `/Root/TestTable` (k, payload, value)
                VALUES (2, "after-drop", 30)
                RETURNING k, virtual_value;
            )", "[[2;300]]");
            exec("ALTER TABLE `/Root/TestTable` DROP COLUMN virtual_value;");
        }

        Y_UNIT_TEST(NotNullPgVirtualDoesNotBecomeWriteConstraint) {
            TTestFixture fixture(R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    payload PgText NOT NULL,
                    virtual_value PgText NOT NULL GENERATED ALWAYS AS (payload) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )");

            fixture.Exec("INSERT INTO TestTable (k, payload) VALUES (1, 'one'pt);");
            fixture.Exec("UPSERT INTO TestTable (k, payload) VALUES (1, 'updated'pt);");
            fixture.Exec("REPLACE INTO TestTable (k, payload) VALUES (2, 'two'pt);");
            fixture.Exec("UPDATE TestTable SET payload = 'again'pt WHERE k = 1;");
            fixture.Check("SELECT k FROM TestTable ORDER BY k;", "[[1];[2]]");
        }

        Y_UNIT_TEST(UnsupportedVirtualColumnUsagesAreRejected) {
            auto appConfig = GeneratedColumnsAppConfig();
            TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
            auto queryClient = kikimr.GetQueryClient();
            auto session = queryClient.GetSession().GetValueSync().GetSession();

            auto exec = [&](const std::string& query) {
                auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n"
                                                                   << result.GetIssues().ToString());
            };
            auto rejects = [&](const std::string& query, const TString& expectedError = {}) {
                auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(!result.IsSuccess(), "expected query to be rejected: " << query);
                if (expectedError) {
                    UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), expectedError, query);
                }
            };

            exec(R"(
                CREATE TABLE Writable (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k)
                );
            )");
            exec("UPSERT INTO Writable (k, a) VALUES (1, 10);");
            rejects("INSERT INTO Writable (k, a, v) VALUES (2, 20, 21);", "cannot be set explicitly");
            rejects("REPLACE INTO Writable (k, a, v) VALUES (2, 20, 21);", "cannot be set explicitly");
            rejects("UPSERT INTO Writable (k, a, v) VALUES (1, 10, 11);", "cannot be set explicitly");
            rejects("UPDATE Writable SET v = 11 WHERE k = 1;", "cannot be set explicitly");
            rejects("UPDATE Writable ON (k, v) VALUES (1, 11);", "cannot be set explicitly");
            rejects(R"(
                CREATE TABLE StoredOverVirtual (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    s Int32 GENERATED ALWAYS AS (COALESCE(v, 0) + 1) STORED,
                    PRIMARY KEY (k)
                );
            )", "references another generated column");
            rejects(R"(
                ALTER TABLE Writable ADD COLUMN s
                    Int32 GENERATED ALWAYS AS (COALESCE(v, 0) + 1) STORED;
            )", "Column addition with a GENERATED ALWAYS AS expression is not supported");
            rejects(R"(
                CREATE TABLE VirtualPrimaryKey (
                    k Int32,
                    v Int32 GENERATED ALWAYS AS (COALESCE(k, 0) + 1) VIRTUAL,
                    PRIMARY KEY (v)
                );
            )", "Generated columns cannot be part of the primary key");
            rejects(R"(
                CREATE TABLE VirtualTtl (
                    k Int32 NOT NULL,
                    ts Timestamp,
                    expires Timestamp GENERATED ALWAYS AS (ts) VIRTUAL,
                    PRIMARY KEY (k)
                ) WITH (TTL = Interval("PT1H") ON expires);
            )", "TTL column expires can not be a GENERATED column");
            rejects(R"(
                CREATE TABLE VirtualOlap (
                    k Int32 NOT NULL,
                    a Int32,
                    v Int32 GENERATED ALWAYS AS (COALESCE(a, 0) + 1) VIRTUAL,
                    PRIMARY KEY (k)
                ) WITH (STORE = COLUMN);
            )", "Generated columns are not supported in column tables");
        }
    } // Y_UNIT_TEST_SUITE(GeneratedVirtual)

}   // namespace NKikimr::NKqp
