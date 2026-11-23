#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/random_provider/random_provider.h>

#include <atomic>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

struct TFixtureOptions {
    bool FeatureFlagEnabled = true;
    bool IndexStreamWrite = true;
    bool GeneratedStored = false;
    bool CompileTimeDefaults = true;
};

NKikimrConfig::TAppConfig DefaultExprAppConfig(const TFixtureOptions& options = {}) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableDefaultFromExpression(options.FeatureFlagEnabled);
    appConfig.MutableFeatureFlags()->SetEnableGeneratedStored(options.GeneratedStored);
    appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(options.CompileTimeDefaults);
    appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(options.IndexStreamWrite);
    return appConfig;
}

// A volatile expression stays on the write path; multiplying its result by zero
// keeps the value predictable so the tests can assert exact rows
constexpr const char* DefaultExpr = "RandomNumber(1) * 0ul + 42ul";

// Distinct ranges make a draw from an earlier execution detectable without probabilities.
class TScopedEpochRandomProvider {
    class TProvider final : public IRandomProvider {
    public:
        explicit TProvider(TIntrusivePtr<IRandomProvider> original)
            : Original(std::move(original))
        {}

        TGUID GenGuid() noexcept override { return Original->GenGuid(); }
        TGUID GenUuid4() noexcept override { return Original->GenUuid4(); }
        ui64 GenRand() noexcept override {
            return (Epoch.load() << 60) | (Counter.fetch_add(1) & ((ui64(1) << 60) - 1));
        }

        std::atomic<ui64> Epoch{0};

    private:
        TIntrusivePtr<IRandomProvider> Original;
        std::atomic<ui64> Counter{0};
    };

public:
    TScopedEpochRandomProvider()
        : Original(TAppData::RandomProvider)
        , Provider(MakeIntrusive<TProvider>(Original))
    {
        TAppData::RandomProvider = Provider;
    }

    ~TScopedEpochRandomProvider() {
        TAppData::RandomProvider = Original;
    }

    void SetEpoch(ui64 epoch) { Provider->Epoch.store(epoch); }

private:
    TIntrusivePtr<IRandomProvider> Original;
    TIntrusivePtr<TProvider> Provider;
};

class TTestFixture {
public:
    explicit TTestFixture(const TFixtureOptions& options = {})
        : TTestFixture(TKikimrSettings(DefaultExprAppConfig(options)).SetWithSampleTables(false))
    {
    }

    explicit TTestFixture(const TKikimrSettings& settings)
        : Kikimr(settings)
        , Db(Kikimr.GetQueryClient())
        , Session(Db.GetSession().GetValueSync().GetSession())
    {
    }

    void Exec(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
    }

    TString Rejects(const std::string& query, const TString& expectedError) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "expected the query to be rejected: " << query);
        const TString issues = result.GetIssues().ToString();
        UNIT_ASSERT_STRING_CONTAINS_C(issues, expectedError, "query: " << query);
        return issues;
    }

    void RejectsWithStatus(const std::string& query, EStatus expectedStatus) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, query << "\n" << result.GetIssues().ToString());
    }

    void Check(const std::string& query, const TString& expected) {
        CompareYson(expected, QueryYson(query));
    }

    void CheckUnordered(const std::string& query, const TString& expected) {
        CompareYsonUnordered(expected, QueryYson(query), TStringBuilder() << "unexpected rows for: " << query);
    }

    TString QueryYson(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());
        return FormatResultSetYson(result.GetResultSet(0));
    }

    void CheckEventually(const std::string& query, const TString& expected) {
        TString actual;
        TString issues;
        const TString normalizedExpected = ReformatYson(expected);
        for (ui32 attempt = 0; attempt < 100; ++attempt) {
            auto result = Session.ExecuteQuery(
                query, TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).GetValueSync();
            issues = result.GetIssues().ToString();
            if (result.IsSuccess()) {
                actual = FormatResultSetYson(result.GetResultSet(0));
                if (ReformatYson(actual) == normalizedExpected) {
                    return;
                }
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_FAIL("eventual query result did not converge: " << query
            << "\nexpected: " << expected << "\nactual: " << actual << "\nissues: " << issues);
    }

    ui64 QueryUint64(const std::string& query) {
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "query failed: " << query << "\n" << result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT_C(parser.TryNextRow(), "query returned no rows: " << query);
        return parser.ColumnParser(0).GetUint64();
    }

    TString ExplainAst(const std::string& query) {
        auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx(), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), "explain failed: " << query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetStats().has_value(), "no stats for: " << query);
        const auto ast = result.GetStats()->GetAst();
        UNIT_ASSERT_C(ast.has_value(), "no AST for: " << query);
        return TString(*ast);
    }

    void CheckStreamLookup(const std::string& query, bool expected) {
        const TString ast = ExplainAst(query);
        const bool hasStreamLookup = ast.Contains("KqpCnStreamLookup");
        UNIT_ASSERT_C(hasStreamLookup == expected,
            "stream lookup expectation mismatch for: " << query
                << "\nexpected: " << (expected ? "yes" : "no")
                << ", found: " << (hasStreamLookup ? "yes" : "no")
                << "\nAST:\n" << ast);
    }

    // The column description as SchemeShard persisted it
    NKikimrSchemeOp::TColumnDescription ColumnDesc(const std::string& path, const std::string& column) {
        auto describe = Kikimr.GetTestClient().Ls(TString(path));
        const auto& table = describe->Record.GetPathDescription().GetTable();
        for (const auto& col : table.GetColumns()) {
            if (col.GetName() == column) {
                return col;
            }
        }
        UNIT_FAIL("column '" << column << "' not found in " << path);
        return {};
    }

    std::string ShowCreateTable(const std::string& path) {
        const std::string query = "SHOW CREATE TABLE `" + path + "`;";
        auto result = Session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        auto ddl = parser.ColumnParser("CreateQuery").GetOptionalUtf8();
        UNIT_ASSERT_C(ddl.has_value(), "SHOW CREATE TABLE returned an empty CreateQuery");
        return *ddl;
    }

    void RestartSchemeShard(const std::string& tablePath) {
        RestartSchemeShardAndSyncCache(*Kikimr.GetTestServer().GetRuntime(), TString(tablePath));
    }

    void DisableExpressionDefaults(const std::string& tablePath) {
        auto& runtime = *Kikimr.GetTestServer().GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetEnableDefaultFromExpression(false);
        }
        RestartSchemeShard(tablePath);
        Session = Db.GetSession().GetValueSync().GetSession();
    }

    TKikimrRunner& Runner() {
        return Kikimr;
    }

    NYdb::NQuery::TSession& QuerySession() {
        return Session;
    }

private:
    TKikimrRunner Kikimr;
    NYdb::NQuery::TQueryClient Db;
    NYdb::NQuery::TSession Session;
};

std::string DefaultExprDDL(const std::string& columnDef, const std::string& prefix = "") {
    return prefix + R"(
        CREATE TABLE TestTable (
            k Int32 NOT NULL,
            other Int32,
            )" + columnDef + R"(,
            PRIMARY KEY (k)
        );
    )";
}

// The table the value tests use: `v` is filled by DefaultExpr when it is absent from the write
std::string ValueTableDDL() {
    return DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr);
}

void AssertFromExpression(const NKikimrSchemeOp::TColumnDescription& col, const TString& exprText) {
    UNIT_ASSERT_C(col.HasDefaultFromExpression(), "expected an expression default: " << col.ShortDebugString());
    const auto& defaultExpression = col.GetDefaultFromExpression();
    UNIT_ASSERT(defaultExpression.GetStored());
    UNIT_ASSERT(defaultExpression.GetWritable());
    UNIT_ASSERT_VALUES_EQUAL(defaultExpression.GetExprText(), exprText);
    UNIT_ASSERT_VALUES_EQUAL_C(defaultExpression.DependencyColumnNamesSize(), 0u,
        "a DEFAULT expression must not have dependencies: " << defaultExpression.ShortDebugString());
}

enum class ESecondaryIndexKind {
    None,
    Sync,
    Async,
};

std::string DmlMatrixDDL(ESecondaryIndexKind indexKind) {
    TStringBuilder ddl;
    ddl << R"(
        CREATE TABLE TestTable (
            k Int32 NOT NULL,
            other Int32,
            v Uint64 DEFAULT )" << DefaultExpr << ",\n"
        << "            PRIMARY KEY (k)";
    switch (indexKind) {
        case ESecondaryIndexKind::None:
            break;
        case ESecondaryIndexKind::Sync:
            ddl << ", INDEX idx_v GLOBAL SYNC ON (v) COVER (other)";
            break;
        case ESecondaryIndexKind::Async:
            ddl << ", INDEX idx_v GLOBAL ASYNC ON (v) COVER (other)";
            break;
    }
    ddl << R"(
        );
    )";
    return ddl;
}

void ExecuteDml(TTestFixture& fixture, const TString& query, const TString& expectedReturning, bool withReturning) {
    if (withReturning) {
        CompareYson(expectedReturning, fixture.QueryYson(query + " RETURNING k, other, v;"));
    } else {
        fixture.Exec(query + ";");
    }
}

void RunDmlMatrix(ESecondaryIndexKind indexKind, bool withReturning, bool enableIndexStreamWrite = true) {
    TTestFixture fixture(TFixtureOptions{.IndexStreamWrite = enableIndexStreamWrite});
    fixture.Exec(DmlMatrixDDL(indexKind));

    ExecuteDml(fixture, "INSERT INTO TestTable (k, other) VALUES (1, 10)", "[[1;[10];[42u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 1;", "[[1;[10];[42u]]]");

    ExecuteDml(fixture, "UPSERT INTO TestTable (k, other) VALUES (2, 20)", "[[2;[20];[42u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 2;", "[[2;[20];[42u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (3, 30, 7ul);");
    ExecuteDml(fixture, "UPSERT INTO TestTable (k, other) VALUES (3, 31)", "[[3;[31];[7u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 3;", "[[3;[31];[7u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (4, 40, 8ul);");
    ExecuteDml(fixture, "REPLACE INTO TestTable (k, other) VALUES (4, 41)", "[[4;[41];[42u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 4;", "[[4;[41];[42u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (5, 50, 9ul);");
    ExecuteDml(fixture, "UPDATE TestTable SET other = 51 WHERE k = 5", "[[5;[51];[9u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 5;", "[[5;[51];[9u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (6, 60, 10ul);");
    ExecuteDml(fixture, "UPDATE TestTable ON (k, other) VALUES (6, 61)", "[[6;[61];[10u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 6;", "[[6;[61];[10u]]]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (7, 70, 11ul);");
    ExecuteDml(fixture, "DELETE FROM TestTable WHERE k = 7", "[[7;[70];[11u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 7;", "[]");

    fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (8, 80, 12ul);");
    ExecuteDml(fixture, "DELETE FROM TestTable ON (k) VALUES (8)", "[[8;[80];[12u]]]", withReturning);
    fixture.Check("SELECT k, other, v FROM TestTable WHERE k = 8;", "[]");

    const TString expected = "[[1;[10];[42u]];[2;[20];[42u]];[3;[31];[7u]];[4;[41];[42u]];[5;[51];[9u]];[6;[61];[10u]]]";
    fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", expected);

    if (indexKind != ESecondaryIndexKind::None) {
        const TString expectedIndex = "[[3;[31];[7u]];[5;[51];[9u]];[6;[61];[10u]];[1;[10];[42u]];[2;[20];[42u]];[4;[41];[42u]]]";
        const std::string query = "SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY v, k;";
        if (indexKind == ESecondaryIndexKind::Async) {
            fixture.CheckEventually(query, expectedIndex);
        } else {
            fixture.Check(query, expectedIndex);
        }
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(DefaultExpr) {
    Y_UNIT_TEST(ExpressionIsStoredAndApplied) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST_TWIN(DefaultDialectIsStableAcrossApis, CreateThroughTableApi) {
        TTestFixture fixture;
        const TString expression = "(RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL)";
        const auto ddl = DefaultExprDDL(TStringBuilder() << "v Bool DEFAULT " << expression);
        auto tableClient = fixture.Runner().GetTableClient();
        if (CreateThroughTableApi) {
            auto schemeSession = tableClient.CreateSession().ExtractValueSync().GetSession();
            auto result = schemeSession.ExecuteSchemeQuery(ddl).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } else {
            fixture.Exec(ddl);
        }
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);

        // The unmatched value is NULL under the canonical Query dialect. The legacy
        // Table dialect would produce false, even though both fit the nullable column.
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        auto dataSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto dataResult = dataSession.ExecuteDataQuery(
            "INSERT INTO `/Root/TestTable` (k) VALUES (2);",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(dataResult.IsSuccess(), dataResult.GetIssues().ToString());

        auto preparedSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = preparedSession.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k) VALUES ($k);
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();
        auto params = prepared.GetParamsBuilder().AddParam("$k").Int32(3).Build().Build();
        auto preparedResult = prepared.Execute(
            NYdb::NTable::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(preparedResult.IsSuccess(), preparedResult.GetIssues().ToString());

        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;#];[3;#]]");
    }

    Y_UNIT_TEST(CreateCommitAndWriteNullableDefaultsInScript) {
        TTestFixture fixture;
        NYdb::NScripting::TScriptingClient scriptingClient(fixture.Runner().GetDriver());
        auto result = scriptingClient.ExecuteYqlScript(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                n Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul,
                v Bool DEFAULT (RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL),
                PRIMARY KEY (k)
            );
            COMMIT;
            INSERT INTO TestTable (k) VALUES (1);
            COMMIT;
            SELECT k, n, v FROM TestTable ORDER BY k;
        )").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
        CompareYson("[[1;[42u];#]]", FormatResultSetYson(result.GetResultSet(0)));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "n"), DefaultExpr);
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"),
            "(RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL)");

        // Inline CREATE metadata has index descriptions without loaded implementation tables.
        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_n GLOBAL SYNC ON (n) COVER (v);");
        fixture.Check("SELECT k, n, v FROM TestTable VIEW idx_n ORDER BY k;", "[[1;[42u];#]]");

        // A separate compilation reloads the persisted metadata, rather than the CREATE cache.
        fixture.Check("INSERT INTO TestTable (k) VALUES (2) RETURNING k, n, v;", "[[2;[42u];#]]");
        fixture.Check("SELECT k, n, v FROM TestTable ORDER BY k;", "[[1;[42u];#];[2;[42u];#]]");
        fixture.Check("SELECT k, n, v FROM TestTable VIEW idx_n ORDER BY k;", "[[1;[42u];#];[2;[42u];#]]");
    }

    Y_UNIT_TEST(NonDeterministicFunction) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("ts Timestamp DEFAULT CurrentUtcTimestamp()"));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "ts"), "CurrentUtcTimestamp()");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, ts IS NOT NULL FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(RandomFunction) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("r Uint64 DEFAULT RandomNumber(1)"));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "r"), "RandomNumber(1)");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, r IS NOT NULL FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(PreparedWritesUseExecutionTimeDefaults) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                ts Timestamp NOT NULL DEFAULT CurrentUtcTimestamp(),
                r Uint64 NOT NULL DEFAULT RandomNumber(1),
                observed_ts Timestamp NOT NULL,
                PRIMARY KEY (k)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "ts"), "CurrentUtcTimestamp()");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "r"), "RandomNumber(1)");

        auto tableClient = fixture.Runner().GetTableClient();
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = tableSession.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k, observed_ts)
            VALUES ($k, CurrentUtcTimestamp())
            RETURNING k, ts, r, observed_ts;
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();
        NYdb::NTable::TExecDataQuerySettings settings;
        settings.KeepInQueryCache(true).CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        // Neither the schema nor the prepared handle changes between executions.
        for (i32 key : {1, 2}) {
            auto params = prepared.GetParamsBuilder().AddParam("$k").Int32(key).Build().Build();
            const auto before = TInstant::Now();
            auto result = prepared.Execute(
                NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
            const auto after = TInstant::Now();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats());
            if (key == 2) {
                UNIT_ASSERT(NYdb::TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache());
            }
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("k").GetInt32(), key);
            const auto timestamp = parser.ColumnParser("ts").GetTimestamp();
            UNIT_ASSERT_C(before <= timestamp && timestamp <= after,
                "DEFAULT timestamp outside its execution window: " << before << " <= " << timestamp << " <= " << after);
            // Zero-argument timestamps use the same execution binding in both expressions.
            UNIT_ASSERT_VALUES_EQUAL(timestamp, parser.ColumnParser("observed_ts").GetTimestamp());
            // Require a non-null random value; compare its actual value with storage below.
            parser.ColumnParser("r").GetUint64();
            UNIT_ASSERT(!parser.TryNextRow());
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), fixture.QueryYson(
                TStringBuilder() << "SELECT k, ts, r, observed_ts FROM TestTable WHERE k = " << key << ";"));
        }
        fixture.Check("SELECT k FROM TestTable ORDER BY k;", "[[1];[2]]");
    }

    Y_UNIT_TEST(NotNullWithNonNullableExpr) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("ts Timestamp NOT NULL DEFAULT CurrentUtcTimestamp()"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, ts IS NOT NULL FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(NotNullDefaultOmitted) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 NOT NULL DEFAULT " << DefaultExpr));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;42u]]");
    }

    // A generated column is computed from the row and so cannot be a key; a DEFAULT column has no
    // such dependency
    Y_UNIT_TEST(OnPrimaryKey) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                id Uint64 NOT NULL DEFAULT )" << DefaultExpr << R"(,
                v Int32,
                PRIMARY KEY (id)
            );
        )");

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "id"), DefaultExpr);

        fixture.Exec("INSERT INTO TestTable (v) VALUES (1);");
        fixture.Check("SELECT id, v FROM TestTable;", "[[42u;[1]]]");
    }

    Y_UNIT_TEST(TtlColumn) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                ts Timestamp DEFAULT CurrentUtcTimestamp(),
                PRIMARY KEY (k)
            ) WITH (TTL = Interval("PT1H") ON ts);
        )");
    }

    Y_UNIT_TEST(MultipleDefaultsIndependent) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Uint64 DEFAULT RandomNumber(1) * 0ul + 1ul,
                b Uint64 DEFAULT RandomNumber(2) * 0ul + 2ul,
                PRIMARY KEY (k)
            );
        )");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, a) VALUES (2, 100ul);");
        fixture.Check("SELECT k, a, b FROM TestTable ORDER BY k;", "[[1;[1u];[2u]];[2;[100u];[2u]]]");
    }

    Y_UNIT_TEST(CoercibleType) {
        TTestFixture fixture;
        // The expression is Uint64, the column is Int64
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << DefaultExpr));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42]]]");
    }

    Y_UNIT_TEST(NamedNodeRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT RandomNumber(1) * $factor + 42ul", "$factor = 0ul;\n"),
            "Unknown name: $factor");
    }

    Y_UNIT_TEST(UnrelatedPragmaAccepted) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr,
            "PRAGMA AnsiInForEmptyOrNullableItemsCollections;\n"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(UnrelatedDeclareAccepted) {
        TTestFixture fixture;
        // Unrelated query declarations are not part of the stored expression
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr,
            "DECLARE $unused AS Int32;\n"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST_TWIN(LiteralDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Uint32 DEFAULT 5"));

        const auto col = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(col.HasDefaultFromLiteral(), col.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(col.GetDefaultFromLiteral().type().type_id()),
            static_cast<int>(Ydb::Type::UINT32));
        UNIT_ASSERT_VALUES_EQUAL(col.GetDefaultFromLiteral().value().uint32_value(), 5u);

        auto tableClient = fixture.Runner().GetTableClient();
        auto session = tableClient.GetSession().GetValueSync().GetSession();
        auto describe = session.DescribeTable("/Root/TestTable").GetValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
        const auto& proto = NYdb::TProtoAccessor::GetProto(describe.GetTableDescription());
        bool found = false;
        for (const auto& column : proto.columns()) {
            if (column.name() == "v") {
                found = true;
                UNIT_ASSERT_C(column.has_from_literal(), column.ShortDebugString());
                UNIT_ASSERT_VALUES_EQUAL(column.from_literal().value().uint32_value(), 5u);
            }
        }
        UNIT_ASSERT_C(found, "column v not found in DescribeTable");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[5u]]]");
    }

    Y_UNIT_TEST_TWIN(ConstantDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int32 DEFAULT (1 + 2) * 3"));
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int32_value(), 9);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[9]]]");
    }

    Y_UNIT_TEST_TWIN(CoercedConstantDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int64 DEFAULT CAST(7 AS Int64) + 2l"));
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int64_value(), 9);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[9]]]");
    }

    Y_UNIT_TEST_TWIN(CoercedDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        for (const auto& [expression, expected] : {
            std::pair{"Abs(-5)", 5},
            std::pair{"ListSum(ListMap(AsList(2, 3), ($x) -> { RETURN $x; }))", 5},
            std::pair{R"(String::LevensteinDistance("a", "b"))", 1}})
        {
            fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << expression));
            const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int64_value(), expected);
            fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
            fixture.Check("SELECT k, v FROM TestTable;", TStringBuilder() << "[[1;[" << expected << "]]]");
            fixture.Exec("DROP TABLE TestTable;");
        }
    }

    Y_UNIT_TEST(RuntimeSettingCoercionStaysExpression) {
        TTestFixture fixture;
        for (const auto* expression : {
            "Length(Yql::HostRuntimeSetting('test'))",
            "Length(Yql::UdfRuntimeSetting('String', 'test'))",
            // The ordinary KQP runner does not install the common DQ transform
            // needed to lower CurrentActorId; this case checks its metadata only.
            "Length(Yql::CurrentActorId())"})
        {
            fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << expression));
            AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
            if (TString(expression).Contains("RuntimeSetting")) {
                // An unknown host setting and an absent UDF setting both return NULL.
                fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
                fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
            }
            fixture.Exec("DROP TABLE TestTable;");
        }
    }

    Y_UNIT_TEST(CurrentActorIdDefaultExecutesWithCommonDqTransform) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableExternalDataSources(false);
        appConfig.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(false);
        appConfig.MutableFeatureFlags()->SetEnableStreamingQueries(false);

        // Proxy bootstrap needs a driver, PQ gateway and credentials even though
        // this test uses only a local table. Keep their owners alive past the runner.
        NYdb::TDriverConfig driverConfig;
        driverConfig.SetDiscoveryMode(NYdb::EDiscoveryMode::Async);
        auto driver = MakeSharedYdbDriverWithStop(std::make_unique<NYdb::TDriver>(driverConfig));
        auto credentialsFactory = NYql::CreateStructuredTokenCredentialsFactory();
        const auto& queryConfig = appConfig.GetQueryServiceConfig();
        auto setupFactory = std::make_shared<TKqpFederatedQuerySetupFactoryMock>(
            nullptr, nullptr, credentialsFactory, nullptr,
            queryConfig.GetS3(), queryConfig.GetGeneric(), queryConfig.GetYt(), nullptr,
            queryConfig.GetSolomon(), nullptr, NYql::NDq::TS3ReadActorFactoryConfig{},
            NYql::CreateCommonDqTaskTransformFactory(), NYql::TPqGatewayConfig{},
            MakePqGatewayFactory(driver, credentialsFactory), nullptr, driver);
        TTestFixture fixture(TKikimrSettings(appConfig)
            .SetWithSampleTables(false)
            .SetFederatedQuerySetupFactory(setupFactory));
        fixture.Exec(DefaultExprDDL("v String NOT NULL DEFAULT Yql::CurrentActorId()"));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "Yql::CurrentActorId()");

        auto db = fixture.Runner().GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            "INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("k").GetInt32(), 1);
        const TString actorId(parser.ColumnParser("v").GetString());
        // Parse validates [NodeId:LocalId:Hint] only; the printable form cannot
        // reconstruct actor identity and is not compared with another callable.
        NActors::TActorId parsedShape;
        UNIT_ASSERT_C(parsedShape.Parse(actorId.data(), actorId.size()), actorId);
        UNIT_ASSERT(!parser.TryNextRow());
        CompareYson(FormatResultSetYson(result.GetResultSet(0)),
            fixture.QueryYson("SELECT k, v FROM TestTable;"));
    }

    Y_UNIT_TEST_TWIN(AddCoercedDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int64"));
        fixture.Exec("ALTER TABLE TestTable ADD COLUMN added Int64 DEFAULT Abs(-5);");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "added");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int64_value(), 5);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, added FROM TestTable;", "[[1;[5]]]");
    }

    Y_UNIT_TEST_TWIN(SetCoercedDefaultPreservesMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int64"));
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT Abs(-5);");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().int64_value(), 5);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[5]]]");
    }

    Y_UNIT_TEST_TWIN(LegacyDefaultConstructorsPreserveLiteralMetadata, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                negative Int32 DEFAULT -5,
                text Utf8 DEFAULT CAST("hello" AS Utf8),
                amount Decimal(22,9) DEFAULT Decimal("1.11", 22, 9),
                json Json DEFAULT Json('{"k":1}'),
                PRIMARY KEY (k)
            );
        )");
        for (const auto* name : {"negative", "text", "amount", "json"}) {
            const auto column = fixture.ColumnDesc("/Root/TestTable", name);
            UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.DebugString());
        }
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, negative, text FROM TestTable;", R"([[1;[-5];["hello"]]])");
        auto result = fixture.Runner().GetQueryClient().ExecuteQuery(
            "SELECT amount, json FROM TestTable;", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const auto amount = parser.ColumnParser("amount").GetOptionalDecimal();
        const auto json = parser.ColumnParser("json").GetOptionalJson();
        UNIT_ASSERT(amount.has_value());
        UNIT_ASSERT(json.has_value());
        UNIT_ASSERT_VALUES_EQUAL(amount->ToString(), "1.11");
        UNIT_ASSERT_VALUES_EQUAL(*json, R"({"k":1})");
        UNIT_ASSERT(!parser.TryNextRow());
    }

    Y_UNIT_TEST_TWIN(NullDefault, Enabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = Enabled});
        fixture.Exec(DefaultExprDDL("v Int32 DEFAULT NULL"));
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
    }

    Y_UNIT_TEST(FeatureFlagDisabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = false});
        fixture.Rejects(ValueTableDDL(), "DEFAULT expressions are disabled");
    }

    Y_UNIT_TEST(CoercibleSourceRequiresFeatureFlag) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = false});
        fixture.Rejects(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << DefaultExpr),
            "DEFAULT expressions are disabled");
    }

    Y_UNIT_TEST(PersistedDefaultWorksAfterFeatureDisabled) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr));
        fixture.DisableExpressionDefaults("/Root/TestTable");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[7u]]]");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(2);",
            "DEFAULT expressions are disabled");
    }

    Y_UNIT_TEST_TWIN(AnsiDefaultPreservesValueAfterReload, Alter) {
        TTestFixture fixture;
        const std::string expression = R"sql(CAST(RandomNumber(1) * 0ul AS String) || '\n' || 'it''s@@')sql";
        if (Alter) {
            fixture.Exec(DefaultExprDDL("v String"));
            fixture.Exec("--!ansi_lexer\nALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " + expression
                + "; INSERT INTO TestTable (k) VALUES (1);");
        } else {
            fixture.Exec(DefaultExprDDL("v String DEFAULT " + expression, "--!ansi_lexer\n")
                + "INSERT INTO TestTable (k) VALUES (1);");
        }
        fixture.RestartSchemeShard("/Root/TestTable");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check(R"sql(SELECT k, v = "0\\nit's@@" FROM TestTable ORDER BY k;)sql",
            "[[1;[%true]];[2;[%true]]]");
    }

    Y_UNIT_TEST(PgDefaultCoercionMatchesAfterReload) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetDisablePgCompatibility(false);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(DefaultExprDDL(
            "v pgint8 DEFAULT CAST(CAST(RandomNumber(1) * 0ul + 42ul AS Int32) AS pgint4)")
            + "INSERT INTO TestTable (k) VALUES (1);");
        UNIT_ASSERT(fixture.ColumnDesc("/Root/TestTable", "v").HasDefaultFromExpression());
        fixture.RestartSchemeShard("/Root/TestTable");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, CAST(v AS Int64) FROM TestTable ORDER BY k;", "[[1;[42]];[2;[42]]]");
    }

    Y_UNIT_TEST(PersistedPgDefaultUsesCurrentCompatibilityPolicy) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetDisablePgCompatibility(false);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        const TString expression = "COALESCE(FromPg(CAST(RandomNumber(1) * 0ul + 42ul AS pgint4)), 0)";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Int32 DEFAULT " << expression));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42]]]");

        auto& runtime = *fixture.Runner().GetTestServer().GetRuntime();
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            NKikimrConfig::TFeatureFlags flags = runtime.GetAppData(node).FeatureFlags;
            flags.SetDisablePgCompatibility(true);
            runtime.GetAppData(node).UpdateRuntimeFlags(flags);
        }
        fixture.RestartSchemeShard("/Root/TestTable");
        fixture.Rejects("INSERT INTO TestTable (k) VALUES (2);", "PostgreSQL compatibility is disabled");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42]];[2;[7]]]");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
    }

    Y_UNIT_TEST(FeatureFlagDisabledPreservesLiteralDefaultsAndWrites) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = false});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                other Int32,
                literal Uint64,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal SET DEFAULT 7ul;");
        fixture.Exec("ALTER TABLE TestTable ADD COLUMN added Uint64 DEFAULT 9ul;");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (2);");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 5);");
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, literal, added) VALUES (3, 30ul, 40ul);");
        fixture.Exec("UPDATE TestTable SET other = 6 WHERE k = 2;");
        fixture.Exec("DELETE FROM TestTable WHERE k = 3;");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal DROP DEFAULT;");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (4);");

        auto tableClient = fixture.Runner().GetTableClient();
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct().AddMember("k").Int32(5).EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "expected a missing literal DEFAULT to be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: added");
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(5)
                    .AddMember("added").OptionalUint64(50)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        fixture.Check("SELECT k, other, literal, added FROM TestTable ORDER BY k;",
            "[[1;#;[7u];[9u]];[2;[6];[7u];[9u]];[4;#;#;[9u]];[5;#;#;[50u]]]");
    }

    Y_UNIT_TEST(ColumnReferenceRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Int32 DEFAULT other + 1"), "Column reference");
    }

    Y_UNIT_TEST(SelfReferenceRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Int32 DEFAULT v + 1"), "Column reference");
    }

    Y_UNIT_TEST(AggregateFunctionVariantsRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT SUM(RandomNumber(1))"), "aggregation function");
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT COUNT(*)"), "aggregation function");
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT ListLength(AGGREGATE_LIST(RandomNumber(1)))"),
            "aggregation function");
    }

    Y_UNIT_TEST(WindowFunctionVariantsRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT RANK() OVER (ORDER BY RandomNumber(1))"),
            "Window and aggregation functions are not allowed");
        fixture.Rejects(DefaultExprDDL("v Uint64 DEFAULT NTILE(2) OVER (ORDER BY RandomNumber(1))"),
            "Window and aggregation functions are not allowed");
    }

    Y_UNIT_TEST(SubqueryRejected) {
        TTestFixture fixture;
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        fixture.Rejects(DefaultExprDDL("v Bool DEFAULT EXISTS (SELECT k FROM Source)"), "subquery");
        fixture.Rejects(DefaultExprDDL("v Bool DEFAULT NOT EXISTS (SELECT k FROM Source)"), "subquery");
    }

    Y_UNIT_TEST(NamedExpressionRejected) {
        TTestFixture fixture;
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        fixture.Rejects(DefaultExprDDL("v Bool DEFAULT 1 IN $ids", "$ids = (SELECT k FROM Source);\n"),
            "Unknown name: $ids");
    }

    Y_UNIT_TEST(ParameterRejected) {
        TTestFixture fixture;
        auto db = fixture.Runner().GetQueryClient();
        auto params = NYdb::TParamsBuilder()
            .AddParam("$p").Int32(7).Build()
            .Build();
        auto result = db.ExecuteQuery(
            DefaultExprDDL("v Int32 DEFAULT $p", "DECLARE $p AS Int32;\n"),
            TTxControl::NoTx(), params).GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "DEFAULT must not retain a query parameter");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown name: $p");
    }

    Y_UNIT_TEST(NonRowCallableRejected) {
        TTestFixture fixture;
        // Concatenating a random value keeps the expression out of constant folding, so the
        // non-row callable survives to the expression compiler
        fixture.Rejects(
            DefaultExprDDL("v String DEFAULT CurrentAuthenticatedUser() || CAST(RandomNumber(1) AS String)"),
            "CurrentAuthenticatedUser");
        fixture.Rejects(
            DefaultExprDDL("v String DEFAULT TablePath() || CAST(RandomNumber(1) AS String)"),
            "TablePath");
        fixture.Rejects(
            DefaultExprDDL("v Int32 DEFAULT EvaluateExpr(1 + 2) + CAST(RandomNumber(1) AS Int32)"),
            "EvaluateExpr");
        fixture.Rejects(
            DefaultExprDDL("v String DEFAULT SecureParam(\"token\") || CAST(RandomNumber(1) AS String)"),
            "SecureParam");
    }

    Y_UNIT_TEST(ListHasItemsAcceptsScalarAndRejectsSubquery) {
        TTestFixture fixture;
        const TString expression = "ListHasItems(ListFilter(AsList(RandomNumber(1)), ($x) -> ($x >= 0ul)))";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Bool DEFAULT " << expression));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[%true]]]");
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        fixture.Rejects("CREATE TABLE Invalid (k Int32 NOT NULL, v Bool DEFAULT "
            "ListHasItems((SELECT k FROM Source)), PRIMARY KEY (k));", "subquery");
    }

    Y_UNIT_TEST(SingleRowExpressionWithNestedLambdaAccepted) {
        TTestFixture fixture;
        const TString expression = "ListSum(ListMap(AsList(RandomNumber(1), RandomNumber(2)), "
            "($item) -> { RETURN $item * 0ul + 1ul; }))";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << expression));
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[2u]]]");
    }

    Y_UNIT_TEST(TypeMismatchRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL(TStringBuilder() << "v String DEFAULT " << DefaultExpr), "type mismatch");
    }

    Y_UNIT_TEST(TypeMismatchReportsDefaultColumnBeforeConversionFailure) {
        TTestFixture fixture;
        const TString issues = fixture.Rejects(
            DefaultExprDDL("v4 Uint32 DEFAULT RandomNumber(2)"),
            "Failed to convert type: Uint64 to Optional<Uint32>");

        const TString context =
            "Default expr v4 type mismatch, expected: Uint32, actual: Uint64";
        const size_t contextPos = issues.find(context);
        const size_t causePos = issues.find("Failed to convert type: Uint64 to Optional<Uint32>");
        UNIT_ASSERT_C(contextPos != TString::npos, issues);
        UNIT_ASSERT_C(contextPos < causePos, issues);
    }

    Y_UNIT_TEST(NotNullOptionalExprRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Int32 NOT NULL DEFAULT CAST(other AS Int32)"), "Column reference");
        fixture.Rejects(DefaultExprDDL("v Uint64 NOT NULL DEFAULT CAST(CAST(RandomNumber(1) AS String) AS Uint64)"),
            "declared NOT NULL");
    }

    Y_UNIT_TEST(NullableOptionalExprAccepted) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(
            R"(v Uint64 DEFAULT CAST("invalid" || CAST(RandomNumber(1) AS String) AS Uint64))"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
    }

    Y_UNIT_TEST_TWIN(OptionalDefaultRejectedBeforeSetNotNull, ProducesNull) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableSetColumnConstraint(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        const TString expression = ProducesNull
            ? R"(CAST("invalid" || CAST(RandomNumber(1) AS String) AS Uint64))"
            : R"(CAST(CAST(RandomNumber(1) AS String) AS Uint64))";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << expression));
        const auto before = fixture.ColumnDesc("/Root/TestTable", "v");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");

        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET NOT NULL;", "declared NOT NULL");
        const auto after = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT(!after.GetNotNull());
        UNIT_ASSERT(!after.GetSetNotNullInProgress());
        UNIT_ASSERT_VALUES_EQUAL(before.GetDefaultFromExpression().SerializeAsString(),
            after.GetDefaultFromExpression().SerializeAsString());
        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, v FROM TestTable WHERE k = 1;", "[[1;[7u]]]");
        fixture.Check("SELECT COUNT(*) FROM TestTable;", "[[2u]]");
    }

    Y_UNIT_TEST(DefaultAndGeneratedRejected) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Rejects(DefaultExprDDL(TStringBuilder()
                << "v Uint64 DEFAULT " << DefaultExpr << " GENERATED ALWAYS AS (CAST(k AS Uint64)) STORED"),
            "same time");
    }

    Y_UNIT_TEST(DefaultAndSerialRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Serial DEFAULT 5"), "already set");
    }

    Y_UNIT_TEST(OlapRejected) {
        TTestFixture fixture;
        fixture.Rejects(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k)
            ) WITH (STORE = COLUMN);
        )", "column tables");
    }

    Y_UNIT_TEST(ExternalTableRejectsExpressionDefault) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableExternalDataSources(true);
        appConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE EXTERNAL DATA SOURCE ExternalSource WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="my-bucket", AUTH_METHOD="NONE"
            );
        )");
        fixture.Rejects(R"(
            CREATE EXTERNAL TABLE TestTable (
                v Uint64 DEFAULT RandomNumber(1) * 0ul + 42ul
            ) WITH (DATA_SOURCE="/Root/ExternalSource", LOCATION="/");
        )", "DEFAULT expressions are not supported in external tables");
        fixture.Exec(R"(
            CREATE EXTERNAL TABLE TestTable (v Uint64)
            WITH (DATA_SOURCE="/Root/ExternalSource", LOCATION="/");
        )");
    }

    Y_UNIT_TEST(AddColumnWithDefaultExprRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32"));
        fixture.Rejects(TStringBuilder()
                << "ALTER TABLE TestTable ADD COLUMN w Uint64 DEFAULT " << DefaultExpr << ";",
            "Column addition with a DEFAULT expression is not supported");
    }

    Y_UNIT_TEST(AddColumnWithCoercibleDefaultExprRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32"));
        fixture.Rejects(TStringBuilder()
                << "ALTER TABLE TestTable ADD COLUMN w Int64 DEFAULT " << DefaultExpr << ";",
            "Column addition with a DEFAULT expression is not supported");
    }

    Y_UNIT_TEST(AddColumnWithDefaultLiteralAccepted) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32"));
        fixture.Exec("ALTER TABLE TestTable ADD COLUMN w Int32 DEFAULT 5;");
        {
            const auto col = fixture.ColumnDesc("/Root/TestTable", "w");
            UNIT_ASSERT_C(col.HasDefaultFromLiteral(), col.ShortDebugString());
        }

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, w FROM TestTable;", "[[1;[5]]]");
    }

    Y_UNIT_TEST(OnlyRequiredExpressionDefaultsAreCompiledIntoWriteAst) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                r Uint64 DEFAULT RandomNumber(1),
                ts Timestamp DEFAULT CurrentUtcTimestamp(),
                PRIMARY KEY (k)
            );
        )");

        for (const TStringBuf operation : {"INSERT", "UPSERT", "REPLACE"}) {
            {
                const TString query = TStringBuilder() << operation
                    << R"( INTO TestTable (k, ts) VALUES (1, Timestamp("2020-01-01T00:00:00Z"));)";
                const TString ast = fixture.ExplainAst(query);
                UNIT_ASSERT_C(!ast.Contains("KqpCnSequencer"), ast);
                UNIT_ASSERT_C(ast.Contains("RandomNumber"),
                    "required DEFAULT expression is absent for: " << query << "\nAST:\n" << ast);
                UNIT_ASSERT_C(!ast.Contains("CurrentUtcTimestamp"),
                    "explicitly supplied DEFAULT expression was compiled for: " << query << "\nAST:\n" << ast);
            }
            {
                const TString query = TStringBuilder() << operation
                    << " INTO TestTable (k, r) VALUES (1, 7ul);";
                const TString ast = fixture.ExplainAst(query);
                UNIT_ASSERT_C(!ast.Contains("KqpCnSequencer"), ast);
                UNIT_ASSERT_C(!ast.Contains("RandomNumber"),
                    "explicitly supplied DEFAULT expression was compiled for: " << query << "\nAST:\n" << ast);
                UNIT_ASSERT_C(ast.Contains("CurrentUtcTimestamp"),
                    "required DEFAULT expression is absent for: " << query << "\nAST:\n" << ast);
            }
            {
                const TString query = TStringBuilder() << operation
                    << R"( INTO TestTable (k, r, ts) VALUES (1, 7ul, Timestamp("2020-01-01T00:00:00Z"));)";
                const TString ast = fixture.ExplainAst(query);
                UNIT_ASSERT_C(!ast.Contains("KqpCnSequencer"), ast);
                UNIT_ASSERT_C(!ast.Contains("RandomNumber") && !ast.Contains("CurrentUtcTimestamp"),
                    "unused DEFAULT expressions were compiled for: " << query << "\nAST:\n" << ast);
            }
        }
    }

    Y_UNIT_TEST_OCTET(UpsertPreparesOmittedDefaultKeys, CompileTimeDefaults, StreamWrite, WithIndex) {
        TTestFixture fixture(TFixtureOptions{
            .IndexStreamWrite = StreamWrite,
            .CompileTimeDefaults = CompileTimeDefaults});
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE LiteralKeyTable (
                k Int32 NOT NULL DEFAULT 1,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k)
            );
            CREATE TABLE CompositeKeyTable (
                k Int32 NOT NULL DEFAULT 1,
                part Uint64 NOT NULL DEFAULT RandomNumber(2) * 0ul + 2ul,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k, part)
            );
        )");
        if (WithIndex) {
            fixture.Exec("ALTER TABLE LiteralKeyTable ADD INDEX idx GLOBAL SYNC ON (other) COVER (v);");
            fixture.Exec("ALTER TABLE CompositeKeyTable ADD INDEX idx GLOBAL SYNC ON (other) COVER (v);");
        }

        UNIT_ASSERT(fixture.ColumnDesc("/Root/LiteralKeyTable", "k").HasDefaultFromLiteral());
        UNIT_ASSERT(fixture.ColumnDesc("/Root/CompositeKeyTable", "k").HasDefaultFromLiteral());
        AssertFromExpression(fixture.ColumnDesc("/Root/CompositeKeyTable", "part"),
            "RandomNumber(2) * 0ul + 2ul");

        fixture.Check("UPSERT INTO LiteralKeyTable (other) VALUES (5) RETURNING k, other, v;",
            "[[1;[5];[42u]]]");
        fixture.Exec("UPSERT INTO LiteralKeyTable (k, v) VALUES (1, 7ul);");
        fixture.Check("UPSERT INTO LiteralKeyTable (other) VALUES (6) RETURNING k, other, v;",
            "[[1;[6];[7u]]]");
        fixture.Check("SELECT k, other, v FROM LiteralKeyTable;", "[[1;[6];[7u]]]");

        fixture.Check("UPSERT INTO CompositeKeyTable (other) VALUES (5) RETURNING k, part, other, v;",
            "[[1;2u;[5];[42u]]]");
        fixture.Exec("UPSERT INTO CompositeKeyTable (k, part, v) VALUES (1, 2ul, 7ul);");
        fixture.Check("UPSERT INTO CompositeKeyTable (other) VALUES (6) RETURNING k, part, other, v;",
            "[[1;2u;[6];[7u]]]");
        fixture.Check("SELECT k, part, other, v FROM CompositeKeyTable;", "[[1;2u;[6];[7u]]]");
        if (WithIndex) {
            fixture.Check("SELECT k, other, v FROM LiteralKeyTable VIEW idx;", "[[1;[6];[7u]]]");
            fixture.Check("SELECT k, part, other, v FROM CompositeKeyTable VIEW idx;", "[[1;2u;[6];[7u]]]");
        }
    }

    Y_UNIT_TEST_TWIN(ExpressionSequenceAndLiteralDefaultCombinations, CompileTimeDefaults) {
        TTestFixture fixture(TFixtureOptions{.CompileTimeDefaults = CompileTimeDefaults});
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                id Serial,
                tag Utf8 NOT NULL,
                expr Uint64 DEFAULT )" << DefaultExpr << R"(,
                literal Uint64,
                PRIMARY KEY (id)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal SET DEFAULT 8ul;");

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "expr"), DefaultExpr);
        const auto literal = fixture.ColumnDesc("/Root/TestTable", "literal");
        UNIT_ASSERT_C(literal.HasDefaultFromLiteral(), literal.ShortDebugString());

        // Exercise every subset of omitted automatic columns: sequence + expression, expression +
        // literal, sequence + literal, all three, each one individually, and none.
        fixture.Exec(R"(INSERT INTO TestTable (tag, literal) VALUES ("a", 101ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (id, tag) VALUES (100, "b");)");
        fixture.Exec(R"(INSERT INTO TestTable (tag, expr) VALUES ("c", 103ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (tag) VALUES ("d");)");
        fixture.Exec(R"(INSERT INTO TestTable (id, tag, expr) VALUES (101, "e", 105ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (id, tag, literal) VALUES (102, "f", 106ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (tag, expr, literal) VALUES ("g", 107ul, 108ul);)");
        fixture.Exec(R"(INSERT INTO TestTable (id, tag, expr, literal) VALUES (103, "h", 109ul, 110ul);)");

        fixture.Check("SELECT tag, id, expr, literal FROM TestTable ORDER BY tag;", R"(
            [["a";1;[42u];[101u]];
             ["b";100;[42u];[8u]];
             ["c";2;[103u];[8u]];
             ["d";3;[42u];[8u]];
             ["e";101;[105u];[8u]];
             ["f";102;[42u];[106u]];
             ["g";4;[107u];[108u]];
             ["h";103;[109u];[110u]]]
        )");
    }

    Y_UNIT_TEST_TWIN(DmlMatrixWithoutIndex, WithReturning) {
        RunDmlMatrix(ESecondaryIndexKind::None, WithReturning);
    }

    Y_UNIT_TEST_QUAD(DmlMatrixWithSyncIndex, WithReturning, EnableIndexStreamWrite) {
        RunDmlMatrix(ESecondaryIndexKind::Sync, WithReturning, EnableIndexStreamWrite);
    }

    Y_UNIT_TEST_QUAD(DmlMatrixWithAsyncIndex, WithReturning, EnableIndexStreamWrite) {
        RunDmlMatrix(ESecondaryIndexKind::Async, WithReturning, EnableIndexStreamWrite);
    }

    Y_UNIT_TEST_TWIN(StreamLookupPlanMatrix, WithReturning) {
        TTestFixture fixture(TFixtureOptions{.IndexStreamWrite = false});
        fixture.Exec(ValueTableDDL());

        const auto query = [=](const TString& write) {
            return WithReturning ? write + " RETURNING k, other, v;" : write + ";";
        };

        fixture.CheckStreamLookup(query("INSERT INTO TestTable (k, other) VALUES (1, 10)"), false);
        fixture.CheckStreamLookup(query("UPSERT INTO TestTable (k, other) VALUES (1, 10)"), true);
        fixture.CheckStreamLookup(query("REPLACE INTO TestTable (k, other) VALUES (1, 10)"), false);
        fixture.CheckStreamLookup(query("UPDATE TestTable SET other = 10 WHERE k = 1"), WithReturning);
        fixture.CheckStreamLookup(query("DELETE FROM TestTable WHERE k = 1"), WithReturning);
        fixture.CheckStreamLookup(query("UPDATE TestTable ON (k, other) VALUES (1, 10)"), WithReturning);
        fixture.CheckStreamLookup(query("DELETE FROM TestTable ON (k) VALUES (1)"), WithReturning);
    }

    Y_UNIT_TEST(InsertOmittedAndExplicit) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[7u]]]");
    }

    Y_UNIT_TEST(UpsertOmittedNewRow) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("UPSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    // The materialized DEFAULT value is used only for the insert half of UPSERT: when the row
    // already exists, omitting the column leaves its stored value alone.
    Y_UNIT_TEST(UpsertOmittedExistingRowKeepsValue) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 5);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");
    }

    Y_UNIT_TEST(ExpressionLiteralAndGeneratedDefaultsStayConsistent) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                other Uint64,
                expr Uint64 DEFAULT )" << DefaultExpr << R"(,
                literal Uint64,
                generated Uint64 GENERATED ALWAYS AS (
                    COALESCE(other, 0ul) + COALESCE(expr, 0ul) + COALESCE(literal, 0ul)
                ) STORED,
                PRIMARY KEY (k),
                INDEX idx_generated GLOBAL ON (generated) COVER (other, expr, literal)
            ) WITH (
                PARTITION_AT_KEYS = (2)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal SET DEFAULT 8ul;");

        const auto literal = fixture.ColumnDesc("/Root/TestTable", "literal");
        UNIT_ASSERT_C(literal.HasDefaultFromLiteral(), literal.ShortDebugString());

        fixture.Exec("INSERT INTO TestTable (k, other, expr, literal) VALUES (1, 1ul, 7ul, 9ul);");
        const TString returned = fixture.QueryYson(R"(
            UPSERT INTO TestTable (k, other) VALUES (1, 5ul), (2, 6ul)
            RETURNING k, other, expr, literal, generated;
        )");

        const TString expected = "[[1;[5u];[7u];[9u];[21u]];[2;[6u];[42u];[8u];[56u]]]";
        CompareYsonUnordered(expected, returned);
        fixture.Check("SELECT k, other, expr, literal, generated FROM TestTable ORDER BY k;", expected);
        fixture.Check("SELECT k, other, expr, literal, generated FROM TestTable VIEW idx_generated ORDER BY generated;",
            expected);
    }

    Y_UNIT_TEST(UpsertNotNullGeneratedDependsOnNotNullExpressionDefault) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                id Int32 NOT NULL,
                a Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 9ul,
                b Uint64,
                g Uint64 NOT NULL GENERATED ALWAYS AS (a + COALESCE(b, 0ul)) STORED,
                PRIMARY KEY (id)
            );
        )");

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "a"), "RandomNumber(1) * 0ul + 9ul");

        // A new row uses the materialized DEFAULT dependency: a = 9, g = 9 + 2
        fixture.Exec("UPSERT INTO TestTable (id, b) VALUES (1, 2);");

        // An existing row keeps its stored dependency instead of replacing it with the DEFAULT
        fixture.Exec("UPSERT INTO TestTable (id, a, b) VALUES (2, 4, 1);");
        fixture.Exec("UPSERT INTO TestTable (id, b) VALUES (2, 3);");

        fixture.Check("SELECT id, a, b, g FROM TestTable ORDER BY id;",
            "[[1;9u;[2u];11u];[2;4u;[3u];7u]]");
    }

    Y_UNIT_TEST(StoredAndVirtualColumnsUseDefaultedValue) {
        auto appConfig = DefaultExprAppConfig(TFixtureOptions{.GeneratedStored = true});
        appConfig.MutableFeatureFlags()->SetEnableGeneratedVirtual(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                s Uint64 NOT NULL GENERATED ALWAYS AS (v + 1ul) STORED,
                g Uint64 NOT NULL GENERATED ALWAYS AS (v + 2ul) VIRTUAL,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL SYNC ON (v) COVER (s)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v, s, g;",
            "[[1;42u;43u;44u]]");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 50ul;");
        fixture.CheckUnordered("UPSERT INTO TestTable (k) VALUES (1), (2) RETURNING k, v, s, g;",
            "[[1;42u;43u;44u];[2;50u;51u;52u]]");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        fixture.RestartSchemeShard("/Root/TestTable");
        fixture.Check("UPDATE TestTable SET v = 7ul WHERE k = 1 RETURNING k, v, s, g;",
            "[[1;7u;8u;9u]]");
        fixture.Check("SELECT k, v, s, g FROM TestTable VIEW idx_v ORDER BY k;",
            "[[1;7u;8u;9u];[2;50u;51u;52u]]");
    }

    Y_UNIT_TEST_QUAD(FailingDefaultOnlyRunsOnUpsertInsert, StreamWrite, WithIndex) {
        TTestFixture fixture(TFixtureOptions{.IndexStreamWrite = StreamWrite});
        fixture.Exec(DefaultExprDDL(
            R"(v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default evaluated"))"));
        if (WithIndex) {
            fixture.Exec("ALTER TABLE TestTable ADD INDEX idx GLOBAL SYNC ON (other) COVER (v);");
        }
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Check("UPSERT INTO TestTable (k, other) VALUES (1, 5) RETURNING k, other, v;",
            "[[1;[5];[7u]]]");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");
        if (WithIndex) {
            fixture.Check("SELECT k, other, v FROM TestTable VIEW idx;", "[[1;[5];[7u]]]");
        }
        fixture.Rejects("UPSERT INTO TestTable (k, other) VALUES (2, 6);", "default evaluated");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");
        fixture.Exec("UPSERT INTO TestTable (k, other, v) VALUES (2, 6, 8ul);");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", "[[1;[5];[7u]];[2;[6];[8u]]]");
        fixture.Exec("UPSERT INTO TestTable (k, v) VALUES (1, NULL);");
        fixture.Check("UPSERT INTO TestTable (k, other) VALUES (1, 9) RETURNING k, other, v;",
            "[[1;[9];#]]");
    }

    Y_UNIT_TEST_QUAD(NullKeyUpsertPreservesLazyDefaults, StreamWrite, WithIndex) {
        TTestFixture fixture(TFixtureOptions{.IndexStreamWrite = StreamWrite});
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE NullKeyTable (
                k Int32,
                other Int32,
                v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default evaluated"),
                PRIMARY KEY (k)
            );
            CREATE TABLE CompositeNullKeyTable (
                k Int32,
                part Int32,
                other Int32,
                v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default evaluated"),
                PRIMARY KEY (k, part)
            );
            CREATE TABLE NullKeyInsertTable (
                k Int32,
                part Int32,
                other Int32,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k, part)
            );
        )");
        if (WithIndex) {
            for (const TString table : {"NullKeyTable", "CompositeNullKeyTable", "NullKeyInsertTable"}) {
                fixture.Exec(TStringBuilder() << "ALTER TABLE " << table
                    << " ADD INDEX idx GLOBAL SYNC ON (other) COVER (v);");
            }
        }

        fixture.Exec("INSERT INTO NullKeyTable (k, v) VALUES (NULL, 7ul);");
        fixture.Check("UPSERT INTO NullKeyTable (k, other) VALUES (NULL, 5) RETURNING k, other, v;",
            "[[#;[5];[7u]]]");
        fixture.Exec("UPSERT INTO NullKeyTable (k, v) VALUES (NULL, NULL);");
        fixture.Check("UPSERT INTO NullKeyTable (k, other) VALUES (NULL, 6) RETURNING k, other, v;",
            "[[#;[6];#]]");
        fixture.Check("SELECT k, other, v FROM NullKeyTable;", "[[#;[6];#]]");
        if (WithIndex) {
            fixture.Check("SELECT k, other, v FROM NullKeyTable VIEW idx;", "[[#;[6];#]]");
        }
        fixture.Exec("DELETE FROM NullKeyTable WHERE k IS NULL;");
        fixture.Rejects("UPSERT INTO NullKeyTable (k, other) VALUES (NULL, 7);", "default evaluated");
        fixture.Check("SELECT COUNT(*) FROM NullKeyTable;", "[[0u]]");
        fixture.Rejects("UPSERT INTO NullKeyTable (other) VALUES (8);", "Missing key column in input: k");

        fixture.Exec("INSERT INTO CompositeNullKeyTable (k, part, v) VALUES (1, NULL, 7ul), (NULL, 1, NULL);");
        const TString existingRows = "[[[1];#;[5];[7u]];[#;[1];[6];#]]";
        fixture.CheckUnordered("UPSERT INTO CompositeNullKeyTable (k, part, other) "
            "VALUES (1, NULL, 5), (NULL, 1, 6) RETURNING k, part, other, v;", existingRows);
        fixture.CheckUnordered("SELECT k, part, other, v FROM CompositeNullKeyTable;", existingRows);
        fixture.Rejects("UPSERT INTO CompositeNullKeyTable (k, part, other) VALUES (NULL, NULL, 7);",
            "default evaluated");
        fixture.CheckUnordered("SELECT k, part, other, v FROM CompositeNullKeyTable;", existingRows);
        if (WithIndex) {
            fixture.CheckUnordered("SELECT k, part, other, v FROM CompositeNullKeyTable VIEW idx;", existingRows);
        }

        const TString insertedRows = "[[#;[1];[10];[42u]];[[1];#;[20];[42u]];[#;#;[30];[42u]]]";
        fixture.CheckUnordered("UPSERT INTO NullKeyInsertTable (k, part, other) "
            "VALUES (NULL, 1, 10), (1, NULL, 20), (NULL, NULL, 30) RETURNING k, part, other, v;", insertedRows);
        fixture.CheckUnordered("SELECT k, part, other, v FROM NullKeyInsertTable;", insertedRows);
        if (WithIndex) {
            fixture.CheckUnordered("SELECT k, part, other, v FROM NullKeyInsertTable VIEW idx;", insertedRows);
        }
    }

    Y_UNIT_TEST(UpsertOmittedLiteralDefaultKeepsExistingRow) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64"));
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT 42ul;");

        const auto literal = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(literal.HasDefaultFromLiteral(), literal.ShortDebugString());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 5);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");
    }

    Y_UNIT_TEST(UpsertExplicitOverridesDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("UPSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[7u]]]");
    }

    Y_UNIT_TEST(ReplaceOmitted) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(UpdateDoesNotApplyDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("UPDATE TestTable SET other = 5 WHERE k = 1;");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");

        fixture.Exec("UPDATE TestTable SET v = 8ul WHERE k = 1;");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[8u]]]");

        fixture.Exec("UPDATE TestTable ON (k, other) VALUES (1, 6);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[6];[8u]]]");
    }

    Y_UNIT_TEST(DeleteUnaffected) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Exec("DELETE FROM TestTable WHERE k = 1;");
        fixture.Check("SELECT k, v FROM TestTable;", "[[2;[42u]]]");
    }

    Y_UNIT_TEST(MultiRowValuesAndInsertFromSelect) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2), (3);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[42u]];[3;[42u]]]");

        fixture.Exec("INSERT INTO TestTable (k) SELECT k + 10 FROM TestTable;");
        fixture.CheckUnordered("SELECT k, v FROM TestTable WHERE k > 10;",
            "[[11;[42u]];[12;[42u]];[13;[42u]]]");
    }

    Y_UNIT_TEST_TWIN(ReturningSelectSourceForms, EnableStreamWrite) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableStreamWrite(EnableStreamWrite);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(ValueTableDDL());
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, other Int32, PRIMARY KEY (k));");
        fixture.Exec("UPSERT INTO Source (k, other) VALUES (1, 11), (2, 22), (3, 33), (4, 44), (5, 55), (6, 66);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (4, 7ul), (6, 8ul);");

        fixture.CheckUnordered(R"(
            INSERT INTO TestTable (k, other) SELECT k, other FROM Source WHERE k IN (1, 2) RETURNING *;
        )", "[[1;[11];[42u]];[2;[22];[42u]]]");
        fixture.Check(R"(
            INSERT OR REVERT INTO TestTable (k, other) SELECT k, other FROM Source WHERE k = 3 RETURNING k;
        )", "[[3]]");
        fixture.CheckUnordered(R"(
            UPSERT INTO TestTable (k, other) SELECT k, other FROM Source WHERE k IN (4, 5) RETURNING v, k;
        )", "[[[7u];4];[[42u];5]]");
        fixture.CheckUnordered(R"(
            REPLACE INTO TestTable (k, other) SELECT k, other FROM Source WHERE k IN (5, 6) RETURNING v;
        )", "[[[42u]];[[42u]]]");
        fixture.Check(R"(
            UPDATE TestTable ON SELECT k, other + 1 AS other FROM Source WHERE k = 4 RETURNING v, other, k;
        )", "[[[7u];[45];4]]");
        fixture.Check(R"(
            DELETE FROM TestTable ON SELECT k FROM Source WHERE k = 2 RETURNING v, k;
        )", "[[[42u];2]]");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;",
            "[[1;[11];[42u]];[3;[33];[42u]];[4;[45];[7u]];[5;[55];[42u]];[6;[66];[42u]]]");
    }

    Y_UNIT_TEST(ParameterizedSelectSourceAppliesDefaults) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul), (4, 8ul);");
        auto params = TParamsBuilder()
            .AddParam("$rows").BeginList()
                .AddListItem().BeginStruct().AddMember("k").Int32(1).EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Int32(2).EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Int32(3).EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Int32(4).EndStruct()
            .EndList().Build()
            .Build();
        const auto write = [&](const TString& statement, const TString& expected) {
            auto result = fixture.QuerySession().ExecuteQuery(
                "DECLARE $rows AS List<Struct<k:Int32>>;\n" + statement,
                TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), statement << "\n" << result.GetIssues().ToString());
            CompareYsonUnordered(expected, FormatResultSetYson(result.GetResultSet(0)));
        };

        write("INSERT INTO TestTable (k) SELECT k FROM AS_TABLE($rows) WHERE k = 1 RETURNING k, v;",
            "[[1;[42u]]]");
        write("UPSERT INTO TestTable (k) SELECT k FROM AS_TABLE($rows) WHERE k IN (2, 3) RETURNING k, v;",
            "[[2;[7u]];[3;[42u]]]");
        write("REPLACE INTO TestTable (k) SELECT k FROM AS_TABLE($rows) WHERE k = 4 RETURNING k, v;",
            "[[4;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[7u]];[3;[42u]];[4;[42u]]]");
    }

    Y_UNIT_TEST(EmptyWritesDoNotEvaluateDefault) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(R"(v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default ran"))"));
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        const auto params = TParamsBuilder()
            .AddParam("$rows").BeginList().AddListItem().BeginStruct().AddMember("k").Int32(2).EndStruct()
            .EndList().Build()
            .Build();

        for (TStringBuf operation : {"INSERT", "INSERT OR REVERT", "UPSERT", "REPLACE"}) {
            fixture.Check(TStringBuilder() << operation << " INTO TestTable (k) SELECT k FROM Source RETURNING v;", "[]");
            const TString query = TStringBuilder() << "DECLARE $rows AS List<Struct<k:Int32>>; "
                << operation << " INTO TestTable (k) SELECT k FROM AS_TABLE($rows) WHERE k < 0 RETURNING v;";
            auto result = fixture.QuerySession().ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
            CompareYson("[]", FormatResultSetYson(result.GetResultSet(0)));
        }

        fixture.Check("UPDATE TestTable SET other = 5 WHERE k = 99 RETURNING v;", "[]");
        fixture.Check("UPDATE TestTable ON (k, other) VALUES (99, 5) RETURNING v;", "[]");
        fixture.Check("DELETE FROM TestTable WHERE k = 99 RETURNING v;", "[]");
        fixture.Check("DELETE FROM TestTable ON (k) VALUES (99) RETURNING v;", "[]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[7u]]]");
    }

    Y_UNIT_TEST(InsertConflictAndRevertPreserveStoredDefaults) {
        TTestFixture fixture;
        fixture.Exec(DmlMatrixDDL(ESecondaryIndexKind::Sync));
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (1, 11, 7ul);");

        fixture.RejectsWithStatus("INSERT INTO TestTable (k, other) VALUES (2, 20), (2, 21);",
            EStatus::PRECONDITION_FAILED);
        fixture.RejectsWithStatus("INSERT INTO TestTable (k, other) VALUES (2, 20), (1, 21);",
            EStatus::PRECONDITION_FAILED);
        fixture.Exec("INSERT OR REVERT INTO TestTable (k, other) VALUES (2, 20), (3, 30), (2, 21);");
        fixture.Exec("INSERT OR REVERT INTO TestTable (k, other) VALUES (2, 20), (1, 21);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[11];[7u]]]");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v;", "[[1;[11];[7u]]]");

        fixture.CheckUnordered("INSERT OR REVERT INTO TestTable (k, other) VALUES (2, 20), (3, 30) RETURNING k, v;",
            "[[2;[42u]];[3;[42u]]]");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (4, 40), (4, 41);");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;",
            "[[1;[11];[7u]];[2;[20];[42u]];[3;[30];[42u]];[4;[41];[42u]]]");
    }

    Y_UNIT_TEST_TWIN(InteractiveTransactionSeesDefaultAndIndex, Commit) {
        TTestFixture fixture;
        fixture.Exec(DmlMatrixDDL(ESecondaryIndexKind::Sync));
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (1, 10, 7ul);");
        auto& session = fixture.QuerySession();

        auto insert = session.ExecuteQuery("INSERT INTO TestTable (k, other) VALUES (2, 20) RETURNING k, v;",
            TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        CompareYson("[[2;[42u]]]", FormatResultSetYson(insert.GetResultSet(0)));
        auto tx = insert.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        // The following partial UPSERT must read this uncommitted override.
        auto update = session.ExecuteQuery("UPDATE TestTable SET v = 8ul WHERE k = 2 AND v = 42ul;",
            TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(update.IsSuccess(), update.GetIssues().ToString());
        auto upsert = session.ExecuteQuery(R"(
            UPSERT INTO TestTable (k, other) VALUES (1, 11), (2, 21) RETURNING k, other, v;
        )", TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        const TString expected = "[[1;[11];[7u]];[2;[21];[8u]]]";
        CompareYsonUnordered(expected, FormatResultSetYson(upsert.GetResultSet(0)));

        const std::string indexQuery = "SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;";
        auto read = session.ExecuteQuery(indexQuery, TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        CompareYson(expected, FormatResultSetYson(read.GetResultSet(0)));

        if (Commit) {
            auto result = tx->Commit().ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } else {
            auto result = tx->Rollback().ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        const TString stored = Commit ? expected : "[[1;[10];[7u]]]";
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", stored);
        fixture.Check(indexQuery, stored);
    }

    Y_UNIT_TEST(InteractiveTransactionConflictRollsBackDefaultAndIndex) {
        TTestFixture fixture;
        fixture.Exec(DmlMatrixDDL(ESecondaryIndexKind::Sync));
        auto& session = fixture.QuerySession();
        auto insert = session.ExecuteQuery("INSERT INTO TestTable (k) VALUES (1);",
            TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
        auto tx = insert.GetTransaction();
        UNIT_ASSERT(tx && tx->IsActive());

        auto conflict = session.ExecuteQuery("INSERT INTO TestTable (k) VALUES (2), (1);",
            TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(conflict.GetStatus(), EStatus::PRECONDITION_FAILED, conflict.GetIssues().ToString());
        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::NOT_FOUND, commit.GetIssues().ToString());
        fixture.Check("SELECT k, v FROM TestTable;", "[]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v;", "[]");
    }

    Y_UNIT_TEST(MultipleStatementsReturnDefaultsAndRollBackOnConflict) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        auto& session = fixture.QuerySession();
        auto result = session.ExecuteQuery(R"(
            INSERT INTO TestTable (k, other) VALUES (1, 10), (2, 20) RETURNING *;
            UPDATE TestTable SET other = 11 WHERE k = 1 AND v = 42ul RETURNING v, k;
            DELETE FROM TestTable WHERE k = 2 AND v = 42ul RETURNING k;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3u);
        CompareYsonUnordered("[[1;[10];[42u]];[2;[20];[42u]]]", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson("[[[42u];1]]", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson("[[2]]", FormatResultSetYson(result.GetResultSet(2)));

        auto conflict = session.ExecuteQuery(R"(
            INSERT INTO TestTable (k) VALUES (3);
            INSERT INTO TestTable (k) VALUES (1);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(conflict.GetStatus(), EStatus::PRECONDITION_FAILED, conflict.GetIssues().ToString());
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[11];[42u]]]");
    }

    Y_UNIT_TEST(MultiShardBatchWritesMaintainDefaultedIndex) {
        auto appConfig = DefaultExprAppConfig();
        auto* batch = appConfig.MutableTableServiceConfig()->MutableBatchOperationSettings();
        batch->SetMaxBatchSize(2);
        batch->SetPartitionExecutionLimit(2);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Uint32 NOT NULL,
                other Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL SYNC ON (v) COVER (other)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED,
                UNIFORM_PARTITIONS = 4
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k, other, v) VALUES (1u, 10, 7ul);");
        fixture.Exec(R"(
            INSERT INTO TestTable (k, other) VALUES (1500000000u, 20), (3500000000u, 30);
        )");
        fixture.CheckUnordered(R"(
            UPSERT INTO TestTable (k, other) VALUES (1u, 11), (2500000000u, 21), (3500000000u, 31)
            RETURNING k, other, v;
        )", "[[1u;11;7u];[2500000000u;21;42u];[3500000000u;31;42u]]");

        fixture.RejectsWithStatus(R"(
            INSERT INTO TestTable (k, other) VALUES (2u, 12), (3500000000u, 32);
        )", EStatus::PRECONDITION_FAILED);
        fixture.Check("SELECT k, v FROM TestTable WHERE k = 2u;", "[]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE other = 12;", "[]");

        fixture.Exec("BATCH UPDATE TestTable SET v = v + 1ul WHERE v = 42ul;");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 42ul;", "[]");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;",
            "[[1u;11;7u];[1500000000u;20;43u];[2500000000u;21;43u];[3500000000u;31;43u]]");
        fixture.Exec("BATCH DELETE FROM TestTable WHERE v = 43ul AND other > 20;");
        fixture.Check("SELECT k, other, v FROM TestTable ORDER BY k;", "[[1u;11;7u];[1500000000u;20;43u]]");
        fixture.Check("SELECT k, other, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1u;11;7u];[1500000000u;20;43u]]");
    }

    Y_UNIT_TEST(RandomDefaultWithConstantDependencyIsSharedBetweenRows) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("r Uint64 NOT NULL DEFAULT RandomNumber(1)"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2), (3), (4), (5), (6), (7), (8);");
        UNIT_ASSERT_VALUES_EQUAL(fixture.QueryUint64("SELECT COUNT(*) FROM TestTable;"), 8u);

        UNIT_ASSERT_VALUES_EQUAL_C(fixture.QueryUint64("SELECT COUNT(DISTINCT r) FROM TestTable;"), 1u,
            "RandomNumber() DEFAULT with a constant dependency must be shared between rows of one write");
    }

    Y_UNIT_TEST(UpdateAndDeleteRequireDefaultedKey) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Uint64 NOT NULL DEFAULT Ensure(RandomNumber(1), false, "key default evaluated"),
                v Int32,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1ul, 1);");

        fixture.Rejects("UPDATE TestTable ON (v) VALUES (2);", "Key columns are not specified");
        fixture.Rejects("DELETE FROM TestTable ON (v) VALUES (2);", "Key columns are not specified");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1u;[1]]]");
    }

    Y_UNIT_TEST(PrimaryKeyDefaultCollision) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                id Uint64 NOT NULL DEFAULT )" << DefaultExpr << R"(,
                v Int32,
                PRIMARY KEY (id)
            );
        )");

        fixture.Exec("INSERT INTO TestTable (v) VALUES (1);");
        fixture.Rejects("INSERT INTO TestTable (v) VALUES (2);", "Conflict with existing key");

        fixture.Exec("UPSERT INTO TestTable (v) VALUES (3);");
        fixture.Check("SELECT id, v FROM TestTable;", "[[42u;[3]]]");
    }

    Y_UNIT_TEST(Returning) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        const TString returned = fixture.QueryYson("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;");
        CompareYson("[[1;[42u]]]", returned);
        CompareYson(returned, fixture.QueryYson("SELECT k, v FROM TestTable;"));
    }

    Y_UNIT_TEST_TWIN(RandomDefaultConsistentWithReturningAndIndex, StreamWrite) {
        // GENERATED columns require the stream writer; legacy still uses actual random values.
        TTestFixture fixture(TFixtureOptions{.IndexStreamWrite = StreamWrite, .GeneratedStored = StreamWrite});
        const TString generatedColumn = StreamWrite ? "g Uint64 GENERATED ALWAYS AS (v) STORED," : "";
        const TString generatedCover = StreamWrite ? ", g" : "";
        const TString columns = TStringBuilder() << "k, a, v" << generatedCover;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1),
                )" << generatedColumn
                << "PRIMARY KEY (k), INDEX idx_a GLOBAL SYNC ON (a) COVER (v" << generatedCover
                << ")) WITH (PARTITION_AT_KEYS = (2));");

        const auto checkWrite = [&](const std::string& write, const TString& predicate,
                                    const TString& expectedKeys, ui64 expectedCount) {
            const TString returned = fixture.QueryYson(TStringBuilder() << write << " RETURNING " << columns << ";");
            CompareYsonUnordered(returned, fixture.QueryYson(
                TStringBuilder() << "SELECT " << columns << " FROM TestTable WHERE " << predicate << ";"));
            CompareYsonUnordered(returned, fixture.QueryYson(
                TStringBuilder() << "SELECT " << columns << " FROM TestTable VIEW idx_a WHERE " << predicate << ";"));
            fixture.Check(TStringBuilder() << "SELECT k, a FROM TestTable WHERE " << predicate << " ORDER BY k;", expectedKeys);
            if (StreamWrite) {
                fixture.Check(TStringBuilder() << "SELECT COUNT(*) FROM TestTable WHERE " << predicate << " AND v = g;",
                    TStringBuilder() << "[[" << expectedCount << "u]]");
            }
        };
        checkWrite("INSERT INTO TestTable (k, a) VALUES (1, 10), (2, 20), (3, 30)",
            "k <= 3", "[[1;10];[2;20];[3;30]]", 3);
        checkWrite("INSERT INTO TestTable (k, a) SELECT k + 10, a + 100 FROM TestTable",
            "k > 10", "[[11;110];[12;120];[13;130]]", 3);
        const TString existing = fixture.QueryYson(TStringBuilder() << "SELECT " << columns << " FROM TestTable WHERE k = 1;");
        checkWrite("UPSERT INTO TestTable (k, a) VALUES (1, 10), (4, 40)",
            "k IN (1, 4)", "[[1;10];[4;40]]", 2);
        CompareYson(existing, fixture.QueryYson(TStringBuilder() << "SELECT " << columns << " FROM TestTable WHERE k = 1;"));
        fixture.Check("SELECT k, a FROM TestTable ORDER BY k;",
            "[[1;10];[2;20];[3;30];[4;40];[11;110];[12;120];[13;130]]");
    }

    Y_UNIT_TEST_TWIN(RandomDefaultKeyConsistentWithReturningAndIndex, StreamWrite) {
        TTestFixture fixture(TFixtureOptions{.IndexStreamWrite = StreamWrite});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Uint64 NOT NULL DEFAULT RandomNumber(1),
                a Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(2),
                PRIMARY KEY (k),
                INDEX idx_a GLOBAL SYNC ON (a) COVER (v)
            );
        )");
        // A constant dependency may be shared between rows, so use a single-row write.
        const TString returned = fixture.QueryYson("INSERT INTO TestTable (a) VALUES (10) RETURNING k, a, v;");
        CompareYson(returned, fixture.QueryYson("SELECT k, a, v FROM TestTable;"));
        CompareYson(returned, fixture.QueryYson("SELECT k, a, v FROM TestTable VIEW idx_a;"));
        fixture.Check("SELECT a FROM TestTable;", "[[10]]");
    }

    Y_UNIT_TEST(CachedRandomDefaultConsistentWithReturningAndIndex) {
        TScopedEpochRandomProvider random;
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1),
                PRIMARY KEY (k),
                INDEX idx_a GLOBAL SYNC ON (a) COVER (v)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "RandomNumber(1)");

        auto tableClient = fixture.Runner().GetTableClient();
        auto session = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = session.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k, a)
            VALUES ($k, $k * 10), ($k + 1, $k * 10 + 1)
            RETURNING k, a, v;
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();
        NYdb::NTable::TExecDataQuerySettings settings;
        settings.KeepInQueryCache(true).CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);

        for (i32 key : {1, 3}) {
            const ui64 epoch = key == 1 ? 1 : 2;
            random.SetEpoch(epoch);
            auto params = prepared.GetParamsBuilder().AddParam("$k").Int32(key).Build().Build();
            auto result = prepared.Execute(
                NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats());
            if (key == 3) {
                UNIT_ASSERT(NYdb::TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache());
            }
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 2u);
            TResultSetParser draws(result.GetResultSet(0));
            while (draws.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(draws.ColumnParser("v").GetUint64() >> 60, epoch);
            }
            const TString returned = FormatResultSetYson(result.GetResultSet(0));
            const TString predicate = TStringBuilder() << "k IN (" << key << ", " << key + 1 << ")";
            // Each execution's actual draws must reach every consumer unchanged.
            CompareYsonUnordered(returned, fixture.QueryYson(
                TStringBuilder() << "SELECT k, a, v FROM TestTable WHERE " << predicate << ";"));
            CompareYsonUnordered(returned, fixture.QueryYson(
                TStringBuilder() << "SELECT k, a, v FROM TestTable VIEW idx_a WHERE " << predicate << ";"));
        }
        fixture.Check("SELECT k, a FROM TestTable ORDER BY k;", "[[1;10];[2;11];[3;30];[4;31]]");
        CompareYsonUnordered(fixture.QueryYson("SELECT k, a, v FROM TestTable;"),
            fixture.QueryYson("SELECT k, a, v FROM TestTable VIEW idx_a;"));
    }

    Y_UNIT_TEST_TWIN(ExplicitNullBypassesFailingDefault, StreamWrite) {
        TTestFixture fixture(TFixtureOptions{.IndexStreamWrite = StreamWrite});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default evaluated"),
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL SYNC ON (v)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"),
            R"(Ensure(RandomNumber(1), false, "default evaluated"))");
        fixture.Check("INSERT INTO TestTable (k, v) VALUES (1, NULL) RETURNING k, v;", "[[1;#]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v;", "[[1;#]]");
        fixture.Check("UPSERT INTO TestTable (k, v) VALUES (2, NULL) RETURNING k, v;", "[[2;#]]");
        for (const auto* write : {
            "UPSERT INTO TestTable (k, v) VALUES (1, NULL)",
            "REPLACE INTO TestTable (k, v) VALUES (1, NULL)",
            "UPDATE TestTable SET v = NULL WHERE k = 1",
            "UPDATE TestTable ON (k, v) VALUES (1, NULL)"})
        {
            fixture.Exec("UPSERT INTO TestTable (k, v) VALUES (1, 7ul);");
            fixture.Check(TStringBuilder() << write << " RETURNING k, v;", "[[1;#]]");
            fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;#]]");
            fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1;#];[2;#]]");
        }
    }

    Y_UNIT_TEST(WithSecondaryIndex) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT )" << DefaultExpr << R"(,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL ON (v)
            );
        )");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (2, 7ul);");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 42ul;", "[[1]]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 7ul;", "[[2]]");
    }

    Y_UNIT_TEST(UniqueIndexRejectsDefaultCollisionsAtomically) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL UNIQUE SYNC ON (v)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul), (2, 8ul);");

        // The omitted values collide with each other, so neither row may be inserted.
        fixture.RejectsWithStatus("INSERT INTO TestTable (k) VALUES (3), (4);", EStatus::PRECONDITION_FAILED);
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;7u];[2;8u]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1;7u];[2;8u]]");

        fixture.Check("INSERT INTO TestTable (k) VALUES (3) RETURNING k, v;", "[[3;42u]]");
        fixture.RejectsWithStatus("INSERT INTO TestTable (k) VALUES (4);", EStatus::PRECONDITION_FAILED);
        fixture.RejectsWithStatus(
            "UPDATE TestTable SET v = CASE WHEN k = 1 THEN 42ul ELSE 99ul END WHERE k < 3;",
            EStatus::PRECONDITION_FAILED);
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;7u];[2;8u];[3;42u]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1;7u];[2;8u];[3;42u]]");
    }

    Y_UNIT_TEST(NullableDefaultTracksUniqueIndexTransitions) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                v Uint64 DEFAULT CAST("invalid" || CAST(RandomNumber(1) AS String) AS Uint64),
                PRIMARY KEY (k),
                INDEX idx_v GLOBAL UNIQUE SYNC ON (v)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v IS NULL ORDER BY k;", "[[1;#];[2;#]]");
        fixture.Exec("UPDATE TestTable SET v = 42ul WHERE k = 1;");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 42ul;", "[[1;[42u]]]");
        fixture.RejectsWithStatus("INSERT INTO TestTable (k, v) VALUES (3, 42ul);", EStatus::PRECONDITION_FAILED);

        fixture.Check("REPLACE INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;#]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 42ul;", "[]");
        fixture.Exec("UPDATE TestTable SET v = 42ul WHERE k = 2;");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v WHERE v = 42ul;", "[[2;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;[42u]]]");
    }

    Y_UNIT_TEST(CompositeAndCoveringIndexesUseStoredDefaultValues) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                tenant Int32 NOT NULL,
                k Int32 NOT NULL,
                bucket Int32 NOT NULL,
                v Uint64 NOT NULL DEFAULT RandomNumber(1) * 0ul + 42ul,
                PRIMARY KEY (tenant, k),
                INDEX idx_async GLOBAL ASYNC ON (bucket) COVER (v)
            );
        )");
        fixture.Exec("INSERT INTO TestTable (tenant, k, bucket) VALUES (1, 1, 10);");
        fixture.Exec("INSERT INTO TestTable (tenant, k, bucket, v) VALUES (2, 1, 20, 7ul);");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 43ul;");
        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_pair GLOBAL SYNC ON (v, bucket);");
        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_cover GLOBAL SYNC ON (bucket) COVER (v);");

        const auto checkIndexes = [&](const TString& expected) {
            fixture.Check("SELECT tenant, k, bucket, v FROM TestTable ORDER BY tenant, k;", expected);
            fixture.Check("SELECT tenant, k, bucket, v FROM TestTable VIEW idx_pair ORDER BY tenant, k;", expected);
            fixture.Check("SELECT tenant, k, bucket, v FROM TestTable VIEW idx_cover ORDER BY tenant, k;", expected);
            fixture.CheckEventually("SELECT tenant, k, bucket, v FROM TestTable VIEW idx_async ORDER BY tenant, k;", expected);
        };
        checkIndexes("[[1;1;10;42u];[2;1;20;7u]]");
        fixture.Exec("UPSERT INTO TestTable (tenant, k, bucket) VALUES (1, 1, 11), (2, 2, 22);");
        fixture.Exec("REPLACE INTO TestTable (tenant, k, bucket) VALUES (2, 1, 21);");
        checkIndexes("[[1;1;11;42u];[2;1;21;43u];[2;2;22;43u]]");
        fixture.Check("SELECT tenant, k FROM TestTable VIEW idx_pair WHERE v = 7ul;", "[]");
        fixture.Check("SELECT tenant, k FROM TestTable VIEW idx_cover WHERE bucket IN (10, 20);", "[]");
    }

    Y_UNIT_TEST_TWIN(FulltextIndexUsesDefaultValueWithReturning, Compact) {
        auto appConfig = DefaultExprAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableFulltextIndex(true);
        appConfig.MutableFeatureFlags()->SetEnableCompactFulltextIndex(Compact);
        appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TTestFixture fixture(TKikimrSettings(appConfig).SetWithSampleTables(false));
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                text String NOT NULL DEFAULT IF(RandomNumber(1) * 0ul = 0ul, "Cats love naps.", "other"),
                PRIMARY KEY (k),
                INDEX idx_text GLOBAL USING fulltext_plain ON (text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
            );
        )");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "text"),
            R"(IF(RandomNumber(1) * 0ul = 0ul, "Cats love naps.", "other"))");
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, text;", R"([[1;"Cats love naps."]])");
        fixture.Check(R"(SELECT k, text FROM TestTable VIEW idx_text WHERE FulltextMatch(text, "cats");)",
            R"([[1;"Cats love naps."]])");
        fixture.Check(R"(UPDATE TestTable SET text = "Dogs bark." WHERE k = 1 RETURNING k, text;)",
            R"([[1;"Dogs bark."]])");
        fixture.Check(R"(SELECT k FROM TestTable VIEW idx_text WHERE FulltextMatch(text, "cats");)", "[]");
        fixture.Check(R"(SELECT k FROM TestTable VIEW idx_text WHERE FulltextMatch(text, "dogs");)", "[[1]]");
    }

    Y_UNIT_TEST(SetDefaultExpr) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);

        fixture.Check("INSERT INTO TestTable (k) VALUES (2) RETURNING k, v;", "[[2;[42u]]]");
        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (3, 7ul);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;[42u]];[3;[7u]]]");

        const TString replacement = "RandomNumber(1) * 0ul + 9ul";
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << replacement << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), replacement);
        fixture.Check("INSERT INTO TestTable (k) VALUES (4) RETURNING k, v;", "[[4;[9u]]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;#];[2;[42u]];[3;[7u]];[4;[9u]]]");
    }

    Y_UNIT_TEST_TWIN(SetCoercedDefaultExpr, FeatureEnabled) {
        TTestFixture fixture(TFixtureOptions{.FeatureFlagEnabled = FeatureEnabled});
        fixture.Exec(DefaultExprDDL("v Int64"));

        const TString query = TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";";
        if (FeatureEnabled) {
            fixture.Exec(query);
            AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
            fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[42]]]");
            fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42]]]");
        } else {
            fixture.Rejects(query, "DEFAULT expressions are disabled");
            const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(!column.HasDefaultFromExpression() && !column.HasDefaultFromLiteral(),
                column.ShortDebugString());
        }
    }

    Y_UNIT_TEST(SetDefaultExprOnPrimaryKeyColumn) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                id Uint64 NOT NULL,
                v Int32,
                PRIMARY KEY (id)
            );
        )");

        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN id SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "id"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (v) VALUES (7) RETURNING id, v;", "[[42u;[7]]]");
        fixture.Exec("INSERT INTO TestTable (id, v) VALUES (8ul, 9);");
        fixture.Check("SELECT id, v FROM TestTable ORDER BY id;", "[[8u;[9]];[42u;[7]]]");
    }

    Y_UNIT_TEST(SetDefaultExprTypeMismatchPreservesDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT AsList(RandomNumber(1));", "type mismatch");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(SetNullableDefaultExprOnNotNullColumnRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64 NOT NULL DEFAULT 7ul"));

        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT IF(RandomNumber(1) = 0ul, NULL, 1ul);",
            "nullable");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultFromLiteral().value().uint64_value(), 7u);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;7u]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;7u]]");
    }

    Y_UNIT_TEST(SetDefaultExprWithSqlBindingRejected) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Rejects(R"(
            $value = RandomNumber(1);
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT $value;
        )", "DEFAULT");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(SetDefaultExprSubqueryRejected) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT (SELECT v FROM TestTable LIMIT 1);",
            "subquery");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;[42u]]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(SetNullableDefaultExprInScript) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Bool DEFAULT true"));
        NYdb::NScripting::TScriptingClient scriptingClient(fixture.Runner().GetDriver());
        auto result = scriptingClient.ExecuteYqlScript(R"(
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT (RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL);
            COMMIT;
        )").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0u);
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"),
            "(RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL)");

        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;#]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;#]]");
    }

    Y_UNIT_TEST(SetNullableDefaultExprInScriptOnNotNullColumnRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Bool NOT NULL DEFAULT true"));
        NYdb::NScripting::TScriptingClient scriptingClient(fixture.Runner().GetDriver());
        auto result = scriptingClient.ExecuteYqlScript(R"(
            ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT (RandomNumber(1) * 0ul + 2ul) IN AsList(1ul, NULL);
            COMMIT;
        )").ExtractValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "nullable DEFAULT accepted for a NOT NULL column");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "nullable");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(column.HasDefaultFromLiteral(), column.ShortDebugString());
        UNIT_ASSERT(column.GetDefaultFromLiteral().value().bool_value());
        fixture.Check("INSERT INTO TestTable (k) VALUES (1) RETURNING k, v;", "[[1;%true]]");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;%true]]");
    }

    Y_UNIT_TEST(SetAndDropDefaultExprAfterRestartWithIndex) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64 DEFAULT 7ul"));
        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("ALTER TABLE TestTable ADD INDEX idx_v GLOBAL SYNC ON (v);");
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);

        fixture.RestartSchemeShard("/Root/TestTable");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), DefaultExpr);
        fixture.Check("INSERT INTO TestTable (k) VALUES (2) RETURNING k, v;", "[[2;[42u]]]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 7ul;", "[[1]]");
        fixture.Check("SELECT k FROM TestTable VIEW idx_v WHERE v = 42ul;", "[[2]]");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        fixture.RestartSchemeShard("/Root/TestTable");
        const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_VALUES_EQUAL(column.GetDefaultValueCase(), NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET);
        fixture.Check("INSERT INTO TestTable (k) VALUES (3) RETURNING k, v;", "[[3;#]]");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[7u]];[2;[42u]];[3;#]]");
        fixture.Check("SELECT k, v FROM TestTable VIEW idx_v ORDER BY k;", "[[1;[7u]];[2;[42u]];[3;#]]");
    }

    // Literal defaults retain their metadata kind when replacing an expression.
    Y_UNIT_TEST(SetLiteralDefaultOverExprColumn) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT 8ul;");
        {
            const auto col = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(col.HasDefaultFromLiteral(), col.ShortDebugString());
        }

        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[8u]]]");
    }

    // DROP DEFAULT removes the persisted default. Prepared-plan invalidation is checked below.
    Y_UNIT_TEST(DropDefaultAffectsNewWrites) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
        {
            const auto col = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(!col.HasDefaultFromExpression(), col.ShortDebugString());
        }

        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;#]]");
    }

    Y_UNIT_TEST_TWIN(PreparedInsertIsRecompiledAfterDefaultChange, DropDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        auto tableClient = fixture.Runner().GetTableClient();
        auto tableSession = tableClient.CreateSession().ExtractValueSync().GetSession();
        auto prepareResult = tableSession.PrepareDataQuery(R"(
            DECLARE $k AS Int32;
            INSERT INTO `/Root/TestTable` (k) VALUES ($k);
        )").ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        auto prepared = prepareResult.GetQuery();

        const auto executePrepared = [&](i32 key) {
            auto params = prepared.GetParamsBuilder()
                .AddParam("$k").Int32(key).Build()
                .Build();
            auto result = prepared.Execute(
                NYdb::NTable::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        executePrepared(1);
        if (DropDefault) {
            fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v DROP DEFAULT;");
            const auto column = fixture.ColumnDesc("/Root/TestTable", "v");
            UNIT_ASSERT_C(!column.HasDefaultFromExpression() && !column.HasDefaultFromLiteral(), column.ShortDebugString());
        } else {
            fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT 8ul;");
        }
        executePrepared(2);

        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;",
            DropDefault ? "[[1;[42u]];[2;#]]" : "[[1;[42u]];[2;[8u]]]");

        const TString replacement = "RandomNumber(1) * 0ul + 9ul";
        fixture.Exec(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << replacement << ";");
        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), replacement);
        executePrepared(3);
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", DropDefault
            ? "[[1;[42u]];[2;#];[3;[9u]]]"
            : "[[1;[42u]];[2;[8u]];[3;[9u]]]");
    }

    Y_UNIT_TEST(CachedWriteStopsUsingReplacedDefault) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        auto session = fixture.Runner().GetTableClient().CreateSession().ExtractValueSync().GetSession();
        const TString query = R"(
            DECLARE $k AS Int32;
            INSERT INTO TestTable (k) VALUES ($k);
        )";
        const auto settings = NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true)
            .CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);
        const auto fromCache = [&](i32 key) {
            auto params = TParamsBuilder().AddParam("$k").Int32(key).Build().Build();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, settings)
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStats().has_value());
            return TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache();
        };

        UNIT_ASSERT(!fromCache(1));
        UNIT_ASSERT(fromCache(2));
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT RandomNumber(1) * 0ul + 50ul;");
        UNIT_ASSERT(!fromCache(3));
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[42u]];[3;[50u]]]");
    }

    Y_UNIT_TEST(AlterOnGeneratedColumnRejected) {
        TTestFixture fixture(TFixtureOptions{.GeneratedStored = true});
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                g Int32 GENERATED ALWAYS AS (k + 1) STORED,
                PRIMARY KEY (k)
            );
        )");

        fixture.Rejects(TStringBuilder()
                << "ALTER TABLE TestTable ALTER COLUMN g SET DEFAULT " << DefaultExpr << ";",
            "DEFAULT of GENERATED column");
        fixture.Rejects("ALTER TABLE TestTable ALTER COLUMN g DROP DEFAULT;", "DEFAULT of GENERATED column");
    }

    Y_UNIT_TEST(SetDefaultExprOnSerialColumnRejected) {
        TTestFixture fixture;
        fixture.Exec("CREATE TABLE TestTable (k Serial, v Int32, PRIMARY KEY (k));");

        fixture.Rejects(TStringBuilder()
                << "ALTER TABLE TestTable ALTER COLUMN k SET DEFAULT " << DefaultExpr << ";",
            "serial/sequence column");
    }

    Y_UNIT_TEST(ShowCreateTable) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        const std::string ddl = fixture.ShowCreateTable("/Root/TestTable");
        UNIT_ASSERT_STRING_CONTAINS(ddl, TStringBuilder() << "DEFAULT " << DefaultExpr);
    }

    Y_UNIT_TEST(ShowCreateTableReplay) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        const auto origin = fixture.ColumnDesc("/Root/TestTable", "v").GetDefaultFromExpression();
        const std::string ddl = fixture.ShowCreateTable("/Root/TestTable");

        fixture.Exec("DROP TABLE TestTable;");
        fixture.Exec(ddl);

        const auto replayed = fixture.ColumnDesc("/Root/TestTable", "v").GetDefaultFromExpression();
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetExprText(), origin.GetExprText());
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetStored(), origin.GetStored());
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetWritable(), origin.GetWritable());

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(DescribeTableHidesExpression) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        auto tableClient = fixture.Runner().GetTableClient();
        auto session = tableClient.GetSession().GetValueSync().GetSession();
        auto describe = session.DescribeTable("/Root/TestTable").GetValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());

        // Column expressions are not part of the public API: the column describes without a default
        const auto& proto = NYdb::TProtoAccessor::GetProto(describe.GetTableDescription());
        bool found = false;
        for (const auto& column : proto.columns()) {
            if (column.name() != "v") {
                continue;
            }
            found = true;
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(column.default_value_case()),
                static_cast<int>(Ydb::Table::ColumnMeta::DEFAULT_VALUE_NOT_SET), column.ShortDebugString());
        }
        UNIT_ASSERT_C(found, "column v not found in DescribeTable");
    }

    Y_UNIT_TEST(DumpDescribeRejectsDefaultExpression) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());
        auto session = fixture.Runner().GetTableClient().CreateSession().ExtractValueSync().GetSession();
        auto describe = session.DescribeTable("/Root/TestTable",
            NYdb::NTable::TDescribeTableSettings().RequestType("dump")).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::UNSUPPORTED, describe.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(describe.GetIssues().ToString(), "/Root/TestTable");
        UNIT_ASSERT_STRING_CONTAINS(describe.GetIssues().ToString(),
            "exporting tables with DEFAULT expressions is not supported");
    }

    Y_UNIT_TEST(SurvivesSchemeShardRestart) {
        TTestFixture fixture;
        const TString expression = DefaultExpr;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << expression));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.RestartSchemeShard("/Root/TestTable");

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), expression);
        fixture.Exec("INSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[42u]]]");
    }

    Y_UNIT_TEST(BulkUpsert) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        auto tableClient = fixture.Runner().GetTableClient();

        {
            // The DEFAULT column is absent from the request: bulk upsert cannot evaluate it
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct().AddMember("k").Int32(1).EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "expected bulk upsert to be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: v");
        }

        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(1)
                    .AddMember("v").OptionalUint64(7)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[7u]]]");
    }

    Y_UNIT_TEST(BulkUpsertWithMixedExpressionAndLiteralDefaults) {
        TTestFixture fixture;
        fixture.Exec(TStringBuilder() << R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                expr Uint64 DEFAULT )" << DefaultExpr << R"(,
                literal Uint64,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN literal SET DEFAULT 8ul;");

        auto tableClient = fixture.Runner().GetTableClient();
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct().AddMember("k").Int32(1).EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "expected both missing DEFAULT columns to be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "expr");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "literal");
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(2)
                    .AddMember("expr").OptionalUint64(20)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "expected the missing literal DEFAULT to be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: literal");
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(3)
                    .AddMember("literal").OptionalUint64(30)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "expected the missing expression DEFAULT to be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: expr");
        }
        {
            NYdb::TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("k").Int32(4)
                    .AddMember("expr").OptionalUint64(40)
                    .AddMember("literal").OptionalUint64(50)
                .EndStruct()
                .EndList();
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        fixture.Check("SELECT k, expr, literal FROM TestTable;", "[[4;[40u];[50u]]]");
    }
}

}   // namespace NKikimr::NKqp
