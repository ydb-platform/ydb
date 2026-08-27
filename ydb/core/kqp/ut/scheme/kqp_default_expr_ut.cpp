#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

NKikimrConfig::TAppConfig DefaultExprAppConfig(
    bool enabled = true,
    bool indexStreamWrite = true,
    bool generatedStored = false)
{
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableDefaultFromExpression(enabled);
    appConfig.MutableFeatureFlags()->SetEnableGeneratedStored(generatedStored);
    appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(true);
    appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(indexStreamWrite);
    return appConfig;
}

// CREATE TABLE stores every captured DEFAULT source as an expression. A volatile expression makes
// it possible to verify that evaluation stays on the write path; multiplying its result by zero
// keeps the value predictable so the tests below can assert exact rows
constexpr const char* DefaultExpr = "RandomNumber(1) * 0ul + 42ul";

class TTestFixture {
public:
    explicit TTestFixture(
        bool featureFlagEnabled = true,
        bool indexStreamWrite = true,
        bool generatedStored = false)
        : Kikimr(TKikimrSettings(DefaultExprAppConfig(featureFlagEnabled, indexStreamWrite, generatedStored))
              .SetWithSampleTables(false))
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
        auto& runtime = *Kikimr.GetTestServer().GetRuntime();
        runtime.Send(MakePipePerNodeCacheID(false), NActors::TActorId(),
            new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), TTestTxConfig::SchemeShard, false));
        Sleep(TDuration::Seconds(3));
        NKikimr::Tests::TClient::RefreshPathCache(&runtime, TString(tablePath));
    }

    TKikimrRunner& Runner() {
        return Kikimr;
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
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(defaultExpression.GetKind()),
        static_cast<int>(NKikimrSchemeOp::TDefaultExpressionColumnDescription::DEFAULT));
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
    TTestFixture fixture(
        /* featureFlagEnabled */ true,
        /* indexStreamWrite */ enableIndexStreamWrite);
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

    Y_UNIT_TEST(NamedNodeContext) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64 DEFAULT RandomNumber(1) * $factor + 42ul", "$factor = 0ul;\n"));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "RandomNumber(1) * $factor + 42ul");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(PragmaContext) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr,
            "PRAGMA AnsiInForEmptyOrNullableItemsCollections;\n"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(UnrelatedDeclareAccepted) {
        TTestFixture fixture;
        // A DECLARE in the statement prefix must be stripped when the expression is recompiled
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << DefaultExpr,
            "DECLARE $unused AS Int32;\n"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[42u]]]");
    }

    Y_UNIT_TEST(LiteralSourceStoredAsExpression) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32 DEFAULT 5"));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "5");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[5]]]");
    }

    Y_UNIT_TEST(DeterministicSourceStoredAsExpression) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Int32 DEFAULT (1 + 2) * 3"));

        AssertFromExpression(fixture.ColumnDesc("/Root/TestTable", "v"), "(1 + 2) * 3");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[9]]]");
    }

    Y_UNIT_TEST(LiteralSourceFallsBackToLiteralWhenFeatureFlagDisabled) {
        TTestFixture fixture(/* featureFlagEnabled */ false);
        fixture.Exec(DefaultExprDDL("v Uint32 DEFAULT 5"));

        const auto col = fixture.ColumnDesc("/Root/TestTable", "v");
        UNIT_ASSERT_C(col.HasDefaultFromLiteral(), col.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(col.GetDefaultFromLiteral().type().type_id()),
            static_cast<int>(Ydb::Type::UINT32));
        UNIT_ASSERT_VALUES_EQUAL(col.GetDefaultFromLiteral().value().uint32_value(), 5u);

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[5u]]]");
    }

    Y_UNIT_TEST(FeatureFlagDisabled) {
        TTestFixture fixture(/* featureFlagEnabled */ false);
        fixture.Rejects(ValueTableDDL(), "DEFAULT expressions are disabled");
    }

    Y_UNIT_TEST(CoercibleSourceRequiresFeatureFlag) {
        TTestFixture fixture(/* featureFlagEnabled */ false);
        fixture.Rejects(DefaultExprDDL(TStringBuilder() << "v Int64 DEFAULT " << DefaultExpr),
            "DEFAULT expressions are disabled");
    }

    Y_UNIT_TEST(FeatureFlagDisabledPreservesLiteralDefaultsAndWrites) {
        TTestFixture fixture(/* featureFlagEnabled */ false);
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

    Y_UNIT_TEST(NamedExpressionSubqueryRejected) {
        TTestFixture fixture;
        fixture.Exec("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY (k));");
        fixture.Rejects(DefaultExprDDL("v Bool DEFAULT 1 IN $ids", "$ids = (SELECT k FROM Source);\n"),
            "subquery");
    }

    Y_UNIT_TEST(ParameterRejected) {
        TTestFixture fixture;
        fixture.Rejects(DefaultExprDDL("v Int32 DEFAULT $p", "DECLARE $p AS Int32;\n"), "Unknown name: $p");
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

    Y_UNIT_TEST(SingleRowExpressionWithNestedLambdaAccepted) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(R"(
            v Int32 DEFAULT CAST(
                ListLength(ListMap(AsList(1, 2), ($item) -> { RETURN $item * 2; }))
                AS Int32
            )
        )"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.Check("SELECT k, v FROM TestTable;", "[[1;[2]]]");
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

    Y_UNIT_TEST(DefaultAndGeneratedRejected) {
        TTestFixture fixture(
            /* featureFlagEnabled */ true,
            /* indexStreamWrite */ true,
            /* generatedStored */ true);
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

    Y_UNIT_TEST(ExpressionSequenceAndLiteralDefaultCombinations) {
        TTestFixture fixture;
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
        TTestFixture fixture(
            /* featureFlagEnabled */ true,
            /* indexStreamWrite */ false);
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
    // already exists, omitting the column leaves its stored value alone. The expression is still
    // evaluated eagerly before the existing-row lookup, as pinned down by the next test
    Y_UNIT_TEST(UpsertOmittedExistingRowKeepsValue) {
        TTestFixture fixture;
        fixture.Exec(ValueTableDDL());

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        fixture.Exec("UPSERT INTO TestTable (k, other) VALUES (1, 5);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[5];[7u]]]");
    }

    Y_UNIT_TEST(ExpressionLiteralAndGeneratedDefaultsStayConsistent) {
        TTestFixture fixture(
            /* featureFlagEnabled */ true,
            /* indexStreamWrite */ true,
            /* generatedStored */ true);
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
        TTestFixture fixture(
            /* featureFlagEnabled */ true,
            /* indexStreamWrite */ true,
            /* generatedStored */ true);
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                id Int32 NOT NULL,
                a Int32 NOT NULL DEFAULT (1 + 2) * 3,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (a + COALESCE(b, 0)) STORED,
                PRIMARY KEY (id)
            );
        )");

        // A new row uses the materialized DEFAULT dependency: a = 9, g = 9 + 2
        fixture.Exec("UPSERT INTO TestTable (id, b) VALUES (1, 2);");

        // An existing row keeps its stored dependency instead of replacing it with the DEFAULT
        fixture.Exec("UPSERT INTO TestTable (id, a, b) VALUES (2, 4, 1);");
        fixture.Exec("UPSERT INTO TestTable (id, b) VALUES (2, 3);");

        fixture.Check("SELECT id, a, b, g FROM TestTable ORDER BY id;",
            "[[1;9;[2];11];[2;4;[3];7]]");
    }

    Y_UNIT_TEST(EagerDefaultFailureOnExistingUpsert) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL(
            R"(v Uint64 DEFAULT Ensure(RandomNumber(1), false, "default evaluated"))"));

        fixture.Exec("INSERT INTO TestTable (k, v) VALUES (1, 7ul);");
        // Missing defaults are intentionally evaluated while building the candidate UPSERT row,
        // before KQP learns that the key exists. Its value is discarded, but an error is observable
        fixture.Rejects("UPSERT INTO TestTable (k, other) VALUES (1, 5);", "default evaluated");

        // Supplying the column explicitly bypasses its DEFAULT expression
        fixture.Exec("UPSERT INTO TestTable (k, other, v) VALUES (1, 6, 7ul);");
        fixture.Check("SELECT k, other, v FROM TestTable;", "[[1;[6];[7u]]]");
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

    Y_UNIT_TEST(RandomDefaultWithConstantDependencyIsSharedBetweenRows) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("r Uint64 NOT NULL DEFAULT RandomNumber(1)"));

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1), (2), (3), (4), (5), (6), (7), (8);");

        UNIT_ASSERT_VALUES_EQUAL_C(fixture.QueryUint64("SELECT COUNT(DISTINCT r) FROM TestTable;"), 1u,
            "RandomNumber() DEFAULT with a constant dependency must be shared between rows of one write");
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

    Y_UNIT_TEST(RandomDefaultConsistentWithReturningAndIndex) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                v Uint64 DEFAULT RandomNumber(1),
                PRIMARY KEY (k),
                INDEX idx_a GLOBAL SYNC ON (a) COVER (v)
            );
        )");

        const TString returned = fixture.QueryYson(
            "INSERT INTO TestTable (k, a) VALUES (1, 10), (2, 20), (3, 30) RETURNING k, a, v;");
        CompareYsonUnordered(returned, fixture.QueryYson("SELECT k, a, v FROM TestTable ORDER BY k;"));
        CompareYsonUnordered(returned, fixture.QueryYson("SELECT k, a, v FROM TestTable VIEW idx_a ORDER BY a;"));
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

    // A DEFAULT expression reaches SchemeShard only through the internal CREATE TABLE path. ALTER
    // travels as a public Ydb::Table::AlterTableRequest, which carries no column expressions
    Y_UNIT_TEST(SetDefaultExprRejected) {
        TTestFixture fixture;
        fixture.Exec(DefaultExprDDL("v Uint64"));

        fixture.Rejects(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT " << DefaultExpr << ";",
            "can only be defined in CREATE TABLE");
    }

    Y_UNIT_TEST(SetDefaultExprOnPrimaryKeyColumnRejected) {
        TTestFixture fixture;
        fixture.Exec(R"(
            CREATE TABLE TestTable (
                id Uint64 NOT NULL,
                v Int32,
                PRIMARY KEY (id)
            );
        )");

        fixture.Rejects(TStringBuilder() << "ALTER TABLE TestTable ALTER COLUMN id SET DEFAULT " << DefaultExpr << ";",
            "can only be defined in CREATE TABLE");
    }

    // Literal defaults are unaffected: they fit the public request
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

    // DROP DEFAULT needs nothing from the request beyond empty_default, so it keeps working, and
    // the schema version bump must invalidate the compiled plan of the earlier INSERT
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

    Y_UNIT_TEST(PreparedInsertIsRecompiledAfterDefaultChange) {
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
        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN v SET DEFAULT 8ul;");
        executePrepared(2);

        fixture.Check("SELECT k, v FROM TestTable ORDER BY k;", "[[1;[42u]];[2;[8u]]]");
    }

    Y_UNIT_TEST(AlterOnGeneratedColumnRejected) {
        TTestFixture fixture(
            /* featureFlagEnabled */ true,
            /* indexStreamWrite */ true,
            /* generatedStored */ true);
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
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(replayed.GetKind()), static_cast<int>(origin.GetKind()));
        UNIT_ASSERT_VALUES_EQUAL(replayed.GetContext(), origin.GetContext());

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

    Y_UNIT_TEST(SurvivesSchemeShardRestart) {
        TTestFixture fixture;
        const TString expression = "RandomNumber(1) * $factor + 42ul";
        fixture.Exec(DefaultExprDDL(TStringBuilder() << "v Uint64 DEFAULT " << expression,
            "$factor = 0ul;\n"));

        const auto beforeRestart = fixture.ColumnDesc("/Root/TestTable", "v").GetDefaultFromExpression();
        UNIT_ASSERT_STRING_CONTAINS(beforeRestart.GetContext(), "$factor = 0ul;");

        fixture.Exec("INSERT INTO TestTable (k) VALUES (1);");
        fixture.RestartSchemeShard("/Root/TestTable");

        const auto afterRestart = fixture.ColumnDesc("/Root/TestTable", "v").GetDefaultFromExpression();
        UNIT_ASSERT_VALUES_EQUAL(afterRestart.GetExprText(), expression);
        UNIT_ASSERT_VALUES_EQUAL(afterRestart.GetContext(), beforeRestart.GetContext());
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
