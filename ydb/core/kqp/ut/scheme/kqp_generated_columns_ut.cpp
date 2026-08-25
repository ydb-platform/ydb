#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

static NKikimrConfig::TAppConfig GeneratedColumnsAppConfig() {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableDefaultFromExpression(false);
    appConfig.MutableFeatureFlags()->SetEnableGeneratedStored(true);
    appConfig.MutableFeatureFlags()->SetEnableGeneratedVirtual(true);
    appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    return appConfig;
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

std::string GeneratedColumnDDL(const std::string& expr, const std::string& prefix = "") {
    return prefix + R"(
        CREATE TABLE TestTable (
            k Int32 NOT NULL,
            a Int32,
            s String,
            v Int32 GENERATED ALWAYS AS ()" +
           expr + R"() STORED,
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
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(generated.GetKind()), static_cast<int>(expectStored
            ? NKikimrSchemeOp::TDefaultExpressionColumnDescription::GENERATED_STORED
            : NKikimrSchemeOp::TDefaultExpressionColumnDescription::GENERATED_VIRTUAL));

        UNIT_ASSERT_VALUES_EQUAL(generated.GetExprText(), "k + 1");
        UNIT_ASSERT(generated.HasContext());

        Cout << "EXPR:" << Endl << generated.GetExprText() << Endl;
        Cout << "CONTEXT:" << Endl << generated.GetContext() << Endl;

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
    explicit TTestFixture(const std::string& createTable, const std::string& seed = "")
        : Kikimr(TKikimrSettings(GeneratedColumnsAppConfig()).SetWithSampleTables(false))
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
        runtime.Send(MakePipePerNodeCacheID(false), NActors::TActorId(),
            new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), TTestTxConfig::SchemeShard, false));
        Sleep(TDuration::Seconds(3));
        NKikimr::Tests::TClient::RefreshPathCache(&runtime, TString(tablePath));
    }

private:
    TKikimrRunner Kikimr;
    NYdb::NQuery::TQueryClient Db;
    NYdb::NQuery::TSession Session;
};

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

    Y_UNIT_TEST(DependsOnDefaultDuringPartialUpsertOfExistingRow) {
        TTestFixture fixture(R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                c Int32 NOT NULL DEFAULT 7,
                g Int32 GENERATED ALWAYS AS (a + c) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL ON (g)
            );
        )");

        fixture.Exec("UPSERT INTO TestTable (k, a, c) VALUES (1, 3, 9);");
        fixture.Check("SELECT k, c, g FROM TestTable VIEW idx_g WHERE g = 12;", "[[1;9;[12]]]");

        fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 5);");
        fixture.Check("SELECT k, a, c, g FROM TestTable;", "[[1;5;9;[14]]]");
        fixture.Check("SELECT k, c, g FROM TestTable VIEW idx_g WHERE g = 12;", "[]");
        fixture.Check("SELECT k, c, g FROM TestTable VIEW idx_g WHERE g = 14;", "[[1;9;[14]]]");
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

    Y_UNIT_TEST(DefaultExprFeatureFlagDisabledPreservesGeneratedAndSerialColumns) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableDefaultFromExpression(false);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                CREATE TABLE TestTable (
                    id Serial,
                    payload Int32,
                    generated Int32 GENERATED ALWAYS AS (COALESCE(payload, 0) + 1) STORED,
                    PRIMARY KEY (id)
                );
            )", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(
                "INSERT INTO TestTable (payload) VALUES (5);", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(
                "SELECT payload, generated FROM TestTable;", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson("[[[5];[6]]]", FormatResultSetYson(result.GetResultSet(0)));
        }
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
        UNIT_ASSERT_VALUES_EQUAL(originSt.GetContext(), originVt.GetContext());
        UNIT_ASSERT_STRING_CONTAINS(originSt.GetContext(), "PRAGMA classic_division");

        const std::string ddl = GetShowCreateTable(session, "/Root/Origin");
        const auto contextPos = ddl.find("PRAGMA classic_division = '0';");
        const auto createTablePos = ddl.find("CREATE TABLE");
        UNIT_ASSERT_C(contextPos != std::string::npos, ddl.c_str());
        UNIT_ASSERT_C(createTablePos != std::string::npos, ddl.c_str());
        UNIT_ASSERT_C(contextPos < createTablePos, ddl.c_str());

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
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(replayedSt.GetKind()),
            static_cast<int>(NKikimrSchemeOp::TDefaultExpressionColumnDescription::GENERATED_STORED));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(replayedSt.GetKind()), static_cast<int>(originSt.GetKind()));
        UNIT_ASSERT_VALUES_EQUAL(replayedSt.GetExprText(), originSt.GetExprText());
        UNIT_ASSERT_VALUES_EQUAL(replayedSt.DependencyColumnNamesSize(), originSt.DependencyColumnNamesSize());
        UNIT_ASSERT_VALUES_EQUAL(replayedSt.GetContext(), "PRAGMA classic_division = '0';\n");

        const auto replayedVt = generatedOf("vt");
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(replayedVt.GetKind()),
            static_cast<int>(NKikimrSchemeOp::TDefaultExpressionColumnDescription::GENERATED_VIRTUAL));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(replayedVt.GetKind()), static_cast<int>(originVt.GetKind()));
        UNIT_ASSERT_VALUES_EQUAL(replayedVt.GetExprText(), originVt.GetExprText());
        UNIT_ASSERT_VALUES_EQUAL(replayedVt.DependencyColumnNamesSize(), originVt.DependencyColumnNamesSize());
        UNIT_ASSERT_VALUES_EQUAL(replayedVt.GetContext(), replayedSt.GetContext());
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
            {GeneratedColumnDDL("k + $x", "$x = SELECT MAX(a) FROM OtherTable;\n"), "subquery"},
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
            {GeneratedColumnDDL("IF(k IN $ids, 1, 0)", "$ids = SELECT a FROM OtherTable;\n"), "subquery"},
            {GeneratedColumnDDL("k + $doubled",
                 "$base = SELECT MAX(a) FROM OtherTable;\n$doubled = $base * 2;\n"),
                "subquery"},
        });
    }

    Y_UNIT_TEST(ParameterVariantsRejected) {
        CheckGeneratedColumnsRejected({
            {GeneratedColumnDDL("$p * a", "DECLARE $p AS Int32;\n"), "Unknown name: $p"},
            {GeneratedColumnDDL("COALESCE($p, k)", "DECLARE $p AS Int32;\n"), "Unknown name: $p"},
            {GeneratedColumnDDL("k + $p + $q", "DECLARE $p AS Int32;\nDECLARE $q AS Int32;\n"), "Unknown name"},
        });
    }

    Y_UNIT_TEST(SingleRowExpressionsAccepted) {
        CheckGeneratedColumnsAccepted({
            {"k + 1", ""},
            {"CASE WHEN k > 0 THEN COALESCE(a, 0) ELSE -1 END", ""},
            {"CAST(ListLength(ListMap(AsList(k, k + 1), ($e) -> { RETURN $e * 2 })) AS Int32)", ""},
            {"k + $c", "$c = 5;\n"},
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

// TODO (ditimizhev): wip
//
// Y_UNIT_TEST_SUITE(GeneratedVirtual) {
//     Y_UNIT_TEST(DependsOnSerial) {
//         auto appConfig = GeneratedColumnsAppConfig();
//         TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

//         auto db = kikimr.GetQueryClient();
//         auto session = db.GetSession().GetValueSync().GetSession();

//         {
//             const std::string query = R"(
//                 CREATE TABLE TestTable (
//                     id Serial,
//                     name String,
//                     g Int32 GENERATED ALWAYS AS (id * 10) VIRTUAL,
//                     PRIMARY KEY (id)
//                 );
//             )";
//             auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//         }

//         {
//             const std::string query = R"(
//                 INSERT INTO TestTable (name) VALUES ("a");
//             )";
//             auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//         }

//         {
//             const std::string query = R"(
//                 SELECT id, g FROM TestTable ORDER BY id;
//             )";
//             auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//             CompareYson(R"([
//                 [1;10]
//             ])",
//                 FormatResultSetYson(result.GetResultSet(0)));
//         }
//     }

//     Y_UNIT_TEST(AlterRejected) {
//         CheckGeneratedColumnAlterRejections("VIRTUAL");
//     }

//     Y_UNIT_TEST(FeatureFlagDisabled) {
//         auto appConfig = GeneratedColumnsAppConfig();
//         appConfig.MutableFeatureFlags()->SetEnableGeneratedVirtual(false);
//         TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

//         auto db = kikimr.GetQueryClient();
//         auto session = db.GetSession().GetValueSync().GetSession();

//         {
//             auto result = session
//                               .ExecuteQuery(R"(
//                 CREATE TABLE TVirtual (
//                     k Int32 NOT NULL,
//                     v Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
//                     PRIMARY KEY (k)
//                 );
//             )",
//                                   TTxControl::NoTx())
//                               .GetValueSync();
//             UNIT_ASSERT_C(!result.IsSuccess(), "VIRTUAL generated column must be rejected when the flag is off");
//             UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "VIRTUAL GENERATED columns are disabled");
//         }

//         {
//             auto result = session
//                               .ExecuteQuery(R"(
//                 CREATE TABLE TStored (
//                     k Int32 NOT NULL,
//                     v Int32 GENERATED ALWAYS AS (k + 1) STORED,
//                     PRIMARY KEY (k)
//                 );
//             )",
//                                   TTxControl::NoTx())
//                               .GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//         }
//     }

//     Y_UNIT_TEST(NotNullOptionalExprRejected) {
//         CheckGeneratedColumnRejected(R"(
//             CREATE TABLE TestTable (
//                 k Int32 NOT NULL,
//                 v1 Int32,
//                 v Int32 NOT NULL GENERATED ALWAYS AS (v1 + 1) VIRTUAL,
//                 PRIMARY KEY (k)
//             );
//         )",
//             "is declared NOT NULL, but its expression can evaluate to NULL");
//     }

//     Y_UNIT_TEST(NotNullJsonExistsRejected) {
//         CheckGeneratedColumnRejected(R"(
//             CREATE TABLE TestTable (
//                 k Int32 NOT NULL,
//                 v Json,
//                 hasKey Bool NOT NULL GENERATED ALWAYS AS (JSON_EXISTS(v, "$.key" UNKNOWN ON ERROR)) VIRTUAL,
//                 PRIMARY KEY (k)
//             );
//         )",
//             "is declared NOT NULL, but its expression can evaluate to NULL");
//     }

//     Y_UNIT_TEST(KeyRejected) {
//         CheckGeneratedColumnRejected(R"(
//             CREATE TABLE TestTable (
//                 k Int32,
//                 v Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
//                 PRIMARY KEY (v)
//             );
//         )",
//             "Generated columns cannot be part of the primary key");
//     }

//     Y_UNIT_TEST(Persisted) {
//         CheckGeneratedColumnPersisted(R"(
//             CREATE TABLE TestTable (
//                 k Int32,
//                 v Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
//                 PRIMARY KEY (k)
//             );
//         )",
//             /* expectStored */ false);
//     }

//     Y_UNIT_TEST(ComputedOnRead) {
//         auto appConfig = GeneratedColumnsAppConfig();
//         TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
//         auto db = kikimr.GetQueryClient();
//         auto session = db.GetSession().GetValueSync().GetSession();

//         {
//             auto result = session
//                               .ExecuteQuery(R"(
//                 CREATE TABLE TestTable (
//                     k Int32,
//                     v Int32 GENERATED ALWAYS AS (k + 1) VIRTUAL,
//                     PRIMARY KEY (k)
//                 );
//             )",
//                                   TTxControl::NoTx())
//                               .GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//         }
//         {
//             auto result = session.ExecuteQuery("UPSERT INTO TestTable (k) VALUES (1), (2);", TTxControl::NoTx()).GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//         }
//         {
//             auto result = session.ExecuteQuery("SELECT k, v FROM TestTable ORDER BY k;", TTxControl::NoTx()).GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//             CompareYson(R"([[[1];[2]];[[2];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
//         }
//         {
//             auto result = session.ExecuteQuery("SELECT v FROM TestTable ORDER BY k;", TTxControl::NoTx()).GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//             CompareYson(R"([[[2]];[[3]]])", FormatResultSetYson(result.GetResultSet(0)));
//         }
//         {
//             auto result = session.ExecuteQuery("SELECT * FROM TestTable ORDER BY k;", TTxControl::NoTx()).GetValueSync();
//             UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
//             CompareYson(R"([[[1];[2]];[[2];[3]]])", FormatResultSetYson(result.GetResultSet(0)));
//         }
//     }
// }

}   // namespace NKikimr::NKqp
