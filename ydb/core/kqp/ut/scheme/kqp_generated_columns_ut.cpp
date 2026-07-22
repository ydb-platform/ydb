#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

static NKikimrConfig::TAppConfig GeneratedColumnsAppConfig() {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableFeatureFlags()->SetEnableGeneratedStored(true);
    appConfig.MutableFeatureFlags()->SetEnableGeneratedVirtual(true);
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
constexpr const char* MultiGeneratedControlRow = "[3;[7];[8];[9];[200];[15];[89]]";
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

class TGeneratedUpdateFixture {
public:
    explicit TGeneratedUpdateFixture(const std::string& createTable, const std::string& seed = "")
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

private:
    TKikimrRunner Kikimr;
    NYdb::NQuery::TQueryClient Db;
    NYdb::NQuery::TSession Session;
};

}   // namespace

Y_UNIT_TEST_SUITE(GeneratedStored) {
    Y_UNIT_TEST(Basic) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v1 Int32,
                    v2 Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2) VALUES (1, 1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[4]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2) VALUES (1, 2);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[5]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2, v1) VALUES (1, 3, 3);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[8]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2) VALUES (1, 5);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[10]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(WithIndex) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v1 Int32,
                    v2 Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                    PRIMARY KEY (k),
                    INDEX idx_v GLOBAL ON (v)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2) VALUES (1, 1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v FROM TestTable VIEW idx_v WHERE v = 4;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[4]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2, v1) VALUES (1, 1, 3);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v FROM TestTable VIEW idx_v WHERE v = 4;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson("[]", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const std::string query = R"(
                SELECT k, v FROM TestTable VIEW idx_v WHERE v = 6;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[6]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2) VALUES (1, 5);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v FROM TestTable VIEW idx_v WHERE v = 10;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[10]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DependsOnDefault) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    c Int32 NOT NULL DEFAULT 7,
                    g Int32 GENERATED ALWAYS AS (k + c) STORED,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k) VALUES (1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, c, g FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;7;[8]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Insert) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v1 Int32,
                    v2 Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Omit v1: new row stores v1 = NULL. v = 1*2 + 1 + COALESCE(NULL, 1) = 4
        {
            const std::string query = R"(
                INSERT INTO TestTable (k, v2) VALUES (1, 1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Supply every dependency. v = 2*2 + 3 + COALESCE(5, 1) = 12
        {
            const std::string query = R"(
                INSERT INTO TestTable (k, v2, v1) VALUES (2, 3, 5);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v1, v FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;#;[4]];
                [2;[5];[12]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Replace) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v1 Int32,
                    v2 Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Seed a row with a non-null v1. v = 1*2 + 1 + COALESCE(5, 1) = 8
        {
            const std::string query = R"(
                INSERT INTO TestTable (k, v2, v1) VALUES (1, 1, 5);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // REPLACE the existing row omitting v1: v1 is reset to NULL. v = 1*2 + 3 + COALESCE(NULL, 1) = 6
        {
            const std::string query = R"(
                REPLACE INTO TestTable (k, v2) VALUES (1, 3);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // REPLACE inserting a new row: v1 = NULL. v = 2*2 + 1 + COALESCE(NULL, 1) = 6
        {
            const std::string query = R"(
                REPLACE INTO TestTable (k, v2) VALUES (2, 1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, v1, v FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;#;[6]];
                [2;#;[6]]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Returning) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v1 Int32,
                    v2 Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto returns = [&](const std::string& query, const TString& expected) {
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        };

        // UPSERT a new row (v1 read back as NULL). v = 1*2 + 1 + COALESCE(NULL, 1) = 4
        returns("UPSERT INTO TestTable (k, v2) VALUES (1, 1) RETURNING k, v;", R"([[1;[4]]])");

        // UPSERT the existing row supplying v1. v = 1*2 + 3 + COALESCE(5, 1) = 10
        returns("UPSERT INTO TestTable (k, v2, v1) VALUES (1, 3, 5) RETURNING k, v;", R"([[1;[10]]])");

        // UPSERT the existing row omitting v1 (== 5): it is read back, not treated as NULL
        // v = 1*2 + 4 + COALESCE(5, 1) = 11
        returns("UPSERT INTO TestTable (k, v2) VALUES (1, 4) RETURNING k, v;", R"([[1;[11]]])");

        // INSERT a new row (v1 = NULL). v = 2*2 + 3 + COALESCE(NULL, 1) = 8
        returns("INSERT INTO TestTable (k, v2) VALUES (2, 3) RETURNING k, v;", R"([[2;[8]]])");

        // REPLACE a new row (v1 = NULL). v = 3*2 + 1 + COALESCE(NULL, 1) = 8
        returns("REPLACE INTO TestTable (k, v2) VALUES (3, 1) RETURNING k, v;", R"([[3;[8]]])");
    }

    Y_UNIT_TEST(ReturningWithIndex) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v1 Int32,
                    v2 Int32 NOT NULL,
                    v Int32 GENERATED ALWAYS AS (k * 2 + v2 + COALESCE(v1, 1)) STORED,
                    PRIMARY KEY (k),
                    INDEX idx_v GLOBAL ON (v)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto returns = [&](const std::string& query, const TString& expected) {
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        };
        auto viaIndex = [&](const std::string& value, const TString& expected) {
            const std::string query = "SELECT k, v FROM TestTable VIEW idx_v WHERE v = " + value + " ORDER BY k;";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        };

        // UPSERT supplying v1. v = 1*2 + 1 + COALESCE(3, 1) = 6
        returns("UPSERT INTO TestTable (k, v2, v1) VALUES (1, 1, 3) RETURNING k, v;", R"([[1;[6]]])");
        viaIndex("6", R"([[1;[6]]])");

        // Partial UPSERT omitting v1 (== 3): read back, index updated. v = 1*2 + 5 + COALESCE(3, 1) = 10
        returns("UPSERT INTO TestTable (k, v2) VALUES (1, 5) RETURNING k, v;", R"([[1;[10]]])");
        viaIndex("6", "[]");
        viaIndex("10", R"([[1;[10]]])");

        // INSERT (v1 = NULL). v = 2*2 + 3 + COALESCE(NULL, 1) = 8
        returns("INSERT INTO TestTable (k, v2) VALUES (2, 3) RETURNING k, v;", R"([[2;[8]]])");

        // REPLACE (v1 = NULL). v = 3*2 + 1 + COALESCE(NULL, 1) = 8
        returns("REPLACE INTO TestTable (k, v2) VALUES (3, 1) RETURNING k, v;", R"([[3;[8]]])");

        // Both k=2 and k=3 land on v == 8 in the index
        viaIndex("8", R"([[2;[8]];[3;[8]]])");
    }

    Y_UNIT_TEST(DependsOnSerial) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    id Serial,
                    name String,
                    g Int32 GENERATED ALWAYS AS (id * 10) STORED,
                    PRIMARY KEY (id)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                INSERT INTO TestTable (name) VALUES ("a"), ("b");
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT id, g FROM TestTable ORDER BY id;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[10]];
                [2;[20]];
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(NotNull) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v1 Int32,
                    v2 Int32 NOT NULL,
                    g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(v1, 0) + v2) STORED,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Inline path: every dependency supplied. g = COALESCE(5, 0) + 1 = 6
        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v1, v2) VALUES (1, 5, 1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Stream-lookup path: partial UPSERT omitting v1 (== 5), which is read back. g = 5 + 3 = 8
        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2) VALUES (1, 3);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // INSERT omitting the nullable v1: it is stored as NULL, and COALESCE keeps g non-NULL
        // g = COALESCE(NULL, 0) + 3 = 3.
        {
            const std::string query = R"(
                INSERT INTO TestTable (k, v2) VALUES (2, 3);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // g is NOT NULL, so it comes back non-optional
        {
            const std::string query = R"(
                SELECT k, g FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;8];
                [2;3]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(NotNullWithIndex) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v1 Int32,
                    v2 Int32 NOT NULL,
                    g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(v1, 0) + v2) STORED,
                    PRIMARY KEY (k),
                    INDEX idx_g GLOBAL ON (g)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // g = COALESCE(3, 0) + 1 = 4
        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v1, v2) VALUES (1, 3, 1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, g FROM TestTable VIEW idx_g WHERE g = 4;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;4]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

        // Partial UPSERT omitting v1 (== 3): read back, index updated. g = 3 + 5 = 8
        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v2) VALUES (1, 5);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT k, g FROM TestTable VIEW idx_g WHERE g = 4;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson("[]", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const std::string query = R"(
                SELECT k, g FROM TestTable VIEW idx_g WHERE g = 8;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;8]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(NotNullDependsOnSerial) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    id Serial,
                    name String,
                    g Int32 NOT NULL GENERATED ALWAYS AS (id * 10) STORED,
                    PRIMARY KEY (id)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                INSERT INTO TestTable (name) VALUES ("a");
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT id, g FROM TestTable ORDER BY id;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;10]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
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

    Y_UNIT_TEST(NonDeterministicRejected) {
        CheckGeneratedColumnRejected(R"(
            CREATE TABLE TestTable (
                k Int32,
                v Int32 GENERATED ALWAYS AS (RandomNumber(k)) STORED,
                PRIMARY KEY (k)
            );
        )",
            "deterministic");
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
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    k Int32 NOT NULL,
                    v Json,
                    hasKey Bool GENERATED ALWAYS AS (JSON_EXISTS(v, "$.key" UNKNOWN ON ERROR)) STORED,
                    PRIMARY KEY (k)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO TestTable (k, v) VALUES
                    (1, CAST(@@{"key": 1}@@ AS Json)),
                    (2, CAST(@@{"other": 1}@@ AS Json)),
                    (3, NULL);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // A NULL document yields a NULL value, which a nullable column stores
        {
            const std::string query = R"(
                SELECT k, hasKey FROM TestTable ORDER BY k;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [1;[%true]];
                [2;[%false]];
                [3;#]
            ])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
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
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session
                              .ExecuteQuery(R"(
                CREATE TABLE TestTable (
                    k Int32,
                    v Int32 GENERATED ALWAYS AS (k + 1) STORED,
                    PRIMARY KEY (k)
                );
            )",
                                  TTxControl::NoTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteQuery("UPSERT INTO TestTable (k, v) VALUES (1, 99);", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "supplying a value for a generated column must be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "cannot be set explicitly");
        }
    }

    Y_UNIT_TEST(TtlRejected) {
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto result = session
                          .ExecuteQuery(R"(
            CREATE TABLE TestTable (
                k Int32,
                base Timestamp,
                ts Timestamp GENERATED ALWAYS AS (base) STORED,
                PRIMARY KEY (k)
            ) WITH (TTL = Interval("PT1H") ON ts);
        )",
                              TTxControl::NoTx())
                          .GetValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "TTL on a generated column must be rejected");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "can not be a GENERATED column");
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
        auto appConfig = GeneratedColumnsAppConfig();
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session
                              .ExecuteQuery(R"(
                CREATE TABLE TestTable (
                    k Int32,
                    a Int32,
                    v Int32 GENERATED ALWAYS AS (a + 1) STORED,
                    PRIMARY KEY (k)
                );
            )",
                                  TTxControl::NoTx())
                              .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteQuery("ALTER TABLE TestTable DROP COLUMN a;", TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), "dropping a dependency of a generated column must be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "used by generated column");
        }
    }

    Y_UNIT_TEST(UpdateSetGeneratedRejected) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        fixture.Rejects("UPDATE TestTable SET g1 = 5 WHERE k = 1;", "cannot be set explicitly");
        fixture.Rejects("UPDATE TestTable SET g2 = 5 WHERE k = 1;", "cannot be set explicitly");

        // Nothing was written
        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[3];[4];[3];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetGeneratedWithDependencyRejected) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        fixture.Rejects("UPDATE TestTable SET a = 5, g1 = 5 WHERE k = 1;", "cannot be set explicitly");
        fixture.Rejects("UPDATE TestTable SET d = 5, g2 = 5 WHERE k = 1;", "cannot be set explicitly");
        fixture.Rejects("UPDATE TestTable SET g1 = 1, g2 = 2 WHERE k = 1;", "cannot be set explicitly");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[3];[4];[3];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetOneDependency) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = COALESCE(10, 0) + COALESCE(2, 0) = 12, g2 untouched (b, c unchanged) = 23
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[10];[2];[3];[4];[12];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetAllDependencies) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 5 + 6 = 11, g2 = 6*10 + 7 = 67
        fixture.Exec("UPDATE TestTable SET a = 5, b = 6, c = 7 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[5];[6];[7];[4];[11];[67]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetSharedDependency) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 1 + 7 = 8, g2 = 7*10 + 3 = 73
        fixture.Exec("UPDATE TestTable SET b = 7 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[7];[3];[4];[8];[73]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetIndependentDependencies) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 4 + 2 = 6, g2 = 2*10 + 9 = 29
        fixture.Exec("UPDATE TestTable SET a = 4, c = 9 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[4];[2];[9];[4];[6];[29]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetNonDependency) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        fixture.Exec("UPDATE TestTable SET d = 42 WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[3];[42];[3];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateSetDependencyToNull) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 1 + 0 = 1, g2 = 0*10 + 3 = 3
        fixture.Exec("UPDATE TestTable SET b = NULL WHERE k = 1;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];#;[3];[4];[1];[3]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereDependency) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // Matches k=1 only. g1 = 10 + 2 = 12, g2 = 23
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE b = 2;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[10];[2];[3];[4];[12];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGenerated) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 == 3 matches k=1 only. g1 = 10 + 2 = 12, g2 = 23
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE g1 = 3;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[10];[2];[3];[4];[12];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGeneratedAndDependency) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // g1 = 3 unchanged (a, b untouched), g2 = 2*10 + 9 = 29
        fixture.Exec("UPDATE TestTable SET c = 9 WHERE g1 = 3 AND b = 2;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[9];[4];[3];[29]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGeneratedSetSharedDependency) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        // WHERE sees the old g2 == 23; after the write g1 = 1 + 7 = 8, g2 = 7*10 + 3 = 73
        fixture.Exec("UPDATE TestTable SET b = 7 WHERE g2 = 23;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[7];[3];[4];[8];[73]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGeneratedNoMatch) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed);

        fixture.Exec("UPDATE TestTable SET a = 10 WHERE g1 = 999;");

        fixture.Check(MultiGeneratedSelect,
            TStringBuilder() << "[[1;[1];[2];[3];[4];[3];[23]];" << MultiGeneratedUntouchedRow << "]");
    }

    Y_UNIT_TEST(UpdateWithIndexOnGenerated) {
        TGeneratedUpdateFixture fixture(IndexedGeneratedTableDDL);
        fixture.Exec("UPSERT INTO TestTable (k, a, b) VALUES (1, 1, 2);");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[[1;[3]]]");

        // g1 = 10 + 2 = 12
        fixture.Exec("UPDATE TestTable SET a = 10 WHERE k = 1;");

        fixture.Check("SELECT k, a, b, g1 FROM TestTable ORDER BY k;", "[[1;[10];[2];[12]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 12;", "[[1;[12]]]");
    }

    Y_UNIT_TEST(UpdateReturningStarNoGeneratedUpdate) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET d = 55 WHERE k < 3 RETURNING *;",
            MultiGeneratedStarOrderSelect,
            "[[[1];[2];[3];[55];[3];[23];1];[[4];[5];[6];[55];[9];[56];2]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];[2];[3];[55];[3];[23]];[2;[4];[5];[6];[55];[9];[56]];" << MultiGeneratedControlRow << "]");
    }

    Y_UNIT_TEST(UpdateReturningStarWithGeneratedUpdate) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // g1 = 100 + b, g2 untouched
        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100 WHERE k < 3 RETURNING *;",
            MultiGeneratedStarOrderSelect,
            "[[[100];[2];[3];[100];[102];[23];1];[[100];[5];[6];[100];[105];[56];2]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[100];[2];[3];[100];[102];[23]];[2;[100];[5];[6];[100];[105];[56]];"
            << MultiGeneratedControlRow << "]");
    }

    Y_UNIT_TEST(UpdateReturningAllColumnsListed) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        // b feeds both: g1 = a + 50, g2 = 50*10 + c
        fixture.CheckReturning(
            "UPDATE TestTable SET b = 50 WHERE k < 3 RETURNING k, a, b, c, d, g1, g2;",
            "SELECT k, a, b, c, d, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[1];[50];[3];[100];[51];[503]];[2;[4];[50];[6];[100];[54];[506]]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];[50];[3];[100];[51];[503]];[2;[4];[50];[6];[100];[54];[506]];"
            << MultiGeneratedControlRow << "]");
    }

    Y_UNIT_TEST(UpdateReturningGeneratedUpdated) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100 WHERE k < 3 RETURNING k, g1;",
            "SELECT k, g1 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[102]];[2;[105]]]");
    }

    Y_UNIT_TEST(UpdateReturningGeneratedNotUpdated) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET d = 55 WHERE k < 3 RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[3];[23]];[2;[9];[56]]]");
    }

    Y_UNIT_TEST(UpdateReturningDependenciesUpdatedAndNot) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100 WHERE k < 3 RETURNING k, a, b, g1;",
            "SELECT k, a, b, g1 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[100];[2];[102]];[2;[100];[5];[105]]]");
    }

    Y_UNIT_TEST(UpdateReturningOneOfTwoGenerated) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100 WHERE k < 3 RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[102];[23]];[2;[105];[56]]]");
    }

    Y_UNIT_TEST(UpdateReturningBothGeneratedSharedDependency) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET b = 50 WHERE k < 3 RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[51];[503]];[2;[54];[506]]]");
    }

    Y_UNIT_TEST(UpdateReturningBothGeneratedIndependentDependencies) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET a = 100, c = 77 WHERE k < 3 RETURNING k, g1, g2;",
            "SELECT k, g1, g2 FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[102];[97]];[2;[105];[127]]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[100];[2];[77];[100];[102];[97]];[2;[100];[5];[77];[100];[105];[127]];"
            << MultiGeneratedControlRow << "]");
    }

    Y_UNIT_TEST(UpdateReturningNoGeneratedColumns) {
        TGeneratedUpdateFixture fixture(MultiGeneratedTableDDL, MultiGeneratedSeed3);

        fixture.CheckReturning(
            "UPDATE TestTable SET d = 55 WHERE k < 3 RETURNING k, d;",
            "SELECT k, d FROM TestTable WHERE k < 3 ORDER BY k;",
            "[[1;[55]];[2;[55]]]");

        fixture.Check(MultiGeneratedSelect, TStringBuilder()
            << "[[1;[1];[2];[3];[55];[3];[23]];[2;[4];[5];[6];[55];[9];[56]];" << MultiGeneratedControlRow << "]");
    }

    Y_UNIT_TEST(UpdateWhereGeneratedWithIndex) {
        TGeneratedUpdateFixture fixture(IndexedGeneratedTableDDL);
        fixture.Exec("UPSERT INTO TestTable (k, a, b) VALUES (1, 1, 2), (2, 5, 5);");

        fixture.Exec("UPDATE TestTable SET b = 7 WHERE g1 = 3;");

        fixture.Check("SELECT k, a, b, g1 FROM TestTable ORDER BY k;", "[[1;[1];[7];[8]];[2;[5];[5];[10]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 3;", "[]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 8;", "[[1;[8]]]");
        fixture.Check("SELECT k, g1 FROM TestTable VIEW idx_g1 WHERE g1 = 10;", "[[2;[10]]]");
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
