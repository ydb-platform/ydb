#include "generated_column_test_utils.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <algorithm>
#include <map>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NGeneratedColumnTest;

struct TReferenceColumn {
    std::string Name;
    std::string Expression;
    bool Stored;
    std::string SeedValue;
};

struct TReferenceCase {
    std::string Name;
    TVector<TReferenceColumn> Columns;
    TVector<std::string> StarNames;
    bool Constant = false;
};

const TVector<TReferenceCase>& ReferenceCases() {
    static const std::string sum = "COALESCE(a, 0) + COALESCE(b, 0)";
    static const TVector<TReferenceCase> cases = {
        {"control", {{"g1", sum, true, "13"}, {"g2", "(" + sum + ") * 2", false, "26"}},
            {"a", "b", "g1", "g2", "k"}},
        {"virtual", {{"g1", sum, false, "13"}, {"g2", "g1 * 2", false, "26"}},
            {"a", "b", "g1", "g2", "k"}},
        {"stored", {{"g1", sum, true, "13"}, {"g2", "g1 * 2", true, "26"}},
            {"a", "b", "g1", "g2", "k"}},
        {"stored_virtual", {{"g1", sum, true, "13"}, {"g2", "g1 * 2", false, "26"}},
            {"a", "b", "g1", "g2", "k"}},
        {"alternating", {{"g0", sum, false, "13"}, {"g1", "g0", true, "13"},
            {"mid", "g1", false, "13"}, {"g2", "mid * 2", true, "26"}},
            {"a", "b", "g0", "g1", "g2", "k", "mid"}},
        {"diamond", {{"root", sum, false, "13"}, {"g1", "root", true, "13"},
            {"rhs", "root", false, "13"}, {"g2", "g1 + rhs", true, "26"}},
            {"a", "b", "g1", "g2", "k", "rhs", "root"}},
        {"independent", {{"g1", sum, true, "13"}, {"g2", "g1 * 2", true, "26"},
            {"other", "COALESCE(b, 0) * 10", true, "30"}},
            {"a", "b", "g1", "g2", "k", "other"}},
        {"constant", {{"g1", "13", true, "13"}, {"g2", "g1 * 2", false, "26"}},
            {"a", "b", "g1", "g2", "k"}, true},
    };
    return cases;
}

std::string JoinNames(const TVector<std::string>& names) {
    std::string result;
    for (const auto& name : names) {
        if (!result.empty()) {
            result += ", ";
        }
        result += name;
    }
    return result;
}

std::string ReferenceDDL(const TReferenceCase& schema, bool reverseColumns = false, bool reverseDependencies = false,
    bool includeIndexes = true) {
    TVector<std::string> columns = {"k Int32 NOT NULL", "a Int32", "b Int32 DEFAULT 3"};
    std::string cover = "b";
    for (const auto& column : schema.Columns) {
        auto expression = column.Expression;
        if (reverseDependencies) {
            const std::string before = "COALESCE(a, 0) + COALESCE(b, 0)";
            const auto offset = expression.find(before);
            if (offset != std::string::npos) {
                expression.replace(offset, before.size(), "COALESCE(b, 0) + COALESCE(a, 0)");
            }
        }
        columns.push_back(column.Name + " Int32 NOT NULL GENERATED ALWAYS AS (" + expression + ") "
            + (column.Stored ? "STORED" : "VIRTUAL"));
        if (column.Stored) {
            cover += ", " + column.Name;
        }
    }
    if (reverseColumns) {
        std::reverse(columns.begin(), columns.end());
    }
    auto ddl = "CREATE TABLE TestTable (" + JoinNames(columns) + ", PRIMARY KEY(k)";
    if (includeIndexes) {
        ddl += ", INDEX idx_cover GLOBAL SYNC ON (a) COVER (" + cover + "), INDEX idx_plain GLOBAL SYNC ON (a)";
    }
    return ddl + ") WITH (PARTITION_AT_KEYS = (2));";
}

NKikimrConfig::TAppConfig ReferenceConfig(bool streamWrite, bool compileTimeDefaults = true) {
    auto config = GeneratedColumnsAppConfig();
    config.MutableTableServiceConfig()->SetEnableStreamWrite(streamWrite);
    config.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(compileTimeDefaults);
    return config;
}

void CheckReferenceResult(const TResultSet& result, const TVector<std::string>& names, const TString& expected) {
    const auto& columns = result.GetColumnsMeta();
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), names.size());
    for (size_t i = 0; i < names.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(columns[i].Name, names[i]);
        TTypeParser type(columns[i].Type);
        const bool optional = names[i] == "a" || names[i] == "b";
        UNIT_ASSERT_VALUES_EQUAL(type.GetKind(),
            optional ? TTypeParser::ETypeKind::Optional : TTypeParser::ETypeKind::Primitive);
        if (optional) {
            type.OpenOptional();
        }
        UNIT_ASSERT_VALUES_EQUAL(type.GetPrimitive(), EPrimitiveType::Int32);
    }
    CompareYson(expected, FormatResultSetYson(result));
}

class TReferenceFixture : public TTestFixture {
public:
    const TReferenceCase& Schema;

    TReferenceFixture(const TReferenceCase& schema, bool streamWrite, bool compileTimeDefaults = true)
        : TTestFixture(ReferenceDDL(schema) + R"(
            CREATE TABLE Source (k Int32 NOT NULL, a Int32, b Int32, PRIMARY KEY(k));
        )", "INSERT INTO Source (k, a, b) VALUES (1, 10, 3), (3, 20, 4);", ReferenceConfig(streamWrite, compileTimeDefaults))
        , Schema(schema)
    {}

    TString SeedProjection(const TVector<std::string>& names) const {
        std::map<std::string, std::string> values = {{"k", "1"}, {"a", "[10]"}, {"b", "[3]"}};
        for (const auto& column : Schema.Columns) {
            values[column.Name] = column.SeedValue;
        }
        TStringBuilder result;
        result << "[[";
        for (size_t i = 0; i < names.size(); ++i) {
            if (i) {
                result << ";";
            }
            result << values.at(names[i]);
        }
        result << "]]";
        return result;
    }

    void CheckProjection(const std::string& query, const TVector<std::string>& names, const TString& expected) {
        auto result = QuerySession().ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), Schema.Name << ": " << query << "\n" << result.GetIssues().ToString());
        CheckReferenceResult(result.GetResultSet(0), names, expected);
    }

    void Seed() {
        Exec("INSERT INTO TestTable (k, a, b) SELECT k, a, b FROM Source;");
    }

    TString Logical(const TString& ordinary, const TString& constant) const {
        return Schema.Constant ? constant : ordinary;
    }

    auto SourceWrite(const std::string& verb, unsigned source, bool reverseInput, const std::string& returning) {
        const bool erase = verb == "DELETE";
        const bool update = verb == "UPDATE";
        const std::string columns = erase ? "k" : reverseInput ? "b, k, a" : "k, a, b";
        std::string query = erase ? "DELETE FROM TestTable ON " : update ? "UPDATE TestTable ON "
            : verb + " INTO TestTable (" + columns + ") ";
        if (source == 0) {
            if (erase || update) {
                query += "(" + columns + ") ";
            }
            query += "VALUES " + std::string(erase ? "(1)" : reverseInput ? "(3, 1, 10)" : "(1, 10, 3)");
        } else {
            query += "SELECT " + columns + " FROM " + std::string(source == 1 ? "Source" : "AS_TABLE($rows)") + " WHERE k = 1";
        }
        query += " RETURNING " + returning + ";";
        if (source != 2) {
            return QuerySession().ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        }
        auto params = TParamsBuilder().AddParam("$rows").BeginList()
            .AddListItem().BeginStruct()
                .AddMember("b").Int32(3).AddMember("k").Int32(1).AddMember("a").Int32(10)
            .EndStruct()
            .EndList().Build().Build();
        query = "DECLARE $rows AS List<Struct<b:Int32,k:Int32,a:Int32>>;" + query;
        return QuerySession().ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(GeneratedReferenceMatrix) {
    Y_UNIT_TEST_TWIN(WritesAndReturning, StreamWrite) {
        for (const auto& schema : ReferenceCases()) {
            TReferenceFixture fixture(schema, StreamWrite);
            const TVector<std::string> explicitNames = {"g2", "k", "g1", "a", "b"};
            for (unsigned source = 0; source < 3; ++source) {
                for (const std::string verb : {"INSERT", "INSERT OR ABORT", "INSERT OR REVERT", "UPSERT", "REPLACE", "UPDATE", "DELETE"}) {
                    for (bool star : {false, true}) {
                        fixture.Exec("DELETE FROM TestTable;");
                        if (verb == "UPDATE" || verb == "DELETE") {
                            fixture.Exec(verb == "UPDATE" ? "INSERT INTO TestTable (k, a, b) VALUES (1, 9, 3);"
                                : "INSERT INTO TestTable (k, a, b) VALUES (1, 10, 3);");
                        }
                        const auto& names = star ? schema.StarNames : explicitNames;
                        auto result = fixture.SourceWrite(verb, source, source % 2, star ? "*" : JoinNames(names));
                        UNIT_ASSERT_C(result.IsSuccess(), schema.Name << ": " << verb << ": " << result.GetIssues().ToString());
                        CheckReferenceResult(result.GetResultSet(0), names, fixture.SeedProjection(names));
                        if (verb == "DELETE") {
                            fixture.Check("SELECT k FROM TestTable;", "[]");
                        } else {
                            fixture.CheckProjection("SELECT " + JoinNames(names) + " FROM TestTable;", names,
                                fixture.SeedProjection(names));
                        }
                    }
                }
            }
            fixture.Exec("DELETE FROM TestTable;");
            fixture.Seed();
            fixture.CheckProjection("UPDATE TestTable SET a = 11 WHERE k = 1 RETURNING g2, g1;",
                {"g2", "g1"}, fixture.Logical("[[28;14]]", "[[26;13]]"));
            fixture.CheckProjection("DELETE FROM TestTable WHERE k = 1 RETURNING g2, g1;",
                {"g2", "g1"}, fixture.Logical("[[28;14]]", "[[26;13]]"));
            fixture.Exec("DELETE FROM TestTable;");
            fixture.Exec("INSERT INTO TestTable (k, a, b) VALUES (1, 10, 3);");
            auto conflict = fixture.QuerySession().ExecuteQuery(
                "INSERT OR ABORT INTO TestTable (k, a, b) VALUES (1, 99, 3), (3, 20, 4);",
                TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(conflict.GetStatus(), EStatus::PRECONDITION_FAILED, conflict.GetIssues().ToString());
            fixture.Check("SELECT k, g1, g2 FROM TestTable;", "[[1;13;26]]");
            fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 7), (3, 7);");
            fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;",
                fixture.Logical("[[1;10;20];[3;10;20]]", "[[1;13;26];[3;13;26]]"));
            fixture.Exec("REPLACE INTO TestTable (k, a, b) VALUES (1, NULL, NULL);");
            fixture.Check("SELECT k, a, b, g1, g2 FROM TestTable WHERE k = 1;",
                fixture.Logical("[[1;#;#;0;0]]", "[[1;#;#;13;26]]"));
        }
    }

    Y_UNIT_TEST_TWIN(ReadPipelines, StreamWrite) {
        for (const auto& schema : ReferenceCases()) {
            TReferenceFixture fixture(schema, StreamWrite);
            fixture.Seed();
            fixture.CheckProjection("SELECT g2, k, g1 FROM TestTable WHERE k = 1;", {"g2", "k", "g1"}, "[[26;1;13]]");
            const auto rows = fixture.Logical("[[1;13;26];[3;24;48]]", "[[1;13;26];[3;13;26]]");
            fixture.CheckProjection("SELECT k, g1, g2 FROM TestTable WHERE k >= 1 AND k <= 3 ORDER BY k;",
                {"k", "g1", "g2"}, rows);
            for (const std::string index : {"idx_cover", "idx_plain"}) {
                fixture.CheckProjection("SELECT k, g1, g2 FROM TestTable VIEW " + index + " WHERE a >= 10 ORDER BY k;",
                    {"k", "g1", "g2"}, rows);
            }
            fixture.Check("SELECT t.k, t.g2 FROM TestTable AS t JOIN Source AS s ON t.k = s.k ORDER BY t.k;",
                fixture.Logical("[[1;26];[3;48]]", "[[1;26];[3;26]]"));
            fixture.Check("SELECT g2 FROM (SELECT k, g1, g2 FROM TestTable WHERE g1 > 0) WHERE k = 1;", "[[26]]");
            fixture.Check("SELECT b, SUM(g2) FROM TestTable GROUP BY b ORDER BY b;",
                fixture.Logical("[[[3];26];[[4];48]]", "[[[3];26];[[4];26]]"));
            fixture.Check("SELECT k, g2, SUM(g1) OVER () FROM TestTable ORDER BY k;",
                fixture.Logical("[[1;26;37];[3;48;37]]", "[[1;26;26];[3;26;26]]"));
            auto stream = fixture.Runner().GetQueryClient().StreamExecuteQuery(
                "SELECT k, g1, g2 FROM TestTable ORDER BY k;", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
            CompareYson(rows, StreamResultToYson(stream));
            auto scan = fixture.Runner().GetTableClient().StreamExecuteScanQuery(
                "SELECT k, g1, g2 FROM TestTable ORDER BY k;").ExtractValueSync();
            UNIT_ASSERT_C(scan.IsSuccess(), scan.GetIssues().ToString());
            CompareYson(rows, CollectStreamResult(scan).ResultSetYson);
        }
    }

    Y_UNIT_TEST_TWIN(IndependentOrderPermutations, StreamWrite) {
        for (const auto& schema : ReferenceCases()) {
            TReferenceFixture fixture(schema, StreamWrite);
            for (unsigned ddl = 0; ddl < 4; ++ddl) {
                fixture.Exec("DROP TABLE TestTable;");
                fixture.Exec(ReferenceDDL(schema, ddl & 1, ddl & 2));
                for (bool inputReverse : {false, true}) {
                    for (bool selectReverse : {false, true}) {
                        for (bool returningReverse : {false, true}) {
                            fixture.Exec("DELETE FROM TestTable;");
                            const TVector<std::string> selected = selectReverse
                                ? TVector<std::string>{"g2", "a", "k", "b", "g1"}
                                : TVector<std::string>{"k", "b", "g1", "a", "g2"};
                            const TVector<std::string> returned = returningReverse
                                ? TVector<std::string>{"b", "g2", "a", "g1", "k"}
                                : TVector<std::string>{"g1", "k", "g2", "b", "a"};
                            auto result = fixture.SourceWrite("UPSERT", ddl % 3, inputReverse, JoinNames(returned));
                            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                            CheckReferenceResult(result.GetResultSet(0), returned, fixture.SeedProjection(returned));
                            fixture.CheckProjection("SELECT " + JoinNames(selected) + " FROM TestTable;", selected,
                                fixture.SeedProjection(selected));
                        }
                    }
                }
                fixture.CheckProjection("SELECT * FROM TestTable;", schema.StarNames, fixture.SeedProjection(schema.StarNames));
                fixture.CheckProjection("UPDATE TestTable SET a = 10 WHERE k = 1 RETURNING *;",
                    schema.StarNames, fixture.SeedProjection(schema.StarNames));
            }
        }
    }

    Y_UNIT_TEST_TWIN(TransactionsAndRetries, StreamWrite) {
        for (const auto& schema : ReferenceCases()) {
            TReferenceFixture fixture(schema, StreamWrite);
            auto& session = fixture.QuerySession();
            auto insert = session.ExecuteQuery("INSERT INTO TestTable (k, a, b) VALUES (1, 10, 3) RETURNING g2, g1;",
                TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
            CompareYson("[[26;13]]", FormatResultSetYson(insert.GetResultSet(0)));
            auto tx = insert.GetTransaction();
            UNIT_ASSERT(tx && tx->IsActive());
            auto update = session.ExecuteQuery("UPSERT INTO TestTable (k, a) VALUES (1, 20), (3, 7);",
                TTxControl::Tx(*tx)).ExtractValueSync();
            UNIT_ASSERT_C(update.IsSuccess(), update.GetIssues().ToString());
            auto visible = session.ExecuteQuery("SELECT k, g1, g2 FROM TestTable VIEW idx_cover ORDER BY k;",
                TTxControl::Tx(*tx)).ExtractValueSync();
            UNIT_ASSERT_C(visible.IsSuccess(), visible.GetIssues().ToString());
            CompareYson(fixture.Logical("[[1;23;46];[3;10;20]]", "[[1;13;26];[3;13;26]]"),
                FormatResultSetYson(visible.GetResultSet(0)));
            UNIT_ASSERT_C(tx->Commit().ExtractValueSync().IsSuccess(), "interactive commit failed");

            auto changed = session.ExecuteQuery("UPDATE TestTable SET a = 30 WHERE k = 1 RETURNING g2, g1;",
                TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(changed.IsSuccess(), changed.GetIssues().ToString());
            CompareYson(fixture.Logical("[[66;33]]", "[[26;13]]"), FormatResultSetYson(changed.GetResultSet(0)));
            auto rollback = changed.GetTransaction();
            UNIT_ASSERT(rollback && rollback->IsActive());
            UNIT_ASSERT(rollback->Rollback().ExtractValueSync().IsSuccess());
            fixture.Check("SELECT g1, g2 FROM TestTable WHERE k = 1;", fixture.Logical("[[23;46]]", "[[13;26]]"));

            auto statements = session.ExecuteQuery(R"(
                UPSERT INTO TestTable (k, a, b) VALUES (1, 5, 6);
                SELECT g1, g2 FROM TestTable WHERE k = 1;
                UPDATE TestTable SET a = 7 WHERE k = 1;
                SELECT g1, g2 FROM TestTable WHERE k = 1;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(statements.IsSuccess(), statements.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(statements.GetResultSets().size(), 2u);
            CompareYson(fixture.Logical("[[11;22]]", "[[13;26]]"), FormatResultSetYson(statements.GetResultSet(0)));
            CompareYson("[[13;26]]", FormatResultSetYson(statements.GetResultSet(1)));

            auto locked = session.ExecuteQuery("SELECT g1, g2 FROM TestTable WHERE k = 1;",
                TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(locked.IsSuccess(), locked.GetIssues().ToString());
            auto staleTx = locked.GetTransaction();
            auto second = fixture.Runner().GetQueryClient().GetSession().ExtractValueSync().GetSession();
            auto competing = second.ExecuteQuery("UPDATE TestTable SET a = 8 WHERE k = 1;",
                TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(competing.IsSuccess(), competing.GetIssues().ToString());
            auto aborted = session.ExecuteQuery("UPDATE TestTable SET a = 9 WHERE k = 1;",
                TTxControl::Tx(*staleTx).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(aborted.GetStatus(), EStatus::ABORTED, aborted.GetIssues().ToString());
            fixture.Exec("UPDATE TestTable SET a = 9 WHERE k = 1;");
            fixture.Check("SELECT g1, g2 FROM TestTable WHERE k = 1;", fixture.Logical("[[15;30]]", "[[13;26]]"));

            auto tableSession = fixture.Runner().GetTableClient().CreateSession().ExtractValueSync().GetSession();
            for (bool inplace : {false, true}) {
                auto result = tableSession.ExecuteDataQuery(
                    std::string("PRAGMA kikimr.OptEnableInplaceUpdate = '") + (inplace ? "true" : "false")
                        + "'; UPDATE TestTable SET a = 9 WHERE k = 1;",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                fixture.Check("SELECT g1, g2 FROM TestTable WHERE k = 1;", fixture.Logical("[[15;30]]", "[[13;26]]"));
            }
        }
    }

    Y_UNIT_TEST_TWIN(DeclaredIntermediateResultTypes, StreamWrite) {
        TTestFixture fixture(R"(
            CREATE TABLE Types (
                k Int32 NOT NULL, a Uint16, n Int32, p PgText NOT NULL,
                mid Uint32 GENERATED ALWAYS AS (a) VIRTUAL,
                out Uint64 GENERATED ALWAYS AS (mid + 1u) STORED,
                nullable_mid Int32 GENERATED ALWAYS AS (n) VIRTUAL,
                required Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(nullable_mid, 5)) STORED,
                pg_mid PgText NOT NULL GENERATED ALWAYS AS (p) VIRTUAL,
                pg_out PgText NOT NULL GENERATED ALWAYS AS (pg_mid) STORED,
                PRIMARY KEY(k)
            );
        )", "", ReferenceConfig(StreamWrite));
        struct TExpectedColumn {
            std::string Name;
            EPrimitiveType Primitive = EPrimitiveType::Int32;
            bool Optional = false;
            bool Pg = false;
        };
        const TVector<TExpectedColumn> expected = {
            {"a", EPrimitiveType::Uint16, true}, {"k"}, {"mid", EPrimitiveType::Uint32, true},
            {"n", EPrimitiveType::Int32, true}, {"nullable_mid", EPrimitiveType::Int32, true},
            {"out", EPrimitiveType::Uint64, true}, {"p", EPrimitiveType::Int32, false, true},
            {"pg_mid", EPrimitiveType::Int32, false, true}, {"pg_out", EPrimitiveType::Int32, false, true},
            {"required"},
        };
        auto check = [&](const TResultSet& result, const TString& values) {
            UNIT_ASSERT_VALUES_EQUAL(result.GetColumnsMeta().size(), expected.size());
            for (size_t i = 0; i < expected.size(); ++i) {
                const auto& column = result.GetColumnsMeta()[i];
                const auto& want = expected[i];
                UNIT_ASSERT_VALUES_EQUAL(column.Name, want.Name);
                TTypeParser type(column.Type);
                const auto kind = want.Pg ? TTypeParser::ETypeKind::Pg
                    : want.Optional ? TTypeParser::ETypeKind::Optional : TTypeParser::ETypeKind::Primitive;
                UNIT_ASSERT_VALUES_EQUAL(type.GetKind(), kind);
                if (want.Pg) {
                    UNIT_ASSERT_VALUES_EQUAL(type.GetPg().TypeName, "pgtext");
                } else {
                    if (want.Optional) {
                        type.OpenOptional();
                    }
                    UNIT_ASSERT_VALUES_EQUAL(type.GetPrimitive(), want.Primitive);
                }
            }
            CompareYson(values, FormatResultSetYson(result));
        };
        auto inserted = fixture.QuerySession().ExecuteQuery(
            "INSERT INTO Types (k, a, p) VALUES (1, 7us, 'hello'pt) RETURNING *;",
            TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
        const TString first = R"([[[7u];1;[7u];#;#;[8u];"hello";"hello";"hello";5]])";
        check(inserted.GetResultSet(0), first);
        auto selected = fixture.QuerySession().ExecuteQuery("SELECT * FROM Types;", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(selected.IsSuccess(), selected.GetIssues().ToString());
        check(selected.GetResultSet(0), first);
        auto changed = fixture.QuerySession().ExecuteQuery(
            "UPDATE Types SET a = NULL, n = 9 WHERE k = 1 RETURNING *;", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(changed.IsSuccess(), changed.GetIssues().ToString());
        check(changed.GetResultSet(0), R"([[#;1;#;[9];[9];#;"hello";"hello";"hello";9]])");
        auto deleted = fixture.QuerySession().ExecuteQuery(
            "DELETE FROM Types WHERE k = 1 RETURNING *;", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(deleted.IsSuccess(), deleted.GetIssues().ToString());
        check(deleted.GetResultSet(0), R"([[#;1;#;[9];[9];#;"hello";"hello";"hello";9]])");
    }


    Y_UNIT_TEST_TWIN(PartialPgUpdatesPreserveIndependentBranch, GeneratedReferences) {
        for (bool streamWrite : {false, true}) {
            const std::string expression = GeneratedReferences ? "pg_mid" : "p";
            TTestFixture fixture("CREATE TABLE PgUpdates (k Int32 NOT NULL, a Int32, p PgText NOT NULL, "
                "pg_mid PgText NOT NULL GENERATED ALWAYS AS (p) VIRTUAL, "
                "pg_out PgText NOT NULL GENERATED ALWAYS AS (" + expression + ") STORED, PRIMARY KEY(k), "
                "INDEX idx GLOBAL SYNC ON (a) COVER(pg_out));", "", ReferenceConfig(streamWrite));
            fixture.Exec("INSERT INTO PgUpdates (k, a, p) VALUES (1, 1, 'hello'pt);");
            fixture.CheckReturning("UPDATE PgUpdates SET a = 2 WHERE k = 1 RETURNING a, p, pg_mid, pg_out;",
                "SELECT a, p, pg_mid, pg_out FROM PgUpdates;", R"([[[2];"hello";"hello";"hello"]])");
            fixture.CheckReturning("UPDATE PgUpdates ON (k, a) VALUES (1, 3) RETURNING a, p, pg_mid, pg_out;",
                "SELECT a, p, pg_mid, pg_out FROM PgUpdates;", R"([[[3];"hello";"hello";"hello"]])");
            fixture.Check("SELECT k, pg_out FROM PgUpdates VIEW idx WHERE a = 3;", R"([[1;"hello"]])");
            fixture.Exec("UPDATE PgUpdates SET p = 'bye'pt WHERE k = 1;");
            auto rejected = fixture.QuerySession().ExecuteQuery("UPDATE PgUpdates SET p = NULL WHERE k = 1;",
                TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!rejected.IsSuccess(), "NOT NULL validation was skipped for a written PG column");
            fixture.Check("SELECT k, p, pg_mid, pg_out FROM PgUpdates;", R"([[1;"bye";"bye";"bye"]])");
            fixture.Check("SELECT k, pg_out FROM PgUpdates VIEW idx WHERE a = 3;", R"([[1;"bye"]])");
        }
    }

    Y_UNIT_TEST(RestartCopyAndPreparedInvalidation) {
        for (const auto& schema : ReferenceCases()) {
            TReferenceFixture fixture(schema, true);
            fixture.Seed();
            const auto original = fixture.ShowCreateTable("/Root/TestTable");
            fixture.RestartSchemeShard("/Root/TestTable");
            fixture.Check("SELECT g1, g2 FROM TestTable WHERE k = 1;", "[[13;26]]");
            UNIT_ASSERT_VALUES_EQUAL(original, fixture.ShowCreateTable("/Root/TestTable"));
            fixture.Exec("UPDATE TestTable SET a = 20 WHERE k = 1;");
            fixture.Check("SELECT g1, g2 FROM TestTable WHERE k = 1;", fixture.Logical("[[23;46]]", "[[13;26]]"));
            auto tableSession = fixture.Runner().GetTableClient().CreateSession().ExtractValueSync().GetSession();
            auto copy = tableSession.CopyTable("/Root/TestTable", "/Root/Copy").ExtractValueSync();
            UNIT_ASSERT_C(copy.IsSuccess(), schema.Name << ": " << copy.GetIssues().ToString());
            fixture.Exec("UPDATE Copy SET a = 30 WHERE k = 1;");
            fixture.Check("SELECT g1, g2 FROM Copy WHERE k = 1;", fixture.Logical("[[33;66]]", "[[13;26]]"));
            fixture.Check("SELECT g1, g2 FROM TestTable WHERE k = 1;", fixture.Logical("[[23;46]]", "[[13;26]]"));

            auto preparation = tableSession.PrepareDataQuery(
                "UPDATE Copy SET a = a + 1 WHERE k = 1;").ExtractValueSync();
            UNIT_ASSERT_C(preparation.IsSuccess(), preparation.GetIssues().ToString());
            auto prepared = preparation.GetQuery();
            auto execute = [&] {
                for (unsigned attempt = 0; attempt < 5; ++attempt) {
                    auto result = prepared.Execute(NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    if (result.IsSuccess()) {
                        return;
                    }
                    UNIT_ASSERT_C(result.GetStatus() == EStatus::ABORTED || result.GetStatus() == EStatus::UNAVAILABLE,
                        result.GetIssues().ToString());
                }
                UNIT_FAIL("prepared query did not recover from schema invalidation");
            };
            execute();
            fixture.Exec("ALTER TABLE Copy DROP INDEX idx_cover;");
            fixture.Exec("ALTER TABLE Copy DROP COLUMN g2;");
            execute();
            fixture.Check("SELECT a, g1 FROM Copy WHERE k = 1;", fixture.Logical("[[[32];35]]", "[[[32];13]]"));
            auto describe = tableSession.DescribeTable("/Root/Copy").ExtractValueSync();
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            for (const auto& column : describe.GetTableDescription().GetColumns()) {
                UNIT_ASSERT_C(column.Name != "g2", "dropped column survived in DescribeTable");
            }
        }
    }


    Y_UNIT_TEST_TWIN(EmptyAndDuplicateInputs, StreamWrite) {
        for (const auto& schema : ReferenceCases()) {
            TReferenceFixture fixture(schema, StreamWrite);
            fixture.Seed();
            const auto original = fixture.Logical("[[1;13;26];[3;24;48]]", "[[1;13;26];[3;13;26]]");
            auto empty = TParamsBuilder().AddParam("$rows").EmptyList(TTypeBuilder().BeginStruct()
                .AddMember("k").Primitive(EPrimitiveType::Int32)
                .AddMember("a").Primitive(EPrimitiveType::Int32)
                .AddMember("b").Primitive(EPrimitiveType::Int32)
                .EndStruct().Build()).Build().Build();
            for (const std::string verb : {"INSERT", "INSERT OR ABORT", "INSERT OR REVERT", "UPSERT", "REPLACE", "UPDATE", "DELETE"}) {
                for (bool parameterized : {false, true}) {
                    const std::string columns = verb == "DELETE" ? "k" : "k, a, b";
                    auto query = verb == "DELETE" ? std::string("DELETE FROM TestTable ON ")
                        : verb == "UPDATE" ? std::string("UPDATE TestTable ON ")
                        : verb + " INTO TestTable (" + columns + ") ";
                    query += "SELECT " + columns + " FROM " + (parameterized ? "AS_TABLE($rows)" : "Source WHERE k < 0")
                        + " RETURNING g2, k, g1;";
                    auto result = parameterized
                        ? fixture.QuerySession().ExecuteQuery("DECLARE $rows AS List<Struct<k:Int32,a:Int32,b:Int32>>;" + query,
                            TTxControl::NoTx(), empty).ExtractValueSync()
                        : fixture.QuerySession().ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), schema.Name << ": " << query << "\n" << result.GetIssues().ToString());
                    CheckReferenceResult(result.GetResultSet(0), {"g2", "k", "g1"}, "[]");
                    fixture.Check("SELECT k, g1, g2 FROM TestTable ORDER BY k;", original);
                    fixture.Check("SELECT k, g1, g2 FROM TestTable VIEW idx_cover ORDER BY k;", original);
                }
            }

            auto duplicate = TParamsBuilder().AddParam("$rows").BeginList()
                .AddListItem().BeginStruct().AddMember("k").Int32(1).AddMember("a").Int32(10).AddMember("b").Int32(3).EndStruct()
                .AddListItem().BeginStruct().AddMember("k").Int32(1).AddMember("a").Int32(20).AddMember("b").Int32(4).EndStruct()
                .EndList().Build().Build();
            for (bool parameterized : {false, true}) {
                auto execute = [&](const std::string& verb) {
                    auto query = verb == "UPDATE" ? std::string("UPDATE TestTable ON ") : verb + " INTO TestTable ";
                    query += parameterized ? "SELECT k, a, b FROM AS_TABLE($rows);" : "(k, a, b) VALUES (1, 10, 3), (1, 20, 4);";
                    return parameterized
                        ? fixture.QuerySession().ExecuteQuery("DECLARE $rows AS List<Struct<k:Int32,a:Int32,b:Int32>>;" + query,
                            TTxControl::NoTx(), duplicate).ExtractValueSync()
                        : fixture.QuerySession().ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
                };
                for (const std::string verb : {"INSERT", "INSERT OR ABORT", "INSERT OR REVERT"}) {
                    fixture.Exec("DELETE FROM TestTable;");
                    auto result = execute(verb);
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), verb == "INSERT OR REVERT" ? EStatus::SUCCESS : EStatus::PRECONDITION_FAILED,
                        result.GetIssues().ToString());
                    fixture.Check("SELECT k FROM TestTable;", "[]");
                    fixture.Check("SELECT k FROM TestTable VIEW idx_cover;", "[]");
                }
                for (const std::string verb : {"UPSERT", "REPLACE", "UPDATE"}) {
                    fixture.Exec("DELETE FROM TestTable;");
                    if (verb == "UPDATE") {
                        fixture.Exec("INSERT INTO TestTable (k, a, b) VALUES (1, 9, 3);");
                    }
                    auto result = execute(verb);
                    UNIT_ASSERT_C(result.IsSuccess(), schema.Name << ": " << verb << ": " << result.GetIssues().ToString());
                    const auto expected = fixture.Logical("[[1;24;48]]", "[[1;13;26]]");
                    fixture.Check("SELECT k, g1, g2 FROM TestTable;", expected);
                    fixture.Check("SELECT k, g1, g2 FROM TestTable VIEW idx_cover;", expected);
                }
                fixture.Exec("DELETE FROM TestTable ON (k) VALUES (1), (1);");
                fixture.Check("SELECT k FROM TestTable;", "[]");
            }
        }
    }

    Y_UNIT_TEST_TWIN(DefaultsAcrossReferenceSchemas, CompileTimeDefaults) {
        for (bool streamWrite : {false, true}) {
            for (const auto& schema : ReferenceCases()) {
                TReferenceFixture fixture(schema, streamWrite, CompileTimeDefaults);
                fixture.Exec("INSERT INTO TestTable (k, a, b) VALUES (1, 10, 7);");
                fixture.Exec("UPSERT INTO TestTable (k, a) VALUES (1, 20), (3, 7);");
                fixture.CheckProjection("SELECT k, g1, g2 FROM TestTable ORDER BY k;", {"k", "g1", "g2"},
                    fixture.Logical("[[1;27;54];[3;10;20]]", "[[1;13;26];[3;13;26]]"));
                fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
                fixture.CheckProjection("SELECT k, a, b, g1, g2 FROM TestTable WHERE k = 1;", {"k", "a", "b", "g1", "g2"},
                    fixture.Logical("[[1;#;[3];3;6]]", "[[1;#;[3];13;26]]"));
            }
        }
    }

    Y_UNIT_TEST(UpdatePragmaModesPreserveGeneratedValues) {
        for (const auto& schema : ReferenceCases()) {
            NKikimrKqp::TKqpSetting unsafeCommit;
            unsafeCommit.SetName("_KqpAllowUnsafeCommit");
            unsafeCommit.SetValue("true");
            TKikimrRunner runner(TKikimrSettings(ReferenceConfig(false)).SetWithSampleTables(false).SetKqpSettings({unsafeCommit}));
            auto session = runner.GetTableClient().CreateSession().ExtractValueSync().GetSession();
            auto created = session.ExecuteSchemeQuery(ReferenceDDL(schema, false, false, false)).ExtractValueSync();
            UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
            auto seed = session.ExecuteDataQuery("INSERT INTO TestTable (k, a, b) VALUES (1, 10, 3), (3, 20, 4);",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(seed.IsSuccess(), seed.GetIssues().ToString());
            for (bool inplace : {false, true}) {
                const auto query = std::string("PRAGMA kikimr.OptEnableInplaceUpdate = '") + (inplace ? "true" : "false")
                    + "'; DECLARE $key AS Int32; UPDATE TestTable SET a = COALESCE(a, 0) + 1 WHERE k = $key;";
                auto explanation = session.ExplainDataQuery(query).ExtractValueSync();
                UNIT_ASSERT_C(explanation.IsSuccess(), explanation.GetIssues().ToString());
                // The compatibility pragma currently has no planner consumer.
                // Exercise both settings and validate the executed update instead.
                auto params = TParamsBuilder().AddParam("$key").Int32(1).Build().Build();
                NYdb::NTable::TExecDataQuerySettings settings;
                settings.CollectQueryStats(NYdb::NTable::ECollectQueryStatsMode::Basic);
                auto changed = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
                UNIT_ASSERT_C(changed.IsSuccess(), changed.GetIssues().ToString());
                UNIT_ASSERT(changed.GetStats());
                const auto& stats = NYdb::TProtoAccessor::GetProto(*changed.GetStats());
                ui64 reads = 0;
                ui64 writes = 0;
                for (const auto& phase : stats.query_phases()) {
                    for (const auto& access : phase.table_access()) {
                        UNIT_ASSERT_VALUES_EQUAL(access.name(), "/Root/TestTable");
                        reads += access.reads().rows();
                        writes += access.updates().rows();
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(reads, 1, stats.DebugString());
                UNIT_ASSERT_VALUES_EQUAL_C(writes, 1, stats.DebugString());
                auto selected = session.ExecuteDataQuery("SELECT k, g1, g2 FROM TestTable ORDER BY k;",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(selected.IsSuccess(), selected.GetIssues().ToString());
                CheckReferenceResult(selected.GetResultSet(0), {"k", "g1", "g2"}, schema.Constant
                    ? "[[1;13;26];[3;13;26]]" : inplace ? "[[1;15;30];[3;24;48]]" : "[[1;14;28];[3;24;48]]");
            }
        }
    }

    Y_UNIT_TEST(BackportedScalarSyntaxSurvivesCreateAndAlter) {
        auto config = ReferenceConfig(true);
        config.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        TTestFixture fixture(R"(
            CREATE TABLE Backported (
                k Int32 NOT NULL, a String NOT NULL,
                g String GENERATED ALWAYS AS (Udf(String::AsciiToUpper, a AS Depends)(a)) STORED,
                v String GENERATED ALWAYS AS (g) VIRTUAL,
                PRIMARY KEY(k)
            );
            CREATE TABLE Legacy (
                k Int32 NOT NULL, a Int32,
                g Bool GENERATED ALWAYS AS (a NOT NULL) STORED,
                PRIMARY KEY(k)
            );
        )", "", config);
        fixture.Exec("INSERT INTO Backported (k, a) VALUES (1, 'hello');");
        fixture.Exec("ALTER TABLE Backported ADD COLUMN unrelated Int32;");
        fixture.Check("SELECT k, g, v FROM Backported;", R"([[1;["HELLO"];["HELLO"]]])");
        fixture.Exec("INSERT INTO Legacy (k, a) VALUES (1, NULL), (2, 3);");
        fixture.Exec("ALTER TABLE Legacy ADD COLUMN unrelated Int32;");
        fixture.Check("SELECT k, g FROM Legacy ORDER BY k;", "[[1;[%false]];[2;[%true]]]");
    }

    Y_UNIT_TEST(CreateCleanupStress) {
        TTestFixture fixture("CREATE TABLE Source (k Int32 NOT NULL, PRIMARY KEY(k));");
        for (unsigned iteration = 0; iteration < 40; ++iteration) {
            fixture.Exec(R"(
                CREATE TABLE Cleanup (
                    k Int32 NOT NULL, a Int32,
                    s Uint64 NOT NULL GENERATED ALWAYS AS (r) STORED,
                    r Uint64 NOT NULL GENERATED ALWAYS AS (RandomNumber(k)) VIRTUAL,
                    z Uint64 NOT NULL GENERATED ALWAYS AS (s) STORED,
                    PRIMARY KEY(k)
                );
            )");
            fixture.Exec("INSERT INTO Cleanup (k, a) VALUES (1, 10);");
            fixture.Exec("UPDATE Cleanup SET a = 20 WHERE k = 1 RETURNING *;");
            fixture.Exec("DELETE FROM Cleanup ON SELECT k FROM Cleanup WHERE s = z RETURNING *;");
            fixture.Check("SELECT k FROM Cleanup;", "[]");
            fixture.Exec("DROP TABLE Cleanup;");
        }
    }
}

} // namespace NKikimr::NKqp
