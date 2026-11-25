#include <yql/essentials/public/fastcheck/linter.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

using namespace NYql;
using namespace NYql::NFastCheck;

namespace {

TChecksRequest MakeCheckRequest() {
    return TChecksRequest{
        .LangVer = GetMaxReleasedLangVersion()};
}

#ifdef YQL_FASTCHECK_TEST_NEW_FEATURES
void CheckStatementKindResult(const TChecksRequest& request, const TSet<EStatementKind>& expectedKinds) {
    bool allowed = true;
    for (const auto statementKind : expectedKinds) {
        allowed &= !request.AllowedStatementKinds || request.AllowedStatementKinds->contains(statementKind);
    }
    const auto result = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(result.Checks.size(), 2);
    for (const auto& check : result.Checks) {
        if (check.CheckName == "parser") {
            UNIT_ASSERT_C(check.Success, request.Program << check.Issues.ToString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(check.CheckName, "translator");
            UNIT_ASSERT_VALUES_EQUAL_C(check.Success, allowed, request.Program << check.Issues.ToString());
            if (!allowed) {
                UNIT_ASSERT_STRING_CONTAINS(check.Issues.ToString(), "disallowed kind");
            }
        }
    }
}

void CheckStatementKinds(const TString& program, const TSet<EStatementKind>& expectedKinds, ESyntax syntax = ESyntax::YQL) {
    auto request = MakeCheckRequest();
    request.Program = program;
    request.Syntax = syntax;
    request.ClusterMapping = {{"plato", "yt"}, {"db", "kikimr"}, {"", "kikimr"}};
    request.Filters = ParseChecks("parser,translator");
    CheckStatementKindResult(request, expectedKinds);
    request.AllowedStatementKinds.ConstructInPlace();
    CheckStatementKindResult(request, expectedKinds);
    for (const auto statementKind : {EStatementKind::InnerDML, EStatementKind::DML,
                                     EStatementKind::DDL, EStatementKind::TxControl}) {
        request.AllowedStatementKinds = TSet<EStatementKind>{statementKind};
        CheckStatementKindResult(request, expectedKinds);
    }
    request.AllowedStatementKinds = TSet<EStatementKind>{EStatementKind::InnerDML, EStatementKind::DML, EStatementKind::DDL, EStatementKind::TxControl};
    CheckStatementKindResult(request, expectedKinds);
}

#endif

} // namespace

namespace NDeduplicateIssues {

TVector<TString> CheckNames = {"lexer", "parser", "translator", "typecheck"};

struct TTestCase {
    ESyntax Syntax;
    TString Program;
    size_t FailedCheckIndex;

    TString ToString() const {
        return TStringBuilder() << "syntax: " << ::ToString(Syntax)
                                << ", failed check: " << CheckNames[FailedCheckIndex];
    }
};

} // namespace NDeduplicateIssues

Y_UNIT_TEST_SUITE(TLinterTests) {
#ifdef YQL_FASTCHECK_TEST_NEW_FEATURES
Y_UNIT_TEST(AllowedStatementKindsAlwaysAllowed) {
    for (const TString& program : {
             "SELECT 1;", "VALUES (1);", "SELECT * FROM plato.Input;",
             "PROCESS plato.Input;",
             "$reducer = ($rows) -> { RETURN $rows; }; REDUCE plato.Input ON key USING ALL $reducer(TableRows());",
             "$value = 1; $query = SELECT $value; SELECT * FROM $query;",
             "DEFINE ACTION $action() AS SELECT 1; END DEFINE; DO $action();",
             "DEFINE SUBQUERY $query() AS SELECT 1; END DEFINE; SELECT * FROM $query();",
             "DECLARE $value AS Int32; SELECT $value;",
             "USE plato; PRAGMA TablePathPrefix = 'prefix'; SELECT * FROM Input;"}) {
        CheckStatementKinds(program, {});
    }
}

Y_UNIT_TEST(AllowedStatementKindsDML) {
    for (const TString& program : {
             "INSERT INTO plato.Output SELECT 1 AS value;",
             "INSERT INTO plato.Output WITH TRUNCATE SELECT 1 AS value;",
             "UPSERT INTO db.Output SELECT 1 AS value;",
             "REPLACE INTO db.Output SELECT 1 AS value;",
             "UPDATE db.Output SET value = 1;",
             "DELETE FROM db.Output;",
             "TRUNCATE TABLE db.Output;"}) {
        CheckStatementKinds(program, {EStatementKind::DML});
        CheckStatementKinds("DEFINE ACTION $action() AS " + program + " END DEFINE;", {EStatementKind::DML});
    }
    CheckStatementKinds("$table = 'Output'; INSERT INTO plato.$table SELECT 1 AS value;", {EStatementKind::DML});
}

Y_UNIT_TEST(AllowedStatementKindsDDL) {
    for (const TString& program : {
             "CREATE TABLE plato.Output (value Int32);",
             "ALTER TABLE db.Output ADD COLUMN value Int32;",
             "DROP TABLE plato.Output;",
             "SHOW CREATE TABLE db.Output;",
             "CHECKSUM TABLE db.Output;"}) {
        CheckStatementKinds(program, {EStatementKind::DDL});
        CheckStatementKinds("DEFINE ACTION $action() AS " + program + " END DEFINE;", {EStatementKind::DDL});
    }
    CheckStatementKinds("$table = 'Output'; DROP TABLE plato.$table;", {EStatementKind::DDL});
}

Y_UNIT_TEST(AllowedStatementKindsInnerDML) {
    for (const TString& program : {
             "INSERT INTO plato.@Output SELECT 1 AS value;",
             "INSERT INTO plato.@Output WITH TRUNCATE SELECT 1 AS value;",
             "DROP TABLE plato.@Output;"}) {
        CheckStatementKinds(program, {EStatementKind::InnerDML});
        CheckStatementKinds("DEFINE ACTION $action() AS " + program + " END DEFINE;", {EStatementKind::InnerDML});
    }
    for (const TString& program : {
             "$table = 'Output'; INSERT INTO plato.@$table SELECT 1 AS value;",
             "$table = 'Output'; DROP TABLE plato.@$table;",
             "USE plato; MATERIALIZE Input INTO $result; SELECT * FROM $result;",
             "USE plato; DEFINE SUBQUERY $query() AS MATERIALIZE Input INTO $result; SELECT * FROM $result; END DEFINE; SELECT * FROM $query();"}) {
        CheckStatementKinds(program, {EStatementKind::InnerDML});
    }
}

Y_UNIT_TEST(AllowedStatementKindsTxControl) {
    for (const TString& program : {"COMMIT;", "ROLLBACK;"}) {
        CheckStatementKinds(program, {EStatementKind::TxControl});
        CheckStatementKinds("DEFINE ACTION $action() AS " + program + " END DEFINE;", {EStatementKind::TxControl});
    }
}

Y_UNIT_TEST(AllowedStatementKindsPostgres) {
    for (const TString& program : {"SELECT 1;", "VALUES (1);", "SHOW server_version;",
                                   "WITH cte AS (SELECT 1 AS value) SELECT * FROM cte;"}) {
        CheckStatementKinds(program, {}, ESyntax::PG);
    }
    for (const TString& program : {
             "INSERT INTO Output VALUES (1);", "UPDATE Output SET value = 1;", "DELETE FROM Output;"}) {
        CheckStatementKinds(program, {EStatementKind::DML}, ESyntax::PG);
    }
    for (const TString& program : {"CREATE TABLE Output (value int);", "DROP TABLE Output;"}) {
        CheckStatementKinds(program, {EStatementKind::DDL}, ESyntax::PG);
    }
    for (const TString& program : {"COMMIT;", "ROLLBACK;", "BEGIN;"}) {
        CheckStatementKinds(program, {EStatementKind::TxControl}, ESyntax::PG);
    }
}

#endif

Y_UNIT_TEST(ListChecksResult) {
    auto res = ListChecks();
    UNIT_ASSERT(!res.empty());
}

Y_UNIT_TEST(DummyLexerSExpr) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "((return world))";
    request.Syntax = ESyntax::SExpr;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "lexer"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "lexer");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(DummyLexerPg) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select 1::text";
    request.Syntax = ESyntax::PG;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "lexer"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "lexer");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodLexerYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "1";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "lexer"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "lexer");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadLexerYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "Я";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "lexer"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "lexer");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(DummyFormatSExpr) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "((return world))";
    request.Syntax = ESyntax::SExpr;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(DummyFormatPg) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select 1::text";
    request.Syntax = ESyntax::PG;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodFormatYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "SELECT\n    1\n;\n";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodFormatYqlWithWinEOL) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "SELECT\r\n    1\r\n;\r\n";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodFormatYqlWithWinEOLInComment) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "--\r\nSELECT\n    1\n;\n\nSELECT\n    2\n;\n";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(UnparsedFormatYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select1\n";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadFormatYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select 1";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(ContextForBadFormatYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "SELECT\n    'привет',1;";
    request.File = "myFile.sql";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 1);
    const auto& pos = res.Checks[0].Issues.back().Position;
    UNIT_ASSERT_VALUES_EQUAL(pos.Row, 2);
    UNIT_ASSERT_VALUES_EQUAL(pos.Column, 13);
}

Y_UNIT_TEST(BadFormatYqlHidden) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select\t1 ";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "format"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "format");
    UNIT_ASSERT(!res.Checks[0].Success);
    UNIT_ASSERT_NO_DIFF(res.Checks[0].Issues.ToString(),
                        "<main>:1:0: Warning: Format mismatch, expected:\n"
                        "SELECT↵\n"
                        "••••1↵\n"
                        ";↵\n\n"
                        "but got:\n"
                        "select→1•\n"
                        ", code: 3\n");
}

Y_UNIT_TEST(GoodParserSExpr) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "((return world))";
    request.Syntax = ESyntax::SExpr;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "parser"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "parser");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadParserSExpr) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = ")";
    request.Syntax = ESyntax::SExpr;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "parser"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "parser");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(GoodParserPg) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select 1::text";
    request.Syntax = ESyntax::PG;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "parser"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "parser");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadParserPg) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "sel";
    request.Syntax = ESyntax::PG;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "parser"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "parser");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(DummyParserPg) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select 1::text";
    request.Syntax = ESyntax::PG;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "parser"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "parser");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodParserYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "SELECT 1";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "parser"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "parser");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadParserYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "1";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "parser"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "parser");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(DummyTranslatorSExpr) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "((return world))";
    request.Syntax = ESyntax::SExpr;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodTranslatorPg) {
    TChecksRequest request = MakeCheckRequest();
    request.ClusterMapping["plato"] = TString(YtProviderName);
    request.Program = "select * from plato.\"Input\"";
    request.Syntax = ESyntax::PG;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadTranslatorPg) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select * from \"Input\"";
    request.Syntax = ESyntax::PG;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(GoodTranslatorYql) {
    TChecksRequest request = MakeCheckRequest();
    request.ClusterMapping["plato"] = TString(YtProviderName);
    request.Program = "use plato; select * from Input";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadTranslatorYql) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select ListLengggth([1,2,3])";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(AllowYqlExportsForLibrary) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "$a = 1; export $a";
    request.Mode = EMode::Library;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(AllowYqlExportsForDefault) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "$a = 1; export $a";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(DisallowYqlExportsForMain) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "$a = 1; export $a";
    request.Syntax = ESyntax::YQL;
    request.Mode = EMode::Main;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(DisallowYqlExportsForView) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "$a = 1; export $a";
    request.Syntax = ESyntax::YQL;
    request.Mode = EMode::View;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(GoodYqlView) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select 1";
    request.Syntax = ESyntax::YQL;
    request.Mode = EMode::View;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadYqlView) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select 1;select 2";
    request.Syntax = ESyntax::YQL;
    request.Mode = EMode::View;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(AllChecks) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "SELECT\n    1\n;\n";
    request.Syntax = ESyntax::YQL;
    auto res = RunChecks(request);
    TSet<TString> passedChecks;
    for (const auto& r : res.Checks) {
        Cerr << r.CheckName << "\n";
        UNIT_ASSERT(r.Success);
        Cerr << r.Issues.ToString();
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
        passedChecks.insert(r.CheckName);
    }

    UNIT_ASSERT_VALUES_EQUAL(passedChecks, ListChecks());
}

Y_UNIT_TEST(AllChecksByStar) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "SELECT\n    1\n;\n";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "*"});
    auto res = RunChecks(request);
    TSet<TString> passedChecks;
    for (const auto& r : res.Checks) {
        Cerr << r.CheckName << "\n";
        UNIT_ASSERT(r.Success);
        Cerr << r.Issues.ToString();
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
        passedChecks.insert(r.CheckName);
    }

    UNIT_ASSERT_VALUES_EQUAL(passedChecks, ListChecks());
}

Y_UNIT_TEST(DeduplicateIssuesWarnings) {
    using namespace NDeduplicateIssues;

    TChecksRequest request = MakeCheckRequest();
    request.Program = R"sql(
        SELECT * FROM (
            SELECT 1 AS a
            ORDER BY a
        );
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(res.Checks[0].Success);
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[1].CheckName, "typecheck");
    UNIT_ASSERT(res.Checks[1].Success);
    UNIT_ASSERT(res.Checks[1].Issues.Size() > 0);

    request.SuppressPrerequisiteIssues = true;
    res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(res.Checks[0].Success);
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[1].CheckName, "typecheck");
    UNIT_ASSERT(res.Checks[1].Success);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[1].Issues.Size(), 0);
}

Y_UNIT_TEST(DeduplicateIssuesMatrix) {
    using namespace NDeduplicateIssues;

    TVector<TTestCase> cases = {
        {.Syntax = ESyntax::YQL,
         .Program = "Я",
         .FailedCheckIndex = 0},
        {.Syntax = ESyntax::YQL,
         .Program = "1",
         .FailedCheckIndex = 1},
        {.Syntax = ESyntax::YQL,
         .Program = "select ListLengggth([1,2,3])",
         .FailedCheckIndex = 2},
        {.Syntax = ESyntax::YQL,
         .Program = "select ListCreate('foo')",
         .FailedCheckIndex = 3},

        {.Syntax = ESyntax::SExpr,
         .Program = "(",
         .FailedCheckIndex = 1},
        {.Syntax = ESyntax::SExpr,
         .Program = R"yql(((return (List (String '"foo")))))yql",
         .FailedCheckIndex = 3},

        {.Syntax = ESyntax::PG,
         .Program = "sel",
         .FailedCheckIndex = 1},
        {.Syntax = ESyntax::PG,
         .Program = "select * from \"Input\"",
         .FailedCheckIndex = 2},
        {.Syntax = ESyntax::PG,
         .Program = R"sql(
             SELECT (Count(x) / (
                 SELECT Count(y)
                 FROM (VALUES ('1')) AS t(y)
                 HAVING Count(x) = Count(y)
             ))
             FROM (VALUES (1)) AS t(x)
             GROUP BY x;
         )sql",
         .FailedCheckIndex = 3},
    };

    for (const TTestCase& test : cases) {
        TChecksRequest request = MakeCheckRequest();
        request.Program = test.Program;
        request.Syntax = test.Syntax;
        request.SuppressPrerequisiteIssues = true;
        request.Filters.ConstructInPlace();
        for (const auto& checkName : CheckNames) {
            request.Filters->push_back(TCheckFilter{.CheckNameGlob = checkName});
        }

        const auto res = RunChecks(request);
        UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), std::size(CheckNames));
        for (size_t i = 0; i < std::size(CheckNames); ++i) {
            const auto& check = res.Checks[i];

            UNIT_ASSERT_VALUES_EQUAL(check.CheckName, CheckNames[i]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                check.Success,
                i < test.FailedCheckIndex,
                test.ToString() << ", target check: " << CheckNames[i]
                                << ", issues: " << check.Issues.ToString());

            UNIT_ASSERT_VALUES_EQUAL_C(
                check.Issues.Size(),
                i == test.FailedCheckIndex ? 1 : 0,
                test.ToString() << ", target check: " << CheckNames[i]
                                << ", issues: " << check.Issues.ToString());
        }
    }
}

Y_UNIT_TEST(NoChecksByStarWithSecondFilter) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "1";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.Include = false, .CheckNameGlob = "*"});
    request.Filters->push_back(TCheckFilter{.Include = true, .CheckNameGlob = "lexer"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "lexer");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
}

Y_UNIT_TEST(BadTranslatorYqlWithoutUseMany) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select * from Input";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(GoodTranslatorYqlWithoutUseSingle) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select * from Input";
    request.ClusterMode = EClusterMode::Single;
    request.ClusterSystem = YtProviderName;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodTranslatorYqlWithoutUseUnknown) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select * from Input";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(BadTranslatorYqlAnotherClusterMany) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select * from foo.Input";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(GoodTranslatorYqlAnotherClusterMany) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "select * from foo.Input";
    request.ClusterSystem = YtProviderName;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodTranslatorYqlUnknownSystemUpdate) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "update foo set value = 1";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodTranslatorYqlUnknownSystemDelete) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "delete from foo where value = 1";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(GoodTranslatorYqlUnknownSystemReplaceInto) {
    TChecksRequest request = MakeCheckRequest();
    request.Program = "replace into foo select 1";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(TooHighLangVersion) {
    TChecksRequest request;
    request.LangVer = GetMaxLangVersion();
    request.ClusterMapping["plato"] = TString(YtProviderName);
    request.Program = "use plato; select * from Input";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    Cerr << res.Checks[0].Issues.ToString();
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
}

Y_UNIT_TEST(MinLangVerInLinter) {
    TChecksRequest request;
    request.LangVer = NYql::MakeLangVersion(2025, 3);
    request.ClusterMapping["plato"] = TString(YtProviderName);
    request.Program = "use plato; select RandomSample(1, key) from Input";
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});
    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    UNIT_ASSERT(res.Checks[0].Issues.Size() > 0);
    const auto& issuesStr = res.Checks[0].Issues.ToString();
    UNIT_ASSERT_STRING_CONTAINS(issuesStr, "RandomSample");
    UNIT_ASSERT_STRING_CONTAINS(issuesStr, "2025.04");
}

Y_UNIT_TEST(UsedFlags) {
    TChecksRequest request;
    request.Program = R"sql(
            $input = AsList(
                AsStruct(1 AS key, 1001 AS subkey, 'AAA' AS value),
            );

            SELECT
                count(DISTINCT i1.key) OVER (
                    PARTITION BY
                        i1.subkey
                ) AS cnt,
            FROM
                AS_TABLE($input) AS i1
        )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(), 0);
}

Y_UNIT_TEST(UdfFilter) {
    TChecksRequest request;
    request.Program = R"sql(
        SELECT Math::FOO(1)
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "translator"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "translator");
    UNIT_ASSERT(!res.Checks[0].Success);
    auto err = res.Checks[0].Issues.ToString();
    UNIT_ASSERT_C(err.find("Unknown function: Math::FOO") != TString::npos, err);
}

Y_UNIT_TEST(TypeCheckResourceLimitIsWarning) {
    TChecksRequest request;
    request.Program = R"sql(
        $o = ($o) -> { RETURN $o($o); };
        SELECT $o($o);
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});
    request.LimitStrictnessFactor = 10;

    const auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());

    bool hasResourceLimitWarning = false;
    for (const auto& topIssue : res.Checks[0].Issues) {
        WalkThroughIssues(topIssue, /*leafOnly=*/false, [&](const TIssue& issue, ui16) {
            UNIT_ASSERT_C(
                issue.GetSeverity() > TSeverityIds::S_ERROR,
                res.Checks[0].Issues.ToString());
            hasResourceLimitWarning |= issue.GetMessage() ==
                                       "Partial type annotation: resource limit exceeded, type inference precision is lowered";
        });
    }
    UNIT_ASSERT(hasResourceLimitWarning);
}

Y_UNIT_TEST(TypeCheckBasicOk) {
    TChecksRequest request;
    request.Program = R"sql(
        SELECT 1
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
}

Y_UNIT_TEST(TypeCheckSeqModeEvaluateIf) {
    TChecksRequest request;
    request.Program = R"sql(
        PRAGMA SeqMode;

        EVALUATE IF TRUE DO BEGIN
            SELECT
                1
            ;
        END DO;
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    const auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
}

Y_UNIT_TEST(TypeCheckBasicFail) {
    TChecksRequest request;
    request.Program = R"sql(
        SELECT ListCreate('foo')
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT(!res.Checks[0].Success);
    auto err = res.Checks[0].Issues.ToString();
    UNIT_ASSERT_C(err.find("2:27: Error: Expected type, but got: String") != TString::npos, err);
}

Y_UNIT_TEST(TypeCheckSelectColumn) {
    TChecksRequest request;
    request.Program = R"sql(
        use plato;
        SELECT x FROM Input;
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
}

Y_UNIT_TEST(TypeCheckSqlInWithUniversalLookup) {
    TChecksRequest request;
    request.Program = R"sql(
        USE hahn;

        $xs = AsList("1", "2", "3", "4", "5");

        $p = ($a) -> {
            RETURN IF($a == "", CAST(NULL AS String), $a) IN $xs;
        };

        INSERT INTO @x (a) VALUES ('');

        COMMIT;

        SELECT
            EnsureType($p(a), Bool?),
        FROM @x;
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
}

Y_UNIT_TEST(TypeCheckInsert) {
    TChecksRequest request;
    request.Program = R"sql(
        use plato;
        INSERT INTO Output
        SELECT x FROM Input;
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
}

Y_UNIT_TEST(TypeCheckColumnExpr) {
    TChecksRequest request;
    request.Program = R"sql(
        use plato;
        SELECT x + 1 FROM Input;
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
}

Y_UNIT_TEST(TypeCheckUdfStrictCasePragma) {
    TChecksRequest request;
    request.Program = R"sql(
        pragma config.flags("UdfStrictCase");
        SELECT 1
    )sql";
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
}

} // Y_UNIT_TEST_SUITE(TLinterTests)
