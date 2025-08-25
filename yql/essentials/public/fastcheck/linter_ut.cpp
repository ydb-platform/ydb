#include "linter.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
using namespace NYql::NFastCheck;

namespace {

TChecksRequest MakeCheckRequest() {
    return TChecksRequest{
        .LangVer = GetMaxReleasedLangVersion()
    };
}

}

Y_UNIT_TEST_SUITE(TLinterTests) {
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
            "SELECT\n"
            "••••1\n"
            ";\n\n"
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
            UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
            UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
            passedChecks.insert(r.CheckName);
        }

        UNIT_ASSERT_VALUES_EQUAL(passedChecks, ListChecks());
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
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
        UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].Issues.Size(),0);
    }

}
