#include "fastcheck.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
using namespace NYql::NFastCheck;

Y_UNIT_TEST_SUITE(TFastCheckTests) {
    Y_UNIT_TEST(ParsePureYqlGood) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = false;
        options.ParseOnly = true;
        TIssues errors;
        UNIT_ASSERT(CheckProgram("(return world)", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(0, errors.Size());
    }

    Y_UNIT_TEST(ParsePureYqlBad) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = false;
        options.ParseOnly = true;
        TIssues errors;
        UNIT_ASSERT(CheckProgram("(return world1)", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(0, errors.Size());
    }

    Y_UNIT_TEST(ParsePureSqlGood) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.ParseOnly = true;
        TIssues errors;
        UNIT_ASSERT(CheckProgram("select 1", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(0, errors.Size());
    }

    Y_UNIT_TEST(ParsePureSqlBad) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.ParseOnly = true;
        TIssues errors;
        UNIT_ASSERT(!CheckProgram("select1", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(1, errors.Size());
    }

    Y_UNIT_TEST(CompilePureYqlBad) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = false;
        options.ParseOnly = false;
        TIssues errors;
        UNIT_ASSERT(!CheckProgram("(return world1)", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(1, errors.Size());
    }

    Y_UNIT_TEST(CompileTableSqlGood) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.ParseOnly = false;
        options.ClusterMapping["plato"] = YtProviderName;
        TIssues errors;
        UNIT_ASSERT(CheckProgram("select key,count(*) from plato.Input group by key", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(0, errors.Size());
    }

    Y_UNIT_TEST(CompileTableSqlBad) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.ParseOnly = false;
        TIssues errors;
        UNIT_ASSERT(!CheckProgram("select key,count(*) from plato.Input", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(1, errors.Size());
    }

    Y_UNIT_TEST(CompileLibrary) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.IsLibrary = true;
        TIssues errors;
        UNIT_ASSERT(CheckProgram("$x = 1; export $x", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(0, errors.Size());
    }

    Y_UNIT_TEST(CompileSqlWithLibsGood) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.ParseOnly = false;
        options.SqlLibs["foo.sql"] = "$x = 1; export $x;";
        TIssues errors;
        UNIT_ASSERT(CheckProgram("pragma library('foo.sql');import foo symbols $x; select $x", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(0, errors.Size());
    }

    Y_UNIT_TEST(ParseSqlWithBadLib) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.ParseOnly = true;
        options.SqlLibs["foo.sql"] = "$x = 1; zexport $x;";
        TIssues errors;
        UNIT_ASSERT(!CheckProgram("pragma library('foo.sql');import foo symbols $x; select $x", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(1, errors.Size());
    }

    Y_UNIT_TEST(CompileSqlWithUnresolvedLib) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.ParseOnly = false;
        options.SqlLibs["foo.sql"] = "$x = 1; export $x;";
        TIssues errors;
        UNIT_ASSERT(!CheckProgram("pragma library('foo.sql');import foo symbols $y; select $y", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(1, errors.Size());
    }

    Y_UNIT_TEST(ParseSqlWithUnresolvedLib) {
        TOptions options;
        options.LangVer = GetMaxReleasedLangVersion();
        options.IsSql = true;
        options.ParseOnly = true;
        options.SqlLibs["foo.sql"] = "$x = 1; export $x;";
        TIssues errors;
        UNIT_ASSERT(CheckProgram("pragma library('foo.sql');import foo symbols $y; select $y", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(0, errors.Size());
    }

    Y_UNIT_TEST(TooHighLangVer) {
        TOptions options;
        options.LangVer = GetMaxLangVersion();
        options.IsSql = false;
        options.ParseOnly = true;
        TIssues errors;
        UNIT_ASSERT(!CheckProgram("(return world)", options, errors));
        UNIT_ASSERT_VALUES_EQUAL(1, errors.Size());
    }

    Y_UNIT_TEST(UsedFlags) {
        TString program = R"sql(
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

        TOptions options;
        options.IsSql = true;

        TIssues errors;
        UNIT_ASSERT_C(CheckProgram(program, options, errors), errors.ToOneLineString());
    }

}
