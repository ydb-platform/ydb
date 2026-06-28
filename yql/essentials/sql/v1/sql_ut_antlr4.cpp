#include "sql_ut_antlr4.h"
#include "sql_translation.h"
#include "format/sql_format.h"
#include "lexer/lexer.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/langver/feature.gen.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <util/generic/map.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>

#include <format>

using namespace NSQLTranslation;

namespace {

TParsedTokenList Tokenize(const TString& query) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();

    auto lexer = NSQLTranslationV1::MakeLexer(lexers, /*ansi=*/false);
    TParsedTokenList tokens;
    NYql::TIssues issues;
    UNIT_ASSERT_C(Tokenize(*lexer, query, "Query", tokens, issues, SQL_MAX_PARSER_ERRORS),
                  issues.ToString());

    return tokens;
}

} // namespace

#define ANTLR_VER 4
#include "sql_ut_common.h"

Y_UNIT_TEST_SUITE(QuerySplit) {

TVector<TString> Statements(const TString& query) {
    google::protobuf::Arena Arena;

    NSQLTranslation::TTranslationSettings settings;
    settings.AnsiLexer = false;
    settings.Arena = &Arena;

    TVector<TString> statements;
    NYql::TIssues issues;

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();

    UNIT_ASSERT(NSQLTranslationV1::SplitQueryToStatements(lexers, parsers, query, statements, issues, settings));

    return statements;
}

Y_UNIT_TEST(Simple) {
    TString query = R"(
        ;
        -- Comment 1
        SELECT * From Input; -- Comment 2
        -- Comment 3
        $a = "a";

        -- Comment 9
        ;

        -- Comment 10

        -- Comment 8

        $b = ($x) -> {
        -- comment 4
        return /* Comment 5 */ $x;
        -- Comment 6
        };

        // Comment 7



        )";

    auto statements = Statements(query);

    UNIT_ASSERT_VALUES_EQUAL(statements.size(), 3);

    UNIT_ASSERT_VALUES_EQUAL(statements[0], "-- Comment 1\n        SELECT * From Input; -- Comment 2\n");
    UNIT_ASSERT_VALUES_EQUAL(statements[1], R"(-- Comment 3
        $a = "a";)");
    UNIT_ASSERT_VALUES_EQUAL(statements[2], R"(-- Comment 10

        -- Comment 8

        $b = ($x) -> {
        -- comment 4
        return /* Comment 5 */ $x;
        -- Comment 6
        };)");
}

Y_UNIT_TEST(Bad1Bad2) {
    TString query = " select1; select2;";

    auto statements = Statements(query);
    UNIT_ASSERT_VALUES_EQUAL(statements.size(), 0);
}

} // Y_UNIT_TEST_SUITE(QuerySplit)

Y_UNIT_TEST_SUITE(ColumnCompression) {

Y_UNIT_TEST(CreateCompressedColumn) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64 NOT NULL,
            v Uint64 COMPRESSION(algorithm=zstd, level=5),
            PRIMARY KEY (k)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "columnCompression");
            UNIT_ASSERT_STRING_CONTAINS(line, "algorithm (String '\"zstd");
            UNIT_ASSERT_STRING_CONTAINS(line, "level (Uint64 '\"5");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(NoColumnCompressionAtCreationIfNotSpecified) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64 NOT NULL,
            v Uint64,
            PRIMARY KEY (k)
        );
    )sql");

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("columnCompression"));
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateCompressedColumnEmptyAttributes) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64 NOT NULL,
            v Uint64 COMPRESSION(),
            PRIMARY KEY (k)
        );
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "columnCompression");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(CreateColumnDoubleCompression) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            k Uint64 NOT NULL,
            v Uint64 COMPRESSION() COMPRESSION(),
            PRIMARY KEY (k)
        );
    )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:36: Error: 'COMPRESSION' option can be specified only once\n");
}

Y_UNIT_TEST(AddCompressedColumn) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ADD COLUMN val Uint64 COMPRESSION(algorithm=zstd, level=7);
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "columnCompression");
            UNIT_ASSERT_STRING_CONTAINS(line, "algorithm (String '\"zstd");
            UNIT_ASSERT_STRING_CONTAINS(line, "level (Uint64 '\"7");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnCompression) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION(algorithm=lz4, level=1);
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "changeCompression");
            UNIT_ASSERT_STRING_CONTAINS(line, "algorithm (String '\"lz4");
            UNIT_ASSERT_STRING_CONTAINS(line, "level (Uint64 '\"1");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(NoColumnCompressionAtAlterIfNotSpecified) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET NOT NULL;
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_EQUAL(TString::npos, line.find("columnCompression"));
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnCompressionEmptyAttributes) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION();
    )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_STRING_CONTAINS(line, "changeCompression");
        }
    };

    TWordCountHive elementStat = {"Write"};
    VerifyProgram(res, elementStat, verifyLine);
    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(AlterColumnCompressionDoubleAlgorithm) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION(algorithm=lz4, algorithm=zstd);
    )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:73: Error: 'algorithm' setting can be specified only once\n");
}

Y_UNIT_TEST(AlterColumnCompressionDoubleLevel) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION(level=1, level=2);
    )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:67: Error: 'level' setting can be specified only once\n");
}

Y_UNIT_TEST(AlterColumnCompressionLevelNegative) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN val SET COMPRESSION(level=-1);
    )sql");

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "<main>:3:63: Error: extraneous input '-' expecting");
}

} // Y_UNIT_TEST_SUITE(ColumnCompression)

Y_UNIT_TEST_SUITE(MaterializeStatement) {

Y_UNIT_TEST(TopLevelBasic) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        MATERIALIZE Input INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(TopLevelUnused) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        MATERIALIZE Input INTO $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 0, prog);
}

Y_UNIT_TEST(TopLevelWithCluster) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        MATERIALIZE plato.Input ON plato INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(WithInnerSelectFromTable) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        MATERIALIZE (SELECT * FROM Input) INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(WithInnerSelectWithoutSource) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        MATERIALIZE (SELECT 1 as a) INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(WithInnerNamedExpr) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        $a = SELECT 1 as a;
        MATERIALIZE $a INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(CompileAsSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;
    settings.Mode = NSQLTranslation::ESqlMode::SUBQUERY;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        MATERIALIZE Input INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CompileAsLimitedView) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;
    settings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        MATERIALIZE Input INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(UseInSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        DEFINE SUBQUERY $sub() AS
            MATERIALIZE (SELECT 1 as a) INTO $tmp;
            MATERIALIZE $tmp INTO $result;
            SELECT * FROM $result;
        END DEFINE;
        SELECT * FROM $sub();
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 2, prog);
}

Y_UNIT_TEST(UseInAction) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        DEFINE ACTION $sub() AS
            MATERIALIZE (SELECT 1 as a) INTO $tmp;
            MATERIALIZE $tmp INTO $result;
            SELECT * FROM $result;
        END DEFINE;
        DO $sub();
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 2, prog);
}

Y_UNIT_TEST(MissingCluster) {
    // No USE and no ON clause - should fail
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        MATERIALIZE plato.Input INTO $result;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "USE statement is missing or cluster not specified in MATERIALIZE");
}

Y_UNIT_TEST(NotAllowedBeforeVer) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::MinLangVersion;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        MATERIALIZE Input INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "MATERIALIZE is not available before language version");
}

Y_UNIT_TEST(PreserveSortInRefSelect) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        $a = SELECT * FROM Input ORDER BY a;
        MATERIALIZE $a INTO $b;
        MATERIALIZE $b INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 1, prog);
}

Y_UNIT_TEST(PreserveSortInSubSelect) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        MATERIALIZE (SELECT * FROM Input ORDER BY a) INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 1, prog);
}

Y_UNIT_TEST(PreserveSortInSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        DEFINE SUBQUERY $sub() AS
            SELECT * FROM Input ORDER BY key;
        END DEFINE;
        MATERIALIZE $sub() INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 1, prog);
}

Y_UNIT_TEST(PreserveSortForTopLevelOnly1) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        $a = SELECT * FROM Input ORDER BY key;
        $b = SELECT * FROM $a ORDER BY key;
        MATERIALIZE $b INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "ORDER BY without LIMIT in subquery will be ignored");

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 1, prog);
}

Y_UNIT_TEST(PreserveSortForTopLevelOnly2) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
        USE plato;
        $a = SELECT * FROM Input ORDER BY key;
        $b = SELECT * FROM $a;
        MATERIALIZE $b INTO $result;
        SELECT * FROM $result;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "ORDER BY without LIMIT in subquery will be ignored");

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 0, prog);
}

} // Y_UNIT_TEST_SUITE(MaterializeStatement)
