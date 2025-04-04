#include "sql_ut_antlr4.h"
#include "format/sql_format.h"
#include "lexer/lexer.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
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

    auto lexer = NSQLTranslationV1::MakeLexer(lexers, false, true);
    TParsedTokenList tokens;
    NYql::TIssues issues;
    UNIT_ASSERT_C(Tokenize(*lexer, query, "Query", tokens, issues, SQL_MAX_PARSER_ERRORS),
                  issues.ToString());

    return tokens;
}

}

#define ANTLR_VER 4
#include "sql_ut_common.h"

Y_UNIT_TEST_SUITE(QuerySplit) {
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

        google::protobuf::Arena Arena;

        NSQLTranslation::TTranslationSettings settings;
        settings.AnsiLexer = false;
        settings.Antlr4Parser = true;
        settings.Arena = &Arena;

        TVector<TString> statements;
        NYql::TIssues issues;

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();

        UNIT_ASSERT(NSQLTranslationV1::SplitQueryToStatements(lexers, parsers, query, statements, issues, settings));

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
}


