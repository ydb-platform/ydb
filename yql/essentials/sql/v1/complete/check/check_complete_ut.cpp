#include "check_complete.h"

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(CheckTests) {

    Y_UNIT_TEST(Runs) {
        TString query = R"(
            SELECT * FROM (SELECT 1 AS x)
        )";

        NSQLTranslationV1::TLexers lexers = {
            .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
            .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
        };

        NSQLTranslationV1::TParsers parsers = {
            .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
            .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(),
        };

        NSQLTranslation::TTranslators translators(
            /* V0 = */ nullptr,
            /* V1 = */ NSQLTranslationV1::MakeTranslator(lexers, parsers),
            /* PG = */ nullptr);

        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;

        NYql::TAstParseResult result = NSQLTranslation::SqlToYql(translators, query, settings);
        Y_ENSURE(result.IsOk());

        Y_ENSURE(CheckComplete(query, *result.Root, result.Issues), result.Issues.ToString());
    }

} // Y_UNIT_TEST_SUITE(CheckTests)
