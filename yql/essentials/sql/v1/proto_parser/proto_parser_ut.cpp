#include "proto_parser.h"

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ProtoParserTests) {

google::protobuf::Message* SqlAST(
    const TString& query,
    NSQLTranslation::TTranslationSettings settings,
    NYql::TIssues& issues)
{
    NSQLTranslationV1::TLexers lexers;
    NSQLTranslationV1::TParsers parsers;

    if (!ParseTranslationSettings(query, settings, issues)) {
        return nullptr;
    }

    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    auto lexer = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer);
    auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
        Y_UNUSED(token);
    };

    if (!lexer->Tokenize(query, "", onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        return nullptr;
    }

    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
        /*isAmbiguityError=*/false,
        /*isAmbiguityDebugging=*/false,
        settings.MaxParseTreeDepth);

    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
        /*isAmbiguityError=*/false,
        /*isAmbiguityDebugging=*/false,
        settings.MaxParseTreeDepth);

    return NSQLTranslationV1::SqlAST(
        parsers,
        query,
        /* queryName = */ "",
        issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS,
        settings.AnsiLexer,
        settings.Arena);
}

TString GenerateQuery(size_t depth = 8 * 1024) {
    const TString prefix = "SELECT 1 FROM (";
    const TString core = "SELECT 1";
    const TString suffix = ")";

    TString query;
    query.reserve(prefix.size() * depth + core.size() + suffix.size() * depth);
    for (size_t i = 0; i < depth; ++i) {
        query += prefix;
    }
    query += core;
    for (size_t i = 0; i < depth; ++i) {
        query += suffix;
    }

    return query;
}

Y_UNIT_TEST(StackOverflowSubquery) {
    const auto query = GenerateQuery();

    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;
    settings.MaxParseTreeDepth = 4 * 1024;

    NYql::TIssues issues;
    const auto* m = SqlAST(query, settings, issues);

    UNIT_ASSERT(!m);
    UNIT_ASSERT_STRING_CONTAINS(issues.ToOneLineString(), "Maximum parse tree depth exceeded");
}

} // Y_UNIT_TEST_SUITE(ProtoParserTests)
