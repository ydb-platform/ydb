#include "sql_ut.h"

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/sql/v1/translation/sql_translation.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

using namespace NSQLTranslationV1;

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

Y_UNIT_TEST_SUITE(TestGetQueryPosition) {

Y_UNIT_TEST(TestTokenFinding) {
    const TString query = TStringBuilder() << R"(
    )" << "\r" << R"(BEGIN)" << "\r\n" << R"(
       )" << "\n\r" << R"(END
    $b = ()" << "\r\r" << R"($x) -> {

    )" << "\n" << R"(
    -- comment A
    return /*Комментарий*/ $x;
    -- Comment B
    };
    )";

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();

    ui64 lexerPosition = 0;
    const auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
        NSQLv1Generated::TToken tokenProto;
        tokenProto.SetLine(token.Line);
        tokenProto.SetColumn(token.LinePos);
        UNIT_ASSERT_VALUES_EQUAL_C(lexerPosition, NSQLTranslationV1::GetQueryPosition(query, tokenProto), token.Line << ":" << token.LinePos << ":'" << token.Content << "'");

        lexerPosition += token.Content.size();
    };

    const auto lexer = NSQLTranslationV1::MakeLexer(lexers, /*ansi=*/false);

    NYql::TIssues issues;
    const bool result = lexer->Tokenize(query, {}, onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS);
    UNIT_ASSERT_C(result, issues.ToOneLineString());
}

Y_UNIT_TEST(TestTokenMissing) {
    const TString query = "BEGIN /*Комментарий*/ \nEND";
    NSQLv1Generated::TToken tokenProto;

    tokenProto.SetLine(3);
    tokenProto.SetColumn(0);
    UNIT_ASSERT_VALUES_EQUAL(std::string::npos, NSQLTranslationV1::GetQueryPosition(query, tokenProto));

    tokenProto.SetLine(2);
    tokenProto.SetColumn(4);
    UNIT_ASSERT_VALUES_EQUAL(std::string::npos, NSQLTranslationV1::GetQueryPosition(query, tokenProto));

    tokenProto.SetLine(1);
    tokenProto.SetColumn(34);
    UNIT_ASSERT_VALUES_EQUAL(std::string::npos, NSQLTranslationV1::GetQueryPosition(query, tokenProto));

    tokenProto.SetLine(1);
    tokenProto.SetColumn(0);
    UNIT_ASSERT_VALUES_EQUAL(0, NSQLTranslationV1::GetQueryPosition(query, tokenProto));
}
} // Y_UNIT_TEST_SUITE(TestGetQueryPosition)
