#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>
#include <ydb/core/kqp/host/new_parser/grammar/default/SQLv1Antlr4Lexer.h>
#include <ydb/core/kqp/host/new_parser/grammar/default/SQLv1Antlr4Parser.h>
#include <ydb/core/kqp/host/new_parser/grammar/ansi/SQLv1Antlr4Lexer.h>
#include <ydb/core/kqp/host/new_parser/grammar/ansi/SQLv1Antlr4Parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <future>

namespace NKikimr::NKqp {

namespace {

// Expose parser/lexer DFA and prediction-context cache addresses from real
// parses. Comparing addresses across threads detects accidental thread-local
// cache generation even when both parsers produce identical syntax results.
template <typename TLexer, typename TParser>
std::array<const void*, 4> ParseAndGetCaches() {
    antlr4::ANTLRInputStream input("SELECT 1 + 2;");
    TLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    TParser parser(&tokens);
    UNIT_ASSERT(parser.sql_query());
    UNIT_ASSERT_VALUES_EQUAL(parser.getNumberOfSyntaxErrors(), 0);
    const auto* lexerSimulator = lexer.template getInterpreter<antlr4::atn::LexerATNSimulator>();
    const auto* parserSimulator = parser.template getInterpreter<antlr4::atn::ParserATNSimulator>();
    return {&lexerSimulator->_decisionToDFA, &lexerSimulator->getSharedContextCache(),
        &parserSimulator->decisionToDFA, &parserSimulator->getSharedContextCache()};
}

// Run parsing on separate asynchronous threads and compare against the main
// thread. Cache sharing is a generated-parser property, so test both dialects.
template <typename TLexer, typename TParser>
void CheckProcessWideCaches() {
    const auto first = ParseAndGetCaches<TLexer, TParser>();
    const auto second = ParseAndGetCaches<TLexer, TParser>();
    auto otherThread = std::async(std::launch::async, [] {
        return ParseAndGetCaches<TLexer, TParser>();
    });
    const auto other = otherThread.get();
    for (size_t i = 0; i < first.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(first[i], second[i]);
        UNIT_ASSERT_VALUES_EQUAL(first[i], other[i]);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlParser) {
    // The default generated target must honor the shared-cache build setting.
    Y_UNIT_TEST(ProcessWideDefaultDfaCache) {
        CheckProcessWideCaches<NKqpDefaultAntlr4::SQLv1Antlr4Lexer, NKqpDefaultAntlr4::SQLv1Antlr4Parser>();
    }

    // Check ANSI independently: it is a separate generated library.
    Y_UNIT_TEST(ProcessWideAnsiDfaCache) {
        CheckProcessWideCaches<NKqpAnsiAntlr4::SQLv1Antlr4Lexer, NKqpAnsiAntlr4::SQLv1Antlr4Parser>();
    }

    // Parsing must not require table metadata or invoke semantic translation.
    Y_UNIT_TEST(AstWithoutSemanticTranslation) {
        // This passes the grammar but would fail function resolution.
        auto parsed = ParseQueryWithNewParser("SELECT NotAnExistingFunction(1);", {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        UNIT_ASSERT(parsed.Issues.Empty());
        UNIT_ASSERT(!parsed.Ast->AnsiLexer);
        const auto& select = std::get<NNewYql::TSelect>(std::get<NNewYql::TQueryStatement>(parsed.Ast->Statements[0].Value).Query->Body);
        const auto& call = std::get<NNewYql::TCall>(select.Columns[0].Expression->Value);
        // ANTLR has already been destroyed. All names and expressions are owned
        // by the independent AST, including unknown functions awaiting binding.
        UNIT_ASSERT_VALUES_EQUAL(std::get<NNewYql::TName>(call.Callable->Value).Name.Spelling, "NotAnExistingFunction");
    }

    // Leading SQL directives must select the lexer before tokenization starts.
    Y_UNIT_TEST(AnsiLexerDirective) {
        auto parsed = ParseQueryWithNewParser("--!ansi_lexer\nSELECT 1 AS \"value\";", {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        UNIT_ASSERT(parsed.Issues.Empty());
        UNIT_ASSERT(parsed.Ast->AnsiLexer);
        const auto& select = std::get<NNewYql::TSelect>(std::get<NNewYql::TQueryStatement>(parsed.Ast->Statements[0].Value).Query->Body);
        UNIT_ASSERT_VALUES_EQUAL(select.Columns[0].Alias->Spelling, "\"value\"");
    }

    // Malformed SQL returns positioned issues, not an unsupported-feature signal.
    Y_UNIT_TEST(SyntaxErrorDoesNotRequestFallback) {
        auto parsed = ParseQueryWithNewParser("SELECT (", {}, false);
        UNIT_ASSERT(!parsed.IsOk());
        UNIT_ASSERT(!parsed.Issues.Empty());
    }

    // Tokenization errors follow the same diagnostic contract as parser errors.
    Y_UNIT_TEST(LexerErrorDoesNotRequestFallback) {
        auto parsed = ParseQueryWithNewParser("SELECT 1; \x01", {}, false);
        UNIT_ASSERT(!parsed.IsOk());
        UNIT_ASSERT(!parsed.Issues.Empty());
    }

    // Valid syntax from another dialect must be delegated without reporting
    // a misleading YQL syntax error.
    Y_UNIT_TEST(UnsupportedDialectRequestsFallback) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ParseQueryWithNewParser("--!syntax_pg\nSELECT 1;", {}, false),
            TNewYqlParserFallback, "only supports YQL v1 syntax");
    }
}

} // namespace NKikimr::NKqp
