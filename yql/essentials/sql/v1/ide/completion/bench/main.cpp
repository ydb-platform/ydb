#include <benchmark/benchmark.h>

#include <yql/essentials/sql/v1/ide/completion/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/ide/completion/sql_complete.h>

#include <yql/essentials/sql/v1/ide/pure_ast/parser.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <yql/essentials/utils/string/trim_indent.h>

#include <library/cpp/unicode/utf8_iter/utf8_iter.h>
#include <library/cpp/unicode/utf8_char/utf8_char.h>

#include <util/generic/xrange.h>
#include <util/system/compiler.h>

namespace NSQLComplete {

NSQLComplete::TLexerSupplier MakePureLexerSupplier() {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
    return [lexers = std::move(lexers)](bool ansi) {
        return NSQLTranslationV1::MakeLexer(
            lexers, ansi, NSQLTranslationV1::ELexerFlavor::Pure);
    };
}

ISqlCompletionEngine::TPtr MakeCompletionEngine() {
    auto names = NSQLComplete::LoadDefaultNameSet();
    auto ranking = NSQLComplete::MakeDefaultRanking();
    auto service = MakeStaticNameService(std::move(names), std::move(ranking));
    return MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));
}

void BenchmarkComplete(benchmark::State& state) {
    auto engine = MakeCompletionEngine();

    TString query =
        "SELECT \n"
        "  123467, \"Hello, {name}! 编码\"}, \n"
        "  (1 + (5 * 1 / 0)), #MIN(identifier), \n"
        "  Bool(field), Math::Sin(var) \n"
        "FROM `local/test/space/table` JOIN test;";
    TCompletionInput input = SharpedInput(query);

    for (const auto _ : state) {
        auto completion = engine->Complete(input);
        benchmark::DoNotOptimize(completion);
    }
}

void BenchmarkTabbing(benchmark::State& state, bool isParseTreeReused) {
    auto engine = MakeCompletionEngine();

    TString input = NYql::TrimIndent(R"sql(
        SELECT
          123467, \"Hello, {name}! 编码\"},
          (1 + (5 * 1 / 0)), MIN(identifier),
          Bool(field), Math::Sin(var)
        FROM `local/test/space/table` JOIN test;
    )sql");

    auto parser = NSQLPureAST::MakeParser();
    auto tree = parser->Parse(input);

    const auto check = [&](TStringBuf prefix) {
        TCompletionInput x = {
            {.Text = input, .CursorPosition = prefix.size()},
            /* .ParseTree = */ isParseTreeReused ? tree : nullptr,
        };

        TCompletion completion = engine->Complete(x).GetValueSync();
        benchmark::DoNotOptimize(completion);
    };

    const auto typing = [&] {
        TString prefix(Reserve(input.size()));
        for (wchar32 c : TUtfIterCode(input)) {
            check(prefix);
            prefix += TUtf8Char(c);
        }
        check(prefix);
    };

    for (const auto _ : state) {
        typing();
    }
}

} // namespace NSQLComplete

BENCHMARK(NSQLComplete::BenchmarkComplete);
BENCHMARK_CAPTURE(NSQLComplete::BenchmarkTabbing, NoParseTree, false);
BENCHMARK_CAPTURE(NSQLComplete::BenchmarkTabbing, WithParseTree, true);
