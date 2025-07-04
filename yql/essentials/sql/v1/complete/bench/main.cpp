#include <benchmark/benchmark.h>

#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/sql_complete.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <util/generic/xrange.h>
#include <util/system/compiler.h>

namespace NSQLComplete {

    NSQLComplete::TLexerSupplier MakePureLexerSupplier() {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
        return [lexers = std::move(lexers)](bool ansi) {
            return NSQLTranslationV1::MakeLexer(
                lexers, ansi, /* antlr4 = */ true,
                NSQLTranslationV1::ELexerFlavor::Pure);
        };
    }

    void BenchmarkComplete(benchmark::State& state) {
        auto names = NSQLComplete::LoadDefaultNameSet();
        auto ranking = NSQLComplete::MakeDefaultRanking();
        auto service = MakeStaticNameService(std::move(names), std::move(ranking));
        auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));

        for (const auto _ : state) {
            auto completion = engine->Complete({"SELECT "});
            benchmark::DoNotOptimize(completion);
        }
    }

} // namespace NSQLComplete

BENCHMARK(NSQLComplete::BenchmarkComplete);
