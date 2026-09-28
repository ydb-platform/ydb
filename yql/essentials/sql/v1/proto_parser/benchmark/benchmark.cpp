#include <benchmark/benchmark.h>

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

#include <yql/essentials/sql/v1/proto_parser/antlr4/parser_cache.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/parser_cache.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>

namespace {

class TParser final {
public:
    TParser() {
        Parsers_.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false,
            /*maxParseTreeDepth=*/Nothing());

        Parsers_.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false,
            /*maxParseTreeDepth=*/Nothing());
    }

    google::protobuf::Message* Parse(const TString& query) {
        NYql::TIssues issues;

        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &Arena_;

        if (!ParseTranslationSettings(query, settings, issues)) {
            return nullptr;
        }

        return NSQLTranslationV1::SqlAST(
            Parsers_,
            query,
            /* queryName = */ "",
            issues,
            NSQLTranslation::SQL_MAX_PARSER_ERRORS,
            settings.AnsiLexer,
            settings.Arena);
    }

    void Clear() {
        Arena_.Reset();
    }

private:
    NSQLTranslationV1::TParsers Parsers_;
    google::protobuf::Arena Arena_;
};

TString Query(TStringBuf name) {
    TString query;
    Y_ENSURE(NResource::FindExact(name, &query), name);
    return query;
}

void BenchmarkCold(TString query, benchmark::State& state) {
    TParser parser;

    for (const auto _ : state) {
        state.PauseTiming();
        NSQLTranslationV1::ClearDefaultParserCache();
        parser.Clear();
        state.ResumeTiming();

        benchmark::DoNotOptimize(parser.Parse(query));
    }
}

void BenchmarkWarm(TString query, benchmark::State& state) {
    TParser parser;

    benchmark::DoNotOptimize(parser.Parse(query));

    for (const auto _ : state) {
        state.PauseTiming();
        parser.Clear();
        state.ResumeTiming();

        benchmark::DoNotOptimize(parser.Parse(query));
    }
}

void BenchmarkAnsiSelectYqlMinimal(benchmark::State& state) {
    TString queryDefault = Query("select-yql-minimal.yql");
    TString queryAnsi = queryDefault;

    queryDefault.prepend("--ansi_lexer\n");
    queryAnsi.prepend("--!ansi_lexer\n");

    TParser parser;

    for (const auto _ : state) {
        state.PauseTiming();
        NSQLTranslationV1::ClearDefaultParserCache();
        NSQLTranslationV1::ClearAnsiParserCache();

        parser.Clear();
        benchmark::DoNotOptimize(parser.Parse(queryDefault));

        parser.Clear();
        state.ResumeTiming();

        benchmark::DoNotOptimize(parser.Parse(queryAnsi));
    }
}

void BenchmarkColdSelectYqlMinimal(benchmark::State& state) {
    BenchmarkCold(Query("select-yql-minimal.yql"), state);
}

void BenchmarkColdYqlTpcdsQ47(benchmark::State& state) {
    BenchmarkCold(Query("yql-tpcds-q47.yql"), state);
}

void BenchmarkWarmSelectYqlMinimal(benchmark::State& state) {
    BenchmarkWarm(Query("select-yql-minimal.yql"), state);
}

void BenchmarkWarmYqlTpcdsQ47(benchmark::State& state) {
    BenchmarkWarm(Query("yql-tpcds-q47.yql"), state);
}

} // namespace

BENCHMARK(BenchmarkAnsiSelectYqlMinimal)->Iterations(250);
BENCHMARK(BenchmarkColdSelectYqlMinimal);
BENCHMARK(BenchmarkWarmSelectYqlMinimal);

BENCHMARK(BenchmarkColdYqlTpcdsQ47);
BENCHMARK(BenchmarkWarmYqlTpcdsQ47);
