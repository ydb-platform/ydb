#include <benchmark/benchmark.h>

#include <yql/essentials/sql/v1/ide/pure_ast/parser.h>

#include <library/cpp/resource/resource.h>

#include <util/generic/yexception.h>

namespace {

TString Query(TStringBuf name) {
    TString query;
    Y_ENSURE(NResource::FindExact(name, &query), name);
    return query;
}

void BenchmarkCold(TString query, benchmark::State& state) {
    auto parser = NSQLPureAST::MakeParser(/*isAnsiLexer=*/false);

    for (const auto _ : state) {
        state.PauseTiming();
        NSQLPureAST::ClearParserCache();
        state.ResumeTiming();

        benchmark::DoNotOptimize(parser->Parse(query));
    }
}

void BenchmarkCool(TString warmup, TString query, benchmark::State& state) {
    auto parser = NSQLPureAST::MakeParser(/*isAnsiLexer=*/false);

    for (const auto _ : state) {
        state.PauseTiming();
        NSQLPureAST::ClearParserCache();
        benchmark::DoNotOptimize(parser->Parse(warmup));
        state.ResumeTiming();

        benchmark::DoNotOptimize(parser->Parse(query));
    }
}

void BenchmarkWarm(TString query, benchmark::State& state) {
    auto parser = NSQLPureAST::MakeParser(/*isAnsiLexer=*/false);

    benchmark::DoNotOptimize(parser->Parse(query));

    for (const auto _ : state) {
        benchmark::DoNotOptimize(parser->Parse(query));
    }
}

void BenchmarkAnsiSelectYqlMinimal(benchmark::State& state) {
    auto queryDefault = Query("select-yql-minimal.yql");
    auto queryAnsi = Query("select-yql-minimal.yql");

    queryDefault.prepend("--ansi_lexer\n");
    queryAnsi.prepend("--!ansi_lexer\n");

    BenchmarkCool(queryDefault, queryAnsi, state);
}

void BenchmarkColdSelectYqlMinimal(benchmark::State& state) {
    BenchmarkCold(Query("select-yql-minimal.yql"), state);
}

void BenchmarkColdYqlTpcdsQ47(benchmark::State& state) {
    BenchmarkCold(Query("yql-tpcds-q47.yql"), state);
}

void BenchmarkCoolSelectYqlMinimal(benchmark::State& state) {
    auto query = Query("select-yql-minimal.yql");
    BenchmarkCool(query, query, state);
}

void BenchmarkCoolYqlTpcdsQ47(benchmark::State& state) {
    auto query = Query("yql-tpcds-q47.yql");
    BenchmarkCool(query, query, state);
}

void BenchmarkCoolYqlTpcdsQ47AfterTpchQ15(benchmark::State& state) {
    BenchmarkCool(Query("yql-tpch-q15.yql"), Query("yql-tpcds-q47.yql"), state);
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
BENCHMARK(BenchmarkCoolSelectYqlMinimal)->Iterations(250);
BENCHMARK(BenchmarkWarmSelectYqlMinimal);

BENCHMARK(BenchmarkColdYqlTpcdsQ47);
BENCHMARK(BenchmarkCoolYqlTpcdsQ47)->Iterations(250);
BENCHMARK(BenchmarkWarmYqlTpcdsQ47);
BENCHMARK(BenchmarkCoolYqlTpcdsQ47AfterTpchQ15)->Iterations(250);
