#include "register.h"

#include <yql/essentials/tools/purebench/gbenchmark/suites/suite.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/generic/yexception.h>
#include <util/system/yassert.h>

namespace NYql::NGBenchmark {
namespace {

void RunCase(benchmark::State& state, const NPureBench::TBenchmarkProgramOptions& options) {
    auto program = NPureBench::CreateBenchmarkProgram(options);
    Y_ENSURE(program, "Benchmark program factory returned no program");
    program->PrepareBenchmark(nullptr);
    for (const auto iteration : state) {
        Y_UNUSED(iteration);
        program->RunMeasureQuery(NPureBench::EOutputMode::Discard);
    }
}

} // namespace

TVector<benchmark::Benchmark*> RegisterSuite(TStringBuf suiteName, TStringBuf suiteDataDirectory) {
    const auto cases = NPureBench::LoadBenchmarkSuite(suiteName, suiteDataDirectory);
    TVector<benchmark::Benchmark*> benchmarks(Reserve(cases.size()));
    for (const auto& benchmarkCase : cases) {
        benchmarks.push_back(benchmark::RegisterBenchmark(benchmarkCase.Name.c_str(),
                                                          [options = benchmarkCase.Options](benchmark::State& state) {
                                                              RunCase(state, options);
                                                          }));
    }
    return benchmarks;
}

} // namespace NYql::NGBenchmark
