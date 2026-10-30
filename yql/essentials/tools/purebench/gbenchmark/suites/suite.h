#pragma once

#include <yql/essentials/tools/purebench/lib/benchmark_program.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql::NPureBench {

struct TBenchmarkCase {
    TString Name;
    TBenchmarkProgramOptions Options;
};

TVector<TBenchmarkCase> LoadBenchmarkSuite(TStringBuf suiteName, TStringBuf suiteDataDirectory);

} // namespace NYql::NPureBench
