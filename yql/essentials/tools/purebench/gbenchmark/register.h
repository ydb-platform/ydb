#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace benchmark {
class Benchmark;
} // namespace benchmark

namespace NYql::NGBenchmark {

TVector<benchmark::Benchmark*> RegisterSuite(TStringBuf suiteName, TStringBuf suiteDataDirectory);

} // namespace NYql::NGBenchmark
