#pragma once

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NYql::NGBenchmark {

TVector<benchmark::Benchmark*> RegisterSuite(TStringBuf suiteName, TStringBuf suiteDataDirectory);

} // namespace NYql::NGBenchmark
