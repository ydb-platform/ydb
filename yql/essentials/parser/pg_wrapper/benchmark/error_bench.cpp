#include "../pg_compat.h"

#include <benchmark/benchmark.h>
#include <util/system/types.h>

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/postgres.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/utils/elog.h>
}

#if defined(NDEBUG) && !defined(_san_enabled_)
constexpr ui32 IterationsCount = 1000000000;
#else
constexpr ui32 IterationsCount = 100000000;
#endif

namespace {

void TestPgTry(benchmark::State& state) {
    for (auto _ : state) {
        volatile ui32 x = 0;
        for (ui32 i = 0; i < IterationsCount; ++i) {
            PG_TRY();
            {
                x += 1;
            }
            PG_CATCH();
            {
            }
            PG_END_TRY();
        }
    }
}
BENCHMARK(TestPgTry);

void TestCppTry(benchmark::State& state) {
    for (auto _ : state) {
        volatile ui32 x = 0;
        for (ui32 i = 0; i < IterationsCount; ++i) {
            try {
                x += 1;
            } catch (...) {
            }
        }
    }
}
BENCHMARK(TestCppTry);

} // namespace
