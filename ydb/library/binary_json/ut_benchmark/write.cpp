#include <benchmark/benchmark.h>

#include <util/random/random.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/library/binary_json/write.h>

// ya test -r -D BENCHMARK_MAKE_LARGE_PART
#ifndef BENCHMARK_MAKE_LARGE_PART
#define BENCHMARK_MAKE_LARGE_PART 0
#endif

using namespace NKikimr::NBinaryJson;

namespace {

static ui64 seed = 0;

NJson::TJsonValue GetTestJson(ui64 depth = 10, ui64 nChildren = 2) {
    NJson::TJsonValue value;
    if (depth == 1) {
        value.SetValue(NUnitTest::RandomString(10, seed++));
        return value;
    }
    for (ui64 i = 0; i < nChildren; ++i) {
        value.InsertValue(NUnitTest::RandomString(10, seed++), GetTestJson(depth - 1));
    }
    return value;
}

TString GetTestJsonString() {
    seed = 42;
    return NJson::WriteJson(GetTestJson(3, 50));
}

static void BenchWriteSimdJson(benchmark::State& state) {
  TString value = GetTestJsonString();
  TStringBuf buf(value);
  for (auto _ : state) {
    auto result = SerializeToBinaryJson(buf);
    benchmark::DoNotOptimize(result);
    benchmark::ClobberMemory();
  }
}

}

BENCHMARK(BenchWriteSimdJson)->MinTime(1);
