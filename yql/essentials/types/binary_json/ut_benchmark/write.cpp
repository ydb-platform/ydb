#include <benchmark/benchmark.h>

#include <util/random/random.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <yql/essentials/types/binary_json/write.h>

// ya test -r -D BENCHMARK_MAKE_LARGE_PART
#ifndef BENCHMARK_MAKE_LARGE_PART
    #define BENCHMARK_MAKE_LARGE_PART 0
#endif

using namespace NKikimr::NBinaryJson;

namespace {

ui64 seed = 0;

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

void BenchWriteSimdJson(benchmark::State& state) {
    TString value = GetTestJsonString();
    TStringBuf buf(value);
    for (auto _ : state) {
        auto result = SerializeToBinaryJson(buf);
        benchmark::DoNotOptimize(result);
        benchmark::ClobberMemory();
    }
}

// Converts many distinct documents one at a time, as a column conversion does, so that the
// per-call cost is measured and not only the parsing of one document.
void BenchWriteSimdJsonRows(benchmark::State& state) {
    seed = 42;
    TVector<TString> rows;
    size_t bytes = 0;
    for (size_t i = 0; i < 1024; ++i) {
        rows.push_back(NJson::WriteJson(GetTestJson(2, state.range(0))));
        bytes += rows.back().size();
    }
    for (auto _ : state) {
        for (const auto& row : rows) {
            auto result = SerializeToBinaryJson(row);
            benchmark::DoNotOptimize(result);
        }
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * rows.size());
    state.SetBytesProcessed(state.iterations() * bytes);
    state.counters["bytes_per_row"] = double(bytes) / rows.size();
}

} // namespace

BENCHMARK(BenchWriteSimdJson)->MinTime(1);
BENCHMARK(BenchWriteSimdJsonRows)->Arg(2)->Arg(8)->Arg(32)->Arg(128)->MinTime(1);
