#include <benchmark/benchmark.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/string/format.h>

#include <library/cpp/yt/misc/guid.h>

namespace NYT::NLogging {
namespace {

////////////////////////////////////////////////////////////////////////////////

const TGuid RequestId(0x12345678, 0x9abcdef0, 0x11223344, 0x55667788);
const std::string User = "robot-benchmark-user";
const std::string Realm = "some-realm";

void BM_WithTag(benchmark::State& state)
{
    TLogger logger("Benchmark");
    for (auto _ : state) {
        auto tagged = logger.WithTag("RequestId: %v, User: %v, Realm: %v", RequestId, User, Realm);
        benchmark::DoNotOptimize(tagged);
    }
}

BENCHMARK(BM_WithTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
