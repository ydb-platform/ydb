#include <benchmark/benchmark.h>
#include <util/generic/xrange.h>
#include <ydb/library/aclib/aclib.h>

namespace NACLib {

namespace {
    struct TFixture : public benchmark::Fixture {
        void SetUp(::benchmark::State& state) 
        {
            const size_t count = state.range(0);
            
            for (auto userId : xrange(count)) {
                Obj.AddAccess(EAccessType::Allow, EAccessRights::SelectRow, "user" + std::to_string(userId), EInheritanceType::InheritContainer);
            }

            Serialized = Obj.SerializeAsString();
        }

        TString Serialized;
        TACL Obj;
    };
}

BENCHMARK_DEFINE_F(TFixture, Deserialize)(benchmark::State& state) {
    for (auto _ : state) {
        benchmark::DoNotOptimize( NACLib::TACL(Serialized));
    }
}

BENCHMARK_DEFINE_F(TFixture, HasAccess)(benchmark::State& state) {
    for (auto _ : state) {
        benchmark::DoNotOptimize(Obj.HasAccess("userxxx"));
    }
}

BENCHMARK_REGISTER_F(TFixture, Deserialize)
    ->ArgsProduct({
        {20, 200, 2000}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TFixture, HasAccess)
    ->ArgsProduct({
        {20, 200, 2000}})
    ->Unit(benchmark::kMicrosecond);

}
