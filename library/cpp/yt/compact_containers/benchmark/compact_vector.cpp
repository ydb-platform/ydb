#include <benchmark/benchmark.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <optional>
#include <string>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int PoolSize = 4096;

enum class EVectorState
{
    Empty,
    SparseInline,
    FullInline,
    OnHeap,
};

const char* GetStateName(EVectorState state)
{
    switch (state) {
        case EVectorState::Empty:
            return "empty";
        case EVectorState::SparseInline:
            return "sparse-inline";
        case EVectorState::FullInline:
            return "full-inline";
        case EVectorState::OnHeap:
            return "heap";
    }
    Y_UNREACHABLE();
}

template <class T, size_t RepresentationSize>
struct TVectorTraits
{
    static_assert(RepresentationSize % sizeof(uintptr_t) == 0);

    static constexpr size_t InlineCapacity =
        (RepresentationSize - alignof(T)) / sizeof(T);
    using TVector = TCompactVector<T, InlineCapacity>;

    static_assert(sizeof(TVector) == RepresentationSize);
};

template <class T, size_t RepresentationSize>
using TVector = typename TVectorTraits<T, RepresentationSize>::TVector;

template <class T, size_t RepresentationSize>
size_t GetElementCount(EVectorState state)
{
    constexpr auto inlineCapacity = TVectorTraits<T, RepresentationSize>::InlineCapacity;
    switch (state) {
        case EVectorState::Empty:
            return 0;
        case EVectorState::SparseInline:
            return 1;
        case EVectorState::FullInline:
            return inlineCapacity;
        case EVectorState::OnHeap:
            return inlineCapacity + 2;
    }
    Y_UNREACHABLE();
}

template <class T, size_t RepresentationSize>
std::string GetLabel(EVectorState state)
{
    return std::string(GetStateName(state)) +
        ", representation=" + std::to_string(RepresentationSize) + " B" +
        ", element=" + std::to_string(sizeof(T)) + " B";
}

template <class T, size_t RepresentationSize>
std::string GetLabel(EVectorState lhsState, EVectorState rhsState, const char* operation)
{
    return std::string(GetStateName(lhsState)) + operation + GetStateName(rhsState) +
        ", representation=" + std::to_string(RepresentationSize) + " B" +
        ", element=" + std::to_string(sizeof(T)) + " B";
}

void ApplyVectorStates(benchmark::Benchmark* benchmark)
{
    benchmark->DenseRange(
        static_cast<int>(EVectorState::SparseInline),
        static_cast<int>(EVectorState::OnHeap));
}

void ApplySwapStates(benchmark::Benchmark* benchmark)
{
    auto sparseInline = static_cast<int>(EVectorState::SparseInline);
    auto fullInline = static_cast<int>(EVectorState::FullInline);
    auto onHeap = static_cast<int>(EVectorState::OnHeap);
    benchmark
        ->Args({sparseInline, fullInline})
        ->Args({fullInline, onHeap})
        ->Args({onHeap, onHeap});
}

#define REGISTER_VECTOR_BENCHMARKS(Benchmark, ApplyFunction) \
    BENCHMARK_TEMPLATE(Benchmark, ui8, 16)->Apply(ApplyFunction); \
    BENCHMARK_TEMPLATE(Benchmark, ui64, 16)->Apply(ApplyFunction); \
    BENCHMARK_TEMPLATE(Benchmark, ui8, 24)->Apply(ApplyFunction); \
    BENCHMARK_TEMPLATE(Benchmark, ui64, 24)->Apply(ApplyFunction); \
    BENCHMARK_TEMPLATE(Benchmark, ui8, 64)->Apply(ApplyFunction); \
    BENCHMARK_TEMPLATE(Benchmark, ui8, 72)->Apply(ApplyFunction)

////////////////////////////////////////////////////////////////////////////////

template <size_t RepresentationSize>
void BM_DefaultConstruct(benchmark::State& state)
{
    state.SetLabel("representation=" + std::to_string(RepresentationSize) + " B");
    for (auto _ : state) {
        TVector<ui8, RepresentationSize> vector;
        benchmark::DoNotOptimize(&vector);
        benchmark::ClobberMemory();
    }
}

BENCHMARK_TEMPLATE(BM_DefaultConstruct, 16);
BENCHMARK_TEMPLATE(BM_DefaultConstruct, 24);
BENCHMARK_TEMPLATE(BM_DefaultConstruct, 64);
BENCHMARK_TEMPLATE(BM_DefaultConstruct, 72);

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t RepresentationSize>
void BM_MoveConstruct(benchmark::State& state)
{
    auto vectorState = static_cast<EVectorState>(state.range(0));
    auto elementCount = GetElementCount<T, RepresentationSize>(vectorState);
    state.SetLabel(GetLabel<T, RepresentationSize>(vectorState));

    struct TCase
    {
        TVector<T, RepresentationSize> Source;
        std::optional<TVector<T, RepresentationSize>> Destination;
    };

    std::vector<TCase> pool;
    auto refill = [&] {
        pool.clear();
        pool.resize(PoolSize);
        for (auto& item : pool) {
            item.Source.resize(elementCount);
        }
    };
    refill();

    int index = PoolSize;
    for (auto _ : state) {
        if (index == 0) {
            state.PauseTiming();
            refill();
            index = PoolSize;
            state.ResumeTiming();
        }
        auto& item = pool[--index];
        item.Destination.emplace(std::move(item.Source));
        benchmark::DoNotOptimize(*item.Destination);
    }
}

REGISTER_VECTOR_BENCHMARKS(BM_MoveConstruct, ApplyVectorStates);

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t RepresentationSize>
void BM_Swap(benchmark::State& state)
{
    auto lhsState = static_cast<EVectorState>(state.range(0));
    auto rhsState = static_cast<EVectorState>(state.range(1));
    state.SetLabel(GetLabel<T, RepresentationSize>(lhsState, rhsState, " <-> "));

    struct TCase
    {
        TVector<T, RepresentationSize> Lhs;
        TVector<T, RepresentationSize> Rhs;
    };

    std::vector<TCase> pool;
    auto refill = [&] {
        pool.clear();
        pool.resize(PoolSize);
        for (auto& item : pool) {
            item.Lhs.resize(GetElementCount<T, RepresentationSize>(lhsState));
            item.Rhs.resize(GetElementCount<T, RepresentationSize>(rhsState));
        }
    };
    refill();

    int index = PoolSize;
    for (auto _ : state) {
        if (index == 0) {
            state.PauseTiming();
            refill();
            index = PoolSize;
            state.ResumeTiming();
        }
        auto& item = pool[--index];
        item.Lhs.swap(item.Rhs);
        benchmark::DoNotOptimize(item.Lhs);
        benchmark::DoNotOptimize(item.Rhs);
    }
}

REGISTER_VECTOR_BENCHMARKS(BM_Swap, ApplySwapStates);

#undef REGISTER_VECTOR_BENCHMARKS

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
