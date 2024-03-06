#include <ydb/library/yql/utils/simd/simd.h>
#include <chrono>
#include <utility>
#include <numeric>
#include <cstring>
#include <memory>


// Wrapper to mesaure time and speed
class BenchmarkPerformancer: public NSimd::Perfomancer::Interface {
public:
    BenchmarkPerformancer(THolder<NSimd::Perfomancer::Interface>&& worker)
        : m_worker(std::move(worker)) {
    }

    void MergeColumns(i8* result, i8* const data[4], size_t sizes[4], size_t length) override {
        auto begin = std::chrono::steady_clock::now();
        m_worker->MergeColumns(result, data, sizes, length);
        auto end = std::chrono::steady_clock::now();

        // Calculate size of data
        ui64 bytes = static_cast<ui64>(length) * static_cast<ui64>(std::accumulate(sizes, sizes + 4, 0ull));
        // Calculate speed of data processing as GB/sec
        auto speed = static_cast<double>(bytes) / (GB * std::chrono::duration<double>(end - begin).count());

        Cerr << "Total time: "
             << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << " [ms]" << Endl;
        Cerr << "Speed: " << speed << " [GB/sec]" << Endl;
        Cerr << "Data size: " << bytes << " [bytes]" << Endl << Endl;

        std::memset(result, 0, bytes);
    }

private:
    static constexpr size_t KB = 1000;
    static constexpr size_t MB = KB * KB;
    static constexpr size_t GB = KB * KB * KB;

private:
    THolder<NSimd::Perfomancer::Interface> m_worker;
};


// Functions to force choice of trait
template <typename TFactory>
auto ChooseAVX2Trait(TFactory& factory) {
    return factory.template Create<NSimd::AVX2Trait>();
}

template <typename TFactory>
auto ChooseSSE42Trait(TFactory& factory) {
    return factory.template Create<NSimd::SSE42Trait>();
}

template <typename TFactory>
auto ChooseFallbackTrait(TFactory& factory) {
    return factory.template Create<NSimd::FallbackTrait>();
}


// Function to call some method of Benchmarker for all archs to measure speed and time
template <typename F, typename... Args>
void CompareAllImplementations(const char* msg, F f, Args&&... args)
{
    Cerr << msg << Endl;

    if (NX86::HaveSSE42()) {
        NSimd::Perfomancer perfomancer;
        auto SSE42worker = ChooseSSE42Trait(perfomancer);
        BenchmarkPerformancer bench{std::move(SSE42worker)};
        (bench.*f)(args...);
    }
    
    if (NX86::HaveAVX2()) {
        NSimd::Perfomancer perfomancer;
        auto AVX2worker = ChooseAVX2Trait(perfomancer);
        BenchmarkPerformancer bench{std::move(AVX2worker)};
        (bench.*f)(args...);
    }

// TODO: uncomment when fallback will be implemented
#if 0
    {
        NSimd::Perfomancer perfomancer;
        auto Fallbackworker = ChooseFallbackTrait(perfomancer);
        BenchmarkPerformancer bench{std::move(Fallbackworker)};
        (bench.*f)(args...);
    }
#endif
}

// To add new method to benchmark just write wrapper-method in BenchmarkPerformancer class
// and call CompareAllImplementations in main passing as an argument the arguments
// and a pointer to the desired interface method
int main() {
    for (size_t length: {100'000, 1'000'000, 10'000'000})
    {
        Cerr << "Benchmark for length: " << length << Endl;
        std::vector<ui32> result(length * 4);
        std::vector<std::vector<ui32>> data_vectors{
            std::vector<ui32>(length, 1),
            std::vector<ui32>(length, 2),
            std::vector<ui32>(length, 3),
            std::vector<ui32>(length, 4)
        };
        i8* data[4]{
            reinterpret_cast<i8*>(data_vectors[0].data()),
            reinterpret_cast<i8*>(data_vectors[1].data()),
            reinterpret_cast<i8*>(data_vectors[2].data()),
            reinterpret_cast<i8*>(data_vectors[3].data())
        };
        size_t sizes[4]{sizeof(ui32), sizeof(ui32), sizeof(ui32), sizeof(ui32)};

        CompareAllImplementations(
            "---- Benchmark of method MergeColumns ----", &NSimd::Perfomancer::Interface::MergeColumns,
            reinterpret_cast<i8*>(result.data()), data, sizes, length);
        Cerr << "------------------------------------------------" << Endl;
    }

    return 0;
}