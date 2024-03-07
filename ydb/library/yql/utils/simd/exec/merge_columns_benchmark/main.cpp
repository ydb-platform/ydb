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

        double time = std::chrono::duration<double>(end - begin).count();
        // Calculate size of data
        ui64 bytes = static_cast<ui64>(length) * static_cast<ui64>(std::accumulate(sizes, sizes + 4, 0ull));
        // Calculate speed of data processing as GB/sec
        double speed;
        const char* msg;
        if (static_cast<double>(bytes) > static_cast<double>(GB) * time) {
            speed = static_cast<double>(bytes) / (static_cast<double>(GB) * time);
            msg = " [GB/sec]";
        }
        else if (static_cast<double>(bytes) > static_cast<double>(MB) * time) {
            speed = static_cast<double>(bytes) / (static_cast<double>(MB) * time);
            msg = " [MB/sec]";
        }
        else {
            speed = static_cast<double>(bytes) / (static_cast<double>(KB) * time);
            msg = " [KB/sec]";
        }

        Cerr << "Total time: " << static_cast<ui64>(time * 1000.0 /* ms in sec */) << " [ms]" << Endl;
        Cerr << "Speed: " << static_cast<ui64>(speed) << msg << Endl;
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
        ui32* pt1 = new ui32[length];
        ui32* pt2 = new ui32[length];
        ui32* pt3 = new ui32[length];
        ui32* pt4 = new ui32[length];

        i8* data[4]{
            reinterpret_cast<i8*>(pt1),
            reinterpret_cast<i8*>(pt2),
            reinterpret_cast<i8*>(pt3),
            reinterpret_cast<i8*>(pt4)
        };
        ui32* result = new ui32[4 * length];
        size_t sizes[4]{sizeof(ui32), sizeof(ui32), sizeof(ui32), sizeof(ui32)};

        CompareAllImplementations(
            "---- Benchmark of method MergeColumns ----\n", &NSimd::Perfomancer::Interface::MergeColumns,
            reinterpret_cast<i8*>(result), data, sizes, length);
        Cerr << "------------------------------------------------" << Endl;

        delete[] result;
        delete[] pt4;
        delete[] pt3;
        delete[] pt2;
        delete[] pt1;
    }

    return 0;
}