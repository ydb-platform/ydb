#include <ydb/library/yql/utils/simd/exec/runtime_dispatching/algo.h>
#include <chrono>
#include <utility>
#include <numeric>
#include <cstring>
#include <memory>

class BenchmarkPerformancer: public Perfomancer::Interface {
public:
    BenchmarkPerformancer(THolder<Perfomancer::Interface>&& worker)
        : m_worker(std::move(worker)) {
    }

    void Add(vvl& columns, vl& result) override {
        auto begin = std::chrono::steady_clock::now();
        m_worker->Add(columns, result);
        auto end = std::chrono::steady_clock::now();

        // Calculate size of data
        ui64 bytes = 0;
        for (const auto& column: columns)
        {
            bytes += sizeof(ui64) * column.size();
        }
        // Calculate speed of data processing as GB/sec
        auto speed = static_cast<double>(bytes) / (GB * std::chrono::duration<double>(end - begin).count());

        Cerr << "Total time: "
             << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << " [ms]" << Endl;
        Cerr << "Speed: " << speed << " [GB/sec]" << Endl;
        Cerr << "Data size: " << bytes << " [bytes]" << Endl << Endl;

        result.assign(columns.back().size(), 0);
    }

private:
    static constexpr size_t KB = 1000;
    static constexpr size_t MB = KB * KB;
    static constexpr size_t GB = KB * KB * KB;

private:
    THolder<Perfomancer::Interface> m_worker;
};


template <typename F, typename... Args>
void CompareAllImplementations(const char* msg, F f, Args&&... args)
{
    Cerr << msg << Endl;
    Perfomancer perfomancer;

    if (NX86::HaveSSE42()) {
        auto SSE42worker = ChooseSSE42Trait(perfomancer);
        BenchmarkPerformancer bench{std::move(SSE42worker)};
        (bench.*f)(args...);
    }
    
    if (NX86::HaveAVX2()) {
        auto AVX2worker = ChooseAVX2Trait(perfomancer);
        BenchmarkPerformancer bench{std::move(AVX2worker)};
        (bench.*f)(args...);
    }
    
    auto Fallbackworker = ChooseFallbackTrait(perfomancer);
    BenchmarkPerformancer bench{std::move(Fallbackworker)};
    (bench.*f)(args...);
}


int main() {
    size_t length = 64e6;
    std::vector<std::vector<ui64>> columns(10, std::vector<ui64>(length, 1e12 + 7));
    std::vector<ui64> result(length);

    CompareAllImplementations(
        "---- Benchmark of method Add ----", &Perfomancer::Interface::Add, columns, result);
    return 0;
}