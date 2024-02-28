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

    void MergeColumns(ui8* result, ui8* const data[4], size_t sizes[4], size_t length) override {
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
    THolder<Perfomancer::Interface> m_worker;
};


template <typename F, typename... Args>
void CompareAllImplementations(const char* msg, F f, Args&&... args)
{
    Cerr << msg << Endl;
    Perfomancer perfomancer;

    Cerr << "Best Trait is: ";
    auto worker = ChooseTrait(perfomancer);

    worker->Add(columns, result);
    result.assign(64e6, 0);

    if (NX86::HaveSSE42()) {
        auto SSE42worker = ChooseSSE42Trait(perfomancer);
        SSE42worker->Add(columns, result);
        result.assign(64e6, 0);
    }
    
    if (NX86::HaveAVX2()) {
        auto AVX2worker = ChooseAVX2Trait(perfomancer);
        AVX2worker->Add(columns, result);
        result.assign(64e6, 0);
    }
    
    auto Fallbackworker = ChooseFallbackTrait(perfomancer);
    BenchmarkPerformancer bench{std::move(Fallbackworker)};
    (bench.*f)(args...); 
}


int main() {
    {
        size_t length = 64e6;
        std::vector<std::vector<ui64>> columns(10, std::vector<ui64>(length, 1e12 + 7));
        std::vector<ui64> result(length);

        CompareAllImplementations(
            "---- Benchmark of method Add ----", &Perfomancer::Interface::Add, columns, result);
    }

    {
        size_t length = 64e6;
        std::vector<ui32> result(length * 4);
        std::vector<std::vector<ui32>> data_vectors{
            std::vector<ui32>(length, 1),
            std::vector<ui32>(length, 2),
            std::vector<ui32>(length, 3),
            std::vector<ui32>(length, 4)
        };
        ui8* data[4]{
            reinterpret_cast<ui8*>(data_vectors[0].data()),
            reinterpret_cast<ui8*>(data_vectors[1].data()),
            reinterpret_cast<ui8*>(data_vectors[2].data()),
            reinterpret_cast<ui8*>(data_vectors[3].data())
        };
        size_t sizes[4]{sizeof(ui32), sizeof(ui32), sizeof(ui32), sizeof(ui32)};

        CompareAllImplementations(
            "---- Benchmark of method MergeColumns ----", &Perfomancer::Interface::MergeColumns,
            reinterpret_cast<ui8*>(result.data()), data, sizes, length);
    }
    
    return 0;
}