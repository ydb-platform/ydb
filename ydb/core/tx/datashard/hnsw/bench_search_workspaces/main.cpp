#include <ydb/core/tx/datashard/hnsw_index.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/random/fast.h>
#include <util/string/cast.h>

#include <algorithm>
#include <barrier>
#include <chrono>
#include <ctime>
#include <iostream>
#include <sstream>
#include <thread>

using namespace NKikimr;
using namespace NKikimr::NDataShard;

namespace {

TString RandomVector(TFastRng<ui64>& rng, ui32 dimension) {
    TString result;
    TStringOutput output(result);
    NKnnVectorSerialization::TSerializer<float> serializer(&output);
    for (ui32 i = 0; i < dimension; ++i)
        serializer.HandleElement(static_cast<float>(rng.GenRandReal1()) * 2.0f - 1.0f);
    serializer.Finish();
    return result;
}

ui64 Run(const std::string& snapshot, const Ydb::Table::VectorIndexSettings& settings,
        const std::vector<TString>& targets, size_t readers, size_t iterations,
        size_t maxIdle, size_t repeat) {
    TString error;
    std::istringstream input(snapshot, std::ios::binary);
    auto graph = THnswIndex::LoadGraph(input, settings, 4_GB, error);
    Y_ENSURE(graph, error);
    auto tracker = std::make_shared<THnswCacheMemoryTracker>();
    tracker->SetLimit(4_GB);
    const auto graphBytes = graph->EstimatedMemoryBytes();
    auto reservation = std::shared_ptr<void>(new int(0), [tracker, graphBytes](void* ptr) {
        delete static_cast<int*>(ptr);
        tracker->Release(graphBytes);
    });
    Y_ENSURE(tracker->TryAcquire(graphBytes));
    graph->SetGraphReservation(std::move(reservation), tracker, maxIdle);
    std::vector<std::unique_ptr<THnswIndex>> views;
    for (size_t i = 0; i < readers; ++i)
        views.push_back(graph->CreateView({10, 0}, std::make_shared<THnswIndexChanges>()));

    std::barrier barrier(readers + 1);
    std::vector<ui64> checksums(readers);
    std::vector<std::vector<double>> latencies(readers);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < readers; ++i) {
        latencies[i].reserve(iterations);
        threads.emplace_back([&, i] {
            const auto search = [&](size_t n) {
                const auto result = views[i]->Search(targets[(n + i) % targets.size()], 10, {10, 0}, 15);
                Y_ENSURE(result.Covered && result.Results.size() == 10);
                for (const auto& [key, _] : result.Results)
                    for (const char ch : key)
                        checksums[i] = checksums[i] * 31 + static_cast<unsigned char>(ch);
            };
            for (size_t n = 0; n < 300; ++n)
                search(n);
            barrier.arrive_and_wait(); // Warm-up complete, wait for the timer.
            barrier.arrive_and_wait();
            for (size_t n = 0; n < iterations; ++n) {
                const auto begin = std::chrono::steady_clock::now();
                search(n);
                const auto elapsed = std::chrono::steady_clock::now() - begin;
                latencies[i].push_back(std::chrono::duration<double, std::micro>(elapsed).count());
            }
            barrier.arrive_and_wait();
        });
    }
    barrier.arrive_and_wait();
    const auto before = graph->GetSearchWorkspaceStats();
    const auto cpuBegin = std::clock();
    const auto begin = std::chrono::steady_clock::now();
    barrier.arrive_and_wait();
    barrier.arrive_and_wait();
    const auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
    const auto cpuSeconds = double(std::clock() - cpuBegin) / CLOCKS_PER_SEC;
    for (auto& thread : threads)
        thread.join();
    const auto after = graph->GetSearchWorkspaceStats();
    ui64 checksum = 0;
    std::vector<double> allLatencies;
    for (size_t i = 0; i < readers; ++i) {
        checksum += checksums[i];
        allLatencies.insert(allLatencies.end(), latencies[i].begin(), latencies[i].end());
    }
    std::sort(allLatencies.begin(), allLatencies.end());
    const auto percentile = [&](size_t p) { return allLatencies[(allLatencies.size() - 1) * p / 100]; };
    std::cout << "{\"rows\":" << graph->Size()
        << ",\"dimension\":" << settings.vector_dimension()
        << ",\"readers\":" << readers << ",\"max_idle\":" << maxIdle
        << ",\"repeat\":" << repeat << ",\"queries\":" << readers * iterations
        << ",\"seconds\":" << seconds << ",\"cpu_seconds\":" << cpuSeconds
        << ",\"qps\":" << readers * iterations / seconds
        << ",\"p50_us\":" << percentile(50) << ",\"p95_us\":" << percentile(95)
        << ",\"p99_us\":" << percentile(99)
        << ",\"allocations\":" << after.Allocations - before.Allocations
        << ",\"allocated_bytes\":" << after.AllocatedBytes - before.AllocatedBytes
        << ",\"reuses\":" << after.Reuses - before.Reuses
        << ",\"frees\":" << after.Frees - before.Frees
        << ",\"admission_failures\":" << after.AllocationFailures - before.AllocationFailures
        << ",\"retained_bytes\":" << after.RetainedBytes
        << ",\"used_bytes\":" << tracker->GetUsed()
        << ",\"checksum\":" << checksum << "}" << std::endl;
    views.clear();
    graph.reset();
    Y_ENSURE(tracker->GetUsed() == 0);
    return checksum;
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc > 5) {
            std::cerr << "Usage: " << argv[0] << " [rows=100000] [queries_per_reader=10000] [readers=4] [repeats=3]\n";
            return 1;
        }
        const size_t rows = argc > 1 ? FromString<size_t>(argv[1]) : 100000;
        const size_t iterations = argc > 2 ? FromString<size_t>(argv[2]) : 10000;
        const size_t readers = argc > 3 ? FromString<size_t>(argv[3]) : 4;
        const size_t repeats = argc > 4 ? FromString<size_t>(argv[4]) : 3;
        Y_ENSURE(rows >= 10 && iterations && readers && repeats);
        Ydb::Table::VectorIndexSettings settings;
        settings.set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
        settings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.set_vector_dimension(200);
        TFastRng<ui64> rng(12345);
        std::vector<std::pair<TString, TString>> vectors;
        for (size_t i = 0; i < rows; ++i)
            vectors.emplace_back(ToString(i), RandomVector(rng, settings.vector_dimension()));
        std::vector<TString> targets;
        for (size_t i = 0; i < 1024; ++i)
            targets.push_back(RandomVector(rng, settings.vector_dimension()));
        TString error;
        auto graph = THnswIndex::Build(settings, vectors, 4_GB, error);
        Y_ENSURE(graph, error);
        vectors.clear();
        vectors.shrink_to_fit();
        std::ostringstream output(std::ios::binary);
        graph->SaveGraph(output);
        graph.reset();
        const auto snapshot = output.str();
        ui64 expectedChecksum = 0;
        bool first = true;
        for (size_t repeat = 0; repeat < repeats; ++repeat) {
            // Alternate order to reduce systematic warm-cache bias.
            for (size_t maxIdle : (repeat % 2 ? std::vector<size_t>{4, 1} : std::vector<size_t>{1, 4})) {
                const auto checksum = Run(snapshot, settings, targets, readers, iterations, maxIdle, repeat);
                if (!first)
                    Y_ENSURE(checksum == expectedChecksum, "Search results changed between policies");
                expectedChecksum = checksum;
                first = false;
            }
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
