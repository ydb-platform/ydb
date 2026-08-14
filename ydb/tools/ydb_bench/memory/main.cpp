#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <iomanip>

#include "options.h"

namespace {
using NMemoryBenchmark::TOptions;

struct alignas(64) TStats {
    uint64_t Operations = 0;
    uint64_t PayloadBytes = 0;
    uint64_t ReadBytes = 0;
    uint64_t WrittenBytes = 0;
    uint64_t Checksum = 0;
};

struct TValues {
    double Operations = 0, PayloadBytes = 0, ReadBytes = 0, WrittenBytes = 0;
};

TValues Values(const TStats& stats) {
    return {double(stats.Operations), double(stats.PayloadBytes), double(stats.ReadBytes), double(stats.WrittenBytes)};
}

TValues Aggregate(const std::vector<TValues>& values, const std::string& aggregation) {
    TValues result;
    auto field = [&](auto getter) {
        std::vector<double> items; for (const auto& value : values) items.push_back(getter(value));
        std::sort(items.begin(), items.end());
        if (aggregation == "sum") { double total = 0; for (double value : items) total += value; return total; }
        if (aggregation == "min") return items.front();
        if (aggregation == "max") return items.back();
        if (aggregation == "mean") { double total = 0; for (double value : items) total += value; return total / items.size(); }
        const size_t middle = items.size() / 2; return items.size() % 2 ? items[middle] : (items[middle - 1] + items[middle]) / 2;
    };
    result.Operations = field([](const TValues& value) { return value.Operations; });
    result.PayloadBytes = field([](const TValues& value) { return value.PayloadBytes; });
    result.ReadBytes = field([](const TValues& value) { return value.ReadBytes; });
    result.WrittenBytes = field([](const TValues& value) { return value.WrittenBytes; });
    return result;
}

void PrintValues(const TValues& value, double elapsed) {
    const double mb = 1000000.0;
    std::cout << value.Operations << ',' << value.PayloadBytes << ',' << value.ReadBytes << ',' << value.WrittenBytes << ','
              << value.Operations / elapsed << ',' << value.PayloadBytes / elapsed / mb << ',' << value.ReadBytes / elapsed / mb << ','
              << value.WrittenBytes / elapsed / mb << ',' << (value.ReadBytes + value.WrittenBytes) / elapsed / mb;
}

uint64_t AvailableMemoryBytes() {
    std::ifstream input("/proc/meminfo"); std::string name, unit; uint64_t value;
    while (input >> name >> value >> unit) {
        if (name == "MemAvailable:") return value * 1024;
    }
    return 0;
}

uint32_t RandomThreads(uint32_t threads, uint32_t percent) {
    if (percent == 0) return 0;
    if (percent == 100) return threads;
    uint32_t value = (threads * percent + 50) / 100;
    if (threads > 1) value = std::min(threads - 1, std::max(1u, value));
    return value;
}

void Worker(uint32_t index, bool random, const TOptions& options, std::barrier<>& ready,
            std::atomic<bool>& stop, TStats& stats) {
    std::unique_ptr<char[]> buffer(new char[options.BufferSize]());
    const uint64_t half = options.BufferSize / 2;
    uint64_t state = 0x629fa923u ^ (uint64_t(index + 1) * 0x9e3779b97f4a7c15ull);
    ready.arrive_and_wait();
    if (!random) {
        uint64_t offset = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            std::memcpy(buffer.get() + half + offset, buffer.get() + offset, options.PartSize);
            stats.Operations++; stats.PayloadBytes += options.PartSize;
            stats.ReadBytes += options.PartSize; stats.WrittenBytes += options.PartSize;
            offset += options.PartSize;
            if (offset + options.PartSize > half) offset = 0;
        }
    } else {
        while (!stop.load(std::memory_order_relaxed)) {
            for (uint32_t i = 0; i < 1024; ++i) {
                state = state * 6364136223846793005ull + 1442695040888963407ull;
                const uint64_t offset = state % half;
                if (options.RandomMode == "copy") {
                    buffer[half + offset] = buffer[offset]; stats.ReadBytes++;
                } else {
                    buffer[half + offset] = static_cast<char>(state);
                }
                stats.Operations++; stats.PayloadBytes++; stats.WrittenBytes++;
            }
        }
    }
    stats.Checksum = static_cast<unsigned char>(buffer[state % options.BufferSize]);
}
}

int main(int argc, char** argv) {
    try {
        const NMemoryBenchmark::TOptions options = NMemoryBenchmark::ParseOptions(argc, argv);
        if (options.BufferSize > std::numeric_limits<uint64_t>::max() / options.Threads) throw std::runtime_error("requested buffer footprint overflows uint64");
        const uint64_t requiredMemory = options.BufferSize * options.Threads;
        const uint64_t availableMemory = AvailableMemoryBytes();
        if (availableMemory && requiredMemory > availableMemory * 8 / 10) {
            throw std::runtime_error("requested worker buffers exceed 80% of MemAvailable");
        }
        const uint32_t randomThreads = RandomThreads(options.Threads, options.RandomPercent);
        const uint32_t sequentialThreads = options.Threads - randomThreads;
        std::vector<TStats> stats(options.Threads); std::vector<std::thread> workers;
        std::barrier ready(NMemoryBenchmark::BarrierParticipantCount(options.Threads)); std::atomic<bool> stop = false;
        for (uint32_t i = 0; i < options.Threads; ++i) {
            // Interleave both workloads deterministically over the selected CPU set.
            const bool random = ((uint64_t(i + 1) * randomThreads) / options.Threads != (uint64_t(i) * randomThreads) / options.Threads);
            workers.emplace_back(Worker, i, random, std::cref(options), std::ref(ready), std::ref(stop), std::ref(stats[i]));
        }
        ready.arrive_and_wait(); const auto start = std::chrono::steady_clock::now();
        std::this_thread::sleep_for(std::chrono::milliseconds(options.DurationMs));
        stop.store(true, std::memory_order_relaxed);
        for (auto& worker : workers) worker.join();
        const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        std::vector<TValues> sequential, random, all;
        for (uint32_t i = 0; i < options.Threads; ++i) {
            const bool isRandom = ((uint64_t(i + 1) * randomThreads) / options.Threads != (uint64_t(i) * randomThreads) / options.Threads);
            const TValues item = Values(stats[i]); (isRandom ? random : sequential).push_back(item); all.push_back(item);
        }
        std::cout << std::setprecision(17);
        std::cout << "threads,random_percent,random_mode,buffer_size_mb,part_size_kb,sequential_threads,random_threads,scope,worker_aggregation,operations,payload_bytes,read_bytes,written_bytes,ops_per_sec,payload_mb_per_sec,read_mb_per_sec,write_mb_per_sec,memory_traffic_mb_per_sec,elapsed_seconds\n";
        for (const auto& group : std::vector<std::pair<std::string, const std::vector<TValues>*>>{{"sequential", &sequential}, {"random", &random}, {"all", &all}}) {
            if (group.second->empty()) continue;
            for (const std::string aggregation : {"sum", "min", "max", "median", "mean"}) {
                std::cout << options.Threads << ',' << options.RandomPercent << ',' << options.RandomMode << ',' << options.BufferSize / (1 << 20) << ',' << options.PartSize / (1 << 10) << ',' << sequentialThreads << ',' << randomThreads << ',' << group.first << ',' << aggregation << ',';
                PrintValues(Aggregate(*group.second, aggregation), elapsed); std::cout << ',' << elapsed << '\n';
            }
        }
        std::cout << "workers.csv\nworker,scope,operations,payload_bytes,read_bytes,written_bytes,ops_per_sec,payload_mb_per_sec,read_mb_per_sec,write_mb_per_sec,memory_traffic_mb_per_sec\n";
        for (uint32_t i = 0; i < options.Threads; ++i) {
            const bool isRandom = ((uint64_t(i + 1) * randomThreads) / options.Threads != (uint64_t(i) * randomThreads) / options.Threads);
            std::cout << i << ',' << (isRandom ? "random" : "sequential") << ','; PrintValues(Values(stats[i]), elapsed); std::cout << '\n';
        }
        return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 2; }
}
