#include <atomic>
#include <barrier>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace {

std::atomic<bool> Stop = false;

struct alignas(64) TSharedLine {
    std::atomic<uint64_t> Turn = 0;
};

struct alignas(64) TStats {
    uint64_t Operations = 0;
    uint64_t Bytes = 0;
    uint64_t Migrations = 0;
    int LastCpu = -1;
};

struct TOptions {
    std::string Mode;
    uint32_t Threads = 0;
    std::vector<int> Cpus;
    std::vector<uint32_t> GroupSizes;
};

std::vector<int> ParseCpuList(const std::string& value) {
    std::vector<int> result;
    size_t offset = 0;
    while (offset < value.size()) {
        const size_t comma = value.find(',', offset);
        result.push_back(std::stoi(value.substr(offset, comma - offset)));
        if (comma == std::string::npos) {
            break;
        }
        offset = comma + 1;
    }
    return result;
}

std::vector<uint32_t> ParseGroupSizes(const std::string& value) {
    std::vector<uint32_t> result;
    for (int item : ParseCpuList(value)) {
        if (item < 2) {
            throw std::runtime_error("coherence group must contain at least two workers");
        }
        result.push_back(static_cast<uint32_t>(item));
    }
    return result;
}

TOptions ParseOptions(int argc, char** argv) {
    TOptions result;
    for (int index = 1; index < argc; index += 2) {
        if (index + 1 == argc) {
            throw std::runtime_error("option without a value");
        }
        const std::string name = argv[index];
        const std::string value = argv[index + 1];
        if (name == "--mode") {
            result.Mode = value;
        } else if (name == "--threads") {
            result.Threads = std::stoul(value);
        } else if (name == "--cpus") {
            result.Cpus = ParseCpuList(value);
        } else if (name == "--groups") {
            result.GroupSizes = ParseGroupSizes(value);
        } else {
            throw std::runtime_error("unknown option: " + name);
        }
    }
    if (result.Mode != "memory-bandwidth" && result.Mode != "coherence-chiplet" &&
        result.Mode != "coherence-numa" && result.Mode != "coherence-all-numa") {
        throw std::runtime_error("unknown background mode: " + result.Mode);
    }
    if (!result.Threads) {
        throw std::runtime_error("threads must be positive");
    }
    if (!result.Cpus.empty() && result.Cpus.size() != result.Threads) {
        throw std::runtime_error("cpu count must match threads");
    }
    if (result.Mode != "memory-bandwidth") {
        if (result.GroupSizes.empty()) {
            result.GroupSizes.push_back(result.Threads);
        }
        uint32_t groupedThreads = 0;
        for (uint32_t size : result.GroupSizes) {
            groupedThreads += size;
        }
        if (groupedThreads != result.Threads) {
            throw std::runtime_error("coherence group sizes must cover all threads");
        }
    }
    return result;
}

void PinThread(int cpu) {
#ifdef __linux__
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0) {
        throw std::runtime_error("cannot set worker affinity");
    }
#else
    (void)cpu;
    throw std::runtime_error("worker affinity is unsupported");
#endif
}

int CurrentCpu() {
#ifdef __linux__
    return sched_getcpu();
#else
    return -1;
#endif
}

void SampleCpu(TStats& stats) {
    const int cpu = CurrentCpu();
    if (stats.LastCpu >= 0 && cpu >= 0 && cpu != stats.LastCpu) {
        ++stats.Migrations;
    }
    stats.LastCpu = cpu;
}

void ClobberMemory() {
#if defined(__GNUC__) || defined(__clang__)
    asm volatile("" : : : "memory");
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

void MemoryWorker(uint32_t index, const TOptions& options, std::barrier<>& ready, TStats& stats) {
    if (!options.Cpus.empty()) {
        PinThread(options.Cpus[index]);
    }
    constexpr size_t BufferSize = 64ull << 20;
    constexpr size_t PartSize = 2ull << 20;
    std::unique_ptr<char[]> buffer(new char[BufferSize]());
    ready.arrive_and_wait();
    size_t offset = 0;
    while (!Stop.load(std::memory_order_relaxed)) {
        std::memcpy(buffer.get() + BufferSize / 2 + offset, buffer.get() + offset, PartSize);
        // The copied bytes are the load itself.  Make the resulting memory
        // state observable so an optimizing compiler cannot remove the copy
        // as a dead store.
        ClobberMemory();
        ++stats.Operations;
        stats.Bytes += PartSize * 2;
        offset = (offset + PartSize) % (BufferSize / 2);
        if ((stats.Operations & 63) == 0) {
            SampleCpu(stats);
        }
    }
    SampleCpu(stats);
}

void CoherenceWorker(uint32_t index, const TOptions& options, std::barrier<>& ready,
                     uint32_t group, uint32_t position, std::vector<TSharedLine>& lines, TStats& stats) {
    if (!options.Cpus.empty()) {
        PinThread(options.Cpus[index]);
    }
    const uint32_t groupSize = options.GroupSizes[group];
    TSharedLine& line = lines[group];
    ready.arrive_and_wait();
    while (!Stop.load(std::memory_order_relaxed)) {
        uint64_t expected = line.Turn.load(std::memory_order_acquire);
        if (expected % groupSize != position) {
            continue;
        }
        if (line.Turn.compare_exchange_weak(expected, expected + 1, std::memory_order_acq_rel)) {
            ++stats.Operations;
            stats.Bytes += 64;
            if ((stats.Operations & 65535) == 0) {
                SampleCpu(stats);
            }
        }
    }
    SampleCpu(stats);
}

void HandleSignal(int) {
    Stop.store(true, std::memory_order_relaxed);
}

} // namespace

int main(int argc, char** argv) {
    try {
        const TOptions options = ParseOptions(argc, argv);
        std::signal(SIGINT, HandleSignal);
        std::signal(SIGTERM, HandleSignal);
        std::barrier ready(static_cast<std::ptrdiff_t>(options.Threads + 1));
        std::vector<TStats> stats(options.Threads);
        std::vector<TSharedLine> lines(options.GroupSizes.size());
        std::vector<std::thread> workers;
        workers.reserve(options.Threads);
        uint32_t group = 0;
        uint32_t groupStart = 0;
        try {
            for (uint32_t index = 0; index < options.Threads; ++index) {
                if (options.Mode == "memory-bandwidth") {
                    workers.emplace_back(MemoryWorker, index, std::cref(options), std::ref(ready), std::ref(stats[index]));
                } else {
                    while (index >= groupStart + options.GroupSizes[group]) {
                        groupStart += options.GroupSizes[group++];
                    }
                    workers.emplace_back(CoherenceWorker, index, std::cref(options), std::ref(ready), group,
                                         index - groupStart,
                                         std::ref(lines), std::ref(stats[index]));
                }
            }
        } catch (...) {
            Stop.store(true, std::memory_order_relaxed);
            for (uint32_t index = static_cast<uint32_t>(workers.size()); index < options.Threads; ++index) {
                ready.arrive_and_drop();
            }
            ready.arrive_and_wait();
            for (auto& worker : workers) {
                worker.join();
            }
            throw;
        }
        ready.arrive_and_wait();
        const auto started = std::chrono::steady_clock::now();
        std::cout << "READY" << std::endl;
        for (auto& worker : workers) {
            worker.join();
        }
        const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
        uint64_t operations = 0;
        uint64_t bytes = 0;
        uint64_t migrations = 0;
        for (const auto& item : stats) {
            operations += item.Operations;
            bytes += item.Bytes;
            migrations += item.Migrations;
        }
        std::cout << "mode,workers,operations,bytes,operations_per_second,mb_per_second,cpu_migrations,elapsed_seconds\n";
        std::cout << options.Mode << ',' << options.Threads << ',' << operations << ',' << bytes << ','
                  << operations / elapsed << ',' << bytes / elapsed / 1000000.0 << ',' << migrations << ',' << elapsed
                  << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
}
