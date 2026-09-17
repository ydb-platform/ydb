#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>
#include <sys/resource.h>

extern "C" void* TCMallocInternalMalloc(size_t);
extern "C" void TCMallocInternalFree(void*) noexcept;

int main(int argc, char** argv) {
    const size_t threads = argc > 1 ? strtoul(argv[1], nullptr, 10) : 1;
    const size_t iterations = argc > 2 ? strtoul(argv[2], nullptr, 10) : 200000;
    if (!threads || threads > 128 || !iterations) return 2;
    std::atomic<size_t> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> workers;
    std::vector<std::vector<long long>> latencies(threads);
    for (size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            latencies[t].reserve(iterations / 512 + 64);
            uint64_t random = 0x123456789abcdefULL + t;
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) {}
            for (size_t i = 0; i < iterations; ++i) {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                const bool sample = (random & 1023) == 0;
                const auto start = sample ? std::chrono::steady_clock::now() :
                    std::chrono::steady_clock::time_point{};
                const size_t sizes[] = {16, 32, 64, 128, 512, 4096};
                const size_t size = sizes[i % 6];
                void* p = TCMallocInternalMalloc(size);
                if (!p) abort();
                auto* bytes = static_cast<volatile unsigned char*>(p);
                bytes[0] = 42;
                bytes[size - 1] = 43;
                TCMallocInternalFree(p);
                if (sample) latencies[t].push_back(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - start).count());
            }
        });
    }
    while (ready.load() != threads) {}
    const auto start = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    std::vector<long long> samples;
    for (const auto& v : latencies) samples.insert(samples.end(), v.begin(), v.end());
    std::sort(samples.begin(), samples.end());
    rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    const auto reserved = tcmalloc::MallocExtension::GetNumericProperty("tcmalloc.generation.reserved_bytes");
    printf("threads=%zu operations=%zu seconds=%.6f ns_per_pair=%.1f sample_p99_ns=%lld maxrss_kib=%ld reserved=%zu\n",
           threads, threads * iterations, seconds, seconds * 1e9 / (threads * iterations),
           samples.empty() ? 0 : samples[samples.size() * 99 / 100], usage.ru_maxrss, reserved.value_or(0));
}
