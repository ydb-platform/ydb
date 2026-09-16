#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace NMemoryBenchmark {

struct TOptions {
    uint32_t Threads = 0;
    uint32_t RandomPercent = 0;
    std::string RandomMode = "copy";
    uint64_t BufferSize = 256ull << 20;
    uint64_t PartSize = 2ull << 20;
    uint32_t DurationMs = 3000;
};

TOptions ParseOptions(int argc, char** argv);

constexpr std::ptrdiff_t BarrierParticipantCount(uint32_t threads) {
    return static_cast<std::ptrdiff_t>(threads) + 1;
}

} // namespace NMemoryBenchmark
