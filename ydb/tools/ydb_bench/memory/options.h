#pragma once

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

} // namespace NMemoryBenchmark
