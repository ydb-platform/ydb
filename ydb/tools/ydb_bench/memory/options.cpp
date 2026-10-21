#include "options.h"

#include <limits>
#include <stdexcept>

namespace NMemoryBenchmark {
namespace {

uint64_t ParseUnsigned(const char* value, const char* name) {
    if (!*value) {
        throw std::runtime_error(std::string("invalid value for ") + name + ": expected an unsigned integer");
    }

    uint64_t result = 0;
    for (const char* cursor = value; *cursor; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            throw std::runtime_error(std::string("invalid value for ") + name + ": expected an unsigned integer");
        }
        const uint64_t digit = *cursor - '0';
        if (result > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
            throw std::runtime_error(std::string("value for ") + name + " is out of range");
        }
        result = result * 10 + digit;
    }
    return result;
}

uint32_t ParseUint32(const char* value, const char* name) {
    const uint64_t result = ParseUnsigned(value, name);
    if (result > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error(std::string("value for ") + name + " is out of range");
    }
    return result;
}

uint64_t ParseBytes(const char* value, const char* name, uint64_t multiplier) {
    const uint64_t result = ParseUnsigned(value, name);
    if (result > std::numeric_limits<uint64_t>::max() / multiplier) {
        throw std::runtime_error(std::string("value for ") + name + " overflows bytes");
    }
    return result * multiplier;
}

} // namespace

TOptions ParseOptions(int argc, char** argv) {
    TOptions result;
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 == argc) {
            throw std::runtime_error("option without a value");
        }
        const std::string name = argv[i];
        const char* value = argv[i + 1];
        if (name == "--threads") {
            result.Threads = ParseUint32(value, "--threads");
        } else if (name == "--random-percent") {
            result.RandomPercent = ParseUint32(value, "--random-percent");
        } else if (name == "--random-mode") {
            result.RandomMode = value;
        } else if (name == "--buffer-size-mb") {
            result.BufferSize = ParseBytes(value, "--buffer-size-mb", 1ull << 20);
        } else if (name == "--part-size-kb") {
            result.PartSize = ParseBytes(value, "--part-size-kb", 1ull << 10);
        } else if (name == "--duration-ms") {
            result.DurationMs = ParseUint32(value, "--duration-ms");
        } else {
            throw std::runtime_error("unknown option: " + name);
        }
    }
    if (!result.Threads) {
        throw std::runtime_error("threads must be positive");
    }
    if (!result.DurationMs) {
        throw std::runtime_error("duration-ms must be positive");
    }
    if (result.RandomPercent > 100) {
        throw std::runtime_error("random-percent must be between 0 and 100");
    }
    if (result.RandomMode != "copy" && result.RandomMode != "write") {
        throw std::runtime_error("random-mode must be copy or write");
    }
    if (result.BufferSize < 2 || result.PartSize == 0 || result.PartSize > result.BufferSize / 2) {
        throw std::runtime_error("invalid buffer/part size");
    }
    return result;
}

} // namespace NMemoryBenchmark
