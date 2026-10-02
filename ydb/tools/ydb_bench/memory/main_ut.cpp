#include <library/cpp/testing/gtest/gtest.h>

#include "options.h"

#include <initializer_list>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace NMemoryBenchmark {
namespace {

TOptions Parse(std::initializer_list<const char*> options) {
    std::vector<char*> argv;
    argv.reserve(options.size() + 1);
    argv.emplace_back(const_cast<char*>("memory_benchmark"));
    for (const char* option : options) {
        argv.emplace_back(const_cast<char*>(option));
    }
    return ParseOptions(argv.size(), argv.data());
}

void AssertInvalid(std::initializer_list<const char*> options, const std::string& expectedError) {
    try {
        Parse(options);
        FAIL() << "expected std::runtime_error containing: " << expectedError;
    } catch (const std::runtime_error& error) {
        EXPECT_NE(std::string(error.what()).find(expectedError), std::string::npos)
            << "actual error: " << error.what();
    } catch (...) {
        FAIL() << "expected std::runtime_error containing: " << expectedError;
    }
}

} // namespace

TEST(TMemoryBenchmarkOptions, RejectsTrailingGarbageAndSigns) {
    for (const char* option : {"--threads", "--random-percent", "--buffer-size-mb", "--part-size-kb", "--duration-ms"}) {
        AssertInvalid({"--threads", "1", option, "1garbage"}, "expected an unsigned integer");
    }
    AssertInvalid({"--threads", "-1"}, "invalid value for --threads");
    AssertInvalid({"--threads", "+1"}, "invalid value for --threads");
}

TEST(TMemoryBenchmarkOptions, RejectsValuesOutsideTheirStorageRange) {
    for (const char* option : {"--threads", "--random-percent", "--duration-ms"}) {
        AssertInvalid({"--threads", "1", option, "4294967296"}, "value for " + std::string(option) + " is out of range");
    }
    AssertInvalid({"--threads", "18446744073709551616"}, "value for --threads is out of range");
}

TEST(TMemoryBenchmarkOptions, RejectsByteConversionOverflow) {
    AssertInvalid({"--threads", "1", "--buffer-size-mb", "17592186044416"}, "value for --buffer-size-mb overflows bytes");
    AssertInvalid({"--threads", "1", "--part-size-kb", "18014398509481984"}, "value for --part-size-kb overflows bytes");
}

TEST(TMemoryBenchmarkOptions, AcceptsUint32Boundaries) {
    const TOptions options = Parse({
        "--threads", "4294967295",
        "--random-percent", "100",
        "--buffer-size-mb", "1",
        "--part-size-kb", "1",
        "--duration-ms", "4294967295",
    });
    EXPECT_EQ(options.Threads, std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(options.DurationMs, std::numeric_limits<uint32_t>::max());
}

TEST(TMemoryBenchmarkOptions, RejectsZeroDuration) {
    AssertInvalid({"--threads", "1", "--duration-ms", "0"}, "duration-ms must be positive");
}

TEST(TMemoryBenchmarkRuntime, PreservesBarrierParticipantCountForMaximumThreads) {
    EXPECT_EQ(
        BarrierParticipantCount(std::numeric_limits<uint32_t>::max()),
        static_cast<std::ptrdiff_t>(std::numeric_limits<uint32_t>::max()) + 1);
}

} // namespace NMemoryBenchmark
