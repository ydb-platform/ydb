#include <library/cpp/testing/unittest/registar.h>

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
    UNIT_ASSERT_EXCEPTION_CONTAINS(Parse(options), std::runtime_error, expectedError);
}

} // namespace

Y_UNIT_TEST_SUITE(TMemoryBenchmarkOptions) {

Y_UNIT_TEST(RejectsTrailingGarbageAndSigns) {
    for (const char* option : {"--threads", "--random-percent", "--buffer-size-mb", "--part-size-kb", "--duration-ms"}) {
        AssertInvalid({"--threads", "1", option, "1garbage"}, "expected an unsigned integer");
    }
    AssertInvalid({"--threads", "-1"}, "invalid value for --threads");
    AssertInvalid({"--threads", "+1"}, "invalid value for --threads");
}

Y_UNIT_TEST(RejectsValuesOutsideTheirStorageRange) {
    for (const char* option : {"--threads", "--random-percent", "--duration-ms"}) {
        AssertInvalid({"--threads", "1", option, "4294967296"}, "value for " + std::string(option) + " is out of range");
    }
    AssertInvalid({"--threads", "18446744073709551616"}, "value for --threads is out of range");
}

Y_UNIT_TEST(RejectsByteConversionOverflow) {
    AssertInvalid({"--threads", "1", "--buffer-size-mb", "17592186044416"}, "value for --buffer-size-mb overflows bytes");
    AssertInvalid({"--threads", "1", "--part-size-kb", "18014398509481984"}, "value for --part-size-kb overflows bytes");
}

Y_UNIT_TEST(AcceptsUint32Boundaries) {
    const TOptions options = Parse({
        "--threads", "4294967295",
        "--random-percent", "100",
        "--buffer-size-mb", "1",
        "--part-size-kb", "1",
        "--duration-ms", "4294967295",
    });
    UNIT_ASSERT_EQUAL(options.Threads, std::numeric_limits<uint32_t>::max());
    UNIT_ASSERT_EQUAL(options.DurationMs, std::numeric_limits<uint32_t>::max());
}

} // Y_UNIT_TEST_SUITE

} // namespace NMemoryBenchmark
