#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NLogging {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticAnalysis, ValidFormats)
{
    // Mock for actual error -- we only care that
    // it is some runtime object.
    [[maybe_unused]] struct TError
    { } error;

    YT_LOG_CHECK_FORMAT("Hello");
    YT_LOG_CHECK_FORMAT("Hello %v", "World!");
    YT_LOG_CHECK_FORMAT("Hello %qv", "World!");
    YT_LOG_CHECK_FORMAT(error);
    YT_LOG_CHECK_FORMAT(error, "Hello");
    YT_LOG_CHECK_FORMAT(error, "Hello %Qhs", "World!");
    YT_LOG_CHECK_FORMAT("Hello %%");
    YT_LOG_CHECK_FORMAT("Hello %" PRIu64, 42);
}

// Uncomment this test to see that we don't have false negatives!
// TEST(TStaticAnalysis, InvalidFormats)
// {
//     YT_LOG_CHECK_FORMAT("Hello", 1);
//     YT_LOG_CHECK_FORMAT("Hello %");
//     YT_LOG_CHECK_FORMAT("Hello %false");
//     YT_LOG_CHECK_FORMAT("Hello ", "World");
//     YT_LOG_CHECK_FORMAT("Hello ", "(World: %v)", 42);
//     YT_LOG_CHECK_FORMAT("Hello %lbov", 42); // There is no 'b' flag.
// }

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
