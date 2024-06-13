#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NLogging {
namespace {

////////////////////////////////////////////////////////////////////////////////

const TLogger Logger{};

TEST(TStaticAnalysisTest, ValidFormats)
{
    YT_LOG_INFO("Hello");
    YT_LOG_INFO("Hello %v", "World!");
    YT_LOG_INFO("Hello %qv", "World!");
    YT_LOG_INFO(42);
    YT_LOG_INFO("Hello %%");
    YT_LOG_INFO("Hello %" PRIu64, 42);

    TStringBuf msg = "Hello";
    YT_LOG_INFO(msg);
}

// Uncomment this test to see that we don't have false negatives!
// TEST(TStaticAnalysisTest, InvalidFormats)
// {
//     YT_LOG_INFO("Hello", 1);
//     YT_LOG_INFO("Hello %");
//     YT_LOG_INFO("Hello %false");
//     YT_LOG_INFO("Hello ", "World");
//     YT_LOG_INFO("Hello ", "(World: %v)", 42);
//     YT_LOG_INFO("Hello %lbov", 42); // There is no 'b' flag.
// }

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
