#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NLogging {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TLogger, NullByDefault)
{
    {
        TLogger logger;
        EXPECT_FALSE(logger);
        EXPECT_FALSE(logger.IsLevelEnabled(ELogLevel::Fatal));
    }
    {
        TLogger logger{"Category"};
        EXPECT_FALSE(logger);
        EXPECT_FALSE(logger.IsLevelEnabled(ELogLevel::Fatal));
    }
}

TEST(TLogger, CopyOfNullLogger)
{
    TLogger nullLogger{/*logManager*/ nullptr, "Category"};
    ASSERT_FALSE(nullLogger);

    auto logger = nullLogger.WithMinLevel(ELogLevel::Debug);

    EXPECT_FALSE(logger);
    EXPECT_FALSE(logger.IsLevelEnabled(ELogLevel::Fatal));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
