#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/error/error.h>

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

TEST(TLogger, LogAlertAndThrowMessage)
{
    try {
        TLogger Logger;
        YT_LOG_ALERT_AND_THROW("Alert message (Arg1: %v, Arg2: %v)",
            1,
            2);
        EXPECT_TRUE(false);
    } catch (const TErrorException& ex) {
        const auto& error = ex.Error();
        EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Fatal);
        EXPECT_EQ(error.GetMessage(), "Malformed request or incorrect state detected");
        EXPECT_EQ(error.Attributes().Get<std::string>("message"), "Alert message (Arg1: 1, Arg2: 2)");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
