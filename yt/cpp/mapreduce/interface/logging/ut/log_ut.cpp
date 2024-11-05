#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/string/cast.h>

using namespace NYT;

TEST(TLoggingTest, FromString) {
    EXPECT_EQ(FromString("error"), ILogger::ELevel::ERROR);
    EXPECT_EQ(FromString("warning"), ILogger::ELevel::ERROR);
    EXPECT_EQ(FromString("info"), ILogger::ELevel::INFO);
    EXPECT_EQ(FromString("debug"), ILogger::ELevel::DEBUG);
    EXPECT_EQ(FromString("ERROR"), ILogger::ELevel::ERROR);
    EXPECT_EQ(FromString("WARNING"), ILogger::ELevel::ERROR);
    EXPECT_EQ(FromString("INFO"), ILogger::ELevel::INFO);
    EXPECT_EQ(FromString("DEBUG"), ILogger::ELevel::DEBUG);
    EXPECT_THROW(FromString<ILogger::ELevel>("no"), yexception);
}
