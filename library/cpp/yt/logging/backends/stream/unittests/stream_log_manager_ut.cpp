#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/logging/backends/stream/stream_log_manager.h>

#include <util/stream/str.h>

#include <util/string/split.h>

namespace NYT::NLogging {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TStreamLogManagerTest, Simple)
{
    TString str;
    {
        TStringOutput output(str);
        auto logManager = CreateStreamLogManager(&output);
        TLogger Logger(logManager.get(), "Test");
        YT_LOG_INFO("Hello world");
    }

    TVector<TStringBuf> tokens;
    Split(str, "\t", tokens);
    EXPECT_GE(std::ssize(tokens), 4);
    EXPECT_EQ(tokens[1], "I");
    EXPECT_EQ(tokens[2], "Test");
    EXPECT_EQ(tokens[3], "Hello world");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
