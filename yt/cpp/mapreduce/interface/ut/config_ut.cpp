#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/config.h>

using namespace NYT;

TEST(TConfigTest, Reset) {
    // Very limited test, checks only one config field.

    auto origConfig = *TConfig::Get();
    TConfig::Get()->Reset();
    EXPECT_EQ(origConfig.Hosts, TConfig::Get()->Hosts);

    TConfig::Get()->Hosts = "hosts/fb867";
    TConfig::Get()->Reset();
    EXPECT_EQ(origConfig.Hosts, TConfig::Get()->Hosts);
}

TEST(TConfigTest, ConfigToString) {
    const auto& config = *TConfig::Get();

    auto configString = ConfigToYsonString(config);

    // Some checks for config parameters with different types
    EXPECT_TRUE(configString.Contains("log_exclude_categories"));
    EXPECT_TRUE(configString.Contains("accept_encoding"));
    EXPECT_TRUE(configString.Contains("wait_lock_poll_interval"));
}

TEST(TConfigTest, ConfigFromString) {
    const auto& origConfig = *TConfig::Get();

    auto configString = ConfigToYsonString(origConfig);

    auto config = ConfigFromYsonString(configString);

    EXPECT_EQ(origConfig.LogExcludeCategories, config.LogExcludeCategories);
    EXPECT_EQ(origConfig.AcceptEncoding, config.AcceptEncoding);
    EXPECT_EQ(origConfig.WaitLockPollInterval, config.WaitLockPollInterval);
}
