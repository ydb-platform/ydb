#include <library/cpp/testing/gtest/gtest.h>

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
