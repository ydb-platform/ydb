#include <library/cpp/testing/unittest/registar.h>

#include <yt/cpp/mapreduce/interface/config.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(ConfigSuite)
{
    Y_UNIT_TEST(TestReset) {
        // very limited test, checks only one config field

        auto origConfig = *TConfig::Get();
        TConfig::Get()->Reset();
        UNIT_ASSERT_VALUES_EQUAL(origConfig.Hosts, TConfig::Get()->Hosts);

        TConfig::Get()->Hosts = "hosts/fb867";
        TConfig::Get()->Reset();
        UNIT_ASSERT_VALUES_EQUAL(origConfig.Hosts, TConfig::Get()->Hosts);
    }
}
