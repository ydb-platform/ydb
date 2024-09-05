#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/api/rpc_proxy/private.h>

namespace NYT::NApi::NRpcProxy {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TProxyUrlTest
    : public ::testing::Test
{ };

TEST_F(TProxyUrlTest, ParseProxyUrlAliasingRules)
{
    auto emptyRules = ParseProxyUrlAliasingRules("");
    ASSERT_EQ(emptyRules.size(), 0u);
    auto notEmptyRules = ParseProxyUrlAliasingRules(R"({primary="localhost:12345"})");
    ASSERT_EQ(notEmptyRules.size(), 1u);
    ASSERT_EQ(notEmptyRules.at("primary"), "localhost:12345");
}

TEST_F(TProxyUrlTest, ApplyProxyUrlAliasingRules)
{
    {
        TString url = "markov";
        ApplyProxyUrlAliasingRules(url, THashMap<std::string, std::string>({{"primary", "localhost:12345"}}));
        ASSERT_EQ(url, "markov");
    }
    // See ENV in ya.make
    {
        TString url = "primary";
        ApplyProxyUrlAliasingRules(url, THashMap<std::string, std::string>({{"primary", "localhost:12345"}}));
        ASSERT_EQ(url, "localhost:12345");
    }
}

TEST_F(TProxyUrlTest, NormalizeHttpProxyUrl)
{
    ASSERT_EQ(NormalizeHttpProxyUrl("markov"), "http://markov.yt.yandex.net");
    // See ENV in ya.make
    ASSERT_EQ(
        NormalizeHttpProxyUrl("primary", THashMap<std::string, std::string>({{"primary", "localhost:12345"}})),
        "http://localhost:12345");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi::NRpcProxy
