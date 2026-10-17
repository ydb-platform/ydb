#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/dns/dns_resolver.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDns {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TDnsResolveOptionsTest, SerializationRoundtrip)
{
    for (auto options : {
        TDnsResolveOptions{.EnableIPv4 = true, .EnableIPv6 = true},
        TDnsResolveOptions{.EnableIPv4 = true, .EnableIPv6 = false},
        TDnsResolveOptions{.EnableIPv4 = false, .EnableIPv6 = true},
    }) {
        EXPECT_EQ(ConvertTo<TDnsResolveOptions>(ConvertToYsonString(options)), options);
    }
}

TEST(TDnsResolveOptionsTest, DeserializeMissingFieldsAsDefaults)
{
    EXPECT_EQ(ConvertTo<TDnsResolveOptions>(TYsonStringBuf("{}")), TDnsResolveOptions{});
    EXPECT_EQ(
        ConvertTo<TDnsResolveOptions>(TYsonStringBuf("{enable_ipv6=%false}")),
        (TDnsResolveOptions{.EnableIPv4 = true, .EnableIPv6 = false}));
}

TEST(TDnsResolveOptionsTest, DeserializeBothFamiliesDisabled)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TDnsResolveOptions>(TYsonStringBuf("{enable_ipv4=%false;enable_ipv6=%false}")),
        "At least one of");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDns
