#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/endpoint_address.h>

namespace NYT::NRpc {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TEndpointAddressTest, ParseWithProtocol)
{
    auto parsed = ParseEndpointAddress("yt-tcp://localhost:9000");
    EXPECT_EQ("yt-tcp", parsed.Protocol);
    EXPECT_EQ("localhost:9000", parsed.Address);
}

TEST(TEndpointAddressTest, ParseWithoutProtocol)
{
    auto parsed = ParseEndpointAddress("localhost:9000");
    EXPECT_EQ(DefaultProtocolName, parsed.Protocol);
    EXPECT_EQ("localhost:9000", parsed.Address);
}

TEST(TEndpointAddressTest, ParseEmptyAddress)
{
    auto parsed = ParseEndpointAddress("yt-tcp://");
    EXPECT_EQ("yt-tcp", parsed.Protocol);
    EXPECT_EQ("", parsed.Address);
}

TEST(TEndpointAddressTest, Format)
{
    auto formatted = FormatEndpointAddress(TParsedEndpointAddress{
        .Protocol = "yt-tcp",
        .Address = "localhost:9000",
    });
    EXPECT_EQ("yt-tcp://localhost:9000", formatted);
}

TEST(TEndpointAddressTest, FormatOmitsDefaultProtocol)
{
    auto formatted = FormatEndpointAddress(
        TParsedEndpointAddress{
            .Protocol = DefaultProtocolName,
            .Address = "localhost:9000",
        },
        TFormatEndpointAddressOptions{
            .OmitDefaultProtocol = true,
        });
    EXPECT_EQ("localhost:9000", formatted);
}

TEST(TEndpointAddressTest, FormatKeepsNonDefaultProtocolWhenOmitRequested)
{
    auto formatted = FormatEndpointAddress(
        TParsedEndpointAddress{
            .Protocol = "custom",
            .Address = "some-host:42",
        },
        TFormatEndpointAddressOptions{
            .OmitDefaultProtocol = true,
        });
    EXPECT_EQ("custom://some-host:42", formatted);
}

TEST(TEndpointAddressTest, RoundtripWithProtocol)
{
    auto address = "custom://some-host:42";
    EXPECT_EQ(address, FormatEndpointAddress(ParseEndpointAddress(address)));
}

TEST(TEndpointAddressTest, RoundtripWithoutProtocol)
{
    auto address = "localhost:9000";
    EXPECT_EQ(address, FormatEndpointAddress(
        ParseEndpointAddress(address),
        TFormatEndpointAddressOptions{
            .OmitDefaultProtocol = true,
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
