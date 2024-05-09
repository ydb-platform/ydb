#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/socket.h>

#include <util/system/fs.h>

#ifdef _unix_
    #include <sys/types.h>
    #include <sys/socket.h>
#endif

namespace NYT::NNet {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TNetworkAddressTest, ParseGoodIPv4)
{
    TNetworkAddressFormatOptions options{
        .IncludePort = false
    };

    auto address = TNetworkAddress::Parse("[192.0.2.33]");
    EXPECT_EQ("tcp://192.0.2.33", ToString(address, options));

    address = TNetworkAddress::Parse("192.0.2.33");
    EXPECT_EQ("tcp://192.0.2.33", ToString(address, options));
}

TEST(TNetworkAddressTest, ParseGoodIPv4WithPort)
{
    auto address = TNetworkAddress::Parse("[192.0.2.33]:1000");
    EXPECT_EQ("tcp://192.0.2.33:1000", ToString(address));

    address = TNetworkAddress::Parse("192.0.2.33:1000");
    EXPECT_EQ("tcp://192.0.2.33:1000", ToString(address));
}

TEST(TNetworkAddressTest, ParseBadIPv4Address)
{
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[192.0.XXX.33]")); // extra symbols
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[192.0.2.33]:")); // no port after colon
}

TEST(TNetworkAddressTest, ParseGoodIPv6)
{
    TNetworkAddressFormatOptions options{
        .IncludePort = false
    };

    auto address = TNetworkAddress::Parse("[2001:db8:8714:3a90::12]");
    EXPECT_EQ("tcp://[2001:db8:8714:3a90::12]", ToString(address, options));
}

TEST(TNetworkAddressTest, IP6Conversion)
{
    auto address = TNetworkAddress::Parse("[2001:db8:8714:3a90::12]");
    auto ip6Address = address.ToIP6Address();

    EXPECT_EQ("2001:db8:8714:3a90::12", ToString(ip6Address));
}

TEST(TNetworkAddressTest, ParseGoodIPv6WithPort)
{
    auto address = TNetworkAddress::Parse("[2001:db8:8714:3a90::12]:1000");
    EXPECT_EQ("tcp://[2001:db8:8714:3a90::12]:1000", ToString(address));
}

TEST(TNetworkAddressTest, ParseBadIPv6Address)
{
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[2001:db8:SOME_STRING:3a90::12]")); // extra symbols
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[2001:db8:8714:3a90::12]:")); // no port after colon
}

TEST(TNetworkAddressTest, FormatParseGoodIPv4NoTcpNoPort)
{
    TNetworkAddressFormatOptions options{
        .IncludePort = false,
        .IncludeTcpProtocol = false
    };

    auto address = TNetworkAddress::Parse("127.0.0.1");
    EXPECT_EQ("127.0.0.1", ToString(address, options));
}

TEST(TNetworkAddressTest, FormatParseGoodIPv6NoTcpNoPort)
{
    TNetworkAddressFormatOptions options{
        .IncludePort = false,
        .IncludeTcpProtocol = false
    };

    auto address = TNetworkAddress::Parse("2001:db8:8714:3a90::12");
    EXPECT_EQ("2001:db8:8714:3a90::12", ToString(address, options));
}

#ifdef _linux_

TEST(TNetworkAddressTest, UnixSocketName)
{
    int fds[2];
    YT_VERIFY(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    EXPECT_EQ("unix://[*unnamed*]", ToString(GetSocketName(fds[0])));

    close(fds[0]);
    close(fds[1]);

    auto address = TNetworkAddress::CreateUnixDomainSocketAddress("abc");
    EXPECT_EQ(Format("unix://%v/abc", NFs::CurrentWorkingDirectory()), ToString(address));

    auto absctractAddress = TNetworkAddress::CreateAbstractUnixDomainSocketAddress("abc");
    EXPECT_EQ("unix://[abc]", ToString(absctractAddress));

    auto binaryString = TString("a\0c", 3);
    auto binaryAbstractAddress = TNetworkAddress::CreateAbstractUnixDomainSocketAddress(binaryString);

    EXPECT_EQ(
        Format("%Qv", TString("unix://[a\\x00c]")),
        Format("%Qv", ToString(binaryAbstractAddress)));
}

#endif

////////////////////////////////////////////////////////////////////////////////

TEST(TIP6AddressTest, ToString)
{
    {
        TIP6Address address;
        ASSERT_EQ("::", ToString(address));
    }

    {
        TIP6Address address;
        address.GetRawWords()[0] = 3;
        ASSERT_EQ("::3", ToString(address));
    }

    {
        TIP6Address address;
        address.GetRawWords()[7] = 0xfff1;
        ASSERT_EQ("fff1::", ToString(address));
    }
}

TEST(TIP6AddressTest, InvalidAddress)
{
    for (const auto& addr : std::vector<TString>{
        ":::",
        "1::1::1",
        "0:1:2:3:4:5:6:7:8",
        "0:1:2:3:4:5:67777",
        "0:1:2:3:4:5:6:7:",
        ":0:1:2:3:4:5:6:7",
        ":1:2:3:4:5:6:7",
        "1:2:3:4:5:6:7:"
    }) {
        EXPECT_THROW(TIP6Address::FromString(addr), TErrorException)
            << addr;
    }
}

std::array<ui16, 8> AddressToWords(const TIP6Address& addr) {
    std::array<ui16, 8> buf;
    std::copy(addr.GetRawWords(), addr.GetRawWords() + 8, buf.begin());
    return buf;
}

TEST(TIP6AddressTest, FromString)
{
    using TTestCase = std::pair<TString, std::array<ui16, 8>>;

    for (const auto& testCase : {
        TTestCase{"0:0:0:0:0:0:0:0", {0, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"0:0:0:0:0:0:0:3", {3, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"0:0:0:0::0:3", {3, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"fff1:1:3:4:5:6:7:8", {8, 7, 6, 5, 4, 3, 1_KB / 1_KB, 0xfff1}},
        TTestCase{"::", {0, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"::1", {1, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"::1:2", {2, 1, 0, 0, 0, 0, 0, 0}},
        TTestCase{"1::", {0, 0, 0, 0, 0, 0, 0, 1}},
        TTestCase{"0:1::", {0, 0, 0, 0, 0, 0, 1, 0}},
        TTestCase{"0:1::1:0:0", {0, 0, 1, 0, 0, 0, 1, 0}},
        TTestCase{"ffab:3:0::1234:6", {0x6, 0x1234, 0, 0, 0, 0, 0x3, 0xffab}}
    }) {
        auto address = TIP6Address::FromString(testCase.first);
        EXPECT_EQ(AddressToWords(address), testCase.second);
    }
}

TEST(TIP6AddressTest, CanonicalText)
{
    for (const auto& str : std::vector<TString>{
        "::",
        "::1",
        "1::",
        "2001:db8::1:0:0:1",
        "::2001:db8:1:0:0:1",
        "::2001:db8:1:1:0:0",
        "2001:db8::1:1:0:0",
        "1:2:3:4:5:6:7:8"
    }) {
        auto address = TIP6Address::FromString(str);
        EXPECT_EQ(str, ToString(address));
    }
}

TEST(TIP6AddressTest, Constructors)
{
    {
        auto address = TIP6Address::FromString("0:0:0:0:0:0:0:3");
        auto mask = TIP6Address::FromString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:0000");
        TIP6Network network(address, mask);
        EXPECT_EQ(network.GetAddress(), address);
        EXPECT_EQ(network.GetMask(), mask);
    }

    {
        auto address = TIP6Address::FromString("0:0:0:0:0:0:0:3");
        auto mask = TIP6Address::FromString("ffff:ffff:ffff:ffff:ffff:ffff:0000:ffff");
        EXPECT_THROW(TIP6Network(address, mask), TErrorException);
    }
}

TEST(TIP6AddressTest, NetworkMask)
{
    using TTestCase = std::tuple<const char*, std::array<ui16, 8>, int>;
    for (const auto& testCase : {
        TTestCase{"::/1", {0, 0, 0, 0, 0, 0, 0, 0x8000}, 1},
        TTestCase{"::/0", {0, 0, 0, 0, 0, 0, 0, 0}, 0},
        TTestCase{"::/24", {0, 0, 0, 0, 0, 0, 0xff00, 0xffff}, 24},
        TTestCase{"::/32", {0, 0, 0, 0, 0, 0, 0xffff, 0xffff}, 32},
        TTestCase{"::/64", {0, 0, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff}, 64},
        TTestCase{"::/128", {0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff}, 128},
    }) {
        auto network = TIP6Network::FromString(std::get<0>(testCase));
        EXPECT_EQ(AddressToWords(network.GetMask()), std::get<1>(testCase));
        EXPECT_EQ(network.GetMaskSize(), std::get<2>(testCase));
        EXPECT_EQ(ToString(network), std::get<0>(testCase));

        auto networkAsString = ToString(network);
        auto networkFromSerializedString = TIP6Network::FromString(networkAsString);
        EXPECT_EQ(AddressToWords(networkFromSerializedString.GetMask()), std::get<1>(testCase));
        EXPECT_EQ(networkFromSerializedString.GetMaskSize(), std::get<2>(testCase));
        EXPECT_EQ(ToString(networkFromSerializedString), std::get<0>(testCase));
    }

    EXPECT_THROW(TIP6Network::FromString("::/129"), TErrorException);
    EXPECT_THROW(TIP6Network::FromString("::/1291"), TErrorException);
}

TEST(TIP6AddressTest, ProjectId)
{
    using TTestCase = std::tuple<const char*, const char*, std::array<ui16, 8>, std::array<ui16, 8>>;
    for (const auto& testCase : {
        TTestCase{
            "10ec9bd@2a02:6b8:fc00::/40",
            "2a02:6b8:fc17:3204:10e:c9bd:0:320f",
            {0, 0, 0xc9bd, 0x10e, 0, 0xfc00, 0x6b8, 0x2a02},
            {0, 0, 0xffff, 0xffff, 0, 0xff00, 0xffff, 0xffff}
        },
        TTestCase{
            "102bcfb@2a02:6b8:c00::/40",
            "2a02:6b8:c13:3120:102:bcfb:0:2334",
            {0, 0, 0xbcfb, 0x102, 0, 0x0c00, 0x6b8, 0x2a02},
            {0, 0, 0xffff, 0xffff, 0, 0xff00, 0xffff, 0xffff}
        },
    }) {
        auto network = TIP6Network::FromString(std::get<0>(testCase));
        auto testAddress = TIP6Address::FromString(std::get<1>(testCase));
        EXPECT_EQ(AddressToWords(network.GetAddress()), std::get<2>(testCase));
        EXPECT_EQ(AddressToWords(network.GetMask()), std::get<3>(testCase));
        EXPECT_TRUE(network.Contains(testAddress));

        auto networkAsString = ToString(network);
        auto networkFromSerializedString = TIP6Network::FromString(networkAsString);
        EXPECT_EQ(AddressToWords(networkFromSerializedString.GetAddress()), std::get<2>(testCase));
        EXPECT_EQ(AddressToWords(networkFromSerializedString.GetMask()), std::get<3>(testCase));
        EXPECT_TRUE(networkFromSerializedString.Contains(testAddress));
    }

    EXPECT_THROW(TIP6Network::FromString("@1::1"), TErrorException);
    EXPECT_THROW(TIP6Network::FromString("123456789@1::1"), TErrorException);
}

TEST(TIP6AddressTest, InvalidInput)
{
    for (const auto& testCase : std::vector<TString>{
        "",
        ":",
        "::/",
        "::/1",
        ":::",
        "::1::",
        "1",
        "1:1",
        "11111::",
        "g::",
        "1:::1",
        "fff1:1:3:4:5:6:7:8:9",
        "fff1:1:3:4:5:6:7:8::",
        "::fff1:1:3:4:5:6:7:8"
    }) {
        EXPECT_THROW(TIP6Address::FromString(testCase), TErrorException)
            << Format("input = %Qv", testCase);

        auto network = testCase + "/32";
        EXPECT_THROW(TIP6Network::FromString(network), TErrorException)
            << Format("input = %Qv", network);
    }
}

TEST(TIP6AddressTest, ToStringFromStringRandom)
{
    for (int i = 0; i < 100; ++i) {
        ui8 bytes[TIP6Address::ByteSize];
        for (int j = 0; j < static_cast<ssize_t>(TIP6Address::ByteSize); ++j) {
            bytes[j] = RandomNumber<ui8>();
        }

        auto address = TIP6Address::FromRawBytes(bytes);
        ASSERT_EQ(address, TIP6Address::FromString(ToString(address)));
    }
}

TEST(TMtnAddressTest, SimpleTest)
{
    TMtnAddress address(TIP6Address::FromString("1361:24ad:4326:bda1:8432:a3fe:3f6c:4b38"));
    EXPECT_EQ(address.GetPrefix(), 0x136124ad43u);
    EXPECT_EQ(address.GetGeo(), 0x26bda1u);
    EXPECT_EQ(address.GetProjectId(), 0x8432a3feu);
    EXPECT_EQ(address.GetHost(), 0x3f6c4b38u);

    address.SetPrefix(0x123456789a);
    EXPECT_EQ(ToString(address.ToIP6Address()), "1234:5678:9a26:bda1:8432:a3fe:3f6c:4b38");

    address.SetGeo(0x123456);
    EXPECT_EQ(ToString(address.ToIP6Address()), "1234:5678:9a12:3456:8432:a3fe:3f6c:4b38");

    address.SetProjectId(0x12345678);
    EXPECT_EQ(ToString(address.ToIP6Address()), "1234:5678:9a12:3456:1234:5678:3f6c:4b38");

    address.SetHost(0x12345678);
    EXPECT_EQ(ToString(address.ToIP6Address()), "1234:5678:9a12:3456:1234:5678:1234:5678");

    EXPECT_THROW(address.SetPrefix(1ull << 41), TErrorException);
    EXPECT_THROW(address.SetGeo(1ull << 25), TErrorException);
    EXPECT_THROW(address.SetProjectId(1ull << 33), TErrorException);
    EXPECT_THROW(address.SetHost(1ull << 33), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

TEST(InferYPCluster, ValidFqdns)
{
    TString gencfgHostName = "sas1-5535-9d7.sas-test.yp.gencfg-c.yandex.net";
    TString ypHostName = "noqpmfiudzbb4hvs.man.yp-c.yandex.net";

    EXPECT_EQ(InferYPClusterFromHostName(gencfgHostName), "sas-test");
    EXPECT_EQ(InferYPClusterFromHostName(ypHostName), "man");
}

TEST(InferYPCluster, InvalidFqdn)
{
    TString hostName = "noqpmfiudzbb4hvs..yp-c.yandex.net";

    EXPECT_EQ(InferYPClusterFromHostName(hostName), std::nullopt);
    EXPECT_EQ(InferYPClusterFromHostName("localhost"), std::nullopt);
    EXPECT_EQ(InferYPClusterFromHostName("noqpmfiudzbb4hvs."), std::nullopt);
    EXPECT_EQ(InferYPClusterFromHostName("yandex.net"), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

TEST(InferYTCluster, ClusterUrls)
{
    EXPECT_EQ(InferYTClusterFromClusterUrl("hume"), "hume");
    EXPECT_EQ(InferYTClusterFromClusterUrl("http://yp-sas.yt.yandex.net"), "yp-sas");
    EXPECT_EQ(InferYTClusterFromClusterUrl("seneca-man.yt.yandex.net"), "seneca-man");
    EXPECT_EQ(InferYTClusterFromClusterUrl("seneca-man..yt.yandex.net"), std::nullopt);
    EXPECT_EQ(InferYTClusterFromClusterUrl("localhost:1245"), std::nullopt);
    EXPECT_EQ(InferYTClusterFromClusterUrl("kek:1245"), std::nullopt);
    EXPECT_EQ(InferYTClusterFromClusterUrl("localhost"), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNet
