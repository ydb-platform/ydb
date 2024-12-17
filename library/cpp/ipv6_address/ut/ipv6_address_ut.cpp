#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/ipv6_address/ipv6_address.h>
#include <unordered_set>

class TIpv6AddressTest: public TTestBase {
    UNIT_TEST_SUITE(TIpv6AddressTest);
    UNIT_TEST(ParseHostAndMayBePortFromString_data);
    UNIT_TEST(CheckAddressValidity)
    UNIT_TEST(CheckToStringConversion)
    UNIT_TEST_SUITE_END();

private:
    void ParseHostAndMayBePortFromString_data();
    void CheckAddressValidity();
    void HashCompileTest();
    void CheckToStringConversion();
};

UNIT_TEST_SUITE_REGISTRATION(TIpv6AddressTest);

using TResult = std::tuple<THostAddressAndPort, TString, TIpPort>;

TResult IpRes(TString Ip, TIpPort Port) {
    bool Ok;
    THostAddressAndPort HostAddressAndPort;
    HostAddressAndPort.Ip = TIpv6Address::FromString(Ip, Ok);
    Y_ABORT_UNLESS(Ok);
    HostAddressAndPort.Port = Port;
    return TResult(HostAddressAndPort, {}, {});
}
TResult HostRes(TString HostName, TIpPort Port) {
    return TResult({}, HostName, Port);
}

void ParseHostAndMayBePortFromString(TString RawStr,
                                     TIpPort DefaultPort,
                                     const TResult ExpectedResult,
                                     const bool ExpectedOk) {
    bool Ok = false;
    const TResult ActualResult = ParseHostAndMayBePortFromString(RawStr, DefaultPort, Ok);

    UNIT_ASSERT(Ok == ExpectedOk);
    if (Ok == false)
        return;

    UNIT_ASSERT(ActualResult == ExpectedResult);
}

void CheckIpDefPortAgainstIpPortDefPort_v4OrHost(TString Ip,
                                                 TIpPort Port,
                                                 const TResult ExpectedResult,
                                                 const bool ExpectedOk) {
    ParseHostAndMayBePortFromString(Ip, Port, ExpectedResult, ExpectedOk);

    TString New = Ip + ":" + ToString(Port);
    ParseHostAndMayBePortFromString(New, Port + 12, ExpectedResult, ExpectedOk);
}

void CheckIpDefPortAgainstIpPortDefPort_v6(TString Ip, TIpPort Port, const TResult ExpectedResult, const bool ExpectedOk) {
    ParseHostAndMayBePortFromString(Ip, Port, ExpectedResult, ExpectedOk);

    TString New = "[" + Ip + "]" + ":" + ToString(Port);
    ParseHostAndMayBePortFromString(New, Port + 12, ExpectedResult, ExpectedOk);
}

void CheckIpDefPortAgainstIpPortDefPort(TString Ip, TIpPort Port, const TResult ExpectedResult, const bool ExpectedOk) {
    if (Ip.find(':') == TString::npos) {
        CheckIpDefPortAgainstIpPortDefPort_v4OrHost(Ip, Port, ExpectedResult, ExpectedOk);
    } else {
        CheckIpDefPortAgainstIpPortDefPort_v6(Ip, Port, ExpectedResult, ExpectedOk);
    }
}

void TIpv6AddressTest::ParseHostAndMayBePortFromString_data() {
    CheckIpDefPortAgainstIpPortDefPort("1.2.3.4", 123, IpRes("1.2.3.4", 123), true);
    ParseHostAndMayBePortFromString("[1.2.3.4]", 123, {}, false);

    ParseHostAndMayBePortFromString("[2001::7348]", 123, IpRes("2001::7348", 123), true);
    CheckIpDefPortAgainstIpPortDefPort("2001::7348", 123, IpRes("2001::7348", 123), true);

    CheckIpDefPortAgainstIpPortDefPort("ya.ru", 123, HostRes("ya.ru", 123), true);
}

void TIpv6AddressTest::CheckAddressValidity() {
    bool Ok;

    constexpr TIpv6Address partsV4 {12, 34, 56, 78};
    static_assert(partsV4.Type() == TIpv6Address::Ipv4);

    constexpr TIpv6Address intV4 {0x0C22384E, TIpv6Address::Ipv4};
    static_assert(partsV4 == intV4);

    const auto parsedV4 = TIpv6Address::FromString("12.34.56.78", Ok);
    UNIT_ASSERT(Ok);
    UNIT_ASSERT_EQUAL(parsedV4, partsV4);

    constexpr TIpv6Address partsV6 {0xFB, 0x1634, 0x19, 0xABED, 0, 0x8001, 0x1670, 0x742};
    static_assert(partsV6.Type() == TIpv6Address::Ipv6);

    constexpr TIpv6Address intV6 {{0x00FB16340019ABED, 0x0000800116700742}, TIpv6Address::Ipv6};
    static_assert(partsV6 == intV6);

    const auto parsedV6 = TIpv6Address::FromString("FB:1634:19:ABED:0:8001:1670:742", Ok);
    UNIT_ASSERT(Ok);
    UNIT_ASSERT_EQUAL(parsedV6, partsV6);

    static_assert(Get127001() == TIpv6Address(0x7F000001, TIpv6Address::Ipv4));
    static_assert(Get1() == TIpv6Address(0, 0, 0, 0, 0, 0, 0, 1));
}

void TIpv6AddressTest::CheckToStringConversion() {
    {
        TString ipPort = "[2aa2::786b]:789";
        bool ok;
        auto result = ParseHostAndMayBePortFromString(ipPort, 80, ok);
        auto hostAddressAndPort = std::get<THostAddressAndPort>(result);
        UNIT_ASSERT_EQUAL(hostAddressAndPort.ToString({}), ipPort);
        UNIT_ASSERT_EQUAL(hostAddressAndPort.ToString(), ipPort);
    }
    {
        TString ipPort = "[2aa2::786b%25]:789";
        bool ok;
        auto result = ParseHostAndMayBePortFromString(ipPort, 80, ok);
        auto hostAddressAndPort = std::get<THostAddressAndPort>(result);
        UNIT_ASSERT_EQUAL(hostAddressAndPort.ToString({.PrintScopeId = true}), ipPort);
    }
}

void TIpv6AddressTest::HashCompileTest() {
    std::unordered_set<TIpv6Address> test;
    Y_UNUSED(test);
}
