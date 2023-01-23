#include "yql_dns_gateway.h"

#include <util/network/address.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

namespace {

using TDnsResponse = std::vector<NAddr::TOpaqueAddr>;

NAddr::TOpaqueAddr Ipv4Addr(const std::string& ip, ui16 port) {
    NAddr::TIPv4Addr ipv4(TIpAddress(IpFromString(ip.c_str()), port));
    return NAddr::TOpaqueAddr(&ipv4);
}

NAddr::TOpaqueAddr Ipv6Addr(const std::string& ip, ui16 port) {
    sockaddr_in6 ipv6_sock = {};
    ipv6_sock.sin6_family = AF_INET6;
    inet_pton(AF_INET6, ip.c_str(), &(ipv6_sock.sin6_addr));
    ipv6_sock.sin6_port = HostToInet(port);
    NAddr::TIPv6Addr ipv6(ipv6_sock);
    return NAddr::TOpaqueAddr(&ipv6);
}

class TTestDNSTable {
public:
    using TKey = std::pair<TString, ui16>;

    TTestDNSTable() = default;

    TTestDNSTable(std::initializer_list<std::pair<TKey, TDnsResponse>> l)
        : DNSMapping(l.begin(), l.end()) { }

    std::vector<NAddr::TOpaqueAddr> Resolve(const TString& host, ui16 port) const {
        auto key = std::make_pair(host, port);

        auto valueIt = DNSMapping.find(key);
        if (valueIt == DNSMapping.end()) {
            throw TNetworkResolutionError(10);
        }
        return valueIt->second;
    }

private:
    std::unordered_map<TKey, TDnsResponse, NHashPrivate::TPairHash<TString, ui16>> DNSMapping;
};

class TTestDNSTableProxy {
public:
    std::vector<NAddr::TOpaqueAddr> Resolve(
        const TString& host, ui16 port) const {
        return getInstance().Resolve(host, port);
    }

    static const TTestDNSTable& getInstance() { return instance; }

    static void setInstance(const TTestDNSTable& table) { instance = table; }

private:
    static TTestDNSTable instance;
};

TTestDNSTable TTestDNSTableProxy::instance;
} // namespace

Y_UNIT_TEST_SUITE(TDNSGatewaySuite) {
    auto CreateDNSConfig(const TResolutionTask& input) {    
        auto config = std::make_unique<TDnsResolverConfig>();
        auto record = config->AddExplicitDNSRecord();
        record->SetAddress(input.Host);
        record->SetPort(input.Port);
        record->SetExpectedIP(input.ExpectedIP.c_str());
        record->SetProtocol(input.ProtocolVersion);
        return config;
    }

    void ValidateDNSCacheContent(
        const curl_slist* actualDNSCacheContent,
        const std::unordered_set<std::string>& expectedDNSCacheContent) {

        auto dnsItemsCount = 0;
        auto it = actualDNSCacheContent;
        while (it) {
            dnsItemsCount++;
            auto actualDNSCacheRecord = std::string(it->data);
            UNIT_ASSERT(expectedDNSCacheContent.contains(actualDNSCacheRecord));
            it = it->next;
        }
        UNIT_ASSERT_VALUES_EQUAL(dnsItemsCount, expectedDNSCacheContent.size());
    }

    void RunTest(
        const TTestDNSTable& dnsResolver,
        const TResolutionTask& input,
        const std::unordered_set<std::string>& expectedDNSCacheContent) {
        // Setup
        TTestDNSTableProxy::setInstance(std::move(dnsResolver));
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters =
            new NMonitoring::TDynamicCounters;
        auto configPtr = CreateDNSConfig(input);
    
        // Execution
        auto dnsGateway =
            TDNSGateway<TTestDNSTableProxy>(*configPtr.get(), counters);

        // Validate results
        if (expectedDNSCacheContent.empty()) {
            UNIT_ASSERT(dnsGateway.GetDNSCurlList() == nullptr);
        } else {
            UNIT_ASSERT(dnsGateway.GetDNSCurlList() != nullptr);
            ValidateDNSCacheContent(
                dnsGateway.GetDNSCurlList().get(), 
                expectedDNSCacheContent);
        }
        // TearDown
        TTestDNSTableProxy::setInstance(TTestDNSTable{});
    }

    Y_UNIT_TEST(ShouldResolveHostnameFromDNSDuringInitialization) {
        auto dnsTable = TTestDNSTable{std::make_pair(
            std::make_pair("localhost", 443),
            std::vector<NAddr::TOpaqueAddr>{Ipv4Addr("127.0.0.1", 443)})};
        auto inputEntry = TResolutionTask{
            "localhost",
            443,
            "127.0.0.2",
            TExplicitDNSRecord_ProtocolVersion_ANY};
        auto expectedDNSCacheContent =
            std::unordered_set<std::string>{"localhost:443:127.0.0.1"};
        RunTest(dnsTable, inputEntry, expectedDNSCacheContent);
    }

    Y_UNIT_TEST(ShouldUsePreviouslyKnownResolutionIfDNSIsNotResponding) {
        auto dnsTable = TTestDNSTable{{}};
        auto inputEntry = TResolutionTask{
            "localhost",
            443,
            "127.0.0.2",
            TExplicitDNSRecord_ProtocolVersion_ANY};
        auto expectedDNSCacheContent =
            std::unordered_set<std::string>{"localhost:443:127.0.0.2"};
        RunTest(dnsTable, inputEntry, expectedDNSCacheContent);
    }

    Y_UNIT_TEST(ShouldFilterDNSAddressedBasedOnProvidedProtocolIPV4Case) {
        auto dnsTable = TTestDNSTable{std::make_pair(
            std::make_pair("localhost", 443),
            std::vector<NAddr::TOpaqueAddr>{
                Ipv4Addr("127.0.0.1", 443), Ipv6Addr("127.0.0.1", 443)})};
        auto inputEntry = TResolutionTask{
            "localhost",
            443,
            "127.0.0.1",
            TExplicitDNSRecord_ProtocolVersion_IPV4};
        auto expectedDNSCacheContent =
            std::unordered_set<std::string>{"localhost:443:127.0.0.1"};
        RunTest(dnsTable, inputEntry, expectedDNSCacheContent);
    }

    Y_UNIT_TEST(ShouldFilterDNSAddressedBasedOnProvidedProtocolIPV6Case) {
        auto dnsTable = TTestDNSTable{std::make_pair(
            std::make_pair("localhost", 443),
            std::vector<NAddr::TOpaqueAddr>{
                Ipv4Addr("127.0.0.1", 443), Ipv6Addr("::1", 443)})};
        auto inputEntry = TResolutionTask{
            "localhost",
            443,
            "127.0.0.1",
            TExplicitDNSRecord_ProtocolVersion_IPV6};
        auto expectedDNSCacheContent =
            std::unordered_set<std::string>{"localhost:443:::1"};
        RunTest(dnsTable, inputEntry, expectedDNSCacheContent);
    }

    Y_UNIT_TEST(ShouldFilterDNSAddressedBasedOnProvidedProtocolANYCase) {
        auto dnsTable = TTestDNSTable{std::make_pair(
            std::make_pair("localhost", 443),
            std::vector<NAddr::TOpaqueAddr>{
                Ipv4Addr("127.0.0.1", 443), Ipv6Addr("::1", 443)})};
        auto inputEntry = TResolutionTask{
            "localhost",
            443,
            "127.0.0.1",
            TExplicitDNSRecord_ProtocolVersion_ANY};
        auto expectedDNSCacheContent =
            std::unordered_set<std::string>{"localhost:443:127.0.0.1,::1"};
        RunTest(dnsTable, inputEntry, expectedDNSCacheContent);
    }

} // Y_UNIT_TEST_SUITE(TDNSGatewaySuite)
} // namespace NYql
