#include <ydb/tests/stress/kv_volume_tool/endpoint_select.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

using NKvVolumeTool::SelectEndpoints;

Ydb::Discovery::EndpointInfo MakeInfo() {
    Ydb::Discovery::EndpointInfo info;
    info.set_address("node.example");
    info.set_port(2135);
    return info;
}

} // namespace

Y_UNIT_TEST_SUITE(KvVolumeEndpointSelect) {
    Y_UNIT_TEST(KeepsEveryIpv4AndIpv6AndUsesHostnameForTls) {
        auto info = MakeInfo();
        info.add_ip_v4("");
        info.add_ip_v4("10.0.0.1");
        info.add_ip_v4("10.0.0.1");
        info.add_ip_v4("10.0.0.2");
        info.add_ip_v6("");
        info.add_ip_v6("2001:db8::1");
        info.add_ip_v6("[::1]");

        const auto endpoints = SelectEndpoints(info, true);

        UNIT_ASSERT_VALUES_EQUAL(endpoints.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(endpoints[0].HostPort, "10.0.0.1:2135");
        UNIT_ASSERT_VALUES_EQUAL(endpoints[1].HostPort, "10.0.0.2:2135");
        UNIT_ASSERT_VALUES_EQUAL(endpoints[2].HostPort, "[2001:db8::1]:2135");
        UNIT_ASSERT_VALUES_EQUAL(endpoints[3].HostPort, "[::1]:2135");
        for (const auto& endpoint : endpoints) {
            UNIT_ASSERT_VALUES_EQUAL(endpoint.SslTargetNameOverride, "node.example");
        }
    }

    Y_UNIT_TEST(IgnoresUnresolvableHostnameWhenAnIpIsPresent) {
        auto info = MakeInfo();
        info.set_address("this.dns.name.is.not.reachable");
        info.add_ip_v4("127.0.0.1");

        const auto endpoints = SelectEndpoints(info, true);

        UNIT_ASSERT_VALUES_EQUAL(endpoints.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(endpoints[0].HostPort, "127.0.0.1:2135");
        UNIT_ASSERT_VALUES_EQUAL(endpoints[0].SslTargetNameOverride, "this.dns.name.is.not.reachable");
    }

    Y_UNIT_TEST(FallsBackToHostnameWhenNoIpIsAdvertised) {
        const auto endpoints = SelectEndpoints(MakeInfo(), true);

        UNIT_ASSERT_VALUES_EQUAL(endpoints.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(endpoints[0].HostPort, "node.example:2135");
        UNIT_ASSERT(endpoints[0].SslTargetNameOverride.empty());
    }

    Y_UNIT_TEST(KeepsExplicitTlsOverride) {
        auto info = MakeInfo();
        info.add_ip_v4("10.0.0.1");
        info.add_ip_v6("2001:db8::1");
        info.set_ssl_target_name_override("balancer.example");

        const auto endpoints = SelectEndpoints(info, true);

        UNIT_ASSERT_VALUES_EQUAL(endpoints.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(endpoints[0].SslTargetNameOverride, "balancer.example");
        UNIT_ASSERT_VALUES_EQUAL(endpoints[1].SslTargetNameOverride, "balancer.example");
    }

    Y_UNIT_TEST(DoesNotInventTlsOverrideWithoutTls) {
        auto info = MakeInfo();
        info.add_ip_v4("10.0.0.1");

        const auto endpoints = SelectEndpoints(info, false);

        UNIT_ASSERT(endpoints[0].SslTargetNameOverride.empty());
        UNIT_ASSERT_VALUES_EQUAL(endpoints[0].HostPort, "10.0.0.1:2135");
    }
}
