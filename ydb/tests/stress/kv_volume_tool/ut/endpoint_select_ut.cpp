#include <ydb/tests/stress/kv_volume_tool/endpoint_select.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

using NKvVolumeTool::AdvanceAfterFailover;
using NKvVolumeTool::SelectNode;
using NKvVolumeTool::TFailoverPosition;
using NKvVolumeTool::TryNextAddress;
using NKvVolumeTool::TryNextNode;

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

        const auto node = SelectNode(info, true);

        UNIT_ASSERT_VALUES_EQUAL(node.Addresses.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(node.Addresses[0], "10.0.0.1:2135");
        UNIT_ASSERT_VALUES_EQUAL(node.Addresses[1], "10.0.0.2:2135");
        UNIT_ASSERT_VALUES_EQUAL(node.Addresses[2], "[2001:db8::1]:2135");
        UNIT_ASSERT_VALUES_EQUAL(node.Addresses[3], "[::1]:2135");
        UNIT_ASSERT_VALUES_EQUAL(node.SslTargetNameOverride, "node.example");
    }

    Y_UNIT_TEST(IgnoresUnresolvableHostnameWhenAnIpIsPresent) {
        auto info = MakeInfo();
        info.set_address("this.dns.name.is.not.reachable");
        info.add_ip_v4("127.0.0.1");

        const auto node = SelectNode(info, true);

        UNIT_ASSERT_VALUES_EQUAL(node.Addresses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(node.Addresses[0], "127.0.0.1:2135");
        UNIT_ASSERT_VALUES_EQUAL(node.SslTargetNameOverride, "this.dns.name.is.not.reachable");
    }

    Y_UNIT_TEST(FallsBackToHostnameWhenNoIpIsAdvertised) {
        const auto node = SelectNode(MakeInfo(), true);

        UNIT_ASSERT_VALUES_EQUAL(node.Addresses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(node.Addresses[0], "node.example:2135");
        UNIT_ASSERT(node.SslTargetNameOverride.empty());
    }

    Y_UNIT_TEST(KeepsExplicitTlsOverride) {
        auto info = MakeInfo();
        info.add_ip_v4("10.0.0.1");
        info.add_ip_v6("2001:db8::1");
        info.set_ssl_target_name_override("balancer.example");

        const auto node = SelectNode(info, true);

        UNIT_ASSERT_VALUES_EQUAL(node.SslTargetNameOverride, "balancer.example");
    }

    Y_UNIT_TEST(DoesNotInventTlsOverrideWithoutTls) {
        auto info = MakeInfo();
        info.add_ip_v4("10.0.0.1");

        const auto node = SelectNode(info, false);

        UNIT_ASSERT(node.SslTargetNameOverride.empty());
        UNIT_ASSERT_VALUES_EQUAL(node.Addresses[0], "10.0.0.1:2135");
    }

    Y_UNIT_TEST(ConnectionFailureTriesEveryAddressBeforeTheNextNode) {
        const TVector<size_t> addressCounts = {2, 1};
        TFailoverPosition position;

        UNIT_ASSERT(AdvanceAfterFailover(position, TryNextAddress, addressCounts));
        UNIT_ASSERT_VALUES_EQUAL(position.Node, 0);
        UNIT_ASSERT_VALUES_EQUAL(position.Address, 1);

        UNIT_ASSERT(AdvanceAfterFailover(position, TryNextAddress, addressCounts));
        UNIT_ASSERT_VALUES_EQUAL(position.Node, 1);
        UNIT_ASSERT_VALUES_EQUAL(position.Address, 0);

        UNIT_ASSERT(!AdvanceAfterFailover(position, TryNextAddress, addressCounts));
    }

    Y_UNIT_TEST(NodeFailureSkipsRemainingAddresses) {
        const TVector<size_t> addressCounts = {3, 2};
        TFailoverPosition position;

        UNIT_ASSERT(AdvanceAfterFailover(position, TryNextNode, addressCounts));
        UNIT_ASSERT_VALUES_EQUAL(position.Node, 1);
        UNIT_ASSERT_VALUES_EQUAL(position.Address, 0);

        UNIT_ASSERT(!AdvanceAfterFailover(position, TryNextNode, addressCounts));
    }

    Y_UNIT_TEST(WrapsUntilEveryNodeWasTriedThenReportsExhaustion) {
        const TVector<size_t> addressCounts = {1, 2, 1};
        TFailoverPosition position;
        position.Node = 2;

        UNIT_ASSERT(AdvanceAfterFailover(position, TryNextAddress, addressCounts));
        UNIT_ASSERT_VALUES_EQUAL(position.Node, 0);
        UNIT_ASSERT_VALUES_EQUAL(position.Address, 0);

        UNIT_ASSERT(AdvanceAfterFailover(position, TryNextAddress, addressCounts));
        UNIT_ASSERT_VALUES_EQUAL(position.Node, 1);
        UNIT_ASSERT_VALUES_EQUAL(position.Address, 0);

        UNIT_ASSERT(AdvanceAfterFailover(position, TryNextAddress, addressCounts));
        UNIT_ASSERT_VALUES_EQUAL(position.Node, 1);
        UNIT_ASSERT_VALUES_EQUAL(position.Address, 1);

        UNIT_ASSERT(!AdvanceAfterFailover(position, TryNextAddress, addressCounts));
    }
} // Y_UNIT_TEST_SUITE(KvVolumeEndpointSelect)
