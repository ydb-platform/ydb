#include <ydb/public/api/grpc/draft/xds/codec/codec.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NEds {

Y_UNIT_TEST_SUITE(EndpointAssignment) {
    Y_UNIT_TEST(PreservesDiscoveryFieldsIncludingTlsAndBridgeState) {
        Ydb::Discovery::ListEndpointsResult input;
        input.set_self_location("dc1");
        input.add_pile_states()->set_state(Ydb::Bridge::PileState::PRIMARY);
        auto* endpoint = input.add_endpoints();
        endpoint->set_address("node.example");
        endpoint->set_port(2135);
        endpoint->set_node_id(17);
        endpoint->set_ssl(true);
        endpoint->set_ssl_target_name_override("db.example");
        endpoint->add_ip_v6("::1");
        endpoint->add_service("table_service");

        Ydb::Xds::DiscoveryResponse response;
        Encode(input, "/db", "v1", "n1", response);
        UNIT_ASSERT_VALUES_EQUAL(response.type_url(), TypeUrl);
        UNIT_ASSERT_VALUES_EQUAL(response.version_info(), "v1");
        UNIT_ASSERT_VALUES_EQUAL(response.nonce(), "n1");
        Ydb::Discovery::ListEndpointsResult decoded;
        TString error;
        UNIT_ASSERT_C(Decode(response, "/db", decoded, error), error);
        UNIT_ASSERT_VALUES_EQUAL(input.SerializeAsString(), decoded.SerializeAsString());
    }

    Y_UNIT_TEST(EmptyAssignmentReplacesMembership) {
        Ydb::Discovery::ListEndpointsResult input;
        input.set_self_location("dc1");
        Ydb::Xds::DiscoveryResponse response;
        Encode(input, "/db", "v2", "n2", response);
        Ydb::Discovery::ListEndpointsResult decoded;
        decoded.add_endpoints()->set_node_id(1);
        TString error;
        UNIT_ASSERT_C(Decode(response, "/db", decoded, error), error);
        UNIT_ASSERT_VALUES_EQUAL(decoded.endpoints_size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(decoded.self_location(), "dc1");
    }

    Y_UNIT_TEST(RejectsInvalidSnapshotWithoutInstallingPartialMembership) {
        Ydb::Discovery::ListEndpointsResult input;
        auto* endpoint = input.add_endpoints();
        endpoint->set_address("target");
        endpoint->set_port(70000);
        Ydb::Xds::DiscoveryResponse response;
        Encode(input, "/db", "v3", "n3", response);
        Ydb::Discovery::ListEndpointsResult installed;
        installed.add_endpoints()->set_address("previous");
        TString error;
        UNIT_ASSERT(!Decode(response, "/other", installed, error));
        UNIT_ASSERT(!Decode(response, "/db", installed, error));
        UNIT_ASSERT_VALUES_EQUAL(installed.endpoints(0).address(), "previous");
    }
}

}
