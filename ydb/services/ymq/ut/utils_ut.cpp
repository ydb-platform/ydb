#include <ydb/services/ymq/rpc_params.h>
#include <ydb/services/ymq/utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpc/grpc.h>
#include <grpc/slice.h>

namespace NKikimr::NYmq {

Y_UNIT_TEST_SUITE(YmqAuditContext) {
    Y_UNIT_TEST(TransportPeerAddresses) {
        const std::pair<TString, TString> cases[] = {
            {"192.0.2.17", "192.0.2.17"},
            {"2001:db8::17", "2001:db8::17"},
            {"::1", "::1"},
            {"fe80::17%253", "fe80::17%253"},
            {"ipv4:192.0.2.17:2135", "192.0.2.17"},
            {"ipv6:[2001:db8::17]:2135", "2001:db8::17"},
            {"ipv6:[::1]:2135", "::1"},
            {"ipv6:[fe80::17%253]:2135", "fe80::17%253"},
        };
        for (const auto& [peer, expected] : cases) {
            UNIT_ASSERT_VALUES_EQUAL_C(GetAuditRemoteAddress(peer), expected, peer);
        }
    }

    Y_UNIT_TEST(MissingAndInvalidPeers) {
        for (const TString peer : {
            "", "localhost", "unknown", "not-an-ip", "unix:/tmp/ymq.sock",
            "ipv4:999.0.2.17:2135", "ipv6:[not-an-ip]:2135",
            "ipv6:%5B::1%5D:2135",
        }) {
            UNIT_ASSERT_C(GetAuditRemoteAddress(peer).empty(), peer);
        }
    }

    Y_UNIT_TEST(SanitizedTokenMetadataIsLocalOnly) {
        const auto key = grpc_slice_from_static_string(V1::SANITIZED_TOKEN.c_str());
        UNIT_ASSERT_VALUES_EQUAL(grpc_header_key_is_legal(key), 0);
        grpc_slice_unref(key);
    }
}

} // namespace NKikimr::NYmq
