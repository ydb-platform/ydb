#include <ydb/services/ymq/rpc_params.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpc/grpc.h>
#include <grpc/slice.h>

namespace NKikimr::NYmq {

Y_UNIT_TEST_SUITE(YmqAuditContext) {
    Y_UNIT_TEST(SanitizedTokenMetadataIsLocalOnly) {
        const auto key = grpc_slice_from_static_string(V1::SANITIZED_TOKEN.c_str());
        UNIT_ASSERT_VALUES_EQUAL(grpc_header_key_is_legal(key), 0);
        grpc_slice_unref(key);
    }
}

} // namespace NKikimr::NYmq
