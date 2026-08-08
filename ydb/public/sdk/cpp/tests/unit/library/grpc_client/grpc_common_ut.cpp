#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/strbuf.h>

using namespace NYdbGrpc;

Y_UNIT_TEST_SUITE(GrpcCommonUserAgentTests) {
    Y_UNIT_TEST(BuildsUserAgentPrefixWithoutHint) {
        const TGRpcClientConfig config("localhost:1", "");
        const TStringBuf userAgentPrefix(config.UserAgentPrefix);

        Cerr << userAgentPrefix << Endl;

        UNIT_ASSERT_C(userAgentPrefix.StartsWith("ydb/"), userAgentPrefix);
        UNIT_ASSERT_C(userAgentPrefix.size() > TStringBuf("ydb/").size(), userAgentPrefix);
    }

    Y_UNIT_TEST(BuildsUserAgentPrefixWithHint) {
        const TGRpcClientConfig configWithoutHint("localhost:1", "");
        const TGRpcClientConfig configWithHint("localhost:1", "grpc_common_ut");
        const TStringBuf prefixWithoutHint(configWithoutHint.UserAgentPrefix);
        const TStringBuf prefixWithHint(configWithHint.UserAgentPrefix);
        constexpr TStringBuf expectedPrefixWithoutHint = "ydb/";
        constexpr TStringBuf expectedPrefixWithHint = "ydb-grpc_common_ut/";

        UNIT_ASSERT_C(prefixWithHint.StartsWith(expectedPrefixWithHint), prefixWithHint);
        UNIT_ASSERT_VALUES_EQUAL(
            prefixWithHint.SubStr(expectedPrefixWithHint.size()),
            prefixWithoutHint.SubStr(expectedPrefixWithoutHint.size()));
    }
}
