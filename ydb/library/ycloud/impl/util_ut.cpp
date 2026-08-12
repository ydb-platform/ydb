#include <ydb/library/ycloud/impl/util.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

Y_UNIT_TEST_SUITE(TBuildUserAgentPrefixTest) {
    Y_UNIT_TEST(BuildEmpty) {
        const auto userAgent = NCloud::BuildUserAgentPrefix("");
        UNIT_ASSERT_C(TStringBuf(userAgent).StartsWith("ydb/"), userAgent);
    }

    Y_UNIT_TEST(BuildWithUserAgent) {
        const auto userAgent = NCloud::BuildUserAgentPrefix("test-user-agent");
        UNIT_ASSERT_C(TStringBuf(userAgent).StartsWith("test-user-agent/"), userAgent);
    }
}
