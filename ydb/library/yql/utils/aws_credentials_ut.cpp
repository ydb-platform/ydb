#include "aws_credentials.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TAwsCredentials) {
    Y_UNIT_TEST(StringFormat) {
        UNIT_ASSERT_VALUES_EQUAL(ConvertBasicToAwsToken("a", "b"), "AWS a:b");
    }

    Y_UNIT_TEST(PrepareAwsHeader) {
        UNIT_ASSERT_VALUES_EQUAL(PrepareAwsHeader(ConvertBasicToAwsToken("a", "b")), "X-Aws-Credentials-Internal-Using:AWS a:b");
        UNIT_ASSERT_VALUES_EQUAL(PrepareAwsHeader("Bearer xxx"), "");
    }

    Y_UNIT_TEST(ExtractAwsCredentials) {
        TSmallVec<TString> headers{{PrepareAwsHeader(ConvertBasicToAwsToken("a", "b"))}};
        UNIT_ASSERT_VALUES_EQUAL(ExtractAwsCredentials(headers), "a:b");
        UNIT_ASSERT_VALUES_EQUAL(ExtractAwsCredentials(headers), "");
    }
}
