#include "credentials.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TCredentials) {
    Y_UNIT_TEST(CheckToken) {
        TS3Credentials::TAuthInfo authInfo{.Token = "test"};
        UNIT_ASSERT_VALUES_EQUAL(authInfo.GetToken(), "test");
    }

    Y_UNIT_TEST(CheckAws) {
        TS3Credentials::TAuthInfo authInfo{.Token = {}, .AwsAccessKey = "key", .AwsAccessSecret = "secret", .AwsRegion = "region"};
        UNIT_ASSERT_VALUES_EQUAL(authInfo.GetAwsSigV4(), "aws:amz:region:s3");
        UNIT_ASSERT_VALUES_EQUAL(authInfo.GetAwsUserPwd(), "key:secret");
    }
}