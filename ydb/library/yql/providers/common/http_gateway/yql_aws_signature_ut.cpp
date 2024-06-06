#include "yql_aws_signature.h"

#include <util/network/address.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TAwsSignature) {
    Y_UNIT_TEST(Sign) {
        NYql::TAwsSignature signature("GET", "http://os.com/my-bucket/year=2024/day=03/", "application/json", {}, "key", "pwd");
        UNIT_ASSERT_VALUES_EQUAL(signature.GetContentType(), "application/json");
        UNIT_ASSERT_VALUES_EQUAL(signature.GetXAmzContentSha256(), "12ae32cb1ec02d01eda3581b127c1fee3b0dc53572ed6baf239721a03d82e126");
        UNIT_ASSERT_STRING_CONTAINS(signature.GetAuthorization(), "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=");
        UNIT_ASSERT_STRING_CONTAINS(signature.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/");
        UNIT_ASSERT_STRING_CONTAINS(signature.GetAuthorization(), "///aws4_request");
        UNIT_ASSERT_STRING_CONTAINS(signature.GetAuthorization(), "Signature=");
        UNIT_ASSERT_VALUES_UNEQUAL(signature.GetAmzDate(), "");
    }

    Y_UNIT_TEST(SignCmp) {
        NYql::TAwsSignature signature1("GET", "http://os.com/my-bucket/year=2024/day=03/", "application/json", {}, "key", "pwd");
        NYql::TAwsSignature signature2("GET", "http://os.com/", "application/json", {}, "key", "pwd");
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetContentType(), signature2.GetContentType());
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetXAmzContentSha256(), signature2.GetXAmzContentSha256());
        UNIT_ASSERT_VALUES_UNEQUAL(signature1.GetAuthorization(), signature2.GetAuthorization());
    }

    Y_UNIT_TEST(SignPayload) {
        NYql::TAwsSignature signature1("GET", "http://os.com/my-bucket/year=2024/day=03/", "application/json", {}, "key", "pwd");
        NYql::TAwsSignature signature2("GET", "http://os.com/", "application/json", "test", "key", "pwd");
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetContentType(), signature2.GetContentType());
        UNIT_ASSERT_VALUES_UNEQUAL(signature1.GetXAmzContentSha256(), signature2.GetXAmzContentSha256());
        UNIT_ASSERT_VALUES_UNEQUAL(signature1.GetAuthorization(), signature2.GetAuthorization());
    }
} // Y_UNIT_TEST_SUITE(TAwsSignature)
} // namespace NYql
