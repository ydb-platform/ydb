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

    Y_UNIT_TEST(SignWithTime) {
        auto time = TInstant::FromValue(30);
        NYql::TAwsSignature signature1("GET", "http://os.com/my-bucket/year=2024/day=03/", "application/json", {}, "key", "pwd", time);
        NYql::TAwsSignature signature2("GET", "http://os.com/", "application/json", "", "key", "pwd", time);
        NYql::TAwsSignature signature3("GET", "http://os.com/my-bucket/year=2024/day=03/", "application/json", "", "key2", "pwd", time);
        NYql::TAwsSignature signature4("POST", "http://os.com/my-bucket/year=2024/day=03/", "application/json", "", "key2", "pwd", time);
        static const TString CONTENT_TYPE = "application/json";
        static const TString X_AMZ_CONTENT_SHA_256 = "12ae32cb1ec02d01eda3581b127c1fee3b0dc53572ed6baf239721a03d82e126";
        static const TString X_AMX_DATE = "19700101T000000Z";
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetContentType(), CONTENT_TYPE);
        UNIT_ASSERT_VALUES_EQUAL(signature2.GetContentType(), CONTENT_TYPE);
        UNIT_ASSERT_VALUES_EQUAL(signature3.GetContentType(), CONTENT_TYPE);
        UNIT_ASSERT_VALUES_EQUAL(signature4.GetContentType(), CONTENT_TYPE);
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetXAmzContentSha256(), X_AMZ_CONTENT_SHA_256);
        UNIT_ASSERT_VALUES_EQUAL(signature2.GetXAmzContentSha256(), X_AMZ_CONTENT_SHA_256);
        UNIT_ASSERT_VALUES_EQUAL(signature3.GetXAmzContentSha256(), X_AMZ_CONTENT_SHA_256);
        UNIT_ASSERT_VALUES_EQUAL(signature4.GetXAmzContentSha256(), X_AMZ_CONTENT_SHA_256);
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetAmzDate(), X_AMX_DATE);
        UNIT_ASSERT_VALUES_EQUAL(signature2.GetAmzDate(), X_AMX_DATE);
        UNIT_ASSERT_VALUES_EQUAL(signature3.GetAmzDate(), X_AMX_DATE);
        UNIT_ASSERT_VALUES_EQUAL(signature4.GetAmzDate(), X_AMX_DATE);
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=fba1374eb117fe9d00fc04bb1b709a3ea6f152232ac1f7dc49117a505f7e9f3f");
        UNIT_ASSERT_VALUES_EQUAL(signature2.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=39b47595c16b5d7b8256fd00e3d17e2157c5050c293306c995ae6980f11c689f");
        UNIT_ASSERT_VALUES_EQUAL(signature3.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=fba1374eb117fe9d00fc04bb1b709a3ea6f152232ac1f7dc49117a505f7e9f3f");
        UNIT_ASSERT_VALUES_EQUAL(signature4.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=a495d0ada1156d7278a56fb754a728d3b9470725208ad422bc299d6d6f793a8b");
    }

    Y_UNIT_TEST(SignWithCanonization) {
        auto time = TInstant::FromValue(30);
        NYql::TAwsSignature signature1("GET", "http://os.com/path?a=3&b=7", "application/json", {}, "key", "pwd", time);
        NYql::TAwsSignature signature2("GET", "http://os.com/path?b=7&a=3", "application/json", {}, "key", "pwd", time);
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetContentType(), signature2.GetContentType());
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetAmzDate(), signature2.GetAmzDate());
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetAuthorization(), signature2.GetAuthorization());
    }
} // Y_UNIT_TEST_SUITE(TAwsSignature)
} // namespace NYql
