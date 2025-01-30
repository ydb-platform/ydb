#include "yql_aws_signature.h"

#include <util/network/address.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/quote/quote.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TAwsSignature) {
    Y_UNIT_TEST(Sign) {
        NYql::TAwsSignature signature("GET", "http://os.com/my-bucket/year=2024/day=03/", "application/json", {}, "key", "pwd");
        UNIT_ASSERT_VALUES_EQUAL(signature.GetContentType(), "application/json");
        UNIT_ASSERT_VALUES_EQUAL(signature.GetXAmzContentSha256(), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
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
        static const TString X_AMZ_CONTENT_SHA_256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
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
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=32e344ef8df96a217ac1780393acc92caf87e73238e444d290cc713b9e81f7e8");
        UNIT_ASSERT_VALUES_EQUAL(signature2.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=90e3a59639f80cff944aae40f3b4dba3435b3cf39b56e6395722469160c21f23");
        UNIT_ASSERT_VALUES_EQUAL(signature3.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=32e344ef8df96a217ac1780393acc92caf87e73238e444d290cc713b9e81f7e8");
        UNIT_ASSERT_VALUES_EQUAL(signature4.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=29af4a2f6e5be24030218e83c8a516a0511449951b078a9e4792a57e983d111a");
    }

    Y_UNIT_TEST(SignWithCanonization) {
        auto time = TInstant::FromValue(30);
        NYql::TAwsSignature signature1("GET", "http://os.com/path?a=3&b=7", "application/json", {}, "key", "pwd", time);
        NYql::TAwsSignature signature2("GET", "http://os.com/path?b=7&a=3", "application/json", {}, "key", "pwd", time);
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetContentType(), signature2.GetContentType());
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetAmzDate(), signature2.GetAmzDate());
        UNIT_ASSERT_VALUES_EQUAL(signature1.GetAuthorization(), signature2.GetAuthorization());
    }

    Y_UNIT_TEST(SignWithEscaping) {
        auto time = TInstant::FromValue(30);
        NYql::TAwsSignature signature("GET", UrlEscapeRet("http://os.com/my-bucket/ !\"#$%&'()+,-./0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz|~/", true), "application/json", {}, "key", "pwd", time);
        UNIT_ASSERT_VALUES_EQUAL(signature.GetContentType(), "application/json");
        UNIT_ASSERT_VALUES_EQUAL(signature.GetXAmzContentSha256(), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        UNIT_ASSERT_VALUES_EQUAL(signature.GetAuthorization(), "AWS4-HMAC-SHA256 Credential=/19700101///aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=21470c8f999941fdc785c508f0c55afa1a12735eddd868aa7276e532d687c436");
        UNIT_ASSERT_VALUES_UNEQUAL(signature.GetAmzDate(), "");
    }
} // Y_UNIT_TEST_SUITE(TAwsSignature)

} // namespace NYql
