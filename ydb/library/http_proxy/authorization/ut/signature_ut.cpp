#include <ydb/library/http_proxy/authorization/signature.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/http_proxy/error/error.h>

using namespace NKikimr::NSQS;

static const TString superSecretAmazonKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

Y_UNIT_TEST_SUITE(TestAwsSignatureV4) {
    Y_UNIT_TEST(TestGetVanilla) {
        const TString request = \
R"__(GET / HTTP/1.1
Host:example.amazonaws.com
X-Amz-Date:20150830T123600Z
Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31)__";

        const TString canonicalizedRequest = \
R"__(GET
/

host:example.amazonaws.com
x-amz-date:20150830T123600Z

host;x-amz-date
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855)__";

        const TString stringToSign = \
R"__(AWS4-HMAC-SHA256
20150830T123600Z
20150830/us-east-1/service/aws4_request
bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63)__";

        TAwsRequestSignV4 signature(request);
        UNIT_ASSERT_EQUAL(canonicalizedRequest, signature.GetCanonicalRequest());
        UNIT_ASSERT_EQUAL(stringToSign, signature.GetStringToSign());
        UNIT_ASSERT_EQUAL("us-east-1", signature.GetRegion());
        UNIT_ASSERT_EQUAL("5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31", signature.CalcSignature(superSecretAmazonKey));
    }

    Y_UNIT_TEST(TestPostVanilla) {
        const TString request = \
R"__(POST / HTTP/1.1
Host:example.amazonaws.com
X-Amz-Date:20150830T123600Z
Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b)__";
        TAwsRequestSignV4 signature(request);
        UNIT_ASSERT_EQUAL("5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b", signature.CalcSignature(superSecretAmazonKey));
    }

    Y_UNIT_TEST(TestPostHeaderCase) {
        const TString request = \
R"__(POST / HTTP/1.1
Host:example.amazonaws.com
My-Header1:VALUE1
X-Amz-Date:20150830T123600Z
Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;my-header1;x-amz-date, Signature=cdbc9802e29d2942e5e10b5bccfdd67c5f22c7c4e8ae67b53629efa58b974b7d)__";
        TAwsRequestSignV4 signature(request);
        UNIT_ASSERT_EQUAL("cdbc9802e29d2942e5e10b5bccfdd67c5f22c7c4e8ae67b53629efa58b974b7d", signature.CalcSignature(superSecretAmazonKey));
    }

    Y_UNIT_TEST(TestGetParamsOrder) {
        const TString request = \
R"__(GET /?Param2=value2&Param1=value1 HTTP/1.1
Host:example.amazonaws.com
X-Amz-Date:20150830T123600Z
Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500)__";
        TAwsRequestSignV4 signature(request);
        UNIT_ASSERT_EQUAL("b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500", signature.CalcSignature(superSecretAmazonKey));
    }

    Y_UNIT_TEST(TestGetUtf8) {
        const TString request = \
R"__(GET /?ሴ=bar HTTP/1.1
Host:example.amazonaws.com
X-Amz-Date:20150830T123600Z
Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=2cdec8eed098649ff3a119c94853b13c643bcf08f8b0a1d91e12c9027818dd04)__";
        TAwsRequestSignV4 signature(request);
        UNIT_ASSERT_EQUAL("2cdec8eed098649ff3a119c94853b13c643bcf08f8b0a1d91e12c9027818dd04", signature.CalcSignature(superSecretAmazonKey));
    }

    Y_UNIT_TEST(TestQueryOrderValue) {
        const TString request = \
R"__(GET /?Param1=value2&Param1=value1 HTTP/1.1
Host:example.amazonaws.com
X-Amz-Date:20150830T123600Z
Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5772eed61e12b33fae39ee5e7012498b51d56abc0abb7c60486157bd471c4694)__";
        TAwsRequestSignV4 signature(request);
        UNIT_ASSERT_EQUAL("5772eed61e12b33fae39ee5e7012498b51d56abc0abb7c60486157bd471c4694", signature.CalcSignature(superSecretAmazonKey));
    }

    Y_UNIT_TEST(TestGetHeaderKeyDuplicate) {
        const TString request = \
R"__(GET / HTTP/1.1
Host:example.amazonaws.com
My-Header1:value4
My-Header1:value1
My-Header1:value3
My-Header1:value2
X-Amz-Date:20150830T123600Z
Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;my-header1;x-amz-date, Signature=08c7e5a9acfcfeb3ab6b2185e75ce8b1deb5e634ec47601a50643f830c755c01)__";
        TAwsRequestSignV4 signature(request);
        UNIT_ASSERT_EQUAL("08c7e5a9acfcfeb3ab6b2185e75ce8b1deb5e634ec47601a50643f830c755c01", signature.CalcSignature(superSecretAmazonKey));
    }

    Y_UNIT_TEST(TestEmptyReq) {
        const TString request = \
R"__(GET /?a=b HTTP/1.1)__";
        TAwsRequestSignV4 signature(request);
        // this signature is totally incorrect, we only test the fact that the code is stable
        UNIT_ASSERT_EQUAL("5b63431f16d95e041cd7507fd76cbc072a1de46c09452621b8f25f1193997c2a", signature.CalcSignature(superSecretAmazonKey));
    }
}
