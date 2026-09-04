#include <ydb/core/http_proxy/utils.h>

#include <ydb/library/actors/http/http.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>

using namespace NKikimr::NHttpProxy;

Y_UNIT_TEST_SUITE(SqsRequestEndpoint) {
    Y_UNIT_TEST(EmptyHostWithoutForwardedHost) {
        UNIT_ASSERT_VALUES_EQUAL(MakeSqsRequestEndpoint("", "", false), "");
        UNIT_ASSERT_VALUES_EQUAL(MakeSqsRequestEndpoint("", "X-Forwarded-Proto: https\r\n", false), "");
    }

    Y_UNIT_TEST(UsesHostAndPlainHttp) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint("sqs.ydb.test:8443", "", false),
            "http://sqs.ydb.test:8443");
    }

    Y_UNIT_TEST(UsesTlsWhenEndpointIsSecure) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint("sqs.ydb.test:8443", "", true),
            "https://sqs.ydb.test:8443");
    }

    Y_UNIT_TEST(PrefersForwardedHostAndProto) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "vla5-2135.lbkx.example.net:8771",
                "X-Forwarded-Host: lbkx.example.net:8443\r\n"
                "X-Forwarded-Proto: HTTPS\r\n",
                false),
            "https://lbkx.example.net:8443");
    }

    Y_UNIT_TEST(TakesFirstForwardedTokenAndLowercasesProto) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend:8771",
                "X-Forwarded-Host: lbkx.example.net:8443, backend:8771\r\n"
                "X-Forwarded-Proto: HTTPS, http\r\n",
                false),
            "https://lbkx.example.net:8443");
    }

    Y_UNIT_TEST(IgnoresInvalidForwardedHostAndUsesRequestHost) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "sqs.ydb.test:8443",
                "X-Forwarded-Host: evil.com/phishing#\r\n",
                false),
            "http://sqs.ydb.test:8443");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "sqs.ydb.test:8443",
                "X-Forwarded-Host: evil.com?next=1\r\n",
                true),
            "https://sqs.ydb.test:8443");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "evil.com/phishing#",
                "X-Forwarded-Host: evil.com/phishing#\r\n",
                false),
            "");
    }

    Y_UNIT_TEST(IgnoresUnknownForwardedProtoAndUsesConnectionScheme) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "sqs.ydb.test:8443",
                "X-Forwarded-Proto: ftp\r\n",
                false),
            "http://sqs.ydb.test:8443");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "sqs.ydb.test:8443",
                "X-Forwarded-Proto: FTP, https\r\n",
                true),
            "https://sqs.ydb.test:8443");
    }

    Y_UNIT_TEST(ParsedHttpRequestWithTlsEndpoint) {
        auto endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
        endpoint->Secure = true;
        const TString raw =
            "POST / HTTP/1.1\r\n"
            "Host: vla5-2135.lbkx.example.net:8771\r\n"
            "X-Forwarded-Host: lbkx.example.net:8443\r\n"
            "X-Forwarded-Proto: HTTPS, http\r\n"
            "\r\n";
        auto request = MakeIntrusive<NHttp::THttpIncomingRequest>(
            raw, endpoint, NHttp::THttpConfig::SocketAddressType{});
        const bool tlsSecure = request->Endpoint && request->Endpoint->Secure;
        UNIT_ASSERT(tlsSecure);
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(request->Host, request->Headers, tlsSecure),
            "https://lbkx.example.net:8443");
    }
}
