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

    Y_UNIT_TEST(ForwardedHeaderPresenceMatrix) {
        struct TCase {
            const char* Name;
            TStringBuf Host;
            TStringBuf Headers;
            bool Tls;
            TStringBuf Expected;
        };

        const TCase cases[] = {
            // No headers at all
            {"empty", "", "", false, ""},
            {"empty_tls", "", "", true, ""},

            // Host only
            {"host_port", "backend.internal:8771", "", false, "http://backend.internal:8771"},
            {"host_port_tls", "backend.internal:8771", "", true, "https://backend.internal:8771"},
            {"host", "backend.internal", "", false, "http://backend.internal"},
            {"host_tls", "backend.internal", "", true, "https://backend.internal"},

            // Only X-Forwarded-Host
            {"xfh", "backend.internal:8771",
             "X-Forwarded-Host: example.com\r\n", false, "http://example.com"},
            {"xfh_port", "backend.internal:8771",
             "X-Forwarded-Host: example.com:8080\r\n", false, "http://example.com:8080"},
            {"xfh_empty_host", "",
             "X-Forwarded-Host: example.com\r\n", false, "http://example.com"},
            {"xfh_port_empty_host", "",
             "X-Forwarded-Host: example.com:8080\r\n", false, "http://example.com:8080"},

            // Only X-Forwarded-Port
            {"xfp_overrides_host_port", "backend.internal:8771",
             "X-Forwarded-Port: 8443\r\n", false, "http://backend.internal:8443"},
            {"xfp_on_host", "backend.internal",
             "X-Forwarded-Port: 8443\r\n", false, "http://backend.internal:8443"},
            {"xfp_empty_host", "",
             "X-Forwarded-Port: 8443\r\n", false, ""},

            // Only X-Forwarded-Proto
            {"xproto_https", "backend.internal:8771",
             "X-Forwarded-Proto: https\r\n", false, "https://backend.internal:8771"},
            {"xproto_https_host", "backend.internal",
             "X-Forwarded-Proto: https\r\n", false, "https://backend.internal"},
            {"xproto_http_overrides_tls", "backend.internal:8771",
             "X-Forwarded-Proto: http\r\n", true, "http://backend.internal:8771"},
            {"xproto_empty_host", "",
             "X-Forwarded-Proto: https\r\n", false, ""},

            // X-Forwarded-Host + X-Forwarded-Port
            {"xfh_xfp", "backend.internal:8771",
             "X-Forwarded-Host: example.com\r\n"
             "X-Forwarded-Port: 8443\r\n",
             false, "http://example.com:8443"},
            {"xfh_port_xfp", "backend.internal:8771",
             "X-Forwarded-Host: example.com:8080\r\n"
             "X-Forwarded-Port: 8443\r\n",
             false, "http://example.com:8443"},
            {"xfh_xfp_empty_host", "",
             "X-Forwarded-Host: example.com\r\n"
             "X-Forwarded-Port: 8443\r\n",
             false, "http://example.com:8443"},

            // X-Forwarded-Host + X-Forwarded-Proto
            {"xfh_xproto", "backend.internal:8771",
             "X-Forwarded-Host: example.com\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://example.com"},
            {"xfh_port_xproto", "backend.internal:8771",
             "X-Forwarded-Host: example.com:8080\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://example.com:8080"},
            {"xfh_xproto_empty_host", "",
             "X-Forwarded-Host: example.com\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://example.com"},

            // X-Forwarded-Port + X-Forwarded-Proto
            {"xfp_xproto", "backend.internal:8771",
             "X-Forwarded-Port: 8443\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://backend.internal:8443"},
            {"xfp_xproto_host", "backend.internal",
             "X-Forwarded-Port: 8443\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://backend.internal:8443"},
            {"xfp_default_https", "backend.internal",
             "X-Forwarded-Port: 443\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://backend.internal"},
            {"xfp_xproto_empty_host", "",
             "X-Forwarded-Port: 8443\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, ""},

            // All three
            {"all", "backend.internal:8771",
             "X-Forwarded-Host: example.com\r\n"
             "X-Forwarded-Port: 8443\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://example.com:8443"},
            {"all_xfh_port_overridden", "backend.internal:8771",
             "X-Forwarded-Host: example.com:8080\r\n"
             "X-Forwarded-Port: 8443\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://example.com:8443"},
            {"all_default_https_port", "backend.internal:8771",
             "X-Forwarded-Host: example.com\r\n"
             "X-Forwarded-Port: 443\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://example.com"},
            {"all_empty_host", "",
             "X-Forwarded-Host: example.com\r\n"
             "X-Forwarded-Port: 8443\r\n"
             "X-Forwarded-Proto: https\r\n",
             false, "https://example.com:8443"},
        };

        for (const auto& c : cases) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                MakeSqsRequestEndpoint(c.Host, c.Headers, c.Tls),
                c.Expected,
                c.Name);
        }
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

    Y_UNIT_TEST(AcceptsForwardedHostWithDomainAndNonStandardPort) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com:8080\r\n",
                false),
            "http://example.com:8080");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com:8080\r\n"
                "X-Forwarded-Proto: https\r\n",
                false),
            "https://example.com:8080");
    }

    Y_UNIT_TEST(PrefersForwardedPortOverHostPort) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com:8080\r\n"
                "X-Forwarded-Port: 9090\r\n",
                false),
            "http://example.com:9090");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com:8080\r\n"
                "X-Forwarded-Port: 8443\r\n"
                "X-Forwarded-Proto: https\r\n",
                false),
            "https://example.com:8443");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com\r\n"
                "X-Forwarded-Port: 8080\r\n",
                false),
            "http://example.com:8080");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Port: 8443\r\n",
                false),
            "http://backend.internal:8443");
    }

    Y_UNIT_TEST(TakesFirstForwardedPortToken) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com:8080\r\n"
                "X-Forwarded-Port: 8443, 8771\r\n",
                false),
            "http://example.com:8443");
    }

    Y_UNIT_TEST(IgnoresInvalidForwardedPortAndUsesHostPort) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com:8080\r\n"
                "X-Forwarded-Port: abc\r\n",
                false),
            "http://example.com:8080");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com:8080\r\n"
                "X-Forwarded-Port: 0\r\n",
                false),
            "http://example.com:8080");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com:8080\r\n"
                "X-Forwarded-Port: 65536\r\n",
                false),
            "http://example.com:8080");
    }

    Y_UNIT_TEST(UsesDefaultPortFromProtoWhenNoPortSpecified) {
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com\r\n"
                "X-Forwarded-Proto: https\r\n",
                false),
            "https://example.com");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com\r\n"
                "X-Forwarded-Proto: http\r\n",
                false),
            "http://example.com");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com\r\n"
                "X-Forwarded-Port: 443\r\n"
                "X-Forwarded-Proto: https\r\n",
                false),
            "https://example.com");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com\r\n"
                "X-Forwarded-Port: 80\r\n"
                "X-Forwarded-Proto: http\r\n",
                false),
            "http://example.com");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(
                "backend.internal:8771",
                "X-Forwarded-Host: example.com\r\n"
                "X-Forwarded-Port: 443\r\n"
                "X-Forwarded-Proto: http\r\n",
                false),
            "http://example.com:443");
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

    Y_UNIT_TEST(ParsedHttpRequestPrefersForwardedPort) {
        auto endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
        endpoint->Secure = false;
        const TString raw =
            "POST / HTTP/1.1\r\n"
            "Host: vla5-2135.lbkx.example.net:8771\r\n"
            "X-Forwarded-Host: example.com:8080\r\n"
            "X-Forwarded-Port: 8443\r\n"
            "X-Forwarded-Proto: https\r\n"
            "\r\n";
        auto request = MakeIntrusive<NHttp::THttpIncomingRequest>(
            raw, endpoint, NHttp::THttpConfig::SocketAddressType{});
        UNIT_ASSERT_VALUES_EQUAL(
            MakeSqsRequestEndpoint(request->Host, request->Headers, false),
            "https://example.com:8443");
    }
}
