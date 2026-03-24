#include <ydb/mvp/core/cracked_page.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NMVP;

Y_UNIT_TEST_SUITE(TCrackedPage) {
    Y_UNIT_TEST(ParseFieldsFromUrl) {
        const TCrackedPage page("grpcs://token.endpoint:443/some/path");

        UNIT_ASSERT(page.IsParsed());
        UNIT_ASSERT_VALUES_EQUAL(page.Url, "grpcs://token.endpoint:443/some/path");
        UNIT_ASSERT_VALUES_EQUAL(page.Scheme, "grpcs");
        UNIT_ASSERT_VALUES_EQUAL(page.Host, "token.endpoint:443");
        UNIT_ASSERT_VALUES_EQUAL(page.Uri, "/some/path");
    }

    Y_UNIT_TEST(SchemeChecks) {
        const TCrackedPage noScheme("token.endpoint:443");
        UNIT_ASSERT(noScheme.IsGrpcSchemeAllowed());
        UNIT_ASSERT(noScheme.IsHttpSchemeAllowed());

        const TCrackedPage grpc("grpc://token.endpoint:443");
        UNIT_ASSERT(grpc.IsGrpcSchemeAllowed());
        UNIT_ASSERT(!grpc.IsHttpSchemeAllowed());

        const TCrackedPage http("http://token.endpoint:80");
        UNIT_ASSERT(!http.IsGrpcSchemeAllowed());
        UNIT_ASSERT(http.IsHttpSchemeAllowed());

        const TCrackedPage unknown("ftp://token.endpoint:21");
        UNIT_ASSERT(!unknown.IsGrpcSchemeAllowed());
        UNIT_ASSERT(!unknown.IsHttpSchemeAllowed());
    }

    Y_UNIT_TEST(PolicyChecksViaMethods) {
        const TCrackedPage grpcSecure("grpcs://token.endpoint:443");
        UNIT_ASSERT(grpcSecure.IsGrpcSchemeAllowed());
        UNIT_ASSERT(grpcSecure.IsSecureScheme());

        const TCrackedPage grpcInsecureRejected("grpc://token.endpoint:443");
        UNIT_ASSERT(grpcInsecureRejected.IsGrpcSchemeAllowed());
        UNIT_ASSERT(!grpcInsecureRejected.IsSecureScheme());

        const TCrackedPage httpInsecure("http://token.endpoint:80");
        UNIT_ASSERT(httpInsecure.IsHttpSchemeAllowed());
        UNIT_ASSERT(!httpInsecure.IsSecureScheme());

        const TCrackedPage httpsSecure("https://token.endpoint:443");
        UNIT_ASSERT(httpsSecure.IsHttpSchemeAllowed());
        UNIT_ASSERT(httpsSecure.IsSecureScheme());
    }

    Y_UNIT_TEST(RequestedHostAllowedByWildcard) {
        const TCrackedPage page("https://service.host.net/api");

        UNIT_ASSERT(page.IsRequestedHostAllowed({"*.host.net"}));
        UNIT_ASSERT(page.IsRequestedHostAllowed({"service.host.net"}));
        UNIT_ASSERT(!page.IsRequestedHostAllowed({"*.other.net"}));
    }

    Y_UNIT_TEST(TryGetHostAndPort) {
        TStringBuf scheme;
        TStringBuf host;
        ui16 port = 0;

        const TCrackedPage grpc("grpcs://token.endpoint:443");
        UNIT_ASSERT(grpc.TryGetHostAndPort(scheme, host, port));
        UNIT_ASSERT_VALUES_EQUAL(scheme, "grpcs://");
        UNIT_ASSERT_VALUES_EQUAL(host, "token.endpoint");
        UNIT_ASSERT_VALUES_EQUAL(port, 443);

        scheme = "";
        host = "";
        port = 0;
        const TCrackedPage noScheme("token.endpoint:2135");
        UNIT_ASSERT(noScheme.TryGetHostAndPort(scheme, host, port));
        UNIT_ASSERT_VALUES_EQUAL(scheme, "");
        UNIT_ASSERT_VALUES_EQUAL(host, "token.endpoint");
        UNIT_ASSERT_VALUES_EQUAL(port, 2135);
    }

    Y_UNIT_TEST(TryGetHostAndPortInvalidPort) {
        TStringBuf scheme;
        TStringBuf host;
        ui16 port = 0;

        const TCrackedPage badPort("grpc://token.endpoint:70000");
        UNIT_ASSERT(!badPort.TryGetHostAndPort(scheme, host, port));
    }
}
