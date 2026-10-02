#include <library/cpp/monlib/service/service.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/network/address.h>
#include <util/network/socket.h>
#include <util/stream/str.h>
#include <util/string/ascii.h>

using namespace NMonitoring;

namespace {
    constexpr TStringBuf Body = "Mary had a little lamb.";

    TString ServeResponse(TStringBuf accept, TStringBuf contentType, TStringBuf contentEncoding = {}) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        THttpServerOptions options(port);
        options.SetThreads(1);
        options.EnableCompression(true);

        TMtHttpServer server(
            options,
            [contentType = TString(contentType), contentEncoding = TString(contentEncoding)](IOutputStream& out, const IHttpRequest&) {
                out << "HTTP/1.1 200 OK\r\n"
                    << "Connection: Close\r\n"
                    << "Content-Type: " << contentType << "\r\n"
                    << "Content-Length: " << Body.size() << "\r\n";
                if (contentEncoding) {
                    out << "Content-Encoding: " << contentEncoding << "\r\n";
                }
                out << "\r\n" << Body;
            });
        server.StartOrThrow();

        TSocket socket(TNetworkAddress("localhost", port), TDuration::Seconds(10));
        TSocketOutput socketOutput(socket);
        socketOutput << "GET / HTTP/1.1\r\n"
                     << "Host: localhost\r\n"
                     << "Accept-Encoding: gzip\r\n"
                     << "Accept: " << accept << "\r\n"
                     << "Connection: Close\r\n"
                     << "\r\n";
        socketOutput.Flush();

        TSocketInput socketInput(socket);
        const TString response = socketInput.ReadAll();
        server.Stop();
        return response;
    }

    void AssertEncodingBypassed(const TString& response) {
        const TString lower = to_lower(response);
        UNIT_ASSERT(!lower.Contains("content-encoding:"));
        UNIT_ASSERT(lower.Contains("content-length:"));
        UNIT_ASSERT(response.EndsWith(Body));
    }
}

Y_UNIT_TEST_SUITE(TMonitoringHttpContentEncodingTest) {
    Y_UNIT_TEST(SolomonAcceptBypassesContentEncoding) {
        AssertEncodingBypassed(ServeResponse(
            "application/json, application/x-solomon-spack",
            "application/octet-stream"));
    }

    Y_UNIT_TEST(SolomonContentTypeBypassesContentEncoding) {
        AssertEncodingBypassed(ServeResponse(
            "application/json",
            "Application/X-Solomon-Multi-Spack; version=1"));
    }

    Y_UNIT_TEST(SolomonResponsePreservesExplicitContentEncodingWithoutEncodingBody) {
        const TString response = ServeResponse(
            "application/json",
            "application/x-solomon-spack",
            "gzip");
        const TString lower = to_lower(response);
        UNIT_ASSERT(lower.Contains("content-encoding: gzip"));
        UNIT_ASSERT(lower.Contains("content-length:"));
        UNIT_ASSERT(response.EndsWith(Body));
    }

    Y_UNIT_TEST(RegularResponseIsCompressed) {
        const TString response = ServeResponse("application/json", "application/json");
        const TString lower = to_lower(response);
        UNIT_ASSERT(lower.Contains("content-encoding: gzip"));
        UNIT_ASSERT(!lower.Contains("content-length:"));
        UNIT_ASSERT(!response.EndsWith(Body));
    }
}
