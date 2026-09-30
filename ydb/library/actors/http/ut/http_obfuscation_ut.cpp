#include <ydb/library/actors/http/http.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NHttp {
namespace {

constexpr TStringBuf SensitiveHeaders[] = {
    "Authorization",
    "Cookie",
    "Set-Cookie",
    "X-Ydb-Auth-Ticket",
    "X-YaCloud-SubjectToken",
};

void CheckHeaderPairs(TStringBuf firstValue, TStringBuf secondValue) {
    for (TStringBuf firstHeader : SensitiveHeaders) {
        for (TStringBuf secondHeader : SensitiveHeaders) {
            const TString raw = TStringBuilder() << firstHeader << ": " << firstValue << "\r\n"
                                    << secondHeader << ": " << secondValue << "\r\n";
            const TString expected = TStringBuilder() << firstHeader << ": <obfuscated>\r\n"
                                        << secondHeader << ": <obfuscated>\r\n";
            UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData(raw), expected);
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(HttpObfuscation) {
    Y_UNIT_TEST(SingleHeader) {
        for (TStringBuf headerName : SensitiveHeaders) {
            const TString raw = TStringBuilder() << headerName << ": example-secret\r\n";
            const TString expected = TStringBuilder() << headerName << ": <obfuscated>\r\n";
            UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData(raw), expected);
        }
    }

    Y_UNIT_TEST(DifferentValues) {
        CheckHeaderPairs("first-secret", "second-secret");
    }

    Y_UNIT_TEST(IdenticalValues) {
        CheckHeaderPairs("example-secret", "example-secret");
    }

    Y_UNIT_TEST(OverlappingValues) {
        CheckHeaderPairs("Bearer example-secret", "Bearer example-secret-suffix");
        CheckHeaderPairs("Bearer example-secret-suffix", "Bearer example-secret");
    }

    Y_UNIT_TEST(RepeatedMixedCaseHeaders) {
        const TString raw = "aUtHoRiZaTiOn: first-secret\r\n"
                            "AUTHORIZATION: second-secret\r\n"
                            "cOoKiE: cookie-secret\r\n"
                            "sEt-CoOkIe: first-cookie\r\n"
                            "SET-COOKIE: second-cookie\r\n"
                            "x-YdB-aUtH-tIcKeT: ticket-secret\r\n"
                            "x-YaClOuD-sUbJeCtToKeN: subject-secret\r\n";
        const TString expected = "aUtHoRiZaTiOn: <obfuscated>\r\n"
                                    "AUTHORIZATION: <obfuscated>\r\n"
                                    "cOoKiE: <obfuscated>\r\n"
                                    "sEt-CoOkIe: <obfuscated>\r\n"
                                    "SET-COOKIE: <obfuscated>\r\n"
                                    "x-YdB-aUtH-tIcKeT: <obfuscated>\r\n"
                                    "x-YaClOuD-sUbJeCtToKeN: <obfuscated>\r\n";
        UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData(raw), expected);
    }

    Y_UNIT_TEST(UnrelatedHeadersAndBody) {
        const TString prefix = "GET /example-secret HTTP/1.1\r\n"
                                "X-Comment: Authorization: example-secret\r\n"
                                "X-Authorization: example-secret\r\n"
                                "Authorization-Info: example-secret\r\n";
        const TString body = "\r\nAuthorization: body-value\r\n"
                                "Cookie: example-secret\r\n";
        const TString raw = prefix + "Authorization: example-secret\r\n" + body;
        const TString expected = prefix + "Authorization: <obfuscated>\r\n" + body;
        UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData(raw), expected);
    }

    Y_UNIT_TEST(WhitespaceAndEmptyValues) {
        const TString raw = "Authorization:secret\r\n"
                            "Cookie:\t  secret with spaces \t\r\n"
                            "Set-Cookie:\r\n"
                            "X-Ydb-Auth-Ticket: \t\r\n";
        const TString expected = "Authorization:<obfuscated>\r\n"
                                    "Cookie:\t  <obfuscated>\r\n"
                                    "Set-Cookie:\r\n"
                                    "X-Ydb-Auth-Ticket: \t\r\n";
        UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData(raw), expected);
    }

    Y_UNIT_TEST(IncompleteHeaders) {
        UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData("Authorization: partial-secret"),
                                    "Authorization: <obfuscated>");
        UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData("Authorization:"), "Authorization:");
        UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData(""), "");
    }

    Y_UNIT_TEST(RequestAndResponse) {
        const TString headers = "Content-Type: text/plain\r\n"
                                "Content-Length: 4\r\n"
                                "X-Ydb-Auth-Ticket: Bearer example-secret-suffix\r\n"
                                "Authorization: Bearer example-secret\r\n\r\nbody";
        const TString expected = "Content-Type: text/plain\r\n"
                                    "Content-Length: 4\r\n"
                                    "X-Ydb-Auth-Ticket: <obfuscated>\r\n"
                                    "Authorization: <obfuscated>\r\n\r\nbody";
        const TString request = "GET / HTTP/1.1\r\n" + headers;
        const TString response = "HTTP/1.1 200 OK\r\n" + headers;
        THttpRequestParser requestParser(request);
        THttpResponseParser responseParser(response);
        UNIT_ASSERT(requestParser.IsReady());
        UNIT_ASSERT(responseParser.IsReady());
        UNIT_ASSERT_VALUES_EQUAL(requestParser.GetObfuscatedData(), "GET / HTTP/1.1\r\n" + expected);
        UNIT_ASSERT_VALUES_EQUAL(responseParser.GetObfuscatedData(), "HTTP/1.1 200 OK\r\n" + expected);

        THttpRequestRenderer requestRenderer;
        THttpResponseRenderer responseRenderer;
        requestRenderer.Assign(request);
        responseRenderer.Assign(response);
        UNIT_ASSERT_VALUES_EQUAL(requestRenderer.GetObfuscatedData(), "GET / HTTP/1.1\r\n" + expected);
        UNIT_ASSERT_VALUES_EQUAL(responseRenderer.GetObfuscatedData(), "HTTP/1.1 200 OK\r\n" + expected);
    }

    Y_UNIT_TEST(LfOnlyAndMixedLineEndings) {
        constexpr TStringBuf lineEndings[] = {"\n", "\r\n"};
        const TString body = "Authorization: body-value\nCookie: body-cookie\r\n";
        for (TStringBuf startLineEnding : lineEndings) {
            for (TStringBuf headerLineEnding : lineEndings) {
                for (TStringBuf separator : lineEndings) {
                    const TString prefix = TStringBuilder()
                        << "Content-Type: text/plain" << headerLineEnding
                        << "Content-Length: " << body.size() << headerLineEnding;
                    const TString headers = TStringBuilder()
                        << prefix
                        << "X-Ydb-Auth-Ticket: Bearer example-secret-suffix" << headerLineEnding
                        << "Authorization: Bearer example-secret" << headerLineEnding
                        << separator << body;
                    const TString expected = TStringBuilder()
                        << prefix
                        << "X-Ydb-Auth-Ticket: <obfuscated>" << headerLineEnding
                        << "Authorization: <obfuscated>" << headerLineEnding
                        << separator << body;

                    const TString requestLine = TStringBuilder() << "GET / HTTP/1.1" << startLineEnding;
                    THttpRequestParser requestParser(requestLine + headers);
                    UNIT_ASSERT(requestParser.IsReady());
                    UNIT_ASSERT_VALUES_EQUAL(requestParser.GetObfuscatedData(), requestLine + expected);

                    const TString responseLine = TStringBuilder() << "HTTP/1.1 200 OK" << startLineEnding;
                    THttpResponseParser responseParser(responseLine + headers);
                    UNIT_ASSERT(responseParser.IsReady());
                    UNIT_ASSERT_VALUES_EQUAL(responseParser.GetObfuscatedData(), responseLine + expected);
                }
            }
        }
    }

    Y_UNIT_TEST(TruncationAfterRedaction) {
        const TString raw = "Authorization: " + TString(3000, 's') + "\r\n";
        UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData(raw), "Authorization: <obfuscated>\r\n");

        const TString body(3000, 'b');
        const TString expected = "Authorization: <obfuscated>\r\n\r\n" + body;
        UNIT_ASSERT_VALUES_EQUAL(GetObfuscatedData(raw + "\r\n" + body),
                                    expected.substr(0, 1000) + " --- <truncated> --- " + expected.substr(expected.size() - 1000));
    }
}

} // namespace NHttp
