#include "httpfsm.h"
#include "library-htfetch_ut_hreflang_in.h"
#include "library-htfetch_ut_hreflang_out.h"

#include <util/generic/ptr.h>
#include <library/cpp/charset/doccodes.h>
#include <library/cpp/testing/unittest/registar.h>

class THttpHeaderParserTestSuite: public TTestBase {
    UNIT_TEST_SUITE(THttpHeaderParserTestSuite);
    UNIT_TEST(TestRequestHeader);
    UNIT_TEST(TestSplitRequestHeader);
    UNIT_TEST(TestTrailingData);
    UNIT_TEST(TestProxyRequestHeader);
    UNIT_TEST(TestIncorrectRequestHeader);
    UNIT_TEST(TestLastModified);
    UNIT_TEST(TestLastModifiedCorrupted);
    UNIT_TEST(TestResponseHeaderOnRequest);
    UNIT_TEST(TestRequestHeaderOnResponse);
    UNIT_TEST(TestXRobotsTagUnknownTags);
    UNIT_TEST(TestXRobotsTagMyBot);
    UNIT_TEST(TestXRobotsTagOtherBot);
    UNIT_TEST(TestXRobotsTagUnavailableAfterAware);
    UNIT_TEST(TestXRobotsTagUnavailableAfterWorks);
    UNIT_TEST(TestXRobotsTagOverridePriority);
    UNIT_TEST(TestXRobotsTagDoesNotBreakCharset);
    UNIT_TEST(TestXRobotsTagAllowsMultiline);
    UNIT_TEST(TestRelCanonical);
    UNIT_TEST(TestHreflang);
    UNIT_TEST(TestHreflangOnLongInput);
    UNIT_TEST(TestMimeType);
    UNIT_TEST(TestRepeatedContentEncoding);
    UNIT_TEST_SUITE_END();

private:
    THolder<THttpHeaderParser> httpHeaderParser;

private:
    void TestStart();
    void TestFinish();

public:
    void TestRequestHeader();
    void TestSplitRequestHeader();
    void TestTrailingData();
    void TestProxyRequestHeader();
    void TestIncorrectRequestHeader();
    void TestLastModified();
    void TestLastModifiedCorrupted();
    void TestResponseHeaderOnRequest();
    void TestRequestHeaderOnResponse();
    void TestXRobotsTagUnknownTags();
    void TestXRobotsTagMyBot();
    void TestXRobotsTagOtherBot();
    void TestXRobotsTagUnavailableAfterAware();
    void TestXRobotsTagUnavailableAfterWorks();
    void TestXRobotsTagOverridePriority();
    void TestXRobotsTagDoesNotBreakCharset();
    void TestXRobotsTagAllowsMultiline();
    void TestRelCanonical();
    void TestHreflang();
    void TestHreflangOnLongInput();
    void TestMimeType();
    void TestRepeatedContentEncoding();
};

void THttpHeaderParserTestSuite::TestStart() {
    httpHeaderParser.Reset(new THttpHeaderParser());
}

void THttpHeaderParserTestSuite::TestFinish() {
    httpHeaderParser.Reset();
}

void THttpHeaderParserTestSuite::TestRequestHeader() {
    TestStart();
    THttpRequestHeader httpRequestHeader;
    httpHeaderParser->Init(&httpRequestHeader);
    const char* request = "GET /search?q=hi HTTP/1.1\r\n"
                          "Host: www.google.ru:8080\r\n\r\n";
    i32 result = httpHeaderParser->Execute(request, strlen(request));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpRequestHeader.http_method, HTTP_METHOD_GET);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.host, "www.google.ru:8080"), 0);
    UNIT_ASSERT_EQUAL(httpRequestHeader.request_uri, "/search?q=hi");
    UNIT_ASSERT_EQUAL(httpRequestHeader.GetUrl(), "http://www.google.ru:8080/search?q=hi");
    UNIT_ASSERT_EQUAL(httpHeaderParser->lastchar - request + 1,
                      (i32)strlen(request));
    UNIT_ASSERT_EQUAL(httpRequestHeader.x_yandex_response_timeout,
                      DEFAULT_RESPONSE_TIMEOUT);
    UNIT_ASSERT_EQUAL(httpRequestHeader.x_yandex_request_priority,
                      DEFAULT_REQUEST_PRIORITY);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.x_yandex_sourcename, ""), 0);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.x_yandex_requesttype, ""), 0);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.x_yandex_fetchoptions, ""), 0);
    TestFinish();
    UNIT_ASSERT_EQUAL(httpRequestHeader.max_age, DEFAULT_MAX_AGE);
}

void THttpHeaderParserTestSuite::TestSplitRequestHeader() {
    TestStart();
    const char* request =
        "GET /search?q=hi HTTP/1.1\r\n"
        "Host:  www.google.ru:8080 \r\n"
        "\r\n";
    const size_t rlen = strlen(request);

    for (size_t n1 = 0; n1 < rlen; n1++) {
        for (size_t n2 = n1; n2 < rlen; n2++) {
            TString s1{request, 0, n1};
            TString s2{request, n1, n2 - n1};
            TString s3{request, n2, rlen - n2};
            UNIT_ASSERT_EQUAL(s1 + s2 + s3, request);

            THttpRequestHeader httpRequestHeader;
            UNIT_ASSERT(0 == httpHeaderParser->Init(&httpRequestHeader));
            i32 result = httpHeaderParser->Execute(s1);
            UNIT_ASSERT_EQUAL(result, 1);
            result = httpHeaderParser->Execute(s2);
            UNIT_ASSERT_EQUAL(result, 1);
            result = httpHeaderParser->Execute(s3);
            UNIT_ASSERT_EQUAL(result, 2);

            UNIT_ASSERT_EQUAL(httpRequestHeader.http_method, HTTP_METHOD_GET);
            UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.host, "www.google.ru:8080"), 0);
            UNIT_ASSERT_EQUAL(httpRequestHeader.request_uri, "/search?q=hi");
        }
    }

    TestFinish();
}

void THttpHeaderParserTestSuite::TestTrailingData() {
    TestStart();
    THttpRequestHeader httpRequestHeader;
    UNIT_ASSERT(0 == httpHeaderParser->Init(&httpRequestHeader));
    const char* request =
        "GET /search?q=hi HTTP/1.1\r\n"
        "Host: www.google.ru:8080\r\n"
        "\r\n"
        "high.ru";
    i32 result = httpHeaderParser->Execute(request, strlen(request));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpRequestHeader.http_method, HTTP_METHOD_GET);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.host, "www.google.ru:8080"), 0);
    UNIT_ASSERT_EQUAL(httpRequestHeader.request_uri, "/search?q=hi");
    UNIT_ASSERT_EQUAL(TString(httpHeaderParser->lastchar + 1), "high.ru");
    UNIT_ASSERT_EQUAL(httpRequestHeader.http_minor, 1);
    UNIT_ASSERT_EQUAL(httpRequestHeader.transfer_chunked, -1);
    UNIT_ASSERT_EQUAL(httpRequestHeader.content_length, -1);
    UNIT_ASSERT_EQUAL(httpRequestHeader.connection_closed, -1);
    TestFinish();
}

void THttpHeaderParserTestSuite::TestProxyRequestHeader() {
    TestStart();
    THttpRequestHeader httpRequestHeader;
    httpHeaderParser->Init(&httpRequestHeader);
    const char* request =
        "GET http://www.google.ru:8080/search?q=hi HTTP/1.1\r\n"
        "X-Yandex-Response-Timeout: 1000\r\n"
        "X-Yandex-Request-Priority: 2\r\n"
        "X-Yandex-Sourcename: orange\r\n"
        "X-Yandex-Requesttype: userproxy\r\n"
        "X-Yandex-FetchOptions: d;c\r\n"
        "Cache-control: max-age=100\r\n"
        "If-Modified-Since: Sat, 29 Oct 1994 19:43:31 GMT\r\n"
        "User-Agent: Yandex/1.01.001 (compatible; Win16; I)\r\n"
        "From: webadmin@yandex.ru\r\n\r\n";
    i32 result = httpHeaderParser->Execute(request, strlen(request));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpRequestHeader.http_method, HTTP_METHOD_GET);
    UNIT_ASSERT_EQUAL(httpRequestHeader.x_yandex_response_timeout, 1000);
    UNIT_ASSERT_EQUAL(httpRequestHeader.x_yandex_request_priority, 2);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.x_yandex_sourcename, "orange"), 0);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.x_yandex_requesttype, "userproxy"), 0);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.x_yandex_fetchoptions, "d;c"), 0);
    UNIT_ASSERT_EQUAL(httpRequestHeader.max_age, 100);
    UNIT_ASSERT_VALUES_EQUAL(httpRequestHeader.if_modified_since,
                             TInstant::ParseIso8601Deprecated("1994-10-29 19:43:31Z").TimeT());
    UNIT_ASSERT_EQUAL(httpRequestHeader.request_uri,
                      "http://www.google.ru:8080/search?q=hi");
    UNIT_ASSERT(httpRequestHeader.GetUrl() ==
                "http://www.google.ru:8080/search?q=hi");
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.host, ""), 0);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.from, "webadmin@yandex.ru"), 0);
    UNIT_ASSERT_EQUAL(strcmp(httpRequestHeader.user_agent,
                             "Yandex/1.01.001 (compatible; Win16; I)"),
                      0);
    UNIT_ASSERT_EQUAL(httpHeaderParser->lastchar - request + 1,
                      (i32)strlen(request));
    TestFinish();
}

void THttpHeaderParserTestSuite::TestIncorrectRequestHeader() {
    TestStart();
    THttpRequestHeader httpRequestHeader;
    httpHeaderParser->Init(&httpRequestHeader);
    const char* request = "GET /search?q=hi HTP/1.1\r\n"
                          "Host: www.google.ru:8080\r\n\r\n";
    i32 result = httpHeaderParser->Execute(request, strlen(request));
    UNIT_ASSERT(result != 2);
    TestFinish();
}

void THttpHeaderParserTestSuite::TestLastModified() {
    TestStart();
    THttpHeader h;
    UNIT_ASSERT(0 == httpHeaderParser->Init(&h));
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "Last-Modified: Thu, 13 Aug 2009 14:27:08 GMT\r\n\r\n";
    UNIT_ASSERT(2 == httpHeaderParser->Execute(headers, strlen(headers)));
    UNIT_ASSERT_VALUES_EQUAL(
        TInstant::ParseIso8601Deprecated("2009-08-13 14:27:08Z").TimeT(),
        h.http_time);
    TestFinish();
}

void THttpHeaderParserTestSuite::TestLastModifiedCorrupted() {
    TestStart();
    THttpHeader h;
    UNIT_ASSERT(0 == httpHeaderParser->Init(&h));
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "Last-Modified: Thu, 13 Aug 2009 14:\r\n\r\n";
    UNIT_ASSERT(2 == httpHeaderParser->Execute(headers, strlen(headers)));
    UNIT_ASSERT(h.http_time < 0); // XXX: don't understand what is the proper value
    TestFinish();
}

void THttpHeaderParserTestSuite::TestXRobotsTagUnknownTags() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "x-robots-tag: asdfasdf asdf asdf,,, , noindex,noodpXXX , NOFOLLOW ,noodpnofollow\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_tag, 3);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_state, "00xxx");
    TestFinish();
}

void THttpHeaderParserTestSuite::TestXRobotsTagMyBot() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "x-robots-tag: yandex: noindex, nofollow\r\n"
        "x-robots-tag: yandexbot: noarchive, noodp\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_tag, 15);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_state, "0000x");
    TestFinish();
}

void THttpHeaderParserTestSuite::TestXRobotsTagOtherBot() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "x-robots-tag: google: noindex, nofollow\r\n"
        "x-robots-tag: googlebot: noarchive, noodp\r\n"
        "x-robots-tag: !still(-other) bot_: foo, noyaca\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_tag, 0);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_state, "xxxxx");
    TestFinish();
}

void THttpHeaderParserTestSuite::TestXRobotsTagUnavailableAfterAware() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    // проверяем только что unavailable_after ничего не ломает
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "x-robots-tag: unavailable_after: 01 Jan 2999 00:00 UTC, noindex, nofollow\r\n"
        "x-robots-tag: yandex: unavailable_after: 01 Jan 2999 00:00 UTC, noarchive, noodp\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_tag, 15);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_state, "0000x");
    TestFinish();
}

void THttpHeaderParserTestSuite::TestXRobotsTagUnavailableAfterWorks() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    // пока не поддерживается
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "x-robots-tag: unavailable_after: 01 Jan 2000 00:00 UTC\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    //UNIT_ASSERT_EQUAL(httpHeader.x_robots_tag, 1);
    //UNIT_ASSERT_EQUAL(httpHeader.x_robots_state, "0xxxx");
    TestFinish();
}

void THttpHeaderParserTestSuite::TestXRobotsTagOverridePriority() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "x-robots-tag: all, none\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_state, "11xxx");
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_tag, 3); // NOTE legacy behavior, should be 0 as `all` overrides
    TestFinish();
}

void THttpHeaderParserTestSuite::TestXRobotsTagDoesNotBreakCharset() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "X-Robots-Tag: noarchive\r\n"
        "Content-Type: application/json; charset=utf-8\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.mime_type, static_cast<ui8>(MIME_JSON));
    UNIT_ASSERT_EQUAL(httpHeader.charset, static_cast<ui8>(CODES_UTF8));
    TestFinish();
}

void THttpHeaderParserTestSuite::TestXRobotsTagAllowsMultiline() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "X-Robots-Tag\r\n"
        " :\r\n"
        " unavailable_since\r\n"
        " :\r\n"
        " ,\r\n"
        " unavailable_since\r\n"
        " :\r\n"
        " 01 Jan 2000\r\n"
        " 00:00 UTC\r\n"
        " ,\r\n"
        " yandexbot\r\n"
        " :\r\n"
        " noindex\r\n"
        " ,\r\n"
        " garbage\r\n"
        " ,\r\n"
        " nofollow\r\n"
        " ,\r\n"
        " other\r\n"
        " bot\r\n"
        " :\r\n"
        " noarchive\r\n"
        " ,\r\n"
        "Content-Type: application/json; charset=utf-8\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.x_robots_state, "00xxx");
    UNIT_ASSERT_EQUAL(httpHeader.mime_type, static_cast<ui8>(MIME_JSON));
    UNIT_ASSERT_EQUAL(httpHeader.charset, static_cast<ui8>(CODES_UTF8));
    TestFinish();
}

void THttpHeaderParserTestSuite::TestHreflang() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "link: <http://www.high.ru/>; rel='alternate'; hreflang='x-default'\r\n"
        "link:  <http://www.high.ru/en.html> ;rel  =  'alternate'  ;hreflang =   en_GB  \r\n"
        "link:  <http://www.high.ru/ru.html>;hreflang =   ru_RU.KOI8-r   ;rel  =  'alternate'  \r\n"
        "\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_VALUES_EQUAL(result, 2);
    // UNIT_ASSERT_VALUES_EQUAL(strcmp(httpHeader.hreflangs, "x-default http://www.high.ru/;"), 0);
    UNIT_ASSERT_VALUES_EQUAL(httpHeader.hreflangs, "x-default http://www.high.ru/\ten_GB http://www.high.ru/en.html\tru_RU.KOI8-r http://www.high.ru/ru.html");
    TestFinish();
}

void THttpHeaderParserTestSuite::TestHreflangOnLongInput() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    TStringBuf testInput(hreflang_ut_in);
    TStringBuf testOut(hreflang_ut_out);
    i32 result = httpHeaderParser->Execute(testInput.data(), testInput.size());
    UNIT_ASSERT_VALUES_EQUAL(result, 2);
    UNIT_ASSERT_VALUES_EQUAL(httpHeader.hreflangs, testOut);
    TestFinish();
}

void THttpHeaderParserTestSuite::TestRelCanonical() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "Link: <http://yandex.ru>; rel = \"canonical\"\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.rel_canonical, "http://yandex.ru");
    TestFinish();
}

void THttpHeaderParserTestSuite::TestResponseHeaderOnRequest() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* request = "GET /search?q=hi HTP/1.1\r\n"
                          "Host: www.google.ru:8080\r\n\r\n";
    i32 result = httpHeaderParser->Execute(request, strlen(request));
    UNIT_ASSERT_EQUAL(result, -3);
    TestFinish();
}

void THttpHeaderParserTestSuite::TestRequestHeaderOnResponse() {
    TestStart();
    THttpRequestHeader httpRequestHeader;
    httpHeaderParser->Init(&httpRequestHeader);
    const char* response = "HTTP/1.1 200 OK\r\n"
                           "Content-Type: text/html\r\n"
                           "Last-Modified: Thu, 13 Aug 2009 14:\r\n\r\n";
    i32 result = httpHeaderParser->Execute(response, strlen(response));
    UNIT_ASSERT_EQUAL(result, -3);
    TestFinish();
}

void THttpHeaderParserTestSuite::TestMimeType() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char* headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json; charset=utf-8\r\n\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.mime_type, static_cast<ui8>(MIME_JSON));
    UNIT_ASSERT_EQUAL(httpHeader.charset, static_cast<ui8>(CODES_UTF8));
    TestFinish();
}

void THttpHeaderParserTestSuite::TestRepeatedContentEncoding() {
    TestStart();
    THttpHeader httpHeader;
    httpHeaderParser->Init(&httpHeader);
    const char *headers =
        "HTTP/1.1 200 OK\r\n"
        "Server: nginx\r\n"
        "Date: Mon, 15 Oct 2018 10:40:44 GMT\r\n"
        "Content-Type: text/plain\r\n"
        "Transfer-Encoding: chunked\r\n"
        "Connection: keep-alive\r\n"
        "Last-Modified: Mon, 15 Oct 2018 03:48:54 GMT\r\n"
        "ETag: W/\"5bc40e26-a956d\"\r\n"
        "X-Autoru-LB: lb-03-sas.prod.vertis.yandex.net\r\n"
        "Content-Encoding: gzip\r\n"
        "Content-Encoding: gzip\r\n"
        "X-UA-Bot: 1\r\n"
        "\r\n";
    i32 result = httpHeaderParser->Execute(headers, strlen(headers));
    UNIT_ASSERT_EQUAL(result, 2);
    UNIT_ASSERT_EQUAL(httpHeader.error, 0);
    UNIT_ASSERT_EQUAL(httpHeader.compression_method, 3);
    TestFinish();
}

UNIT_TEST_SUITE_REGISTRATION(THttpHeaderParserTestSuite);

Y_UNIT_TEST_SUITE(TestHttpChunkParser) {
    static THttpChunkParser initParser() {
        THttpChunkParser parser;
        parser.Init();
        return parser;
    }

    static THttpChunkParser parseByteByByte(const TStringBuf& blob, const TVector<int>& states) {
        UNIT_ASSERT(states.size() <= blob.size());
        THttpChunkParser parser{initParser()};
        for (size_t n = 0; n < states.size(); n++) {
            const TStringBuf d{blob, n, 1};
            int code = parser.Execute(d.data(), d.size());
            Cout << TString(d).Quote() << " " << code << Endl;
            UNIT_ASSERT_EQUAL(code, states[n]);
        }
        return parser;
    }

    static THttpChunkParser parseBytesWithLastState(const TStringBuf& blob, const int last_state) {
        TVector<int> states(blob.size() - 1, 1);
        states.push_back(last_state);
        return parseByteByByte(blob, states);
    }

    Y_UNIT_TEST(TestWithoutEolHead) {
        const TStringBuf blob{
            "4\r\n"
            "____\r\n"};
        TVector<int> states{
            -1, /* 1, -1,
            1, -1,  1, -1, 1, -1 */};
        // as soon as error happens parser state should be considered
        // undefined, state is meaningless after the very first `-1`
        // moreover, testenv produces `states[1] == -1` for this input and
        // my local build produces `states[1] == 1`.
        parseByteByByte(blob, states);
    }

    Y_UNIT_TEST(TestTrivialChunk) {
        const TStringBuf blob{
            "\r\n"
            "4\r\n"};
        THttpChunkParser parser(parseBytesWithLastState(blob, 2));
        UNIT_ASSERT_EQUAL(parser.chunk_length, 4);
        UNIT_ASSERT_EQUAL(parser.cnt64, 4);
    }

    Y_UNIT_TEST(TestNegative) {
        const TStringBuf blob{
            "\r\n"
            "-1"};
        TVector<int> states{
            1, 1,
            -1,
            /* 1 */};
        parseByteByByte(blob, states);
    }

    Y_UNIT_TEST(TestLeadingZero) {
        const TStringBuf blob{
            "\r\n"
            "042\r\n"};
        THttpChunkParser parser(parseBytesWithLastState(blob, 2));
        UNIT_ASSERT_EQUAL(parser.chunk_length, 0x42);
    }

    Y_UNIT_TEST(TestIntOverflow) {
        const TStringBuf blob{
            "\r\n"
            "deadbeef"};
        THttpChunkParser parser(parseBytesWithLastState(blob, -2));
        UNIT_ASSERT_EQUAL(parser.chunk_length, 0);
        UNIT_ASSERT_EQUAL(parser.cnt64, 0xdeadbeef);
    }

    Y_UNIT_TEST(TestTrivialChunkWithTail) {
        const TStringBuf blob{
            "\r\n"
            "4\r\n"
            "_" // first byte of the chunk
        };
        TVector<int> states{
            1, 1,
            1, 1, 2,
            -1};
        parseByteByByte(blob, states);
    }

    Y_UNIT_TEST(TestLastChunk) {
        // NB: current parser does not permit whitespace before `foo`,
        // but I've never seen the feature in real-life traffic
        const TStringBuf blob{
            "\r\n"
            "000 ;foo = bar \r\n"
            "Trailer: bar\r\n"
            "\r\n"};
        THttpChunkParser parser(parseBytesWithLastState(blob, 2));
        UNIT_ASSERT_EQUAL(parser.chunk_length, 0);
    }
}
