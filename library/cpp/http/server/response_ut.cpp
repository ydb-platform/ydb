#include "response.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

Y_UNIT_TEST_SUITE(TestHttpResponse) {
    Y_UNIT_TEST(TestCodeOnly) {
        UNIT_ASSERT_STRINGS_EQUAL(ToString(THttpResponse()), "HTTP/1.1 200 Ok\r\n\r\n");
        UNIT_ASSERT_STRINGS_EQUAL(ToString(THttpResponse(HTTP_NOT_FOUND)), "HTTP/1.1 404 Not found\r\n\r\n");
    }

    Y_UNIT_TEST(TestRedirect) {
        THttpResponse resp = THttpResponse(HTTP_FOUND).AddHeader("Location", "yandex.ru");
        UNIT_ASSERT_STRINGS_EQUAL(ToString(resp), "HTTP/1.1 302 Moved temporarily\r\n"
                                                  "Location: yandex.ru\r\n"
                                                  "\r\n");
    }

    Y_UNIT_TEST(TestAddHeader) {
        THttpResponse resp(HTTP_FORBIDDEN);
        resp.AddHeader(THttpInputHeader("X-Header-1", "ValueOne"));
        resp.AddHeader("X-Header-2", 10);
        resp.AddHeader("X-Header-3", true);

        const char* EXPECTED = "HTTP/1.1 403 Forbidden\r\n"
                               "X-Header-1: ValueOne\r\n"
                               "X-Header-2: 10\r\n"
                               "X-Header-3: 1\r\n"
                               "\r\n";
        UNIT_ASSERT_STRINGS_EQUAL(ToString(resp), EXPECTED);
    }

    Y_UNIT_TEST(TestAddMultipleHeaders) {
        THttpHeaders headers;
        headers.AddHeader(THttpInputHeader("X-Header-1", "ValueOne"));
        headers.AddHeader(THttpInputHeader("X-Header-2", "ValueTwo"));
        headers.AddHeader(THttpInputHeader("X-Header-3", "ValueThree"));

        const char* EXPECTED = "HTTP/1.1 403 Forbidden\r\n"
                               "X-Header-1: ValueOne\r\n"
                               "X-Header-2: ValueTwo\r\n"
                               "X-Header-3: ValueThree\r\n"
                               "\r\n";
        UNIT_ASSERT_STRINGS_EQUAL(ToString(THttpResponse(HTTP_FORBIDDEN).AddMultipleHeaders(headers)),
                                  EXPECTED);
    }

    Y_UNIT_TEST(TestGetHeaders) {
        THttpResponse resp(HTTP_FORBIDDEN);

        THttpHeaders headers;
        headers.AddHeader(THttpInputHeader("X-Header-1", "ValueOne"));
        headers.AddHeader(THttpInputHeader("X-Header-2", "ValueTwo"));
        headers.AddHeader(THttpInputHeader("X-Header-3", "ValueThree"));
        resp.AddMultipleHeaders(headers);
        resp.AddHeader("X-Header-4", "ValueFour");

        const THttpHeaders& gotHeaders = resp.GetHeaders();
        UNIT_ASSERT_VALUES_EQUAL(gotHeaders.Count(), 4);
        UNIT_ASSERT(gotHeaders.HasHeader("X-Header-1"));
        UNIT_ASSERT_STRINGS_EQUAL(gotHeaders.FindHeader("X-Header-1")->Value(), "ValueOne");
        UNIT_ASSERT(gotHeaders.HasHeader("X-Header-4"));
        UNIT_ASSERT_STRINGS_EQUAL(gotHeaders.FindHeader("X-Header-4")->Value(), "ValueFour");
    }


    Y_UNIT_TEST(TestSetContent) {
        const char* EXPECTED = "HTTP/1.1 200 Ok\r\n"
                               "Content-Length: 10\r\n"
                               "\r\n"
                               "0123456789";
        UNIT_ASSERT_STRINGS_EQUAL(ToString(THttpResponse().SetContent("0123456789")),
                                  EXPECTED);
    }

    Y_UNIT_TEST(TestSetContentWithContentType) {
        const char* EXPECTED = "HTTP/1.1 200 Ok\r\n"
                               "Content-Type: text/xml\r\n"
                               "Content-Length: 28\r\n"
                               "\r\n"
                               "<xml><tag value=\"1\" /></xml>";
        THttpResponse resp;
        resp.SetContent("<xml><tag value=\"1\" /></xml>").SetContentType("text/xml");
        UNIT_ASSERT_STRINGS_EQUAL(ToString(resp), EXPECTED);
    }

    Y_UNIT_TEST(TestCopyConstructor) {
        THttpResponse resp(HTTP_FORBIDDEN);
        resp.AddHeader(THttpInputHeader("X-Header-1", "ValueOne"))
            .AddHeader("X-Header-2", "ValueTwo")
            .AddHeader(THttpInputHeader("X-Header-3", "ValueThree"))
            .SetContent("Some stuff")
            .SetContentType("text/plain");

        THttpResponse copy = resp;
        UNIT_ASSERT_STRINGS_EQUAL(ToString(copy), ToString(resp));
    }

    Y_UNIT_TEST(TestAssignment) {
        THttpResponse resp(HTTP_FORBIDDEN);
        resp.AddHeader(THttpInputHeader("X-Header-1", "ValueOne"));
        resp.AddHeader(THttpInputHeader("X-Header-2", "ValueTwo"));
        resp.AddHeader(THttpInputHeader("X-Header-3", "ValueThree"));
        resp.SetContent("Some stuff").SetContentType("text/plain");

        THttpResponse copy;
        copy = resp;
        UNIT_ASSERT_STRINGS_EQUAL(ToString(copy), ToString(resp));
    }

    Y_UNIT_TEST(TestEmptyContent) {
        UNIT_ASSERT_STRINGS_EQUAL(ToString(THttpResponse().SetContent("")), "HTTP/1.1 200 Ok\r\n\r\n");
    }

    Y_UNIT_TEST(TestReturnReference) {
        THttpResponse resp;
        UNIT_ASSERT_EQUAL(&resp, &resp.AddHeader("Header1", 1));
        UNIT_ASSERT_EQUAL(&resp, &resp.AddHeader(THttpInputHeader("Header2", "2")));

        THttpHeaders headers;
        headers.AddHeader(THttpInputHeader("Header3", "3"));
        headers.AddHeader(THttpInputHeader("Header4", "4"));
        UNIT_ASSERT_EQUAL(&resp, &resp.AddMultipleHeaders(headers));

        UNIT_ASSERT_EQUAL(&resp, &resp.SetContent("some stuff"));
        UNIT_ASSERT_EQUAL(&resp, &resp.SetContent("some other stuff").SetContentType("text/plain"));
    }

    Y_UNIT_TEST(TestSetContentType) {
        const char* EXPECTED = "HTTP/1.1 200 Ok\r\n"
                               "Content-Type: text/xml\r\n"
                               "Content-Length: 28\r\n"
                               "\r\n"
                               "<xml><tag value=\"1\" /></xml>";
        THttpResponse resp;
        resp.SetContent("<xml><tag value=\"1\" /></xml>")
            .SetContentType("application/json")
            .SetContentType("text/xml");
        UNIT_ASSERT_STRINGS_EQUAL(ToString(resp), EXPECTED);
    }

    Y_UNIT_TEST(TestAddOrReplaceHeader) {
        THttpResponse resp(HTTP_FORBIDDEN);
        resp.AddOrReplaceHeader(THttpInputHeader("X-Header-1", "ValueOne"));
        resp.AddOrReplaceHeader("X-Header-2", 10);
        resp.AddOrReplaceHeader("X-Header-3", true);

        const char* EXPECTED_ADDED = "HTTP/1.1 403 Forbidden\r\n"
                               "X-Header-1: ValueOne\r\n"
                               "X-Header-2: 10\r\n"
                               "X-Header-3: 1\r\n"
                               "\r\n";
        UNIT_ASSERT_STRINGS_EQUAL(ToString(resp), EXPECTED_ADDED);

        resp.AddOrReplaceHeader(THttpInputHeader("X-Header-1", "ValueTwo"));
        resp.AddOrReplaceHeader("X-Header-2", 20);
        resp.AddOrReplaceHeader("X-Header-3", false);

        const char* EXPECTED_REPLACED = "HTTP/1.1 403 Forbidden\r\n"
                               "X-Header-1: ValueTwo\r\n"
                               "X-Header-2: 20\r\n"
                               "X-Header-3: 0\r\n"
                               "\r\n";
        UNIT_ASSERT_STRINGS_EQUAL(ToString(resp), EXPECTED_REPLACED);
    }
}
