#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/http/misc/httpcodes.h>

using namespace NKikimr::NHttpProxy;
using namespace NKikimr::Tests;
using namespace NActors;

#include "datastreams_fixture.h"

Y_UNIT_TEST_SUITE(TestMalformedRequest) {

    void TestContentLengthDiff(THttpProxyTestMock& mock, const std::optional<int> contentLengthDiff, TStringBuf compressed) {
        constexpr TStringBuf headers[]{
            "POST /Root HTTP/1.1",
            "Host:example.amazonaws.com",
            "X-Amz-Target:AmazonSQS.CreateQueue",
            "X-Amz-Date:20150830T123600Z",
            "Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/ru-central1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b)__",
            "Content-Type:application/json",
        };

        constexpr TStringBuf jsonStr = R"-({"QueueName": "Example"})-";
        constexpr TStringBuf deflateJsonStr = "\x78\x01\xab\x56\x0a\x2c\x4d\x2d\x4d\xf5\x4b\xcc\x4d\x55\xb2\x52\x50\x72\xad\x48\xcc\x2d\xc8\x49\x55\xaa\x05\x00\x66\x69\x08\x2d"sv;
        constexpr TStringBuf gzipJsonStr = "\x1f\x8b\x08\x00\xd8\x6e\xae\x68\x02\xff\xab\x56\x0a\x2c\x4d\x2d\x4d\xf5\x4b\xcc\x4d\x55\xb2\x52\x50\x72\xad\x48\xcc\x2d\xc8\x49\x55\xaa\x05\x00\x32\xf8\x6a\x11\x18\x00\x00\x00"sv;
        static_assert(jsonStr.size() != deflateJsonStr.size());
        static_assert(jsonStr.size() != gzipJsonStr.size());
        TStringBuf payload = jsonStr;
        if (compressed == "deflate") {
            payload = deflateJsonStr;
        } else if (compressed == "gzip") {
            payload = gzipJsonStr;
        } else {
            UNIT_ASSERT(compressed.empty());
        }
        std::optional<ssize_t> contentLength = std::nullopt;
        if (contentLengthDiff.has_value()) {
            ssize_t sz = payload.size();
            sz += contentLengthDiff.value();
            sz = Max<ssize_t>(1, sz);
            contentLength = sz;
        }

        TString request;
        {
            TStringOutput ss{request};
            for (const auto h : headers) {
                ss << h << "\r\n";
            }
            if (compressed) {
                ss << "Content-Encoding: " << compressed << "\r\n";
            }
            if (contentLength) {
                ss << "Content-Length: " << *contentLength << "\r\n";
            }
            ss << "\r\n";
            ss << payload;
            ss.Finish();
        }

        auto post = [&request, &mock]() -> THttpResult {
            TNetworkAddress addr("::", mock.HttpServicePort);
            TSocket sock(addr);
            {
                TSocketOutput so(sock);
                so.Write(request);
                so.Finish();
           }

            TSocketInput si(sock);
            THttpInput input(&si);

            bool gotRequestId{false};
            for (auto& header : input.Headers()) {
                gotRequestId |= header.Name() == "x-amzn-requestid";
            }
            Y_ABORT_UNLESS(gotRequestId);
            ui32 httpCode = ParseHttpRetCode(input.FirstLine());
            TString description(StripString(TStringBuf(input.FirstLine()).After(' ').After(' ')));
            TString responseBody = input.ReadAll();
            Cerr << "Http output full " << responseBody << Endl;
            return {httpCode, description, responseBody};
        };

        const bool expectCorrect = contentLengthDiff == 0;  // Content-Length is mandatory

        TMaybe<THttpResult> res;
        try {
            res = post();
        } catch (const THttpReadException&) {
            if (expectCorrect) {
                throw;
            }
        }

        if (expectCorrect) {
            UNIT_ASSERT(res.Defined());
            UNIT_ASSERT_VALUES_EQUAL_C(res->HttpCode, 200, LabeledOutput(res->HttpCode, res->Description, res->Body));
            NJson::TJsonMap json;
            UNIT_ASSERT(NJson::ReadJsonTree(res->Body, &json, true));
        } else {
            if (res) {
                // Option A: Server returns error code
                UNIT_ASSERT_C(IsUserError(res->HttpCode), LabeledOutput(res->HttpCode, res->Description, res->Body));
            } else {
                // Option B: Server may ignore malformed request and close connection
            }
        }
    }

    Y_UNIT_TEST_F(ContentLengthNone, THttpProxyTestMock) {
        TestContentLengthDiff(*this, std::nullopt, "");
    }

    Y_UNIT_TEST_F(ContentLengthCorrect, THttpProxyTestMock) {
        TestContentLengthDiff(*this, 0, "");
    }

    Y_UNIT_TEST_F(ContentLengthLower, THttpProxyTestMock) {
        TestContentLengthDiff(*this, -3, "");
    }

    Y_UNIT_TEST_F(ContentLengthHigher, THttpProxyTestMock) {
        TestContentLengthDiff(*this, +3000, "");
    }

    Y_UNIT_TEST_F(CompressedDeflateContentLengthNone, THttpProxyTestMock) {
        TestContentLengthDiff(*this, std::nullopt, "deflate");
    }

    Y_UNIT_TEST_F(CompressedDeflateContentLengthCorrect, THttpProxyTestMock) {
        UNIT_ASSERT_TEST_FAILS_C(TestContentLengthDiff(*this, 0, "deflate"), "XFail: deflate decode is broken: POST body decompressed twice");
    }

    Y_UNIT_TEST_F(CompressedDeflateContentLengthLower, THttpProxyTestMock) {
        if ("the deflate fails if the sream is terminated prematurely") {
            return;
        }
        TestContentLengthDiff(*this, -3000, "deflate");
    }

    Y_UNIT_TEST_F(CompressedDeflateContentLengthHigher, THttpProxyTestMock) {
        TestContentLengthDiff(*this, +3000, "deflate");
    }

    Y_UNIT_TEST_F(CompressedGzipContentLengthNone, THttpProxyTestMock) {
        TestContentLengthDiff(*this, std::nullopt, "gzip");
    }

    Y_UNIT_TEST_F(CompressedGzipContentLengthCorrect, THttpProxyTestMock) {
        UNIT_ASSERT_TEST_FAILS_C(TestContentLengthDiff(*this, 0, "gzip"), "X-Fail: gzip body doesn't decoded befor json parsing");
    }

    Y_UNIT_TEST_F(CompressedGzipContentLengthLower, THttpProxyTestMock) {
        TestContentLengthDiff(*this, -3000, "gzip");
    }

    Y_UNIT_TEST_F(CompressedGzipContentLengthHigher, THttpProxyTestMock) {
        TestContentLengthDiff(*this, +3000, "gzip");
    }

} // Y_UNIT_TEST_SUITE(TestMalformedRequest)
