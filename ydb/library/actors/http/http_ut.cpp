#include "http.h"
#include "http_proxy.h"

#include <ydb/core/security/certificate_check/cert_auth_utils.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/http/ut/tls_client_connection.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/resource/resource.h>
#include <util/system/tempfile.h>
#include <thread>

enum EService : NActors::NLog::EComponent {
    MIN,
    Logger,
    MVP,
    MAX
};

namespace {

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

template <typename HttpType>
void EatPartialString(TIntrusivePtr<HttpType>& request, const TString& data) {
    for (char c : data) {
        request->EnsureEnoughSpaceAvailable(1);
        memcpy(request->Pos(), &c, 1);
        request->Advance(1);
    }
}

}

Y_UNIT_TEST_SUITE(HttpProxy) {
    Y_UNIT_TEST(BasicParsing) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "GET /test HTTP/1.1\r\nHost: test\r\nSome-Header: 32344\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "GET");
        UNIT_ASSERT_EQUAL(request->URL, "/test");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->Host, "test");
        UNIT_ASSERT_EQUAL(request->Headers, "Host: test\r\nSome-Header: 32344\r\n\r\n");
    }

    Y_UNIT_TEST(HeaderParsingError_Request) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "GET /test HTTP/1.1\r\nHost: test\r\nHeader-Without-A-Colon\r\n\r\n");
        UNIT_ASSERT_C(request->IsError(), static_cast<int>(request->Stage));
        UNIT_ASSERT_EQUAL_C(request->GetErrorText(), "Invalid http header", static_cast<int>(request->LastSuccessStage));
    }

    Y_UNIT_TEST(HeaderParsingError_Response) {
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(nullptr);
        EatPartialString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nHeader-Without-A-Colon\r\n\r\n");
        UNIT_ASSERT_C(response->IsError(), static_cast<int>(response->Stage));
        UNIT_ASSERT_EQUAL_C(response->GetErrorText(), "Invalid http header", static_cast<int>(response->LastSuccessStage));
    }

    Y_UNIT_TEST(GetWithSpecifiedContentType) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "GET /test HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nSome-Header: 32344\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "GET");
        UNIT_ASSERT_EQUAL(request->URL, "/test");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->Host, "test");
        UNIT_ASSERT_EQUAL(request->Headers, "Host: test\r\nContent-Type: application/json\r\nSome-Header: 32344\r\n\r\n");
    }

    Y_UNIT_TEST(BasicParsingChunkedBodyRequest) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "POST /Url HTTP/1.1\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "POST");
        UNIT_ASSERT_EQUAL(request->URL, "/Url");
        UNIT_ASSERT_EQUAL(request->Connection, "close");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->TransferEncoding, "chunked");
        UNIT_ASSERT_EQUAL(request->Body, "this is test.");
    }

    Y_UNIT_TEST(BasicParsingChunkedBigBodyRequest) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        TString bigChunk = TString(NHttp::THttpConfig::BUFFER_MIN_STEP, 'a');
        TString testBody;
        EatPartialString(request, "POST /Url HTTP/1.1\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n");
        for (size_t size = 0; size < NHttp::THttpConfig::BUFFER_SIZE; size += bigChunk.size()) {
            EatPartialString(request, (std::ostringstream() << std::hex << bigChunk.size() << "\r\n").str());
            EatPartialString(request, bigChunk);
            EatPartialString(request, "\r\n");
            testBody += bigChunk;
        }
        EatPartialString(request, "0\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "POST");
        UNIT_ASSERT_EQUAL(request->URL, "/Url");
        UNIT_ASSERT_EQUAL(request->Connection, "close");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->TransferEncoding, "chunked");
        UNIT_ASSERT_EQUAL(request->Body, testBody);
    }

    Y_UNIT_TEST(BasicParsingBigHeadersRequest) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "POST /Url HTTP/1.1\r\nConnection: close\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Header);
        TString bigChunk = TString(NHttp::THttpIncomingRequest::MaxHeaderSize * 2, 'a');
        EatPartialString(request, bigChunk);
        UNIT_ASSERT_VALUES_EQUAL(int(request->Stage), int(NHttp::THttpIncomingRequest::EParseStage::Error));
        UNIT_ASSERT_EQUAL(request->LastSuccessStage, NHttp::THttpIncomingRequest::EParseStage::Header);
    }

    Y_UNIT_TEST(BrokenParsingMethod) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "POSTPOSTPOSTPOSTPOSTPOSTPOST /Url HTTP/1.1\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n12345678901234567890\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Error);
        UNIT_ASSERT_EQUAL(request->LastSuccessStage, NHttp::THttpIncomingRequest::EParseStage::Method);
    }

    Y_UNIT_TEST(BrokenParsingChunkedBodyRequest) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "POST /Url HTTP/1.1\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n12345678901234567890\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Error);
        UNIT_ASSERT_EQUAL(request->LastSuccessStage, NHttp::THttpIncomingRequest::EParseStage::ChunkLength);
    }

    Y_UNIT_TEST(BasicPost) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "POST /Url HTTP/1.1\r\nConnection: close\r\nContent-Length: 13\r\n\r\nthis is test.");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "POST");
        UNIT_ASSERT_EQUAL(request->URL, "/Url");
        UNIT_ASSERT_EQUAL(request->Connection, "close");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->TransferEncoding, "");
        UNIT_ASSERT_EQUAL(request->Body, "this is test.");
    }

    Y_UNIT_TEST(BasicParsingChunkedBodyResponse) {
        NHttp::THttpOutgoingRequestPtr request = nullptr; //new NHttp::THttpOutgoingRequest();
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatPartialString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        UNIT_ASSERT_EQUAL(response->Stage, NHttp::THttpIncomingResponse::EParseStage::Done);
        UNIT_ASSERT_EQUAL(response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Connection, "close");
        UNIT_ASSERT_EQUAL(response->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(response->Version, "1.1");
        UNIT_ASSERT_EQUAL(response->TransferEncoding, "chunked");
        UNIT_ASSERT_EQUAL(response->Body, "this is test.");
    }

    Y_UNIT_TEST(InvalidParsingChunkedBody) {
        NHttp::THttpOutgoingRequestPtr request = nullptr; //new NHttp::THttpOutgoingRequest();
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatPartialString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        UNIT_ASSERT(response->IsError());
    }

    Y_UNIT_TEST(AdvancedParsingChunkedBody) {
        NHttp::THttpOutgoingRequestPtr request = nullptr; //new NHttp::THttpOutgoingRequest();
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatPartialString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nthis\r\n\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        UNIT_ASSERT_EQUAL(response->Stage, NHttp::THttpIncomingResponse::EParseStage::Done);
        UNIT_ASSERT_EQUAL(response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Connection, "close");
        UNIT_ASSERT_EQUAL(response->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(response->Version, "1.1");
        UNIT_ASSERT_EQUAL(response->TransferEncoding, "chunked");
        UNIT_ASSERT_EQUAL(response->Body, "this\r\n is test.");
    }

    Y_UNIT_TEST(CreateCompressedResponse) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "GET /Url HTTP/1.1\r\nConnection: close\r\nAccept-Encoding: gzip, deflate\r\n\r\n");
        NHttp::THttpOutgoingResponsePtr response = new NHttp::THttpOutgoingResponse(request, "HTTP", "1.1", "200", "OK");
        TString compressedBody = "something very long to compress with deflate algorithm. something very long to compress with deflate algorithm.";
        response->EnableCompression();
        size_t size1 = response->Size();
        response->SetBody(compressedBody);
        size_t size2 = response->Size();
        size_t compressedBodySize = size2 - size1;
        UNIT_ASSERT_VALUES_EQUAL("deflate", response->ContentEncoding);
        UNIT_ASSERT(compressedBodySize < compressedBody.size());
        NHttp::THttpOutgoingResponsePtr response2 = response->Duplicate(request);
        UNIT_ASSERT_VALUES_EQUAL(response->Body, response2->Body);
        UNIT_ASSERT_VALUES_EQUAL(response->ContentLength, response2->ContentLength);
        UNIT_ASSERT_VALUES_EQUAL(response->Size(), response2->Size());
    }

    Y_UNIT_TEST(BasicPartialParsing) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "GET /test HTTP/1.1\r\nHost: test\r\nSome-Header: 32344\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "GET");
        UNIT_ASSERT_EQUAL(request->URL, "/test");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->Host, "test");
        UNIT_ASSERT_EQUAL(request->Headers, "Host: test\r\nSome-Header: 32344\r\n\r\n");
    }

    Y_UNIT_TEST(BasicPartialParsingChunkedBody) {
        NHttp::THttpOutgoingRequestPtr request = nullptr; //new NHttp::THttpOutgoingRequest();
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatPartialString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        UNIT_ASSERT_EQUAL(response->Stage, NHttp::THttpIncomingResponse::EParseStage::Done);
        UNIT_ASSERT_EQUAL(response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Connection, "close");
        UNIT_ASSERT_EQUAL(response->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(response->Version, "1.1");
        UNIT_ASSERT_EQUAL(response->TransferEncoding, "chunked");
        UNIT_ASSERT_EQUAL(response->Body, "this is test.");
    }

    Y_UNIT_TEST(BasicParsingContentLength0) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "GET /test HTTP/1.1\r\nHost: test\r\nContent-Length: 0\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "GET");
        UNIT_ASSERT_EQUAL(request->URL, "/test");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->Host, "test");
        UNIT_ASSERT_EQUAL(request->Headers, "Host: test\r\nContent-Length: 0\r\n\r\n");
    }

    Y_UNIT_TEST(AdvancedParsing) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, "GE");
        EatWholeString(request, "T");
        EatWholeString(request, " ");
        EatWholeString(request, "/test");
        EatWholeString(request, " HTTP/1.1\r");
        EatWholeString(request, "\nHo");
        EatWholeString(request, "st: test");
        EatWholeString(request, "\r\n");
        EatWholeString(request, "Some-Header: 32344\r\n\r");
        EatWholeString(request, "\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "GET");
        UNIT_ASSERT_EQUAL(request->URL, "/test");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->Host, "test");
        UNIT_ASSERT_EQUAL(request->Headers, "Host: test\r\nSome-Header: 32344\r\n\r\n");
    }

    Y_UNIT_TEST(AdvancedPartialParsing) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatPartialString(request, "GE");
        EatPartialString(request, "T");
        EatPartialString(request, " ");
        EatPartialString(request, "/test");
        EatPartialString(request, " HTTP/1.1\r");
        EatPartialString(request, "\nHo");
        EatPartialString(request, "st: test");
        EatPartialString(request, "\r\n");
        EatPartialString(request, "Some-Header: 32344\r\n\r");
        EatPartialString(request, "\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "GET");
        UNIT_ASSERT_EQUAL(request->URL, "/test");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->Host, "test");
        UNIT_ASSERT_EQUAL(request->Headers, "Host: test\r\nSome-Header: 32344\r\n\r\n");
    }

    Y_UNIT_TEST(BasicRenderBodyWithHeadersAndCookies) {
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet("http://www.yandex.ru/data/url");
        NHttp::THeadersBuilder headers;
        NHttp::TCookiesBuilder cookies;
        cookies.Set("cookie1", "123456");
        cookies.Set("cookie2", "45678");
        headers.Set("Cookie", cookies.Render());
        request->Set(headers);
        TString requestData = request->AsString();
        UNIT_ASSERT_VALUES_EQUAL(requestData, "GET /data/url HTTP/1.1\r\nHost: www.yandex.ru\r\nAccept: */*\r\nCookie: cookie1=123456; cookie2=45678;\r\n");
    }

    Y_UNIT_TEST(BasicRenderOutgoingResponse) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, "GET /test HTTP/1.1\r\nHost: test\r\nSome-Header: 32344\r\n\r\n");

        NHttp::THttpOutgoingResponsePtr httpResponseOk = request->CreateResponseOK("response ok");
        UNIT_ASSERT_EQUAL(httpResponseOk->Stage, NHttp::THttpOutgoingResponse::ERenderStage::Done);
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseOk->Status, "200");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseOk->Message, "OK");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseOk->ContentType, "text/html");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseOk->Body, "response ok");

        NHttp::THttpOutgoingResponsePtr httpResponseBadRequest = request->CreateResponseBadRequest();
        UNIT_ASSERT_EQUAL(httpResponseBadRequest->Stage, NHttp::THttpOutgoingResponse::ERenderStage::Done);
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseBadRequest->Status, "400");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseBadRequest->Message, "Bad Request");
        UNIT_ASSERT(httpResponseBadRequest->ContentType.empty());
        UNIT_ASSERT(httpResponseBadRequest->Body.empty());

        NHttp::THttpOutgoingResponsePtr httpResponseNotFound = request->CreateResponseNotFound();
        UNIT_ASSERT_EQUAL(httpResponseNotFound->Stage, NHttp::THttpOutgoingResponse::ERenderStage::Done);
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseNotFound->Status, "404");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseNotFound->Message, "Not Found");
        UNIT_ASSERT(httpResponseNotFound->ContentType.empty());
        UNIT_ASSERT(httpResponseNotFound->Body.empty());

        NHttp::THttpOutgoingResponsePtr httpResponseServiceUnavailable = request->CreateResponseServiceUnavailable();
        UNIT_ASSERT_EQUAL(httpResponseServiceUnavailable->Stage, NHttp::THttpOutgoingResponse::ERenderStage::Done);
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseServiceUnavailable->Status, "503");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseServiceUnavailable->Message, "Service Unavailable");
        UNIT_ASSERT(httpResponseServiceUnavailable->ContentType.empty());
        UNIT_ASSERT(httpResponseServiceUnavailable->Body.empty());

        NHttp::THttpOutgoingResponsePtr httpResponseGatewayTimeout = request->CreateResponseGatewayTimeout("gateway timeout body");
        UNIT_ASSERT_EQUAL(httpResponseGatewayTimeout->Stage, NHttp::THttpOutgoingResponse::ERenderStage::Done);
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseGatewayTimeout->Status, "504");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseGatewayTimeout->Message, "Gateway Timeout");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseGatewayTimeout->ContentType, "text/html");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseGatewayTimeout->Body, "gateway timeout body");

        NHttp::THttpOutgoingResponsePtr httpIncompleteResponse = request->CreateIncompleteResponse("401", "Unauthorized");
        UNIT_ASSERT_EQUAL(httpIncompleteResponse->Stage, NHttp::THttpOutgoingResponse::ERenderStage::Header);
        UNIT_ASSERT_STRINGS_EQUAL(httpIncompleteResponse->Status, "401");
        UNIT_ASSERT_STRINGS_EQUAL(httpIncompleteResponse->Message, "Unauthorized");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->CreateResponse("401", "Unauthorized");
        UNIT_ASSERT_EQUAL(httpResponse->Stage, NHttp::THttpOutgoingResponse::ERenderStage::Done);
        UNIT_ASSERT_STRINGS_EQUAL(httpResponse->Status, "401");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponse->Message, "Unauthorized");

        NHttp::THeadersBuilder headers;
        NHttp::TCookiesBuilder cookies;
        cookies.Set("cookie1", "123456");
        headers.Set("Set-Cookie", cookies.Render());
        headers.Set("Location", "http://www.yandex.ru/data/url");

        NHttp::THttpOutgoingResponsePtr httpResponseRedirect = request->CreateResponse("302", "Found", headers);
        UNIT_ASSERT_EQUAL(httpResponseRedirect->Stage, NHttp::THttpOutgoingResponse::ERenderStage::Done);
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseRedirect->Status, "302");
        UNIT_ASSERT_STRINGS_EQUAL(httpResponseRedirect->Message, "Found");
        UNIT_ASSERT_STRING_CONTAINS(httpResponseRedirect->Headers, "Set-Cookie: cookie1=123456;");
        UNIT_ASSERT_STRING_CONTAINS(httpResponseRedirect->Headers, "Location: http://www.yandex.ru/data/url");
    }

    Y_UNIT_TEST(BasicRunning4) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
        //actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n6\r\npassed\r\n0\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Response->Body, "passed");
    }

    Y_UNIT_TEST(BasicRunning6) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
        //actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/test");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n6\r\npassed\r\n0\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Response->Body, "passed");
    }

    Y_UNIT_TEST(TlsRunning) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();

        TString certificateContent = R"___(-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCzRZjodO7Aqe1w
RyOj6kG6g2nn8ZGAxfao4mLT0jDTbVksrhV/h2s3uldLkFo5WrNQ8WZe+iIbXeFL
s8tO6hslzreo9sih2IHoRcH5KnS/6YTqVhRTJb1jE2dM8NwYbwTi+T2Pe0FrBPjI
kgVO50gAtYl9C+fc715uZiSKW+rRlP5OoFTwxrOjiU27RPZjFYyWK9wTI1Es9uRr
lbZbLl5cY6dK2J1AViRraaYKCWO26VbOPWLsY4OD3e+ZXIc3OMCz6Yb0wmRPeJ60
bbbkGfI8O27kDdv69MAWHIm0yYMzKEnom1dce7rNQNDEqJfocsYIsg+EvayT1yQ9
KTBegw7LAgMBAAECggEBAKaOCrotqYQmXArsjRhFFDwMy+BKdzyEr93INrlFl0dX
WHpCYobRcbOc1G3H94tB0UdqgAnNqtJyLlb+++ydZAuEOu4oGc8EL+10ofq0jzOd
6Xct8kQt0/6wkFDTlii9PHUDy0X65ZRgUiNGRtg/2I2QG+SpowmI+trm2xwQueFs
VaWrjc3cVvXx0b8Lu7hqZUv08kgC38stzuRk/n2T5VWSAr7Z4ZWQbO918Dv35HUw
Wy/0jNUFP9CBCvFJ4l0OoH9nYhWFG+HXWzNdw6/Hca4jciRKo6esCiOZ9uWYv/ec
/NvX9rgFg8G8/SrTisX10+Bbeq+R1RKwq/IG409TH4ECgYEA14L+3QsgNIUMeYAx
jSCyk22R/tOHI1BM+GtKPUhnwHlAssrcPcxXMJovl6WL93VauYjym0wpCz9urSpA
I2CqTsG8GYciA6Dr3mHgD6cK0jj9UPAU6EnZ5S0mjhPqKZqutu9QegzD2uESvuN8
36xezwQthzAf0nI/P3sJGjVXjikCgYEA1POm5xcV6SmM6HnIdadEebhzZIJ9TXQz
ry3Jj3a7CKyD5C7fAdkHUTCjgT/2ElxPi9ABkZnC+d/cW9GtJFa0II5qO/agm3KQ
ZXYiutu9A7xACHYFXRiJEjVUdGG9dKMVOHUEa8IHEgrrcUVM/suy/GgutywIfaXs
y58IFP24K9MCgYEAk6zjz7wL+XEiNy+sxLQfKf7vB9sSwxQHakK6wHuY/L8Zomp3
uLEJHfjJm/SIkK0N2g0JkXkCtv5kbKyC/rsCeK0wo52BpVLjzaLr0k34kE0U6B1b
dkEE2pGx1bG3x4KDLj+Wuct9ecK5Aa0IqIyI+vo16GkFpUM8K9e3SQo8UOECgYEA
sCZYAkILYtJ293p9giz5rIISGasDAUXE1vxWBXEeJ3+kneTTnZCrx9Im/ewtnWR0
fF90XL9HFDDD88POqAd8eo2zfKR2l/89SGBfPBg2EtfuU9FkgGyiPciVcqvC7q9U
B15saMKX3KnhtdGwbfeLt9RqCCTJZT4SUSDcq5hwdvcCgYAxY4Be8mNipj8Cgg22
mVWSolA0TEzbtUcNk6iGodpi+Z0LKpsPC0YRqPRyh1K+rIltG1BVdmUBHcMlOYxl
lWWvbJH6PkJWy4n2MF7PO45kjN3pPZg4hgH63JjZeAineBwEArUGb9zHnvzcdRvF
wuQ2pZHL/HJ0laUSieHDJ5917w==
-----END PRIVATE KEY-----


-----BEGIN CERTIFICATE-----
MIIDjTCCAnWgAwIBAgIURt5IBx0J3xgEaQvmyrFH2A+NkpMwDQYJKoZIhvcNAQEL
BQAwVjELMAkGA1UEBhMCUlUxDzANBgNVBAgMBk1vc2NvdzEPMA0GA1UEBwwGTW9z
Y293MQ8wDQYDVQQKDAZZYW5kZXgxFDASBgNVBAMMC3Rlc3Qtc2VydmVyMB4XDTE5
MDkyMDE3MTQ0MVoXDTQ3MDIwNDE3MTQ0MVowVjELMAkGA1UEBhMCUlUxDzANBgNV
BAgMBk1vc2NvdzEPMA0GA1UEBwwGTW9zY293MQ8wDQYDVQQKDAZZYW5kZXgxFDAS
BgNVBAMMC3Rlc3Qtc2VydmVyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKC
AQEAs0WY6HTuwKntcEcjo+pBuoNp5/GRgMX2qOJi09Iw021ZLK4Vf4drN7pXS5Ba
OVqzUPFmXvoiG13hS7PLTuobJc63qPbIodiB6EXB+Sp0v+mE6lYUUyW9YxNnTPDc
GG8E4vk9j3tBawT4yJIFTudIALWJfQvn3O9ebmYkilvq0ZT+TqBU8Mazo4lNu0T2
YxWMlivcEyNRLPbka5W2Wy5eXGOnStidQFYka2mmCgljtulWzj1i7GODg93vmVyH
NzjAs+mG9MJkT3ietG225BnyPDtu5A3b+vTAFhyJtMmDMyhJ6JtXXHu6zUDQxKiX
6HLGCLIPhL2sk9ckPSkwXoMOywIDAQABo1MwUTAdBgNVHQ4EFgQUDv/xuJ4CvCgG
fPrZP3hRAt2+/LwwHwYDVR0jBBgwFoAUDv/xuJ4CvCgGfPrZP3hRAt2+/LwwDwYD
VR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAinKpMYaA2tjLpAnPVbjy
/ZxSBhhB26RiQp3Re8XOKyhTWqgYE6kldYT0aXgK9x9mPC5obQannDDYxDc7lX+/
qP/u1X81ZcDRo/f+qQ3iHfT6Ftt/4O3qLnt45MFM6Q7WabRm82x3KjZTqpF3QUdy
tumWiuAP5DMd1IRDtnKjFHO721OsEsf6NLcqdX89bGeqXDvrkwg3/PNwTyW5E7cj
feY8L2eWtg6AJUnIBu11wvfzkLiH3QKzHvO/SIZTGf5ihDsJ3aKEE9UNauTL3bVc
CRA/5XcX13GJwHHj6LCoc3sL7mt8qV9HKY2AOZ88mpObzISZxgPpdKCfjsrdm63V
6g==
-----END CERTIFICATE-----)___";

        TTempFileHandle certificateFile;

        certificateFile.Write(certificateContent.data(), certificateContent.size());

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);

        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(port);
        ///////// https configuration
        add->Secure = true;
        add->CertificateFile = certificateFile.Name();
        add->PrivateKeyFile = certificateFile.Name();
        /////////
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), add.Release()), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("https://[::1]:" + ToString(port) + "/test");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n6\r\npassed\r\n0\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Response->Body, "passed");
    }

    /*Y_UNIT_TEST(AdvancedRunning) {
        THolder<NActors::TActorSystemSetup> setup = MakeHolder<NActors::TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 1;
        setup->Executors = new TAutoPtr<NActors::IExecutorPool>[1];
        setup->Executors[0] = new NActors::TBasicExecutorPool(0, 2, 10);
        setup->Scheduler = new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig(512, 100));
        NActors::TActorSystem actorSystem(setup);
        actorSystem.Start();
        NHttp::THttpProxy* incomingProxy = new NHttp::THttpProxy();
        NActors::TActorId incomingProxyId = actorSystem.Register(incomingProxy);
        actorSystem.Send(incomingProxyId, new NHttp::TEvHttpProxy::TEvAddListeningPort(13337));

        NHttp::THttpProxy* outgoingProxy = new NHttp::THttpProxy();
        NActors::TActorId outgoingProxyId = actorSystem.Register(outgoingProxy);

        THolder<NHttp::THttpStaticStringRequest> httpRequest = MakeHolder<NHttp::THttpStaticStringRequest>("GET /test HTTP/1.1\r\n\r\n");
        actorSystem.Send(outgoingProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest("[::]:13337", std::move(httpRequest)));

        Sleep(TDuration::Minutes(60));
    }*/

    Y_UNIT_TEST(TooLongURL) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_TRACE);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        TString longUrl;
        longUrl.append(9000, 'X');
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/" + longUrl);
        httpRequest->Set("Connection", "close");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "400");
        UNIT_ASSERT_EQUAL(response->Response->Body, "Invalid url");
    }

    Y_UNIT_TEST(TooLongHeader) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_TRACE);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/test");
        httpRequest->Set("Connection", "close");
        TString longHeader;
        longHeader.append(9000, 'X');
        httpRequest->Set(longHeader, "data");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "400");
        UNIT_ASSERT_EQUAL(response->Response->Body, "Invalid http header");
    }

    Y_UNIT_TEST(HeaderWithoutAColon) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        actorSystem.SetUseRealInterconnect();
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/test");
        httpRequest->Append("Header-Without-A-Colon\r\n");
        httpRequest->FinishHeader();
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "400");
        UNIT_ASSERT_EQUAL(response->Response->Body, "Invalid http header");
    }

    void SimulateSleep(NActors::TTestActorRuntimeBase& actorSystem, TDuration duration) {
        auto sleepEdgeActor = actorSystem.AllocateEdgeActor();
        actorSystem.Schedule(new NActors::IEventHandle(sleepEdgeActor, sleepEdgeActor, new NActors::TEvents::TEvWakeup()), duration);
        actorSystem.GrabEdgeEventRethrow<NActors::TEvents::TEvWakeup>(sleepEdgeActor);
    }

    Y_UNIT_TEST(TooManyRequests) {
        static constexpr int MaxRequestsPerSecond = 5;
        NActors::TTestActorRuntimeBase actorSystem;
        actorSystem.SetUseRealInterconnect();
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        auto* addPortEvent = new NHttp::TEvHttpProxy::TEvAddListeningPort(port);
        addPortEvent->MaxRequestsPerSecond = MaxRequestsPerSecond;
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), addPortEvent), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        for (int i = 0; i < MaxRequestsPerSecond + 1; ++i) {
            actorSystem.Send(new NActors::IEventHandle(proxyId, clientId,
                new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/test"))), 0, true);
        }

        for (int i = 0; i < MaxRequestsPerSecond ; ++i) {
            // ok
            NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
            UNIT_ASSERT_EQUAL(request->Request->URL, "/test");
        }

        {
            // error
            NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
            UNIT_ASSERT_EQUAL(response->Response->Status, "429");
            UNIT_ASSERT_EQUAL(response->Response->Message, "Too Many Requests");
        }

        SimulateSleep(actorSystem, TDuration::Seconds(1));

        for (int i = 0; i < MaxRequestsPerSecond + 1; ++i) {
            actorSystem.Send(new NActors::IEventHandle(proxyId, clientId,
                new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(port) + "/test"))), 0, true);
        }

        for (int i = 0; i < MaxRequestsPerSecond ; ++i) {
            // ok
            NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
            UNIT_ASSERT_EQUAL(request->Request->URL, "/test");
        }

        {
            // error
            NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
            UNIT_ASSERT_EQUAL(response->Response->Status, "429");
            UNIT_ASSERT_EQUAL(response->Response->Message, "Too Many Requests");
        }
    }

    Y_UNIT_TEST(ChunkedResponse1) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk1 = httpResponse->CreateDataChunk("pas");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk1)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk2 = httpResponse->CreateDataChunk("sed");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk2)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk3 = httpResponse->CreateDataChunk(); // end of data
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk3)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Error, "");
        UNIT_ASSERT_EQUAL(response->Response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Response->Body, "passed");
    }

    Y_UNIT_TEST(ChunkedResponse2) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk1 = httpResponse->CreateDataChunk("pas");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk1)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk2 = httpResponse->CreateDataChunk("sed");
        httpDataChunk2->SetEndOfData(); // end of data
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk2)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Error, "");
        UNIT_ASSERT_EQUAL(response->Response->Status, "200");
        UNIT_ASSERT_EQUAL(response->Response->Body, "passed");
    }

    Y_UNIT_TEST(ChunkedResponse3) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#else
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_CRIT);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk1 = httpResponse->CreateDataChunk("pas");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk1)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk2 = httpResponse->CreateDataChunk("sed");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk2)), 0, true);

        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk("error")), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT(response->Response->IsError());
        UNIT_ASSERT_EQUAL(response->Error, "ConnectionClosed");
    }

    Y_UNIT_TEST(StreamingResponse1) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->StreamContentTypes = {"text/plain"};
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk1 = httpResponse->CreateDataChunk("pas");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk1)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk2 = httpResponse->CreateDataChunk("sed");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk2)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk3 = httpResponse->CreateDataChunk(); // end of data
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk3)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT(!dataChunk1->Error);
        UNIT_ASSERT_EQUAL(dataChunk1->Data, "pas");
        UNIT_ASSERT_EQUAL(dataChunk1->IsEndOfData(), false);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT(!dataChunk2->Error);
        UNIT_ASSERT_EQUAL(dataChunk2->Data, "sed");
        UNIT_ASSERT_EQUAL(dataChunk2->IsEndOfData(), false);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk3 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT(!dataChunk3->Error);
        UNIT_ASSERT(!dataChunk3->Data);
        UNIT_ASSERT(dataChunk3->IsEndOfData());
    }

    Y_UNIT_TEST(StreamingResponse2) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->StreamContentTypes = {"text/plain"};
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk1 = httpResponse->CreateDataChunk("pas");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk1)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk2 = httpResponse->CreateDataChunk("sed");
        httpDataChunk2->SetEndOfData(); // end of data
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk2)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT(!dataChunk1->Error);
        UNIT_ASSERT_EQUAL(dataChunk1->Data, "pas");
        UNIT_ASSERT_EQUAL(dataChunk1->IsEndOfData(), false);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT(!dataChunk2->Error);
        UNIT_ASSERT_EQUAL(dataChunk2->Data, "sed");
        UNIT_ASSERT_EQUAL(dataChunk2->IsEndOfData(), false);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk3 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT(!dataChunk3->Error);
        UNIT_ASSERT(!dataChunk3->Data);
        UNIT_ASSERT(dataChunk3->IsEndOfData());
    }

    Y_UNIT_TEST(StreamingResponse3) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#else
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_CRIT);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->StreamContentTypes = {"text/plain"};
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk1 = httpResponse->CreateDataChunk("pas");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk1)), 0, true);

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk2 = httpResponse->CreateDataChunk("sed");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk2)), 0, true);

        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk("error")), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT(!dataChunk1->Error);
        UNIT_ASSERT_EQUAL(dataChunk1->Data, "pas");
        UNIT_ASSERT_EQUAL(dataChunk1->IsEndOfData(), false);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT(!dataChunk2->Error);
        UNIT_ASSERT_EQUAL(dataChunk2->Data, "sed");
        UNIT_ASSERT_EQUAL(dataChunk2->IsEndOfData(), false);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk3 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);

        UNIT_ASSERT_VALUES_EQUAL(dataChunk3->Error, "ConnectionClosed");
        UNIT_ASSERT(!dataChunk3->Data);
    }

    Y_UNIT_TEST(StreamingFatResponse1) {
        constexpr size_t ChunkSize = 65536; // 64K
        constexpr int ChunkCount = 10; // 10 chunks
        constexpr size_t TotalSize = ChunkSize * ChunkCount; // 640K
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->StreamContentTypes = {"text/plain"};
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        TString originalBody;

        for (int i = 0; i < ChunkCount; ++i) {
            TString longChunk;
            longChunk.append(ChunkSize, 'X');
            originalBody += longChunk;
            NHttp::THttpOutgoingDataChunkPtr httpDataChunk = httpResponse->CreateDataChunk(longChunk);
            actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk)), 0, true);
        }

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk = httpResponse->CreateDataChunk(); // end of data
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");

        TString responseBody;
        for (int i = 0; i < ChunkCount; ++i) {
            NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);
            UNIT_ASSERT(!dataChunk->Error);
            UNIT_ASSERT_EQUAL(dataChunk->IsEndOfData(), false);
            responseBody.append(dataChunk->Data);
        }

        UNIT_ASSERT_VALUES_EQUAL(responseBody.size(), TotalSize);
        UNIT_ASSERT_EQUAL(responseBody, originalBody);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);
        UNIT_ASSERT(!dataChunk->Error);
        UNIT_ASSERT(!dataChunk->Data);
        UNIT_ASSERT(dataChunk->IsEndOfData());
    }

    Y_UNIT_TEST(StreamingCompressedFatResponse1) {
        constexpr size_t ChunkSize = 65536; // 64K
        constexpr int ChunkCount = 10; // 10 chunks
        constexpr size_t TotalSize = ChunkSize * ChunkCount; // 640K
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateHttpRequest("GET", "127.0.0.1:" + ToString(port), "/test");
        httpRequest->Set("Accept-Encoding", "gzip, deflate");
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->StreamContentTypes = {"text/plain"};
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nContent-Encoding: deflate\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        TString originalBody;

        for (int i = 0; i < ChunkCount; ++i) {
            TString longChunk;
            longChunk.append(ChunkSize, 'X');
            originalBody += longChunk;
            NHttp::THttpOutgoingDataChunkPtr httpDataChunk = httpResponse->CreateDataChunk(longChunk);
            actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk)), 0, true);
        }

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk = httpResponse->CreateDataChunk(); // end of data
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");

        TString responseBody;
        for (int i = 0; i < ChunkCount; ++i) {
            NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);
            UNIT_ASSERT(!dataChunk->Error);
            UNIT_ASSERT_EQUAL(dataChunk->IsEndOfData(), false);
            responseBody.append(dataChunk->Data);
        }

        UNIT_ASSERT_VALUES_EQUAL(responseBody.size(), TotalSize);
        UNIT_ASSERT_EQUAL(responseBody, originalBody);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);
        UNIT_ASSERT(!dataChunk->Error);
        UNIT_ASSERT(!dataChunk->Data);
        UNIT_ASSERT(dataChunk->IsEndOfData());
    }

    Y_UNIT_TEST(StreamingCompressedFatRandomResponse1) {
        constexpr size_t ChunkSize = 65536; // 64K
        constexpr int ChunkCount = 10; // 10 chunks
        constexpr size_t TotalSize = ChunkSize * ChunkCount; // 640K
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateHttpRequest("GET", "127.0.0.1:" + ToString(port), "/test");
        httpRequest->Set("Accept-Encoding", "gzip, deflate");
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->StreamContentTypes = {"text/plain"};
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nContent-Encoding: deflate\r\nTransfer-Encoding: chunked\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);

        TString originalBody;

        for (int i = 0; i < ChunkCount; ++i) {
            TString longChunk;
            for (size_t j = 0; j < ChunkSize; ++j) {
                longChunk.append(1, 'A' + (rand() % 26)); // random character from A-Z
            }
            originalBody += longChunk;
            NHttp::THttpOutgoingDataChunkPtr httpDataChunk = httpResponse->CreateDataChunk(longChunk);
            actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk)), 0, true);
        }

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk = httpResponse->CreateDataChunk(); // end of data
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse* response = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Response->Status, "200");

        TString responseBody;
        for (int i = 0; i < ChunkCount; ++i) {
            NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);
            UNIT_ASSERT(!dataChunk->Error);
            UNIT_ASSERT_EQUAL(dataChunk->IsEndOfData(), false);
            responseBody.append(dataChunk->Data);
        }

        UNIT_ASSERT_VALUES_EQUAL(responseBody.size(), TotalSize);
        UNIT_ASSERT_EQUAL(responseBody, originalBody);

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);
        UNIT_ASSERT(!dataChunk->Error);
        UNIT_ASSERT(!dataChunk->Data);
        UNIT_ASSERT(dataChunk->IsEndOfData());
    }

    Y_UNIT_TEST(StreamingResponseWithProgress1) {
        constexpr size_t ChunkSize = 400; // 400 bytes
        constexpr int ChunkCount = 100; // 100 chunks
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_DEBUG);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test");
        NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest);
        event->StreamContentTypes = {"text/plain"};
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, event), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");

        TString responseString = "HTTP/1.1 200 Found\r\nConnection: Close\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n";
        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString(responseString);
        auto response = new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse);
        response->ProgressNotificationBytes = ChunkSize; // notify for every byte
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, response), 0, true);

        NHttp::TEvHttpProxy::TEvHttpOutgoingResponseProgress* headersProgress = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponseProgress>(handle);
        UNIT_ASSERT_VALUES_EQUAL(headersProgress->Bytes, responseString.size());
        UNIT_ASSERT_VALUES_EQUAL(headersProgress->DataChunks, 0);

        ui64 totalBytes = responseString.size();

        for (int i = 0; i < ChunkCount; ++i) {
            TString longChunk(ChunkSize, 'X');
            NHttp::THttpOutgoingDataChunkPtr httpDataChunk = httpResponse->CreateDataChunk(longChunk);
            actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk)), 0, true);

            totalBytes += longChunk.size() + 7; // 7 bytes for chunk header and footer

            NHttp::TEvHttpProxy::TEvHttpOutgoingResponseProgress* chunkProgress = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponseProgress>(handle);
            UNIT_ASSERT_VALUES_EQUAL(chunkProgress->Bytes, totalBytes);
            UNIT_ASSERT_VALUES_EQUAL(chunkProgress->DataChunks, static_cast<ui64>(i + 1));
        }

        NHttp::THttpOutgoingDataChunkPtr httpDataChunk = httpResponse->CreateDataChunk(); // end of data
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(httpDataChunk)), 0, true);

        totalBytes += 5; // "0\r\n\r\n"

        NHttp::TEvHttpProxy::TEvHttpOutgoingResponseProgress* finalProgress = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponseProgress>(handle);
        UNIT_ASSERT_VALUES_EQUAL(finalProgress->Bytes, totalBytes);
        UNIT_ASSERT_VALUES_EQUAL(finalProgress->DataChunks, ChunkCount + 1);

        NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse* incompleteResponse = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncompleteIncomingResponse>(handle);

        UNIT_ASSERT_VALUES_EQUAL(incompleteResponse->Response->Status, "200");

        for (int i = 0; i < ChunkCount; ++i) {
            NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);
            UNIT_ASSERT(!dataChunk->Error);
            UNIT_ASSERT_VALUES_EQUAL(dataChunk->IsEndOfData(), false);
        }

        NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk* dataChunk = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingDataChunk>(handle);
        UNIT_ASSERT(!dataChunk->Error);
        UNIT_ASSERT(!dataChunk->Data);
        UNIT_ASSERT(dataChunk->IsEndOfData());
    }

    Y_UNIT_TEST(RequestAfter307) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
#ifndef NDEBUG
        actorSystem.SetLogPriority(NActorsServices::HTTP, NActors::NLog::PRI_TRACE);
#endif

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test1", serverId)), 0, true);
        actorSystem.Send(new NActors::IEventHandle(proxyId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test2", serverId)), 0, true);

        NActors::TActorId clientId = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest1 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/test1");
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest1, true)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request1->Request->URL, "/test1");

        NHttp::THttpOutgoingResponsePtr httpResponse1 = request1->Request->CreateResponseString("HTTP/1.1 307 Temporary Redirect\r\nLocation: /test2\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse1)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response1->Response->Status, "307");

        auto location = NHttp::THeaders(response1->Response->Headers)["Location"];
        NHttp::THttpOutgoingRequestPtr httpRequest2 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + location);
        actorSystem.Send(new NActors::IEventHandle(proxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest2, true)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);

        UNIT_ASSERT_EQUAL(request2->Request->URL, "/test2");

        NHttp::THttpOutgoingResponsePtr httpResponse2 = request2->Request->CreateResponseString("HTTP/1.1 200 Ok\r\nContent-Length: 0\r\n\r\n");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse2)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);

        UNIT_ASSERT_EQUAL(response2->Response->Status, "200");
    }
}

Y_UNIT_TEST_SUITE(THttpProxyWithMTls) {
    struct TMtlsTestSetup {
        TStringStream LogStream;
        TAutoPtr<TLogBackend> LogBackend;
        NKikimr::TCertAndKey CaCertAndKey;
        NKikimr::TCertAndKey ServerCertAndKey;
        NKikimr::TCertAndKey ClientCertAndKey;
        NKikimr::TCertAndKey UntrustedCaCertAndKey;
        NKikimr::TCertAndKey UntrustedClientCertAndKey;
        TTempFileHandle CaCertFile;
        TTempFileHandle ServerCertFile;
        TTempFileHandle ServerKeyFile;
        TTempFileHandle ClientCertFile;
        TTempFileHandle ClientKeyFile;
        TTempFileHandle UntrustedCaCertFile;
        TTempFileHandle UntrustedClientCertFile;
        TTempFileHandle UntrustedClientKeyFile;
        NActors::TTestActorRuntimeBase ActorSystem;
        TPortManager PortManager;
        TIpPort Port;
        NActors::TActorId ProxyId;
        NActors::TActorId ServerId;

        TMtlsTestSetup(const bool useRealThreads = false, const bool secureConnection = true)
            : LogBackend(new TStreamLogBackend(&LogStream))
            , ActorSystem(1, useRealThreads)
        {
            // Generate certificates
            CaCertAndKey = NKikimr::GenerateCA(NKikimr::TProps::AsCA());
            ServerCertAndKey = NKikimr::GenerateSignedCert(CaCertAndKey, NKikimr::TProps::AsServer());
            ClientCertAndKey = NKikimr::GenerateSignedCert(CaCertAndKey, NKikimr::TProps::AsClient());

            NKikimr::TProps untrustedCaProps = NKikimr::TProps::AsCA();
            untrustedCaProps.CommonName = "Untrusted " + untrustedCaProps.CommonName;
            UntrustedCaCertAndKey = NKikimr::GenerateCA(untrustedCaProps);

            NKikimr::TProps untrustedClientProps = NKikimr::TProps::AsClient();
            untrustedClientProps.CommonName = "Untrusted " + untrustedClientProps.CommonName;
            UntrustedClientCertAndKey = NKikimr::GenerateSignedCert(UntrustedCaCertAndKey, untrustedClientProps);

            // Write certificates to files
            CaCertFile.Write(CaCertAndKey.Certificate.c_str(), CaCertAndKey.Certificate.size());
            ServerCertFile.Write(ServerCertAndKey.Certificate.c_str(), ServerCertAndKey.Certificate.size());
            ServerKeyFile.Write(ServerCertAndKey.PrivateKey.c_str(), ServerCertAndKey.PrivateKey.size());
            ClientCertFile.Write(ClientCertAndKey.Certificate.c_str(), ClientCertAndKey.Certificate.size());
            ClientKeyFile.Write(ClientCertAndKey.PrivateKey.c_str(), ClientCertAndKey.PrivateKey.size());
            UntrustedCaCertFile.Write(UntrustedCaCertAndKey.Certificate.c_str(), UntrustedCaCertAndKey.Certificate.size());
            UntrustedClientCertFile.Write(UntrustedClientCertAndKey.Certificate.c_str(), UntrustedClientCertAndKey.Certificate.size());
            UntrustedClientKeyFile.Write(UntrustedClientCertAndKey.PrivateKey.c_str(), UntrustedClientCertAndKey.PrivateKey.size());

            ActorSystem.SetLogBackend(LogBackend);
            ActorSystem.Initialize();

            NActors::IActor* proxy = NHttp::CreateHttpProxy();
            ProxyId = ActorSystem.Register(proxy);

            Port = PortManager.GetTcpPort();
            THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> add = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(Port);
            if (secureConnection) {
                add->Secure = true;
                add->CertificateFile = ServerCertFile.Name();
                add->PrivateKeyFile = ServerKeyFile.Name();
                add->CaFile = CaCertFile.Name(); // enables mTLS
            }
            ActorSystem.Send(new NActors::IEventHandle(ProxyId, ActorSystem.AllocateEdgeActor(), add.Release()), 0, true);
            TAutoPtr<NActors::IEventHandle> handle;
            ActorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

            ServerId = ActorSystem.AllocateEdgeActor();
            ActorSystem.Send(new NActors::IEventHandle(ProxyId, ServerId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/test", ServerId)), 0, true);
        }
    };

    Y_UNIT_TEST(ValidClientCertificate) {
        TMtlsTestSetup setup;

        const TString httpRequest = "GET /test HTTP/1.1\r\nHost: 127.0.0.1:" + ToString(setup.Port) + "\r\nConnection: close\r\n\r\n";
        std::thread clientThread([&setup, httpRequest]() {
            // We run it in a separate thread because GrabEdgeEvent() blocks the main thread waiting for events.
            // Without a separate thread, we would have a deadlock: main thread blocked in GrabEdgeEvent,
            // client thread blocked waiting for response from server.
            NHttp::NTest::SendTlsRequest(setup.Port, setup.ClientCertFile.Name(), setup.ClientKeyFile.Name(), setup.CaCertFile.Name(), httpRequest);
        });

        TAutoPtr<NActors::IEventHandle> handle;
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request1 = setup.ActorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(request1->Request->URL, "/test");
        UNIT_ASSERT(!request1->Request->MTlsClientCertificate.empty());

        clientThread.join();
    }

    Y_UNIT_TEST(UntrustedClientCertificate) {
        // Need real threads, since we can't use GrabEdgeEvent – there's no events to detect errors
        TMtlsTestSetup setup(/* useRealThreads */ true);

        const TString httpRequest = "GET /test HTTP/1.1\r\nHost: 127.0.0.1:" + ToString(setup.Port) + "\r\nConnection: close\r\n\r\n";
        std::thread clientThread([&setup, httpRequest]() {
            // We run it in a separate thread because GrabEdgeEvent() blocks the main thread waiting for events.
            // Without a separate thread, we would have a deadlock: main thread blocked in GrabEdgeEvent,
            // client thread blocked waiting for response from server.
            NHttp::NTest::SendTlsRequest(setup.Port, setup.UntrustedClientCertFile.Name(), setup.UntrustedClientKeyFile.Name(), setup.CaCertFile.Name(), httpRequest);
        });

        const TDuration timeout = TDuration::Seconds(2);
        const TInstant deadline = TInstant::Now() + timeout;
        bool errorFound = false;
        while (TInstant::Now() < deadline) {
            if (setup.LogStream.Str().Contains("connection closed - error in Accept")) {
                errorFound = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(errorFound, "No connection error happened for untrusted client");

        clientThread.join();
    }

    Y_UNIT_TEST(NoClientCertificate) {
        TMtlsTestSetup setup;

        const TString httpRequest = "GET /test HTTP/1.1\r\nHost: 127.0.0.1:" + ToString(setup.Port) + "\r\nConnection: close\r\n\r\n";
        std::thread clientThread([&setup, httpRequest]() {
            // We run it in a separate thread because GrabEdgeEvent() blocks the main thread waiting for events.
            // Without a separate thread, we would have a deadlock: main thread blocked in GrabEdgeEvent,
            // client thread blocked waiting for response from server.
            NHttp::NTest::SendTlsRequest(setup.Port, "", "", setup.CaCertFile.Name(), httpRequest);
        });

        TAutoPtr<NActors::IEventHandle> handle;
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = setup.ActorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");
        UNIT_ASSERT(request->Request->MTlsClientCertificate.empty());

        clientThread.join();
    }

    Y_UNIT_TEST(NotSecureConnection) {
        TMtlsTestSetup setup(/* useRealThreads */ false, /* secureConnection */ false);

        NActors::TActorId clientId = setup.ActorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://[::1]:" + ToString(setup.Port) + "/test");
        setup.ActorSystem.Send(new NActors::IEventHandle(setup.ProxyId, clientId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest)), 0, true);

        TAutoPtr<NActors::IEventHandle> handle;
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* request = setup.ActorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(request->Request->URL, "/test");
        UNIT_ASSERT(request->Request->MTlsClientCertificate.empty());

        NHttp::THttpOutgoingResponsePtr httpResponse = request->Request->CreateResponseString("HTTP/1.1 200 OK\r\nConnection: Close\r\n\r\n");
        setup.ActorSystem.Send(new NActors::IEventHandle(handle->Sender, setup.ServerId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse)), 0, true);
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* response = setup.ActorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_EQUAL(response->Response->Status, "200");
    }

}