#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <util/system/tempfile.h>
#include "http.h"
#include "http_proxy.h"



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
        EatWholeString(request, "GET /test HTTP/1.1\r\nHost: test\r\nSome-Header: 32344\r\n\r\n");
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
        EatWholeString(request, "GET /test HTTP/1.1\r\nHost: test\r\nHeader-Without-A-Colon\r\n\r\n");
        UNIT_ASSERT_C(request->IsError(), static_cast<int>(request->Stage));
        UNIT_ASSERT_EQUAL_C(request->GetErrorText(), "Invalid http header", static_cast<int>(request->LastSuccessStage));
    }

    Y_UNIT_TEST(HeaderParsingError_Response) {
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(nullptr);
        EatWholeString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nHeader-Without-A-Colon\r\n\r\n");
        UNIT_ASSERT_C(response->IsError(), static_cast<int>(response->Stage));
        UNIT_ASSERT_EQUAL_C(response->GetErrorText(), "Invalid http header", static_cast<int>(response->LastSuccessStage));
    }

    Y_UNIT_TEST(GetWithSpecifiedContentType) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, "GET /test HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nSome-Header: 32344\r\n\r\n");
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
        EatWholeString(request, "POST /Url HTTP/1.1\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_EQUAL(request->Method, "POST");
        UNIT_ASSERT_EQUAL(request->URL, "/Url");
        UNIT_ASSERT_EQUAL(request->Connection, "close");
        UNIT_ASSERT_EQUAL(request->Protocol, "HTTP");
        UNIT_ASSERT_EQUAL(request->Version, "1.1");
        UNIT_ASSERT_EQUAL(request->TransferEncoding, "chunked");
        UNIT_ASSERT_EQUAL(request->Body, "this is test.");
    }

    Y_UNIT_TEST(BasicPost) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, "POST /Url HTTP/1.1\r\nConnection: close\r\nContent-Length: 13\r\n\r\nthis is test.");
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
        EatWholeString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
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
        EatWholeString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        UNIT_ASSERT(response->IsError());
    }

    Y_UNIT_TEST(AdvancedParsingChunkedBody) {
        NHttp::THttpOutgoingRequestPtr request = nullptr; //new NHttp::THttpOutgoingRequest();
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
        EatWholeString(response, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nthis\r\n\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
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
        EatWholeString(request, "GET /Url HTTP/1.1\r\nConnection: close\r\nAccept-Encoding: gzip, deflate\r\n\r\n");
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
        NActors::TTestActorRuntimeBase actorSystem;
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
        NActors::TTestActorRuntimeBase actorSystem;
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
        NActors::TTestActorRuntimeBase actorSystem;
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

    Y_UNIT_TEST(TooLongHeader) {
        NActors::TTestActorRuntimeBase actorSystem;
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
        NActors::TTestActorRuntimeBase actorSystem;
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
}
