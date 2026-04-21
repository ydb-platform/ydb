#include "http.h"
#include "http_proxy.h"
#include "http_cache.h"

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace {

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

TString CompressGzip(TStringBuf data) {
    NHttp::TCompressContext ctx;
    ctx.InitCompress("gzip");
    return ctx.Compress(data, true);
}

TString CompressDeflate(TStringBuf data) {
    NHttp::TCompressContext ctx;
    ctx.InitCompress("deflate");
    return ctx.Compress(data, true);
}

}

Y_UNIT_TEST_SUITE(HttpCacheCompression) {

    // Test that THttpIncomingResponse::Duplicate strips Content-Encoding and fixes Content-Length
    // This is the core fix: when we cache a compressed response and then duplicate it,
    // the body is already decompressed during parsing, so we must strip Content-Encoding
    // and update Content-Length to match the decompressed body size.
    Y_UNIT_TEST(IncomingResponseDuplicateStripsContentEncoding) {
        TString body = "this is a test body that should be long enough to compress well with gzip algorithm";
        TString compressedBody = CompressGzip(body);

        NHttp::THttpOutgoingRequestPtr request1 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/test");
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request1);
        TString rawResponse = TStringBuilder()
            << "HTTP/1.1 200 OK\r\n"
            << "Content-Type: application/json\r\n"
            << "Content-Encoding: gzip\r\n"
            << "Content-Length: " << compressedBody.size() << "\r\n"
            << "\r\n"
            << compressedBody;
        EatWholeString(response, rawResponse);
        UNIT_ASSERT_EQUAL(response->Stage, NHttp::THttpIncomingResponse::EParseStage::Done);
        // Body should be decompressed after parsing
        UNIT_ASSERT_VALUES_EQUAL(response->Body, body);

        // Duplicate for a different request
        NHttp::THttpOutgoingRequestPtr request2 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/test");
        NHttp::THttpIncomingResponsePtr duplicated = response->Duplicate(request2);

        UNIT_ASSERT_VALUES_EQUAL(duplicated->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(duplicated->Body, body);
        // Content-Encoding must be stripped
        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->ContentEncoding), "");
        // Content-Length must match the decompressed body
        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->ContentLength), ToString(body.size()));
    }

    // Test that duplicating a response with deflate encoding also works
    Y_UNIT_TEST(IncomingResponseDuplicateStripsDeflateEncoding) {
        TString body = "test body for deflate compression that is long enough to compress";
        TString compressedBody = CompressDeflate(body);

        NHttp::THttpOutgoingRequestPtr request1 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/test");
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request1);
        TString rawResponse = TStringBuilder()
            << "HTTP/1.1 200 OK\r\n"
            << "Content-Encoding: deflate\r\n"
            << "Content-Length: " << compressedBody.size() << "\r\n"
            << "\r\n"
            << compressedBody;
        EatWholeString(response, rawResponse);
        UNIT_ASSERT_EQUAL(response->Stage, NHttp::THttpIncomingResponse::EParseStage::Done);
        UNIT_ASSERT_VALUES_EQUAL(response->Body, body);

        NHttp::THttpOutgoingRequestPtr request2 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/test");
        NHttp::THttpIncomingResponsePtr duplicated = response->Duplicate(request2);

        UNIT_ASSERT_VALUES_EQUAL(duplicated->Body, body);
        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->ContentEncoding), "");
        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->ContentLength), ToString(body.size()));
    }

    // Test that duplicating an uncompressed response works fine (no Content-Encoding change)
    Y_UNIT_TEST(IncomingResponseDuplicateUncompressed) {
        TString body = "plain uncompressed body";

        NHttp::THttpOutgoingRequestPtr request1 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/test");
        NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request1);
        TString rawResponse = TStringBuilder()
            << "HTTP/1.1 200 OK\r\n"
            << "Content-Length: " << body.size() << "\r\n"
            << "\r\n"
            << body;
        EatWholeString(response, rawResponse);
        UNIT_ASSERT_EQUAL(response->Stage, NHttp::THttpIncomingResponse::EParseStage::Done);
        UNIT_ASSERT_VALUES_EQUAL(response->Body, body);

        NHttp::THttpOutgoingRequestPtr request2 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/test");
        NHttp::THttpIncomingResponsePtr duplicated = response->Duplicate(request2);

        UNIT_ASSERT_VALUES_EQUAL(duplicated->Body, body);
        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->ContentEncoding), "");
        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->ContentLength), ToString(body.size()));
    }

    // Test that THttpOutgoingResponse::Duplicate re-compresses for a new request with different Accept-Encoding
    Y_UNIT_TEST(OutgoingResponseDuplicateRecompresses) {
        // Create original request with gzip encoding
        std::vector<TString> compressContentTypes = {"text/plain"};
        auto endpoint = std::make_shared<NHttp::TPrivateEndpointInfo>(compressContentTypes);
        NHttp::THttpIncomingRequestPtr request1 = new NHttp::THttpIncomingRequest(endpoint, {});
        EatWholeString(request1, TString("GET /test HTTP/1.1\r\nAccept-Encoding: gzip\r\n\r\n"));
        UNIT_ASSERT_EQUAL(request1->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);

        NHttp::THttpOutgoingResponsePtr response = new NHttp::THttpOutgoingResponse(request1, "HTTP", "1.1", "200", "OK");
        response->Set("Content-Type", "text/plain");
        response->EnableCompression();
        TString body = "test body content for recompression that needs to be long enough to actually compress";
        response->SetBody(body);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->ContentEncoding), "gzip");
        UNIT_ASSERT_VALUES_EQUAL(response->Body, body); // Body getter should return decompressed

        // Duplicate for a request with deflate
        NHttp::THttpIncomingRequestPtr request2 = new NHttp::THttpIncomingRequest(endpoint, {});
        EatWholeString(request2, TString("GET /test HTTP/1.1\r\nAccept-Encoding: deflate\r\n\r\n"));

        NHttp::THttpOutgoingResponsePtr duplicated = response->Duplicate(request2);
        UNIT_ASSERT_VALUES_EQUAL(duplicated->Body, body);
        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->ContentEncoding), "deflate");
    }

    // Test that THttpOutgoingResponse::Duplicate handles request with no Accept-Encoding
    Y_UNIT_TEST(OutgoingResponseDuplicateNoCompression) {
        std::vector<TString> compressContentTypes = {"text/plain"};
        auto endpoint = std::make_shared<NHttp::TPrivateEndpointInfo>(compressContentTypes);
        NHttp::THttpIncomingRequestPtr request1 = new NHttp::THttpIncomingRequest(endpoint, {});
        EatWholeString(request1, TString("GET /test HTTP/1.1\r\nAccept-Encoding: gzip\r\n\r\n"));

        NHttp::THttpOutgoingResponsePtr response = new NHttp::THttpOutgoingResponse(request1, "HTTP", "1.1", "200", "OK");
        response->Set("Content-Type", "text/plain");
        response->EnableCompression();
        TString body = "test body content for compression testing purposes that is long enough";
        response->SetBody(body);
        UNIT_ASSERT_VALUES_EQUAL(TString(response->ContentEncoding), "gzip");

        // Duplicate for a request without Accept-Encoding (no compression)
        NHttp::THttpIncomingRequestPtr request2 = new NHttp::THttpIncomingRequest(endpoint, {});
        EatWholeString(request2, TString("GET /test HTTP/1.1\r\n\r\n"));

        NHttp::THttpOutgoingResponsePtr duplicated = response->Duplicate(request2);
        UNIT_ASSERT_VALUES_EQUAL(duplicated->Body, body);
        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->ContentEncoding), "");
    }

    // Test that outgoing request Duplicate properly strips Accept-Encoding when requested
    Y_UNIT_TEST(OutgoingRequestDuplicateStripsAcceptEncoding) {
        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/data");
        request->Set("Accept-Encoding", "gzip, deflate");

        NHttp::THeadersBuilder extraHeaders;
        extraHeaders.Set("Accept-Encoding", {}); // erase Accept-Encoding
        NHttp::THttpOutgoingRequestPtr duplicated = request->Duplicate(extraHeaders);

        NHttp::THeaders headers(duplicated->Headers);
        UNIT_ASSERT_VALUES_EQUAL(TString(headers["Accept-Encoding"]), "");
    }

    // Test that incoming request Duplicate properly strips Accept-Encoding
    Y_UNIT_TEST(IncomingRequestDuplicateStripsAcceptEncoding) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, TString("GET /test HTTP/1.1\r\nHost: example.com\r\nAccept-Encoding: gzip, deflate\r\n\r\n"));
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        UNIT_ASSERT_VALUES_EQUAL(TString(request->AcceptEncoding), "gzip, deflate");

        NHttp::THeadersBuilder extraHeaders;
        extraHeaders.Set("Accept-Encoding", {});
        NHttp::THttpIncomingRequestPtr duplicated = request->Duplicate(extraHeaders);

        UNIT_ASSERT_VALUES_EQUAL(TString(duplicated->AcceptEncoding), "");
    }
}

Y_UNIT_TEST_SUITE(HttpOutgoingCache) {

    // Test the outgoing cache: first request with Accept-Encoding gets cached without compression,
    // subsequent requests with different or no Accept-Encoding get proper responses
    Y_UNIT_TEST(OutgoingCacheStripsCompression) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        // Create outgoing cache with a policy that caches everything
        NHttp::TCachePolicy policy;
        policy.TimeToExpire = TDuration::Seconds(60);
        policy.TimeToRefresh = TDuration::Seconds(30);
        NActors::IActor* cache = NHttp::CreateOutgoingHttpCache(proxyId, [policy](const NHttp::THttpRequest*) {
            return policy;
        });
        NActors::TActorId cacheId = actorSystem.Register(cache);

        // Register a handler on the server side
        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(cacheId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/data", serverId)), 0, true);

        // First request: client requests with Accept-Encoding: gzip
        NActors::TActorId client1Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest1 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/data");
        httpRequest1->Set("Accept-Encoding", "gzip");
        actorSystem.Send(new NActors::IEventHandle(cacheId, client1Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest1)), 0, true);

        // Server receives the request - Accept-Encoding should be stripped by the cache
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* serverRequest = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(serverRequest->Request->URL, "/data");
        UNIT_ASSERT_VALUES_EQUAL(TString(serverRequest->Request->AcceptEncoding), "");

        // Server responds with plain (uncompressed) data since cache stripped Accept-Encoding
        TString responseBody = "response data from server";
        NHttp::THttpOutgoingResponsePtr serverResponse = serverRequest->Request->CreateResponseOK(responseBody, "text/plain");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(serverResponse)), 0, true);

        // Client 1 receives the response
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse1->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse1->Response->Body, responseBody);

        // Second request from a different client with no Accept-Encoding - should get cached response
        NActors::TActorId client2Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest2 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/data");
        actorSystem.Send(new NActors::IEventHandle(cacheId, client2Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest2)), 0, true);

        // Client 2 should receive the cached response directly (server should NOT get another request)
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse2->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse2->Response->Body, responseBody);

        // Third request with deflate Accept-Encoding - should also get cached response
        NActors::TActorId client3Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest3 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/data");
        httpRequest3->Set("Accept-Encoding", "deflate");
        actorSystem.Send(new NActors::IEventHandle(cacheId, client3Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest3)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse3 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse3->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse3->Response->Body, responseBody);
    }

    // Test outgoing cache with multiple waiters: several clients send requests before the first one completes
    Y_UNIT_TEST(OutgoingCacheMultipleWaiters) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NHttp::TCachePolicy policy;
        policy.TimeToExpire = TDuration::Seconds(60);
        policy.TimeToRefresh = TDuration::Seconds(30);
        NActors::IActor* cache = NHttp::CreateOutgoingHttpCache(proxyId, [policy](const NHttp::THttpRequest*) {
            return policy;
        });
        NActors::TActorId cacheId = actorSystem.Register(cache);

        NActors::TActorId serverId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(cacheId, serverId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/data", serverId)), 0, true);

        // Send two requests before the server responds
        NActors::TActorId client1Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest1 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/data");
        httpRequest1->Set("Accept-Encoding", "gzip");
        actorSystem.Send(new NActors::IEventHandle(cacheId, client1Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest1)), 0, true);

        NActors::TActorId client2Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest2 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/data");
        httpRequest2->Set("Accept-Encoding", "deflate");
        actorSystem.Send(new NActors::IEventHandle(cacheId, client2Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest2)), 0, true);

        // Server should only get ONE request (second is waiting)
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* serverRequest = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(serverRequest->Request->URL, "/data");
        UNIT_ASSERT_VALUES_EQUAL(TString(serverRequest->Request->AcceptEncoding), "");

        TString responseBody = "shared response data";
        NHttp::THttpOutgoingResponsePtr serverResponse = serverRequest->Request->CreateResponseOK(responseBody, "text/plain");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, serverId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(serverResponse)), 0, true);

        // Both clients should get the response
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse1->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse1->Response->Body, responseBody);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse2->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse2->Response->Body, responseBody);
    }
}

Y_UNIT_TEST_SUITE(HttpIncomingCache) {

    // Test incoming cache: handler receives requests without compression,
    // cached responses are re-adapted for clients with different Accept-Encoding
    Y_UNIT_TEST(IncomingCacheStoresUncompressedAndRecompresses) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NHttp::TCachePolicy policy;
        policy.TimeToExpire = TDuration::Seconds(60);
        policy.TimeToRefresh = TDuration::Seconds(30);
        NActors::IActor* cache = NHttp::CreateIncomingHttpCache(proxyId, [policy](const NHttp::THttpRequest*) {
            return policy;
        });
        NActors::TActorId cacheId = actorSystem.Register(cache);

        // Register the handler via the cache
        NActors::TActorId handlerId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(cacheId, handlerId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/api/data", handlerId)), 0, true);

        // First client request: with Accept-Encoding: gzip
        NActors::TActorId client1Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest1 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/api/data");
        httpRequest1->Set("Accept-Encoding", "gzip");
        actorSystem.Send(new NActors::IEventHandle(proxyId, client1Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest1)), 0, true);

        // The incoming cache receives the request and forwards to handler WITHOUT compression
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* handlerRequest = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(handlerRequest->Request->URL, "/api/data");
        // AcceptEncoding should be cleared by the cache
        UNIT_ASSERT_VALUES_EQUAL(TString(handlerRequest->Request->AcceptEncoding), "");

        // Handler responds with uncompressed data
        TString responseBody = "response data from the handler";
        NHttp::THttpOutgoingResponsePtr handlerResponse = handlerRequest->Request->CreateResponseOK(responseBody, "text/plain");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, handlerId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(handlerResponse)), 0, true);

        // Client 1 receives the response
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse1->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse1->Response->Body, responseBody);

        // Second client request: with no Accept-Encoding - should get cached response
        NActors::TActorId client2Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest2 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/api/data");
        actorSystem.Send(new NActors::IEventHandle(proxyId, client2Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest2)), 0, true);

        // Handler should NOT receive another request (it's cached)
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse2->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse2->Response->Body, responseBody);

        // Third client request: with Accept-Encoding: deflate
        NActors::TActorId client3Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest3 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/api/data");
        httpRequest3->Set("Accept-Encoding", "deflate");
        actorSystem.Send(new NActors::IEventHandle(proxyId, client3Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest3)), 0, true);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse3 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse3->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse3->Response->Body, responseBody);
    }

    // Test incoming cache with multiple waiters before handler responds
    Y_UNIT_TEST(IncomingCacheMultipleWaiters) {
        NActors::TTestActorRuntimeBase actorSystem(1, true);
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();

        NActors::IActor* proxy = NHttp::CreateHttpProxy();
        NActors::TActorId proxyId = actorSystem.Register(proxy);
        actorSystem.Send(new NActors::IEventHandle(proxyId, actorSystem.AllocateEdgeActor(), new NHttp::TEvHttpProxy::TEvAddListeningPort(port)), 0, true);
        actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle);

        NHttp::TCachePolicy policy;
        policy.TimeToExpire = TDuration::Seconds(60);
        policy.TimeToRefresh = TDuration::Seconds(30);
        NActors::IActor* cache = NHttp::CreateIncomingHttpCache(proxyId, [policy](const NHttp::THttpRequest*) {
            return policy;
        });
        NActors::TActorId cacheId = actorSystem.Register(cache);

        NActors::TActorId handlerId = actorSystem.AllocateEdgeActor();
        actorSystem.Send(new NActors::IEventHandle(cacheId, handlerId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/api/data", handlerId)), 0, true);

        // Send two requests: first with gzip, second with deflate
        NActors::TActorId client1Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest1 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/api/data");
        httpRequest1->Set("Accept-Encoding", "gzip");
        actorSystem.Send(new NActors::IEventHandle(proxyId, client1Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest1)), 0, true);

        NActors::TActorId client2Id = actorSystem.AllocateEdgeActor();
        NHttp::THttpOutgoingRequestPtr httpRequest2 = NHttp::THttpOutgoingRequest::CreateRequestGet("http://127.0.0.1:" + ToString(port) + "/api/data");
        httpRequest2->Set("Accept-Encoding", "deflate");
        actorSystem.Send(new NActors::IEventHandle(proxyId, client2Id, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest2)), 0, true);

        // Handler receives only ONE request (the cache combining them)
        NHttp::TEvHttpProxy::TEvHttpIncomingRequest* handlerRequest = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_EQUAL(handlerRequest->Request->URL, "/api/data");
        UNIT_ASSERT_VALUES_EQUAL(TString(handlerRequest->Request->AcceptEncoding), "");

        TString responseBody = "shared cache response";
        NHttp::THttpOutgoingResponsePtr handlerResponse = handlerRequest->Request->CreateResponseOK(responseBody, "text/plain");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, handlerId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(handlerResponse)), 0, true);

        // Both clients get correct responses
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse1 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse1->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse1->Response->Body, responseBody);

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse* clientResponse2 = actorSystem.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(clientResponse2->Response->Status, "200");
        UNIT_ASSERT_VALUES_EQUAL(clientResponse2->Response->Body, responseBody);
    }
}
