#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/map.h>
#include <ydb/library/testlib/service_mocks/session_service_mock.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include "oidc_protected_page_handler.h"
#include "oidc_session_create_handler.h"
#include "restore_context_handler.h"
#include "oidc_settings.h"
#include "openid_connect.h"
#include "context.h"
#include "context_storage.h"

using namespace NMVP::NOIDC;

namespace {

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

}

Y_UNIT_TEST_SUITE(OidcProxyTests) {
    void RequestWithIamTokenTest(NMvp::EAccessServiceType profile) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AllowedProxyHosts = {allowedProxyHost},
            .AccessServiceType = profile
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        const TString iamToken {"protected_page_iam_token"};
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, "GET /" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                                "Host: oidcproxy.net\r\n"
                                "Authorization: Bearer " + iamToken + "\r\n");
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        TAutoPtr<IEventHandle> handle;

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer " + iamToken);
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "200");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Body, "this is test.");
    }

    Y_UNIT_TEST(RequestWithIamTokenYandex) {
        RequestWithIamTokenTest(NMvp::yandex_v2);
    }

    Y_UNIT_TEST(RequestWithIamTokenNebius) {
        RequestWithIamTokenTest(NMvp::nebius_v1);
    }

    void NonAuthorizeRequestWithOptionMethodTest(NMvp::EAccessServiceType profile) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AllowedProxyHosts = {allowedProxyHost},
            .AccessServiceType = profile
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, "OPTIONS /" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                                "Host: oidcproxy.net\r\n");
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        TAutoPtr<IEventHandle> handle;

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        NHttp::THttpOutgoingRequestPtr outgoingRequest = outgoingRequestEv->Request;
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequest->Method, "OPTIONS");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequest->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequest->URL, "/counters");
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequest);
        EatWholeString(incomingResponse, "HTTP/1.1 204 No Content\r\nConnection: close\r\n"
                                                  "Access-Control-Allow-Origin: https://monitoring.ydb.test.ru\r\n"
                                                  "Access-Control-Allow-Headers: Content-Type,Authorization,Origin,Accept\r\n"
                                                  "Access-Control-Allow-Methods: OPTIONS, GET, POST, PUT, DELETE\r\n"
                                                  "Access-Control-Allow-Credentials: true\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequest, incomingResponse)));

        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "204");
        NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT_STRINGS_EQUAL(headers.Get("Access-Control-Allow-Origin"), "https://monitoring.ydb.test.ru");
        UNIT_ASSERT_STRINGS_EQUAL(headers.Get("Access-Control-Allow-Headers"), "Content-Type,Authorization,Origin,Accept");
        UNIT_ASSERT_STRINGS_EQUAL(headers.Get("Access-Control-Allow-Methods"), "OPTIONS, GET, POST, PUT, DELETE");
        UNIT_ASSERT_STRINGS_EQUAL(headers.Get("Access-Control-Allow-Credentials"), "true");
    }

    Y_UNIT_TEST(NonAuthorizeRequestWithOptionMethodYandex) {
        NonAuthorizeRequestWithOptionMethodTest(NMvp::yandex_v2);
    }

    Y_UNIT_TEST(NonAuthorizeRequestWithOptionMethodNebius) {
        NonAuthorizeRequestWithOptionMethodTest(NMvp::nebius_v1);
    }

    void SessionServiceCheckValidCookieTest(NMvp::EAccessServiceType profile) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AllowedProxyHosts = {allowedProxyHost},
            .AccessServiceType = profile
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, "GET /" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                                "Host: oidcproxy.net\r\n"
                                "Cookie: yc_session=allowed_session_cookie\r\n\r\n");
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        TAutoPtr<IEventHandle> handle;

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer protected_page_iam_token");
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "200");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Body, "this is test.");
    }

    Y_UNIT_TEST(SessionServiceCheckValidCookieYandex) {
        NonAuthorizeRequestWithOptionMethodTest(NMvp::yandex_v2);
    }

    Y_UNIT_TEST(SessionServiceCheckValidCookieNebius) {
        NonAuthorizeRequestWithOptionMethodTest(NMvp::nebius_v1);
    }

    Y_UNIT_TEST(ProxyOnHttpsHost) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AllowedProxyHosts = {allowedProxyHost},
            .AccessServiceType = NMvp::yandex_v2
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, "GET /" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                                "Host: oidcproxy.net\r\n"
                                "Cookie: yc_session=allowed_session_cookie\r\n\r\n");
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        TAutoPtr<IEventHandle> handle;

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer protected_page_iam_token");
        UNIT_ASSERT_EQUAL(outgoingRequestEv->Request->Secure, false);
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        const TString errorResponseBody {"The plain HTTP request was sent to HTTPS port"};
        EatWholeString(incomingResponse, "HTTP/1.1 400 Bad Request\r\n"
                                         "Connection: close\r\n"
                                         "Content-Type: text/html\r\n"
                                         "Content-Length: " + ToString(errorResponseBody.size()) + "\r\n\r\n" + errorResponseBody);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer protected_page_iam_token");
        UNIT_ASSERT_EQUAL(outgoingRequestEv->Request->Secure, true);
        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        const TString okResponseBody {"this is test"};
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                         "Connection: close\r\n"
                                         "Content-Type: text/html\r\n"
                                         "Content-Length: " + ToString(okResponseBody.size()) + "\r\n\r\n" + okResponseBody);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "200");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Body, okResponseBody);

        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer protected_page_iam_token");
        UNIT_ASSERT_EQUAL(outgoingRequestEv->Request->Secure, false);
        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        const TString errorJsonResponseBody {"{\"status\":\"400\", \"message\":\"Table does not exist\"}"};
        EatWholeString(incomingResponse, "HTTP/1.1 400 Bad Request\r\n"
                                         "Connection: close\r\n"
                                         "Content-Type: application/json; charset=utf-8\r\n"
                                         "Content-Length: " + ToString(errorJsonResponseBody.size()) + "\r\n\r\n" + errorJsonResponseBody);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "400");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Body, errorJsonResponseBody);
    }


    Y_UNIT_TEST(FixLocationHeader) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page:1234"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AllowedProxyHosts = {allowedProxyHost},
            .AccessServiceType = NMvp::yandex_v2
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, "GET /http://" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                                "Host: oidcproxy.net\r\n"
                                "Cookie: yc_session=allowed_session_cookie\r\n\r\n");
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        TAutoPtr<IEventHandle> handle;

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer protected_page_iam_token");
        UNIT_ASSERT_EQUAL(outgoingRequestEv->Request->Secure, false);

        // Location start with '/'
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 307 Temporary Redirect\r\n"
                                        "Connection: close\r\n"
                                        "Location: /node/12345/counters\r\n"
                                        "Content-Length:0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "307");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Headers, "Location: /http://" + allowedProxyHost + "/node/12345/counters");

        // Location start with "//"
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);

        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 302 Found\r\n"
                                        "Connection: close\r\n"
                                        "Location: //new.oidc.proxy.host:1234/node/12345/counters\r\n"
                                        "Content-Length:0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Headers, "Location: /http://new.oidc.proxy.host:1234/node/12345/counters");

        // Location start with ".."
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);

        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 302 Found\r\n"
                                        "Connection: close\r\n"
                                        "Location: ../node/12345/counters\r\n"
                                        "Content-Length:0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Headers, "Location: ../node/12345/counters");

        // Location is absolute URL
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);

        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 302 Found\r\n"
                                        "Connection: close\r\n"
                                        "Location: https://some.new.oidc.host:9876/counters/v1\r\n"
                                        "Content-Length:0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Headers, "Location: /https://some.new.oidc.host:9876/counters/v1");

        // Location is sub-resources URL
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);

        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 302 Found\r\n"
                                        "Connection: close\r\n"
                                        "Location: v1/\r\n"
                                        "Content-Length:0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Headers, "Location: v1/");
    }


    Y_UNIT_TEST(ExchangeNebius) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .AllowedProxyHosts = {allowedProxyHost},
            .AccessServiceType = NMvp::nebius_v1
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, "GET /" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                                "Host: oidcproxy.net\r\n"
                                "Cookie: yc_session=allowed_session_cookie;"
                                + CreateSecureCookie(settings.ClientId, "session_cookie") + "\r\n\r\n");
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        TAutoPtr<IEventHandle> handle;

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, "auth.test.net");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/oauth2/session/exchange");
        UNIT_ASSERT_EQUAL(outgoingRequestEv->Request->Secure, true);
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        TString okResponseBody {"{\"access_token\": \"access_token\"}"};
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                         "Connection: close\r\n"
                                         "Content-Type: text/html\r\n"
                                         "Content-Length: " + ToString(okResponseBody.size()) + "\r\n\r\n" + okResponseBody);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer access_token");
        UNIT_ASSERT_EQUAL(outgoingRequestEv->Request->Secure, false);
        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        okResponseBody = "this is test";
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                         "Connection: close\r\n"
                                         "Content-Type: text/html\r\n"
                                         "Content-Length: " + ToString(okResponseBody.size()) + "\r\n\r\n" + okResponseBody);

        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));
        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "200");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Body, "this is test");
    }

    Y_UNIT_TEST(SessionServiceCheckAuthorizationFail) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AllowedProxyHosts = {allowedProxyHost}
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.IsTokenAllowed = false;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, "GET /" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                                "Host: oidcproxy.net\r\n"
                                "Cookie: yc_session=allowed_session_cookie\r\n\r\n");
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)));
        TAutoPtr<IEventHandle> handle;
        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "401");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Message, "Authorization IAM token are invalid or may have expired");
    }

    class TRedirectStrategyBase {
    public:
        virtual TString CreateRequest(const TString& request) = 0;
        virtual TString GetRedirectUrl(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv) = 0;
        virtual void CheckRedirectStatus(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv) = 0;
        virtual void CheckLocationHeader(const TStringBuf& location, const TString& host, const TString& url) = 0;
        virtual void CheckRequestedAddress(const TStringBuf& body, const TString& host, const TString& url) = 0;
        virtual TString GetUrlFromLocationHeader(const TStringBuf& location) = 0;
        virtual void CheckSpecificHeaders(const NHttp::THeaders&) {}
        virtual bool IsAjaxRequest() const {
            return false;
        }
    };

    class TRedirectStrategy : public TRedirectStrategyBase {
    public:
        TString GetRedirectUrl(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv) override {
            const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
            UNIT_ASSERT(headers.Has("Location"));
            return TString(headers.Get("Location"));
        }

        TString CreateRequest(const TString& request) override {
            TString newRequest = request;
            newRequest += "\r\n";
            return newRequest;
        }

        void CheckRedirectStatus(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv) override {
            UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        }

        void CheckLocationHeader(const TStringBuf& location, const TString& host, const TString& url) override {
            Y_UNUSED(host);
            UNIT_ASSERT_STRINGS_EQUAL(location, url);
        }

        virtual void CheckRequestedAddress(const TStringBuf& body, const TString& host, const TString& url) override {
            Y_UNUSED(host);
            UNIT_ASSERT_STRING_CONTAINS(body, "\"requested_address\":\"" + url + "\"");
        }

        TString GetUrlFromLocationHeader(const TStringBuf& location) override {
            return TString(location);
        }
    };

    class TAjaxRedirectStrategy : public TRedirectStrategyBase {
    public:
        TString GetRedirectUrl(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv) override {
            NJson::TJsonReaderConfig jsonConfig;
            NJson::TJsonValue jsonValue;
            TString errorMessage;
            TString authUrl;
            if (NJson::ReadJsonTree(outgoingResponseEv->Response->Body, &jsonConfig, &jsonValue)) {
                const NJson::TJsonValue* jsonErrorMessage;
                if (jsonValue.GetValuePointer("error", &jsonErrorMessage)) {
                    errorMessage = jsonErrorMessage->GetStringRobust();
                }
                const NJson::TJsonValue* jsonAuthUrl;
                if (jsonValue.GetValuePointer("authUrl", &jsonAuthUrl)) {
                    authUrl = jsonAuthUrl->GetStringRobust();
                }
            }
            const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
            UNIT_ASSERT(!headers.Has("Location"));
            UNIT_ASSERT_STRINGS_EQUAL(errorMessage, "Authorization Required");
            return authUrl;
        }

        TString CreateRequest(const TString& request) override {
            TString newRequest = request;
            newRequest += "Accept: application/json, text/plain\r\n\r\n";
            return newRequest;
        }

        void CheckRedirectStatus(NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv) override {
            UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "401");
        }

        void CheckLocationHeader(const TStringBuf& location, const TString& host, const TString& url) override {
            UNIT_ASSERT_STRINGS_EQUAL(location, "https://" + host + url);
        }

        virtual void CheckRequestedAddress(const TStringBuf& body, const TString& host, const TString& url) override {
            UNIT_ASSERT_STRING_CONTAINS(body, "\"requested_address\":\"https://" + host + url + "\"");
        }

        TString GetUrlFromLocationHeader(const TStringBuf& location) override {
            TStringBuf scheme, host, url;
            NHttp::CrackURL(location, scheme, host, url);
            return TString(url);
        }

        void CheckSpecificHeaders(const NHttp::THeaders& headers) override {
            static const TStringBuf accessControlAllowOrigin {"Access-Control-Allow-Origin"};
            static const TStringBuf accessControlAllowCredentials {"Access-Control-Allow-Credentials"};
            static const TStringBuf accessControlAllowHeaders {"Access-Control-Allow-Headers"};
            static const TStringBuf accessControlAllowMethods {"Access-Control-Allow-Methods"};

            UNIT_ASSERT(headers.Has(accessControlAllowOrigin));
            UNIT_ASSERT_STRINGS_EQUAL("*", headers.Get(accessControlAllowOrigin));

            UNIT_ASSERT(headers.Has(accessControlAllowCredentials));
            UNIT_ASSERT_STRINGS_EQUAL("true", headers.Get(accessControlAllowCredentials));

            UNIT_ASSERT(headers.Has(accessControlAllowHeaders));
            UNIT_ASSERT_STRINGS_EQUAL("Content-Type,Authorization,Origin,Accept", headers.Get(accessControlAllowHeaders));

            UNIT_ASSERT(headers.Has(accessControlAllowMethods));
            UNIT_ASSERT_STRINGS_EQUAL("OPTIONS, GET, POST", headers.Get(accessControlAllowMethods));
        }

        bool IsAjaxRequest() const override {
            return true;
        }
    };

    void FullAuthorizationFlow(TRedirectStrategyBase& redirectStrategy, bool storeContextOnHost = true) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .ClientId = "client_id",
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "0123456789abcdef",
            .AllowedProxyHosts = {allowedProxyHost},
            .StoreContextOnHost = storeContextOnHost
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        sessionServiceMock.AllowedAccessTokens.insert("access_token_value");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        incomingRequest->Endpoint->Secure = true;

        const TString hostProxy = "oidcproxy.net";
        const TString protectedPage = "/" + allowedProxyHost + "/counters";

        EatWholeString(incomingRequest, redirectStrategy.CreateRequest("GET " + protectedPage + " HTTP/1.1\r\n"
                                                                                   "Host: " + hostProxy + "\r\n"
                                                                                   "Cookie: yc_session=invalid_cookie\r\n"
                                                                                   "Referer: https://" + hostProxy + protectedPage + "\r\n"));
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        TAutoPtr<IEventHandle> handle;
        NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        redirectStrategy.CheckRedirectStatus(outgoingResponseEv);
        TString location = redirectStrategy.GetRedirectUrl(outgoingResponseEv);
        UNIT_ASSERT_STRING_CONTAINS(location, "https://auth.test.net/oauth/authorize");
        UNIT_ASSERT_STRING_CONTAINS(location, "response_type=code");
        UNIT_ASSERT_STRING_CONTAINS(location, "scope=openid");
        UNIT_ASSERT_STRING_CONTAINS(location, "client_id=" + settings.ClientId);
        UNIT_ASSERT_STRING_CONTAINS(location, "redirect_uri=https://" + hostProxy + "/auth/callback");

        NHttp::TUrlParameters urlParameters(location);
        const TString state = urlParameters["state"];

        const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        TStringBuf setCookie;
        if (storeContextOnHost) {
            UNIT_ASSERT(!headers.Has("Set-Cookie"));
        } else {
            UNIT_ASSERT(headers.Has("Set-Cookie"));
            setCookie = headers.Get("Set-Cookie");
            UNIT_ASSERT_STRING_CONTAINS(setCookie, CreateNameYdbOidcCookie(settings.ClientSecret, state));
        }
        redirectStrategy.CheckSpecificHeaders(headers);

        const NActors::TActorId sessionCreator = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorage));
        incomingRequest = new NHttp::THttpIncomingRequest();
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << state << " HTTP/1.1\r\n";
        request << "Host: " + hostProxy + "\r\n";
        if (!storeContextOnHost) {
            request << "Cookie: " << setCookie.NextTok(";") << "\r\n";
        }
        EatWholeString(incomingRequest, redirectStrategy.CreateRequest(request));
        runtime.Send(new IEventHandle(sessionCreator, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        const TStringBuf& body = outgoingRequestEv->Request->Body;
        UNIT_ASSERT_STRING_CONTAINS(body, "code=code_template");
        UNIT_ASSERT_STRING_CONTAINS(body, "grant_type=authorization_code");

        const TString authorizationServerResponse = R"___({"access_token":"access_token_value","token_type":"bearer","expires_in":43199,"scope":"openid","id_token":"id_token_value"})___";
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(authorizationServerResponse.length()) + "\r\n\r\n" + authorizationServerResponse);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Message, "Cookie set");
        const NHttp::THeaders protectedPageHeaders(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(protectedPageHeaders.Has("Location"));
        redirectStrategy.CheckLocationHeader(protectedPageHeaders.Get("Location"), hostProxy, protectedPage);
        UNIT_ASSERT(protectedPageHeaders.Has("Set-Cookie"));
        TStringBuf sessionCookie = protectedPageHeaders.Get("Set-Cookie");
        UNIT_ASSERT_STRINGS_EQUAL(sessionCookie, "yc_session=allowed_session_cookie; SameSite=None");

        incomingRequest = new NHttp::THttpIncomingRequest();
        request.clear();
        TString redirectUrl = redirectStrategy.GetUrlFromLocationHeader(protectedPageHeaders.Get("Location"));
        request << "GET " << redirectUrl << " HTTP/1.1\r\n";
        request << "Host: " + hostProxy + "\r\n";
        request << "Cookie: " << sessionCookie.NextTok(';') << "\r\n";
        EatWholeString(incomingRequest, redirectStrategy.CreateRequest(request));
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer protected_page_iam_token");
        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "200");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Body, "this is test.");
    }

    Y_UNIT_TEST(FullAuthorizationFlow) {
        TRedirectStrategy redirectStrategy;
        FullAuthorizationFlow(redirectStrategy, false);
    }

    Y_UNIT_TEST(FullAuthorizationFlowAjax) {
        TAjaxRedirectStrategy redirectStrategy;
        FullAuthorizationFlow(redirectStrategy, false);
    }

    Y_UNIT_TEST(FullAuthorizationFlowWithSaveContextOnHost) {
        TRedirectStrategy redirectStrategy;
        FullAuthorizationFlow(redirectStrategy);
    }

    Y_UNIT_TEST(FullAuthorizationFlowAjaxWithSaveContextOnHost) {
        TAjaxRedirectStrategy redirectStrategy;
        FullAuthorizationFlow(redirectStrategy);
    }

    void WrongStateAuthorizationFlow(TRedirectStrategyBase& redirectStrategy, bool storeContextOnHost = true) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .ClientId = "client_id",
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "0123456789abcdef",
            .StoreContextOnHost = storeContextOnHost
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedAccessTokens.insert("valid_access_token");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const NActors::TActorId sessionCreator = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorage));
        const TString wrongState = "wrong_state";
        const TString hostProxy = "oidcproxy.net";
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << wrongState << " HTTP/1.1\r\n";
        request << "Host: " + hostProxy + "\r\n";
        if (!storeContextOnHost) {
            const TString state = "test_state";
            TContext context(state, "/requested/page", redirectStrategy.IsAjaxRequest());
            request << "Cookie: " << context.CreateYdbOidcCookie(settings.ClientSecret) << "\r\n";
        }
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, redirectStrategy.CreateRequest(request));
        incomingRequest->Endpoint->Secure = true;
        runtime.Send(new IEventHandle(sessionCreator, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        TAutoPtr<IEventHandle> handle;
        NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "400");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Body, "Unknown error has occurred. Please open the page again");

    }

    Y_UNIT_TEST(WrongStateAuthorizationFlow) {
        TRedirectStrategy redirectStrategy;
        WrongStateAuthorizationFlow(redirectStrategy, false);
    }

    Y_UNIT_TEST(WrongStateAuthorizationFlowAjax) {
        TAjaxRedirectStrategy redirectStrategy;
        WrongStateAuthorizationFlow(redirectStrategy, false);
    }

    Y_UNIT_TEST(WrongStateAuthorizationFlowWithSaveContextOnHost) {
        TRedirectStrategy redirectStrategy;
        WrongStateAuthorizationFlow(redirectStrategy);
    }

    Y_UNIT_TEST(WrongStateAuthorizationFlowAjaxWithSaveContextOnHost) {
        TAjaxRedirectStrategy redirectStrategy;
        WrongStateAuthorizationFlow(redirectStrategy);
    }

    void SessionServiceCreateAuthorizationFail(bool storeContextOnHost = true) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef",
            .StoreContextOnHost = storeContextOnHost
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId sessionCreator = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.IsTokenAllowed = false;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        TContext context("test_state", "/requested/page", false);
        if (storeContextOnHost) {
            contextStorage.Write(context);
        }
        const TString stateParam = (storeContextOnHost ? context.CreateStateContainer(settings.ClientSecret, "localhost") : context.GetState());
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << stateParam << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        if (!storeContextOnHost) {
            request << "Cookie: " << context.CreateYdbOidcCookie(settings.ClientSecret) << "\r\n\r\n";
        }
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, request);
        runtime.Send(new IEventHandle(sessionCreator, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        const TStringBuf& body = outgoingRequestEv->Request->Body;
        UNIT_ASSERT_STRING_CONTAINS(body, "code=code_template");
        UNIT_ASSERT_STRING_CONTAINS(body, "grant_type=authorization_code");

        const TString authorizationServerResponse = R"___({"access_token":"access_token_value","token_type":"bearer","expires_in":43199,"scope":"openid","id_token":"id_token_value"})___";
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(authorizationServerResponse.length()) + "\r\n\r\n" + authorizationServerResponse);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));
        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "401");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Message, "Authorization IAM token are invalid or may have expired");
    }

    Y_UNIT_TEST(SessionServiceCreateAuthorizationFail) {
        SessionServiceCreateAuthorizationFail(false);
    }

    Y_UNIT_TEST(SessionServiceCreateAuthorizationFailWithSaveContextOnHost) {
        SessionServiceCreateAuthorizationFail(true);
    }

    void SessionServiceCreateAccessTokenInvalid(TRedirectStrategyBase& redirectStrategy, bool storeContextOnHost) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .ClientId = "client_id",
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef",
            .StoreContextOnHost = storeContextOnHost
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedAccessTokens.insert("valid_access_token");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const NActors::TActorId sessionCreator = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorage));
        TContext context("test_state", "/requested/page", redirectStrategy.IsAjaxRequest());
        if (storeContextOnHost) {
            contextStorage.Write(context);
        }
        const TString stateParam = (storeContextOnHost ? context.CreateStateContainer(settings.ClientSecret, "localhost") : context.GetState());
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << stateParam << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        if (!storeContextOnHost) {
            request << "Cookie: " << context.CreateYdbOidcCookie(settings.ClientSecret) << "\r\n";
        }
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, redirectStrategy.CreateRequest(request));
        incomingRequest->Endpoint->Secure = true;
        runtime.Send(new IEventHandle(sessionCreator, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        const TStringBuf& body = outgoingRequestEv->Request->Body;
        UNIT_ASSERT_STRING_CONTAINS(body, "code=code_template");
        UNIT_ASSERT_STRING_CONTAINS(body, "grant_type=authorization_code");

        const TString authorizationServerResponse = R"___({"access_token":"invalid_access_token","token_type":"bearer","expires_in":43199,"scope":"openid","id_token":"id_token_value"})___";
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(authorizationServerResponse.length()) + "\r\n\r\n" + authorizationServerResponse);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));
        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(headers.Has("Location"));
        TStringBuf location = headers.Get("Location");
        UNIT_ASSERT_STRING_CONTAINS(location, "/requested/page");
    }

    Y_UNIT_TEST(SessionServiceCreateAccessTokenInvalid) {
        TRedirectStrategy redirectStrategy;
        SessionServiceCreateAccessTokenInvalid(redirectStrategy, false);
    }

    Y_UNIT_TEST(SessionServiceCreateAccessTokenInvalidAjax) {
        TAjaxRedirectStrategy redirectStrategy;
        SessionServiceCreateAccessTokenInvalid(redirectStrategy, false);
    }

    Y_UNIT_TEST(SessionServiceCreateAccessTokenInvalidWithSaveContextOnHost) {
        TRedirectStrategy redirectStrategy;
        SessionServiceCreateAccessTokenInvalid(redirectStrategy, true);
    }

    Y_UNIT_TEST(SessionServiceCreateAccessTokenInvalidAjaxWithSaveContextOnHost) {
        TAjaxRedirectStrategy redirectStrategy;
        SessionServiceCreateAccessTokenInvalid(redirectStrategy, true);
    }

    void SessionServiceCreateOpenIdScopeMissed(bool storeContextOnHost) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef",
            .StoreContextOnHost = storeContextOnHost
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId sessionCreator = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.IsOpenIdScopeMissed = true;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());


        TContext context("test_state", "/requested/page", false);
        const TString stateParam = (storeContextOnHost ? context.CreateStateContainer(settings.ClientSecret, "localhost") : context.GetState());
        if (storeContextOnHost) {
            contextStorage.Write(context);
        }
        TStringBuilder request;
        request << "GET /callback?code=code_template&state=" << stateParam << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        if (!storeContextOnHost) {
            request << "Cookie: " << context.CreateYdbOidcCookie(settings.ClientSecret) << "\r\n\r\n";
        }
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, request);
        runtime.Send(new IEventHandle(sessionCreator, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        const TStringBuf& body = outgoingRequestEv->Request->Body;
        UNIT_ASSERT_STRING_CONTAINS(body, "code=code_template");
        UNIT_ASSERT_STRING_CONTAINS(body, "grant_type=authorization_code");

        const TString authorizationServerResponse = R"___({"access_token":"access_token_value","token_type":"bearer","expires_in":43199,"scope":"openid","id_token":"id_token_value"})___";
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(authorizationServerResponse.length()) + "\r\n\r\n" + authorizationServerResponse);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));
        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "412");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Message, "Openid scope is missed for specified access_token");
    }

    Y_UNIT_TEST(SessionServiceCreateOpenIdScopeMissed) {
        SessionServiceCreateOpenIdScopeMissed(false);
    }

    Y_UNIT_TEST(SessionServiceCreateOpenIdScopeMissedWithSaveContextOnHost) {
        SessionServiceCreateOpenIdScopeMissed(true);
    }

    void SessionServiceCreateWithSeveralCookies(TRedirectStrategyBase& redirectStrategy, bool storeContextOnHost) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef",
            .StoreContextOnHost = storeContextOnHost
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedAccessTokens.insert("valid_access_token");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const NActors::TActorId sessionCreator = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorage));
        TContext context1("first_request_state", "/requested/page", redirectStrategy.IsAjaxRequest());
        TContext context2("second_request_state", "/requested/page", redirectStrategy.IsAjaxRequest());
        if (storeContextOnHost) {
            contextStorage.Write(context1);
            contextStorage.Write(context2);
        }
        const TString stateParam = (storeContextOnHost ? context1.CreateStateContainer(settings.ClientSecret, "localhost") : context1.GetState());
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << stateParam << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        if (!storeContextOnHost) {
            request << "Cookie: " << context1.CreateYdbOidcCookie(settings.ClientSecret) << "; " << context2.CreateYdbOidcCookie(settings.ClientSecret) << "\r\n";
        }
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, redirectStrategy.CreateRequest(request));
        incomingRequest->Endpoint->Secure = true;
        runtime.Send(new IEventHandle(sessionCreator, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        const TStringBuf& body = outgoingRequestEv->Request->Body;
        UNIT_ASSERT_STRING_CONTAINS(body, "code=code_template");
        UNIT_ASSERT_STRING_CONTAINS(body, "grant_type=authorization_code");
    }

    Y_UNIT_TEST(SessionServiceCreateWithSeveralCookies) {
        TRedirectStrategy redirectStrategy;
        SessionServiceCreateWithSeveralCookies(redirectStrategy, false);
    }

    Y_UNIT_TEST(SessionServiceCreateWithSeveralCookiesAjax) {
        TAjaxRedirectStrategy redirectStrategy;
        SessionServiceCreateWithSeveralCookies(redirectStrategy, false);
    }

    Y_UNIT_TEST(SessionServiceCreateWithSeveralCookiesWithSaveContextOnHost) {
        TRedirectStrategy redirectStrategy;
        SessionServiceCreateWithSeveralCookies(redirectStrategy, true);
    }

    Y_UNIT_TEST(SessionServiceCreateWithSeveralCookiesAjaxWithSaveContextOnHost) {
        TAjaxRedirectStrategy redirectStrategy;
        SessionServiceCreateWithSeveralCookies(redirectStrategy, true);
    }

    Y_UNIT_TEST(AllowedHostsList) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        std::vector<TString> allowedProxyHosts {
            "first.ydb.viewer.page",
            "second.ydb.viewer.page",
            "https://some.monitoring.page"
        };

        std::vector<TString> forbiddenProxyHosts {
            "first.ydb.viewer.forbidden.page",
            "second.ydb.viewer.forbidden.page"
        };

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "0123456789abcdef",
            .AllowedProxyHosts = {
                "*.viewer.page",
                "some.monitoring.page"
            }
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        sessionServiceMock.AllowedAccessTokens.insert("access_token_value");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const TString hostProxy = "oidcproxy.net";
        const TString protectedPage = "/counters";

        auto checkAllowedHostList = [&] (const TString& requestedHost, const TString& expectedStatus, const TString& expectedBodyContent = "") {
            const TString url = "/" + requestedHost + protectedPage;
            TStringBuilder httpRequest;
            httpRequest << "GET " + url + " HTTP/1.1\r\n"
                        << "Host: " + hostProxy + "\r\n"
                        << "Cookie: yc_session=invalid_cookie\r\n"
                        << "Referer: https://" + hostProxy + url + "\r\n";

            NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
            incomingRequest->Endpoint->Secure = true;
            EatWholeString(incomingRequest, httpRequest);
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

            TAutoPtr<IEventHandle> handle;
            NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, expectedStatus);
            if (!expectedBodyContent.empty()) {
                UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Body, expectedBodyContent);
            }
        };

        for (const TString& allowedHost : allowedProxyHosts) {
            checkAllowedHostList(allowedHost, "302");
        }

        for (const TString& forbiddenHost : forbiddenProxyHosts) {
            checkAllowedHostList(forbiddenHost, "403", "403 Forbidden host: " + forbiddenHost);
        }
    }

    Y_UNIT_TEST(HandleNullResponseFromProtectedResource) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AllowedProxyHosts = {allowedProxyHost},
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorage));

        const TString iamToken {"protected_page_iam_token"};
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, "GET /" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                                "Host: oidcproxy.net\r\n"
                                "Authorization: Bearer " + iamToken + "\r\n");
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
        TAutoPtr<IEventHandle> handle;

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer " + iamToken);

        const TString expectedError = "Response is NULL for some reason";
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, nullptr, expectedError)));

        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "400");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Body, expectedError);
    }

    void FullAuthorizationFlowWithStoreContextOnOtherHost(TRedirectStrategyBase& redirectStrategy) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .ClientId = "client_id",
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "0123456789abcdef",
            .AllowedProxyHosts = {allowedProxyHost},
            .StoreContextOnHost = true
        };

        TContextStorage contextStorageFirstHost;
        TContextStorage contextStorageSecondHost;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings, &contextStorageFirstHost));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        sessionServiceMock.AllowedAccessTokens.insert("access_token_value");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        // Try request protected resource
        const TString workerName1 = "oidc-01.host.net";
        NHttp::THttpIncomingRequestPtr incomingRequestToOidcHost1 = new NHttp::THttpIncomingRequest();
        incomingRequestToOidcHost1->Endpoint->Secure = true;
        incomingRequestToOidcHost1->Endpoint->WorkerName = workerName1;

        const TString hostProxy = "oidcproxy.net";
        const TString protectedPage = "/" + allowedProxyHost + "/counters";

        EatWholeString(incomingRequestToOidcHost1, redirectStrategy.CreateRequest("GET " + protectedPage + " HTTP/1.1\r\n"
                                                                                   "Host: " + hostProxy + "\r\n"
                                                                                   "Cookie: yc_session=invalid_cookie\r\n"
                                                                                   "Referer: https://" + hostProxy + protectedPage + "\r\n"));
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequestToOidcHost1)));

        // Need authorization. Return code to /auth/callback handler
        TAutoPtr<IEventHandle> handle;
        NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        redirectStrategy.CheckRedirectStatus(outgoingResponseEv);
        TString location = redirectStrategy.GetRedirectUrl(outgoingResponseEv);
        UNIT_ASSERT_STRING_CONTAINS(location, "https://auth.test.net/oauth/authorize");
        UNIT_ASSERT_STRING_CONTAINS(location, "response_type=code");
        UNIT_ASSERT_STRING_CONTAINS(location, "scope=openid");
        UNIT_ASSERT_STRING_CONTAINS(location, "client_id=" + settings.ClientId);
        UNIT_ASSERT_STRING_CONTAINS(location, "redirect_uri=https://" + hostProxy + "/auth/callback");

        NHttp::TUrlParameters urlParameters(location);
        const TString stateParam = urlParameters["state"];

        const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(!headers.Has("Set-Cookie"));
        redirectStrategy.CheckSpecificHeaders(headers);

        const NActors::TActorId sessionCreator = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorageSecondHost));
        const TString workerName2 = "oidc-02.host.net";
        NHttp::THttpIncomingRequestPtr incomingRequestToOidcHost2 = new NHttp::THttpIncomingRequest();
        incomingRequestToOidcHost2->Endpoint->Secure = true;
        incomingRequestToOidcHost2->Endpoint->WorkerName = workerName2;
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << stateParam << " HTTP/1.1\r\n";
        request << "Host: " << hostProxy << "\r\n";
        EatWholeString(incomingRequestToOidcHost2, redirectStrategy.CreateRequest(request));
        runtime.Send(new IEventHandle(sessionCreator, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequestToOidcHost2)));

        // Context was not found on host2. Restore context on host1
        NJson::TJsonValue jsonValue;
        NJson::TJsonReaderConfig jsonConfig;
        NJson::ReadJsonTree(Base64DecodeUneven(stateParam), &jsonConfig, &jsonValue);
        const NJson::TJsonValue* jsonStateContainer = nullptr;
        jsonValue.GetValuePointer("container", &jsonStateContainer);
        TString stateContainer = jsonStateContainer->GetStringRobust();
        NJson::ReadJsonTree(Base64Decode(stateContainer), &jsonConfig, &jsonValue);
        const NJson::TJsonValue* jsonState = nullptr;
        jsonValue.GetValuePointer("state", &jsonState);
        const TString expectedState = jsonState->GetStringRobust();

        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        TActorId createSessionActor = handle->Sender;
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        incomingResponse = incomingResponse->Duplicate(outgoingRequestEv->Request);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, workerName1);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/context?state=" + expectedState);
        const NActors::TActorId restoreContextHandler = runtime.Register(new TRestoreContextHandler(edge, &contextStorageFirstHost));
        incomingRequestToOidcHost1 = new NHttp::THttpIncomingRequest();
        incomingRequestToOidcHost1->Endpoint->Secure = true;
        incomingRequestToOidcHost1->Endpoint->WorkerName = workerName1;
        request.clear();
        request << "GET /context?state=" << expectedState << " HTTP/1.1\r\n"
                   "Host: " << workerName1 << "\r\n";
        EatWholeString(incomingRequestToOidcHost1, redirectStrategy.CreateRequest(request));
        runtime.Send(new IEventHandle(restoreContextHandler, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequestToOidcHost1)));
        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        const TStringBuf& outgoingResponseBody = outgoingResponseEv->Response->Body;
        redirectStrategy.CheckRequestedAddress(outgoingResponseBody, hostProxy, protectedPage);
        const TString expectedAjaxRequest = (redirectStrategy.IsAjaxRequest() ? "true" : "false");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseBody, "\"is_ajax_request\":" + expectedAjaxRequest);

        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(outgoingResponseBody.length()) + "\r\n\r\n" + outgoingResponseBody);
        runtime.Send(new IEventHandle(createSessionActor, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(incomingResponse->GetRequest(), incomingResponse)));

        // State is OK. Context was restored
        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        const TStringBuf& outgoingRequestBody = outgoingRequestEv->Request->Body;
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestBody, "code=code_template");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestBody, "grant_type=authorization_code");

        const TString authorizationServerResponse = R"___({"access_token":"access_token_value","token_type":"bearer","expires_in":43199,"scope":"openid","id_token":"id_token_value"})___";
        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(authorizationServerResponse.length()) + "\r\n\r\n" + authorizationServerResponse);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Message, "Cookie set");
        const NHttp::THeaders protectedPageHeaders(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(protectedPageHeaders.Has("Location"));
        redirectStrategy.CheckLocationHeader(protectedPageHeaders.Get("Location"), hostProxy, protectedPage);
        UNIT_ASSERT(protectedPageHeaders.Has("Set-Cookie"));
        TStringBuf sessionCookie = protectedPageHeaders.Get("Set-Cookie");
        UNIT_ASSERT_STRINGS_EQUAL(sessionCookie, "yc_session=allowed_session_cookie; SameSite=None");

        // Get session cookie. Try request original resource
        incomingRequestToOidcHost1 = new NHttp::THttpIncomingRequest();
        incomingRequestToOidcHost1->Endpoint->Secure = true;
        incomingRequestToOidcHost1->Endpoint->WorkerName = workerName1;
        request.clear();
        TString redirectUrl = redirectStrategy.GetUrlFromLocationHeader(protectedPageHeaders.Get("Location"));
        request << "GET " << redirectUrl << " HTTP/1.1\r\n";
        request << "Host: " + hostProxy + "\r\n";
        request << "Cookie: " << sessionCookie.NextTok(';') << "\r\n";
        EatWholeString(incomingRequestToOidcHost1, redirectStrategy.CreateRequest(request));
        runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequestToOidcHost1)));

        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, allowedProxyHost);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/counters");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Authorization: Bearer protected_page_iam_token");
        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nthis\r\n4\r\n is \r\n5\r\ntest.\r\n0\r\n\r\n");
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "200");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Body, "this is test.");
    }

    Y_UNIT_TEST(FullAuthorizationFlowWithStoreContextOnOtherHost) {
        TRedirectStrategy redirectStrategy;
        FullAuthorizationFlowWithStoreContextOnOtherHost(redirectStrategy);
    }

    Y_UNIT_TEST(FullAuthorizationFlowAjaxWithStoreContextOnOtherHost) {
        TAjaxRedirectStrategy redirectStrategy;
        FullAuthorizationFlowWithStoreContextOnOtherHost(redirectStrategy);
    }

    Y_UNIT_TEST(RestoreContextFromHostOtherHostUnavailable) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef",
            .StoreContextOnHost = true
        };

        TContextStorage contextStorage;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId sessionCreatorHandler = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorage));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.IsOpenIdScopeMissed = true;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const TString expectedState = "test_state";
        const TString workerName2 = "oidc-02.host.net";
        TContext context(expectedState, "/requested/page", false);
        const TString stateParam = context.CreateStateContainer(settings.ClientSecret, workerName2);
        TStringBuilder request;
        request << "GET /callback?code=code_template&state=" << stateParam << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        const TString workerName1 = "oidc-01.host.net";
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        incomingRequest->Endpoint->Secure = true;
        incomingRequest->Endpoint->WorkerName = workerName1;
        EatWholeString(incomingRequest, request);
        runtime.Send(new IEventHandle(sessionCreatorHandler, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        // Context was not found on host1. Try restore context on host2
        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        TActorId createSessionActor = handle->Sender;
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        incomingResponse = incomingResponse->Duplicate(outgoingRequestEv->Request);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, workerName2);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/context?state=" + expectedState);

        // host2 is unavailable
        EatWholeString(incomingResponse, "HTTP/1.1 503 Service Unavailable\r\n"
                                                    "Connection: close\r\n"
                                                    "Transfer-Encoding: chunked\r\n\r\n7\r\nService\r\n12\r\n Unavailable\r\n0\r\n\r\n");
        runtime.Send(new IEventHandle(createSessionActor, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(incomingResponse->GetRequest(), incomingResponse)));

        // Can not restore context.
        auto outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "400");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Body, "Unknown error has occurred. Please open the page again");
    }

    Y_UNIT_TEST(RestoreContextFromHostCanNotFindContextOnOtherHost) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef",
            .StoreContextOnHost = true
        };

        TContextStorage contextStorageHost1;
        TContextStorage contextStorageHost2;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId sessionCreatorHandler = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorageHost1));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.IsOpenIdScopeMissed = true;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const TString expectedState = "test_state";
        const TString workerName2 = "oidc-02.host.net";
        TContext context(expectedState, "/requested/page", false);
        const TString stateParam = context.CreateStateContainer(settings.ClientSecret, workerName2);
        TStringBuilder request;
        request << "GET /callback?code=code_template&state=" << stateParam << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        const TString workerName1 = "oidc-01.host.net";
        NHttp::THttpIncomingRequestPtr incomingRequestToOidcHost1 = new NHttp::THttpIncomingRequest();
        incomingRequestToOidcHost1->Endpoint->Secure = true;
        incomingRequestToOidcHost1->Endpoint->WorkerName = workerName1;
        EatWholeString(incomingRequestToOidcHost1, request);
        runtime.Send(new IEventHandle(sessionCreatorHandler, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequestToOidcHost1)));

        // Context was not found on host1. Try restore context on host2
        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        TActorId createSessionActor = handle->Sender;
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        incomingResponse = incomingResponse->Duplicate(outgoingRequestEv->Request);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, workerName2);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/context?state=" + expectedState);

        // Request to host2 to restore context
        const NActors::TActorId restoreContextHandler = runtime.Register(new TRestoreContextHandler(edge, &contextStorageHost2));
        NHttp::THttpIncomingRequestPtr incomingRequestToOidcHost2 = new NHttp::THttpIncomingRequest();
        incomingRequestToOidcHost2->Endpoint->Secure = true;
        incomingRequestToOidcHost2->Endpoint->WorkerName = workerName2;
        request.clear();
        request << "GET /context?state=" << expectedState << " HTTP/1.1\r\n"
                   "Host: " << workerName2 << "\r\n";
        EatWholeString(incomingRequestToOidcHost2, request);
        runtime.Send(new IEventHandle(restoreContextHandler, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequestToOidcHost2)));
        NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "401");

        // host2 is unavailable
        EatWholeString(incomingResponse, "HTTP/1.1 401 Unauthorized\r\n"
                                                    "Connection: close\r\n"
                                                    "Transfer-Encoding: chunked\r\n\r\n12\r\nUnauthorized\r\n0\r\n\r\n");
        runtime.Send(new IEventHandle(createSessionActor, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(incomingResponse->GetRequest(), incomingResponse)));

        // Can not restore context.
        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "400");
        UNIT_ASSERT_STRING_CONTAINS(outgoingResponseEv->Response->Body, "Unknown error has occurred. Please open the page again");
    }

    Y_UNIT_TEST(RestoreContextFromHostOtherHostReturnExpiredState) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef",
            .StoreContextOnHost = true
        };

        TContextStorage contextStorageHost1;
        TContextStorage contextStorageHost2;

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId sessionCreatorHandler = runtime.Register(new TSessionCreateHandler(edge, settings, &contextStorageHost1));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.IsOpenIdScopeMissed = true;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const TString expectedState = "test_state";
        const TString workerName2 = "oidc-02.host.net";
        TContext context(expectedState, "/requested/page", false);
        const TString stateParam = context.CreateStateContainer(settings.ClientSecret, workerName2);
        contextStorageHost2.Write(context);
        TStringBuilder request;
        request << "GET /callback?code=code_template&state=" << stateParam << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        const TString workerName1 = "oidc-01.host.net";
        NHttp::THttpIncomingRequestPtr incomingRequestToOidcHost1 = new NHttp::THttpIncomingRequest();
        incomingRequestToOidcHost1->Endpoint->Secure = true;
        incomingRequestToOidcHost1->Endpoint->WorkerName = workerName1;
        EatWholeString(incomingRequestToOidcHost1, request);
        runtime.Send(new IEventHandle(sessionCreatorHandler, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequestToOidcHost1)));

        // Context was not found on host1. Try restore context on host2
        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        TActorId createSessionActor = handle->Sender;
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        incomingResponse = incomingResponse->Duplicate(outgoingRequestEv->Request);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, workerName2);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/context?state=" + expectedState);

        // Request to host2 to restore context
        const NActors::TActorId restoreContextHandler = runtime.Register(new TRestoreContextHandler(edge, &contextStorageHost2));
        NHttp::THttpIncomingRequestPtr incomingRequestToOidcHost2 = new NHttp::THttpIncomingRequest();
        incomingRequestToOidcHost2->Endpoint->Secure = true;
        incomingRequestToOidcHost2->Endpoint->WorkerName = workerName2;
        request.clear();
        request << "GET /context?state=" << expectedState << " HTTP/1.1\r\n"
                   "Host: " << workerName2 << "\r\n";
        EatWholeString(incomingRequestToOidcHost2, request);
        runtime.Send(new IEventHandle(restoreContextHandler, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequestToOidcHost2)));
        NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "200");

        const TString bodyWithExpiredState = "{\"requested_address\":\"/requested/page\","
                                              "\"is_ajax_request\":false,"
                                              "\"expiration_time\":100}";

        // host2 return expired state
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(bodyWithExpiredState.length()) + "\r\n\r\n" + bodyWithExpiredState);
        runtime.Send(new IEventHandle(createSessionActor, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(incomingResponse->GetRequest(), incomingResponse)));

        // State is expired. Try request protected resource again
        outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(headers.Has("Location"));
        UNIT_ASSERT_STRINGS_EQUAL(headers.Get("Location"), "/requested/page");
    }
}

Y_UNIT_TEST_SUITE(StorageContextTests) {
    Y_UNIT_TEST(ContextRemoveAfterTimeExpire) {
        TContextStorage storage;
        storage.SetTtl(TDuration::Seconds(5));
        TContext context("state1", "/protected/page");
        storage.Write(context);
        auto storageRecord = storage.Find("state1");
        UNIT_ASSERT(storageRecord.first);

        storage.Refresh(TInstant::Now() + TDuration::Minutes(15)); // context should be removed after 10m 5s
        storageRecord = storage.Find("state1");
        UNIT_ASSERT(!storageRecord.first);
    }

    Y_UNIT_TEST(ContextRemoveAfterTimeExpire3Records) {
        TContextStorage storage;
        storage.SetTtl(TDuration::Seconds(10));
        TContext context1("state1", "/protected/page");
        storage.Write(context1);
        Sleep(TDuration::Seconds(2));
        TContext context2("state2", "/protected/page");
        storage.Write(context2);
        Sleep(TDuration::Seconds(2));
        TContext context3("state3", "/protected/page");
        storage.Write(context3);
        Sleep(TDuration::Seconds(2));

        auto storageRecord = storage.Find("state1");
        UNIT_ASSERT(storageRecord.first);
        storageRecord = storage.Find("state2");
        UNIT_ASSERT(storageRecord.first);
        storageRecord = storage.Find("state3");
        UNIT_ASSERT(storageRecord.first);

        storage.Refresh(TInstant::Now() + TDuration::Minutes(10) + TDuration::Seconds(5));
        storageRecord = storage.Find("state1");
        UNIT_ASSERT(!storageRecord.first);
        storageRecord = storage.Find("state2");
        UNIT_ASSERT(storageRecord.first);
        storageRecord = storage.Find("state3");
        UNIT_ASSERT(storageRecord.first);

        storage.Refresh(TInstant::Now() + TDuration::Minutes(10) + TDuration::Seconds(7));
        storageRecord = storage.Find("state1");
        UNIT_ASSERT(!storageRecord.first);
        storageRecord = storage.Find("state2");
        UNIT_ASSERT(!storageRecord.first);
        storageRecord = storage.Find("state3");
        UNIT_ASSERT(storageRecord.first);

        storage.Refresh(TInstant::Now() + TDuration::Minutes(11));
        storageRecord = storage.Find("state1");
        UNIT_ASSERT(!storageRecord.first);
        storageRecord = storage.Find("state2");
        UNIT_ASSERT(!storageRecord.first);
        storageRecord = storage.Find("state3");
        UNIT_ASSERT(!storageRecord.first);
    }
}
