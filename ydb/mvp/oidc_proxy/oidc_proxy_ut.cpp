#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/map.h>
#include <ydb/library/testlib/service_mocks/session_service_mock.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include "oidc_protected_page.h"
#include "oidc_protected_page_handler.h"
#include "oidc_session_create_handler.h"

namespace {

template <typename HttpType>
void EatWholeString(TIntrusivePtr<HttpType>& request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

}

Y_UNIT_TEST_SUITE(Mvp) {
    void OpenIdConnectRequestWithIamTokenTest(NMvp::EAccessServiceType profile) {
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

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new NMVP::TProtectedPageHandler(edge, settings));

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

    Y_UNIT_TEST(OpenIdConnectRequestWithIamTokenYandex) {
        OpenIdConnectRequestWithIamTokenTest(NMvp::yandex_v2);
    }

    Y_UNIT_TEST(OpenIdConnectRequestWithIamTokenNebius) {
        OpenIdConnectRequestWithIamTokenTest(NMvp::nebius_v1);
    }

    void OpenIdConnectNonAuthorizeRequestWithOptionMethodTest(NMvp::EAccessServiceType profile) {
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

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new NMVP::TProtectedPageHandler(edge, settings));

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

    Y_UNIT_TEST(OpenIdConnectNonAuthorizeRequestWithOptionMethodYandex) {
        OpenIdConnectNonAuthorizeRequestWithOptionMethodTest(NMvp::yandex_v2);
    }

    Y_UNIT_TEST(OpenIdConnectNonAuthorizeRequestWithOptionMethodNebius) {
        OpenIdConnectNonAuthorizeRequestWithOptionMethodTest(NMvp::nebius_v1);
    }

    void OpenIdConnectSessionServiceCheckValidCookieTest(NMvp::EAccessServiceType profile) {
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

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new NMVP::TProtectedPageHandler(edge, settings));

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

    Y_UNIT_TEST(OpenIdConnectSessionServiceCheckValidCookieYandex) {
        OpenIdConnectNonAuthorizeRequestWithOptionMethodTest(NMvp::yandex_v2);
    }

    Y_UNIT_TEST(OpenIdConnectSessionServiceCheckValidCookieNebius) {
        OpenIdConnectNonAuthorizeRequestWithOptionMethodTest(NMvp::nebius_v1);
    }

    Y_UNIT_TEST(OpenIdConnectProxyOnHttpsHost) {
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

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new NMVP::TProtectedPageHandler(edge, settings));

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
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Body, "this is test");
    }


    Y_UNIT_TEST(OpenIdConnectExchangeNebius) {
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

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new NMVP::TProtectedPageHandler(edge, settings));

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

    Y_UNIT_TEST(OpenIdConnectSessionServiceCheckAuthorizationFail) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        const TString allowedProxyHost {"ydb.viewer.page"};

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AllowedProxyHosts = {allowedProxyHost}
        };

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new NMVP::TProtectedPageHandler(edge, settings));

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

    void OidcFullAuthorizationFlow(TRedirectStrategyBase& redirectStrategy) {
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
            .AllowedProxyHosts = {allowedProxyHost}
        };

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new NMVP::TProtectedPageHandler(edge, settings));

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
        UNIT_ASSERT(headers.Has("Set-Cookie"));
        TStringBuf setCookie = headers.Get("Set-Cookie");
        UNIT_ASSERT_STRING_CONTAINS(setCookie, CreateNameYdbOidcCookie(settings.ClientSecret, state));
        redirectStrategy.CheckSpecificHeaders(headers);

        const NActors::TActorId sessionCreator = runtime.Register(new NMVP::TSessionCreateHandler(edge, settings));
        incomingRequest = new NHttp::THttpIncomingRequest();
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << state << " HTTP/1.1\r\n";
        request << "Host: " + hostProxy + "\r\n";
        request << "Cookie: " << setCookie.NextTok(";") << "\r\n";
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

    Y_UNIT_TEST(OpenIdConnectFullAuthorizationFlow) {
        TRedirectStrategy redirectStrategy;
        OidcFullAuthorizationFlow(redirectStrategy);
    }

    Y_UNIT_TEST(OpenIdConnectFullAuthorizationFlowAjax) {
        TAjaxRedirectStrategy redirectStrategy;
        OidcFullAuthorizationFlow(redirectStrategy);
    }

    void OidcWrongStateAuthorizationFlow(TRedirectStrategyBase& redirectStrategy) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .ClientId = "client_id",
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "0123456789abcdef"
        };

        const NActors::TActorId edge = runtime.AllocateEdgeActor();

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedAccessTokens.insert("valid_access_token");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const NActors::TActorId sessionCreator = runtime.Register(new NMVP::TSessionCreateHandler(edge, settings));
        const TString state = "test_state";
        const TString wrongState = "wrong_state";
        const TString hostProxy = "oidcproxy.net";
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << state << " HTTP/1.1\r\n";
        request << "Host: " + hostProxy + "\r\n";
        request << "Cookie: " << CreateNameYdbOidcCookie(settings.ClientSecret, wrongState) << "=" << GenerateCookie(wrongState, "/requested/page", settings.ClientSecret, redirectStrategy.IsAjaxRequest()) << "\r\n";
        NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
        EatWholeString(incomingRequest, redirectStrategy.CreateRequest(request));
        incomingRequest->Endpoint->Secure = true;
        runtime.Send(new IEventHandle(sessionCreator, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));

        TAutoPtr<IEventHandle> handle;
        NHttp::TEvHttpProxy::TEvHttpOutgoingResponse* outgoingResponseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingResponseEv->Response->Status, "302");
        const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(headers.Has("Location"));
        TString location = TString(headers.Get("Location"));
        UNIT_ASSERT_STRING_CONTAINS(location, "https://auth.test.net/oauth/authorize");
        UNIT_ASSERT_STRING_CONTAINS(location, "response_type=code");
        UNIT_ASSERT_STRING_CONTAINS(location, "scope=openid");
        UNIT_ASSERT_STRING_CONTAINS(location, "client_id=" + settings.ClientId);
        UNIT_ASSERT_STRING_CONTAINS(location, "redirect_uri=https://" + hostProxy + "/auth/callback");
    }

    Y_UNIT_TEST(OpenIdConnectotWrongStateAuthorizationFlow) {
        TRedirectStrategy redirectStrategy;
        OidcWrongStateAuthorizationFlow(redirectStrategy);
    }

    Y_UNIT_TEST(OpenIdConnectotWrongStateAuthorizationFlowAjax) {
        TAjaxRedirectStrategy redirectStrategy;
        OidcWrongStateAuthorizationFlow(redirectStrategy);
    }

    Y_UNIT_TEST(OpenIdConnectSessionServiceCreateAuthorizationFail) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef"
        };

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId sessionCreator = runtime.Register(new NMVP::TSessionCreateHandler(edge, settings));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.IsTokenAllowed = false;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const TString state = "test_state";
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << state << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        const TString oidcCookie = CreateNameYdbOidcCookie(settings.ClientSecret, state);
        request << "Cookie: " << oidcCookie << "=" << GenerateCookie(state, "/requested/page", settings.ClientSecret, false) << "\r\n\r\n";
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
        const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(headers.Has("Set-Cookie"));
        UNIT_ASSERT_STRINGS_EQUAL(headers.Get("Set-Cookie"), oidcCookie + "=; Path=/auth/callback; Max-Age=0");
    }

    void SessionServiceCreateAccessTokenInvalid(TRedirectStrategyBase& redirectStrategy) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .ClientId = "client_id",
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef"
        };

        const NActors::TActorId edge = runtime.AllocateEdgeActor();

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedAccessTokens.insert("valid_access_token");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const NActors::TActorId sessionCreator = runtime.Register(new NMVP::TSessionCreateHandler(edge, settings));
        const TString state = "test_state";
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << state << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        request << "Cookie: " << CreateNameYdbOidcCookie(settings.ClientSecret, state) << "=" << GenerateCookie(state, "/requested/page", settings.ClientSecret, redirectStrategy.IsAjaxRequest()) << "\r\n";
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
        redirectStrategy.CheckRedirectStatus(outgoingResponseEv);
        TString location = redirectStrategy.GetRedirectUrl(outgoingResponseEv);
        UNIT_ASSERT_STRING_CONTAINS(location, "https://auth.test.net/oauth/authorize");
        UNIT_ASSERT_STRING_CONTAINS(location, "response_type=code");
        UNIT_ASSERT_STRING_CONTAINS(location, "scope=openid");
        UNIT_ASSERT_STRING_CONTAINS(location, "client_id=" + settings.ClientId);
        UNIT_ASSERT_STRING_CONTAINS(location, "redirect_uri=https://oidcproxy.net/auth/callback");

        NHttp::TUrlParameters urlParameters(location);
        const TString newState = urlParameters["state"];

        NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(headers.Has("Set-Cookie"));
        const TStringBuf setCookie = headers.Get("Set-Cookie");
        UNIT_ASSERT_STRING_CONTAINS(setCookie, CreateNameYdbOidcCookie(settings.ClientSecret, newState));
        redirectStrategy.CheckSpecificHeaders(headers);
    }

    Y_UNIT_TEST(OpenIdConnectSessionServiceCreateAccessTokenInvalid) {
        TRedirectStrategy redirectStrategy;
        SessionServiceCreateAccessTokenInvalid(redirectStrategy);
    }

    Y_UNIT_TEST(OpenIdConnectSessionServiceCreateAccessTokenInvalidAjax) {
        TAjaxRedirectStrategy redirectStrategy;
        SessionServiceCreateAccessTokenInvalid(redirectStrategy);
    }

    Y_UNIT_TEST(OpenIdConnectSessionServiceCreateOpenIdScopeMissed) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef"
        };

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId sessionCreator = runtime.Register(new NMVP::TSessionCreateHandler(edge, settings));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.IsOpenIdScopeMissed = true;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const TString state = "test_state";
        TStringBuilder request;
        request << "GET /callback?code=code_template&state=" << state << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        const TString oidcCookie = CreateNameYdbOidcCookie(settings.ClientSecret, state);
        request << "Cookie: " << oidcCookie << "=" << GenerateCookie(state, "/requested/page", settings.ClientSecret, false) << "\r\n\r\n";
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
        const NHttp::THeaders headers(outgoingResponseEv->Response->Headers);
        UNIT_ASSERT(headers.Has("Set-Cookie"));
        UNIT_ASSERT_STRINGS_EQUAL(headers.Get("Set-Cookie"), oidcCookie + "=; Path=/auth/callback; Max-Age=0");
    }

    void SessionServiceCreateWithSeveralCookies(TRedirectStrategyBase& redirectStrategy) {
        TPortManager tp;
        ui16 sessionServicePort = tp.GetPort(8655);
        TMvpTestRuntime runtime;
        runtime.Initialize();

        TOpenIdConnectSettings settings {
            .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
            .AuthorizationServerAddress = "https://auth.test.net",
            .ClientSecret = "123456789abcdef"
        };

        const NActors::TActorId edge = runtime.AllocateEdgeActor();

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedAccessTokens.insert("valid_access_token");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const NActors::TActorId sessionCreator = runtime.Register(new NMVP::TSessionCreateHandler(edge, settings));
        TStringBuf firstRequestState = "first_request_state";
        TStringBuf secondRequestState = "second_request_state";
        TString firstCookie {CreateNameYdbOidcCookie(settings.ClientSecret, firstRequestState) + "=" + GenerateCookie(firstRequestState, "/requested/page", settings.ClientSecret, redirectStrategy.IsAjaxRequest())};
        TString secondCookie {CreateNameYdbOidcCookie(settings.ClientSecret, secondRequestState) + "=" + GenerateCookie(secondRequestState, "/requested/page", settings.ClientSecret, redirectStrategy.IsAjaxRequest())};
        TStringBuilder request;
        request << "GET /auth/callback?code=code_template&state=" << firstRequestState << " HTTP/1.1\r\n";
        request << "Host: oidcproxy.net\r\n";
        request << "Cookie: " << firstCookie << "; " << secondCookie << "\r\n";
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

    Y_UNIT_TEST(OpenIdConnectSessionServiceCreateWithSeveralCookies) {
        TRedirectStrategy redirectStrategy;
        SessionServiceCreateWithSeveralCookies(redirectStrategy);
    }

    Y_UNIT_TEST(OpenIdConnectSessionServiceCreateWithSeveralCookiesAjax) {
        TAjaxRedirectStrategy redirectStrategy;
        SessionServiceCreateWithSeveralCookies(redirectStrategy);
    }

    Y_UNIT_TEST(OpenIdConnectAllowedHostsList) {
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

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        const NActors::TActorId target = runtime.Register(new NMVP::TProtectedPageHandler(edge, settings));

        TSessionServiceMock sessionServiceMock;
        sessionServiceMock.AllowedCookies.second = "allowed_session_cookie";
        sessionServiceMock.AllowedAccessTokens.insert("access_token_value");
        grpc::ServerBuilder builder;
        builder.AddListeningPort(settings.SessionServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&sessionServiceMock);
        std::unique_ptr<grpc::Server> sessionServer(builder.BuildAndStart());

        const TString hostProxy = "oidcproxy.net";
        const TString protectedPage = "/counters";

        auto checkAllowedHostList = [&] (const TString& requestedHost, const TString& expectedStatus) {
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
        };

        for (const TString& allowedHost : allowedProxyHosts) {
            checkAllowedHostList(allowedHost, "302");
        }

        for (const TString& forbiddenHost : forbiddenProxyHosts) {
            checkAllowedHostList(forbiddenHost, "404");
        }
    }
}
