#include "mvp_tokens.h"
#include "merger.h"
#include "reducer.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/map.h>
#include <ydb/library/testlib/service_mocks/session_service_mock.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include "mvp_test_runtime.h"

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
    Y_UNIT_TEST(TokenatorGetMetadataTokenGood) {
        TPortManager tp;
        TMvpTestRuntime runtime;
        runtime.Initialize();

        NMvp::TTokensConfig tokensConfig;
        auto metadataTokenList = tokensConfig.MutableMetadataTokenInfo();
        auto metadataTokenInfo = metadataTokenList->Add();
        const TString tokenName = "metadataTokenName";
        metadataTokenInfo->SetName(tokenName);

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        NMVP::TMvpTokenator* tokenator = NMVP::TMvpTokenator::CreateTokenator(tokensConfig, edge);
        runtime.Register(tokenator);

        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, "169.254.169.254");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/computeMetadata/v1/instance/service-accounts/default/token");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Metadata-Flavor: Google");


        const TString authorizationServerResponse = R"___({"access_token":"test.iam.token","expires_in":41133,"token_type":"Bearer"})___";
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(authorizationServerResponse.length()) + "\r\n\r\n" + authorizationServerResponse);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        TString token = tokenator->GetToken(tokenName);
        UNIT_ASSERT(!token.empty());
        UNIT_ASSERT_STRINGS_EQUAL(token, "Bearer test.iam.token");
    }

    Y_UNIT_TEST(TokenatorRefreshMetadataTokenGood) {
        TPortManager tp;
        TMvpTestRuntime runtime;
        runtime.Initialize();

        NMvp::TTokensConfig tokensConfig;
        auto metadataTokenList = tokensConfig.MutableMetadataTokenInfo();
        auto metadataTokenInfo = metadataTokenList->Add();
        const TString tokenName = "metadataTokenName";
        metadataTokenInfo->SetName(tokenName);

        const NActors::TActorId edge = runtime.AllocateEdgeActor();
        NMVP::TMvpTokenator* tokenator = NMVP::TMvpTokenator::CreateTokenator(tokensConfig, edge);
        auto target = runtime.Register(tokenator);

        TAutoPtr<IEventHandle> handle;
        auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, "169.254.169.254");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/computeMetadata/v1/instance/service-accounts/default/token");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Metadata-Flavor: Google");


        const TString authorizationServerResponse = R"___({"access_token":"test.iam.token","expires_in":3,"token_type":"Bearer"})___";
        NHttp::THttpIncomingResponsePtr incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(authorizationServerResponse.length()) + "\r\n\r\n" + authorizationServerResponse);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        TString token = tokenator->GetToken(tokenName);
        UNIT_ASSERT(!token.empty());
        UNIT_ASSERT_STRINGS_EQUAL(token, "Bearer test.iam.token");

        Sleep(TDuration::Seconds(5));

        runtime.Send(new IEventHandle(target, edge, new NActors::TEvents::TEvWakeup()));

        outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, "169.254.169.254");
        UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->URL, "/computeMetadata/v1/instance/service-accounts/default/token");
        UNIT_ASSERT_STRING_CONTAINS(outgoingRequestEv->Request->Headers, "Metadata-Flavor: Google");


        const TString refreshedAuthorizationServerResponse = R"___({"access_token":"refreshed.test.iam.token","expires_in":3600,"token_type":"Bearer"})___";
        incomingResponse = new NHttp::THttpIncomingResponse(outgoingRequestEv->Request);
        EatWholeString(incomingResponse, "HTTP/1.1 200 OK\r\n"
                                                    "Connection: close\r\n"
                                                    "Content-Type: application/json; charset=utf-8\r\n"
                                                    "Content-Length: " + ToString(refreshedAuthorizationServerResponse.length()) + "\r\n\r\n" + refreshedAuthorizationServerResponse);
        runtime.Send(new IEventHandle(handle->Sender, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoingRequestEv->Request, incomingResponse)));

        token = tokenator->GetToken(tokenName);
        UNIT_ASSERT(!token.empty());
        UNIT_ASSERT_STRINGS_EQUAL(token, "Bearer refreshed.test.iam.token");
    }
}
