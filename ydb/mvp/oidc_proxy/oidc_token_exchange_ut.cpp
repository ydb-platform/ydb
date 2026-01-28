#include "oidc_protected_page_handler.h"
#include "oidc_session_create_handler.h"
#include "oidc_impersonate_start_page_nebius.h"
#include "oidc_impersonate_stop_page_nebius.h"
#include "oidc_settings.h"
#include "openid_connect.h"
#include "context.h"
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/public/api/client/nc_private/iam/v1/token_exchange_service.grpc.pb.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/map.h>

using namespace NMVP::NOIDC;
using namespace NActors;

namespace {
class TFakeNebiusTokenExchangeServiceStrict : public nebius::iam::v1::TokenExchangeService::Service {
public:
    TFakeNebiusTokenExchangeServiceStrict(const TString& expectedSaId, const TString& expectedJwtToken, const TString& iamToken)
        : ExpectedSaId(expectedSaId)
        , ExpectedJwtToken(expectedJwtToken)
        , IamToken(iamToken)
    {}

    grpc::Status Exchange(grpc::ServerContext* , const nebius::iam::v1::ExchangeTokenRequest* request, nebius::iam::v1::CreateTokenResponse* response) override {
        if (request->subject_token_type() == "urn:nebius:params:oauth:token-type:subject_identifier") {
            if (request->subject_token() != ExpectedSaId) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "service account id mismatch");
            }
            if (request->actor_token() != ExpectedJwtToken) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "jwt token mismatch");
            }
        } else if (request->subject_token_type() == "urn:ietf:params:oauth:token-type:jwt") {
            if (request->subject_token() != ExpectedJwtToken) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "jwt token mismatch");
            }
        } else {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unknown subject_token_type");
        }

        response->set_access_token(IamToken);
        return grpc::Status::OK;
    }

private:
    TString ExpectedSaId;
    TString ExpectedJwtToken;
    TString IamToken;
};

// new: flat context + common runner (IamToken and AccessServiceType are fixed inside runner)
struct TFederatedTestContext {
    NMvp::TJwtInfo_EAuthMethod JwtAuthMethod = NMvp::TJwtInfo::static_creds;
    TString ExpectedSaId;
    TString ExpectedJwtToken;
    TString SaId;
    TString JwtToken;
    bool ExpectAuthHeader = true;
};

void RunTokenatorIntegrationTest(TFederatedTestContext& ctx) {
    TPortManager tp;
    ui16 tokenExchangePort = tp.GetPort(9000);
    ui16 sessionServicePort = tp.GetPort(8655);

    NMvp::TTokensConfig tokensConfig;
    tokensConfig.set_accessservicetype(NMvp::nebius_v1);
    auto jwtList = tokensConfig.MutableJwtInfo();
    auto jwt = jwtList->Add();
    jwt->SetAuthMethod(ctx.JwtAuthMethod);
    jwt->SetName("nebiusJwt");
    jwt->SetAccountId(ctx.SaId);
    jwt->SetToken(ctx.JwtToken);

    const TString fixedIamToken = "iam_from_tokenator";

    std::unique_ptr<grpc::Server> tokenExchangeServer;
    std::unique_ptr<TFakeNebiusTokenExchangeServiceStrict> localMock;
    if (!ctx.ExpectedSaId.empty() && !ctx.ExpectedJwtToken.empty()) {
        localMock = std::make_unique<TFakeNebiusTokenExchangeServiceStrict>(ctx.ExpectedSaId, ctx.ExpectedJwtToken, fixedIamToken);
        TString endpoint = TStringBuilder() << "localhost:" << tokenExchangePort;
        tokensConfig.MutableJwtInfo(0)->SetEndpoint(endpoint);
        grpc::ServerBuilder teBuilder;
        teBuilder.AddListeningPort(endpoint, grpc::InsecureServerCredentials()).RegisterService(localMock.get());
        tokenExchangeServer = teBuilder.BuildAndStart();
    }

    TMvpTestRuntime runtime(1, true);
    runtime.Initialize();

    const TString allowedProxyHost {"ydb.viewer.page"};
    const NActors::TActorId edge = runtime.AllocateEdgeActor();

    NMVP::TMvpTokenator* tokenator = NMVP::TMvpTokenator::CreateTokenator(tokensConfig, edge);
    runtime.Register(tokenator);
    runtime.GetActorSystem(0)->AppData<NMVP::TMVPAppData>()->Tokenator = tokenator;
    Sleep(TDuration::Seconds(1));

    TOpenIdConnectSettings settings {
        .SessionServiceEndpoint = "localhost:" + ToString(sessionServicePort),
        .SessionServiceTokenName = "nebiusJwt",
        .AuthorizationServerAddress = "http://auth.test.net",
        .AllowedProxyHosts = {allowedProxyHost},
        .AccessServiceType = static_cast<NMvp::EAccessServiceType>(tokensConfig.accessservicetype())
    };

    const NActors::TActorId target = runtime.Register(new TProtectedPageHandler(edge, settings));

    NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
    EatWholeString(incomingRequest, "GET /" + allowedProxyHost + "/counters HTTP/1.1\r\n"
                "Host: oidcproxy.net\r\n"
                "Cookie: yc_session=allowed_session_cookie;" + CreateNameSessionCookie(settings.ClientId) + "=" + Base64Encode("session_cookie") + "\r\n\r\n");

    runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(incomingRequest)));
    TAutoPtr<IEventHandle> handle;

    auto outgoingRequestEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);

    UNIT_ASSERT_STRINGS_EQUAL(outgoingRequestEv->Request->Host, "auth.test.net");
    const bool hasAuth = outgoingRequestEv->Request->Headers.find(TString("Authorization: Bearer ") + fixedIamToken) != TString::npos;
    UNIT_ASSERT_VALUES_EQUAL(hasAuth, ctx.ExpectAuthHeader);
}

} // namespace

Y_UNIT_TEST_SUITE(MvpTokenator) {
    Y_UNIT_TEST(FederatedCredsSuccess) {
        TFederatedTestContext ctx;
        ctx.ExpectedSaId = "serviceaccount-expected";
        ctx.ExpectedJwtToken = "short_jwt_token";
        ctx.JwtAuthMethod = NMvp::TJwtInfo::federated_creds;
        ctx.SaId = "serviceaccount-expected";
        ctx.JwtToken = "short_jwt_token";

        ctx.ExpectAuthHeader = true;
        RunTokenatorIntegrationTest(ctx);
    }

    Y_UNIT_TEST(FederatedCredsSaIdMismatch) {
        TFederatedTestContext ctx;
        ctx.ExpectedSaId = "serviceaccount-expected";
        ctx.ExpectedJwtToken = "short_jwt_token";
        ctx.JwtAuthMethod = NMvp::TJwtInfo::federated_creds;
        ctx.SaId = "wrong-serviceaccount";
        ctx.JwtToken = "short_jwt_token";
        ctx.ExpectAuthHeader = false;
        RunTokenatorIntegrationTest(ctx);
    }

    Y_UNIT_TEST(FederatedCredsJwtTokenMismatch) {
        TFederatedTestContext ctx;
        ctx.ExpectedSaId = "serviceaccount-expected";
        ctx.ExpectedJwtToken = "short_jwt_token";
        ctx.JwtAuthMethod = NMvp::TJwtInfo::federated_creds;
        ctx.SaId = "serviceaccount-expected";
        ctx.JwtToken = "wrong_jwt_token";
        ctx.ExpectAuthHeader = false;
        RunTokenatorIntegrationTest(ctx);
    }
}
