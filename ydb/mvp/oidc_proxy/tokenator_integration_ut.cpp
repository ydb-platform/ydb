#include "oidc_protected_page_handler.h"
#include "oidc_settings.h"
#include "openid_connect.h"
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/public/api/client/nc_private/iam/v1/token_exchange_service.grpc.pb.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NMVP::NOIDC;
using namespace NActors;

namespace {
class TTokenExchangeServiceMock : public nebius::iam::v1::TokenExchangeService::Service {
public:
    TTokenExchangeServiceMock(const TString& expectedSaId, const TString& expectedJwtToken, const TString& iamToken)
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
    TString ExpectedSaId;
    TString ExpectedJwtToken;
    TString SaId;
    TString FederatedJwtTokenPath;
    bool OmitExplicitCredsTypeAndTokenType = false;
    bool ExpectAuthHeader = true;
};

void RunTokenatorIntegrationTest(TFederatedTestContext& ctx) {
    TPortManager tp;
    ui16 tokenExchangePort = tp.GetPort(9000);
    ui16 sessionServicePort = tp.GetPort(8655);
    TString endpoint = TStringBuilder() << "localhost:" << tokenExchangePort;

    NMvp::TTokensConfig tokensConfig;
    tokensConfig.SetAccessServiceType(NMvp::nebius_v1);
    auto tokenExchangeList = tokensConfig.MutableOAuth2Exchange();
    auto tokenExchange = tokenExchangeList->Add();
    tokenExchange->SetName("nebiusJwt");
    tokenExchange->SetTokenEndpoint(endpoint);
    auto* subjectCreds = tokenExchange->MutableSubjectCredentials();
    subjectCreds->SetToken(ctx.SaId);
    auto* actorCreds = tokenExchange->MutableActorCredentials();
    actorCreds->SetTokenFile(ctx.FederatedJwtTokenPath);
    if (!ctx.OmitExplicitCredsTypeAndTokenType) {
        subjectCreds->SetType(NMvp::TOAuth2Exchange::TCredentials::FIXED);
        subjectCreds->SetTokenType("urn:nebius:params:oauth:token-type:subject_identifier");
        actorCreds->SetType(NMvp::TOAuth2Exchange::TCredentials::FIXED);
        actorCreds->SetTokenType("urn:ietf:params:oauth:token-type:jwt");
    }

    const TString fixedIamToken = "iam_from_tokenator";

    std::unique_ptr<grpc::Server> tokenExchangeServer;
    std::unique_ptr<TTokenExchangeServiceMock> localMock;
    localMock = std::make_unique<TTokenExchangeServiceMock>(ctx.ExpectedSaId, ctx.ExpectedJwtToken, fixedIamToken);
    grpc::ServerBuilder teBuilder;
    teBuilder.AddListeningPort(endpoint, grpc::InsecureServerCredentials()).RegisterService(localMock.get());
    tokenExchangeServer = teBuilder.BuildAndStart();

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
        .AccessServiceType = static_cast<NMvp::EAccessServiceType>(tokensConfig.GetAccessServiceType())
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
        TTempFileHandle tmpToken = MakeTestFile("short_jwt_token", "mvp_federated_jwt", ".token");
        TFederatedTestContext ctx;
        ctx.ExpectedSaId = "serviceaccount-expected";
        ctx.ExpectedJwtToken = "short_jwt_token";
        ctx.SaId = "serviceaccount-expected";
        ctx.FederatedJwtTokenPath = tmpToken.Name();

        ctx.ExpectAuthHeader = true;
        RunTokenatorIntegrationTest(ctx);
    }

    Y_UNIT_TEST(FederatedCredsSaIdMismatch) {
        TTempFileHandle tmpToken = MakeTestFile("short_jwt_token", "mvp_federated_jwt", ".token");
        TFederatedTestContext ctx;
        ctx.ExpectedSaId = "serviceaccount-expected";
        ctx.ExpectedJwtToken = "short_jwt_token";
        ctx.SaId = "wrong-serviceaccount";
        ctx.FederatedJwtTokenPath = tmpToken.Name();
        ctx.ExpectAuthHeader = false;
        RunTokenatorIntegrationTest(ctx);
    }

    Y_UNIT_TEST(FederatedCredsJwtTokenMismatch) {
        TTempFileHandle tmpToken = MakeTestFile("wrong_jwt_token", "mvp_federated_jwt", ".token");
        TFederatedTestContext ctx;
        ctx.ExpectedSaId = "serviceaccount-expected";
        ctx.ExpectedJwtToken = "short_jwt_token";
        ctx.SaId = "serviceaccount-expected";
        ctx.FederatedJwtTokenPath = tmpToken.Name();
        ctx.ExpectAuthHeader = false;
        RunTokenatorIntegrationTest(ctx);
    }

    Y_UNIT_TEST(FederatedCredsShorthandWithoutTypeAndTokenTypeRejected) {
        TTempFileHandle tmpToken = MakeTestFile("short_jwt_token", "mvp_federated_jwt", ".token");
        TFederatedTestContext ctx;
        ctx.ExpectedSaId = "serviceaccount-expected";
        ctx.ExpectedJwtToken = "short_jwt_token";
        ctx.SaId = "serviceaccount-expected";
        ctx.FederatedJwtTokenPath = tmpToken.Name();
        ctx.OmitExplicitCredsTypeAndTokenType = true;
        ctx.ExpectAuthHeader = false;
        RunTokenatorIntegrationTest(ctx);
    }

} // Y_UNIT_TEST_SUITE(MvpTokenator)
