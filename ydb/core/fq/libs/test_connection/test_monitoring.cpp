#include "probes.h"
#include "test_connection.h"

#include <ydb/core/fq/libs/actors/clusters_from_connections.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/test_connection/events/events.h>
#include <ydb/library/security/util.h>

#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/utils/url_builder.h>

#include <ydb/library/yql/providers/solomon/async_io/dq_solomon_write_actor.h>
#include <ydb/library/yql/utils/actors/http_sender_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NFq {

LWTRACE_USING(YQ_TEST_CONNECTION_PROVIDER);

using namespace NActors;

class TTestMonitoringConnectionActor : public NActors::TActorBootstrapped<TTestMonitoringConnectionActor> {
    TActorId Sender;
    TActorId HttpProxyId;
    ui64 Cookie;
    TString Endpoint;
    TString Scope;
    TString User;
    TString Token;
    TTestConnectionRequestCountersPtr Counters;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    NFq::TSigner::TPtr Signer;
    NYql::TSolomonClusterConfig ClusterConfig;
    const TInstant StartTime = TInstant::Now();

public:
    TTestMonitoringConnectionActor(
        const FederatedQuery::Monitoring& monitoring,
        const TActorId& sender,
        ui64 cookie,
        const TString& endpoint,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters)
        : Sender(sender)
        , Cookie(cookie)
        , Endpoint(endpoint)
        , Scope(scope)
        , User(user)
        , Token(token)
        , Counters(counters)
        , CredentialsFactory(credentialsFactory)
        , Signer(signer)
        , ClusterConfig(NFq::CreateSolomonClusterConfig({}, token, endpoint, signer ? signer->SignAccountId(monitoring.auth().service_account().id()) : "", monitoring))
    {
        Counters->InFly->Inc();
    }

    static constexpr char ActorName[] = "YQ_TEST_MONITORING_CONNECTION";

    void Bootstrap() {
        TC_LOG_D(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Starting test monitoring connection actor. Actor id: " << SelfId());
        Become(&TTestMonitoringConnectionActor::StateFunc);

        try {
            HttpProxyId = Register(NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance()));
            const NHttp::THttpOutgoingRequestPtr httpRequest = BuildSolomonRequest();
            auto retryPolicy = NYql::NDq::THttpSenderRetryPolicy::GetNoRetryPolicy();
            const TActorId httpSenderId = Register(NYql::NDq::CreateHttpSenderActor(SelfId(), HttpProxyId, retryPolicy));
            Send(httpSenderId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest), /*flags=*/0, Cookie);
            TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " send request " << httpRequest->Method << " " << httpRequest->Protocol << " " << httpRequest->Host << " " << httpRequest->URL << " " << httpRequest->Body);
        } catch (...) {
            ReplyError(CurrentExceptionMessage());
        }
    }

    void FillAuth(NHttp::THttpOutgoingRequestPtr& httpRequest) {
        const TString authorizationHeader = "Authorization";
        const auto structedToken = NYql::ComposeStructuredTokenJsonForServiceAccount(ClusterConfig.GetServiceAccountId(), ClusterConfig.GetServiceAccountIdSignature(), ClusterConfig.GetToken());
        const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, structedToken);
        const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();

        switch (static_cast<NYql::NSo::NProto::ESolomonClusterType>(ClusterConfig.GetClusterType())) {
            case NYql::NSo::NProto::ESolomonClusterType::CT_SOLOMON:
                httpRequest->Set(authorizationHeader, "OAuth " + authToken);
                break;
            case NYql::NSo::NProto::ESolomonClusterType::CT_MONITORING:
                httpRequest->Set(authorizationHeader, "Bearer " + authToken);
                break;
            default:
                Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(ClusterConfig.GetClusterType()));
        }
    }

    NHttp::THttpOutgoingRequestPtr BuildSolomonRequest() {
        const TString url = NYql::NDq::GetSolomonUrl(ClusterConfig.GetCluster(),
                            ClusterConfig.GetUseSsl(),
                            ClusterConfig.GetPath().GetProject(),
                            ClusterConfig.GetPath().GetCluster(),
                            {},
                            static_cast<NYql::NSo::NProto::ESolomonClusterType>(ClusterConfig.GetClusterType()));
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(url);
        FillAuth(httpRequest);
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/json");
        httpRequest->Set<&NHttp::THttpRequest::Body>("{}");
        return httpRequest;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NYql::NDq::TEvHttpBase::TEvSendResult, Handle);
    )

    void Handle(NYql::NDq::TEvHttpBase::TEvSendResult::TPtr& ev) {
        const auto* res = ev->Get();
        if (res->HttpIncomingResponse->Get()->Response->Status == "400") {
            TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " ok " << res->HttpIncomingResponse->Get()->ToString());
            ReplyOk();
            return;
        }

        const TString& error = res->HttpIncomingResponse->Get()->GetError();
        TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " access problem " << res->HttpIncomingResponse->Get()->ToString() << " " << error);
        ReplyError(error);
    }

    void DestroyActor(bool isSuccess) {
        Counters->InFly->Dec();
        TDuration delta = TInstant::Now() - StartTime;
        Counters->LatencyMs->Collect(delta.MilliSeconds());
        LWPROBE(TestMonitoringConnectionRequest, Scope, User, delta, isSuccess);
        Send(HttpProxyId, new NActors::TEvents::TEvPoison());
        PassAway();
    }

    void ReplyError(const TString& message) {
        Counters->Error->Inc();
        Send(Sender, new NFq::TEvTestConnection::TEvTestConnectionResponse(NYql::TIssues{MakeErrorIssue(NFq::TIssuesIds::BAD_REQUEST, "Monitoring: " + message)}), Cookie);
        DestroyActor(false /* success */);
    }

    void ReplyOk() {
        Counters->Ok->Inc();
        Send(Sender,  new NFq::TEvTestConnection::TEvTestConnectionResponse(FederatedQuery::TestConnectionResult{}), Cookie);
        DestroyActor(true /* success */);
    }
};

NActors::IActor* CreateTestMonitoringConnectionActor(
        const FederatedQuery::Monitoring& monitoring,
        const TActorId& sender,
        ui64 cookie,
        const TString& endpoint,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters) {
    return new TTestMonitoringConnectionActor(
                    monitoring, sender,
                    cookie, endpoint, credentialsFactory,
                    scope, user, token, signer, counters);
}

} // namespace NFq
