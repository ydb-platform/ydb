#include "events/events.h"
#include "probes.h"
#include "test_connection.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/fq/libs/actors/clusters_from_connections.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/library/security/util.h>

#include <ydb/library/yql/providers/s3/provider/yql_s3_provider_impl.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/utils/url_builder.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>

namespace {

struct TEvPrivate {
    enum EEv {
        EvDiscoveryResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvDiscoveryResponse : NActors::TEventLocal<TEvDiscoveryResponse, EvDiscoveryResponse> {
        bool IsSuccess = false;
        TString ErrorMessage;
        TString RequestId;

        TEvDiscoveryResponse(const TString& errorMessage, const TString& requestId)
            : IsSuccess(false)
            , ErrorMessage(errorMessage)
            , RequestId(requestId)
        {}

        TEvDiscoveryResponse(const TString& requestId)
            : IsSuccess(true)
            , RequestId(requestId)
        {}
    };
};

}

namespace NFq {

LWTRACE_USING(YQ_TEST_CONNECTION_PROVIDER);

using namespace NActors;

class TTestObjectStorageConnectionActor : public NActors::TActorBootstrapped<TTestObjectStorageConnectionActor> {
    TTestConnectionRequestCountersPtr Counters;
    TActorId Sender;
    TString Scope;
    TString User;
    TString Token;
    ui64 Cookie;
    NYql::IHTTPGateway::TPtr Gateway;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    NYql::TS3ClusterConfig ClusterConfig;

    const TInstant StartTime = TInstant::Now();

public:
    TTestObjectStorageConnectionActor(
        const FederatedQuery::ObjectStorageConnection& os,
        const NFq::NConfig::TCommonConfig& commonConfig,
        const TActorId& sender,
        ui64 cookie,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
         NYql::IHTTPGateway::TPtr gateway,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters)
        : Counters(counters)
        , Sender(sender)
        , Scope(scope)
        , User(user)
        , Token(token)
        , Cookie(cookie)
        , Gateway(gateway)
        , CredentialsFactory(credentialsFactory)
        , ClusterConfig(NFq::CreateS3ClusterConfig({}, token, commonConfig.GetObjectStorageEndpoint(), signer ? signer->SignAccountId(os.auth().service_account().id()) : "", os))
    {
        Counters->InFly->Inc();
    }

    static constexpr char ActorName[] = "YQ_TEST_OBJECT_STORAGE_CONNECTION";

    void Bootstrap() {
        Become(&TTestObjectStorageConnectionActor::StateFunc);
        TC_LOG_D(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Starting test object storage connection actor. Actor id: " << SelfId());
        try {
            SendDiscover();
        } catch (...) {
            ReplyError(CurrentExceptionMessage());
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvDiscoveryResponse, Handle);
    )

    void Handle(TEvPrivate::TEvDiscoveryResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        if (response.IsSuccess) {
            ReplyOk(response.RequestId);
        } else {
            ReplyError(TStringBuilder{} << response.ErrorMessage << ", request id: [" << response.RequestId << "]");
        }
    }

private:
    static void SendError(TActorId self, NActors::TActorSystem* as, const TString& errorMessage, const TString& requestId) {
        as->Send(new IEventHandle(self, self, new TEvPrivate::TEvDiscoveryResponse(errorMessage, requestId), 0));
    }

    static void SendOk(TActorId self, NActors::TActorSystem* as, const TString& requestId) {
        as->Send(new IEventHandle(self, self, new TEvPrivate::TEvDiscoveryResponse(requestId), 0));
    }

    static void DiscoveryCallback(NYql::IHTTPGateway::TResult&& result, TActorId self, const TString& requestId, NActors::TActorSystem* as) {
        if (!result.Issues) {
            try {
                const NXml::TDocument xml(result.Content.Extract(), NXml::TDocument::String);
                if (const auto& root = xml.Root(); root.Name() == "Error") {
                    const auto& code = root.Node("Code", true).Value<TString>();
                    const auto& message = root.Node("Message", true).Value<TString>();
                    SendError(self, as, TStringBuilder() << message << ", code: " << code, requestId);
                } else if (root.Name() != "ListBucketResult") {
                    SendError(self, as, TStringBuilder() << "Unexpected response '" << root.Name() << "' on discovery.", requestId);
                } else {
                    SendOk(self, as, requestId);
                }
            } catch (const std::exception& ex) {
                SendError(self, as, TStringBuilder() << "Exception occurred: " << ex.what(), requestId);
            }
        } else {
            SendError(self, as, TStringBuilder() << "Issues occurred: " << result.Issues.ToOneLineString(), requestId);
        }
    }

    static ERetryErrorClass RetryS3SlowDown(CURLcode curlResponseCode, long httpResponseCode) {
        return curlResponseCode == CURLE_OK && httpResponseCode == 503 ? ERetryErrorClass::LongRetry : ERetryErrorClass::NoRetry; // S3 Slow Down == 503
    }

    void SendDiscover() {
        const auto structedToken = NYql::ComposeStructuredTokenJsonForServiceAccount(ClusterConfig.GetServiceAccountId(), ClusterConfig.GetServiceAccountIdSignature(), ClusterConfig.GetToken());
        const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, structedToken);
        const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();

        TString requestId = CreateGuidAsString();
        NYql::IHTTPGateway::THeaders headers = NYql::IHTTPGateway::MakeYcHeaders(requestId, authToken, {});

        const auto retryPolicy = NYql::IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy(RetryS3SlowDown);

        NYql::TUrlBuilder urlBuilder(ClusterConfig.GetUrl());
        const auto url = urlBuilder.AddUrlParam("list-type", "2")
                                   .AddUrlParam("max-keys", "1")
                                   .Build();

        Gateway->Download(
                url,
                headers,
                0U,
                0U,
                std::bind(&DiscoveryCallback, std::placeholders::_1, SelfId(), requestId, TActivationContext::ActorSystem()),
                /*data=*/"",
                retryPolicy
            );
    }

    void DestroyActor(bool success = true) {
        Counters->InFly->Dec();
        TDuration delta = TInstant::Now() - StartTime;
        Counters->LatencyMs->Collect(delta.MilliSeconds());
        LWPROBE(TestObjectStorageConnectionRequest, Scope, User, delta, success);
        PassAway();
    }

    void ReplyError(const TString& message) {
        TC_LOG_D(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Invalid access for object storage connection: " << message);
        Counters->Error->Inc();
        Send(Sender, new NFq::TEvTestConnection::TEvTestConnectionResponse(NYql::TIssues{MakeErrorIssue(NFq::TIssuesIds::BAD_REQUEST, "Object Storage: " + message)}), 0, Cookie);
        DestroyActor(false /* success */);
    }

    void ReplyOk(const TString& requestId) {
        TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Access is valid for object storage connection, request id: [" << requestId << "]");
        Counters->Ok->Inc();
        Send(Sender, new NFq::TEvTestConnection::TEvTestConnectionResponse(FederatedQuery::TestConnectionResult{}), 0, Cookie);
        DestroyActor();
    }
};

NActors::IActor* CreateTestObjectStorageConnectionActor(
        const FederatedQuery::ObjectStorageConnection& os,
        const NFq::NConfig::TCommonConfig& commonConfig,
        const TActorId& sender,
        ui64 cookie,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
         NYql::IHTTPGateway::TPtr gateway,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters) {
    return new TTestObjectStorageConnectionActor(
                    os, commonConfig, sender,
                    cookie, credentialsFactory, gateway,
                    scope, user, token, signer, counters);
}

} // namespace NFq
