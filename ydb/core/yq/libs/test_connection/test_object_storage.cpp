#include "events/events.h"
#include "probes.h"
#include "test_connection.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <ydb/core/yq/libs/actors/clusters_from_connections.h>
#include <ydb/core/yq/libs/config/yq_issue.h>
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

        TEvDiscoveryResponse(const TString& errorMessage)
            : IsSuccess(false)
            , ErrorMessage(errorMessage)
        {}

        TEvDiscoveryResponse()
            : IsSuccess(true)
        {}
    };
};

}

namespace NYq {

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
        const YandexQuery::ObjectStorageConnection& os,
        const NYq::NConfig::TCommonConfig& commonConfig,
        const TActorId& sender,
        ui64 cookie,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
         NYql::IHTTPGateway::TPtr gateway,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NYq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters)
        : Counters(counters)
        , Sender(sender)
        , Scope(scope)
        , User(user)
        , Token(token)
        , Cookie(cookie)
        , Gateway(gateway)
        , CredentialsFactory(credentialsFactory)
        , ClusterConfig(NYq::CreateS3ClusterConfig({}, token, commonConfig.GetObjectStorageEndpoint(), signer ? signer->SignAccountId(os.auth().service_account().id()) : "", os))
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
            ReplyOk();
        } else {
            ReplyError(response.ErrorMessage);
        }
    }

private:
    static void SendError(TActorId self, NActors::TActorSystem* as, const TString& errorMessage) {
        as->Send(new IEventHandle(self, self, new TEvPrivate::TEvDiscoveryResponse(errorMessage), 0));
    }

    static void SendOk(TActorId self, NActors::TActorSystem* as) {
        as->Send(new IEventHandle(self, self, new TEvPrivate::TEvDiscoveryResponse(), 0));
    }

    static void DiscoveryCallback(NYql::IHTTPGateway::TResult&& result, TActorId self, NActors::TActorSystem* as) {
        switch (result.index()) {
        case 0U: try {
            const NXml::TDocument xml(std::get<NYql::IHTTPGateway::TContent>(std::move(result)).Extract(), NXml::TDocument::String);
            if (const auto& root = xml.Root(); root.Name() == "Error") {
                const auto& code = root.Node("Code", true).Value<TString>();
                const auto& message = root.Node("Message", true).Value<TString>();
                SendError(self, as, TStringBuilder() << message << ", code: " << code);
                return;
            } else if (root.Name() != "ListBucketResult") {
                SendError(self, as, TStringBuilder() << "Unexpected response '" << root.Name() << "' on discovery.");
                return;
            } else {
                break;
            }
        } catch (const std::exception& ex) {
            SendError(self, as, TStringBuilder() << "Exception occurred: " << ex.what());
            return;
        }
        case 1U:
            SendError(self, as, TStringBuilder() << "Issues occurred: " << std::get<NYql::TIssues>(result).ToString());
            return;
        default:
            SendError(self, as, TStringBuilder() << "Undefined variant index: " << result.index());
            return;
        }
        SendOk(self, as);
    }

    static ERetryErrorClass RetryS3SlowDown(long httpResponseCode) {
        return httpResponseCode == 503 ? ERetryErrorClass::LongRetry : ERetryErrorClass::NoRetry; // S3 Slow Down == 503
    }

    void SendDiscover() {
        const auto structedToken = NYql::ComposeStructuredTokenJsonForServiceAccount(ClusterConfig.GetServiceAccountId(), ClusterConfig.GetServiceAccountIdSignature(), ClusterConfig.GetToken());
        const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, structedToken);
        const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();

        NYql::IHTTPGateway::THeaders headers;
        if (authToken) {
            headers.push_back(TString("X-YaCloud-SubjectToken:") += authToken);
        }

        const auto retryPolicy = IRetryPolicy<long>::GetExponentialBackoffPolicy(RetryS3SlowDown);

        NYql::TUrlBuilder urlBuilder(ClusterConfig.GetUrl());
        const auto url = urlBuilder.AddUrlParam("list-type", "2")
                                   .AddUrlParam("max-keys", "1")
                                   .Build();

        Gateway->Download(
                url,
                headers,
                0U,
                std::bind(&DiscoveryCallback, std::placeholders::_1, SelfId(), TActivationContext::ActorSystem()),
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
        Send(Sender, new NYq::TEvTestConnection::TEvTestConnectionResponse(NYql::TIssues{MakeErrorIssue(NYq::TIssuesIds::BAD_REQUEST, message)}), 0, Cookie);
        DestroyActor(false /* success */);
    }

    void ReplyOk() {
        TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Access is valid for object storage connection");
        Counters->Ok->Inc();
        Send(Sender, new NYq::TEvTestConnection::TEvTestConnectionResponse(YandexQuery::TestConnectionResult{}), 0, Cookie);
        DestroyActor();
    }
};

NActors::IActor* CreateTestObjectStorageConnectionActor(
        const YandexQuery::ObjectStorageConnection& os,
        const NYq::NConfig::TCommonConfig& commonConfig,
        const TActorId& sender,
        ui64 cookie,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
         NYql::IHTTPGateway::TPtr gateway,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NYq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters) {
    return new TTestObjectStorageConnectionActor(
                    os, commonConfig, sender,
                    cookie, credentialsFactory, gateway,
                    scope, user, token, signer, counters);
}

} // namespace NYq
