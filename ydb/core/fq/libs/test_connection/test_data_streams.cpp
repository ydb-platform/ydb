#include "events/events.h"
#include "probes.h"
#include "test_connection.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/fq/libs/actors/clusters_from_connections.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/library/security/util.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

namespace {

struct TEvPrivate {
    enum EEv {
        EvResolveDbResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvOpenSessionResponse,
        EvCheckListStreamsResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvResolveDbResponse : NActors::TEventLocal<TEvResolveDbResponse, EvResolveDbResponse> {
        NYql::TDatabaseResolverResponse Result;

        TEvResolveDbResponse(const NYql::TDatabaseResolverResponse& result)
            : Result(result)
        {}
    };

    struct TEvOpenSessionResponse : NActors::TEventLocal<TEvOpenSessionResponse, EvOpenSessionResponse> {
        bool IsSuccess = false;
        TString ErrorMessage;

        TEvOpenSessionResponse(const TString& errorMessage)
            : IsSuccess(false)
            , ErrorMessage(errorMessage)
        {}

        TEvOpenSessionResponse()
            : IsSuccess(true)
        {}
    };

    struct TEvCheckListStreamsResponse : NActors::TEventLocal<TEvCheckListStreamsResponse, EvCheckListStreamsResponse> {
        bool IsSuccess = false;
        TString ErrorMessage;

        TEvCheckListStreamsResponse(const TString& errorMessage)
            : IsSuccess(false)
            , ErrorMessage(errorMessage)
        {}

        TEvCheckListStreamsResponse()
            : IsSuccess(true)
        {}
    };
};

}

namespace NFq {

LWTRACE_USING(YQ_TEST_CONNECTION_PROVIDER);

using namespace NActors;

class TTestDataStreamsConnectionActor : public NActors::TActorBootstrapped<TTestDataStreamsConnectionActor> {
    inline static const TString SessionName = "test_connection_data_streams";

    NFq::NConfig::TCommonConfig CommonConfig;
    TActorId Sender;
    ui64 Cookie;
    TString Scope;
    TString User;
    TString Token;
    TTestConnectionRequestCountersPtr Counters;
    NFq::TYqSharedResources::TPtr SharedResources;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    ::NPq::NConfigurationManager::IConnections::TPtr CmConnections;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    std::shared_ptr<NYql::IDatabaseAsyncResolver> DbResolver;
    NYql::TPqClusterConfig ClusterConfig{};
    TString StructuredToken{};
    NYql::IPqGateway::TPtr Gateway{};
    const TInstant StartTime = TInstant::Now();

public:
    TTestDataStreamsConnectionActor(
        const FederatedQuery::DataStreams& ds,
        const NFq::NConfig::TCommonConfig& commonConfig,
        const std::shared_ptr<NYql::IDatabaseAsyncResolver>& dbResolver,
        const TActorId& sender,
        ui64 cookie,
        const NFq::TYqSharedResources::TPtr& sharedResources,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const ::NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters)
        : CommonConfig(commonConfig)
        , Sender(sender)
        , Cookie(cookie)
        , Scope(scope)
        , User(user)
        , Token(token)
        , Counters(counters)
        , SharedResources(sharedResources)
        , CredentialsFactory(credentialsFactory)
        , CmConnections(cmConnections)
        , FunctionRegistry(functionRegistry)
        , DbResolver(dbResolver)
        , ClusterConfig(CreateClusterConfig(SessionName, CommonConfig, Token, signer, ds))
        , StructuredToken(NYql::ComposeStructuredTokenJsonForServiceAccount(ClusterConfig.GetServiceAccountId(), ClusterConfig.GetServiceAccountIdSignature(), ClusterConfig.GetToken()))
    {
        Counters->InFly->Inc();
    }

    static constexpr char ActorName[] = "YQ_TEST_DATA_STREAMS_CONNECTION";

    void Bootstrap() {
        TC_LOG_D(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Starting test data stream connection actor. Actor id: " << SelfId());
        TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Structured token: " << StructuredToken << " service-account: " << ClusterConfig.GetServiceAccountId() << " signature: " << ClusterConfig.GetServiceAccountIdSignature() << " token: " << NKikimr::MaskTicket(ClusterConfig.GetToken()));
        Become(&TTestDataStreamsConnectionActor::StateFunc);
        SendResolveDatabaseId();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvResolveDbResponse, Handler);
        hFunc(TEvPrivate::TEvOpenSessionResponse, Handler);
        hFunc(TEvPrivate::TEvCheckListStreamsResponse, Handler);
    )

private:
    void SendResolveDatabaseId() {
        if (ClusterConfig.GetDatabase()) {
            TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Database from connection settings " << ClusterConfig.GetDatabase());
            SendOpenSession();
            return;
        }
        NYql::IDatabaseAsyncResolver::TDatabaseAuthMap ids;
        ids[std::pair{ClusterConfig.GetDatabaseId(), NYql::EDatabaseType::DataStreams}] = {StructuredToken, CommonConfig.GetUseBearerForYdb()};
        DbResolver->ResolveIds(ids).Apply([self=SelfId(), as=TActivationContext::ActorSystem()](const auto& future) {
            try {
                auto result = future.GetValue();
                as->Send(new IEventHandle(self, self, new TEvPrivate::TEvResolveDbResponse(result), 0));
            } catch (...) {
                as->Send(new IEventHandle(self, self, new TEvPrivate::TEvResolveDbResponse(NYql::TDatabaseResolverResponse{{}, false, NYql::TIssues{MakeErrorIssue(NFq::TIssuesIds::BAD_REQUEST, CurrentExceptionMessage())}}), 0));
            }
        });
    }

    void Handler(TEvPrivate::TEvResolveDbResponse::TPtr& ev) {
        const auto& response = ev->Get()->Result;
        if (!response.Success) {
            TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Resolve datababse id " << ClusterConfig.GetDatabaseId() << " error " << response.Issues.ToOneLineString());
            ReplyError(response.Issues);
            return;
        }

        auto it = response.DatabaseDescriptionMap.find(std::pair{ClusterConfig.GetDatabaseId(), NYql::EDatabaseType::DataStreams});
        if (it == response.DatabaseDescriptionMap.end()) {
            TC_LOG_E(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Test data streams connection: database is not found for database_id " << ClusterConfig.GetDatabaseId());
            ReplyError(TStringBuilder{} << "Test data streams connection: database is not found for database_id " << ClusterConfig.GetDatabaseId());
            return;
        }

        TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Resolve datababse id result: " << it->second.Database);
        ClusterConfig.SetDatabase(it->second.Database);
        ClusterConfig.SetEndpoint(it->second.Endpoint);
        ClusterConfig.SetUseSsl(it->second.Secure);
        SendOpenSession();
    }

    void SendOpenSession() {
        Gateway = NYql::CreatePqNativeGateway(CreateGatewayServices());
        Gateway->OpenSession(SessionName, {}).Apply([self=SelfId(), as=TActivationContext::ActorSystem()](const auto& future) {
            try {
                future.TryRethrow();
                as->Send(new IEventHandle(self, self, new TEvPrivate::TEvOpenSessionResponse(), 0));
            } catch (...) {
                as->Send(new IEventHandle(self, self, new TEvPrivate::TEvOpenSessionResponse(CurrentExceptionMessage()), 0));
            }
        });
    }

    void Handler(TEvPrivate::TEvOpenSessionResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        if (!response.IsSuccess) {
            TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Open session error " << response.ErrorMessage);
            ReplyError(response.ErrorMessage);
            return;
        }
        TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Open session: ok");
        SendCheckListStreams();
    }

    void SendCheckListStreams() {
        Gateway->ListStreams(SessionName, SessionName, ClusterConfig.GetDatabase(), StructuredToken, 1).Apply([self=SelfId(), as=TActivationContext::ActorSystem()](const auto& future) {
            try {
                future.TryRethrow();
                as->Send(new IEventHandle(self, self, new TEvPrivate::TEvCheckListStreamsResponse(), 0));
            } catch (...) {
                as->Send(new IEventHandle(self, self, new TEvPrivate::TEvCheckListStreamsResponse(CurrentExceptionMessage()), 0));
            }
        });
    }

    void Handler(TEvPrivate::TEvCheckListStreamsResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        if (!response.IsSuccess) {
            TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Check list strems error " << response.ErrorMessage);
            ReplyError(response.ErrorMessage);
            return;
        }
        TC_LOG_T(Scope << " " << User << " " << NKikimr::MaskTicket(Token) << " Check list streams: ok");
        ReplyOk();
    }

    void DestroyActor(bool success = true) {
        Counters->InFly->Dec();
        TDuration delta = TInstant::Now() - StartTime;
        Counters->LatencyMs->Collect(delta.MilliSeconds());
        LWPROBE(TestDataStreamsConnectionRequest, Scope, User, delta, success);
        PassAway();
    }

    void ReplyError(const NYql::TIssues& issues) {
        Counters->Error->Inc();
        Send(Sender, new NFq::TEvTestConnection::TEvTestConnectionResponse(issues), 0, Cookie);
        DestroyActor(false /* success */);
    }

    void ReplyError(const TString& message) {
        ReplyError(NYql::TIssues{MakeErrorIssue(NFq::TIssuesIds::BAD_REQUEST, "Data Streams: " + message)});
    }

    void ReplyOk() {
        Counters->Ok->Inc();
        Send(Sender, new NFq::TEvTestConnection::TEvTestConnectionResponse(FederatedQuery::TestConnectionResult{}), 0, Cookie);
        DestroyActor();
    }

    static NYql::TPqClusterConfig CreateClusterConfig(const TString& sessionName, const NFq::NConfig::TCommonConfig& commonConfig, const TString& token, const NFq::TSigner::TPtr& signer, const FederatedQuery::DataStreams& ds) {
        const auto& auth = ds.auth();
        const TString signedAccountId = signer && auth.has_service_account() ? signer->SignAccountId(auth.service_account().id()) : TString{};
        return NFq::CreatePqClusterConfig(sessionName, commonConfig.GetUseBearerForYdb(), token, signedAccountId, ds);
    }

    NYql::TPqGatewayServices CreateGatewayServices() {
        NYql::TPqGatewayConfig config;
        *config.AddClusterMapping() = ClusterConfig;
        NYql::TPqGatewayServices pqServices(
            SharedResources->UserSpaceYdbDriver,
            CmConnections,
            CredentialsFactory,
            std::make_shared<NYql::TPqGatewayConfig>(config),
            FunctionRegistry
        );
        return pqServices;
    }
};

NActors::IActor* CreateTestDataStreamsConnectionActor(
        const FederatedQuery::DataStreams& ds,
        const NFq::NConfig::TCommonConfig& commonConfig,
        const std::shared_ptr<NYql::IDatabaseAsyncResolver>& dbResolver,
        const TActorId& sender,
        ui64 cookie,
        const NFq::TYqSharedResources::TPtr& sharedResources,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const ::NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TString& scope,
        const TString& user,
        const TString& token,
        const NFq::TSigner::TPtr& signer,
        const TTestConnectionRequestCountersPtr& counters) {
    return new TTestDataStreamsConnectionActor(
                    ds, commonConfig, dbResolver, sender,
                    cookie, sharedResources, credentialsFactory,
                    cmConnections, functionRegistry,
                    scope, user, token, signer, counters);
}

} // namespace NFq
