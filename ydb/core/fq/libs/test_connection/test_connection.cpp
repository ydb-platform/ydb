#include "events/events.h"
#include "probes.h"
#include "test_connection.h"
#include "request_validators.h"

#include <ydb/core/mon/mon.h>
#include <ydb/core/fq/libs/actors/database_resolver.h>
#include <ydb/core/fq/libs/actors/proxy.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>
#include <ydb/core/fq/libs/control_plane_storage/config.h>

#include <ydb/library/security/util.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NFq {

LWTRACE_USING(YQ_TEST_CONNECTION_PROVIDER);

using namespace NActors;

class TTestConnectionActor : public NActors::TActorBootstrapped<TTestConnectionActor> {

    enum ERequestTypeScope {
        RTS_TEST_DATA_STREAMS_CONNECTION,
        RTS_TEST_MONITORING_CONNECTION,
        RTS_TEST_OBJECT_STORAGE_CONNECTION,
        RTS_TEST_UNSUPPORTED_CONNECTION,
        RTS_MAX,
    };

    class TCounters: public virtual TThrRefBase {
        struct TMetricsScope {
            TString CloudId;
            TString Scope;

            bool operator<(const TMetricsScope& right) const {
                return std::tie(CloudId, Scope) < std::tie(right.CloudId, right.Scope);
            }
        };

        using TScopeCounters = std::array<TTestConnectionRequestCountersPtr, RTS_MAX>;
        using TScopeCountersPtr = std::shared_ptr<TScopeCounters>;

        TMap<TMetricsScope, TScopeCountersPtr> ScopeCounters;
        ::NMonitoring::TDynamicCounterPtr Counters;

        ERequestTypeScope ToType(FederatedQuery::ConnectionSetting::ConnectionCase connectionCase) {
            switch (connectionCase) {
            case FederatedQuery::ConnectionSetting::kDataStreams:
                return RTS_TEST_DATA_STREAMS_CONNECTION;
            case FederatedQuery::ConnectionSetting::kObjectStorage:
                return RTS_TEST_OBJECT_STORAGE_CONNECTION;
            case FederatedQuery::ConnectionSetting::kMonitoring:
                return RTS_TEST_MONITORING_CONNECTION;
            default:
                return RTS_TEST_UNSUPPORTED_CONNECTION;
            }
        }

    public:
        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {}

        TTestConnectionRequestCountersPtr GetScopeCounters(const TString& cloudId, const TString& scope, FederatedQuery::ConnectionSetting::ConnectionCase connectionCase) {
            ERequestTypeScope type = ToType(connectionCase);
            TMetricsScope key{cloudId, scope};
            auto it = ScopeCounters.find(key);
            if (it != ScopeCounters.end()) {
                return (*it->second)[type];
            }

            auto scopeRequests = std::make_shared<TScopeCounters>(CreateArray<RTS_MAX, TTestConnectionRequestCountersPtr>({
                { MakeIntrusive<TTestConnectionRequestCounters>("TestDataStreamsConnection") },
                { MakeIntrusive<TTestConnectionRequestCounters>("TestMonitoringConnection") },
                { MakeIntrusive<TTestConnectionRequestCounters>("TestObjectStorageConnection") },
                { MakeIntrusive<TTestConnectionRequestCounters>("TestUnsupportedConnection") },
            }));

            auto scopeCounters = (cloudId ? Counters->GetSubgroup("cloud_id", cloudId) : Counters)
                                    ->GetSubgroup("scope", scope);

            for (auto& request: *scopeRequests) {
                request->Register(scopeCounters);
            }

            ScopeCounters[key] = scopeRequests;
            return (*scopeRequests)[type];
        }
    };

    NConfig::TTestConnectionConfig Config;
    ::NFq::TControlPlaneStorageConfig ControlPlaneStorageConfig;
    NConfig::TCommonConfig CommonConfig;
    NFq::TYqSharedResources::TPtr SharedResouces;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    NPq::NConfigurationManager::IConnections::TPtr CmConnections;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TCounters Counters;
    NFq::TSigner::TPtr Signer;
    TActorId DatabaseResolverActor;
    std::shared_ptr<NYql::IDatabaseAsyncResolver> DbResolver;
    NYql::IHTTPGateway::TPtr HttpGateway;
    NYql::IMdbEndpointGenerator::TPtr MdbEndpointGenerator;

public:
    TTestConnectionActor(
        const NConfig::TTestConnectionConfig& config,
        const NConfig::TControlPlaneStorageConfig& controlPlaneStorageConfig,
        const NYql::TS3GatewayConfig& s3Config,
        const NConfig::TCommonConfig& commonConfig,
        const ::NFq::TSigner::TPtr& signer,
        const NFq::TYqSharedResources::TPtr& sharedResources,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const NYql::IHTTPGateway::TPtr& httpGateway,
        const ::NMonitoring::TDynamicCounterPtr& counters)
        : Config(config)
        , ControlPlaneStorageConfig(controlPlaneStorageConfig, s3Config, commonConfig, {})
        , CommonConfig(commonConfig)
        , SharedResouces(sharedResources)
        , CredentialsFactory(credentialsFactory)
        , CmConnections(cmConnections)
        , FunctionRegistry(functionRegistry)
        , Counters(counters)
        , Signer(signer)
        , HttpGateway(httpGateway)
        , MdbEndpointGenerator(NFq::MakeMdbEndpointGeneratorGeneric(commonConfig.GetMdbTransformHost()))
    {}

    static constexpr char ActorName[] = "YQ_TEST_CONNECTION";

    void Bootstrap() {
        TC_LOG_D("Starting yandex query test connection. Actor id: " << SelfId());

        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YQ_TEST_CONNECTION_PROVIDER));

        DatabaseResolverActor = Register(NFq::CreateDatabaseResolver(NFq::MakeYqlAnalyticsHttpProxyId(), CredentialsFactory));
        DbResolver = std::make_shared<NFq::TDatabaseAsyncResolverImpl>(
                        NActors::TActivationContext::ActorSystem(), DatabaseResolverActor,
                        CommonConfig.GetYdbMvpCloudEndpoint(), CommonConfig.GetMdbGateway(),
                        MdbEndpointGenerator
                        );

        Become(&TTestConnectionActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvTestConnection::TEvTestConnectionRequest, Handle);
        hFunc(NMon::TEvHttpInfo, Handle);
    )

    void Handle(TEvTestConnection::TEvTestConnectionRequest::TPtr& ev) {
        NYql::TIssues issues = ValidateTestConnection(ev, ControlPlaneStorageConfig.Proto.GetMaxRequestSize(), ControlPlaneStorageConfig.AvailableConnections, ControlPlaneStorageConfig.Proto.GetDisableCurrentIam());
        const TString& cloudId = ev->Get()->CloudId;
        const TString& scope = ev->Get()->Scope;
        const TString& user = ev->Get()->User;
        const TString& token = ev->Get()->Token;
        FederatedQuery::TestConnectionRequest request = std::move(ev->Get()->Request);
        TTestConnectionRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, request.setting().connection_case());
        if (issues) {
            requestCounters->Error->Inc();
            TC_LOG_D("TestConnectionRequest: validation failed " << scope << " " << user << " " << NKikimr::MaskTicket(token) << issues.ToOneLineString());
            Send(ev->Sender, new TEvTestConnection::TEvTestConnectionResponse(issues), 0, ev->Cookie);
            return;
        }

        TC_LOG_T("TestConnectionRequest: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << request.DebugString());
        switch (request.setting().connection_case()) {
            case FederatedQuery::ConnectionSetting::kDataStreams: {
                Register(CreateTestDataStreamsConnectionActor(
                                *request.mutable_setting()->mutable_data_streams(),
                                CommonConfig, DbResolver, ev->Sender,
                                ev->Cookie, SharedResouces,
                                CredentialsFactory, CmConnections, FunctionRegistry,
                                scope, user, token,
                                Signer, requestCounters));
                break;
            }
            case FederatedQuery::ConnectionSetting::kObjectStorage: {
                Register(CreateTestObjectStorageConnectionActor(
                                *request.mutable_setting()->mutable_object_storage(),
                                CommonConfig, ev->Sender,
                                ev->Cookie, CredentialsFactory,
                                HttpGateway, scope, user, token,
                                Signer, requestCounters));
                break;
            }
            case FederatedQuery::ConnectionSetting::kMonitoring: {
                TString monitoringEndpoint = CommonConfig.GetMonitoringEndpoint();
                Register(CreateTestMonitoringConnectionActor(
                                *request.mutable_setting()->mutable_monitoring(),
                                ev->Sender, ev->Cookie, monitoringEndpoint,
                                CredentialsFactory, scope, user, token,
                                Signer, requestCounters));
                break;
            }
            default: {
                LWPROBE(TestUnsupportedConnectionRequest, scope, user);
                requestCounters->Error->Inc();
                TC_LOG_E("TestConnectionRequest: unimplemented " << scope << " " << user << " " << NKikimr::MaskTicket(token) << request.DebugString());
                Send(ev->Sender, new TEvTestConnection::TEvTestConnectionResponse(NYql::TIssues{MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Unimplemented yet")}), 0, ev->Cookie);
            }
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
                str << Endl;
            }
        }
        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }
};

NActors::TActorId TestConnectionActorId() {
    constexpr TStringBuf name = "TSTCONN";
    return NActors::TActorId(0, name);
}

NActors::IActor* CreateTestConnectionActor(
        const NConfig::TTestConnectionConfig& config,
        const NConfig::TControlPlaneStorageConfig& controlPlaneStorageConfig,
        const NYql::TS3GatewayConfig& s3Config,
        const NConfig::TCommonConfig& commonConfig,
        const ::NFq::TSigner::TPtr& signer,
        const NFq::TYqSharedResources::TPtr& sharedResources,
        const NYql::ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
        const NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const NYql::IHTTPGateway::TPtr& httpGateway,
        const ::NMonitoring::TDynamicCounterPtr& counters) {
    return new TTestConnectionActor(config, controlPlaneStorageConfig,
                                    s3Config, commonConfig,
                                    signer, sharedResources,
                                    credentialsFactory, cmConnections,
                                    functionRegistry, httpGateway, counters);
}

} // namespace NFq
