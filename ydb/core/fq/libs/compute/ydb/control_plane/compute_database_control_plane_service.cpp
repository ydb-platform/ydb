#include "compute_database_control_plane_service.h"

#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/compute/ydb/synchronization_service/synchronization_service.h>
#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/public/lib/fq/scope.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TCreateDatabaseRequestActor : public NActors::TActorBootstrapped<TCreateDatabaseRequestActor> {
public:
    TCreateDatabaseRequestActor(const TActorId& databaseClientActorId, const TActorId& synchronizationServiceActorId, const NFq::NConfig::TComputeConfig& config, const NFq::NConfig::TYdbStorageConfig& executionConnection, TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& request)
        : DatabaseClientActorId(databaseClientActorId)
        , SynchronizationServiceActorId(synchronizationServiceActorId)
        , Config(config)
        , Request(request)
        , ExecutionConnection(executionConnection)
    {}

    static constexpr char ActorName[] = "FQ_CREATE_DATABASE_REQUEST_ACTOR";

    void Bootstrap() {
        const auto& controlPlane = Config.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
            case NConfig::TYdbComputeControlPlane::kSingle: {
                *Result.mutable_connection() = Config.GetYdb().GetControlPlane().GetSingle().GetConnection();
                Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvSynchronizeRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope, Config.GetYdb().GetControlPlane().GetSingle().GetConnection()});
            }
            break;
            case NConfig::TYdbComputeControlPlane::kCms:
            case NConfig::TYdbComputeControlPlane::kYdbcp:
            Send(NFq::ControlPlaneStorageServiceActorId(), new TEvControlPlaneStorage::TEvDescribeDatabaseRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope});
            break;
        }

        Become(&TCreateDatabaseRequestActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneStorage::TEvDescribeDatabaseResponse, Handle);
        hFunc(TEvYdbCompute::TEvCreateDatabaseResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateDatabaseResponse, Handle);
        hFunc(TEvYdbCompute::TEvSynchronizeResponse, Handle);
    )

    void Handle(TEvControlPlaneStorage::TEvDescribeDatabaseResponse::TPtr& ev) {
        const auto& issues = ev->Get()->Issues;
        const auto& result = ev->Get()->Record;

        if (issues && issues.back().IssueCode == TIssuesIds::ACCESS_DENIED) {
            Send(DatabaseClientActorId, new TEvYdbCompute::TEvCreateDatabaseRequest{Request->Get()->CloudId, Request->Get()->Scope, Request->Get()->BasePath, Request->Get()->Path, ExecutionConnection});
            return;
        }

        if (issues) {
            FailedAndPassAway(issues);
            return;
        }

        Result = result;
        Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvSynchronizeRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope, result.connection()});
    }

    void Handle(TEvYdbCompute::TEvCreateDatabaseResponse::TPtr& ev) {
        const auto issues = ev->Get()->Issues;
        if (issues) {
            FailedAndPassAway(issues);
            return;
        }

        Result = ev->Get()->Result;
        Send(ControlPlaneStorageServiceActorId(), new TEvControlPlaneStorage::TEvCreateDatabaseRequest{Request->Get()->CloudId, Request->Get()->Scope, Result});
    }

    void Handle(TEvControlPlaneStorage::TEvCreateDatabaseResponse::TPtr& ev) {
        const auto issues = ev->Get()->Issues;
        if (issues) {
            FailedAndPassAway(issues);
            return;
        }

        Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvSynchronizeRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope, Result.connection()});
    }

    void Handle(TEvYdbCompute::TEvSynchronizeResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            FailedAndPassAway(response.Issues);
            return;
        }

        Send(Request->Sender, new TEvYdbCompute::TEvCreateDatabaseResponse{Result});
        PassAway();
    }

    void FailedAndPassAway(const NYql::TIssues& issues) {
        Send(Request->Sender, new TEvYdbCompute::TEvCreateDatabaseResponse{issues});
        PassAway();
    }

private:
    TActorId DatabaseClientActorId;
    TActorId SynchronizationServiceActorId;
    NFq::NConfig::TComputeConfig Config;
    TEvYdbCompute::TEvCreateDatabaseRequest::TPtr Request;
    FederatedQuery::Internal::ComputeDatabaseInternal Result;
    NFq::NConfig::TYdbStorageConfig ExecutionConnection;
};

class TComputeDatabaseControlPlaneServiceActor : public NActors::TActorBootstrapped<TComputeDatabaseControlPlaneServiceActor> {
    struct TClientConfig {
        TActorId ActorId;
        NConfig::TComputeDatabaseConfig Config;
    };

public:
    TComputeDatabaseControlPlaneServiceActor(const NFq::NConfig::TComputeConfig& config,
                                             const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
                                             const NConfig::TCommonConfig& commonConfig,
                                             const TSigner::TPtr& signer,
                                             const TYqSharedResources::TPtr& yqSharedResources,
                                             const ::NMonitoring::TDynamicCounterPtr& counters)
        : Config(config)
        , CommonConfig(commonConfig)
        , Signer(signer)
        , YqSharedResources(yqSharedResources)
        , CredentialsProviderFactory(credentialsProviderFactory)
        , Counters(counters)
    {}

    static constexpr char ActorName[] = "FQ_COMPUTE_DATABASE_SERVICE_ACTOR";

    void Bootstrap() {
        SynchronizationServiceActorId = Register(CreateSynchronizationServiceActor(CommonConfig, 
                                                                                          Config,
                                                                                          Signer, 
                                                                                          YqSharedResources, 
                                                                                          CredentialsProviderFactory, 
                                                                                          Counters).release());
        const auto& controlPlane = Config.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
            case NConfig::TYdbComputeControlPlane::kSingle:
            break;
            case NConfig::TYdbComputeControlPlane::kCms:
                CreateCmsClientActors(controlPlane.GetCms());
            break;
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                CreateControlPlaneClientActors(controlPlane.GetYdbcp());
            break;
        }
        Become(&TComputeDatabaseControlPlaneServiceActor::StateFunc);
    }

    static NCloud::TGrpcClientSettings CreateGrpcClientSettings(const NConfig::TComputeDatabaseConfig& config) {
        NCloud::TGrpcClientSettings settings;
        const auto& connection = config.GetControlPlaneConnection();
        settings.Endpoint = connection.GetEndpoint();
        settings.EnableSsl = connection.GetUseSsl();
        if (connection.GetCertificateFile()) {
            settings.CertificateRootCA = StripString(TFileInput(connection.GetCertificateFile()).ReadAll());
        }
        return settings;
    }

    void CreateCmsClientActors(const NConfig::TYdbComputeControlPlane::TCms& cmsConfig) {
        const auto& mapping = cmsConfig.GetDatabaseMapping();
        for (const auto& config: mapping.GetCommon()) {
            CommonDatabaseClients.push_back({Register(CreateCmsGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider()).release()), config});
        }

        Y_VERIFY(CommonDatabaseClients);

        for (const auto& [scope, config]: mapping.GetScopeToComputeDatabase()) {
            ScopeToDatabaseClient[scope] = {Register(CreateCmsGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider()).release()), config};
        }
    }

    void CreateControlPlaneClientActors(const NConfig::TYdbComputeControlPlane::TYdbcp& controlPlaneConfig) {
        const auto& mapping = controlPlaneConfig.GetDatabaseMapping();
        for (const auto& config: mapping.GetCommon()) {
            CommonDatabaseClients.push_back({Register(CreateYdbcpGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider()).release()), config});
        }

        Y_VERIFY(CommonDatabaseClients);

        for (const auto& [scope, config]: mapping.GetScopeToComputeDatabase()) {
            ScopeToDatabaseClient[scope] = {Register(CreateYdbcpGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider()).release()), config};
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCreateDatabaseRequest, Handle);
    )

    void Handle(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev) {
        if (Config.GetYdb().GetControlPlane().HasSingle()) {
            Register(new TCreateDatabaseRequestActor(TActorId{}, SynchronizationServiceActorId, Config, Config.GetYdb().GetControlPlane().GetSingle().GetConnection(), ev));
            return;
        }

        const auto& scope = ev->Get()->Scope;
        auto it = ScopeToDatabaseClient.find(scope);
        if (it != ScopeToDatabaseClient.end()) {
            FillRequest(ev, it->second.Config);
            Register(new TCreateDatabaseRequestActor(it->second.ActorId, SynchronizationServiceActorId, Config, it->second.Config.GetExecutionConnection(), ev));
            return;
        }
        const auto& clientConfig = CommonDatabaseClients[MultiHash(scope) % CommonDatabaseClients.size()];
        FillRequest(ev, clientConfig.Config);
        Register(new TCreateDatabaseRequestActor(clientConfig.ActorId, SynchronizationServiceActorId, Config, clientConfig.Config.GetExecutionConnection(), ev));
    }

    void FillRequest(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev, const NConfig::TComputeDatabaseConfig& config) {
        NYdb::NFq::TScope scope(ev.Get()->Get()->Scope);
        ev.Get()->Get()->BasePath = config.GetControlPlaneConnection().GetDatabase();
        const TString databaseName = Config.GetYdb().GetControlPlane().GetDatabasePrefix() + scope.ParseFolder();
        ev.Get()->Get()->Path = config.GetTenant() ? config.GetTenant() + "/" + databaseName: databaseName;
    }

private:
    TActorId SynchronizationServiceActorId;
    NFq::NConfig::TComputeConfig Config;
    TVector<TClientConfig> CommonDatabaseClients;
    TMap<TString, TClientConfig> ScopeToDatabaseClient;
    NConfig::TCommonConfig CommonConfig;
    TSigner::TPtr Signer;
    TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    ::NMonitoring::TDynamicCounterPtr Counters;
};

std::unique_ptr<NActors::IActor> CreateComputeDatabaseControlPlaneServiceActor(const NFq::NConfig::TComputeConfig& config,
                                                                               const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
                                                                               const NConfig::TCommonConfig& commonConfig,
                                                                               const TSigner::TPtr& signer,
                                                                               const TYqSharedResources::TPtr& yqSharedResources,
                                                                               const ::NMonitoring::TDynamicCounterPtr& counters) {
    return std::make_unique<TComputeDatabaseControlPlaneServiceActor>(config,
                                                                      credentialsProviderFactory,
                                                                      commonConfig,
                                                                      signer,
                                                                      yqSharedResources,
                                                                      counters);
}

NActors::TActorId ComputeDatabaseControlPlaneServiceActorId() {
    constexpr TStringBuf name = "COMDBSRV";
    return NActors::TActorId(0, name);
}

}
