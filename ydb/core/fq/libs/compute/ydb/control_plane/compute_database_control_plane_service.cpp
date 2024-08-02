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

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseControlPlane]: " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

namespace {

struct TDatabaseClients {
    struct TClientConfig {
        TActorId ActorId;
        NConfig::TComputeDatabaseConfig Config;
        TActorId DatabasesCacheActorId;
        TActorId MonitoringActorId;
    };

    std::optional<TClientConfig> GetClient(const TString& scope, const TString& endpoint, const TString& database) const {
        auto currentClient = GetClient(scope);
        if (currentClient.Config.GetExecutionConnection().GetEndpoint() == endpoint && database.StartsWith(currentClient.Config.GetTenant())) {
            return currentClient;
        }

        for (const auto& client: CommonDatabaseClients) {
            if (client.Config.GetExecutionConnection().GetEndpoint() == endpoint && database.StartsWith(client.Config.GetTenant())) {
                return client;
            }
        }

        return {};
    }

    TClientConfig GetClient(const TString& scope) const {
        auto it = ScopeToDatabaseClient.find(scope);
        if (it != ScopeToDatabaseClient.end()) {
            return it->second;
        }
        Y_ABORT_UNLESS(CommonDatabaseClients);
        return CommonDatabaseClients[MultiHash(scope) % CommonDatabaseClients.size()];
    }

    TVector<TClientConfig> CommonDatabaseClients;
    TMap<TString, TClientConfig> ScopeToDatabaseClient;
};

class TCreateDatabaseRequestActor : public NActors::TActorBootstrapped<TCreateDatabaseRequestActor> {
public:
    TCreateDatabaseRequestActor(const std::shared_ptr<TDatabaseClients>& clients, const TActorId& synchronizationServiceActorId, const NFq::NConfig::TComputeConfig& config, TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& request)
        : Scope(request->Get()->Scope)
        , Clients(clients)
        , SynchronizationServiceActorId(synchronizationServiceActorId)
        , Config(config)
        , Request(request)
    {}

    static constexpr char ActorName[] = "FQ_CREATE_DATABASE_REQUEST_ACTOR";

    void Bootstrap() {
        const auto& controlPlane = Config.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
            case NConfig::TYdbComputeControlPlane::kSingle: {
                LOG_T("Scope: " << Scope << " Single control plane mode has been chosen");
                const auto& singleConfig = Config.GetYdb().GetControlPlane().GetSingle();
                *Result.mutable_connection() = singleConfig.GetConnection();
                Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvSynchronizeRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope, singleConfig.GetConnection(), singleConfig.GetWorkloadManagerConfig()});
            }
            break;
            case NConfig::TYdbComputeControlPlane::kCms:
            case NConfig::TYdbComputeControlPlane::kYdbcp:
            LOG_T("Scope: " << Scope << " CMS or YDBCP mode has been chosen");
            Send(NFq::ControlPlaneStorageServiceActorId(), new TEvControlPlaneStorage::TEvDescribeDatabaseRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope});
            break;
        }

        Become(&TCreateDatabaseRequestActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneStorage::TEvDescribeDatabaseResponse, Handle);
        hFunc(TEvYdbCompute::TEvCreateDatabaseResponse, Handle);
        hFunc(TEvYdbCompute::TEvAddDatabaseResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateDatabaseResponse, Handle);
        hFunc(TEvYdbCompute::TEvSynchronizeResponse, Handle);
        hFunc(TEvYdbCompute::TEvCheckDatabaseResponse, Handle);
        hFunc(TEvYdbCompute::TEvInvalidateSynchronizationResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyDatabaseResponse, Handle);
    )

    void Handle(TEvControlPlaneStorage::TEvDescribeDatabaseResponse::TPtr& ev) {
        const auto& issues = ev->Get()->Issues;
        const auto& result = ev->Get()->Record;

        if (issues && issues.back().IssueCode == TIssuesIds::ACCESS_DENIED) {
            LOG_T("Scope: " << Scope << " Couldn't find the information about database in control plane storage for this scope");
            Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvInvalidateSynchronizationRequest(Scope));
            return;
        }

        if (issues) {
            LOG_E("Scope: " << Scope << " Describe database (control plane storage) has been failed. " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }

        Result = result;
        auto lastAccessAtEvent = std::make_unique<TEvControlPlaneStorage::TEvModifyDatabaseRequest>(Request->Get()->CloudId, Scope);
        lastAccessAtEvent->LastAccessAt = TInstant::Now();
        Send(ControlPlaneStorageServiceActorId(), lastAccessAtEvent.release(), 0, UpdateLastAccessAtCookie);
        auto client = Clients->GetClient(Scope, Result.connection().endpoint(), Result.connection().database());
        if (!client) {
            auto issues = NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Couldn't find a database client for scope " << Scope << ". Checking the existence of the database failed after DescribeDatabaseRequest. Please contact internal support"}};
            LOG_E("Scope: " << Scope << " Connection: " << Result.ShortDebugString() << " " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }
        Send(client->DatabasesCacheActorId, std::make_unique<TEvYdbCompute::TEvCheckDatabaseRequest>(Result.connection().database()));
    }

    void Handle(TEvYdbCompute::TEvCheckDatabaseResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Issues) {
            LOG_E("Scope: " << Scope << " The database existence check has been failed. " << response.Issues.ToOneLineString());
            FailedAndPassAway(response.Issues);
            return;
        }

        auto client = ev->Cookie == OnlyDatabaseCreateCookie
            ? Clients->GetClient(Scope, Result.connection().endpoint(), Result.connection().database())
            : Clients->GetClient(Scope);

        if (!client) {
            auto issues = NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Couldn't find a database client for scope " << Scope << ". Checking the existence of the database failed after InvalidateSynchronizationRequest. Please contact internal support"}};
            LOG_E("Scope: " << Scope << " Connection: " << Result.ShortDebugString() << " " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }

        if (response.IsExists) {
            Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvSynchronizeRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope, Result.connection(), client->Config.GetWorkloadManagerConfig()});
        } else {
            auto invalidateSynchronizationEvent = std::make_unique<TEvControlPlaneStorage::TEvModifyDatabaseRequest>(Request->Get()->CloudId, Scope);
            invalidateSynchronizationEvent->Synchronized = false;
            invalidateSynchronizationEvent->WorkloadManagerSynchronized = false;
            Send(ControlPlaneStorageServiceActorId(), invalidateSynchronizationEvent.release(), 0, OnlyDatabaseCreateCookie);
        }
    }

    void Handle(TEvYdbCompute::TEvInvalidateSynchronizationResponse::TPtr& ev) {
        auto client = ev->Cookie == OnlyDatabaseCreateCookie
            ? Clients->GetClient(Scope, Result.connection().endpoint(), Result.connection().database())
            : Clients->GetClient(Scope);
        if (!client) {
            auto issues = NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Couldn't find a database client for scope " << Scope << ". Checking the existence of the database failed after InvalidateSynchronizationRequest. Please contact internal support"}};
            LOG_E("Scope: " << Scope << " Connection: " << Result.ShortDebugString() << " " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }
        FillRequest(Request, client->Config);
        Send(client->ActorId, new TEvYdbCompute::TEvCreateDatabaseRequest{Request->Get()->CloudId, Scope, Request->Get()->BasePath, Request->Get()->Path, client->Config.GetExecutionConnection()}, 0, ev->Cookie);
    }

    void Handle(TEvYdbCompute::TEvAddDatabaseResponse::TPtr& ev) {
        auto client = ev->Cookie == OnlyDatabaseCreateCookie
            ? Clients->GetClient(Scope, Result.connection().endpoint(), Result.connection().database())
            : Clients->GetClient(Scope);

        if (!client) {
            auto issues = NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Couldn't find a database client for scope " << Scope << ". Checking the existence of the database failed after InvalidateSynchronizationRequest. Please contact internal support"}};
            LOG_E("Scope: " << Scope << " Connection: " << Result.ShortDebugString() << " " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }

        if (ev->Cookie == OnlyDatabaseCreateCookie) {
            Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvSynchronizeRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope, Result.connection(), client->Config.GetWorkloadManagerConfig()});
            return;
        }
        Send(ControlPlaneStorageServiceActorId(), new TEvControlPlaneStorage::TEvCreateDatabaseRequest{Request->Get()->CloudId, Scope, Result});
    }

    void Handle(TEvControlPlaneStorage::TEvModifyDatabaseResponse::TPtr& ev) {
        const auto issues = ev->Get()->Issues;
        if (issues) {
            LOG_E("Scope: " << Scope << " The modification of the information about database has been failed. " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }

        if (ev->Cookie == UpdateLastAccessAtCookie) {
            return;
        }

        if (ev->Cookie == InvalidateSynchronizationCooke) {
            Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvInvalidateSynchronizationRequest(Scope));
            return;
        }

        if (ev->Cookie == OnlyDatabaseCreateCookie) {
            Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvInvalidateSynchronizationRequest(Scope), 0, OnlyDatabaseCreateCookie);
            return;
        }
    }

    void Handle(TEvYdbCompute::TEvCreateDatabaseResponse::TPtr& ev) {
        const auto issues = ev->Get()->Issues;
        if (issues) {
            LOG_E("Scope: " << Scope << " CreateDatabaseRequest (compute) has been failed. " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }

        Result = ev->Get()->Result;
        auto client = Clients->GetClient(Scope, Result.connection().endpoint(), Result.connection().database());
        if (!client) {
            auto issues = NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Couldn't find a database client for scope " << Scope << ". Checking the existence of the database failed after CreateDatabaseRequest. Please contact internal support"}};
            LOG_E("Scope: " << Scope << " Connection: " << Result.ShortDebugString() << " " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }
        Send(client->DatabasesCacheActorId, new TEvYdbCompute::TEvAddDatabaseRequest{Result.connection().database()}, 0, ev->Cookie);
    }

    void Handle(TEvControlPlaneStorage::TEvCreateDatabaseResponse::TPtr& ev) {
        const auto issues = ev->Get()->Issues;
        if (issues) {
            LOG_E("Scope: " << Scope << " CreateDatabaseRequest (control plane storage) has been failed. " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }

        auto client = ev->Cookie == OnlyDatabaseCreateCookie
            ? Clients->GetClient(Scope, Result.connection().endpoint(), Result.connection().database())
            : Clients->GetClient(Scope);

        if (!client) {
            auto issues = NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Couldn't find a database client for scope " << Scope << ". Checking the existence of the database failed after CreateDatabaseRequest. Please contact internal support"}};
            LOG_E("Scope: " << Scope << " Connection: " << Result.ShortDebugString() << " " << issues.ToOneLineString());
            FailedAndPassAway(issues);
            return;
        }

        Send(SynchronizationServiceActorId, new TEvYdbCompute::TEvSynchronizeRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope, Result.connection(), client->Config.GetWorkloadManagerConfig()});
    }

    void Handle(TEvYdbCompute::TEvSynchronizeResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Scope: " << Scope << " SynchronizeRequest has been failed. " << response.Issues.ToOneLineString());
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

    void FillRequest(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev, const NConfig::TComputeDatabaseConfig& config) {
        NYdb::NFq::TScope scope(ev.Get()->Get()->Scope);
        ev.Get()->Get()->BasePath = config.GetControlPlaneConnection().GetDatabase();
        const TString databaseName = Config.GetYdb().GetControlPlane().GetDatabasePrefix() + scope.ParseFolder();
        ev.Get()->Get()->Path = config.GetTenant() ? config.GetTenant() + "/" + databaseName: databaseName;
    }

private:
    TString Scope;
    std::shared_ptr<TDatabaseClients> Clients;
    TActorId SynchronizationServiceActorId;
    NFq::NConfig::TComputeConfig Config;
    TEvYdbCompute::TEvCreateDatabaseRequest::TPtr Request;
    FederatedQuery::Internal::ComputeDatabaseInternal Result;

    enum : ui64 {
        InvalidateSynchronizationCooke = 1,
        UpdateLastAccessAtCookie,
        OnlyDatabaseCreateCookie
    };
};

}

class TComputeDatabaseControlPlaneServiceActor : public NActors::TActorBootstrapped<TComputeDatabaseControlPlaneServiceActor> {


public:
    TComputeDatabaseControlPlaneServiceActor(const NFq::NConfig::TComputeConfig& config,
                                             const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
                                             const NConfig::TCommonConfig& commonConfig,
                                             const TSigner::TPtr& signer,
                                             const TYqSharedResources::TPtr& yqSharedResources,
                                             const ::NMonitoring::TDynamicCounterPtr& counters)
        : Config(config)
        , Clients(std::make_shared<TDatabaseClients>())
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
                CreateSingleClientActors(controlPlane.GetSingle());
            break;
            case NConfig::TYdbComputeControlPlane::kCms:
                CreateCmsClientActors(controlPlane.GetCms(), controlPlane.GetDatabasesCacheReloadPeriod());
            break;
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                CreateControlPlaneClientActors(controlPlane.GetYdbcp(), controlPlane.GetDatabasesCacheReloadPeriod());
            break;
        }
        Become(&TComputeDatabaseControlPlaneServiceActor::StateFunc);
    }

    static NGrpcActorClient::TGrpcClientSettings CreateGrpcClientSettings(const NConfig::TYdbStorageConfig& connection) {
        NGrpcActorClient::TGrpcClientSettings settings;
        settings.Endpoint = connection.GetEndpoint();
        settings.EnableSsl = connection.GetUseSsl();
        if (connection.GetCertificateFile()) {
            settings.CertificateRootCA = StripString(TFileInput(connection.GetCertificateFile()).ReadAll());
        }
        return settings;
    }

    static NGrpcActorClient::TGrpcClientSettings CreateGrpcClientSettings(const NConfig::TComputeDatabaseConfig& config) {
        NGrpcActorClient::TGrpcClientSettings settings;
        const auto& connection = config.GetControlPlaneConnection();
        settings.Endpoint = connection.GetEndpoint();
        settings.EnableSsl = connection.GetUseSsl();
        if (connection.GetCertificateFile()) {
            settings.CertificateRootCA = StripString(TFileInput(connection.GetCertificateFile()).ReadAll());
        }
        return settings;
    }

    void CreateSingleClientActors(const NConfig::TYdbComputeControlPlane::TSingle& singleConfig) {
        auto globalLoadConfig = Config.GetYdb().GetLoadControlConfig();
        if (globalLoadConfig.GetEnable()) {
            TActorId clientActor;
            auto monitoringEndpoint = globalLoadConfig.GetMonitoringEndpoint();
            auto credentialsProvider = CredentialsProviderFactory(GetYdbCredentialSettings(singleConfig.GetConnection()))->CreateProvider();
            if (monitoringEndpoint) {
                clientActor = Register(CreateMonitoringRestClientActor(monitoringEndpoint, singleConfig.GetConnection().GetDatabase(), credentialsProvider).release());
            } else {
                clientActor = Register(CreateMonitoringGrpcClientActor(CreateGrpcClientSettings(singleConfig.GetConnection()), credentialsProvider).release());
            }
            MonitoringActorId = Register(CreateDatabaseMonitoringActor(clientActor, globalLoadConfig, Counters).release());
        }
    }

    void CreateCmsClientActors(const NConfig::TYdbComputeControlPlane::TCms& cmsConfig, const TString& databasesCacheReloadPeriod) {
        const auto& mapping = cmsConfig.GetDatabaseMapping();
        auto globalLoadConfig = Config.GetYdb().GetLoadControlConfig();
        for (const auto& config: mapping.GetCommon()) {
            auto databaseCounters = Counters->GetSubgroup("database", config.GetControlPlaneConnection().GetDatabase());
            const auto clientActor = Register(CreateCmsGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider()).release());
            const auto cacheActor = Register(CreateComputeDatabasesCacheActor(clientActor, databasesCacheReloadPeriod, databaseCounters).release());
            TActorId databaseMonitoringActor;
            const NConfig::TLoadControlConfig& loadConfig = config.GetLoadControlConfig().GetEnable()
                ? config.GetLoadControlConfig()
                : globalLoadConfig;
            if (loadConfig.GetEnable()) {
                TActorId clientActor;
                auto monitoringEndpoint = loadConfig.GetMonitoringEndpoint();
                auto credentialsProvider = CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider();
                if (monitoringEndpoint) {
                    clientActor = Register(CreateMonitoringRestClientActor(monitoringEndpoint, config.GetControlPlaneConnection().GetDatabase(), credentialsProvider).release());
                } else {
                    clientActor = Register(CreateMonitoringGrpcClientActor(CreateGrpcClientSettings(config), credentialsProvider).release());
                }
                databaseMonitoringActor = Register(CreateDatabaseMonitoringActor(clientActor, loadConfig, databaseCounters).release());
            }
            Clients->CommonDatabaseClients.push_back({clientActor, config, cacheActor, databaseMonitoringActor});
        }

        Y_ABORT_UNLESS(Clients->CommonDatabaseClients);

        for (const auto& [scope, config]: mapping.GetScopeToComputeDatabase()) {
            auto databaseCounters = Counters->GetSubgroup("database", config.GetControlPlaneConnection().GetDatabase());
            const auto clientActor = Register(CreateCmsGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider()).release());
            const auto cacheActor = Register(CreateComputeDatabasesCacheActor(clientActor, databasesCacheReloadPeriod, databaseCounters).release());
            TActorId databaseMonitoringActor;
            const NConfig::TLoadControlConfig& loadConfig = config.GetLoadControlConfig().GetEnable()
                ? config.GetLoadControlConfig()
                : globalLoadConfig;
            if (loadConfig.GetEnable()) {
                TActorId clientActor;
                auto monitoringEndpoint = loadConfig.GetMonitoringEndpoint();
                auto credentialsProvider = CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider();
                if (monitoringEndpoint) {
                    clientActor = Register(CreateMonitoringRestClientActor(monitoringEndpoint, config.GetControlPlaneConnection().GetDatabase(), credentialsProvider).release());
                } else {
                    clientActor = Register(CreateMonitoringGrpcClientActor(CreateGrpcClientSettings(config), credentialsProvider).release());
                }
                databaseMonitoringActor = Register(CreateDatabaseMonitoringActor(clientActor, loadConfig, databaseCounters).release());
            }
            Clients->ScopeToDatabaseClient[scope] = {clientActor, config, cacheActor, databaseMonitoringActor};
        }
    }

    void CreateControlPlaneClientActors(const NConfig::TYdbComputeControlPlane::TYdbcp& controlPlaneConfig, const TString& databasesCacheReloadPeriod) {
        const auto& mapping = controlPlaneConfig.GetDatabaseMapping();
        for (const auto& config: mapping.GetCommon()) {
            auto databaseCounters = Counters->GetSubgroup("database", config.GetControlPlaneConnection().GetDatabase());
            const auto clientActor = Register(CreateYdbcpGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider()).release());
            const auto cacheActor = Register(CreateComputeDatabasesCacheActor(clientActor, databasesCacheReloadPeriod, databaseCounters).release());
            Clients->CommonDatabaseClients.push_back({clientActor, config, cacheActor, {}});
        }

        Y_ABORT_UNLESS(Clients->CommonDatabaseClients);

        for (const auto& [scope, config]: mapping.GetScopeToComputeDatabase()) {
            auto databaseCounters = Counters->GetSubgroup("database", config.GetControlPlaneConnection().GetDatabase());
            const auto clientActor = Register(CreateYdbcpGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.GetControlPlaneConnection()))->CreateProvider()).release());
            const auto cacheActor = Register(CreateComputeDatabasesCacheActor(clientActor, databasesCacheReloadPeriod, databaseCounters).release());
            Clients->ScopeToDatabaseClient[scope] = {clientActor, config, cacheActor, {}};
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCreateDatabaseRequest, Handle);
        hFunc(TEvYdbCompute::TEvCpuLoadRequest, Handle);
        hFunc(TEvYdbCompute::TEvCpuQuotaRequest, Handle);
        hFunc(TEvYdbCompute::TEvCpuQuotaAdjust, Handle);
    )

    void Handle(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev) {
        Register(new TCreateDatabaseRequestActor(Clients, SynchronizationServiceActorId, Config, ev));
    }

    void Handle(TEvYdbCompute::TEvCpuLoadRequest::TPtr& ev) {
        auto actorId = GetMonitoringActorIdByScope(ev.Get()->Get()->Scope);
        if (actorId != TActorId{}) {
            Send(ev->Forward(actorId));
        } else {
            Send(ev->Sender, new TEvYdbCompute::TEvCpuLoadResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Cluster load monitoring disabled"}}), 0, ev->Cookie);
        }
    }

    void Handle(TEvYdbCompute::TEvCpuQuotaRequest::TPtr& ev) {
        auto actorId = GetMonitoringActorIdByScope(ev.Get()->Get()->Scope);
        if (actorId != TActorId{}) {
            Send(ev->Forward(actorId));
        } else {
            Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(), 0, ev->Cookie);
        }
    }

    void Handle(TEvYdbCompute::TEvCpuQuotaAdjust::TPtr& ev) {
        auto actorId = GetMonitoringActorIdByScope(ev.Get()->Get()->Scope);
        if (actorId != TActorId{}) {
            Send(ev->Forward(actorId));
        }
    }

private:
    TActorId GetMonitoringActorIdByScope(const TString& scope) {
        return Config.GetYdb().GetControlPlane().HasSingle()
            ? MonitoringActorId
            : Clients->GetClient(scope).MonitoringActorId;
    }

    TActorId SynchronizationServiceActorId;
    NFq::NConfig::TComputeConfig Config;
    std::shared_ptr<TDatabaseClients> Clients;
    NConfig::TCommonConfig CommonConfig;
    TSigner::TPtr Signer;
    TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    ::NMonitoring::TDynamicCounterPtr Counters;
    TActorId MonitoringClientActorId;
    TActorId MonitoringActorId;
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
