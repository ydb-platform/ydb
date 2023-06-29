#include "compute_database_control_plane_service.h"

#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/public/lib/fq/scope.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_query/client.h>
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
    TCreateDatabaseRequestActor(const TActorId& databaseClientActorId, TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& request)
        : DatabaseClientActorId(databaseClientActorId)
        , Request(request)
    {}

    static constexpr char ActorName[] = "FQ_CREATE_DATABASE_REQUEST_ACTOR";

    void Bootstrap() {
        Send(NFq::ControlPlaneStorageServiceActorId(), new TEvControlPlaneStorage::TEvDescribeDatabaseRequest{Request.Get()->Get()->CloudId, Request.Get()->Get()->Scope});
        Become(&TCreateDatabaseRequestActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneStorage::TEvDescribeDatabaseResponse, Handle);
        hFunc(TEvYdbCompute::TEvCreateDatabaseResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateDatabaseResponse, Handle);
    )

    void Handle(TEvControlPlaneStorage::TEvDescribeDatabaseResponse::TPtr& ev) {
        const auto issues = ev->Get()->Issues;
        const auto result = ev->Get()->Record;

        if (issues && issues.back().IssueCode == TIssuesIds::ACCESS_DENIED) {
            Send(DatabaseClientActorId, new TEvYdbCompute::TEvCreateDatabaseRequest{Request->Get()->CloudId, Request->Get()->Scope, Request->Get()->BasePath, Request->Get()->Path});
            return;
        }

        if (issues) {
            FailedAndPassAway(issues);
            return;
        }

        Send(Request->Sender, new TEvYdbCompute::TEvCreateDatabaseResponse{result});
        PassAway();
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

        Send(Request->Sender, new TEvYdbCompute::TEvCreateDatabaseResponse{Result});
        PassAway();
    }

    void FailedAndPassAway(const NYql::TIssues& issues) {
        Send(Request->Sender, new TEvYdbCompute::TEvCreateDatabaseResponse{issues});
        PassAway();
    }

    TActorId DatabaseClientActorId;
    TEvYdbCompute::TEvCreateDatabaseRequest::TPtr Request;
    FederatedQuery::Internal::ComputeDatabaseInternal Result;
};

class TComputeDatabaseControlPlaneServiceActor : public NActors::TActorBootstrapped<TComputeDatabaseControlPlaneServiceActor> {
    struct TClientConfig {
        TActorId ActorId;
        NConfig::TComputeDatabaseConfig Config;
    };

public:
    TComputeDatabaseControlPlaneServiceActor(const NFq::NConfig::TComputeConfig& config, const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
        : Config(config)
        , CredentialsProviderFactory(credentialsProviderFactory)
    {}

    static constexpr char ActorName[] = "FQ_COMPUTE_DATABASE_SERVICE_ACTOR";

    void Bootstrap() {
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
        const auto& connection = config.GetConnection();
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
            CommonDatabaseClients.push_back({Register(CreateCmsGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.connection()))->CreateProvider()).release()), config});
        }

        Y_VERIFY(CommonDatabaseClients);

        for (const auto& [scope, config]: mapping.GetScopeToComputeDatabase()) {
            ScopeToDatabaseClient[scope] = {Register(CreateCmsGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.connection()))->CreateProvider()).release()), config};
        }
    }

    void CreateControlPlaneClientActors(const NConfig::TYdbComputeControlPlane::TYdbcp& controlPlaneConfig) {
        const auto& mapping = controlPlaneConfig.GetDatabaseMapping();
        for (const auto& config: mapping.GetCommon()) {
            CommonDatabaseClients.push_back({Register(CreateYdbcpGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.connection()))->CreateProvider()).release()), config});
        }

        Y_VERIFY(CommonDatabaseClients);

        for (const auto& [scope, config]: mapping.GetScopeToComputeDatabase()) {
            ScopeToDatabaseClient[scope] = {Register(CreateYdbcpGrpcClientActor(CreateGrpcClientSettings(config), CredentialsProviderFactory(GetYdbCredentialSettings(config.connection()))->CreateProvider()).release()), config};
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCreateDatabaseRequest, Handle);
    )

    void Handle(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev) {
        if (Config.GetYdb().GetControlPlane().HasSingle()) {
            FederatedQuery::Internal::ComputeDatabaseInternal result;
            *result.mutable_connection() = Config.GetYdb().GetControlPlane().GetSingle().GetConnection();
            Send(ev->Sender, new TEvYdbCompute::TEvCreateDatabaseResponse(result));
            return;
        }

        const auto& scope = ev->Get()->Scope;
        auto it = ScopeToDatabaseClient.find(scope);
        if (it != ScopeToDatabaseClient.end()) {
            FillRequest(ev, it->second.Config);
            Register(new TCreateDatabaseRequestActor(it->second.ActorId, ev));
            return;
        }
        const auto& clientConfig = CommonDatabaseClients[MultiHash(scope) % CommonDatabaseClients.size()];
        FillRequest(ev, clientConfig.Config);
        Register(new TCreateDatabaseRequestActor(clientConfig.ActorId, ev));
    }

    void FillRequest(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev, const NConfig::TComputeDatabaseConfig& config) {
        NYdb::NFq::TScope scope(ev.Get()->Get()->Scope);
        ev.Get()->Get()->BasePath = config.GetConnection().GetDatabase();
        ev.Get()->Get()->Path = config.GetTenant() ? config.GetTenant() + "/" + scope.ParseFolder() : scope.ParseFolder();
    }

private:
    NFq::NConfig::TComputeConfig Config;
    TVector<TClientConfig> CommonDatabaseClients;
    TMap<TString, TClientConfig> ScopeToDatabaseClient;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
};

std::unique_ptr<NActors::IActor> CreateComputeDatabaseControlPlaneServiceActor(const NFq::NConfig::TComputeConfig& config, const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    return std::make_unique<TComputeDatabaseControlPlaneServiceActor>(config, credentialsProviderFactory);
}

NActors::TActorId ComputeDatabaseControlPlaneServiceActorId() {
    constexpr TStringBuf name = "COMDBSRV";
    return NActors::TActorId(0, name);
}

}
