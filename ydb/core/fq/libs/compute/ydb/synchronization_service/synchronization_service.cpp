#include "synchronization_service.h"


#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/compute/common/utils.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/ydb_schema_query_actor.h>
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


#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [SynchronizationService]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [SynchronizationService]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [SynchronizationService]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [SynchronizationService]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [SynchronizationService]: " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TSynchronizeScopeActor : public NActors::TActorBootstrapped<TSynchronizeScopeActor> {
public:
    TSynchronizeScopeActor(const TActorId& parentActorId,
                           const TString& cloudId,
                           const TString& scope,
                           const NConfig::TCommonConfig& commonConfig,
                           const NConfig::TComputeConfig& computeConfig,
                           const NFq::NConfig::TYdbStorageConfig& connectionConfig,
                           const TSigner::TPtr& signer,
                           const TYqSharedResources::TPtr& yqSharedResources,
                           const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
                           const ::NMonitoring::TDynamicCounterPtr& counters)
        : ParentActorId(parentActorId)
        , CloudId(cloudId)
        , Scope(scope)
        , CommonConfig(commonConfig)
        , ComputeConfig(computeConfig)
        , ConnectionConfig(connectionConfig)
        , Signer(signer)
        , YqSharedResources(yqSharedResources)
        , CredentialsProviderFactory(credentialsProviderFactory)
        , Counters(counters)
    {}

    static constexpr char ActorName[] = "FQ_SYNCHRONIZE_SCOPE_ACTOR";

    void Bootstrap() {
        LOG_I("Start synchronization for the scope " << Scope);
        Client = CreateNewTableClient(ConnectionConfig,
                                      YqSharedResources,
                                      CredentialsProviderFactory);
        Become(&TSynchronizeScopeActor::StateFetchConnectionsFunc);

        const auto& controlPlane = ComputeConfig.GetProto().GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
            case NConfig::TYdbComputeControlPlane::kSingle:
            LOG_I("Start fetch connections stage for the scope (single) " << Scope);
            SendListConnections();
            break;
            case NConfig::TYdbComputeControlPlane::kCms:
            case NConfig::TYdbComputeControlPlane::kYdbcp:
            Send(NFq::ControlPlaneStorageServiceActorId(), new TEvControlPlaneStorage::TEvDescribeDatabaseRequest{CloudId, Scope});
            return;
        }
    }

    STRICT_STFUNC(StateFetchConnectionsFunc,
        hFunc(TEvControlPlaneStorage::TEvListConnectionsResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeDatabaseResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyDatabaseResponse, Handle);
    )

    void Handle(const TEvControlPlaneStorage::TEvDescribeDatabaseResponse::TPtr& ev) {
        const auto& issues = ev.Get()->Get()->Issues;
        if (issues) {
            LOG_E("DescribeDatabaseResponse, scope = " << Scope << " (failed): " << issues.ToOneLineString());
            ReplyErrorAndPassAway(issues, "Error describe a database at the synchronization stage");
            return;
        }
        const auto& result = ev.Get()->Get()->Record;
        if (result.synchronized()) {
            LOG_I("Synchronization has already completed for the scope " << Scope);
            ReplyAndPassAway();
        } else {
            LOG_I("Start fetch connections stage for the scope (cms or ydbcp) " << Scope);
            SendListConnections();
        }
    }

    void Handle(const TEvControlPlaneStorage::TEvListConnectionsResponse::TPtr& ev) {
        const auto& issues = ev.Get()->Get()->Issues;
        if (issues) {
            LOG_E("ListConnectionsResponse, scope = " << Scope << " page token = " << PageToken << " (failed): " << issues.ToOneLineString());
            ReplyErrorAndPassAway(issues, "Error getting a list of connections at the synchronization stage");
            return;
        }

        const auto& result = ev->Get()->Result;
        for (const auto& connection: result.connection()) {
            const auto& id = connection.meta().id();
            LOG_I("Received connection: scope = " << Scope << " , id = " << id << ", type = " << static_cast<int>(connection.content().setting().connection_case()));
            Connections[id] = connection;
        }

        const TString& nextPageToken = result.next_page_token();
        if (nextPageToken) {
            PageToken = nextPageToken;
            SendListConnections();
        } else {
            LOG_I("Start fetch bindings stage for the scope " << Scope);
            Become(&TSynchronizeScopeActor::StateFetchBindingsFunc);
            PageToken = {};
            SendListBindings();
        }
    }

    STRICT_STFUNC(StateFetchBindingsFunc,
        hFunc(TEvControlPlaneStorage::TEvListBindingsResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeBindingResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyDatabaseResponse, Handle);
    )

    void Handle(const TEvControlPlaneStorage::TEvListBindingsResponse::TPtr& ev) {
        const auto& issues = ev.Get()->Get()->Issues;
        if (issues) {
            LOG_E("ListBindingsResponse, scope = " << Scope << " page token = " << PageToken << " (failed): " << issues.ToOneLineString());
            ReplyErrorAndPassAway(issues, "Error getting a list of bindings at the synchronization stage");
            return;
        }
    
        const auto& result = ev->Get()->Result;
        for (const auto& binding: result.binding()) {
            const auto& id = binding.meta().id();
            LOG_I("Received binding id: scope = " << Scope << " , id = " << id << ", type = " << FederatedQuery::BindingSetting::BindingType_Name(binding.type()));
            
            BindingIds.insert(binding.meta().id());
        }

        const TString& nextPageToken = result.next_page_token();
        if (nextPageToken) {
            PageToken = nextPageToken;
            SendListBindings();
        } else {
            LOG_I("Start describe bindings stage for the scope " << Scope);
            PageToken = {};
            SendDescribeBindings();
        }
    }

    void Handle(const TEvControlPlaneStorage::TEvDescribeBindingResponse::TPtr& ev) {
        const auto& issues = ev.Get()->Get()->Issues;
        if (issues) {
            LOG_E("DescribeBindingResponse, scope = " << Scope << " (failed): " << issues.ToOneLineString());
            ReplyErrorAndPassAway(issues, "Error getting a describe of binding at the synchronization stage");
            return;
        }
        const auto& result = ev->Get()->Result;
        const auto& id = result.binding().meta().id();
        LOG_I("Received binding: scope = " << Scope << " , id = " << id << ", type = " << static_cast<int>(result.binding().content().setting().binding_case()));
        Bindings[result.binding().meta().id()] = result.binding();

        if (BindingIds.size() == Bindings.size()) {
            LOG_I("Start create external data sources stage for the scope " << Scope);
            Become(&TSynchronizeScopeActor::StateCreateExternalDataSourcesFunc);
            CreateExternalDataSources();
        }
    }

    STRICT_STFUNC(StateCreateExternalDataSourcesFunc,
        hFunc(TEvControlPlaneProxy::TEvCreateConnectionRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvCreateConnectionResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyDatabaseResponse, Handle);
    )

    void Handle(const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr&) {
        CreatedConnections++;
        if (CreatedConnections == Connections.size()) {
            LOG_I("Start create external tables stage for the scope " << Scope);
            Become(&TSynchronizeScopeActor::StateCreateExternalTablesFunc);
            CreateExternalTables();
        }
    }

    void Handle(const TEvControlPlaneProxy::TEvCreateConnectionResponse::TPtr& ev) {
        LOG_E("Create external data source response (error): " << CreatedConnections << " of " << Connections.size() << ", issues = " << ev.Get()->Get()->Issues.ToOneLineString());
        ReplyErrorAndPassAway(ev.Get()->Get()->Issues, "Сonnection creation error at the synchronization stage");
    }

    STRICT_STFUNC(StateCreateExternalTablesFunc,
        hFunc(TEvControlPlaneProxy::TEvCreateBindingRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvCreateBindingResponse, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyDatabaseResponse, Handle);
    )

    void Handle(const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr&) {
        CreatedBindings++;
        if (CreatedBindings == Bindings.size()) {
            SendFinalModifyDatabase();
        }
    }

    void Handle(const TEvControlPlaneProxy::TEvCreateBindingResponse::TPtr& ev) {
        LOG_E("Create external table response (error): " << CreatedBindings << " of " << Bindings.size() << ", issues = " << ev.Get()->Get()->Issues.ToOneLineString());
        ReplyErrorAndPassAway(ev.Get()->Get()->Issues, "Binding creation error at the synchronization stage");
    }

    void Handle(const TEvControlPlaneStorage::TEvModifyDatabaseResponse::TPtr& ev) {
        const auto& issues = ev->Get()->Issues;
        if (ev->Get()->Issues) {
            LOG_E("ModifyDatabaseResponse, scope = " << Scope << " (failed): " << issues.ToOneLineString());
            ReplyErrorAndPassAway(issues, "Error modify a database at the synchronization stage");
            return;
        }

        LOG_I("Synchronization has already completed for the scope (cms or ydbcp) " << Scope);
        ReplyAndPassAway();
    }

private:
    std::shared_ptr<NYdb::NTable::TTableClient> CreateNewTableClient(const NFq::NConfig::TYdbStorageConfig& connection,
                                                                     const TYqSharedResources::TPtr& yqSharedResources,
                                                                     const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
        return ::NFq::CreateNewTableClient(Scope, ComputeConfig, connection, yqSharedResources, credentialsProviderFactory);
    }

    NYql::TIssues ValidateSources() {
        NYql::TIssues issues;
        issues.AddIssues(ValidateAuth());
        issues.AddIssues(ValidateNameUniqueness());
        return issues;
    }

    NYql::TIssues ValidateNameUniqueness() {
        TMap<TString, TString> names;
        for (const auto& [_, connection]: Connections) {
            const auto& meta = connection.meta();
            const auto& content = connection.content();
            auto it = names.find(content.name());
            if (it != names.end()) {
                return NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Two sources have the same name: connection id = " << meta.id() << " and " << it->second}};
            }
            names[content.name()] = TStringBuilder{} << "connection id = " << meta.id();
        }

        for (const auto& [_, binding]: Bindings) {
            const auto& meta = binding.meta();
            const auto& content = binding.content();
            auto it = names.find(content.name());
            if (it != names.end()) {
                return NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Two sources have the same name: binding id = " << meta.id() << " and " << it->second}};
            }
            names[content.name()] = TStringBuilder{} << "binding id = " << meta.id();
        }
        return {};
    }

    NYql::TIssues ValidateAuth() {
        for (const auto& [_, connection]: Connections) {
            const auto& auth = GetAuth(connection);
            const auto& meta = connection.meta();
            const auto& content = connection.content();
            switch (auth.identity_case()) {
            case FederatedQuery::IamAuth::kNone:
            case FederatedQuery::IamAuth::kServiceAccount:
                return {};
            case FederatedQuery::IamAuth::kCurrentIam:
            case FederatedQuery::IamAuth::IDENTITY_NOT_SET:
                return NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Unsupported auth method for connection id " << meta.id() << " with name " << content.name() << " at the synchronization stage"}};
            }
        }
        return {};
    }

    FederatedQuery::IamAuth GetAuth(const FederatedQuery::Connection& connection) {
        switch (connection.content().setting().connection_case()) {
        case FederatedQuery::ConnectionSetting::kObjectStorage:
            return connection.content().setting().object_storage().auth();
        case FederatedQuery::ConnectionSetting::kYdbDatabase:
            return connection.content().setting().ydb_database().auth();
        case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            return connection.content().setting().clickhouse_cluster().auth();
        case FederatedQuery::ConnectionSetting::kDataStreams:
            return connection.content().setting().data_streams().auth();
        case FederatedQuery::ConnectionSetting::kMonitoring:
            return connection.content().setting().monitoring().auth();
        case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
            return connection.content().setting().postgresql_cluster().auth();
        case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
            return FederatedQuery::IamAuth{};
        }
    }

    void ExcludeUnsupportedExternalDataSources() {
        TVector<TString> excludeIds;
        for (const auto& [_, connection]: Connections) {
            const auto& meta = connection.meta();
            const auto& content = connection.content();
            const auto& setting = content.setting();

            if (!ComputeConfig.IsConnectionCaseEnabled(setting.connection_case())) {
                LOG_I("Exclude connection by type: scope = " << Scope << " , id = " << meta.id() << ", type = " << static_cast<int>(setting.connection_case()));
                excludeIds.push_back(meta.id());
            }

            switch (content.acl().visibility()) {
                case FederatedQuery::Acl::SCOPE:
                break;
                case FederatedQuery::Acl::VISIBILITY_UNSPECIFIED:
                case FederatedQuery::Acl::PRIVATE:
                case FederatedQuery::Acl_Visibility_Acl_Visibility_INT_MIN_SENTINEL_DO_NOT_USE_:
                case FederatedQuery::Acl_Visibility_Acl_Visibility_INT_MAX_SENTINEL_DO_NOT_USE_:
                LOG_I("Exclude connection by visibility: scope = " << Scope << " , id = " << meta.id() << ", visibility = " << FederatedQuery::Acl::Visibility_Name(content.acl().visibility()));
                excludeIds.push_back(meta.id());
            }
        }
        for (const auto& excludeId: excludeIds) {
            Connections.erase(excludeId);
        }
    }

    void ExcludeUnsupportedExternalTables() {
        TVector<TString> excludeIds;
        for (const auto& [_, binding]: Bindings) {
            const auto& meta = binding.meta();
            const auto& content = binding.content();
            const auto& setting = content.setting();
            switch (setting.binding_case()) {
            case FederatedQuery::BindingSetting::kObjectStorage:
            break;
            case FederatedQuery::BindingSetting::kDataStreams:
            case FederatedQuery::BindingSetting::BINDING_NOT_SET:
                LOG_I("Exclude binding by type: scope = " << Scope << " , id = " << meta.id() << ", type = " << static_cast<int>(setting.binding_case()));
                excludeIds.push_back(meta.id());
            break;
            }

            switch (content.acl().visibility()) {
                case FederatedQuery::Acl::SCOPE:
                break;
                case FederatedQuery::Acl::VISIBILITY_UNSPECIFIED:
                case FederatedQuery::Acl::PRIVATE:
                case FederatedQuery::Acl_Visibility_Acl_Visibility_INT_MIN_SENTINEL_DO_NOT_USE_:
                case FederatedQuery::Acl_Visibility_Acl_Visibility_INT_MAX_SENTINEL_DO_NOT_USE_:
                LOG_I("Exclude binding by visibility: scope = " << Scope << " , id = " << meta.id() << ", visibility = " << FederatedQuery::Acl::Visibility_Name(content.acl().visibility()));
                excludeIds.push_back(meta.id());
            }
        }
        for (const auto& excludeId: excludeIds) {
            Bindings.erase(excludeId);
            BindingIds.erase(excludeId);
        }
    }

    void CreateExternalDataSources() {
        ExcludeUnsupportedExternalDataSources();
        ExcludeUnsupportedExternalTables();
        auto issues = ValidateSources();
        if (issues) {
            LOG_I("Validate sources (error): scope = " << Scope << " " << issues.ToOneLineString());
            ReplyErrorAndPassAway(issues);
            return;
        }

        for (const auto& connection: Connections) {
            FederatedQuery::CreateConnectionRequest proto;
            *proto.mutable_content() = connection.second.content();
            TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request = 
                (NActors::TEventHandle<TEvControlPlaneProxy::TEvCreateConnectionRequest>*)
                new IEventHandle(SelfId(), SelfId(), new TEvControlPlaneProxy::TEvCreateConnectionRequest{{}, proto, {}, {}, {}});

            request.Get()->Get()->YDBClient = Client;
            
            Register(NFq::NPrivate::MakeCreateConnectionActor(
                SelfId(),
                request,
                TDuration::Seconds(30),
                Counters,
                CommonConfig,
                Signer,
                true
            ));
        }
        if (Connections.empty()) {
            SendFinalModifyDatabase();
        }
    }

    void CreateExternalTables() {
        for (const auto& binding: Bindings) {
            FederatedQuery::CreateBindingRequest proto;
            *proto.mutable_content() = binding.second.content();
            TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr request = 
                (NActors::TEventHandle<TEvControlPlaneProxy::TEvCreateBindingRequest>*)
                new IEventHandle(SelfId(), SelfId(), new TEvControlPlaneProxy::TEvCreateBindingRequest{{}, proto, {}, {}, {}});
            
            request.Get()->Get()->YDBClient = Client;
            auto it = Connections.find(binding.second.content().connection_id());
            if (it == Connections.end()) {
                ReplyErrorAndPassAway(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Can't find conection id = " << binding.second.content().connection_id()}});
                return;
            }
            request.Get()->Get()->ConnectionName = it->second.content().name();

            Register(NFq::NPrivate::MakeCreateBindingActor(
                SelfId(),
                request,
                TDuration::Seconds(30),
                Counters,
                true
            ));
        }

        if (Bindings.empty()) {
            SendFinalModifyDatabase();
        }
    }

    void ReplyAndPassAway() {
        Send(ParentActorId, new TEvYdbCompute::TEvSynchronizeResponse{Scope});
        PassAway();
    }

    void ReplyErrorAndPassAway(NYql::TIssues issues, const TString& errorMessage = {}) {
        if (errorMessage) {
            issues.AddIssue(errorMessage);
        }
        Send(ParentActorId, new TEvYdbCompute::TEvSynchronizeResponse{Scope, issues, NYdb::EStatus::GENERIC_ERROR});
        PassAway();
    }

    void SendListConnections() const {
        LOG_T("Send list connections: scope = " << Scope << ", page token = " << PageToken);
        auto request = CreateListRequest<FederatedQuery::ListConnectionsRequest>(PageToken);
        TPermissions permissions = CreateSuperUserPermissions();
        std::unique_ptr<TEvControlPlaneStorage::TEvListConnectionsRequest> event{new TEvControlPlaneStorage::TEvListConnectionsRequest{
                Scope, request, "internal@user", "internal@token", {}, 
                permissions, {}, {}, {}
            }};
        Send(ControlPlaneStorageServiceActorId(), event.release());
    }

    void SendListBindings() const {
        LOG_T("Send list bindings: scope = " << Scope << ", page token = " << PageToken);
        auto request = CreateListRequest<FederatedQuery::ListBindingsRequest>(PageToken);
        TPermissions permissions = CreateSuperUserPermissions();
        std::unique_ptr<TEvControlPlaneStorage::TEvListBindingsRequest> event{new TEvControlPlaneStorage::TEvListBindingsRequest{
                Scope, request, "internal@user", "internal@token", {}, 
                permissions, {}, {}, {}
            }};
        Send(ControlPlaneStorageServiceActorId(), event.release());
    }

    void SendDescribeBindings() {
        for (const auto& bindingId: BindingIds) {
            SendDescribeBinding(bindingId);
        }

        if (BindingIds.empty()) {
            LOG_I("Start create external data sources stage for the scope (bindigns list is empty) " << Scope);
            Become(&TSynchronizeScopeActor::StateCreateExternalDataSourcesFunc);
            CreateExternalDataSources();
        }
    }

    void SendDescribeBinding(const TString& bindingId) const {
        LOG_T("Send describe binding: scope = " << Scope << ", binding id = " << bindingId);
        FederatedQuery::DescribeBindingRequest request;
        request.set_binding_id(bindingId);
        TPermissions permissions = CreateSuperUserPermissions();
        std::unique_ptr<TEvControlPlaneStorage::TEvDescribeBindingRequest> event{new TEvControlPlaneStorage::TEvDescribeBindingRequest{
                Scope, request, "internal@user", "internal@token", {}, 
                permissions, {}, {}, {}
            }};
        Send(ControlPlaneStorageServiceActorId(), event.release());
    }

    void SendFinalModifyDatabase() {
        const auto& controlPlane = ComputeConfig.GetProto().GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
            case NConfig::TYdbComputeControlPlane::kSingle:
            LOG_I("Synchronization has already completed for the scope (single) " << Scope);
            ReplyAndPassAway();
            break;
            case NConfig::TYdbComputeControlPlane::kCms:
            case NConfig::TYdbComputeControlPlane::kYdbcp:
            std::unique_ptr<TEvControlPlaneStorage::TEvModifyDatabaseRequest> event{new TEvControlPlaneStorage::TEvModifyDatabaseRequest{CloudId, Scope}};
            event->Synchronized = true;
            Send(ControlPlaneStorageServiceActorId(), event.release());
            return;
        }
    }

    TPermissions CreateSuperUserPermissions() const {
        TPermissions permissions;
        permissions.SetAll();
        return permissions;
    }

    template<typename T>
    T CreateListRequest(const TString& pageToken) const {
        T request;
        request.set_page_token(pageToken);
        request.set_limit(10);
        return request;
    }

private:
    TActorId ParentActorId;
    TString CloudId;
    TString Scope;
    NConfig::TCommonConfig CommonConfig;
    NFq::TComputeConfig ComputeConfig;
    NFq::NConfig::TYdbStorageConfig ConnectionConfig;
    TSigner::TPtr Signer;
    TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;

    TString PageToken;
    TSet<TString> BindingIds;
    uint64_t CreatedConnections = 0;
    uint64_t CreatedBindings = 0;
    TMap<TString, FederatedQuery::Connection> Connections;
    TMap<TString, FederatedQuery::Binding> Bindings;
    NFq::NPrivate::TCounters Counters;
    std::shared_ptr<NYdb::NTable::TTableClient> Client;
};

class TSynchronizatinServiceActor : public NActors::TActorBootstrapped<TSynchronizatinServiceActor> {
    enum class EScopeStatus {
        IN_PROGRESS = 0,
        SYNCHRONIZED = 1
    };

    struct TScopeState {
        EScopeStatus Status;
        TVector<TEvYdbCompute::TEvSynchronizeRequest::TPtr> Requests;
    };

public:
    TSynchronizatinServiceActor(const NConfig::TCommonConfig& commonConfig,
                                const NConfig::TComputeConfig& computeConfig,
                                const TSigner::TPtr& signer,
                                const TYqSharedResources::TPtr& yqSharedResources,
                                const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
                                const ::NMonitoring::TDynamicCounterPtr& counters)
        : CommonConfig(commonConfig)
        , ComputeConfig(computeConfig)
        , Signer(signer)
        , YqSharedResources(yqSharedResources)
        , CredentialsProviderFactory(credentialsProviderFactory)
        , Counters(counters)
    {}

    static constexpr char ActorName[] = "FQ_SYNCHRONIZATION_SERVICE_ACTOR";

    void Bootstrap() {
        Become(&TSynchronizatinServiceActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvSynchronizeRequest, Handle);
        hFunc(TEvYdbCompute::TEvSynchronizeResponse, Handle);
    )

    void Handle(TEvYdbCompute::TEvSynchronizeRequest::TPtr& ev) {
        const TString& cloudId = ev->Get()->CloudId;
        const TString& scope = ev->Get()->Scope;
        if (!ComputeConfig.GetYdb().GetSynchronizationService().GetEnable()) {
            Send(ev->Sender, new TEvYdbCompute::TEvSynchronizeResponse{scope});
            return;
        }

        const NFq::NConfig::TYdbStorageConfig& connectionConfig = ev->Get()->ConnectionConfig;
        auto it = Cache.find(scope);
        if (it == Cache.end()) {
            auto& item = Cache[scope];
            item.Status = EScopeStatus::IN_PROGRESS;
            item.Requests.push_back(ev);
            Register(new TSynchronizeScopeActor{SelfId(), cloudId, scope, CommonConfig, ComputeConfig, connectionConfig, Signer, YqSharedResources, CredentialsProviderFactory,  Counters});
            return;
        }

        switch (it->second.Status) {
            case EScopeStatus::SYNCHRONIZED: {
                Send(ev->Sender, new TEvYdbCompute::TEvSynchronizeResponse{scope});
                break;
            }
            case EScopeStatus::IN_PROGRESS: {
                it->second.Requests.push_back(ev);
                break;
            }
        }
    }

    void Handle(TEvYdbCompute::TEvSynchronizeResponse::TPtr& ev) {
        auto it = Cache.find(ev->Get()->Scope);
        if (it == Cache.end()) {
            LOG_E("Response: not found for scope " << ev->Get()->Scope);
            return;
        }
    
        for (const auto& request: it->second.Requests) {
            Send(request->Sender, new TEvYdbCompute::TEvSynchronizeResponse{ev->Get()->Scope, ev->Get()->Issues, ev->Get()->Status});
        }

        it->second.Requests.clear();

        if (ev->Get()->Status == NYdb::EStatus::SUCCESS) {
            it->second.Status = EScopeStatus::SYNCHRONIZED;
        } else {
            Cache.erase(it);
        }
    }

private:
    TMap<TString, TScopeState> Cache;
    NConfig::TCommonConfig CommonConfig;
    NConfig::TComputeConfig ComputeConfig;
    TSigner::TPtr Signer;
    TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    ::NMonitoring::TDynamicCounterPtr Counters;
};

std::unique_ptr<NActors::IActor> CreateSynchronizationServiceActor(const NConfig::TCommonConfig& commonConfig,
                                                                   const NConfig::TComputeConfig& computeConfig,
                                                                   const TSigner::TPtr& signer,
                                                                   const TYqSharedResources::TPtr& yqSharedResources,
                                                                   const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
                                                                   const ::NMonitoring::TDynamicCounterPtr& counters) {
    return std::make_unique<TSynchronizatinServiceActor>(commonConfig, computeConfig, signer, yqSharedResources, credentialsProviderFactory, counters);
}

}
