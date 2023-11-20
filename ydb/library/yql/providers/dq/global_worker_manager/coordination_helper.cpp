#include "coordination_helper.h"
#include "global_worker_manager.h"
#include "service_node_pinger.h"

#include <ydb/library/yql/providers/dq/actors/yt/nodeid_assigner.h>
#include <ydb/library/yql/providers/dq/actors/yt/nodeid_cleaner.h>
#include <ydb/library/yql/providers/dq/actors/yt/worker_registrator.h>
#include <ydb/library/yql/providers/dq/actors/yt/lock.h>
#include <ydb/library/yql/providers/dq/actors/yt/yt_wrapper.h>
#include <ydb/library/yql/providers/dq/actors/dummy_lock.h>
#include <ydb/library/yql/providers/dq/service/interconnect_helpers.h>
#include <ydb/library/yql/providers/dq/task_runner/file_cache.h>
#include <ydb/library/yql/providers/dq/runtime/runtime_data.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/fluent.h>

#include <util/system/getpid.h>
#include <util/system/env.h>

namespace NYql {

using namespace NActors;

class TCoordinationHelper: public ICoordinationHelper
{
public:
    TCoordinationHelper(
        const NProto::TDqConfig::TYtCoordinator& config,
        const NProto::TDqConfig::TScheduler& schedulerConfig,
        const TString& role,
        ui16 interconnectPort,
        const TString& hostName,
        const TString& ip)
        : Config(config), SchedulerConfig(schedulerConfig)
        , Role(role)
        , Host(hostName)
        , Ip(ip)
        , InterconnectPort(interconnectPort)
    { }

    ui32 GetNodeId() override
    {
        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));
        return NodeId;
    }

    ui32 GetNodeId(
        const TMaybe<ui32> nodeId,
        const TMaybe<TString>& grpcPort,
        ui32 minNodeId,
        ui32 maxNodeId,
        const THashMap<TString, TString>& attributes) override
    {
        if (grpcPort) {
            GrpcPort = *grpcPort;
        }

        if (nodeId) {
            NodeId = *nodeId;
        }

        if (NodeId != static_cast<ui32>(-1)) {
            return NodeId;
        }

        NYql::TAssignNodeIdOptions options;

        options.ClusterName = Config.GetClusterName();
        options.User = Config.GetUser();
        options.Token = Config.GetToken();
        options.Prefix = Config.GetPrefix();
        options.Role = Role;
        options.Attributes[NCommonAttrs::ROLE_ATTR] = Role;
        if (grpcPort) {
            options.Attributes[NCommonAttrs::GRPCPORT_ATTR] = *grpcPort;
        }
        options.Attributes[NCommonAttrs::INTERCONNECTPORT_ATTR] = ToString(InterconnectPort);
        options.Attributes[NCommonAttrs::HOSTNAME_ATTR] = Host;
        options.Attributes[NCommonAttrs::REVISION_ATTR] = GetRevision();

        options.NodeName = Host + ":" + ToString(GetPID()) + ":" + ToString(InterconnectPort);
        options.NodeId = nodeId;
        options.MinNodeId = minNodeId;
        options.MaxNodeId = maxNodeId;

        NodeName = options.NodeName;

        for (const auto& [k, v]: attributes) {
            options.Attributes[k] = v;
        }

        NodeId = AssignNodeId(options);
        return NodeId;
    }

    NActors::IActor* CreateLockOnCluster(NActors::TActorId ytWrapper, const TString& prefix, const TString& lockName, bool temporary) override {
        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));

        auto attributes = NYT::BuildYsonNodeFluently()
            .BeginMap()
                .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(NodeId)
                .Item(NCommonAttrs::HOSTNAME_ATTR).Value(GetHostname())
                .Item(NCommonAttrs::GRPCPORT_ATTR).Value(GrpcPort) // optional
                .Item(NCommonAttrs::UPLOAD_EXECUTABLE_ATTR).Value("true") // compat
            .EndMap();

        return CreateYtLock(
                ytWrapper,
                prefix + "/locks",
                lockName,
                NYT::NodeToYsonString(attributes),
                temporary);
    }

    NActors::IActor* CreateLock(const TString& lockName, bool temporary) override {
        return CreateLockOnCluster(GetWrapper(), Config.GetPrefix(), lockName, temporary);
    }

    void StartRegistrator(NActors::TActorSystem* actorSystem) override {
        TWorkerRegistratorOptions wro;
        wro.NodeName = NodeName;
        wro.Prefix = Config.GetPrefix() + "/" + Role;
        Register(actorSystem, CreateWorkerRegistrator(GetWrapper(actorSystem), wro));
    }

    void StartCleaner(NActors::TActorSystem* actorSystem, const TMaybe<TString>& role) override {
        TNodeIdCleanerOptions co;
        co.Prefix = Config.GetPrefix() + "/" + role.GetOrElse(Role);
        Register(actorSystem, CreateNodeIdCleaner(GetWrapper(actorSystem), co));
    }

    TString GetHostname() override {
        return Host;
    }

    TString GetIp() override {
        return Ip;
    }

    IServiceNodeResolver::TPtr CreateServiceNodeResolver(
        NActors::TActorSystem* actorSystem,
        const TVector<TString>& hostPortPairs) override
    {
        if (!hostPortPairs.empty()) {
            return CreateStaticResolver(hostPortPairs);
        } else {
            TDynamicResolverOptions options;
            options.YtWrapper = GetWrapper(actorSystem);
            options.Prefix = Config.GetPrefix() + "/service_node";
            return CreateDynamicResolver(actorSystem, options);
        }
    }

    void StartGlobalWorker(NActors::TActorSystem* actorSystem, const TVector<TResourceManagerOptions>& resourceUploaderOptions, IMetricsRegistryPtr metricsRegistry) override
    {
        if (Config.GetLockType() != "dummy") {
            GetWrapper();
        }
        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));
        auto actorId = Register(actorSystem, CreateGlobalWorkerManager(this, resourceUploaderOptions, std::move(metricsRegistry), SchedulerConfig));
        actorSystem->RegisterLocalService(NDqs::MakeWorkerManagerActorID(NodeId), actorId);
    }

    const NProto::TDqConfig::TYtCoordinator& GetConfig() override {
        return Config;
    }

    const NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem, const TString& clusterName, const TString& user, const TString& token) override {
        auto key = std::make_tuple(clusterName, user, token);
        auto guard = Guard(Mutex);
        auto it = Yt.find(key);
        if (it != Yt.end()) {
            return it->second;
        } else {
            auto client = GetYtClient(clusterName, user, token);
            auto wrapper = CreateYtWrapper(client);
            auto actorId = Register(actorSystem, wrapper);
            Yt.emplace(key, actorId);
            return actorId;
        }
    }

    const NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem) override {
        return GetWrapper(actorSystem, Config.GetClusterName(), Config.GetUser(), Config.GetToken());
    }

    const NActors::TActorId GetWrapper() override {
        auto key = std::make_tuple(Config.GetClusterName(), Config.GetUser(), Config.GetToken());
        auto guard = Guard(Mutex);
        auto it = Yt.find(key);
        Y_ABORT_UNLESS(it != Yt.end());
        return it->second;
    }

    NActors::IActor* CreateServiceNodePinger(const IServiceNodeResolver::TPtr& ptr, const TResourceManagerOptions& rmOptions, const THashMap<TString, TString>& attributes) override {
        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));
        return ::NYql::CreateServiceNodePinger(NodeId, Ip, InterconnectPort, Role, attributes, ptr, this, rmOptions);
    }

    TWorkerRuntimeData* GetRuntimeData() override {
        return &RuntimeData;
    }

    void Stop(NActors::TActorSystem* actorSystem) override {
        for (auto id : Children) {
            actorSystem->Send(id, new TEvents::TEvPoison());
        }
        Children.clear();
    }

    TString GetRevision() override {
        if (Config.HasRevision()) {
            return Config.GetRevision();
        } else {
            return GetProgramCommitId();
        }
    }

protected:
    TActorId Register(TActorSystem* actorSystem, IActor* actor) {
        auto id = actorSystem->Register(actor);
        Children.push_back(id);
        return id;
    }

    NYT::NApi::IClientPtr GetYtClient(const TString& clusterName, const TString& user, const TString& token)
    {
        NYT::NApi::NRpcProxy::TConnectionConfigPtr config = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
        config->RequestCodec = NYT::NCompression::ECodec::Lz4;
        config->ClusterUrl = clusterName;
        config->ConnectionType = NYT::NApi::EConnectionType::Rpc;

        auto connection = NYT::NApi::NRpcProxy::CreateConnection(config);

        NYT::NApi::TClientOptions options;
        options.User = user;
        options.Token = token;

        auto client = connection->CreateClient(options);
        return client;
    }

    NYT::NApi::IClientPtr GetYtClient()
    {
        return GetYtClient(Config.GetClusterName(), Config.GetUser(), Config.GetToken());
    }

    const NProto::TDqConfig::TYtCoordinator Config;
    const NProto::TDqConfig::TScheduler SchedulerConfig;

    TString Role;

    TString Host;
    TString Ip;

    TString NodeName;

    TMutex Mutex;
    THashMap<std::tuple<TString, TString, TString>, TActorId> Yt;

    ui32 NodeId = -1;
    TString GrpcPort = "";
    ui16 InterconnectPort;

    TWorkerRuntimeData RuntimeData;
    IFileCache::TPtr FileCache;

    TVector<TActorId> Children;
};

class TCoordinationHelperWithDummyLock: public TCoordinationHelper
{
public:
    TCoordinationHelperWithDummyLock(
        const NProto::TDqConfig::TYtCoordinator& config,
        const NProto::TDqConfig::TScheduler& schedulerConfig,
        const TString& role,
        ui16 interconnectPort,
        const TString& host,
        const TString& ip)
        : TCoordinationHelper(config, schedulerConfig, role, interconnectPort, host, ip)
    { }

    NActors::IActor* CreateLockOnCluster(NActors::TActorId ytWrapper, const TString& prefix, const TString& lockName, bool temporary) override {
        Y_UNUSED(ytWrapper);
        Y_UNUSED(prefix);
        Y_UNUSED(temporary);

        Y_ABORT_UNLESS(NodeId != static_cast<ui32>(-1));

        auto attributes = NYT::BuildYsonNodeFluently()
            .BeginMap()
                .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(NodeId)
                .Item(NCommonAttrs::HOSTNAME_ATTR).Value(GetHostname())
                .Item(NCommonAttrs::GRPCPORT_ATTR).Value(GrpcPort) // optional
                .Item(NCommonAttrs::UPLOAD_EXECUTABLE_ATTR).Value("true") // compat
            .EndMap();

        return CreateDummyLock(
                lockName,
                NYT::NodeToYsonString(attributes));
    }

    NActors::IActor* CreateLock(const TString& lockName, bool temporary) override {
        return CreateLockOnCluster(NActors::TActorId(), Config.GetPrefix(), lockName, temporary);
    }
};

ICoordinationHelper::TPtr CreateCoordiantionHelper(const NProto::TDqConfig::TYtCoordinator& cfg, const NProto::TDqConfig::TScheduler& schedulerConfig, const TString& role, ui16 interconnectPort, const TString& host, const TString& ip)
{
    NProto::TDqConfig::TYtCoordinator config = cfg;
    auto clusterName = config.GetClusterName();

    TString userName;
    TString token;

    if (config.HasToken()) {
        // internal job
        userName = config.GetUser();
        token = config.GetToken();
    } else if (!clusterName.empty()) {
        std::tie(userName, token) = NDqs::GetUserToken(
            config.HasUser() ? TMaybe<TString>(config.GetUser()) : TMaybe<TString>(),
            config.HasTokenFile() ? TMaybe<TString>(config.GetTokenFile()) : TMaybe<TString>()
        );
    }

    auto prefix = config.GetPrefix();

    if (config.GetLockType() != "dummy") {
        Y_ABORT_UNLESS(!token.empty());
        Y_ABORT_UNLESS(!clusterName.empty());
        Y_ABORT_UNLESS(!userName.empty());
        Y_ABORT_UNLESS(!prefix.empty());
    }

    Y_ABORT_UNLESS(!role.empty());

    config.SetUser(userName);
    config.SetToken(token);

    if (config.GetLockType() == "dummy") {
        return new TCoordinationHelperWithDummyLock(config, schedulerConfig, role, interconnectPort, host, ip);
    } else {
        return new TCoordinationHelper(config, schedulerConfig, role, interconnectPort, host, ip);
    }
}

} // namespace NYql
