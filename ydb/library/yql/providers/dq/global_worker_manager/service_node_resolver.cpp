#include "service_node_resolver.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/yson/node/node_io.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/yt/yt_wrapper.h>
#include <ydb/library/yql/providers/dq/common/attrs.h>

#include <util/system/mutex.h>
#include <util/random/random.h>

namespace NYql {

using namespace NThreading;
using namespace NActors;

struct TNodeInfo {
    NYdbGrpc::TGRpcClientConfig ClientConfig;
    ui32 NodeId;
};

class TNodesHolder {
public:
    TMaybe<TNodeInfo> GetNode() {
        TGuard<TMutex> guard(Mutex);
        if (Nodes.empty()) {
            return {};
        } else {
            auto index = RandomNumber(Min<ui32>(3, Nodes.size()));
            return Nodes[index];
        }
    }

    void Swap(TVector<TNodeInfo>& nodes)
    {
        TGuard<TMutex> guard(Mutex);
        Nodes.swap(nodes);
    }

private:
    TMutex Mutex;
    TVector<TNodeInfo> Nodes;
};

class TAbstractNodeResolver: public IServiceNodeResolver {
public:
    TAbstractNodeResolver()
        : ClientLow(1)
        , ChannelPool(NYdbGrpc::TTcpKeepAliveSettings{true, 30, 5, 10})
    { }

    ~TAbstractNodeResolver()
    {
        Stop();
    }

    virtual TMaybe<TNodeInfo> GetNode() = 0;

    TFuture<TConnectionResult> GetConnection() override {
        auto nodeInfo = GetNode();

        if (!nodeInfo) {
            return MakeFuture(NCommon::ResultFromError<TConnectionResult>("Unable to get node"));
        }

        auto context = ClientLow.CreateContext();
        if (!context) {
            return MakeFuture(NCommon::ResultFromError<TConnectionResult>("Unable to create grpc request context"));
        }

        std::unique_ptr<NYdbGrpc::TServiceConnection<Yql::DqsProto::DqService>> conn;
        ChannelPool.GetStubsHolderLocked(
            nodeInfo->ClientConfig.Locator, nodeInfo->ClientConfig, [&conn, this](NYdbGrpc::TStubsHolder& holder) mutable {
            conn.reset(ClientLow.CreateGRpcServiceConnection<Yql::DqsProto::DqService>(holder).release());
        });

        return MakeFuture(TConnectionResult(std::move(conn), std::move(context), nodeInfo->NodeId, nodeInfo->ClientConfig.Locator));
    }

    void Stop() override {
        ClientLow.Stop(true);
    }

protected:
    NYdbGrpc::TGRpcClientLow ClientLow;
    NYdbGrpc::TChannelPool ChannelPool;
};

class TNodeUpdater: public TActor<TNodeUpdater> {
public:
    static constexpr char ActorName[] = "NODE_UPDATER";

    TNodeUpdater(const std::shared_ptr<TNodesHolder>& holder, const TDynamicResolverOptions& options)
        : TActor(&TNodeUpdater::Handler)
        , Holder(holder)
        , Options(options)
        , LastUpdateTime(TInstant::Now())
    { }

    TEvListNode* ListNodeCommand() {
        NYT::NApi::TListNodeOptions options;
        options.Attributes = {
            NCommonAttrs::ACTOR_NODEID_ATTR,
            NCommonAttrs::ROLE_ATTR,
            NCommonAttrs::HOSTNAME_ATTR,
            NCommonAttrs::GRPCPORT_ATTR,
            "modification_time"
        };
        if (InvalidateCache) {
            InvalidateCache = false;
        } else {
            options.ReadFrom = NYT::NApi::EMasterChannelKind::Cache;
        }
        return new TEvListNode(Options.Prefix, options);
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        Y_UNUSED(parentId);
        return new IEventHandle(Options.YtWrapper, self, ListNodeCommand(), 0);
    }

    STRICT_STFUNC(Handler, {
        HFunc(TEvListNodeResponse, OnListNodeResponse)
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        cFunc(NActors::TEvents::TEvWakeup::EventType, [&] () {
            InvalidateCache = true;
        })
    });

    void ProcessResult(const TVector<NYT::TNode>& nodes)
    {
        TVector<std::tuple<TInstant, TString, ui32>> hostPort;
        hostPort.reserve(nodes.size());
        for (const auto& node : nodes) {
            const auto& attributes = node.GetAttributes().AsMap();
            auto maybeRole = attributes.find(NCommonAttrs::ROLE_ATTR);
            auto maybeHostname = attributes.find(NCommonAttrs::HOSTNAME_ATTR);
            auto maybeGrpcPort = attributes.find(NCommonAttrs::GRPCPORT_ATTR);
            auto maybeNodeId = attributes.find(NCommonAttrs::ACTOR_NODEID_ATTR);

            auto maybeModificationTime = attributes.find("modification_time");
            YQL_ENSURE(maybeModificationTime != attributes.end());

            if (maybeRole == attributes.end()
                || maybeHostname == attributes.end()
                || maybeGrpcPort == attributes.end()
                || maybeNodeId == attributes.end())
            {
                continue;
            }

            if (maybeRole->second != "service_node") {
                continue;
            }

            ui32 nodeId = maybeNodeId->second.AsUint64();

            auto modificationTime = TInstant::ParseIso8601(maybeModificationTime->second.AsString());

            hostPort.emplace_back(modificationTime, maybeHostname->second.AsString() + ":" + maybeGrpcPort->second.AsString(), nodeId);
        }

        std::sort(hostPort.begin(), hostPort.end());

        TVector<TNodeInfo> locations;
        locations.reserve(hostPort.size());
        for (auto it = hostPort.rbegin(); it != hostPort.rend(); ++it) {
            locations.push_back({NYdbGrpc::TGRpcClientConfig(std::get<1>(*it), TDuration::Seconds(15)), std::get<2>(*it)});
        }

        if (locations.empty()) {
            YQL_CLOG(WARN, ProviderDq) << "Empty locations";
        }

        Holder->Swap(locations);
    }

    void OnListNodeResponse(TEvListNodeResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto now = TInstant::Now();

        auto result = std::get<0>(*ev->Get());

        try {
            ProcessResult(NYT::NodeFromYsonString(result.ValueOrThrow()).AsList());
        } catch (...) {
            YQL_CLOG(ERROR, ProviderDq) << "Error on list node '" << ToString(result) << "' " << CurrentExceptionMessage();
        }

        TActivationContext::Schedule(Options.UpdatePeriod, new IEventHandle(Options.YtWrapper, SelfId(), ListNodeCommand(), 0));

        LastUpdateTime = now;
    }

private:
    std::shared_ptr<TNodesHolder> Holder;
    TDynamicResolverOptions Options;
    TInstant LastUpdateTime;
    bool InvalidateCache = false;
};

class TDynamicResolver: public TAbstractNodeResolver {
public:
    TDynamicResolver(NActors::TActorSystem* actorSystem, const TDynamicResolverOptions& options)
        : Nodes(new TNodesHolder)
        , ActorSystem(actorSystem)
        , Options(options)
        , NodeUpdater(ActorSystem->Register(new TNodeUpdater(Nodes, Options)))
    { }

    ~TDynamicResolver()
    { }

    TMaybe<TNodeInfo> GetNode() override {
        return Nodes->GetNode();
    }

    void InvalidateCache() override {
        ActorSystem->Send(NodeUpdater, new TEvents::TEvWakeup);
    }

private:
    std::shared_ptr<TNodesHolder> Nodes;

    NActors::TActorSystem* ActorSystem;
    TDynamicResolverOptions Options;
    NActors::TActorId NodeUpdater;
};

class TStaticResolver: public TAbstractNodeResolver {
public:
    TStaticResolver(const TVector<TString>& hostPortPairs)
    {
        Nodes.reserve(hostPortPairs.size());
        for (const auto& hostPort : hostPortPairs) {
            Nodes.emplace_back(hostPort, TDuration::Seconds(1));
        }
    }

    TMaybe<TNodeInfo> GetNode() override {
        auto clientConfig = NYdbGrpc::TGRpcClientConfig{Nodes[CurrentNodeIndex]};
        CurrentNodeIndex = (CurrentNodeIndex + 1) % Nodes.size();

        return TNodeInfo{clientConfig, 0};
    }

    void InvalidateCache() override {
    }

private:
    TVector<NYdbGrpc::TGRpcClientConfig> Nodes;
    int CurrentNodeIndex = 0;
};

TSingleNodeResolver::TSingleNodeResolver()
    : ClientLow(1)
    , ChannelPool(NYdbGrpc::TTcpKeepAliveSettings{true, 30, 5, 10})
{ }

TSingleNodeResolver::~TSingleNodeResolver()
{
    ClientLow.Stop(true);
}

TFuture<IServiceNodeResolver::TConnectionResult> TSingleNodeResolver::GetConnection() {
    auto clientConfig = NYdbGrpc::TGRpcClientConfig(LeaderHostPort);

    auto context = ClientLow.CreateContext();
    if (!context) {
        return MakeFuture(NCommon::ResultFromError<IServiceNodeResolver::TConnectionResult>("Unable to create grpc request context"));
    }

    std::unique_ptr<NYdbGrpc::TServiceConnection<Yql::DqsProto::DqService>> conn;
    ChannelPool.GetStubsHolderLocked(
        clientConfig.Locator, clientConfig, [&conn, this](NYdbGrpc::TStubsHolder& holder) mutable {
        conn.reset(ClientLow.CreateGRpcServiceConnection<Yql::DqsProto::DqService>(holder).release());
    });

    return MakeFuture(IServiceNodeResolver::TConnectionResult(std::move(conn), std::move(context)));
}

void TSingleNodeResolver::SetLeaderHostPort(const TString& leaderHostPort) {
    LeaderHostPort = leaderHostPort;
}

IServiceNodeResolver::TPtr CreateStaticResolver(const TVector<TString>& hostPortPairs) {
    return std::make_shared<TStaticResolver>(hostPortPairs);
}

IServiceNodeResolver::TPtr CreateDynamicResolver(NActors::TActorSystem* actorSystem, const TDynamicResolverOptions& options) {
    Y_ABORT_UNLESS(options.YtWrapper);
    Y_ABORT_UNLESS(!options.Prefix.empty());

    return std::make_shared<TDynamicResolver>(actorSystem, options);
}

} // namespace NYql
