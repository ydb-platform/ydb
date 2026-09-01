#include "msgbus_server_pq_metacache.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/public/pq_database.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <library/cpp/json/json_reader.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_METACACHE

namespace NKikimr::NMsgBusProxy {

namespace NPqMetaCacheV2 {

class TPersQueueMetaCacheActor : public TActorBootstrapped<TPersQueueMetaCacheActor> {
    using TBase = TActorBootstrapped<TPersQueueMetaCacheActor>;
public:
    TPersQueueMetaCacheActor(TPersQueueMetaCacheActor&&) = default;
    TPersQueueMetaCacheActor& operator=(TPersQueueMetaCacheActor&&) = default;

    TPersQueueMetaCacheActor(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters)
    {
        Y_UNUSED(Counters);
    }

    TPersQueueMetaCacheActor(const NActors::TActorId& /*schemeBoardCacheId*/)
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_META_CACHE;
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TPersQueueMetaCacheActor::StateFunc);

        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            return;
        }

        auto& metaCacheConfig = AppData(ctx)->PQConfig.GetPQDiscoveryConfig();
        if (!metaCacheConfig.GetLBFrontEnabled()) {
            return;
        }
        if (metaCacheConfig.GetUseDynNodesMapping()) {
            TStringBuf tenant = AppData(ctx)->TenantName;
            tenant.SkipPrefix("/");
            tenant.ChopSuffix("/");
            if (tenant != "Root") {
                YDB_LOG_NOTICE_CTX(ctx, "Started on tenant, will not request hive",
                    {"tenant", tenant});
                OnDynNode = true;
            } else {
                StartHivePipe(ctx);
                ProcessNodesInfoWork(ctx);
            }
        }
    }

    ~TPersQueueMetaCacheActor() {
    }

private:
    static ui64 GetHiveTabletId(const TActorContext& ctx) {
        return AppData(ctx)->DomainsInfo->GetHive();
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
        ProcessNodesInfoWork(ctx);
    }

    void HandleGetNodesMapping(TEvPqNewMetaCache::TEvGetNodesMappingRequest::TPtr& ev, const TActorContext& ctx) {
        NodesMappingWaiters.emplace(std::move(ev->Sender));
        ProcessNodesInfoWork(ctx);
    }

    void StartHivePipe(const TActorContext& ctx) {
        auto hiveTabletId = GetHiveTabletId(ctx);
        YDB_LOG_DEBUG_CTX(ctx, "Start pipe to hive",
            {"tablet", hiveTabletId});
        auto pipeRetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeRetryPolicy.MaxRetryTime = TDuration::Seconds(1);
        NTabletPipe::TClientConfig pipeConfig{.RetryPolicy = pipeRetryPolicy};
        HivePipeClient = ctx.RegisterWithSameMailbox(
                NTabletPipe::CreateClient(ctx.SelfID, hiveTabletId, pipeConfig)
        );
    }

    void HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        switch (ev->Get()->Status) {
            case NKikimrProto::EReplyStatus::OK:
            case NKikimrProto::EReplyStatus::ALREADY:
                break;
            default:
                return HandlePipeDestroyed(ctx);
        }
        YDB_LOG_DEBUG_CTX(ctx, "Hive pipe connected");
        ProcessNodesInfoWork(ctx);
    }

    void HandlePipeDestroyed(const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Hive pipe destroyed");
        NTabletPipe::CloseClient(ctx, HivePipeClient);
        HivePipeClient = TActorId();
        StartHivePipe(ctx);
        ResetHiveRequestState(ctx);
    }

    enum class EWakeupTag {
        WakeForQuery = 1,
        WakeForHive = 2
    };

    void ResetHiveRequestState(const TActorContext& ctx) {
        if (NextHiveRequestDeadline == TInstant::Zero()) {
            NextHiveRequestDeadline = ctx.Now() + TDuration::Seconds(5);
        }
        RequestedNodesInfo = false;
        ctx.Schedule(
                TDuration::Seconds(5),
                new NActors::TEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::WakeForHive))
        );
    }

    void ProcessNodesInfoWork(const TActorContext& ctx) {
        if (OnDynNode) {
            ProcessNodesInfoWaitersQueue(false, ctx);
            return;
        }
        if (DynamicNodesMapping != nullptr && LastNodesInfoUpdate != TInstant::Zero()) {
            const auto nextNodesUpdateTs = LastNodesInfoUpdate + TDuration::MilliSeconds(
                    AppData(ctx)->PQConfig.GetPQDiscoveryConfig().GetNodesMappingRescanIntervalMilliSeconds()
            );
            if (ctx.Now() < nextNodesUpdateTs)
                return ProcessNodesInfoWaitersQueue(true, ctx);
        }
        if (RequestedNodesInfo)
            return;

        if (NextHiveRequestDeadline != TInstant::Zero() && ctx.Now() < NextHiveRequestDeadline) {
            ResetHiveRequestState(ctx);
            return;
        }
        NextHiveRequestDeadline = ctx.Now() + TDuration::Seconds(5);
        RequestedNodesInfo = true;

        NActorsProto::TRemoteHttpInfo info;
        {
            auto* param = info.AddQueryParams();
            param->SetKey("page");
            param->SetValue("MemStateNodes");
        }
        {
            auto* param = info.AddQueryParams();
            param->SetKey("format");
            param->SetValue("json");
        }
        info.SetPath("/app");
        YDB_LOG_DEBUG_CTX(ctx, "Send Hive nodes state request");
        NTabletPipe::SendData(ctx, HivePipeClient, new NActors::NMon::TEvRemoteHttpInfo(info));
    }

    void HandleHiveMonResponse(NMon::TEvRemoteJsonInfoRes::TPtr& ev, const TActorContext& ctx) {
        ResetHiveRequestState(ctx);
        YDB_LOG_DEBUG_CTX(ctx, "Got Hive landing data response",
            {"ev", ev->Get()->Json});
        TStringInput input(ev->Get()->Json);
        auto jsonValue = NJson::ReadJsonTree(&input, true);
        const auto& rootMap = jsonValue.GetMap();
        ui32 aliveNodes = rootMap.find("AliveNodes")->second.GetUInteger();
        if (!aliveNodes) {
            return;
        }
        const auto& nodes = rootMap.find("Nodes")->second.GetArray();
        TSet<ui32> staticNodeIds;
        TVector<ui32> dynamicNodes;
        ui64 maxStaticNodeId = 0;
        for (const auto& node : nodes) {
            const auto& nodeMap = node.GetMap();
            ui64 nodeId = nodeMap.find("Id")->second.GetUInteger();
            if (nodeMap.find("Domain")->second.GetString() == "/Root") {
                maxStaticNodeId = std::max(maxStaticNodeId, nodeId);
                if (nodeMap.find("Alive")->second.GetBoolean() && !nodeMap.find("Down")->second.GetBoolean()) {
                    staticNodeIds.insert(nodeId);
                }
            } else {
                dynamicNodes.push_back(nodeId);
            }
        }
        if (staticNodeIds.empty()) {
            return;
        }
        DynamicNodesMapping.reset(new THashMap<ui32, ui32>());
        for (auto& dynNodeId : dynamicNodes) {
            ui32 hash_ = dynNodeId % (maxStaticNodeId + 1);
            auto iter = staticNodeIds.lower_bound(hash_);
            DynamicNodesMapping->insert(std::make_pair(
                    dynNodeId,
                    iter == staticNodeIds.end() ? *staticNodeIds.begin() : *iter
            ));
        }
        LastNodesInfoUpdate = ctx.Now();
        ProcessNodesInfoWaitersQueue(true, ctx);
    }

    void ProcessNodesInfoWaitersQueue(bool status, const TActorContext& ctx) {
        if (DynamicNodesMapping == nullptr) {
            Y_ABORT_UNLESS(!status);
            DynamicNodesMapping.reset(new THashMap<ui32, ui32>);
        }
        while(!NodesMappingWaiters.empty()) {
            ctx.Send(NodesMappingWaiters.front(),
                     new TEvPqNewMetaCache::TEvGetNodesMappingResponse(DynamicNodesMapping, status));
            NodesMappingWaiters.pop();
        }
    }

public:
    void Die(const TActorContext& ctx) {
        TBase::Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
          HFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
          HFunc(TEvPqNewMetaCache::TEvGetNodesMappingRequest, HandleGetNodesMapping)
          HFunc(NMon::TEvRemoteJsonInfoRes, HandleHiveMonResponse)
          SFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeDestroyed)
          HFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected)
    )

private:
    ::NMonitoring::TDynamicCounterPtr Counters;

    TQueue<TActorId> NodesMappingWaiters;

    TActorId HivePipeClient;
    bool RequestedNodesInfo = false;
    TInstant NextHiveRequestDeadline = TInstant::Zero();
    TInstant LastNodesInfoUpdate = TInstant::Now();
    bool OnDynNode = false;

    std::shared_ptr<THashMap<ui32, ui32>> DynamicNodesMapping;
};

IActor* CreatePQMetaCache(const NMonitoring::TDynamicCounterPtr& counters) {
    return new TPersQueueMetaCacheActor(counters);
}

IActor* CreatePQMetaCache(const NActors::TActorId& schemeBoardCacheId) {
    return new TPersQueueMetaCacheActor(schemeBoardCacheId);
}

} // namespace NPqMetaCacheV2

} // namespace NKikimr::NMsgBusProxy
