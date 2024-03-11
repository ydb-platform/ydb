#include "grpc_pq_schema.h"

#include "actors/schema_actors.h"
#include "actors/events.h"

#include <ydb/core/persqueue/cluster_tracker.h>

#include <algorithm>
#include <shared_mutex>

using namespace NActors;
using namespace NKikimrClient;

using grpc::Status;

namespace NKikimr::NGRpcProxy::V1 {

///////////////////////////////////////////////////////////////////////////////

using namespace PersQueue::V1;

class TPQSchemaService : public NActors::TActorBootstrapped<TPQSchemaService>, IClustersCfgProvider {
public:
    TPQSchemaService(IClustersCfgProvider** p);

    void Bootstrap(const TActorContext& ctx);

    TIntrusiveConstPtr<TClustersCfg> GetCfg() const override;

private:
    TString AvailableLocalCluster();

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, Handle);
        }
    }

private:
    void Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev);

    mutable std::shared_mutex Mtx;
    TIntrusivePtr<TClustersCfg> ClustersCfg;
};

IActor* CreatePQSchemaService(IClustersCfgProvider** p) {
    return new TPQSchemaService(p);
}

TPQSchemaService::TPQSchemaService(IClustersCfgProvider** p)
    : ClustersCfg(MakeIntrusive<TClustersCfg>())
{
    // used from grpc handlers.
    // GetCfg method in called in the grpc thread context
    // We have guarantee this object is created before grpc start
    // and the the object destroyed after grpc stop
    *p = this;
}

void TPQSchemaService::Bootstrap(const TActorContext& ctx) {
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) { // ToDo[migration]: switch to haveClusters
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, "TPQSchemaService: send TEvClusterTracker::TEvSubscribe");

        ctx.Send(NPQ::NClusterTracker::MakeClusterTrackerID(),
                 new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe);
    }

    Become(&TThis::StateFunc);
}

void TPQSchemaService::Handle(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->ClustersList);
    Y_ABORT_UNLESS(ev->Get()->ClustersList->Clusters.size());

    const auto& clusters = ev->Get()->ClustersList->Clusters;

    auto cfg = MakeIntrusive<TClustersCfg>();

    auto it = std::find_if(begin(clusters), end(clusters), [](const auto& cluster) { return cluster.IsLocal; });
    if (it != end(clusters)) {
        cfg->LocalCluster = it->Name;
    }

    cfg->Clusters.resize(clusters.size());
    for (size_t i = 0; i < clusters.size(); ++i) {
        cfg->Clusters[i] = clusters[i].Name;
    }

    std::unique_lock lock(Mtx);
    ClustersCfg = cfg;
}

TIntrusiveConstPtr<TClustersCfg> TPQSchemaService::GetCfg() const {
    std::shared_lock lock(Mtx);
    return ClustersCfg;
}

}

namespace NKikimr {
namespace NGRpcService {

void EnsureReq(const IRequestOpCtx* ctx, const TIntrusiveConstPtr<NGRpcProxy::V1::TClustersCfg>& cfg) {
    if (Y_UNLIKELY(!ctx))
        throw yexception() << "no req ctx after cast";

    if (Y_UNLIKELY(!cfg))
        throw yexception() << "no cluster cfg provided";
}

void EnsureReq(const IRequestOpCtx* ctx) {
    if (Y_UNLIKELY(!ctx))
        throw yexception() << "no req ctx after cast";
}

void DoDropTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f) {
    auto p = dynamic_cast<TEvDropTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new drop topic request");
    f.RegisterActor(new NGRpcProxy::V1::TDropTopicActor(p));
}

void DoCreateTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f,
    TIntrusiveConstPtr<NGRpcProxy::V1::TClustersCfg> cfg)
{
    auto p = dynamic_cast<TEvCreateTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new create topic request");
    f.RegisterActor(new NGRpcProxy::V1::TCreateTopicActor(p, cfg->LocalCluster, cfg->Clusters));
}

void DoAlterTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f) {
    auto p = dynamic_cast<TEvAlterTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new alter topic request");
    f.RegisterActor(new NGRpcProxy::V1::TAlterTopicActor(p));
}

void DoDescribeTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f) {
    auto p = dynamic_cast<TEvDescribeTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Describe topic request");
    f.RegisterActor(new NGRpcProxy::V1::TDescribeTopicActor(p));
}

void DoDescribeConsumerRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f) {
    auto p = dynamic_cast<TEvDescribeConsumerRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Describe consumer request");
    f.RegisterActor(new NGRpcProxy::V1::TDescribeConsumerActor(p));
}

void DoDescribePartitionRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f) {
    auto p = dynamic_cast<TEvDescribePartitionRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Describe partition request");
    f.RegisterActor(new NGRpcProxy::V1::TDescribePartitionActor(p));
}

void DoPQDropTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f) {
    auto p = dynamic_cast<TEvPQDropTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Drop topic request");
    f.RegisterActor(new NGRpcProxy::V1::TPQDropTopicActor(p));
}

void DoPQCreateTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f,
    TIntrusiveConstPtr<NGRpcProxy::V1::TClustersCfg> cfg)
{
    auto p = dynamic_cast<TEvPQCreateTopicRequest*>(ctx.release());

    EnsureReq(p, cfg);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Create topic request");
    f.RegisterActor(new NGRpcProxy::V1::TPQCreateTopicActor(p, cfg->LocalCluster, cfg->Clusters));
}

void DoPQAlterTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f,
    TIntrusiveConstPtr<NGRpcProxy::V1::TClustersCfg> cfg)
{
    auto p = dynamic_cast<TEvPQAlterTopicRequest*>(ctx.release());

    EnsureReq(p, cfg);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Alter topic request");
    f.RegisterActor(new NGRpcProxy::V1::TPQAlterTopicActor(p, cfg->LocalCluster));
}

void DoPQDescribeTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f) {
    auto p = dynamic_cast<TEvPQDescribeTopicRequest*>(ctx.release());
    
    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Describe topic request");
    f.RegisterActor(new NGRpcProxy::V1::TPQDescribeTopicActor(p));
}

void DoPQAddReadRuleRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f) {
    auto p = dynamic_cast<TEvPQAddReadRuleRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Add read rules request");
    f.RegisterActor(new NGRpcProxy::V1::TAddReadRuleActor(p));
}

void DoPQRemoveReadRuleRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f) {
    auto p = dynamic_cast<TEvPQRemoveReadRuleRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Remove read rules request");
    f.RegisterActor(new NGRpcProxy::V1::TRemoveReadRuleActor(p));
}

#ifdef DECLARE_RPC
#error DECLARE_RPC macro already defined
#endif

#define DECLARE_RPC(name) template<> IActor* TEv##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) { \
    return new NKikimr::NGRpcProxy::V1::T##name##Actor(msg);\
    }

DECLARE_RPC(DescribeTopic);
DECLARE_RPC(DescribeConsumer);
DECLARE_RPC(DescribePartition);

#undef DECLARE_RPC

}
}
