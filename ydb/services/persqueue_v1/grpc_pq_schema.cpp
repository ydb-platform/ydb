#include "grpc_pq_schema.h"

#include "actors/schema_actors.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/persqueue/obfuscate/obfuscate.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <algorithm>

using namespace NActors;
using namespace NKikimrClient;

using grpc::Status;

namespace NKikimr::NGRpcProxy::V1 {

///////////////////////////////////////////////////////////////////////////////

using namespace PersQueue::V1;


IActor* CreatePQSchemaService(const TActorId& schemeCache, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TPQSchemaService(schemeCache, counters);
}



TPQSchemaService::TPQSchemaService(const TActorId& schemeCache,
                             TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
    : SchemeCache(schemeCache)
    , Counters(counters)
    , LocalCluster("")
{
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
    Y_VERIFY(ev->Get()->ClustersList);
    Y_VERIFY(ev->Get()->ClustersList->Clusters.size());

    const auto& clusters = ev->Get()->ClustersList->Clusters;

    LocalCluster = {};

    auto it = std::find_if(begin(clusters), end(clusters), [](const auto& cluster) { return cluster.IsLocal; });
    if (it != end(clusters)) {
        LocalCluster = it->Name;
    }

    Clusters.resize(clusters.size());
    for (size_t i = 0; i < clusters.size(); ++i) {
        Clusters[i] = clusters[i].Name;
    }
}

// unused ?
// google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> FillResponse(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode code) {
//     google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> res;
//     FillIssue(res.Add(), code, errorReason);
//     return res;
// }


void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvPQDropTopicRequest::TPtr& ev, const TActorContext& ctx) {

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new drop topic request");

    ctx.Register(new TPQDropTopicActor(ev->Release().Release()));
}


void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvDropTopicRequest::TPtr& ev, const TActorContext& ctx) {

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new drop topic request");

    ctx.Register(new TDropTopicActor(ev->Release().Release()));
}



void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvPQAlterTopicRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new Alter topic request");
    ctx.Register(new TPQAlterTopicActor(ev->Release().Release(), LocalCluster));
}

void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvAlterTopicRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new Alter topic request");
    ctx.Register(new TAlterTopicActor(ev->Release().Release()));
}


void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvPQAddReadRuleRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new Add read rules request");
    ctx.Register(new TAddReadRuleActor(ev->Release().Release()));
}

void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvPQRemoveReadRuleRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new Remove read rules request");
    ctx.Register(new TRemoveReadRuleActor(ev->Release().Release()));
}

void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvPQCreateTopicRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new create topic request");
    ctx.Register(new TPQCreateTopicActor(ev->Release().Release(), LocalCluster, Clusters));
}

void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvCreateTopicRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new create topic request");
    ctx.Register(new TCreateTopicActor(ev->Release().Release(), LocalCluster, Clusters));
}


void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvPQDescribeTopicRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new Describe topic request");
    ctx.Register(new TPQDescribeTopicActor(ev->Release().Release()));
}

void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvDescribeTopicRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new Describe topic request");
    ctx.Register(new TDescribeTopicActor(ev->Release().Release()));
}

void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvDescribeConsumerRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new Describe consumer request");
    ctx.Register(new TDescribeConsumerActor(ev->Release().Release()));
}

void TPQSchemaService::Handle(NKikimr::NGRpcService::TEvDescribePartitionRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "new Describe partition request");
    ctx.Register(new TDescribePartitionActor(ev->Release().Release()));
}

}

namespace NKikimr {
namespace NGRpcService {

void TGRpcRequestProxyHandleMethods::Handle(TEvPQDropTopicRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvPQCreateTopicRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvPQAlterTopicRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvPQDescribeTopicRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvDropTopicRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvCreateTopicRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvAlterTopicRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvDescribeTopicRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvDescribePartitionRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void NKikimr::NGRpcService::TGRpcRequestProxyHandleMethods::Handle(NKikimr::NGRpcService::TEvDescribeConsumerRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvPQAddReadRuleRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
}

void TGRpcRequestProxyHandleMethods::Handle(TEvPQRemoveReadRuleRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(NKikimr::NGRpcProxy::V1::GetPQSchemaServiceActorID(), ev->Release().Release());
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
