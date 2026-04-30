#include "grpc_pq_schema.h"

#include "actors/schema_actors.h"
#include "actors/read_session_actor.h"

#include <ydb/services/persqueue_v1/actors/schema/pqv1/actors.h>
#include <ydb/services/persqueue_v1/actors/schema/topic/actors.h>

#include <ydb/core/persqueue/public/cluster_tracker/cluster_tracker.h>

#include <algorithm>
#include <shared_mutex>

using namespace NActors;
using namespace NKikimrClient;

using grpc::Status;

namespace NKikimr {
namespace NGRpcService {

void EnsureReq(const IRequestOpCtx* ctx) {
    if (Y_UNLIKELY(!ctx))
        throw yexception() << "no req ctx after cast";
}

void DoDropTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f) {
    auto p = dynamic_cast<TEvDropTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new drop topic request");
    f.RegisterActor(NKikimr::NGRpcProxy::V1::NTopic::CreateDropTopicActor(p));
}

void DoCreateTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f)
{
    auto p = dynamic_cast<TEvCreateTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new create topic request");
    f.RegisterActor(NKikimr::NGRpcProxy::V1::NTopic::CreateCreateTopicActor(p));
}

void DoAlterTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f) {
    auto* p = ctx.release();
    Y_VERIFY_DEBUG(dynamic_cast<const Ydb::Topic::AlterTopicRequest*>(p->GetRequest()));

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new alter topic request");
    f.RegisterActor(NKikimr::NGRpcProxy::V1::NTopic::CreateAlterTopicActor(p));
}

void DoDescribeTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f) {
    auto* p = ctx.release();
    Y_VERIFY_DEBUG(dynamic_cast<const Ydb::Topic::DescribeTopicRequest*>(p->GetRequest()));

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

void DoCommitOffsetRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider&) {
    std::unique_ptr<TEvCommitOffsetRequest> p;
    p.reset(dynamic_cast<TEvCommitOffsetRequest*>(ctx.release()));

    EnsureReq(p.get());

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Commit Offset request");
    TActivationContext::Send(NKikimr::NGRpcProxy::V1::GetPQReadServiceActorID(), std::move(p));
}

void DoPQDropTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const NKikimr::NGRpcService::IFacilityProvider& f) {
    auto p = dynamic_cast<TEvPQDropTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Drop topic request");
    f.RegisterActor(NGRpcProxy::V1::NPQv1::CreateDropTopicActor(p));
}

void DoPQCreateTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f)
{
    auto p = dynamic_cast<TEvPQCreateTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Create topic request");
    f.RegisterActor(NGRpcProxy::V1::NPQv1::CreateCreateTopicActor(p));
}

void DoPQAlterTopicRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f)
{
    auto p = dynamic_cast<TEvPQAlterTopicRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Alter topic request");
    f.RegisterActor(NGRpcProxy::V1::NPQv1::CreateAlterTopicActor(p));
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
    f.RegisterActor(NGRpcProxy::V1::NPQv1::CreateAddConsumerActor(p));
}

void DoPQRemoveReadRuleRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider& f) {
    auto p = dynamic_cast<TEvPQRemoveReadRuleRequest*>(ctx.release());

    EnsureReq(p);

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::PQ_READ_PROXY, "new Remove read rules request");
    f.RegisterActor(NGRpcProxy::V1::NPQv1::CreateRemoveConsumerActor(p));
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
