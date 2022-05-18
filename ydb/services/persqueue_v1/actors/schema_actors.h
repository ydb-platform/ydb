#pragma once

#include <ydb/services/lib/actors/pq_schema_actor.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;


class TDropTopicActor : public TPQGrpcSchemaBase<TDropTopicActor, NKikimr::NGRpcService::TEvPQDropTopicRequest> {
using TBase = TPQGrpcSchemaBase<TDropTopicActor, TEvPQDropTopicRequest>;

public:
     TDropTopicActor(NKikimr::NGRpcService::TEvPQDropTopicRequest* request);
    ~TDropTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }
};

class TDescribeTopicActor : public TPQGrpcSchemaBase<TDescribeTopicActor, NKikimr::NGRpcService::TEvPQDescribeTopicRequest>
                          , public TCdcStreamCompatible
{
using TBase = TPQGrpcSchemaBase<TDescribeTopicActor, TEvPQDescribeTopicRequest>;

public:
     TDescribeTopicActor(NKikimr::NGRpcService::TEvPQDescribeTopicRequest* request);
    ~TDescribeTopicActor() = default;

    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
};


class TAddReadRuleActor : public TUpdateSchemeActor<TAddReadRuleActor, TEvPQAddReadRuleRequest>
                        , public TCdcStreamCompatible
{
    using TBase = TUpdateSchemeActor<TAddReadRuleActor, TEvPQAddReadRuleRequest>;

public:
    TAddReadRuleActor(NKikimr::NGRpcService::TEvPQAddReadRuleRequest *request);

    void Bootstrap(const NActors::TActorContext &ctx);
    void ModifyPersqueueConfig(const TActorContext& ctx,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo);
};

class TRemoveReadRuleActor : public TUpdateSchemeActor<TRemoveReadRuleActor, TEvPQRemoveReadRuleRequest>
                           , public TCdcStreamCompatible
{
    using TBase = TUpdateSchemeActor<TRemoveReadRuleActor, TEvPQRemoveReadRuleRequest>;

public:
    TRemoveReadRuleActor(NKikimr::NGRpcService::TEvPQRemoveReadRuleRequest* request);

    void Bootstrap(const NActors::TActorContext &ctx);
    void ModifyPersqueueConfig(const TActorContext& ctx,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo);
};


class TCreateTopicActor : public TPQGrpcSchemaBase<TCreateTopicActor, NKikimr::NGRpcService::TEvPQCreateTopicRequest> {
using TBase = TPQGrpcSchemaBase<TCreateTopicActor, TEvPQCreateTopicRequest>;

public:
     TCreateTopicActor(NKikimr::NGRpcService::TEvPQCreateTopicRequest* request, const TString& localCluster, const TVector<TString>& clusters);
    ~TCreateTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }

private:
    TString LocalCluster;
    TVector<TString> Clusters;
};

class TAlterTopicActor : public TPQGrpcSchemaBase<TAlterTopicActor, NKikimr::NGRpcService::TEvPQAlterTopicRequest> {
using TBase = TPQGrpcSchemaBase<TAlterTopicActor, TEvPQAlterTopicRequest>;

public:
     TAlterTopicActor(NKikimr::NGRpcService::TEvPQAlterTopicRequest* request, const TString& localCluster);
    ~TAlterTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }

private:
    TString LocalCluster;
};

}
