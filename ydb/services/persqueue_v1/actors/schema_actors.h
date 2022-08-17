#pragma once

#include <ydb/services/lib/actors/pq_schema_actor.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;


class TDropPropose {
public:
    TDropPropose() {}
    virtual ~TDropPropose() {}

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                         const TString& workingDir, const TString& name);
};

class TPQDropTopicActor : public TPQGrpcSchemaBase<TPQDropTopicActor, NKikimr::NGRpcService::TEvPQDropTopicRequest>, public TDropPropose {
using TBase = TPQGrpcSchemaBase<TPQDropTopicActor, TEvPQDropTopicRequest>;

public:
     TPQDropTopicActor(NKikimr::NGRpcService::TEvPQDropTopicRequest* request);
    ~TPQDropTopicActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }
};

class TDropTopicActor : public TPQGrpcSchemaBase<TDropTopicActor, NKikimr::NGRpcService::TEvDropTopicRequest>, public TDropPropose {
using TBase = TPQGrpcSchemaBase<TDropTopicActor, TEvDropTopicRequest>;

public:
     TDropTopicActor(NKikimr::NGRpcService::TEvDropTopicRequest* request);
    ~TDropTopicActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }
};


class TPQDescribeTopicActor : public TPQGrpcSchemaBase<TPQDescribeTopicActor, NKikimr::NGRpcService::TEvPQDescribeTopicRequest>
                            , public TCdcStreamCompatible
{
using TBase = TPQGrpcSchemaBase<TPQDescribeTopicActor, TEvPQDescribeTopicRequest>;

public:
     TPQDescribeTopicActor(NKikimr::NGRpcService::TEvPQDescribeTopicRequest* request);
    ~TPQDescribeTopicActor() = default;

    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
};

class TDescribeTopicActor : public TPQGrpcSchemaBase<TDescribeTopicActor, NKikimr::NGRpcService::TEvDescribeTopicRequest>
                          , public TCdcStreamCompatible
{
using TBase = TPQGrpcSchemaBase<TDescribeTopicActor, TEvDescribeTopicRequest>;

public:
     TDescribeTopicActor(NKikimr::NGRpcService::TEvDescribeTopicRequest* request);
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


class TPQCreateTopicActor : public TPQGrpcSchemaBase<TPQCreateTopicActor, NKikimr::NGRpcService::TEvPQCreateTopicRequest> {
using TBase = TPQGrpcSchemaBase<TPQCreateTopicActor, TEvPQCreateTopicRequest>;

public:
     TPQCreateTopicActor(NKikimr::NGRpcService::TEvPQCreateTopicRequest* request, const TString& localCluster, const TVector<TString>& clusters);
    ~TPQCreateTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }

private:
    TString LocalCluster;
    TVector<TString> Clusters;
};


class TCreateTopicActor : public TPQGrpcSchemaBase<TCreateTopicActor, NKikimr::NGRpcService::TEvCreateTopicRequest> {
using TBase = TPQGrpcSchemaBase<TCreateTopicActor, TEvCreateTopicRequest>;

public:
     TCreateTopicActor(NKikimr::NGRpcService::TEvCreateTopicRequest* request, const TString& localCluster, const TVector<TString>& clusters);
    ~TCreateTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }

private:
    TString LocalCluster;
    TVector<TString> Clusters;
};


class TPQAlterTopicActor : public TPQGrpcSchemaBase<TPQAlterTopicActor, NKikimr::NGRpcService::TEvPQAlterTopicRequest> {
using TBase = TPQGrpcSchemaBase<TPQAlterTopicActor, TEvPQAlterTopicRequest>;

public:
     TPQAlterTopicActor(NKikimr::NGRpcService::TEvPQAlterTopicRequest* request, const TString& localCluster);
    ~TPQAlterTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx){ Y_UNUSED(ev); Y_UNUSED(ctx); }

private:
    TString LocalCluster;
};


class TAlterTopicActor : public TUpdateSchemeActor<TAlterTopicActor, TEvAlterTopicRequest>
                        , public TCdcStreamCompatible
{
    using TBase = TUpdateSchemeActor<TAlterTopicActor, TEvAlterTopicRequest>;

public:
    TAlterTopicActor(NKikimr::NGRpcService::TEvAlterTopicRequest *request);

    void Bootstrap(const NActors::TActorContext &ctx);
    void ModifyPersqueueConfig(const TActorContext& ctx,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo);
};


}
