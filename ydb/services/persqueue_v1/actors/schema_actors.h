#pragma once

#include "events.h"
#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/core/persqueue/events/global.h>
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
     TDropTopicActor(NKikimr::NGRpcService::IRequestOpCtx* request);
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

class TDescribeTopicActorImpl
{
protected:
    struct TTabletInfo {
        ui64 TabletId;
        std::vector<ui32> Partitions;
        TActorId Pipe;
        ui32 NodeId = 0;
        ui32 RetriesLeft = 3;
    };
public:
    TDescribeTopicActorImpl(const TString& consumer);
    virtual ~TDescribeTopicActorImpl() = default;

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);

    void Handle(NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQProxy::TEvRequestTablet::TPtr& ev, const TActorContext& ctx);

    bool ProcessTablets(const NKikimrSchemeOp::TPersQueueGroupDescription& description, const TActorContext& ctx);

    void RequestTablet(TTabletInfo& tablet, const TActorContext& ctx);
    void RequestTablet(ui64 tabletId, const TActorContext& ctx);
    void RestartTablet(ui64 tabletId, const TActorContext& ctx, TActorId pipe = {}, const TDuration& delay = TDuration::Zero());
    void RequestAdditionalInfo(const TActorContext& ctx);

    bool StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    void Bootstrap(const NActors::TActorContext& ctx);

    virtual void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) = 0;

    virtual void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) = 0;
    virtual void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) = 0;
    virtual void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) = 0;
    virtual void Reply(const TActorContext& ctx) = 0;

private:

    std::map<ui64, TTabletInfo> Tablets;
    ui32 RequestsInfly = 0;

    ui64 BalancerTabletId = 0;

protected:
    TString Consumer;
};

class TDescribeTopicActor : public TPQGrpcSchemaBase<TDescribeTopicActor, NKikimr::NGRpcService::TEvDescribeTopicRequest>
                          , public TCdcStreamCompatible
                          , public TDescribeTopicActorImpl
{
using TBase = TPQGrpcSchemaBase<TDescribeTopicActor, NKikimr::NGRpcService::TEvDescribeTopicRequest>;
using TTabletInfo = TDescribeTopicActorImpl::TTabletInfo;

public:
     TDescribeTopicActor(NKikimr::NGRpcService::TEvDescribeTopicRequest* request);
     TDescribeTopicActor(NKikimr::NGRpcService::IRequestOpCtx * ctx);

    ~TDescribeTopicActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);
    void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) override;

    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) override;
    virtual void Reply(const TActorContext& ctx) override;

private:
    Ydb::Topic::DescribeTopicResult Result;
};

class TDescribeConsumerActor : public TPQGrpcSchemaBase<TDescribeConsumerActor, NKikimr::NGRpcService::TEvDescribeConsumerRequest>
                          , public TCdcStreamCompatible
                          , public TDescribeTopicActorImpl
{
using TBase = TPQGrpcSchemaBase<TDescribeConsumerActor, NKikimr::NGRpcService::TEvDescribeConsumerRequest>;
using TTabletInfo = TDescribeTopicActorImpl::TTabletInfo;

public:
     TDescribeConsumerActor(NKikimr::NGRpcService::TEvDescribeConsumerRequest* request);
     TDescribeConsumerActor(NKikimr::NGRpcService::IRequestOpCtx * ctx);

    ~TDescribeConsumerActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) override;
    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) override;
    virtual void Reply(const TActorContext& ctx) override;

private:
    Ydb::Topic::DescribeConsumerResult Result;
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
    TCreateTopicActor(NKikimr::NGRpcService::IRequestOpCtx* request);
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
    TAlterTopicActor(NKikimr::NGRpcService::IRequestOpCtx* request);

    void Bootstrap(const NActors::TActorContext &ctx);
    void ModifyPersqueueConfig(const TActorContext& ctx,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo);
};


}
