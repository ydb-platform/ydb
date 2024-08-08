#pragma once

#include "events.h"
#include <ydb/core/persqueue/events/global.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/core/client/server/ic_nodes_cache_service.h>

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

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev){ Y_UNUSED(ev); }
};

class TDropTopicActor : public TPQGrpcSchemaBase<TDropTopicActor, NKikimr::NGRpcService::TEvDropTopicRequest>, public TDropPropose {
using TBase = TPQGrpcSchemaBase<TDropTopicActor, TEvDropTopicRequest>;

public:
     TDropTopicActor(NKikimr::NGRpcService::TEvDropTopicRequest* request);
     TDropTopicActor(NKikimr::NGRpcService::IRequestOpCtx* request);
    ~TDropTopicActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev){ Y_UNUSED(ev); }
};

class TPQDescribeTopicActor : public TPQGrpcSchemaBase<TPQDescribeTopicActor, NKikimr::NGRpcService::TEvPQDescribeTopicRequest>
                            , public TCdcStreamCompatible
{
using TBase = TPQGrpcSchemaBase<TPQDescribeTopicActor, TEvPQDescribeTopicRequest>;

public:
     TPQDescribeTopicActor(NKikimr::NGRpcService::TEvPQDescribeTopicRequest* request);
    ~TPQDescribeTopicActor() = default;

    void StateWork(TAutoPtr<IEventHandle>& ev);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
};

struct TDescribeTopicActorSettings {
    enum class EMode {
        DescribeTopic,
        DescribeConsumer,
        DescribePartitions,
    };
    EMode Mode;
    TString Consumer;
    TSet<TString> Consumers;
    TVector<ui32> Partitions;
    bool RequireStats = false;
    bool RequireLocation = false;

    TDescribeTopicActorSettings()
        : Mode(EMode::DescribeTopic)
    {}

    TDescribeTopicActorSettings(const TString& consumer)
        : Mode(EMode::DescribeConsumer)
        , Consumer(consumer)
    {}
    TDescribeTopicActorSettings(EMode mode, bool requireStats, bool requireLocation)
        : Mode(mode)
        , RequireStats(requireStats)
        , RequireLocation(requireLocation)
    {}

    static TDescribeTopicActorSettings DescribeTopic(bool requireStats, bool requireLocation, const TVector<ui32>& partitions = {}) {
        TDescribeTopicActorSettings res{EMode::DescribeTopic, requireStats, requireLocation};
        res.Partitions = partitions;
        return res;
    }

    static TDescribeTopicActorSettings DescribeConsumer(const TString& consumer, bool requireStats, bool requireLocation)
    {
        TDescribeTopicActorSettings res{EMode::DescribeConsumer, requireStats, requireLocation};
        res.Consumer = consumer;
        return res;
    }

    static TDescribeTopicActorSettings GetPartitionsLocation(const TVector<ui32>& partitions) {
        TDescribeTopicActorSettings res{EMode::DescribePartitions, false, true};
        res.Partitions = partitions;
        return res;
    }

    static TDescribeTopicActorSettings DescribePartitionSettings(ui32 partition, bool stats, bool location) {
        TDescribeTopicActorSettings res{EMode::DescribePartitions, stats, location};
        res.Partitions = {partition};
        return res;
    }

};

class TDescribeTopicActorImpl
{
protected:
    struct TTabletInfo {
        ui64 TabletId = 0;
        std::vector<ui32> Partitions;
        TActorId Pipe;
        ui32 NodeId = 0;
        ui32 RetriesLeft = 3;
        bool ResultRecived = false;
        ui64 Generation = 0;
        TTabletInfo() = default;
        TTabletInfo(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

public:
    TDescribeTopicActorImpl(const TDescribeTopicActorSettings& settings);
    virtual ~TDescribeTopicActorImpl() = default;

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);

    void Handle(NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQProxy::TEvRequestTablet::TPtr& ev, const TActorContext& ctx);

    bool ProcessTablets(const NKikimrSchemeOp::TPersQueueGroupDescription& description, const TActorContext& ctx);

    void RequestTablet(TTabletInfo& tablet, const TActorContext& ctx);
    void RequestTablet(ui64 tabletId, const TActorContext& ctx);
    void RestartTablet(ui64 tabletId, const TActorContext& ctx, TActorId pipe = {}, const TDuration& delay = TDuration::Zero());
    void RequestBalancer(const TActorContext& ctx);
    void RequestPartitionStatus(const TTabletInfo& tablet, const TActorContext& ctx);
    void RequestPartitionsLocation(const TActorContext& ctx);
    void RequestReadSessionsInfo(const TActorContext& ctx);
    void CheckCloseBalancerPipe(const TActorContext& ctx);

    bool StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

    virtual void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) = 0;

    virtual void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
                            const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) = 0;
    virtual void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev,
                               const TActorContext& ctx) = 0;
    virtual void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev,
                               const TActorContext& ctx) = 0;
    virtual bool ApplyResponse(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr&, const TActorContext&) = 0;

    virtual void Reply(const TActorContext& ctx) = 0;

private:
    std::map<ui64, TTabletInfo> Tablets;
    ui32 RequestsInfly = 0;

    bool GotLocation = false;
    bool GotReadSessions = false;

    TActorId* BalancerPipe = nullptr;

protected:
    ui64 BalancerTabletId = 0;
    ui32 TotalPartitions = 0;
    TDescribeTopicActorSettings Settings;
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

    void StateWork(TAutoPtr<IEventHandle>& ev);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) override;
    bool ApplyResponse(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext& ctx) override;
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

    void StateWork(TAutoPtr<IEventHandle>& ev);

    void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) override;
    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) override;
    bool ApplyResponse(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext& ctx) override;
    virtual void Reply(const TActorContext& ctx) override;

private:
    Ydb::Topic::DescribeConsumerResult Result;
};

class TDescribePartitionActor : public TPQGrpcSchemaBase<TDescribePartitionActor, NKikimr::NGRpcService::TEvDescribePartitionRequest>
                              , public TDescribeTopicActorImpl
{
using TBase = TPQGrpcSchemaBase<TDescribePartitionActor, NKikimr::NGRpcService::TEvDescribePartitionRequest>;
using TTabletInfo = TDescribeTopicActorImpl::TTabletInfo;

public:
     TDescribePartitionActor(NKikimr::NGRpcService::TEvDescribePartitionRequest* request);
     TDescribePartitionActor(NKikimr::NGRpcService::IRequestOpCtx * ctx);

    ~TDescribePartitionActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    void StateWork(TAutoPtr<IEventHandle>& ev);

    void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
                    const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) override;
    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) override;
    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) override;
    bool ApplyResponse(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext& ctx) override;

    virtual void Reply(const TActorContext& ctx) override;

private:

    bool NeedToRequestWithDescribeSchema(TAutoPtr<IEventHandle>& ev);

private:
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> PQGroupInfo;
    Ydb::Topic::DescribePartitionResult Result;
};

class TAddReadRuleActor : public TUpdateSchemeActor<TAddReadRuleActor, TEvPQAddReadRuleRequest>
                        , public TCdcStreamCompatible
{
    using TBase = TUpdateSchemeActor<TAddReadRuleActor, TEvPQAddReadRuleRequest>;

public:
    TAddReadRuleActor(NKikimr::NGRpcService::TEvPQAddReadRuleRequest *request);

    void Bootstrap(const NActors::TActorContext& ctx);
    void ModifyPersqueueConfig(TAppData* appData,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo) override;
};

class TRemoveReadRuleActor : public TUpdateSchemeActor<TRemoveReadRuleActor, TEvPQRemoveReadRuleRequest>
                           , public TCdcStreamCompatible
{
    using TBase = TUpdateSchemeActor<TRemoveReadRuleActor, TEvPQRemoveReadRuleRequest>;

public:
    TRemoveReadRuleActor(NKikimr::NGRpcService::TEvPQRemoveReadRuleRequest* request);

    void Bootstrap(const NActors::TActorContext &ctx);
    void ModifyPersqueueConfig(TAppData* appData,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo) override;
};


class TPQCreateTopicActor : public TPQGrpcSchemaBase<TPQCreateTopicActor, NKikimr::NGRpcService::TEvPQCreateTopicRequest> {
    using TBase = TPQGrpcSchemaBase<TPQCreateTopicActor, TEvPQCreateTopicRequest>;

public:
    TPQCreateTopicActor(NKikimr::NGRpcService::TEvPQCreateTopicRequest* request, const TString& localCluster, const TVector<TString>& clusters);
    ~TPQCreateTopicActor() = default;

    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                             const TString& workingDir, const TString& name);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev){ Y_UNUSED(ev); }

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

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev){ Y_UNUSED(ev); }

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

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev){ Y_UNUSED(ev); }

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

    void Bootstrap(const NActors::TActorContext& ctx);
    void ModifyPersqueueConfig(TAppData* appData,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo) override;
};


class TAlterTopicActorInternal : public TPQInternalSchemaActor<TAlterTopicActorInternal, NKikimr::NGRpcProxy::V1::TAlterTopicRequest,
                                                               TEvPQProxy::TEvAlterTopicResponse>
                               , public TUpdateSchemeActorBase<TAlterTopicActorInternal>
                               , public TCdcStreamCompatible
{
    using TUpdateSchemeBase = TUpdateSchemeActorBase<TAlterTopicActorInternal>;
    using TRequest = NKikimr::NGRpcProxy::V1::TAlterTopicRequest;
    using TActorBase = TPQInternalSchemaActor<TAlterTopicActorInternal, TRequest, TEvPQProxy::TEvAlterTopicResponse>;

public:
    TAlterTopicActorInternal(NKikimr::NGRpcProxy::V1::TAlterTopicRequest&& request,
                             NThreading::TPromise<TAlterTopicResponse>&& promise,
                             bool notExistsOk);

    void Bootstrap(const NActors::TActorContext& ctx) override;
    void ModifyPersqueueConfig(TAppData* appData,
                               NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                               const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                               const NKikimrSchemeOp::TDirEntry& selfInfo) override;

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) override;

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        TActorBase::StateWork(ev);
    }

protected:
    bool RespondOverride(Ydb::StatusIds::StatusCode status, bool notFound) override;

private:
    NThreading::TPromise<TAlterTopicResponse> Promise;
    bool MissingOk;
};

class TPartitionsLocationActor : public TPQInternalSchemaActor<TPartitionsLocationActor,
                                                               TGetPartitionsLocationRequest,
                                                               TEvPQProxy::TEvPartitionLocationResponse>
                               , public TDescribeTopicActorImpl {

using TBase = TPQInternalSchemaActor<TPartitionsLocationActor, TGetPartitionsLocationRequest,
                                     TEvPQProxy::TEvPartitionLocationResponse>;

public:
    TPartitionsLocationActor(const TGetPartitionsLocationRequest& request, const TActorId& requester);

    ~TPartitionsLocationActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx) override;

    void StateWork(TAutoPtr<IEventHandle>& ev);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) override;
    void ApplyResponse(TTabletInfo&,
                      NKikimr::TEvPersQueue::TEvStatusResponse::TPtr&,
                      const TActorContext&) override {
        Y_ABORT();
    }
    virtual void ApplyResponse(TTabletInfo&, TEvPersQueue::TEvReadSessionsInfoResponse::TPtr&,
                               const TActorContext&) override {
        Y_ABORT();
    }

    void Finalize();

    bool ApplyResponse(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext& ctx) override;
    void Reply(const TActorContext&) override {};

    void Handle(NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse::TPtr& ev);
    void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext&) override;
private:
    void SendNodesRequest() const;

    NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse::TPtr NodesInfoEv;
    THashSet<ui64> PartitionIds;

    bool GotPartitions = false;
    bool GotNodesInfo = false;
};

} // namespace NKikimr::NGRpcProxy::V1
