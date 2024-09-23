#include "proxy.h"
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/tx_allocator_client/client.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/protos/counters_tx_proxy.pb.h>

namespace NKikimr {
using namespace NTabletFlatExecutor;

namespace NTxProxy {

template<class EventType>
struct TDelayedQueue {
    typedef typename EventType::TPtr EventTypePtr;
    struct TRequest : public std::pair<typename EventType::TPtr, TInstant> {
        typedef std::pair<typename EventType::TPtr, TInstant> TBase;

        using TBase::TBase; //import constructors

        EventTypePtr& GetRequest() {
            return TBase::first;
        }
        TInstant GetExpireMoment() const {
            return TBase::second;
        }
    };
    typedef TAutoPtr<TRequest> TRequestPtr;
    typedef TOneOneQueueInplace<TRequest*, 64> TQueueType;
    typedef TAutoPtr<TQueueType, typename TQueueType::TPtrCleanDestructor> TSafeQueue;
    TSafeQueue Queue;

    TDelayedQueue()
        : Queue(new TQueueType())
    {}
};

class TTxProxy : public TActorBootstrapped<TTxProxy> {
    TTxProxyServices Services;

    THolder<NTabletPipe::IClientCache> PipeClientCache;
    TTxAllocatorClient TxAllocatorClient;

    static const TDuration TimeoutDelayedRequest;
    typedef TDelayedQueue<TEvTxUserProxy::TEvProposeTransaction> TDelayedProposal;
    TDelayedProposal DelayedProposal;
    typedef TDelayedQueue<TEvTxUserProxy::TEvProposeKqpTransaction> TDelayedKqpProposal;
    TDelayedKqpProposal DelayedKqpProposal;
    typedef TDelayedQueue<TEvTxUserProxy::TEvAllocateTxId> TDelayedAllocateTxId;
    TDelayedAllocateTxId DelayedAllocateTxId;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> CacheCounters;
    TIntrusivePtr<TTxProxyMon> TxProxyMon;
    TRequestControls RequestControls;

    void Die(const TActorContext &ctx) override {
        ctx.Send(Services.SchemeCache, new TEvents::TEvPoisonPill());
        ctx.Send(Services.LeaderPipeCache, new TEvents::TEvPoisonPill());
        ctx.Send(Services.FollowerPipeCache, new TEvents::TEvPoisonPill());

        PipeClientCache->Detach(ctx);
        PipeClientCache.Destroy();

        return TActor::Die(ctx);
    }

    void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() <<
                    " HANDLE TEvPoisonPill" <<
                    " from Sender# " << ev->Sender.ToString());
        Die(ctx);
    }

    void ReplyDecline(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status, const TEvTxUserProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TX_PROXY,
                   "actor# " << SelfId() <<
                   " DECLINE TEvProposeTransactionStatus " <<
                   " reason# " << status <<
                   " reply to# " << ev->Sender.ToString());

        ctx.Send(ev->Sender, new TEvTxUserProxy::TEvProposeTransactionStatus(status), 0, ev->Cookie);
    }

    void ReplyNotImplemented(const TEvTxUserProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
        TxProxyMon->TxNotImplemented->Inc();
        const NKikimrTxUserProxy::TTransaction &tx = ev->Get()->Record.GetTransaction();
        LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() <<
                    " Cookie# " << (ui64)ev->Cookie <<
                    " userReqId# \"" << tx.GetUserRequestId() << "\"" <<
                    " RESPONSE Status# NotImplemented Type# Unknown");
        return ReplyDecline(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::NotImplemented, ev, ctx);
    }

    void Decline(const TEvTxUserProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
        if (ev->Get()->HasSchemeProposal()) {
            LOG_WARN_S(ctx, NKikimrServices::TX_PROXY,
                       "actor# " << SelfId() <<
                       " DECLINE TEvProposeTransaction SchemeRequest" <<
                       " reason# " << TEvTxUserProxy::TResultStatus::EStatus::ProxyNotReady <<
                       " replyto# " << ev->Sender.ToString());

            TxProxyMon->SchemeRequestProxyNotReady->Inc();
            return ReplyDecline(TEvTxUserProxy::TResultStatus::EStatus::ProxyNotReady, ev, ctx);
        }

        if (ev->Get()->HasMakeProposal()) {
            LOG_WARN_S(ctx, NKikimrServices::TX_PROXY,
                       "actor# " << SelfId() <<
                       " DECLINE TEvProposeTransaction DataReques" <<
                       " reason# " << TEvTxUserProxy::TResultStatus::EStatus::ProxyNotReady <<
                       " replyto# " << ev->Sender.ToString());

            TxProxyMon->MakeRequestProxyNotReady->Inc();
            return ReplyDecline(TEvTxUserProxy::TResultStatus::EStatus::ProxyNotReady, ev, ctx);
        }

        ReplyNotImplemented(ev, ctx);
    }

    void DelayRequest(TEvTxUserProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
        auto request = new TDelayedProposal::TRequest(ev, ctx.Now() + TimeoutDelayedRequest);
        DelayedProposal.Queue->Push(request);

    }

    void DelayRequest(TEvTxUserProxy::TEvProposeKqpTransaction::TPtr &ev, const TActorContext &ctx) {
        auto request = new TDelayedKqpProposal::TRequest(ev, ctx.Now() + TimeoutDelayedRequest);
        DelayedKqpProposal.Queue->Push(request);
    }

    void DelayRequest(TEvTxUserProxy::TEvAllocateTxId::TPtr &ev, const TActorContext &ctx) {
        auto request = new TDelayedAllocateTxId::TRequest(ev, ctx.Now() + TimeoutDelayedRequest);
        DelayedAllocateTxId.Queue->Push(request);
    }

    template<class EventType>
    void PlayQueue(TDelayedQueue<EventType> &delayed, const TActorContext &ctx) {
        typedef typename TDelayedQueue<EventType>::TRequestPtr TRequestPtr;

        while (delayed.Queue->Head()) {
            TVector<ui64> txIds = TxAllocatorClient.AllocateTxIds(1, ctx);
            if (!txIds) {
                return;
            }
            TRequestPtr extracted = delayed.Queue->Pop();
            ProcessRequest(extracted->GetRequest(), ctx, txIds.front());
        }
    }

    void PlayDelayed(const TActorContext &ctx) {
        PlayQueue(DelayedProposal, ctx);
        PlayQueue(DelayedKqpProposal, ctx);
        PlayQueue(DelayedAllocateTxId, ctx);
    }

    template<class EventType>
    void CheckTimeout(TDelayedQueue<EventType> &delayed, const TActorContext &ctx) {
        typedef typename TDelayedQueue<EventType>::TRequestPtr TRequestPtr;

        while (const auto head = delayed.Queue->Head()) {
            const TInstant &expireAt = head->GetExpireMoment();
            if (expireAt > ctx.Now()) {
                break;
            }
            TRequestPtr extracted = delayed.Queue->Pop();
            Decline(extracted->GetRequest(), ctx);
        }
    }

    void CheckTimeouts(const TActorContext &ctx) {
        CheckTimeout(DelayedProposal, ctx);
    }

    static NTabletPipe::TClientConfig InitPipeClientConfig() {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {
            .RetryLimitCount = 3,
            .MinRetryTime = TDuration::MilliSeconds(100),
            .MaxRetryTime = TDuration::Seconds(1),
            .BackoffMultiplier = 5,
        };
        return config;
    }

    static const NTabletPipe::TClientConfig& GetPipeClientConfig() {
        static const NTabletPipe::TClientConfig config = InitPipeClientConfig();
        return config;
    }

    void Handle(TEvTxAllocator::TEvAllocateResult::TPtr &ev, const TActorContext &ctx) {
        if (!TxAllocatorClient.OnAllocateResult(ev, ctx)) {
            return;
        }

        PlayDelayed(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                   "actor# " << SelfId() <<
                   " HANDLE TEvClientConnected " << (PipeClientCache->OnConnect(ev) ? "success connect" : "fail connect") <<
                   " from tablet# " << msg->TabletId);

        if(!PipeClientCache->OnConnect(ev)) {
            CheckTimeouts(ctx);
            TxAllocatorClient.SendRequest(msg->TabletId, ctx);
            return;
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
        LOG_WARN_S(ctx, NKikimrServices::TX_PROXY,
                   "actor# " << SelfId() <<
                   " HANDLE TEvClientDestroyed" <<
                   " from tablet# " << msg->TabletId);

        PipeClientCache->OnDisconnect(ev);
        CheckTimeouts(ctx);
        TxAllocatorClient.SendRequest(msg->TabletId, ctx);
    }

    void ProcessRequest(TEvTxUserProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx, ui64 txid) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() <<
                    " TxId# " << txid <<
                    " ProcessProposeTransaction");

        RequestControls.Reqister(ctx);

        // process scheme transactions
        const NKikimrTxUserProxy::TTransaction &tx = ev->Get()->Record.GetTransaction();
        if (ev->Get()->HasSchemeProposal()) {
            // todo: in-fly and shutdown
            Y_DEBUG_ABORT_UNLESS(txid != 0);
            ui64 cookie = ev->Cookie;
            const TString userRequestId = tx.GetUserRequestId();
            TAutoPtr<TEvTxProxyReq::TEvSchemeRequest> request = new TEvTxProxyReq::TEvSchemeRequest(ev);
            const TActorId reqId = ctx.ExecutorThread.RegisterActor(CreateTxProxyFlatSchemeReq(Services, txid, request, TxProxyMon));
            TxProxyMon->SchemeRequest->Inc();
            LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                         "actor# " << SelfId() <<
                         " Cookie# " << cookie <<
                         " userReqId# \"" << userRequestId << "\""
                         " txid# " << txid <<
                         " SEND to# " << reqId.ToString().data());
            return;
        }

        // otherwise process data transaction
        if (ev->Get()->HasMakeProposal()) {
            // todo: in-fly and shutdown
            Y_DEBUG_ABORT_UNLESS(txid != 0);
            const TActorId reqId = ctx.ExecutorThread.RegisterActor(CreateTxProxyDataReq(Services, txid, TxProxyMon, RequestControls));
            TxProxyMon->MakeRequest->Inc();
            LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                         "actor# " << SelfId() <<
                         " Cookie# " << (ui64)ev->Cookie <<
                         " userReqId# \"" << tx.GetUserRequestId() << "\"" <<
                         " txid# " << txid <<
                         " SEND to# " << reqId.ToString().data() <<
                         " DataReq marker# P0");
            ctx.Send(reqId, new TEvTxProxyReq::TEvMakeRequest(ev));
            return;
        }

        if (ev->Get()->HasSnapshotProposal()) {
            auto cookie = ev->Cookie;
            auto userReqId = tx.GetUserRequestId();
            const TActorId reqId = ctx.ExecutorThread.RegisterActor(CreateTxProxySnapshotReq(Services, txid, std::move(ev), TxProxyMon));
            TxProxyMon->SnapshotRequest->Inc();
            LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                         "actor# " << SelfId() <<
                         " Cookie# " << cookie <<
                         " userReqId# \"" << userReqId << "\"" <<
                         " txid# " << txid <<
                         " reqId# " << reqId.ToString() <<
                         " SnapshotReq marker# P0");
            return;
        }

        if (ev->Get()->HasCommitWritesProposal()) {
            auto cookie = ev->Cookie;
            auto userReqId = tx.GetUserRequestId();
            const TActorId reqId = ctx.ExecutorThread.RegisterActor(CreateTxProxyCommitWritesReq(Services, txid, std::move(ev), TxProxyMon));
            TxProxyMon->CommitWritesRequest->Inc();
            LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                         "actor# " << SelfId() <<
                         " Cookie# " << cookie <<
                         " userReqId# \"" << userReqId << "\"" <<
                         " txid# " << txid <<
                         " reqId# " << reqId.ToString() <<
                         " CommitWritesReq marker# P0");
            return;
        }

        ReplyNotImplemented(ev, ctx);
    }

    void Handle(TEvTxUserProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
        const NKikimrTxUserProxy::TTransaction &tx = ev->Get()->Record.GetTransaction();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                   "actor# " << SelfId() <<
                   " Handle TEvProposeTransaction");

        const bool needTxId = ev->Get()->NeedTxId();

        if (needTxId) {
            auto txIds = TxAllocatorClient.AllocateTxIds(1, ctx);
            if (!txIds) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                           "actor# " << SelfId() <<
                           " Cookie# " << (ui64)ev->Cookie <<
                           " userReqId# \"" << tx.GetUserRequestId() << "\""
                           " DELAY REQUEST, wait txids from allocator" <<
                           " Type# Scheme");
                return DelayRequest(ev, ctx);
            }

            return ProcessRequest(ev, ctx, txIds.front());
        }

        return ProcessRequest(ev, ctx, 0);
    }

    void ProcessRequest(TEvTxUserProxy::TEvProposeKqpTransaction::TPtr& ev, const TActorContext& ctx, ui64 txid) {
        TxProxyMon->KqpRequest->Inc();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() <<
                    " TxId# " << txid <<
                    " ProcessProposeKqpTransaction");

        auto executerEv = MakeHolder<NKqp::TEvKqpExecuter::TEvTxRequest>();
        ActorIdToProto(ev->Sender, executerEv->Record.MutableTarget());
        executerEv->Record.MutableRequest()->SetTxId(txid);
        ctx.Send(ev->Get()->ExecuterId, executerEv.Release());
    }

    void Handle(TEvTxUserProxy::TEvProposeKqpTransaction::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                   "actor# " << SelfId() <<
                   " Handle TEvExecuteKqpTransaction");

        auto txIds = TxAllocatorClient.AllocateTxIds(1, ctx);
        if (!txIds) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                       "actor# " << SelfId() <<
                       " Cookie# " << (ui64)ev->Cookie <<
                       " DELAY REQUEST, wait txids from allocator" <<
                       " Type# Scheme");
            return DelayRequest(ev, ctx);
        }

        ProcessRequest(ev, ctx, txIds.front());
    }

    void ProcessRequest(TEvTxUserProxy::TEvAllocateTxId::TPtr &ev, const TActorContext &ctx, ui64 txId) {
        auto reply = MakeHolder<TEvTxUserProxy::TEvAllocateTxIdResult>(txId, Services, TxProxyMon);
        ctx.Send(ev->Sender, reply.Release(), 0, ev->Cookie);
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxId::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() << " Handle TEvAllocateTxId");

        auto txIds = TxAllocatorClient.AllocateTxIds(1, ctx);
        if (!txIds) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                        "actor# " << SelfId()
                        << " Cookie# " << ev->Cookie
                        << " DELAY REQUEST, wait txids from allocator"
                        << " Type# AllocateTxId");
            return DelayRequest(ev, ctx);
        }

        ProcessRequest(ev, ctx, txIds.front());
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesRequest::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() << " Handle TEvGetProxyServicesRequest");

        auto reply = MakeHolder<TEvTxUserProxy::TEvGetProxyServicesResponse>(Services);
        ctx.Send(ev->Sender, reply.Release(), 0, ev->Cookie);
    }

    void Handle(TEvTxUserProxy::TEvNavigate::TPtr &ev, const TActorContext &ctx) {
        TString path = ev->Get()->Record.GetDescribePath().GetPath();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                     "actor# " << SelfId() <<
                     " Handle TEvNavigate " <<
                     " describe path " << path);

        TxProxyMon->Navigate->Inc();
        TActorId reqId = ctx.ExecutorThread.RegisterActor(CreateTxProxyDescribeFlatSchemeReq(Services, TxProxyMon));
        ctx.Send(reqId, new TEvTxProxyReq::TEvNavigateScheme(ev));
    }

    void Handle(TEvTxUserProxy::TEvInvalidateTable::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() <<
                    " HANDLE EvInvalidateTable");
        ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(TTableId(ev.Get()->Get()->Record.GetSchemeShardId(), ev.Get()->Get()->Record.GetTableId()), ev.Get()->Sender));
    }

    void Handle(TEvTxProxySchemeCache::TEvInvalidateTableResult::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() <<
                    " HANDLE EvInvalidateTableResult");
        ctx.Send(ev.Get()->Get()->Sender, new TEvTxUserProxy::TEvInvalidateTableResult);
    }

public:
    TTxProxy(const TVector<ui64> &txAllocators)
        : PipeClientCache(NTabletPipe::CreateUnboundedClientCache(GetPipeClientConfig()))
        , TxAllocatorClient(NKikimrServices::TX_PROXY, PipeClientCache.Get(), txAllocators)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() <<
                    " Bootstrap");
        TxProxyMon = new TTxProxyMon(AppData(ctx)->Counters);
        CacheCounters = GetServiceCounters(AppData(ctx)->Counters, "proxy")->GetSubgroup("subsystem", "cache");

        Services.Proxy = SelfId();

        auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(AppData(ctx), CacheCounters);
        Services.SchemeCache = ctx.ExecutorThread.RegisterActor(CreateSchemeBoardSchemeCache(cacheConfig.Get()));
        ctx.ExecutorThread.ActorSystem->RegisterLocalService(MakeSchemeCacheID(), Services.SchemeCache);

        // PipePerNodeCaches are an external dependency
        Services.LeaderPipeCache = MakePipePerNodeCacheID(false);
        Services.FollowerPipeCache = MakePipePerNodeCacheID(true);

        TxAllocatorClient.Bootstrap(ctx);

        Become(&TThis::StateWork);
        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                    "actor# " << SelfId() <<
                    " Become StateWork (SchemeCache " << Services.SchemeCache << ")");
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_PROXY_ACTOR;
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTxAllocator::TEvAllocateResult, Handle);

            HFunc(TEvTxUserProxy::TEvNavigate, Handle);
            HFunc(TEvTxUserProxy::TEvProposeTransaction, Handle);

            HFunc(TEvTxUserProxy::TEvInvalidateTable, Handle);
            HFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult, Handle);

            HFunc(TEvTxUserProxy::TEvProposeKqpTransaction, Handle);
            HFunc(TEvTxUserProxy::TEvAllocateTxId, Handle);

            HFunc(TEvTxUserProxy::TEvGetProxyServicesRequest, Handle);

            HFunc(TEvents::TEvPoisonPill, Handle);
        default:
            ALOG_ERROR(NKikimrServices::TX_PROXY,
                        "actor# " << SelfId() <<
                            " IGNORING message type# " <<  ev->GetTypeRewrite() <<
                            " from Sender# " << ev->Sender.ToString() <<
                            " at StateWork");
            break;
        }
    }
};

const TDuration TTxProxy::TimeoutDelayedRequest = TDuration::Seconds(15);

}

IActor* CreateTxProxy(const TVector<ui64> &allocators) {
    return new NTxProxy::TTxProxy(allocators);
}

}
