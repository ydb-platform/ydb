#pragma once 
 
#include <library/cpp/actors/core/hfunc.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/aclib/aclib.h>
#include "msgbus_server.h" 
 
namespace NKikimr { 
namespace NMsgBusProxy { 
 
struct TMessageBusDbOpsCounters : TAtomicRefCount<TMessageBusDbOpsCounters> { 
    TIntrusivePtr<NMonitoring::TDynamicCounters> DbOperationsCounters; 
    NMon::THistogramCounterHelper RequestTotalTimeHistogram; 
    NMon::THistogramCounterHelper RequestPrepareTimeHistogram; 
    NMon::THistogramCounterHelper RequestUpdateTimeHistogram; 
    NMon::THistogramCounterHelper RequestSelectTimeHistogram; 
    NMon::THistogramCounterHelper RequestSchemaTimeHistogram; 
    NMon::THistogramCounterHelper RequestBatchTimeHistogram; 
    NMon::THistogramCounterHelper RequestQueryTimeHistogram; 
 
    TMessageBusDbOpsCounters(const NMonitoring::TDynamicCounterPtr& counters) { 
        DbOperationsCounters = GetServiceCounters(counters, "proxy")->GetSubgroup("subsystem", "db");
        RequestTotalTimeHistogram.Init(DbOperationsCounters.Get(), "RequestTotalTime", "ms", 1, 20); 
        RequestPrepareTimeHistogram.Init(DbOperationsCounters.Get(), "RequestPrepareTime", "ms", 1, 20); 
        RequestUpdateTimeHistogram.Init(DbOperationsCounters.Get(), "RequestUpdateTime", "ms", 1, 20); 
        RequestSelectTimeHistogram.Init(DbOperationsCounters.Get(), "RequestSelectTime", "ms", 1, 20); 
        RequestSchemaTimeHistogram.Init(DbOperationsCounters.Get(), "RequestSchemaTime", "ms", 1, 20); 
        RequestBatchTimeHistogram.Init(DbOperationsCounters.Get(), "RequestBatchTime", "ms", 1, 20); 
        RequestQueryTimeHistogram.Init(DbOperationsCounters.Get(), "RequestQueryTime", "ms", 1, 20); 
    } 
}; 
 
class TMessageBusServerProxy : public TActorBootstrapped<TMessageBusServerProxy> { 
    TMessageBusServer* const Server;
    std::shared_ptr<IPersQueueGetReadSessionsInfoWorkerFactory> PQReadSessionsInfoWorkerFactory;
 
    TIntrusivePtr<NMonitoring::TDynamicCounters> SchemeCacheCounters; 
 
    TIntrusivePtr<TMessageBusDbOpsCounters> DbOperationsCounters; 
 
    TActorId SchemeCache;
    TActorId PqMetaCache;
 
public: 
    TActorId SelfID;
    TActorId TxProxy;
 
private: 
    void Handle(TEvBusProxy::TEvRequest::TPtr &ev, const TActorContext &ctx); 
    void Handle(TEvBusProxy::TEvNavigate::TPtr &ev, const TActorContext &ctx); 
    void Handle(TEvBusProxy::TEvPersQueue::TPtr &ev, const TActorContext &ctx); 
    void Handle(TEvBusProxy::TEvFlatTxRequest::TPtr &ev, const TActorContext &ctx); 
    void Handle(TEvBusProxy::TEvFlatDescribeRequest::TPtr &ev, const TActorContext &ctx); 
    void Handle(TEvBusProxy::TEvDbSchema::TPtr &ev, const TActorContext &ctx); 
    void Handle(TEvBusProxy::TEvDbOperation::TPtr &ev, const TActorContext &ctx); 
    void Handle(TEvBusProxy::TEvDbBatch::TPtr &ev, const TActorContext &ctx); 
    void Handle(TEvBusProxy::TEvInitRoot::TPtr &ev, const TActorContext &ctx); 
 
public: 
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_PROXY_ACTOR;
    }

    TMessageBusServerProxy(
        TMessageBusServer* server,
        std::shared_ptr<IPersQueueGetReadSessionsInfoWorkerFactory> pqReadSessionsInfoWorkerFactory
    )
        : Server(server) 
        , PQReadSessionsInfoWorkerFactory(pqReadSessionsInfoWorkerFactory)
    { 
    } 
 
    ~TMessageBusServerProxy() { 
        if (Server) {
            Server->ShutdownSession();
        }
    } 
 
    void Bootstrap(const TActorContext &ctx); 
 
    //STFUNC(StateFunc) 
    void StateFunc(TAutoPtr<NActors::IEventHandle> &ev, const NActors::TActorContext &ctx) { 
        switch (ev->GetTypeRewrite()) { 
            HFunc(TEvBusProxy::TEvRequest, Handle); 
            HFunc(TEvBusProxy::TEvPersQueue, Handle); 
            HFunc(TEvBusProxy::TEvFlatTxRequest, Handle); 
            HFunc(TEvBusProxy::TEvFlatDescribeRequest, Handle); 
            HFunc(TEvBusProxy::TEvDbOperation, Handle); 
            HFunc(TEvBusProxy::TEvDbSchema, Handle); 
            HFunc(TEvBusProxy::TEvDbBatch, Handle); 
            HFunc(TEvBusProxy::TEvInitRoot, Handle); 
        } 
    } 
 
    TIntrusivePtr<NACLib::TUserToken> GetUserToken(const TString& tokenString) {
        // TODO: tokenString -> UID -> (cache) -> tokenObject 
        // HACK tokenObject(tokenString) - just for testing purposes 
        TIntrusivePtr<NACLib::TUserToken> tokenObject = new NACLib::TUserToken(tokenString, TVector<NACLib::TSID>());
        return tokenObject; 
    } 
}; 
 
} 
} 
