#pragma once

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/aclib/aclib.h>
#include "msgbus_server.h"

namespace NKikimr {
namespace NMsgBusProxy {

struct TMessageBusDbOpsCounters : TAtomicRefCount<TMessageBusDbOpsCounters> {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> DbOperationsCounters;
    NMonitoring::THistogramPtr RequestTotalTimeHistogram;
    NMonitoring::THistogramPtr RequestPrepareTimeHistogram;
    NMonitoring::THistogramPtr RequestUpdateTimeHistogram;
    NMonitoring::THistogramPtr RequestSelectTimeHistogram;
    NMonitoring::THistogramPtr RequestSchemaTimeHistogram;
    NMonitoring::THistogramPtr RequestBatchTimeHistogram;
    NMonitoring::THistogramPtr RequestQueryTimeHistogram;

    TMessageBusDbOpsCounters(const ::NMonitoring::TDynamicCounterPtr& counters) {
        DbOperationsCounters = GetServiceCounters(counters, "proxy")->GetSubgroup("subsystem", "db");

        RequestTotalTimeHistogram = DbOperationsCounters->GetHistogram("RequestTotalTimeMs",
            NMonitoring::ExponentialHistogram(20, 2, 1));
        RequestPrepareTimeHistogram = DbOperationsCounters->GetHistogram("RequestPrepareTimeMs",
            NMonitoring::ExponentialHistogram(20, 2, 1));
        RequestUpdateTimeHistogram = DbOperationsCounters->GetHistogram("RequestUpdateTimeMs",
            NMonitoring::ExponentialHistogram(20, 2, 1));
        RequestSelectTimeHistogram = DbOperationsCounters->GetHistogram("RequestSelectTimeMs",
            NMonitoring::ExponentialHistogram(20, 2, 1));
        RequestSchemaTimeHistogram = DbOperationsCounters->GetHistogram("RequestSchemaTimeMs",
            NMonitoring::ExponentialHistogram(20, 2, 1));
        RequestBatchTimeHistogram = DbOperationsCounters->GetHistogram("RequestBatchTimeMs",
            NMonitoring::ExponentialHistogram(20, 2, 1));
        RequestQueryTimeHistogram = DbOperationsCounters->GetHistogram("RequestQueryTimeMs",
            NMonitoring::ExponentialHistogram(20, 2, 1));
    }
};

class TMessageBusServerProxy : public TActorBootstrapped<TMessageBusServerProxy> {
    TMessageBusServer* const Server;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> SchemeCacheCounters;

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
    void Handle(TEvBusProxy::TEvInitRoot::TPtr &ev, const TActorContext &ctx);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_PROXY_ACTOR;
    }

    TMessageBusServerProxy(TMessageBusServer* server)
        : Server(server)
    {
    }

    ~TMessageBusServerProxy() {
        if (Server) {
            Server->ShutdownSession();
        }
    }

    void Bootstrap(const TActorContext &ctx);

    //STFUNC(StateFunc)
    void StateFunc(TAutoPtr<NActors::IEventHandle> &ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBusProxy::TEvRequest, Handle);
            HFunc(TEvBusProxy::TEvPersQueue, Handle);
            HFunc(TEvBusProxy::TEvFlatTxRequest, Handle);
            HFunc(TEvBusProxy::TEvFlatDescribeRequest, Handle);
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
