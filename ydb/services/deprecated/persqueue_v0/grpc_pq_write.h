#pragma once

#include "grpc_pq_clusters_updater_actor.h"
#include "grpc_pq_session.h"

#include <ydb/core/client/server/grpc_base.h>

#include <ydb/services/deprecated/persqueue_v0/api/grpc/persqueue.grpc.pb.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/system/mutex.h>

namespace NKikimr {
namespace NGRpcProxy {

// Класс, отвечающий за обработку запросов на запись.

class TPQWriteServiceImpl : public IPQClustersUpdaterCallback, public std::enable_shared_from_this<TPQWriteServiceImpl> {

    class TSession : public ISession<NPersQueue::TWriteRequest, NPersQueue::TWriteResponse>
    {

        void OnCreated() override;
        void OnRead(const NPersQueue::TWriteRequest& request) override;
        void OnDone() override;
        void OnWriteDone(ui64) override {};

    public:
        TSession(std::shared_ptr<TPQWriteServiceImpl> proxy,
                 grpc::ServerCompletionQueue* cq, ui64 cookie, const TActorId& schemeCache,
                 TIntrusivePtr<NMonitoring::TDynamicCounters> counters, bool needDiscoverClusters);
        void Start() override;
        ui64 GetCookie() const;
        void DestroyStream(const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode) override;
        bool IsShuttingDown() const override;

    private:
        [[nodiscard]] bool CreateActor(const TString& localCluster);
        void SendEvent(NActors::IEventBase* ev);

    private:
        std::shared_ptr<TPQWriteServiceImpl> Proxy;
        const ui64 Cookie;

        NActors::TActorId ActorId;

        const NActors::TActorId SchemeCache;

        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

        bool NeedDiscoverClusters;
        bool IsDone = false;
    };
    using TSessionRef = TIntrusivePtr<TSession>;

public:
     TPQWriteServiceImpl(const std::vector<grpc::ServerCompletionQueue*>& cqs,
                     NActors::TActorSystem* as, const NActors::TActorId& schemeCache, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                     const ui32 maxSessions);
    virtual ~TPQWriteServiceImpl() = default;

    void SetupIncomingRequests();

    virtual void RequestSession(::grpc::ServerContext* context, ::grpc::ServerAsyncReaderWriter< ::NPersQueue::TWriteResponse, ::NPersQueue::TWriteRequest>* stream,
                    ::grpc::CompletionQueue* new_call_cq, ::grpc::ServerCompletionQueue* notification_cq, void *tag) = 0;

    void StopService() {
        AtomicSet(ShuttingDown_, 1);
    }

    bool IsShuttingDown() const {
        return AtomicGet(ShuttingDown_);
    }
    void InitClustersUpdater();

private:
    ui64 NextCookie();

    //! Unregistry session object.
    void ReleaseSession(TSessionRef session);

    //! Start listening for incoming connections.
    void WaitWriteSession();
    bool TooMuchSessions();
    TMaybe<TString> AvailableLocalCluster();
    NAddressClassifier::TLabeledAddressClassifier::TConstPtr GetClassifier() const {
        auto g(Guard(Lock));
        return DatacenterClassifier;
    }
    void CheckClusterChange(const TString& localCluster, const bool enabled) override;
    void NetClassifierUpdated(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier) override;

private:
    grpc::ServerContext Context;
    const std::vector<grpc::ServerCompletionQueue*>& CQS;

    NActors::TActorSystem* ActorSystem;
    NActors::TActorId SchemeCache;

    TAtomic LastCookie = 0;

    TMutex Lock;
    THashMap<ui64, TSessionRef> Sessions;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    ui32 MaxSessions;
    TMaybe<TString> AvailableLocalClusterName;
    TString SelectSourceIdQuery;
    TString UpdateSourceIdQuery;
    TString DeleteSourceIdQuery;

    TAtomic ShuttingDown_ = 0;

    bool NeedDiscoverClusters; // Legacy mode OR account-mode in multi-cluster setup;

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier; // Detects client's datacenter by IP. May be null
};


class TPQWriteService : public TPQWriteServiceImpl {
public:
    TPQWriteService(NPersQueue::PersQueueService::AsyncService* service,
                     const std::vector<grpc::ServerCompletionQueue*>& cqs,
                     NActors::TActorSystem* as, const NActors::TActorId& schemeCache, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                     const ui32 maxSessions)
        : TPQWriteServiceImpl(cqs, as, schemeCache, counters, maxSessions)
        , Service(service)
    {}

    virtual ~TPQWriteService()
    {}

    void RequestSession(::grpc::ServerContext* context, ::grpc::ServerAsyncReaderWriter< ::NPersQueue::TWriteResponse, ::NPersQueue::TWriteRequest>* stream,
                    ::grpc::CompletionQueue* new_call_cq, ::grpc::ServerCompletionQueue* notification_cq, void *tag) override
    {
        Service->RequestWriteSession(context, stream, new_call_cq, notification_cq, tag);
    }

private:
    NPersQueue::PersQueueService::AsyncService* Service;
};


}
}
