#pragma once

#include "persqueue.h"
#include "grpc_pq_clusters_updater_actor.h"
#include "grpc_pq_session.h"

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <ydb/library/grpc/server/grpc_request.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NKikimr {
namespace NGRpcProxy {

class TPQReadService : public IPQClustersUpdaterCallback, public std::enable_shared_from_this<TPQReadService> {
    class TSession
        : public ISession<NPersQueue::TReadRequest, NPersQueue::TReadResponse>
    {

    public:
        void OnCreated() override;
        void OnRead(const NPersQueue::TReadRequest& request) override;
        void OnDone() override;
        void OnWriteDone(ui64 size) override;
        void DestroyStream(const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode) override;
        bool IsShuttingDown() const override;

        TSession(std::shared_ptr<TPQReadService> proxy,
             grpc::ServerCompletionQueue* cq, ui64 cookie, const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
             TIntrusivePtr<NMonitoring::TDynamicCounters> counters, bool needDiscoverClusters,
             const NPersQueue::TConverterFactoryPtr& converterFactory);

        void Start() override;
        void SendEvent(NActors::IEventBase* ev);

    private:
        void CreateActor(std::unique_ptr<NPersQueue::TTopicsListController>&& topicsHandler);
        ui64 GetCookie() const;

    private:
        std::shared_ptr<TPQReadService> Proxy;
        const ui64 Cookie;

        NActors::TActorId ActorId;

        const NActors::TActorId SchemeCache;
        const NActors::TActorId NewSchemeCache;

        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
        bool NeedDiscoverClusters;

        NPersQueue::TConverterFactoryPtr TopicConverterFactory;

    };

    using TSessionRef = TIntrusivePtr<TSession>;

public:

    TPQReadService(NGRpcService::TGRpcPersQueueService* service,
                     const std::vector<grpc::ServerCompletionQueue*>& cqs,
                     NActors::TActorSystem* as, const NActors::TActorId& schemeCache, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                     const ui32 maxSessions);

    virtual ~TPQReadService()
    {}

    void RequestSession(::grpc::ServerContext* context, ::grpc::ServerAsyncReaderWriter< ::NPersQueue::TReadResponse, ::NPersQueue::TReadRequest>* stream,
                    ::grpc::CompletionQueue* new_call_cq, ::grpc::ServerCompletionQueue* notification_cq, void *tag)
    {
        Service->GetService()->RequestReadSession(context, stream, new_call_cq, notification_cq, tag);
    }

    void SetupIncomingRequests();

    void StopService() {
        AtomicSet(ShuttingDown_, 1);
    }

    bool IsShuttingDown() const {
        return AtomicGet(ShuttingDown_);
    }

    TVector<TString> GetClusters() const {
        auto g(Guard(Lock));
        return Clusters;
    }
    TString GetLocalCluster() const {
        auto g(Guard(Lock));
        return LocalCluster;
    }

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr GetClassifier() const {
        auto g(Guard(Lock));
        return DatacenterClassifier;
    }

private:
    ui64 NextCookie();

    void CheckClustersListChange(const TVector<TString>& clusters) override;
    void CheckClusterChange(const TString& localCluster, const bool enabled) override;
    void NetClassifierUpdated(NAddressClassifier::TLabeledAddressClassifier::TConstPtr classifier) override;
    void UpdateTopicsHandler();
    //! Unregistry session object.
    void ReleaseSession(ui64 cookie);

    //! Start listening for incoming connections.
    void WaitReadSession();

    bool TooMuchSessions();

private:
    NKikimr::NGRpcService::TGRpcPersQueueService* Service;

    grpc::ServerContext Context;
    std::vector<grpc::ServerCompletionQueue*> CQS;
    NActors::TActorSystem* ActorSystem;
    NActors::TActorId SchemeCache;
    NActors::TActorId NewSchemeCache;

    TAtomic LastCookie = 0;
    TMutex Lock;
    THashMap<ui64, TSessionRef> Sessions;

    TVector<TString> Clusters;
    TString LocalCluster;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    ui32 MaxSessions;

    TAtomic ShuttingDown_ = 0;

    NAddressClassifier::TLabeledAddressClassifier::TConstPtr DatacenterClassifier; // Detects client's datacenter by IP. May be null

    bool NeedDiscoverClusters;
    NPersQueue::TConverterFactoryPtr TopicConverterFactory;
    std::unique_ptr<NPersQueue::TTopicsListController> TopicsHandler;
};


}
}
