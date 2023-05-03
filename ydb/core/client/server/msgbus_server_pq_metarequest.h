#pragma once
#include "msgbus_server_persqueue.h"

namespace NKikimr {
namespace NMsgBusProxy {

//
// GetTopicMetadata command
//
//
class TPersQueueGetTopicMetadataProcessor : public TPersQueueBaseRequestProcessor {
public:
    TPersQueueGetTopicMetadataProcessor(const NKikimrClient::TPersQueueRequest& request, const TActorId& schemeCache);

private:
    THolder<IActor> CreateTopicSubactor(const TSchemeEntry& topicEntry, const TString& name) override;
};

class TPersQueueGetTopicMetadataTopicWorker : public TReplierToParent<TTopicInfoBasedActor> {
public:
    TPersQueueGetTopicMetadataTopicWorker(const TActorId& parent, const TSchemeEntry& topicEntry, const TString& name);

    void BootstrapImpl(const TActorContext& ctx) override;
    void Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) override;
};


//
// GetPartitionOffsets command
//

class TPersQueueGetPartitionOffsetsProcessor : public TPersQueueBaseRequestProcessor {
public:
    TPersQueueGetPartitionOffsetsProcessor(const NKikimrClient::TPersQueueRequest& request, const TActorId& metaCacheId);

private:
    THolder<IActor> CreateTopicSubactor(const TSchemeEntry& topicEntry, const TString& name) override;

private:
    THashMap<TString, std::shared_ptr<THashSet<ui64>>> PartitionsToRequest;
};

class TPersQueueGetPartitionOffsetsTopicWorker : public TReplierToParent<TPipesWaiterActor<TTopicInfoBasedActor, TEvPersQueue::TEvOffsetsResponse>> {
public:
    TPersQueueGetPartitionOffsetsTopicWorker(const TActorId& parent,
                                             const TSchemeEntry& topicEntry, const TString& name,
                                             const std::shared_ptr<THashSet<ui64>>& partitionsToRequest,
                                             const std::shared_ptr<const NKikimrClient::TPersQueueRequest>& requestProto);

    void BootstrapImpl(const TActorContext& ctx) override;
    bool OnPipeEventsAreReady(const TActorContext& ctx) override;
    void Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) override;

private:
    std::shared_ptr<THashSet<ui64>> PartitionsToRequest;
    std::shared_ptr<const NKikimrClient::TPersQueueRequest> RequestProto;
};


//
// GetPartitionStatus command
//

class TPersQueueGetPartitionStatusProcessor : public TPersQueueBaseRequestProcessor {
public:
    TPersQueueGetPartitionStatusProcessor(const NKikimrClient::TPersQueueRequest& request, const TActorId& schemeCache);

private:
    THolder<IActor> CreateTopicSubactor(const TSchemeEntry& topicEntry, const TString& name) override;

private:
    THashMap<TString, std::shared_ptr<THashSet<ui64>>> PartitionsToRequest;
};

class TPersQueueGetPartitionStatusTopicWorker : public TReplierToParent<TPipesWaiterActor<TTopicInfoBasedActor, TEvPersQueue::TEvStatusResponse>> {
public:
    TPersQueueGetPartitionStatusTopicWorker(const TActorId& parent,
                                            const TTopicInfoBasedActor::TSchemeEntry& topicEntry,
                                            const TString& name,
                                            const std::shared_ptr<THashSet<ui64>>& partitionsToRequest,
                                            const std::shared_ptr<const NKikimrClient::TPersQueueRequest>& requestProto);

    void BootstrapImpl(const TActorContext& ctx) override;
    bool OnPipeEventsAreReady(const TActorContext& ctx) override;
    void Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) override;

private:
    std::shared_ptr<THashSet<ui64>> PartitionsToRequest;
    std::shared_ptr<const NKikimrClient::TPersQueueRequest> RequestProto;
};


//
// GetPartitionLocations command
//

class TPersQueueGetPartitionLocationsProcessor : public TPersQueueBaseRequestProcessor {
public:
    TPersQueueGetPartitionLocationsProcessor(const NKikimrClient::TPersQueueRequest& request, const TActorId& schemeCache);

private:
    THolder<IActor> CreateTopicSubactor(const TSchemeEntry& topicEntry, const TString& name) override;

private:
    THashMap<TString, std::shared_ptr<THashSet<ui64>>> PartitionsToRequest;
};

class TPersQueueGetPartitionLocationsTopicWorker : public TReplierToParent<TPipesWaiterActor<TTopicInfoBasedActor, TEvTabletPipe::TEvClientConnected>> {
public:
    TPersQueueGetPartitionLocationsTopicWorker(const TActorId& parent,
                                               const TTopicInfoBasedActor::TSchemeEntry& topicEntry, const TString& name,
                                               const std::shared_ptr<THashSet<ui64>>& partitionsToRequest,
                                               const std::shared_ptr<const NKikimrClient::TPersQueueRequest>& requestProto,
                                               std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo);

    void BootstrapImpl(const TActorContext& ctx) override;
    bool OnPipeEventsAreReady(const TActorContext& ctx) override;
    void Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) override;

private:
    std::shared_ptr<THashSet<ui64>> PartitionsToRequest;
    std::shared_ptr<const NKikimrClient::TPersQueueRequest> RequestProto;
    THashMap<ui32, ui64> PartitionToTablet;
    std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> NodesInfo;
};


//
// GetReadSessionsInfo command
//

class TPersQueueGetReadSessionsInfoProcessor : public TPersQueueBaseRequestProcessor {
public:
    TPersQueueGetReadSessionsInfoProcessor(
        const NKikimrClient::TPersQueueRequest& request,
        const TActorId& schemeCache
    );

    bool ReadyForAnswer(const TActorContext& ctx) override {
        if (TPersQueueBaseRequestProcessor::ReadyForAnswer(ctx)) {
            if (HasSessionsRequest || ReadSessions.empty()) {
                return true;
            }
            HasSessionsRequest = true;
            auto actorId = ctx.Register(CreateSessionsSubactor(std::move(ReadSessions), ctx).Release());
            Children.emplace(actorId, MakeHolder<TPerTopicInfo>());
        }
        return false;
    }

    void Handle(TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext&) {
        for (auto & s : ev->Get()->Record.GetReadSessions()) {
            if (!s.GetSession().empty()) {
                TActorId actor = ActorIdFromProto(s.GetSessionActor());
                ReadSessions.insert(std::make_pair(s.GetSession(), actor));
            }
        }
    }


    STFUNC(StateFunc) override {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvReadSessionsInfoResponse, Handle);
            default:
                TPersQueueBaseRequestProcessor::StateFunc(ev);
        }
    }

private:

    THolder<IActor> CreateTopicSubactor(const TSchemeEntry& topicEntry, const TString& name) override;
    THolder<IActor> CreateSessionsSubactor(const THashMap<TString, TActorId>&& readSessions, const TActorContext& ctx);

    mutable bool HasSessionsRequest = false;
    THashMap<TString, TActorId> ReadSessions;
};


class TPersQueueGetReadSessionsInfoTopicWorker : public TReplierToParent<TPipesWaiterActor<TTopicInfoBasedActor, TEvPersQueue::TEvOffsetsResponse>> {
public:
    TPersQueueGetReadSessionsInfoTopicWorker(const TActorId& parent,
                                             const TTopicInfoBasedActor::TSchemeEntry& topicEntry, const TString& name,
                                             const std::shared_ptr<const NKikimrClient::TPersQueueRequest>& requestProto,
                                             std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo);

    void BootstrapImpl(const TActorContext& ctx) override;
    void Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) override;
    bool OnPipeEventsAreReady(const TActorContext& ctx) override;

    void Die(const TActorContext& ctx) override;

    void SendReadSessionsInfoToBalancer(const TActorContext& ctx);
    bool ReadyToAnswer() const;

    // true returned from this function means that we called Die().
    [[nodiscard]] bool WaitAllPipeEvents(const TActorContext& ctx);
    STFUNC(WaitAllPipeEventsStateFunc);

    void Handle(TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx);
    TString GetHostName(ui32 hostId) const;
    bool OnClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) override;
    bool HandleConnect(TEvTabletPipe::TEvClientConnected* ev, const TActorContext& ctx);
    bool HandleDestroy(TEvTabletPipe::TEvClientDestroyed* ev, const TActorContext& ctx);

private:
    std::shared_ptr<const NKikimrClient::TPersQueueRequest> RequestProto;
    TActorId BalancerPipe;
    TEvPersQueue::TEvReadSessionsInfoResponse::TPtr BalancerResponse;
    bool BalancerReplied = false;
    bool PipeEventsAreReady = false;
    THashMap<ui32, ui64> PartitionToTablet;
    THashMap<ui64, ui32> TabletNodes;
    std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> NodesInfo;
};

} // namespace NMsgBusProxy
} // namespace NKikimr
