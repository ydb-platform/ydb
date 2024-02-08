#pragma once

#include "grpc_server.h"
#include "msgbus_tabletreq.h"
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>

#include <util/generic/ptr.h>
#include <util/system/compiler.h>

namespace NKikimr {
namespace NMsgBusProxy {

const TString& TopicPrefix(const TActorContext& ctx);

struct TProcessingResult {
    EResponseStatus Status = MSTATUS_OK;
    NPersQueue::NErrorCode::EErrorCode ErrorCode;
    TString Reason;
    bool IsFatal = false;
};

TProcessingResult ProcessMetaCacheAllTopicsResponse(NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeAllTopicsResponse::TPtr& response);
TProcessingResult ProcessMetaCacheSingleTopicsResponse(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry);

// Worker actor creation
IActor* CreateMessageBusServerPersQueue(
    TBusMessageContext& msg,
    const TActorId& schemeCache
);
IActor* CreateActorServerPersQueue(
    const TActorId& parentId,
    const NKikimrClient::TPersQueueRequest& request,
    const TActorId& schemeCache
);


NKikimrClient::TResponse CreateErrorReply(EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason);

template <class TResponseEvent>
inline ui64 GetTabletId(const TResponseEvent* ev) {
    return ev->Record.GetTabletId();
}

template <>
inline ui64 GetTabletId<TEvTabletPipe::TEvClientConnected>(const TEvTabletPipe::TEvClientConnected* ev) {
    return ev->TabletId;
}

// Base class for PQ requests. It requests EvGetNode and creates worker actors for concrete topics.
// Than it starts merge over children responses.
// To use actor you need to:
// 1. Inherit from it.
// 2. Implement CreateTopicSubactor() and, optionally, MergeSubactorReplies() methods.
class TPersQueueBaseRequestProcessor : public TActorBootstrapped<TPersQueueBaseRequestProcessor> {
protected:
    using TSchemeEntry = NSchemeCache::TSchemeCacheNavigate::TEntry;
    using TPQGroupInfoPtr = TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>;
    using ESchemeStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

    struct TPerTopicInfo {
        TPerTopicInfo()
        { }
        explicit TPerTopicInfo(const TSchemeEntry& topicEntry, NPersQueue::TTopicConverterPtr& topicConverter)
            : TopicEntry(topicEntry)
            , Converter(topicConverter)
        {
        }

        TActorId ActorId;
        TSchemeEntry TopicEntry;
        NPersQueue::TTopicConverterPtr Converter;
        NKikimrClient::TResponse Response;
        bool ActorAnswered = false;
    };

public:
    class TNodesInfo {
    public:
        THolder<TEvInterconnect::TEvNodesInfo> NodesInfoReply;
        THashMap<ui32, TString> HostNames;
        THashMap<TString, ui32> MinNodeIdByHost;
        std::shared_ptr<THashMap<ui32, ui32>> DynToStaticNode;

        bool Ready = false;
        void ProcessNodesMapping(NPqMetaCacheV2::TEvPqNewMetaCache::TEvGetNodesMappingResponse::TPtr& ev,
                                 const TActorContext& ctx);
        explicit TNodesInfo(THolder<TEvInterconnect::TEvNodesInfo> nodesInfoReply, const TActorContext& ctx);
    private:
        void FinalizeWhenReady(const TActorContext& ctx);
        void Finalize(const TActorContext& ctx);
    };

public:
    static const TDuration TIMEOUT;

    TInstant StartTimestamp = TInstant::Zero();
    bool NeedChildrenCreation = false;

    ui32 ChildrenCreated = 0;
    bool ChildrenCreationDone = false;

    std::deque<THolder<TPerTopicInfo>> ChildrenToCreate;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_BASE_REQUEST_PROCESSOR;
    }

protected:
    TPersQueueBaseRequestProcessor(const NKikimrClient::TPersQueueRequest& request, const TActorId& pqMetaCacheId, bool listNodes);

    ~TPersQueueBaseRequestProcessor();

public:
    void Bootstrap(const TActorContext& ctx);

protected:
    bool CreateChildrenIfNeeded(const TActorContext& ctx);

    virtual THolder<IActor> CreateTopicSubactor(const TSchemeEntry& topicEntry, const TString& name) = 0; // Creates actor for processing one concrete topic.
    virtual NKikimrClient::TResponse MergeSubactorReplies();

    virtual void SendReplyAndDie(NKikimrClient::TResponse&& record, const TActorContext& ctx) = 0;
    void SendErrorReplyAndDie(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason);

    bool ReadyToCreateChildren() const;

    // true returned from this function means that we called Die().
    [[nodiscard]] bool CreateChildren(const TActorContext& ctx);

    virtual bool ReadyForAnswer(const TActorContext& ctx);
    void AnswerAndDie(const TActorContext& ctx);
    void GetTopicsListOrThrow(const ::google::protobuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& requests, THashMap<TString, std::shared_ptr<THashSet<ui64>>>& partitionsToRequest);

    virtual STFUNC(StateFunc);

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx);
    void Handle(NPqMetaCacheV2::TEvPqNewMetaCache::TEvGetNodesMappingResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeAllTopicsResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void HandleTimeout(const TActorContext& ctx);

    void Die(const TActorContext& ctx) override;

protected:
    // Request
    std::shared_ptr<const NKikimrClient::TPersQueueRequest> RequestProto;
    const TString RequestId;
    THashSet<TString> TopicsToRequest; // Topics that we need to request. If this set id empty, we are interested in all existing topics.

    const TActorId PqMetaCache;
    THashMap<TActorId, THolder<TPerTopicInfo>> Children;
    size_t ChildrenAnswered = 0;
    std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> TopicsDescription;
    TVector<NPersQueue::TTopicConverterPtr> TopicsConverters;

    // Nodes info
    const bool ListNodes;
    std::shared_ptr<TNodesInfo> NodesInfo;
    ui64 NodesPingsPending = 0;
};

// Helper actor that sends TEvGetBalancerDescribe and checks ACL (ACL is not implemented yet).
class TTopicInfoBasedActor : public TActorBootstrapped<TTopicInfoBasedActor> {
protected:
    using TSchemeEntry = NSchemeCache::TSchemeCacheNavigate::TEntry;
    using TPQGroupInfoPtr = TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>;
    using ESchemeStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

    TTopicInfoBasedActor(const TSchemeEntry& topicEntry, const TString& topicName);

    virtual void BootstrapImpl(const TActorContext& ctx) = 0;
    virtual void Answer(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) = 0;

    virtual void SendReplyAndDie(NKikimrClient::TResponse&& record, const TActorContext& ctx) = 0;

    STFUNC(StateFunc);

    template<typename T>
    void Become(T stateFunc) {
        IActorCallback::Become(stateFunc);
    }

protected:
    TActorId SchemeCache;
    TSchemeEntry SchemeEntry;
    TString Name;
    TProcessingResult ProcessingResult;
public:
    void Bootstrap(const TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_BASE_REQUEST_PROCESSOR;
    }
};

template <class TBase>
class TReplierToParent : public TBase {
public:
    template <class... T>
    explicit TReplierToParent(const TActorId& parent, T&&... t)
        : TBase(std::forward<T>(t)...)
        , Parent(parent)
    {
    }

protected:
    void SendReplyAndDie(NKikimrClient::TResponse&& record, const TActorContext& ctx) override {
        THolder<TEvPersQueue::TEvResponse> result(new TEvPersQueue::TEvResponse());
        result->Record.Swap(&record);

        ctx.Send(Parent, result.Release());

        Die(ctx);
    }
    void Die(const TActorContext& ctx) override {
        TBase::Die(ctx);
    }

    void SendErrorReplyAndDie(const TActorContext& ctx, EResponseStatus status, NPersQueue::NErrorCode::EErrorCode code, const TString& errorReason) {
        SendReplyAndDie(CreateErrorReply(status, code, errorReason), ctx);
    }

protected:
    const TActorId Parent;
};

// Pipe client helpers
template <class TBase, class TPipeEvent>
class TPipesWaiterActor : public TBase {
protected:
    template <class... T>
    explicit TPipesWaiterActor(T&&... t)
        : TBase(std::forward<T>(t)...)
    {
    }

    TActorId CreatePipe(ui64 tabletId, const TActorContext& ctx) {
        NTabletPipe::TClientConfig clientConfig;
        const TActorId pipe = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
        Y_ABORT_UNLESS(Pipes.emplace(tabletId, pipe).second);

        return pipe;
    }

    bool HasTabletPipe(ui64 tabletId) const {
        return IsIn(Pipes, tabletId);
    }

    template <class TEventPtr>
    TActorId CreatePipeAndSend(ui64 tabletId, const TActorContext& ctx, TEventPtr ev) {
        const TActorId pipe = CreatePipe(tabletId, ctx);
        NTabletPipe::SendData(ctx, pipe, ev.Release());
        return pipe;
    }

    // Wait in case TPipeEvent is not TEvTabletPipe::TEvClientConnected.
    // true returned from this function means that we called Die().
    [[nodiscard]] bool WaitAllPipeEvents(const TActorContext& ctx) {
        static_assert(TPipeEvent::EventType != TEvTabletPipe::TEvClientConnected::EventType, "Use WaitAllConnections()");

        if (EventsAreReady()) {
            if (OnPipeEventsAreReady(ctx)) {
                return true;
            }
        } else {
            TBase::Become(&TPipesWaiterActor::WaitAllPipeEventsStateFunc);
        }
        return false;
    }

    // Wait in case TPipeEvent is TEvTabletPipe::TEvClientConnected.
    // true returned from this function means that we called Die().
    [[nodiscard]] bool WaitAllConnections(const TActorContext& ctx) {
        static_assert(TPipeEvent::EventType == TEvTabletPipe::TEvClientConnected::EventType, "Use WaitAllPipeEvents()");

        if (EventsAreReady()) {
            if (OnPipeEventsAreReady(ctx)) {
                return true;
            }
        } else {
            TBase::Become(&TPipesWaiterActor::WaitAllConnectionsStateFunc);
        }
        return false;
    }

    // true returned from this function means that we called Die().
    [[nodiscard]] virtual bool OnPipeEvent(ui64 tabletId, typename TPipeEvent::TPtr& ev, const TActorContext& /*ctx*/) {
        Y_ABORT_UNLESS(!IsIn(PipeAnswers, tabletId) || !PipeAnswers.find(tabletId)->second);
        PipeAnswers[tabletId] = ev;
        return false;
    }

    // true returned from this function means that we called Die().
    [[nodiscard]] virtual bool OnClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& /*ev*/, const TActorContext& /*ctx*/) {
        return false;
    }

    // true returned from this function means that we called Die().
    [[nodiscard]] virtual bool OnPipeEventsAreReady(const TActorContext& ctx) = 0;

    void Die(const TActorContext& ctx) override {
        for (const auto& pipe : Pipes) {
            NTabletPipe::CloseClient(ctx, pipe.second);
        }
        TBase::Die(ctx);
    }

    STFUNC(WaitAllPipeEventsStateFunc) {
        static_assert(TPipeEvent::EventType != TEvTabletPipe::TEvClientConnected::EventType, "Use WaitAllConnectionsStateFunc");
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TPipeEvent, HandlePipeEvent);
            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
        default:
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::PERSQUEUE, "Unexpected event type: " << ev->GetTypeRewrite() << ", " << ev->ToString());
        }
    }

    STFUNC(WaitAllConnectionsStateFunc) {
        static_assert(TPipeEvent::EventType == TEvTabletPipe::TEvClientConnected::EventType, "Use WaitAllPipeEventsStateFunc");
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, HandlePipeEvent);
            CFunc(NActors::TEvents::TSystem::PoisonPill, Die);
        default:
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::PERSQUEUE, "Unexpected event type: " << ev->GetTypeRewrite() << ", " << ev->ToString());
        }
    }

    void HandlePipeEvent(typename TPipeEvent::TPtr& ev, const TActorContext& ctx) {
        const ui64 tabletId = GetTabletId(ev->Get());
        Y_ABORT_UNLESS(tabletId != 0);
        if (PipeAnswers.find(tabletId) != PipeAnswers.end())
            return;

        if (OnPipeEvent(tabletId, ev, ctx)) {
            return;
        }
        if (EventsAreReady()) {
            if (OnPipeEventsAreReady(ctx)) {
                return;
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected* msg = ev->Get();
        const ui64 tabletId = GetTabletId(msg);
        Y_ABORT_UNLESS(tabletId != 0);

        if (msg->Status != NKikimrProto::OK) {
            // Create record for answer
            PipeAnswers[tabletId];
            if (EventsAreReady()) {
                if (OnPipeEventsAreReady(ctx)) {
                    return;
                }
            }
        } else {
            if (OnClientConnected(ev, ctx)) {
                return;
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        // Create record for answer
        const ui64 tabletId = ev->Get()->TabletId;
        Y_ABORT_UNLESS(tabletId != 0);

        PipeAnswers[tabletId];
        if (EventsAreReady()) {
            if (OnPipeEventsAreReady(ctx)) {
                return;
            }
        }
    }

    bool EventsAreReady() const {
        return Pipes.size() == PipeAnswers.size();
    }

protected:
    THashMap<ui64, TActorId> Pipes; // Tablet id -> pipe
    THashMap<ui64, typename TPipeEvent::TPtr> PipeAnswers; // Mapped by tablet id
};

} // namespace NMsgBusProxy
} // namespace NKikimr
