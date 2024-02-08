#pragma once

#include "msgbus_server_persqueue.h"

#include <ydb/services/persqueue_v1/actors/read_session_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>


namespace NKikimr {
namespace NMsgBusProxy {

class IPersQueueGetReadSessionsInfoWorker : public TActorBootstrapped<IPersQueueGetReadSessionsInfoWorker> {
public:
    IPersQueueGetReadSessionsInfoWorker(
        const TActorId& parentId,
        const THashMap<TString, TActorId>& readSessions,
        std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo
    );

    virtual ~IPersQueueGetReadSessionsInfoWorker() = default;

    void Bootstrap(const TActorContext& ctx);

    bool ReadyToAnswer() const;
    void Answer(const TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_BASE_REQUEST_PROCESSOR;
    }

protected:
    virtual STFUNC(StateFunc) = 0;

    template <class TEvReadSessionStatusResponseType>
        void HandleStatusResponse(typename TEvReadSessionStatusResponseType::TPtr& ev, const TActorContext& ctx) {    ProcessStatus(ev->Get()->Record, ctx);
    }; //TODO: move to other actor

    void ProcessStatus(const NKikimrPQ::TReadSessionStatusResponse& response, const TActorContext& ctx);
    virtual void SendStatusRequest(const TString& sessionName, TActorId id, const TActorContext& ctx) = 0;

    template <class TEvStatusRequestType>
    void SendStatusRequest(TActorId actorId, const TActorContext& ctx) const {
        ctx.Send(
            actorId,
            new TEvStatusRequestType(),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession
        );
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx); //TODO: move to other actor
    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx); //TODO: move to other actor

private:
    TString GetHostName(ui32 hostId) const;

    const TActorId ParentId;

    THashMap<TString, TActorId> ReadSessions; //TODO: move to other actor
    THashMap<TString, NKikimrPQ::TReadSessionStatusResponse> ReadSessionStatus; //TODO: move to other actor

    THashSet<TString> ReadSessionNames;

    std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> NodesInfo;
};

class TPersQueueGetReadSessionsInfoWorker : public IPersQueueGetReadSessionsInfoWorker {
public:
    using TBase = IPersQueueGetReadSessionsInfoWorker;
    using TBase::TBase;
    using TBase::SendStatusRequest;

    STFUNC(StateFunc) override {
        switch (ev->GetTypeRewrite()) {
            HFunc(NGRpcProxy::V1::TEvPQProxy::TEvReadSessionStatusResponse, HandleStatusResponse<NGRpcProxy::V1::TEvPQProxy::TEvReadSessionStatusResponse>);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
        }
    }
private:
    void SendStatusRequest(const TString& sessionName, TActorId actorId, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(sessionName.EndsWith("_v1"));
        SendStatusRequest<NGRpcProxy::V1::TEvPQProxy::TEvReadSessionStatus>(actorId, ctx);
    }
};

class IPersQueueGetReadSessionsInfoWorkerFactory {
public:
    virtual ~IPersQueueGetReadSessionsInfoWorkerFactory() = default;
    virtual THolder<IPersQueueGetReadSessionsInfoWorker> Create(
        const TActorId& parentId,
        const THashMap<TString, TActorId>& readSessions,
        std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo
    ) const = 0;
};

class TPersQueueGetReadSessionsInfoWorkerFactory : public IPersQueueGetReadSessionsInfoWorkerFactory {
public:
    THolder<IPersQueueGetReadSessionsInfoWorker> Create(
        const TActorId& parentId,
        const THashMap<TString, TActorId>& readSessions,
        std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo
    ) const override {
        return MakeHolder<TPersQueueGetReadSessionsInfoWorker>(parentId, readSessions, nodesInfo);
    }
};

} // namespace NMsgBusProxy
} // namespace NKikimr
