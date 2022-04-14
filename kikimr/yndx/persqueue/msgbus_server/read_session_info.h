#pragma once

#include <ydb/core/client/server/msgbus_server_pq_read_session_info.h>

#include <kikimr/yndx/grpc_services/persqueue/grpc_pq_actor.h>


namespace NKikimr {
namespace NMsgBusProxy {

class TPersQueueGetReadSessionsInfoWorkerWithPQv0 : public IPersQueueGetReadSessionsInfoWorker {
public:
    using TBase = IPersQueueGetReadSessionsInfoWorker;
    using TBase::TBase;
    using TBase::SendStatusRequest;

    STFUNC(StateFunc) override {
        switch (ev->GetTypeRewrite()) {
            HFunc(NGRpcProxy::TEvPQProxy::TEvReadSessionStatusResponse, HandleStatusResponse<NGRpcProxy::TEvPQProxy::TEvReadSessionStatusResponse>);
            HFunc(NGRpcProxy::V1::TEvPQProxy::TEvReadSessionStatusResponse, HandleStatusResponse<NGRpcProxy::V1::TEvPQProxy::TEvReadSessionStatusResponse>);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
        }
    }

private:
    void SendStatusRequest(const TString& sessionName, TActorId actorId, const TActorContext& ctx) override;
};

class TPersQueueGetReadSessionsInfoWorkerWithPQv0Factory : public IPersQueueGetReadSessionsInfoWorkerFactory {
public:
    THolder<IPersQueueGetReadSessionsInfoWorker> Create(
        const TActorId& parentId,
        const THashMap<TString, TActorId>& readSessions,
        std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo
    ) const override {
        return MakeHolder<TPersQueueGetReadSessionsInfoWorkerWithPQv0>(parentId, readSessions, nodesInfo);
    }
};

} // namespace NMsgBusProxy
} // namespace NKikimr

