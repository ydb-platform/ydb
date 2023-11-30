#pragma once
#include "defs.h"
#include "log.h"
#include "events.h"
#include <ydb/core/ymq/base/query_id.h>

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/ymq/actor/actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/queue.h>
#include <util/generic/vector.h>

namespace NKikimr::NSQS {

struct TReplierToSenderActorCallback : public IReplyCallback {
    TReplierToSenderActorCallback(TSqsEvents::TEvSqsRequest::TPtr& ev)
        : Request(ev)
    {
    }

    void DoSendReply(const NKikimrClient::TSqsResponse& resp) override {
        NKikimrClient::TSqsResponse response = resp;
        response.SetRequestId(Request->Get()->Record.GetRequestId());

        RLOG_SQS_REQ_TRACE(Request->Get()->Record.GetRequestId(), "Sending sqs response: " << response);
        const TActorId selfId = TActivationContext::AsActorContext().SelfID;
        TActivationContext::Send(
            new IEventHandle(
                Request->Sender,
                selfId,
                new TSqsEvents::TEvSqsResponse(std::move(response))));
    }

    TSqsEvents::TEvSqsRequest::TPtr Request;
};

class TSqsProxyService
    : public TActorBootstrapped<TSqsProxyService>
{
private:
    struct TReloadStateRequestsInfo : public TAtomicRefCount<TReloadStateRequestsInfo> {
        TString User;
        TString Queue;

        TInstant RequestSendedAt;
        TInstant ReloadStateBorder;
        bool PlannedToSend = false;
        bool LeaderNodeRequested = false;
    };

    using TReloadStateRequestsInfoPtr = TIntrusivePtr<TReloadStateRequestsInfo>;

public:
    struct TNodeInfo;
    using TNodeInfoRef = TIntrusivePtr<TNodeInfo>;

    struct TProxyRequestInfo;
    using TProxyRequestInfoRef = TIntrusivePtr<TProxyRequestInfo>;

public:
    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_PROXY_SERVICE_ACTOR;
    }

private:
    void SendProxyRequestToNode(TNodeInfo& nodeInfo, TProxyRequestInfoRef request);

    TNodeInfoRef GetNodeInfo(ui32 nodeId);

    void SendProxyError(TProxyRequestInfoRef request, TSqsEvents::TEvProxySqsResponse::EProxyStatus proxyStatus);
    void SendProxyErrors(TNodeInfo& nodeInfo, TSqsEvents::TEvProxySqsResponse::EProxyStatus proxyStatus);


    TReloadStateRequestsInfoPtr GetReloadStateRequestsInfo(const TString& user, const TString& queue);
    void RemoveReloadStateRequestsInfo(const TString& user, const TString& queue);
    void ScheduleReloadStateRequest(TReloadStateRequestsInfoPtr info);
    void SendReloadStateIfNeeded(TSqsEvents::TEvGetLeaderNodeForQueueResponse::TPtr& ev);
    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
    void RequestLeaderNode(TReloadStateRequestsInfoPtr info);
    void RequestLeaderNode(const TString& reqId, const TString& user, const TString& queue);

private:
    STATEFN(StateFunc);
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandleSqsRequest(TSqsEvents::TEvSqsRequest::TPtr& ev); // request from proxy
    void HandleProxySqsRequest(TSqsEvents::TEvProxySqsRequest::TPtr& ev); // request for proxying
    void HandleSqsResponse(TSqsEvents::TEvSqsResponse::TPtr& ev); // response for proxying
    void HandleDisconnect(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleConnect(TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev);
    void HandleDisconnect(ui32 nodeId);
    void HandleGetLeaderNodeForQueueResponse(TSqsEvents::TEvGetLeaderNodeForQueueResponse::TPtr& ev);
    void HandleReloadStateRequest(TSqsEvents::TEvReloadStateRequest::TPtr& ev);
    void HandleReloadStateResponse(TSqsEvents::TEvReloadStateResponse::TPtr& ev);

private:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> SqsCounters_;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> YmqPublicCounters_;

    /// A map of node ids to TNodeIfno
    THashMap<ui32, TNodeInfoRef> NodesInfo_;

    THashMap<TString, TProxyRequestInfoRef> RequestsToProxy_;

    THashMap<TString, THashMap<TString, TReloadStateRequestsInfoPtr>> ReloadStateRequestsInfo_;
    TDeque<std::pair<TInstant, TReloadStateRequestsInfoPtr>> ReloadStatePlanningToSend_;
    TMap<TInstant, THashSet<TReloadStateRequestsInfoPtr>> ReloadStateRequestSended_;
    TString ReloadStateRequestId_;
    
};

} // namespace NKikimr::NSQS
