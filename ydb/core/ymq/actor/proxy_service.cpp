#include "actor.h"
#include "executor.h"
#include "log.h"
#include "service.h"
#include "queue_leader.h"
#include "params.h"
#include "proxy_service.h"
#include "serviceid.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/ymq/actor/actor.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/core/ymq/base/secure_protobuf_printer.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/queue.h>

#include <queue>

using namespace NKikimrTxUserProxy;
using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

constexpr TDuration RESEND_RELOAD_REQUEST_PERIOD = TDuration::Seconds(5);

struct TSqsProxyService::TNodeInfo : public TAtomicRefCount<TNodeInfo> {
    explicit TNodeInfo(ui32 nodeId)
        : NodeId(nodeId)
    {
    }

    ui32 NodeId = 0;
    THashMap<TString, TProxyRequestInfoRef> Requests; // request id -> request // sent proxy requests
};

struct TSqsProxyService::TProxyRequestInfo : public TAtomicRefCount<TProxyRequestInfo> {
    explicit TProxyRequestInfo(TSqsEvents::TEvProxySqsRequest::TPtr&& ev)
        : RequestId(ev->Get()->Record.GetRequestId())
        , ProxyActorId(ev->Sender)
        , ProxyRequest(std::move(ev))
    {
    }

    TString  RequestId;
    TActorId ProxyActorId;
    TSqsEvents::TEvProxySqsRequest::TPtr ProxyRequest;
};

void TSqsProxyService::Bootstrap() {
    LOG_SQS_INFO("Start SQS proxy service actor");
    Become(&TThis::StateFunc);

    SqsCounters_ = GetSqsServiceCounters(AppData()->Counters, "core");
    YmqPublicCounters_ = GetYmqPublicCounters(AppData()->Counters);

    ReloadStateRequestId_ = CreateGuidAsString();
    Send(SelfId(), new TEvents::TEvWakeup());
}

void TSqsProxyService::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    ev->Get()->Call();
}

void TSqsProxyService::HandleSqsRequest(TSqsEvents::TEvSqsRequest::TPtr& ev) {
    auto replier = MakeHolder<TReplierToSenderActorCallback>(ev);
    const auto& request = replier->Request->Get()->Record;
    RLOG_SQS_REQ_DEBUG(request.GetRequestId(), "Received Sqs Request: " << SecureShortUtf8DebugString(request));
    Register(CreateActionActor(request, std::move(replier)));
}

void TSqsProxyService::HandleProxySqsRequest(TSqsEvents::TEvProxySqsRequest::TPtr& ev) {
    TProxyRequestInfoRef request = new TProxyRequestInfo(std::move(ev));
    RequestsToProxy_.emplace(request->RequestId, request);
    RequestLeaderNode(request->RequestId, request->ProxyRequest->Get()->UserName, request->ProxyRequest->Get()->QueueName);
}

static TSqsEvents::TEvProxySqsResponse::EProxyStatus GetLeaderNodeForQueueStatusToProxyStatus(TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus status) {
    switch (status) {
    case TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::OK:
        return TSqsEvents::TEvProxySqsResponse::EProxyStatus::OK;
    case TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::NoUser:
        return TSqsEvents::TEvProxySqsResponse::EProxyStatus::UserDoesNotExist;
    case TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::NoQueue:
        return TSqsEvents::TEvProxySqsResponse::EProxyStatus::QueueDoesNotExist;
    case TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::FailedToConnectToLeader:
        return TSqsEvents::TEvProxySqsResponse::EProxyStatus::SessionError;
    case TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::Throttled:
        return TSqsEvents::TEvProxySqsResponse::EProxyStatus::Throttled;
    case TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::Error:
    default:
        return TSqsEvents::TEvProxySqsResponse::EProxyStatus::LeaderResolvingError;
    }
}

TSqsProxyService::TReloadStateRequestsInfoPtr TSqsProxyService::GetReloadStateRequestsInfo(const TString& user, const TString& queue) {
    auto userIt = ReloadStateRequestsInfo_.find(user);
    if (userIt != ReloadStateRequestsInfo_.end()) {
        auto queueIt = userIt->second.find(queue);
        if (queueIt != userIt->second.end()) {
            return queueIt->second;
        }
    }
    return nullptr;
}

void TSqsProxyService::RemoveReloadStateRequestsInfo(const TString& user, const TString& queue) {
    auto userIt = ReloadStateRequestsInfo_.find(user);
    if (userIt != ReloadStateRequestsInfo_.end()) {
        userIt->second.erase(queue);
        if (userIt->second.empty()) {
            ReloadStateRequestsInfo_.erase(userIt);
        }
    }
}

void TSqsProxyService::ScheduleReloadStateRequest(TReloadStateRequestsInfoPtr info) {
    info->RequestSendedAt = TInstant::Zero();
    info->LeaderNodeRequested = false;
    info->PlannedToSend = true;
    ReloadStatePlanningToSend_.push_back(std::make_pair(TActivationContext::Now() + RESEND_RELOAD_REQUEST_PERIOD, info));
    
}

void TSqsProxyService::SendReloadStateIfNeeded(TSqsEvents::TEvGetLeaderNodeForQueueResponse::TPtr& ev) {
    auto info = GetReloadStateRequestsInfo(ev->Get()->UserName, ev->Get()->QueueName);
    if (!info) {
        return;
    }

    if (info->RequestSendedAt == TInstant::Zero() && !info->PlannedToSend) {
        if (ev->Get()->Status == TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::OK) {
            Send(MakeSqsServiceID(ev->Get()->NodeId), new TSqsEvents::TEvReloadStateRequest(info->User, info->Queue));

            info->RequestSendedAt = TActivationContext::Now();
            ReloadStateRequestSended_[info->RequestSendedAt].insert(info);
        } else if (
            ev->Get()->Status == TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::NoUser
            || ev->Get()->Status == TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::NoQueue
        ) {
            RemoveReloadStateRequestsInfo(ev->Get()->UserName, ev->Get()->QueueName);
        } else {
            ScheduleReloadStateRequest(info);
        }
    }
}

void TSqsProxyService::HandleWakeup(TEvents::TEvWakeup::TPtr& /*ev*/, const TActorContext& /*ctx*/) {
    auto now = TActivationContext::Now();
    while (!ReloadStatePlanningToSend_.empty() && ReloadStatePlanningToSend_.front().first <= now) {
        auto info = ReloadStatePlanningToSend_.front().second;
        ReloadStatePlanningToSend_.pop_front();
        
        RequestLeaderNode(info);
    }
    auto timeoutBorder = now - RESEND_RELOAD_REQUEST_PERIOD;
    while (!ReloadStateRequestSended_.empty() && ReloadStateRequestSended_.begin()->first <= timeoutBorder) {
        for (auto info : ReloadStateRequestSended_.begin()->second) {
            RequestLeaderNode(info);
        }
        ReloadStateRequestSended_.erase(ReloadStateRequestSended_.begin());
    }
    
    Schedule(TDuration::MilliSeconds(100), new TEvWakeup());
}

void TSqsProxyService::HandleGetLeaderNodeForQueueResponse(TSqsEvents::TEvGetLeaderNodeForQueueResponse::TPtr& ev) {
    RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "Got leader node for queue response. Node id: " << ev->Get()->NodeId << ". Status: " << static_cast<int>(ev->Get()->Status));
    
    if (ev->Get()->RequestId == ReloadStateRequestId_) {
        SendReloadStateIfNeeded(ev);
        return;
    }
    
    const auto requestIt = RequestsToProxy_.find(ev->Get()->RequestId);
    if (requestIt == RequestsToProxy_.end()) {
        RLOG_SQS_REQ_ERROR(ev->Get()->RequestId, "Request was not found in requests to proxy map");
        return;
    }
    TProxyRequestInfoRef request = requestIt->second;
    RequestsToProxy_.erase(requestIt);

    if (ev->Get()->Status == TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::OK) {
        TNodeInfoRef nodeInfo = GetNodeInfo(ev->Get()->NodeId);
        SendProxyRequestToNode(*nodeInfo, request);
    } else {
        SendProxyError(request, GetLeaderNodeForQueueStatusToProxyStatus(ev->Get()->Status));
    }
}

void TSqsProxyService::HandleSqsResponse(TSqsEvents::TEvSqsResponse::TPtr& ev) {
    LOG_SQS_TRACE("HandleSqsResponse " << SecureShortUtf8DebugString(ev->Get()->Record));
    const ui32 nodeId = ev->Sender.NodeId();
    const auto nodeInfoIt = NodesInfo_.find(nodeId);
    if (nodeInfoIt == NodesInfo_.end()) {
        LOG_SQS_ERROR("Failed to find node id " << nodeId << " for response " << ev->Get()->Record);
        return;
    }
    const TString& requestId = ev->Get()->Record.GetRequestId();
    auto& requests = nodeInfoIt->second->Requests;
    const auto proxyRequestIt = requests.find(requestId);
    if (proxyRequestIt == requests.end()) {
        LOG_SQS_ERROR("Failed to find request " << requestId << " for node id " << nodeId << ". Response: " << ev->Get()->Record);
        return;
    }
    LOG_SQS_TRACE("Sending answer to proxy actor " << proxyRequestIt->second->ProxyActorId << ": " << SecureShortUtf8DebugString(ev->Get()->Record));
    Send(proxyRequestIt->second->ProxyActorId, new TSqsEvents::TEvProxySqsResponse(std::move(ev->Get()->Record)));
    requests.erase(proxyRequestIt);
}

void TSqsProxyService::HandleDisconnect(ui32 nodeId) {
    auto nodeIt = NodesInfo_.find(nodeId);
    if (nodeIt != NodesInfo_.end()) {
        SendProxyErrors(*nodeIt->second, TSqsEvents::TEvProxySqsResponse::EProxyStatus::SessionError);
    }
}

void TSqsProxyService::HandleDisconnect(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    const ui32 nodeId = ev->Get()->NodeId;
    LOG_SQS_TRACE("HandleDisconnect from node " << nodeId);
    HandleDisconnect(nodeId);
}

void TSqsProxyService::HandleConnect(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    LOG_SQS_TRACE("HandleConnect from node " << ev->Get()->NodeId);
}

void TSqsProxyService::HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
    const ui32 nodeId = ev->Sender.NodeId();
    LOG_SQS_TRACE("HandleUndelivered from node " << nodeId << ", reason: " << ev->Get()->Reason << ", unsure: " << ev->Get()->Unsure);
    HandleDisconnect(nodeId);
}

void TSqsProxyService::RequestLeaderNode(TReloadStateRequestsInfoPtr info) {
    info->PlannedToSend = false;
    info->LeaderNodeRequested = true;
    info->RequestSendedAt = TInstant::Zero();
    
    RequestLeaderNode(ReloadStateRequestId_, info->User, info->Queue);
}

void TSqsProxyService::RequestLeaderNode(const TString& reqId, const TString& user, const TString& queue) {
    Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvGetLeaderNodeForQueueRequest(reqId, user, queue));
    RLOG_SQS_REQ_DEBUG(reqId, "Send get leader node request to sqs service for " << user << "/" << queue);
}

void TSqsProxyService::HandleReloadStateRequest(TSqsEvents::TEvReloadStateRequest::TPtr& ev) {
    const auto& target = ev->Get()->Record.GetTarget();
    
    auto info = GetReloadStateRequestsInfo(target.GetUserName(), target.GetQueueName());
    if (!info) {
        info = new TReloadStateRequestsInfo();
        info->User = target.GetUserName();
        info->Queue = target.GetQueueName();
        ReloadStateRequestsInfo_[target.GetUserName()][target.GetQueueName()] = info;
    }

    info->ReloadStateBorder = TActivationContext::Now();
    if (info->RequestSendedAt == TInstant::Zero() && !info->LeaderNodeRequested) {
        RequestLeaderNode(ReloadStateRequestId_, target.GetUserName(), target.GetQueueName());
    }
}

void TSqsProxyService::HandleReloadStateResponse(TSqsEvents::TEvReloadStateResponse::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto& who = record.GetWho();
    auto info = GetReloadStateRequestsInfo(who.GetUserName(), who.GetQueueName());
    
    if (!info) {
        return;
    }
    ReloadStateRequestSended_[info->RequestSendedAt].erase(info);
    TInstant reloadedAt = TInstant::MilliSeconds(record.GetReloadedAtMs());
    if (reloadedAt < info->ReloadStateBorder) {
        ScheduleReloadStateRequest(info);
    } else {
        RemoveReloadStateRequestsInfo(who.GetUserName(), who.GetQueueName());
    }
}


STATEFN(TSqsProxyService::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TSqsEvents::TEvExecuted,              HandleExecuted);
        hFunc(TSqsEvents::TEvSqsRequest,            HandleSqsRequest); // request to queue leader node (proxied) // creates request worker and calls it
        hFunc(TSqsEvents::TEvProxySqsRequest,       HandleProxySqsRequest); // request from proxy on our node // proxies request to queue leader node (TEvSqsRequest)
        hFunc(TSqsEvents::TEvSqsResponse,           HandleSqsResponse); // response from other node on TEvSqsRequest // sends response to source proxy on our node
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnect);
        hFunc(TEvInterconnect::TEvNodeConnected,    HandleConnect);
        hFunc(TEvents::TEvUndelivered,              HandleUndelivered);
        hFunc(TSqsEvents::TEvGetLeaderNodeForQueueResponse, HandleGetLeaderNodeForQueueResponse);
        hFunc(TSqsEvents::TEvReloadStateRequest, HandleReloadStateRequest);
        hFunc(TSqsEvents::TEvReloadStateResponse, HandleReloadStateResponse);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
    default:
        LOG_SQS_ERROR("Unknown type of event came to SQS service actor: " << ev->Type << " (" << ev->GetTypeName() << "), sender: " << ev->Sender);
    }
}

void TSqsProxyService::SendProxyRequestToNode(TNodeInfo& nodeInfo, TProxyRequestInfoRef request) {
    RLOG_SQS_REQ_TRACE(request->RequestId, "Sending request from proxy to leader node " << nodeInfo.NodeId << ": " << SecureShortUtf8DebugString(request->ProxyRequest->Get()->Record));
    Send(MakeSqsProxyServiceID(nodeInfo.NodeId), new TSqsEvents::TEvSqsRequest(std::move(request->ProxyRequest->Get()->Record)),
             IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    nodeInfo.Requests.emplace(request->RequestId, std::move(request));
}

TSqsProxyService::TNodeInfoRef TSqsProxyService::GetNodeInfo(ui32 nodeId) {
    const auto nodeInfoIt = NodesInfo_.find(nodeId);
    if (nodeInfoIt != NodesInfo_.end()) {
        return nodeInfoIt->second;
    }

    // create new node info
    TNodeInfoRef nodeInfo = new TNodeInfo(nodeId);
    NodesInfo_[nodeId] = nodeInfo;
    return nodeInfo;
}

void TSqsProxyService::SendProxyError(TProxyRequestInfoRef request, TSqsEvents::TEvProxySqsResponse::EProxyStatus proxyStatus) {
    RLOG_SQS_REQ_TRACE(request->RequestId, "Sending proxy status " << proxyStatus << " to proxy actor");
    THolder<TSqsEvents::TEvProxySqsResponse> answer = MakeHolder<TSqsEvents::TEvProxySqsResponse>();
    answer->ProxyStatus = proxyStatus;
    Send(request->ProxyActorId, std::move(answer));
}

void TSqsProxyService::SendProxyErrors(TNodeInfo& nodeInfo, TSqsEvents::TEvProxySqsResponse::EProxyStatus proxyStatus) {
    for (auto& req : nodeInfo.Requests) {
        SendProxyError(std::move(req.second), proxyStatus);
    }
    nodeInfo.Requests.clear();
}

IActor* CreateSqsProxyService() {
    return new TSqsProxyService();
}

} // namespace NKikimr::NSQS
