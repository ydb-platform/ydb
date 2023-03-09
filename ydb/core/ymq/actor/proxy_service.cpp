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
    Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvGetLeaderNodeForQueueRequest(request->RequestId, request->ProxyRequest->Get()->UserName, request->ProxyRequest->Get()->QueueName));
    RLOG_SQS_REQ_DEBUG(request->RequestId, "Send get leader node request to sqs service");
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
    case TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::Error:
    default:
        return TSqsEvents::TEvProxySqsResponse::EProxyStatus::LeaderResolvingError;
    }
}

void TSqsProxyService::HandleGetLeaderNodeForQueueResponse(TSqsEvents::TEvGetLeaderNodeForQueueResponse::TPtr& ev) {
    RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "Got leader node for queue response. Node id: " << ev->Get()->NodeId << ". Status: " << static_cast<int>(ev->Get()->Status));
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
