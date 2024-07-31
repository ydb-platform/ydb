#pragma once

#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/time_provider/time_provider.h>
#include "viewer.h"
#include "json_pipe_req.h"
#include "wb_merge.h"
#include "wb_group.h"
#include "wb_filter.h"
#include "log.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

struct TEvPrivate {
    enum EEv {
        EvRetryNodeRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvRetryNodeRequest : NActors::TEventLocal<TEvRetryNodeRequest, EvRetryNodeRequest> {
        TNodeId NodeId;

        TEvRetryNodeRequest(TNodeId nodeId)
            : NodeId(nodeId)
        {}
    };
};

template<typename TRequestEventType, typename TResponseEventType>
class TWhiteboardRequest : public TViewerPipeClient {
protected:
    using TThis = TWhiteboardRequest<TRequestEventType, TResponseEventType>;
    using TBase = TViewerPipeClient;
    using TResponseType = typename TResponseEventType::ProtoRecordType;
    TRequestSettings RequestSettings;
    THolder<TRequestEventType> Request;
    std::unordered_set<TNodeId> NodeIds;
    TMap<TNodeId, TResponseType> PerNodeStateInfo; // map instead of unordered_map only for sorting reason
    std::unordered_map<TNodeId, TString> NodeErrors;
    TInstant NodesRequestedTime;
    std::unordered_map<TNodeId, ui32> NodeRetries;
    TString LogPrefix;

public:
    TString GetLogPrefix() {
        return LogPrefix;
    }

    TWhiteboardRequest() = default;

    THolder<TRequestEventType> BuildRequest() {
        THolder<TRequestEventType> request = MakeHolder<TRequestEventType>();
        constexpr bool hasFormat = requires(const TRequestEventType* r) {r->Record.GetFormat();};
        if constexpr (hasFormat) {
            if (!RequestSettings.Format.empty()) {
                request->Record.SetFormat(RequestSettings.Format);
            }
        }
        if (RequestSettings.ChangedSince != 0) {
            request->Record.SetChangedSince(RequestSettings.ChangedSince);
        }
        return request;
    }

    THolder<TRequestEventType> CloneRequest() {
        THolder<TRequestEventType> request = MakeHolder<TRequestEventType>();
        request->Record.MergeFrom(Request->Record);
        return request;
    }

    void SendNodeRequestToWhiteboard(TNodeId nodeId) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        THolder<TRequestEventType> request = CloneRequest();
        BLOG_TRACE("Sent WhiteboardRequest to " << nodeId << " Request: " << request->Record.ShortDebugString());
        TBase::SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        NodeIds.insert(nodeId);
    }

    void SendNodeRequest(const std::vector<TNodeId> nodeIds) {
        for (TNodeId nodeId : nodeIds) {
            SendNodeRequestToWhiteboard(nodeId);
        }
    }

    void PassAway() override {
        for (const TNodeId nodeId : NodeIds) {
            TBase::Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

    void Bootstrap() override {
        std::replace(RequestSettings.FilterNodeIds.begin(),
                     RequestSettings.FilterNodeIds.end(),
                     (ui32)0,
                     TlsActivationContext->ActorSystem()->NodeId);

        TBase::InitConfig(RequestSettings);
        Request = BuildRequest();
        if (RequestSettings.FilterNodeIds.empty()) {
            if (RequestSettings.AliveOnly) {
                static const TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(TBase::SelfId().NodeId());
                TBase::SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvNodeStateRequest());
            } else {
                const TActorId nameserviceId = GetNameserviceActorId();
                TBase::Send(nameserviceId, new TEvInterconnect::TEvListNodes());
            }
            TBase::Become(&TThis::StateRequestedBrowse);
        } else {
            NodesRequestedTime = AppData()->TimeProvider->Now();
            SendNodeRequest(RequestSettings.FilterNodeIds);
            TBase::Become(&TThis::StateRequestedNodeInfo);
        }
        TBase::Schedule(TDuration::MilliSeconds(RequestSettings.Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            hFunc(TEvPrivate::TEvRetryNodeRequest, HandleRetryNode);
            hFunc(TEvWhiteboard::TEvNodeStateResponse, HandleBrowse);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STATEFN(StateRequestedNodeInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TResponseEventType, HandleNodeInfo);
            hFunc(TEvPrivate::TEvRetryNodeRequest, HandleRetryNode);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        TNodeId maxAllowedNodeId = std::numeric_limits<TNodeId>::max();
        if (RequestSettings.StaticNodesOnly.value_or(TWhiteboardInfo<TResponseType>::StaticNodesOnly)) {
            TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
            if (dynamicNameserviceConfig) {
                maxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
            }
        }
        NodesRequestedTime = AppData()->TimeProvider->Now();
        const TEvInterconnect::TEvNodesInfo* nodesInfo = ev->Get();
        std::vector<TNodeId> nodeIds;
        for (const auto& ni : nodesInfo->Nodes) {
            if (ni.NodeId <= maxAllowedNodeId) {
                nodeIds.push_back(ni.NodeId);
            }
        }
        SendNodeRequest(nodeIds);
        if (TBase::Requests > 0) {
            TBase::Become(&TThis::StateRequestedNodeInfo);
        } else {
            ReplyAndPassAway();
        }
    }

    static TNodeId GetNodeIdFromPeerName(const TString& peerName) {
        TString::size_type colonPos = peerName.find(':');
        if (colonPos != TString::npos) {
            return FromStringWithDefault<TNodeId>(peerName.substr(0, colonPos), 0);
        }
        return 0;
    }

    void HandleBrowse(TEvWhiteboard::TEvNodeStateResponse::TPtr& ev) {
        TNodeId maxAllowedNodeId = std::numeric_limits<TNodeId>::max();
        if (RequestSettings.StaticNodesOnly.value_or(TWhiteboardInfo<TResponseType>::StaticNodesOnly)) {
            TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
            if (dynamicNameserviceConfig) {
                maxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
            }
        }
        NodesRequestedTime = AppData()->TimeProvider->Now();
        const TEvWhiteboard::TEvNodeStateResponse* nodesInfo = ev->Get();
        std::vector<TNodeId> nodeIds;
        for (const auto& ni : nodesInfo->Record.GetNodeStateInfo()) {
            if (ni.GetConnected()) {
                TNodeId nodeId = GetNodeIdFromPeerName(ni.GetPeerName());
                if (nodeId != 0 && nodeId <= maxAllowedNodeId) {
                    nodeIds.push_back(nodeId);
                }
            }
        }
        nodeIds.push_back(TBase::SelfId().NodeId());
        SendNodeRequest(nodeIds);
        if (TBase::Requests > 0) {
            TBase::Become(&TThis::StateRequestedNodeInfo);
        } else {
            ReplyAndPassAway();
        }
    }

    bool RetryRequest(TNodeId nodeId) {
        if (RequestSettings.Retries) {
            if (++NodeRetries[nodeId] <= RequestSettings.Retries) {
                TBase::Schedule(RequestSettings.RetryPeriod, new TEvPrivate::TEvRetryNodeRequest(nodeId));
                return true;
            }
        }
        return false;
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        static const TString error = "Undelivered";
        TNodeId nodeId = ev.Get()->Cookie;
        if (PerNodeStateInfo.emplace(nodeId, TResponseType{}).second) {
            NodeErrors[nodeId] = error;
            if (!RetryRequest(nodeId)) {
                TBase::RequestDone();
            }
        } else {
            if (NodeErrors[nodeId] == error) {
                if (!RetryRequest(nodeId)) {
                    TBase::RequestDone();
                }
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        static const TString error = "Node disconnected";
        TNodeId nodeId = ev->Get()->NodeId;
        if (PerNodeStateInfo.emplace(nodeId, TResponseType{}).second) {
            NodeErrors[nodeId] = error;
            if (!RetryRequest(nodeId)) {
                TBase::RequestDone();
            }
        } else {
            if (NodeErrors[nodeId] == error) {
                if (!RetryRequest(nodeId)) {
                    TBase::RequestDone();
                }
            }
        }
    }

    template<typename ResponseRecordType>
    TString GetResponseDuration(ResponseRecordType& record) {
        constexpr bool hasResponseDuration = requires(const ResponseRecordType& r) {r.GetResponseDuration();};
        if constexpr (hasResponseDuration) {
            return TStringBuilder() << " ResponseDuration: " << record.GetResponseDuration() << "us";
        } else {
            return {};
        }
    }

    template<typename ResponseRecordType>
    TString GetProcessDuration(ResponseRecordType& record) {
        constexpr bool hasProcessDuration = requires(const ResponseRecordType& r) {r.GetProcessDuration();};
        if constexpr (hasProcessDuration) {
            return TStringBuilder() << " ProcessDuration: " << record.GetProcessDuration() << "us";
        } else {
            return {};
        }
    }

    template<typename ResponseRecordType>
    void OnRecordReceived(ResponseRecordType& record, TNodeId nodeId) {
        record.SetResponseDuration((AppData()->TimeProvider->Now() - NodesRequestedTime).MicroSeconds());
        BLOG_TRACE("Received " << typeid(TResponseEventType).name() << " from " << nodeId << GetResponseDuration(record) << GetProcessDuration(record));
    }

    void HandleNodeInfo(typename TResponseEventType::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        OnRecordReceived(ev->Get()->Record, nodeId);
        PerNodeStateInfo[nodeId] = std::move(ev->Get()->Record);
        NodeErrors.erase(nodeId);
        TBase::RequestDone();
    }

    void HandleRetryNode(TEvPrivate::TEvRetryNodeRequest::TPtr& ev) {
        SendNodeRequest({ev->Get()->NodeId});
        TBase::RequestDone(); // previous, failed one
    }

    void HandleTimeout() {
        ReplyAndPassAway();
    }
};

}
}
