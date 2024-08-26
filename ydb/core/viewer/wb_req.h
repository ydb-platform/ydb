#pragma once
#include "json_pipe_req.h"
#include "log.h"
#include "viewer.h"
#include "wb_filter.h"
#include "wb_group.h"
#include "wb_merge.h"
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NViewer {

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
    std::unordered_map<TNodeId, TRequestResponse<TResponseEventType>> NodeResponses;
    TInstant NodesRequestedTime;
    std::unordered_map<TNodeId, ui32> NodeRetries;
    TString LogPrefix;

public:
    TString GetLogPrefix() {
        return LogPrefix;
    }

    TWhiteboardRequest(NWilson::TTraceId traceId)
        : TBase(std::move(traceId))
    {}

    TWhiteboardRequest(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    virtual THolder<TRequestEventType> BuildRequest() {
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
        NodeResponses[nodeId] = MakeWhiteboardRequest(nodeId, CloneRequest().Release());
    }

    void SendNodeRequest(const std::vector<TNodeId> nodeIds) {
        for (TNodeId nodeId : nodeIds) {
            SendNodeRequestToWhiteboard(nodeId);
        }
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
        if (NodeResponses[nodeId].Error(error)) {
            if (!RetryRequest(nodeId)) {
                RequestDone();
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        static const TString error = "Node disconnected";
        TNodeId nodeId = ev->Get()->NodeId;
        if (NodeResponses[nodeId].Error(error)) {
            if (!RetryRequest(nodeId)) {
                RequestDone();
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

    void HandleNodeInfo(typename TResponseEventType::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        ev->Get()->Record.SetResponseDuration((AppData()->TimeProvider->Now() - NodesRequestedTime).MicroSeconds());
        NodeResponses[nodeId].Set(std::move(ev));
        //PerNodeStateInfo[nodeId] = std::move(ev->Get()->Record);
        TBase::RequestDone();
    }

    void HandleRetryNode(TEvPrivate::TEvRetryNodeRequest::TPtr& ev) {
        NodeResponses.erase(ev->Get()->NodeId);
        SendNodeRequest({ev->Get()->NodeId});
        TBase::RequestDone(); // previous, failed one
    }

    void HandleTimeout() {
        ReplyAndPassAway();
    }

    TMap<TNodeId, TResponseType> GetPerNodeStateInfo() { // map instead of unordered_map only for sorting reason
        TMap<TNodeId, TResponseType> perNodeStateInfo;
        for (const auto& [nodeId, response] : NodeResponses) {
            if (response.IsOk()) {
                perNodeStateInfo.emplace(nodeId, std::move(response->Record));
            } else {
                perNodeStateInfo[nodeId]; // empty data for failed requests
            }
        }
        return perNodeStateInfo;
    }
};

}
