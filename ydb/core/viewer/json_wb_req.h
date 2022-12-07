#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/viewer/json/json.h>
#include <library/cpp/actors/interconnect/interconnect.h>
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

template <typename RequestType, typename ResponseType>
class TJsonWhiteboardRequest : public TViewerPipeClient<TJsonWhiteboardRequest<RequestType, ResponseType>> {
protected:
    using TThis = TJsonWhiteboardRequest<RequestType, ResponseType>;
    using TBase = TViewerPipeClient<TThis>;
    IViewer* Viewer;
    TActorId Initiator;
    NMon::TEvHttpInfo::TPtr Event;
    std::vector<TNodeId> FilterNodeIds;
    ui64 ChangedSince;
    bool AliveOnly = false;
    std::unordered_set<TNodeId> NodeIds;
    TMap<TNodeId, THolder<ResponseType>> PerNodeStateInfo; // map instead of unordered_map only for sorting reason
    std::unordered_map<TNodeId, TString> NodeErrors;
    TInstant NodesRequestedTime;
    TString GroupFields;
    TString FilterFields;
    TString MergeFields;
    TJsonSettings JsonSettings;
    bool AllEnums = false;
    ui32 Timeout = 0;
    ui32 Retries = 0;
    std::unordered_map<TNodeId, ui32> NodeRetries;
    bool StaticNodesOnly = TWhiteboardInfo<ResponseType>::StaticNodesOnly;
    TDuration RetryPeriod = TDuration::MilliSeconds(500);
    TString LogPrefix;
    TString Format;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TString GetLogPrefix() {
        return LogPrefix;
    }

    TJsonWhiteboardRequest(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Initiator(ev->Sender)
        , Event(ev)
    {}

    THolder<RequestType> BuildRequest(TNodeId nodeId) {
        Y_UNUSED(nodeId);
        THolder<RequestType> request = MakeHolder<RequestType>();
        constexpr bool hasFormat = requires(const RequestType* r) {r->Record.GetFormat();};
        if constexpr (hasFormat) {
            request->Record.SetFormat(Format);
        }
        if (ChangedSince != 0) {
            request->Record.SetChangedSince(ChangedSince);
        }
        return request;
    }

    void SendNodeRequest(TNodeId nodeId) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        THolder<RequestType> request = BuildRequest(nodeId);
        TBase::SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        NodeIds.insert(nodeId);
    }

    void SendNodeRequest(const std::vector<TNodeId> nodeIds) {
        for (TNodeId nodeId : nodeIds) {
            SendNodeRequest(nodeId);
        }
    }

    void PassAway() override {
        for (const TNodeId nodeId : NodeIds) {
            TBase::Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

    virtual void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        TBase::InitConfig(params);
        SplitIds(params.Get("node_id"), ',', FilterNodeIds);
        std::replace(FilterNodeIds.begin(), FilterNodeIds.end(), (ui32)0, TlsActivationContext->ActorSystem()->NodeId);
        {
            TString merge = params.Get("merge");
            if (merge.empty() || merge == "1" || merge == "true") {
                MergeFields = TWhiteboardInfo<ResponseType>::GetDefaultMergeField();
            } else if (merge == "0" || merge == "false") {
                MergeFields.clear();
            } else {
                MergeFields = merge;
            }
        }
        ChangedSince = FromStringWithDefault<ui64>(params.Get("since"), 0);
        AliveOnly = FromStringWithDefault<bool>(params.Get("alive"), AliveOnly);
        GroupFields = params.Get("group");
        FilterFields = params.Get("filter");
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        JsonSettings.EmptyRepeated = FromStringWithDefault<bool>(params.Get("empty_repeated"), false);
        AllEnums = FromStringWithDefault<bool>(params.Get("all"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Retries = FromStringWithDefault<ui32>(params.Get("retries"), 0);
        RetryPeriod = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("retry_period"), RetryPeriod.MilliSeconds()));
        StaticNodesOnly = FromStringWithDefault<bool>(params.Get("static"), StaticNodesOnly);
        Format = params.Get("format");
        if (FilterNodeIds.empty()) {
            if (AliveOnly) {
                static const TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(TBase::SelfId().NodeId());
                TBase::SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvNodeStateRequest());
            } else {
                const TActorId nameserviceId = GetNameserviceActorId();
                TBase::Send(nameserviceId, new TEvInterconnect::TEvListNodes());
            }
            TBase::Become(&TThis::StateRequestedBrowse);
        } else {
            NodesRequestedTime = AppData()->TimeProvider->Now();
            SendNodeRequest(FilterNodeIds);
            TBase::Become(&TThis::StateRequestedNodeInfo);
        }
        TBase::Schedule(TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
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
            hFunc(ResponseType, HandleNodeInfo);
            hFunc(TEvPrivate::TEvRetryNodeRequest, HandleRetryNode);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        ui32 maxAllowedNodeId = std::numeric_limits<ui32>::max();
        if (StaticNodesOnly) {
            TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
            if (dynamicNameserviceConfig) {
                maxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
            }
        }
        NodesRequestedTime = AppData()->TimeProvider->Now();
        const TEvInterconnect::TEvNodesInfo* nodesInfo = ev->Get();
        for (const auto& ni : nodesInfo->Nodes) {
            if (ni.NodeId <= maxAllowedNodeId) {
                SendNodeRequest(ni.NodeId);
            }
        }
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
        if (StaticNodesOnly) {
            TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
            if (dynamicNameserviceConfig) {
                maxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
            }
        }
        NodesRequestedTime = AppData()->TimeProvider->Now();
        const TEvWhiteboard::TEvNodeStateResponse* nodesInfo = ev->Get();
        for (const auto& ni : nodesInfo->Record.GetNodeStateInfo()) {
            if (ni.GetConnected()) {
                TNodeId nodeId = GetNodeIdFromPeerName(ni.GetPeerName());
                if (nodeId != 0 && nodeId <= maxAllowedNodeId) {
                    SendNodeRequest(nodeId);
                }
            }
        }
        SendNodeRequest(TBase::SelfId().NodeId());
        if (TBase::Requests > 0) {
            TBase::Become(&TThis::StateRequestedNodeInfo);
        } else {
            ReplyAndPassAway();
        }
    }

    bool RetryRequest(TNodeId nodeId) {
        if (Retries) {
            if (++NodeRetries[nodeId] <= Retries) {
                TBase::Schedule(RetryPeriod, new TEvPrivate::TEvRetryNodeRequest(nodeId));
                return true;
            }
        }
        return false;
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        static const TString error = "Undelivered";
        TNodeId nodeId = ev.Get()->Cookie;
        if (PerNodeStateInfo.emplace(nodeId, nullptr).second) {
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
        if (PerNodeStateInfo.emplace(nodeId, nullptr).second) {
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
        BLOG_TRACE("Received " << typeid(ResponseType).name() << " from " << nodeId << GetResponseDuration(record) << GetProcessDuration(record));
    }

    void HandleNodeInfo(typename ResponseType::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        OnRecordReceived(ev->Get()->Record, nodeId);
        PerNodeStateInfo[nodeId] = ev->Release();
        NodeErrors.erase(nodeId);
        TBase::RequestDone();
    }

    void HandleRetryNode(TEvPrivate::TEvRetryNodeRequest::TPtr& ev) {
        SendNodeRequest(ev->Get()->NodeId);
        TBase::RequestDone(); // previous, failed one
    }

    void HandleTimeout() {
        //ctx.Send(Initiator, new NMon::TEvHttpInfoRes(GetHTTPGATEWAYTIMEOUT(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        //Die(ctx);
        ReplyAndPassAway();
    }

    virtual void FilterResponse(THolder<ResponseType>& response) {
        if (response != nullptr) {
            if (!FilterFields.empty()) {
                response = FilterWhiteboardResponses(response, FilterFields);
            }
        }
        if (response != nullptr) {
            if (!GroupFields.empty()) {
                response = GroupWhiteboardResponses(response, GroupFields, AllEnums);
            }
        }
    }

    void RenderResponse(TStringStream& json, const THolder<ResponseType>& response) {
        if (response != nullptr) {
            TProtoToJson::ProtoToJson(json, response->Record, JsonSettings);
        } else {
            json << "null";
        }
    }

    void ReplyAndPassAway() {
        try {
            TStringStream json;
            if (!MergeFields.empty()) {
                ui32 errors = 0;
                TString error;
                if (!FilterNodeIds.empty()) {
                    for (TNodeId nodeId : FilterNodeIds) {
                        auto it = NodeErrors.find(nodeId);
                        if (it != NodeErrors.end()) {
                            if (error.empty()) {
                                error = it->second;
                            }
                            errors++;
                        }
                    }
                }
                if (errors > 0 && errors == FilterNodeIds.size()) {
                    json << "{\"Error\":\"" << TProtoToJson::EscapeJsonString(error) << "\"}";
                } else {
                    THolder<ResponseType> response = MergeWhiteboardResponses(PerNodeStateInfo, MergeFields); // PerNodeStateInfo will be invalidated
                    FilterResponse(response);
                    RenderResponse(json, response);
                }
            } else {
                json << '{';
                for (auto it = PerNodeStateInfo.begin(); it != PerNodeStateInfo.end(); ++it) {
                    if (it != PerNodeStateInfo.begin()) {
                        json << ',';
                    }
                    json << '"' << it->first << "\":";
                    THolder<ResponseType>& response = it->second;
                    if (response) {
                        FilterResponse(response);
                        RenderResponse(json, response);
                    } else {
                        auto itNodeError = NodeErrors.find(it->first);
                        if (itNodeError != NodeErrors.end()) {
                            json << "{\"Error\":\"" << TProtoToJson::EscapeJsonString(itNodeError->second) << "\"}";
                        } else {
                            json << "null";
                        }
                    }
                }
                json << '}';
            }
            TBase::Send(Initiator, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        } catch (const std::exception& e) {
            TBase::Send(Initiator, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 400 Bad Request\r\n\r\n") + e.what(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        }
        PassAway();
    }
};

template <typename RequestType, typename ResponseType>
struct TJsonRequestParameters<TJsonWhiteboardRequest<RequestType, ResponseType>> {
    static TString GetParameters() {
        return R"___([{"name":"node_id","in":"query","description":"node identifier","required":false,"type":"integer"},)___"
               R"___({"name":"merge","in":"query","description":"merge information from nodes","required":false,"type":"boolean"},)___"
               R"___({"name":"group","in":"query","description":"group information by field","required":false,"type":"string"},)___"
               R"___({"name":"all","in":"query","description":"return all possible key combinations (for enums only)","required":false,"type":"boolean"},)___"
               R"___({"name":"filter","in":"query","description":"filter information by field","required":false,"type":"string"},)___"
               R"___({"name":"alive","in":"query","description":"request from alive (connected) nodes only","required":false,"type":"boolean"},)___"
               R"___({"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},)___"
               R"___({"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},)___"
               R"___({"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"},)___"
               R"___({"name":"retries","in":"query","description":"number of retries","required":false,"type":"integer"},)___"
               R"___({"name":"retry_period","in":"query","description":"retry period in ms","required":false,"type":"integer","default":500},)___"
               R"___({"name":"static","in":"query","description":"request from static nodes only","required":false,"type":"boolean"},)___"
               R"___({"name":"since","in":"query","description":"filter by update time","required":false,"type":"string"}])___";
    }
};

template <typename RequestType, typename ResponseType>
struct TJsonRequestSchema<TJsonWhiteboardRequest<RequestType, ResponseType>> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<typename ResponseType::ProtoRecordType>(stream);
        return stream.Str();
    }
};

}
}
