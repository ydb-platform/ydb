#pragma once
#include <unordered_map>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include "viewer.h"
#include "json_pipe_req.h"
#include "json_sysinfo.h"
#include "json_pdiskinfo.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;
using ::google::protobuf::FieldDescriptor;

class TJsonNodes : public TViewerPipeClient<TJsonNodes> {
    using TThis = TJsonNodes;
    using TBase = TViewerPipeClient<TJsonNodes>;
    using TNodeId = ui32;
    using TPDiskId = std::pair<TNodeId, ui32>;
    IViewer* Viewer;
    TActorId Initiator;
    NMon::TEvHttpInfo::TPtr Event;
    std::unique_ptr<TEvInterconnect::TEvNodesInfo> NodesInfo;
    std::unordered_map<TNodeId, THolder<TEvWhiteboard::TEvPDiskStateResponse>> PDiskInfo;
    std::unordered_map<TNodeId, THolder<TEvWhiteboard::TEvSystemStateResponse>> SysInfo;
    std::unordered_map<TString, THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>> DescribeResult;
    std::unique_ptr<TEvBlobStorage::TEvControllerConfigResponse> BaseConfig;
    std::unordered_map<ui32, const NKikimrBlobStorage::TBaseConfig::TGroup*> BaseConfigGroupIndex;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString FilterTenant;
    TString FilterStoragePool;
    std::unordered_set<TNodeId> FilterNodeIds;
    std::unordered_set<ui32> FilterGroupIds;
    std::unordered_set<TNodeId> NodeIds;

    enum class EWith {
        Everything,
        MissingDisks,
        SpaceProblems,
    };

    EWith With = EWith::Everything;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonNodes(IViewer* viewer, const TRequest& request)
        : Viewer(viewer)
        , Initiator(request.Event->Sender)
        , Event(request.Event)
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        FilterTenant = params.Get("tenant");
        FilterStoragePool = params.Get("pool");
        SplitIds(params.Get("node_id"), ',', FilterNodeIds);
        if (params.Get("with") == "missing") {
            With = EWith::MissingDisks;
        } if (params.Get("with") == "space") {
            With = EWith::SpaceProblems;
        }
    }

    void Bootstrap() {
        RequestBSControllerConfig();
        if (!FilterTenant.empty()) {
            SendNavigate(FilterTenant);
        } else {
            auto itZero = FilterNodeIds.find(0);
            if (itZero != FilterNodeIds.end()) {
                FilterNodeIds.erase(itZero);
                FilterNodeIds.insert(TlsActivationContext->ActorSystem()->NodeId);
            }
            if (FilterNodeIds.empty()) {
                SendRequest(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
            } else {
                for (ui32 nodeId : FilterNodeIds) {
                    SendNodeRequest(nodeId);
                }
            }
        }
        if (Requests == 0) {
            ReplyAndPassAway();
            return;
        }
        TBase::Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void SendNavigate(const TString& path) {
        TAutoPtr<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
        if (!Event->Get()->UserToken.empty()) {
            request->Record.SetUserToken(Event->Get()->UserToken);
        }
        NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
        record->SetPath(path);
        request->Record.SetUserToken(Event->Get()->UserToken);
        TActorId txproxy = MakeTxProxyID();
        Send(txproxy, request.Release());
        ++Requests;
    }

    void PassAway() override {
        for (const TNodeId nodeId : NodeIds) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

    void SendNodeRequest(ui32 nodeId) {
        if (NodeIds.insert(nodeId).second) {
            TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
            SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvSystemStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
            SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvPDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        }
    }

    void SendGroupNodeRequests(ui32 groupId) {
        auto itBaseConfigGroupIndex = BaseConfigGroupIndex.find(groupId);
        if (itBaseConfigGroupIndex != BaseConfigGroupIndex.end()) {
            for (const NKikimrBlobStorage::TVSlotId& vslot : itBaseConfigGroupIndex->second->GetVSlotId()) {
                FilterNodeIds.emplace(vslot.GetNodeId());
                SendNodeRequest(vslot.GetNodeId());
            }
        }
    }

    void Handle(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr& ev) {
        for (const auto& matchingGroups : ev->Get()->Record.GetMatchingGroups()) {
            for (const auto& group : matchingGroups.GetGroups()) {
                TString storagePoolName = group.GetStoragePoolName();
                if (FilterStoragePool.empty() || FilterStoragePool == storagePoolName) {
                    if (FilterGroupIds.emplace(group.GetGroupID()).second && BaseConfig) {
                        SendGroupNodeRequests(group.GetGroupID());
                    }
                }
            }
        }
        RequestDone();
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(ev->Get()->Record);
        if (pbRecord.HasResponse() && pbRecord.GetResponse().StatusSize() > 0) {
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            if (pbStatus.HasBaseConfig()) {
                BaseConfig.reset(ev->Release().Release());
                const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(BaseConfig->Record);
                const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
                const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
                for (const NKikimrBlobStorage::TBaseConfig::TGroup& group : pbConfig.GetGroup()) {
                    BaseConfigGroupIndex[group.GetGroupId()] = &group;
                }
                for (ui32 groupId : FilterGroupIds) {
                    SendGroupNodeRequests(groupId);
                }
            }
        }
        RequestDone();
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        ui32 maxAllowedNodeId = std::numeric_limits<ui32>::max();
        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        if (dynamicNameserviceConfig) {
            maxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
        }
        NodesInfo.reset(ev->Release().Release());
        for (const auto& ni : NodesInfo->Nodes) {
            if (ni.NodeId <= maxAllowedNodeId) {
                SendNodeRequest(ni.NodeId);
            }
        }
        RequestDone();
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        TString path = ev->Get()->GetRecord().GetPath();
        const NKikimrSchemeOp::TPathDescription& pathDescription = ev->Get()->GetRecord().GetPathDescription();
        for (const auto& storagePool : pathDescription.GetDomainDescription().GetStoragePools()) {
            TString storagePoolName = storagePool.GetName();
            THolder<TEvBlobStorage::TEvControllerSelectGroups> request = MakeHolder<TEvBlobStorage::TEvControllerSelectGroups>();
            request->Record.SetReturnAllMatchingGroups(true);
            request->Record.AddGroupParameters()->MutableStoragePoolSpecifier()->SetName(storagePoolName);
            RequestBSControllerSelectGroups(std::move(request));
        }

        DescribeResult[path] = ev->Release();
        RequestDone();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->SourceType) {
        case TEvWhiteboard::EvSystemStateRequest:
            if (SysInfo.emplace(nodeId, nullptr).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvPDiskStateRequest:
            if (PDiskInfo.emplace(nodeId, nullptr).second) {
                RequestDone();
            }
            break;
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        if (SysInfo.emplace(nodeId, nullptr).second) {
            RequestDone();
        }
        if (PDiskInfo.emplace(nodeId, nullptr).second) {
            RequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        SysInfo[nodeId] = ev->Release();
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        PDiskInfo[nodeId] = ev->Release();
        RequestDone();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvBlobStorage::TEvControllerSelectGroupsResult, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ReplyAndPassAway() {
        NKikimrViewer::TNodesInfo nodesInfo;
        std::unordered_map<TNodeId, NKikimrViewer::TNodeInfo*> nodeIndex;
        std::unordered_map<TPDiskId, NKikimrWhiteboard::TPDiskStateInfo*> pDiskIndex;

        std::function<NKikimrViewer::TNodeInfo&(TNodeId)> getNode = [&nodesInfo, &nodeIndex](TNodeId nodeId) -> NKikimrViewer::TNodeInfo& {
            auto itNode = nodeIndex.find(nodeId);
            if (itNode != nodeIndex.end()) {
                return *(itNode->second);
            }
            NKikimrViewer::TNodeInfo& nodeInfo = *nodesInfo.AddNodes();
            nodeInfo.SetNodeId(nodeId);
            nodeIndex.emplace(nodeId, &nodeInfo);
            return nodeInfo;
        };

        std::function<NKikimrWhiteboard::TPDiskStateInfo&(TPDiskId)> getPDisk = [&getNode, &pDiskIndex](TPDiskId pDiskId) -> NKikimrWhiteboard::TPDiskStateInfo& {
            auto itPDisk = pDiskIndex.find(pDiskId);
            if (itPDisk != pDiskIndex.end()) {
                return *(itPDisk->second);
            }
            NKikimrViewer::TNodeInfo& nodeInfo = getNode(pDiskId.first);
            NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo = *nodeInfo.AddPDisks();
            pDiskInfo.SetPDiskId(pDiskId.second);
            pDiskIndex.emplace(pDiskId, &pDiskInfo);
            return pDiskInfo;
        };

        if (BaseConfig) {
            const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(BaseConfig->Record);
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
            for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pDisk : pbConfig.GetPDisk()) {
                TPDiskId pDiskId(pDisk.GetNodeId(), pDisk.GetPDiskId());
                NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo = getPDisk(pDiskId);
                pDiskInfo.SetPath(pDisk.GetPath());
                pDiskInfo.SetGuid(pDisk.GetGuid());
                pDiskInfo.SetCategory(static_cast<ui64>(pDisk.GetType()));
                pDiskInfo.SetTotalSize(pDisk.GetPDiskMetrics().GetTotalSize());
                pDiskInfo.SetAvailableSize(pDisk.GetPDiskMetrics().GetAvailableSize());
            }
        }

        for (TNodeId nodeId : NodeIds) {
            if (!FilterNodeIds.empty() && FilterNodeIds.count(nodeId) == 0) {
                continue;
            }
            NKikimrViewer::TNodeInfo& nodeInfo = getNode(nodeId);
            auto itSystemState = SysInfo.find(nodeId);
            if (itSystemState != SysInfo.end() && itSystemState->second) {
                *nodeInfo.MutableSystemState() = itSystemState->second->Record.GetSystemStateInfo(0);
            } else {
                auto* icNodeInfo = NodesInfo->GetNodeInfo(nodeId);
                if (icNodeInfo != nullptr) {
                    nodeInfo.MutableSystemState()->SetHost(icNodeInfo->Host);
                }
            }
            auto itPDiskState = PDiskInfo.find(nodeId);
            if (itPDiskState != PDiskInfo.end() && itPDiskState->second) {
                for (const auto& protoPDiskInfo : itPDiskState->second->Record.GetPDiskStateInfo()) {
                    NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo = getPDisk({nodeId, protoPDiskInfo.GetPDiskId()});
                    pDiskInfo.MergeFrom(protoPDiskInfo);
                }
            }
        }

        ui64 totalNodes = nodesInfo.NodesSize();

        if (!FilterNodeIds.empty() || !FilterTenant.empty()) {
            for (auto itNode = nodesInfo.MutableNodes()->begin(); itNode != nodesInfo.MutableNodes()->end();) {
                if (FilterNodeIds.count(itNode->GetNodeId()) == 0) {
                    itNode = nodesInfo.MutableNodes()->erase(itNode);
                } else {
                    ++itNode;
                }
            }
        }

        if (With == EWith::MissingDisks) {
            for (auto itNode = nodesInfo.MutableNodes()->begin(); itNode != nodesInfo.MutableNodes()->end();) {
                size_t disksNormal = 0;
                for (const NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo : itNode->GetPDisks()) {
                    if (pDiskInfo.GetState() == NKikimrBlobStorage::TPDiskState::Normal) {
                        ++disksNormal;
                    }
                }
                if (itNode->PDisksSize() == disksNormal && disksNormal != 0) {
                    itNode = nodesInfo.MutableNodes()->erase(itNode);
                } else {
                    ++itNode;
                }
            }
        }

        if (With == EWith::SpaceProblems) {
            for (auto itNode = nodesInfo.MutableNodes()->begin(); itNode != nodesInfo.MutableNodes()->end();) {
                if (itNode->GetSystemState().GetMaxDiskUsage() < 85) {
                    itNode = nodesInfo.MutableNodes()->erase(itNode);
                } else {
                    ++itNode;
                }
            }
        }

        ui64 foundNodes = nodesInfo.NodesSize();

        nodesInfo.SetTotalNodes(totalNodes);
        nodesInfo.SetFoundNodes(foundNodes);

        TStringStream json;
        TProtoToJson::ProtoToJson(json, nodesInfo, JsonSettings);
        Send(Initiator, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        ReplyAndPassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonNodes> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrViewer::TNodesInfo>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonNodes> {
    static TString GetParameters() {
        return R"___([{"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as numbers","required":false,"type":"boolean"},
                      {"name":"tenant","in":"query","description":"tenant filter","required":false,"type":"string"},
                      {"name":"with","in":"query","description":"filter groups by missing or space","required":false,"type":"string"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonNodes> {
    static TString GetSummary() {
        return "\"Storage nodes info\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonNodes> {
    static TString GetDescription() {
        return "\"Storage state by physicall nodes\"";
    }
};

}
}
