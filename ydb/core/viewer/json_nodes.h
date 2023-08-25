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
#include "viewer_helper.h"
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
    std::unordered_map<TNodeId, NKikimrWhiteboard::TEvPDiskStateResponse> PDiskInfo;
    std::unordered_map<TNodeId, NKikimrWhiteboard::TEvVDiskStateResponse> VDiskInfo;
    std::unordered_map<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse> TabletInfo;
    std::unordered_map<TNodeId, NKikimrWhiteboard::TEvSystemStateResponse> SysInfo;
    std::unordered_map<TString, THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>> DescribeResult;
    std::unique_ptr<TEvBlobStorage::TEvControllerConfigResponse> BaseConfig;
    std::unordered_map<ui32, const NKikimrBlobStorage::TBaseConfig::TGroup*> BaseConfigGroupIndex;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString FilterTenant;
    TString FilterStoragePool;
    std::unordered_set<TNodeId> FilterNodeIds;
    std::unordered_set<ui32> FilterGroupIds;
    std::unordered_set<TNodeId> PassedNodeIds;
    std::vector<TNodeId> NodeIds;
    std::optional<ui32> Offset;
    std::optional<ui32> Limit;
    ui32 UptimeSeconds = 0;
    bool ProblemNodesOnly = false;
    TString Filter;

    enum class EWith {
        Everything,
        MissingDisks,
        SpaceProblems,
    };
    EWith With = EWith::Everything;

    enum class EType {
        Any,
        Static,
        Dynamic,
    };
    EType Type = EType::Any;

    enum class ESort {
        NodeId,
        Host,
        DC,
        Rack,
        Version,
        Uptime,
        Memory,
        CPU,
        LoadAverage,
    };
    ESort Sort = ESort::NodeId;
    bool ReverseSort = false;
    bool SortedNodeList = false;
    bool LimitApplied = false;

    bool Storage = true;
    bool Tablets = true;
    bool ResolveGroupsToNodes = false;
    TNodeId MinAllowedNodeId = std::numeric_limits<TNodeId>::min();
    TNodeId MaxAllowedNodeId = std::numeric_limits<TNodeId>::max();
    ui32 RequestsBeforeNodeList = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TString GetLogPrefix() {
        static TString prefix = "json/nodes ";
        return prefix;
    }

    TJsonNodes(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Initiator(ev->Sender)
        , Event(std::move(ev))
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        UptimeSeconds = FromStringWithDefault<ui32>(params.Get("uptime"), 0);
        ProblemNodesOnly = FromStringWithDefault<bool>(params.Get("problems_only"), ProblemNodesOnly);
        Filter = params.Get("filter");
        FilterTenant = params.Get("tenant");
        FilterStoragePool = params.Get("pool");
        SplitIds(params.Get("node_id"), ',', FilterNodeIds);
        auto itZero = FilterNodeIds.find(0);
        if (itZero != FilterNodeIds.end()) {
            FilterNodeIds.erase(itZero);
            FilterNodeIds.insert(TlsActivationContext->ActorSystem()->NodeId);
        }
        if (params.Get("with") == "missing") {
            With = EWith::MissingDisks;
        } else if (params.Get("with") == "space") {
            With = EWith::SpaceProblems;
        }
        if (params.Has("offset")) {
            Offset = FromStringWithDefault<ui32>(params.Get("offset"), 0);
        }
        if (params.Has("limit")) {
            Limit = FromStringWithDefault<ui32>(params.Get("limit"), std::numeric_limits<ui32>::max());
        }
        if (params.Get("type") == "static") {
            Type = EType::Static;
        } else if (params.Get("type") == "dynamic") {
            Type = EType::Dynamic;
        } else if (params.Get("type") == "any") {
            Type = EType::Any;
        }
        Storage = FromStringWithDefault<bool>(params.Get("storage"), Storage);
        Tablets = FromStringWithDefault<bool>(params.Get("tablets"), Tablets);
        ResolveGroupsToNodes = FromStringWithDefault<bool>(params.Get("resolve_groups"), ResolveGroupsToNodes);
        TStringBuf sort = params.Get("sort");
        if (sort) {
            if (sort.StartsWith("-") || sort.StartsWith("+")) {
                ReverseSort = (sort[0] == '-');
                sort.Skip(1);
            }
            if (sort == "NodeId") {
                Sort = ESort::NodeId;
            } else if (sort == "Host") {
                Sort = ESort::Host;
            } else if (sort == "DC") {
                Sort = ESort::DC;
            } else if (sort == "Rack") {
                Sort = ESort::Rack;
            } else if (sort == "Version") {
                Sort = ESort::Version;
            } else if (sort == "Uptime") {
                Sort = ESort::Uptime;
            } else if (sort == "Memory") {
                Sort = ESort::Memory;
            } else if (sort == "CPU") {
                Sort = ESort::CPU;
            } else if (sort == "LoadAverage") {
                Sort = ESort::LoadAverage;
            }
        }
    }

    void Bootstrap() {
        BLOG_TRACE("Bootstrap()");
        if (Type != EType::Any) {
            TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
            if (dynamicNameserviceConfig) {
                if (Type == EType::Static) {
                    MaxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
                }
                if (Type == EType::Dynamic) {
                    MinAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId + 1;
                }
            }
        }

        if (Storage) {
            BLOG_TRACE("RequestBSControllerConfig()");
            RequestBSControllerConfig();
        }

        if (!FilterTenant.empty()) {
            if (Type == EType::Static || Type == EType::Any) {
                if (ResolveGroupsToNodes) {
                    if (!Storage) {
                        BLOG_TRACE("RequestBSControllerConfig()");
                        RequestBSControllerConfig();
                    }
                    ++RequestsBeforeNodeList;
                }
            }
            if (Type == EType::Dynamic || Type == EType::Any) {
                BLOG_TRACE("RequestStateStorageEndpointsLookup()");
                RequestStateStorageEndpointsLookup(FilterTenant); // to get dynamic nodes
                ++RequestsBeforeNodeList;
            }
        }
        BLOG_TRACE("Request TEvListNodes");
        SendRequest(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        ++RequestsBeforeNodeList;
        if (Requests == 0) {
            ReplyAndPassAway();
            return;
        }
        TBase::Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        for (const TNodeId nodeId : NodeIds) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

    void SendNodeRequest(TNodeId nodeId) {
        if (PassedNodeIds.insert(nodeId).second) {
            if (SortedNodeList && With == EWith::Everything) {
                // optimization for paging with default sort
                LimitApplied = true;
                if (Offset.has_value()) {
                    if (PassedNodeIds.size() <= Offset.value()) {
                        return;
                    }
                }
                if (Limit.has_value()) {
                    if (NodeIds.size() >= Limit.value()) {
                        return;
                    }
                }
            }
            NodeIds.push_back(nodeId); // order is important
            TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
            SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvSystemStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
            if (Storage) {
                SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvPDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
                SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvVDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
            }
            if (Tablets) {
                auto request = std::make_unique<TEvWhiteboard::TEvTabletStateRequest>();
                request->Record.SetGroupBy("Type,State");
                SendRequest(whiteboardServiceId, request.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
            }
        }
    }

    void ProcessNodeIds() {
        BLOG_TRACE("ProcessNodeIds()");

        switch (Sort) {
            case ESort::NodeId: {
                SortCollection(NodesInfo->Nodes, [](const TEvInterconnect::TNodeInfo& node) { return node.NodeId;}, ReverseSort);
                SortedNodeList = true;
                break;
            }
            case ESort::Host: {
                SortCollection(NodesInfo->Nodes, [](const TEvInterconnect::TNodeInfo& node) { return node.Host;}, ReverseSort);
                SortedNodeList = true;
                break;
            }
            case ESort::DC: {
                SortCollection(NodesInfo->Nodes, [](const TEvInterconnect::TNodeInfo& node) { return node.Location.GetDataCenterId();}, ReverseSort);
                SortedNodeList = true;
                break;
            }
            default:
                break;
        }

        for (const auto& ni : NodesInfo->Nodes) {
            if ((FilterNodeIds.empty() || FilterNodeIds.count(ni.NodeId) > 0) && ni.NodeId >= MinAllowedNodeId && ni.NodeId <= MaxAllowedNodeId) {
                SendNodeRequest(ni.NodeId);
            }
        }
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        BLOG_TRACE("Received TEvControllerConfigResponse");
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
            }
        }
        if (ResolveGroupsToNodes) {
            RequestTxProxyDescribe(FilterTenant); // to get storage pools and then groups and then pdisks
            ++RequestsBeforeNodeList;

            if (--RequestsBeforeNodeList == 0) {
                ProcessNodeIds();
            }
        }

        RequestDone();
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        BLOG_TRACE("Received TEvDescribeSchemeResult");
        TString path = ev->Get()->GetRecord().GetPath();
        const NKikimrSchemeOp::TPathDescription& pathDescription = ev->Get()->GetRecord().GetPathDescription();
        if (Storage) {
            for (const auto& storagePool : pathDescription.GetDomainDescription().GetStoragePools()) {
                TString storagePoolName = storagePool.GetName();
                THolder<TEvBlobStorage::TEvControllerSelectGroups> request = MakeHolder<TEvBlobStorage::TEvControllerSelectGroups>();
                request->Record.SetReturnAllMatchingGroups(true);
                request->Record.AddGroupParameters()->MutableStoragePoolSpecifier()->SetName(storagePoolName);
                RequestBSControllerSelectGroups(std::move(request));
                ++RequestsBeforeNodeList;
            }
        }

        DescribeResult[path] = ev->Release();

        if (--RequestsBeforeNodeList == 0) {
            ProcessNodeIds();
        }
        RequestDone();
    }

    void Handle(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr& ev) {
        BLOG_TRACE("Received TEvControllerSelectGroupsResult");
        for (const auto& matchingGroups : ev->Get()->Record.GetMatchingGroups()) {
            for (const auto& group : matchingGroups.GetGroups()) {
                TString storagePoolName = group.GetStoragePoolName();
                if (FilterStoragePool.empty() || FilterStoragePool == storagePoolName) {
                    if (FilterGroupIds.emplace(group.GetGroupID()).second && BaseConfig) {
                        auto itBaseConfigGroupIndex = BaseConfigGroupIndex.find(group.GetGroupID());
                        if (itBaseConfigGroupIndex != BaseConfigGroupIndex.end()) {
                            for (const NKikimrBlobStorage::TVSlotId& vslot : itBaseConfigGroupIndex->second->GetVSlotId()) {
                                FilterNodeIds.insert(vslot.GetNodeId());
                            }
                        }
                    }
                }
            }
        }
        if (--RequestsBeforeNodeList == 0) {
            ProcessNodeIds();
        }
        RequestDone();
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        BLOG_TRACE("Received TEvNodesInfo");
        NodesInfo.reset(ev->Release().Release());
        if (--RequestsBeforeNodeList == 0) {
            ProcessNodeIds();
        }
        RequestDone();
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BLOG_TRACE("Received TEvBoardInfo");
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            for (const auto& [actorId, infoEntry] : ev->Get()->InfoEntries) {
                FilterNodeIds.insert(actorId.NodeId());
            }
        }
        if (--RequestsBeforeNodeList == 0) {
            ProcessNodeIds();
        }
        RequestDone();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->SourceType) {
        case TEvWhiteboard::EvSystemStateRequest:
            if (SysInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvPDiskStateRequest:
            if (PDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvPDiskStateResponse{}).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvVDiskStateRequest:
            if (VDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvVDiskStateResponse{}).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvTabletStateRequest:
            if (TabletInfo.emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                RequestDone();
            }
            break;
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        if (SysInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
            RequestDone();
        }
        if (Storage) {
            if (PDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvPDiskStateResponse{}).second) {
                RequestDone();
            }
            if (VDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvVDiskStateResponse{}).second) {
                RequestDone();
            }
        }
        if (Tablets) {
            if (TabletInfo.emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                RequestDone();
            }
        }
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        SysInfo[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        PDiskInfo[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        VDiskInfo[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        bool needToGroup = ev->Get()->Record.TabletStateInfoSize() > 0 && !ev->Get()->Record.GetTabletStateInfo(0).HasCount();
        TabletInfo[nodeId] = std::move(ev->Get()->Record);
        if (needToGroup) { // for compatibility with older versions
            GroupWhiteboardResponses(TabletInfo[nodeId], "Type,Overall", false);
        }
        RequestDone();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvBlobStorage::TEvControllerSelectGroupsResult, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    NKikimrWhiteboard::TPDiskStateInfo& GetPDisk(TPDiskId pDiskId) {
        auto itPDiskInfo = PDiskInfo.find(pDiskId.first);
        if (itPDiskInfo == PDiskInfo.end()) {
            itPDiskInfo = PDiskInfo.insert({pDiskId.first, NKikimrWhiteboard::TEvPDiskStateResponse{}}).first;
        }

        for (auto& pDiskInfo : *itPDiskInfo->second.mutable_pdiskstateinfo()) {
            if (pDiskInfo.pdiskid() == pDiskId.second) {
                return pDiskInfo;
            }
        }

        NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo = *itPDiskInfo->second.add_pdiskstateinfo();
        pDiskInfo.SetPDiskId(pDiskId.second);
        return pDiskInfo;
    }

    static double GetCPU(const NKikimrWhiteboard::TSystemStateInfo& sysInfo) {
        double cpu = 0;
        if (sysInfo.PoolStatsSize() > 0) {
            for (const auto& ps : sysInfo.GetPoolStats()) {
                cpu = std::max(cpu, ps.GetUsage());
            }
        }
        return cpu;
    }

    static double GetLoadAverage(const NKikimrWhiteboard::TSystemStateInfo& sysInfo) {
        if (sysInfo.LoadAverageSize() > 0) {
            return sysInfo.GetLoadAverage(0);
        }
        return 0;
    }

    bool CheckNodeFilters(TNodeId nodeId) {
        auto itSysInfo = SysInfo.find(nodeId);
        if (itSysInfo != SysInfo.end()) {
            if (itSysInfo->second.SystemStateInfoSize() == 1) {
                const NKikimrWhiteboard::TSystemStateInfo& sysInfo = itSysInfo->second.GetSystemStateInfo(0);
                if (UptimeSeconds > 0 && sysInfo.HasStartTime() && itSysInfo->second.HasResponseTime()
                        && itSysInfo->second.GetResponseTime() - sysInfo.GetStartTime() > UptimeSeconds * 1000) {
                    return false;
                }
                if (ProblemNodesOnly && sysInfo.HasSystemState()
                        && GetViewerFlag(sysInfo.GetSystemState()) == NKikimrViewer::EFlag::Green) {
                    return false;
                }
                if (Filter) {
                    if (sysInfo.HasHost() && sysInfo.GetHost().Contains(Filter)) {
                        return true;
                    }
                    if (std::to_string(nodeId).contains(Filter)) {
                        return true;
                    }
                    return false;
                }
            }
        }
        return true;
    }

    void ReplyAndPassAway() {
        NKikimrViewer::TNodesInfo result;
        if (Storage && BaseConfig) {
            const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(BaseConfig->Record);
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
            for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pDisk : pbConfig.GetPDisk()) {
                if (!FilterNodeIds.empty() && FilterNodeIds.count(pDisk.GetNodeId()) == 0) {
                    continue;
                }
                if (pDisk.GetNodeId() < MinAllowedNodeId || pDisk.GetNodeId() > MaxAllowedNodeId) {
                    continue;
                }
                TPDiskId pDiskId(pDisk.GetNodeId(), pDisk.GetPDiskId());
                NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo = GetPDisk(pDiskId);
                pDiskInfo.SetPath(pDisk.GetPath());
                pDiskInfo.SetGuid(pDisk.GetGuid());
                pDiskInfo.SetCategory(static_cast<ui64>(pDisk.GetType()));
                if (pDiskInfo.GetTotalSize() == 0) {
                    pDiskInfo.SetTotalSize(pDisk.GetPDiskMetrics().GetTotalSize());
                }
                if (pDiskInfo.GetAvailableSize() == 0) {
                    pDiskInfo.SetAvailableSize(pDisk.GetPDiskMetrics().GetAvailableSize());
                }
            }
        }

        for (TNodeId nodeId : NodeIds) {
            if (!CheckNodeFilters(nodeId))
                continue;
            if (Storage) {
                if (With == EWith::MissingDisks) {
                    auto itPDiskState = PDiskInfo.find(nodeId);
                    if (itPDiskState != PDiskInfo.end()) {
                        int disksNormal = 0;
                        for (const auto& protoPDiskInfo : itPDiskState->second.GetPDiskStateInfo()) {
                            if (protoPDiskInfo.state() == NKikimrBlobStorage::TPDiskState::Normal) {
                                ++disksNormal;
                            }
                        }
                        if (itPDiskState->second.pdiskstateinfo_size() == disksNormal) {
                            continue;
                        }
                    }
                }
                if (With == EWith::SpaceProblems) {
                    auto itSystemState = SysInfo.find(nodeId);
                    if (itSystemState != SysInfo.end() && itSystemState->second.SystemStateInfoSize() > 0) {
                        if (itSystemState->second.GetSystemStateInfo(0).GetMaxDiskUsage() < 0.85) {
                            continue;
                        }
                    }
                }
            }
            NKikimrViewer::TNodeInfo& nodeInfo = *result.add_nodes();
            nodeInfo.set_nodeid(nodeId);
            auto itSystemState = SysInfo.find(nodeId);
            if (itSystemState != SysInfo.end() && itSystemState->second.SystemStateInfoSize() > 0) {
                *nodeInfo.MutableSystemState() = itSystemState->second.GetSystemStateInfo(0);
            } else if (NodesInfo != nullptr) {
                auto* icNodeInfo = NodesInfo->GetNodeInfo(nodeId);
                if (icNodeInfo != nullptr) {
                    nodeInfo.MutableSystemState()->SetHost(icNodeInfo->Host);
                }
            }
            if (Storage) {
                auto itPDiskState = PDiskInfo.find(nodeId);
                if (itPDiskState != PDiskInfo.end()) {
                    for (auto& protoPDiskInfo : *itPDiskState->second.MutablePDiskStateInfo()) {
                        NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo = *nodeInfo.AddPDisks();
                        pDiskInfo = std::move(protoPDiskInfo);
                    }
                }
                auto itVDiskState = VDiskInfo.find(nodeId);
                if (itVDiskState != VDiskInfo.end()) {
                    for (auto& protoVDiskInfo : *itVDiskState->second.MutableVDiskStateInfo()) {
                        NKikimrWhiteboard::TVDiskStateInfo& vDiskInfo = *nodeInfo.AddVDisks();
                        vDiskInfo = std::move(protoVDiskInfo);
                    }
                }
            }
            if (Tablets) {
                auto itTabletState = TabletInfo.find(nodeId);
                if (itTabletState != TabletInfo.end()) {
                    for (auto& protoTabletInfo : *itTabletState->second.MutableTabletStateInfo()) {
                        NKikimrWhiteboard::TTabletStateInfo& tabletInfo = *nodeInfo.AddTablets();
                        tabletInfo = std::move(protoTabletInfo);
                    }
                }
            }
        }

        ui64 totalNodes = PassedNodeIds.size();
        ui64 foundNodes;

        if (With == EWith::Everything) {
            foundNodes = totalNodes;
        } else {
            foundNodes = result.NodesSize();
        }

        if (!SortedNodeList) {
            switch (Sort) {
                case ESort::NodeId:
                case ESort::Host:
                case ESort::DC:
                    // already sorted
                    break;
                case ESort::Rack:
                    SortCollection(*result.MutableNodes(), [](const NKikimrViewer::TNodeInfo& node) { return node.GetSystemState().GetRack();}, ReverseSort);
                    break;
                case ESort::Version:
                    SortCollection(*result.MutableNodes(), [](const NKikimrViewer::TNodeInfo& node) { return node.GetSystemState().GetVersion();}, ReverseSort);
                    break;
                case ESort::Uptime:
                    SortCollection(*result.MutableNodes(), [](const NKikimrViewer::TNodeInfo& node) { return node.GetSystemState().GetStartTime();}, ReverseSort);
                    break;
                case ESort::Memory:
                    SortCollection(*result.MutableNodes(), [](const NKikimrViewer::TNodeInfo& node) { return node.GetSystemState().GetMemoryUsed();}, ReverseSort);
                    break;
                case ESort::CPU:
                    SortCollection(*result.MutableNodes(), [](const NKikimrViewer::TNodeInfo& node) { return GetCPU(node.GetSystemState());}, ReverseSort);
                    break;
                case ESort::LoadAverage:
                    SortCollection(*result.MutableNodes(), [](const NKikimrViewer::TNodeInfo& node) { return GetLoadAverage(node.GetSystemState());}, ReverseSort);
                    break;
            }
        }

        if (!LimitApplied) {
            auto& nodes = *result.MutableNodes();
            if (Offset.has_value()) {
                if (size_t(nodes.size()) > Offset.value()) {
                    nodes.erase(nodes.begin(), std::next(nodes.begin(), Offset.value()));
                } else {
                    nodes.Clear();
                }
            }
            if (Limit.has_value()) {
                if (size_t(nodes.size()) > Limit.value()) {
                    nodes.erase(std::next(nodes.begin(), Limit.value()), nodes.end());
                }
            }
        }

        result.SetTotalNodes(totalNodes);
        result.SetFoundNodes(foundNodes);

        TStringStream json;
        TProtoToJson::ProtoToJson(json, result, JsonSettings);
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
                      {"name":"with","in":"query","description":"filter nodes by missing disks or space","required":false,"type":"string"},
                      {"name":"type","in":"query","description":"nodes type to get (static,dynamic,any)","required":false,"type":"string"},
                      {"name":"storage","in":"query","description":"return storage info","required":false,"type":"boolean"},
                      {"name":"tablets","in":"query","description":"return tablets info","required":false,"type":"boolean"},
                      {"name":"sort","in":"query","description":"sort by (NodeId,Host,DC,Rack,Version,Uptime,Memory,CPU,LoadAverage)","required":false,"type":"string"},
                      {"name":"offset","in":"query","description":"skip N nodes","required":false,"type":"integer"},
                      {"name":"limit","in":"query","description":"limit to N nodes","required":false,"type":"integer"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"},
                      {"name":"uptime","in":"query","description":"return only nodes with less uptime in sec.","required":false,"type":"integer"},
                      {"name":"problems_only","in":"query","description":"return only problem nodes","required":false,"type":"boolean"},
                      {"name":"filter","in":"query","description":"filter nodes by id or host","required":false,"type":"string"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonNodes> {
    static TString GetSummary() {
        return "\"Nodes info\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonNodes> {
    static TString GetDescription() {
        return "\"Information about nodes\"";
    }
};

}
}
