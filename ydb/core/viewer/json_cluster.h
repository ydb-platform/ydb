#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/viewer/json/json.h>
#include "json_pipe_req.h"
#include "viewer.h"
#include "viewer_probes.h"

LWTRACE_USING(VIEWER_PROVIDER);

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;
using ::google::protobuf::FieldDescriptor;

class TJsonCluster : public TViewerPipeClient<TJsonCluster> {
    using TThis = TJsonCluster;
    using TBase = TViewerPipeClient<TJsonCluster>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TMap<TNodeId, NKikimrWhiteboard::TEvSystemStateResponse> SystemInfo;
    TMap<TNodeId, NKikimrWhiteboard::TEvVDiskStateResponse> VDiskInfo;
    TMap<TNodeId, NKikimrWhiteboard::TEvPDiskStateResponse> PDiskInfo;
    TMap<TNodeId, NKikimrWhiteboard::TEvBSGroupStateResponse> BSGroupInfo;
    TMap<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse> TabletInfo;
    THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    TSet<TNodeId> NodesAlive;
    TJsonSettings JsonSettings;
    ui32 Timeout;
    ui32 TenantsNumber = 0;
    bool Tablets = false;

    struct TEventLog {
        bool IsTimeout = false;
        TInstant StartTime;
        TInstant StartHandleListTenantsResponseTime;
        TInstant StartHandleNodesInfoTime;
        TInstant StartMergeBSGroupsTime;
        TInstant StartMergeVDisksTime;
        TInstant StartMergePDisksTime;
        TInstant StartMergeTabletsTime;
        TInstant StartResponseBuildingTime;
    };
    TEventLog EventLog;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonCluster(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Tablets = FromStringWithDefault<bool>(params.Get("tablets"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
    }

    void Bootstrap(const TActorContext& ) {
        EventLog.StartTime = TActivationContext::Now();
        SendRequest(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        RequestConsoleListTenants();
        Become(&TThis::StateRequested, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        if (NodesInfo != nullptr) {
            TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
            for (const auto& ni : NodesInfo->Nodes) {
                if (ni.NodeId <= dynamicNameserviceConfig->MaxStaticNodeId) {
                    Send(TActivationContext::InterconnectProxy(ni.NodeId), new TEvents::TEvUnsubscribe);
                }
            }
        }
        TBase::PassAway();
    }

    void SendWhiteboardTabletStateRequest() {
        THashSet<TTabletId> filterTablets;
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        if (const auto& domain = domains->Domain) {
            for (TTabletId id : domain->Coordinators) {
                filterTablets.emplace(id);
            }
            for (TTabletId id : domain->Mediators) {
                filterTablets.emplace(id);
            }
            for (TTabletId id : domain->TxAllocators) {
                filterTablets.emplace(id);
            }
            filterTablets.emplace(domain->SchemeRoot);
            filterTablets.emplace(domains->GetHive());
        }
        filterTablets.emplace(MakeBSControllerID());
        filterTablets.emplace(MakeDefaultHiveID());
        filterTablets.emplace(MakeCmsID());
        filterTablets.emplace(MakeNodeBrokerID());
        filterTablets.emplace(MakeTenantSlotBrokerID());
        filterTablets.emplace(MakeConsoleID());
        const NKikimrSchemeOp::TPathDescription& pathDescription(DescribeResult->GetRecord().GetPathDescription());
        if (pathDescription.HasDomainDescription()) {
            const NKikimrSubDomains::TDomainDescription& domainDescription(pathDescription.GetDomainDescription());
            for (TTabletId tabletId : domainDescription.GetProcessingParams().GetCoordinators()) {
                filterTablets.emplace(tabletId);
            }
            for (TTabletId tabletId : domainDescription.GetProcessingParams().GetMediators()) {
                filterTablets.emplace(tabletId);
            }
            if (domainDescription.HasDomainKey()) {
                if (domainDescription.GetDomainKey().HasSchemeShard()) {
                    filterTablets.emplace(domainDescription.GetDomainKey().GetSchemeShard());
                }
            }
        }

        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        for (const auto& ni : NodesInfo->Nodes) {
            if (ni.NodeId <= dynamicNameserviceConfig->MaxStaticNodeId) {
                TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(ni.NodeId);
                auto request = new TEvWhiteboard::TEvTabletStateRequest();
                for (TTabletId id: filterTablets) {
                    request->Record.AddFilterTabletId(id);
                }
                SendRequest(whiteboardServiceId, request, IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ni.NodeId);
            }
        }
    }

    void SendWhiteboardRequests() {
        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        for (const auto& ni : NodesInfo->Nodes) {
            TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(ni.NodeId);
            SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvSystemStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ni.NodeId);

            if (ni.NodeId <= dynamicNameserviceConfig->MaxStaticNodeId) {
                SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvVDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ni.NodeId);
                SendRequest(whiteboardServiceId,new TEvWhiteboard::TEvPDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ni.NodeId);
                SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvBSGroupStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ni.NodeId);
            }
        }
        if (Tablets) {
            SendWhiteboardTabletStateRequest();
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        EventLog.StartHandleNodesInfoTime = TActivationContext::Now();
        NodesInfo = ev->Release();
        // before making requests to Whiteboard with the Tablets parameter, we need to review the TEvDescribeSchemeResult information
        if (Tablets) {
            THolder<TEvTxUserProxy::TEvNavigate> request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            if (!Event->Get()->UserToken.empty()) {
                request->Record.SetUserToken(Event->Get()->UserToken);
            }
            NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
            TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
            if (const auto& domain = domains->Domain) {
                TString domainPath = "/" + domain->Name;
                record->SetPath(domainPath);
            }
            record->MutableOptions()->SetReturnPartitioningInfo(false);
            record->MutableOptions()->SetReturnPartitionConfig(false);
            record->MutableOptions()->SetReturnChildren(false);
            SendRequest(MakeTxProxyID(), request.Release());
        } else {
            SendWhiteboardRequests();
        }

        RequestDone();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        ui32 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->SourceType) {
        case TEvWhiteboard::EvSystemStateRequest:
            if (SystemInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvVDiskStateRequest:
            if (VDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvVDiskStateResponse{}).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvPDiskStateRequest:
            if (PDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvPDiskStateResponse{}).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvBSGroupStateRequest:
            if (BSGroupInfo.emplace(nodeId, NKikimrWhiteboard::TEvBSGroupStateResponse{}).second) {
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

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        ui32 nodeId = ev->Get()->NodeId;
        if (SystemInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
            RequestDone();
        }
        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        if (nodeId <= dynamicNameserviceConfig->MaxStaticNodeId) {
            if (VDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvVDiskStateResponse{}).second) {
                RequestDone();
            }
            if (PDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvPDiskStateResponse{}).second) {
                RequestDone();
            }
            if (BSGroupInfo.emplace(nodeId, NKikimrWhiteboard::TEvBSGroupStateResponse{}).second) {
                RequestDone();
            }
            if (Tablets) {
                if (TabletInfo.emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                    RequestDone();
                }
            }
        }
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        SystemInfo[nodeId] = std::move(ev->Get()->Record);
        NodesAlive.insert(nodeId);
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        VDiskInfo[nodeId] = std::move(ev->Get()->Record);
        NodesAlive.insert(nodeId);
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        PDiskInfo[nodeId] = std::move(ev->Get()->Record);
        NodesAlive.insert(nodeId);
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvBSGroupStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        BSGroupInfo[nodeId] = std::move(ev->Get()->Record);
        NodesAlive.insert(nodeId);
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        TabletInfo[nodeId] = std::move(ev->Get()->Record);
        NodesAlive.insert(nodeId);
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        EventLog.StartHandleListTenantsResponseTime = TActivationContext::Now();
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        TenantsNumber = listTenantsResult.paths().size();
        RequestDone();
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        if (ev->Get()->GetRecord().GetStatus() == NKikimrScheme::StatusSuccess) {
            DescribeResult = ev->Release();
            SendWhiteboardRequests();
        }
        RequestDone();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            RequestDone();
        }
    }

    STATEFN(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    NKikimrWhiteboard::TEvBSGroupStateResponse MergedBSGroupInfo;
    NKikimrWhiteboard::TEvVDiskStateResponse MergedVDiskInfo;
    NKikimrWhiteboard::TEvPDiskStateResponse MergedPDiskInfo;
    NKikimrWhiteboard::TEvTabletStateResponse MergedTabletInfo;
    TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&> VDisksIndex;
    TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&> PDisksIndex;

    void ReplyAndPassAway() {
        EventLog.StartMergeBSGroupsTime = TActivationContext::Now();
        MergeWhiteboardResponses(MergedBSGroupInfo, BSGroupInfo);
        EventLog.StartMergeVDisksTime = TActivationContext::Now();
        MergeWhiteboardResponses(MergedVDiskInfo, VDiskInfo);
        EventLog.StartMergePDisksTime = TActivationContext::Now();
        MergeWhiteboardResponses(MergedPDiskInfo, PDiskInfo);

        EventLog.StartMergeTabletsTime = TActivationContext::Now();
        THashSet<TTabletId> tablets;
        if (Tablets) {
            MergeWhiteboardResponses(MergedTabletInfo, TabletInfo);
        }

        EventLog.StartResponseBuildingTime = TActivationContext::Now();
        if (Tablets) {
            TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
            if (const auto& domain = domains->Domain) {
                tablets.emplace(MakeBSControllerID());
                tablets.emplace(MakeDefaultHiveID());
                tablets.emplace(MakeCmsID());
                tablets.emplace(MakeNodeBrokerID());
                tablets.emplace(MakeTenantSlotBrokerID());
                tablets.emplace(MakeConsoleID());
                tablets.emplace(domain->SchemeRoot);
                tablets.emplace(domains->GetHive());
                for (TTabletId id : domain->Coordinators) {
                    tablets.emplace(id);
                }
                for (TTabletId id : domain->Mediators) {
                    tablets.emplace(id);
                }
                for (TTabletId id : domain->TxAllocators) {
                    tablets.emplace(id);
                }
            }

            if (DescribeResult) {
                const NKikimrSchemeOp::TPathDescription& pathDescription(DescribeResult->GetRecord().GetPathDescription());
                if (pathDescription.HasDomainDescription()) {
                    const NKikimrSubDomains::TDomainDescription& domainDescription(pathDescription.GetDomainDescription());
                    for (TTabletId tabletId : domainDescription.GetProcessingParams().GetCoordinators()) {
                        tablets.emplace(tabletId);
                    }
                    for (TTabletId tabletId : domainDescription.GetProcessingParams().GetMediators()) {
                        tablets.emplace(tabletId);
                    }
                    if (domainDescription.HasDomainKey()) {
                        if (domainDescription.GetDomainKey().HasSchemeShard()) {
                            tablets.emplace(domainDescription.GetDomainKey().GetSchemeShard());
                        }
                    }
                }
            }
        }

        ui64 totalStorageSize = 0;
        ui64 availableStorageSize = 0;

        for (auto& element : TWhiteboardInfo<NKikimrWhiteboard::TEvPDiskStateResponse>::GetElementsField(MergedPDiskInfo)) {
            if (element.HasTotalSize() && element.HasAvailableSize()) {
                totalStorageSize += element.GetTotalSize();
                availableStorageSize += element.GetAvailableSize();
            }
            element.SetStateFlag(GetWhiteboardFlag(GetPDiskStateFlag(element)));
            element.SetOverall(GetWhiteboardFlag(GetPDiskOverallFlag(element)));
            PDisksIndex.emplace(TWhiteboardInfo<NKikimrWhiteboard::TEvPDiskStateResponse>::GetElementKey(element), element);
        }
        for (auto& element : TWhiteboardInfo<NKikimrWhiteboard::TEvVDiskStateResponse>::GetElementsField(MergedVDiskInfo)) {
            element.SetOverall(GetWhiteboardFlag(GetVDiskOverallFlag(element)));
            VDisksIndex.emplace(TWhiteboardInfo<NKikimrWhiteboard::TEvVDiskStateResponse>::GetElementKey(element), element);
        }
        NKikimrViewer::EFlag flag = NKikimrViewer::Grey;
        for (const auto& element : TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse>::GetElementsField(MergedBSGroupInfo)) {
            flag = Max(flag, GetBSGroupOverallFlag(element, VDisksIndex, PDisksIndex));
        }
        ui32 numberOfCpus = 0;
        double loadAverage = 0;
        THashSet<TString> dataCenters;
        THashSet<TString> versions;
        THashSet<TString> hosts;
        THashMap<TString, int> names;
        for (const auto& [nodeId, sysInfo] : SystemInfo) {
            if (sysInfo.SystemStateInfoSize() > 0) {
                const NKikimrWhiteboard::TSystemStateInfo& systemState = sysInfo.GetSystemStateInfo(0);
                if (systemState.HasNumberOfCpus() && (!systemState.HasHost() || hosts.emplace(systemState.GetHost()).second)) {
                    numberOfCpus += systemState.GetNumberOfCpus();
                    if (systemState.LoadAverageSize() > 0) {
                        loadAverage += systemState.GetLoadAverage(0);
                    }
                }
                if (systemState.HasDataCenter()) {
                    dataCenters.insert(systemState.GetDataCenter());
                }
                if (systemState.HasVersion()) {
                    versions.insert(systemState.GetVersion());
                }
                if (systemState.HasClusterName()) {
                    names[systemState.GetClusterName()]++;
                }
            }
        }

        NKikimrViewer::TClusterInfo pbCluster;

        if (Tablets) {
            for (const NKikimrWhiteboard::TTabletStateInfo& tabletInfo : MergedTabletInfo.GetTabletStateInfo()) {
                if (tablets.contains(tabletInfo.GetTabletId())) {
                    NKikimrWhiteboard::TTabletStateInfo* tablet = pbCluster.AddSystemTablets();
                    tablet->CopyFrom(tabletInfo);
                    auto tabletFlag = GetWhiteboardFlag(GetFlagFromTabletState(tablet->GetState()));
                    tablet->SetOverall(tabletFlag);
                    flag = Max(flag, GetViewerFlag(tabletFlag));
                }
            }
            pbCluster.SetTablets(MergedTabletInfo.TabletStateInfoSize());
        }
        pbCluster.SetTenants(TenantsNumber);

        pbCluster.SetOverall(flag);
        if (NodesInfo != nullptr) {
            pbCluster.SetNodesTotal(NodesInfo->Nodes.size());
            pbCluster.SetNodesAlive(NodesAlive.size());
        }
        pbCluster.SetNumberOfCpus(numberOfCpus);
        pbCluster.SetLoadAverage(loadAverage);
        pbCluster.SetStorageTotal(totalStorageSize);
        pbCluster.SetStorageUsed(totalStorageSize - availableStorageSize);
        pbCluster.SetHosts(hosts.size());
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        if (const auto& domain = domains->Domain) {
            TString domainName = "/" + domain->Name;
            pbCluster.SetDomain(domainName);
        }
        for (const TString& dc : dataCenters) {
            pbCluster.AddDataCenters(dc);
        }
        for (const TString& version : versions) {
            pbCluster.AddVersions(version);
        }
        auto itMax = std::max_element(names.begin(), names.end(), [](const auto& a, const auto& b) {
            return a.second < b.second;
        });
        if (itMax != names.end()) {
            pbCluster.SetName(itMax->first);
        }

        TStringStream json;
        TProtoToJson::ProtoToJson(json, pbCluster, JsonSettings);
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));

        const TInstant now = TActivationContext::Now();
        LWPROBE(ViewerClusterHandler, TBase::SelfId().NodeId(), Tablets, EventLog.IsTimeout,
            EventLog.StartTime.MilliSeconds(),
            (now - EventLog.StartTime).MilliSeconds(),
            (EventLog.StartHandleListTenantsResponseTime - EventLog.StartTime).MilliSeconds(),
            (EventLog.StartHandleNodesInfoTime - EventLog.StartTime).MilliSeconds(),
            (EventLog.StartMergeBSGroupsTime - EventLog.StartTime).MilliSeconds(),
            (EventLog.StartMergeVDisksTime - EventLog.StartMergeBSGroupsTime).MilliSeconds(),
            (EventLog.StartMergePDisksTime - EventLog.StartMergeVDisksTime).MilliSeconds(),
            (EventLog.StartMergeTabletsTime - EventLog.StartMergePDisksTime).MilliSeconds(),
            (EventLog.StartResponseBuildingTime - EventLog.StartMergeTabletsTime).MilliSeconds(),
            (now - EventLog.StartResponseBuildingTime).MilliSeconds()
        );

        PassAway();
    }

    void HandleTimeout() {
        EventLog.IsTimeout = true;
        ReplyAndPassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonCluster> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TClusterInfo>();
    }
};

template <>
struct TJsonRequestParameters<TJsonCluster> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: tablets
              in: query
              description: return system tablets state
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonCluster> {
    static TString GetSummary() {
        return "Cluster information";
    }
};

template <>
struct TJsonRequestDescription<TJsonCluster> {
    static TString GetDescription() {
        return "Returns information about cluster";
    }
};

}
}
