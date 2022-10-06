#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;
using ::google::protobuf::FieldDescriptor;

class TJsonCluster : public TActorBootstrapped<TJsonCluster> {
    using TThis = TJsonCluster;
    using TBase = TActorBootstrapped<TJsonCluster>;
    IViewer* Viewer;
    TActorId Initiator;
    ui32 Requested;
    ui32 Received;
    NMon::TEvHttpInfo::TPtr Event;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TMap<TNodeId, THolder<TEvWhiteboard::TEvSystemStateResponse>> SystemInfo;
    TMap<TNodeId, THolder<TEvWhiteboard::TEvVDiskStateResponse>> VDiskInfo;
    TMap<TNodeId, THolder<TEvWhiteboard::TEvPDiskStateResponse>> PDiskInfo;
    TMap<TNodeId, THolder<TEvWhiteboard::TEvBSGroupStateResponse>> BSGroupInfo;
    TMap<TNodeId, THolder<TEvWhiteboard::TEvTabletStateResponse>> TabletInfo;
    THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    TSet<TNodeId> NodesAlive;
    TJsonSettings JsonSettings;
    ui32 Timeout;
    bool Tablets = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonCluster(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Initiator(ev->Sender)
        , Requested(0)
        , Received(0)
        , Event(ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Tablets = FromStringWithDefault<bool>(params.Get("tablets"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
    }

    void Bootstrap(const TActorContext& ctx) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        TBase::Become(&TThis::StateRequestedBrowse);
        ctx.Schedule(TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void Die(const TActorContext& ctx) override {
        if (NodesInfo != nullptr) {
            for (const auto& ni : NodesInfo->Nodes) {
                ctx.Send(TActivationContext::InterconnectProxy(ni.NodeId), new TEvents::TEvUnsubscribe());
            }
        }
        TBase::Die(ctx);
    }

    void SendRequest(ui32 nodeId, const TActorContext& ctx) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvSystemStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++Requested;
        ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvVDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++Requested;
        ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvPDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++Requested;
        ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvBSGroupStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++Requested;
        if (Tablets) {
            ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvTabletStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
            ++Requested;
        }
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        if (Tablets) {
            THolder<TEvTxUserProxy::TEvNavigate> request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            if (!Event->Get()->UserToken.empty()) {
                request->Record.SetUserToken(Event->Get()->UserToken);
            }
            TIntrusivePtr<TDomainsInfo> domains = AppData(ctx)->DomainsInfo;
            TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;
            TString domainPath = "/" + domain->Name;
            NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
            record->SetPath(domainPath);
            record->MutableOptions()->SetReturnPartitioningInfo(false);
            record->MutableOptions()->SetReturnPartitionConfig(false);
            record->MutableOptions()->SetReturnChildren(false);
            TActorId txproxy = MakeTxProxyID();
            ctx.Send(txproxy, request.Release());
            ++Requested;
        }

        NodesInfo = ev->Release();
        for (const auto& ni : NodesInfo->Nodes) {
            SendRequest(ni.NodeId, ctx);
        }
        if (Requested > 0) {
            TBase::Become(&TThis::StateRequestedNodeInfo);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->SourceType) {
        case TEvWhiteboard::EvSystemStateRequest:
            if (VDiskInfo.emplace(nodeId, nullptr).second) {
                RequestDone(ctx);
            }
            break;
        case TEvWhiteboard::EvVDiskStateRequest:
            if (VDiskInfo.emplace(nodeId, nullptr).second) {
                RequestDone(ctx);
            }
            break;
        case TEvWhiteboard::EvPDiskStateRequest:
            if (PDiskInfo.emplace(nodeId, nullptr).second) {
                RequestDone(ctx);
            }
            break;
        case TEvWhiteboard::EvBSGroupStateRequest:
            if (BSGroupInfo.emplace(nodeId, nullptr).second) {
                RequestDone(ctx);
            }
            break;
        case TEvWhiteboard::EvTabletStateRequest:
            if (TabletInfo.emplace(nodeId, nullptr).second) {
                RequestDone(ctx);
            }
            break;
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev->Get()->NodeId;
        if (SystemInfo.emplace(nodeId, nullptr).second) {
            RequestDone(ctx);
        }
        if (VDiskInfo.emplace(nodeId, nullptr).second) {
            RequestDone(ctx);
        }
        if (PDiskInfo.emplace(nodeId, nullptr).second) {
            RequestDone(ctx);
        }
        if (BSGroupInfo.emplace(nodeId, nullptr).second) {
            RequestDone(ctx);
        }
        if (Tablets) {
            if (TabletInfo.emplace(nodeId, nullptr).second) {
                RequestDone(ctx);
            }
        }
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        SystemInfo[nodeId] = ev->Release();
        NodesAlive.insert(nodeId);
        RequestDone(ctx);
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        VDiskInfo[nodeId] = ev->Release();
        NodesAlive.insert(nodeId);
        RequestDone(ctx);
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        PDiskInfo[nodeId] = ev->Release();
        NodesAlive.insert(nodeId);
        RequestDone(ctx);
    }

    void Handle(TEvWhiteboard::TEvBSGroupStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        BSGroupInfo[nodeId] = ev->Release();
        NodesAlive.insert(nodeId);
        RequestDone(ctx);
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        TabletInfo[nodeId] = ev->Release();
        NodesAlive.insert(nodeId);
        RequestDone(ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext &ctx) {
        if (ev->Get()->GetRecord().GetStatus() == NKikimrScheme::StatusSuccess) {
            DescribeResult = ev->Release();
        }
        RequestDone(ctx);
    }

    void RequestDone(const TActorContext& ctx) {
        ++Received;
        if (Received == Requested) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            RequestDone(ctx);
        }
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateRequestedNodeInfo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            HFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            HFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            HFunc(TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            HFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    THolder<TEvWhiteboard::TEvBSGroupStateResponse> MergedBSGroupInfo;
    THolder<TEvWhiteboard::TEvVDiskStateResponse> MergedVDiskInfo;
    THolder<TEvWhiteboard::TEvPDiskStateResponse> MergedPDiskInfo;
    THolder<TEvWhiteboard::TEvTabletStateResponse> MergedTabletInfo;
    TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&> VDisksIndex;
    TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&> PDisksIndex;

    void ReplyAndDie(const TActorContext& ctx) {
        TStringStream json;
        MergedBSGroupInfo = MergeWhiteboardResponses(BSGroupInfo, TWhiteboardInfo<TEvWhiteboard::TEvBSGroupStateResponse>::GetDefaultMergeField());
        MergedVDiskInfo = MergeWhiteboardResponses(VDiskInfo, TWhiteboardInfo<TEvWhiteboard::TEvVDiskStateResponse>::GetDefaultMergeField());
        MergedPDiskInfo = MergeWhiteboardResponses(PDiskInfo, TWhiteboardInfo<TEvWhiteboard::TEvPDiskStateResponse>::GetDefaultMergeField());

        THashSet<TTabletId> tablets;

        if (Tablets) {
            MergedTabletInfo = MergeWhiteboardResponses(TabletInfo, TWhiteboardInfo<TEvWhiteboard::TEvTabletStateResponse>::GetDefaultMergeField());
            TIntrusivePtr<TDomainsInfo> domains = AppData(ctx)->DomainsInfo;
            TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;
            ui32 hiveDomain = domains->GetHiveDomainUid(domain->DefaultHiveUid);
            ui64 defaultStateStorageGroup = domains->GetDefaultStateStorageGroup(hiveDomain);
            tablets.emplace(MakeBSControllerID(defaultStateStorageGroup));
            tablets.emplace(MakeConsoleID(defaultStateStorageGroup));
            tablets.emplace(domain->SchemeRoot);
            tablets.emplace(domains->GetHive(domain->DefaultHiveUid));
            for (TTabletId id : domain->Coordinators) {
                tablets.emplace(id);
            }
            for (TTabletId id : domain->Mediators) {
                tablets.emplace(id);
            }
            for (TTabletId id : domain->TxAllocators) {
                tablets.emplace(id);
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

        for (auto& element : *TWhiteboardInfo<TEvWhiteboard::TEvPDiskStateResponse>::GetElementsField(MergedPDiskInfo.Get())) {
            if (element.HasTotalSize() && element.HasAvailableSize()) {
                totalStorageSize += element.GetTotalSize();
                availableStorageSize += element.GetAvailableSize();
            }
            element.SetStateFlag(GetWhiteboardFlag(GetPDiskStateFlag(element)));
            element.SetOverall(GetWhiteboardFlag(GetPDiskOverallFlag(element)));
            PDisksIndex.emplace(TWhiteboardInfo<TEvWhiteboard::TEvPDiskStateResponse>::GetElementKey(element), element);
        }
        for (auto& element : *TWhiteboardInfo<TEvWhiteboard::TEvVDiskStateResponse>::GetElementsField(MergedVDiskInfo.Get())) {
            element.SetOverall(GetWhiteboardFlag(GetVDiskOverallFlag(element)));
            VDisksIndex.emplace(TWhiteboardInfo<TEvWhiteboard::TEvVDiskStateResponse>::GetElementKey(element), element);
        }
        NKikimrViewer::EFlag flag = NKikimrViewer::Grey;
        for (const auto& element : *TWhiteboardInfo<TEvWhiteboard::TEvBSGroupStateResponse>::GetElementsField(MergedBSGroupInfo.Get())) {
            flag = Max(flag, GetBSGroupOverallFlag(element, VDisksIndex, PDisksIndex));
        }
        ui32 numberOfCpus = 0;
        double loadAverage = 0;
        THashSet<TString> dataCenters;
        THashSet<TString> versions;
        THashSet<TString> hosts;
        THashMap<TString, int> names;
        for (const auto& pr : SystemInfo) {
            if (pr.second != nullptr) {
                const NKikimrWhiteboard::TSystemStateInfo& systemState = pr.second->Record.GetSystemStateInfo(0);
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
            std::unordered_set<std::pair<ui64, ui64>> tenants; /// group by tenantid (TDomainKey)
            for (const NKikimrWhiteboard::TTabletStateInfo& tabletInfo : MergedTabletInfo->Record.GetTabletStateInfo()) {
                if (tablets.contains(tabletInfo.GetTabletId())) {
                    NKikimrWhiteboard::TTabletStateInfo* tablet = pbCluster.AddSystemTablets();
                    tablet->CopyFrom(tabletInfo);
                    auto tabletFlag = GetWhiteboardFlag(GetFlagFromTabletState(tablet->GetState()));
                    tablet->SetOverall(tabletFlag);
                    flag = Max(flag, GetViewerFlag(tabletFlag));
                }
                std::pair<ui64, ui64> tenantId = {0, 0};
                if (tabletInfo.HasTenantId()) {
                    tenantId = {tabletInfo.GetTenantId().GetSchemeShard(), tabletInfo.GetTenantId().GetPathId()};
                }
                tenants.emplace(tenantId);
            }
            pbCluster.SetTablets(MergedTabletInfo->Record.TabletStateInfoSize());
            pbCluster.SetTenants(tenants.size());
        }

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
        TProtoToJson::ProtoToJson(json, pbCluster, JsonSettings);
        ctx.Send(Initiator, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext& ctx) {
        ReplyAndDie(ctx);
    }
};

template <>
struct TJsonRequestSchema<TJsonCluster> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrViewer::TClusterInfo>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonCluster> {
    static TString GetParameters() {
        return R"___([{"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"tablets","in":"query","description":"return system tablets state","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonCluster> {
    static TString GetSummary() {
        return "\"Cluster information\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonCluster> {
    static TString GetDescription() {
        return "\"Returns information about cluster\"";
    }
};

}
}
