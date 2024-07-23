#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"
#include "viewer_helper.h"
#include "json_pipe_req.h"
#include "wb_aggregate.h"
#include "wb_merge.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonCompute : public TViewerPipeClient {
    using TThis = TJsonCompute;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    THashMap<TString, NKikimrViewer::TTenant> TenantByPath;
    THashMap<TPathId, NKikimrViewer::TTenant> TenantBySubDomainKey;
    THashMap<TPathId, TTabletId> HiveBySubDomainKey;
    THashMap<TString, TPathId> SubDomainKeyByPath;
    THashMap<TString, THolder<NSchemeCache::TSchemeCacheNavigate>> NavigateResult;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveDomainStats>> HiveDomainStats;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStats;
    THashMap<TNodeId, TVector<const NKikimrWhiteboard::TTabletStateInfo*>> TabletInfoIndex;
    THashMap<TNodeId, const NKikimrHive::THiveNodeStats*> HiveNodeStatsIndex;
    THashMap<TNodeId, TString> TenantPathByNodeId;
    NMon::TEvHttpInfo::TPtr Event;
    TVector<TNodeId> NodeIds;
    THashSet<TNodeId> PassedNodeIds;
    THashSet<TNodeId> FoundNodeIds;
    THashMap<TNodeId, NKikimrWhiteboard::TEvSystemStateResponse> NodeSysInfo;
    TMap<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse> NodeTabletInfo;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString User;
    TString Path;
    TString DomainPath;
    TPathId FilterSubDomain;
    bool Tablets = true;
    TTabletId RootHiveId = 0;
    bool RootHiveRequested = false;
    NKikimrViewer::TComputeInfo Result;
    ui32 UptimeSecondsFilter = 0;
    bool ProblemNodesFilter = false;
    TString TextFilter;

    enum class EVersion {
        v1,
        v2 // only this works with sorting
    };
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
    EVersion Version = EVersion::v1;
    std::optional<ui32> Offset;
    std::optional<ui32> Limit;
    ESort Sort = ESort::NodeId;
    bool ReverseSort = false;
    bool IsNodesListSorted = false;

public:
    TJsonCompute(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    TString GetDomainId(TPathId pathId) {
        return TStringBuilder() << pathId.OwnerId << '-' << pathId.LocalPathId;
    }

    bool IsFitsToPath(const TString& path) const {
        if (Path.empty()) {
            return true;
        }
        if (Path == path) {
            return true;
        }
        if (Path == DomainPath) {
            return false;
        }
        if (Path.StartsWith(path)) {
            return true;
        }
        return false;
    }

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Tablets = FromStringWithDefault<bool>(params.Get("tablets"), Tablets);
        Path = params.Get("path");
        UptimeSecondsFilter = FromStringWithDefault<ui32>(params.Get("uptime"), 0);
        ProblemNodesFilter = FromStringWithDefault<bool>(params.Get("problems_only"), ProblemNodesFilter);
        TextFilter = params.Get("filter");
        if (params.Has("offset")) {
            Offset = FromStringWithDefault<ui32>(params.Get("offset"), 0);
        }
        if (params.Has("limit")) {
            Limit = FromStringWithDefault<ui32>(params.Get("limit"), std::numeric_limits<ui32>::max());
        }
        TString version = params.Get("version");
        if (version == "v1") {
            Version = EVersion::v1;
        } else if (version == "v2") {
            Version = EVersion::v2;
        }
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

        SendRequest(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();

        RequestConsoleListTenants();

        DomainPath = "/" + domain->Name;
        if (Path.empty() || DomainPath == Path) {
            NKikimrViewer::TTenant& tenant = TenantByPath[DomainPath];
            tenant.SetName(DomainPath);
            tenant.SetState(Ydb::Cms::GetDatabaseStatusResult::RUNNING);
            tenant.SetType(NKikimrViewer::Domain);
            RequestSchemeCacheNavigate(DomainPath);
        }
        RootHiveId = domains->GetHive();
        if (Requests == 0) {
            ReplyAndPassAway();
        }

        Become(&TThis::StateRequested, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        for (const TNodeId nodeId : NodeIds) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
        }
        TBase::PassAway();
    }

    STATEFN(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvHive::TEvResponseHiveDomainStats, Handle);
            hFunc(TEvHive::TEvResponseHiveNodeStats, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev) {
        NodesInfo = ev->Release();
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            if (IsFitsToPath(path)) {
                TString p(Path.empty() ? path : Path);
                TenantByPath[p];
                RequestSchemeCacheNavigate(p);
            }
        }
        RequestDone();
    }

    void Handle(TEvHive::TEvResponseHiveDomainStats::TPtr& ev) {
        for (const NKikimrHive::THiveDomainStats& hiveStat : ev->Get()->Record.GetDomainStats()) {
            TPathId subDomainKey({hiveStat.GetShardId(), hiveStat.GetPathId()});
            if (FilterSubDomain && FilterSubDomain != subDomainKey) {
                continue;
            }
            NKikimrViewer::TTenant& tenant = TenantBySubDomainKey[subDomainKey];
            if (ev->Cookie != HiveBySubDomainKey[subDomainKey]) {
                continue; // we avoid overwrite of tenant stats by root stats
            }
            tenant.SetId(GetDomainId({hiveStat.GetShardId(), hiveStat.GetPathId()}));
            tenant.MutableStateStats()->CopyFrom(hiveStat.GetStateStats());
            tenant.MutableMetrics()->CopyFrom(hiveStat.GetMetrics());
            tenant.MutableNodeIds()->CopyFrom(hiveStat.GetNodeIds());
            tenant.SetAliveNodes(hiveStat.GetAliveNodes());
        }
        HiveDomainStats[ev->Cookie] = std::move(ev->Release());
        RequestDone();
    }

    bool IsPageNode(TNodeId nodeId) {
        if (PassedNodeIds.insert(nodeId).second) {
            if (Offset.has_value()) {
                if (PassedNodeIds.size() <= Offset.value()) {
                    return false;
                }
            }
            if (Limit.has_value()) {
                if (NodeIds.size() >= Limit.value()) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    bool IsRequiredNode(TNodeId nodeId) {
        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        return nodeId > dynamicNameserviceConfig->MaxStaticNodeId && (!IsNodesListSorted || IsPageNode(nodeId));
    }

    bool NeedNodesSorting() {
        return Version == EVersion::v2;
    }

    bool IsNodeFilter() {
        return ProblemNodesFilter || UptimeSecondsFilter > 0 && TextFilter;
    }

    void Handle(TEvHive::TEvResponseHiveNodeStats::TPtr& ev) {
        BLOG_TRACE("ProcessNodeIds()");

        auto nodeStats = ev->Get()->Record.GetNodeStats();
        if (NeedNodesSorting() && Sort == ESort::NodeId && !IsNodeFilter()) {
            SortCollection(nodeStats, [](const NKikimrHive::THiveNodeStats& node) { return node.GetNodeId();}, ReverseSort);
            IsNodesListSorted = true;
        }
        for (const NKikimrHive::THiveNodeStats& nodeStat : nodeStats) {
            auto nodeId = nodeStat.GetNodeId();
            if (IsRequiredNode(nodeId)) {
                const auto& nodeDomain = nodeStat.GetNodeDomain();
                const TPathId subDomain(nodeDomain.GetSchemeShard(), nodeDomain.GetPathId());
                if (FilterSubDomain && FilterSubDomain != subDomain) {
                    continue;
                }
                NodeIds.emplace_back(nodeId); // order is important
                TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
                THolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest> request = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
                SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
                if (Tablets && !ev->Get()->Record.GetExtendedTabletInfo()) {
                    THolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest> request = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest>();
                    SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
                }
            }
        }
        HiveNodeStats[ev->Cookie] = std::move(ev->Release());
        RequestDone();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ResultSet.size() == 1 && ev->Get()->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            const NSchemeCache::TSchemeCacheNavigate::TEntry& result(ev->Get()->Request->ResultSet.front());
            TPathId pathId;
            if (!Path.empty() && result.Self) {
                switch (result.Self->Info.GetPathType()) {
                    case NKikimrSchemeOp::EPathTypeSubDomain:
                    case NKikimrSchemeOp::EPathTypeExtSubDomain:
                        pathId = TPathId();
                        break;
                    default:
                        pathId = TPathId(result.Self->Info.GetSchemeshardId(), result.Self->Info.GetPathId());
                        break;
                }
            }
            auto domainInfo = result.DomainInfo;
            ui64 hiveId = domainInfo->Params.GetHive();
            if (hiveId == 0) {
                if (!RootHiveRequested) {
                    hiveId = RootHiveId;
                    RootHiveRequested = true;
                }
            }
            if (hiveId) {
                RequestHiveDomainStats(hiveId);
                RequestHiveNodeStats(hiveId, pathId);
                HiveBySubDomainKey[domainInfo->DomainKey] = hiveId;
            }
            if (domainInfo->ResourcesDomainKey != domainInfo->DomainKey) {
                TenantBySubDomainKey[domainInfo->ResourcesDomainKey].SetType(NKikimrViewer::Shared);
                TenantBySubDomainKey[domainInfo->DomainKey].SetType(NKikimrViewer::Serverless);
                TenantBySubDomainKey[domainInfo->DomainKey].SetResourceId(GetDomainId(domainInfo->ResourcesDomainKey));
            }

            TString path = CanonizePath(result.Path);
            SubDomainKeyByPath[path] = domainInfo->DomainKey;
            NavigateResult[path] = std::move(ev->Get()->Request);
            if (IsFitsToPath(path)) {
                FilterSubDomain = domainInfo->DomainKey;
            }
        }
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        NodeSysInfo[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        NodeTabletInfo[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvSystemStateRequest) {
            if (NodeSysInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvTabletStateRequest) {
            if (NodeTabletInfo.emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                RequestDone();
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
    }

    bool CheckNodeFilters(TNodeId nodeId) {
        auto itSysInfo = NodeSysInfo.find(nodeId);
        if (itSysInfo != NodeSysInfo.end()) {
            if (itSysInfo->second.SystemStateInfoSize() == 1) {
                const NKikimrWhiteboard::TSystemStateInfo& sysInfo = itSysInfo->second.GetSystemStateInfo(0);
                if (UptimeSecondsFilter > 0 && sysInfo.HasStartTime() && sysInfo.HasChangeTime()
                        && sysInfo.GetChangeTime() - sysInfo.GetStartTime() > UptimeSecondsFilter * 1000) {
                    return false;
                }
                if (ProblemNodesFilter && sysInfo.HasSystemState()
                        && GetViewerFlag(sysInfo.GetSystemState()) == NKikimrViewer::EFlag::Green) {
                    return false;
                }
                if (TextFilter) {
                    if (sysInfo.HasHost() && sysInfo.GetHost().Contains(TextFilter)) {
                        return true;
                    }
                    if (std::to_string(nodeId).contains(TextFilter)) {
                        return true;
                    }
                    return false;
                }
            }
        }
        return true;
    }

    static double GetLoadAverage(const NKikimrViewer::TComputeNodeInfo& nodeInfo) {
        if (nodeInfo.LoadAverageSize() > 0 && nodeInfo.GetNumberOfCpus() > 0) {
            return nodeInfo.GetLoadAverage(0) * 100 / nodeInfo.GetNumberOfCpus();
        }
        return 0;
    }

    static double GetCPU(const NKikimrViewer::TComputeNodeInfo& nodeInfo) {
        double cpu = 0;
        if (nodeInfo.PoolStatsSize() > 0) {
            for (const auto& ps : nodeInfo.GetPoolStats()) {
                cpu = std::max(cpu, ps.GetUsage());
            }
        }
        return cpu;
    }

    void PaginateNodes(::google::protobuf::RepeatedPtrField<NKikimrViewer::TComputeNodeInfo>& nodes) {
        switch (Sort) {
            case ESort::NodeId:
                // already sorted
                break;
            case ESort::Host:
                SortCollection(nodes, [](const NKikimrViewer::TComputeNodeInfo& node) { return node.GetHost();}, ReverseSort);
                break;
            case ESort::DC:
                SortCollection(nodes, [](const NKikimrViewer::TComputeNodeInfo& node) { return node.GetDataCenter();}, ReverseSort);
                break;
            case ESort::Rack:
                SortCollection(nodes, [](const NKikimrViewer::TComputeNodeInfo& node) { return node.GetRack();}, ReverseSort);
                break;
            case ESort::Version:
                SortCollection(nodes, [](const NKikimrViewer::TComputeNodeInfo& node) { return node.GetVersion();}, ReverseSort);
                break;
            case ESort::Uptime:
                SortCollection(nodes, [](const NKikimrViewer::TComputeNodeInfo& node) { return node.GetStartTime();}, ReverseSort);
                break;
            case ESort::Memory:
                SortCollection(nodes, [](const NKikimrViewer::TComputeNodeInfo& node) { return node.GetMemoryUsed();}, ReverseSort);
                break;
            case ESort::CPU:
                SortCollection(nodes, [](const NKikimrViewer::TComputeNodeInfo& node) { return GetCPU(node);}, ReverseSort);
                break;
            case ESort::LoadAverage:
                SortCollection(nodes, [](const NKikimrViewer::TComputeNodeInfo& node) { return GetLoadAverage(node);}, ReverseSort);
                break;
        }

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

    void FillResponseNode(const TNodeId nodeId, const TString& path) {
        if (!CheckNodeFilters(nodeId))
            return;
        FoundNodeIds.insert(nodeId);
        NKikimrViewer::TComputeNodeInfo& computeNodeInfo = Version == EVersion::v1
            ? *Result.MutableTenants(Result.TenantsSize() - 1)->AddNodes()
            : *Result.AddNodes();
        if (Version == EVersion::v2) {
            computeNodeInfo.SetTenant(path);
        }
        computeNodeInfo.SetNodeId(nodeId);
        auto itSysInfo = NodeSysInfo.find(nodeId);
        if (itSysInfo != NodeSysInfo.end()) {
            if (itSysInfo->second.SystemStateInfoSize() == 1) {
                const NKikimrWhiteboard::TSystemStateInfo& sysInfo = itSysInfo->second.GetSystemStateInfo(0);
                if (sysInfo.HasStartTime()) {
                    computeNodeInfo.SetStartTime(sysInfo.GetStartTime());
                }
                if (sysInfo.HasChangeTime()) {
                    computeNodeInfo.SetChangeTime(sysInfo.GetChangeTime());
                }
                computeNodeInfo.MutableSystemLocation()->MergeFrom(sysInfo.GetSystemLocation());
                computeNodeInfo.MutableLoadAverage()->MergeFrom(sysInfo.GetLoadAverage());
                if (sysInfo.HasNumberOfCpus()) {
                    computeNodeInfo.SetNumberOfCpus(sysInfo.GetNumberOfCpus());
                }
                // TODO(xenoxeno)
                if (sysInfo.HasSystemState()) {
                    computeNodeInfo.SetOverall(GetViewerFlag(sysInfo.GetSystemState()));
                }
                if (sysInfo.HasNodeId()) {
                    computeNodeInfo.SetNodeId(sysInfo.GetNodeId());
                }
                if (sysInfo.HasDataCenter()) {
                    computeNodeInfo.SetDataCenter(sysInfo.GetDataCenter());
                }
                if (sysInfo.HasRack()) {
                    computeNodeInfo.SetRack(sysInfo.GetRack());
                }
                if (sysInfo.HasHost()) {
                    computeNodeInfo.SetHost(sysInfo.GetHost());
                }
                if (sysInfo.HasVersion()) {
                    computeNodeInfo.SetVersion(sysInfo.GetVersion());
                }
                if (sysInfo.HasMemoryUsed()) {
                    computeNodeInfo.SetMemoryUsed(sysInfo.GetMemoryUsed());
                }
                if (sysInfo.HasMemoryLimit()) {
                    computeNodeInfo.SetMemoryLimit(sysInfo.GetMemoryLimit());
                }
                computeNodeInfo.MutablePoolStats()->MergeFrom(sysInfo.GetPoolStats());
                computeNodeInfo.MutableEndpoints()->MergeFrom(sysInfo.GetEndpoints());
                computeNodeInfo.MutableRoles()->MergeFrom(sysInfo.GetRoles());

            }
        }
        auto itTabletInfo = TabletInfoIndex.find(nodeId);
        if (itTabletInfo != TabletInfoIndex.end()) {
            THashMap<std::pair<NKikimrTabletBase::TTabletTypes::EType, NKikimrViewer::EFlag>, NKikimrViewer::TTabletStateInfo> tablets;
            for (const auto* pTabletInfo : itTabletInfo->second) {
                const auto& tabletInfo = *pTabletInfo;
                if (tabletInfo.GetState() != NKikimrWhiteboard::TTabletStateInfo::Deleted) {
                    NKikimrViewer::EFlag state = GetFlagFromTabletState(tabletInfo.GetState());
                    auto& tablet = tablets[std::make_pair(tabletInfo.GetType(), state)];
                    tablet.SetCount(tablet.GetCount() + 1);
                }
            }
            for (const auto& [prTypeState, tabletInfo] : tablets) {
                NKikimrViewer::TTabletStateInfo& tablet = *computeNodeInfo.AddTablets();
                tablet.MergeFrom(tabletInfo);
                tablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(prTypeState.first));
                tablet.SetState(prTypeState.second);
            }
        }
        auto itHiveNodeStats = HiveNodeStatsIndex.find(nodeId);
        if (itHiveNodeStats != HiveNodeStatsIndex.end()) {
            computeNodeInfo.MutableMetrics()->CopyFrom(itHiveNodeStats->second->GetMetrics());
            for (const auto& state : itHiveNodeStats->second->GetStateStats()) {
                if (state.HasTabletType()) {
                    NKikimrViewer::TTabletStateInfo& tablet = *computeNodeInfo.AddTablets();
                    tablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(state.GetTabletType()));
                    tablet.SetCount(state.GetCount());
                    NKikimrViewer::EFlag flag = GetFlagFromTabletState(state.GetVolatileState());
                    tablet.SetState(flag);
                }
            }
        }
    }

    void ReplyAndPassAway() override {
        NKikimrWhiteboard::TEvTabletStateResponse tabletInfo;
        MergeWhiteboardResponses(tabletInfo, NodeTabletInfo);
        for (const auto& info : tabletInfo.GetTabletStateInfo()) {
            TabletInfoIndex[info.GetNodeId()].emplace_back(&info);
        }
        auto itRootHiveNodeStats = HiveNodeStats.find(RootHiveId);
        if (itRootHiveNodeStats != HiveNodeStats.end()) {
            for (const auto& stats : itRootHiveNodeStats->second->Record.GetNodeStats()) {
                HiveNodeStatsIndex[stats.GetNodeId()] = &stats;
            }
        }
        for (const auto& prStats : HiveNodeStats) {
            if (prStats.first != RootHiveId) {
                for (const auto& stats : prStats.second->Record.GetNodeStats()) {
                    HiveNodeStatsIndex[stats.GetNodeId()] = &stats;
                }
            }
        }

            for (const std::pair<const TString, NKikimrViewer::TTenant>& prTenant : TenantByPath) {
                const TString& path = prTenant.first;
                if (Version == EVersion::v1) {
                    NKikimrViewer::TComputeTenantInfo& computeTenantInfo = *Result.AddTenants();
                    computeTenantInfo.SetName(path);
                    // TODO(xenoxeno)
                    computeTenantInfo.SetOverall(NKikimrViewer::EFlag::Green);
                }
                auto itSubDomainKey = SubDomainKeyByPath.find(path);
                if (itSubDomainKey != SubDomainKeyByPath.end()) {
                    TPathId subDomainKey(itSubDomainKey->second);
                    const NKikimrViewer::TTenant& tenantBySubDomainKey(TenantBySubDomainKey[subDomainKey]);
                    for (TNodeId nodeId : tenantBySubDomainKey.GetNodeIds()) {
                        if (IsNodesListSorted) {
                            TenantPathByNodeId[nodeId] = path;
                        } else {
                            FillResponseNode(nodeId, path);
                        }
                    }
                }
            }

        if (IsNodesListSorted) {
            for (TNodeId nodeId : NodeIds) {
                FillResponseNode(nodeId, TenantPathByNodeId[nodeId]);
            }
        } else if (NeedNodesSorting()) {
            PaginateNodes(*Result.MutableNodes());
        }

        Result.SetTotalNodes(NodeIds.size());
        Result.SetFoundNodes(FoundNodeIds.size());
        // TODO(xenoxeno)
        Result.SetOverall(NKikimrViewer::EFlag::Green);

        TStringStream json;
        TProtoToJson::ProtoToJson(json, Result, JsonSettings);
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Result.AddErrors("Timeout occurred");
        ReplyAndPassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonCompute> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TNetInfo>();
    }
};

template <>
struct TJsonRequestParameters<TJsonCompute> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: version
              in: query
              description: query version (v1, v2)
              required: false
              type: string
            - name: path
              in: query
              description: schema path
              required: false
              type: string
            - name: enums
              in: query
              description: convert enums to strings
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
            - name: uptime
              in: query
              description: return only nodes with less uptime in sec.
              required: false
              type: integer
            - name: problems_only
              in: query
              description: return only problem nodes
              required: false
              type: boolean
            - name: filter
              in: query
              description: filter nodes by id or host
              required: false
              type: string
            - name: sort
              in: query
              description: sort by (NodeId,Host,DC,Rack,Version,Uptime,Memory,CPU,LoadAverage)
              required: false
              type: string
            - name: offset
              in: query
              description: skip N nodes
              required: false
              type: integer
            - name: limit
              in: query
              description: limit to N nodes
              required: false
              type: integer
        )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonCompute> {
    static TString GetSummary() {
        return "Database compute information";
    }
};

template <>
struct TJsonRequestDescription<TJsonCompute> {
    static TString GetDescription() {
        return "Returns information about compute layer of database";
    }
};

}
}
