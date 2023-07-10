#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/mon.h>
#include <library/cpp/actors/core/interconnect.h>
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
#include "json_pipe_req.h"
#include "wb_aggregate.h"
#include "wb_merge.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonCompute : public TViewerPipeClient<TJsonCompute> {
    using TBase = TViewerPipeClient<TJsonCompute>;
    IViewer* Viewer;
    THashMap<TString, NKikimrViewer::TTenant> TenantByPath;
    THashMap<TPathId, NKikimrViewer::TTenant> TenantBySubDomainKey;
    THashMap<TPathId, TTabletId> HiveBySubDomainKey;
    THashMap<TString, TPathId> SubDomainKeyByPath;
    THashMap<TString, THolder<NSchemeCache::TSchemeCacheNavigate>> NavigateResult;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveDomainStats>> HiveDomainStats;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStats;
    NMon::TEvHttpInfo::TPtr Event;
    THashSet<TNodeId> NodeIds;
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
    ui32 UptimeSeconds = 0;
    bool ProblemNodesOnly = false;
    TString Filter;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

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

    void Bootstrap(const TActorContext& ) {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Tablets = FromStringWithDefault<bool>(params.Get("tablets"), Tablets);
        Path = params.Get("path");
        UptimeSeconds = FromStringWithDefault<ui32>(params.Get("uptime"), 0);
        ProblemNodesOnly = FromStringWithDefault<bool>(params.Get("problems_only"), ProblemNodesOnly);
        Filter = params.Get("filter");

        SendRequest(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;

        RequestConsoleListTenants();

        DomainPath = "/" + domain->Name;
        if (Path.empty() || DomainPath == Path) {
            NKikimrViewer::TTenant& tenant = TenantByPath[DomainPath];
            tenant.SetName(DomainPath);
            tenant.SetState(Ydb::Cms::GetDatabaseStatusResult::RUNNING);
            tenant.SetType(NKikimrViewer::Domain);
            RequestSchemeCacheNavigate(DomainPath);
        }
        RootHiveId = domains->GetHive(domain->DefaultHiveUid);
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

    void Handle(TEvHive::TEvResponseHiveNodeStats::TPtr& ev) {
        for (const NKikimrHive::THiveNodeStats& nodeStat : ev->Get()->Record.GetNodeStats()) {
            auto nodeId = nodeStat.GetNodeId();
            if (NodeIds.insert(nodeId).second) {
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
            ui64 pathId = 0;
            if (!Path.empty() && result.Self) {
                switch (result.Self->Info.GetPathType()) {
                    case NKikimrSchemeOp::EPathTypeSubDomain:
                    case NKikimrSchemeOp::EPathTypeExtSubDomain:
                        pathId = 0;
                        break;
                    default:
                        pathId = result.Self->Info.GetPathId();
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
                if (UptimeSeconds > 0 && sysInfo.HasStartTime() && sysInfo.HasChangeTime()
                        && sysInfo.GetChangeTime() - sysInfo.GetStartTime() > UptimeSeconds * 1000) {
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
        THashMap<TNodeId, TVector<const NKikimrWhiteboard::TTabletStateInfo*>> tabletInfoIndex;
        NKikimrWhiteboard::TEvTabletStateResponse tabletInfo;
        MergeWhiteboardResponses(tabletInfo, NodeTabletInfo);
        for (const auto& info : tabletInfo.GetTabletStateInfo()) {
            tabletInfoIndex[info.GetNodeId()].emplace_back(&info);
        }
        THashMap<TNodeId, const NKikimrHive::THiveNodeStats*> hiveNodeStatsIndex;
        auto itRootHiveNodeStats = HiveNodeStats.find(RootHiveId);
        if (itRootHiveNodeStats != HiveNodeStats.end()) {
            for (const auto& stats : itRootHiveNodeStats->second->Record.GetNodeStats()) {
                hiveNodeStatsIndex[stats.GetNodeId()] = &stats;
            }
        }
        for (const auto& prStats : HiveNodeStats) {
            if (prStats.first != RootHiveId) {
                for (const auto& stats : prStats.second->Record.GetNodeStats()) {
                    hiveNodeStatsIndex[stats.GetNodeId()] = &stats;
                }
            }
        }
        for (const std::pair<const TString, NKikimrViewer::TTenant>& prTenant : TenantByPath) {
            const TString& path = prTenant.first;
            NKikimrViewer::TComputeTenantInfo& computeTenantInfo = *Result.AddTenants();
            computeTenantInfo.SetName(path);
            auto itSubDomainKey = SubDomainKeyByPath.find(path);
            if (itSubDomainKey != SubDomainKeyByPath.end()) {
                TPathId subDomainKey(itSubDomainKey->second);
                const NKikimrViewer::TTenant& tenantBySubDomainKey(TenantBySubDomainKey[subDomainKey]);
                for (TNodeId nodeId : tenantBySubDomainKey.GetNodeIds()) {
                    if (!CheckNodeFilters(nodeId))
                        continue;
                    NKikimrViewer::TComputeNodeInfo& computeNodeInfo = *computeTenantInfo.AddNodes();
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
                    auto itTabletInfo = tabletInfoIndex.find(nodeId);
                    if (itTabletInfo != tabletInfoIndex.end()) {
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
                    auto itHiveNodeStats = hiveNodeStatsIndex.find(nodeId);
                    if (itHiveNodeStats != hiveNodeStatsIndex.end()) {
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
            }

            // TODO(xenoxeno)
            computeTenantInfo.SetOverall(NKikimrViewer::EFlag::Green);
        }

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
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrViewer::TNetInfo>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonCompute> {
    static TString GetParameters() {
        return R"___([{"name":"path","in":"query","description":"schema path","required":false,"type":"string"},
                      {"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"},
                      {"name":"uptime","in":"query","description":"return only nodes with less uptime in sec.","required":false,"type":"integer"},
                      {"name":"problems_only","in":"query","description":"return only problem nodes","required":false,"type":"boolean"},
                      {"name":"filter","in":"query","description":"filter nodes by id or host","required":false,"type":"string"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonCompute> {
    static TString GetSummary() {
        return "\"Database compute information\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonCompute> {
    static TString GetDescription() {
        return "\"Returns information about compute layer of database\"";
    }
};

}
}
