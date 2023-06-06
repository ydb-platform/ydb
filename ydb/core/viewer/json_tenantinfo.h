#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/protos/services.pb.h>
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
#include "log.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonTenantInfo : public TViewerPipeClient<TJsonTenantInfo> {
    using TBase = TViewerPipeClient<TJsonTenantInfo>;
    IViewer* Viewer;
    THashMap<TString, NKikimrViewer::TTenant> TenantByPath;
    THashMap<TPathId, NKikimrViewer::TTenant> TenantBySubDomainKey;
    THashMap<TString, THolder<NSchemeCache::TSchemeCacheNavigate>> NavigateResult;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveDomainStats>> HiveDomainStats;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveStorageStats>> HiveStorageStats;
    NMon::TEvHttpInfo::TPtr Event;
    THashSet<TNodeId> NodeIds;
    THashMap<TNodeId, TString> NodeIdsToTenant; // for tablet info
    TMap<TNodeId, NKikimrWhiteboard::TEvSystemStateResponse> NodeSysInfo;
    THashMap<TString, TMap<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse>> TenantNodeTabletInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString User;
    TString Path;
    bool Tablets = false;
    bool SystemTablets = false;
    bool Storage = false;
    bool Nodes = false;
    bool OffloadMerge = false;
    THashMap<TString, size_t> TenantOffloadMergeNodes;
    THashMap<TString, std::vector<TNodeId>> TenantNodes;
    THashMap<TString, NKikimrViewer::TEvViewerResponse> TenantMergedTabletInfo;
    TTabletId RootHiveId = 0;
    TString RootId; // id of root domain (tenant)
    NKikimrViewer::TTenantInfo Result;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonTenantInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    TString GetLogPrefix() {
        static TString prefix = "json/tenantinfo ";
        return prefix;
    }

    TString GetDomainId(TPathId pathId) {
        return TStringBuilder() << pathId.OwnerId << '-' << pathId.LocalPathId;
    }

    void Bootstrap() {
        BLOG_TRACE("Bootstrap()");
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Followers = false;
        Metrics = true;
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Tablets = FromStringWithDefault<bool>(params.Get("tablets"), Tablets);
        SystemTablets = FromStringWithDefault<bool>(params.Get("system_tablets"), Tablets); // Tablets here is by design
        Storage = FromStringWithDefault<bool>(params.Get("storage"), Storage);
        Nodes = FromStringWithDefault<bool>(params.Get("nodes"), Nodes);
        User = params.Get("user");
        Path = params.Get("path");
        OffloadMerge = FromStringWithDefault<bool>(params.Get("offload_merge"), OffloadMerge);

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;

        RequestConsoleListTenants();

        TString domainPath = "/" + domain->Name;
        if (Path.empty() || domainPath == Path) {
            TPathId subDomainKey(domain->SchemeRoot, 1);
            NKikimrViewer::TTenant& tenant = TenantBySubDomainKey[subDomainKey];
            tenant.SetId(GetDomainId(subDomainKey));
            tenant.SetState(Ydb::Cms::GetDatabaseStatusResult::RUNNING);
            tenant.SetType(NKikimrViewer::Domain);
            RequestSchemeCacheNavigate(domainPath);
        }
        RootId = GetDomainId({domain->SchemeRoot, 1});
        RootHiveId = domains->GetHive(domain->DefaultHiveUid);
        RequestHiveDomainStats(RootHiveId);
        if (Storage) {
            RequestHiveStorageStats(RootHiveId);
        }

        if (Requests == 0) {
            ReplyAndPassAway();
        }

        Become(&TThis::StateRequested, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        for (const TNodeId nodeId : NodeIds) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        for (const auto& [nodeId, tenantId] : NodeIdsToTenant) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
        BLOG_TRACE("PassAway()");
    }

    STATEFN(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvGetTenantStatusResponse, Handle);
            hFunc(TEvHive::TEvResponseHiveDomainStats, Handle);
            hFunc(TEvHive::TEvResponseHiveStorageStats, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(TEvViewer::TEvViewerResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        BLOG_TRACE("Received ListTenantsResponse");
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            if (!Path.empty() && path != Path) {
                continue;
            }
            RequestConsoleGetTenantStatus(path);
            RequestSchemeCacheNavigate(path);
        }
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr& ev) {
        BLOG_TRACE("Received GetTenantStatusResponse");
        Ydb::Cms::GetDatabaseStatusResult getTenantStatusResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&getTenantStatusResult);
        TString path = getTenantStatusResult.path();
        NKikimrViewer::TTenant& tenant = TenantByPath[path];
        tenant.SetName(path);
        tenant.SetState(getTenantStatusResult.state());
        if (getTenantStatusResult.has_required_shared_resources()) {
            tenant.SetType(NKikimrViewer::Shared);
            RequestSchemeCacheNavigate(path);
        }
        for (const Ydb::Cms::StorageUnits& unit : getTenantStatusResult.allocated_resources().storage_units()) {
            NKikimrViewer::TTenantResource& resource = *tenant.MutableResources()->AddAllocated();
            resource.SetType("storage");
            resource.SetKind(unit.unit_kind());
            resource.SetCount(unit.count());
        }
        for (const Ydb::Cms::StorageUnits& unit : getTenantStatusResult.required_resources().storage_units()) {
            NKikimrViewer::TTenantResource& resource = *tenant.MutableResources()->AddRequired();
            resource.SetType("storage");
            resource.SetKind(unit.unit_kind());
            resource.SetCount(unit.count());
        }
        for (const Ydb::Cms::ComputationalUnits& unit : getTenantStatusResult.allocated_resources().computational_units()) {
            NKikimrViewer::TTenantResource& resource = *tenant.MutableResources()->AddAllocated();
            resource.SetType("compute");
            resource.SetZone(unit.availability_zone());
            resource.SetKind(unit.unit_kind());
            resource.SetCount(unit.count());
        }
        for (const Ydb::Cms::ComputationalUnits& unit : getTenantStatusResult.required_resources().computational_units()) {
            NKikimrViewer::TTenantResource& resource = *tenant.MutableResources()->AddRequired();
            resource.SetType("compute");
            resource.SetZone(unit.availability_zone());
            resource.SetKind(unit.unit_kind());
            resource.SetCount(unit.count());
        }

        RequestDone();
    }

    static TNodeId SelectTargetNode(const std::vector<TNodeId>& nodesIds, size_t offset = 0) {
        std::vector<TNodeId> nodes(nodesIds);
        auto itPos = std::next(nodes.begin(), offset % nodes.size());
        std::nth_element(nodes.begin(), itPos, nodes.end());
        return *itPos;
    }

    void Handle(TEvHive::TEvResponseHiveDomainStats::TPtr& ev) {
        for (const NKikimrHive::THiveDomainStats& hiveStat : ev->Get()->Record.GetDomainStats()) {
            TPathId subDomainKey({hiveStat.GetShardId(), hiveStat.GetPathId()});
            NKikimrViewer::TTenant& tenant = TenantBySubDomainKey[subDomainKey];
            auto tenantId = GetDomainId({hiveStat.GetShardId(), hiveStat.GetPathId()});
            tenant.SetId(tenantId);
            if (ev->Cookie != RootHiveId || tenant.GetId() == RootId) {
                if (!tenant.HasMetrics()) {
                    tenant.MutableMetrics()->CopyFrom(hiveStat.GetMetrics());
                }
                if (tenant.StateStatsSize() == 0) {
                    tenant.MutableStateStats()->CopyFrom(hiveStat.GetStateStats());
                }
                if (tenant.NodeIdsSize() == 0) {
                    tenant.MutableNodeIds()->CopyFrom(hiveStat.GetNodeIds());
                }
                if (tenant.GetAliveNodes() == 0) {
                    tenant.SetAliveNodes(hiveStat.GetAliveNodes());
                }
            }

            BLOG_TRACE("Received HiveDomainStats for " << tenant.GetId() << " from " << ev->Cookie);

            for (TNodeId nodeId : hiveStat.GetNodeIds()) {
                TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
                if (NodeIds.insert(nodeId).second) {
                    THolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest> request = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
                    SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
                }
                if (Tablets && !OffloadMerge && NodeIdsToTenant.insert({nodeId, tenantId}).second) {
                    THolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest> request = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest>();
                    request->Record.SetFormat("packed5");
                    BLOG_TRACE("Tenant " << tenant.GetId() << " send to " << nodeId << " TEvTabletStateRequest: " << request->Record.ShortDebugString());
                    SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
                }
            }
            if (Tablets && OffloadMerge && hiveStat.NodeIdsSize() > 0 && TenantMergedTabletInfo.emplace(tenant.GetId(), NKikimrViewer::TEvViewerResponse{}).second) {
                THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
                std::vector<TNodeId> nodesIds;
                nodesIds.reserve(hiveStat.NodeIdsSize());
                for (auto nodeId : hiveStat.GetNodeIds()) {
                    nodesIds.push_back(nodeId);
                    NodeIdsToTenant.insert({nodeId, tenantId});
                    request->Record.MutableLocation()->AddNodeId(nodeId);
                }
                TenantNodes[tenant.GetId()] = nodesIds;
                TNodeId nodeId = SelectTargetNode(nodesIds, TenantOffloadMergeNodes[tenant.GetId()]++);
                TActorId viewerServiceId = MakeViewerID(nodeId);
                request->Record.MutableTabletRequest()->SetFormat("packed5");
                request->Record.SetTimeout(Timeout / 3);
                BLOG_TRACE("Tenant " << tenant.GetId() << " send to " << nodeId << " TEvViewerRequest: " << request->Record.ShortDebugString());
                SendRequest(viewerServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
            }
        }
        HiveDomainStats[ev->Cookie] = std::move(ev->Release());
        RequestDone();
    }

    void Handle(TEvHive::TEvResponseHiveStorageStats::TPtr& ev) {
        BLOG_TRACE("Received HiveStorageStats from " << ev->Cookie);
        HiveStorageStats[ev->Cookie] = std::move(ev->Release());
        RequestDone();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ResultSet.size() == 1 && ev->Get()->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            auto domainInfo = ev->Get()->Request->ResultSet.begin()->DomainInfo;
            TTabletId hiveId = domainInfo->Params.GetHive();
            if (hiveId) {
                RequestHiveDomainStats(hiveId);
                if (Storage) {
                    RequestHiveStorageStats(hiveId);
                }
            }
            NKikimrViewer::TTenant& tenant = TenantBySubDomainKey[domainInfo->DomainKey];
            if (domainInfo->ResourcesDomainKey != domainInfo->DomainKey) {
                NKikimrViewer::TTenant& sharedTenant = TenantBySubDomainKey[domainInfo->ResourcesDomainKey];
                if (sharedTenant.GetType() != NKikimrViewer::Shared) {
                    sharedTenant.SetType(NKikimrViewer::Shared);
                    RequestSchemeCacheNavigate(domainInfo->ResourcesDomainKey);
                }
                tenant.SetType(NKikimrViewer::Serverless);
                tenant.SetResourceId(GetDomainId(domainInfo->ResourcesDomainKey));
            }
            TString id = GetDomainId(domainInfo->DomainKey);
            TString path = CanonizePath(ev->Get()->Request->ResultSet.begin()->Path);
            BLOG_TRACE("Received Navigate for " << id << " " << path);
            tenant.SetId(id);
            tenant.SetName(path);
            if (tenant.GetType() == NKikimrViewer::UnknownTenantType) {
                tenant.SetType(NKikimrViewer::Dedicated);
            }
            NavigateResult[id] = std::move(ev->Get()->Request);
        }
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        BLOG_TRACE("Received TEvSystemStateResponse from " << nodeId);
        NodeSysInfo[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        BLOG_TRACE("Received TEvTabletStateResponse from " << nodeId << " with "
            << TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse>::GetElementsCount(ev->Get()->Record) << " tablets");
        auto tenantId = NodeIdsToTenant[nodeId];
        TenantNodeTabletInfo[tenantId][nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        BLOG_TRACE("Received TEvViewerResponse from " << nodeId << " with "
            << TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse>::GetElementsCount(ev->Get()->Record.GetTabletResponse()) << " tablets");
        auto tenantId = NodeIdsToTenant[nodeId];
        TenantMergedTabletInfo[tenantId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        ui32 nodeId = ev.Get()->Cookie;
        BLOG_TRACE("Undelivered for node " << nodeId << " event " << ev->Get()->SourceType);
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvSystemStateRequest) {
            if (NodeSysInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvTabletStateRequest) {
            auto tenantId = NodeIdsToTenant[nodeId];
            if (TenantNodeTabletInfo[tenantId].emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NViewer::TEvViewer::EvViewerRequest) {
            THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
            auto tenantId = NodeIdsToTenant[nodeId];
            const std::vector<TNodeId>& nodesIds = TenantNodes[tenantId];
            TNodeId nodeId = SelectTargetNode(nodesIds, TenantOffloadMergeNodes[tenantId]++);
            TActorId viewerServiceId = MakeViewerID(nodeId);
            request->Record.MutableTabletRequest()->SetFormat("packed5");
            request->Record.SetTimeout(Timeout / 3);
            BLOG_TRACE("Tenant " << tenantId << " send to " << nodeId << " TEvViewerRequest: " << request->Record.ShortDebugString());
            SendRequest(viewerServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        ui32 nodeId = ev->Get()->NodeId;
        BLOG_TRACE("NodeDisconnected for node " << nodeId);
        if (NodeSysInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
            RequestDone();
        }
        auto tenantId = NodeIdsToTenant[nodeId];
        if (TenantNodeTabletInfo[tenantId].emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
            RequestDone();
        }
        if (!TenantNodes[tenantId].empty()) {
            THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
            const std::vector<TNodeId>& nodesIds = TenantNodes[tenantId];
            TNodeId nodeId = SelectTargetNode(nodesIds, TenantOffloadMergeNodes[tenantId]++);
            TActorId viewerServiceId = MakeViewerID(nodeId);
            request->Record.MutableTabletRequest()->SetFormat("packed5");
            request->Record.SetTimeout(Timeout / 3);
            BLOG_TRACE("Tenant " << tenantId << " send to " << nodeId << " TEvViewerRequest: " << request->Record.ShortDebugString());
            SendRequest(viewerServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        }
    }

    void ReplyAndPassAway() {
        BLOG_TRACE("ReplyAndPassAway() started");
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;
        for (const auto& [subDomainKey, tenantBySubDomainKey] : TenantBySubDomainKey) {
            TString id(GetDomainId(subDomainKey));
            NKikimrWhiteboard::TEvTabletStateResponse tabletInfo;
            THashMap<TTabletId, const NKikimrWhiteboard::TTabletStateInfo*> tabletInfoIndex;
            if (Tablets) {
                if (OffloadMerge) {
                    tabletInfo = std::move(*(TenantMergedTabletInfo[id].MutableTabletResponse()));
                } else {
                    TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse>::MergeResponses(tabletInfo, TenantNodeTabletInfo[id]);
                }
                if (SystemTablets) {
                    for (const auto& info : TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse>::GetElementsField(tabletInfo)) {
                        tabletInfoIndex[info.GetTabletId()] = &info;
                    }
                }
            }

            NKikimrViewer::EFlag overall = NKikimrViewer::EFlag::Grey;
            auto itNavigate = NavigateResult.find(id);
            if (itNavigate != NavigateResult.end()) {
                NSchemeCache::TSchemeCacheNavigate::TEntry entry = itNavigate->second->ResultSet.front();
                TString path = CanonizePath(entry.Path);
                if (Path && Path != path) {
                    continue;
                }
                std::unordered_set<TString> users;
                if (entry.SecurityObject) {
                    users.emplace(entry.SecurityObject->GetOwnerSID());
                    for (const NACLibProto::TACE& ace : entry.SecurityObject->GetACL().GetACE()) {
                        if (ace.GetAccessType() == (ui32)NACLib::EAccessType::Allow) {
                            users.emplace(ace.GetSID());
                        }
                    }
                }
                if (!User.empty() && users.count(User) == 0) {
                    continue;
                }
                NKikimrViewer::TTenant& tenant = *Result.AddTenantInfo();
                auto itTenantByPath = TenantByPath.find(path);
                if (itTenantByPath != TenantByPath.end()) {
                    tenant = std::move(itTenantByPath->second);
                    TenantByPath.erase(itTenantByPath);
                }
                if (tenant.GetType() == NKikimrViewer::UnknownTenantType) {
                    tenant.MergeFrom(tenantBySubDomainKey);
                } else {
                    auto oldType = tenant.GetType();
                    tenant.MergeFrom(tenantBySubDomainKey);
                    tenant.SetType(oldType);
                }
                if (!tenant.GetId()) {
                    tenant.SetId(GetDomainId(subDomainKey));
                }
                if (tenant.GetType() == NKikimrViewer::UnknownTenantType) {
                    tenant.SetType(NKikimrViewer::Dedicated);
                }
                tenant.SetCreateTime(TInstant::MicroSeconds(entry.CreateStep).MilliSeconds());
                if (entry.SecurityObject) {
                    tenant.SetOwner(entry.SecurityObject->GetOwnerSID());
                }
                for (const TString& user : users) {
                    tenant.AddUsers(user);
                }
                for (const auto& userAttribute : entry.Attributes) {
                    tenant.MutableUserAttributes()->insert({userAttribute.first, userAttribute.second});
                }

                TStackVec<TTabletId, 64> tablets;
                for (TTabletId tabletId : entry.DomainInfo->Params.GetCoordinators()) {
                    tablets.emplace_back(tabletId);
                }
                for (TTabletId tabletId : entry.DomainInfo->Params.GetMediators()) {
                    tablets.emplace_back(tabletId);
                }
                if (entry.DomainInfo->Params.HasSchemeShard()) {
                    tablets.emplace_back(entry.DomainInfo->Params.GetSchemeShard());
                } else {
                    tablets.emplace_back(domain->SchemeRoot);

                    ui32 hiveDomain = domains->GetHiveDomainUid(domain->DefaultHiveUid);
                    ui64 defaultStateStorageGroup = domains->GetDefaultStateStorageGroup(hiveDomain);
                    tablets.emplace_back(MakeBSControllerID(defaultStateStorageGroup));
                    tablets.emplace_back(MakeConsoleID(defaultStateStorageGroup));
                }
                TTabletId hiveId = domains->GetHive(domain->DefaultHiveUid);
                if (entry.DomainInfo->Params.HasHive()) {
                    hiveId = entry.DomainInfo->Params.GetHive();
                } else {
                    if (tenant.GetType() == NKikimrViewer::Serverless) {
                        auto itResourceNavigate = NavigateResult.find(tenant.GetResourceId());
                        if (itResourceNavigate != NavigateResult.end()) {
                            NSchemeCache::TSchemeCacheNavigate::TEntry entry = itResourceNavigate->second->ResultSet.front();
                            if (entry.DomainInfo->Params.HasHive()) {
                                hiveId = entry.DomainInfo->Params.GetHive();
                            }
                        }
                    }
                }
                tablets.emplace_back(hiveId);

                if (SystemTablets) {
                    for (TTabletId tabletId : tablets) {
                        auto it = tabletInfoIndex.find(tabletId);
                        if (it != tabletInfoIndex.end()) {
                            NKikimrWhiteboard::TTabletStateInfo* tabletInfo = tenant.AddSystemTablets();
                            tabletInfo->CopyFrom(*it->second);
                            NKikimrViewer::EFlag flag = GetFlagFromTabletState(tabletInfo->GetState());
                            tabletInfo->SetOverall(GetWhiteboardFlag(flag));
                            overall = Max(overall, flag);
                        }
                    }
                }

                if (Storage) {
                    auto itHiveStorageStats = HiveStorageStats.find(hiveId);
                    if (itHiveStorageStats != HiveStorageStats.end()) {
                        const NKikimrHive::TEvResponseHiveStorageStats& record = itHiveStorageStats->second.Get()->Record;
                        uint64 storageAllocatedSize = 0;
                        uint64 storageMinAvailableSize = std::numeric_limits<ui64>::max();
                        uint64 storageGroups = 0;
                        for (const NKikimrHive::THiveStoragePoolStats& poolStat : record.GetPools()) {
                            if (poolStat.GetName().StartsWith(tenantBySubDomainKey.GetName())) {
                                for (const NKikimrHive::THiveStorageGroupStats& groupStat : poolStat.GetGroups()) {
                                    storageAllocatedSize += groupStat.GetAllocatedSize();
                                    storageMinAvailableSize = std::min(storageMinAvailableSize, groupStat.GetAvailableSize());
                                    ++storageGroups;
                                }
                            }
                        }
                        tenant.SetStorageAllocatedSize(storageAllocatedSize);
                        tenant.SetStorageMinAvailableSize(storageMinAvailableSize);
                        tenant.SetStorageGroups(storageGroups);
                    }
                }

                THashSet<TNodeId> tenantNodes;

                for (TNodeId nodeId : tenant.GetNodeIds()) {
                    auto itNodeInfo = NodeSysInfo.find(nodeId);
                    if (itNodeInfo != NodeSysInfo.end()) {
                        if (itNodeInfo->second.SystemStateInfoSize() == 1) {
                            const auto& nodeInfo = itNodeInfo->second.GetSystemStateInfo(0);
                            if (Nodes) {
                                tenant.AddNodes()->CopyFrom(nodeInfo);
                            }
                            for (const auto& poolStat : nodeInfo.GetPoolStats()) {
                                TString poolName = poolStat.GetName();
                                NKikimrWhiteboard::TSystemStateInfo_TPoolStats* targetPoolStat = nullptr;
                                for (NKikimrWhiteboard::TSystemStateInfo_TPoolStats& ps : *tenant.MutablePoolStats()) {
                                    if (ps.GetName() == poolName) {
                                        targetPoolStat = &ps;
                                        break;
                                    }
                                }
                                if (targetPoolStat == nullptr) {
                                    targetPoolStat = tenant.AddPoolStats();
                                    targetPoolStat->SetName(poolName);
                                }
                                double poolUsage = targetPoolStat->GetUsage() * targetPoolStat->GetThreads();
                                poolUsage += poolStat.GetUsage() * poolStat.GetThreads();
                                ui32 poolThreads = targetPoolStat->GetThreads() + poolStat.GetThreads();
                                if (poolThreads != 0) {
                                    double threadUsage = poolUsage / poolThreads;
                                    targetPoolStat->SetUsage(threadUsage);
                                    targetPoolStat->SetThreads(poolThreads);
                                }
                                tenant.SetCoresUsed(tenant.GetCoresUsed() + poolStat.GetUsage() * poolStat.GetThreads());
                            }
                            if (nodeInfo.HasMemoryUsed()) {
                                tenant.SetMemoryUsed(tenant.GetMemoryUsed() + nodeInfo.GetMemoryUsed());
                            }
                            if (nodeInfo.HasMemoryLimit()) {
                                tenant.SetMemoryLimit(tenant.GetMemoryLimit() + nodeInfo.GetMemoryLimit());
                            }
                            overall = Max(overall, GetViewerFlag(nodeInfo.GetSystemState()));
                        }
                    }
                    tenantNodes.emplace(nodeId);
                }

                if (tenant.GetType() == NKikimrViewer::Serverless) {
                    tenant.SetStorageAllocatedSize(tenant.GetMetrics().GetStorage());
                    tenant.SetMemoryUsed(tenant.GetMetrics().GetMemory());
                    tenant.ClearMemoryLimit();
                    tenant.SetCoresUsed(static_cast<double>(tenant.GetMetrics().GetCPU()) / 1000000);
                }

                {
                    THashMap<std::pair<NKikimrTabletBase::TTabletTypes::EType, NKikimrViewer::EFlag>, NKikimrViewer::TTabletStateInfo> tablets;

                    if (Tablets) {
                        for (const auto& pbTablet : tabletInfo.GetTabletStateInfo()) {
                            if (tenantNodes.count(pbTablet.GetNodeId()) > 0) {
                                NKikimrViewer::EFlag state = GetFlagFromTabletState(pbTablet.GetState());
                                auto& tablet = tablets[std::make_pair(pbTablet.GetType(), state)];
                                tablet.SetCount(tablet.GetCount() + 1);
                            }
                        }
                    }
                    for (const auto& [prTypeState, prTabletInfo] : tablets) {
                        NKikimrViewer::TTabletStateInfo& tablet = *tenant.AddTablets();
                        tablet.MergeFrom(prTabletInfo);
                        tablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(prTypeState.first));
                        tablet.SetState(prTypeState.second);
                    }
                }
                tenant.SetOverall(overall);
            }
        }
        for (const std::pair<const TString, NKikimrViewer::TTenant>& prTenant : TenantByPath) {
            const TString& path(prTenant.first);
            if (Path && Path != path) {
                continue;
            }
            if (!User.empty()) {
                continue;
            }
            const NKikimrViewer::TTenant& tenantByPath(prTenant.second);
            NKikimrViewer::EFlag overall = NKikimrViewer::EFlag::Red;
            NKikimrViewer::TTenant& tenant = *Result.AddTenantInfo();
            tenant.MergeFrom(tenantByPath);
            tenant.SetName(path);
            tenant.SetOverall(overall);
        }
        std::sort(Result.MutableTenantInfo()->begin(), Result.MutableTenantInfo()->end(), 
            [](const NKikimrViewer::TTenant& a, const NKikimrViewer::TTenant& b) {
                return a.name() < b.name();
            });
        TStringStream json;
        TProtoToJson::ProtoToJson(json, Result, JsonSettings);
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        BLOG_TRACE("Timeout occurred");
        Result.AddErrors("Timeout occurred");
        ReplyAndPassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonTenantInfo> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NKikimrViewer::TTenantInfo>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonTenantInfo> {
    static TString GetParameters() {
        return R"___([{"name":"path","in":"query","description":"schema path","required":false,"type":"string"},
                      {"name":"followers","in":"query","description":"return followers","required":false,"type":"boolean"},
                      {"name":"metrics","in":"query","description":"return tablet metrics","required":false,"type":"boolean"},
                      {"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"tablets","in":"query","description":"return system tablets","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonTenantInfo> {
    static TString GetSummary() {
        return "\"Tenant info (detailed)\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonTenantInfo> {
    static TString GetDescription() {
        return "\"Returns information about tenants\"";
    }
};

}
}
