#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
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
#include "log.h"
#include "viewer_request.h"

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
    THashSet<TNodeId> SubscribedNodeIds;
    THashMap<TNodeId, TString> NodeIdsToTenant; // for tablet info
    TMap<TNodeId, NKikimrWhiteboard::TEvSystemStateResponse> NodeSysInfo;
    THashMap<TString, TMap<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse>> TenantNodeTabletInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString User;
    TString Path;
    TString DomainPath;
    bool Tablets = false;
    bool SystemTablets = false;
    bool Storage = false;
    bool Nodes = false;
    bool OffloadMerge = false;
    THashMap<TString, size_t> TenantOffloadMergeTablets;
    THashMap<TString, size_t> TenantOffloadNodesInfo;
    THashMap<TString, std::vector<TNodeId>> TenantNodes;
    THashMap<TString, NKikimrViewer::TEvViewerResponse> TenantMergedTabletInfo;
    THashMap<TString, NKikimrViewer::TEvViewerResponse> TenantNodesSystemInfo;
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

    bool IsFilterByPath() {
        return !Path.empty() && DomainPath != Path;
    }

    bool IsValidTenant(const TString& path) {
        return !IsFilterByPath() || Path == path;
    }

    bool IsFilterByOwner() {
        return !User.empty();
    }

    bool IsValidOwner(const std::unordered_set<TString>& users) {
        return !IsFilterByOwner() || users.count(User) != 0;
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

        DomainPath = "/" + domain->Name;
        if (!IsFilterByPath()) {
            TPathId subDomainKey(domain->SchemeRoot, 1);
            NKikimrViewer::TTenant& tenant = TenantBySubDomainKey[subDomainKey];
            tenant.SetId(GetDomainId(subDomainKey));
            tenant.SetState(Ydb::Cms::GetDatabaseStatusResult::RUNNING);
            tenant.SetType(NKikimrViewer::Domain);
            RequestSchemeCacheNavigate(DomainPath);
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
        for (const TNodeId nodeId : SubscribedNodeIds) {
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
            if (!IsValidTenant(path)) {
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
        Ydb::Cms::DatabaseQuotas& quotas = *tenant.MutableDatabaseQuotas();
        quotas.MergeFrom(getTenantStatusResult.database_quotas());

        RequestDone();
    }

    static TNodeId SelectTargetNode(const std::vector<TNodeId>& nodesIds, size_t offset = 0) {
        std::vector<TNodeId> nodes(nodesIds);
        auto itPos = std::next(nodes.begin(), offset % nodes.size());
        std::nth_element(nodes.begin(), itPos, nodes.end());
        return *itPos;
    }

    void SendViewerTabletRequest(const TString& tenantId) {
        const std::vector<TNodeId>& nodesIds = TenantNodes[tenantId];
        TNodeId nodeId = SelectTargetNode(nodesIds, TenantOffloadMergeTablets[tenantId]++);
        SubscribedNodeIds.insert(nodeId);
        TActorId viewerServiceId = MakeViewerID(nodeId);

        THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
        request->Record.MutableTabletRequest()->SetFormat("packed5");
        request->Record.SetTimeout(Timeout / 3);
        for (auto nodeId : nodesIds) {
            request->Record.MutableLocation()->AddNodeId(nodeId);
        }
        BLOG_TRACE("Tenant " << tenantId << " send to " << nodeId << " TEvViewerRequest: " << request->Record.ShortDebugString());
        ViewerWhiteboardCookie cookie(NKikimrViewer::TEvViewerRequest::kTabletRequest, nodeId);
        SendRequest(viewerServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie.ToUi64());
    }

    void SendViewerSystemRequest(const TString& tenantId) {
        const std::vector<TNodeId>& nodesIds = TenantNodes[tenantId];
        TNodeId nodeId = SelectTargetNode(nodesIds, TenantOffloadNodesInfo[tenantId]++);
        SubscribedNodeIds.insert(nodeId);
        TActorId viewerServiceId = MakeViewerID(nodeId);

        THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
        request->Record.MutableSystemRequest();
        request->Record.SetTimeout(Timeout / 3);
        for (auto nodeId : nodesIds) {
            request->Record.MutableLocation()->AddNodeId(nodeId);
        }
        BLOG_TRACE("Tenant " << tenantId << " send to " << nodeId << " TEvViewerRequest: " << request->Record.ShortDebugString());
        ViewerWhiteboardCookie cookie (NKikimrViewer::TEvViewerRequest::kSystemRequest, nodeId);
        SendRequest(viewerServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie.ToUi64());
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

            if (!OffloadMerge) {
                for (TNodeId nodeId : hiveStat.GetNodeIds()) {
                    TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
                    if (SubscribedNodeIds.insert(nodeId).second) {
                        THolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest> request = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
                        SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
                    }
                    if (Tablets && NodeIdsToTenant.insert({nodeId, tenantId}).second) {
                        THolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest> request = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest>();
                        request->Record.SetFormat("packed5");
                        BLOG_TRACE("Tenant " << tenant.GetId() << " send to " << nodeId << " TEvTabletStateRequest: " << request->Record.ShortDebugString());
                        SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
                    }
                }
            } else if (hiveStat.NodeIdsSize() > 0) {
                std::vector<TNodeId> nodesIds;
                nodesIds.reserve(hiveStat.NodeIdsSize());
                for (auto nodeId : hiveStat.GetNodeIds()) {
                    nodesIds.push_back(nodeId);
                    NodeIdsToTenant.insert({nodeId, tenantId});
                }
                TenantNodes[tenantId] = nodesIds;

                if (TenantNodesSystemInfo.emplace(tenantId, NKikimrViewer::TEvViewerResponse{}).second) {
                    SendViewerSystemRequest(tenantId);
                }
                if (Tablets && TenantMergedTabletInfo.emplace(tenantId, NKikimrViewer::TEvViewerResponse{}).second) {
                    SendViewerTabletRequest(tenantId);
                }
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
        auto tenantId = NodeIdsToTenant[nodeId];
        switch (ev->Get()->Record.GetResponseCase()) {
            case NKikimrViewer::TEvViewerResponse::kTabletResponse:
                BLOG_TRACE("Received TEvViewerResponse from " << nodeId << " with "
                    << TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse>::GetElementsCount(ev->Get()->Record.GetTabletResponse())
                    << " tablets");
                TenantMergedTabletInfo[tenantId] = std::move(ev->Get()->Record);
                RequestDone();
                break;
            case NKikimrViewer::TEvViewerResponse::kSystemResponse:
                BLOG_TRACE("Received TEvViewerResponse from " << nodeId);
                TenantNodesSystemInfo[tenantId] = std::move(ev->Get()->Record);
                RequestDone();
                break;
            case NKikimrViewer::TEvViewerResponse::kQueryResponse:
            case NKikimrViewer::TEvViewerResponse::kReserved14:
            case NKikimrViewer::TEvViewerResponse::kReserved15:
            case NKikimrViewer::TEvViewerResponse::kReserved16:
            case NKikimrViewer::TEvViewerResponse::RESPONSE_NOT_SET:
                break;
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvSystemStateRequest) {
            ui32 nodeId = ev.Get()->Cookie;
            BLOG_TRACE("Undelivered for node " << nodeId << " event " << ev->Get()->SourceType);
            if (NodeSysInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvTabletStateRequest) {
            ui32 nodeId = ev.Get()->Cookie;
            BLOG_TRACE("Undelivered for node " << nodeId << " event " << ev->Get()->SourceType);
            auto tenantId = NodeIdsToTenant[nodeId];
            if (TenantNodeTabletInfo[tenantId].emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NViewer::TEvViewer::EvViewerRequest) {
            ViewerWhiteboardCookie cookie(ev.Get()->Cookie);
            BLOG_TRACE("Undelivered for node " << cookie.GetNodeId() << " event " << ev->Get()->SourceType);
            auto tenantId = NodeIdsToTenant[cookie.GetNodeId()];
            switch (cookie.GetRequestCase()) {
                case NKikimrViewer::TEvViewerRequest::kTabletRequest:
                    SendViewerTabletRequest(tenantId);
                    break;
                case NKikimrViewer::TEvViewerRequest::kSystemRequest:
                    SendViewerSystemRequest(tenantId);
                    break;
                case NKikimrViewer::TEvViewerRequest::kQueryRequest:
                case NKikimrViewer::TEvViewerRequest::kReserved14:
                case NKikimrViewer::TEvViewerRequest::kReserved15:
                case NKikimrViewer::TEvViewerRequest::kReserved16:
                case NKikimrViewer::TEvViewerRequest::REQUEST_NOT_SET:
                    break;
            }
            RequestDone();
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        TString& tenantId = NodeIdsToTenant[nodeId];
        BLOG_TRACE("NodeDisconnected for node " << nodeId);
        if (!OffloadMerge) {
            if (NodeSysInfo.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
                RequestDone();
            }
            if (Tablets && TenantNodeTabletInfo[tenantId].emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                RequestDone();
            }
        } else if (!TenantNodes[tenantId].empty()) {
            if (Tablets) {
                SendViewerTabletRequest(tenantId);
                RequestDone();
            }
            SendViewerSystemRequest(tenantId);
            RequestDone();
        }
    }

    void ReplyAndPassAway() {
        BLOG_TRACE("ReplyAndPassAway() started");
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;
        THashMap<TString, NKikimrViewer::EFlag> OverallByDomainId;
        TMap<TNodeId, NKikimrWhiteboard::TSystemStateInfo> NodeSystemStateInfo;

        if (OffloadMerge) {
            for (auto& [tenantId, record] : TenantNodesSystemInfo) {
                for (auto& systemState : *(record.MutableSystemResponse()->MutableSystemStateInfo())) {
                    auto ni = systemState.GetNodeId();
                    NodeSystemStateInfo[ni] = std::move(systemState);
                }
            }
        } else {
            for (auto& [nodeId, record] : NodeSysInfo) {
                if (record.SystemStateInfoSize() == 1) {
                    NodeSystemStateInfo[nodeId] = std::move(record.GetSystemStateInfo(0));
                }
            }
        }

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
                if (!IsValidTenant(path)) {
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
                if (!IsValidOwner(users)) {
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
                        uint64 storageAvailableSize = 0;
                        uint64 storageMinAvailableSize = std::numeric_limits<ui64>::max();
                        uint64 storageGroups = 0;
                        for (const NKikimrHive::THiveStoragePoolStats& poolStat : record.GetPools()) {
                            if (poolStat.GetName().StartsWith(tenantBySubDomainKey.GetName())) {
                                for (const NKikimrHive::THiveStorageGroupStats& groupStat : poolStat.GetGroups()) {
                                    storageAllocatedSize += groupStat.GetAllocatedSize();
                                    storageAvailableSize += groupStat.GetAvailableSize();
                                    storageMinAvailableSize = std::min(storageMinAvailableSize, groupStat.GetAvailableSize());
                                    ++storageGroups;
                                }
                            }
                        }
                        uint64 storageAllocatedLimit = storageAllocatedSize + storageAvailableSize;
                        tenant.SetStorageAllocatedSize(storageAllocatedSize);
                        tenant.SetStorageAllocatedLimit(storageAllocatedLimit);
                        tenant.SetStorageMinAvailableSize(storageMinAvailableSize);
                        tenant.SetStorageGroups(storageGroups);
                        auto& ssdUsage = *tenant.AddStorageUsage();
                        ssdUsage.SetType(NKikimrViewer::TStorageUsage::SSD);
                        if (entry.DomainDescription
                            && entry.DomainDescription->Description.HasDatabaseQuotas()
                            && entry.DomainDescription->Description.HasDiskSpaceUsage()) {
                            ssdUsage.SetSize(entry.DomainDescription->Description.GetDiskSpaceUsage().GetTables().GetTotalSize());
                            ssdUsage.SetLimit(entry.DomainDescription->Description.GetDatabaseQuotas().data_size_hard_quota());
                        } else {
                            ssdUsage.SetSize(storageAllocatedSize);
                            ssdUsage.SetLimit(storageAllocatedLimit);
                        }
                        //// TODO(andrew-rykov)
                        //auto& hddUsage = *tenant.AddStorageUsage();
                        //hddUsage.SetType(NKikimrViewer::TStorageUsage::HDD);
                    }
                }

                THashSet<TNodeId> tenantNodes;

                for (TNodeId nodeId : tenant.GetNodeIds()) {
                    auto itNodeInfo = NodeSystemStateInfo.find(nodeId);
                    if (itNodeInfo != NodeSystemStateInfo.end()) {
                        if (Nodes) {
                            tenant.AddNodes()->CopyFrom(itNodeInfo->second);
                        }
                        for (const auto& poolStat : itNodeInfo->second.GetPoolStats()) {
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
                        if (itNodeInfo->second.HasMemoryUsed()) {
                            tenant.SetMemoryUsed(tenant.GetMemoryUsed() + itNodeInfo->second.GetMemoryUsed());
                        }
                        if (itNodeInfo->second.HasMemoryLimit()) {
                            tenant.SetMemoryLimit(tenant.GetMemoryLimit() + itNodeInfo->second.GetMemoryLimit());
                        }
                        overall = Max(overall, GetViewerFlag(itNodeInfo->second.GetSystemState()));
                    }
                    tenantNodes.emplace(nodeId);
                }

                if (tenant.GetType() == NKikimrViewer::Serverless) {
                    tenant.SetStorageAllocatedSize(tenant.GetMetrics().GetStorage());
                    const bool noExclusiveNodes = tenantNodes.empty();
                    if (noExclusiveNodes) {
                        tenant.SetMemoryUsed(tenant.GetMetrics().GetMemory());
                        tenant.ClearMemoryLimit();
                        tenant.SetCoresUsed(static_cast<double>(tenant.GetMetrics().GetCPU()) / 1000000);
                    }
                }

                if (Tablets) {
                    THashMap<std::pair<NKikimrTabletBase::TTabletTypes::EType, NKikimrViewer::EFlag>, ui32> tablets;
                    for (const auto& pbTablet : tabletInfo.GetTabletStateInfo()) {
                        if (tenantNodes.count(pbTablet.GetNodeId()) > 0) {
                            NKikimrViewer::EFlag state = GetFlagFromTabletState(pbTablet.GetState());
                            tablets[std::make_pair(pbTablet.GetType(), state)]++;
                        }
                    }

                    for (const auto& [prTypeState, prTabletCount] : tablets) {
                        NKikimrViewer::TTabletStateInfo& tablet = *tenant.AddTablets();
                        tablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(prTypeState.first));
                        tablet.SetState(prTypeState.second);
                        tablet.SetCount(prTabletCount);
                    }
                }
                tenant.SetOverall(overall);
                OverallByDomainId[tenant.GetId()] = overall;
            }
        }
        for (const std::pair<const TString, NKikimrViewer::TTenant>& prTenant : TenantByPath) {
            const TString& path(prTenant.first);
            if (!IsValidTenant(path)) {
                continue;
            }
            if (IsFilterByOwner()) {
                continue;
            }
            const NKikimrViewer::TTenant& tenantByPath(prTenant.second);
            NKikimrViewer::EFlag overall = NKikimrViewer::EFlag::Red;
            NKikimrViewer::TTenant& tenant = *Result.AddTenantInfo();
            tenant.MergeFrom(tenantByPath);
            tenant.SetName(path);
            tenant.SetOverall(overall);
            if (tenant.GetId()) {
                OverallByDomainId[tenant.GetId()] = overall;
            }
        }
        for (NKikimrViewer::TTenant& tenant: *Result.MutableTenantInfo()) {
            if (tenant.GetType() != NKikimrViewer::Serverless) {
                continue;
            }
            auto it = OverallByDomainId.find(tenant.GetResourceId());
            if (it != OverallByDomainId.end()) {
                tenant.SetOverall(it->second);
            }
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
                      {"name":"user","in":"query","description":"tenant owner","required":false,"type":"string"},
                      {"name":"followers","in":"query","description":"return followers","required":false,"type":"boolean"},
                      {"name":"metrics","in":"query","description":"return tablet metrics","required":false,"type":"boolean"},
                      {"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"tablets","in":"query","description":"return tablets","required":false,"type":"boolean"},
                      {"name":"system_tablets","in":"query","description":"return system tablets","required":false,"type":"boolean"},
                      {"name":"offload_merge","in":"query","description":"use offload merge","required":false,"type":"boolean"},
                      {"name":"storage","in":"query","description":"return storage info","required":false,"type":"boolean"},
                      {"name":"nodes","in":"query","description":"return nodes info","required":false,"type":"boolean"},
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
