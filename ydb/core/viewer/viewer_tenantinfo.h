#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include "viewer.h"
#include "viewer_request.h"
#include "viewer_tabletinfo.h"
#include "wb_aggregate.h"
#include "wb_merge.h"

namespace NKikimr::NViewer {

using namespace NActors;

NKikimrViewer::EFlag GetViewerFlag(Ydb::Monitoring::StatusFlag::Status flag);

class TJsonTenantInfo : public TViewerPipeClient {
    using TThis = TJsonTenantInfo;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    THashMap<TString, NKikimrViewer::TTenant> TenantByPath;
    THashMap<TPathId, NKikimrViewer::TTenant> TenantBySubDomainKey;
    THashMap<TString, NKikimrViewer::EFlag> HcOverallByTenantPath;
    THashMap<TString, THolder<NSchemeCache::TSchemeCacheNavigate>> NavigateResult;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveDomainStats>> HiveDomainStats;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveStorageStats>> HiveStorageStats;
    NMon::TEvHttpInfo::TPtr Event;
    THashSet<TNodeId> Subscribers;
    THashSet<TNodeId> WhiteboardNodesRequested;
    THashSet<TString> OffloadTenantsRequested;
    THashSet<TString> MetadataCacheRequested;
    THashMap<TNodeId, TString> NodeIdsToTenant; // for tablet info
    TMap<TNodeId, NKikimrWhiteboard::TEvSystemStateResponse> WhiteboardSystemStateResponse;
    THashMap<TString, TMap<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse>> WhiteboardTabletStateResponse;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString User;
    TString Path;
    TString DomainPath;
    bool Tablets = false;
    bool SystemTablets = false;
    bool Storage = false;
    bool Nodes = false;
    bool Users = false;
    bool OffloadMerge = false;
    THashMap<TString, std::vector<TNodeId>> TenantNodes;
    THashMap<TString, NKikimrViewer::TEvViewerResponse> OffloadMergedTabletStateResponse;
    THashMap<TString, NKikimrViewer::TEvViewerResponse> OffloadMergedSystemStateResponse;
    THashMap<TString, NKikimrViewer::TStorageUsage::EType> StoragePoolType;
    TTabletId RootHiveId = 0;
    TString RootId; // id of root domain (tenant)
    NKikimrViewer::TTenantInfo Result;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse>> GetStoragePoolsResponse;

    struct TStorageQuota {
        uint64 SoftQuota = 0;
        uint64 HardQuota = 0;
    };

public:
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

    void Bootstrap() override {
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
        Users = FromStringWithDefault<bool>(params.Get("users"), Users);
        User = params.Get("user");
        Path = params.Get("path");
        OffloadMerge = FromStringWithDefault<bool>(params.Get("offload_merge"), OffloadMerge);

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();

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
        RootHiveId = domains->GetHive();
        RequestHiveDomainStats(RootHiveId);
        if (Storage) {
            RequestHiveStorageStats(RootHiveId);
        }
        GetStoragePoolsResponse = RequestBSControllerPools();

        if (Requests == 0) {
            ReplyAndPassAway();
        }

        Become(&TThis::StateRequested, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        for (const TNodeId nodeId : Subscribers) {
            if (nodeId != SelfId().NodeId()) {
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
            }
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
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(NHealthCheck::TEvSelfCheckResultProto, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, Handle);
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

            if (AppData()->FeatureFlags.GetEnableDbMetadataCache()) {
                RequestStateStorageMetadataCacheEndpointsLookup(path);
            }
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

    void SendWhiteboardSystemStateRequest(const TNodeId nodeId) {
        Subscribers.insert(nodeId);
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        THolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest> request = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>();
        BLOG_TRACE("Tenant " << NodeIdsToTenant[nodeId] << " send to " << nodeId << " TEvSystemStateRequest: " << request->Record.ShortDebugString());
        SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
    }

    void SendWhiteboardTabletStateRequest(const TNodeId nodeId) {
        Subscribers.insert(nodeId);
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        THolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest> request = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest>();
        request->Record.SetFormat("packed5");
        BLOG_TRACE("Tenant " << NodeIdsToTenant[nodeId] << " send to " << nodeId << " TEvTabletStateRequest: " << request->Record.ShortDebugString());
        SendRequest(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
    }

    void SendWhiteboardRequests(const TNodeId nodeId) {
        if (WhiteboardNodesRequested.insert(nodeId).second) {
            SendWhiteboardSystemStateRequest(nodeId);
            if (Tablets) {
                SendWhiteboardTabletStateRequest(nodeId);
            }
        }
    }

    void SendOffloadRequests(const TString& tenantId) {
        std::vector<TNodeId>& nodesIds = TenantNodes[tenantId];
        if (!nodesIds.empty() && OffloadTenantsRequested.insert(tenantId).second) {
            ui64 hash = std::hash<TString>()(Event->Get()->Request.GetRemoteAddr());
            auto itPos = std::next(nodesIds.begin(), hash % nodesIds.size());
            std::nth_element(nodesIds.begin(), itPos, nodesIds.end());
            TNodeId nodeId = *itPos;

            Subscribers.insert(nodeId);
            TActorId viewerServiceId = MakeViewerID(nodeId);

            THolder<TEvViewer::TEvViewerRequest> sysRequest = MakeHolder<TEvViewer::TEvViewerRequest>();
            sysRequest->Record.MutableSystemRequest();
            sysRequest->Record.SetTimeout(Timeout / 3);
            for (auto nodeId : nodesIds) {
                sysRequest->Record.MutableLocation()->AddNodeId(nodeId);
            }
            BLOG_TRACE("Tenant " << tenantId << " send to " << nodeId << " TEvViewerRequest: " << sysRequest->Record.ShortDebugString());
            ViewerWhiteboardCookie cookie (NKikimrViewer::TEvViewerRequest::kSystemRequest, nodeId);
            SendRequest(viewerServiceId, sysRequest.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie.ToUi64());

            if (Tablets) {
                THolder<TEvViewer::TEvViewerRequest> tblRequest = MakeHolder<TEvViewer::TEvViewerRequest>();
                tblRequest->Record.MutableTabletRequest()->SetFormat("packed5");
                tblRequest->Record.SetTimeout(Timeout / 3);
                for (auto nodeId : nodesIds) {
                    tblRequest->Record.MutableLocation()->AddNodeId(nodeId);
                }
                BLOG_TRACE("Tenant " << tenantId << " send to " << nodeId << " TEvViewerRequest: " << tblRequest->Record.ShortDebugString());
                ViewerWhiteboardCookie cookie(NKikimrViewer::TEvViewerRequest::kTabletRequest, nodeId);
                SendRequest(viewerServiceId, tblRequest.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie.ToUi64());
            }
        }
    }

    void Handle(TEvHive::TEvResponseHiveDomainStats::TPtr& ev) {
        for (const NKikimrHive::THiveDomainStats& hiveStat : ev->Get()->Record.GetDomainStats()) {
            TPathId subDomainKey({hiveStat.GetShardId(), hiveStat.GetPathId()});
            NKikimrViewer::TTenant& tenant = TenantBySubDomainKey[subDomainKey];
            TString tenantId = GetDomainId({hiveStat.GetShardId(), hiveStat.GetPathId()});
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
            std::vector<TNodeId> nodesIds;
            nodesIds.reserve(hiveStat.NodeIdsSize());
            for (auto nodeId : hiveStat.GetNodeIds()) {
                nodesIds.push_back(nodeId);
                NodeIdsToTenant.insert({nodeId, tenantId});
            }
            TenantNodes[tenantId] = nodesIds;

            if (OffloadMerge) {
                SendOffloadRequests(tenantId);
            } else {
                for (TNodeId nodeId : hiveStat.GetNodeIds()) {
                    SendWhiteboardRequests(nodeId);
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
        WhiteboardSystemStateResponse[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        BLOG_TRACE("Received TEvTabletStateResponse from " << nodeId);
        auto tenantId = NodeIdsToTenant[nodeId];
        WhiteboardTabletStateResponse[tenantId][nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(NHealthCheck::TEvSelfCheckResultProto::TPtr& ev) {
        auto result = std::move(ev->Get()->Record);
        if (result.database_status_size() == 1) {
            HcOverallByTenantPath.emplace(result.database_status(0).name(), GetViewerFlag(result.database_status(0).overall()));
        }

        RequestDone();
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        auto activeNode = TDatabaseMetadataCache::PickActiveNode(ev->Get()->InfoEntries);
        if (activeNode != 0) {
            Subscribers.insert(activeNode);
            std::optional<TActorId> cache = MakeDatabaseMetadataCacheId(activeNode);
            auto request = MakeHolder<NHealthCheck::TEvSelfCheckRequestProto>();
            if (MetadataCacheRequested.insert(ev->Get()->Path).second) {
                SendRequest(*cache, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, activeNode);
            }
        }
        RequestDone();
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        auto tenantId = NodeIdsToTenant[nodeId];
        switch (ev->Get()->Record.GetResponseCase()) {
            case NKikimrViewer::TEvViewerResponse::kTabletResponse:
                BLOG_TRACE("Received TEvViewerResponse from " << nodeId);
                OffloadMergedTabletStateResponse[tenantId] = std::move(ev->Get()->Record);
                RequestDone();
                break;
            case NKikimrViewer::TEvViewerResponse::kSystemResponse:
                BLOG_TRACE("Received TEvViewerResponse from " << nodeId);
                OffloadMergedSystemStateResponse[tenantId] = std::move(ev->Get()->Record);
                RequestDone();
                break;
            default:
                break;
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        if (ev->Get()->SourceType == NHealthCheck::EvSelfCheckRequestProto) {
            ui32 nodeId = ev.Get()->Cookie;
            BLOG_TRACE("Undelivered for node " << nodeId << " event " << ev->Get()->SourceType);
            auto tenantId = NodeIdsToTenant[nodeId];
            if (HcOverallByTenantPath.emplace(tenantId, NKikimrViewer::EFlag::Grey).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvSystemStateRequest) {
            ui32 nodeId = ev.Get()->Cookie;
            BLOG_TRACE("Undelivered for node " << nodeId << " event " << ev->Get()->SourceType);
            if (WhiteboardSystemStateResponse.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvTabletStateRequest) {
            ui32 nodeId = ev.Get()->Cookie;
            BLOG_TRACE("Undelivered for node " << nodeId << " event " << ev->Get()->SourceType);
            auto tenantId = NodeIdsToTenant[nodeId];
            if (WhiteboardTabletStateResponse[tenantId].emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NViewer::TEvViewer::EvViewerRequest) {
            ViewerWhiteboardCookie cookie(ev.Get()->Cookie);
            auto nodeId = cookie.GetNodeId();
            auto tenantId = NodeIdsToTenant[nodeId];
            BLOG_TRACE("Undelivered for node " << cookie.GetNodeId() << " event " << ev->Get()->SourceType);
            switch (cookie.GetRequestCase()) {
                case NKikimrViewer::TEvViewerRequest::kTabletRequest:
                    if (OffloadMergedTabletStateResponse.emplace(tenantId, NKikimrViewer::TEvViewerResponse{}).second) {
                        // fallback
                        for (TNodeId nodeId : TenantNodes[tenantId]) {
                            SendWhiteboardTabletStateRequest(nodeId);
                        }
                        RequestDone();
                    };

                    break;
                case NKikimrViewer::TEvViewerRequest::kSystemRequest:
                    if (OffloadMergedSystemStateResponse.emplace(tenantId, NKikimrViewer::TEvViewerResponse{}).second) {
                        // fallback
                        for (TNodeId nodeId : TenantNodes[tenantId]) {
                            SendWhiteboardSystemStateRequest(nodeId);
                        }
                        RequestDone();
                    }
                    break;
                default:
                    break;
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        auto tenantId = NodeIdsToTenant[nodeId];
        BLOG_TRACE("NodeDisconnected for nodeId " << nodeId);

        if (OffloadTenantsRequested.count(tenantId) > 0) {
            // fallback
            if (OffloadMergedSystemStateResponse.emplace(tenantId, NKikimrViewer::TEvViewerResponse{}).second) {
                for (TNodeId nodeId : TenantNodes[tenantId]) {
                    SendWhiteboardSystemStateRequest(nodeId);
                }
                RequestDone();
            }
            if (Tablets && OffloadMergedSystemStateResponse.emplace(tenantId, NKikimrViewer::TEvViewerResponse{}).second) {
                for (TNodeId nodeId : TenantNodes[tenantId]) {
                    SendWhiteboardSystemStateRequest(nodeId);
                }
                RequestDone();
            }
        }
        if (WhiteboardNodesRequested.count(nodeId) > 0) {
            if (WhiteboardSystemStateResponse.emplace(nodeId, NKikimrWhiteboard::TEvSystemStateResponse{}).second) {
                RequestDone();
            }
            if (Tablets && WhiteboardTabletStateResponse[tenantId].emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                RequestDone();
            }
        }
        if (MetadataCacheRequested.count(tenantId) > 0) {
            if (HcOverallByTenantPath.emplace(tenantId, NKikimrViewer::EFlag::Grey).second) {
                RequestDone();
            }
        }
    }

    NKikimrViewer::TStorageUsage::EType GetStorageType(const TString& poolKind) {
        auto kind = to_lower(poolKind);
        if (kind.StartsWith("ssd") || kind.StartsWith("nvme")) {
            return NKikimrViewer::TStorageUsage::SSD;
        }
        if (kind.StartsWith("hdd") || kind.StartsWith("rot")) {
            return NKikimrViewer::TStorageUsage::HDD;
        }
        return NKikimrViewer::TStorageUsage::None;
    }

    NKikimrViewer::TStorageUsage::EType GuessStorageType(const NKikimrSubDomains::TDomainDescription& domainDescription) {
        NKikimrViewer::TStorageUsage::EType type = NKikimrViewer::TStorageUsage::SSD;
        for (const auto& pool : domainDescription.GetStoragePools()) {
            auto poolType = GetStorageType(pool.GetKind());
            if (poolType != NKikimrViewer::TStorageUsage::None) {
                type = poolType;
                break;
            }
        }
        return type;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TString error = TStringBuilder() << "Failed to establish pipe: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status);
            if (ev->Get()->TabletId == GetBSControllerId()) {
                if (GetStoragePoolsResponse.has_value()) {
                    GetStoragePoolsResponse->Error(error);
                }
            }
        }
        TBase::Handle(ev); // all RequestDone() are handled by base handler
    }

    void Handle(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        GetStoragePoolsResponse->Set(std::move(ev));

        if (GetStoragePoolsResponse && GetStoragePoolsResponse->IsOk()) {
            for (const NKikimrSysView::TStoragePoolEntry& entry : GetStoragePoolsResponse->Get()->Record.GetEntries()) {
                StoragePoolType[entry.GetInfo().GetName()] = GetStorageType(entry.GetInfo().GetKind());
            }
        }

        RequestDone();
    }

    void ReplyAndPassAway() override {
        BLOG_TRACE("ReplyAndPassAway() started");
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();
        THashMap<TString, NKikimrViewer::EFlag> OverallByDomainId;
        TMap<TNodeId, NKikimrWhiteboard::TSystemStateInfo> NodeSystemStateInfo;

        for (auto& [tenantId, record] : OffloadMergedSystemStateResponse) {
            for (auto& systemState : *(record.MutableSystemResponse()->MutableSystemStateInfo())) {
                auto ni = systemState.GetNodeId();
                NodeSystemStateInfo[ni] = std::move(systemState);
            }
        }
        for (auto& [nodeId, record] : WhiteboardSystemStateResponse) {
            if (record.SystemStateInfoSize() == 1) {
                NodeSystemStateInfo[nodeId] = std::move(record.GetSystemStateInfo(0));
            }
        }

        for (const auto& [subDomainKey, tenantBySubDomainKey] : TenantBySubDomainKey) {
            TString id(GetDomainId(subDomainKey));
            NKikimrWhiteboard::TEvTabletStateResponse tabletInfo;
            THashMap<TTabletId, const NKikimrWhiteboard::TTabletStateInfo*> tabletInfoIndex;
            if (Tablets) {
                if (WhiteboardTabletStateResponse[id].size() > 0) {
                    TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse>::MergeResponses(tabletInfo, WhiteboardTabletStateResponse[id]);
                } else if (OffloadMerge) {
                    tabletInfo = std::move(*(OffloadMergedTabletStateResponse[id].MutableTabletResponse()));
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
                if(!User.empty() || Users) {
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
                    tablets.emplace_back(MakeBSControllerID());
                    tablets.emplace_back(MakeConsoleID());
                }
                TTabletId hiveId = domains->GetHive();
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
                    THashMap<NKikimrViewer::TStorageUsage::EType, ui64> databaseStorageByType;
                    auto itHiveStorageStats = HiveStorageStats.find(hiveId);
                    if (itHiveStorageStats != HiveStorageStats.end()) {
                        const NKikimrHive::TEvResponseHiveStorageStats& record = itHiveStorageStats->second.Get()->Record;
                        uint64 storageAllocatedSize = 0;
                        uint64 storageAvailableSize = 0;
                        uint64 storageMinAvailableSize = std::numeric_limits<ui64>::max();
                        uint64 storageGroups = 0;
                        for (const NKikimrHive::THiveStoragePoolStats& poolStat : record.GetPools()) {
                            if (poolStat.GetName().StartsWith(tenantBySubDomainKey.GetName())) {
                                NKikimrViewer::TStorageUsage::EType storageType = StoragePoolType[poolStat.GetName()];
                                for (const NKikimrHive::THiveStorageGroupStats& groupStat : poolStat.GetGroups()) {
                                    storageAllocatedSize += groupStat.GetAllocatedSize();
                                    storageAvailableSize += groupStat.GetAvailableSize();
                                    databaseStorageByType[storageType] += groupStat.GetAllocatedSize();
                                    databaseStorageByType[storageType] += groupStat.GetAvailableSize();
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
                    }

                    THashMap<NKikimrViewer::TStorageUsage::EType, ui64> tablesStorageByType;
                    THashMap<NKikimrViewer::TStorageUsage::EType, TStorageQuota> storageQuotasByType;

                    for (const auto& quota : tenant.GetDatabaseQuotas().storage_quotas()) {
                        auto type = GetStorageType(quota.unit_kind());
                        auto& usage = storageQuotasByType[type];
                        usage.SoftQuota += quota.data_size_soft_quota();
                        usage.HardQuota += quota.data_size_hard_quota();
                    }

                    if (entry.DomainDescription) {
                        for (const auto& poolUsage : entry.DomainDescription->Description.GetDiskSpaceUsage().GetStoragePoolsUsage()) {
                            auto type = GetStorageType(poolUsage.GetPoolKind());
                            tablesStorageByType[type] += poolUsage.GetTotalSize();
                        }

                        if (tablesStorageByType.empty() && entry.DomainDescription->Description.HasDiskSpaceUsage()) {
                            tablesStorageByType[GuessStorageType(entry.DomainDescription->Description)] =
                                entry.DomainDescription->Description.GetDiskSpaceUsage().GetTables().GetTotalSize();
                        }

                        if (storageQuotasByType.empty()) {
                            auto& quotas = storageQuotasByType[GuessStorageType(entry.DomainDescription->Description)];
                            quotas.HardQuota = entry.DomainDescription->Description.GetDatabaseQuotas().data_size_hard_quota();
                            quotas.SoftQuota = entry.DomainDescription->Description.GetDatabaseQuotas().data_size_soft_quota();
                        }
                    }

                    for (const auto& [type, size] : tablesStorageByType) {
                        auto it = storageQuotasByType.find(type);
                        if (it != storageQuotasByType.end() && it->second.SoftQuota) {
                            auto& tablesStorage = *tenant.AddTablesStorage();
                            tablesStorage.SetType(type);
                            tablesStorage.SetSize(size);
                            tablesStorage.SetLimit(it->second.SoftQuota);
                            tablesStorage.SetSoftQuota(it->second.SoftQuota);
                            tablesStorage.SetHardQuota(it->second.HardQuota);
                        }
                    }

                    for (const auto& [type, size] : databaseStorageByType) {
                        auto& databaseStorage = *tenant.AddDatabaseStorage();
                        databaseStorage.SetType(type);
                        databaseStorage.SetSize(size);
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
                if (HcOverallByTenantPath.count(path) > 0 && HcOverallByTenantPath[path] != NKikimrViewer::EFlag::Grey) {
                    tenant.SetOverall(HcOverallByTenantPath[path]);
                    OverallByDomainId[tenant.GetId()] = HcOverallByTenantPath[path];
                } else {
                    tenant.SetOverall(overall);
                    OverallByDomainId[tenant.GetId()] = overall;
                }
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
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        BLOG_TRACE("Timeout occurred");
        Result.AddErrors("Timeout occurred");
        ReplyAndPassAway();
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Tenant info (detailed)",
            .Description = "Returns information about tenants",
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "schema path",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "user",
            .Description = "tenant owner",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "followers",
            .Description = "return followers",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "metrics",
            .Description = "return tablet metrics",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "tablets",
            .Description = "return tablets",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "system_tablets",
            .Description = "return system tablets",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "offload_merge",
            .Description = "use offload merge",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "storage",
            .Description = "return storage info",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "nodes",
            .Description = "return nodes info",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "ui64",
            .Description = "return ui64 as number",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TTenantInfo>());
        return yaml;
    }
};

}
