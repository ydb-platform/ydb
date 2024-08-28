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
    using TBase::ReplyAndPassAway;
    std::optional<TRequestResponse<NConsole::TEvConsole::TEvListTenantsResponse>> ListTenantsResponse;
    std::unordered_map<TString, TRequestResponse<NConsole::TEvConsole::TEvGetTenantStatusResponse>> TenantStatusResponses;
    std::unordered_map<TString, TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> NavigateKeySetResult;
    std::unordered_map<TTabletId, TRequestResponse<TEvHive::TEvResponseHiveDomainStats>> HiveDomainStats;
    std::unordered_map<TTabletId, TRequestResponse<TEvHive::TEvResponseHiveStorageStats>> HiveStorageStats;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvSystemStateResponse>> SystemStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvTabletStateResponse>> TabletStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> OffloadedSystemStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> OffloadedTabletStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<NHealthCheck::TEvSelfCheckResultProto>> SelfCheckResults;

    THashMap<TString, NKikimrViewer::TTenant> TenantByPath;
    THashMap<TPathId, NKikimrViewer::TTenant> TenantBySubDomainKey;
    THashMap<TString, NKikimrViewer::EFlag> HcOverallByTenantPath;
    THashSet<TNodeId> Subscribers;
    THashSet<TNodeId> WhiteboardNodesRequested;
    THashSet<TString> OffloadTenantsRequested;
    THashSet<TString> MetadataCacheRequested;
    THashMap<TNodeId, TString> NodeIdsToTenant; // for tablet info
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString User;
    TString Database;
    TString DomainPath;
    bool Tablets = false;
    bool SystemTablets = false;
    bool Storage = false;
    bool Nodes = false;
    bool Users = false;
    bool OffloadMerge = false;
    bool MetadataCache = true;
    bool Direct = false;
    THashMap<TString, std::vector<TNodeId>> TenantNodes;
    TTabletId RootHiveId = 0;
    TString RootId; // id of root domain (tenant)
    NKikimrViewer::TTenantInfo Result;

    struct TStorageQuota {
        uint64 SoftQuota = 0;
        uint64 HardQuota = 0;
    };

public:
    TJsonTenantInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    TString GetLogPrefix() {
        static TString prefix = "json/tenantinfo ";
        return prefix;
    }

    TString GetDomainId(TPathId pathId) {
        return TStringBuilder() << pathId.OwnerId << '-' << pathId.LocalPathId;
    }

    bool IsValidTenant(const TString& path) {
        return Database.empty() || Database == path;
    }

    bool IsFilterByOwner() {
        return !User.empty();
    }

    bool IsValidOwner(const std::unordered_set<TString>& users) {
        return !IsFilterByOwner() || users.count(User) != 0;
    }

    void RequestMetadataCacheHealthCheck(const TString& path) {
        if (AppData()->FeatureFlags.GetEnableDbMetadataCache() && MetadataCache) {
            RequestStateStorageMetadataCacheEndpointsLookup(path);
        }
    }

    void Bootstrap() override {
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
        Database = params.Get("database");
        if (Database.empty()) {
            Database = params.Get("path");
        }
        Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
        OffloadMerge = FromStringWithDefault<bool>(params.Get("offload_merge"), OffloadMerge);
        MetadataCache = FromStringWithDefault<bool>(params.Get("metadata_cache"), MetadataCache);
        Direct |= !TBase::Event->Get()->Request.GetHeader("X-Forwarded-From-Node").empty(); // we're already forwarding
        Direct |= (Database == AppData()->TenantName); // we're already on the right node
        if (Database && !Direct) {
            return RedirectToDatabase(Database); // to find some dynamic node and redirect query there
        } else {
            TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
            auto* domain = domains->GetDomain();
            DomainPath = "/" + domain->Name;
            TPathId rootPathId(domain->SchemeRoot, 1);
            RootId = GetDomainId(rootPathId);
            RootHiveId = domains->GetHive();

            if (Database.empty()) {
                ListTenantsResponse = MakeRequestConsoleListTenants();
            } else {
                TenantStatusResponses[Database] = MakeRequestConsoleGetTenantStatus(Database);
                NavigateKeySetResult[Database] = MakeRequestSchemeCacheNavigate(Database);
            }

            if (Database.empty() || Database == DomainPath) {
                NKikimrViewer::TTenant& tenant = TenantBySubDomainKey[rootPathId];
                tenant.SetId(RootId);
                tenant.SetState(Ydb::Cms::GetDatabaseStatusResult::RUNNING);
                tenant.SetType(NKikimrViewer::Domain);
                tenant.SetName(DomainPath);
                NavigateKeySetResult[DomainPath] = MakeRequestSchemeCacheNavigate(DomainPath);
                RequestMetadataCacheHealthCheck(DomainPath);
            }

            HiveDomainStats[RootHiveId] = MakeRequestHiveDomainStats(RootHiveId);
            if (Storage) {
                HiveStorageStats[RootHiveId] = MakeRequestHiveStorageStats(RootHiveId);
            }

            Become(&TThis::StateCollectingInfo, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
        }
    }

    void PassAway() override {
        for (const TNodeId nodeId : Subscribers) {
            if (nodeId != SelfId().NodeId()) {
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
            }
        }
        TBase::PassAway();
    }

    STATEFN(StateCollectingInfo) {
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
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TString error = TStringBuilder() << "Failed to establish pipe: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status);
            if (ev->Get()->TabletId == GetConsoleId()) {
                if (ListTenantsResponse) {
                    ListTenantsResponse->Error(error);
                }
                for (auto& [path, response] : TenantStatusResponses) {
                    response.Error(error);
                }
            }
            {
                auto it = HiveDomainStats.find(ev->Get()->TabletId);
                if (it != HiveDomainStats.end()) {
                    it->second.Error(error);
                }
            }
            {
                auto it = HiveStorageStats.find(ev->Get()->TabletId);
                if (it != HiveStorageStats.end()) {
                    it->second.Error(error);
                }
            }
        }
        TBase::Handle(ev); // all RequestDone() are handled by base handler
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        ListTenantsResponse->Set(std::move(ev));
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ListTenantsResponse->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            TenantStatusResponses[path] = MakeRequestConsoleGetTenantStatus(path);
            NavigateKeySetResult[path] = MakeRequestSchemeCacheNavigate(path);
            RequestMetadataCacheHealthCheck(path);
        }
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr& ev) {
        Ydb::Cms::GetDatabaseStatusResult getTenantStatusResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&getTenantStatusResult);
        TString path = getTenantStatusResult.path();
        TenantStatusResponses[path].Set(std::move(ev));
        NKikimrViewer::TTenant& tenant = TenantByPath[path];
        tenant.SetName(path);
        tenant.SetState(getTenantStatusResult.state());
        if (getTenantStatusResult.has_required_shared_resources()) {
            tenant.SetType(NKikimrViewer::Shared);
            if (NavigateKeySetResult.count(path) == 0) {
                NavigateKeySetResult[path] = MakeRequestSchemeCacheNavigate(path);
            }
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
        if (SystemStateResponse.count(nodeId) == 0) {
            SystemStateResponse.emplace(nodeId, MakeRequest<TEvWhiteboard::TEvSystemStateResponse>(whiteboardServiceId,
                new TEvWhiteboard::TEvSystemStateRequest(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                nodeId));
        }
    }

    void SendWhiteboardTabletStateRequest(const TNodeId nodeId) {
        Subscribers.insert(nodeId);
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        if (TabletStateResponse.count(nodeId) == 0) {
            auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateRequest>();
            request->Record.SetFormat("packed5");
            TabletStateResponse.emplace(nodeId, MakeRequest<TEvWhiteboard::TEvTabletStateResponse>(whiteboardServiceId,
                request.release(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                nodeId));
        }
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
            THolder<TEvViewer::TEvViewerRequest> sysRequest = MakeHolder<TEvViewer::TEvViewerRequest>();
            sysRequest->Record.MutableSystemRequest();
            sysRequest->Record.SetTimeout(Timeout / 3);
            for (auto nodeId : nodesIds) {
                sysRequest->Record.MutableLocation()->AddNodeId(nodeId);
            }
            OffloadedSystemStateResponse[nodeId] = MakeRequestViewer(nodeId, sysRequest.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);

            if (Tablets) {
                THolder<TEvViewer::TEvViewerRequest> tblRequest = MakeHolder<TEvViewer::TEvViewerRequest>();
                tblRequest->Record.MutableTabletRequest()->SetFormat("packed5");
                tblRequest->Record.SetTimeout(Timeout / 3);
                for (auto nodeId : nodesIds) {
                    tblRequest->Record.MutableLocation()->AddNodeId(nodeId);
                }
                OffloadedTabletStateResponse[nodeId] = MakeRequestViewer(nodeId, tblRequest.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
            }
        }
    }

    void Handle(TEvHive::TEvResponseHiveDomainStats::TPtr& ev) {
        auto& response = HiveDomainStats[ev->Cookie];
        response.Set(std::move(ev));
        for (const NKikimrHive::THiveDomainStats& hiveStat : response.Get()->Record.GetDomainStats()) {
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

            std::vector<TNodeId> nodesIds;
            nodesIds.reserve(hiveStat.NodeIdsSize());
            for (auto nodeId : hiveStat.GetNodeIds()) {
                nodesIds.push_back(nodeId);
                NodeIdsToTenant.insert({nodeId, tenantId});
            }
            TenantNodes[tenantId] = nodesIds;

            if (Database.empty() || Database == tenant.GetName()) {
                if (OffloadMerge) {
                    SendOffloadRequests(tenantId);
                } else {
                    for (TNodeId nodeId : hiveStat.GetNodeIds()) {
                        SendWhiteboardRequests(nodeId);
                    }
                }
            }
        }

        RequestDone();
    }

    void Handle(TEvHive::TEvResponseHiveStorageStats::TPtr& ev) {
        HiveStorageStats[ev->Cookie].Set(std::move(ev));
        RequestDone();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TString path = GetPath(ev);
        auto& result(NavigateKeySetResult[path]);
        result.Set(std::move(ev));
        if (result.Get()->Request->ResultSet.size() == 1 && result.Get()->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            auto domainInfo = result.Get()->Request->ResultSet.begin()->DomainInfo;
            TTabletId hiveId = domainInfo->Params.GetHive();
            if (hiveId) {
                if (HiveDomainStats.count(hiveId) == 0) {
                    HiveDomainStats[hiveId] = MakeRequestHiveDomainStats(hiveId);
                }
                if (Storage) {
                    if (HiveStorageStats.count(hiveId) == 0) {
                        HiveStorageStats[hiveId] = MakeRequestHiveStorageStats(hiveId);
                    }
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
            tenant.SetId(id);
            tenant.SetName(path);
            if (tenant.GetType() == NKikimrViewer::UnknownTenantType) {
                tenant.SetType(NKikimrViewer::Dedicated);
            }
        }
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        SystemStateResponse[nodeId].Set(std::move(ev));
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        TabletStateResponse[nodeId].Set(std::move(ev));
        RequestDone();
    }

    void Handle(NHealthCheck::TEvSelfCheckResultProto::TPtr& ev) {
        TNodeId nodeId = ev->Cookie;
        auto& selfCheckResult(SelfCheckResults[nodeId]);
        selfCheckResult.Set(std::move(ev));
        auto& result(selfCheckResult.Get()->Record);
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
            if (MetadataCacheRequested.insert(ev->Get()->Path).second) {
                SelfCheckResults[activeNode] = MakeRequest<NHealthCheck::TEvSelfCheckResultProto>(*cache, new NHealthCheck::TEvSelfCheckRequestProto,
                    IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, activeNode);
            }
        }
        RequestDone();
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        auto tenantId = NodeIdsToTenant[nodeId];
        switch (ev->Get()->Record.GetResponseCase()) {
            case NKikimrViewer::TEvViewerResponse::kTabletResponse:
                OffloadedTabletStateResponse[nodeId].Set(std::move(ev));
                RequestDone();
                break;
            case NKikimrViewer::TEvViewerResponse::kSystemResponse:
                OffloadedSystemStateResponse[nodeId].Set(std::move(ev));
                RequestDone();
                break;
            default:
                break;
        }
    }

    template<typename TRequestResponse>
    bool HasUnfinishedRequest(std::unordered_map<TNodeId, TRequestResponse>& responses, TNodeId nodeId) {
        auto itRequest = responses.find(nodeId);
        if (itRequest != responses.end()) {
            return !itRequest->second.IsDone();
        }
        return false;
    }

    template<typename TRequestResponse>
    void ErrorRequest(std::unordered_map<TNodeId, TRequestResponse>& responses, TNodeId nodeId, const TString& error) {
        auto itRequest = responses.find(nodeId);
        if (itRequest != responses.end()) {
            if (itRequest->second.Error(error)) {
                RequestDone();
            }
        }
    }

    void ErrorRequests(TNodeId nodeId, const TString& error) {
        if (HasUnfinishedRequest(OffloadedSystemStateResponse, nodeId)) {
            // fallback
            for (TNodeId nodeId : TenantNodes[NodeIdsToTenant[nodeId]]) {
                SendWhiteboardSystemStateRequest(nodeId);
            }
        }
        if (HasUnfinishedRequest(OffloadedTabletStateResponse, nodeId)) {
            // fallback
            for (TNodeId nodeId : TenantNodes[NodeIdsToTenant[nodeId]]) {
                SendWhiteboardTabletStateRequest(nodeId);
            }
        }
        ErrorRequest(SelfCheckResults, nodeId, error);
        ErrorRequest(SystemStateResponse, nodeId, error);
        ErrorRequest(TabletStateResponse, nodeId, error);
        ErrorRequest(OffloadedSystemStateResponse, nodeId, error);
        ErrorRequest(OffloadedTabletStateResponse, nodeId, error);
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        ui32 nodeId = ev.Get()->Cookie;
        ErrorRequests(nodeId, "undelivered");
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        ErrorRequests(nodeId, "disconnected");
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

    void ReplyAndPassAway() override {
        Result.SetVersion(2);
        THashMap<TString, NKikimrViewer::EFlag> OverallByDomainId;
        std::unordered_map<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*> nodeSystemStateInfo;

        for (const auto& [nodeId, request] : OffloadedSystemStateResponse) {
            if (request.IsOk()) {
                for (const auto& systemState : request.Get()->Record.GetSystemResponse().GetSystemStateInfo()) {
                    nodeSystemStateInfo[systemState.GetNodeId()] = &systemState;
                }
            }
        }

        for (const auto& [nodeId, request] : SystemStateResponse) {
            if (request.IsOk()) {
                nodeSystemStateInfo[nodeId] = &(request.Get()->Record.GetSystemStateInfo(0));
            }
        }
        for (const auto& [subDomainKey, tenantBySubDomainKey] : TenantBySubDomainKey) {
            TString name(tenantBySubDomainKey.GetName());
            if (!IsValidTenant(name)) {
                continue;
            }
            TString id(GetDomainId(subDomainKey));
            NKikimrWhiteboard::TEvTabletStateResponse tabletInfo;
            THashMap<TTabletId, const NKikimrWhiteboard::TTabletStateInfo*> tabletInfoIndex;

            if (Tablets) {
                const auto& tenantNodes(TenantNodes[name]);
                bool hasTabletInfo = false;
                for (TNodeId nodeId : tenantNodes) {
                    auto it = OffloadedTabletStateResponse.find(nodeId);
                    if (it != OffloadedTabletStateResponse.end() && it->second.IsOk()) {
                        tabletInfo = std::move(*(it->second.Get()->Record.MutableTabletResponse()));
                        hasTabletInfo = true;
                        break;
                    }
                }

                if (!hasTabletInfo) {
                    TMap<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse> tabletInfoForMerge;
                    for (TNodeId nodeId : tenantNodes) {
                        auto it = TabletStateResponse.find(nodeId);
                        if (it != TabletStateResponse.end() && it->second.IsOk()) {
                            tabletInfoForMerge.emplace(nodeId, std::move(it->second.Get()->Record));
                        }
                    }
                    TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse>::MergeResponses(tabletInfo, tabletInfoForMerge);
                }

                if (SystemTablets) {
                    for (const auto& info : TWhiteboardInfo<NKikimrWhiteboard::TEvTabletStateResponse>::GetElementsField(tabletInfo)) {
                        tabletInfoIndex[info.GetTabletId()] = &info;
                    }
                }
            }

            NKikimrViewer::EFlag overall = NKikimrViewer::EFlag::Grey;
            auto itNavigate = NavigateKeySetResult.find(name);
            if (itNavigate != NavigateKeySetResult.end() && itNavigate->second.IsOk()) {
                NSchemeCache::TSchemeCacheNavigate::TEntry entry = itNavigate->second.Get()->Request->ResultSet.front();
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
                auto itTenantByPath = TenantByPath.find(name);
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

                TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
                auto* domain = domains->GetDomain();
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
                        auto itResourceNavigate = NavigateKeySetResult.find(tenant.GetResourceId());
                        if (itResourceNavigate != NavigateKeySetResult.end() && itResourceNavigate->second.IsOk()) {
                            NSchemeCache::TSchemeCacheNavigate::TEntry entry = itResourceNavigate->second.Get()->Request->ResultSet.front();
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
                    THashMap<NKikimrViewer::TStorageUsage::EType, std::pair<ui64, ui64>> databaseStorageByType;
                    auto itHiveStorageStats = HiveStorageStats.find(hiveId);
                    if (itHiveStorageStats != HiveStorageStats.end() && itHiveStorageStats->second.IsOk()) {
                        const NKikimrHive::TEvResponseHiveStorageStats& record = itHiveStorageStats->second.Get()->Record;
                        if (entry.DomainDescription) {
                            uint64 storageAllocatedSize = 0;
                            uint64 storageAvailableSize = 0;
                            uint64 storageMinAvailableSize = std::numeric_limits<ui64>::max();
                            uint64 storageGroups = 0;
                            std::unordered_map<TString, NKikimrViewer::TStorageUsage::EType> storagePoolType;
                            for (const auto& storagePool : entry.DomainDescription->Description.GetStoragePools()) {
                                storagePoolType[storagePool.GetName()] = GetStorageType(storagePool.GetKind());
                            }
                            for (const NKikimrHive::THiveStoragePoolStats& poolStat : record.GetPools()) {
                                if (storagePoolType.count(poolStat.GetName())) {
                                    NKikimrViewer::TStorageUsage::EType storageType = storagePoolType[poolStat.GetName()];
                                    for (const NKikimrHive::THiveStorageGroupStats& groupStat : poolStat.GetGroups()) {
                                        storageAllocatedSize += groupStat.GetAllocatedSize();
                                        storageAvailableSize += groupStat.GetAvailableSize();
                                        databaseStorageByType[storageType].first += groupStat.GetAllocatedSize();
                                        databaseStorageByType[storageType].second += groupStat.GetAvailableSize();
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
                        auto& tablesStorage = *tenant.AddTablesStorage();
                        tablesStorage.SetType(type);
                        tablesStorage.SetSize(size);
                        if (it != storageQuotasByType.end()) {
                            tablesStorage.SetLimit(it->second.SoftQuota);
                            tablesStorage.SetSoftQuota(it->second.SoftQuota);
                            tablesStorage.SetHardQuota(it->second.HardQuota);
                        }
                    }

                    for (const auto& [type, pr] : databaseStorageByType) {
                        auto& databaseStorage = *tenant.AddDatabaseStorage();
                        databaseStorage.SetType(type);
                        databaseStorage.SetSize(pr.first);
                        databaseStorage.SetLimit(pr.first + pr.second);
                    }
                }

                THashSet<TNodeId> tenantNodes;

                for (TNodeId nodeId : tenant.GetNodeIds()) {
                    auto itNodeInfo = nodeSystemStateInfo.find(nodeId);
                    if (itNodeInfo != nodeSystemStateInfo.end()) {
                        const auto& nodeInfo(*(itNodeInfo->second));
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
                auto itOverall = HcOverallByTenantPath.find(name);
                if (itOverall != HcOverallByTenantPath.end()) {
                    tenant.SetOverall(itOverall->second);
                    OverallByDomainId[tenant.GetId()] = itOverall->second;
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
        ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
    }

    void HandleTimeout() {
        Result.AddErrors("timeout");
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
