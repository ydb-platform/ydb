#pragma once

#include "builder.h"
#include "log.h"
#include "health_check.h"
#include "health_check_structs.h"
#include "health_check_utils.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/core/util/tuples.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <regex>

static decltype(auto) make_vslot_tuple(const NKikimrBlobStorage::TVSlotId& id) {
    return std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId());
}

template <>
struct std::equal_to<NKikimrBlobStorage::TVSlotId> {
    bool operator ()(const NKikimrBlobStorage::TVSlotId& a, const NKikimrBlobStorage::TVSlotId& b) const {
        return make_vslot_tuple(a) == make_vslot_tuple(b);
    }
};

template <>
struct std::hash<NKikimrBlobStorage::TVSlotId> {
    size_t operator ()(const NKikimrBlobStorage::TVSlotId& a) const {
        auto tp = make_vslot_tuple(a);
        return std::hash<decltype(tp)>()(tp);
    }
};

#define BLOG_CRIT(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::HEALTH, stream)
#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::HEALTH, stream)

namespace NKikimr::NHealthCheck {

using namespace NActors;
using namespace Ydb;
using namespace NSchemeShard;
using namespace NSysView;
using namespace NConsole;

void RemoveUnrequestedEntries(Ydb::Monitoring::SelfCheckResult& result, const Ydb::Monitoring::SelfCheckRequest& request) {
    if (!request.return_verbose_status()) {
        result.clear_database_status();
    }
    if (request.minimum_status() != Ydb::Monitoring::StatusFlag::UNSPECIFIED) {
        for (auto itIssue = result.mutable_issue_log()->begin(); itIssue != result.mutable_issue_log()->end();) {
            if (itIssue->status() < request.minimum_status()) {
                itIssue = result.mutable_issue_log()->erase(itIssue);
            } else {
                ++itIssue;
            }
        }
    }
    if (request.maximum_level() != 0) {
        for (auto itIssue = result.mutable_issue_log()->begin(); itIssue != result.mutable_issue_log()->end();) {
            if (itIssue->level() > request.maximum_level()) {
                itIssue = result.mutable_issue_log()->erase(itIssue);
            } else {
                ++itIssue;
            }
        }
    }
}

struct TEvPrivate {
    enum EEv {
        EvRetryNodeWhiteboard = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvRetryNodeWhiteboard : NActors::TEventLocal<TEvRetryNodeWhiteboard, EvRetryNodeWhiteboard> {
        TNodeId NodeId;
        int EventId;

        TEvRetryNodeWhiteboard(TNodeId nodeId, int eventId)
            : NodeId(nodeId)
            , EventId(eventId)
        {}
    };
};

class TSelfCheckRequest : public TActorBootstrapped<TSelfCheckRequest> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MONITORING_REQUEST; }

    TActorId Sender;
    THolder<TEvSelfCheckRequest> Request;
    ui64 Cookie;
    NWilson::TSpan Span;

    TSelfCheckRequest(const TActorId& sender, THolder<TEvSelfCheckRequest> request, ui64 cookie, NWilson::TTraceId&& traceId, const NKikimrConfig::THealthCheckConfig& config)
        : Sender(sender)
        , Request(std::move(request))
        , Cookie(cookie)
        , Span(TComponentTracingLevels::TTablet::Basic, std::move(traceId), "health_check", NWilson::EFlags::AUTO_END)
        , HealthCheckConfig(config)
    {}

    enum ETimeoutTag {
        TimeoutBSC,
        TimeoutFinal,
    };

    static bool IsSuccess(const std::unique_ptr<TEvSchemeShard::TEvDescribeSchemeResult>& ev) {
        return ev->GetRecord().status() == NKikimrScheme::StatusSuccess;
    }

    static TString GetError(const std::unique_ptr<TEvSchemeShard::TEvDescribeSchemeResult>& ev) {
        return NKikimrScheme::EStatus_Name(ev->GetRecord().status());
    }

    static bool IsSuccess(const std::unique_ptr<TEvStateStorage::TEvBoardInfo>& ev) {
        return ev->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok;
    }

    static TString GetError(const std::unique_ptr<TEvStateStorage::TEvBoardInfo>& ev) {
        switch (ev->Status) {
            case TEvStateStorage::TEvBoardInfo::EStatus::Ok:
                return "Ok";
            case TEvStateStorage::TEvBoardInfo::EStatus::Unknown:
                return "Unknown";
            case TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable:
                return "NotAvailable";
        }
    }

    TString FilterDatabase;
    THashMap<TSubDomainKey, TString> FilterDomainKey;
    TVector<TActorId> PipeClients;
    int Requests = 0;
    TString DomainPath;
    TTabletId ConsoleId;
    TTabletId BsControllerId;
    TTabletId RootSchemeShardId;
    TTabletId RootHiveId;
    THashMap<TString, TRequestResponse<TEvSchemeShard::TEvDescribeSchemeResult>> DescribeByPath;
    THashMap<TTabletId, TRequestResponse<TEvSysView::TEvGetPartitionStatsResult>> GetPartitionStatsResult;
    THashMap<TString, THashSet<TString>> PathsByPoolName;
    THashMap<TString, THolder<NTenantSlotBroker::TEvTenantSlotBroker::TEvTenantState>> TenantStateByPath;
    THashMap<TTabletId, TRequestResponse<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStats;
    THashMap<TTabletId, TRequestResponse<TEvHive::TEvResponseHiveInfo>> HiveInfo;
    ui64 HiveNodeStatsToGo = 0;
    std::optional<TRequestResponse<TEvConsole::TEvListTenantsResponse>> ListTenants;
    std::optional<TRequestResponse<TEvInterconnect::TEvNodesInfo>> NodesInfo;
    THashMap<TNodeId, const TEvInterconnect::TNodeInfo*> MergedNodeInfo;
    std::optional<TRequestResponse<TEvSysView::TEvGetStoragePoolsResponse>> StoragePools;
    std::optional<TRequestResponse<TEvSysView::TEvGetGroupsResponse>> Groups;
    std::optional<TRequestResponse<TEvSysView::TEvGetVSlotsResponse>> VSlots;
    std::optional<TRequestResponse<TEvSysView::TEvGetPDisksResponse>> PDisks;
    std::optional<TRequestResponse<TEvNodeWardenStorageConfig>> NodeWardenStorageConfig;
    std::optional<TRequestResponse<TEvStateStorage::TEvBoardInfo>> DatabaseBoardInfo;
    THashSet<TNodeId> UnknownStaticGroups;

    const NKikimrConfig::THealthCheckConfig& HealthCheckConfig;

    std::vector<TNodeId> SubscribedNodeIds;
    THashSet<TNodeId> StorageNodeIds;
    THashSet<TNodeId> ComputeNodeIds;
    std::unordered_map<std::pair<TNodeId, int>, ui32> NodeRetries;
    ui32 MaxRetries = 2;
    TDuration RetryDelay = TDuration::MilliSeconds(500);

    THashMap<TString, TDatabaseState> DatabaseState;
    THashMap<TPathId, TString> SharedDatabases;

    THashMap<TNodeId, TRequestResponse<TEvWhiteboard::TEvSystemStateResponse>> NodeSystemState;
    THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*> MergedNodeSystemState;

    std::unordered_map<TString, const NKikimrSysView::TPDiskEntry*> PDisksMap;
    std::unordered_map<TString, Ydb::Monitoring::StatusFlag::Status> VDiskStatuses;

    // BSC case
    THashMap<ui64, TStoragePoolState> StoragePoolState;
    THashSet<ui64> StoragePoolSeen;
    THashMap<ui32, TGroupState> GroupState;
    // no BSC case
    THashMap<TString, TStoragePoolState> StoragePoolStateByName;
    THashSet<TString> StoragePoolSeenByName;

    THashSet<TNodeId> UnavailableStorageNodes;
    THashSet<TNodeId> UnavailableComputeNodes;

    THashMap<TNodeId, TRequestResponse<TEvWhiteboard::TEvVDiskStateResponse>> NodeVDiskState;
    TList<NKikimrWhiteboard::TVDiskStateInfo> VDisksAppended;
    std::unordered_map<TString, const NKikimrWhiteboard::TVDiskStateInfo*> MergedVDiskState;

    THashMap<TNodeId, TRequestResponse<TEvWhiteboard::TEvPDiskStateResponse>> NodePDiskState;
    TList<NKikimrWhiteboard::TPDiskStateInfo> PDisksAppended;
    std::unordered_map<TString, const NKikimrWhiteboard::TPDiskStateInfo*> MergedPDiskState;

    THashMap<TNodeId, TRequestResponse<TEvWhiteboard::TEvBSGroupStateResponse>> NodeBSGroupState;
    TList<NKikimrWhiteboard::TBSGroupStateInfo> BSGroupAppended;
    std::unordered_map<TGroupId, const NKikimrWhiteboard::TBSGroupStateInfo*> MergedBSGroupState;

    THashMap<ui64, TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> NavigateKeySet;

    THashMap<TString, THintOverloadedShard> OverloadedShardHints;
    static constexpr size_t MAX_OVERLOADED_SHARDS_HINTS = 10;
    static constexpr double OVERLOADED_SHARDS_CPU_CORES = 0.75;

    TTabletRequestsState TabletRequests;

    TDuration Timeout = TDuration::MilliSeconds(HealthCheckConfig.GetTimeout());
    bool ReturnHints = false;
    static constexpr TStringBuf STATIC_STORAGE_POOL_NAME = "static";

    bool IsSpecificDatabaseFilter() const {
        return FilterDatabase && FilterDatabase != DomainPath;
    }

    void Bootstrap() {
        FilterDatabase = Request->Database;
        if (Request->Request.operation_params().has_operation_timeout()) {
            Timeout = GetDuration(Request->Request.operation_params().operation_timeout());
        }
        ReturnHints = Request->Request.return_hints() && IsSpecificDatabaseFilter();
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();
        DomainPath = "/" + domain->Name;
        RootSchemeShardId = domain->SchemeRoot;
        ConsoleId = MakeConsoleID();
        RootHiveId = domains->GetHive();
        BsControllerId = MakeBSControllerID();

        if (ConsoleId) {
            TabletRequests.TabletStates[ConsoleId].Database = DomainPath;
            TabletRequests.TabletStates[ConsoleId].Type = TTabletTypes::Console;
            if (!FilterDatabase) {
                ListTenants = RequestListTenants();
            } else {
                RequestSchemeCacheNavigate(FilterDatabase);
            }
        }

        if (RootSchemeShardId && !IsSpecificDatabaseFilter()) {
            TabletRequests.TabletStates[RootSchemeShardId].Database = DomainPath;
            TabletRequests.TabletStates[RootSchemeShardId].Type = TTabletTypes::SchemeShard;
            DescribeByPath[DomainPath] = RequestDescribe(RootSchemeShardId, DomainPath);
        }

        if (BsControllerId) {
            TabletRequests.TabletStates[BsControllerId].Database = DomainPath;
            TabletRequests.TabletStates[BsControllerId].Type = TTabletTypes::BSController;
            RequestBsController();
        }


        NodesInfo = TRequestResponse<TEvInterconnect::TEvNodesInfo>(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, "TEvInterconnect::TEvListNodes"));
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(), 0/*flags*/, 0/*cookie*/, Span.GetTraceId());
        ++Requests;

        Become(&TThis::StateWait);
        Schedule(TDuration::MilliSeconds(Timeout.MilliSeconds() / 2), new TEvents::TEvWakeup(TimeoutBSC)); // 50% timeout (for bsc)
        Schedule(Timeout, new TEvents::TEvWakeup(TimeoutFinal)); // timeout for the rest
    }

    bool HaveAllBSControllerInfo() {
        return StoragePools && StoragePools->IsOk() && Groups && Groups->IsOk() && VSlots && VSlots->IsOk() && PDisks && PDisks->IsOk();
    }

    bool NeedWhiteboardInfoForGroup(TGroupId groupId) {
        return UnknownStaticGroups.contains(groupId) || (!HaveAllBSControllerInfo() && IsStaticGroup(groupId));
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
        NodeWardenStorageConfig->Set(std::move(ev));
        if (const NKikimrBlobStorage::TStorageConfig& config = *NodeWardenStorageConfig->Get()->Config; config.HasBlobStorageConfig()) {
            if (const auto& bsConfig = config.GetBlobStorageConfig(); bsConfig.HasServiceSet()) {
                const auto& staticConfig = bsConfig.GetServiceSet();
                for (const NKikimrBlobStorage::TNodeWardenServiceSet_TPDisk& pDisk : staticConfig.pdisks()) {
                    auto pDiskId = GetPDiskId(pDisk);
                    auto itPDisk = MergedPDiskState.find(pDiskId);
                    if (itPDisk == MergedPDiskState.end()) {
                        PDisksAppended.emplace_back();
                        NKikimrWhiteboard::TPDiskStateInfo& pbPDisk = PDisksAppended.back();
                        itPDisk = MergedPDiskState.emplace(pDiskId, &pbPDisk).first;
                        pbPDisk.SetNodeId(pDisk.GetNodeID());
                        pbPDisk.SetPDiskId(pDisk.GetPDiskID());
                        pbPDisk.SetPath(pDisk.GetPath());
                        pbPDisk.SetGuid(pDisk.GetPDiskGuid());
                        pbPDisk.SetCategory(static_cast<ui64>(pDisk.GetPDiskCategory()));
                    }
                }
                for (const NKikimrBlobStorage::TNodeWardenServiceSet_TVDisk& vDisk : staticConfig.vdisks()) {
                    auto vDiskId = GetVDiskId(vDisk);
                    auto itVDisk = MergedVDiskState.find(vDiskId);
                    if (itVDisk == MergedVDiskState.end()) {
                        VDisksAppended.emplace_back();
                        NKikimrWhiteboard::TVDiskStateInfo& pbVDisk = VDisksAppended.back();
                        itVDisk = MergedVDiskState.emplace(vDiskId, &pbVDisk).first;
                        pbVDisk.MutableVDiskId()->CopyFrom(vDisk.vdiskid());
                        pbVDisk.SetNodeId(vDisk.GetVDiskLocation().GetNodeID());
                        pbVDisk.SetPDiskId(vDisk.GetVDiskLocation().GetPDiskID());
                    }

                    auto groupId = vDisk.GetVDiskID().GetGroupID();
                    if (NeedWhiteboardInfoForGroup(groupId)) {
                        BLOG_D("Requesting whiteboard for group " << groupId);
                        RequestStorageNode(vDisk.GetVDiskLocation().GetNodeID());
                    }
                }
                for (const NKikimrBlobStorage::TGroupInfo& group : staticConfig.groups()) {
                    TString storagePoolName = group.GetStoragePoolName();
                    if (!storagePoolName) {
                        storagePoolName = STATIC_STORAGE_POOL_NAME;
                    }
                    StoragePoolStateByName[storagePoolName].Groups.emplace(group.groupid());
                    StoragePoolStateByName[storagePoolName].Name = storagePoolName;

                    if (!IsSpecificDatabaseFilter()) {
                        DatabaseState[DomainPath].StoragePoolNames.emplace(storagePoolName);
                    }
                }
            }
        }

        RequestDone("TEvNodeWardenStorageConfig");
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(TEvHive::TEvResponseHiveNodeStats, Handle);
            hFunc(TEvHive::TEvResponseHiveInfo, Handle);
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle)
            hFunc(TEvSysView::TEvGetStoragePoolsResponse, Handle);
            hFunc(TEvSysView::TEvGetGroupsResponse, Handle);
            hFunc(TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(TEvSysView::TEvGetPDisksResponse, Handle);
            hFunc(TEvSysView::TEvGetPartitionStatsResult, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvPrivate::TEvRetryNodeWhiteboard, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvents::TEvWakeup, HandleTimeout);
            hFunc(TEvNodeWardenStorageConfig, Handle);
        }
    }

    void RequestDone(const char* name) {
        --Requests;
        if (Requests == 0) {
            ReplyAndPassAway();
        }
        if (Requests < 0) {
            BLOG_CRIT("Requests < 0 in RequestDone(" << name << ")");
        }
    }

    NTabletPipe::TClientRetryPolicy GetPipeRetryPolicy() const {
        return {
            .RetryLimitCount = 4,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::Seconds(1),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true,
        };
    }

    template<typename ResponseType>
    [[nodiscard]] TRequestResponse<ResponseType> RequestTabletPipe(TTabletId tabletId,
                                                                   IEventBase* payload,
                                                                   std::optional<TTabletRequestsState::ERequestId> requestId = std::nullopt) {
        TString key = TypeName(*payload);
        ui64 cookie;
        if (requestId) {
            cookie = *requestId;
            TabletRequests.MakeRequest(tabletId, key, cookie);
        } else {
            cookie = TabletRequests.MakeRequest(tabletId, key);
        }
        TRequestResponse<ResponseType> response(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, key));
        if (Span) {
            response.Span.Attribute("tablet_id", ::ToString(tabletId));
        }
        TTabletRequestsState::TTabletState& requestState(TabletRequests.TabletStates[tabletId]);
        if (!requestState.TabletPipe) {
            requestState.TabletPipe = RegisterWithSameMailbox(NTabletPipe::CreateClient(
                SelfId(),
                tabletId,
                GetPipeRetryPolicy()));
            PipeClients.emplace_back(requestState.TabletPipe);
        }
        NTabletPipe::SendData(SelfId(), requestState.TabletPipe, payload, cookie, response.Span.GetTraceId());
        ++Requests;
        return response;
    }

    std::unordered_map<TTabletId, std::vector<TString>> TabletToDescribePath;

    [[nodiscard]] TRequestResponse<TEvSchemeShard::TEvDescribeSchemeResult> RequestDescribe(TTabletId schemeShardId, const TString& path,
            const NKikimrSchemeOp::TDescribeOptions* options = nullptr) {
        THolder<TEvSchemeShard::TEvDescribeScheme> request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>();
        NKikimrSchemeOp::TDescribePath& record = request->Record;
        record.SetPath(path);
        if (options) {
            record.MutableOptions()->CopyFrom(*options);
        } else {
            record.MutableOptions()->SetReturnPartitioningInfo(false);
            record.MutableOptions()->SetReturnPartitionConfig(false);
            record.MutableOptions()->SetReturnChildren(false);
        }
        auto response = RequestTabletPipe<TEvSchemeShard::TEvDescribeSchemeResult>(schemeShardId, request.Release());
        if (response.Span) {
            response.Span.Attribute("path", path);
        }
        TabletToDescribePath[schemeShardId].emplace_back(path);
        return response;
    }

    [[nodiscard]] TRequestResponse<TEvSysView::TEvGetPartitionStatsResult> RequestPartitionStats(TTabletId schemeShardId, TSubDomainKey subDomainKey) {
        THolder<TEvSysView::TEvGetPartitionStats> request = MakeHolder<TEvSysView::TEvGetPartitionStats>();
        NKikimrSysView::TEvGetPartitionStats& record = request->Record;
        record.MutableFilter()->MutableNotLess()->SetCPUCores(OVERLOADED_SHARDS_CPU_CORES);
        record.SetDomainKeyOwnerId(subDomainKey.GetSchemeShard());
        record.SetDomainKeyPathId(subDomainKey.GetPathId());
        return RequestTabletPipe<TEvSysView::TEvGetPartitionStatsResult>(schemeShardId, request.Release(), TTabletRequestsState::RequestGetPartitionStats);
    }

    [[nodiscard]] TRequestResponse<TEvHive::TEvResponseHiveInfo> RequestHiveInfo(TTabletId hiveId) {
        THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
        request->Record.SetReturnFollowers(true);
        return RequestTabletPipe<TEvHive::TEvResponseHiveInfo>(hiveId, request.Release());
    }

    [[nodiscard]] TRequestResponse<TEvHive::TEvResponseHiveNodeStats> RequestHiveNodeStats(TTabletId hiveId) {
        THolder<TEvHive::TEvRequestHiveNodeStats> request = MakeHolder<TEvHive::TEvRequestHiveNodeStats>();
        return RequestTabletPipe<TEvHive::TEvResponseHiveNodeStats>(hiveId, request.Release());
    }

    [[nodiscard]] TRequestResponse<TEvConsole::TEvListTenantsResponse> RequestListTenants() {
        THolder<TEvConsole::TEvListTenantsRequest> request = MakeHolder<TEvConsole::TEvListTenantsRequest>();
        return RequestTabletPipe<TEvConsole::TEvListTenantsResponse>(ConsoleId, request.Release());
    }

    void RequestBsController() {
        THolder<TEvSysView::TEvGetStoragePoolsRequest> requestPools = MakeHolder<TEvSysView::TEvGetStoragePoolsRequest>();
        StoragePools = RequestTabletPipe<TEvSysView::TEvGetStoragePoolsResponse>(BsControllerId, requestPools.Release(), TTabletRequestsState::RequestStoragePools);
        THolder<TEvSysView::TEvGetGroupsRequest> requestGroups = MakeHolder<TEvSysView::TEvGetGroupsRequest>();
        Groups = RequestTabletPipe<TEvSysView::TEvGetGroupsResponse>(BsControllerId, requestGroups.Release(), TTabletRequestsState::RequestGroups);
        THolder<TEvSysView::TEvGetVSlotsRequest> requestVSlots = MakeHolder<TEvSysView::TEvGetVSlotsRequest>();
        VSlots = RequestTabletPipe<TEvSysView::TEvGetVSlotsResponse>(BsControllerId, requestVSlots.Release(), TTabletRequestsState::RequestVSlots);
        THolder<TEvSysView::TEvGetPDisksRequest> requestPDisks = MakeHolder<TEvSysView::TEvGetPDisksRequest>();
        PDisks = RequestTabletPipe<TEvSysView::TEvGetPDisksResponse>(BsControllerId, requestPDisks.Release(), TTabletRequestsState::RequestPDisks);
    }

    [[nodiscard]] TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(THolder<TSchemeCacheNavigate> request) {
        TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> response(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, TypeName(*request.Get())));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), 0/*flags*/, 0/*cookie*/, response.Span.GetTraceId());
        ++Requests;
        return response;
    }

    [[nodiscard]] TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(const TString& path, ui64 cookie) {
        THolder<TSchemeCacheNavigate> request = MakeHolder<TSchemeCacheNavigate>();
        request->Cookie = cookie;
        TSchemeCacheNavigate::TEntry& entry = request->ResultSet.emplace_back();
        entry.Path = NKikimr::SplitPath(path);
        entry.Operation = TSchemeCacheNavigate::EOp::OpPath;
        auto response = MakeRequestSchemeCacheNavigate(std::move(request));
        if (response.Span) {
            response.Span.Attribute("path", path);
        }
        return response;
    }

    [[nodiscard]] TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> MakeRequestSchemeCacheNavigate(const TPathId& pathId, ui64 cookie) {
        THolder<TSchemeCacheNavigate> request = MakeHolder<TSchemeCacheNavigate>();
        request->Cookie = cookie;
        TSchemeCacheNavigate::TEntry& entry = request->ResultSet.emplace_back();
        entry.TableId.PathId = pathId;
        entry.RequestType = TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;
        entry.Operation = TSchemeCacheNavigate::EOp::OpPath;
        auto response = MakeRequestSchemeCacheNavigate(std::move(request));
        if (response.Span) {
            response.Span.Attribute("path_id", pathId.ToString());
        }
        return response;
    }

    ui64 RequestSchemeCacheNavigate(const TString& path) {
        ui64 cookie = NavigateKeySet.size();
        NavigateKeySet.emplace(cookie, MakeRequestSchemeCacheNavigate(path, cookie));
        return cookie;
    }

    ui64 RequestSchemeCacheNavigate(const TPathId& pathId) {
        ui64 cookie = NavigateKeySet.size();
        NavigateKeySet.emplace(cookie, MakeRequestSchemeCacheNavigate(pathId, cookie));
        return cookie;
    }

    TRequestResponse<TEvStateStorage::TEvBoardInfo> MakeRequestStateStorageEndpointsLookup(const TString& path, ui64 cookie = 0) {
        TRequestResponse<TEvStateStorage::TEvBoardInfo> response(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, "BoardLookupActor"));
        RegisterWithSameMailbox(CreateBoardLookupActor(MakeEndpointsBoardPath(path),
                                                    SelfId(),
                                                    EBoardLookupMode::Second, {}, cookie));
        if (response.Span) {
            response.Span.Attribute("path", path);
        }
        ++Requests;
        return response;
    }

    template<typename TEvent>
    [[nodiscard]] TRequestResponse<typename WhiteboardResponse<TEvent>::Type> RequestNodeWhiteboard(TNodeId nodeId, std::initializer_list<int> fields = {}) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        auto request = MakeHolder<TEvent>();
        for (int field : fields) {
            request->Record.AddFieldsRequired(field);
        }
        TRequestResponse<typename WhiteboardResponse<TEvent>::Type> response(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, TypeName(*request.Get())));
        if (response.Span) {
            response.Span.Attribute("target_node_id", nodeId);
        }
        SubscribedNodeIds.push_back(nodeId);
        Send(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId, response.Span.GetTraceId());
        return response;
    }

    void RequestGenericNode(TNodeId nodeId) {
        if (NodeSystemState.count(nodeId) == 0) {
            NodeSystemState.emplace(nodeId, RequestNodeWhiteboard<TEvWhiteboard::TEvSystemStateRequest>(nodeId, {-1}));
            ++Requests;
        }
    }

    void RequestComputeNode(TNodeId nodeId) {
        if (ComputeNodeIds.emplace(nodeId).second) {
            RequestGenericNode(nodeId);
        }
    }

    void RequestStorageNode(TNodeId nodeId) {
        if (StorageNodeIds.emplace(nodeId).second) {
            RequestGenericNode(nodeId);
            if (NodeVDiskState.count(nodeId) == 0) {
                NodeVDiskState.emplace(nodeId, RequestNodeWhiteboard<TEvWhiteboard::TEvVDiskStateRequest>(nodeId));
                ++Requests;
            }
            if (NodePDiskState.count(nodeId) == 0) {
                NodePDiskState.emplace(nodeId, RequestNodeWhiteboard<TEvWhiteboard::TEvPDiskStateRequest>(nodeId));
                ++Requests;
            }
            if (NodeBSGroupState.count(nodeId) == 0) {
                NodeBSGroupState.emplace(nodeId, RequestNodeWhiteboard<TEvWhiteboard::TEvBSGroupStateRequest>(nodeId));
                ++Requests;
            }
        }
    }

    [[nodiscard]] TRequestResponse<TEvNodeWardenStorageConfig> RequestStorageConfig() {
        TRequestResponse<TEvNodeWardenStorageConfig> response(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, TypeName<TEvNodeWardenQueryStorageConfig>()));
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(false), 0/*flags*/, 0/*cookie*/, response.Span.GetTraceId());
        ++Requests;
        return response;
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        DatabaseBoardInfo->Set(std::move(ev));
        if (DatabaseBoardInfo->IsOk()) {
            TDatabaseState& database = DatabaseState[FilterDatabase];
            for (const auto& entry : DatabaseBoardInfo->Get()->InfoEntries) {
                if (!entry.second.Dropped) {
                    TNodeId nodeId = entry.first.NodeId();
                    RequestComputeNode(nodeId);
                    database.ComputeNodeIds.push_back(nodeId);
                }
            }
        }
        RequestDone("TEvStateStorage::TEvBoardInfo");
    }

    void Handle(TEvPrivate::TEvRetryNodeWhiteboard::TPtr& ev) {
        auto eventId = ev->Get()->EventId;
        auto nodeId = ev->Get()->NodeId;
        switch (eventId) {
            case TEvWhiteboard::EvSystemStateRequest:
                NodeSystemState.erase(nodeId);
                NodeSystemState[nodeId] = RequestNodeWhiteboard<TEvWhiteboard::TEvSystemStateRequest>(nodeId);
                break;
            case TEvWhiteboard::EvVDiskStateRequest:
                NodeVDiskState.erase(nodeId);
                NodeVDiskState[nodeId] = RequestNodeWhiteboard<TEvWhiteboard::TEvVDiskStateRequest>(nodeId);
                break;
            case TEvWhiteboard::EvPDiskStateRequest:
                NodePDiskState.erase(nodeId);
                NodePDiskState[nodeId] = RequestNodeWhiteboard<TEvWhiteboard::TEvPDiskStateRequest>(nodeId);
                break;
            case TEvWhiteboard::EvBSGroupStateRequest:
                NodeBSGroupState.erase(nodeId);
                NodeBSGroupState[nodeId] = RequestNodeWhiteboard<TEvWhiteboard::TEvBSGroupStateRequest>(nodeId);
                break;
            default:
                RequestDone("unsupported event scheduled");
                break;
        }
    }

    template<typename TEvent>
    bool RetryRequestNodeWhiteboard(TNodeId nodeId) {
        if (NodeRetries[{nodeId, TEvent::EventType}]++ < MaxRetries) {
            Schedule(RetryDelay, new TEvPrivate::TEvRetryNodeWhiteboard(nodeId, TEvent::EventType));
            return true;
        }
        return false;
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        TString error = "Undelivered";
        if (ev->Get()->SourceType == TEvWhiteboard::EvSystemStateRequest) {
            if (NodeSystemState.count(nodeId) && NodeSystemState[nodeId].Error(error)) {
                if (!RetryRequestNodeWhiteboard<TEvWhiteboard::TEvSystemStateRequest>(nodeId)) {
                    RequestDone("undelivered of TEvSystemStateRequest");
                    UnavailableComputeNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == TEvWhiteboard::EvVDiskStateRequest) {
            if (NodeVDiskState.count(nodeId) && NodeVDiskState[nodeId].Error(error)) {
                if (!RetryRequestNodeWhiteboard<TEvWhiteboard::TEvVDiskStateRequest>(nodeId)) {
                    RequestDone("undelivered of TEvVDiskStateRequest");
                    UnavailableStorageNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == TEvWhiteboard::EvPDiskStateRequest) {
            if (NodePDiskState.count(nodeId) && NodePDiskState[nodeId].Error(error)) {
                if (!RetryRequestNodeWhiteboard<TEvWhiteboard::TEvPDiskStateRequest>(nodeId)) {
                    RequestDone("undelivered of TEvPDiskStateRequest");
                    UnavailableStorageNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == TEvWhiteboard::EvBSGroupStateRequest) {
            if (NodeBSGroupState.count(nodeId) && NodeBSGroupState[nodeId].Error(error)) {
                if (!RetryRequestNodeWhiteboard<TEvWhiteboard::TEvBSGroupStateRequest>(nodeId)) {
                    RequestDone("undelivered of TEvBSGroupStateRequest");
                }
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        TString error = "NodeDisconnected";
        if (NodeSystemState.count(nodeId) && NodeSystemState[nodeId].Error(error)) {
            if (!RetryRequestNodeWhiteboard<TEvWhiteboard::TEvSystemStateRequest>(nodeId)) {
                RequestDone("node disconnected with TEvSystemStateRequest");
                UnavailableComputeNodes.insert(nodeId);
            }
        }
        if (NodeVDiskState.count(nodeId) && NodeVDiskState[nodeId].Error(error)) {
            if (!RetryRequestNodeWhiteboard<TEvWhiteboard::TEvVDiskStateRequest>(nodeId)) {
                RequestDone("node disconnected with TEvVDiskStateRequest");
                UnavailableStorageNodes.insert(nodeId);
            }
        }
        if (NodePDiskState.count(nodeId) && NodePDiskState[nodeId].Error(error)) {
            if (!RetryRequestNodeWhiteboard<TEvWhiteboard::TEvPDiskStateRequest>(nodeId)) {
                RequestDone("node disconnected with TEvPDiskStateRequest");
                UnavailableStorageNodes.insert(nodeId);
            }
        }
        if (NodeBSGroupState.count(nodeId) && NodeBSGroupState[nodeId].Error(error)) {
            if (!RetryRequestNodeWhiteboard<TEvWhiteboard::TEvBSGroupStateRequest>(nodeId)) {
                RequestDone("node disconnected with TEvBSGroupStateRequest");
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TTabletId tabletId = ev->Get()->TabletId;
            TString error = "Can't connect pipe";
            if (HiveNodeStats.count(tabletId) != 0) {
                if (HiveNodeStats[tabletId].Error(error)) {
                    if (FilterDatabase) {
                        DatabaseBoardInfo = MakeRequestStateStorageEndpointsLookup(FilterDatabase);
                    }
                }
            }
            if (HiveInfo.count(tabletId) != 0) {
                HiveInfo[tabletId].Error(error);
            }
            if (tabletId == ConsoleId && ListTenants) {
                ListTenants->Error(error);
            }
            if (tabletId == BsControllerId) {
                if (StoragePools) {
                    StoragePools->Error(error);
                }
                if (Groups) {
                    Groups->Error(error);
                }
                if (VSlots) {
                    VSlots->Error(error);
                }
                if (PDisks) {
                    PDisks->Error(error);
                }
                if (FilterDatabase.empty() || FilterDatabase == DomainPath) {
                    if (!NodeWardenStorageConfig) {
                        NodeWardenStorageConfig = RequestStorageConfig();
                    }
                }
            }
            for (const TString& path : TabletToDescribePath[tabletId]) {
                if (DescribeByPath.count(path) != 0) {
                    DescribeByPath[path].Error(error);
                }
            }
            if (GetPartitionStatsResult.count(tabletId) != 0) {
                GetPartitionStatsResult[tabletId].Error(error);
            }
            TabletRequests.TabletStates[tabletId].IsUnresponsive = true;
            for (const auto& [requestId, requestState] : TabletRequests.RequestsInFlight) {
                if (requestState.TabletId == tabletId) {
                    RequestDone("unsuccessful TEvClientConnected");
                }
            }
        }
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
            case TimeoutBSC:
                Span.Event("TimeoutBSC");
                if (!HaveAllBSControllerInfo()) {
                    if (FilterDatabase.empty() || FilterDatabase == DomainPath) {
                        if (!NodeWardenStorageConfig) {
                            NodeWardenStorageConfig = RequestStorageConfig();
                        }
                    }
                    TString error = "Timeout";
                    if (StoragePools && StoragePools->Error(error)) {
                        RequestDone("TEvGetStoragePoolsRequest");
                    }
                    if (Groups && Groups->Error(error)) {
                        RequestDone("TEvGetGroupsRequest");
                    }
                    if (VSlots && VSlots->Error(error)) {
                        RequestDone("TEvGetVSlotsRequest");
                    }
                    if (PDisks && PDisks->Error(error)) {
                        RequestDone("TEvGetPDisksRequest");
                    }
                }
                break;
            case TimeoutFinal:
                Span.Event("TimeoutFinal");
                ReplyAndPassAway();
                break;
        }
    }

    bool IsStaticNode(const TNodeId nodeId) const {
        TAppData* appData = AppData();
        if (appData->DynamicNameserviceConfig) {
            return nodeId <= AppData()->DynamicNameserviceConfig->MaxStaticNodeId;
        } else {
            return true;
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        bool needComputeFromStaticNodes = !IsSpecificDatabaseFilter();
        NodesInfo->Set(std::move(ev));
        for (const auto& ni : NodesInfo->Get()->Nodes) {
            MergedNodeInfo[ni.NodeId] = &ni;
            if (IsStaticNode(ni.NodeId) && needComputeFromStaticNodes) {
                DatabaseState[DomainPath].ComputeNodeIds.push_back(ni.NodeId);
                RequestComputeNode(ni.NodeId);
            }
        }
        RequestDone("TEvNodesInfo");
    }

    bool NeedWhiteboardForStaticGroupsWithUnknownStatus() {
        return NodeWardenStorageConfig && !IsSpecificDatabaseFilter();
    }

    void Handle(TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(TTabletRequestsState::RequestStoragePools);
        StoragePools->Set(std::move(ev));
        AggregateBSControllerState();
        RequestDone("TEvGetStoragePoolsRequest");
    }

    void Handle(TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(TTabletRequestsState::RequestGroups);
        Groups->Set(std::move(ev));
        AggregateBSControllerState();
        RequestDone("TEvGetGroupsRequest");
    }

    void Handle(TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(TTabletRequestsState::RequestVSlots);
        VSlots->Set(std::move(ev));
        AggregateBSControllerState();
        RequestDone("TEvGetVSlotsRequest");
    }

    void Handle(TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(TTabletRequestsState::RequestPDisks);
        PDisks->Set(std::move(ev));
        AggregateBSControllerState();
        RequestDone("TEvGetPDisksRequest");
    }

    void Handle(TEvSysView::TEvGetPartitionStatsResult::TPtr& ev) {
        //TTabletId schemeShardId = TabletRequests.CompleteRequest(ev->Cookie);
        TTabletId schemeShardId = TabletRequests.CompleteRequest(TTabletRequestsState::RequestGetPartitionStats);
        if (schemeShardId) {
            auto& partitionStatsResult(GetPartitionStatsResult[schemeShardId]);
            partitionStatsResult.Set(std::move(ev));
            if (partitionStatsResult.IsOk()) {
                std::map<double, std::pair<const NKikimrSysView::TPartitionStatsResult*, const NKikimrSysView::TPartitionStats*>> overloadedPaths;
                for (const NKikimrSysView::TPartitionStatsResult& statsPath : partitionStatsResult.Get()->Record.GetStats()) {
                    double cores = 0;
                    const NKikimrSysView::TPartitionStats* stats = nullptr;
                    for (const auto& statsShard : statsPath.GetStats()) {
                        if (statsShard.GetCPUCores() >= OVERLOADED_SHARDS_CPU_CORES && cores < statsShard.GetCPUCores()) {
                            cores = statsShard.GetCPUCores();
                            stats = &statsShard;
                        }
                    }
                    if (stats) {
                        overloadedPaths.emplace(cores, std::make_pair(&statsPath, stats));
                    }
                }
                for (auto rit = overloadedPaths.rbegin(); rit != overloadedPaths.rend(); ++rit) {
                    const NKikimrSysView::TPartitionStatsResult* statsPath = rit->second.first;
                    const NKikimrSysView::TPartitionStats* statsShard = rit->second.second;
                    TString path = statsPath->GetPath();
                    if (OverloadedShardHints.size() < MAX_OVERLOADED_SHARDS_HINTS) {
                        if (DescribeByPath.count(path) == 0) {
                            NKikimrSchemeOp::TDescribeOptions options;
                            options.SetReturnPartitioningInfo(true);
                            options.SetReturnPartitionConfig(true);
                            options.SetReturnChildren(false);
                            options.SetReturnRangeKey(false);
                            DescribeByPath[path] = RequestDescribe(schemeShardId, path, &options);
                            auto& hint = OverloadedShardHints[path];
                            hint.CPUCores = statsShard->GetCPUCores();
                            hint.TabletId = statsShard->GetTabletId();
                            hint.FollowerId = statsShard->GetFollowerId();
                            hint.SchemeShardId = schemeShardId;
                        }
                    }
                }
            }
        }
        RequestDone("TEvGetPartitionStatsResult");
    }

    bool IsMaximumShardsReached(const TEvSchemeShard::TEvDescribeSchemeResult& response) {
        const auto& description(response.GetRecord().GetPathDescription());
        const auto& policy(description.GetTable().GetPartitionConfig().GetPartitioningPolicy());

        return policy.HasMaxPartitionsCount()
            && policy.GetMaxPartitionsCount() != 0
            && policy.GetMaxPartitionsCount() <= description.TablePartitionsSize();
    }

    bool CheckOverloadedShardHint(const TEvSchemeShard::TEvDescribeSchemeResult& response, THintOverloadedShard& hint) {
        const auto& policy(response.GetRecord().GetPathDescription().GetTable().GetPartitionConfig().GetPartitioningPolicy());
        if (!policy.GetSplitByLoadSettings().GetEnabled()) {
            hint.Message = "Split by load is disabled on the table"; // do not change without changing the logic in the UI
            return true;
        }
        if (IsMaximumShardsReached(response)) {
            hint.Message = "The table has reached its maximum number of shards"; // do not change without changing the logic in the UI
            return true;
        }
        return false;
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        TString path = ev->Get()->GetRecord().path();
        auto& response = DescribeByPath[path];
        response.Set(std::move(ev));
        if (response.IsOk()) {
            auto itOverloadedShardHint = OverloadedShardHints.find(path);
            if (itOverloadedShardHint != OverloadedShardHints.end()) {
                if (!CheckOverloadedShardHint(*response.Get(), itOverloadedShardHint->second)) {
                    OverloadedShardHints.erase(itOverloadedShardHint);
                }
            } else {
                TDatabaseState& state(DatabaseState[path]);
                state.Path = path;
                for (const auto& storagePool : response.Get()->GetRecord().pathdescription().domaindescription().storagepools()) {
                    TString storagePoolName = storagePool.name();
                    state.StoragePoolNames.emplace(storagePoolName);
                    PathsByPoolName[storagePoolName].emplace(path); // no poolId in TEvDescribeSchemeResult, so it's neccesary to keep poolNames instead
                }
                if (path == DomainPath) {
                    state.StoragePoolNames.emplace(STATIC_STORAGE_POOL_NAME);
                    state.StoragePools.emplace(0); // static group has poolId = 0
                    StoragePoolState[0].Name = STATIC_STORAGE_POOL_NAME;
                }
                state.StorageUsage = response.Get()->GetRecord().pathdescription().domaindescription().diskspaceusage().tables().totalsize();
                state.StorageQuota = response.Get()->GetRecord().pathdescription().domaindescription().databasequotas().data_size_hard_quota();
            }
        }
        RequestDone("TEvDescribeSchemeResult");
    }

    bool NeedToAskHive(TTabletId hiveId) const {
        return HiveNodeStats.count(hiveId) == 0 || HiveInfo.count(hiveId) == 0;
    }

    void AskHive(const TString& database, TTabletId hiveId) {
        TabletRequests.TabletStates[hiveId].Database = database;
        TabletRequests.TabletStates[hiveId].Type = TTabletTypes::Hive;
        if (HiveNodeStats.count(hiveId) == 0) {
            HiveNodeStats[hiveId] = RequestHiveNodeStats(hiveId);
            ++HiveNodeStatsToGo;
        }
        if (HiveInfo.count(hiveId) == 0) {
            HiveInfo[hiveId] = RequestHiveInfo(hiveId);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>& response = NavigateKeySet[ev->Get()->Request->Cookie];
        response.Set(std::move(ev));
        if (response.IsOk()) {
            auto domainInfo = response.Get()->Request->ResultSet.begin()->DomainInfo;
            TString path = CanonizePath(response.Get()->Request->ResultSet.begin()->Path);
            if (domainInfo->IsServerless()) {
                if (NeedHealthCheckForServerless(domainInfo)) {
                    if (SharedDatabases.emplace(domainInfo->ResourcesDomainKey, path).second) {
                        RequestSchemeCacheNavigate(domainInfo->ResourcesDomainKey);
                    }
                    DatabaseState[path].ResourcePathId = domainInfo->ResourcesDomainKey;
                    DatabaseState[path].ServerlessComputeResourcesMode = domainInfo->ServerlessComputeResourcesMode;
                } else {
                    DatabaseState.erase(path);
                    RequestDone("TEvNavigateKeySetResult");
                    return;
                }
            }

            TSubDomainKey subDomainKey(domainInfo->DomainKey.OwnerId, domainInfo->DomainKey.LocalPathId);
            FilterDomainKey[subDomainKey] = path;

            TTabletId hiveId = domainInfo->Params.GetHive();
            if (hiveId) {
                DatabaseState[path].HiveId = hiveId;
                if (NeedToAskHive(hiveId)) {
                    AskHive(path, hiveId);
                }
            } else if (RootHiveId && NeedToAskHive(RootHiveId)) {
                DatabaseState[DomainPath].HiveId = RootHiveId;
                AskHive(DomainPath, RootHiveId);
            }

            TTabletId schemeShardId = domainInfo->Params.GetSchemeShard();
            if (!schemeShardId) {
                schemeShardId = RootSchemeShardId;
            } else {
                DatabaseState[path].SchemeShardId = schemeShardId;
                TabletRequests.TabletStates[schemeShardId].Database = path;
                TabletRequests.TabletStates[schemeShardId].Type = TTabletTypes::SchemeShard;
            }
            if (DescribeByPath.count(path) == 0) {
                DescribeByPath[path] = RequestDescribe(schemeShardId, path);
            }
            if (ReturnHints && schemeShardId != RootSchemeShardId) {
                if (GetPartitionStatsResult.count(schemeShardId) == 0) {
                    GetPartitionStatsResult[schemeShardId] = RequestPartitionStats(schemeShardId, subDomainKey);
                }
            }
        }
        RequestDone("TEvNavigateKeySetResult");
    }

    bool NeedHealthCheckForServerless(TIntrusivePtr<TDomainInfo> domainInfo) const {
        return IsSpecificDatabaseFilter()
            || domainInfo->ServerlessComputeResourcesMode == NKikimrSubDomains::EServerlessComputeResourcesModeExclusive;
    }

    void Handle(TEvHive::TEvResponseHiveNodeStats::TPtr& ev) {
        TTabletId hiveId = TabletRequests.CompleteRequest(ev->Cookie);
        TInstant aliveBarrier = TInstant::Now() - TDuration::Minutes(5);
        {
            auto& response = HiveNodeStats[hiveId];
            response.Set(std::move(ev));
            if (hiveId != RootHiveId) {
                for (const NKikimrHive::THiveNodeStats& hiveStat : response.Get()->Record.GetNodeStats()) {
                    if (hiveStat.HasNodeDomain()) {
                        TSubDomainKey domainKey(hiveStat.GetNodeDomain());
                        auto itFilterDomainKey = FilterDomainKey.find(domainKey);
                        if (itFilterDomainKey != FilterDomainKey.end()) {
                            if (!hiveStat.HasLastAliveTimestamp() || TInstant::MilliSeconds(hiveStat.GetLastAliveTimestamp()) > aliveBarrier) {
                                RequestComputeNode(hiveStat.GetNodeId());
                            }
                        }
                    }
                }
            }
        }
        --HiveNodeStatsToGo;
        if (HiveNodeStatsToGo == 0) {
            if (HiveNodeStats.count(RootHiveId) != 0 && HiveNodeStats[RootHiveId].IsOk()) {
                const auto& rootResponse(HiveNodeStats[RootHiveId]);
                for (const NKikimrHive::THiveNodeStats& hiveStat : rootResponse.Get()->Record.GetNodeStats()) {
                    if (hiveStat.HasNodeDomain()) {
                        TSubDomainKey domainKey(hiveStat.GetNodeDomain());
                        auto itFilterDomainKey = FilterDomainKey.find(domainKey);
                        if (itFilterDomainKey != FilterDomainKey.end()) {
                            if (!hiveStat.HasLastAliveTimestamp() || TInstant::MilliSeconds(hiveStat.GetLastAliveTimestamp()) > aliveBarrier) {
                                RequestComputeNode(hiveStat.GetNodeId());
                            }
                        }
                    }
                }
            }
        }
        RequestDone("TEvResponseHiveNodeStats");
    }

    void Handle(TEvHive::TEvResponseHiveInfo::TPtr& ev) {
        TTabletId hiveId = TabletRequests.CompleteRequest(ev->Cookie);
        HiveInfo[hiveId].Set(std::move(ev));
        RequestDone("TEvResponseHiveInfo");
    }

    void Handle(TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        ListTenants->Set(std::move(ev));
        RequestSchemeCacheNavigate(DomainPath);
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ListTenants->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            DatabaseState[path];
            RequestSchemeCacheNavigate(path);
        }
        RequestDone("TEvListTenantsResponse");
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        TNodeId nodeId = ev.Get()->Cookie;
        auto& nodeSystemState(NodeSystemState[nodeId]);
        nodeSystemState.Set(std::move(ev));
        for (NKikimrWhiteboard::TSystemStateInfo& state : *nodeSystemState->Record.MutableSystemStateInfo()) {
            state.set_nodeid(nodeId);
            MergedNodeSystemState[nodeId] = &state;
        }
        RequestDone("TEvSystemStateResponse");
    }

    void AggregateHiveInfo() {
        TNodeTabletState::TTabletStateSettings settings;
        for (auto& [dbPath, dbState] : DatabaseState) {
            const auto& hiveResponse = HiveInfo[dbState.HiveId];
            if (hiveResponse.IsOk()) {
                settings.AliveBarrier = TInstant::MilliSeconds(hiveResponse->Record.GetResponseTimestamp()) - TDuration::Minutes(5);
                settings.MaxRestartsPerPeriod = HealthCheckConfig.GetThresholds().GetTabletsRestartsOrange();
                for (const NKikimrHive::TTabletInfo& hiveTablet : hiveResponse->Record.GetTablets()) {
                    TSubDomainKey tenantId = TSubDomainKey(hiveTablet.GetObjectDomain());
                    auto itDomain = FilterDomainKey.find(tenantId);
                    TDatabaseState* database = nullptr;
                    if (itDomain == FilterDomainKey.end()) {
                        if (!FilterDatabase || FilterDatabase == dbPath) {
                            database = &dbState;
                        } else {
                            continue;
                        }
                    } else {
                        auto itDatabase = DatabaseState.find(itDomain->second);
                        if (itDatabase != DatabaseState.end()) {
                            database = &itDatabase->second;
                        } else {
                            continue;
                        }
                    }
                    auto tabletId = std::make_pair(hiveTablet.GetTabletID(), hiveTablet.GetFollowerID());
                    database->MergedTabletState.emplace(tabletId, &hiveTablet);
                    TNodeId nodeId = hiveTablet.GetNodeID();
                    switch (hiveTablet.GetVolatileState()) {
                        case NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_STARTING:
                        case NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_RUNNING:
                            break;
                        default:
                            nodeId = 0;
                            break;
                    }
                    database->MergedNodeTabletState[nodeId].AddTablet(hiveTablet, settings);
                }
            }
        }
    }

    void AggregateHiveNodeStats() {
        for (const auto& [hiveId, hiveResponse] : HiveNodeStats) {
            if (hiveResponse.IsOk()) {
                for (const NKikimrHive::THiveNodeStats& hiveStat : hiveResponse->Record.GetNodeStats()) {
                    if (hiveStat.HasNodeDomain()) {
                        TSubDomainKey domainKey(hiveStat.GetNodeDomain());
                        auto itFilterDomainKey = FilterDomainKey.find(domainKey);
                        if (itFilterDomainKey != FilterDomainKey.end()) {
                            TString path(itFilterDomainKey->second);
                            TDatabaseState& state(DatabaseState[path]);
                            state.ComputeNodeIds.emplace_back(hiveStat.GetNodeId());
                            state.NodeRestartsPerPeriod[hiveStat.GetNodeId()] = hiveStat.GetRestartsPerPeriod();
                        }
                    }
                }
            }
        }
    }

    void AggregateStoragePools() {
        for (const auto& [poolId, pool] : StoragePoolState) {
            for (const TString& path : PathsByPoolName[pool.Name]) {
                DatabaseState[path].StoragePools.emplace(poolId);
            }
        }
    }

    void AggregateBSControllerState() {
        if (!HaveAllBSControllerInfo()) {
            return;
        }
        for (const auto& group : Groups->Get()->Record.GetEntries()) {
            auto groupId = group.GetKey().GetGroupId();
            auto poolId = group.GetInfo().GetStoragePoolId();
            auto& groupState = GroupState[groupId];
            groupState.ErasureSpecies = group.GetInfo().GetErasureSpeciesV2();
            groupState.Generation = group.GetInfo().GetGeneration();
            groupState.LayoutCorrect = group.GetInfo().GetLayoutCorrect();
            StoragePoolState[poolId].Groups.emplace(groupId);
        }
        for (const auto& vSlot : VSlots->Get()->Record.GetEntries()) {
            auto vSlotId = GetVSlotId(vSlot.GetKey());
            auto groupStateIt = GroupState.find(vSlot.GetInfo().GetGroupId());
            if (groupStateIt != GroupState.end() && vSlot.GetInfo().GetGroupGeneration() == groupStateIt->second.Generation) {
                groupStateIt->second.VSlots.push_back(&vSlot);
            }
        }
        for (const auto& pool : StoragePools->Get()->Record.GetEntries()) { // there is no specific pool for static group here
            ui64 poolId = pool.GetKey().GetStoragePoolId();
            TString storagePoolName = pool.GetInfo().GetName();
            StoragePoolState[poolId].Name = storagePoolName;
        }
        for (const auto& pDisk : PDisks->Get()->Record.GetEntries()) {
            auto pDiskId = GetPDiskId(pDisk.GetKey());
            PDisksMap.emplace(pDiskId, &pDisk);
        }
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        TNodeId nodeId = ev.Get()->Cookie;
        auto& nodeVDiskState(NodeVDiskState[nodeId]);
        nodeVDiskState.Set(std::move(ev));
        for (NKikimrWhiteboard::TVDiskStateInfo& state : *nodeVDiskState->Record.MutableVDiskStateInfo()) {
            state.set_nodeid(nodeId);
            auto id = GetVDiskId(state.vdiskid());
            MergedVDiskState[id] = &state;
        }
        RequestDone("TEvVDiskStateResponse");
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        TNodeId nodeId = ev.Get()->Cookie;
        auto& nodePDiskState(NodePDiskState[nodeId]);
        nodePDiskState.Set(std::move(ev));
        for (NKikimrWhiteboard::TPDiskStateInfo& state : *nodePDiskState->Record.MutablePDiskStateInfo()) {
            state.set_nodeid(nodeId);
            auto id = GetPDiskId(state);
            MergedPDiskState[id] = &state;
        }
        RequestDone("TEvPDiskStateResponse");
    }

    void Handle(TEvWhiteboard::TEvBSGroupStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        auto& nodeBSGroupState(NodeBSGroupState[nodeId]);
        nodeBSGroupState.Set(std::move(ev));
        for (NKikimrWhiteboard::TBSGroupStateInfo& state : *nodeBSGroupState->Record.MutableBSGroupStateInfo()) {
            state.set_nodeid(nodeId);
            TString storagePoolName = state.storagepoolname();
            TGroupID groupId(state.groupid());
            const NKikimrWhiteboard::TBSGroupStateInfo*& current(MergedBSGroupState[state.groupid()]);
            if (current == nullptr || current->GetGroupGeneration() < state.GetGroupGeneration()) {
                current = &state;
            }
            if (storagePoolName.empty() && groupId.ConfigurationType() != EGroupConfigurationType::Static) {
                continue;
            }
            StoragePoolStateByName[storagePoolName].Groups.emplace(state.groupid());
            StoragePoolStateByName[storagePoolName].Name = storagePoolName;
        }
        RequestDone("TEvBSGroupStateResponse");
    }

    bool TruncateIssuesWithStatusWhileBeyondLimit(Ydb::Monitoring::SelfCheckResult& result, ui64 byteLimit, Ydb::Monitoring::StatusFlag::Status status) {
        auto byteResult = result.ByteSizeLong();
        if (byteResult <= byteLimit) {
            return true;
        }

        int totalIssues = result.issue_log_size();
        TVector<int> indexesToRemove;
        for (int i = 0; i < totalIssues && byteResult > byteLimit; ++i) {
            if (result.issue_log(i).status() == status) {
                byteResult -= result.issue_log(i).ByteSizeLong();
                indexesToRemove.push_back(i);
            }
        }

        for (auto it = indexesToRemove.rbegin(); it != indexesToRemove.rend(); ++it) {
            result.mutable_issue_log()->SwapElements(*it, result.issue_log_size() - 1);
            result.mutable_issue_log()->RemoveLast();
        }

        return byteResult <= byteLimit;
    }

    void TruncateIssuesIfBeyondLimit(Ydb::Monitoring::SelfCheckResult& result, ui64 byteLimit) {
        auto truncateStatusPriority = {
            Ydb::Monitoring::StatusFlag::BLUE,
            Ydb::Monitoring::StatusFlag::YELLOW,
            Ydb::Monitoring::StatusFlag::ORANGE,
            Ydb::Monitoring::StatusFlag::RED
        };
        for (Ydb::Monitoring::StatusFlag::Status truncateStatus: truncateStatusPriority) {
            if (TruncateIssuesWithStatusWhileBeyondLimit(result, byteLimit, truncateStatus)) {
                break;
            }
        }
    }

    struct TCtx;
    using TCtx = TBuilderContext<
        TBuilderResult<TCtx>,
        TBuilderDatabase<TCtx>,
        TBuilderCompute<TCtx>,
        TBuilderSystemTablets<TCtx>,
        TBuilderTablets<TCtx>,
        TBuilderStorage<TCtx>,
        TBuilderStoragePool<TCtx>,
        TBuilderStorageGroup<TCtx>,
        TBuilderStorageVDisk<TCtx>,
        TBuilderStoragePDisk<TCtx>,
        TBuilderStorageGroupWithWhiteboard<TCtx>,
        TBuilderStorageVDiskWithWhiteboard<TCtx>,
        TBuilderStoragePDiskWithWhiteboard<TCtx>
    >;

    THolder<TCtx> MakeBuilderContext() {
        auto ctx = MakeHolder<TCtx>();
        ctx->BuilderResult = MakeHolder<TBuilderResult<TCtx>>(*ctx, domainPath, filterDatabase, haveAllBSControllerInfo,
                                                            databaseState, storagePoolState, storagePoolSeen,
                                                            storagePoolStateByName, storagePoolSeenByName);
        ctx->BuilderDatabase = MakeHolder<TBuilderDatabase<TCtx>>(*ctx, isSpecificDatabaseFilter);
        ctx->BuilderCompute = MakeHolder<TBuilderCompute<TCtx>>(*ctx, filterDomainKey, mergedNodeSystemState);
        ctx->BuilderSystemTablets = MakeHolder<TBuilderSystemTablets<TCtx>>(*ctx);
        ctx->BuilderTablets = MakeHolder<TBuilderTablets<TCtx>>(*ctx);
        ctx->BuilderStorage = MakeHolder<TBuilderStorage<TCtx>>(*ctx);
        ctx->BuilderStoragePool = MakeHolder<TBuilderStoragePool<TCtx>>(*ctx);
        ctx->BuilderStorageGroup = MakeHolder<TBuilderStorageGroup<TCtx>>(*ctx);
        ctx->BuilderStorageVDisk = MakeHolder<TBuilderStorageVDisk<TCtx>>(*ctx);
        ctx->BuilderStoragePDisk = MakeHolder<TBuilderStoragePDisk<TCtx>>(*ctx);
        ctx->BuilderStorageGroupWithWhiteboard = MakeHolder<TBuilderStorageGroupWithWhiteboard<TCtx>>(*ctx);
        ctx->BuilderStorageVDiskWithWhiteboard = MakeHolder<TBuilderStorageVDiskWithWhiteboard<TCtx>>(*ctx);
        ctx->BuilderStoragePDiskWithWhiteboard = MakeHolder<TBuilderStoragePDiskWithWhiteboard<TCtx>>(*ctx);
    }

    void ReplyAndPassAway() {
        Span.Event("ReplyAndPassAway");
        THolder<TEvSelfCheckResult> response = MakeHolder<TEvSelfCheckResult>();
        Ydb::Monitoring::SelfCheckResult& result = builder;

        AggregateHiveInfo();
        AggregateHiveNodeStats();
        AggregateStoragePools();

        for (auto& [requestId, request] : TabletRequests.RequestsInFlight) {
            auto tabletId = request.TabletId;
            TabletRequests.TabletStates[tabletId].IsUnresponsive = true;
        }

        auto builder = MakeBuilder();
        builder.Result.Build({&result});
        RemoveUnrequestedEntries(result, Request->Request);

        FillNodeInfo(SelfId().NodeId(), result.mutable_location());

        auto byteSize = result.ByteSizeLong();
        auto byteLimit = 50_MB - 1_KB; // 1_KB - for HEALTHCHECK STATUS issue going last
        TruncateIssuesIfBeyondLimit(result, byteLimit);

        if (byteSize > 30_MB) {
            auto* issue = result.add_issue_log();
            issue->set_type("HEALTHCHECK_STATUS");
            issue->set_level(1);
            if (byteSize > byteLimit) {
                issue->set_status(Ydb::Monitoring::StatusFlag::RED);
                issue->set_message("Healthcheck response size exceeds 50 MB and will be truncated");
            } else if (byteSize > 40_MB) {
                issue->set_status(Ydb::Monitoring::StatusFlag::ORANGE);
                issue->set_message("Healthcheck response size exceeds 40 MB");
            } else if (byteSize > 30_MB) {
                issue->set_status(Ydb::Monitoring::StatusFlag::YELLOW);
                issue->set_message("Healthcheck response size exceeds 30 MB");
            }
            TStringStream id;
            id << Ydb::Monitoring::StatusFlag_Status_Name(issue->status());
            id << '-' << TSelfCheckResult::crc16(issue->message());
            issue->set_id(id.Str());
        }

        Send(Sender, response.Release(), 0, Cookie);

        for (TActorId pipe : PipeClients) {
            NTabletPipe::CloseClient(SelfId(), pipe);
        }
        for (TNodeId nodeId : SubscribedNodeIds) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        PassAway();
    }

    class TSelfCheckBuilder {
public:
    THolder<IBuilderResult> Result;
    THolder<IBuilderDatabase> Database;
    THolder<IBuilderCompute> Compute;
    THolder<IBuilderComputeDatabase> ComputeDatabase;
    THolder<IBuilderComputeNode> ComputeNode;
    THolder<IBuilderStorage> Storage;
    THolder<IBuilderStoragePool> StoragePool;
    THolder<IBuilderStorageGroup> StorageGroup;
    THolder<IBuilderStorageVDisk> StorageVDisk;
    THolder<IBuilderStoragePDisk> StoragePDisk;
    THolder<IBuilderStorageGroupWithWhiteboard> StorageGroupWithWhiteboard;
    THolder<IBuilderStorageVDiskWithWhiteboard> StorageVDiskWithWhiteboard;
    THolder<IBuilderStoragePDiskWithWhiteboard> StoragePDiskWithWhiteboard;

    void Init(THolder<IBuilderResult> result,
                      THolder<IBuilderDatabase> database,
                      THolder<IBuilderCompute> compute,
                      THolder<IBuilderComputeDatabase> computeDatabase,
                      THolder<IBuilderComputeNode> computeNode,
                      THolder<IBuilderStorage> storage,
                      THolder<IBuilderStoragePool> storagePool,
                      THolder<IBuilderStorageGroup> storageGroup,
                      THolder<IBuilderStorageGroupWithWhiteboard> storageGroupWithWhiteboard,
                      THolder<IBuilderStoragePDisk> storagePDisk,
                      THolder<IBuilderStoragePDiskWithWhiteboard> storagePDiskWithWhiteboard,
                      THolder<IBuilderStorageVDisk> storageVDisk,
                      THolder<IBuilderStorageVDiskWithWhiteboard> storageVDiskWithWhiteboard) {
        Result = std::move(result);
        Database = std::move(database);
        Compute = std::move(compute);
        ComputeDatabase = std::move(computeDatabase);
        ComputeNode = std::move(computeNode);
        Storage = std::move(storage);
        StoragePool = std::move(storagePool);
        StorageGroup = std::move(storageGroup);
        StorageGroupWithWhiteboard = std::move(storageGroupWithWhiteboard);
        StoragePDisk = std::move(storagePDisk);
        StoragePDiskWithWhiteboard = std::move(storagePDiskWithWhiteboard);
        StorageVDisk = std::move(storageVDisk);
        StorageVDiskWithWhiteboard = std::move(storageVDiskWithWhiteboard);
    }
};
};

} // NKikimr::NHealthCheck
