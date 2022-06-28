#include "health_check.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/digest/old_crc/crc.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/grpc/client/grpc_client_low.h>

#include <util/random/shuffle.h>

#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/core/util/tuples.h>

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>

static decltype(auto) make_vslot_tuple(const NKikimrBlobStorage::TVSlotId& id) {
    return std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId());
}

namespace std {

template <>
struct equal_to<NKikimrBlobStorage::TVSlotId> {
    bool operator ()(const NKikimrBlobStorage::TVSlotId& a, const NKikimrBlobStorage::TVSlotId& b) const {
        return make_vslot_tuple(a) == make_vslot_tuple(b);
    }
};

template <>
struct hash<NKikimrBlobStorage::TVSlotId> {
    size_t operator ()(const NKikimrBlobStorage::TVSlotId& a) const {
        auto tp = make_vslot_tuple(a);
        return hash<decltype(tp)>()(tp);
    }
};

}

#define BLOG_CRIT(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::HEALTH, stream)

namespace NKikimr {
namespace NHealthCheck {

using namespace NActors;
using namespace Ydb;

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

    TSelfCheckRequest(const TActorId& sender, THolder<TEvSelfCheckRequest> request, ui64 cookie)
        : Sender(sender)
        , Request(std::move(request))
        , Cookie(cookie)
    {}

    using TGroupId = ui32;

    struct TTenantInfo {
        TString Name;
        Ydb::Cms::GetDatabaseStatusResult::State State;
    };

    struct TNodeTabletState {
        struct TTabletStateSettings {
            TInstant AliveBarrier;
            ui32 MaxRestartsPerPeriod = 30; // per hour
            ui32 MaxTabletIdsStored = 10;
            bool ReportGoodTabletsIds = false;
        };

        enum class ETabletState {
            Good,
            Stopped,
            RestartsTooOften,
            Dead,
        };

        struct TNodeTabletStateCount {
            NKikimrTabletBase::TTabletTypes::EType Type;
            ETabletState State;
            int Count = 1;
            TStackVec<TString> Identifiers;

            TNodeTabletStateCount(const NKikimrHive::TTabletInfo& info, const TTabletStateSettings& settings) {
                Type = info.tablettype();
                if (info.volatilestate() == NKikimrHive::TABLET_VOLATILE_STATE_STOPPED) {
                    State = ETabletState::Stopped;
                } else if (info.volatilestate() != NKikimrHive::TABLET_VOLATILE_STATE_RUNNING
                            && TInstant::MilliSeconds(info.lastalivetimestamp()) < settings.AliveBarrier
                            && info.tabletbootmode() == NKikimrHive::TABLET_BOOT_MODE_DEFAULT) {
                    State = ETabletState::Dead;
                } else if (info.restartsperperiod() >= settings.MaxRestartsPerPeriod) {
                    State = ETabletState::RestartsTooOften;
                } else {
                    State = ETabletState::Good;
                }
            }

            bool operator ==(const TNodeTabletStateCount& o) const {
                return State == o.State && Type == o.Type;
            }
        };

        TStackVec<TNodeTabletStateCount> Count;

        void AddTablet(const NKikimrHive::TTabletInfo& info, const TTabletStateSettings& settings) {
            TNodeTabletStateCount tabletState(info, settings);
            auto itCount = Find(Count, tabletState);
            if (itCount != Count.end()) {
                itCount->Count++;
            } else {
                Count.emplace_back(tabletState);
                itCount = std::prev(Count.end());
            }
            if (itCount->State != ETabletState::Good || settings.ReportGoodTabletsIds) {
                if (itCount->Identifiers.size() < settings.MaxTabletIdsStored) {
                    TStringBuilder id;
                    id << info.tabletid();
                    if (info.followerid()) {
                        id << '.' << info.followerid();
                    }
                    itCount->Identifiers.emplace_back(id);
                }
            }
        }
    };

    struct TStoragePoolState {
        TString Kind;
        THashSet<TGroupId> Groups;
        THashSet<TGroupId> AuthenticGroups;
    };

    struct TDatabaseState {
        TTabletId HiveId = {};
        TPathId ResourcePathId = {};
        TVector<TNodeId> ComputeNodeIds;
        TVector<TString> StoragePoolNames;
        THashMap<std::pair<TTabletId, TFollowerId>, const NKikimrHive::TTabletInfo*> MergedTabletState;
        THashMap<TNodeId, TNodeTabletState> MergedNodeTabletState;
    };

    struct TSelfCheckResult {
        struct TIssueRecord {
            Ydb::Monitoring::IssueLog IssueLog;
            TString Tag;
        };

        Ydb::Monitoring::StatusFlag::Status OverallStatus = Ydb::Monitoring::StatusFlag::GREY;
        TList<TIssueRecord> IssueLog;
        Ydb::Monitoring::Location Location;
        int Level = 1;
        TString Type;

        static bool IsErrorStatus(Ydb::Monitoring::StatusFlag::Status status) {
            return status != Ydb::Monitoring::StatusFlag::GREEN;
        }

        static TString crc16(const TString& data) {
            return Sprintf("%04x", (ui32)::crc16(data.data(), data.size()));
        }

        static TString GetIssueId(const Ydb::Monitoring::IssueLog& issueLog) {
            TStringStream id;
            id << Ydb::Monitoring::StatusFlag_Status_Name(issueLog.status());
            id << '-' << crc16(issueLog.message());
            const Ydb::Monitoring::Location& location(issueLog.location());
            if (location.storage().node().id()) {
                id << '-' << location.storage().node().id();
            } else {
                if (location.storage().node().host()) {
                    id << '-' << location.storage().node().host();
                }
                if (location.storage().node().port()) {
                    id << '-' << location.storage().node().port();
                }
            }
            if (location.storage().pool().group().vdisk().id()) {
                id << '-' << location.storage().pool().group().vdisk().id();
            } else {
                if (location.storage().pool().group().id()) {
                    id << '-' << location.storage().pool().group().id();
                } else {
                    if (location.storage().pool().name()) {
                        id << '-' << crc16(location.storage().pool().name());
                    }
                }
            }
            if (location.storage().pool().group().vdisk().pdisk().id()) {
                id << '-' << location.storage().pool().group().vdisk().pdisk().id();
            }
            if (location.compute().node().id()) {
                id << '-' << location.compute().node().id();
            } else {
                if (location.compute().node().host()) {
                    id << '-' << location.compute().node().host();
                }
                if (location.compute().node().port()) {
                    id << '-' << location.compute().node().port();
                }
            }
            if (location.compute().pool().name()) {
                id << '-' << location.compute().pool().name();
            }
            if (location.compute().tablet().type()) {
                id << '-' << location.compute().tablet().type();
            }
            return id.Str();
        }

        void ReportStatus(Ydb::Monitoring::StatusFlag::Status status,
                          const TString& message = {},
                          const TString& setTag = {},
                          std::initializer_list<TString> includeTags = {}) {
            OverallStatus = MaxStatus(OverallStatus, status);
            if (IsErrorStatus(status)) {
                std::vector<TString> reason;
                if (includeTags.size() != 0) {
                    for (const TIssueRecord& record : IssueLog) {
                        for (const TString& tag : includeTags) {
                            if (record.Tag == tag) {
                                reason.push_back(record.IssueLog.id());
                                break;
                            }
                        }
                    }
                }
                std::sort(reason.begin(), reason.end());
                reason.erase(std::unique(reason.begin(), reason.end()), reason.end());
                TIssueRecord& issueRecord(*IssueLog.emplace(IssueLog.begin()));
                Ydb::Monitoring::IssueLog& issueLog(issueRecord.IssueLog);
                issueLog.set_status(status);
                issueLog.set_message(message);
                if (Location.ByteSizeLong() > 0) {
                    issueLog.mutable_location()->CopyFrom(Location);
                }
                issueLog.set_id(GetIssueId(issueLog));
                if (Type) {
                    issueLog.set_type(Type);
                }
                issueLog.set_level(Level);
                if (!reason.empty()) {
                    for (const TString& r : reason) {
                        issueLog.add_reason(r);
                    }
                }
                if (setTag) {
                    issueRecord.Tag = setTag;
                }
            }
        }

        bool HasTags(std::initializer_list<TString> tags) const {
            for (const TIssueRecord& record : IssueLog) {
                for (const TString& tag : tags) {
                    if (record.Tag == tag) {
                        return true;
                    }
                }
            }
            return false;
        }

        Ydb::Monitoring::StatusFlag::Status GetOverallStatus() const {
            return OverallStatus;
        }

        void SetOverallStatus(Ydb::Monitoring::StatusFlag::Status status) {
            OverallStatus = status;
        }

        void InheritFrom(TSelfCheckResult& lower) {
            if (lower.GetOverallStatus() >= OverallStatus) {
                OverallStatus = lower.GetOverallStatus();
            }
            IssueLog.splice(IssueLog.end(), std::move(lower.IssueLog));
        }
    };

    struct TSelfCheckContext : TSelfCheckResult {
        TSelfCheckResult* Upper;

        TSelfCheckContext(TSelfCheckResult* upper)
            : Upper(upper)
        {
            Location.CopyFrom(upper->Location);
            Level = upper->Level + 1;
        }

        TSelfCheckContext(TSelfCheckResult* upper, const TString& type)
            : TSelfCheckContext(upper)
        {
            Type = type;
        }

        TSelfCheckContext(const TSelfCheckContext&) = delete;

        ~TSelfCheckContext() {
            Upper->InheritFrom(*this);
        }
    };

    TString FilterDatabase;
    THashMap<TSubDomainKey, TString> FilterDomainKey;
    TVector<TActorId> PipeClients;
    int Requests = 0;
    TString DomainPath;
    TTabletId ConsoleId;
    TTabletId BsControllerId;
    TTabletId RootSchemeShardId;
    TTabletId RootHiveId;
    THashMap<TString, TTenantInfo> TenantByPath;
    THashMap<TString, THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>> DescribeByPath;
    THashMap<TString, Ydb::Cms::GetDatabaseStatusResult> DatabaseStatusByPath;
    THashMap<TString, THolder<NTenantSlotBroker::TEvTenantSlotBroker::TEvTenantState>> TenantStateByPath;
    THashMap<TString, THolder<NSchemeCache::TSchemeCacheNavigate>> NavigateResult;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveDomainStats>> HiveDomainStats;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStats;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveInfo>> HiveInfo;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    THashMap<TNodeId, const TEvInterconnect::TNodeInfo*> MergedNodeInfo;
    THolder<TEvBlobStorage::TEvControllerConfigResponse> BaseConfig;

    THashSet<TNodeId> NodeIds;
    THashSet<TNodeId> StorageNodeIds;
    THashSet<TNodeId> ComputeNodeIds;
    std::unordered_map<std::pair<TNodeId, int>, ui32> NodeRetries;
    ui32 MaxRetries = 3;
    TDuration RetryDelay = TDuration::MilliSeconds(250);

    THashMap<TString, TDatabaseState> DatabaseState;
    THashMap<TPathId, TString> SharedDatabases;

    THashMap<TNodeId, THolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse>> NodeSystemState;
    THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*> MergedNodeSystemState;

    THashMap<TNodeId, THolder<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse>> NodeVDiskState;
    TList<NKikimrWhiteboard::TVDiskStateInfo> VDisksAppended;
    std::unordered_map<TString, const NKikimrWhiteboard::TVDiskStateInfo*> MergedVDiskState;
    std::unordered_set<TString> ValidVDisks;

    THashMap<TNodeId, THolder<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse>> NodePDiskState;
    TList<NKikimrWhiteboard::TPDiskStateInfo> PDisksAppended;
    std::unordered_map<TString, const NKikimrWhiteboard::TPDiskStateInfo*> MergedPDiskState;
    std::unordered_set<TString> ValidPDisks;

    THashMap<TNodeId, THolder<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse>> NodeBSGroupState;
    TList<NKikimrWhiteboard::TBSGroupStateInfo> BSGroupAppended;
    std::unordered_map<TGroupId, const NKikimrWhiteboard::TBSGroupStateInfo*> MergedBSGroupState;
    std::unordered_set<TGroupId> ValidGroups;

    THashMap<TString, TStoragePoolState> StoragePoolState;
    THashSet<TString> StoragePoolSeen;

    THashSet<TNodeId> UnavailableStorageNodes;
    THashSet<TNodeId> UnavailableComputeNodes;

    struct TTabletRequestsState {
        struct TTabletState {
            TTabletTypes::EType Type = TTabletTypes::Unknown;
            TString Database;
            bool IsUnresponsive = false;
            TDuration MaxResponseTime;
            TActorId TabletPipe = {};
        };

        struct TRequestState {
            TTabletId TabletId;
            TString Key;
            TMonotonic StartTime;
        };

        std::unordered_map<TTabletId, TTabletState> TabletStates;
        std::unordered_map<ui64, TRequestState> RequestsInFlight;
        ui64 RequestId = 0;

        ui64 MakeRequest(TTabletId tabletId, const TString& key) {
            ++RequestId;
            RequestsInFlight.emplace(RequestId, TRequestState{tabletId, key, TMonotonic::Now()});
            return RequestId;
        }

        TTabletId CompleteRequest(ui64 requestId) {
            TTabletId tabletId = {};
            TMonotonic finishTime = TMonotonic::Now();
            auto itRequest = RequestsInFlight.find(requestId);
            if (itRequest != RequestsInFlight.end()) {
                TDuration responseTime = finishTime - itRequest->second.StartTime;
                tabletId = itRequest->second.TabletId;
                TTabletState& tabletState = TabletStates[tabletId];
                if (responseTime > tabletState.MaxResponseTime) {
                    tabletState.MaxResponseTime = responseTime;
                }
                RequestsInFlight.erase(itRequest);
            }
            return tabletId;
        }
    };

    TTabletRequestsState TabletRequests;

    TDuration Timeout = TDuration::MilliSeconds(10000);

    static constexpr TStringBuf STATIC_STORAGE_POOL_NAME = "static";

    void Bootstrap() {
        FilterDatabase = Request->Database;
        if (Request->Request.operation_params().has_operation_timeout()) {
            Timeout = GetDuration(Request->Request.operation_params().operation_timeout());
        }
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        TIntrusivePtr<TDomainsInfo::TDomain> domain = domains->Domains.begin()->second;
        DomainPath = "/" + domain->Name;
        RootSchemeShardId = domain->SchemeRoot;
        auto group = domains->GetDefaultStateStorageGroup(domain->DomainUid);
        ConsoleId = MakeConsoleID(group);
        RootHiveId = domains->GetHive(domain->DefaultHiveUid);
        BsControllerId = MakeBSControllerID(group);

        if (ConsoleId) {
            TabletRequests.TabletStates[ConsoleId].Database = DomainPath;
            TabletRequests.TabletStates[ConsoleId].Type = TTabletTypes::Console;
            if (FilterDatabase) {
                if (FilterDatabase != DomainPath) {
                    RequestTenantStatus(FilterDatabase);
                } else {
                    TTenantInfo& tenant = TenantByPath[DomainPath];
                    tenant.Name = DomainPath;
                    RequestSchemeCacheNavigate(DomainPath);
                }
            } else {
                TTenantInfo& tenant = TenantByPath[DomainPath];
                tenant.Name = DomainPath;
                RequestSchemeCacheNavigate(DomainPath);
                RequestListTenants();
            }
        }

        if (RootHiveId) {
            TabletRequests.TabletStates[RootHiveId].Database = DomainPath;
            TabletRequests.TabletStates[RootHiveId].Type = TTabletTypes::Hive;
            //RequestHiveDomainStats(RootHiveId);
            RequestHiveNodeStats(RootHiveId);
            RequestHiveInfo(RootHiveId);
        }

        if (RootSchemeShardId && (!FilterDatabase || FilterDatabase == DomainPath)) {
            TabletRequests.TabletStates[RootSchemeShardId].Database = DomainPath;
            TabletRequests.TabletStates[RootSchemeShardId].Type = TTabletTypes::SchemeShard;
            RequestDescribe(RootSchemeShardId, DomainPath);
        }

        if (BsControllerId) {
            TabletRequests.TabletStates[BsControllerId].Database = DomainPath;
            TabletRequests.TabletStates[BsControllerId].Type = TTabletTypes::BSController;
            RequestConfig();
        }

        const NKikimrBlobStorage::TNodeWardenServiceSet& staticConfig = *AppData()->StaticBlobStorageConfig.Get();
        for (const NKikimrBlobStorage::TNodeWardenServiceSet_TPDisk& pDisk : staticConfig.pdisks()) {
            auto pDiskId = GetPDiskId(pDisk);
            ValidPDisks.emplace(pDiskId);
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
                RequestStorageNode(pDisk.GetNodeID());
            }
        }
        for (const NKikimrBlobStorage::TNodeWardenServiceSet_TVDisk& vDisk : staticConfig.vdisks()) {
            auto vDiskId = GetVDiskId(vDisk);
            ValidVDisks.emplace(vDiskId);
            auto itVDisk = MergedVDiskState.find(vDiskId);
            if (itVDisk == MergedVDiskState.end()) {
                VDisksAppended.emplace_back();
                NKikimrWhiteboard::TVDiskStateInfo& pbVDisk = VDisksAppended.back();
                itVDisk = MergedVDiskState.emplace(vDiskId, &pbVDisk).first;
                pbVDisk.MutableVDiskId()->CopyFrom(vDisk.vdiskid());
                pbVDisk.SetNodeId(vDisk.GetVDiskLocation().GetNodeID());
                pbVDisk.SetPDiskId(vDisk.GetVDiskLocation().GetPDiskID());
            }
        }
        for (const NKikimrBlobStorage::TGroupInfo& group : staticConfig.groups()) {
            ValidGroups.emplace(group.GetGroupID());
            TString storagePoolName = group.GetStoragePoolName();
            if (!storagePoolName) {
                storagePoolName = STATIC_STORAGE_POOL_NAME;
            }
            StoragePoolState[storagePoolName].Groups.emplace(group.groupid());
            if (!FilterDatabase || FilterDatabase == DomainPath) {
                DatabaseState[DomainPath].StoragePoolNames.emplace_back(storagePoolName);
            }
        }
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        ++Requests;

        Become(&TThis::StateWait, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvGetTenantStatusResponse, Handle);
            hFunc(TEvHive::TEvResponseHiveDomainStats, Handle);
            hFunc(TEvHive::TEvResponseHiveNodeStats, Handle);
            hFunc(TEvHive::TEvResponseHiveInfo, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle)
            hFunc(TEvBlobStorage::TEvControllerSelectGroupsResult, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            //hFunc(NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvPrivate::TEvRetryNodeWhiteboard, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
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

    void RequestTabletPipe(TTabletId tabletId, const TString& key, IEventBase* payload) {
        auto requestId = TabletRequests.MakeRequest(tabletId, key);
        TTabletRequestsState::TTabletState& requestState(TabletRequests.TabletStates[tabletId]);
        if (!requestState.TabletPipe) {
            requestState.TabletPipe = RegisterWithSameMailbox(NTabletPipe::CreateClient(
                SelfId(),
                tabletId,
                NTabletPipe::TClientRetryPolicy::WithRetries()));
            PipeClients.emplace_back(requestState.TabletPipe);
        }
        NTabletPipe::SendData(SelfId(), requestState.TabletPipe, payload, requestId);
        ++Requests;
    }

    void RequestDescribe(TTabletId schemeShardId, const TString& path) {
        THolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme> request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>();
        NKikimrSchemeOp::TDescribePath& record = request->Record;
        record.SetPath(path);
        record.MutableOptions()->SetReturnPartitioningInfo(false);
        record.MutableOptions()->SetReturnPartitionConfig(false);
        record.MutableOptions()->SetReturnChildren(false);
        RequestTabletPipe(schemeShardId, "TEvDescribeScheme:" + path, request.Release());
    }

    void RequestHiveInfo(TTabletId hiveId) {
        THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
        request->Record.SetReturnFollowers(true);
        RequestTabletPipe(hiveId, "TEvRequestHiveInfo", request.Release());
    }

    void RequestHiveDomainStats(TTabletId hiveId) {
        THolder<TEvHive::TEvRequestHiveDomainStats> request = MakeHolder<TEvHive::TEvRequestHiveDomainStats>();
        request->Record.SetReturnFollowers(true);
        request->Record.SetReturnMetrics(true);
        RequestTabletPipe(hiveId, "TEvRequestHiveDomainStats", request.Release());
    }

    void RequestHiveNodeStats(TTabletId hiveId) {
        THolder<TEvHive::TEvRequestHiveNodeStats> request = MakeHolder<TEvHive::TEvRequestHiveNodeStats>();
        RequestTabletPipe(hiveId, "TEvRequestHiveNodeStats", request.Release());
    }

    void RequestTenantStatus(const TString& path) {
        THolder<NConsole::TEvConsole::TEvGetTenantStatusRequest> request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
        request->Record.MutableRequest()->set_path(path);
        RequestTabletPipe(ConsoleId, "TEvGetTenantStatusRequest:" + path, request.Release());
    }

    void RequestListTenants() {
        THolder<NConsole::TEvConsole::TEvListTenantsRequest> request = MakeHolder<NConsole::TEvConsole::TEvListTenantsRequest>();
        RequestTabletPipe(ConsoleId, "TEvListTenantsRequest", request.Release());
    }

    void RequestSelectGroups(const TString& storagePoolName) {
        THolder<TEvBlobStorage::TEvControllerSelectGroups> request = MakeHolder<TEvBlobStorage::TEvControllerSelectGroups>();
        request->Record.SetReturnAllMatchingGroups(true);
        request->Record.AddGroupParameters()->MutableStoragePoolSpecifier()->SetName(storagePoolName);
        RequestTabletPipe(BsControllerId, "TEvControllerSelectGroups:" + storagePoolName, request.Release());
    }

    void RequestConfig() {
        THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
        RequestTabletPipe(BsControllerId, "TEvControllerConfigRequest", request.Release());
    }

    void RequestSchemeCacheNavigate(const TString& path) {
        THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
        request->ResultSet.emplace_back(entry);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        ++Requests;
    }

    void RequestSchemeCacheNavigate(const TPathId& pathId) {
        THolder<NSchemeCache::TSchemeCacheNavigate> request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId.PathId = pathId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
        request->ResultSet.emplace_back(entry);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        ++Requests;
    }

    template<typename TEvent>
    void RequestNodeWhiteboard(TNodeId nodeId) {
        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
        auto request = MakeHolder<TEvent>();
        Send(whiteboardServiceId, request.Release(), IEventHandle::FlagTrackDelivery, nodeId);
    }

    void RequestGenericNode(TNodeId nodeId) {
        if (NodeIds.emplace(nodeId).second) {
            Send(TlsActivationContext->ActorSystem()->InterconnectProxy(nodeId), new TEvents::TEvSubscribe());
            RequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>(nodeId);
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
            RequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateRequest>(nodeId);
            ++Requests;
            RequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateRequest>(nodeId);
            ++Requests;
            RequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateRequest>(nodeId);
            ++Requests;
        }
    }

    void Handle(TEvPrivate::TEvRetryNodeWhiteboard::TPtr& ev) {
        switch (ev->Get()->EventId) {
            case NNodeWhiteboard::TEvWhiteboard::EvSystemStateRequest:
                RequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>(ev->Get()->NodeId);
                break;
            case NNodeWhiteboard::TEvWhiteboard::EvVDiskStateRequest:
                RequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateRequest>(ev->Get()->NodeId);
                break;
            case NNodeWhiteboard::TEvWhiteboard::EvPDiskStateRequest:
                RequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateRequest>(ev->Get()->NodeId);
                break;
            case NNodeWhiteboard::TEvWhiteboard::EvBSGroupStateRequest:
                RequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateRequest>(ev->Get()->NodeId);
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
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvSystemStateRequest) {
            if (NodeIds.count(nodeId) != 0 && NodeSystemState.count(nodeId) == 0) {
                if (!RetryRequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>(nodeId)) {
                    NodeSystemState.emplace(nodeId, nullptr);
                    RequestDone("undelivered of TEvSystemStateRequest");
                    UnavailableComputeNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvVDiskStateRequest) {
            if (StorageNodeIds.count(nodeId) != 0 && NodeVDiskState.count(nodeId) == 0) {
                if (!RetryRequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateRequest>(nodeId)) {
                    NodeVDiskState.emplace(nodeId, nullptr);
                    RequestDone("undelivered of TEvVDiskStateRequest");
                    UnavailableStorageNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvPDiskStateRequest) {
            if (StorageNodeIds.count(nodeId) != 0 && NodePDiskState.count(nodeId) == 0) {
                if (!RetryRequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateRequest>(nodeId)) {
                    NodePDiskState.emplace(nodeId, nullptr);
                    RequestDone("undelivered of TEvPDiskStateRequest");
                    UnavailableStorageNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvBSGroupStateRequest) {
            if (StorageNodeIds.count(nodeId) != 0 && NodeBSGroupState.count(nodeId) == 0) {
                if (!RetryRequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateRequest>(nodeId)) {
                    NodeBSGroupState.emplace(nodeId, nullptr);
                    RequestDone("undelivered of TEvBSGroupStateRequest");
                }
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        if (NodeIds.count(nodeId) != 0 && NodeSystemState.count(nodeId) == 0) {
            if (!RetryRequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest>(nodeId)) {
                NodeSystemState.emplace(nodeId, nullptr);
                RequestDone("node disconnected with TEvSystemStateRequest");
                UnavailableComputeNodes.insert(nodeId);
            }
        }
        if (StorageNodeIds.count(nodeId) != 0 && NodeVDiskState.count(nodeId) == 0) {
            if (!RetryRequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateRequest>(nodeId)) {
                NodeVDiskState.emplace(nodeId, nullptr);
                RequestDone("node disconnected with TEvVDiskStateRequest");
                UnavailableStorageNodes.insert(nodeId);
            }
        }
        if (StorageNodeIds.count(nodeId) != 0 && NodePDiskState.count(nodeId) == 0) {
            if (!RetryRequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateRequest>(nodeId)) {
                NodePDiskState.emplace(nodeId, nullptr);
                RequestDone("node disconnected with TEvPDiskStateRequest");
                UnavailableStorageNodes.insert(nodeId);
            }
        }
        if (StorageNodeIds.count(nodeId) != 0 && NodeBSGroupState.count(nodeId) == 0) {
            if (!RetryRequestNodeWhiteboard<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateRequest>(nodeId)) {
                NodeBSGroupState.emplace(nodeId, nullptr);
                RequestDone("node disconnected with TEvBSGroupStateRequest");
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TTabletId tabletId = ev->Get()->TabletId;
            for (const auto& [requestId, requestState] : TabletRequests.RequestsInFlight) {
                if (requestState.TabletId == tabletId) {
                    RequestDone("unsuccessful TEvClientConnected");
                }
            }
        }
    }

    void HandleTimeout() {
        ReplyAndPassAway();
    }

    bool IsStaticNode(const TEvInterconnect::TNodeInfo& nodeInfo) const {
        TAppData* appData = AppData();
        if (appData->DynamicNameserviceConfig) {
            return nodeInfo.NodeId <= AppData()->DynamicNameserviceConfig->MaxStaticNodeId;
        } else {
            return true;
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        bool needComputeFromStaticNodes = (!FilterDatabase || FilterDatabase == DomainPath);
        NodesInfo = ev->Release();
        for (const auto& ni : NodesInfo->Nodes) {
            MergedNodeInfo[ni.NodeId] = &ni;
            if (IsStaticNode(ni) && needComputeFromStaticNodes) {
                DatabaseState[DomainPath].ComputeNodeIds.push_back(ni.NodeId);
                RequestComputeNode(ni.NodeId);
            }
        }
        RequestDone("TEvNodesInfo");
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(ev->Get()->Record);
        if (pbRecord.HasResponse() && pbRecord.GetResponse().StatusSize() > 0) {
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            if (pbStatus.HasBaseConfig()) {
                const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
                for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pDisk : pbConfig.GetPDisk()) {
                    RequestStorageNode(pDisk.GetNodeId());
                }
                BaseConfig = ev->Release();
            }
        }
        RequestDone("TEvControllerConfigResponse");
    }

    void Handle(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        for (const auto& matchingGroups : ev->Get()->Record.matchinggroups()) {
            for (const auto& group : matchingGroups.groups()) {
                TString storagePoolName = group.storagepoolname();
                StoragePoolState[storagePoolName].Groups.emplace(group.groupid());
                StoragePoolState[storagePoolName].AuthenticGroups.emplace(group.groupid());
            }
        }
        RequestDone("TEvControllerSelectGroupsResult");
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        if (ev->Get()->GetRecord().GetStatus() == NKikimrScheme::StatusSuccess) {
            TString path = ev->Get()->GetRecord().GetPath();
            TDatabaseState& state(DatabaseState[path]);
            for (const auto& storagePool : ev->Get()->GetRecord().GetPathDescription().GetDomainDescription().GetStoragePools()) {
                TString storagePoolName = storagePool.name();
                state.StoragePoolNames.emplace_back(storagePoolName);
                StoragePoolState[storagePoolName].Kind = storagePool.kind();
                RequestSelectGroups(storagePoolName);
            }
            if (path == DomainPath) {
                state.StoragePoolNames.emplace_back(STATIC_STORAGE_POOL_NAME);
            }
            DescribeByPath[path] = ev->Release();
        }
        RequestDone("TEvDescribeSchemeResult");
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ResultSet.size() == 1 && ev->Get()->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            auto domainInfo = ev->Get()->Request->ResultSet.begin()->DomainInfo;
            TString path = CanonizePath(ev->Get()->Request->ResultSet.begin()->Path);

            if (domainInfo->DomainKey != domainInfo->ResourcesDomainKey) {
                if (SharedDatabases.emplace(domainInfo->ResourcesDomainKey, path).second) {
                    RequestSchemeCacheNavigate(domainInfo->ResourcesDomainKey);
                }
                DatabaseState[path].ResourcePathId = domainInfo->ResourcesDomainKey;
            }
            TTabletId hiveId = domainInfo->Params.GetHive();
            if (hiveId) {
                DatabaseState[path].HiveId = hiveId;
                TabletRequests.TabletStates[hiveId].Database = path;
                TabletRequests.TabletStates[hiveId].Type = TTabletTypes::Hive;
                //RequestHiveDomainStats(hiveId);
                RequestHiveNodeStats(hiveId);
                RequestHiveInfo(hiveId);
            }
            FilterDomainKey[TSubDomainKey(domainInfo->DomainKey.OwnerId, domainInfo->DomainKey.LocalPathId)] = path;
            NavigateResult[path] = std::move(ev->Get()->Request);
            TTabletId schemeShardId = domainInfo->Params.GetSchemeShard();
            if (!schemeShardId) {
                schemeShardId = RootSchemeShardId;
            } else {
                TabletRequests.TabletStates[schemeShardId].Database = path;
                TabletRequests.TabletStates[schemeShardId].Type = TTabletTypes::SchemeShard;
            }
            RequestDescribe(schemeShardId, path);
        }
        RequestDone("TEvNavigateKeySetResult");
    }

    void Handle(TEvHive::TEvResponseHiveDomainStats::TPtr& ev) {
        TTabletId hiveId = TabletRequests.CompleteRequest(ev->Cookie);
        for (const NKikimrHive::THiveDomainStats& hiveStat : ev->Get()->Record.GetDomainStats()) {
            for (TNodeId nodeId : hiveStat.GetNodeIds()) {
                RequestComputeNode(nodeId);
            }
        }
        HiveDomainStats[hiveId] = std::move(ev->Release());
        RequestDone("TEvResponseHiveDomainStats");
    }

    void Handle(TEvHive::TEvResponseHiveNodeStats::TPtr& ev) {
        TTabletId hiveId = TabletRequests.CompleteRequest(ev->Cookie);
        for (const NKikimrHive::THiveNodeStats& hiveStat : ev->Get()->Record.GetNodeStats()) {
            RequestComputeNode(hiveStat.GetNodeId());
        }
        HiveNodeStats[hiveId] = std::move(ev->Release());
        RequestDone("TEvResponseHiveNodeStats");
    }

    void Handle(TEvHive::TEvResponseHiveInfo::TPtr& ev) {
        TTabletId hiveId = TabletRequests.CompleteRequest(ev->Cookie);
        HiveInfo[hiveId] = std::move(ev->Release());
        RequestDone("TEvResponseHiveInfo");
    }

    void Handle(NConsole::TEvConsole::TEvGetTenantStatusResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        auto& operation(ev->Get()->Record.GetResponse().operation());
        if (operation.ready() && operation.status() == Ydb::StatusIds::SUCCESS) {
            Ydb::Cms::GetDatabaseStatusResult getTenantStatusResult;
            operation.result().UnpackTo(&getTenantStatusResult);
            TString path = getTenantStatusResult.path();
            DatabaseStatusByPath[path] = std::move(getTenantStatusResult);
            DatabaseState[path];
            RequestSchemeCacheNavigate(path);
        }
        RequestDone("TEvGetTenantStatusResponse");
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            RequestTenantStatus(path);
            DatabaseState[path];
        }
        RequestDone("TEvListTenantsResponse");
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        TNodeId nodeId = ev.Get()->Cookie;
        if (NodeSystemState.count(nodeId) == 0) {
            auto& nodeSystemState(NodeSystemState[nodeId]);
            nodeSystemState = ev->Release();
            for (NKikimrWhiteboard::TSystemStateInfo& state : *nodeSystemState->Record.MutableSystemStateInfo()) {
                state.set_nodeid(nodeId);
                MergedNodeSystemState[nodeId] = &state;
            }
            RequestDone("TEvSystemStateResponse");
        }
    }

    void AggregateHiveInfo() {
        TNodeTabletState::TTabletStateSettings settings;
        settings.AliveBarrier = TInstant::Now() - TDuration::Minutes(5);
        for (const auto& [hiveId, hiveResponse] : HiveInfo) {
            if (hiveResponse) {
                for (const NKikimrHive::TTabletInfo& hiveTablet : hiveResponse->Record.GetTablets()) {
                    TSubDomainKey tenantId = TSubDomainKey(hiveTablet.GetObjectDomain());
                    auto itDomain = FilterDomainKey.find(tenantId);
                    if (itDomain == FilterDomainKey.end()) {
                        continue;
                    }
                    auto itDatabase = DatabaseState.find(itDomain->second);
                    if (itDatabase == DatabaseState.end()) {
                        continue;
                    }
                    TDatabaseState& database = itDatabase->second;
                    auto tabletId = std::make_pair(hiveTablet.GetTabletID(), hiveTablet.GetFollowerID());
                    database.MergedTabletState.emplace(tabletId, &hiveTablet);
                    TNodeId nodeId = hiveTablet.GetNodeID();
                    switch (hiveTablet.GetVolatileState()) {
                        case NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_STARTING:
                        case NKikimrHive::ETabletVolatileState::TABLET_VOLATILE_STATE_RUNNING:
                            break;
                        default:
                            nodeId = 0;
                            break;
                    }
                    database.MergedNodeTabletState[nodeId].AddTablet(hiveTablet, settings);
                }
            }
        }
    }

    void AggregateHiveDomainStats() {
        for (const auto& [hiveId, hiveResponse] : HiveDomainStats) {
            if (hiveResponse) {
                for (const NKikimrHive::THiveDomainStats& hiveStat : hiveResponse->Record.GetDomainStats()) {
                    TSubDomainKey domainKey({hiveStat.GetShardId(), hiveStat.GetPathId()});
                    auto itFilterDomainKey = FilterDomainKey.find(domainKey);
                    if (itFilterDomainKey != FilterDomainKey.end()) {
                        TString path(itFilterDomainKey->second);
                        TDatabaseState& state(DatabaseState[path]);
                        for (TNodeId nodeId : hiveStat.GetNodeIds()) {
                            state.ComputeNodeIds.emplace_back(nodeId);
                        }
                    }
                }
            }
        }
    }

    void AggregateHiveNodeStats() {
        for (const auto& [hiveId, hiveResponse] : HiveNodeStats) {
            if (hiveResponse) {
                for (const NKikimrHive::THiveNodeStats& hiveStat : hiveResponse->Record.GetNodeStats()) {
                    if (hiveStat.HasNodeDomain()) {
                        TSubDomainKey domainKey(hiveStat.GetNodeDomain());
                        auto itFilterDomainKey = FilterDomainKey.find(domainKey);
                        if (itFilterDomainKey != FilterDomainKey.end()) {
                            TString path(itFilterDomainKey->second);
                            TDatabaseState& state(DatabaseState[path]);
                            state.ComputeNodeIds.emplace_back(hiveStat.GetNodeId());
                        }
                    }
                }
            }
        }
    }

    void AggregateBSControllerState() {
        if (BaseConfig) {
            const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(BaseConfig->Record);
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            if (pbStatus.HasBaseConfig()) {
                const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
                for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pDisk : pbConfig.GetPDisk()) {
                    auto pDiskId = GetPDiskId(pDisk);
                    ValidPDisks.emplace(pDiskId);
                    auto itPDisk = MergedPDiskState.find(pDiskId);
                    if (itPDisk == MergedPDiskState.end()) {
                        PDisksAppended.emplace_back();
                        NKikimrWhiteboard::TPDiskStateInfo& pbPDisk = PDisksAppended.back();
                        itPDisk = MergedPDiskState.emplace(pDiskId, &pbPDisk).first;
                        pbPDisk.SetNodeId(pDisk.GetNodeId());
                        pbPDisk.SetPDiskId(pDisk.GetPDiskId());
                        pbPDisk.SetPath(pDisk.GetPath());
                        pbPDisk.SetGuid(pDisk.GetGuid());
                        pbPDisk.SetCategory(static_cast<ui64>(pDisk.GetType()));
                        pbPDisk.SetTotalSize(pDisk.GetPDiskMetrics().GetTotalSize());
                        pbPDisk.SetAvailableSize(pDisk.GetPDiskMetrics().GetAvailableSize());
                    }
                }
                std::unordered_map<NKikimrBlobStorage::TVSlotId, const NKikimrBlobStorage::TBaseConfig::TVSlot*> slotsIndex;
                for (const NKikimrBlobStorage::TBaseConfig::TVSlot& vDisk : pbConfig.GetVSlot()) {
                    slotsIndex[vDisk.GetVSlotId()] = &vDisk;
                    auto vDiskId = GetVDiskId(vDisk);
                    ValidVDisks.emplace(vDiskId);
                    auto itVDisk = MergedVDiskState.find(vDiskId);
                    if (itVDisk == MergedVDiskState.end()) {
                        VDisksAppended.emplace_back();
                        NKikimrWhiteboard::TVDiskStateInfo& pbVDisk = VDisksAppended.back();
                        itVDisk = MergedVDiskState.emplace(vDiskId, &pbVDisk).first;
                        auto* pVDiskId = pbVDisk.MutableVDiskId();
                        pVDiskId->SetGroupID(vDisk.groupid());
                        pVDiskId->SetGroupGeneration(vDisk.groupgeneration());
                        pVDiskId->SetRing(vDisk.failrealmidx());
                        pVDiskId->SetDomain(vDisk.faildomainidx());
                        pVDiskId->SetVDisk(vDisk.vslotid().vslotid());
                        pbVDisk.SetNodeId(vDisk.GetVSlotId().GetNodeId());
                        pbVDisk.SetPDiskId(vDisk.GetVSlotId().GetPDiskId());
                        pbVDisk.SetAllocatedSize(vDisk.GetVDiskMetrics().GetAllocatedSize());
                    }
                }
                for (const NKikimrBlobStorage::TBaseConfig::TGroup& group : pbConfig.GetGroup()) {
                    auto groupId = group.GetGroupId();
                    ValidGroups.emplace(groupId);
                    auto itGroup = MergedBSGroupState.find(groupId);
                    if (itGroup == MergedBSGroupState.end()) {
                        BSGroupAppended.emplace_back();
                        NKikimrWhiteboard::TBSGroupStateInfo& pbGroup = BSGroupAppended.back();
                        itGroup = MergedBSGroupState.emplace(groupId, &pbGroup).first;
                        pbGroup.SetGroupID(group.GetGroupId());
                        pbGroup.SetGroupGeneration(group.GetGroupGeneration());
                        pbGroup.SetErasureSpecies(group.GetErasureSpecies());
                        for (const auto& vSlotId : group.GetVSlotId()) {
                            auto itSlot = slotsIndex.find(vSlotId);
                            if (itSlot != slotsIndex.end()) {
                                const auto& vSlot(*(itSlot->second));
                                VDiskIDFromVDiskID(TVDiskID(vSlot.GetGroupId(),
                                                            vSlot.GetGroupGeneration(),
                                                            vSlot.GetFailRealmIdx(),
                                                            vSlot.GetFailDomainIdx(),
                                                            vSlot.GetVDiskIdx()), pbGroup.AddVDiskIds());
                            }
                        }
                    }
                }
            }
        }
        for (auto itPDisk = MergedPDiskState.begin(); itPDisk != MergedPDiskState.end();) {
            if (ValidPDisks.count(itPDisk->first)) {
                ++itPDisk;
            } else {
                itPDisk = MergedPDiskState.erase(itPDisk);
            }
        }
        for (auto itVDisk = MergedVDiskState.begin(); itVDisk != MergedVDiskState.end();) {
            if (ValidVDisks.count(itVDisk->first)) {
                ++itVDisk;
            } else {
                itVDisk = MergedVDiskState.erase(itVDisk);
            }
        }
        for (auto itGroup = MergedBSGroupState.begin(); itGroup != MergedBSGroupState.end();) {
            if (ValidGroups.count(itGroup->first)) {
                ++itGroup;
            } else {
                itGroup = MergedBSGroupState.erase(itGroup);
            }
        }
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        TNodeId nodeId = ev.Get()->Cookie;
        if (NodeVDiskState.count(nodeId) == 0) {
            auto& nodeVDiskState(NodeVDiskState[nodeId]);
            nodeVDiskState = ev->Release();
            for (NKikimrWhiteboard::TVDiskStateInfo& state : *nodeVDiskState->Record.MutableVDiskStateInfo()) {
                state.set_nodeid(nodeId);
                auto id = GetVDiskId(state.vdiskid());
                MergedVDiskState[id] = &state;
            }
            RequestDone("TEvVDiskStateResponse");
        }
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        TNodeId nodeId = ev.Get()->Cookie;
        if (NodePDiskState.count(nodeId) == 0) {
            auto& nodePDiskState(NodePDiskState[nodeId]);
            nodePDiskState = ev->Release();
            for (NKikimrWhiteboard::TPDiskStateInfo& state : *nodePDiskState->Record.MutablePDiskStateInfo()) {
                state.set_nodeid(nodeId);
                auto id = GetPDiskId(state);
                MergedPDiskState[id] = &state;
            }
            RequestDone("TEvPDiskStateResponse");
        }
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        if (NodeBSGroupState.count(nodeId) == 0) {
            auto& nodeBSGroupState(NodeBSGroupState[nodeId]);
            nodeBSGroupState = ev->Release();
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
                StoragePoolState[storagePoolName].Groups.emplace(state.groupid());
            }
            RequestDone("TEvBSGroupStateResponse");
        }
    }

    static Ydb::Monitoring::StatusFlag::Status MaxStatus(Ydb::Monitoring::StatusFlag::Status a, Ydb::Monitoring::StatusFlag::Status b) {
        return static_cast<Ydb::Monitoring::StatusFlag::Status>(std::max<int>(a, b));
    }

    static Ydb::Monitoring::StatusFlag::Status MinStatus(Ydb::Monitoring::StatusFlag::Status a, Ydb::Monitoring::StatusFlag::Status b) {
        return static_cast<Ydb::Monitoring::StatusFlag::Status>(std::min<int>(a, b));
    }

    static Ydb::Monitoring::StatusFlag::Status StatusFromWhiteboardFlag(NKikimrWhiteboard::EFlag flag) {
        switch(flag) {
        case NKikimrWhiteboard::EFlag::Green:
            return Ydb::Monitoring::StatusFlag::GREEN;
        case NKikimrWhiteboard::EFlag::Yellow:
            return Ydb::Monitoring::StatusFlag::YELLOW;
        case NKikimrWhiteboard::EFlag::Orange:
            return Ydb::Monitoring::StatusFlag::ORANGE;
        case NKikimrWhiteboard::EFlag::Red:
            return Ydb::Monitoring::StatusFlag::RED;
        default:
            return Ydb::Monitoring::StatusFlag::GREY;
        }
    }

    static Ydb::Monitoring::StatusFlag::Status StatusFrom(const NKikimrWhiteboard::TSystemStateInfo& systemStateInfo) {
        return StatusFromWhiteboardFlag(systemStateInfo.GetSystemState());
    }

    static Ydb::Monitoring::StatusFlag::Status StatusFrom(const NKikimrWhiteboard::TTabletStateInfo& tabletStateInfo) {
        TInstant now(TInstant::Now());
        if (tabletStateInfo.state() == NKikimrWhiteboard::TTabletStateInfo::Active) {
            if (now - TInstant::MilliSeconds(tabletStateInfo.changetime()) > TDuration::Seconds(30)) {
                return Ydb::Monitoring::StatusFlag::GREEN;
            } else {
                return Ydb::Monitoring::StatusFlag::YELLOW;
            }
        } else {
            if (tabletStateInfo.leader()) {
                return Ydb::Monitoring::StatusFlag::RED;
            } else {
                return Ydb::Monitoring::StatusFlag::BLUE;
            }
        }
    }

    static Ydb::Monitoring::StatusFlag::Status StatusFrom(const NKikimrHive::TTabletInfo& tabletInfo) {
        switch (tabletInfo.volatilestate()) {
            case NKikimrHive::TABLET_VOLATILE_STATE_RUNNING:
                return Ydb::Monitoring::StatusFlag::GREEN;
            case NKikimrHive::TABLET_VOLATILE_STATE_STARTING:
                return Ydb::Monitoring::StatusFlag::YELLOW;
            default:
                return Ydb::Monitoring::StatusFlag::RED;
        }
    }

    static TString GetNodeLocation(const TEvInterconnect::TNodeInfo& nodeInfo) {
        return TStringBuilder() << nodeInfo.NodeId << '/' << nodeInfo.Host << ':' << nodeInfo.Port;
    }

    static void Check(TSelfCheckContext& context, const NKikimrWhiteboard::TSystemStateInfo::TPoolStats& poolStats) {
        if (poolStats.name() == "System" || poolStats.name() == "IC" || poolStats.name() == "IO") {
            if (poolStats.usage() >= 0.99) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Pool usage over 99%", "overload-state");
            } else if (poolStats.usage() >= 0.95) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Pool usage over 95%", "overload-state");
            } else if (poolStats.usage() >= 0.90) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Pool usage over 90%", "overload-state");
            } else {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        } else {
            if (poolStats.usage() >= 0.99) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Pool usage over 99%", "overload-state");
            } else if (poolStats.usage() >= 0.95) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Pool usage over 95%", "overload-state");
            } else {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        }
    }

    Ydb::Monitoring::StatusFlag::Status FillSystemTablets(TSelfCheckContext context) {
        TString databaseId = context.Location.database().name();
        for (auto& [tabletId, tablet] : TabletRequests.TabletStates) {
            if (tablet.Database == databaseId) {
                context.Location.mutable_compute()->clear_tablet();
                auto& protoTablet = *context.Location.mutable_compute()->mutable_tablet();
                if (tablet.IsUnresponsive || tablet.MaxResponseTime >= TDuration::MilliSeconds(1000)) {
                    if (tablet.Type != TTabletTypes::Unknown) {
                        protoTablet.set_type(TTabletTypes::EType_Name(tablet.Type));
                    }
                    protoTablet.add_id(ToString(tabletId));
                    if (tablet.IsUnresponsive) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet is unresponsive", "system-tablet-state");
                    } else if (tablet.MaxResponseTime >= TDuration::MilliSeconds(5000)) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "System tablet response time is over 5000ms", "system-tablet-state");
                    } else if (tablet.MaxResponseTime >= TDuration::MilliSeconds(1000)) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "System tablet response time is over 1000ms", "system-tablet-state");
                    }
                }
            }
        }
        return context.GetOverallStatus();
    }

    Ydb::Monitoring::StatusFlag::Status FillTablets(TDatabaseState& databaseState,
                                                    TNodeId nodeId,
                                                    google::protobuf::RepeatedPtrField<Ydb::Monitoring::ComputeTabletStatus>& parent,
                                                    TSelfCheckContext& context) {
        Ydb::Monitoring::StatusFlag::Status tabletsStatus = Ydb::Monitoring::StatusFlag::GREEN;
        auto itNodeTabletState = databaseState.MergedNodeTabletState.find(nodeId);
        if (itNodeTabletState != databaseState.MergedNodeTabletState.end()) {
            TSelfCheckContext tabletsContext(&context);
            for (const auto& count : itNodeTabletState->second.Count) {
                if (count.Count > 0) {
                    TSelfCheckContext tabletContext(&tabletsContext, "TABLET");
                    auto& protoTablet = *tabletContext.Location.mutable_compute()->mutable_tablet();
                    protoTablet.set_type(TTabletTypes::EType_Name(count.Type));
                    protoTablet.set_count(count.Count);
                    if (!count.Identifiers.empty()) {
                        for (const TString& id : count.Identifiers) {
                            protoTablet.add_id(id);
                        }
                    }
                    Ydb::Monitoring::ComputeTabletStatus& computeTabletStatus = *parent.Add();
                    computeTabletStatus.set_type(NKikimrTabletBase::TTabletTypes::EType_Name(count.Type));
                    computeTabletStatus.set_count(count.Count);
                    for (const TString& id : count.Identifiers) {
                        computeTabletStatus.add_id(id);
                    }
                    switch (count.State) {
                        case TNodeTabletState::ETabletState::Good:
                            computeTabletStatus.set_state("GOOD");
                            tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                            break;
                        case TNodeTabletState::ETabletState::Stopped:
                            computeTabletStatus.set_state("STOPPED");
                            tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                            break;
                        case TNodeTabletState::ETabletState::RestartsTooOften:
                            computeTabletStatus.set_state("RESTARTS_TOO_OFTEN");
                            tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Tablets are restarting too often", "tablet-state");
                            break;
                        case TNodeTabletState::ETabletState::Dead:
                            computeTabletStatus.set_state("DEAD");
                            tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Tablets are dead", "tablet-state");
                            break;
                    }
                    computeTabletStatus.set_overall(tabletContext.GetOverallStatus());
                    tabletsStatus = MaxStatus(tabletsStatus, tabletContext.GetOverallStatus());
                }
            }
        }
        return tabletsStatus;
    }

    void FillComputeNodeStatus(TNodeId nodeId, Ydb::Monitoring::ComputeNodeStatus& computeNodeStatus, TSelfCheckContext context) {
        const TEvInterconnect::TNodeInfo* nodeInfo = nullptr;
        auto itNodeInfo = MergedNodeInfo.find(nodeId);
        if (itNodeInfo != MergedNodeInfo.end()) {
            nodeInfo = itNodeInfo->second;
        }
        TString id(ToString(nodeId));

        context.Location.mutable_compute()->mutable_node()->set_id(nodeId);
        if (nodeInfo) {
            context.Location.mutable_compute()->mutable_node()->set_host(nodeInfo->Host);
            context.Location.mutable_compute()->mutable_node()->set_port(nodeInfo->Port);
        }

        auto itNodeSystemState = MergedNodeSystemState.find(nodeId);
        if (itNodeSystemState != MergedNodeSystemState.end()) {
            const NKikimrWhiteboard::TSystemStateInfo& nodeSystemState(*itNodeSystemState->second);

            for (const auto& poolStat : nodeSystemState.poolstats()) {
                TSelfCheckContext poolContext(&context, "COMPUTE_POOL");
                poolContext.Location.mutable_compute()->mutable_pool()->set_name(poolStat.name());
                Check(poolContext, poolStat);
                Ydb::Monitoring::ThreadPoolStatus& threadPoolStatus = *computeNodeStatus.add_pools();
                threadPoolStatus.set_name(poolStat.name());
                threadPoolStatus.set_usage(poolStat.usage());
                threadPoolStatus.set_overall(poolContext.GetOverallStatus());
            }

            if (nodeSystemState.loadaverage_size() > 0 && nodeSystemState.numberofcpus() > 0) {
                TSelfCheckContext laContext(&context, "LOAD_AVERAGE");
                Ydb::Monitoring::LoadAverageStatus& loadAverageStatus = *computeNodeStatus.mutable_load();
                loadAverageStatus.set_load(nodeSystemState.loadaverage(0));
                loadAverageStatus.set_cores(nodeSystemState.numberofcpus());
                if (loadAverageStatus.load() > loadAverageStatus.cores()) {
                    laContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "LoadAverage above 100%", "overload-state");
                } else {
                    laContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                }
                loadAverageStatus.set_overall(laContext.GetOverallStatus());
            }
        } else {
            // context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
            //                      TStringBuilder() << "Compute node is not available",
            //                      "node-state");
        }
        computeNodeStatus.set_id(id);
        computeNodeStatus.set_overall(context.GetOverallStatus());
    }

    void FillCompute(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckContext context) {
        TVector<TNodeId>* computeNodeIds = &databaseState.ComputeNodeIds;
        if (databaseState.ResourcePathId) {
            auto itDatabase = FilterDomainKey.find(TSubDomainKey(databaseState.ResourcePathId.OwnerId, databaseState.ResourcePathId.LocalPathId));
            if (itDatabase != FilterDomainKey.end()) {
                const TString& sharedDatabaseName = itDatabase->second;
                TDatabaseState& sharedDatabase = DatabaseState[sharedDatabaseName];
                computeNodeIds = &sharedDatabase.ComputeNodeIds;
            }
        }

        std::sort(computeNodeIds->begin(), computeNodeIds->end());
        computeNodeIds->erase(std::unique(computeNodeIds->begin(), computeNodeIds->end()), computeNodeIds->end());
        if (computeNodeIds->empty()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There are no compute nodes");
        } else {
            Ydb::Monitoring::StatusFlag::Status systemStatus = FillSystemTablets({&context, "SYSTEM_TABLET"});
            if (systemStatus != Ydb::Monitoring::StatusFlag::GREEN && systemStatus != Ydb::Monitoring::StatusFlag::GREY) {
                context.ReportStatus(systemStatus, "Compute has issues with system tablets", "compute-state", {"system-tablet-state"});
            }
            Ydb::Monitoring::StatusFlag::Status nodesStatus = Ydb::Monitoring::StatusFlag::GREEN;
            for (TNodeId nodeId : *computeNodeIds) {
                auto& computeNode = *computeStatus.add_nodes();
                FillComputeNodeStatus(nodeId, computeNode, {&context, "COMPUTE_NODE"});
                nodesStatus = MaxStatus(nodesStatus, computeNode.overall());
            }
            if (nodesStatus != Ydb::Monitoring::StatusFlag::GREEN) {
                context.ReportStatus(nodesStatus, "Compute is overloaded", "compute-state", {"overload-state"});
            }
            Ydb::Monitoring::StatusFlag::Status tabletsStatus = Ydb::Monitoring::StatusFlag::GREEN;
            computeNodeIds->push_back(0); // for tablets without node
            for (TNodeId nodeId : *computeNodeIds) {
                tabletsStatus = MaxStatus(tabletsStatus, FillTablets(databaseState, nodeId, *computeStatus.mutable_tablets(), context));
            }
            if (tabletsStatus != Ydb::Monitoring::StatusFlag::GREEN) {
                context.ReportStatus(tabletsStatus, "Compute has issues with tablets", "compute-state", {"tablet-state"});
            }
        }
        computeStatus.set_overall(context.GetOverallStatus());
    }

    static TString GetVDiskId(const NKikimrBlobStorage::TVDiskID& protoVDiskId) {
        return TStringBuilder()
                << protoVDiskId.groupid() << '-'
                << protoVDiskId.groupgeneration() << '-'
                << protoVDiskId.ring() << '-'
                << protoVDiskId.domain() << '-'
                << protoVDiskId.vdisk();
    }

    static TString GetVDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& protoVSlotId) {
        return TStringBuilder()
                << protoVSlotId.groupid() << '-'
                << protoVSlotId.groupgeneration() << '-'
                << protoVSlotId.failrealmidx() << '-'
                << protoVSlotId.faildomainidx() << '-'
                << protoVSlotId.vdiskidx();
    }

    static TString GetVDiskId(const NKikimrBlobStorage::TNodeWardenServiceSet_TVDisk& protoVDiskId) {
        return GetVDiskId(protoVDiskId.vdiskid());
    }

    static TString GetVDiskId(const NKikimrWhiteboard::TVDiskStateInfo vDiskInfo) {
        return GetVDiskId(vDiskInfo.vdiskid());
    }

    static TString GetPDiskId(const NKikimrWhiteboard::TVDiskStateInfo vDiskInfo) {
        return TStringBuilder() << vDiskInfo.nodeid() << "-" << vDiskInfo.pdiskid();
    }

    static TString GetPDiskId(const NKikimrWhiteboard::TPDiskStateInfo pDiskInfo) {
        return TStringBuilder() << pDiskInfo.nodeid() << "-" << pDiskInfo.pdiskid();
    }

    static TString GetPDiskId(const NKikimrBlobStorage::TBaseConfig::TPDisk& pDisk) {
        return TStringBuilder() << pDisk.nodeid() << "-" << pDisk.pdiskid();
    }

    static TString GetPDiskId(const NKikimrBlobStorage::TNodeWardenServiceSet_TPDisk& pDisk) {
        return TStringBuilder() << pDisk.nodeid() << "-" << pDisk.pdiskid();
    }

    void FillPDiskStatus(const TString& pDiskId, const NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo, Ydb::Monitoring::StoragePDiskStatus& storagePDiskStatus, TSelfCheckContext context) {
        context.Location.clear_database(); // PDisks are shared between databases
        context.Location.mutable_storage()->mutable_pool()->clear_name(); // PDisks are shared between pools
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // PDisks are shared between groups
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->clear_id(); // PDisks are shared between vdisks
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->set_id(pDiskId);
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->set_path(pDiskInfo.path());
        storagePDiskStatus.set_id(pDiskId);

        if (pDiskInfo.HasState()) {
            switch (pDiskInfo.GetState()) {
                case NKikimrBlobStorage::TPDiskState::Normal:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    break;
                case NKikimrBlobStorage::TPDiskState::Initial:
                case NKikimrBlobStorage::TPDiskState::InitialFormatRead:
                case NKikimrBlobStorage::TPDiskState::InitialSysLogRead:
                case NKikimrBlobStorage::TPDiskState::InitialCommonLogRead:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW,
                                         TStringBuilder() << "PDisk state is " << NKikimrBlobStorage::TPDiskState::E_Name(pDiskInfo.GetState()),
                                         "pdisk-state");
                    break;
                case NKikimrBlobStorage::TPDiskState::InitialFormatReadError:
                case NKikimrBlobStorage::TPDiskState::InitialSysLogReadError:
                case NKikimrBlobStorage::TPDiskState::InitialSysLogParseError:
                case NKikimrBlobStorage::TPDiskState::InitialCommonLogReadError:
                case NKikimrBlobStorage::TPDiskState::InitialCommonLogParseError:
                case NKikimrBlobStorage::TPDiskState::CommonLoggerInitError:
                case NKikimrBlobStorage::TPDiskState::OpenFileError:
                case NKikimrBlobStorage::TPDiskState::ChunkQuotaError:
                case NKikimrBlobStorage::TPDiskState::DeviceIoError:
                case NKikimrBlobStorage::TPDiskState::Missing:
                case NKikimrBlobStorage::TPDiskState::Timeout:
                case NKikimrBlobStorage::TPDiskState::NodeDisconnected:
                case NKikimrBlobStorage::TPDiskState::Unknown:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                         TStringBuilder() << "PDisk state is " << NKikimrBlobStorage::TPDiskState::E_Name(pDiskInfo.GetState()),
                                         "pdisk-state");
                    break;
                case NKikimrBlobStorage::TPDiskState::Reserved14:
                case NKikimrBlobStorage::TPDiskState::Reserved15:
                case NKikimrBlobStorage::TPDiskState::Reserved16:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Unknown PDisk state");
                    break;
            }

            //if (pDiskInfo.HasAvailableSize() && pDiskInfo.GetTotalSize() != 0) {
            if (pDiskInfo.GetAvailableSize() != 0 && pDiskInfo.GetTotalSize() != 0) { // hotfix until KIKIMR-12659
                double avail = (double)pDiskInfo.GetAvailableSize() / pDiskInfo.GetTotalSize();
                if (avail < 0.06) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Available size is less than 6%", "pdisk-space");
                } else if (avail < 0.09) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Available size is less than 9%", "pdisk-space");
                } else if (avail < 0.12) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Available size is less than 12%", "pdisk-space");
                }
            }
        } else {
            if (UnavailableStorageNodes.count(pDiskInfo.nodeid()) != 0) {
                TSelfCheckContext nodeContext(&context, "STORAGE_NODE");
                nodeContext.Location.mutable_storage()->clear_pool();
                nodeContext.Location.mutable_storage()->mutable_node()->set_id(pDiskInfo.nodeid());
                const TEvInterconnect::TNodeInfo* nodeInfo = nullptr;
                auto itNodeInfo = MergedNodeInfo.find(pDiskInfo.nodeid());
                if (itNodeInfo != MergedNodeInfo.end()) {
                    nodeInfo = itNodeInfo->second;
                }
                if (nodeInfo) {
                    nodeContext.Location.mutable_storage()->mutable_node()->set_host(nodeInfo->Host);
                    nodeContext.Location.mutable_storage()->mutable_node()->set_port(nodeInfo->Port);
                }
                nodeContext.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                         TStringBuilder() << "Storage node is not available",
                                         "node-state");
            }
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                 TStringBuilder() << "PDisk is not available",
                                 "pdisk-state",
                                 {"node-state"});
        }

        storagePDiskStatus.set_overall(context.GetOverallStatus());
    }

    static Ydb::Monitoring::StatusFlag::Status GetFlagFromWhiteboardFlag(NKikimrWhiteboard::EFlag flag) {
        switch (flag) {
            case NKikimrWhiteboard::EFlag::Green:
                return Ydb::Monitoring::StatusFlag::GREEN;
            case NKikimrWhiteboard::EFlag::Yellow:
                return Ydb::Monitoring::StatusFlag::YELLOW;
            case NKikimrWhiteboard::EFlag::Orange:
                return Ydb::Monitoring::StatusFlag::ORANGE;
            case NKikimrWhiteboard::EFlag::Red:
                return Ydb::Monitoring::StatusFlag::RED;
            default:
                return Ydb::Monitoring::StatusFlag::UNSPECIFIED;
        }
    }

    void FillVDiskStatus(const TString& vDiskId, const NKikimrWhiteboard::TVDiskStateInfo& vDiskInfo, Ydb::Monitoring::StorageVDiskStatus& storageVDiskStatus, TSelfCheckContext context) {
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->set_id(vDiskId);
        storageVDiskStatus.set_id(vDiskId);
        TString pDiskId = GetPDiskId(vDiskInfo);
        auto itPDisk = MergedPDiskState.find(pDiskId);
        if (itPDisk != MergedPDiskState.end()) {
            FillPDiskStatus(pDiskId, *itPDisk->second, *storageVDiskStatus.mutable_pdisk(), {&context, "PDISK"});
        }

        if (!vDiskInfo.HasVDiskState()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                 TStringBuilder() << "VDisk is not available",
                                 "vdisk-state",
                                 {"pdisk-state"});
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        switch (vDiskInfo.GetVDiskState()) {
            case NKikimrWhiteboard::EVDiskState::OK:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
            case NKikimrWhiteboard::EVDiskState::Initial:
            case NKikimrWhiteboard::EVDiskState::SyncGuidRecovery:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW,
                                     TStringBuilder() << "VDisk state is " << NKikimrWhiteboard::EVDiskState_Name(vDiskInfo.GetVDiskState()),
                                     "vdisk-state");
                break;
            case NKikimrWhiteboard::EVDiskState::LocalRecoveryError:
            case NKikimrWhiteboard::EVDiskState::SyncGuidRecoveryError:
            case NKikimrWhiteboard::EVDiskState::PDiskError:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                     TStringBuilder() << "VDisk state is " << NKikimrWhiteboard::EVDiskState_Name(vDiskInfo.GetVDiskState()),
                                     "vdisk-state",
                                     {"pdisk-state"});
                break;
        }

        if (vDiskInfo.HasDiskSpace()) {
            switch(vDiskInfo.GetDiskSpace()) {
                case NKikimrWhiteboard::EFlag::Green:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    break;
                case NKikimrWhiteboard::EFlag::Red:
                    break;
                    context.ReportStatus(GetFlagFromWhiteboardFlag(vDiskInfo.GetDiskSpace()),
                                         TStringBuilder() << "DiskSpace is " << NKikimrWhiteboard::EFlag_Name(vDiskInfo.GetDiskSpace()),
                                         "vdisk-state",
                                         {"pdisk-space"});
                default:
                    context.ReportStatus(GetFlagFromWhiteboardFlag(vDiskInfo.GetDiskSpace()),
                                         TStringBuilder() << "DiskSpace is " << NKikimrWhiteboard::EFlag_Name(vDiskInfo.GetDiskSpace()),
                                         "vdisk-space",
                                         {"pdisk-space"});
                    break;
            }
        }

        if (context.GetOverallStatus() == Ydb::Monitoring::StatusFlag::GREEN && !vDiskInfo.GetReplicated()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Replication in progress", "vdisk-state");
        }

        storageVDiskStatus.set_overall(context.GetOverallStatus());
    }

    static const inline TString NONE = "none";
    static const inline TString BLOCK_4_2 = "block-4-2";
    static const inline TString MIRROR_3_DC = "mirror-3-dc";

    static void IncrementFor(TStackVec<std::pair<ui32, int>>& realms, ui32 realm) {
        auto itRealm = FindIf(realms, [realm](const std::pair<ui32, int>& p) -> bool {
            return p.first == realm;
        });
        if (itRealm == realms.end()) {
            itRealm = realms.insert(realms.end(), { realm, 1 });
        } else {
            itRealm->second++;
        }
    }

    void FillGroupStatus(TGroupId groupId, const NKikimrWhiteboard::TBSGroupStateInfo& groupInfo, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckContext context) {
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->set_id(ToString(groupId));
        storageGroupStatus.set_id(ToString(groupId));
        int disksColors[Ydb::Monitoring::StatusFlag::Status_ARRAYSIZE] = {};
        TStackVec<std::pair<ui32, int>> failedRealms;
        int failedDisks = 0;
        for (const auto& protoVDiskId : groupInfo.vdiskids()) {
            TString vDiskId = GetVDiskId(protoVDiskId);
            auto itVDisk = MergedVDiskState.find(vDiskId);
            const TEvInterconnect::TNodeInfo* nodeInfo = nullptr;
            if (itVDisk != MergedVDiskState.end()) {
                TNodeId nodeId = itVDisk->second->nodeid();
                auto itNodeInfo = MergedNodeInfo.find(nodeId);
                if (itNodeInfo != MergedNodeInfo.end()) {
                    nodeInfo = itNodeInfo->second;
                }
                context.Location.mutable_storage()->mutable_node()->set_id(nodeId);
            } else {
                context.Location.mutable_storage()->mutable_node()->clear_id();
            }
            if (nodeInfo) {
                context.Location.mutable_storage()->mutable_node()->set_host(nodeInfo->Host);
                context.Location.mutable_storage()->mutable_node()->set_port(nodeInfo->Port);
            } else {
                context.Location.mutable_storage()->mutable_node()->clear_host();
                context.Location.mutable_storage()->mutable_node()->clear_port();
            }
            Ydb::Monitoring::StorageVDiskStatus& vDiskStatus = *storageGroupStatus.add_vdisks();
            FillVDiskStatus(vDiskId, itVDisk != MergedVDiskState.end() ? *itVDisk->second : NKikimrWhiteboard::TVDiskStateInfo(), vDiskStatus, {&context, "VDISK"});
            ++disksColors[vDiskStatus.overall()];
            switch (vDiskStatus.overall()) {
            case Ydb::Monitoring::StatusFlag::BLUE: // disk is good, but not available
            case Ydb::Monitoring::StatusFlag::RED: // disk is bad, probably not available
            case Ydb::Monitoring::StatusFlag::GREY: // the status is absent, the disk is not available
                IncrementFor(failedRealms, protoVDiskId.ring());
                ++failedDisks;
                break;
            default:
                break;
            }
        }

        context.Location.mutable_storage()->clear_node(); // group doesn't have node
        context.OverallStatus = MinStatus(context.OverallStatus, Ydb::Monitoring::StatusFlag::YELLOW);

        if (groupInfo.erasurespecies() == NONE) {
            if (failedDisks > 0) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", "group-state", {"vdisk-state"});
            }
        } else if (groupInfo.erasurespecies() == BLOCK_4_2) {
            if (failedDisks > 2) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", "group-state", {"vdisk-state"});
            } else if (failedDisks > 1) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", "group-state", {"vdisk-state"});
            } else if (failedDisks > 0) {
                if (disksColors[Ydb::Monitoring::StatusFlag::BLUE] == failedDisks) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", "group-state", {"vdisk-state"});
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", "group-state", {"vdisk-state"});
                }
            }
        } else if (groupInfo.erasurespecies() == MIRROR_3_DC) {
            if (failedRealms.size() > 2 || (failedRealms.size() == 2 && failedRealms[0].second > 1 && failedRealms[1].second > 1)) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", "group-state", {"vdisk-state"});
            } else if (failedRealms.size() == 2) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", "group-state", {"vdisk-state"});
            } else if (failedDisks > 0) {
                if (disksColors[Ydb::Monitoring::StatusFlag::BLUE] == failedDisks) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", "group-state", {"vdisk-state"});
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", "group-state", {"vdisk-state"});
                }
            }
        }

        storageGroupStatus.set_overall(context.GetOverallStatus());
    }

    void FillPoolStatus(const TString& poolName, const TStoragePoolState& pool, Ydb::Monitoring::StoragePoolStatus& storagePoolStatus, TSelfCheckContext context) {
        context.Location.mutable_storage()->mutable_pool()->set_name(poolName);
        storagePoolStatus.set_id(poolName);
        for (auto groupId : pool.Groups) {
            auto itGroup = MergedBSGroupState.find(groupId);
            if (itGroup != MergedBSGroupState.end()) {
                FillGroupStatus(groupId, *itGroup->second, *storagePoolStatus.add_groups(), {&context, "STORAGE_GROUP"});
            }
        }
        switch (context.GetOverallStatus()) {
            case Ydb::Monitoring::StatusFlag::BLUE:
            case Ydb::Monitoring::StatusFlag::YELLOW:
                context.ReportStatus(context.GetOverallStatus(), "Pool degraded", "pool-state", {"group-state"});
                break;
            case Ydb::Monitoring::StatusFlag::ORANGE:
                context.ReportStatus(context.GetOverallStatus(), "Pool has no redundancy", "pool-state", {"group-state"});
                break;
            case Ydb::Monitoring::StatusFlag::RED:
                context.ReportStatus(context.GetOverallStatus(), "Pool failed", "pool-state", {"group-state"});
                break;
            default:
                break;
        }
        storagePoolStatus.set_overall(context.GetOverallStatus());
    }

    void FillStorage(TDatabaseState& databaseState, Ydb::Monitoring::StorageStatus& storageStatus, TSelfCheckContext context) {
        if (databaseState.StoragePoolNames.empty()) {
            // pointless in real life
            // context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There are no storage pools");
        } else {
            for (const TString& poolName : databaseState.StoragePoolNames) {
                auto itStoragePoolState = StoragePoolState.find(poolName);
                if (itStoragePoolState != StoragePoolState.end()) {
                    if (!itStoragePoolState->second.AuthenticGroups.empty()) {
                        itStoragePoolState->second.Groups = itStoragePoolState->second.AuthenticGroups;
                    }
                    FillPoolStatus(poolName, itStoragePoolState->second, *storageStatus.add_pools(), {&context, "STORAGE_POOL"});
                    StoragePoolSeen.emplace(poolName);
                }
            }
            switch (context.GetOverallStatus()) {
                case Ydb::Monitoring::StatusFlag::BLUE:
                case Ydb::Monitoring::StatusFlag::YELLOW:
                    context.ReportStatus(context.GetOverallStatus(), "Storage degraded", "storage-state", {"pool-state"});
                    break;
                case Ydb::Monitoring::StatusFlag::ORANGE:
                    context.ReportStatus(context.GetOverallStatus(), "Storage has no redundancy", "storage-state", {"pool-state"});
                    break;
                case Ydb::Monitoring::StatusFlag::RED:
                    context.ReportStatus(context.GetOverallStatus(), "Storage failed", "storage-state", {"pool-state"});
                    break;
                default:
                    break;
            }
        }
        storageStatus.set_overall(context.GetOverallStatus());
    }

    void FillResult(Ydb::Monitoring::SelfCheckResult& result) {
        Ydb::Monitoring::StatusFlag::Status overall = Ydb::Monitoring::StatusFlag::GREY;
        std::unordered_set<std::pair<TString, TString>> issueIds;
        bool hasDegraded = false;
        for (auto& [path, state] : DatabaseState) {
            Ydb::Monitoring::DatabaseStatus& databaseStatus(*result.add_database_status());
            TSelfCheckResult context;
            context.Type = "DATABASE";
            context.Location.mutable_database()->set_name(path);
            databaseStatus.set_name(path);
            FillCompute(state, *databaseStatus.mutable_compute(), {&context, "COMPUTE"});
            FillStorage(state, *databaseStatus.mutable_storage(), {&context, "STORAGE"});
            if (databaseStatus.compute().overall() != Ydb::Monitoring::StatusFlag::GREEN
                    && databaseStatus.storage().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
                context.ReportStatus(MaxStatus(databaseStatus.compute().overall(), databaseStatus.storage().overall()),
                    "Database has multiple issues", "database-state", {"compute-state", "storage-state"});
            } else if (databaseStatus.compute().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
                context.ReportStatus(databaseStatus.compute().overall(), "Database has compute issues", "database-state", {"compute-state"});
            } else if (databaseStatus.storage().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
                context.ReportStatus(databaseStatus.storage().overall(), "Database has storage issues", "database-state", {"storage-state"});
            }
            databaseStatus.set_overall(context.GetOverallStatus());
            overall = MaxStatus(overall, context.GetOverallStatus());
            for (auto& issueRecord : context.IssueLog) {
                std::pair<TString, TString> key{issueRecord.IssueLog.location().database().name(), issueRecord.IssueLog.id()};
                if (issueIds.emplace(key).second) {
                    result.mutable_issue_log()->Add()->CopyFrom(issueRecord.IssueLog);
                }
            }
            if (!hasDegraded && overall != Ydb::Monitoring::StatusFlag::GREEN && context.HasTags({"storage-state"})) {
                hasDegraded = true;
            }
        }
        if (DatabaseState.empty()) {
            Ydb::Monitoring::DatabaseStatus& databaseStatus(*result.add_database_status());
            TSelfCheckResult context;
            context.Location.mutable_database()->set_name(DomainPath);
            databaseStatus.set_name(DomainPath);
            {
                FillSystemTablets({&context, "SYSTEM_TABLET"});
                overall = MaxStatus(overall, context.GetOverallStatus());
            }
        }
        if (!FilterDatabase) {
            TDatabaseState unknownDatabase;
            for (auto& [name, pool] : StoragePoolState) {
                if (StoragePoolSeen.count(name) == 0) {
                    unknownDatabase.StoragePoolNames.push_back(name);
                }
            }
            if (!unknownDatabase.StoragePoolNames.empty()) {
                Ydb::Monitoring::DatabaseStatus& databaseStatus(*result.add_database_status());
                TSelfCheckResult context;
                FillStorage(unknownDatabase, *databaseStatus.mutable_storage(), {&context, "STORAGE"});
                databaseStatus.set_overall(context.GetOverallStatus());
                overall = MaxStatus(overall, context.GetOverallStatus());
                for (auto& issueRecord : context.IssueLog) {
                    std::pair<TString, TString> key{issueRecord.IssueLog.location().database().name(), issueRecord.IssueLog.id()};
                    if (issueIds.emplace(key).second) {
                        result.mutable_issue_log()->Add()->CopyFrom(issueRecord.IssueLog);
                    }
                }
            }
        }
        switch (overall) {
        case Ydb::Monitoring::StatusFlag::GREEN:
            result.set_self_check_result(Ydb::Monitoring::SelfCheck::GOOD);
            break;
        case Ydb::Monitoring::StatusFlag::YELLOW:
            if (hasDegraded) {
                result.set_self_check_result(Ydb::Monitoring::SelfCheck::DEGRADED);
            } else {
                result.set_self_check_result(Ydb::Monitoring::SelfCheck::GOOD);
            }
            break;
        case Ydb::Monitoring::StatusFlag::BLUE:
            result.set_self_check_result(Ydb::Monitoring::SelfCheck::DEGRADED);
            break;
        case Ydb::Monitoring::StatusFlag::ORANGE:
            result.set_self_check_result(Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED);
            break;
        case Ydb::Monitoring::StatusFlag::RED:
            result.set_self_check_result(Ydb::Monitoring::SelfCheck::EMERGENCY);
            break;
        default:
            break;
        }
    }

    void ReplyAndPassAway() {
        THolder<TEvSelfCheckResult> response = MakeHolder<TEvSelfCheckResult>();
        Ydb::Monitoring::SelfCheckResult& result = response->Result;

        AggregateHiveInfo();
        AggregateHiveNodeStats();
        AggregateBSControllerState();

        for (auto& [requestId, request] : TabletRequests.RequestsInFlight) {
            auto tabletId = request.TabletId;
            TabletRequests.TabletStates[tabletId].IsUnresponsive = true;
        }

        FillResult(result);

        if (!Request->Request.return_verbose_status()) {
            result.clear_database_status();
        }
        if (Request->Request.minimum_status() != Ydb::Monitoring::StatusFlag::UNSPECIFIED) {
            for (auto itIssue = result.mutable_issue_log()->begin(); itIssue != result.mutable_issue_log()->end();) {
                if (itIssue->status() < Request->Request.minimum_status()) {
                    itIssue = result.mutable_issue_log()->erase(itIssue);
                } else {
                    ++itIssue;
                }
            }
        }
        if (Request->Request.maximum_level() != 0) {
            for (auto itIssue = result.mutable_issue_log()->begin(); itIssue != result.mutable_issue_log()->end();) {
                if (itIssue->level() > Request->Request.maximum_level()) {
                    itIssue = result.mutable_issue_log()->erase(itIssue);
                } else {
                    ++itIssue;
                }
            }
        }

        for (TActorId pipe : PipeClients) {
            NTabletPipe::CloseClient(SelfId(), pipe);
        }

        Send(Sender, response.Release(), 0, Cookie);

        for (TNodeId nodeId : NodeIds) {
            Send(TlsActivationContext->ActorSystem()->InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        PassAway();
    }
};

template<typename RequestType>
class TNodeCheckRequest : public TActorBootstrapped<TNodeCheckRequest<RequestType>> {
public:
    using TBase = TActorBootstrapped<TNodeCheckRequest<RequestType>>;
    using TThis = TNodeCheckRequest<RequestType>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MONITORING_REQUEST; }

    struct TEvPrivate {
        enum EEv {
            EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvError,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

        struct TEvResult : TEventLocal<TEvResult, EvResult> {
            Ydb::Monitoring::NodeCheckResponse Response;

            TEvResult(Ydb::Monitoring::NodeCheckResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvError : TEventLocal<TEvError, EvError> {
            NGrpc::TGrpcStatus Status;

            TEvError(NGrpc::TGrpcStatus&& status)
                : Status(std::move(status))
            {}
        };
    };

    TDuration Timeout = TDuration::MilliSeconds(10000);
    std::shared_ptr<NGrpc::TGRpcClientLow> GRpcClientLow;
    TActorId Sender;
    THolder<RequestType> Request;
    ui64 Cookie;
    Ydb::Monitoring::SelfCheckResult Result;

    TNodeCheckRequest(std::shared_ptr<NGrpc::TGRpcClientLow> grpcClient, const TActorId& sender, THolder<RequestType> request, ui64 cookie)
        : GRpcClientLow(grpcClient)
        , Sender(sender)
        , Request(std::move(request))
        , Cookie(cookie)
    {
        Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_UNSPECIFIED);
    }

    void Bootstrap();

    void AddIssue(Ydb::Monitoring::StatusFlag::Status status, const TString& message) {
        auto* issue = Result.add_issue_log();
        issue->set_id(std::to_string(Result.issue_log_size()));
        issue->set_status(status);
        issue->set_message(message);
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        NGrpc::TGRpcClientConfig config;
        for (const auto& systemStateInfo : ev->Get()->Record.GetSystemStateInfo()) {
            for (const auto& endpoint : systemStateInfo.GetEndpoints()) {
                if (endpoint.GetName() == "grpc") {
                    config.Locator = "localhost" + endpoint.GetAddress();
                    break;
                } else if (endpoint.GetName() == "grpcs") {
                    config.Locator = "localhost" + endpoint.GetAddress();
                    config.EnableSsl = true;
                    break;
                }
            }
            break;
        }
        if (!config.Locator) {
            AddIssue(Ydb::Monitoring::StatusFlag::RED, "Couldn't find local gRPC endpoint");
            ReplyAndPassAway();
        }
        NActors::TActorSystem* actorSystem = TlsActivationContext->ActorSystem();
        NActors::TActorId actorId = TBase::SelfId();
        Ydb::Monitoring::NodeCheckRequest request;
        NGrpc::TResponseCallback<Ydb::Monitoring::NodeCheckResponse> responseCb =
            [actorId, actorSystem, context = GRpcClientLow->CreateContext()](NGrpc::TGrpcStatus&& status, Ydb::Monitoring::NodeCheckResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new typename TEvPrivate::TEvResult(std::move(response)));
            } else {
                actorSystem->Send(actorId, new typename TEvPrivate::TEvError(std::move(status)));
            }
        };
        NGrpc::TCallMeta meta;
        meta.Timeout = Timeout;
        auto service = GRpcClientLow->CreateGRpcServiceConnection<::Ydb::Monitoring::V1::MonitoringService>(config);
        service->DoRequest(request, std::move(responseCb), &Ydb::Monitoring::V1::MonitoringService::Stub::AsyncNodeCheck, meta);
    }

    void Handle(typename TEvPrivate::TEvResult::TPtr& ev) {
        auto& operation(ev->Get()->Response.operation());
        if (operation.ready() && operation.status() == Ydb::StatusIds::SUCCESS) {
            operation.result().UnpackTo(&Result);
        } else {
            Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_MAINTENANCE_REQUIRED);
            AddIssue(Ydb::Monitoring::StatusFlag::RED, "Local gRPC returned error");
        }
        ReplyAndPassAway();
    }

    void Handle(typename TEvPrivate::TEvError::TPtr& ev) {
        Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_MAINTENANCE_REQUIRED);
        AddIssue(Ydb::Monitoring::StatusFlag::RED, "Local gRPC request failed");
        Y_UNUSED(ev);
        ReplyAndPassAway();
    }

    void HandleTimeout() {
        Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_MAINTENANCE_REQUIRED);
        AddIssue(Ydb::Monitoring::StatusFlag::RED, "Timeout");
        ReplyAndPassAway();
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev, const TActorContext&) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvPrivate::TEvResult, Handle);
            hFunc(TEvPrivate::TEvError, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void FillResult(Ydb::Monitoring::SelfCheckResult& result) {
        result = std::move(Result);
    }

    void ReplyAndPassAway();
};

template<>
void TNodeCheckRequest<TEvNodeCheckRequest>::ReplyAndPassAway() {
    THolder<TEvSelfCheckResult> response = MakeHolder<TEvSelfCheckResult>();
    Ydb::Monitoring::SelfCheckResult& result = response->Result;
    FillResult(result);
    Send(Sender, response.Release(), 0, Cookie);
    PassAway();
}

template<>
void TNodeCheckRequest<NMon::TEvHttpInfo>::ReplyAndPassAway() {
    static const char HTTPJSON_GOOD[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/json\r\n\r\n";
    static const char HTTPJSON_NOT_GOOD[] = "HTTP/1.1 500 Failed\r\nContent-Type: application/json\r\n\r\n";

    Ydb::Monitoring::SelfCheckResult result;
    FillResult(result);
    auto config = NProtobufJson::TProto2JsonConfig()
            .SetFormatOutput(false)
            .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
    TStringStream json;
    if (result.self_check_result() == Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_GOOD) {
        json << HTTPJSON_GOOD;
    } else {
        json << HTTPJSON_NOT_GOOD;
    }
    NProtobufJson::Proto2Json(result, json, config);
    Send(Sender, new NMon::TEvHttpInfoRes(json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom), 0, Cookie);
    PassAway();
}

template<>
void TNodeCheckRequest<TEvNodeCheckRequest>::Bootstrap() {
    if (Request->Request.operation_params().has_operation_timeout()) {
        Timeout = GetDuration(Request->Request.operation_params().operation_timeout());
    }
    Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_GOOD);
    ReplyAndPassAway();
}

template<>
void TNodeCheckRequest<NMon::TEvHttpInfo>::Bootstrap() {
    TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(TBase::SelfId().NodeId());
    TBase::Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest());
    const auto& params(Request->Request.GetParams());
    Timeout = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("timeout"), Timeout.MilliSeconds()));
    TBase::Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
}

class THealthCheckService : public TActorBootstrapped<THealthCheckService> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MONITORING_SERVICE; }

    THealthCheckService()
    {
    }

    void Bootstrap() {
        TMon* mon = AppData()->Mon;
        if (mon) {
            mon->RegisterActorPage({
                .RelPath = "status",
                .ActorSystem = TlsActivationContext->ExecutorThread.ActorSystem,
                .ActorId = SelfId(),
            });
        }
        Become(&THealthCheckService::StateWork);
    }

    void Handle(TEvSelfCheckRequest::TPtr& ev) {
        Register(new TSelfCheckRequest(ev->Sender, ev.Get()->Release(), ev->Cookie));
    }

    std::shared_ptr<NGrpc::TGRpcClientLow> GRpcClientLow;

    void Handle(TEvNodeCheckRequest::TPtr& ev) {
        if (!GRpcClientLow) {
            GRpcClientLow = std::make_shared<NGrpc::TGRpcClientLow>();
        }
        Register(new TNodeCheckRequest<TEvNodeCheckRequest>(GRpcClientLow, ev->Sender, ev.Get()->Release(), ev->Cookie));
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        if (ev->Get()->Request.GetPath() == "/status") {
            if (!GRpcClientLow) {
                GRpcClientLow = std::make_shared<NGrpc::TGRpcClientLow>();
            }
            Register(new TNodeCheckRequest<NMon::TEvHttpInfo>(GRpcClientLow, ev->Sender, ev.Get()->Release(), ev->Cookie));
        } else {
            Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND, 0, NMon::IEvHttpInfoRes::EContentType::Custom), 0, ev->Cookie);
        }
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev, const TActorContext&) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSelfCheckRequest, Handle);
            hFunc(TEvNodeCheckRequest, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};

IActor* CreateHealthCheckService() {
    return new THealthCheckService();
}

} // namespace NHealthCheck
} // namespace NKikimr
