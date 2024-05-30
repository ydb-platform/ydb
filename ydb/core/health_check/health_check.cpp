#include "health_check.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/digest/old_crc/crc.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/library/grpc/client/grpc_client_low.h>

#include <util/random/shuffle.h>

#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/core/util/tuples.h>

#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <regex>

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

using NNodeWhiteboard::TNodeId;
using NNodeWhiteboard::TTabletId;

namespace NHealthCheck {

using namespace NActors;
using namespace Ydb;

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

    TSelfCheckRequest(const TActorId& sender, THolder<TEvSelfCheckRequest> request, ui64 cookie)
        : Sender(sender)
        , Request(std::move(request))
        , Cookie(cookie)
    {}

    using TGroupId = ui32;

    enum ETags {
        None,
        DBState,
        StorageState,
        PoolState,
        GroupState,
        VDiskState,
        PDiskState,
        NodeState,
        VDiskSpace,
        PDiskSpace,
        ComputeState,
        TabletState,
        SystemTabletState,
        OverloadState,
        SyncState,
        Uptime,
    };

    enum ETimeoutTag {
        TimeoutBSC,
        TimeoutFinal,
    };

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
            bool IsHiveSynchronizationPeriod = false;
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
            bool Leader;
            int Count = 1;
            TStackVec<TString> Identifiers;

            TNodeTabletStateCount(const NKikimrHive::TTabletInfo& info, const TTabletStateSettings& settings) {
                Type = info.tablettype();
                Leader = info.followerid() == 0;
                if (info.volatilestate() == NKikimrHive::TABLET_VOLATILE_STATE_STOPPED) {
                    State = ETabletState::Stopped;
                } else if (!settings.IsHiveSynchronizationPeriod
                            && info.volatilestate() != NKikimrHive::TABLET_VOLATILE_STATE_RUNNING
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
                return State == o.State && Type == o.Type && Leader == o.Leader;
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
        TString Name;
        THashSet<TGroupId> Groups;
    };

    struct TDatabaseState {
        TTabletId HiveId = {};
        TPathId ResourcePathId = {};
        TVector<TNodeId> ComputeNodeIds;
        THashSet<ui64> StoragePools;        // BSConfig case
        THashSet<TString> StoragePoolNames; // no BSConfig case
        THashMap<std::pair<TTabletId, NNodeWhiteboard::TFollowerId>, const NKikimrHive::TTabletInfo*> MergedTabletState;
        THashMap<TNodeId, TNodeTabletState> MergedNodeTabletState;
        THashMap<TNodeId, ui32> NodeRestartsPerPeriod;
        ui64 StorageQuota = 0;
        ui64 StorageUsage = 0;
        TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;
    };

    struct TSelfCheckResult {
        struct TIssueRecord {
            Ydb::Monitoring::IssueLog IssueLog;
            ETags Tag;
        };

        Ydb::Monitoring::StatusFlag::Status OverallStatus = Ydb::Monitoring::StatusFlag::GREY;
        TList<TIssueRecord> IssueRecords;
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
            const Ydb::Monitoring::Location& location(issueLog.location());
            if (location.database().name()) {
                id << '-' << crc16(location.database().name());
            }
            id << '-' << crc16(issueLog.message());
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
            if (!location.storage().pool().group().vdisk().id().empty()) {
                id << '-' << location.storage().pool().group().vdisk().id()[0];
            } else {
                if (!location.storage().pool().group().id().empty()) {
                    id << '-' << location.storage().pool().group().id()[0];
                } else {
                    if (location.storage().pool().name()) {
                        id << '-' << crc16(location.storage().pool().name());
                    }
                }
            }
            if (!location.storage().pool().group().vdisk().pdisk().empty() && location.storage().pool().group().vdisk().pdisk()[0].id()) {
                id << '-' << location.storage().pool().group().vdisk().pdisk()[0].id();
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
                          ETags setTag = ETags::None,
                          std::initializer_list<ETags> includeTags = {}) {
            OverallStatus = MaxStatus(OverallStatus, status);
            if (IsErrorStatus(status)) {
                std::vector<TString> reason;
                if (includeTags.size() != 0) {
                    for (const TIssueRecord& record : IssueRecords) {
                        for (const ETags& tag : includeTags) {
                            if (record.Tag == tag) {
                                reason.push_back(record.IssueLog.id());
                                break;
                            }
                        }
                    }
                }
                std::sort(reason.begin(), reason.end());
                reason.erase(std::unique(reason.begin(), reason.end()), reason.end());
                TIssueRecord& issueRecord(*IssueRecords.emplace(IssueRecords.begin()));
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
                if (setTag != ETags::None) {
                    issueRecord.Tag = setTag;
                }
            }
        }

        bool HasTags(std::initializer_list<ETags> tags) const {
            for (const TIssueRecord& record : IssueRecords) {
                for (const ETags tag : tags) {
                    if (record.Tag == tag) {
                        return true;
                    }
                }
            }
            return false;
        }

        Ydb::Monitoring::StatusFlag::Status FindMaxStatus(std::initializer_list<ETags> tags) const {
            Ydb::Monitoring::StatusFlag::Status status = Ydb::Monitoring::StatusFlag::GREY;
            for (const TIssueRecord& record : IssueRecords) {
                for (const ETags tag : tags) {
                    if (record.Tag == tag) {
                        status = MaxStatus(status, record.IssueLog.status());
                    }
                }
            }
            return status;
        }

        void ReportWithMaxChildStatus(const TString& message = {},
                                        ETags setTag = ETags::None,
                                        std::initializer_list<ETags> includeTags = {}) {
            if (HasTags(includeTags)) {
                ReportStatus(FindMaxStatus(includeTags), message, setTag, includeTags);
            }
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
            IssueRecords.splice(IssueRecords.end(), std::move(lower.IssueRecords));
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
    THashMap<TString, THashSet<TString>> PathsByPoolName;
    THashMap<TString, Ydb::Cms::GetDatabaseStatusResult> DatabaseStatusByPath;
    THashMap<TString, THolder<NTenantSlotBroker::TEvTenantSlotBroker::TEvTenantState>> TenantStateByPath;
    THashMap<TString, THolder<NSchemeCache::TSchemeCacheNavigate>> NavigateResult;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveDomainStats>> HiveDomainStats;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStats;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveInfo>> HiveInfo;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    THashMap<TNodeId, const TEvInterconnect::TNodeInfo*> MergedNodeInfo;
    THolder<TEvBlobStorage::TEvControllerConfigResponse> BaseConfig;
    bool RequestedStorageConfig = false;
    THashSet<TNodeId> UnknownStaticGroups;

    THashSet<TNodeId> NodeIds;
    THashSet<TNodeId> StorageNodeIds;
    THashSet<TNodeId> ComputeNodeIds;
    std::unordered_map<std::pair<TNodeId, int>, ui32> NodeRetries;
    ui32 MaxRetries = 20;
    TDuration RetryDelay = TDuration::MilliSeconds(250);

    THashMap<TString, TDatabaseState> DatabaseState;
    THashMap<TPathId, TString> SharedDatabases;

    THashMap<TNodeId, THolder<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse>> NodeSystemState;
    THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*> MergedNodeSystemState;

    std::unordered_map<TString, const NKikimrBlobStorage::TBaseConfig::TPDisk*> BSConfigPDisks;
    std::unordered_map<TString, const NKikimrBlobStorage::TBaseConfig::TVSlot*> BSConfigVSlots;
    std::unordered_map<ui32, const NKikimrBlobStorage::TBaseConfig::TGroup*> BSConfigGroups;
    std::unordered_map<ui32, const NKikimrBlobStorage::TBaseConfig::TNode*> BSConfigNodes;

    std::unordered_set<TString> ValidVDisks;
    std::unordered_set<TString> ValidPDisks;
    std::unordered_set<TGroupId> ValidGroups;

    // BSConfig case
    THashMap<ui64, TStoragePoolState> StoragePoolState;
    THashSet<ui64> StoragePoolSeen;
    // no BSConfig case
    THashMap<TString, TStoragePoolState> StoragePoolStateByName;
    THashSet<TString> StoragePoolSeenByName;

    THashSet<TNodeId> UnavailableStorageNodes;
    THashSet<TNodeId> UnavailableComputeNodes;

    THashMap<TNodeId, THolder<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse>> NodeVDiskState;
    TList<NKikimrWhiteboard::TVDiskStateInfo> VDisksAppended;
    std::unordered_map<TString, const NKikimrWhiteboard::TVDiskStateInfo*> MergedVDiskState;

    THashMap<TNodeId, THolder<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse>> NodePDiskState;
    TList<NKikimrWhiteboard::TPDiskStateInfo> PDisksAppended;
    std::unordered_map<TString, const NKikimrWhiteboard::TPDiskStateInfo*> MergedPDiskState;

    THashMap<TNodeId, THolder<NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse>> NodeBSGroupState;
    TList<NKikimrWhiteboard::TBSGroupStateInfo> BSGroupAppended;
    std::unordered_map<TGroupId, const NKikimrWhiteboard::TBSGroupStateInfo*> MergedBSGroupState;

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

    TDuration Timeout = TDuration::MilliSeconds(20000);
    static constexpr TStringBuf STATIC_STORAGE_POOL_NAME = "static";

    bool IsSpecificDatabaseFilter() const {
        return FilterDatabase && FilterDatabase != DomainPath;
    }

    bool IsTimeDifferenceCheckNode(const TNodeId nodeId) const {
        if (!IsSpecificDatabaseFilter()) {
            return true;
        }

        auto it = DatabaseState.find(FilterDatabase);
        if (it == DatabaseState.end()) {
            return false;
        }
        auto& computeNodeIds = it->second.ComputeNodeIds;

        return std::find(computeNodeIds.begin(), computeNodeIds.end(), nodeId) != computeNodeIds.end();
    }

    void Bootstrap() {
        FilterDatabase = Request->Database;
        if (Request->Request.operation_params().has_operation_timeout()) {
            Timeout = GetDuration(Request->Request.operation_params().operation_timeout());
        }
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
                TTenantInfo& tenant = TenantByPath[DomainPath];
                tenant.Name = DomainPath;
                RequestSchemeCacheNavigate(DomainPath);
                RequestListTenants();
            } else if (FilterDatabase != DomainPath) {
                RequestTenantStatus(FilterDatabase);
            } else {
                TTenantInfo& tenant = TenantByPath[DomainPath];
                tenant.Name = DomainPath;
                RequestSchemeCacheNavigate(DomainPath);
            }
        }

        if (RootHiveId) {
            TabletRequests.TabletStates[RootHiveId].Database = DomainPath;
            TabletRequests.TabletStates[RootHiveId].Type = TTabletTypes::Hive;
            //RequestHiveDomainStats(RootHiveId);
            RequestHiveNodeStats(RootHiveId);
            RequestHiveInfo(RootHiveId);
        }

        if (RootSchemeShardId && !IsSpecificDatabaseFilter()) {
            TabletRequests.TabletStates[RootSchemeShardId].Database = DomainPath;
            TabletRequests.TabletStates[RootSchemeShardId].Type = TTabletTypes::SchemeShard;
            RequestDescribe(RootSchemeShardId, DomainPath);
        }

        if (BsControllerId) {
            TabletRequests.TabletStates[BsControllerId].Database = DomainPath;
            TabletRequests.TabletStates[BsControllerId].Type = TTabletTypes::BSController;
            RequestConfig();
        }

        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        ++Requests;

        Become(&TThis::StateWait);
        Schedule(TDuration::MilliSeconds(Timeout.MilliSeconds() / 2), new TEvents::TEvWakeup(TimeoutBSC)); // 50% timeout (for bsc)
        Schedule(Timeout, new TEvents::TEvWakeup(TimeoutFinal)); // timeout for the rest
    }

    bool NeedWhiteboardInfoForGroup(TGroupId groupId) {
        return BaseConfig && UnknownStaticGroups.count(groupId) != 0 || !BaseConfig && IsStaticGroup(groupId);
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr ev) {
        if (const NKikimrBlobStorage::TStorageConfig& config = *ev->Get()->Config; config.HasBlobStorageConfig()) {
            if (const auto& bsConfig = config.GetBlobStorageConfig(); bsConfig.HasServiceSet()) {
                const auto& staticConfig = bsConfig.GetServiceSet();
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

                    auto groupId = vDisk.GetVDiskID().GetGroupID();
                    if (NeedWhiteboardInfoForGroup(groupId)) {
                        RequestStorageNode(vDisk.GetVDiskLocation().GetNodeID());
                    }
                }
                for (const NKikimrBlobStorage::TGroupInfo& group : staticConfig.groups()) {
                    ValidGroups.emplace(group.GetGroupID());
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
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvGetTenantStatusResponse, Handle);
            hFunc(TEvHive::TEvResponseHiveDomainStats, Handle);
            hFunc(TEvHive::TEvResponseHiveNodeStats, Handle);
            hFunc(TEvHive::TEvResponseHiveInfo, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle)
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvPrivate::TEvRetryNodeWhiteboard, Handle);
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

    void RequestConfig() {
        THolder<TEvBlobStorage::TEvControllerConfigRequest> request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
        request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool()->SetBoxId(Max<ui64>());
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

    void RequestStorageConfig() {
        if (!RequestedStorageConfig) {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(false));
            RequestedStorageConfig = true;
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

    void HandleTimeout(TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
            case TimeoutBSC:
                if (!BaseConfig) {
                    RequestStorageConfig();
                }
                break;
            case TimeoutFinal:
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
        NodesInfo = ev->Release();
        for (const auto& ni : NodesInfo->Nodes) {
            MergedNodeInfo[ni.NodeId] = &ni;
            if (IsStaticNode(ni.NodeId) && needComputeFromStaticNodes) {
                DatabaseState[DomainPath].ComputeNodeIds.push_back(ni.NodeId);
                RequestComputeNode(ni.NodeId);
            }
        }
        RequestDone("TEvNodesInfo");
    }

    bool IsStaticGroup(TGroupId groupId) {
        return !(groupId & 0x80000000);
    }

    bool NeedWhiteboardInfo(const NKikimrBlobStorage::TBaseConfig::TGroup& group) {
        return IsStaticGroup(group.groupid())
                        && (group.operatingstatus() == NKikimrBlobStorage::TGroupStatus::UNKNOWN
                        || group.operatingstatus() == NKikimrBlobStorage::TGroupStatus::DISINTEGRATED);
    }

    void AggregateUnknownStatusStaticGroups() {
        const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(BaseConfig->Record);
        const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
        const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
        for (const NKikimrBlobStorage::TBaseConfig::TGroup& group : pbConfig.GetGroup()) {
            if (NeedWhiteboardInfo(group)) {
                UnknownStaticGroups.emplace(group.groupid());
            }
        }
    }

    bool NeedWhiteboardForStaticGroupsWithUnknownStatus() {
        return RequestedStorageConfig && !IsSpecificDatabaseFilter();
    }

    bool ReadyBscInfo(TGroupId groupId) {
        auto itGroup = BSConfigGroups.find(groupId);
        if (itGroup != BSConfigGroups.end()) {
            return !NeedWhiteboardInfo(*itGroup->second);
        }
        return BaseConfig != nullptr;
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(ev->Get()->Record);
        if (pbRecord.HasResponse() && pbRecord.GetResponse().StatusSize() > 1) {
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            if (pbStatus.HasBaseConfig()) {
                BaseConfig = ev->Release();
                AggregateUnknownStatusStaticGroups();
                if (UnknownStaticGroups.size() > 0) {
                    RequestStorageConfig();
                }
            }
        }
        RequestDone("TEvControllerConfigResponse");
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        if (ev->Get()->GetRecord().status() == NKikimrScheme::StatusSuccess) {
            TString path = ev->Get()->GetRecord().path();
            TDatabaseState& state(DatabaseState[path]);
            for (const auto& storagePool : ev->Get()->GetRecord().pathdescription().domaindescription().storagepools()) {
                TString storagePoolName = storagePool.name();
                state.StoragePoolNames.emplace(storagePoolName);
                PathsByPoolName[storagePoolName].emplace(path); // no poolId in TEvDescribeSchemeResult, so it's neccesary to keep poolNames instead
            }
            if (path == DomainPath) {
                state.StoragePoolNames.emplace(STATIC_STORAGE_POOL_NAME);
                state.StoragePools.emplace(0); // static group has poolId = 0
                StoragePoolState[0].Name = STATIC_STORAGE_POOL_NAME;
            }
            state.StorageUsage = ev->Get()->GetRecord().pathdescription().domaindescription().diskspaceusage().tables().totalsize();
            state.StorageQuota = ev->Get()->GetRecord().pathdescription().domaindescription().databasequotas().data_size_hard_quota();

            DescribeByPath[path] = ev->Release();
        }
        RequestDone("TEvDescribeSchemeResult");
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ResultSet.size() == 1 && ev->Get()->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            auto domainInfo = ev->Get()->Request->ResultSet.begin()->DomainInfo;
            TString path = CanonizePath(ev->Get()->Request->ResultSet.begin()->Path);
            if (domainInfo->IsServerless()) {
                if (NeedHealthCheckForServerless(domainInfo)) {
                    if (SharedDatabases.emplace(domainInfo->ResourcesDomainKey, path).second) {
                        RequestSchemeCacheNavigate(domainInfo->ResourcesDomainKey);
                    }
                    DatabaseState[path].ResourcePathId = domainInfo->ResourcesDomainKey;
                    DatabaseState[path].ServerlessComputeResourcesMode = domainInfo->ServerlessComputeResourcesMode;
                } else {
                    DatabaseState.erase(path);
                    DatabaseStatusByPath.erase(path);
                    RequestDone("TEvNavigateKeySetResult");
                    return;
                }
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

    bool NeedHealthCheckForServerless(TIntrusivePtr<NSchemeCache::TDomainInfo> domainInfo) const {
        return IsSpecificDatabaseFilter()
            || domainInfo->ServerlessComputeResourcesMode == NKikimrSubDomains::EServerlessComputeResourcesModeExclusive;
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
        TInstant aliveBarrier = TInstant::Now() - TDuration::Minutes(5);
        for (const NKikimrHive::THiveNodeStats& hiveStat : ev->Get()->Record.GetNodeStats()) {
            if (!hiveStat.HasLastAliveTimestamp() || TInstant::MilliSeconds(hiveStat.GetLastAliveTimestamp()) > aliveBarrier) {
                RequestComputeNode(hiveStat.GetNodeId());
            }
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

    static const int HIVE_SYNCHRONIZATION_PERIOD_MS = 10000;

    bool IsHiveSynchronizationPeriod(NKikimrHive::TEvResponseHiveInfo& hiveInfo) {
        return hiveInfo.GetResponseTimestamp() < hiveInfo.GetStartTimeTimestamp() + HIVE_SYNCHRONIZATION_PERIOD_MS;
    }

    void AggregateHiveInfo() {
        TNodeTabletState::TTabletStateSettings settings;
        settings.AliveBarrier = TInstant::Now() - TDuration::Minutes(5);
        for (const auto& [hiveId, hiveResponse] : HiveInfo) {
            if (hiveResponse) {
                settings.IsHiveSynchronizationPeriod = IsHiveSynchronizationPeriod(hiveResponse->Record);
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
                            state.NodeRestartsPerPeriod[hiveStat.GetNodeId()] = hiveStat.GetRestartsPerPeriod();
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
                    BSConfigPDisks.emplace(pDiskId, &pDisk);
                }
                std::unordered_map<NKikimrBlobStorage::TVSlotId, const NKikimrBlobStorage::TBaseConfig::TVSlot*> slotsIndex;
                for (const NKikimrBlobStorage::TBaseConfig::TVSlot& vSlot : pbConfig.GetVSlot()) {
                    slotsIndex[vSlot.GetVSlotId()] = &vSlot;
                    auto vSlotId = GetVSlotId(vSlot.GetVSlotId());
                    ValidVDisks.emplace(vSlotId);
                    BSConfigVSlots.emplace(vSlotId, &vSlot);
                }
                for (const NKikimrBlobStorage::TBaseConfig::TGroup& group : pbConfig.GetGroup()) {
                    auto groupId = group.GetGroupId();
                    auto poolId = group.GetStoragePoolId();
                    ValidGroups.emplace(groupId);
                    BSConfigGroups.emplace(groupId, &group);
                    StoragePoolState[poolId].Groups.emplace(group.groupid());
                }
                for (const NKikimrBlobStorage::TBaseConfig::TNode& node : pbConfig.GetNode()) {
                    auto nodeId = node.GetNodeId();
                    BSConfigNodes.emplace(nodeId, &node);
                }
            }
            const NKikimrBlobStorage::TConfigResponse::TStatus& spStatus(pbRecord.GetResponse().GetStatus(1));
            for (const NKikimrBlobStorage::TDefineStoragePool& pool : spStatus.GetStoragePool()) { // there is no specific pool for static group here
                ui64 poolId = pool.GetStoragePoolId();
                TString storagePoolName = pool.GetName();
                StoragePoolState[poolId].Name = storagePoolName;

                for (const TString& path : PathsByPoolName[storagePoolName]) {
                    DatabaseState[path].StoragePools.emplace(poolId);
                }
            }
        }
        for (auto itPDisk = BSConfigPDisks.begin(); itPDisk != BSConfigPDisks.end();) {
            if (ValidPDisks.count(itPDisk->first)) {
                ++itPDisk;
            } else {
                itPDisk = BSConfigPDisks.erase(itPDisk);
            }
        }
        for (auto itVDisk = BSConfigVSlots.begin(); itVDisk != BSConfigVSlots.end();) {
            if (ValidVDisks.count(itVDisk->first)) {
                ++itVDisk;
            } else {
                itVDisk = BSConfigVSlots.erase(itVDisk);
            }
        }
        for (auto itGroup = BSConfigGroups.begin(); itGroup != BSConfigGroups.end();) {
            if (ValidGroups.count(itGroup->first)) {
                ++itGroup;
            } else {
                itGroup = BSConfigGroups.erase(itGroup);
            }
        }
    }

    static Ydb::Monitoring::StatusFlag::Status MaxStatus(Ydb::Monitoring::StatusFlag::Status a, Ydb::Monitoring::StatusFlag::Status b) {
        return static_cast<Ydb::Monitoring::StatusFlag::Status>(std::max<int>(a, b));
    }

    static Ydb::Monitoring::StatusFlag::Status MinStatus(Ydb::Monitoring::StatusFlag::Status a, Ydb::Monitoring::StatusFlag::Status b) {
        return static_cast<Ydb::Monitoring::StatusFlag::Status>(std::min<int>(a, b));
    }

    static TString GetNodeLocation(const TEvInterconnect::TNodeInfo& nodeInfo) {
        return TStringBuilder() << nodeInfo.NodeId << '/' << nodeInfo.Host << ':' << nodeInfo.Port;
    }

    static void Check(TSelfCheckContext& context, const NKikimrWhiteboard::TSystemStateInfo::TPoolStats& poolStats) {
        if (poolStats.name() == "System" || poolStats.name() == "IC" || poolStats.name() == "IO") {
            if (poolStats.usage() >= 0.99) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Pool usage is over than 99%", ETags::OverloadState);
            } else if (poolStats.usage() >= 0.95) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Pool usage is over than 95%", ETags::OverloadState);
            } else {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        } else {
            if (poolStats.usage() >= 0.99) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Pool usage is over than 99%", ETags::OverloadState);
            } else {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        }
    }

    Ydb::Monitoring::StatusFlag::Status FillSystemTablets(TDatabaseState& databaseState, TSelfCheckContext context) {
        TString databaseId = context.Location.database().name();
        for (auto& [tabletId, tablet] : TabletRequests.TabletStates) {
            if (tablet.Database == databaseId) {
                auto tabletIt = databaseState.MergedTabletState.find(std::make_pair(tabletId, 0));
                if (tabletIt != databaseState.MergedTabletState.end()) {
                    auto nodeId = tabletIt->second->GetNodeID();
                    if (nodeId) {
                        FillNodeInfo(nodeId, context.Location.mutable_node());
                    }
                }

                context.Location.mutable_compute()->clear_tablet();
                auto& protoTablet = *context.Location.mutable_compute()->mutable_tablet();
                auto timeoutMs = Timeout.MilliSeconds();
                auto orangeTimeout = timeoutMs / 2;
                auto yellowTimeout = timeoutMs / 10;
                if (tablet.IsUnresponsive || tablet.MaxResponseTime >= TDuration::MilliSeconds(yellowTimeout)) {
                    if (tablet.Type != TTabletTypes::Unknown) {
                        protoTablet.set_type(TTabletTypes::EType_Name(tablet.Type));
                    }
                    protoTablet.add_id(ToString(tabletId));
                    if (tablet.IsUnresponsive) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet is unresponsive", ETags::SystemTabletState);
                    } else if (tablet.MaxResponseTime >= TDuration::MilliSeconds(orangeTimeout)) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, TStringBuilder() << "System tablet response time is over " << orangeTimeout << "ms", ETags::SystemTabletState);
                    } else if (tablet.MaxResponseTime >= TDuration::MilliSeconds(yellowTimeout)) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, TStringBuilder() << "System tablet response time is over " << yellowTimeout << "ms", ETags::SystemTabletState);
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
                    FillNodeInfo(nodeId, tabletContext.Location.mutable_node());
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
                            tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Tablets are restarting too often", ETags::TabletState);
                            break;
                        case TNodeTabletState::ETabletState::Dead:
                            computeTabletStatus.set_state("DEAD");
                            if (count.Leader) {
                                tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Tablets are dead", ETags::TabletState);
                            } else {
                                tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Followers are dead", ETags::TabletState);
                            }
                            break;
                    }
                    computeTabletStatus.set_overall(tabletContext.GetOverallStatus());
                    tabletsStatus = MaxStatus(tabletsStatus, tabletContext.GetOverallStatus());
                }
            }
        }
        return tabletsStatus;
    }

    void FillNodeInfo(TNodeId nodeId, Ydb::Monitoring::LocationNode* node) {
        const TEvInterconnect::TNodeInfo* nodeInfo = nullptr;
        auto itNodeInfo = MergedNodeInfo.find(nodeId);
        if (itNodeInfo != MergedNodeInfo.end()) {
            nodeInfo = itNodeInfo->second;
        }
        TString id(ToString(nodeId));

        node->set_id(nodeId);
        if (nodeInfo) {
            node->set_host(nodeInfo->Host);
            node->set_port(nodeInfo->Port);
        }
    }

    void FillComputeNodeStatus(TDatabaseState& databaseState,TNodeId nodeId, Ydb::Monitoring::ComputeNodeStatus& computeNodeStatus, TSelfCheckContext context) {
        FillNodeInfo(nodeId, context.Location.mutable_compute()->mutable_node());

        TSelfCheckContext rrContext(&context, "NODE_UPTIME");
        if (databaseState.NodeRestartsPerPeriod[nodeId] >= 30) {
            rrContext.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Node is restarting too often", ETags::Uptime);
        } else if (databaseState.NodeRestartsPerPeriod[nodeId] >= 10) {
            rrContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "The number of node restarts has increased", ETags::Uptime);
        } else {
            rrContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
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
                    laContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "LoadAverage above 100%", ETags::OverloadState);
                } else {
                    laContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                }
                loadAverageStatus.set_overall(laContext.GetOverallStatus());
            }
        } else {
            // context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
            //                      TStringBuilder() << "Compute node is not available",
             //                      ETags::NodeState);
        }
        computeNodeStatus.set_id(ToString(nodeId));
        computeNodeStatus.set_overall(context.GetOverallStatus());
    }

    void FillCompute(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckContext context) {
        TVector<TNodeId>* computeNodeIds = &databaseState.ComputeNodeIds;
        if (databaseState.ResourcePathId
            && databaseState.ServerlessComputeResourcesMode != NKikimrSubDomains::EServerlessComputeResourcesModeExclusive)
        {
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
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There are no compute nodes", ETags::ComputeState);
        } else {
            Ydb::Monitoring::StatusFlag::Status systemStatus = FillSystemTablets(databaseState, {&context, "SYSTEM_TABLET"});
            if (systemStatus != Ydb::Monitoring::StatusFlag::GREEN && systemStatus != Ydb::Monitoring::StatusFlag::GREY) {
                context.ReportStatus(systemStatus, "Compute has issues with system tablets", ETags::ComputeState, {ETags::SystemTabletState});
            }
            for (TNodeId nodeId : *computeNodeIds) {
                auto& computeNode = *computeStatus.add_nodes();
                FillComputeNodeStatus(databaseState, nodeId, computeNode, {&context, "COMPUTE_NODE"});
            }
            context.ReportWithMaxChildStatus("Some nodes are restarting too often", ETags::ComputeState, {ETags::Uptime});
            context.ReportWithMaxChildStatus("Compute is overloaded", ETags::ComputeState, {ETags::OverloadState});
            Ydb::Monitoring::StatusFlag::Status tabletsStatus = Ydb::Monitoring::StatusFlag::GREEN;
            computeNodeIds->push_back(0); // for tablets without node
            for (TNodeId nodeId : *computeNodeIds) {
                tabletsStatus = MaxStatus(tabletsStatus, FillTablets(databaseState, nodeId, *computeStatus.mutable_tablets(), context));
            }
            if (tabletsStatus != Ydb::Monitoring::StatusFlag::GREEN) {
                context.ReportStatus(tabletsStatus, "Compute has issues with tablets", ETags::ComputeState, {ETags::TabletState});
            }
        }
        computeStatus.set_overall(context.GetOverallStatus());
    }

    static TString GetVSlotId(const NKikimrBlobStorage::TVSlotId& protoVSlotId) {
        return TStringBuilder()
                << protoVSlotId.nodeid() << '-'
                << protoVSlotId.pdiskid() << '-'
                << protoVSlotId.vslotid();
    }

    static TString GetVDiskId(const NKikimrBlobStorage::TVDiskID& protoVDiskId) {
        return TStringBuilder()
                << protoVDiskId.groupid() << '-'
                << protoVDiskId.groupgeneration() << '-'
                << protoVDiskId.ring() << '-'
                << protoVDiskId.domain() << '-'
                << protoVDiskId.vdisk();
    }

    static TString GetVDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& protoVSlot) {
        return TStringBuilder()
                << protoVSlot.groupid() << '-'
                << protoVSlot.groupgeneration() << '-'
                << protoVSlot.failrealmidx() << '-'
                << protoVSlot.faildomainidx() << '-'
                << protoVSlot.vdiskidx();
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

    static TString GetPDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& vSlot) {
        return TStringBuilder() << vSlot.vslotid().nodeid() << "-" << vSlot.vslotid().pdiskid();
    }

    static TString GetPDiskId(const NKikimrBlobStorage::TNodeWardenServiceSet_TPDisk& pDisk) {
        return TStringBuilder() << pDisk.nodeid() << "-" << pDisk.pdiskid();
    }

    void FillPDiskStatus(const TString& pDiskId, Ydb::Monitoring::StoragePDiskStatus& storagePDiskStatus, TSelfCheckContext context) {
        context.Location.clear_database(); // PDisks are shared between databases
        context.Location.mutable_storage()->mutable_pool()->clear_name(); // PDisks are shared between pools
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // PDisks are shared between groups
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->clear_id(); // PDisks are shared between vdisks
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->Clear();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_pdisk();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_id(pDiskId);
        storagePDiskStatus.set_id(pDiskId);

        auto itPDisk = BSConfigPDisks.find(pDiskId);
        if (itPDisk == BSConfigPDisks.end()) { // this report, in theory, can't happen because there was pdisk mention in bsc vslot info. this pdisk info have to exists in bsc too
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide expected pdisk information", ETags::PDiskState);
            storagePDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        ui32 nodeId = itPDisk->second->nodeid();
        auto itNode = BSConfigNodes.find(nodeId);
        if (itNode == BSConfigNodes.end()) { // this report, in theory, can't happen because there was node mention in bsc pdisk info. this node info have to exists in bsc too
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide expected node information", ETags::PDiskState);
            storagePDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_path(itPDisk->second->path());
        auto& pDiskMetrics = itPDisk->second->pdiskmetrics();
        auto& nodeInfo = itNode->second;

        if (nodeInfo->lastdisconnecttimestamp() > nodeInfo->lastconnecttimestamp()) {
            {
                TSelfCheckContext nodeContext(&context, "STORAGE_NODE");
                nodeContext.Location.mutable_storage()->clear_pool();
                nodeContext.Location.mutable_storage()->mutable_node()->set_id(nodeId);
                const NKikimrBlobStorage::TBaseConfig::TNode* nodeInfo = nullptr;
                auto itNodeInfo = BSConfigNodes.find(nodeId);
                if (itNodeInfo != BSConfigNodes.end()) {
                    nodeInfo = itNodeInfo->second;
                }
                if (nodeInfo) {
                    nodeContext.Location.mutable_storage()->mutable_node()->set_host(nodeInfo->hostkey().fqdn());
                    nodeContext.Location.mutable_storage()->mutable_node()->set_port(nodeInfo->hostkey().icport());
                }
                nodeContext.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                            TStringBuilder() << "Storage node is not available",
                                            ETags::NodeState);
            }
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                 TStringBuilder() << "PDisk is not available",
                                 ETags::PDiskState,
                                 {ETags::NodeState});
        } else if (pDiskMetrics.HasState()) {
            switch (pDiskMetrics.GetState()) {
                case NKikimrBlobStorage::TPDiskState::Normal:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    break;
                case NKikimrBlobStorage::TPDiskState::Initial:
                case NKikimrBlobStorage::TPDiskState::InitialFormatRead:
                case NKikimrBlobStorage::TPDiskState::InitialSysLogRead:
                case NKikimrBlobStorage::TPDiskState::InitialCommonLogRead:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW,
                                         TStringBuilder() << "PDisk state is " << NKikimrBlobStorage::TPDiskState::E_Name(pDiskMetrics.GetState()),
                                         ETags::PDiskState);
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
                                         TStringBuilder() << "PDisk state is " << NKikimrBlobStorage::TPDiskState::E_Name(pDiskMetrics.GetState()),
                                         ETags::PDiskState);
                    break;
                case NKikimrBlobStorage::TPDiskState::Reserved14:
                case NKikimrBlobStorage::TPDiskState::Reserved15:
                case NKikimrBlobStorage::TPDiskState::Reserved16:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Unknown PDisk state", ETags::PDiskState);
                    break;
            }

            //if (pDiskInfo.HasAvailableSize() && pDiskInfo.GetTotalSize() != 0) {
            if (pDiskMetrics.GetAvailableSize() != 0 && pDiskMetrics.GetTotalSize() != 0) { // hotfix until KIKIMR-12659
                double avail = (double)pDiskMetrics.GetAvailableSize() / pDiskMetrics.GetTotalSize();
                if (avail < 0.06) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Available size is less than 6%", ETags::PDiskSpace);
                } else if (avail < 0.09) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Available size is less than 9%", ETags::PDiskSpace);
                } else if (avail < 0.12) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Available size is less than 12%", ETags::PDiskSpace);
                }
            }
        } else {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                 TStringBuilder() << "PDisk is not available",
                                 ETags::PDiskState,
                                 {ETags::NodeState});
        }

        storagePDiskStatus.set_overall(context.GetOverallStatus());
    }

    static Ydb::Monitoring::StatusFlag::Status GetFlagFromBSPDiskSpaceColor(NKikimrBlobStorage::TPDiskSpaceColor::E flag) {
        switch (flag) {
            case NKikimrBlobStorage::TPDiskSpaceColor::GREEN:
            case NKikimrBlobStorage::TPDiskSpaceColor::CYAN:
                return Ydb::Monitoring::StatusFlag::GREEN;
            case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW:
            case NKikimrBlobStorage::TPDiskSpaceColor::YELLOW:
                return Ydb::Monitoring::StatusFlag::YELLOW;
            case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE:
            case NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE:
            case NKikimrBlobStorage::TPDiskSpaceColor::ORANGE:
                return Ydb::Monitoring::StatusFlag::ORANGE;
            case NKikimrBlobStorage::TPDiskSpaceColor::RED:
                return Ydb::Monitoring::StatusFlag::RED;
            default:
                return Ydb::Monitoring::StatusFlag::UNSPECIFIED;
        }
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

    void FillVDiskStatus(const TString& vSlotId, Ydb::Monitoring::StorageVDiskStatus& storageVDiskStatus, TSelfCheckContext context) {
        auto itVSlot = BSConfigVSlots.find(vSlotId);
        const TEvInterconnect::TNodeInfo* nodeInfo = nullptr;

        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id()->Clear();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // you can see VDisks Group Id in vSlotId field
        if (itVSlot != BSConfigVSlots.end()) {
            TNodeId nodeId = itVSlot->second->vslotid().nodeid();
            auto itNodeInfo = MergedNodeInfo.find(nodeId);
            if (itNodeInfo != MergedNodeInfo.end()) {
                nodeInfo = itNodeInfo->second;
            }
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_id(GetVDiskId(*itVSlot->second));
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

        storageVDiskStatus.set_id(vSlotId);

        if (itVSlot == BSConfigVSlots.end()) { // this report, in theory, can't happen because there was slot mention in bsc group info. this slot info have to exists in bsc too
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide information", ETags::VDiskState);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        auto& vslot = *itVSlot->second;

        const auto *descriptor = NKikimrBlobStorage::EVDiskStatus_descriptor();
        auto status = descriptor->FindValueByName(vslot.status());
        if (!status) { // this case is not expected because becouse bsc assignes status according EVDiskStatus enum
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide known status", ETags::VDiskState);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        if (itVSlot->second->vslotid().has_pdiskid()) {
            TString pDiskId = GetPDiskId(*itVSlot->second);
            FillPDiskStatus(pDiskId, *storageVDiskStatus.mutable_pdisk(), {&context, "PDISK"});
        }

        switch (status->number()) {
            case NKikimrBlobStorage::ERROR:  { // the disk is not operational at all
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "VDisk is not available", ETags::VDiskState,{ETags::PDiskState});
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
            }
            case NKikimrBlobStorage::INIT_PENDING: { // initialization in process
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, TStringBuilder() << "VDisk is being initialized", ETags::VDiskState);
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
            }
            case NKikimrBlobStorage::REPLICATING: { // the disk accepts queries, but not all the data was replicated
                context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, TStringBuilder() << "Replication in progress", ETags::VDiskState);
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
            }
            case NKikimrBlobStorage::READY: { // the disk is fully operational and does not affect group fault tolerance
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        }

        switch (vslot.vdiskmetrics().spacecolor()) {
            case NKikimrBlobStorage::TPDiskSpaceColor::GREEN:
            case NKikimrBlobStorage::TPDiskSpaceColor::CYAN:
                if (context.IssueRecords.size() == 0) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                } else {
                    context.ReportStatus(context.IssueRecords.begin()->IssueLog.status(),
                                        TStringBuilder() << "VDisk have space issue",
                                        ETags::VDiskState,
                                        {ETags::PDiskSpace});
                }
                break;
            default:
                context.ReportStatus(GetFlagFromBSPDiskSpaceColor(vslot.vdiskmetrics().spacecolor()),
                                    TStringBuilder() << "DiskSpace is " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(vslot.vdiskmetrics().spacecolor()),
                                    ETags::VDiskSpace,
                                    {ETags::PDiskSpace});
                break;
        }

        storageVDiskStatus.set_overall(context.GetOverallStatus());
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
                StoragePoolStateByName[storagePoolName].Groups.emplace(state.groupid());
                StoragePoolStateByName[storagePoolName].Name = storagePoolName;
            }
            RequestDone("TEvBSGroupStateResponse");
        }
    }

    void FillPDiskStatusWithWhiteboard(const TString& pDiskId, const NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo, Ydb::Monitoring::StoragePDiskStatus& storagePDiskStatus, TSelfCheckContext context) {
        context.Location.clear_database(); // PDisks are shared between databases
        if (context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->empty()) {
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_pdisk();
        }
        context.Location.mutable_storage()->mutable_pool()->clear_name(); // PDisks are shared between pools
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // PDisks are shared between groups
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->clear_id(); // PDisks are shared between vdisks
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_id(pDiskId);
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_path(pDiskInfo.path());
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
                                         ETags::PDiskState);
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
                                         ETags::PDiskState);
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
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Available size is less than 6%", ETags::PDiskSpace);
                } else if (avail < 0.09) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Available size is less than 9%", ETags::PDiskSpace);
                } else if (avail < 0.12) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Available size is less than 12%", ETags::PDiskSpace);
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
                                         ETags::NodeState);
            }
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                 TStringBuilder() << "PDisk is not available",
                                 ETags::PDiskState,
                                 {ETags::NodeState});
        }

        storagePDiskStatus.set_overall(context.GetOverallStatus());
    }

    void FillVDiskStatusWithWhiteboard(const TString& vDiskId, const NKikimrWhiteboard::TVDiskStateInfo& vDiskInfo, Ydb::Monitoring::StorageVDiskStatus& storageVDiskStatus, TSelfCheckContext context) {
        if (context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id()->empty()) {
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_id();
        }
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->set_id(0, vDiskId);
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // you can see VDisks Group Id in vDiskId field
        storageVDiskStatus.set_id(vDiskId);
        TString pDiskId = GetPDiskId(vDiskInfo);
        auto itPDisk = MergedPDiskState.find(pDiskId);
        if (itPDisk != MergedPDiskState.end()) {
            FillPDiskStatusWithWhiteboard(pDiskId, *itPDisk->second, *storageVDiskStatus.mutable_pdisk(), {&context, "PDISK"});
        }

        if (!vDiskInfo.HasVDiskState()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                 TStringBuilder() << "VDisk is not available",
                                 ETags::VDiskState,
                                 {ETags::PDiskState});
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        switch (vDiskInfo.GetVDiskState()) {
            case NKikimrWhiteboard::EVDiskState::OK:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
            case NKikimrWhiteboard::EVDiskState::Initial:
            case NKikimrWhiteboard::EVDiskState::SyncGuidRecovery:
                context.IssueRecords.clear();
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW,
                                     TStringBuilder() << "VDisk state is " << NKikimrWhiteboard::EVDiskState_Name(vDiskInfo.GetVDiskState()),
                                     ETags::VDiskState);
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
            case NKikimrWhiteboard::EVDiskState::LocalRecoveryError:
            case NKikimrWhiteboard::EVDiskState::SyncGuidRecoveryError:
            case NKikimrWhiteboard::EVDiskState::PDiskError:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                     TStringBuilder() << "VDisk state is " << NKikimrWhiteboard::EVDiskState_Name(vDiskInfo.GetVDiskState()),
                                     ETags::VDiskState,
                                     {ETags::PDiskState});
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
        }

        if (!vDiskInfo.GetReplicated()) {
            context.IssueRecords.clear();
            context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Replication in progress", ETags::VDiskState);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        if (vDiskInfo.HasDiskSpace()) {
            switch(vDiskInfo.GetDiskSpace()) {
                case NKikimrWhiteboard::EFlag::Green:
                    if (context.IssueRecords.size() == 0) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    } else {
                        context.ReportStatus(context.IssueRecords.begin()->IssueLog.status(),
                                            TStringBuilder() << "VDisk is degraded",
                                            ETags::VDiskState,
                                            {ETags::PDiskSpace});
                    }
                    break;
                case NKikimrWhiteboard::EFlag::Red:
                    context.ReportStatus(GetFlagFromWhiteboardFlag(vDiskInfo.GetDiskSpace()),
                                         TStringBuilder() << "DiskSpace is " << NKikimrWhiteboard::EFlag_Name(vDiskInfo.GetDiskSpace()),
                                         ETags::VDiskState,
                                         {ETags::PDiskSpace});
                    break;
                default:
                    context.ReportStatus(GetFlagFromWhiteboardFlag(vDiskInfo.GetDiskSpace()),
                                         TStringBuilder() << "DiskSpace is " << NKikimrWhiteboard::EFlag_Name(vDiskInfo.GetDiskSpace()),
                                         ETags::VDiskSpace,
                                         {ETags::PDiskSpace});
                    break;
            }
        }

        storageVDiskStatus.set_overall(context.GetOverallStatus());
    }

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

    void FillGroupStatusWithWhiteboard(TGroupId groupId, const NKikimrWhiteboard::TBSGroupStateInfo& groupInfo, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckContext context) {
        if (context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_id()->empty()) {
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->add_id();
        }
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->set_id(0, ToString(groupId));
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
            FillVDiskStatusWithWhiteboard(vDiskId, itVDisk != MergedVDiskState.end() ? *itVDisk->second : NKikimrWhiteboard::TVDiskStateInfo(), vDiskStatus, {&context, "VDISK"});
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
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", ETags::GroupState, {ETags::VDiskState});
            } else if (disksColors[Ydb::Monitoring::StatusFlag::YELLOW] > 0) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
            }
        } else if (groupInfo.erasurespecies() == BLOCK_4_2) {
            if (failedDisks > 2) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", ETags::GroupState, {ETags::VDiskState});
            } else if (failedDisks > 1) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", ETags::GroupState, {ETags::VDiskState});
            } else if (failedDisks > 0) {
                if (disksColors[Ydb::Monitoring::StatusFlag::BLUE] == failedDisks) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                }
            } else if (disksColors[Ydb::Monitoring::StatusFlag::YELLOW] > 0) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
            }
        } else if (groupInfo.erasurespecies() == MIRROR_3_DC) {
            if (failedRealms.size() > 2 || (failedRealms.size() == 2 && failedRealms[0].second > 1 && failedRealms[1].second > 1)) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", ETags::GroupState, {ETags::VDiskState});
            } else if (failedRealms.size() == 2) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", ETags::GroupState, {ETags::VDiskState});
            } else if (failedDisks > 0) {
                if (disksColors[Ydb::Monitoring::StatusFlag::BLUE] == failedDisks) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                }
            } else if (disksColors[Ydb::Monitoring::StatusFlag::YELLOW] > 0) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
            }
        }

        storageGroupStatus.set_overall(context.GetOverallStatus());
    }

    static const inline TString NONE = "none";
    static const inline TString BLOCK_4_2 = "block-4-2";
    static const inline TString MIRROR_3_DC = "mirror-3-dc";
    static const int MERGING_IGNORE_SIZE = 4;

    struct TMergeIssuesContext {
        std::unordered_map<ETags, TList<TSelfCheckContext::TIssueRecord>> recordsMap;
        std::unordered_set<TString> removeIssuesIds;

        TMergeIssuesContext(TList<TSelfCheckContext::TIssueRecord>& records) {
            for (auto it = records.begin(); it != records.end(); ) {
                auto move = it++;
                recordsMap[move->Tag].splice(recordsMap[move->Tag].end(), records, move);
            }
        }

        void RemoveUnlinkIssues(TList<TSelfCheckContext::TIssueRecord>& records) {
            bool isRemovingIssuesIteration = true;
            while (isRemovingIssuesIteration) {
                isRemovingIssuesIteration = false;

                std::unordered_set<TString> necessaryIssuesIds;
                for (auto it = records.begin(); it != records.end(); it++) {
                    auto reasons = it->IssueLog.reason();
                    for (auto reasonIt = reasons.begin(); reasonIt != reasons.end(); reasonIt++) {
                        necessaryIssuesIds.insert(*reasonIt);
                    }
                }

                for (auto it = records.begin(); it != records.end(); ) {
                    if (!necessaryIssuesIds.contains(it->IssueLog.id()) && removeIssuesIds.contains(it->IssueLog.id())) {
                        auto reasons = it->IssueLog.reason();
                        for (auto reasonIt = reasons.begin(); reasonIt != reasons.end(); reasonIt++) {
                            removeIssuesIds.insert(*reasonIt);
                        }
                        isRemovingIssuesIteration = true;
                        it = records.erase(it);
                    } else {
                        it++;
                    }
                }
            }

            {
                std::unordered_set<TString> issueIds;
                for (auto it = records.begin(); it != records.end(); it++) {
                    issueIds.insert(it->IssueLog.id());
                }

                for (auto it = records.begin(); it != records.end(); it++) {
                    auto reasons = it->IssueLog.mutable_reason();
                    for (auto reasonIt = reasons->begin(); reasonIt != reasons->end(); ) {
                        if (!issueIds.contains(*reasonIt)) {
                            reasonIt = reasons->erase(reasonIt);
                        } else {
                            reasonIt++;
                        }
                    }
                }
            }
        }

        void RenameMergingIssues(TList<TSelfCheckContext::TIssueRecord>& records) {
            for (auto it = records.begin(); it != records.end(); it++) {
                if (it->IssueLog.count() > 0) {
                    TString message = it->IssueLog.message();
                    switch (it->Tag) {
                        case ETags::GroupState: {
                            message = std::regex_replace(message.c_str(), std::regex("^Group has "), "Groups have ");
                            message = std::regex_replace(message.c_str(), std::regex("^Group is "), "Groups are ");
                            message = std::regex_replace(message.c_str(), std::regex("^Group "), "Groups ");
                            break;
                        }
                        case ETags::VDiskState: {
                            message = std::regex_replace(message.c_str(), std::regex("^VDisk has "), "VDisk have ");
                            message = std::regex_replace(message.c_str(), std::regex("^VDisk is "), "VDisks are ");
                            message = std::regex_replace(message.c_str(), std::regex("^VDisk "), "VDisk ");
                            break;
                        }
                        case ETags::PDiskState: {
                            message = std::regex_replace(message.c_str(), std::regex("^PDisk has "), "PDisk have ");
                            message = std::regex_replace(message.c_str(), std::regex("^PDisk is "), "PDisks are ");
                            message = std::regex_replace(message.c_str(), std::regex("^PDisk "), "PDisk ");
                            break;
                        }
                        default:
                            break;
                    }
                    it->IssueLog.set_message(message);
                }
            }
        }

        void FillRecords(TList<TSelfCheckContext::TIssueRecord>& records) {
            for(auto it = recordsMap.begin(); it != recordsMap.end(); ++it) {
                records.splice(records.end(), it->second);
            }
            RemoveUnlinkIssues(records);
            RenameMergingIssues(records);
        }

        TList<TSelfCheckContext::TIssueRecord>& GetRecords(ETags tag) {
            return recordsMap[tag];
        }
    };

    bool FindRecordsForMerge(TList<TSelfCheckContext::TIssueRecord>& records, TList<TSelfCheckContext::TIssueRecord>& similar, TList<TSelfCheckContext::TIssueRecord>& merged) {
        while (!records.empty() && similar.empty()) {
            similar.splice(similar.end(), records, records.begin());
            for (auto it = records.begin(); it != records.end(); ) {
                bool isSimilar = it->IssueLog.status() == similar.begin()->IssueLog.status()
                    && it->IssueLog.message() == similar.begin()->IssueLog.message()
                    && it->IssueLog.level() == similar.begin()->IssueLog.level() ;
                if (isSimilar && similar.begin()->Tag == ETags::VDiskState) {
                    isSimilar = it->IssueLog.location().storage().node().id() == similar.begin()->IssueLog.location().storage().node().id();
                }
                if (isSimilar) {
                    auto move = it++;
                    similar.splice(similar.end(), records, move);
                } else {
                    ++it;
                }
            }

            if (similar.size() == 1) {
                merged.splice(merged.end(), similar);
            }
        }

        return !similar.empty();
    }

    std::shared_ptr<TList<TSelfCheckContext::TIssueRecord>> FindChildrenRecords(TList<TSelfCheckContext::TIssueRecord>& records, TSelfCheckContext::TIssueRecord& parent) {
        std::shared_ptr<TList<TSelfCheckContext::TIssueRecord>> children(new TList<TSelfCheckContext::TIssueRecord>);
        std::unordered_set<TString> childrenIds;
        for (auto reason: parent.IssueLog.reason()) {
            childrenIds.insert(reason);
        }

        for (auto it = records.begin(); it != records.end(); ) {
            if (childrenIds.contains(it->IssueLog.id())) {
                auto move = it++;
                children->splice(children->end(), records, move);
            } else {
                it++;
            }
        }

        return children;
    }

    void MoveDataInFirstRecord(TMergeIssuesContext& context, TList<TSelfCheckContext::TIssueRecord>& similar) {
        auto mainReasons = similar.begin()->IssueLog.mutable_reason();
        std::unordered_set<TString> ids;
        ids.insert(similar.begin()->IssueLog.id());
        std::unordered_set<TString> mainReasonIds;
        for (auto it = mainReasons->begin(); it != mainReasons->end(); it++) {
            mainReasonIds.insert(*it);
        }

        for (auto it = std::next(similar.begin(), 1); it != similar.end(); ) {
            if (ids.contains(it->IssueLog.id())) {
                it++;
                continue;
            }
            ids.insert(it->IssueLog.id());

            switch (similar.begin()->Tag) {
                case ETags::GroupState: {
                    auto mainGroupIds = similar.begin()->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_id();
                    auto donorGroupIds = it->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_id();
                    mainGroupIds->Add(donorGroupIds->begin(), donorGroupIds->end());
                    break;
                }
                case ETags::VDiskState: {
                    auto mainVdiskIds = similar.begin()->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id();
                    auto donorVdiskIds = it->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id();
                    mainVdiskIds->Add(donorVdiskIds->begin(), donorVdiskIds->end());
                    break;
                }
                case ETags::PDiskState: {
                    auto mainPdisk = similar.begin()->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk();
                    auto donorPdisk = it->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk();
                    mainPdisk->Add(donorPdisk->begin(), donorPdisk->end());
                    break;
                }
                default:
                    break;
            }

            auto donorReasons = it->IssueLog.mutable_reason();
            for (auto donorReasonIt = donorReasons->begin(); donorReasonIt != donorReasons->end(); donorReasonIt++) {
                if (!mainReasonIds.contains(*donorReasonIt)) {
                    mainReasons->Add(donorReasonIt->c_str());
                    mainReasonIds.insert(*donorReasonIt);
                }
            }

            context.removeIssuesIds.insert(it->IssueLog.id());
            it = similar.erase(it);
        }

        similar.begin()->IssueLog.set_count(ids.size());
        similar.begin()->IssueLog.set_listed(ids.size());
    }

    void MergeLevelRecords(TMergeIssuesContext& context, TList<TSelfCheckContext::TIssueRecord>& records) {
        TList<TSelfCheckContext::TIssueRecord> handled;
        while (!records.empty()) {
            TList<TSelfCheckContext::TIssueRecord> similar;
            if (FindRecordsForMerge(records, similar, handled)) {
                MoveDataInFirstRecord(context, similar);
                handled.splice(handled.end(), similar, similar.begin());
            }
        }
        records.splice(records.end(), handled);
    }

    void MergeLevelRecords(TMergeIssuesContext& context, ETags levelTag) {
        auto& records = context.GetRecords(levelTag);
        MergeLevelRecords(context, records);
    }

    void MergeLevelRecords(TMergeIssuesContext& context, ETags levelTag, ETags upperTag) {
        auto& levelRecords = context.GetRecords(levelTag);
        auto& upperRecords = context.GetRecords(upperTag);

        for (auto it = upperRecords.begin(); it != upperRecords.end(); it++) {
            auto children = FindChildrenRecords(levelRecords, *it);
            if (children->size() > 1) {
                MergeLevelRecords(context, *children);
            }
            levelRecords.splice(levelRecords.end(), *children);
        }
    }

    int GetIssueCount(TSelfCheckContext::TIssueRecord& record) {
        return record.IssueLog.count() == 0 ? 1 : record.IssueLog.count();
    }

    void SetIssueCount(TSelfCheckContext::TIssueRecord& record, int value) {
        if (record.IssueLog.listed() == 0) {
            record.IssueLog.set_listed(1);
        }
        record.IssueLog.set_count(value);
    }

    int GetIssueListed(TSelfCheckContext::TIssueRecord& record) {
        return record.IssueLog.listed() == 0 ? 1 : record.IssueLog.listed();
    }

    void SetIssueListed(TSelfCheckContext::TIssueRecord& record, int value) {
        if (record.IssueLog.count() == 0) {
            record.IssueLog.set_count(1);
        }
        record.IssueLog.set_listed(value);
    }

    void FillGroupStatus(TGroupId groupId, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckContext context) {
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_id()->Clear();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->add_id(ToString(groupId));
        storageGroupStatus.set_id(ToString(groupId));

        auto itGroup = BSConfigGroups.find(groupId);
        if (itGroup == BSConfigGroups.end()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide information", ETags::GroupState);
            storageGroupStatus.set_overall(context.GetOverallStatus());
            return;
        }

        auto& groupInfo = *itGroup->second;
        bool onlyGoodDisks = true;
        for (const auto& vSlotIdProto : groupInfo.vslotid()) {
            TString vSlotId = GetVSlotId(vSlotIdProto);
            Ydb::Monitoring::StorageVDiskStatus& vDiskStatus = *storageGroupStatus.add_vdisks();
            FillVDiskStatus(vSlotId, vDiskStatus, {&context, "VDISK"});
            onlyGoodDisks &= vDiskStatus.overall() != Ydb::Monitoring::StatusFlag::RED
                && vDiskStatus.overall() != Ydb::Monitoring::StatusFlag::GREY;
        }

        context.Location.mutable_storage()->clear_node(); // group doesn't have node
        context.OverallStatus = MinStatus(context.OverallStatus, Ydb::Monitoring::StatusFlag::YELLOW);

        switch (groupInfo.operatingstatus()) {
            case NKikimrBlobStorage::TGroupStatus::FULL: { // all VDisks of the group are READY for specific period of time
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
            }
            case NKikimrBlobStorage::TGroupStatus::PARTIAL: { // some of VDisks are operational, but group is not yet DEGRADED
                if ((groupInfo.erasurespecies() == BLOCK_4_2 || groupInfo.erasurespecies() == MIRROR_3_DC) && onlyGoodDisks) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                }
                break;
            }
            case NKikimrBlobStorage::TGroupStatus::DEGRADED: { // group is DEGRADED -- one random failure may lead to group loss (but may not lead too)
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", ETags::GroupState, {ETags::VDiskState});
                break;
            }
            case NKikimrBlobStorage::TGroupStatus::DISINTEGRATED: { // group is not available for operation
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", ETags::GroupState, {ETags::VDiskState});
                break;
            }
            case NKikimrBlobStorage::TGroupStatus::UNKNOWN: { // default value, can't happen
            default:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "System tablet BSC provided unexpected group status", ETags::GroupState, {ETags::VDiskState});
                break;
            }
        }

        storageGroupStatus.set_overall(context.GetOverallStatus());
    }

    void MergeRecords(TList<TSelfCheckContext::TIssueRecord>& records) {
        TMergeIssuesContext mergeContext(records);
        if (Request->Request.merge_records()) {
            MergeLevelRecords(mergeContext, ETags::GroupState);
            MergeLevelRecords(mergeContext, ETags::VDiskState, ETags::GroupState);
            MergeLevelRecords(mergeContext, ETags::PDiskState, ETags::VDiskState);
        }
        mergeContext.FillRecords(records);
    }

    void FillPoolStatus(const TStoragePoolState& pool, Ydb::Monitoring::StoragePoolStatus& storagePoolStatus, TSelfCheckContext context) {
        context.Location.mutable_storage()->mutable_pool()->set_name(pool.Name);
        storagePoolStatus.set_id(pool.Name);
        for (auto groupId : pool.Groups) {
            if (ReadyBscInfo(groupId)) {
                FillGroupStatus(groupId, *storagePoolStatus.add_groups(), {&context, "STORAGE_GROUP"});
            } else if (IsStaticGroup(groupId)) {
                auto itGroup = MergedBSGroupState.find(groupId);
                if (itGroup != MergedBSGroupState.end()) {
                    FillGroupStatusWithWhiteboard(groupId, *itGroup->second, *storagePoolStatus.add_groups(), {&context, "STORAGE_GROUP"});
                }
            }
        }

        MergeRecords(context.IssueRecords);

        switch (context.GetOverallStatus()) {
            case Ydb::Monitoring::StatusFlag::BLUE:
            case Ydb::Monitoring::StatusFlag::YELLOW:
                context.ReportStatus(context.GetOverallStatus(), "Pool degraded", ETags::PoolState, {ETags::GroupState});
                break;
            case Ydb::Monitoring::StatusFlag::ORANGE:
                context.ReportStatus(context.GetOverallStatus(), "Pool has no redundancy", ETags::PoolState, {ETags::GroupState});
                break;
            case Ydb::Monitoring::StatusFlag::RED:
                context.ReportStatus(context.GetOverallStatus(), "Pool failed", ETags::PoolState, {ETags::GroupState});
                break;
            default:
                break;
        }
        storagePoolStatus.set_overall(context.GetOverallStatus());
    }

    void FillStorage(TDatabaseState& databaseState, Ydb::Monitoring::StorageStatus& storageStatus, TSelfCheckContext context) {
        if (BaseConfig && databaseState.StoragePools.empty()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There are no storage pools", ETags::StorageState);
        } else {
            if (BaseConfig) {
                for (const ui64 poolId : databaseState.StoragePools) {
                    auto itStoragePoolState = StoragePoolState.find(poolId);
                    if (itStoragePoolState != StoragePoolState.end() && itStoragePoolState->second.Groups) {
                        FillPoolStatus(itStoragePoolState->second, *storageStatus.add_pools(), {&context, "STORAGE_POOL"});
                        StoragePoolSeen.emplace(poolId);
                    }
                }
            } else {
                for (const TString& poolName : databaseState.StoragePoolNames) {
                    auto itStoragePoolState = StoragePoolStateByName.find(poolName);
                    if (itStoragePoolState != StoragePoolStateByName.end()) {
                        FillPoolStatus(itStoragePoolState->second, *storageStatus.add_pools(), {&context, "STORAGE_POOL"});
                        StoragePoolSeenByName.emplace(poolName);
                    }
                }
            }
            switch (context.GetOverallStatus()) {
                case Ydb::Monitoring::StatusFlag::BLUE:
                case Ydb::Monitoring::StatusFlag::YELLOW:
                    context.ReportStatus(context.GetOverallStatus(), "Storage degraded", ETags::StorageState, {ETags::PoolState});
                    break;
                case Ydb::Monitoring::StatusFlag::ORANGE:
                    context.ReportStatus(context.GetOverallStatus(), "Storage has no redundancy", ETags::StorageState, {ETags::PoolState});
                    break;
                case Ydb::Monitoring::StatusFlag::RED:
                    context.ReportStatus(context.GetOverallStatus(), "Storage failed", ETags::StorageState, {ETags::PoolState});
                    break;
                default:
                    break;
            }
            if (!BaseConfig) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide information", ETags::StorageState);
            }
        }
        if (databaseState.StorageQuota > 0) {
            auto usage = (float)databaseState.StorageUsage / databaseState.StorageQuota;
            if (usage > 0.9) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Storage usage over 90%", ETags::StorageState);
            } else if (usage > 0.85) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Storage usage over 85%", ETags::StorageState);
            } else if (usage > 0.75) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Storage usage over 75%", ETags::StorageState);
            }
        }
        storageStatus.set_overall(context.GetOverallStatus());
    }

    struct TOverallStateContext {
        Ydb::Monitoring::SelfCheckResult* Result;
        Ydb::Monitoring::StatusFlag::Status Status = Ydb::Monitoring::StatusFlag::GREY;
        bool HasDegraded = false;
        std::unordered_set<std::pair<TString, TString>> IssueIds;

        TOverallStateContext(Ydb::Monitoring::SelfCheckResult* result) {
            Result = result;
        }

        void FillSelfCheckResult() {
            switch (Status) {
            case Ydb::Monitoring::StatusFlag::GREEN:
                Result->set_self_check_result(Ydb::Monitoring::SelfCheck::GOOD);
                break;
            case Ydb::Monitoring::StatusFlag::YELLOW:
                if (HasDegraded) {
                    Result->set_self_check_result(Ydb::Monitoring::SelfCheck::DEGRADED);
                } else {
                    Result->set_self_check_result(Ydb::Monitoring::SelfCheck::GOOD);
                }
                break;
            case Ydb::Monitoring::StatusFlag::BLUE:
                Result->set_self_check_result(Ydb::Monitoring::SelfCheck::DEGRADED);
                break;
            case Ydb::Monitoring::StatusFlag::ORANGE:
                Result->set_self_check_result(Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED);
                break;
            case Ydb::Monitoring::StatusFlag::RED:
                Result->set_self_check_result(Ydb::Monitoring::SelfCheck::EMERGENCY);
                break;
            default:
                break;
            }
        }

        void UpdateMaxStatus(Ydb::Monitoring::StatusFlag::Status status) {
            Status = MaxStatus(Status, status);
        }

        void AddIssues(TList<TSelfCheckResult::TIssueRecord>& issueRecords) {
            for (auto& issueRecord : issueRecords) {
                std::pair<TString, TString> key{issueRecord.IssueLog.location().database().name(), issueRecord.IssueLog.id()};
                if (IssueIds.emplace(key).second) {
                    Result->mutable_issue_log()->Add()->CopyFrom(issueRecord.IssueLog);
                }
            }
        }
    };

    void FillDatabaseResult(TOverallStateContext& context, const TString& path, TDatabaseState& state) {
        Ydb::Monitoring::DatabaseStatus& databaseStatus(*context.Result->add_database_status());
        TSelfCheckResult dbContext;
        dbContext.Type = "DATABASE";
        dbContext.Location.mutable_database()->set_name(path);
        databaseStatus.set_name(path);
        FillCompute(state, *databaseStatus.mutable_compute(), {&dbContext, "COMPUTE"});
        FillStorage(state, *databaseStatus.mutable_storage(), {&dbContext, "STORAGE"});
        if (databaseStatus.compute().overall() != Ydb::Monitoring::StatusFlag::GREEN
                && databaseStatus.storage().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
            dbContext.ReportStatus(MaxStatus(databaseStatus.compute().overall(), databaseStatus.storage().overall()),
                "Database has multiple issues", ETags::DBState, { ETags::ComputeState, ETags::StorageState});
        } else if (databaseStatus.compute().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
            dbContext.ReportStatus(databaseStatus.compute().overall(), "Database has compute issues", ETags::DBState, {ETags::ComputeState});
        } else if (databaseStatus.storage().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
            dbContext.ReportStatus(databaseStatus.storage().overall(), "Database has storage issues", ETags::DBState, {ETags::StorageState});
        }
        databaseStatus.set_overall(dbContext.GetOverallStatus());
        context.UpdateMaxStatus(dbContext.GetOverallStatus());
        context.AddIssues(dbContext.IssueRecords);
        if (!context.HasDegraded && context.Status != Ydb::Monitoring::StatusFlag::GREEN && dbContext.HasTags({ETags::StorageState})) {
            context.HasDegraded = true;
        }
    }

    const TDuration MAX_CLOCKSKEW_ORANGE_ISSUE_TIME = TDuration::MicroSeconds(25000);
    const TDuration MAX_CLOCKSKEW_YELLOW_ISSUE_TIME = TDuration::MicroSeconds(5000);

    void FillNodesSyncStatus(TOverallStateContext& context) {
        long maxClockSkewUs = 0;
        TNodeId maxClockSkewPeerId = 0;
        TNodeId maxClockSkewNodeId = 0;
        for (auto& [nodeId, nodeSystemState] : MergedNodeSystemState) {
            if (IsTimeDifferenceCheckNode(nodeId) && IsTimeDifferenceCheckNode(nodeSystemState->GetMaxClockSkewPeerId())
                    && abs(nodeSystemState->GetMaxClockSkewWithPeerUs()) > maxClockSkewUs) {
                maxClockSkewUs = abs(nodeSystemState->GetMaxClockSkewWithPeerUs());
                maxClockSkewPeerId = nodeSystemState->GetMaxClockSkewPeerId();
                maxClockSkewNodeId = nodeId;
            }
        }
        if (!maxClockSkewNodeId) {
            return;
        }

        TSelfCheckResult syncContext;
        syncContext.Type = "NODES_TIME_DIFFERENCE";
        FillNodeInfo(maxClockSkewNodeId, syncContext.Location.mutable_node());
        FillNodeInfo(maxClockSkewPeerId, syncContext.Location.mutable_peer());

        TDuration maxClockSkewTime = TDuration::MicroSeconds(maxClockSkewUs);
        if (maxClockSkewTime > MAX_CLOCKSKEW_ORANGE_ISSUE_TIME) {
            syncContext.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, TStringBuilder() << "The nodes have a time difference of " << maxClockSkewTime.MilliSeconds() << " ms", ETags::SyncState);
        } else if (maxClockSkewTime > MAX_CLOCKSKEW_YELLOW_ISSUE_TIME) {
            syncContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, TStringBuilder() << "The nodes have a time difference of " << maxClockSkewTime.MilliSeconds() << " ms", ETags::SyncState);
        } else {
            syncContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
        }

        context.UpdateMaxStatus(syncContext.GetOverallStatus());
        context.AddIssues(syncContext.IssueRecords);
    }

    void FillResult(TOverallStateContext context) {
        if (IsSpecificDatabaseFilter()) {
            FillDatabaseResult(context, FilterDatabase, DatabaseState[FilterDatabase]);
        } else {
            for (auto& [path, state] : DatabaseState) {
                FillDatabaseResult(context, path, state);
            }
        }
        FillNodesSyncStatus(context);
        if (DatabaseState.empty()) {
            Ydb::Monitoring::DatabaseStatus& databaseStatus(*context.Result->add_database_status());
            TSelfCheckResult tabletContext;
            tabletContext.Location.mutable_database()->set_name(DomainPath);
            databaseStatus.set_name(DomainPath);
            {
                TDatabaseState databaseState;
                FillSystemTablets(databaseState, {&tabletContext, "SYSTEM_TABLET"});
                context.UpdateMaxStatus(tabletContext.GetOverallStatus());
            }
        }
        if (!FilterDatabase) {
            TDatabaseState unknownDatabase;
            bool fillUnknownDatabase = false;
            if (BaseConfig) {
                for (auto& [id, pool] : StoragePoolState) {
                    if (StoragePoolSeen.count(id) == 0) {
                        unknownDatabase.StoragePools.insert(id);
                    }
                }
                fillUnknownDatabase = !unknownDatabase.StoragePools.empty();
            } else {
                for (auto& [name, pool] : StoragePoolStateByName) {
                    if (StoragePoolSeenByName.count(name) == 0) {
                        unknownDatabase.StoragePoolNames.insert(name);
                    }
                }
                fillUnknownDatabase = !unknownDatabase.StoragePoolNames.empty();
            }
            if (fillUnknownDatabase) {
                Ydb::Monitoring::DatabaseStatus& databaseStatus(*context.Result->add_database_status());
                TSelfCheckResult storageContext;
                FillStorage(unknownDatabase, *databaseStatus.mutable_storage(), {&storageContext, "STORAGE"});
                databaseStatus.set_overall(storageContext.GetOverallStatus());
                context.UpdateMaxStatus(storageContext.GetOverallStatus());
                context.AddIssues(storageContext.IssueRecords);
            }
        }
        context.FillSelfCheckResult();
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

        FillResult({&result});
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
            NYdbGrpc::TGrpcStatus Status;

            TEvError(NYdbGrpc::TGrpcStatus&& status)
                : Status(std::move(status))
            {}
        };
    };

    TDuration Timeout = TDuration::MilliSeconds(10000);
    std::shared_ptr<NYdbGrpc::TGRpcClientLow> GRpcClientLow;
    TActorId Sender;
    THolder<RequestType> Request;
    ui64 Cookie;
    Ydb::Monitoring::SelfCheckResult Result;

    TNodeCheckRequest(std::shared_ptr<NYdbGrpc::TGRpcClientLow> grpcClient, const TActorId& sender, THolder<RequestType> request, ui64 cookie)
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
        NYdbGrpc::TGRpcClientConfig config;
        for (const auto& systemStateInfo : ev->Get()->Record.GetSystemStateInfo()) {
            for (const auto& endpoint : systemStateInfo.GetEndpoints()) {
                if (endpoint.GetName() == "grpc") {
                    config.Locator = "localhost" + endpoint.GetAddress();
                    break;
                } else if (endpoint.GetName() == "grpcs") {
                    config.Locator = "localhost" + endpoint.GetAddress();
                    config.EnableSsl = true;
                    config.SslTargetNameOverride = systemStateInfo.GetHost();
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
        NYdbGrpc::TResponseCallback<Ydb::Monitoring::NodeCheckResponse> responseCb =
            [actorId, actorSystem, context = GRpcClientLow->CreateContext()](NYdbGrpc::TGrpcStatus&& status, Ydb::Monitoring::NodeCheckResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new typename TEvPrivate::TEvResult(std::move(response)));
            } else {
                actorSystem->Send(actorId, new typename TEvPrivate::TEvError(std::move(status)));
            }
        };
        NYdbGrpc::TCallMeta meta;
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

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
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
                .UseAuth = false,
            });
        }
        Become(&THealthCheckService::StateWork);
    }

    void Handle(TEvSelfCheckRequest::TPtr& ev) {
        Register(new TSelfCheckRequest(ev->Sender, ev.Get()->Release(), ev->Cookie));
    }

    std::shared_ptr<NYdbGrpc::TGRpcClientLow> GRpcClientLow;

    void Handle(TEvNodeCheckRequest::TPtr& ev) {
        if (!GRpcClientLow) {
            GRpcClientLow = std::make_shared<NYdbGrpc::TGRpcClientLow>();
        }
        Register(new TNodeCheckRequest<TEvNodeCheckRequest>(GRpcClientLow, ev->Sender, ev.Get()->Release(), ev->Cookie));
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        if (ev->Get()->Request.GetPath() == "/status") {
            if (!GRpcClientLow) {
                GRpcClientLow = std::make_shared<NYdbGrpc::TGRpcClientLow>();
            }
            Register(new TNodeCheckRequest<NMon::TEvHttpInfo>(GRpcClientLow, ev->Sender, ev.Get()->Release(), ev->Cookie));
        } else {
            Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND, 0, NMon::IEvHttpInfoRes::EContentType::Custom), 0, ev->Cookie);
        }
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
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
