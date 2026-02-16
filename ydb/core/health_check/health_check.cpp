#include "health_check.h"

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/digest/old_crc/crc.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <util/random/shuffle.h>

#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/core/util/tuples.h>

#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/sys_view/common/events.h>

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <regex>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

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
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::HEALTH, stream)

namespace NKikimr::NHealthCheck {

using namespace NActors;
using namespace Ydb;
using namespace NSchemeCache;
using namespace NSchemeShard;
using namespace NSysView;
using namespace NConsole;
using namespace NNodeWhiteboard;
using NNodeWhiteboard::TTabletId;

const TString NONE = "none";
const TString BLOCK_4_2 = "block-4-2";
const TString MIRROR_3_DC = "mirror-3-dc";

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

    using TGroupId = ui32;

    enum class ETags {
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
        NodeClockSkew,
        DatabaseClockSkew,
        Uptime,
        QuotaUsage,
        BridgeGroupState,
        PileComputeState,
        StateStorage,
        StateStorageRing,
        StateStorageNode,
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
            ui32 MaxRestartsPerPeriod; // per hour
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
            bool Leader;
            int Count = 1;
            TStackVec<TString> Identifiers;

            static ETabletState GetState(const NKikimrHive::TTabletInfo& info, const TTabletStateSettings& settings) {
                if (info.volatilestate() == NKikimrHive::TABLET_VOLATILE_STATE_STOPPED) {
                    return ETabletState::Stopped;
                }
                ETabletState state = (info.restartsperperiod() >= settings.MaxRestartsPerPeriod) ? ETabletState::RestartsTooOften : ETabletState::Good;
                if (info.volatilestate() == NKikimrHive::TABLET_VOLATILE_STATE_RUNNING) {
                    return state;
                }
                if (info.tabletbootmode() != NKikimrHive::TABLET_BOOT_MODE_DEFAULT) {
                    return state;
                }
                if (info.has_lockedtoactor()) {
                    return state;
                }
                if (info.lastalivetimestamp() != 0 && TInstant::MilliSeconds(info.lastalivetimestamp()) < settings.AliveBarrier) {
                    // Tablet is not alive for a long time
                    // We should report it as dead unless it's just waiting to be created
                    if (info.generation() == 0 && info.volatilestate() == NKikimrHive::TABLET_VOLATILE_STATE_BOOTING && !info.inwaitqueue()) {
                        return state;
                    }
                    return ETabletState::Dead;
                }
                return state;

            }

            TNodeTabletStateCount(const NKikimrHive::TTabletInfo& info, const TTabletStateSettings& settings)
                : Type(info.tablettype())
                , State(GetState(info, settings))
                , Leader(info.followerid() == 0)
            {
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
        TTabletId SchemeShardId = {};
        TPathId ResourcePathId = {};
        TVector<TNodeId> ComputeNodeIds;
        THashSet<ui64> StoragePools;        // BSConfig case
        THashSet<TString> StoragePoolNames; // no BSConfig case
        THashMap<std::pair<TTabletId, TFollowerId>, const NKikimrHive::TTabletInfo*> MergedTabletState;
        THashMap<TNodeId, TNodeTabletState> MergedNodeTabletState;
        THashMap<TNodeId, ui32> NodeRestartsPerPeriod;
        ui64 StorageQuota = 0;
        ui64 StorageUsage = 0;
        TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;
        TString Path;
        THashMap<TString, TVector<TNodeId>> PileNodeIds;
        ui64 MaxClockSkewUs = 0; // maximum clock skew between database nodes
        TNodeId MaxClockSkewNodeId = 0; // node id reported by most of the other nodes
        i64 MaxClockSkewNodeAvgUs = 0; // average clock skew reported by most of the other nodes
    };

    struct TGroupState {
        TGroupId Id;
        TString ErasureSpecies;
        std::vector<const NKikimrSysView::TVSlotEntry*> VSlots;
        ui32 Generation;
        bool LayoutCorrect = true;
        std::vector<TGroupState*> BridgeGroups;
        TBridgePileId BridgePileId;
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

        static TString crc32(const TString& data) {
            return Sprintf("%08x", (ui32)::crc32(data.data(), data.size()));
        }

        static TString GetIssueId(const Ydb::Monitoring::IssueLog& issueLog) {
            const Ydb::Monitoring::Location& location(issueLog.location());
            TStringStream id;
            if (issueLog.status() != Ydb::Monitoring::StatusFlag::UNSPECIFIED) {
                id << Ydb::Monitoring::StatusFlag_Status_Name(issueLog.status()) << '-';
            }
            id << crc16(TStringBuilder() << issueLog.message() << issueLog.type());
            if (location.database().name()) {
                id << '-' << crc32(location.database().name());
            }
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
                        id << '-' << crc32(location.storage().pool().name());
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
            if (location.compute().schema().path()) {
                id << '-' << crc32(location.compute().schema().path());
            }
            if (location.compute().state_storage().pile().name()) {
                id << '-' << location.compute().state_storage().pile().name();
            }
            if (location.compute().state_storage().ring()) {
                id << '-' << location.compute().state_storage().ring();
            }
            if (location.compute().state_storage().node().id()) {
                id << '-' << location.compute().state_storage().node().id();
            }
            return id.Str();
        }

        void ReportStatus(Ydb::Monitoring::StatusFlag::Status status,
                          const TString& message,
                          ETags setTag,
                          std::initializer_list<ETags> includeTags,
                          const TList<TIssueRecord>& includeRecords) {
            OverallStatus = MaxStatus(OverallStatus, status);
            if (IsErrorStatus(status)) {
                std::vector<TString> reason;
                if (includeTags.size() != 0) {
                    for (const TIssueRecord& record : includeRecords) {
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
                TIssueRecord& issueRecord(*IssueRecords.emplace(IssueRecords.end()));
                Ydb::Monitoring::IssueLog& issueLog(issueRecord.IssueLog);
                issueLog.set_status(status);
                issueLog.set_message(message);
                if (Location.ByteSizeLong() > 0) {
                    issueLog.mutable_location()->CopyFrom(Location);
                }
                if (Type) {
                    issueLog.set_type(Type);
                }
                issueLog.set_id(GetIssueId(issueLog));
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

        void ReportStatus(Ydb::Monitoring::StatusFlag::Status status,
                          const TString& message = {},
                          ETags setTag = ETags::None,
                          std::initializer_list<ETags> includeTags = {}) {
            ReportStatus(status, message, setTag, includeTags, IssueRecords);
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

        bool ReportWithMaxChildStatus(const TString& message = {},
                                        ETags setTag = ETags::None,
                                        std::initializer_list<ETags> includeTags = {}) {
            if (HasTags(includeTags)) {
                ReportStatus(FindMaxStatus(includeTags), message, setTag, includeTags);
                return true;
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
            IssueRecords.splice(IssueRecords.end(), std::move(lower.IssueRecords));
        }
    };

    struct TSelfCheckContext : TSelfCheckResult {
        TSelfCheckResult* Upper;

        TSelfCheckContext(TSelfCheckResult* upper)
            : Upper(upper)
        {
            if (Upper) {
                Location.CopyFrom(upper->Location);
                Level = upper->Level + 1;
            }
        }

        TSelfCheckContext(TSelfCheckResult* upper, const TString& type)
            : TSelfCheckContext(upper)
        {
            Type = type;
        }

        TSelfCheckContext(const TSelfCheckContext&) = delete;

        ~TSelfCheckContext() {
            if (Upper) {
                Upper->InheritFrom(*this);
            }
        }
    };

    template<typename T>
    struct TRequestResponse {
        std::variant<std::monostate, std::unique_ptr<T>, TString> Response;
        NWilson::TSpan Span;

        TRequestResponse() = default;
        TRequestResponse(NWilson::TSpan&& span)
            : Span(std::move(span))
        {}

        TRequestResponse(const TRequestResponse&) = delete;
        TRequestResponse(TRequestResponse&&) = default;
        TRequestResponse& operator =(const TRequestResponse&) = delete;
        TRequestResponse& operator =(TRequestResponse&&) = default;

        bool Set(std::unique_ptr<T>&& response) {
            if (IsDone()) {
                return false;
            }
            constexpr bool hasErrorCheck = requires(const std::unique_ptr<T>& r) {TSelfCheckRequest::IsSuccess(r);};
            if constexpr (hasErrorCheck) {
                if (!TSelfCheckRequest::IsSuccess(response)) {
                    return Error(TSelfCheckRequest::GetError(response));
                }
            }
            Span.EndOk();
            Response = std::move(response);
            return true;
        }

        bool Set(TAutoPtr<TEventHandle<T>>&& response) {
            return Set(std::unique_ptr<T>(response->Release().Release()));
        }

        bool Error(const TString& error) {
            if (!IsDone()) {
                Span.EndError(error);
                Response = error;
                return true;
            }
            return false;
        }

        bool IsOk() const {
            return std::holds_alternative<std::unique_ptr<T>>(Response);
        }

        bool IsError() const {
            return std::holds_alternative<TString>(Response);
        }

        bool IsDone() const {
            return Response.index() != 0;
        }

        explicit operator bool() const {
            return IsOk();
        }

        T* Get() {
            return std::get<std::unique_ptr<T>>(Response).get();
        }

        const T* Get() const {
            return std::get<std::unique_ptr<T>>(Response).get();
        }

        T& GetRef() {
            return *Get();
        }

        const T& GetRef() const {
            return *Get();
        }

        T* operator ->() {
            return Get();
        }

        const T* operator ->() const {
            return Get();
        }

        T& operator *() {
            return GetRef();
        }

        const T& operator *() const {
            return GetRef();
        }

        TString GetError() const {
            return std::get<TString>(Response);
        }

        void Event(const TString& name) {
            if (Span) {
                Span.Event(name);
            }
        }
    };

    static bool IsSuccess(const std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySetResult>& ev) {
        return (ev->Request->ResultSet.size() > 0) && (std::find_if(ev->Request->ResultSet.begin(), ev->Request->ResultSet.end(),
            [](const auto& entry) {
                return entry.Status == TSchemeCacheNavigate::EStatus::Ok;
            }) != ev->Request->ResultSet.end());
    }

    static TString GetError(const std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySetResult>& ev) {
        if (ev->Request->ResultSet.size() == 0) {
            return "empty response";
        }
        for (const auto& entry : ev->Request->ResultSet) {
            if (entry.Status != TSchemeCacheNavigate::EStatus::Ok) {
                switch (entry.Status) {
                    case TSchemeCacheNavigate::EStatus::Ok:
                        return "Ok";
                    case TSchemeCacheNavigate::EStatus::Unknown:
                        return "Unknown";
                    case TSchemeCacheNavigate::EStatus::RootUnknown:
                        return "RootUnknown";
                    case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                        return "PathErrorUnknown";
                    case TSchemeCacheNavigate::EStatus::PathNotTable:
                        return "PathNotTable";
                    case TSchemeCacheNavigate::EStatus::PathNotPath:
                        return "PathNotPath";
                    case TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                        return "TableCreationNotComplete";
                    case TSchemeCacheNavigate::EStatus::LookupError:
                        return "LookupError";
                    case TSchemeCacheNavigate::EStatus::RedirectLookupError:
                        return "RedirectLookupError";
                    case TSchemeCacheNavigate::EStatus::AccessDenied:
                        return "AccessDenied";
                    default:
                        return ::ToString(static_cast<int>(entry.Status));
                }
            }
        }
        return "no error";
    }

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
    THashMap<TSubDomainKey, std::optional<TString>> FilterDomainKey; // nullopt for serverless
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
    std::vector<std::unordered_set<TNodeId>> PileNumToNodeIds;
    std::optional<TRequestResponse<TEvSysView::TEvGetStoragePoolsResponse>> StoragePools;
    std::optional<TRequestResponse<TEvSysView::TEvGetGroupsResponse>> Groups;
    std::optional<TRequestResponse<TEvSysView::TEvGetVSlotsResponse>> VSlots;
    std::optional<TRequestResponse<TEvSysView::TEvGetPDisksResponse>> PDisks;
    std::optional<TRequestResponse<TEvNodeWardenStorageConfig>> NodeWardenStorageConfig;
    std::optional<TRequestResponse<TEvStateStorage::TEvBoardInfo>> DatabaseBoardInfo;
    std::optional<TRequestResponse<TEvStateStorage::TEvListStateStorageResult>> StateStorageInfo;
    std::optional<TRequestResponse<TEvStateStorage::TEvListSchemeBoardResult>> SchemeBoardInfo;
    std::optional<TRequestResponse<TEvStateStorage::TEvListBoardResult>> BoardInfo;
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

    struct THintOverloadedShard {
        TTabletId TabletId;
        TFollowerId FollowerId;
        double CPUCores;
        TTabletId SchemeShardId;
        TString Message;
    };

    THashMap<TString, THintOverloadedShard> OverloadedShardHints;
    static constexpr size_t MAX_OVERLOADED_SHARDS_HINTS = 10;
    static constexpr double OVERLOADED_SHARDS_CPU_CORES = 0.75;

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

        // Some tablets (currently only BSC sys view) do not set response cookie
        // So we use special constant ids for those requests
        enum ERequestId : ui64 {
            RequestStoragePools = 1'000'000,
            RequestGroups,
            RequestVSlots,
            RequestPDisks,
            RequestGetPartitionStats,
        };

        void MakeRequest(TTabletId tabletId, const TString& key, ui64 requestId) {
            RequestsInFlight.emplace(requestId, TRequestState{tabletId, key, TMonotonic::Now()});
        }

        ui64 MakeRequest(TTabletId tabletId, const TString& key) {
            MakeRequest(tabletId, key, ++RequestId);
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
            DatabaseState[DomainPath].SchemeShardId = RootSchemeShardId;
        }

        if (BsControllerId) {
            TabletRequests.TabletStates[BsControllerId].Database = DomainPath;
            TabletRequests.TabletStates[BsControllerId].Type = TTabletTypes::BSController;
            RequestBsController();
        }

        if (!IsSpecificDatabaseFilter() || IsBridgeMode(TActivationContext::AsActorContext())) {
            NodeWardenStorageConfig = RequestStorageConfig();
        }

        if (!IsSpecificDatabaseFilter()) {
            StateStorageInfo = TRequestResponse<TEvStateStorage::TEvListStateStorageResult>(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, "TEvStateStorage::TEvListStateStorageResult"));
            Send(MakeStateStorageProxyID(), new TEvStateStorage::TEvListStateStorage(), 0/*flags*/, 0/*cookie*/, Span.GetTraceId());
            SchemeBoardInfo = TRequestResponse<TEvStateStorage::TEvListSchemeBoardResult>(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, "TEvStateStorage::TEvListSchemeBoardResult"));
            Send(MakeStateStorageProxyID(), new TEvStateStorage::TEvListSchemeBoard(false), 0/*flags*/, 0/*cookie*/, Span.GetTraceId());
            BoardInfo = TRequestResponse<TEvStateStorage::TEvListBoardResult>(Span.CreateChild(TComponentTracingLevels::TTablet::Detailed, "TEvStateStorage::TEvListBoardResult"));
            Send(MakeStateStorageProxyID(), new TEvStateStorage::TEvListBoard(), 0/*flags*/, 0/*cookie*/, Span.GetTraceId());
            Requests += 3;
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
        if (!NodeWardenStorageConfig->Set(std::move(ev))) {
            return;
        }
        const NKikimrBlobStorage::TStorageConfig& config = *NodeWardenStorageConfig->Get()->Config;
        RequestDone("TEvNodeWardenStorageConfig");
        if (config.GetSelfManagementConfig().GetEnabled() && config.GetGeneration() == 0) {
            auto result = MakeHolder<TEvSelfCheckResult>();
            result->Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_MAINTENANCE_REQUIRED);
            auto* issue = result->Result.add_issue_log();
            issue->set_id("0");
            issue->set_status(Ydb::Monitoring::StatusFlag::RED);
            issue->set_message("Cluster is not bootstrapped");
            return ReplyAndPassAway(std::move(result));
        }
        if (config.HasBlobStorageConfig()) {
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
    }

    void RequestNodes(TIntrusiveConstPtr<TStateStorageInfo> info) {
        for (const auto& group : info->RingGroups) {
            for (const auto& ring : group.Rings) {
                for (const auto& replica : ring.Replicas) {
                    RequestGenericNode(replica.NodeId());
                }
            }
        }
    }

    void Handle(TEvStateStorage::TEvListStateStorageResult::TPtr& ev) {
        if (StateStorageInfo->Set(std::move(ev))) {
            RequestNodes(StateStorageInfo->Get()->Info);
            RequestDone("TEvListStateStorageResult");
        }
    }

    void Handle(TEvStateStorage::TEvListSchemeBoardResult::TPtr& ev) {
        if (SchemeBoardInfo->Set(std::move(ev))) {
            RequestNodes(SchemeBoardInfo->Get()->Info);
            RequestDone("TEvListSсhemeBoardResult");
        }
    }

    void Handle(TEvStateStorage::TEvListBoardResult::TPtr& ev) {
        if (BoardInfo->Set(std::move(ev))) {
            RequestNodes(BoardInfo->Get()->Info);
            RequestDone("TEvListBoardResult");
        }
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
            hFunc(TEvStateStorage::TEvListStateStorageResult, Handle);
            hFunc(TEvStateStorage::TEvListSchemeBoardResult, Handle);
            hFunc(TEvStateStorage::TEvListBoardResult, Handle);
        }
    }

    void RequestDone(const char* name) {
        --Requests;
        BLOG_TRACE("RequestDone(" << name << "): remaining " << Requests);
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

    [[nodiscard]] TRequestResponse<TEvHive::TEvResponseHiveInfo> RequestHiveInfo(TTabletId hiveId, std::optional<TSubDomainKey> filterDomain) {
        THolder<TEvHive::TEvRequestHiveInfo> request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
        request->Record.SetReturnFollowers(true);
        if (filterDomain) {
            request->Record.MutableFilterTabletsByObjectDomain()->CopyFrom(*filterDomain);
        }
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
        request->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
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
        request->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
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
    std::vector<int> GetRequiredFields();

    template<>
    std::vector<int> GetRequiredFields<TEvWhiteboard::TEvSystemStateRequest>() {
        return {
                NKikimrWhiteboard::TSystemStateInfo::kPoolStatsFieldNumber,
                NKikimrWhiteboard::TSystemStateInfo::kLoadAverageFieldNumber,
                NKikimrWhiteboard::TSystemStateInfo::kNumberOfCpusFieldNumber,
                NKikimrWhiteboard::TSystemStateInfo::kMaxClockSkewPeerIdFieldNumber,
                NKikimrWhiteboard::TSystemStateInfo::kLocationFieldNumber,
                NKikimrWhiteboard::TSystemStateInfo::kMaxClockSkewWithPeerUsFieldNumber,
            };
    }

    template<>
    std::vector<int> GetRequiredFields<TEvWhiteboard::TEvVDiskStateRequest>() {
        return {
                NKikimrWhiteboard::TVDiskStateInfo::kVDiskIdFieldNumber,
                NKikimrWhiteboard::TVDiskStateInfo::kPDiskIdFieldNumber,
                NKikimrWhiteboard::TVDiskStateInfo::kVDiskStateFieldNumber,
                NKikimrWhiteboard::TVDiskStateInfo::kReplicatedFieldNumber,
                NKikimrWhiteboard::TVDiskStateInfo::kDiskSpaceFieldNumber,
        };
    }

    template<>
    std::vector<int> GetRequiredFields<TEvWhiteboard::TEvPDiskStateRequest>() {
        return {
                NKikimrWhiteboard::TPDiskStateInfo::kPDiskIdFieldNumber,
                NKikimrWhiteboard::TPDiskStateInfo::kPathFieldNumber,
                NKikimrWhiteboard::TPDiskStateInfo::kAvailableSizeFieldNumber,
                NKikimrWhiteboard::TPDiskStateInfo::kTotalSizeFieldNumber,
                NKikimrWhiteboard::TPDiskStateInfo::kStateFieldNumber,
        };
    }

    template<>
    std::vector<int> GetRequiredFields<TEvWhiteboard::TEvBSGroupStateRequest>() {
        return {
                NKikimrWhiteboard::TBSGroupStateInfo::kGroupGenerationFieldNumber,
                NKikimrWhiteboard::TBSGroupStateInfo::kGroupIDFieldNumber,
                NKikimrWhiteboard::TBSGroupStateInfo::kStoragePoolNameFieldNumber,
                NKikimrWhiteboard::TBSGroupStateInfo::kBridgePileIdFieldNumber,
                NKikimrWhiteboard::TBSGroupStateInfo::kErasureSpeciesFieldNumber,
                NKikimrWhiteboard::TBSGroupStateInfo::kVDiskIdsFieldNumber,
        };
    }

    template<typename TEvent>
    [[nodiscard]] TRequestResponse<typename WhiteboardResponse<TEvent>::Type> RequestNodeWhiteboard(TNodeId nodeId) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        auto request = MakeHolder<TEvent>();
        for (int field : GetRequiredFields<TEvent>()) {
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
            NodeSystemState.emplace(nodeId, RequestNodeWhiteboard<TEvWhiteboard::TEvSystemStateRequest>(nodeId));
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
        if (!DatabaseBoardInfo->Set(std::move(ev))) {
            return;
        }
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
            case TEvWhiteboard::EvSystemStateRequest: {
                auto& request = NodeSystemState[nodeId];
                if (!request.IsOk()) {
                    request = RequestNodeWhiteboard<TEvWhiteboard::TEvSystemStateRequest>(nodeId);
                }
                break;
            }
            case TEvWhiteboard::EvVDiskStateRequest: {
                auto& request = NodeVDiskState[nodeId];
                if (!request.IsOk()) {
                    request = RequestNodeWhiteboard<TEvWhiteboard::TEvVDiskStateRequest>(nodeId);
                }
                break;
            }
            case TEvWhiteboard::EvPDiskStateRequest: {
                auto& request = NodePDiskState[nodeId];
                if (!request.IsOk()) {
                    request = RequestNodeWhiteboard<TEvWhiteboard::TEvPDiskStateRequest>(nodeId);
                }
                break;
            }
            case TEvWhiteboard::EvBSGroupStateRequest: {
                auto& request = NodeBSGroupState[nodeId];
                if (!request.IsOk()) {
                    request = RequestNodeWhiteboard<TEvWhiteboard::TEvBSGroupStateRequest>(nodeId);
                }
                break;
            }
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
                if (RetryRequestNodeWhiteboard<TEvWhiteboard::TEvSystemStateRequest>(nodeId)) {
                    NodeSystemState.erase(nodeId);
                } else {
                    RequestDone("undelivered of TEvSystemStateRequest");
                    UnavailableComputeNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == TEvWhiteboard::EvVDiskStateRequest) {
            if (NodeVDiskState.count(nodeId) && NodeVDiskState[nodeId].Error(error)) {
                if (RetryRequestNodeWhiteboard<TEvWhiteboard::TEvVDiskStateRequest>(nodeId)) {
                    NodeVDiskState.erase(nodeId);
                } else {
                    RequestDone("undelivered of TEvVDiskStateRequest");
                    UnavailableStorageNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == TEvWhiteboard::EvPDiskStateRequest) {
            if (NodePDiskState.count(nodeId) && NodePDiskState[nodeId].Error(error)) {
                if (RetryRequestNodeWhiteboard<TEvWhiteboard::TEvPDiskStateRequest>(nodeId)) {
                    NodePDiskState.erase(nodeId);
                } else {
                    RequestDone("undelivered of TEvPDiskStateRequest");
                    UnavailableStorageNodes.insert(nodeId);
                }
            }
        }
        if (ev->Get()->SourceType == TEvWhiteboard::EvBSGroupStateRequest) {
            if (NodeBSGroupState.count(nodeId) && NodeBSGroupState[nodeId].Error(error)) {
                if (RetryRequestNodeWhiteboard<TEvWhiteboard::TEvBSGroupStateRequest>(nodeId)) {
                    NodeBSGroupState.erase(nodeId);
                } else {
                    RequestDone("undelivered of TEvBSGroupStateRequest");
                }
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        TString error = "NodeDisconnected";
        if (NodeSystemState.count(nodeId) && NodeSystemState[nodeId].Error(error)) {
            if (RetryRequestNodeWhiteboard<TEvWhiteboard::TEvSystemStateRequest>(nodeId)) {
                NodeSystemState.erase(nodeId);
            } else {
                RequestDone("node disconnected with TEvSystemStateRequest");
                UnavailableComputeNodes.insert(nodeId);
            }
        }
        if (NodeVDiskState.count(nodeId) && NodeVDiskState[nodeId].Error(error)) {
            if (RetryRequestNodeWhiteboard<TEvWhiteboard::TEvVDiskStateRequest>(nodeId)) {
                NodeVDiskState.erase(nodeId);
            } else {
                RequestDone("node disconnected with TEvVDiskStateRequest");
                UnavailableStorageNodes.insert(nodeId);
            }
        }
        if (NodePDiskState.count(nodeId) && NodePDiskState[nodeId].Error(error)) {
            if (RetryRequestNodeWhiteboard<TEvWhiteboard::TEvPDiskStateRequest>(nodeId)) {
                NodePDiskState.erase(nodeId);
            } else {
                RequestDone("node disconnected with TEvPDiskStateRequest");
                UnavailableStorageNodes.insert(nodeId);
            }
        }
        if (NodeBSGroupState.count(nodeId) && NodeBSGroupState[nodeId].Error(error)) {
            if (RetryRequestNodeWhiteboard<TEvWhiteboard::TEvBSGroupStateRequest>(nodeId)) {
                NodeBSGroupState.erase(nodeId);
            } else {
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
        if (!NodesInfo->Set(std::move(ev))) {
            return;
        }
        for (const auto& ni : NodesInfo->Get()->Nodes) {
            MergedNodeInfo[ni.NodeId] = &ni;
            if (IsStaticNode(ni.NodeId) && needComputeFromStaticNodes) {
                DatabaseState[DomainPath].ComputeNodeIds.push_back(ni.NodeId);
                RequestComputeNode(ni.NodeId);
            }
        }
        if (NodesInfo->Get()->PileMap) {
            for (size_t pileNum = 0; pileNum < NodesInfo->Get()->PileMap->size(); ++pileNum) {
                PileNumToNodeIds.emplace_back();
                for (TNodeId nodeId : (*NodesInfo->Get()->PileMap)[pileNum]) {
                    PileNumToNodeIds[pileNum].insert(nodeId);
                }
            }
        }
        RequestDone("TEvNodesInfo");
    }

    bool IsStaticGroup(TGroupId groupId) {
        return !(groupId & 0x80000000);
    }

    bool NeedWhiteboardForStaticGroupsWithUnknownStatus() {
        return NodeWardenStorageConfig && !IsSpecificDatabaseFilter();
    }

    void Handle(TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(TTabletRequestsState::RequestStoragePools);
        if (!StoragePools->Set(std::move(ev))) {
            return;
        }
        AggregateBSControllerState();
        RequestDone("TEvGetStoragePoolsRequest");
    }

    void Handle(TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(TTabletRequestsState::RequestGroups);
        if (!Groups->Set(std::move(ev))) {
            return;
        }
        AggregateBSControllerState();
        RequestDone("TEvGetGroupsRequest");
    }

    void Handle(TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(TTabletRequestsState::RequestVSlots);
        if (!VSlots->Set(std::move(ev))) {
            return;
        }
        AggregateBSControllerState();
        RequestDone("TEvGetVSlotsRequest");
    }

    void Handle(TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(TTabletRequestsState::RequestPDisks);
        if (!PDisks->Set(std::move(ev))) {
            return;
        }
        AggregateBSControllerState();
        RequestDone("TEvGetPDisksRequest");
    }

    void Handle(TEvSysView::TEvGetPartitionStatsResult::TPtr& ev) {
        //TTabletId schemeShardId = TabletRequests.CompleteRequest(ev->Cookie);
        TTabletId schemeShardId = TabletRequests.CompleteRequest(TTabletRequestsState::RequestGetPartitionStats);
        if (schemeShardId) {
            auto& partitionStatsResult(GetPartitionStatsResult[schemeShardId]);
            if (!partitionStatsResult.Set(std::move(ev))) {
                return;
            }
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
        if (!response.Set(std::move(ev))) {
            return;
        }
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

    void AskHive(const TString& database, TTabletId hiveId, std::optional<TSubDomainKey> filterDomain) {
        TabletRequests.TabletStates[hiveId].Database = database;
        TabletRequests.TabletStates[hiveId].Type = TTabletTypes::Hive;
        if (HiveNodeStats.count(hiveId) == 0) {
            HiveNodeStats[hiveId] = RequestHiveNodeStats(hiveId);
            ++HiveNodeStatsToGo;
        }
        if (HiveInfo.count(hiveId) == 0) {
            HiveInfo[hiveId] = RequestHiveInfo(hiveId, filterDomain);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>& response = NavigateKeySet[ev->Get()->Request->Cookie];
        if (!response.Set(std::move(ev))) {
            return;
        }
        if (response.IsOk()) {
            auto domainInfo = response.Get()->Request->ResultSet.begin()->DomainInfo;
            TString path = CanonizePath(response.Get()->Request->ResultSet.begin()->Path);
            TSubDomainKey subDomainKey(domainInfo->DomainKey.OwnerId, domainInfo->DomainKey.LocalPathId);
            if (domainInfo->IsServerless()) {
                if (NeedHealthCheckForServerless(domainInfo)) {
                    if (SharedDatabases.emplace(domainInfo->ResourcesDomainKey, path).second) {
                        RequestSchemeCacheNavigate(domainInfo->ResourcesDomainKey);
                    }
                    DatabaseState[path].ResourcePathId = domainInfo->ResourcesDomainKey;
                    DatabaseState[path].ServerlessComputeResourcesMode = domainInfo->ServerlessComputeResourcesMode;
                } else {
                    DatabaseState.erase(path);
                    FilterDomainKey[subDomainKey] = std::nullopt;
                    RequestDone("TEvNavigateKeySetResult");
                    return;
                }
            }

            FilterDomainKey[subDomainKey] = path;

            TTabletId hiveId = domainInfo->Params.GetHive();
            auto filterDomain = IsSpecificDatabaseFilter() ? std::make_optional(subDomainKey) : std::nullopt;
            if (hiveId) {
                DatabaseState[path].HiveId = hiveId;
                if (NeedToAskHive(hiveId)) {
                    AskHive(path, hiveId, filterDomain);
                }
            } else if (RootHiveId && NeedToAskHive(RootHiveId)) {
                DatabaseState[DomainPath].HiveId = RootHiveId;
                AskHive(DomainPath, RootHiveId, filterDomain);
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
        } else {
            BLOG_D("TEvNavigateKeySetResult error: " << response.GetError());
            if (response.GetError() == "PathErrorUnknown") {
                auto result = MakeHolder<TEvSelfCheckResult>();
                result->Result.set_self_check_result(Ydb::Monitoring::SelfCheck_Result::SelfCheck_Result_UNSPECIFIED);
                auto* issue = result->Result.add_issue_log();
                issue->set_id("0");
                issue->set_status(Ydb::Monitoring::StatusFlag::GREY);
                issue->set_message("Database does not exist");
                return ReplyAndPassAway(std::move(result));
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
            if (!response.Set(std::move(ev))) {
                return;
            }
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
        if (!HiveInfo[hiveId].Set(std::move(ev))) {
            return;
        }
        RequestDone("TEvResponseHiveInfo");
    }

    void Handle(TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        TabletRequests.CompleteRequest(ev->Cookie);
        if (!ListTenants->Set(std::move(ev))) {
            return;
        }
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
        if (!nodeSystemState.Set(std::move(ev))) {
            return;
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
                        continue;
                    } else if (!itDomain->second) {
                        if (!FilterDatabase || FilterDatabase == dbPath) {
                            database = &dbState;
                        } else {
                            continue;
                        }
                    } else {
                        auto itDatabase = DatabaseState.find(*itDomain->second);
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
                        if (itFilterDomainKey != FilterDomainKey.end() && itFilterDomainKey->second) {
                            TString path(*itFilterDomainKey->second);
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
            groupState.Id = groupId;
            groupState.ErasureSpecies = group.GetInfo().GetErasureSpeciesV2();
            groupState.Generation = group.GetInfo().GetGeneration();
            groupState.LayoutCorrect = group.GetInfo().GetLayoutCorrect();
            groupState.BridgePileId = TBridgePileId::FromProto(&group.GetInfo(), &NKikimrSysView::TGroupInfo::GetBridgePileId);
            if (group.GetInfo().HasProxyGroupId()) {
                GroupState[group.GetInfo().GetProxyGroupId()].BridgeGroups.push_back(&groupState);
            } else {
                StoragePoolState[poolId].Groups.emplace(groupId);
            }
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

    void AggregateWhiteboard() {
        for (auto& [nodeId, nodeSystemState] : NodeSystemState) {
            if (nodeSystemState.IsOk()) {
                for (NKikimrWhiteboard::TSystemStateInfo& state : *nodeSystemState->Record.MutableSystemStateInfo()) {
                    state.set_nodeid(nodeId);
                    MergedNodeSystemState[nodeId] = &state;
                }
            }
        }
        for (auto& [nodeId, nodeVDiskState] : NodeVDiskState) {
            if (nodeVDiskState.IsOk()) {
                for (NKikimrWhiteboard::TVDiskStateInfo& state : *nodeVDiskState->Record.MutableVDiskStateInfo()) {
                    state.set_nodeid(nodeId);
                    auto id = GetVDiskId(state.vdiskid());
                    MergedVDiskState[id] = &state;
                }
            }
        }
        for (auto& [nodeId, nodePDiskState] : NodePDiskState) {
            if (nodePDiskState.IsOk()) {
                for (NKikimrWhiteboard::TPDiskStateInfo& state : *nodePDiskState->Record.MutablePDiskStateInfo()) {
                    state.set_nodeid(nodeId);
                    auto id = GetPDiskId(state);
                    MergedPDiskState[id] = &state;
                }
            }
        }
        for (auto& [nodeId, nodeBSGroupState] : NodeBSGroupState) {
            if (nodeBSGroupState.IsOk()) {
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
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Pool usage is over 99%", ETags::OverloadState);
            } else if (poolStats.usage() >= 0.95) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Pool usage is over 95%", ETags::OverloadState);
            } else {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        } else {
            if (poolStats.usage() >= 0.99) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Pool usage is over 99%", ETags::OverloadState);
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

    void FillComputeNodeStatus(TDatabaseState& databaseState, TNodeId nodeId, Ydb::Monitoring::ComputeNodeStatus& computeNodeStatus, TSelfCheckContext context) {
        FillNodeInfo(nodeId, context.Location.mutable_compute()->mutable_node());

        TSelfCheckContext rrContext(&context, "NODE_UPTIME");
        if (databaseState.NodeRestartsPerPeriod[nodeId] >= HealthCheckConfig.GetThresholds().GetNodeRestartsOrange()) {
            rrContext.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Node is restarting too often", ETags::Uptime);
        } else if (databaseState.NodeRestartsPerPeriod[nodeId] >= HealthCheckConfig.GetThresholds().GetNodeRestartsYellow()) {
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

            if (databaseState.MaxClockSkewNodeId == nodeId) {
                TSelfCheckContext tdContext(&context, "CLOCK_SKEW");
                Ydb::Monitoring::ClockSkewStatus& clockSkewStatus = *computeNodeStatus.mutable_clock_skew();
                ui64 clockSkew = abs(databaseState.MaxClockSkewNodeAvgUs);
                clockSkewStatus.set_clock_skew(-databaseState.MaxClockSkewNodeAvgUs / 1000); // in ms
                if (clockSkew >= HealthCheckConfig.GetThresholds().GetNodesTimeDifferenceOrange()) {
                    tdContext.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Node clock skew exceeds threshold", ETags::NodeClockSkew);
                } else if (clockSkew >= HealthCheckConfig.GetThresholds().GetNodesTimeDifferenceYellow()) {
                    tdContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Node clock skew above recommended limit", ETags::NodeClockSkew);
                } else {
                    tdContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                }
                clockSkewStatus.set_overall(tdContext.GetOverallStatus());
            }
        } else {
            // context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
            //                      TStringBuilder() << "Compute node is not available",
             //                      ETags::NodeState);
        }
        computeNodeStatus.set_id(ToString(nodeId));
        computeNodeStatus.set_overall(context.GetOverallStatus());
    }

    void FillComputePileStatus(TDatabaseState& databaseState, size_t pileNum, const TString& pileName,
            const TVector<TNodeId>& computeNodeIds, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckContext context) {
        // temporary solution - it's better to rework the concept of "piles" in health check
        if (PileNumToNodeIds.size() <= pileNum) {
            return;
        }
        context.Location.mutable_compute()->mutable_pile()->set_name(pileName);
        const auto& pileNodes(PileNumToNodeIds[pileNum]);
        for (TNodeId nodeId : computeNodeIds) {
            if (pileNodes.count(nodeId) == 0) {
                continue;
            }
            auto& computeNode = *computeStatus.add_nodes();
            FillComputeNodeStatus(databaseState, nodeId, computeNode, {&context, "COMPUTE_NODE"});

        }
        FillComputeDatabaseClockSkew(databaseState, computeStatus, {&context, "CLOCK_SKEW"}, context);
        context.ReportWithMaxChildStatus("Some nodes are restarting too often", ETags::PileComputeState, {ETags::Uptime});
        context.ReportWithMaxChildStatus("Compute is overloaded", ETags::PileComputeState, {ETags::OverloadState});
        context.ReportWithMaxChildStatus("Compute quota usage", ETags::PileComputeState, {ETags::QuotaUsage});
        if (!context.ReportWithMaxChildStatus("Clock skew issues", ETags::PileComputeState, {ETags::DatabaseClockSkew})) {
            context.ReportWithMaxChildStatus("Clock skew issues", ETags::PileComputeState, {ETags::NodeClockSkew});
        }
    }

    void FillComputeDatabaseQuota(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckContext context) {
        auto itDescribe = DescribeByPath.find(databaseState.Path);
        if (itDescribe != DescribeByPath.end() && itDescribe->second.IsOk()) {
            const auto& domain(itDescribe->second->GetRecord().GetPathDescription().GetDomainDescription());
            if (domain.GetPathsLimit() > 0) {
                float usage = (float)domain.GetPathsInside() / domain.GetPathsLimit();
                computeStatus.set_paths_quota_usage(usage);
                if (static_cast<i64>(domain.GetPathsLimit()) - static_cast<i64>(domain.GetPathsInside()) <= 1) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Paths quota exhausted", ETags::QuotaUsage);
                } else if (usage >= 0.99) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Paths quota usage is over 99%", ETags::QuotaUsage);
                } else if (usage >= 0.90) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Paths quota usage is over 90%", ETags::QuotaUsage);
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                }
            }
            if (domain.GetShardsLimit() > 0) {
                float usage = (float)domain.GetShardsInside() / domain.GetShardsLimit();
                computeStatus.set_shards_quota_usage(usage);
                if (static_cast<i64>(domain.GetShardsLimit()) - static_cast<i64>(domain.GetShardsInside()) <= 1) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Shards quota exhausted", ETags::QuotaUsage);
                } else if (usage >= 0.99) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Shards quota usage is over 99%", ETags::QuotaUsage);
                } else if (usage >= 0.90) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Shards quota usage is over 90%", ETags::QuotaUsage);
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                }
            }
        }
    }

    void FillComputeDatabaseClockSkew(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckContext context, const TSelfCheckContext& relatedContext) {
        ui64 clockSkew = databaseState.MaxClockSkewUs;
        if (clockSkew >= HealthCheckConfig.GetThresholds().GetNodesTimeDifferenceOrange()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Database clock skew exceeds threshold", ETags::DatabaseClockSkew, {ETags::NodeClockSkew}, relatedContext.IssueRecords);
        } else if (clockSkew >= HealthCheckConfig.GetThresholds().GetNodesTimeDifferenceYellow()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Database clock skew above recommended limit", ETags::DatabaseClockSkew, {ETags::NodeClockSkew}, relatedContext.IssueRecords);
        } else {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
        }
        computeStatus.mutable_clock_skew()->set_clock_skew(clockSkew / 1000); // in ms
        computeStatus.mutable_clock_skew()->set_overall(context.GetOverallStatus());
    }

    struct TNodeClockSkewState {
        TNodeId NodeId = 0;
        int NumberOfReporters = 0;
        i64 SumClockSkewUs = 0;
    };

    void CalculateClockSkewState(TDatabaseState& databaseState, const TVector<TNodeId>& computeNodes) {
        std::unordered_map<TNodeId, TNodeClockSkewState> nodeClockSkewState;
        for (TNodeId nodeId : computeNodes) {
            auto itNodeSystemState = MergedNodeSystemState.find(nodeId);
            if (itNodeSystemState != MergedNodeSystemState.end()) {
                const NKikimrWhiteboard::TSystemStateInfo& nodeSystemState(*itNodeSystemState->second);
                if (nodeSystemState.HasMaxClockSkewPeerId()) {
                    TNodeId peerId = nodeSystemState.GetMaxClockSkewPeerId();
                    i64 timeDifferenceUs = nodeSystemState.GetMaxClockSkewWithPeerUs();
                    auto& clockSkewState = nodeClockSkewState[peerId];
                    clockSkewState.NumberOfReporters++;
                    clockSkewState.SumClockSkewUs += timeDifferenceUs;
                    databaseState.MaxClockSkewUs = std::max<ui64>(databaseState.MaxClockSkewUs, abs(timeDifferenceUs));
                }
            }
        }
        auto itMaxClockSkew = std::ranges::max_element(nodeClockSkewState, [](const auto& a, const auto& b) {
            return a.second.NumberOfReporters < b.second.NumberOfReporters;
        });
        if (itMaxClockSkew != nodeClockSkewState.end()) {
            if (itMaxClockSkew->second.NumberOfReporters * 2 >= static_cast<int>(nodeClockSkewState.size())) { // at least 50% of reporters
                databaseState.MaxClockSkewNodeId = itMaxClockSkew->first;
                databaseState.MaxClockSkewNodeAvgUs = itMaxClockSkew->second.SumClockSkewUs / itMaxClockSkew->second.NumberOfReporters;
            }
        }
    }

    void FillCompute(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckContext context) {
        TVector<TNodeId>* computeNodeIds = &databaseState.ComputeNodeIds;
        if (databaseState.ResourcePathId
            && databaseState.ServerlessComputeResourcesMode != NKikimrSubDomains::EServerlessComputeResourcesModeExclusive)
        {
            auto itDatabase = FilterDomainKey.find(TSubDomainKey(databaseState.ResourcePathId.OwnerId, databaseState.ResourcePathId.LocalPathId));
            if (itDatabase != FilterDomainKey.end() && itDatabase->second) {
                const TString& sharedDatabaseName = *itDatabase->second;
                TDatabaseState& sharedDatabase = DatabaseState[sharedDatabaseName];
                computeNodeIds = &sharedDatabase.ComputeNodeIds;
            }
        }
        std::sort(computeNodeIds->begin(), computeNodeIds->end());
        computeNodeIds->erase(std::unique(computeNodeIds->begin(), computeNodeIds->end()), computeNodeIds->end());
        bool bridgeMode = NodeWardenStorageConfig && NodeWardenStorageConfig->IsOk() && NodeWardenStorageConfig->Get()->BridgeInfo
             && !NodeWardenStorageConfig->Get()->BridgeInfo->Piles.empty();
        if (computeNodeIds->empty()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There are no compute nodes", ETags::ComputeState);
        } else {
            if (bridgeMode) {
                const auto& piles(NodeWardenStorageConfig->Get()->BridgeInfo->Piles);
                TVector<TNodeId> activePileNodeIds;
                for (size_t pileNum = 0; pileNum < piles.size(); ++pileNum) {
                    if (piles[pileNum].IsPrimary || piles[pileNum].IsBeingPromoted) {
                        if (PileNumToNodeIds.size() <= pileNum) {
                            continue;
                        }
                        const auto& pileNodes(PileNumToNodeIds[pileNum]);
                        for (TNodeId nodeId : *computeNodeIds) {
                            if (pileNodes.count(nodeId) > 0) {
                                activePileNodeIds.push_back(nodeId);
                            }
                        }
                    }
                }
                CalculateClockSkewState(databaseState, activePileNodeIds);
                for (size_t pileNum = 0; pileNum < piles.size(); ++pileNum) {
                    FillComputePileStatus(databaseState, pileNum, piles[pileNum].Name, *computeNodeIds, computeStatus, {&context, "BRIDGE_PILE"});
                }
                context.ReportWithMaxChildStatus("There are compute issues", ETags::ComputeState, {ETags::PileComputeState});
            } else {
                CalculateClockSkewState(databaseState, *computeNodeIds);
                for (TNodeId nodeId : *computeNodeIds) {
                    auto& computeNode = *computeStatus.add_nodes();
                    FillComputeNodeStatus(databaseState, nodeId, computeNode, {&context, "COMPUTE_NODE"});
                }
                FillComputeDatabaseClockSkew(databaseState, computeStatus, {&context, "CLOCK_SKEW"}, context);
                context.ReportWithMaxChildStatus("Some nodes are restarting too often", ETags::ComputeState, {ETags::Uptime});
                context.ReportWithMaxChildStatus("Compute is overloaded", ETags::ComputeState, {ETags::OverloadState});
                context.ReportWithMaxChildStatus("Compute quota usage", ETags::ComputeState, {ETags::QuotaUsage});
                if (!context.ReportWithMaxChildStatus("Clock skew issues", ETags::ComputeState, {ETags::DatabaseClockSkew})) {
                    context.ReportWithMaxChildStatus("Clock skew issues", ETags::ComputeState, {ETags::NodeClockSkew});
                }
            }
        }
        Ydb::Monitoring::StatusFlag::Status systemStatus = FillSystemTablets(databaseState, {&context, "SYSTEM_TABLET"});
        if (systemStatus != Ydb::Monitoring::StatusFlag::GREEN && systemStatus != Ydb::Monitoring::StatusFlag::GREY) {
            context.ReportStatus(systemStatus, "Compute has issues with system tablets", ETags::ComputeState, {ETags::SystemTabletState});
        }
        FillComputeDatabaseQuota(databaseState, computeStatus, {&context, "COMPUTE_QUOTA"});
        Ydb::Monitoring::StatusFlag::Status tabletsStatus = Ydb::Monitoring::StatusFlag::GREEN;
        computeNodeIds->push_back(0); // for tablets without node
        for (TNodeId nodeId : *computeNodeIds) {
            tabletsStatus = MaxStatus(tabletsStatus, FillTablets(databaseState, nodeId, *computeStatus.mutable_tablets(), context));
        }
        if (tabletsStatus != Ydb::Monitoring::StatusFlag::GREEN) {
            context.ReportStatus(tabletsStatus, "Compute has issues with tablets", ETags::ComputeState, {ETags::TabletState});
        }
        if (ReturnHints) {
            auto schemeShardId = databaseState.SchemeShardId;
            if (schemeShardId) {
                for (const auto& [path, hint] : OverloadedShardHints) {
                    if (hint.SchemeShardId == schemeShardId) {
                        TSelfCheckContext hintContext(&context, "HINT-OVERLOADED-SHARD");
                        //hintContext.Location.mutable_compute()->mutable_tablet()->set_type(NKikimrTabletBase::TTabletTypes::EType_Name(NKikimrTabletBase::TTabletTypes::DataShard));
                        TStringBuilder tabletId;
                        tabletId << hint.TabletId;
                        if (hint.FollowerId) {
                            tabletId << '-' << hint.FollowerId;
                        }
                        hintContext.Location.mutable_compute()->mutable_tablet()->add_id(tabletId);
                        hintContext.Location.mutable_compute()->mutable_schema()->set_type("table");
                        hintContext.Location.mutable_compute()->mutable_schema()->set_path(path);
                        hintContext.ReportStatus(Ydb::Monitoring::StatusFlag::UNSPECIFIED, hint.Message);
                    }
                }
            }
        }
        computeStatus.set_overall(context.GetOverallStatus());
    }

    static TString GetVSlotId(const NKikimrSysView::TVSlotKey& vSlotKey) {
        return TStringBuilder()
                << vSlotKey.GetNodeId() << '-'
                << vSlotKey.GetPDiskId() << '-'
                << vSlotKey.GetVSlotId();
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

    static TString GetVDiskId(const NKikimrSysView::TVSlotInfo& vSlot) {
        return TStringBuilder()
               << vSlot.GetGroupId() << '-'
               << vSlot.GetGroupGeneration() << '-'
               << vSlot.GetFailRealm() << '-'
               << vSlot.GetFailDomain() << '-'
               << vSlot.GetVDisk();
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

    static TString GetPDiskId(const NKikimrSysView::TVSlotKey& vSlotKey) {
        return TStringBuilder() << vSlotKey.GetNodeId() << "-" << vSlotKey.GetPDiskId();
    }

    static TString GetPDiskId(const NKikimrSysView::TPDiskKey& pDiskKey) {
        return TStringBuilder() << pDiskKey.GetNodeId() << "-" << pDiskKey.GetPDiskId();
    }

    ui32 ParsePDiskStatusOrUnknown(const TString& statusString) {
        static const auto* descriptor = NKikimrBlobStorage::EDriveStatus_descriptor();
        const auto* valueDescriptor = descriptor->FindValueByName(statusString);
        return valueDescriptor ? valueDescriptor->number() : NKikimrBlobStorage::UNKNOWN;
    }

    void ReportPDiskState(NKikimrBlobStorage::TPDiskState::E stateEnum, TSelfCheckContext& context) {
        using E = NKikimrBlobStorage::TPDiskState;

        switch (stateEnum) {
            case E::Normal:
            case E::Stopped:
            case E::Initial:
            case E::InitialFormatRead:
            case E::InitialSysLogRead:
            case E::InitialCommonLogRead:
            case E::Unknown:
            case E::Reserved15:
            case E::Reserved16:
            case E::Reserved17:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
            case E::InitialFormatReadError:
            case E::InitialSysLogReadError:
            case E::InitialSysLogParseError:
            case E::InitialCommonLogReadError:
            case E::InitialCommonLogParseError:
            case E::CommonLoggerInitError:
            case E::OpenFileError:
            case E::ChunkQuotaError:
            case E::DeviceIoError:
            case E::Missing:
            case E::Timeout:
            case E::NodeDisconnected:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                    TStringBuilder() << "PDisk state is " << E::E_Name(stateEnum),
                                    ETags::PDiskState);
                break;
        }
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

        auto itPDisk = PDisksMap.find(pDiskId);
        if (itPDisk == PDisksMap.end()) { // this report, in theory, can't happen because there was pdisk mention in bsc vslot info. this pdisk info have to exists in bsc too
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide expected pdisk information", ETags::PDiskState);
            storagePDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        const auto& pDisk = itPDisk->second->GetInfo();

        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_path(pDisk.GetPath());

        // Prefer the explicit PDisk state if available, as it reflects the current status.
        // If not set, fall back to pDisk.GetStatusV2() for backward compatibility.
        if (pDisk.HasState()) {
            const auto& stateString = pDisk.GetState();
            const auto* descriptor = NKikimrBlobStorage::TPDiskState::E_descriptor();
            auto state = descriptor->FindValueByName(stateString);
            auto stateEnum = state ? static_cast<NKikimrBlobStorage::TPDiskState::E>(state->number()) : NKikimrBlobStorage::TPDiskState::Unknown;
            ReportPDiskState(stateEnum, context);
        } else {
            ui32 statusNumber = ParsePDiskStatusOrUnknown(pDisk.GetStatusV2());
            switch (statusNumber) {
                case NKikimrBlobStorage::ACTIVE:
                case NKikimrBlobStorage::INACTIVE: {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    break;
                }
                case NKikimrBlobStorage::FAULTY:
                case NKikimrBlobStorage::BROKEN:
                case NKikimrBlobStorage::TO_BE_REMOVED: {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                        TStringBuilder() << "PDisk state is " << pDisk.GetStatusV2(),
                                        ETags::PDiskState);
                    break;
                }
            }
        }

        if (pDisk.GetAvailableSize() != 0 && pDisk.GetTotalSize() != 0) { // do not replace it with Has()
            double avail = (double)pDisk.GetAvailableSize() / pDisk.GetTotalSize();
            if (avail < 0.06) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Available size is less than 6%", ETags::PDiskSpace);
            } else if (avail < 0.09) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Available size is less than 9%", ETags::PDiskSpace);
            } else if (avail < 0.12) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Available size is less than 12%", ETags::PDiskSpace);
            }
        }

        // do not propagate RED status to vdisk - so that vdisk is not considered down when computing group status
        context.OverallStatus = MinStatus(context.OverallStatus, Ydb::Monitoring::StatusFlag::ORANGE);
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

    void FillVDiskStatus(const NKikimrSysView::TVSlotEntry* vSlot, Ydb::Monitoring::StorageVDiskStatus& storageVDiskStatus, TSelfCheckContext context) {
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id()->Clear();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // you can see VDisks Group Id in vSlotId field

        TNodeId nodeId = vSlot->GetKey().GetNodeId();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_id(GetVDiskId(vSlot->GetInfo()));
        context.Location.mutable_storage()->mutable_node()->set_id(nodeId);

        auto itNodeInfo = MergedNodeInfo.find(nodeId);
        if (itNodeInfo != MergedNodeInfo.end()) {
            context.Location.mutable_storage()->mutable_node()->set_host(itNodeInfo->second->Host);
            context.Location.mutable_storage()->mutable_node()->set_port(itNodeInfo->second->Port);
        } else {
            context.Location.mutable_storage()->mutable_node()->clear_host();
            context.Location.mutable_storage()->mutable_node()->clear_port();
        }

        storageVDiskStatus.set_id(GetVSlotId(vSlot->GetKey()));

        const auto& vSlotInfo = vSlot->GetInfo();

        if (!vSlotInfo.HasStatusV2()) {
            // this should mean that BSC recently restarted and does not have accurate data yet - we should not report to avoid false positives
            context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        const auto *descriptor = NKikimrBlobStorage::EVDiskStatus_descriptor();
        auto status = descriptor->FindValueByName(vSlot->GetInfo().GetStatusV2());
        if (!status) { // this case is not expected because becouse bsc assignes status according EVDiskStatus enum
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide known status", ETags::VDiskState);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        if (vSlot->GetKey().HasPDiskId()) {
            TString pDiskId = GetPDiskId(vSlot->GetKey());
            FillPDiskStatus(pDiskId, *storageVDiskStatus.mutable_pdisk(), {&context, "PDISK"});
        }

        auto statusEnum = static_cast<NKikimrBlobStorage::EVDiskStatus>(status->number());
        if (statusEnum == NKikimrBlobStorage::ERROR) {
            // the disk is not operational at all
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "VDisk is not available", ETags::VDiskState,{ETags::PDiskState});
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        if (vSlotInfo.HasIsThrottling() && vSlotInfo.GetIsThrottling()) {
            // throttling is active
            auto message = TStringBuilder() << "VDisk is being throttled, rate "
                << vSlotInfo.GetThrottlingRate() << " per mille";
            context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, message, ETags::VDiskState);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        switch (statusEnum) {
            case NKikimrBlobStorage::ERROR: {
                // already handled above
                break;
            }
            case NKikimrBlobStorage::REPLICATING: { // the disk accepts queries, but not all the data was replicated
                context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, TStringBuilder() << "Replication in progress", ETags::VDiskState);
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
            }
            case NKikimrBlobStorage::INIT_PENDING:
            case NKikimrBlobStorage::READY: { // the disk is fully operational and does not affect group fault tolerance
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        }

        storageVDiskStatus.set_overall(context.GetOverallStatus());
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        TNodeId nodeId = ev.Get()->Cookie;
        auto& nodeVDiskState(NodeVDiskState[nodeId]);
        if (!nodeVDiskState.Set(std::move(ev))) {
            return;
        }
        RequestDone("TEvVDiskStateResponse");
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        TNodeId nodeId = ev.Get()->Cookie;
        auto& nodePDiskState(NodePDiskState[nodeId]);
        if (!nodePDiskState.Set(std::move(ev))) {
            return;
        }
        RequestDone("TEvPDiskStateResponse");
    }

    void Handle(TEvWhiteboard::TEvBSGroupStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        auto& nodeBSGroupState(NodeBSGroupState[nodeId]);
        if (!nodeBSGroupState.Set(std::move(ev))) {
            return;
        }
        RequestDone("TEvBSGroupStateResponse");
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
            ReportPDiskState(pDiskInfo.GetState(), context);

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
            case NKikimrWhiteboard::EVDiskState::Initial:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
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

    class TGroupChecker {
        TString ErasureSpecies;
        bool LayoutCorrect;
        int FailedDisks = 0;
        std::array<int, Ydb::Monitoring::StatusFlag::Status_ARRAYSIZE> DisksColors = {};
        TStackVec<std::pair<ui32, int>> FailedRealms;
        ETags Tag;

        void IncrementFor(ui32 realm) {
            auto itRealm = FindIf(FailedRealms, [realm](const std::pair<ui32, int>& p) -> bool {
                return p.first == realm;
            });
            if (itRealm == FailedRealms.end()) {
                itRealm = FailedRealms.insert(FailedRealms.end(), { realm, 1 });
            } else {
                itRealm->second++;
            }
        }

    public:
        TGroupChecker(const TString& erasure, const bool layoutCorrect = true, ETags tag = ETags::GroupState)
            : ErasureSpecies(erasure)
            , LayoutCorrect(layoutCorrect)
            , Tag(tag)
        {}

        void AddVDiskStatus(Ydb::Monitoring::StatusFlag::Status status, ui32 realm) {
            ++DisksColors[status];
            switch (status) {
                case Ydb::Monitoring::StatusFlag::BLUE: // disk is good, but not available
                // No yellow or orange status here - this is intentional - they are used when a disk is running out of space, but is currently available
                case Ydb::Monitoring::StatusFlag::RED: // disk is bad, probably not available
                case Ydb::Monitoring::StatusFlag::GREY: // the status is absent, the disk is not available
                    IncrementFor(realm);
                    ++FailedDisks;
                    break;
                default:
                    break;
            }
        }

        void ReportStatus(TSelfCheckContext& context) const {
            context.OverallStatus = Ydb::Monitoring::StatusFlag::GREEN;
            if (!LayoutCorrect) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group layout is incorrect", Tag);
            }
            if (ErasureSpecies == NONE) {
                if (FailedDisks > 0) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", Tag, {ETags::VDiskState});
                }
            } else if (ErasureSpecies == BLOCK_4_2) {
                if (FailedDisks > 2) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", Tag, {ETags::VDiskState});
                } else if (FailedDisks > 1) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", Tag, {ETags::VDiskState});
                } else if (FailedDisks > 0) {
                    if (DisksColors[Ydb::Monitoring::StatusFlag::BLUE] == FailedDisks) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", Tag, {ETags::VDiskState});
                    } else {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", Tag, {ETags::VDiskState});
                    }
                }
            } else if (ErasureSpecies == MIRROR_3_DC) {
                if (FailedRealms.size() > 2 || (FailedRealms.size() == 2 && FailedRealms[0].second > 1 && FailedRealms[1].second > 1)) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", Tag, {ETags::VDiskState});
                } else if (FailedRealms.size() == 2) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", Tag, {ETags::VDiskState});
                } else if (FailedDisks > 0) {
                    if (DisksColors[Ydb::Monitoring::StatusFlag::BLUE] == FailedDisks) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", Tag, {ETags::VDiskState});
                    } else {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", Tag, {ETags::VDiskState});
                    }
                }
            }
        }
    };

    void FillGroupStatusWithWhiteboard(TGroupId groupId, const NKikimrWhiteboard::TBSGroupStateInfo& groupInfo, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckContext context) {
        if (context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_id()->empty()) {
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->add_id();
        }
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->set_id(0, ToString(groupId));
        if (groupInfo.HasBridgePileId() && NodeWardenStorageConfig && NodeWardenStorageConfig->Get()->BridgeInfo) {
            const auto& pileId = TBridgePileId::FromProto(&groupInfo, &NKikimrWhiteboard::TBSGroupStateInfo::GetBridgePileId);
            const auto& pile = NodeWardenStorageConfig->Get()->BridgeInfo->GetPile(pileId)->Name;
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name(pile);
        }
        storageGroupStatus.set_id(ToString(groupId));
        TGroupChecker checker(groupInfo.erasurespecies());
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
            checker.AddVDiskStatus(vDiskStatus.overall(), protoVDiskId.ring());
        }

        context.Location.mutable_storage()->clear_node(); // group doesn't have node
        context.OverallStatus = MinStatus(context.OverallStatus, Ydb::Monitoring::StatusFlag::YELLOW);
        checker.ReportStatus(context);

        BLOG_D("Group " << groupId << " has status " << context.GetOverallStatus());
        storageGroupStatus.set_overall(context.GetOverallStatus());
    }

    static const int MERGING_IGNORE_SIZE = 4;

    struct TMergeIssuesContext {
        std::unordered_map<ETags, TList<TSelfCheckContext::TIssueRecord>> recordsMap;
        std::unordered_set<TString> removeIssuesIds;
        std::unordered_map<TString, TSelfCheckContext::TIssueRecord*> issueById;

        TMergeIssuesContext(TList<TSelfCheckContext::TIssueRecord>& records) {
            for (auto it = records.begin(); it != records.end(); ) {
                auto move = it++;
                issueById.emplace(move->IssueLog.id(), &(*move));
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
            for (auto it = records.rbegin(); it != records.rend(); it++) {
                auto message = it->IssueLog.message();
                if (it->IssueLog.count() > 0) {
                    switch (it->Tag) {
                        case ETags::BridgeGroupState:
                        case ETags::GroupState: {
                            message = std::regex_replace(message.c_str(), std::regex("^Group has "), "Groups have ");
                            message = std::regex_replace(message.c_str(), std::regex("^Group is "), "Groups are ");
                            message = std::regex_replace(message.c_str(), std::regex("^Group "), "Groups ");
                            break;
                        }
                        case ETags::VDiskState: {
                            message = std::regex_replace(message.c_str(), std::regex("^VDisk has "), "VDisks have ");
                            message = std::regex_replace(message.c_str(), std::regex("^VDisk is "), "VDisks are ");
                            message = std::regex_replace(message.c_str(), std::regex("^VDisk "), "VDisks ");
                            break;
                        }
                        case ETags::PDiskState: {
                            message = std::regex_replace(message.c_str(), std::regex("^PDisk has "), "PDisks have ");
                            message = std::regex_replace(message.c_str(), std::regex("^PDisk is "), "PDisks are ");
                            message = std::regex_replace(message.c_str(), std::regex("^PDisk "), "PDisks ");
                            break;
                        }
                        case ETags::StateStorageRing: {
                            message = std::regex_replace(message.c_str(), std::regex("^Ring has "), "Rings have ");
                            message = std::regex_replace(message.c_str(), std::regex("^Ring is "), "Rings are ");
                            message = std::regex_replace(message.c_str(), std::regex("^Ring "), "Rings ");
                            break;
                        }
                        case ETags::StateStorageNode: {
                            message = std::regex_replace(message.c_str(), std::regex("^Ring has "), "Rings have ");
                            message = std::regex_replace(message.c_str(), std::regex("^Ring is "), "Rings are ");
                            message = std::regex_replace(message.c_str(), std::regex("^Ring "), "Rings ");
                            break;
                        }
                        default:
                            break;
                    }
                }
                if (IsBridgeMode(TActivationContext::AsActorContext()) && it->Tag == ETags::GroupState) {
                    if (it->IssueLog.reason_size() == 1) {
                        const auto& reason = issueById[it->IssueLog.reason(0)]->IssueLog;
                        if (message.find("in pile") == std::string::npos) {
                            message = TStringBuilder() << reason.message() <<  " in pile " << reason.location().storage().pool().group().pile().name();
                        }
                        it->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->CopyFrom(reason.location().storage().pool().group().pile());
                    }
                }
                if (IsBridgeMode(TActivationContext::AsActorContext()) && (it->Tag == ETags::PoolState || it->Tag == ETags::StorageState)) {
                    auto reasonPiles = it->IssueLog.reason() | std::views::transform([this](auto&& reason) {return issueById[reason]->IssueLog.location().storage().pool().group().pile().name();});
                    std::unordered_set<TString> piles(reasonPiles.begin(), reasonPiles.end());;
                    if (piles.size() == 1 && !piles.begin()->empty()) {
                        TString pile = *piles.begin();
                        if (message.find("in pile") == std::string::npos) {
                            message = TStringBuilder() << message << " in pile " << pile;
                        }
                        it->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name(pile);
                    }
                }
                it->IssueLog.set_message(message);
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
                    && it->IssueLog.level() == similar.begin()->IssueLog.level();
                if (isSimilar && similar.begin()->Tag == ETags::VDiskState) {
                    isSimilar = it->IssueLog.location().storage().node().id() == similar.begin()->IssueLog.location().storage().node().id();
                }
                if (isSimilar && similar.begin()->IssueLog.location().storage().pool().group().has_pile()) {
                    isSimilar = it->IssueLog.location().storage().pool().group().pile().name()
                        == similar.begin()->IssueLog.location().storage().pool().group().pile().name();
                }
                if (isSimilar && similar.begin()->IssueLog.location().compute().state_storage().has_pile()) {
                    isSimilar = it->IssueLog.location().compute().state_storage().pile().name()
                        == similar.begin()->IssueLog.location().compute().state_storage().pile().name();
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
                case ETags::BridgeGroupState:
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

    void CheckGroupVSlots(TGroupChecker& checker, std::ranges::range auto&& slots, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckContext& context) {
        for (const auto* slot : slots) {
            const auto& slotInfo = slot->GetInfo();
            auto slotId = GetVSlotId(slot->GetKey());
            auto [statusIt, inserted] = VDiskStatuses.emplace(slotId, Ydb::Monitoring::StatusFlag::UNSPECIFIED);
            if (inserted) {
                Ydb::Monitoring::StorageVDiskStatus& vDiskStatus = *storageGroupStatus.add_vdisks();
                FillVDiskStatus(slot, vDiskStatus, {&context, "VDISK"});
                statusIt->second = vDiskStatus.overall();
            }
            checker.AddVDiskStatus(statusIt->second, slotInfo.GetFailRealm());
        }
        context.OverallStatus = MinStatus(context.OverallStatus, Ydb::Monitoring::StatusFlag::YELLOW);
    }

    void FillGroupStatus(TGroupId groupId, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckContext context) {
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_id()->Clear();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->add_id(ToString(groupId));
        storageGroupStatus.set_id(ToString(groupId));

        auto itGroup = GroupState.find(groupId);
        if (itGroup == GroupState.end()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "Group has no vslots", ETags::GroupState);
            storageGroupStatus.set_overall(context.GetOverallStatus());
            return;
        }

        if (itGroup->second.BridgeGroups.empty()) {
            TGroupChecker checker(itGroup->second.ErasureSpecies, itGroup->second.LayoutCorrect);
            const auto& slots = itGroup->second.VSlots;
            CheckGroupVSlots(checker, slots, storageGroupStatus, context);
            checker.ReportStatus(context);
        } else {
            auto getStatus = [&](const TGroupState* group) {
                TGroupChecker checker(group->ErasureSpecies, group->LayoutCorrect, ETags::BridgeGroupState);
                TSelfCheckContext pileContext(&context, "BRIDGE_GROUP");
                if (group->BridgePileId.GetPileIndex() < AppData()->BridgeConfig.PilesSize()) {
                    const auto& pileName = AppData()->BridgeConfig.GetPiles(group->BridgePileId.GetPileIndex()).GetName();
                    pileContext.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name(pileName);
                } else { // this fallback should not ever happen - but is used in tests
                    pileContext.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name(group->BridgePileId.ToString());
                }
                pileContext.Location.mutable_storage()->mutable_pool()->mutable_group()->set_id(0, ToString(group->Id));
                CheckGroupVSlots(checker, group->VSlots, storageGroupStatus, pileContext);
                checker.ReportStatus(pileContext);
                return pileContext.GetOverallStatus();
            };
            auto isSyncPile = [&](const TGroupState* group) {
                if (!group->BridgePileId) {
                    return true;
                }
                return NodeWardenStorageConfig->Get()->BridgeInfo->GetPile(group->BridgePileId)->State == NKikimrBridge::TClusterState::SYNCHRONIZED;
            };
            auto [minStatus, maxStatus] = std::ranges::minmax(itGroup->second.BridgeGroups | std::views::filter(isSyncPile) | std::views::transform(getStatus));
            Ydb::Monitoring::StatusFlag::Status status = maxStatus;
            if (maxStatus == Ydb::Monitoring::StatusFlag::RED) { // only report group as dead if it is dead in all piles
                status = MaxStatus(minStatus, Ydb::Monitoring::StatusFlag::ORANGE);
            }
            context.OverallStatus = status;
            if (status != Ydb::Monitoring::StatusFlag::GREEN) {
                TStringBuilder issue;
                switch (maxStatus) {
                    default:
                    case Ydb::Monitoring::StatusFlag::BLUE:
                        issue << "Group has issues";
                        break;
                    case Ydb::Monitoring::StatusFlag::YELLOW:
                        issue << "Group degraded";
                        break;
                    case Ydb::Monitoring::StatusFlag::ORANGE:
                        issue << "Group has no redundancy";
                        break;
                    case Ydb::Monitoring::StatusFlag::RED:
                        issue << "Group dead";
                        break;
                }
                if (minStatus == maxStatus) {
                    issue << " in all piles";
                } else {
                    issue << " in some piles";
                }
                context.ReportStatus(status, issue, ETags::GroupState, {ETags::BridgeGroupState});
            }
        }

        context.Location.mutable_storage()->clear_node(); // group doesn't have node;

        storageGroupStatus.set_overall(context.GetOverallStatus());
    }

    void MergeRecords(TList<TSelfCheckContext::TIssueRecord>& records) {
        TMergeIssuesContext mergeContext(records);
        if (Request->Request.merge_records()) {
            MergeLevelRecords(mergeContext, ETags::GroupState);
            MergeLevelRecords(mergeContext, ETags::BridgeGroupState, ETags::GroupState);
            MergeLevelRecords(mergeContext, ETags::VDiskState, ETags::BridgeGroupState);
            MergeLevelRecords(mergeContext, ETags::VDiskState, ETags::GroupState);
            MergeLevelRecords(mergeContext, ETags::PDiskState, ETags::VDiskState);
            MergeLevelRecords(mergeContext, ETags::StateStorageRing);
            MergeLevelRecords(mergeContext, ETags::StateStorageNode, ETags::StateStorageRing);
        }
        mergeContext.FillRecords(records);
    }

    void FillPoolStatus(const TStoragePoolState& pool, Ydb::Monitoring::StoragePoolStatus& storagePoolStatus, TSelfCheckContext context) {
        context.Location.mutable_storage()->mutable_pool()->set_name(pool.Name);
        storagePoolStatus.set_id(pool.Name);
        bool haveInfo = HaveAllBSControllerInfo();
        for (auto groupId : pool.Groups) {
            if (haveInfo && !UnknownStaticGroups.contains(groupId)) {
                FillGroupStatus(groupId, *storagePoolStatus.add_groups(), {&context, "STORAGE_GROUP"});
            } else if (IsStaticGroup(groupId)) {
                auto itGroup = MergedBSGroupState.find(groupId);
                if (itGroup != MergedBSGroupState.end()) {
                    FillGroupStatusWithWhiteboard(groupId, *itGroup->second, *storagePoolStatus.add_groups(), {&context, "STORAGE_GROUP"});
                }
            }
        }

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
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
        }
        storagePoolStatus.set_overall(context.GetOverallStatus());
    }

    void FillStorage(TDatabaseState& databaseState, Ydb::Monitoring::StorageStatus& storageStatus, TSelfCheckContext context) {
        if (HaveAllBSControllerInfo() && databaseState.StoragePools.empty()) {
            if (TabletRequests.TabletStates[databaseState.SchemeShardId].IsUnresponsive) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREY, "Could not get data on storage", ETags::StorageState);
            } else {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There are no storage pools", ETags::StorageState);
            }
        } else {
            if (HaveAllBSControllerInfo()) {
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
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    break;
            }
            if (!HaveAllBSControllerInfo()) {
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
        MergeRecords(context.IssueRecords);
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
        if (!IsSpecificDatabaseFilter()) {
            dbContext.Location.mutable_database()->set_name(path);
        }
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

    void FillStateStorage(TOverallStateContext& context, TString type, TIntrusiveConstPtr<TStateStorageInfo> info) {
        TSelfCheckResult ssContext;
        ssContext.Type = type;
        for (const auto& ringGroup : info->RingGroups) {
            if (ringGroup.State != ERingGroupState::PRIMARY && ringGroup.State != ERingGroupState::SYNCHRONIZED) {
                continue;
            }
            TSelfCheckResult* currentContext = &ssContext;
            TSelfCheckContext pileContext(&ssContext, TStringBuilder() << "PILE_" << type);
            if ((bool)ringGroup.BridgePileId && NodeWardenStorageConfig && NodeWardenStorageConfig->IsOk()) {
                const auto& pileName = NodeWardenStorageConfig->Get()->BridgeInfo->GetPile(ringGroup.BridgePileId)->Name;
                pileContext.Location.mutable_compute()->mutable_state_storage()->mutable_pile()->set_name(pileName);
                currentContext = &pileContext;
            }
            ui32 disabledRings = 0;
            ui32 badRings = 0;
            for (size_t ringIdx = 0; ringIdx < ringGroup.Rings.size(); ++ringIdx) {
                const auto& ring = ringGroup.Rings[ringIdx];
                TSelfCheckContext ringContext(currentContext, TStringBuilder() << type << "_RING");
                ringContext.Location.mutable_compute()->mutable_state_storage()->set_ring(ringIdx + 1);
                if (ring.IsDisabled) {
                    ++disabledRings;
                    continue;
                }
                for (const auto& replica : ring.Replicas) {
                    const auto node = replica.NodeId();
                    if (!NodeSystemState[node].IsOk()) {
                        TSelfCheckContext nodeContext(&ringContext, TStringBuilder() << type << "_NODE");
                        nodeContext.Location.mutable_compute()->mutable_state_storage()->mutable_node()->set_id(node);
                        nodeContext.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Node is not available", ETags::StateStorageNode);
                    }
                }
                ringContext.ReportWithMaxChildStatus("Ring has unavailable nodes", ETags::StateStorageRing, {ETags::StateStorageNode});
                if (ringContext.GetOverallStatus() == Ydb::Monitoring::StatusFlag::RED) {
                    ++badRings;
                }
            }
            if (disabledRings + badRings > (ringGroup.NToSelect - 1) / 2) {
                currentContext->ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There is not enough functional rings", ETags::StateStorage);
            } else if (badRings > 1) {
                currentContext->ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Multiple rings have unavailable replicas", ETags::StateStorage);
            } else if (badRings > 0) {
                currentContext->ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "One ring has unavailable replicas", ETags::StateStorage);
            }
        }
        MergeRecords(ssContext.IssueRecords);
        context.UpdateMaxStatus(ssContext.GetOverallStatus());
        context.AddIssues(ssContext.IssueRecords);
    }

    void FillResult(TOverallStateContext context) {
        if (IsSpecificDatabaseFilter()) {
            FillDatabaseResult(context, FilterDatabase, DatabaseState[FilterDatabase]);
        } else {
            for (auto& [path, state] : DatabaseState) {
                FillDatabaseResult(context, path, state);
            }
            if (StateStorageInfo && StateStorageInfo->IsOk()) {
                FillStateStorage(context, "STATE_STORAGE", StateStorageInfo->Get()->Info);
            }
            if (SchemeBoardInfo && SchemeBoardInfo->IsOk()) {
                FillStateStorage(context, "SCHEME_BOARD", SchemeBoardInfo->Get()->Info);
            }
            if (BoardInfo && BoardInfo->IsOk()) {
                FillStateStorage(context, "BOARD", BoardInfo->Get()->Info);
            }
        }
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
            if (HaveAllBSControllerInfo()) {
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
        Span.Event("ReplyAndPassAway");
        THolder<TEvSelfCheckResult> response = MakeHolder<TEvSelfCheckResult>();
        Ydb::Monitoring::SelfCheckResult& result = response->Result;

        AggregateHiveInfo();
        AggregateHiveNodeStats();
        AggregateStoragePools();
        AggregateWhiteboard();

        for (auto& [requestId, request] : TabletRequests.RequestsInFlight) {
            auto tabletId = request.TabletId;
            TabletRequests.TabletStates[tabletId].IsUnresponsive = true;
        }

        FillResult({&result});
        RemoveUnrequestedEntries(result, Request->Request);

        return ReplyAndPassAway(std::move(response));
    }

    void ReplyAndPassAway(THolder<TEvSelfCheckResult> response) {
        Ydb::Monitoring::SelfCheckResult& result = response->Result;

        FillNodeInfo(SelfId().NodeId(), result.mutable_location());
        if (NodeWardenStorageConfig && NodeWardenStorageConfig->Get()->BridgeInfo) {
            const auto& selfPileName = NodeWardenStorageConfig->Get()->BridgeInfo->SelfNodePile->Name;
            result.mutable_location()->mutable_pile()->set_name(selfPileName);
        }

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

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
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
        if (config.Locator.empty()) {
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
        meta.Timeout = Timeout ? NYdb::TDeadline::SafeDurationCast(Timeout) : NYdb::TDeadline::Duration::max();
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
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
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
    TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(TBase::SelfId().NodeId());
    TBase::Send(whiteboardServiceId, new TEvWhiteboard::TEvSystemStateRequest());
    const auto& params(Request->Request.GetParams());
    Timeout = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("timeout"), Timeout.MilliSeconds()));
    TBase::Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
}

class THealthCheckService : public TActorBootstrapped<THealthCheckService> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MONITORING_SERVICE; }
    NKikimrConfig::THealthCheckConfig HealthCheckConfig;

    THealthCheckService()
    {
    }

    void Bootstrap() {
        HealthCheckConfig.CopyFrom(AppData()->HealthCheckConfig);
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({NKikimrConsole::TConfigItem::HealthCheckConfigItem}));
        TMon* mon = AppData()->Mon;
        if (mon) {
            mon->RegisterActorPage({
                .RelPath = "status",
                .ActorSystem = TActivationContext::ActorSystem(),
                .ActorId = SelfId(),
                .AuthMode = TMon::EAuthMode::Disabled,
            });
        }
        Become(&THealthCheckService::StateWork);
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetConfig().HasHealthCheckConfig()) {
            HealthCheckConfig.CopyFrom(record.GetConfig().GetHealthCheckConfig());
        }
        Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
    }

    void Handle(TEvSelfCheckRequest::TPtr& ev) {
        Register(new TSelfCheckRequest(ev->Sender, ev.Get()->Release(), ev->Cookie, std::move(ev->TraceId), HealthCheckConfig));
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
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};

IActor* CreateHealthCheckService() {
    return new THealthCheckService();
}

}
