#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include "viewer.h"
#include "viewer_helper.h"
#include "viewer_tabletinfo.h"
#include "wb_group.h"
#include <library/cpp/protobuf/json/proto2json.h>

template<>
struct std::hash<NKikimr::TSubDomainKey> {
    std::size_t operator ()(const NKikimr::TSubDomainKey& s) const {
        return s.Hash();
    }
};

namespace NKikimr::NViewer {

using namespace NProtobufJson;
using namespace NActors;
using namespace NNodeWhiteboard;

enum class ENodeFields : ui8 {
    NodeInfo,
    SystemState,
    PDisks,
    VDisks,
    Tablets,
    NodeId,
    HostName,
    DC,
    Rack,
    Version,
    Uptime,
    Memory,
    CPU,
    LoadAverage,
    Missing,
    DiskSpaceUsage,
    SubDomainKey,
    DisconnectTime,
    Database,
    COUNT
};

constexpr ui8 operator +(ENodeFields e) {
    return static_cast<ui8>(e);
}

class TJsonNodes : public TViewerPipeClient {
    using TThis = TJsonNodes;
    using TBase = TViewerPipeClient;
    using TNodeId = ui32;
    using TPDiskId = std::pair<TNodeId, ui32>;
    using TFieldsType = std::bitset<+ENodeFields::COUNT>;

    enum ENavigateRequest {
        ENavigateRequestDatabase,
        ENavigateRequestResource,
        ENavigateRequestPath,
    };

    enum EBoardInfoRequest {
        EBoardInfoRequestDatabase,
        EBoardInfoRequestResource,
    };

    std::optional<TRequestResponse<TEvInterconnect::TEvNodesInfo>> NodesInfoResponse;
    std::optional<TRequestResponse<TEvWhiteboard::TEvNodeStateResponse>> NodeStateResponse;
    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> DatabaseNavigateResponse;
    std::optional<TRequestResponse<TEvStateStorage::TEvBoardInfo>> DatabaseBoardInfoResponse;
    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> ResourceNavigateResponse;
    std::optional<TRequestResponse<TEvStateStorage::TEvBoardInfo>> ResourceBoardInfoResponse;
    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> PathNavigateResponse;
    std::unordered_map<TTabletId, TRequestResponse<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStats;

    std::vector<TTabletId> HivesToAsk;
    bool AskHiveAboutPaths = false;

    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse>> StoragePoolsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse>> GroupsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse>> VSlotsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse>> PDisksResponse;

    int WhiteboardStateRequestsInFlight = 0;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvSystemStateResponse>> SystemStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvVDiskStateResponse>> VDiskStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvPDiskStateResponse>> PDiskStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvTabletStateResponse>> TabletStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> SystemViewerResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> TabletViewerResponse;

    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    enum ETimeoutTag : ui64 {
        NoTimeout,
        TimeoutTablets,
        TimeoutFinal,
    };

    ETimeoutTag CurrentTimeoutState = NoTimeout;

    TString Database;
    TString SharedDatabase;
    bool Direct = false;
    bool FilterDatabase = false;
    bool HasDatabaseNodes = false;
    TPathId FilterPathId;
    TSubDomainKey SubDomainKey;
    TSubDomainKey SharedSubDomainKey;
    bool FilterSubDomainKey = false;
    TString FilterPath;
    TString FilterStoragePool;
    std::pair<ui64, ui64> FilterStoragePoolId;
    std::unordered_set<TNodeId> FilterNodeIds;
    std::unordered_set<ui32> FilterGroupIds;
    std::optional<std::size_t> Offset;
    std::optional<std::size_t> Limit;
    ui32 UptimeSeconds = 0;
    bool ProblemNodesOnly = false;
    TString Filter;
    bool AllWhiteboardFields = false;

    enum class EWith {
        Everything,
        MissingDisks,
        SpaceProblems,
    };
    EWith With = EWith::Everything;

    enum class EType {
        Any,
        Static,
        Dynamic,
    };
    EType Type = EType::Any;

    enum class EFilterStorageStage {
        None,
        Pools,
        Groups,
        VSlots,
    };

    EFilterStorageStage FilterStorageStage = EFilterStorageStage::None;
    TNodeId MinAllowedNodeId = std::numeric_limits<TNodeId>::min();
    TNodeId MaxAllowedNodeId = std::numeric_limits<TNodeId>::max();
    std::optional<std::size_t> MaximumDisksPerNode;
    std::optional<std::size_t> MaximumSlotsPerDisk;
    ui32 SpaceUsageProblem = 90; // %
    bool OffloadMerge = true;
    size_t OffloadMergeAttempts = 2;

    struct TNode {
        TEvInterconnect::TNodeInfo NodeInfo;
        NKikimrWhiteboard::TSystemStateInfo SystemState;
        std::vector<NKikimrWhiteboard::TPDiskStateInfo> PDisks;
        std::vector<NKikimrSysView::TPDiskEntry> SysViewPDisks;
        std::vector<NKikimrWhiteboard::TVDiskStateInfo> VDisks;
        std::vector<NKikimrSysView::TVSlotEntry> SysViewVDisks;
        std::vector<NKikimrViewer::TTabletStateInfo> Tablets;
        TSubDomainKey SubDomainKey;
        TString Database;
        ui32 MissingDisks = 0;
        float DiskSpaceUsage = 0; // the highest
        bool Problems = false;
        bool Connected = false;
        bool Disconnected = false;
        bool HasDisks = false;
        bool GotDatabaseFromDatabaseBoardInfo = false;
        bool GotDatabaseFromResourceBoardInfo = false;

        TNodeId GetNodeId() const {
            return NodeInfo.NodeId;
        }

        TString GetHostName() const {
            if (NodeInfo.Host) {
                return NodeInfo.Host;
            }
            if (SystemState.GetHost()) {
                return SystemState.GetHost();
            }
            if (NodeInfo.ResolveHost) {
                return NodeInfo.ResolveHost;
            }
            return {};
        }

        TString GetDataCenter() const {
            if (NodeInfo.Location.GetDataCenterId()) {
                return NodeInfo.Location.GetDataCenterId();
            }
            return SystemState.GetLocation().GetDataCenter();
        }

        TString GetRack() const {
            if (NodeInfo.Location.GetRackId()) {
                return NodeInfo.Location.GetRackId();
            }
            return SystemState.GetLocation().GetRack();
        }

        void Cleanup() {
            if (SystemState.HasSystemLocation()) {
                SystemState.ClearSystemLocation();
            }
            if (SystemState.HasLocation()) {
                if (SystemState.GetLocation().GetDataCenter().empty()) {
                    SystemState.MutableLocation()->ClearDataCenter();
                }
                if (SystemState.GetLocation().GetRack().empty()) {
                    SystemState.MutableLocation()->ClearRack();
                }
                if (SystemState.GetLocation().GetUnit().empty() || SystemState.GetLocation().GetUnit() == "0") {
                    SystemState.MutableLocation()->ClearUnit();
                }
            }
        }

        void CalcDatabase() {
            if (SystemState.TenantsSize() == 1) {
                Database = SystemState.GetTenants(0);
            }
        }

        void CalcDisks() {
            MissingDisks = 0;
            DiskSpaceUsage = 0;
            if (!PDisks.empty()) {
                for (const auto& pdisk : PDisks) {
                    float diskSpaceUsage = pdisk.GetTotalSize() ? 100.0 * (pdisk.GetTotalSize() - pdisk.GetAvailableSize()) / pdisk.GetTotalSize() : 0;
                    DiskSpaceUsage = std::max(DiskSpaceUsage, diskSpaceUsage);
                    if (pdisk.state() == NKikimrBlobStorage::TPDiskState::Normal) {
                        continue;
                    }
                    ++MissingDisks;
                }
            } else {
                for (const auto& entry : SysViewPDisks) {
                    const auto& pdisk(entry.GetInfo());
                    float diskSpaceUsage = pdisk.GetTotalSize() ? 100.0 * (pdisk.GetTotalSize() - pdisk.GetAvailableSize()) / pdisk.GetTotalSize() : 0;
                    DiskSpaceUsage = std::max(DiskSpaceUsage, diskSpaceUsage);
                    NKikimrBlobStorage::EDriveStatus driveStatus = NKikimrBlobStorage::EDriveStatus::UNKNOWN;
                    if (NKikimrBlobStorage::EDriveStatus_Parse(pdisk.GetStatusV2(), &driveStatus)) {
                        switch (driveStatus) {
                            case NKikimrBlobStorage::EDriveStatus::ACTIVE:
                            case NKikimrBlobStorage::EDriveStatus::INACTIVE:
                                continue;
                            default:
                                ++MissingDisks;
                                break;
                        }
                    }
                }
            }
        }

        void DisconnectNode() {
            Problems = true;
            Disconnected = true;
            if (!SystemState.HasDisconnectTime()) {
                TInstant disconnectTime;
                for (const auto& entry : SysViewPDisks) {
                    const auto& pdisk(entry.GetInfo());
                    disconnectTime = std::max(disconnectTime, TInstant::MicroSeconds(pdisk.GetStatusChangeTimestamp()));
                }
                if (disconnectTime) {
                    SystemState.SetDisconnectTime(disconnectTime.Seconds());
                }
            }
        }

        void RemapDisks() {
            if (PDisks.empty() && !SysViewPDisks.empty()) {
                for (const auto& entry : SysViewPDisks) {
                    const auto& pdisk(entry.GetInfo());
                    auto& pDiskState = PDisks.emplace_back();
                    NKikimrBlobStorage::EDriveStatus driveStatus = NKikimrBlobStorage::EDriveStatus::UNKNOWN;
                    if (NKikimrBlobStorage::EDriveStatus_Parse(pdisk.GetStatusV2(), &driveStatus)) {
                        switch (driveStatus) {
                            case NKikimrBlobStorage::EDriveStatus::ACTIVE:
                            case NKikimrBlobStorage::EDriveStatus::INACTIVE:
                                pDiskState.SetState(NKikimrBlobStorage::TPDiskState::Normal);
                                break;
                            default:
                                break;
                        }
                    }

                    pDiskState.SetPDiskId(entry.GetKey().GetPDiskId());
                    pDiskState.SetNodeId(entry.GetKey().GetNodeId());
                    pDiskState.SetPath(pdisk.GetPath());
                    pDiskState.SetGuid(pdisk.GetGuid());
                    pDiskState.SetTotalSize(pdisk.GetTotalSize());
                    pDiskState.SetAvailableSize(pdisk.GetAvailableSize());
                    pDiskState.SetExpectedSlotCount(pdisk.GetExpectedSlotCount());
                }
            }
            if (VDisks.empty() && !SysViewVDisks.empty()) {
                for (const auto& entry : SysViewVDisks) {
                    const auto& vdisk(entry.GetInfo());
                    auto& vDiskState = VDisks.emplace_back();
                    vDiskState.MutableVDiskId()->SetGroupID(vdisk.GetGroupId());
                    vDiskState.MutableVDiskId()->SetGroupGeneration(vdisk.GetGroupGeneration());
                    vDiskState.MutableVDiskId()->SetRing(vdisk.GetFailRealm());
                    vDiskState.MutableVDiskId()->SetDomain(vdisk.GetFailDomain());
                    vDiskState.MutableVDiskId()->SetVDisk(vdisk.GetVDisk());
                    vDiskState.SetNodeId(entry.GetKey().GetNodeId());
                    vDiskState.SetPDiskId(entry.GetKey().GetPDiskId());
                    vDiskState.SetAllocatedSize(vdisk.GetAllocatedSize());
                    vDiskState.SetAvailableSize(vdisk.GetAvailableSize());
                    vDiskState.SetVDiskSlotId(entry.GetKey().GetVSlotId());
                    NKikimrBlobStorage::EVDiskStatus vDiskStatus;
                    if (NKikimrBlobStorage::EVDiskStatus_Parse(vdisk.GetStatusV2(), &vDiskStatus)) {
                        switch(vDiskStatus) {
                            case NKikimrBlobStorage::EVDiskStatus::ERROR:
                                vDiskState.SetVDiskState(NKikimrWhiteboard::EVDiskState::LocalRecoveryError);
                                break;
                            case NKikimrBlobStorage::EVDiskStatus::INIT_PENDING:
                                vDiskState.SetVDiskState(NKikimrWhiteboard::EVDiskState::Initial);
                                break;
                            case NKikimrBlobStorage::EVDiskStatus::REPLICATING:
                                vDiskState.SetVDiskState(NKikimrWhiteboard::EVDiskState::OK);
                                vDiskState.SetReplicated(false);
                                break;
                            case NKikimrBlobStorage::EVDiskStatus::READY:
                                vDiskState.SetVDiskState(NKikimrWhiteboard::EVDiskState::OK);
                                break;
                        }
                    }
                }
            }
        }

        bool IsStatic() const {
            return NodeInfo.IsStatic;
        }

        NKikimrWhiteboard::EFlag GetOverall() const {
            return SystemState.GetSystemState();
        }

        int GetCandidateScore() const {
            int score = 0;
            if (Connected) {
                score += 100;
            }
            if (IsStatic()) {
                score += 10;
            }
            return score;
        }

        TString GetDiskUsageForGroup() const {
            //return TStringBuilder() << std::ceil(std::clamp<float>(DiskSpaceUsage, 0, 100) / 5) * 5 << '%';
            // we want 0%-95% groups instead of 5%-100% groups
            return TStringBuilder() << std::floor(std::clamp<float>(DiskSpaceUsage, 0, 100) / 5) * 5 << '%';
        }

        TString GetUptimeForGroup(TInstant now) const {
            if (!Disconnected) {
                auto uptime = static_cast<int>(now.Seconds()) - SystemState.GetStartTime();
                if (uptime < 60 * 10) {
                    return "uptime < 10m";
                }
                if (uptime < 60 * 60) {
                    return "uptime < 1h";
                }
                if (uptime < 60 * 60 * 24) {
                    return "uptime < 24h";
                }
                if (uptime < 60 * 60 * 24 * 7) {
                    return "uptime < 1 week";
                }
                return "uptime > 1 week";
            } else {
                if (SystemState.HasDisconnectTime()) {
                    auto downtime = static_cast<int>(now.Seconds()) - SystemState.GetDisconnectTime();
                    if (downtime < 60 * 10) {
                        return "downtime < 10m";
                    }
                    if (downtime < 60 * 60) {
                        return "downtime < 1h";
                    }
                    if (downtime < 60 * 60 * 24) {
                        return "downtime < 24h";
                    }
                    if (downtime < 60 * 60 * 24 * 7) {
                        return "downtime < 1 week";
                    }
                    return "downtime > 1 week";
                } else {
                    return "disconnected";
                }
            }
        }

        TString GetVersionForGroup() const {
            if (SystemState.HasVersion()) {
                return SystemState.GetVersion();
            } else {
                return "unknown";
            }
        }

        bool HasDatabase(const TString& database) const {
            return Database == database;
        }

        bool HasSubDomainKey(const TSubDomainKey& subDomainKey) const {
            return SubDomainKey == subDomainKey;
        }
    };

    struct TNodeGroup {
        TString Name;
        std::vector<TNode*> Nodes;
    };

    struct TNodeBatch {
        std::vector<TNode*> NodesToAskFor;
        std::vector<TNode*> NodesToAskAbout;
        size_t Offset = 0;
        bool HasStaticNodes = false;

        TNodeId ChooseNodeId() {
            if (Offset >= NodesToAskFor.size()) {
                return 0;
            }
            return NodesToAskFor[Offset++]->GetNodeId();
        }
    };

    using TNodeData = std::vector<TNode>;
    using TNodeView = std::deque<TNode*>;

    TNodeData NodeData;
    TNodeView NodeView;
    std::vector<TNodeGroup> NodeGroups;
    std::unordered_map<TNodeId, TNode*> NodesByNodeId;
    std::unordered_map<TNodeId, TNodeBatch> NodeBatches;

    TFieldsType FieldsRequired;
    TFieldsType FieldsAvailable;
    const TFieldsType FieldsAll = TFieldsType().set();
    const TFieldsType FieldsNodeInfo = TFieldsType().set(+ENodeFields::NodeInfo)
                                                    .set(+ENodeFields::NodeId)
                                                    .set(+ENodeFields::HostName)
                                                    .set(+ENodeFields::DC)
                                                    .set(+ENodeFields::Rack);
    const TFieldsType FieldsSystemState = TFieldsType().set(+ENodeFields::SystemState)
                                                       .set(+ENodeFields::Database)
                                                       .set(+ENodeFields::Version)
                                                       .set(+ENodeFields::Uptime)
                                                       .set(+ENodeFields::Memory)
                                                       .set(+ENodeFields::CPU)
                                                       .set(+ENodeFields::LoadAverage);
    const TFieldsType FieldsPDisks = TFieldsType().set(+ENodeFields::PDisks)
                                                  .set(+ENodeFields::Missing)
                                                  .set(+ENodeFields::DiskSpaceUsage);
    const TFieldsType FieldsVDisks = TFieldsType().set(+ENodeFields::VDisks);
    const TFieldsType FieldsTablets = TFieldsType().set(+ENodeFields::Tablets);
    const TFieldsType FieldsHiveNodeStat = TFieldsType().set(+ENodeFields::SubDomainKey)
                                                        .set(+ENodeFields::DisconnectTime);

    const std::unordered_map<ENodeFields, TFieldsType> DependentFields = {
        { ENodeFields::DC, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Rack, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Uptime, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Version, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Missing, TFieldsType().set(+ENodeFields::PDisks) },
    };

    bool FieldsNeeded(TFieldsType fields) const {
        return (FieldsRequired & (fields & ~FieldsAvailable)).any();
    }

    ENodeFields SortBy = ENodeFields::NodeId;
    bool ReverseSort = false;
    ENodeFields GroupBy = ENodeFields::NodeId;
    bool NeedFilter = false;
    bool NeedGroup = false;
    bool NeedSort = false;
    bool NeedLimit = false;
    ui64 TotalNodes = 0;
    ui64 FoundNodes = 0;
    bool NoRack = false;
    bool NoDC = false;
    std::vector<TString> Problems;

    void AddProblem(const TString& problem) {
        for (const auto& p : Problems) {
            if (p == problem) {
                return;
            }
        }
        Problems.push_back(problem);
    }

    static ENodeFields ParseENodeFields(TStringBuf field) {
        ENodeFields result = ENodeFields::COUNT;
        if (field == "NodeId" || field == "Id") {
            result = ENodeFields::NodeId;
        } else if (field == "Host") {
            result = ENodeFields::HostName;
        } else if (field == "DC") {
            result = ENodeFields::DC;
        } else if (field == "Rack") {
            result = ENodeFields::Rack;
        } else if (field == "Version") {
            result = ENodeFields::Version;
        } else if (field == "Uptime") {
            result = ENodeFields::Uptime;
        } else if (field == "Memory") {
            result = ENodeFields::Memory;
        } else if (field == "CPU") {
            result = ENodeFields::CPU;
        } else if (field == "LoadAverage") {
            result = ENodeFields::LoadAverage;
        } else if (field == "Missing") {
            result = ENodeFields::Missing;
        } else if (field == "DiskSpaceUsage") {
            result = ENodeFields::DiskSpaceUsage;
        } else if (field == "DisconnectTime") {
            result = ENodeFields::DisconnectTime;
        } else if (field == "Database") {
            result = ENodeFields::Database;
        } else if (field == "SubDomainKey") {
            result = ENodeFields::SubDomainKey;
        } else if (field == "SystemState") {
            result = ENodeFields::SystemState;
        } else if (field == "PDisks") {
            result = ENodeFields::PDisks;
        } else if (field == "VDisks") {
            result = ENodeFields::VDisks;
        } else if (field == "Tablets") {
            result = ENodeFields::Tablets;
        }
        return result;
    }

public:
    TString GetLogPrefix() {
        static TString prefix = "json/nodes ";
        return prefix;
    }

    TJsonNodes(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        FieldsRequired.set(+ENodeFields::NodeId);
        UptimeSeconds = FromStringWithDefault<ui32>(params.Get("uptime"), 0);
        ProblemNodesOnly = FromStringWithDefault<bool>(params.Get("problems_only"), ProblemNodesOnly);
        Filter = params.Get("filter");
        if (UptimeSeconds || ProblemNodesOnly || !Filter.empty()) {
            FieldsRequired.set(+ENodeFields::SystemState);
        }
        Database = params.Get("database");
        if (!Database) {
            Database = params.Get("tenant");
        }
        FilterPath = params.Get("path");
        if (FilterPath && !Database) {
            Database = FilterPath;
        }
        if (Database) {
            FilterDatabase = true;
        }
        if (FilterPath == Database) {
            FilterPath.clear();
        }

        OffloadMerge = FromStringWithDefault<bool>(params.Get("offload_merge"), OffloadMerge);
        OffloadMergeAttempts = FromStringWithDefault<bool>(params.Get("offload_merge_attempts"), OffloadMergeAttempts);
        Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
        FilterStoragePool = params.Get("pool");
        if (FilterStoragePool.empty()) {
            FilterStoragePool = params.Get("storage_pool");
        }
        if (params.Has("group_id")) {
            FilterGroupIds.insert(FromStringWithDefault<ui32>(params.Get("group_id"), -1));
        }
        SplitIds(params.Get("node_id"), ',', FilterNodeIds);
        auto itZero = FilterNodeIds.find(0);
        if (itZero != FilterNodeIds.end()) {
            FilterNodeIds.erase(itZero);
            FilterNodeIds.insert(TlsActivationContext->ActorSystem()->NodeId);
        }
        if (params.Get("with") == "missing") {
            With = EWith::MissingDisks;
            FieldsRequired.set(+ENodeFields::Missing);
        } else if (params.Get("with") == "space") {
            With = EWith::SpaceProblems;
            FieldsRequired.set(+ENodeFields::DiskSpaceUsage);
        }
        if (params.Get("type") == "static") {
            Type = EType::Static;
            FieldsRequired.set(+ENodeFields::NodeInfo);
        } else if (params.Get("type") == "dynamic") {
            Type = EType::Dynamic;
            FieldsRequired.set(+ENodeFields::NodeInfo);
        } else if (params.Get("type") == "any") {
            Type = EType::Any;
        }
        NeedFilter = (With != EWith::Everything) || (Type != EType::Any) || !Filter.empty() || !FilterNodeIds.empty() || ProblemNodesOnly || UptimeSeconds > 0;
        if (params.Has("offset")) {
            Offset = FromStringWithDefault<ui32>(params.Get("offset"), 0);
            NeedLimit = true;
        }
        if (params.Has("limit")) {
            Limit = FromStringWithDefault<ui32>(params.Get("limit"), std::numeric_limits<ui32>::max());
            NeedLimit = true;
        }
        if (FromStringWithDefault<bool>(params.Get("storage"))) {
            FieldsRequired.set(+ENodeFields::PDisks);
            FieldsRequired.set(+ENodeFields::VDisks);
        }
        if (FromStringWithDefault<bool>(params.Get("tablets"))) {
            FieldsRequired.set(+ENodeFields::Tablets);
        }
        TStringBuf sort = params.Get("sort");
        if (sort) {
            NeedSort = true;
            if (sort.StartsWith("-") || sort.StartsWith("+")) {
                ReverseSort = (sort[0] == '-');
                sort.Skip(1);
            }
            SortBy = ParseENodeFields(sort);
            FieldsRequired.set(+SortBy);
        }
        TString fieldsRequired = params.Get("fields_required");
        if (!fieldsRequired.empty()) {
            if (fieldsRequired == "all") {
                FieldsRequired = FieldsAll;
            } else {
                TStringBuf source = fieldsRequired;
                for (TStringBuf value = source.NextTok(','); !value.empty(); value = source.NextTok(',')) {
                    ENodeFields field = ParseENodeFields(value);
                    if (field != ENodeFields::COUNT) {
                        FieldsRequired.set(+field);
                    }
                }
            }
        } else {
            FieldsRequired.set(+ENodeFields::SystemState);
        }
        TStringBuf group = params.Get("group");
        if (group) {
            NeedGroup = true;
            GroupBy = ParseENodeFields(group);
            FieldsRequired.set(+GroupBy);
            NeedSort = false;
            NeedLimit = false;
        }
        for (auto field = +ENodeFields::NodeId; field != +ENodeFields::COUNT; ++field) {
            if (FieldsRequired.test(field)) {
                auto itDependentFields = DependentFields.find(static_cast<ENodeFields>(field));
                if (itDependentFields != DependentFields.end()) {
                    FieldsRequired |= itDependentFields->second;
                }
            }
        }
        if (FromStringWithDefault<bool>(params.Get("all_whiteboard_fields"), false)) {
            AllWhiteboardFields = true;
        }
    }

    void Bootstrap() override {
        Direct |= !TBase::Event->Get()->Request.GetHeader("X-Forwarded-From-Node").empty(); // we're already forwarding
        Direct |= (Database == AppData()->TenantName) || Database.empty(); // we're already on the right node or don't use database filter

        if (Database && !Direct) {
            return RedirectToDatabase(Database); // to find some dynamic node and redirect query there
        } else {
            if (FieldsNeeded(FieldsNodeInfo)) {
                NodesInfoResponse = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
                NodeStateResponse = MakeWhiteboardRequest(TActivationContext::ActorSystem()->NodeId, new TEvWhiteboard::TEvNodeStateRequest());
            }
            if (FilterStoragePool || !FilterGroupIds.empty()) {
                FilterDatabase = false; // we disable database filter if we're filtering by pool or group
            }
            if (FilterDatabase) {
                DatabaseNavigateResponse = MakeRequestSchemeCacheNavigate(Database, ENavigateRequestDatabase);
                if (!FieldsNeeded(FieldsHiveNodeStat) && !(FilterPath && FieldsNeeded(FieldsTablets))) {
                    DatabaseBoardInfoResponse = MakeRequestStateStorageEndpointsLookup(Database, EBoardInfoRequestDatabase);
                }
            }
            if (FilterPath && FieldsNeeded(FieldsTablets)) {
                PathNavigateResponse = MakeRequestSchemeCacheNavigate(FilterPath, ENavigateRequestPath);
            }
            if (FilterStoragePool) {
                StoragePoolsResponse = RequestBSControllerPools();
                GroupsResponse = RequestBSControllerGroups();
                VSlotsResponse = RequestBSControllerVSlots();
                FilterStorageStage = EFilterStorageStage::Pools;
            } else if (!FilterGroupIds.empty()) {
                VSlotsResponse = RequestBSControllerVSlots();
                FilterStorageStage = EFilterStorageStage::VSlots;
            }
            if (With != EWith::Everything) {
                PDisksResponse = RequestBSControllerPDisks();
            }
            if (ProblemNodesOnly || GroupBy == ENodeFields::Uptime) {
                FieldsRequired.set(+ENodeFields::SystemState);
                TTabletId rootHiveId = AppData()->DomainsInfo->GetHive();
                HivesToAsk.push_back(rootHiveId);
                if (!PDisksResponse) {
                    PDisksResponse = RequestBSControllerPDisks();
                }
            }
            if (FieldsNeeded(FieldsHiveNodeStat) && !FilterDatabase && !FilterPath) {
                TTabletId rootHiveId = AppData()->DomainsInfo->GetHive();
                HivesToAsk.push_back(rootHiveId);
            }
            Schedule(TDuration::MilliSeconds(Timeout * 50 / 100), new TEvents::TEvWakeup(TimeoutTablets)); // 50% timeout (for tablets)
            TBase::Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup(TimeoutFinal));
        }
    }

    void InvalidateNodes() {
        NodesByNodeId.clear();
    }

    void RebuildNodesByNodeId() {
        NodesByNodeId.clear();
        for (TNode* node : NodeView) {
            NodesByNodeId.emplace(node->GetNodeId(), node);
        }
    }

    TNode* FindNode(TNodeId nodeId) {
        if (NodesByNodeId.empty()) {
            RebuildNodesByNodeId();
        }
        auto itNode = NodesByNodeId.find(nodeId);
        if (itNode != NodesByNodeId.end()) {
            return itNode->second;
        }
        return nullptr;
    }

    bool PreFilterDone() const {
        return !FilterDatabase && FilterStorageStage == EFilterStorageStage::None;
    }

    bool FilterDone() const {
        return PreFilterDone() && !NeedFilter;
    }

    void ApplyFilter() {
        // database pre-filter, affects TotalNodes count
        if (FilterDatabase) {
            if (FilterSubDomainKey && FieldsAvailable.test(+ENodeFields::SubDomainKey)) {
                TNodeView nodeView;
                if (HasDatabaseNodes) {
                    for (TNode* node : NodeView) {
                        if (node->HasSubDomainKey(SubDomainKey)) {
                            nodeView.push_back(node);
                        }
                    }
                } else {
                    for (TNode* node : NodeView) {
                        if (node->HasSubDomainKey(SharedSubDomainKey)) {
                            nodeView.push_back(node);
                        }
                    }
                }
                NodeView.swap(nodeView);
                FoundNodes = TotalNodes = NodeView.size();
                InvalidateNodes();
                FilterDatabase = false;
            } else if (FieldsAvailable.test(+ENodeFields::Database)) {
                TNodeView nodeView;
                if (HasDatabaseNodes) {
                    for (TNode* node : NodeView) {
                        if (node->HasDatabase(Database)) {
                            nodeView.push_back(node);
                        }
                    }
                } else {
                    for (TNode* node : NodeView) {
                        if (node->HasDatabase(SharedDatabase)) {
                            nodeView.push_back(node);
                        }
                    }
                }
                NodeView.swap(nodeView);
                FoundNodes = TotalNodes = NodeView.size();
                InvalidateNodes();
                FilterDatabase = false;
            } else {
                return;
            }
        }
        // storage/nodes pre-filter, affects TotalNodes count
        if (FilterStorageStage != EFilterStorageStage::None) {
            return;
        }
        if (!FilterNodeIds.empty() && FieldsAvailable.test(+ENodeFields::NodeId)) {
            TNodeView nodeView;
            for (TNode* node : NodeView) {
                if (FilterNodeIds.count(node->GetNodeId()) > 0) {
                    nodeView.push_back(node);
                }
            }
            NodeView.swap(nodeView);
            FoundNodes = TotalNodes = NodeView.size();
            InvalidateNodes();
            FilterNodeIds.clear();
        }
        if (NeedFilter) {
            if (With == EWith::MissingDisks && FieldsAvailable.test(+ENodeFields::Missing)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->MissingDisks != 0) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                With = EWith::Everything;
                InvalidateNodes();
            }
            if (With == EWith::SpaceProblems && FieldsAvailable.test(+ENodeFields::DiskSpaceUsage)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->DiskSpaceUsage >= SpaceUsageProblem) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                With = EWith::Everything;
                InvalidateNodes();
            }
            if (Type != EType::Any && FieldsAvailable.test(+ENodeFields::NodeInfo)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if ((Type == EType::Static && node->IsStatic()) || (Type == EType::Dynamic && !node->IsStatic())) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                Type = EType::Any;
                InvalidateNodes();
            }
            if (ProblemNodesOnly && FieldsAvailable.test(+ENodeFields::SystemState)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->GetOverall() != NKikimrWhiteboard::EFlag::Green) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                ProblemNodesOnly = false;
                InvalidateNodes();
            }
            if (UptimeSeconds > 0 && FieldsAvailable.test(+ENodeFields::SystemState)) {
                ui64 limitSeconds = TInstant::Now().Seconds() - UptimeSeconds;
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->SystemState.GetStartTime() >= limitSeconds) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                UptimeSeconds = 0;
                InvalidateNodes();
            }
            if (!Filter.empty() && FieldsAvailable.test(+ENodeFields::NodeInfo)) {
                TVector<TString> filterWords = SplitString(Filter, " ");
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    bool match = false;
                    for (const TString& word : filterWords) {
                        if (node->GetHostName().Contains(word)) {
                            match = true;
                            break;
                        } else if (::ToString(node->GetNodeId()).Contains(word)) {
                            match = true;
                            break;
                        }
                    }
                    if (match) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                Filter.clear();
                InvalidateNodes();
            }
            NeedFilter = (With != EWith::Everything) || (Type != EType::Any) || !Filter.empty() || !FilterNodeIds.empty() || ProblemNodesOnly || UptimeSeconds > 0;
            FoundNodes = NodeView.size();
        }
    }

    template<typename F>
    void GroupCollection(F&& groupBy) {
        std::unordered_map<TString, size_t> nodeGroups;
        NodeGroups.clear();
        for (TNode* node : NodeView) {
            auto gb = groupBy(node);
            TNodeGroup* nodeGroup = nullptr;
            auto it = nodeGroups.find(gb);
            if (it == nodeGroups.end()) {
                nodeGroups.emplace(gb, NodeGroups.size());
                nodeGroup = &NodeGroups.emplace_back();
                nodeGroup->Name = gb;
            } else {
                nodeGroup = &NodeGroups[it->second];
            }
            nodeGroup->Nodes.push_back(node);
        }
    }

    void ApplyGroup() {
        if (FilterDone() && NeedGroup && FieldsAvailable.test(+GroupBy)) {
            switch (GroupBy) {
                case ENodeFields::NodeId:
                    GroupCollection([](const TNode* node) { return ToString(node->GetNodeId()); });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::HostName:
                    GroupCollection([](const TNode* node) { return node->GetHostName(); });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::Database:
                    GroupCollection([](const TNode* node) { return node->Database; });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::DiskSpaceUsage:
                    GroupCollection([](const TNode* node) { return node->GetDiskUsageForGroup(); });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::DC:
                    GroupCollection([](const TNode* node) { return node->GetDataCenter(); });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::Rack:
                    GroupCollection([](const TNode* node) { return node->GetRack(); });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::Missing:
                    GroupCollection([](const TNode* node) { return ToString(node->MissingDisks); });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::Uptime:
                    GroupCollection([now = TInstant::Now()](const TNode* node) { return node->GetUptimeForGroup(now); });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::Version:
                    GroupCollection([](const TNode* node) { return node->GetVersionForGroup(); });
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.Name; });
                    break;
                case ENodeFields::NodeInfo:
                case ENodeFields::SystemState:
                case ENodeFields::PDisks:
                case ENodeFields::VDisks:
                case ENodeFields::Tablets:
                case ENodeFields::SubDomainKey:
                case ENodeFields::COUNT:
                case ENodeFields::Memory:
                case ENodeFields::CPU:
                case ENodeFields::LoadAverage:
                case ENodeFields::DisconnectTime:
                    break;
            }
            NeedGroup = false;
        }
    }

    void ApplySort() {
        if (FilterDone() && NeedSort && FieldsAvailable.test(+SortBy)) {
            switch (SortBy) {
                case ENodeFields::NodeId:
                    SortCollection(NodeView, [](const TNode* node) { return node->GetNodeId(); }, ReverseSort);
                    break;
                case ENodeFields::HostName:
                    SortCollection(NodeView, [](const TNode* node) { return node->GetHostName(); }, ReverseSort);
                    break;
                case ENodeFields::DC:
                    SortCollection(NodeView, [](const TNode* node) { return node->NodeInfo.Location.GetDataCenterId(); }, ReverseSort);
                    break;
                case ENodeFields::Rack:
                    SortCollection(NodeView, [](const TNode* node) { return node->NodeInfo.Location.GetRackId(); }, ReverseSort);
                    break;
                case ENodeFields::Version:
                    SortCollection(NodeView, [](const TNode* node) { return node->SystemState.GetVersion(); }, ReverseSort);
                    break;
                case ENodeFields::Uptime:
                    SortCollection(NodeView, [](const TNode* node) { return node->SystemState.GetStartTime(); }, ReverseSort);
                    break;
                case ENodeFields::Memory:
                case ENodeFields::CPU:
                case ENodeFields::LoadAverage:
                case ENodeFields::Missing:
                case ENodeFields::DiskSpaceUsage:
                case ENodeFields::NodeInfo:
                case ENodeFields::SystemState:
                case ENodeFields::PDisks:
                case ENodeFields::VDisks:
                case ENodeFields::Tablets:
                case ENodeFields::SubDomainKey:
                case ENodeFields::Database:
                case ENodeFields::DisconnectTime:
                case ENodeFields::COUNT:
                    break;
            }
            NeedSort = false;
            InvalidateNodes();
        }
    }

    void ApplyLimit() {
        if (FilterDone() && !NeedSort && !NeedGroup && NeedLimit) {
            if (Offset) {
                NodeView.erase(NodeView.begin(), NodeView.begin() + std::min(*Offset, NodeView.size()));
                InvalidateNodes();
            }
            if (Limit) {
                NodeView.resize(std::min(*Limit, NodeView.size()));
                InvalidateNodes();
            }
            NeedLimit = false;
        }
    }

    void ApplyEverything() {
        ApplyFilter();
        ApplyGroup();
        ApplySort();
        ApplyLimit();
    }

    static constexpr size_t BATCH_SIZE = 200;

    void BuildCandidates(TNodeBatch& batch, std::vector<TNode*>& candidates) {
        auto itCandidate = candidates.begin();
        for (; itCandidate != candidates.end() && batch.NodesToAskFor.size() < OffloadMergeAttempts; ++itCandidate) {
            batch.NodesToAskFor.push_back(*itCandidate);
        }
        candidates.erase(candidates.begin(), itCandidate);
        for (TNode* node : batch.NodesToAskAbout) {
            if (node->IsStatic()) {
                batch.HasStaticNodes = true;
            }
        }
    }

    void SplitBatch(TNodeBatch& nodeBatch, std::vector<TNodeBatch>& batches) {
        std::vector<TNode*> candidates = nodeBatch.NodesToAskAbout;
        std::sort(candidates.begin(), candidates.end(), [](TNode* a, TNode* b) {
            return a->GetCandidateScore() > b->GetCandidateScore();
        });
        while (nodeBatch.NodesToAskAbout.size() > BATCH_SIZE) {
            TNodeBatch newBatch;
            size_t splitSize = std::min(BATCH_SIZE, nodeBatch.NodesToAskAbout.size() / 2);
            newBatch.NodesToAskAbout.reserve(splitSize);
            for (size_t i = 0; i < splitSize; ++i) {
                newBatch.NodesToAskAbout.push_back(nodeBatch.NodesToAskAbout.back());
                nodeBatch.NodesToAskAbout.pop_back();
            }
            BuildCandidates(newBatch, candidates);
            batches.emplace_back(std::move(newBatch));
        }
        if (!nodeBatch.NodesToAskAbout.empty()) {
            BuildCandidates(nodeBatch, candidates);
            batches.emplace_back(std::move(nodeBatch));
        }
    }

    std::vector<TNodeBatch> BatchNodes() {
        std::vector<TNodeBatch> batches;
        if (OffloadMerge) {
            std::unordered_map<TSubDomainKey, TNodeBatch> batchSubDomain;
            std::unordered_map<TString, TNodeBatch> batchDataCenters;
            for (TNode* node : NodeView) {
                if (node->IsStatic()) {
                    batchDataCenters[node->GetDataCenter()].NodesToAskAbout.push_back(node);
                } else {
                    batchSubDomain[node->SubDomainKey].NodesToAskAbout.push_back(node);
                }
            }
            for (auto& [subDomainKey, nodeBatch] : batchSubDomain) {
                if (nodeBatch.NodesToAskAbout.size() == 1) {
                    TNode* node = nodeBatch.NodesToAskAbout.front();
                    batchDataCenters[node->GetDataCenter()].NodesToAskAbout.push_back(node);
                } else {
                    SplitBatch(nodeBatch, batches);
                }
            }
            for (auto& [dataCenter, nodeBatch] : batchDataCenters) {
                SplitBatch(nodeBatch, batches);
            }
        } else {
            TNodeBatch nodeBatch;
            for (TNode* node : NodeView) {
                nodeBatch.NodesToAskAbout.push_back(node);
            }
            SplitBatch(nodeBatch, batches);
        }
        return batches;
    }

    bool HiveResponsesDone() const {
        for (const auto& [hiveId, hiveNodeStats] : HiveNodeStats) {
            if (!hiveNodeStats.IsDone()) {
                return false;
            }
        }
        return !HiveNodeStats.empty();
    }

    bool TimeToAskHive() {
        if (NodesInfoResponse && !NodesInfoResponse->IsDone()) {
            return false;
        }
        if (DatabaseNavigateResponse && !DatabaseNavigateResponse->IsDone()) {
            return false;
        }
        if (ResourceNavigateResponse && !ResourceNavigateResponse->IsDone()) {
            return false;
        }
        if (PathNavigateResponse && !PathNavigateResponse->IsDone()) {
            return false;
        }
        return CurrentTimeoutState < TimeoutTablets;
    }

    bool TimeToAskWhiteboard() {
        if (NodesInfoResponse && !NodesInfoResponse->IsDone()) {
            return false;
        }
        if (NodeStateResponse && !NodeStateResponse->IsDone()) {
            return false;
        }
        if (DatabaseNavigateResponse && !DatabaseNavigateResponse->IsDone()) {
            return false;
        }
        if (ResourceNavigateResponse && !ResourceNavigateResponse->IsDone()) {
            return false;
        }
        if (PathNavigateResponse && !PathNavigateResponse->IsDone()) {
            return false;
        }
        if (DatabaseBoardInfoResponse && !DatabaseBoardInfoResponse->IsDone()) {
            return false;
        }
        if (ResourceBoardInfoResponse && !ResourceBoardInfoResponse->IsDone()) {
            return false;
        }
        for (const auto& [hiveId, hiveNodeStats] : HiveNodeStats) {
            if (!hiveNodeStats.IsDone()) {
                AddEvent("HiveNodeStats not done");
                return false;
            }
        }
        if (StoragePoolsResponse && !StoragePoolsResponse->IsDone()) {
            return false;
        }
        if (GroupsResponse && !GroupsResponse->IsDone()) {
            return false;
        }
        if (VSlotsResponse && !VSlotsResponse->IsDone()) {
            return false;
        }
        if (PDisksResponse && !PDisksResponse->IsDone()) {
            return false;
        }
        if (!SystemStateResponse.empty() || !TabletStateResponse.empty() || !PDiskStateResponse.empty()
            || !VDiskStateResponse.empty() || !SystemViewerResponse.empty() || !TabletViewerResponse.empty()) {
            return false;
        }
        return CurrentTimeoutState < TimeoutFinal;
    }

    static TString GetDatabaseFromEndpointsBoardPath(const TString& path) {
        TStringBuf db(path);
        db.SkipPrefix("gpc+");
        return TString(db);
    }

    void ProcessResponses() {
        AddEvent("ProcessResponses");
        if (NodesInfoResponse && NodesInfoResponse->IsDone()) {
            if (NodesInfoResponse->IsOk()) {
                bool seenDC = false;
                bool seenRack = false;
                for (const auto& ni : NodesInfoResponse->Get()->Nodes) {
                    TNode& node = NodeData.emplace_back();
                    node.NodeInfo = ni;
                    if (ni.Host && !node.SystemState.GetHost()) {
                        node.SystemState.SetHost(ni.Host);
                    }
                    if (ni.Location.GetDataCenterId() != 0) {
                        seenDC = true;
                    }
                    if (ni.Location.GetRackId() != 0) {
                        seenRack = true;
                    }
                }
                for (TNode& node : NodeData) {
                    NodeView.emplace_back(&node);
                }
                InvalidateNodes();
                FieldsAvailable |= FieldsNodeInfo;
                FoundNodes = TotalNodes = NodeView.size();
                NoDC = !seenDC;
                NoRack = !seenRack;
            } else {
                AddProblem("no-nodes-info");
            }
            NodesInfoResponse.reset();
        }

        if (NodeStateResponse && NodeStateResponse->IsDone() && TotalNodes > 0) {
            if (NodeStateResponse->IsOk()) {
                for (const auto& nodeStateInfo : NodeStateResponse->Get()->Record.GetNodeStateInfo()) {
                    if (nodeStateInfo.GetConnected()) {
                        TNodeId nodeId = FromStringWithDefault(TStringBuf(nodeStateInfo.GetPeerName()).Before(':'), 0);
                        if (nodeId) {
                            TNode* node = FindNode(nodeId);
                            if (node) {
                                node->Connected = true;
                            }
                        }
                    }
                }
            } else {
                AddProblem("no-node-state-info");
            }
            NodeStateResponse.reset();
        }

        if (DatabaseNavigateResponse && DatabaseNavigateResponse->IsDone()) { // database hive and subdomain key
            if (DatabaseNavigateResponse->IsOk()) {
                auto* ev = DatabaseNavigateResponse->Get();
                if (ev->Request->ResultSet.size() == 1 && ev->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    TSchemeCacheNavigate::TEntry& entry(ev->Request->ResultSet.front());
                    if (entry.DomainInfo) {
                        if (entry.DomainInfo->ResourcesDomainKey && entry.DomainInfo->DomainKey != entry.DomainInfo->ResourcesDomainKey) {
                            TPathId resourceDomainKey(entry.DomainInfo->ResourcesDomainKey);
                            ResourceNavigateResponse = MakeRequestSchemeCacheNavigate(resourceDomainKey, ENavigateRequestResource);
                        }
                        if (FieldsNeeded(FieldsHiveNodeStat) || (FilterPath && FieldsNeeded(FieldsTablets))) {
                            const auto ownerId = entry.DomainInfo->DomainKey.OwnerId;
                            const auto localPathId = entry.DomainInfo->DomainKey.LocalPathId;
                            SubDomainKey = TSubDomainKey(ownerId, localPathId);
                            if (FilterDatabase) {
                                FilterSubDomainKey = true;
                            }
                            HivesToAsk.push_back(AppData()->DomainsInfo->GetHive());
                            if (entry.DomainInfo->Params.HasHive()) {
                                HivesToAsk.push_back(entry.DomainInfo->Params.GetHive());
                            }
                        }
                    }
                }
            } else {
                NodeView.clear();
                AddProblem("no-database-info");
            }
            DatabaseNavigateResponse.reset();
        }

        if (ResourceNavigateResponse && ResourceNavigateResponse->IsDone()) { // database hive and subdomain key
            if (ResourceNavigateResponse->IsOk()) {
                auto* ev = ResourceNavigateResponse->Get();
                if (ev->Request->ResultSet.size() == 1 && ev->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    TSchemeCacheNavigate::TEntry& entry(ev->Request->ResultSet.front());
                    auto path = CanonizePath(entry.Path);
                    SharedDatabase = path;
                    if (FieldsNeeded(FieldsHiveNodeStat) || (FilterPath && FieldsNeeded(FieldsTablets))) {
                        HivesToAsk.push_back(AppData()->DomainsInfo->GetHive());
                        if (entry.DomainInfo) {
                            const auto ownerId = entry.DomainInfo->DomainKey.OwnerId;
                            const auto localPathId = entry.DomainInfo->DomainKey.LocalPathId;
                            SharedSubDomainKey = TSubDomainKey(ownerId, localPathId);
                            if (FilterDatabase) {
                                FilterSubDomainKey = true;
                            }
                            if (entry.DomainInfo->Params.HasHive()) {
                                HivesToAsk.push_back(entry.DomainInfo->Params.GetHive());
                            }
                        }
                    } else {
                        ResourceBoardInfoResponse = MakeRequestStateStorageEndpointsLookup(path, EBoardInfoRequestResource);
                    }
                }
            } else {
                NodeView.clear();
                AddProblem("no-shared-database-info");
            }
            ResourceNavigateResponse.reset();
        }

        if (PathNavigateResponse && PathNavigateResponse->IsDone()) { // filter path id
            if (PathNavigateResponse->IsOk()) {
                auto* ev = PathNavigateResponse->Get();
                if (ev->Request->ResultSet.size() == 1 && ev->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    TSchemeCacheNavigate::TEntry& entry(ev->Request->ResultSet.front());
                    if (entry.Self) {
                        FilterPathId = TPathId(entry.Self->Info.GetSchemeshardId(), entry.Self->Info.GetPathId());
                        AskHiveAboutPaths = true;
                        HivesToAsk.push_back(AppData()->DomainsInfo->GetHive());
                        if (entry.DomainInfo) {
                            const auto ownerId = entry.DomainInfo->DomainKey.OwnerId;
                            const auto localPathId = entry.DomainInfo->DomainKey.LocalPathId;
                            SubDomainKey = TSubDomainKey(ownerId, localPathId);
                            if (FilterDatabase) {
                                FilterSubDomainKey = true;
                            }
                            if (entry.DomainInfo->Params.HasHive()) {
                                HivesToAsk.push_back(entry.DomainInfo->Params.GetHive());
                            }
                        }
                    }
                }
            } else {
                AddProblem("no-path-info");
            }
            PathNavigateResponse.reset();
        }

        if (DatabaseBoardInfoResponse && DatabaseBoardInfoResponse->IsDone() && TotalNodes > 0) {
            if (DatabaseBoardInfoResponse->IsOk() && DatabaseBoardInfoResponse->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
                TString database = GetDatabaseFromEndpointsBoardPath(DatabaseBoardInfoResponse->Get()->Path);
                for (const auto& entry : DatabaseBoardInfoResponse->Get()->InfoEntries) {
                    if (!entry.second.Dropped) {
                        TNode* node = FindNode(entry.first.NodeId());
                        if (node) {
                            node->Database = database;
                            node->GotDatabaseFromDatabaseBoardInfo = true;
                            HasDatabaseNodes = true;
                        }
                    }
                }
                FieldsAvailable.set(+ENodeFields::Database);
            } else {
                AddProblem("no-database-board-info");
            }
            DatabaseBoardInfoResponse.reset();
        }

        if (ResourceBoardInfoResponse && ResourceBoardInfoResponse->IsDone() && TotalNodes > 0) {
            if (ResourceBoardInfoResponse->IsOk() && ResourceBoardInfoResponse->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
                TString database = GetDatabaseFromEndpointsBoardPath(ResourceBoardInfoResponse->Get()->Path);
                for (const auto& entry : ResourceBoardInfoResponse->Get()->InfoEntries) {
                    if (!entry.second.Dropped) {
                        TNode* node = FindNode(entry.first.NodeId());
                        if (node) {
                            node->Database = database;
                            node->GotDatabaseFromResourceBoardInfo = true;
                        }
                    }
                }
                FieldsAvailable.set(+ENodeFields::Database);
            } else {
                AddProblem("no-shared-database-board-info");
            }
            ResourceBoardInfoResponse.reset();
        }

        if (TimeToAskHive() && !HivesToAsk.empty()) {
            AddEvent("TimeToAskHive");
            std::sort(HivesToAsk.begin(), HivesToAsk.end());
            HivesToAsk.erase(std::unique(HivesToAsk.begin(), HivesToAsk.end()), HivesToAsk.end());
            for (TTabletId hiveId : HivesToAsk) {
                auto request = std::make_unique<TEvHive::TEvRequestHiveNodeStats>();
                request->Record.SetReturnMetrics(true);
                request->Record.SetReturnExtendedTabletInfo(true);
                if (AskHiveAboutPaths) {
                    request->Record.SetFilterTabletsBySchemeShardId(FilterPathId.OwnerId);
                    request->Record.SetFilterTabletsByPathId(FilterPathId.LocalPathId);
                }
                HiveNodeStats.emplace(hiveId, MakeRequestHiveNodeStats(hiveId, request.release()));
            }
            HivesToAsk.clear();
        }

        if (HiveResponsesDone()) {
            AddEvent("HiveResponsesDone");
            for (const auto& [hiveId, nodeStats] : HiveNodeStats) {
                if (nodeStats.IsDone()) {
                    if (nodeStats.IsOk()) {
                        for (const NKikimrHive::THiveNodeStats& nodeStats : nodeStats.Get()->Record.GetNodeStats()) {
                            ui32 nodeId = nodeStats.GetNodeId();
                            TNode* node = FindNode(nodeId);
                            if (node) {
                                for (const NKikimrHive::THiveDomainStatsStateCount& stateStats : nodeStats.GetStateStats()) {
                                    NKikimrViewer::TTabletStateInfo& viewerTablet(node->Tablets.emplace_back());
                                    viewerTablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(stateStats.GetTabletType()));
                                    viewerTablet.SetCount(stateStats.GetCount());
                                    viewerTablet.SetState(GetFlagFromTabletState(stateStats.GetVolatileState()));
                                    FieldsAvailable.set(+ENodeFields::Tablets);
                                }
                                if (nodeStats.HasLastAliveTimestamp()) {
                                    node->SystemState.SetDisconnectTime(std::max(node->SystemState.GetDisconnectTime(), nodeStats.GetLastAliveTimestamp() / 1000)); // seconds
                                    FieldsAvailable.set(+ENodeFields::DisconnectTime);
                                }
                                if (nodeStats.HasNodeDomain()) {
                                    node->SubDomainKey = TSubDomainKey(nodeStats.GetNodeDomain());
                                    FieldsAvailable.set(+ENodeFields::SubDomainKey);
                                    if (node->SubDomainKey == SubDomainKey) {
                                        HasDatabaseNodes = true;
                                    }
                                }
                            }
                        }
                    } else {
                        AddProblem("hive-no-data");
                    }
                }
            }
            HiveNodeStats.clear();
        }

        if (FilterStorageStage == EFilterStorageStage::Pools && StoragePoolsResponse && StoragePoolsResponse->IsDone()) {
            if (StoragePoolsResponse->IsOk()) {
                for (const auto& storagePoolEntry : StoragePoolsResponse->Get()->Record.GetEntries()) {
                    if (storagePoolEntry.GetInfo().GetName() == FilterStoragePool) {
                        FilterStoragePoolId = {storagePoolEntry.GetKey().GetBoxId(), storagePoolEntry.GetKey().GetStoragePoolId()};
                        break;
                    }
                }
                FilterStorageStage = EFilterStorageStage::Groups;
            } else {
                AddProblem("bsc-storage-pools-no-data");
            }
            StoragePoolsResponse.reset();
        }
        if (FilterStorageStage == EFilterStorageStage::Groups && GroupsResponse && GroupsResponse->IsDone()) {
            if (GroupsResponse->IsOk()) {
                for (const auto& groupEntry : GroupsResponse->Get()->Record.GetEntries()) {
                    if (groupEntry.GetInfo().GetBoxId() == FilterStoragePoolId.first
                        && groupEntry.GetInfo().GetStoragePoolId() == FilterStoragePoolId.second) {
                        FilterGroupIds.insert(groupEntry.GetKey().GetGroupId());
                    }
                }
                FilterStorageStage = EFilterStorageStage::VSlots;
            } else {
                AddProblem("bsc-storage-groups-no-data");
            }
            GroupsResponse.reset();
        }
        if (FilterStorageStage == EFilterStorageStage::VSlots && VSlotsResponse && VSlotsResponse->IsDone()) {
            if (VSlotsResponse->IsOk()) {
                std::unordered_map<std::pair<TNodeId, ui32>, std::size_t> slotsPerDisk;
                for (const auto& slotEntry : VSlotsResponse->Get()->Record.GetEntries()) {
                    if (FilterGroupIds.count(slotEntry.GetInfo().GetGroupId()) > 0) {
                        FilterNodeIds.insert(slotEntry.GetKey().GetNodeId());
                        TNode* node = FindNode(slotEntry.GetKey().GetNodeId());
                        if (node) {
                            node->SysViewVDisks.emplace_back(slotEntry);
                            node->HasDisks = true;
                        }
                    }
                    TNode* node = FindNode(slotEntry.GetKey().GetNodeId());
                    if (node) {
                        node->HasDisks = true;
                    }
                    auto& slots = slotsPerDisk[{slotEntry.GetKey().GetNodeId(), slotEntry.GetKey().GetPDiskId()}];
                    ++slots;
                    MaximumSlotsPerDisk = std::max(MaximumSlotsPerDisk.value_or(0), slots);
                }
                FilterStorageStage = EFilterStorageStage::None;
                ApplyEverything();
            } else {
                AddProblem("bsc-storage-slots-no-data");
            }
            VSlotsResponse.reset();
        }
        if (PDisksResponse && PDisksResponse->IsDone()) {
            if (PDisksResponse->IsOk()) {
                std::unordered_map<TNodeId, std::size_t> disksPerNode;
                for (const auto& pdiskEntry : PDisksResponse->Get()->Record.GetEntries()) {
                    TNode* node = FindNode(pdiskEntry.GetKey().GetNodeId());
                    if (node) {
                        node->SysViewPDisks.emplace_back(pdiskEntry);
                        node->HasDisks = true;
                    }
                    auto& disks = disksPerNode[pdiskEntry.GetKey().GetNodeId()];
                    ++disks;
                    MaximumDisksPerNode = std::max(MaximumDisksPerNode.value_or(0), disks);
                }
                for (TNode* node : NodeView) {
                    node->CalcDisks();
                }
                FieldsAvailable.set(+ENodeFields::Missing);
                FieldsAvailable.set(+ENodeFields::DiskSpaceUsage);
            } else {
                AddProblem("bsc-pdisks-no-data");
            }
            PDisksResponse.reset();
        }

        if (TimeToAskWhiteboard() && FieldsAvailable.test(+ENodeFields::NodeInfo)) {
            AddEvent("TimeToAskWhiteboard");
            ApplyEverything();
            if (FilterDatabase) {
                FieldsRequired.set(+ENodeFields::SystemState);
            }
            std::vector<TNodeBatch> batches = BatchNodes();
            SendWhiteboardRequests(batches);
        }
    }

    template<typename TWhiteboardEvent>
    void InitWhiteboardRequest(TWhiteboardEvent* request) {
        if (AllWhiteboardFields) {
            request->AddFieldsRequired(-1);
        }
    }

    void SendWhiteboardSystemAndTabletsBatch(TNodeBatch& batch) {
        TNodeId nodeId = OffloadMerge ? batch.ChooseNodeId() : 0;
        if (batch.HasStaticNodes && (FieldsNeeded(FieldsVDisks) || FieldsNeeded(FieldsPDisks))) {
            nodeId = 0; // we need to ask for all nodes anyway
        }
        if (nodeId) {
            if (FieldsNeeded(FieldsSystemState) && SystemViewerResponse.count(nodeId) == 0) {
                auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                InitWhiteboardRequest(viewerRequest->Record.MutableSystemRequest());
                viewerRequest->Record.SetTimeout(Timeout / 2);
                for (const TNode* node : batch.NodesToAskAbout) {
                    viewerRequest->Record.MutableLocation()->AddNodeId(node->GetNodeId());
                }
                SystemViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                NodeBatches.emplace(nodeId, batch);
                ++WhiteboardStateRequestsInFlight;
            }
            if (FieldsNeeded(FieldsTablets) && TabletViewerResponse.count(nodeId) == 0) {
                auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                viewerRequest->Record.MutableTabletRequest()->SetGroupBy("NodeId,Type,State");
                viewerRequest->Record.SetTimeout(Timeout / 2);
                for (const TNode* node : batch.NodesToAskAbout) {
                    viewerRequest->Record.MutableLocation()->AddNodeId(node->GetNodeId());
                }
                TabletViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                NodeBatches.emplace(nodeId, batch);
                ++WhiteboardStateRequestsInFlight;
            }
        } else {
            for (const TNode* node : batch.NodesToAskAbout) {
                if (node->Disconnected) {
                    continue;
                }
                TNodeId nodeId = node->GetNodeId();
                if (FieldsNeeded(FieldsSystemState)) {
                    if (SystemStateResponse.count(nodeId) == 0) {
                        auto request = new TEvWhiteboard::TEvSystemStateRequest();
                        InitWhiteboardRequest(&request->Record);
                        SystemStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request));
                        ++WhiteboardStateRequestsInFlight;
                    }
                }
                if (FieldsNeeded(FieldsTablets)) {
                    if (TabletStateResponse.count(nodeId) == 0) {
                        auto request = std::make_unique<TEvWhiteboard::TEvTabletStateRequest>();
                        request->Record.SetGroupBy("Type,State");
                        TabletStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request.release()));
                        ++WhiteboardStateRequestsInFlight;
                    }
                }
            }
        }
    }

    void SendWhiteboardRequest(TNodeBatch& batch) {
        SendWhiteboardSystemAndTabletsBatch(batch);
        for (const TNode* node : batch.NodesToAskAbout) {
            TNodeId nodeId = node->GetNodeId();

            if (node->IsStatic()) {
                if (FieldsNeeded(FieldsVDisks)) {
                    if (VDiskStateResponse.count(nodeId) == 0) {
                        auto request = new TEvWhiteboard::TEvVDiskStateRequest();
                        InitWhiteboardRequest(&request->Record);
                        VDiskStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request));
                        ++WhiteboardStateRequestsInFlight;
                    }
                }
                if (FieldsNeeded(FieldsPDisks)) {
                    if (PDiskStateResponse.count(nodeId) == 0) {
                        auto request = new TEvWhiteboard::TEvPDiskStateRequest();
                        InitWhiteboardRequest(&request->Record);
                        PDiskStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request));
                        ++WhiteboardStateRequestsInFlight;
                    }
                }
            }
        }
    }

    void SendWhiteboardRequests(std::vector<TNodeBatch>& batches) {
        for (TNodeBatch& batch : batches) {
            SendWhiteboardRequest(batch);
        }
    }

    void ProcessWhiteboard() {
        if (FieldsNeeded(FieldsSystemState)) {
            std::unordered_set<TNodeId> removeNodes;
            for (const auto& [responseNodeId, response] : SystemViewerResponse) {
                if (response.IsOk()) {
                    const auto& systemResponse(response.Get()->Record.GetSystemResponse());
                    for (const auto& systemInfo : systemResponse.GetSystemStateInfo()) {
                        TNodeId nodeId = systemInfo.GetNodeId();
                        TNode* node = FindNode(nodeId);
                        if (node) {
                            node->SystemState.MergeFrom(systemInfo);
                            node->Cleanup();
                            node->CalcDatabase();
                            if (Database && node->Database) {
                                if (node->Database != Database && (!SharedDatabase || node->Database != SharedDatabase)) {
                                    removeNodes.insert(nodeId);
                                }
                            }
                        }
                    }
                }
            }
            for (const auto& [nodeId, response] : SystemStateResponse) {
                if (response.IsOk()) {
                    const auto& systemState(response.Get()->Record);
                    if (systemState.SystemStateInfoSize() > 0) {
                        TNode* node = FindNode(nodeId);
                        if (node) {
                            node->SystemState.MergeFrom(systemState.GetSystemStateInfo(0));
                            node->Cleanup();
                            node->CalcDatabase();
                            if (Database && node->Database) {
                                if (node->Database != Database && (!SharedDatabase || node->Database != SharedDatabase)) {
                                    removeNodes.insert(nodeId);
                                }
                            }
                        }
                    }
                }
            }
            if (!removeNodes.empty()) {
                NodeView.erase(std::remove_if(NodeView.begin(), NodeView.end(), [&removeNodes](const TNode* node) { return removeNodes.count(node->GetNodeId()) > 0; }), NodeView.end());
                TotalNodes = FoundNodes = NodeView.size();
                InvalidateNodes();
            }
            FieldsAvailable |= FieldsSystemState;
            FieldsAvailable.set(+ENodeFields::Database);
        }
        if (FieldsNeeded(FieldsTablets)) {
            for (auto& [nodeId, response] : TabletViewerResponse) {
                if (response.IsOk()) {
                    auto& tabletResponse(*(response.Get()->Record.MutableTabletResponse()));
                    if (tabletResponse.TabletStateInfoSize() > 0 && !tabletResponse.GetTabletStateInfo(0).HasCount()) {
                        GroupWhiteboardResponses(tabletResponse, "NodeId,Type,State");
                    }
                    for (const auto& tabletState : tabletResponse.GetTabletStateInfo()) {
                        TNode* node = FindNode(tabletState.GetNodeId());
                        if (node) {
                            if (tabletState.GetState() != NKikimrWhiteboard::TTabletStateInfo::Dead) {
                                NKikimrViewer::TTabletStateInfo& viewerTablet(node->Tablets.emplace_back());
                                viewerTablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(tabletState.GetType()));
                                viewerTablet.SetState(GetFlagFromTabletState(tabletState.GetState()));
                                viewerTablet.SetCount(tabletState.GetCount());
                            }
                        }
                    }
                }
            }
            for (auto& [nodeId, response] : TabletStateResponse) {
                if (response.IsOk()) {
                    const auto& tabletState(response.Get()->Record);
                    TNode* node = FindNode(nodeId);
                    if (node) {
                        for (const auto& protoTabletState : tabletState.GetTabletStateInfo()) {
                            if (protoTabletState.GetState() != NKikimrWhiteboard::TTabletStateInfo::Dead) {
                                NKikimrViewer::TTabletStateInfo& viewerTablet(node->Tablets.emplace_back());
                                viewerTablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(protoTabletState.GetType()));
                                viewerTablet.SetState(GetFlagFromTabletState(protoTabletState.GetState()));
                                viewerTablet.SetCount(protoTabletState.GetCount());
                            }
                        }
                    }
                }
            }
            FieldsAvailable |= FieldsTablets;
        }
        if (FieldsNeeded(FieldsVDisks)) {
            for (const auto& [nodeId, response] : VDiskStateResponse) {
                if (response.IsOk()) {
                    const auto& vDiskState(response.Get()->Record);
                    TNode* node = FindNode(nodeId);
                    if (node) {
                        for (const auto& protoVDiskState : vDiskState.GetVDiskStateInfo()) {
                            node->VDisks.emplace_back(protoVDiskState);
                        }
                    }
                }
            }
            FieldsAvailable |= FieldsVDisks;
        }
        if (FieldsNeeded(FieldsPDisks)) {
            for (const auto& [nodeId, response] : PDiskStateResponse) {
                if (response.IsOk()) {
                    const auto& pDiskState(response.Get()->Record);
                    TNode* node = FindNode(nodeId);
                    if (node) {
                        for (const auto& protoPDiskState : pDiskState.GetPDiskStateInfo()) {
                            node->PDisks.emplace_back(protoPDiskState);
                        }
                        node->CalcDisks();
                    }
                    MaximumDisksPerNode = std::max(MaximumDisksPerNode.value_or(0), pDiskState.PDiskStateInfoSize());
                }
            }
            FieldsAvailable |= FieldsPDisks;
            FieldsAvailable.set(+ENodeFields::Missing);
            FieldsAvailable.set(+ENodeFields::DiskSpaceUsage);
        }
        ApplyEverything();
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        NodesInfoResponse->Set(std::move(ev));
        ProcessResponses();
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvNodeStateResponse::TPtr& ev) {
        NodeStateResponse->Set(std::move(ev));
        ProcessResponses();
        RequestDone();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Cookie == ENavigateRequestDatabase) {
            DatabaseNavigateResponse->Set(std::move(ev));
        } else if (ev->Cookie == ENavigateRequestResource) {
            ResourceNavigateResponse->Set(std::move(ev));
        } else if (ev->Cookie == ENavigateRequestPath) {
            PathNavigateResponse->Set(std::move(ev));
        }
        ProcessResponses();
        RequestDone();
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (ev->Cookie == EBoardInfoRequestDatabase) {
            DatabaseBoardInfoResponse->Set(std::move(ev));
        } else if (ev->Cookie == EBoardInfoRequestResource) {
            ResourceBoardInfoResponse->Set(std::move(ev));
        }
        ProcessResponses();
        RequestDone();
    }

    void Handle(TEvHive::TEvResponseHiveNodeStats::TPtr& ev) {
        HiveNodeStats[ev->Cookie].Set(std::move(ev));
        ProcessResponses();
        RequestDone();
    }

    void WhiteboardRequestDone() {
        --WhiteboardStateRequestsInFlight;
        if (WhiteboardStateRequestsInFlight == 0) {
            ProcessWhiteboard();
        }
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        SystemStateResponse[nodeId].Set(std::move(ev));
        WhiteboardRequestDone();
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        VDiskStateResponse[nodeId].Set(std::move(ev));
        WhiteboardRequestDone();
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        PDiskStateResponse[nodeId].Set(std::move(ev));
        WhiteboardRequestDone();
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        TabletStateResponse[nodeId].Set(std::move(ev));
        WhiteboardRequestDone();
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->Record.Response_case()) {
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kSystemResponse:
                SystemViewerResponse[nodeId].Set(std::move(ev));
                NodeBatches.erase(nodeId);
                WhiteboardRequestDone();
                return;
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kTabletResponse:
                TabletViewerResponse[nodeId].Set(std::move(ev));
                NodeBatches.erase(nodeId);
                WhiteboardRequestDone();
                return;
            default:
                break;
        }
        TString error("WrongResponse");
        {
            auto itSystemViewerResponse = SystemViewerResponse.find(nodeId);
            if (itSystemViewerResponse != SystemViewerResponse.end()) {
                if (itSystemViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardSystemAndTabletsBatch(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletViewerResponse = TabletViewerResponse.find(nodeId);
            if (itTabletViewerResponse != TabletViewerResponse.end()) {
                if (itTabletViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardSystemAndTabletsBatch(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        StoragePoolsResponse->Set(std::move(ev));
        ProcessResponses();
        RequestDone();
    }

    void Handle(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        GroupsResponse->Set(std::move(ev));
        ProcessResponses();
        RequestDone();
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        VSlotsResponse->Set(std::move(ev));
        ProcessResponses();
        RequestDone();
    }

    void Handle(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        PDisksResponse->Set(std::move(ev));
        ProcessResponses();
        RequestDone();
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        TNode* node = FindNode(nodeId);
        if (node) {
            node->DisconnectNode();
            if (FieldsRequired.test(+ENodeFields::PDisks) || FieldsRequired.test(+ENodeFields::VDisks)) {
                node->RemapDisks();
            }
        }
        TString error("NodeDisconnected");
        {
            auto itSystemStateResponse = SystemStateResponse.find(nodeId);
            if (itSystemStateResponse != SystemStateResponse.end()) {
                if (itSystemStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itVDiskStateResponse = VDiskStateResponse.find(nodeId);
            if (itVDiskStateResponse != VDiskStateResponse.end()) {
                if (itVDiskStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itPDiskStateResponse = PDiskStateResponse.find(nodeId);
            if (itPDiskStateResponse != PDiskStateResponse.end()) {
                if (itPDiskStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletStateResponse = TabletStateResponse.find(nodeId);
            if (itTabletStateResponse != TabletStateResponse.end()) {
                if (itTabletStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itSystemViewerResponse = SystemViewerResponse.find(nodeId);
            if (itSystemViewerResponse != SystemViewerResponse.end()) {
                if (itSystemViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardSystemAndTabletsBatch(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletViewerResponse = TabletViewerResponse.find(nodeId);
            if (itTabletViewerResponse != TabletViewerResponse.end()) {
                if (itTabletViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardSystemAndTabletsBatch(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
    }

    bool OnBscError(const TString& error) {
        bool result = false;
        if (StoragePoolsResponse && StoragePoolsResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        if (GroupsResponse && GroupsResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        if (VSlotsResponse && VSlotsResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        if (PDisksResponse && PDisksResponse->Error(error)) {
            ProcessResponses();
            result = true;
        }
        return result;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TString error = TStringBuilder() << "Failed to establish pipe to " << ev->Get()->TabletId << ": "
                << NKikimrProto::EReplyStatus_Name(ev->Get()->Status);
            auto it = HiveNodeStats.find(ev->Get()->TabletId);
            if (it != HiveNodeStats.end()) {
                if (it->second.Error(error)) {
                    AddProblem("hive-error");
                    ProcessResponses();
                }
            }
            if (ev->Get()->TabletId == GetBSControllerId()) {
                if (OnBscError(error)) {
                    AddProblem("bsc-error");
                }
            }
        }
        TBase::Handle(ev); // all RequestDone() are handled by base handler
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr& ev) {
        CurrentTimeoutState = static_cast<ETimeoutTag>(ev->Get()->Tag);
        TString error = "Timeout";
        if (ev->Get()->Tag == TimeoutTablets) {
            if (NodesInfoResponse && NodesInfoResponse->Error(error)) {
                ProcessResponses();
            }
            if (NodeStateResponse && NodeStateResponse->Error(error)) {
                ProcessResponses();
            }
            if (DatabaseNavigateResponse && DatabaseNavigateResponse->Error(error)) {
                ProcessResponses();
            }
            if (ResourceNavigateResponse && ResourceNavigateResponse->Error(error)) {
                ProcessResponses();
            }
            if (PathNavigateResponse && PathNavigateResponse->Error(error)) {
                ProcessResponses();
            }
            if (OnBscError(error)) {
                AddProblem("bsc-timeout");
            }
            RequestDone(FailPipeConnect(GetBSControllerId()));
            for (auto& [hiveId, response] : HiveNodeStats) {
                if (response.Error(error)) {
                    AddProblem("hive-timeout");
                    ProcessResponses();
                    RequestDone(FailPipeConnect(hiveId));
                }
            }
        }
        if (ev->Get()->Tag == TimeoutFinal) {
            for (auto& [nodeId, response] : SystemViewerResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : TabletViewerResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : SystemStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : VDiskStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : PDiskStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : TabletStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            if (WaitingForResponse()) {
                ReplyAndPassAway();
            }
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvHive::TEvResponseHiveNodeStats, Handle);
            hFunc(NSysView::TEvSysView::TEvGetGroupsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetPDisksResponse, Handle);
            hFunc(TEvViewer::TEvViewerResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvents::TEvWakeup, HandleTimeout);
        }
    }

    void ReplyAndPassAway() override {
        AddEvent("ReplyAndPassAway");
        ApplyEverything();
        NKikimrViewer::TNodesInfo json;
        json.SetVersion(2);
        json.SetFieldsAvailable(FieldsAvailable.to_string());
        json.SetFieldsRequired(FieldsRequired.to_string());
        if (NeedFilter) {
            json.SetNeedFilter(true);
        }
        if (NeedGroup) {
            json.SetNeedGroup(true);
        }
        if (NeedSort) {
            json.SetNeedSort(true);
        }
        if (NeedLimit) {
            json.SetNeedLimit(true);
        }
        json.SetTotalNodes(TotalNodes);
        json.SetFoundNodes(FoundNodes);
        if (MaximumDisksPerNode.has_value()) {
            json.SetMaximumDisksPerNode(MaximumDisksPerNode.value());
        }
        if (MaximumSlotsPerDisk.has_value()) {
            json.SetMaximumSlotsPerDisk(MaximumSlotsPerDisk.value());
        }
        if (NoDC) {
            json.SetNoDC(true);
        }
        if (NoRack) {
            json.SetNoRack(true);
        }
        for (auto problem : Problems) {
            json.AddProblems(problem);
        }
        if (NodeGroups.empty()) {
            for (TNode* node : NodeView) {
                NKikimrViewer::TNodeInfo& jsonNode = *json.AddNodes();
                if (FieldsAvailable.test(+ENodeFields::NodeInfo)) {
                    jsonNode.SetNodeId(node->GetNodeId());
                }
                if (FieldsAvailable.test(+ENodeFields::NodeInfo) || FieldsAvailable.test(+ENodeFields::SystemState)) {
                    *jsonNode.MutableSystemState() = std::move(node->SystemState);
                }
                if (FieldsAvailable.test(+ENodeFields::PDisks)) {
                    for (NKikimrWhiteboard::TPDiskStateInfo& pDisk : node->PDisks) {
                        (*jsonNode.AddPDisks()) = std::move(pDisk);
                    }
                    std::sort(node->PDisks.begin(), node->PDisks.end(), [](const NKikimrWhiteboard::TPDiskStateInfo& a, const NKikimrWhiteboard::TPDiskStateInfo& b) {
                        return a.pdiskid() < b.pdiskid();
                    });
                }
                if (FieldsAvailable.test(+ENodeFields::VDisks)) {
                    for (NKikimrWhiteboard::TVDiskStateInfo& vDisk : node->VDisks) {
                        (*jsonNode.AddVDisks()) = std::move(vDisk);
                    }
                    std::sort(node->VDisks.begin(), node->VDisks.end(), [](const NKikimrWhiteboard::TVDiskStateInfo& a, const NKikimrWhiteboard::TVDiskStateInfo& b) {
                        return VDiskIDFromVDiskID(a.vdiskid()) < VDiskIDFromVDiskID(b.vdiskid());
                    });
                }
                if (FieldsAvailable.test(+ENodeFields::Tablets)) {
                    for (NKikimrViewer::TTabletStateInfo& tablet : node->Tablets) {
                        (*jsonNode.AddTablets()) = std::move(tablet);
                    }
                    std::sort(node->Tablets.begin(), node->Tablets.end(), [](const NKikimrViewer::TTabletStateInfo& a, const NKikimrViewer::TTabletStateInfo& b) {
                        return a.type() < b.type();
                    });
                }
            }
        } else {
            for (const TNodeGroup& nodeGroup : NodeGroups) {
                NKikimrViewer::TNodeGroup& jsonNodeGroup = *json.AddNodeGroups();
                jsonNodeGroup.SetGroupName(nodeGroup.Name);
                jsonNodeGroup.SetNodeCount(nodeGroup.Nodes.size());
            }
        }
        TStringStream out;
        Proto2Json(json, out, {
            .EnumMode = TProto2JsonConfig::EnumValueMode::EnumName,
            .StringifyNumbers = TProto2JsonConfig::EStringifyNumbersMode::StringifyInt64Always,
            .WriteNanAsString = true,
        });
        TBase::ReplyAndPassAway(GetHTTPOKJSON(out.Str()));
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Nodes info",
            .Description = "Information about nodes",
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "path to schema object",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "with",
            .Description = "filter nodes by missing disks or space",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "storage",
            .Description = "return storage info",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "tablets",
            .Description = "return tablets info",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "sort",
            .Description = "sort by (NodeId,Host,DC,Rack,Version,Uptime,Missing)",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "group",
            .Description = "group by (NodeId,Host,DC,Rack,Version,Uptime,Missing)",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "offset",
            .Description = "skip N nodes",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "limit",
            .Description = "limit to N nodes",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "uptime",
            .Description = "return only nodes with less uptime in sec.",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "problems_only",
            .Description = "return only problem nodes",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "filter",
            .Description = "filter nodes by id or host",
            .Type = "string",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TNodesInfo>());
        return yaml;
    }
};

}
