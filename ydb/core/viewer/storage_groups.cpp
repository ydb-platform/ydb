#include <unordered_map>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include "viewer.h"
#include "viewer_helper.h"
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "json_vdiskinfo.h"
#include "json_pdiskinfo.h"
#include "json_bsgroupinfo.h"

namespace NKikimr {
namespace NViewer {

using namespace NProtobufJson;

using TNodeId = ui32;
using TGroupId = ui32;

struct TPDiskId {
    TNodeId NodeId;
    ui32 PDiskId;

    TPDiskId() = default;

    TPDiskId(TNodeId nodeId, ui32 pdiskId)
        : NodeId(nodeId)
        , PDiskId(pdiskId)
    {}

    TPDiskId(const NKikimrSysView::TPDiskKey& key)
        : NodeId(key.GetNodeId())
        , PDiskId(key.GetPDiskId())
    {}

    TPDiskId(const NKikimrSysView::TVSlotKey& key)
        : NodeId(key.GetNodeId())
        , PDiskId(key.GetPDiskId())
    {}

    bool operator ==(const TPDiskId& a) const = default;
};

struct TVSlotId : TPDiskId {
    ui32 VDiskSlotId;

    TVSlotId() = default;

    TVSlotId(TNodeId nodeId, ui32 pdiskId, ui32 vdiskSlotId)
        : TPDiskId(nodeId, pdiskId)
        , VDiskSlotId(vdiskSlotId)
    {}

    TVSlotId(const NKikimrBlobStorage::TVSlotId& proto)
        : TVSlotId(proto.GetNodeId(), proto.GetPDiskId(), proto.GetVSlotId())
    {}

    TVSlotId(const NKikimrSysView::TVSlotKey& proto)
        : TVSlotId(proto.GetNodeId(), proto.GetPDiskId(), proto.GetVSlotId())
    {}

    TVSlotId(const NKikimrWhiteboard::TVDiskStateInfo& proto)
        : TVSlotId(proto.GetNodeId(), proto.GetPDiskId(), proto.GetVDiskSlotId())
    {}

    bool operator ==(const TVSlotId& a) const = default;
};

}
}

template<>
struct std::hash<NKikimr::NViewer::TPDiskId> {
    std::size_t operator ()(const NKikimr::NViewer::TPDiskId& s) const {
        return ::std::hash<NKikimr::NViewer::TNodeId>()(s.NodeId)
            ^ (::std::hash<ui32>()(s.PDiskId) << 1);
    }
};

template<>
struct std::hash<NKikimr::NViewer::TVSlotId> {
    std::size_t operator ()(const NKikimr::NViewer::TVSlotId& s) const {
        return ::std::hash<NKikimr::NViewer::TNodeId>()(s.NodeId)
            ^ (::std::hash<ui32>()(s.PDiskId) << 1)
            ^ (::std::hash<ui32>()(s.VDiskSlotId) << 2);
    }
};

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

using ::google::protobuf::FieldDescriptor;

enum class EGroupFields : ui8 {
    GroupId,
    PoolName,
    Kind,
    MediaType,
    Erasure,
    MissingDisks,
    State,
    Usage,
    Encryption,
    Used,
    Limit,
    Read,
    Write,
    Available,
    AllocationUnits,
    NodeId,
    PDiskId,
    VDisk, // VDisk information
    PDisk, // PDisk information
    COUNT
};

constexpr ui8 operator +(EGroupFields e) {
    return static_cast<ui8>(e);
}

using TFieldsType = std::bitset<+EGroupFields::COUNT>;

class TStorageGroups : public TViewerPipeClient<TStorageGroups> {
public:
    using TBase = TViewerPipeClient<TStorageGroups>;
    using TThis = TStorageGroups;

    // Common
    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> DatabaseNavigateResult;
    std::unordered_map<TPathId, TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> NavigateKeySetResult;
    std::unordered_map<TPathId, TTabletId> PathId2HiveId;
    std::unordered_map<TTabletId, TRequestResponse<TEvHive::TEvResponseHiveStorageStats>> HiveStorageStats;
    ui64 HiveStorageStatsInFlight = 0;

    // BSC
    bool FallbackToWhiteboard = false;
    bool FillDisksFromWhiteboard = false;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse>> GetGroupsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse>> GetStoragePoolsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse>> GetVSlotsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse>> GetPDisksResponse;

    // Whiteboard
    std::optional<TRequestResponse<TEvInterconnect::TEvNodesInfo>> NodesInfo;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvBSGroupStateResponse>> BSGroupStateResponse;
    ui64 BSGroupStateRequestsInFlight = 0;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvVDiskStateResponse>> VDiskStateResponse;
    ui64 VDiskStateRequestsInFlight = 0;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvPDiskStateResponse>> PDiskStateResponse;
    ui64 PDiskStateRequestsInFlight = 0;

    ui32 Timeout = 0;
    TString Database;
    bool Direct = false;
    TString Filter;
    std::unordered_set<TString> FilterStoragePools;
    std::unordered_set<TGroupId> FilterGroupIds;
    std::unordered_set<TNodeId> FilterNodeIds;
    std::unordered_set<ui32> FilterPDiskIds;
    std::vector<TNodeId> SubscriptionNodeIds;

    enum class EWith {
        Everything,
        MissingDisks,
        SpaceProblems,
    };

    enum ETimeoutTag {
        TimeoutBSC,
        TimeoutFinal,
    };

    EGroupFields GroupSort = EGroupFields::PoolName;
    EWith With = EWith::Everything;
    bool ReverseSort = false;
    std::optional<std::size_t> Offset;
    std::optional<std::size_t> Limit;
    ui32 SpaceUsageProblem = 90; // %

    struct TPDisk {
        ui32 PDiskId = 0;
        TNodeId NodeId = 0;
        TString Type;
        TString Path;
        ui64 Guid = 0;
        ui64 AvailableSize = 0;
        ui64 TotalSize = 0;
        TString Status;
        TInstant StatusChangeTimestamp;
        ui64 EnforcedDynamicSlotSize = 0;
        ui32 ExpectedSlotCount = 0;
        ui32 NumActiveSlots = 0;
        ui64 Category = 0;
        TString DecommitStatus;
        NKikimrViewer::EFlag DiskSpace = NKikimrViewer::EFlag::Grey;

        void SetCategory(ui64 category) {
            Category = category;
            switch (TPDiskCategory(Category).Type()) {
                case NPDisk::EDeviceType::DEVICE_TYPE_ROT:
                    Type = "hdd";
                    break;
                case NPDisk::EDeviceType::DEVICE_TYPE_SSD:
                    Type = "ssd";
                    break;
                case NPDisk::EDeviceType::DEVICE_TYPE_NVME:
                    Type = "nvme";
                    break;
                case NPDisk::EDeviceType::DEVICE_TYPE_UNKNOWN:
                    break;
            }
        }

        ui64 GetSlotTotalSize() const {
            if (EnforcedDynamicSlotSize) {
                return EnforcedDynamicSlotSize;
            }
            if (ExpectedSlotCount) {
                return TotalSize / ExpectedSlotCount;
            }
            if (NumActiveSlots) {
                return TotalSize / NumActiveSlots;
            }
            return TotalSize;
        }

        TString GetPDiskId() const {
            return TStringBuilder() << NodeId << '-' << PDiskId;
        }
    };

    struct TVDisk {
        TVDiskID VDiskId;
        TVSlotId VSlotId;
        ui64 AllocatedSize = 0;
        ui64 AvailableSize = 0;
        TString Status;
        NKikimrBlobStorage::EVDiskStatus VDiskStatus = NKikimrBlobStorage::EVDiskStatus::ERROR;
        ui64 Read = 0;
        ui64 Write = 0;
        NKikimrViewer::EFlag DiskSpace = NKikimrViewer::EFlag::Grey;
        bool Donor = false;
        std::vector<TVSlotId> Donors;

        TString GetVDiskId() const {
            return TStringBuilder() << VDiskId.GroupID.GetRawId() << '-'
                                    << VDiskId.GroupGeneration << '-'
                                    << int(VDiskId.FailRealm) << '-'
                                    << int(VDiskId.FailDomain) << '-'
                                    << int(VDiskId.VDisk);
        }
    };

    struct TGroup {
        TString PoolName;
        TGroupId GroupId = 0;
        ui32 GroupGeneration = 0;
        ui64 BoxId = 0;
        ui64 PoolId = 0;
        ui64 SchemeShardId = 0;
        ui64 PathId = 0;
        TString Kind;
        TString MediaType;
        TString Erasure;
        TErasureType::EErasureSpecies ErasureSpecies = TErasureType::ErasureNone;
        TString State;
        ui32 EncryptionMode = 0;
        ui64 AllocationUnits = 0;
        float Usage = 0;
        ui64 Used = 0;
        ui64 Limit = 0;
        ui64 Available = 0;
        ui64 Read = 0;
        ui64 Write = 0;
        ui32 MissingDisks = 0;
        ui64 PutTabletLogLatency = 0;
        ui64 PutUserDataLatency = 0;
        ui64 GetFastLatency = 0;
        NKikimrViewer::EFlag Overall = NKikimrViewer::EFlag::Grey;
        NKikimrViewer::EFlag DiskSpace = NKikimrViewer::EFlag::Grey;

        std::vector<TVDisk> VDisks;
        std::vector<TNodeId> VDiskNodeIds; // filter nodes to request disk info from the whiteboard. could be duplicated.

        TString PrintDomains(const std::vector<ui8>& failedDomains) {
            TString result;
            result += ::ToString(failedDomains.size());
            result += '(';
            for (ui8 domains : failedDomains) {
                if (!result.empty()) {
                    result += ',';
                }
                result += ::ToString(domains);
            }
            result += ')';
            return result;
        }

        // none: ok, dead:1
        // block-4-2: ok, replicating:1 starting:1, degraded:1, degraded:2, dead:3
        // mirror-3-dc: ok, degraded:1(1), degraded:1(2), degraded:1(3), degraded:2(3,1), dead:3(3,1,1)

        void CalcState() {
            MissingDisks = 0;
            ui64 allocated = 0;
            ui64 limit = 0;
            ui32 startingDisks = 0;
            ui32 replicatingDisks = 0;
            static_assert(sizeof(TVDiskID::FailDomain) == 1, "expecting byte");
            static_assert(sizeof(TVDiskID::FailRealm) == 1, "expecting byte");
            std::vector<ui8> failedDomainsPerRealm;
            for (const TVDisk& vdisk : VDisks) {
                if (vdisk.VDiskStatus != NKikimrBlobStorage::EVDiskStatus::READY) {
                    if (ErasureSpecies == TErasureType::ErasureMirror3dc) {
                        if (failedDomainsPerRealm.size() <= vdisk.VDiskId.FailRealm) {
                            failedDomainsPerRealm.resize(vdisk.VDiskId.FailRealm + 1);
                        }
                        failedDomainsPerRealm[vdisk.VDiskId.FailRealm]++;
                    }
                    ++MissingDisks;
                    if (vdisk.VDiskStatus == NKikimrBlobStorage::EVDiskStatus::INIT_PENDING) {
                        ++startingDisks;
                    }
                    if (vdisk.VDiskStatus == NKikimrBlobStorage::EVDiskStatus::REPLICATING) {
                        ++replicatingDisks;
                    }
                }
                allocated += vdisk.AllocatedSize;
                limit += vdisk.AllocatedSize + vdisk.AvailableSize;
                DiskSpace = std::max(DiskSpace, vdisk.DiskSpace);
            }
            if (MissingDisks == 0) {
                Overall = NKikimrViewer::EFlag::Green;
                State = "ok";
            } else {
                if (ErasureSpecies == TErasureType::ErasureNone) {
                    TString state;
                    Overall = NKikimrViewer::EFlag::Red;
                    if (MissingDisks == startingDisks) {
                        state = "starting";
                    } else {
                        state = "dead";
                    }
                    State = TStringBuilder() << state << ':' << MissingDisks;
                } else if (ErasureSpecies == TErasureType::Erasure4Plus2Block) {
                    TString state;
                    if (MissingDisks > 2) {
                        Overall = NKikimrViewer::EFlag::Red;
                        state = "dead";
                    } else if (MissingDisks == 2) {
                        Overall = NKikimrViewer::EFlag::Orange;
                        state = "degraded";
                    } else if (MissingDisks == 1) {
                        if (MissingDisks == replicatingDisks + startingDisks) {
                            Overall = NKikimrViewer::EFlag::Blue;
                            if (replicatingDisks) {
                                state = "replicating";
                            } else {
                                state = "starting";
                            }
                        } else {
                            Overall = NKikimrViewer::EFlag::Yellow;
                            state = "degraded";
                        }
                    }
                    State = TStringBuilder() << state << ':' << MissingDisks;
                } else if (ErasureSpecies == TErasureType::ErasureMirror3dc) {
                    std::sort(failedDomainsPerRealm.begin(), failedDomainsPerRealm.end(), std::greater<ui8>());
                    while (!failedDomainsPerRealm.empty() && failedDomainsPerRealm.back() == 0) {
                        failedDomainsPerRealm.pop_back();
                    }
                    TString state;
                    if (failedDomainsPerRealm.size() > 2 || (failedDomainsPerRealm.size() == 2 && failedDomainsPerRealm[1] > 1)) {
                        Overall = NKikimrViewer::EFlag::Red;
                        state = "dead";
                    } else if (failedDomainsPerRealm.size() == 2) {
                        Overall = NKikimrViewer::EFlag::Orange;
                        state = "degraded";
                    } else if (failedDomainsPerRealm.size()) {
                        if (MissingDisks == replicatingDisks + startingDisks) {
                            Overall = NKikimrViewer::EFlag::Blue;
                            if (replicatingDisks > startingDisks) {
                                state = "replicating";
                            } else {
                                state = "starting";
                            }
                        } else {
                            Overall = NKikimrViewer::EFlag::Yellow;
                            state = "degraded";
                        }
                    }
                    State = TStringBuilder() << state << ':' << PrintDomains(failedDomainsPerRealm);
                }
            }
            Used = allocated;
            Limit = limit;
            Usage = Limit ? 100.0 * Used / Limit : 0;
            if (Usage >= 95) {
                DiskSpace = std::max(DiskSpace, NKikimrViewer::EFlag::Red);
            } else if (Usage >= 90) {
                DiskSpace = std::max(DiskSpace, NKikimrViewer::EFlag::Orange);
            } else if (Usage >= 85) {
                DiskSpace = std::max(DiskSpace, NKikimrViewer::EFlag::Yellow);
            } else {
                DiskSpace = std::max(DiskSpace, NKikimrViewer::EFlag::Green);
            }
        }

        void CalcAvailable(const std::unordered_map<TPDiskId, TPDisk>& pDisks) {
            ui64 available = 0;
            for (const TVDisk& vdisk : VDisks) {
                auto itPDisk = pDisks.find(vdisk.VSlotId);
                if (itPDisk != pDisks.end()) {
                    available += std::min(itPDisk->second.GetSlotTotalSize() - vdisk.AllocatedSize, vdisk.AvailableSize);
                    DiskSpace = std::max(DiskSpace, vdisk.DiskSpace);
                }
            }
            Available = available;
        }

        void CalcReadWrite() {
            ui64 read = 0;
            ui64 write = 0;
            for (const TVDisk& vdisk : VDisks) {
                read += vdisk.Read;
                write += vdisk.Write;
            }
            Read = read;
            Write = write;
        }
    };

    std::vector<TGroup> Groups;
    std::unordered_map<TGroupId, TGroup*> GroupsByGroupId;
    std::unordered_map<TPDiskId, TPDisk> PDisks;
    std::unordered_map<TVSlotId, const NKikimrSysView::TVSlotInfo*> VSlotsByVSlotId;
    std::unordered_map<TVSlotId, const NKikimrWhiteboard::TVDiskStateInfo*> VDisksByVSlotId;
    std::unordered_map<TPDiskId, const NKikimrWhiteboard::TPDiskStateInfo*> PDisksByPDiskId;

    TFieldsType FieldsRequired;
    TFieldsType FieldsAvailable;
    const TFieldsType FieldsAll = TFieldsType().set();
    const TFieldsType FieldsBsGroups = TFieldsType().set(+EGroupFields::GroupId)
                                                    .set(+EGroupFields::Erasure)
                                                    .set(+EGroupFields::Usage)
                                                    .set(+EGroupFields::Used)
                                                    .set(+EGroupFields::Limit);
    const TFieldsType FieldsBsPools = TFieldsType().set(+EGroupFields::PoolName)
                                                   .set(+EGroupFields::Kind)
                                                   .set(+EGroupFields::MediaType)
                                                   .set(+EGroupFields::Encryption);
    const TFieldsType FieldsBsVSlots = TFieldsType().set(+EGroupFields::NodeId)
                                                    .set(+EGroupFields::PDiskId)
                                                    .set(+EGroupFields::VDisk);
    const TFieldsType FieldsBsPDisks = TFieldsType().set(+EGroupFields::PDisk);
    const TFieldsType FieldsGroupState = TFieldsType().set(+EGroupFields::Used)
                                                      .set(+EGroupFields::Limit)
                                                      .set(+EGroupFields::Available)
                                                      .set(+EGroupFields::Usage)
                                                      .set(+EGroupFields::MissingDisks)
                                                      .set(+EGroupFields::State);
    const TFieldsType FieldsHive = TFieldsType().set(+EGroupFields::AllocationUnits);
    const TFieldsType FieldsWbGroups = TFieldsType().set(+EGroupFields::GroupId)
                                                    .set(+EGroupFields::Erasure)
                                                    .set(+EGroupFields::PoolName)
                                                    .set(+EGroupFields::Encryption);
    const TFieldsType FieldsWbDisks = TFieldsType().set(+EGroupFields::NodeId)
                                                   .set(+EGroupFields::PDiskId)
                                                   .set(+EGroupFields::VDisk)
                                                   .set(+EGroupFields::PDisk)
                                                   .set(+EGroupFields::Read)
                                                   .set(+EGroupFields::Write);

    bool FieldsNeeded(TFieldsType fields) const {
        return (FieldsRequired & (fields & ~FieldsAvailable)).any();
    }

    bool NeedFilter = false;
    bool NeedSort = false;
    bool NeedLimit = false;
    ui64 TotalGroups = 0;
    ui64 FoundGroups = 0;

    static EGroupFields ParseEGroupFields(TStringBuf field) {
        EGroupFields result = EGroupFields::COUNT;
        if (field == "PoolName") {
            result = EGroupFields::PoolName;
        } else if (field == "Kind") {
            result = EGroupFields::Kind;
        } else if (field == "MediaType") {
            result = EGroupFields::MediaType;
        } else if (field == "Erasure") {
            result = EGroupFields::Erasure;
        } else if (field == "Degraded" || field == "MissingDisks") {
            result = EGroupFields::MissingDisks;
        } else if (field == "State") {
            result = EGroupFields::State;
        } else if (field == "Usage") {
            result = EGroupFields::Usage;
        } else if (field == "GroupId") {
            result = EGroupFields::GroupId;
        } else if (field == "Encryption") {
            result = EGroupFields::Encryption;
        } else if (field == "Used") {
            result = EGroupFields::Used;
        } else if (field == "Limit") {
            result = EGroupFields::Limit;
        } else if (field == "Read") {
            result = EGroupFields::Read;
        } else if (field == "Write") {
            result = EGroupFields::Write;
        } else if (field == "AllocationUnits") {
            result = EGroupFields::AllocationUnits;
        } else if (field == "VDisk") {
            result = EGroupFields::VDisk;
        } else if (field == "PDisk") {
            result = EGroupFields::PDisk;
        }
        return result;
    }

    TStorageGroups(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Database = params.Get("tenant");
        if (Database.empty()) {
            Database = params.Get("database");
        }
        if (!Database.empty()) {
            NeedFilter = true;
        }
        Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
        TString filterStoragePool = params.Get("pool");
        if (!filterStoragePool.empty()) {
            FilterStoragePools.emplace(filterStoragePool);
        }
        SplitIds(params.Get("node_id"), ',', FilterNodeIds);
        SplitIds(params.Get("pdisk_id"), ',', FilterPDiskIds);
        SplitIds(params.Get("group_id"), ',', FilterGroupIds);
        if (!FilterStoragePools.empty() || !FilterNodeIds.empty() || !FilterPDiskIds.empty() || !FilterGroupIds.empty()) {
            NeedFilter = true;
        }
        if (params.Has("filter")) {
            Filter = params.Get("filter");
            NeedFilter = true;
        }
        if (params.Get("with") == "missing") {
            With = EWith::MissingDisks;
            NeedFilter = true;
        } if (params.Get("with") == "space") {
            With = EWith::SpaceProblems;
            NeedFilter = true;
        }
        if (params.Has("offset")) {
            Offset = FromStringWithDefault<std::size_t>(params.Get("offset"), 0);
            NeedLimit = true;
        }
        if (params.Has("limit")) {
            Limit = FromStringWithDefault<std::size_t>(params.Get("limit"), std::numeric_limits<ui32>::max());
            NeedLimit = true;
        }
        TStringBuf sort = params.Get("sort");
        if (sort) {
            NeedSort = true;
            if (sort.StartsWith("-") || sort.StartsWith("+")) {
                ReverseSort = (sort[0] == '-');
                sort.Skip(1);
            }
            GroupSort = ParseEGroupFields(sort);
            FieldsRequired.set(+GroupSort);
        }
        bool whiteboardOnly = FromStringWithDefault<bool>(params.Get("whiteboard_only"), false);
        if (whiteboardOnly) {
            FieldsRequired |= FieldsWbGroups;
            FieldsRequired |= FieldsWbDisks;
            FallbackToWhiteboard = true;
        }
        bool bscOnly = FromStringWithDefault<bool>(params.Get("bsc_only"), false);
        if (bscOnly) {
            FieldsRequired |= FieldsBsGroups;
            FieldsRequired |= FieldsBsPools;
            FieldsRequired |= FieldsBsVSlots;
            FieldsRequired |= FieldsBsPDisks;
        }
        bool groupsOnly = FromStringWithDefault<bool>(params.Get("groups_only"), false);
        if (groupsOnly) {
            FieldsRequired.set(+EGroupFields::GroupId);
        }
        FillDisksFromWhiteboard = FromStringWithDefault<bool>(params.Get("fill_disks_from_whiteboard"), FillDisksFromWhiteboard);
        TString fieldsRequired = FromStringWithDefault<TString>(params.Get("fields_required"));
        if (!fieldsRequired.empty()) {
            if (fieldsRequired == "all") {
                FieldsRequired = FieldsAll;
            } else {
                TStringBuf source = fieldsRequired;
                for (TStringBuf value = source.NextTok(", "); !value.empty(); value = source.NextTok(", ")) {
                    EGroupFields field = ParseEGroupFields(value);
                    if (field != EGroupFields::COUNT) {
                        FieldsRequired.set(+field);
                    }
                }
            }
        }
        if (FieldsRequired.none()) {
            FieldsRequired = FieldsAll;
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    virtual void Bootstrap() {
        Direct |= TBase::Event->Get()->Request.GetUri().StartsWith("/node/"); // we're already forwarding
        Direct |= (Database == AppData()->TenantName) || Database.empty(); // we're already on the right node or don't use database filter

        if (Database && !Direct) {
            BLOG_TRACE("Requesting StateStorageEndpointsLookup for " << Database);
            RequestStateStorageEndpointsLookup(Database); // to find some dynamic node and redirect query there
        } else {
            if (Database) {
                DatabaseNavigateResult = MakeRequestSchemeCacheNavigate(Database, 0);
            }
            if (FallbackToWhiteboard) {
                RequestWhiteboard();
            } else {
                GetGroupsResponse = RequestBSControllerGroups();
                if (FieldsNeeded(FieldsBsPools)) {
                    GetStoragePoolsResponse = RequestBSControllerPools();
                }
                if (FieldsNeeded(FieldsBsVSlots)) {
                    GetVSlotsResponse = RequestBSControllerVSlots();
                }
                if (FieldsNeeded(FieldsBsPDisks)) {
                    GetPDisksResponse = RequestBSControllerPDisks();
                }
            }
        }
        TBase::Become(&TThis::StateWork);
        Schedule(TDuration::MilliSeconds(Timeout * 50 / 100), new TEvents::TEvWakeup(TimeoutBSC)); // 50% timeout (for bsc)
        Schedule(TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup(TimeoutFinal)); // timeout for the rest
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BLOG_TRACE("Received TEvBoardInfo");
        TBase::ReplyAndPassAway(MakeForward(GetNodesFromBoardReply(ev)));
    }

    void PassAway() override {
        std::vector<bool> passedNodes;
        for (const TNodeId nodeId : SubscriptionNodeIds) {
            if (passedNodes.size() <= nodeId) {
                passedNodes.resize(nodeId + 1);
            } else {
                if (passedNodes[nodeId]) {
                    continue;
                }
            }
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
            passedNodes[nodeId] = true;
        }
        TBase::PassAway();
    }

    void ApplyFilter() {
        if (NeedFilter) {
            if (!FilterGroupIds.empty()) {
                for (auto itGroup = Groups.begin(); itGroup != Groups.end(); ++itGroup) {
                    if (FilterGroupIds.count(itGroup->GroupId)) {
                        continue;
                    }
                    itGroup = Groups.erase(itGroup);
                }
                FilterGroupIds.clear();
                GroupsByGroupId.clear();
            }
            if (!FilterStoragePools.empty() && FieldsAvailable.test(+EGroupFields::PoolName)) {
                for (auto itGroup = Groups.begin(); itGroup != Groups.end(); ++itGroup) {
                    if (FilterStoragePools.count(itGroup->PoolName)) {
                        continue;
                    }
                    itGroup = Groups.erase(itGroup);
                }
                FilterStoragePools.clear();
                GroupsByGroupId.clear();
            }
            if (!FilterNodeIds.empty() && FieldsAvailable.test(+EGroupFields::NodeId)) {
                for (auto itGroup = Groups.begin(); itGroup != Groups.end(); ++itGroup) {
                    bool found = false;
                    for (const auto& vdisk : itGroup->VDisks) {
                        if (FilterNodeIds.count(vdisk.VSlotId.NodeId)) {
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        continue;
                    }
                    itGroup = Groups.erase(itGroup);
                }
                FilterNodeIds.clear();
                GroupsByGroupId.clear();
            }
            if (!FilterPDiskIds.empty() && FieldsAvailable.test(+EGroupFields::PDiskId)) {
                for (auto itGroup = Groups.begin(); itGroup != Groups.end(); ++itGroup) {
                    bool found = false;
                    for (const auto& vdisk : itGroup->VDisks) {
                        if (FilterPDiskIds.count(vdisk.VSlotId.PDiskId)) {
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        continue;
                    }
                    itGroup = Groups.erase(itGroup);
                }
                FilterPDiskIds.clear();
                GroupsByGroupId.clear();
            }
            if (With == EWith::MissingDisks && FieldsAvailable.test(+EGroupFields::MissingDisks)) {
                for (auto itGroup = Groups.begin(); itGroup != Groups.end(); ++itGroup) {
                    if (itGroup->MissingDisks != 0) {
                        continue;
                    }
                    itGroup = Groups.erase(itGroup);
                }
                With = EWith::Everything;
                GroupsByGroupId.clear();
            }
            if (With == EWith::SpaceProblems && FieldsAvailable.test(+EGroupFields::Usage)) {
                for (auto itGroup = Groups.begin(); itGroup != Groups.end(); ++itGroup) {
                    if (itGroup->Usage >= SpaceUsageProblem) {
                        continue;
                    }
                    itGroup = Groups.erase(itGroup);
                }
                With = EWith::Everything;
                GroupsByGroupId.clear();
            }
            if (!Filter.empty() && FieldsAvailable.test(+EGroupFields::PoolName) && FieldsAvailable.test(+EGroupFields::GroupId)) {
                for (auto itGroup = Groups.begin(); itGroup != Groups.end(); ++itGroup) {
                    if (itGroup->PoolName.Contains(Filter)) {
                        continue;
                    }
                    if (::ToString(itGroup->GroupId).Contains(Filter)) {
                        continue;
                    }
                    itGroup = Groups.erase(itGroup);
                }
                Filter.clear();
                GroupsByGroupId.clear();
            }
            NeedFilter = (With == EWith::Everything) && Filter.empty() && FilterNodeIds.empty() && FilterPDiskIds.empty() && FilterGroupIds.empty();
            FoundGroups = Groups.size();
        }
    }

    void ApplySort() {
        if (NeedSort && FieldsAvailable.test(+GroupSort)) {
            switch (GroupSort) {
                case EGroupFields::GroupId:
                    SortCollection(Groups, [](const TGroup& group) { return group.GroupId; }, ReverseSort);
                    break;
                case EGroupFields::Erasure:
                    SortCollection(Groups, [](const TGroup& group) { return group.Erasure; }, ReverseSort);
                    break;
                case EGroupFields::Usage:
                    SortCollection(Groups, [](const TGroup& group) { return group.Usage; }, ReverseSort);
                    break;
                case EGroupFields::Used:
                    SortCollection(Groups, [](const TGroup& group) { return group.Used; }, ReverseSort);
                    break;
                case EGroupFields::Limit:
                    SortCollection(Groups, [](const TGroup& group) { return group.Limit; }, ReverseSort);
                    break;
                case EGroupFields::Available:
                    SortCollection(Groups, [](const TGroup& group) { return group.Available; }, ReverseSort);
                    break;
                case EGroupFields::PoolName:
                    SortCollection(Groups, [](const TGroup& group) { return group.PoolName; }, ReverseSort);
                    break;
                case EGroupFields::Kind:
                    SortCollection(Groups, [](const TGroup& group) { return group.Kind; }, ReverseSort);
                    break;
                case EGroupFields::Encryption:
                    SortCollection(Groups, [](const TGroup& group) { return group.EncryptionMode; }, ReverseSort);
                    break;
                case EGroupFields::AllocationUnits:
                    SortCollection(Groups, [](const TGroup& group) { return group.AllocationUnits; }, ReverseSort);
                    break;
                case EGroupFields::MediaType:
                    SortCollection(Groups, [](const TGroup& group) { return group.MediaType; }, ReverseSort);
                    break;
                case EGroupFields::MissingDisks:
                    SortCollection(Groups, [](const TGroup& group) { return group.MissingDisks; }, ReverseSort);
                    break;
                case EGroupFields::Read:
                    SortCollection(Groups, [](const TGroup& group) { return group.Read; }, ReverseSort);
                    break;
                case EGroupFields::Write:
                    SortCollection(Groups, [](const TGroup& group) { return group.Write; }, ReverseSort);
                    break;
                case EGroupFields::State:
                    SortCollection(Groups, [](const TGroup& group) { return group.State; }, ReverseSort);
                    break;
                case EGroupFields::PDiskId:
                case EGroupFields::NodeId:
                case EGroupFields::PDisk:
                case EGroupFields::VDisk:
                case EGroupFields::COUNT:
                    break;
            }
            NeedSort = false;
            GroupsByGroupId.clear();
        }
    }

    void ApplyLimit() {
        if (!NeedFilter && !NeedSort && NeedLimit) {
            if (Offset) {
                Groups.erase(Groups.begin(), Groups.begin() + std::min(*Offset, Groups.size()));
                GroupsByGroupId.clear();
            }
            if (Limit) {
                Groups.resize(std::min(*Limit, Groups.size()));
                GroupsByGroupId.clear();
            }
            NeedLimit = false;
        }
    }

    void ApplyEverything() {
        ApplyFilter();
        ApplySort();
        ApplyLimit();
    }

    void CollectHiveData() {
        if (FieldsNeeded(FieldsHive)) {
            if (!Groups.empty()) {
                ui64 hiveId = AppData()->DomainsInfo->GetHive();
                if (hiveId != TDomainsInfo::BadTabletId) {
                    if (HiveStorageStats.count(hiveId) == 0) {
                        HiveStorageStats.emplace(hiveId, MakeRequestHiveStorageStats(hiveId));
                        ++HiveStorageStatsInFlight;
                    }
                }
            }
            for (const TGroup& group : Groups) {
                TPathId pathId(group.SchemeShardId, group.PathId);
                if (NavigateKeySetResult.count(pathId) == 0) {
                    ui64 cookie = NavigateKeySetResult.size();
                    NavigateKeySetResult.emplace(pathId, MakeRequestSchemeCacheNavigate(pathId, cookie));
                }
            }
        }
    }

    void RebuildGroupsByGroupId() {
        GroupsByGroupId.clear();
        for (TGroup& group : Groups) {
            GroupsByGroupId.emplace(group.GroupId, &group);
        }
    }

    static TPathId GetPathId(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ResultSet.size() == 1) {
            if (ev->Get()->Request->ResultSet.begin()->Self) {
                const auto& info = ev->Get()->Request->ResultSet.begin()->Self->Info;
                return TPathId(info.GetSchemeshardId(), info.GetPathId());
            }
            if (ev->Get()->Request->ResultSet.begin()->TableId) {
                return ev->Get()->Request->ResultSet.begin()->TableId.PathId;
            }
        }
        return {};
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        bool firstNavigate = (ev->Cookie == 0);
        TPathId pathId = GetPathId(ev);
        if (firstNavigate && DatabaseNavigateResult.has_value() && pathId) {
            NavigateKeySetResult.emplace(pathId, std::move(*DatabaseNavigateResult));
        }
        auto itNavigateKeySetResult = NavigateKeySetResult.find(pathId);
        if (itNavigateKeySetResult == NavigateKeySetResult.end()) {
            BLOG_W("Invalid NavigateKeySetResult PathId: " << pathId << " Path: " << CanonizePath(ev->Get()->Request->ResultSet.begin()->Path));
            return RequestDone();
        }
        auto& navigateResult(itNavigateKeySetResult->second);
        if (ev->Get()->Request->ResultSet.size() == 1) {
            if (ev->Get()->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                navigateResult.Set(std::move(ev));
            } else {
                navigateResult.Error(TStringBuilder() << "Error " << ev->Get()->Request->ResultSet.begin()->Status);
            }
        } else {
            navigateResult.Error(TStringBuilder() << "Invalid number of results: " << ev->Get()->Request->ResultSet.size());
        }
        if (navigateResult.IsOk()) {
            TString path = CanonizePath(navigateResult->Request->ResultSet.begin()->Path);
            TIntrusiveConstPtr<TSchemeCacheNavigate::TDomainDescription> domainDescription = navigateResult->Request->ResultSet.begin()->DomainDescription;
            TIntrusiveConstPtr<NSchemeCache::TDomainInfo> domainInfo = navigateResult->Request->ResultSet.begin()->DomainInfo;
            if (domainInfo != nullptr && domainDescription != nullptr) {
                if (FieldsNeeded(FieldsHive)) {
                    TTabletId hiveId = domainInfo->Params.GetHive();
                    if (hiveId != 0 && HiveStorageStats.count(hiveId) == 0) {
                        HiveStorageStats.emplace(hiveId, MakeRequestHiveStorageStats(hiveId));
                        ++HiveStorageStatsInFlight;
                    }
                }
                if (Database && firstNavigate) { // filter by database
                    for (const auto& storagePool : domainDescription->Description.GetStoragePools()) {
                        TString storagePoolName = storagePool.GetName();
                        FilterStoragePools.emplace(storagePoolName);
                    }
                }
            }
        }
        RequestDone();
    }

    void Handle(TEvHive::TEvResponseHiveStorageStats::TPtr& ev) {
        auto itHiveStorageStats = HiveStorageStats.find(ev->Cookie);
        if (itHiveStorageStats != HiveStorageStats.end()) {
            itHiveStorageStats->second.Set(std::move(ev));
            if (GroupsByGroupId.empty()) {
                RebuildGroupsByGroupId();
            }
            for (const auto& pbPool : itHiveStorageStats->second->Record.GetPools()) {
                for (const auto& pbGroup : pbPool.GetGroups()) {
                    auto itGroup = GroupsByGroupId.find(pbGroup.GetGroupID());
                    if (itGroup != GroupsByGroupId.end()) {
                        itGroup->second->AllocationUnits += pbGroup.GetAcquiredUnits();
                    }
                }
            }

        }
        if (--HiveStorageStatsInFlight == 0) {
            FieldsAvailable |= FieldsHive;
            ApplyEverything();
        }
        RequestDone();
    }

    static TString GetMediaType(const TString& mediaType) {
        if (mediaType.StartsWith("Type:")) {
            return mediaType.substr(5);
        }
        return mediaType;
    }

    void FillVDiskFromVSlotInfo(TVDisk& vDisk, TVSlotId vSlotId, const NKikimrSysView::TVSlotInfo& info) {
        vDisk.VDiskId = TVDiskID(info.GetGroupId(),
                                info.GetGroupGeneration(),
                                static_cast<ui8>(info.GetFailRealm()),
                                static_cast<ui8>(info.GetFailDomain()),
                                static_cast<ui8>(info.GetVDisk()));
        vDisk.VSlotId = vSlotId;
        vDisk.AllocatedSize = info.GetAllocatedSize();
        vDisk.AvailableSize = info.GetAvailableSize();
        //vDisk.Kind = info.GetKind();
        vDisk.Status = info.GetStatusV2();
        NKikimrBlobStorage::EVDiskStatus_Parse(info.GetStatusV2(), &vDisk.VDiskStatus);
    }

    void ProcessBSControllerResponses() {
        if (GetGroupsResponse && GetGroupsResponse->IsOk() && FieldsNeeded(FieldsBsGroups)) {
            Groups.reserve(GetGroupsResponse->Get()->Record.EntriesSize());
            for (const NKikimrSysView::TGroupEntry& entry : GetGroupsResponse->Get()->Record.GetEntries()) {
                const NKikimrSysView::TGroupInfo& info = entry.GetInfo();
                TGroup& group = Groups.emplace_back();
                group.GroupId = entry.GetKey().GetGroupId();
                group.GroupGeneration = info.GetGeneration();
                group.BoxId = info.GetBoxId();
                group.PoolId = info.GetStoragePoolId();
                group.Erasure = info.GetErasureSpeciesV2();
                group.ErasureSpecies = TErasureType::ErasureSpeciesByName(group.Erasure);
                //group.Used = info.GetAllocatedSize();
                //group.Limit = info.GetAllocatedSize() + info.GetAvailableSize();
                //group.Usage = group.Limit ? 100.0 * group.Used / group.Limit : 0;
                group.PutTabletLogLatency = info.GetPutTabletLogLatency();
                group.PutUserDataLatency = info.GetPutUserDataLatency();
                group.GetFastLatency = info.GetGetFastLatency();
            }
            GroupsByGroupId.clear();
            FoundGroups = TotalGroups = Groups.size();
            FieldsAvailable |= FieldsBsGroups;
            ApplyEverything();
            RequestDone();
        }
        if (FieldsAvailable.test(+EGroupFields::GroupId) && GetStoragePoolsResponse && GetStoragePoolsResponse->IsOk() && FieldsNeeded(FieldsBsPools)) {
            std::unordered_map<std::pair<ui64, ui64>, const NKikimrSysView::TStoragePoolInfo*> indexStoragePool; // (box, id) -> pool
            for (const NKikimrSysView::TStoragePoolEntry& entry : GetStoragePoolsResponse->Get()->Record.GetEntries()) {
                const auto& key = entry.GetKey();
                const NKikimrSysView::TStoragePoolInfo& pool = entry.GetInfo();
                indexStoragePool.emplace(std::make_pair(key.GetBoxId(), key.GetStoragePoolId()), &pool);
            }
            ui64 rootSchemeshardId = AppData()->DomainsInfo->Domain->SchemeRoot;
            for (TGroup& group : Groups) {
                if (group.BoxId == 0 && group.PoolId == 0) {
                    group.PoolName = "static";
                    group.Kind = ""; // TODO ?
                    group.MediaType = ""; // TODO ?
                    group.SchemeShardId = rootSchemeshardId;
                    group.PathId = 1;
                    group.EncryptionMode = 0; // TODO ?
                } else {
                    auto itStoragePool = indexStoragePool.find({group.BoxId, group.PoolId});
                    if (itStoragePool != indexStoragePool.end()) {
                        const NKikimrSysView::TStoragePoolInfo* pool = itStoragePool->second;
                        group.PoolName = pool->GetName();
                        group.Kind = pool->GetKind();
                        group.SchemeShardId = pool->GetSchemeshardId();
                        group.PathId = pool->GetPathId();
                        group.MediaType = GetMediaType(pool->GetPDiskFilter());
                        if (!group.Erasure) {
                            group.Erasure = pool->GetErasureSpeciesV2();
                            group.ErasureSpecies = TErasureType::ErasureSpeciesByName(group.Erasure);
                        }
                        group.EncryptionMode = pool->GetEncryptionMode();
                    } else {
                        BLOG_W("Storage pool not found for group " << group.GroupId << " box " << group.BoxId << " pool " << group.PoolId);
                    }
                }
            }
            FieldsAvailable |= FieldsBsPools;
            ApplyEverything();
            CollectHiveData();
            RequestDone();
        }
        if (FieldsAvailable.test(+EGroupFields::GroupId) && GetVSlotsResponse && GetVSlotsResponse->IsOk() && FieldsNeeded(FieldsBsVSlots)) {
            if (GroupsByGroupId.empty()) {
                RebuildGroupsByGroupId();
            }
            for (const NKikimrSysView::TVSlotEntry& entry : GetVSlotsResponse->Get()->Record.GetEntries()) {
                const NKikimrSysView::TVSlotKey& key = entry.GetKey();
                const NKikimrSysView::TVSlotInfo& info = entry.GetInfo();
                VSlotsByVSlotId[key] = &info;
                auto itGroup = GroupsByGroupId.find(info.GetGroupId());
                if (itGroup != GroupsByGroupId.end() && itGroup->second->GroupGeneration == info.GetGroupGeneration()) {
                    TGroup& group = *itGroup->second;
                    TVDisk& vDisk = group.VDisks.emplace_back();
                    FillVDiskFromVSlotInfo(vDisk, key, info);
                    group.VDiskNodeIds.push_back(vDisk.VSlotId.NodeId);
                }
            }
            FieldsAvailable |= FieldsBsVSlots;
            ApplyEverything();
            for (TGroup& group : Groups) {
                group.CalcState();
            }
            ApplyEverything();
            if (FieldsAvailable.test(+EGroupFields::PDisk) && FieldsNeeded(+EGroupFields::Available)) {
                for (TGroup& group : Groups) {
                    group.CalcAvailable(PDisks);
                }
                FieldsAvailable.set(+EGroupFields::Available);
                ApplyEverything();
            }
            if (FieldsNeeded(FieldsWbDisks)) {
                for (TGroup& group : Groups) {
                    for (TNodeId nodeId : group.VDiskNodeIds) {
                        SendWhiteboardDisksRequest(nodeId);
                    }
                }
            }
            RequestDone();
        }
        if (GetPDisksResponse && GetPDisksResponse->IsOk() && FieldsNeeded(FieldsBsPDisks)) {
            for (const NKikimrSysView::TPDiskEntry& entry : GetPDisksResponse->Get()->Record.GetEntries()) {
                const NKikimrSysView::TPDiskKey& key = entry.GetKey();
                const NKikimrSysView::TPDiskInfo& info = entry.GetInfo();
                TPDisk& pDisk = PDisks[key];
                pDisk.PDiskId = key.GetPDiskId();
                pDisk.NodeId = key.GetNodeId();
                pDisk.SetCategory(info.GetCategory());
                pDisk.Path = info.GetPath();
                pDisk.Guid = info.GetGuid();
                pDisk.AvailableSize = info.GetAvailableSize();
                pDisk.TotalSize = info.GetTotalSize();
                pDisk.Status = info.GetStatusV2();
                pDisk.StatusChangeTimestamp = TInstant::MicroSeconds(info.GetStatusChangeTimestamp());
                pDisk.EnforcedDynamicSlotSize = info.GetEnforcedDynamicSlotSize();
                pDisk.ExpectedSlotCount = info.GetExpectedSlotCount();
                pDisk.NumActiveSlots = info.GetNumActiveSlots();
                pDisk.Category = info.GetCategory();
                pDisk.DecommitStatus = info.GetDecommitStatus();
            }
            FieldsAvailable |= FieldsBsPDisks;
            if (FieldsAvailable.test(+EGroupFields::VDisk) && FieldsNeeded(+EGroupFields::Available)) {
                for (TGroup& group : Groups) {
                    group.CalcAvailable(PDisks);
                }
                FieldsAvailable.set(+EGroupFields::Available);
                ApplyEverything();
            }
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        GetGroupsResponse->Set(std::move(ev));
        if (FallbackToWhiteboard) {
            RequestDone();
            return;
        }
        ProcessBSControllerResponses();
    }

    void Handle(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        GetStoragePoolsResponse->Set(std::move(ev));
        if (FallbackToWhiteboard) {
            RequestDone();
            return;
        }
        ProcessBSControllerResponses();
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        GetVSlotsResponse->Set(std::move(ev));
        if (FallbackToWhiteboard) {
            RequestDone();
            return;
        }
        ProcessBSControllerResponses();
    }

    void Handle(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        GetPDisksResponse->Set(std::move(ev));
        if (FallbackToWhiteboard) {
            RequestDone();
            return;
        }
        ProcessBSControllerResponses();
    }

    void RequestNodesList() {
        if (!NodesInfo.has_value()) {
            NodesInfo = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        NodesInfo->Set(std::move(ev));
        ui32 maxAllowedNodeId = std::numeric_limits<ui32>::max();
        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        if (dynamicNameserviceConfig) {
            maxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
        }
        for (const auto& ni : NodesInfo->Get()->Nodes) {
            if (ni.NodeId <= maxAllowedNodeId) {
                SendWhiteboardGroupRequest(ni.NodeId);
            }
        }
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvBSGroupStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        BSGroupStateResponse[nodeId].Set(std::move(ev));
        BSGroupRequestDone();
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        auto& vDiskStateResponse = VDiskStateResponse[nodeId];
        vDiskStateResponse.Set(std::move(ev));
        for (const NKikimrWhiteboard::TVDiskStateInfo& info : vDiskStateResponse->Record.GetVDiskStateInfo()) {
            for (const auto& vSlotId : info.GetDonors()) {
                SendWhiteboardDisksRequest(vSlotId.GetNodeId());
            }
        }

        VDiskRequestDone();
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        PDiskStateResponse[nodeId].Set(std::move(ev));
        PDiskRequestDone();
        RequestDone();
    }

    void ProcessWhiteboardGroups() {
        std::unordered_map<ui32, const NKikimrWhiteboard::TBSGroupStateInfo*> latestGroupInfo;
        for (const auto& [nodeId, bsGroupStateResponse] : BSGroupStateResponse) {
            if (bsGroupStateResponse.IsOk()) {
                for (const NKikimrWhiteboard::TBSGroupStateInfo& info : bsGroupStateResponse->Record.GetBSGroupStateInfo()) {
                    TString storagePoolName = info.GetStoragePoolName();
                    if (storagePoolName.empty()) {
                        continue;
                    }
                    if (info.VDiskNodeIdsSize() == 0) {
                        continue;
                    }
                    auto itLatest = latestGroupInfo.find(info.GetGroupID());
                    if (itLatest == latestGroupInfo.end()) {
                        latestGroupInfo.emplace(info.GetGroupID(), &info);
                    } else {
                        if (info.GetGroupGeneration() > itLatest->second->GetGroupGeneration()) {
                            itLatest->second = &info;
                        }
                    }
                }
            }
        }
        Groups.reserve(latestGroupInfo.size()); // to keep cache stable after emplace
        RebuildGroupsByGroupId();
        size_t capacity = Groups.capacity();
        for (const auto& [groupId, info] : latestGroupInfo) {
            auto itGroup = GroupsByGroupId.find(groupId);
            if (itGroup == GroupsByGroupId.end()) {
                TGroup& group = Groups.emplace_back();
                group.GroupId = groupId;
                group.GroupGeneration = info->GetGroupGeneration();
                group.Erasure = info->GetErasureSpecies();
                group.ErasureSpecies = TErasureType::ErasureSpeciesByName(group.Erasure);
                group.PoolName = info->GetStoragePoolName();
                group.EncryptionMode = info->GetEncryption();
                for (auto nodeId : info->GetVDiskNodeIds()) {
                    group.VDiskNodeIds.push_back(nodeId);
                }
                if (capacity != Groups.capacity()) {
                    // we expect to never do this
                    RebuildGroupsByGroupId();
                    capacity = Groups.capacity();
                }
            } else {
                TGroup& group = *itGroup->second;
                if (group.VDiskNodeIds.empty()) {
                    for (auto nodeId : info->GetVDiskNodeIds()) {
                        group.VDiskNodeIds.push_back(nodeId);
                    }
                }
            }
        }
        FieldsAvailable |= FieldsWbGroups;
        FoundGroups = TotalGroups = Groups.size();
        ApplyEverything();
        if (FieldsNeeded(FieldsWbDisks)) {
            std::unordered_set<TNodeId> nodeIds;
            for (const TGroup& group : Groups) {
                for (const TVDisk& vdisk : group.VDisks) {
                    TNodeId nodeId = vdisk.VSlotId.NodeId;
                    if (nodeIds.insert(nodeId).second) {
                        SendWhiteboardDisksRequest(nodeId);
                    }
                }
            }
        }
    }

    void FillVDiskFromVDiskStateInfo(TVDisk& vDisk, const TVSlotId& vSlotId, const NKikimrWhiteboard::TVDiskStateInfo& info) {
        vDisk.VDiskId = VDiskIDFromVDiskID(info.GetVDiskId());
        vDisk.VSlotId = vSlotId;
        if (!vDisk.AllocatedSize) {
            vDisk.AllocatedSize = info.GetAllocatedSize();
        }
        if (!vDisk.AvailableSize) {
            vDisk.AvailableSize = info.GetAvailableSize();
        }
        if (!vDisk.Read) {
            vDisk.Read = info.GetReadThroughput();
        }
        if (!vDisk.Write) {
            vDisk.Write = info.GetWriteThroughput();
        }
        if (!vDisk.Status) {
            switch (info.GetVDiskState()) {
                case NKikimrWhiteboard::EVDiskState::Initial:
                case NKikimrWhiteboard::EVDiskState::SyncGuidRecovery:
                    vDisk.VDiskStatus = NKikimrBlobStorage::EVDiskStatus::INIT_PENDING;
                    break;
                case NKikimrWhiteboard::EVDiskState::LocalRecoveryError:
                case NKikimrWhiteboard::EVDiskState::SyncGuidRecoveryError:
                case NKikimrWhiteboard::EVDiskState::PDiskError:
                    vDisk.VDiskStatus = NKikimrBlobStorage::EVDiskStatus::ERROR;
                    break;
                case NKikimrWhiteboard::EVDiskState::OK:
                    vDisk.VDiskStatus = info.GetReplicated() ? NKikimrBlobStorage::EVDiskStatus::READY : NKikimrBlobStorage::EVDiskStatus::REPLICATING;
                    break;
            }
            vDisk.Status = NKikimrBlobStorage::EVDiskStatus_Name(vDisk.VDiskStatus);
        }
        vDisk.DiskSpace = static_cast<NKikimrViewer::EFlag>(info.GetDiskSpace());
        vDisk.Donor = info.GetDonorMode();
        for (auto& donor : info.GetDonors()) {
            vDisk.Donors.emplace_back(donor);
        }
    }

    void ProcessWhiteboardDisks() {
        if (GroupsByGroupId.empty()) {
            RebuildGroupsByGroupId();
        }
        for (const auto& [nodeId, vDiskStateResponse] : VDiskStateResponse) {
            if (vDiskStateResponse.IsOk()) {
                for (const NKikimrWhiteboard::TVDiskStateInfo& info : vDiskStateResponse->Record.GetVDiskStateInfo()) {
                    TVSlotId vSlotId(nodeId, info.GetPDiskId(), info.GetVDiskSlotId());
                    VDisksByVSlotId[vSlotId] = &info;
                    ui32 groupId = info.GetVDiskId().GetGroupID();
                    ui32 groupGeneration = info.GetVDiskId().GetGroupGeneration();
                    auto itGroup = GroupsByGroupId.find(groupId);
                    if (itGroup != GroupsByGroupId.end() && itGroup->second->GroupGeneration == groupGeneration) {
                        TGroup& group = *(itGroup->second);
                        TVDisk* vDisk = nullptr;
                        TVDiskID vDiskId = VDiskIDFromVDiskID(info.GetVDiskId());
                        for (TVDisk& disk : group.VDisks) {
                            if (disk.VDiskId.SameDisk(vDiskId)) {
                                vDisk = &disk;
                                break;
                            }
                        }
                        if (vDisk == nullptr) {
                            vDisk = &(group.VDisks.emplace_back());
                            vDisk->VDiskId = vDiskId;
                        }
                        FillVDiskFromVDiskStateInfo(*vDisk, vSlotId, info);
                    }
                }
            }
        }
        for (const auto& [nodeId, pDiskStateResponse] : PDiskStateResponse) {
            if (pDiskStateResponse.IsOk()) {
                for (const NKikimrWhiteboard::TPDiskStateInfo& info : pDiskStateResponse->Record.GetPDiskStateInfo()) {
                    PDisksByPDiskId[TPDiskId(nodeId, info.GetPDiskId())] = &info;
                    TPDisk& pDisk = PDisks[{nodeId, info.GetPDiskId()}];
                    pDisk.PDiskId = info.GetPDiskId();
                    pDisk.NodeId = nodeId;
                    //pDisk.Type = info.GetType();
                    //pDisk.Kind = info.GetKind();
                    pDisk.Path = info.GetPath();
                    pDisk.Guid = info.GetGuid();
                    pDisk.AvailableSize = info.GetAvailableSize();
                    pDisk.TotalSize = info.GetTotalSize();
                    //pDisk.Status = info.GetStatus();
                    if (pDisk.EnforcedDynamicSlotSize < info.GetEnforcedDynamicSlotSize()) {
                        pDisk.EnforcedDynamicSlotSize = info.GetEnforcedDynamicSlotSize();
                    }
                    if (pDisk.ExpectedSlotCount < info.GetExpectedSlotCount()) {
                        pDisk.ExpectedSlotCount = info.GetExpectedSlotCount();
                    }
                    if (pDisk.NumActiveSlots < info.GetNumActiveSlots()) {
                        pDisk.NumActiveSlots = info.GetNumActiveSlots();
                    }
                    pDisk.SetCategory(info.GetCategory());
                    //pDisk.DecommitStatus = info.GetDecommitStatus();
                    float usage = pDisk.TotalSize ? 100.0 * (pDisk.TotalSize - pDisk.AvailableSize) / pDisk.TotalSize : 0;
                    if (usage >= 95) {
                        pDisk.DiskSpace = NKikimrViewer::EFlag::Red;
                    } else if (usage >= 90) {
                        pDisk.DiskSpace = NKikimrViewer::EFlag::Orange;
                    } else if (usage >= 85) {
                        pDisk.DiskSpace = NKikimrViewer::EFlag::Yellow;
                    } else {
                        pDisk.DiskSpace = NKikimrViewer::EFlag::Green;
                    }
                }
            }
        }
        FieldsAvailable |= FieldsWbDisks;
        for (TGroup& group : Groups) {
            group.CalcReadWrite();
        }
        ApplyEverything();
        if (FieldsNeeded(+EGroupFields::Available)) {
            for (TGroup& group : Groups) {
                group.CalcAvailable(PDisks);
            }
            FieldsAvailable.set(+EGroupFields::Available);
            ApplyEverything();
        }
        if (FieldsNeeded(FieldsGroupState)) {
            for (TGroup& group : Groups) {
                group.CalcState();
            }
            FieldsAvailable |= FieldsGroupState;
            ApplyEverything();
        }
    }

    void BSGroupRequestDone() {
        if (--BSGroupStateRequestsInFlight == 0) {
            ProcessWhiteboardGroups();
        }
    }

    void VDiskRequestDone() {
        --VDiskStateRequestsInFlight;
        if (VDiskStateRequestsInFlight == 0 && PDiskStateRequestsInFlight == 0) {
            ProcessWhiteboardDisks();
        }
    }

    void PDiskRequestDone() {
        --PDiskStateRequestsInFlight;
        if (VDiskStateRequestsInFlight == 0 && PDiskStateRequestsInFlight == 0) {
            ProcessWhiteboardDisks();
        }
    }

    void SendWhiteboardGroupRequest(ui32 nodeId) {
        if (BSGroupStateResponse.count(nodeId) == 0) {
            TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
            BSGroupStateResponse.emplace(nodeId, MakeRequest<TEvWhiteboard::TEvBSGroupStateResponse>(whiteboardServiceId,
                new TEvWhiteboard::TEvBSGroupStateRequest(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                nodeId));
            SubscriptionNodeIds.push_back(nodeId);
            ++BSGroupStateRequestsInFlight;
        }
    }

    void SendWhiteboardDisksRequest(ui32 nodeId) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        if (VDiskStateResponse.count(nodeId) == 0) {
            VDiskStateResponse.emplace(nodeId, MakeRequest<TEvWhiteboard::TEvVDiskStateResponse>(whiteboardServiceId,
                new TEvWhiteboard::TEvVDiskStateRequest(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                nodeId));
            ++VDiskStateRequestsInFlight;
            SubscriptionNodeIds.push_back(nodeId);
        }
        if (PDiskStateResponse.count(nodeId) == 0) {
            PDiskStateResponse.emplace(nodeId, MakeRequest<TEvWhiteboard::TEvPDiskStateResponse>(whiteboardServiceId,
                new TEvWhiteboard::TEvPDiskStateRequest(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                nodeId));
            ++PDiskStateRequestsInFlight;
            SubscriptionNodeIds.push_back(nodeId);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        {
            auto itVDiskStateResponse = VDiskStateResponse.find(nodeId);
            if (itVDiskStateResponse != VDiskStateResponse.end()) {
                if (itVDiskStateResponse->second.Error("NodeDisconnected")) {
                    VDiskRequestDone();
                    RequestDone();
                }
            }
        }
        {
            auto itPDiskStateResponse = PDiskStateResponse.find(nodeId);
            if (itPDiskStateResponse != PDiskStateResponse.end()) {
                if (itPDiskStateResponse->second.Error("NodeDisconnected")) {
                    PDiskRequestDone();
                    RequestDone();
                }
            }
        }
        {
            auto itBSGroupStateResponse = BSGroupStateResponse.find(nodeId);
            if (itBSGroupStateResponse != BSGroupStateResponse.end()) {
                if (itBSGroupStateResponse->second.Error("NodeDisconnected")) {
                    BSGroupRequestDone();
                    RequestDone();
                }
            }
        }
    }

    void RequestWhiteboard() {
        FallbackToWhiteboard = true;
        RequestNodesList();
    }

    void OnBscError(const TString& error) {
        if (GetGroupsResponse.has_value()) {
            GetGroupsResponse->Error(error);
        }
        if (GetStoragePoolsResponse.has_value()) {
            GetStoragePoolsResponse->Error(error);
        }
        if (GetVSlotsResponse.has_value()) {
            GetVSlotsResponse->Error(error);
        }
        if (GetPDisksResponse.has_value()) {
            GetPDisksResponse->Error(error);
        }
        RequestWhiteboard();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TString error = TStringBuilder() << "Failed to establish pipe: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status);
            auto it = HiveStorageStats.find(ev->Get()->TabletId);
            if (it != HiveStorageStats.end()) {
                it->second.Error(error);
                if (--HiveStorageStatsInFlight == 0) {
                    FieldsAvailable |= FieldsHive;
                }
            }
            if (ev->Get()->TabletId == GetBSControllerId()) {
                OnBscError(error);
            }
        }
        TBase::Handle(ev); // all RequestDone() are handled by base handler
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(NSysView::TEvSysView::TEvGetGroupsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetPDisksResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvHive::TEvResponseHiveStorageStats, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvents::TEvWakeup, HandleTimeout);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            IgnoreFunc(TEvents::TEvUndelivered/* , Undelivered */);
        }
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
            case TimeoutBSC:
                OnBscError("timeout");
                break;
            case TimeoutFinal:
                FilterNodeIds.clear();
                break;
        }
        ReplyAndPassAway();
    }

    void RenderVDisk(NKikimrViewer::TStorageVDisk& jsonVDisk, const TVDisk& vdisk) {
        jsonVDisk.SetVDiskId(vdisk.GetVDiskId());
        jsonVDisk.SetNodeId(vdisk.VSlotId.NodeId);
        jsonVDisk.SetAllocatedSize(vdisk.AllocatedSize);
        jsonVDisk.SetAvailableSize(vdisk.AvailableSize);
        jsonVDisk.SetStatus(vdisk.Status);
        if (vdisk.DiskSpace != NKikimrViewer::Grey) {
            jsonVDisk.SetDiskSpace(vdisk.DiskSpace);
        }
        auto itVDiskByVSlotId = VDisksByVSlotId.find(vdisk.VSlotId);
        if (itVDiskByVSlotId != VDisksByVSlotId.end()) {
            auto& whiteboard = *jsonVDisk.MutableWhiteboard();
            whiteboard.CopyFrom(*(itVDiskByVSlotId->second));
            if (whiteboard.GetReplicated() || (whiteboard.GetReplicationProgress() == NAN)) {
                whiteboard.ClearReplicationProgress();
                whiteboard.ClearReplicationSecondsRemaining();
            }
        }

        auto itPDisk = PDisks.find(vdisk.VSlotId);
        if (itPDisk != PDisks.end()) {
            const TPDisk& pdisk = itPDisk->second;
            NKikimrViewer::TStoragePDisk& jsonPDisk = *jsonVDisk.MutablePDisk();
            jsonPDisk.SetPDiskId(pdisk.GetPDiskId());
            jsonPDisk.SetPath(pdisk.Path);
            jsonPDisk.SetType(pdisk.Type);
            jsonPDisk.SetGuid(::ToString(pdisk.Guid));
            jsonPDisk.SetCategory(pdisk.Category);
            jsonPDisk.SetTotalSize(pdisk.TotalSize);
            jsonPDisk.SetAvailableSize(pdisk.AvailableSize);
            jsonPDisk.SetStatus(pdisk.Status);
            jsonPDisk.SetDecommitStatus(pdisk.DecommitStatus);
            jsonPDisk.SetSlotSize(pdisk.GetSlotTotalSize());
            if (pdisk.DiskSpace != NKikimrViewer::Grey) {
                jsonPDisk.SetDiskSpace(pdisk.DiskSpace);
            }
            auto itPDiskByPDiskId = PDisksByPDiskId.find(vdisk.VSlotId);
            if (itPDiskByPDiskId != PDisksByPDiskId.end()) {
                jsonPDisk.MutableWhiteboard()->CopyFrom(*(itPDiskByPDiskId->second));
            }
        }
        if (!vdisk.Donors.empty()) {
            for (const TVSlotId& donorId : vdisk.Donors) {
                NKikimrViewer::TStorageVDisk& jsonDonor = *jsonVDisk.AddDonors();
                TVDisk donor;
                auto itVSlotInfo = VSlotsByVSlotId.find(donorId);
                if (itVSlotInfo != VSlotsByVSlotId.end()) {
                    FillVDiskFromVSlotInfo(donor, donorId, *(itVSlotInfo->second));
                }
                auto itVDiskInfo = VDisksByVSlotId.find(donorId);
                if (itVDiskInfo != VDisksByVSlotId.end()) {
                    FillVDiskFromVDiskStateInfo(donor, donorId, *(itVDiskInfo->second));
                }
                RenderVDisk(jsonDonor, donor);
            }
        }
    }

    void ReplyAndPassAway() {
        ApplyEverything();
        NKikimrViewer::TStorageGroupsInfo json;
        json.SetVersion(1);
        json.SetFieldsAvailable(FieldsAvailable.to_ulong());
        json.SetFieldsRequired(FieldsRequired.to_ulong());
        if (NeedFilter) {
            json.SetNeedFilter(true);
        }
        if (NeedSort) {
            json.SetNeedSort(true);
        }
        if (NeedLimit) {
            json.SetNeedLimit(true);
        }
        json.SetTotalGroups(TotalGroups);
        json.SetFoundGroups(FoundGroups);
        for (const TGroup& group : Groups) {
            NKikimrViewer::TStorageGroupInfo& jsonGroup = *json.AddStorageGroups();
            jsonGroup.SetGroupId(::ToString(group.GroupId));
            if (group.GroupGeneration) {
                jsonGroup.SetGroupGeneration(group.GroupGeneration);
            }
            if (FieldsAvailable.test(+EGroupFields::PoolName)) {
                jsonGroup.SetPoolName(group.PoolName);
            }
            for (const TVDisk& vdisk : group.VDisks) {
                RenderVDisk(*jsonGroup.AddVDisks(), vdisk);
            }
            if (FieldsAvailable.test(+EGroupFields::Encryption)) {
                jsonGroup.SetEncryption(group.EncryptionMode);
            }
            if (group.Overall != NKikimrViewer::Grey) {
                jsonGroup.SetOverall(group.Overall);
            }
            if (group.DiskSpace != NKikimrViewer::Grey) {
                jsonGroup.SetDiskSpace(group.DiskSpace);
            }
            if (FieldsAvailable.test(+EGroupFields::Kind)) {
                jsonGroup.SetKind(group.Kind);
            }
            if (FieldsAvailable.test(+EGroupFields::MediaType)) {
                jsonGroup.SetMediaType(group.MediaType);
            }
            if (FieldsAvailable.test(+EGroupFields::Erasure)) {
                jsonGroup.SetErasureSpecies(group.Erasure);
            }
            if (FieldsAvailable.test(+EGroupFields::AllocationUnits)) {
                jsonGroup.SetAllocationUnits(group.AllocationUnits);
            }
            if (FieldsAvailable.test(+EGroupFields::State)) {
                jsonGroup.SetState(group.State);
            }
            if (FieldsAvailable.test(+EGroupFields::MissingDisks)) {
                jsonGroup.SetMissingDisks(group.MissingDisks);
            }
            if (FieldsAvailable.test(+EGroupFields::Used)) {
                jsonGroup.SetUsed(group.Used);
            }
            if (FieldsAvailable.test(+EGroupFields::Limit)) {
                jsonGroup.SetLimit(group.Limit);
            }
            if (FieldsAvailable.test(+EGroupFields::Read)) {
                jsonGroup.SetRead(group.Read);
            }
            if (FieldsAvailable.test(+EGroupFields::Write)) {
                jsonGroup.SetWrite(group.Write);
            }
            if (FieldsAvailable.test(+EGroupFields::Usage)) {
                jsonGroup.SetUsage(group.Usage);
            }
            if (FieldsAvailable.test(+EGroupFields::Available)) {
                jsonGroup.SetAvailable(group.Available);
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
};

template <>
YAML::Node TJsonRequestSwagger<TStorageGroups>::GetSwagger() {
    YAML::Node node = YAML::Load(R"___(
        post:
          tags:
          - storage
          summary: Storage groups
          description: Information about storage groups
          parameters:
              - name: database
                in: query
                description: database name
                required: false
                type: string
              - name: pool
                in: query
                description: storage pool name
                required: false
                type: string
              - name: node_id
                in: query
                description: node id
                required: false
                type: integer
              - name: pdisk_id
                in: query
                description: pdisk id
                required: false
                type: integer
              - name: group_id
                in: query
                description: group id
                required: false
                type: integer
              - name: need_groups
                in: query
                description: return groups information
                required: false
                type: boolean
                default: true
              - name: need_disks
                in: query
                description: return disks information
                required: false
                type: boolean
                default: true
              - name: with
                in: query
                description: >
                   filter groups by missing or space:
                    * `missing`
                    * `space`
                required: false
                type: string
              - name: sort
                in: query
                description: >
                  sort by:
                   * `PoolName`
                   * `Kind`
                   * `MediaType`
                   * `Erasure`
                   * `MissingDisks`
                   * `Usage`
                   * `GroupId`
                   * `Used`
                   * `Limit`
                   * `Usage`
                   * `Read`
                   * `Write`
                required: false
                type: string
              - name: offset
                in: query
                description: skip N nodes
                required: false
                type: integer
              - name: limit
                in: query
                description: limit to N nodes
                required: false
                type: integer
              - name: timeout
                in: query
                description: timeout in ms
                required: false
                type: integer
          responses:
            200:
              description: OK
              content:
                application/json:
                  schema:
                    type: object
                    description: format depends on schema parameter
            400:
              description: Bad Request
            403:
              description: Forbidden
            504:
              description: Gateway Timeout
            )___");
    node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TStorageGroupsInfo>();
    return node;
}


void InitStorageGroupsJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/storage/groups", new TJsonHandler<TStorageGroups>);
}

} // namespace NViewer
} // namespace NKikimr
