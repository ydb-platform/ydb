#pragma once
#include "json_storage_base.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

using ::google::protobuf::FieldDescriptor;

class TJsonStorage : public TJsonStorageBase {
    using TBase = TJsonStorageBase;
    using TThis = TJsonStorage;

    bool NeedGroups = true;
    bool NeedDisks = true;
    bool NeedDonors = true;

    enum class EGroupSort {
        PoolName,
        Kind,
        MediaType,
        Erasure,
        Degraded,
        Usage,
        GroupId,
        Used,
        Limit,
        Read,
        Write
    };
    enum class EVersion {
        v1,
        v2 // only this works with sorting, limiting and filtering with usage buckets
    };
    EVersion Version = EVersion::v1;
    EGroupSort GroupSort = EGroupSort::PoolName;
    bool ReverseSort = false;
    std::optional<ui32> Offset;
    std::optional<ui32> Limit;

    ui32 UsagePace = 5;
    TVector<ui32> UsageBuckets;

    struct TGroupRow {
        TString PoolName;
        TString GroupId;
        TString Kind;
        TString MediaType;
        TString Erasure;
        ui32 Degraded;
        float Usage;
        uint64 Used;
        uint64 Limit;
        uint64 Read;
        uint64 Write;

        TGroupRow()
            : Degraded(0)
            , Usage(0)
            , Used(0)
            , Limit(0)
            , Read(0)
            , Write(0)
        {}
    };
    THashMap<TString, TGroupRow> GroupRowsByGroupId;

public:
    TJsonStorage(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        NeedGroups = FromStringWithDefault<bool>(params.Get("need_groups"), true);
        NeedDisks = FromStringWithDefault<bool>(params.Get("need_disks"), NeedGroups);
        NeedDonors = FromStringWithDefault<bool>(params.Get("need_donors"), NeedDonors);
        NeedGroups = Max(NeedGroups, NeedDisks);
        UsagePace = FromStringWithDefault<uint32>(params.Get("usage_pace"), UsagePace);
        if (UsagePace == 0) {
            Send(Initiator, new NMon::TEvHttpInfoRes(Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request"), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            PassAway();
        }
        SplitIds(params.Get("usage_buckets"), ',', UsageBuckets);
        Sort(UsageBuckets);

        TString version = params.Get("version");
        if (version == "v1") {
            Version = EVersion::v1;
        } else if (version == "v2") {
            Version = EVersion::v2;
        }
        Offset = FromStringWithDefault<ui32>(params.Get("offset"), 0);
        Limit = FromStringWithDefault<ui32>(params.Get("limit"), std::numeric_limits<ui32>::max());
        TStringBuf sort = params.Get("sort");
        if (sort) {
            if (sort.StartsWith("-") || sort.StartsWith("+")) {
                ReverseSort = (sort[0] == '-');
                sort.Skip(1);
            }
            if (sort == "PoolName") {
                GroupSort = EGroupSort::PoolName;
            } else if (sort == "Kind") {
                GroupSort = EGroupSort::Kind;
            } else if (sort == "MediaType") {
                GroupSort = EGroupSort::MediaType;
            } else if (sort == "Erasure") {
                GroupSort = EGroupSort::Erasure;
            } else if (sort == "Degraded") {
                GroupSort = EGroupSort::Degraded;
            } else if (sort == "Usage") {
                GroupSort = EGroupSort::Usage;
            } else if (sort == "GroupId") {
                GroupSort = EGroupSort::GroupId;
            } else if (sort == "Used") {
                GroupSort = EGroupSort::Used;
            } else if (sort == "Limit") {
                GroupSort = EGroupSort::Limit;
            } else if (sort == "Read") {
                GroupSort = EGroupSort::Read;
            } else if (sort == "Write") {
                GroupSort = EGroupSort::Write;
            }
        }
    }

    void Bootstrap() override {
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        ui64 hiveId = domains->GetHive();
        if (hiveId != TDomainsInfo::BadTabletId) {
            RequestHiveStorageStats(hiveId);
        }
        TBase::Bootstrap();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TBase::Handle(ev, true);
    }

    void RemapGroup(IOutputStream& json,
                    const ::google::protobuf::Message& protoFrom,
                    const TJsonSettings& jsonSettings) {
        const auto& info = static_cast<const NKikimrViewer::TStorageGroupInfo&>(protoFrom);
        TString groupId = info.GetGroupId();
        if (Version == EVersion::v2) {
            const auto& groupRow = GroupRowsByGroupId[groupId];
            json << "\"PoolName\":\"" << groupRow.PoolName << "\",";
            json << "\"Kind\":\"" << groupRow.Kind << "\",";
            json << "\"MediaType\":\"" << groupRow.MediaType << "\",";
            json << "\"Erasure\":\"" << groupRow.Erasure << "\",";
            json << "\"Degraded\":\"" << groupRow.Degraded << "\",";
            json << "\"Usage\":\"" << groupRow.Usage << "\",";
            json << "\"Used\":\"" << groupRow.Used << "\",";
            json << "\"Limit\":\"" << groupRow.Limit << "\",";
            json << "\"Read\":\"" << groupRow.Read << "\",";
            json << "\"Write\":\"" << groupRow.Write << "\",";
        }
        auto ib = BSGroupIndex.find(groupId);
        if (ib != BSGroupIndex.end()) {
            TProtoToJson::ProtoToJsonInline(json, ib->second, jsonSettings);
            if (auto ih = BSGroupHiveIndex.find(groupId); ih != BSGroupHiveIndex.end()) {
                json << ',';
                TProtoToJson::ProtoToJsonInline(json, ih->second, jsonSettings);
            }
            if (auto io = BSGroupOverall.find(groupId); io != BSGroupOverall.end()) {
                json << ",\"Overall\":\"" << io->second << "\"";
            }
        }
    }

    void RemapVDisks(IOutputStream& json,
                     const ::google::protobuf::Message& protoFrom,
                     const TJsonSettings& jsonSettings) {
        NKikimrWhiteboard::EFlag diskSpace = NKikimrWhiteboard::Grey;
        json << "\"VDisks\":[";
        const auto& info = static_cast<const NKikimrWhiteboard::TBSGroupStateInfo&>(protoFrom);
        const auto& vDiskIds = info.GetVDiskIds();
        for (auto iv = vDiskIds.begin(); iv != vDiskIds.end(); ++iv) {
            if (iv != vDiskIds.begin()) {
                json << ',';
            }
            const NKikimrBlobStorage::TVDiskID& vDiskId = *iv;
            auto ie = VDisksIndex.find(vDiskId);
            if (ie != VDisksIndex.end()) {
                json << '{';
                TProtoToJson::ProtoToJsonInline(json, ie->second, jsonSettings);
                if (auto io = VDisksOverall.find(vDiskId); io != VDisksOverall.end()) {
                    json << ",\"Overall\":\"" << io->second << "\"";
                }
                json << '}';
                diskSpace = std::max(diskSpace, ie->second.GetDiskSpace());
            } else {
                json << "{\"VDiskId\":";
                TProtoToJson::ProtoToJson(json, vDiskId, jsonSettings);
                json << "}";
            }
        }
        json << ']';
        if (diskSpace != NKikimrWhiteboard::Grey) {
            json << ",\"DiskSpace\":\"";
            json << NKikimrWhiteboard::EFlag_Name(diskSpace);
            json << "\"";
        }
    }

    void RemapDonors(IOutputStream& json,
                     const ::google::protobuf::Message& protoFrom,
                     const TJsonSettings& jsonSettings) {
        const auto& info = static_cast<const NKikimrWhiteboard::TVDiskStateInfo&>(protoFrom);
        const auto& donors = info.GetDonors();
        if (donors.empty()) {
            return;
        }
        json << "\"Donors\":[";
        for (auto id = donors.begin(); id != donors.end(); ++id) {
            if (id != donors.begin()) {
                json << ',';
            }
            const NKikimrBlobStorage::TVSlotId& vSlotId = *id;
            auto ie = VSlotsIndex.find(vSlotId);
            if (ie != VSlotsIndex.end()) {
                json << '{';
                TProtoToJson::ProtoToJsonInline(json, ie->second, jsonSettings);
                json << '}';
            } else {
                json << "{";
                TProtoToJson::ProtoToJsonInline(json, vSlotId, jsonSettings);
                json << "}";
            }
        }
        json << ']';
    }

    void RemapPDisk(IOutputStream& json,
                    const ::google::protobuf::Message& protoFrom,
                    const TJsonSettings& jsonSettings) {
        json << "\"PDisk\":";
        const auto& info = static_cast<const NKikimrWhiteboard::TVDiskStateInfo&>(protoFrom);
        ui32 nodeId = info.GetNodeId();
        ui32 pDiskId = info.GetPDiskId();
        auto ie = PDisksIndex.find(std::make_pair(nodeId, pDiskId));
        if (ie != PDisksIndex.end()) {
            TProtoToJson::ProtoToJson(json, ie->second, jsonSettings);
            if (auto io = PDisksOverall.find(std::make_pair(nodeId, pDiskId)); io != PDisksOverall.end()) {
                json << ",\"Overall\":\"" << io->second << "\"";
            }
        } else {
            json << "{\"PDiskId\":" << pDiskId << ",\"NodeId\":" << nodeId << "}";
        }
    }

    bool CheckGroupFilters(const TString& groupId, const TString& poolName, const TGroupRow& groupRow) {
        if (!EffectiveGroupFilter.contains(groupId)) {
            return false;
        }
        switch (With) {
            case EWith::MissingDisks:
                if (BSGroupWithMissingDisks.count(groupId) == 0) {
                    return false;
                }
                break;
            case EWith::SpaceProblems:
                if (BSGroupWithSpaceProblems.count(groupId) == 0 && groupRow.Usage < 0.8) {
                    return false;
                }
                break;
            case EWith::Everything:
                break;
        }
        if (Filter) {
            if (poolName.Contains(Filter)) {
                return true;
            }
            if (groupId.Contains(Filter)) {
                return true;
            }
            return false;
        }
        return true;
    }

    void ReplyAndPassAway() override {
        if (CheckAdditionalNodesInfoNeeded()) {
            return;
        }
        CollectDiskInfo(true);
        ParsePDisksFromBaseConfig();
        ParseVDisksFromBaseConfig();

        for (const auto& [hiveId, hiveStats] : HiveStorageStats) {
            for (auto& pbPool : *hiveStats->Record.MutablePools()) {
                for (auto& pbGroup : *pbPool.MutableGroups()) {
                    TString groupId = ToString(pbGroup.GetGroupID());
                    NKikimrHive::THiveStorageGroupStats& stats = BSGroupHiveIndex[groupId];
                    stats.SetAcquiredUnits(stats.GetAcquiredUnits() + pbGroup.GetAcquiredUnits());
                    stats.SetAcquiredIOPS(stats.GetAcquiredIOPS() + pbGroup.GetAcquiredIOPS());
                    stats.SetAcquiredThroughput(stats.GetAcquiredThroughput() + pbGroup.GetAcquiredThroughput());
                    stats.SetAcquiredSize(stats.GetAcquiredSize() + pbGroup.GetAcquiredSize());
                    stats.SetMaximumIOPS(stats.GetMaximumIOPS() + pbGroup.GetMaximumIOPS());
                    stats.SetMaximumThroughput(stats.GetMaximumThroughput() + pbGroup.GetMaximumThroughput());
                    stats.SetMaximumSize(stats.GetMaximumSize() + pbGroup.GetMaximumSize());
                }
            }
        }
        ui64 foundGroups = 0;
        ui64 totalGroups = 0;
        TVector<TGroupRow> GroupRows;
        for (const auto& [poolName, poolInfo] : StoragePoolInfo) {
            if ((!FilterTenant.empty() || !FilterStoragePools.empty()) && FilterStoragePools.count(poolName) == 0) {
                continue;
            }
            NKikimrViewer::TStoragePoolInfo* pool = StorageInfo.AddStoragePools();
            for (TString groupId : poolInfo.Groups) {
                TGroupRow row;
                row.PoolName = poolName;
                row.GroupId = groupId;
                row.Kind = poolInfo.Kind;
                row.MediaType = poolInfo.MediaType;
                auto ib = BSGroupIndex.find(groupId);
                if (ib != BSGroupIndex.end()) {
                    row.Erasure = ib->second.GetErasureSpecies();
                    const auto& vDiskIds = ib->second.GetVDiskIds();
                    for (auto iv = vDiskIds.begin(); iv != vDiskIds.end(); ++iv) {
                        const NKikimrBlobStorage::TVDiskID& vDiskId = *iv;
                        auto ie = VDisksIndex.find(vDiskId);
                        bool degraded = false;
                        if (ie != VDisksIndex.end()) {
                            ui32 nodeId = ie->second.GetNodeId();
                            ui32 pDiskId = ie->second.GetPDiskId();
                            degraded |= !ie->second.GetReplicated() || ie->second.GetVDiskState() != NKikimrWhiteboard::EVDiskState::OK;
                            row.Used += ie->second.GetAllocatedSize();
                            row.Limit += ie->second.GetAllocatedSize() + ie->second.GetAvailableSize();
                            row.Read += ie->second.GetReadThroughput();
                            row.Write += ie->second.GetWriteThroughput();

                            auto ip = PDisksIndex.find(std::make_pair(nodeId, pDiskId));
                            if (ip != PDisksIndex.end()) {
                                degraded |= ip->second.GetState() != NKikimrBlobStorage::TPDiskState::Normal;
                                if (!ie->second.HasAvailableSize()) {
                                    row.Limit += ip->second.GetAvailableSize();
                                }
                            }
                        }
                        if (degraded) {
                            row.Degraded++;
                        }
                    }
                }
                row.Usage = row.Limit == 0 ? 100 : (float)row.Used / row.Limit;

                ++totalGroups;
                if (!CheckGroupFilters(groupId, poolName, row)) {
                    continue;
                }
                ++foundGroups;
                if (Version == EVersion::v1) {
                    pool->AddGroups()->SetGroupId(groupId);
                    pool->SetMediaType(poolInfo.MediaType);
                } else if (Version == EVersion::v2) {
                    if (!UsageBuckets.empty() && !BinarySearch(UsageBuckets.begin(), UsageBuckets.end(), (ui32)(row.Usage * 100) / UsagePace)) {
                        continue;
                    }
                    GroupRows.emplace_back(row);
                    GroupRowsByGroupId[groupId] = row;
                }
                auto itHiveGroup = BSGroupHiveIndex.find(groupId);
                if (itHiveGroup != BSGroupHiveIndex.end()) {
                    pool->SetAcquiredUnits(pool->GetAcquiredUnits() + itHiveGroup->second.GetAcquiredUnits());
                    pool->SetAcquiredIOPS(pool->GetAcquiredIOPS() + itHiveGroup->second.GetAcquiredIOPS());
                    pool->SetAcquiredThroughput(pool->GetAcquiredThroughput() + itHiveGroup->second.GetAcquiredThroughput());
                    pool->SetAcquiredSize(pool->GetAcquiredSize() + itHiveGroup->second.GetAcquiredSize());
                    pool->SetMaximumIOPS(pool->GetMaximumIOPS() + itHiveGroup->second.GetMaximumIOPS());
                    pool->SetMaximumThroughput(pool->GetMaximumThroughput() + itHiveGroup->second.GetMaximumThroughput());
                    pool->SetMaximumSize(pool->GetMaximumSize() + itHiveGroup->second.GetMaximumSize());
                }
            }
            if (pool->GroupsSize() == 0) {
                StorageInfo.MutableStoragePools()->RemoveLast();
                continue;
            }
            if (!poolName.empty()) {
                pool->SetName(poolName);
            }
            if (!poolInfo.Kind.empty()) {
                pool->SetKind(poolInfo.Kind);
            }
            pool->SetOverall(poolInfo.Overall);
        }

        if (Version == EVersion::v2) {
            switch (GroupSort) {
                case EGroupSort::PoolName:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.PoolName;}, ReverseSort);
                    break;
                case EGroupSort::GroupId:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.GroupId;}, ReverseSort);
                    break;
                case EGroupSort::Kind:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.Kind;}, ReverseSort);
                    break;
                case EGroupSort::MediaType:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.MediaType;}, ReverseSort);
                    break;
                case EGroupSort::Erasure:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.Erasure;}, ReverseSort);
                    break;
                case EGroupSort::Degraded:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.Degraded;}, ReverseSort);
                    break;
                case EGroupSort::Usage:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.Usage;}, ReverseSort);
                    break;
                case EGroupSort::Used:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.Used;}, ReverseSort);
                    break;
                case EGroupSort::Limit:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.Limit;}, ReverseSort);
                    break;
                case EGroupSort::Read:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.Read;}, ReverseSort);
                    break;
                case EGroupSort::Write:
                    SortCollection(GroupRows, [](const TGroupRow& node) { return node.Write;}, ReverseSort);
                    break;
            }

            ui32 start = Offset.has_value() ? Offset.value() : 0;
            ui32 end = GroupRows.size();
            if (Limit.has_value()) {
                end = Min(end, start + Limit.value());
            }
            for (ui32 i = start; i < end; ++i) {
                NKikimrViewer::TStorageGroupInfo* group = StorageInfo.AddStorageGroups();
                group->SetGroupId(GroupRows[i].GroupId);
            }
        }

        const FieldDescriptor* field;
        if (NeedGroups) {
            field = NKikimrViewer::TStorageGroupInfo::descriptor()->FindFieldByName("GroupId");
            JsonSettings.FieldRemapper[field] = [this](
                    IOutputStream& json,
                    const ::google::protobuf::Message& protoFrom,
                    const TJsonSettings& jsonSettings) -> void {
                RemapGroup(json, protoFrom, jsonSettings);
            };
        }
        if (NeedDisks) {
            field = NKikimrWhiteboard::TBSGroupStateInfo::descriptor()->FindFieldByName("VDiskIds");
            JsonSettings.FieldRemapper[field] = [this](
                    IOutputStream& json,
                    const ::google::protobuf::Message& protoFrom,
                    const TJsonSettings& jsonSettings) -> void {
                RemapVDisks(json, protoFrom, jsonSettings);
            };
            field = NKikimrWhiteboard::TVDiskStateInfo::descriptor()->FindFieldByName("PDiskId");
            JsonSettings.FieldRemapper[field] = [this](
                    IOutputStream& json,
                    const ::google::protobuf::Message& protoFrom,
                    const TJsonSettings& jsonSettings) -> void {
                RemapPDisk(json, protoFrom, jsonSettings);
            };
            if (NeedDonors) {
                field = NKikimrWhiteboard::TVDiskStateInfo::descriptor()->FindFieldByName("Donors");
                JsonSettings.FieldRemapper[field] = [this](
                        IOutputStream& json,
                        const ::google::protobuf::Message& protoFrom,
                        const TJsonSettings& jsonSettings) -> void {
                    RemapDonors(json, protoFrom, jsonSettings);
                };
            }
        }
        StorageInfo.SetTotalGroups(totalGroups);
        StorageInfo.SetFoundGroups(foundGroups);

        TStringStream json;
        TProtoToJson::ProtoToJson(json, StorageInfo, JsonSettings);
        Send(Initiator, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonStorage> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TStorageInfo>();
    }
};

template <>
struct TJsonRequestParameters<TJsonStorage> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
              - name: enums
                in: query
                description: convert enums to strings
                required: false
                type: boolean
              - name: ui64
                in: query
                description: return ui64 as number
                required: false
                type: boolean
              - name: tenant
                in: query
                description: tenant name
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
                description: filter groups by missing or space
                required: false
                type: string
              - name: version
                in: query
                description: query version (v1, v2)
                required: false
                type: string
              - name: usage_pace
                in: query
                description: bucket size as a percentage
                required: false
                type: integer
                default: 5
              - name: usage_buckets
                in: query
                description: filter groups by usage buckets
                required: false
                type: integer
              - name: sort
                in: query
                description: sort by (PoolName,Kind,MediaType,Erasure,Degraded,Usage,GroupId,Used,Limit,Read,Write)
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
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonStorage> {
    static TString GetSummary() {
        return "Storage information";
    }
};

template <>
struct TJsonRequestDescription<TJsonStorage> {
    static TString GetDescription() {
        return "Returns information about storage";
    }
};

}
}
