#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/blobstorage_common.h>
#include "layout_helpers.h"
#include "group_geometry_info.h"
#include "group_mapper.h"
#include "group_layout_checker.h"
#include "ut_helpers.h"

using namespace NKikimr;
using namespace NKikimr::NBsController;

class TTestContext {
    struct TPDiskRecord {
        ui32 DataCenterId;
        ui32 RoomId;
        ui32 RackId;
        ui32 BodyId;
        ui32 NumActiveSlots;
        ui32 SlotSizeInUnits;
        std::optional<TString> DiskScope;

        TPDiskRecord(ui32 dataCenterId, ui32 roomId, ui32 rackId, ui32 bodyId, ui32 slotSizeInUnits = 0u)
            : DataCenterId(dataCenterId)
            , RoomId(roomId)
            , RackId(rackId)
            , BodyId(bodyId)
            , NumActiveSlots(0)
            , SlotSizeInUnits(slotSizeInUnits)
        {}

        TNodeLocation GetLocation() const {
            NActorsInterconnect::TNodeLocation proto;
            proto.SetDataCenter(ToString(DataCenterId));
            proto.SetModule(ToString(RoomId));
            proto.SetRack(ToString(RackId));
            proto.SetUnit(ToString(BodyId));
            return TNodeLocation(proto);
        }
    };

    struct TGroupRecord {
        TGroupMapper::TGroupDefinition Group;
        TVector<TPDiskId> PDisks;
        ui32 GroupSizeInUnits = 0;
    };

    TMap<TPDiskId, TPDiskRecord> PDisks;
    TMap<ui32, TGroupRecord> Groups;
    ui32 NextGroupId = 1;

public:
    TTestContext(ui32 numDataCenters, ui32 numRooms, ui32 numRacks, ui32 numBodies, ui32 numDisks) {
        ui32 nodeId = 1;
        ui32 pdiskId = 1;
        ui32 dataCenter = 1;
        ui32 room = 1;
        ui32 rack = 1;
        ui32 body = 1;
        for (ui32 a = 0; a < numDataCenters; ++a, ++dataCenter) {
            for (ui32 b = 0; b < numRooms; ++b, ++room) {
                for (ui32 c = 0; c < numRacks; ++c, ++rack) {
                    for (ui32 d = 0; d < numBodies; ++d, ++body, ++nodeId) {
                        for (ui32 e = 0; e < numDisks; ++e, ++pdiskId) {
                            PDisks.emplace(TPDiskId(nodeId, pdiskId), TPDiskRecord(dataCenter, room, rack, body));
                        }
                    }
                }
            }
        }
    }

    TTestContext(ui32 numDataCenters, ui32 numRooms, ui32 numRacks, ui32 numBodies, std::vector<ui32> disksSizeInUnits) {
        ui32 nodeId = 1;
        ui32 pdiskId = 1;
        ui32 dataCenter = 1;
        ui32 room = 1;
        ui32 rack = 1;
        ui32 body = 1;
        for (ui32 a = 0; a < numDataCenters; ++a, ++dataCenter) {
            for (ui32 b = 0; b < numRooms; ++b, ++room) {
                for (ui32 c = 0; c < numRacks; ++c, ++rack) {
                    for (ui32 d = 0; d < numBodies; ++d, ++body, ++nodeId) {
                        for (ui32 e = 0; e < disksSizeInUnits.size(); ++e, ++pdiskId) {
                            TPDiskRecord pdisk(dataCenter, room, rack, body, disksSizeInUnits[e]);
                            PDisks.emplace(TPDiskId(nodeId, pdiskId), pdisk);
                        }
                    }
                }
            }
        }
    }

    TTestContext(const std::vector<std::tuple<ui32, ui32, ui32, ui32, ui32>>& disks) {
        ui32 nodeId = 1;
        for (const auto& disk : disks) {
            ui32 dataCenter, room, rack, body, numDisks;
            std::tie(dataCenter, room, rack, body, numDisks) = disk;
            for (ui32 pdiskId = 1; numDisks--; ++pdiskId) {
                const bool inserted = PDisks.emplace(TPDiskId(nodeId, pdiskId), TPDiskRecord(dataCenter, room, rack, body)).second;
                UNIT_ASSERT(inserted);
            }
            ++nodeId;
        }
    }

    TTestContext(const std::vector<std::vector<ui32>>& disposition, ui32 numDisks) {
        ui32 nodeId = 1;
        ui32 dataCenter = 1;
        ui32 room = 1;
        for (const auto& realm : disposition) {
            ui32 rack = 1;
            for (const auto& domain : realm) {
                for (ui32 body = 1; body <= domain; ++body) {
                    for (ui32 pdiskId = 1, i = numDisks; i--; ++pdiskId) {
                        const bool inserted = PDisks.emplace(TPDiskId(nodeId, pdiskId), TPDiskRecord(dataCenter, room, rack, body)).second;
                        UNIT_ASSERT(inserted);
                    }

                    ++nodeId;
                }
                ++rack;
            }
            ++dataCenter;
        }
    }

    static TGroupGeometryInfo CreateGroupGeometry(TBlobStorageGroupType type, ui32 numFailRealms = 0, ui32 numFailDomains = 0,
            ui32 numVDisks = 0, ui32 realmBegin = 0, ui32 realmEnd = 0, ui32 domainBegin = 0, ui32 domainEnd = 0) {
        NKikimrBlobStorage::TGroupGeometry g;
        g.SetNumFailRealms(numFailRealms);
        g.SetNumFailDomainsPerFailRealm(numFailDomains);
        g.SetNumVDisksPerFailDomain(numVDisks);
        g.SetRealmLevelBegin(realmBegin);
        g.SetRealmLevelEnd(realmEnd);
        g.SetDomainLevelBegin(domainBegin);
        g.SetDomainLevelEnd(domainEnd);
        return TGroupGeometryInfo(type, g);
    }

    ui32 GetTotalDisks() const {
        return PDisks.size();
    }

    TVector<ui32> GetNumActiveSlots() const {
        TVector<ui32> slots;
        for (const auto& pair : PDisks) {
            slots.push_back(pair.second.NumActiveSlots);
        }
        return slots;
    }

    template<typename TFunc>
    void IterateGroups(TFunc&& callback) {
        for (const auto& kv : Groups) {
            callback(kv.second.PDisks);
        }
    }

    template<typename TFunc>
    void IteratePDisks(TFunc&& callback) {
        for (auto& [k, v] : PDisks) {
            callback(k, v);
        }
    }

    ui32 GetDataCenter(TPDiskId pdiskId) const {
        const auto it = PDisks.find(pdiskId);
        UNIT_ASSERT(it != PDisks.end());
        return it->second.DataCenterId;
    }

    TNodeLocation GetLocation(TPDiskId pdiskId) const {
        const auto it = PDisks.find(pdiskId);
        UNIT_ASSERT(it != PDisks.end());
        return it->second.GetLocation();
    }

    std::vector<std::tuple<ui32, ui32, ui32, ui32>> ExportLayout() const {
        std::vector<std::tuple<ui32, ui32, ui32, ui32>> res;
        for (const auto& [pdiskId, pdisk] : PDisks) {
            res.emplace_back(pdisk.DataCenterId, pdisk.RoomId, pdisk.RackId, pdisk.BodyId);
        }
        return res;
    }

    void ImportLayout(const std::vector<std::tuple<ui32, ui32, ui32, ui32>>& v) {
        size_t index = 0;
        for (auto& [pdiskId, pdisk] : PDisks) {
            UNIT_ASSERT(index != v.size());
            std::tie(pdisk.DataCenterId, pdisk.RoomId, pdisk.RackId, pdisk.BodyId) = v[index];
            ++index;
        }
        UNIT_ASSERT(index == v.size());
    }

    void AllocateGroupCatchingError(TGroupMapper& mapper, TGroupMapper::TGroupDefinition& group, TGroupMapperError& error) {
        ui32 groupId = NextGroupId++;
        bool success = mapper.AllocateGroup(groupId, group, {}, {}, 0, 0, false, TBridgePileId(), error);
        UNIT_ASSERT_C(!success, "Allocation should have failed");
    }

    ui32 AllocateGroup(TGroupMapper& mapper, TGroupMapper::TGroupDefinition& group, ui32 groupSizeInUnits = 0u, bool allowFailure = false) {
        ui32 groupId = NextGroupId++;
        TGroupMapperError error;
        bool success = mapper.AllocateGroup(groupId, group, {}, {}, groupSizeInUnits, 0, false, TBridgePileId(), error);
        if (!success && allowFailure) {
            Ctest << "error# " << error.ErrorMessage << Endl;
            return 0;
        }
        UNIT_ASSERT_C(success, error.ErrorMessage);
        TGroupRecord& record = Groups[groupId];
        record.Group = group;
        record.GroupSizeInUnits = groupSizeInUnits;
        for (const auto& realm : group) {
            for (const auto& domain : realm) {
                for (const auto& pdiskId : domain) {
                    record.PDisks.push_back(pdiskId);
                    TPDiskRecord& pdisk = PDisks.at(pdiskId);
                    pdisk.NumActiveSlots += TPDiskConfig::GetOwnerWeight(groupSizeInUnits, pdisk.SlotSizeInUnits);
                }
            }
        }
        return groupId;
    }

    TGroupMapper::TGroupDefinition ReallocateGroup(TGroupMapper& mapper, ui32 groupId, const TSet<TPDiskId>& unusableDisks,
            bool makeThemForbidden = false, bool requireOperational = false, bool allowError = false) {
        TGroupRecord& group = Groups.at(groupId);

        TGroupMapper::TForbiddenPDisks forbid(unusableDisks.begin(), unusableDisks.end());
        if (!makeThemForbidden) {
            forbid.clear();
        }

        // remove unusable disks from the set
        THashMap<TVDiskIdShort, TPDiskId> replacedDisks;
        for (ui32 i = 0; i < group.Group.size(); ++i) {
            for (ui32 j = 0; j < group.Group[i].size(); ++j) {
                for (ui32 k = 0; k < group.Group[i][j].size(); ++k) {
                    TPDiskId& pdiskId = group.Group[i][j][k];
                    TPDiskRecord& pdisk = PDisks.at(pdiskId);
                    pdisk.NumActiveSlots -= TPDiskConfig::GetOwnerWeight(group.GroupSizeInUnits, pdisk.SlotSizeInUnits);
                    if (unusableDisks.count(pdiskId)) {
                        replacedDisks.emplace(TVDiskIdShort(i, j, k), std::exchange(pdiskId, {}));
                    }
                }
            }
        }

        Ctest << "groupId# " << groupId << " reallocating group# " << FormatGroup(group.Group) << Endl;

        TGroupMapperError error;
        bool success = mapper.AllocateGroup(groupId, group.Group, replacedDisks, std::move(forbid), group.GroupSizeInUnits, 0,
            requireOperational, TBridgePileId(), error);
        if (!success) {
            Ctest << "error# " << error.ErrorMessage << Endl;
            if (allowError) {
                // revert group to its original state
                for (const auto& [vdiskId, pdiskId] : replacedDisks) {
                    group.Group[vdiskId.FailRealm][vdiskId.FailDomain][vdiskId.VDisk] = pdiskId;
                }
                for (auto& realm : group.Group) {
                    for (auto& domain : realm) {
                        for (auto& pdiskId : domain) {
                            TPDiskRecord& pdisk = PDisks.at(pdiskId);
                            pdisk.NumActiveSlots += TPDiskConfig::GetOwnerWeight(group.GroupSizeInUnits, pdisk.SlotSizeInUnits);
                        }
                    }
                }
                return {};
            }
        }
        UNIT_ASSERT(success);

        group.PDisks.clear();
        for (const auto& realm : group.Group) {
            for (const auto& domain : realm) {
                for (const auto& pdiskId : domain) {
                    group.PDisks.push_back(pdiskId);
                    TPDiskRecord& pdisk = PDisks.at(pdiskId);
                    pdisk.NumActiveSlots += TPDiskConfig::GetOwnerWeight(group.GroupSizeInUnits, pdisk.SlotSizeInUnits);
                }
            }
        }

        return group.Group;
    }

    enum class ESanitizeResult {
        SUCCESS,
        FAIL,
        ALREADY,
    };

    using TSanitizeGroupResult = std::pair<ESanitizeResult, TGroupMapper::TGroupDefinition>;
    TSanitizeGroupResult SanitizeGroup(TGroupMapper& mapper, ui32 groupId, const TSet<TPDiskId>& unusableDisks,
            bool makeThemForbidden = false, bool requireOperational = false, bool allowError = false,
            std::pair<TVDiskIdShort, TPDiskId>* movedDisk = nullptr) {
        TGroupRecord& group = Groups.at(groupId);

        TGroupMapper::TForbiddenPDisks forbid(unusableDisks.begin(), unusableDisks.end());
        if (!makeThemForbidden) {
            forbid.clear();
        }

        Ctest << "groupId# " << groupId << " sanitizing group# " << FormatGroup(group.Group) << Endl;
        for (ui32 i = 0; i < group.Group.size(); ++i) {
            for (ui32 j = 0; j < group.Group[i].size(); ++j) {
                for (ui32 k = 0; k < group.Group[i][j].size(); ++k) {
                    auto& pdiskId = group.Group[i][j][k];
                    TPDiskRecord& pdisk = PDisks.at(pdiskId);
                    pdisk.NumActiveSlots -= TPDiskConfig::GetOwnerWeight(group.GroupSizeInUnits, pdisk.SlotSizeInUnits);
                }
            }
        }

        TGroupMapper::TMisplacedVDisks result = mapper.FindMisplacedVDisks(group.Group, group.GroupSizeInUnits);
        if (result) {
            Ctest << "error# " << result.ErrorReason << Endl;
            if (allowError) {
                for (auto& realm : group.Group) {
                    for (auto& domain : realm) {
                        for (auto& pdiskId : domain) {
                            TPDiskRecord& pdisk = PDisks.at(pdiskId);
                            pdisk.NumActiveSlots += TPDiskConfig::GetOwnerWeight(group.GroupSizeInUnits, pdisk.SlotSizeInUnits);
                        }
                    }
                }
                return {ESanitizeResult::FAIL, {}};
            }
        }

        ESanitizeResult status = ESanitizeResult::ALREADY;
        TString error;

        if (!result.Disks.empty()) {
            status = ESanitizeResult::FAIL;
            for (auto vdisk : result.Disks) {
                auto target = mapper.TargetMisplacedVDisk(TGroupId::FromValue(groupId), group.Group, vdisk,
                    std::move(forbid), group.GroupSizeInUnits, 0, requireOperational, TBridgePileId(), error);
                if (target) {
                    status = ESanitizeResult::SUCCESS;
                    if (movedDisk) {
                        *movedDisk = {vdisk, *target};
                    }
                    break;
                }
            }
        }

        if (status == ESanitizeResult::FAIL) {
            Ctest << "Sanitation failed! Last error reason: " << error << Endl;
        }

        group.PDisks.clear();
        for (const auto& realm : group.Group) {
            for (const auto& domain : realm) {
                for (const auto& pdiskId : domain) {
                    group.PDisks.push_back(pdiskId);
                    TPDiskRecord& pdisk = PDisks.at(pdiskId);
                    pdisk.NumActiveSlots += TPDiskConfig::GetOwnerWeight(group.GroupSizeInUnits, pdisk.SlotSizeInUnits);
                }
            }
        }

        return {status, group.Group};
    }

    void SetGroup(ui32 groupId, const TGroupMapper::TGroupDefinition& group) {
        auto& g = Groups[groupId];
        for (const TPDiskId& pdiskId : g.PDisks) {
            TPDiskRecord& pdisk = PDisks.at(pdiskId);
            pdisk.NumActiveSlots -= TPDiskConfig::GetOwnerWeight(g.GroupSizeInUnits, pdisk.SlotSizeInUnits);
        }
        g.Group = group;
        g.PDisks.clear();
        for (const auto& realm : g.Group) {
            for (const auto& domain : realm) {
                for (const auto& pdiskId : domain) {
                    g.PDisks.push_back(pdiskId);
                    TPDiskRecord& pdisk = PDisks.at(pdiskId);
                    pdisk.NumActiveSlots += TPDiskConfig::GetOwnerWeight(g.GroupSizeInUnits, pdisk.SlotSizeInUnits);
                }
            }
        }
    }

    TString FormatGroup(const TGroupMapper::TGroupDefinition& group) {
        TStringStream str;
        str << "[";
        for (auto it = group.begin(); it != group.end(); ++it) {
            if (it != group.begin()) {
                str << " ";
            }
            str << "[";
            for (auto jt = it->begin(); jt != it->end(); ++jt) {
                if (jt != it->begin()) {
                    str << " ";
                }
                str << "[";
                for (auto kt = jt->begin(); kt != jt->end(); ++kt) {
                    str << kt->ToString();
                }
                str << "]";
            }
            str << "]";
        }
        str << "]";
        return str.Str();
    }

    TString FormatGroup(ui32 groupId) {
        const auto it = Groups.find(groupId);
        UNIT_ASSERT(it != Groups.end());
        return FormatGroup(it->second.Group);
    }

    void CheckGroupErasure(const TGroupMapper::TGroupDefinition& group, ui32 decommittedDataCenter = 0) {
        TSet<ui32> dataCenters;
        for (const auto& realm : group) {
            TMaybe<ui32> dataCenter;
            TSet<std::tuple<ui32, ui32, ui32>> domains;
            for (const auto& domain : realm) {
                TMaybe<std::tuple<ui32, ui32, ui32>> currentDom;
                for (const auto& pdisk : domain) {
                    const TPDiskRecord& record = PDisks.at(pdisk);
                    if (record.DataCenterId != decommittedDataCenter) { // ignore entries from decommitted data center
                        if (dataCenter) {
                            if (*dataCenter != decommittedDataCenter && record.DataCenterId != decommittedDataCenter) {
                                UNIT_ASSERT_VALUES_EQUAL(*dataCenter, record.DataCenterId);
                            }
                        } else {
                            dataCenter = record.DataCenterId;
                            const bool inserted = dataCenters.insert(*dataCenter).second;
                            UNIT_ASSERT(inserted);
                        }
                    }
                    auto dom = std::make_tuple(record.DataCenterId, record.RoomId, record.RackId);
                    if (currentDom) {
                        // check that all disks from the same domain reside in the same domain :)
                        UNIT_ASSERT_EQUAL(dom, *currentDom);
                    } else {
                        currentDom = dom;
                        const bool inserted = domains.insert(dom).second;
                        UNIT_ASSERT(inserted); // check if it is new domain
                    }
                }
            }
        }
    }

    void CheckIfGroupsAreMappedCompact() {
        // create PDisk -> GroupId mapping
        TMap<TPDiskId, TVector<ui32>> pdiskToGroup;
        for (const auto& pair : Groups) {
            const TGroupRecord& group = pair.second;
            for (const TPDiskId& pdisk : group.PDisks) {
                pdiskToGroup[pdisk].push_back(pair.first);
            }
        }

        for (const auto& pair : Groups) {
            const TGroupRecord& group = pair.second;

            // first, pick up all PDisks from this group and check other groups on these PDisks
            TSet<ui32> groupIds;
            for (const TPDiskId& pdisk : group.PDisks) {
                for (ui32 groupId : pdiskToGroup.at(pdisk)) {
                    groupIds.insert(groupId);
                }
            }

            // now lets see if each of these groups occupies the same set of PDisks
            for (ui32 groupId : groupIds) {
                auto sorted = [](TVector<TPDiskId> pdisks) {
                    std::sort(pdisks.begin(), pdisks.end());
                    return pdisks;
                };
                UNIT_ASSERT_EQUAL(sorted(Groups.at(groupId).PDisks), sorted(group.PDisks));
            }
        }
    }

    void PopulateGroupMapper(TGroupMapper& mapper, ui32 expectedSlotCount = 16, TSet<TPDiskId> unusableDisks = {},
            TSet<TPDiskId> nonoperationalDisks = {}, std::optional<ui32> decommittedDataCenter = std::nullopt, bool equalSlots = true) {
        std::map<TPDiskId, std::vector<ui32>> groupDisks;
        for (const auto& [groupId, group] : Groups) {
            for (TPDiskId pdiskId : group.PDisks) {
                groupDisks[pdiskId].push_back(groupId);
            }
        }
        for (const auto& pair : PDisks) {
            auto& g = groupDisks[pair.first];
            const auto& location = pair.second.GetLocation().GetLegacyValue();
            mapper.RegisterPDisk({
                .PDiskId = pair.first,
                .Location = pair.second.GetLocation(),
                .Usable = !unusableDisks.count(pair.first),
                .NumActiveSlots = pair.second.NumActiveSlots,
                .ExpectedSlotCount = equalSlots || location.Rack < 8 ? expectedSlotCount : 2 * expectedSlotCount,
                .SlotSizeInUnits = pair.second.SlotSizeInUnits,
                .SlotSizeInBytes = 0,
                .Groups{g.begin(), g.end()},
                .SpaceAvailable = 0,
                .Operational = !nonoperationalDisks.contains(pair.first),
                .Decommitted = decommittedDataCenter == pair.second.DataCenterId,
                .DiskScope = pair.second.DiskScope,
            });
        }
    }

    void DumpGroup(const TGroupMapper::TGroupDefinition& group) {
        std::set<std::tuple<ui32, ui32, ui32>> locations;
        for (const auto& [pdiskId, pdisk] : PDisks) {
            locations.emplace(pdisk.DataCenterId, pdisk.RoomId, pdisk.RackId);
        }

        std::unordered_map<ui32, ui32> dataCenterToColumn;
        std::unordered_map<ui32, std::unordered_map<std::tuple<ui32, ui32>, ui32>> rackToColumn;
        for (const auto& x : locations) {
            const ui32 dataCenterId = std::get<0>(x);
            const ui32 roomId = std::get<1>(x);
            const ui32 rackId = std::get<2>(x);
            dataCenterToColumn.try_emplace(dataCenterId, dataCenterToColumn.size());
            auto& rtc = rackToColumn[dataCenterId];
            rtc.try_emplace(std::make_tuple(roomId, rackId), rtc.size());
        }

        std::vector<std::vector<TString>> cells(dataCenterToColumn.size());
        for (const auto& [dataCenterId, racks] : rackToColumn) {
            cells[dataCenterToColumn[dataCenterId]].resize(racks.size());
        }

        ui32 maxCellWidth = 0;
        for (ui32 failRealmIdx = 0; failRealmIdx < group.size(); ++failRealmIdx) {
            for (ui32 failDomainIdx = 0; failDomainIdx < group[failRealmIdx].size(); ++failDomainIdx) {
                for (const TPDiskId& pdiskId : group[failRealmIdx][failDomainIdx]) {
                    if (pdiskId != TPDiskId()) {
                        const auto it = PDisks.find(pdiskId);
                        UNIT_ASSERT(it != PDisks.end());
                        const TPDiskRecord& pdisk = it->second;
                        auto& cell = cells[dataCenterToColumn[pdisk.DataCenterId]]
                            [rackToColumn[pdisk.DataCenterId][{pdisk.RoomId, pdisk.RackId}]];
                        if (cell) {
                            cell += ", ";
                        }
                        cell += TStringBuilder() << failRealmIdx << "/" << failDomainIdx;
                        maxCellWidth = Max<ui32>(maxCellWidth, cell.size());
                    }
                }
            }
        }

        if (!maxCellWidth) {
            ++maxCellWidth;
        }

        for (ui32 row = 0;; ++row) {
            bool done = true;
            TStringBuilder s;
            for (ui32 column = 0; column < cells.size(); ++column) {
                if (row >= cells[column].size()) {
                    s << TString(maxCellWidth, ' ');
                } else if (const auto& cell = cells[column][row]) {
                    s << cell << TString(maxCellWidth - cell.size(), ' ');
                    done = false;
                } else {
                    s << TString(maxCellWidth, 'X');
                    done = false;
                }
                if (column != cells.size() - 1) {
                    s << ' ';
                }
            }
            if (done) {
                break;
            } else {
                Ctest << s << Endl;
            }
        }
    }

    bool CheckGroupPlacement(const TGroupMapper::TGroupDefinition& group, TGroupGeometryInfo geom, TString& error) {
        NLayoutChecker::TDomainMapper domainMapper;
        std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition> pdisks;
        for (ui32 failRealm = 0; failRealm < geom.GetNumFailRealms(); ++failRealm) {
            for (ui32 failDomain = 0; failDomain < geom.GetNumFailDomainsPerFailRealm(); ++failDomain) {
                for (ui32 vdisk = 0; vdisk < geom.GetNumVDisksPerFailDomain(); ++vdisk) {
                    const auto pdiskId = group[failRealm][failDomain][vdisk];
                    const TPDiskRecord& record = PDisks.at(pdiskId);
                    pdisks[pdiskId] = NLayoutChecker::TPDiskLayoutPosition(domainMapper,
                            record.GetLocation(),
                            record.DiskScope,
                            pdiskId,
                            geom
                    );
                }
            }
        }

        return CheckLayoutByGroupDefinition(group, pdisks, geom, true, error);
    }

    TPDiskId GetGroupDiskId(ui32 groupId, ui32 realm = 0, ui32 domain = 0, ui32 disk = 0) const {
        const auto it = Groups.find(groupId);
        UNIT_ASSERT(it != Groups.end());
        const TGroupRecord& group = it->second;
        return group.Group[realm][domain][disk];
    }
};

static TNodeLocation MakeTestLocation(ui32 nodeId, ui32 rackId = 0) {
    NActorsInterconnect::TNodeLocation proto;
    proto.SetDataCenter("1");
    proto.SetModule("1");
    proto.SetRack(ToString(rackId ? rackId : nodeId));
    proto.SetUnit(ToString(nodeId));
    return TNodeLocation(proto);
}

Y_UNIT_TEST_SUITE(TGroupMapperTest) {

    Y_UNIT_TEST(SimplestErasureNone) {
        // Single node with single PDisk with 2 slots on it
        TTestContext context(1, 1, 1, 1, 1);
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1));
        context.PopulateGroupMapper(mapper, 2);

        UNIT_ASSERT_VALUES_EQUAL(context.GetTotalDisks(), 1);
        UNIT_ASSERT_VALUES_EQUAL(0, context.GetNumActiveSlots()[0]);

        TGroupMapper::TGroupDefinition g1, g2;

        UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, g1));
        UNIT_ASSERT_VALUES_EQUAL(1, context.GetNumActiveSlots()[0]);

        UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, g2));
        UNIT_ASSERT_VALUES_EQUAL(2, context.GetNumActiveSlots()[0]);

        Ctest << "group after allocation:" << Endl;
        context.DumpGroup(g1);
        context.DumpGroup(g2);
    }

    Y_UNIT_TEST(PlacementSearchMatchesSmallTopologyCapacity) {
        for (ui32 domainsPerRealm : {1u, 2u}) {
            const auto geometry = TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone,
                                                                    2, domainsPerRealm, 1);
            TTestContext context(2, 1, 2, 1, 2);
            for (ui32 availableMask = 0; availableMask < 256; ++availableMask) {
                TGroupMapper mapper(geometry);
                ui32 index = 0;
                ui32 available[2][2] = {};
                context.IteratePDisks([&](TPDiskId pdiskId, const auto&) {
                    const bool usable = availableMask & (1u << index);
                    available[index / 4][index / 2 % 2] += usable;
                    UNIT_ASSERT(mapper.RegisterPDisk({
                        .PDiskId = pdiskId,
                        .Location = context.GetLocation(pdiskId),
                        .Usable = usable,
                        .NumActiveSlots = index % 3,
                        .ExpectedSlotCount = 4,
                        .Operational = true,
                    }));
                    ++index;
                });

                bool canAllocate = true;
                for (const auto& realm : available) {
                    const ui32 domains = std::count_if(std::begin(realm), std::end(realm), [&](ui32 count) {
                        return count != 0;
                    });
                    canAllocate &= domains >= domainsPerRealm;
                }

                TGroupMapper::TGroupDefinition group;
                TGroupMapperError error;
                const bool success = mapper.AllocateGroup(1, group, {}, {}, 0, 0, true, {}, error);
                UNIT_ASSERT_VALUES_EQUAL_C(success, canAllocate,
                                           "mask# " << availableMask << " domainsPerRealm# " << domainsPerRealm << " " << error.ErrorMessage);
                TSet<TPDiskId> selected;
                if (success) {
                    TString layoutError;
                    UNIT_ASSERT_C(context.CheckGroupPlacement(group, geometry, layoutError), layoutError);
                    TGroupMapper::Traverse(group, [&](TVDiskIdShort, TPDiskId pdiskId) {
                        UNIT_ASSERT(availableMask & (1u << (pdiskId.PDiskId - 1)));
                        UNIT_ASSERT(selected.insert(pdiskId).second);
                    });
                }
                index = 0;
                context.IteratePDisks([&](TPDiskId pdiskId, const auto&) {
                    const auto disk = mapper.UnregisterPDisk(pdiskId);
                    const bool allocated = selected.contains(pdiskId);
                    UNIT_ASSERT_VALUES_EQUAL(disk.NumActiveSlots, index++ % 3 + allocated);
                    UNIT_ASSERT_VALUES_EQUAL(disk.Groups.size(), allocated ? 1 : 0);
                    if (allocated) {
                        UNIT_ASSERT_VALUES_EQUAL(disk.Groups.front(), 1);
                    }
                });
                Ctest << domainsPerRealm << ":" << availableMask << ":" << success
                      << ":" << context.FormatGroup(group) << Endl;
            }
        }
    }

    Y_UNIT_TEST(PlacementSearchFailureDoesNotAffectNextRequest) {
        for (bool ignoreLayout : {false, true}) {
            const auto geometry = TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 2, 2, 1);
            TTestContext context(3, 1, 3, 1, 2);
            TGroupMapper mapper(geometry);
            TGroupMapper reference(geometry);
            context.PopulateGroupMapper(mapper, 3);
            context.PopulateGroupMapper(reference, 3);

            TGroupMapper::TReassignmentRequest request;
            request.ExistingGroup = false;
            request.IgnoreGroupLayoutChecks = ignoreLayout;
            TGroupMapper::TReassignmentRequest failedRequest;
            failedRequest.IgnoreGroupLayoutChecks = ignoreLayout;
            for (ui32 groupId = 1; groupId <= 8; ++groupId) {
                request.GroupId = groupId;
                if (!failedRequest.VDisks.empty()) {
                    UNIT_ASSERT(!mapper.AllocateGroupReassignment(failedRequest).Success);
                }
                const auto actual = mapper.AllocateGroupReassignment(request);
                const auto expected = reference.AllocateGroupReassignment(request);
                UNIT_ASSERT_C(actual.Success, actual.Error.ErrorMessage);
                UNIT_ASSERT_C(expected.Success, expected.Error.ErrorMessage);
                UNIT_ASSERT_EQUAL(actual.Group, expected.Group);
                Ctest << ignoreLayout << ":" << groupId << ":" << context.FormatGroup(actual.Group) << Endl;
                failedRequest.GroupId = groupId;
                failedRequest.VDisks.clear();
                TGroupMapper::Traverse(actual.Group, [&](TVDiskIdShort vdiskId, TPDiskId pdiskId) {
                    failedRequest.VDisks.push_back({.VDiskId = vdiskId, .PDiskId = pdiskId});
                });
                failedRequest.VDisks.front().Reassignment = TGroupMapper::TReplaceVDisk{};
                failedRequest.VDisks.back().Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{TPDiskId(1000, 1)};
            }
            context.IteratePDisks([&](TPDiskId pdiskId, const auto&) {
                const auto actual = mapper.UnregisterPDisk(pdiskId);
                const auto expected = reference.UnregisterPDisk(pdiskId);
                UNIT_ASSERT_VALUES_EQUAL(actual.NumActiveSlots, expected.NumActiveSlots);
                UNIT_ASSERT_EQUAL(actual.Groups, expected.Groups);
            });
        }
    }

    Y_UNIT_TEST(ReassignmentPreservesOtherGroupsOnSharedPDisks) {
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1));
        auto registerDisk = [&](ui32 nodeId, TStackVec<ui32, 16> groups) {
            UNIT_ASSERT(mapper.RegisterPDisk({
                .PDiskId = TPDiskId(nodeId, 1),
                .Location = MakeTestLocation(nodeId),
                .Usable = true,
                .NumActiveSlots = static_cast<ui32>(groups.size()),
                .ExpectedSlotCount = 4,
                .Groups = std::move(groups),
                .Operational = true,
            }));
        };
        registerDisk(1, {10});
        registerDisk(2, {20});
        registerDisk(3, {5, 30});

        TGroupMapper::TReassignmentRequest request;
        request.GroupId = 10;
        request.VDisks.push_back({.VDiskId = TVDiskIdShort(0, 0, 0), .PDiskId = TPDiskId(1, 1)});
        for (ui32 nodeId : {2u, 3u}) {
            const TPDiskId target(nodeId, 1);
            request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{target};
            const auto outcome = mapper.AllocateGroupReassignment(request);
            UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][0][0], target);
            request.VDisks.front().PDiskId = target;
        }

        const auto source = mapper.UnregisterPDisk(TPDiskId(1, 1));
        const auto intermediate = mapper.UnregisterPDisk(TPDiskId(2, 1));
        const auto destination = mapper.UnregisterPDisk(TPDiskId(3, 1));
        UNIT_ASSERT_VALUES_EQUAL(source.NumActiveSlots, 0);
        UNIT_ASSERT(source.Groups.empty());
        UNIT_ASSERT_VALUES_EQUAL(intermediate.NumActiveSlots, 1);
        UNIT_ASSERT_EQUAL(intermediate.Groups, (TStackVec<ui32, 16>{20}));
        UNIT_ASSERT_VALUES_EQUAL(destination.NumActiveSlots, 3);
        UNIT_ASSERT_EQUAL(destination.Groups, (TStackVec<ui32, 16>{5, 10, 30}));
    }

    Y_UNIT_TEST(LayoutRepairReservesWeightedSlotsOnlyOnSuccess) {
        const auto geometry = TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 2, 1);
        TGroupMapper mapper(geometry);
        for (ui32 nodeId = 1; nodeId <= 3; ++nodeId) {
            const bool occupied = nodeId <= 2;
            UNIT_ASSERT(mapper.RegisterPDisk({
                .PDiskId = TPDiskId(nodeId, 1),
                .Location = MakeTestLocation(nodeId, occupied ? 1 : 2),
                .Usable = true,
                .NumActiveSlots = occupied ? 2u : 0u,
                .ExpectedSlotCount = occupied ? 2u : 1u,
                .SlotSizeInUnits = occupied ? 1u : 2u,
                .Groups = occupied ? TStackVec<ui32, 16>{1} : TStackVec<ui32, 16>{},
                .Operational = true,
            }));
        }
        TGroupMapper::TGroupDefinition group{{{TPDiskId(1, 1)}, {TPDiskId(2, 1)}}};
        const auto original = group;
        const TVDiskIdShort replaced(0, 0, 0);
        TString error;
        UNIT_ASSERT(!mapper.TargetMisplacedVDisk(TGroupId::FromValue(1), group, replaced,
                                                 {TPDiskId(3, 1)}, 2, 0, true, {}, error));
        UNIT_ASSERT_EQUAL(group, original);
        const auto target = mapper.TargetMisplacedVDisk(TGroupId::FromValue(1), group, replaced, {}, 2, 0, true, {}, error);
        UNIT_ASSERT_C(target.has_value(), error);
        UNIT_ASSERT_VALUES_EQUAL(*target, TPDiskId(3, 1));
        UNIT_ASSERT_VALUES_EQUAL(group[0][0][0], *target);
        UNIT_ASSERT_VALUES_EQUAL(group[0][1][0], original[0][1][0]);

        const auto source = mapper.UnregisterPDisk(TPDiskId(1, 1));
        const auto preserved = mapper.UnregisterPDisk(TPDiskId(2, 1));
        const auto destination = mapper.UnregisterPDisk(*target);
        UNIT_ASSERT_VALUES_EQUAL(source.NumActiveSlots, 0);
        UNIT_ASSERT(source.Groups.empty());
        UNIT_ASSERT_VALUES_EQUAL(preserved.NumActiveSlots, 2);
        UNIT_ASSERT_VALUES_EQUAL(destination.NumActiveSlots, 1);
        UNIT_ASSERT_EQUAL(preserved.Groups, (TStackVec<ui32, 16>{1}));
        UNIT_ASSERT_EQUAL(destination.Groups, preserved.Groups);
    }

    Y_UNIT_TEST(SlotSizeInBytesLimitsRequiredSpace) {
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1));
        UNIT_ASSERT(mapper.RegisterPDisk({
            .PDiskId = TPDiskId(1, 1),
            .Location = MakeTestLocation(1),
            .Usable = true,
            .NumActiveSlots = 0,
            .ExpectedSlotCount = 2,
            .SlotSizeInUnits = 1,
            .SlotSizeInBytes = 100,
            .Groups{},
            .SpaceAvailable = 1000,
            .Operational = true,
            .Decommitted = false,
        }));

        TGroupMapper::TGroupDefinition group;
        TGroupMapperError error;
        UNIT_ASSERT_C(!mapper.AllocateGroup(1, group, {}, {}, 2, 150, false, TBridgePileId(), error), error.ErrorMessage);
    }

    Y_UNIT_TEST(PlacementSnapshotAppliesPDiskEligibilityAndSpacePolicies) {
        auto canAllocate = [](TGroupMapper::TOptions options, bool ready, bool operational,
                              NKikimrBlobStorage::TMaintenanceStatus::E maintenanceStatus) {
            NKikimrBlobStorage::TPDiskMetrics metrics;
            metrics.SetTotalSize(1000);
            metrics.SetAvailableSize(300);

            TGroupMapper::TPlacementSnapshot state;
            state.PDisks.push_back({
                .PDiskId = TPDiskId(1, 1),
                .Location = MakeTestLocation(1),
                .ExpectedSlotCount = 2,
                .SlotSizeInUnits = 1,
                .Space = TGroupMapper::CapturePDiskSpace(metrics),
                .Operational = operational,
                .MaintenanceStatus = maintenanceStatus,
            });
            state.Groups.push_back({
                .GroupId = 10,
                .MaxVDiskAllocatedSize = 250,
            });
            state.VSlots.push_back({
                .VSlotId = TVSlotId(TPDiskId(1, 1), 1),
                .PDiskId = TPDiskId(1, 1),
                .GroupId = 10,
                .Ready = ready,
                .AllocatedSize = 100,
                .SpaceUsed = 100,
            });

            TGroupMapper::TReassignmentRequest request;
            request.GroupId = 1;
            request.MinimumRequiredSpace = 200;
            request.ExistingGroup = false;
            auto geometry = TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1);
            return TGroupMapper::PlanGroupReassignment(std::move(geometry), options, std::move(state),
                                                       std::move(request)).Success;
        };

        const auto noMaintenance = NKikimrBlobStorage::TMaintenanceStatus::NO_REQUEST;
        UNIT_ASSERT(!canAllocate({}, false, true, noMaintenance));
        UNIT_ASSERT(canAllocate({}, true, true, noMaintenance));
        UNIT_ASSERT(canAllocate({.IgnoreVSlotQuotaCheck = true}, false, true, noMaintenance));
        UNIT_ASSERT(!canAllocate({.SettleOnlyOnOperationalDisks = true}, true, false, noMaintenance));
        UNIT_ASSERT(!canAllocate({}, true, true, NKikimrBlobStorage::TMaintenanceStatus::NO_NEW_VDISKS));

        auto getRejectedPDisk = [](bool isSelfHealReasonDecommit) {
            TGroupMapper::TPlacementSnapshot state;
            state.PDisks.push_back({
                .PDiskId = TPDiskId(1, 1),
                .Location = MakeTestLocation(1),
                .ExpectedSlotCount = 1,
                .SlotSizeInUnits = 1,
                .Operational = true,
                .DecommitStatus = NKikimrBlobStorage::DECOMMIT_REJECTED,
            });

            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1),
                                {.IsSelfHealReasonDecommit = isSelfHealReasonDecommit});
            mapper.Populate(std::move(state));
            return mapper.UnregisterPDisk(TPDiskId(1, 1));
        };

        const auto regularPDisk = getRejectedPDisk(false);
        UNIT_ASSERT(regularPDisk.Usable);
        UNIT_ASSERT(regularPDisk.Decommitted);

        const auto selfHealPDisk = getRejectedPDisk(true);
        UNIT_ASSERT(!selfHealPDisk.Usable);
        UNIT_ASSERT(selfHealPDisk.Decommitted);
    }

    Y_UNIT_TEST(CapturesPDiskOperationalAndSpaceState) {
        NKikimrBlobStorage::TPDiskMetrics metrics;

        UNIT_ASSERT(TGroupMapper::IsPDiskOperational(true, nullptr));
        UNIT_ASSERT(TGroupMapper::IsPDiskOperational(true, &metrics));
        UNIT_ASSERT(!TGroupMapper::IsPDiskOperational(false, &metrics));

        metrics.SetState(NKikimrBlobStorage::TPDiskState::Normal);
        UNIT_ASSERT(TGroupMapper::IsPDiskOperational(true, &metrics));
        metrics.SetState(NKikimrBlobStorage::TPDiskState::OpenFileError);
        UNIT_ASSERT(!TGroupMapper::IsPDiskOperational(true, &metrics));

        metrics.SetAvailableSize(800);
        metrics.SetTotalSize(1000);
        auto space = TGroupMapper::CapturePDiskSpace(metrics);
        metrics.SetAvailableSize(100);
        UNIT_ASSERT_VALUES_EQUAL(TGroupMapper::CalculateSpaceAvailable(space, NKikimrBlobStorage::TPDiskSpaceColor::GREEN, 150), 650);

        metrics.SetEnforcedDynamicSlotSize(500);
        space = TGroupMapper::CapturePDiskSpace(metrics);
        UNIT_ASSERT_VALUES_EQUAL(TGroupMapper::CalculateSpaceAvailable(space, NKikimrBlobStorage::TPDiskSpaceColor::YELLOW, 150), 425);
    }

    Y_UNIT_TEST(ReassignmentRackScoreIgnoresUnusablePDisks) {
        const TNodeLocation rackOne = MakeTestLocation(1, 1);
        const TNodeLocation rackTwo = MakeTestLocation(2, 2);

        TGroupMapper::TPlacementSnapshot state;
        state.PDisks = {
            {
                .PDiskId = TPDiskId(1, 1),
                .Location = rackOne,
                .ExpectedSlotCount = 1,
                .SlotSizeInUnits = 1,
                .Operational = true,
            },
            {
                .PDiskId = TPDiskId(2, 1),
                .Location = rackTwo,
                .ExpectedSlotCount = 2,
                .SlotSizeInUnits = 1,
                .Operational = true,
            },
            {
                .PDiskId = TPDiskId(3, 1),
                .Location = MakeTestLocation(3, 1),
                .Usable = false,
                .ExpectedSlotCount = 100,
                .SlotSizeInUnits = 1,
                .Operational = true,
            },
        };

        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1),
                            {.PreferLessOccupiedRack = true, .IgnoreVSlotQuotaCheck = true});
        mapper.Populate(std::move(state));

        const auto& slotTracker = mapper.GetPDiskSlotTracker();
        UNIT_ASSERT_VALUES_EQUAL(slotTracker.GetFreeSlotsOnRack(rackOne.GetRackId()), 1);
        UNIT_ASSERT_VALUES_EQUAL(slotTracker.GetFreeSlotsOnRack(rackTwo.GetRackId()), 2);

        TGroupMapper::TReassignmentRequest request;
        request.GroupId = 1;
        request.ExistingGroup = false;
        const auto outcome = mapper.AllocateGroupReassignment(std::move(request));
        UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][0][0], TPDiskId(2, 1));
    }

    Y_UNIT_TEST(PlacementBuilderBuildsTrackers) {
        const TPDiskId pdiskId(1, 1);
        const TVSlotId vslotId(pdiskId, 1);
        const TNodeLocation rackOne = MakeTestLocation(1, 1);
        const TNodeLocation rackTwo = MakeTestLocation(2, 2);

        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1), {
            .PreferLessOccupiedRack = true,
            .WithAttentionToReplication = true,
            .IgnoreVSlotQuotaCheck = true,
        });
        TGroupMapper::TPlacementBuilder builder(mapper);
        builder.AddGroup({
            .GroupId = 1,
            .GroupGeneration = 1,
            .GroupSizeInUnits = 3,
            .MaxVDiskAllocatedSize = 100,
        });
        builder.AddPDisk({
            .PDiskId = pdiskId,
            .Location = rackOne,
            .ExpectedSlotCount = 10,
            .SlotSizeInUnits = 1,
            .Operational = true,
        });
        builder.AddPDisk({
            .PDiskId = TPDiskId(2, 1),
            .Location = rackTwo,
            .ExpectedSlotCount = 10,
            .SlotSizeInUnits = 1,
            .Operational = true,
        });
        builder.AddVSlot({
            .VSlotId = vslotId,
            .PDiskId = pdiskId,
            .GroupId = 1,
            .GroupGeneration = 1,
            .VDiskId = TVDiskIdShort(0, 0, 0),
            .Ready = false,
            .Replicating = true,
            .AllocatedSize = 100,
            .SpaceUsed = 100,
        });
        builder.Finish();

        const auto& tracker = mapper.GetPDiskSlotTracker();
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetFreeSlotsOnRack(rackOne.GetRackId()), 7);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetFreeSlotsOnRack(rackTwo.GetRackId()), 10);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetReplicatingVDisksOnNode(pdiskId.NodeId), 1);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetReplicatingVDisksOnPDisk(pdiskId), 1);

        TGroupMapper precomputedMapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1),
                                       {.WithAttentionToReplication = true});
        TGroupMapper::TPlacementBuilder precomputedBuilder(precomputedMapper);
        TPDiskSlotTracker precomputedTracker;
        precomputedTracker.AddReplicatingVSlot(pdiskId);
        precomputedBuilder.SetPrecomputedReplicationTracker(std::move(precomputedTracker));
        precomputedBuilder.AddVSlot({
            .VSlotId = vslotId,
            .PDiskId = pdiskId,
            .Replicating = true,
        });
        precomputedBuilder.Finish();
        const auto& resultingPrecomputedTracker = precomputedMapper.GetPDiskSlotTracker();
        UNIT_ASSERT_VALUES_EQUAL(resultingPrecomputedTracker.GetReplicatingVDisksOnNode(pdiskId.NodeId), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultingPrecomputedTracker.GetReplicatingVDisksOnPDisk(pdiskId), 1);

        TGroupMapper::TReassignmentRequest request;
        request.GroupId = 1;
        request.GroupGeneration = 1;
        request.VDisks.push_back({
            .VDiskId = TVDiskIdShort(0, 0, 0),
            .PDiskId = pdiskId,
        });
        const auto outcome = mapper.AllocateGroupReassignment(std::move(request));
        UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
        UNIT_ASSERT_VALUES_EQUAL(outcome.RequiredSpace, 100);
    }

    Y_UNIT_TEST(ReassignmentDestroyedVSlotIsNotOccupiedByGroup) {
        TGroupMapper::TPlacementSnapshot state;
        state.PDisks.push_back({
            .PDiskId = TPDiskId(1, 1),
            .Location = MakeTestLocation(1),
            .ExpectedSlotCount = 2,
            .SlotSizeInUnits = 1,
            .Operational = true,
        });

        state.VSlots.push_back({
            .VSlotId = TVSlotId(TPDiskId(1, 1), 1),
            .PDiskId = TPDiskId(1, 1),
            .GroupId = 1,
            .GroupGeneration = 1,
            .VDiskId = TVDiskIdShort(0, 0, 0),
            .OccupiedByGroup = false,
            .Ready = true,
        });

        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1));
        mapper.Populate(std::move(state));
        const auto pdisk = mapper.UnregisterPDisk(TPDiskId(1, 1));
        UNIT_ASSERT_VALUES_EQUAL(pdisk.NumActiveSlots, 1);
        UNIT_ASSERT(pdisk.Groups.empty());
    }

    Y_UNIT_TEST(ReassignmentPlannerAppliesTargetPolicies) {
        TVector<TGroupMapper::TVDiskPlacement> disks{
            {.AllocatedSize = 300},
            {.AllocatedSize = 500, .Reassignment = TGroupMapper::TReplaceVDisk{}},
        };
        UNIT_ASSERT_VALUES_EQUAL(TGroupMapper::CalculateRequiredSpace(disks), 300);
        const TVector<TGroupMapper::TVDiskPlacement> allReplacedDisks{disks.back()};
        UNIT_ASSERT_VALUES_EQUAL(TGroupMapper::CalculateRequiredSpace(allReplacedDisks), Min<i64>());

        auto reassign = [&](TGroupMapper::TVDiskReassignment reassignment, bool tryToRelocateLocallyFirst,
                            ui32 secondLocalPDiskNumActiveSlots = 0) {
            TGroupMapper::TPlacementSnapshot snapshot;
            auto addPDisk = [&](TPDiskId pdiskId, ui32 numActiveSlots) {
                snapshot.PDisks.push_back({
                    .PDiskId = pdiskId,
                    .Location = MakeTestLocation(pdiskId.NodeId),
                    .NumActiveSlots = numActiveSlots,
                    .ExpectedSlotCount = 2,
                    .SlotSizeInUnits = 1,
                    .Operational = true,
                });
            };
            addPDisk(TPDiskId(1, 1), 1);
            addPDisk(TPDiskId(1, 2), secondLocalPDiskNumActiveSlots);
            addPDisk(TPDiskId(2, 1), 0);
            snapshot.VSlots.push_back({
                .VSlotId = TVSlotId(TPDiskId(1, 1), 1),
                .PDiskId = TPDiskId(1, 1),
                .GroupId = 1,
                .GroupGeneration = 1,
                .CountedInNumActiveSlots = false,
                .AllocatedSize = 500,
            });

            TGroupMapper::TReassignmentRequest request;
            request.GroupId = 1;
            request.GroupGeneration = 1;
            request.MinimumRequiredSpace = 200;
            request.TryToRelocateLocallyFirst = tryToRelocateLocallyFirst;
            request.VDisks.push_back({
                .VDiskId = TVDiskIdShort(0, 0, 0),
                .PDiskId = TPDiskId(1, 1),
                .Reassignment = std::move(reassignment),
            });

            auto geometry = TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1);
            auto outcome = TGroupMapper::PlanGroupReassignment(std::move(geometry), {.IgnoreVSlotQuotaCheck = true},
                                                               std::move(snapshot), std::move(request));
            UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
            UNIT_ASSERT(outcome.Error.ErrorMessage.empty());
            UNIT_ASSERT_VALUES_EQUAL(outcome.RequiredSpace, 200);
            return outcome.Group[0][0][0];
        };

        UNIT_ASSERT_VALUES_EQUAL(reassign(TGroupMapper::TReplaceVDisk{}, true), TPDiskId(1, 2));
        UNIT_ASSERT_VALUES_EQUAL(reassign(TGroupMapper::TReplaceVDiskOnPDisk{TPDiskId(2, 1)}, true), TPDiskId(2, 1));
        UNIT_ASSERT_VALUES_EQUAL(reassign(TGroupMapper::TReplaceVDisk{.RequireSameNode = true}, false), TPDiskId(1, 2));
        UNIT_ASSERT_VALUES_EQUAL(reassign(TGroupMapper::TForceVDiskOnPDisk{TPDiskId(1, 2)}, false, 2), TPDiskId(1, 2));
    }

    Y_UNIT_TEST(ReassignmentReservesSlotsOnlyOnSuccess) {
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 2, 1));
        TGroupMapper::TPlacementSnapshot snapshot;
        TGroupMapper::TReassignmentRequest request;
        request.GroupId = 1;
        for (ui32 nodeId = 1; nodeId <= 4; ++nodeId) {
            const TPDiskId pdiskId(nodeId, 1);
            snapshot.PDisks.push_back({
                .PDiskId = pdiskId,
                .Location = MakeTestLocation(nodeId),
                .ExpectedSlotCount = 1,
                .SlotSizeInUnits = 1,
                .Operational = true,
            });
            if (nodeId <= 2) {
                const TVDiskIdShort vdiskId(0, nodeId - 1, 0);
                snapshot.VSlots.push_back({
                    .VSlotId = TVSlotId(pdiskId, 1),
                    .PDiskId = pdiskId,
                    .GroupId = request.GroupId,
                    .VDiskId = vdiskId,
                });
                request.VDisks.push_back({
                    .VDiskId = vdiskId,
                    .PDiskId = pdiskId,
                    .Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{TPDiskId(3, 1)},
                });
            }
        }
        mapper.Populate(std::move(snapshot));

        UNIT_ASSERT(!mapper.AllocateGroupReassignment(request).Success);
        request.VDisks.back().Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{TPDiskId(4, 1)};
        const auto reassigned = mapper.AllocateGroupReassignment(std::move(request));
        UNIT_ASSERT_C(reassigned.Success, reassigned.Error.ErrorMessage);
        UNIT_ASSERT_VALUES_EQUAL(reassigned.Group[0][0][0], TPDiskId(3, 1));
        UNIT_ASSERT_VALUES_EQUAL(reassigned.Group[0][1][0], TPDiskId(4, 1));

        TGroupMapper::TReassignmentRequest create;
        create.GroupId = 2;
        create.ExistingGroup = false;
        const auto allocated = mapper.AllocateGroupReassignment(create);
        UNIT_ASSERT_C(allocated.Success, allocated.Error.ErrorMessage);
        TSet<TPDiskId> occupied;
        TGroupMapper::Traverse(allocated.Group, [&](TVDiskIdShort, TPDiskId pdiskId) {
            occupied.insert(pdiskId);
        });
        UNIT_ASSERT_VALUES_EQUAL(occupied.size(), 2);
        UNIT_ASSERT(occupied.contains(TPDiskId(1, 1)));
        UNIT_ASSERT(occupied.contains(TPDiskId(2, 1)));

        create.GroupId = 3;
        UNIT_ASSERT(!mapper.AllocateGroupReassignment(std::move(create)).Success);
    }

    Y_UNIT_TEST(ReassignmentPrioritizesLayoutThenLocalityThenOperationalDisks) {
        TGroupMapper::TPlacementSnapshot snapshot;
        TGroupMapper::TReassignmentRequest request;
        request.GroupId = 1;
        request.GroupGeneration = 1;
        request.TryToRelocateLocallyFirst = true;
        request.IgnoreGroupLayoutChecks = true;
        for (ui32 nodeId = 1; nodeId <= 9; ++nodeId) {
            snapshot.PDisks.push_back({
                .PDiskId = TPDiskId(nodeId, 1),
                .Location = MakeTestLocation(nodeId),
                .NumActiveSlots = nodeId <= 8 ? 1u : 0u,
                .ExpectedSlotCount = 2,
                .SlotSizeInUnits = 1,
                .Operational = true,
            });
            if (nodeId <= 8) {
                request.VDisks.push_back({
                    .VDiskId = TVDiskIdShort(0, nodeId - 1, 0),
                    .PDiskId = TPDiskId(nodeId, 1),
                });
            }
        }
        auto local = snapshot.PDisks.front();
        local.PDiskId = TPDiskId(1, 2);
        local.NumActiveSlots = 0;
        local.Operational = false;
        snapshot.PDisks.push_back(std::move(local));
        request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDisk{};
        auto reassign = [&](bool onlyOperational = false) {
            return TGroupMapper::PlanGroupReassignment(
                TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8, 1),
                {.SettleOnlyOnOperationalDisks = onlyOperational}, snapshot, request);
        };
        auto checkPlacement = [&](bool onlyOperational, TPDiskId expected, bool layoutCorrect) {
            const auto outcome = reassign(onlyOperational);
            UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
            UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][0][0], expected);
            UNIT_ASSERT_VALUES_EQUAL(outcome.LayoutCorrect, layoutCorrect);
        };

        checkPlacement(false, TPDiskId(1, 2), true);
        checkPlacement(true, TPDiskId(9, 1), true);
        request.TryToRelocateLocallyFirst = false;
        checkPlacement(false, TPDiskId(9, 1), true);

        request.TryToRelocateLocallyFirst = true;
        snapshot.PDisks.front().Location = MakeTestLocation(1, 2);
        snapshot.PDisks.back().Location = MakeTestLocation(1, 2);
        snapshot.PDisks.back().Operational = true;
        snapshot.PDisks[8].Operational = false;
        checkPlacement(false, TPDiskId(9, 1), true);
        checkPlacement(true, TPDiskId(1, 2), false);

        request.IgnoreGroupLayoutChecks = false;
        UNIT_ASSERT(!reassign(true).Success);
        checkPlacement(false, TPDiskId(9, 1), true);
    }

    auto MakeLayoutOverrideSetup(ui32 numNodes = 9) {
        TGroupMapper::TPlacementSnapshot snapshot;
        TGroupMapper::TReassignmentRequest request;
        request.GroupId = 1;
        request.GroupGeneration = 1;
        request.MinimumRequiredSpace = 200;
        for (ui32 nodeId = 1; nodeId <= numNodes; ++nodeId) {
            snapshot.PDisks.push_back({
                .PDiskId = TPDiskId(nodeId, 1),
                .Location = MakeTestLocation(nodeId, nodeId == 9 ? 2 : nodeId),
                .NumActiveSlots = nodeId <= 8 ? 1u : 0u,
                .ExpectedSlotCount = 2,
                .SlotSizeInUnits = 1,
                .SlotSizeInBytes = 1000,
                .Space = TGroupMapper::TPDiskSpaceState{.AvailableSize = 1000, .TotalSize = 2000},
                .Operational = true,
            });
            if (nodeId <= 8) {
                request.VDisks.push_back({
                    .VDiskId = TVDiskIdShort(0, nodeId - 1, 0),
                    .PDiskId = TPDiskId(nodeId, 1),
                });
            }
        }
        request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDisk{};
        return std::pair{std::move(snapshot), std::move(request)};
    }

    TGroupMapper::TReassignmentOutcome ReassignBlock42Group(const TGroupMapper::TPlacementSnapshot& snapshot,
                                                            const TGroupMapper::TReassignmentRequest& request,
                                                            TGroupMapper::TOptions options = {}) {
        return TGroupMapper::PlanGroupReassignment(
            TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8, 1),
            options, snapshot, request);
    }

    void CheckLayoutOverridePreservesTargetPolicies(bool explicitTarget) {
        auto [snapshot, request] = MakeLayoutOverrideSetup();
        auto setReassignment = [&] {
            if (explicitTarget) {
                request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{TPDiskId(9, 1)};
            } else {
                request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDisk{};
            }
        };
        setReassignment();
        auto reassign = [&](TGroupMapper::TOptions options = {}) {
            return ReassignBlock42Group(snapshot, request, options);
        };

        UNIT_ASSERT(!reassign().Success);
        request.IgnoreGroupLayoutChecks = true;
        const auto outcome = reassign();
        UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
        UNIT_ASSERT(!outcome.LayoutCorrect);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][0][0], TPDiskId(9, 1));

        auto& target = snapshot.PDisks.back();
        const auto eligible = target;
        target.Usable = false;
        UNIT_ASSERT(!reassign().Success);
        target = eligible;
        target.NumActiveSlots = target.ExpectedSlotCount;
        UNIT_ASSERT(!reassign().Success);
        target = eligible;
        target.SlotSizeInBytes = 100;
        UNIT_ASSERT(!reassign().Success);
        target = eligible;
        target.Space->AvailableSize = 100;
        UNIT_ASSERT(!reassign().Success);
        target = eligible;
        target.Operational = false;
        UNIT_ASSERT(!reassign({.SettleOnlyOnOperationalDisks = true}).Success);
        target = eligible;
        request.ForbiddenPDisks.insert(target.PDiskId);
        UNIT_ASSERT(!reassign().Success);
        request.ForbiddenPDisks.clear();
        request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{TPDiskId(2, 1)};
        UNIT_ASSERT(!reassign().Success);
        request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDisk{.RequireSameNode = true};
        UNIT_ASSERT(!reassign().Success);

        setReassignment();
        request.IgnoreGroupLayoutChecks = false;
        target.Location = MakeTestLocation(9);
        const auto repaired = reassign();
        UNIT_ASSERT_C(repaired.Success, repaired.Error.ErrorMessage);
        UNIT_ASSERT(repaired.LayoutCorrect);
    }

    Y_UNIT_TEST(LayoutOverridePreservesTargetPolicies) {
        CheckLayoutOverridePreservesTargetPolicies(true);
    }

    Y_UNIT_TEST(LayoutOverridePreservesAutomaticPlacementPolicies) {
        CheckLayoutOverridePreservesTargetPolicies(false);
    }

    Y_UNIT_TEST(LayoutOverrideRespectsMixedTargets) {
        auto [snapshot, request] = MakeLayoutOverrideSetup(10);
        request.IgnoreGroupLayoutChecks = true;
        request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{TPDiskId(9, 1)};
        request.VDisks.back().Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{TPDiskId(9, 1)};
        UNIT_ASSERT(!ReassignBlock42Group(snapshot, request).Success);

        request.VDisks.front().Reassignment = TGroupMapper::TReplaceVDisk{};
        const auto outcome = ReassignBlock42Group(snapshot, request);
        UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][0][0], TPDiskId(10, 1));
        UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][7][0], TPDiskId(9, 1));
        UNIT_ASSERT(!outcome.LayoutCorrect);
    }

    Y_UNIT_TEST(LayoutOverridePrefersCorrectLayout) {
        auto [snapshot, request] = MakeLayoutOverrideSetup(10);
        request.IgnoreGroupLayoutChecks = true;
        snapshot.PDisks.back().NumActiveSlots = 1;
        const auto outcome = ReassignBlock42Group(snapshot, request);
        UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][0][0], TPDiskId(10, 1));
        UNIT_ASSERT(outcome.LayoutCorrect);
    }

    Y_UNIT_TEST(LayoutOverridePreservesRequiredNodeInMixedPlacement) {
        auto [snapshot, request] = MakeLayoutOverrideSetup(10);
        request.IgnoreGroupLayoutChecks = true;
        snapshot.PDisks[8].PDiskId = TPDiskId(8, 2);
        snapshot.PDisks[8].Location = MakeTestLocation(8);
        snapshot.PDisks.back().Location = MakeTestLocation(10, 2);
        request.VDisks.back().Reassignment = TGroupMapper::TReplaceVDisk{.RequireSameNode = true};
        const auto outcome = ReassignBlock42Group(snapshot, request);
        UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][0][0], TPDiskId(10, 1));
        UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][7][0], TPDiskId(8, 2));
        UNIT_ASSERT(!outcome.LayoutCorrect);
    }

    Y_UNIT_TEST(LayoutOverrideCanReplaceWholeRealmOrGroup) {
        for (ui32 numReplaced : {3u, 9u}) {
            TGroupMapper::TPlacementSnapshot snapshot;
            TGroupMapper::TReassignmentRequest request;
            request.GroupId = 1;
            request.GroupGeneration = 1;
            for (ui32 nodeId = 1; nodeId <= 9 + numReplaced; ++nodeId) {
                NActorsInterconnect::TNodeLocation location;
                location.SetDataCenter(ToString(nodeId <= 9 ? (nodeId - 1) / 3 : 1));
                location.SetRack(ToString(nodeId <= 9 ? nodeId : 4));
                snapshot.PDisks.push_back({
                    .PDiskId = TPDiskId(nodeId, 1),
                    .Location = TNodeLocation(location),
                    .NumActiveSlots = nodeId <= 9 ? 1u : 0u,
                    .ExpectedSlotCount = 2,
                    .SlotSizeInUnits = 1,
                    .Operational = true,
                });
                if (nodeId <= 9) {
                    auto& disk = request.VDisks.emplace_back();
                    disk.VDiskId = TVDiskIdShort((nodeId - 1) / 3, (nodeId - 1) % 3, 0);
                    disk.PDiskId = TPDiskId(nodeId, 1);
                    if (nodeId <= numReplaced) {
                        disk.Reassignment = TGroupMapper::TReplaceVDisk{};
                    }
                }
            }
            auto reassign = [&] {
                return TGroupMapper::PlanGroupReassignment(
                    TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1),
                    {}, snapshot, request);
            };
            UNIT_ASSERT(!reassign().Success);
            request.IgnoreGroupLayoutChecks = true;
            const auto outcome = reassign();
            UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
            UNIT_ASSERT(!outcome.LayoutCorrect);
            TSet<TPDiskId> used;
            TGroupMapper::Traverse(outcome.Group, [&](TVDiskIdShort id, TPDiskId pdiskId) {
                const ui32 oldNode = id.FailRealm * 3 + id.FailDomain + 1;
                UNIT_ASSERT(used.insert(pdiskId).second);
                if (oldNode <= numReplaced) {
                    UNIT_ASSERT(pdiskId.NodeId > 9);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(pdiskId, TPDiskId(oldNode, 1));
                }
            });
        }
    }

    Y_UNIT_TEST(ReassignmentRequiredSpaceFromUntouchedVDiskState) {
        TGroupMapper::TPlacementSnapshot state;
        auto addPDisk = [&](TPDiskId pdiskId, ui32 numActiveSlots) {
            state.PDisks.push_back({
                .PDiskId = pdiskId,
                .Location = MakeTestLocation(pdiskId.NodeId),
                .NumActiveSlots = numActiveSlots,
                .ExpectedSlotCount = 2,
                .SlotSizeInUnits = 1,
                .Operational = true,
            });
        };
        addPDisk(TPDiskId(1, 1), 1);
        addPDisk(TPDiskId(2, 1), 1);
        addPDisk(TPDiskId(3, 1), 0);

        state.VSlots = {
            {
                .VSlotId = TVSlotId(TPDiskId(1, 1), 1),
                .PDiskId = TPDiskId(1, 1),
                .GroupId = 1,
                .GroupGeneration = 1,
                .VDiskId = TVDiskIdShort(0, 0, 0),
                .CountedInNumActiveSlots = false,
                .AllocatedSize = 500,
            },
            {
                .VSlotId = TVSlotId(TPDiskId(2, 1), 1),
                .PDiskId = TPDiskId(2, 1),
                .GroupId = 1,
                .GroupGeneration = 1,
                .VDiskId = TVDiskIdShort(0, 1, 0),
                .CountedInNumActiveSlots = false,
                .AllocatedSize = 100,
            },
        };

        TGroupMapper::TReassignmentRequest request;
        request.GroupId = 1;
        request.GroupGeneration = 1;
        request.VDisks = {
            {
                .VDiskId = TVDiskIdShort(0, 0, 0),
                .PDiskId = TPDiskId(1, 1),
            },
            {
                .VDiskId = TVDiskIdShort(0, 1, 0),
                .PDiskId = TPDiskId(2, 1),
                .Reassignment = TGroupMapper::TReplaceVDisk{},
            },
        };

        auto geometry = TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 2, 1);
        const auto outcome = TGroupMapper::PlanGroupReassignment(std::move(geometry), {.IgnoreVSlotQuotaCheck = true},
                                                                 std::move(state), std::move(request));
        UNIT_ASSERT_C(outcome.Success, outcome.Error.ErrorMessage);
        UNIT_ASSERT_VALUES_EQUAL(outcome.RequiredSpace, 500);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Group[0][1][0], TPDiskId(3, 1));
    }

    Y_UNIT_TEST(SimplestMirror3dc) {
        // Each node has 3 disks
        TTestContext context(
            {
                {1, 1, 1, 1, 3},
                {2, 1, 2, 1, 3},
                {3, 1, 3, 1, 3},
            }
        );

        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, 10, 20, 10, 256));
        context.PopulateGroupMapper(mapper, 1);

        TGroupMapper::TGroupDefinition g1;
        UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, g1));
        context.DumpGroup(g1);

        for (ui32 numActiveSlots : context.GetNumActiveSlots()) {
            UNIT_ASSERT_VALUES_EQUAL(1, numActiveSlots);
        }

        TGroupMapper::TGroupDefinition g2;
        UNIT_ASSERT_EQUAL(0, context.AllocateGroup(mapper, g2, {}, true));
        context.DumpGroup(g2);

        for (ui32 numActiveSlots : context.GetNumActiveSlots()) {
            UNIT_ASSERT_VALUES_EQUAL(1, numActiveSlots);
        }
    }

    Y_UNIT_TEST(DifferentGroupSizeInUnits) {
        {
            TTestContext context(1, 1, 1, 1, std::vector({1u}));

            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureNone, 1, 1, 1));
            context.PopulateGroupMapper(mapper, 3);

            UNIT_ASSERT_VALUES_EQUAL(context.GetTotalDisks(), 1);
            UNIT_ASSERT_VALUES_EQUAL(0, context.GetNumActiveSlots()[0]);

            TGroupMapper::TGroupDefinition g;

            UNIT_ASSERT_EQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 4u, true));
            UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 2u));
            UNIT_ASSERT_VALUES_EQUAL(2, context.GetNumActiveSlots()[0]);

            UNIT_ASSERT_EQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 4u, true));
            UNIT_ASSERT_EQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 2u, true));
            UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 1u));
            UNIT_ASSERT_VALUES_EQUAL(3, context.GetNumActiveSlots()[0]);

            // error# no group options PDisks# {[(1:1-s[2/2])]}
            UNIT_ASSERT_EQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 1u, true));
        }

        {
            const ui32 numRacks = 12;
            const ui32 expectedSlotCount = 2;
            TTestContext context(1, 1, numRacks, 1, {1u, 2u, 4u});
            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8, 1));
            context.PopulateGroupMapper(mapper, expectedSlotCount);
            UNIT_ASSERT_VALUES_EQUAL(context.GetTotalDisks(), numRacks*3);

            auto NumActiveSlotsOnPDisk = [&](ui32 pdisk) {
                ui32 sum = 0;
                auto slots = context.GetNumActiveSlots();
                for (ui32 rack = 0; rack < numRacks; rack++) {
                    sum += slots[rack*3 + pdisk];
                }
                return sum;
            };

            TGroupMapper::TGroupDefinition g;

            // Matching SizeInUnits has better score than FreeSlots
            UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 2u));
            UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 2u));
            UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 2u));
            // First 3 groups all occupy double-unit pdisk
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(0), 0);
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(1), 8*3);
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(2), 0);

            // Better occupy smaller pdisk than bigger
            UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 2u));
            // The next group occupies single-unit pdisks
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(0), 8*1*2);
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(1), 8*3*1);
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(2), 0);

            UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, (g.clear(), g), 2u));
            // One more group occupies 4 single-unit pdisks and 4 quad-unit
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(0), 8*1*2 + 4*1*2);
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(1), 8*3*1);
            UNIT_ASSERT_VALUES_EQUAL(NumActiveSlotsOnPDisk(2), 4*1*1);
        }
    }

    Y_UNIT_TEST(MapperSequentialCalls) {
        TTestContext globalContext(3, 3, 4, 3, 4);
        TTestContext localContext(3, 3, 4, 3, 4);

        TGroupMapper globalMapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8, 2));
        globalContext.PopulateGroupMapper(globalMapper, 16);
        for (ui32 i = 0; i < globalContext.GetTotalDisks(); ++i) {
            Ctest << i << "/" << globalContext.GetTotalDisks() << Endl;

            TGroupMapper::TGroupDefinition group;
            globalContext.AllocateGroup(globalMapper, group);

            TGroupMapper localMapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8, 2));
            localContext.PopulateGroupMapper(localMapper, 16);
            TGroupMapper::TGroupDefinition localGroup;
            localContext.AllocateGroup(localMapper, localGroup);

            UNIT_ASSERT_EQUAL(localGroup, group);
        }
    }

    void TestBlock42(ui32 numVDisksPerFailDomain) {
        TTestContext context(3, 4, 20, 5, 4);
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8, numVDisksPerFailDomain));
        context.PopulateGroupMapper(mapper, 8 * numVDisksPerFailDomain);
        for (ui32 i = 0; i < context.GetTotalDisks(); ++i) {
            Ctest << i << "/" << context.GetTotalDisks() << Endl;
            TGroupMapper::TGroupDefinition group;
            context.AllocateGroup(mapper, group);
            context.CheckGroupErasure(group);
        }
        TVector<ui32> slots = context.GetNumActiveSlots();
        for (ui32 numActiveSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(8 * numVDisksPerFailDomain, numActiveSlots);
        }
        context.CheckIfGroupsAreMappedCompact();
    }

    Y_UNIT_TEST(Block42_1disk) {
        TestBlock42(1);
    }

    Y_UNIT_TEST(Mirror3dc) {
        TTestContext context(6, 3, 3, 3, 3);
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));
        context.PopulateGroupMapper(mapper, 9);
        for (ui32 i = 0; i < context.GetTotalDisks(); ++i) {
            Ctest << i << "/" << context.GetTotalDisks() << Endl;
            TGroupMapper::TGroupDefinition group;
            context.AllocateGroup(mapper, group);
            context.CheckGroupErasure(group);
        }
        TVector<ui32> slots = context.GetNumActiveSlots();
        for (ui32 numActiveSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(9, numActiveSlots);
        }
        context.CheckIfGroupsAreMappedCompact();
    }

    Y_UNIT_TEST(Mirror3dc3Nodes) {
        // Each node has 3 disks.
        TTestContext context(
            {
                {1, 1, 1, 1, 3},
                {2, 1, 2, 1, 3},
                {3, 1, 3, 1, 3},
            }
        );

        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, 10, 20, 10, 256));
        context.PopulateGroupMapper(mapper, 9);

        TGroupMapper::TGroupDefinition group;
        UNIT_ASSERT_UNEQUAL(0, context.AllocateGroup(mapper, group));
    }

    Y_UNIT_TEST(GroupMapperErrorExample) {
        // 3 dc 3 nodes config, but with incorrect domain level end, so this result in error
        TTestContext context(
            {
                {1, 1, 1, 1, 3},
                {2, 1, 2, 1, 3},
                {3, 1, 3, 1, 3},
            }
        );

        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, 10, 20, 10, 40));
        context.PopulateGroupMapper(mapper, 9);

        TGroupMapper::TGroupDefinition group;
        TGroupMapperError error;
        context.AllocateGroupCatchingError(mapper, group, error);
        UNIT_ASSERT_VALUES_EQUAL(error.FailRealmsWithMissingDomainsCount, 3);
        UNIT_ASSERT_VALUES_EQUAL(error.MissingFailRealmsCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(error.OkDisksCount, 9);
        UNIT_ASSERT_VALUES_EQUAL(error.DomainsWithMissingDisksCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(error.RealmLocationKey, "DataCenter");
        UNIT_ASSERT_VALUES_EQUAL(error.DomainLocationKey, "Rack");
    }

    Y_UNIT_TEST(NonUniformCluster) {
        std::vector<std::tuple<ui32, ui32, ui32, ui32, ui32>> disks;
        for (ui32 rack = 0, body = 0; rack < 12; ++rack) {
            for (ui32 i = 0; i < (rack < 4 ? 32 : 33); ++i, ++body) {
                disks.emplace_back(1, 1, rack, body, 8);
            }
        }
        std::random_shuffle(disks.begin(), disks.end());
        TTestContext context(disks);
        UNIT_ASSERT_VALUES_EQUAL(8 * (4 * 32 + 8 * 33), context.GetTotalDisks());
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
        context.PopulateGroupMapper(mapper, 8);
        for (ui32 i = 0; i < context.GetTotalDisks(); ++i) {
            Ctest << i << "/" << context.GetTotalDisks() << Endl;
            TGroupMapper::TGroupDefinition group;
            context.AllocateGroup(mapper, group);
            context.CheckGroupErasure(group);
        }
        TVector<ui32> slots = context.GetNumActiveSlots();
        for (ui32 numActiveSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(8, numActiveSlots);
        }
    }

    Y_UNIT_TEST(InterlacedRacksWithoutInterlacedNodes) {
        TTestContext context(
            {
                {1, 1, 1, 1, 1}, // node 1
                {1, 1, 2, 2, 1},
                {1, 1, 3, 3, 2}, // node 3 has two disks
                {1, 1, 4, 4, 1},
                {1, 1, 5, 5, 1},
                {1, 1, 6, 6, 1},
                {1, 1, 2, 7, 1}, // node 7 is in the same rack as node 2
                {1, 1, 8, 8, 1},
                {1, 1, 3, 9, 1}, // node 9 is in the same rack as node 3
            }
        );

        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
        context.PopulateGroupMapper(mapper, 8);

        TGroupMapper::TGroupDefinition group;
        group.emplace_back(TVector<TVector<TPDiskId>>(8));
        auto& g = group[0];

        for (int i = 0; i < 8; i++) {
            g[i].emplace_back(TPDiskId(i + 1, 1));
        }

        context.SetGroup(1, group);

        TGroupMapper::TGroupDefinition newGroup = context.ReallocateGroup(mapper, 1, {TPDiskId(8, 1)});

        UNIT_ASSERT_EQUAL_C(TPDiskId(9, 1), newGroup[0][7][0], context.FormatGroup(newGroup));
    }

    Y_UNIT_TEST(WithAttentionToRacksAndReplication) {
        TTestContext context(
            {
                // DC 1
                // Rack 1
                {1, 1, 1, 1, 3},
                {1, 1, 1, 2, 3},
                // Rack 2
                {1, 1, 2, 1, 3},
                {1, 1, 2, 2, 3},
                // Rack 3
                {1, 1, 3, 1, 3},
                {1, 1, 3, 2, 3},
                // Rack 4
                {1, 1, 4, 1, 3},
                {1, 1, 4, 2, 3},

                // DC 2
                // Rack 1
                {2, 1, 1, 1, 3},
                {2, 1, 1, 2, 3},
                // Rack 2
                {2, 1, 2, 1, 3},
                {2, 1, 2, 2, 3},
                // Rack 3
                {2, 1, 3, 1, 3},
                {2, 1, 3, 2, 3},

                // DC 3
                // Rack 1
                {3, 1, 1, 1, 3},
                {3, 1, 1, 2, 3},
                // Rack 2
                {3, 1, 2, 1, 3},
                {3, 1, 2, 2, 3},
                // Rack 3
                {3, 1, 3, 1, 3},
                {3, 1, 3, 2, 3},
            }
        );

        TGroupMapper::TGroupDefinition group;

        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1), false, true, true);
        TPDiskSlotTracker s;
        mapper.SetPDiskSlotTracker(std::move(s));
        context.PopulateGroupMapper(mapper, 8);

        ui32 groupId = context.AllocateGroup(mapper, group);

        // All disks and racks are in the same state, so we pick a disk on node 7 (by NumDomainMatchingDisks heuristic)
        TGroupMapper::TGroupDefinition newGroup = context.ReallocateGroup(mapper, groupId, {TPDiskId(1, 1)});
        UNIT_ASSERT_EQUAL_C(TPDiskId(7, 1), context.GetGroupDiskId(1), context.FormatGroup(1));

        // This time rack 4 has more free slots than other racks in DC 1, it will be picked
        s = TPDiskSlotTracker();
        s.AddFreeSlotsForRack("DC=1/M=1/4", 1);
        mapper.SetPDiskSlotTracker(std::move(s));
        newGroup = context.ReallocateGroup(mapper, groupId, {TPDiskId(7, 1)});
        UNIT_ASSERT_EQUAL_C(TPDiskId(7, 2), context.GetGroupDiskId(1), context.FormatGroup(1));

        // Now we only select from the first rack, but we will change replicating disks per node.
        s = TPDiskSlotTracker();
        s.AddFreeSlotsForRack("DC=1/M=1/1", 1);
        // Now node 1 has more replicating disks, so other node will be picked.
        s.AddReplicatingVSlot(TPDiskId(1, 1));
        mapper.SetPDiskSlotTracker(std::move(s));
        newGroup = context.ReallocateGroup(mapper, groupId, {TPDiskId(7, 2)});
        UNIT_ASSERT_EQUAL_C(TPDiskId(2, 1), context.GetGroupDiskId(1), context.FormatGroup(1));

        // Now select from the first node only. Pick the disk
        s = TPDiskSlotTracker();
        s.AddFreeSlotsForRack("DC=1/M=1/1", 1);
        // Both disks on node 1 has replicating slots, so we will pick disk 2 on node 2.
        s.AddReplicatingVSlot(TPDiskId(1, 1));
        s.AddReplicatingVSlot(TPDiskId(1, 2));
        mapper.SetPDiskSlotTracker(std::move(s));
        newGroup = context.ReallocateGroup(mapper, groupId, {TPDiskId(2, 1)});
        UNIT_ASSERT_EQUAL_C(TPDiskId(2, 2), context.GetGroupDiskId(1), context.FormatGroup(1));
    }

    Y_UNIT_TEST(NonUniformClusterDifferentSlotsPerDisk) {
        std::vector<std::tuple<ui32, ui32, ui32, ui32, ui32>> disks;
        for (ui32 rack = 0; rack < 12; ++rack) {
            disks.emplace_back(1, 1, rack, 1, 1);
        }
        std::random_shuffle(disks.begin(), disks.end());
        TTestContext context(disks);
        UNIT_ASSERT_VALUES_EQUAL((8 + 4), context.GetTotalDisks());
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
        context.PopulateGroupMapper(mapper, 8, {}, {}, std::nullopt, false);
        for (ui32 i = 0; i < 16; ++i) {
            Ctest << i << "/" << 16 << Endl;
            TGroupMapper::TGroupDefinition group;
            context.AllocateGroup(mapper, group);
            context.CheckGroupErasure(group);
        }
        TVector<ui32> slots = context.GetNumActiveSlots();
        ui64 slots_total = 0;
        for (ui32 numActiveSlots : slots) {
            slots_total += numActiveSlots;
            Ctest << "slots " << numActiveSlots << " ";
        }
        Ctest << slots_total << Endl;
        UNIT_ASSERT_VALUES_EQUAL(slots_total, 8 * 8 + 4 * 16);
    }

    Y_UNIT_TEST(NonUniformCluster2) {
        std::vector<std::tuple<ui32, ui32, ui32, ui32, ui32>> disks;
        for (ui32 rack = 0, body = 0; rack < 12; ++rack) {
            ui32 array[] = {
                168, 168, 168, 168,
                96, 96, 96, 96,
                80, 80, 80, 80,
            };
            ui32 numDisks = array[rack];
            for (ui32 i = 0; i < numDisks / 8; ++i, ++body) {
                disks.emplace_back(1, 1, rack, body, 8);
            }
        }
        std::random_shuffle(disks.begin(), disks.end());
        TTestContext context(disks);
        UNIT_ASSERT_VALUES_EQUAL(8 * (168 + 96 + 80) / 2, context.GetTotalDisks());
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
        context.PopulateGroupMapper(mapper, 8);
        for (ui32 i = 0; i < context.GetTotalDisks(); ++i) {
            Ctest << i << "/" << context.GetTotalDisks() << Endl;
            TGroupMapper::TGroupDefinition group;
            context.AllocateGroup(mapper, group);
            context.CheckGroupErasure(group);
        }
        TVector<ui32> slots = context.GetNumActiveSlots();
        for (ui32 numActiveSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(8, numActiveSlots);
        }
    }

    Y_UNIT_TEST(NonUniformClusterMirror3dc) {
        std::vector<std::vector<ui32>> disposition{
            { // datacenter1
                4, 4, 4, 4, 4, 2, 2, 4, 2, 5, 5, 5,
            },
            { // datacenter2
                2, 2, 2, 2, 2, 2, 1, 1, 2, 4, 8, 8, 9,
            },
            { // datacenter3
                4, 4, 1, 3, 4, 4, 2, 6, 9, 8,
            },
        };
        TTestContext context(disposition, 4);
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));
        context.PopulateGroupMapper(mapper, 9);
        for (ui32 i = 0; i < context.GetTotalDisks(); ++i) {
            Ctest << i << "/" << context.GetTotalDisks() << Endl;
            TGroupMapper::TGroupDefinition group;
            context.AllocateGroup(mapper, group);
            context.CheckGroupErasure(group);

            TVector<ui32> slots = context.GetNumActiveSlots();
            UNIT_ASSERT(slots);
            ui32 min = slots[0];
            ui32 max = slots[0];
            for (size_t i = 1; i < slots.size(); ++i) {
                min = Min(min, slots[i]);
                max = Max(max, slots[i]);
            }
            UNIT_ASSERT_C(max - min <= 1, Sprintf("min# %" PRIu32 " max# %" PRIu32, min, max));
        }
        TVector<ui32> slots = context.GetNumActiveSlots();
        for (ui32 numActiveSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(9, numActiveSlots);
        }
    }

    Y_UNIT_TEST(NonUniformClusterMirror3dcWithUnusableDomain) {
        std::vector<std::vector<ui32>> disposition{
            { // datacenter1
                4, 4, 4, 4, 4, 2, 2, 4, 2, 5, 5, 5,
            },
            { // datacenter2
                2, 2, 2, 2, 2, 2, 1, 1, 2, 4, 8, 8, 9,
            },
            { // datacenter3
                4, 4, 1, 3, 4, 4, 2, 6, 9, 8,
            },
            { // datacenter4
                1,
            },
        };
        TTestContext context(disposition, 4);
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));
        context.PopulateGroupMapper(mapper, 9);
        for (ui32 i = 0; i < context.GetTotalDisks() - 4; ++i) {
            Ctest << i << "/" << (context.GetTotalDisks() - 4) << Endl;
            TGroupMapper::TGroupDefinition group;
            context.AllocateGroup(mapper, group);
            context.CheckGroupErasure(group);

            TVector<ui32> slots = context.GetNumActiveSlots();
            UNIT_ASSERT(slots);
            ui32 min = Max<ui32>();
            ui32 max = 0;
            for (const ui32 x : slots) {
                if (x) {
                    min = Min(min, x);
                    max = Max(max, x);
                }
            }
            UNIT_ASSERT_C(max - min <= 1, Sprintf("min# %" PRIu32 " max# %" PRIu32, min, max));
        }
        TVector<ui32> slots = context.GetNumActiveSlots();
        for (ui32 numActiveSlots : slots) {
            if (numActiveSlots) {
                UNIT_ASSERT_VALUES_EQUAL(9, numActiveSlots);
            }
        }
    }

    Y_UNIT_TEST(MakeDisksUnusable) {
        TTestContext context(1, 1, 10, 1, 1);
        TVector<ui32> groupIds;
        {
            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
            context.PopulateGroupMapper(mapper, 8);
            for (ui32 i = 0; i < context.GetTotalDisks(); ++i) {
                TGroupMapper::TGroupDefinition group;
                ui32 groupId = context.AllocateGroup(mapper, group);
                groupIds.push_back(groupId);
                Ctest << "groupId# " << groupId << " content# " << context.FormatGroup(group) << Endl;
                context.CheckGroupErasure(group);
                context.ReallocateGroup(mapper, groupId, {});
            }
        }
        Ctest << "remapping disks" << Endl;
        {
            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
            TSet<TPDiskId> unusableDisks;
            context.IterateGroups([&](const auto& pdisks) {
                for (const TPDiskId& pdiskId : pdisks) {
                    if (unusableDisks.size() < 2) {
                        if (unusableDisks.insert(pdiskId).second) {
                            Ctest << "making unusable disk# " << pdiskId.ToString() << Endl;
                        }
                    }
                }
            });
            context.PopulateGroupMapper(mapper, 10, unusableDisks);
            for (ui32 groupId : groupIds) {
                auto group = context.ReallocateGroup(mapper, groupId, unusableDisks);
                Ctest << "groupId# " << groupId << " new content# " << context.FormatGroup(group) << Endl;
                context.CheckGroupErasure(group);
            }
        }
    }

    Y_UNIT_TEST(MakeDisksNonoperational) {
        TTestContext context(1, 1, 10, 1, 1);
        TVector<ui32> groupIds;
        {
            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
            context.PopulateGroupMapper(mapper, 8);
            for (ui32 i = 0; i < context.GetTotalDisks(); ++i) {
                TGroupMapper::TGroupDefinition group;
                ui32 groupId = context.AllocateGroup(mapper, group);
                groupIds.push_back(groupId);
                Ctest << "groupId# " << groupId << " content# " << context.FormatGroup(group) << Endl;
                context.CheckGroupErasure(group);
                context.ReallocateGroup(mapper, groupId, {});
            }
        }
        Ctest << "remapping disks" << Endl;
        {
            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
            TSet<TPDiskId> unusableDisks, nonoperationalDisks;
            context.IterateGroups([&](const auto& pdisks) {
                for (const TPDiskId& pdiskId : pdisks) {
                    if (unusableDisks.size() < 2) {
                        if (unusableDisks.insert(pdiskId).second) {
                            Ctest << "making unusable disk# " << pdiskId.ToString() << Endl;
                            continue;
                        }
                    }
                }
            });
            context.IteratePDisks([&](const auto& pdiskId, const auto&) {
                nonoperationalDisks.insert(pdiskId);
            });
            context.PopulateGroupMapper(mapper, 10, unusableDisks, nonoperationalDisks);
            ui32 hasEmpty = false;
            for (ui32 groupId : groupIds) {
                auto tmp = context.ReallocateGroup(mapper, groupId, unusableDisks, false, true, true);
                hasEmpty |= tmp.empty();
                auto group = context.ReallocateGroup(mapper, groupId, unusableDisks);
                Ctest << "groupId# " << groupId << " new content# " << context.FormatGroup(group) << Endl;
                context.CheckGroupErasure(group);
            }
            UNIT_ASSERT(hasEmpty);
        }
    }

    Y_UNIT_TEST(MakeDisksForbidden) {
        TTestContext context(1, 1, 10, 1, 1);
        TVector<ui32> groupIds;
        {
            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
            context.PopulateGroupMapper(mapper, 8);
            for (ui32 i = 0; i < context.GetTotalDisks(); ++i) {
                TGroupMapper::TGroupDefinition group;
                ui32 groupId = context.AllocateGroup(mapper, group);
                groupIds.push_back(groupId);
                Ctest << "groupId# " << groupId << " content# " << context.FormatGroup(group) << Endl;
                context.CheckGroupErasure(group);
                context.ReallocateGroup(mapper, groupId, {});
            }
        }
        Ctest << "remapping disks" << Endl;
        {
            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
            TSet<TPDiskId> unusableDisks;
            context.IterateGroups([&](const auto& pdisks) {
                for (const TPDiskId& pdiskId : pdisks) {
                    if (unusableDisks.size() < 2) {
                        if (unusableDisks.insert(pdiskId).second) {
                            Ctest << "making unusable disk# " << pdiskId.ToString() << Endl;
                        }
                    }
                }
            });
            context.PopulateGroupMapper(mapper, 10, {});
            for (ui32 groupId : groupIds) {
                auto group = context.ReallocateGroup(mapper, groupId, unusableDisks, true);
                Ctest << "groupId# " << groupId << " new content# " << context.FormatGroup(group) << Endl;
                context.CheckGroupErasure(group);
            }
        }
    }

    Y_UNIT_TEST(MonteCarlo) {
        auto rand = [](ui32 min, ui32 max) {
            return min + RandomNumber(max - min + 1);
        };
        for (size_t k = 0; k < 1000; ++k) {
            std::vector<std::tuple<ui32, ui32, ui32, ui32, ui32>> disks;

            const ui32 numDisks = rand(2, 4);
            ui32 numDataCenters = rand(1, 3);
            for (ui32 dataCenter = 1; dataCenter <= numDataCenters; ++dataCenter) {
                const ui32 room = 1;
                ui32 numRacks = rand(8, 12);
                for (ui32 rack = 1; rack <= numRacks; ++rack) {
                    ui32 numBodies = rand(3, 5);
                    for (ui32 body = 1; body <= numBodies; ++body) {
                        disks.emplace_back(dataCenter, room, rack, body, numDisks);
                    }
                }
            }

            Ctest << "iteration# " << k << " numBodies# " << disks.size() << " numDisks# " << numDisks << Endl;

            const ui32 expectedSlotCount = 16;
            TTestContext context(std::move(disks));
            context.IteratePDisks([&](auto&, auto& v) {
                v.NumActiveSlots = rand(0, expectedSlotCount);
            });
            for (;;) {
                Ctest << "spawning new mapper" << Endl;
                TGroupMapper mapper(TTestContext::CreateGroupGeometry(numDataCenters >= 3
                    ? TBlobStorageGroupType::ErasureMirror3dc
                    : TBlobStorageGroupType::Erasure4Plus2Block));
                context.PopulateGroupMapper(mapper, expectedSlotCount);
                TGroupMapper::TGroupDefinition group;
                while (context.AllocateGroup(mapper, group, 0u, true)) {
                    group.clear();
                    if (rand(0, 99) < 5) {
                        goto next_cycle;
                    }
                }
                break;
            next_cycle:;
            }
        }
    }

    Y_UNIT_TEST(ReassignGroupTest3dc) {
        for (ui32 i = 0; i < 10000; ++i) {
            Ctest << "iteration# " << i << Endl;

            const ui32 numDataCenters = 5;
            const ui32 numRacks = 5;
            TTestContext context(numDataCenters, 1, numRacks, 1, 1);

            TGroupMapper::TGroupDefinition group;
            ui32 groupId;
            {
                TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));
                context.PopulateGroupMapper(mapper, 1);
                groupId = context.AllocateGroup(mapper, group);
                Ctest << "group after allocation:" << Endl;
                context.DumpGroup(group);
            }

            ui32 decommittedDataCenter = RandomNumber<ui32>(numDataCenters + 1);
            Ctest << "decommittedDataCenter# " << decommittedDataCenter << Endl;
            {
                // randomly move some of disks from decommitted datacenter
                TSet<TPDiskId> unusableDisks;
                for (auto& realm : group) {
                    for (auto& domain : realm) {
                        for (auto& pdisk : domain) {
                            if (context.GetDataCenter(pdisk) == decommittedDataCenter && RandomNumber(2u)) {
                                unusableDisks.insert(pdisk);
                            }
                        }
                    }
                }

                TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));
                context.PopulateGroupMapper(mapper, 1, {}, {}, decommittedDataCenter);
                group = context.ReallocateGroup(mapper, groupId, unusableDisks);
                Ctest << "group after data center decommission:" << Endl;
                context.DumpGroup(group);
            }

            TSet<TPDiskId> unusableDisks;
            ui32 unusableDataCenter = RandomNumber<ui32>(numDataCenters + 1);
            Ctest << "unusableDataCenter# " << unusableDataCenter << Endl;
            if (unusableDataCenter) {
                context.IteratePDisks([&](const auto& pdiskId, const auto& record) {
                    if (record.DataCenterId == unusableDataCenter) {
                        unusableDisks.insert(pdiskId);
                    }
                });
            }

            for (ui32 i = 0; i < 2; ++i) {
                if (const ui32 unusableDataCenter = RandomNumber<ui32>(numDataCenters + 1)) {
                    const ui32 unusableRack = 1 + RandomNumber<ui32>(numRacks);
                    context.IteratePDisks([&](const auto& pdiskId, const auto& record) {
                        if (record.DataCenterId == unusableDataCenter && record.RackId == unusableRack) {
                            unusableDisks.insert(pdiskId);
                        }
                    });
                }
            }

            {
                TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));
                context.PopulateGroupMapper(mapper, 1);
                auto group = context.ReallocateGroup(mapper, groupId, unusableDisks);
                Ctest << "group after reallocation:" << Endl;
                context.DumpGroup(group);
                context.CheckGroupErasure(group, decommittedDataCenter);
            }

            Ctest << Endl;
        }
    }

    Y_UNIT_TEST(ManualTargetPDiskConstraint) {
        TTestContext context(1, 1, 12, 1, 2);
        const auto geometry = TTestContext::CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block);

        TGroupMapper mapper(geometry);
        context.PopulateGroupMapper(mapper, 8);

        TGroupMapper::TGroupDefinition group;
        const ui32 groupId = context.AllocateGroup(mapper, group);

        struct TPlacement {
            TVDiskIdShort VDisk;
            TPDiskId PDisk;
        };

        TVector<TPlacement> placements;
        TSet<ui32> usedNodes;
        TGroupMapper::Traverse(group, [&](TVDiskIdShort vdiskId, TPDiskId pdiskId) {
            placements.push_back({vdiskId, pdiskId});
            usedNodes.insert(pdiskId.NodeId);
        });

        const size_t expectedSlots = geometry.GetNumFailRealms()
            * geometry.GetNumFailDomainsPerFailRealm()
            * geometry.GetNumVDisksPerFailDomain();
        UNIT_ASSERT_VALUES_EQUAL(placements.size(), expectedSlots);

        const auto source = placements.front();
        const auto conflicting = placements[1];

        std::optional<TPDiskId> invalidTarget;
        std::optional<TPDiskId> validTarget;
        context.IteratePDisks([&](const TPDiskId& pdiskId, const auto&) {
            if (!invalidTarget && pdiskId.NodeId == conflicting.PDisk.NodeId && pdiskId != conflicting.PDisk) {
                invalidTarget = pdiskId;
            }
            if (!validTarget && !usedNodes.count(pdiskId.NodeId)) {
                validTarget = pdiskId;
            }
        });

        UNIT_ASSERT(invalidTarget);
        UNIT_ASSERT(validTarget);

        auto allocateWithTarget = [&](TPDiskId target) {
            TGroupMapper localMapper(geometry);
            context.PopulateGroupMapper(localMapper, 8);

            auto candidate = group;
            candidate[source.VDisk.FailRealm][source.VDisk.FailDomain][source.VDisk.VDisk] = TPDiskId();

            TGroupMapper::TGroupConstraintsDefinition constraints;
            UNIT_ASSERT(geometry.ResizeGroup(constraints));
            constraints[source.VDisk.FailRealm][source.VDisk.FailDomain][source.VDisk.VDisk].PDiskId = target;

            THashMap<TVDiskIdShort, TPDiskId> replacedDisks;
            replacedDisks.emplace(source.VDisk, source.PDisk);

            TGroupMapperError error;
            const bool success = localMapper.AllocateGroup(groupId, candidate, constraints, replacedDisks, {}, 0, 0, false,
                TBridgePileId(), error);
            return std::make_pair(success, candidate);
        };

        {
            auto [success, candidate] = allocateWithTarget(*validTarget);
            UNIT_ASSERT_C(success, "expected exact target PDisk to be allocatable");
            UNIT_ASSERT_VALUES_EQUAL(candidate[source.VDisk.FailRealm][source.VDisk.FailDomain][source.VDisk.VDisk], *validTarget);
        }

        {
            auto [success, candidate] = allocateWithTarget(*invalidTarget);
            UNIT_ASSERT_C(!success, "target on an already occupied node must be rejected");
        }
    }

    Y_UNIT_TEST(SanitizeGroupTest3dc) {
        const ui32 numDataCenters = 3;
        const ui32 numRacks = 5;
        const ui32 numDisks = 3;
        TTestContext context(numDataCenters, 1, numRacks, 1, numDisks);
        TGroupMapper::TGroupDefinition groupDef;
        ui32 groupId;
        {
            TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));
            context.PopulateGroupMapper(mapper, 1);
            groupId = context.AllocateGroup(mapper, groupDef);
            Ctest << "group after allocation:" << Endl;
            context.DumpGroup(groupDef);
        }

        for (ui32 n = 0; n < 1000; ++n) {
            Ctest << Endl << "iteration# " << n << Endl;

            auto layout = context.ExportLayout();
            std::random_shuffle(layout.begin(), layout.end());
            context.ImportLayout(layout);

            Ctest << "group after layout shuffling:" << Endl;
            context.DumpGroup(groupDef);

            ui32 sanitationStep = 0;

            TGroupMapper::TGroupDefinition group = groupDef;
            TString path = "";
            TSet<TGroupMapper::TGroupDefinition> seen;
            TSet<TVDiskIdShort> vdiskItems;
            TSet<TPDiskId> pdiskItems;

            while (true) {
                const auto [it, inserted] = seen.insert(group);
                UNIT_ASSERT(inserted);
                UNIT_ASSERT(seen.size() <= 9);
                Ctest << "processing path# " << path << Endl;

                TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));

                context.SetGroup(groupId, group);
                context.PopulateGroupMapper(mapper, 2);

                std::pair<TVDiskIdShort, TPDiskId> movedDisk;
                auto [res, tempGroup] = context.SanitizeGroup(mapper, groupId, {}, false, false, false, &movedDisk);
                Ctest << "Sanititaion step# " << sanitationStep++ << ", sanitizer ";
                switch (res) {
                case TTestContext::ESanitizeResult::FAIL:
                    Ctest << "FAIL" << Endl;
                    UNIT_FAIL("Sanitizing failed");
                    break;
                case TTestContext::ESanitizeResult::ALREADY:
                    Ctest << "ALREADY" << Endl;
                    break;
                case TTestContext::ESanitizeResult::SUCCESS:
                    Ctest << "SUCCESS" << Endl;
                    break;
                }

                path = TStringBuilder() << path << "/" << (int)movedDisk.first.FailRealm << ":"
                    << (int)movedDisk.first.FailDomain << ":" << (int)movedDisk.first.VDisk << "@" << movedDisk.second;
                Ctest << "path# " << path << Endl;
                context.DumpGroup(tempGroup);
                if (res == TTestContext::ESanitizeResult::ALREADY) {
                    TString error;
                    UNIT_ASSERT_C(context.CheckGroupPlacement(group, TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc), error), error);
                    break;
                }

                Ctest << Endl;
                group = tempGroup;

                const auto [it1, inserted1] = vdiskItems.insert(movedDisk.first);
                UNIT_ASSERT_C(inserted1, "Duplicate group cell# " << movedDisk.first);

                const auto [it2, inserted2] = pdiskItems.insert(movedDisk.second);
                UNIT_ASSERT_C(inserted2, "Duplicate origin PDisk# " << movedDisk.second);
            }
        }
    }

    Y_UNIT_TEST(CheckNotToBreakFailModel) {
        TTestContext context(4, 1, 3, 1, 1);
        TGroupMapper::TGroupDefinition group;
        TGroupMapper mapper(TTestContext::CreateGroupGeometry(TBlobStorageGroupType::ErasureMirror3dc));
        context.PopulateGroupMapper(mapper, 1);
        ui32 groupId = context.AllocateGroup(mapper, group);
        Ctest << "group after allocation:" << Endl;
        context.DumpGroup(group);
        group = context.ReallocateGroup(mapper, groupId, {group[0][0][0]}, false, false, true);
        Ctest << "group after reallocation:" << Endl;
        context.DumpGroup(group);
        UNIT_ASSERT(group.empty());
    }
}
