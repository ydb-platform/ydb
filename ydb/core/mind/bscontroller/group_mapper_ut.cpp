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
        ui32 NumSlots;

        TPDiskRecord(ui32 dataCenterId, ui32 roomId, ui32 rackId, ui32 bodyId)
            : DataCenterId(dataCenterId)
            , RoomId(roomId)
            , RackId(rackId)
            , BodyId(bodyId)
            , NumSlots(0)
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

    TVector<ui32> GetSlots() const {
        TVector<ui32> slots;
        for (const auto& pair : PDisks) {
            slots.push_back(pair.second.NumSlots);
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

    ui32 AllocateGroup(TGroupMapper& mapper, TGroupMapper::TGroupDefinition& group, bool allowFailure = false) {
        ui32 groupId = NextGroupId++;
        TString error;
        bool success = mapper.AllocateGroup(groupId, group, {}, {}, 0, false, error);
        if (!success && allowFailure) {
            return 0;
        }
        UNIT_ASSERT_C(success, error);
        TGroupRecord& record = Groups[groupId];
        record.Group = group;
        for (const auto& realm : group) {
            for (const auto& domain : realm) {
                for (const auto& pdisk : domain) {
                    record.PDisks.push_back(pdisk);
                    ++PDisks.at(pdisk).NumSlots;
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
                    auto& pdisk = group.Group[i][j][k];
                    --PDisks.at(pdisk).NumSlots;
                    if (unusableDisks.count(pdisk)) {
                        replacedDisks.emplace(TVDiskIdShort(i, j, k), std::exchange(pdisk, {}));
                    }
                }
            }
        }

        Ctest << "groupId# " << groupId << " reallocating group# " << FormatGroup(group.Group) << Endl;

        TString error;
        bool success = mapper.AllocateGroup(groupId, group.Group, replacedDisks, std::move(forbid), 0,
            requireOperational, error);
        if (!success) {
            Ctest << "error# " << error << Endl;
            if (allowError) {
                // revert group to its original state
                for (const auto& [vdiskId, pdiskId] : replacedDisks) {
                    group.Group[vdiskId.FailRealm][vdiskId.FailDomain][vdiskId.VDisk] = pdiskId;
                }
                for (auto& realm : group.Group) {
                    for (auto& domain : realm) {
                        for (auto& pdisk : domain) {
                            ++PDisks.at(pdisk).NumSlots;
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
                for (const auto& pdisk : domain) {
                    group.PDisks.push_back(pdisk);
                    ++PDisks.at(pdisk).NumSlots;
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
                    auto& pdisk = group.Group[i][j][k];
                    --PDisks.at(pdisk).NumSlots;
                }
            }
        }

        TGroupMapper::TMisplacedVDisks result = mapper.FindMisplacedVDisks(group.Group);
        if (result) {
            Ctest << "error# " << result.ErrorReason << Endl;
            if (allowError) {
                for (auto& realm : group.Group) {
                    for (auto& domain : realm) {
                        for (auto& pdisk : domain) {
                            ++PDisks.at(pdisk).NumSlots;
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
                    std::move(forbid), 0, requireOperational, error);
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
                for (const auto& pdisk : domain) {
                    group.PDisks.push_back(pdisk);
                    ++PDisks.at(pdisk).NumSlots;
                }
            }
        }

        return {status, group.Group};
    }

    void SetGroup(ui32 groupId, const TGroupMapper::TGroupDefinition& group) {
        auto& g = Groups[groupId];
        for (const TPDiskId& pdiskId : g.PDisks) {
            --PDisks.at(pdiskId).NumSlots;
        }
        g.Group = group;
        g.PDisks.clear();
        for (const auto& realm : g.Group) {
            for (const auto& domain : realm) {
                for (const auto& pdisk : domain) {
                    g.PDisks.push_back(pdisk);
                    ++PDisks.at(pdisk).NumSlots;
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

    void PopulateGroupMapper(TGroupMapper& mapper, ui32 maxSlots = 16, TSet<TPDiskId> unusableDisks = {},
            TSet<TPDiskId> nonoperationalDisks = {}, std::optional<ui32> decommittedDataCenter = std::nullopt) {
        std::map<TPDiskId, std::vector<ui32>> groupDisks;
        for (const auto& [groupId, group] : Groups) {
            for (TPDiskId pdiskId : group.PDisks) {
                groupDisks[pdiskId].push_back(groupId);
            }
        }
        for (const auto& pair : PDisks) {
            auto& g = groupDisks[pair.first];
            mapper.RegisterPDisk({
                .PDiskId = pair.first,
                .Location = pair.second.GetLocation(),
                .Usable = !unusableDisks.count(pair.first),
                .NumSlots = pair.second.NumSlots,
                .MaxSlots = maxSlots,
                .Groups{g.begin(), g.end()},
                .SpaceAvailable = 0,
                .Operational = !nonoperationalDisks.contains(pair.first),
                .Decommitted = decommittedDataCenter == pair.second.DataCenterId,
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
                    pdisks[pdiskId] = NLayoutChecker::TPDiskLayoutPosition(domainMapper,
                            PDisks.at(pdiskId).GetLocation(),
                            pdiskId,
                            geom
                    );
                }
            }
        }

        return CheckLayoutByGroupDefinition(group, pdisks, geom, true, error);
    }
};

Y_UNIT_TEST_SUITE(TGroupMapperTest) {

    Y_UNIT_TEST(MapperSequentialCalls) {
        TTestContext globalContext(3, 4, 20, 5, 4);
        TTestContext localContext(3, 4, 20, 5, 4);

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
        TVector<ui32> slots = context.GetSlots();
        for (ui32 numSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(8 * numVDisksPerFailDomain, numSlots);
        }
        context.CheckIfGroupsAreMappedCompact();
    }

    Y_UNIT_TEST(Block42_1disk) {
        TestBlock42(1);
    }

    Y_UNIT_TEST(Block42_2disk) {
        TestBlock42(2);
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
        TVector<ui32> slots = context.GetSlots();
        for (ui32 numSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(9, numSlots);
        }
        context.CheckIfGroupsAreMappedCompact();
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
        TVector<ui32> slots = context.GetSlots();
        for (ui32 numSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(8, numSlots);
        }
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
        TVector<ui32> slots = context.GetSlots();
        for (ui32 numSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(8, numSlots);
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

            TVector<ui32> slots = context.GetSlots();
            UNIT_ASSERT(slots);
            ui32 min = slots[0];
            ui32 max = slots[0];
            for (size_t i = 1; i < slots.size(); ++i) {
                min = Min(min, slots[i]);
                max = Max(max, slots[i]);
            }
            UNIT_ASSERT_C(max - min <= 1, Sprintf("min# %" PRIu32 " max# %" PRIu32, min, max));
        }
        TVector<ui32> slots = context.GetSlots();
        for (ui32 numSlots : slots) {
            UNIT_ASSERT_VALUES_EQUAL(9, numSlots);
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

            TVector<ui32> slots = context.GetSlots();
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
        TVector<ui32> slots = context.GetSlots();
        for (ui32 numSlots : slots) {
            if (numSlots) {
                UNIT_ASSERT_VALUES_EQUAL(9, numSlots);
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

            const ui32 maxSlots = 16;
            TTestContext context(std::move(disks));
            context.IteratePDisks([&](auto&, auto& v) {
                v.NumSlots = rand(0, maxSlots);
            });
            for (;;) {
                Ctest << "spawning new mapper" << Endl;
                TGroupMapper mapper(TTestContext::CreateGroupGeometry(numDataCenters >= 3
                    ? TBlobStorageGroupType::ErasureMirror3dc
                    : TBlobStorageGroupType::Erasure4Plus2Block));
                context.PopulateGroupMapper(mapper, maxSlots);
                TGroupMapper::TGroupDefinition group;
                while (context.AllocateGroup(mapper, group, true)) {
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
