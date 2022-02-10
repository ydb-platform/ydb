#include <library/cpp/testing/unittest/registar.h>

#include "group_geometry_info.h"
#include "group_mapper.h"
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

    ui32 AllocateGroup(TGroupMapper& mapper, TGroupMapper::TGroupDefinition& group, bool allowFailure = false) {
        ui32 groupId = NextGroupId++;
        TString error;
        bool success = mapper.AllocateGroup(groupId, group, nullptr, 0, {}, 0, false, error);
        if (!success && allowFailure) {
            return 0;
        }
        if (!success) {
            Ctest << "error# " << error << Endl;
        }
        UNIT_ASSERT(success);
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
            bool makeThemForbidden = false, bool requireOperational = false, bool requireError = false) {
        TGroupRecord& group = Groups.at(groupId);

        TGroupMapper::TForbiddenPDisks forbid(unusableDisks.begin(), unusableDisks.end());
        if (!makeThemForbidden) {
            forbid.clear();
        }

        // remove unusable disks from the set
        std::vector<TPDiskId> replaced;
        for (auto& realm : group.Group) {
            for (auto& domain : realm) {
                for (auto& pdisk : domain) {
                    --PDisks.at(pdisk).NumSlots;
                    if (unusableDisks.count(pdisk)) {
                        replaced.push_back(std::exchange(pdisk, {}));
                    }
                }
            }
        }

        Ctest << "groupId# " << groupId << " reallocating group# " << FormatGroup(group.Group) << Endl;

        TString error;
        bool success = mapper.AllocateGroup(groupId, group.Group, replaced.data(), replaced.size(), std::move(forbid),
            0, requireOperational, error);
        if (!success) {
            if (requireError) {
                return {};
            }
            Ctest << "error# " << error << Endl;
        } else {
            UNIT_ASSERT(!requireError);
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

    void CheckGroupErasure(const TGroupMapper::TGroupDefinition& group) {
        TSet<ui32> dataCenters;
        for (const auto& realm : group) {
            TMaybe<ui32> dataCenter;
            TSet<std::tuple<ui32, ui32>> domains;
            for (const auto& domain : realm) {
                TMaybe<std::tuple<ui32, ui32>> currentDom;
                for (const auto& pdisk : domain) {
                    const TPDiskRecord& record = PDisks.at(pdisk);
                    if (dataCenter) {
                        UNIT_ASSERT_VALUES_EQUAL(*dataCenter, record.DataCenterId);
                    } else {
                        dataCenter = record.DataCenterId;
                        const bool inserted = dataCenters.insert(*dataCenter).second;
                        UNIT_ASSERT(inserted);
                    }
                    std::tuple<ui32, ui32> dom = {record.RoomId, record.RackId};
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
            TSet<TPDiskId> nonoperationalDisks = {}) {
        std::map<TPDiskId, std::vector<ui32>> groupDisks;
        for (const auto& [groupId, group] : Groups) {
            for (TPDiskId pdiskId : group.PDisks) {
                groupDisks[pdiskId].push_back(groupId);
            }
        }
        for (const auto& pair : PDisks) {
            auto& g = groupDisks[pair.first];
            mapper.RegisterPDisk(pair.first, pair.second.GetLocation(), !unusableDisks.count(pair.first),
                pair.second.NumSlots, maxSlots, g.data(), g.size(), 0, nonoperationalDisks.count(pair.first));
        }
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
            for (ui32 groupId : groupIds) {
                auto group = context.ReallocateGroup(mapper, groupId, unusableDisks, true, true);
                group = context.ReallocateGroup(mapper, groupId, unusableDisks);
                Ctest << "groupId# " << groupId << " new content# " << context.FormatGroup(group) << Endl;
                context.CheckGroupErasure(group);
            }
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

}
