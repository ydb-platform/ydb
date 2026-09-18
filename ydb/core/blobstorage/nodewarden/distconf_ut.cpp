#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <ydb/library/pdisk_io/sector_map.h>
#include <ydb/core/util/random.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/blobstorage/nodewarden/distconf.h>

namespace NKikimr {
namespace NBlobStorageNodeWardenTest{

Y_UNIT_TEST_SUITE(TDistconfGenerateConfigTest) {
    NKikimrBlobStorage::TStorageConfig MakeBlock82Config(ui32 nodes) {
        NKikimrBlobStorage::TStorageConfig config;
        auto* hostConfig = config.MutableBlobStorageConfig()->AddDefineHostConfig();
        hostConfig->SetHostConfigId(1);
        auto* drive = hostConfig->AddDrive();
        drive->SetPath("/dev/disk1");
        drive->SetType(NKikimrBlobStorage::SSD);
        for (ui32 id = 1; id <= nodes; ++id) {
            auto* node = config.AddAllNodes();
            node->SetNodeId(id);
            node->SetHost(TStringBuilder() << "storage-" << id);
            node->SetPort(19001);
            node->MutableLocation()->SetDataCenter("dc-1");
            node->MutableLocation()->SetRack(ToString(id));
            node->MutableLocation()->SetUnit(ToString(id));
            auto* host = config.MutableBlobStorageConfig()->MutableDefineBox()->AddHost();
            host->SetHostConfigId(1);
            host->SetEnforcedNodeId(id);
        }
        return config;
    }

    Y_UNIT_TEST(Block82StaticBootstrapValidationAndQuorum) {
        for (const ui32 nodes : {11, 12}) {
            auto config = MakeBlock82Config(nodes);
            NStorage::TDistributedConfigKeeper keeper(nullptr, config, true);
            auto allocate = [&] {
                keeper.AllocateStaticGroup(&config, 0, 1, TBlobStorageGroupType::Erasure8Plus2Block,
                    {}, {}, NKikimrBlobStorage::SSD, {}, {}, 0, nullptr, false, false, false);
            };
            if (nodes == 11) {
                UNIT_ASSERT_EXCEPTION(allocate(), NStorage::TDistributedConfigKeeper::TExConfigError);
                continue;
            }
            allocate();
            UNIT_ASSERT(!NStorage::ValidateConfig(config));
            const auto& ss = config.GetBlobStorageConfig().GetServiceSet();
            UNIT_ASSERT_VALUES_EQUAL(ss.GroupsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ss.VDisksSize(), 12);
            UNIT_ASSERT_VALUES_EQUAL(ss.GetGroups(0).GetErasureSpecies(), 19);
            UNIT_ASSERT_VALUES_EQUAL(ss.GetGroups(0).RingsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ss.GetGroups(0).GetRings(0).FailDomainsSize(), 12);
            TNodeWardenConfig nwConfig(nullptr);
            for (ui32 missing = 0; missing <= 3; ++missing) {
                auto successful = [&](auto callback) {
                    for (ui32 i = 0; i != 12 - missing; ++i) {
                        const auto& location = ss.GetGroups(0).GetRings(0).GetFailDomains(i).GetVDiskLocations(0);
                        callback(NStorage::TNodeIdentifier(config.GetAllNodes(location.GetNodeID() - 1)),
                            TString("/dev/disk1"), std::make_optional(location.GetPDiskGuid()));
                    }
                };
                UNIT_ASSERT_VALUES_EQUAL(NStorage::HasConfigQuorum(config, successful, nwConfig), missing <= 2);
            }
            NKikimrBlobStorage::TStorageConfig reloaded;
            UNIT_ASSERT(reloaded.ParseFromString(config.SerializeAsString()));
            UNIT_ASSERT(!NStorage::ValidateConfig(reloaded));
            reloaded.SetGeneration(config.GetGeneration() + 1);
            UNIT_ASSERT(!NStorage::ValidateConfigUpdate(config, reloaded));
            reloaded.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(0)->SetErasureSpecies(4);
            UNIT_ASSERT(NStorage::ValidateConfigUpdate(config, reloaded));
            for (ui32 species : {ui32(TErasureType::ErasureSpeciesCount), Max<ui32>()}) {
                reloaded.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(0)->SetErasureSpecies(species);
                const auto error = NStorage::ValidateConfig(reloaded);
                UNIT_ASSERT(error);
                UNIT_ASSERT_STRING_CONTAINS(*error, "unknown ErasureSpecies");
            }
        }
    }

    Y_UNIT_TEST(Block82StaticHighDomainReassignWithDonor) {
        for (ui32 domain : {10, 11}) {
            auto config = MakeBlock82Config(13);
            NStorage::TDistributedConfigKeeper keeper(nullptr, config, true);
            keeper.AllocateStaticGroup(&config, 0, 1, TBlobStorageGroupType::Erasure8Plus2Block,
                {}, {}, NKikimrBlobStorage::SSD, {}, {NBsController::TPDiskId(13, 1)}, 0, nullptr, false, false, false);
            UNIT_ASSERT(!NStorage::ValidateConfig(config));
            auto proposed = config;
            proposed.SetGeneration(config.GetGeneration() + 1);
            NKikimrBlobStorage::TBaseConfig baseConfig;
            auto* spare = baseConfig.AddPDisk();
            spare->SetNodeId(13);
            spare->SetPDiskId(1);
            spare->SetPath("/dev/disk1");
            spare->SetType(NKikimrBlobStorage::SSD);
            spare->SetGuid(13);
            spare->SetDriveStatus(NKikimrBlobStorage::ACTIVE);
            spare->SetDecommitStatus(NKikimrBlobStorage::DECOMMIT_NONE);
            keeper.AllocateStaticGroup(&proposed, 0, 2, TBlobStorageGroupType::Erasure8Plus2Block,
                {}, {}, NKikimrBlobStorage::SSD, {{TVDiskIdShort(0, domain, 0), NBsController::TPDiskId(13, 1)}},
                {}, 0, &baseConfig, true, false, false);
            const auto error = NStorage::ValidateConfigUpdate(config, proposed);
            UNIT_ASSERT_C(!error, error.value_or(""));
            const auto& ss = proposed.GetBlobStorageConfig().GetServiceSet();
            UNIT_ASSERT_VALUES_EQUAL(ss.GetGroups(0).GetGroupGeneration(), 2);
            UNIT_ASSERT_VALUES_EQUAL(ss.GetGroups(0).GetRings(0).GetFailDomains(domain).GetVDiskLocations(0).GetNodeID(), 13);
            UNIT_ASSERT_VALUES_EQUAL(ss.VDisksSize(), 13);
            UNIT_ASSERT_VALUES_EQUAL(CountIf(ss.GetVDisks(), [](const auto& disk) { return disk.HasDonorMode(); }), 1);
            NKikimrBlobStorage::TStorageConfig reloaded;
            UNIT_ASSERT(reloaded.ParseFromString(proposed.SerializeAsString()));
            UNIT_ASSERT(!NStorage::ValidateConfig(reloaded));

            // Reassign another position before the first donor has been removed.
            // The untouched active position must keep its new location, and its
            // donor must keep the old generation referenced by the acceptor.
            const auto& oldLocation = config.GetBlobStorageConfig().GetServiceSet().GetGroups(0)
                .GetRings(0).GetFailDomains(domain).GetVDiskLocations(0);
            auto* oldPDisk = baseConfig.AddPDisk();
            oldPDisk->SetNodeId(oldLocation.GetNodeID());
            oldPDisk->SetPDiskId(oldLocation.GetPDiskID());
            oldPDisk->SetPath("/dev/disk1");
            oldPDisk->SetType(NKikimrBlobStorage::SSD);
            oldPDisk->SetGuid(oldLocation.GetPDiskGuid());
            oldPDisk->SetDriveStatus(NKikimrBlobStorage::ACTIVE);
            oldPDisk->SetDecommitStatus(NKikimrBlobStorage::DECOMMIT_NONE);
            const ui32 nextDomain = domain == 10 ? 11 : 10;
            for (bool retiredDonor : {false, true}) {
                auto previous = proposed;
                if (retiredDonor) {
                    for (auto& disk : *previous.MutableBlobStorageConfig()->MutableServiceSet()->MutableVDisks()) {
                        if (disk.HasDonorMode()) {
                            disk.ClearDonorMode();
                            disk.SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);
                        }
                        disk.ClearDonors();
                    }
                }
                auto next = previous;
                next.SetGeneration(previous.GetGeneration() + 1);
                keeper.AllocateStaticGroup(&next, 0, 3, TBlobStorageGroupType::Erasure8Plus2Block,
                    {}, {}, NKikimrBlobStorage::SSD,
                    {{TVDiskIdShort(0, nextDomain, 0), NBsController::TPDiskId(oldLocation.GetNodeID(), oldLocation.GetPDiskID())}},
                    {}, 0, &baseConfig, true, false, false);
                const auto nextError = NStorage::ValidateConfigUpdate(previous, next);
                UNIT_ASSERT_C(!nextError, nextError.value_or(""));
                const auto& nextGroup = next.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
                UNIT_ASSERT_VALUES_EQUAL(nextGroup.GetRings(0).GetFailDomains(domain).GetVDiskLocations(0).GetNodeID(), 13);
                UNIT_ASSERT_VALUES_EQUAL(nextGroup.GetRings(0).GetFailDomains(nextDomain).GetVDiskLocations(0).GetNodeID(), oldLocation.GetNodeID());
            }
        }
    }


    Y_UNIT_TEST(AllocateStaticGroupRespectsExpectedSlotSizeFromBaseConfig) {
        NKikimrBlobStorage::TStorageConfig config;
        auto *node = config.AddAllNodes();
        node->SetNodeId(1);
        node->MutableLocation()->SetDataCenter("dc-1");
        node->MutableLocation()->SetRack("rack-1");
        node->MutableLocation()->SetUnit("unit-1");

        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, config, true);

        NKikimrBlobStorage::TBaseConfig baseConfig;
        baseConfig.MutableSettings()->AddDefaultMaxSlots(16);

        auto *pdisk = baseConfig.AddPDisk();
        pdisk->SetNodeId(1);
        pdisk->SetPDiskId(1);
        pdisk->SetPath("/dev/disk1");
        pdisk->SetType(NKikimrBlobStorage::SSD);
        pdisk->SetKind(0);
        pdisk->SetGuid(1);
        pdisk->SetDriveStatus(NKikimrBlobStorage::ACTIVE);
        pdisk->SetDecommitStatus(NKikimrBlobStorage::DECOMMIT_NONE);
        pdisk->SetExpectedSlotCount(4);
        pdisk->SetExpectedSlotSize(100);
        pdisk->MutablePDiskConfig()->SetExpectedSlotSize(100);
        pdisk->MutablePDiskConfig()->SetMaxSlots(4);
        pdisk->MutablePDiskMetrics()->SetTotalSize(1000);
        pdisk->MutablePDiskMetrics()->SetAvailableSize(1000);

        NKikimrBlobStorage::TGroupGeometry geometry;
        geometry.SetNumFailRealms(1);
        geometry.SetNumFailDomainsPerFailRealm(1);
        geometry.SetNumVDisksPerFailDomain(1);

        try {
            keeper.AllocateStaticGroup(&config, 0 /*groupId*/, 1 /*groupGeneration*/,
                TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone),
                geometry, {} /*pdiskFilters*/, std::make_optional(NKikimrBlobStorage::SSD),
                {} /*replacedDisks*/, {} /*forbid*/, 200 /*requiredSpace*/,
                &baseConfig, false, false, false);
            UNIT_FAIL("Expected group allocation to fail");
        } catch (const NStorage::TDistributedConfigKeeper::TExConfigError& ex) {
            const TString error = ex.what();
            UNIT_ASSERT_C(error.Contains("group allocation failed"), error);
        }
    }

    Y_UNIT_TEST(AllocateStaticGroupOnFreshDrivesWithExpectedSlotSize) {
        // Bootstrap self-assembly: the static group must be allocatable on drives that come
        // straight from the config with expected_slot_size + max_slots, when neither the
        // materialized ExpectedSlotCount nor PDisk metrics exist yet. MaxSlots serves as the
        // slot count upper bound until NodeWarden computes the real value from the drive size.
        NKikimrBlobStorage::TStorageConfig config;
        auto *node = config.AddAllNodes();
        node->SetNodeId(1);
        node->MutableLocation()->SetDataCenter("dc-1");
        node->MutableLocation()->SetRack("rack-1");
        node->MutableLocation()->SetUnit("unit-1");

        auto *bsConfig = config.MutableBlobStorageConfig();
        auto *hostConfig = bsConfig->AddDefineHostConfig();
        hostConfig->SetHostConfigId(1);
        auto *drive = hostConfig->AddDrive();
        drive->SetPath("/dev/disk1");
        drive->SetType(NKikimrBlobStorage::SSD);
        drive->MutablePDiskConfig()->SetExpectedSlotSize(100ull << 30);
        drive->MutablePDiskConfig()->SetMaxSlots(4);

        auto *host = bsConfig->MutableDefineBox()->AddHost();
        host->SetHostConfigId(1);
        host->SetEnforcedNodeId(1);

        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, config, true);

        NKikimrBlobStorage::TGroupGeometry geometry;
        geometry.SetNumFailRealms(1);
        geometry.SetNumFailDomainsPerFailRealm(1);
        geometry.SetNumVDisksPerFailDomain(1);

        keeper.AllocateStaticGroup(&config, 0 /*groupId*/, 1 /*groupGeneration*/,
            TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone),
            geometry, {} /*pdiskFilters*/, std::make_optional(NKikimrBlobStorage::SSD),
            {} /*replacedDisks*/, {} /*forbid*/, 0 /*requiredSpace*/,
            nullptr /*baseConfig*/, false, false, false);

        const auto& serviceSet = config.GetBlobStorageConfig().GetServiceSet();
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.PDisksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.GetPDisks(0).GetPath(), "/dev/disk1");
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.GetPDisks(0).GetPDiskConfig().GetExpectedSlotSize(), 100ull << 30);
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.GetPDisks(0).GetPDiskConfig().GetMaxSlots(), 4);
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.VDisksSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.GroupsSize(), 1);
    }

    NKikimrConfig::TDomainsConfig::TStateStorage GenerateSimpleStateStorage(ui32 nodes) {
        NKikimrConfig::TDomainsConfig::TStateStorage ss;
        NKikimrBlobStorage::TStorageConfig config;
        for (ui32 i : xrange(nodes)) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(i + 1);
        }
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, config, true);
        keeper.GenerateStateStorageConfig(&ss, config);
        return ss;
    }

    NKikimrConfig::TDomainsConfig::TStateStorage GenerateDCStateStorage(ui32 dcCnt, ui32 racksCnt,  ui32 nodesInRack) {
        NKikimrBlobStorage::TStorageConfig config;
        ui32 nodeId = 1;
        for (ui32 dc : xrange(dcCnt)) {
            for (ui32 rack : xrange(racksCnt)) {
                for (auto _ : xrange(nodesInRack)) {
                    auto *node = config.AddAllNodes();
                    node->SetNodeId(nodeId++);
                    node->MutableLocation()->SetDataCenter("dc-" + std::to_string(dc));
                    node->MutableLocation()->SetRack(std::to_string(rack));
                }
            }
        }
        NKikimrConfig::TDomainsConfig::TStateStorage ss;
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, config, true);
        keeper.GenerateStateStorageConfig(&ss, config);
        return ss;
    }

    void CheckStateStorage(const NKikimrConfig::TDomainsConfig::TStateStorage& ss, ui32 nToSelect, const std::unordered_set<ui32>& nodes) {
        auto &rg = ss.GetRing();
        Cerr << "Actual: " << ss << " Expected: NToSelect: " << nToSelect << Endl;
        UNIT_ASSERT_EQUAL(rg.GetNToSelect(), nToSelect);
        UNIT_ASSERT_EQUAL(rg.NodeSize(), nodes.size());
        std::unordered_set<ui32> usedNodes;
        for (ui32 i : xrange(nodes.size())) {
            auto n = rg.GetNode(i);
            UNIT_ASSERT(nodes.contains(n));
            UNIT_ASSERT(usedNodes.insert(n).second);
        }
    }

    Y_UNIT_TEST(GenerateConfigSimpleCases) {
        CheckStateStorage(GenerateSimpleStateStorage(1), 1, {1});
        CheckStateStorage(GenerateSimpleStateStorage(2), 1, {1, 2});
        CheckStateStorage(GenerateSimpleStateStorage(3), 3, {1, 2, 3});
        CheckStateStorage(GenerateSimpleStateStorage(8), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateSimpleStateStorage(9), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateSimpleStateStorage(12), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 20), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateDCStateStorage(1, 10, 5), 5, {1, 6, 11, 16, 21, 26, 31, 36});
    }

    Y_UNIT_TEST(GenerateConfig3DCCases) {
        CheckStateStorage(GenerateDCStateStorage(3, 1, 1), 3, {1, 2, 3});
        CheckStateStorage(GenerateDCStateStorage(3, 1, 2), 3, {1, 3, 5});
        CheckStateStorage(GenerateDCStateStorage(3, 1, 3), 9, {1, 2, 3, 4, 5, 6, 7, 8, 9});
        CheckStateStorage(GenerateDCStateStorage(3, 1, 18), 9, {1, 2, 3, 19, 20, 21, 37, 38, 39});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});
    }
}
}
}
