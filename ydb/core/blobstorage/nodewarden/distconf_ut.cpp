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
