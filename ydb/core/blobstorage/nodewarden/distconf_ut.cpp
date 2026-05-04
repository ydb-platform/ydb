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

    NKikimrConfig::TDomainsConfig::TStateStorage GenerateSimpleStateStorage(ui32 nodes, std::unordered_set<ui32> usedNodes = {}, ui32 overrideReplicasInRingCount = 0, ui32 overrideRingsCount = 0, ui32 replicasSpecificVolume = 200) {
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        NKikimrConfig::TDomainsConfig::TStateStorage ss;
        NKikimrBlobStorage::TStorageConfig config;
        for (ui32 i : xrange(nodes)) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(i + 1);
        }
        keeper.GenerateStateStorageConfig(&ss, config, usedNodes, {}, overrideReplicasInRingCount, overrideRingsCount, replicasSpecificVolume);
        return ss;
    }

    NKikimrConfig::TDomainsConfig::TStateStorage GenerateDCStateStorage(
        ui32 dcCnt
        , ui32 racksCnt
        , ui32 nodesInRack
        , std::unordered_map<ui32, ui32> nodesState = {}
        , std::unordered_set<ui32> usedNodes = {}
        , std::vector<ui32> oldConfig = {}
        , ui32 oldNToSelect = 9
        , ui32 overrideReplicasInRingCount = 0
        , ui32 overrideRingsCount = 0
        , ui32 replicasSpecificVolume = 1000
    ) {
        NKikimrBlobStorage::TStorageConfig config;
        ui32 nodeId = 1;
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        for (ui32 dc : xrange(dcCnt)) {
            for (ui32 rack : xrange(racksCnt)) {
                for (auto _ : xrange(nodesInRack)) {
                    auto *node = config.AddAllNodes();
                    keeper.SelfHealNodesState[nodeId] = 0;
                    node->SetNodeId(nodeId++);
                    node->MutableLocation()->SetDataCenter("dc-" + std::to_string(dc));
                    node->MutableLocation()->SetRack(std::to_string(rack));
                }
            }
        }
        NKikimrConfig::TDomainsConfig::TStateStorage oldSS;
        if (!oldConfig.empty()) {
            auto* rg = oldSS.AddRingGroups();
            rg->SetNToSelect(oldNToSelect);
            for (ui32 node : oldConfig) {
                auto* ssRing = rg->AddRing();
                ssRing->AddNode(node);
            }
        }
        NKikimrConfig::TDomainsConfig::TStateStorage ss;
        for (auto [nodeId, state] : nodesState) {
            keeper.SelfHealNodesState[nodeId] = state;
        }
        keeper.GenerateStateStorageConfig(&ss, config, usedNodes, oldSS, overrideReplicasInRingCount, overrideRingsCount, replicasSpecificVolume);
        return ss;
    }

    void CheckStateStorage(const NKikimrConfig::TDomainsConfig::TStateStorage& ss, ui32 nToSelect, const std::unordered_set<ui32>& nodes) {
        auto &rg = ss.GetRingGroups(0);
        Cerr << "Actual: " << ss << " Expected: NToSelect: " << nToSelect << Endl;
        UNIT_ASSERT_EQUAL(rg.GetNToSelect(), nToSelect);
        UNIT_ASSERT_EQUAL(rg.RingSize(), nodes.size());
        std::unordered_set<ui32> usedNodes;
        for (ui32 i : xrange(nodes.size())) {
            UNIT_ASSERT_EQUAL(rg.GetRing(i).NodeSize(), 1);
            auto n = rg.GetRing(i).GetNode(0);
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

    void CheckStateStorage2(const NKikimrConfig::TDomainsConfig::TStateStorage& ss, std::string expected) {
        TString actual = TStringBuilder() << ss;
        if (actual != expected) {
            Cerr << "Err Actual: " << ss << Endl;
            Cerr << "Err Expected: " << expected << Endl;
        }
        UNIT_ASSERT_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(GenerateConfig1DCBigCases) {
        CheckStateStorage2(GenerateDCStateStorage(1, 1, 1000), "{ RingGroups { NToSelect: 5 "
            "Ring { Node: 1 Node: 2 } Ring { Node: 3 Node: 4 } Ring { Node: 5 Node: 6 } "
            "Ring { Node: 7 Node: 8 } Ring { Node: 9 Node: 10 } Ring { Node: 11 Node: 12 } "
            "Ring { Node: 13 Node: 14 } Ring { Node: 15 Node: 16 } } }");
        CheckStateStorage2(GenerateDCStateStorage(1, 2, 1000), "{ RingGroups { NToSelect: 5 "
            "Ring { Node: 1 Node: 2 Node: 3 } Ring { Node: 4 Node: 5 Node: 6 } Ring { Node: 7 Node: 8 Node: 9 } "
            "Ring { Node: 10 Node: 11 Node: 12 } Ring { Node: 13 Node: 14 Node: 15 } Ring { Node: 16 Node: 17 Node: 18 } "
            "Ring { Node: 19 Node: 20 Node: 21 } Ring { Node: 22 Node: 23 Node: 24 } } }");
        CheckStateStorage2(GenerateDCStateStorage(1, 10, 100), "{ RingGroups { NToSelect: 5 "
            "Ring { Node: 1 Node: 2 } Ring { Node: 101 Node: 102 } Ring { Node: 201 Node: 202 } "
            "Ring { Node: 301 Node: 302 } Ring { Node: 401 Node: 402 } Ring { Node: 501 Node: 502 } "
            "Ring { Node: 601 Node: 602 } Ring { Node: 701 Node: 702 } } }");
    }

    Y_UNIT_TEST(GenerateConfig3DCBigCases) {
        CheckStateStorage2(GenerateDCStateStorage(3, 1, 300), "{ RingGroups { NToSelect: 9 "
            "Ring { Node: 1 } Ring { Node: 2 } Ring { Node: 3 } "
            "Ring { Node: 301 } Ring { Node: 302 } Ring { Node: 303 } "
            "Ring { Node: 601 } Ring { Node: 602 } Ring { Node: 603 } } }");
        CheckStateStorage2(GenerateDCStateStorage(3, 10, 100), "{ RingGroups { NToSelect: 9 "
            "Ring { Node: 1 Node: 2 Node: 3 Node: 4 } Ring { Node: 101 Node: 102 Node: 103 Node: 104 } "
            "Ring { Node: 201 Node: 202 Node: 203 Node: 204 } Ring { Node: 1001 Node: 1002 Node: 1003 Node: 1004 } "
            "Ring { Node: 1101 Node: 1102 Node: 1103 Node: 1104 } Ring { Node: 1201 Node: 1202 Node: 1203 Node: 1204 } "
            "Ring { Node: 2001 Node: 2002 Node: 2003 Node: 2004 } Ring { Node: 2101 Node: 2102 Node: 2103 Node: 2104 } "
            "Ring { Node: 2201 Node: 2202 Node: 2203 Node: 2204 } } }");
    }

    Y_UNIT_TEST(IgnoreNodes) {
        CheckStateStorage(GenerateDCStateStorage(1, 1, 20, { {3, 2} }), 5, {1, 2, 4, 5, 6, 7, 8, 9});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 20, { {3, 2}, {7, 4}, {10, 3} }), 5, {1, 2, 4, 5, 6, 8, 9, 11});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 10, { {3, 2}, {7, 4}, {10, 3} }), 5, {1, 2, 3, 4, 5, 6, 8, 9});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 10, { {3, 3}, {7, 4}, {10, 2} }), 5, {1, 2, 4, 5, 6, 8, 9, 10});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 2} }), 9, {1, 4, 7, 10, 14, 16, 19, 22, 25});
    }

    Y_UNIT_TEST(BadRack) {
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 5}, {14, 3}, {15, 4} }), 9, {1, 4, 7, 10, 14, 16, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 2}, {14, 3}, {15, 4} }), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 3}, {14, 4}, {15, 2} }), 9, {1, 4, 7, 10, 15, 16, 19, 22, 25});
    }

    Y_UNIT_TEST(ExtraDCHelp) {
        CheckStateStorage(GenerateDCStateStorage(4, 3, 1, { {3, 2} }), 9, {1, 2, 4, 5, 6, 7, 8, 9, 10});
        CheckStateStorage(GenerateDCStateStorage(4, 3, 1, { {6, 2} }), 9, {1, 2, 3, 4, 5, 7, 8, 9, 10});
        CheckStateStorage(GenerateDCStateStorage(4, 3, 1, { {9, 2}, {8, 4} }), 9, {1, 2, 3, 4, 5, 6, 7, 10, 11});
    }


    Y_UNIT_TEST(UsedNodes) {
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 2} }, { 1, 2, 3, 4, 5, 6 }), 9, {1, 4, 7, 10, 14, 16, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(1, 1, 20, { {3, 2} }, { 1, 2, 3, 4, 9 }), 5, {5, 6, 7, 8, 10, 11, 12, 13});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {13, 2} }, { 4, 16 }), 9, {1, 5, 7, 10, 14, 17, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(4, 3, 1, { {3, 2} }, { 1 }), 9, {2, 4, 5, 6, 7, 8, 9, 10, 11});
    }


    Y_UNIT_TEST(UseOldNodesInDisconnectedDC) {
        // DC is connected, not enough bad nodes in DC - normak config generation
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {10, 2}, {11, 4}, {13, 3}, {14, 4} }, {}, {1, 5, 8, 10, 14, 17, 19, 22, 25}), 9, {1, 4, 7, 12, 15, 16, 19, 22, 25});
        // Disconnected DC, but current config is invalid, build new config without usage of node statuses in disconnected DC
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {10, 2}, {11, 4}, {13, 3}, {14, 4}, {15, 2} }, {}, {1, 5, 8, 10, 14, 17, 19, 22, 25}, 5), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});
        // DC disconnected - use previous config for this DC
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, { {10, 2}, {11, 4}, {13, 3}, {14, 4}, {15, 2} }, {}, {1, 5, 8, 10, 14, 17, 19, 22, 25}, 9), 9, {1, 4, 7, 10, 14, 17, 19, 22, 25});
    }

    Y_UNIT_TEST(GenerateConfigReplicasOverrides) {
        CheckStateStorage(GenerateSimpleStateStorage(100, {}, 0, 0), 5, {1, 2, 3, 4, 5, 6, 7, 8});
        CheckStateStorage(GenerateSimpleStateStorage(100, {}, 1, 1), 1, {1});
        CheckStateStorage2(GenerateSimpleStateStorage(100, {}, 3, 3),
            "{ RingGroups { NToSelect: 3 Ring { Node: 1 Node: 2 Node: 3 }"
            " Ring { Node: 4 Node: 5 Node: 6 } Ring { Node: 7 Node: 8 Node: 9 } } }");
        CheckStateStorage(GenerateSimpleStateStorage(100, {}, 20, 10), 5, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        CheckStateStorage2(GenerateSimpleStateStorage(16, {}, 4, 2), "{ RingGroups { NToSelect: 1 Ring { Node: 1 Node: 2 Node: 3 Node: 4 } Ring { Node: 5 Node: 6 Node: 7 Node: 8 } } }");
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 0, 0), 9, {1, 4, 7, 10, 13, 16, 19, 22, 25});
        CheckStateStorage(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 1, 3), 3, {1, 10, 19});
        CheckStateStorage2(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 2, 3), "{ RingGroups { NToSelect: 3 Ring { Node: 1 Node: 2 } Ring { Node: 10 Node: 11 } Ring { Node: 19 Node: 20 } } }");
        CheckStateStorage2(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 3, 3), "{ RingGroups { NToSelect: 3 Ring { Node: 1 Node: 2 Node: 3 } Ring { Node: 10 Node: 11 Node: 12 } Ring { Node: 19 Node: 20 Node: 21 } } }");
        CheckStateStorage2(GenerateDCStateStorage(3, 3, 3, {}, {}, {}, 9, 2, 1),"{ RingGroups { NToSelect: 9 "
            "Ring { Node: 1 Node: 2 } Ring { Node: 4 Node: 5 } Ring { Node: 7 Node: 8 } "
            "Ring { Node: 10 Node: 11 } Ring { Node: 13 Node: 14 } Ring { Node: 16 Node: 17 } "
            "Ring { Node: 19 Node: 20 } Ring { Node: 22 Node: 23 } Ring { Node: 25 Node: 26 } } }");
    }

    Y_UNIT_TEST(GenerateConfigReplicasSpecificVolume) {
        CheckStateStorage2(GenerateSimpleStateStorage(100, {}, 0, 3, 35),
            "{ RingGroups { NToSelect: 3 Ring { Node: 1 Node: 2 Node: 3 }"
            " Ring { Node: 4 Node: 5 Node: 6 } Ring { Node: 7 Node: 8 Node: 9 } } }");
        CheckStateStorage2(GenerateSimpleStateStorage(16, {}, 0, 2, 5), "{ RingGroups { NToSelect: 1 Ring { Node: 1 Node: 2 Node: 3 Node: 4 } Ring { Node: 5 Node: 6 Node: 7 Node: 8 } } }");
    }

Y_UNIT_TEST_SUITE(TDeriveStorageConfigCleanupTest) {

    struct TTestSetup {
        NKikimrBlobStorage::TStorageConfig StorageConfig;
        NKikimrConfig::TAppConfig AppConfig;

        void AddNode(ui32 nodeId, const TString& host = "host", ui16 port = 19000) {
            {
                auto* node = StorageConfig.AddAllNodes();
                node->SetNodeId(nodeId);
                node->SetHost(host);
                node->SetPort(port);
            }
            {
                auto* node = AppConfig.MutableNameserviceConfig()->AddNode();
                node->SetNodeId(nodeId);
                node->SetInterconnectHost(host);
                node->SetPort(port);
            }
        }

        void RemoveNodeFromAppConfig(ui32 nodeId) {
            auto* ns = AppConfig.MutableNameserviceConfig();
            auto* nodes = ns->MutableNode();
            for (int i = nodes->size() - 1; i >= 0; --i) {
                if (nodes->Get(i).GetNodeId() == nodeId) {
                    nodes->DeleteSubrange(i, 1);
                }
            }
        }

        void AddPDisk(ui32 nodeId, ui32 pdiskId, const TString& path = "/dev/disk") {
            auto* pdisk = StorageConfig.MutableBlobStorageConfig()->MutableServiceSet()->AddPDisks();
            pdisk->SetNodeID(nodeId);
            pdisk->SetPDiskID(pdiskId);
            pdisk->SetPath(path);
            pdisk->SetPDiskGuid(nodeId * 1000 + pdiskId);
        }

        void AddVDisk(ui32 nodeId, ui32 pdiskId, ui32 vslotId, ui32 groupId, ui32 generation,
                      ui32 domain, NKikimrBlobStorage::EEntityStatus status = NKikimrBlobStorage::EEntityStatus::INITIAL) {
            auto* vdisk = StorageConfig.MutableBlobStorageConfig()->MutableServiceSet()->AddVDisks();
            auto* vid = vdisk->MutableVDiskID();
            vid->SetGroupID(groupId);
            vid->SetGroupGeneration(generation);
            vid->SetRing(0);
            vid->SetDomain(domain);
            vid->SetVDisk(0);
            auto* loc = vdisk->MutableVDiskLocation();
            loc->SetNodeID(nodeId);
            loc->SetPDiskID(pdiskId);
            loc->SetVDiskSlotID(vslotId);
            loc->SetPDiskGuid(nodeId * 1000 + pdiskId);
            if (status != NKikimrBlobStorage::EEntityStatus::INITIAL) {
                vdisk->SetEntityStatus(status);
            }
        }

        void AddGroup(ui32 groupId, ui32 generation, const std::vector<std::tuple<ui32, ui32, ui32>>& vdiskLocs) {
            auto* group = StorageConfig.MutableBlobStorageConfig()->MutableServiceSet()->AddGroups();
            group->SetGroupID(groupId);
            group->SetGroupGeneration(generation);
            group->SetErasureSpecies(4);
            auto* ring = group->AddRings();
            for (const auto& [nodeId, pdiskId, vslotId] : vdiskLocs) {
                auto* fd = ring->AddFailDomains();
                auto* loc = fd->AddVDiskLocations();
                loc->SetNodeID(nodeId);
                loc->SetPDiskID(pdiskId);
                loc->SetVDiskSlotID(vslotId);
                loc->SetPDiskGuid(nodeId * 1000 + pdiskId);
            }
        }

        void EnableSelfManagement() {
            StorageConfig.MutableSelfManagementConfig()->SetEnabled(true);
            AppConfig.MutableSelfManagementConfig()->SetEnabled(true);
        }

        void SyncAppConfigBlobStorage() {
            AppConfig.MutableBlobStorageConfig()->MutableServiceSet();
        }

        void MirrorServiceSetIntoAppConfig() {
            AppConfig.MutableBlobStorageConfig()->MutableServiceSet()->CopyFrom(
                StorageConfig.GetBlobStorageConfig().GetServiceSet());
        }

        bool Derive(TString* error = nullptr) {
            TString err;
            if (!error) error = &err;
            return NKikimr::NStorage::DeriveStorageConfig(AppConfig, &StorageConfig, error);
        }

        void AddDefineBoxHost(ui32 nodeId, ui64 hostConfigId = 1) {
            auto* host = AppConfig.MutableBlobStorageConfig()->MutableDefineBox()->AddHost();
            host->SetEnforcedNodeId(nodeId);
            host->SetHostConfigId(hostConfigId);
            auto* host2 = StorageConfig.MutableBlobStorageConfig()->MutableDefineBox()->AddHost();
            host2->SetEnforcedNodeId(nodeId);
            host2->SetHostConfigId(hostConfigId);
        }

        int CountPDisks() const { return StorageConfig.GetBlobStorageConfig().GetServiceSet().PDisksSize(); }
        int CountVDisks() const { return StorageConfig.GetBlobStorageConfig().GetServiceSet().VDisksSize(); }
        int CountDefineBoxHosts() const {
            return StorageConfig.GetBlobStorageConfig().HasDefineBox()
                ? StorageConfig.GetBlobStorageConfig().GetDefineBox().HostSize() : 0;
        }

        bool HasPDiskOnNode(ui32 nodeId) const {
            for (const auto& p : StorageConfig.GetBlobStorageConfig().GetServiceSet().GetPDisks()) {
                if (p.GetNodeID() == nodeId) return true;
            }
            return false;
        }

        bool HasVDiskOnNode(ui32 nodeId) const {
            for (const auto& v : StorageConfig.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
                if (v.HasVDiskLocation() && v.GetVDiskLocation().GetNodeID() == nodeId) return true;
            }
            return false;
        }

        bool HasDefineBoxHostForNode(ui32 nodeId) const {
            if (!StorageConfig.GetBlobStorageConfig().HasDefineBox()) return false;
            for (const auto& host : StorageConfig.GetBlobStorageConfig().GetDefineBox().GetHost()) {
                if (host.GetEnforcedNodeId() == nodeId) return true;
            }
            return false;
        }
    };

    Y_UNIT_TEST(DestroyVDiskAndPDiskCleanedOnNodeRemoval) {
        TTestSetup s;
        s.EnableSelfManagement();
        for (ui32 i = 1; i <= 8; ++i) {
            s.AddNode(i, "host-" + std::to_string(i));
            s.AddPDisk(i, 1, "/dev/disk" + std::to_string(i));
            s.AddDefineBoxHost(i);
        }

        s.AddVDisk(1, 1, 1, 0, 1, 0, NKikimrBlobStorage::EEntityStatus::DESTROY);
        for (ui32 i = 2; i <= 8; ++i) {
            s.AddVDisk(i, 1, 1, 0, 2, i - 1);
        }
        std::vector<std::tuple<ui32, ui32, ui32>> groupLocs;
        for (ui32 i = 2; i <= 8; ++i) {
            groupLocs.emplace_back(i, 1, 1);
        }
        s.AddGroup(0, 2, groupLocs);
        s.SyncAppConfigBlobStorage();

        s.RemoveNodeFromAppConfig(1);

        TString error;
        UNIT_ASSERT_C(s.Derive(&error), "DeriveStorageConfig failed: " << error);

        UNIT_ASSERT(!s.HasVDiskOnNode(1));
        UNIT_ASSERT(!s.HasPDiskOnNode(1));
        UNIT_ASSERT(!s.HasDefineBoxHostForNode(1));
        UNIT_ASSERT(s.HasPDiskOnNode(2));
        UNIT_ASSERT(s.HasVDiskOnNode(2));
        UNIT_ASSERT(s.HasDefineBoxHostForNode(2));
        UNIT_ASSERT_EQUAL(s.CountPDisks(), 7);
        UNIT_ASSERT_EQUAL(s.CountVDisks(), 7);
        UNIT_ASSERT_EQUAL(s.CountDefineBoxHosts(), 7);
    }

    Y_UNIT_TEST(LiveVDiskOnRemovedNodeBlocksCleanup) {
        TTestSetup s;
        s.EnableSelfManagement();
        for (ui32 i = 1; i <= 4; ++i) {
            s.AddNode(i, "host-" + std::to_string(i));
            s.AddPDisk(i, 1, "/dev/disk" + std::to_string(i));
            s.AddVDisk(i, 1, 1, 0, 1, i - 1);
        }
        std::vector<std::tuple<ui32, ui32, ui32>> groupLocs;
        for (ui32 i = 1; i <= 4; ++i) {
            groupLocs.emplace_back(i, 1, 1);
        }
        s.AddGroup(0, 1, groupLocs);
        s.SyncAppConfigBlobStorage();
        s.RemoveNodeFromAppConfig(1);

        TString error;
        bool ok = s.Derive(&error);
        UNIT_ASSERT_C(ok, "DeriveStorageConfig failed: " << error);
        UNIT_ASSERT(s.HasVDiskOnNode(1));
        UNIT_ASSERT(s.HasPDiskOnNode(1));
    }

    Y_UNIT_TEST(NoCleanupWithoutSelfManagement) {
        TTestSetup s;
        for (ui32 i = 1; i <= 4; ++i) {
            s.AddNode(i, "host-" + std::to_string(i));
            s.AddPDisk(i, 1, "/dev/disk" + std::to_string(i));
            s.AddDefineBoxHost(i);
        }

        s.AddVDisk(1, 1, 1, 0, 1, 0, NKikimrBlobStorage::EEntityStatus::DESTROY);
        for (ui32 i = 2; i <= 4; ++i) {
            s.AddVDisk(i, 1, 1, 0, 2, i - 1);
        }
        std::vector<std::tuple<ui32, ui32, ui32>> groupLocs;
        for (ui32 i = 2; i <= 4; ++i) {
            groupLocs.emplace_back(i, 1, 1);
        }
        s.AddGroup(0, 2, groupLocs);
        s.MirrorServiceSetIntoAppConfig();
        s.RemoveNodeFromAppConfig(1);

        TString error;
        UNIT_ASSERT_C(s.Derive(&error), "DeriveStorageConfig failed: " << error);

        UNIT_ASSERT(s.HasVDiskOnNode(1));
        UNIT_ASSERT(s.HasPDiskOnNode(1));
        UNIT_ASSERT(s.HasDefineBoxHostForNode(1));
    }
}

}
}
}
