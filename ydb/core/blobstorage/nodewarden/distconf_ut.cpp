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

    NKikimrConfig::TDomainsConfig::TStateStorage GenerateSimpleStateStorage(ui32 nodes, std::unordered_set<ui32> usedNodes = {}) {
        NKikimr::NStorage::TDistributedConfigKeeper keeper(nullptr, nullptr, true);
        NKikimrConfig::TDomainsConfig::TStateStorage ss;
        NKikimrBlobStorage::TStorageConfig config;
        for (ui32 i : xrange(nodes)) {
            auto *node = config.AddAllNodes();
            node->SetNodeId(i + 1);
        }
        keeper.GenerateStateStorageConfig(&ss, config, usedNodes);
        return ss;
    }

    NKikimrConfig::TDomainsConfig::TStateStorage GenerateDCStateStorage(ui32 dcCnt, ui32 racksCnt,  ui32 nodesInRack, std::unordered_map<ui32, ui32> nodesState = {}, std::unordered_set<ui32> usedNodes = {}, std::vector<ui32> oldConfig = {}, ui32 oldNToSelect = 9) {
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
        keeper.GenerateStateStorageConfig(&ss, config, usedNodes, oldSS);
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
}
}
}
