#include <library/cpp/testing/unittest/registar.h>
#include "hive_impl.h"
#include "object_distribution.h"

#include <util/datetime/cputimer.h>
#include <util/stream/null.h>

#include <map>
#include <random>

using namespace NKikimr;
using namespace NHive;

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

Y_UNIT_TEST_SUITE(ObjectDistribution) {
    Y_UNIT_TEST(TestImbalanceCalcualtion) {
        static constexpr size_t NUM_NODES = 8;
        static constexpr size_t NUM_OBJECTS = 250;
        static constexpr size_t NUM_OPERATIONS = 10'000;
        static constexpr TSubDomainKey TEST_DOMAIN = {1, 1};

        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        THive hive(hiveStorage.Get(), TActorId());
        std::map<std::pair<TNodeId, TFullObjectId>, ui64> trueDistribution;

        std::mt19937 engine(42);
        std::uniform_int_distribution<TObjectId> pickObject(0, NUM_OBJECTS - 1);
        std::uniform_int_distribution<TNodeId> pickNode(0, NUM_NODES - 1);
        std::bernoulli_distribution subtract(0.2);

        std::unordered_map<TNodeId, TNodeInfo> nodes;
        NKikimrLocal::TTabletAvailability dummyTabletAvailability;
        dummyTabletAvailability.SetType(TTabletTypes::Dummy);
        TObjectDistributions objectDistributions(nodes);
        for (TNodeId nodeId = 0; nodeId < NUM_NODES; ++nodeId) {
            TNodeInfo& node = nodes.emplace(std::piecewise_construct, std::tuple<TNodeId>(nodeId), std::tuple<TNodeId, THive&>(nodeId, hive)).first->second;
            node.ServicedDomains.push_back(TEST_DOMAIN);
            node.RegisterInDomains();
            node.LocationAcquired = true;
            node.TabletAvailability.emplace(std::piecewise_construct,
                                            std::tuple<TTabletTypes::EType>(TTabletTypes::Dummy),
                                            std::tuple<NKikimrLocal::TTabletAvailability>(dummyTabletAvailability));
        }

        for (size_t i = 0; i < NUM_OPERATIONS; i++) {
            TLeaderTabletInfo tablet(0, hive);
            tablet.AssignDomains(TEST_DOMAIN, {});
            tablet.ObjectId.second = pickObject(engine);
            tablet.SetType(TTabletTypes::Dummy);
            TFullObjectId object = tablet.ObjectId;
            TNodeId node = pickNode(engine);
            ui64& curCount = trueDistribution[{node, object}];
            i64 diff = 1;
            if (curCount > 0 && subtract(engine)) {
                diff = -1;
            }
            curCount += diff;
            objectDistributions.UpdateCountForTablet(tablet, nodes.at(node), diff);
        }

        ui64 imbalancedObjects = 0;
        for (const auto& [object, it] : objectDistributions.Distributions) {
            ui64 maxCnt = 0;
            ui64 minCnt = NUM_OPERATIONS;
            ui64 total = 0;
            ui64 nonZeroCount = 0;
            for (TNodeId node = 0; node < NUM_NODES; ++node) {
                ui64 cnt = trueDistribution[{node, object}];
               /* if (cnt == 0) {
                    continue;
                }*/
                maxCnt = std::max(maxCnt, cnt);
                minCnt = std::min(minCnt, cnt);
                total += cnt;
                ++nonZeroCount;
            }
            if (maxCnt == 0) {
                continue;
            }
            double trueImbalance = (std::max<double>(maxCnt - minCnt, 1) - 1) / maxCnt;
            // std::cerr << "imbalance for " << object << " should be " << trueImbalance << std::endl;
            double imbalance = it->GetImbalance();
            UNIT_ASSERT_DOUBLES_EQUAL(trueImbalance, imbalance, 1e-5);

            imbalancedObjects += (trueImbalance > 1e-7);

            double mean = (double)total / nonZeroCount;
            double varianceNumerator = 0;
            for (TNodeId node = 0; node < NUM_NODES; ++node) {
                ui64 cnt = trueDistribution[{node, object}];
               /* if (cnt == 0) {
                    continue;
                }*/
                varianceNumerator += (mean - cnt) * (mean - cnt);
            }
            double trueVariance = varianceNumerator / nonZeroCount;
            double variance = it->GetVariance();
            UNIT_ASSERT_DOUBLES_EQUAL(trueVariance, variance, 1e-5);
        }
        UNIT_ASSERT_VALUES_EQUAL(imbalancedObjects, objectDistributions.GetImbalancedObjectsCount());
    }

    Y_UNIT_TEST(TestAllowedDomainsAndDown) {
        // Object0 lives in DomainA, Object1 lives in DomainB, Object2 lives in both
        // First half of nodes serves DomainA, second half serves DomainB
        // + sometimes nodes are down

        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        THive hive(hiveStorage.Get(), TActorId());
        std::map<std::pair<TNodeId, TFullObjectId>, ui64> trueDistribution;

        static constexpr size_t NUM_NODES = 10;
        static constexpr size_t NUM_OPERATIONS = 10'000;
        static constexpr TSubDomainKey DOMAIN_A = {1, 1};
        static constexpr TSubDomainKey DOMAIN_B = {2, 2};
        std::vector<std::optional<TLeaderTabletInfo>> tablets(3);
        std::vector<std::uniform_int_distribution<TNodeId>> pickNodeForObject;
        for (size_t i = 0; i < 3; i++) {
            tablets[i].emplace(i + 1, hive);
            tablets[i]->ObjectId = {1, i + 1};
            tablets[i]->SetType(TTabletTypes::Dummy);
        }
        tablets[0]->AssignDomains(DOMAIN_A, {});
        pickNodeForObject.emplace_back(0, NUM_NODES / 2 - 1);
        tablets[1]->AssignDomains(DOMAIN_B, {});
        pickNodeForObject.emplace_back(NUM_NODES / 2, NUM_NODES - 1);
        tablets[2]->AssignDomains({}, {DOMAIN_A, DOMAIN_B});
        pickNodeForObject.emplace_back(0, NUM_NODES - 1);

        std::unordered_map<TNodeId, TNodeInfo> nodes;
        TObjectDistributions objectDistributions(nodes);
        NKikimrLocal::TTabletAvailability dummyTabletAvailability;
        dummyTabletAvailability.SetType(TTabletTypes::Dummy);
        for (TNodeId nodeId = 0; nodeId < NUM_NODES; ++nodeId) {
            TNodeInfo& node = nodes.emplace(std::piecewise_construct, std::tuple<TNodeId>(nodeId), std::tuple<TNodeId, THive&>(nodeId, hive)).first->second;
            node.ServicedDomains.push_back(nodeId * 2 < NUM_NODES ? DOMAIN_A : DOMAIN_B);
            node.RegisterInDomains();
            node.LocationAcquired = true;
            node.TabletAvailability.emplace(std::piecewise_construct,
                                            std::tuple<TTabletTypes::EType>(TTabletTypes::Dummy),
                                            std::tuple<NKikimrLocal::TTabletAvailability>(dummyTabletAvailability));
        }

        std::mt19937 engine(42);
        std::uniform_int_distribution<size_t> pickObject(0, 2);
        std::uniform_int_distribution<TNodeId> pickNode(0, NUM_NODES - 1);
        std::bernoulli_distribution subtract(0.2);
        std::bernoulli_distribution toggleNode(0.3);

        std::list<TTabletInfo> dummyTablets;

        for (size_t iter = 0; iter < NUM_OPERATIONS; ++iter) {
            if (toggleNode(engine)) {
                auto nodeId = pickNode(engine);
                auto& node = nodes.at(nodeId);
                if (node.Down) {
                    Ctest << "AddNode " << nodeId << Endl;
                    node.Down = false;
                    objectDistributions.AddNode(node);
                } else {
                    Ctest << "RemoveNode " << nodeId << Endl;
                    node.Down = true;
                    objectDistributions.RemoveNode(node);
                }
            }
            auto i = pickObject(engine);
            const auto& tablet = *tablets[i];
            TFullObjectId object = tablet.ObjectId;
            TNodeId nodeId = pickNodeForObject[i](engine);
            auto& node = nodes.at(nodeId);
            ui64& curCount = trueDistribution[{nodeId, object}];
            i64 diff;
            if (curCount > 0 && subtract(engine)) {
                diff = -1;
                auto& tabletsOfObject = node.TabletsOfObject[object];
                tabletsOfObject.erase(tabletsOfObject.begin());
            } else {
                diff = 1;
                dummyTablets.emplace_back(NKikimr::NHive::TTabletInfo::ETabletRole::Leader, hive);
                node.TabletsOfObject[object].insert(&dummyTablets.back());
            }
            curCount += diff;
            Ctest << ToString(object) << ": " << diff << " on " << nodeId << Endl;
            objectDistributions.UpdateCountForTablet(tablet, node, diff);
        }

        auto getTrueImbalance = [&](TFullObjectId object, TNodeId nodeBegin, TNodeId nodeEnd) {
            i64 minCnt = NUM_OPERATIONS;
            i64 maxCnt = 0;
            for (TNodeId nodeId = nodeBegin; nodeId < nodeEnd; ++nodeId) {
                if (nodes.at(nodeId).Down) {
                    continue;
                }
                i64 cnt = trueDistribution[{nodeId, object}];
                minCnt = std::min(minCnt, cnt);
                maxCnt = std::max(maxCnt, cnt);
            }
            return (std::max<double>(maxCnt - minCnt, 1) - 1) / maxCnt;
        };

        Ctest << "Final state: " << Endl;
        for (size_t i = 0; i < 3; ++i) {
            for (TNodeId nodeId = 0; nodeId < NUM_NODES; ++nodeId) {
                Ctest << trueDistribution[{nodeId, tablets[i]->ObjectId}] << "\t";
            }
            Ctest << "\n";
        }
        for (TNodeId nodeId = 0; nodeId < NUM_NODES; ++nodeId) {
            Ctest << (nodes.at(nodeId).Down ? "-" : "+") << "\t";
        }
        Ctest << "\n";

        auto& distributions = objectDistributions.Distributions;
        UNIT_ASSERT_DOUBLES_EQUAL(distributions.at(tablets[0]->ObjectId)->GetImbalance(), getTrueImbalance(tablets[0]->ObjectId, 0, NUM_NODES / 2), 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(distributions.at(tablets[1]->ObjectId)->GetImbalance(), getTrueImbalance(tablets[1]->ObjectId, NUM_NODES / 2, NUM_NODES), 1e-5);
        UNIT_ASSERT_DOUBLES_EQUAL(distributions.at(tablets[2]->ObjectId)->GetImbalance(), getTrueImbalance(tablets[2]->ObjectId, 0, NUM_NODES), 1e-5);

    }

    Y_UNIT_TEST(TestAddSameNode) {
        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        THive hive(hiveStorage.Get(), TActorId());

        std::unordered_map<TNodeId, TNodeInfo> nodes;
        TObjectDistributions objectDistributions(nodes);

        TNodeInfo node1(1, hive);
        TNodeInfo node2(2, hive);
        TTabletInfo tablet(NKikimr::NHive::TTabletInfo::ETabletRole::Leader, hive);
        node1.TabletsOfObject[{1, 1}].insert(&tablet);
        objectDistributions.AddNode(node1);
        objectDistributions.AddNode(node2);

        auto imbalance = objectDistributions.GetMaxImbalance();
        for (int i = 0; i < 10; ++i) {
            objectDistributions.AddNode(node1);
        }
        UNIT_ASSERT_VALUES_EQUAL(imbalance, objectDistributions.GetMaxImbalance());

    }

    Y_UNIT_TEST(TestManyIrrelevantNodes) {
        static constexpr size_t NUM_NODES = 10'000;
        static constexpr size_t NUM_OBJECTS = 10'000;
        static constexpr TSubDomainKey DOMAIN_A = {1, 1};
        static constexpr TSubDomainKey DOMAIN_B = {2, 2};

        TIntrusivePtr<TTabletStorageInfo> hiveStorage = new TTabletStorageInfo;
        hiveStorage->TabletType = TTabletTypes::Hive;
        THive hive(hiveStorage.Get(), TActorId());

        std::unordered_map<TNodeId, TNodeInfo> nodes;
        TObjectDistributions objectDistributions(nodes);
        NKikimrLocal::TTabletAvailability dummyTabletAvailability;
        dummyTabletAvailability.SetType(TTabletTypes::Dummy);
        for (TNodeId nodeId = 0; nodeId < NUM_NODES; ++nodeId) {
            TNodeInfo& node = nodes.emplace(std::piecewise_construct, std::tuple<TNodeId>(nodeId), std::tuple<TNodeId, THive&>(nodeId, hive)).first->second;
            node.ServicedDomains.push_back(nodeId == 0 ? DOMAIN_A : DOMAIN_B);
            node.RegisterInDomains();
            node.LocationAcquired = true;
            node.TabletAvailability.emplace(std::piecewise_construct,
                                            std::tuple<TTabletTypes::EType>(TTabletTypes::Dummy),
                                            std::tuple<NKikimrLocal::TTabletAvailability>(dummyTabletAvailability));
        }

        for (size_t i = 0; i < NUM_OBJECTS; i++) {
            TLeaderTabletInfo tablet(0, hive);
            tablet.AssignDomains(DOMAIN_A, {});
            tablet.ObjectId = {1, i + 1};
            tablet.SetType(TTabletTypes::Dummy);
            objectDistributions.UpdateCountForTablet(tablet, nodes.at(0), +1);
        }

        TProfileTimer timer;
        for (const auto& [nodeId, node] : nodes) {
            objectDistributions.AddNode(node);
        }

        double passed = timer.Get().SecondsFloat();
        Cerr << "Took " << passed << " seconds" << Endl;
#ifndef SANITIZER_TYPE
#ifdef NDEBUG
        UNIT_ASSERT_GE(NUM_NODES / passed, 1000);
#endif
#endif
    }
}
