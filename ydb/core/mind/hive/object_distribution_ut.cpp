#include <library/cpp/testing/unittest/registar.h>
#include "hive_impl.h"
#include "object_distribution.h"

#include <map>
#include <random>

using namespace NKikimr;
using namespace NHive;


Y_UNIT_TEST_SUITE(ObjectDistribuiton) {
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
        TObjectDistributions objectDistributions(nodes);
        for (TNodeId nodeId = 0; nodeId < NUM_NODES; ++nodeId) {
            TNodeInfo& node = nodes.emplace(std::piecewise_construct, std::tuple<TNodeId>(nodeId), std::tuple<TNodeId, THive&>(nodeId, hive)).first->second;
            node.ServicedDomains.push_back(TEST_DOMAIN);
            node.RegisterInDomains();
            node.LocationAcquired = true;
        }

        for (size_t i = 0; i < NUM_OPERATIONS; i++) {
            TLeaderTabletInfo tablet(0, hive);
            tablet.AssignDomains(TEST_DOMAIN, {});
            tablet.ObjectId.second = pickObject(engine);
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
}
