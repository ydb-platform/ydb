#include "../kqp_scatter_topology.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp;

Y_UNIT_TEST_SUITE(TKqpScatterTopologyTest) {
    using TNodes = std::vector<std::optional<ui64>>;

    static auto CheckTopology(const TNodes& producers, const TNodes& consumers) {
        const auto topology = MakeLocalScatterTopology(producers, consumers);
        UNIT_ASSERT_VALUES_EQUAL(topology.size(), producers.size());
        UNIT_ASSERT(topology == MakeLocalScatterTopology(producers, consumers));

        struct TNodeCounts {
            size_t Producers = 0;
            size_t Consumers = 0;
            size_t OutputCapacity = 0;
            size_t LocalChannels = 0;
            size_t LocalFirstChannels = 0;
        };
        THashMap<ui64, TNodeCounts> nodes;
        for (const auto& node : consumers) {
            if (node) {
                ++nodes[*node].Consumers;
            }
        }
        std::vector<size_t> incoming(consumers.size(), 0);
        for (size_t producer = 0; producer < producers.size(); ++producer) {
            const size_t degree = consumers.size() / producers.size()
                + (producer < consumers.size() % producers.size());
            UNIT_ASSERT_VALUES_EQUAL(topology[producer].size(), degree);
            if (producers[producer]) {
                auto& node = nodes[*producers[producer]];
                ++node.Producers;
                node.OutputCapacity += degree;
            }
            bool hasRemote = false;
            for (size_t consumer : topology[producer]) {
                UNIT_ASSERT(consumer < consumers.size());
                ++incoming[consumer];
                if (producers[producer] && producers[producer] == consumers[consumer]) {
                    UNIT_ASSERT_C(!hasRemote, "local channels must precede remote channels");
                    auto& node = nodes[*producers[producer]];
                    ++node.LocalChannels;
                    node.LocalFirstChannels += consumer == topology[producer].front();
                } else {
                    hasRemote = true;
                }
            }
        }
        for (size_t channels : incoming) {
            UNIT_ASSERT_VALUES_EQUAL(channels, 1);
        }
        // These bounds depend only on capacities, not on the matching algorithm.
        for (const auto& [nodeId, node] : nodes) {
            UNIT_ASSERT_VALUES_EQUAL_C(node.LocalChannels, std::min(node.OutputCapacity, node.Consumers), nodeId);
            UNIT_ASSERT_VALUES_EQUAL_C(node.LocalFirstChannels, std::min(node.Producers, node.Consumers), nodeId);
        }
        return topology;
    }

    Y_UNIT_TEST(MatchesNodesInsteadOfTaskIndices) {
        const auto topology = CheckTopology({20, 10}, {10, 10, 20, 20, 20});
        UNIT_ASSERT(topology[0] == (std::vector<size_t>{2, 3, 4}));
        UNIT_ASSERT(topology[1] == (std::vector<size_t>{0, 1}));
    }

    Y_UNIT_TEST(ReservesOtherProducersLocalConsumers) {
        const auto topology = CheckTopology({1, 2}, {2, 2, 3, 3});
        UNIT_ASSERT(topology[0] == (std::vector<size_t>{2, 3}));
        UNIT_ASSERT(topology[1] == (std::vector<size_t>{0, 1}));
    }

    Y_UNIT_TEST(SharesLocalFirstChannelsBeforeRemainingFanOut) {
        const auto topology = CheckTopology({1, 1, 1, 2}, {1, 1, 1, 2, 2, 2, 2, 2});
        UNIT_ASSERT(topology[0] == (std::vector<size_t>{0, 5}));
        UNIT_ASSERT(topology[1] == (std::vector<size_t>{1, 6}));
        UNIT_ASSERT(topology[2] == (std::vector<size_t>{2, 7}));
        UNIT_ASSERT(topology[3] == (std::vector<size_t>{3, 4}));
    }

    Y_UNIT_TEST(UnknownNodesDoNotClaimLocality) {
        const auto topology = CheckTopology({std::nullopt, 7}, {7, std::nullopt, 7, std::nullopt});
        UNIT_ASSERT(topology[0] == (std::vector<size_t>{1, 3}));
        UNIT_ASSERT(topology[1] == (std::vector<size_t>{0, 2}));

        const auto unplaced = CheckTopology(TNodes(3), TNodes(8));
        UNIT_ASSERT(unplaced[0] == (std::vector<size_t>{0, 1, 2}));
        UNIT_ASSERT(unplaced[1] == (std::vector<size_t>{3, 4, 5}));
        UNIT_ASSERT(unplaced[2] == (std::vector<size_t>{6, 7}));
    }

    Y_UNIT_TEST(TopologyInvariantsAcrossWidthsAndPlacements) {
        const TNodes nodeIds = {0, 50003, 17, std::nullopt};
        for (size_t n = 1; n <= 9; ++n) {
            for (size_t m = n; m <= 19; ++m) {
                for (size_t layout = 0; layout < 12; ++layout) {
                    TNodes producers(n), consumers(m);
                    for (size_t i = 0; i < n; ++i) {
                        producers[i] = nodeIds[(i * (layout / 4 + 1) + layout) % nodeIds.size()];
                    }
                    for (size_t i = 0; i < m; ++i) {
                        consumers[i] = nodeIds[(i * (layout % 3 + 1) + layout / 3) % nodeIds.size()];
                    }
                    CheckTopology(producers, consumers);
                }
            }
        }
    }

    Y_UNIT_TEST(RejectsUnsupportedWidths) {
        UNIT_ASSERT_EXCEPTION(MakeLocalScatterTopology(TNodes{}, TNodes(1)), yexception);
        UNIT_ASSERT_EXCEPTION(MakeLocalScatterTopology(TNodes(2), TNodes(1)), yexception);
    }
}
