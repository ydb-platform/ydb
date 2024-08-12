#include <library/cpp/testing/unittest/registar.h>

#include "dq_opt_log.h"
#include "dq_opt_join.h"

#include "dq_opt_make_join_hypergraph.h"

using namespace NYql;
using namespace NNodes;
using namespace NYql::NDq;

std::shared_ptr<IBaseOptimizerNode> CreateChain(size_t size, TString onAttribute, TString tablePrefix="e") {
    std::shared_ptr<IBaseOptimizerNode> root = std::make_shared<TRelOptimizerNode>(tablePrefix + "1", nullptr);
    for (size_t i = 1; i < size; ++i) {
        auto eiStr = tablePrefix + ToString(i + 1);
        auto eiPrevStr = tablePrefix + ToString(i);

        auto ei = std::make_shared<TRelOptimizerNode>(eiStr, nullptr);

        std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>> joinConditions;
        joinConditions.insert({TJoinColumn(eiPrevStr, onAttribute), TJoinColumn(eiStr, onAttribute)});

        root = std::make_shared<TJoinOptimizerNode>(
            root, ei, joinConditions, EJoinKind::InnerJoin, EJoinAlgoType::Undefined
        );
    }

    return root;
}

Y_UNIT_TEST_SUITE(HypergraphBuild) {
    using TNodeSet = std::bitset<64>;

    void CheckClique(const TJoinHypergraph<TNodeSet>& graph) {
        size_t nodeCount = graph.GetNodes().size();

        for (size_t i = 0; i < nodeCount; ++i) {
            for (size_t j = 0; j < nodeCount; ++j) {
                if (i == j) {
                    continue;
                }

                TNodeSet lhs;
                lhs[i] = 1;
                TNodeSet rhs;
                rhs[j] = 1;

                UNIT_ASSERT(graph.FindEdgeBetween(lhs, rhs));
            }
        }
    }

    Y_UNIT_TEST(SimpleChain3NodesTransitiveClosure) {
        auto root = CreateChain(3, "Konstantin Vedernikov sidit na zp");
        auto graph = MakeJoinHypergraph<TNodeSet>(root);

        UNIT_ASSERT(graph.GetEdges().size() == 6);

        CheckClique(graph); 
    }

    Y_UNIT_TEST(SimpleChain4NodesTransitiveClosure) {
        auto root = CreateChain(4, "Ya hochu pitsu");
        auto graph = MakeJoinHypergraph<TNodeSet>(root);
        
        UNIT_ASSERT(graph.GetEdges().size() == 12);

        CheckClique(graph); 
    }

    Y_UNIT_TEST(SimpleChain5NodesTransitiveClosure) {
        auto root = CreateChain(5, "Dota2");
        auto graph = MakeJoinHypergraph<TNodeSet>(root);

        UNIT_ASSERT(graph.GetEdges().size() == 20);

        CheckClique(graph); 
    }

    Y_UNIT_TEST(ComplexTransitiveClosure) {
        auto lhs = CreateChain(3, "228", "a");
        auto rhs = CreateChain(2, "1337", "b");

        std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>> joinConditions;
        joinConditions.insert({TJoinColumn("a3", "1337"), TJoinColumn("b1", "1337")});

        // a1 --228-- a2 --228-- a3 --1337-- b1 --1337-- b2
        auto root = std::make_shared<TJoinOptimizerNode>(
            lhs, rhs, joinConditions, EJoinKind::InnerJoin, EJoinAlgoType::Undefined
        );

        joinConditions.clear();

        joinConditions.insert({TJoinColumn("c2", "123"), TJoinColumn("b2", "123")});
        rhs = CreateChain(2, "228", "c");

        // a1 --228-- a2 --228-- a3 --1337-- b1 --1337-- b2 --123-- c1 --228-- c2 
        // ^ we don't want to have transitive closure between c and a
        root = std::make_shared<TJoinOptimizerNode>(
            root, rhs, joinConditions, EJoinKind::InnerJoin, EJoinAlgoType::Undefined
        );

        auto graph = MakeJoinHypergraph<TNodeSet>(root);

        auto a1 = graph.GetNodesByRelNames({"a1"});
        auto a2 = graph.GetNodesByRelNames({"a2"});
        auto a3 = graph.GetNodesByRelNames({"a3"});
        auto b1 = graph.GetNodesByRelNames({"b1"});
        auto b2 = graph.GetNodesByRelNames({"b2"});
        auto c1 = graph.GetNodesByRelNames({"c1"});
        auto c2 = graph.GetNodesByRelNames({"c2"});

        UNIT_ASSERT(graph.FindEdgeBetween(a1, a2));
        UNIT_ASSERT(graph.FindEdgeBetween(a2, a3));
        UNIT_ASSERT(graph.FindEdgeBetween(a3, a1));

        UNIT_ASSERT(graph.FindEdgeBetween(a3, b1));
        UNIT_ASSERT(graph.FindEdgeBetween(b1, b2));
        UNIT_ASSERT(graph.FindEdgeBetween(b2, a3));

        UNIT_ASSERT(!graph.FindEdgeBetween(c1, a1));
        UNIT_ASSERT(!graph.FindEdgeBetween(c1, a2));
        UNIT_ASSERT(!graph.FindEdgeBetween(c1, a3));
        UNIT_ASSERT(!graph.FindEdgeBetween(c2, a1));
        UNIT_ASSERT(!graph.FindEdgeBetween(c2, a2));
        UNIT_ASSERT(!graph.FindEdgeBetween(c2, a3));
    }
}
