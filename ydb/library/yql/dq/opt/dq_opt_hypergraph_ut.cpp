#include <library/cpp/testing/unittest/registar.h>

#include "dq_opt_log.h"
#include "dq_opt_join.h"

#include <util/string/split.h>

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
            root, ei, joinConditions, EJoinKind::InnerJoin, EJoinAlgoType::Undefined, false, false
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
            lhs, rhs, joinConditions, EJoinKind::InnerJoin, EJoinAlgoType::Undefined, false, false
        );

        joinConditions.clear();

        joinConditions.insert({TJoinColumn("c2", "123"), TJoinColumn("b2", "123")});
        rhs = CreateChain(2, "228", "c");

        // a1 --228-- a2 --228-- a3 --1337-- b1 --1337-- b2 --123-- c1 --228-- c2 
        // ^ we don't want to have transitive closure between c and a
        root = std::make_shared<TJoinOptimizerNode>(
            root, rhs, joinConditions, EJoinKind::InnerJoin, EJoinAlgoType::Undefined, false, false
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

    template <typename TJoinArg>
    std::shared_ptr<IBaseOptimizerNode> GetJoinArg(const TJoinArg& joinArg) {
        if constexpr (std::is_same_v<TJoinArg, std::shared_ptr<IBaseOptimizerNode>>) {
            return joinArg;
        } else if (std::is_convertible_v<TJoinArg, std::string>) {
            std::shared_ptr<IBaseOptimizerNode> root = std::make_shared<TRelOptimizerNode>(joinArg, nullptr);
            return root;
        } else {
            static_assert(std::is_convertible_v<TJoinArg, std::string> || 
                        std::is_same_v<TJoinArg, std::shared_ptr<IBaseOptimizerNode>>, 
                        "Args of join must be either Join or TString, for example: Join(Join('A', 'B'), 'C')");
        }

        Y_UNREACHABLE();
    }


    template<typename TLhsArg, typename TRhsArg>
    std::shared_ptr<IBaseOptimizerNode> Join(const TLhsArg& lhsArg, const TRhsArg& rhsArg, TString on="") {
        if constexpr (std::is_convertible_v<TLhsArg, std::string> && std::is_convertible_v<TRhsArg, std::string>) {
            on = Sprintf("%s,%s", lhsArg, rhsArg);
        }

        if (on.empty()) {
            throw std::invalid_argument("Bad argument.");
        }
        
        std::string lhsCond, rhsCond;
        Split(on, ",", lhsCond, rhsCond);

        auto col = ToString(rand());
        std::shared_ptr<IBaseOptimizerNode> root = std::make_shared<TJoinOptimizerNode>(
            TJoinOptimizerNode(
                GetJoinArg(lhsArg),
                GetJoinArg(rhsArg),
                {{TJoinColumn(lhsCond.c_str(), col), TJoinColumn(rhsCond.c_str(), col)}},
                EJoinKind::InnerJoin,
                EJoinAlgoType::Undefined,
                false,
                false
            )
        );
        return root;
    }

    Y_UNIT_TEST(AnyJoinConstraints1) {
        auto anyJoin = Join(Join(Join("A", "B"), "C", /*on=*/ "B,C"), "D", "C,D");
        std::static_pointer_cast<TJoinOptimizerNode>(anyJoin)->LeftAny = true;
        auto join = Join(anyJoin,"E", /*on=*/ "A,E");
        
        auto graph = MakeJoinHypergraph<TNodeSet>(join);
        Cout << graph.String() << Endl;
        UNIT_ASSERT(graph.GetEdges().size() !=  graph.GetSimpleEdges.size());
    }

    Y_UNIT_TEST(AnyJoinConstraints2) {
        auto anyJoin = Join(Join("A", "B"), Join("C", "D"), /*on=*/"B,C");
        std::static_pointer_cast<TJoinOptimizerNode>(anyJoin)->RightAny = true;
        auto join = Join(anyJoin,"E", /*on=*/ "C,E");
        
        auto graph = MakeJoinHypergraph<TNodeSet>(join);
        Cout << graph.String() << Endl;
        UNIT_ASSERT(graph.GetEdges().size() !=  graph.GetSimpleEdges.size());
    }
}
