#include <library/cpp/testing/unittest/registar.h>

#include "dq_opt_log.h"
#include "dq_opt_join.h"

#include <util/string/split.h>

#include "dq_opt_make_join_hypergraph.h"
#include "dq_opt_log.h"

#include <memory>

using namespace NYql;
using namespace NNodes;
using namespace NYql::NDq;

std::shared_ptr<IBaseOptimizerNode> CreateChain(size_t size, TString onAttribute, TString tablePrefix="e") {
    std::shared_ptr<IBaseOptimizerNode> root = std::make_shared<TRelOptimizerNode>(tablePrefix + "1", std::make_shared<TOptimizerStatistics>());
    root->Stats->Labels = std::make_shared<TVector<TString>>(TVector<TString>{tablePrefix + "1"});
    for (size_t i = 1; i < size; ++i) {
        auto eiStr = tablePrefix + ToString(i + 1);
        auto eiPrevStr = tablePrefix + ToString(i);

        auto ei = std::make_shared<TRelOptimizerNode>(eiStr, std::make_shared<TOptimizerStatistics>());
        ei->Stats->Labels = std::make_shared<TVector<TString>>(TVector<TString>{eiStr});

        TVector<NDq::TJoinColumn> leftKeys = {TJoinColumn(eiPrevStr, onAttribute)};
        TVector<NDq::TJoinColumn> rightKeys = {TJoinColumn(eiStr, onAttribute)};

        root = std::make_shared<TJoinOptimizerNode>(
            root, ei, leftKeys, rightKeys, EJoinKind::InnerJoin, EJoinAlgoType::Undefined, false, false
        );
    }

    return root;
}

template <typename TProviderContext = TBaseProviderContext>
std::shared_ptr<IBaseOptimizerNode> Enumerate(const std::shared_ptr<IBaseOptimizerNode>& root, const TOptimizerHints& hints = {}) {
    auto ctx = TProviderContext();
    auto optimizer = 
        std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(ctx, std::numeric_limits<ui32>::max()));
    
    Y_ENSURE(root->Kind == EOptimizerNodeKind::JoinNodeType);
    auto res = optimizer->JoinSearch(std::static_pointer_cast<TJoinOptimizerNode>(root), hints);
    Cout << "Optimized Tree:" << Endl;
    std::stringstream ss; res->Print(ss);
    Cout << ss.str() << Endl;
    return res;
}

TVector<TJoinColumn> CollectConditions(const std::shared_ptr<IBaseOptimizerNode>& node) {
    if (node->Kind != EOptimizerNodeKind::JoinNodeType) {
        return {};
    }

    auto joinNode = std::static_pointer_cast<TJoinOptimizerNode>(node);
    auto lhsConds = CollectConditions(joinNode->LeftArg);
    auto rhsConds = CollectConditions(joinNode->RightArg);
    lhsConds.insert(lhsConds.end(), rhsConds.begin(), rhsConds.end());
    for (const auto& [lhsCond, rhsCond]: Zip(joinNode->LeftJoinKeys, joinNode->RightJoinKeys)) {
        lhsConds.push_back(lhsCond);
        lhsConds.push_back(rhsCond);
    }
    
    return lhsConds;
}

bool HaveSameConditions(const std::shared_ptr<IBaseOptimizerNode>& actual, std::shared_ptr<IBaseOptimizerNode> expected) {
    auto actualConds = CollectConditions(actual);
    auto expectedConds = CollectConditions(expected);

    return 
        std::unordered_set<TJoinColumn, TJoinColumn::THashFunction>(actualConds.begin(), actualConds.end()) == 
        std::unordered_set<TJoinColumn, TJoinColumn::THashFunction>(expectedConds.begin(), expectedConds.end());
}

bool HaveSameConditionCount(const std::shared_ptr<IBaseOptimizerNode>& actual, std::shared_ptr<IBaseOptimizerNode> expected) {
    auto actualConds = CollectConditions(actual);
    auto expectedConds = CollectConditions(expected);
    
    return actualConds.size() == expectedConds.size() &&
        std::unordered_set<TJoinColumn, TJoinColumn::THashFunction>(actualConds.begin(), actualConds.end()).size() == 
        std::unordered_set<TJoinColumn, TJoinColumn::THashFunction>(expectedConds.begin(), expectedConds.end()).size();
}

Y_UNIT_TEST_SUITE(HypergraphBuild) {
    using TNodeSet64 = std::bitset<64>;
    using TNodeSet128 = std::bitset<256>;

    void CheckClique(const TJoinHypergraph<TNodeSet64>& graph) {
        size_t nodeCount = graph.GetNodes().size();

        for (size_t i = 0; i < nodeCount; ++i) {
            for (size_t j = 0; j < nodeCount; ++j) {
                if (i == j) continue; 

                TNodeSet64 lhs; lhs[i] = 1;
                TNodeSet64 rhs; rhs[j] = 1;
                UNIT_ASSERT(graph.FindEdgeBetween(lhs, rhs));
            }
        }
    }

    Y_UNIT_TEST(SimpleChain3NodesTransitiveClosure) {
        auto root = CreateChain(3, "Konstantin Vedernikov sidit na zp");
        auto graph = MakeJoinHypergraph<TNodeSet64>(root);

        UNIT_ASSERT(graph.GetEdges().size() == 6);

        CheckClique(graph); 
        Enumerate(root);
    }

    Y_UNIT_TEST(SimpleChain4NodesTransitiveClosure) {
        auto root = CreateChain(4, "Ya hochu pitsu");
        auto graph = MakeJoinHypergraph<TNodeSet64>(root);
        
        UNIT_ASSERT(graph.GetEdges().size() == 12);

        CheckClique(graph); 
        Enumerate(root);
    }

    Y_UNIT_TEST(SimpleChain5NodesTransitiveClosure) {
        auto root = CreateChain(5, "Dota2");
        auto graph = MakeJoinHypergraph<TNodeSet64>(root);

        UNIT_ASSERT(graph.GetEdges().size() == 20);

        CheckClique(graph); 
        Enumerate(root);
    }

    Y_UNIT_TEST(ComplexTransitiveClosure) {
        auto lhs = CreateChain(3, "228", "a");
        auto rhs = CreateChain(2, "1337", "b");

        TVector<NDq::TJoinColumn> leftKeys = {TJoinColumn("a3", "1337")};
        TVector<NDq::TJoinColumn> rightKeys = {TJoinColumn("b1", "1337")};

        // a1 --228-- a2 --228-- a3 --1337-- b1 --1337-- b2
        auto root = std::make_shared<TJoinOptimizerNode>(
            lhs, rhs, leftKeys, rightKeys, EJoinKind::InnerJoin, EJoinAlgoType::Undefined, false, false
        );

        leftKeys.clear();
        rightKeys.clear();

        leftKeys.push_back(TJoinColumn("c2", "123"));
        rightKeys.push_back(TJoinColumn("b2", "123"));

        rhs = CreateChain(2, "228", "c");

        // a1 --228-- a2 --228-- a3 --1337-- b1 --1337-- b2 --123-- c1 --228-- c2 
        // ^ we don't want to have transitive closure between c and a
        root = std::make_shared<TJoinOptimizerNode>(
            root, rhs, leftKeys, rightKeys, EJoinKind::InnerJoin, EJoinAlgoType::Undefined, false, false
        );

        auto graph = MakeJoinHypergraph<TNodeSet64>(root);
        Cout << graph.String() << Endl;

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

        Enumerate(root);
    }

    template <typename TJoinArg>
    std::shared_ptr<IBaseOptimizerNode> GetJoinArg(const TJoinArg& joinArg) {
        if constexpr (std::is_same_v<TJoinArg, std::shared_ptr<IBaseOptimizerNode>>) {
            return joinArg;
        } else if constexpr (std::is_convertible_v<TJoinArg, std::string>) {
            std::shared_ptr<IBaseOptimizerNode> root = std::make_shared<TRelOptimizerNode>(joinArg, std::make_shared<TOptimizerStatistics>());
            root->Stats->Nrows = rand() % 100 + 1;
            return root;
        } else {
            static_assert(
                std::is_convertible_v<TJoinArg, std::string> || std::is_same_v<TJoinArg, std::shared_ptr<IBaseOptimizerNode>>, 
                "Args of join must be either Join or TString, for example: Join(Join('A', 'B'), 'C')"
            );
        }

        Y_UNREACHABLE();
    }


    /* Example of usage: Join("A", "B", "A.id=B.id,A.kek=B.kek,A=B") (only equijoin supported)*/
    template<typename TLhsArg, typename TRhsArg>
    std::shared_ptr<IBaseOptimizerNode> Join(const TLhsArg& lhsArg, const TRhsArg& rhsArg, TString on="", EJoinKind kind = EJoinKind::InnerJoin) {
        if constexpr (std::is_convertible_v<TLhsArg, std::string> && std::is_convertible_v<TRhsArg, std::string>) {
            if (on.Empty()) {
                on = Sprintf("%s=%s", lhsArg, rhsArg);
            }
        }

        if (on.empty()) {
            throw std::invalid_argument("Bad argument.");
        }

        TVector<TString> conds;
        Split(on, ",", conds);
        TVector<TJoinColumn> leftJoinCond;
        TVector<TJoinColumn> rightJoinCond;
        for (const TString& cond: conds) {
            std::string lhsCond, rhsCond; // "A.id B.id"
            Split(cond, "=", lhsCond, rhsCond);

            if (lhsCond.contains(".") && rhsCond.contains(".")) {
                std::string lhsTable, lhsAttr;
                Split(lhsCond, ".", lhsTable, lhsAttr);
                std::string rhsTable, rhsAttr;
                Split(rhsCond, ".", rhsTable, rhsAttr);
                leftJoinCond.push_back(TJoinColumn(std::move(lhsTable), std::move(lhsAttr)));
                rightJoinCond.push_back(TJoinColumn(std::move(rhsTable), std::move(rhsAttr)));
            } else {
                TString attr = ToString(rand());
                leftJoinCond.push_back(TJoinColumn(std::move(lhsCond), attr));
                rightJoinCond.push_back(TJoinColumn(std::move(rhsCond), attr));
 
            }
        }

        std::shared_ptr<IBaseOptimizerNode> root = std::make_shared<TJoinOptimizerNode>(
            TJoinOptimizerNode(
                GetJoinArg(lhsArg),
                GetJoinArg(rhsArg),
                leftJoinCond,
                rightJoinCond,
                kind,
                EJoinAlgoType::Undefined,
                false,
                false
            )
        );
        return root;
    }

    template<typename TLhsArg, typename TRhsArg>
    std::shared_ptr<IBaseOptimizerNode> FullJoin(const TLhsArg& lhsArg, const TRhsArg& rhsArg, TString on="") {
        return Join(lhsArg, rhsArg, on, EJoinKind::OuterJoin);
    }

    template<typename TLhsArg, typename TRhsArg>
    std::shared_ptr<IBaseOptimizerNode> LeftJoin(const TLhsArg& lhsArg, const TRhsArg& rhsArg, TString on="") {
        return Join(lhsArg, rhsArg, on, EJoinKind::LeftJoin);
    }

    template<typename TLhsArg, typename TRhsArg>
    std::shared_ptr<IBaseOptimizerNode> CrossJoin(const TLhsArg& lhsArg, const TRhsArg& rhsArg, TString on="") {
        return Join(lhsArg, rhsArg, on, EJoinKind::Cross);
    }

    Y_UNIT_TEST(AnyJoinWithTransitiveClosure) {
        auto root = Join("A", Join("B", Join("C", "D", "C.id=D.id"), "B.id=C.id"), "A.id=B.id");
        std::static_pointer_cast<TJoinOptimizerNode>(root)->LeftAny = true;

        auto graph = MakeJoinHypergraph<TNodeSet64>(root);
        Cout << graph.String() << Endl;
        
        auto A = graph.GetNodesByRelNames({"A"});
        auto B = graph.GetNodesByRelNames({"B"});
        auto C = graph.GetNodesByRelNames({"C"});
        auto D = graph.GetNodesByRelNames({"D"});

        UNIT_ASSERT(graph.FindEdgeBetween(B, D));
        UNIT_ASSERT(!graph.FindEdgeBetween(A, D));
        UNIT_ASSERT(!graph.FindEdgeBetween(A, C));

        Enumerate(root);
    }

    Y_UNIT_TEST(AnyJoinConstraints1) {
        auto anyJoin = Join(Join("A", "B"), "C", /*on=*/ "B=C");
        std::static_pointer_cast<TJoinOptimizerNode>(anyJoin)->LeftAny = true;
        auto join = Join(anyJoin, "D", /*on=*/"A=D");
        
        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;
        UNIT_ASSERT(graph.GetEdges().size() !=  graph.GetSimpleEdges().size());

        Enumerate(join);
    }


    Y_UNIT_TEST(AnyJoinConstraints2) {
        auto anyJoin = Join(Join(Join("A", "B"), "C", /*on=*/ "B=C"), "D", "C=D");
        std::static_pointer_cast<TJoinOptimizerNode>(anyJoin)->LeftAny = true;
        auto join = Join(anyJoin, "E", /*on=*/ "A=E");
        
        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;
        UNIT_ASSERT(graph.GetEdges().size() !=  graph.GetSimpleEdges().size());

        Enumerate(join);
    }

    Y_UNIT_TEST(AnyJoinConstraints3) {
        auto anyJoin = Join(Join("A", "B"), Join("C", "D"), /*on=*/"B=C");
        std::static_pointer_cast<TJoinOptimizerNode>(anyJoin)->RightAny = true;
        auto join = Join(anyJoin, "E", /*on=*/ "C=E");
        
        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;
        UNIT_ASSERT(graph.GetEdges().size() !=  graph.GetSimpleEdges().size());

        Enumerate(join);
    }

    Y_UNIT_TEST(IsReorderableConstraint) {
        auto nonReorderable = Join(Join(Join("A", "B"), "C", /*on=*/ "B=C"), "D", "C=D");
        std::static_pointer_cast<TJoinOptimizerNode>(nonReorderable)->IsReorderable = false;
        auto join = Join(nonReorderable, "E", /*on=*/ "A=E");
        
        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;
        UNIT_ASSERT(graph.GetEdges().size() !=  graph.GetSimpleEdges().size());

        Enumerate(join);
    }

    Y_UNIT_TEST(JoinKindConflictSimple) {
        auto join = Join(FullJoin("A", "B"), "C", "B=C");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;

        UNIT_ASSERT(graph.GetEdges().size() == 4);

        auto A = graph.GetNodesByRelNames({"A"});
        auto B = graph.GetNodesByRelNames({"B"});
        auto C = graph.GetNodesByRelNames({"C"});
        UNIT_ASSERT(graph.FindEdgeBetween(A, B));
        UNIT_ASSERT(graph.FindEdgeBetween(B, A));
        UNIT_ASSERT(!graph.FindEdgeBetween(A | B, C)->IsSimple());
        UNIT_ASSERT(!graph.FindEdgeBetween(C, A | B)->IsSimple());

        Enumerate(join);
    }

    Y_UNIT_TEST(SimpleCycle) {
        auto join = Join("A", Join("B", "C"), "A=B,A=C");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;
        for (const auto& e: graph.GetEdges()) {
            UNIT_ASSERT(e.IsSimple());
        }

        auto optimizedJoin = Enumerate(join);
        UNIT_ASSERT(HaveSameConditions(optimizedJoin, join));
    }

    /* We shouldn't have complex edges in inner equijoins */
    Y_UNIT_TEST(TransitiveClosurePlusCycle) {
        auto join = Join("A", Join("B", Join("C", "D", "C.c0=D.d"), "B.b=C.c,B.b0=D.d1"), "A.a=B.b");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;
        for (const auto& e: graph.GetEdges()) {
            UNIT_ASSERT(e.IsSimple());
        }

        auto A = graph.GetNodesByRelNames({"A"});
        auto C = graph.GetNodesByRelNames({"C"});
        UNIT_ASSERT(graph.FindEdgeBetween(A, C));

        auto optimizedJoin = Enumerate(join);
        UNIT_ASSERT(HaveSameConditionCount(optimizedJoin, join));
    }

    Y_UNIT_TEST(CondsThatMayCauseATransitiveClosureButTheyMustNot) {
        auto join = Join("A", "B", "A.DOTA=B.LOL,A.LOL=B.LOL");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;
        for (const auto& e: graph.GetEdges()) {
            UNIT_ASSERT(e.IsSimple());
        }

        auto optimizedJoin = Enumerate(join);
        UNIT_ASSERT(HaveSameConditions(optimizedJoin, join));
    }

    Y_UNIT_TEST(TransitiveClosureManyCondsBetweenJoin) {
        auto join = FullJoin(Join(Join("A", "B", "A.ID=B.ID,A.LOL=B.LOL"), "C", "A.ID=C.ID,A.KEK=C.KEK"), "D", "A.ID=D.ID");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;

        auto B = graph.GetNodesByRelNames({"B"});
        auto C = graph.GetNodesByRelNames({"C"});
        UNIT_ASSERT(graph.FindEdgeBetween(B, C));

        {
            auto optimizedJoin = Enumerate(join, TOptimizerHints::Parse("Rows(B C # 0)"));
            UNIT_ASSERT(HaveSameConditionCount(optimizedJoin, join));
        }
        {
            auto optimizedJoin = Enumerate(join, TOptimizerHints::Parse("JoinOrder((A B) C)"));
            UNIT_ASSERT(HaveSameConditions(optimizedJoin, join));
        }
    }

    Y_UNIT_TEST(ManyCondsBetweenJoinForTransitiveClosure) {
        auto join = Join(Join("A", "B", "A.PUDGE=B.PUDGE,A.DOTA=B.DOTA"), "C", "A.PUDGE=C.PUDGE,A.DOTA=C.DOTA");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;

        auto B = graph.GetNodesByRelNames({"B"});
        auto C = graph.GetNodesByRelNames({"C"});
        UNIT_ASSERT(graph.FindEdgeBetween(B, C));

        {
            auto optimizedJoin = Enumerate(join, TOptimizerHints::Parse("Rows(B C # 0)"));
            UNIT_ASSERT(HaveSameConditionCount(optimizedJoin, join));
        }
    }

    auto MakeClique(size_t size) {
        std::shared_ptr<IBaseOptimizerNode> root = Join("R0", "R1", "R0.id=R1.id");

        for (size_t i = 2; i < size; ++i) {
            TString attr = ToString(rand());
            TString on = Sprintf("R%ld.id=R%ld.id", i - 1, i);
            root = Join(root, Sprintf("R%ld", i), on);
        }

        auto graph = MakeJoinHypergraph<TNodeSet64>(root);
        Cout << graph.String() << Endl;
        CheckClique(graph);

        return root;
    }

    auto MakeChain(size_t size) {
        std::shared_ptr<IBaseOptimizerNode> root = Join("R0", "R1");

        for (size_t i = 2; i < size; ++i) {
            TString attr = ToString(rand());
            TString on = Sprintf("R%ld.%s=R%ld.%s", i - 1, attr.c_str(), i, attr.c_str());
            root = Join(root, Sprintf("R%ld", i), on);
        }

        auto graph = MakeJoinHypergraph<std::bitset<196>>(root);
        Cout << graph.String() << Endl;
        return root;
    }

    auto MakeStar(size_t size) {
        std::shared_ptr<IBaseOptimizerNode> root = Join("R0", "R1");

        for (size_t i = 2; i < size; ++i) {
            TString attr = ToString(rand());
            TString on = Sprintf("R0.%s=R%ld.%s", attr.c_str(), i, attr.c_str());
            root = Join(root, Sprintf("R%ld", i), on);
        }

        auto graph = MakeJoinHypergraph<TNodeSet64>(root);
        Cout << graph.String() << Endl;
        return root;
    }

    Y_UNIT_TEST(JoinTopologiesBenchmark) {
        #if defined(_asan_enabled_)
            enum { CliqueSize = 0, ChainSize = 0, StarSize = 0 };
            std::cerr << "test is not running for ASAN!" << std::endl;
            return;
        #elif !defined(NDEBUG)
            enum { CliqueSize = 11, ChainSize = 71, StarSize = 15 };
        #else
            enum { CliqueSize = 15, ChainSize = 165, StarSize = 20 };
        #endif

        {
            size_t cliqueSize = CliqueSize;
            auto startClique = std::chrono::high_resolution_clock::now();
            Enumerate(MakeClique(cliqueSize));
            auto endClique = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> durationClique = endClique - startClique;
            std::cerr << Sprintf("Time for Enumerate(MakeClique(%ld)): %f seconds", cliqueSize, durationClique.count()) << std::endl;
        }

        {
            size_t starSize = StarSize;
            auto startStar = std::chrono::high_resolution_clock::now();
            Enumerate(MakeStar(starSize));
            auto endStar = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> durationStar = endStar - startStar;
            std::cerr << Sprintf("Time for Enumerate(MakeStar(%ld)): %f seconds", starSize, durationStar.count()) << std::endl;
        }

        {
            size_t chainSize = ChainSize;
            auto startChain = std::chrono::high_resolution_clock::now();
            Enumerate(MakeChain(chainSize));
            auto endChain = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> durationChain = endChain - startChain;
            std::cerr << Sprintf("Time for Enumerate(MakeChain(%ld)): %f seconds", chainSize, durationChain.count()) << std::endl;
        }
    }

}
