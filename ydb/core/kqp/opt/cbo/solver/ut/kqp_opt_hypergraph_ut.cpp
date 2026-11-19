#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_join.h>

#include <util/string/split.h>

#include "kqp_opt_make_join_hypergraph.h"
#include "kqp_opt_join_condition_completion.h"
#include "kqp_opt_join_cost_based.h"

#include <memory>
#include <map>
#include <optional>
#include <set>
#include <tuple>

using namespace NKikimr::NKqp;

std::shared_ptr<IBaseOptimizerNode> CreateChain(size_t size, TString onAttribute, TString tablePrefix="e") {
    std::shared_ptr<IBaseOptimizerNode> root = std::make_shared<TRelOptimizerNode>(tablePrefix + "1", TOptimizerStatistics());
    root->Stats.Labels = std::make_shared<TVector<TString>>(TVector<TString>{tablePrefix + "1"});
    for (size_t i = 1; i < size; ++i) {
        auto eiStr = tablePrefix + ToString(i + 1);
        auto eiPrevStr = tablePrefix + ToString(i);

        auto ei = std::make_shared<TRelOptimizerNode>(eiStr, TOptimizerStatistics());
        ei->Stats.Labels = std::make_shared<TVector<TString>>(TVector<TString>{eiStr});

        TVector<TJoinColumn> leftKeys = {TJoinColumn(eiPrevStr, onAttribute)};
        TVector<TJoinColumn> rightKeys = {TJoinColumn(eiStr, onAttribute)};

        root = std::make_shared<TJoinOptimizerNode>(
            root, ei, leftKeys, rightKeys, EJoinKind::InnerJoin, EJoinAlgoType::Undefined, false, false
        );
    }

    return root;
}

struct TTestContext : public TBaseProviderContext {
    bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& ,
        const std::shared_ptr<IBaseOptimizerNode>& ,
        const TVector<TJoinColumn>& ,
        const TVector<TJoinColumn>& ,
        EJoinAlgoType ,
        EJoinKind
    ) override {
        return true;
    }
};

template <typename TProviderContext = TTestContext>
std::shared_ptr<IBaseOptimizerNode> Enumerate(const std::shared_ptr<IBaseOptimizerNode>& root, const TOptimizerHints& hints = {}, bool shuffleElimination = false, bool completeNonInnerConditions = true) {
    auto ctx = TProviderContext();
    NYql::TExprContext dummyCtx;

    TSimpleSharedPtr<TOrderingsStateMachine> orderings;
    if (shuffleElimination) {
        auto graph = MakeJoinHypergraph<std::bitset<64>>(root);
        TFDStorage storage;
        for (const auto& edge : graph.GetEdges()) {
            if (edge.JoinKind == EJoinKind::Cross) {
                continue;
            }
            storage.AddInterestingOrdering(edge.LeftJoinKeys, TOrdering::EShuffle, nullptr);
            for (const auto& [lhs, rhs] : Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
                storage.AddFD(lhs, rhs, TFunctionalDependency::EEquivalence, false, nullptr);
            }
        }
        orderings = MakeSimpleShared<TOrderingsStateMachine>(std::move(storage), TOrdering::EShuffle);
        for (const auto& node : graph.GetNodes()) {
            node.RelationOptimizerNode->Stats.LogicalOrderings = orderings->CreateState();
        }
    }
    TCBOSettings settings{};
    settings.EnableNonInnerJoinConditionCompletion = completeNonInnerConditions;
    auto optimizer =
        std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(ctx, settings, dummyCtx, shuffleElimination, orderings));

    Y_ENSURE(root->Kind == EOptimizerNodeKind::JoinNodeType);
    auto res = optimizer->JoinSearch(std::static_pointer_cast<TJoinOptimizerNode>(root), hints);
    Cout << "Optimized Tree:" << Endl;
    std::stringstream ss; res->Print(ss);
    Cout << ss.str() << Endl;
    return res;
}

using TTestCondition = std::tuple<TJoinColumn, TJoinColumn, bool>;

std::set<TTestCondition> CollectConditions(const std::shared_ptr<IBaseOptimizerNode>& node) {
    if (node->Kind != EOptimizerNodeKind::JoinNodeType) {
        return {};
    }
    auto join = std::static_pointer_cast<TJoinOptimizerNode>(node);
    auto result = CollectConditions(join->LeftArg);
    const auto rightConditions = CollectConditions(join->RightArg);
    result.insert(rightConditions.begin(), rightConditions.end());
    auto add = [&](const TJoinColumn& left, const TJoinColumn& right) {
        const bool equalNulls = IsEqualNullsKey(left, right);
        result.emplace(right < left ? right : left, right < left ? left : right, equalNulls);
    };
    for (const auto& [left, right] : Zip(join->LeftJoinKeys, join->RightJoinKeys)) {
        add(left, right);
    }
    for (const auto& [left, right] : join->PostJoinConditions) {
        add(left, right);
    }
    return result;
}

bool HasAllConditions(const std::shared_ptr<IBaseOptimizerNode>& actual, const std::shared_ptr<IBaseOptimizerNode>& expected) {
    const auto actualConditions = CollectConditions(actual);
    const auto expectedConditions = CollectConditions(expected);
    return std::includes(actualConditions.begin(), actualConditions.end(), expectedConditions.begin(), expectedConditions.end());
}

// Small bag-semantics interpreter for checking predicate placement independently
// of the enumerator. Absent columns denote null-extended values.
using TTestRow = std::map<TString, std::optional<int>>;
using TTestBag = TVector<TTestRow>;
using TTestTables = std::map<TString, TTestBag>;

TTestBag EvaluateJoin(const std::shared_ptr<IBaseOptimizerNode>& node, const TTestTables& tables) {
    if (node->Kind == RelNodeType) {
        return tables.at(node->Labels().front());
    }
    const auto join = std::static_pointer_cast<TJoinOptimizerNode>(node);
    UNIT_ASSERT(join->JoinType == EJoinKind::InnerJoin || join->JoinType == EJoinKind::LeftJoin);
    const auto left = EvaluateJoin(join->LeftArg, tables);
    const auto right = EvaluateJoin(join->RightArg, tables);
    auto matches = [](const TTestRow& row, const auto& conditions) {
        auto value = [&](const TJoinColumn& column) -> std::optional<int> {
            const auto it = row.find(column.RelName + "." + column.AttributeName);
            return it == row.end() ? std::nullopt : it->second;
        };
        for (const auto& [lhs, rhs] : conditions) {
            const auto l = value(lhs), r = value(rhs);
            if (l != r || (!IsEqualNullsKey(lhs, rhs) && (!l || !r))) {
                return false;
            }
        }
        return true;
    };
    TTestBag result;
    for (const auto& l : left) {
        bool matched = false;
        for (const auto& r : right) {
            auto row = l;
            row.insert(r.begin(), r.end());
            if (matches(row, Zip(join->LeftJoinKeys, join->RightJoinKeys))) {
                matched = true;
                if (matches(row, join->PostJoinConditions)) {
                    result.push_back(std::move(row));
                }
            }
        }
        if (!matched && join->JoinType == EJoinKind::LeftJoin && matches(l, join->PostJoinConditions)) {
            result.push_back(l);
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}

Y_UNIT_TEST_SUITE(KqpHypergraphBuild) {
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

        TVector<TJoinColumn> leftKeys = {TJoinColumn("a3", "1337")};
        TVector<TJoinColumn> rightKeys = {TJoinColumn("b1", "1337")};

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
            std::shared_ptr<IBaseOptimizerNode> root = std::make_shared<TRelOptimizerNode>(joinArg, TOptimizerStatistics());
            root->Stats.Nrows = rand() % 100 + 1;
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
            if (on.empty()) {
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
    std::shared_ptr<IBaseOptimizerNode> CrossJoin(const TLhsArg& lhsArg, const TRhsArg& rhsArg) {
        return std::make_shared<TJoinOptimizerNode>(
            TJoinOptimizerNode(
                GetJoinArg(lhsArg),
                GetJoinArg(rhsArg),
                {},
                {},
                EJoinKind::Cross,
                EJoinAlgoType::Undefined,
                false,
                false
            )
        );
    }

    Y_UNIT_TEST(SimpleDimpleJoin) {
        auto join = Join("A", "B");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;

        Enumerate(join);
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

    Y_UNIT_TEST(SplitInnerJoinRestoresPostJoinFilter) {
        for (bool shuffle : {false, true}) {
            for (bool equalNulls : {false, true}) {
                auto a = GetJoinArg("A");
                auto b = GetJoinArg("B");
                auto c = GetJoinArg("C");
                a->Stats.Nrows = 100;
                b->Stats.Nrows = 1000;
                c->Stats.Nrows = 1;
                auto original = Join(LeftJoin(a, b, "A.id=B.a"), c, "A.id=C.a,B.x=C.x");
                auto source = std::static_pointer_cast<TJoinOptimizerNode>(original);
                source->LeftJoinKeys.back().EqualNulls = source->RightJoinKeys.back().EqualNulls = equalNulls;
                auto optimized = Enumerate(original, {}, shuffle);
                auto result = std::static_pointer_cast<TJoinOptimizerNode>(optimized);
                UNIT_ASSERT_VALUES_EQUAL(result->JoinType, EJoinKind::LeftJoin);
                UNIT_ASSERT(result->RightArg == b);
                UNIT_ASSERT_VALUES_EQUAL(result->LeftJoinKeys.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result->PostJoinConditions.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result->PostJoinConditions.front().first.EqualNulls, equalNulls);
                UNIT_ASSERT(HasAllConditions(optimized, original));

                const TVector<TVector<std::optional<int>>> bags{
                    {}, {std::nullopt}, {0}, {1}, {0, 0}, {std::nullopt, 1}, {std::nullopt, std::nullopt}
                };
                for (const auto& bValues : bags) {
                    for (const auto& cValues : bags) {
                        TTestTables tables{{"A", {{{"A.id", 1}}}}, {"B", {}}, {"C", {}}};
                        for (const auto& x : bValues) {
                            tables["B"].push_back({{"B.a", 1}, {"B.x", x}});
                        }
                        for (const auto& x : cValues) {
                            tables["C"].push_back({{"C.a", 1}, {"C.x", x}});
                        }
                        UNIT_ASSERT(EvaluateJoin(original, tables) == EvaluateJoin(optimized, tables));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(PostJoinFiltersStayInsideNullableInput) {
        for (bool shuffle : {false, true}) {
            auto abc = Join(LeftJoin("A", "B", "A.id=B.a"), "C", "A.id=C.a,B.x=C.x");
            auto abcde = Join(LeftJoin(abc, "D", "A.id=D.a"), "E", "A.id=E.a,D.y=E.y");
            for (auto node : {abc, abcde}) {
                auto join = std::static_pointer_cast<TJoinOptimizerNode>(node);
                join->LeftJoinKeys.back().EqualNulls = join->RightJoinKeys.back().EqualNulls = true;
            }
            auto original = LeftJoin("F", abcde, "F.id=A.id");
            auto optimized = Enumerate(original,
                TOptimizerHints::Parse("JoinOrder(F ((((A C) B) E) D))"), shuffle);
            UNIT_ASSERT(optimized != original);
            UNIT_ASSERT(HasAllConditions(optimized, original));
            auto root = std::static_pointer_cast<TJoinOptimizerNode>(optimized);
            UNIT_ASSERT(root->PostJoinConditions.empty());
            auto dJoin = std::static_pointer_cast<TJoinOptimizerNode>(root->RightArg);
            UNIT_ASSERT_VALUES_EQUAL(dJoin->JoinType, EJoinKind::LeftJoin);
            UNIT_ASSERT_VALUES_EQUAL(dJoin->PostJoinConditions.size(), 1);
            auto eJoin = std::static_pointer_cast<TJoinOptimizerNode>(dJoin->LeftArg);
            auto bJoin = std::static_pointer_cast<TJoinOptimizerNode>(eJoin->LeftArg);
            UNIT_ASSERT_VALUES_EQUAL(bJoin->JoinType, EJoinKind::LeftJoin);
            UNIT_ASSERT_VALUES_EQUAL(bJoin->PostJoinConditions.size(), 1);

            const TTestTables tables{
                {"A", {{{"A.id", 1}}, {{"A.id", 2}}, {{"A.id", 3}}}},
                {"F", {{{"F.id", 1}}, {{"F.id", 2}}, {{"F.id", 3}}}},
                {"B", {{{"B.a", 2}, {"B.x", 7}}}},
                {"C", {{{"C.a", 1}, {"C.x", std::nullopt}}, {{"C.a", 2}, {"C.x", 8}}, {{"C.a", 3}, {"C.x", std::nullopt}}}},
                {"D", {{{"D.a", 3}, {"D.y", 9}}}},
                {"E", {{{"E.a", 1}, {"E.y", std::nullopt}}, {{"E.a", 2}, {"E.y", std::nullopt}}, {{"E.a", 3}, {"E.y", 10}}}}
            };
            const auto expected = EvaluateJoin(original, tables);
            UNIT_ASSERT_VALUES_EQUAL(expected.size(), 3);
            UNIT_ASSERT(expected == EvaluateJoin(optimized, tables));

            // Moving either residual to the final LEFT join's output is wrong.
            for (auto join : {bJoin, dJoin}) {
                root->PostJoinConditions.swap(join->PostJoinConditions);
                UNIT_ASSERT(expected != EvaluateJoin(optimized, tables));
                root->PostJoinConditions.swap(join->PostJoinConditions);
            }
        }
    }

    Y_UNIT_TEST(PostJoinFilterCanBeDelayedAcrossInnerJoins) {
        auto original = Join(Join(LeftJoin("A", "B", "A.id=B.a"), "C", "A.c=C.a,B.x=C.x"),
            "D", "A.d=D.a");
        auto optimized = Enumerate(original, TOptimizerHints::Parse("JoinOrder(((A C) D) B)"), false);
        UNIT_ASSERT(optimized != original);
        UNIT_ASSERT(HasAllConditions(optimized, original));
        UNIT_ASSERT_VALUES_EQUAL(std::static_pointer_cast<TJoinOptimizerNode>(optimized)->PostJoinConditions.size(), 1);
    }

    Y_UNIT_TEST(PostJoinFilterCanCrossAPreservedOuterInput) {
        auto abc = Join(LeftJoin("A", "B", "A.id=B.a"), "C", "A.id=C.a,B.x=C.x");
        auto source = std::static_pointer_cast<TJoinOptimizerNode>(abc);
        source->LeftJoinKeys.back().EqualNulls = source->RightJoinKeys.back().EqualNulls = true;
        auto original = LeftJoin(abc, "D", "A.d=D.a");
        auto optimized = Enumerate(original, TOptimizerHints::Parse("JoinOrder(((A C) D) B)"), false);
        UNIT_ASSERT(optimized != original);
        UNIT_ASSERT(HasAllConditions(optimized, original));
        auto root = std::static_pointer_cast<TJoinOptimizerNode>(optimized);
        UNIT_ASSERT_VALUES_EQUAL(root->PostJoinConditions.size(), 1);
        TTestTables tables{
            {"A", {{{"A.id", 1}, {"A.d", 1}}}}, {"B", {}},
            {"C", {{{"C.a", 1}, {"C.x", std::nullopt}}}},
            {"D", {{{"D.a", 1}, {"D.id", 10}}, {{"D.a", 1}, {"D.id", 11}}}}
        };
        UNIT_ASSERT_VALUES_EQUAL(EvaluateJoin(original, tables).size(), 2);
        UNIT_ASSERT(EvaluateJoin(original, tables) == EvaluateJoin(optimized, tables));
        tables["B"] = {{{"B.a", 1}, {"B.x", 7}}};
        UNIT_ASSERT(EvaluateJoin(original, tables).empty());
        UNIT_ASSERT(EvaluateJoin(optimized, tables).empty());
    }

    Y_UNIT_TEST(PostJoinFilterRestoresAnEarlierUnsplitPredicate) {
        auto abc = Join(LeftJoin("A", "B", "A.id=B.a"), "C", "B.x=C.x");
        auto source = std::static_pointer_cast<TJoinOptimizerNode>(abc);
        source->LeftJoinKeys.front().EqualNulls = source->RightJoinKeys.front().EqualNulls = true;
        auto original = Join(abc, "D", "A.d=D.a,C.d=D.c");
        auto optimized = Enumerate(original, TOptimizerHints::Parse("JoinOrder(((A D) C) B)"), false);
        UNIT_ASSERT(optimized != original);
        UNIT_ASSERT(HasAllConditions(optimized, original));
        auto root = std::static_pointer_cast<TJoinOptimizerNode>(optimized);
        UNIT_ASSERT_VALUES_EQUAL(root->JoinType, EJoinKind::LeftJoin);
        UNIT_ASSERT_VALUES_EQUAL(root->PostJoinConditions.size(), 1);
        UNIT_ASSERT(root->PostJoinConditions.front().first.EqualNulls);
    }

    Y_UNIT_TEST(PostJoinFilterWithReassociatedLeftJoins) {
        // Strict B.x=C.x rejects an unmatched B. The two LEFT joins can
        // reassociate, and the displaced INNER key B.y=D.y still needs a filter.
        auto abc = LeftJoin(LeftJoin("A", "B", "A.id=B.a"), "C", "B.x=C.x");
        auto original = Join(abc, "D", "A.id=D.a,B.y=D.y");
        auto source = std::static_pointer_cast<TJoinOptimizerNode>(original);
        source->LeftJoinKeys.back().EqualNulls = source->RightJoinKeys.back().EqualNulls = true;
        const TTestTables tables{
            {"A", {{{"A.id", 1}}, {{"A.id", 2}}, {{"A.id", 3}}}},
            {"B", {{{"B.a", 1}, {"B.x", 7}, {"B.y", 10}}, {{"B.a", 2}, {"B.x", 7}, {"B.y", 20}}}},
            {"C", {{{"C.x", 7}, {"C.id", 8}}, {{"C.x", std::nullopt}, {"C.id", 9}}}},
            {"D", {{{"D.a", 1}, {"D.y", 10}}, {{"D.a", 2}, {"D.y", 0}}, {{"D.a", 3}, {"D.y", std::nullopt}}}}
        };
        const auto expected = EvaluateJoin(original, tables);
        UNIT_ASSERT_VALUES_EQUAL(expected.size(), 2);
        for (bool shuffle : {false, true}) {
            auto optimized = Enumerate(original, TOptimizerHints::Parse("JoinOrder((A D) (B C))"), shuffle);
            const auto root = std::static_pointer_cast<TJoinOptimizerNode>(optimized);
            UNIT_ASSERT(optimized != original);
            UNIT_ASSERT_VALUES_EQUAL(root->RightArg->Labels().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(root->PostJoinConditions.size(), 1);
            UNIT_ASSERT(HasAllConditions(optimized, original));
            UNIT_ASSERT(expected == EvaluateJoin(optimized, tables));
        }
    }

    Y_UNIT_TEST(OnlyOrdinaryInnerPredicatesCanBecomeFilters) {
        for (bool any : {false, true}) {
            auto original = Join(LeftJoin("A", "B", "A.id=B.a"), "C", "A.c=C.a,B.x=C.x");
            auto join = std::static_pointer_cast<TJoinOptimizerNode>(original);
            join->RightAny = any;
            join->IsReorderable = any;
            auto graph = MakeJoinHypergraph<TNodeSet64>(original);
            auto candidate = LeftJoin(Join("A", "C", "A.c=C.a"), "B", "A.id=B.a");
            UNIT_ASSERT_EXCEPTION_CONTAINS(AddMissingConditions(graph, candidate), yexception, "Cannot place");
            UNIT_ASSERT(std::static_pointer_cast<TJoinOptimizerNode>(candidate)->PostJoinConditions.empty());
        }
    }

    Y_UNIT_TEST(SplitInnerJoinAboveLeftJoinStillReorders) {
        for (bool shuffleElimination : {false, true}) {
            auto join = Join(
                Join(LeftJoin("A", "B", "A.id=B.a_id"), "C", "A.x=C.x"),
                "D", "A.z=D.z,C.w=D.w");

            // Splitting above a left join remains useful: C-D can be joined
            // first, and the left join can move above both inner joins.
            auto optimized = Enumerate(join,
                TOptimizerHints::Parse("JoinOrder(((C D) A) B)"), shuffleElimination);
            UNIT_ASSERT(optimized != join);
            UNIT_ASSERT(HasAllConditions(optimized, join));
            auto root = std::static_pointer_cast<TJoinOptimizerNode>(optimized);
            UNIT_ASSERT_VALUES_EQUAL(root->JoinType, EJoinKind::LeftJoin);
            UNIT_ASSERT_VALUES_EQUAL(root->RightArg->Labels().front(), "B");
            auto inner = std::static_pointer_cast<TJoinOptimizerNode>(root->LeftArg);
            UNIT_ASSERT_VALUES_EQUAL(inner->RightArg->Labels().front(), "A");
            UNIT_ASSERT_VALUES_EQUAL(inner->LeftArg->Labels().size(), 2);
        }
    }

    Y_UNIT_TEST(ConditionCompletionIsOptInForNewRbo) {
        for (bool shuffle : {false, true}) {
            auto makeQuery = [] {
                return Join(LeftJoin("A", "B", "A.k=B.a"), "C", "A.k=C.a,B.x=C.x");
            };
            const auto hints = TOptimizerHints::Parse("JoinOrder((A C) B)");
            auto legacy = std::static_pointer_cast<TJoinOptimizerNode>(Enumerate(makeQuery(), hints, shuffle, false));
            auto newRbo = std::static_pointer_cast<TJoinOptimizerNode>(Enumerate(makeQuery(), hints, shuffle, true));
            // Legacy CBO retains its original ON-key completion; only NewRBO
            // restores the displaced equality after null extension.
            UNIT_ASSERT(legacy->PostJoinConditions.empty());
            UNIT_ASSERT(!newRbo->PostJoinConditions.empty());
        }
    }

    Y_UNIT_TEST(ConditionCompletionDoesNotDeduplicateDifferentRightKeys) {
        auto original = LeftJoin("A", "B", "A.k=B.x,A.k=B.y");
        auto graph = MakeJoinHypergraph<TNodeSet64>(original);
        auto candidate = LeftJoin("A", "B", "A.k=B.x");
        AddMissingConditions(graph, candidate);

        auto join = std::static_pointer_cast<TJoinOptimizerNode>(candidate);
        UNIT_ASSERT_VALUES_EQUAL(join->LeftJoinKeys.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(join->RightJoinKeys.size(), 2);
        UNIT_ASSERT(HasAllConditions(candidate, original));
        UNIT_ASSERT(join->PostJoinConditions.empty());
    }

    Y_UNIT_TEST(ConditionCompletionPreservesTransitiveReordering) {
        for (bool shuffleElimination : {false, true}) {
            auto join = Join(Join("A", "B", "A.k=B.k"), "C", "B.k=C.k");

            // A-C is inferred. Completion keeps the reordered tree and
            // explicitly restores both original equalities.
            auto optimized = Enumerate(join,
                TOptimizerHints::Parse("JoinOrder((A C) B)"), shuffleElimination);
            UNIT_ASSERT(optimized != join);
            UNIT_ASSERT(HasAllConditions(optimized, join));
            auto root = std::static_pointer_cast<TJoinOptimizerNode>(optimized);
            UNIT_ASSERT_VALUES_EQUAL(root->LeftArg->Labels().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(root->RightArg->Labels().front(), "B");
        }
    }

    Y_UNIT_TEST(ConditionCompletionMustEstablishOriginalEqualities) {
        for (bool shuffle : {false, true}) {
            auto original = Join(Join("A", "B", "A.x=B.x,A.y=B.y"), "C", "A.x=C.x,A.y=C.y,B.x=C.y");
            auto optimized = Enumerate(original, TOptimizerHints::Parse("JoinOrder((A B) C)"), shuffle);
            UNIT_ASSERT(optimized != original);
            UNIT_ASSERT(HasAllConditions(optimized, original));

            // All columns share an original equivalence class, but the chosen
            // A-B and A-C keys alone only establish separate x and y groups.
            TTestTables tables{
                {"A", {{{"A.x", 1}, {"A.y", 2}}}},
                {"B", {{{"B.x", 1}, {"B.y", 2}}}},
                {"C", {{{"C.x", 1}, {"C.y", 2}}}}
            };
            UNIT_ASSERT(EvaluateJoin(original, tables).empty());
            UNIT_ASSERT(EvaluateJoin(optimized, tables).empty());
            for (auto& [name, rows] : tables) {
                rows.front()[name + ".y"] = 1;
            }
            UNIT_ASSERT_VALUES_EQUAL(EvaluateJoin(optimized, tables).size(), 1);
        }
    }

    Y_UNIT_TEST(ConditionCompletionIncludesInferredEqualities) {
        auto original = Join(Join("A", "B", "A.x=B.x"), "C", "B.x=C.x");
        auto graph = MakeJoinHypergraph<TNodeSet64>(original);
        UNIT_ASSERT(graph.FindEdgeBetween(graph.GetNodesByRelNames({"A"}), graph.GetNodesByRelNames({"C"})));

        // Completion also restores the inferred A.x=C.x from the live graph.
        AddMissingConditions(graph, original);
        UNIT_ASSERT_VALUES_EQUAL(CollectConditions(original).size(), 3);
    }

    Y_UNIT_TEST(ConditionCompletionRestoresMissingColumnPair) {
        auto original = Join(Join("A", "B", "A.x=B.x,A.y=B.y"), "C", "A.x=C.x,A.y=C.y,B.x=C.y");
        auto graph = MakeJoinHypergraph<TNodeSet64>(original);
        auto incomplete = Join(Join("A", "B", "A.x=B.x,A.y=B.y"), "C", "A.x=C.x,A.y=C.y");

        // Every column occurs, but B.x=C.y is absent.
        UNIT_ASSERT(!HasAllConditions(incomplete, original));
        AddMissingConditions(graph, incomplete);
        UNIT_ASSERT(HasAllConditions(incomplete, original));
    }

    Y_UNIT_TEST(ConditionCompletionDistinguishesNullSafeEquality) {
        auto original = Join("A", "B", "A.x=B.x,A.x=B.x");
        auto source = std::static_pointer_cast<TJoinOptimizerNode>(original);
        source->LeftJoinKeys.back().EqualNulls = source->RightJoinKeys.back().EqualNulls = true;
        auto graph = MakeJoinHypergraph<TNodeSet64>(original);
        auto candidate = Join("A", "B", "A.x=B.x");
        AddMissingConditions(graph, candidate);
        UNIT_ASSERT(HasAllConditions(candidate, original));
    }

    Y_UNIT_TEST(ConditionCompletionRejectsMissingOuterJoin) {
        auto original = LeftJoin("A", "B", "A.x=B.x");
        auto graph = MakeJoinHypergraph<TNodeSet64>(original);
        auto candidate = Join("A", "B", "A.x=B.x");
        UNIT_ASSERT_EXCEPTION_CONTAINS(AddMissingConditions(graph, candidate), yexception, "Cannot place");
    }

    Y_UNIT_TEST(ConditionCompletionRestoresNullSafePredicatesSharingAColumn) {
        for (bool shuffleElimination : {false, true}) {
            auto join = Join(Join("B", "C", "B.x=C.x"), "A", "B.k=A.k,C.k=A.k");
            auto root = std::static_pointer_cast<TJoinOptimizerNode>(join);
            for (auto& key : root->LeftJoinKeys) {
                key.EqualNulls = true;
            }
            for (auto& key : root->RightJoinKeys) {
                key.EqualNulls = true;
            }

            // These predicates share A.k, but neither implies the other.
            // The chosen edge supplies one; completion must restore the second.
            auto optimized = Enumerate(join,
                TOptimizerHints::Parse("JoinOrder(A (B C))"), shuffleElimination);
            UNIT_ASSERT(optimized != join);
            UNIT_ASSERT(HasAllConditions(optimized, join));
            auto result = std::static_pointer_cast<TJoinOptimizerNode>(optimized);
            UNIT_ASSERT_VALUES_EQUAL(result->LeftJoinKeys.size(), 2);
            UNIT_ASSERT(result->LeftJoinKeys[0].EqualNulls && result->LeftJoinKeys[1].EqualNulls);
            UNIT_ASSERT(result->RightJoinKeys[0].EqualNulls && result->RightJoinKeys[1].EqualNulls);
        }
    }

    Y_UNIT_TEST(ShuffleEliminationRespectsCrossJoinOrderHint) {
        auto small = GetJoinArg("Small");
        auto big = GetJoinArg("Big");
        small->Stats.Nrows = 10;
        big->Stats.Nrows = 1000000;
        auto query = CrossJoin(small, big);

        auto unhinted = std::static_pointer_cast<TJoinOptimizerNode>(Enumerate(query, {}, true));
        UNIT_ASSERT_VALUES_EQUAL(unhinted->RightArg->Labels().front(), "Small");

        auto hinted = std::static_pointer_cast<TJoinOptimizerNode>(
            Enumerate(query, TOptimizerHints::Parse("JoinOrder(Small Big)"), true));
        UNIT_ASSERT(hinted != query);
        UNIT_ASSERT_VALUES_EQUAL(hinted->RightArg->Labels().front(), "Big");
    }

    Y_UNIT_TEST(CrossJoinEliminationSimple) {
        // A simple tree A - C - B. The inner join A=C,B=C will be split
        // into two inner joins. And cross join {A}->{B} should be
        // eliminated completely, leaving only two edges.

        auto join = Join(CrossJoin("A", "B"), "C", "C=A,C=B");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;

        UNIT_ASSERT(graph.GetEdges().size() == 2*2);

        auto A = graph.GetNodesByRelNames({"A"});
        auto B = graph.GetNodesByRelNames({"B"});
        auto C = graph.GetNodesByRelNames({"C"});

        // Inner join should split into two:
        UNIT_ASSERT(graph.FindEdgeBetween(A, C));
        UNIT_ASSERT(graph.FindEdgeBetween(C, B));

        // Cross join should be eliminated:
        UNIT_ASSERT(!graph.FindEdgeBetween(A, B));

        Enumerate(join);
    }

    Y_UNIT_TEST(CrossJoinEliminationComplex) {
        // Significantly more elaborate example, which contains
        // 5 cross joins, only two of which are real.

        // Not only that, it requires two passes of AddCrossJoins method,
        // which makes it a good test of correctness of connectivity logic.
        //
        // Why does it require two passes?
        //
        // 1. First edge that will be generated in the hypergraph is
        //    left join  {A, B}->{M}. This happens because the tree
        //    is traversed depth-first left-to-right and all cross
        //    joins are skipped in the initial hypergraph (and added
        //    back later only if it is absolutely necessary).
        // 2. This edge is disabled on the first pass, because A and B
        //    are not yet connected (they will be connected later
        //    by outside inner joins {F}->{A}, {F}->{B})
        // 3. Because of that no cross joins can be enabled on the
        //    first connectivity pass (both of them depend on A,B,M
        //    all being connected). Therefore single pass will not
        //    suffice -- if we stopped there, resulting graph would
        //    have 3 separate connected components.
        // 4. On the second pass {A, B}->{M} will be enabled, which
        //    then will allow both real cross joins to be added too


        // So this example has real and fake cross joins below and
        // above a hyperedge (caused by left join)

        auto join =
            CrossJoin(
                Join(
                    CrossJoin(
                        CrossJoin(
                            LeftJoin(
                                CrossJoin(
                                    CrossJoin(
                                        CrossJoin("A", "B"),
                                        "C"
                                    ),
                                    "D"
                                ),
                                "M",
                                "M=A,M=B"
                            ),
                            "H"
                        ),
                        "E"
                    ),
                    "F",
                    "A=F,B=F,C=F,D=F,E=F"
                ),
                "N"
            );

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;

        UNIT_ASSERT(graph.GetEdges().size() == 2*8);

        auto A = graph.GetNodesByRelNames({"A"});
        auto B = graph.GetNodesByRelNames({"B"});
        auto C = graph.GetNodesByRelNames({"C"});
        auto D = graph.GetNodesByRelNames({"D"});
        auto M = graph.GetNodesByRelNames({"M"});
        auto H = graph.GetNodesByRelNames({"H"});
        auto E = graph.GetNodesByRelNames({"E"});
        auto F = graph.GetNodesByRelNames({"F"});
        auto N = graph.GetNodesByRelNames({"N"});

        auto assertEdge = [&](TNodeSet64 lhs, TNodeSet64 rhs, EJoinKind kind) {
            auto* edge = graph.FindEdgeBetween(lhs, rhs);
            UNIT_ASSERT(edge);
            UNIT_ASSERT(edge->JoinKind == kind);
        };

        assertEdge(A | B, M, EJoinKind::LeftJoin);
        assertEdge(A, F, EJoinKind::InnerJoin);
        assertEdge(B, F, EJoinKind::InnerJoin);
        assertEdge(C, F, EJoinKind::InnerJoin);
        assertEdge(D, F, EJoinKind::InnerJoin);
        assertEdge(E, F, EJoinKind::InnerJoin);
        assertEdge(A | B | C | D | M, H, EJoinKind::Cross);
        assertEdge(A | B | C | D | M | H | E | F, N, EJoinKind::Cross);

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
        UNIT_ASSERT(HasAllConditions(optimizedJoin, join));
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
        UNIT_ASSERT(HasAllConditions(optimizedJoin, join));
    }

    Y_UNIT_TEST(CondsThatMayCauseATransitiveClosureButTheyMustNot) {
        auto join = Join("A", "B", "A.DOTA=B.LOL,A.LOL=B.LOL");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;
        for (const auto& e: graph.GetEdges()) {
            UNIT_ASSERT(e.IsSimple());
        }

        auto optimizedJoin = Enumerate(join);
        UNIT_ASSERT(HasAllConditions(optimizedJoin, join));
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
            UNIT_ASSERT(HasAllConditions(optimizedJoin, join));
        }
        {
            auto optimizedJoin = Enumerate(join, TOptimizerHints::Parse("JoinOrder((A B) C)"));
            UNIT_ASSERT(HasAllConditions(optimizedJoin, join));
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
            UNIT_ASSERT(HasAllConditions(optimizedJoin, join));
        }
    }

    Y_UNIT_TEST(TwoJoinsTwoTheSameKeyInCyclicGraphTransitiveClosure) {
        auto join = Join(Join("A", "B", "A.a_a=B.b_a"), "C", "A.a_a=C.c_a,B.b_b=C.c_a");

        auto graph = MakeJoinHypergraph<TNodeSet64>(join);
        Cout << graph.String() << Endl;

        auto A = graph.GetNodesByRelNames({"A"});
        auto B = graph.GetNodesByRelNames({"B"});
        auto C = graph.GetNodesByRelNames({"C"});

        // Check that edge lhsNodes -> rhsNodes exists and has the same
        // condition as specified in expectedCondition in format:
        // "a_a=b_b,a_b=b_b" (note lhs, rhs are sorted and conditions are sorted)
        auto checkConditions = [&graph](auto lhsNodes, auto rhsNodes, const std::string& expectedCondition) {
            auto* edge = graph.FindEdgeBetween(lhsNodes, rhsNodes);
            UNIT_ASSERT(edge);

            // Extract actual join key names from hyperedge
            UNIT_ASSERT_EQUAL(edge->LeftJoinKeys.size(), edge->RightJoinKeys.size());
            std::vector<std::pair<std::string, std::string>> actualJoinKeys;
            for (ui32 i = 0; i < edge->LeftJoinKeys.size(); ++ i) {
                std::string lhs = edge->LeftJoinKeys[i].AttributeName.c_str();
                std::string rhs = edge->RightJoinKeys[i].AttributeName.c_str();
                actualJoinKeys.emplace_back(std::min(lhs, rhs), std::max(lhs, rhs));
            }

            std::sort(actualJoinKeys.begin(), actualJoinKeys.end());

            std::string actualCondition = "";
            for (auto [lhs, rhs] : actualJoinKeys) {
                if (!actualCondition.empty()) {
                    actualCondition += ",";
                }

                actualCondition += lhs + "=" + rhs;
            }

            // Ensure expected and actual join keys are the same
            UNIT_ASSERT_STRINGS_EQUAL(actualCondition, expectedCondition);
        };

        checkConditions(A, B, "a_a=b_a,a_a=b_b");
        checkConditions(B, A, "a_a=b_a,a_a=b_b");
        checkConditions(A, C, "a_a=c_a");
        checkConditions(C, A, "a_a=c_a");
        checkConditions(B, C, "b_a=c_a,b_b=c_a");
        checkConditions(C, B, "b_a=c_a,b_b=c_a");
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
            enum { CliqueSize = 0, StarSize = 0, ChainSize = 0 };
            std::cerr << "test is not running for ASAN!" << std::endl;
            return;
        #elif !defined(NDEBUG)
            enum { CliqueSize = 6, StarSize = 6, ChainSize = 6 };
        #else
            enum { CliqueSize = 10, StarSize = 14, ChainSize = 28 }; // after
            // enum { CliqueSize = 15, StarSize = 20, ChainSize = 165, }; // before
        #endif

        TVector<double> cliqueTime{};
        TVector<double> starTime{};
        TVector<double> chainTime{};

        for (size_t i = 0; i < 3; ++i) {
            srand(2281337);

            {
                auto startClique = std::chrono::high_resolution_clock::now();
                Enumerate(MakeClique(CliqueSize));
                auto endClique = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> durationClique = endClique - startClique;
                cliqueTime.push_back(durationClique.count());
            }

            {
                auto startStar = std::chrono::high_resolution_clock::now();
                Enumerate(MakeStar(StarSize));
                auto endStar = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> durationStar = endStar - startStar;
                starTime.push_back(durationStar.count());
            }

            {
                auto startChain = std::chrono::high_resolution_clock::now();
                Enumerate(MakeChain(ChainSize));
                auto endChain = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> durationChain = endChain - startChain;
                chainTime.push_back(durationChain.count());
            }
        }

        auto toString = [](const TVector<double>& v) -> TVector<TString> {
            TVector<TString> res;
            for (auto el: v) { res.push_back(ToString(el)); }
            return res;
        };

        auto mean = [](const TVector<double>& v) -> double {
            double sum = std::accumulate(v.begin(), v.end(), 0.0);
            return sum / static_cast<double>(v.size());
        };

        std::cerr << Sprintf("Time for Enumerate(MakeClique(%u)): mean: %f, values: [%s] seconds ", CliqueSize, mean(cliqueTime), JoinSeq(",", toString(cliqueTime)).c_str()) << std::endl;
        std::cerr << Sprintf("Time for Enumerate(MakeStar(%u)): mean: %f, values: [%s] seconds", StarSize, mean(starTime), JoinSeq(",", toString(starTime)).c_str()) << std::endl;
        std::cerr << Sprintf("Time for Enumerate(MakeChain(%u)): mean: %f, values [%s] seconds", ChainSize, mean(chainTime), JoinSeq(",", toString(chainTime)).c_str()) << std::endl;
    }
}
