#include "../mkql_match_recognize_nfa.h"
#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/core/sql_types/match_recognize.h>
#include <library/cpp/testing/unittest/registar.h>
#include <vector>
#include <algorithm>

namespace NKikimr::NMiniKQL::NMatchRecognize {
namespace {

struct TNfaSetup {
    TNfaSetup(const TRowPattern& pattern)
        : Setup(GetAuxCallableFactory())
        , Graph(InitComputationGrah(pattern))
        , Nfa(InitNfa(pattern))
    {}

    THolder<IComputationGraph> InitComputationGrah(const TRowPattern& pattern) {
        auto& env = *Setup.Env;
        TStructTypeBuilder indexRangeTypeBuilder(env);
        indexRangeTypeBuilder.Add("From", TDataType::Create(NUdf::TDataType<ui64>::Id, env));
        indexRangeTypeBuilder.Add("To", TDataType::Create(NUdf::TDataType<ui64>::Id, env));
        const auto& rangeList = TListType::Create(indexRangeTypeBuilder.Build(), env);
        const auto& vars = GetPatternVars(pattern);
        VarCount = vars.size();
        TStructTypeBuilder matchedVarsTypeBuilder(env);
        for (const auto& var: vars) {
            matchedVarsTypeBuilder.Add(var, rangeList);
        }

        auto& pgmBuilder = *Setup.PgmBuilder;
        TCallableBuilder callableBuilder(env, "TestNfa", env.GetTypeOfVoidLazy());
        callableBuilder.Add(pgmBuilder.Arg(matchedVarsTypeBuilder.Build()));
        for (size_t i = 0; i != VarCount; ++i) {
            callableBuilder.Add(pgmBuilder.Arg(pgmBuilder.NewDataType(NUdf::EDataSlot::Bool)));
        }
        auto testNfa = TRuntimeNode(callableBuilder.Build(), false);
        auto graph = Setup.BuildGraph(testNfa);
        return graph;
    }

    static THashMap<TString, size_t> BuildVarLookup(const TRowPattern& pattern) {
        const auto& vars = GetPatternVars(pattern);
        std::vector<TString> varVec{vars.cbegin(), vars.cend()};
        //Simulate implicit name ordering in YQL structs
        sort(varVec.begin(), varVec.end());
        THashMap<TString, size_t> varNameLookup;
        for(size_t i = 0; i != vars.size(); ++i) {
            varNameLookup[varVec[i]] = i;
        }
        return varNameLookup;
    }

    TNfa InitNfa(const TRowPattern& pattern) {
        const auto& transitionGraph = TNfaTransitionGraphBuilder::Create(pattern, BuildVarLookup(pattern));
        TComputationNodePtrVector defines;
        defines.reserve(Defines.size());
        for (auto& d: Defines) {
            defines.push_back(d);
        }
        return TNfa(transitionGraph, MatchedVars, defines, TAfterMatchSkipTo{EAfterMatchSkipTo::PastLastRow, ""});
    }

    TComputationNodeFactory GetAuxCallableFactory() {
        return [this](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (callable.GetType()->GetName() == "TestNfa") {
                MatchedVars = static_cast<IComputationExternalNode *>(LocateNode(ctx.NodeLocator, *callable.GetInput(0).GetNode()));
                for (size_t i = 0; i != VarCount; ++i) {
                    auto d =  callable.GetInput(1 + i).GetNode();
                    const auto& nn = LocateNode(ctx.NodeLocator, *d);
                    Defines.push_back(static_cast<IComputationExternalNode *>(nn));
                }
                return new TExternalComputationNode(ctx.Mutables);
            }
            return GetBuiltinFactory()(callable, ctx);
        };
    }

    size_t GetMatchedCount() {
        size_t result = 0;
        while (Nfa.GetMatched()) {
            ++result;
        }
        return result;
    }

    TComputationContext& Ctx() const {
        return Graph->GetContext();
    }
    TSetup<false> Setup;
    IComputationExternalNode* MatchedVars;
    TComputationExternalNodePtrVector Defines;
    size_t VarCount;
    THolder<IComputationGraph> Graph;
    TNfa Nfa;
};

static TVector<size_t> CountNonEpsilonInputs(const TNfaTransitionGraph& graph) {
    TVector<size_t> nonEpsIns(graph.Transitions.size());
    for (size_t node = 0; node != graph.Transitions.size(); node++) {
        if (!std::holds_alternative<TEpsilonTransitions>(graph.Transitions[node])) {
            std::visit(TNfaTransitionDestinationVisitor([&](size_t toNode){
                nonEpsIns[toNode]++;
                return 0;
            }), graph.Transitions[node]);
        }
    }
    return nonEpsIns;
}

static TVector<size_t> CountNonEpsilonOutputs(const TNfaTransitionGraph& graph) {
    TVector<size_t> nonEpsOuts(graph.Transitions.size());
    nonEpsOuts.resize(graph.Transitions.size());
    for (size_t node = 0; node < graph.Transitions.size(); node++) {
        if (!std::holds_alternative<TEpsilonTransitions>(graph.Transitions[node])) {
            nonEpsOuts[node]++;
        }
    }
    return nonEpsOuts;
}

} //namespace

Y_UNIT_TEST_SUITE(MatchRecognizeNfa) {

    Y_UNIT_TEST(OutputStateHasNoOutputEdges) {
        TScopedAlloc alloc(__LOCATION__);
        const TRowPattern pattern{{TRowPatternFactor{"A", 1, 1, false, false, false}}};
        const auto transitionGraph = TNfaTransitionGraphBuilder::Create(pattern, {{"A", 0}});
        const auto& output = transitionGraph->Transitions.at(transitionGraph->Output);
        UNIT_ASSERT(std::get_if<TVoidTransition>(&output));
    }
    Y_UNIT_TEST(EpsilonChainsEliminated) {
        TScopedAlloc alloc(__LOCATION__);
        const TRowPattern pattern{
            {
                TRowPatternFactor{"A", 1, 1, false, false, false},
                TRowPatternFactor{"B", 1, 100, false, false, false},
                TRowPatternFactor{
                    TRowPattern{
                        {TRowPatternFactor{"C", 1, 1, false, false, false}},
                        {TRowPatternFactor{"D", 1, 1, false, false, false}}
                    },
                    1, 1, false, false, false
                }
            },
            {
                TRowPatternFactor{
                    TRowPattern{{
                                    TRowPatternFactor{"E", 1, 1, false, false, false},
                                    TRowPatternFactor{"F", 1, 100, false, false, false},
                                }},
                    2, 100, false, false, false
                },
                TRowPatternFactor{"G", 1, 1, false, false, false}
            }
        };
        const auto graph = TNfaTransitionGraphBuilder::Create(pattern, TNfaSetup::BuildVarLookup(pattern));
        auto nonEpsIns = CountNonEpsilonInputs(*graph);
        auto nonEpsOuts = CountNonEpsilonOutputs(*graph);
        for(size_t node = 0; node < nonEpsIns.size(); node++) {
            if (node == graph->Input) {
                continue;
            }
            if (node == graph->Output) {
                continue;
            }
            UNIT_ASSERT_GT(nonEpsIns[node] + nonEpsOuts[node], 0);
        }
    }
    Y_UNIT_TEST(SingleEpsilonsEliminated) {
        TScopedAlloc alloc(__LOCATION__);
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"B", 1, 1, false, false, false},
        }};
        const auto graph = TNfaTransitionGraphBuilder::Create(pattern, TNfaSetup::BuildVarLookup(pattern));
        for (size_t node = 0; node != graph->Transitions.size(); node++) {
            if (std::holds_alternative<TEpsilonTransitions>(graph->Transitions[node])) {
                continue;
            }
            std::visit(TNfaTransitionDestinationVisitor([&](size_t toNode) -> size_t {
                if (auto *tr = std::get_if<TEpsilonTransitions>(&graph->Transitions[toNode])) {
                    UNIT_ASSERT_UNEQUAL(tr->To.size(), 1);
                }
                return toNode;
            }), graph->Transitions[node]);
        }
    }


    //Tests for NFA-based engine for MATCH_RECOGNIZE
    //In the full implementation pattern variables are calculated as lambda predicates on input partition
    //For the sake of simplification, in these tests predicates are replaced with bool literal values,
    //that can be set explicitly in the tests body. So, the values of input rows are irrelevant and not used.
    TMemoryUsageInfo memUsage("MatchedVars");
    Y_UNIT_TEST(SingleVarAcceptNothing) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{TRowPatternFactor{"A", 1, 1, false, false, false}}};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{false});
        TSparseList list;
        for (size_t i = 0; i != 100; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(0, setup.GetMatchedCount());
        }
    }
    Y_UNIT_TEST(SingleVarAcceptEveryRow) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{TRowPatternFactor{"A", 1, 1, false, false, false}}};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        TSparseList list;
        for (size_t i = 0; i != 100; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(1, setup.GetMatchedCount());
        }
    }
    Y_UNIT_TEST(SingleAlternatedVarAcceptEven) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{TRowPatternFactor{"A", 1, 1, false, false, false}}};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        TSparseList list;
        for (size_t i = 0; i != 100; ++i) {
            //change the value of the var
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i % 2});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(i % 2,  setup.GetMatchedCount());
        }
    }
    Y_UNIT_TEST(SingleVarRepeatedAndCheckRanges) {
        constexpr size_t inputSize = 100;
        constexpr size_t seriesLength = 6;
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        // "A{1, 6}"
        const TRowPattern pattern{
            {TRowPatternFactor{"A", 1, seriesLength, false, false, false}}
        };
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        TSparseList list;
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        for (size_t i = 0; i < inputSize; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            if (i + 1 < seriesLength) {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.GetMatchedCount());
            } else {
                TVector<size_t> expectedTo(seriesLength);
                Iota(expectedTo.begin(), expectedTo.end(), i - seriesLength + 1);
                for (size_t matchCount = 0; matchCount < seriesLength; ++matchCount) {
                    auto match = setup.Nfa.GetMatched();
                    UNIT_ASSERT_C(match, i);
                    auto vars = match->Vars;
                    UNIT_ASSERT_VALUES_EQUAL_C(1, vars.size(), i);
                    auto var = vars[0];
                    UNIT_ASSERT_VALUES_EQUAL_C(1, var.size(), i);
                    if (auto expectedToIter = Find(expectedTo, var[0].To());
                        expectedToIter != expectedTo.end()) {
                        expectedTo.erase(expectedToIter);
                    }
                    UNIT_ASSERT_VALUES_EQUAL_C(i - seriesLength + 1, var[0].From(), i);
                }
                UNIT_ASSERT_VALUES_EQUAL_C(0, expectedTo.size(), i);
            }
        }
    }
    Y_UNIT_TEST(SingleVarDuplicated) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        // "A A A"
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"A", 1, 1, false, false, false}
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        TSparseList list;
        for (size_t i = 0; i != 100; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            const auto expected = (i < 2) ? 0 : 1;
            UNIT_ASSERT_VALUES_EQUAL(expected, setup.GetMatchedCount()); //expect matches starting with the 2nd row
        }
    }

    Y_UNIT_TEST(TwoSeqAlternatedVarsAcceptEven) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        //"A B"
        const TRowPattern pattern{{
                TRowPatternFactor{"A", 1, 1, false, false, false},
                TRowPatternFactor{"B", 1, 1, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& defineB = setup.Defines.at(1);
        auto& ctx = setup.Ctx();
        TSparseList list;
        for (size_t i = 0; i != 100; ++i) {
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i % 2 == 0});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{i % 2 == 1});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(i % 2, setup.GetMatchedCount());
        }
    }

    Y_UNIT_TEST(TwoORedAlternatedVarsAcceptEvery) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        //"A | B"
        const TRowPattern pattern{
              {TRowPatternFactor{"A", 1, 1, false, false, false}},
              {TRowPatternFactor{"B", 1, 1, false, false, false}},
        };
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& defineB = setup.Defines.at(1);
        auto& ctx = setup.Ctx();
        TSparseList list;
        for (size_t i = 0; i != 100; ++i) {
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i % 2 == 0});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{i % 2 == 1});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(1, setup.GetMatchedCount());
        }
    }

    //Match every contiguous subset (n*n of input size)
    //Pattern: Any{1,100}
    //Input: any event matches Any
    Y_UNIT_TEST(AnyStar) {
        constexpr size_t inputSize = 100;
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{
            TRowPatternFactor{"ANY", 1, inputSize, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        TSparseList list;
        for (size_t i = 0; i < inputSize; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            if (i + 1 == inputSize) {
                UNIT_ASSERT_VALUES_EQUAL((i + 1) * (i + 4) / 2 - 1, setup.Nfa.GetActiveStatesCount());
                UNIT_ASSERT_VALUES_EQUAL(inputSize, setup.GetMatchedCount());
            } else {
                UNIT_ASSERT_VALUES_EQUAL((i + 1) * (i + 4) / 2, setup.Nfa.GetActiveStatesCount());
                UNIT_ASSERT_VALUES_EQUAL(0, setup.GetMatchedCount());
            }
        }
    }
    Y_UNIT_TEST(AnyStarUnused) {
        constexpr size_t inputSize = 100;
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{
            TRowPatternFactor{"ANY", 1, inputSize, false, false, true},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        TSparseList list;
        for (size_t i = 0; i < inputSize; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            if (i + 1 == inputSize) {
                UNIT_ASSERT_VALUES_EQUAL((i + 1) * (i + 4) / 2 - 1, setup.Nfa.GetActiveStatesCount());
                UNIT_ASSERT_VALUES_EQUAL(inputSize, setup.GetMatchedCount());
            } else {
                UNIT_ASSERT_VALUES_EQUAL((i + 1) * (i + 4) / 2, setup.Nfa.GetActiveStatesCount());
                UNIT_ASSERT_VALUES_EQUAL(0, setup.GetMatchedCount());
            }
        }
    }

    //Pattern: A*
    //Input: intermittent series events that match A
    Y_UNIT_TEST(AStar) {
        constexpr size_t inputSize = 100;
        constexpr size_t seriesPeriod = 10;
        constexpr size_t seriesLength = 3;
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, seriesLength, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        TSparseList list;
        for (size_t i = 0; i < inputSize; ++i) {
            auto seriesIndex = i % seriesPeriod;
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{seriesIndex < seriesLength});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            if (seriesIndex + 1 == seriesLength) {
                UNIT_ASSERT_VALUES_EQUAL(seriesLength, setup.GetMatchedCount());
                UNIT_ASSERT_VALUES_EQUAL(seriesIndex * (seriesIndex + 3) / 2, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex < seriesLength) {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.GetMatchedCount());
                UNIT_ASSERT_VALUES_EQUAL((seriesIndex + 1) * (seriesIndex + 4) / 2, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex == seriesLength) {
                UNIT_ASSERT_VALUES_EQUAL((seriesIndex - 1) * seriesIndex / 2, setup.GetMatchedCount());
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.GetMatchedCount());
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            }
        }
    }

    //Pattern: A ANY* B
    //Input: x x x A x x x B x x x
    Y_UNIT_TEST(A_AnyStar_B_SingleMatch) {
        constexpr size_t inputSize = 100;
        constexpr size_t offsetA = 24;
        constexpr size_t offsetB = 75;
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, offsetB - offsetA - 1, false, false, false},
            TRowPatternFactor{"B", 1, 1, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& defineAny = setup.Defines.at(1);
        auto& defineB = setup.Defines.at(2);
        auto& ctx = setup.Ctx();
        TSparseList list;
        defineAny->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        for (size_t i = 0; i < inputSize; ++i) {
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i == offsetA});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{i == offsetB});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(i == offsetB ? 1 : 0, setup.GetMatchedCount());
            if (i < offsetA) {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            } else if (i == offsetA) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else if (i + 1 < offsetB) {
                UNIT_ASSERT_VALUES_EQUAL(2, setup.Nfa.GetActiveStatesCount());
            } else if (i + 1 == offsetB) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            }
        }
    }

    //Pattern: A ANY* B
    //Input: x x x A x x x B x x x A x x x B ...
    Y_UNIT_TEST(A_AnyStar_B_Series) {
        constexpr size_t inputSize = 100;
        constexpr size_t seriesPeriod = 10;
        constexpr size_t offsetA = 2;
        constexpr size_t offsetB = 7;
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, offsetB - offsetA - 1, false, false, false},
            TRowPatternFactor{"B", 1, 1, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& defineAny = setup.Defines.at(1);
        auto& defineB = setup.Defines.at(2);
        auto& ctx = setup.Ctx();
        TSparseList list;
        defineAny->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        for (size_t i = 0; i < inputSize; ++i) {
            auto seriesIndex = i % seriesPeriod;
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{seriesIndex == offsetA});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{seriesIndex == offsetB});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(seriesIndex == offsetB ? 1 : 0, setup.GetMatchedCount());
            if (seriesIndex < offsetA) {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex == offsetA) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex + 1 < offsetB) {
                UNIT_ASSERT_VALUES_EQUAL(2, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex + 1 == offsetB) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            }
        }
    }

    //Pattern: A ANY* B ANY* C
    //Input: x x x A x x x B x x x C x x x
    Y_UNIT_TEST(A_AnyStar_B_AnyStar_C_SingleMatch) {
        constexpr size_t inputSize = 100;
        constexpr size_t offsetA = 20;
        constexpr size_t offsetB = 50;
        constexpr size_t offsetC = 80;
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, offsetB - offsetA - 1, false, false, false},
            TRowPatternFactor{"B", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, offsetC - offsetB - 1, false, false, false},
            TRowPatternFactor{"C", 1, 1, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& defineAny = setup.Defines.at(1);
        auto& defineB = setup.Defines.at(2);
        auto& defineC = setup.Defines.at(3);
        auto& ctx = setup.Ctx();
        TSparseList list;
        defineAny->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        for (size_t i = 0; i < inputSize; ++i) {
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i == offsetA});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{i == offsetB});
            defineC->SetValue(ctx, NUdf::TUnboxedValuePod{i == offsetC});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(i == offsetC ? 1 : 0, setup.GetMatchedCount());
            if (i < offsetA) {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            } else if (i == offsetA) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else if (i + 1 < offsetB) {
                UNIT_ASSERT_VALUES_EQUAL(2, setup.Nfa.GetActiveStatesCount());
            } else if (i + 1 == offsetB || i == offsetB) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else if (i + 1 < offsetC) {
                UNIT_ASSERT_VALUES_EQUAL(2, setup.Nfa.GetActiveStatesCount());
            } else if (i + 1 == offsetC) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            }
        }
    }

    //Pattern: A ANY* B ANY* C
    //Input: x x x A x x x B x x x C x x x A x x x B x x x C x x x
    Y_UNIT_TEST(A_AnyStar_B_AnyStar_C_Series) {
        constexpr size_t inputSize = 100;
        constexpr size_t seriesPeriod = 10;
        constexpr size_t offsetA = 2;
        constexpr size_t offsetB = 5;
        constexpr size_t offsetC = 8;
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, offsetB - offsetA - 1, false, false, false},
            TRowPatternFactor{"B", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, offsetC - offsetB - 1, false, false, false},
            TRowPatternFactor{"C", 1, 1, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& defineAny = setup.Defines.at(1);
        auto& defineB = setup.Defines.at(2);
        auto& defineC = setup.Defines.at(3);
        auto& ctx = setup.Ctx();
        TSparseList list;
        defineAny->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        for (size_t i = 0; i < inputSize; ++i) {
            auto seriesIndex = i % seriesPeriod;
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{seriesIndex == offsetA});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{seriesIndex == offsetB});
            defineC->SetValue(ctx, NUdf::TUnboxedValuePod{seriesIndex == offsetC});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(seriesIndex == offsetC ? 1 : 0, setup.GetMatchedCount());
            if (seriesIndex < offsetA) {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex == offsetA) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex + 1 < offsetB) {
                UNIT_ASSERT_VALUES_EQUAL(2, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex + 1 == offsetB || seriesIndex == offsetB) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex + 1 < offsetC) {
                UNIT_ASSERT_VALUES_EQUAL(2, setup.Nfa.GetActiveStatesCount());
            } else if (seriesIndex + 1 == offsetC) {
                UNIT_ASSERT_VALUES_EQUAL(1, setup.Nfa.GetActiveStatesCount());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            }
        }
    }
}

} //namespace NKikimr::NMiniKQL::NMatchRecognize
