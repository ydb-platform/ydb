#include "../mkql_match_recognize_nfa.h"
#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/core/sql_types/match_recognize.h>
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
        return TNfa(transitionGraph, MatchedVars, defines);
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
                    UNIT_ASSERT_UNEQUAL(tr->size(), 1);
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
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        // "A{4, 6}"
        const TRowPattern pattern{{TRowPatternFactor{"A", 4, 6, false, false, false}}};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        TSparseList list;
        for (size_t i = 0; i != 100; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            if (i < 3) {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.GetMatchedCount());
            } else if (i <= 5) {
                UNIT_ASSERT_VALUES_EQUAL(i - 2, setup.GetMatchedCount());
            } else { //expect 3 matches
                THashSet<size_t> expectedFrom{i - 5, i - 4, i - 3};
                for (size_t c = 0; c != 3; ++c) {
                    auto m = setup.Nfa.GetMatched();
                    UNIT_ASSERT(m);
                    UNIT_ASSERT_VALUES_EQUAL(1, m->size()); //single var
                    auto v = m->at(0);
                    UNIT_ASSERT_VALUES_EQUAL(1, v.size()); //single range
                    expectedFrom.erase(v[0].From());
                    UNIT_ASSERT_VALUES_EQUAL(i, v[0].To());
                }
                UNIT_ASSERT_VALUES_EQUAL(0, expectedFrom.size());
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
    //Pattern: Any*
    //Input any event matches Any
    Y_UNIT_TEST(AnyStar) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        //"Any*"
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1000000000, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        TSparseList list;
        const size_t inputSize = 100;
        size_t totalMatches = 0;
        for (size_t i = 0; i != inputSize; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            const auto matches = setup.GetMatchedCount();
            totalMatches += matches;
            UNIT_ASSERT_VALUES_EQUAL(i + 1, matches);
            UNIT_ASSERT_VALUES_EQUAL(i + 1, setup.Nfa.GetActiveStatesCount());
        }
        UNIT_ASSERT_VALUES_EQUAL(inputSize * (inputSize + 1) / 2, totalMatches);
    }

    Y_UNIT_TEST(UnusedVarIgnored) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{
            TRowPatternFactor{"ANY", 1, 100, false, false, true},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        defineA->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        TSparseList list;
        const size_t inputSize = 100;
        for (size_t i = 0; i != inputSize; ++i) {
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_EQUAL(0, setup.Nfa.GetMatched().value()[0].size());
        }
    }

    //Pattern: A*
    //Input: intermittent series events that match A
    Y_UNIT_TEST(AStar) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        //"A*"
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1000000000, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& ctx = setup.Ctx();
        TSparseList list;
        const size_t inputSize = 100;
        const size_t seriesPeriod = 10;
        const size_t seriesLength =  3;
        size_t totalMatches = 0;
        //Intermittent series of matched events
        for (size_t i = 0; i != inputSize; ++i) {
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i % seriesPeriod < seriesLength});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            const auto matches = setup.GetMatchedCount();
            totalMatches += matches;
            if (i % seriesPeriod < seriesLength) {
                UNIT_ASSERT_VALUES_EQUAL(i % seriesPeriod + 1, matches);
                UNIT_ASSERT(setup.Nfa.GetActiveStatesCount() <= 2 * seriesLength);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(0, matches);
                UNIT_ASSERT_VALUES_EQUAL(0, setup.Nfa.GetActiveStatesCount());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(
            (inputSize / seriesPeriod) * (seriesLength * (seriesLength + 1)) / 2,
            totalMatches)
        ;
    }

    //Pattern: A ANY* B
    //Input: x x x A x x x B x x x
    Y_UNIT_TEST(A_AnyStar_B_SingleMatch) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        //"A ANY* B"
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, 1000000000, false, false, false},
            TRowPatternFactor{"B", 1, 1, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& defineAny = setup.Defines.at(1);
        auto& defineB = setup.Defines.at(2);
        auto& ctx = setup.Ctx();
        TSparseList list;
        defineAny->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        const size_t size = 100;
        //Number of active states doesn't depend on input size for a single match
        for (size_t i = 0; i != size; ++i) {
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i == 10});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{i == 60});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(i == 60 ? 1 : 0, setup.GetMatchedCount());
            UNIT_ASSERT(setup.Nfa.GetActiveStatesCount() <= 3);
        }
    }

    //Pattern: A ANY* B
    //Input: x x x A x x x B x x x A x x x B ...
    Y_UNIT_TEST(A_AnyStar_B_Series) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        //"A ANY* B"
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, 1000000000, false, false, false},
            TRowPatternFactor{"B", 1, 1, false, false, false},
        }};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines.at(0);
        auto& defineAny = setup.Defines.at(1);
        auto& defineB = setup.Defines.at(2);
        auto& ctx = setup.Ctx();
        TSparseList list;
        defineAny->SetValue(ctx, NUdf::TUnboxedValuePod{true});
        const size_t inputSize = 100;
        const size_t seriesPeriod = 10;
        const size_t offsetA = 2;
        const size_t offsetB = 7;
        size_t totalMatches = 0;
        for (size_t i = 0; i != inputSize; ++i) {
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i % seriesPeriod == offsetA});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{i % seriesPeriod == offsetB});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            const auto expectedMatches = (i / seriesPeriod + 1);
            if (i % seriesPeriod == offsetB) {
                //Any matched A is a part for every subsequent B, because B matches ANY
                const auto matches = setup.GetMatchedCount();
                totalMatches += expectedMatches;
                UNIT_ASSERT_VALUES_EQUAL(expectedMatches, matches);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(0, setup.GetMatchedCount());
                const auto expectedStates = (i / seriesPeriod + 1) * 2 + 2;
                UNIT_ASSERT(setup.Nfa.GetActiveStatesCount() <= expectedStates);
            }
        }
        const auto seriesCount = inputSize / seriesPeriod;
        UNIT_ASSERT_VALUES_EQUAL(
                seriesCount * (seriesCount + 1) / 2,
                totalMatches
        );
    }

    //Pattern: A ANY* B ANY* C
    //Input: x x x A x x x B x x x C x x x
    Y_UNIT_TEST(A_AnyStar_B_AnyStar_C_SingleMatch) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        //"A ANY* B ANY* C"
        const TRowPattern pattern{{
            TRowPatternFactor{"A", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, 1000000000, false, false, false},
            TRowPatternFactor{"B", 1, 1, false, false, false},
            TRowPatternFactor{"ANY", 1, 1000000000, false, false, false},
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
        const size_t inputSize = 100;
        const size_t seriesPeriod = 100;
        const size_t offsetA = 20;
        const size_t offsetB = 50;
        const size_t offsetC = 80;
        for (size_t i = 0; i != inputSize; ++i) {
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i % seriesPeriod == offsetA});
            defineB->SetValue(ctx, NUdf::TUnboxedValuePod{i % seriesPeriod == offsetB});
            defineC->SetValue(ctx, NUdf::TUnboxedValuePod{i % seriesPeriod == offsetC});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            const auto matches = setup.GetMatchedCount();
            UNIT_ASSERT_VALUES_EQUAL(i == 80 ? 1: 0, matches);
        }
    }
}

} //namespace NKikimr::NMiniKQL::NMatchRecognize
