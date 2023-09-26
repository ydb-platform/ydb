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
        , Graph(InitComutationGrah(pattern))
        , Nfa(InitNfa(pattern))
    {}

    THolder<IComputationGraph> InitComutationGrah(const TRowPattern& pattern) {
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
        TCallableBuilder callableBuilder(env, "TestNfa", env.GetTypeOfVoid());
        callableBuilder.Add(pgmBuilder.Arg(matchedVarsTypeBuilder.Build()));
        for (size_t i = 0; i != VarCount; ++i) {
            callableBuilder.Add(pgmBuilder.Arg(pgmBuilder.NewDataType(NUdf::EDataSlot::Bool)));
        }
        auto testNfa = TRuntimeNode(callableBuilder.Build(), false);
        auto graph = Setup.BuildGraph(testNfa);
        return graph;
    }

    TNfa InitNfa(const TRowPattern& pattern) {
        const auto& vars = GetPatternVars(pattern);
        std::vector<TString> varVec{vars.cbegin(), vars.cend()};
        //Simulate implicit name ordering in YQL structs
        sort(varVec.begin(), varVec.end());
        THashMap<TString, size_t> varNameLookup;
        for(size_t i = 0; i != vars.size(); ++i) {
            varNameLookup[varVec[i]] = i;
        }
        const auto& transitionGraph = TNfaTransitionGraph::Create(pattern, varNameLookup);
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

} //namespace

Y_UNIT_TEST_SUITE(MatchRecognizeNfa) {
    TMemoryUsageInfo memUsage("MatchedVars");
    Y_UNIT_TEST(SingleVarAcceptNothing) {
        TScopedAlloc alloc(__LOCATION__);
        THolderFactory holderFactory(alloc.Ref(), memUsage);
        const TRowPattern pattern{{TRowPatternFactor{"A", 1 ,1, false, false}}};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines[0];
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
        const TRowPattern pattern{{TRowPatternFactor{"A", 1 ,1, false, false}}};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines[0];
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
        const TRowPattern pattern{{TRowPatternFactor{"A", 1 ,1, false, false}}};
        TNfaSetup setup{pattern};
        auto& defineA = setup.Defines[0];
        auto& ctx = setup.Ctx();
        TSparseList list;
        for (size_t i = 0; i != 100; ++i) {
            //change the value of the var
            defineA->SetValue(ctx, NUdf::TUnboxedValuePod{i % 2});
            setup.Nfa.ProcessRow(list.Append(NUdf::TUnboxedValue{}), ctx);
            UNIT_ASSERT_VALUES_EQUAL(i % 2,  setup.GetMatchedCount());
        }
    }
}

} //namespace NKikimr::NMiniKQL::NMatchRecognize
