#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

#include <vector>
#include <utility>
#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

namespace {
struct TSetup {
    TSetup(TScopedAlloc& alloc)
        : Alloc(alloc)
    {
        FunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        RandomProvider = CreateDeterministicRandomProvider(1);
        TimeProvider = CreateDeterministicTimeProvider(10000000);

        Env.Reset(new TTypeEnvironment(Alloc));
        PgmBuilder.Reset(new TProgramBuilder(*Env, *FunctionRegistry));
    }

    THolder<IComputationGraph> BuildGraph(TRuntimeNode pgm, const std::vector<TNode*>& entryPoints = std::vector<TNode*>()) {
        Explorer.Walk(pgm.GetNode(), *Env);
        TComputationPatternOpts opts(Alloc.Ref(), *Env, GetBuiltinFactory(),
            FunctionRegistry.Get(),
            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi);
        Pattern = MakeComputationPattern(Explorer, pgm, entryPoints, opts);
        TComputationOptsFull compOpts = opts.ToComputationOptions(*RandomProvider, *TimeProvider);
        return Pattern->Clone(compOpts);
    }

    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;

    TScopedAlloc& Alloc;
    THolder<TTypeEnvironment> Env;
    THolder<TProgramBuilder> PgmBuilder;

    TExploringNodeVisitor Explorer;
    IComputationPattern::TPtr Pattern;
};
}

Y_UNIT_TEST_SUITE(TestCompactMultiDict) {
    Y_UNIT_TEST(TestIterate) {
        TScopedAlloc alloc(__LOCATION__);

        TSetup setup(alloc);

        const std::vector<std::pair<ui32, std::vector<ui32>>> items = {{1, {1, 2}}, {2, {1}}, {3, {0}}, {6, {1, 7}}};

        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;
        TVector<TRuntimeNode> rItems;
        for (auto& [k, vv]: items) {
            for (auto& v: vv) {
                rItems.push_back(pgmBuilder.NewTuple({pgmBuilder.NewDataLiteral<ui32>(k), pgmBuilder.NewDataLiteral<ui32>(v)}));
            }
        }
        auto ui32Type = pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
        auto list = pgmBuilder.NewList(pgmBuilder.NewTupleType({ui32Type, ui32Type}), rItems);

        auto dict = pgmBuilder.ToHashedDict(list, /*all*/true,
            [&pgmBuilder](TRuntimeNode item) { return pgmBuilder.Nth(item, 0); },
            [&pgmBuilder](TRuntimeNode item) { return pgmBuilder.Nth(item, 1); },
            /*isCompact*/true,
            items.size());

        auto graph = setup.BuildGraph(dict, {});
        NUdf::TUnboxedValue res = graph->GetValue();

        std::vector<ui32> keyVals;
        for (NUdf::TUnboxedValue keys = res.GetKeysIterator(), v; keys.Next(v);) {
            keyVals.push_back(v.Get<ui32>());
        }
        UNIT_ASSERT_VALUES_EQUAL(keyVals.size(), items.size());
        std::sort(keyVals.begin(), keyVals.end());
        UNIT_ASSERT(
            std::equal(keyVals.begin(), keyVals.end(), items.begin(),
                [](ui32 l, const std::pair<ui32, std::vector<ui32>>& r) { return l == r.first; }
            )
        );

        std::vector<std::vector<ui32>> origPayloads;
        for (auto& [k, vv]: items) {
            origPayloads.push_back(vv);
            std::sort(origPayloads.back().begin(), origPayloads.back().end());
        }
        std::sort(origPayloads.begin(), origPayloads.end());

        std::vector<std::vector<ui32>> payloadVals;
        for (NUdf::TUnboxedValue payloads = res.GetPayloadsIterator(), v; payloads.Next(v);) {
            payloadVals.emplace_back();
            for (NUdf::TUnboxedValue i = v.GetListIterator(), p; i.Next(p);) {
                payloadVals.back().push_back(p.Get<ui32>());
            }
            std::sort(payloadVals.back().begin(), payloadVals.back().end());
        }
        std::sort(payloadVals.begin(), payloadVals.end());
        UNIT_ASSERT_VALUES_EQUAL(origPayloads, payloadVals);

        std::vector<std::pair<ui32, std::vector<ui32>>> vals;
        for (NUdf::TUnboxedValue values = res.GetDictIterator(), k, payloads; values.NextPair(k, payloads);) {
            vals.emplace_back(k.Get<ui32>(), std::vector<ui32>{});
            for (NUdf::TUnboxedValue i = payloads.GetListIterator(), p; i.Next(p);) {
                vals.back().second.push_back(p.Get<ui32>());
            }
            std::sort(vals.back().second.begin(), vals.back().second.end());
        }
        UNIT_ASSERT_VALUES_EQUAL(items, vals);
    }
}
}
}
