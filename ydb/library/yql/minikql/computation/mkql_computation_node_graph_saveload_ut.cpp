#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_graph_saveload.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {
    TIntrusivePtr<IRandomProvider> CreateRandomProvider() {
        return CreateDeterministicRandomProvider(1);
    }

    TIntrusivePtr<ITimeProvider> CreateTimeProvider() {
        return CreateDeterministicTimeProvider(10000000);
    }

    TComputationNodeFactory GetAuxCallableFactory() {
        return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (callable.GetType()->GetName() == "OneYieldStream") {
                return new TExternalComputationNode(ctx.Mutables);
            }

            return GetBuiltinFactory()(callable, ctx);
        };
    }

    struct TSetup {
        TSetup(TScopedAlloc& alloc)
            : Alloc(alloc)
        {
            FunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
            RandomProvider = CreateRandomProvider();
            TimeProvider = CreateTimeProvider();

            Env.Reset(new TTypeEnvironment(Alloc));
            PgmBuilder.Reset(new TProgramBuilder(*Env, *FunctionRegistry));
        }

        THolder<IComputationGraph> BuildGraph(TRuntimeNode pgm, const std::vector<TNode*>& entryPoints = std::vector<TNode*>()) {
            Explorer.Walk(pgm.GetNode(), *Env);
            TComputationPatternOpts opts(Alloc.Ref(), *Env, GetAuxCallableFactory(),
                FunctionRegistry.Get(),
                NUdf::EValidateMode::None, NUdf::EValidatePolicy::Fail, "OFF", EGraphPerProcess::Multi);
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

    struct TStreamWithYield : public NUdf::TBoxedValue {
        TStreamWithYield(const TUnboxedValueVector& items, ui32 yieldPos, ui32 index)
            : Items(items)
            , YieldPos(yieldPos)
            , Index(index)
        {}

    private:
        TUnboxedValueVector Items;
        ui32 YieldPos;
        ui32 Index;

        ui32 GetTraverseCount() const override {
            return 0;
        }

        NUdf::TUnboxedValue Save() const override {
            return NUdf::TUnboxedValue::Zero();
        }

        bool Load2(const NUdf::TUnboxedValue& state) override {
            Y_UNUSED(state);
            return false;
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            if (Index >= Items.size()) {
                return NUdf::EFetchStatus::Finish;
            }
            if (Index == YieldPos) {
                return NUdf::EFetchStatus::Yield;
            }
            result = Items[Index++];
            return NUdf::EFetchStatus::Ok;
        }
    };
}

Y_UNIT_TEST_SUITE(TMiniKQLSaveLoadTest) {
    Y_UNIT_TEST(TestSqueezeSaveLoad) {
        TScopedAlloc alloc(__LOCATION__);

        const std::vector<ui32> items = {2, 3, 4, 5, 6, 7, 8};

        auto buildGraph = [&items] (TSetup& setup, ui32 yieldPos, ui32 startIndex) -> THolder<IComputationGraph> {
            TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

            auto dataType = pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
            auto streamType = pgmBuilder.NewStreamType(dataType);

            TCallableBuilder inStream(pgmBuilder.GetTypeEnvironment(), "OneYieldStream", streamType);
            auto streamNode = inStream.Build();

            auto pgmReturn = pgmBuilder.Squeeze(
                TRuntimeNode(streamNode, false),
                pgmBuilder.NewDataLiteral<ui32>(1),
                [&](TRuntimeNode item, TRuntimeNode state) {
                    return pgmBuilder.Add(item, state);
                },
                [](TRuntimeNode state) {
                    return state;
                },
                [](TRuntimeNode state) {
                    return state;
                });

            TUnboxedValueVector streamItems;
            for (auto item : items) {
                streamItems.push_back(NUdf::TUnboxedValuePod(item));
            }

            auto graph = setup.BuildGraph(pgmReturn, {streamNode});
            auto streamValue = NUdf::TUnboxedValuePod(new TStreamWithYield(streamItems, yieldPos, startIndex));
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(streamValue));
            return graph;
        };

        for (ui32 yieldPos = 0; yieldPos < items.size(); ++yieldPos) {
            TSetup setup1(alloc);
            auto graph1 = buildGraph(setup1, yieldPos, 0);

            auto root1 = graph1->GetValue();
            NUdf::TUnboxedValue res;
            auto status = root1.Fetch(res);
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Yield);

            TString graphState;
            SaveGraphState(&root1, 1, 0ULL, graphState);

            TSetup setup2(alloc);
            auto graph2 = buildGraph(setup2, -1, yieldPos);

            auto root2 = graph2->GetValue();
            LoadGraphState(&root2, 1, 0ULL, graphState);

            status = root2.Fetch(res);
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Ok);
            UNIT_ASSERT_EQUAL(res.Get<ui32>(), 36);

            status = root2.Fetch(res);
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Finish);
        }
    }

    Y_UNIT_TEST(TestSqueeze1SaveLoad) {
        TScopedAlloc alloc(__LOCATION__);

        const std::vector<ui32> items = {1, 2, 3, 4, 5, 6, 7, 8};

        auto buildGraph = [&items] (TSetup& setup, ui32 yieldPos, ui32 startIndex) -> THolder<IComputationGraph> {
            TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

            auto dataType = pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
            auto streamType = pgmBuilder.NewStreamType(dataType);

            TCallableBuilder inStream(pgmBuilder.GetTypeEnvironment(), "OneYieldStream", streamType);
            auto streamNode = inStream.Build();

            auto pgmReturn = pgmBuilder.Squeeze1(
                TRuntimeNode(streamNode, false),
                [](TRuntimeNode item) {
                    return item;
                },
                [&](TRuntimeNode item, TRuntimeNode state) {
                    return pgmBuilder.Add(item, state);
                },
                [](TRuntimeNode state) {
                    return state;
                },
                [](TRuntimeNode state) {
                    return state;
                });

            TUnboxedValueVector streamItems;
            for (auto item : items) {
                streamItems.push_back(NUdf::TUnboxedValuePod(item));
            }

            auto graph = setup.BuildGraph(pgmReturn, {streamNode});
            auto streamValue = NUdf::TUnboxedValuePod(new TStreamWithYield(streamItems, yieldPos, startIndex));
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(streamValue));
            return graph;
        };

        for (ui32 yieldPos = 0; yieldPos < items.size(); ++yieldPos) {
            TSetup setup1(alloc);
            auto graph1 = buildGraph(setup1, yieldPos, 0);

            auto root1 = graph1->GetValue();

            NUdf::TUnboxedValue res;
            auto status = root1.Fetch(res);
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Yield);

            TString graphState;
            SaveGraphState(&root1, 1, 0ULL, graphState);

            TSetup setup2(alloc);
            auto graph2 = buildGraph(setup2, -1, yieldPos);

            auto root2 = graph2->GetValue();
            LoadGraphState(&root2, 1, 0ULL, graphState);

            status = root2.Fetch(res);
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Ok);
            UNIT_ASSERT_EQUAL(res.Get<ui32>(), 36);

            status = root2.Fetch(res);
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Finish);
        }
    }

    Y_UNIT_TEST(TestHoppingSaveLoad) {
        TScopedAlloc alloc(__LOCATION__);

        const std::vector<std::pair<i64, ui32>> items = {
            {1, 2},
            {2, 3},
            {15, 4},
            {23, 6},
            {24, 5},
            {25, 7},
            {40, 2},
            {47, 1},
            {51, 6},
            {59, 2},
            {85, 8},
            {55, 1000},
            {200, 0}
        };

        auto buildGraph = [&items] (TSetup& setup, ui32 yieldPos, ui32 startIndex) -> THolder<IComputationGraph> {
            TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

            auto structType = pgmBuilder.NewEmptyStructType();
            structType = pgmBuilder.NewStructType(structType, "time",
                pgmBuilder.NewDataType(NUdf::TDataType<NUdf::TTimestamp>::Id));
            structType = pgmBuilder.NewStructType(structType, "sum",
                pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            auto timeIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("time");
            auto sumIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("sum");

            auto inStreamType = pgmBuilder.NewStreamType(structType);

            TCallableBuilder inStream(pgmBuilder.GetTypeEnvironment(), "OneYieldStream", inStreamType);
            auto streamNode = inStream.Build();

            ui64 hop = 10, interval = 30, delay = 20;

            auto pgmReturn = pgmBuilder.HoppingCore(
                TRuntimeNode(streamNode, false),
                [&](TRuntimeNode item) { // timeExtractor
                    return pgmBuilder.Member(item, "time");
                },
                [&](TRuntimeNode item) { // init
                    std::vector<std::pair<std::string_view, TRuntimeNode>> members;
                    members.emplace_back("sum", pgmBuilder.Member(item, "sum"));
                    return pgmBuilder.NewStruct(members);
                },
                [&](TRuntimeNode item, TRuntimeNode state) { // update
                    auto add = pgmBuilder.AggrAdd(
                        pgmBuilder.Member(item, "sum"),
                        pgmBuilder.Member(state, "sum"));
                    std::vector<std::pair<std::string_view, TRuntimeNode>> members;
                    members.emplace_back("sum", add);
                    return pgmBuilder.NewStruct(members);
                },
                [&](TRuntimeNode state) { // save
                    return pgmBuilder.Member(state, "sum");
                },
                [&](TRuntimeNode savedState) { // load
                    std::vector<std::pair<std::string_view, TRuntimeNode>> members;
                    members.emplace_back("sum", savedState);
                    return pgmBuilder.NewStruct(members);
                },
                [&](TRuntimeNode state1, TRuntimeNode state2) { // merge
                    auto add = pgmBuilder.AggrAdd(
                        pgmBuilder.Member(state1, "sum"),
                        pgmBuilder.Member(state2, "sum"));
                    std::vector<std::pair<std::string_view, TRuntimeNode>> members;
                    members.emplace_back("sum", add);
                    return pgmBuilder.NewStruct(members);
                },
                [&](TRuntimeNode state, TRuntimeNode time) { // finish
                    Y_UNUSED(time);
                    std::vector<std::pair<std::string_view, TRuntimeNode>> members;
                    members.emplace_back("sum", pgmBuilder.Member(state, "sum"));
                    return pgmBuilder.NewStruct(members);
                },
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&hop, sizeof(hop))), // hop
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&interval, sizeof(interval))), // interval
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&delay, sizeof(delay)))  // delay
            );

            auto graph = setup.BuildGraph(pgmReturn, {streamNode});

            TUnboxedValueVector streamItems;
            for (size_t i = 0; i < items.size(); ++i) {
                NUdf::TUnboxedValue* itemsPtr;
                auto structValues = graph->GetHolderFactory().CreateDirectArrayHolder(2, itemsPtr);
                itemsPtr[timeIndex] = NUdf::TUnboxedValuePod(items[i].first);
                itemsPtr[sumIndex] = NUdf::TUnboxedValuePod(items[i].second);
                streamItems.push_back(std::move(structValues));
            }

            auto streamValue = NUdf::TUnboxedValuePod(new TStreamWithYield(streamItems, yieldPos, startIndex));
            graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(streamValue));
            return graph;
        };

        for (ui32 yieldPos = 0; yieldPos < items.size(); ++yieldPos) {
            std::vector<ui32> result;

            TSetup setup1(alloc);
            auto graph1 = buildGraph(setup1, yieldPos, 0);
            auto root1 = graph1->GetValue();

            NUdf::EFetchStatus status = NUdf::EFetchStatus::Ok;
            while (status == NUdf::EFetchStatus::Ok) {
                NUdf::TUnboxedValue val;
                status = root1.Fetch(val);
                if (status == NUdf::EFetchStatus::Ok) {
                    result.push_back(val.GetElement(0).Get<ui32>());
                }
            }
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Yield);

            TString graphState;
            SaveGraphState(&root1, 1, 0ULL, graphState);

            TSetup setup2(alloc);
            auto graph2 = buildGraph(setup2, -1, yieldPos);
            auto root2 = graph2->GetValue();
            LoadGraphState(&root2, 1, 0ULL, graphState);

            status = NUdf::EFetchStatus::Ok;
            while (status == NUdf::EFetchStatus::Ok) {
                NUdf::TUnboxedValue val;
                status = root2.Fetch(val);
                if (status == NUdf::EFetchStatus::Ok) {
                    result.push_back(val.GetElement(0).Get<ui32>());
                }
            }
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Finish);

            const std::vector<ui32> resultCompare = {5, 9, 27, 22, 21, 11, 11, 8, 8, 8, 8};
            UNIT_ASSERT_EQUAL(result, resultCompare);
        }
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
