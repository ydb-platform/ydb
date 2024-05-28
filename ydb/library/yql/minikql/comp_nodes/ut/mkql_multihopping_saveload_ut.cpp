#include "../mkql_multihopping.h"
#include "mkql_computation_node_ut.h"
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
    TComputationNodeFactory GetAuxCallableFactory(TWatermark& watermark) {
        return [&watermark](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (callable.GetType()->GetName() == "OneYieldStream") {
                return new TExternalComputationNode(ctx.Mutables);
            } else if (callable.GetType()->GetName() == "MultiHoppingCore") {
                return WrapMultiHoppingCore(callable, ctx, watermark);
            }

            return GetBuiltinFactory()(callable, ctx);
        };
    }
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

    THolder<IComputationGraph> BuildGraph(TSetup<false>& setup, const std::vector<std::tuple<ui32, i64, ui32>> items,
                                          ui32 yieldPos, ui32 startIndex, bool dataWatermarks) {
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto structType = pgmBuilder.NewEmptyStructType();
        structType = pgmBuilder.NewStructType(structType, "key",
            pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
        structType = pgmBuilder.NewStructType(structType, "time",
            pgmBuilder.NewDataType(NUdf::TDataType<NUdf::TTimestamp>::Id));
        structType = pgmBuilder.NewStructType(structType, "sum",
            pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
        auto keyIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("key");
        auto timeIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("time");
        auto sumIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("sum");

        auto inStreamType = pgmBuilder.NewStreamType(structType);

        TCallableBuilder inStream(pgmBuilder.GetTypeEnvironment(), "OneYieldStream", inStreamType);
        auto streamNode = inStream.Build();

        ui64 hop = 10, interval = 30, delay = 20;

        auto pgmReturn = pgmBuilder.MultiHoppingCore(
            TRuntimeNode(streamNode, false),
            [&](TRuntimeNode item) { // keyExtractor
                return pgmBuilder.Member(item, "key");
            },
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
            [&](TRuntimeNode key, TRuntimeNode state, TRuntimeNode time) { // finish
                Y_UNUSED(time);
                std::vector<std::pair<std::string_view, TRuntimeNode>> members;
                members.emplace_back("key", key);
                members.emplace_back("sum", pgmBuilder.Member(state, "sum"));
                return pgmBuilder.NewStruct(members);
            },
            pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&hop, sizeof(hop))), // hop
            pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&interval, sizeof(interval))), // interval
            pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&delay, sizeof(delay))),  // delay
            pgmBuilder.NewDataLiteral<bool>(dataWatermarks),  // dataWatermarks
            pgmBuilder.NewDataLiteral<bool>(false)
        );

        auto graph = setup.BuildGraph(pgmReturn, {streamNode});

        TUnboxedValueVector streamItems;
        for (size_t i = 0; i < items.size(); ++i) {
            NUdf::TUnboxedValue* itemsPtr;
            auto structValues = graph->GetHolderFactory().CreateDirectArrayHolder(3, itemsPtr);
            itemsPtr[keyIndex] = NUdf::TUnboxedValuePod(std::get<0>(items[i]));
            itemsPtr[timeIndex] = NUdf::TUnboxedValuePod(std::get<1>(items[i]));
            itemsPtr[sumIndex] = NUdf::TUnboxedValuePod(std::get<2>(items[i]));
            streamItems.push_back(std::move(structValues));
        }

        auto streamValue = NUdf::TUnboxedValuePod(new TStreamWithYield(streamItems, yieldPos, startIndex));
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(streamValue));
        return graph;
    }
}

Y_UNIT_TEST_SUITE(TMiniKQLMultiHoppingSaveLoadTest) {
    void TestWithSaveLoadImpl(
        const std::vector<std::tuple<ui32, i64, ui32>> input,
        const std::vector<std::tuple<ui32, ui32>> expected,
        bool withTraverse,
        bool dataWatermarks)
    {
        TWatermark watermark;
        for (ui32 yieldPos = 0; yieldPos < input.size(); ++yieldPos) {
            std::vector<std::tuple<ui32, ui32>> result;

            TSetup<false> setup1(GetAuxCallableFactory(watermark));
            auto graph1 = BuildGraph(setup1, input, yieldPos, 0, dataWatermarks);
            auto root1 = graph1->GetValue();

            NUdf::EFetchStatus status = NUdf::EFetchStatus::Ok;
            while (status == NUdf::EFetchStatus::Ok) {
                NUdf::TUnboxedValue val;
                status = root1.Fetch(val);
                if (status == NUdf::EFetchStatus::Ok) {
                    result.emplace_back(val.GetElement(0).Get<ui32>(), val.GetElement(1).Get<ui32>());
                }
            }
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Yield);

            TString graphState;
            if (withTraverse) {
                SaveGraphState(&root1, 1, 0ULL, graphState);
            } else {
                graphState = graph1->SaveGraphState();
            }

            TSetup<false> setup2(GetAuxCallableFactory(watermark));
            auto graph2 = BuildGraph(setup2, input, -1, yieldPos, dataWatermarks);
            NUdf::TUnboxedValue root2;
            if (withTraverse) {
                root2 = graph2->GetValue();
                LoadGraphState(&root2, 1, 0ULL, graphState);
            } else {
                graph2->LoadGraphState(graphState);
                root2 = graph2->GetValue();
            }

            status = NUdf::EFetchStatus::Ok;
            while (status == NUdf::EFetchStatus::Ok) {
                NUdf::TUnboxedValue val;
                status = root2.Fetch(val);
                if (status == NUdf::EFetchStatus::Ok) {
                    result.emplace_back(val.GetElement(0).Get<ui32>(), val.GetElement(1).Get<ui32>());
                }
            }
            UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Finish);

            auto sortedExpected = expected;
            std::sort(result.begin(), result.end());
            std::sort(sortedExpected.begin(), sortedExpected.end());
            UNIT_ASSERT_EQUAL(result, sortedExpected);
        }
    }

    const std::vector<std::tuple<ui32, i64, ui32>> input1 = {
        // Group; Time; Value
        {2, 1, 2},
        {1, 1, 2},
        {2, 2, 3},
        {1, 2, 3},
        {2, 15, 4},
        {1, 15, 4},
        {2, 23, 6},
        {1, 23, 6},
        {2, 24, 5},
        {1, 24, 5},
        {2, 25, 7},
        {1, 25, 7},
        {2, 40, 2},
        {1, 40, 2},
        {2, 47, 1},
        {1, 47, 1},
        {2, 51, 6},
        {1, 51, 6},
        {2, 59, 2},
        {1, 59, 2},
        {2, 85, 8},
        {1, 85, 8}
    };

    const std::vector<std::tuple<ui32, ui32>> expected = {
        {1, 8}, {1, 8}, {1, 8}, {1, 8},
        {1, 11}, {1, 11}, {1, 21}, {1, 22},
        {1, 27},
        {2, 8}, {2, 8}, {2, 8}, {2, 8},
        {2, 11}, {2, 11}, {2, 21},
        {2, 22}, {2, 27}};

    Y_UNIT_TEST(Test1) {
        TestWithSaveLoadImpl(input1, expected, true, false);
    }

    Y_UNIT_TEST(Test2) {
        TestWithSaveLoadImpl(input1, expected, false, false);
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
