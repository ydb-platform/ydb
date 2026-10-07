#include <yql/essentials/minikql/comp_nodes/mkql_multihopping.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/core/sql_types/hopping.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_graph_saveload.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NKikimr::NMiniKQL {

namespace {
using TWatermarksPattern = std::vector<std::tuple<ui32, TInstant>>;
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
struct TStreamWithYield: public NUdf::TBoxedValue {
    TStreamWithYield(TUnboxedValueVector items, ui32 yieldPos, ui32 index, TWatermark& watermark, TWatermarksPattern watermarksPattern)
        : Items_(std::move(items))
        , YieldPos_(yieldPos)
        , Index_(index)
        , Watermark_(watermark)
        , WatermarksPattern_(std::move(watermarksPattern))
        , WatermarkIndex_(0)
    {
    }

private:
    TUnboxedValueVector Items_;
    ui32 YieldPos_;
    ui32 Index_;
    TWatermark& Watermark_;
    TWatermarksPattern WatermarksPattern_;
    ui32 WatermarkIndex_;

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
        if (Index_ >= Items_.size()) {
            return NUdf::EFetchStatus::Finish;
        }
        if (Index_ == YieldPos_) {
            return NUdf::EFetchStatus::Yield;
        }
        if (WatermarkIndex_ < WatermarksPattern_.size()) {
            auto [patternIndex, patternValue] = WatermarksPattern_[WatermarkIndex_];
            if (Index_ >= patternIndex) {
                Watermark_.WatermarkIn = patternValue;
                return NUdf::EFetchStatus::Yield;
            }
        }
        result = Items_[Index_++];
        return NUdf::EFetchStatus::Ok;
    }
};

THolder<IComputationGraph> BuildGraph(TSetup<false>& setup, const std::vector<std::tuple<ui32, i64, ui32>> items,
                                      ui32 yieldPos, ui32 startIndex, bool dataWatermarks,
                                      bool withWatermarks, TWatermark& watermark,
                                      const TWatermarksPattern& watermarksPattern, bool checkMinWindowStart = false, bool adjustLateEvents = false) {
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

    ui64 hop = 10;
    ui64 interval = 30;
    ui64 delay = 20;

    auto pgmReturn = pgmBuilder.MultiHoppingCore(
        TRuntimeNode(streamNode, /*isImmediate=*/false),
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
            std::vector<std::pair<std::string_view, TRuntimeNode>> members;
            members.emplace_back("key", key);
            members.emplace_back("sum", pgmBuilder.Member(state, "sum"));
            members.emplace_back("time", time);
            return pgmBuilder.NewStruct(members);
        },
        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&hop, sizeof(hop))),           // hop
        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&interval, sizeof(interval))), // interval
        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&delay, sizeof(delay))),       // delay
        pgmBuilder.NewDataLiteral<bool>(dataWatermarks),
        pgmBuilder.NewDataLiteral<bool>(withWatermarks),
        {}, // SizeLimit
        {}, // TimeLimit
        {}, // EarlyPolicy
        adjustLateEvents ? pgmBuilder.NewDataLiteral<ui32>(static_cast<ui32>(NYql::NHoppingWindow::EPolicy::Adjust)) : TRuntimeNode{},
        checkMinWindowStart
    );

    auto graph = setup.BuildGraph(pgmReturn, {streamNode});

    TUnboxedValueVector streamItems;
    for (const auto& item : items) {
        NUdf::TUnboxedValue* itemsPtr;
        auto structValues = graph->GetHolderFactory().CreateDirectArrayHolder(3, itemsPtr);
        itemsPtr[keyIndex] = NUdf::TUnboxedValuePod(std::get<0>(item));
        itemsPtr[timeIndex] = NUdf::TUnboxedValuePod(std::get<1>(item));
        itemsPtr[sumIndex] = NUdf::TUnboxedValuePod(std::get<2>(item));
        streamItems.emplace_back(std::move(structValues));
    }

    auto streamValue = NUdf::TUnboxedValuePod(new TStreamWithYield(streamItems, yieldPos, startIndex, watermark, watermarksPattern));
    graph->GetEntryPoint(0, /*require=*/true)->SetValue(graph->GetContext(), std::move(streamValue));
    return graph;
}
} // namespace

Y_UNIT_TEST_SUITE(TDqMultiHoppingSaveLoadTest) {
Y_UNIT_TEST(LegacyProgramsPreserveCheckpointVersions) {
    for (const auto timestamp : {10, 100}) {
        const std::vector<std::tuple<ui32, i64, ui32>> input = {{1, timestamp, 2}, {2, 150, 3}};
        TWatermark watermark;
        TSetup<false> setup(GetAuxCallableFactory(watermark));
        auto graph = BuildGraph(setup, input, 1, 0, false, true, watermark, {});
        auto root = graph->GetValue();
        NUdf::TUnboxedValue value;
        UNIT_ASSERT_VALUES_EQUAL(root.Fetch(value), NUdf::EFetchStatus::Yield);
        const auto checkpoint = graph->SaveGraphState();
        TStringBuf data(checkpoint);
        const auto size = ReadUi64(data);
        UNIT_ASSERT_VALUES_EQUAL(size, data.size());
        TInputSerializer in(data, EMkqlStateType::SIMPLE_BLOB);
        UNIT_ASSERT_VALUES_EQUAL(in.GetStateVersion(), timestamp == 10 ? 1 : 2);

        for (bool enableReplay : {false, true}) {
            TSetup<false> restoredSetup(GetAuxCallableFactory(watermark));
            auto restored = BuildGraph(restoredSetup, input, 1, 1, false, true, watermark, {}, enableReplay);
            restored->LoadGraphState(checkpoint);
            UNIT_ASSERT(restored->GetValue().IsBoxed());
            const auto saved = restored->SaveGraphState();
            if (!enableReplay) {
                UNIT_ASSERT_VALUES_EQUAL(saved, checkpoint);
            } else {
                TStringBuf data(saved);
                const auto size = ReadUi64(data);
                UNIT_ASSERT_VALUES_EQUAL(size, data.size());
                TInputSerializer in(data, EMkqlStateType::SIMPLE_BLOB);
                UNIT_ASSERT_VALUES_EQUAL(in.GetStateVersion(), 3);
            }
        }
    }
}

Y_UNIT_TEST(LegacyProgramsLoadReplayState) {
    TWatermark watermark;
    const std::vector<std::tuple<ui32, i64, ui32>> input = {{1, 10, 2}, {2, 150, 3}};
    TSetup<false> setup(GetAuxCallableFactory(watermark));
    auto graph = BuildGraph(setup, input, 1, 0, false, true, watermark, {}, true);
    auto root = graph->GetValue();
    NUdf::TUnboxedValue value;
    UNIT_ASSERT_VALUES_EQUAL(root.Fetch(value), NUdf::EFetchStatus::Yield);
    const auto checkpoint = graph->SaveGraphState();
    TSetup<false> legacySetup(GetAuxCallableFactory(watermark));
    auto legacy = BuildGraph(legacySetup, input, 1, 1, false, true, watermark, {});
    legacy->LoadGraphState(checkpoint);
    UNIT_ASSERT(legacy->GetValue().IsBoxed());
    const auto saved = legacy->SaveGraphState();
    TStringBuf data(saved);
    const auto size = ReadUi64(data);
    UNIT_ASSERT_VALUES_EQUAL(size, data.size());
    TInputSerializer in(data, EMkqlStateType::SIMPLE_BLOB);
    UNIT_ASSERT_VALUES_EQUAL(in.GetStateVersion(), 1);
}

Y_UNIT_TEST_TWIN(ReplayOnlyProducesCompleteWindowsAfterSaveLoad, AdjustLateEvents) {
    const std::vector<std::tuple<ui32, i64, ui32>> input = {
        {1, 95, 1000}, {1, 100, 2}, {1, 110, 3}, {1, 130, 4}, {2, 140, 7}
    };
    const std::vector<std::tuple<ui32, ui32, ui64>> expected = {
        {1, AdjustLateEvents ? 1005 : 5, 130}, {1, 7, 140}, {1, 4, 150}, {1, 4, 160},
        {2, 7, 150}, {2, 7, 160}, {2, 7, 170}
    };
    for (ui32 yieldPos = 0; yieldPos < input.size(); ++yieldPos) {
        TWatermark watermark;
        TSetup<false> setup1(GetAuxCallableFactory(watermark));
        auto graph1 = BuildGraph(setup1, input, yieldPos, 0, false, true, watermark, {}, true, AdjustLateEvents);
        // Version 3 prefix: the minimum complete-window start as a hop index.
        TString state;
        WriteUi32(state, static_cast<ui32>(EMkqlStateType::SIMPLE_BLOB));
        WriteUi32(state, 3);
        WriteUi64(state, 10); // 100 microseconds / 10-microsecond hop.
        WriteUi32(state, 0);
        WriteBool(state, false);
        TString recoveryState;
        TNodeStateHelper::AddNodeState(recoveryState, state);
        graph1->LoadGraphState(recoveryState);
        auto root1 = graph1->GetValue();
        NUdf::TUnboxedValue value;
        UNIT_ASSERT_VALUES_EQUAL(root1.Fetch(value), NUdf::EFetchStatus::Yield);
        const auto checkpoint = graph1->SaveGraphState();

        TSetup<false> setup2(GetAuxCallableFactory(watermark));
        auto graph2 = BuildGraph(setup2, input, -1, yieldPos, false, true, watermark, {}, true, AdjustLateEvents);
        graph2->LoadGraphState(checkpoint);
        auto root2 = graph2->GetValue();
        NUdf::TUnboxedValue restoredValue;
        std::vector<std::tuple<ui32, ui32, ui64>> actual;
        while (root2.Fetch(restoredValue) == NUdf::EFetchStatus::Ok) {
            actual.emplace_back(restoredValue.GetElement(0).Get<ui32>(), restoredValue.GetElement(1).Get<ui32>(), restoredValue.GetElement(2).Get<ui64>());
        }
        auto sortedExpected = expected;
        std::sort(actual.begin(), actual.end());
        std::sort(sortedExpected.begin(), sortedExpected.end());
        UNIT_ASSERT_EQUAL_C(actual, sortedExpected, "yieldPos = " << yieldPos);
    }
}

Y_UNIT_TEST_TWIN(RestoredBoundaryRespectsLateEventPolicy, AdjustLateEvents) {
    for (bool hasOpenWindows : {false, true}) {
        TWatermark watermark;
        if (hasOpenWindows) {
            watermark.WatermarkIn = TInstant::MicroSeconds(220);
        }
        const std::vector<std::tuple<ui32, i64, ui32>> input = {
            {1, hasOpenWindows ? 200 : 10, 2}, {1, 150, 3}, {2, 150, 5}, {3, 210, 7}
        };
        TSetup<false> setup(GetAuxCallableFactory(watermark));
        auto graph = BuildGraph(setup, input, 1, 0, false, true, watermark, {}, true, AdjustLateEvents);
        auto root = graph->GetValue();
        NUdf::TUnboxedValue value;
        UNIT_ASSERT_VALUES_EQUAL(root.Fetch(value), NUdf::EFetchStatus::Yield);
        if (!hasOpenWindows) {
            watermark.WatermarkIn = TInstant::MicroSeconds(220);
            while (root.Fetch(value) == NUdf::EFetchStatus::Ok) {
            }
        }
        const auto checkpoint = graph->SaveGraphState();

        // The checkpoint carries the window boundary even without an input watermark.
        watermark.WatermarkIn.Clear();
        TSetup<false> restoredSetup(GetAuxCallableFactory(watermark));
        auto restored = BuildGraph(restoredSetup, input, -1, 1, false, true, watermark, {}, true, AdjustLateEvents);
        restored->LoadGraphState(checkpoint);
        auto restoredRoot = restored->GetValue();
        NUdf::TUnboxedValue restoredValue;
        std::vector<std::tuple<ui32, ui32, ui64>> actual;
        while (restoredRoot.Fetch(restoredValue) == NUdf::EFetchStatus::Ok) {
            actual.emplace_back(restoredValue.GetElement(0).Get<ui32>(), restoredValue.GetElement(1).Get<ui32>(), restoredValue.GetElement(2).Get<ui64>());
        }
        std::vector<std::tuple<ui32, ui32, ui64>> expected = {{3, 7, 230}, {3, 7, 240}};
        if (hasOpenWindows || AdjustLateEvents) {
            expected.emplace_back(1, (hasOpenWindows ? 2 : 0) + (AdjustLateEvents ? 3 : 0), 230);
        }
        if (AdjustLateEvents) {
            expected.emplace_back(2, 5, 230);
        }
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        UNIT_ASSERT_EQUAL_C(actual, expected, "hasOpenWindows = " << hasOpenWindows);
    }
}

Y_UNIT_TEST(RecoveryMetadataSurvivesClosingAllWindows) {
    TWatermark watermark;
    TSetup<false> setup(GetAuxCallableFactory(watermark));
    auto graph = BuildGraph(setup, {{1, 10, 2}, {2, 120, 3}}, 1, 0, false, true, watermark, {}, true);
    auto root = graph->GetValue();
    NUdf::TUnboxedValue value;
    UNIT_ASSERT_VALUES_EQUAL(root.Fetch(value), NUdf::EFetchStatus::Yield);
    const auto readInfo = [&]() {
        const auto checkpoint = graph->SaveGraphState();
        TStringBuf data(checkpoint);
        const auto size = ReadUi64(data);
        UNIT_ASSERT_VALUES_EQUAL(size, data.size());
        TInputSerializer in(data, EMkqlStateType::SIMPLE_BLOB);
        UNIT_ASSERT_VALUES_EQUAL(in.GetStateVersion(), 3);
        return in.Read<ui64>();
    };
    const auto before = readInfo();
    UNIT_ASSERT_VALUES_EQUAL(before, 0);

    watermark.WatermarkIn = TInstant::MicroSeconds(100);
    while (root.Fetch(value) == NUdf::EFetchStatus::Ok) {
    }
    const auto after = readInfo();
    UNIT_ASSERT_VALUES_EQUAL(after, 8);
}

void TestWithSaveLoadImpl(
    const std::vector<std::tuple<ui32, i64, ui32>> input,
    const std::vector<std::tuple<ui32, ui32, ui64>> expected,
    bool withTraverse,
    bool dataWatermarks,
    bool withWatermarks = false,
    const TWatermarksPattern& watermarksPattern = {},
    bool checkMinWindowStart = false,
    bool adjustLateEvents = false)
{
    TWatermark watermark;
    UNIT_ASSERT(!dataWatermarks || !withWatermarks);
    for (ui32 yieldPos = 0; yieldPos < input.size(); ++yieldPos) {
        std::vector<std::tuple<ui32, ui32, ui64>> result;

        TSetup<false> setup1(GetAuxCallableFactory(watermark));
        auto graph1 = BuildGraph(setup1, input, yieldPos, 0, dataWatermarks, withWatermarks, watermark, watermarksPattern, checkMinWindowStart, adjustLateEvents);
        auto root1 = graph1->GetValue();

        NUdf::EFetchStatus status = NUdf::EFetchStatus::Ok;
        while (status == NUdf::EFetchStatus::Ok) {
            NUdf::TUnboxedValue val;
            status = root1.Fetch(val);
            if (status == NUdf::EFetchStatus::Ok) {
                result.emplace_back(val.GetElement(0).Get<ui32>(), val.GetElement(1).Get<ui32>(), val.GetElement(2).Get<ui64>());
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
        auto graph2 = BuildGraph(setup2, input, -1, yieldPos, dataWatermarks, withWatermarks, watermark, watermarksPattern, checkMinWindowStart, adjustLateEvents);
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
                result.emplace_back(val.GetElement(0).Get<ui32>(), val.GetElement(1).Get<ui32>(), val.GetElement(2).Get<ui64>());
            }
        }
        UNIT_ASSERT_EQUAL(status, NUdf::EFetchStatus::Finish);

        auto copy = result;
        auto sortedExpected = expected;
        std::sort(result.begin(), result.end());
        std::sort(sortedExpected.begin(), sortedExpected.end());
        UNIT_ASSERT_EQUAL_C(result, sortedExpected, " withTraverse = " << withTraverse << " dataWatermarks = " << dataWatermarks << " withWatermarks = " << withWatermarks << " yieldPos = " << yieldPos);
    }
}

Y_UNIT_TEST_TWIN(DataWatermarksRestoreLateEvents, AdjustLateEvents) {
    const std::vector<std::tuple<ui32, i64, ui32>> input = {
        {1, 100, 2},
        {2, 250, 7}, // Watermark 230: minimum window start 210, key 1 removed.
        {1, 150, 3}, // Removed key.
        {3, 150, 5}, // New key.
        {2, 150, 11}, // Existing key, whose window starts at 230.
        {4, 230, 13}, // Behind the last event, but inside unclosed windows.
        {5, 210, 17}, // Exactly at the saved minimum window start.
        {6, 269, 19}, // Watermark advances to 240.
        {7, 270, 23}, // Watermark 250: minimum window start 230.
        {1, 220, 29}, // The minimum advances after restore, too.
        {8, 230, 31},
        {9, 215, 37}
    };
    std::vector<std::tuple<ui32, ui32, ui64>> expected = {
        {1, 2, 110}, {1, 2, 120}, {1, 2, 130},
        {2, AdjustLateEvents ? 18 : 7, 260}, {2, 7, 270}, {2, 7, 280},
        {4, 13, 240}, {4, 13, 250}, {4, 13, 260},
        {5, 17, 240},
        {6, 19, 270}, {6, 19, 280}, {6, 19, 290},
        {7, 23, 280}, {7, 23, 290}, {7, 23, 300},
        {8, 31, 260}
    };
    if (AdjustLateEvents) {
        expected.insert(expected.end(), {{1, 3, 240}, {3, 5, 240}, {1, 29, 260}, {9, 37, 260}});
    }
    for (bool withTraverse : {false, true}) {
        TestWithSaveLoadImpl(input, expected, withTraverse, /*dataWatermarks=*/true,
            /*withWatermarks=*/false, {}, /*checkMinWindowStart=*/true, AdjustLateEvents);
    }
}

} // Y_UNIT_TEST_SUITE(TDqMultiHoppingSaveLoadTest)

} // namespace NKikimr::NMiniKQL
