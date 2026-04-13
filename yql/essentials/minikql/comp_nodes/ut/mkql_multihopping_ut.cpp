#include "../mkql_multihopping.h"
#include "mkql_computation_node_ut.h"
#include <yql/essentials/core/sql_types/hopping.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_graph_saveload.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

namespace {
struct TInputItem {
    ui32 Key = 0;
    i64 Time = 0;
    ui32 Val = 0;
};

struct TOutputItem {
    ui32 Key = 0;
    TString Val;
    ui64 Time = 0;

    bool operator<(const TOutputItem& o) const {
        return Key < o.Key || (Key == o.Key && (Time < o.Time || (Time == o.Time && Val < o.Val)));
    }
    bool operator==(const TOutputItem& o) const = default;
};

[[maybe_unused]] IOutputStream& operator<<(IOutputStream& output, const TOutputItem& item) {
    return output << "TItem{Key = " << item.Key << ", Val = '" << item.Val << "', Time = " << item.Time << "}";
}

using TOutputGroup = std::vector<TOutputItem>;

using TCheckCallback = std::function<void()>;

using TStatsMap = TMap<TString, i64>;

TStatsMap DefaultStatsMap = {
    {"MultiHop_NewHopsCount", 0},
    {"MultiHop_FarFutureEventsCount", 0},
    {"MultiHop_LateThrownEventsCount", 0},
    {"MultiHop_EmptyTimeCount", 0},
    {"MultiHop_KeysCount", 1},
    {"MultiHop_FarFutureStateSize", 0},
};

using TEncoder = std::function<NUdf::TUnboxedValue(const TInputItem&, const THolderFactory&)>;
using TDecoder = std::function<TOutputItem(const NUdf::TUnboxedValue&)>;
using TFetchCallback = std::function<NUdf::EFetchStatus(NUdf::TUnboxedValue&)>;
using TFetchFactory = std::function<TFetchCallback(TUnboxedValueVector&&)>;

TFetchFactory DefaultFetchFactory = [](TUnboxedValueVector&& input) -> TFetchCallback {
    return [input = std::move(input),
            inputIndex = 0ull](NUdf::TUnboxedValue& result) mutable -> NUdf::EFetchStatus {
        if (inputIndex >= input.size()) {
            return NUdf::EFetchStatus::Finish;
        }
        result = input[inputIndex++];
        return NUdf::EFetchStatus::Ok;
    };
};

TComputationNodeFactory GetAuxCallableFactory(TWatermark& watermark) {
    return [&watermark](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "MyStream") {
            return new TExternalComputationNode(ctx.Mutables);
        } else if (callable.GetType()->GetName() == "MultiHoppingCore") {
            return WrapMultiHoppingCore(callable, ctx, watermark);
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}

using TSetupFactory = std::function<TSetup<false>()>;

TWatermark GlobalWatermark;
TSetupFactory DefaultSetupFactory = []() -> TSetup<false> {
    return TSetup<false>(GetAuxCallableFactory(GlobalWatermark));
};

struct TStream: public NUdf::TBoxedValue {
    TStream(TCheckCallback&& checkCallback, TFetchCallback&& fetchCallback)
        : CheckCallback_(std::move(checkCallback))
        , FetchCallback_(std::move(fetchCallback))
    {
    }

private:
    TCheckCallback CheckCallback_;
    TFetchCallback FetchCallback_;

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
        CheckCallback_();
        return FetchCallback_(result);
    }
};

std::tuple<TType*, TEncoder, TDecoder> BuildInputType(TSetup<false>& setup) {
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    auto structType = pgmBuilder.NewEmptyStructType();
    structType = pgmBuilder.NewStructType(structType, "key",
                                          pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
    structType = pgmBuilder.NewStructType(structType, "time",
                                          pgmBuilder.NewDataType(NUdf::TDataType<NUdf::TTimestamp>::Id));
    structType = pgmBuilder.NewStructType(structType, "sum",
                                          pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
    structType = pgmBuilder.NewStructType(structType, "str",
                                          pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id));
    auto keyIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("key");
    auto timeIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("time");
    auto strIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("str");
    auto sumIndex = AS_TYPE(TStructType, structType)->GetMemberIndex("sum");
    auto numFields = AS_TYPE(TStructType, structType)->GetMembersCount();

    auto encode = [keyIndex, timeIndex, sumIndex, numFields](const TInputItem& input, const THolderFactory& holderFactory) -> NUdf::TUnboxedValue {
        NUdf::TUnboxedValue* itemsPtr;
        auto structValues = holderFactory.CreateDirectArrayHolder(numFields, itemsPtr);
        itemsPtr[keyIndex] = NUdf::TUnboxedValuePod(input.Key);
        itemsPtr[timeIndex] = NUdf::TUnboxedValuePod(input.Time);
        itemsPtr[sumIndex] = NUdf::TUnboxedValuePod(input.Val);
        return structValues;
    };

    auto decode = [keyIndex, timeIndex, strIndex](const NUdf::TUnboxedValue& result) -> TOutputItem {
        return {
            result.GetElement(keyIndex).Get<ui32>(),
            TString(result.GetElements()[strIndex].AsStringRef()),
            result.GetElement(timeIndex).Get<ui64>(),
        };
    };

    return {structType, encode, decode};
}

using EHoppingWindowPolicy = NYql::NHoppingWindow::EPolicy;

THolder<IComputationGraph> BuildGraph(
    TSetup<false>& setup,
    TType* itemType,
    ui64 hop,
    ui64 interval,
    ui64 delay,
    bool dataWatermarks,
    bool watermarkMode,
    ui64 farFutureSizeLimit = Max<ui64>(),
    ui64 farFutureTimeLimitUs = Max<ui64>(),
    EHoppingWindowPolicy earlyPolicy = EHoppingWindowPolicy::Drop,
    EHoppingWindowPolicy latePolicy = EHoppingWindowPolicy::Drop) {
#if MKQL_RUNTIME_VERSION < 70U
    Y_UNUSED(farFutureSizeLimit);
    Y_UNUSED(farFutureTimeLimitUs);
    Y_UNUSED(earlyPolicy);
    Y_UNUSED(latePolicy);
#endif
    TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

    auto inStreamType = pgmBuilder.NewStreamType(itemType);

    TCallableBuilder inStream(pgmBuilder.GetTypeEnvironment(), "MyStream", inStreamType);
    auto streamNode = inStream.Build();

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
            members.emplace_back("str", pgmBuilder.ToString(pgmBuilder.Member(item, "sum")));
            return pgmBuilder.NewStruct(members);
        },
        [&](TRuntimeNode item, TRuntimeNode state) { // update
            auto add = pgmBuilder.AggrConcat(
                pgmBuilder.AggrConcat(
                    pgmBuilder.AggrConcat(
                        pgmBuilder.Member(state, "str"),
                        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(" ")),
                    pgmBuilder.ToString(
                        pgmBuilder.Member(item, "sum"))),
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(" Add"));
            std::vector<std::pair<std::string_view, TRuntimeNode>> members;
            members.emplace_back("str", add);
            return pgmBuilder.NewStruct(members);
        },
        [&](TRuntimeNode state) { // save
            return pgmBuilder.Member(state, "str");
        },
        [&](TRuntimeNode savedState) { // load
            std::vector<std::pair<std::string_view, TRuntimeNode>> members;
            members.emplace_back("str", savedState);
            return pgmBuilder.NewStruct(members);
        },
        [&](TRuntimeNode state1, TRuntimeNode state2) { // merge
            auto add = pgmBuilder.AggrConcat(
                pgmBuilder.AggrConcat(
                    pgmBuilder.AggrConcat(
                        pgmBuilder.Member(state1, "str"),
                        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(" ")),
                    pgmBuilder.Member(state2, "str")),
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(" Merge"));
            std::vector<std::pair<std::string_view, TRuntimeNode>> members;
            members.emplace_back("str", add);
            return pgmBuilder.NewStruct(members);
        },
        [&](TRuntimeNode key, TRuntimeNode state, TRuntimeNode time) { // finish
            std::vector<std::pair<std::string_view, TRuntimeNode>> members;
            members.emplace_back("key", key);
            members.emplace_back("str", pgmBuilder.Member(state, "str"));
            members.emplace_back("sum", time);
            members.emplace_back("time", time);
            return pgmBuilder.NewStruct(members);
        },
        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&hop, sizeof(hop))),           // hop
        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&interval, sizeof(interval))), // interval
        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&delay, sizeof(delay))),       // delay
        pgmBuilder.NewDataLiteral<bool>(dataWatermarks),
        pgmBuilder.NewDataLiteral<bool>(watermarkMode),
#if MKQL_RUNTIME_VERSION >= 70U
        farFutureSizeLimit == NYql::NHoppingWindow::TSettings{}.FarFutureSizeLimit ? pgmBuilder.NewVoid() : pgmBuilder.NewDataLiteral<ui64>(farFutureSizeLimit),
        farFutureTimeLimitUs == NYql::NHoppingWindow::TSettings{}.FarFutureTimeLimit.MicroSeconds() ? pgmBuilder.NewVoid() : pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Interval>(NUdf::TStringRef((const char*)&farFutureTimeLimitUs, sizeof(farFutureTimeLimitUs))),
        earlyPolicy == NYql::NHoppingWindow::TSettings{}.EarlyPolicy ? pgmBuilder.NewVoid() : pgmBuilder.NewDataLiteral<ui32>((ui32)earlyPolicy),
        latePolicy == NYql::NHoppingWindow::TSettings{}.LatePolicy ? pgmBuilder.NewVoid() : pgmBuilder.NewDataLiteral<ui32>((ui32)latePolicy)
#else
        {}, // SizeLimit
        {}, // TimeLimit
        {}, // EarlyPolicy
        {}  // LatePolicy
#endif
    );

    return setup.BuildGraph(pgmReturn, {streamNode});
}
} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLMultiHoppingTest) {
void TestImpl(
    const std::vector<TInputItem>& input,
    const std::vector<TOutputGroup>& expected,
    const TStatsMap& expectedStatsMap,
    ui64 hop = 10,
    ui64 interval = 30,
    ui64 delay = 20,
    bool dataWatermarks = false,
    bool watermarkMode = false,
    ui64 farFutureSizeLimit = Max<ui64>(),
    ui64 farFutureTimeLimitUs = Max<ui64>(),
    EHoppingWindowPolicy earlyPolicy = EHoppingWindowPolicy::Drop,
    EHoppingWindowPolicy latePolicy = EHoppingWindowPolicy::Drop,
    TFetchFactory fetchFactory = DefaultFetchFactory,
    TSetupFactory setupFactory = DefaultSetupFactory) {
    auto setup = setupFactory();

    auto [itemType, encode, decode] = BuildInputType(setup);

    auto graph = BuildGraph(setup, itemType, hop, interval, delay, dataWatermarks, watermarkMode, farFutureSizeLimit, farFutureTimeLimitUs, earlyPolicy, latePolicy);

    size_t index = 0;
    std::vector<TOutputItem> actual;
    auto checkCallback = [&expected, &index, &actual]() -> void {
        UNIT_ASSERT_LT_C(index, expected.size(), index << " < " << expected.size());
        auto expectedItems = expected[index];
        std::sort(expectedItems.begin(), expectedItems.end());
        std::sort(actual.begin(), actual.end());
        UNIT_ASSERT_VALUES_EQUAL_C(expectedItems, actual, index);
        ++index;
        actual.clear();
    };

    TUnboxedValueVector boxedInput;
    for (size_t i = 0; i < input.size(); ++i) {
        boxedInput.push_back(encode(input[i], graph->GetHolderFactory()));
    }
    auto fetchCallback = fetchFactory(std::move(boxedInput));

    auto streamValue = NUdf::TUnboxedValuePod(new TStream(checkCallback, std::move(fetchCallback)));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(streamValue));

    auto root = graph->GetValue();

    auto status = NUdf::EFetchStatus::Ok;
    while (NUdf::EFetchStatus::Finish != status) {
        NUdf::TUnboxedValue result;
        status = root.Fetch(result);
        if (status == NUdf::EFetchStatus::Ok) {
            actual.push_back(decode(result));
        }
    }

    checkCallback();
    UNIT_ASSERT_VALUES_EQUAL(expected.size(), index);

    TStatsMap actualStatsMap;
    setup.StatsRegistry->ForEachStat([&expectedStatsMap, &actualStatsMap](const TStatKey& key, i64 value) {
        if (auto iter = expectedStatsMap.find(key.GetName());
            iter != expectedStatsMap.end()) {
            actualStatsMap.emplace(key.GetName(), value);
        }
    });
    UNIT_ASSERT_VALUES_EQUAL(expectedStatsMap, actualStatsMap);
}

void TestWatermarksImpl(
    const std::vector<TInputItem>& input,
    const std::vector<TOutputGroup>& expected,
    const std::vector<std::pair<ui64, TInstant>>& watermarks,
    const TStatsMap& expectedStatsMap,
    ui64 hop = 10,
    ui64 interval = 30,
    ui64 delay = 20,
    ui64 farFutureSizeLimit = Max<ui64>(),
    ui64 farFutureTimeLimitUs = Max<ui64>(),
    EHoppingWindowPolicy earlyPolicy = EHoppingWindowPolicy::Drop,
    EHoppingWindowPolicy latePolicy = EHoppingWindowPolicy::Drop) {
    TWatermark watermark;
    auto fetchFactory = [watermarks = watermarks, &watermark](TUnboxedValueVector input) -> TFetchCallback {
        return [input = input,
                inputIndex = 0ull,
                watermarks = watermarks,
                watermarkIndex = 0ull,
                &watermark](NUdf::TUnboxedValue& result) mutable -> NUdf::EFetchStatus {
            if (watermarkIndex < watermarks.size() && watermarks[watermarkIndex].first == inputIndex) {
                watermark.WatermarkIn = watermarks[watermarkIndex].second;
                ++watermarkIndex;
                return NUdf::EFetchStatus::Yield;
            }
            if (inputIndex >= input.size()) {
                return NUdf::EFetchStatus::Finish;
            }
            result = input[inputIndex++];
            return NUdf::EFetchStatus::Ok;
        };
    };
    auto setupFactory = [&watermark]() -> TSetup<false> {
        return TSetup<false>(GetAuxCallableFactory(watermark));
    };
    TestImpl(
        input,
        expected,
        expectedStatsMap,
        hop,
        interval,
        delay,
        false,
        true,
        farFutureSizeLimit,
        farFutureTimeLimitUs,
        earlyPolicy,
        latePolicy,
        fetchFactory,
        setupFactory);
}

#if MKQL_RUNTIME_VERSION >= 70U
Y_UNIT_TEST(TestThrowWatermarkFromPast) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
            {1, "4", 210},
            {1, "4", 220},
            {1, "4", 230},
            {1, "5", 310},
            {1, "5", 320},
            {1, "5", 330},
            {1, "6", 410},
            {1, "6", 420},
            {1, "6", 430},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {2, TInstant::MicroSeconds(20)},
        {3, TInstant::MicroSeconds(40)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 15;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 5;
    expectedStatsMap["MultiHop_KeysCount"] = 0;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap);
}

Y_UNIT_TEST(TestAdjustWatermarkFromPastTime0) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "5 6 Add", 70},
            {1, "5 6 Add", 80},
            {1, "5 6 Add", 90},
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
            {1, "4", 210},
            {1, "4", 220},
            {1, "4", 230},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {2, TInstant::MicroSeconds(20)},
        {3, TInstant::MicroSeconds(40)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 12;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 3;
    expectedStatsMap["MultiHop_KeysCount"] = 1;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                           // hop
                       30,                           // interval
                       20,                           // delay
                       Max<ui64>(),                  // farFutureSizeLimit
                       0,                            // farFutureTimeLimit
                       EHoppingWindowPolicy::Adjust, // earlyPolicy
                       EHoppingWindowPolicy::Drop);  // latePolicy
}

Y_UNIT_TEST(TestAdjustWatermarkFromPastTime100) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "5 6 Add 2 Merge", 120},
            {1, "5 6 Add 2 Merge", 130},
            {1, "3 5 6 Add Merge", 140},
            {1, "3", 150},
            {1, "3", 160},
            {1, "4", 210},
            {1, "4", 220},
            {1, "4", 230},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {2, TInstant::MicroSeconds(20)},
        {3, TInstant::MicroSeconds(40)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 9;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 5;
    expectedStatsMap["MultiHop_KeysCount"] = 0;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                           // hop
                       30,                           // interval
                       20,                           // delay
                       Max<ui64>(),                  // farFutureSizeLimit
                       100,                          // farFutureTimeLimit
                       EHoppingWindowPolicy::Adjust, // earlyPolicy
                       EHoppingWindowPolicy::Drop);  // latePolicy
}
#endif

Y_UNIT_TEST(TestThrowWatermarkFromFuture) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({})};
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {2, TInstant::MicroSeconds(1000)},
        {3, TInstant::MicroSeconds(2000)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 6;
    expectedStatsMap["MultiHop_LateThrownEventsCount"] = 3;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 2;
    expectedStatsMap["MultiHop_KeysCount"] = 0;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap);
}

#if MKQL_RUNTIME_VERSION >= 70U
Y_UNIT_TEST(TestAdjustWatermarkFromFuture) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({
            {1, "4", 1010},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "5 6 Add", 2010},
        })};
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {2, TInstant::MicroSeconds(1000)},
        {3, TInstant::MicroSeconds(2000)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 8;
    expectedStatsMap["MultiHop_LateThrownEventsCount"] = 3;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 2;
    expectedStatsMap["MultiHop_KeysCount"] = 1;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                            // hop
                       30,                            // interval
                       20,                            // delay
                       Max<ui64>(),                   // farFutureSizeLimit
                       Max<ui64>(),                   // farFutureTimeLimit
                       EHoppingWindowPolicy::Drop,    // earlyPolicy
                       EHoppingWindowPolicy::Adjust); // latePolicy
}

Y_UNIT_TEST(TestWatermarkFlow1) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "4", 210},
            {1, "4", 220},
            {1, "4", 230},
            {1, "5", 310},
            {1, "5", 320},
            {1, "5", 330},
            {1, "6", 410},
            {1, "6", 420},
            {1, "6", 430},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 15;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 4;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap);
}
#endif

Y_UNIT_TEST(TestWatermarkFlow1CloseTime0) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
        }),
        TOutputGroup({
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({
            {1, "4", 210},
            {1, "4", 220},
            {1, "4", 230},
        }),
        TOutputGroup({
            {1, "5", 310},
            {1, "5", 320},
            {1, "5", 330},
        }),
        TOutputGroup({
            {1, "6", 410},
            {1, "6", 420},
            {1, "6", 430},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 15;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 0;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                          // hop
                       30,                          // interval
                       20,                          // delay
                       Max<ui64>(),                 // farFutureSizeLimit
                       0,                           // farFutureTimeLimit
                       EHoppingWindowPolicy::Close, // earlyPolicy
                       EHoppingWindowPolicy::Drop); // latePolicy
}

#if MKQL_RUNTIME_VERSION >= 70U
Y_UNIT_TEST(TestWatermarkFlow1CloseTime100) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
        }),
        TOutputGroup({
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({
            {1, "4", 210},
            {1, "4", 220},
            {1, "4", 230},
        }),
        TOutputGroup({
            {1, "5", 310},
            {1, "5", 320},
            {1, "5", 330},
        }),
        TOutputGroup({
            {1, "6", 410},
            {1, "6", 420},
            {1, "6", 430},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 15;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 4;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                          // hop
                       30,                          // interval
                       20,                          // delay
                       Max<ui64>(),                 // farFutureSizeLimit
                       100,                         // farFutureTimeLimit
                       EHoppingWindowPolicy::Close, // earlyPolicy
                       EHoppingWindowPolicy::Drop); // latePolicy
}

Y_UNIT_TEST(TestWatermarkFlow1CloseCount0) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
        }),
        TOutputGroup({
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({
            {1, "4", 210},
            {1, "4", 220},
            {1, "4", 230},
        }),
        TOutputGroup({
            {1, "5", 310},
            {1, "5", 320},
            {1, "5", 330},
        }),
        TOutputGroup({
            {1, "6", 410},
            {1, "6", 420},
            {1, "6", 430},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 15;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 4;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                          // hop
                       30,                          // interval
                       20,                          // delay
                       0,                           // farFutureSizeLimit
                       Max<ui64>(),                 // farFutureTimeLimit
                       EHoppingWindowPolicy::Close, // earlyPolicy
                       EHoppingWindowPolicy::Drop); // latePolicy
}

Y_UNIT_TEST(TestWatermarkFlow1CloseCount1) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
        }),
        TOutputGroup({
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({
            {1, "4", 210},
            {1, "4", 220},
            {1, "4", 230},
        }),
        TOutputGroup({
            {1, "5", 310},
            {1, "5", 320},
            {1, "5", 330},
            {1, "6", 410},
            {1, "6", 420},
            {1, "6", 430},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 15;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 4;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                          // hop
                       30,                          // interval
                       20,                          // delay
                       1,                           // farFutureSizeLimit
                       Max<ui64>(),                 // farFutureTimeLimit
                       EHoppingWindowPolicy::Close, // earlyPolicy
                       EHoppingWindowPolicy::Drop); // latePolicy
}

Y_UNIT_TEST(TestWatermarkFlow1DropTime0) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 3;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 0;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                          // hop
                       30,                          // interval
                       20,                          // delay
                       Max<ui64>(),                 // farFutureSizeLimit
                       0,                           // farFutureTimeLimit
                       EHoppingWindowPolicy::Drop,  // earlyPolicy
                       EHoppingWindowPolicy::Drop); // latePolicy
}

Y_UNIT_TEST(TestWatermarkFlow1DropTime100) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            // no watermark -> time limit unused
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 6;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 1;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                          // hop
                       30,                          // interval
                       20,                          // delay
                       Max<ui64>(),                 // farFutureSizeLimit
                       100,                         // farFutureTimeLimit
                       EHoppingWindowPolicy::Drop,  // earlyPolicy
                       EHoppingWindowPolicy::Drop); // latePolicy
}

Y_UNIT_TEST(TestWatermarkFlow1DropCount0) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 3;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 4;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                          // hop
                       30,                          // interval
                       20,                          // delay
                       0,                           // farFutureSizeLimit
                       Max<ui64>(),                 // farFutureTimeLimit
                       EHoppingWindowPolicy::Drop,  // earlyPolicy
                       EHoppingWindowPolicy::Drop); // latePolicy
}

Y_UNIT_TEST(TestWatermarkFlow1DropCount1) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 131, 3},
        {1, 200, 4},
        {1, 300, 5},
        {1, 400, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2", 110},
            {1, "2", 120},
            {1, "2", 130},
            {1, "3", 140},
            {1, "3", 150},
            {1, "3", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "5", 310},
            {1, "5", 320},
            {1, "5", 330},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(100)},
        {3, TInstant::MicroSeconds(200)}};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 9;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 4;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap,
                       10,                          // hop
                       30,                          // interval
                       20,                          // delay
                       1,                           // farFutureSizeLimit
                       Max<ui64>(),                 // farFutureTimeLimit
                       EHoppingWindowPolicy::Drop,  // earlyPolicy
                       EHoppingWindowPolicy::Drop); // latePolicy
}

Y_UNIT_TEST(TestWatermarkFlow2) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 100, 2},
        {1, 105, 3},
        {1, 80, 4},
        {1, 107, 5},
        {1, 106, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "4", 90},
            {1, "4", 100},
            {1, "2 3 Add 5 Add 6 Add 4 Merge", 110},
            {1, "2 3 Add 5 Add 6 Add", 120},
            {1, "2 3 Add 5 Add 6 Add", 130},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(76)},
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 5;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 4;
    expectedStatsMap["MultiHop_KeysCount"] = 1;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap);
}

Y_UNIT_TEST(TestWatermarkFlow3) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 90, 2},
        {1, 99, 3},
        {1, 80, 4},
        {1, 107, 5},
        {1, 106, 6}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "4", 90},
            {1, "2 3 Add 4 Merge", 100},
            {1, "5 6 Add 2 3 Add 4 Merge Merge", 110},
            {1, "5 6 Add 2 3 Add Merge", 120},
            {1, "5 6 Add", 130},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {0, TInstant::MicroSeconds(76)},
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 5;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 2;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap);
}
#endif

Y_UNIT_TEST(TestWatermarkFlowOverflow) {
    // TODO this tests fails before this change, but it does not exercise
    // exact expected bug scenario (hop stuck forever in the future)
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 1, 2},
        {1, 2, 3},
        {1, 5, 4},
        {1, 6, 5},
        {1, 7, 6},
        {1, 8, 7},
        {1, 9, 8},
        {1, 10, 9},
        {1, 11, 10},
        {1, 22, 11},
        {1, 23, 12},
        {1, 24, 13},
        {1, 100, 14},
        {1, 117, 15},
        {1, 121, 16},
        {1, 126, 17},
    };
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "11 12 Add 13 Add 9 10 Add 2 3 Add 4 Add 5 Add 6 Add 7 Add 8 Add Merge Merge", 100},
            {1, "14 11 12 Add 13 Add 9 10 Add Merge Merge", 110},
        }),
        TOutputGroup({}),
        TOutputGroup({
            {1, "15 14 11 12 Add 13 Add Merge Merge", 120},
        }),
        TOutputGroup({
            {1, "16 17 Add 15 14 Merge Merge", 130},
            {1, "16 17 Add 15 14 Merge Merge", 140},
            {1, "16 17 Add 15 14 Merge Merge", 150},
            {1, "16 17 Add 15 14 Merge Merge", 160},
            {1, "16 17 Add 15 14 Merge Merge", 170},
            {1, "16 17 Add 15 14 Merge Merge", 180},
            {1, "16 17 Add 15 14 Merge Merge", 190},
            {1, "16 17 Add 15 14 Merge Merge", 200},
            {1, "16 17 Add 15 Merge", 210},
            {1, "16 17 Add", 220},
        }),
    };
    const std::vector<std::pair<ui64, TInstant>> watermarks = {
        {9, TInstant::MicroSeconds(1)},
        {12, TInstant::MicroSeconds(2)},
        {14, TInstant::MicroSeconds(50)},
        {15, TInstant::MicroSeconds(110)},
        {16, TInstant::MicroSeconds(120)},
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 13;
    expectedStatsMap["MultiHop_FarFutureEventsCount"] = 1;

    TestWatermarksImpl(input, expected, watermarks, expectedStatsMap, 10, 100, 20);
}

Y_UNIT_TEST(TestDataWatermarks) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {2, 101, 2},
        {1, 111, 3},
        {2, 140, 5},
        {2, 160, 1}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({{1, "2", 110}, {1, "3 2 Merge", 120}, {2, "2", 110}, {2, "2", 120}}),
        TOutputGroup({{2, "2", 130}, {1, "3 2 Merge", 130}, {1, "3", 140}}),
        TOutputGroup({{2, "5", 150}, {2, "5", 160}, {2, "1 5 Merge", 170}, {2, "1", 180}, {2, "1", 190}}),
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 12;

    TestImpl(input, expected, expectedStatsMap, 10, 30, 20, true);
}

Y_UNIT_TEST(TestDataWatermarksNoGarbage) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 100, 2},
        {2, 150, 1}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({{1, "2", 110}, {1, "2", 120}, {1, "2", 130}}),
        TOutputGroup({{2, "1", 160}, {2, "1", 170}, {2, "1", 180}}),
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 6;

    TestImpl(input, expected, expectedStatsMap, 10, 30, 20, true, false);
}

Y_UNIT_TEST(TestValidness1) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {2, 101, 2},
        {1, 111, 3},
        {2, 140, 5},
        {2, 160, 1}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {2, "2", 110},
            {2, "2", 120},
        }),
        TOutputGroup({
            {2, "2", 130},
        }),
        TOutputGroup({
            {1, "2", 110},
            {1, "3 2 Merge", 120},
            {1, "3 2 Merge", 130},
            {1, "3", 140},
            {2, "5", 150},
            {2, "5", 160},
            {2, "1 5 Merge", 170},
            {2, "1", 190},
            {2, "1", 180},
        }),
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 12;
    expectedStatsMap["MultiHop_KeysCount"] = 2;

    TestImpl(input, expected, expectedStatsMap);
}

Y_UNIT_TEST(TestValidness2) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {2, 101, 2},
        {1, 101, 2},
        {2, 102, 3},
        {1, 102, 3},
        {2, 115, 4},
        {1, 115, 4},
        {2, 123, 6},
        {1, 123, 6},
        {2, 124, 5},
        {1, 124, 5},
        {2, 125, 7},
        {1, 125, 7},
        {2, 140, 2},
        {1, 140, 2},
        {2, 147, 1},
        {1, 147, 1},
        {2, 151, 6},
        {1, 151, 6},
        {2, 159, 2},
        {1, 159, 2},
        {2, 185, 8},
        {1, 185, 8}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "2 3 Add", 110},
            {1, "4 2 3 Add Merge", 120},
            {2, "2 3 Add", 110},
            {2, "4 2 3 Add Merge", 120},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {2, "6 5 Add 7 Add 4 2 3 Add Merge Merge", 130},
            {1, "6 5 Add 7 Add 4 2 3 Add Merge Merge", 130},
        }),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {2, "6 5 Add 7 Add 4 Merge", 140},
            {2, "2 1 Add 6 5 Add 7 Add Merge", 150},
            {2, "6 2 Add 2 1 Add Merge", 160},
            {1, "6 5 Add 7 Add 4 Merge", 140},
            {1, "2 1 Add 6 5 Add 7 Add Merge", 150},
            {1, "6 2 Add 2 1 Add Merge", 160},
        }),
        TOutputGroup({}),
        TOutputGroup({
            {1, "6 2 Add 2 1 Add Merge", 170},
            {1, "6 2 Add", 180},
            {1, "8", 190},
            {1, "8", 200},
            {1, "8", 210},
            {2, "6 2 Add 2 1 Add Merge", 170},
            {2, "6 2 Add", 180},
            {2, "8", 190},
            {2, "8", 200},
            {2, "8", 210},
        }),
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 22;
    expectedStatsMap["MultiHop_KeysCount"] = 2;

    TestImpl(input, expected, expectedStatsMap, 10, 30, 20, true);
}

Y_UNIT_TEST(TestValidness3) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 105, 1},
        {1, 107, 4},
        {2, 106, 3},
        {1, 111, 7},
        {1, 117, 3},
        {2, 110, 2},
        {1, 108, 9},
        {1, 121, 4},
        {2, 107, 2},
        {2, 141, 5},
        {1, 141, 10}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}), TOutputGroup({}), TOutputGroup({}), TOutputGroup({}),
        TOutputGroup({}), TOutputGroup({}), TOutputGroup({}),
        TOutputGroup({
            {1, "1 4 Add 9 Add", 110},
            {2, "3", 110},
        }),
        TOutputGroup({}),
        TOutputGroup({
            {2, "2 3 2 Add Merge", 115},
            {2, "2", 120},
            {1, "7 1 4 Add 9 Add Merge", 115},
            {1, "3 7 Merge", 120},
            {1, "4 3 Merge", 125},
            {1, "4", 130},
        }),
        TOutputGroup({}),
        TOutputGroup({
            {1, "10", 145},
            {1, "10", 150},
            {2, "5", 145},
            {2, "5", 150},
        })};
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 12;
    expectedStatsMap["MultiHop_KeysCount"] = 2;

    TestImpl(input, expected, expectedStatsMap, 5, 10, 10, true);
}

Y_UNIT_TEST(TestDelay) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 3},
        {1, 111, 5},
        {1, 120, 7},
        {1, 80, 9},
        {1, 79, 11}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({
            {1, "3 9 Merge", 110},
            {1, "5 3 Merge", 120},
            {1, "7 5 3 Merge Merge", 130},
            {1, "7 5 Merge", 140},
            {1, "7", 150},
        }),
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 5;
    expectedStatsMap["MultiHop_LateThrownEventsCount"] = 1;

    TestImpl(input, expected, expectedStatsMap);
}

Y_UNIT_TEST(TestWindowsBeforeFirstElement) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 101, 2},
        {1, 111, 3}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({{1, "2", 110}, {1, "3 2 Merge", 120}, {1, "3 2 Merge", 130}, {1, "3", 140}}),
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 4;

    TestImpl(input, expected, expectedStatsMap);
}

Y_UNIT_TEST(TestSubzeroValues) {
    const std::vector<TInputItem> input = {
        // Group; Time; Value
        {1, 1, 2}};
    const std::vector<TOutputGroup> expected = {
        TOutputGroup({}),
        TOutputGroup({}),
        TOutputGroup({{1, "2", 30}}),
    };
    auto expectedStatsMap = DefaultStatsMap;
    expectedStatsMap["MultiHop_NewHopsCount"] = 1;

    TestImpl(input, expected, expectedStatsMap);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLMultiHoppingTest)

} // namespace NKikimr::NMiniKQL
