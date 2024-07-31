#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mock_spiller_factory_ut.h>

#include <cstring>
#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {
namespace {

constexpr auto border = 9124596000000000ULL;
constexpr ui64 g_Yield = std::numeric_limits<ui64>::max();
constexpr ui64 g_TestYieldStreamData[] = {0, 1, 0, 2, g_Yield, 0, g_Yield, 1, 2, 0, 1, 3, 0, g_Yield, 1, 2};

class TTestStreamWrapper: public TMutableComputationNode<TTestStreamWrapper> {
using TBaseComputation = TMutableComputationNode<TTestStreamWrapper>;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx)
            : TBase(memInfo), CompCtx(compCtx)
        {}
    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {

            constexpr auto size = Y_ARRAY_SIZE(g_TestYieldStreamData);
            if (Index == size) {
                return NUdf::EFetchStatus::Finish;
            }

            const auto val = g_TestYieldStreamData[Index];
            if (g_Yield == val) {
                ++Index;
                return NUdf::EFetchStatus::Yield;
            }

            NUdf::TUnboxedValue* items = nullptr;
            result = CompCtx.HolderFactory.CreateDirectArrayHolder(2, items);
            items[0] = NUdf::TUnboxedValuePod(val);
            items[1] =  NUdf::TUnboxedValuePod(MakeString(ToString(val)));

            ++Index;

            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& CompCtx;
        ui64 Index = 0;
    };

    TTestStreamWrapper(TComputationMutables& mutables)
        : TBaseComputation(mutables)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx);
    }
private:
    void RegisterDependencies() const final {}
};

IComputationNode* WrapTestStream(const TComputationNodeFactoryContext& ctx) {
    return new TTestStreamWrapper(ctx.Mutables);
}

TComputationNodeFactory GetNodeFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestYieldStream") {
            return WrapTestStream(ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

template <bool LLVM>
TRuntimeNode MakeStream(TSetup<LLVM>& setup) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    TCallableBuilder callableBuilder(*setup.Env, "TestYieldStream",
        pb.NewStreamType(
            pb.NewStructType({
                {TStringBuf("a"), pb.NewDataType(NUdf::EDataSlot::Uint64)},
                {TStringBuf("b"), pb.NewDataType(NUdf::EDataSlot::String)}
            })
        )
    );

    return TRuntimeNode(callableBuilder.Build(), false);
}

template <bool OverFlow>
TRuntimeNode Combine(TProgramBuilder& pb, TRuntimeNode stream, std::function<TRuntimeNode(TRuntimeNode, TRuntimeNode)> finishLambda) {
    const auto keyExtractor = [&](TRuntimeNode item) {
        return pb.Member(item, "a");
    };
    const auto init = [&](TRuntimeNode /*key*/, TRuntimeNode item) {
        return item;
    };
    const auto update = [&](TRuntimeNode /*key*/, TRuntimeNode item, TRuntimeNode state) {
        const auto a = pb.Add(pb.Member(item, "a"), pb.Member(state, "a"));
        const auto b = pb.Concat(pb.Member(item, "b"), pb.Member(state, "b"));
        return pb.NewStruct({
            {TStringBuf("a"), a},
            {TStringBuf("b"), b},
        });
    };

    return OverFlow ?
        pb.FromFlow(pb.CombineCore(pb.ToFlow(stream), keyExtractor, init, update, finishLambda, 64ul << 20)):
        pb.CombineCore(stream, keyExtractor, init, update, finishLambda, 64ul << 20);
}

template<bool SPILLING>
TRuntimeNode WideLastCombiner(TProgramBuilder& pb, TRuntimeNode flow, const TProgramBuilder::TWideLambda& extractor, const TProgramBuilder::TBinaryWideLambda& init, const TProgramBuilder::TTernaryWideLambda& update, const TProgramBuilder::TBinaryWideLambda& finish) {
    return SPILLING ?
        pb.WideLastCombinerWithSpilling(flow, extractor, init, update, finish):
        pb.WideLastCombiner(flow, extractor, init, update, finish);
}

void CheckIfStreamHasExpectedStringValues(const NUdf::TUnboxedValue& streamValue, std::unordered_set<TString>& expected) {
        NUdf::TUnboxedValue item;
        NUdf::EFetchStatus fetchStatus;
        while (!expected.empty()) {
            fetchStatus = streamValue.Fetch(item);
            UNIT_ASSERT_UNEQUAL(fetchStatus, NUdf::EFetchStatus::Finish);
            if (fetchStatus == NYql::NUdf::EFetchStatus::Yield) continue;

            const auto actual = TString(item.AsStringRef());

            auto it = expected.find(actual);
            UNIT_ASSERT(it != expected.end());
            expected.erase(it);
        }
        fetchStatus = streamValue.Fetch(item);
        UNIT_ASSERT_EQUAL(fetchStatus, NUdf::EFetchStatus::Finish);
}

} // unnamed

#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
Y_UNIT_TEST_SUITE(TMiniKQLWideCombinerTest) {
    Y_UNIT_TEST_LLVM(TestLongStringsRefCounting) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, dataType});

        const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
        const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

        const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
        const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

        const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
        const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
        const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
        const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
        const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
        const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
        const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
        const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
        const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

        const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

        const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
        const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

        const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

        const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
        const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
        const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
        const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
        const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }), -100000LL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {pb.NewOptional(items.back()), pb.NewOptional(keys.front()), pb.NewEmptyOptional(optionalType), pb.NewEmptyOptional(optionalType)};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.NewOptional(items.back()), state.front(), state[1U], state[2U]};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                state.erase(state.cbegin());
                return {pb.FlatMap(pb.NewList(optionalType, state), [&](TRuntimeNode item) { return item; } )};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode {
                return pb.Fold1(items.front(),
                    [&](TRuntimeNode item) { return item; },
                    [&](TRuntimeNode item, TRuntimeNode state) {
                        return pb.AggrConcat(pb.AggrConcat(state, pb.NewDataLiteral<NUdf::EDataSlot::String>(" / ")), item);
                    }
                );
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "key one");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long value 2 / key two");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long key one");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long value 8 / very long value 7 / very long value 6");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestLongStringsPasstroughtRefCounting) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto tupleType = pb.NewTupleType({dataType, dataType});

        const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
        const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

        const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
        const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

        const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
        const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
        const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
        const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
        const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
        const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
        const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
        const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
        const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

        const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

        const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
        const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

        const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

        const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
        const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
        const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
        const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
        const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }), -1000000LL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.back(), keys.front(), items.back(), items.front()};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {items.back(), keys.front(), state[2U], state.back()};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return state;
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode {
                return pb.Fold1(pb.NewList(dataType, items),
                    [&](TRuntimeNode item) { return item; },
                    [&](TRuntimeNode item, TRuntimeNode state) {
                        return pb.AggrConcat(pb.AggrConcat(state, pb.NewDataLiteral<NUdf::EDataSlot::String>(" / ")), item);
                    }
                );
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long value 1 / key one / very long value 1 / key one");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long value 3 / key two / very long value 2 / key two");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long value 4 / very long key one / very long value 4 / very long key one");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "very long value 9 / very long key two / very long value 5 / very long key two");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoNotCalculateUnusedInput) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optionalType, dataType});

        const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
        const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

        const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
        const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
        const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
        const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
        const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");

        const auto empty = pb.NewDataLiteral<NUdf::EDataSlot::String>("");

        const auto none = pb.NewEmptyOptional(optionalType);

        const auto data1 = pb.NewTuple(tupleType, {keyOne, none, value1});
        const auto data2 = pb.NewTuple(tupleType, {keyTwo, none, value2});
        const auto data3 = pb.NewTuple(tupleType, {keyTwo, none, value3});
        const auto data4 = pb.NewTuple(tupleType, {keyOne, none, value4});
        const auto data5 = pb.NewTuple(tupleType, {keyOne, none, value5});
        const auto data6 = pb.NewTuple(tupleType, {keyOne, none, value1});
        const auto data7 = pb.NewTuple(tupleType, {keyOne, none, value2});
        const auto data8 = pb.NewTuple(tupleType, {keyTwo, none, value3});
        const auto data9 = pb.NewTuple(tupleType, {keyTwo, none, value4});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto landmine = pb.NewDataLiteral<NUdf::EDataSlot::String>("ACHTUNG MINEN!");

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Unwrap(pb.Nth(item, 1U), landmine, __FILE__, __LINE__, 0), pb.Nth(item, 2U)}; }), -1000000LL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.back(), keys.front(), empty, empty};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {items.back(), state.front(), state[1U], state[2U]};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                state.insert(state.cbegin(), keys.cbegin(), keys.cend());
                return {pb.NewList(dataType, state)};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode {
                return pb.Fold1(items.front(),
                    [&](TRuntimeNode item) { return item; },
                    [&](TRuntimeNode item, TRuntimeNode state) {
                        return pb.AggrConcat(pb.AggrConcat(state, pb.NewDataLiteral<NUdf::EDataSlot::String>(" / ")), item);
                    }
                );
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "key one / value 2 / value 1 / value 5 / value 4");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "key two / value 4 / value 3 / value 3 / value 2");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoNotCalculateUnusedOutput) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optionalType, dataType});

        const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
        const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

        const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
        const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
        const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
        const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
        const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");

        const auto empty = pb.NewDataLiteral<NUdf::EDataSlot::String>("");

        const auto none = pb.NewEmptyOptional(optionalType);

        const auto data1 = pb.NewTuple(tupleType, {keyOne, none, value1});
        const auto data2 = pb.NewTuple(tupleType, {keyTwo, none, value2});
        const auto data3 = pb.NewTuple(tupleType, {keyTwo, none, value3});
        const auto data4 = pb.NewTuple(tupleType, {keyOne, none, value4});
        const auto data5 = pb.NewTuple(tupleType, {keyOne, none, value5});
        const auto data6 = pb.NewTuple(tupleType, {keyOne, none, value1});
        const auto data7 = pb.NewTuple(tupleType, {keyOne, none, value2});
        const auto data8 = pb.NewTuple(tupleType, {keyTwo, none, value3});
        const auto data9 = pb.NewTuple(tupleType, {keyTwo, none, value4});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto landmine = pb.NewDataLiteral<NUdf::EDataSlot::String>("ACHTUNG MINEN!");

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }), 0ULL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items[1U], items.back()};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.Concat(state.front(), items[1U]), pb.AggrConcat(pb.AggrConcat(state.back(), pb.NewDataLiteral<NUdf::EDataSlot::String>(", ")), items.back())};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.Unwrap(state.front(), landmine, __FILE__, __LINE__, 0),  pb.AggrConcat(pb.AggrConcat(keys.front(), pb.NewDataLiteral<NUdf::EDataSlot::String>(": ")), state.back())};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.back(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "key one: value 1, value 4, value 5, value 1, value 2");
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item, "key two: value 2, value 3, value 3, value 4");
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestThinAllLambdas) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto tupleType = pb.NewTupleType({});
        const auto data = pb.NewTuple({});

        const auto list = pb.NewList(tupleType, {data, data, data, data});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(list),
            [](TRuntimeNode) -> TRuntimeNode::TList { return {}; }), 0ULL,
            [](TRuntimeNode::TList items) { return items; },
            [](TRuntimeNode::TList, TRuntimeNode::TList items) { return items; },
            [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
            [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; }),
            [&](TRuntimeNode::TList) { return pb.NewTuple({}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 46u
    Y_UNIT_TEST_LLVM(TestHasLimitButPasstroughtYields) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto stream = MakeStream<LLVM>(setup);
        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(stream),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Member(item, "a"), pb.Member(item, "b")}; }), -123456789LL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return items; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {state.front(), pb.AggrConcat(state.back(), items.back())}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList { return state; }),
            [&](TRuntimeNode::TList items) { return items.back(); }
        ));
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "00000");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "1111");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "222");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "3");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Finish);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Finish);
    }
#endif
}

Y_UNIT_TEST_SUITE(TMiniKQLWideCombinerPerfTest) {
    Y_UNIT_TEST_LLVM(TestSumDoubleBooleanKeys) {
        TSetup<LLVM> setup;

        double positive = 0.0, negative = 0.0;
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            (sample.second > 0.0 ? positive : negative) += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }), 0ULL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.AggrGreater(items.front(), pb.NewDataLiteral(0.0))}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return items; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.front())}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList { return state; }),
            [&](TRuntimeNode::TList items) { return items.front(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        std::transform(I8Samples.cbegin(), I8Samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto first = value.GetElement(0);
        const auto second = value.GetElement(1);
        const auto t2 = TInstant::Now();

        if (first.template Get<double>() > 0.0) {
            UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), positive);
            UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), negative);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), negative);
            UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), positive);
        }

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleBooleanKeys) {
        TSetup<LLVM> setup;
        auto samples = I8Samples;
        samples.emplace_back(-1, -1.0); //ensure to have at least one negative value
        samples.emplace_back(1, 1.0); //ensure to have at least one positive value
        double pSum = 0.0, nSum = 0.0, pMax = 0.0, nMax = -1000.0, pMin = 1000.0, nMin = 0.0;
        const auto t = TInstant::Now();
        for (const auto& sample : samples) {
            if (sample.second > 0.0) {
                pSum += sample.second;
                pMax = std::max(pMax, sample.second);
                pMin = std::min(pMin, sample.second);
            } else {
                nSum += sample.second;
                nMax = std::max(nMax, sample.second);
                nMin = std::min(nMin, sample.second);
            }
        }

        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }), 0ULL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.AggrGreater(items.front(), pb.NewDataLiteral(0.0))}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), items.front(), items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.front()), pb.AggrMin(state[1U], items.front()), pb.AggrMax(state.back(), items.back()) };
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList { return state; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(samples.size(), items));
        std::transform(samples.cbegin(), samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto first = value.GetElement(0);
        const auto second = value.GetElement(1);
        const auto t2 = TInstant::Now();

        if (first.GetElement(0).template Get<double>() > 0.0) {
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(0).template Get<double>(), pSum);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(1).template Get<double>(), pMin);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(2).template Get<double>(), pMax);

            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(0).template Get<double>(), nSum);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(1).template Get<double>(), nMin);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(2).template Get<double>(), nMax);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(0).template Get<double>(), nSum);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(1).template Get<double>(), nMin);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(2).template Get<double>(), nMax);

            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(0).template Get<double>(), pSum);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(1).template Get<double>(), pMin);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(2).template Get<double>(), pMax);
        }

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleSmallKey) {
        TSetup<LLVM> setup;

        std::unordered_map<i8, double> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            expects.emplace(sample.first, 0.0).first->second += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<i8, double>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<i8, double> l, const std::pair<i8, double> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(item, 0U), pb.Nth(item, 1U) }; }), 0ULL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.back())}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), state.front()}; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        for (const auto& sample : I8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElement(0).template Get<i8>(), ptr[i].GetElement(1).template Get<double>());
        }

        std::sort(two.begin(), two.end(), [](const std::pair<i8, double> l, const std::pair<i8, double> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleSmallKey) {
        TSetup<LLVM> setup;

        std::unordered_map<i8, std::array<double, 3U>> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, std::numeric_limits<double>::max(), std::numeric_limits<double>::min()}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<i8, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<i8, std::array<double, 3U>> l, const std::pair<i8, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(item, 0U), pb.Nth(item, 1U) }; }), 0ULL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back(), items.back(), items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.back()), pb.AggrMin(state[1U], items.back()), pb.AggrMax(state.back(), items.back())}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { state.insert(state.cbegin(), keys.front()); return state; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        for (const auto& sample : I8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElement(0).template Get<i8>(), std::array<double, 3U>{ptr[i].GetElement(1).template Get<double>(), ptr[i].GetElement(2).template Get<double>(), ptr[i].GetElement(3).template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<i8, std::array<double, 3U>> l, const std::pair<i8, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleStringKey) {
        TSetup<LLVM> setup;

        std::vector<std::pair<std::string, double>> stringI8Samples(I8Samples.size());
        std::transform(I8Samples.cbegin(), I8Samples.cend(), stringI8Samples.begin(), [](std::pair<i8, double> src){ return std::make_pair(ToString(src.first), src.second); });

        std::unordered_map<std::string, double> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : stringI8Samples) {
            expects.emplace(sample.first, 0.0).first->second += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::string_view, double>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::string_view, double> l, const std::pair<std::string_view, double> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<const char*>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(item, 0U), pb.Nth(item, 1U) }; }), 0ULL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.back())}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), state.front()}; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(stringI8Samples.size(), items));
        for (const auto& sample : stringI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod::Embedded(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElements()->AsStringRef(), ptr[i].GetElement(1).template Get<double>());
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::string_view, double> l, const std::pair<std::string_view, double> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleStringKey) {
        TSetup<LLVM> setup;

        std::vector<std::pair<std::string, double>> stringI8Samples(I8Samples.size());
        std::transform(I8Samples.cbegin(), I8Samples.cend(), stringI8Samples.begin(), [](std::pair<i8, double> src){ return std::make_pair(ToString(src.first), src.second); });

        std::unordered_map<std::string, std::array<double, 3U>> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : stringI8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, +1E7, -1E7}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::string_view, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::string_view, std::array<double, 3U>> l, const std::pair<std::string_view, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<const char*>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(item, 0U), pb.Nth(item, 1U) }; }), 0ULL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back(), items.back(), items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.back()), pb.AggrMin(state[1U], items.back()), pb.AggrMax(state.back(), items.back())}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { state.insert(state.cbegin(), keys.front()); return state; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(stringI8Samples.size(), items));
        for (const auto& sample : stringI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod::Embedded(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElements()->AsStringRef(), std::array<double, 3U>{ptr[i].GetElement(1).template Get<double>(), ptr[i].GetElement(2).template Get<double>(), ptr[i].GetElement(3).template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::string_view, std::array<double, 3U>> l, const std::pair<std::string_view, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumTupleKey) {
        TSetup<LLVM> setup;

        std::vector<std::pair<std::pair<ui32, std::string>, double>> pairSamples(Ui16Samples.size());
        std::transform(Ui16Samples.cbegin(), Ui16Samples.cend(), pairSamples.begin(), [](std::pair<ui16, double> src){ return std::make_pair(std::make_pair(ui32(src.first / 10U % 100U), ToString(src.first % 10U)), src.second); });

        struct TPairHash { size_t operator()(const std::pair<ui32, std::string>& p) const { return CombineHashes(std::hash<ui32>()(p.first), std::hash<std::string_view>()(p.second)); } };

        std::unordered_map<std::pair<ui32, std::string>, std::array<double, 3U>, TPairHash> expects;
        const auto t = TInstant::Now();
        for (const auto& sample : pairSamples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, +1E7, -1E7}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::pair<ui32, std::string>, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> l, const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<const char*>::Id)}), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(pb.Nth(item, 0U), 0U), pb.Nth(pb.Nth(item, 0U), 1U), pb.Nth(item, 1U) }; }), 0ULL,
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), items[1U]}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back(), items.back(), items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.back()), pb.AggrMin(state[1U], items.back()), pb.AggrMax(state.back(), items.back()) };
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), keys.back(), state.front(), state[1U], state.back()}; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple({pb.NewTuple({items[0U], items[1U]}), items[2U], items[3U], items[4U]}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(pairSamples.size(), items));
        for (const auto& sample : pairSamples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
            NUdf::TUnboxedValue* keys = nullptr;
            pair[0] =  graph->GetHolderFactory().CreateDirectArrayHolder(2U, keys);
            keys[0] = NUdf::TUnboxedValuePod(sample.first.first);
            keys[1] = NUdf::TUnboxedValuePod::Embedded(sample.first.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            const auto elements = ptr[i].GetElements();
            two.emplace_back(std::make_pair(elements[0].GetElement(0).template Get<ui32>(), (elements[0].GetElements()[1]).AsStringRef()), std::array<double, 3U>{elements[1].template Get<double>(), elements[2].template Get<double>(), elements[3].template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> l, const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestTpch) {
        TSetup<LLVM> setup;

        struct TPairHash { size_t operator()(const std::pair<std::string_view, std::string_view>& p) const { return CombineHashes(std::hash<std::string_view>()(p.first), std::hash<std::string_view>()(p.second)); } };

        std::unordered_map<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>, TPairHash> expects;
        const auto t = TInstant::Now();
        for (auto& sample : TpchSamples) {
            if (std::get<0U>(sample) <= border) {
                const auto& ins = expects.emplace(std::pair<std::string_view, std::string_view>{std::get<1U>(sample), std::get<2U>(sample)}, std::pair<ui64, std::array<double, 5U>>{0ULL, {0., 0., 0., 0., 0.}});
                auto& item = ins.first->second;
                ++item.first;
                std::get<0U>(item.second) += std::get<3U>(sample);
                std::get<1U>(item.second) += std::get<5U>(sample);
                std::get<2U>(item.second) += std::get<6U>(sample);
                const auto v = std::get<3U>(sample) * (1. - std::get<5U>(sample));
                std::get<3U>(item.second) += v;
                std::get<4U>(item.second) += v * (1. + std::get<4U>(sample));
            }
        }
        for (auto& item : expects) {
            std::get<1U>(item.second.second) /= item.second.first;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::pair<std::string, std::string>, std::pair<ui64, std::array<double, 5U>>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> l, const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({
            pb.NewDataType(NUdf::TDataType<ui64>::Id),
            pb.NewDataType(NUdf::TDataType<const char*>::Id),
            pb.NewDataType(NUdf::TDataType<const char*>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id)
        }));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideCombiner(
            pb.WideFilter(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U), pb.Nth(item, 4U), pb.Nth(item, 5U), pb.Nth(item, 6U)}; }),
                [&](TRuntimeNode::TList items) { return pb.AggrLessOrEqual(items.front(), pb.NewDataLiteral<ui64>(border)); }
            ), 0ULL,
            [&](TRuntimeNode::TList item) -> TRuntimeNode::TList { return {item[1U], item[2U]}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                const auto price = items[3U];
                const auto disco = items[5U];
                const auto v = pb.Mul(price, pb.Sub(pb.NewDataLiteral<double>(1.), disco));
                return {pb.NewDataLiteral<ui64>(1ULL), price, disco, items[6U], v, pb.Mul(v, pb.Add(pb.NewDataLiteral<double>(1.), items[4U]))};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                const auto price = items[3U];
                const auto disco = items[5U];
                const auto v = pb.Mul(price, pb.Sub(pb.NewDataLiteral<double>(1.), disco));
                return {pb.Increment(state[0U]), pb.AggrAdd(state[1U], price), pb.AggrAdd(state[2U], disco), pb.AggrAdd(state[3U], items[6U]), pb.AggrAdd(state[4U], v), pb.AggrAdd(state[5U], pb.Mul(v, pb.Add(pb.NewDataLiteral<double>(1.), items[4U])))};
            },
            [&](TRuntimeNode::TList key, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {key.front(), key.back(), state[0U], state[1U], pb.Div(state[2U], state[0U]), state[3U], state[4U], state[5U]}; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(TpchSamples.size(), items));
        for (const auto& sample : TpchSamples) {
            NUdf::TUnboxedValue* elements = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(7U, elements);
            elements[0] = NUdf::TUnboxedValuePod(std::get<0U>(sample));
            elements[1] = NUdf::TUnboxedValuePod::Embedded(std::get<1U>(sample));
            elements[2] = NUdf::TUnboxedValuePod::Embedded(std::get<2U>(sample));
            elements[3] = NUdf::TUnboxedValuePod(std::get<3U>(sample));
            elements[4] = NUdf::TUnboxedValuePod(std::get<4U>(sample));
            elements[5] = NUdf::TUnboxedValuePod(std::get<5U>(sample));
            elements[6] = NUdf::TUnboxedValuePod(std::get<6U>(sample));
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            const auto elements = ptr[i].GetElements();
            two.emplace_back(std::make_pair(elements[0].AsStringRef(), elements[1].AsStringRef()), std::pair<ui64, std::array<double, 5U>>{elements[2].template Get<ui64>(), {elements[3].template Get<double>(), elements[4].template Get<double>(), elements[5].template Get<double>(), elements[6].template Get<double>(), elements[7].template Get<double>()}});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> l, const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }
}
#endif
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 29u
Y_UNIT_TEST_SUITE(TMiniKQLWideLastCombinerTest) {
    Y_UNIT_TEST_LLVM_SPILLING(TestLongStringsRefCounting) {
        // Currently LLVM version doesn't support spilling.
        if (LLVM && SPILLING) return;
        // callable WideLastCombinerWithSpilling was introduced in 49 version of runtime
        if (MKQL_RUNTIME_VERSION < 49U && SPILLING) return;

        TSetup<LLVM, SPILLING> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, dataType});

        const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
        const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

        const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
        const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

        const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
        const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
        const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
        const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
        const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
        const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
        const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
        const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
        const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

        const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

        const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
        const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

        const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

        const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
        const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
        const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
        const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
        const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(WideLastCombiner<SPILLING>(pb, pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {pb.NewOptional(items.back()), pb.NewOptional(keys.front()), pb.NewEmptyOptional(optionalType), pb.NewEmptyOptional(optionalType)};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.NewOptional(items.back()), state.front(), state[1U], state[2U]};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                state.erase(state.cbegin());
                return {pb.FlatMap(pb.NewList(optionalType, state), [&](TRuntimeNode item) { return item; } )};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode {
                return pb.Fold1(items.front(),
                    [&](TRuntimeNode item) { return item; },
                    [&](TRuntimeNode item, TRuntimeNode state) {
                        return pb.AggrConcat(pb.AggrConcat(state, pb.NewDataLiteral<NUdf::EDataSlot::String>(" / ")), item);
                    }
                );
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        if (SPILLING) {
            graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
        }

        const auto streamVal = graph->GetValue();
        std::unordered_set<TString> expected {
            "key one",
            "very long value 2 / key two",
            "very long key one",
            "very long value 8 / very long value 7 / very long value 6"
        };

        CheckIfStreamHasExpectedStringValues(streamVal, expected);
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestLongStringsPasstroughtRefCounting) {
        // Currently LLVM version doesn't support spilling.
        if (LLVM && SPILLING) return;
        // callable WideLastCombinerWithSpilling was introduced in 49 version of runtime
        if (MKQL_RUNTIME_VERSION < 49U && SPILLING) return;
        TSetup<LLVM, SPILLING> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto tupleType = pb.NewTupleType({dataType, dataType});

        const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
        const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

        const auto longKeyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key one");
        const auto longKeyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long key two");

        const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 1");
        const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 2");
        const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 3");
        const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 4");
        const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 5");
        const auto value6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 6");
        const auto value7 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 7");
        const auto value8 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 8");
        const auto value9 = pb.NewDataLiteral<NUdf::EDataSlot::String>("very long value 9");

        const auto data1 = pb.NewTuple(tupleType, {keyOne, value1});

        const auto data2 = pb.NewTuple(tupleType, {keyTwo, value2});
        const auto data3 = pb.NewTuple(tupleType, {keyTwo, value3});

        const auto data4 = pb.NewTuple(tupleType, {longKeyOne, value4});

        const auto data5 = pb.NewTuple(tupleType, {longKeyTwo, value5});
        const auto data6 = pb.NewTuple(tupleType, {longKeyTwo, value6});
        const auto data7 = pb.NewTuple(tupleType, {longKeyTwo, value7});
        const auto data8 = pb.NewTuple(tupleType, {longKeyTwo, value8});
        const auto data9 = pb.NewTuple(tupleType, {longKeyTwo, value9});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(WideLastCombiner<SPILLING>(pb, pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.back(), keys.front(), items.back(), items.front()};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {items.back(), keys.front(), state[2U], state.back()};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return state;
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode {
                return pb.Fold1(pb.NewList(dataType, items),
                    [&](TRuntimeNode item) { return item; },
                    [&](TRuntimeNode item, TRuntimeNode state) {
                        return pb.AggrConcat(pb.AggrConcat(state, pb.NewDataLiteral<NUdf::EDataSlot::String>(" / ")), item);
                    }
                );
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        if (SPILLING) {
            graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
        }

        const auto streamVal = graph->GetValue();
        std::unordered_set<TString> expected {
            "very long value 1 / key one / very long value 1 / key one",
            "very long value 3 / key two / very long value 2 / key two",
            "very long value 4 / very long key one / very long value 4 / very long key one",
            "very long value 9 / very long key two / very long value 5 / very long key two"
        };

        CheckIfStreamHasExpectedStringValues(streamVal, expected);
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestDoNotCalculateUnusedInput) {
        // Test is broken. Remove this if after YQL-18808.
        if (SPILLING) return;

        // Currently LLVM version doesn't support spilling.
        if (LLVM && SPILLING) return;
        // callable WideLastCombinerWithSpilling was introduced in 49 version of runtime
        if (MKQL_RUNTIME_VERSION < 49U && SPILLING) return;
        TSetup<LLVM, SPILLING> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optionalType, dataType});

        const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
        const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

        const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
        const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
        const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
        const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
        const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");

        const auto empty = pb.NewDataLiteral<NUdf::EDataSlot::String>("");

        const auto none = pb.NewEmptyOptional(optionalType);

        const auto data1 = pb.NewTuple(tupleType, {keyOne, none, value1});
        const auto data2 = pb.NewTuple(tupleType, {keyTwo, none, value2});
        const auto data3 = pb.NewTuple(tupleType, {keyTwo, none, value3});
        const auto data4 = pb.NewTuple(tupleType, {keyOne, none, value4});
        const auto data5 = pb.NewTuple(tupleType, {keyOne, none, value5});
        const auto data6 = pb.NewTuple(tupleType, {keyOne, none, value1});
        const auto data7 = pb.NewTuple(tupleType, {keyOne, none, value2});
        const auto data8 = pb.NewTuple(tupleType, {keyTwo, none, value3});
        const auto data9 = pb.NewTuple(tupleType, {keyTwo, none, value4});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto landmine = pb.NewDataLiteral<NUdf::EDataSlot::String>("ACHTUNG MINEN!");

        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(WideLastCombiner<SPILLING>(pb, pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Unwrap(pb.Nth(item, 1U), landmine, __FILE__, __LINE__, 0), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items.back(), keys.front(), empty, empty};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {items.back(), state.front(), state[1U], state[2U]};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                state.insert(state.cbegin(), keys.cbegin(), keys.cend());
                return {pb.NewList(dataType, state)};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode {
                return pb.Fold1(items.front(),
                    [&](TRuntimeNode item) { return item; },
                    [&](TRuntimeNode item, TRuntimeNode state) {
                        return pb.AggrConcat(pb.AggrConcat(state, pb.NewDataLiteral<NUdf::EDataSlot::String>(" / ")), item);
                    }
                );
            }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        if (SPILLING) {
            graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
        }

        const auto streamVal = graph->GetValue();
        std::unordered_set<TString> expected {
            "key one / value 2 / value 1 / value 5 / value 4",
            "key two / value 4 / value 3 / value 3 / value 2"
        };

        CheckIfStreamHasExpectedStringValues(streamVal, expected);
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestDoNotCalculateUnusedOutput) {
        // Currently LLVM version doesn't support spilling.
        if (LLVM && SPILLING) return;
        // callable WideLastCombinerWithSpilling was introduced in 49 version of runtime
        if (MKQL_RUNTIME_VERSION < 49U && SPILLING) return;
        TSetup<LLVM, SPILLING> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<const char*>::Id);
        const auto optionalType = pb.NewOptionalType(dataType);
        const auto tupleType = pb.NewTupleType({dataType, optionalType, dataType});

        const auto keyOne = pb.NewDataLiteral<NUdf::EDataSlot::String>("key one");
        const auto keyTwo = pb.NewDataLiteral<NUdf::EDataSlot::String>("key two");

        const auto value1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 1");
        const auto value2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 2");
        const auto value3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 3");
        const auto value4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 4");
        const auto value5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("value 5");

        const auto empty = pb.NewDataLiteral<NUdf::EDataSlot::String>("");

        const auto none = pb.NewEmptyOptional(optionalType);

        const auto data1 = pb.NewTuple(tupleType, {keyOne, none, value1});
        const auto data2 = pb.NewTuple(tupleType, {keyTwo, none, value2});
        const auto data3 = pb.NewTuple(tupleType, {keyTwo, none, value3});
        const auto data4 = pb.NewTuple(tupleType, {keyOne, none, value4});
        const auto data5 = pb.NewTuple(tupleType, {keyOne, none, value5});
        const auto data6 = pb.NewTuple(tupleType, {keyOne, none, value1});
        const auto data7 = pb.NewTuple(tupleType, {keyOne, none, value2});
        const auto data8 = pb.NewTuple(tupleType, {keyTwo, none, value3});
        const auto data9 = pb.NewTuple(tupleType, {keyTwo, none, value4});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4, data5, data6, data7, data8, data9});

        const auto landmine = pb.NewDataLiteral<NUdf::EDataSlot::String>("ACHTUNG MINEN!");

        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(WideLastCombiner<SPILLING>(pb, pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                return {items[1U], items.back()};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.Concat(state.front(), items[1U]), pb.AggrConcat(pb.AggrConcat(state.back(), pb.NewDataLiteral<NUdf::EDataSlot::String>(", ")), items.back())};
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.Unwrap(state.front(), landmine, __FILE__, __LINE__, 0),  pb.AggrConcat(pb.AggrConcat(keys.front(), pb.NewDataLiteral<NUdf::EDataSlot::String>(": ")), state.back())};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.back(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        if (SPILLING) {
            graph->GetContext().SpillerFactory = std::make_shared<TMockSpillerFactory>();
        }

        const auto streamVal = graph->GetValue();
        std::unordered_set<TString> expected {
            "key one: value 1, value 4, value 5, value 1, value 2",
            "key two: value 2, value 3, value 3, value 4"
        };

        CheckIfStreamHasExpectedStringValues(streamVal, expected);
    }

    Y_UNIT_TEST_LLVM_SPILLING(TestThinAllLambdas) {
        // Currently LLVM version doesn't support spilling.
        if (LLVM && SPILLING) return;
        // callable WideLastCombinerWithSpilling was introduced in 49 version of runtime
        if (MKQL_RUNTIME_VERSION < 49U && SPILLING) return;
        TSetup<LLVM, SPILLING> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto tupleType = pb.NewTupleType({});
        const auto data = pb.NewTuple({});

        const auto list = pb.NewList(tupleType, {data, data, data, data});

        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(WideLastCombiner<SPILLING>(pb, pb.ExpandMap(pb.ToFlow(list),
            [](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
            [](TRuntimeNode::TList items) { return items; },
            [](TRuntimeNode::TList, TRuntimeNode::TList items) { return items; },
            [](TRuntimeNode::TList, TRuntimeNode::TList, TRuntimeNode::TList state) { return state; },
            [](TRuntimeNode::TList, TRuntimeNode::TList state) { return state; }),
            [&](TRuntimeNode::TList) { return pb.NewTuple({}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue item;
        const auto fetchStatus = streamVal.Fetch(item);
        UNIT_ASSERT_EQUAL(fetchStatus, NUdf::EFetchStatus::Finish);
    }
}

Y_UNIT_TEST_SUITE(TMiniKQLWideLastCombinerPerfTest) {
    Y_UNIT_TEST_LLVM(TestSumDoubleBooleanKeys) {
        TSetup<LLVM> setup;

        double positive = 0.0, negative = 0.0;
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            (sample.second > 0.0 ? positive : negative) += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideLastCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.AggrGreater(items.front(), pb.NewDataLiteral(0.0))}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return items; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.front())}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList { return state; }),
            [&](TRuntimeNode::TList items) { return items.front(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        std::transform(I8Samples.cbegin(), I8Samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto first = value.GetElement(0);
        const auto second = value.GetElement(1);
        const auto t2 = TInstant::Now();

        if (first.template Get<double>() > 0.0) {
            UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), positive);
            UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), negative);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), negative);
            UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), positive);
        }

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleBooleanKeys) {
        TSetup<LLVM> setup;

        double pSum = 0.0, nSum = 0.0, pMax = 0.0, nMax = -1000.0, pMin = 1000.0, nMin = 0.0;
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            if (sample.second > 0.0) {
                pSum += sample.second;
                pMax = std::max(pMax, sample.second);
                pMin = std::min(pMin, sample.second);
            } else {
                nSum += sample.second;
                nMax = std::max(nMax, sample.second);
                nMin = std::min(nMin, sample.second);
            }
        }

        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideLastCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.AggrGreater(items.front(), pb.NewDataLiteral(0.0))}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), items.front(), items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.front()), pb.AggrMin(state[1U], items.front()), pb.AggrMax(state.back(), items.back()) };
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList state) -> TRuntimeNode::TList { return state; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        std::transform(I8Samples.cbegin(), I8Samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto first = value.GetElement(0);
        const auto second = value.GetElement(1);
        const auto t2 = TInstant::Now();

        if (first.GetElement(0).template Get<double>() > 0.0) {
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(0).template Get<double>(), pSum);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(1).template Get<double>(), pMin);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(2).template Get<double>(), pMax);

            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(0).template Get<double>(), nSum);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(1).template Get<double>(), nMin);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(2).template Get<double>(), nMax);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(0).template Get<double>(), nSum);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(1).template Get<double>(), nMin);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(2).template Get<double>(), nMax);

            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(0).template Get<double>(), pSum);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(1).template Get<double>(), pMin);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(2).template Get<double>(), pMax);
        }

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleSmallKey) {
        TSetup<LLVM> setup;

        std::unordered_map<i8, double> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            expects.emplace(sample.first, 0.0).first->second += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<i8, double>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<i8, double> l, const std::pair<i8, double> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideLastCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(item, 0U), pb.Nth(item, 1U) }; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.back())}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), state.front()}; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        for (const auto& sample : I8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElement(0).template Get<i8>(), ptr[i].GetElement(1).template Get<double>());
        }

        std::sort(two.begin(), two.end(), [](const std::pair<i8, double> l, const std::pair<i8, double> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleSmallKey) {
        TSetup<LLVM> setup;

        std::unordered_map<i8, std::array<double, 3U>> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, std::numeric_limits<double>::max(), std::numeric_limits<double>::min()}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<i8, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<i8, std::array<double, 3U>> l, const std::pair<i8, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideLastCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(item, 0U), pb.Nth(item, 1U) }; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back(), items.back(), items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.back()), pb.AggrMin(state[1U], items.back()), pb.AggrMax(state.back(), items.back())}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { state.insert(state.cbegin(), keys.front()); return state; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        for (const auto& sample : I8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElement(0).template Get<i8>(), std::array<double, 3U>{ptr[i].GetElement(1).template Get<double>(), ptr[i].GetElement(2).template Get<double>(), ptr[i].GetElement(3).template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<i8, std::array<double, 3U>> l, const std::pair<i8, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleStringKey) {
        TSetup<LLVM> setup;

        std::vector<std::pair<std::string, double>> stringI8Samples(I8Samples.size());
        std::transform(I8Samples.cbegin(), I8Samples.cend(), stringI8Samples.begin(), [](std::pair<i8, double> src){ return std::make_pair(ToString(src.first), src.second); });

        std::unordered_map<std::string, double> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : stringI8Samples) {
            expects.emplace(sample.first, 0.0).first->second += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::string_view, double>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::string_view, double> l, const std::pair<std::string_view, double> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<const char*>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideLastCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(item, 0U), pb.Nth(item, 1U) }; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.back())}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), state.front()}; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(stringI8Samples.size(), items));
        for (const auto& sample : stringI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod::Embedded(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElements()->AsStringRef(), ptr[i].GetElement(1).template Get<double>());
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::string_view, double> l, const std::pair<std::string_view, double> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleStringKey) {
        TSetup<LLVM> setup;

        std::vector<std::pair<std::string, double>> stringI8Samples(I8Samples.size());
        std::transform(I8Samples.cbegin(), I8Samples.cend(), stringI8Samples.begin(), [](std::pair<i8, double> src){ return std::make_pair(ToString(src.first), src.second); });

        std::unordered_map<std::string, std::array<double, 3U>> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : stringI8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, +1E7, -1E7}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::string_view, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::string_view, std::array<double, 3U>> l, const std::pair<std::string_view, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<const char*>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideLastCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(item, 0U), pb.Nth(item, 1U) }; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back(), items.back(), items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.AggrAdd(state.front(), items.back()), pb.AggrMin(state[1U], items.back()), pb.AggrMax(state.back(), items.back())}; },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { state.insert(state.cbegin(), keys.front()); return state; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(stringI8Samples.size(), items));
        for (const auto& sample : stringI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod::Embedded(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElements()->AsStringRef(), std::array<double, 3U>{ptr[i].GetElement(1).template Get<double>(), ptr[i].GetElement(2).template Get<double>(), ptr[i].GetElement(3).template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::string_view, std::array<double, 3U>> l, const std::pair<std::string_view, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumTupleKey) {
        TSetup<LLVM> setup;

        std::vector<std::pair<std::pair<ui32, std::string>, double>> pairSamples(Ui16Samples.size());
        std::transform(Ui16Samples.cbegin(), Ui16Samples.cend(), pairSamples.begin(), [](std::pair<ui16, double> src){ return std::make_pair(std::make_pair(ui32(src.first / 10U % 100U), ToString(src.first % 10U)), src.second); });

        struct TPairHash { size_t operator()(const std::pair<ui32, std::string>& p) const { return CombineHashes(std::hash<ui32>()(p.first), std::hash<std::string_view>()(p.second)); } };

        std::unordered_map<std::pair<ui32, std::string>, std::array<double, 3U>, TPairHash> expects;
        const auto t = TInstant::Now();
        for (const auto& sample : pairSamples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, +1E7, -1E7}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::pair<ui32, std::string>, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> l, const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<const char*>::Id)}), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideLastCombiner(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return { pb.Nth(pb.Nth(item, 0U), 0U), pb.Nth(pb.Nth(item, 0U), 1U), pb.Nth(item, 1U) }; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), items[1U]}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.back(), items.back(), items.back()}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.AggrAdd(state.front(), items.back()), pb.AggrMin(state[1U], items.back()), pb.AggrMax(state.back(), items.back()) };
            },
            [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {keys.front(), keys.back(), state.front(), state[1U], state.back()}; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple({pb.NewTuple({items[0U], items[1U]}), items[2U], items[3U], items[4U]}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(pairSamples.size(), items));
        for (const auto& sample : pairSamples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
            NUdf::TUnboxedValue* keys = nullptr;
            pair[0] =  graph->GetHolderFactory().CreateDirectArrayHolder(2U, keys);
            keys[0] = NUdf::TUnboxedValuePod(sample.first.first);
            keys[1] = NUdf::TUnboxedValuePod::Embedded(sample.first.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            const auto elements = ptr[i].GetElements();
            two.emplace_back(std::make_pair(elements[0].GetElement(0).template Get<ui32>(), (elements[0].GetElements()[1]).AsStringRef()), std::array<double, 3U>{elements[1].template Get<double>(), elements[2].template Get<double>(), elements[3].template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> l, const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestTpch) {
        TSetup<LLVM> setup;

        struct TPairHash { size_t operator()(const std::pair<std::string_view, std::string_view>& p) const { return CombineHashes(std::hash<std::string_view>()(p.first), std::hash<std::string_view>()(p.second)); } };

        std::unordered_map<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>, TPairHash> expects;
        const auto t = TInstant::Now();
        for (auto& sample : TpchSamples) {
            if (std::get<0U>(sample) <= border) {
                const auto& ins = expects.emplace(std::pair<std::string_view, std::string_view>{std::get<1U>(sample), std::get<2U>(sample)}, std::pair<ui64, std::array<double, 5U>>{0ULL, {0., 0., 0., 0., 0.}});
                auto& item = ins.first->second;
                ++item.first;
                std::get<0U>(item.second) += std::get<3U>(sample);
                std::get<1U>(item.second) += std::get<5U>(sample);
                std::get<2U>(item.second) += std::get<6U>(sample);
                const auto v = std::get<3U>(sample) * (1. - std::get<5U>(sample));
                std::get<3U>(item.second) += v;
                std::get<4U>(item.second) += v * (1. + std::get<4U>(sample));
            }
        }
        for (auto& item : expects) {
            std::get<1U>(item.second.second) /= item.second.first;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::pair<std::string, std::string>, std::pair<ui64, std::array<double, 5U>>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> l, const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({
            pb.NewDataType(NUdf::TDataType<ui64>::Id),
            pb.NewDataType(NUdf::TDataType<const char*>::Id),
            pb.NewDataType(NUdf::TDataType<const char*>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id)
        }));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideLastCombiner(
            pb.WideFilter(pb.ExpandMap(pb.ToFlow(TRuntimeNode(list, false)),
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U), pb.Nth(item, 4U), pb.Nth(item, 5U), pb.Nth(item, 6U)}; }),
                [&](TRuntimeNode::TList items) { return pb.AggrLessOrEqual(items.front(), pb.NewDataLiteral<ui64>(border)); }
            ),
            [&](TRuntimeNode::TList item) -> TRuntimeNode::TList { return {item[1U], item[2U]}; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
                const auto price = items[3U];
                const auto disco = items[5U];
                const auto v = pb.Mul(price, pb.Sub(pb.NewDataLiteral<double>(1.), disco));
                return {pb.NewDataLiteral<ui64>(1ULL), price, disco, items[6U], v, pb.Mul(v, pb.Add(pb.NewDataLiteral<double>(1.), items[4U]))};
            },
            [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                const auto price = items[3U];
                const auto disco = items[5U];
                const auto v = pb.Mul(price, pb.Sub(pb.NewDataLiteral<double>(1.), disco));
                return {pb.Increment(state[0U]), pb.AggrAdd(state[1U], price), pb.AggrAdd(state[2U], disco), pb.AggrAdd(state[3U], items[6U]), pb.AggrAdd(state[4U], v), pb.AggrAdd(state[5U], pb.Mul(v, pb.Add(pb.NewDataLiteral<double>(1.), items[4U])))};
            },
            [&](TRuntimeNode::TList key, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {key.front(), key.back(), state[0U], state[1U], pb.Div(state[2U], state[0U]), state[3U], state[4U], state[5U]}; }),
            [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(TpchSamples.size(), items));
        for (const auto& sample : TpchSamples) {
            NUdf::TUnboxedValue* elements = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(7U, elements);
            elements[0] = NUdf::TUnboxedValuePod(std::get<0U>(sample));
            elements[1] = NUdf::TUnboxedValuePod::Embedded(std::get<1U>(sample));
            elements[2] = NUdf::TUnboxedValuePod::Embedded(std::get<2U>(sample));
            elements[3] = NUdf::TUnboxedValuePod(std::get<3U>(sample));
            elements[4] = NUdf::TUnboxedValuePod(std::get<4U>(sample));
            elements[5] = NUdf::TUnboxedValuePod(std::get<5U>(sample));
            elements[6] = NUdf::TUnboxedValuePod(std::get<6U>(sample));
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            const auto elements = ptr[i].GetElements();
            two.emplace_back(std::make_pair(elements[0].AsStringRef(), elements[1].AsStringRef()), std::pair<ui64, std::array<double, 5U>>{elements[2].template Get<ui64>(), {elements[3].template Get<double>(), elements[4].template Get<double>(), elements[5].template Get<double>(), elements[6].template Get<double>(), elements[7].template Get<double>()}});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> l, const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }
}
#endif
}
}
